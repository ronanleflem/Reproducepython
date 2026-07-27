#!/usr/bin/env python3
"""Broker ROUTER pour reproduire la perte de reply ZMQ."""

from __future__ import annotations

import argparse
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import zmq

from config import (
    BrokerConfig,
    DEFAULT_BROKER_ENDPOINT,
    DEFAULT_MONITOR_ENDPOINT,
    add_zmq_options_args,
    apply_production_timings,
    env_log_level,
    zmq_options_from_args,
)
from logging_setup import setup_logging
from protocol import (
    Message,
    MsgType,
    WorkerState,
    log_send_result,
    log_transport_recv,
)
from socket_monitor import SocketMonitor

log = setup_logging("broker", env_log_level())


@dataclass
class WorkerRecord:
    routing_id: bytes
    worker_id: str
    session_id: str
    socket_generation: int
    state: WorkerState
    last_heartbeat: float
    current_job_id: str | None = None
    expired_at: float | None = None


@dataclass
class JobRecord:
    job_id: str
    worker_id: str
    session_id: str
    routing_id: bytes
    dispatched_at: float
    completed: bool = False
    result_accepted: bool = False
    result_rejected: bool = False
    reject_reason: str | None = None


@dataclass
class BrokerStats:
    worker_expired: bool = False
    raw_message_received: bool = False
    result_accepted: bool = False
    result_rejected: bool = False
    result_missing: bool = True


class Broker:
    def __init__(self, config: BrokerConfig) -> None:
        self.config = config
        self.ctx = zmq.Context.instance()
        self.socket = self.ctx.socket(zmq.ROUTER)
        self._heartbeat_counter = 0
        self._workers: dict[bytes, WorkerRecord] = {}
        self._jobs: dict[str, JobRecord] = {}
        self._expired_workers: list[WorkerRecord] = []
        self._stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._monitor: SocketMonitor | None = None
        self.stats = BrokerStats()
        self._job_dispatched = threading.Event()
        self._result_handled = threading.Event()

    def _log_effective_options(self) -> None:
        s = self.socket
        log.info(
            "ZMQ_OPTIONS component=broker IMMEDIATE=%s LINGER=%s "
            "RECONNECT_IVL=%s RECONNECT_IVL_MAX=%s SNDTIMEO=%s",
            s.get(zmq.IMMEDIATE),
            s.get(zmq.LINGER),
            s.get(zmq.RECONNECT_IVL),
            s.get(zmq.RECONNECT_IVL_MAX),
            s.get(zmq.SNDTIMEO),
        )

    def setup(self) -> None:
        opts = self.config.zmq_options
        opts.apply(self.socket)
        if self.config.monitor_enabled:
            self._monitor = SocketMonitor(
                "broker", f"{DEFAULT_MONITOR_ENDPOINT}-broker", log
            )
            self._monitor.attach(self.socket)
        self.socket.bind(self.config.endpoint)
        log.info("Broker bound to %s", self.config.endpoint)
        self._log_effective_options()
        if self._monitor:
            self._monitor.start()

    def _send_to_worker(self, routing_id: bytes, msg: Message) -> bool:
        frames = [routing_id] + msg.to_frames()
        start = time.monotonic()
        error: str | None = None
        errno_val: int | None = None
        success = False
        try:
            self.socket.send_multipart(frames)
            success = True
        except zmq.Again as exc:
            error = "zmq.Again"
            errno_val = exc.errno
        except zmq.ZMQError as exc:
            error = str(exc)
            errno_val = exc.errno
        duration_ms = (time.monotonic() - start) * 1000
        log_send_result(
            log,
            component="broker",
            msg_type=msg.msg_type.value,
            success=success,
            duration_ms=duration_ms,
            socket_generation=msg.socket_generation,
            session_id=msg.session_id,
            error=error,
            errno=errno_val,
        )
        return success

    def _expire_stale_workers(self) -> None:
        now = time.monotonic()
        timeout = self.config.worker_timeout
        for routing_id, worker in list(self._workers.items()):
            if worker.state == WorkerState.EXPIRED:
                continue
            age = now - worker.last_heartbeat
            if age > timeout:
                log.warning(
                    "APP worker_expired worker_id=%s session_id=%s routing_id=%s "
                    "state=%s last_heartbeat_age=%.3fs timeout=%.3fs job_id=%s",
                    worker.worker_id,
                    worker.session_id,
                    routing_id.hex(),
                    worker.state.value,
                    age,
                    timeout,
                    worker.current_job_id,
                )
                worker.state = WorkerState.EXPIRED
                worker.expired_at = now
                self.stats.worker_expired = True
                self._expired_workers.append(worker)
                del self._workers[routing_id]

    def _handle_ready(self, routing_id: bytes, msg: Message) -> None:
        worker = WorkerRecord(
            routing_id=routing_id,
            worker_id=msg.worker_id,
            session_id=msg.session_id,
            socket_generation=msg.socket_generation,
            state=WorkerState.READY,
            last_heartbeat=time.monotonic(),
        )
        self._workers[routing_id] = worker
        log.info(
            "APP ready_accepted worker_id=%s session_id=%s routing_id=%s "
            "socket_generation=%d",
            msg.worker_id,
            msg.session_id,
            routing_id.hex(),
            msg.socket_generation,
        )
        ack = Message(
            msg_type=MsgType.READY_ACK,
            worker_id=msg.worker_id,
            session_id=msg.session_id,
            socket_generation=msg.socket_generation,
            timestamp=time.time(),
        )
        self._send_to_worker(routing_id, ack)

    def _dispatch_job(self, routing_id: bytes, worker: WorkerRecord) -> None:
        job_id = str(uuid.uuid4())
        worker.state = WorkerState.BUSY
        worker.current_job_id = job_id
        self._jobs[job_id] = JobRecord(
            job_id=job_id,
            worker_id=worker.worker_id,
            session_id=worker.session_id,
            routing_id=routing_id,
            dispatched_at=time.time(),
        )
        job_msg = Message(
            msg_type=MsgType.JOB,
            worker_id=worker.worker_id,
            session_id=worker.session_id,
            socket_generation=worker.socket_generation,
            job_id=job_id,
            timestamp=time.time(),
            payload={"duration_hint": self.config.heartbeat_interval},
        )
        log.info(
            "APP job_dispatched job_id=%s worker_id=%s session_id=%s routing_id=%s",
            job_id,
            worker.worker_id,
            worker.session_id,
            routing_id.hex(),
        )
        self._send_to_worker(routing_id, job_msg)
        self._job_dispatched.set()

    def _handle_heartbeat(self, routing_id: bytes, msg: Message) -> None:
        worker = self._workers.get(routing_id)
        if worker:
            worker.last_heartbeat = time.monotonic()
            log.debug(
                "APP heartbeat_from_known_worker worker_id=%s session_id=%s "
                "heartbeat_counter=%d",
                msg.worker_id,
                msg.session_id,
                msg.heartbeat_counter,
            )
        else:
            log.info(
                "APP heartbeat_from_unknown_worker routing_id=%s worker_id=%s "
                "session_id=%s heartbeat_counter=%d",
                routing_id.hex(),
                msg.worker_id,
                msg.session_id,
                msg.heartbeat_counter,
            )

    def _handle_result(self, routing_id: bytes, msg: Message) -> None:
        worker = self._workers.get(routing_id)
        job = self._jobs.get(msg.job_id) if msg.job_id else None

        log.info(
            "APP result_received routing_id=%s worker_id=%s session_id=%s "
            "job_id=%s worker_known=%s worker_state=%s job_known=%s "
            "job_completed=%s policy=%s",
            routing_id.hex(),
            msg.worker_id,
            msg.session_id,
            msg.job_id,
            worker is not None,
            worker.state.value if worker else "N/A",
            job is not None,
            job.completed if job else "N/A",
            self.config.result_policy,
        )

        self.stats.raw_message_received = True
        accepted = False
        reject_reason: str | None = None

        if self.config.result_policy == "job-id":
            if job is None:
                reject_reason = "job_unknown"
            elif job.completed:
                reject_reason = "job_already_completed"
            else:
                accepted = True
        else:  # strict
            if worker is None:
                reject_reason = "worker_not_in_registry"
            elif worker.state != WorkerState.BUSY:
                reject_reason = f"worker_not_busy_state={worker.state.value}"
            elif worker.session_id != msg.session_id:
                reject_reason = (
                    f"session_mismatch expected={worker.session_id} "
                    f"got={msg.session_id}"
                )
            elif worker.current_job_id != msg.job_id:
                reject_reason = (
                    f"job_mismatch expected={worker.current_job_id} "
                    f"got={msg.job_id}"
                )
            else:
                accepted = True

        if accepted:
            self.stats.result_accepted = True
            self.stats.result_missing = False
            if job:
                job.completed = True
                job.result_accepted = True
            if worker:
                worker.state = WorkerState.READY
                worker.current_job_id = None
            ack = Message(
                msg_type=MsgType.RESULT_ACK,
                worker_id=msg.worker_id,
                session_id=msg.session_id,
                socket_generation=msg.socket_generation,
                job_id=msg.job_id,
                timestamp=time.time(),
            )
            self._send_to_worker(routing_id, ack)
            log.info(
                "APP result_accepted job_id=%s worker_id=%s session_id=%s",
                msg.job_id,
                msg.worker_id,
                msg.session_id,
            )
        else:
            self.stats.result_rejected = True
            self.stats.result_missing = False
            if job:
                job.result_rejected = True
                job.reject_reason = reject_reason
            log.warning(
                "APP result_rejected job_id=%s worker_id=%s session_id=%s "
                "reason=%s routing_id=%s",
                msg.job_id,
                msg.worker_id,
                msg.session_id,
                reject_reason,
                routing_id.hex(),
            )

        self._result_handled.set()

    def _process_message(self, routing_id: bytes, frames: list[bytes]) -> None:
        try:
            msg = Message.from_frames(frames)
        except (ValueError, KeyError, UnicodeDecodeError) as exc:
            log.error(
                "APP parse_error routing_id=%s frames=%d error=%s",
                routing_id.hex(),
                len(frames),
                exc,
            )
            return

        log.info(
            "APP message_parsed type=%s worker_id=%s session_id=%s job_id=%s "
            "socket_generation=%d heartbeat_counter=%d",
            msg.msg_type.value,
            msg.worker_id,
            msg.session_id,
            msg.job_id,
            msg.socket_generation,
            msg.heartbeat_counter,
        )

        if msg.msg_type == MsgType.READY:
            self._handle_ready(routing_id, msg)
        elif msg.msg_type == MsgType.HEARTBEAT:
            self._handle_heartbeat(routing_id, msg)
        elif msg.msg_type == MsgType.RESULT:
            self._handle_result(routing_id, msg)
        else:
            log.warning(
                "APP unexpected_message_type type=%s routing_id=%s",
                msg.msg_type.value,
                routing_id.hex(),
            )

    def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            self._expire_stale_workers()
            self._heartbeat_counter += 1
            now = time.time()
            for routing_id, worker in list(self._workers.items()):
                hb = Message(
                    msg_type=MsgType.HEARTBEAT,
                    worker_id=worker.worker_id,
                    session_id=worker.session_id,
                    socket_generation=worker.socket_generation,
                    heartbeat_counter=self._heartbeat_counter,
                    timestamp=now,
                )
                self._send_to_worker(routing_id, hb)
            self._stop.wait(self.config.heartbeat_interval)

    def run_once(self, auto_dispatch: bool = True, timeout: float = 60.0) -> BrokerStats:
        """Exécute un cycle broker : READY -> JOB -> attendre RESULT."""
        global log
        log = setup_logging("broker", self.config.log_level)
        self.setup()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name="broker-heartbeat", daemon=True
        )
        self._heartbeat_thread.start()

        deadline = time.monotonic() + timeout
        job_sent = False
        post_result_grace = 1.0  # laisser le worker recevoir un éventuel RESULT_ACK

        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)

        while time.monotonic() < deadline:
            if self._result_handled.is_set():
                # Grace period pour RESULT_ACK sortant
                grace_deadline = time.monotonic() + post_result_grace
                while time.monotonic() < grace_deadline:
                    remaining = int((grace_deadline - time.monotonic()) * 1000)
                    if remaining <= 0:
                        break
                    events = dict(poller.poll(min(remaining, 200)))
                    if self.socket in events:
                        raw = self.socket.recv_multipart()
                        routing_id = raw[0]
                        payload_frames = raw[1:]
                        log_transport_recv(log, routing_id, payload_frames, "broker")
                        self._process_message(routing_id, payload_frames)
                break

            remaining = int((deadline - time.monotonic()) * 1000)
            if remaining <= 0:
                break
            events = dict(poller.poll(min(remaining, 500)))
            if self.socket in events:
                raw = self.socket.recv_multipart()
                routing_id = raw[0]
                payload_frames = raw[1:]
                log_transport_recv(log, routing_id, payload_frames, "broker")
                self._process_message(routing_id, payload_frames)

            if auto_dispatch and not job_sent:
                for routing_id, worker in list(self._workers.items()):
                    if worker.state == WorkerState.READY:
                        self._dispatch_job(routing_id, worker)
                        job_sent = True
                        break

            self._expire_stale_workers()

        if not self.stats.raw_message_received:
            self.stats.result_missing = True

        return self.stats

    def run_forever(self) -> None:
        self.setup()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name="broker-heartbeat", daemon=True
        )
        self._heartbeat_thread.start()
        log.info("Broker running forever on %s", self.config.endpoint)
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)
        while not self._stop.is_set():
            events = dict(poller.poll(500))
            if self.socket in events:
                raw = self.socket.recv_multipart()
                routing_id = raw[0]
                payload_frames = raw[1:]
                log_transport_recv(log, routing_id, payload_frames, "broker")
                self._process_message(routing_id, payload_frames)
            self._expire_stale_workers()

    def shutdown(self) -> None:
        self._stop.set()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=1.0)
        if self._monitor:
            self._monitor.stop()
        try:
            self.socket.close(linger=0)
        except Exception:
            pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ZMQ reply-loss reproducer broker")
    parser.add_argument("--endpoint", default=DEFAULT_BROKER_ENDPOINT)
    parser.add_argument("--heartbeat-interval", type=float, default=None)
    parser.add_argument("--liveness-multiplier", type=float, default=None)
    parser.add_argument(
        "--result-policy",
        choices=["strict", "job-id"],
        default="strict",
    )
    parser.add_argument("--production", action="store_true")
    parser.add_argument("--no-monitor", action="store_true")
    parser.add_argument("--log-level", default=env_log_level())
    add_zmq_options_args(parser)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    global log
    log = setup_logging("broker", args.log_level)

    cfg = BrokerConfig(
        endpoint=args.endpoint,
        result_policy=args.result_policy,
        zmq_options=zmq_options_from_args(args),
        monitor_enabled=not args.no_monitor,
        log_level=args.log_level,
    )
    if args.production:
        apply_production_timings(cfg)
    if args.heartbeat_interval is not None:
        cfg.heartbeat_interval = args.heartbeat_interval
    if args.liveness_multiplier is not None:
        cfg.liveness_multiplier = args.liveness_multiplier

    log.info(
        "Broker config endpoint=%s heartbeat_interval=%.3f worker_timeout=%.3f "
        "result_policy=%s production=%s",
        cfg.endpoint,
        cfg.heartbeat_interval,
        cfg.worker_timeout,
        cfg.result_policy,
        args.production,
    )

    broker = Broker(cfg)
    try:
        broker.run_forever()
    except KeyboardInterrupt:
        log.info("Broker interrupted")
    finally:
        broker.shutdown()


if __name__ == "__main__":
    main()
