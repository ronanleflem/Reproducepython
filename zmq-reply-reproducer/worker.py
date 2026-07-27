#!/usr/bin/env python3
"""Worker DEALER pour reproduire la perte de reply ZMQ."""

from __future__ import annotations

import argparse
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import zmq

from config import (
    DEFAULT_BROKER_ENDPOINT,
    DEFAULT_MONITOR_ENDPOINT,
    JOB_DURATION,
    WorkerConfig,
    add_zmq_options_args,
    apply_production_timings,
    env_log_level,
    zmq_options_from_args,
)
from logging_setup import setup_logging
from payload_utils import make_result_payload
from protocol import (
    Message,
    MsgType,
    log_send_result,
    log_transport_recv,
    new_session_id,
)
from socket_monitor import SocketMonitor
from timeline import SessionValidationMode, TimelineCollector, TransportCase

log = setup_logging("worker", env_log_level())


@dataclass
class WorkerStats:
    send_succeeded: bool = False
    result_ack_received: bool = False
    worker_was_ready: bool = False
    session_validated_heartbeat: bool = False
    session_validated_ready_ack: bool = False
    session_validated_result_ack: bool = False
    reconnect_events: int = 0
    manual_reconnect_performed: bool = False
    transport_case: TransportCase = TransportCase.INCONCLUSIVE
    job_id: str = ""


@dataclass
class JobRequest:
    job_id: str
    session_id: str
    socket_generation: int


class WorkerSocket:
    """Encapsule la socket DEALER avec reconnect léger/complet."""

    def __init__(
        self,
        config: WorkerConfig,
        timeline: TimelineCollector | None = None,
    ) -> None:
        self.config = config
        self.timeline = timeline
        self.ctx = zmq.Context.instance()
        self.socket: zmq.Socket | None = None
        self.session_id = new_session_id()
        self.socket_generation = 0
        self._monitor: SocketMonitor | None = None
        self._identity = config.worker_id.encode("utf-8")
        self.manual_reconnect_count = 0
        self.auto_reconnect_detected = False

    def _create_socket(self) -> zmq.Socket:
        sock = self.ctx.socket(zmq.DEALER)
        opts = self.config.zmq_options
        opts.apply(sock, identity=self.config.worker_id)
        if self.config.monitor_enabled:
            monitor_ep = f"{DEFAULT_MONITOR_ENDPOINT}-{self.config.worker_id}-{id(self)}"
            self._monitor = SocketMonitor(
                "worker",
                monitor_ep,
                log,
                timeline=self.timeline,
                generation_getter=lambda: self.socket_generation,
                worker_id=self.config.worker_id,
                session_id_getter=lambda: self.session_id,
            )
            self._monitor.attach(sock)
        return sock

    def _log_effective_options(self) -> None:
        assert self.socket is not None
        s = self.socket
        log.info(
            "ZMQ_OPTIONS component=worker generation=%d session_id=%s "
            "IMMEDIATE=%s LINGER=%s RECONNECT_IVL=%s RECONNECT_IVL_MAX=%s "
            "SNDTIMEO=%s IDENTITY=%s",
            self.socket_generation,
            self.session_id,
            s.get(zmq.IMMEDIATE),
            s.get(zmq.LINGER),
            s.get(zmq.RECONNECT_IVL),
            s.get(zmq.RECONNECT_IVL_MAX),
            s.get(zmq.SNDTIMEO),
            s.get(zmq.IDENTITY).decode("utf-8", errors="replace"),
        )

    def connect(self) -> None:
        self.socket = self._create_socket()
        self.socket.connect(self.config.broker_endpoint)
        self._log_effective_options()
        if self._monitor:
            self._monitor.start()
        log.info(
            "Socket connected generation=%d session_id=%s endpoint=%s",
            self.socket_generation,
            self.session_id,
            self.config.broker_endpoint,
        )
        if self.timeline:
            self.timeline.emit(
                "SOCKET_CONNECTED",
                component="worker",
                worker_id=self.config.worker_id,
                session_id=self.session_id,
                socket_generation=self.socket_generation,
            )

    def reconnect_light(self) -> None:
        """disconnect/connect sur la même socket."""
        assert self.socket is not None
        self.manual_reconnect_count += 1
        log.info(
            "Reconnect LIGHT generation=%d session_id=%s (session unchanged)",
            self.socket_generation,
            self.session_id,
        )
        if self.timeline:
            self.timeline.emit(
                "MANUAL_RECONNECT_LIGHT",
                component="worker",
                worker_id=self.config.worker_id,
                session_id=self.session_id,
                socket_generation=self.socket_generation,
                details={"attempt": self.manual_reconnect_count},
            )
        self.socket.disconnect(self.config.broker_endpoint)
        self.socket.connect(self.config.broker_endpoint)

    def reconnect_full(self) -> None:
        """Fermeture complète et nouvelle socket avec nouveau session_id."""
        old_gen = self.socket_generation
        old_session = self.session_id
        if self._monitor:
            self._monitor.stop()
            self._monitor = None
        if self.socket:
            self.socket.close(linger=self.config.zmq_options.linger)
            self.socket = None
        self.socket_generation += 1
        self.session_id = new_session_id()
        self.manual_reconnect_count += 1
        self.socket = self._create_socket()
        self.socket.connect(self.config.broker_endpoint)
        self._log_effective_options()
        if self._monitor:
            self._monitor.start()
        log.info(
            "Reconnect FULL old_generation=%d new_generation=%d "
            "new_session_id=%s",
            old_gen,
            self.socket_generation,
            self.session_id,
        )
        if self.timeline:
            self.timeline.emit(
                "MANUAL_RECONNECT_FULL",
                component="worker",
                worker_id=self.config.worker_id,
                session_id=self.session_id,
                socket_generation=self.socket_generation,
                details={
                    "old_generation": old_gen,
                    "old_session_id": old_session,
                    "attempt": self.manual_reconnect_count,
                },
            )

    def reconnect_socket(self) -> None:
        if self.config.reconnect_mode == "light":
            self.reconnect_light()
        else:
            self.reconnect_full()

    def send_message(self, msg: Message) -> bool:
        assert self.socket is not None
        frames = msg.to_frames()
        flags = 0
        if self.config.send_mode == "dontwait":
            flags = zmq.DONTWAIT
        start = time.monotonic()
        error: str | None = None
        errno_val: int | None = None
        success = False
        try:
            self.socket.send_multipart(frames, flags=flags)
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
            component="worker",
            msg_type=msg.msg_type.value,
            success=success,
            duration_ms=duration_ms,
            socket_generation=self.socket_generation,
            session_id=self.session_id,
            error=error,
            errno=errno_val,
            job_id=msg.job_id,
            worker_id=msg.worker_id,
            payload_bytes=msg.payload_bytes,
        )
        if msg.msg_type == MsgType.RESULT:
            event = "RESULT_SENT" if success else "RESULT_SEND_FAILED"
            if self.timeline:
                self.timeline.emit(
                    event,
                    component="worker",
                    worker_id=msg.worker_id,
                    session_id=self.session_id,
                    socket_generation=self.socket_generation,
                    job_id=msg.job_id,
                    details={
                        "duration_ms": duration_ms,
                        "error": error,
                        "payload_bytes": msg.payload_bytes,
                    },
                )
        elif msg.msg_type == MsgType.READY and self.timeline:
            self.timeline.emit(
                "READY_SENT",
                component="worker",
                worker_id=msg.worker_id,
                session_id=self.session_id,
                socket_generation=self.socket_generation,
            )
        return success

    def recv_message(self, timeout_ms: int = 5000) -> Message | None:
        assert self.socket is not None
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)
        events = dict(poller.poll(timeout_ms))
        if self.socket not in events:
            return None
        frames = self.socket.recv_multipart()
        msg = Message.from_frames(frames)
        log_transport_recv(
            log,
            None,
            frames,
            "worker",
            worker_id=self.config.worker_id,
            session_id=self.session_id,
            socket_generation=self.socket_generation,
            job_id=msg.job_id,
            payload_bytes=msg.payload_bytes,
        )
        return msg

    @property
    def reconnect_events(self) -> int:
        return self._monitor.reconnect_event_count if self._monitor else 0

    def shutdown(self) -> None:
        if self._monitor:
            self._monitor.stop()
        if self.socket:
            self.socket.close(linger=self.config.zmq_options.linger)
            self.socket = None


class Worker:
    def __init__(
        self,
        config: WorkerConfig,
        timeline: TimelineCollector | None = None,
        run_id: str = "",
    ) -> None:
        self.config = config
        self.timeline = timeline or TimelineCollector(run_id=run_id, enabled=True)
        self.sock = WorkerSocket(config, timeline=self.timeline)
        self.stats = WorkerStats()
        self._current_session_id = ""
        self._job_queue: queue.Queue[JobRequest | None] = queue.Queue()
        self._result_queue: queue.Queue[tuple[str, str]] = queue.Queue()
        self._stop = threading.Event()
        self._network_thread: threading.Thread | None = None
        self._job_thread: threading.Thread | None = None
        self._pending_job: JobRequest | None = None
        self._last_heartbeat_session: str | None = None
        self._current_job_id = ""

    def _classify_heartbeat_session(self, msg: Message) -> str:
        if msg.session_id == self.sock.session_id:
            return "current_session"
        if msg.session_id == self._current_session_id and self._current_session_id:
            return "old_session"
        return "unknown_session"

    def _handle_heartbeat(self, msg: Message, job_id: str = "") -> None:
        classification = self._classify_heartbeat_session(msg)
        log.info(
            "APP heartbeat_received counter=%d session_id=%s "
            "current_session=%s classification=%s socket_generation=%d",
            msg.heartbeat_counter,
            msg.session_id,
            self.sock.session_id,
            classification,
            self.sock.socket_generation,
        )
        self._last_heartbeat_session = msg.session_id
        if self.timeline:
            self.timeline.emit(
                "HEARTBEAT_RECEIVED",
                component="worker",
                worker_id=self.config.worker_id,
                session_id=msg.session_id,
                socket_generation=self.sock.socket_generation,
                job_id=job_id or self._current_job_id,
                details={
                    "classification": classification,
                    "heartbeat_counter": msg.heartbeat_counter,
                    "reliable_session_indicator": classification == "current_session",
                },
            )
        if classification == "current_session":
            self.stats.session_validated_heartbeat = True

    def _send_ready_and_wait_ack(self, timeout: float = 3.0) -> bool:
        ready = Message(
            msg_type=MsgType.READY,
            worker_id=self.config.worker_id,
            session_id=self.sock.session_id,
            socket_generation=self.sock.socket_generation,
            timestamp=time.time(),
        )
        if not self.sock.send_message(ready):
            return False
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            msg = self.sock.recv_message(timeout_ms=500)
            if msg is None:
                continue
            if msg.msg_type == MsgType.READY_ACK:
                if msg.session_id == self.sock.session_id:
                    log.info(
                        "APP ready_ack_received session_id=%s generation=%d",
                        msg.session_id,
                        self.sock.socket_generation,
                    )
                    self._current_session_id = msg.session_id
                    self.stats.worker_was_ready = True
                    self.stats.session_validated_ready_ack = True
                    if self.timeline:
                        self.timeline.emit(
                            "READY_ACK_RECEIVED",
                            component="worker",
                            worker_id=self.config.worker_id,
                            session_id=msg.session_id,
                            socket_generation=self.sock.socket_generation,
                        )
                    return True
                log.warning(
                    "APP ready_ack_wrong_session expected=%s got=%s",
                    self.sock.session_id,
                    msg.session_id,
                )
            elif msg.msg_type == MsgType.HEARTBEAT:
                self._handle_heartbeat(msg)
        return False

    def _wait_for_heartbeat(
        self, expected_session: str | None = None, timeout: float = 3.0
    ) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            msg = self.sock.recv_message(timeout_ms=500)
            if msg is None:
                continue
            if msg.msg_type == MsgType.HEARTBEAT:
                self._handle_heartbeat(msg, job_id=self._current_job_id)
                if expected_session and msg.session_id == expected_session:
                    return True
                if expected_session is None:
                    return True
        if self.timeline:
            self.timeline.emit(
                "HEARTBEAT_WAIT_TIMEOUT",
                component="worker",
                worker_id=self.config.worker_id,
                session_id=self.sock.session_id,
                socket_generation=self.sock.socket_generation,
                job_id=self._current_job_id,
            )
        return False

    def _execute_job_blocking(self, job_id: str) -> None:
        log.info(
            "APP job_started job_id=%s duration=%.3fs threading=%s",
            job_id,
            self.config.job_duration,
            self.config.threading_mode,
        )
        if self.timeline:
            self.timeline.emit(
                "JOB_PROCESSING_STARTED",
                component="worker",
                worker_id=self.config.worker_id,
                session_id=self.sock.session_id,
                socket_generation=self.sock.socket_generation,
                job_id=job_id,
            )
        if self.config.network_delay > 0:
            time.sleep(self.config.network_delay)
        time.sleep(self.config.job_duration)
        log.info("APP job_finished job_id=%s", job_id)
        if self.timeline:
            self.timeline.emit(
                "JOB_PROCESSING_FINISHED",
                component="worker",
                worker_id=self.config.worker_id,
                session_id=self.sock.session_id,
                socket_generation=self.sock.socket_generation,
                job_id=job_id,
            )

    def _send_result(self, job_id: str) -> bool:
        if self.config.reply_delay > 0:
            log.info("APP reply_delay sleeping %.3fs", self.config.reply_delay)
            time.sleep(self.config.reply_delay)
        payload_bytes = self.config.result_payload_bytes
        binary_data = b""
        if payload_bytes > 0:
            binary_data = make_result_payload(payload_bytes, self.config.payload_seed)
            log.info(
                "APP result_payload_prepared job_id=%s payload_bytes=%d seed=%d",
                job_id,
                payload_bytes,
                self.config.payload_seed,
            )
        result = Message(
            msg_type=MsgType.RESULT,
            worker_id=self.config.worker_id,
            session_id=self.sock.session_id,
            socket_generation=self.sock.socket_generation,
            job_id=job_id,
            timestamp=time.time(),
            payload={"strategy": self.config.strategy},
            binary_data=binary_data,
        )
        ok = self.sock.send_message(result)
        self.stats.send_succeeded = ok
        return ok

    def _apply_post_job_strategy(self, job_id: str) -> bool:
        strategy = self.config.strategy
        log.info(
            "APP applying_strategy strategy=%s reconnect_mode=%s job_id=%s",
            strategy,
            self.config.reconnect_mode,
            job_id,
        )
        if self.timeline:
            self.timeline.emit(
                "POST_JOB_STRATEGY_START",
                component="worker",
                worker_id=self.config.worker_id,
                session_id=self.sock.session_id,
                socket_generation=self.sock.socket_generation,
                job_id=job_id,
                details={"strategy": strategy, "reconnect_mode": self.config.reconnect_mode},
            )

        if strategy == "no-reconnect":
            return self._send_result(job_id)

        if strategy == "auto-reconnect-only":
            if self.config.reconnect_delay > 0:
                time.sleep(self.config.reconnect_delay)
            if self.timeline:
                self.timeline.emit(
                    "AUTO_RECONNECT_ONLY",
                    component="worker",
                    worker_id=self.config.worker_id,
                    session_id=self.sock.session_id,
                    socket_generation=self.sock.socket_generation,
                    job_id=job_id,
                    details={"manual_reconnect": False},
                )
            return self._send_result(job_id)

        if strategy == "manual-reconnect-immediate":
            self.sock.reconnect_socket()
            self.stats.manual_reconnect_performed = True
            return self._send_result(job_id)

        if strategy == "manual-reconnect-heartbeat":
            self.sock.reconnect_socket()
            self.stats.manual_reconnect_performed = True
            if not self._wait_for_heartbeat(
                expected_session=self.sock.session_id, timeout=3.0
            ):
                log.warning("APP heartbeat_wait_timeout after reconnect")
            return self._send_result(job_id)

        if strategy == "manual-reconnect-ready-ack":
            self.sock.reconnect_full()
            self.stats.manual_reconnect_performed = True
            if not self._send_ready_and_wait_ack():
                log.warning("APP ready_ack_timeout after reconnect")
            return self._send_result(job_id)

        if strategy == "reconnect-then-delay":
            self.sock.reconnect_socket()
            self.stats.manual_reconnect_performed = True
            if self.config.reconnect_delay > 0:
                time.sleep(self.config.reconnect_delay)
            return self._send_result(job_id)

        log.error("APP unknown_strategy %s", strategy)
        return False

    def _wait_result_ack(self, job_id: str, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            msg = self.sock.recv_message(timeout_ms=500)
            if msg is None:
                continue
            if msg.msg_type == MsgType.HEARTBEAT:
                self._handle_heartbeat(msg, job_id=job_id)
                continue
            if msg.msg_type == MsgType.RESULT_ACK and msg.job_id == job_id:
                log.info("APP result_ack_received job_id=%s", job_id)
                self.stats.result_ack_received = True
                self.stats.session_validated_result_ack = True
                if self.timeline:
                    self.timeline.emit(
                        "RESULT_ACK_RECEIVED",
                        component="worker",
                        worker_id=self.config.worker_id,
                        session_id=msg.session_id,
                        socket_generation=self.sock.socket_generation,
                        job_id=job_id,
                    )
                return True
        if self.timeline:
            self.timeline.emit(
                "RESULT_ACK_TIMEOUT",
                component="worker",
                worker_id=self.config.worker_id,
                session_id=self.sock.session_id,
                socket_generation=self.sock.socket_generation,
                job_id=job_id,
            )
        return False

    def _run_blocking_network_loop(self) -> WorkerStats:
        self.sock.connect()
        if not self._send_ready_and_wait_ack():
            log.error("APP initial_ready_ack_failed")
            return self.stats

        msg = self.sock.recv_message(timeout_ms=30000)
        if msg is None or msg.msg_type != MsgType.JOB:
            log.error("APP no_job_received type=%s", msg.msg_type if msg else None)
            return self.stats

        job_id = msg.job_id
        self._current_job_id = job_id
        self.stats.job_id = job_id
        log.info("APP job_received job_id=%s session_id=%s", job_id, msg.session_id)
        if self.timeline:
            self.timeline.emit(
                "JOB_RECEIVED",
                component="worker",
                worker_id=self.config.worker_id,
                session_id=msg.session_id,
                socket_generation=msg.socket_generation,
                job_id=job_id,
            )

        self._execute_job_blocking(job_id)
        self._apply_post_job_strategy(job_id)
        self._wait_result_ack(job_id, timeout=self.config.result_ack_timeout)
        self.stats.reconnect_events = self.sock.reconnect_events
        return self.stats

    def _network_loop_separate_with_strategy(self) -> None:
        self.sock.connect()
        if not self._send_ready_and_wait_ack():
            self._stop.set()
            return

        poller = zmq.Poller()
        assert self.sock.socket is not None
        poller.register(self.sock.socket, zmq.POLLIN)
        job_received = False
        pending_result: tuple[str, str] | None = None

        while not self._stop.is_set():
            if pending_result is None:
                try:
                    pending_result = self._result_queue.get_nowait()
                except queue.Empty:
                    pass

            if pending_result is not None:
                job_id, strat = pending_result
                self._apply_post_job_strategy_on_network(job_id, strat)
                pending_result = None

            events = dict(poller.poll(200))
            if self.sock.socket in events:
                msg = self.sock.recv_message(timeout_ms=0)
                if msg:
                    if msg.msg_type == MsgType.JOB and not job_received:
                        log.info("APP job_received job_id=%s", msg.job_id)
                        job_received = True
                        self._current_job_id = msg.job_id
                        self.stats.job_id = msg.job_id
                        if self.timeline:
                            self.timeline.emit(
                                "JOB_RECEIVED",
                                component="worker",
                                worker_id=self.config.worker_id,
                                session_id=msg.session_id,
                                socket_generation=msg.socket_generation,
                                job_id=msg.job_id,
                            )
                        threading.Thread(
                            target=self._job_worker_separate,
                            args=(msg.job_id,),
                            daemon=True,
                        ).start()
                    elif msg.msg_type == MsgType.HEARTBEAT:
                        self._handle_heartbeat(msg)
                    elif msg.msg_type == MsgType.RESULT_ACK:
                        self.stats.result_ack_received = True
                        self.stats.session_validated_result_ack = True

    def _job_worker_separate(self, job_id: str) -> None:
        self._execute_job_blocking(job_id)
        self._result_queue.put((job_id, self.config.strategy))

    def _apply_post_job_strategy_on_network(self, job_id: str, strategy: str) -> None:
        saved = self.config.strategy
        self.config.strategy = strategy  # type: ignore[assignment]
        self._apply_post_job_strategy(job_id)
        self.config.strategy = saved

    def _run_separate_job_thread(self) -> WorkerStats:
        self._network_thread = threading.Thread(
            target=self._network_loop_separate_with_strategy,
            name="worker-network",
            daemon=True,
        )
        self._network_thread.start()

        deadline = time.monotonic() + 60.0
        while time.monotonic() < deadline and not self.stats.send_succeeded:
            time.sleep(0.1)

        if self._network_thread.is_alive():
            self._stop.set()
            self._network_thread.join(timeout=5.0)
        self.stats.reconnect_events = self.sock.reconnect_events
        return self.stats

    def run(self) -> WorkerStats:
        global log
        log = setup_logging("worker", self.config.log_level)
        log.info(
            "Worker starting id=%s strategy=%s reconnect_mode=%s "
            "threading=%s job_duration=%.3f worker_timeout=%.3f",
            self.config.worker_id,
            self.config.strategy,
            self.config.reconnect_mode,
            self.config.threading_mode,
            self.config.job_duration,
            self.config.worker_timeout,
        )
        try:
            if self.config.threading_mode == "separate-job-thread":
                return self._run_separate_job_thread()
            return self._run_blocking_network_loop()
        finally:
            self.sock.shutdown()

    def session_validation_mode(self) -> SessionValidationMode:
        if self.config.strategy == "manual-reconnect-heartbeat":
            return SessionValidationMode.HEARTBEAT
        if self.config.strategy == "manual-reconnect-ready-ack":
            return SessionValidationMode.READY_ACK
        return SessionValidationMode.RESULT_ACK


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ZMQ reply-loss reproducer worker")
    parser.add_argument("--broker", default=DEFAULT_BROKER_ENDPOINT)
    parser.add_argument("--worker-id", default="worker-1")
    parser.add_argument(
        "--strategy",
        choices=[
            "no-reconnect",
            "manual-reconnect-immediate",
            "manual-reconnect-heartbeat",
            "manual-reconnect-ready-ack",
            "auto-reconnect-only",
            "reconnect-then-delay",
        ],
        default="no-reconnect",
    )
    parser.add_argument("--reconnect-mode", choices=["light", "full"], default="full")
    parser.add_argument(
        "--threading-mode",
        choices=["blocking-network-loop", "separate-job-thread"],
        default="blocking-network-loop",
    )
    parser.add_argument("--send-mode", choices=["blocking", "dontwait"], default="blocking")
    parser.add_argument("--job-duration", type=float, default=None)
    parser.add_argument("--heartbeat-interval", type=float, default=None)
    parser.add_argument("--liveness-multiplier", type=float, default=None)
    parser.add_argument("--reconnect-delay", type=float, default=0.0)
    parser.add_argument("--reply-delay", type=float, default=0.0)
    parser.add_argument("--network-delay", type=float, default=0.0)
    parser.add_argument(
        "--result-payload-bytes",
        type=int,
        default=0,
        help="Taille du blob binaire RESULT (0 = message léger)",
    )
    parser.add_argument(
        "--payload-seed",
        type=int,
        default=0,
        help="Seed pour générer un payload binaire reproductible",
    )
    parser.add_argument("--result-ack-timeout", type=float, default=2.0)
    parser.add_argument("--production", action="store_true")
    parser.add_argument("--no-monitor", action="store_true")
    parser.add_argument("--log-level", default=env_log_level())
    add_zmq_options_args(parser)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    global log
    log = setup_logging("worker", args.log_level)

    cfg = WorkerConfig(
        broker_endpoint=args.broker,
        worker_id=args.worker_id,
        strategy=args.strategy,
        reconnect_mode=args.reconnect_mode,
        threading_mode=args.threading_mode,
        send_mode=args.send_mode,
        reconnect_delay=args.reconnect_delay,
        reply_delay=args.reply_delay,
        network_delay=args.network_delay,
        result_payload_bytes=args.result_payload_bytes,
        payload_seed=args.payload_seed,
        result_ack_timeout=args.result_ack_timeout,
        zmq_options=zmq_options_from_args(args),
        monitor_enabled=not args.no_monitor,
        log_level=args.log_level,
    )
    if args.production:
        apply_production_timings(cfg)
    if args.job_duration is not None:
        cfg.job_duration = args.job_duration
    if args.heartbeat_interval is not None:
        cfg.heartbeat_interval = args.heartbeat_interval
    if args.liveness_multiplier is not None:
        cfg.liveness_multiplier = args.liveness_multiplier

    worker = Worker(cfg)
    stats = worker.run()
    log.info(
        "Worker finished send_succeeded=%s result_ack=%s "
        "session_hb=%s session_ready=%s session_result_ack=%s",
        stats.send_succeeded,
        stats.result_ack_received,
        stats.session_validated_heartbeat,
        stats.session_validated_ready_ack,
        stats.session_validated_result_ack,
    )


if __name__ == "__main__":
    main()
