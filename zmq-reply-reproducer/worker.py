#!/usr/bin/env python3
"""Worker DEALER pour reproduire la perte de reply ZMQ."""

from __future__ import annotations

import argparse
import queue
import threading
import time
from dataclasses import dataclass
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
from protocol import (
    Message,
    MsgType,
    log_send_result,
    log_transport_recv,
    new_session_id,
)
from socket_monitor import SocketMonitor

log = setup_logging("worker", env_log_level())


@dataclass
class WorkerStats:
    send_succeeded: bool = False
    result_ack_received: bool = False
    worker_was_ready: bool = False


@dataclass
class JobRequest:
    job_id: str
    session_id: str
    socket_generation: int


class WorkerSocket:
    """Encapsule la socket DEALER avec reconnect léger/complet."""

    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        self.ctx = zmq.Context.instance()
        self.socket: zmq.Socket | None = None
        self.session_id = new_session_id()
        self.socket_generation = 0
        self._monitor: SocketMonitor | None = None
        self._identity = config.worker_id.encode("utf-8")

    def _create_socket(self) -> zmq.Socket:
        sock = self.ctx.socket(zmq.DEALER)
        opts = self.config.zmq_options
        opts.apply(sock, identity=self.config.worker_id)
        if self.config.monitor_enabled:
            monitor_ep = f"{DEFAULT_MONITOR_ENDPOINT}-{self.config.worker_id}"
            self._monitor = SocketMonitor("worker", monitor_ep, log)
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

    def reconnect_light(self) -> None:
        """disconnect/connect sur la même socket."""
        assert self.socket is not None
        log.info(
            "Reconnect LIGHT generation=%d session_id=%s (session unchanged)",
            self.socket_generation,
            self.session_id,
        )
        self.socket.disconnect(self.config.broker_endpoint)
        self.socket.connect(self.config.broker_endpoint)

    def reconnect_full(self) -> None:
        """Fermeture complète et nouvelle socket avec nouveau session_id."""
        old_gen = self.socket_generation
        if self._monitor:
            self._monitor.stop()
            self._monitor = None
        if self.socket:
            self.socket.close(linger=self.config.zmq_options.linger)
            self.socket = None
        self.socket_generation += 1
        self.session_id = new_session_id()
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
        log_transport_recv(log, None, frames, "worker")
        return Message.from_frames(frames)

    def shutdown(self) -> None:
        if self._monitor:
            self._monitor.stop()
        if self.socket:
            self.socket.close(linger=self.config.zmq_options.linger)
            self.socket = None


class Worker:
    def __init__(self, config: WorkerConfig) -> None:
        self.config = config
        self.sock = WorkerSocket(config)
        self.stats = WorkerStats()
        self._current_session_id = ""
        self._job_queue: queue.Queue[JobRequest | None] = queue.Queue()
        self._result_queue: queue.Queue[tuple[str, str]] = queue.Queue()
        self._stop = threading.Event()
        self._network_thread: threading.Thread | None = None
        self._job_thread: threading.Thread | None = None
        self._pending_job: JobRequest | None = None
        self._last_heartbeat_session: str | None = None

    def _classify_heartbeat_session(self, msg: Message) -> str:
        if msg.session_id == self.sock.session_id:
            return "current_session"
        if msg.session_id == self._current_session_id and self._current_session_id:
            return "old_session"
        return "unknown_session"

    def _handle_heartbeat(self, msg: Message) -> None:
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
                self._handle_heartbeat(msg)
                if expected_session and msg.session_id == expected_session:
                    return True
                if expected_session is None:
                    return True
        return False

    def _execute_job_blocking(self, job_id: str) -> None:
        log.info(
            "APP job_started job_id=%s duration=%.3fs threading=%s",
            job_id,
            self.config.job_duration,
            self.config.threading_mode,
        )
        time.sleep(self.config.job_duration)
        log.info("APP job_finished job_id=%s", job_id)

    def _send_result(self, job_id: str) -> bool:
        if self.config.reply_delay > 0:
            log.info("APP reply_delay sleeping %.3fs", self.config.reply_delay)
            time.sleep(self.config.reply_delay)
        result = Message(
            msg_type=MsgType.RESULT,
            worker_id=self.config.worker_id,
            session_id=self.sock.session_id,
            socket_generation=self.sock.socket_generation,
            job_id=job_id,
            timestamp=time.time(),
            payload={"strategy": self.config.strategy},
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

        if strategy == "no-reconnect":
            return self._send_result(job_id)

        if strategy == "auto-reconnect-only":
            if self.config.reconnect_delay > 0:
                time.sleep(self.config.reconnect_delay)
            return self._send_result(job_id)

        if strategy == "manual-reconnect-immediate":
            self.sock.reconnect_socket()
            return self._send_result(job_id)

        if strategy == "manual-reconnect-heartbeat":
            self.sock.reconnect_socket()
            if not self._wait_for_heartbeat(
                expected_session=self.sock.session_id, timeout=3.0
            ):
                log.warning("APP heartbeat_wait_timeout after reconnect")
            return self._send_result(job_id)

        if strategy == "manual-reconnect-ready-ack":
            self.sock.reconnect_full()
            if not self._send_ready_and_wait_ack():
                log.warning("APP ready_ack_timeout after reconnect")
            return self._send_result(job_id)

        if strategy == "reconnect-then-delay":
            self.sock.reconnect_socket()
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
                self._handle_heartbeat(msg)
                continue
            if msg.msg_type == MsgType.RESULT_ACK and msg.job_id == job_id:
                log.info("APP result_ack_received job_id=%s", job_id)
                self.stats.result_ack_received = True
                return True
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
        log.info("APP job_received job_id=%s session_id=%s", job_id, msg.session_id)

        self._execute_job_blocking(job_id)
        self._apply_post_job_strategy(job_id)
        self._wait_result_ack(job_id, timeout=self.config.result_ack_timeout)
        return self.stats

    def _network_loop_separate(self) -> None:
        """Thread réseau unique : seule thread touchant la socket ZMQ."""
        self.sock.connect()
        if not self._send_ready_and_wait_ack():
            log.error("APP initial_ready_ack_failed")
            self._stop.set()
            return

        poller = zmq.Poller()
        assert self.sock.socket is not None
        poller.register(self.sock.socket, zmq.POLLIN)

        while not self._stop.is_set():
            # Envoyer résultats en attente
            try:
                job_id, _ = self._result_queue.get_nowait()
                self._send_result(job_id)
            except queue.Empty:
                pass

            events = dict(poller.poll(200))
            if self.sock.socket not in events:
                continue

            msg = self.sock.recv_message(timeout_ms=0)
            if msg is None:
                continue

            if msg.msg_type == MsgType.JOB:
                log.info("APP job_received job_id=%s", msg.job_id)
                self._job_queue.put(
                    JobRequest(
                        job_id=msg.job_id,
                        session_id=msg.session_id,
                        socket_generation=msg.socket_generation,
                    )
                )
            elif msg.msg_type == MsgType.HEARTBEAT:
                self._handle_heartbeat(msg)
            elif msg.msg_type == MsgType.RESULT_ACK:
                log.info("APP result_ack_received job_id=%s", msg.job_id)
                self.stats.result_ack_received = True
            elif msg.msg_type == MsgType.READY_ACK:
                if msg.session_id == self.sock.session_id:
                    self.stats.worker_was_ready = True

    def _job_loop_separate(self) -> None:
        """Thread job : sleep bloquant, ne touche jamais la socket."""
        job = self._job_queue.get()
        if job is None:
            return
        self._execute_job_blocking(job.job_id)

        strategy = self.config.strategy
        if strategy in ("no-reconnect", "auto-reconnect-only"):
            self._result_queue.put((job.job_id, strategy))
        elif strategy == "manual-reconnect-immediate":
            # Reconnect must happen on network thread - signal via queue
            self._result_queue.put((job.job_id, "manual-reconnect-immediate"))
        elif strategy == "manual-reconnect-heartbeat":
            self._result_queue.put((job.job_id, "manual-reconnect-heartbeat"))
        elif strategy == "manual-reconnect-ready-ack":
            self._result_queue.put((job.job_id, "manual-reconnect-ready-ack"))
        elif strategy == "reconnect-then-delay":
            self._result_queue.put((job.job_id, "reconnect-then-delay"))
        else:
            self._result_queue.put((job.job_id, strategy))

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
        return self.stats

    def _network_loop_separate_with_strategy(self) -> None:
        """Variante réseau qui gère aussi les reconnect selon stratégie."""
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
                        threading.Thread(
                            target=self._job_worker_separate,
                            args=(msg.job_id,),
                            daemon=True,
                        ).start()
                    elif msg.msg_type == MsgType.HEARTBEAT:
                        self._handle_heartbeat(msg)
                    elif msg.msg_type == MsgType.RESULT_ACK:
                        self.stats.result_ack_received = True

    def _job_worker_separate(self, job_id: str) -> None:
        self._execute_job_blocking(job_id)
        self._result_queue.put((job_id, self.config.strategy))

    def _apply_post_job_strategy_on_network(self, job_id: str, strategy: str) -> None:
        saved = self.config.strategy
        self.config.strategy = strategy  # type: ignore[assignment]
        self._apply_post_job_strategy(job_id)
        self.config.strategy = saved

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
        "Worker finished send_succeeded=%s result_ack=%s",
        stats.send_succeeded,
        stats.result_ack_received,
    )


if __name__ == "__main__":
    main()
