"""Thread de monitoring ZeroMQ (ne touche pas la socket métier)."""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, Callable

import zmq

if TYPE_CHECKING:
    from timeline import TimelineCollector
    from zmq import Socket

EVENT_NAMES = {
    zmq.EVENT_CONNECTED: "EVENT_CONNECTED",
    zmq.EVENT_CONNECT_DELAYED: "EVENT_CONNECT_DELAYED",
    zmq.EVENT_CONNECT_RETRIED: "EVENT_CONNECT_RETRIED",
    zmq.EVENT_LISTENING: "EVENT_LISTENING",
    zmq.EVENT_BIND_FAILED: "EVENT_BIND_FAILED",
    zmq.EVENT_ACCEPTED: "EVENT_ACCEPTED",
    zmq.EVENT_ACCEPT_FAILED: "EVENT_ACCEPT_FAILED",
    zmq.EVENT_CLOSED: "EVENT_CLOSED",
    zmq.EVENT_CLOSE_FAILED: "EVENT_CLOSE_FAILED",
    zmq.EVENT_DISCONNECTED: "EVENT_DISCONNECTED",
    zmq.EVENT_MONITOR_STOPPED: "EVENT_MONITOR_STOPPED",
    zmq.EVENT_HANDSHAKE_FAILED_NO_DETAIL: "EVENT_HANDSHAKE_FAILED_NO_DETAIL",
    zmq.EVENT_HANDSHAKE_SUCCEEDED: "EVENT_HANDSHAKE_SUCCEEDED",
    zmq.EVENT_HANDSHAKE_FAILED_PROTOCOL: "EVENT_HANDSHAKE_FAILED_PROTOCOL",
    zmq.EVENT_HANDSHAKE_FAILED_AUTH: "EVENT_HANDSHAKE_FAILED_AUTH",
}

# Événements ciblés pour l'investigation reconnexion
RECONNECT_EVENTS = frozenset(
    {
        "EVENT_CONNECTED",
        "EVENT_CONNECT_DELAYED",
        "EVENT_CONNECT_RETRIED",
        "EVENT_DISCONNECTED",
        "EVENT_HANDSHAKE_SUCCEEDED",
        "EVENT_HANDSHAKE_FAILED",
        "EVENT_HANDSHAKE_FAILED_NO_DETAIL",
        "EVENT_HANDSHAKE_FAILED_PROTOCOL",
        "EVENT_HANDSHAKE_FAILED_AUTH",
        "EVENT_CLOSED",
    }
)

# Compatibilité pyzmq versions
if hasattr(zmq, "EVENT_HANDSHAKE_FAILED"):
    EVENT_NAMES[zmq.EVENT_HANDSHAKE_FAILED] = "EVENT_HANDSHAKE_FAILED"


class SocketMonitor:
    """Écoute les événements ZMQ sur une socket monitor dédiée."""

    def __init__(
        self,
        component: str,
        monitor_endpoint: str,
        log: logging.Logger,
        *,
        timeline: TimelineCollector | None = None,
        generation_getter: Callable[[], int] | None = None,
        worker_id: str = "",
        session_id_getter: Callable[[], str] | None = None,
    ) -> None:
        self.component = component
        self.monitor_endpoint = monitor_endpoint
        self.log = log
        self.timeline = timeline
        self._generation_getter = generation_getter or (lambda: 0)
        self._session_id_getter = session_id_getter or (lambda: "")
        self.worker_id = worker_id
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._ctx: zmq.Context | None = None
        self._mon_socket: Socket | None = None
        self.event_count = 0
        self.reconnect_event_count = 0

    def attach(self, socket: Socket) -> None:
        socket.monitor(self.monitor_endpoint, zmq.EVENT_ALL)

    def start(self) -> None:
        self._stop.clear()
        self._ctx = zmq.Context.instance()
        self._mon_socket = self._ctx.socket(zmq.PAIR)
        self._mon_socket.connect(self.monitor_endpoint)
        self._thread = threading.Thread(
            target=self._run,
            name=f"monitor-{self.component}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        if self._mon_socket:
            self._mon_socket.close(linger=0)
            self._mon_socket = None

    def _run(self) -> None:
        assert self._mon_socket is not None
        poller = zmq.Poller()
        poller.register(self._mon_socket, zmq.POLLIN)
        while not self._stop.is_set():
            events = dict(poller.poll(200))
            if self._mon_socket not in events:
                continue
            try:
                msg = self._mon_socket.recv_multipart(zmq.NOBLOCK)
            except zmq.Again:
                continue
            if len(msg) < 2:
                continue
            event_id = int.from_bytes(msg[0], "little")
            event_value = int.from_bytes(msg[1], "little")
            endpoint = msg[2].decode("utf-8", errors="replace") if len(msg) > 2 else ""
            name = EVENT_NAMES.get(event_id, f"EVENT_{event_id}")
            gen = self._generation_getter()
            session_id = self._session_id_getter()
            mono = time.monotonic()
            wall = time.time()
            self.event_count += 1
            if name in RECONNECT_EVENTS:
                self.reconnect_event_count += 1

            self.log.info(
                "SOCKET_MONITOR component=%s event=%s value=%d endpoint=%s "
                "mono_ts=%.6f wall_ts=%.6f socket_generation=%d "
                "job_id=N/A worker_id=%s session_id=%s routing_id=N/A",
                self.component,
                name,
                event_value,
                endpoint,
                mono,
                wall,
                gen,
                self.worker_id or "N/A",
                session_id or "N/A",
            )
            if self.timeline:
                self.timeline.emit(
                    name,
                    component=self.component,
                    worker_id=self.worker_id,
                    session_id=session_id,
                    socket_generation=gen,
                    details={"endpoint": endpoint, "value": event_value},
                )
