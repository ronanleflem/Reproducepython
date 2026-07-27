"""Protocole applicatif multipart pour broker ROUTER <-> worker DEALER."""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class MsgType(str, Enum):
    READY = "READY"
    READY_ACK = "READY_ACK"
    HEARTBEAT = "HEARTBEAT"
    JOB = "JOB"
    RESULT = "RESULT"
    RESULT_ACK = "RESULT_ACK"


class WorkerState(str, Enum):
    READY = "READY"
    BUSY = "BUSY"
    EXPIRED = "EXPIRED"
    REMOVED = "REMOVED"
    RECONNECTED = "RECONNECTED"
    UNKNOWN = "UNKNOWN"


@dataclass
class Message:
    msg_type: MsgType
    worker_id: str = ""
    session_id: str = ""
    socket_generation: int = 0
    job_id: str = ""
    timestamp: float = field(default_factory=time.time)
    heartbeat_counter: int = 0
    payload: dict[str, Any] = field(default_factory=dict)

    def to_frames(self) -> list[bytes]:
        body = {
            "worker_id": self.worker_id,
            "session_id": self.session_id,
            "socket_generation": self.socket_generation,
            "job_id": self.job_id,
            "timestamp": self.timestamp,
            "heartbeat_counter": self.heartbeat_counter,
            "payload": self.payload,
        }
        return [self.msg_type.value.encode("utf-8"), json.dumps(body).encode("utf-8")]

    @classmethod
    def from_frames(cls, frames: list[bytes]) -> Message:
        if len(frames) < 2:
            raise ValueError(f"Message requires at least 2 frames, got {len(frames)}")
        msg_type = MsgType(frames[0].decode("utf-8"))
        body = json.loads(frames[1].decode("utf-8"))
        return cls(
            msg_type=msg_type,
            worker_id=body.get("worker_id", ""),
            session_id=body.get("session_id", ""),
            socket_generation=body.get("socket_generation", 0),
            job_id=body.get("job_id", ""),
            timestamp=body.get("timestamp", time.time()),
            heartbeat_counter=body.get("heartbeat_counter", 0),
            payload=body.get("payload", {}),
        )


def new_session_id() -> str:
    return str(uuid.uuid4())


def frame_summary(frame: bytes, max_len: int = 200) -> str:
    try:
        text = frame.decode("utf-8")
        if len(text) > max_len:
            return f"{text[:max_len]}...({len(frame)}B)"
        return text
    except UnicodeDecodeError:
        return f"<binary {len(frame)}B: {frame[:32]!r}>"


def summarize_frames(frames: list[bytes]) -> list[str]:
    return [frame_summary(f) for f in frames]


def _correlation_suffix(
    *,
    job_id: str = "",
    worker_id: str = "",
    session_id: str = "",
    socket_generation: int = 0,
    routing_id: str | bytes | None = "",
) -> str:
    rid = ""
    if isinstance(routing_id, bytes):
        rid = routing_id.hex()
    elif routing_id:
        rid = str(routing_id)
    return (
        f"job_id={job_id or 'N/A'} worker_id={worker_id or 'N/A'} "
        f"session_id={session_id or 'N/A'} socket_generation={socket_generation} "
        f"routing_id={rid or 'N/A'}"
    )


def log_transport_recv(
    log: logging.Logger,
    routing_id: bytes | None,
    frames: list[bytes],
    component: str,
    *,
    job_id: str = "",
    worker_id: str = "",
    session_id: str = "",
    socket_generation: int = 0,
) -> None:
    corr = _correlation_suffix(
        job_id=job_id,
        worker_id=worker_id,
        session_id=session_id,
        socket_generation=socket_generation,
        routing_id=routing_id,
    )
    log.info(
        "TRANSPORT_RECV component=%s mono_ts=%.6f wall_ts=%.6f "
        "frame_count=%d frames=%s %s",
        component,
        time.monotonic(),
        time.time(),
        len(frames),
        summarize_frames(frames),
        corr,
    )


def log_send_result(
    log: logging.Logger,
    *,
    component: str,
    msg_type: str,
    success: bool,
    duration_ms: float,
    socket_generation: int,
    session_id: str,
    error: str | None = None,
    errno: int | None = None,
    job_id: str = "",
    worker_id: str = "",
    routing_id: str | bytes | None = "",
) -> None:
    corr = _correlation_suffix(
        job_id=job_id,
        worker_id=worker_id,
        session_id=session_id,
        socket_generation=socket_generation,
        routing_id=routing_id,
    )
    log.info(
        "SEND_RESULT component=%s msg_type=%s success=%s duration_ms=%.3f "
        "error=%s errno=%s %s",
        component,
        msg_type,
        success,
        duration_ms,
        error,
        errno,
        corr,
    )
