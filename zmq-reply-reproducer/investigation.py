"""Utilitaires d'investigation : routing, états, session."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from protocol import WorkerState

if TYPE_CHECKING:
    from timeline import TimelineCollector

log = logging.getLogger(__name__)


@dataclass
class RoutingVerification:
    routing_id_received: str
    worker_found: bool
    session_found: bool
    job_found: bool
    worker_state: str
    worker_id: str = ""
    session_id: str = ""
    job_id: str = ""
    expected_routing_id: str = ""
    expected_session_id: str = ""
    expected_job_id: str = ""
    inconsistencies: list[str] | None = None

    def has_inconsistency(self) -> bool:
        return bool(self.inconsistencies)


def verify_routing(
    routing_id: bytes,
    msg_worker_id: str,
    msg_session_id: str,
    msg_job_id: str,
    workers: dict[bytes, Any],
    jobs: dict[str, Any],
    worker_by_id: dict[str, Any] | None = None,
) -> RoutingVerification:
    """Vérifie la cohérence routing/session/job à chaque réception broker."""
    rid_hex = routing_id.hex()
    worker = workers.get(routing_id)
    job = jobs.get(msg_job_id) if msg_job_id else None

    session_found = False
    expected_session = ""
    expected_job = ""
    expected_routing = ""
    worker_state = WorkerState.UNKNOWN.value

    if worker:
        session_found = worker.session_id == msg_session_id
        expected_session = worker.session_id
        expected_job = worker.current_job_id or ""
        expected_routing = worker.routing_id.hex()
        worker_state = worker.state.value
    elif worker_by_id and msg_worker_id:
        alt = worker_by_id.get(msg_worker_id)
        if alt:
            expected_routing = alt.routing_id.hex()
            expected_session = alt.session_id
            expected_job = alt.current_job_id or ""
            worker_state = alt.state.value

    inconsistencies: list[str] = []
    if worker is None:
        inconsistencies.append("worker_not_in_registry_for_routing_id")
    if msg_job_id and job is None:
        inconsistencies.append("job_not_in_registry")
    if worker and not session_found:
        inconsistencies.append(
            f"session_mismatch expected={worker.session_id} got={msg_session_id}"
        )
    if worker and msg_job_id and worker.current_job_id and worker.current_job_id != msg_job_id:
        inconsistencies.append(
            f"job_mismatch expected={worker.current_job_id} got={msg_job_id}"
        )
    if worker_by_id and msg_worker_id:
        known = worker_by_id.get(msg_worker_id)
        if known and known.routing_id != routing_id:
            inconsistencies.append(
                f"routing_id_changed expected={known.routing_id.hex()} got={rid_hex}"
            )

    return RoutingVerification(
        routing_id_received=rid_hex,
        worker_found=worker is not None,
        session_found=session_found,
        job_found=job is not None,
        worker_state=worker_state,
        worker_id=msg_worker_id,
        session_id=msg_session_id,
        job_id=msg_job_id,
        expected_routing_id=expected_routing,
        expected_session_id=expected_session,
        expected_job_id=expected_job,
        inconsistencies=inconsistencies,
    )


def log_routing_verification(
    logger: logging.Logger,
    rv: RoutingVerification,
    msg_type: str,
) -> None:
    logger.info(
        "ROUTING_VERIFY msg_type=%s routing_id_received=%s worker_found=%s "
        "session_found=%s job_found=%s worker_state=%s "
        "job_id=%s worker_id=%s session_id=%s socket_generation=N/A "
        "expected_routing=%s expected_session=%s expected_job=%s",
        msg_type,
        rv.routing_id_received,
        rv.worker_found,
        rv.session_found,
        rv.job_found,
        rv.worker_state,
        rv.job_id or "N/A",
        rv.worker_id or "N/A",
        rv.session_id or "N/A",
        rv.expected_routing_id or "N/A",
        rv.expected_session_id or "N/A",
        rv.expected_job_id or "N/A",
    )
    if rv.has_inconsistency():
        logger.warning(
            "ROUTING_INCONSISTENCY msg_type=%s job_id=%s worker_id=%s "
            "session_id=%s routing_id=%s issues=%s",
            msg_type,
            rv.job_id or "N/A",
            rv.worker_id or "N/A",
            rv.session_id or "N/A",
            rv.routing_id_received,
            rv.inconsistencies,
        )


def log_state_transition(
    logger: logging.Logger,
    timeline: TimelineCollector | None,
    *,
    component: str,
    worker_id: str,
    session_id: str,
    socket_generation: int,
    job_id: str,
    routing_id: str | bytes,
    from_state: WorkerState,
    to_state: WorkerState,
    reason: str,
) -> None:
    rid = routing_id.hex() if isinstance(routing_id, bytes) else routing_id
    logger.info(
        "STATE_TRANSITION component=%s from=%s to=%s reason=%s "
        "job_id=%s worker_id=%s session_id=%s socket_generation=%d routing_id=%s",
        component,
        from_state.value,
        to_state.value,
        reason,
        job_id or "N/A",
        worker_id,
        session_id,
        socket_generation,
        rid or "N/A",
    )
    if timeline:
        timeline.emit(
            f"STATE_{from_state.value}_TO_{to_state.value}",
            component=component,
            worker_id=worker_id,
            session_id=session_id,
            socket_generation=socket_generation,
            job_id=job_id,
            routing_id=rid,
            details={"reason": reason},
        )


def log_transport_case(
    logger: logging.Logger,
    case: str,
    *,
    job_id: str,
    worker_id: str,
    session_id: str,
    socket_generation: int,
    routing_id: str = "",
    details: dict[str, Any] | None = None,
) -> None:
    logger.warning(
        "TRANSPORT_CASE case=%s job_id=%s worker_id=%s session_id=%s "
        "socket_generation=%d routing_id=%s details=%s",
        case,
        job_id or "N/A",
        worker_id or "N/A",
        session_id or "N/A",
        socket_generation,
        routing_id or "N/A",
        details or {},
    )
