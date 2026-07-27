"""Timeline d'investigation : collecte, corrélation et classification des événements."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any

log = logging.getLogger(__name__)


class TransportCase(str, Enum):
    """Classification des trois cas de perte transport."""

    CASE1_SEND_NO_RECV = "CASE1_send_ok_broker_never_received"
    CASE2_RECV_REJECTED = "CASE2_broker_received_then_rejected"
    CASE3_ACCEPTED_ACK_LOST = "CASE3_broker_accepted_ack_lost"
    SUCCESS = "SUCCESS_end_to_end"
    INCONCLUSIVE = "INCONCLUSIVE"


class SessionValidationMode(str, Enum):
    HEARTBEAT = "heartbeat"
    READY_ACK = "ready_ack"
    RESULT_ACK = "result_ack"


@dataclass
class TimelineEvent:
    event: str
    mono_ts: float
    wall_ts: float
    worker_id: str = ""
    session_id: str = ""
    socket_generation: int = 0
    job_id: str = ""
    routing_id: str = ""
    thread: str = ""
    process: int = field(default_factory=os.getpid)
    component: str = ""
    run_id: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def correlation_line(self) -> str:
        return (
            f"job_id={self.job_id or 'N/A'} worker_id={self.worker_id or 'N/A'} "
            f"session_id={self.session_id or 'N/A'} "
            f"socket_generation={self.socket_generation} "
            f"routing_id={self.routing_id or 'N/A'}"
        )


@dataclass
class RunOutcome:
    run_id: str
    scenario: str
    strategy: str
    reconnect_mode: str
    session_validation: str
    worker_expired: bool = False
    send_succeeded: bool = False
    raw_message_received: bool = False
    result_accepted: bool = False
    result_rejected: bool = False
    result_missing: bool = True
    result_ack_received: bool = False
    transport_case: TransportCase = TransportCase.INCONCLUSIVE
    routing_id_changes: int = 0
    session_changes: int = 0
    reconnect_events: int = 0
    session_validated_heartbeat: bool = False
    session_validated_ready_ack: bool = False
    session_validated_result_ack: bool = False
    reject_reason: str | None = None
    job_id: str = ""
    seed: int | None = None
    parameters: dict[str, Any] = field(default_factory=dict)


@dataclass
class CampaignReport:
    seed: int
    total_runs: int = 0
    losses: int = 0
    replies_never_received: int = 0
    replies_rejected: int = 0
    reconnections: int = 0
    routing_changes: int = 0
    session_changes: int = 0
    success_by_strategy: dict[str, int] = field(default_factory=dict)
    runs_by_strategy: dict[str, int] = field(default_factory=dict)
    success_by_reconnect_mode: dict[str, int] = field(default_factory=dict)
    runs_by_reconnect_mode: dict[str, int] = field(default_factory=dict)
    success_by_session_validation: dict[str, int] = field(default_factory=dict)
    runs_by_session_validation: dict[str, int] = field(default_factory=dict)
    success_with_heartbeat: int = 0
    success_with_ready_ack: int = 0
    success_with_result_ack: int = 0
    runs_with_heartbeat_validation: int = 0
    runs_with_ready_ack_validation: int = 0
    runs_with_result_ack_validation: int = 0
    runs_by_transport_case: dict[str, int] = field(default_factory=dict)
    outcomes: list[RunOutcome] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["outcomes"] = [asdict(o) for o in self.outcomes]
        for o in d["outcomes"]:
            o["transport_case"] = (
                o["transport_case"].value
                if isinstance(o["transport_case"], TransportCase)
                else o["transport_case"]
            )
        return d


class TimelineCollector:
    """Collecteur thread-safe d'événements par job."""

    def __init__(self, run_id: str = "", enabled: bool = True) -> None:
        self.run_id = run_id
        self.enabled = enabled
        self._lock = threading.Lock()
        self._events: list[TimelineEvent] = []
        self._job_ids: set[str] = set()

    def emit(
        self,
        event: str,
        *,
        component: str = "",
        worker_id: str = "",
        session_id: str = "",
        socket_generation: int = 0,
        job_id: str = "",
        routing_id: str | bytes | None = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        if not self.enabled:
            return
        rid = ""
        if isinstance(routing_id, bytes):
            rid = routing_id.hex()
        elif routing_id:
            rid = str(routing_id)

        ev = TimelineEvent(
            event=event,
            mono_ts=time.monotonic(),
            wall_ts=time.time(),
            worker_id=worker_id,
            session_id=session_id,
            socket_generation=socket_generation,
            job_id=job_id,
            routing_id=rid,
            thread=threading.current_thread().name,
            component=component,
            run_id=self.run_id,
            details=details or {},
        )
        with self._lock:
            self._events.append(ev)
            if job_id:
                self._job_ids.add(job_id)

        log.info(
            "TIMELINE event=%s component=%s %s thread=%s details=%s",
            event,
            component,
            ev.correlation_line(),
            ev.thread,
            ev.details,
        )

    def events_for_job(self, job_id: str) -> list[TimelineEvent]:
        with self._lock:
            return [e for e in self._events if e.job_id == job_id]

    def all_events(self) -> list[TimelineEvent]:
        with self._lock:
            return list(self._events)

    def job_ids(self) -> list[str]:
        with self._lock:
            return sorted(self._job_ids)

    def print_timeline(self, job_id: str | None = None) -> str:
        """Retourne une timeline chronologique formatée."""
        with self._lock:
            events = sorted(
                [e for e in self._events if not job_id or e.job_id == job_id],
                key=lambda e: e.mono_ts,
            )
        lines = [f"=== Timeline run_id={self.run_id} job_id={job_id or 'ALL'} ==="]
        for e in events:
            lines.append(
                f"  [{e.mono_ts:12.6f} mono / {e.wall_ts:.6f} wall] "
                f"{e.event:<35} {e.correlation_line()} "
                f"thread={e.thread} process={e.process}"
            )
            if e.details:
                lines.append(f"      details={e.details}")
        text = "\n".join(lines)
        log.info("\n%s", text)
        return text

    def count_events(self, event_name: str) -> int:
        with self._lock:
            return sum(1 for e in self._events if e.event == event_name)

    def clear(self) -> None:
        with self._lock:
            self._events.clear()
            self._job_ids.clear()


def classify_transport_case(outcome: RunOutcome) -> TransportCase:
    if outcome.result_accepted and outcome.result_ack_received:
        return TransportCase.SUCCESS
    if outcome.send_succeeded and not outcome.raw_message_received:
        return TransportCase.CASE1_SEND_NO_RECV
    if outcome.raw_message_received and outcome.result_rejected:
        return TransportCase.CASE2_RECV_REJECTED
    if outcome.result_accepted and not outcome.result_ack_received:
        return TransportCase.CASE3_ACCEPTED_ACK_LOST
    if outcome.result_missing and not outcome.send_succeeded:
        return TransportCase.INCONCLUSIVE
    if outcome.result_accepted:
        return TransportCase.SUCCESS
    return TransportCase.INCONCLUSIVE


def build_campaign_report(seed: int, outcomes: list[RunOutcome]) -> CampaignReport:
    report = CampaignReport(seed=seed, total_runs=len(outcomes), outcomes=outcomes)

    for o in outcomes:
        o.transport_case = classify_transport_case(o)
        case_key = o.transport_case.value
        report.runs_by_transport_case[case_key] = (
            report.runs_by_transport_case.get(case_key, 0) + 1
        )

        is_success = o.transport_case == TransportCase.SUCCESS
        if not is_success:
            report.losses += 1
        if o.result_missing or (
            o.send_succeeded and not o.raw_message_received
        ):
            report.replies_never_received += 1
        if o.result_rejected:
            report.replies_rejected += 1
        report.reconnections += o.reconnect_events
        report.routing_changes += o.routing_id_changes
        report.session_changes += o.session_changes

        report.runs_by_strategy[o.strategy] = report.runs_by_strategy.get(o.strategy, 0) + 1
        report.runs_by_reconnect_mode[o.reconnect_mode] = (
            report.runs_by_reconnect_mode.get(o.reconnect_mode, 0) + 1
        )
        report.runs_by_session_validation[o.session_validation] = (
            report.runs_by_session_validation.get(o.session_validation, 0) + 1
        )

        if o.session_validation == SessionValidationMode.HEARTBEAT.value:
            report.runs_with_heartbeat_validation += 1
            if is_success:
                report.success_with_heartbeat += 1
        elif o.session_validation == SessionValidationMode.READY_ACK.value:
            report.runs_with_ready_ack_validation += 1
            if is_success:
                report.success_with_ready_ack += 1
        elif o.session_validation == SessionValidationMode.RESULT_ACK.value:
            report.runs_with_result_ack_validation += 1
            if is_success:
                report.success_with_result_ack += 1

        if is_success:
            report.success_by_strategy[o.strategy] = (
                report.success_by_strategy.get(o.strategy, 0) + 1
            )
            report.success_by_reconnect_mode[o.reconnect_mode] = (
                report.success_by_reconnect_mode.get(o.reconnect_mode, 0) + 1
            )
            report.success_by_session_validation[o.session_validation] = (
                report.success_by_session_validation.get(o.session_validation, 0) + 1
            )

    return report


def print_campaign_report(report: CampaignReport) -> None:
    print()
    print("=" * 72)
    print("RAPPORT DE CAMPAGNE D'INVESTIGATION")
    print("=" * 72)
    print(f"Seed                    : {report.seed}")
    print(f"Runs totaux             : {report.total_runs}")
    print(f"Pertes                  : {report.losses}")
    print(f"Replies jamais reçus    : {report.replies_never_received}")
    print(f"Replies rejetés         : {report.replies_rejected}")
    print(f"Reconnexions            : {report.reconnections}")
    print(f"Changements routing_id  : {report.routing_changes}")
    print(f"Changements session     : {report.session_changes}")
    print()
    print("Cas transport:")
    for case, count in sorted(report.runs_by_transport_case.items()):
        print(f"  {case:<45} {count:>5}")
    print()
    print("Succès par stratégie:")
    for strat in sorted(report.runs_by_strategy.keys()):
        runs = report.runs_by_strategy[strat]
        ok = report.success_by_strategy.get(strat, 0)
        pct = (ok / runs * 100) if runs else 0
        print(f"  {strat:<45} {ok:>5}/{runs:<5} ({pct:.1f}%)")
    print()
    print("Succès par mode reconnect:")
    for mode in sorted(report.runs_by_reconnect_mode.keys()):
        runs = report.runs_by_reconnect_mode[mode]
        ok = report.success_by_reconnect_mode.get(mode, 0)
        pct = (ok / runs * 100) if runs else 0
        print(f"  {mode:<45} {ok:>5}/{runs:<5} ({pct:.1f}%)")
    print()
    print("Validation de session (taux de réussite):")
    hb_runs = report.runs_with_heartbeat_validation
    ra_runs = report.runs_with_ready_ack_validation
    res_runs = report.runs_with_result_ack_validation
    hb_pct = (report.success_with_heartbeat / hb_runs * 100) if hb_runs else 0
    ra_pct = (report.success_with_ready_ack / ra_runs * 100) if ra_runs else 0
    res_pct = (report.success_with_result_ack / res_runs * 100) if res_runs else 0
    print(f"  heartbeat   : {report.success_with_heartbeat}/{hb_runs} ({hb_pct:.1f}%)")
    print(f"  READY_ACK   : {report.success_with_ready_ack}/{ra_runs} ({ra_pct:.1f}%)")
    print(f"  RESULT_ACK  : {report.success_with_result_ack}/{res_runs} ({res_pct:.1f}%)")
    print("=" * 72)


def save_campaign_report(report: CampaignReport, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report.to_dict(), f, indent=2, ensure_ascii=False)
    log.info("Campaign report saved to %s", path)
