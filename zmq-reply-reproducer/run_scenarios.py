#!/usr/bin/env python3
"""Lanceur de scénarios automatiques pour le reproducer ZMQ."""

from __future__ import annotations

import argparse
import io
import random
import socket
import sys
import threading
import time
import uuid
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

from broker import Broker
from config import (
    HEARTBEAT_INTERVAL,
    JOB_DURATION,
    LIVENESS_MULTIPLIER,
    BrokerConfig,
    ScenarioConfig,
    WorkerConfig,
    ZmqSocketOptions,
)
from logging_setup import setup_logging
from payload_utils import jitter_payload_bytes, payload_timeout_bonus
from timeline import (
    RunOutcome,
    TimelineCollector,
    TransportCase,
    build_campaign_report,
    classify_transport_case,
    print_campaign_report,
    save_campaign_report,
)
from trace_store import TraceSession
from worker import Worker

log = setup_logging("scenarios", "WARNING")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def apply_jitter(rng: random.Random, value: float, jitter: float) -> float:
    if jitter <= 0:
        return value
    factor = 1.0 + rng.uniform(-jitter, jitter)
    return max(0.01, value * factor)


@dataclass
class ScenarioDef:
    name: str
    strategy: str
    reconnect_mode: str = "full"
    job_duration: float | None = None
    short_job: bool = False
    reconnect_delay: float = 0.0
    reply_delay: float = 0.0
    network_delay: float = 0.0
    zmq_immediate: int = 0


@dataclass
class RunResult:
    scenario: str
    worker_expired: bool = False
    send_succeeded: bool = False
    raw_message_received: bool = False
    result_accepted: bool = False
    result_rejected: bool = False
    result_missing: bool = True
    result_ack_received: bool = False
    transport_case: str = ""
    job_id: str = ""
    payload_bytes: int = 0


@dataclass
class ScenarioSummary:
    scenario: str
    runs: int = 0
    worker_expired: int = 0
    send_succeeded: int = 0
    raw_message_received: int = 0
    result_accepted: int = 0
    result_rejected: int = 0
    result_missing: int = 0
    case1_send_no_recv: int = 0
    case2_recv_rejected: int = 0
    case3_ack_lost: int = 0

    @property
    def success_rate(self) -> float:
        if self.runs == 0:
            return 0.0
        return self.result_accepted / self.runs * 100.0


DEFAULT_SCENARIOS: list[ScenarioDef] = [
    ScenarioDef(name="job_lt_timeout", strategy="no-reconnect", short_job=True),
    ScenarioDef(name="job_gt_timeout_no_reconnect", strategy="no-reconnect"),
    ScenarioDef(
        name="job_gt_timeout_reconnect_light_immediate",
        strategy="manual-reconnect-immediate",
        reconnect_mode="light",
    ),
    ScenarioDef(
        name="job_gt_timeout_reconnect_full_immediate",
        strategy="manual-reconnect-immediate",
        reconnect_mode="full",
    ),
    ScenarioDef(
        name="job_gt_timeout_reconnect_then_heartbeat",
        strategy="manual-reconnect-heartbeat",
        reconnect_mode="full",
    ),
    ScenarioDef(
        name="job_gt_timeout_reconnect_then_ready_ack",
        strategy="manual-reconnect-ready-ack",
        reconnect_mode="full",
    ),
    ScenarioDef(name="job_gt_timeout_auto_reconnect", strategy="auto-reconnect-only"),
    ScenarioDef(
        name="job_gt_timeout_reconnect_then_delay",
        strategy="reconnect-then-delay",
        reconnect_mode="full",
        reconnect_delay=0.5,
    ),
]


def run_single_scenario(
    scenario: ScenarioDef,
    cfg: ScenarioConfig,
    heartbeat_interval: float,
    worker_timeout: float,
    base_job_duration: float,
    rng: random.Random,
    quiet: bool = True,
    run_index: int = 0,
    trace: TraceSession | None = None,
) -> tuple[RunResult, RunOutcome]:
    run_id = str(uuid.uuid4())
    port = free_port()
    endpoint = f"tcp://127.0.0.1:{port}"

    hb_interval = apply_jitter(rng, heartbeat_interval, cfg.jitter)
    job_duration = base_job_duration
    if scenario.short_job:
        job_duration = worker_timeout * 0.4
    elif scenario.job_duration is not None:
        job_duration = scenario.job_duration
    else:
        job_duration = apply_jitter(rng, base_job_duration, cfg.jitter)

    reconnect_delay = apply_jitter(rng, scenario.reconnect_delay, cfg.jitter)
    reply_delay = apply_jitter(rng, scenario.reply_delay, cfg.jitter)
    network_delay = apply_jitter(rng, scenario.network_delay, cfg.jitter)
    if cfg.result_payload_bytes > 0:
        result_payload_bytes = jitter_payload_bytes(
            rng, cfg.result_payload_bytes, cfg.payload_jitter
        )
    else:
        result_payload_bytes = 0
    payload_seed = (cfg.seed or 0) + run_index
    scenario_timeout = cfg.scenario_timeout + payload_timeout_bonus(
        result_payload_bytes
    )
    log_level = "INFO" if trace else ("ERROR" if quiet else "INFO")

    timeline = TimelineCollector(run_id=run_id, enabled=cfg.timeline_enabled)

    run_params = {
        "job_duration": job_duration,
        "reconnect_delay": reconnect_delay,
        "reply_delay": reply_delay,
        "network_delay": network_delay,
        "result_payload_bytes": result_payload_bytes,
        "payload_seed": payload_seed,
        "heartbeat_interval": hb_interval,
        "worker_timeout": worker_timeout,
        "scenario_timeout": scenario_timeout,
        "result_policy": cfg.result_policy,
        "run_index": run_index,
    }

    capture_ctx = (
        trace.begin_run(scenario.name, run_index, quiet=quiet)
        if trace
        else nullcontext()
    )

    with capture_ctx as capture:
        broker_cfg = BrokerConfig(
            endpoint=endpoint,
            heartbeat_interval=hb_interval,
            liveness_multiplier=LIVENESS_MULTIPLIER,
            result_policy=cfg.result_policy,
            zmq_options=ZmqSocketOptions(
                immediate=scenario.zmq_immediate or cfg.zmq_immediate,
                linger=cfg.zmq_linger,
                reconnect_ivl=cfg.zmq_reconnect_ivl,
                reconnect_ivl_max=cfg.zmq_reconnect_ivl_max,
                sndtimeo=cfg.zmq_sndtimeo,
            ),
            monitor_enabled=cfg.monitor_enabled,
            log_level=log_level,
        )

        worker_cfg = WorkerConfig(
            broker_endpoint=endpoint,
            worker_id=f"worker-{port}",
            strategy=scenario.strategy,  # type: ignore[arg-type]
            reconnect_mode=scenario.reconnect_mode,  # type: ignore[arg-type]
            threading_mode=cfg.threading_mode,
            job_duration=job_duration,
            heartbeat_interval=hb_interval,
            liveness_multiplier=LIVENESS_MULTIPLIER,
            reconnect_delay=reconnect_delay,
            reply_delay=reply_delay,
            network_delay=network_delay,
            result_ack_timeout=1.0,
            result_payload_bytes=result_payload_bytes,
            payload_seed=payload_seed,
            zmq_options=ZmqSocketOptions(
                immediate=scenario.zmq_immediate or cfg.zmq_immediate,
                linger=cfg.zmq_linger,
                reconnect_ivl=cfg.zmq_reconnect_ivl,
                reconnect_ivl_max=cfg.zmq_reconnect_ivl_max,
                sndtimeo=cfg.zmq_sndtimeo,
            ),
            monitor_enabled=cfg.monitor_enabled,
            log_level=log_level,
        )

        broker = Broker(broker_cfg, timeline=timeline, run_id=run_id)
        broker_stats_holder: list[Any] = []
        broker_error: list[Exception] = []

        def broker_thread() -> None:
            try:
                stats = broker.run_once(
                    auto_dispatch=True,
                    timeout=scenario_timeout,
                )
                broker_stats_holder.append(stats)
            except Exception as exc:
                broker_error.append(exc)
            finally:
                broker.shutdown()

        bt = threading.Thread(target=broker_thread, daemon=True)
        bt.start()
        time.sleep(0.3)

        worker = Worker(worker_cfg, timeline=timeline, run_id=run_id)
        worker_stats = worker.run()

        bt.join(timeout=scenario_timeout + 3)
        if broker_error:
            log.error("Broker error: %s", broker_error[0])

        bstats = broker_stats_holder[0] if broker_stats_holder else broker.stats

        outcome = RunOutcome(
            run_id=run_id,
            scenario=scenario.name,
            strategy=scenario.strategy,
            reconnect_mode=scenario.reconnect_mode,
            session_validation=worker.session_validation_mode().value,
            worker_expired=bstats.worker_expired,
            send_succeeded=worker_stats.send_succeeded,
            raw_message_received=bstats.raw_message_received,
            result_accepted=bstats.result_accepted,
            result_rejected=bstats.result_rejected,
            result_missing=bstats.result_missing,
            result_ack_received=worker_stats.result_ack_received,
            reject_reason=bstats.reject_reason,
            job_id=bstats.job_id or worker_stats.job_id,
            seed=cfg.seed,
            parameters=run_params,
        )
        transport = classify_transport_case(outcome)
        if worker_stats.send_succeeded and not bstats.raw_message_received:
            transport = TransportCase.CASE1_SEND_NO_RECV
        elif bstats.result_accepted and not worker_stats.result_ack_received:
            transport = TransportCase.CASE3_ACCEPTED_ACK_LOST
        outcome.transport_case = transport

        if not quiet and outcome.job_id:
            timeline.print_timeline(outcome.job_id)

        if trace and capture is not None:
            capture.save(
                outcome=outcome,
                timeline=timeline,
                extra=run_params,
            )

    result = RunResult(
        scenario=scenario.name,
        worker_expired=bstats.worker_expired,
        send_succeeded=worker_stats.send_succeeded,
        raw_message_received=bstats.raw_message_received,
        result_accepted=bstats.result_accepted,
        result_rejected=bstats.result_rejected,
        result_missing=bstats.result_missing,
        result_ack_received=worker_stats.result_ack_received,
        transport_case=transport.value,
        job_id=outcome.job_id,
        payload_bytes=result_payload_bytes,
    )
    return result, outcome


def aggregate_results(results: list[RunResult]) -> dict[str, ScenarioSummary]:
    summaries: dict[str, ScenarioSummary] = {}
    for r in results:
        if r.scenario not in summaries:
            summaries[r.scenario] = ScenarioSummary(scenario=r.scenario)
        s = summaries[r.scenario]
        s.runs += 1
        if r.worker_expired:
            s.worker_expired += 1
        if r.send_succeeded:
            s.send_succeeded += 1
        if r.raw_message_received:
            s.raw_message_received += 1
        if r.result_accepted:
            s.result_accepted += 1
        if r.result_rejected:
            s.result_rejected += 1
        if r.result_missing:
            s.result_missing += 1
        if r.transport_case == TransportCase.CASE1_SEND_NO_RECV.value:
            s.case1_send_no_recv += 1
        elif r.transport_case == TransportCase.CASE2_RECV_REJECTED.value:
            s.case2_recv_rejected += 1
        elif r.transport_case == TransportCase.CASE3_ACCEPTED_ACK_LOST.value:
            s.case3_ack_lost += 1
    return summaries


def print_summary(summaries: dict[str, ScenarioSummary]) -> None:
    header = (
        f"{'strategy':<45} {'runs':>5} {'expired':>8} {'send_ok':>8} "
        f"{'raw_rx':>7} {'accepted':>9} {'rejected':>9} {'missing':>8} "
        f"{'case1':>6} {'case2':>6} {'case3':>6} {'success%':>9}"
    )
    print(header)
    print("-" * len(header))
    for name in sorted(summaries.keys()):
        s = summaries[name]
        print(
            f"{s.scenario:<45} {s.runs:>5} {s.worker_expired:>8} "
            f"{s.send_succeeded:>8} {s.raw_message_received:>7} "
            f"{s.result_accepted:>9} {s.result_rejected:>9} "
            f"{s.result_missing:>8} {s.case1_send_no_recv:>6} "
            f"{s.case2_recv_rejected:>6} {s.case3_ack_lost:>6} "
            f"{s.success_rate:>8.1f}%"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ZMQ reply-loss scenarios")
    parser.add_argument("--runs", type=int, default=20)
    parser.add_argument("--jitter", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--result-payload-bytes",
        type=int,
        default=0,
        help="Taille moyenne du payload RESULT (0 = léger, ex: 20971520 pour ~20 Mo)",
    )
    parser.add_argument(
        "--payload-jitter",
        type=float,
        default=0.15,
        help="Jitter relatif sur la taille du payload",
    )
    parser.add_argument("--production", action="store_true")
    parser.add_argument("--result-policy", choices=["strict", "job-id"], default="strict")
    parser.add_argument(
        "--threading-mode",
        choices=["blocking-network-loop", "separate-job-thread"],
        default="blocking-network-loop",
    )
    parser.add_argument("--scenario-timeout", type=float, default=30.0)
    parser.add_argument("--zmq-immediate", type=int, default=0, choices=[0, 1])
    parser.add_argument("--scenarios", nargs="*", help="Subset of scenario names")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--no-monitor", action="store_true")
    parser.add_argument("--report", default="", help="Chemin rapport JSON")
    parser.add_argument(
        "--trace-dir",
        default="",
        help="Répertoire d'archivage des traces (ex: traces). Vide = pas d'archive",
    )
    parser.add_argument(
        "--trace-label",
        default="",
        help="Libellé optionnel pour retrouver la session (ex: premier-test)",
    )
    parser.add_argument(
        "--save-traces",
        action="store_true",
        help="Raccourci pour --trace-dir traces",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.verbose:
        global log
        log = setup_logging("scenarios", "INFO")

    seed = args.seed if args.seed is not None else int(time.time())
    rng = random.Random(seed)
    trace_dir = args.trace_dir or ("traces" if args.save_traces else "")

    heartbeat_interval = HEARTBEAT_INTERVAL
    liveness_multiplier = LIVENESS_MULTIPLIER
    base_job_duration = JOB_DURATION

    if args.production:
        from config import PROD_HEARTBEAT_INTERVAL, PROD_JOB_DURATION, PROD_LIVENESS_MULTIPLIER

        heartbeat_interval = PROD_HEARTBEAT_INTERVAL
        liveness_multiplier = PROD_LIVENESS_MULTIPLIER
        base_job_duration = PROD_JOB_DURATION

    worker_timeout = heartbeat_interval * liveness_multiplier

    cfg = ScenarioConfig(
        runs=args.runs,
        jitter=args.jitter,
        production=args.production,
        result_policy=args.result_policy,
        threading_mode=args.threading_mode,
        scenario_timeout=args.scenario_timeout,
        zmq_immediate=args.zmq_immediate,
        seed=seed,
        monitor_enabled=not args.no_monitor,
        report_path=args.report,
        result_payload_bytes=args.result_payload_bytes,
        payload_jitter=args.payload_jitter,
        trace_dir=trace_dir,
        trace_label=args.trace_label,
    )

    scenarios = DEFAULT_SCENARIOS
    if args.scenarios:
        names = set(args.scenarios)
        scenarios = [s for s in DEFAULT_SCENARIOS if s.name in names]

    print(
        f"Running {args.runs} iterations per scenario (seed={seed}) "
        f"(heartbeat={heartbeat_interval}s, timeout={worker_timeout}s, "
        f"job={base_job_duration}s, jitter={args.jitter}, "
        f"payload={args.result_payload_bytes}B±{args.payload_jitter:.0%})"
    )
    print()

    trace_session: TraceSession | None = None
    if trace_dir:
        trace_session = TraceSession(
            trace_dir,
            source="scenarios",
            label=args.trace_label,
            seed=seed,
            command=" ".join(sys.argv),
            config={
                "runs": args.runs,
                "jitter": args.jitter,
                "result_policy": args.result_policy,
                "result_payload_bytes": args.result_payload_bytes,
                "payload_jitter": args.payload_jitter,
                "scenarios": [s.name for s in scenarios],
            },
        )
        trace_session.enable_stdout_tee()

    all_results: list[RunResult] = []
    all_outcomes: list[RunOutcome] = []
    for scenario in scenarios:
        for i in range(args.runs):
            result, outcome = run_single_scenario(
                scenario,
                cfg,
                heartbeat_interval,
                worker_timeout,
                base_job_duration,
                rng,
                quiet=not args.verbose,
                run_index=i,
                trace=trace_session,
            )
            all_results.append(result)
            all_outcomes.append(outcome)
            if args.verbose:
                print(
                    f"  [{scenario.name} run {i+1}] {result} "
                    f"case={result.transport_case}"
                )

    summaries = aggregate_results(all_results)
    print_summary(summaries)

    report = build_campaign_report(seed, all_outcomes) if all_outcomes else None
    if args.report and report:
        save_campaign_report(report, args.report)
        print(f"\nRapport JSON: {args.report}")

    if trace_session:
        path = trace_session.finalize(
            summary_data=report.to_dict() if report else None,
            outcomes=all_outcomes,
        )
        print(f"\nTraces archivées: {path}")
        print(f"  python3 list_traces.py list")
        print(f"  python3 list_traces.py show {trace_session.session_id}/001_...")


if __name__ == "__main__":
    main()
