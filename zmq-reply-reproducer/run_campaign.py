#!/usr/bin/env python3
"""Campagnes de stress automatiques pour le laboratoire d'investigation ZMQ."""

from __future__ import annotations

import argparse
import random
import socket
import sys
import threading
import time
import uuid
from contextlib import nullcontext
from dataclasses import dataclass, field
from typing import Any

from broker import Broker
from config import (
    HEARTBEAT_INTERVAL,
    JOB_DURATION,
    LIVENESS_MULTIPLIER,
    BrokerConfig,
    CampaignConfig,
    WorkerConfig,
    ZmqSocketOptions,
)
from logging_setup import setup_logging
from payload_utils import (
    CAMPAIGN_AVG_PAYLOAD_BYTES,
    jitter_payload_bytes,
    payload_timeout_bonus,
)
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

log = setup_logging("campaign", "WARNING")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def apply_jitter(rng: random.Random, value: float, jitter: float) -> float:
    if jitter <= 0:
        return value
    factor = 1.0 + rng.uniform(-jitter, jitter)
    return max(0.01, value * factor)


STRATEGIES = [
    "no-reconnect",
    "manual-reconnect-immediate",
    "manual-reconnect-heartbeat",
    "manual-reconnect-ready-ack",
    "auto-reconnect-only",
    "reconnect-then-delay",
]

RECONNECT_MODES = ["light", "full"]


@dataclass
class CampaignScenario:
  name: str
  strategy: str
  reconnect_mode: str = "full"
  job_duration: float | None = None
  short_job: bool = False
  reconnect_delay: float = 0.0
  reply_delay: float = 0.0
  network_delay: float = 0.0
  zmq_immediate: int = 0


def generate_scenario_matrix(rng: random.Random, count: int) -> list[CampaignScenario]:
    """Génère une matrice de scénarios variés pour la campagne."""
    scenarios: list[CampaignScenario] = []
    for i in range(count):
        strategy = rng.choice(STRATEGIES)
        reconnect_mode = rng.choice(RECONNECT_MODES)
        short_job = rng.random() < 0.1
        scenarios.append(
            CampaignScenario(
                name=f"campaign_{i:04d}_{strategy}_{reconnect_mode}",
                strategy=strategy,
                reconnect_mode=reconnect_mode,
                short_job=short_job,
                reconnect_delay=rng.uniform(0.0, 1.0),
                reply_delay=rng.uniform(0.0, 0.5),
                network_delay=rng.uniform(0.0, 0.3),
                zmq_immediate=rng.choice([0, 1]),
            )
        )
    return scenarios


def run_single_campaign_scenario(
    scenario: CampaignScenario,
    cfg: CampaignConfig,
    heartbeat_interval: float,
    worker_timeout: float,
    base_job_duration: float,
    rng: random.Random,
    run_index: int,
    trace: TraceSession | None = None,
    quiet: bool = True,
) -> RunOutcome:
    run_id = str(uuid.uuid4())
    port = free_port()
    endpoint = f"tcp://127.0.0.1:{port}"

    hb_interval = apply_jitter(rng, heartbeat_interval, cfg.jitter)
    if scenario.short_job:
        job_duration = worker_timeout * rng.uniform(0.2, 0.45)
    elif scenario.job_duration is not None:
        job_duration = scenario.job_duration
    else:
        job_duration = apply_jitter(rng, base_job_duration, cfg.jitter)

    reconnect_delay = apply_jitter(rng, scenario.reconnect_delay, cfg.jitter)
    reply_delay = apply_jitter(rng, scenario.reply_delay, cfg.jitter)
    network_delay = apply_jitter(rng, scenario.network_delay, cfg.jitter)
    result_payload_bytes = jitter_payload_bytes(
        rng, cfg.avg_payload_bytes, cfg.payload_jitter
    )
    payload_seed = cfg.seed + run_index
    scenario_timeout = cfg.scenario_timeout + payload_timeout_bonus(
        result_payload_bytes
    )
    log_level = "INFO" if trace else ("INFO" if cfg.verbose else "ERROR")

    timeline = TimelineCollector(run_id=run_id, enabled=cfg.timeline_enabled)

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
            worker_id=f"worker-{port}-{run_index}",
            strategy=scenario.strategy,  # type: ignore[arg-type]
            reconnect_mode=scenario.reconnect_mode,  # type: ignore[arg-type]
            threading_mode=cfg.threading_mode,
            job_duration=job_duration,
            heartbeat_interval=hb_interval,
            liveness_multiplier=LIVENESS_MULTIPLIER,
            reconnect_delay=reconnect_delay,
            reply_delay=reply_delay,
            network_delay=network_delay,
            result_ack_timeout=1.5,
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
            log.error("Broker error in run %s: %s", run_id, broker_error[0])

        bstats = broker_stats_holder[0] if broker_stats_holder else broker.stats

        if worker_stats.send_succeeded and not bstats.raw_message_received:
            bstats.transport_case = TransportCase.CASE1_SEND_NO_RECV
            bstats.result_missing = True
        elif bstats.result_accepted and not worker_stats.result_ack_received:
            bstats.transport_case = TransportCase.CASE3_ACCEPTED_ACK_LOST
        elif bstats.result_accepted and worker_stats.result_ack_received:
            bstats.transport_case = TransportCase.SUCCESS

        session_mode = worker.session_validation_mode().value

        outcome = RunOutcome(
            run_id=run_id,
            scenario=scenario.name,
            strategy=scenario.strategy,
            reconnect_mode=scenario.reconnect_mode,
            session_validation=session_mode,
            worker_expired=bstats.worker_expired,
            send_succeeded=worker_stats.send_succeeded,
            raw_message_received=bstats.raw_message_received,
            result_accepted=bstats.result_accepted,
            result_rejected=bstats.result_rejected,
            result_missing=bstats.result_missing,
            result_ack_received=worker_stats.result_ack_received,
            routing_id_changes=bstats.routing_id_changes,
            session_changes=bstats.session_changes,
            reconnect_events=max(
                bstats.reconnect_events, worker_stats.reconnect_events
            ),
            session_validated_heartbeat=worker_stats.session_validated_heartbeat,
            session_validated_ready_ack=worker_stats.session_validated_ready_ack,
            session_validated_result_ack=worker_stats.session_validated_result_ack,
            reject_reason=bstats.reject_reason,
            job_id=bstats.job_id or worker_stats.job_id,
            seed=cfg.seed,
            parameters={
                "job_duration": job_duration,
                "reconnect_delay": reconnect_delay,
                "reply_delay": reply_delay,
                "network_delay": network_delay,
                "result_payload_bytes": result_payload_bytes,
                "payload_seed": payload_seed,
                "heartbeat_interval": hb_interval,
                "worker_timeout": worker_timeout,
                "zmq_immediate": scenario.zmq_immediate,
                "run_index": run_index,
            },
        )
        outcome.transport_case = classify_transport_case(outcome)

        if worker_stats.send_succeeded and not bstats.raw_message_received:
            outcome.transport_case = TransportCase.CASE1_SEND_NO_RECV
        elif bstats.result_accepted and not worker_stats.result_ack_received:
            outcome.transport_case = TransportCase.CASE3_ACCEPTED_ACK_LOST

        if cfg.print_timelines and outcome.job_id:
            timeline.print_timeline(outcome.job_id)

        if cfg.verbose:
            print(
                f"  [{scenario.name}] case={outcome.transport_case.value} "
                f"send={outcome.send_succeeded} rx={outcome.raw_message_received} "
                f"accepted={outcome.result_accepted} ack={outcome.result_ack_received}"
            )

        if trace and capture is not None:
            capture.save(outcome=outcome, timeline=timeline)

    return outcome


def run_campaign(cfg: CampaignConfig, trace: TraceSession | None = None) -> list[RunOutcome]:
    rng = random.Random(cfg.seed)

    heartbeat_interval = HEARTBEAT_INTERVAL
    liveness_multiplier = LIVENESS_MULTIPLIER
    base_job_duration = JOB_DURATION

    if cfg.production:
        from config import PROD_HEARTBEAT_INTERVAL, PROD_JOB_DURATION, PROD_LIVENESS_MULTIPLIER

        heartbeat_interval = PROD_HEARTBEAT_INTERVAL
        liveness_multiplier = PROD_LIVENESS_MULTIPLIER
        base_job_duration = PROD_JOB_DURATION

    worker_timeout = heartbeat_interval * liveness_multiplier
    scenarios = generate_scenario_matrix(rng, cfg.runs)

    print(
        f"Campagne: {cfg.runs} scénarios (seed={cfg.seed}, "
        f"heartbeat={heartbeat_interval}s, timeout={worker_timeout}s, "
        f"job_base={base_job_duration}s, jitter={cfg.jitter}, "
        f"payload_avg={cfg.avg_payload_bytes}B±{cfg.payload_jitter:.0%})"
    )

    outcomes: list[RunOutcome] = []
    for i, scenario in enumerate(scenarios):
        outcome = run_single_campaign_scenario(
            scenario,
            cfg,
            heartbeat_interval,
            worker_timeout,
            base_job_duration,
            rng,
            run_index=i,
            trace=trace,
            quiet=not cfg.verbose,
        )
        outcomes.append(outcome)
        if (i + 1) % 50 == 0:
            print(f"  Progression: {i + 1}/{cfg.runs}")

    return outcomes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Campagne de stress pour le laboratoire d'investigation ZMQ"
    )
    parser.add_argument("--runs", type=int, default=300)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--jitter", type=float, default=0.15)
    parser.add_argument(
        "--avg-payload-bytes",
        type=int,
        default=CAMPAIGN_AVG_PAYLOAD_BYTES,
        help="Taille moyenne du payload RESULT (~20 Mo par défaut)",
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
    parser.add_argument("--scenario-timeout", type=float, default=20.0)
    parser.add_argument("--zmq-immediate", type=int, default=0, choices=[0, 1])
    parser.add_argument("--no-monitor", action="store_true")
    parser.add_argument("--no-timeline", action="store_true")
    parser.add_argument("--report", default="campaign_report.json")
    parser.add_argument("--print-timelines", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--trace-dir", default="", help="Répertoire d'archivage des traces")
    parser.add_argument("--trace-label", default="", help="Libellé de session")
    parser.add_argument("--save-traces", action="store_true", help="Archive dans traces/")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.verbose:
        global log
        log = setup_logging("campaign", "INFO")

    cfg = CampaignConfig(
        runs=args.runs,
        seed=args.seed,
        jitter=args.jitter,
        production=args.production,
        result_policy=args.result_policy,
        threading_mode=args.threading_mode,
        scenario_timeout=args.scenario_timeout,
        zmq_immediate=args.zmq_immediate,
        avg_payload_bytes=args.avg_payload_bytes,
        payload_jitter=args.payload_jitter,
        monitor_enabled=not args.no_monitor,
        timeline_enabled=not args.no_timeline,
        report_path=args.report,
        print_timelines=args.print_timelines,
        verbose=args.verbose,
        trace_dir=args.trace_dir or ("traces" if args.save_traces else ""),
        trace_label=args.trace_label,
    )

    trace_session: TraceSession | None = None
    if cfg.trace_dir:
        trace_session = TraceSession(
            cfg.trace_dir,
            source="campaign",
            label=cfg.trace_label,
            seed=cfg.seed,
            command=" ".join(sys.argv),
            config={
                "runs": cfg.runs,
                "jitter": cfg.jitter,
                "result_policy": cfg.result_policy,
                "avg_payload_bytes": cfg.avg_payload_bytes,
            },
        )
        trace_session.enable_stdout_tee()

    outcomes = run_campaign(cfg, trace=trace_session)
    report = build_campaign_report(cfg.seed, outcomes)
    print_campaign_report(report)

    if cfg.report_path:
        save_campaign_report(report, cfg.report_path)
        print(f"\nRapport JSON: {cfg.report_path}")

    if trace_session:
        path = trace_session.finalize(
            summary_data=report.to_dict(),
            outcomes=outcomes,
        )
        print(f"\nTraces archivées: {path}")


if __name__ == "__main__":
    main()
