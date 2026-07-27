#!/usr/bin/env python3
"""Lanceur de scénarios automatiques pour le reproducer ZMQ."""

from __future__ import annotations

import argparse
import random
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from broker import Broker
from config import (
    HEARTBEAT_INTERVAL,
    JOB_DURATION,
    LIVENESS_MULTIPLIER,
    ScenarioConfig,
    WorkerConfig,
    ZmqSocketOptions,
)
from logging_setup import setup_logging
from worker import Worker

log = setup_logging("scenarios", "WARNING")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def apply_jitter(value: float, jitter: float) -> float:
    if jitter <= 0:
        return value
    factor = 1.0 + random.uniform(-jitter, jitter)
    return max(0.01, value * factor)


@dataclass
class ScenarioDef:
    name: str
    strategy: str
    reconnect_mode: str = "full"
    job_duration: float | None = None  # None = use default long job
    short_job: bool = False
    reconnect_delay: float = 0.0
    reply_delay: float = 0.0
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
    quiet: bool = True,
) -> RunResult:
    port = free_port()
    endpoint = f"tcp://127.0.0.1:{port}"

    hb_interval = apply_jitter(heartbeat_interval, cfg.jitter)
    job_duration = base_job_duration
    if scenario.short_job:
        job_duration = worker_timeout * 0.4
    elif scenario.job_duration is not None:
        job_duration = scenario.job_duration
    else:
        job_duration = apply_jitter(base_job_duration, cfg.jitter)

    reconnect_delay = apply_jitter(scenario.reconnect_delay, cfg.jitter)
    reply_delay = apply_jitter(scenario.reply_delay, cfg.jitter)
    log_level = "ERROR" if quiet else "INFO"

    from config import BrokerConfig

    broker_cfg = BrokerConfig(
        endpoint=endpoint,
        heartbeat_interval=hb_interval,
        liveness_multiplier=LIVENESS_MULTIPLIER,
        result_policy=cfg.result_policy,
        zmq_options=ZmqSocketOptions(
            immediate=cfg.zmq_immediate,
            linger=cfg.zmq_linger,
            reconnect_ivl=cfg.zmq_reconnect_ivl,
            reconnect_ivl_max=cfg.zmq_reconnect_ivl_max,
            sndtimeo=cfg.zmq_sndtimeo,
        ),
        monitor_enabled=False,
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
        result_ack_timeout=1.0,
        zmq_options=ZmqSocketOptions(
            immediate=scenario.zmq_immediate or cfg.zmq_immediate,
            linger=cfg.zmq_linger,
            reconnect_ivl=cfg.zmq_reconnect_ivl,
            reconnect_ivl_max=cfg.zmq_reconnect_ivl_max,
            sndtimeo=cfg.zmq_sndtimeo,
        ),
        monitor_enabled=False,
        log_level=log_level,
    )

    broker = Broker(broker_cfg)
    broker_stats_holder: list[Any] = []
    broker_error: list[Exception] = []

    def broker_thread() -> None:
        try:
            stats = broker.run_once(
                auto_dispatch=True,
                timeout=cfg.scenario_timeout,
            )
            broker_stats_holder.append(stats)
        except Exception as exc:
            broker_error.append(exc)
        finally:
            broker.shutdown()

    bt = threading.Thread(target=broker_thread, daemon=True)
    bt.start()
    time.sleep(0.3)

    worker = Worker(worker_cfg)
    worker_stats = worker.run()

    bt.join(timeout=cfg.scenario_timeout + 3)
    if broker_error:
        log.error("Broker error: %s", broker_error[0])

    bstats = broker_stats_holder[0] if broker_stats_holder else broker.stats

    result = RunResult(
        scenario=scenario.name,
        worker_expired=bstats.worker_expired,
        send_succeeded=worker_stats.send_succeeded,
        raw_message_received=bstats.raw_message_received,
        result_accepted=bstats.result_accepted,
        result_rejected=bstats.result_rejected,
        result_missing=bstats.result_missing,
    )
    return result


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
    return summaries


def print_summary(summaries: dict[str, ScenarioSummary]) -> None:
    header = (
        f"{'strategy':<45} {'runs':>5} {'expired':>8} {'send_ok':>8} "
        f"{'raw_rx':>7} {'accepted':>9} {'rejected':>9} {'missing':>8} "
        f"{'success%':>9}"
    )
    print(header)
    print("-" * len(header))
    for name in sorted(summaries.keys()):
        s = summaries[name]
        print(
            f"{s.scenario:<45} {s.runs:>5} {s.worker_expired:>8} "
            f"{s.send_succeeded:>8} {s.raw_message_received:>7} "
            f"{s.result_accepted:>9} {s.result_rejected:>9} "
            f"{s.result_missing:>8} {s.success_rate:>8.1f}%"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ZMQ reply-loss scenarios")
    parser.add_argument("--runs", type=int, default=20)
    parser.add_argument("--jitter", type=float, default=0.0)
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.verbose:
        global log
        log = setup_logging("scenarios", "INFO")

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
    )

    scenarios = DEFAULT_SCENARIOS
    if args.scenarios:
        names = set(args.scenarios)
        scenarios = [s for s in DEFAULT_SCENARIOS if s.name in names]

    print(
        f"Running {args.runs} iterations per scenario "
        f"(heartbeat={heartbeat_interval}s, timeout={worker_timeout}s, "
        f"job={base_job_duration}s, jitter={args.jitter})"
    )
    print()

    all_results: list[RunResult] = []
    for scenario in scenarios:
        for i in range(args.runs):
            result = run_single_scenario(
                scenario, cfg, heartbeat_interval, worker_timeout, base_job_duration,
                quiet=not args.verbose,
            )
            all_results.append(result)
            if args.verbose:
                print(f"  [{scenario.name} run {i+1}] {result}")

    summaries = aggregate_results(all_results)
    print_summary(summaries)


if __name__ == "__main__":
    main()
