#!/usr/bin/env python3
"""Matrice de combinaisons des 7 leviers d'amélioration ZMQ."""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from config import HEARTBEAT_INTERVAL, JOB_DURATION, LIVENESS_MULTIPLIER, ScenarioConfig
from logging_setup import setup_logging
from run_scenarios import ScenarioDef, aggregate_results, run_single_scenario
from trace_store import TraceSession

log = setup_logging("combinations", "WARNING")


@dataclass
class CombinationDef:
    """Une combinaison de leviers à tester."""

    name: str
    label: str
    threading_mode: str = "blocking-network-loop"
    echo_heartbeat: bool = False
    strategy: str = "manual-reconnect-heartbeat"
    ready_ack_timeout: float = 3.0
    result_policy: str = "job-id"
    reconnect_mode: str = "full"
    zmq_immediate: int = 0

    def flags_on(self) -> list[str]:
        flags = []
        if self.threading_mode == "separate-job-thread":
            flags.append("separate-thread")
        if self.echo_heartbeat:
            flags.append("echo-hb")
        if self.strategy != "manual-reconnect-heartbeat":
            flags.append(self.strategy.replace("manual-reconnect-", ""))
        if self.ready_ack_timeout > 3.0:
            flags.append(f"ack{int(self.ready_ack_timeout)}s")
        if self.result_policy == "strict":
            flags.append("strict")
        if self.reconnect_mode == "light":
            flags.append("light")
        if self.zmq_immediate:
            flags.append("zmq-imm")
        return flags or ["baseline"]


# Référence = Série #1 (blocking + heartbeat + job-id)
BASELINE = CombinationDef(
    name="c00_baseline",
    label="Référence Série #1",
)

COMBINATIONS: list[CombinationDef] = [
    BASELINE,
    # --- Levier seul (un facteur changé) ---
    CombinationDef(
        name="c01_separate_thread",
        label="Levier 1 : separate-job-thread",
        threading_mode="separate-job-thread",
    ),
    CombinationDef(
        name="c02_echo_no_reconnect",
        label="Levier 2 : echo heartbeat + no-reconnect",
        threading_mode="separate-job-thread",
        echo_heartbeat=True,
        strategy="no-reconnect",
    ),
    CombinationDef(
        name="c03_strategy_immediate",
        label="Levier 3a : reconnect immediate",
        strategy="manual-reconnect-immediate",
    ),
    CombinationDef(
        name="c04_strategy_ready_ack",
        label="Levier 3b : reconnect ready-ack",
        strategy="manual-reconnect-ready-ack",
    ),
    CombinationDef(
        name="c05_ready_ack_10s",
        label="Levier 4 : timeout READY_ACK 10s",
        strategy="manual-reconnect-ready-ack",
        ready_ack_timeout=10.0,
    ),
    CombinationDef(
        name="c06_policy_strict",
        label="Levier 5 : politique strict",
        result_policy="strict",
    ),
    CombinationDef(
        name="c07_reconnect_light",
        label="Levier 6 : reconnect light",
        reconnect_mode="light",
    ),
    CombinationDef(
        name="c08_zmq_immediate",
        label="Levier 7 : ZMQ_IMMEDIATE=1",
        zmq_immediate=1,
    ),
    # --- Paires prometteuses ---
    CombinationDef(
        name="c09_sep_heartbeat",
        label="Pair : separate + heartbeat",
        threading_mode="separate-job-thread",
    ),
    CombinationDef(
        name="c10_sep_ready_ack_10",
        label="Pair : separate + ready-ack 10s",
        threading_mode="separate-job-thread",
        strategy="manual-reconnect-ready-ack",
        ready_ack_timeout=10.0,
    ),
    CombinationDef(
        name="c11_echo_heartbeat_strategy",
        label="Pair : echo + heartbeat reconnect",
        threading_mode="separate-job-thread",
        echo_heartbeat=True,
    ),
    CombinationDef(
        name="c12_sep_light",
        label="Pair : separate + reconnect light",
        threading_mode="separate-job-thread",
        reconnect_mode="light",
    ),
    # --- Stacks (tout cumuler) ---
    CombinationDef(
        name="c13_stack_transport",
        label="Stack transport : sep + echo + heartbeat",
        threading_mode="separate-job-thread",
        echo_heartbeat=True,
    ),
    CombinationDef(
        name="c14_stack_no_reconnect",
        label="Stack idéal : sep + echo + no-reconnect",
        threading_mode="separate-job-thread",
        echo_heartbeat=True,
        strategy="no-reconnect",
    ),
    CombinationDef(
        name="c15_stack_full",
        label="Stack complet : sep + echo + ready-ack 10s + light",
        threading_mode="separate-job-thread",
        echo_heartbeat=True,
        strategy="manual-reconnect-ready-ack",
        ready_ack_timeout=10.0,
        reconnect_mode="light",
    ),
    CombinationDef(
        name="c16_kitchen_sink",
        label="Kitchen sink : tout sauf strict",
        threading_mode="separate-job-thread",
        echo_heartbeat=True,
        strategy="manual-reconnect-ready-ack",
        ready_ack_timeout=10.0,
        reconnect_mode="light",
        zmq_immediate=1,
    ),
]


@dataclass
class CombinationResult:
    combination: str
    label: str
    flags: list[str]
    runs: int = 0
    accepted: int = 0
    expired: int = 0
    case1: int = 0
    case2: int = 0
    case3: int = 0
    ack_received: int = 0
    outcomes: list[dict] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        return (self.accepted / self.runs * 100) if self.runs else 0.0


def combination_to_scenario(combo: CombinationDef) -> ScenarioDef:
    return ScenarioDef(
        name=combo.name,
        strategy=combo.strategy,
        reconnect_mode=combo.reconnect_mode,
        zmq_immediate=combo.zmq_immediate,
    )


def run_combination(
    combo: CombinationDef,
    runs: int,
    cfg: ScenarioConfig,
    rng,
    trace: TraceSession | None,
) -> CombinationResult:
    heartbeat_interval = HEARTBEAT_INTERVAL
    worker_timeout = heartbeat_interval * LIVENESS_MULTIPLIER
    base_job_duration = JOB_DURATION

    result = CombinationResult(
        combination=combo.name,
        label=combo.label,
        flags=combo.flags_on(),
    )

    scenario = combination_to_scenario(combo)
    combo_cfg = ScenarioConfig(
        runs=runs,
        jitter=cfg.jitter,
        production=cfg.production,
        result_policy=combo.result_policy,  # type: ignore[arg-type]
        threading_mode=combo.threading_mode,  # type: ignore[arg-type]
        reconnect_mode=combo.reconnect_mode,  # type: ignore[arg-type]
        zmq_immediate=combo.zmq_immediate,
        scenario_timeout=cfg.scenario_timeout,
        seed=cfg.seed,
        monitor_enabled=cfg.monitor_enabled,
        timeline_enabled=cfg.timeline_enabled,
        result_payload_bytes=cfg.result_payload_bytes,
        payload_jitter=cfg.payload_jitter,
        echo_heartbeat=combo.echo_heartbeat,
        ready_ack_timeout=combo.ready_ack_timeout,
    )

    for i in range(runs):
        run_result, outcome = run_single_scenario(
            scenario,
            combo_cfg,
            heartbeat_interval,
            worker_timeout,
            base_job_duration,
            rng,
            quiet=True,
            run_index=i,
            trace=trace,
        )
        result.runs += 1
        if run_result.result_accepted:
            result.accepted += 1
        if run_result.worker_expired:
            result.expired += 1
        if run_result.transport_case == "CASE1_send_ok_broker_never_received":
            result.case1 += 1
        elif run_result.transport_case == "CASE2_broker_received_then_rejected":
            result.case2 += 1
        elif "CASE3" in run_result.transport_case:
            result.case3 += 1
        if run_result.result_ack_received:
            result.ack_received += 1
        result.outcomes.append(
            {
                "accepted": run_result.result_accepted,
                "expired": run_result.worker_expired,
                "transport_case": run_result.transport_case,
                "ack_received": run_result.result_ack_received,
            }
        )

    return result


def print_ranking(results: list[CombinationResult], baseline_rate: float) -> None:
    ranked = sorted(results, key=lambda r: r.success_rate, reverse=True)
    header = (
        f"{'#':>3} {'combinaison':<28} {'flags':<35} "
        f"{'accept%':>8} {'Δ base':>7} {'exp':>4} {'c1':>4} {'c2':>4} {'c3':>4}"
    )
    print(header)
    print("-" * len(header))
    for i, r in enumerate(ranked, 1):
        delta = r.success_rate - baseline_rate
        flags = ",".join(r.flags)
        print(
            f"{i:>3} {r.combination:<28} {flags:<35} "
            f"{r.success_rate:>7.1f}% {delta:>+6.1f} "
            f"{r.expired:>4} {r.case1:>4} {r.case2:>4} {r.case3:>4}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Matrice de combinaisons des leviers ZMQ")
    parser.add_argument("--runs", type=int, default=10, help="Runs par combinaison")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--result-payload-bytes",
        type=int,
        default=20 * 1024 * 1024,
    )
    parser.add_argument("--payload-jitter", type=float, default=0.15)
    parser.add_argument("--scenario-timeout", type=float, default=35.0)
    parser.add_argument(
        "--combinations",
        nargs="*",
        help="Sous-ensemble (noms c00_… ou indices 0-16)",
    )
    parser.add_argument("--report", default="", help="Chemin JSON du rapport")
    parser.add_argument("--save-traces", action="store_true")
    parser.add_argument("--trace-label", default="combination-matrix")
    parser.add_argument("--no-monitor", action="store_true")
    return parser.parse_args()


def select_combinations(names: list[str] | None) -> list[CombinationDef]:
    if not names:
        return COMBINATIONS
    selected: list[CombinationDef] = []
    by_name = {c.name: c for c in COMBINATIONS}
    for n in names:
        if n in by_name:
            selected.append(by_name[n])
        elif n.isdigit() and int(n) < len(COMBINATIONS):
            selected.append(COMBINATIONS[int(n)])
        else:
            log.warning("Combinaison inconnue: %s", n)
    return selected or COMBINATIONS


def main() -> None:
    args = parse_args()
    import random

    rng = random.Random(args.seed)
    combos = select_combinations(args.combinations)
    trace_dir = "traces" if args.save_traces else ""

    cfg = ScenarioConfig(
        runs=args.runs,
        seed=args.seed,
        scenario_timeout=args.scenario_timeout,
        result_payload_bytes=args.result_payload_bytes,
        payload_jitter=args.payload_jitter,
        monitor_enabled=not args.no_monitor,
    )

    print(
        f"Matrice de combinaisons — {len(combos)} configs × {args.runs} runs "
        f"(seed={args.seed}, payload={args.result_payload_bytes}B)"
    )
    print()

    trace: TraceSession | None = None
    if trace_dir:
        trace = TraceSession(
            trace_dir,
            source="combinations",
            label=args.trace_label,
            seed=args.seed,
            command=" ".join(sys.argv),
            config={
                "runs_per_combo": args.runs,
                "combinations": [c.name for c in combos],
                "result_payload_bytes": args.result_payload_bytes,
            },
        )
        trace.enable_stdout_tee()

    all_results: list[CombinationResult] = []
    t0 = time.monotonic()
    for idx, combo in enumerate(combos):
        print(f"[{idx + 1}/{len(combos)}] {combo.name} — {combo.label}")
        cr = run_combination(combo, args.runs, cfg, rng, trace)
        all_results.append(cr)
        print(f"  → {cr.accepted}/{cr.runs} ({cr.success_rate:.0f}%) expired={cr.expired}")

    baseline_rate = next(
        (r.success_rate for r in all_results if r.combination == BASELINE.name),
        0.0,
    )
    print()
    print("=== CLASSEMENT (broker accepte RESULT) ===")
    print_ranking(all_results, baseline_rate)

    report = {
        "seed": args.seed,
        "runs_per_combination": args.runs,
        "baseline_success_rate": baseline_rate,
        "elapsed_seconds": time.monotonic() - t0,
        "combinations": [
            {
                **asdict(r),
                "success_rate": r.success_rate,
            }
            for r in sorted(all_results, key=lambda x: x.success_rate, reverse=True)
        ],
    }

    report_path = args.report or "combination_report.json"
    Path(report_path).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nRapport JSON : {report_path}")
    if trace:
        print(f"Traces : {trace.session_path}")


if __name__ == "__main__":
    main()
