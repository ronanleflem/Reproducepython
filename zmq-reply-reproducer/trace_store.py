"""Archivage des traces de test pour reprise ultérieure."""

from __future__ import annotations

import json
import logging
import re
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, TextIO

from timeline import RunOutcome, TimelineCollector, TransportCase

DEFAULT_TRACE_DIR = "traces"
LOG_FORMAT = "%(asctime)s.%(msecs)03d %(levelname)s [%(name)s] %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"

_TRACE_LOGGERS = ("broker", "worker", "scenarios", "campaign", "timeline")


class _EnumEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, Enum):
            return o.value
        return super().default(o)


def _slug(text: str, max_len: int = 48) -> str:
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", text.strip())
    return s[:max_len].strip("_") or "run"


def outcome_to_dict(outcome: RunOutcome) -> dict[str, Any]:
    d = asdict(outcome)
    tc = d.get("transport_case")
    if isinstance(tc, TransportCase):
        d["transport_case"] = tc.value
    return d


@dataclass
class TraceSessionInfo:
    session_id: str
    path: str
    source: str
    label: str = ""
    created_at: str = ""
    seed: int | None = None
    command: str = ""
    config: dict[str, Any] = field(default_factory=dict)
    run_count: int = 0


class TraceRunCapture:
    """Capture les logs d'un run unique dans un fichier."""

    def __init__(self, run_dir: Path, *, quiet: bool = True) -> None:
        self.run_dir = run_dir
        self.quiet = quiet
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = run_dir / "run.log"
        self._handler = logging.FileHandler(self.log_path, encoding="utf-8")
        self._handler.setLevel(logging.INFO)
        self._handler.setFormatter(
            logging.Formatter(fmt=LOG_FORMAT, datefmt=LOG_DATEFMT)
        )
        self._attached: list[logging.Logger] = []
        self._saved_levels: list[tuple[logging.Logger, int]] = []
        self._saved_stream_levels: list[tuple[logging.Handler, int]] = []

    def __enter__(self) -> TraceRunCapture:
        for name in _TRACE_LOGGERS:
            logger = logging.getLogger(name)
            self._saved_levels.append((logger, logger.level))
            logger.setLevel(logging.INFO)
            for h in logger.handlers:
                if isinstance(h, logging.StreamHandler) and not isinstance(
                    h, logging.FileHandler
                ):
                    self._saved_stream_levels.append((h, h.level))
                    h.setLevel(logging.ERROR if self.quiet else logging.INFO)
            logger.addHandler(self._handler)
            self._attached.append(logger)
        return self

    def __exit__(self, *args: object) -> None:
        for logger in self._attached:
            logger.removeHandler(self._handler)
        for logger, level in self._saved_levels:
            logger.setLevel(level)
        for handler, level in self._saved_stream_levels:
            handler.setLevel(level)
        self._handler.close()

    def save(
        self,
        *,
        outcome: RunOutcome,
        timeline: TimelineCollector,
        extra: dict[str, Any] | None = None,
    ) -> None:
        run_data = outcome_to_dict(outcome)
        if extra:
            run_data["extra"] = extra
        with open(self.run_dir / "run.json", "w", encoding="utf-8") as f:
            json.dump(run_data, f, indent=2, ensure_ascii=False, cls=_EnumEncoder)

        events = [asdict(e) for e in timeline.all_events()]
        with open(self.run_dir / "timeline.json", "w", encoding="utf-8") as f:
            json.dump(events, f, indent=2, ensure_ascii=False)

        with open(self.run_dir / "timeline.txt", "w", encoding="utf-8") as f:
            f.write(self._format_timeline(timeline, outcome.job_id))

        readme = self._run_readme(outcome)
        (self.run_dir / "README.txt").write_text(readme, encoding="utf-8")

    @staticmethod
    def _format_timeline(timeline: TimelineCollector, job_id: str | None) -> str:
        events = sorted(
            [e for e in timeline.all_events() if not job_id or e.job_id == job_id],
            key=lambda e: e.mono_ts,
        )
        lines = [f"=== Timeline job_id={job_id or 'ALL'} ==="]
        for e in events:
            lines.append(
                f"  [{e.mono_ts:12.6f}] {e.event:<35} {e.correlation_line()}"
            )
            if e.details:
                lines.append(f"      details={e.details}")
        return "\n".join(lines) + "\n"

    @staticmethod
    def _run_readme(outcome: RunOutcome) -> str:
        tc = (
            outcome.transport_case.value
            if isinstance(outcome.transport_case, TransportCase)
            else outcome.transport_case
        )
        lines = [
            f"scenario     : {outcome.scenario}",
            f"run_id       : {outcome.run_id}",
            f"job_id       : {outcome.job_id}",
            f"transport    : {tc}",
            f"strategy     : {outcome.strategy}",
            f"reconnect    : {outcome.reconnect_mode}",
            f"send_ok      : {outcome.send_succeeded}",
            f"raw_rx       : {outcome.raw_message_received}",
            f"accepted     : {outcome.result_accepted}",
            f"rejected     : {outcome.result_rejected}",
            f"ack_rx       : {outcome.result_ack_received}",
            f"reject_reason: {outcome.reject_reason or 'N/A'}",
            "",
            "Fichiers:",
            "  run.json       — résultat structuré",
            "  timeline.json  — événements chronologiques",
            "  timeline.txt   — timeline lisible",
            "  run.log        — logs complets du run",
        ]
        return "\n".join(lines) + "\n"


class TraceSession:
    """Session d'archivage : un dossier par lancement de tests."""

    def __init__(
        self,
        base_dir: str | Path = DEFAULT_TRACE_DIR,
        *,
        source: str = "scenarios",
        label: str = "",
        seed: int | None = None,
        command: str = "",
        config: dict[str, Any] | None = None,
    ) -> None:
        self.base_dir = Path(base_dir)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        parts = [ts]
        if seed is not None:
            parts.append(f"seed{seed}")
        if label:
            parts.append(_slug(label))
        self.session_id = "_".join(parts)
        self.dir = self.base_dir / self.session_id
        self.runs_dir = self.dir / "runs"
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.info = TraceSessionInfo(
            session_id=self.session_id,
            path=str(self.dir),
            source=source,
            label=label,
            created_at=datetime.now(timezone.utc).isoformat(),
            seed=seed,
            command=command,
            config=config or {},
        )
        self._run_counter = 0
        self._combined_log = open(self.dir / "session.log", "w", encoding="utf-8")
        self._stdout_tee: _StdoutTee | None = None

    def enable_stdout_tee(self) -> None:
        """Duplique stdout vers session.log (tableaux, prints)."""
        if self._stdout_tee is None:
            self._stdout_tee = _StdoutTee(sys.stdout, self._combined_log)
            sys.stdout = self._stdout_tee

    def begin_run(self, scenario: str, run_index: int, *, quiet: bool = True) -> TraceRunCapture:
        self._run_counter += 1
        name = f"{self._run_counter:03d}_{_slug(scenario)}_r{run_index}"
        return TraceRunCapture(self.runs_dir / name, quiet=quiet)

    def finalize(
        self,
        *,
        summary_text: str = "",
        summary_data: dict[str, Any] | None = None,
        outcomes: list[RunOutcome] | None = None,
    ) -> Path:
        self.info.run_count = self._run_counter
        with open(self.dir / "session.json", "w", encoding="utf-8") as f:
            json.dump(asdict(self.info), f, indent=2, ensure_ascii=False)

        if summary_text:
            (self.dir / "summary.txt").write_text(summary_text, encoding="utf-8")
        if summary_data:
            with open(self.dir / "summary.json", "w", encoding="utf-8") as f:
                json.dump(summary_data, f, indent=2, ensure_ascii=False, cls=_EnumEncoder)
        if outcomes:
            rows = [outcome_to_dict(o) for o in outcomes]
            with open(self.dir / "outcomes.json", "w", encoding="utf-8") as f:
                json.dump(rows, f, indent=2, ensure_ascii=False, cls=_EnumEncoder)

        readme = self._session_readme()
        (self.dir / "README.txt").write_text(readme, encoding="utf-8")

        if self._stdout_tee:
            sys.stdout = self._stdout_tee.original
            self._stdout_tee = None
        self._combined_log.close()
        return self.dir

    def _session_readme(self) -> str:
        lines = [
            "SESSION DE TEST ZMQ — ARCHIVE",
            "=" * 40,
            f"session_id : {self.session_id}",
            f"created_at : {self.info.created_at}",
            f"source     : {self.info.source}",
            f"label      : {self.info.label or 'N/A'}",
            f"seed       : {self.info.seed}",
            f"runs       : {self.info.run_count}",
            f"command    : {self.info.command}",
            "",
            "Contenu:",
            "  session.json   — métadonnées",
            "  session.log    — sortie console du lancement",
            "  summary.txt    — tableau de synthèse",
            "  outcomes.json  — tous les runs (JSON)",
            "  runs/          — un dossier par run",
            "",
            "Rejouer un scénario:",
            "  python3 list_traces.py show <session_id>/<run_dir>",
            "  python3 list_traces.py replay-hint <session_id>/<run_dir>",
            "",
            "Exemple:",
            f"  python3 list_traces.py show {self.session_id}/001_...",
        ]
        return "\n".join(lines) + "\n"


class _StdoutTee:
    def __init__(self, original: TextIO, log_file: TextIO) -> None:
        self.original = original
        self.log_file = log_file

    def write(self, data: str) -> int:
        self.original.write(data)
        self.log_file.write(data)
        self.log_file.flush()
        return len(data)

    def flush(self) -> None:
        self.original.flush()
        self.log_file.flush()

    def isatty(self) -> bool:
        return self.original.isatty()


def list_sessions(base_dir: str | Path = DEFAULT_TRACE_DIR) -> list[TraceSessionInfo]:
    root = Path(base_dir)
    if not root.is_dir():
        return []
    sessions: list[TraceSessionInfo] = []
    for path in sorted(root.iterdir(), reverse=True):
        meta = path / "session.json"
        if not meta.is_file():
            continue
        with open(meta, encoding="utf-8") as f:
            data = json.load(f)
        sessions.append(TraceSessionInfo(**data))
    return sessions


def print_sessions_table(base_dir: str | Path = DEFAULT_TRACE_DIR) -> None:
    sessions = list_sessions(base_dir)
    if not sessions:
        print(f"Aucune trace dans {base_dir}/")
        return
    print(f"{'session_id':<45} {'source':<12} {'runs':>5}  {'seed':>6}  label")
    print("-" * 90)
    for s in sessions:
        print(
            f"{s.session_id:<45} {s.source:<12} {s.run_count:>5}  "
            f"{s.seed if s.seed is not None else 'N/A':>6}  {s.label}"
        )


def _resolve_run_dir(base_dir: str | Path, session_run: str) -> Path:
    """Résout session_id/run_name vers traces/SESSION/runs/run_name."""
    base = Path(base_dir)
    raw = Path(session_run)
    if raw.is_absolute() and (raw / "run.json").is_file():
        return raw
    candidate = base / session_run
    if candidate.is_dir() and (candidate / "run.json").is_file():
        return candidate
    if "/" in session_run:
        session_id, run_name = session_run.split("/", 1)
        nested = base / session_id / "runs" / run_name
        if nested.is_dir():
            return nested
    return candidate


def show_run(base_dir: str | Path, session_run: str) -> None:
    """Affiche un run archivé. session_run = 'SESSION_ID/003_scenario' ou chemin complet."""
    run_dir = _resolve_run_dir(base_dir, session_run)
    if not (run_dir / "run.json").is_file():
        print(f"Run introuvable: {session_run} (cherché: {run_dir})")
        return
    readme = run_dir / "README.txt"
    if readme.is_file():
        print(readme.read_text(encoding="utf-8"))
    timeline_txt = run_dir / "timeline.txt"
    if timeline_txt.is_file():
        print(timeline_txt.read_text(encoding="utf-8"))
    log_file = run_dir / "run.log"
    if log_file.is_file():
        print("--- run.log (dernières 40 lignes) ---")
        lines = log_file.read_text(encoding="utf-8").splitlines()
        for line in lines[-40:]:
            print(line)


def replay_hint(base_dir: str | Path, session_run: str) -> None:
    run_dir = _resolve_run_dir(base_dir, session_run)
    if not (run_dir / "run.json").is_file():
        print(f"Run introuvable: {session_run}")
        return
    with open(run_dir / "run.json", encoding="utf-8") as f:
        run = json.load(f)
    params = run.get("parameters") or run.get("extra") or {}
    seed = run.get("seed", 42)
    scenario = run.get("scenario", "")
    payload = int(params.get("result_payload_bytes", 0))
    policy = params.get("result_policy", "strict")
    print("Commande suggérée pour rejouer ce run :")
    print()
    cmd = (
        f"python3 run_scenarios.py --runs 1 --seed {seed} "
        f"--scenarios {scenario} --save-traces --trace-label replay"
    )
    if payload:
        cmd += f" --result-payload-bytes {payload}"
    if policy and policy != "strict":
        cmd += f" --result-policy {policy}"
    cmd += " --verbose"
    print(cmd)
    print()
    print(f"Archivé dans : {run_dir}")
