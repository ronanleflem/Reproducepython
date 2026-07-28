"""Configuration centralisée pour le reproducer ZMQ."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass, field
from typing import Literal

# --- Valeurs accélérées (défaut) ---
HEARTBEAT_INTERVAL = 0.5
LIVENESS_MULTIPLIER = 3
WORKER_TIMEOUT = HEARTBEAT_INTERVAL * LIVENESS_MULTIPLIER  # 1.5 s
JOB_DURATION = 5.0

# --- Scénario production ---
PROD_HEARTBEAT_INTERVAL = 5.0
PROD_LIVENESS_MULTIPLIER = 12
PROD_WORKER_TIMEOUT = PROD_HEARTBEAT_INTERVAL * PROD_LIVENESS_MULTIPLIER  # 60 s
PROD_JOB_DURATION = 540.0

# --- Endpoints ---
DEFAULT_BROKER_ENDPOINT = "tcp://127.0.0.1:5555"
DEFAULT_MONITOR_ENDPOINT = "inproc://socket-monitor"

# --- Types ---
ResultPolicy = Literal["strict", "job-id"]
ReconnectMode = Literal["light", "full"]
Strategy = Literal[
    "no-reconnect",
    "manual-reconnect-immediate",
    "manual-reconnect-heartbeat",
    "manual-reconnect-ready-ack",
    "auto-reconnect-only",
    "reconnect-then-delay",
]
ThreadingMode = Literal["blocking-network-loop", "separate-job-thread"]
SendMode = Literal["blocking", "dontwait"]


@dataclass
class ZmqSocketOptions:
    immediate: int = 0
    linger: int = 0
    reconnect_ivl: int = 100
    reconnect_ivl_max: int = 0
    sndtimeo: int = -1
    rcvtimeo: int = -1
    identity: str = ""

    def apply(self, socket, identity: str | None = None) -> None:
        import zmq

        socket.setsockopt(zmq.IMMEDIATE, self.immediate)
        socket.setsockopt(zmq.LINGER, self.linger)
        socket.setsockopt(zmq.RECONNECT_IVL, self.reconnect_ivl)
        if self.reconnect_ivl_max > 0:
            socket.setsockopt(zmq.RECONNECT_IVL_MAX, self.reconnect_ivl_max)
        if self.sndtimeo >= 0:
            socket.setsockopt(zmq.SNDTIMEO, self.sndtimeo)
        if self.rcvtimeo >= 0:
            socket.setsockopt(zmq.RCVTIMEO, self.rcvtimeo)
        ident = identity or self.identity
        if ident:
            socket.setsockopt(zmq.IDENTITY, ident.encode("utf-8"))


@dataclass
class BrokerConfig:
    endpoint: str = DEFAULT_BROKER_ENDPOINT
    heartbeat_interval: float = HEARTBEAT_INTERVAL
    liveness_multiplier: float = LIVENESS_MULTIPLIER
    result_policy: ResultPolicy = "strict"
    zmq_options: ZmqSocketOptions = field(default_factory=ZmqSocketOptions)
    monitor_enabled: bool = True
    log_level: str = "INFO"

    @property
    def worker_timeout(self) -> float:
        return self.heartbeat_interval * self.liveness_multiplier


@dataclass
class WorkerConfig:
    broker_endpoint: str = DEFAULT_BROKER_ENDPOINT
    worker_id: str = "worker-1"
    strategy: Strategy = "no-reconnect"
    reconnect_mode: ReconnectMode = "full"
    threading_mode: ThreadingMode = "blocking-network-loop"
    send_mode: SendMode = "blocking"
    job_duration: float = JOB_DURATION
    heartbeat_interval: float = HEARTBEAT_INTERVAL
    liveness_multiplier: float = LIVENESS_MULTIPLIER
    reconnect_delay: float = 0.0
    reply_delay: float = 0.0
    network_delay: float = 0.0
    result_ack_timeout: float = 2.0
    ready_ack_timeout: float = 3.0
    echo_heartbeat: bool = False
    result_payload_bytes: int = 0
    payload_seed: int = 0
    zmq_options: ZmqSocketOptions = field(default_factory=ZmqSocketOptions)
    monitor_enabled: bool = True
    log_level: str = "INFO"

    @property
    def worker_timeout(self) -> float:
        return self.heartbeat_interval * self.liveness_multiplier


@dataclass
class ScenarioConfig:
    runs: int = 20
    jitter: float = 0.0
    production: bool = False
    broker_endpoint: str = DEFAULT_BROKER_ENDPOINT
    result_policy: ResultPolicy = "strict"
    threading_mode: ThreadingMode = "blocking-network-loop"
    reconnect_mode: ReconnectMode = "full"
    zmq_immediate: int = 0
    zmq_linger: int = 0
    zmq_reconnect_ivl: int = 100
    zmq_reconnect_ivl_max: int = 0
    zmq_sndtimeo: int = -1
    scenario_timeout: float = 15.0
    seed: int | None = None
    monitor_enabled: bool = True
    timeline_enabled: bool = True
    report_path: str = ""
    result_payload_bytes: int = 0
    payload_jitter: float = 0.0
    trace_dir: str = ""
    trace_label: str = ""
    echo_heartbeat: bool = False
    ready_ack_timeout: float = 3.0


@dataclass
class CampaignConfig:
    """Configuration pour campagnes de stress à grande échelle."""

    runs: int = 300
    seed: int = 42
    jitter: float = 0.15
    production: bool = False
    result_policy: ResultPolicy = "strict"
    threading_mode: ThreadingMode = "blocking-network-loop"
    scenario_timeout: float = 20.0
    zmq_immediate: int = 0
    zmq_linger: int = 0
    zmq_reconnect_ivl: int = 100
    zmq_reconnect_ivl_max: int = 0
    zmq_sndtimeo: int = -1
    monitor_enabled: bool = True
    timeline_enabled: bool = True
    report_path: str = "campaign_report.json"
    print_timelines: bool = False
    verbose: bool = False
    avg_payload_bytes: int = 20 * 1024 * 1024
    payload_jitter: float = 0.15
    trace_dir: str = ""
    trace_label: str = ""


def apply_production_timings(cfg: BrokerConfig | WorkerConfig) -> None:
    cfg.heartbeat_interval = PROD_HEARTBEAT_INTERVAL
    cfg.liveness_multiplier = PROD_LIVENESS_MULTIPLIER
    if isinstance(cfg, WorkerConfig):
        cfg.job_duration = PROD_JOB_DURATION


def add_zmq_options_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--zmq-immediate", type=int, default=0, choices=[0, 1])
    parser.add_argument("--zmq-linger", type=int, default=0)
    parser.add_argument("--zmq-reconnect-ivl", type=int, default=100)
    parser.add_argument("--zmq-reconnect-ivl-max", type=int, default=0)
    parser.add_argument("--zmq-sndtimeo", type=int, default=-1, help="ms, -1 = blocking")


def zmq_options_from_args(args: argparse.Namespace) -> ZmqSocketOptions:
    return ZmqSocketOptions(
        immediate=args.zmq_immediate,
        linger=args.zmq_linger,
        reconnect_ivl=args.zmq_reconnect_ivl,
        reconnect_ivl_max=args.zmq_reconnect_ivl_max,
        sndtimeo=args.zmq_sndtimeo,
    )


def env_log_level() -> str:
    return os.environ.get("LOG_LEVEL", "INFO").upper()
