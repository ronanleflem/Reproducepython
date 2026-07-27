"""Utilitaires de logging structuré."""

from __future__ import annotations

import logging
import sys


def setup_logging(name: str, level: str = "INFO") -> logging.Logger:
    log = logging.getLogger(name)
    log.setLevel(getattr(logging, level.upper(), logging.INFO))
    if log.handlers:
        for handler in log.handlers:
            handler.setLevel(getattr(logging, level.upper(), logging.INFO))
        return log
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s.%(msecs)03d %(levelname)s [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    log.addHandler(handler)
    log.propagate = False
    return log
