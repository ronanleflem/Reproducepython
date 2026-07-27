#!/usr/bin/env python3
"""Parcourir et consulter les traces de test archivées."""

from __future__ import annotations

import argparse

from trace_store import (
    DEFAULT_TRACE_DIR,
    print_sessions_table,
    replay_hint,
    show_run,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consulter les traces de test ZMQ")
    parser.add_argument(
        "--trace-dir",
        default=DEFAULT_TRACE_DIR,
        help="Répertoire racine des traces",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="Lister les sessions archivées")

    show_p = sub.add_parser("show", help="Afficher un run (session_id/run_dir)")
    show_p.add_argument("path", help="Ex: 20260727T230000Z_seed42/001_scenario")

    replay_p = sub.add_parser("replay-hint", help="Commande pour rejouer un run")
    replay_p.add_argument("path", help="Ex: 20260727T230000Z_seed42/001_scenario")

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "list":
        print_sessions_table(args.trace_dir)
    elif args.command == "show":
        show_run(args.trace_dir, args.path)
    elif args.command == "replay-hint":
        replay_hint(args.trace_dir, args.path)


if __name__ == "__main__":
    main()
