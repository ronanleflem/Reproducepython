#!/usr/bin/env python3
"""
Télécharge une plage de chapitres MangaDex (v2 — lot + reprise).

Exemple : python3 download_batch.py --from 1529 --to 1531
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from mdx_common import (
    MARTIAL_PEAK_MANGA_ID,
    chapter_ready,
    chapter_sort_key,
    chapter_zip_path,
    download_chapter,
    fetch_chapter_meta,
    finalize_chapter,
    load_aggregate_chapters,
    safe_dir_name,
)


def parse_bound(value: str) -> float:
    try:
        return float(value)
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Numéro invalide: {value!r}") from e


def chapters_in_range(
    index: dict[str, str], start: float, end: float
) -> list[tuple[str, str]]:
    if start > end:
        start, end = end, start
    pairs = [(num, cid) for num, cid in index.items() if start <= chapter_sort_key(num) <= end]
    pairs.sort(key=lambda t: chapter_sort_key(t[0]))
    return pairs


def find_existing_dir(out_root: Path, chapter_num: str) -> Path | None:
    prefix = f"chapter-{chapter_num}"
    exact = out_root / prefix
    if exact.is_dir():
        return exact
    matches = sorted(p for p in out_root.glob(f"{prefix}-*") if p.is_dir())
    return matches[0] if matches else None


def find_existing_zip(out_root: Path, chapter_num: str) -> Path | None:
    prefix = f"chapter-{chapter_num}"
    exact = out_root / f"{prefix}.zip"
    if exact.is_file():
        return exact
    matches = sorted(out_root.glob(f"{prefix}-*.zip"))
    return matches[0] if matches else None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Télécharge une plage de chapitres MangaDex (v2)."
    )
    parser.add_argument("--manga-id", default=MARTIAL_PEAK_MANGA_ID)
    parser.add_argument("--from", dest="from_", type=parse_bound, required=True, metavar="N")
    parser.add_argument("--to", type=parse_bound, required=True, metavar="N")
    parser.add_argument("--lang", default="en")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("downloads/martial-peak"),
    )
    parser.add_argument("--data-saver", action="store_true")
    parser.add_argument("--delay", type=float, default=0.35, help="Pause entre pages.")
    parser.add_argument(
        "--chapter-delay",
        type=float,
        default=1.0,
        help="Pause entre chapitres (limite API at-home).",
    )
    parser.add_argument(
        "--no-skip-complete",
        action="store_true",
        help="Retélécharger même si le dossier semble complet.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Passer au chapitre suivant en cas d'erreur.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Lister les chapitres sans télécharger.",
    )
    parser.add_argument(
        "--no-zip",
        action="store_true",
        help="Ne pas créer de fichier .zip par chapitre.",
    )
    args = parser.parse_args()

    print(f"Index aggregate ({args.lang})…")
    index = load_aggregate_chapters(args.manga_id, args.lang)
    planned = chapters_in_range(index, args.from_, args.to)
    if not planned:
        raise SystemExit(
            f"Aucun chapitre entre {args.from_} et {args.to} (lang={args.lang})."
        )

    print(f"{len(planned)} chapitre(s) dans la plage.")
    ok, skipped, failed = 0, 0, 0
    errors: list[str] = []

    for i, (chapter_num, chapter_id) in enumerate(planned, start=1):
        print(f"\n[{i}/{len(planned)}] Chapitre {chapter_num} ({chapter_id})")
        existing = find_existing_dir(args.out, chapter_num)
        existing_zip = find_existing_zip(args.out, chapter_num)
        if existing and not existing_zip:
            existing_zip = chapter_zip_path(existing)
        if not args.no_skip_complete and chapter_ready(existing, existing_zip):
            target = existing_zip or (chapter_zip_path(existing) if existing else None)
            print(f"  skip (complet) → {target or existing}")
            skipped += 1
            continue
        if (
            not args.no_skip_complete
            and existing
            and not args.no_zip
        ):
            zp = finalize_chapter(existing, make_zip=True)
            if zp and chapter_ready(existing, zp):
                print(f"  skip (zip créé) → {zp}")
                skipped += 1
                continue

        if args.dry_run:
            print("  dry-run")
            ok += 1
            continue

        try:
            _, title = fetch_chapter_meta(chapter_id)
            dest = args.out / safe_dir_name(chapter_num, title)
            if existing and existing.is_dir():
                dest = existing
            print(f"  → {dest}")
            download_chapter(
                chapter_id,
                dest,
                data_saver=args.data_saver,
                delay_s=args.delay,
                make_zip=not args.no_zip,
            )
            ok += 1
        except Exception as e:
            failed += 1
            msg = f"ch.{chapter_num}: {e}"
            errors.append(msg)
            print(f"  ERREUR: {e}", file=sys.stderr)
            if not args.continue_on_error:
                break

        if i < len(planned):
            time.sleep(args.chapter_delay)

    print(
        f"\nRésumé: {ok} ok, {skipped} ignorés (déjà complets), {failed} échec(s)."
    )
    if errors:
        print("Détails:", file=sys.stderr)
        for line in errors:
            print(f"  - {line}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
