#!/usr/bin/env python3
"""
Télécharge un chapitre MangaDex (v1 — un chapitre à la fois).

Martial Peak (EN) : https://mangadex.org/title/b1461071-bfbb-43e7-a5b6-a7ba5904649f
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from mdx_common import (
    MARTIAL_PEAK_MANGA_ID,
    chapter_zip_path,
    download_chapter,
    fetch_chapter_meta,
    resolve_chapter_from_aggregate,
    safe_dir_name,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Télécharge un chapitre MangaDex (v1 — Martial Peak par défaut)."
    )
    parser.add_argument(
        "--manga-id",
        default=MARTIAL_PEAK_MANGA_ID,
        help="UUID du titre (défaut: Martial Peak).",
    )
    parser.add_argument(
        "--chapter",
        type=str,
        help="Numéro de chapitre (ex. 1, 3844).",
    )
    parser.add_argument(
        "--chapter-id",
        help="UUID du chapitre (ignore --chapter si fourni).",
    )
    parser.add_argument(
        "--lang",
        default="en",
        help="Langue des chapitres (défaut: en).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("downloads/martial-peak"),
        help="Dossier de sortie racine.",
    )
    parser.add_argument(
        "--data-saver",
        action="store_true",
        help="Images compressées (data-saver) au lieu de la qualité data.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.35,
        help="Pause entre chaque page (respect rate-limit MangaDex).",
    )
    parser.add_argument(
        "--no-zip",
        action="store_true",
        help="Ne pas créer de .zip après le téléchargement.",
    )
    args = parser.parse_args()

    if not args.chapter_id and not args.chapter:
        parser.error("Indique --chapter NUM ou --chapter-id UUID.")

    if args.chapter_id:
        chapter_id = args.chapter_id
        chapter_num, title = fetch_chapter_meta(chapter_id)
        if args.chapter:
            chapter_num = args.chapter
    else:
        try:
            chapter_id, _ = resolve_chapter_from_aggregate(
                args.manga_id, args.chapter, args.lang
            )
        except LookupError as e:
            raise SystemExit(str(e)) from e
        chapter_num, title = fetch_chapter_meta(chapter_id)
        if not chapter_num or chapter_num == "unknown":
            chapter_num = args.chapter

    dest = args.out / safe_dir_name(str(chapter_num), title)
    print(f"Chapitre {chapter_num} ({chapter_id}) → {dest}")
    n = download_chapter(
        chapter_id,
        dest,
        data_saver=args.data_saver,
        delay_s=args.delay,
        make_zip=not args.no_zip,
    )
    zip_msg = ""
    if not args.no_zip:
        zip_msg = f", zip: {chapter_zip_path(dest)}"
    print(f"Terminé: {n} page(s) dans {dest}{zip_msg}")


if __name__ == "__main__":
    main()
