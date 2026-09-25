#!/usr/bin/env python3
"""
Télécharge un chapitre MangaDex (v1 — un chapitre à la fois).

Martial Peak (EN) : https://mangadex.org/title/b1461071-bfbb-43e7-a5b6-a7ba5904649f
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

API_BASE = "https://api.mangadex.org"
MARTIAL_PEAK_MANGA_ID = "b1461071-bfbb-43e7-a5b6-a7ba5904649f"
USER_AGENT = "MartialPeakDownloader/1.0 (personal script; +https://mangadex.org)"


def api_get(path: str, params: dict | None = None) -> dict:
    url = f"{API_BASE}{path}"
    if params:
        url = f"{url}?{urllib.parse.urlencode(params, doseq=True)}"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())


def resolve_chapter_from_aggregate(
    manga_id: str, chapter_number: str, lang: str
) -> tuple[str, str | None]:
    """Résout numéro → (chapter_id, titre) via /aggregate (rapide même avec 3000+ chapitres)."""
    payload = api_get(
        f"/manga/{manga_id}/aggregate",
        {"translatedLanguage[]": lang},
    )
    for vol in payload.get("volumes", {}).values():
        chapters = vol.get("chapters") or {}
        entry = chapters.get(chapter_number)
        if not entry:
            continue
        if entry.get("isUnavailable"):
            continue
        chapter_id = entry["id"]
        # Titre non présent dans aggregate ; optionnel via /chapter/{id}
        return chapter_id, None

    raise SystemExit(
        f"Aucun chapitre {chapter_number!r} en {lang} pour le manga {manga_id}."
    )


def fetch_chapter_meta(chapter_id: str) -> tuple[str, str | None]:
    payload = api_get(f"/chapter/{chapter_id}")
    attrs = payload["data"]["attributes"]
    return attrs.get("chapter") or "unknown", attrs.get("title") or None


def chapter_at_home(chapter_id: str) -> dict:
    return api_get(f"/at-home/server/{chapter_id}")


def safe_dir_name(chapter: str, title: str | None) -> str:
    label = f"chapter-{chapter}"
    if title:
        slug = re.sub(r"[^\w\s-]", "", title, flags=re.UNICODE).strip()
        slug = re.sub(r"[-\s]+", "-", slug)
        if slug:
            label = f"{label}-{slug[:60]}"
    return label


def download_file(url: str, dest: Path) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=120) as resp:
        dest.write_bytes(resp.read())


def download_chapter(
    chapter_id: str,
    out_dir: Path,
    *,
    data_saver: bool = False,
    delay_s: float = 0.35,
) -> int:
    home = chapter_at_home(chapter_id)
    base = home["baseUrl"]
    ch = home["chapter"]
    hash_ = ch["hash"]
    files: list[str] = ch["dataSaver"] if data_saver else ch["data"]
    quality = "data-saver" if data_saver else "data"

    out_dir.mkdir(parents=True, exist_ok=True)
    meta = {
        "chapter_id": chapter_id,
        "hash": hash_,
        "quality": quality,
        "pages": len(files),
        "base_url": base,
    }
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    for i, name in enumerate(files, start=1):
        url = f"{base}/{quality}/{hash_}/{name}"
        ext = Path(name).suffix or ".jpg"
        dest = out_dir / f"{i:03d}{ext}"
        if dest.exists():
            print(f"  skip {dest.name} (déjà présent)")
            continue
        print(f"  {dest.name} …")
        try:
            download_file(url, dest)
        except urllib.error.HTTPError as e:
            raise SystemExit(f"Échec téléchargement page {i}: {e.code} {url}") from e
        time.sleep(delay_s)

    return len(files)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Télécharge un chapitre MangaDex (v1 — Martial Peak par défaut)."
    )
    parser.add_argument(
        "--manga-id",
        default=MARTIAL_PEAK_MANGA_ID,
        help=f"UUID du titre (défaut: Martial Peak).",
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
    args = parser.parse_args()

    if not args.chapter_id and not args.chapter:
        parser.error("Indique --chapter NUM ou --chapter-id UUID.")

    if args.chapter_id:
        chapter_id = args.chapter_id
        chapter_num, title = fetch_chapter_meta(chapter_id)
        if args.chapter:
            chapter_num = args.chapter
    else:
        chapter_id, _ = resolve_chapter_from_aggregate(
            args.manga_id, args.chapter, args.lang
        )
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
    )
    print(f"Terminé: {n} page(s) dans {dest}")


if __name__ == "__main__":
    main()
