"""Client MangaDex partagé (v1 + v2)."""

from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

API_BASE = "https://api.mangadex.org"
MARTIAL_PEAK_MANGA_ID = "b1461071-bfbb-43e7-a5b6-a7ba5904649f"
USER_AGENT = "MartialPeakDownloader/2.0 (personal script; +https://mangadex.org)"


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


def load_aggregate_chapters(manga_id: str, lang: str) -> dict[str, str]:
    """Numéro de chapitre → UUID (chapitres disponibles uniquement)."""
    payload = api_get(
        f"/manga/{manga_id}/aggregate",
        {"translatedLanguage[]": lang},
    )
    index: dict[str, str] = {}
    for vol in payload.get("volumes", {}).values():
        for ch_num, entry in (vol.get("chapters") or {}).items():
            if entry.get("isUnavailable"):
                continue
            index[str(ch_num)] = entry["id"]
    return index


def resolve_chapter_from_aggregate(
    manga_id: str, chapter_number: str, lang: str
) -> tuple[str, str | None]:
    index = load_aggregate_chapters(manga_id, lang)
    chapter_id = index.get(chapter_number)
    if not chapter_id:
        raise LookupError(
            f"Aucun chapitre {chapter_number!r} en {lang} pour le manga {manga_id}."
        )
    return chapter_id, None


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


def chapter_dir_complete(out_dir: Path) -> bool:
    meta_path = out_dir / "meta.json"
    if not meta_path.exists():
        return False
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    pages = int(meta.get("pages") or 0)
    if pages <= 0:
        return False
    image_ext = {".jpg", ".jpeg", ".png", ".webp"}
    count = sum(
        1
        for p in out_dir.iterdir()
        if p.is_file() and p.suffix.lower() in image_ext and len(p.stem) == 3 and p.stem.isdigit()
    )
    return count >= pages


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
            raise RuntimeError(f"Échec page {i}: HTTP {e.code}") from e
        time.sleep(delay_s)

    return len(files)


def chapter_sort_key(chapter_number: str) -> float:
    try:
        return float(chapter_number)
    except ValueError:
        return float("inf")
