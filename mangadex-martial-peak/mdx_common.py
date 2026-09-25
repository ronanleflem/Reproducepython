"""Client MangaDex partagé (v1 + v2)."""

from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

API_BASE = "https://api.mangadex.org"
MARTIAL_PEAK_MANGA_ID = "b1461071-bfbb-43e7-a5b6-a7ba5904649f"
USER_AGENT = "MartialPeakDownloader/2.1 (personal script; +https://mangadex.org)"
IMAGE_EXT = {".jpg", ".jpeg", ".png", ".webp"}
PAGE_NAME = re.compile(r"^\d{3}\.(jpg|jpeg|png|webp)$", re.IGNORECASE)


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


def chapter_zip_path(chapter_dir: Path) -> Path:
    return chapter_dir.parent / f"{chapter_dir.name}.zip"


def iter_page_files(chapter_dir: Path) -> list[Path]:
    files = [
        p
        for p in chapter_dir.iterdir()
        if p.is_file()
        and p.suffix.lower() in IMAGE_EXT
        and len(p.stem) == 3
        and p.stem.isdigit()
    ]
    return sorted(files, key=lambda p: p.name)


def read_expected_pages(
    chapter_dir: Path | None = None, zip_path: Path | None = None
) -> int | None:
    if chapter_dir and (chapter_dir / "meta.json").is_file():
        meta = json.loads((chapter_dir / "meta.json").read_text(encoding="utf-8"))
        pages = int(meta.get("pages") or 0)
        return pages if pages > 0 else None
    if zip_path and zip_path.is_file():
        with zipfile.ZipFile(zip_path, "r") as zf:
            if "meta.json" not in zf.namelist():
                return None
            meta = json.loads(zf.read("meta.json"))
            pages = int(meta.get("pages") or 0)
            return pages if pages > 0 else None
    return None


def count_zip_pages(zip_path: Path) -> int:
    with zipfile.ZipFile(zip_path, "r") as zf:
        return sum(1 for name in zf.namelist() if PAGE_NAME.match(Path(name).name))


def chapter_zip_valid(
    zip_path: Path, chapter_dir: Path | None = None
) -> bool:
    if not zip_path.is_file():
        return False
    expected = read_expected_pages(chapter_dir, zip_path)
    if expected is None:
        return count_zip_pages(zip_path) > 0
    return count_zip_pages(zip_path) >= expected


def chapter_dir_complete(out_dir: Path) -> bool:
    expected = read_expected_pages(chapter_dir=out_dir)
    if expected is None:
        return False
    return len(iter_page_files(out_dir)) >= expected


def chapter_ready(chapter_dir: Path | None, zip_path: Path | None) -> bool:
    """Chapitre OK si le zip est valide (dossier optionnel pour la reprise)."""
    if zip_path and chapter_zip_valid(zip_path, chapter_dir):
        return True
    if chapter_dir and chapter_dir_complete(chapter_dir):
        zp = chapter_zip_path(chapter_dir)
        if chapter_zip_valid(zp, chapter_dir):
            return True
    return False


def build_chapter_zip(chapter_dir: Path) -> Path:
    if not chapter_dir_complete(chapter_dir):
        raise RuntimeError(f"Dossier incomplet, impossible de zipper: {chapter_dir}")

    zip_path = chapter_zip_path(chapter_dir)
    pages = iter_page_files(chapter_dir)
    tmp_path = zip_path.with_suffix(".zip.part")

    with zipfile.ZipFile(tmp_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        meta = chapter_dir / "meta.json"
        if meta.is_file():
            zf.write(meta, arcname="meta.json")
        for page in pages:
            zf.write(page, arcname=page.name)

    tmp_path.replace(zip_path)
    return zip_path


def finalize_chapter(chapter_dir: Path, *, make_zip: bool = True) -> Path | None:
    if not make_zip:
        return None
    if not chapter_dir_complete(chapter_dir):
        return None
    zip_path = chapter_zip_path(chapter_dir)
    if chapter_zip_valid(zip_path, chapter_dir):
        return zip_path
    zip_path = build_chapter_zip(chapter_dir)
    print(f"  zip → {zip_path}")
    return zip_path


def download_chapter(
    chapter_id: str,
    out_dir: Path,
    *,
    data_saver: bool = False,
    delay_s: float = 0.35,
    make_zip: bool = True,
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

    finalize_chapter(out_dir, make_zip=make_zip)
    return len(files)


def chapter_sort_key(chapter_number: str) -> float:
    try:
        return float(chapter_number)
    except ValueError:
        return float("inf")
