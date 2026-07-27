"""Génération de payloads volumineux reproductibles pour les RESULT."""

from __future__ import annotations

# ~20 Mo — taille typique observée en production
CAMPAIGN_AVG_PAYLOAD_BYTES = 20 * 1024 * 1024

# Chunk réutilisé pour éviter d'allouer 20 Mo à chaque appel
_CHUNK_CACHE: dict[tuple[int, int], bytes] = {}


def make_result_payload(size_bytes: int, seed: int = 0) -> bytes:
    """Construit un blob binaire déterministe de taille donnée."""
    if size_bytes <= 0:
        return b""

    cache_key = (size_bytes, seed)
    if cache_key in _CHUNK_CACHE:
        return _CHUNK_CACHE[cache_key]

    marker = f"ZMQLAB:{seed}:".encode("ascii")
    unit = marker + b"\x00" * max(1, 64 - len(marker))
    repeats = (size_bytes + len(unit) - 1) // len(unit)
    data = (unit * repeats)[:size_bytes]
    _CHUNK_CACHE[cache_key] = data
    return data


def jitter_payload_bytes(
    rng,
    base_bytes: int,
    jitter: float,
    *,
    min_bytes: int = 0,
) -> int:
    """Applique un jitter relatif à une taille de payload."""
    if base_bytes <= 0:
        return 0
    factor = 1.0 + rng.uniform(-jitter, jitter)
    return max(min_bytes, int(base_bytes * factor))


def payload_timeout_bonus(payload_bytes: int, bytes_per_second: float = 25_000_000) -> float:
    """Temps supplémentaire recommandé pour transférer un gros payload (localhost)."""
    if payload_bytes <= 0:
        return 0.0
    return payload_bytes / bytes_per_second + 1.0
