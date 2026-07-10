"""
File hashing and optional Binance-published checksum verification.

Every ingested source file gets a SHA-256 computed and stored in the state
DB regardless of whether a Binance `.CHECKSUM` sidecar is present — this is
what makes `raw_file_sha256` in the state DB a meaningful, independent
identity check for a source file (e.g. detecting silent corruption/
tampering between download and ingestion), not just a cache key.

If Binance's own `<filename>.CHECKSUM` sidecar (published alongside every
archive on data.binance.vision) is present next to the source file, it is
verified too — a genuine independent check against Binance's own published
hash, not just internal self-consistency. Optional because Binance Vision
downloads don't always include the sidecar unless explicitly fetched, but
strongly recommended: download the `.CHECKSUM` file alongside the archive
and place it in the same folder to get this extra layer of verification.
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_CHUNK_SIZE = 4 * 1024 * 1024  # 4MB read chunks — low, constant RAM regardless of file size


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(_CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def find_checksum_sidecar(source_path: Path) -> Optional[Path]:
    candidate = source_path.with_name(source_path.name + ".CHECKSUM")
    return candidate if candidate.exists() else None


def verify_against_sidecar(source_path: Path, computed_sha256: str) -> Optional[bool]:
    """Returns True/False if a sidecar was found and checked, None if no
    sidecar was present (not a failure — just means this extra check
    wasn't available for this file).
    """
    sidecar = find_checksum_sidecar(source_path)
    if sidecar is None:
        return None

    content = sidecar.read_text(encoding="utf-8").strip()
    # Binance's .CHECKSUM format: "<hexdigest>  <filename>" (sha256sum -c compatible)
    published_hash = content.split()[0].lower()
    matches = published_hash == computed_sha256.lower()
    if not matches:
        logger.error(
            "Checksum MISMATCH for %s: computed=%s published=%s (sidecar: %s)",
            source_path.name, computed_sha256, published_hash, sidecar,
        )
    return matches
