"""
Discovery and verification, adapted for the incremental one-month-at-a-time
workflow: the inbox folder (`paths.raw_data_dir`) typically holds 0-1 files
at any time (source files get deleted after verified ingestion), so it can
no longer answer "what's been done" for the whole history — see state.py
module docstring. This module's job is split accordingly:

  - scan_inbox / verify_source_file: what's CURRENTLY sitting in the inbox,
    ready to ingest (or not — structural/CRC/checksum/market-identity
    checks all happen here, BEFORE ingestion ever touches the file).
  - next_required_month / missing_months: derived from the STATE DB, the
    durable record, not the folder.
"""

from __future__ import annotations

import dataclasses
import logging
import re
import zipfile
from pathlib import Path
from typing import List, Optional

from .errors import CorruptedArchiveError, WrongMarketError
from .hashing import compute_sha256, verify_against_sidecar
from .source_input import detect_layout, find_inner_csv_name, peek_first_lines
from .state import SourceVerificationRecord, StateStore

logger = logging.getLogger(__name__)

FILENAME_RE = re.compile(r"^(?P<symbol>[A-Z0-9]+)-trades-(?P<year>\d{4})-(?P<month>\d{2})\.(?P<ext>zip|csv)$")


@dataclasses.dataclass(frozen=True)
class InboxFile:
    path: Path
    symbol: str
    year: int
    month: int
    format: str  # "zip" | "csv"
    size_bytes: int

    @property
    def year_month(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def scan_inbox(raw_dir: Path, expected_symbol: str) -> List[InboxFile]:
    files = []
    for path in sorted(list(raw_dir.glob("*.zip")) + list(raw_dir.glob("*.csv"))):
        m = FILENAME_RE.match(path.name)
        if not m or m.group("symbol") != expected_symbol:
            continue
        files.append(
            InboxFile(
                path=path, symbol=m.group("symbol"), year=int(m.group("year")), month=int(m.group("month")),
                format=m.group("ext"), size_bytes=path.stat().st_size,
            )
        )
    return files


def find_unrecognized_inbox_files(raw_dir: Path) -> List[Path]:
    """Files sitting in the inbox that don't match the expected naming
    convention at all — worth surfacing rather than silently ignoring,
    since a silently-skipped file is exactly what causes a confusing gap
    later.
    """
    all_files = list(raw_dir.glob("*.zip")) + list(raw_dir.glob("*.csv"))
    return [p for p in all_files if not FILENAME_RE.match(p.name)]


def _structural_check(path: Path) -> tuple[str, Optional[str]]:
    """Cheap structural check (no CRC, no hashing). Returns (status, detail)."""
    if path.suffix.lower() == ".zip":
        try:
            with zipfile.ZipFile(path) as zf:
                csv_entries = [i for i in zf.infolist() if i.filename.lower().endswith(".csv")]
                other = [i for i in zf.infolist() if not i.filename.lower().endswith(".csv")]
                if len(csv_entries) != 1 or other:
                    return "unsupported_layout", f"{len(csv_entries)} csv entries, {len(other)} other entries"
                if csv_entries[0].file_size < 50:
                    return "empty", f"CSV entry is only {csv_entries[0].file_size} bytes uncompressed"
        except zipfile.BadZipFile as e:
            return "corrupted", f"not a valid zip container: {e}"
    else:  # csv
        if path.stat().st_size < 50:
            return "empty", f"file is only {path.stat().st_size} bytes"
    return "ok", None


def _deep_crc_check(path: Path) -> tuple[str, Optional[str]]:
    """ZIP only — full CRC-32 verification. No equivalent container-level
    check exists for a raw CSV; SHA-256 + checksum sidecar is what stands
    in for it (see hashing.py).
    """
    if path.suffix.lower() != ".zip":
        return "ok", None
    try:
        with zipfile.ZipFile(path) as zf:
            bad = zf.testzip()
            if bad is not None:
                return "corrupted", f"CRC mismatch in entry: {bad}"
    except Exception as e:  # noqa: BLE001
        return "corrupted", f"error during CRC verification: {e}"
    return "ok", None


def _market_identity_check(path: Path) -> tuple[str, Optional[str]]:
    """Peeks the file's actual columns to confirm it's Futures (6 columns),
    not Spot (7 columns) — see schema.py. Runs as part of verification so a
    wrong-market file is caught before ingestion, not partway through it.
    """
    try:
        if path.suffix.lower() == ".zip":
            with zipfile.ZipFile(path) as zf:
                inner_name = find_inner_csv_name(zf)
                with zf.open(inner_name, "r") as stream:
                    lines = peek_first_lines(stream, n=2)
        else:
            with open(path, "r", encoding="utf-8", errors="replace") as stream:
                lines = peek_first_lines(stream, n=2)
        detect_layout(lines, path.name)
    except WrongMarketError as e:
        return "wrong_market", str(e)
    except CorruptedArchiveError as e:
        return "corrupted", str(e)
    return "ok", None


def verify_source_file(path: Path, state: StateStore, deep: bool = True) -> SourceVerificationRecord:
    """Full verification pipeline for one inbox file: structural check,
    market-identity check, deep CRC (zip only), SHA-256 + optional
    Binance .CHECKSUM sidecar. Cached by (path, size, mtime).
    """
    stat = path.stat()
    cached = state.get_cached_source_verification(str(path), stat.st_size, stat.st_mtime)
    if cached is not None:
        return cached

    fmt = "zip" if path.suffix.lower() == ".zip" else "csv"
    status, detail = _structural_check(path)

    if status == "ok":
        status, detail = _market_identity_check(path)

    if status == "ok" and deep:
        status, detail = _deep_crc_check(path)

    sha256 = None
    sidecar_result = None
    if status == "ok":
        logger.info("Hashing %s (SHA-256)...", path.name)
        sha256 = compute_sha256(path)
        sidecar_result = verify_against_sidecar(path, sha256)
        if sidecar_result is False:
            status, detail = "corrupted", "SHA-256 does not match Binance .CHECKSUM sidecar"

    record = SourceVerificationRecord(
        path=str(path), size_bytes=stat.st_size, mtime=stat.st_mtime, status=status,
        source_format=fmt, sha256=sha256,
        checksum_sidecar_verified=(None if sidecar_result is None else int(sidecar_result)),
        detail=detail, verified_at=None,
    )
    state.upsert_source_verification(record)
    return record


def next_required_month(state: StateStore, configured_start_month: str) -> str:
    """The next month to download, per Part II's 'next month required'
    report field — derived from the state DB (what's actually been
    ingested), not the inbox folder.
    """
    done_months = sorted(r.year_month for r in state.list_all() if r.status == "done")
    if not done_months:
        return configured_start_month
    last_year, last_month = (int(x) for x in done_months[-1].split("-"))
    last_month += 1
    if last_month == 13:
        last_month = 1
        last_year += 1
    return f"{last_year:04d}-{last_month:02d}"


def missing_months(state: StateStore, configured_start_month: str) -> List[str]:
    """Gaps between configured_start_month and the latest DONE month, per
    the state DB. With strictly sequential incremental ingestion this
    should normally be empty — a non-empty result means a month was
    skipped, which is worth surfacing explicitly.
    """
    done_months = sorted(r.year_month for r in state.list_all() if r.status == "done")
    if not done_months:
        return []
    present = set(done_months)
    start_y, start_m = (int(x) for x in configured_start_month.split("-"))
    end_y, end_m = (int(x) for x in done_months[-1].split("-"))

    gaps = []
    y, m = start_y, start_m
    while (y, m) <= (end_y, end_m):
        key = f"{y:04d}-{m:02d}"
        if key not in present:
            gaps.append(key)
        m += 1
        if m == 13:
            m = 1
            y += 1
    return gaps
