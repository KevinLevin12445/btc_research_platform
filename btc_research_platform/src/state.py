"""
The State Database — the authoritative source of truth for what's in the
research database. Filesystem scanning alone never determines historical
completeness (Part II, "State Database" — since source files get deleted
after verified ingestion, the raw folder usually holds at most one pending
month and cannot answer "what's done" for the whole history).

Backed by SQLite (stdlib, transactional, WAL mode) — safe to interrupt at
any point without corruption, unlike a flat JSON log.

Two tables:

  month_status       One row per (symbol, year_month). Tracks not just
                      pass/fail but a `checkpoint` field for within-month
                      resumability (source_verified -> ticks_written ->
                      ticks_verified -> features_generated -> reported),
                      version stamps (schema/feature/software) per Part
                      II's Data Versioning requirement, the source file's
                      SHA-256 (an independent identity check, not just a
                      cache key), and a `verification_status` distinct
                      from `status` — a month can be fully WRITTEN
                      (status='done') without yet being independently
                      RE-VERIFIED (verification_status='verified'); only
                      the latter licenses deleting the source file.

  source_verification  Cache of structural/CRC/checksum verification for
                      whatever's currently in the inbox folder, keyed by
                      (path, size, mtime) so unchanged files aren't
                      re-verified every run.
"""

from __future__ import annotations

import dataclasses
import sqlite3
import time
from pathlib import Path
from typing import List, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS month_status (
    symbol TEXT NOT NULL,
    year_month TEXT NOT NULL,
    status TEXT NOT NULL,                  -- pending | in_progress | done | failed | paused_low_disk
    checkpoint TEXT,                        -- source_verified | ticks_written | ticks_verified | features_generated | reported
    verification_status TEXT,               -- not_verified | verified | verification_failed
    source_path TEXT,
    source_format TEXT,                     -- zip | csv
    source_size_bytes INTEGER,
    source_sha256 TEXT,
    checksum_sidecar_verified INTEGER,      -- 1 / 0 / NULL (no sidecar was present)
    rows_written INTEGER,
    output_path TEXT,
    min_trade_id INTEGER,
    max_trade_id INTEGER,
    min_trade_time_ns INTEGER,
    max_trade_time_ns INTEGER,
    n_price_or_qty_anomalies INTEGER,
    n_nonmonotonic_time INTEGER,
    n_trade_id_gaps INTEGER,
    total_missing_trade_ids INTEGER,
    n_duplicate_trade_ids INTEGER,
    n_duplicate_rows INTEGER,
    n_abnormal_price_jumps INTEGER,
    schema_version TEXT,
    feature_version TEXT,
    software_version TEXT,
    started_at REAL,
    finished_at REAL,
    verified_at REAL,
    source_deleted_at REAL,
    error_message TEXT,
    PRIMARY KEY (symbol, year_month)
);

CREATE TABLE IF NOT EXISTS source_verification (
    path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    mtime REAL NOT NULL,
    status TEXT NOT NULL,           -- ok | corrupted | empty | unsupported_layout | wrong_market
    source_format TEXT,             -- zip | csv
    sha256 TEXT,
    checksum_sidecar_verified INTEGER,
    detail TEXT,
    verified_at REAL,
    PRIMARY KEY (path, size_bytes, mtime)
);
"""


@dataclasses.dataclass
class MonthRecord:
    symbol: str
    year_month: str
    status: str
    checkpoint: Optional[str] = None
    verification_status: Optional[str] = None
    source_path: Optional[str] = None
    source_format: Optional[str] = None
    source_size_bytes: Optional[int] = None
    source_sha256: Optional[str] = None
    checksum_sidecar_verified: Optional[int] = None
    rows_written: Optional[int] = None
    output_path: Optional[str] = None
    min_trade_id: Optional[int] = None
    max_trade_id: Optional[int] = None
    min_trade_time_ns: Optional[int] = None
    max_trade_time_ns: Optional[int] = None
    n_price_or_qty_anomalies: Optional[int] = None
    n_nonmonotonic_time: Optional[int] = None
    n_trade_id_gaps: Optional[int] = None
    total_missing_trade_ids: Optional[int] = None
    n_duplicate_trade_ids: Optional[int] = None
    n_duplicate_rows: Optional[int] = None
    n_abnormal_price_jumps: Optional[int] = None
    schema_version: Optional[str] = None
    feature_version: Optional[str] = None
    software_version: Optional[str] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    verified_at: Optional[float] = None
    source_deleted_at: Optional[float] = None
    error_message: Optional[str] = None


@dataclasses.dataclass
class SourceVerificationRecord:
    path: str
    size_bytes: int
    mtime: float
    status: str
    source_format: Optional[str] = None
    sha256: Optional[str] = None
    checksum_sidecar_verified: Optional[int] = None
    detail: Optional[str] = None
    verified_at: Optional[float] = None


class StateStore:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), timeout=30, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.executescript(SCHEMA)  # executescript: SCHEMA has multiple statements
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # ---------------------------------------------------------------- month_status

    def get_record(self, symbol: str, year_month: str) -> Optional[MonthRecord]:
        cur = self._conn.execute("SELECT * FROM month_status WHERE symbol=? AND year_month=?", (symbol, year_month))
        row = cur.fetchone()
        if row is None:
            return None
        cols = [d[0] for d in cur.description]
        return MonthRecord(**dict(zip(cols, row)))

    def is_done(self, symbol: str, year_month: str) -> bool:
        r = self.get_record(symbol, year_month)
        return r is not None and r.status == "done"

    def create_if_not_exists(self, symbol: str, year_month: str, **initial_fields) -> MonthRecord:
        """Insert a new row if one doesn't already exist; no-op (returns
        the existing row untouched) if it does. This is the ONLY way a
        month_status row comes into existence — every subsequent change
        goes through update_fields, which touches only the columns given.
        """
        existing = self.get_record(symbol, year_month)
        if existing is not None:
            return existing
        record = MonthRecord(symbol=symbol, year_month=year_month, status=initial_fields.pop("status", "pending"), **initial_fields)
        fields = [f.name for f in dataclasses.fields(MonthRecord)]
        placeholders = ", ".join("?" for _ in fields)
        values = [getattr(record, f) for f in fields]
        self._conn.execute(f"INSERT INTO month_status ({', '.join(fields)}) VALUES ({placeholders})", values)
        self._conn.commit()
        return record

    def update_fields(self, symbol: str, year_month: str, **fields) -> None:
        """Partial update: touches ONLY the given columns, leaving every
        other already-recorded field untouched. This is the fix for a real
        bug earlier in this module's design — an upsert-the-whole-record
        approach silently NULLs out every field not explicitly passed on
        each call, which is exactly the kind of silent data loss Part II's
        validation philosophy prohibits. Requires the row to already exist
        (via create_if_not_exists) — raises if it doesn't, rather than
        silently creating a sparse row that would look like a genuine gap.
        """
        if not fields:
            return
        if self.get_record(symbol, year_month) is None:
            raise ValueError(f"No existing month_status row for ({symbol}, {year_month}); call create_if_not_exists first.")
        set_clause = ", ".join(f"{k}=?" for k in fields)
        values = list(fields.values()) + [symbol, year_month]
        self._conn.execute(f"UPDATE month_status SET {set_clause} WHERE symbol=? AND year_month=?", values)
        self._conn.commit()

    def list_all(self) -> List[MonthRecord]:
        cur = self._conn.execute("SELECT * FROM month_status ORDER BY year_month")
        cols = [d[0] for d in cur.description]
        return [MonthRecord(**dict(zip(cols, row))) for row in cur.fetchall()]

    # ------------------------------------------------------------- source_verification

    def get_cached_source_verification(self, path: str, size_bytes: int, mtime: float) -> Optional[SourceVerificationRecord]:
        cur = self._conn.execute(
            "SELECT * FROM source_verification WHERE path=? AND size_bytes=? AND mtime=?", (path, size_bytes, mtime)
        )
        row = cur.fetchone()
        if row is None:
            return None
        cols = [d[0] for d in cur.description]
        return SourceVerificationRecord(**dict(zip(cols, row)))

    def upsert_source_verification(self, record: SourceVerificationRecord) -> None:
        self._conn.execute(
            """
            INSERT INTO source_verification
                (path, size_bytes, mtime, status, source_format, sha256, checksum_sidecar_verified, detail, verified_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path, size_bytes, mtime) DO UPDATE SET
                status=excluded.status, source_format=excluded.source_format, sha256=excluded.sha256,
                checksum_sidecar_verified=excluded.checksum_sidecar_verified, detail=excluded.detail,
                verified_at=excluded.verified_at
            """,
            (
                record.path, record.size_bytes, record.mtime, record.status, record.source_format,
                record.sha256, record.checksum_sidecar_verified, record.detail, record.verified_at or time.time(),
            ),
        )
        self._conn.commit()

    # ------------------------------------------------------------- cumulative stats

    def cumulative_stats(self) -> dict:
        """Powers the 'cumulative database size / cumulative research
        statistics' section of the per-month report (Part II 'Data Quality
        Report'). Computed from month_status, never from a folder scan.
        """
        done = [r for r in self.list_all() if r.status == "done"]
        return {
            "months_done": len(done),
            "total_trades": sum(r.rows_written or 0 for r in done),
            "total_duplicate_trade_ids": sum(r.n_duplicate_trade_ids or 0 for r in done),
            "total_price_qty_anomalies": sum(r.n_price_or_qty_anomalies or 0 for r in done),
            "total_abnormal_price_jumps": sum(r.n_abnormal_price_jumps or 0 for r in done),
            "earliest_month": min((r.year_month for r in done), default=None),
            "latest_month": max((r.year_month for r in done), default=None),
        }
