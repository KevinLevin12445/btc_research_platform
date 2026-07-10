"""
Chunked, atomic Parquet writing.

Two safety/performance properties, carried forward as hard-won lessons:

1. ATOMIC COMMIT: always write to a `*.tmp.parquet` path, `os.replace()`
   to the final filename only after the writer closes cleanly (atomic on
   both Windows and POSIX). A crash mid-month can never leave a
   half-written file mistaken for complete.

2. DISK-SPACE GUARD: checked before opening the writer AND before every
   chunk write. Raises DiskSpaceGuardError immediately rather than risking
   a corrupt, truncated file.

3. ROW GROUP SIZE, decoupled from read-chunk size: `chunk_rows` (pandas
   parse batch) and `row_group_size` (physical Parquet row group) are
   independent — smaller row groups give DuckDB finer min/max-based
   pruning on trade_time_ns for date-range queries, at a small cost in
   Parquet metadata overhead and slightly less effective per-group
   compression. A deliberate, disclosed trade-off.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from .errors import DiskSpaceGuardError
from .schema import TICK_SCHEMA

logger = logging.getLogger(__name__)


def free_space_gb(path: Path) -> float:
    return shutil.disk_usage(path).free / 1e9


def check_disk_space(path: Path, min_free_gb: float) -> None:
    free = free_space_gb(path)
    if free < min_free_gb:
        raise DiskSpaceGuardError(
            f"Free space on {path.drive or path.anchor} is {free:.2f} GB, below the "
            f"configured minimum of {min_free_gb} GB. Stopping before writing more data."
        )


class MonthlyParquetWriter:
    def __init__(
        self,
        final_path: Path,
        min_free_disk_gb: float,
        compression: str = "zstd",
        row_group_size: Optional[int] = 500_000,
    ):
        self.final_path = Path(final_path)
        self.tmp_path = self.final_path.with_suffix(self.final_path.suffix + ".tmp")
        self.min_free_disk_gb = min_free_disk_gb
        self.row_group_size = row_group_size
        self.final_path.parent.mkdir(parents=True, exist_ok=True)

        if self.tmp_path.exists():
            logger.warning("Removing stale temp file from a previous interrupted run: %s", self.tmp_path)
            self.tmp_path.unlink()

        check_disk_space(self.final_path.parent, self.min_free_disk_gb)
        self._writer = pq.ParquetWriter(str(self.tmp_path), schema=TICK_SCHEMA, compression=compression)
        self.rows_written = 0

    def write(self, table: pa.Table) -> None:
        if table.num_rows == 0:
            return
        check_disk_space(self.final_path.parent, self.min_free_disk_gb)
        self._writer.write_table(table, row_group_size=self.row_group_size)
        self.rows_written += table.num_rows

    def finalize(self) -> Path:
        self._writer.close()
        self.tmp_path.replace(self.final_path)
        logger.info("Committed %s rows to %s", f"{self.rows_written:,}", self.final_path)
        return self.final_path

    def abort(self) -> None:
        try:
            self._writer.close()
        finally:
            if self.tmp_path.exists():
                self.tmp_path.unlink()
