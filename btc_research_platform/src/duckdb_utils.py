"""
Shared DuckDB connection setup, and a helper for building
`read_parquet([...])` calls from an explicit list of file paths rather
than a directory glob (state-DB-resolved, since months can in principle
live anywhere — see pipeline.get_all_tick_paths).

--- create_duckdb_connection ---
HARD-LEARNED LESSON, carried forward: an earlier version of this project
hit a DuckDB OutOfMemoryException generating 1-second bars over ~1.3
billion rows. Root cause: DuckDB spills large aggregations to a temp
directory on disk when they exceed `memory_limit`, and by default it picks
that directory's location AND its size cap implicitly (tied to "available
disk space where temp_directory is located"). With the output drive nearly
full at the time, DuckDB auto-detected ~798 MB of spill room and ran out
immediately. Every DuckDB connection in this project goes through this one
function, which sets `temp_directory` and `max_temp_directory_size`
EXPLICITLY — the exact failure mode this incident exposed.
"""

from __future__ import annotations

import logging
from typing import List

import duckdb

logger = logging.getLogger(__name__)


def format_parquet_path_list(paths: List[str]) -> str:
    if not paths:
        raise ValueError("No parquet paths provided — nothing to query.")
    escaped = [p.replace("\\", "/").replace("'", "''") for p in paths]
    return "[" + ", ".join(f"'{p}'" for p in escaped) + "]"


def create_duckdb_connection(config) -> duckdb.DuckDBPyConnection:
    temp_dir = config.paths.warehouse_dir / "_duckdb_tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA memory_limit='{config.processing.duckdb_memory_limit_gb}GB'")
    con.execute(f"PRAGMA threads={config.processing.duckdb_threads}")
    con.execute(f"PRAGMA temp_directory='{temp_dir.as_posix()}'")
    con.execute(f"PRAGMA max_temp_directory_size='{config.processing.duckdb_max_temp_gb}GiB'")
    con.execute("PRAGMA preserve_insertion_order=false")
    logger.info(
        "DuckDB connection: memory_limit=%dGB, threads=%d, temp_directory=%s, max_temp_directory_size=%dGiB",
        config.processing.duckdb_memory_limit_gb, config.processing.duckdb_threads,
        temp_dir, config.processing.duckdb_max_temp_gb,
    )
    return con
