"""
Session-anchored VWAP. Needs NO tick-level scan: the 1-minute bar table
already carries `volume`/`quote_volume` (captured from ticks at
bar-generation time), so session-cumulative VWAP is a running sum of
those two columns, partitioned by UTC day.

Daily/weekly/monthly SESSION STATISTICS are already fully covered by the
'1d'/'1w'/'1mo' calendar bar tables (they carry `vwap` too) — not
duplicated here.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


def compute_session_vwap(con: duckdb.DuckDBPyConnection, bars_1min_path: str, output_path: Path) -> None:
    query = f"""
        WITH base AS (
            SELECT *, date_trunc('day', bar_time) AS session
            FROM read_parquet('{bars_1min_path}')
        )
        SELECT
            bar_time, session, close,
            sum(quote_volume) OVER (PARTITION BY session ORDER BY bar_time) AS session_cumulative_quote_volume,
            sum(volume) OVER (PARTITION BY session ORDER BY bar_time) AS session_cumulative_volume,
            sum(quote_volume) OVER (PARTITION BY session ORDER BY bar_time)
                / NULLIF(sum(volume) OVER (PARTITION BY session ORDER BY bar_time), 0) AS session_vwap,
            close - (
                sum(quote_volume) OVER (PARTITION BY session ORDER BY bar_time)
                / NULLIF(sum(volume) OVER (PARTITION BY session ORDER BY bar_time), 0)
            ) AS vwap_deviation
        FROM base
        ORDER BY bar_time
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    con.execute(f"COPY ({query}) TO '{tmp_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    tmp_path.replace(output_path)
    logger.info("Wrote %s", output_path)
