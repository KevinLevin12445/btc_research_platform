"""
Footprint reconstruction from raw trade ticks — the one derived dataset
genuinely expensive to defer, since it needs a full scan/GROUP BY over the
raw tick corpus (unlike bars/delta, which are cheap to (re)compute from
the already-small bar tables).

Generated at ONE fine resolution (config default: 1-minute x $10 price
bins). Coarser footprint later is a cheap re-aggregation of THIS table
(sum rows into wider buckets) — never a re-scan of raw ticks.

"buy_volume"/"sell_volume" are TAKER-SIDE traded volume (from
is_buyer_maker), not resting order-book depth — see features/orderflow.py.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List

import duckdb

from ..duckdb_utils import format_parquet_path_list

logger = logging.getLogger(__name__)

_FIXED_UNIT_MAP = {
    "1s": "1 second", "1min": "1 minute", "5min": "5 minute", "15min": "15 minute",
    "30min": "30 minute", "1h": "1 hour", "4h": "4 hour",
}


def compute_footprint(con: duckdb.DuckDBPyConnection, tick_paths: List[str], output_path: Path, bar_interval: str, price_bin_size: float) -> None:
    if bar_interval not in _FIXED_UNIT_MAP:
        raise ValueError(f"footprint_bar_interval must be one of {list(_FIXED_UNIT_MAP)}, got {bar_interval!r}")

    bucket_expr = f"time_bucket(INTERVAL '{_FIXED_UNIT_MAP[bar_interval]}', to_timestamp(trade_time_ns / 1e9))"
    query = f"""
        SELECT
            {bucket_expr} AS bar_time,
            floor(price / {price_bin_size}) * {price_bin_size} AS price_bin,
            sum(CASE WHEN is_buyer_maker THEN 0 ELSE qty END) AS buy_volume,
            sum(CASE WHEN is_buyer_maker THEN qty ELSE 0 END) AS sell_volume,
            sum(CASE WHEN is_buyer_maker THEN 0 ELSE qty END) - sum(CASE WHEN is_buyer_maker THEN qty ELSE 0 END) AS delta,
            sum(qty) AS total_volume,
            count(*) AS trade_count
        FROM read_parquet({format_parquet_path_list(tick_paths)})
        GROUP BY {bucket_expr}, floor(price / {price_bin_size}) * {price_bin_size}
        ORDER BY bar_time, price_bin
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    logger.info("Generating footprint (%s bars x %.2f price bins) -> %s (full tick-corpus scan)", bar_interval, price_bin_size, output_path)
    con.execute(f"COPY ({query}) TO '{tmp_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    tmp_path.replace(output_path)
    logger.info("Wrote %s", output_path)
