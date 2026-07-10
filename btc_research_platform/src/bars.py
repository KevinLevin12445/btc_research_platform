"""
Bar generation via DuckDB, run against an EXPLICIT LIST of tick Parquet
file paths (resolved from the state DB manifest — see
pipeline.get_all_tick_paths), never a directory glob.

Two aggregation styles: FIXED-WIDTH intervals (1s..4h) use `time_bucket`;
CALENDAR intervals (1d/1w/1mo) use `date_trunc`, which aligns to actual
calendar boundaries rather than a fixed-size window.

Every bar carries a genuine per-bar VWAP (sum(quote_qty)/sum(qty)) —
requires the tick-level scan bars.py already does, and cannot be
reconstructed later from OHLCV alone.

Taker buy/sell volume, from `is_buyer_maker` (real exchange data):
    is_buyer_maker = true  -> resting order was a BUY  -> taker SOLD  (taker_sell_volume)
    is_buyer_maker = false -> resting order was a SELL  -> taker BOUGHT (taker_buy_volume)
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List

import duckdb

from .duckdb_utils import create_duckdb_connection, format_parquet_path_list

logger = logging.getLogger(__name__)

_AGG_SELECT = """
    SELECT
        any_value(symbol) AS symbol,
        {bucket_expr} AS bar_time,
        arg_min(price, trade_time_ns) AS open,
        max(price) AS high,
        min(price) AS low,
        arg_max(price, trade_time_ns) AS close,
        sum(qty) AS volume,
        sum(quote_qty) AS quote_volume,
        sum(quote_qty) / NULLIF(sum(qty), 0) AS vwap,
        sum(CASE WHEN is_buyer_maker THEN 0 ELSE qty END) AS taker_buy_volume,
        sum(CASE WHEN is_buyer_maker THEN qty ELSE 0 END) AS taker_sell_volume,
        count(*) AS trade_count,
        avg(qty) AS avg_trade_size,
        max(qty) AS max_trade_size
    FROM read_parquet({path_list})
    GROUP BY {bucket_expr}
    ORDER BY {bucket_expr}
"""

_FIXED_UNIT_MAP = {
    "1s": "1 second", "1min": "1 minute", "5min": "5 minute", "15min": "15 minute",
    "30min": "30 minute", "1h": "1 hour", "4h": "4 hour",
}
_CALENDAR_UNIT_MAP = {"1d": "day", "1w": "week", "1mo": "month"}


def generate_bar(con: duckdb.DuckDBPyConnection, tick_paths: List[str], output_path: Path, interval: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")

    if interval in _FIXED_UNIT_MAP:
        bucket_expr = f"time_bucket(INTERVAL '{_FIXED_UNIT_MAP[interval]}', to_timestamp(trade_time_ns / 1e9))"
    elif interval in _CALENDAR_UNIT_MAP:
        bucket_expr = f"date_trunc('{_CALENDAR_UNIT_MAP[interval]}', to_timestamp(trade_time_ns / 1e9))"
    else:
        raise ValueError(f"Unknown bar interval: {interval}")

    query = _AGG_SELECT.format(bucket_expr=bucket_expr, path_list=format_parquet_path_list(tick_paths))
    copy_sql = f"COPY ({query}) TO '{tmp_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    logger.info("Generating %s bars -> %s", interval, output_path)
    con.execute(copy_sql)
    tmp_path.replace(output_path)


def generate_all_bars(config, tick_paths: List[str]) -> None:
    if not tick_paths:
        logger.warning("No tick paths available — skipping bar generation entirely.")
        return

    symbol = config.project.symbol
    con = create_duckdb_connection(config)
    try:
        for interval in config.bars.fixed_intervals + config.bars.calendar_intervals:
            output_path = config.paths.bars_dir / symbol / f"{symbol}_{interval}.parquet"
            generate_bar(con, tick_paths, output_path, interval)
    finally:
        con.close()
