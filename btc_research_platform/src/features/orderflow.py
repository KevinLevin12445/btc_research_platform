"""
Order-flow features: delta, cumulative delta, large trades, stacked
imbalance, delta divergence, absorption candidates, iceberg candidates.

Real exchange data (`is_buyer_maker` is a genuine trade-level aggressor
tag) — Delta/Cumulative Delta/taker volume here are backtestable
historical facts, not approximations.

Two things this dataset genuinely CANNOT give you, stated once:
  - Resting order-book bid/ask depth — only taker-side traded volume is
    available, not literal book depth (needs a separate depth dataset).
  - True iceberg detection — `detect_iceberg_candidates` is an explicitly
    labeled heuristic with a real false-positive rate.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List

import duckdb

from ..duckdb_utils import format_parquet_path_list

logger = logging.getLogger(__name__)


def _atomic_copy(con: duckdb.DuckDBPyConnection, query: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    con.execute(f"COPY ({query}) TO '{tmp_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    tmp_path.replace(output_path)
    logger.info("Wrote %s", output_path)


def compute_orderflow_bars(con: duckdb.DuckDBPyConnection, bars_path: str, output_path: Path) -> None:
    """Delta, cumulative delta (continuous + session-anchored), buy/sell
    participation ratio, at whatever bar resolution `bars_path` points to.
    """
    query = f"""
        WITH base AS (
            SELECT *,
                taker_buy_volume - taker_sell_volume AS delta,
                taker_buy_volume / NULLIF(taker_buy_volume + taker_sell_volume, 0) AS taker_buy_ratio
            FROM read_parquet('{bars_path}')
        )
        SELECT *,
            sum(delta) OVER (ORDER BY bar_time) AS cumulative_delta,
            sum(delta) OVER (PARTITION BY date_trunc('day', bar_time) ORDER BY bar_time) AS session_cumulative_delta
        FROM base
        ORDER BY bar_time
    """
    _atomic_copy(con, query, output_path)


def detect_large_trades(con: duckdb.DuckDBPyConnection, tick_paths: List[str], output_path: Path, quantile: float) -> None:
    path_list = format_parquet_path_list(tick_paths)
    threshold = con.execute(f"SELECT approx_quantile(qty, {quantile}) FROM read_parquet({path_list})").fetchone()[0]
    logger.info("Large-trade threshold (p%.1f of qty): %.6f", quantile * 100, threshold)
    query = f"""
        SELECT trade_id, price, qty, quote_qty, trade_time_ns, is_buyer_maker
        FROM read_parquet({path_list})
        WHERE qty >= {threshold}
        ORDER BY trade_time_ns
    """
    _atomic_copy(con, query, output_path)


def detect_stacked_imbalances(
    con: duckdb.DuckDBPyConnection, bars_path: str, output_path: Path, ratio: float, min_consecutive_bars: int
) -> None:
    """Gaps-and-islands SQL pattern: runs of `min_consecutive_bars`+
    consecutive bars where one side's taker volume is >= `ratio`x the other.
    """
    query = f"""
        WITH base AS (
            SELECT *,
                CASE
                    WHEN taker_buy_volume >= {ratio} * NULLIF(taker_sell_volume, 0) THEN 'buy'
                    WHEN taker_sell_volume >= {ratio} * NULLIF(taker_buy_volume, 0) THEN 'sell'
                    ELSE 'none'
                END AS imbalance_side
            FROM read_parquet('{bars_path}')
        ),
        islands AS (
            SELECT *,
                row_number() OVER (ORDER BY bar_time)
                    - row_number() OVER (PARTITION BY imbalance_side ORDER BY bar_time) AS island_id
            FROM base
            WHERE imbalance_side != 'none'
        ),
        sized AS (
            SELECT *, count(*) OVER (PARTITION BY imbalance_side, island_id) AS island_length
            FROM islands
        )
        SELECT bar_time, close, volume, taker_buy_volume, taker_sell_volume, imbalance_side, island_length
        FROM sized
        WHERE island_length >= {min_consecutive_bars}
        ORDER BY bar_time
    """
    _atomic_copy(con, query, output_path)


def detect_delta_divergence(con: duckdb.DuckDBPyConnection, orderflow_bars_path: str, output_path: Path, lookback_bars: int) -> None:
    query = f"""
        WITH base AS (
            SELECT *,
                max(close) OVER (ORDER BY bar_time ROWS BETWEEN {lookback_bars} PRECEDING AND 1 PRECEDING) AS prior_high,
                min(close) OVER (ORDER BY bar_time ROWS BETWEEN {lookback_bars} PRECEDING AND 1 PRECEDING) AS prior_low,
                max(cumulative_delta) OVER (ORDER BY bar_time ROWS BETWEEN {lookback_bars} PRECEDING AND 1 PRECEDING) AS prior_delta_high,
                min(cumulative_delta) OVER (ORDER BY bar_time ROWS BETWEEN {lookback_bars} PRECEDING AND 1 PRECEDING) AS prior_delta_low
            FROM read_parquet('{orderflow_bars_path}')
        )
        SELECT bar_time, close, cumulative_delta,
            (close > prior_high AND cumulative_delta <= prior_delta_high) AS bearish_divergence,
            (close < prior_low AND cumulative_delta >= prior_delta_low) AS bullish_divergence
        FROM base
        WHERE prior_high IS NOT NULL
        QUALIFY bearish_divergence OR bullish_divergence
        ORDER BY bar_time
    """
    _atomic_copy(con, query, output_path)


def detect_absorption_candidates(con: duckdb.DuckDBPyConnection, bars_path: str, output_path: Path, volume_quantile: float = 0.9) -> None:
    """absorption_candidate: volume in the top decile for a trailing
    30-day window, but price range in the BOTTOM half of its own trailing
    distribution — large size traded with little net price movement.

    NOT YET IMPLEMENTED: an `exhaustion_candidate` flag is a natural
    extension (join stacked-imbalance islands to the bar immediately after
    each island, compare that bar's close direction to the island's) —
    deliberately left out rather than shipped half-verified.
    """
    query = f"""
        WITH base AS (
            SELECT *,
                (high - low) AS bar_range,
                quantile_cont(volume, {volume_quantile}) OVER (ORDER BY bar_time ROWS BETWEEN 43200 PRECEDING AND CURRENT ROW) AS vol_p90_30d,
                quantile_cont(high - low, 0.5) OVER (ORDER BY bar_time ROWS BETWEEN 43200 PRECEDING AND CURRENT ROW) AS range_median_30d
            FROM read_parquet('{bars_path}')
        )
        SELECT bar_time, close, volume, bar_range,
            (volume >= vol_p90_30d AND bar_range <= range_median_30d) AS absorption_candidate
        FROM base
        WHERE vol_p90_30d IS NOT NULL
        QUALIFY absorption_candidate
        ORDER BY bar_time
    """
    _atomic_copy(con, query, output_path)


def detect_iceberg_candidates(
    con: duckdb.DuckDBPyConnection, tick_paths: List[str], output_path: Path, window_seconds: int = 2, min_repeats: int = 5
) -> None:
    """HEURISTIC ONLY — see module docstring. Groups trades by (price,
    aggressor side, N-second bucket); flags unusually high repeat counts.
    """
    query = f"""
        WITH bucketed AS (
            SELECT
                price, is_buyer_maker,
                time_bucket(INTERVAL '{window_seconds} second', to_timestamp(trade_time_ns / 1e9)) AS bucket_time,
                count(*) AS n_trades, sum(qty) AS total_qty, stddev_pop(qty) AS qty_stddev
            FROM read_parquet({format_parquet_path_list(tick_paths)})
            GROUP BY 1, 2, 3
        )
        SELECT * FROM bucketed WHERE n_trades >= {min_repeats} ORDER BY bucket_time
    """
    _atomic_copy(con, query, output_path)
