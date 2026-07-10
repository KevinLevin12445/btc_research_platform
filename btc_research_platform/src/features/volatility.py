"""
Volatility features: Wilder's ATR and rolling realized volatility of log
returns. Wilder's smoothing is a recursive EMA (each value depends on the
previous), not a natural SQL window function — True Range is computed
across the full history in one DuckDB pass, and the short recursive
smoothing step runs in pandas afterward (the True Range series is small
even for years of hourly bars).
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


def _atomic_write_parquet(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    df.to_parquet(tmp_path, compression="zstd", index=False)
    tmp_path.replace(output_path)
    logger.info("Wrote %s (%d rows)", output_path, len(df))


def compute_atr(con: duckdb.DuckDBPyConnection, bars_path: str, output_path: Path, period: int) -> None:
    tr_query = f"""
        WITH base AS (
            SELECT bar_time, high, low, close, lag(close) OVER (ORDER BY bar_time) AS prev_close
            FROM read_parquet('{bars_path}')
        )
        SELECT bar_time, GREATEST(high - low, ABS(high - prev_close), ABS(low - prev_close)) AS true_range
        FROM base WHERE prev_close IS NOT NULL ORDER BY bar_time
    """
    df = con.execute(tr_query).fetch_df()
    if df.empty:
        logger.warning("No bar data found for ATR computation (path=%s)", bars_path)
        return
    df["atr"] = df["true_range"].ewm(alpha=1.0 / period, adjust=False).mean()  # Wilder's == EWMA(alpha=1/period), exact not approximate
    _atomic_write_parquet(df, output_path)


def compute_realized_volatility(con: duckdb.DuckDBPyConnection, bars_path: str, output_path: Path, window: int) -> None:
    query = f"""
        WITH base AS (
            SELECT bar_time, close, ln(close / lag(close) OVER (ORDER BY bar_time)) AS log_return
            FROM read_parquet('{bars_path}')
        )
        SELECT bar_time, close, log_return,
            stddev_samp(log_return) OVER (ORDER BY bar_time ROWS BETWEEN {window - 1} PRECEDING AND CURRENT ROW) AS realized_vol
        FROM base WHERE log_return IS NOT NULL ORDER BY bar_time
    """
    df = con.execute(query).fetch_df()
    if df.empty:
        logger.warning("No bar data found for realized volatility computation (path=%s)", bars_path)
        return
    _atomic_write_parquet(df, output_path)
