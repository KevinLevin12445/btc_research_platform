"""
Auction Market Theory / Volume Profile features. Builds the (session,
price_bin) volume histogram directly from tick Parquet — genuine
aggregation over real traded volume. The histogram itself is small once
aggregated (sessions x bins, not sessions x ticks), so the sequential
value-area-expansion algorithm — not naturally vectorizable in SQL — runs
in pandas without RAM concerns.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List

import duckdb
import numpy as np
import pandas as pd

from ..duckdb_utils import format_parquet_path_list

logger = logging.getLogger(__name__)


def _atomic_write_parquet(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    df.to_parquet(tmp_path, compression="zstd", index=False)
    tmp_path.replace(output_path)
    logger.info("Wrote %s (%d rows)", output_path, len(df))


def _session_histogram(con: duckdb.DuckDBPyConnection, tick_paths: List[str], bin_size: float) -> pd.DataFrame:
    query = f"""
        SELECT
            date_trunc('day', to_timestamp(trade_time_ns / 1e9)) AS session,
            floor(price / {bin_size}) * {bin_size} AS price_bin,
            sum(qty) AS volume
        FROM read_parquet({format_parquet_path_list(tick_paths)})
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    return con.execute(query).fetch_df()


def _session_range(con: duckdb.DuckDBPyConnection, tick_paths: List[str]) -> pd.DataFrame:
    query = f"""
        SELECT
            date_trunc('day', to_timestamp(trade_time_ns / 1e9)) AS session,
            min(price) AS day_low, max(price) AS day_high, sum(qty) AS day_volume
        FROM read_parquet({format_parquet_path_list(tick_paths)})
        GROUP BY 1
        ORDER BY 1
    """
    return con.execute(query).fetch_df()


def _value_area_expansion(session_hist: pd.DataFrame, value_area_pct: float) -> tuple[float, float, float]:
    hist = session_hist.sort_values("price_bin").reset_index(drop=True)
    total_volume = hist["volume"].sum()
    poc_idx = hist["volume"].idxmax()
    poc_price = hist.loc[poc_idx, "price_bin"]

    lo_idx, hi_idx = poc_idx, poc_idx
    accumulated = hist.loc[poc_idx, "volume"]
    target = total_volume * value_area_pct

    while accumulated < target and (lo_idx > 0 or hi_idx < len(hist) - 1):
        vol_below = hist.loc[lo_idx - 1, "volume"] if lo_idx > 0 else -1
        vol_above = hist.loc[hi_idx + 1, "volume"] if hi_idx < len(hist) - 1 else -1
        if vol_above >= vol_below:
            hi_idx += 1
            accumulated += hist.loc[hi_idx, "volume"]
        else:
            lo_idx -= 1
            accumulated += hist.loc[lo_idx, "volume"]

    return poc_price, hist.loc[hi_idx, "price_bin"], hist.loc[lo_idx, "price_bin"]


def compute_session_profiles(con: duckdb.DuckDBPyConnection, tick_paths: List[str], output_dir: Path, bin_size: float, value_area_pct: float) -> None:
    hist = _session_histogram(con, tick_paths, bin_size)
    if hist.empty:
        logger.warning("No tick data found for volume profile computation.")
        return
    ranges = _session_range(con, tick_paths)

    annotated_frames, summary_rows = [], []
    for session, group in hist.groupby("session"):
        poc_price, vah_price, val_price = _value_area_expansion(group, value_area_pct)
        group = group.copy()
        group["is_poc"] = np.isclose(group["price_bin"], poc_price)
        group["in_value_area"] = group["price_bin"].between(val_price, vah_price)
        hvn_threshold, lvn_threshold = group["volume"].quantile(0.90), group["volume"].quantile(0.10)
        group["is_hvn"] = group["volume"] >= hvn_threshold
        group["is_lvn"] = group["volume"] <= lvn_threshold
        annotated_frames.append(group)
        summary_rows.append({"session": session, "poc": poc_price, "vah": vah_price, "val": val_price})

    annotated = pd.concat(annotated_frames, ignore_index=True)
    summary = pd.DataFrame(summary_rows).merge(ranges, on="session", how="left").sort_values("session").reset_index(drop=True)

    naked_flags = []
    for i, row in summary.iterrows():
        later = summary.iloc[i + 1:]
        revisited = ((later["day_low"] <= row["poc"]) & (row["poc"] <= later["day_high"])).any()
        naked_flags.append(not revisited)
    summary["is_poc_naked_as_of_latest_session"] = naked_flags

    _atomic_write_parquet(annotated, output_dir / "session_volume_profile.parquet")
    _atomic_write_parquet(summary, output_dir / "session_profile_summary.parquet")


def compute_developing_poc(con: duckdb.DuckDBPyConnection, tick_paths: List[str], output_dir: Path, bin_size: float, resolution_minutes: int = 15) -> None:
    query = f"""
        WITH stepped AS (
            SELECT
                date_trunc('day', to_timestamp(trade_time_ns / 1e9)) AS session,
                time_bucket(INTERVAL '{resolution_minutes} minute', to_timestamp(trade_time_ns / 1e9)) AS step_time,
                floor(price / {bin_size}) * {bin_size} AS price_bin, qty
            FROM read_parquet({format_parquet_path_list(tick_paths)})
        ),
        step_hist AS (
            SELECT session, step_time, price_bin, sum(qty) AS step_volume FROM stepped GROUP BY 1, 2, 3
        ),
        running AS (
            SELECT session, step_time, price_bin,
                sum(step_volume) OVER (PARTITION BY session, price_bin ORDER BY step_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_volume
            FROM step_hist
        )
        SELECT session, step_time, price_bin, running_volume,
            row_number() OVER (PARTITION BY session, step_time ORDER BY running_volume DESC) AS rank_in_step
        FROM running
        QUALIFY rank_in_step = 1
        ORDER BY session, step_time
    """
    df = con.execute(query).fetch_df().drop(columns=["rank_in_step"]).rename(columns={"price_bin": "developing_poc"})
    _atomic_write_parquet(df, output_dir / "developing_poc.parquet")
