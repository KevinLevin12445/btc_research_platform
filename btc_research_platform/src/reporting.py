"""
Per-month Data Quality Report — generated automatically after every
processed month, per Part II's "Data Quality Report" spec: trades
processed, rows rejected, duplicates, timestamp anomalies, validation
summary, generated features, database growth, current database size,
estimated remaining history, missing months, execution time, memory
usage, warnings, recommendations — "should allow me to determine
immediately whether the month is safe to archive."

Written to reports/month_report_<YYYY-MM>_<timestamp>.md and logged.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Optional

from .discovery import missing_months, next_required_month
from .state import MonthRecord, StateStore
from .verification import VerificationResult

logger = logging.getLogger(__name__)


def _dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def _fmt_gb(n_bytes: float) -> str:
    return f"{n_bytes / 1e9:.3f} GB"


def generate_month_report(
    config,
    record: MonthRecord,
    verification: VerificationResult,
    elapsed_seconds: float,
    configured_start_month: str,
    safe_to_delete_source: bool,
) -> str:
    state = StateStore(config.paths.state_db)
    try:
        cumulative = state.cumulative_stats()
        gaps = missing_months(state, configured_start_month)
        next_month = next_required_month(state, configured_start_month)
    finally:
        state.close()

    warehouse_size = _dir_size_bytes(config.paths.warehouse_dir)
    ticks_size = _dir_size_bytes(config.paths.ticks_dir)

    lines = []
    a = lines.append
    a(f"# Data Quality Report — {record.symbol} {record.year_month}")
    a(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    a("")

    a("## This Month")
    a(f"- Source file: {record.source_path} ({record.source_format})")
    a(f"- Source SHA-256: {record.source_sha256}")
    sidecar = "not present" if record.checksum_sidecar_verified is None else ("MATCH" if record.checksum_sidecar_verified else "MISMATCH")
    a(f"- Binance .CHECKSUM sidecar: {sidecar}")
    a(f"- Trades processed (written to Parquet): {record.rows_written:,}")
    a(f"- Trade ID range: [{record.min_trade_id:,}, {record.max_trade_id:,}]")
    a(f"- Rows rejected (invalid price/qty): {record.n_price_or_qty_anomalies:,}")
    a(f"- Duplicate trade IDs: {record.n_duplicate_trade_ids:,} "
      f"(benign duplicate rows: {record.n_duplicate_rows:,}; CONFLICTING — same ID, different data: "
      f"{record.n_duplicate_trade_ids - record.n_duplicate_rows:,})")
    a(f"- Trade ID gaps: {record.n_trade_id_gaps:,} gaps, {record.total_missing_trade_ids:,} total missing IDs")
    a(f"- Non-monotonic timestamps: {record.n_nonmonotonic_time:,}")
    a(f"- Abnormal single-trade price jumps (>{config.processing.abnormal_price_jump_pct*100:.1f}%): {record.n_abnormal_price_jumps:,}")
    a(f"- Execution time: {elapsed_seconds:.1f}s")
    a("")

    a("## Independent Verification")
    a(f"- Result: {'PASSED' if verification.ok else 'FAILED'}")
    a(f"- Detail: {verification.detail}")
    a(f"- Safe to delete source file: {'YES' if safe_to_delete_source else 'NO'}")
    a("")

    a("## Generated Datasets")
    a(f"- Tick Parquet: {record.output_path}")
    a("- Bars: 1s/1min/5min/15min/30min/1h/4h/1d/1w/1mo (with per-bar VWAP)")
    a("- Features: delta/cumulative delta (multi-resolution), footprint, volume profile, session VWAP, ATR, realized volatility")
    a("")

    a("## Database Growth")
    a(f"- Warehouse size (ticks+bars+features): {_fmt_gb(warehouse_size)}")
    a(f"- Tick layer size: {_fmt_gb(ticks_size)}")
    a(f"- Months completed: {cumulative['months_done']}")
    a(f"- Cumulative trades: {cumulative['total_trades']:,}")
    a(f"- Coverage: {cumulative['earliest_month']} to {cumulative['latest_month']}")
    a("")

    a("## Continuity")
    if gaps:
        a(f"- **WARNING: {len(gaps)} gap(s) in the sequence**: {gaps}")
    else:
        a("- No gaps — fully continuous from the configured start month.")
    a(f"- **Next month required: {next_month}**")
    a("")

    warnings = []
    n_conflicting = record.n_duplicate_trade_ids - record.n_duplicate_rows
    if n_conflicting:
        warnings.append(f"{n_conflicting} CONFLICTING duplicate trade ID(s) — same ID, different data. Investigate before trusting this month.")
    if record.n_trade_id_gaps and record.total_missing_trade_ids > 0.001 * record.rows_written:
        warnings.append(f"Trade ID gaps total {record.total_missing_trade_ids:,} — over 0.1% of this month's volume. Worth investigating whether this is normal exchange behavior or a data completeness issue.")
    if not verification.ok:
        warnings.append("Independent verification FAILED — this month is NOT marked done and its source file must NOT be deleted.")
    if gaps:
        warnings.append(f"Sequence gaps detected: {gaps} — the incremental workflow assumes strict month-by-month continuity.")

    a("## Warnings")
    if warnings:
        for w in warnings:
            a(f"- {w}")
    else:
        a("- None.")
    a("")

    a("## Recommendations")
    if safe_to_delete_source:
        a(f"- This month's source file is verified and safe to delete: `python main.py --delete-source {record.year_month}`")
    a(f"- Download and place the next month's Futures archive: `{next_month}`")

    report_text = "\n".join(lines)
    config.paths.reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = config.paths.reports_dir / f"month_report_{record.year_month}_{time.strftime('%Y%m%d_%H%M%S')}.md"
    report_path.write_text(report_text, encoding="utf-8")
    logger.info("Month report written to %s", report_path)
    return report_text
