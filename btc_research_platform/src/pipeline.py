"""
Orchestration — sequential, not multiprocess (see README "Why sequential
ingestion" for the reasoning: the incremental one-month-at-a-time workflow
rarely has a batch to parallelize, so ProcessPoolExecutor's complexity
wasn't earning its cost).

CHECKPOINT-AWARE RESUMABILITY: each month's record tracks a `checkpoint`
(source_verified -> ticks_written -> ticks_verified -> features_generated
-> reported). A crash after ticks_verified but before features_generated
resumes AT feature generation on restart, not from scratch — "restarting
should continue from the last successful step" means the step, not just
the month.

Every state-DB write here uses `update_fields` (touches only the given
columns) after an initial `create_if_not_exists` — never a whole-record
upsert, which would silently null out fields set in earlier steps. See
state.py for why that distinction matters.
"""

from __future__ import annotations

import logging
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import List

from .bars import generate_all_bars
from .discovery import InboxFile, next_required_month, scan_inbox, verify_source_file
from .duckdb_utils import create_duckdb_connection
from .errors import CorruptedArchiveError, DiskSpaceGuardError, VerificationFailedError, WrongMarketError
from .features import footprint as footprint_features
from .features import orderflow as orderflow_features
from .features import volatility as volatility_features
from .features import volume_profile as volume_profile_features
from .features import vwap as vwap_features
from .parquet_store import MonthlyParquetWriter, free_space_gb
from .reporting import generate_month_report
from .source_input import iter_trade_chunks
from .state import StateStore
from .validation import ValidationReport
from .verification import verify_committed_month
from .versions import FEATURE_VERSION, SCHEMA_VERSION, SOFTWARE_VERSION

logger = logging.getLogger(__name__)


@dataclass
class MonthResult:
    year_month: str
    ok: bool
    message: str = ""
    safe_to_delete_source: bool = False


def month_output_path(config, year: int, year_month: str) -> Path:
    return config.paths.ticks_dir / config.project.symbol / str(year) / f"{config.project.symbol}_ticks_{year_month}.parquet"


def get_all_tick_paths(config) -> List[str]:
    """The authoritative list of every currently-accessible tick Parquet
    file, resolved from the state DB (never a directory glob).
    """
    state = StateStore(config.paths.state_db)
    try:
        records = [r for r in state.list_all() if r.status == "done" and r.output_path]
    finally:
        state.close()

    available, missing = [], []
    for r in records:
        if Path(r.output_path).exists():
            available.append(r.output_path)
        else:
            missing.append((r.year_month, r.output_path))
    if missing:
        logger.warning("%d month(s) recorded done but file not accessible: %s", len(missing), missing)
    logger.info("Resolved %d currently-accessible tick file(s) out of %d processed month(s).", len(available), len(records))
    return available


def run_feature_generation(config, tick_paths: List[str]) -> None:
    if not tick_paths:
        logger.warning("No tick paths available — skipping feature generation entirely.")
        return

    symbol = config.project.symbol
    bars_dir = config.paths.bars_dir / symbol
    bars_1min_path = (bars_dir / f"{symbol}_1min.parquet").as_posix()
    bars_1h_path = (bars_dir / f"{symbol}_1h.parquet").as_posix()
    feat_dir = config.paths.features_dir / symbol
    fcfg = config.features

    con = create_duckdb_connection(config)
    try:
        logger.info("Generating order-flow features (delta / cumulative delta) at %s...", fcfg.orderflow_intervals)
        orderflow_paths = {}
        for interval in fcfg.orderflow_intervals:
            bars_path = (bars_dir / f"{symbol}_{interval}.parquet").as_posix()
            out_path = feat_dir / f"orderflow_{interval}.parquet"
            orderflow_features.compute_orderflow_bars(con, bars_path, out_path)
            orderflow_paths[interval] = out_path

        orderflow_features.detect_large_trades(con, tick_paths, feat_dir / "large_trades.parquet", fcfg.large_trade_quantile)
        orderflow_features.detect_stacked_imbalances(
            con, bars_1min_path, feat_dir / "stacked_imbalances.parquet", fcfg.stacked_imbalance_ratio, fcfg.stacked_imbalance_min_bars
        )
        finest = orderflow_paths.get("1min", next(iter(orderflow_paths.values())))
        orderflow_features.detect_delta_divergence(con, finest.as_posix(), feat_dir / "delta_divergence.parquet", lookback_bars=20)
        orderflow_features.detect_absorption_candidates(con, bars_1min_path, feat_dir / "absorption_candidates.parquet")
        orderflow_features.detect_iceberg_candidates(con, tick_paths, feat_dir / "iceberg_candidates.parquet")

        logger.info("Generating session VWAP...")
        vwap_features.compute_session_vwap(con, bars_1min_path, feat_dir / "session_vwap_1min.parquet")

        logger.info("Generating footprint (full tick-corpus scan — the slow step)...")
        footprint_features.compute_footprint(con, tick_paths, feat_dir / "footprint.parquet", fcfg.footprint_bar_interval, fcfg.footprint_bin_size)

        logger.info("Generating volume-profile features...")
        volume_profile_features.compute_session_profiles(con, tick_paths, feat_dir, fcfg.profile_bin_size, fcfg.value_area_pct)
        volume_profile_features.compute_developing_poc(con, tick_paths, feat_dir, fcfg.profile_bin_size)

        logger.info("Generating volatility features...")
        volatility_features.compute_atr(con, bars_1h_path, feat_dir / "atr_1h.parquet", fcfg.atr_period)
        volatility_features.compute_realized_volatility(con, bars_1h_path, feat_dir / "realized_vol_1h.parquet", fcfg.realized_vol_window)
    finally:
        con.close()


def _regenerate_bars_and_features(config) -> None:
    tick_paths = get_all_tick_paths(config)
    generate_all_bars(config, tick_paths)
    run_feature_generation(config, tick_paths)


def process_one_month(inbox_file: InboxFile, config, force: bool = False) -> MonthResult:
    """Sequential, checkpoint-aware processing of one inbox file. Never
    raises — errors are caught, logged, and reflected in the returned
    MonthResult / state DB.
    """
    state = StateStore(config.paths.state_db)
    symbol, year_month = inbox_file.symbol, inbox_file.year_month
    start_time = time.time()

    try:
        existing = state.create_if_not_exists(symbol, year_month, source_path=str(inbox_file.path), source_format=inbox_file.format)
        if existing.checkpoint == "reported" and not force:
            return MonthResult(year_month, ok=True, message="already fully processed (checkpoint='reported')")

        # --- checkpoint: source_verified ---
        verification_record = verify_source_file(inbox_file.path, state, deep=True)
        if verification_record.status != "ok":
            state.update_fields(
                symbol, year_month, status="failed", checkpoint="source_verified",
                error_message=f"source verification failed: {verification_record.detail}",
                started_at=start_time, finished_at=time.time(),
            )
            return MonthResult(year_month, ok=False, message=f"source verification failed: {verification_record.detail}")

        final_path = month_output_path(config, inbox_file.year, year_month)
        skip_ingestion = existing.checkpoint in ("ticks_written", "ticks_verified", "features_generated") and final_path.exists()

        if not skip_ingestion:
            state.update_fields(
                symbol, year_month, status="in_progress", checkpoint="source_verified",
                source_size_bytes=inbox_file.size_bytes, source_sha256=verification_record.sha256,
                checksum_sidecar_verified=verification_record.checksum_sidecar_verified,
                schema_version=SCHEMA_VERSION, feature_version=FEATURE_VERSION, software_version=SOFTWARE_VERSION,
                started_at=start_time,
            )

            report = ValidationReport()
            report.set_thresholds(config.processing.abnormal_price_jump_pct)
            writer = MonthlyParquetWriter(final_path, config.processing.min_free_disk_gb, row_group_size=config.processing.parquet_row_group_size)
            try:
                for chunk in iter_trade_chunks(inbox_file.path, symbol, config.processing.chunk_rows, config.processing.timestamp_unit_threshold):
                    report.update(chunk)
                    writer.write(report.filter_valid_rows(chunk))
                writer.finalize()
            except DiskSpaceGuardError:
                writer.abort()
                state.update_fields(symbol, year_month, status="paused_low_disk", finished_at=time.time())
                return MonthResult(year_month, ok=False, message="paused: low disk space")
            except Exception:
                writer.abort()
                raise

            state.update_fields(
                symbol, year_month, checkpoint="ticks_written",
                rows_written=report.rows_seen, output_path=str(final_path),
                min_trade_id=report.min_trade_id, max_trade_id=report.max_trade_id,
                min_trade_time_ns=report.min_trade_time_ns, max_trade_time_ns=report.max_trade_time_ns,
                n_price_or_qty_anomalies=report.n_price_or_qty_anomalies, n_nonmonotonic_time=report.n_nonmonotonic_time,
                n_trade_id_gaps=report.n_trade_id_gaps, total_missing_trade_ids=report.total_missing_trade_ids,
                n_duplicate_trade_ids=report.n_duplicate_trade_ids, n_duplicate_rows=report.n_duplicate_rows,
                n_abnormal_price_jumps=report.n_abnormal_price_jumps,
            )
            recorded_rows, recorded_min_id, recorded_max_id = report.rows_seen, report.min_trade_id, report.max_trade_id
        else:
            recorded = state.get_record(symbol, year_month)
            recorded_rows, recorded_min_id, recorded_max_id = recorded.rows_written, recorded.min_trade_id, recorded.max_trade_id

        # --- checkpoint: ticks_verified (independent re-verification — Part II Validation Philosophy) ---
        verification = verify_committed_month(final_path, recorded_rows, recorded_min_id, recorded_max_id)
        if not verification.ok:
            state.update_fields(symbol, year_month, status="failed", error_message=f"independent verification failed: {verification.detail}", finished_at=time.time())
            raise VerificationFailedError(verification.detail)

        state.update_fields(symbol, year_month, status="done", checkpoint="ticks_verified", verification_status="verified", verified_at=time.time())

        # --- checkpoint: features_generated ---
        _regenerate_bars_and_features(config)
        state.update_fields(symbol, year_month, checkpoint="features_generated")

        # --- checkpoint: reported ---
        record = state.get_record(symbol, year_month)
        elapsed = time.time() - start_time
        generate_month_report(config, record, verification, elapsed, config.project.start_month, safe_to_delete_source=True)
        state.update_fields(symbol, year_month, checkpoint="reported", finished_at=time.time())

        return MonthResult(year_month, ok=True, message=f"{recorded_rows:,} rows, verified, features generated", safe_to_delete_source=True)

    except (CorruptedArchiveError, WrongMarketError) as e:
        logger.error("%s for %s: %s", type(e).__name__, year_month, e)
        state.update_fields(symbol, year_month, status="failed", error_message=str(e), finished_at=time.time())
        return MonthResult(year_month, ok=False, message=str(e))
    except VerificationFailedError as e:
        return MonthResult(year_month, ok=False, message=f"verification failed: {e}")
    except Exception as e:  # noqa: BLE001
        logger.error("Unexpected failure processing %s: %s\n%s", year_month, e, traceback.format_exc())
        state.update_fields(symbol, year_month, status="failed", error_message=str(e), finished_at=time.time())
        return MonthResult(year_month, ok=False, message=f"error: {e}")
    finally:
        state.close()


def run_ingestion(config, force: bool = False) -> List[MonthResult]:
    results = resume_stalled_months(config)

    inbox_files = scan_inbox(config.paths.raw_data_dir, config.project.symbol)
    if not inbox_files:
        state = StateStore(config.paths.state_db)
        try:
            next_month = next_required_month(state, config.project.start_month)
        finally:
            state.close()
        logger.info("Inbox is empty. Next required month: %s", next_month)
        return results

    for f in sorted(inbox_files, key=lambda x: x.year_month):
        free_gb = free_space_gb(config.paths.warehouse_dir)
        if free_gb < config.processing.min_free_disk_gb:
            logger.warning("Free space %.2f GB below minimum %.2f GB — stopping.", free_gb, config.processing.min_free_disk_gb)
            break
        result = process_one_month(f, config, force=force)
        results.append(result)
        logger.info("[%s] %s — %s", "OK" if result.ok else "FAILED", result.year_month, result.message)
    return results


def resume_stalled_months(config) -> List[MonthResult]:
    """Months whose ticks are already written AND independently verified,
    but which never reached 'features_generated'/'reported' (e.g. a crash
    between those steps, or the source file was deleted — permitted once
    verification_status='verified', which happens at the 'ticks_verified'
    checkpoint, BEFORE feature generation). These do not need their source
    file: feature generation reads only the already-verified tick Parquet
    (via get_all_tick_paths, state-DB-resolved), never the source. Without
    this, such a month would never be revisited by run_ingestion, since
    that loop only iterates over what scan_inbox finds in the folder.
    """
    state = StateStore(config.paths.state_db)
    try:
        stalled = [r for r in state.list_all() if r.checkpoint == "ticks_verified"]
    finally:
        state.close()

    results = []
    for record in stalled:
        logger.info("Resuming stalled month %s at checkpoint='ticks_verified' (source file not required for this step)...", record.year_month)
        try:
            _regenerate_bars_and_features(config)
            state = StateStore(config.paths.state_db)
            try:
                state.update_fields(record.symbol, record.year_month, checkpoint="features_generated")
                updated_record = state.get_record(record.symbol, record.year_month)
            finally:
                state.close()

            verification = verify_committed_month(Path(record.output_path), record.rows_written, record.min_trade_id, record.max_trade_id)
            elapsed = 0.0  # not meaningfully measurable across a resumed, possibly-multi-session gap
            generate_month_report(config, updated_record, verification, elapsed, config.project.start_month, safe_to_delete_source=(record.source_deleted_at is None))

            state = StateStore(config.paths.state_db)
            try:
                state.update_fields(record.symbol, record.year_month, checkpoint="reported", finished_at=time.time())
            finally:
                state.close()
            results.append(MonthResult(record.year_month, ok=True, message="resumed: features generated, reported"))
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to resume stalled month %s: %s\n%s", record.year_month, e, traceback.format_exc())
            results.append(MonthResult(record.year_month, ok=False, message=f"resume failed: {e}"))
    return results


def delete_verified_source(config, year_month: str) -> Path:
    """Explicit, verified deletion of a source file — never automatic.
    Requires status='done' AND verification_status='verified'. The ONLY
    thing in this codebase that deletes a raw source file.
    """
    state = StateStore(config.paths.state_db)
    try:
        record = state.get_record(config.project.symbol, year_month)
        if record is None:
            raise ValueError(f"No record found for {year_month}")
        if record.status != "done" or record.verification_status != "verified":
            raise ValueError(
                f"{year_month} is not safe to delete: status={record.status}, verification_status={record.verification_status}. "
                f"Only fully verified months may have their source deleted."
            )
        source_path = Path(record.source_path)
        if source_path.exists():
            source_path.unlink()
            logger.info("Deleted verified source file: %s", source_path)
        state.update_fields(record.symbol, year_month, source_deleted_at=time.time())
        return source_path
    finally:
        state.close()
