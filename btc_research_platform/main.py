#!/usr/bin/env python
"""
BTC Research Platform — CLI entry point (Phase 1: Ingestion, Validation,
Storage, Core Features).

Usage:
    python main.py --stage discover     # verify whatever's in the inbox folder, report, NO ingestion
    python main.py --stage ingest       # process pending inbox file(s), sequentially, checkpoint-resumable
    python main.py --stage bars         # (re)generate all bars from currently-available ticks
    python main.py --stage features     # (re)generate all feature tables
    python main.py --status             # per-month status table + next-required-month
    python main.py --delete-source 2023-01   # explicit, verified deletion of one month's source file

Incremental workflow: download ONE month's Futures archive (zip or csv)
into paths.raw_data_dir, run --stage ingest, review the auto-generated
report, delete the source once confirmed safe, repeat.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.bars import generate_all_bars  # noqa: E402
from src.config import load_config  # noqa: E402
from src.discovery import missing_months, next_required_month, scan_inbox, verify_source_file  # noqa: E402
from src.logging_setup import setup_logging  # noqa: E402
from src.pipeline import delete_verified_source, get_all_tick_paths, run_feature_generation, run_ingestion  # noqa: E402
from src.state import StateStore  # noqa: E402


def cmd_discover(config) -> None:
    inbox_files = scan_inbox(config.paths.raw_data_dir, config.project.symbol)
    state = StateStore(config.paths.state_db)
    try:
        next_month = next_required_month(state, config.project.start_month)
        gaps = missing_months(state, config.project.start_month)
    finally:
        state.close()

    print(f"Inbox folder: {config.paths.raw_data_dir}")
    print(f"Next required month: {next_month}")
    if gaps:
        print(f"WARNING — sequence gaps: {gaps}")

    if not inbox_files:
        print("No recognized Futures trades file currently in the inbox.")
        return

    state = StateStore(config.paths.state_db)
    try:
        for f in inbox_files:
            record = verify_source_file(f.path, state, deep=True)
            print(f"\n{f.path.name} ({f.year_month}):")
            print(f"  status: {record.status}")
            if record.detail:
                print(f"  detail: {record.detail}")
            if record.status == "ok":
                print(f"  sha256: {record.sha256}")
                sidecar = "not present" if record.checksum_sidecar_verified is None else ("MATCH" if record.checksum_sidecar_verified else "MISMATCH")
                print(f"  .CHECKSUM sidecar: {sidecar}")
    finally:
        state.close()


def cmd_status(config) -> None:
    state = StateStore(config.paths.state_db)
    try:
        records = state.list_all()
        next_month = next_required_month(state, config.project.start_month)
        gaps = missing_months(state, config.project.start_month)
        cumulative = state.cumulative_stats()
    finally:
        state.close()

    if not records:
        print("No processing history yet.")
    else:
        header = f"{'YEAR-MONTH':<10} {'STATUS':<16} {'CHECKPOINT':<20} {'VERIFICATION':<18} {'ROWS':>14}"
        print(header)
        print("-" * len(header))
        for r in records:
            rows = f"{r.rows_written:,}" if r.rows_written is not None else "-"
            print(f"{r.year_month:<10} {r.status:<16} {(r.checkpoint or '-'):<20} {(r.verification_status or '-'):<18} {rows:>14}")

    print(f"\nMonths done: {cumulative['months_done']}  |  Cumulative trades: {cumulative['total_trades']:,}")
    print(f"Coverage: {cumulative['earliest_month']} to {cumulative['latest_month']}")
    print(f"Next required month: {next_month}")
    if gaps:
        print(f"WARNING — sequence gaps: {gaps}")


def main() -> None:
    parser = argparse.ArgumentParser(description="BTC Research Platform — Phase 1")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--stage", choices=["discover", "ingest", "bars", "features"])
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--delete-source", metavar="YYYY-MM")
    parser.add_argument("--force", action="store_true", help="Reprocess a month even if already fully reported")
    args = parser.parse_args()

    config = load_config(args.config)
    setup_logging(config.paths.log_dir)

    if args.status:
        cmd_status(config)
        return

    if args.delete_source:
        path = delete_verified_source(config, args.delete_source)
        print(f"Deleted: {path}")
        return

    if not args.stage:
        parser.error("--stage is required (or use --status / --delete-source)")

    if args.stage == "discover":
        cmd_discover(config)
    elif args.stage == "ingest":
        run_ingestion(config, force=args.force)
    elif args.stage == "bars":
        generate_all_bars(config, get_all_tick_paths(config))
    elif args.stage == "features":
        run_feature_generation(config, get_all_tick_paths(config))


if __name__ == "__main__":
    main()
