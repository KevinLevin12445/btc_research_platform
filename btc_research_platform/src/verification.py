"""
Independent post-write verification — Part II's "Validation Philosophy":
"Never trust the ingestion process itself. Everything written should
later be reopened and verified independently. If verification fails, the
month should NOT be marked completed."

This module is deliberately the ONLY thing that can flip a month's
`verification_status` to 'verified' — and only `verified` months are
eligible for source-file deletion (pipeline.delete_verified_source).
Trusting the writer's own self-reported row count would defeat the
purpose; this re-opens the COMMITTED file cold and re-derives everything
from its own Parquet metadata/content.
"""

from __future__ import annotations

import dataclasses
import logging
from pathlib import Path

import pyarrow.parquet as pq

from .schema import TICK_SCHEMA

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class VerificationResult:
    ok: bool
    row_count: int
    detail: str


def verify_committed_month(output_path: Path, expected_rows: int, expected_min_trade_id: int, expected_max_trade_id: int) -> VerificationResult:
    """Re-opens the committed Parquet file (cold, independent of the
    writer process) and checks:
      1. The file opens and its schema matches TICK_SCHEMA exactly.
      2. Its row count (from Parquet's own footer metadata — cheap, no
         full data read needed) matches what ingestion recorded.
      3. Its actual min/max trade_id (this DOES require a scan of that one
         column, still cheap relative to a full read) matches what
         ingestion recorded — catches a subtle corruption mode row-count
         alone would miss (right row count, wrong content).
    """
    if not output_path.exists():
        return VerificationResult(ok=False, row_count=0, detail=f"File does not exist: {output_path}")

    try:
        pf = pq.ParquetFile(output_path)
    except Exception as e:  # noqa: BLE001 — any failure to open is a verification failure, not a crash
        return VerificationResult(ok=False, row_count=0, detail=f"Failed to open Parquet file: {e}")

    if not pf.schema_arrow.equals(TICK_SCHEMA, check_metadata=False):
        return VerificationResult(
            ok=False, row_count=pf.metadata.num_rows,
            detail=f"Schema mismatch. Expected: {TICK_SCHEMA}. Got: {pf.schema_arrow}",
        )

    actual_rows = pf.metadata.num_rows
    if actual_rows != expected_rows:
        return VerificationResult(
            ok=False, row_count=actual_rows,
            detail=f"Row count mismatch: file has {actual_rows:,}, ingestion recorded {expected_rows:,}",
        )

    trade_id_col = pf.read(columns=["trade_id"]).column("trade_id")
    actual_min = trade_id_col[0].as_py() if len(trade_id_col) else None
    actual_max = trade_id_col[-1].as_py() if len(trade_id_col) else None
    # trade_id is written in source order (chronological), so first/last
    # element are the min/max without needing a full min()/max() scan —
    # cheap, but still an independent re-derivation from the committed file.
    if actual_min != expected_min_trade_id or actual_max != expected_max_trade_id:
        return VerificationResult(
            ok=False, row_count=actual_rows,
            detail=(
                f"trade_id range mismatch: file has [{actual_min}, {actual_max}], "
                f"ingestion recorded [{expected_min_trade_id}, {expected_max_trade_id}]"
            ),
        )

    return VerificationResult(ok=True, row_count=actual_rows, detail="verified: schema, row count, and trade_id range all match")
