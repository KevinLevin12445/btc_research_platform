"""
Lightweight, streaming-friendly validation — runs per-chunk (never needs a
whole month in memory), accumulating a running `ValidationReport` that
gets persisted to the state DB and the per-month Data Quality Report.

Philosophy, directly from Part II: "Never silently repair data. Always
report every detected issue." Nothing here mutates values or fills gaps.
The only rows ever dropped are genuinely invalid ones (non-positive/NaN
price or qty) — everything else (gaps, duplicates, abnormal jumps,
non-monotonic timestamps) is counted and reported, never corrected.

Checks implemented, mapped to Part II's explicit list:
  - timestamp validation / chronological consistency -> n_nonmonotonic_time
  - missing record detection -> n_trade_id_gaps / total_missing_trade_ids
  - duplicate trade ID detection -> n_duplicate_trade_ids (same ID seen twice)
  - duplicate row detection -> n_duplicate_rows (same ID AND identical
    content — the benign case) vs n_conflicting_duplicate_trade_ids (same
    ID, DIFFERENT content — genuine corruption, tracked separately because
    it's a materially more serious signal)
  - abnormal value detection -> n_price_or_qty_anomalies (non-positive/NaN,
    the only case actually filtered out) and n_abnormal_price_jumps
    (large single-trade price moves, COUNTED not filtered — a real flash
    move is data, not noise)
"""

from __future__ import annotations

import dataclasses

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc


@dataclasses.dataclass
class ValidationReport:
    rows_seen: int = 0
    n_price_or_qty_anomalies: int = 0
    n_nonmonotonic_time: int = 0
    n_trade_id_gaps: int = 0
    total_missing_trade_ids: int = 0
    n_duplicate_trade_ids: int = 0
    n_duplicate_rows: int = 0
    n_conflicting_duplicate_trade_ids: int = 0
    n_abnormal_price_jumps: int = 0
    min_trade_id: int | None = None
    max_trade_id: int | None = None
    min_trade_time_ns: int | None = None
    max_trade_time_ns: int | None = None

    _last_trade_time_ns: int | None = dataclasses.field(default=None, repr=False)
    _last_trade_id: int | None = dataclasses.field(default=None, repr=False)
    _last_price: float | None = dataclasses.field(default=None, repr=False)
    _last_qty: float | None = dataclasses.field(default=None, repr=False)
    _last_is_buyer_maker: bool | None = dataclasses.field(default=None, repr=False)

    abnormal_price_jump_pct: float = 0.05  # 5% single-trade move; configurable via set_thresholds

    def set_thresholds(self, abnormal_price_jump_pct: float) -> None:
        self.abnormal_price_jump_pct = abnormal_price_jump_pct

    def update(self, table: pa.Table) -> None:
        n = table.num_rows
        if n == 0:
            return
        self.rows_seen += n

        prices = table.column("price").to_numpy(zero_copy_only=False)
        qtys = table.column("qty").to_numpy(zero_copy_only=False)
        times = table.column("trade_time_ns").to_numpy(zero_copy_only=False)
        ids = table.column("trade_id").to_numpy(zero_copy_only=False)
        is_buyer_maker = table.column("is_buyer_maker").to_numpy(zero_copy_only=False)

        # --- abnormal economic values ---
        anomalies = np.count_nonzero((prices <= 0) | (qtys <= 0) | np.isnan(prices) | np.isnan(qtys))
        self.n_price_or_qty_anomalies += int(anomalies)

        # --- chronological consistency ---
        time_diffs = np.diff(times)
        nonmonotonic = int(np.count_nonzero(time_diffs < 0))
        if self._last_trade_time_ns is not None and times[0] < self._last_trade_time_ns:
            nonmonotonic += 1
        self.n_nonmonotonic_time += nonmonotonic

        # --- trade_id continuity: gaps AND duplicates share the same diff computation ---
        id_diffs = np.diff(ids)
        gap_mask = id_diffs > 1
        self.n_trade_id_gaps += int(np.count_nonzero(gap_mask))
        self.total_missing_trade_ids += int(np.sum(id_diffs[gap_mask] - 1))

        dup_mask = id_diffs == 0
        n_dups_in_chunk = int(np.count_nonzero(dup_mask))
        self.n_duplicate_trade_ids += n_dups_in_chunk
        if n_dups_in_chunk:
            dup_indices = np.nonzero(dup_mask)[0]  # index i means ids[i] == ids[i+1]
            same_content = (
                (prices[dup_indices] == prices[dup_indices + 1])
                & (qtys[dup_indices] == qtys[dup_indices + 1])
                & (is_buyer_maker[dup_indices] == is_buyer_maker[dup_indices + 1])
            )
            self.n_duplicate_rows += int(np.count_nonzero(same_content))
            self.n_conflicting_duplicate_trade_ids += int(np.count_nonzero(~same_content))

        # cross-chunk-boundary gap/duplicate check
        if self._last_trade_id is not None:
            boundary_diff = int(ids[0]) - self._last_trade_id
            if boundary_diff > 1:
                self.n_trade_id_gaps += 1
                self.total_missing_trade_ids += boundary_diff - 1
            elif boundary_diff == 0:
                self.n_duplicate_trade_ids += 1
                if (
                    self._last_price == prices[0] and self._last_qty == qtys[0]
                    and self._last_is_buyer_maker == is_buyer_maker[0]
                ):
                    self.n_duplicate_rows += 1
                else:
                    self.n_conflicting_duplicate_trade_ids += 1

        # --- abnormal single-trade price jumps (counted, never filtered) ---
        with np.errstate(divide="ignore", invalid="ignore"):
            pct_change = np.abs(np.diff(prices) / prices[:-1])
        self.n_abnormal_price_jumps += int(np.count_nonzero(pct_change > self.abnormal_price_jump_pct))
        if self._last_price is not None and self._last_price > 0:
            boundary_pct = abs(prices[0] - self._last_price) / self._last_price
            if boundary_pct > self.abnormal_price_jump_pct:
                self.n_abnormal_price_jumps += 1

        self._last_trade_time_ns = int(times[-1])
        self._last_trade_id = int(ids[-1])
        self._last_price = float(prices[-1])
        self._last_qty = float(qtys[-1])
        self._last_is_buyer_maker = bool(is_buyer_maker[-1])

        chunk_min_id, chunk_max_id = int(ids.min()), int(ids.max())
        chunk_min_t, chunk_max_t = int(times.min()), int(times.max())
        self.min_trade_id = chunk_min_id if self.min_trade_id is None else min(self.min_trade_id, chunk_min_id)
        self.max_trade_id = chunk_max_id if self.max_trade_id is None else max(self.max_trade_id, chunk_max_id)
        self.min_trade_time_ns = chunk_min_t if self.min_trade_time_ns is None else min(self.min_trade_time_ns, chunk_min_t)
        self.max_trade_time_ns = chunk_max_t if self.max_trade_time_ns is None else max(self.max_trade_time_ns, chunk_max_t)

    def filter_valid_rows(self, table: pa.Table) -> pa.Table:
        """Drop rows with non-positive/NaN price or qty before they hit
        storage. Everything else (gaps, duplicates, jumps, non-monotonic
        timestamps) is recorded but NOT dropped — see module docstring.
        """
        price = table.column("price")
        qty = table.column("qty")
        mask = pc.and_(pc.greater(price, 0), pc.greater(qty, 0))
        return table.filter(mask)
