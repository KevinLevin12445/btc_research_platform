"""
Schema definition and normalization for Binance USD-M Futures monthly
"trades" data (Binance Vision or live API export, ZIP or CSV).

CONFIRMED (via Binance's own binance-public-data schema documentation,
cross-checked directly against this project's real files) — trades file
column layout differs BY MARKET, not by file vintage:

    SPOT:    trade_id, price, qty, quote_qty, time, is_buyer_maker, is_best_match  (7 columns)
    FUTURES: trade_id, price, qty, quote_qty, time, is_buyer_maker                  (6 columns)

`is_best_match` is a byproduct of Spot's Smart-Order-Routing best-price
matching — it has no equivalent concept in the Futures matching engine and
is never present in genuine Futures trade exports. This directly resolved
a real incident in this project: a batch of "BTCUSDT-trades-*.csv" files
turned out to be Spot data (7 columns) mistakenly present in what was
meant to be a pure Futures research folder.

CONSEQUENCE FOR THIS MODULE: column count is treated as a MARKET IDENTITY
CHECK, not a schema variant to tolerate. Exactly 6 columns is required;
7 columns raises WrongMarketError (this is very likely Spot data, not a
Futures peculiarity); anything else raises CorruptedArchiveError (a
genuinely unrecognized layout, a different failure mode). This is a
deliberate correction from an earlier version of this project, which
null-filled a possibly-missing 7th column — that was solving a problem
that didn't actually exist once the real cause (wrong market, not a
missing column) was understood. See errors.py for both exception types.

IMPORTANT CORRECTNESS NOTE — timestamp precision:
Binance changed trade timestamps from millisecond to microsecond epoch
values for spot data starting 2025-01-01, and futures data around the same
period. Every timestamp value is classified individually by magnitude and
normalized to nanoseconds, rather than branching on file date (the exact
futures cutover date isn't cleanly documented):

    value >= timestamp_unit_threshold (default 1e15)  -> microseconds
    value <  timestamp_unit_threshold                  -> milliseconds

A millisecond epoch timestamp for any realistic date is ~13 digits
(~1.7-1.9e12); a microsecond one for the same date is ~16 digits
(~1.7-1.9e15). 1e15 sits cleanly between the two with a huge margin.
"""

from __future__ import annotations

import numpy as np
import pyarrow as pa

# Binance USD-M Futures trades file column order (headerless in most
# files; see source_input.detect_header for files that do include one).
FUTURES_RAW_COLUMNS = ["trade_id", "price", "qty", "quote_qty", "trade_time_raw", "is_buyer_maker"]

# Output schema. `symbol` is constant per file (near-zero storage cost via
# Parquet dictionary/RLE encoding) — future-proofing for multi-symbol
# Futures expansion (e.g. ETHUSDT) without ever needing to rebuild
# already-ingested months. No `is_best_match` column: see module docstring
# — it is never legitimately present in Futures data, so carrying an
# always-null column forward would document a wrong assumption, not a
# real possibility.
TICK_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("trade_id", pa.int64()),
        pa.field("price", pa.float64()),
        pa.field("qty", pa.float64()),
        pa.field("quote_qty", pa.float64()),
        pa.field("trade_time_ns", pa.int64()),
        pa.field("is_buyer_maker", pa.bool_()),
    ]
)

# Deliberate non-change, stated explicitly since it's a locked-in decision:
# price/qty stay float64, not decimal128. float64 has far more precision
# than BTC's price ticks need at any realistic price level, and DuckDB/
# Arrow aggregates are materially faster over float64. Exact-decimal
# storage matters more for settlement/accounting systems than analytical
# research databases.


def normalize_timestamps(raw_values: np.ndarray, unit_threshold: float) -> np.ndarray:
    """Vectorized per-value ms/us classification, normalized to int64 nanoseconds.

    Applied per-row (not per-file) deliberately: immune to a file that
    straddles the precision change, or a stray malformed value.
    """
    raw_values = raw_values.astype(np.float64)
    is_microseconds = raw_values >= unit_threshold
    ns = np.where(is_microseconds, raw_values * 1_000.0, raw_values * 1_000_000.0)
    return ns.astype(np.int64)
