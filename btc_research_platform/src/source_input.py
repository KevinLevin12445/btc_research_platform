"""
Unified source reader: transparently handles both ZIP and CSV inputs,
auto-detected by extension — no manual configuration, per the incremental
workflow where either format might be what's actually downloaded.

ZIP path streams directly from the compressed entry (`zipfile.ZipFile.open()`)
without ever writing a decompressed CSV to disk. CSV path reads the file
directly. Both converge on the same chunked-pyarrow-Table interface.

Corruption detection: for ZIP, zipfile validates CRC-32 as the stream is
read to completion (raises BadZipFile on mismatch) as a second layer on
top of the explicit CRC pass in discovery.py. For CSV, there is no
container-level checksum — the SHA-256 + optional Binance .CHECKSUM sidecar
check (hashing.py) is what stands in for it, run once by discovery.py
before this module ever touches the file.

MARKET IDENTITY CHECK: column count is checked BEFORE any row is parsed.
Exactly 6 columns is required (Futures). 7 columns raises WrongMarketError
— treated as a hard stop, not a warning, because it very likely means the
file is Spot data (see schema.py). Anything else raises
CorruptedArchiveError as a genuinely unrecognized layout.

Public interface used by discovery.py's verification pass (peek_first_lines,
find_inner_csv_name, detect_layout) as well as by ingestion itself — these
are shared, not module-private, since both modules need to answer the same
question ("what does this file actually contain?") at different times.
"""

from __future__ import annotations

import logging
import zipfile
from pathlib import Path
from typing import Iterator, List

import pandas as pd
import pyarrow as pa

from .errors import CorruptedArchiveError, WrongMarketError
from .schema import FUTURES_RAW_COLUMNS, TICK_SCHEMA, normalize_timestamps

logger = logging.getLogger(__name__)

_DTYPES = {
    "trade_id": "int64",
    "price": "float64",
    "qty": "float64",
    "quote_qty": "float64",
    "trade_time_raw": "int64",
    "is_buyer_maker": "bool",
}


def peek_first_lines(stream, n: int = 2) -> List[str]:
    lines = []
    for _ in range(n):
        line = stream.readline()
        if not line:
            break
        lines.append(line.decode("utf-8", errors="replace") if isinstance(line, bytes) else line)
    return [l.strip() for l in lines]


def detect_header(first_data_like_line: str) -> bool:
    """A data row's first field is a numeric trade id; a header row's is not."""
    first_field = first_data_like_line.split(",")[0]
    return not first_field.lstrip("-").isdigit()


def detect_layout(lines: List[str], source_name: str) -> tuple[bool, int]:
    """Returns (has_header, n_columns). Raises WrongMarketError / CorruptedArchiveError."""
    if not lines:
        raise CorruptedArchiveError(f"{source_name}: file appears to be empty")

    header = detect_header(lines[0])
    data_line = lines[1] if header and len(lines) > 1 else lines[0]
    n_columns = len(data_line.split(","))

    if n_columns == 7:
        raise WrongMarketError(
            f"{source_name}: has 7 columns, matching Binance SPOT trades layout "
            f"(includes isBestMatch), not the 6-column Futures layout. This file "
            f"does not belong in the Futures ingestion pipeline — verify its source "
            f"and, if it is genuinely Spot data, archive it separately (see README "
            f"'Two-Venue Research Model')."
        )
    if n_columns != 6:
        raise CorruptedArchiveError(f"{source_name}: unexpected column count {n_columns} (expected 6 for Futures)")
    return header, n_columns


def find_inner_csv_name(zf: zipfile.ZipFile) -> str:
    names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    if not names:
        raise CorruptedArchiveError("No CSV entry found inside archive")
    if len(names) > 1:
        logger.warning("Archive contains multiple CSV entries; using the first: %s", names[0])
    return names[0]


def iter_trade_chunks(
    source_path: Path,
    symbol: str,
    chunk_rows: int,
    timestamp_unit_threshold: float,
    max_rows: int | None = None,
) -> Iterator[pa.Table]:
    """Yield normalized pyarrow Tables of up to `chunk_rows` rows each,
    from either a .zip or .csv source, auto-detected by extension.

    `max_rows`, if given, stops after approximately that many rows have
    been yielded — used for sampling.
    """
    suffix = source_path.suffix.lower()
    if suffix == ".zip":
        yield from _iter_from_zip(source_path, symbol, chunk_rows, timestamp_unit_threshold, max_rows)
    elif suffix == ".csv":
        yield from _iter_from_csv(source_path, symbol, chunk_rows, timestamp_unit_threshold, max_rows)
    else:
        raise CorruptedArchiveError(f"{source_path.name}: unsupported extension {suffix!r} (expected .zip or .csv)")


def _iter_from_zip(
    zip_path: Path, symbol: str, chunk_rows: int, timestamp_unit_threshold: float, max_rows: int | None
) -> Iterator[pa.Table]:
    rows_yielded = 0
    try:
        with zipfile.ZipFile(zip_path) as zf:
            inner_name = find_inner_csv_name(zf)
            with zf.open(inner_name, "r") as peek_stream:
                lines = peek_first_lines(peek_stream, n=2)
            header, _ = detect_layout(lines, zip_path.name)

            with zf.open(inner_name, "r") as raw_stream:
                reader = pd.read_csv(
                    raw_stream, header=0 if header else None, names=FUTURES_RAW_COLUMNS,
                    dtype=_DTYPES, chunksize=chunk_rows, engine="c",
                )
                for raw_chunk in reader:
                    yield _finalize_chunk(raw_chunk, symbol, timestamp_unit_threshold)
                    rows_yielded += len(raw_chunk)
                    if max_rows is not None and rows_yielded >= max_rows:
                        return
    except zipfile.BadZipFile as e:
        raise CorruptedArchiveError(f"{zip_path.name}: bad zip / CRC failure ({e})") from e
    except pd.errors.ParserError as e:
        raise CorruptedArchiveError(f"{zip_path.name}: CSV parse error ({e})") from e
    except ValueError as e:
        raise CorruptedArchiveError(f"{zip_path.name}: unexpected schema/values ({e})") from e


def _iter_from_csv(
    csv_path: Path, symbol: str, chunk_rows: int, timestamp_unit_threshold: float, max_rows: int | None
) -> Iterator[pa.Table]:
    rows_yielded = 0
    try:
        with open(csv_path, "r", encoding="utf-8", errors="replace") as peek_stream:
            lines = peek_first_lines(peek_stream, n=2)
        header, _ = detect_layout(lines, csv_path.name)

        reader = pd.read_csv(
            csv_path, header=0 if header else None, names=FUTURES_RAW_COLUMNS,
            dtype=_DTYPES, chunksize=chunk_rows, engine="c",
        )
        for raw_chunk in reader:
            yield _finalize_chunk(raw_chunk, symbol, timestamp_unit_threshold)
            rows_yielded += len(raw_chunk)
            if max_rows is not None and rows_yielded >= max_rows:
                return
    except pd.errors.ParserError as e:
        raise CorruptedArchiveError(f"{csv_path.name}: CSV parse error ({e})") from e
    except ValueError as e:
        raise CorruptedArchiveError(f"{csv_path.name}: unexpected schema/values ({e})") from e


def _finalize_chunk(raw_chunk: pd.DataFrame, symbol: str, timestamp_unit_threshold: float) -> pa.Table:
    n = len(raw_chunk)
    ns = normalize_timestamps(raw_chunk["trade_time_raw"].to_numpy(), timestamp_unit_threshold)
    return pa.table(
        {
            "symbol": pa.array([symbol] * n, type=pa.string()),
            "trade_id": pa.array(raw_chunk["trade_id"].to_numpy(), type=pa.int64()),
            "price": pa.array(raw_chunk["price"].to_numpy(), type=pa.float64()),
            "qty": pa.array(raw_chunk["qty"].to_numpy(), type=pa.float64()),
            "quote_qty": pa.array(raw_chunk["quote_qty"].to_numpy(), type=pa.float64()),
            "trade_time_ns": pa.array(ns, type=pa.int64()),
            "is_buyer_maker": pa.array(raw_chunk["is_buyer_maker"].to_numpy(), type=pa.bool_()),
        },
        schema=TICK_SCHEMA,
    )
