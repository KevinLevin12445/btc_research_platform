# BTC Research Platform — Phase 1

An institutional-grade quantitative research platform for discovering
statistically valid trading edges in **BTCUSDT USD-M Perpetual Futures**
(Binance) — not a database, not a script collection, not a trading bot.
Phase 1 covers Ingestion, Validation, Storage, and core Feature
Engineering. See "Phasing" below for what comes next and why it isn't
built yet.

## Continuity

The previous PC was formatted; the previous implementation was lost. The
**research is not** — this is a rebuild of the software, not a new
project. Every architectural decision below that says "carried forward"
reflects a lesson learned the hard way in the prior implementation
(usually via a real crash or a real data anomaly), not a fresh guess.

## Two-Venue Research Model

- **Binance BTCUSDT USD-M Futures** — the research laboratory. Real order
  book, real trade prints with genuine aggressor-side data, real
  liquidations. This is what Phase 1 ingests and what all research happens
  against.
- **BTCUSD CFD** — the validation/execution environment. A broker-provided
  derivative, not a centralized exchange — different market structure,
  different (or absent) order-flow signals. A hypothesis validated only on
  Binance Futures stays labeled "Binance Futures validated only" until it
  is specifically re-tested against CFD data and shown to hold up.
- Transferable across venues (properties of the underlying asset's price
  path): price structure, volatility regimes, session behavior, VWAP
  relationships, statistical tendencies.
- NOT transferable without explicit validation (Binance-specific
  microstructure): order book signals, exchange-specific liquidity events,
  futures liquidation data, exchange-specific order flow signals.
- This distinction is architectural, not just documentation: any feature
  that depends on Binance-exclusive information must be clearly
  identifiable as such, so it can never accidentally end up assumed
  available in a CFD-execution context.

## Phasing

```
Phase 1: Ingestion, validation, storage, core feature generation      <- this document
Phase 2: Statistical exploration and hypothesis generation
Phase 3: Strategy implementation and backtesting
Phase 4: Robustness testing — walk-forward, Monte Carlo, regime
         stability, AND cross-venue (Binance -> CFD) validation
Phase 5: Execution-specific components
```

Phases 2–5 are deliberately not built yet. Building a hypothesis
management system, an execution engine with a dozen order types, or a
prop-firm rule simulator before a single month of verified data exists
would be exactly the kind of complexity the platform's own research
philosophy warns against — "simple systems supported by evidence are
preferable to complicated systems supported only by intuition" applies to
the platform's own architecture, not just to trading strategies.

## A confirmed incident, and why Phase 1 looks the way it does

Ten `BTCUSDT-trades-*.csv` files appeared in the data folder after the
format. Structural investigation (not assumption) resolved this: they
carry 7 columns including `isBestMatch`, and Binance's own schema
documentation confirms that field exists **only** in Spot trade exports —
Futures exports are always 6 columns. These were Spot data, not Futures,
now archived separately in `Desktop/Spot/` and permanently excluded from
this database. This is why **column count is treated as a market-identity
check, not a schema variant** throughout this codebase (see `schema.py`
and `source_input.py`) — a 7-column file is a hard stop
(`WrongMarketError`), not something to tolerate or null-fill.

## Architecture

### Storage: DuckDB + Parquet (unchanged; no superior alternative identified)
Monthly-partitioned Parquet, queried out-of-core by DuckDB. `float64` for
price/qty (sufficient precision, faster aggregates than `decimal128` —
this matters more for research than settlement-grade exactness). `symbol`
column in every tick/bar row for future multi-symbol expansion without a
rebuild.

### Dual input support: ZIP or CSV, auto-detected
`source_input.py` streams from either without manual configuration — ZIP
streams directly from the compressed entry (never writes a decompressed
CSV to disk); CSV is read directly.

### DuckDB temp-directory configuration (hard-learned, elevated to first-class)
An earlier run hit an `OutOfMemoryException` generating 1-second bars over
~1.3B rows: DuckDB's spill-to-disk temp directory and size cap were left
to auto-detection, which resolved to ~798MB on a nearly-full drive. Every
DuckDB connection now goes through `duckdb_utils.create_duckdb_connection`,
which sets `temp_directory` and `max_temp_directory_size` explicitly.

### Sequential ingestion, not multiprocess
An earlier version used `ProcessPoolExecutor` to parallelize across a
batch of pending months. The incremental one-month-at-a-time workflow
rarely has a batch to parallelize — the added complexity (Windows spawn
semantics, cross-process state access) wasn't earning its cost. Removed.

### Checkpoint-aware resumability
Each month's state DB record tracks a `checkpoint`: `source_verified` ->
`ticks_written` -> `ticks_verified` -> `features_generated` -> `reported`.
A crash after `ticks_verified` resumes at feature generation on restart,
not from scratch.

### Independent post-write verification (`verification.py`)
Never trust the writer's own row count. After a month's Parquet file is
committed, it is **re-opened cold** and its row count, schema, and
trade-ID range are re-derived independently. Only a month that passes
this is `verification_status='verified'` — and only `verified` months are
eligible for source-file deletion.

### The state DB is the sole source of truth
Since source files get deleted after verified ingestion, the inbox folder
usually holds at most one file — it cannot answer "what's been done" for
the whole history. `next_required_month`, `missing_months`, and cumulative
statistics are all derived from `month_status`, never from a folder scan.

A real bug caught and fixed during this build: the first version of
`StateStore` used a whole-record upsert, which would have silently
NULLed out every field not explicitly touched by a partial update (e.g.
setting `checkpoint="features_generated"` alone would have erased
`rows_written`, `status`, everything else). Fixed with a
`create_if_not_exists` / `update_fields` split — `update_fields` touches
only the columns given.

### Validation, never silent repair
Only genuinely invalid rows (non-positive/NaN price or qty) are dropped.
Everything else is counted and reported, never corrected: chronological
inconsistencies, trade-ID gaps (missing records), trade-ID duplicates
(split into benign identical-content duplicates vs. **conflicting**
duplicates — same ID, different data, a materially more serious signal),
and abnormal single-trade price jumps (a real flash move is data, not
noise).

### Hashing and checksum verification
Every source file gets a SHA-256 recorded in the state DB — an
independent identity check, not just a cache key. If Binance's published
`.CHECKSUM` sidecar is present alongside the source file, it's verified
too. Recommendation: download the `.CHECKSUM` file alongside each archive.

### Feature scope decision
Only the previously-validated core set is implemented: OHLCV bars (all
intervals, each with a genuine per-bar VWAP), delta/cumulative delta
(1min/5min/15min/1h), footprint (built once at 1min/$10 bins — the one
feature expensive enough to justify eager generation, since it needs a
full tick-corpus scan; coarser footprint later is a cheap re-aggregation
of this table), volume profile (POC/VAH/VAL/HVN/LVN/developing/naked),
session VWAP, ATR, realized volatility. The full Part III feature
taxonomy (Parkinson/Garman-Klass volatility, distribution skew/kurtosis,
a dozen Delta variants, etc.) is an intentional backlog — pulled in per
specific hypothesis, not implemented speculatively.

## Project layout

```
btc_research_platform/
  config/config.yaml
  main.py
  src/
    config.py, logging_setup.py, errors.py, versions.py
    schema.py            futures-only tick schema; column count = market-identity check
    hashing.py            SHA-256 + Binance .CHECKSUM sidecar verification
    source_input.py       unified zip/csv reader, header/column-count/market detection
    discovery.py           inbox scanning + verification; state-DB-derived next-month/gaps
    state.py               SQLite: month_status (checkpoints, versions, hashes) + source_verification cache
    validation.py          streaming per-chunk validation (gaps, duplicates, jumps)
    parquet_store.py        atomic, disk-space-guarded, row-group-tuned writer
    verification.py         independent post-write re-verification
    duckdb_utils.py          shared DuckDB connection (temp dir fix) + path-list helper
    pipeline.py              sequential, checkpoint-resumable orchestration
    reporting.py             per-month Data Quality Report
    bars.py
    features/
      orderflow.py, footprint.py, vwap.py, volume_profile.py, volatility.py
  state/pipeline_state.db
  logs/, reports/
  warehouse/ticks|bars|features/BTCUSDT/...
```

## Running it — the incremental workflow

```powershell
cd C:\Users\Usuario\Desktop\btc_research_platform
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# 1. Download ONE month's Futures archive from
#    https://data.binance.vision/data/futures/um/monthly/trades/BTCUSDT/
#    (zip or csv, either works) — and its .CHECKSUM sidecar if you want
#    the extra verification layer — into C:\Users\Usuario\Desktop\Data

# 2. Preview/verify without ingesting (optional but recommended first time):
python main.py --stage discover

# 3. Ingest:
python main.py --stage ingest

# 4. Review the auto-generated report in reports/month_report_<YYYY-MM>_*.md
#    — it tells you explicitly whether the source is safe to delete.

# 5. If safe:
python main.py --delete-source 2023-01

# 6. Repeat from step 1 with the next month.

# Check status / cumulative stats / next required month any time:
python main.py --status
```

## Prerequisites not yet resolved

- **Python is not installed** on this machine (checked directly — no
  install under the usual path). Install Python 3.11+ before running
  anything above.
- **Free disk space** unconfirmed since the Spot CSVs were archived —
  run `Get-PSDrive -PSProvider FileSystem` to check. The incremental
  one-month workflow needs far less headroom than the earlier full-history
  batch approach (roughly 1-2GB permanent Parquet per month, plus the
  `duckdb_max_temp_gb` spill allowance), but it's still worth confirming
  before the first real ingest.
