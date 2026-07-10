"""
Typed configuration loading. Dependency-light: dataclasses + PyYAML.
Fails fast on a bad config file rather than surfacing as a confusing
KeyError deep inside a run.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import List

import yaml


@dataclasses.dataclass(frozen=True)
class ProjectConfig:
    symbol: str
    market: str
    start_month: str  # e.g. "2023-01" — the configured beginning of the incremental sequence


@dataclasses.dataclass(frozen=True)
class PathsConfig:
    raw_data_dir: Path      # the "inbox" — where you drop the next month's zip/csv
    warehouse_dir: Path
    state_db: Path
    log_dir: Path
    reports_dir: Path

    @property
    def ticks_dir(self) -> Path:
        return self.warehouse_dir / "ticks"

    @property
    def bars_dir(self) -> Path:
        return self.warehouse_dir / "bars"

    @property
    def features_dir(self) -> Path:
        return self.warehouse_dir / "features"


@dataclasses.dataclass(frozen=True)
class ProcessingConfig:
    chunk_rows: int
    parquet_row_group_size: int
    min_free_disk_gb: float
    timestamp_unit_threshold: float
    abnormal_price_jump_pct: float
    duckdb_memory_limit_gb: int
    duckdb_threads: int
    duckdb_max_temp_gb: int


@dataclasses.dataclass(frozen=True)
class BarsConfig:
    fixed_intervals: List[str]
    calendar_intervals: List[str]


@dataclasses.dataclass(frozen=True)
class FeaturesConfig:
    atr_period: int
    realized_vol_window: int
    large_trade_quantile: float
    value_area_pct: float
    profile_bin_size: float
    stacked_imbalance_ratio: float
    stacked_imbalance_min_bars: int
    orderflow_intervals: List[str]
    footprint_bar_interval: str
    footprint_bin_size: float


@dataclasses.dataclass(frozen=True)
class PlatformConfig:
    project: ProjectConfig
    paths: PathsConfig
    processing: ProcessingConfig
    bars: BarsConfig
    features: FeaturesConfig


def _require(d: dict, key: str, section: str):
    if key not in d:
        raise ValueError(f"config.yaml: missing required key '{key}' in section '{section}'")
    return d[key]


def load_config(config_path: Path | str) -> PlatformConfig:
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    proj = raw.get("project", {})
    project = ProjectConfig(
        symbol=_require(proj, "symbol", "project"),
        market=_require(proj, "market", "project"),
        start_month=_require(proj, "start_month", "project"),
    )

    p = raw.get("paths", {})
    paths = PathsConfig(
        raw_data_dir=Path(_require(p, "raw_data_dir", "paths")),
        warehouse_dir=Path(_require(p, "warehouse_dir", "paths")),
        state_db=Path(_require(p, "state_db", "paths")),
        log_dir=Path(_require(p, "log_dir", "paths")),
        reports_dir=Path(_require(p, "reports_dir", "paths")),
    )

    proc = raw.get("processing", {})
    processing = ProcessingConfig(
        chunk_rows=int(_require(proc, "chunk_rows", "processing")),
        parquet_row_group_size=int(_require(proc, "parquet_row_group_size", "processing")),
        min_free_disk_gb=float(_require(proc, "min_free_disk_gb", "processing")),
        timestamp_unit_threshold=float(_require(proc, "timestamp_unit_threshold", "processing")),
        abnormal_price_jump_pct=float(_require(proc, "abnormal_price_jump_pct", "processing")),
        duckdb_memory_limit_gb=int(_require(proc, "duckdb_memory_limit_gb", "processing")),
        duckdb_threads=int(_require(proc, "duckdb_threads", "processing")),
        duckdb_max_temp_gb=int(_require(proc, "duckdb_max_temp_gb", "processing")),
    )

    b = raw.get("bars", {})
    bars = BarsConfig(
        fixed_intervals=list(_require(b, "fixed_intervals", "bars")),
        calendar_intervals=list(_require(b, "calendar_intervals", "bars")),
    )

    feat = raw.get("features", {})
    features = FeaturesConfig(
        atr_period=int(_require(feat, "atr_period", "features")),
        realized_vol_window=int(_require(feat, "realized_vol_window", "features")),
        large_trade_quantile=float(_require(feat, "large_trade_quantile", "features")),
        value_area_pct=float(_require(feat, "value_area_pct", "features")),
        profile_bin_size=float(_require(feat, "profile_bin_size", "features")),
        stacked_imbalance_ratio=float(_require(feat, "stacked_imbalance_ratio", "features")),
        stacked_imbalance_min_bars=int(_require(feat, "stacked_imbalance_min_bars", "features")),
        orderflow_intervals=list(_require(feat, "orderflow_intervals", "features")),
        footprint_bar_interval=str(_require(feat, "footprint_bar_interval", "features")),
        footprint_bin_size=float(_require(feat, "footprint_bin_size", "features")),
    )

    cfg = PlatformConfig(project=project, paths=paths, processing=processing, bars=bars, features=features)
    _ensure_directories(cfg)
    return cfg


def _ensure_directories(cfg: PlatformConfig) -> None:
    for d in (
        cfg.paths.raw_data_dir, cfg.paths.warehouse_dir, cfg.paths.ticks_dir, cfg.paths.bars_dir,
        cfg.paths.features_dir, cfg.paths.log_dir, cfg.paths.reports_dir, cfg.paths.state_db.parent,
    ):
        d.mkdir(parents=True, exist_ok=True)
