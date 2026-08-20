"""Bounded MBP-10 preflight for flow acceptance and exact-price refill.

This is deliberately not a campaign runner.  It scans only the already-visible
Discovery partition, constructs a small causal decision table in memory,
performs expanding chronological checks, and prints one JSON document.  The
optional ``--build-missing-execution-cache`` switch publishes only the frozen
daily BBO caches needed to make the execution comparison complete.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import stat as stat_module
import sys
from collections import defaultdict, deque
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from scipy.stats import spearmanr
from sklearn.ensemble import HistGradientBoostingRegressor

TICK_SIZE_RAW = 50_000
UNDEFINED_PRICE = 9_223_372_036_854_775_807
F_LAST = 128
BAD_FLAG_MASK = 4 | 8 | 32  # MAYBE_BAD_BOOK | BAD_TS_RECV | SNAPSHOT
SECOND_NS = 1_000_000_000
FIVE_MINUTES_S = 300
HORIZONS_MINUTES = (30, 60, 120, 360)
REFILL_HORIZONS_SECONDS = (5, 30, 120)
MAX_CONSECUTIVE_STALE_5M_BUCKETS = 2
ROUTING_DELAY_NS = SECOND_NS
MAX_ATTEMPT_WAIT_NS = SECOND_NS
LATEST_FOLLOWUP_DECISION_SECOND_UTC = 15 * 3_600 + 55 * 60
MEMORY_LOOKBACK_S = 6 * 60 * 60
MEMORY_RADIUS_TICKS = 8
EXECUTION_CACHE_SCHEMA = "systematic_fx.phase1a_daily_executable_cache.v1"
EXECUTION_CACHE_VERSION = "phase1a_daily_executable_cache_v1"
EXECUTION_CACHE_INDEX_SCHEMA = "systematic_fx.phase1a_daily_executable_cache_index.v1"
DISCOVERY_DATES_SHA256 = "4768fe608449a2e62c1a98ae0acfe71f8dac3e9fc4664d7e8857f99977e4bab0"
EXECUTION_REQUESTS_SHA256 = "09b8747646f6bfeb121e9de2a6a25a97b71d41740b1cc9e195ab6478e687785e"
PATH_CONTAMINATION_FIELDS = (
    "locked_seconds",
    "crossed_seconds",
    "maybe_bad_book_seconds",
    "bad_ts_recv_seconds",
    "reset_seen_seconds",
    "recovery_marker_seconds",
    "recovery_required_seconds",
    "off_tick_grid_seconds",
)

BENCHMARK_DATES = (
    "2022-01-12",
    "2022-02-09",
    "2022-03-23",
    "2022-05-11",
    "2022-06-22",
    "2022-08-10",
    "2022-09-14",
    "2022-11-15",
    "2022-12-14",
    "2023-02-15",
    "2023-03-15",
    "2023-05-17",
    "2023-06-14",
    "2023-07-19",
)

RAW_COLUMNS = [
    "ts_recv",
    "instrument_id",
    "action",
    "side",
    "price",
    "size",
    "flags",
]
for _level in range(10):
    _suffix = f"{_level:02d}"
    RAW_COLUMNS.extend(
        [
            f"bid_px_{_suffix}",
            f"ask_px_{_suffix}",
            f"bid_sz_{_suffix}",
            f"ask_sz_{_suffix}",
        ]
    )

PRICE_FEATURES = (
    "price_ret_5m",
    "price_ret_30m",
    "price_ret_60m",
    "price_vol_30m",
    "price_range_30m",
    "price_close_location_30m",
    "clock_sin",
    "clock_cos",
)

LIQUIDITY_FEATURES = (
    "flow_signed",
    "flow_abs",
    "flow_buy",
    "flow_sell",
    "trade_event_count",
    "impact_aligned_ticks",
    "impact_buy_ticks",
    "impact_sell_ticks",
    "zero_impact_flow_share",
    "trade_center_offset_ticks",
    "trade_center_migration_ticks",
    "book_center_offset_mean",
    "book_center_offset_last",
    "book_center_offset_change",
    "book_depth_total_mean",
    "book_depth_imbalance_mean",
    "book_depth_imbalance_last",
    "spread_ticks_mean",
    "refill_5s_buy",
    "refill_5s_sell",
    "breach_5s_buy",
    "breach_5s_sell",
    "refill_30s_buy",
    "refill_30s_sell",
    "breach_30s_buy",
    "breach_30s_sell",
    "refill_120s_buy",
    "refill_120s_sell",
    "breach_120s_buy",
    "breach_120s_sell",
    "depletion_5s_buy",
    "depletion_5s_sell",
    "depletion_30s_buy",
    "depletion_30s_sell",
    "depletion_120s_buy",
    "depletion_120s_sell",
    "response_flow_5s_buy",
    "response_flow_5s_sell",
    "response_flow_30s_buy",
    "response_flow_30s_sell",
    "response_flow_120s_buy",
    "response_flow_120s_sell",
    "response_impact_5s_buy",
    "response_impact_5s_sell",
    "response_impact_30s_buy",
    "response_impact_30s_sell",
    "response_impact_120s_buy",
    "response_impact_120s_sell",
    "response_immediate_impact_5s_buy",
    "response_immediate_impact_5s_sell",
    "response_immediate_impact_30s_buy",
    "response_immediate_impact_30s_sell",
    "response_immediate_impact_120s_buy",
    "response_immediate_impact_120s_sell",
    "acceptance_path_5s_buy",
    "acceptance_path_5s_sell",
    "acceptance_path_30s_buy",
    "acceptance_path_30s_sell",
    "acceptance_path_120s_buy",
    "acceptance_path_120s_sell",
)

MEMORY_FEATURES = (
    "memory_support_recovered",
    "memory_resistance_recovered",
    "memory_support_unrecovered",
    "memory_resistance_unrecovered",
    "memory_support_refill_ratio",
    "memory_resistance_refill_ratio",
    "memory_nearest_support_distance",
    "memory_nearest_resistance_distance",
    "memory_support_age_minutes",
    "memory_resistance_age_minutes",
    "memory_support_recovered_at_bbo",
    "memory_resistance_recovered_at_bbo",
    "memory_support_grid_recontact_recovered",
    "memory_resistance_grid_recontact_recovered",
    "memory_support_grid_recontact_unrecovered",
    "memory_resistance_grid_recontact_unrecovered",
    "memory_grid_recontact_net_log_strength",
    "memory_grid_recontact_side_count",
    "memory_net_log_strength",
    "memory_level_count",
)

FOLLOWUP_PLAN = {
    "candidate": "LIQUIDITY_ONLY_360M_WITH_GRID_RECONTACT_MEMORY",
    "cost_ticks": {"standard": 10, "stress": 14},
    "date_blocks": 5,
    "entry_exit": {
        "attempt": "FIRST_PHYSICAL_EVENT_AT_OR_AFTER_ELIGIBILITY",
        "attempt_wait_ns": MAX_ATTEMPT_WAIT_NS,
        "pricing": "MARKETABLE_BBO_WITHOUT_FAVORABLE_MOVE_FILTER",
        "routing_delay_ns": ROUTING_DELAY_NS,
        "same_physical_event_reuse": False,
    },
    "latest_decision_second_utc": LATEST_FOLLOWUP_DECISION_SECOND_UTC,
    "memory": {
        "publication_grid_seconds": FIVE_MINUTES_S,
        "radius_ticks": MEMORY_RADIUS_TICKS,
        "refill_horizon_seconds": 30,
        "ttl_seconds": MEMORY_LOOKBACK_S,
    },
    "midpoint48_score_exclusion_sha256": (
        "adcf9ada5e0dcbb3e7fb300ffef31ff164f5fdaf8274c974fe49089e414e0512"
    ),
    "model": {
        "baseline_features": list(LIQUIDITY_FEATURES),
        "memory_features": list(MEMORY_FEATURES),
        "signal_quantiles": [0.20, 0.80],
    },
    "oos_test_blocks": [1, 2, 3, 4],
    "oos_universe": {
        "economic_signals": "CAUSAL_FEATURE_ELIGIBLE_ROWS_WITHOUT_FUTURE_LABEL_FILTER",
        "ic": "PAIRED_LABEL_COMPLETE_SUBSET_ONLY",
    },
    "bootstrap": {
        "iterations": 10_000,
        "quantile_method": "linear",
        "resampling_unit": "SOURCE_DATE_STRATIFIED_BY_BLOCK",
    },
    "go_thresholds": {
        "active_scoring_dates_min": 96,
        "b4_net10_ev_positive": True,
        "b4_net14_positive": True,
        "baseline_and_memory_cache_unavailable_max": 0,
        "baseline_and_memory_exit_censored_max": 0,
        "block_active_dates_min": 24,
        "block_fills_min": 48,
        "bootstrap_daily_increment_lcb_95_positive": True,
        "bootstrap_net10_ev_lcb_95_positive": True,
        "fills_min": 192,
        "memory_b4_ic_delta_positive": True,
        "memory_mean_fold_ic_delta_positive": True,
        "memory_mean_fold_ic_positive": True,
        "net10_ev_positive": True,
        "net14_total_positive": True,
        "positive_blocks_min": 3,
        "positive_ic_delta_folds_min": 3,
        "profit_factor_net10_min": 1.05,
        "worst_block_net14_ev_min": -2.0,
    },
}
FOLLOWUP_PLAN_SHA256 = hashlib.sha256(
    json.dumps(
        FOLLOWUP_PLAN,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
).hexdigest()


@dataclass(frozen=True)
class Selection:
    source_date: str
    raw_symbol: str
    instrument_id: int
    source_relative_uri: str
    source_sha256: str


@dataclass(frozen=True)
class ExecutionCacheRequest:
    source_date: str
    raw_symbol: str
    source_relative_uri: str
    source_sha256: str
    source_row_count: int
    event_index_offset: int
    request_index_path: Path


@dataclass(frozen=True)
class ExecutionCache:
    path: Path
    sha256: str
    byte_size: int
    cached_quote_count: int
    event_index_offset: int


@dataclass(frozen=True)
class RefillEpisode:
    event_end_ns: int
    direction: int
    price_raw: int
    post_size: int
    depletion: int
    flow_volume: int
    event_mid_x2_raw: int
    immediate_aligned_impact_ticks: float


def _ceil_div(value: np.ndarray | int, width: int) -> np.ndarray | int:
    return (value + width - 1) // width


def _log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def _finite(value: float) -> float | None:
    return float(value) if math.isfinite(float(value)) else None


def _bounded_fresh_path(values: np.ndarray) -> bool:
    fresh = np.asarray(values, dtype=bool)
    if len(fresh) == 0 or not bool(fresh[0]) or not bool(fresh[-1]):
        return False
    longest = 0
    current = 0
    for stale in ~fresh:
        current = current + 1 if bool(stale) else 0
        longest = max(longest, current)
    return longest <= MAX_CONSECUTIVE_STALE_5M_BUCKETS


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_discovery_selections(root: Path) -> tuple[list[str], dict[str, Selection]]:
    split_path = root / "data/derived/manifests/phase1a_performance_free_split_v1.json"
    with split_path.open("r", encoding="utf-8") as handle:
        split = json.load(handle)
    discovery = list(split["partitions"]["discovery"]["source_dates"])
    discovery_sha256 = hashlib.sha256(
        json.dumps(
            discovery,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    ).hexdigest()
    if discovery_sha256 != DISCOVERY_DATES_SHA256:
        raise RuntimeError("Discovery date allowlist identity drifted")
    records: dict[str, Selection] = {}
    feature_root = root / ("data/derived/features_1s/version=phase1a_mbp10_screening_v1")
    for source_date in discovery:
        matches = list(feature_root.glob(f"contract=*/source_date={source_date}/part-000.parquet"))
        if len(matches) > 1:
            raise RuntimeError(f"multiple derived 1s selections on {source_date}")
        if not matches:
            continue
        path = matches[0]
        raw_symbol = path.parent.parent.name.split("=", 1)[1]
        feature_parquet = pq.ParquetFile(path)
        feature_metadata = feature_parquet.schema_arrow.metadata or {}
        expected_metadata = {
            b"systematic_fx.source_date": source_date,
            b"systematic_fx.contract": raw_symbol,
        }
        if any(
            feature_metadata.get(key, b"").decode("ascii") != value
            for key, value in expected_metadata.items()
        ):
            raise RuntimeError(f"derived 1s lineage drifted on {source_date}")
        raw_source_sha = feature_metadata.get(b"systematic_fx.source_sha256")
        raw_instrument_id = feature_metadata.get(b"systematic_fx.instrument_id")
        if raw_source_sha is None or raw_instrument_id is None:
            raise RuntimeError(f"derived 1s identity metadata missing on {source_date}")
        source_sha256 = raw_source_sha.decode("ascii")
        instrument_table = feature_parquet.read(columns=["instrument_id"], use_threads=True)
        if instrument_table.num_rows == 0:
            continue
        ids = np.unique(_numpy_column(instrument_table, "instrument_id"))
        if len(ids) != 1:
            raise RuntimeError(f"derived 1s selection is not unique for {source_date}")
        if int(raw_instrument_id.decode("ascii")) != int(ids[0]):
            raise RuntimeError(f"derived 1s instrument metadata drifted on {source_date}")
        year, month, day = source_date.split("-")
        candidate = Selection(
            source_date=source_date,
            raw_symbol=raw_symbol,
            instrument_id=int(ids[0]),
            source_relative_uri=(
                f"mbp-10/{year}/{month}/{day}/glbx-mdp3-{year}{month}{day}.mbp-10.parquet"
            ),
            source_sha256=source_sha256,
        )
        prior = records.get(source_date)
        if prior is not None and prior != candidate:
            raise RuntimeError(f"cache and feature selections conflict for {source_date}")
        records[source_date] = candidate
    available = [source_date for source_date in discovery if source_date in records]
    if len(available) < 100:
        raise RuntimeError("too few visible Discovery selections")
    return discovery, records


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _execution_cache_requests(
    root: Path,
    discovery: list[str],
    selections: dict[str, Selection],
) -> dict[str, ExecutionCacheRequest]:
    requests: dict[str, ExecutionCacheRequest] = {}
    request_documents: list[dict[str, object]] = []
    event_index_offset = 0
    index_root = root / (
        f"data/derived/backtest_event_cache/{EXECUTION_CACHE_VERSION}/request_index"
    )
    for source_date in discovery:
        year, month, day = source_date.split("-")
        raw_path = root / (
            f"data/mbp-10/{year}/{month}/{day}/glbx-mdp3-{year}{month}{day}.mbp-10.parquet"
        )
        source_rows = pq.ParquetFile(raw_path).metadata.num_rows
        selection = selections.get(source_date)
        if selection is not None:
            five_path = root / (
                "data/derived/research_5m/version=phase1a_mbp10_screening_v1/"
                f"contract={selection.raw_symbol}/source_date={source_date}/part-000.parquet"
            )
            metadata = pq.ParquetFile(five_path).schema_arrow.metadata or {}
            five_bindings = {
                b"systematic_fx.source_date": source_date,
                b"systematic_fx.contract": selection.raw_symbol,
                b"systematic_fx.instrument_id": str(selection.instrument_id),
                b"systematic_fx.source_sha256": selection.source_sha256,
            }
            if any(
                metadata.get(key, b"").decode("ascii") != value
                for key, value in five_bindings.items()
            ):
                raise RuntimeError(f"1s/5m lineage drifted on {source_date}")
            source_sha256 = selection.source_sha256
            request_document = {
                "cache_schema": EXECUTION_CACHE_SCHEMA,
                "cache_version": EXECUTION_CACHE_VERSION,
                "event_index_offset": event_index_offset,
                "raw_symbol": selection.raw_symbol,
                "source_date": source_date,
                "source_relative_uri": selection.source_relative_uri,
                "source_sha256": source_sha256,
            }
            request_documents.append(request_document)
            request_sha256 = hashlib.sha256(_canonical_json_bytes(request_document)).hexdigest()
            requests[source_date] = ExecutionCacheRequest(
                source_date=source_date,
                raw_symbol=selection.raw_symbol,
                source_relative_uri=selection.source_relative_uri,
                source_sha256=source_sha256,
                source_row_count=source_rows,
                event_index_offset=event_index_offset,
                request_index_path=(index_root / f"request_sha256={request_sha256}.json"),
            )
        event_index_offset += source_rows
    if set(requests) != set(selections):
        raise RuntimeError("execution-cache requests do not cover frozen selections")
    requests_sha256 = hashlib.sha256(
        json.dumps(
            request_documents,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    ).hexdigest()
    if requests_sha256 != EXECUTION_REQUESTS_SHA256:
        raise RuntimeError("execution-cache request vector identity drifted")
    return requests


def _load_execution_caches(
    root: Path,
    requests: dict[str, ExecutionCacheRequest],
    selections: dict[str, Selection],
    *,
    verify_content: bool = False,
) -> tuple[dict[str, ExecutionCache], list[str]]:
    caches: dict[str, ExecutionCache] = {}
    missing: list[str] = []
    for source_date, request in requests.items():
        index_path = request.request_index_path
        if not index_path.is_file():
            missing.append(source_date)
            continue
        index_stat = index_path.stat(follow_symlinks=False)
        if (
            index_path.is_symlink()
            or not stat_module.S_ISREG(index_stat.st_mode)
            or stat_module.S_IMODE(index_stat.st_mode) != 0o444
            or index_stat.st_nlink != 1
        ):
            raise RuntimeError(f"execution-cache index is not immutable on {source_date}")
        raw = index_path.read_bytes()
        try:
            document = json.loads(raw)
        except json.JSONDecodeError as error:
            raise RuntimeError(f"invalid execution-cache index on {source_date}") from error
        if raw != _canonical_json_bytes(document):
            raise RuntimeError(f"noncanonical execution-cache index on {source_date}")
        if set(document) != {"artifact_schema", "request", "report"}:
            raise RuntimeError(f"execution-cache index keys drifted on {source_date}")
        if document.get("artifact_schema") != EXECUTION_CACHE_INDEX_SCHEMA:
            raise RuntimeError(f"execution-cache index schema drift on {source_date}")
        expected_request = {
            "cache_schema": EXECUTION_CACHE_SCHEMA,
            "cache_version": EXECUTION_CACHE_VERSION,
            "event_index_offset": request.event_index_offset,
            "raw_symbol": request.raw_symbol,
            "source_date": request.source_date,
            "source_relative_uri": request.source_relative_uri,
            "source_sha256": request.source_sha256,
        }
        if document.get("request") != expected_request:
            raise RuntimeError(f"execution-cache request drift on {source_date}")
        report = document.get("report")
        if not isinstance(report, dict):
            raise TypeError(f"execution-cache report missing on {source_date}")
        expected_report_keys = {
            "byte_size",
            "cache_relative_path",
            "cached_quote_count",
            "event_index_offset",
            "first_event_index",
            "first_ts_recv_ns",
            "instrument_id",
            "last_event_index",
            "last_ts_recv_ns",
            "last_valid_event_index",
            "last_valid_ts_recv_ns",
            "raw_symbol",
            "sha256",
            "source_date",
            "source_relative_uri",
            "source_row_count",
            "source_sha256",
            "valid_quote_count",
        }
        if set(report) != expected_report_keys:
            raise RuntimeError(f"execution-cache report keys drifted on {source_date}")
        selection = selections[source_date]
        bindings = {
            "source_date": source_date,
            "raw_symbol": selection.raw_symbol,
            "instrument_id": selection.instrument_id,
            "source_relative_uri": selection.source_relative_uri,
            "source_sha256": request.source_sha256,
            "source_row_count": request.source_row_count,
            "event_index_offset": request.event_index_offset,
        }
        if any(report.get(key) != value for key, value in bindings.items()):
            raise RuntimeError(f"execution-cache report lineage drift on {source_date}")
        digest = report.get("sha256")
        byte_size = report.get("byte_size")
        cached_count = report.get("cached_quote_count")
        relative = report.get("cache_relative_path")
        if not (
            isinstance(digest, str)
            and len(digest) == 64
            and all(character in "0123456789abcdef" for character in digest)
            and isinstance(byte_size, int)
            and byte_size > 0
            and isinstance(cached_count, int)
            and cached_count > 0
            and isinstance(relative, str)
        ):
            raise RuntimeError(f"execution-cache identity invalid on {source_date}")
        relative_path = Path(relative)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise RuntimeError(f"execution-cache path escapes data root on {source_date}")
        expected_parent = (
            "derived",
            "backtest_event_cache",
            EXECUTION_CACHE_VERSION,
        )
        if relative_path.parts[:-1] != expected_parent:
            raise RuntimeError(f"execution-cache directory drift on {source_date}")
        cache_path = root / "data" / relative_path
        if cache_path.name != f"sha256={digest}.parquet":
            raise RuntimeError(f"execution-cache filename drift on {source_date}")
        cache_stat = cache_path.stat(follow_symlinks=False)
        if (
            cache_path.is_symlink()
            or not stat_module.S_ISREG(cache_stat.st_mode)
            or cache_stat.st_size != byte_size
            or stat_module.S_IMODE(cache_stat.st_mode) != 0o444
            or cache_stat.st_nlink != 1
        ):
            raise RuntimeError(f"execution-cache file identity drift on {source_date}")
        if verify_content and _file_sha256(cache_path) != digest:
            raise RuntimeError(f"execution-cache content SHA drift on {source_date}")
        parquet = pq.ParquetFile(cache_path)
        if parquet.metadata.num_rows != cached_count:
            raise RuntimeError(f"execution-cache row count drift on {source_date}")
        expected_columns = (
            "event_index",
            "ts_recv_ns",
            "best_bid_ticks",
            "best_ask_ticks",
            "valid",
            "sequence",
            "source_row_index",
            "row_group_index",
            "row_index",
            "invalid_reason",
        )
        if tuple(parquet.schema_arrow.names) != expected_columns:
            raise RuntimeError(f"execution-cache schema incomplete on {source_date}")
        expected_schema = pa.schema(
            [
                pa.field("event_index", pa.int64(), nullable=False),
                pa.field("ts_recv_ns", pa.int64(), nullable=False),
                pa.field("best_bid_ticks", pa.int64(), nullable=True),
                pa.field("best_ask_ticks", pa.int64(), nullable=True),
                pa.field("valid", pa.bool_(), nullable=False),
                pa.field("sequence", pa.uint32(), nullable=False),
                pa.field("source_row_index", pa.int64(), nullable=False),
                pa.field("row_group_index", pa.int32(), nullable=False),
                pa.field("row_index", pa.int32(), nullable=False),
                pa.field("invalid_reason", pa.string(), nullable=True),
            ]
        )
        if not parquet.schema_arrow.remove_metadata().equals(expected_schema):
            raise RuntimeError(f"execution-cache field types drifted on {source_date}")
        raw_metadata = (parquet.schema_arrow.metadata or {}).get(b"systematic_fx.cache")
        try:
            cache_metadata = None if raw_metadata is None else json.loads(raw_metadata)
        except (TypeError, json.JSONDecodeError) as error:
            raise RuntimeError(f"execution-cache metadata invalid on {source_date}") from error
        expected_metadata = {
            "cache_schema": EXECUTION_CACHE_SCHEMA,
            "cache_version": EXECUTION_CACHE_VERSION,
            "event_index_offset": request.event_index_offset,
            "instrument_id": selection.instrument_id,
            "raw_symbol": selection.raw_symbol,
            "source_date": source_date,
            "source_relative_uri": selection.source_relative_uri,
            "source_row_count": request.source_row_count,
            "source_sha256": request.source_sha256,
        }
        if cache_metadata != expected_metadata:
            raise RuntimeError(f"execution-cache metadata drift on {source_date}")
        numeric_bounds = (
            report["first_event_index"],
            report["last_event_index"],
            report["first_ts_recv_ns"],
            report["last_ts_recv_ns"],
            report["valid_quote_count"],
        )
        if not all(isinstance(value, int) for value in numeric_bounds):
            raise RuntimeError(f"execution-cache report bounds invalid on {source_date}")
        if (
            report["first_event_index"] < request.event_index_offset
            or report["last_event_index"] < report["first_event_index"]
            or report["last_ts_recv_ns"] < report["first_ts_recv_ns"]
            or not 0 <= report["valid_quote_count"] <= cached_count
        ):
            raise RuntimeError(f"execution-cache report bounds drifted on {source_date}")
        caches[source_date] = ExecutionCache(
            path=cache_path,
            sha256=digest,
            byte_size=byte_size,
            cached_quote_count=cached_count,
            event_index_offset=request.event_index_offset,
        )
    return caches, missing


def _build_missing_execution_caches(
    root: Path,
    requests: dict[str, ExecutionCacheRequest],
    missing: list[str],
) -> None:
    if not missing:
        return
    if missing != sorted(set(missing)) or not set(missing).issubset(requests):
        raise RuntimeError("missing execution-cache request set is not canonical")
    from systematic_fx.backtest.event_cache import (
        DailyCacheSpec,
        build_daily_cache_batch,
    )

    specs = tuple(
        DailyCacheSpec(
            source_date=date.fromisoformat(source_date),
            source_parquet_path=root / "data" / requests[source_date].source_relative_uri,
            source_sha256=requests[source_date].source_sha256,
            raw_symbol=requests[source_date].raw_symbol,
            event_index_offset=requests[source_date].event_index_offset,
        )
        for source_date in missing
    )

    def progress(report: Any, completed: int, total: int) -> None:
        source_date = report.source_date
        _log(f"[execution-cache {completed}/{total}] {source_date}")

    build_daily_cache_batch(
        specs,
        data_root=root / "data",
        max_workers=2,
        progress_callback=progress,
    )


def _midpoint_sample(source_dates: list[str], per_block: int) -> tuple[list[str], dict[str, int]]:
    blocks = [list(block) for block in np.array_split(np.asarray(source_dates), 4)]
    chosen: list[str] = []
    block_by_date: dict[str, int] = {}
    for block_index, block in enumerate(blocks):
        if len(block) < per_block:
            raise RuntimeError("sample block is too short")
        indices = [
            min(len(block) - 1, int((j + 0.5) * len(block) / per_block)) for j in range(per_block)
        ]
        if len(set(indices)) != len(indices):
            raise RuntimeError("midpoint sampling produced duplicate dates")
        for index in indices:
            source_date = str(block[index])
            chosen.append(source_date)
            block_by_date[source_date] = block_index
    return chosen, block_by_date


def _read_selected_table(path: Path, instrument_id: int) -> tuple[pa.Table, int]:
    parquet = pq.ParquetFile(path)
    pieces: list[pa.Table] = []
    source_rows = 0
    for row_group in range(parquet.metadata.num_row_groups):
        table = parquet.read_row_group(row_group, columns=RAW_COLUMNS, use_threads=True)
        source_rows += table.num_rows
        indices = pc.indices_nonzero(
            pc.equal(
                table["instrument_id"],
                pa.scalar(instrument_id, type=pa.uint32()),
            )
        )
        if len(indices):
            pieces.append(table.take(indices))
    if not pieces:
        raise RuntimeError(f"selected instrument {instrument_id} has no rows in {path}")
    return pa.concat_tables(pieces), source_rows


def _numpy_column(table: pa.Table, name: str) -> np.ndarray:
    return table[name].combine_chunks().to_numpy(zero_copy_only=False)


def _book_matrices(table: pa.Table) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    bid_px = np.column_stack([_numpy_column(table, f"bid_px_{level:02d}") for level in range(10)])
    ask_px = np.column_stack([_numpy_column(table, f"ask_px_{level:02d}") for level in range(10)])
    bid_sz = np.column_stack([_numpy_column(table, f"bid_sz_{level:02d}") for level in range(10)])
    ask_sz = np.column_stack([_numpy_column(table, f"ask_sz_{level:02d}") for level in range(10)])
    return bid_px, ask_px, bid_sz, ask_sz


def _exact_size(prices: np.ndarray, sizes: np.ndarray, price: int) -> int | None:
    matches = np.flatnonzero(prices == price)
    if len(matches) == 1:
        return int(sizes[int(matches[0])])
    if len(matches) > 1:
        raise RuntimeError("duplicate exact price in one side of L10 book")
    return None


def _valid_bbo(bid_px: np.ndarray, ask_px: np.ndarray) -> np.ndarray:
    bid = bid_px[:, 0]
    ask = ask_px[:, 0]
    return (bid != UNDEFINED_PRICE) & (ask != UNDEFINED_PRICE) & (bid < ask)


def _completed_event_state(
    ts_ns: np.ndarray,
    action: np.ndarray,
    flags: np.ndarray,
    bid_px: np.ndarray,
    ask_px: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    event_ends = np.flatnonzero((flags & F_LAST) != 0)
    if len(event_ends) == 0 or int(event_ends[-1]) != len(flags) - 1:
        raise RuntimeError("selected physical stream does not end on F_LAST")
    event_starts = np.r_[0, event_ends[:-1] + 1]
    invalid_rows = ((flags & BAD_FLAG_MASK) != 0) | (action == "R")
    invalid_counts = np.add.reduceat(invalid_rows.astype(np.int64), event_starts)
    valid = (invalid_counts == 0) & _valid_bbo(bid_px[event_ends], ask_px[event_ends])
    event_end_ns = ts_ns[event_ends].astype(np.int64, copy=False)
    if np.any(np.diff(event_end_ns) < 0):
        raise RuntimeError("selected completed events are not ordered by ts_recv")
    return event_ends, event_end_ns, valid


def _aggregate_trade_events(
    ts_ns: np.ndarray,
    action: np.ndarray,
    side: np.ndarray,
    price: np.ndarray,
    size: np.ndarray,
    flags: np.ndarray,
    bid_px: np.ndarray,
    ask_px: np.ndarray,
    bid_sz: np.ndarray,
    ask_sz: np.ndarray,
) -> tuple[pd.DataFrame, list[RefillEpisode], np.ndarray, dict[str, int]]:
    event_ends = np.flatnonzero((flags & F_LAST) != 0)
    if len(event_ends) == 0 or int(event_ends[-1]) != len(flags) - 1:
        raise RuntimeError("selected physical stream does not end on F_LAST")
    trade_indices = np.flatnonzero(action == "T")
    event_positions = np.searchsorted(event_ends, trade_indices, side="left")
    unique_events, first_positions, counts = np.unique(
        event_positions, return_index=True, return_counts=True
    )
    records: list[dict[str, float | int]] = []
    episodes: list[RefillEpisode] = []
    anchors = 0
    depleted_anchors = 0
    clean_trade_indices: list[int] = []
    for event_position, first_position, count in zip(
        unique_events.tolist(), first_positions.tolist(), counts.tolist(), strict=True
    ):
        indices = trade_indices[first_position : first_position + count]
        end_index = int(event_ends[event_position])
        start_index = 0 if event_position == 0 else int(event_ends[event_position - 1]) + 1
        first_trade = int(indices[0])
        if first_trade != start_index:
            continue
        if np.any((flags[start_index : end_index + 1] & BAD_FLAG_MASK) != 0):
            continue
        if np.any(action[start_index : end_index + 1] == "R"):
            continue
        if not (
            _valid_bbo(bid_px[[first_trade]], ask_px[[first_trade]])[0]
            and _valid_bbo(bid_px[[end_index]], ask_px[[end_index]])[0]
        ):
            continue
        signed = np.where(
            side[indices] == "B", size[indices], np.where(side[indices] == "A", -size[indices], 0)
        )
        event_flow = int(signed.sum())
        abs_flow = int(np.abs(signed).sum())
        if abs_flow == 0:
            continue
        clean_trade_indices.extend(indices.tolist())
        pre_mid_x2 = int(bid_px[first_trade, 0]) + int(ask_px[first_trade, 0])
        post_mid_x2 = int(bid_px[end_index, 0]) + int(ask_px[end_index, 0])
        delta_ticks = (post_mid_x2 - pre_mid_x2) / (2.0 * TICK_SIZE_RAW)
        flow_sign = 1 if event_flow > 0 else (-1 if event_flow < 0 else 0)
        aligned = flow_sign * delta_ticks if flow_sign else 0.0
        bucket_s = int(
            _ceil_div(int(ts_ns[end_index]), FIVE_MINUTES_S * SECOND_NS) * FIVE_MINUTES_S
        )
        records.append(
            {
                "bucket_s": bucket_s,
                "flow_signed": event_flow,
                "flow_abs": abs_flow,
                "flow_buy": int(size[indices][side[indices] == "B"].sum()),
                "flow_sell": int(size[indices][side[indices] == "A"].sum()),
                "trade_event_count": 1,
                "impact_aligned_numerator": aligned * abs_flow,
                "impact_buy_numerator": delta_ticks
                * int(size[indices][side[indices] == "B"].sum()),
                "impact_sell_numerator": -delta_ticks
                * int(size[indices][side[indices] == "A"].sum()),
                "zero_impact_flow": abs_flow if delta_ticks == 0 else 0,
            }
        )
        anchors_by_key: dict[tuple[int, int], int] = defaultdict(int)
        for index in indices.tolist():
            direction = 1 if side[index] == "B" else (-1 if side[index] == "A" else 0)
            if direction:
                anchors_by_key[(direction, int(price[index]))] += int(size[index])
        for (direction, anchor_price), anchor_volume in anchors_by_key.items():
            anchors += 1
            if anchor_price % TICK_SIZE_RAW:
                continue
            if direction > 0:
                pre_size = _exact_size(ask_px[first_trade], ask_sz[first_trade], anchor_price)
                post_size = _exact_size(ask_px[end_index], ask_sz[end_index], anchor_price)
            else:
                pre_size = _exact_size(bid_px[first_trade], bid_sz[first_trade], anchor_price)
                post_size = _exact_size(bid_px[end_index], bid_sz[end_index], anchor_price)
            if pre_size is None:
                continue
            if post_size is None:
                post_value, _breached, post_censored = _observe_exact_price(
                    direction,
                    anchor_price,
                    ask_px[end_index] if direction > 0 else bid_px[end_index],
                    ask_sz[end_index] if direction > 0 else bid_sz[end_index],
                )
                if post_censored or post_value is None:
                    continue
            else:
                post_value = post_size
            depletion = pre_size - post_value
            if depletion <= 0:
                continue
            depleted_anchors += 1
            episodes.append(
                RefillEpisode(
                    event_end_ns=int(ts_ns[end_index]),
                    direction=direction,
                    price_raw=anchor_price,
                    post_size=post_value,
                    depletion=depletion,
                    flow_volume=anchor_volume,
                    event_mid_x2_raw=post_mid_x2,
                    immediate_aligned_impact_ticks=direction * delta_ticks,
                )
            )
    return (
        pd.DataFrame.from_records(records),
        episodes,
        np.asarray(clean_trade_indices, dtype=np.int64),
        {
            "trade_rows": len(trade_indices),
            "trade_events": len(unique_events),
            "trade_price_anchors": anchors,
            "depleted_trade_price_anchors": depleted_anchors,
        },
    )


def _second_snapshots(
    event_end_ns: np.ndarray,
    event_valid: np.ndarray,
    bid_px: np.ndarray,
    ask_px: np.ndarray,
    bid_sz: np.ndarray,
    ask_sz: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    event_seconds = _ceil_div(event_end_ns, SECOND_NS).astype(np.int64)
    last = np.r_[event_seconds[1:] != event_seconds[:-1], True]
    indices = np.flatnonzero(last)
    indices = indices[event_valid[indices]]
    seconds = event_seconds[indices]
    return seconds, bid_px[indices], ask_px[indices], bid_sz[indices], ask_sz[indices]


def _observe_exact_price(
    direction: int,
    price: int,
    prices: np.ndarray,
    sizes: np.ndarray,
) -> tuple[int | None, bool, bool]:
    exact = _exact_size(prices, sizes, price)
    if exact is not None:
        return exact, False, False
    defined = prices[prices != UNDEFINED_PRICE]
    if len(defined) == 0:
        return None, False, True
    best = int(defined[0])
    deepest = int(defined[-1])
    if direction > 0:  # consumed ask
        if price < best:
            return 0, True, False
        if price > deepest:
            return None, False, True
    else:  # consumed bid
        if price > best:
            return 0, True, False
        if price < deepest:
            return None, False, True
    return 0, False, False


def _refill_records(
    episodes: list[RefillEpisode],
    event_end_ns: np.ndarray,
    event_valid: np.ndarray,
    bid_px: np.ndarray,
    ask_px: np.ndarray,
    bid_sz: np.ndarray,
    ask_sz: np.ndarray,
) -> tuple[pd.DataFrame, dict[str, int]]:
    records: list[dict[str, float | int | bool]] = []
    censored = 0
    observed = 0
    invalid_prefix = np.cumsum((~event_valid).astype(np.int64))
    for episode in episodes:
        start_position = int(np.searchsorted(event_end_ns, episode.event_end_ns, side="left"))
        for horizon in REFILL_HORIZONS_SECONDS:
            target_ns = episode.event_end_ns + horizon * SECOND_NS
            position = int(np.searchsorted(event_end_ns, target_ns, side="right")) - 1
            if position < start_position:
                censored += 1
                continue
            if target_ns - int(event_end_ns[position]) > SECOND_NS:
                censored += 1
                continue
            invalid_count = int(invalid_prefix[position]) - (
                int(invalid_prefix[start_position]) if start_position >= 0 else 0
            )
            if invalid_count or not bool(event_valid[position]):
                censored += 1
                continue
            if not _valid_bbo(bid_px[position : position + 1], ask_px[position : position + 1])[0]:
                censored += 1
                continue
            if episode.direction > 0:
                value, breached, is_censored = _observe_exact_price(
                    episode.direction,
                    episode.price_raw,
                    ask_px[position],
                    ask_sz[position],
                )
            else:
                value, breached, is_censored = _observe_exact_price(
                    episode.direction,
                    episode.price_raw,
                    bid_px[position],
                    bid_sz[position],
                )
            if is_censored or value is None:
                censored += 1
                continue
            observed += 1
            recovered = max(0, min(episode.depletion, value - episode.post_size))
            refill = recovered / episode.depletion
            target_mid_x2 = int(bid_px[position, 0]) + int(ask_px[position, 0])
            aligned_impact = (
                episode.direction
                * (target_mid_x2 - episode.event_mid_x2_raw)
                / (2.0 * TICK_SIZE_RAW)
            )
            bucket_s = int(_ceil_div(target_ns, FIVE_MINUTES_S * SECOND_NS) * FIVE_MINUTES_S)
            records.append(
                {
                    "bucket_s": bucket_s,
                    "target_ns": target_ns,
                    "horizon_s": horizon,
                    "direction": episode.direction,
                    "price_ticks": episode.price_raw // TICK_SIZE_RAW,
                    "depletion": episode.depletion,
                    "refill_numerator": refill * episode.depletion,
                    "breach_weight": episode.depletion if breached else 0,
                    "flow_volume": episode.flow_volume,
                    "response_impact_numerator": aligned_impact * episode.flow_volume,
                    "immediate_impact_numerator": (
                        episode.immediate_aligned_impact_ticks * episode.flow_volume
                    ),
                    "acceptance_path_numerator": aligned_impact
                    * (1.0 - refill)
                    * episode.flow_volume,
                }
            )
    return pd.DataFrame.from_records(records), {
        "refill_observed": observed,
        "refill_censored": censored,
    }


def _book_records(
    seconds: np.ndarray,
    bid_px: np.ndarray,
    ask_px: np.ndarray,
    bid_sz: np.ndarray,
    ask_sz: np.ndarray,
) -> pd.DataFrame:
    valid = _valid_bbo(bid_px, ask_px)
    bid_defined = bid_px != UNDEFINED_PRICE
    ask_defined = ask_px != UNDEFINED_PRICE
    bid_weights = np.where(bid_defined, bid_sz, 0).astype(np.float64)
    ask_weights = np.where(ask_defined, ask_sz, 0).astype(np.float64)
    bid_prices = np.where(bid_defined, bid_px, 0).astype(np.float64)
    ask_prices = np.where(ask_defined, ask_px, 0).astype(np.float64)
    total_bid = bid_weights.sum(axis=1)
    total_ask = ask_weights.sum(axis=1)
    total = total_bid + total_ask
    center = np.divide(
        (bid_prices * bid_weights).sum(axis=1) + (ask_prices * ask_weights).sum(axis=1),
        total,
        out=np.full(len(seconds), np.nan),
        where=total > 0,
    )
    mid = (bid_px[:, 0].astype(np.float64) + ask_px[:, 0].astype(np.float64)) / 2.0
    imbalance = np.divide(
        total_bid - total_ask,
        total,
        out=np.full(len(seconds), np.nan),
        where=total > 0,
    )
    spread = (ask_px[:, 0].astype(np.float64) - bid_px[:, 0].astype(np.float64)) / TICK_SIZE_RAW
    frame = pd.DataFrame(
        {
            "bucket_s": (_ceil_div(seconds, FIVE_MINUTES_S) * FIVE_MINUTES_S).astype(np.int64),
            "second_s": seconds,
            "valid": valid,
            "center_offset": (center - mid) / TICK_SIZE_RAW,
            "depth_total": total,
            "depth_imbalance": imbalance,
            "spread_ticks": spread,
            "decision_bid_ticks": bid_px[:, 0].astype(np.float64) / TICK_SIZE_RAW,
            "decision_ask_ticks": ask_px[:, 0].astype(np.float64) / TICK_SIZE_RAW,
        }
    )
    return frame.loc[frame["valid"] & np.isfinite(frame["center_offset"])].copy()


def _weighted_mean(group: pd.DataFrame, numerator: str, denominator: str) -> float:
    total = float(group[denominator].sum())
    return float(group[numerator].sum() / total) if total > 0 else math.nan


def _assemble_micro_features(
    event_frame: pd.DataFrame,
    refill_frame: pd.DataFrame,
    book_frame: pd.DataFrame,
    trade_buckets: np.ndarray,
    trade_prices: np.ndarray,
    trade_sizes: np.ndarray,
) -> pd.DataFrame:
    pieces: list[pd.DataFrame] = []
    if not event_frame.empty:
        rows: list[dict[str, float | int]] = []
        for bucket, group in event_frame.groupby("bucket_s", sort=True):
            abs_flow = float(group["flow_abs"].sum())
            buy = float(group["flow_buy"].sum())
            sell = float(group["flow_sell"].sum())
            rows.append(
                {
                    "bucket_s": int(bucket),
                    "flow_signed": float(group["flow_signed"].sum()),
                    "flow_abs": abs_flow,
                    "flow_buy": buy,
                    "flow_sell": sell,
                    "trade_event_count": int(group["trade_event_count"].sum()),
                    "impact_aligned_ticks": float(
                        group["impact_aligned_numerator"].sum() / abs_flow
                    )
                    if abs_flow
                    else math.nan,
                    "impact_buy_ticks": float(group["impact_buy_numerator"].sum() / buy)
                    if buy
                    else math.nan,
                    "impact_sell_ticks": float(group["impact_sell_numerator"].sum() / sell)
                    if sell
                    else math.nan,
                    "zero_impact_flow_share": float(group["zero_impact_flow"].sum() / abs_flow)
                    if abs_flow
                    else math.nan,
                }
            )
        pieces.append(pd.DataFrame.from_records(rows).set_index("bucket_s"))
    if len(trade_buckets):
        trades = pd.DataFrame(
            {
                "bucket_s": trade_buckets,
                "price": trade_prices.astype(np.float64),
                "size": trade_sizes.astype(np.float64),
            }
        )
        trades["weighted_price"] = trades["price"] * trades["size"]
        center = trades.groupby("bucket_s", sort=True).agg(
            weighted_price=("weighted_price", "sum"), trade_size=("size", "sum")
        )
        center["trade_center_raw"] = center["weighted_price"] / center["trade_size"]
        pieces.append(center[["trade_center_raw"]])
    if not refill_frame.empty:
        rows = []
        for (bucket, horizon, direction), group in refill_frame.groupby(
            ["bucket_s", "horizon_s", "direction"], sort=True
        ):
            depletion = float(group["depletion"].sum())
            label = "buy" if int(direction) > 0 else "sell"
            rows.append(
                {
                    "bucket_s": int(bucket),
                    "name": f"{int(horizon)}s_{label}",
                    "refill": float(group["refill_numerator"].sum() / depletion)
                    if depletion
                    else math.nan,
                    "breach": float(group["breach_weight"].sum() / depletion)
                    if depletion
                    else math.nan,
                    "depletion": depletion,
                    "response_flow": float(group["flow_volume"].sum()),
                    "response_impact": _weighted_mean(
                        group, "response_impact_numerator", "flow_volume"
                    ),
                    "response_immediate_impact": _weighted_mean(
                        group, "immediate_impact_numerator", "flow_volume"
                    ),
                    "acceptance_path": _weighted_mean(
                        group, "acceptance_path_numerator", "flow_volume"
                    ),
                }
            )
        wide_rows: dict[int, dict[str, float | int]] = defaultdict(dict)
        for row in rows:
            bucket = int(row["bucket_s"])
            name = str(row["name"])
            wide_rows[bucket][f"refill_{name}"] = float(row["refill"])
            wide_rows[bucket][f"breach_{name}"] = float(row["breach"])
            wide_rows[bucket][f"depletion_{name}"] = float(row["depletion"])
            wide_rows[bucket][f"response_flow_{name}"] = float(row["response_flow"])
            wide_rows[bucket][f"response_impact_{name}"] = float(row["response_impact"])
            wide_rows[bucket][f"response_immediate_impact_{name}"] = float(
                row["response_immediate_impact"]
            )
            wide_rows[bucket][f"acceptance_path_{name}"] = float(row["acceptance_path"])
        refill_wide = pd.DataFrame.from_dict(wide_rows, orient="index")
        refill_wide.index.name = "bucket_s"
        pieces.append(refill_wide)
    if not book_frame.empty:
        grouped = book_frame.groupby("bucket_s", sort=True)
        book = grouped.agg(
            book_center_offset_mean=("center_offset", "mean"),
            book_center_offset_last=("center_offset", "last"),
            book_center_offset_first=("center_offset", "first"),
            book_depth_total_mean=("depth_total", "mean"),
            book_depth_imbalance_mean=("depth_imbalance", "mean"),
            book_depth_imbalance_last=("depth_imbalance", "last"),
            spread_ticks_mean=("spread_ticks", "mean"),
            decision_bid_ticks=("decision_bid_ticks", "last"),
            decision_ask_ticks=("decision_ask_ticks", "last"),
        )
        book["book_center_offset_change"] = (
            book["book_center_offset_last"] - book["book_center_offset_first"]
        )
        pieces.append(book.drop(columns=["book_center_offset_first"]))
    if not pieces:
        return pd.DataFrame()
    result = pd.concat(pieces, axis=1).sort_index()
    return result


def _liquidity_memory_features(
    refill_frame: pd.DataFrame,
    base: pd.DataFrame,
) -> pd.DataFrame:
    columns = list(MEMORY_FEATURES)

    def empty_result() -> pd.DataFrame:
        result = pd.DataFrame(index=base.index, columns=columns, dtype=np.float64)
        zero_columns = [
            name for name in columns if "distance" not in name and "age_minutes" not in name
        ]
        result[zero_columns] = 0.0
        return result

    if refill_frame.empty:
        return empty_result()
    memory = refill_frame.loc[refill_frame["horizon_s"] == 30]
    if memory.empty:
        return empty_result()
    grouped = (
        memory.groupby(["bucket_s", "direction", "price_ticks"], sort=True)
        .agg(
            depletion=("depletion", "sum"),
            recovered=("refill_numerator", "sum"),
        )
        .reset_index()
        .sort_values(["bucket_s", "direction", "price_ticks"])
    )
    records = list(grouped.itertuples(index=False, name=None))
    active: deque[tuple[int, int, int, float, float]] = deque()
    state: dict[tuple[int, int], list[Any]] = {}
    cursor = 0
    rows: list[dict[str, float | int]] = []

    def add_record(record: tuple[int, int, int, float, float]) -> None:
        bucket, direction, price_ticks, depletion, recovered = record
        key = (direction, price_ticks)
        entry = state.setdefault(key, [0.0, 0.0, deque(), False])
        entry[0] += depletion
        entry[1] += recovered
        entry[2].append(bucket)
        active.append(record)

    def remove_record(record: tuple[int, int, int, float, float]) -> None:
        bucket, direction, price_ticks, depletion, recovered = record
        key = (direction, price_ticks)
        entry = state[key]
        entry[0] -= depletion
        entry[1] -= recovered
        removed_bucket = entry[2].popleft()
        if removed_bucket != bucket:
            raise RuntimeError("liquidity-memory publication order regressed")
        if not entry[2]:
            del state[key]

    def side_summary(
        *,
        direction: int,
        prices: range,
        reference_price: int,
        bucket_s: int,
    ) -> tuple[float, float, float, float, float, float, int]:
        depletion = 0.0
        recovered = 0.0
        nearest_distance = math.inf
        nearest_age = math.nan
        at_bbo = 0.0
        levels = 0
        for price_ticks in prices:
            entry = state.get((direction, price_ticks))
            if entry is None or entry[0] <= 0:
                continue
            levels += 1
            depletion += float(entry[0])
            recovered += float(entry[1])
            if price_ticks == reference_price:
                at_bbo += float(entry[1])
            distance = abs(price_ticks - reference_price)
            age = (bucket_s - int(entry[2][-1])) / 60.0
            if distance < nearest_distance or (
                distance == nearest_distance
                and (not math.isfinite(nearest_age) or age < nearest_age)
            ):
                nearest_distance = float(distance)
                nearest_age = float(age)
        ratio = recovered / depletion if depletion > 0 else 0.0
        return (
            recovered,
            max(0.0, depletion - recovered),
            ratio,
            nearest_distance if math.isfinite(nearest_distance) else math.nan,
            nearest_age,
            at_bbo,
            levels,
        )

    for bucket, base_row in base.sort_index().iterrows():
        bucket_s = int(bucket)
        pending: list[tuple[int, int, int, float, float]] = []
        while cursor < len(records) and int(records[cursor][0]) <= bucket_s:
            raw = records[cursor]
            pending.append(
                (
                    int(raw[0]),
                    int(raw[1]),
                    int(raw[2]),
                    float(raw[3]),
                    float(raw[4]),
                )
            )
            cursor += 1
        cutoff = bucket_s - MEMORY_LOOKBACK_S
        while active and active[0][0] < cutoff:
            remove_record(active.popleft())
        bid_value = float(base_row["bid_ticks"])
        ask_value = float(base_row["ask_ticks"])
        grid_valid = (
            bool(base_row["decision_fresh"])
            and bool(base_row["path_contamination_free"])
            and (
                math.isfinite(bid_value)
                and math.isfinite(ask_value)
                and bid_value.is_integer()
                and ask_value.is_integer()
                and bid_value < ask_value
            )
        )
        bid = int(bid_value) if grid_valid else 0
        ask = int(ask_value) if grid_valid else 0
        current_bbo_keys = {(-1, bid), (1, ask)} if grid_valid else set()

        support_exact = state.get((-1, bid)) if grid_valid else None
        resistance_exact = state.get((1, ask)) if grid_valid else None
        support_recontact = bool(support_exact is not None and bool(support_exact[3]))
        resistance_recontact = bool(resistance_exact is not None and bool(resistance_exact[3]))
        support_recontact_recovered = (
            float(support_exact[1]) if support_recontact and support_exact is not None else 0.0
        )
        resistance_recontact_recovered = (
            float(resistance_exact[1])
            if resistance_recontact and resistance_exact is not None
            else 0.0
        )
        support_recontact_unrecovered = (
            max(0.0, float(support_exact[0]) - float(support_exact[1]))
            if support_recontact and support_exact is not None
            else 0.0
        )
        resistance_recontact_unrecovered = (
            max(0.0, float(resistance_exact[0]) - float(resistance_exact[1]))
            if resistance_recontact and resistance_exact is not None
            else 0.0
        )
        if support_recontact and support_exact is not None:
            support_exact[3] = False
        if resistance_recontact and resistance_exact is not None:
            resistance_exact[3] = False

        if grid_valid:
            for key, entry in state.items():
                if key not in current_bbo_keys:
                    entry[3] = True

        for record in pending:
            if record[0] < cutoff:
                continue
            add_record(record)
            key = (record[1], record[2])
            if grid_valid and key not in current_bbo_keys:
                state[key][3] = True

        if not grid_valid:
            rows.append(
                {
                    "bucket_s": bucket_s,
                    **{
                        name: 0.0
                        for name in columns
                        if "distance" not in name and "age_minutes" not in name
                    },
                }
            )
            continue
        support = side_summary(
            direction=-1,
            prices=range(bid - MEMORY_RADIUS_TICKS, bid + 1),
            reference_price=bid,
            bucket_s=bucket_s,
        )
        resistance = side_summary(
            direction=1,
            prices=range(ask, ask + MEMORY_RADIUS_TICKS + 1),
            reference_price=ask,
            bucket_s=bucket_s,
        )
        rows.append(
            {
                "bucket_s": bucket_s,
                "memory_support_recovered": support[0],
                "memory_resistance_recovered": resistance[0],
                "memory_support_unrecovered": support[1],
                "memory_resistance_unrecovered": resistance[1],
                "memory_support_refill_ratio": support[2],
                "memory_resistance_refill_ratio": resistance[2],
                "memory_nearest_support_distance": support[3],
                "memory_nearest_resistance_distance": resistance[3],
                "memory_support_age_minutes": support[4],
                "memory_resistance_age_minutes": resistance[4],
                "memory_support_recovered_at_bbo": support[5],
                "memory_resistance_recovered_at_bbo": resistance[5],
                "memory_support_grid_recontact_recovered": support_recontact_recovered,
                "memory_resistance_grid_recontact_recovered": resistance_recontact_recovered,
                "memory_support_grid_recontact_unrecovered": support_recontact_unrecovered,
                "memory_resistance_grid_recontact_unrecovered": resistance_recontact_unrecovered,
                "memory_grid_recontact_net_log_strength": math.log1p(
                    support_recontact_recovered + resistance_recontact_unrecovered
                )
                - math.log1p(resistance_recontact_recovered + support_recontact_unrecovered),
                "memory_grid_recontact_side_count": int(support_recontact)
                + int(resistance_recontact),
                "memory_net_log_strength": math.log1p(support[0] + resistance[1])
                - math.log1p(resistance[0] + support[1]),
                "memory_level_count": support[6] + resistance[6],
            }
        )
    result = pd.DataFrame.from_records(rows).set_index("bucket_s")
    return result.reindex(columns=columns)


def _execution_quote_frame(
    base: pd.DataFrame,
    cache: ExecutionCache,
    clean_buckets: set[int],
) -> tuple[pd.DataFrame, dict[str, int]]:
    wanted = [
        "event_index",
        "ts_recv_ns",
        "best_bid_ticks",
        "best_ask_ticks",
        "valid",
        "sequence",
        "source_row_index",
    ]
    table = pq.ParquetFile(cache.path).read(columns=wanted, use_threads=True)
    if table.num_rows != cache.cached_quote_count:
        raise RuntimeError("execution-cache row count changed during read")
    event_index = _numpy_column(table, "event_index").astype(np.int64)
    ts_ns = _numpy_column(table, "ts_recv_ns").astype(np.int64)
    bid_ticks = np.asarray(table["best_bid_ticks"].combine_chunks().to_pylist(), dtype=np.float64)
    ask_ticks = np.asarray(table["best_ask_ticks"].combine_chunks().to_pylist(), dtype=np.float64)
    row_valid = np.asarray(table["valid"].combine_chunks().to_pylist(), dtype=bool)
    sequence = _numpy_column(table, "sequence").astype(np.uint64)
    source_row_index = _numpy_column(table, "source_row_index").astype(np.int64)
    if len(ts_ns) == 0 or np.any(np.diff(event_index) <= 0) or np.any(np.diff(ts_ns) < 0):
        raise RuntimeError("execution-cache order is not canonical")
    if np.any(event_index - cache.event_index_offset != source_row_index):
        raise RuntimeError("execution-cache source-row lineage drifted")
    if np.any(sequence > np.iinfo(np.uint32).max):
        raise RuntimeError("execution-cache sequence is outside uint32")
    valid_quotes = (
        np.isfinite(bid_ticks)
        & np.isfinite(ask_ticks)
        & (bid_ticks == np.floor(bid_ticks))
        & (ask_ticks == np.floor(ask_ticks))
        & (bid_ticks < ask_ticks)
    )
    if np.any(row_valid & ~valid_quotes):
        raise RuntimeError("execution-cache marks an invalid BBO as executable")
    invalid_prefix = np.r_[0, np.cumsum((~row_valid).astype(np.int64))]
    gap_invalid = np.zeros(len(ts_ns), dtype=bool)
    gap_invalid[1:] = np.diff(ts_ns) > SECOND_NS
    gap_prefix = np.r_[0, np.cumsum(gap_invalid.astype(np.int64))]

    def valid_range(first: int, last: int) -> bool:
        if first > last:
            return True
        return int(invalid_prefix[last + 1] - invalid_prefix[first]) == 0

    def gap_range(first: int, last: int) -> bool:
        if first > last:
            return True
        return int(gap_prefix[last + 1] - gap_prefix[first]) == 0

    def routed_attempt(
        decision_ns: int,
    ) -> tuple[tuple[int, int] | None, str]:
        decision_position = int(np.searchsorted(ts_ns, decision_ns, side="right")) - 1
        if decision_position < 0 or not bool(row_valid[decision_position]):
            return None, "INVALID_DECISION_BBO"
        if decision_ns - int(ts_ns[decision_position]) > SECOND_NS:
            return None, "STALE_DECISION_BBO"
        eligibility_ns = decision_ns + ROUTING_DELAY_NS
        attempt_position = int(np.searchsorted(ts_ns, eligibility_ns, side="left"))
        if attempt_position >= len(ts_ns):
            return None, "NO_ATTEMPT_EVENT"
        if int(ts_ns[attempt_position]) - eligibility_ns > MAX_ATTEMPT_WAIT_NS:
            return None, "ATTEMPT_WAIT_GT_1S"
        snapshot_position = (
            attempt_position
            if int(ts_ns[attempt_position]) == eligibility_ns
            else attempt_position - 1
        )
        if snapshot_position < decision_position:
            return None, "ELIGIBILITY_PRECEDES_DECISION"
        if eligibility_ns - int(ts_ns[snapshot_position]) > SECOND_NS:
            return None, "STALE_ELIGIBILITY_BBO"
        if not valid_range(decision_position + 1, attempt_position) or not gap_range(
            decision_position + 1, snapshot_position
        ):
            return None, "INVALID_OR_STALE_ROUTE"
        return (snapshot_position, attempt_position), "FILLED"

    rows: list[dict[str, float | int]] = []
    considered = 0
    entry_fills = 0
    round_trips = 0
    reasons: dict[str, int] = defaultdict(int)
    adverse_long = 0
    adverse_short = 0
    for bucket, base_row in base.sort_index().iterrows():
        bucket_s = int(bucket)
        if bucket_s not in clean_buckets or not bool(base_row["decision_fresh"]):
            continue
        considered += 1
        entry, entry_reason = routed_attempt(bucket_s * SECOND_NS)
        if entry is None:
            reasons[f"ENTRY_{entry_reason}"] += 1
            rows.append(
                {
                    "bucket_s": bucket_s,
                    "execution_cache_available": 1,
                    "exec_entry_fillable": 0,
                    "exec_exit_fillable": 0,
                }
            )
            continue
        entry_fills += 1
        entry_snapshot, entry_position = entry
        target_bucket_s = bucket_s + 360 * 60
        target_base_valid = (
            target_bucket_s in base.index
            and target_bucket_s in clean_buckets
            and bool(base.loc[target_bucket_s, "decision_fresh"])
        )
        if target_base_valid:
            exit_attempt, exit_reason = routed_attempt(target_bucket_s * SECOND_NS)
        else:
            exit_attempt, exit_reason = None, "INVALID_TARGET_DECISION"
        entry_bid = int(bid_ticks[entry_position])
        entry_ask = int(ask_ticks[entry_position])
        entry_snapshot_bid = int(bid_ticks[entry_snapshot])
        entry_snapshot_ask = int(ask_ticks[entry_snapshot])
        adverse_long += int(entry_ask > entry_snapshot_ask)
        adverse_short += int(entry_bid < entry_snapshot_bid)
        row: dict[str, float | int] = {
            "bucket_s": bucket_s,
            "execution_cache_available": 1,
            "exec_entry_fillable": 1,
            "exec_exit_fillable": int(exit_attempt is not None),
            "entry_fill_ns": int(ts_ns[entry_position]),
            "entry_attempt_delay_ms": (
                int(ts_ns[entry_position]) - (bucket_s * SECOND_NS + ROUTING_DELAY_NS)
            )
            / 1_000_000.0,
            "entry_long_quote_move_ticks": entry_ask - entry_snapshot_ask,
            "entry_short_quote_move_ticks": entry_snapshot_bid - entry_bid,
        }
        if exit_attempt is None:
            reasons[f"EXIT_{exit_reason}"] += 1
            rows.append(row)
            continue
        round_trips += 1
        exit_snapshot, exit_position = exit_attempt
        exit_bid = int(bid_ticks[exit_position])
        exit_ask = int(ask_ticks[exit_position])
        exit_snapshot_bid = int(bid_ticks[exit_snapshot])
        exit_snapshot_ask = int(ask_ticks[exit_snapshot])
        row.update(
            {
                "long_exec_gross_360m": float(exit_bid - entry_ask),
                "short_exec_gross_360m": float(entry_bid - exit_ask),
                "exit_fill_ns": int(ts_ns[exit_position]),
                "exit_attempt_delay_ms": (
                    int(ts_ns[exit_position]) - (target_bucket_s * SECOND_NS + ROUTING_DELAY_NS)
                )
                / 1_000_000.0,
                "exit_long_quote_move_ticks": exit_snapshot_bid - exit_bid,
                "exit_short_quote_move_ticks": exit_ask - exit_snapshot_ask,
            }
        )
        rows.append(row)
    if not rows:
        frame = pd.DataFrame(dtype=np.float64)
        frame.index.name = "bucket_s"
    else:
        frame = pd.DataFrame.from_records(rows).set_index("bucket_s")
    audit = {
        "execution_decisions_considered": considered,
        "execution_entry_fills": entry_fills,
        "execution_round_trips": round_trips,
        "execution_entry_adverse_long": adverse_long,
        "execution_entry_adverse_short": adverse_short,
    }
    audit.update({f"execution_reason_{key}": value for key, value in sorted(reasons.items())})
    return frame, audit


def _load_five_minute_frame(path: Path) -> pd.DataFrame:
    parquet = pq.ParquetFile(path)
    names = set(parquet.schema_arrow.names)
    wanted = [
        "bucket_end",
        "last_bid_px_00_ticks",
        "last_ask_px_00_ticks",
        "mid_px_x2_raw_high",
        "mid_px_x2_raw_low",
        "mid_px_x2_raw_close",
        "decision_quote_fresh",
        *PATH_CONTAMINATION_FIELDS,
    ]
    missing = set(wanted) - names
    if missing:
        raise RuntimeError(f"5m artifact missing fields: {sorted(missing)}")
    table = parquet.read(columns=wanted, use_threads=True)
    bucket_ns = pc.cast(table["bucket_end"].combine_chunks(), pa.int64()).to_numpy(
        zero_copy_only=False
    )
    frame = pd.DataFrame(
        {
            "bucket_s": (bucket_ns // SECOND_NS).astype(np.int64),
            "bid_ticks": _numpy_column(table, "last_bid_px_00_ticks").astype(np.float64),
            "ask_ticks": _numpy_column(table, "last_ask_px_00_ticks").astype(np.float64),
            "high_half_ticks": _numpy_column(table, "mid_px_x2_raw_high").astype(np.float64)
            / TICK_SIZE_RAW,
            "low_half_ticks": _numpy_column(table, "mid_px_x2_raw_low").astype(np.float64)
            / TICK_SIZE_RAW,
            "close_half_ticks": _numpy_column(table, "mid_px_x2_raw_close").astype(np.float64)
            / TICK_SIZE_RAW,
            "decision_fresh": np.asarray(table["decision_quote_fresh"].to_pylist(), dtype=bool),
        }
    ).set_index("bucket_s")
    frame["path_contamination_free"] = True
    for name in PATH_CONTAMINATION_FIELDS:
        frame["path_contamination_free"] &= _numpy_column(table, name) == 0
    return frame.sort_index()


def _price_and_labels(base: pd.DataFrame, micro: pd.DataFrame, source_date: str) -> pd.DataFrame:
    frame = base.join(micro, how="left")
    for name in ("decision_bid_ticks", "decision_ask_ticks"):
        if name not in frame:
            frame[name] = math.nan
    micro_present = frame["decision_bid_ticks"].notna() & frame["decision_ask_ticks"].notna()
    micro_bbo = micro_present & (
        frame["decision_bid_ticks"].notna()
        & frame["decision_ask_ticks"].notna()
        & (frame["decision_bid_ticks"] < frame["decision_ask_ticks"])
    )
    frame.loc[micro_bbo, "bid_ticks"] = frame.loc[micro_bbo, "decision_bid_ticks"]
    frame.loc[micro_bbo, "ask_ticks"] = frame.loc[micro_bbo, "decision_ask_ticks"]
    frame["decision_fresh"] = frame["decision_fresh"] & (~micro_present | micro_bbo)
    for name in LIQUIDITY_FEATURES:
        if name not in frame:
            frame[name] = math.nan
    if "trade_center_raw" not in frame:
        frame["trade_center_raw"] = math.nan
    zero_fields = (
        "flow_signed",
        "flow_abs",
        "flow_buy",
        "flow_sell",
        "trade_event_count",
        "depletion_5s_buy",
        "depletion_5s_sell",
        "depletion_30s_buy",
        "depletion_30s_sell",
        "depletion_120s_buy",
        "depletion_120s_sell",
    )
    frame[list(zero_fields)] = frame[list(zero_fields)].fillna(0.0)
    frame["trade_center_offset_ticks"] = (
        frame["trade_center_raw"] - frame["close_half_ticks"] * TICK_SIZE_RAW / 2.0
    ) / TICK_SIZE_RAW
    by_bucket = {int(bucket): position for position, bucket in enumerate(frame.index)}
    trade_center = frame["trade_center_raw"].to_numpy(dtype=np.float64)
    migration = np.full(len(frame), np.nan)
    for position, bucket in enumerate(frame.index):
        prior = by_bucket.get(int(bucket) - FIVE_MINUTES_S)
        if (
            prior is not None
            and math.isfinite(trade_center[position])
            and math.isfinite(trade_center[prior])
        ):
            migration[position] = (trade_center[position] - trade_center[prior]) / TICK_SIZE_RAW
    frame["trade_center_migration_ticks"] = migration
    history_contamination_free = np.zeros(len(frame), dtype=bool)
    for position, bucket in enumerate(frame.index):
        history_positions = [
            by_bucket.get(int(bucket) - offset * FIVE_MINUTES_S) for offset in range(12, -1, -1)
        ]
        if all(item is not None for item in history_positions):
            history = frame.iloc[np.asarray(history_positions, dtype=np.int64)]
            history_contamination_free[position] = bool(
                history["path_contamination_free"].all()
            ) and _bounded_fresh_path(history["decision_fresh"].to_numpy(dtype=bool))
    frame["history_contamination_free"] = history_contamination_free
    close = frame["close_half_ticks"].to_numpy(dtype=np.float64)
    high_values = frame["high_half_ticks"].to_numpy(dtype=np.float64)
    low_values = frame["low_half_ticks"].to_numpy(dtype=np.float64)
    ret_5m = np.full(len(frame), np.nan)
    ret_30m = np.full(len(frame), np.nan)
    ret_60m = np.full(len(frame), np.nan)
    vol_30m = np.full(len(frame), np.nan)
    range_30m = np.full(len(frame), np.nan)
    close_location = np.full(len(frame), np.nan)
    for position, bucket in enumerate(frame.index):
        for lag_s, target in ((300, ret_5m), (1_800, ret_30m), (3_600, ret_60m)):
            prior = by_bucket.get(int(bucket) - lag_s)
            if prior is not None and math.isfinite(close[position]) and math.isfinite(close[prior]):
                target[position] = (close[position] - close[prior]) / 2.0
        window_positions = [
            by_bucket.get(int(bucket) - offset * 300) for offset in range(6, -1, -1)
        ]
        if all(window_position is not None for window_position in window_positions):
            exact = np.asarray(window_positions, dtype=np.int64)
            window_close = close[exact]
            window_high = high_values[exact[-6:]]
            window_low = low_values[exact[-6:]]
            if np.all(np.isfinite(window_close)):
                vol_30m[position] = float(np.std(np.diff(window_close) / 2.0, ddof=0))
            if np.all(np.isfinite(window_high)) and np.all(np.isfinite(window_low)):
                maximum = float(np.max(window_high))
                minimum = float(np.min(window_low))
                width = maximum - minimum
                range_30m[position] = width / 2.0
                if width > 0 and math.isfinite(close[position]):
                    close_location[position] = (close[position] - minimum) / width
    frame["price_ret_5m"] = ret_5m
    frame["price_ret_30m"] = ret_30m
    frame["price_ret_60m"] = ret_60m
    frame["price_vol_30m"] = vol_30m
    frame["price_range_30m"] = range_30m
    frame["price_close_location_30m"] = close_location
    seconds_in_day = frame.index.to_numpy(dtype=np.int64) % 86_400
    angle = 2.0 * np.pi * seconds_in_day / 86_400.0
    frame["clock_sin"] = np.sin(angle)
    frame["clock_cos"] = np.cos(angle)
    for horizon in HORIZONS_MINUTES:
        steps_s = horizon * 60
        mid_label = np.full(len(frame), np.nan)
        long_gross = np.full(len(frame), np.nan)
        short_gross = np.full(len(frame), np.nan)
        for position, bucket in enumerate(frame.index):
            target = by_bucket.get(int(bucket) + steps_s)
            if (
                target is None
                or not bool(frame.iloc[position]["decision_fresh"])
                or not bool(frame.iloc[target]["decision_fresh"])
            ):
                continue
            if any(
                int(bucket) + offset not in by_bucket
                for offset in range(FIVE_MINUTES_S, steps_s + FIVE_MINUTES_S, FIVE_MINUTES_S)
            ):
                continue
            path_positions = [
                by_bucket[int(bucket) + offset]
                for offset in range(0, steps_s + FIVE_MINUTES_S, FIVE_MINUTES_S)
            ]
            path = frame.iloc[path_positions]
            if not _bounded_fresh_path(path["decision_fresh"].to_numpy(dtype=bool)):
                continue
            if not bool(path["path_contamination_free"].all()):
                continue
            current = frame.iloc[position]
            future = frame.iloc[target]
            if not all(
                math.isfinite(float(value))
                for value in (
                    current["bid_ticks"],
                    current["ask_ticks"],
                    future["bid_ticks"],
                    future["ask_ticks"],
                    current["close_half_ticks"],
                    future["close_half_ticks"],
                )
            ):
                continue
            mid_label[position] = (
                float(future["close_half_ticks"]) - float(current["close_half_ticks"])
            ) / 2.0
            long_gross[position] = float(future["bid_ticks"]) - float(current["ask_ticks"])
            short_gross[position] = float(current["bid_ticks"]) - float(future["ask_ticks"])
        frame[f"label_{horizon}m"] = mid_label
        frame[f"long_gross_{horizon}m"] = long_gross
        frame[f"short_gross_{horizon}m"] = short_gross
    frame["source_date"] = source_date
    frame["bucket_s"] = frame.index.astype(np.int64)
    return frame.reset_index(drop=True)


def _extract_day(
    root: Path,
    selection: Selection,
    *,
    followup360: bool = False,
    execution_cache: ExecutionCache | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    started = perf_counter()
    raw_path = root / "data" / selection.source_relative_uri
    table, source_rows = _read_selected_table(raw_path, selection.instrument_id)
    ts_ns = (
        pc.cast(table["ts_recv"].combine_chunks(), pa.int64())
        .to_numpy(zero_copy_only=False)
        .astype(np.int64)
    )
    if np.any(np.diff(ts_ns) < 0):
        raise RuntimeError(f"selected ts_recv order regressed on {selection.source_date}")
    action = np.asarray(table["action"].combine_chunks().to_pylist(), dtype="U1")
    side = np.asarray(table["side"].combine_chunks().to_pylist(), dtype="U1")
    price = _numpy_column(table, "price").astype(np.int64)
    size = _numpy_column(table, "size").astype(np.int64)
    flags = _numpy_column(table, "flags").astype(np.uint8)
    bid_px, ask_px, bid_sz, ask_sz = _book_matrices(table)
    five_path = root / (
        "data/derived/research_5m/version=phase1a_mbp10_screening_v1/"
        f"contract={selection.raw_symbol}/source_date={selection.source_date}/part-000.parquet"
    )
    base = _load_five_minute_frame(five_path)
    clean_buckets = set(
        base.index[base["path_contamination_free"]].to_numpy(dtype=np.int64).tolist()
    )
    event_frame, episodes, clean_trade_indices, event_audit = _aggregate_trade_events(
        ts_ns, action, side, price, size, flags, bid_px, ask_px, bid_sz, ask_sz
    )
    if not event_frame.empty:
        event_frame = event_frame.loc[event_frame["bucket_s"].isin(clean_buckets)].copy()
    episodes = [
        episode
        for episode in episodes
        if int(_ceil_div(episode.event_end_ns, FIVE_MINUTES_S * SECOND_NS) * FIVE_MINUTES_S)
        in clean_buckets
    ]
    event_audit["eligible_depleted_trade_price_anchors"] = len(episodes)
    event_ends, event_end_ns, event_valid = _completed_event_state(
        ts_ns, action, flags, bid_px, ask_px
    )
    seconds, sec_bid_px, sec_ask_px, sec_bid_sz, sec_ask_sz = _second_snapshots(
        event_end_ns,
        event_valid,
        bid_px[event_ends],
        ask_px[event_ends],
        bid_sz[event_ends],
        ask_sz[event_ends],
    )
    refill_frame, refill_audit = _refill_records(
        episodes,
        event_end_ns,
        event_valid,
        bid_px[event_ends],
        ask_px[event_ends],
        bid_sz[event_ends],
        ask_sz[event_ends],
    )
    book_frame = _book_records(seconds, sec_bid_px, sec_ask_px, sec_bid_sz, sec_ask_sz)
    if not refill_frame.empty:
        refill_frame = refill_frame.loc[refill_frame["bucket_s"].isin(clean_buckets)].copy()
    if not book_frame.empty:
        book_frame = book_frame.loc[book_frame["bucket_s"].isin(clean_buckets)].copy()
    trade_event_positions = np.searchsorted(event_ends, clean_trade_indices, side="left")
    trade_end_ns = ts_ns[event_ends[trade_event_positions]]
    trade_buckets = (_ceil_div(trade_end_ns, FIVE_MINUTES_S * SECOND_NS) * FIVE_MINUTES_S).astype(
        np.int64
    )
    clean_trade_mask = np.isin(trade_buckets, np.asarray(sorted(clean_buckets), dtype=np.int64))
    trade_buckets = trade_buckets[clean_trade_mask]
    clean_trade_indices = clean_trade_indices[clean_trade_mask]
    micro = _assemble_micro_features(
        event_frame,
        refill_frame,
        book_frame,
        trade_buckets,
        price[clean_trade_indices],
        size[clean_trade_indices],
    )
    execution_audit = {
        "execution_decisions_considered": 0,
        "execution_entry_fills": 0,
        "execution_round_trips": 0,
        "execution_entry_adverse_long": 0,
        "execution_entry_adverse_short": 0,
    }
    memory_audit = {
        "memory_state_decisions": 0,
        "memory_grid_recontact_decisions": 0,
    }
    if followup360:
        memory = _liquidity_memory_features(refill_frame, base)
        if execution_cache is None:
            execution = pd.DataFrame(index=base.index)
            execution["execution_cache_available"] = 0
            execution["exec_entry_fillable"] = 0
            execution["exec_exit_fillable"] = 0
        else:
            execution, execution_audit = _execution_quote_frame(
                base,
                execution_cache,
                clean_buckets,
            )
        micro = pd.concat([micro, memory, execution], axis=1).sort_index()
        memory_audit = {
            "memory_state_decisions": int((memory["memory_level_count"].fillna(0) > 0).sum()),
            "memory_grid_recontact_decisions": int(
                (memory["memory_grid_recontact_side_count"].fillna(0) > 0).sum()
            ),
        }
    result = _price_and_labels(base, micro, selection.source_date)
    elapsed = perf_counter() - started
    audit = {
        "source_date": selection.source_date,
        "raw_symbol": selection.raw_symbol,
        "raw_bytes": raw_path.stat().st_size,
        "source_rows": source_rows,
        "selected_rows": table.num_rows,
        "completed_seconds": len(seconds),
        "decision_rows": len(result),
        "elapsed_seconds": elapsed,
        **event_audit,
        **refill_audit,
        **execution_audit,
        **memory_audit,
    }
    return result, audit


def _safe_spearman(y: np.ndarray, prediction: np.ndarray) -> float:
    if len(y) < 3 or np.nanstd(y) == 0 or np.nanstd(prediction) == 0:
        return math.nan
    return float(spearmanr(y, prediction).statistic)


def _model() -> HistGradientBoostingRegressor:
    return HistGradientBoostingRegressor(
        loss="squared_error",
        learning_rate=0.05,
        max_iter=100,
        max_leaf_nodes=15,
        min_samples_leaf=30,
        l2_regularization=1.0,
        random_state=1729,
    )


def _nonoverlap_signals(
    rows: pd.DataFrame,
    prediction: np.ndarray,
    low: float,
    high: float,
    horizon_minutes: int,
) -> pd.DataFrame:
    candidates = rows[
        [
            "source_date",
            "bucket_s",
            f"long_gross_{horizon_minutes}m",
            f"short_gross_{horizon_minutes}m",
        ]
    ].copy()
    candidates["prediction"] = prediction
    candidates["direction"] = np.where(prediction >= high, 1, np.where(prediction <= low, -1, 0))
    candidates = candidates.loc[candidates["direction"] != 0].sort_values(
        ["source_date", "bucket_s"]
    )
    kept: list[int] = []
    horizon_s = horizon_minutes * 60
    last_by_date: dict[str, int] = {}
    for index, row in candidates.iterrows():
        source_date = str(row["source_date"])
        bucket = int(row["bucket_s"])
        if bucket - last_by_date.get(source_date, -(10**18)) < horizon_s:
            continue
        kept.append(index)
        last_by_date[source_date] = bucket
    selected = candidates.loc[kept].copy()
    selected["gross"] = np.where(
        selected["direction"] > 0,
        selected[f"long_gross_{horizon_minutes}m"],
        selected[f"short_gross_{horizon_minutes}m"],
    )
    selected["net10"] = selected["gross"] - 10.0
    selected["net14"] = selected["gross"] - 14.0
    return selected


def _evaluate_feature_set(
    frame: pd.DataFrame,
    features: Iterable[str],
    horizon: int,
) -> tuple[np.ndarray, np.ndarray, pd.DataFrame, list[dict[str, Any]]]:
    names = list(features)
    valid = frame[f"label_{horizon}m"].notna()
    valid &= frame["history_contamination_free"]
    valid &= np.isfinite(frame[list(PRICE_FEATURES)].to_numpy(dtype=np.float64)).all(axis=1)
    valid &= np.isfinite(frame[list(LIQUIDITY_FEATURES)].to_numpy(dtype=np.float64)).any(axis=1)
    data = frame.loc[valid].copy()
    predictions: list[np.ndarray] = []
    outcomes: list[np.ndarray] = []
    signal_frames: list[pd.DataFrame] = []
    folds: list[dict[str, Any]] = []
    for test_block in (1, 2, 3):
        train = data.loc[data["block"] < test_block]
        test = data.loc[data["block"] == test_block]
        if len(train) < 100 or len(test) < 20:
            continue
        x_train = train[names].replace([np.inf, -np.inf], np.nan).to_numpy(dtype=np.float64)
        x_test = test[names].replace([np.inf, -np.inf], np.nan).to_numpy(dtype=np.float64)
        y_train = train[f"label_{horizon}m"].to_numpy(dtype=np.float64)
        y_test = test[f"label_{horizon}m"].to_numpy(dtype=np.float64)
        model = _model().fit(x_train, y_train)
        train_prediction = model.predict(x_train)
        test_prediction = model.predict(x_test)
        low, high = np.quantile(train_prediction, [0.20, 0.80])
        signals = _nonoverlap_signals(test, test_prediction, float(low), float(high), horizon)
        signals["block"] = test_block
        signal_frames.append(signals)
        predictions.append(test_prediction)
        outcomes.append(y_test)
        folds.append(
            {
                "block": test_block,
                "train_rows": len(train),
                "test_rows": len(test),
                "ic": _finite(_safe_spearman(y_test, test_prediction)),
                "signals": len(signals),
                "net14": _finite(float(signals["net14"].sum())) if len(signals) else None,
            }
        )
    if not predictions:
        return np.array([]), np.array([]), pd.DataFrame(), folds
    return (
        np.concatenate(outcomes),
        np.concatenate(predictions),
        pd.concat(signal_frames, ignore_index=True) if signal_frames else pd.DataFrame(),
        folds,
    )


def _performance(signals: pd.DataFrame) -> dict[str, Any]:
    if signals.empty:
        return {"count": 0}
    net10 = signals["net10"].to_numpy(dtype=np.float64)
    gains = float(net10[net10 > 0].sum())
    losses = float(-net10[net10 < 0].sum())
    return {
        "count": len(signals),
        "distinct_dates": int(signals["source_date"].nunique()),
        "gross_ev": _finite(float(signals["gross"].mean())),
        "net10_ev": _finite(float(net10.mean())),
        "net14_total": _finite(float(signals["net14"].sum())),
        "profit_factor_net10": _finite(gains / losses)
        if losses
        else (None if gains == 0 else "INF"),
        "positive_blocks": int(
            sum(group["net14"].sum() > 0 for _, group in signals.groupby("block"))
        ),
        "worst_block_ev_net14": _finite(
            min(group["net14"].mean() for _, group in signals.groupby("block"))
        ),
        "block_counts": {str(int(block)): len(group) for block, group in signals.groupby("block")},
        "block_net14": {
            str(int(block)): float(group["net14"].sum())
            for block, group in signals.groupby("block")
        },
    }


def _run_models(frame: pd.DataFrame) -> dict[str, Any]:
    results: dict[str, Any] = {}
    for horizon in HORIZONS_MINUTES:
        horizon_result: dict[str, Any] = {}
        for label, features in (
            ("price", PRICE_FEATURES),
            ("liquidity", LIQUIDITY_FEATURES),
            ("combined", (*PRICE_FEATURES, *LIQUIDITY_FEATURES)),
        ):
            y, prediction, signals, folds = _evaluate_feature_set(frame, features, horizon)
            fold_ics = [fold["ic"] for fold in folds if isinstance(fold.get("ic"), (int, float))]
            horizon_result[label] = {
                "oos_rows": len(y),
                "diagnostic_raw_pooled_ic": (
                    _finite(_safe_spearman(y, prediction)) if len(y) else None
                ),
                "mean_fold_ic": _finite(float(np.mean(fold_ics))) if fold_ics else None,
                "signals": _performance(signals),
                "folds": folds,
            }
        price_ic = horizon_result["price"]["diagnostic_raw_pooled_ic"]
        combined_ic = horizon_result["combined"]["diagnostic_raw_pooled_ic"]
        horizon_result["diagnostic_raw_pooled_combined_minus_price_ic"] = (
            None if price_ic is None or combined_ic is None else combined_ic - price_ic
        )
        price_folds = {fold["block"]: fold["ic"] for fold in horizon_result["price"]["folds"]}
        combined_folds = {fold["block"]: fold["ic"] for fold in horizon_result["combined"]["folds"]}
        paired_deltas = {
            str(block): combined_folds[block] - price_folds[block]
            for block in sorted(set(price_folds) & set(combined_folds))
            if isinstance(price_folds[block], (int, float))
            and isinstance(combined_folds[block], (int, float))
        }
        horizon_result["combined_minus_price_fold_ic"] = paired_deltas
        horizon_result["combined_minus_price_mean_fold_ic"] = (
            _finite(float(np.mean(list(paired_deltas.values())))) if paired_deltas else None
        )
        results[f"{horizon}m"] = horizon_result
    return results


def _followup_nonoverlap_signals(
    rows: pd.DataFrame,
    prediction: np.ndarray,
    low: float,
    high: float,
) -> tuple[pd.DataFrame, dict[str, int]]:
    required = [
        "source_date",
        "block",
        "bucket_s",
        "price_vol_30m",
        "execution_cache_available",
        "exec_entry_fillable",
        "exec_exit_fillable",
        "entry_fill_ns",
        "exit_fill_ns",
        "long_exec_gross_360m",
        "short_exec_gross_360m",
    ]
    missing = set(required) - set(rows.columns)
    if missing:
        raise RuntimeError(f"follow-up execution fields missing: {sorted(missing)}")
    candidates = rows[required].copy()
    candidates["prediction"] = prediction
    candidates["direction"] = np.where(
        prediction >= high,
        1,
        np.where(prediction <= low, -1, 0),
    )
    candidates = candidates.loc[candidates["direction"] != 0].sort_values(
        ["source_date", "bucket_s"]
    )
    audit = {
        "candidates": len(candidates),
        "cache_unavailable": 0,
        "entry_no_fill": 0,
        "exit_censored": 0,
        "overlap_skipped": 0,
        "completed": 0,
    }
    kept: list[int] = []
    last_exit_by_date: dict[str, int] = {}
    for index, row in candidates.iterrows():
        source_date = str(row["source_date"])
        if float(row["execution_cache_available"]) != 1.0:
            audit["cache_unavailable"] += 1
            continue
        if float(row["exec_entry_fillable"]) != 1.0 or not math.isfinite(
            float(row["entry_fill_ns"])
        ):
            audit["entry_no_fill"] += 1
            continue
        entry_ns = int(row["entry_fill_ns"])
        if entry_ns <= last_exit_by_date.get(source_date, -(10**30)):
            audit["overlap_skipped"] += 1
            continue
        if float(row["exec_exit_fillable"]) != 1.0 or not math.isfinite(float(row["exit_fill_ns"])):
            audit["exit_censored"] += 1
            last_exit_by_date[source_date] = 10**30
            continue
        exit_ns = int(row["exit_fill_ns"])
        if exit_ns <= entry_ns:
            raise RuntimeError("follow-up execution interval is not positive")
        gross_column = (
            "long_exec_gross_360m" if int(row["direction"]) > 0 else "short_exec_gross_360m"
        )
        if not math.isfinite(float(row[gross_column])):
            raise RuntimeError("completed execution interval lacks gross PnL")
        kept.append(index)
        last_exit_by_date[source_date] = exit_ns
        audit["completed"] += 1
    selected = candidates.loc[kept].copy()
    if selected.empty:
        selected["gross"] = pd.Series(dtype=np.float64)
        selected["net10"] = pd.Series(dtype=np.float64)
        selected["net14"] = pd.Series(dtype=np.float64)
        return selected, audit
    selected["gross"] = np.where(
        selected["direction"] > 0,
        selected["long_exec_gross_360m"],
        selected["short_exec_gross_360m"],
    )
    selected["net10"] = selected["gross"] - 10.0
    selected["net14"] = selected["gross"] - 14.0
    intervals = selected.sort_values(["source_date", "entry_fill_ns"])
    for _source_date, group in intervals.groupby("source_date", sort=False):
        starts = group["entry_fill_ns"].to_numpy(dtype=np.int64)
        ends = group["exit_fill_ns"].to_numpy(dtype=np.int64)
        if len(starts) > 1 and np.any(starts[1:] < ends[:-1]):
            raise RuntimeError("follow-up execution intervals overlap")
    return selected, audit


def _followup_daily_performance(
    signals: pd.DataFrame,
    score_dates_by_block: dict[int, list[str]],
) -> tuple[dict[str, Any], pd.DataFrame]:
    rows = [
        {"source_date": source_date, "block": block}
        for block in (1, 2, 3, 4)
        for source_date in score_dates_by_block[block]
    ]
    daily = pd.DataFrame.from_records(rows).set_index("source_date")
    daily["count"] = 0
    daily["p10"] = 0.0
    daily["p14"] = 0.0
    if not signals.empty:
        grouped = signals.groupby("source_date", sort=True).agg(
            count=("net10", "size"),
            p10=("net10", "sum"),
            p14=("net14", "sum"),
        )
        unknown = set(grouped.index) - set(daily.index)
        if unknown:
            raise RuntimeError(f"signals contain excluded scoring dates: {sorted(unknown)}")
        daily.loc[grouped.index, ["count", "p10", "p14"]] = grouped[["count", "p10", "p14"]]
    count = int(daily["count"].sum())
    gains = float(signals.loc[signals["net10"] > 0, "net10"].sum()) if count else 0.0
    losses = float(-signals.loc[signals["net10"] < 0, "net10"].sum()) if count else 0.0
    blocks: dict[str, dict[str, Any]] = {}
    for block in (1, 2, 3, 4):
        block_daily = daily.loc[daily["block"] == block]
        block_count = int(block_daily["count"].sum())
        block_net10 = float(block_daily["p10"].sum())
        block_net14 = float(block_daily["p14"].sum())
        blocks[str(block)] = {
            "scoring_dates": len(block_daily),
            "active_dates": int((block_daily["count"] > 0).sum()),
            "count": block_count,
            "net10_ev": _finite(block_net10 / block_count) if block_count else None,
            "net14": _finite(block_net14),
            "net14_ev": _finite(block_net14 / block_count) if block_count else None,
        }
    block_evs = [
        value["net14_ev"]
        for value in blocks.values()
        if isinstance(value["net14_ev"], (int, float))
    ]
    summary = {
        "count": count,
        "active_scoring_dates": int((daily["count"] > 0).sum()),
        "gross_ev": _finite(float(signals["gross"].mean())) if count else None,
        "net10_ev": _finite(float(daily["p10"].sum()) / count) if count else None,
        "net14_total": _finite(float(daily["p14"].sum())),
        "profit_factor_net10": (
            _finite(gains / losses) if losses else (None if gains == 0 else "INF")
        ),
        "positive_blocks": int(sum(value["net14"] > 0 for value in blocks.values())),
        "worst_block_ev_net14": _finite(min(block_evs)) if block_evs else None,
        "blocks": blocks,
    }
    return summary, daily


def _followup_diagnostics(signals: pd.DataFrame) -> dict[str, Any]:
    if signals.empty:
        return {"utc_sessions": {}, "volatility_tertiles": {}}

    def summarize(group: pd.DataFrame) -> dict[str, Any]:
        return {
            "count": len(group),
            "gross_ev": _finite(float(group["gross"].mean())),
            "net10_ev": _finite(float(group["net10"].mean())),
            "net14": _finite(float(group["net14"].sum())),
        }

    copy = signals.copy()
    hours = (copy["bucket_s"].to_numpy(dtype=np.int64) % 86_400) // 3_600
    session = np.select(
        [hours < 6, hours < 12, hours < 18],
        ["UTC00_05", "UTC06_11", "UTC12_17"],
        default="UTC18_23",
    )
    copy["session"] = session
    sessions = {str(name): summarize(group) for name, group in copy.groupby("session", sort=True)}
    finite_vol = copy.loc[np.isfinite(copy["price_vol_30m"])].copy()
    vol_groups: dict[str, Any] = {}
    if len(finite_vol) >= 3:
        first, second = np.quantile(
            finite_vol["price_vol_30m"].to_numpy(dtype=np.float64),
            [1 / 3, 2 / 3],
        )
        finite_vol["volatility_tertile"] = np.where(
            finite_vol["price_vol_30m"] <= first,
            "LOW",
            np.where(finite_vol["price_vol_30m"] <= second, "MID", "HIGH"),
        )
        vol_groups = {
            str(name): summarize(group)
            for name, group in finite_vol.groupby("volatility_tertile", sort=True)
        }
        vol_groups["cutoffs"] = {"one_third": float(first), "two_thirds": float(second)}
    return {"utc_sessions": sessions, "volatility_tertiles": vol_groups}


def _followup_bootstrap(
    memory_daily: pd.DataFrame,
    baseline_daily: pd.DataFrame,
) -> dict[str, Any]:
    if not memory_daily.index.equals(baseline_daily.index):
        raise RuntimeError("follow-up daily comparison is not paired")
    blocks = memory_daily["block"].to_numpy(dtype=np.int64)
    count = memory_daily["count"].to_numpy(dtype=np.int64)
    p10 = memory_daily["p10"].to_numpy(dtype=np.float64)
    delta14 = memory_daily["p14"].to_numpy(dtype=np.float64) - baseline_daily["p14"].to_numpy(
        dtype=np.float64
    )
    indexes = {block: np.flatnonzero(blocks == block) for block in (1, 2, 3, 4)}
    seed = int(FOLLOWUP_PLAN_SHA256[:16], 16) % (2**32)
    rng = np.random.default_rng(seed)
    ev_values = np.full(10_000, np.nan)
    delta_values = np.full(10_000, np.nan)
    for iteration in range(10_000):
        sampled = np.concatenate(
            [
                rng.choice(indexes[block], size=len(indexes[block]), replace=True)
                for block in (1, 2, 3, 4)
            ]
        )
        denominator = int(count[sampled].sum())
        if denominator > 0:
            ev_values[iteration] = float(p10[sampled].sum()) / denominator
        delta_values[iteration] = float(delta14[sampled].mean())
    finite_ev = ev_values[np.isfinite(ev_values)]
    finite_delta = delta_values[np.isfinite(delta_values)]
    if len(finite_ev) != 10_000 or len(finite_delta) != 10_000:
        raise RuntimeError("follow-up bootstrap produced a zero denominator")
    method = str(FOLLOWUP_PLAN["bootstrap"]["quantile_method"])
    return {
        "iterations": 10_000,
        "seed": seed,
        "quantile_method": method,
        "memory_net10_ev_lcb_95": float(np.quantile(finite_ev, 0.05, method=method)),
        "memory_net10_ev_ucb_90": float(np.quantile(finite_ev, 0.90, method=method)),
        "daily_net14_increment_lcb_95": float(np.quantile(finite_delta, 0.05, method=method)),
        "daily_net14_increment_ucb_90": float(np.quantile(finite_delta, 0.90, method=method)),
    }


def _run_followup360(
    frame: pd.DataFrame,
    score_dates_by_block: dict[int, list[str]],
) -> dict[str, Any]:
    if "long_exec_gross_360m" not in frame or "short_exec_gross_360m" not in frame:
        raise RuntimeError("follow-up tried to use legacy bar gross columns")
    feature_valid = frame["history_contamination_free"].copy()
    feature_valid &= np.isfinite(frame[list(PRICE_FEATURES)].to_numpy(dtype=np.float64)).all(axis=1)
    feature_valid &= np.isfinite(frame[list(LIQUIDITY_FEATURES)].to_numpy(dtype=np.float64)).any(
        axis=1
    )
    feature_valid &= (
        frame["bucket_s"].to_numpy(dtype=np.int64) % 86_400
    ) <= LATEST_FOLLOWUP_DECISION_SECOND_UTC
    data = frame.loc[feature_valid].copy()
    baseline_features = list(LIQUIDITY_FEATURES)
    memory_features = [*LIQUIDITY_FEATURES, *MEMORY_FEATURES]
    fold_rows: list[dict[str, Any]] = []
    signals_by_model: dict[str, list[pd.DataFrame]] = {"baseline": [], "memory": []}
    audits_by_model: dict[str, list[dict[str, Any]]] = {"baseline": [], "memory": []}
    for test_block in (1, 2, 3, 4):
        train = data.loc[(data["block"] < test_block) & data["label_360m"].notna()]
        test = data.loc[(data["block"] == test_block) & data["score_eligible"]]
        ic_mask = test["label_360m"].notna().to_numpy(dtype=bool)
        expected_dates = set(score_dates_by_block[test_block])
        if set(test["source_date"]) - expected_dates:
            raise RuntimeError("midpoint48 date leaked into follow-up scoring rows")
        if len(train) < 100 or len(test) < 20 or int(ic_mask.sum()) < 20:
            raise RuntimeError(f"follow-up fold {test_block} lacks model support")
        y_train = train["label_360m"].to_numpy(dtype=np.float64)
        y_test_ic = test.loc[ic_mask, "label_360m"].to_numpy(dtype=np.float64)
        fold: dict[str, Any] = {
            "block": test_block,
            "train_rows": len(train),
            "execution_test_rows": len(test),
            "ic_test_rows": int(ic_mask.sum()),
            "scoring_dates": len(expected_dates),
        }
        for label, names in (
            ("baseline", baseline_features),
            ("memory", memory_features),
        ):
            x_train = train[names].replace([np.inf, -np.inf], np.nan).to_numpy(dtype=np.float64)
            x_test = test[names].replace([np.inf, -np.inf], np.nan).to_numpy(dtype=np.float64)
            model = _model().fit(x_train, y_train)
            train_prediction = model.predict(x_train)
            test_prediction = model.predict(x_test)
            low, high = np.quantile(train_prediction, [0.20, 0.80])
            signals, signal_audit = _followup_nonoverlap_signals(
                test,
                test_prediction,
                float(low),
                float(high),
            )
            signals_by_model[label].append(signals)
            signal_audit["block"] = test_block
            audits_by_model[label].append(signal_audit)
            fold[f"{label}_ic"] = _finite(_safe_spearman(y_test_ic, test_prediction[ic_mask]))
            fold[f"{label}_signals"] = len(signals)
            fold[f"{label}_threshold_low"] = float(low)
            fold[f"{label}_threshold_high"] = float(high)
        baseline_ic = fold["baseline_ic"]
        memory_ic = fold["memory_ic"]
        fold["memory_minus_baseline_ic"] = (
            None if baseline_ic is None or memory_ic is None else float(memory_ic - baseline_ic)
        )
        fold_rows.append(fold)

    signal_frames = {
        label: pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        for label, parts in signals_by_model.items()
    }
    baseline_performance, baseline_daily = _followup_daily_performance(
        signal_frames["baseline"], score_dates_by_block
    )
    memory_performance, memory_daily = _followup_daily_performance(
        signal_frames["memory"], score_dates_by_block
    )
    baseline_ics = [row["baseline_ic"] for row in fold_rows]
    memory_ics = [row["memory_ic"] for row in fold_rows]
    deltas = [row["memory_minus_baseline_ic"] for row in fold_rows]
    if not all(isinstance(value, (int, float)) for value in (*baseline_ics, *memory_ics, *deltas)):
        raise RuntimeError("follow-up IC folds are incomplete")
    daily_delta = memory_daily["p14"] - baseline_daily["p14"]
    bootstrap = _followup_bootstrap(memory_daily, baseline_daily)
    audit_totals: dict[str, dict[str, int]] = {}
    for label, audits in audits_by_model.items():
        totals = {
            key: int(sum(int(audit.get(key, 0)) for audit in audits))
            for key in (
                "candidates",
                "cache_unavailable",
                "entry_no_fill",
                "exit_censored",
                "overlap_skipped",
                "completed",
            )
        }
        audit_totals[label] = totals
    result = {
        "folds": fold_rows,
        "baseline": {
            "mean_fold_ic": float(np.mean(baseline_ics)),
            "performance": baseline_performance,
            "signal_audit": audit_totals["baseline"],
        },
        "memory": {
            "mean_fold_ic": float(np.mean(memory_ics)),
            "performance": memory_performance,
            "signal_audit": audit_totals["memory"],
            "diagnostics_only": _followup_diagnostics(signal_frames["memory"]),
        },
        "increment": {
            "fold_ic_delta": {
                str(row["block"]): row["memory_minus_baseline_ic"] for row in fold_rows
            },
            "mean_fold_ic_delta": float(np.mean(deltas)),
            "positive_ic_delta_folds": int(sum(float(value) > 0 for value in deltas)),
            "b4_ic_delta": float(deltas[-1]),
            "daily_net14_total": float(daily_delta.sum()),
            "daily_net14_mean": float(daily_delta.mean()),
            "b4_daily_net14_total": float(daily_delta.loc[memory_daily["block"] == 4].sum()),
        },
        "bootstrap": bootstrap,
    }
    result["decision"] = _followup_go_stop(result)
    return result


def _followup_go_stop(result: dict[str, Any]) -> dict[str, Any]:
    reasons: list[str] = []
    performance = result["memory"]["performance"]
    blocks = performance["blocks"]
    audit = result["memory"]["signal_audit"]
    baseline_audit = result["baseline"]["signal_audit"]
    if performance["count"] < 192:
        reasons.append("FILLS_LT_192")
    if performance["active_scoring_dates"] < 96:
        reasons.append("ACTIVE_DATES_LT_96")
    if any(blocks[str(block)]["count"] < 48 for block in (1, 2, 3, 4)):
        reasons.append("BLOCK_FILLS_LT_48")
    if any(blocks[str(block)]["active_dates"] < 24 for block in (1, 2, 3, 4)):
        reasons.append("BLOCK_ACTIVE_DATES_LT_24")
    if audit["cache_unavailable"]:
        reasons.append("MEMORY_EXECUTION_CACHE_INCOMPLETE")
    if baseline_audit["cache_unavailable"]:
        reasons.append("BASELINE_EXECUTION_CACHE_INCOMPLETE")
    if audit["exit_censored"]:
        reasons.append("MEMORY_EXIT_CENSOR_PRESENT")
    if baseline_audit["exit_censored"]:
        reasons.append("BASELINE_EXIT_CENSOR_PRESENT")
    if not (isinstance(performance["net10_ev"], (int, float)) and performance["net10_ev"] > 0):
        reasons.append("NET10_EV_NOT_POSITIVE")
    pf = performance["profit_factor_net10"]
    if pf != "INF" and not (isinstance(pf, (int, float)) and pf >= 1.05):
        reasons.append("PF_NET10_LT_1_05")
    if not (
        isinstance(performance["net14_total"], (int, float)) and performance["net14_total"] > 0
    ):
        reasons.append("NET14_NOT_POSITIVE")
    if performance["positive_blocks"] < 3:
        reasons.append("POSITIVE_BLOCKS_LT_3")
    if not (
        isinstance(performance["worst_block_ev_net14"], (int, float))
        and performance["worst_block_ev_net14"] >= -2
    ):
        reasons.append("WORST_BLOCK_EV_LT_MINUS_2")
    b4 = blocks["4"]
    if not (isinstance(b4["net10_ev"], (int, float)) and b4["net10_ev"] > 0):
        reasons.append("B4_NET10_EV_NOT_POSITIVE")
    if not (isinstance(b4["net14"], (int, float)) and b4["net14"] > 0):
        reasons.append("B4_NET14_NOT_POSITIVE")
    if not (result["memory"]["mean_fold_ic"] > 0):
        reasons.append("MEMORY_MEAN_FOLD_IC_NOT_POSITIVE")
    increment = result["increment"]
    if not (increment["mean_fold_ic_delta"] > 0):
        reasons.append("MEAN_IC_INCREMENT_NOT_POSITIVE")
    if increment["positive_ic_delta_folds"] < 3:
        reasons.append("POSITIVE_IC_DELTA_FOLDS_LT_3")
    if not (increment["b4_ic_delta"] > 0):
        reasons.append("B4_IC_DELTA_NOT_POSITIVE")
    bootstrap = result["bootstrap"]
    if not (bootstrap["memory_net10_ev_lcb_95"] > 0):
        reasons.append("BOOTSTRAP_NET10_EV_LCB_NOT_POSITIVE")
    if not (bootstrap["daily_net14_increment_lcb_95"] > 0):
        reasons.append("BOOTSTRAP_INCREMENT_LCB_NOT_POSITIVE")
    return {
        "decision": "GO" if not reasons else "STOP",
        "reasons": reasons,
    }


def _go_stop(models: dict[str, Any]) -> dict[str, Any]:
    candidates: list[str] = []
    details: dict[str, list[str]] = {}
    for horizon in (60, 120, 360):
        key = f"{horizon}m"
        result = models[key]
        signals = result["combined"]["signals"]
        reasons: list[str] = []
        if signals.get("count", 0) < 48:
            reasons.append("SIGNALS_LT_48")
        if signals.get("distinct_dates", 0) < 24:
            reasons.append("DATES_LT_24")
        block_counts = signals.get("block_counts", {})
        if any(int(block_counts.get(str(block), 0)) < 8 for block in (1, 2, 3)):
            reasons.append("BLOCK_SIGNALS_LT_8")
        if not (isinstance(signals.get("net10_ev"), (int, float)) and signals["net10_ev"] > 0):
            reasons.append("NET10_EV_NOT_POSITIVE")
        pf = signals.get("profit_factor_net10")
        if pf != "INF" and not (isinstance(pf, (int, float)) and pf >= 1.05):
            reasons.append("PF_NET10_LT_1_05")
        if not (
            isinstance(signals.get("net14_total"), (int, float)) and signals["net14_total"] > 0
        ):
            reasons.append("NET14_NOT_POSITIVE")
        if int(signals.get("positive_blocks", 0)) < 2:
            reasons.append("POSITIVE_BLOCKS_LT_2")
        if not (
            isinstance(signals.get("worst_block_ev_net14"), (int, float))
            and signals["worst_block_ev_net14"] >= -2
        ):
            reasons.append("WORST_BLOCK_EV_LT_MINUS_2")
        lift = result.get("combined_minus_price_mean_fold_ic")
        if not (isinstance(lift, (int, float)) and lift > 0):
            reasons.append("NO_INCREMENT_OVER_PRICE")
        combined_mean_ic = result["combined"].get("mean_fold_ic")
        if not (isinstance(combined_mean_ic, (int, float)) and combined_mean_ic > 0):
            reasons.append("COMBINED_MEAN_FOLD_IC_NOT_POSITIVE")
        positive_ic_lift_folds = sum(
            value > 0 for value in result.get("combined_minus_price_fold_ic", {}).values()
        )
        if positive_ic_lift_folds < 2:
            reasons.append("IC_LIFT_POSITIVE_FOLDS_LT_2")
        if len(result.get("combined_minus_price_fold_ic", {})) != 3:
            reasons.append("PAIRED_IC_FOLDS_NE_3")
        details[key] = reasons
        if not reasons:
            candidates.append(key)
    return {
        "decision": "GO" if candidates else "STOP",
        "passing_horizons": candidates,
        "reasons": details,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sample",
        choices=("benchmark14", "midpoint48", "full_discovery"),
        default="benchmark14",
    )
    parser.add_argument("--time-cap-seconds", type=int, default=14_400)
    parser.add_argument("--models", action="store_true")
    parser.add_argument("--followup360", action="store_true")
    parser.add_argument("--build-missing-execution-cache", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    started = perf_counter()
    root = _project_root()
    discovery, selections = _load_discovery_selections(root)
    available = [source_date for source_date in discovery if source_date in selections]
    if len(discovery) != 495 or discovery != sorted(set(discovery)):
        raise RuntimeError("Discovery date partition drifted")
    expected_missing_selections = {
        "2022-01-02",
        "2022-04-15",
        "2022-04-17",
        "2022-12-26",
        "2023-01-02",
    }
    if set(discovery) - set(available) != expected_missing_selections:
        raise RuntimeError("visible selection date set drifted")
    midpoint48, _midpoint_blocks = _midpoint_sample(discovery, 12)
    midpoint48_sha256 = hashlib.sha256(
        json.dumps(
            sorted(midpoint48),
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    ).hexdigest()
    if midpoint48_sha256 != FOLLOWUP_PLAN["midpoint48_score_exclusion_sha256"]:
        raise RuntimeError("midpoint48 exclusion identity drifted")
    seen48 = set(midpoint48)
    cache_requests: dict[str, ExecutionCacheRequest] = {}
    execution_caches: dict[str, ExecutionCache] = {}
    missing_execution_caches: list[str] = []
    if args.followup360 or args.build_missing_execution_cache:
        cache_requests = _execution_cache_requests(root, discovery, selections)
        execution_caches, missing_execution_caches = _load_execution_caches(
            root,
            cache_requests,
            selections,
            verify_content=(
                args.sample == "full_discovery" and not args.build_missing_execution_cache
            ),
        )
        if args.build_missing_execution_cache and missing_execution_caches:
            _log(f"building {len(missing_execution_caches)} missing immutable execution caches")
            _build_missing_execution_caches(
                root,
                cache_requests,
                missing_execution_caches,
            )
        if args.build_missing_execution_cache:
            execution_caches, missing_execution_caches = _load_execution_caches(
                root,
                cache_requests,
                selections,
                verify_content=(args.sample == "full_discovery"),
            )
        if args.sample == "full_discovery" and missing_execution_caches:
            raise RuntimeError(
                "full follow-up requires complete execution caches; missing "
                f"{missing_execution_caches}"
            )
    score_dates_by_block: dict[int, list[str]] | None = None
    if args.sample == "benchmark14":
        chosen = [source_date for source_date in BENCHMARK_DATES if source_date in selections]
        if len(chosen) != len(BENCHMARK_DATES):
            raise RuntimeError("benchmark date selection is incomplete")
        quartiles = np.array_split(np.asarray(available), 4)
        block_by_date = {
            str(source_date): block
            for block, dates in enumerate(quartiles)
            for source_date in dates.tolist()
        }
    elif args.sample == "midpoint48":
        chosen, block_by_date = _midpoint_sample(discovery, 12)
        missing = [source_date for source_date in chosen if source_date not in selections]
        if missing:
            raise RuntimeError(f"midpoint sample lacks frozen selection: {missing}")
    else:
        manifest_blocks = [list(block) for block in np.array_split(np.asarray(discovery), 5)]
        if [len(block) for block in manifest_blocks] != [99, 99, 99, 99, 99]:
            raise RuntimeError("full Discovery blocks are not equal")
        selected_blocks = [
            [str(source_date) for source_date in block if str(source_date) in selections]
            for block in manifest_blocks
        ]
        if [len(block) for block in selected_blocks] != [96, 99, 99, 97, 99]:
            raise RuntimeError("full Discovery selected-block counts drifted")
        block_by_date = {
            source_date: block
            for block, dates in enumerate(selected_blocks)
            for source_date in dates
        }
        chosen = [source_date for source_date in available]
        score_dates_by_block = {
            block: [
                source_date for source_date in selected_blocks[block] if source_date not in seen48
            ]
            for block in (1, 2, 3, 4)
        }
        if [len(score_dates_by_block[block]) for block in (1, 2, 3, 4)] != [90, 89, 88, 89]:
            raise RuntimeError("full Discovery clean scoring-date counts drifted")
        if args.models and not args.followup360:
            raise RuntimeError("full Discovery models require --followup360")
    frames: list[pd.DataFrame] = []
    audits: list[dict[str, Any]] = []
    for position, source_date in enumerate(chosen, start=1):
        if perf_counter() - started >= args.time_cap_seconds:
            raise TimeoutError("preflight time cap reached during extraction")
        frame, audit = _extract_day(
            root,
            selections[source_date],
            followup360=args.followup360,
            execution_cache=execution_caches.get(source_date),
        )
        frame["block"] = block_by_date[source_date]
        frame["score_eligible"] = bool(
            args.sample != "full_discovery"
            or (block_by_date[source_date] > 0 and source_date not in seen48)
        )
        frames.append(frame)
        audits.append(audit)
        _log(
            f"[{position}/{len(chosen)}] {source_date} rows={audit['selected_rows']} "
            f"anchors={audit['depleted_trade_price_anchors']} elapsed={audit['elapsed_seconds']:.2f}s"
        )
    combined = pd.concat(frames, ignore_index=True).sort_values(["source_date", "bucket_s"])
    support = {
        "dates": len(chosen),
        "decision_rows": len(combined),
        "fresh_decisions": int(combined["decision_fresh"].sum()),
        "trade_rows": int(sum(audit["trade_rows"] for audit in audits)),
        "trade_events": int(sum(audit["trade_events"] for audit in audits)),
        "trade_price_anchors": int(sum(audit["trade_price_anchors"] for audit in audits)),
        "depleted_trade_price_anchors": int(
            sum(audit["depleted_trade_price_anchors"] for audit in audits)
        ),
        "eligible_depleted_trade_price_anchors": int(
            sum(audit["eligible_depleted_trade_price_anchors"] for audit in audits)
        ),
        "refill_observed": int(sum(audit["refill_observed"] for audit in audits)),
        "refill_censored": int(sum(audit["refill_censored"] for audit in audits)),
    }
    if args.followup360:
        support.update(
            {
                "memory_state_decisions": int(
                    sum(audit["memory_state_decisions"] for audit in audits)
                ),
                "memory_grid_recontact_decisions": int(
                    sum(audit["memory_grid_recontact_decisions"] for audit in audits)
                ),
                "execution_decisions_considered": int(
                    sum(audit["execution_decisions_considered"] for audit in audits)
                ),
                "execution_entry_fills": int(
                    sum(audit["execution_entry_fills"] for audit in audits)
                ),
                "execution_round_trips": int(
                    sum(audit["execution_round_trips"] for audit in audits)
                ),
                "execution_entry_adverse_long": int(
                    sum(audit["execution_entry_adverse_long"] for audit in audits)
                ),
                "execution_entry_adverse_short": int(
                    sum(audit["execution_entry_adverse_short"] for audit in audits)
                ),
            }
        )
    if args.models and args.sample == "full_discovery":
        assert score_dates_by_block is not None
        models = _run_followup360(combined, score_dates_by_block)
    elif args.models:
        models = _run_models(combined)
    else:
        models = None
    if models is not None and args.sample == "full_discovery":
        decision = models["decision"]
    elif models is not None and args.sample == "midpoint48":
        decision = _go_stop(models)
    else:
        decision = {
            "decision": (
                "EXPAND_TO_48"
                if support["eligible_depleted_trade_price_anchors"] >= 1_000
                else "STOP"
            ),
            "reason": "SUPPORT_PREFLIGHT_ONLY",
        }
    output = {
        "schema": (
            "flow_liquidity_transition_discovery_followup.v1"
            if args.sample == "full_discovery"
            else "flow_acceptance_markout_preflight.v1"
        ),
        "code_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "followup_plan": FOLLOWUP_PLAN if args.followup360 else None,
        "followup_plan_sha256": FOLLOWUP_PLAN_SHA256 if args.followup360 else None,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "sample": args.sample,
        "source_dates": chosen,
        "visible_partition": {
            "name": "DISCOVERY",
            "available_selected_dates": len(available),
            "first": available[0],
            "last": available[-1],
            "later_partitions_opened": False,
            "midpoint48_excluded_from_scoring": (
                len(seen48) if args.sample == "full_discovery" else 0
            ),
        },
        "execution_cache": (
            {
                "covered_dates": len(execution_caches),
                "missing_dates": missing_execution_caches,
                "bytes": int(sum(item.byte_size for item in execution_caches.values())),
                "rows": int(sum(item.cached_quote_count for item in execution_caches.values())),
            }
            if args.followup360
            else None
        ),
        "support": support,
        "throughput": {
            "elapsed_seconds": perf_counter() - started,
            "raw_bytes": int(sum(audit["raw_bytes"] for audit in audits)),
            "source_rows": int(sum(audit["source_rows"] for audit in audits)),
            "selected_rows": int(sum(audit["selected_rows"] for audit in audits)),
        },
        "models": models,
        "decision": decision,
        "limitations": (
            [
                "OUTCOME_INFORMED_DISCOVERY_SCREENING_ONLY",
                "MIDPOINT48_EXCLUDED_FROM_ALL_NEW_SCORING",
                "SOURCE_DATE_PROXY_WITHOUT_DEFINITION_STATUS",
                "BBO_TOP_OF_BOOK_ONE_LOT_WITHOUT_BROKER_FILL_PROOF",
                "GRID_RECONTACT_IS_NOT_EVENT_LEVEL_REVISIT",
                "MAX_TWO_CONSECUTIVE_STALE_5M_BUCKETS",
                "NO_LATER_WF_OR_HOLDOUT_ARTIFACT_OPENED",
                "NO_PROMOTION_AUTHORITY",
            ]
            if args.followup360
            else [
                "SCREENING_PREFLIGHT_ONLY",
                "SOURCE_DATE_PROXY_WITHOUT_DEFINITION_STATUS",
                "NO_LATER_WF_OR_HOLDOUT_OPENED",
                "NO_EXECUTABLE_EVENT_REPLAY",
                "SAME_CLOSE_BBO_WITHOUT_ROUTING_DELAY",
                "MAX_TWO_CONSECUTIVE_STALE_5M_BUCKETS",
                "NO_PROMOTION_AUTHORITY",
            ]
        ),
    }
    print(
        json.dumps(
            output, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
