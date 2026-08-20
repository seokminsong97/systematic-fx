"""Bounded L10 order-topology transition preflight.

This pilot is intentionally narrow.  It asks whether order counts, average
displayed order size, and near-book concentration add information beyond the
already-tested price and aggregate-liquidity state.  It uses only the frozen
14-date Discovery benchmark, one 360-minute horizon, one model specification,
and the existing routed-BBO execution contract.
"""

from __future__ import annotations

import hashlib
import json
import stat as stat_module
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

_SCRIPT_DIRECTORY = str(Path(__file__).resolve().parent)
if _SCRIPT_DIRECTORY not in sys.path:
    sys.path.insert(0, _SCRIPT_DIRECTORY)

import flow_acceptance_markout_preflight as base

TIME_CAP_SECONDS = 600
HORIZON_MINUTES = 360
SIGNAL_QUANTILES = (0.10, 0.90)
LATEST_DECISION_SECOND_UTC = 15 * 3_600 + 55 * 60
MIN_TRAIN_ROWS = 100
MIN_EXECUTION_TEST_ROWS = 20
MIN_IC_TEST_ROWS = 20
EXPECTED_BASE_CODE_SHA256 = "ddcb86fab1bd855f5b5560433903392c1b8eb848c67d941832f6d9df31559862"

CORE_FEATURES = (
    *base.PRICE_FEATURES,
    "flow_signed",
    "flow_abs",
    "flow_buy",
    "flow_sell",
    "trade_event_count",
    "impact_aligned_ticks",
    "impact_buy_ticks",
    "impact_sell_ticks",
    "zero_impact_flow_share",
    "book_center_offset_mean",
    "book_center_offset_last",
    "book_center_offset_change",
    "book_depth_total_mean",
    "book_depth_imbalance_mean",
    "book_depth_imbalance_last",
    "spread_ticks_mean",
)

TOPOLOGY_STATE_FIELDS = (
    "order_count_imbalance",
    "mean_order_size_imbalance",
    "near_size_share_imbalance",
    "near_count_share_imbalance",
    "size_centroid_skew",
    "count_centroid_skew",
    "total_order_count_log",
    "mean_order_size_log",
)

TOPOLOGY_FEATURES = tuple(
    f"topology_{field}_{suffix}"
    for field in TOPOLOGY_STATE_FIELDS
    for suffix in ("mean", "last", "change")
)

TOPOLOGY_RAW_COLUMNS = [
    "ts_recv",
    "instrument_id",
    "action",
    "flags",
]
for _level in range(10):
    _suffix = f"{_level:02d}"
    TOPOLOGY_RAW_COLUMNS.extend(
        [
            f"bid_px_{_suffix}",
            f"ask_px_{_suffix}",
            f"bid_sz_{_suffix}",
            f"ask_sz_{_suffix}",
            f"bid_ct_{_suffix}",
            f"ask_ct_{_suffix}",
        ]
    )

PILOT_PLAN = {
    "name": "L10_ORDER_TOPOLOGY_TRANSITION_360M_V1",
    "purpose": "ORDER_COUNT_SIZE_AND_CONCENTRATION_INCREMENT_OVER_AGGREGATE_LIQUIDITY",
    "sample_dates": list(base.BENCHMARK_DATES),
    "horizon_minutes": HORIZON_MINUTES,
    "feature_sets": {
        "baseline": list(CORE_FEATURES),
        "topology": [*CORE_FEATURES, *TOPOLOGY_FEATURES],
    },
    "folds": [
        {"train_date_count": 5, "test_date_positions": [5, 6, 7]},
        {"train_date_count": 8, "test_date_positions": [8, 9, 10]},
        {"train_date_count": 11, "test_date_positions": [11, 12, 13]},
    ],
    "signal_quantiles": list(SIGNAL_QUANTILES),
    "minimum_model_rows": {
        "train": MIN_TRAIN_ROWS,
        "execution_test": MIN_EXECUTION_TEST_ROWS,
        "ic_test": MIN_IC_TEST_ROWS,
    },
    "execution": {
        "routing_delay_ns": base.ROUTING_DELAY_NS,
        "attempt_wait_ns": base.MAX_ATTEMPT_WAIT_NS,
        "pricing": "FIRST_PHYSICAL_EVENT_BBO_WITHOUT_FAVORABLE_SELECTION",
        "same_physical_event_reuse": False,
    },
    "cost_ticks": {"standard": 10, "stress": 14},
    "base_code_sha256": EXPECTED_BASE_CODE_SHA256,
    "go_thresholds": {
        "completed_fills_min": 18,
        "active_dates_min": 8,
        "long_fills_min": 6,
        "short_fills_min": 6,
        "chronological_half_completed_min": {"first": 6, "last": 8},
        "cache_unavailable_max": 0,
        "exit_censored_max": 0,
        "gross_ev_strictly_gt": 14,
        "net10_ev_positive": True,
        "profit_factor_net10_min": 1.05,
        "chronological_half_net10_ev_positive": True,
        "mean_topology_ic_positive": True,
        "mean_ic_delta_positive": True,
        "positive_ic_delta_folds_min": 2,
        "net10_ev_delta_positive": True,
        "complete_net10_ev_delta_folds_required": 3,
        "positive_net10_ev_delta_folds_min": 2,
    },
    "time_cap_seconds": TIME_CAP_SECONDS,
    "no_parameter_grid": True,
}
PILOT_PLAN_SHA256 = hashlib.sha256(
    json.dumps(
        PILOT_PLAN,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
).hexdigest()


def _canonical_json(document: object) -> str:
    return json.dumps(
        document,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _numpy_column(table: pa.Table, name: str) -> np.ndarray:
    return table[name].combine_chunks().to_numpy(zero_copy_only=False)


def _read_selected_topology_table(path: Path, instrument_id: int) -> tuple[pa.Table, int]:
    parquet = pq.ParquetFile(path)
    if set(TOPOLOGY_RAW_COLUMNS) - set(parquet.schema_arrow.names):
        missing = sorted(set(TOPOLOGY_RAW_COLUMNS) - set(parquet.schema_arrow.names))
        raise RuntimeError(f"topology source columns missing: {missing}")
    pieces: list[pa.Table] = []
    source_rows = 0
    for row_group in range(parquet.metadata.num_row_groups):
        table = parquet.read_row_group(
            row_group,
            columns=TOPOLOGY_RAW_COLUMNS,
            use_threads=True,
        )
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
        raise RuntimeError(f"selected instrument {instrument_id} has no topology rows in {path}")
    return pa.concat_tables(pieces), source_rows


def _matrix(table: pa.Table, side: str, field: str) -> np.ndarray:
    return np.column_stack(
        [_numpy_column(table, f"{side}_{field}_{level:02d}") for level in range(10)]
    )


def _divide(numerator: np.ndarray, denominator: np.ndarray) -> np.ndarray:
    return np.divide(
        numerator,
        denominator,
        out=np.full(len(numerator), np.nan, dtype=np.float64),
        where=denominator > 0,
    )


def _bounded_imbalance(lhs: np.ndarray, rhs: np.ndarray) -> np.ndarray:
    return _divide(lhs - rhs, lhs + rhs)


def _topology_frame(
    root: Path,
    selection: base.Selection,
    expected_source_rows: int,
) -> tuple[pd.DataFrame, dict[str, int]]:
    raw_path = root / "data" / selection.source_relative_uri
    raw_stat = raw_path.stat(follow_symlinks=False)
    if raw_path.is_symlink() or not stat_module.S_ISREG(raw_stat.st_mode):
        raise RuntimeError(f"topology raw source is not a regular file: {selection.source_date}")
    if base._file_sha256(raw_path) != selection.source_sha256:
        raise RuntimeError(f"topology raw source SHA drifted: {selection.source_date}")
    table, source_rows = _read_selected_topology_table(raw_path, selection.instrument_id)
    if source_rows != expected_source_rows:
        raise RuntimeError(f"topology raw source row count drifted: {selection.source_date}")
    ts_ns = (
        pc.cast(table["ts_recv"].combine_chunks(), pa.int64())
        .to_numpy(zero_copy_only=False)
        .astype(np.int64)
    )
    action = np.asarray(table["action"].combine_chunks().to_pylist(), dtype="U1")
    flags = _numpy_column(table, "flags").astype(np.uint8)
    bid_px = _matrix(table, "bid", "px").astype(np.int64)
    ask_px = _matrix(table, "ask", "px").astype(np.int64)
    bid_sz = _matrix(table, "bid", "sz").astype(np.float64)
    ask_sz = _matrix(table, "ask", "sz").astype(np.float64)
    bid_ct = _matrix(table, "bid", "ct").astype(np.float64)
    ask_ct = _matrix(table, "ask", "ct").astype(np.float64)

    event_ends, event_end_ns, event_valid = base._completed_event_state(
        ts_ns,
        action,
        flags,
        bid_px,
        ask_px,
    )
    event_seconds = base._ceil_div(event_end_ns, base.SECOND_NS).astype(np.int64)
    last_in_second = np.r_[event_seconds[1:] != event_seconds[:-1], True]
    snapshot_positions = np.flatnonzero(last_in_second & event_valid)
    row_positions = event_ends[snapshot_positions]
    seconds = event_seconds[snapshot_positions]

    bid_defined = bid_px[row_positions] != base.UNDEFINED_PRICE
    ask_defined = ask_px[row_positions] != base.UNDEFINED_PRICE
    bid_sizes = np.where(bid_defined, bid_sz[row_positions], 0.0)
    ask_sizes = np.where(ask_defined, ask_sz[row_positions], 0.0)
    bid_counts = np.where(bid_defined, bid_ct[row_positions], 0.0)
    ask_counts = np.where(ask_defined, ask_ct[row_positions], 0.0)

    bid_size_total = bid_sizes.sum(axis=1)
    ask_size_total = ask_sizes.sum(axis=1)
    bid_count_total = bid_counts.sum(axis=1)
    ask_count_total = ask_counts.sum(axis=1)
    bid_mean_order_size = _divide(bid_size_total, bid_count_total)
    ask_mean_order_size = _divide(ask_size_total, ask_count_total)
    bid_near_size_share = _divide(bid_sizes[:, :3].sum(axis=1), bid_size_total)
    ask_near_size_share = _divide(ask_sizes[:, :3].sum(axis=1), ask_size_total)
    bid_near_count_share = _divide(bid_counts[:, :3].sum(axis=1), bid_count_total)
    ask_near_count_share = _divide(ask_counts[:, :3].sum(axis=1), ask_count_total)
    levels = np.arange(10, dtype=np.float64)
    bid_size_centroid = _divide((bid_sizes * levels).sum(axis=1), bid_size_total) / 9.0
    ask_size_centroid = _divide((ask_sizes * levels).sum(axis=1), ask_size_total) / 9.0
    bid_count_centroid = _divide((bid_counts * levels).sum(axis=1), bid_count_total) / 9.0
    ask_count_centroid = _divide((ask_counts * levels).sum(axis=1), ask_count_total) / 9.0
    total_size = bid_size_total + ask_size_total
    total_count = bid_count_total + ask_count_total

    snapshot = pd.DataFrame(
        {
            "bucket_s": (base._ceil_div(seconds, base.FIVE_MINUTES_S) * base.FIVE_MINUTES_S),
            "order_count_imbalance": _bounded_imbalance(
                bid_count_total,
                ask_count_total,
            ),
            "mean_order_size_imbalance": _bounded_imbalance(
                bid_mean_order_size,
                ask_mean_order_size,
            ),
            "near_size_share_imbalance": bid_near_size_share - ask_near_size_share,
            "near_count_share_imbalance": bid_near_count_share - ask_near_count_share,
            "size_centroid_skew": ask_size_centroid - bid_size_centroid,
            "count_centroid_skew": ask_count_centroid - bid_count_centroid,
            "total_order_count_log": np.log1p(total_count),
            "mean_order_size_log": np.log1p(_divide(total_size, total_count)),
        }
    )
    finite = np.isfinite(snapshot[list(TOPOLOGY_STATE_FIELDS)].to_numpy(dtype=np.float64)).all(
        axis=1
    )
    snapshot = snapshot.loc[finite].copy()

    five_path = root / (
        "data/derived/research_5m/version=phase1a_mbp10_screening_v1/"
        f"contract={selection.raw_symbol}/source_date={selection.source_date}/part-000.parquet"
    )
    base_frame = base._load_five_minute_frame(five_path)
    clean_buckets = set(
        base_frame.index[base_frame["path_contamination_free"]].to_numpy(dtype=np.int64).tolist()
    )
    snapshot = snapshot.loc[snapshot["bucket_s"].isin(clean_buckets)].copy()

    rows: list[dict[str, float | int]] = []
    for bucket_s, group in snapshot.groupby("bucket_s", sort=True):
        row: dict[str, float | int] = {"bucket_s": int(bucket_s)}
        for field in TOPOLOGY_STATE_FIELDS:
            values = group[field].to_numpy(dtype=np.float64)
            row[f"topology_{field}_mean"] = float(np.mean(values))
            row[f"topology_{field}_last"] = float(values[-1])
            row[f"topology_{field}_change"] = float(values[-1] - values[0])
        rows.append(row)
    result = pd.DataFrame.from_records(rows)
    if result.empty:
        result = pd.DataFrame(columns=["bucket_s", *TOPOLOGY_FEATURES])
    if result["bucket_s"].duplicated().any():
        raise RuntimeError("topology aggregation produced duplicate buckets")
    return result, {
        "source_rows": source_rows,
        "selected_rows": table.num_rows,
        "valid_second_snapshots": len(snapshot),
        "topology_buckets": len(result),
    }


def _causal_mask(frame: pd.DataFrame) -> np.ndarray:
    price_complete = np.isfinite(frame[list(base.PRICE_FEATURES)].to_numpy(dtype=np.float64)).all(
        axis=1
    )
    topology_complete = np.isfinite(frame[list(TOPOLOGY_FEATURES)].to_numpy(dtype=np.float64)).all(
        axis=1
    )
    seconds = frame["bucket_s"].to_numpy(dtype=np.int64) % 86_400
    return (
        frame["history_contamination_free"].to_numpy(dtype=bool)
        & price_complete
        & topology_complete
        & (seconds <= LATEST_DECISION_SECOND_UTC)
    )


def _performance(signals: pd.DataFrame, score_dates: Sequence[str]) -> dict[str, Any]:
    count = len(signals)
    gains = float(signals.loc[signals["net10"] > 0, "net10"].sum()) if count else 0.0
    losses = float(-signals.loc[signals["net10"] < 0, "net10"].sum()) if count else 0.0
    midpoint = len(score_dates) // 2
    halves: dict[str, dict[str, float | int | None]] = {}
    for label, dates in (
        ("first", score_dates[:midpoint]),
        ("last", score_dates[midpoint:]),
    ):
        group = signals.loc[signals["source_date"].isin(dates)]
        halves[label] = {
            "count": len(group),
            "net10_ev": float(group["net10"].mean()) if len(group) else None,
        }
    return {
        "count": count,
        "active_dates": int(signals["source_date"].nunique()) if count else 0,
        "long_count": int((signals["direction"] > 0).sum()) if count else 0,
        "short_count": int((signals["direction"] < 0).sum()) if count else 0,
        "gross_total": float(signals["gross"].sum()) if count else 0.0,
        "gross_ev": float(signals["gross"].mean()) if count else None,
        "net10_total": float(signals["net10"].sum()) if count else 0.0,
        "net10_ev": float(signals["net10"].mean()) if count else None,
        "net14_total": float(signals["net14"].sum()) if count else 0.0,
        "profit_factor_net10": (float(gains / losses) if losses else ("INF" if gains else None)),
        "chronological_halves": halves,
    }


def _decision(result: dict[str, Any], elapsed_seconds: float) -> dict[str, Any]:
    reasons: list[str] = []
    performance = result["topology"]["performance"]
    audit = result["topology"]["signal_audit"]
    baseline_audit = result["baseline"]["signal_audit"]
    if elapsed_seconds > TIME_CAP_SECONDS:
        reasons.append("TIME_CAP_EXCEEDED")
    if performance["count"] < 18:
        reasons.append("FILLS_LT_18")
    if performance["active_dates"] < 8:
        reasons.append("ACTIVE_DATES_LT_8")
    if performance["long_count"] < 6:
        reasons.append("LONG_FILLS_LT_6")
    if performance["short_count"] < 6:
        reasons.append("SHORT_FILLS_LT_6")
    if audit["cache_unavailable"]:
        reasons.append("TOPOLOGY_EXECUTION_CACHE_INCOMPLETE")
    if audit["exit_censored"]:
        reasons.append("TOPOLOGY_EXIT_CENSOR_PRESENT")
    if baseline_audit["cache_unavailable"]:
        reasons.append("BASELINE_EXECUTION_CACHE_INCOMPLETE")
    if baseline_audit["exit_censored"]:
        reasons.append("BASELINE_EXIT_CENSOR_PRESENT")
    gross_ev = performance["gross_ev"]
    if not (isinstance(gross_ev, (int, float)) and gross_ev > 14):
        reasons.append("GROSS_EV_NOT_GT_14")
    net10_ev = performance["net10_ev"]
    if not (isinstance(net10_ev, (int, float)) and net10_ev > 0):
        reasons.append("NET10_EV_NOT_POSITIVE")
    pf = performance["profit_factor_net10"]
    if pf != "INF" and not (isinstance(pf, (int, float)) and pf >= 1.05):
        reasons.append("PF_NET10_LT_1_05")
    if any(
        not (isinstance(half["net10_ev"], (int, float)) and half["net10_ev"] > 0)
        for half in performance["chronological_halves"].values()
    ):
        reasons.append("CHRONOLOGICAL_HALF_NET10_NOT_POSITIVE")
    half_counts = performance["chronological_halves"]
    if half_counts["first"]["count"] < 6:
        reasons.append("FIRST_HALF_FILLS_LT_6")
    if half_counts["last"]["count"] < 8:
        reasons.append("LAST_HALF_FILLS_LT_8")
    if not (result["topology"]["mean_fold_ic"] > 0):
        reasons.append("TOPOLOGY_MEAN_IC_NOT_POSITIVE")
    increment = result["increment"]
    if not (increment["mean_fold_ic_delta"] > 0):
        reasons.append("MEAN_IC_DELTA_NOT_POSITIVE")
    if increment["positive_ic_delta_folds"] < 2:
        reasons.append("POSITIVE_IC_DELTA_FOLDS_LT_2")
    if not (
        isinstance(increment["net10_ev_delta"], (int, float)) and increment["net10_ev_delta"] > 0
    ):
        reasons.append("NET10_EV_DELTA_NOT_POSITIVE")
    if increment["complete_net10_ev_delta_folds"] != 3:
        reasons.append("NET10_EV_DELTA_FOLDS_INCOMPLETE")
    if increment["positive_net10_ev_delta_folds"] < 2:
        reasons.append("POSITIVE_NET10_EV_DELTA_FOLDS_LT_2")
    return {"decision": "GO" if not reasons else "STOP", "reasons": reasons}


def _run_models(frame: pd.DataFrame) -> dict[str, Any]:
    dates = list(base.BENCHMARK_DATES)
    folds = ((5, dates[5:8]), (8, dates[8:11]), (11, dates[11:14]))
    signals_by_model: dict[str, list[pd.DataFrame]] = {"baseline": [], "topology": []}
    audits_by_model: dict[str, list[dict[str, int]]] = {"baseline": [], "topology": []}
    fold_rows: list[dict[str, Any]] = []
    causal = _causal_mask(frame)
    for fold_number, (train_count, test_dates) in enumerate(folds, start=1):
        train_dates = set(dates[:train_count])
        test_date_set = set(test_dates)
        train_mask = (
            causal
            & frame["source_date"].isin(train_dates).to_numpy(dtype=bool)
            & frame["label_360m"].notna().to_numpy(dtype=bool)
        )
        test_mask = causal & frame["source_date"].isin(test_date_set).to_numpy(dtype=bool)
        train = frame.loc[train_mask].copy()
        test = frame.loc[test_mask].copy()
        test["block"] = fold_number
        y_train = train["label_360m"].to_numpy(dtype=np.float64)
        ic_mask = test["label_360m"].notna().to_numpy(dtype=bool)
        if (
            len(train) < MIN_TRAIN_ROWS
            or len(test) < MIN_EXECUTION_TEST_ROWS
            or int(ic_mask.sum()) < MIN_IC_TEST_ROWS
        ):
            raise RuntimeError(
                f"topology fold {fold_number} lacks rows: "
                f"train={len(train)} execution_test={len(test)} ic_test={int(ic_mask.sum())}"
            )
        y_ic = test.loc[ic_mask, "label_360m"].to_numpy(dtype=np.float64)
        fold: dict[str, Any] = {
            "fold": fold_number,
            "train_dates": sorted(train_dates),
            "test_dates": list(test_dates),
            "train_rows": len(train),
            "execution_test_rows": len(test),
            "ic_test_rows": int(ic_mask.sum()),
        }
        for label, features in (
            ("baseline", CORE_FEATURES),
            ("topology", (*CORE_FEATURES, *TOPOLOGY_FEATURES)),
        ):
            model = base._model()
            x_train = train[list(features)].to_numpy(dtype=np.float64)
            x_test = test[list(features)].to_numpy(dtype=np.float64)
            model.fit(x_train, y_train)
            train_prediction = model.predict(x_train)
            low, high = np.quantile(train_prediction, SIGNAL_QUANTILES)
            test_prediction = model.predict(x_test)
            signals, audit = base._followup_nonoverlap_signals(
                test,
                test_prediction,
                float(low),
                float(high),
            )
            if not signals.empty:
                signals_by_model[label].append(signals)
            audits_by_model[label].append(audit)
            fold[f"{label}_ic"] = base._finite(base._safe_spearman(y_ic, test_prediction[ic_mask]))
            fold[f"{label}_threshold_low"] = float(low)
            fold[f"{label}_threshold_high"] = float(high)
            fold[f"{label}_signals"] = len(signals)
            fold[f"{label}_net10_ev"] = float(signals["net10"].mean()) if len(signals) else None
        if not all(isinstance(fold[name], (int, float)) for name in ("baseline_ic", "topology_ic")):
            raise RuntimeError(f"topology fold {fold_number} IC is unavailable")
        fold["topology_minus_baseline_ic"] = float(fold["topology_ic"] - fold["baseline_ic"])
        baseline_net10_ev = fold["baseline_net10_ev"]
        topology_net10_ev = fold["topology_net10_ev"]
        fold["topology_minus_baseline_net10_ev"] = (
            float(topology_net10_ev - baseline_net10_ev)
            if isinstance(baseline_net10_ev, (int, float))
            and isinstance(topology_net10_ev, (int, float))
            else None
        )
        fold_rows.append(fold)

    score_dates = dates[5:]
    result: dict[str, Any] = {"folds": fold_rows}
    for label in ("baseline", "topology"):
        signals = (
            pd.concat(signals_by_model[label], ignore_index=True)
            if signals_by_model[label]
            else pd.DataFrame()
        )
        audits = audits_by_model[label]
        audit_totals = {
            key: int(sum(audit[key] for audit in audits))
            for key in (
                "candidates",
                "cache_unavailable",
                "entry_no_fill",
                "exit_censored",
                "overlap_skipped",
                "completed",
            )
        }
        result[label] = {
            "mean_fold_ic": float(np.mean([fold[f"{label}_ic"] for fold in fold_rows])),
            "performance": _performance(signals, score_dates),
            "signal_audit": audit_totals,
        }
    deltas = [float(fold["topology_minus_baseline_ic"]) for fold in fold_rows]
    net10_deltas = [
        fold["topology_minus_baseline_net10_ev"]
        for fold in fold_rows
        if isinstance(fold["topology_minus_baseline_net10_ev"], (int, float))
    ]
    baseline_net10_ev = result["baseline"]["performance"]["net10_ev"]
    topology_net10_ev = result["topology"]["performance"]["net10_ev"]
    result["increment"] = {
        "fold_ic_delta": {
            str(fold["fold"]): fold["topology_minus_baseline_ic"] for fold in fold_rows
        },
        "mean_fold_ic_delta": float(np.mean(deltas)),
        "positive_ic_delta_folds": int(sum(delta > 0 for delta in deltas)),
        "fold_net10_ev_delta": {
            str(fold["fold"]): fold["topology_minus_baseline_net10_ev"] for fold in fold_rows
        },
        "net10_ev_delta": (
            float(topology_net10_ev - baseline_net10_ev)
            if isinstance(baseline_net10_ev, (int, float))
            and isinstance(topology_net10_ev, (int, float))
            else None
        ),
        "positive_net10_ev_delta_folds": int(sum(delta > 0 for delta in net10_deltas)),
        "complete_net10_ev_delta_folds": len(net10_deltas),
    }
    return result


def main() -> int:
    started = perf_counter()
    actual_base_sha256 = hashlib.sha256(Path(base.__file__).read_bytes()).hexdigest()
    if actual_base_sha256 != EXPECTED_BASE_CODE_SHA256:
        raise RuntimeError("frozen topology base implementation drifted")
    root = base._project_root()
    discovery, selections = base._load_discovery_selections(root)
    dates = list(base.BENCHMARK_DATES)
    if any(source_date not in selections for source_date in dates):
        raise RuntimeError("topology benchmark selection is incomplete")
    requests = base._execution_cache_requests(root, discovery, selections)
    benchmark_requests = {source_date: requests[source_date] for source_date in dates}
    caches, missing = base._load_execution_caches(
        root,
        benchmark_requests,
        selections,
        verify_content=True,
    )
    if missing or any(source_date not in caches for source_date in dates):
        raise RuntimeError("topology benchmark execution cache is incomplete")

    frames: list[pd.DataFrame] = []
    audits: list[dict[str, Any]] = []
    for position, source_date in enumerate(dates, start=1):
        if perf_counter() - started >= TIME_CAP_SECONDS:
            raise TimeoutError("topology preflight time cap reached")
        selection = selections[source_date]
        frame, base_audit = base._extract_day(
            root,
            selection,
            followup360=True,
            execution_cache=caches[source_date],
        )
        topology, topology_audit = _topology_frame(
            root,
            selection,
            benchmark_requests[source_date].source_row_count,
        )
        merged = frame.merge(topology, on="bucket_s", how="left", validate="one_to_one")
        frames.append(merged)
        audits.append({"source_date": source_date, **base_audit, **topology_audit})
        print(
            f"[{position}/{len(dates)}] {source_date} "
            f"selected={base_audit['selected_rows']} topology_buckets={len(topology)}",
            file=sys.stderr,
            flush=True,
        )
    combined = pd.concat(frames, ignore_index=True).sort_values(["source_date", "bucket_s"])
    models = _run_models(combined)
    elapsed = perf_counter() - started
    decision = _decision(models, elapsed)
    output = {
        "schema": "l10_order_topology_transition_preflight.v1",
        "code_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "base_code_sha256": actual_base_sha256,
        "plan": PILOT_PLAN,
        "plan_sha256": PILOT_PLAN_SHA256,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "visible_partition": {
            "name": "DISCOVERY_BENCHMARK14",
            "source_dates": dates,
            "later_partitions_opened": False,
        },
        "support": {
            "dates": len(dates),
            "decision_rows": len(combined),
            "causal_rows": int(_causal_mask(combined).sum()),
            "valid_second_snapshots": int(sum(audit["valid_second_snapshots"] for audit in audits)),
            "topology_buckets": int(sum(audit["topology_buckets"] for audit in audits)),
        },
        "models": models,
        "decision": decision,
        "elapsed_seconds": elapsed,
        "limitations": [
            "OUTCOME_INFORMED_DISCOVERY_PREFLIGHT_ONLY",
            "BENCHMARK14_NOT_FUTURE_OOS",
            "TOP10_AGGREGATED_BOOK_NOT_MBO_QUEUE_PRIORITY",
            "ORDER_COUNT_IS_DISPLAYED_AGGREGATE_COUNT",
            "BBO_ONE_LOT_WITHOUT_BROKER_FILL_PROOF",
            "NO_LATER_WF_OR_HOLDOUT_ARTIFACT_OPENED",
            "NO_PARAMETER_OR_HORIZON_GRID",
            "NO_PROMOTION_AUTHORITY",
        ],
    }
    print(_canonical_json(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
