"""Outcome-blind feasibility gate for multiscale bar-price x trade-flow ML.

This program deliberately stops before reading returns, execution caches, Search
results, or any model outcome.  It answers a narrower question: can the exact
all-cases PRICE36 + FLOW30 feature representation and two deterministic,
edge-disjoint date-block target nulls be constructed on the frozen 14-date
benchmark within a small resource budget?
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import resource
import signal
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from time import monotonic
from types import SimpleNamespace
from typing import Final

for _thread_variable in (
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
):
    os.environ[_thread_variable] = "1"

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import maximum_bipartite_matching
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_squared_error

_IMPORT_ROOT = Path(__file__).resolve().parents[1]
if str(_IMPORT_ROOT) not in sys.path:
    sys.path.insert(0, str(_IMPORT_ROOT))

from campaigns.ai_all_cases_v1 import ml, pipeline

SECOND_NS: Final = 1_000_000_000
FIVE_MINUTES_S: Final = 300
HORIZON_SECONDS: Final = 3_600
MIN_NULL_ANCHOR_SEPARATION_SECONDS: Final = 7 * 3_600
LATEST_DECISION_SECOND_UTC: Final = 15 * 3_600 + 55 * 60
MAX_CONSECUTIVE_STALE_BUCKETS: Final = 2
MAX_DERANGEMENT_ROWS: Final = 2_000
MAX_RUNTIME_SECONDS: Final = 600.0
MAX_RSS_BYTES: Final = 4 * 1024**3
MIN_FEATURE_TRAIN_ROWS: Final = 300
MIN_FEATURE_TEST_ROWS: Final = 200
MIN_TEST_NONOVERLAP_ROWS: Final = 24
MIN_TOTAL_TEST_NONOVERLAP_ROWS: Final = 80
STAGE_KEY: Final = "BAR_FLOW_INTERACTION_ML_FEASIBILITY_V1"
SCHEMA: Final = "systematic_fx.bar_flow_interaction_ml_feasibility.v1"

EXPECTED_ML_SOURCE_SHA256: Final = (
    "4bfd6e856291baa031be55bcd40e06bc2395e7ff3612fa52f81a39559b4519d8"
)
EXPECTED_PIPELINE_SOURCE_SHA256: Final = (
    "cfe01fd6614a8f196b341878482593a0c50ae294349af706b59906911ceb8e0a"
)
EXPECTED_TRANSITIVE_SOURCE_SHA256: Final = {
    "campaigns/ai_all_cases_v1/config.py": (
        "4baa6ad4161fd58b3759f00d2f2e226a728c1fbf1e74f46b75ee4dfb600a8312"
    ),
    "campaigns/ai_all_cases_v1/ml.py": EXPECTED_ML_SOURCE_SHA256,
    "campaigns/ai_all_cases_v1/pipeline.py": EXPECTED_PIPELINE_SOURCE_SHA256,
    "scripts/ai_pattern_holdout_engine.py": (
        "613f76e22a793839eb148d57a8e7f384229eec8e7119c212b48df1595191eb3d"
    ),
    "src/systematic_fx/features/bars.py": (
        "cc35241b80310e0f8ee6f12c7bceb241fa6200f878b88c1155f71ee320d9d203"
    ),
    "src/systematic_fx/features/screening.py": (
        "8da7b7a5d0059cbc54a9ca238c4846e6362d332fb4c458c7e59658c5c5f85e0d"
    ),
    "src/systematic_fx/research/bar_pipeline.py": (
        "e32d50b7a1ad6feee9d1d67db614af6a87e02611821e88bd26428e4986a250c0"
    ),
    "src/systematic_fx/validation/bar_splits.py": (
        "07b0690e9456b261fc2e8033e13096ffbb0e258e353b54e2d0e9b08bd181a616"
    ),
}
EXPECTED_SCREENING_FORMULA_SHA256: Final = (
    "2e9665350022cdac7b53fa526b5e445d74c2799a4afb022271038ca513fef3b8"
)

BENCHMARK_DATES: Final = (
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
DATE_BLOCK_SLICES: Final = ((0, 5), (5, 8), (8, 11), (11, 14))
EXPECTED_STRUCTURAL_COUNTS: Final = (328, 278, 283, 234)
EXPECTED_STRUCTURAL_NONOVERLAP: Final = (45, 29, 32, 29)
EXPECTED_NULL_TRAIN_COUNTS: Final = (328, 606, 889)

CONTAMINATION_FIELDS: Final = (
    "locked_seconds",
    "crossed_seconds",
    "maybe_bad_book_seconds",
    "bad_ts_recv_seconds",
    "reset_seen_seconds",
    "recovery_marker_seconds",
    "recovery_required_seconds",
    "off_tick_grid_seconds",
)
STRUCTURAL_COLUMNS: Final = (
    "bucket_end",
    "decision_quote_fresh",
    *CONTAMINATION_FIELDS,
)

FEATURE66: Final = ml.PRICE_FEATURE_NAMES + ml.FLOW_FEATURE_NAMES
if (
    len(ml.PRICE_FEATURE_NAMES) != 36
    or len(ml.FLOW_FEATURE_NAMES) != 30
    or len(FEATURE66) != 66
    or len(set(FEATURE66)) != 66
    or ml.FLOW_TIME_REGIME_90[:66] != FEATURE66
):  # pragma: no cover - import-time frozen-source closure
    raise RuntimeError("PRICE36/FLOW30 feature closure drifted")


class FeasibilityError(RuntimeError):
    """Fail-closed feasibility result."""


class DateBlockError(FeasibilityError):
    """The outcome-free date-block null geometry is infeasible."""


@dataclass(frozen=True, slots=True)
class StructuralRows:
    history_keys: frozenset[tuple[str, str, int]]
    future_capacity_keys: frozenset[tuple[str, str, int]]
    intersection_keys: frozenset[tuple[str, str, int]]
    history_counts: tuple[int, ...]
    future_counts: tuple[int, ...]
    intersection_counts: tuple[int, ...]
    nonoverlap_counts: tuple[int, ...]
    artifact_bindings: tuple[dict[str, object], ...]


@dataclass(frozen=True, slots=True)
class DateBlockGeometry:
    row_ids: tuple[str, ...]
    product_keys: tuple[str, ...]
    source_dates: tuple[date, ...]
    planned_start_ns: tuple[int, ...]
    planned_end_ns: tuple[int, ...]

    def __post_init__(self) -> None:
        count = len(self.row_ids)
        if (
            count < 3
            or len(set(self.row_ids)) != count
            or any(
                len(values) != count
                for values in (
                    self.product_keys,
                    self.source_dates,
                    self.planned_start_ns,
                    self.planned_end_ns,
                )
            )
            or any(not value for value in self.product_keys)
            or any(
                end - start != HORIZON_SECONDS * SECOND_NS
                for start, end in zip(
                    self.planned_start_ns,
                    self.planned_end_ns,
                    strict=True,
                )
            )
        ):
            raise DateBlockError("DATE_BLOCK_GEOMETRY_INVALID")


@dataclass(frozen=True, slots=True)
class DateBlockPlan:
    destinations: tuple[int, ...]
    first_sources: tuple[int, ...]
    second_sources: tuple[int, ...]
    minimum_destination_degree: int
    minimum_source_degree: int
    sha256: str


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _peak_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1024


def _check_resource_budget(started: float) -> None:
    if monotonic() - started > MAX_RUNTIME_SECONDS:
        raise FeasibilityError("STOP_TIMEOUT")
    if _peak_rss_bytes() > MAX_RSS_BYTES:
        raise FeasibilityError("STOP_RSS_CAP")


def _timeout_handler(_signum: int, _frame: object) -> None:
    raise FeasibilityError("STOP_TIMEOUT")


def _bounded_fresh_path(values: np.ndarray) -> bool:
    fresh = np.asarray(values, dtype=np.bool_)
    if len(fresh) == 0 or not bool(fresh[0]) or not bool(fresh[-1]):
        return False
    longest = 0
    current = 0
    for stale in ~fresh:
        current = current + 1 if bool(stale) else 0
        longest = max(longest, current)
    return longest <= MAX_CONSECUTIVE_STALE_BUCKETS


def _greedy_nonoverlap_count(keys: Sequence[tuple[str, str, int]]) -> int:
    last_exit_by_date: dict[str, int] = {}
    kept = 0
    for source_date, _contract, decision_ns in sorted(keys):
        planned_start = decision_ns + SECOND_NS
        if planned_start <= last_exit_by_date.get(source_date, -1):
            continue
        last_exit_by_date[source_date] = planned_start + HORIZON_SECONDS * SECOND_NS
        kept += 1
    return kept


def _structural_rows(root: Path) -> StructuralRows:
    feature_root = root / "data/derived/research_5m/version=phase1a_mbp10_screening_v1"
    history_keys: set[tuple[str, str, int]] = set()
    future_keys: set[tuple[str, str, int]] = set()
    intersection_keys: set[tuple[str, str, int]] = set()
    history_by_date: dict[str, int] = {}
    future_by_date: dict[str, int] = {}
    intersection_by_date: dict[str, int] = {}
    bindings: list[dict[str, object]] = []

    for source_date in BENCHMARK_DATES:
        matches = tuple(feature_root.glob(f"contract=*/source_date={source_date}/part-000.parquet"))
        if len(matches) != 1:
            raise FeasibilityError(f"STRUCTURAL_ARTIFACT_COUNT_{source_date}")
        path = matches[0]
        if path.is_symlink() or not path.is_file():
            raise FeasibilityError(f"STRUCTURAL_ARTIFACT_TYPE_{source_date}")
        parquet = pq.ParquetFile(path)
        metadata = parquet.schema_arrow.metadata or {}
        contract = path.parent.parent.name.split("=", 1)[1]
        expected_metadata = {
            b"systematic_fx.source_date": source_date,
            b"systematic_fx.contract": contract,
            b"systematic_fx.granularity": "5m",
            b"systematic_fx.formula_sha256": EXPECTED_SCREENING_FORMULA_SHA256,
        }
        if any(
            metadata.get(key, b"").decode("ascii") != expected
            for key, expected in expected_metadata.items()
        ):
            raise FeasibilityError(f"STRUCTURAL_LINEAGE_{source_date}")
        if any(name not in parquet.schema_arrow.names for name in STRUCTURAL_COLUMNS):
            raise FeasibilityError(f"STRUCTURAL_SCHEMA_{source_date}")
        table = parquet.read(columns=list(STRUCTURAL_COLUMNS), use_threads=True)
        bucket_ns = np.asarray(
            table["bucket_end"].combine_chunks().cast(pa.int64()).to_numpy(),
            dtype=np.int64,
        )
        fresh = np.asarray(
            table["decision_quote_fresh"].combine_chunks().to_numpy(zero_copy_only=False),
            dtype=np.bool_,
        )
        clean = np.ones(len(bucket_ns), dtype=np.bool_)
        for field in CONTAMINATION_FIELDS:
            clean &= np.asarray(table[field].combine_chunks().to_numpy()) == 0
        if (
            len(bucket_ns) == 0
            or np.any(np.diff(bucket_ns) <= 0)
            or np.any(bucket_ns % (FIVE_MINUTES_S * SECOND_NS) != 0)
        ):
            raise FeasibilityError(f"STRUCTURAL_BUCKET_ORDER_{source_date}")
        by_bucket = {int(value): index for index, value in enumerate(bucket_ns)}
        if len(by_bucket) != len(bucket_ns):
            raise FeasibilityError(f"STRUCTURAL_BUCKET_DUPLICATE_{source_date}")

        history_count = 0
        future_count = 0
        intersection_count = 0
        for decision_ns in bucket_ns:
            second_utc = int(decision_ns // SECOND_NS) % 86_400
            if second_utc > LATEST_DECISION_SECOND_UTC:
                continue
            history_positions = [
                by_bucket.get(int(decision_ns) - offset * FIVE_MINUTES_S * SECOND_NS)
                for offset in range(12, -1, -1)
            ]
            future_positions = [
                by_bucket.get(int(decision_ns) + offset * FIVE_MINUTES_S * SECOND_NS)
                for offset in range(13)
            ]
            history_ok = False
            future_ok = False
            if all(index is not None for index in history_positions):
                indexes = np.asarray(history_positions, dtype=np.int64)
                history_ok = bool(clean[indexes].all()) and _bounded_fresh_path(fresh[indexes])
            if all(index is not None for index in future_positions):
                indexes = np.asarray(future_positions, dtype=np.int64)
                future_ok = bool(clean[indexes].all()) and _bounded_fresh_path(fresh[indexes])
            key = (source_date, contract, int(decision_ns))
            if history_ok:
                history_keys.add(key)
                history_count += 1
            if future_ok:
                future_keys.add(key)
                future_count += 1
            if history_ok and future_ok:
                intersection_keys.add(key)
                intersection_count += 1
        history_by_date[source_date] = history_count
        future_by_date[source_date] = future_count
        intersection_by_date[source_date] = intersection_count
        bindings.append(
            {
                "contract": contract,
                "file_sha256": _file_sha256(path),
                "row_count": int(table.num_rows),
                "source_date": source_date,
                "source_sha256": metadata.get(b"systematic_fx.source_sha256", b"").decode("ascii"),
            }
        )

    history_counts = tuple(
        sum(history_by_date[value] for value in BENCHMARK_DATES[start:stop])
        for start, stop in DATE_BLOCK_SLICES
    )
    future_counts = tuple(
        sum(future_by_date[value] for value in BENCHMARK_DATES[start:stop])
        for start, stop in DATE_BLOCK_SLICES
    )
    intersection_counts = tuple(
        sum(intersection_by_date[value] for value in BENCHMARK_DATES[start:stop])
        for start, stop in DATE_BLOCK_SLICES
    )
    nonoverlap_counts = tuple(
        _greedy_nonoverlap_count(
            [key for key in intersection_keys if key[0] in BENCHMARK_DATES[start:stop]]
        )
        for start, stop in DATE_BLOCK_SLICES
    )
    if intersection_counts != EXPECTED_STRUCTURAL_COUNTS:
        raise FeasibilityError("STRUCTURAL_COUNT_DRIFT")
    if nonoverlap_counts != EXPECTED_STRUCTURAL_NONOVERLAP:
        raise FeasibilityError("STRUCTURAL_NONOVERLAP_DRIFT")
    return StructuralRows(
        frozenset(history_keys),
        frozenset(future_keys),
        frozenset(intersection_keys),
        history_counts,
        future_counts,
        intersection_counts,
        nonoverlap_counts,
        tuple(bindings),
    )


def _anchor_id(item: object) -> str:
    bar = item.bar
    return _sha256(
        {
            "contract": bar.contract,
            "decision_ns": int(bar.end_ns),
            "entry_ns": int(bar.end_ns) + SECOND_NS,
            "outcome_span_id": int(item.outcome_span_id),
            "schema": "systematic_fx.bar_flow_interaction_anchor.v1",
            "segment_id": int(bar.segment_id),
            "source_date": bar.source_date.isoformat(),
        }
    )


def _causal_feature_rows(root: Path) -> tuple[ml.CausalFeatureRows, np.ndarray, dict[str, object]]:
    actual_hashes = {
        relative_path: _file_sha256(root / relative_path)
        for relative_path in EXPECTED_TRANSITIVE_SOURCE_SHA256
    }
    if actual_hashes != EXPECTED_TRANSITIVE_SOURCE_SHA256:
        raise FeasibilityError("FROZEN_SOURCE_SHA256_DRIFT")

    search_plan = pipeline._plans(root).search
    last_benchmark = date.fromisoformat(BENCHMARK_DATES[-1])
    partitions = tuple(
        partition for partition in search_plan.partitions if partition.source_date <= last_benchmark
    )
    partition_dates = {partition.source_date.isoformat() for partition in partitions}
    if not set(BENCHMARK_DATES).issubset(partition_dates):
        raise FeasibilityError("BENCHMARK_DATE_NOT_IN_SEARCH_FEATURE_PARTITIONS")
    if not partitions or max(partition.source_date for partition in partitions) != last_benchmark:
        raise FeasibilityError("FEATURE_PARTITION_CUTOFF_DRIFT")
    bars_by_timeframe = {
        timeframe: pipeline._load_stage_bars(root, partitions, timeframe)
        for timeframe in ml.TF_ORDER
    }
    state = SimpleNamespace(
        bars_by_timeframe=bars_by_timeframe,
        plan=SimpleNamespace(stage_key=STAGE_KEY),
    )
    series = pipeline._ml_bar_series(state)
    stage_dates = tuple(sorted({item.bar.source_date for item in bars_by_timeframe[300]}))
    rank_by_date = {value: index for index, value in enumerate(stage_dates)}
    benchmark_date_values = {date.fromisoformat(value) for value in BENCHMARK_DATES}
    items = tuple(
        sorted(
            (
                item
                for item in bars_by_timeframe[300]
                if item.bar.source_date in benchmark_date_values
                and (item.bar.end_ns // SECOND_NS) % 86_400 <= LATEST_DECISION_SECOND_UTC
            ),
            key=lambda item: (
                item.bar.end_ns,
                item.bar.contract,
                item.outcome_span_id,
                item.bar.segment_id,
            ),
        )
    )
    row_ids = tuple(_anchor_id(item) for item in items)
    schedule_sha256 = _sha256(
        {
            "row_ids": list(row_ids),
            "schema": "systematic_fx.bar_flow_interaction_entry_schedule.v1",
        }
    )
    anchors = ml.CausalAnchorRows(
        row_ids=row_ids,
        decision_ns=np.asarray([item.bar.end_ns for item in items], dtype=np.int64),
        entry_ns=np.asarray(
            [item.bar.end_ns + SECOND_NS for item in items],
            dtype=np.int64,
        ),
        source_dates=tuple(item.bar.source_date for item in items),
        contracts=tuple(item.bar.contract for item in items),
        outcome_span_ids=np.asarray(
            [item.outcome_span_id for item in items],
            dtype=np.int64,
        ),
        segment_ids=np.asarray(
            [item.bar.segment_id for item in items],
            dtype=np.uint64,
        ),
        stage_date_ranks=np.asarray(
            [rank_by_date[item.bar.source_date] for item in items],
            dtype=np.int64,
        ),
        stage_key=STAGE_KEY,
        decision_timeframe_seconds=300,
        entry_schedule_sha256=schedule_sha256,
    )
    rows90 = ml.build_causal_feature_rows(
        anchors=anchors,
        bars_by_timeframe=series,
        feature_set_id="FLOW_TIME_REGIME_90",
    )
    if rows90.feature_names[:66] != FEATURE66:
        raise FeasibilityError("FEATURE66_POSITION_DRIFT")
    matrix66 = np.asarray(rows90.values[:, :66], dtype=np.float64)
    if matrix66.shape != (rows90.row_count, 66) or not np.isfinite(matrix66).all():
        raise FeasibilityError("FEATURE66_MATRIX_INVALID")
    audit = {
        "anchor_count": len(items),
        "bar_counts": {
            str(timeframe): len(bars_by_timeframe[timeframe]) for timeframe in ml.TF_ORDER
        },
        "feature66_names_sha256": _sha256(list(FEATURE66)),
        "feature_rows_artifact_sha256": rows90.artifact_sha256,
        "feature_rows_count": rows90.row_count,
        "feature_source_commitment_sha256": rows90.source_commitment_sha256,
        "last_loaded_source_date": last_benchmark.isoformat(),
        "partition_count": len(partitions),
        "source_sha256": actual_hashes,
    }
    return rows90, matrix66, audit


def _product_key(contract: str) -> str:
    match = re.fullmatch(r"(.+?)[FGHJKMNQUVXZ][0-9]+", contract)
    if match is None or not match.group(1):
        raise FeasibilityError(f"CONTRACT_PRODUCT_KEY_INVALID_{contract}")
    return match.group(1)


def _perfect_matching(allowed: np.ndarray) -> tuple[int, ...] | None:
    graph = csr_matrix(allowed.astype(np.int8, copy=False))
    result = np.asarray(
        maximum_bipartite_matching(graph, perm_type="column"),
        dtype=np.int64,
    )
    count = allowed.shape[0]
    if result.shape != (count,) or np.any(result < 0) or set(result.tolist()) != set(range(count)):
        return None
    return tuple(int(value) for value in result)


def _validate_date_block_plan(
    geometry: DateBlockGeometry,
    destinations: tuple[int, ...],
    first_sources: tuple[int, ...],
    second_sources: tuple[int, ...],
) -> None:
    destination_set = set(destinations)
    if (
        len(destinations) != len(destination_set)
        or set(first_sources) != destination_set
        or set(second_sources) != destination_set
        or any(first == second for first, second in zip(first_sources, second_sources, strict=True))
    ):
        raise DateBlockError("DATE_BLOCK_BIJECTION_INVALID")
    minimum_separation = MIN_NULL_ANCHOR_SEPARATION_SECONDS * SECOND_NS
    for destination, first, second in zip(
        destinations,
        first_sources,
        second_sources,
        strict=True,
    ):
        for source in (first, second):
            if (
                destination == source
                or geometry.product_keys[destination] != geometry.product_keys[source]
                or geometry.source_dates[destination] == geometry.source_dates[source]
                or abs(geometry.planned_start_ns[destination] - geometry.planned_start_ns[source])
                < minimum_separation
            ):
                raise DateBlockError("DATE_BLOCK_EDGE_INVALID")


def date_block_derangements(
    geometry: DateBlockGeometry,
    training_indexes: Sequence[int],
) -> DateBlockPlan:
    raw = tuple(int(value) for value in training_indexes)
    if len(raw) != len(set(raw)):
        raise DateBlockError("DATE_BLOCK_TRAINING_INDEX_DUPLICATE")
    if any(value < 0 or value >= len(geometry.row_ids) for value in raw):
        raise DateBlockError("DATE_BLOCK_TRAINING_INDEX_OUT_OF_RANGE")
    destinations = tuple(
        sorted(
            raw,
            key=lambda index: (
                geometry.product_keys[index],
                geometry.source_dates[index],
                geometry.planned_start_ns[index],
                geometry.row_ids[index],
            ),
        )
    )
    count = len(destinations)
    if not 3 <= count <= MAX_DERANGEMENT_ROWS:
        raise DateBlockError("DATE_BLOCK_ROW_COUNT_OUT_OF_RANGE")
    products = np.asarray(
        [geometry.product_keys[index] for index in destinations],
        dtype=object,
    )
    dates = np.asarray(
        [geometry.source_dates[index] for index in destinations],
        dtype=object,
    )
    starts = np.asarray(
        [geometry.planned_start_ns[index] for index in destinations],
        dtype=np.int64,
    )
    separated = (
        np.abs(starts[:, None] - starts[None, :]) >= MIN_NULL_ANCHOR_SEPARATION_SECONDS * SECOND_NS
    )
    allowed = (
        (products[:, None] == products[None, :]) & (dates[:, None] != dates[None, :]) & separated
    )
    np.fill_diagonal(allowed, False)
    destination_degree = allowed.sum(axis=1)
    source_degree = allowed.sum(axis=0)
    if np.any(destination_degree < 2):
        raise DateBlockError("DATE_BLOCK_DESTINATION_DEGREE_LT_2")
    if np.any(source_degree < 2):
        raise DateBlockError("DATE_BLOCK_SOURCE_DEGREE_LT_2")
    first_local = _perfect_matching(allowed)
    if first_local is None:
        raise DateBlockError("DATE_BLOCK_FIRST_BIJECTION_INFEASIBLE")
    second_allowed = allowed.copy()
    second_allowed[np.arange(count), np.asarray(first_local)] = False
    second_local = _perfect_matching(second_allowed)
    if second_local is None:
        raise DateBlockError("DATE_BLOCK_SECOND_BIJECTION_INFEASIBLE")
    first_sources = tuple(destinations[index] for index in first_local)
    second_sources = tuple(destinations[index] for index in second_local)
    _validate_date_block_plan(geometry, destinations, first_sources, second_sources)
    document = {
        "destinations": [geometry.row_ids[index] for index in destinations],
        "first_sources": [geometry.row_ids[index] for index in first_sources],
        "schema": "systematic_fx.date_block_derangement.v1",
        "second_sources": [geometry.row_ids[index] for index in second_sources],
    }
    return DateBlockPlan(
        destinations,
        first_sources,
        second_sources,
        int(destination_degree.min()),
        int(source_degree.min()),
        _sha256(document),
    )


def _synthetic_geometry() -> DateBlockGeometry:
    date_sizes = (61, 67, 65, 70, 65, 91, 93, 94, 94, 94, 95, 155, 156)
    rows: list[tuple[str, str, date, int, int]] = []
    for date_index, count in enumerate(date_sizes):
        synthetic_date = date.fromordinal(date(2020, 1, 1).toordinal() + date_index * 2)
        midnight_ns = (
            (synthetic_date.toordinal() - date(1970, 1, 1).toordinal()) * 86_400 * SECOND_NS
        )
        for row_index in range(count):
            start = midnight_ns + (8 * 3_600 + row_index * 300) * SECOND_NS
            row_id = _sha256(
                {
                    "date_index": date_index,
                    "row_index": row_index,
                    "schema": "synthetic.date_block_row.v1",
                }
            )
            rows.append(
                (
                    row_id,
                    "6E",
                    synthetic_date,
                    start,
                    start + HORIZON_SECONDS * SECOND_NS,
                )
            )
    return DateBlockGeometry(
        tuple(value[0] for value in rows),
        tuple(value[1] for value in rows),
        tuple(value[2] for value in rows),
        tuple(value[3] for value in rows),
        tuple(value[4] for value in rows),
    )


def _synthetic_hgb_regression() -> dict[str, float]:
    rng = np.random.default_rng(1729)
    train_count = 1_200
    test_count = 400
    total = train_count + test_count
    price = rng.normal(size=total)
    flow = rng.normal(size=total)
    noise = rng.normal(scale=0.08, size=total)
    interaction_target = price * flow + noise
    independent_increment_target = price + noise
    price_matrix = price.reshape(-1, 1)
    augmented_matrix = np.column_stack((price, flow))
    geometry = _synthetic_geometry()
    training_indexes = tuple(range(train_count))
    null_plan = date_block_derangements(geometry, training_indexes)
    if null_plan.destinations != training_indexes:
        raise FeasibilityError("SYNTHETIC_NULL_DESTINATION_ORDER_DRIFT")

    parameters = {
        "early_stopping": False,
        "l2_regularization": 1.0,
        "learning_rate": 0.05,
        "loss": "squared_error",
        "max_bins": 255,
        "max_iter": 200,
        "max_leaf_nodes": 7,
        "min_samples_leaf": 40,
        "random_state": 1729,
    }

    def prediction(matrix: np.ndarray, target: np.ndarray) -> np.ndarray:
        return (
            HistGradientBoostingRegressor(**parameters)
            .fit(
                matrix[:train_count],
                target[:train_count],
            )
            .predict(matrix[train_count:])
        )

    price_prediction = prediction(price_matrix, interaction_target)
    real_prediction = prediction(augmented_matrix, interaction_target)
    first_null_target = np.asarray(
        [interaction_target[index] for index in null_plan.first_sources],
        dtype=np.float64,
    )
    second_null_target = np.asarray(
        [interaction_target[index] for index in null_plan.second_sources],
        dtype=np.float64,
    )
    first_null_model = HistGradientBoostingRegressor(**parameters).fit(
        augmented_matrix[:train_count],
        first_null_target,
    )
    second_null_model = HistGradientBoostingRegressor(**parameters).fit(
        augmented_matrix[:train_count],
        second_null_target,
    )
    interaction_test = interaction_target[train_count:]
    price_mse = float(mean_squared_error(interaction_test, price_prediction))
    real_mse = float(mean_squared_error(interaction_test, real_prediction))
    first_null_mse = float(
        mean_squared_error(
            interaction_test,
            first_null_model.predict(augmented_matrix[train_count:]),
        )
    )
    second_null_mse = float(
        mean_squared_error(
            interaction_test,
            second_null_model.predict(augmented_matrix[train_count:]),
        )
    )
    control_price_mse = float(
        mean_squared_error(
            independent_increment_target[train_count:],
            prediction(price_matrix, independent_increment_target),
        )
    )
    control_augmented_mse = float(
        mean_squared_error(
            independent_increment_target[train_count:],
            prediction(augmented_matrix, independent_increment_target),
        )
    )
    interaction_improvement = (price_mse - real_mse) / price_mse
    control_increment = (control_price_mse - control_augmented_mse) / control_price_mse
    if (
        interaction_improvement < 0.50
        or real_mse >= 0.50 * min(first_null_mse, second_null_mse)
        or abs(control_increment) > 0.05
    ):
        raise FeasibilityError("SYNTHETIC_HGB_REGRESSION_FAILED")
    return {
        "control_augmented_mse": control_augmented_mse,
        "control_increment": control_increment,
        "control_price_mse": control_price_mse,
        "interaction_augmented_mse": real_mse,
        "interaction_improvement": interaction_improvement,
        "interaction_null1_mse": first_null_mse,
        "interaction_null2_mse": second_null_mse,
        "interaction_price_mse": price_mse,
    }


def _synthetic_regressions() -> dict[str, object]:
    geometry = _synthetic_geometry()
    prefix_counts = EXPECTED_NULL_TRAIN_COUNTS
    hashes = []
    for count in prefix_counts:
        forward = date_block_derangements(geometry, tuple(range(count)))
        reversed_input = date_block_derangements(geometry, tuple(reversed(range(count))))
        if forward.sha256 != reversed_input.sha256:
            raise FeasibilityError("DATE_BLOCK_INPUT_ORDER_DEPENDENT")
        hashes.append(forward.sha256)

    dominant_span_count = 200
    other_span_count = 100
    if 2 * max(dominant_span_count, other_span_count) <= (dominant_span_count + other_span_count):
        raise FeasibilityError("OLD_NULL_FAILURE_FIXTURE_INVALID")

    too_close = DateBlockGeometry(
        ("a", "b", "c"),
        ("6E", "6E", "6E"),
        (date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)),
        (SECOND_NS, 2 * SECOND_NS, 3 * SECOND_NS),
        (
            (1 + HORIZON_SECONDS) * SECOND_NS,
            (2 + HORIZON_SECONDS) * SECOND_NS,
            (3 + HORIZON_SECONDS) * SECOND_NS,
        ),
    )
    try:
        date_block_derangements(too_close, (0, 1, 2))
    except DateBlockError as error:
        if str(error) != "DATE_BLOCK_DESTINATION_DEGREE_LT_2":
            raise
    else:  # pragma: no cover - fail-closed synthetic invariant
        raise FeasibilityError("DATE_BLOCK_TOO_CLOSE_FIXTURE_PASSED")

    return {
        "date_block_plan_sha256s": hashes,
        "hgb": _synthetic_hgb_regression(),
        "old_exact_contract_span_fixture": "INFEASIBLE_AS_EXPECTED",
        "too_close_fixture": "INFEASIBLE_AS_EXPECTED",
    }


def _feature_diagnostics(
    rows90: ml.CausalFeatureRows,
    matrix66: np.ndarray,
    structural: StructuralRows,
) -> tuple[DateBlockGeometry, tuple[int, ...], dict[str, object]]:
    paired_indexes = tuple(
        index
        for index, (source_date, contract, decision_ns) in enumerate(
            zip(rows90.source_dates, rows90.contracts, rows90.decision_ns, strict=True)
        )
        if (source_date.isoformat(), contract, int(decision_ns)) in structural.intersection_keys
    )
    if not paired_indexes:
        raise FeasibilityError("FEATURE_STRUCTURAL_INTERSECTION_EMPTY")
    paired_counts = tuple(
        sum(
            1
            for index in paired_indexes
            if rows90.source_dates[index].isoformat() in BENCHMARK_DATES[start:stop]
        )
        for start, stop in DATE_BLOCK_SLICES
    )
    history_feature_count = sum(
        1
        for index, (source_date, contract, decision_ns) in enumerate(
            zip(rows90.source_dates, rows90.contracts, rows90.decision_ns, strict=True)
        )
        if (source_date.isoformat(), contract, int(decision_ns)) in structural.history_keys
    )
    paired_nonoverlap = tuple(
        _greedy_nonoverlap_count(
            [
                (
                    rows90.source_dates[index].isoformat(),
                    rows90.contracts[index],
                    int(rows90.decision_ns[index]),
                )
                for index in paired_indexes
                if rows90.source_dates[index].isoformat() in BENCHMARK_DATES[start:stop]
            ]
        )
        for start, stop in DATE_BLOCK_SLICES
    )
    if paired_counts[0] < MIN_FEATURE_TRAIN_ROWS or any(
        value < MIN_FEATURE_TEST_ROWS for value in paired_counts[1:]
    ):
        raise FeasibilityError("FEATURE_ROW_SUPPORT_INSUFFICIENT")
    if (
        any(value < MIN_TEST_NONOVERLAP_ROWS for value in paired_nonoverlap[1:])
        or sum(paired_nonoverlap[1:]) < MIN_TOTAL_TEST_NONOVERLAP_ROWS
    ):
        raise FeasibilityError("FEATURE_NONOVERLAP_SUPPORT_INSUFFICIENT")
    selected_matrix = matrix66[np.asarray(paired_indexes, dtype=np.int64)]
    unique_counts = tuple(len(np.unique(selected_matrix[:, column])) for column in range(66))
    timeframe_variation: dict[str, dict[str, int]] = {}
    for timeframe in ml.TF_ORDER:
        prefix = f"tf{timeframe:04d}_"
        price_positions = [
            index
            for index, name in enumerate(FEATURE66)
            if name.startswith(prefix) and name in ml.PRICE_FEATURE_NAMES
        ]
        flow_positions = [
            index
            for index, name in enumerate(FEATURE66)
            if name.startswith(prefix) and name in ml.FLOW_FEATURE_NAMES
        ]
        price_nonconstant = sum(unique_counts[index] > 1 for index in price_positions)
        flow_nonconstant = sum(unique_counts[index] > 1 for index in flow_positions)
        if price_nonconstant < 1 or flow_nonconstant < 1:
            raise FeasibilityError("FEATURE_TIMEFRAME_VARIATION_MISSING")
        timeframe_variation[str(timeframe)] = {
            "flow_nonconstant": flow_nonconstant,
            "price_nonconstant": price_nonconstant,
        }

    row_ids = tuple(rows90.row_ids[index] for index in paired_indexes)
    source_dates = tuple(rows90.source_dates[index] for index in paired_indexes)
    starts = tuple(int(rows90.entry_ns[index]) for index in paired_indexes)
    geometry = DateBlockGeometry(
        row_ids=row_ids,
        product_keys=tuple(_product_key(rows90.contracts[index]) for index in paired_indexes),
        source_dates=source_dates,
        planned_start_ns=starts,
        planned_end_ns=tuple(value + HORIZON_SECONDS * SECOND_NS for value in starts),
    )
    diagnostics = {
        "buy_sell_available_mean": {
            str(timeframe): float(
                np.mean(
                    selected_matrix[
                        :,
                        FEATURE66.index(f"tf{timeframe:04d}_buy_sell_available"),
                    ]
                )
            )
            for timeframe in ml.TF_ORDER
        },
        "feature_complete_counts": list(paired_counts),
        "feature_complete_nonoverlap": list(paired_nonoverlap),
        "feature_complete_total": len(paired_indexes),
        "history_feature_complete_total": history_feature_count,
        "minimum_feature_unique_count": min(unique_counts),
        "timeframe_variation": timeframe_variation,
        "unique_counts_sha256": _sha256(list(unique_counts)),
    }
    return geometry, paired_indexes, diagnostics


def _actual_null_plans(
    geometry: DateBlockGeometry,
) -> tuple[dict[str, object], ...]:
    output = []
    for fold_index, train_stop in enumerate((5, 8, 11), start=1):
        allowed_dates = {date.fromisoformat(value) for value in BENCHMARK_DATES[:train_stop]}
        indexes = tuple(
            index
            for index, source_date in enumerate(geometry.source_dates)
            if source_date in allowed_dates
        )
        if len(indexes) != EXPECTED_NULL_TRAIN_COUNTS[fold_index - 1]:
            raise FeasibilityError("DATE_BLOCK_TRAIN_ROW_COUNT_DRIFT")
        plan = date_block_derangements(geometry, indexes)
        reversed_plan = date_block_derangements(geometry, tuple(reversed(indexes)))
        if plan.sha256 != reversed_plan.sha256:
            raise FeasibilityError("DATE_BLOCK_ACTUAL_INPUT_ORDER_DEPENDENT")
        output.append(
            {
                "fold": fold_index,
                "minimum_destination_degree": plan.minimum_destination_degree,
                "minimum_source_degree": plan.minimum_source_degree,
                "plan_sha256": plan.sha256,
                "training_rows": len(indexes),
            }
        )
    return tuple(output)


def _plan_document() -> dict[str, object]:
    return {
        "actual_model_fit_count": 0,
        "benchmark_dates": list(BENCHMARK_DATES),
        "feature_names": list(FEATURE66),
        "future_capacity_is_not_a_scoring_mask": True,
        "horizon_seconds": HORIZON_SECONDS,
        "max_rss_bytes": MAX_RSS_BYTES,
        "max_runtime_seconds": MAX_RUNTIME_SECONDS,
        "minimum_feature_test_rows": MIN_FEATURE_TEST_ROWS,
        "minimum_feature_train_rows": MIN_FEATURE_TRAIN_ROWS,
        "minimum_test_nonoverlap_rows": MIN_TEST_NONOVERLAP_ROWS,
        "minimum_total_test_nonoverlap_rows": MIN_TOTAL_TEST_NONOVERLAP_ROWS,
        "minimum_null_anchor_separation_seconds": MIN_NULL_ANCHOR_SEPARATION_SECONDS,
        "null_kind": "TWO_EDGE_DISJOINT_DATE_BLOCK_PERFECT_MATCHINGS",
        "read_scope": [
            "FULL_FROZEN_DATASET_MANIFEST_METADATA_AND_CALENDAR",
            "TRADE_BAR_ARTIFACT_VALUES_THROUGH_LAST_BENCHMARK_ONLY",
            "BENCHMARK14_STRUCTURAL_ARTIFACT_BYTES_HASHED_ONLY_STRUCTURAL_COLUMNS_DECODED",
            "FROZEN_ALL_CASES_FEATURE_SOURCE",
        ],
        "schema": "systematic_fx.bar_flow_interaction_ml_feasibility_plan.v1",
    }


def _run(project_root: Path) -> dict[str, object]:
    started = monotonic()
    structural = _structural_rows(project_root)
    _check_resource_budget(started)
    rows90, matrix66, feature_audit = _causal_feature_rows(project_root)
    _check_resource_budget(started)
    geometry, _paired_indexes, feature_diagnostics = _feature_diagnostics(
        rows90,
        matrix66,
        structural,
    )
    _check_resource_budget(started)
    null_plans = _actual_null_plans(geometry)
    _check_resource_budget(started)
    synthetic = _synthetic_regressions()
    _check_resource_budget(started)
    elapsed = monotonic() - started
    peak_rss = _peak_rss_bytes()
    if elapsed > MAX_RUNTIME_SECONDS:
        raise FeasibilityError("STOP_TIMEOUT")
    if peak_rss > MAX_RSS_BYTES:
        raise FeasibilityError("STOP_RSS_CAP")
    plan = _plan_document()
    return {
        "decision": "GO_FEATURE_AND_NULL_FEASIBILITY_ONLY",
        "elapsed_seconds": elapsed,
        "feature_audit": feature_audit,
        "feature_diagnostics": feature_diagnostics,
        "static_forbidden_reader_audit": "PASS",
        "null_plans": list(null_plans),
        "outcome_model_fit_count": 0,
        "peak_rss_bytes": peak_rss,
        "plan": plan,
        "plan_sha256": _sha256(plan),
        "platform": platform.platform(),
        "schema": SCHEMA,
        "structural": {
            "artifact_bindings_sha256": _sha256(list(structural.artifact_bindings)),
            "future_counts": list(structural.future_counts),
            "history_counts": list(structural.history_counts),
            "intersection_counts": list(structural.intersection_counts),
            "intersection_total": len(structural.intersection_keys),
            "nonoverlap_counts": list(structural.nonoverlap_counts),
        },
        "synthetic_regressions": synthetic,
    }


def run(project_root: Path) -> dict[str, object]:
    previous_handler = signal.signal(signal.SIGALRM, _timeout_handler)
    signal.setitimer(signal.ITIMER_REAL, MAX_RUNTIME_SECONDS)
    try:
        return _run(project_root)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, previous_handler)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    args = parser.parse_args()
    root = args.project_root.resolve()
    try:
        result = run(root)
    except (
        FeasibilityError,
        ml.AllCasesMLError,
        pipeline.AllCasesPipelineError,
    ) as error:
        failure = {
            "decision": "STOP",
            "error": str(error),
            "outcome_model_fit_count": 0,
            "schema": SCHEMA,
        }
        print(_canonical_json_bytes(failure).decode("ascii"))
        return 1
    print(_canonical_json_bytes(result).decode("ascii"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
