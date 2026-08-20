"""Small retrospective test of multiscale bar-price x trade-flow interaction.

This is a deliberately bounded successor to
``bar_flow_interaction_ml_feasibility.py``.  It evaluates one previously
unexecuted Direct-ML representation (PRICE36 versus PRICE36 + FLOW30) at one
60-minute horizon on the frozen fourteen-date benchmark.  Every test-fold
prediction and planned, non-overlapping signal schedule is hashed before that
fold's execution-cache values are opened.

The result is screening evidence only.  It does not open Search, walk-forward,
holdout, or any later partition and it never creates an execution cache.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import resource
import signal
import sys
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from time import monotonic
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
import pyarrow.parquet as pq
from scipy.stats import spearmanr
from sklearn.ensemble import HistGradientBoostingRegressor

_IMPORT_ROOT = Path(__file__).resolve().parents[1]
if str(_IMPORT_ROOT) not in sys.path:
    sys.path.insert(0, str(_IMPORT_ROOT))

from scripts import bar_flow_interaction_ml_feasibility as feature_gate
from scripts import flow_acceptance_markout_preflight as execution_base

SECOND_NS: Final = 1_000_000_000
HORIZON_SECONDS: Final = 3_600
ROUTING_DELAY_NS: Final = SECOND_NS
MAX_ATTEMPT_WAIT_NS: Final = SECOND_NS
MAX_RUNTIME_SECONDS: Final = 600.0
MAX_RSS_BYTES: Final = 4 * 1024**3
PRICE_FEATURE_COUNT: Final = 36
FLOW_FEATURE_COUNT: Final = 30
ACTION_RATE: Final = 0.10
MODEL_RANDOM_STATE: Final = 1_729
SCHEMA: Final = "systematic_fx.bar_flow_interaction_actual_preflight.v1"
STAGE_KEY: Final = "BAR_FLOW_INTERACTION_ACTUAL_60M_V1"

EXPECTED_FEATURE_GATE_SHA256: Final = (
    "7ae10b975ff1fd0ffdab42dcf66eddebba7e0079750b4cf590baf970406c67f0"
)
EXPECTED_EXECUTION_BASE_SHA256: Final = (
    "ddcb86fab1bd855f5b5560433903392c1b8eb848c67d941832f6d9df31559862"
)
EXPECTED_HISTORY_FEATURE_COUNTS: Final = (429, 315, 323, 277)
EXPECTED_LABEL_LATTICE_COUNTS: Final = (328, 278, 283, 232)
MIN_TRAIN_LABEL_ROWS: Final = (300, 550, 800)
MIN_TEST_IC_ROWS: Final = 200

# At the official 10% absolute admission rate, the three test folds contain
# 315/323/277 causal rows.  These are deliberately modest screening minima;
# every economic and null gate below must also pass.
MIN_REAL_FLOW_CANDIDATES_PER_FOLD: Final = 20
MIN_REAL_FLOW_COMPLETED_PER_FOLD: Final = 6
MIN_REAL_FLOW_COMPLETED_TOTAL: Final = 21
MIN_REAL_FLOW_ACTIVE_DATES: Final = 7
MIN_REAL_FLOW_SIDE_COMPLETED: Final = 5
MIN_CONTROL_COMPLETED_PER_FOLD: Final = 5
MIN_CONTROL_COMPLETED_TOTAL: Final = 18
MAX_ENTRY_NO_FILL_FRACTION: Final = 0.20

BENCHMARK_DATES: Final = feature_gate.BENCHMARK_DATES
DATE_BLOCK_SLICES: Final = feature_gate.DATE_BLOCK_SLICES
FEATURE66: Final = feature_gate.FEATURE66
MODEL_KEYS: Final = (
    "REAL_PRICE",
    "REAL_FLOW",
    "NULL1_PRICE",
    "NULL1_FLOW",
    "NULL2_PRICE",
    "NULL2_FLOW",
)

HGB_PARAMETERS: Final = {
    "early_stopping": False,
    "l2_regularization": 1.0,
    "learning_rate": 0.05,
    "loss": "squared_error",
    "max_bins": 255,
    "max_iter": 200,
    "max_leaf_nodes": 7,
    "min_samples_leaf": 40,
    "random_state": MODEL_RANDOM_STATE,
}


class ActualPreflightError(RuntimeError):
    """Fail-closed actual-preflight result."""


@dataclass(frozen=True, slots=True)
class Attempt:
    snapshot_position: int
    position: int
    event_index: int
    ts_ns: int
    bid_ticks: int
    ask_ticks: int


@dataclass(frozen=True, slots=True)
class RouteView:
    event_index: np.ndarray
    ts_ns: np.ndarray
    bid_ticks: np.ndarray
    ask_ticks: np.ndarray
    valid: np.ndarray
    invalid_prefix: np.ndarray
    gap_prefix: np.ndarray


@dataclass(frozen=True, slots=True)
class RouteOutcome:
    entry_reason: str
    exit_reason: str
    entry_fill_ns: int | None = None
    exit_fill_ns: int | None = None
    entry_event_index: int | None = None
    exit_event_index: int | None = None
    entry_bid_ticks: int | None = None
    entry_ask_ticks: int | None = None
    exit_bid_ticks: int | None = None
    exit_ask_ticks: int | None = None

    @property
    def entry_filled(self) -> bool:
        return self.entry_reason == "FILLED"

    @property
    def completed(self) -> bool:
        return self.entry_filled and self.exit_reason == "FILLED"

    @property
    def long_gross(self) -> float | None:
        if not self.completed:
            return None
        assert self.exit_bid_ticks is not None and self.entry_ask_ticks is not None
        return float(self.exit_bid_ticks - self.entry_ask_ticks)

    @property
    def short_gross(self) -> float | None:
        if not self.completed:
            return None
        assert self.entry_bid_ticks is not None and self.exit_ask_ticks is not None
        return float(self.entry_bid_ticks - self.exit_ask_ticks)

    @property
    def midpoint_move_ticks(self) -> float | None:
        if not self.completed:
            return None
        values = (
            self.entry_bid_ticks,
            self.entry_ask_ticks,
            self.exit_bid_ticks,
            self.exit_ask_ticks,
        )
        assert all(value is not None for value in values)
        entry_bid, entry_ask, exit_bid, exit_ask = (int(value) for value in values)
        result = ((exit_bid + exit_ask) - (entry_bid + entry_ask)) / 2.0
        if not math.isclose(
            result,
            (float(self.long_gross) - float(self.short_gross)) / 2.0,
            rel_tol=0.0,
            abs_tol=0.0,
        ):
            raise ActualPreflightError("ROUTED_TARGET_IDENTITY_DRIFT")
        return result


@dataclass(frozen=True, slots=True)
class PlannedSignal:
    row_index: int
    row_id: str
    source_date: str
    decision_ns: int
    direction: int
    prediction: float


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


def _float_hex(values: Sequence[float] | np.ndarray) -> list[str]:
    output = []
    for value in values:
        item = float(value)
        if not math.isfinite(item):
            raise ActualPreflightError("NONFINITE_FLOAT_IN_FROZEN_DOCUMENT")
        output.append(item.hex())
    return output


def _peak_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1024


def _check_budget(started: float) -> None:
    if monotonic() - started > MAX_RUNTIME_SECONDS:
        raise ActualPreflightError("STOP_TIMEOUT")
    if _peak_rss_bytes() > MAX_RSS_BYTES:
        raise ActualPreflightError("STOP_RSS_CAP")


def _timeout_handler(_signum: int, _frame: object) -> None:
    raise ActualPreflightError("STOP_TIMEOUT")


def _numpy_column(table: object, name: str) -> np.ndarray:
    return np.asarray(table[name].combine_chunks().to_numpy(zero_copy_only=False))


def _load_route_view(cache: execution_base.ExecutionCache) -> RouteView:
    wanted = (
        "event_index",
        "ts_recv_ns",
        "best_bid_ticks",
        "best_ask_ticks",
        "valid",
        "sequence",
        "source_row_index",
    )
    table = pq.ParquetFile(cache.path).read(columns=list(wanted), use_threads=True)
    if table.num_rows != cache.cached_quote_count:
        raise ActualPreflightError("EXECUTION_CACHE_ROW_COUNT_CHANGED")
    event_index = _numpy_column(table, "event_index").astype(np.int64)
    ts_ns = _numpy_column(table, "ts_recv_ns").astype(np.int64)
    bid_ticks = np.asarray(table["best_bid_ticks"].combine_chunks().to_pylist(), dtype=np.float64)
    ask_ticks = np.asarray(table["best_ask_ticks"].combine_chunks().to_pylist(), dtype=np.float64)
    valid = np.asarray(table["valid"].combine_chunks().to_pylist(), dtype=np.bool_)
    sequence = _numpy_column(table, "sequence").astype(np.uint64)
    source_row_index = _numpy_column(table, "source_row_index").astype(np.int64)
    if (
        len(ts_ns) == 0
        or np.any(np.diff(event_index) <= 0)
        or np.any(np.diff(ts_ns) < 0)
        or np.any(event_index - cache.event_index_offset != source_row_index)
        or np.any(sequence > np.iinfo(np.uint32).max)
    ):
        raise ActualPreflightError("EXECUTION_CACHE_ORDER_OR_LINEAGE_DRIFT")
    valid_quotes = (
        np.isfinite(bid_ticks)
        & np.isfinite(ask_ticks)
        & (bid_ticks == np.floor(bid_ticks))
        & (ask_ticks == np.floor(ask_ticks))
        & (bid_ticks < ask_ticks)
    )
    if np.any(valid & ~valid_quotes):
        raise ActualPreflightError("EXECUTION_CACHE_INVALID_EXECUTABLE_BBO")
    invalid_prefix = np.r_[0, np.cumsum((~valid).astype(np.int64))]
    gap_invalid = np.zeros(len(ts_ns), dtype=np.bool_)
    gap_invalid[1:] = np.diff(ts_ns) > SECOND_NS
    gap_prefix = np.r_[0, np.cumsum(gap_invalid.astype(np.int64))]
    return RouteView(
        event_index=event_index,
        ts_ns=ts_ns,
        bid_ticks=bid_ticks,
        ask_ticks=ask_ticks,
        valid=valid,
        invalid_prefix=invalid_prefix,
        gap_prefix=gap_prefix,
    )


def _prefix_range_is_zero(prefix: np.ndarray, first: int, last: int) -> bool:
    if first > last:
        return True
    return int(prefix[last + 1] - prefix[first]) == 0


def _routed_attempt(view: RouteView, decision_ns: int) -> tuple[Attempt | None, str]:
    decision_position = int(np.searchsorted(view.ts_ns, decision_ns, side="right")) - 1
    if decision_position < 0 or not bool(view.valid[decision_position]):
        return None, "INVALID_DECISION_BBO"
    if decision_ns - int(view.ts_ns[decision_position]) > SECOND_NS:
        return None, "STALE_DECISION_BBO"
    eligibility_ns = decision_ns + ROUTING_DELAY_NS
    attempt_position = int(np.searchsorted(view.ts_ns, eligibility_ns, side="left"))
    if attempt_position >= len(view.ts_ns):
        return None, "NO_ATTEMPT_EVENT"
    if int(view.ts_ns[attempt_position]) - eligibility_ns > MAX_ATTEMPT_WAIT_NS:
        return None, "ATTEMPT_WAIT_GT_1S"
    snapshot_position = (
        attempt_position
        if int(view.ts_ns[attempt_position]) == eligibility_ns
        else attempt_position - 1
    )
    if snapshot_position < decision_position:
        return None, "ELIGIBILITY_PRECEDES_DECISION"
    if eligibility_ns - int(view.ts_ns[snapshot_position]) > SECOND_NS:
        return None, "STALE_ELIGIBILITY_BBO"
    if not _prefix_range_is_zero(
        view.invalid_prefix,
        decision_position + 1,
        attempt_position,
    ) or not _prefix_range_is_zero(
        view.gap_prefix,
        decision_position + 1,
        snapshot_position,
    ):
        return None, "INVALID_OR_STALE_ROUTE"
    return (
        Attempt(
            snapshot_position=snapshot_position,
            position=attempt_position,
            event_index=int(view.event_index[attempt_position]),
            ts_ns=int(view.ts_ns[attempt_position]),
            bid_ticks=int(view.bid_ticks[attempt_position]),
            ask_ticks=int(view.ask_ticks[attempt_position]),
        ),
        "FILLED",
    )


def _route_60m(view: RouteView, decision_ns: int) -> RouteOutcome:
    entry, entry_reason = _routed_attempt(view, decision_ns)
    if entry is None:
        return RouteOutcome(entry_reason=entry_reason, exit_reason="NOT_ATTEMPTED")
    exit_attempt, exit_reason = _routed_attempt(
        view,
        decision_ns + HORIZON_SECONDS * SECOND_NS,
    )
    if exit_attempt is None:
        return RouteOutcome(
            entry_reason="FILLED",
            exit_reason=exit_reason,
            entry_fill_ns=entry.ts_ns,
            entry_event_index=entry.event_index,
            entry_bid_ticks=entry.bid_ticks,
            entry_ask_ticks=entry.ask_ticks,
        )
    if exit_attempt.event_index <= entry.event_index or exit_attempt.ts_ns <= entry.ts_ns:
        raise ActualPreflightError("EXECUTION_INTERVAL_NOT_STRICTLY_POSITIVE")
    return RouteOutcome(
        entry_reason="FILLED",
        exit_reason="FILLED",
        entry_fill_ns=entry.ts_ns,
        exit_fill_ns=exit_attempt.ts_ns,
        entry_event_index=entry.event_index,
        exit_event_index=exit_attempt.event_index,
        entry_bid_ticks=entry.bid_ticks,
        entry_ask_ticks=entry.ask_ticks,
        exit_bid_ticks=exit_attempt.bid_ticks,
        exit_ask_ticks=exit_attempt.ask_ticks,
    )


def _synthetic_route_regressions() -> dict[str, object]:
    decision = 10 * SECOND_NS
    target = decision + HORIZON_SECONDS * SECOND_NS
    ts_ns = np.asarray(
        [decision, decision + SECOND_NS, target, target + SECOND_NS],
        dtype=np.int64,
    )
    event_index = np.arange(100, 104, dtype=np.int64)

    def view_with(valid: Sequence[bool], *, final_delay_ns: int = 0) -> RouteView:
        local_ts = ts_ns.copy()
        local_ts[-1] += final_delay_ns
        validity = np.asarray(valid, dtype=np.bool_)
        invalid_prefix = np.r_[0, np.cumsum((~validity).astype(np.int64))]
        gaps = np.zeros(len(local_ts), dtype=np.bool_)
        gaps[1:] = np.diff(local_ts) > SECOND_NS
        return RouteView(
            event_index=event_index,
            ts_ns=local_ts,
            bid_ticks=np.asarray([99.0, 100.0, 109.0, 110.0]),
            ask_ticks=np.asarray([101.0, 102.0, 111.0, 112.0]),
            valid=validity,
            invalid_prefix=invalid_prefix,
            gap_prefix=np.r_[0, np.cumsum(gaps.astype(np.int64))],
        )

    completed = _route_60m(view_with((True, True, True, True)), decision)
    if (
        not completed.completed
        or completed.long_gross != 8.0
        or completed.short_gross != -12.0
        or completed.midpoint_move_ticks != 10.0
    ):
        raise ActualPreflightError("SYNTHETIC_ROUTE_GROSS_FAILED")
    invalid_first = _route_60m(view_with((True, False, True, True)), decision)
    if invalid_first.entry_reason != "INVALID_OR_STALE_ROUTE" or invalid_first.entry_filled:
        raise ActualPreflightError("SYNTHETIC_INVALID_FIRST_EVENT_CHASED")
    late_exit = _route_60m(
        view_with((True, True, True, True), final_delay_ns=SECOND_NS + 1),
        decision,
    )
    if late_exit.exit_reason != "ATTEMPT_WAIT_GT_1S":
        raise ActualPreflightError("SYNTHETIC_ATTEMPT_WAIT_BOUNDARY_FAILED")
    return {
        "exact_d_plus_1_and_d_plus_3600_plus_1": "PASS",
        "gross_and_midpoint_identity": "PASS",
        "invalid_first_event_no_chase": "PASS",
        "wait_over_one_second_rejected": "PASS",
    }


class SequentialOutcomeAccessor:
    """Open benchmark execution values only after each fold schedule is frozen."""

    def __init__(
        self,
        root: Path,
        requests: dict[str, execution_base.ExecutionCacheRequest],
        selections: dict[str, execution_base.Selection],
        decision_indexes_by_date: dict[str, tuple[tuple[int, int], ...]],
    ) -> None:
        self.root = root
        self.requests = requests
        self.selections = selections
        self.decision_indexes_by_date = decision_indexes_by_date
        self.opened_dates: list[str] = []
        self.outcomes: dict[int, RouteOutcome] = {}
        self.bindings: list[dict[str, object]] = []
        self.route_reasons: dict[str, dict[str, int]] = {}

    def _open(self, dates: Sequence[str]) -> None:
        chosen = tuple(dates)
        if any(value in self.opened_dates for value in chosen):
            raise ActualPreflightError("EXECUTION_DATE_REOPENED")
        request_subset = {value: self.requests[value] for value in chosen}
        selection_subset = {value: self.selections[value] for value in chosen}
        caches, missing = execution_base._load_execution_caches(
            self.root,
            request_subset,
            selection_subset,
            verify_content=True,
        )
        if missing or set(caches) != set(chosen):
            raise ActualPreflightError("BENCHMARK_EXECUTION_CACHE_MISSING")
        for source_date in chosen:
            cache = caches[source_date]
            view = _load_route_view(cache)
            reasons: dict[str, int] = defaultdict(int)
            for row_index, decision_ns in self.decision_indexes_by_date[source_date]:
                outcome = _route_60m(view, decision_ns)
                if row_index in self.outcomes:
                    raise ActualPreflightError("EXECUTION_OUTCOME_DUPLICATE")
                self.outcomes[row_index] = outcome
                reasons[f"ENTRY_{outcome.entry_reason}"] += 1
                reasons[f"EXIT_{outcome.exit_reason}"] += 1
            self.bindings.append(
                {
                    "byte_size": cache.byte_size,
                    "cached_quote_count": cache.cached_quote_count,
                    "sha256": cache.sha256,
                    "source_date": source_date,
                }
            )
            self.route_reasons[source_date] = dict(sorted(reasons.items()))
            del view
        self.opened_dates.extend(chosen)

    def open_initial_training(self) -> None:
        expected = tuple(BENCHMARK_DATES[:5])
        if self.opened_dates:
            raise ActualPreflightError("INITIAL_OUTCOME_ACCESS_NOT_FIRST")
        self._open(expected)
        if tuple(self.opened_dates) != expected:
            raise ActualPreflightError("INITIAL_OUTCOME_DATE_ORDER_DRIFT")

    def open_test_after_freeze(
        self,
        *,
        train_stop: int,
        test_stop: int,
        freeze_sha256: str,
    ) -> None:
        if (
            len(freeze_sha256) != 64
            or any(character not in "0123456789abcdef" for character in freeze_sha256)
            or tuple(self.opened_dates) != tuple(BENCHMARK_DATES[:train_stop])
        ):
            raise ActualPreflightError("TEST_OUTCOME_OPENED_BEFORE_PREDICTION_FREEZE")
        self._open(BENCHMARK_DATES[train_stop:test_stop])
        if tuple(self.opened_dates) != tuple(BENCHMARK_DATES[:test_stop]):
            raise ActualPreflightError("SEQUENTIAL_OUTCOME_DATE_ORDER_DRIFT")


def _model() -> HistGradientBoostingRegressor:
    return HistGradientBoostingRegressor(**HGB_PARAMETERS)


def _safe_spearman(target: np.ndarray, prediction: np.ndarray) -> float | None:
    if len(target) < 2 or len(target) != len(prediction):
        return None
    result = float(spearmanr(target, prediction).statistic)
    return result if math.isfinite(result) else None


def _target_for_row(
    index: int,
    *,
    outcomes: dict[int, RouteOutcome],
    atr_ticks: np.ndarray,
) -> float | None:
    outcome = outcomes.get(index)
    if outcome is None or not outcome.completed:
        return None
    atr = float(atr_ticks[index])
    move = outcome.midpoint_move_ticks
    if move is None or not math.isfinite(atr) or atr <= 0:
        return None
    result = float(move) / atr
    return result if math.isfinite(result) else None


def _target_is_available(
    index: int,
    *,
    outcomes: dict[int, RouteOutcome],
    atr_ticks: np.ndarray,
) -> bool:
    """Check only label membership; do not read or calculate its numeric value."""

    outcome = outcomes.get(index)
    atr = float(atr_ticks[index])
    return outcome is not None and outcome.completed and math.isfinite(atr) and atr > 0


def _training_null_plan(
    rows: object,
    train_indexes: tuple[int, ...],
) -> tuple[np.ndarray, np.ndarray, dict[str, object]]:
    starts = tuple(int(rows.entry_ns[index]) for index in train_indexes)
    geometry = feature_gate.DateBlockGeometry(
        row_ids=tuple(rows.row_ids[index] for index in train_indexes),
        product_keys=tuple(
            feature_gate._product_key(rows.contracts[index]) for index in train_indexes
        ),
        source_dates=tuple(rows.source_dates[index] for index in train_indexes),
        planned_start_ns=starts,
        planned_end_ns=tuple(value + HORIZON_SECONDS * SECOND_NS for value in starts),
    )
    plan = feature_gate.date_block_derangements(geometry, tuple(range(len(train_indexes))))
    first_mapping = np.empty(len(train_indexes), dtype=np.int64)
    second_mapping = np.empty(len(train_indexes), dtype=np.int64)
    first_mapping[np.asarray(plan.destinations, dtype=np.int64)] = np.asarray(
        plan.first_sources,
        dtype=np.int64,
    )
    second_mapping[np.asarray(plan.destinations, dtype=np.int64)] = np.asarray(
        plan.second_sources,
        dtype=np.int64,
    )
    expected = set(range(len(train_indexes)))
    if (
        set(first_mapping.tolist()) != expected
        or set(second_mapping.tolist()) != expected
        or np.any(first_mapping == second_mapping)
    ):
        raise ActualPreflightError("TRAINING_NULL_MAPPING_INVALID")
    return (
        first_mapping,
        second_mapping,
        {
            "minimum_destination_degree": plan.minimum_destination_degree,
            "minimum_source_degree": plan.minimum_source_degree,
            "plan_sha256": plan.sha256,
        },
    )


def _planned_schedule(
    rows: object,
    test_indexes: tuple[int, ...],
    prediction: np.ndarray,
    threshold: float,
    atr_ticks: np.ndarray,
) -> tuple[tuple[PlannedSignal, ...], dict[str, int]]:
    if len(test_indexes) != len(prediction):
        raise ActualPreflightError("TEST_PREDICTION_ALIGNMENT_DRIFT")
    candidates: list[PlannedSignal] = []
    for local_index, row_index in enumerate(test_indexes):
        score = float(prediction[local_index])
        predicted_edge_ticks = abs(score) * float(atr_ticks[row_index]) - 14.0
        if abs(score) < threshold or predicted_edge_ticks <= 0.0 or score == 0.0:
            continue
        candidates.append(
            PlannedSignal(
                row_index=row_index,
                row_id=rows.row_ids[row_index],
                source_date=rows.source_dates[row_index].isoformat(),
                decision_ns=int(rows.decision_ns[row_index]),
                direction=1 if score > 0 else -1,
                prediction=score,
            )
        )
    candidates.sort(key=lambda item: (item.source_date, item.decision_ns, item.row_id))
    last_planned_exit: dict[str, int] = {}
    planned: list[PlannedSignal] = []
    overlap_skipped = 0
    for item in candidates:
        entry_eligibility = item.decision_ns + ROUTING_DELAY_NS
        if entry_eligibility <= last_planned_exit.get(item.source_date, -(10**30)):
            overlap_skipped += 1
            continue
        planned.append(item)
        last_planned_exit[item.source_date] = (
            item.decision_ns + HORIZON_SECONDS * SECOND_NS + ROUTING_DELAY_NS
        )
    for source_date in sorted({item.source_date for item in planned}):
        chosen = [item for item in planned if item.source_date == source_date]
        for left, right in pairwise(chosen):
            left_exit = left.decision_ns + HORIZON_SECONDS * SECOND_NS + ROUTING_DELAY_NS
            right_entry = right.decision_ns + ROUTING_DELAY_NS
            if right_entry <= left_exit:
                raise ActualPreflightError("PLANNED_SIGNAL_INTERVALS_OVERLAP")
    return tuple(planned), {
        "candidates": len(candidates),
        "planned_nonoverlap": len(planned),
        "planned_overlap_skipped": overlap_skipped,
    }


def _schedule_document(schedule: Sequence[PlannedSignal]) -> list[dict[str, object]]:
    return [
        {
            "decision_ns": item.decision_ns,
            "direction": item.direction,
            "prediction_hex": item.prediction.hex(),
            "row_id": item.row_id,
            "source_date": item.source_date,
        }
        for item in schedule
    ]


def _evaluate_schedule(
    schedule: Sequence[PlannedSignal],
    outcomes: dict[int, RouteOutcome],
    future_complete_indexes: frozenset[int],
) -> tuple[list[dict[str, object]], dict[str, int]]:
    audit = {
        "scheduled": len(schedule),
        "cache_unavailable": 0,
        "entry_no_fill": 0,
        "future_path_censored": 0,
        "exit_censored": 0,
        "blocked_after_censor": 0,
        "completed": 0,
    }
    blocked_dates: set[str] = set()
    trades: list[dict[str, object]] = []
    for item in schedule:
        if item.source_date in blocked_dates:
            audit["blocked_after_censor"] += 1
            continue
        outcome = outcomes.get(item.row_index)
        if outcome is None:
            audit["cache_unavailable"] += 1
            continue
        if not outcome.entry_filled:
            audit["entry_no_fill"] += 1
            continue
        if item.row_index not in future_complete_indexes:
            audit["future_path_censored"] += 1
            blocked_dates.add(item.source_date)
            continue
        if not outcome.completed:
            audit["exit_censored"] += 1
            blocked_dates.add(item.source_date)
            continue
        gross = outcome.long_gross if item.direction > 0 else outcome.short_gross
        assert gross is not None
        trades.append(
            {
                "direction": item.direction,
                "gross": float(gross),
                "net10": float(gross) - 10.0,
                "net14": float(gross) - 14.0,
                "row_id": item.row_id,
                "source_date": item.source_date,
            }
        )
        audit["completed"] += 1
    if sum(
        audit[key]
        for key in (
            "cache_unavailable",
            "entry_no_fill",
            "future_path_censored",
            "exit_censored",
            "blocked_after_censor",
            "completed",
        )
    ) != len(schedule):
        raise ActualPreflightError("SIGNAL_AUDIT_NOT_EXHAUSTIVE")
    return trades, audit


def _performance(trades: Sequence[dict[str, object]]) -> dict[str, object]:
    count = len(trades)
    gross = np.asarray([float(item["gross"]) for item in trades], dtype=np.float64)
    net10 = np.asarray([float(item["net10"]) for item in trades], dtype=np.float64)
    net14 = np.asarray([float(item["net14"]) for item in trades], dtype=np.float64)
    gains = float(net10[net10 > 0].sum()) if count else 0.0
    losses = float(-net10[net10 < 0].sum()) if count else 0.0
    return {
        "active_dates": len({str(item["source_date"]) for item in trades}),
        "count": count,
        "gross_ev": None if not count else float(gross.mean()),
        "gross_total": float(gross.sum()),
        "long_count": sum(int(item["direction"]) > 0 for item in trades),
        "net10_ev": None if not count else float(net10.mean()),
        "net10_total": float(net10.sum()),
        "net14_total": float(net14.sum()),
        "profit_factor_net10": (
            "INF" if gains > 0 and losses == 0 else (None if losses == 0 else gains / losses)
        ),
        "short_count": sum(int(item["direction"]) < 0 for item in trades),
    }


def _plan_document() -> dict[str, object]:
    return {
        "action": {
            "direction": "SIGN_OF_PREDICTION",
            "predicted_edge": "ABS_SCORE_TIMES_CAUSAL_5M_ATR_MINUS_14_STRICTLY_POSITIVE",
            "rate": ACTION_RATE,
            "threshold": "TRAIN_ABS_PREDICTION_Q90_NUMPY_HIGHER",
        },
        "benchmark_dates": list(BENCHMARK_DATES),
        "censor_policy": ("NO_BACKFILL;FUTURE_PATH_OR_EXIT_CENSOR_BLOCKS_REMAINDER_OF_SOURCE_DATE"),
        "economics_ticks": {"standard_total_friction": 10, "stress_total_friction": 14},
        "feature_sets": {"baseline": "PRICE36", "candidate": "PRICE36_PLUS_FLOW30"},
        "folds": [
            {
                "train_dates": list(BENCHMARK_DATES[:stop]),
                "test_dates": list(BENCHMARK_DATES[stop : stop + 3]),
            }
            for stop in (5, 8, 11)
        ],
        "gates": {
            "controls_completed_per_fold_min": MIN_CONTROL_COMPLETED_PER_FOLD,
            "controls_completed_total_min": MIN_CONTROL_COMPLETED_TOTAL,
            "entry_no_fill_fraction_max": MAX_ENTRY_NO_FILL_FRACTION,
            "flow_active_dates_min": MIN_REAL_FLOW_ACTIVE_DATES,
            "flow_candidates_per_fold_min": MIN_REAL_FLOW_CANDIDATES_PER_FOLD,
            "flow_completed_per_fold_min": MIN_REAL_FLOW_COMPLETED_PER_FOLD,
            "flow_completed_total_min": MIN_REAL_FLOW_COMPLETED_TOTAL,
            "flow_each_side_completed_min": MIN_REAL_FLOW_SIDE_COMPLETED,
            "gross_ev_strictly_gt": 14,
            "ic_test_rows_per_fold_min": MIN_TEST_IC_ROWS,
            "net10_profit_factor_min": 1.05,
            "train_label_rows_min": list(MIN_TRAIN_LABEL_ROWS),
        },
        "hgb_parameters": HGB_PARAMETERS,
        "horizon_seconds": HORIZON_SECONDS,
        "max_rss_bytes": MAX_RSS_BYTES,
        "max_runtime_seconds": MAX_RUNTIME_SECONDS,
        "model_fit_count": 18,
        "null": "TWO_EDGE_DISJOINT_DATE_BLOCK_TARGET_DERANGEMENTS_ON_COMPLETED_TRAIN_ROWS",
        "prediction_first": True,
        "route": "D_PLUS_1S_AND_D_PLUS_3600S_PLUS_1S_FIRST_PHYSICAL_EVENT_NO_CHASE",
        "schema": "systematic_fx.bar_flow_interaction_actual_preflight_plan.v1",
        "target": "ROUTED_BBO_MIDPOINT_MOVE_TICKS_DIV_CAUSAL_5M_ATR20_TICKS",
        "test_signal_mask": "HISTORY_ONLY_FEATURE_COMPLETE_NEVER_FUTURE_OR_EXECUTION_FILTERED",
    }


def _decision(
    fold_rows: Sequence[dict[str, object]],
    aggregate: dict[str, dict[str, object]],
    ic_summary: dict[str, object],
) -> dict[str, object]:
    reasons: list[str] = []
    real_flow = aggregate["REAL_FLOW"]
    real_price = aggregate["REAL_PRICE"]
    if any(
        int(row["schedule_audits"]["REAL_FLOW"]["candidates"]) < MIN_REAL_FLOW_CANDIDATES_PER_FOLD
        for row in fold_rows
    ):
        reasons.append("FLOW_CANDIDATES_PER_FOLD_LT_20")
    if any(
        int(row["performance"]["REAL_FLOW"]["count"]) < MIN_REAL_FLOW_COMPLETED_PER_FOLD
        for row in fold_rows
    ):
        reasons.append("FLOW_COMPLETED_PER_FOLD_LT_6")
    if int(real_flow["count"]) < MIN_REAL_FLOW_COMPLETED_TOTAL:
        reasons.append("FLOW_COMPLETED_TOTAL_LT_21")
    if int(real_flow["active_dates"]) < MIN_REAL_FLOW_ACTIVE_DATES:
        reasons.append("FLOW_ACTIVE_DATES_LT_7")
    if int(real_flow["long_count"]) < MIN_REAL_FLOW_SIDE_COMPLETED:
        reasons.append("FLOW_LONG_COMPLETED_LT_5")
    if int(real_flow["short_count"]) < MIN_REAL_FLOW_SIDE_COMPLETED:
        reasons.append("FLOW_SHORT_COMPLETED_LT_5")
    for key in MODEL_KEYS:
        if any(
            int(row["performance"][key]["count"]) < MIN_CONTROL_COMPLETED_PER_FOLD
            for row in fold_rows
        ):
            reasons.append(f"{key}_COMPLETED_PER_FOLD_LT_5")
        if int(aggregate[key]["count"]) < MIN_CONTROL_COMPLETED_TOTAL:
            reasons.append(f"{key}_COMPLETED_TOTAL_LT_18")
        audit = aggregate[key]["signal_audit"]
        if int(audit["cache_unavailable"]) != 0:
            reasons.append(f"{key}_CACHE_UNAVAILABLE")
        if int(audit["future_path_censored"]) != 0:
            reasons.append(f"{key}_FUTURE_PATH_CENSOR_PRESENT")
        if int(audit["exit_censored"]) != 0 or int(audit["blocked_after_censor"]) != 0:
            reasons.append(f"{key}_EXIT_CENSOR_PRESENT")
        scheduled = int(audit["scheduled"])
        if scheduled == 0 or int(audit["entry_no_fill"]) / scheduled > MAX_ENTRY_NO_FILL_FRACTION:
            reasons.append(f"{key}_ENTRY_NO_FILL_GT_20PCT")
    gross_ev = real_flow["gross_ev"]
    if not isinstance(gross_ev, (int, float)) or not float(gross_ev) > 14.0:
        reasons.append("FLOW_GROSS_EV_NOT_GT_14")
    profit_factor = real_flow["profit_factor_net10"]
    if profit_factor != "INF" and (
        not isinstance(profit_factor, (int, float)) or float(profit_factor) < 1.05
    ):
        reasons.append("FLOW_NET10_PF_LT_1_05")
    real_flow_fold_net = [
        float(row["performance"]["REAL_FLOW"]["net10_total"]) for row in fold_rows
    ]
    if sum(value > 0 for value in real_flow_fold_net) < 2:
        reasons.append("FLOW_POSITIVE_NET10_FOLDS_LT_2")
    if real_flow_fold_net[-1] <= 0:
        reasons.append("FLOW_FOLD3_NET10_NOT_POSITIVE")

    economic_delta = {
        world: float(aggregate[f"{world}_FLOW"]["net10_total"])
        - float(aggregate[f"{world}_PRICE"]["net10_total"])
        for world in ("REAL", "NULL1", "NULL2")
    }
    if not economic_delta["REAL"] > max(0.0, economic_delta["NULL1"], economic_delta["NULL2"]):
        reasons.append("REAL_ECONOMIC_INCREMENT_NOT_GT_NULLS_AND_ZERO")
    fold_real_deltas = [
        float(row["performance"]["REAL_FLOW"]["net10_total"])
        - float(row["performance"]["REAL_PRICE"]["net10_total"])
        for row in fold_rows
    ]
    if sum(value > 0 for value in fold_real_deltas) < 2:
        reasons.append("REAL_POSITIVE_ECONOMIC_INCREMENT_FOLDS_LT_2")
    if fold_real_deltas[-1] <= 0:
        reasons.append("REAL_FOLD3_ECONOMIC_INCREMENT_NOT_POSITIVE")

    flow_ics = [row["ics"]["REAL_FLOW"] for row in fold_rows]
    real_ic_deltas = [row["ics"]["REAL_FLOW"] - row["ics"]["REAL_PRICE"] for row in fold_rows]
    if not float(ic_summary["REAL_FLOW_mean_fold_ic"]) > 0:
        reasons.append("FLOW_MEAN_FOLD_IC_NOT_POSITIVE")
    if sum(float(value) > 0 for value in flow_ics) < 2:
        reasons.append("FLOW_POSITIVE_IC_FOLDS_LT_2")
    null_ic_deltas = {
        world: float(ic_summary[f"{world}_FLOW_mean_fold_ic"])
        - float(ic_summary[f"{world}_PRICE_mean_fold_ic"])
        for world in ("REAL", "NULL1", "NULL2")
    }
    if not null_ic_deltas["REAL"] > max(0.0, null_ic_deltas["NULL1"], null_ic_deltas["NULL2"]):
        reasons.append("REAL_IC_INCREMENT_NOT_GT_NULLS_AND_ZERO")
    if sum(value > 0 for value in real_ic_deltas) < 2:
        reasons.append("REAL_POSITIVE_IC_INCREMENT_FOLDS_LT_2")
    if real_ic_deltas[-1] <= 0:
        reasons.append("REAL_FOLD3_IC_INCREMENT_NOT_POSITIVE")
    if int(real_price["count"]) == 0:  # defensive, already covered by support gates
        reasons.append("REAL_PRICE_CONTROL_EMPTY")
    return {
        "decision": "GO_TO_FRESH_INDEPENDENT_DATA" if not reasons else "STOP",
        "economic_net10_increment": economic_delta,
        "fold_real_net10_increment": fold_real_deltas,
        "ic_increment": null_ic_deltas,
        "reasons": reasons,
    }


def _run(root: Path) -> dict[str, object]:
    started = monotonic()
    actual_sources = {
        "execution_base": _file_sha256(root / "scripts/flow_acceptance_markout_preflight.py"),
        "feature_gate": _file_sha256(root / "scripts/bar_flow_interaction_ml_feasibility.py"),
    }
    expected_sources = {
        "execution_base": EXPECTED_EXECUTION_BASE_SHA256,
        "feature_gate": EXPECTED_FEATURE_GATE_SHA256,
    }
    if actual_sources != expected_sources:
        raise ActualPreflightError("FROZEN_PARENT_SOURCE_SHA_DRIFT")
    synthetic = _synthetic_route_regressions()
    structural = feature_gate._structural_rows(root)
    rows, matrix66, feature_audit = feature_gate._causal_feature_rows(root)
    _check_budget(started)
    if len(FEATURE66) != PRICE_FEATURE_COUNT + FLOW_FEATURE_COUNT:
        raise ActualPreflightError("FEATURE_COUNT_DRIFT")
    history_indexes = tuple(
        index
        for index, (source_date, contract, decision_ns) in enumerate(
            zip(rows.source_dates, rows.contracts, rows.decision_ns, strict=True)
        )
        if (source_date.isoformat(), contract, int(decision_ns)) in structural.history_keys
    )
    history_counts = tuple(
        sum(
            rows.source_dates[index].isoformat() in BENCHMARK_DATES[start:stop]
            for index in history_indexes
        )
        for start, stop in DATE_BLOCK_SLICES
    )
    label_lattice_counts = tuple(
        sum(
            (
                rows.source_dates[index].isoformat(),
                rows.contracts[index],
                int(rows.decision_ns[index]),
            )
            in structural.intersection_keys
            and rows.source_dates[index].isoformat() in BENCHMARK_DATES[start:stop]
            for index in history_indexes
        )
        for start, stop in DATE_BLOCK_SLICES
    )
    if history_counts != EXPECTED_HISTORY_FEATURE_COUNTS:
        raise ActualPreflightError("HISTORY_FEATURE_COUNT_DRIFT")
    if label_lattice_counts != EXPECTED_LABEL_LATTICE_COUNTS:
        raise ActualPreflightError("LABEL_LATTICE_COUNT_DRIFT")
    atr_ticks = np.asarray(rows.atr_ticks_by_timeframe[:, 0], dtype=np.float64)
    if (
        atr_ticks.shape != (rows.row_count,)
        or np.any(~np.isfinite(atr_ticks))
        or np.any(atr_ticks <= 0)
    ):
        raise ActualPreflightError("CAUSAL_5M_ATR_INVALID")

    decision_indexes_by_date = {
        source_date: tuple(
            (index, int(rows.decision_ns[index]))
            for index in history_indexes
            if rows.source_dates[index].isoformat() == source_date
        )
        for source_date in BENCHMARK_DATES
    }
    discovery, selections = execution_base._load_discovery_selections(root)
    requests = execution_base._execution_cache_requests(root, discovery, selections)
    if any(
        source_date not in requests or source_date not in selections
        for source_date in BENCHMARK_DATES
    ):
        raise ActualPreflightError("BENCHMARK_EXECUTION_REQUEST_MISSING")
    for index in history_indexes:
        source_date = rows.source_dates[index].isoformat()
        if rows.contracts[index] != selections[source_date].raw_symbol:
            raise ActualPreflightError("FEATURE_EXECUTION_CONTRACT_DRIFT")
    accessor = SequentialOutcomeAccessor(root, requests, selections, decision_indexes_by_date)
    plan = _plan_document()
    plan_sha256 = _sha256(plan)
    accessor.open_initial_training()
    _check_budget(started)
    fold_outputs: list[dict[str, object]] = []
    all_trades: dict[str, list[dict[str, object]]] = {key: [] for key in MODEL_KEYS}
    all_audits: dict[str, list[dict[str, int]]] = {key: [] for key in MODEL_KEYS}
    freeze_sha256s: list[str] = []

    for fold_index, train_stop in enumerate((5, 8, 11), start=1):
        test_stop = train_stop + 3
        train_dates = set(BENCHMARK_DATES[:train_stop])
        test_dates = set(BENCHMARK_DATES[train_stop:test_stop])
        train_candidates = tuple(
            index
            for index in history_indexes
            if rows.source_dates[index].isoformat() in train_dates
            and (
                rows.source_dates[index].isoformat(),
                rows.contracts[index],
                int(rows.decision_ns[index]),
            )
            in structural.intersection_keys
        )
        train_indexes = tuple(
            index
            for index in train_candidates
            if _target_is_available(index, outcomes=accessor.outcomes, atr_ticks=atr_ticks)
        )
        if len(train_indexes) < MIN_TRAIN_LABEL_ROWS[fold_index - 1]:
            raise ActualPreflightError(
                f"FOLD_{fold_index}_TRAIN_LABEL_ROWS_LT_{MIN_TRAIN_LABEL_ROWS[fold_index - 1]}"
            )
        test_indexes = tuple(
            index for index in history_indexes if rows.source_dates[index].isoformat() in test_dates
        )
        if len(test_indexes) != EXPECTED_HISTORY_FEATURE_COUNTS[fold_index]:
            raise ActualPreflightError(f"FOLD_{fold_index}_TEST_HISTORY_COUNT_DRIFT")
        # Freeze the two exact metadata-only bijections before reading any
        # routed target value from the already-fixed completed-row set.
        null1_mapping, null2_mapping, null_audit = _training_null_plan(
            rows,
            train_indexes,
        )
        real_target = np.asarray(
            [
                float(_target_for_row(index, outcomes=accessor.outcomes, atr_ticks=atr_ticks))
                for index in train_indexes
            ],
            dtype=np.float64,
        )
        null1_target = np.asarray(real_target[null1_mapping], dtype=np.float64)
        null2_target = np.asarray(real_target[null2_mapping], dtype=np.float64)
        null_audit = {
            **null_audit,
            "null1_target_sha256": _sha256(_float_hex(null1_target)),
            "null2_target_sha256": _sha256(_float_hex(null2_target)),
        }
        targets_by_world = {
            "REAL": real_target,
            "NULL1": null1_target,
            "NULL2": null2_target,
        }
        train_matrix = matrix66[np.asarray(train_indexes, dtype=np.int64)]
        test_matrix = matrix66[np.asarray(test_indexes, dtype=np.int64)]
        predictions: dict[str, np.ndarray] = {}
        thresholds: dict[str, float] = {}
        schedules: dict[str, tuple[PlannedSignal, ...]] = {}
        schedule_audits: dict[str, dict[str, int]] = {}
        for world in ("REAL", "NULL1", "NULL2"):
            for family, width in (("PRICE", PRICE_FEATURE_COUNT), ("FLOW", len(FEATURE66))):
                key = f"{world}_{family}"
                model = _model().fit(train_matrix[:, :width], targets_by_world[world])
                train_prediction = np.asarray(
                    model.predict(train_matrix[:, :width]), dtype=np.float64
                )
                test_prediction = np.asarray(
                    model.predict(test_matrix[:, :width]), dtype=np.float64
                )
                threshold = float(
                    np.quantile(np.abs(train_prediction), 1.0 - ACTION_RATE, method="higher")
                )
                if (
                    not math.isfinite(threshold)
                    or threshold < 0
                    or not np.isfinite(test_prediction).all()
                ):
                    raise ActualPreflightError("MODEL_PREDICTION_OR_THRESHOLD_INVALID")
                schedule, schedule_audit = _planned_schedule(
                    rows,
                    test_indexes,
                    test_prediction,
                    threshold,
                    atr_ticks,
                )
                predictions[key] = test_prediction
                thresholds[key] = threshold
                schedules[key] = schedule
                schedule_audits[key] = schedule_audit

        freeze_document = {
            "feature_gate_sha256": actual_sources["feature_gate"],
            "fold": fold_index,
            "hgb_parameters": HGB_PARAMETERS,
            "null": null_audit,
            "opened_outcome_dates_before_freeze": list(accessor.opened_dates),
            "plan_sha256": plan_sha256,
            "predictions": {key: _float_hex(predictions[key]) for key in MODEL_KEYS},
            "real_target_sha256": _sha256(_float_hex(real_target)),
            "schedules": {key: _schedule_document(schedules[key]) for key in MODEL_KEYS},
            "schema": "systematic_fx.bar_flow_interaction_prediction_freeze.v1",
            "test_row_ids": [rows.row_ids[index] for index in test_indexes],
            "threshold_hex": {key: thresholds[key].hex() for key in MODEL_KEYS},
            "train_row_ids": [rows.row_ids[index] for index in train_indexes],
        }
        freeze_sha256 = _sha256(freeze_document)
        freeze_sha256s.append(freeze_sha256)
        accessor.open_test_after_freeze(
            train_stop=train_stop,
            test_stop=test_stop,
            freeze_sha256=freeze_sha256,
        )
        _check_budget(started)

        ic_indexes = tuple(
            index
            for index in test_indexes
            if (
                rows.source_dates[index].isoformat(),
                rows.contracts[index],
                int(rows.decision_ns[index]),
            )
            in structural.intersection_keys
            and _target_for_row(index, outcomes=accessor.outcomes, atr_ticks=atr_ticks) is not None
        )
        if len(ic_indexes) < MIN_TEST_IC_ROWS:
            raise ActualPreflightError(f"FOLD_{fold_index}_IC_ROWS_LT_{MIN_TEST_IC_ROWS}")
        test_local_by_global = {index: position for position, index in enumerate(test_indexes)}
        ic_positions = np.asarray(
            [test_local_by_global[index] for index in ic_indexes], dtype=np.int64
        )
        ic_target = np.asarray(
            [
                float(_target_for_row(index, outcomes=accessor.outcomes, atr_ticks=atr_ticks))
                for index in ic_indexes
            ],
            dtype=np.float64,
        )
        future_complete_indexes = frozenset(
            index
            for index in test_indexes
            if (
                rows.source_dates[index].isoformat(),
                rows.contracts[index],
                int(rows.decision_ns[index]),
            )
            in structural.future_capacity_keys
        )
        ics: dict[str, float] = {}
        performance: dict[str, dict[str, object]] = {}
        signal_audits: dict[str, dict[str, int]] = {}
        for key in MODEL_KEYS:
            ic = _safe_spearman(ic_target, predictions[key][ic_positions])
            if ic is None:
                raise ActualPreflightError(f"FOLD_{fold_index}_{key}_IC_INVALID")
            ics[key] = ic
            trades, signal_audit = _evaluate_schedule(
                schedules[key],
                accessor.outcomes,
                future_complete_indexes,
            )
            for trade in trades:
                trade["fold"] = fold_index
            all_trades[key].extend(trades)
            all_audits[key].append(signal_audit)
            performance[key] = _performance(trades)
            signal_audits[key] = signal_audit
        fold_outputs.append(
            {
                "fold": fold_index,
                "freeze_sha256": freeze_sha256,
                "ic_rows": len(ic_indexes),
                "ics": ics,
                "null_plan": null_audit,
                "performance": performance,
                "schedule_audits": schedule_audits,
                "signal_audits": signal_audits,
                "test_history_rows": len(test_indexes),
                "train_label_rows": len(train_indexes),
            }
        )

    if tuple(accessor.opened_dates) != BENCHMARK_DATES:
        raise ActualPreflightError("BENCHMARK_OUTCOME_DATE_CLOSURE_FAILED")
    aggregate: dict[str, dict[str, object]] = {}
    for key in MODEL_KEYS:
        aggregate[key] = _performance(all_trades[key])
        aggregate[key]["signal_audit"] = {
            field: int(sum(audit[field] for audit in all_audits[key]))
            for field in (
                "scheduled",
                "cache_unavailable",
                "entry_no_fill",
                "future_path_censored",
                "exit_censored",
                "blocked_after_censor",
                "completed",
            )
        }
    ic_summary = {
        f"{key}_mean_fold_ic": float(np.mean([row["ics"][key] for row in fold_outputs]))
        for key in MODEL_KEYS
    }
    decision = _decision(fold_outputs, aggregate, ic_summary)
    elapsed = monotonic() - started
    _check_budget(started)
    return {
        "aggregate": aggregate,
        "cache_bindings_sha256": _sha256(accessor.bindings),
        "code_sha256": _file_sha256(Path(__file__).resolve()),
        "decision": decision,
        "elapsed_seconds": elapsed,
        "feature_audit": feature_audit,
        "folds": fold_outputs,
        "freeze_sha256s": freeze_sha256s,
        "history_feature_counts": list(history_counts),
        "ic_summary": ic_summary,
        "label_lattice_counts": list(label_lattice_counts),
        "opened_outcome_dates": list(accessor.opened_dates),
        "parent_source_sha256": actual_sources,
        "peak_rss_bytes": _peak_rss_bytes(),
        "plan": plan,
        "plan_sha256": plan_sha256,
        "platform": platform.platform(),
        "route_reasons_sha256": _sha256(accessor.route_reasons),
        "schema": SCHEMA,
        "synthetic_route_regressions": synthetic,
    }


def run(root: Path) -> dict[str, object]:
    previous_handler = signal.signal(signal.SIGALRM, _timeout_handler)
    signal.setitimer(signal.ITIMER_REAL, MAX_RUNTIME_SECONDS)
    try:
        return _run(root)
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
    try:
        result = run(args.project_root.resolve())
    except (RuntimeError, ValueError, OSError) as error:
        print(
            _canonical_json_bytes(
                {
                    "decision": "STOP",
                    "error": str(error),
                    "schema": SCHEMA,
                }
            ).decode("ascii")
        )
        return 1
    print(_canonical_json_bytes(result).decode("ascii"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
