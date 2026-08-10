"""Immutable artifact contracts for bar-state conditional Discovery v2.

The campaign deliberately reuses the hardened bar artifact publisher.  The
wrapper in this module narrows that generic primitive to one campaign-owned
directory, a fixed set of evidence roles, and an explicit Discovery-only
lineage document.  Large row sets are Parquet; models and summaries are strict
canonical JSON.  Pickle and other executable model serializations are never
accepted.
"""

from __future__ import annotations

import json
import os
import re
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from functools import lru_cache
from pathlib import Path
from statistics import median
from types import MappingProxyType
from typing import BinaryIO, Final, Literal

import pyarrow as pa
import pyarrow.parquet as pq

from systematic_fx.research.bar_artifacts import (
    BAR_PATTERN_ARTIFACT_ROOT,
    BarArtifactDescriptor,
    BarArtifactError,
    PublishedBarArtifact,
    arrow_schema_sha256,
    open_verified_bar_artifact,
    publish_bar_artifact_bytes,
    publish_bar_artifact_open_file,
    publish_bar_json_artifact,
    publish_bar_parquet_table,
)
from systematic_fx.research.bar_state_config import (
    BAR_STATE_ECONOMIC_MULTIPLIERS,
    BAR_STATE_OUTER_SPLIT_SHA256,
)
from systematic_fx.research.bar_state_model import BarStateModelError, CanonicalBarStateModel
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.validation.bar_state_splits import (
    BAR_STATE_BOOTSTRAP_EVALUATION_CALENDAR_SCHEMA,
    BAR_STATE_FROZEN_BOOTSTRAP_EVALUATION_CALENDAR_SHA256,
    BAR_STATE_FROZEN_SPLIT_SHA256,
    BarStateSplitPlan,
    frozen_bar_state_bootstrap_evaluation_calendar,
)

BAR_STATE_CAMPAIGN_KEY: Final = "bar_state_conditional_v2"
BAR_STATE_ARTIFACT_TYPE: Final = BAR_STATE_CAMPAIGN_KEY
BAR_STATE_ARTIFACT_ROOT: Final = BAR_PATTERN_ARTIFACT_ROOT / BAR_STATE_ARTIFACT_TYPE
BAR_STATE_ARTIFACT_IDENTITY_SCHEMA: Final = "systematic_fx.bar_state_artifact_lineage.v1"
BAR_STATE_DISCOVERY_SCOPE_SCHEMA: Final = "systematic_fx.bar_state_discovery_scope.v1"
BAR_STATE_PARENT_REFERENCE_SCHEMA: Final = "systematic_fx.bar_state_parent_artifact.v1"

BAR_STATE_RAW_SOURCE_MANIFEST_SHA256: Final = (
    "14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de"
)
BAR_STATE_BAR_DATASET_MANIFEST_SHA256: Final = (
    "e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc"
)
BAR_STATE_SPLIT_PLAN_SHA256: Final = (
    "5594725f6769a706018d414a5b27e3903f1d7d1cc22c98e93b6e973ead1af043"
)

BarStateArtifactKind = Literal[
    "CODE_SNAPSHOT",
    "REGISTRATION",
    "FEATURE",
    "LABEL",
    "MODEL",
    "OOS_TRADE",
    "GLOBAL_RESULT",
    "TERMINAL_RESULT",
]

BAR_STATE_ARTIFACT_SCHEMA_BY_KIND: Final[dict[BarStateArtifactKind, str]] = {
    "CODE_SNAPSHOT": "systematic_fx.code_snapshot.v2",
    "REGISTRATION": "systematic_fx.bar_state_registration_artifact.v1",
    "FEATURE": "systematic_fx.bar_state_feature_artifact.v1",
    "LABEL": "systematic_fx.bar_state_label_artifact.v1",
    "MODEL": "systematic_fx.bar_state_model_artifact.v1",
    "OOS_TRADE": "systematic_fx.bar_state_oos_trade_artifact.v1",
    "GLOBAL_RESULT": "systematic_fx.bar_state_global_result_artifact.v1",
    "TERMINAL_RESULT": "systematic_fx.bar_state_terminal_result_artifact.v1",
}
BAR_STATE_PARQUET_KINDS: Final = frozenset({"FEATURE", "LABEL", "OOS_TRADE"})
BAR_STATE_JSON_KINDS: Final = frozenset(
    {"CODE_SNAPSHOT", "REGISTRATION", "MODEL", "GLOBAL_RESULT", "TERMINAL_RESULT"}
)
BAR_STATE_TERMINAL_COMPACT_KEYS: Final = frozenset(
    {
        "candidate_key",
        "discovery_final_fit_model_sha256",
        "final_label",
        "positive_component_size",
        "price_policy",
        "rejection_reasons",
        "selected_stop_loss_index",
        "selected_take_profit_index",
    }
)
BAR_STATE_CANDIDATE_SELECTION_KEYS: Final = frozenset(
    {
        "bootstrap_lower_bound_ev_ticks",
        "candidate_key",
        "final_label",
        "maximum_drawdown_ticks",
        "moderate_ev_ticks",
        "positive_component_size",
        "positive_inner_fold_count",
        "rejection_reasons",
        "selected_stop_loss_index",
        "selected_stop_loss_multiplier",
        "selected_take_profit_index",
        "selected_take_profit_multiplier",
        "worst_fold_moderate_ev_ticks",
    }
)
BAR_STATE_FINALIST_MODEL_BINDING_KEYS: Final = frozenset(
    {"candidate_key", "feature_set_id", "model_sha256", "timeframe_seconds"}
)
BAR_STATE_GLOBAL_DISCOVERY_KEYS: Final = frozenset(
    {
        "axis_resolutions",
        "bh_family_size",
        "bootstrap_convention",
        "bootstrap_evaluation_calendar",
        "bootstrap_evaluation_calendar_sha256",
        "candidate_results",
        "candidate_signal_decision_counts",
        "candidate_support",
        "cell_summaries",
        "discovery_final_fit_models",
        "discovery_finalist_model_bindings",
        "feature_exclusion_qc",
        "finalist_keys",
        "memory_plan",
        "multiplicity_results",
        "observed_utc_months",
        "portfolio_executed_trade_record_count",
        "portfolio_signal_count",
        "schema",
        "signal_count",
    }
)
BAR_STATE_GLOBAL_FINAL_FIT_MODEL_KEYS: Final = frozenset(
    {
        "feature_set_id",
        "fit_key",
        "label_maturity_end_active_ordinal",
        "model",
        "model_sha256",
        "schema",
        "timeframe_seconds",
        "training_decision_end_active_ordinal",
    }
)
BAR_STATE_CANDIDATE_SELECTION_PROJECTION_KEYS: Final = frozenset(
    {
        "candidate_key",
        "final_label",
        "positive_component_size",
        "rejection_reasons",
        "selected_stop_loss_index",
        "selected_take_profit_index",
    }
)
BAR_STATE_GLOBAL_EVIDENCE_PROJECTION_SCHEMA: Final = (
    "systematic_fx.bar_state_global_evidence_projection.v1"
)

_BAR_STATE_CANDIDATE_KEYS: Final = tuple(
    f"bsv2_tf{timeframe:04d}_fs{feature_set}_cm{margin}"
    for timeframe in (300, 1_800)
    for feature_set in ("morphology", "state")
    for margin in ("005", "010", "015")
)
_BAR_STATE_SCENARIO_IDS: Final = (
    "BASELINE",
    "MODERATE_COMBINED",
    "SEVERE_DIAGNOSTIC",
)
_BAR_STATE_FOLD_KEYS: Final = (
    "discovery_inner_1",
    "discovery_inner_2",
    "discovery_inner_3",
)
_BAR_STATE_OBSERVED_UTC_MONTHS: Final = tuple(
    [f"2022-{month:02d}" for month in range(5, 13)] + [f"2023-{month:02d}" for month in range(1, 9)]
)
_BAR_STATE_BOOTSTRAP_CONVENTION: Final = (
    "FOLD_LOCAL_STATIONARY_PCG64_ALIGNED_EXIT_DAY_NET_AND_FILL_COUNTS"
)
_BAR_STATE_BOOTSTRAP_CACHE_MAXIMUM: Final = 16
_BAR_STATE_BOOTSTRAP_VALIDATION_CACHE: dict[str, str] = {}
_BAR_STATE_BOOTSTRAP_VALIDATION_LOCK = threading.Lock()
_BAR_STATE_SUPPORT_KEYS: Final = frozenset(
    {
        "candidate_key",
        "distinct_signal_day_count",
        "raw_directional_signal_count",
        "raw_signal_count_by_fold",
        "timeframe_seconds",
    }
)
_BAR_STATE_AXIS_RESOLUTION_KEYS: Final = frozenset(
    {
        "axis_vector_sha256",
        "candidate_key",
        "filled_directional_signal_count",
        "per_signal_distinct_count_histogram",
        "unique_axis_vector_count",
    }
)
_BAR_STATE_MEMORY_PLAN_KEYS: Final = frozenset(
    {
        "accumulator_count",
        "candidate_count",
        "grid_cell_count",
        "input_signal_count",
        "maximum_input_signal_count",
        "maximum_resident_one_second_rows",
        "one_second_row_count",
        "outcome_span_count",
        "resident_outcome_span_limit",
        "retained_trade_record_count",
        "scenario_count",
    }
)
_BAR_STATE_MULTIPLICITY_KEYS: Final = frozenset(
    {
        "adjusted_p_value",
        "bh_rejected",
        "bootstrap_lower_bound_ev_ticks",
        "candidate_key",
        "deterministic_gate_passed",
        "raw_p_value",
        "rejection_reasons",
        "stop_loss_index",
        "take_profit_index",
    }
)
_BAR_STATE_CELL_KEYS: Final = frozenset(
    {
        "allocated_fixed_cost_ticks",
        "blocks",
        "calendar_month_net_pnl_usd",
        "candidate_key",
        "cell_id",
        "daily_fill_count",
        "daily_net_pnl_ticks",
        "distinct_stop_loss_distance_count",
        "distinct_take_profit_distance_count",
        "entry_fill_count",
        "entry_not_filled_count",
        "fully_loaded_net_ev_ticks",
        "fully_loaded_net_pnl_ticks",
        "gross_pnl_ticks",
        "maximum_drawdown_ticks",
        "no_trade_count",
        "positive_gross_by_contract",
        "profit_factor",
        "same_second_stop_first_count",
        "scenario_id",
        "signal_count",
        "skipped_occupied_count",
        "stop_first_count",
        "stop_loss_multiplier",
        "take_profit_first_count",
        "take_profit_multiplier",
        "terminal_exit_count",
        "variable_cost_ticks",
    }
)

_SHA256 = re.compile(r"[0-9a-f]{64}")
_CANONICAL_KEY = re.compile(r"[a-z0-9][a-z0-9_:-]*")
_BAR_STATE_CANDIDATE_KEY = re.compile(
    r"bsv2_tf(?P<timeframe>0300|1800)_fs(?P<feature_set>morphology|state)_"
    r"cm(?:005|010|015)"
)


class BarStateArtifactError(BarArtifactError):
    """A v2 artifact or its Discovery-only lineage is invalid."""


def _sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise BarStateArtifactError(f"{label} must be a lowercase SHA-256")
    return value


def _canonical_key(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _CANONICAL_KEY.fullmatch(value) is None:
        raise BarStateArtifactError(f"{label} is not canonical")
    return value


def _canonical_mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise BarStateArtifactError(f"{label} must be a mapping")
    try:
        encoded = canonical_json_bytes(value)
    except (TypeError, ValueError) as error:
        raise BarStateArtifactError(f"{label} must be strict canonical JSON") from error
    decoded = __import__("json").loads(encoded)
    if not isinstance(decoded, dict):  # pragma: no cover - Mapping canonicalizes to an object
        raise BarStateArtifactError(f"{label} must encode an object")
    return MappingProxyType(decoded)


def _decoded_mapping(value: object, *, label: str) -> Mapping[str, object]:
    """View a child of an already strict-canonical decoded root without re-encoding it."""

    if not isinstance(value, dict):
        raise BarStateArtifactError(f"{label} must be an object")
    return value


def _bar_state_candidate_dimensions(candidate_key: object) -> tuple[str, int, str]:
    if not isinstance(candidate_key, str):
        raise BarStateArtifactError("bar-state candidate_key must be a string")
    matched = _BAR_STATE_CANDIDATE_KEY.fullmatch(candidate_key)
    if matched is None:
        raise BarStateArtifactError("bar-state candidate_key is outside the frozen catalog")
    return candidate_key, int(matched.group("timeframe")), matched.group("feature_set").upper()


def _exact_int(value: object, *, label: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise BarStateArtifactError(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        raise BarStateArtifactError(f"{label} must be at least {minimum}")
    return value


def _exact_bool(value: object, *, label: str) -> bool:
    if not isinstance(value, bool):
        raise BarStateArtifactError(f"{label} must be a boolean")
    return value


def _fraction_value(
    value: object,
    *,
    label: str,
    minimum: Fraction | None = None,
    maximum: Fraction | None = None,
) -> Fraction:
    ratio = _canonical_mapping(value, label=label)
    if set(ratio) != {"denominator", "numerator"}:
        raise BarStateArtifactError(f"{label} must use the exact rational key set")
    numerator = _exact_int(ratio.get("numerator"), label=f"{label} numerator")
    denominator = _exact_int(ratio.get("denominator"), label=f"{label} denominator", minimum=1)
    result = Fraction(numerator, denominator)
    if dict(ratio) != {"denominator": result.denominator, "numerator": result.numerator}:
        raise BarStateArtifactError(f"{label} must be reduced and canonical")
    if minimum is not None and result < minimum:
        raise BarStateArtifactError(f"{label} is below its minimum")
    if maximum is not None and result > maximum:
        raise BarStateArtifactError(f"{label} exceeds its maximum")
    return result


def _decimal_value(
    value: object,
    *,
    label: str,
    nullable: bool = False,
    allow_infinity: bool = False,
) -> Decimal | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or not value:
        raise BarStateArtifactError(f"{label} must be a decimal string")
    try:
        result = Decimal(value)
    except InvalidOperation as error:
        raise BarStateArtifactError(f"{label} is not a decimal string") from error
    if result.is_nan() or (result.is_infinite() and not allow_infinity):
        raise BarStateArtifactError(f"{label} must be finite")
    if format(result, "f") != value:
        raise BarStateArtifactError(f"{label} must use canonical fixed-point text")
    return result


def _reason_list(value: object, *, label: str) -> list[str]:
    if (
        not isinstance(value, list)
        or any(not isinstance(item, str) or not item for item in value)
        or len(value) != len(set(value))
    ):
        raise BarStateArtifactError(f"{label} must be a unique non-empty string list")
    return value


def bar_state_candidate_selection(
    document: Mapping[str, object],
    *,
    expected_candidate_key: str | None = None,
) -> dict[str, object]:
    """Validate one exact selection and bind its indices to the frozen 7-axis grid."""

    selection = _canonical_mapping(document, label="bar-state candidate selection")
    if set(selection) != BAR_STATE_CANDIDATE_SELECTION_KEYS:
        raise BarStateArtifactError("bar-state candidate selection key set drifted")
    candidate_key, _timeframe, _feature_set = _bar_state_candidate_dimensions(
        selection.get("candidate_key")
    )
    if expected_candidate_key is not None and candidate_key != expected_candidate_key:
        raise BarStateArtifactError("bar-state candidate selection identity drifted")
    final_label = selection.get("final_label")
    if final_label not in {"FINALIST", "REJECTED"}:
        raise BarStateArtifactError("bar-state final_label must be FINALIST or REJECTED")
    selected = final_label == "FINALIST"
    rejection_reasons = selection.get("rejection_reasons")
    if (
        not isinstance(rejection_reasons, list)
        or any(not isinstance(item, str) or not item for item in rejection_reasons)
        or (selected and rejection_reasons)
        or (not selected and not rejection_reasons)
    ):
        raise BarStateArtifactError("bar-state rejection reasons differ from final_label")
    for field in ("positive_component_size", "positive_inner_fold_count"):
        value = selection.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise BarStateArtifactError(f"bar-state {field} must be a non-negative integer")
    frozen_multipliers = tuple(item.as_dict() for item in BAR_STATE_ECONOMIC_MULTIPLIERS)
    capped_qualified_reject = rejection_reasons == ["MAXIMUM_FINALIST_LIMIT"]
    has_selected_cell = selected or capped_qualified_reject
    if has_selected_cell and int(selection["positive_component_size"]) < 9:
        raise BarStateArtifactError(
            "bar-state selected cell lacks the frozen positive component size"
        )
    for axis in ("stop_loss", "take_profit"):
        index_field = f"selected_{axis}_index"
        multiplier_field = f"selected_{axis}_multiplier"
        index = selection.get(index_field)
        multiplier = selection.get(multiplier_field)
        if not has_selected_cell:
            if index is not None or multiplier is not None:
                raise BarStateArtifactError(
                    "rejected bar-state selection claims an economic-axis cell"
                )
            continue
        if (
            isinstance(index, bool)
            or not isinstance(index, int)
            or index < 0
            or index >= len(frozen_multipliers)
        ):
            raise BarStateArtifactError("bar-state selected economic-axis index is invalid")
        if multiplier != frozen_multipliers[index]:
            raise BarStateArtifactError(
                "bar-state selected multiplier differs from its frozen 7-axis index"
            )
    return dict(selection)


def bar_state_candidate_selection_projection(
    document: Mapping[str, object],
) -> dict[str, object]:
    """Return the compact selection-only identity shared across evidence roles."""

    selection = bar_state_candidate_selection(document)
    projection = {
        "candidate_key": selection["candidate_key"],
        "final_label": selection["final_label"],
        "positive_component_size": selection["positive_component_size"],
        "rejection_reasons": selection["rejection_reasons"],
        "selected_stop_loss_index": selection["selected_stop_loss_index"],
        "selected_take_profit_index": selection["selected_take_profit_index"],
    }
    if set(projection) != BAR_STATE_CANDIDATE_SELECTION_PROJECTION_KEYS:  # pragma: no cover
        raise AssertionError("candidate selection projection key set drift")
    return projection


def bar_state_price_policy_from_selection(
    selection_document: Mapping[str, object],
) -> dict[str, object]:
    """Derive the only authorized exact-price policy from a frozen selection."""

    selection = bar_state_candidate_selection(selection_document)
    return {
        "entry_reference": "NEXT_SIGNAL_BAR_FIRST_TRADE_PLUS_SCENARIO_ADVERSITY",
        "long": {
            "buying_price": "ENTRY_FILL_PRICE",
            "loss_price": "ENTRY_FILL_PRICE_MINUS_REALIZED_STOP_LOSS_TICKS",
            "sell_price": "ENTRY_FILL_PRICE_PLUS_REALIZED_TAKE_PROFIT_TICKS",
        },
        "selected_stop_loss_multiplier": selection["selected_stop_loss_multiplier"],
        "selected_take_profit_multiplier": selection["selected_take_profit_multiplier"],
        "short": {
            "buying_price": "ENTRY_FILL_PRICE_MINUS_REALIZED_TAKE_PROFIT_TICKS",
            "loss_price": "ENTRY_FILL_PRICE_PLUS_REALIZED_STOP_LOSS_TICKS",
            "sell_price": "ENTRY_FILL_PRICE",
        },
        "trade_level_exact_prices_artifact_role": "OOS_TRADE",
    }


def _validated_candidate_support(
    raw_support: object,
    *,
    label: str,
    expected_candidate_key: str | None = None,
) -> Mapping[str, object]:
    support = _decoded_mapping(raw_support, label=label)
    if set(support) != _BAR_STATE_SUPPORT_KEYS:
        raise BarStateArtifactError(f"{label} key set drifted")
    key, timeframe, _feature_set = _bar_state_candidate_dimensions(support.get("candidate_key"))
    if support.get("timeframe_seconds") != timeframe or (
        expected_candidate_key is not None and key != expected_candidate_key
    ):
        raise BarStateArtifactError(f"{label} identity drifted")
    raw_count = _exact_int(
        support.get("raw_directional_signal_count"),
        label=f"{label} raw directional signal count",
        minimum=0,
    )
    distinct_days = _exact_int(
        support.get("distinct_signal_day_count"),
        label=f"{label} distinct signal day count",
        minimum=0,
    )
    if distinct_days > raw_count or distinct_days > 311:
        raise BarStateArtifactError(f"{label} day count is impossible")
    raw_folds = support.get("raw_signal_count_by_fold")
    if not isinstance(raw_folds, list) or len(raw_folds) != 3:
        raise BarStateArtifactError(f"{label} lacks three inner folds")
    fold_keys: list[str] = []
    fold_total = 0
    for raw_fold in raw_folds:
        fold = _decoded_mapping(raw_fold, label=f"{label} fold")
        if set(fold) != {"fold_key", "signal_count"}:
            raise BarStateArtifactError(f"{label} fold key set drifted")
        fold_key = fold.get("fold_key")
        if not isinstance(fold_key, str):
            raise BarStateArtifactError(f"{label} fold key must be a string")
        fold_keys.append(fold_key)
        fold_total += _exact_int(
            fold.get("signal_count"),
            label=f"{label} fold signal count",
            minimum=0,
        )
    if tuple(fold_keys) != _BAR_STATE_FOLD_KEYS or fold_total != raw_count:
        raise BarStateArtifactError(f"{label} fold counts do not bind raw support")
    return support


def _global_candidate_support(
    discovery: Mapping[str, object],
) -> dict[str, Mapping[str, object]]:
    raw_supports = discovery.get("candidate_support")
    if not isinstance(raw_supports, list) or len(raw_supports) != 12:
        raise BarStateArtifactError("global candidate support must contain twelve objects")
    supports: dict[str, Mapping[str, object]] = {}
    ordered_keys: list[str] = []
    for raw_support in raw_supports:
        support = _validated_candidate_support(raw_support, label="global candidate support")
        key = str(support["candidate_key"])
        if key in supports:
            raise BarStateArtifactError("global candidate support identity drifted")
        supports[key] = support
        ordered_keys.append(key)
    if tuple(ordered_keys) != _BAR_STATE_CANDIDATE_KEYS:
        raise BarStateArtifactError("global candidate support order/catalog drifted")
    return supports


def _global_signal_counts(
    discovery: Mapping[str, object],
    supports: Mapping[str, Mapping[str, object]],
) -> tuple[dict[str, tuple[int, int, int]], int]:
    raw_counts = _decoded_mapping(
        discovery.get("candidate_signal_decision_counts"),
        label="global candidate signal decision counts",
    )
    if set(raw_counts) != set(_BAR_STATE_CANDIDATE_KEYS):
        raise BarStateArtifactError("global signal decision candidate catalog drifted")
    counts: dict[str, tuple[int, int, int]] = {}
    total = 0
    group_totals: dict[tuple[int, str], int] = {}
    for key in _BAR_STATE_CANDIDATE_KEYS:
        value = _decoded_mapping(raw_counts[key], label="global candidate decision counts")
        if set(value) != {"LONG", "NO_TRADE", "SHORT"}:
            raise BarStateArtifactError("global candidate decision count key set drifted")
        long_count = _exact_int(value.get("LONG"), label="global LONG count", minimum=0)
        no_trade_count = _exact_int(value.get("NO_TRADE"), label="global NO_TRADE count", minimum=0)
        short_count = _exact_int(value.get("SHORT"), label="global SHORT count", minimum=0)
        candidate_total = long_count + no_trade_count + short_count
        if candidate_total <= 0:
            raise BarStateArtifactError("global candidate signal stream cannot be empty")
        if long_count + short_count != supports[key]["raw_directional_signal_count"]:
            raise BarStateArtifactError("global decision counts differ from candidate support")
        _candidate, timeframe, feature_set = _bar_state_candidate_dimensions(key)
        prior_total = group_totals.setdefault((timeframe, feature_set), candidate_total)
        if prior_total != candidate_total:
            raise BarStateArtifactError("margin variants have different signal universes")
        counts[key] = (long_count, no_trade_count, short_count)
        total += candidate_total
    signal_count = _exact_int(discovery.get("signal_count"), label="global signal_count", minimum=1)
    portfolio_signal_count = _exact_int(
        discovery.get("portfolio_signal_count"),
        label="global portfolio_signal_count",
        minimum=1,
    )
    if total != signal_count or portfolio_signal_count != signal_count:
        raise BarStateArtifactError("global signal totals do not balance")
    return counts, signal_count


def _global_axis_resolutions(
    discovery: Mapping[str, object],
    supports: Mapping[str, Mapping[str, object]],
) -> dict[str, Mapping[str, object]]:
    raw_axes = discovery.get("axis_resolutions")
    if not isinstance(raw_axes, list) or len(raw_axes) != 12:
        raise BarStateArtifactError("global axis resolutions must contain twelve objects")
    axes: dict[str, Mapping[str, object]] = {}
    ordered_keys: list[str] = []
    for raw_axis in raw_axes:
        axis = _decoded_mapping(raw_axis, label="global axis resolution")
        if set(axis) != _BAR_STATE_AXIS_RESOLUTION_KEYS:
            raise BarStateArtifactError("global axis resolution key set drifted")
        key, _timeframe, _feature_set = _bar_state_candidate_dimensions(axis.get("candidate_key"))
        if key in axes:
            raise BarStateArtifactError("global axis resolution identity is duplicated")
        raw_hashes = axis.get("axis_vector_sha256")
        if not isinstance(raw_hashes, list) or len(raw_hashes) != 7:
            raise BarStateArtifactError("global axis resolution must bind seven vectors")
        hashes = tuple(_sha256(item, label="global axis vector") for item in raw_hashes)
        unique_count = _exact_int(
            axis.get("unique_axis_vector_count"),
            label="global unique axis vector count",
            minimum=1,
        )
        if unique_count != len(set(hashes)):
            raise BarStateArtifactError("global unique axis vector count drifted")
        filled_count = _exact_int(
            axis.get("filled_directional_signal_count"),
            label="global filled directional signal count",
            minimum=0,
        )
        if filled_count > supports[key]["raw_directional_signal_count"]:
            raise BarStateArtifactError("global filled directional count exceeds support")
        raw_histogram = axis.get("per_signal_distinct_count_histogram")
        if not isinstance(raw_histogram, list):
            raise BarStateArtifactError("global axis histogram must be a list")
        histogram_keys: list[int] = []
        histogram_total = 0
        for raw_bucket in raw_histogram:
            bucket = _decoded_mapping(raw_bucket, label="global axis histogram bucket")
            if set(bucket) != {"distinct_count", "signal_count"}:
                raise BarStateArtifactError("global axis histogram key set drifted")
            distinct_count = _exact_int(
                bucket.get("distinct_count"),
                label="global per-signal distinct axis count",
                minimum=1,
            )
            if distinct_count > 7:
                raise BarStateArtifactError("global per-signal axis count exceeds seven")
            histogram_keys.append(distinct_count)
            histogram_total += _exact_int(
                bucket.get("signal_count"),
                label="global axis histogram signal count",
                minimum=1,
            )
        if histogram_keys != sorted(set(histogram_keys)) or histogram_total != filled_count:
            raise BarStateArtifactError("global axis histogram does not balance")
        axes[key] = axis
        ordered_keys.append(key)
    if tuple(ordered_keys) != _BAR_STATE_CANDIDATE_KEYS:
        raise BarStateArtifactError("global axis resolution order/catalog drifted")
    return axes


def _global_feature_exclusion_qc(discovery: Mapping[str, object]) -> None:
    raw_qc = discovery.get("feature_exclusion_qc")
    expected_groups = (
        (300, "MORPHOLOGY"),
        (300, "STATE"),
        (1_800, "MORPHOLOGY"),
        (1_800, "STATE"),
    )
    if not isinstance(raw_qc, list) or len(raw_qc) != len(expected_groups):
        raise BarStateArtifactError("global feature exclusion QC must contain four groups")
    actual_groups: list[tuple[int, str]] = []
    for raw_group in raw_qc:
        group = _decoded_mapping(raw_group, label="global feature exclusion QC")
        if set(group) != {"exclusion_counts_by_reason", "feature_set_id", "timeframe_seconds"}:
            raise BarStateArtifactError("global feature exclusion QC key set drifted")
        timeframe = _exact_int(group.get("timeframe_seconds"), label="global feature QC timeframe")
        feature_set = group.get("feature_set_id")
        if not isinstance(feature_set, str):
            raise BarStateArtifactError("global feature QC feature_set_id must be a string")
        actual_groups.append((timeframe, feature_set))
        counts = _decoded_mapping(
            group.get("exclusion_counts_by_reason"),
            label="global feature exclusion reason counts",
        )
        allowed = {"ZERO_ATR"}
        if (timeframe, feature_set) == (300, "STATE"):
            allowed.add("MISSING_CAUSALLY_COMPLETED_30M_ATR_RETURN")
        if not set(counts).issubset(allowed):
            raise BarStateArtifactError("global feature exclusion reason is impossible")
        for reason, count in counts.items():
            if not isinstance(reason, str):  # pragma: no cover - JSON object keys are strings
                raise BarStateArtifactError("global feature exclusion reason must be a string")
            _exact_int(count, label="global feature exclusion count", minimum=1)
    if tuple(actual_groups) != expected_groups:
        raise BarStateArtifactError("global feature exclusion QC group order drifted")


def _global_memory_plan(discovery: Mapping[str, object], *, signal_count: int) -> None:
    memory = _decoded_mapping(discovery.get("memory_plan"), label="global memory plan")
    if set(memory) != _BAR_STATE_MEMORY_PLAN_KEYS:
        raise BarStateArtifactError("global memory plan key set drifted")
    expected = {
        "accumulator_count": 1_764,
        "candidate_count": 12,
        "grid_cell_count": 49,
        "input_signal_count": signal_count,
        "maximum_input_signal_count": 1_000_000,
        "resident_outcome_span_limit": 1,
        "retained_trade_record_count": 0,
        "scenario_count": 3,
    }
    if any(memory.get(key) != value for key, value in expected.items()):
        raise BarStateArtifactError("global memory plan constants or signal count drifted")
    maximum_rows = _exact_int(
        memory.get("maximum_resident_one_second_rows"),
        label="global maximum resident one-second rows",
        minimum=1,
    )
    total_rows = _exact_int(
        memory.get("one_second_row_count"),
        label="global one-second row count",
        minimum=1,
    )
    span_count = _exact_int(
        memory.get("outcome_span_count"), label="global outcome span count", minimum=1
    )
    if (
        maximum_rows != 1_481_453
        or total_rows != 7_573_041
        or span_count != 10
        or maximum_rows > total_rows
        or maximum_rows * span_count < total_rows
        or signal_count > 1_000_000
    ):
        raise BarStateArtifactError("global memory plan bounds are inconsistent")


def _global_observed_months(discovery: Mapping[str, object]) -> tuple[str, ...]:
    raw_months = discovery.get("observed_utc_months")
    if not isinstance(raw_months, list) or tuple(raw_months) != _BAR_STATE_OBSERVED_UTC_MONTHS:
        raise BarStateArtifactError("global observed UTC months differ from frozen Discovery")
    return _BAR_STATE_OBSERVED_UTC_MONTHS


@dataclass(frozen=True, slots=True)
class _BootstrapCellEvidence:
    entry_fill_count: int
    fully_loaded_net_pnl_ticks: int
    daily_net_pnl_ticks: tuple[tuple[date, int], ...]
    daily_fill_count: tuple[tuple[date, int], ...]


@lru_cache(maxsize=1)
def _bar_state_bootstrap_weights() -> tuple[object, ...]:
    from systematic_fx.research.bar_state_selection import _stationary_weights

    return tuple(_stationary_weights((117, 117, 137)))


def _global_bootstrap_evaluation_calendars(
    discovery: Mapping[str, object],
) -> tuple[Mapping[str, object], tuple[object, ...], str]:
    from systematic_fx.research.bar_state_selection import StateFoldEvaluationCalendar

    raw_calendar = _canonical_mapping(
        discovery.get("bootstrap_evaluation_calendar"),
        label="global bootstrap evaluation calendar",
    )
    if set(raw_calendar) != {
        "evaluation_calendar",
        "folds",
        "nested_split_plan_sha256",
        "outer_split_plan_sha256",
        "schema",
    }:
        raise BarStateArtifactError("global bootstrap evaluation calendar key set drifted")
    if (
        raw_calendar.get("evaluation_calendar") != "OOS_DECISIONS_PLUS_20_ACTIVE_DAY_OUTCOME_TAIL"
        or raw_calendar.get("nested_split_plan_sha256") != BAR_STATE_FROZEN_SPLIT_SHA256
        or raw_calendar.get("outer_split_plan_sha256") != BAR_STATE_OUTER_SPLIT_SHA256
        or raw_calendar.get("schema") != BAR_STATE_BOOTSTRAP_EVALUATION_CALENDAR_SCHEMA
    ):
        raise BarStateArtifactError("global bootstrap evaluation calendar identity drifted")
    declared_sha256 = _sha256(
        discovery.get("bootstrap_evaluation_calendar_sha256"),
        label="global bootstrap evaluation calendar SHA256",
    )
    computed_sha256 = canonical_sha256(raw_calendar)
    if (
        declared_sha256 != computed_sha256
        or computed_sha256 != BAR_STATE_FROZEN_BOOTSTRAP_EVALUATION_CALENDAR_SHA256
    ):
        raise BarStateArtifactError("global bootstrap evaluation calendar hash drifted")

    raw_folds = raw_calendar.get("folds")
    if not isinstance(raw_folds, list) or len(raw_folds) != 3:
        raise BarStateArtifactError("global bootstrap calendar must contain three folds")
    calendars: list[StateFoldEvaluationCalendar] = []
    flattened_dates: list[date] = []
    for raw_fold, expected_key, expected_count in zip(
        raw_folds,
        _BAR_STATE_FOLD_KEYS,
        (117, 117, 137),
        strict=True,
    ):
        fold = _decoded_mapping(raw_fold, label="global bootstrap calendar fold")
        if set(fold) != {"active_date_count", "active_dates", "fold_key"}:
            raise BarStateArtifactError("global bootstrap calendar fold key set drifted")
        raw_dates = fold.get("active_dates")
        if (
            fold.get("fold_key") != expected_key
            or _exact_int(
                fold.get("active_date_count"),
                label="global bootstrap active-date count",
                minimum=1,
            )
            != expected_count
            or not isinstance(raw_dates, list)
            or len(raw_dates) != expected_count
        ):
            raise BarStateArtifactError("global bootstrap calendar fold identity drifted")
        parsed_dates: list[date] = []
        for raw_date in raw_dates:
            if not isinstance(raw_date, str):
                raise BarStateArtifactError("global bootstrap calendar date must be text")
            try:
                parsed_date = date.fromisoformat(raw_date)
            except ValueError as error:
                raise BarStateArtifactError("global bootstrap calendar date is invalid") from error
            if parsed_date.isoformat() != raw_date:
                raise BarStateArtifactError("global bootstrap calendar date is non-canonical")
            parsed_dates.append(parsed_date)
        if tuple(parsed_dates) != tuple(sorted(set(parsed_dates))):
            raise BarStateArtifactError("global bootstrap fold calendar is not sorted/unique")
        calendar = StateFoldEvaluationCalendar(expected_key, tuple(parsed_dates))
        calendars.append(calendar)
        flattened_dates.extend(parsed_dates)
    if tuple(flattened_dates) != tuple(sorted(set(flattened_dates))):
        raise BarStateArtifactError("global bootstrap fold calendars overlap or reorder")
    return raw_calendar, tuple(calendars), computed_sha256


def _global_authoritative_bootstrap_validation(
    discovery: Mapping[str, object],
    *,
    calendars: Sequence[object],
    calendar_sha256: str,
) -> str:
    from systematic_fx.research.bar_state_selection import (
        BarStateSelectionError,
        _bootstrap_cell,
    )

    raw_cells = discovery.get("cell_summaries")
    raw_multiplicity = discovery.get("multiplicity_results")
    if not isinstance(raw_cells, list) or not isinstance(raw_multiplicity, list):
        raise BarStateArtifactError("global bootstrap evidence ledgers must be lists")
    input_sha256 = canonical_sha256(
        {
            "bootstrap_evaluation_calendar_sha256": calendar_sha256,
            "cell_summaries_sha256": canonical_sha256(raw_cells),
            "multiplicity_results_sha256": canonical_sha256(raw_multiplicity),
            "schema": "systematic_fx.bar_state_bootstrap_validation_input.v1",
        }
    )
    with _BAR_STATE_BOOTSTRAP_VALIDATION_LOCK:
        cached = _BAR_STATE_BOOTSTRAP_VALIDATION_CACHE.get(input_sha256)
    if cached is not None:
        return cached

    computed_by_daily_signature: dict[tuple[object, ...], tuple[Fraction | None, Fraction]] = {}
    bootstrap_cell_count = 0
    weights: tuple[object, ...] | None = None
    for candidate_ordinal, candidate_key in enumerate(_BAR_STATE_CANDIDATE_KEYS):
        moderate_offset = candidate_ordinal * 3 * 49 + 49
        multiplicity_offset = candidate_ordinal * 49
        for take_profit_index in range(7):
            for stop_loss_index in range(7):
                cell_offset = take_profit_index * 7 + stop_loss_index
                cell = _decoded_mapping(
                    raw_cells[moderate_offset + cell_offset],
                    label="global bootstrap moderate cell",
                )
                result = _decoded_mapping(
                    raw_multiplicity[multiplicity_offset + cell_offset],
                    label="global bootstrap multiplicity result",
                )
                reasons = tuple(
                    _reason_list(
                        result.get("rejection_reasons"),
                        label="global bootstrap rejection reasons",
                    )
                )
                if reasons not in {(), ("BOOTSTRAP_LOWER_BOUND",)}:
                    continue
                if (
                    cell.get("candidate_key") != candidate_key
                    or cell.get("scenario_id") != "MODERATE_COMBINED"
                    or result.get("candidate_key") != candidate_key
                    or result.get("take_profit_index") != take_profit_index
                    or result.get("stop_loss_index") != stop_loss_index
                ):
                    raise BarStateArtifactError("global bootstrap coordinate order drifted")
                raw_daily_net = cell.get("daily_net_pnl_ticks")
                raw_daily_fill = cell.get("daily_fill_count")
                if not isinstance(raw_daily_net, list) or not isinstance(raw_daily_fill, list):
                    raise BarStateArtifactError("global bootstrap daily evidence must be lists")
                daily_net = tuple(
                    (
                        date.fromisoformat(
                            str(
                                _decoded_mapping(value, label="global bootstrap daily net")[
                                    "active_date"
                                ]
                            )
                        ),
                        int(
                            _decoded_mapping(value, label="global bootstrap daily net")[
                                "net_pnl_ticks"
                            ]
                        ),
                    )
                    for value in raw_daily_net
                )
                daily_fill = tuple(
                    (
                        date.fromisoformat(
                            str(
                                _decoded_mapping(value, label="global bootstrap daily fill")[
                                    "active_date"
                                ]
                            )
                        ),
                        int(
                            _decoded_mapping(value, label="global bootstrap daily fill")[
                                "fill_count"
                            ]
                        ),
                    )
                    for value in raw_daily_fill
                )
                evidence = _BootstrapCellEvidence(
                    entry_fill_count=int(cell["entry_fill_count"]),
                    fully_loaded_net_pnl_ticks=int(cell["fully_loaded_net_pnl_ticks"]),
                    daily_net_pnl_ticks=daily_net,
                    daily_fill_count=daily_fill,
                )
                signature = (
                    evidence.entry_fill_count,
                    evidence.fully_loaded_net_pnl_ticks,
                    evidence.daily_net_pnl_ticks,
                    evidence.daily_fill_count,
                )
                actual = computed_by_daily_signature.get(signature)
                if actual is None:
                    if weights is None:
                        weights = _bar_state_bootstrap_weights()
                    try:
                        bootstrap = _bootstrap_cell(
                            evidence,  # type: ignore[arg-type]
                            calendars=calendars,  # type: ignore[arg-type]
                            weights=weights,  # type: ignore[arg-type]
                        )
                    except BarStateSelectionError as error:
                        raise BarStateArtifactError(
                            "global bootstrap evidence failed authoritative recomputation"
                        ) from error
                    actual = bootstrap.lower_bound_ev_ticks, bootstrap.p_value
                    computed_by_daily_signature[signature] = actual
                actual_lower, actual_p = actual
                recorded_lower = (
                    None
                    if result.get("bootstrap_lower_bound_ev_ticks") is None
                    else _fraction_value(
                        result.get("bootstrap_lower_bound_ev_ticks"),
                        label="global recorded bootstrap lower bound",
                    )
                )
                recorded_p = _fraction_value(
                    result.get("raw_p_value"),
                    label="global recorded bootstrap p-value",
                    minimum=Fraction(0),
                    maximum=Fraction(1),
                )
                expected_pass = actual_lower is not None and actual_lower > 0
                if (
                    recorded_lower != actual_lower
                    or reasons != (() if expected_pass else ("BOOTSTRAP_LOWER_BOUND",))
                    or result.get("deterministic_gate_passed") is not expected_pass
                    or recorded_p != (actual_p if expected_pass else Fraction(1))
                ):
                    raise BarStateArtifactError(
                        "global multiplicity bootstrap result differs from frozen PCG64 replay"
                    )
                bootstrap_cell_count += 1

    receipt_sha256 = canonical_sha256(
        {
            "bootstrap_cell_count": bootstrap_cell_count,
            "input_sha256": input_sha256,
            "schema": "systematic_fx.bar_state_bootstrap_validation_receipt.v1",
        }
    )
    with _BAR_STATE_BOOTSTRAP_VALIDATION_LOCK:
        if len(_BAR_STATE_BOOTSTRAP_VALIDATION_CACHE) >= _BAR_STATE_BOOTSTRAP_CACHE_MAXIMUM:
            _BAR_STATE_BOOTSTRAP_VALIDATION_CACHE.pop(
                next(iter(_BAR_STATE_BOOTSTRAP_VALIDATION_CACHE))
            )
        _BAR_STATE_BOOTSTRAP_VALIDATION_CACHE[input_sha256] = receipt_sha256
    return receipt_sha256


def _global_multiplicity_results(
    discovery: Mapping[str, object],
    selections: Mapping[str, Mapping[str, object]],
) -> dict[tuple[str, int, int], Mapping[str, object]]:
    raw_results = discovery.get("multiplicity_results")
    if not isinstance(raw_results, list) or len(raw_results) != 588:
        raise BarStateArtifactError("global multiplicity ledger must contain 588 cells")
    expected_coordinates = tuple(
        (candidate_key, take_profit_index, stop_loss_index)
        for candidate_key in _BAR_STATE_CANDIDATE_KEYS
        for take_profit_index in range(7)
        for stop_loss_index in range(7)
    )
    results: dict[tuple[str, int, int], Mapping[str, object]] = {}
    raw_p_values: dict[tuple[str, int, int], Fraction] = {}
    adjusted_p_values: dict[tuple[str, int, int], Fraction] = {}
    lower_bounds: dict[tuple[str, int, int], Fraction | None] = {}
    ordered_coordinates: list[tuple[str, int, int]] = []
    for raw_result in raw_results:
        result = _decoded_mapping(raw_result, label="global multiplicity result")
        if set(result) != _BAR_STATE_MULTIPLICITY_KEYS:
            raise BarStateArtifactError("global multiplicity result key set drifted")
        candidate_key, _timeframe, _feature_set = _bar_state_candidate_dimensions(
            result.get("candidate_key")
        )
        take_profit_index = _exact_int(
            result.get("take_profit_index"),
            label="global multiplicity take-profit index",
            minimum=0,
        )
        stop_loss_index = _exact_int(
            result.get("stop_loss_index"),
            label="global multiplicity stop-loss index",
            minimum=0,
        )
        if take_profit_index > 6 or stop_loss_index > 6:
            raise BarStateArtifactError("global multiplicity coordinate exceeds the 7x7 grid")
        coordinate = candidate_key, take_profit_index, stop_loss_index
        if coordinate in results:
            raise BarStateArtifactError("global multiplicity coordinate is duplicated")
        reasons = _reason_list(
            result.get("rejection_reasons"), label="global multiplicity rejection reasons"
        )
        deterministic = _exact_bool(
            result.get("deterministic_gate_passed"),
            label="global multiplicity deterministic gate",
        )
        if deterministic != (not reasons):
            raise BarStateArtifactError("global multiplicity reasons differ from gate state")
        raw_p = _fraction_value(
            result.get("raw_p_value"),
            label="global multiplicity raw p-value",
            minimum=Fraction(0),
            maximum=Fraction(1),
        )
        adjusted_p = _fraction_value(
            result.get("adjusted_p_value"),
            label="global multiplicity adjusted p-value",
            minimum=Fraction(0),
            maximum=Fraction(1),
        )
        raw_lower = result.get("bootstrap_lower_bound_ev_ticks")
        lower = (
            None
            if raw_lower is None
            else _fraction_value(raw_lower, label="global multiplicity bootstrap lower bound")
        )
        if deterministic:
            if lower is None or lower <= 0:
                raise BarStateArtifactError(
                    "eligible multiplicity cell lacks a positive bootstrap lower bound"
                )
            lattice_position = raw_p * 10_001
            if lattice_position.denominator != 1 or not 1 <= lattice_position.numerator <= 10_001:
                raise BarStateArtifactError(
                    "eligible multiplicity p-value is outside the 10000-replicate lattice"
                )
        elif reasons == ["BOOTSTRAP_LOWER_BOUND"]:
            if raw_p != 1 or (lower is not None and lower > 0):
                raise BarStateArtifactError(
                    "post-bootstrap rejected multiplicity evidence is inconsistent"
                )
        elif lower is not None or raw_p != 1 or "BOOTSTRAP_LOWER_BOUND" in reasons:
            raise BarStateArtifactError(
                "pre-bootstrap rejected multiplicity evidence is inconsistent"
            )
        _exact_bool(result.get("bh_rejected"), label="global multiplicity BH decision")
        results[coordinate] = result
        raw_p_values[coordinate] = raw_p
        adjusted_p_values[coordinate] = adjusted_p
        lower_bounds[coordinate] = lower
        ordered_coordinates.append(coordinate)
    if tuple(ordered_coordinates) != expected_coordinates:
        raise BarStateArtifactError("global multiplicity ledger order/universe drifted")

    family: list[tuple[Fraction, tuple[str, int, int] | None, str]] = [
        (Fraction(1), None, f"predecessor_{index:03d}") for index in range(216)
    ]
    family.extend(
        (value, key, f"state:{key[0]}:{key[1]}:{key[2]}") for key, value in raw_p_values.items()
    )
    ordered_family = sorted(family, key=lambda item: (item[0], item[2]))
    cutoff: Fraction | None = None
    for rank, (value, _coordinate, _stable) in enumerate(ordered_family, start=1):
        if value <= Fraction(1, 20) * rank / 804:
            cutoff = value
    adjusted_by_stable: dict[str, Fraction] = {}
    running = Fraction(1)
    for rank in range(804, 0, -1):
        value, _coordinate, stable = ordered_family[rank - 1]
        running = min(running, value * 804 / rank)
        adjusted_by_stable[stable] = min(Fraction(1), running)
    for coordinate, result in results.items():
        stable = f"state:{coordinate[0]}:{coordinate[1]}:{coordinate[2]}"
        expected_adjusted = adjusted_by_stable[stable]
        expected_rejected = cutoff is not None and raw_p_values[coordinate] <= cutoff
        if (
            adjusted_p_values[coordinate] != expected_adjusted
            or result["bh_rejected"] != expected_rejected
        ):
            raise BarStateArtifactError("global multiplicity BH adjustment/decision drifted")

    for candidate_key, selection in selections.items():
        reasons = selection["rejection_reasons"]
        has_selected_cell = selection["final_label"] == "FINALIST" or reasons == [
            "MAXIMUM_FINALIST_LIMIT"
        ]
        eligible = {
            (take_profit_index, stop_loss_index)
            for take_profit_index in range(7)
            for stop_loss_index in range(7)
            if results[candidate_key, take_profit_index, stop_loss_index]["bh_rejected"]
            and results[candidate_key, take_profit_index, stop_loss_index][
                "deterministic_gate_passed"
            ]
        }
        remaining = set(eligible)
        maximum_component_size = 0
        while remaining:
            stack = [min(remaining)]
            component_size = 0
            while stack:
                coordinate = stack.pop()
                if coordinate not in remaining:
                    continue
                remaining.remove(coordinate)
                component_size += 1
                take_profit_index, stop_loss_index = coordinate
                stack.extend(
                    neighbor
                    for neighbor in (
                        (take_profit_index - 1, stop_loss_index),
                        (take_profit_index + 1, stop_loss_index),
                        (take_profit_index, stop_loss_index - 1),
                        (take_profit_index, stop_loss_index + 1),
                    )
                    if neighbor in remaining
                )
            maximum_component_size = max(maximum_component_size, component_size)
        if has_selected_cell != (maximum_component_size >= 9):
            raise BarStateArtifactError(
                "global candidate label differs from its post-BH component eligibility"
            )
        if not has_selected_cell:
            if (
                selection["positive_component_size"] != 0
                or selection["positive_inner_fold_count"] != 0
                or any(
                    selection[field] is not None
                    for field in (
                        "bootstrap_lower_bound_ev_ticks",
                        "maximum_drawdown_ticks",
                        "moderate_ev_ticks",
                        "worst_fold_moderate_ev_ticks",
                    )
                )
            ):
                raise BarStateArtifactError(
                    "global rejected candidate claims selected-cell economics"
                )
            continue
        component_size = _exact_int(
            selection["positive_component_size"],
            label="global selected positive component size",
            minimum=9,
        )
        positive_folds = _exact_int(
            selection["positive_inner_fold_count"],
            label="global selected positive inner-fold count",
            minimum=2,
        )
        moderate_ev = _decimal_value(
            selection["moderate_ev_ticks"], label="global selected moderate EV"
        )
        worst_fold_ev = _decimal_value(
            selection["worst_fold_moderate_ev_ticks"],
            label="global selected worst-fold moderate EV",
        )
        maximum_drawdown = _exact_int(
            selection["maximum_drawdown_ticks"],
            label="global selected maximum drawdown",
            minimum=0,
        )
        if (
            component_size > 49
            or positive_folds > 3
            or moderate_ev is None
            or moderate_ev <= 0
            or worst_fold_ev is None
            or worst_fold_ev < -2
            or maximum_drawdown < 0  # pragma: no cover - exact-int proves this
        ):
            raise BarStateArtifactError("global selected candidate economics drifted")
        coordinate = (
            candidate_key,
            int(selection["selected_take_profit_index"]),
            int(selection["selected_stop_loss_index"]),
        )
        result = results[coordinate]
        selection_lower_raw = selection["bootstrap_lower_bound_ev_ticks"]
        selection_lower = (
            None
            if selection_lower_raw is None
            else _fraction_value(
                selection_lower_raw,
                label="global selected candidate bootstrap lower bound",
            )
        )
        if (
            not result["deterministic_gate_passed"]
            or not result["bh_rejected"]
            or selection_lower is None
            or selection_lower != lower_bounds[coordinate]
        ):
            raise BarStateArtifactError(
                "global selected candidate does not bind an eligible multiplicity cell"
            )
    return results


def _validated_active_date(value: object, *, observed_months: tuple[str, ...]) -> str:
    if not isinstance(value, str):
        raise BarStateArtifactError("global daily evidence active_date must be a string")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise BarStateArtifactError("global daily evidence active_date is invalid") from error
    if parsed.isoformat() != value or value[:7] not in observed_months:
        raise BarStateArtifactError("global daily evidence falls outside observed months")
    return value


def _global_support_gate_reasons(support: Mapping[str, object]) -> tuple[str, ...]:
    timeframe = int(support["timeframe_seconds"])
    minimum_raw, minimum_fold, minimum_days = (160, 25, 40) if timeframe == 300 else (100, 15, 35)
    raw_folds = support["raw_signal_count_by_fold"]
    if not isinstance(raw_folds, list):  # pragma: no cover - support validation proves this
        raise BarStateArtifactError("global support fold evidence must be a list")
    reasons: list[str] = []
    if int(support["raw_directional_signal_count"]) < minimum_raw:
        reasons.append("SUPPORT_RAW_SIGNALS")
    if any(
        int(_decoded_mapping(value, label="global support gate fold")["signal_count"])
        < minimum_fold
        for value in raw_folds
    ):
        reasons.append("SUPPORT_SIGNALS_PER_FOLD")
    if int(support["distinct_signal_day_count"]) < minimum_days:
        reasons.append("SUPPORT_DISTINCT_SIGNAL_DAYS")
    return tuple(reasons)


def _global_positive_component_by_coordinate(
    cells: Mapping[tuple[str, str, int, int], Mapping[str, object]],
    *,
    candidate_key: str,
) -> dict[tuple[int, int], frozenset[tuple[int, int]]]:
    positive: set[tuple[int, int]] = set()
    for take_profit_index in range(7):
        for stop_loss_index in range(7):
            baseline = cells[
                candidate_key,
                "BASELINE",
                take_profit_index,
                stop_loss_index,
            ]
            moderate = cells[
                candidate_key,
                "MODERATE_COMBINED",
                take_profit_index,
                stop_loss_index,
            ]
            baseline_ev = _decimal_value(
                baseline["fully_loaded_net_ev_ticks"],
                label="global baseline component EV",
                nullable=True,
            )
            moderate_ev = _decimal_value(
                moderate["fully_loaded_net_ev_ticks"],
                label="global moderate component EV",
                nullable=True,
            )
            if (
                baseline_ev is not None
                and baseline_ev > 0
                and moderate_ev is not None
                and moderate_ev > 0
                and int(moderate["fully_loaded_net_pnl_ticks"]) > 0
            ):
                positive.add((take_profit_index, stop_loss_index))

    result: dict[tuple[int, int], frozenset[tuple[int, int]]] = {}
    remaining = set(positive)
    while remaining:
        stack = [min(remaining)]
        component: set[tuple[int, int]] = set()
        while stack:
            coordinate = stack.pop()
            if coordinate not in remaining:
                continue
            remaining.remove(coordinate)
            component.add(coordinate)
            take_profit_index, stop_loss_index = coordinate
            stack.extend(
                neighbor
                for neighbor in (
                    (take_profit_index - 1, stop_loss_index),
                    (take_profit_index + 1, stop_loss_index),
                    (take_profit_index, stop_loss_index - 1),
                    (take_profit_index, stop_loss_index + 1),
                )
                if neighbor in remaining
            )
        frozen = frozenset(component)
        result.update(dict.fromkeys(frozen, frozen))
    return result


def _global_cell_gate_reasons(
    cells: Mapping[tuple[str, str, int, int], Mapping[str, object]],
    *,
    candidate_key: str,
    coordinate: tuple[int, int],
    positive_component_by_coordinate: Mapping[tuple[int, int], frozenset[tuple[int, int]]],
    support_reasons: Sequence[str],
    unique_axis_vector_count: int,
) -> tuple[str, ...]:
    take_profit_index, stop_loss_index = coordinate
    baseline = cells[candidate_key, "BASELINE", take_profit_index, stop_loss_index]
    moderate = cells[candidate_key, "MODERATE_COMBINED", take_profit_index, stop_loss_index]
    severe = cells[candidate_key, "SEVERE_DIAGNOSTIC", take_profit_index, stop_loss_index]
    baseline_ev = _decimal_value(
        baseline["fully_loaded_net_ev_ticks"],
        label="global baseline gate EV",
        nullable=True,
    )
    moderate_ev = _decimal_value(
        moderate["fully_loaded_net_ev_ticks"],
        label="global moderate gate EV",
        nullable=True,
    )
    severe_ev = _decimal_value(
        severe["fully_loaded_net_ev_ticks"],
        label="global severe gate EV",
        nullable=True,
    )
    reasons = list(support_reasons)
    if unique_axis_vector_count < 4:
        reasons.append("GRID_AXIS_VECTOR_COLLAPSE")
    if baseline_ev is None or baseline_ev <= 0:
        reasons.append("BASELINE_NET_EV")
    component = positive_component_by_coordinate.get(coordinate, frozenset())
    if len(component) < 9:
        reasons.append("POSITIVE_COMPONENT_SIZE")
    neighborhood = {
        (other_take_profit, other_stop_loss)
        for other_take_profit in range(max(0, take_profit_index - 1), min(7, take_profit_index + 2))
        for other_stop_loss in range(max(0, stop_loss_index - 1), min(7, stop_loss_index + 2))
    }
    positive_neighbors = {
        item
        for item in neighborhood
        if (
            (
                neighbor_baseline_ev := _decimal_value(
                    cells[candidate_key, "BASELINE", *item]["fully_loaded_net_ev_ticks"],
                    label="global neighboring baseline EV",
                    nullable=True,
                )
            )
            is not None
            and neighbor_baseline_ev > 0
            and (
                neighbor_moderate_ev := _decimal_value(
                    cells[candidate_key, "MODERATE_COMBINED", *item]["fully_loaded_net_ev_ticks"],
                    label="global neighboring moderate EV",
                    nullable=True,
                )
            )
            is not None
            and neighbor_moderate_ev > 0
            and int(cells[candidate_key, "MODERATE_COMBINED", *item]["fully_loaded_net_pnl_ticks"])
            > 0
        )
    }
    if len(positive_neighbors) < 7:
        reasons.append("POSITIVE_3X3_STABILITY")
    neighbor_evs = [
        value
        for item in sorted(neighborhood - {coordinate})
        if (
            value := _decimal_value(
                cells[candidate_key, "MODERATE_COMBINED", *item]["fully_loaded_net_ev_ticks"],
                label="global neighbor-median EV",
                nullable=True,
            )
        )
        is not None
    ]
    if (
        moderate_ev is None
        or not neighbor_evs
        or median(neighbor_evs) < moderate_ev * Decimal("0.5")
    ):
        reasons.append("NEIGHBOR_MEDIAN_EV")

    moderate_fill_count = int(moderate["entry_fill_count"])
    raw_blocks = moderate["blocks"]
    if not isinstance(raw_blocks, list):  # pragma: no cover - cell validation proves this
        raise BarStateArtifactError("global moderate gate blocks must be a list")
    blocks = tuple(
        _decoded_mapping(value, label="global moderate gate block") for value in raw_blocks
    )
    if moderate_fill_count < 40:
        reasons.append("MINIMUM_FILLED_ROUND_TRIPS")
    if any(int(block["entry_fill_count"]) < 8 for block in blocks):
        reasons.append("MINIMUM_FILLS_PER_FOLD")
    if sum(int(block["fully_loaded_net_pnl_ticks"]) > 0 for block in blocks) < 2:
        reasons.append("MINIMUM_POSITIVE_FOLDS")
    if int(moderate["fully_loaded_net_pnl_ticks"]) <= 0:
        reasons.append("MODERATE_NET_PNL")
    moderate_calendar = _decimal_value(
        moderate["calendar_month_net_pnl_usd"], label="global moderate gate calendar PnL"
    )
    if moderate_calendar is None or moderate_calendar <= 0:  # pragma: no cover - non-null above
        reasons.append("MODERATE_CALENDAR_NET_PNL")
    moderate_profit_factor = _decimal_value(
        moderate["profit_factor"],
        label="global moderate gate profit factor",
        nullable=True,
        allow_infinity=True,
    )
    if moderate_profit_factor is None or moderate_profit_factor < Decimal("1.1"):
        reasons.append("MODERATE_PROFIT_FACTOR")
    block_evs = [
        _decimal_value(
            block["fully_loaded_net_ev_ticks"],
            label="global moderate gate block EV",
            nullable=True,
        )
        for block in blocks
    ]
    if any(value is None for value in block_evs) or min(
        value for value in block_evs if value is not None
    ) < Decimal(-2):
        reasons.append("MODERATE_WORST_FOLD_EV")
    if severe_ev is None or severe_ev < 0:
        reasons.append("SEVERE_NET_EV")

    fold_positive = [int(block["positive_gross_ticks"]) for block in blocks]
    fold_total = sum(fold_positive)
    if fold_total <= 0 or Fraction(max(fold_positive, default=0), fold_total) > Fraction(1, 2):
        reasons.append("FOLD_POSITIVE_GROSS_CONCENTRATION")
    raw_contracts = moderate["positive_gross_by_contract"]
    if not isinstance(raw_contracts, list):  # pragma: no cover - cell validation proves this
        raise BarStateArtifactError("global moderate gate contracts must be a list")
    contract_positive = [
        int(_decoded_mapping(value, label="global moderate gate contract")["positive_gross_ticks"])
        for value in raw_contracts
    ]
    contract_total = sum(contract_positive)
    if contract_total <= 0 or Fraction(
        max(contract_positive, default=0), contract_total
    ) > Fraction(1, 2):
        reasons.append("CONTRACT_POSITIVE_GROSS_CONCENTRATION")
    return tuple(dict.fromkeys(reasons))


def _global_cell_summaries(
    discovery: Mapping[str, object],
    *,
    supports: Mapping[str, Mapping[str, object]],
    axes: Mapping[str, Mapping[str, object]],
    signal_counts: Mapping[str, tuple[int, int, int]],
    selections: Mapping[str, Mapping[str, object]],
    multiplicity: Mapping[tuple[str, int, int], Mapping[str, object]],
    observed_months: tuple[str, ...],
    evaluation_calendar: Mapping[str, object],
) -> dict[str, int]:
    raw_cells = discovery.get("cell_summaries")
    if not isinstance(raw_cells, list) or len(raw_cells) != 1_764:
        raise BarStateArtifactError("global portfolio surface must contain 1764 cells")
    raw_calendar_folds = evaluation_calendar.get("folds")
    if not isinstance(raw_calendar_folds, list):  # pragma: no cover - calendar validation proves it
        raise BarStateArtifactError("global evaluation calendar folds must be a list")
    calendar_dates_by_fold = {
        str(fold["fold_key"]): frozenset(str(value) for value in fold["active_dates"])
        for raw_fold in raw_calendar_folds
        for fold in (_decoded_mapping(raw_fold, label="global evaluation calendar fold"),)
    }
    visible_calendar_dates = frozenset().union(*calendar_dates_by_fold.values())
    multipliers = tuple(item.as_dict() for item in BAR_STATE_ECONOMIC_MULTIPLIERS)
    expected_coordinates = tuple(
        (candidate_key, scenario_id, take_profit_index, stop_loss_index)
        for candidate_key in _BAR_STATE_CANDIDATE_KEYS
        for scenario_id in _BAR_STATE_SCENARIO_IDS
        for take_profit_index in range(7)
        for stop_loss_index in range(7)
    )
    scenario_variable_cost = {
        "BASELINE": 4,
        "MODERATE_COMBINED": 5,
        "SEVERE_DIAGNOSTIC": 6,
    }
    scenario_allocated_fixed_cost = {
        "BASELINE": 4,
        "MODERATE_COMBINED": 5,
        "SEVERE_DIAGNOSTIC": 6,
    }
    scenario_fixed_multiplier = {
        "BASELINE": Decimal(1),
        "MODERATE_COMBINED": Decimal(5) / Decimal(4),
        "SEVERE_DIAGNOSTIC": Decimal(3) / Decimal(2),
    }
    take_profit_distinct: dict[tuple[str, int], int] = {}
    stop_loss_distinct: dict[tuple[str, int], int] = {}
    cells: dict[tuple[str, str, int, int], Mapping[str, object]] = {}
    executed_trade_records = 0
    executed_trade_records_by_candidate = dict.fromkeys(_BAR_STATE_CANDIDATE_KEYS, 0)
    actual_coordinates: list[tuple[str, str, int, int]] = []
    for raw_cell, expected_coordinate in zip(raw_cells, expected_coordinates, strict=True):
        cell = _decoded_mapping(raw_cell, label="global portfolio cell")
        if set(cell) != _BAR_STATE_CELL_KEYS:
            raise BarStateArtifactError("global portfolio cell key set drifted")
        candidate_key, scenario_id, take_profit_index, stop_loss_index = expected_coordinate
        if cell.get("candidate_key") != candidate_key or cell.get("scenario_id") != scenario_id:
            raise BarStateArtifactError("global portfolio cell identity/order drifted")
        take_profit_multiplier = multipliers[take_profit_index]
        stop_loss_multiplier = multipliers[stop_loss_index]
        if (
            cell.get("take_profit_multiplier") != take_profit_multiplier
            or cell.get("stop_loss_multiplier") != stop_loss_multiplier
            or cell.get("cell_id")
            != (
                f"tpm{take_profit_multiplier['numerator']}_"
                f"{take_profit_multiplier['denominator']}_"
                f"slm{stop_loss_multiplier['numerator']}_"
                f"{stop_loss_multiplier['denominator']}"
            )
        ):
            raise BarStateArtifactError("global portfolio cell frozen grid identity drifted")
        actual_coordinates.append(expected_coordinate)
        cells[expected_coordinate] = cell

        nonnegative_fields = (
            "allocated_fixed_cost_ticks",
            "distinct_stop_loss_distance_count",
            "distinct_take_profit_distance_count",
            "entry_fill_count",
            "entry_not_filled_count",
            "maximum_drawdown_ticks",
            "no_trade_count",
            "same_second_stop_first_count",
            "signal_count",
            "skipped_occupied_count",
            "stop_first_count",
            "take_profit_first_count",
            "terminal_exit_count",
            "variable_cost_ticks",
        )
        values = {
            field: _exact_int(cell.get(field), label=f"global portfolio {field}", minimum=0)
            for field in nonnegative_fields
        }
        gross = _exact_int(cell.get("gross_pnl_ticks"), label="global portfolio gross PnL")
        net = _exact_int(cell.get("fully_loaded_net_pnl_ticks"), label="global portfolio net PnL")
        long_count, no_trade_count, short_count = signal_counts[candidate_key]
        directional_count = long_count + short_count
        filled_directional_count = int(axes[candidate_key]["filled_directional_signal_count"])
        if (
            values["signal_count"] != directional_count + no_trade_count
            or values["no_trade_count"] != no_trade_count
            or values["entry_not_filled_count"]
            != int(supports[candidate_key]["raw_directional_signal_count"])
            - filled_directional_count
            or values["signal_count"]
            != values["no_trade_count"]
            + values["entry_not_filled_count"]
            + values["skipped_occupied_count"]
            + values["entry_fill_count"]
            or filled_directional_count
            != values["skipped_occupied_count"] + values["entry_fill_count"]
            or values["entry_fill_count"]
            != values["take_profit_first_count"]
            + values["stop_first_count"]
            + values["terminal_exit_count"]
            or values["same_second_stop_first_count"] > values["stop_first_count"]
        ):
            raise BarStateArtifactError("global portfolio cell counts do not balance")
        if (
            values["variable_cost_ticks"]
            != values["entry_fill_count"] * scenario_variable_cost[scenario_id]
            or values["allocated_fixed_cost_ticks"]
            != values["entry_fill_count"] * scenario_allocated_fixed_cost[scenario_id]
            or net != gross - values["variable_cost_ticks"] - values["allocated_fixed_cost_ticks"]
        ):
            raise BarStateArtifactError("global portfolio cell cost accounting drifted")

        expected_ev = (
            None
            if values["entry_fill_count"] == 0
            else format(Decimal(net) / Decimal(values["entry_fill_count"]), "f")
        )
        if cell.get("fully_loaded_net_ev_ticks") != expected_ev:
            raise BarStateArtifactError("global portfolio cell net EV drifted")
        calendar_value = _decimal_value(
            cell.get("calendar_month_net_pnl_usd"), label="global calendar-month net PnL"
        )
        expected_calendar = (
            Decimal(gross - values["variable_cost_ticks"]) * Decimal("6.25")
            - Decimal(len(observed_months))
            * Decimal("500.00")
            * scenario_fixed_multiplier[scenario_id]
        )
        if calendar_value != expected_calendar:
            raise BarStateArtifactError("global portfolio calendar accounting drifted")
        profit_factor = _decimal_value(
            cell.get("profit_factor"),
            label="global portfolio profit factor",
            nullable=True,
            allow_infinity=True,
        )
        if profit_factor is not None and profit_factor < 0:
            raise BarStateArtifactError("global portfolio profit factor cannot be negative")

        raw_blocks = cell.get("blocks")
        if not isinstance(raw_blocks, list) or len(raw_blocks) != 3:
            raise BarStateArtifactError("global portfolio cell lacks three fold blocks")
        block_keys: list[str] = []
        block_fill_total = 0
        block_net_total = 0
        block_positive_total = 0
        block_fill_by_key: dict[str, int] = {}
        block_net_by_key: dict[str, int] = {}
        for raw_block in raw_blocks:
            block = _decoded_mapping(raw_block, label="global portfolio block")
            if set(block) != {
                "block_key",
                "entry_fill_count",
                "fully_loaded_net_ev_ticks",
                "fully_loaded_net_pnl_ticks",
                "maximum_drawdown_ticks",
                "positive_gross_ticks",
            }:
                raise BarStateArtifactError("global portfolio block key set drifted")
            block_key = block.get("block_key")
            if not isinstance(block_key, str):
                raise BarStateArtifactError("global portfolio block key must be a string")
            block_keys.append(block_key)
            block_fills = _exact_int(
                block.get("entry_fill_count"), label="global block fill count", minimum=0
            )
            block_net = _exact_int(
                block.get("fully_loaded_net_pnl_ticks"), label="global block net PnL"
            )
            _exact_int(
                block.get("maximum_drawdown_ticks"),
                label="global block maximum drawdown",
                minimum=0,
            )
            block_positive = _exact_int(
                block.get("positive_gross_ticks"),
                label="global block positive gross",
                minimum=0,
            )
            expected_block_ev = (
                None if block_fills == 0 else format(Decimal(block_net) / Decimal(block_fills), "f")
            )
            if block.get("fully_loaded_net_ev_ticks") != expected_block_ev:
                raise BarStateArtifactError("global portfolio block EV drifted")
            block_fill_total += block_fills
            block_net_total += block_net
            block_positive_total += block_positive
            block_fill_by_key[block_key] = block_fills
            block_net_by_key[block_key] = block_net
        if (
            tuple(block_keys) != _BAR_STATE_FOLD_KEYS
            or block_fill_total != values["entry_fill_count"]
            or block_net_total != net
        ):
            raise BarStateArtifactError("global portfolio block totals do not balance")

        raw_daily_net = cell.get("daily_net_pnl_ticks")
        raw_daily_fill = cell.get("daily_fill_count")
        if not isinstance(raw_daily_net, list) or not isinstance(raw_daily_fill, list):
            raise BarStateArtifactError("global portfolio daily evidence must be lists")
        daily_net: dict[str, int] = {}
        for raw_value in raw_daily_net:
            value = _decoded_mapping(raw_value, label="global daily net PnL")
            if set(value) != {"active_date", "net_pnl_ticks"}:
                raise BarStateArtifactError("global daily net PnL key set drifted")
            active_date = _validated_active_date(
                value.get("active_date"), observed_months=observed_months
            )
            if active_date in daily_net:
                raise BarStateArtifactError("global daily net PnL date is duplicated")
            daily_net[active_date] = _exact_int(
                value.get("net_pnl_ticks"), label="global daily net PnL ticks"
            )
        daily_fill: dict[str, int] = {}
        for raw_value in raw_daily_fill:
            value = _decoded_mapping(raw_value, label="global daily fill count")
            if set(value) != {"active_date", "fill_count"}:
                raise BarStateArtifactError("global daily fill count key set drifted")
            active_date = _validated_active_date(
                value.get("active_date"), observed_months=observed_months
            )
            if active_date in daily_fill:
                raise BarStateArtifactError("global daily fill date is duplicated")
            daily_fill[active_date] = _exact_int(
                value.get("fill_count"), label="global daily fill count", minimum=1
            )
        if (
            tuple(daily_net) != tuple(sorted(daily_net))
            or tuple(daily_fill) != tuple(sorted(daily_fill))
            or tuple(daily_net) != tuple(daily_fill)
            or sum(daily_net.values()) != net
            or sum(daily_fill.values()) != values["entry_fill_count"]
        ):
            raise BarStateArtifactError("global portfolio daily evidence does not balance")
        if not set(daily_net).issubset(visible_calendar_dates) or any(
            sum(value for active_date, value in daily_net.items() if active_date in fold_dates)
            != block_net_by_key[fold_key]
            or sum(value for active_date, value in daily_fill.items() if active_date in fold_dates)
            != block_fill_by_key[fold_key]
            for fold_key, fold_dates in calendar_dates_by_fold.items()
        ):
            raise BarStateArtifactError(
                "global portfolio daily evidence differs from exact fold blocks"
            )

        raw_contracts = cell.get("positive_gross_by_contract")
        if not isinstance(raw_contracts, list):
            raise BarStateArtifactError("global positive gross by contract must be a list")
        contracts: list[str] = []
        contract_positive_total = 0
        for raw_contract in raw_contracts:
            contract = _decoded_mapping(raw_contract, label="global contract positive gross")
            if set(contract) != {"contract", "positive_gross_ticks"}:
                raise BarStateArtifactError("global contract positive gross key set drifted")
            contract_key = contract.get("contract")
            if not isinstance(contract_key, str) or not contract_key:
                raise BarStateArtifactError("global portfolio contract key is invalid")
            contracts.append(contract_key)
            contract_positive_total += _exact_int(
                contract.get("positive_gross_ticks"),
                label="global contract positive gross ticks",
                minimum=0,
            )
        if contracts != sorted(set(contracts)) or contract_positive_total != block_positive_total:
            raise BarStateArtifactError("global portfolio positive gross totals do not balance")

        tp_distinct = values["distinct_take_profit_distance_count"]
        sl_distinct = values["distinct_stop_loss_distance_count"]
        if filled_directional_count == 0:
            if tp_distinct != 0 or sl_distinct != 0:
                raise BarStateArtifactError("empty axis claims distinct realized distances")
        elif tp_distinct < 1 or sl_distinct < 1:
            raise BarStateArtifactError("filled axis lacks realized distances")
        tp_prior = take_profit_distinct.setdefault((candidate_key, take_profit_index), tp_distinct)
        sl_prior = stop_loss_distinct.setdefault((candidate_key, stop_loss_index), sl_distinct)
        if tp_prior != tp_distinct or sl_prior != sl_distinct:
            raise BarStateArtifactError("global realized distance counts vary across same axis")
        executed_trade_records += values["entry_fill_count"]
        executed_trade_records_by_candidate[candidate_key] += values["entry_fill_count"]
    if tuple(actual_coordinates) != expected_coordinates:  # pragma: no cover - zip proves this
        raise BarStateArtifactError("global portfolio coordinate universe drifted")
    portfolio_executed = _exact_int(
        discovery.get("portfolio_executed_trade_record_count"),
        label="global executed trade record count",
        minimum=0,
    )
    if portfolio_executed != executed_trade_records:
        raise BarStateArtifactError("global executed trade total differs from cell surface")

    exact_cell_reasons: dict[tuple[str, int, int], tuple[str, ...]] = {}
    for candidate_key in _BAR_STATE_CANDIDATE_KEYS:
        component_by_coordinate = _global_positive_component_by_coordinate(
            cells, candidate_key=candidate_key
        )
        support_reasons = _global_support_gate_reasons(supports[candidate_key])
        unique_axis_count = int(axes[candidate_key]["unique_axis_vector_count"])
        for take_profit_index in range(7):
            for stop_loss_index in range(7):
                coordinate = candidate_key, take_profit_index, stop_loss_index
                reasons = _global_cell_gate_reasons(
                    cells,
                    candidate_key=candidate_key,
                    coordinate=(take_profit_index, stop_loss_index),
                    positive_component_by_coordinate=component_by_coordinate,
                    support_reasons=support_reasons,
                    unique_axis_vector_count=unique_axis_count,
                )
                result = multiplicity[coordinate]
                raw_lower = result["bootstrap_lower_bound_ev_ticks"]
                lower = (
                    None
                    if raw_lower is None
                    else _fraction_value(
                        raw_lower,
                        label="global gate-recomputed bootstrap lower bound",
                    )
                )
                if not reasons and (lower is None or lower <= 0):
                    reasons = ("BOOTSTRAP_LOWER_BOUND",)
                actual_reasons = tuple(
                    _reason_list(
                        result["rejection_reasons"],
                        label="global gate-recomputed rejection reasons",
                    )
                )
                if (
                    actual_reasons != reasons
                    or result["deterministic_gate_passed"] != (not reasons)
                    or (reasons and reasons != ("BOOTSTRAP_LOWER_BOUND",) and lower is not None)
                ):
                    raise BarStateArtifactError(
                        "global multiplicity gate differs from portfolio/support/axis evidence"
                    )
                exact_cell_reasons[coordinate] = reasons

    for candidate_key, selection in selections.items():
        reasons = selection["rejection_reasons"]
        has_selected_cell = selection["final_label"] == "FINALIST" or reasons == [
            "MAXIMUM_FINALIST_LIMIT"
        ]
        if not has_selected_cell:
            expected_reasons = sorted(
                {
                    reason
                    for take_profit_index in range(7)
                    for stop_loss_index in range(7)
                    for reason in exact_cell_reasons[
                        candidate_key,
                        take_profit_index,
                        stop_loss_index,
                    ]
                }
            )
            expected_reasons.append(
                "POST_BH_COMPONENT_SIZE"
                if any(
                    multiplicity[candidate_key, take_profit_index, stop_loss_index]["bh_rejected"]
                    for take_profit_index in range(7)
                    for stop_loss_index in range(7)
                )
                else "BH_MULTIPLICITY"
            )
            if reasons != expected_reasons:
                raise BarStateArtifactError(
                    "global rejected candidate reasons differ from exact cell ledger"
                )
            continue
        take_profit_index = int(selection["selected_take_profit_index"])
        stop_loss_index = int(selection["selected_stop_loss_index"])
        moderate = cells[
            candidate_key,
            "MODERATE_COMBINED",
            take_profit_index,
            stop_loss_index,
        ]
        raw_blocks = moderate["blocks"]
        if not isinstance(raw_blocks, list):  # pragma: no cover - cell validation proves this
            raise BarStateArtifactError("global selected moderate blocks must be a list")
        block_evs = [
            _decimal_value(
                _decoded_mapping(block, label="global selected moderate block").get(
                    "fully_loaded_net_ev_ticks"
                ),
                label="global selected moderate block EV",
            )
            for block in raw_blocks
        ]
        if any(value is None for value in block_evs):
            raise BarStateArtifactError("global selected candidate has an empty moderate fold")
        expected_worst = format(
            min(value for value in block_evs if value is not None),
            "f",
        )
        positive_folds = sum(
            _exact_int(
                _decoded_mapping(block, label="global selected moderate block").get(
                    "fully_loaded_net_pnl_ticks"
                ),
                label="global selected moderate block PnL",
            )
            > 0
            for block in raw_blocks
        )
        if (
            selection["moderate_ev_ticks"] != moderate["fully_loaded_net_ev_ticks"]
            or selection["maximum_drawdown_ticks"] != moderate["maximum_drawdown_ticks"]
            or selection["worst_fold_moderate_ev_ticks"] != expected_worst
            or selection["positive_inner_fold_count"] != positive_folds
        ):
            raise BarStateArtifactError(
                "global selected candidate economics differ from moderate cell"
            )

        eligible = {
            (tp_index, sl_index)
            for tp_index in range(7)
            for sl_index in range(7)
            if multiplicity[candidate_key, tp_index, sl_index]["bh_rejected"]
            and multiplicity[candidate_key, tp_index, sl_index]["deterministic_gate_passed"]
        }
        remaining = set(eligible)
        components: list[set[tuple[int, int]]] = []
        while remaining:
            stack = [min(remaining)]
            component: set[tuple[int, int]] = set()
            while stack:
                coordinate = stack.pop()
                if coordinate not in remaining:
                    continue
                remaining.remove(coordinate)
                component.add(coordinate)
                tp_index, sl_index = coordinate
                stack.extend(
                    neighbor
                    for neighbor in (
                        (tp_index - 1, sl_index),
                        (tp_index + 1, sl_index),
                        (tp_index, sl_index - 1),
                        (tp_index, sl_index + 1),
                    )
                    if neighbor in remaining
                )
            components.append(component)
        selected_coordinate = take_profit_index, stop_loss_index
        selected_components = [
            component for component in components if selected_coordinate in component
        ]
        if len(selected_components) != 1:
            raise BarStateArtifactError("global selected cell lacks one BH component")
        component = selected_components[0]
        maximum_component_size = max((len(value) for value in components), default=0)
        if (
            len(component) != selection["positive_component_size"]
            or len(component) != maximum_component_size
            or len(component) < 9
        ):
            raise BarStateArtifactError(
                "global selected cell differs from its largest post-BH component medoid"
            )

        medoid_candidates: list[tuple[int, int]] = []
        for largest_component in (
            value for value in components if len(value) == maximum_component_size
        ):
            medoid_scores = {
                coordinate: sum(
                    abs(coordinate[0] - other[0]) + abs(coordinate[1] - other[1])
                    for other in largest_component
                )
                for coordinate in largest_component
            }
            minimum_score = min(medoid_scores.values())
            medoid_candidates.extend(
                coordinate for coordinate, score in medoid_scores.items() if score == minimum_score
            )

        def cell_rank(
            coordinate: tuple[int, int], ranked_candidate_key: str = candidate_key
        ) -> tuple[object, ...]:
            ranked_moderate = cells[
                ranked_candidate_key,
                "MODERATE_COMBINED",
                coordinate[0],
                coordinate[1],
            ]
            ranked_blocks = ranked_moderate["blocks"]
            if not isinstance(ranked_blocks, list):  # pragma: no cover - validation proves this
                raise BarStateArtifactError("global ranked moderate blocks must be a list")
            ranked_block_evs = [
                _decimal_value(
                    _decoded_mapping(block, label="global ranked moderate block")[
                        "fully_loaded_net_ev_ticks"
                    ],
                    label="global ranked moderate block EV",
                )
                for block in ranked_blocks
            ]
            ranked_moderate_ev = _decimal_value(
                ranked_moderate["fully_loaded_net_ev_ticks"],
                label="global ranked moderate EV",
            )
            ranked_lower = _fraction_value(
                multiplicity[ranked_candidate_key, coordinate[0], coordinate[1]][
                    "bootstrap_lower_bound_ev_ticks"
                ],
                label="global ranked bootstrap lower bound",
            )
            if ranked_moderate_ev is None or any(value is None for value in ranked_block_evs):
                raise BarStateArtifactError("global ranked cell lacks exact economics")
            return (
                min(value for value in ranked_block_evs if value is not None),
                ranked_moderate_ev,
                Decimal(ranked_lower.numerator) / Decimal(ranked_lower.denominator),
                -int(ranked_moderate["maximum_drawdown_ticks"]),
                -coordinate[1],
                -coordinate[0],
                ranked_candidate_key,
            )

        if selected_coordinate != max(medoid_candidates, key=cell_rank):
            raise BarStateArtifactError(
                "global selected cell differs from the frozen post-BH cell rank"
            )
    return executed_trade_records_by_candidate


@dataclass(frozen=True, slots=True)
class BarStateGlobalResultProjection:
    """Semantic projection cryptographically shared by GLOBAL and TERMINAL evidence."""

    candidate_selections: Mapping[str, Mapping[str, object]]
    finalist_bindings: Mapping[str, Mapping[str, object]]
    finalist_keys: tuple[str, ...]
    candidate_selection_sha256_by_key: Mapping[str, str]
    candidate_selection_projection_sha256_by_key: Mapping[str, str]
    candidate_evidence_slice_by_key: Mapping[str, Mapping[str, object]]
    candidate_evidence_slice_sha256_by_key: Mapping[str, str]
    candidate_oos_trade_record_count_by_key: Mapping[str, int]
    finalist_model_binding_sha256_by_key: Mapping[str, str]
    bootstrap_validation_sha256: str
    evidence_projection: Mapping[str, object]
    evidence_projection_sha256: str


def bar_state_global_result_projection(
    document: Mapping[str, object],
) -> BarStateGlobalResultProjection:
    """Validate the exact production GLOBAL schema and finalist-model bindings."""

    outer = _canonical_mapping(document, label="global result document")
    if (
        set(outer) != {"candidate_count", "discovery_result", "schema"}
        or outer.get("candidate_count") != 12
        or outer.get("schema") != BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["GLOBAL_RESULT"]
    ):
        raise BarStateArtifactError("global result document has an unexpected schema or key set")
    discovery = _decoded_mapping(outer.get("discovery_result"), label="global Discovery result")
    if (
        set(discovery) != BAR_STATE_GLOBAL_DISCOVERY_KEYS
        or discovery.get("schema") != "systematic_fx.bar_state_selection.v1"
        or discovery.get("bh_family_size") != 804
        or discovery.get("bootstrap_convention") != _BAR_STATE_BOOTSTRAP_CONVENTION
    ):
        raise BarStateArtifactError("global Discovery result has an unexpected schema or key set")

    raw_selections = discovery.get("candidate_results")
    raw_finalists = discovery.get("finalist_keys")
    raw_bindings = discovery.get("discovery_finalist_model_bindings")
    raw_models = discovery.get("discovery_final_fit_models")
    if (
        not isinstance(raw_selections, list)
        or len(raw_selections) != 12
        or not isinstance(raw_finalists, list)
        or not isinstance(raw_bindings, list)
        or not isinstance(raw_models, list)
    ):
        raise BarStateArtifactError("global Discovery result lacks its exact candidate universe")

    selections: dict[str, Mapping[str, object]] = {}
    for raw_selection in raw_selections:
        if not isinstance(raw_selection, Mapping):
            raise BarStateArtifactError("global candidate selection must be an object")
        selection = bar_state_candidate_selection(raw_selection)
        key = str(selection["candidate_key"])
        if key in selections:
            raise BarStateArtifactError("global candidate selection identities are not unique")
        selections[key] = MappingProxyType(selection)
    if len(selections) != 12:
        raise BarStateArtifactError("global candidate selection universe is not twelve")
    if tuple(selections) != _BAR_STATE_CANDIDATE_KEYS:
        raise BarStateArtifactError("global candidate selections differ from frozen catalog order")

    supports = _global_candidate_support(discovery)
    signal_counts, signal_count = _global_signal_counts(discovery, supports)
    axes = _global_axis_resolutions(discovery, supports)
    _global_feature_exclusion_qc(discovery)
    _global_memory_plan(discovery, signal_count=signal_count)
    observed_months = _global_observed_months(discovery)
    bootstrap_calendar, bootstrap_calendars, bootstrap_calendar_sha256 = (
        _global_bootstrap_evaluation_calendars(discovery)
    )
    multiplicity = _global_multiplicity_results(discovery, selections)
    oos_trade_record_counts = _global_cell_summaries(
        discovery,
        supports=supports,
        axes=axes,
        signal_counts=signal_counts,
        selections=selections,
        multiplicity=multiplicity,
        observed_months=observed_months,
        evaluation_calendar=bootstrap_calendar,
    )
    bootstrap_validation_sha256 = _global_authoritative_bootstrap_validation(
        discovery,
        calendars=bootstrap_calendars,
        calendar_sha256=bootstrap_calendar_sha256,
    )

    finalists = tuple(_bar_state_candidate_dimensions(item)[0] for item in raw_finalists)
    selected_keys = {
        key for key, selection in selections.items() if selection["final_label"] == "FINALIST"
    }
    if (
        len(finalists) > 4
        or len(set(finalists)) != len(finalists)
        or set(finalists) != selected_keys
    ):
        raise BarStateArtifactError("global finalists differ from exact candidate selections")

    def finalist_rank_key(candidate_key: str) -> tuple[object, ...]:
        selection = selections[candidate_key]
        lower = _fraction_value(
            selection["bootstrap_lower_bound_ev_ticks"],
            label="global finalist-rank bootstrap lower bound",
        )
        worst = _decimal_value(
            selection["worst_fold_moderate_ev_ticks"],
            label="global finalist-rank worst-fold EV",
        )
        moderate = _decimal_value(
            selection["moderate_ev_ticks"], label="global finalist-rank moderate EV"
        )
        if worst is None or moderate is None:  # pragma: no cover - multiplicity proves this
            raise BarStateArtifactError("global finalist rank lacks economics")
        lower_decimal = Decimal(lower.numerator) / Decimal(lower.denominator)
        return (
            -int(selection["positive_inner_fold_count"]),
            -worst,
            -lower_decimal,
            -moderate,
            int(selection["maximum_drawdown_ticks"]),
            int(selection["selected_stop_loss_index"]),
            int(selection["selected_take_profit_index"]),
            candidate_key,
        )

    qualified_keys = tuple(
        key
        for key, selection in selections.items()
        if selection["final_label"] == "FINALIST"
        or selection["rejection_reasons"] == ["MAXIMUM_FINALIST_LIMIT"]
    )
    ranked_qualified = tuple(sorted(qualified_keys, key=finalist_rank_key))
    if finalists != ranked_qualified[:4]:
        raise BarStateArtifactError("global finalist rank order differs from frozen policy")

    bindings: dict[str, Mapping[str, object]] = {}
    ordered_binding_keys: list[str] = []
    for raw_binding in raw_bindings:
        binding = _canonical_mapping(raw_binding, label="global finalist model binding")
        if set(binding) != BAR_STATE_FINALIST_MODEL_BINDING_KEYS:
            raise BarStateArtifactError("global finalist model binding key set drifted")
        key, timeframe, feature_set = _bar_state_candidate_dimensions(binding.get("candidate_key"))
        if (
            binding.get("timeframe_seconds") != timeframe
            or binding.get("feature_set_id") != feature_set
        ):
            raise BarStateArtifactError("global finalist model binding dimensions drifted")
        _sha256(binding.get("model_sha256"), label="global finalist model_sha256")
        if key in bindings:
            raise BarStateArtifactError("global finalist model binding is duplicated")
        bindings[key] = binding
        ordered_binding_keys.append(key)
    if tuple(ordered_binding_keys) != finalists:
        raise BarStateArtifactError("global finalist model bindings differ from finalist order")

    model_sha256_by_group: dict[tuple[int, str], str] = {}
    model_groups_in_order: list[tuple[int, str]] = []
    for raw_model in raw_models:
        model = _canonical_mapping(raw_model, label="global Discovery final-fit model")
        if (
            set(model) != BAR_STATE_GLOBAL_FINAL_FIT_MODEL_KEYS
            or model.get("fit_key") != "discovery_final_fit"
            or model.get("schema") != "systematic_fx.bar_state_final_fit_model.v1"
            or model.get("training_decision_end_active_ordinal") != 469
            or model.get("label_maturity_end_active_ordinal") != 489
            or model.get("timeframe_seconds") not in {300, 1800}
            or model.get("feature_set_id") not in {"MORPHOLOGY", "STATE"}
        ):
            raise BarStateArtifactError("global Discovery final-fit model schema drifted")
        model_body = _canonical_mapping(
            model.get("model"), label="global Discovery final-fit model body"
        )
        model_sha256 = _sha256(
            model.get("model_sha256"), label="global Discovery final-fit model_sha256"
        )
        try:
            parsed_model = CanonicalBarStateModel.from_canonical_bytes(
                canonical_json_bytes(model_body) + b"\n"
            )
        except (BarStateModelError, TypeError, ValueError) as error:
            raise BarStateArtifactError(
                "global Discovery final-fit model failed strict decoding"
            ) from error
        if (
            parsed_model.sha256 != model_sha256
            or parsed_model.timeframe_seconds != model.get("timeframe_seconds")
            or parsed_model.feature_set_id != model.get("feature_set_id")
            or parsed_model.model_id
            != (
                f"bsv2_tf{parsed_model.timeframe_seconds:04d}_"
                f"fs{parsed_model.feature_set_id.lower()}_discovery_final_fit"
            )
        ):
            raise BarStateArtifactError("global Discovery final-fit model identity/content drifted")
        group = int(model["timeframe_seconds"]), str(model["feature_set_id"])
        if group in model_sha256_by_group:
            raise BarStateArtifactError("global Discovery final-fit model group is duplicated")
        model_sha256_by_group[group] = model_sha256
        model_groups_in_order.append(group)
    if model_groups_in_order != sorted(model_groups_in_order):
        raise BarStateArtifactError("global Discovery final-fit model catalog order drifted")
    binding_sha256_by_group: dict[tuple[int, str], str] = {}
    for binding in bindings.values():
        group = int(binding["timeframe_seconds"]), str(binding["feature_set_id"])
        prior = binding_sha256_by_group.setdefault(group, str(binding["model_sha256"]))
        if prior != binding["model_sha256"]:
            raise BarStateArtifactError(
                "same finalist model group binds different final-fit models"
            )
    if model_sha256_by_group != binding_sha256_by_group:
        raise BarStateArtifactError("global final-fit models differ from finalist bindings")

    selection_hashes = {key: canonical_sha256(selection) for key, selection in selections.items()}
    selection_projection_hashes = {
        key: canonical_sha256(bar_state_candidate_selection_projection(selection))
        for key, selection in selections.items()
    }
    binding_hashes = {key: canonical_sha256(bindings.get(key)) for key in selections}
    candidate_evidence_slices = {
        key: {
            "candidate_support": dict(supports[key]),
            "multiplicity_cells": [
                dict(multiplicity[key, take_profit_index, stop_loss_index])
                for take_profit_index in range(7)
                for stop_loss_index in range(7)
            ],
        }
        for key in selections
    }
    candidate_evidence_slice_hashes = {
        key: canonical_sha256(value) for key, value in candidate_evidence_slices.items()
    }
    evidence_projection = {
        "axis_resolution_count": len(discovery["axis_resolutions"]),  # type: ignore[arg-type]
        "axis_resolutions_sha256": canonical_sha256(discovery["axis_resolutions"]),
        "bh_family_size": discovery["bh_family_size"],
        "bootstrap_convention": discovery["bootstrap_convention"],
        "bootstrap_evaluation_calendar_sha256": bootstrap_calendar_sha256,
        "bootstrap_evaluation_fold_count": len(bootstrap_calendar["folds"]),  # type: ignore[arg-type]
        "bootstrap_validation_sha256": bootstrap_validation_sha256,
        "candidate_signal_decision_counts_sha256": canonical_sha256(
            discovery["candidate_signal_decision_counts"]
        ),
        "candidate_oos_trade_record_count_by_key": oos_trade_record_counts,
        "candidate_support_count": len(discovery["candidate_support"]),  # type: ignore[arg-type]
        "candidate_support_sha256": canonical_sha256(discovery["candidate_support"]),
        "cell_summaries_sha256": canonical_sha256(discovery["cell_summaries"]),
        "cell_summary_count": len(discovery["cell_summaries"]),  # type: ignore[arg-type]
        "feature_exclusion_qc_count": len(discovery["feature_exclusion_qc"]),  # type: ignore[arg-type]
        "feature_exclusion_qc_sha256": canonical_sha256(discovery["feature_exclusion_qc"]),
        "memory_plan_sha256": canonical_sha256(discovery["memory_plan"]),
        "multiplicity_result_count": len(discovery["multiplicity_results"]),  # type: ignore[arg-type]
        "multiplicity_results_sha256": canonical_sha256(discovery["multiplicity_results"]),
        "observed_utc_month_count": len(observed_months),
        "observed_utc_months_sha256": canonical_sha256(discovery["observed_utc_months"]),
        "portfolio_executed_trade_record_count": discovery["portfolio_executed_trade_record_count"],
        "portfolio_signal_count": discovery["portfolio_signal_count"],
        "schema": BAR_STATE_GLOBAL_EVIDENCE_PROJECTION_SCHEMA,
        "signal_count": discovery["signal_count"],
    }
    return BarStateGlobalResultProjection(
        candidate_selections=MappingProxyType(selections),
        finalist_bindings=MappingProxyType(bindings),
        finalist_keys=finalists,
        candidate_selection_sha256_by_key=MappingProxyType(selection_hashes),
        candidate_selection_projection_sha256_by_key=MappingProxyType(selection_projection_hashes),
        candidate_evidence_slice_by_key=MappingProxyType(candidate_evidence_slices),
        candidate_evidence_slice_sha256_by_key=MappingProxyType(candidate_evidence_slice_hashes),
        candidate_oos_trade_record_count_by_key=MappingProxyType(oos_trade_record_counts),
        finalist_model_binding_sha256_by_key=MappingProxyType(binding_hashes),
        bootstrap_validation_sha256=bootstrap_validation_sha256,
        evidence_projection=MappingProxyType(evidence_projection),
        evidence_projection_sha256=canonical_sha256(evidence_projection),
    )


def validate_bar_state_global_bootstrap(
    document: Mapping[str, object],
    *,
    split_plan: BarStateSplitPlan | None = None,
) -> str:
    """Authoritatively replay GLOBAL bootstrap evidence and optionally bind its source plan."""

    projection = bar_state_global_result_projection(document)
    if split_plan is not None:
        expected_calendar = frozen_bar_state_bootstrap_evaluation_calendar(split_plan)
        outer = _canonical_mapping(document, label="global bootstrap-bound document")
        discovery = _decoded_mapping(
            outer.get("discovery_result"), label="global bootstrap-bound Discovery result"
        )
        actual_calendar = _canonical_mapping(
            discovery.get("bootstrap_evaluation_calendar"),
            label="global bootstrap-bound evaluation calendar",
        )
        if actual_calendar != expected_calendar:
            raise BarStateArtifactError(
                "global bootstrap calendar differs from the prepared frozen split"
            )
    return projection.bootstrap_validation_sha256


def bar_state_model_package_binding(
    document: Mapping[str, object],
    *,
    expected_candidate_key: str,
    expected_binding: Mapping[str, object] | None,
) -> Mapping[str, object] | None:
    """Strictly decode a candidate MODEL package and recover its final-fit binding."""

    candidate_key, timeframe, feature_set = _bar_state_candidate_dimensions(expected_candidate_key)
    outer = _canonical_mapping(document, label="candidate MODEL package")
    if (
        set(outer) != {"candidate_key", "fold_model_count", "fold_models", "schema"}
        or outer.get("candidate_key") != candidate_key
        or outer.get("schema") != BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["MODEL"]
    ):
        raise BarStateArtifactError("candidate MODEL package schema or identity drifted")
    raw_models = outer.get("fold_models")
    if (
        not isinstance(raw_models, list)
        or isinstance(outer.get("fold_model_count"), bool)
        or outer.get("fold_model_count") != len(raw_models)
    ):
        raise BarStateArtifactError("candidate MODEL package count drifted")
    binding = (
        None
        if expected_binding is None
        else _canonical_mapping(expected_binding, label="candidate MODEL finalist binding")
    )
    if binding is not None and (
        set(binding) != BAR_STATE_FINALIST_MODEL_BINDING_KEYS
        or binding.get("candidate_key") != candidate_key
        or binding.get("timeframe_seconds") != timeframe
        or binding.get("feature_set_id") != feature_set
    ):
        raise BarStateArtifactError("candidate MODEL finalist binding drifted")
    expected_count = 3 + int(binding is not None)
    if len(raw_models) != expected_count:
        raise BarStateArtifactError("candidate MODEL package final-fit cardinality drifted")
    for index, raw_model in enumerate(raw_models, start=1):
        wrapper = _canonical_mapping(raw_model, label="candidate MODEL wrapper")
        final_fit = index == 4
        expected_keys = (
            BAR_STATE_GLOBAL_FINAL_FIT_MODEL_KEYS - {"feature_set_id", "timeframe_seconds"}
            if final_fit
            else frozenset({"fold_key", "model", "model_sha256", "schema"})
        )
        if set(wrapper) != expected_keys:
            raise BarStateArtifactError("candidate MODEL wrapper key set drifted")
        if final_fit:
            if (
                wrapper.get("fit_key") != "discovery_final_fit"
                or wrapper.get("schema") != "systematic_fx.bar_state_final_fit_model.v1"
                or wrapper.get("training_decision_end_active_ordinal") != 469
                or wrapper.get("label_maturity_end_active_ordinal") != 489
            ):
                raise BarStateArtifactError("candidate final-fit MODEL wrapper drifted")
            expected_model_id = (
                f"bsv2_tf{timeframe:04d}_fs{feature_set.lower()}_discovery_final_fit"
            )
        else:
            if (
                wrapper.get("fold_key") != f"discovery_inner_{index}"
                or wrapper.get("schema") != "systematic_fx.bar_state_fold_model.v1"
            ):
                raise BarStateArtifactError("candidate inner MODEL wrapper drifted")
            expected_model_id = (
                f"bsv2_tf{timeframe:04d}_fs{feature_set.lower()}_discovery_inner_{index}"
            )
        model_body = _canonical_mapping(wrapper.get("model"), label="candidate MODEL body")
        model_sha256 = _sha256(wrapper.get("model_sha256"), label="candidate MODEL model_sha256")
        try:
            parsed_model = CanonicalBarStateModel.from_canonical_bytes(
                canonical_json_bytes(model_body) + b"\n"
            )
        except (BarStateModelError, TypeError, ValueError) as error:
            raise BarStateArtifactError("candidate MODEL failed strict decoding") from error
        if (
            parsed_model.sha256 != model_sha256
            or parsed_model.model_id != expected_model_id
            or parsed_model.timeframe_seconds != timeframe
            or parsed_model.feature_set_id != feature_set
        ):
            raise BarStateArtifactError("candidate MODEL identity/content drifted")
        if final_fit and binding is not None and binding.get("model_sha256") != model_sha256:
            raise BarStateArtifactError("candidate MODEL differs from finalist binding")
    return binding


@dataclass(frozen=True, slots=True)
class BarStateModelPackageProjection:
    """Non-executable semantic identity of one verified candidate MODEL package."""

    binding: Mapping[str, object] | None
    projection: Mapping[str, object]
    record_count: int
    sha256: str


def bar_state_model_package_projection(
    document: Mapping[str, object],
    *,
    expected_candidate_key: str,
    expected_binding: Mapping[str, object] | None,
) -> BarStateModelPackageProjection:
    """Verify a MODEL package and hash its exact fold/final-fit model identities."""

    binding = bar_state_model_package_binding(
        document,
        expected_candidate_key=expected_candidate_key,
        expected_binding=expected_binding,
    )
    outer = _canonical_mapping(document, label="candidate MODEL package projection")
    raw_models = outer["fold_models"]
    if not isinstance(raw_models, list):  # pragma: no cover - binding validator proves this
        raise BarStateArtifactError("candidate MODEL package models must be a list")
    fit_keys: list[str] = []
    model_sha256_by_fit_key: dict[str, str] = {}
    wrapper_sha256_by_fit_key: dict[str, str] = {}
    for raw_model in raw_models:
        wrapper = _canonical_mapping(raw_model, label="candidate MODEL projection wrapper")
        model_sha256 = _sha256(
            wrapper.get("model_sha256"), label="candidate MODEL projection model_sha256"
        )
        if wrapper.get("schema") == "systematic_fx.bar_state_final_fit_model.v1":
            fit_key = "discovery_final_fit"
        else:
            fold_key = wrapper.get("fold_key")
            if not isinstance(fold_key, str):  # pragma: no cover - binding validator proves this
                raise BarStateArtifactError("candidate MODEL projection lacks fold_key")
            fit_key = fold_key
        fit_keys.append(fit_key)
        model_sha256_by_fit_key[fit_key] = model_sha256
        wrapper_sha256_by_fit_key[fit_key] = canonical_sha256(wrapper)
    final_fit_count = int(binding is not None)
    projection = {
        "candidate_key": expected_candidate_key,
        "final_fit_model_count": final_fit_count,
        "finalist_model_binding_sha256": canonical_sha256(binding),
        "fit_keys": fit_keys,
        "inner_model_count": 3,
        "model_sha256_by_fit_key": model_sha256_by_fit_key,
        "record_count": len(raw_models),
        "schema": "systematic_fx.bar_state_model_package_projection.v1",
        "wrapper_count": len(raw_models),
        "wrapper_sha256_by_fit_key": wrapper_sha256_by_fit_key,
    }
    frozen_binding = None if binding is None else MappingProxyType(dict(binding))
    return BarStateModelPackageProjection(
        binding=frozen_binding,
        projection=MappingProxyType(projection),
        record_count=len(raw_models),
        sha256=canonical_sha256(projection),
    )


def _validated_terminal_multiplicity_cells(
    raw_cells: object,
    *,
    expected_candidate_key: str,
) -> tuple[Mapping[str, object], ...]:
    if not isinstance(raw_cells, list) or len(raw_cells) != 49:
        raise BarStateArtifactError("terminal multiplicity slice must contain exactly 49 cells")
    result: list[Mapping[str, object]] = []
    expected_coordinates = tuple(
        (take_profit_index, stop_loss_index)
        for take_profit_index in range(7)
        for stop_loss_index in range(7)
    )
    for raw_cell, expected_coordinate in zip(raw_cells, expected_coordinates, strict=True):
        cell = _decoded_mapping(raw_cell, label="terminal multiplicity cell")
        if set(cell) != _BAR_STATE_MULTIPLICITY_KEYS:
            raise BarStateArtifactError("terminal multiplicity cell key set drifted")
        if cell.get("candidate_key") != expected_candidate_key:
            raise BarStateArtifactError("terminal multiplicity candidate identity drifted")
        take_profit_index = _exact_int(
            cell.get("take_profit_index"),
            label="terminal multiplicity take-profit index",
            minimum=0,
        )
        stop_loss_index = _exact_int(
            cell.get("stop_loss_index"),
            label="terminal multiplicity stop-loss index",
            minimum=0,
        )
        if (take_profit_index, stop_loss_index) != expected_coordinate:
            raise BarStateArtifactError("terminal multiplicity coordinate order drifted")
        reasons = _reason_list(
            cell.get("rejection_reasons"), label="terminal multiplicity rejection reasons"
        )
        deterministic = _exact_bool(
            cell.get("deterministic_gate_passed"),
            label="terminal multiplicity deterministic gate",
        )
        if deterministic != (not reasons):
            raise BarStateArtifactError("terminal multiplicity reasons differ from gate state")
        raw_p = _fraction_value(
            cell.get("raw_p_value"),
            label="terminal multiplicity raw p-value",
            minimum=Fraction(0),
            maximum=Fraction(1),
        )
        _fraction_value(
            cell.get("adjusted_p_value"),
            label="terminal multiplicity adjusted p-value",
            minimum=Fraction(0),
            maximum=Fraction(1),
        )
        _exact_bool(cell.get("bh_rejected"), label="terminal multiplicity BH decision")
        raw_lower = cell.get("bootstrap_lower_bound_ev_ticks")
        lower = (
            None
            if raw_lower is None
            else _fraction_value(raw_lower, label="terminal multiplicity bootstrap lower bound")
        )
        if deterministic:
            lattice_position = raw_p * 10_001
            if (
                lower is None
                or lower <= 0
                or lattice_position.denominator != 1
                or not 1 <= lattice_position.numerator <= 10_001
            ):
                raise BarStateArtifactError("terminal eligible multiplicity evidence drifted")
        elif reasons == ["BOOTSTRAP_LOWER_BOUND"]:
            if raw_p != 1 or (lower is not None and lower > 0):
                raise BarStateArtifactError("terminal post-bootstrap evidence drifted")
        elif lower is not None or raw_p != 1 or "BOOTSTRAP_LOWER_BOUND" in reasons:
            raise BarStateArtifactError("terminal pre-bootstrap evidence drifted")
        result.append(cell)
    return tuple(result)


def bar_state_terminal_compact_summary(
    document: Mapping[str, object],
) -> dict[str, object]:
    """Recompute the sole compact projection authorized by terminal JSON evidence."""

    outer = _canonical_mapping(document, label="terminal document")
    if (
        set(outer)
        != {
            "candidate_key",
            "compact_summary",
            "decision_label",
            "result",
            "schema",
            "trial_status",
        }
        or outer.get("schema") != BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["TERMINAL_RESULT"]
    ):
        raise BarStateArtifactError("terminal document has an unexpected schema or key set")
    result = _canonical_mapping(outer.get("result"), label="terminal result")
    if (
        set(result)
        != {
            "candidate_selection",
            "candidate_support",
            "discovery_final_fit_model",
            "multiplicity_cells",
            "price_policy",
            "schema",
        }
        or result.get("schema") != "systematic_fx.bar_state_candidate_result.v1"
    ):
        raise BarStateArtifactError("terminal result has an unexpected schema or key set")
    candidate_key = outer.get("candidate_key")
    _bar_state_candidate_dimensions(candidate_key)
    selection = bar_state_candidate_selection(
        result.get("candidate_selection"),  # type: ignore[arg-type]
        expected_candidate_key=str(candidate_key),
    )
    _validated_candidate_support(
        result.get("candidate_support"),
        label="terminal candidate support",
        expected_candidate_key=str(candidate_key),
    )
    _validated_terminal_multiplicity_cells(
        result.get("multiplicity_cells"),
        expected_candidate_key=str(candidate_key),
    )
    trial_status = outer.get("trial_status")
    decision_label = outer.get("decision_label")
    final_label = selection.get("final_label")
    if final_label not in {"FINALIST", "REJECTED"}:
        raise BarStateArtifactError("terminal final_label must be FINALIST or REJECTED")
    selected = final_label == "FINALIST"
    if trial_status != ("SUCCEEDED" if selected else "REJECTED") or decision_label != (
        "DISCOVERY_FINALIST" if selected else "DISCOVERY_REJECT"
    ):
        raise BarStateArtifactError("terminal result identity or decision state drifted")
    final_fit = result.get("discovery_final_fit_model")
    if selected:
        final_fit_mapping = _canonical_mapping(
            final_fit,
            label="terminal Discovery final-fit binding",
        )
        if (
            set(final_fit_mapping) != BAR_STATE_FINALIST_MODEL_BINDING_KEYS
            or final_fit_mapping.get("candidate_key") != candidate_key
        ):
            raise BarStateArtifactError("terminal final-fit binding key set or identity drifted")
        _key, expected_timeframe, expected_feature_set = _bar_state_candidate_dimensions(
            candidate_key
        )
        if (
            final_fit_mapping.get("timeframe_seconds") != expected_timeframe
            or final_fit_mapping.get("feature_set_id") != expected_feature_set
        ):
            raise BarStateArtifactError("terminal final-fit binding dimensions drifted")
        model_sha256 = _sha256(
            final_fit_mapping.get("model_sha256"),
            label="terminal final-fit model_sha256",
        )
    elif final_fit is None:
        model_sha256 = None
    else:
        raise BarStateArtifactError("non-finalist terminal result claims a final-fit model")
    expected_price_policy = bar_state_price_policy_from_selection(selection)
    if result.get("price_policy") != expected_price_policy:
        raise BarStateArtifactError(
            "terminal price policy differs from the frozen selected economic-axis cell"
        )
    compact = {
        "candidate_key": candidate_key,
        "discovery_final_fit_model_sha256": model_sha256,
        "final_label": final_label,
        "positive_component_size": selection.get("positive_component_size"),
        "price_policy": expected_price_policy,
        "rejection_reasons": selection.get("rejection_reasons"),
        "selected_stop_loss_index": selection.get("selected_stop_loss_index"),
        "selected_take_profit_index": selection.get("selected_take_profit_index"),
    }
    encoded = _canonical_mapping(outer.get("compact_summary"), label="terminal compact summary")
    if set(encoded) != BAR_STATE_TERMINAL_COMPACT_KEYS or dict(encoded) != compact:
        raise BarStateArtifactError("terminal compact summary differs from its result projection")
    return compact


@dataclass(frozen=True, slots=True)
class BarStateDiscoveryScope:
    """Exact visible interval that the v2 Discovery runner may open."""

    split_plan_sha256: str
    split_key: str
    result_visibility: str
    start_date: str
    decision_end_date: str
    outcome_end_date: str
    start_active_ordinal: int
    decision_end_active_ordinal: int
    outcome_end_active_ordinal: int

    def __post_init__(self) -> None:
        _sha256(self.split_plan_sha256, label="split_plan_sha256")
        if self.split_plan_sha256 != BAR_STATE_SPLIT_PLAN_SHA256:
            raise BarStateArtifactError("bar-state v2 requires the frozen split plan")
        expected: dict[str, object] = {
            "split_key": "discovery",
            "result_visibility": "VISIBLE",
            "start_date": "2022-01-03",
            "decision_end_date": "2023-07-10",
            "outcome_end_date": "2023-08-02",
            "start_active_ordinal": 1,
            "decision_end_active_ordinal": 469,
            "outcome_end_active_ordinal": 489,
        }
        for field, value in expected.items():
            if getattr(self, field) != value:
                raise BarStateArtifactError(
                    f"Discovery scope {field} must equal the frozen value {value!r}"
                )

    def as_dict(self) -> dict[str, object]:
        return {
            "decision_end_active_ordinal": self.decision_end_active_ordinal,
            "decision_end_date": self.decision_end_date,
            "outcome_end_active_ordinal": self.outcome_end_active_ordinal,
            "outcome_end_date": self.outcome_end_date,
            "result_visibility": self.result_visibility,
            "schema": BAR_STATE_DISCOVERY_SCOPE_SCHEMA,
            "split_key": self.split_key,
            "split_plan_sha256": self.split_plan_sha256,
            "start_active_ordinal": self.start_active_ordinal,
            "start_date": self.start_date,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


def frozen_bar_state_discovery_scope() -> BarStateDiscoveryScope:
    """Return the sole split scope accepted by v2 Discovery artifacts."""

    return BarStateDiscoveryScope(
        split_plan_sha256=BAR_STATE_SPLIT_PLAN_SHA256,
        split_key="discovery",
        result_visibility="VISIBLE",
        start_date="2022-01-03",
        decision_end_date="2023-07-10",
        outcome_end_date="2023-08-02",
        start_active_ordinal=1,
        decision_end_active_ordinal=469,
        outcome_end_active_ordinal=489,
    )


@dataclass(frozen=True, slots=True)
class BarStateParentArtifact:
    """Lossless content and contract identity for one parent artifact."""

    artifact_key: str
    artifact_identity_sha256: str
    content_sha256: str
    byte_size: int

    def __post_init__(self) -> None:
        _canonical_key(self.artifact_key, label="parent artifact_key")
        _sha256(self.artifact_identity_sha256, label="parent artifact identity")
        _sha256(self.content_sha256, label="parent artifact content")
        if isinstance(self.byte_size, bool) or not isinstance(self.byte_size, int):
            raise BarStateArtifactError("parent artifact byte_size must be an integer")
        if self.byte_size < 0:
            raise BarStateArtifactError("parent artifact byte_size cannot be negative")

    @classmethod
    def from_published(cls, artifact: PublishedBarArtifact) -> BarStateParentArtifact:
        return cls(
            artifact_key=artifact.descriptor.artifact_key,
            artifact_identity_sha256=artifact.descriptor.identity_sha256,
            content_sha256=artifact.sha256,
            byte_size=artifact.byte_size,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_identity_sha256": self.artifact_identity_sha256,
            "artifact_key": self.artifact_key,
            "byte_size": self.byte_size,
            "content_sha256": self.content_sha256,
            "schema": BAR_STATE_PARENT_REFERENCE_SCHEMA,
        }


@dataclass(frozen=True, slots=True)
class BarStateArtifactLineage:
    """Complete immutable lineage shared by every v2 evidence artifact."""

    config_file_sha256: str
    config_semantic_sha256: str
    candidate_catalog_sha256: str
    training_plan_sha256: str
    code_snapshot_sha256: str
    dependency_lock_sha256: str
    runtime_environment_sha256: str
    ordered_run_set_sha256: str
    discovery_scope: BarStateDiscoveryScope
    raw_source_manifest_sha256: str = BAR_STATE_RAW_SOURCE_MANIFEST_SHA256
    bar_dataset_manifest_sha256: str = BAR_STATE_BAR_DATASET_MANIFEST_SHA256
    candidate_key: str | None = None
    candidate_definition_sha256: str | None = None
    run_fingerprint: str | None = None
    parent_artifacts: tuple[BarStateParentArtifact, ...] = ()

    def __post_init__(self) -> None:
        for field in (
            "config_file_sha256",
            "config_semantic_sha256",
            "candidate_catalog_sha256",
            "training_plan_sha256",
            "code_snapshot_sha256",
            "dependency_lock_sha256",
            "runtime_environment_sha256",
            "ordered_run_set_sha256",
            "raw_source_manifest_sha256",
            "bar_dataset_manifest_sha256",
        ):
            _sha256(getattr(self, field), label=field)
        if self.raw_source_manifest_sha256 != BAR_STATE_RAW_SOURCE_MANIFEST_SHA256:
            raise BarStateArtifactError("raw source manifest differs from the frozen source")
        if self.bar_dataset_manifest_sha256 != BAR_STATE_BAR_DATASET_MANIFEST_SHA256:
            raise BarStateArtifactError("bar dataset manifest differs from the frozen dataset")
        if not isinstance(self.discovery_scope, BarStateDiscoveryScope):
            raise BarStateArtifactError("discovery_scope must be BarStateDiscoveryScope")
        candidate_fields = (
            self.candidate_key,
            self.candidate_definition_sha256,
            self.run_fingerprint,
        )
        if any(value is None for value in candidate_fields) != all(
            value is None for value in candidate_fields
        ):
            raise BarStateArtifactError(
                "candidate_key, candidate_definition_sha256, and run_fingerprint "
                "must be supplied together"
            )
        if self.candidate_key is not None:
            _canonical_key(self.candidate_key, label="candidate_key")
            _sha256(self.candidate_definition_sha256, label="candidate_definition_sha256")
            _sha256(self.run_fingerprint, label="run_fingerprint")
        if not isinstance(self.parent_artifacts, tuple) or any(
            not isinstance(item, BarStateParentArtifact) for item in self.parent_artifacts
        ):
            raise BarStateArtifactError("parent_artifacts must be a tuple of references")
        parent_keys = tuple(item.artifact_key for item in self.parent_artifacts)
        if parent_keys != tuple(sorted(set(parent_keys))):
            raise BarStateArtifactError("parent_artifacts must be unique and key-sorted")

    def as_dict(self) -> dict[str, object]:
        return {
            "bar_dataset_manifest_sha256": self.bar_dataset_manifest_sha256,
            "candidate_catalog_sha256": self.candidate_catalog_sha256,
            "candidate_definition_sha256": self.candidate_definition_sha256,
            "candidate_key": self.candidate_key,
            "code_snapshot_sha256": self.code_snapshot_sha256,
            "config_file_sha256": self.config_file_sha256,
            "config_semantic_sha256": self.config_semantic_sha256,
            "dependency_lock_sha256": self.dependency_lock_sha256,
            "discovery_scope": self.discovery_scope.as_dict(),
            "discovery_scope_sha256": self.discovery_scope.sha256,
            "ordered_run_set_sha256": self.ordered_run_set_sha256,
            "parent_artifacts": [item.as_dict() for item in self.parent_artifacts],
            "raw_source_manifest_sha256": self.raw_source_manifest_sha256,
            "run_fingerprint": self.run_fingerprint,
            "runtime_environment_sha256": self.runtime_environment_sha256,
            "schema": BAR_STATE_ARTIFACT_IDENTITY_SCHEMA,
            "training_plan_sha256": self.training_plan_sha256,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


def _artifact_kind(value: object) -> BarStateArtifactKind:
    if value not in BAR_STATE_ARTIFACT_SCHEMA_BY_KIND:
        raise BarStateArtifactError(
            f"artifact kind must be one of {sorted(BAR_STATE_ARTIFACT_SCHEMA_BY_KIND)}"
        )
    return value  # type: ignore[return-value]


def bar_state_artifact_descriptor(
    *,
    kind: BarStateArtifactKind,
    artifact_key_suffix: str,
    record_count: int,
    schema_sha256: str,
    lineage: BarStateArtifactLineage,
    logical_identity: Mapping[str, object],
    media_type: str,
    file_suffix: str,
) -> BarArtifactDescriptor:
    """Build one campaign-namespaced descriptor with complete lineage."""

    kind = _artifact_kind(kind)
    suffix = _canonical_key(artifact_key_suffix, label="artifact_key_suffix")
    if not isinstance(lineage, BarStateArtifactLineage):
        raise BarStateArtifactError("lineage must be BarStateArtifactLineage")
    extra = _canonical_mapping(logical_identity, label="logical_identity")
    reserved = {"artifact_kind", "campaign_key", "lineage", "lineage_sha256"}
    if reserved.intersection(extra):
        raise BarStateArtifactError("logical_identity uses a reserved lineage key")
    logical = {
        "artifact_kind": kind,
        "campaign_key": BAR_STATE_CAMPAIGN_KEY,
        "lineage": lineage.as_dict(),
        "lineage_sha256": lineage.sha256,
        **dict(extra),
    }
    return BarArtifactDescriptor(
        artifact_key=f"{BAR_STATE_CAMPAIGN_KEY}:{kind.lower()}:{suffix}",
        artifact_type=BAR_STATE_ARTIFACT_TYPE,
        artifact_schema=BAR_STATE_ARTIFACT_SCHEMA_BY_KIND[kind],
        artifact_version=1,
        record_count=record_count,
        schema_sha256=_sha256(schema_sha256, label="schema_sha256"),
        source_manifest_sha256=lineage.bar_dataset_manifest_sha256,
        logical_identity=logical,
        media_type=media_type,
        file_suffix=file_suffix,
    )


def publish_bar_state_parquet(
    project_root: Path,
    *,
    kind: BarStateArtifactKind,
    artifact_key_suffix: str,
    table: pa.Table,
    lineage: BarStateArtifactLineage,
    logical_identity: Mapping[str, object],
) -> PublishedBarArtifact:
    """Publish one immutable feature, label, or OOS-trade Parquet shard."""

    kind = _artifact_kind(kind)
    if kind not in BAR_STATE_PARQUET_KINDS:
        raise BarStateArtifactError(f"{kind} is not a Parquet artifact kind")
    if not isinstance(table, pa.Table):
        raise BarStateArtifactError("table must be a pyarrow.Table")
    descriptor = bar_state_artifact_descriptor(
        kind=kind,
        artifact_key_suffix=artifact_key_suffix,
        record_count=table.num_rows,
        schema_sha256=arrow_schema_sha256(table.schema),
        lineage=lineage,
        logical_identity=logical_identity,
        media_type="application/vnd.apache.parquet",
        file_suffix=".parquet",
    )
    return publish_bar_parquet_table(project_root, descriptor, table)


def publish_bar_state_parquet_open_file(
    project_root: Path,
    *,
    kind: BarStateArtifactKind,
    artifact_key_suffix: str,
    source: BinaryIO,
    row_count: int,
    schema: pa.Schema,
    lineage: BarStateArtifactLineage,
    logical_identity: Mapping[str, object],
) -> PublishedBarArtifact:
    """Publish a large caller-streamed Parquet file without materializing it."""

    kind = _artifact_kind(kind)
    if kind not in BAR_STATE_PARQUET_KINDS:
        raise BarStateArtifactError(f"{kind} is not a Parquet artifact kind")
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
        raise BarStateArtifactError("row_count must be a non-negative integer")
    if not isinstance(schema, pa.Schema):
        raise BarStateArtifactError("schema must be a pyarrow.Schema")
    try:
        source.flush()
        os.fsync(source.fileno())
        source.seek(0)
        parquet = pq.ParquetFile(source)
        if parquet.metadata.num_rows != row_count or parquet.schema_arrow != schema:
            raise BarStateArtifactError("streamed Parquet row count or schema drift")
        source.seek(0)
    except BarStateArtifactError:
        raise
    except (AttributeError, OSError, ValueError, pa.ArrowException) as error:
        raise BarStateArtifactError("cannot verify streamed Parquet") from error
    descriptor = bar_state_artifact_descriptor(
        kind=kind,
        artifact_key_suffix=artifact_key_suffix,
        record_count=row_count,
        schema_sha256=arrow_schema_sha256(schema),
        lineage=lineage,
        logical_identity=logical_identity,
        media_type="application/vnd.apache.parquet",
        file_suffix=".parquet",
    )
    return publish_bar_artifact_open_file(project_root, descriptor, source)


def publish_bar_state_json(
    project_root: Path,
    *,
    kind: BarStateArtifactKind,
    artifact_key_suffix: str,
    document: Mapping[str, object],
    record_count: int,
    lineage: BarStateArtifactLineage,
    logical_identity: Mapping[str, object],
) -> PublishedBarArtifact:
    """Publish one canonical non-executable model or summary document."""

    kind = _artifact_kind(kind)
    if kind not in BAR_STATE_JSON_KINDS:
        raise BarStateArtifactError(f"{kind} is not a JSON artifact kind")
    canonical = _canonical_mapping(document, label="document")
    expected_schema = BAR_STATE_ARTIFACT_SCHEMA_BY_KIND[kind]
    schema_field = "artifact_schema" if kind == "CODE_SNAPSHOT" else "schema"
    if canonical.get(schema_field) != expected_schema:
        raise BarStateArtifactError(f"document {schema_field} must equal {expected_schema!r}")
    document_schema_sha256 = canonical_sha256(
        {
            "artifact_kind": kind,
            "required_top_level_keys": sorted(canonical),
            "schema": expected_schema,
        }
    )
    descriptor = bar_state_artifact_descriptor(
        kind=kind,
        artifact_key_suffix=artifact_key_suffix,
        record_count=record_count,
        schema_sha256=document_schema_sha256,
        lineage=lineage,
        logical_identity=logical_identity,
        media_type="application/json",
        file_suffix=".json",
    )
    if kind == "CODE_SNAPSHOT":
        return publish_bar_artifact_bytes(
            project_root,
            descriptor,
            canonical_json_bytes(canonical),
        )
    return publish_bar_json_artifact(project_root, descriptor, canonical)


def _validate_published_kind(
    artifact: PublishedBarArtifact,
    *,
    expected_kinds: frozenset[str],
) -> BarStateArtifactKind:
    if not isinstance(artifact, PublishedBarArtifact):
        raise BarStateArtifactError("artifact must be PublishedBarArtifact")
    logical = artifact.descriptor.logical_identity
    kind = _artifact_kind(logical.get("artifact_kind"))
    if kind not in expected_kinds:
        raise BarStateArtifactError(f"artifact kind {kind} is not valid for this loader")
    if (
        artifact.descriptor.artifact_type != BAR_STATE_ARTIFACT_TYPE
        or artifact.descriptor.artifact_schema != BAR_STATE_ARTIFACT_SCHEMA_BY_KIND[kind]
        or logical.get("campaign_key") != BAR_STATE_CAMPAIGN_KEY
    ):
        raise BarStateArtifactError("published artifact campaign or schema drift")
    lineage = logical.get("lineage")
    if not isinstance(lineage, Mapping):
        raise BarStateArtifactError("published artifact lacks canonical lineage")
    if logical.get("lineage_sha256") != canonical_sha256(lineage):
        raise BarStateArtifactError("published artifact lineage hash drift")
    if lineage.get("discovery_scope_sha256") != frozen_bar_state_discovery_scope().sha256:
        raise BarStateArtifactError("published artifact is outside frozen Discovery")
    return kind


def load_verified_bar_state_json(
    project_root: Path,
    artifact: PublishedBarArtifact,
    *,
    maximum_bytes: int = 64 * 1024 * 1024,
) -> dict[str, object]:
    """Reopen, rehash, and strict-decode one campaign JSON artifact."""

    kind = _validate_published_kind(artifact, expected_kinds=BAR_STATE_JSON_KINDS)
    if isinstance(maximum_bytes, bool) or not isinstance(maximum_bytes, int) or maximum_bytes <= 0:
        raise BarStateArtifactError("maximum_bytes must be a positive integer")
    with open_verified_bar_artifact(project_root, artifact) as held:
        details = os.fstat(held.descriptor)
        if details.st_size > maximum_bytes:
            raise BarStateArtifactError("JSON artifact exceeds the loader byte limit")
        os.lseek(held.descriptor, 0, os.SEEK_SET)
        payload = os.read(held.descriptor, maximum_bytes + 1)
        if len(payload) != details.st_size:
            raise BarStateArtifactError("JSON artifact changed during held decode")
        try:
            decoded = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise BarStateArtifactError("JSON artifact cannot be decoded") from error
        document = _canonical_mapping(decoded, label="JSON artifact document")
        schema_field = "artifact_schema" if kind == "CODE_SNAPSHOT" else "schema"
        if document.get(schema_field) != BAR_STATE_ARTIFACT_SCHEMA_BY_KIND[kind]:
            raise BarStateArtifactError("JSON artifact document schema drift")
        return dict(document)


def load_verified_bar_state_parquet(
    project_root: Path,
    artifact: PublishedBarArtifact,
    *,
    columns: Sequence[str] | None = None,
) -> pa.Table:
    """Reopen, rehash, and decode one complete campaign Parquet artifact.

    The held descriptor, rather than the path, is passed to Arrow.  A caller may
    project columns for memory, but row-count and full file-schema identities
    are always checked against the descriptor before returning.
    """

    _validate_published_kind(artifact, expected_kinds=BAR_STATE_PARQUET_KINDS)
    if columns is not None and (
        isinstance(columns, (str, bytes))
        or any(not isinstance(item, str) or not item for item in columns)
    ):
        raise BarStateArtifactError("columns must be a sequence of non-empty names")
    with open_verified_bar_artifact(project_root, artifact) as held:
        duplicate = os.dup(held.descriptor)
        try:
            with os.fdopen(duplicate, "rb", closefd=True) as source:
                parquet = pq.ParquetFile(source)
                if arrow_schema_sha256(parquet.schema_arrow) != artifact.descriptor.schema_sha256:
                    raise BarStateArtifactError("Parquet file schema hash drift")
                if parquet.metadata.num_rows != artifact.descriptor.record_count:
                    raise BarStateArtifactError("Parquet file row count drift")
                table = parquet.read(columns=None if columns is None else list(columns))
        except Exception:
            # ``fdopen`` owns the duplicate only after construction.
            try:
                os.close(duplicate)
            except OSError:
                pass
            raise
    if table.num_rows != artifact.descriptor.record_count:
        raise BarStateArtifactError("decoded Parquet row count drift")
    return table


def ordered_parent_artifacts(
    artifacts: Sequence[PublishedBarArtifact],
) -> tuple[BarStateParentArtifact, ...]:
    """Return key-sorted unique parent references or fail on aliasing."""

    parents = tuple(
        sorted(
            (BarStateParentArtifact.from_published(item) for item in artifacts),
            key=lambda item: item.artifact_key,
        )
    )
    if len({item.artifact_key for item in parents}) != len(parents):
        raise BarStateArtifactError("parent artifacts contain a duplicate artifact key")
    return parents
