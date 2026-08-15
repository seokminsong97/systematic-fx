"""Deterministic, causal ML primitives for the all-cases v1 campaign.

This module is deliberately filesystem-free.  Callers must construct verified
feature/target matrices outside this boundary and may only pass Search targets
to the fitting APIs.  A fitted model is represented as canonical JSON (float
values use ``float.hex``); no pickle or executable estimator state is stored.
"""

from __future__ import annotations

import hashlib
import json
import math
import platform
import sys
import warnings
from collections import defaultdict, deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields, is_dataclass, replace
from datetime import date, datetime
from enum import StrEnum
from fractions import Fraction
from functools import cache
from itertools import pairwise
from types import MappingProxyType, SimpleNamespace
from typing import TYPE_CHECKING, Final, Literal

import numpy as np
import scipy
import sklearn
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import ElasticNet, LogisticRegression
from sklearn.preprocessing import StandardScaler

if TYPE_CHECKING:
    from .symbolic import (
        CausalExpertFeatureArtifact,
        CompleteStrategyRecipe,
        EntryOrderBatch,
        FrozenEntryOrder,
    )

ML_SCHEMA: Final = "systematic_fx.ai_all_cases_ml_model.v1"
DIRECT_CATALOG_SCHEMA: Final = "systematic_fx.ai_all_cases_direct_ml_catalog.v1"
META_CATALOG_SCHEMA: Final = "systematic_fx.ai_all_cases_meta_ml_catalog.v1"
SEARCH_BLOCK_SCHEMA: Final = "systematic_fx.ai_all_cases_ml_search_blocks.v1"
PREDICTION_SCHEMA: Final = "systematic_fx.ai_all_cases_ml_prediction.v1"
FROZEN_MASK_SCHEMA: Final = "systematic_fx.ai_all_cases_frozen_ml_mask.v1"
CONTROL_ALIGNMENT_SCHEMA: Final = "systematic_fx.ai_all_cases_ml_control_alignment.v1"
EXECUTION_SCHEDULE_SCHEMA: Final = "systematic_fx.ai_all_cases_ml_execution_schedule.v2"
STAGE_PARTITION_DATE_CERTIFICATE_SCHEMA: Final = (
    "systematic_fx.ai_all_cases_stage_partition_date_certificate.v1"
)
META_GATE_SCHEDULE_SCHEMA: Final = "systematic_fx.ai_all_cases_meta_anchor_gate_schedule.v2"
META_GATE_MASK_SCHEMA: Final = "systematic_fx.ai_all_cases_frozen_meta_anchor_gate.v1"
SYMBOLIC_RANKING_CERTIFICATE_SCHEMA: Final = (
    "systematic_fx.ai_all_cases_symbolic_ranking_certificate.v2"
)
ML_SEARCH_EVALUATION_SCHEMA: Final = "systematic_fx.ai_all_cases_ml_search_evaluation.v1"
ML_SEARCH_GATE_SCHEMA: Final = "systematic_fx.ai_all_cases_ml_search_gate.v1"
ML_COMPUTE_FEASIBILITY_SCHEMA: Final = "systematic_fx.ai_all_cases_ml_compute_feasibility.v2"

TF_ORDER: Final = (300, 1_800, 3_600)
DIRECT_HORIZONS_SECONDS: Final = (3_600, 10_800, 21_600)
DIRECT_ACTION_RATES: Final = (Fraction(1, 20), Fraction(1, 10))
META_RETAIN_RATES: Final = (Fraction(3, 10), Fraction(1, 2))
DIRECT_LEARNERS: Final = ("ENET_A", "ENET_B", "HGB_7", "HGB_15")
META_CLASSIFIERS: Final = ("META_ENET", "META_HGB_7")
NULL_WORLD_ORDER: Final = ("REAL", "CIRCULAR_TARGET", "MATCHED_TARGET")
TOTAL_FRICTION_TICKS: Final = 14
MASTER_SEED: Final = "ai-all-cases-v1"
EXPECTED_SKLEARN_VERSION: Final = "1.9.0"
SEARCH_DECISION_DATE_COUNT: Final = 469
SEARCH_BLOCK_SIZES: Final = (59, 59, 59, 59, 59, 58, 58, 58)
SEARCH_OUTER_FOLD_KEYS: Final = ("B3", "B4", "B5", "B6", "B7", "B8")
MAX_TRAINING_ROWS_PER_MODEL: Final = 110_000
MAX_RAW_FEATURES: Final = 221
MAX_TRANSFORMED_FEATURES: Final = 442
MAX_SEARCH_MODEL_FITS: Final = 5_040
MIN_CAUSAL_HISTORY_BARS: Final = 50

# The runner must set these before importing numerical libraries.  They are
# part of the campaign contract even though this pure module does not mutate
# process environment.
DETERMINISTIC_THREAD_ENV: Final = {
    "MKL_NUM_THREADS": "1",
    "NUMEXPR_NUM_THREADS": "1",
    "OMP_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "VECLIB_MAXIMUM_THREADS": "1",
}

_PRICE_SUFFIXES: Final = (
    "ret_atr_1",
    "ret_atr_2",
    "ret_atr_3",
    "ret_atr_6",
    "ret_atr_12",
    "ret_atr_24",
    "body_atr",
    "range_atr",
    "upper_wick_atr",
    "lower_wick_atr",
    "close_location",
    "gap_atr",
)
_EMA_PAIRS: Final = ((5, 13), (8, 21), (12, 26), (20, 50))
_MACD_TRIPLES: Final = ((5, 13, 4), (8, 21, 5), (12, 26, 9), (19, 39, 9))
_TECH_SUFFIXES: Final = (
    *(
        item
        for fast, slow in _EMA_PAIRS
        for item in (
            f"ema_{fast}_{slow}_distance_atr",
            f"ema_{fast}_{slow}_fast_slope_atr",
        )
    ),
    *(
        item
        for fast, slow, signal in _MACD_TRIPLES
        for item in (
            f"macd_{fast}_{slow}_{signal}_hist_atr",
            f"macd_{fast}_{slow}_{signal}_hist_delta_atr",
        )
    ),
    "rsi_7",
    "rsi_14",
    "rsi_21",
    *(item for period in (5, 9, 14) for item in (f"stoch_{period}_k", f"stoch_{period}_k_minus_d")),
    "bollinger_z_10",
    "bollinger_z_20",
    "bollinger_z_40",
    "efficiency_ratio_10",
    "efficiency_ratio_20",
    "efficiency_ratio_40",
    "donchian_position_6",
    "donchian_position_12",
    "donchian_position_24",
    "donchian_position_48",
    "vwap_distance_atr_12",
    "vwap_distance_atr_24",
    "vwap_distance_atr_48",
    "atr_ratio_3_12",
    "atr_ratio_6_24",
    "atr_ratio_12_48",
)
_FLOW_SUFFIXES: Final = (
    "volume_z_12",
    "volume_z_24",
    "volume_z_48",
    "trade_count_z_12",
    "trade_count_z_24",
    "trade_count_z_48",
    "buy_imbalance_1",
    "buy_imbalance_12",
    "buy_imbalance_24",
    "buy_sell_available",
)


def _tf_names(suffixes: Sequence[str]) -> tuple[str, ...]:
    return tuple(f"tf{timeframe:04d}_{suffix}" for timeframe in TF_ORDER for suffix in suffixes)


PRICE_FEATURE_NAMES: Final = _tf_names(_PRICE_SUFFIXES)
TECH_FEATURE_NAMES: Final = _tf_names(_TECH_SUFFIXES)
FLOW_FEATURE_NAMES: Final = _tf_names(_FLOW_SUFFIXES)
TIME_FEATURE_NAMES: Final = (
    "utc_sin",
    "utc_cos",
    "utc_4h_00",
    "utc_4h_04",
    "utc_4h_08",
    "utc_4h_12",
    "utc_4h_16",
    "utc_4h_20",
    *(f"utc_weekday_{value}" for value in range(7)),
)
_CROSS_METRICS: Final = (
    "return_sign_agreement",
    "ema_8_21_agreement",
    "volatility_regime_agreement",
)
_CROSS_PAIRS: Final = (
    "tf0300_tf1800",
    "tf0300_tf3600",
    "tf1800_tf3600",
)
CROSS_FEATURE_NAMES: Final = tuple(
    f"cross_{metric}_{pair}" for metric in _CROSS_METRICS for pair in _CROSS_PAIRS
)
EXPERT_FEATURE_NAMES: Final = (
    "expert_signal_strength",
    "expert_event_age_native_bars",
    "expert_context_relation",
    "expert_atr_ticks",
    "expert_signal_range_atr",
    "expert_time_to_entry_seconds",
    "expert_planned_entry_distance_atr",
    "expert_reward_risk_ratio",
)

PRICE_GEOMETRY_36: Final = PRICE_FEATURE_NAMES
TECHNICAL_STATE_159: Final = PRICE_FEATURE_NAMES + TECH_FEATURE_NAMES
FLOW_TIME_REGIME_90: Final = (
    PRICE_FEATURE_NAMES + FLOW_FEATURE_NAMES + TIME_FEATURE_NAMES + CROSS_FEATURE_NAMES
)
FULL_MTF_213: Final = (
    PRICE_FEATURE_NAMES
    + TECH_FEATURE_NAMES
    + FLOW_FEATURE_NAMES
    + TIME_FEATURE_NAMES
    + CROSS_FEATURE_NAMES
)
FULL_MTF_PLUS_EXPERT_221: Final = FULL_MTF_213 + EXPERT_FEATURE_NAMES
FEATURE_NAMES_BY_SET: Final = {
    "PRICE_GEOMETRY_36": PRICE_GEOMETRY_36,
    "TECHNICAL_STATE_159": TECHNICAL_STATE_159,
    "FLOW_TIME_REGIME_90": FLOW_TIME_REGIME_90,
    "FULL_MTF_213": FULL_MTF_213,
    "FULL_MTF_PLUS_EXPERT_221": FULL_MTF_PLUS_EXPERT_221,
}
DIRECT_FEATURE_SET_ORDER: Final = (
    "PRICE_GEOMETRY_36",
    "TECHNICAL_STATE_159",
    "FLOW_TIME_REGIME_90",
    "FULL_MTF_213",
)
META_FEATURE_SET_ORDER: Final = ("FULL_MTF_213", "FULL_MTF_PLUS_EXPERT_221")

if (
    len(PRICE_GEOMETRY_36) != 36
    or len(TECHNICAL_STATE_159) != 159
    or len(FLOW_TIME_REGIME_90) != 90
    or len(FULL_MTF_213) != 213
    or len(FULL_MTF_PLUS_EXPERT_221) != 221
    or any(len(names) != len(set(names)) for names in FEATURE_NAMES_BY_SET.values())
):  # pragma: no cover - import-time campaign invariant
    raise RuntimeError("all-cases feature closure differs")


class AllCasesMLError(ValueError):
    """A feature matrix, model, fold, or canonical artifact is invalid."""


class MLIneligibilityReason(StrEnum):
    INSUFFICIENT_FOLD_ROWS = "INSUFFICIENT_FOLD_ROWS"
    NULL_DERANGEMENT_INFEASIBLE = "NULL_DERANGEMENT_INFEASIBLE"
    ALL_MISSING_TRAINING_COLUMN = "ALL_MISSING_TRAINING_COLUMN"
    SINGLE_CLASS_TRAINING_TARGET = "SINGLE_CLASS_TRAINING_TARGET"
    MODEL_NONCONVERGENCE = "MODEL_NONCONVERGENCE"
    REAL_MASK_EMPTY = "REAL_MASK_EMPTY"
    CONTROL_ALIGNMENT_INSUFFICIENT = "CONTROL_ALIGNMENT_INSUFFICIENT"
    CONTROL_MASKS_NOT_DISTINCT = "CONTROL_MASKS_NOT_DISTINCT"
    INSUFFICIENT_BASE_STRATEGY_RANK = "INSUFFICIENT_BASE_STRATEGY_RANK"


class MLCandidateIneligible(AllCasesMLError):
    """Expected candidate-local failure; integrity/schema errors never use this type."""

    def __init__(
        self,
        reason: MLIneligibilityReason,
        message: str,
        *,
        candidate_id: str | None = None,
        scope_key: str | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.candidate_id = candidate_id
        self.scope_key = scope_key

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "message": str(self),
            "reason": self.reason.value,
            "schema": "systematic_fx.ai_all_cases_ml_ineligibility.v1",
            "scope_key": self.scope_key,
        }


class NullWorld(StrEnum):
    REAL = "REAL"
    CIRCULAR_TARGET = "CIRCULAR_TARGET"
    MATCHED_TARGET = "MATCHED_TARGET"


class TradeDirection(StrEnum):
    SHORT = "SHORT"
    FLAT = "FLAT"
    LONG = "LONG"


def canonical_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError) as error:
        raise AllCasesMLError("value is not canonical-JSON serializable") from error


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _fraction_payload(value: Fraction) -> dict[str, int]:
    return {"denominator": value.denominator, "numerator": value.numerator}


def _float_hex(value: float) -> str:
    result = float(value)
    if not math.isfinite(result):
        raise AllCasesMLError("canonical model cannot contain non-finite floats")
    return result.hex()


def _decode_float_hex(value: object, *, label: str) -> float:
    if not isinstance(value, str):
        raise AllCasesMLError(f"{label} is not hexadecimal float text")
    try:
        result = float.fromhex(value)
    except ValueError as error:
        raise AllCasesMLError(f"{label} is not hexadecimal float text") from error
    if not math.isfinite(result) or result.hex() != value:
        raise AllCasesMLError(f"{label} is not a canonical finite float")
    return result


def _seed(identity: str, world: str, fold_key: str, purpose: str) -> int:
    digest = canonical_sha256(
        {
            "fold_key": fold_key,
            "identity": identity,
            "master_seed": MASTER_SEED,
            "purpose": purpose,
            "world": world,
        }
    )
    return int(digest[:8], 16)


@dataclass(frozen=True, slots=True)
class DirectCandidate:
    selection_rank: int
    decision_timeframe_seconds: int
    feature_set_id: str
    learner_id: str
    horizon_seconds: int
    action_rate: Fraction
    candidate_id: str

    def semantic_dict(self) -> dict[str, object]:
        return {
            "action_rate": _fraction_payload(self.action_rate),
            "decision_timeframe_seconds": self.decision_timeframe_seconds,
            "feature_set_id": self.feature_set_id,
            "horizon_seconds": self.horizon_seconds,
            "learner_id": self.learner_id,
            "schema": "systematic_fx.ai_all_cases_direct_ml_candidate.v1",
            "selection_rank": self.selection_rank,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.semantic_dict(), "candidate_id": self.candidate_id}


@dataclass(frozen=True, slots=True)
class MetaCandidate:
    selection_rank: int
    symbolic_rank_slot: int
    feature_set_id: str
    classifier_id: str
    retain_rate: Fraction
    candidate_id: str

    def semantic_dict(self) -> dict[str, object]:
        return {
            "classifier_id": self.classifier_id,
            "feature_set_id": self.feature_set_id,
            "retain_rate": _fraction_payload(self.retain_rate),
            "schema": "systematic_fx.ai_all_cases_meta_ml_candidate.v1",
            "selection_rank": self.selection_rank,
            "symbolic_rank_policy": "PRIOR_OUTER_TRAIN_ONLY_COMPLETE_STRATEGY_RANK",
            "symbolic_rank_slot": self.symbolic_rank_slot,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.semantic_dict(), "candidate_id": self.candidate_id}


@cache
def _resolve_anchor_policy_components(
    base_candidate_id: str,
    context_id: str,
    time_filter_id: str,
    delay_id: str,
) -> tuple[object, object, object]:
    """Resolve one exact finite-catalog AnchorPolicy without scanning 1.9M rows."""

    from .symbolic import (
        CONTEXT_COUNT,
        DELAY_COUNT,
        POLICY_SCHEMA,
        TIME_FILTER_COUNT,
        AnchorPolicy,
        build_base_event_catalog,
        build_context_catalog,
        build_delay_catalog,
        build_time_filter_catalog,
    )

    base = next(
        (
            item
            for item in build_base_event_catalog().candidates
            if item.candidate_id == base_candidate_id
        ),
        None,
    )
    context = next(
        (item for item in build_context_catalog() if item.context_id == context_id),
        None,
    )
    time_filter = next(
        (item for item in build_time_filter_catalog() if item.time_filter_id == time_filter_id),
        None,
    )
    delay = next(
        (item for item in build_delay_catalog() if item.delay_id == delay_id),
        None,
    )
    if base is None or context is None or time_filter is None or delay is None:
        raise AllCasesMLError("ranked symbolic AnchorPolicy component is outside its catalog")
    policy_rank = (
        ((base.selection_rank - 1) * CONTEXT_COUNT + context.selection_rank - 1) * TIME_FILTER_COUNT
        + time_filter.selection_rank
        - 1
    ) * DELAY_COUNT + delay.selection_rank
    definition = {
        "base_candidate_id": base.candidate_id,
        "context_id": context.context_id,
        "delay_id": delay.delay_id,
        "schema": POLICY_SCHEMA,
        "time_filter_id": time_filter.time_filter_id,
    }
    policy = AnchorPolicy(
        policy_rank,
        canonical_sha256(definition),
        base.candidate_id,
        context.context_id,
        time_filter.time_filter_id,
        delay.delay_id,
    )
    return base, context, policy


@dataclass(frozen=True, slots=True)
class RankedSymbolicStrategy:
    """One exact slot in a prior-prefix-only symbolic ranking."""

    rank_slot: int
    strategy_id: str
    trigger_family: str
    anchor_policy_id: str
    base_candidate_id: str
    context_id: str
    time_filter_id: str
    delay_id: str
    entry_policy_id: str
    exit_policy_id: str

    def __post_init__(self) -> None:
        if (
            isinstance(self.rank_slot, bool)
            or not isinstance(self.rank_slot, int)
            or not 1 <= self.rank_slot <= 24
            or not isinstance(self.trigger_family, str)
            or not self.trigger_family
        ):
            raise AllCasesMLError("ranked symbolic strategy differs")
        from .symbolic import (
            COMPLETE_STRATEGY_SCHEMA,
            build_entry_catalog,
            build_exit_catalog,
        )

        for label, value in (
            ("strategy_id", self.strategy_id),
            ("anchor_policy_id", self.anchor_policy_id),
            ("base_candidate_id", self.base_candidate_id),
            ("context_id", self.context_id),
            ("time_filter_id", self.time_filter_id),
            ("delay_id", self.delay_id),
            ("entry_policy_id", self.entry_policy_id),
            ("exit_policy_id", self.exit_policy_id),
        ):
            if not _is_sha256(value):
                raise AllCasesMLError(f"ranked symbolic {label} differs")
        base, _context, policy = _resolve_anchor_policy_components(
            self.base_candidate_id,
            self.context_id,
            self.time_filter_id,
            self.delay_id,
        )
        if (
            policy.policy_id != self.anchor_policy_id
            or base.family != self.trigger_family
            or self.entry_policy_id
            not in {item.entry_id for item in build_entry_catalog().candidates}
            or self.exit_policy_id not in {item.exit_id for item in build_exit_catalog().candidates}
        ):
            raise AllCasesMLError("ranked symbolic policy or catalog recipe differs")
        recipe = {
            "anchor_policy_id": self.anchor_policy_id,
            "entry_policy_id": self.entry_policy_id,
            "exit_policy_id": self.exit_policy_id,
            "schema": COMPLETE_STRATEGY_SCHEMA,
        }
        if canonical_sha256(recipe) != self.strategy_id:
            raise AllCasesMLError("ranked symbolic strategy id differs from its exact recipe")

    def as_dict(self) -> dict[str, object]:
        return {
            "anchor_policy_id": self.anchor_policy_id,
            "base_candidate_id": self.base_candidate_id,
            "context_id": self.context_id,
            "delay_id": self.delay_id,
            "entry_policy_id": self.entry_policy_id,
            "exit_policy_id": self.exit_policy_id,
            "rank_slot": self.rank_slot,
            "strategy_id": self.strategy_id,
            "time_filter_id": self.time_filter_id,
            "trigger_family": self.trigger_family,
        }

    @classmethod
    def from_dict(cls, value: object) -> RankedSymbolicStrategy:
        if not isinstance(value, dict) or set(value) != {
            "anchor_policy_id",
            "base_candidate_id",
            "context_id",
            "delay_id",
            "entry_policy_id",
            "exit_policy_id",
            "rank_slot",
            "strategy_id",
            "time_filter_id",
            "trigger_family",
        }:
            raise AllCasesMLError("ranked symbolic strategy document differs")
        if (
            isinstance(value["rank_slot"], bool)
            or not isinstance(value["rank_slot"], int)
            or not isinstance(value["strategy_id"], str)
            or not isinstance(value["trigger_family"], str)
            or not isinstance(value["anchor_policy_id"], str)
            or not isinstance(value["base_candidate_id"], str)
            or not isinstance(value["context_id"], str)
            or not isinstance(value["time_filter_id"], str)
            or not isinstance(value["delay_id"], str)
            or not isinstance(value["entry_policy_id"], str)
            or not isinstance(value["exit_policy_id"], str)
        ):
            raise AllCasesMLError("ranked symbolic strategy value differs")
        result = cls(
            value["rank_slot"],
            value["strategy_id"],
            value["trigger_family"],
            value["anchor_policy_id"],
            value["base_candidate_id"],
            value["context_id"],
            value["time_filter_id"],
            value["delay_id"],
            value["entry_policy_id"],
            value["exit_policy_id"],
        )
        if result.as_dict() != value:
            raise AllCasesMLError("ranked symbolic strategy did not round trip")
        return result


@dataclass(frozen=True, slots=True)
class SymbolicRankingCertificate:
    """Typed proof that a rank slot came only from one world's prior date prefix."""

    null_world: str
    fold_key: str
    training_dates: tuple[date, ...]
    ranked_strategies: tuple[RankedSymbolicStrategy, ...]
    artifact_sha256: str

    def definition_dict(self) -> dict[str, object]:
        return {
            "fold_key": self.fold_key,
            "null_world": self.null_world,
            "ranked_strategies": [item.as_dict() for item in self.ranked_strategies],
            "schema": SYMBOLIC_RANKING_CERTIFICATE_SCHEMA,
            "training_dates": [value.isoformat() for value in self.training_dates],
        }

    def __post_init__(self) -> None:
        if (
            self.null_world not in NULL_WORLD_ORDER
            or self.fold_key not in (*SEARCH_OUTER_FOLD_KEYS, "SEARCH_FINAL")
            or not self.training_dates
            or any(
                isinstance(value, datetime) or not isinstance(value, date)
                for value in self.training_dates
            )
            or tuple(sorted(set(self.training_dates))) != self.training_dates
            or len(self.ranked_strategies) > 24
            or tuple(item.rank_slot for item in self.ranked_strategies)
            != tuple(range(1, len(self.ranked_strategies) + 1))
            or len({item.strategy_id for item in self.ranked_strategies})
            != len(self.ranked_strategies)
            or len(self.artifact_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.artifact_sha256)
            or canonical_sha256(self.definition_dict()) != self.artifact_sha256
        ):
            raise AllCasesMLError("symbolic ranking certificate differs")

    def strategy_at_rank(self, rank_slot: int) -> RankedSymbolicStrategy | None:
        if (
            isinstance(rank_slot, bool)
            or not isinstance(rank_slot, int)
            or not 1 <= rank_slot <= 24
        ):
            raise AllCasesMLError("symbolic rank slot differs")
        return (
            self.ranked_strategies[rank_slot - 1]
            if rank_slot <= len(self.ranked_strategies)
            else None
        )

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}

    @classmethod
    def from_dict(cls, value: object) -> SymbolicRankingCertificate:
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "artifact_sha256",
                "fold_key",
                "null_world",
                "ranked_strategies",
                "schema",
                "training_dates",
            }
            or value["schema"] != SYMBOLIC_RANKING_CERTIFICATE_SCHEMA
        ):
            raise AllCasesMLError("symbolic ranking certificate document differs")
        strategies = value["ranked_strategies"]
        dates = value["training_dates"]
        if (
            not isinstance(value["null_world"], str)
            or not isinstance(value["fold_key"], str)
            or not isinstance(value["artifact_sha256"], str)
            or not isinstance(strategies, list)
            or not isinstance(dates, list)
            or any(not isinstance(item, str) for item in dates)
        ):
            raise AllCasesMLError("symbolic ranking certificate value differs")
        try:
            result = cls(
                value["null_world"],
                value["fold_key"],
                tuple(date.fromisoformat(item) for item in dates),
                tuple(RankedSymbolicStrategy.from_dict(item) for item in strategies),
                value["artifact_sha256"],
            )
        except ValueError as error:
            raise AllCasesMLError("symbolic ranking certificate date differs") from error
        if result.as_dict() != value:
            raise AllCasesMLError("symbolic ranking certificate did not round trip")
        return result


def build_symbolic_ranking_certificate(
    *,
    null_world: NullWorld | str,
    fold_key: str,
    training_dates: Sequence[date],
    ranked_strategies: Sequence[RankedSymbolicStrategy],
) -> SymbolicRankingCertificate:
    normalized_world = _normalized_world(null_world)
    strategies = tuple(ranked_strategies)
    dates = tuple(training_dates)
    definition = {
        "fold_key": fold_key,
        "null_world": normalized_world.value,
        "ranked_strategies": [item.as_dict() for item in strategies],
        "schema": SYMBOLIC_RANKING_CERTIFICATE_SCHEMA,
        "training_dates": [value.isoformat() for value in dates],
    }
    return SymbolicRankingCertificate(
        normalized_world.value,
        fold_key,
        dates,
        strategies,
        canonical_sha256(definition),
    )


def _require_ranked_strategy(
    certificate: SymbolicRankingCertificate,
    candidate: MetaCandidate,
    *,
    scope_key: str | None = None,
) -> RankedSymbolicStrategy:
    strategy = certificate.strategy_at_rank(candidate.symbolic_rank_slot)
    if strategy is None:
        raise MLCandidateIneligible(
            MLIneligibilityReason.INSUFFICIENT_BASE_STRATEGY_RANK,
            "the prior-prefix symbolic ranking does not contain this meta rank slot",
            candidate_id=candidate.candidate_id,
            scope_key=scope_key or certificate.fold_key,
        )
    return strategy


def build_direct_candidate_catalog() -> tuple[DirectCandidate, ...]:
    output: list[DirectCandidate] = []
    rank = 0
    for timeframe in TF_ORDER:
        for feature_set_id in DIRECT_FEATURE_SET_ORDER:
            for learner_id in DIRECT_LEARNERS:
                for horizon in DIRECT_HORIZONS_SECONDS:
                    for rate in DIRECT_ACTION_RATES:
                        rank += 1
                        provisional = DirectCandidate(
                            rank,
                            timeframe,
                            feature_set_id,
                            learner_id,
                            horizon,
                            rate,
                            "0" * 64,
                        )
                        output.append(
                            DirectCandidate(
                                rank,
                                timeframe,
                                feature_set_id,
                                learner_id,
                                horizon,
                                rate,
                                canonical_sha256(provisional.semantic_dict()),
                            )
                        )
    result = tuple(output)
    if len(result) != 288 or len({item.candidate_id for item in result}) != 288:
        raise AllCasesMLError("direct ML catalog differs")
    return result


def build_meta_candidate_catalog() -> tuple[MetaCandidate, ...]:
    output: list[MetaCandidate] = []
    rank = 0
    for symbolic_rank in range(1, 25):
        for feature_set_id in META_FEATURE_SET_ORDER:
            for classifier_id in META_CLASSIFIERS:
                for retain_rate in META_RETAIN_RATES:
                    rank += 1
                    provisional = MetaCandidate(
                        rank,
                        symbolic_rank,
                        feature_set_id,
                        classifier_id,
                        retain_rate,
                        "0" * 64,
                    )
                    output.append(
                        MetaCandidate(
                            rank,
                            symbolic_rank,
                            feature_set_id,
                            classifier_id,
                            retain_rate,
                            canonical_sha256(provisional.semantic_dict()),
                        )
                    )
    result = tuple(output)
    if len(result) != 192 or len({item.candidate_id for item in result}) != 192:
        raise AllCasesMLError("meta-label ML catalog differs")
    return result


def learner_recipe_document(
    candidate: DirectCandidate | MetaCandidate,
) -> dict[str, object]:
    """Return the exact estimator parameters whose fitted state is serialized."""

    if isinstance(candidate, DirectCandidate):
        learner_id = candidate.learner_id
        if learner_id in {"ENET_A", "ENET_B"}:
            parameters: dict[str, object] = {
                "alpha_hex": _float_hex(0.001 if learner_id == "ENET_A" else 0.01),
                "fit_intercept": True,
                "l1_ratio_hex": _float_hex(0.1 if learner_id == "ENET_A" else 0.5),
                "max_iter": 50_000,
                "selection": "cyclic",
                "tol_hex": _float_hex(1e-8),
            }
            predictor_kind = "ELASTIC_NET_REGRESSOR"
        elif learner_id in {"HGB_7", "HGB_15"}:
            parameters = {
                "early_stopping": False,
                "l2_regularization_hex": _float_hex(1.0),
                "learning_rate_hex": _float_hex(0.05),
                "loss": "squared_error",
                "max_bins": 255,
                "max_iter": 200,
                "max_leaf_nodes": 7 if learner_id == "HGB_7" else 15,
                "min_samples_leaf": 40,
            }
            predictor_kind = "HGB_REGRESSOR"
        else:
            raise AllCasesMLError("unknown direct learner recipe")
    else:
        learner_id = candidate.classifier_id
        if learner_id == "META_ENET":
            parameters = {
                "C_hex": _float_hex(0.1),
                "class_weight": "balanced",
                "fit_intercept": True,
                "max_iter": 50_000,
                "n_jobs": 1,
                "penalty": "l2",
                "solver": "liblinear",
                "tol_hex": _float_hex(1e-8),
            }
            predictor_kind = "LOGISTIC_BINARY_CLASSIFIER"
        elif learner_id == "META_HGB_7":
            parameters = {
                "early_stopping": False,
                "l2_regularization_hex": _float_hex(1.0),
                "learning_rate_hex": _float_hex(0.05),
                "loss": "log_loss",
                "max_bins": 255,
                "max_iter": 200,
                "max_leaf_nodes": 7,
                "min_samples_leaf": 40,
            }
            predictor_kind = "HGB_BINARY_CLASSIFIER"
        else:
            raise AllCasesMLError("unknown meta learner recipe")
    return {
        "learner_id": learner_id,
        "parameters": parameters,
        "predictor_kind": predictor_kind,
        "random_state": "DERIVED_FROM_FIT_RECIPE_WORLD_FOLD",
        "schema": "systematic_fx.ai_all_cases_ml_learner_recipe.v1",
    }


def direct_fit_recipe_document(candidate: DirectCandidate) -> dict[str, object]:
    """Identity of fitted state shared by the two direct action rates."""

    return {
        "decision_timeframe_seconds": candidate.decision_timeframe_seconds,
        "feature_set_id": candidate.feature_set_id,
        "horizon_seconds": candidate.horizon_seconds,
        "learner_recipe": learner_recipe_document(candidate),
        "learner_id": candidate.learner_id,
        "schema": "systematic_fx.ai_all_cases_direct_fit_recipe.v1",
    }


def meta_fit_recipe_document(candidate: MetaCandidate) -> dict[str, object]:
    """Identity of fitted state shared by the two meta retain rates."""

    return {
        "classifier_id": candidate.classifier_id,
        "feature_set_id": candidate.feature_set_id,
        "learner_recipe": learner_recipe_document(candidate),
        "schema": "systematic_fx.ai_all_cases_meta_fit_recipe.v1",
        "symbolic_rank_policy": "PRIOR_OUTER_TRAIN_ONLY_COMPLETE_STRATEGY_RANK",
        "symbolic_rank_slot": candidate.symbolic_rank_slot,
    }


def direct_fit_recipe_id(candidate: DirectCandidate) -> str:
    return canonical_sha256(direct_fit_recipe_document(candidate))


def meta_fit_recipe_id(candidate: MetaCandidate) -> str:
    return canonical_sha256(meta_fit_recipe_document(candidate))


DIRECT_CANDIDATE_CATALOG: Final = build_direct_candidate_catalog()
META_CANDIDATE_CATALOG: Final = build_meta_candidate_catalog()
DIRECT_CANDIDATE_BY_ID: Final = {
    candidate.candidate_id: candidate for candidate in DIRECT_CANDIDATE_CATALOG
}
META_CANDIDATE_BY_ID: Final = {
    candidate.candidate_id: candidate for candidate in META_CANDIDATE_CATALOG
}
DIRECT_FIT_RECIPE_DOCUMENTS: Final = tuple(
    {
        canonical_sha256(direct_fit_recipe_document(candidate)): direct_fit_recipe_document(
            candidate
        )
        for candidate in DIRECT_CANDIDATE_CATALOG
    }.values()
)
META_FIT_RECIPE_DOCUMENTS: Final = tuple(
    {
        canonical_sha256(meta_fit_recipe_document(candidate)): meta_fit_recipe_document(candidate)
        for candidate in META_CANDIDATE_CATALOG
    }.values()
)
DIRECT_FIT_RECIPE_SHA256: Final = canonical_sha256(list(DIRECT_FIT_RECIPE_DOCUMENTS))
META_FIT_RECIPE_SHA256: Final = canonical_sha256(list(META_FIT_RECIPE_DOCUMENTS))
DIRECT_CATALOG_SHA256: Final = canonical_sha256(
    [item.as_dict() for item in DIRECT_CANDIDATE_CATALOG]
)
META_CATALOG_SHA256: Final = canonical_sha256([item.as_dict() for item in META_CANDIDATE_CATALOG])
if len(DIRECT_FIT_RECIPE_DOCUMENTS) != 144 or len(META_FIT_RECIPE_DOCUMENTS) != 96:
    raise RuntimeError("all-cases shared fit-recipe closure differs")


def direct_catalog_document() -> dict[str, object]:
    """Return the complete ordered direct-model catalog and its identity."""

    candidates = [item.as_dict() for item in DIRECT_CANDIDATE_CATALOG]
    return {
        "candidate_count": len(candidates),
        "candidates": candidates,
        "catalog_sha256": canonical_sha256(candidates),
        "schema": DIRECT_CATALOG_SCHEMA,
    }


def meta_catalog_document() -> dict[str, object]:
    """Return the complete ordered meta-label catalog and its identity."""

    candidates = [item.as_dict() for item in META_CANDIDATE_CATALOG]
    return {
        "candidate_count": len(candidates),
        "candidates": candidates,
        "catalog_sha256": canonical_sha256(candidates),
        "schema": META_CATALOG_SCHEMA,
    }


def ml_candidate_family_document(
    candidate: DirectCandidate | MetaCandidate,
    *,
    base_trigger_family: str | None = None,
) -> dict[str, object]:
    """Return the exact diversity family used by the maximum-two Search cap."""

    if isinstance(candidate, DirectCandidate):
        if DIRECT_CANDIDATE_BY_ID.get(candidate.candidate_id) != candidate:
            raise AllCasesMLError("direct family candidate is not frozen")
        return {
            "candidate_kind": "DIRECT",
            "decision_timeframe_seconds": candidate.decision_timeframe_seconds,
            "feature_set_id": candidate.feature_set_id,
            "schema": "systematic_fx.ai_all_cases_ml_family.v1",
        }
    if META_CANDIDATE_BY_ID.get(candidate.candidate_id) != candidate:
        raise AllCasesMLError("meta family candidate is not frozen")
    if not isinstance(base_trigger_family, str) or not base_trigger_family:
        raise AllCasesMLError("meta family requires its final inherited base trigger family")
    return {
        "base_trigger_family": base_trigger_family,
        "candidate_kind": "META",
        "schema": "systematic_fx.ai_all_cases_ml_family.v1",
    }


def ml_candidate_family_key(
    candidate: DirectCandidate | MetaCandidate,
    *,
    base_trigger_family: str | None = None,
) -> str:
    return canonical_sha256(
        ml_candidate_family_document(
            candidate,
            base_trigger_family=base_trigger_family,
        )
    )


@dataclass(frozen=True, slots=True)
class MLFitWorkloadRecord:
    """One exact learner slice of the finite Search fitting workload."""

    candidate_kind: str
    learner_id: str
    fit_recipe_count: int
    worlds: tuple[str, ...]
    scope_keys: tuple[str, ...]
    maximum_raw_feature_count: int
    maximum_transformed_feature_count: int
    fit_recipe_catalog_sha256: str

    def __post_init__(self) -> None:
        expected_learners = {
            "DIRECT": DIRECT_LEARNERS,
            "META": META_CLASSIFIERS,
        }
        expected_widths = {
            "DIRECT": (len(FULL_MTF_213), 2 * len(FULL_MTF_213)),
            "META": (len(FULL_MTF_PLUS_EXPERT_221), 2 * len(FULL_MTF_PLUS_EXPERT_221)),
        }
        expected_catalog_sha256 = {
            "DIRECT": DIRECT_FIT_RECIPE_SHA256,
            "META": META_FIT_RECIPE_SHA256,
        }
        if (
            not isinstance(self.candidate_kind, str)
            or self.candidate_kind not in expected_learners
            or not isinstance(self.learner_id, str)
            or self.learner_id not in expected_learners[self.candidate_kind]
            or isinstance(self.fit_recipe_count, bool)
            or not isinstance(self.fit_recipe_count, int)
            or self.fit_recipe_count <= 0
            or any(
                isinstance(value, bool) or not isinstance(value, int)
                for value in (
                    self.maximum_raw_feature_count,
                    self.maximum_transformed_feature_count,
                )
            )
            or self.worlds != tuple(NULL_WORLD_ORDER)
            or self.scope_keys != (*SEARCH_OUTER_FOLD_KEYS, "SEARCH_FINAL")
            or (
                self.maximum_raw_feature_count,
                self.maximum_transformed_feature_count,
            )
            != expected_widths[self.candidate_kind]
            or not isinstance(self.fit_recipe_catalog_sha256, str)
            or self.fit_recipe_catalog_sha256 != expected_catalog_sha256[self.candidate_kind]
        ):
            raise AllCasesMLError("ML fit workload record differs")

    @property
    def search_fit_count(self) -> int:
        return self.fit_recipe_count * len(self.worlds) * len(self.scope_keys)

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_kind": self.candidate_kind,
            "fit_recipe_catalog_sha256": self.fit_recipe_catalog_sha256,
            "fit_recipe_count": self.fit_recipe_count,
            "learner_id": self.learner_id,
            "maximum_raw_feature_count": self.maximum_raw_feature_count,
            "maximum_training_rows": MAX_TRAINING_ROWS_PER_MODEL,
            "maximum_transformed_feature_count": self.maximum_transformed_feature_count,
            "scope_keys": list(self.scope_keys),
            "search_fit_count": self.search_fit_count,
            "worlds": list(self.worlds),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> MLFitWorkloadRecord:
        expected_keys = {
            "candidate_kind",
            "fit_recipe_catalog_sha256",
            "fit_recipe_count",
            "learner_id",
            "maximum_raw_feature_count",
            "maximum_training_rows",
            "maximum_transformed_feature_count",
            "scope_keys",
            "search_fit_count",
            "worlds",
        }
        if set(value) != expected_keys:
            raise AllCasesMLError("ML fit workload keys differ")
        worlds = value["worlds"]
        scope_keys = value["scope_keys"]
        if (
            not isinstance(worlds, list)
            or not isinstance(scope_keys, list)
            or not all(isinstance(item, str) for item in (*worlds, *scope_keys))
        ):
            raise AllCasesMLError("ML fit workload domains differ")
        try:
            result = cls(
                value["candidate_kind"],
                value["learner_id"],
                value["fit_recipe_count"],
                tuple(worlds),
                tuple(scope_keys),
                value["maximum_raw_feature_count"],
                value["maximum_transformed_feature_count"],
                value["fit_recipe_catalog_sha256"],
            )
        except TypeError as error:
            raise AllCasesMLError("ML fit workload value differs") from error
        if result.as_dict() != value:
            raise AllCasesMLError("ML fit workload did not round trip")
        return result


@dataclass(frozen=True, slots=True)
class MLSyntheticFitMeasurement:
    """Typed outcome-free max-shape benchmark evidence for one learner."""

    candidate_kind: str
    learner_id: str
    learner_recipe_sha256: str
    target_recipe: str
    dataset_recipe_sha256: str
    dataset_generation_milliseconds: int
    preprocess_milliseconds: int
    fit_milliseconds: int
    process_wall_milliseconds: int
    maximum_resident_bytes: int
    scale_features: bool
    tree_count: int
    maximum_observed_leaf_count: int

    @property
    def dataset_recipe(self) -> dict[str, object]:
        return {
            "feature_generator": "NUMPY_PCG64_STANDARD_NORMAL_FLOAT64",
            "feature_seed": 20_860_815,
            "missingness": "RAW_DIAGONAL_FIRST_221_ONE_NAN_PER_COLUMN",
            "raw_feature_count": MAX_RAW_FEATURES,
            "row_count": MAX_TRAINING_ROWS_PER_MODEL,
            "target": self.target_recipe,
        }

    def __post_init__(self) -> None:
        if (
            not isinstance(self.candidate_kind, str)
            or not isinstance(self.learner_id, str)
            or not isinstance(self.learner_recipe_sha256, str)
            or not isinstance(self.target_recipe, str)
            or not isinstance(self.dataset_recipe_sha256, str)
            or type(self.scale_features) is not bool
        ):
            raise AllCasesMLError("synthetic ML fit measurement differs")
        if self.candidate_kind == "DIRECT":
            catalog: Sequence[DirectCandidate | MetaCandidate] = DIRECT_CANDIDATE_CATALOG
            matching = [
                candidate
                for candidate in catalog
                if isinstance(candidate, DirectCandidate)
                and candidate.learner_id == self.learner_id
            ]
        elif self.candidate_kind == "META":
            catalog = META_CANDIDATE_CATALOG
            matching = [
                candidate
                for candidate in catalog
                if isinstance(candidate, MetaCandidate)
                and candidate.classifier_id == self.learner_id
            ]
        else:
            matching = []
        expected_tree_caps = {
            "HGB_7": 7,
            "HGB_15": 15,
            "META_HGB_7": 7,
        }
        is_hgb = self.learner_id in expected_tree_caps
        timing_values = (
            self.dataset_generation_milliseconds,
            self.preprocess_milliseconds,
            self.fit_milliseconds,
            self.process_wall_milliseconds,
            self.maximum_resident_bytes,
        )
        if (
            not matching
            or any(
                isinstance(value, bool) or not isinstance(value, int)
                for value in (self.tree_count, self.maximum_observed_leaf_count)
            )
            or any(
                isinstance(value, bool) or not isinstance(value, int) or value <= 0
                for value in timing_values
            )
            or self.dataset_recipe_sha256 != canonical_sha256(self.dataset_recipe)
            or self.learner_recipe_sha256 != canonical_sha256(learner_recipe_document(matching[0]))
            or self.process_wall_milliseconds
            != self.dataset_generation_milliseconds
            + self.preprocess_milliseconds
            + self.fit_milliseconds
            or self.scale_features != (not is_hgb)
            or (
                is_hgb
                and (
                    self.tree_count != 200
                    or self.maximum_observed_leaf_count != expected_tree_caps[self.learner_id]
                )
            )
            or (not is_hgb and (self.tree_count != 0 or self.maximum_observed_leaf_count != 0))
        ):
            raise AllCasesMLError("synthetic ML fit measurement differs")

    def as_dict(self) -> dict[str, object]:
        return {
            "benchmark_random_state": 20_860_815,
            "candidate_kind": self.candidate_kind,
            "dataset_generation_milliseconds": self.dataset_generation_milliseconds,
            "dataset_recipe": self.dataset_recipe,
            "dataset_recipe_sha256": self.dataset_recipe_sha256,
            "fit_milliseconds": self.fit_milliseconds,
            "learner_id": self.learner_id,
            "learner_recipe_sha256": self.learner_recipe_sha256,
            "maximum_observed_leaf_count": self.maximum_observed_leaf_count,
            "maximum_resident_bytes": self.maximum_resident_bytes,
            "preprocess_milliseconds": self.preprocess_milliseconds,
            "process_wall_milliseconds": self.process_wall_milliseconds,
            "raw_feature_count": MAX_RAW_FEATURES,
            "row_count": MAX_TRAINING_ROWS_PER_MODEL,
            "scale_features": self.scale_features,
            "thread_count": 1,
            "transformed_feature_count": MAX_TRANSFORMED_FEATURES,
            "tree_count": self.tree_count,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> MLSyntheticFitMeasurement:
        expected_keys = {
            "benchmark_random_state",
            "candidate_kind",
            "dataset_generation_milliseconds",
            "dataset_recipe",
            "dataset_recipe_sha256",
            "fit_milliseconds",
            "learner_id",
            "learner_recipe_sha256",
            "maximum_observed_leaf_count",
            "maximum_resident_bytes",
            "preprocess_milliseconds",
            "process_wall_milliseconds",
            "raw_feature_count",
            "row_count",
            "scale_features",
            "thread_count",
            "transformed_feature_count",
            "tree_count",
        }
        if set(value) != expected_keys or not isinstance(value["dataset_recipe"], Mapping):
            raise AllCasesMLError("synthetic ML fit measurement keys differ")
        dataset_recipe = value["dataset_recipe"]
        try:
            result = cls(
                value["candidate_kind"],
                value["learner_id"],
                value["learner_recipe_sha256"],
                dataset_recipe["target"],
                value["dataset_recipe_sha256"],
                value["dataset_generation_milliseconds"],
                value["preprocess_milliseconds"],
                value["fit_milliseconds"],
                value["process_wall_milliseconds"],
                value["maximum_resident_bytes"],
                value["scale_features"],
                value["tree_count"],
                value["maximum_observed_leaf_count"],
            )
        except (KeyError, TypeError) as error:
            raise AllCasesMLError("synthetic ML fit measurement value differs") from error
        if result.as_dict() != value:
            raise AllCasesMLError("synthetic ML fit measurement did not round trip")
        return result


@dataclass(frozen=True, slots=True)
class MLComputeFeasibilityEvidence:
    """Frozen executable-path projection backed by typed max-shape measurements."""

    workloads: tuple[MLFitWorkloadRecord, ...]
    measurements: tuple[MLSyntheticFitMeasurement, ...]

    def __post_init__(self) -> None:
        direct_counts: dict[str, int] = defaultdict(int)
        meta_counts: dict[str, int] = defaultdict(int)
        for document in DIRECT_FIT_RECIPE_DOCUMENTS:
            direct_counts[str(document["learner_id"])] += 1
        for document in META_FIT_RECIPE_DOCUMENTS:
            meta_counts[str(document["classifier_id"])] += 1
        expected_counts = {
            **{("DIRECT", key): value for key, value in direct_counts.items()},
            **{("META", key): value for key, value in meta_counts.items()},
        }
        workload_keys = tuple((item.candidate_kind, item.learner_id) for item in self.workloads)
        measurement_keys = tuple(
            (item.candidate_kind, item.learner_id) for item in self.measurements
        )
        expected_keys = tuple(("DIRECT", item) for item in DIRECT_LEARNERS) + tuple(
            ("META", item) for item in META_CLASSIFIERS
        )
        if (
            workload_keys != expected_keys
            or measurement_keys != expected_keys
            or any(
                item.fit_recipe_count != expected_counts[(item.candidate_kind, item.learner_id)]
                for item in self.workloads
            )
            or sum(item.search_fit_count for item in self.workloads) != MAX_SEARCH_MODEL_FITS
        ):
            raise AllCasesMLError("ML compute feasibility workload differs")

    @property
    def sequential_ml_seconds(self) -> int:
        measurements = {(item.candidate_kind, item.learner_id): item for item in self.measurements}
        return sum(
            math.ceil(
                workload.search_fit_count
                * measurements[
                    (workload.candidate_kind, workload.learner_id)
                ].process_wall_milliseconds
                / 1_000
            )
            for workload in self.workloads
        )

    @property
    def safety_adjusted_ml_seconds(self) -> int:
        return math.ceil(self.sequential_ml_seconds * 3 / 2)

    @property
    def projected_campaign_seconds(self) -> int:
        return self.safety_adjusted_ml_seconds + 36_440 + 10_000

    def as_dict(self) -> dict[str, object]:
        measurements = {(item.candidate_kind, item.learner_id): item for item in self.measurements}
        workload_projection = []
        for workload in self.workloads:
            measurement = measurements[(workload.candidate_kind, workload.learner_id)]
            workload_projection.append(
                {
                    **workload.as_dict(),
                    "measured_max_shape_process_wall_milliseconds": (
                        measurement.process_wall_milliseconds
                    ),
                    "projected_sequential_seconds": math.ceil(
                        workload.search_fit_count * measurement.process_wall_milliseconds / 1_000
                    ),
                }
            )
        peak_bytes = max(item.maximum_resident_bytes for item in self.measurements)
        maximum_state_retained_bytes = 5_865_284
        maximum_live_states = 21
        maximum_cache_retained_bytes = maximum_state_retained_bytes * maximum_live_states
        cached_matrix_value_bytes = (
            MAX_TRAINING_ROWS_PER_MODEL
            * (len(FULL_MTF_213) + len(FULL_MTF_PLUS_EXPERT_221))
            * len(NULL_WORLD_ORDER)
            * (len(SEARCH_OUTER_FOLD_KEYS) + 1)
            * np.dtype(np.float64).itemsize
        )
        cached_matrix_auxiliary_reserve_bytes = 2 * 1_024**3
        feature_bar_and_artifact_reserve_bytes = 2 * 1_024**3
        combined_peak_bytes = (
            peak_bytes
            + maximum_state_retained_bytes * (maximum_live_states - 1)
            + cached_matrix_value_bytes
            + cached_matrix_auxiliary_reserve_bytes
            + feature_bar_and_artifact_reserve_bytes
        )
        document = {
            "benchmark_date": "2026-08-15",
            "benchmark_environment": {
                "machine": "arm64",
                "numpy": "2.5.1",
                "operating_system": "macOS-26.5.1-arm64-arm-64bit",
                "python": "3.12.13",
                "sklearn": EXPECTED_SKLEARN_VERSION,
                "thread_environment": dict(DETERMINISTIC_THREAD_ENV),
            },
            "cache_retention_benchmark": {
                "cache_final_state_count": 0,
                "cache_peak_retained_bytes": maximum_cache_retained_bytes,
                "cache_peak_state_count": maximum_live_states,
                "measurement": (
                    "CPYTHON_REACHABLE_SIZE_HGB15_200_TREE_STATE_WITH_110000_UNIQUE_FLOAT_SCORES"
                ),
                "single_state_retained_bytes": maximum_state_retained_bytes,
                "validation": (
                    "SECOND_RATE_HIT_OR_TYPED_ONE_SIDED_DISCARD_EVICTION_BEFORE_NEXT_"
                    "FIT_RECIPE;TERMINAL_FINAL_ZERO_AND_24_CHUNK_AGGREGATE"
                ),
            },
            "execution_path": "SEQUENTIAL_ONE_FIT_AT_A_TIME_NO_WORKER_DIVISOR",
            "extrapolation": {
                "cached_training_matrix_lifetime": (
                    "DIRECT_PHASE_GLOBAL_MAX_36_KEYS_META_CHUNK_LOCAL_CLEAR_MAX_2_KEYS_42_MATRICES"
                ),
                "cached_training_matrix_auxiliary_reserve_bytes": (
                    cached_matrix_auxiliary_reserve_bytes
                ),
                "cached_training_matrix_value_bytes_upper": cached_matrix_value_bytes,
                "combined_transient_plus_live_cache_peak_bytes": combined_peak_bytes,
                "feature_bar_and_artifact_reserve_bytes": (feature_bar_and_artifact_reserve_bytes),
                "memory_cap_bytes": 32 * 1_024**3,
                "memory_safety_factor_denominator": 1,
                "memory_safety_factor_numerator": 2,
                "maximum_search_model_fits": MAX_SEARCH_MODEL_FITS,
                "orchestration_and_io_reserve_seconds": 10_000,
                "peak_measured_resident_bytes": peak_bytes,
                "safety_adjusted_peak_resident_bytes": 2 * combined_peak_bytes,
                "safety_adjusted_sequential_ml_seconds": self.safety_adjusted_ml_seconds,
                "sequential_ml_seconds": self.sequential_ml_seconds,
                "symbolic_phase_reserve_seconds": 36_440,
                "time_safety_factor_denominator": 2,
                "time_safety_factor_numerator": 3,
                "total_campaign_projected_seconds": self.projected_campaign_seconds,
                "wall_cap_seconds": 172_800,
                "within_memory_cap": 2 * combined_peak_bytes <= 32 * 1_024**3,
                "within_wall_cap": self.projected_campaign_seconds <= 172_800,
                "worst_live_meta_training_matrix_count": (
                    len(META_FEATURE_SET_ORDER)
                    * len(NULL_WORLD_ORDER)
                    * (len(SEARCH_OUTER_FOLD_KEYS) + 1)
                ),
            },
            "measurements": [item.as_dict() for item in self.measurements],
            "null_permutation_benchmark": {
                "circular_wall_milliseconds": 38,
                "matched_wall_milliseconds": 439,
                "maximum_resident_bytes": 351_272_960,
                "row_count": MAX_TRAINING_ROWS_PER_MODEL,
                "scenario": "ONE_CONTRACT_UNEVEN_FEASIBLE_OUTCOME_SPANS_EXACT_BIJECTION",
                "validation": "NO_ROW_DROP_NO_SAME_SPAN_SOURCE_DISTINCT_WORLD_MAPPINGS",
            },
            "raw_feature_count": MAX_RAW_FEATURES,
            "row_count": MAX_TRAINING_ROWS_PER_MODEL,
            "schema": ML_COMPUTE_FEASIBILITY_SCHEMA,
            "thread_count_each_fit": 1,
            "transformed_feature_count": MAX_TRANSFORMED_FEATURES,
            "workload_projection": workload_projection,
        }
        return {**document, "benchmark_sha256": canonical_sha256(document)}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> MLComputeFeasibilityEvidence:
        expected_keys = {
            "benchmark_date",
            "benchmark_environment",
            "benchmark_sha256",
            "cache_retention_benchmark",
            "execution_path",
            "extrapolation",
            "measurements",
            "null_permutation_benchmark",
            "raw_feature_count",
            "row_count",
            "schema",
            "thread_count_each_fit",
            "transformed_feature_count",
            "workload_projection",
        }
        measurements = value.get("measurements")
        projections = value.get("workload_projection")
        if (
            set(value) != expected_keys
            or not isinstance(measurements, list)
            or not isinstance(projections, list)
        ):
            raise AllCasesMLError("ML compute feasibility evidence keys differ")
        workload_fields = {
            "candidate_kind",
            "fit_recipe_catalog_sha256",
            "fit_recipe_count",
            "learner_id",
            "maximum_raw_feature_count",
            "maximum_training_rows",
            "maximum_transformed_feature_count",
            "scope_keys",
            "search_fit_count",
            "worlds",
        }
        try:
            result = cls(
                tuple(
                    MLFitWorkloadRecord.from_dict({key: item[key] for key in workload_fields})
                    for item in projections
                ),
                tuple(MLSyntheticFitMeasurement.from_dict(item) for item in measurements),
            )
        except (KeyError, TypeError) as error:
            raise AllCasesMLError("ML compute feasibility evidence value differs") from error
        if result.as_dict() != value:
            raise AllCasesMLError("ML compute feasibility evidence did not round trip")
        return result


def _ml_fit_workloads() -> tuple[MLFitWorkloadRecord, ...]:
    direct_counts: dict[str, int] = defaultdict(int)
    meta_counts: dict[str, int] = defaultdict(int)
    for document in DIRECT_FIT_RECIPE_DOCUMENTS:
        direct_counts[str(document["learner_id"])] += 1
    for document in META_FIT_RECIPE_DOCUMENTS:
        meta_counts[str(document["classifier_id"])] += 1
    domains = (tuple(NULL_WORLD_ORDER), (*SEARCH_OUTER_FOLD_KEYS, "SEARCH_FINAL"))
    return tuple(
        MLFitWorkloadRecord(
            "DIRECT",
            learner_id,
            direct_counts[learner_id],
            *domains,
            len(FULL_MTF_213),
            2 * len(FULL_MTF_213),
            DIRECT_FIT_RECIPE_SHA256,
        )
        for learner_id in DIRECT_LEARNERS
    ) + tuple(
        MLFitWorkloadRecord(
            "META",
            learner_id,
            meta_counts[learner_id],
            *domains,
            len(FULL_MTF_PLUS_EXPERT_221),
            2 * len(FULL_MTF_PLUS_EXPERT_221),
            META_FIT_RECIPE_SHA256,
        )
        for learner_id in META_CLASSIFIERS
    )


def _synthetic_fit_measurements() -> tuple[MLSyntheticFitMeasurement, ...]:
    regression_target = "0.5*X0-0.25*X1+0.125*X2+PCG64_NEXT_NORMAL*0.1_BEFORE_MISSINGNESS"
    classification_target = (
        "INT8[(0.5*X0-0.25*X1+0.125*X2+PCG64_NEXT_NORMAL*0.1)>0]_BEFORE_MISSINGNESS"
    )
    regression_dataset_sha256 = "460be44ee2bf5b8ea84769814acd1f1b24ff8602a522cccf36ecd8428fb81fbf"
    classification_dataset_sha256 = (
        "a14bfd4e98c6e60044b23bf06f00475f71b29c21c1ba0d8e885c7307e46254f8"
    )
    values = (
        (
            "DIRECT",
            "ENET_A",
            "1ecf3e340adde98573f82bdd99786be51b31afd8fc37d09c711ab6763c10764d",
            regression_target,
            regression_dataset_sha256,
            83,
            584,
            126,
            793,
            2_848_915_456,
            True,
            0,
            0,
        ),
        (
            "DIRECT",
            "ENET_B",
            "4a5caa088083d22f2455b984f602433f319891bf8383b13020fc4043407cb89e",
            regression_target,
            regression_dataset_sha256,
            83,
            584,
            117,
            784,
            2_848_915_456,
            True,
            0,
            0,
        ),
        (
            "DIRECT",
            "HGB_7",
            "86c913888ef535c100dc5e22dfdf3adfa87813978a4d71bc474880eac20c90da",
            regression_target,
            regression_dataset_sha256,
            84,
            478,
            24_734,
            25_296,
            1_347_682_304,
            False,
            200,
            7,
        ),
        (
            "DIRECT",
            "HGB_15",
            "73c79466c0a75b0023767bc2ba3eb24d71dc1f23609ccbd9c690e7bd1f865e96",
            regression_target,
            regression_dataset_sha256,
            82,
            471,
            28_976,
            29_529,
            1_366_048_768,
            False,
            200,
            15,
        ),
        (
            "META",
            "META_ENET",
            "9f9b2cec421ae2b5bf0d75db9aa4c62c2f52fc74d98c05d759574f02c6ebcb01",
            classification_target,
            classification_dataset_sha256,
            83,
            584,
            5_833,
            6_500,
            2_848_915_456,
            True,
            0,
            0,
        ),
        (
            "META",
            "META_HGB_7",
            "e783f1a4b08c48e78dd26f1feab26b96d00e87481e8b719a078653de31fa223d",
            classification_target,
            classification_dataset_sha256,
            84,
            455,
            27_722,
            28_261,
            1_349_992_448,
            False,
            200,
            7,
        ),
    )
    return tuple(MLSyntheticFitMeasurement(*value) for value in values)


def ml_compute_feasibility_evidence() -> MLComputeFeasibilityEvidence:
    """Return typed max-shape evidence for the actual sequential runner path."""

    return MLComputeFeasibilityEvidence(_ml_fit_workloads(), _synthetic_fit_measurements())


def ml_compute_feasibility_document() -> dict[str, object]:
    """Return the strict serialized max-shape feasibility evidence."""

    return ml_compute_feasibility_evidence().as_dict()


def shared_fit_cache_schedule_document() -> dict[str, object]:
    """Prove bounded cache liveness under the exact ordered candidate catalogs."""

    fit_sequence = tuple(
        (world, fold_key) for world in NULL_WORLD_ORDER for fold_key in SEARCH_OUTER_FOLD_KEYS
    ) + tuple((world, "SEARCH_FINAL") for world in NULL_WORLD_ORDER)

    def simulate(
        catalog: Sequence[DirectCandidate | MetaCandidate],
        *,
        candidate_kind: str,
    ) -> dict[str, object]:
        active: set[tuple[str, str, str]] = set()
        consumed: set[tuple[str, str, str]] = set()
        fit_count = 0
        cache_hits = 0
        peak_state_count = 0
        for candidate in catalog:
            recipe_id = (
                direct_fit_recipe_id(candidate)
                if isinstance(candidate, DirectCandidate)
                else meta_fit_recipe_id(candidate)
            )
            for world, scope_key in fit_sequence:
                key = recipe_id, world, scope_key
                if key in active:
                    active.remove(key)
                    consumed.add(key)
                    cache_hits += 1
                elif key in consumed:
                    raise AllCasesMLError("fit cache catalog key occurs more than twice")
                else:
                    active.add(key)
                    fit_count += 1
                    peak_state_count = max(peak_state_count, len(active))
        expected_fit_count = 3_024 if candidate_kind == "DIRECT" else 2_016
        if (
            active
            or fit_count != expected_fit_count
            or cache_hits != expected_fit_count
            or len(consumed) != expected_fit_count
            or peak_state_count != len(fit_sequence)
        ):
            raise AllCasesMLError("fit cache catalog schedule differs")
        return {
            "cache_hits": cache_hits,
            "candidate_count": len(catalog),
            "candidate_kind": candidate_kind,
            "eviction_count": cache_hits,
            "final_state_count": len(active),
            "fit_count": fit_count,
            "peak_state_count": peak_state_count,
            "rate_variants_per_fit_recipe": 2,
        }

    direct = simulate(DIRECT_CANDIDATE_CATALOG, candidate_kind="DIRECT")
    meta = simulate(META_CANDIDATE_CATALOG, candidate_kind="META")
    document = {
        "asymmetric_early_exit_policy": (
            "DISCARD_LEFTOVER_STATES_BEFORE_NEXT_FIT_RECIPE_AND_AT_CHUNK_TERMINUS;"
            "TYPED_DISCARDED_STATE_COUNT_PLUS_FINAL_ZERO"
        ),
        "direct": direct,
        "fit_order": [
            {"fold_key": fold_key, "null_world": world} for world, fold_key in fit_sequence
        ],
        "meta": meta,
        "schema": "systematic_fx.ai_all_cases_shared_fit_cache_schedule.v1",
        "total_cache_hits": direct["cache_hits"] + meta["cache_hits"],
        "total_fit_count": direct["fit_count"] + meta["fit_count"],
    }
    return {**document, "schedule_sha256": canonical_sha256(document)}


def ml_engine_contract() -> dict[str, object]:
    """Return the finite, precommittable ML engine recipe."""

    from .symbolic import EXPERT_FEATURE_FORMULA_SHA256

    return {
        "admission": {
            "direct": "TRAIN_ONLY_ABSOLUTE_SCORE_QUANTILE_THEN_EXPECTED_GROSS_TICKS_GT_14",
            "meta": "TRAIN_ONLY_POSITIVE_PROBABILITY_QUANTILE",
            "quantile_method": "NUMPY_HIGHER",
            "tie_policy": "ADMIT_ALL_EQUAL_TO_THRESHOLD",
            "world_policy": "EACH_WORLD_OWN_TRAIN_ONLY_THRESHOLD_BEFORE_CONTROL_ALIGNMENT",
        },
        "catalogs": {
            "direct_count": 288,
            "direct_sha256": DIRECT_CATALOG_SHA256,
            "meta_count": 192,
            "meta_sha256": META_CATALOG_SHA256,
        },
        "compute_caps": {
            "direct_fits": 3_024,
            "hgb_max_leaf_nodes": 15,
            "hgb_max_nodes_per_tree": 29,
            "hgb_max_trees": 200,
            "linear_max_iterations": 50_000,
            "maximum_raw_features": MAX_RAW_FEATURES,
            "maximum_search_model_fits": MAX_SEARCH_MODEL_FITS,
            "maximum_training_rows_per_model": MAX_TRAINING_ROWS_PER_MODEL,
            "maximum_transformed_features": MAX_TRANSFORMED_FEATURES,
            "meta_fits": 2_016,
            "wf_holdout_fits": 0,
        },
        "compute_feasibility": ml_compute_feasibility_document(),
        "control_alignment": {
            "controls": "TOP_SCORE_WITHOUT_REPLACEMENT_FROM_INDEPENDENTLY_REQUESTED_POOL",
            "count_key": "DECISION_DATE_PLUS_PREDICTED_OR_BASE_DIRECTION",
            "direct_count_target": "REAL_POST_GLOBAL_OCCUPANCY_MASK",
            "meta_count_target": "REAL_ANCHOR_GATE_BEFORE_SYMBOLIC_PATH_EXECUTION",
            "distinctness": "REAL_CIRCULAR_MATCHED_ALIGNED_MASKS_PAIRWISE_DISTINCT",
            "failure": "CANDIDATE_LOCAL_INELIGIBLE_NO_ROW_DROP",
            "proof": "SOURCE_AND_ALIGNED_MASK_SHA_COUNTS_AND_SELECTED_ROW_IDS",
            "scope": "SEARCH_OOF_AND_EACH_WF_OR_HOLDOUT_PARTITION",
        },
        "cost_ticks": TOTAL_FRICTION_TICKS,
        "direct_row_execution": {
            "decision_date": "VERIFIED_NATIVE_BAR_SOURCE_DATE",
            "decision_ns": "EXACT_FEATURE_CUTOFF_CLOCK_COMPLETED_NATIVE_BAR_END",
            "economics": {
                "allocated_fixed_cost_ticks": 5,
                "entry_adverse_ticks": 2,
                "exit_adverse_ticks": 2,
                "total_friction_ticks": 14,
                "variable_cost_ticks": 5,
            },
            "eligibility": "ONE_STRUCTURALLY_ELIGIBLE_COMPLETED_NATIVE_TIMEFRAME_BAR",
            "entry": (
                "FLOOR_NEXT_EXACT_SAME_LINEAGE_5M_FIRST_TRADE_NS_TO_ONE_SECOND;"
                "DECISION_NS_LTE_ENTRY_NS_LT_DECISION_NS_PLUS_300S"
            ),
            "entry_price": "EXACT_NEXT_SAME_LINEAGE_5M_OPEN_TICKS",
            "entry_schedule_proof": (
                "RUNNER_BINDS_ROW_DECISION_ENTRY_NEXT5M_LINEAGE_SHA_BEFORE_1S;"
                "LATER_1S_STREAM_MUST_VALIDATE_SCHEDULED_INTERVAL_AND_OPEN"
            ),
            "exact_evaluation_response": (
                "CALLER_SUPPLIED_SIGNED_INT64_TERMINAL_MOVE_TICKS;DIRECTION_TIMES_MOVE_MINUS_14"
            ),
            "post_freeze_invalid_no_fill_shortened_censored_or_cross_lineage": (
                "FATAL_INTEGRITY_FAILURE_NEVER_ROW_EXCLUSION"
            ),
            "path": "EXACT_SAME_CONTRACT_OUTCOME_SPAN_SEGMENT_FULL_1H_3H_6H",
            "target": "EXACT_SIGNED_TERMINAL_MOVE_TICKS_DIV_CAUSAL_NATIVE_ATR20_TICKS",
        },
        "fit_recipe_sharing": {
            "cache_key": "FIT_RECIPE_ID_WORLD_FOLD_TRAINING_ROWS_SHA256",
            "chunk_policy": "ONE_FRESH_CACHE_PER_FIXED_CHUNK_RESUME_SAFE",
            "direct_recipe_count": 144,
            "direct_recipe_sha256": DIRECT_FIT_RECIPE_SHA256,
            "eviction": (
                "SECOND_RATE_VARIANT_HIT_IMMEDIATELY_POPS_STATE;ONE_SIDED_RATE_"
                "EARLY_EXIT_REQUIRES_EXPLICIT_TYPED_DISCARD_BEFORE_TERMINAL_PROOF"
            ),
            "meta_recipe_count": 96,
            "meta_recipe_sha256": META_FIT_RECIPE_SHA256,
            "policy": "RATE_VARIANTS_SHARE_PREPROCESSOR_PREDICTOR_AND_TRAINING_SCORES",
            "rate_specific_state": "TRAIN_SCORE_QUANTILE_THRESHOLD_AND_CANDIDATE_ARTIFACT_ONLY",
            "schedule_evidence": shared_fit_cache_schedule_document(),
            "terminal_proof": (
                "24_TYPED_CHUNK_EVIDENCE_RECORDS_AGGREGATE_FITS_HITS_DISCARDS_EVICTIONS_"
                "FINAL_ZERO_AND_PEAK_LTE_21"
            ),
        },
        "ineligibility": {
            "allowed_candidate_local_reasons": [value.value for value in MLIneligibilityReason],
            "handling": "RECORD_REASON_AND_CONTINUE_ALL_480_FIXED_CANDIDATES",
            "integrity_schema_hash_lineage_or_roundtrip_errors": "FATAL_NEVER_BLANKET_CAUGHT",
        },
        "cross_validation": {
            "block_sizes": list(SEARCH_BLOCK_SIZES),
            "decision_date_count": SEARCH_DECISION_DATE_COUNT,
            "final_refit_key": "SEARCH_FINAL",
            "outer_fold_keys": list(SEARCH_OUTER_FOLD_KEYS),
            "outer_policy": "EXPANDING_PRIOR_BLOCKS_VALIDATE_NEXT_B3_THROUGH_B8",
            "purge": "DROP_TRAIN_LABEL_EXIT_NS_GTE_FIRST_VALIDATION_ENTRY_NS",
            "selection_evidence": "CONCATENATED_OUTER_VALIDATION_ONLY",
            "score_artifact_binding": (
                "CANDIDATE_TASK_TIMEFRAME_HORIZON_PLUS_EXACT_ROW_ENTRY_EXIT_AND_PER_FOLD_"
                "OUTCOME_LINEAGE_OPPORTUNITY_LATTICE_ENTRY_SCHEDULE_SOURCE_MATRIX_AND_"
                "EXACT_OUTCOME_VALUES_SHA256"
            ),
        },
        "feature_sets": {
            key: {"count": len(value), "ordered_names_sha256": canonical_sha256(list(value))}
            for key, value in FEATURE_NAMES_BY_SET.items()
        },
        "feature_engine": {
            "artifact": (
                "FROZEN_TYPED_ROWS_SHA256_OVER_SOURCE_INPUT_COMMITMENT_EXACT_FLOAT_HEX_VALUES_"
                "NATIVE_ATR_FULL_ROW_LINEAGE_RETAINED_INDEXES_ALIGNED_BAR_INDEXES_AND_EXPERT_SHA"
            ),
            "alignment": (
                "LATEST_COMPLETED_BAR_AT_OR_BEFORE_DECISION_NS_COMPATIBLE_WITH_CONTRACT_SPAN_"
                "AND_SAME_DATE_SEGMENT_OR_ADJACENT_DATE_96H_BRIDGE"
            ),
            "atr": "SIMPLE_ROLLING_MEAN_TRUE_RANGE_20",
            "bollinger": "CURRENT_MINUS_ROLLING_MEAN_DIV_POPULATION_STD_ZERO_TO_ZERO",
            "buy_sell_missing": "AVAILABILITY_ZERO_AND_IMBALANCE_ZERO_IF_WINDOW_INCOMPLETE",
            "close_location_and_channel_zero_range": "ONE_HALF",
            "cross_agreement": "PRODUCT_OF_TERNARY_SIGNS",
            "ema": "ADJUST_FALSE_ALPHA_2_DIV_SPAN_PLUS_1_SEEDED_FIRST_CLOSE",
            "expert_8": (
                "TYPED_SYMBOLIC_CAUSAL_EXPERT_ARTIFACTS_ONLY_EXACT_RATIONAL_TO_FLOAT64_"
                "WITH_ORDER_ANCHOR_POLICY_RECIPE_FORMULA_AND_ARTIFACT_SHA_BINDING"
            ),
            "expert_8_formula_sha256": EXPERT_FEATURE_FORMULA_SHA256,
            "minimum_completed_bars_each_timeframe": MIN_CAUSAL_HISTORY_BARS,
            "ohlc_input_units": "VERIFIED_TRADE_BAR_INTEGER_TICKS_REPRESENTED_AS_FLOAT64",
            "normalizer": "SAME_TIMEFRAME_CAUSAL_ATR20_TICKS_NO_TICK_SIZE_RECONVERSION",
            "time_feature_clock": "DECISION_NS_ONLY_NEVER_SCHEDULED_ENTRY_NS",
            "rsi": "SIMPLE_ROLLING_GAIN_LOSS_MEAN_ZERO_ZERO_TO_50",
            "stochastic_d": "SIMPLE_MEAN_LAST_3_K",
            "time": "DECISION_NS_UTC_SECOND_OF_DAY_AND_MONDAY_ZERO_WEEKDAY",
            "continuity": (
                "SAME_DATE_EXACT_TIMEFRAME_ADJACENCY_AND_CONTRACT_SPAN_SEGMENT;"
                "CROSS_DATE_SAME_CONTRACT_SPAN_ADJACENT_STAGE_RANK_GAP_LTE_96H;"
                "RESET_ALL_ROLLING_AND_EMA_AT_EVERY_BREAK_AND_STAGE"
            ),
            "volume_z": "CURRENT_MINUS_ROLLING_MEAN_DIV_POPULATION_STD_ZERO_TO_ZERO",
            "vwap_price": "HLC3_WEIGHTED_BY_VOLUME_ZERO_VOLUME_TO_HLC3_MEAN",
        },
        "learners": {
            "ENET_A": {
                "alpha_hex": _float_hex(0.001),
                "fit_intercept": True,
                "l1_ratio_hex": _float_hex(0.1),
                "max_iter": 50_000,
                "selection": "cyclic",
                "tol_hex": _float_hex(1e-8),
            },
            "ENET_B": {
                "alpha_hex": _float_hex(0.01),
                "fit_intercept": True,
                "l1_ratio_hex": _float_hex(0.5),
                "max_iter": 50_000,
                "selection": "cyclic",
                "tol_hex": _float_hex(1e-8),
            },
            "HGB_15": {
                "early_stopping": False,
                "l2_regularization_hex": _float_hex(1.0),
                "learning_rate_hex": _float_hex(0.05),
                "loss": "squared_error",
                "max_bins": 255,
                "max_iter": 200,
                "max_leaf_nodes": 15,
                "min_samples_leaf": 40,
            },
            "HGB_7": {
                "early_stopping": False,
                "l2_regularization_hex": _float_hex(1.0),
                "learning_rate_hex": _float_hex(0.05),
                "loss": "squared_error",
                "max_bins": 255,
                "max_iter": 200,
                "max_leaf_nodes": 7,
                "min_samples_leaf": 40,
            },
            "META_ENET": {
                "C_hex": _float_hex(0.1),
                "class_weight": "balanced",
                "fit_intercept": True,
                "max_iter": 50_000,
                "n_jobs": 1,
                "penalty": "l2",
                "solver": "liblinear",
                "tol_hex": _float_hex(1e-8),
            },
            "META_HGB_7": {
                "early_stopping": False,
                "l2_regularization_hex": _float_hex(1.0),
                "learning_rate_hex": _float_hex(0.05),
                "loss": "log_loss",
                "max_bins": 255,
                "max_iter": 200,
                "max_leaf_nodes": 7,
                "min_samples_leaf": 40,
            },
        },
        "libraries": {
            "numpy": np.__version__,
            "scipy": scipy.__version__,
            "sklearn": sklearn.__version__,
            "sklearn_required": EXPECTED_SKLEARN_VERSION,
        },
        "master_seed": MASTER_SEED,
        "nulls": {
            "algorithmic_complexity": "O_N_LOG_N_TIME_AND_O_N_MEMORY_NO_RECURSIVE_MATCHING",
            "circular": (
                "SAME_CONTRACT_CHRONOLOGICAL_ROTATION_BY_MAXIMUM_SPAN_COUNT_OR_"
                "COMPLEMENT_WITH_CANONICAL_SPAN_BLOCK_FALLBACK"
            ),
            "matched": (
                "SAME_CONTRACT_SPAN_BLOCK_ORDER_BY_COARSE_STRATUM_EXACT_STRATUM_"
                "SEEDED_ROW_HASH_THEN_SPAN_SAFE_MAXIMUM_COUNT_ROTATION_WITH_CANONICAL_"
                "SAME_SPAN_SWAP_OR_UNIQUE_SPAN_ROTATION_IF_EQUAL_TO_CIRCULAR"
            ),
            "matched_distinct_failure": "CANDIDATE_LOCAL_NULL_DERANGEMENT_INFEASIBLE",
            "matched_fallback_counts": "REALIZED_EXACT_COARSE_SAME_CONTRACT_ANY_COUNTS_COMMITTED",
            "order": list(NULL_WORLD_ORDER),
            "per_fold_refit": True,
            "pre_fit_probe": "B3_THROUGH_B8_PURGED_PREFIXES_AND_SEARCH_FINAL_BOTH_NULLS",
            "row_policy": "EXACT_BIJECTION_NO_CARDINALITY_CHANGE_OR_SILENT_DROP",
            "same_outcome_span_source": "PROHIBITED",
            "seed_derivation": (
                "UINT32_FIRST_8_HEX_SHA256_CANONICAL_JSON_OF_MASTER_IDENTITY_WORLD_FOLD_PURPOSE"
            ),
            "target_source_scope": "TRAINING_PREFIX_ONLY",
        },
        "match_strata": {
            "coarse": "DECISION_TIMEFRAME_PLUS_DECISION_NS_UTC_4H_BUCKET",
            "exact": "COARSE_PLUS_DECISION_DATE_WEEKDAY_PLUS_NATIVE_ATR20_FLOOR_LOG2_TICK_BIN",
            "source": "PURE_CAUSAL_FEATURE_ROWS_NO_TARGETS_OR_GLOBAL_QUANTILES",
        },
        "occupancy": {
            "direct_application": (
                "ONCE_AFTER_CHRONOLOGICAL_B3_THROUGH_B8_CONCATENATION_OR_PARTITION_MASK"
            ),
            "equality_rule": "ENTRY_AT_PREVIOUS_EXIT_IS_ALLOWED",
            "meta_application": (
                "ANCHOR_GATE_FILTERS_FROZEN_SYMBOLIC_ORDER_IDS_THEN_SHARED_SYMBOLIC_ENGINE_"
                "RESOLVES_FILL_EXIT_AND_GLOBAL_OCCUPANCY"
            ),
            "position_limit": "ONE_GLOBAL_POSITION_PER_CANDIDATE_MASK",
            "reset": "NEVER_AT_FOLD_CONTRACT_SPAN_SEGMENT_OR_DATE_BOUNDARY",
            "signal_accounting": (
                "RAW_SIGNAL_COUNT_AND_DAYS_FROM_REQUESTED_PRE_OCCUPANCY;"
                "FILLS_DAYS_AND_PNL_FROM_POST_OCCUPANCY_ADMITTED"
            ),
        },
        "opportunity_lattice": {
            "coverage": "EVERY_5M_BAR_END_DECISION_PLUS_300S_THROUGH_PLUS_25200S",
            "binding": "SHA256_IN_TRAINING_MATRIX_SCHEDULE_MASK_AND_OUTCOME_ROWS",
            "freeze": "CANDIDATE_INDEPENDENT_FEATURE_ONLY_BEFORE_ANY_1S_OUTCOME_OPEN",
            "horizon": "UNIFORM_7H_SAME_LINEAGE_5M_COMPLETE_CASE_COVERS_MAXIMUM_DIRECT_6H",
            "post_freeze_missing_or_cross_lineage": "FATAL_INTEGRITY_FAILURE",
            "row_cardinality": (
                "RAW_ANCHORS_AND_ELIGIBILITY_BITS_COMMITTED;ONLY_PRE_OUTCOME_ELIGIBLE_"
                "SUBSET_ALLOWED;NO_OUTCOME_DEPENDENT_ROW_DROP"
            ),
            "schedule_identity_check": (
                "EVERY_FEATURE_ROW_ID_MUST_EQUAL_LATTICE_DECISION_ENTRY_SOURCE_DATE_CONTRACT_"
                "OUTCOME_SPAN_SEGMENT_AND_ELIGIBLE_TRUE"
            ),
        },
        "oos_prediction_freeze": {
            "direct_input": (
                "TYPED_CAUSAL_FEATURE_ROWS_ONLY;VALUES_AND_NATIVE_TIMEFRAME_ATR_DERIVED_"
                "INTERNALLY;RAW_ARRAY_OR_REVERSED_SIDE_CHANNEL_UNREPRESENTABLE"
            ),
            "schedule_binding": (
                "EXACT_CANDIDATE_FEATURE_SET_TASK_TIMEFRAME_HORIZON_STAGE_ROW_DATE_"
                "DECISION_ENTRY_CONTRACT_SPAN_SEGMENT_ENTRY_SCHEDULE_AND_CAUSAL_FEATURE_ROWS_SHA"
            ),
            "stage_date_domain": (
                "TYPED_STAGE_PARTITION_DATE_CERTIFICATE_WITH_DERIVED_UPSTREAM_PLAN_SHA_AND_"
                "ARTIFACT_SHA;ECONOMICS_EMITS_EXPLICIT_ZERO_FOR_EVERY_CERTIFIED_DATE"
            ),
            "scheduled_exit": "ENTRY_NS_PLUS_EXACT_CANDIDATE_HORIZON_NS",
        },
        "preprocessing": {
            "all_missing_training_column": "MODEL_INELIGIBLE_FAIL_CLOSED",
            "imputation": "TRAIN_ONLY_COLUMN_MEDIAN",
            "missing_indicators": "ONLY_COLUMNS_MISSING_IN_TRAINING",
            "scaling": "TRAIN_ONLY_STANDARD_SCALER_LINEAR_MODELS_ONLY",
        },
        "portable_model_artifact": {
            "deep_freeze": "CANDIDATE_LEARNER_RECIPE_AND_PREDICTOR_NESTED_JSON_IMMUTABLE",
            "hgb_validation": "EXACTLY_200_TREES_AND_CANDIDATE_SPECIFIC_7_OR_15_LEAF_CAP",
            "learner_identity": (
                "EXACT_SERIALIZED_PARAMETER_DOCUMENT_SHA_BOUND_INSIDE_PREDICTOR_"
                "DISTINGUISHES_ENET_A_ENET_B_HGB_7_HGB_15_AND_META_LEARNERS"
            ),
        },
        "seed_derivation_example_real_b3": _seed("0" * 64, "REAL", "B3", "FIT"),
        "schema": "systematic_fx.ai_all_cases_ml_contract.v1",
        "search_economics_and_selection": {
            "daily_domain": "EVERY_FROZEN_DECISION_DATE_WITH_EXPLICIT_ZERO",
            "direct_family": "DECISION_TIMEFRAME_SECONDS_PLUS_FEATURE_SET_ID",
            "diversity": {
                "action_identity": "ROW_ID_PLUS_DIRECTION_POST_GLOBAL_OCCUPANCY",
                "daily_pnl_rule": (
                    "25_TIMES_COVARIANCE_NUMERATOR_SQUARED_LT_16_TIMES_VARIANCE_PRODUCT"
                ),
                "jaccard_rule": "5_TIMES_INTERSECTION_LT_4_TIMES_UNION",
                "policy": "GREEDY_IN_FROZEN_ECONOMIC_RANK_ORDER",
                "zero_variance": "FAIL_CLOSED_PAIR_REJECTED",
            },
            "exact_arithmetic": "INT64_INPUT_TICKS_INTEGER_AGGREGATES_AND_FRACTION_EV",
            "family_maximum": 2,
            "gates": {
                "active_entry_days_minimum": 30,
                "active_signal_days_minimum": 40,
                "fills_each_outer_minimum": 5,
                "fills_minimum": 48,
                "positive_outer_minimum": 4,
                "positive_reporting_minimum": 3,
                "profit_factor_minimum": "21/20",
                "raw_signals_each_outer_minimum": 6,
                "raw_signals_each_reporting_minimum": 6,
                "raw_signals_minimum": 60,
                "real_net_strictly_above_both_controls": True,
                "standard_and_18_tick_stress_net_strictly_positive": True,
            },
            "meta_family": "INHERITED_PREFIX_OR_SEARCH_FINAL_BASE_TRIGGER_FAMILY",
            "ranking": [
                "POSITIVE_OUTER_VALIDATION_COUNT_DESC",
                "WORST_OUTER_VALIDATION_EV_DESC",
                "STRESS_18_TICK_NET_DESC",
                "MEDIAN_OUTER_VALIDATION_EV_DESC",
                "MAXIMUM_DRAWDOWN_ASC",
                "CANONICAL_CANDIDATE_ID_ASC",
            ],
            "selection_direct_maximum": 4,
            "selection_meta_maximum": 4,
            "selection_total_maximum": 8,
        },
        "symbolic_meta_rank": {
            "certificate": (
                "TYPED_WORLD_FOLD_EXACT_TRAIN_DATE_DOMAIN_ORDERED_ZERO_TO_24_STRATEGIES_"
                "WITH_EXACT_BASE_CONTEXT_TIME_FILTER_DELAY_ANCHOR_ENTRY_EXIT_POLICY_IDS_"
                "CATALOG_REDERIVATION_TRIGGER_FAMILY_AND_DERIVED_STRATEGY_ID"
            ),
            "expert_rebuild": (
                "REBUILD_EACH_EXPERT_ARTIFACT_FROM_CERTIFIED_CATALOG_POLICY_ORDER_AND_EXIT;"
                "PLUS_EXPERT_LAST_EIGHT_FLOAT_HEX_MUST_EQUAL_EXACT_RATIONAL_CONVERSION"
            ),
            "final": "FULL_SEARCH_RERANK_THEN_FIT_AND_FREEZE_AVAILABLE_STRATEGY_SLOTS",
            "missing_rank": "CANDIDATE_LOCAL_INSUFFICIENT_BASE_STRATEGY_RANK",
            "outer": "RERANK_COMPLETE_SYMBOLIC_STRATEGIES_USING_PRIOR_OUTER_TRAIN_ONLY",
            "rank_slot_count": 24,
            "world": "RERUN_SYMBOLIC_RANK_PREPROCESS_AND_FIT_INDEPENDENTLY_FOR_EACH_NULL_WORLD",
        },
        "meta_oos_execution": {
            "decision_ns": "ANCHOR_RECORD_ANCHOR_NS_EQUALS_ANCHOR_KEY_INDEX_3",
            "forbidden_pre_freeze_fields": [
                "ACTUAL_ENTRY_NS",
                "ENTRY_FILL_TICKS",
                "ACTUAL_EXIT_NS",
                "REALIZED_NET_TICKS",
            ],
            "gate": "SEARCH_FINAL_PROBABILITY_OVER_FROZEN_SYMBOLIC_ORDER_ID_AT_ANCHOR",
            "recipe_binding": (
                "TYPED_COMPLETE_STRATEGY_RECIPE_ENTRY_ORDER_BATCH_AND_FORMULA_BOUND_EXPERT_"
                "ARTIFACTS_MUST_MATCH_CERTIFIED_ANCHOR_ENTRY_EXIT_POLICY_IDS"
            ),
            "routing": "ALIGNED_GATE_FILTER_THEN_SHARED_SYMBOLIC_PATH_EVALUATOR",
        },
        "typed_integer_validation": {
            "direct_outcome_inputs": (
                "RAW_FILL_EXIT_SPAN_SEGMENT_ENTRY_AND_TERMINAL_ARRAYS_MUST_HAVE_SIGNED_OR_"
                "UNSIGNED_INTEGER_DTYPE;BOOL_FLOAT_AND_STRING_COERCION_PROHIBITED"
            ),
            "meta_outcome_inputs": (
                "RAW_BASE_ENTRY_LABEL_EXIT_SPAN_SEGMENT_AND_DIRECTION_ARRAYS_REQUIRE_INTEGER_"
                "DTYPE;DIRECTIONS_EXACTLY_PLUS_OR_MINUS_ONE;BOOL_FLOAT_STRING_REJECTED"
            ),
            "outcome_free_sources": (
                "BAR_END_DECISION_ENTRY_SPAN_SEGMENT_AND_STAGE_RANK_ARRAYS_REQUIRE_RAW_"
                "SIGNED_OR_UNSIGNED_INTEGER_DTYPE;BOOL_FLOAT_AND_STRING_REJECTED"
            ),
        },
        "typed_continuous_validation": {
            "policy": (
                "RAW_CONTINUOUS_ARRAYS_REQUIRE_SIGNED_UNSIGNED_OR_FLOATING_NUMPY_DTYPE;"
                "BOOL_STRING_AND_OBJECT_COERCION_PROHIBITED"
            ),
            "scope": "CAUSAL_BARS_FEATURE_ARTIFACTS_TRAINING_MATRICES_AND_ATR_INPUTS",
        },
        "thread_environment": dict(DETERMINISTIC_THREAD_ENV),
        "wf_holdout": {
            "daily_output": "EXACT_INTEGER_NET_TICKS_ALL_DATES_EXPLICIT_ZERO_BY_WORLD",
            "daily_domain": "FULL_VERIFIED_STAGE_DECISION_DATE_TUPLE_BOUND_IN_PRE_OUTCOME_SCHEDULE",
            "mask_before_outcome": True,
            "model": "SEARCH_FINAL_FROZEN_CANONICAL_ARTIFACT_REOPENED_BYTE_EXACT",
            "refit": False,
        },
        "wf_refit": False,
    }


def ml_contract() -> dict[str, object]:
    """Backward-compatible name for :func:`ml_engine_contract`."""

    return ml_engine_contract()


@dataclass(slots=True)
class CausalBarSeries:
    """Verified stage-local bars with immutable lineage and availability time."""

    timeframe_seconds: int
    bar_end_ns: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    trade_count: np.ndarray
    source_dates: tuple[date, ...]
    contracts: tuple[str, ...]
    outcome_span_ids: np.ndarray
    segment_ids: np.ndarray
    stage_date_ranks: np.ndarray
    stage_key: str
    buy_volume: np.ndarray | None = None
    sell_volume: np.ndarray | None = None
    _indices_by_contract_span: dict[tuple[str, int], tuple[int, ...]] = field(
        init=False,
        repr=False,
    )
    _ends_by_contract_span: dict[tuple[str, int], tuple[int, ...]] = field(
        init=False,
        repr=False,
    )
    _continuity_by_index: np.ndarray = field(init=False, repr=False)
    _run_length_by_index: np.ndarray = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.timeframe_seconds not in TF_ORDER:
            raise AllCasesMLError("bar-series timeframe differs")
        raw_end = np.asarray(self.bar_end_ns)
        if raw_end.ndim != 1 or len(raw_end) < MIN_CAUSAL_HISTORY_BARS:
            raise AllCasesMLError("bar-series end times differ")
        end = _deep_exact_int64_array(
            self.bar_end_ns,
            (len(raw_end),),
            "bar_end_ns",
        )
        if np.any(end <= 0):
            raise AllCasesMLError("bar-series end times differ")
        self.bar_end_ns = end
        count = len(end)
        if (
            len(self.source_dates) != count
            or len(self.contracts) != count
            or any(
                isinstance(value, datetime) or not isinstance(value, date)
                for value in self.source_dates
            )
            or any(not isinstance(value, str) or not value for value in self.contracts)
            or not isinstance(self.stage_key, str)
            or not self.stage_key
        ):
            raise AllCasesMLError("bar-series lineage metadata differs")
        self.outcome_span_ids = _deep_exact_int64_array(
            self.outcome_span_ids,
            (count,),
            "bar outcome_span_ids",
        )
        self.segment_ids = _deep_exact_int64_array(
            self.segment_ids,
            (count,),
            "bar segment_ids",
        )
        self.stage_date_ranks = _deep_exact_int64_array(
            self.stage_date_ranks,
            (count,),
            "bar stage_date_ranks",
        )
        if (
            np.any(self.outcome_span_ids <= 0)
            or np.any(self.segment_ids <= 0)
            or np.any(self.stage_date_ranks < 0)
        ):
            raise AllCasesMLError("bar-series lineage integers differ")
        date_ranks: dict[date, int] = {}
        for source_date, rank in zip(self.source_dates, self.stage_date_ranks, strict=True):
            prior = date_ranks.setdefault(source_date, int(rank))
            if prior != int(rank):
                raise AllCasesMLError("one stage date maps to multiple ranks")
        ordered_identity = tuple(
            (
                int(end[index]),
                self.contracts[index],
                int(self.outcome_span_ids[index]),
                int(self.segment_ids[index]),
            )
            for index in range(count)
        )
        if ordered_identity != tuple(sorted(set(ordered_identity))):
            raise AllCasesMLError("bar-series canonical lineage order differs")
        for name in ("open", "high", "low", "close", "volume", "trade_count"):
            value = _deep_numeric_float64_array(getattr(self, name), (count,), name)
            if not np.isfinite(value).all():
                raise AllCasesMLError(f"bar-series {name} is non-finite")
            setattr(self, name, value)
        if (
            np.any(self.high < np.maximum(self.open, self.close))
            or np.any(self.low > np.minimum(self.open, self.close))
            or np.any(self.high < self.low)
            or np.any(self.volume < 0)
            or np.any(self.trade_count < 0)
        ):
            raise AllCasesMLError("bar-series OHLC or flow invariants differ")
        if (self.buy_volume is None) != (self.sell_volume is None):
            raise AllCasesMLError("buy/sell volume availability is partial")
        if self.buy_volume is not None:
            buy = _deep_numeric_float64_array(self.buy_volume, (count,), "buy_volume")
            sell = _deep_numeric_float64_array(self.sell_volume, (count,), "sell_volume")
            paired = np.isnan(buy) == np.isnan(sell)
            if (
                not paired.all()
                or np.isinf(buy).any()
                or np.isinf(sell).any()
                or np.any(buy[~np.isnan(buy)] < 0)
                or np.any(sell[~np.isnan(sell)] < 0)
            ):
                raise AllCasesMLError("buy/sell volume values differ")
            self.buy_volume = buy
            self.sell_volume = sell
        groups: dict[tuple[str, int], list[int]] = defaultdict(list)
        for index in range(count):
            groups[self.contracts[index], int(self.outcome_span_ids[index])].append(index)
        self._indices_by_contract_span = {key: tuple(indexes) for key, indexes in groups.items()}
        self._ends_by_contract_span = {
            key: tuple(int(end[index]) for index in indexes)
            for key, indexes in self._indices_by_contract_span.items()
        }
        continuity = np.zeros(count, dtype=np.bool_)
        run_length = np.ones(count, dtype=np.int64)
        duration_ns = self.timeframe_seconds * 1_000_000_000
        for indexes in self._indices_by_contract_span.values():
            for previous, current in pairwise(indexes):
                same_date = self.source_dates[previous] == self.source_dates[current]
                continues = (
                    int(end[previous]) == int(end[current]) - duration_ns
                    and self.contracts[previous] == self.contracts[current]
                    and self.outcome_span_ids[previous] == self.outcome_span_ids[current]
                    and self.segment_ids[previous] == self.segment_ids[current]
                    if same_date
                    else int(self.stage_date_ranks[current])
                    == int(self.stage_date_ranks[previous]) + 1
                    and self.contracts[previous] == self.contracts[current]
                    and self.outcome_span_ids[previous] == self.outcome_span_ids[current]
                    and 0
                    <= int(end[current]) - duration_ns - int(end[previous])
                    <= 96 * 3_600 * 1_000_000_000
                )
                continuity[current] = continues
                run_length[current] = run_length[previous] + 1 if continues else 1
        continuity.setflags(write=False)
        run_length.setflags(write=False)
        self._continuity_by_index = continuity
        self._run_length_by_index = run_length


def _rolling_sum(values: np.ndarray, period: int) -> np.ndarray:
    output = np.full(len(values), np.nan, dtype=np.float64)
    cumulative = np.concatenate(([0.0], np.cumsum(values, dtype=np.float64)))
    output[period - 1 :] = cumulative[period:] - cumulative[:-period]
    return output


def _rolling_mean(values: np.ndarray, period: int) -> np.ndarray:
    return _rolling_sum(values, period) / period


def _rolling_extreme(values: np.ndarray, period: int, *, minimum: bool) -> np.ndarray:
    output = np.full(len(values), np.nan, dtype=np.float64)
    queue: deque[int] = deque()
    for index, value in enumerate(values):
        while queue and queue[0] <= index - period:
            queue.popleft()
        while queue and (values[queue[-1]] >= value if minimum else values[queue[-1]] <= value):
            queue.pop()
        queue.append(index)
        if index >= period - 1:
            output[index] = values[queue[0]]
    return output


def _rolling_z(values: np.ndarray, period: int) -> np.ndarray:
    mean = _rolling_mean(values, period)
    variance = np.maximum(_rolling_mean(values * values, period) - mean * mean, 0.0)
    standard_deviation = np.sqrt(variance)
    output = np.zeros(len(values), dtype=np.float64)
    valid = np.isfinite(mean) & (standard_deviation > 0)
    output[valid] = (values[valid] - mean[valid]) / standard_deviation[valid]
    return output


def _ema(values: np.ndarray, span: int) -> np.ndarray:
    alpha = 2.0 / (span + 1.0)
    output = np.empty(len(values), dtype=np.float64)
    output[0] = values[0]
    for index in range(1, len(values)):
        output[index] = alpha * values[index] + (1.0 - alpha) * output[index - 1]
    return output


def _divide(
    numerator: np.ndarray,
    denominator: np.ndarray,
    *,
    zero_value: float = 0.0,
) -> np.ndarray:
    output = np.full(np.broadcast_shapes(numerator.shape, denominator.shape), zero_value)
    valid = np.isfinite(numerator) & np.isfinite(denominator) & (denominator != 0)
    np.divide(numerator, denominator, out=output, where=valid)
    return output


def _true_range(series: CausalBarSeries) -> np.ndarray:
    previous_close = np.concatenate(([series.close[0]], series.close[:-1]))
    return np.maximum.reduce(
        (
            series.high - series.low,
            np.abs(series.high - previous_close),
            np.abs(series.low - previous_close),
        )
    )


def _contiguous_feature_columns(
    series: CausalBarSeries | SimpleNamespace,
) -> tuple[dict[str, np.ndarray], np.ndarray]:
    close = series.close
    true_range = _true_range(series)
    atr20 = _rolling_mean(true_range, 20)
    columns: dict[str, np.ndarray] = {}
    for lag in (1, 2, 3, 6, 12, 24):
        difference = np.zeros(len(close), dtype=np.float64)
        difference[lag:] = close[lag:] - close[:-lag]
        columns[f"ret_atr_{lag}"] = _divide(difference, atr20)
    columns["body_atr"] = _divide(series.close - series.open, atr20)
    columns["range_atr"] = _divide(series.high - series.low, atr20)
    columns["upper_wick_atr"] = _divide(series.high - np.maximum(series.open, series.close), atr20)
    columns["lower_wick_atr"] = _divide(np.minimum(series.open, series.close) - series.low, atr20)
    columns["close_location"] = _divide(
        series.close - series.low,
        series.high - series.low,
        zero_value=0.5,
    )
    previous_close = np.concatenate(([series.close[0]], series.close[:-1]))
    columns["gap_atr"] = _divide(series.open - previous_close, atr20)

    for fast, slow in _EMA_PAIRS:
        fast_ema = _ema(close, fast)
        slow_ema = _ema(close, slow)
        fast_delta = np.concatenate(([0.0], np.diff(fast_ema)))
        columns[f"ema_{fast}_{slow}_distance_atr"] = _divide(fast_ema - slow_ema, atr20)
        columns[f"ema_{fast}_{slow}_fast_slope_atr"] = _divide(fast_delta, atr20)
    for fast, slow, signal in _MACD_TRIPLES:
        macd = _ema(close, fast) - _ema(close, slow)
        histogram = macd - _ema(macd, signal)
        histogram_delta = np.concatenate(([0.0], np.diff(histogram)))
        columns[f"macd_{fast}_{slow}_{signal}_hist_atr"] = _divide(histogram, atr20)
        columns[f"macd_{fast}_{slow}_{signal}_hist_delta_atr"] = _divide(histogram_delta, atr20)
    delta = np.concatenate(([0.0], np.diff(close)))
    gains = np.maximum(delta, 0.0)
    losses = np.maximum(-delta, 0.0)
    for period in (7, 14, 21):
        average_gain = _rolling_mean(gains, period)
        average_loss = _rolling_mean(losses, period)
        rsi = np.full(len(close), 50.0)
        only_gain = (average_gain > 0) & (average_loss == 0)
        both = (average_gain > 0) & (average_loss > 0)
        rsi[only_gain] = 100.0
        rsi[both] = 100.0 - 100.0 / (1.0 + average_gain[both] / average_loss[both])
        columns[f"rsi_{period}"] = rsi
    for period in (5, 9, 14):
        lowest = _rolling_extreme(series.low, period, minimum=True)
        highest = _rolling_extreme(series.high, period, minimum=False)
        stochastic = 100.0 * _divide(close - lowest, highest - lowest, zero_value=0.5)
        stochastic_d = np.full(len(close), 50.0)
        for index in range(period + 1, len(close)):
            stochastic_d[index] = float(np.mean(stochastic[index - 2 : index + 1]))
        columns[f"stoch_{period}_k"] = stochastic
        columns[f"stoch_{period}_k_minus_d"] = stochastic - stochastic_d
    for period in (10, 20, 40):
        columns[f"bollinger_z_{period}"] = _rolling_z(close, period)
    absolute_delta = np.abs(delta)
    for period in (10, 20, 40):
        path = _rolling_sum(absolute_delta, period)
        displacement = np.zeros(len(close), dtype=np.float64)
        displacement[period:] = np.abs(close[period:] - close[:-period])
        efficiency = _divide(displacement, path)
        efficiency[:period] = 0.0
        columns[f"efficiency_ratio_{period}"] = efficiency
    for period in (6, 12, 24, 48):
        lowest = _rolling_extreme(series.low, period, minimum=True)
        highest = _rolling_extreme(series.high, period, minimum=False)
        columns[f"donchian_position_{period}"] = _divide(
            close - lowest, highest - lowest, zero_value=0.5
        )
    typical_price = (series.high + series.low + series.close) / 3.0
    for period in (12, 24, 48):
        volume_sum = _rolling_sum(series.volume, period)
        weighted_sum = _rolling_sum(typical_price * series.volume, period)
        fallback = _rolling_mean(typical_price, period)
        vwap = np.array(fallback, copy=True)
        valid = volume_sum > 0
        vwap[valid] = weighted_sum[valid] / volume_sum[valid]
        columns[f"vwap_distance_atr_{period}"] = _divide(close - vwap, atr20)
    for short, long in ((3, 12), (6, 24), (12, 48)):
        columns[f"atr_ratio_{short}_{long}"] = _divide(
            _rolling_mean(true_range, short), _rolling_mean(true_range, long)
        )

    for period in (12, 24, 48):
        columns[f"volume_z_{period}"] = _rolling_z(series.volume, period)
        columns[f"trade_count_z_{period}"] = _rolling_z(series.trade_count, period)
    if series.buy_volume is None or series.sell_volume is None:
        availability = np.zeros(len(close), dtype=np.float64)
        buy = np.zeros(len(close), dtype=np.float64)
        sell = np.zeros(len(close), dtype=np.float64)
    else:
        availability = (~np.isnan(series.buy_volume)).astype(np.float64)
        buy = np.nan_to_num(series.buy_volume, nan=0.0)
        sell = np.nan_to_num(series.sell_volume, nan=0.0)
    for period in (1, 12, 24):
        buy_sum = buy if period == 1 else _rolling_sum(buy, period)
        sell_sum = sell if period == 1 else _rolling_sum(sell, period)
        available_count = availability if period == 1 else _rolling_sum(availability, period)
        imbalance = _divide(buy_sum - sell_sum, buy_sum + sell_sum)
        imbalance[available_count != period] = 0.0
        columns[f"buy_imbalance_{period}"] = imbalance
    columns["buy_sell_available"] = availability
    return columns, atr20


def _timeframe_feature_columns(
    series: CausalBarSeries,
) -> tuple[dict[str, np.ndarray], np.ndarray]:
    """Compute each continuity run independently, including EMA initialization."""

    output = {
        suffix: np.full(len(series.close), np.nan, dtype=np.float64)
        for suffix in (*_PRICE_SUFFIXES, *_TECH_SUFFIXES, *_FLOW_SUFFIXES)
    }
    atr20 = np.full(len(series.close), np.nan, dtype=np.float64)
    for group_indexes in series._indices_by_contract_span.values():
        run_start = 0
        for position in range(1, len(group_indexes) + 1):
            boundary = (
                position == len(group_indexes)
                or not series._continuity_by_index[group_indexes[position]]
            )
            if not boundary:
                continue
            selected = np.asarray(group_indexes[run_start:position], dtype=np.int64)
            view = SimpleNamespace(
                open=series.open[selected],
                high=series.high[selected],
                low=series.low[selected],
                close=series.close[selected],
                volume=series.volume[selected],
                trade_count=series.trade_count[selected],
                buy_volume=(None if series.buy_volume is None else series.buy_volume[selected]),
                sell_volume=(None if series.sell_volume is None else series.sell_volume[selected]),
            )
            local_columns, local_atr = _contiguous_feature_columns(view)
            for suffix, values in local_columns.items():
                output[suffix][selected] = values
            atr20[selected] = local_atr
            run_start = position
    return output, atr20


@dataclass(slots=True)
class CausalAnchorRows:
    """Stage-local anchor identities used for lineage-compatible as-of joins."""

    row_ids: tuple[str, ...]
    decision_ns: np.ndarray
    entry_ns: np.ndarray
    source_dates: tuple[date, ...]
    contracts: tuple[str, ...]
    outcome_span_ids: np.ndarray
    segment_ids: np.ndarray
    stage_date_ranks: np.ndarray
    stage_key: str
    decision_timeframe_seconds: int
    entry_schedule_sha256: str

    def __post_init__(self) -> None:
        count = len(self.row_ids)
        if (
            count < 2
            or len(set(self.row_ids)) != count
            or len(self.source_dates) != count
            or len(self.contracts) != count
            or any(
                isinstance(value, datetime) or not isinstance(value, date)
                for value in self.source_dates
            )
            or any(not isinstance(value, str) or not value for value in self.contracts)
            or not isinstance(self.stage_key, str)
            or not self.stage_key
            or self.decision_timeframe_seconds not in TF_ORDER
        ):
            raise AllCasesMLError("causal anchor metadata differs")
        self.decision_ns = _deep_exact_int64_array(
            self.decision_ns,
            (count,),
            "anchor decision_ns",
        )
        self.entry_ns = _deep_exact_int64_array(
            self.entry_ns,
            (count,),
            "anchor entry_ns",
        )
        self.outcome_span_ids = _deep_exact_int64_array(
            self.outcome_span_ids,
            (count,),
            "anchor outcome_span_ids",
        )
        self.segment_ids = _deep_exact_int64_array(
            self.segment_ids,
            (count,),
            "anchor segment_ids",
        )
        self.stage_date_ranks = _deep_exact_int64_array(
            self.stage_date_ranks,
            (count,),
            "anchor stage_date_ranks",
        )
        if (
            np.any(self.decision_ns <= 0)
            or np.any(self.entry_ns < self.decision_ns)
            or np.any(self.entry_ns % 1_000_000_000 != 0)
            or np.any(self.outcome_span_ids <= 0)
            or np.any(self.segment_ids <= 0)
            or np.any(self.stage_date_ranks < 0)
            or tuple(zip(self.decision_ns.tolist(), self.row_ids, strict=True))
            != tuple(sorted(zip(self.decision_ns.tolist(), self.row_ids, strict=True)))
            or tuple(zip(self.entry_ns.tolist(), self.row_ids, strict=True))
            != tuple(sorted(zip(self.entry_ns.tolist(), self.row_ids, strict=True)))
            or len(self.entry_schedule_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.entry_schedule_sha256)
        ):
            raise AllCasesMLError("causal anchor lineage or chronology differs")
        date_ranks: dict[date, int] = {}
        for source_date, rank in zip(self.source_dates, self.stage_date_ranks, strict=True):
            prior = date_ranks.setdefault(source_date, int(rank))
            if prior != int(rank):
                raise AllCasesMLError("one anchor stage date maps to multiple ranks")


@dataclass(frozen=True, slots=True)
class StructuralOpportunityLattice:
    """Feature-only 7h same-lineage 5m coverage over every raw anchor."""

    row_ids: tuple[str, ...]
    decision_ns: tuple[int, ...]
    entry_ns: tuple[int, ...]
    source_dates: tuple[date, ...]
    contracts: tuple[str, ...]
    outcome_span_ids: tuple[int, ...]
    segment_ids: tuple[int, ...]
    eligible: tuple[bool, ...]
    stage_key: str
    entry_schedule_sha256: str
    lookahead_seconds: int
    artifact_sha256: str

    def definition_dict(self) -> dict[str, object]:
        return {
            "entry_schedule_sha256": self.entry_schedule_sha256,
            "lookahead_seconds": self.lookahead_seconds,
            "rows": [
                {
                    "contract": contract,
                    "decision_ns": decision,
                    "eligible": eligible,
                    "entry_ns": entry,
                    "outcome_span_id": span,
                    "row_id": row_id,
                    "segment_id": segment,
                    "source_date": source_date.isoformat(),
                }
                for (
                    row_id,
                    decision,
                    entry,
                    source_date,
                    contract,
                    span,
                    segment,
                    eligible,
                ) in zip(
                    self.row_ids,
                    self.decision_ns,
                    self.entry_ns,
                    self.source_dates,
                    self.contracts,
                    self.outcome_span_ids,
                    self.segment_ids,
                    self.eligible,
                    strict=True,
                )
            ],
            "schema": "systematic_fx.ai_all_cases_structural_opportunity_lattice.v1",
            "stage_key": self.stage_key,
        }

    def __post_init__(self) -> None:
        count = len(self.row_ids)
        if (
            count < 2
            or len(set(self.row_ids)) != count
            or any(
                len(values) != count
                for values in (
                    self.decision_ns,
                    self.entry_ns,
                    self.source_dates,
                    self.contracts,
                    self.outcome_span_ids,
                    self.segment_ids,
                    self.eligible,
                )
            )
            or self.lookahead_seconds != 25_200
            or not self.stage_key
            or len(self.entry_schedule_sha256) != 64
            or len(self.artifact_sha256) != 64
            or canonical_sha256(self.definition_dict()) != self.artifact_sha256
        ):
            raise AllCasesMLError("structural opportunity lattice differs")

    @property
    def eligible_row_ids(self) -> tuple[str, ...]:
        return tuple(
            row_id for row_id, eligible in zip(self.row_ids, self.eligible, strict=True) if eligible
        )

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def build_structural_opportunity_lattice(
    anchors: CausalAnchorRows,
    five_minute_bars: CausalBarSeries,
) -> StructuralOpportunityLattice:
    """Freeze uniform 7h future 5m coverage without reading any 1s price outcome."""

    if (
        five_minute_bars.timeframe_seconds != 300
        or five_minute_bars.stage_key != anchors.stage_key
        or np.any(anchors.entry_ns >= anchors.decision_ns + 300 * 1_000_000_000)
    ):
        raise AllCasesMLError("structural lattice stage or direct entry schedule differs")
    exact_bars = {
        (
            int(five_minute_bars.bar_end_ns[index]),
            five_minute_bars.contracts[index],
            int(five_minute_bars.outcome_span_ids[index]),
            int(five_minute_bars.segment_ids[index]),
        )
        for index in range(len(five_minute_bars.bar_end_ns))
    }
    step_ns = 300 * 1_000_000_000
    lookahead_seconds = 25_200
    step_count = lookahead_seconds // 300
    eligible = tuple(
        all(
            (
                int(anchors.decision_ns[index]) + step * step_ns,
                anchors.contracts[index],
                int(anchors.outcome_span_ids[index]),
                int(anchors.segment_ids[index]),
            )
            in exact_bars
            for step in range(1, step_count + 1)
        )
        for index in range(len(anchors.row_ids))
    )
    definition = {
        "entry_schedule_sha256": anchors.entry_schedule_sha256,
        "lookahead_seconds": lookahead_seconds,
        "rows": [
            {
                "contract": anchors.contracts[index],
                "decision_ns": int(anchors.decision_ns[index]),
                "eligible": eligible[index],
                "entry_ns": int(anchors.entry_ns[index]),
                "outcome_span_id": int(anchors.outcome_span_ids[index]),
                "row_id": anchors.row_ids[index],
                "segment_id": int(anchors.segment_ids[index]),
                "source_date": anchors.source_dates[index].isoformat(),
            }
            for index in range(len(anchors.row_ids))
        ],
        "schema": "systematic_fx.ai_all_cases_structural_opportunity_lattice.v1",
        "stage_key": anchors.stage_key,
    }
    return StructuralOpportunityLattice(
        anchors.row_ids,
        tuple(int(value) for value in anchors.decision_ns),
        tuple(int(value) for value in anchors.entry_ns),
        anchors.source_dates,
        anchors.contracts,
        tuple(int(value) for value in anchors.outcome_span_ids),
        tuple(int(value) for value in anchors.segment_ids),
        eligible,
        anchors.stage_key,
        anchors.entry_schedule_sha256,
        lookahead_seconds,
        canonical_sha256(definition),
    )


def apply_structural_opportunity_lattice(
    anchors: CausalAnchorRows,
    lattice: StructuralOpportunityLattice,
) -> CausalAnchorRows:
    """Take the pre-outcome complete cases while retaining raw counts in the lattice."""

    if (
        anchors.row_ids != lattice.row_ids
        or tuple(int(value) for value in anchors.decision_ns) != lattice.decision_ns
        or tuple(int(value) for value in anchors.entry_ns) != lattice.entry_ns
        or anchors.source_dates != lattice.source_dates
        or anchors.contracts != lattice.contracts
        or tuple(int(value) for value in anchors.outcome_span_ids) != lattice.outcome_span_ids
        or tuple(int(value) for value in anchors.segment_ids) != lattice.segment_ids
        or anchors.stage_key != lattice.stage_key
        or anchors.entry_schedule_sha256 != lattice.entry_schedule_sha256
    ):
        raise AllCasesMLError("anchor/lattice binding differs")
    indexes = np.flatnonzero(np.asarray(lattice.eligible, dtype=np.bool_))
    if len(indexes) < 2:
        raise AllCasesMLError("structural lattice retains fewer than two anchors")
    return CausalAnchorRows(
        row_ids=tuple(anchors.row_ids[index] for index in indexes),
        decision_ns=anchors.decision_ns[indexes],
        entry_ns=anchors.entry_ns[indexes],
        source_dates=tuple(anchors.source_dates[index] for index in indexes),
        contracts=tuple(anchors.contracts[index] for index in indexes),
        outcome_span_ids=anchors.outcome_span_ids[indexes],
        segment_ids=anchors.segment_ids[indexes],
        stage_date_ranks=anchors.stage_date_ranks[indexes],
        stage_key=anchors.stage_key,
        decision_timeframe_seconds=anchors.decision_timeframe_seconds,
        entry_schedule_sha256=anchors.entry_schedule_sha256,
    )


def _causal_feature_source_commitment_sha256(
    anchors: CausalAnchorRows,
    bars_by_timeframe: Mapping[int, CausalBarSeries],
) -> str:
    """Commit every outcome-free input byte from which features are derived."""

    digest = hashlib.sha256()
    digest.update(
        canonical_json_bytes(
            {
                "entry_schedule_sha256": anchors.entry_schedule_sha256,
                "schema": "systematic_fx.ai_all_cases_causal_feature_source.v1",
                "stage_key": anchors.stage_key,
                "timeframes": list(TF_ORDER),
            }
        )
    )
    digest.update(b"\n")
    for index, row_id in enumerate(anchors.row_ids):
        digest.update(
            canonical_json_bytes(
                {
                    "contract": anchors.contracts[index],
                    "decision_ns": int(anchors.decision_ns[index]),
                    "entry_ns": int(anchors.entry_ns[index]),
                    "outcome_span_id": int(anchors.outcome_span_ids[index]),
                    "row_id": row_id,
                    "segment_id": int(anchors.segment_ids[index]),
                    "source_date": anchors.source_dates[index].isoformat(),
                    "stage_date_rank": int(anchors.stage_date_ranks[index]),
                }
            )
        )
        digest.update(b"\n")
    for timeframe in TF_ORDER:
        series = bars_by_timeframe[timeframe]
        digest.update(
            canonical_json_bytes(
                {
                    "buy_sell_available": series.buy_volume is not None,
                    "stage_key": series.stage_key,
                    "timeframe_seconds": series.timeframe_seconds,
                }
            )
        )
        digest.update(b"\n")
        for index in range(len(series.bar_end_ns)):
            digest.update(
                canonical_json_bytes(
                    {
                        "bar_end_ns": int(series.bar_end_ns[index]),
                        "buy_volume_hex": (
                            None
                            if series.buy_volume is None
                            else (
                                "nan"
                                if math.isnan(float(series.buy_volume[index]))
                                else _float_hex(float(series.buy_volume[index]))
                            )
                        ),
                        "close_hex": _float_hex(float(series.close[index])),
                        "contract": series.contracts[index],
                        "high_hex": _float_hex(float(series.high[index])),
                        "low_hex": _float_hex(float(series.low[index])),
                        "open_hex": _float_hex(float(series.open[index])),
                        "outcome_span_id": int(series.outcome_span_ids[index]),
                        "segment_id": int(series.segment_ids[index]),
                        "sell_volume_hex": (
                            None
                            if series.sell_volume is None
                            else (
                                "nan"
                                if math.isnan(float(series.sell_volume[index]))
                                else _float_hex(float(series.sell_volume[index]))
                            )
                        ),
                        "source_date": series.source_dates[index].isoformat(),
                        "stage_date_rank": int(series.stage_date_ranks[index]),
                        "trade_count_hex": _float_hex(float(series.trade_count[index])),
                        "volume_hex": _float_hex(float(series.volume[index])),
                    }
                )
            )
            digest.update(b"\n")
    return digest.hexdigest()


def _causal_feature_rows_artifact_sha256(rows: CausalFeatureRows) -> str:
    """Hash exact float bytes and complete lineage without a giant JSON allocation."""

    digest = hashlib.sha256()
    digest.update(
        canonical_json_bytes(
            {
                "entry_schedule_sha256": rows.entry_schedule_sha256,
                "expert_formula_sha256": rows.expert_formula_sha256,
                "feature_names": list(rows.feature_names),
                "feature_set_id": rows.feature_set_id,
                "schema": "systematic_fx.ai_all_cases_causal_feature_rows.v1",
                "source_commitment_sha256": rows.source_commitment_sha256,
                "stage_key": rows.stage_key,
                "task_timeframe_seconds": rows.decision_timeframe_seconds,
            }
        )
    )
    digest.update(b"\n")
    for index, row_id in enumerate(rows.row_ids):
        digest.update(
            canonical_json_bytes(
                {
                    "aligned_bar_indexes": [
                        int(value) for value in rows.aligned_bar_indexes[index]
                    ],
                    "atr_ticks_hex": [
                        _float_hex(float(value)) for value in rows.atr_ticks_by_timeframe[index]
                    ],
                    "contract": rows.contracts[index],
                    "decision_ns": int(rows.decision_ns[index]),
                    "entry_ns": int(rows.entry_ns[index]),
                    "expert_artifact_sha256": (
                        None
                        if rows.expert_artifact_sha256s is None
                        else rows.expert_artifact_sha256s[index]
                    ),
                    "outcome_span_id": int(rows.outcome_span_ids[index]),
                    "retained_input_index": int(rows.retained_input_indexes[index]),
                    "row_id": row_id,
                    "segment_id": int(rows.segment_ids[index]),
                    "source_date": rows.source_dates[index].isoformat(),
                    "stage_date_rank": int(rows.stage_date_ranks[index]),
                    "values_hex": [_float_hex(float(value)) for value in rows.values[index]],
                }
            )
        )
        digest.update(b"\n")
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class CausalFeatureRows:
    """Outcome-free feature rows aligned only to completed bars."""

    feature_set_id: str
    feature_names: tuple[str, ...]
    row_ids: tuple[str, ...]
    decision_ns: np.ndarray
    entry_ns: np.ndarray
    source_dates: tuple[date, ...]
    contracts: tuple[str, ...]
    outcome_span_ids: np.ndarray
    segment_ids: np.ndarray
    stage_date_ranks: np.ndarray
    stage_key: str
    decision_timeframe_seconds: int
    entry_schedule_sha256: str
    source_commitment_sha256: str
    retained_input_indexes: np.ndarray
    aligned_bar_indexes: np.ndarray
    values: np.ndarray
    atr_ticks_by_timeframe: np.ndarray
    expert_artifact_sha256s: tuple[str, ...] | None = None
    expert_formula_sha256: str | None = None
    artifact_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        expected = FEATURE_NAMES_BY_SET.get(self.feature_set_id)
        count = len(self.row_ids)
        if (
            expected is None
            or self.feature_names != expected
            or count < 2
            or any(not isinstance(value, str) or not value for value in self.row_ids)
            or len(set(self.row_ids)) != count
            or len(self.source_dates) != count
            or len(self.contracts) != count
            or any(
                isinstance(value, datetime) or not isinstance(value, date)
                for value in self.source_dates
            )
            or any(not isinstance(value, str) or not value for value in self.contracts)
            or not isinstance(self.stage_key, str)
            or not self.stage_key
            or isinstance(self.decision_timeframe_seconds, bool)
            or not isinstance(self.decision_timeframe_seconds, int)
            or self.decision_timeframe_seconds not in TF_ORDER
        ):
            raise AllCasesMLError("causal feature-row schema differs")
        object.__setattr__(self, "row_ids", tuple(self.row_ids))
        object.__setattr__(self, "feature_names", tuple(self.feature_names))
        object.__setattr__(self, "source_dates", tuple(self.source_dates))
        object.__setattr__(self, "contracts", tuple(self.contracts))
        if self.expert_artifact_sha256s is not None:
            object.__setattr__(self, "expert_artifact_sha256s", tuple(self.expert_artifact_sha256s))
        object.__setattr__(
            self,
            "decision_ns",
            _deep_exact_int64_array(
                self.decision_ns,
                (count,),
                "feature decision_ns",
            ),
        )
        object.__setattr__(
            self,
            "entry_ns",
            _deep_exact_int64_array(self.entry_ns, (count,), "feature entry_ns"),
        )
        object.__setattr__(
            self,
            "retained_input_indexes",
            _deep_exact_int64_array(
                self.retained_input_indexes,
                (count,),
                "retained_input_indexes",
            ),
        )
        object.__setattr__(
            self,
            "outcome_span_ids",
            _deep_exact_int64_array(
                self.outcome_span_ids,
                (count,),
                "feature outcome_span_ids",
            ),
        )
        object.__setattr__(
            self,
            "segment_ids",
            _deep_exact_int64_array(
                self.segment_ids,
                (count,),
                "feature segment_ids",
            ),
        )
        object.__setattr__(
            self,
            "stage_date_ranks",
            _deep_exact_int64_array(
                self.stage_date_ranks,
                (count,),
                "feature stage_date_ranks",
            ),
        )
        object.__setattr__(
            self,
            "aligned_bar_indexes",
            _deep_exact_int64_array(
                self.aligned_bar_indexes,
                (count, len(TF_ORDER)),
                "aligned_bar_indexes",
            ),
        )
        object.__setattr__(
            self,
            "values",
            _deep_numeric_float64_array(
                self.values,
                (count, len(self.feature_names)),
                "feature values",
            ),
        )
        object.__setattr__(
            self,
            "atr_ticks_by_timeframe",
            _deep_numeric_float64_array(
                self.atr_ticks_by_timeframe,
                (count, len(TF_ORDER)),
                "ATR tick values",
            ),
        )
        expert = self.feature_set_id == "FULL_MTF_PLUS_EXPERT_221"
        if expert:
            from .symbolic import EXPERT_FEATURE_FORMULA_SHA256

            if (
                self.expert_artifact_sha256s is None
                or len(self.expert_artifact_sha256s) != count
                or any(
                    len(value) != 64
                    or any(character not in "0123456789abcdef" for character in value)
                    for value in self.expert_artifact_sha256s
                )
                or self.expert_formula_sha256 != EXPERT_FEATURE_FORMULA_SHA256
            ):
                raise AllCasesMLError("causal expert artifact binding differs")
        elif self.expert_artifact_sha256s is not None or self.expert_formula_sha256 is not None:
            raise AllCasesMLError("non-expert feature rows cannot bind expert artifacts")
        if (
            np.any(np.diff(self.retained_input_indexes) <= 0)
            or np.isinf(self.values).any()
            or np.isnan(self.values).any()
            or not np.isfinite(self.atr_ticks_by_timeframe).all()
            or np.any(self.atr_ticks_by_timeframe <= 0)
            or np.any(self.outcome_span_ids <= 0)
            or np.any(self.segment_ids <= 0)
            or np.any(self.stage_date_ranks < 0)
            or np.any(self.entry_ns < self.decision_ns)
            or np.any(self.entry_ns % 1_000_000_000 != 0)
            or tuple(zip(self.decision_ns.tolist(), self.row_ids, strict=True))
            != tuple(sorted(zip(self.decision_ns.tolist(), self.row_ids, strict=True)))
            or not _is_sha256(self.entry_schedule_sha256)
            or not _is_sha256(self.source_commitment_sha256)
        ):
            raise AllCasesMLError("causal feature values or chronology differ")
        object.__setattr__(self, "artifact_sha256", _causal_feature_rows_artifact_sha256(self))

    @property
    def row_count(self) -> int:
        return len(self.row_ids)

    @property
    def expert_artifact_commitment_sha256(self) -> str | None:
        if self.expert_artifact_sha256s is None:
            return None
        return canonical_sha256(
            {
                "artifact_sha256s": list(self.expert_artifact_sha256s),
                "formula_sha256": self.expert_formula_sha256,
                "schema": "systematic_fx.ai_all_cases_ml_expert_adapter.v1",
            }
        )

    def for_feature_set(self, feature_set_id: str) -> CausalFeatureRows:
        desired = FEATURE_NAMES_BY_SET.get(feature_set_id)
        if desired is None:
            raise AllCasesMLError("unknown feature set")
        positions = {name: index for index, name in enumerate(self.feature_names)}
        if any(name not in positions for name in desired):
            raise AllCasesMLError("source feature rows do not contain requested feature set")
        indexes = [positions[name] for name in desired]
        return CausalFeatureRows(
            feature_set_id,
            desired,
            self.row_ids,
            self.decision_ns,
            self.entry_ns,
            self.source_dates,
            self.contracts,
            self.outcome_span_ids,
            self.segment_ids,
            self.stage_date_ranks,
            self.stage_key,
            self.decision_timeframe_seconds,
            self.entry_schedule_sha256,
            self.source_commitment_sha256,
            self.retained_input_indexes,
            self.aligned_bar_indexes,
            self.values[:, indexes],
            self.atr_ticks_by_timeframe,
            self.expert_artifact_sha256s if feature_set_id == "FULL_MTF_PLUS_EXPERT_221" else None,
            self.expert_formula_sha256 if feature_set_id == "FULL_MTF_PLUS_EXPERT_221" else None,
        )

    def take(self, indexes: Sequence[int]) -> CausalFeatureRows:
        selected_tuple = _normalized_indexes(indexes, self.row_count)
        selected = np.asarray(selected_tuple, dtype=np.int64)
        return CausalFeatureRows(
            self.feature_set_id,
            self.feature_names,
            tuple(self.row_ids[index] for index in selected_tuple),
            self.decision_ns[selected],
            self.entry_ns[selected],
            tuple(self.source_dates[index] for index in selected_tuple),
            tuple(self.contracts[index] for index in selected_tuple),
            self.outcome_span_ids[selected],
            self.segment_ids[selected],
            self.stage_date_ranks[selected],
            self.stage_key,
            self.decision_timeframe_seconds,
            self.entry_schedule_sha256,
            self.source_commitment_sha256,
            self.retained_input_indexes[selected],
            self.aligned_bar_indexes[selected],
            self.values[selected],
            self.atr_ticks_by_timeframe[selected],
            (
                None
                if self.expert_artifact_sha256s is None
                else tuple(self.expert_artifact_sha256s[index] for index in selected_tuple)
            ),
            self.expert_formula_sha256,
        )


def build_causal_feature_rows(
    *,
    anchors: CausalAnchorRows,
    bars_by_timeframe: Mapping[int, CausalBarSeries],
    feature_set_id: str = "FULL_MTF_213",
    expert_artifacts: Sequence[CausalExpertFeatureArtifact] | None = None,
) -> CausalFeatureRows:
    """Build the exact 213/221 causal columns without accepting any outcomes."""

    identifiers = anchors.row_ids
    decisions = anchors.decision_ns
    entries = anchors.entry_ns
    if (
        len(identifiers) < 2
        or len(set(identifiers)) != len(identifiers)
        or set(bars_by_timeframe) != set(TF_ORDER)
        or feature_set_id not in FEATURE_NAMES_BY_SET
    ):
        raise AllCasesMLError("causal feature request differs")
    source_commitment_sha256 = _causal_feature_source_commitment_sha256(
        anchors,
        bars_by_timeframe,
    )
    timeframe_columns: dict[int, dict[str, np.ndarray]] = {}
    atr_by_timeframe: dict[int, np.ndarray] = {}
    aligned_columns: list[np.ndarray] = []
    for timeframe in TF_ORDER:
        series = bars_by_timeframe[timeframe]
        if series.timeframe_seconds != timeframe or series.stage_key != anchors.stage_key:
            raise AllCasesMLError("bar mapping/timeframe binding differs")
        columns, atr = _timeframe_feature_columns(series)
        timeframe_columns[timeframe] = columns
        atr_by_timeframe[timeframe] = atr
        aligned_for_timeframe = np.full(len(identifiers), -1, dtype=np.int64)
        for anchor_index in range(len(identifiers)):
            key = (
                anchors.contracts[anchor_index],
                int(anchors.outcome_span_ids[anchor_index]),
            )
            group_indexes = series._indices_by_contract_span.get(key, ())
            group_ends = series._ends_by_contract_span.get(key, ())
            position = int(np.searchsorted(group_ends, int(decisions[anchor_index]), side="right"))
            for group_position in range(position - 1, -1, -1):
                bar_index = group_indexes[group_position]
                same_date = series.source_dates[bar_index] == anchors.source_dates[anchor_index]
                compatible = (
                    int(series.stage_date_ranks[bar_index])
                    == int(anchors.stage_date_ranks[anchor_index])
                    and int(series.segment_ids[bar_index]) == int(anchors.segment_ids[anchor_index])
                    if same_date
                    else int(anchors.stage_date_ranks[anchor_index])
                    == int(series.stage_date_ranks[bar_index]) + 1
                    and 0
                    <= int(decisions[anchor_index]) - int(series.bar_end_ns[bar_index])
                    <= 96 * 3_600 * 1_000_000_000
                )
                if compatible:
                    aligned_for_timeframe[anchor_index] = bar_index
                    break
        aligned_columns.append(aligned_for_timeframe)
    aligned = np.column_stack(aligned_columns).astype(np.int64)
    valid = np.all(aligned >= 0, axis=1)
    for column, timeframe in enumerate(TF_ORDER):
        indexes = np.maximum(aligned[:, column], 0)
        atr = atr_by_timeframe[timeframe][indexes]
        valid &= np.isfinite(atr) & (atr > 0)
        valid &= bars_by_timeframe[timeframe].bar_end_ns[indexes] <= decisions
        valid &= (
            bars_by_timeframe[timeframe]._run_length_by_index[indexes] >= MIN_CAUSAL_HISTORY_BARS
        )
        if timeframe == anchors.decision_timeframe_seconds:
            valid &= bars_by_timeframe[timeframe].bar_end_ns[indexes] == decisions
    retained = np.flatnonzero(valid)
    if len(retained) < 2:
        raise AllCasesMLError("fewer than two anchors have complete causal history")
    aligned_retained = aligned[retained]
    full_columns: dict[str, np.ndarray] = {}
    for timeframe_column, timeframe in enumerate(TF_ORDER):
        prefix = f"tf{timeframe:04d}_"
        bar_indexes = aligned_retained[:, timeframe_column]
        for suffix, values in timeframe_columns[timeframe].items():
            full_columns[prefix + suffix] = values[bar_indexes]

    retained_decisions = decisions[retained]
    retained_entries = entries[retained]
    nanoseconds_per_day = 86_400_000_000_000
    seconds = (retained_decisions % nanoseconds_per_day).astype(np.float64) / 1_000_000_000
    angle = 2.0 * np.pi * seconds / 86_400.0
    full_columns["utc_sin"] = np.sin(angle)
    full_columns["utc_cos"] = np.cos(angle)
    four_hour_slot = (seconds // 14_400).astype(np.int8)
    for index, hour in enumerate((0, 4, 8, 12, 16, 20)):
        full_columns[f"utc_4h_{hour:02d}"] = (four_hour_slot == index).astype(np.float64)
    epoch_days = retained_decisions // nanoseconds_per_day
    weekdays = (epoch_days + 3) % 7
    for weekday in range(7):
        full_columns[f"utc_weekday_{weekday}"] = (weekdays == weekday).astype(np.float64)

    sampled_suffixes = {
        timeframe: {
            suffix: values[aligned_retained[:, timeframe_column]]
            for suffix, values in timeframe_columns[timeframe].items()
        }
        for timeframe_column, timeframe in enumerate(TF_ORDER)
    }
    for metric in _CROSS_METRICS:
        source_suffix = {
            "return_sign_agreement": "ret_atr_1",
            "ema_8_21_agreement": "ema_8_21_distance_atr",
            "volatility_regime_agreement": "atr_ratio_6_24",
        }[metric]
        for pair in _CROSS_PAIRS:
            left, right = (int(value[2:]) for value in pair.split("_"))
            left_values = sampled_suffixes[left][source_suffix]
            right_values = sampled_suffixes[right][source_suffix]
            if metric == "volatility_regime_agreement":
                left_values = left_values - 1.0
                right_values = right_values - 1.0
            full_columns[f"cross_{metric}_{pair}"] = np.sign(left_values) * np.sign(right_values)
    full_values = np.column_stack([full_columns[name] for name in FULL_MTF_213])
    selected_expert_sha256s: tuple[str, ...] | None = None
    expert_formula_sha256: str | None = None
    if feature_set_id == "FULL_MTF_PLUS_EXPERT_221":
        from .symbolic import (
            EXPERT_FEATURE_FORMULA_SHA256,
            CausalExpertFeatureArtifact,
        )
        from .symbolic import (
            EXPERT_FEATURE_NAMES as SYMBOLIC_EXPERT_FEATURE_NAMES,
        )

        artifacts = () if expert_artifacts is None else tuple(expert_artifacts)
        if (
            len(artifacts) != len(identifiers)
            or any(not isinstance(item, CausalExpertFeatureArtifact) for item in artifacts)
            or tuple(SYMBOLIC_EXPERT_FEATURE_NAMES) != EXPERT_FEATURE_NAMES
            or tuple(item.order_id for item in artifacts) != identifiers
            or len(
                {
                    (
                        item.anchor_policy_id,
                        item.base_candidate_id,
                        item.context_id,
                        item.exit_policy_id,
                        item.formula_sha256,
                    )
                    for item in artifacts
                }
            )
            != 1
            or any(
                item.formula_sha256 != EXPERT_FEATURE_FORMULA_SHA256
                or item.anchor_key[:4]
                != (
                    anchors.contracts[index],
                    int(anchors.outcome_span_ids[index]),
                    int(anchors.segment_ids[index]),
                    int(anchors.decision_ns[index]),
                )
                for index, item in enumerate(artifacts)
            )
        ):
            raise AllCasesMLError("expert feature set requires typed causal symbolic artifacts")
        experts = np.asarray(
            [[float(value.fraction) for value in artifact.values] for artifact in artifacts],
            dtype=np.float64,
        )
        if (
            experts.shape != (len(identifiers), len(EXPERT_FEATURE_NAMES))
            or not np.isfinite(experts).all()
        ):
            raise AllCasesMLError("causal expert rational conversion differs")
        selected_expert_sha256s = tuple(artifacts[index].artifact_sha256 for index in retained)
        expert_formula_sha256 = EXPERT_FEATURE_FORMULA_SHA256
        source_values = np.column_stack((full_values, experts[retained]))
        source_names = FULL_MTF_PLUS_EXPERT_221
    else:
        if expert_artifacts is not None:
            raise AllCasesMLError("non-expert feature set cannot accept expert artifacts")
        source_values = full_values
        source_names = FULL_MTF_213
    positions = {name: index for index, name in enumerate(source_names)}
    desired_names = FEATURE_NAMES_BY_SET[feature_set_id]
    desired_values = source_values[:, [positions[name] for name in desired_names]]
    atr_values = np.column_stack(
        [
            atr_by_timeframe[timeframe][aligned_retained[:, timeframe_column]]
            for timeframe_column, timeframe in enumerate(TF_ORDER)
        ]
    )
    return CausalFeatureRows(
        feature_set_id,
        desired_names,
        tuple(identifiers[index] for index in retained),
        retained_decisions,
        retained_entries,
        tuple(anchors.source_dates[index] for index in retained),
        tuple(anchors.contracts[index] for index in retained),
        anchors.outcome_span_ids[retained],
        anchors.segment_ids[retained],
        anchors.stage_date_ranks[retained],
        anchors.stage_key,
        anchors.decision_timeframe_seconds,
        anchors.entry_schedule_sha256,
        source_commitment_sha256,
        retained,
        aligned_retained,
        desired_values,
        atr_values,
        selected_expert_sha256s,
        expert_formula_sha256,
    )


def build_match_strata(feature_rows: CausalFeatureRows) -> tuple[str, ...]:
    """Build fixed, outcome-free target-control strata from causal row state."""

    timeframe_column = TF_ORDER.index(feature_rows.decision_timeframe_seconds)
    nanoseconds_per_day = 86_400_000_000_000
    output: list[str] = []
    for index in range(feature_rows.row_count):
        decision_ns = int(feature_rows.decision_ns[index])
        utc_hour = (decision_ns % nanoseconds_per_day) // 3_600_000_000_000
        utc_bucket = 4 * (utc_hour // 4)
        atr_ticks = float(feature_rows.atr_ticks_by_timeframe[index, timeframe_column])
        if not math.isfinite(atr_ticks) or atr_ticks <= 0:  # pragma: no cover - row invariant
            raise AllCasesMLError("match-stratum ATR differs")
        atr_power_of_two_bin = math.frexp(atr_ticks)[1] - 1
        utc_weekday = (decision_ns // nanoseconds_per_day + 3) % 7
        coarse = f"tf{feature_rows.decision_timeframe_seconds:04d}_utc4h_{utc_bucket:02d}"
        output.append(f"{coarse}|weekday_{utc_weekday}|atr20_log2_{atr_power_of_two_bin:+04d}")
    return tuple(output)


@dataclass(slots=True)
class TrainingMatrix:
    """One chronologically ordered, lineage-bound model matrix."""

    feature_set_id: str
    feature_names: tuple[str, ...]
    row_ids: tuple[str, ...]
    decision_dates: tuple[date, ...]
    decision_ns: np.ndarray
    entry_ns: np.ndarray
    label_exit_ns: np.ndarray
    values: np.ndarray
    targets: np.ndarray
    atr_ticks: np.ndarray
    contracts: tuple[str, ...]
    outcome_span_ids: np.ndarray
    segment_ids: np.ndarray
    outcome_lineage_sha256: str
    opportunity_lattice_sha256: str
    entry_schedule_sha256: str
    match_strata: tuple[str, ...]
    base_directions: np.ndarray | None = None
    terminal_move_ticks: np.ndarray | None = None
    realized_net_ticks: np.ndarray | None = None
    task_timeframe_seconds: int | None = None
    task_horizon_seconds: int | None = None
    base_strategy_id: str | None = None
    base_trigger_family: str | None = None
    symbolic_ranking_certificate: SymbolicRankingCertificate | None = None
    expert_artifact_sha256s: tuple[str, ...] | None = None
    expert_formula_sha256: str | None = None

    def __post_init__(self) -> None:
        expected = FEATURE_NAMES_BY_SET.get(self.feature_set_id)
        if expected is None or self.feature_names != expected:
            raise AllCasesMLError("training matrix feature schema differs")
        count = len(self.row_ids)
        if count < 2 or count > MAX_TRAINING_ROWS_PER_MODEL or len(set(self.row_ids)) != count:
            raise AllCasesMLError("training matrix row count or identities differ")
        if (
            len(self.decision_dates) != count
            or len(self.contracts) != count
            or len(self.match_strata) != count
            or any(
                isinstance(item, datetime) or not isinstance(item, date)
                for item in self.decision_dates
            )
            or any(not isinstance(item, str) or not item for item in self.contracts)
            or any(not isinstance(item, str) or not item for item in self.match_strata)
        ):
            raise AllCasesMLError("training matrix row metadata differs")
        self.decision_ns = _deep_exact_int64_array(
            self.decision_ns,
            (count,),
            "decision_ns",
        )
        self.entry_ns = _deep_exact_int64_array(self.entry_ns, (count,), "entry_ns")
        self.label_exit_ns = _deep_exact_int64_array(self.label_exit_ns, (count,), "label_exit_ns")
        self.values = _deep_numeric_float64_array(
            self.values, (count, len(self.feature_names)), "values"
        )
        self.targets = _deep_numeric_float64_array(self.targets, (count,), "targets")
        self.atr_ticks = _deep_numeric_float64_array(self.atr_ticks, (count,), "atr_ticks")
        self.outcome_span_ids = _deep_exact_int64_array(
            self.outcome_span_ids, (count,), "outcome_span_ids"
        )
        self.segment_ids = _deep_exact_int64_array(self.segment_ids, (count,), "segment_ids")
        if (
            np.any(self.outcome_span_ids <= 0)
            or np.any(self.segment_ids <= 0)
            or len(self.outcome_lineage_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.outcome_lineage_sha256)
            or len(self.opportunity_lattice_sha256) != 64
            or any(
                character not in "0123456789abcdef" for character in self.opportunity_lattice_sha256
            )
            or len(self.entry_schedule_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.entry_schedule_sha256)
        ):
            raise AllCasesMLError("training outcome lineage differs")
        if self.base_directions is not None:
            exact_directions = _exact_int64_array(
                self.base_directions,
                (count,),
                "base_directions",
            )
            if any(int(value) not in {-1, 1} for value in exact_directions):
                raise AllCasesMLError("base directions must be signed one or minus one")
            self.base_directions = _readonly_array(
                exact_directions,
                np.int8,
                (count,),
                "base_directions",
            )
        if self.terminal_move_ticks is not None:
            self.terminal_move_ticks = _exact_int64_array(
                self.terminal_move_ticks,
                (count,),
                "terminal_move_ticks",
            )
        if self.realized_net_ticks is not None:
            self.realized_net_ticks = _exact_int64_array(
                self.realized_net_ticks,
                (count,),
                "realized_net_ticks",
            )
        expert_columns = self.feature_set_id == "FULL_MTF_PLUS_EXPERT_221"
        meta_rows = self.base_directions is not None
        if expert_columns or meta_rows:
            from .symbolic import EXPERT_FEATURE_FORMULA_SHA256

            if (
                self.expert_artifact_sha256s is None
                or len(self.expert_artifact_sha256s) != count
                or any(
                    len(value) != 64
                    or any(character not in "0123456789abcdef" for character in value)
                    for value in self.expert_artifact_sha256s
                )
                or self.expert_formula_sha256 != EXPERT_FEATURE_FORMULA_SHA256
            ):
                raise AllCasesMLError("training matrix expert artifact binding differs")
        elif self.expert_artifact_sha256s is not None or self.expert_formula_sha256 is not None:
            raise AllCasesMLError("direct non-expert training matrix bound expert artifacts")
        for value, label in (
            (self.task_timeframe_seconds, "task_timeframe_seconds"),
            (self.task_horizon_seconds, "task_horizon_seconds"),
        ):
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value <= 0
            ):
                raise AllCasesMLError(f"{label} must be a positive integer when present")
        symbolic_values = (
            self.base_strategy_id,
            self.base_trigger_family,
            self.symbolic_ranking_certificate,
        )
        if any(value is None for value in symbolic_values) != all(
            value is None for value in symbolic_values
        ):
            raise AllCasesMLError("symbolic strategy/ranking/world binding is partial")
        for value in (self.base_strategy_id, self.symbolic_ranking_sha256):
            if value is not None and (
                len(value) != 64 or any(character not in "0123456789abcdef" for character in value)
            ):
                raise AllCasesMLError("symbolic strategy/ranking identity differs")
        if self.base_trigger_family is not None and (
            not isinstance(self.base_trigger_family, str) or not self.base_trigger_family
        ):
            raise AllCasesMLError("symbolic base trigger family differs")
        if self.base_directions is None:
            if (
                self.terminal_move_ticks is None
                or self.realized_net_ticks is not None
                or any(value is not None for value in symbolic_values)
            ):
                raise AllCasesMLError("direct matrix exact terminal/meta state differs")
        elif (
            self.terminal_move_ticks is not None
            or self.realized_net_ticks is None
            or any(value is None for value in symbolic_values)
        ):
            raise AllCasesMLError("meta matrix requires realized economics and ranking world")
        if np.isinf(self.values).any() or not np.isfinite(self.targets).all():
            raise AllCasesMLError(
                "features may contain NaN but not infinity; targets must be finite"
            )
        if not np.isfinite(self.atr_ticks).all() or np.any(self.atr_ticks <= 0):
            raise AllCasesMLError("ATR values must be finite and positive")
        if np.any(self.entry_ns < self.decision_ns) or np.any(self.label_exit_ns <= self.entry_ns):
            raise AllCasesMLError("every label must mature strictly after entry")
        keys = tuple(zip(self.entry_ns.tolist(), self.row_ids, strict=True))
        if keys != tuple(sorted(keys)):
            raise AllCasesMLError("training rows must be chronologically ordered")

    @property
    def row_count(self) -> int:
        return len(self.row_ids)

    def take(self, indexes: Sequence[int], *, targets: np.ndarray | None = None) -> TrainingMatrix:
        selected = np.asarray(indexes, dtype=np.int64)
        if (
            selected.ndim != 1
            or not len(selected)
            or np.any(selected < 0)
            or np.any(selected >= self.row_count)
        ):
            raise AllCasesMLError("matrix subset indexes are invalid")
        chosen_targets = (
            self.targets[selected] if targets is None else np.asarray(targets, dtype=np.float64)
        )
        return TrainingMatrix(
            self.feature_set_id,
            self.feature_names,
            tuple(self.row_ids[index] for index in selected),
            tuple(self.decision_dates[index] for index in selected),
            self.decision_ns[selected],
            self.entry_ns[selected],
            self.label_exit_ns[selected],
            self.values[selected],
            chosen_targets,
            self.atr_ticks[selected],
            tuple(self.contracts[index] for index in selected),
            self.outcome_span_ids[selected],
            self.segment_ids[selected],
            self.outcome_lineage_sha256,
            self.opportunity_lattice_sha256,
            self.entry_schedule_sha256,
            tuple(self.match_strata[index] for index in selected),
            None if self.base_directions is None else self.base_directions[selected],
            None if self.terminal_move_ticks is None else self.terminal_move_ticks[selected],
            None if self.realized_net_ticks is None else self.realized_net_ticks[selected],
            self.task_timeframe_seconds,
            self.task_horizon_seconds,
            self.base_strategy_id,
            self.base_trigger_family,
            self.symbolic_ranking_certificate,
            (
                None
                if self.expert_artifact_sha256s is None
                else tuple(self.expert_artifact_sha256s[index] for index in selected)
            ),
            self.expert_formula_sha256,
        )

    @property
    def symbolic_ranking_sha256(self) -> str | None:
        certificate = self.symbolic_ranking_certificate
        return None if certificate is None else certificate.artifact_sha256

    @property
    def symbolic_ranking_world(self) -> str | None:
        certificate = self.symbolic_ranking_certificate
        return None if certificate is None else certificate.null_world


def _readonly_array(
    value: object, dtype: np.dtype, shape: tuple[int, ...], label: str
) -> np.ndarray:
    result = np.array(value, dtype=dtype, copy=True)
    if result.shape != shape:
        raise AllCasesMLError(f"{label} shape differs")
    result.setflags(write=False)
    return result


def _deep_readonly_array(
    value: object, dtype: np.dtype, shape: tuple[int, ...], label: str
) -> np.ndarray:
    """Copy into immutable bytes so callers cannot re-enable NumPy writes."""

    copied = np.array(value, dtype=dtype, copy=True, order="C")
    if copied.shape != shape:
        raise AllCasesMLError(f"{label} shape differs")
    result = np.frombuffer(copied.tobytes(order="C"), dtype=copied.dtype).reshape(shape)
    result.setflags(write=False)
    return result


def _deep_numeric_float64_array(
    value: object,
    shape: tuple[int, ...],
    label: str,
) -> np.ndarray:
    """Deep-freeze a raw real numeric array without bool/string coercion."""

    source = np.asarray(value)
    if source.shape != shape or source.dtype.kind not in {"f", "i", "u"}:
        raise AllCasesMLError(f"{label} must be a raw numeric float/integer array")
    return _deep_readonly_array(source, np.float64, shape, label)


def _deep_exact_int64_array(value: object, shape: tuple[int, ...], label: str) -> np.ndarray:
    exact = _exact_int64_array(value, shape, label)
    result = np.frombuffer(exact.tobytes(order="C"), dtype=np.int64).reshape(shape)
    result.setflags(write=False)
    return result


def _exact_int64_array(value: object, shape: tuple[int, ...], label: str) -> np.ndarray:
    source = np.asarray(value)
    if source.shape != shape or source.dtype.kind not in {"i", "u"}:
        raise AllCasesMLError(f"{label} must be an exact integral tick array")
    if source.dtype.kind == "u" and source.size and int(source.max()) > np.iinfo(np.int64).max:
        raise AllCasesMLError(f"{label} overflows int64")
    result = np.array(source, dtype=np.int64, copy=True)
    if any(int(left) != int(right) for left, right in zip(source.flat, result.flat, strict=True)):
        raise AllCasesMLError(f"{label} overflows int64")
    result.setflags(write=False)
    return result


def _is_exact_int64_scalar(value: object) -> bool:
    return (
        not isinstance(value, (bool, np.bool_))
        and isinstance(value, (int, np.integer))
        and np.iinfo(np.int64).min <= int(value) <= np.iinfo(np.int64).max
    )


def _exact_int64_tuple(values: Sequence[object], *, label: str) -> tuple[int, ...]:
    raw = tuple(values)
    if any(not _is_exact_int64_scalar(value) for value in raw):
        raise AllCasesMLError(f"{label} must contain exact int64 scalars")
    return tuple(int(value) for value in raw)


def training_rows_sha256(matrix: TrainingMatrix) -> str:
    digest = hashlib.sha256()
    digest.update(matrix.feature_set_id.encode("ascii") + b"\0")
    digest.update(canonical_json_bytes(list(matrix.feature_names)))
    digest.update((matrix.base_strategy_id or "DIRECT").encode("ascii") + b"\0")
    digest.update((matrix.base_trigger_family or "DIRECT").encode("ascii") + b"\0")
    digest.update((matrix.symbolic_ranking_sha256 or "DIRECT").encode("ascii") + b"\0")
    digest.update((matrix.symbolic_ranking_world or "DIRECT").encode("ascii") + b"\0")
    digest.update((matrix.expert_formula_sha256 or "NO_EXPERT").encode("ascii") + b"\0")
    digest.update(matrix.outcome_lineage_sha256.encode("ascii") + b"\0")
    digest.update(matrix.opportunity_lattice_sha256.encode("ascii") + b"\0")
    digest.update(matrix.entry_schedule_sha256.encode("ascii") + b"\0")
    for index, row_id in enumerate(matrix.row_ids):
        digest.update(row_id.encode("utf-8") + b"\0")
        digest.update(matrix.decision_dates[index].isoformat().encode("ascii") + b"\0")
        for value in (
            int(matrix.decision_ns[index]),
            int(matrix.entry_ns[index]),
            int(matrix.label_exit_ns[index]),
            int(matrix.outcome_span_ids[index]),
            int(matrix.segment_ids[index]),
        ):
            digest.update(str(value).encode("ascii") + b"\0")
        digest.update(matrix.contracts[index].encode("ascii") + b"\0")
        digest.update(matrix.match_strata[index].encode("utf-8") + b"\0")
        if matrix.expert_artifact_sha256s is not None:
            digest.update(matrix.expert_artifact_sha256s[index].encode("ascii") + b"\0")
        digest.update(_float_hex(float(matrix.targets[index])).encode("ascii") + b"\0")
        digest.update(_float_hex(float(matrix.atr_ticks[index])).encode("ascii") + b"\0")
        for value in matrix.values[index]:
            encoded = "nan" if math.isnan(float(value)) else _float_hex(float(value))
            digest.update(encoded.encode("ascii") + b"\0")
        if matrix.base_directions is not None:
            digest.update(str(int(matrix.base_directions[index])).encode("ascii") + b"\0")
        if matrix.terminal_move_ticks is not None:
            digest.update(str(int(matrix.terminal_move_ticks[index])).encode("ascii") + b"\0")
        if matrix.realized_net_ticks is not None:
            digest.update(str(int(matrix.realized_net_ticks[index])).encode("ascii") + b"\0")
    return digest.hexdigest()


def outcome_values_sha256(matrix: TrainingMatrix) -> str:
    """Commit exact row-keyed economics independently of fitted feature values."""

    direct = matrix.terminal_move_ticks is not None
    meta = matrix.realized_net_ticks is not None
    if direct == meta:
        raise AllCasesMLError("matrix exact outcome kind differs")
    values = matrix.terminal_move_ticks if direct else matrix.realized_net_ticks
    if values is None:  # pragma: no cover - guarded above
        raise AllCasesMLError("matrix exact outcome values are missing")
    digest = hashlib.sha256()
    digest.update(b"systematic_fx.ai_all_cases_ml_outcome_values.v1\0")
    digest.update(("DIRECT_TERMINAL_MOVE_TICKS" if direct else "META_REALIZED_NET_TICKS").encode())
    digest.update(b"\0")
    digest.update(matrix.outcome_lineage_sha256.encode("ascii") + b"\0")
    digest.update(matrix.opportunity_lattice_sha256.encode("ascii") + b"\0")
    digest.update(matrix.entry_schedule_sha256.encode("ascii") + b"\0")
    for index, row_id in enumerate(matrix.row_ids):
        digest.update(row_id.encode("utf-8") + b"\0")
        digest.update(str(int(matrix.entry_ns[index])).encode("ascii") + b"\0")
        digest.update(str(int(matrix.label_exit_ns[index])).encode("ascii") + b"\0")
        digest.update(str(int(values[index])).encode("ascii") + b"\0")
        if matrix.base_directions is not None:
            digest.update(str(int(matrix.base_directions[index])).encode("ascii") + b"\0")
    return digest.hexdigest()


def _normalized_direct_terminal_targets(
    terminal_move_ticks: np.ndarray,
    atr_ticks: np.ndarray,
) -> np.ndarray:
    """Normalize exact signed terminal tick moves by causal ATR ticks for fitting."""

    source = np.asarray(terminal_move_ticks)
    if source.ndim != 1 or not len(source):
        raise AllCasesMLError("direct terminal tick moves differ")
    moves = _exact_int64_array(source, source.shape, "terminal_move_ticks")
    atr = _deep_numeric_float64_array(atr_ticks, moves.shape, "atr_ticks")
    if atr.shape != moves.shape or not np.isfinite(atr).all() or np.any(atr <= 0):
        raise AllCasesMLError("direct terminal-target inputs differ")
    result = np.asarray(moves, dtype=np.float64) / atr
    result = np.array(result, dtype=np.float64, copy=True)
    result.setflags(write=False)
    return result


def build_meta_profitability_targets(fully_loaded_net_ticks: np.ndarray) -> np.ndarray:
    """Label a complete base trade one iff its fully-loaded net ticks are positive."""

    source = np.asarray(fully_loaded_net_ticks)
    if source.ndim != 1 or not len(source):
        raise AllCasesMLError("meta net-outcome values differ")
    net = _exact_int64_array(source, source.shape, "fully_loaded_net_ticks")
    result = (net > 0).astype(np.float64)
    result.setflags(write=False)
    return result


def build_direct_training_matrix(
    candidate: DirectCandidate,
    feature_rows: CausalFeatureRows,
    *,
    fill_ns: np.ndarray,
    label_exit_ns: np.ndarray,
    entry_ticks: np.ndarray,
    terminal_ticks: np.ndarray,
    outcome_contracts: Sequence[str],
    outcome_span_ids: np.ndarray,
    segment_ids: np.ndarray,
    valid_label_paths: np.ndarray,
    outcome_lineage_sha256: str,
    opportunity_lattice_sha256: str,
) -> TrainingMatrix:
    """Bind verified full-horizon 1s paths to one direct candidate."""

    if DIRECT_CANDIDATE_BY_ID.get(candidate.candidate_id) != candidate:
        raise AllCasesMLError("direct assembly candidate is not frozen")
    if feature_rows.decision_timeframe_seconds != candidate.decision_timeframe_seconds:
        raise AllCasesMLError("direct native decision timeframe differs")
    rows = feature_rows.for_feature_set(candidate.feature_set_id)
    source_count = rows.row_count
    fills = _exact_int64_array(fill_ns, (source_count,), "fill_ns")
    exits = _exact_int64_array(label_exit_ns, (source_count,), "label_exit_ns")
    entry = _exact_int64_array(entry_ticks, (source_count,), "entry_ticks")
    terminal = _exact_int64_array(terminal_ticks, (source_count,), "terminal_ticks")
    contract_values = tuple(outcome_contracts)
    spans = _exact_int64_array(outcome_span_ids, (source_count,), "outcome_span_ids")
    segments = _exact_int64_array(segment_ids, (source_count,), "segment_ids")
    valid_paths = np.asarray(valid_label_paths)
    strata = build_match_strata(rows)
    if (
        len(contract_values) != source_count
        or valid_paths.shape != (source_count,)
        or valid_paths.dtype != np.bool_
        or len(outcome_lineage_sha256) != 64
        or any(character not in "0123456789abcdef" for character in outcome_lineage_sha256)
        or len(opportunity_lattice_sha256) != 64
        or any(character not in "0123456789abcdef" for character in opportunity_lattice_sha256)
    ):
        raise AllCasesMLError("direct training metadata differs")
    if not bool(np.all(valid_paths)):
        raise AllCasesMLError("frozen direct opportunity lattice has a missing outcome path")
    if np.any(entry <= 0) or np.any(terminal <= 0):
        raise AllCasesMLError("direct entry/terminal ticks must be positive")
    move_values = [int(right) - int(left) for left, right in zip(entry, terminal, strict=True)]
    if any(
        value < np.iinfo(np.int64).min or value > np.iinfo(np.int64).max for value in move_values
    ):
        raise AllCasesMLError("direct terminal tick move overflows int64")
    terminal_move_ticks = np.asarray(move_values, dtype=np.int64)
    eligible = np.arange(source_count, dtype=np.int64)
    if (
        any(contract_values[index] != rows.contracts[index] for index in eligible)
        or np.any(spans[eligible] != rows.outcome_span_ids[eligible])
        or np.any(segments[eligible] != rows.segment_ids[eligible])
        or np.any(fills[eligible] != rows.entry_ns[eligible])
        or np.any(fills[eligible] < rows.decision_ns[eligible])
        or np.any(fills[eligible] >= rows.decision_ns[eligible] + 300 * 1_000_000_000)
        or np.any(exits[eligible] != fills[eligible] + candidate.horizon_seconds * 1_000_000_000)
    ):
        raise AllCasesMLError("direct path is shortened, late-filled, or cross-lineage")
    selected_rows = rows.take(tuple(int(index) for index in eligible))
    timeframe_column = TF_ORDER.index(candidate.decision_timeframe_seconds)
    atr_ticks = selected_rows.atr_ticks_by_timeframe[:, timeframe_column]
    targets = _normalized_direct_terminal_targets(terminal_move_ticks[eligible], atr_ticks)
    return TrainingMatrix(
        feature_set_id=selected_rows.feature_set_id,
        feature_names=selected_rows.feature_names,
        row_ids=selected_rows.row_ids,
        decision_dates=selected_rows.source_dates,
        decision_ns=selected_rows.decision_ns,
        entry_ns=fills[eligible],
        label_exit_ns=exits[eligible],
        values=selected_rows.values,
        targets=targets,
        atr_ticks=atr_ticks,
        contracts=tuple(contract_values[index] for index in eligible),
        outcome_span_ids=spans[eligible],
        segment_ids=segments[eligible],
        outcome_lineage_sha256=outcome_lineage_sha256,
        opportunity_lattice_sha256=opportunity_lattice_sha256,
        entry_schedule_sha256=selected_rows.entry_schedule_sha256,
        match_strata=tuple(strata[index] for index in eligible),
        terminal_move_ticks=terminal_move_ticks[eligible],
        task_timeframe_seconds=candidate.decision_timeframe_seconds,
        task_horizon_seconds=candidate.horizon_seconds,
    )


def _require_meta_recipe_inputs(
    candidate: MetaCandidate,
    feature_rows: CausalFeatureRows,
    *,
    symbolic_ranking_certificate: SymbolicRankingCertificate,
    strategy_recipe: CompleteStrategyRecipe,
    base_order_batch: EntryOrderBatch,
    expert_artifacts: Sequence[CausalExpertFeatureArtifact],
    scope_key: str,
) -> tuple[RankedSymbolicStrategy, tuple[CausalExpertFeatureArtifact, ...]]:
    """Bind a rank slot to one typed, feature-only symbolic recipe and its orders."""

    from .symbolic import (
        EXPERT_FEATURE_FORMULA_SHA256,
        CausalExpertFeatureArtifact,
        CompleteStrategyRecipe,
        EntryOrderBatch,
        FrozenEntryOrder,
        build_causal_expert_feature_artifact,
        build_exit_catalog,
    )

    if (
        META_CANDIDATE_BY_ID.get(candidate.candidate_id) != candidate
        or not isinstance(strategy_recipe, CompleteStrategyRecipe)
        or not isinstance(base_order_batch, EntryOrderBatch)
    ):
        raise AllCasesMLError("meta recipe inputs are not frozen typed artifacts")
    ranked = _require_ranked_strategy(
        symbolic_ranking_certificate,
        candidate,
        scope_key=scope_key,
    )
    recipe_binding = (
        strategy_recipe.strategy_id,
        strategy_recipe.anchor_policy_id,
        strategy_recipe.entry_policy_id,
        strategy_recipe.exit_policy_id,
    )
    ranked_binding = (
        ranked.strategy_id,
        ranked.anchor_policy_id,
        ranked.entry_policy_id,
        ranked.exit_policy_id,
    )
    orders = base_order_batch.orders
    artifacts = tuple(expert_artifacts)
    base_candidate, context, anchor_policy = _resolve_anchor_policy_components(
        ranked.base_candidate_id,
        ranked.context_id,
        ranked.time_filter_id,
        ranked.delay_id,
    )
    exit_policy = next(
        (item for item in build_exit_catalog().candidates if item.exit_id == ranked.exit_policy_id),
        None,
    )
    if (
        recipe_binding != ranked_binding
        or base_order_batch.anchor_policy_id != ranked.anchor_policy_id
        or len(orders) != feature_rows.row_count
        or tuple(order.order_id for order in orders) != feature_rows.row_ids
        or any(
            not isinstance(order, FrozenEntryOrder)
            or order.entry_policy_id != ranked.entry_policy_id
            for order in orders
        )
        or len(artifacts) != feature_rows.row_count
        or any(not isinstance(item, CausalExpertFeatureArtifact) for item in artifacts)
        or tuple(item.order_id for item in artifacts) != feature_rows.row_ids
        or exit_policy is None
        or any(
            item.anchor_policy_id != ranked.anchor_policy_id
            or item.base_candidate_id != ranked.base_candidate_id
            or item.context_id != ranked.context_id
            or item.exit_policy_id != ranked.exit_policy_id
            or item.formula_sha256 != EXPERT_FEATURE_FORMULA_SHA256
            or item.anchor_key != order.anchor.outcome_key
            for item, order in zip(artifacts, orders, strict=True)
        )
        or feature_rows.expert_artifact_sha256s is not None
        and feature_rows.expert_artifact_sha256s
        != tuple(item.artifact_sha256 for item in artifacts)
        or feature_rows.expert_formula_sha256 is not None
        and feature_rows.expert_formula_sha256 != EXPERT_FEATURE_FORMULA_SHA256
    ):
        raise AllCasesMLError("meta rank slot, recipe, orders, or Expert-8 binding differs")
    rebuilt_artifacts = tuple(
        build_causal_expert_feature_artifact(
            base_candidate,
            context,
            anchor_policy,
            order.anchor,
            order,
            exit_policy,
        )
        for order in orders
    )
    if rebuilt_artifacts != artifacts:
        raise AllCasesMLError("meta Expert-8 artifacts do not rebuild from certified recipe")
    if feature_rows.feature_set_id == "FULL_MTF_PLUS_EXPERT_221":
        positions = {name: index for index, name in enumerate(feature_rows.feature_names)}
        if any(
            tuple(
                _float_hex(float(feature_rows.values[row_index, positions[name]]))
                for name in EXPERT_FEATURE_NAMES
            )
            != tuple(_float_hex(float(value.fraction)) for value in artifact.values)
            for row_index, artifact in enumerate(rebuilt_artifacts)
        ):
            raise AllCasesMLError("meta Expert-8 feature columns differ from exact rationals")
    return ranked, artifacts


def build_meta_training_matrix(
    candidate: MetaCandidate,
    feature_rows: CausalFeatureRows,
    *,
    base_row_indexes: Sequence[int],
    base_entry_ns: np.ndarray,
    fully_loaded_net_ticks: np.ndarray,
    base_directions: np.ndarray,
    atr_ticks: np.ndarray,
    label_exit_ns: np.ndarray,
    outcome_contracts: Sequence[str],
    outcome_span_ids: np.ndarray,
    segment_ids: np.ndarray,
    valid_label_paths: np.ndarray,
    outcome_lineage_sha256: str,
    opportunity_lattice_sha256: str,
    symbolic_ranking_certificate: SymbolicRankingCertificate,
    strategy_recipe: CompleteStrategyRecipe,
    base_order_batch: EntryOrderBatch,
    expert_artifacts: Sequence[CausalExpertFeatureArtifact],
) -> TrainingMatrix:
    """Build one rank-slot gate matrix after prior-prefix symbolic reranking."""

    rows = feature_rows.for_feature_set(candidate.feature_set_id)
    ranked_strategy, typed_experts = _require_meta_recipe_inputs(
        candidate,
        rows,
        symbolic_ranking_certificate=symbolic_ranking_certificate,
        strategy_recipe=strategy_recipe,
        base_order_batch=base_order_batch,
        expert_artifacts=expert_artifacts,
        scope_key=symbolic_ranking_certificate.fold_key,
    )
    source_count = rows.row_count
    requested_indexes = _normalized_indexes(base_row_indexes, source_count)
    valid_paths = np.asarray(valid_label_paths)
    if valid_paths.shape != (source_count,) or valid_paths.dtype != np.bool_:
        raise AllCasesMLError("meta valid-path mask differs")
    if any(not bool(valid_paths[index]) for index in requested_indexes):
        raise AllCasesMLError("frozen meta base-trade lattice has a missing outcome path")
    indexes = requested_indexes
    if len(indexes) < 2:
        raise AllCasesMLError("meta candidate has fewer than two verified base paths")
    selected = np.asarray(indexes, dtype=np.int64)
    entries = _exact_int64_array(base_entry_ns, (source_count,), "base_entry_ns")
    contract_values = tuple(outcome_contracts)
    spans = _exact_int64_array(outcome_span_ids, (source_count,), "outcome_span_ids")
    segments = _exact_int64_array(segment_ids, (source_count,), "segment_ids")
    strata = build_match_strata(rows)
    net = _exact_int64_array(
        fully_loaded_net_ticks,
        (source_count,),
        "fully_loaded_net_ticks",
    )
    exact_directions = _exact_int64_array(
        base_directions,
        (source_count,),
        "base_directions",
    )
    if any(int(value) not in {-1, 1} for value in exact_directions):
        raise AllCasesMLError("base directions must be signed one or minus one")
    directions = np.asarray(exact_directions, dtype=np.int8)
    atr = _deep_numeric_float64_array(atr_ticks, (source_count,), "atr_ticks")
    exits = _exact_int64_array(label_exit_ns, (source_count,), "label_exit_ns")
    if (
        len(contract_values) != source_count
        or net.shape != (source_count,)
        or atr.shape != (source_count,)
        or len(outcome_lineage_sha256) != 64
        or any(character not in "0123456789abcdef" for character in outcome_lineage_sha256)
        or len(opportunity_lattice_sha256) != 64
        or any(character not in "0123456789abcdef" for character in opportunity_lattice_sha256)
    ):
        raise AllCasesMLError("meta training metadata differs")
    if (
        any(contract_values[index] != rows.contracts[index] for index in indexes)
        or np.any(spans[selected] != rows.outcome_span_ids[selected])
        or np.any(segments[selected] != rows.segment_ids[selected])
        or np.any(entries[selected] != rows.entry_ns[selected])
        or np.any(entries[selected] < rows.decision_ns[selected])
        or np.any(exits[selected] <= entries[selected])
    ):
        raise AllCasesMLError("meta base path is invalid or cross-lineage")
    selected_row_ids = tuple(rows.row_ids[index] for index in indexes)
    selected_exits = exits[selected]
    return TrainingMatrix(
        feature_set_id=rows.feature_set_id,
        feature_names=rows.feature_names,
        row_ids=selected_row_ids,
        decision_dates=tuple(rows.source_dates[index] for index in indexes),
        decision_ns=rows.decision_ns[selected],
        entry_ns=entries[selected],
        label_exit_ns=selected_exits,
        values=rows.values[selected],
        targets=build_meta_profitability_targets(net[selected]),
        atr_ticks=atr[selected],
        contracts=tuple(contract_values[index] for index in indexes),
        outcome_span_ids=spans[selected],
        segment_ids=segments[selected],
        outcome_lineage_sha256=outcome_lineage_sha256,
        opportunity_lattice_sha256=opportunity_lattice_sha256,
        entry_schedule_sha256=rows.entry_schedule_sha256,
        match_strata=tuple(strata[index] for index in indexes),
        base_directions=directions[selected],
        realized_net_ticks=net[selected],
        base_strategy_id=ranked_strategy.strategy_id,
        base_trigger_family=ranked_strategy.trigger_family,
        symbolic_ranking_certificate=symbolic_ranking_certificate,
        expert_artifact_sha256s=tuple(typed_experts[index].artifact_sha256 for index in indexes),
        expert_formula_sha256=typed_experts[0].formula_sha256,
    )


@dataclass(frozen=True, slots=True)
class SearchOuterFold:
    """An expanding-window Search fold; its validation block is never fitted."""

    fold_key: str
    training_block_keys: tuple[str, ...]
    validation_block_key: str
    training_dates: tuple[date, ...]
    validation_dates: tuple[date, ...]

    def __post_init__(self) -> None:
        if (
            self.fold_key not in SEARCH_OUTER_FOLD_KEYS
            or self.validation_block_key != self.fold_key
            or not self.training_block_keys
            or not self.training_dates
            or not self.validation_dates
            or tuple(sorted(self.training_dates)) != self.training_dates
            or tuple(sorted(self.validation_dates)) != self.validation_dates
            or len({*self.training_dates, *self.validation_dates})
            != len(self.training_dates) + len(self.validation_dates)
            or self.training_dates[-1] >= self.validation_dates[0]
        ):
            raise AllCasesMLError("Search outer-fold chronology differs")

    def as_dict(self) -> dict[str, object]:
        return {
            "fold_key": self.fold_key,
            "training_block_keys": list(self.training_block_keys),
            "training_dates": [value.isoformat() for value in self.training_dates],
            "validation_block_key": self.validation_block_key,
            "validation_dates": [value.isoformat() for value in self.validation_dates],
        }


@dataclass(frozen=True, slots=True)
class SearchBlockPlan:
    decision_dates: tuple[date, ...]
    blocks: tuple[tuple[date, ...], ...]
    outer_folds: tuple[SearchOuterFold, ...]

    def __post_init__(self) -> None:
        if (
            len(self.decision_dates) != SEARCH_DECISION_DATE_COUNT
            or len(set(self.decision_dates)) != SEARCH_DECISION_DATE_COUNT
            or tuple(sorted(self.decision_dates)) != self.decision_dates
            or tuple(len(block) for block in self.blocks) != SEARCH_BLOCK_SIZES
            or tuple(value for block in self.blocks for value in block) != self.decision_dates
            or tuple(fold.fold_key for fold in self.outer_folds) != SEARCH_OUTER_FOLD_KEYS
        ):
            raise AllCasesMLError("Search block plan differs")

    def as_dict(self) -> dict[str, object]:
        return {
            "blocks": [
                {
                    "block_key": f"B{index + 1}",
                    "dates": [value.isoformat() for value in block],
                }
                for index, block in enumerate(self.blocks)
            ],
            "decision_date_count": len(self.decision_dates),
            "outer_folds": [fold.as_dict() for fold in self.outer_folds],
            "schema": SEARCH_BLOCK_SCHEMA,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


def build_search_block_plan(decision_dates: Sequence[date]) -> SearchBlockPlan:
    """Split the exact Search calendar into the frozen eight chronological blocks."""

    dates = tuple(decision_dates)
    if (
        len(dates) != SEARCH_DECISION_DATE_COUNT
        or any(isinstance(value, datetime) or not isinstance(value, date) for value in dates)
        or tuple(sorted(set(dates))) != dates
    ):
        raise AllCasesMLError("Search requires 469 unique, strictly ordered decision dates")
    blocks: list[tuple[date, ...]] = []
    cursor = 0
    for size in SEARCH_BLOCK_SIZES:
        blocks.append(dates[cursor : cursor + size])
        cursor += size
    outer_folds = []
    for validation_index in range(2, 8):
        training_blocks = blocks[:validation_index]
        outer_folds.append(
            SearchOuterFold(
                f"B{validation_index + 1}",
                tuple(f"B{index + 1}" for index in range(validation_index)),
                f"B{validation_index + 1}",
                tuple(value for block in training_blocks for value in block),
                blocks[validation_index],
            )
        )
    return SearchBlockPlan(dates, tuple(blocks), tuple(outer_folds))


@dataclass(frozen=True, slots=True)
class PurgedFoldRows:
    fold_key: str
    training_indexes: tuple[int, ...]
    validation_indexes: tuple[int, ...]
    first_validation_entry_ns: int
    purged_training_count: int

    def __post_init__(self) -> None:
        if (
            self.fold_key not in SEARCH_OUTER_FOLD_KEYS
            or len(self.training_indexes) < 2
            or not self.validation_indexes
            or tuple(sorted(set(self.training_indexes))) != self.training_indexes
            or tuple(sorted(set(self.validation_indexes))) != self.validation_indexes
            or set(self.training_indexes).intersection(self.validation_indexes)
            or self.first_validation_entry_ns <= 0
            or self.purged_training_count < 0
        ):
            raise AllCasesMLError("purged fold rows differ")


def purged_fold_rows(matrix: TrainingMatrix, fold: SearchOuterFold) -> PurgedFoldRows:
    """Resolve row indexes and remove every training label overlapping validation."""

    training_dates = set(fold.training_dates)
    validation_dates = set(fold.validation_dates)
    provisional_training = tuple(
        index for index, value in enumerate(matrix.decision_dates) if value in training_dates
    )
    validation = tuple(
        index for index, value in enumerate(matrix.decision_dates) if value in validation_dates
    )
    if len(provisional_training) < 2 or not validation:
        raise MLCandidateIneligible(
            MLIneligibilityReason.INSUFFICIENT_FOLD_ROWS,
            "model has insufficient rows in a Search fold",
            scope_key=fold.fold_key,
        )
    first_validation_entry = min(int(matrix.entry_ns[index]) for index in validation)
    training = tuple(
        index
        for index in provisional_training
        if int(matrix.label_exit_ns[index]) < first_validation_entry
    )
    if len(training) < 2:
        raise MLCandidateIneligible(
            MLIneligibilityReason.INSUFFICIENT_FOLD_ROWS,
            "model has fewer than two rows after the exact label-overlap purge",
            scope_key=fold.fold_key,
        )
    return PurgedFoldRows(
        fold.fold_key,
        training,
        validation,
        first_validation_entry,
        len(provisional_training) - len(training),
    )


def _normalized_indexes(indexes: Sequence[int], row_count: int) -> tuple[int, ...]:
    raw = tuple(indexes)
    if any(
        isinstance(index, (bool, np.bool_)) or not isinstance(index, (int, np.integer))
        for index in raw
    ):
        raise AllCasesMLError("target-permutation training indexes must be exact integers")
    selected = tuple(int(index) for index in raw)
    if (
        len(selected) < 2
        or tuple(sorted(set(selected))) != selected
        or selected[0] < 0
        or selected[-1] >= row_count
    ):
        raise AllCasesMLError("target-permutation training indexes differ")
    return selected


def _randomization_identity(candidate_id: str) -> str:
    direct = DIRECT_CANDIDATE_BY_ID.get(candidate_id)
    if direct is not None:
        return direct_fit_recipe_id(direct)
    meta = META_CANDIDATE_BY_ID.get(candidate_id)
    if meta is not None:
        return meta_fit_recipe_id(meta)
    return candidate_id


def _circular_group_mapping(
    matrix: TrainingMatrix,
    group: tuple[int, ...],
    *,
    candidate_id: str,
    fold_key: str,
) -> dict[int, int]:
    if len(group) < 2:
        raise MLCandidateIneligible(
            MLIneligibilityReason.NULL_DERANGEMENT_INFEASIBLE,
            "circular null group cannot be deranged",
            candidate_id=candidate_id,
            scope_key=fold_key,
        )
    span_counts: dict[int, int] = defaultdict(int)
    for index in group:
        span_counts[int(matrix.outcome_span_ids[index])] += 1
    maximum_span_count = max(span_counts.values())
    if maximum_span_count * 2 > len(group):
        raise MLCandidateIneligible(
            MLIneligibilityReason.NULL_DERANGEMENT_INFEASIBLE,
            "circular null has an outcome span above half its contract",
            candidate_id=candidate_id,
            scope_key=fold_key,
        )
    preferred = (
        maximum_span_count
        if _seed(candidate_id, "CIRCULAR_TARGET", fold_key, "ROTATION_SIDE") % 2 == 0
        else len(group) - maximum_span_count
    )
    shifts = tuple(dict.fromkeys((preferred, maximum_span_count, len(group) - maximum_span_count)))
    for shift in shifts:
        mapping = {
            destination: group[(position + shift) % len(group)]
            for position, destination in enumerate(group)
        }
        if all(
            matrix.outcome_span_ids[destination] != matrix.outcome_span_ids[source]
            for destination, source in mapping.items()
        ):
            return mapping
    # A real manifest normally has contiguous outcome spans.  The canonical
    # span-block fallback keeps the algorithm exact and O(N) for interleaved
    # verified manifests instead of searching all O(N) shifts.
    span_blocks: dict[int, list[int]] = {}
    for index in group:
        span_blocks.setdefault(int(matrix.outcome_span_ids[index]), []).append(index)
    ordered = tuple(index for block in span_blocks.values() for index in block)
    for shift in shifts:
        mapping = {
            destination: ordered[(position + shift) % len(group)]
            for position, destination in enumerate(ordered)
        }
        if all(
            matrix.outcome_span_ids[destination] != matrix.outcome_span_ids[source]
            for destination, source in mapping.items()
        ):
            return mapping
    raise MLCandidateIneligible(
        MLIneligibilityReason.NULL_DERANGEMENT_INFEASIBLE,
        "circular null cannot avoid an identical outcome span",
        candidate_id=candidate_id,
        scope_key=fold_key,
    )


def _matched_group_mapping(
    matrix: TrainingMatrix,
    group: tuple[int, ...],
    *,
    candidate_id: str,
    fold_key: str,
) -> dict[int, int]:
    """Build an O(N log N) stratum-aware span-block derangement."""

    if len(group) < 2:
        raise MLCandidateIneligible(
            MLIneligibilityReason.NULL_DERANGEMENT_INFEASIBLE,
            "matched null stratum cannot be deranged",
            candidate_id=candidate_id,
            scope_key=fold_key,
        )

    def coarse(value: str) -> str:
        return value.split("|", 1)[0]

    by_span: dict[int, list[int]] = defaultdict(list)
    for index in group:
        by_span[int(matrix.outcome_span_ids[index])].append(index)
    maximum_span_count = max(len(values) for values in by_span.values())
    if maximum_span_count * 2 > len(group):
        raise MLCandidateIneligible(
            MLIneligibilityReason.NULL_DERANGEMENT_INFEASIBLE,
            "matched null has an outcome span above half its contract",
            candidate_id=candidate_id,
            scope_key=fold_key,
        )

    def seeded(index: int, purpose: str) -> str:
        return canonical_sha256(
            {
                "candidate_id": candidate_id,
                "fold_key": fold_key,
                "purpose": purpose,
                "row_id": matrix.row_ids[index],
            }
        )

    span_order = tuple(sorted(by_span))
    destinations = tuple(
        index
        for span in span_order
        for index in sorted(
            by_span[span],
            key=lambda value: (
                coarse(matrix.match_strata[value]),
                matrix.match_strata[value],
                seeded(value, "MATCHED_DESTINATION_ORDER"),
            ),
        )
    )
    source_blocks = {
        span: sorted(
            by_span[span],
            key=lambda value: (
                coarse(matrix.match_strata[value]),
                matrix.match_strata[value],
                seeded(value, "MATCHED_SOURCE_ORDER"),
            ),
        )
        for span in span_order
    }

    def mapping_for(source_order: tuple[int, ...], shift: int) -> dict[int, int]:
        return {
            destination: source_order[(position + shift) % len(group)]
            for position, destination in enumerate(destinations)
        }

    source_orders = (
        tuple(index for span in span_order for index in source_blocks[span]),
        tuple(index for span in span_order for index in reversed(source_blocks[span])),
    )
    shifts = tuple(dict.fromkeys((maximum_span_count, len(group) - maximum_span_count)))
    for source_order in source_orders:
        for shift in shifts:
            mapping = mapping_for(source_order, shift)
            if all(
                destination != source
                and matrix.outcome_span_ids[destination] != matrix.outcome_span_ids[source]
                for destination, source in mapping.items()
            ):
                return mapping
    raise MLCandidateIneligible(
        MLIneligibilityReason.NULL_DERANGEMENT_INFEASIBLE,
        "matched null cannot construct a span-safe derangement",
        candidate_id=candidate_id,
        scope_key=fold_key,
    )


def _alternate_matched_sources(
    matrix: TrainingMatrix,
    selected: tuple[int, ...],
    sources: tuple[int, ...],
    circular: tuple[int, ...],
    *,
    candidate_id: str,
    fold_key: str,
) -> tuple[int, ...]:
    """Choose a second canonical span-safe bijection when the first matched map collides."""

    candidate = list(sources)
    positions_by_contract_span: dict[tuple[str, int], list[int]] = defaultdict(list)
    positions_by_contract: dict[str, list[int]] = defaultdict(list)
    for position, source in enumerate(sources):
        contract = matrix.contracts[selected[position]]
        if matrix.contracts[source] != contract:
            raise AllCasesMLError("matched null escaped its destination contract")
        positions_by_contract_span[contract, int(matrix.outcome_span_ids[source])].append(position)
        positions_by_contract[contract].append(position)

    # Swapping two sources from the same span preserves every destination/span
    # exclusion while deterministically changing the bijection.
    for key in sorted(positions_by_contract_span):
        positions = positions_by_contract_span[key]
        if len(positions) >= 2:
            left, right = positions[:2]
            candidate[left], candidate[right] = candidate[right], candidate[left]
            result = tuple(candidate)
            if result != circular:
                return result
            candidate[left], candidate[right] = candidate[right], candidate[left]

    # If every span is unique within a contract, either of the two canonical
    # cyclic directions is safe for groups of at least three rows.
    for contract in sorted(positions_by_contract):
        positions = positions_by_contract[contract]
        if len(positions) < 3:
            continue
        group = tuple(selected[position] for position in positions)
        for shift in (1, len(group) - 1):
            alternative = list(sources)
            for local_position, output_position in enumerate(positions):
                alternative[output_position] = group[(local_position + shift) % len(group)]
            result = tuple(alternative)
            if result != circular and all(
                matrix.outcome_span_ids[destination] != matrix.outcome_span_ids[source]
                for destination, source in zip(selected, result, strict=True)
            ):
                return result

    raise MLCandidateIneligible(
        MLIneligibilityReason.NULL_DERANGEMENT_INFEASIBLE,
        "matched null has no second span-safe bijection distinct from circular",
        candidate_id=candidate_id,
        scope_key=fold_key,
    )


def target_permutation_indexes(
    matrix: TrainingMatrix,
    training_indexes: Sequence[int],
    *,
    world: NullWorld | str,
    candidate_id: str,
    fold_key: str,
) -> tuple[int, ...]:
    """Map each training row to a training-only target source for one null world."""

    selected = _normalized_indexes(training_indexes, matrix.row_count)
    try:
        normalized_world = NullWorld(world)
    except ValueError as error:
        raise AllCasesMLError("unknown null world") from error
    if len(candidate_id) != 64 or not fold_key:
        raise AllCasesMLError("null permutation identity differs")
    randomization_identity = _randomization_identity(candidate_id)
    if normalized_world is NullWorld.REAL:
        return selected

    groups: dict[tuple[str, ...], list[int]] = defaultdict(list)
    for index in selected:
        key = (matrix.contracts[index],)
        groups[key].append(index)
    mapping: dict[int, int] = {}
    for key in sorted(groups):
        group = tuple(groups[key])
        group_mapping = (
            _circular_group_mapping(
                matrix,
                group,
                candidate_id=randomization_identity,
                fold_key=fold_key,
            )
            if normalized_world is NullWorld.CIRCULAR_TARGET
            else _matched_group_mapping(
                matrix,
                group,
                candidate_id=randomization_identity,
                fold_key=fold_key,
            )
        )
        mapping.update(group_mapping)
    sources = tuple(mapping[index] for index in selected)
    if (
        len(mapping) != len(selected)
        or set(sources) != set(selected)
        or any(destination == source for destination, source in zip(selected, sources, strict=True))
        or any(
            matrix.outcome_span_ids[destination] == matrix.outcome_span_ids[source]
            for destination, source in zip(selected, sources, strict=True)
        )
    ):
        raise AllCasesMLError("null target mapping is not an exact distinct bijection")
    if normalized_world is NullWorld.MATCHED_TARGET:
        circular = target_permutation_indexes(
            matrix,
            selected,
            world=NullWorld.CIRCULAR_TARGET,
            candidate_id=randomization_identity,
            fold_key=fold_key,
        )
        if sources == circular:
            sources = _alternate_matched_sources(
                matrix,
                selected,
                sources,
                circular,
                candidate_id=candidate_id,
                fold_key=fold_key,
            )
    return sources


@dataclass(frozen=True, slots=True)
class TargetPermutationPlan:
    world: str
    candidate_id: str
    fold_key: str
    destination_indexes: tuple[int, ...]
    source_indexes: tuple[int, ...]
    exact_stratum_count: int
    coarse_stratum_count: int
    same_contract_fallback_count: int

    def __post_init__(self) -> None:
        count = len(self.destination_indexes)
        if (
            self.world not in NULL_WORLD_ORDER
            or len(self.candidate_id) != 64
            or not self.fold_key
            or count < 2
            or len(self.source_indexes) != count
            or set(self.destination_indexes) != set(self.source_indexes)
            or min(
                self.exact_stratum_count,
                self.coarse_stratum_count,
                self.same_contract_fallback_count,
            )
            < 0
        ):
            raise AllCasesMLError("target permutation-plan binding differs")
        assigned = (
            self.exact_stratum_count + self.coarse_stratum_count + self.same_contract_fallback_count
        )
        if self.world == NullWorld.MATCHED_TARGET.value:
            if assigned != count:
                raise AllCasesMLError("matched permutation fallback counts differ")
        elif assigned != 0:
            raise AllCasesMLError("non-matched permutation cannot report fallback tiers")

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "destination_indexes": list(self.destination_indexes),
            "exact_stratum_count": self.exact_stratum_count,
            "fold_key": self.fold_key,
            "same_contract_fallback_count": self.same_contract_fallback_count,
            "coarse_stratum_count": self.coarse_stratum_count,
            "schema": "systematic_fx.ai_all_cases_target_permutation_plan.v1",
            "source_indexes": list(self.source_indexes),
            "world": self.world,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


def target_permutation_plan(
    matrix: TrainingMatrix,
    training_indexes: Sequence[int],
    *,
    world: NullWorld | str,
    candidate_id: str,
    fold_key: str,
) -> TargetPermutationPlan:
    destinations = _normalized_indexes(training_indexes, matrix.row_count)
    normalized_world = _normalized_world(world)
    sources = target_permutation_indexes(
        matrix,
        destinations,
        world=normalized_world,
        candidate_id=candidate_id,
        fold_key=fold_key,
    )
    exact = coarse = fallback = 0
    if normalized_world is NullWorld.MATCHED_TARGET:
        for destination, source in zip(destinations, sources, strict=True):
            if matrix.match_strata[destination] == matrix.match_strata[source]:
                exact += 1
            elif (
                matrix.match_strata[destination].split("|", 1)[0]
                == (matrix.match_strata[source].split("|", 1)[0])
            ):
                coarse += 1
            else:
                fallback += 1
    return TargetPermutationPlan(
        normalized_world.value,
        candidate_id,
        fold_key,
        destinations,
        sources,
        exact,
        coarse,
        fallback,
    )


def probe_null_world_feasibility(
    matrix: TrainingMatrix,
    search_plan: SearchBlockPlan,
    *,
    candidate_id: str,
) -> dict[str, object]:
    """Fail before fitting unless both nulls preserve every required fold row."""

    records: list[dict[str, object]] = []
    fold_indexes = [
        (fold.fold_key, purged_fold_rows(matrix, fold).training_indexes)
        for fold in search_plan.outer_folds
    ]
    fold_indexes.append(("SEARCH_FINAL", tuple(range(matrix.row_count))))
    for fold_key, indexes in fold_indexes:
        for world in (NullWorld.CIRCULAR_TARGET, NullWorld.MATCHED_TARGET):
            plan = target_permutation_plan(
                matrix,
                indexes,
                world=world,
                candidate_id=candidate_id,
                fold_key=fold_key,
            )
            records.append(
                {
                    "coarse_stratum_count": plan.coarse_stratum_count,
                    "exact_stratum_count": plan.exact_stratum_count,
                    "fold_key": fold_key,
                    "permutation_plan_sha256": plan.sha256,
                    "row_count": len(indexes),
                    "same_contract_fallback_count": plan.same_contract_fallback_count,
                    "world": world.value,
                }
            )
    document = {
        "candidate_id": candidate_id,
        "records": records,
        "schema": "systematic_fx.ai_all_cases_null_feasibility.v1",
        "search_block_plan_sha256": search_plan.sha256,
        "training_rows_sha256": training_rows_sha256(matrix),
    }
    return {**document, "report_sha256": canonical_sha256(document)}


def probe_meta_crossfit_null_feasibility(
    candidate: MetaCandidate,
    matrices_by_world_and_fold: Mapping[str, Mapping[str, TrainingMatrix]],
    search_plan: SearchBlockPlan,
) -> dict[str, object]:
    """Preflight independently reranked fold matrices for every target world."""

    _validate_meta_world_fold_matrices(candidate, matrices_by_world_and_fold, search_plan)
    records: list[dict[str, object]] = []
    for fold in search_plan.outer_folds:
        for world in (NullWorld.CIRCULAR_TARGET, NullWorld.MATCHED_TARGET):
            matrix = matrices_by_world_and_fold[world.value][fold.fold_key]
            indexes = purged_fold_rows(matrix, fold).training_indexes
            plan = target_permutation_plan(
                matrix,
                indexes,
                world=world,
                candidate_id=candidate.candidate_id,
                fold_key=fold.fold_key,
            )
            records.append(
                {
                    "base_strategy_id": matrix.base_strategy_id,
                    "coarse_stratum_count": plan.coarse_stratum_count,
                    "exact_stratum_count": plan.exact_stratum_count,
                    "fold_key": fold.fold_key,
                    "permutation_plan_sha256": plan.sha256,
                    "row_count": len(indexes),
                    "same_contract_fallback_count": plan.same_contract_fallback_count,
                    "symbolic_ranking_sha256": matrix.symbolic_ranking_sha256,
                    "world": world.value,
                }
            )
    document = {
        "candidate_id": candidate.candidate_id,
        "records": records,
        "schema": "systematic_fx.ai_all_cases_meta_null_feasibility.v1",
        "search_block_plan_sha256": search_plan.sha256,
    }
    return {**document, "report_sha256": canonical_sha256(document)}


def _validate_meta_world_fold_matrices(
    candidate: MetaCandidate,
    matrices_by_world_and_fold: Mapping[str, Mapping[str, TrainingMatrix]],
    search_plan: SearchBlockPlan,
) -> None:
    if set(matrices_by_world_and_fold) != set(NULL_WORLD_ORDER):
        raise AllCasesMLError("meta cross-fit requires all three independently reranked worlds")
    for world in NULL_WORLD_ORDER:
        matrices = matrices_by_world_and_fold[world]
        if set(matrices) != set(SEARCH_OUTER_FOLD_KEYS):
            raise AllCasesMLError("meta cross-fit requires B3 through B8 in every world")
        for fold_key in SEARCH_OUTER_FOLD_KEYS:
            matrix = matrices[fold_key]
            fold = next(item for item in search_plan.outer_folds if item.fold_key == fold_key)
            certificate = matrix.symbolic_ranking_certificate
            if (
                matrix.feature_set_id != candidate.feature_set_id
                or matrix.base_directions is None
                or matrix.realized_net_ticks is None
                or matrix.symbolic_ranking_world != world
                or certificate is None
                or certificate.fold_key != fold_key
                or certificate.training_dates != fold.training_dates
            ):
                raise AllCasesMLError("meta world/fold matrix binding differs")
            ranked_strategy = _require_ranked_strategy(certificate, candidate, scope_key=fold_key)
            if (
                matrix.base_strategy_id != ranked_strategy.strategy_id
                or matrix.base_trigger_family != ranked_strategy.trigger_family
            ):
                raise AllCasesMLError("meta candidate rank-slot certificate binding differs")
    for fold_key in SEARCH_OUTER_FOLD_KEYS:
        ranking_hashes = tuple(
            matrices_by_world_and_fold[world][fold_key].symbolic_ranking_sha256
            for world in NULL_WORLD_ORDER
        )
        if len(set(ranking_hashes)) != len(NULL_WORLD_ORDER):
            raise AllCasesMLError("meta null world reused a REAL symbolic ranking certificate")


def permuted_training_targets(
    matrix: TrainingMatrix,
    training_indexes: Sequence[int],
    *,
    world: NullWorld | str,
    candidate_id: str,
    fold_key: str,
) -> np.ndarray:
    """Return target values in destination-row order using no non-training source."""

    source_indexes = target_permutation_indexes(
        matrix,
        training_indexes,
        world=world,
        candidate_id=candidate_id,
        fold_key=fold_key,
    )
    result = np.array(matrix.targets[np.asarray(source_indexes, dtype=np.int64)], copy=True)
    result.setflags(write=False)
    return result


@dataclass(frozen=True, slots=True)
class FrozenPreprocessor:
    """Train-only median imputer, missing flags, and optional scaler."""

    raw_feature_count: int
    medians: tuple[float, ...]
    missing_indicator_indexes: tuple[int, ...]
    scaler_mean: tuple[float, ...] | None
    scaler_scale: tuple[float, ...] | None

    def __post_init__(self) -> None:
        if (
            isinstance(self.raw_feature_count, bool)
            or not isinstance(self.raw_feature_count, int)
            or self.raw_feature_count <= 0
            or len(self.medians) != self.raw_feature_count
            or any(
                isinstance(index, bool) or not isinstance(index, int)
                for index in self.missing_indicator_indexes
            )
        ):
            raise AllCasesMLError("preprocessor raw width differs")
        if any(not math.isfinite(value) for value in self.medians):
            raise AllCasesMLError("preprocessor medians must be finite")
        if self.missing_indicator_indexes != tuple(
            sorted(set(self.missing_indicator_indexes))
        ) or any(
            index < 0 or index >= self.raw_feature_count for index in self.missing_indicator_indexes
        ):
            raise AllCasesMLError("missing-indicator indexes differ")
        width = self.transformed_feature_count
        if (self.scaler_mean is None) != (self.scaler_scale is None):
            raise AllCasesMLError("preprocessor scaler state is partial")
        if self.scaler_mean is not None and (
            len(self.scaler_mean) != width
            or len(self.scaler_scale or ()) != width
            or any(not math.isfinite(value) for value in self.scaler_mean)
            or any(not math.isfinite(value) or value <= 0 for value in (self.scaler_scale or ()))
        ):
            raise AllCasesMLError("preprocessor scaler state differs")

    @property
    def transformed_feature_count(self) -> int:
        return self.raw_feature_count + len(self.missing_indicator_indexes)

    @classmethod
    def fit(cls, values: np.ndarray, *, scale: bool) -> FrozenPreprocessor:
        matrix = np.asarray(values, dtype=np.float64)
        if matrix.ndim != 2 or not matrix.shape[0] or not matrix.shape[1] or np.isinf(matrix).any():
            raise AllCasesMLError("preprocessor input matrix differs")
        medians: list[float] = []
        indicator_indexes: list[int] = []
        for index in range(matrix.shape[1]):
            column = matrix[:, index]
            present = column[~np.isnan(column)]
            if not len(present):
                raise MLCandidateIneligible(
                    MLIneligibilityReason.ALL_MISSING_TRAINING_COLUMN,
                    "training feature is entirely missing; model fails closed",
                )
            medians.append(float(np.median(present)))
            if np.isnan(column).any():
                indicator_indexes.append(index)
        provisional = cls(matrix.shape[1], tuple(medians), tuple(indicator_indexes), None, None)
        transformed = provisional.transform(matrix)
        if not scale:
            return provisional
        scaler = StandardScaler(copy=True, with_mean=True, with_std=True).fit(transformed)
        mean = tuple(float(value) for value in scaler.mean_)
        scale_values = tuple(float(value) for value in scaler.scale_)
        return cls(
            matrix.shape[1],
            tuple(medians),
            tuple(indicator_indexes),
            mean,
            scale_values,
        )

    def transform(self, values: np.ndarray) -> np.ndarray:
        matrix = np.array(values, dtype=np.float64, copy=True)
        if matrix.ndim != 2 or matrix.shape[1] != self.raw_feature_count or np.isinf(matrix).any():
            raise AllCasesMLError("prediction feature width or values differ")
        missing = np.isnan(matrix)
        for index, median in enumerate(self.medians):
            matrix[missing[:, index], index] = median
        if self.missing_indicator_indexes:
            indicators = missing[:, self.missing_indicator_indexes].astype(np.float64)
            matrix = np.concatenate((matrix, indicators), axis=1)
        if self.scaler_mean is not None:
            matrix = (matrix - np.asarray(self.scaler_mean)) / np.asarray(self.scaler_scale)
        if not np.isfinite(matrix).all():  # pragma: no cover - guarded above
            raise AllCasesMLError("preprocessor produced non-finite values")
        return matrix

    def as_dict(self) -> dict[str, object]:
        return {
            "medians_hex": [_float_hex(value) for value in self.medians],
            "missing_indicator_indexes": list(self.missing_indicator_indexes),
            "raw_feature_count": self.raw_feature_count,
            "scaler_mean_hex": (
                None
                if self.scaler_mean is None
                else [_float_hex(value) for value in self.scaler_mean]
            ),
            "scaler_scale_hex": (
                None
                if self.scaler_scale is None
                else [_float_hex(value) for value in self.scaler_scale]
            ),
            "schema": "systematic_fx.ai_all_cases_ml_preprocessor.v1",
            "transformed_feature_count": self.transformed_feature_count,
        }

    @classmethod
    def from_dict(cls, value: object) -> FrozenPreprocessor:
        keys = {
            "medians_hex",
            "missing_indicator_indexes",
            "raw_feature_count",
            "scaler_mean_hex",
            "scaler_scale_hex",
            "schema",
            "transformed_feature_count",
        }
        if (
            not isinstance(value, dict)
            or set(value) != keys
            or value["schema"] != ("systematic_fx.ai_all_cases_ml_preprocessor.v1")
        ):
            raise AllCasesMLError("preprocessor document schema differs")
        medians_raw = value["medians_hex"]
        indicators_raw = value["missing_indicator_indexes"]
        if not isinstance(medians_raw, list) or not isinstance(indicators_raw, list):
            raise AllCasesMLError("preprocessor arrays differ")
        if (
            isinstance(value["raw_feature_count"], bool)
            or not isinstance(value["raw_feature_count"], int)
            or isinstance(value["transformed_feature_count"], bool)
            or not isinstance(value["transformed_feature_count"], int)
            or any(isinstance(item, bool) or not isinstance(item, int) for item in indicators_raw)
        ):
            raise AllCasesMLError("preprocessor widths must be exact integers")

        def decode_optional(raw: object, label: str) -> tuple[float, ...] | None:
            if raw is None:
                return None
            if not isinstance(raw, list):
                raise AllCasesMLError(f"{label} differs")
            return tuple(_decode_float_hex(item, label=label) for item in raw)

        result = cls(
            value["raw_feature_count"],
            tuple(_decode_float_hex(item, label="median") for item in medians_raw),
            tuple(indicators_raw),
            decode_optional(value["scaler_mean_hex"], "scaler_mean"),
            decode_optional(value["scaler_scale_hex"], "scaler_scale"),
        )
        if result.as_dict() != value:
            raise AllCasesMLError("preprocessor document did not round trip")
        return result


def _hgb_predictor_document(
    estimator: object,
    *,
    classifier: bool,
    learner_recipe_sha256: str,
) -> dict[str, object]:
    baseline = getattr(estimator, "_baseline_prediction", None)
    predictors = getattr(estimator, "_predictors", None)
    if (
        not isinstance(baseline, np.ndarray)
        or baseline.shape != (1, 1)
        or not isinstance(predictors, list)
        or not predictors
    ):
        raise AllCasesMLError("HGB fitted state differs from sklearn 1.9 contract")
    trees = []
    for iteration in predictors:
        if not isinstance(iteration, list) or len(iteration) != 1:
            raise AllCasesMLError("HGB tree multiplicity differs")
        predictor = iteration[0]
        if len(predictor.raw_left_cat_bitsets) or len(predictor.binned_left_cat_bitsets):
            raise AllCasesMLError("categorical HGB state is prohibited")
        nodes = []
        for node in predictor.nodes:
            nodes.append(
                {
                    "feature_idx": int(node["feature_idx"]),
                    "is_leaf": bool(node["is_leaf"]),
                    "left": int(node["left"]),
                    "missing_go_to_left": bool(node["missing_go_to_left"]),
                    "right": int(node["right"]),
                    "threshold_hex": _float_hex(float(node["num_threshold"])),
                    "value_hex": _float_hex(float(node["value"])),
                }
            )
        trees.append(nodes)
    return {
        "baseline_hex": _float_hex(float(baseline[0, 0])),
        "kind": "HGB_BINARY_CLASSIFIER" if classifier else "HGB_REGRESSOR",
        "learner_recipe_sha256": learner_recipe_sha256,
        "trees": trees,
    }


def _validate_predictor(
    value: object,
    transformed_width: int,
    *,
    expected_learner_recipe_sha256: str | None = None,
    expected_tree_count: int | None = None,
    maximum_leaf_nodes: int | None = None,
) -> dict[str, object]:
    if not isinstance(value, dict) or not isinstance(value.get("kind"), str):
        raise AllCasesMLError("predictor document differs")
    recipe_sha256 = value.get("learner_recipe_sha256")
    if (
        not isinstance(recipe_sha256, str)
        or len(recipe_sha256) != 64
        or any(character not in "0123456789abcdef" for character in recipe_sha256)
        or expected_learner_recipe_sha256 is not None
        and recipe_sha256 != expected_learner_recipe_sha256
    ):
        raise AllCasesMLError("predictor learner-recipe binding differs")
    kind = value["kind"]
    if kind in {"ELASTIC_NET_REGRESSOR", "LOGISTIC_BINARY_CLASSIFIER"}:
        expected = {
            "coefficient_hex",
            "intercept_hex",
            "kind",
            "learner_recipe_sha256",
        }
        if set(value) != expected or not isinstance(value["coefficient_hex"], list):
            raise AllCasesMLError("linear predictor schema differs")
        coefficients = [
            _decode_float_hex(item, label="coefficient") for item in value["coefficient_hex"]
        ]
        _decode_float_hex(value["intercept_hex"], label="intercept")
        if len(coefficients) != transformed_width:
            raise AllCasesMLError("linear predictor width differs")
        return value
    if kind not in {"HGB_REGRESSOR", "HGB_BINARY_CLASSIFIER"} or set(value) != {
        "baseline_hex",
        "kind",
        "learner_recipe_sha256",
        "trees",
    }:
        raise AllCasesMLError("HGB predictor schema differs")
    _decode_float_hex(value["baseline_hex"], label="HGB baseline")
    trees = value["trees"]
    if not isinstance(trees, list) or not trees:
        raise AllCasesMLError("HGB tree list differs")
    if expected_tree_count is not None and len(trees) != expected_tree_count:
        raise AllCasesMLError("HGB tree count differs from its exact learner recipe")
    node_keys = {
        "feature_idx",
        "is_leaf",
        "left",
        "missing_go_to_left",
        "right",
        "threshold_hex",
        "value_hex",
    }
    for nodes in trees:
        if not isinstance(nodes, list) or not nodes:
            raise AllCasesMLError("HGB node list differs")
        for index, node in enumerate(nodes):
            if not isinstance(node, dict) or set(node) != node_keys:
                raise AllCasesMLError("HGB node schema differs")
            feature = node["feature_idx"]
            if (
                isinstance(feature, bool)
                or not isinstance(feature, int)
                or not 0 <= feature < transformed_width
            ):
                raise AllCasesMLError("HGB feature index differs")
            if not isinstance(node["is_leaf"], bool) or not isinstance(
                node["missing_go_to_left"], bool
            ):
                raise AllCasesMLError("HGB Boolean node state differs")
            _decode_float_hex(node["threshold_hex"], label="HGB threshold")
            _decode_float_hex(node["value_hex"], label="HGB value")
            for key in ("left", "right"):
                child = node[key]
                if (
                    isinstance(child, bool)
                    or not isinstance(child, int)
                    or not 0 <= child < len(nodes)
                ):
                    raise AllCasesMLError("HGB child index differs")
            if not node["is_leaf"] and node["left"] == node["right"]:
                raise AllCasesMLError("HGB branch children are identical")
            if index == 0 and node["is_leaf"] and len(nodes) != 1:
                raise AllCasesMLError("HGB root reachability differs")
        reachable: set[int] = set()
        active: set[int] = set()

        def visit(
            index: int,
            *,
            tree_nodes: list[dict[str, object]] = nodes,
            reached: set[int] = reachable,
            in_path: set[int] = active,
        ) -> None:
            if index in in_path:
                raise AllCasesMLError("HGB predictor contains a cycle")
            if index in reached:
                raise AllCasesMLError("HGB predictor node has multiple parents")
            reached.add(index)
            node = tree_nodes[index]
            if node["is_leaf"]:
                return
            in_path.add(index)
            visit(node["left"])
            visit(node["right"])
            in_path.remove(index)

        visit(0)
        if reachable != set(range(len(nodes))):
            raise AllCasesMLError("HGB predictor contains unreachable nodes")
        leaf_count = sum(bool(node["is_leaf"]) for node in nodes)
        if maximum_leaf_nodes is not None and (
            leaf_count > maximum_leaf_nodes or len(nodes) > 2 * maximum_leaf_nodes - 1
        ):
            raise AllCasesMLError("HGB tree exceeds its exact learner leaf/node cap")
    return value


def _deep_freeze_json(value: object) -> object:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise AllCasesMLError("canonical model JSON mapping key differs")
        return MappingProxyType({key: _deep_freeze_json(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze_json(item) for item in value)
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise AllCasesMLError("canonical model JSON value differs")


def _deep_thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _deep_thaw_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_deep_thaw_json(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class CanonicalMLModel:
    model_id: str
    candidate_id: str
    fit_recipe_id: str
    learner_recipe: Mapping[str, object]
    candidate: Mapping[str, object]
    feature_set_id: str
    feature_names: tuple[str, ...]
    response_kind: Literal["SIGNED_NORMALIZED_RETURN", "POSITIVE_NET_PROBABILITY"]
    null_world: str
    fold_key: str
    preprocessor: FrozenPreprocessor
    predictor: Mapping[str, object]
    admission_rate: Fraction
    admission_threshold: float
    training_row_count: int
    training_rows_sha256: str
    random_state: int
    base_strategy_id: str | None = None
    base_trigger_family: str | None = None
    symbolic_ranking_certificate: SymbolicRankingCertificate | None = None
    numpy_version: str = np.__version__
    sklearn_version: str = sklearn.__version__
    python_version: str = platform.python_version()

    def __post_init__(self) -> None:
        if (
            len(self.model_id) != 64
            or len(self.candidate_id) != 64
            or len(self.fit_recipe_id) != 64
            or any(character not in "0123456789abcdef" for character in self.model_id)
            or any(character not in "0123456789abcdef" for character in self.candidate_id)
            or any(character not in "0123456789abcdef" for character in self.fit_recipe_id)
        ):
            raise AllCasesMLError("canonical model identity differs")
        if self.feature_names != FEATURE_NAMES_BY_SET.get(self.feature_set_id):
            raise AllCasesMLError("canonical model feature set differs")
        if self.response_kind not in {"SIGNED_NORMALIZED_RETURN", "POSITIVE_NET_PROBABILITY"}:
            raise AllCasesMLError("canonical model response differs")
        if self.null_world not in NULL_WORLD_ORDER or self.fold_key not in (
            *SEARCH_OUTER_FOLD_KEYS,
            "SEARCH_FINAL",
        ):
            raise AllCasesMLError("canonical model world/fold differs")
        expected_candidate = (
            DIRECT_CANDIDATE_BY_ID.get(self.candidate_id)
            if self.response_kind == "SIGNED_NORMALIZED_RETURN"
            else META_CANDIDATE_BY_ID.get(self.candidate_id)
        )
        expected_rates = (
            DIRECT_ACTION_RATES
            if self.response_kind == "SIGNED_NORMALIZED_RETURN"
            else META_RETAIN_RATES
        )
        candidate_document = _deep_thaw_json(self.candidate)
        if expected_candidate is None or candidate_document != expected_candidate.as_dict():
            raise AllCasesMLError("canonical model candidate binding differs")
        expected_fit_recipe_id = (
            direct_fit_recipe_id(expected_candidate)
            if isinstance(expected_candidate, DirectCandidate)
            else meta_fit_recipe_id(expected_candidate)
        )
        if self.fit_recipe_id != expected_fit_recipe_id:
            raise AllCasesMLError("canonical model fit-recipe binding differs")
        expected_learner_recipe = learner_recipe_document(expected_candidate)
        learner_recipe = _deep_thaw_json(self.learner_recipe)
        if learner_recipe != expected_learner_recipe:
            raise AllCasesMLError("canonical model exact learner recipe differs")
        if self.feature_set_id != expected_candidate.feature_set_id:
            raise AllCasesMLError("canonical model candidate feature binding differs")
        if self.admission_rate not in expected_rates:
            raise AllCasesMLError("canonical model admission rate differs")
        if not math.isfinite(self.admission_threshold) or self.admission_threshold < 0:
            raise AllCasesMLError("canonical model admission threshold differs")
        if (
            self.training_row_count < 2
            or self.training_row_count > MAX_TRAINING_ROWS_PER_MODEL
            or len(self.training_rows_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.training_rows_sha256)
        ):
            raise AllCasesMLError("canonical model training identity differs")
        if (
            isinstance(self.random_state, bool)
            or not isinstance(self.random_state, int)
            or not 0 <= self.random_state <= 0xFFFF_FFFF
        ):
            raise AllCasesMLError("canonical model random state differs")
        if not self.numpy_version or not self.sklearn_version or not self.python_version:
            raise AllCasesMLError("canonical model library versions differ")
        learner_recipe_sha256 = canonical_sha256(expected_learner_recipe)
        expected_parameters = expected_learner_recipe["parameters"]
        if not isinstance(expected_parameters, dict):  # pragma: no cover - frozen recipe guard
            raise AllCasesMLError("canonical learner parameters differ")
        predictor_document = _deep_thaw_json(self.predictor)
        predictor = _validate_predictor(
            predictor_document,
            self.preprocessor.transformed_feature_count,
            expected_learner_recipe_sha256=learner_recipe_sha256,
            expected_tree_count=(
                expected_parameters.get("max_iter")
                if expected_learner_recipe["predictor_kind"]
                in {"HGB_REGRESSOR", "HGB_BINARY_CLASSIFIER"}
                else None
            ),
            maximum_leaf_nodes=(
                expected_parameters.get("max_leaf_nodes")
                if expected_learner_recipe["predictor_kind"]
                in {"HGB_REGRESSOR", "HGB_BINARY_CLASSIFIER"}
                else None
            ),
        )
        kind = predictor["kind"]
        expected_kind = (
            "ELASTIC_NET_REGRESSOR"
            if isinstance(expected_candidate, DirectCandidate)
            and expected_candidate.learner_id in {"ENET_A", "ENET_B"}
            else "HGB_REGRESSOR"
            if isinstance(expected_candidate, DirectCandidate)
            else "LOGISTIC_BINARY_CLASSIFIER"
            if expected_candidate.classifier_id == "META_ENET"
            else "HGB_BINARY_CLASSIFIER"
        )
        if kind != expected_kind:
            raise AllCasesMLError("candidate learner/predictor kind differs")
        linear = kind in {"ELASTIC_NET_REGRESSOR", "LOGISTIC_BINARY_CLASSIFIER"}
        if linear != (self.preprocessor.scaler_mean is not None):
            raise AllCasesMLError("candidate learner/preprocessor scaling differs")
        expected_random_state = _seed(self.fit_recipe_id, self.null_world, self.fold_key, "FIT")
        if self.random_state != expected_random_state:
            raise AllCasesMLError("canonical model derived random state differs")
        if (
            self.numpy_version != np.__version__
            or self.sklearn_version != sklearn.__version__
            or self.python_version != platform.python_version()
        ):
            raise AllCasesMLError("canonical model runtime version binding differs")
        if self.response_kind == "SIGNED_NORMALIZED_RETURN":
            if any(
                value is not None
                for value in (
                    self.base_strategy_id,
                    self.base_trigger_family,
                    self.symbolic_ranking_certificate,
                )
            ):
                raise AllCasesMLError("direct model cannot bind a symbolic strategy")
        else:
            for value in (self.base_strategy_id, self.symbolic_ranking_sha256):
                if (
                    value is None
                    or len(value) != 64
                    or any(character not in "0123456789abcdef" for character in value)
                ):
                    raise AllCasesMLError("meta model symbolic strategy binding differs")
            if not isinstance(self.base_trigger_family, str) or not self.base_trigger_family:
                raise AllCasesMLError("meta model base trigger family differs")
            certificate = self.symbolic_ranking_certificate
            if certificate is None:
                raise AllCasesMLError("meta model ranking certificate is missing")
            ranked_strategy = certificate.strategy_at_rank(expected_candidate.symbolic_rank_slot)
            if ranked_strategy is None:
                raise AllCasesMLError("meta model ranking certificate lacks its rank slot")
            if (
                certificate.null_world != self.null_world
                or certificate.fold_key != self.fold_key
                or self.base_strategy_id != ranked_strategy.strategy_id
                or self.base_trigger_family != ranked_strategy.trigger_family
            ):
                raise AllCasesMLError("meta model rank-slot certificate binding differs")
        expected_model_id = _model_id(
            candidate_id=self.candidate_id,
            world=NullWorld(self.null_world),
            fold_key=self.fold_key,
            row_sha256=self.training_rows_sha256,
            base_strategy_id=self.base_strategy_id,
            base_trigger_family=self.base_trigger_family,
            symbolic_ranking_sha256=self.symbolic_ranking_sha256,
        )
        if self.model_id != expected_model_id:
            raise AllCasesMLError("canonical model derived identity differs")
        object.__setattr__(self, "candidate", _deep_freeze_json(candidate_document))
        object.__setattr__(self, "learner_recipe", _deep_freeze_json(learner_recipe))
        object.__setattr__(self, "predictor", _deep_freeze_json(predictor_document))

    @property
    def symbolic_ranking_sha256(self) -> str | None:
        certificate = self.symbolic_ranking_certificate
        return None if certificate is None else certificate.artifact_sha256

    def as_dict(self) -> dict[str, object]:
        return {
            "admission_rate": _fraction_payload(self.admission_rate),
            "admission_threshold_hex": _float_hex(self.admission_threshold),
            "artifact_encoding": "CANONICAL_JSON_FLOAT_HEX_NO_PICKLE",
            "base_strategy_id": self.base_strategy_id,
            "base_trigger_family": self.base_trigger_family,
            "candidate": _deep_thaw_json(self.candidate),
            "candidate_id": self.candidate_id,
            "feature_names": list(self.feature_names),
            "feature_set_id": self.feature_set_id,
            "fit_recipe_id": self.fit_recipe_id,
            "learner_recipe": _deep_thaw_json(self.learner_recipe),
            "fold_key": self.fold_key,
            "model_id": self.model_id,
            "null_world": self.null_world,
            "numpy_version": self.numpy_version,
            "predictor": _deep_thaw_json(self.predictor),
            "preprocessor": self.preprocessor.as_dict(),
            "python_version": self.python_version,
            "random_state": self.random_state,
            "response_kind": self.response_kind,
            "schema": ML_SCHEMA,
            "sklearn_version": self.sklearn_version,
            "symbolic_ranking_certificate": (
                None
                if self.symbolic_ranking_certificate is None
                else self.symbolic_ranking_certificate.as_dict()
            ),
            "training_row_count": self.training_row_count,
            "training_rows_sha256": self.training_rows_sha256,
        }

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.as_dict())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def from_canonical_bytes(cls, raw: bytes) -> CanonicalMLModel:
        try:
            document = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise AllCasesMLError("model artifact is invalid JSON") from error
        keys = {
            "admission_rate",
            "admission_threshold_hex",
            "artifact_encoding",
            "base_strategy_id",
            "base_trigger_family",
            "candidate",
            "candidate_id",
            "feature_names",
            "feature_set_id",
            "fit_recipe_id",
            "learner_recipe",
            "fold_key",
            "model_id",
            "null_world",
            "numpy_version",
            "predictor",
            "preprocessor",
            "python_version",
            "random_state",
            "response_kind",
            "schema",
            "sklearn_version",
            "symbolic_ranking_certificate",
            "training_row_count",
            "training_rows_sha256",
        }
        if (
            not isinstance(document, dict)
            or set(document) != keys
            or document["schema"] != ML_SCHEMA
            or document["artifact_encoding"] != "CANONICAL_JSON_FLOAT_HEX_NO_PICKLE"
            or canonical_json_bytes(document) != raw
        ):
            raise AllCasesMLError("model artifact canonical schema differs")
        rate = document["admission_rate"]
        if not isinstance(rate, dict) or set(rate) != {"denominator", "numerator"}:
            raise AllCasesMLError("model admission rate encoding differs")
        string_keys = (
            "model_id",
            "candidate_id",
            "fit_recipe_id",
            "feature_set_id",
            "response_kind",
            "null_world",
            "fold_key",
            "training_rows_sha256",
            "numpy_version",
            "sklearn_version",
            "python_version",
        )
        if (
            any(not isinstance(document[key], str) for key in string_keys)
            or not isinstance(document["candidate"], dict)
            or not isinstance(document["learner_recipe"], dict)
            or not isinstance(document["feature_names"], list)
            or any(not isinstance(item, str) for item in document["feature_names"])
            or isinstance(document["training_row_count"], bool)
            or not isinstance(document["training_row_count"], int)
            or isinstance(document["random_state"], bool)
            or not isinstance(document["random_state"], int)
            or isinstance(rate["numerator"], bool)
            or not isinstance(rate["numerator"], int)
            or isinstance(rate["denominator"], bool)
            or not isinstance(rate["denominator"], int)
            or document["base_strategy_id"] is not None
            and not isinstance(document["base_strategy_id"], str)
            or document["base_trigger_family"] is not None
            and not isinstance(document["base_trigger_family"], str)
        ):
            raise AllCasesMLError("model artifact value types differ")
        preprocessor = FrozenPreprocessor.from_dict(document["preprocessor"])
        certificate_document = document["symbolic_ranking_certificate"]
        certificate = (
            None
            if certificate_document is None
            else SymbolicRankingCertificate.from_dict(certificate_document)
        )
        try:
            model = cls(
                document["model_id"],
                document["candidate_id"],
                document["fit_recipe_id"],
                document["learner_recipe"],
                document["candidate"],
                document["feature_set_id"],
                tuple(document["feature_names"]),
                document["response_kind"],
                document["null_world"],
                document["fold_key"],
                preprocessor,
                _validate_predictor(document["predictor"], preprocessor.transformed_feature_count),
                Fraction(rate["numerator"], rate["denominator"]),
                _decode_float_hex(document["admission_threshold_hex"], label="admission threshold"),
                document["training_row_count"],
                document["training_rows_sha256"],
                document["random_state"],
                document["base_strategy_id"],
                document["base_trigger_family"],
                certificate,
                document["numpy_version"],
                document["sklearn_version"],
                document["python_version"],
            )
        except AllCasesMLError:
            raise
        except (TypeError, ValueError, ZeroDivisionError) as error:
            raise AllCasesMLError("model artifact values differ") from error
        if model.canonical_bytes != raw:
            raise AllCasesMLError("model artifact did not round trip exactly")
        return model

    def predict_scores(self, values: np.ndarray) -> np.ndarray:
        transformed = self.preprocessor.transform(values)
        predictor = _deep_thaw_json(self.predictor)
        if not isinstance(predictor, dict):  # pragma: no cover - model invariant
            raise AllCasesMLError("canonical predictor mapping differs")
        kind = predictor["kind"]
        if kind in {"ELASTIC_NET_REGRESSOR", "LOGISTIC_BINARY_CLASSIFIER"}:
            coefficient = np.asarray(
                [
                    _decode_float_hex(item, label="coefficient")
                    for item in predictor["coefficient_hex"]
                ]
            )
            raw = transformed @ coefficient + _decode_float_hex(
                predictor["intercept_hex"], label="intercept"
            )
        else:
            raw = np.full(
                transformed.shape[0],
                _decode_float_hex(predictor["baseline_hex"], label="HGB baseline"),
                dtype=np.float64,
            )
            for nodes in predictor["trees"]:
                for row_index, row in enumerate(transformed):
                    node_index = 0
                    while not nodes[node_index]["is_leaf"]:
                        node = nodes[node_index]
                        value = row[node["feature_idx"]]
                        go_left = (
                            node["missing_go_to_left"]
                            if math.isnan(float(value))
                            else value
                            <= _decode_float_hex(node["threshold_hex"], label="HGB threshold")
                        )
                        node_index = node["left"] if go_left else node["right"]
                    raw[row_index] += _decode_float_hex(
                        nodes[node_index]["value_hex"], label="HGB leaf"
                    )
        if kind in {"LOGISTIC_BINARY_CLASSIFIER", "HGB_BINARY_CLASSIFIER"}:
            positive = raw >= 0
            result = np.empty_like(raw)
            result[positive] = 1.0 / (1.0 + np.exp(-raw[positive]))
            exponent = np.exp(raw[~positive])
            result[~positive] = exponent / (1.0 + exponent)
            return result
        return raw


@dataclass(frozen=True, slots=True)
class _CachedFitState:
    fit_recipe_id: str
    response_kind: Literal["SIGNED_NORMALIZED_RETURN", "POSITIVE_NET_PROBABILITY"]
    feature_set_id: str
    feature_names: tuple[str, ...]
    null_world: str
    fold_key: str
    preprocessor: FrozenPreprocessor
    predictor: Mapping[str, object]
    training_scores: tuple[float, ...]
    training_row_count: int
    training_rows_sha256: str
    random_state: int
    base_strategy_id: str | None
    base_trigger_family: str | None
    symbolic_ranking_certificate: SymbolicRankingCertificate | None


def _retained_python_bytes(value: object, seen: set[int] | None = None) -> int:
    """Conservatively count the reachable Python payload retained by a cache state."""

    visited = set() if seen is None else seen
    identity = id(value)
    if identity in visited:
        return 0
    visited.add(identity)
    size = sys.getsizeof(value)
    if isinstance(value, Mapping):
        return size + sum(
            _retained_python_bytes(key, visited) + _retained_python_bytes(item, visited)
            for key, item in value.items()
        )
    if isinstance(value, (tuple, list, set, frozenset, deque)):
        return size + sum(_retained_python_bytes(item, visited) for item in value)
    if is_dataclass(value) and not isinstance(value, type):
        return size + sum(
            _retained_python_bytes(getattr(value, item.name), visited) for item in fields(value)
        )
    return size


@dataclass(frozen=True, slots=True)
class SharedFitCacheEvidence:
    """Terminal proof that every rate-shared state was hit or explicitly discarded."""

    maximum_fit_count: int
    fit_count: int
    cache_hits: int
    discarded_state_count: int
    eviction_count: int
    final_state_count: int
    peak_state_count: int
    final_retained_bytes: int
    peak_retained_bytes: int

    def __post_init__(self) -> None:
        integers = tuple(getattr(self, item.name) for item in fields(self))
        if (
            any(
                isinstance(value, bool) or not isinstance(value, int) or value < 0
                for value in integers
            )
            or self.maximum_fit_count <= 0
            or self.fit_count > self.maximum_fit_count
            or self.cache_hits + self.discarded_state_count != self.fit_count
            or self.eviction_count != self.fit_count
            or self.final_state_count != 0
            or self.final_retained_bytes != 0
            or (
                self.fit_count == 0
                and (self.peak_state_count != 0 or self.peak_retained_bytes != 0)
            )
            or (
                self.fit_count > 0
                and (not 1 <= self.peak_state_count <= 21 or self.peak_retained_bytes <= 0)
            )
        ):
            raise AllCasesMLError("shared fit cache terminal evidence differs")

    def as_dict(self) -> dict[str, object]:
        return {
            "cache_hits": self.cache_hits,
            "discarded_state_count": self.discarded_state_count,
            "eviction_count": self.eviction_count,
            "final_retained_bytes": self.final_retained_bytes,
            "final_state_count": self.final_state_count,
            "fit_count": self.fit_count,
            "maximum_fit_count": self.maximum_fit_count,
            "peak_retained_bytes": self.peak_retained_bytes,
            "peak_state_count": self.peak_state_count,
            "schema": "systematic_fx.ai_all_cases_shared_fit_cache_evidence.v1",
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> SharedFitCacheEvidence:
        expected_keys = {
            "cache_hits",
            "discarded_state_count",
            "eviction_count",
            "final_retained_bytes",
            "final_state_count",
            "fit_count",
            "maximum_fit_count",
            "peak_retained_bytes",
            "peak_state_count",
            "schema",
        }
        if set(value) != expected_keys:
            raise AllCasesMLError("shared fit cache evidence keys differ")
        try:
            result = cls(
                value["maximum_fit_count"],
                value["fit_count"],
                value["cache_hits"],
                value["discarded_state_count"],
                value["eviction_count"],
                value["final_state_count"],
                value["peak_state_count"],
                value["final_retained_bytes"],
                value["peak_retained_bytes"],
            )
        except TypeError as error:
            raise AllCasesMLError("shared fit cache evidence value differs") from error
        if result.as_dict() != value:
            raise AllCasesMLError("shared fit cache evidence did not round trip")
        return result


@dataclass(frozen=True, slots=True)
class SharedFitCacheAggregateEvidence:
    """Resume-safe aggregate of the 24 independently drained chunk caches."""

    candidate_kind: str
    chunks: tuple[SharedFitCacheEvidence, ...]

    def __post_init__(self) -> None:
        maximum_per_chunk = {"DIRECT": 126, "META": 84}
        maximum_total = {"DIRECT": 3_024, "META": 2_016}
        if (
            self.candidate_kind not in maximum_per_chunk
            or len(self.chunks) != 24
            or any(
                item.maximum_fit_count != maximum_per_chunk[self.candidate_kind]
                for item in self.chunks
            )
            or sum(item.fit_count for item in self.chunks) > maximum_total[self.candidate_kind]
            or sum(item.cache_hits for item in self.chunks)
            + sum(item.discarded_state_count for item in self.chunks)
            != sum(item.fit_count for item in self.chunks)
            or sum(item.eviction_count for item in self.chunks)
            != sum(item.fit_count for item in self.chunks)
        ):
            raise AllCasesMLError("shared fit cache chunk aggregate differs")

    def as_dict(self) -> dict[str, object]:
        document = {
            "cache_hits": sum(item.cache_hits for item in self.chunks),
            "candidate_kind": self.candidate_kind,
            "chunk_count": len(self.chunks),
            "chunks": [item.as_dict() for item in self.chunks],
            "eviction_count": sum(item.eviction_count for item in self.chunks),
            "discarded_state_count": sum(item.discarded_state_count for item in self.chunks),
            "final_retained_bytes": sum(item.final_retained_bytes for item in self.chunks),
            "final_state_count": sum(item.final_state_count for item in self.chunks),
            "fit_count": sum(item.fit_count for item in self.chunks),
            "maximum_chunk_peak_retained_bytes": max(
                item.peak_retained_bytes for item in self.chunks
            ),
            "maximum_chunk_peak_state_count": max(item.peak_state_count for item in self.chunks),
            "maximum_fit_count": sum(item.maximum_fit_count for item in self.chunks),
            "schema": "systematic_fx.ai_all_cases_shared_fit_cache_aggregate.v1",
        }
        return {**document, "artifact_sha256": canonical_sha256(document)}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> SharedFitCacheAggregateEvidence:
        expected_keys = {
            "artifact_sha256",
            "cache_hits",
            "candidate_kind",
            "chunk_count",
            "chunks",
            "discarded_state_count",
            "eviction_count",
            "final_retained_bytes",
            "final_state_count",
            "fit_count",
            "maximum_chunk_peak_retained_bytes",
            "maximum_chunk_peak_state_count",
            "maximum_fit_count",
            "schema",
        }
        chunks = value.get("chunks")
        if set(value) != expected_keys or not isinstance(chunks, list):
            raise AllCasesMLError("shared fit cache aggregate keys differ")
        try:
            result = cls(
                value["candidate_kind"],
                tuple(SharedFitCacheEvidence.from_dict(item) for item in chunks),
            )
        except TypeError as error:
            raise AllCasesMLError("shared fit cache aggregate value differs") from error
        if result.as_dict() != value:
            raise AllCasesMLError("shared fit cache aggregate did not round trip")
        return result


def aggregate_shared_fit_cache_evidence(
    candidate_kind: str,
    chunks: Sequence[SharedFitCacheEvidence],
) -> SharedFitCacheAggregateEvidence:
    """Build the strict resume-safe terminal cache proof for one ML phase."""

    return SharedFitCacheAggregateEvidence(candidate_kind, tuple(chunks))


class SharedFitCache:
    """Explicit in-memory reuse of fitted state across rate-only variants."""

    __slots__ = (
        "_consumed_keys",
        "_state_bytes",
        "_states",
        "cache_hits",
        "current_retained_bytes",
        "discarded_state_count",
        "eviction_count",
        "fit_count",
        "peak_retained_bytes",
        "peak_state_count",
    )

    def __init__(self) -> None:
        self._states: dict[tuple[str, str, str, str], _CachedFitState] = {}
        self._state_bytes: dict[tuple[str, str, str, str], int] = {}
        self._consumed_keys: set[tuple[str, str, str, str]] = set()
        self.fit_count = 0
        self.cache_hits = 0
        self.discarded_state_count = 0
        self.eviction_count = 0
        self.current_retained_bytes = 0
        self.peak_state_count = 0
        self.peak_retained_bytes = 0

    @staticmethod
    def _key(
        fit_recipe_id: str,
        null_world: str,
        fold_key: str,
        training_rows_sha256: str,
    ) -> tuple[str, str, str, str]:
        return (fit_recipe_id, null_world, fold_key, training_rows_sha256)

    def _get(
        self,
        fit_recipe_id: str,
        null_world: str,
        fold_key: str,
        training_rows_sha256: str,
    ) -> _CachedFitState | None:
        key = self._key(fit_recipe_id, null_world, fold_key, training_rows_sha256)
        if key in self._consumed_keys:
            raise AllCasesMLError("shared fit cache key was requested beyond its two variants")
        foreign_keys = tuple(
            sorted(
                retained_key for retained_key in self._states if retained_key[0] != fit_recipe_id
            )
        )
        self._discard_keys(foreign_keys)
        state = self._states.pop(key, None)
        if state is not None:
            self.cache_hits += 1
            self.eviction_count += 1
            self.current_retained_bytes -= self._state_bytes.pop(key)
            self._consumed_keys.add(key)
        return state

    def _store(self, state: _CachedFitState) -> None:
        key = self._key(
            state.fit_recipe_id,
            state.null_world,
            state.fold_key,
            state.training_rows_sha256,
        )
        if key in self._states or key in self._consumed_keys:
            raise AllCasesMLError("shared fit cache key was stored twice")
        foreign_keys = tuple(
            sorted(
                retained_key
                for retained_key in self._states
                if retained_key[0] != state.fit_recipe_id
            )
        )
        self._discard_keys(foreign_keys)
        if len(self._states) >= 21:
            raise AllCasesMLError("shared fit cache exceeded the 21-scope live bound")
        retained_bytes = _retained_python_bytes(state)
        self._states[key] = state
        self._state_bytes[key] = retained_bytes
        self.fit_count += 1
        self.current_retained_bytes += retained_bytes
        self.peak_state_count = max(self.peak_state_count, len(self._states))
        self.peak_retained_bytes = max(
            self.peak_retained_bytes,
            self.current_retained_bytes,
        )

    @property
    def state_count(self) -> int:
        return len(self._states)

    def _discard_keys(self, keys: Sequence[tuple[str, str, str, str]]) -> int:
        for key in keys:
            if key not in self._states:
                raise AllCasesMLError("shared fit cache discard key is absent")
            self._states.pop(key)
            self.current_retained_bytes -= self._state_bytes.pop(key)
            self._consumed_keys.add(key)
        discarded = len(keys)
        self.discarded_state_count += discarded
        self.eviction_count += discarded
        if self.current_retained_bytes < 0:
            raise AllCasesMLError("shared fit cache discard accounting differs")
        return discarded

    def discard_unconsumed_states(self) -> int:
        """Drain one-sided rate states and record them as explicit discards."""

        keys = tuple(sorted(self._states))
        discarded = self._discard_keys(keys)
        if self.current_retained_bytes != 0 or self._state_bytes:
            raise AllCasesMLError("shared fit cache discard accounting differs")
        return discarded

    def terminal_evidence(self, *, maximum_fit_count: int) -> SharedFitCacheEvidence:
        """Fail closed unless the exact two-rate catalog traversal drained the cache."""

        return SharedFitCacheEvidence(
            maximum_fit_count,
            self.fit_count,
            self.cache_hits,
            self.discarded_state_count,
            self.eviction_count,
            self.state_count,
            self.peak_state_count,
            self.current_retained_bytes,
            self.peak_retained_bytes,
        )


def _model_from_cached_state(
    candidate: DirectCandidate | MetaCandidate,
    state: _CachedFitState,
) -> CanonicalMLModel:
    direct = isinstance(candidate, DirectCandidate)
    expected_response = "SIGNED_NORMALIZED_RETURN" if direct else "POSITIVE_NET_PROBABILITY"
    expected_recipe_id = (
        direct_fit_recipe_id(candidate) if direct else meta_fit_recipe_id(candidate)
    )
    if (
        state.response_kind != expected_response
        or state.fit_recipe_id != expected_recipe_id
        or state.feature_set_id != candidate.feature_set_id
    ):
        raise AllCasesMLError("shared fitted state/candidate binding differs")
    admission_rate = candidate.action_rate if direct else candidate.retain_rate
    threshold = _admission_threshold(
        np.asarray(state.training_scores),
        admission_rate,
        absolute=direct,
    )
    return CanonicalMLModel(
        model_id=_model_id(
            candidate_id=candidate.candidate_id,
            world=NullWorld(state.null_world),
            fold_key=state.fold_key,
            row_sha256=state.training_rows_sha256,
            base_strategy_id=state.base_strategy_id,
            base_trigger_family=state.base_trigger_family,
            symbolic_ranking_sha256=(
                None
                if state.symbolic_ranking_certificate is None
                else state.symbolic_ranking_certificate.artifact_sha256
            ),
        ),
        candidate_id=candidate.candidate_id,
        fit_recipe_id=state.fit_recipe_id,
        learner_recipe=learner_recipe_document(candidate),
        candidate=candidate.as_dict(),
        feature_set_id=state.feature_set_id,
        feature_names=state.feature_names,
        response_kind=state.response_kind,
        null_world=state.null_world,
        fold_key=state.fold_key,
        preprocessor=state.preprocessor,
        predictor=state.predictor,
        admission_rate=admission_rate,
        admission_threshold=threshold,
        training_row_count=state.training_row_count,
        training_rows_sha256=state.training_rows_sha256,
        random_state=state.random_state,
        base_strategy_id=state.base_strategy_id,
        base_trigger_family=state.base_trigger_family,
        symbolic_ranking_certificate=state.symbolic_ranking_certificate,
    )


def _ensure_fitting_runtime() -> None:
    if sklearn.__version__ != EXPECTED_SKLEARN_VERSION:
        raise AllCasesMLError(
            f"model fitting requires sklearn {EXPECTED_SKLEARN_VERSION}, got {sklearn.__version__}"
        )


def _normalized_world(world: NullWorld | str) -> NullWorld:
    try:
        return NullWorld(world)
    except ValueError as error:
        raise AllCasesMLError("unknown null world") from error


def _training_matrix_with_targets(
    matrix: TrainingMatrix,
    *,
    world: NullWorld,
    training_targets: np.ndarray | None,
) -> TrainingMatrix:
    if world is NullWorld.REAL:
        if training_targets is not None:
            raise AllCasesMLError("REAL fit cannot accept replacement targets")
        return matrix
    if training_targets is None:
        raise AllCasesMLError("null-world fit requires explicit training-only targets")
    targets = np.asarray(training_targets, dtype=np.float64)
    if targets.shape != (matrix.row_count,) or not np.isfinite(targets).all():
        raise AllCasesMLError("replacement training targets differ")
    return matrix.take(tuple(range(matrix.row_count)), targets=targets)


def _linear_predictor_document(
    coefficient: np.ndarray,
    intercept: float,
    *,
    classifier: bool,
    learner_recipe_sha256: str,
) -> dict[str, object]:
    values = np.asarray(coefficient, dtype=np.float64)
    if values.ndim != 1 or not np.isfinite(values).all() or not math.isfinite(intercept):
        raise AllCasesMLError("linear fitted state is non-finite")
    return {
        "coefficient_hex": [_float_hex(float(value)) for value in values],
        "intercept_hex": _float_hex(intercept),
        "kind": "LOGISTIC_BINARY_CLASSIFIER" if classifier else "ELASTIC_NET_REGRESSOR",
        "learner_recipe_sha256": learner_recipe_sha256,
    }


def _admission_threshold(scores: np.ndarray, rate: Fraction, *, absolute: bool) -> float:
    values = np.asarray(scores, dtype=np.float64)
    if values.ndim != 1 or not len(values) or not np.isfinite(values).all():
        raise AllCasesMLError("training scores differ")
    ranked = np.abs(values) if absolute else values
    threshold = float(np.quantile(ranked, 1.0 - float(rate), method="higher"))
    if not math.isfinite(threshold) or threshold < 0:
        raise AllCasesMLError("training admission threshold differs")
    return threshold


def _model_id(
    *,
    candidate_id: str,
    world: NullWorld,
    fold_key: str,
    row_sha256: str,
    base_strategy_id: str | None,
    base_trigger_family: str | None,
    symbolic_ranking_sha256: str | None,
) -> str:
    return canonical_sha256(
        {
            "base_strategy_id": base_strategy_id,
            "base_trigger_family": base_trigger_family,
            "candidate_id": candidate_id,
            "fold_key": fold_key,
            "row_sha256": row_sha256,
            "schema": "systematic_fx.ai_all_cases_ml_model_identity.v1",
            "symbolic_ranking_sha256": symbolic_ranking_sha256,
            "world": world.value,
        }
    )


def _check_fold_key(fold_key: str) -> None:
    if fold_key not in (*SEARCH_OUTER_FOLD_KEYS, "SEARCH_FINAL"):
        raise AllCasesMLError("model fitting is restricted to frozen Search fold keys")


def fit_direct_model(
    candidate: DirectCandidate,
    matrix: TrainingMatrix,
    *,
    world: NullWorld | str = NullWorld.REAL,
    fold_key: str = "SEARCH_FINAL",
    training_targets: np.ndarray | None = None,
    cache: SharedFitCache | None = None,
) -> CanonicalMLModel:
    """Fit one direct Search model; null targets must already be training-only."""

    _ensure_fitting_runtime()
    _check_fold_key(fold_key)
    expected = DIRECT_CANDIDATE_BY_ID.get(candidate.candidate_id)
    if expected != candidate:
        raise AllCasesMLError("direct candidate is not in the frozen catalog")
    if (
        matrix.feature_set_id != candidate.feature_set_id
        or matrix.task_timeframe_seconds != candidate.decision_timeframe_seconds
        or matrix.task_horizon_seconds != candidate.horizon_seconds
        or matrix.base_directions is not None
        or matrix.base_strategy_id is not None
        or matrix.base_trigger_family is not None
        or matrix.symbolic_ranking_sha256 is not None
    ):
        raise AllCasesMLError("direct matrix/candidate task binding differs")
    normalized_world = _normalized_world(world)
    training = _training_matrix_with_targets(
        matrix,
        world=normalized_world,
        training_targets=training_targets,
    )
    fit_recipe_id = direct_fit_recipe_id(candidate)
    learner_recipe_sha256 = canonical_sha256(learner_recipe_document(candidate))
    row_sha256 = training_rows_sha256(training)
    if cache is not None:
        cached = cache._get(
            fit_recipe_id,
            normalized_world.value,
            fold_key,
            row_sha256,
        )
        if cached is not None:
            return _model_from_cached_state(candidate, cached)
    random_state = _seed(fit_recipe_id, normalized_world.value, fold_key, "FIT")
    linear = candidate.learner_id in {"ENET_A", "ENET_B"}
    preprocessor = FrozenPreprocessor.fit(training.values, scale=linear)
    transformed = preprocessor.transform(training.values)
    if transformed.shape[1] > MAX_TRANSFORMED_FEATURES:
        raise AllCasesMLError("transformed feature cap exceeded")

    with warnings.catch_warnings():
        warnings.simplefilter("error", ConvergenceWarning)
        try:
            if candidate.learner_id == "ENET_A":
                estimator: object = ElasticNet(
                    alpha=0.001,
                    l1_ratio=0.1,
                    fit_intercept=True,
                    max_iter=50_000,
                    tol=1e-8,
                    selection="cyclic",
                    random_state=random_state,
                ).fit(transformed, training.targets)
            elif candidate.learner_id == "ENET_B":
                estimator = ElasticNet(
                    alpha=0.01,
                    l1_ratio=0.5,
                    fit_intercept=True,
                    max_iter=50_000,
                    tol=1e-8,
                    selection="cyclic",
                    random_state=random_state,
                ).fit(transformed, training.targets)
            elif candidate.learner_id in {"HGB_7", "HGB_15"}:
                estimator = HistGradientBoostingRegressor(
                    loss="squared_error",
                    learning_rate=0.05,
                    max_iter=200,
                    max_leaf_nodes=7 if candidate.learner_id == "HGB_7" else 15,
                    min_samples_leaf=40,
                    l2_regularization=1.0,
                    max_bins=255,
                    early_stopping=False,
                    random_state=random_state,
                ).fit(transformed, training.targets)
            else:  # pragma: no cover - catalog invariant
                raise AllCasesMLError("unknown direct learner")
        except ConvergenceWarning as error:
            raise MLCandidateIneligible(
                MLIneligibilityReason.MODEL_NONCONVERGENCE,
                "direct model did not converge",
                candidate_id=candidate.candidate_id,
                scope_key=fold_key,
            ) from error

    if linear:
        iterations = int(getattr(estimator, "n_iter_", 50_000))
        if iterations >= 50_000:
            raise MLCandidateIneligible(
                MLIneligibilityReason.MODEL_NONCONVERGENCE,
                "direct linear model reached its iteration cap",
                candidate_id=candidate.candidate_id,
                scope_key=fold_key,
            )
        predictor = _linear_predictor_document(
            np.asarray(estimator.coef_),
            float(estimator.intercept_),
            classifier=False,
            learner_recipe_sha256=learner_recipe_sha256,
        )
    else:
        if int(getattr(estimator, "n_iter_", -1)) != 200:
            raise AllCasesMLError("direct HGB iteration count differs")
        predictor = _hgb_predictor_document(
            estimator,
            classifier=False,
            learner_recipe_sha256=learner_recipe_sha256,
        )

    provisional = CanonicalMLModel(
        model_id=_model_id(
            candidate_id=candidate.candidate_id,
            world=normalized_world,
            fold_key=fold_key,
            row_sha256=row_sha256,
            base_strategy_id=None,
            base_trigger_family=None,
            symbolic_ranking_sha256=None,
        ),
        candidate_id=candidate.candidate_id,
        fit_recipe_id=fit_recipe_id,
        learner_recipe=learner_recipe_document(candidate),
        candidate=candidate.as_dict(),
        feature_set_id=candidate.feature_set_id,
        feature_names=matrix.feature_names,
        response_kind="SIGNED_NORMALIZED_RETURN",
        null_world=normalized_world.value,
        fold_key=fold_key,
        preprocessor=preprocessor,
        predictor=predictor,
        admission_rate=candidate.action_rate,
        admission_threshold=0.0,
        training_row_count=training.row_count,
        training_rows_sha256=row_sha256,
        random_state=random_state,
    )
    portable_scores = provisional.predict_scores(training.values)
    estimator_scores = np.asarray(estimator.predict(transformed), dtype=np.float64)
    if not np.allclose(portable_scores, estimator_scores, rtol=0.0, atol=1e-12):
        raise AllCasesMLError("portable direct predictor differs from sklearn")
    state = _CachedFitState(
        fit_recipe_id,
        "SIGNED_NORMALIZED_RETURN",
        candidate.feature_set_id,
        matrix.feature_names,
        normalized_world.value,
        fold_key,
        preprocessor,
        predictor,
        tuple(float(value) for value in portable_scores),
        training.row_count,
        row_sha256,
        random_state,
        None,
        None,
        None,
    )
    if cache is not None:
        cache._store(state)
    return _model_from_cached_state(candidate, state)


def fit_meta_model(
    candidate: MetaCandidate,
    matrix: TrainingMatrix,
    *,
    ranking_training_dates: Sequence[date],
    world: NullWorld | str = NullWorld.REAL,
    fold_key: str = "SEARCH_FINAL",
    training_targets: np.ndarray | None = None,
    cache: SharedFitCache | None = None,
) -> CanonicalMLModel:
    """Fit a gate for one prior-prefix-ranked complete symbolic strategy."""

    _ensure_fitting_runtime()
    _check_fold_key(fold_key)
    expected = META_CANDIDATE_BY_ID.get(candidate.candidate_id)
    if expected != candidate:
        raise AllCasesMLError("meta candidate is not in the frozen catalog")
    if (
        matrix.feature_set_id != candidate.feature_set_id
        or matrix.base_directions is None
        or matrix.realized_net_ticks is None
        or matrix.base_strategy_id is None
        or matrix.base_trigger_family is None
        or matrix.symbolic_ranking_certificate is None
    ):
        raise AllCasesMLError("meta matrix/candidate symbolic binding differs")
    normalized_world = _normalized_world(world)
    certificate = matrix.symbolic_ranking_certificate
    ranked_strategy = _require_ranked_strategy(certificate, candidate, scope_key=fold_key)
    matrix_dates = tuple(sorted(set(matrix.decision_dates)))
    expected_ranking_dates = tuple(ranking_training_dates)
    if (
        certificate.null_world != normalized_world.value
        or certificate.fold_key != fold_key
        or not expected_ranking_dates
        or tuple(sorted(set(expected_ranking_dates))) != expected_ranking_dates
        or certificate.training_dates != expected_ranking_dates
        or matrix.base_strategy_id != ranked_strategy.strategy_id
        or matrix.base_trigger_family != ranked_strategy.trigger_family
        or not set(matrix_dates).issubset(certificate.training_dates)
    ):
        raise AllCasesMLError("meta matrix ranking was not rebuilt for this null world")
    training = _training_matrix_with_targets(
        matrix,
        world=normalized_world,
        training_targets=training_targets,
    )
    classes = {float(value) for value in training.targets}
    if classes != {0.0, 1.0}:
        raise MLCandidateIneligible(
            MLIneligibilityReason.SINGLE_CLASS_TRAINING_TARGET,
            "meta target must contain both binary classes",
            candidate_id=candidate.candidate_id,
            scope_key=fold_key,
        )
    fit_recipe_id = meta_fit_recipe_id(candidate)
    learner_recipe_sha256 = canonical_sha256(learner_recipe_document(candidate))
    row_sha256 = training_rows_sha256(training)
    if cache is not None:
        cached = cache._get(
            fit_recipe_id,
            normalized_world.value,
            fold_key,
            row_sha256,
        )
        if cached is not None:
            return _model_from_cached_state(candidate, cached)
    random_state = _seed(fit_recipe_id, normalized_world.value, fold_key, "FIT")
    linear = candidate.classifier_id == "META_ENET"
    preprocessor = FrozenPreprocessor.fit(training.values, scale=linear)
    transformed = preprocessor.transform(training.values)
    if transformed.shape[1] > MAX_TRANSFORMED_FEATURES:
        raise AllCasesMLError("transformed feature cap exceeded")

    with warnings.catch_warnings():
        warnings.simplefilter("error", ConvergenceWarning)
        warnings.filterwarnings(
            "ignore",
            message="'penalty' was deprecated.*",
            category=FutureWarning,
        )
        warnings.filterwarnings(
            "ignore",
            message="'n_jobs' has no effect.*",
            category=FutureWarning,
        )
        try:
            if candidate.classifier_id == "META_ENET":
                estimator: object = LogisticRegression(
                    solver="liblinear",
                    penalty="l2",
                    C=0.1,
                    class_weight="balanced",
                    fit_intercept=True,
                    max_iter=50_000,
                    tol=1e-8,
                    random_state=random_state,
                    n_jobs=1,
                ).fit(transformed, training.targets.astype(np.int8))
            elif candidate.classifier_id == "META_HGB_7":
                estimator = HistGradientBoostingClassifier(
                    loss="log_loss",
                    learning_rate=0.05,
                    max_iter=200,
                    max_leaf_nodes=7,
                    min_samples_leaf=40,
                    l2_regularization=1.0,
                    max_bins=255,
                    early_stopping=False,
                    random_state=random_state,
                ).fit(transformed, training.targets.astype(np.int8))
            else:  # pragma: no cover - catalog invariant
                raise AllCasesMLError("unknown meta classifier")
        except ConvergenceWarning as error:
            raise MLCandidateIneligible(
                MLIneligibilityReason.MODEL_NONCONVERGENCE,
                "meta model did not converge",
                candidate_id=candidate.candidate_id,
                scope_key=fold_key,
            ) from error

    if linear:
        iterations = int(np.max(np.asarray(getattr(estimator, "n_iter_", [50_000]))))
        if iterations >= 50_000:
            raise MLCandidateIneligible(
                MLIneligibilityReason.MODEL_NONCONVERGENCE,
                "meta linear model reached its iteration cap",
                candidate_id=candidate.candidate_id,
                scope_key=fold_key,
            )
        coefficient = np.asarray(estimator.coef_, dtype=np.float64)
        intercept = np.asarray(estimator.intercept_, dtype=np.float64)
        if coefficient.shape != (1, transformed.shape[1]) or intercept.shape != (1,):
            raise AllCasesMLError("meta logistic fitted shape differs")
        predictor = _linear_predictor_document(
            coefficient[0],
            float(intercept[0]),
            classifier=True,
            learner_recipe_sha256=learner_recipe_sha256,
        )
    else:
        if int(getattr(estimator, "n_iter_", -1)) != 200:
            raise AllCasesMLError("meta HGB iteration count differs")
        predictor = _hgb_predictor_document(
            estimator,
            classifier=True,
            learner_recipe_sha256=learner_recipe_sha256,
        )

    provisional = CanonicalMLModel(
        model_id=_model_id(
            candidate_id=candidate.candidate_id,
            world=normalized_world,
            fold_key=fold_key,
            row_sha256=row_sha256,
            base_strategy_id=matrix.base_strategy_id,
            base_trigger_family=matrix.base_trigger_family,
            symbolic_ranking_sha256=matrix.symbolic_ranking_sha256,
        ),
        candidate_id=candidate.candidate_id,
        fit_recipe_id=fit_recipe_id,
        learner_recipe=learner_recipe_document(candidate),
        candidate=candidate.as_dict(),
        feature_set_id=candidate.feature_set_id,
        feature_names=matrix.feature_names,
        response_kind="POSITIVE_NET_PROBABILITY",
        null_world=normalized_world.value,
        fold_key=fold_key,
        preprocessor=preprocessor,
        predictor=predictor,
        admission_rate=candidate.retain_rate,
        admission_threshold=0.0,
        training_row_count=training.row_count,
        training_rows_sha256=row_sha256,
        random_state=random_state,
        base_strategy_id=matrix.base_strategy_id,
        base_trigger_family=matrix.base_trigger_family,
        symbolic_ranking_certificate=certificate,
    )
    portable_scores = provisional.predict_scores(training.values)
    estimator_scores = np.asarray(estimator.predict_proba(transformed)[:, 1], dtype=np.float64)
    if not np.allclose(portable_scores, estimator_scores, rtol=0.0, atol=1e-12):
        raise AllCasesMLError("portable meta predictor differs from sklearn")
    state = _CachedFitState(
        fit_recipe_id,
        "POSITIVE_NET_PROBABILITY",
        candidate.feature_set_id,
        matrix.feature_names,
        normalized_world.value,
        fold_key,
        preprocessor,
        predictor,
        tuple(float(value) for value in portable_scores),
        training.row_count,
        row_sha256,
        random_state,
        matrix.base_strategy_id,
        matrix.base_trigger_family,
        certificate,
    )
    if cache is not None:
        cache._store(state)
    return _model_from_cached_state(candidate, state)


@dataclass(frozen=True, slots=True)
class PredictionBatch:
    model_sha256: str
    candidate_id: str
    row_ids: tuple[str, ...]
    scores: tuple[float, ...]
    requested: tuple[bool, ...]
    admitted: tuple[bool, ...]
    directions: tuple[TradeDirection, ...]
    estimated_net_edge_ticks: tuple[float | None, ...]
    source_admitted: tuple[bool, ...] | None = None
    source_directions: tuple[TradeDirection, ...] | None = None

    def __post_init__(self) -> None:
        count = len(self.row_ids)
        source_admitted = self.admitted if self.source_admitted is None else self.source_admitted
        source_directions = (
            self.directions if self.source_directions is None else self.source_directions
        )
        object.__setattr__(self, "source_admitted", tuple(source_admitted))
        object.__setattr__(self, "source_directions", tuple(source_directions))
        if (
            len(self.model_sha256) != 64
            or len(self.candidate_id) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.model_sha256 + self.candidate_id
            )
            or not count
            or any(not isinstance(value, str) or not value for value in self.row_ids)
            or len(set(self.row_ids)) != count
            or len(self.scores) != count
            or len(self.requested) != count
            or len(self.admitted) != count
            or len(source_admitted) != count
            or any(
                admitted and not requested
                for requested, admitted in zip(self.requested, self.admitted, strict=True)
            )
            or any(
                admitted and not requested
                for requested, admitted in zip(self.requested, source_admitted, strict=True)
            )
            or any(
                admitted and not source
                for admitted, source in zip(self.admitted, source_admitted, strict=True)
            )
            or len(self.directions) != count
            or len(source_directions) != count
            or len(self.estimated_net_edge_ticks) != count
            or any(type(value) is not bool for value in (*self.requested, *self.admitted))
            or any(type(value) is not bool for value in source_admitted)
            or any(not isinstance(value, TradeDirection) for value in self.directions)
            or any(not isinstance(value, TradeDirection) for value in source_directions)
            or any(
                admitted != (direction is not TradeDirection.FLAT)
                for admitted, direction in zip(self.admitted, self.directions, strict=True)
            )
            or any(
                admitted != (direction is not TradeDirection.FLAT)
                for admitted, direction in zip(source_admitted, source_directions, strict=True)
            )
            or any(
                admitted and direction is not source_direction
                for admitted, direction, source_direction in zip(
                    self.admitted,
                    self.directions,
                    source_directions,
                    strict=True,
                )
            )
            or any(not math.isfinite(value) for value in self.scores)
            or any(
                value is not None and not math.isfinite(value)
                for value in self.estimated_net_edge_ticks
            )
        ):
            raise AllCasesMLError("prediction batch differs")

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "model_sha256": self.model_sha256,
            "rows": [
                {
                    "admitted": self.admitted[index],
                    "requested": self.requested[index],
                    "source_admitted": self.source_admitted[index],
                    "source_direction": self.source_directions[index].value,
                    "direction": self.directions[index].value,
                    "estimated_net_edge_ticks_hex": (
                        None
                        if self.estimated_net_edge_ticks[index] is None
                        else _float_hex(self.estimated_net_edge_ticks[index])
                    ),
                    "row_id": row_id,
                    "score_hex": _float_hex(self.scores[index]),
                }
                for index, row_id in enumerate(self.row_ids)
            ],
            "schema": PREDICTION_SCHEMA,
        }

    @classmethod
    def from_dict(cls, value: object) -> PredictionBatch:
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "candidate_id",
                "model_sha256",
                "rows",
                "schema",
            }
            or value["schema"] != PREDICTION_SCHEMA
        ):
            raise AllCasesMLError("prediction batch document differs")
        rows = value["rows"]
        row_keys = {
            "admitted",
            "direction",
            "estimated_net_edge_ticks_hex",
            "requested",
            "source_admitted",
            "source_direction",
            "row_id",
            "score_hex",
        }
        if (
            not isinstance(value["model_sha256"], str)
            or not isinstance(value["candidate_id"], str)
            or not isinstance(rows, list)
            or any(not isinstance(item, dict) or set(item) != row_keys for item in rows)
            or any(
                not isinstance(item["row_id"], str)
                or not isinstance(item["requested"], bool)
                or not isinstance(item["admitted"], bool)
                or not isinstance(item["source_admitted"], bool)
                or not isinstance(item["direction"], str)
                or not isinstance(item["source_direction"], str)
                for item in rows
            )
        ):
            raise AllCasesMLError("prediction batch document values differ")
        try:
            result = cls(
                value["model_sha256"],
                value["candidate_id"],
                tuple(item["row_id"] for item in rows),
                tuple(
                    _decode_float_hex(item["score_hex"], label="prediction score") for item in rows
                ),
                tuple(item["requested"] for item in rows),
                tuple(item["admitted"] for item in rows),
                tuple(TradeDirection(item["direction"]) for item in rows),
                tuple(
                    None
                    if item["estimated_net_edge_ticks_hex"] is None
                    else _decode_float_hex(
                        item["estimated_net_edge_ticks_hex"], label="prediction edge"
                    )
                    for item in rows
                ),
                tuple(item["source_admitted"] for item in rows),
                tuple(TradeDirection(item["source_direction"]) for item in rows),
            )
        except ValueError as error:
            raise AllCasesMLError("prediction batch document value differs") from error
        if result.as_dict() != value:
            raise AllCasesMLError("prediction batch document did not round trip")
        return result


def predict_actions(
    model: CanonicalMLModel,
    values: np.ndarray,
    *,
    row_ids: Sequence[str],
    atr_ticks: np.ndarray | None = None,
    base_directions: np.ndarray | None = None,
) -> PredictionBatch:
    """Score frozen Search state; this function cannot refit on WF or holdout."""

    identifiers = tuple(row_ids)
    if not identifiers or len(set(identifiers)) != len(identifiers):
        raise AllCasesMLError("prediction row identities differ")
    matrix = np.asarray(values, dtype=np.float64)
    if matrix.shape != (len(identifiers), len(model.feature_names)):
        raise AllCasesMLError("prediction matrix shape differs")
    scores_array = model.predict_scores(matrix)
    scores = tuple(float(value) for value in scores_array)
    admitted: list[bool] = []
    directions: list[TradeDirection] = []
    edges: list[float | None] = []
    if model.response_kind == "SIGNED_NORMALIZED_RETURN":
        if base_directions is not None or atr_ticks is None:
            raise AllCasesMLError("direct prediction requires ATR and no base direction")
        atr = np.asarray(atr_ticks, dtype=np.float64)
        if atr.shape != (len(identifiers),) or not np.isfinite(atr).all() or np.any(atr <= 0):
            raise AllCasesMLError("prediction ATR values differ")
        for score, scale in zip(scores, atr, strict=True):
            edge = abs(score) * float(scale) - TOTAL_FRICTION_TICKS
            keep = abs(score) >= model.admission_threshold and edge > 0.0 and score != 0.0
            admitted.append(keep)
            directions.append(
                TradeDirection.LONG
                if keep and score > 0.0
                else TradeDirection.SHORT
                if keep
                else TradeDirection.FLAT
            )
            edges.append(edge)
    else:
        if atr_ticks is not None or base_directions is None:
            raise AllCasesMLError("meta prediction requires base directions and no ATR")
        base = np.asarray(base_directions, dtype=np.int8)
        if base.shape != (len(identifiers),) or any(int(value) not in {-1, 1} for value in base):
            raise AllCasesMLError("prediction base directions differ")
        for score, direction in zip(scores, base, strict=True):
            keep = score >= model.admission_threshold
            admitted.append(keep)
            directions.append(
                TradeDirection.LONG
                if keep and direction == 1
                else TradeDirection.SHORT
                if keep
                else TradeDirection.FLAT
            )
            edges.append(None)
    return PredictionBatch(
        model.sha256,
        model.candidate_id,
        identifiers,
        scores,
        tuple(admitted),
        tuple(admitted),
        tuple(directions),
        tuple(edges),
    )


def apply_nonoverlap_occupancy(
    predictions: PredictionBatch,
    *,
    entry_ns: np.ndarray,
    planned_exit_ns: np.ndarray,
    contracts: Sequence[str],
) -> PredictionBatch:
    """Keep one global position per candidate/mask; equality at exit is available."""

    count = len(predictions.row_ids)
    entries = np.asarray(entry_ns, dtype=np.int64)
    exits = np.asarray(planned_exit_ns, dtype=np.int64)
    contract_values = tuple(contracts)
    if (
        entries.shape != (count,)
        or exits.shape != (count,)
        or len(contract_values) != count
        or np.any(entries <= 0)
        or np.any(exits <= entries)
        or tuple(zip(entries.tolist(), predictions.row_ids, strict=True))
        != tuple(sorted(zip(entries.tolist(), predictions.row_ids, strict=True)))
    ):
        raise AllCasesMLError("occupancy schedule differs")
    occupied_through_ns = -1
    admitted: list[bool] = []
    directions: list[TradeDirection] = []
    for index, requested in enumerate(predictions.requested):
        available = int(entries[index]) >= occupied_through_ns
        keep = requested and available
        admitted.append(keep)
        directions.append(predictions.directions[index] if keep else TradeDirection.FLAT)
        if keep:
            occupied_through_ns = int(exits[index])
    return PredictionBatch(
        predictions.model_sha256,
        predictions.candidate_id,
        predictions.row_ids,
        predictions.scores,
        predictions.requested,
        tuple(admitted),
        tuple(directions),
        predictions.estimated_net_edge_ticks,
    )


@dataclass(frozen=True, slots=True)
class CrossFittedScores:
    candidate_id: str
    null_world: str
    row_indexes: tuple[int, ...]
    row_ids: tuple[str, ...]
    fold_keys: tuple[str, ...]
    scores: tuple[float, ...]
    requested: tuple[bool, ...]
    admitted: tuple[bool, ...]
    directions: tuple[TradeDirection, ...]
    estimated_net_edge_ticks: tuple[float | None, ...]
    decision_dates: tuple[date, ...]
    entry_ns: tuple[int, ...]
    planned_exit_ns: tuple[int, ...]
    contracts: tuple[str, ...]
    fold_model_sha256: tuple[str, ...]
    fold_admission_thresholds: tuple[float, ...]
    task_timeframe_seconds: int | None
    task_horizon_seconds: int | None
    fold_outcome_lineage_sha256s: tuple[str, ...]
    fold_opportunity_lattice_sha256s: tuple[str, ...]
    fold_entry_schedule_sha256s: tuple[str, ...]
    fold_source_matrix_sha256s: tuple[str, ...]
    fold_outcome_values_sha256s: tuple[str, ...]
    source_admitted: tuple[bool, ...]
    source_directions: tuple[TradeDirection, ...]
    fold_base_strategy_ids: tuple[str, ...] = ()
    fold_base_trigger_families: tuple[str, ...] = ()
    fold_symbolic_ranking_certificates: tuple[SymbolicRankingCertificate, ...] = ()

    def __post_init__(self) -> None:
        count = len(self.row_indexes)
        if (
            len(self.candidate_id) != 64
            or any(character not in "0123456789abcdef" for character in self.candidate_id)
            or self.null_world not in NULL_WORLD_ORDER
            or not count
            or any(
                isinstance(value, bool) or not isinstance(value, int) for value in self.row_indexes
            )
            or len(set(self.row_indexes)) != count
            or len(self.row_ids) != count
            or any(not isinstance(value, str) or not value for value in self.row_ids)
            or len(set(self.row_ids)) != count
            or len(self.fold_keys) != count
            or any(value not in SEARCH_OUTER_FOLD_KEYS for value in self.fold_keys)
            or len(self.scores) != count
            or len(self.requested) != count
            or len(self.admitted) != count
            or len(self.source_admitted) != count
            or any(
                admitted and not requested
                for requested, admitted in zip(self.requested, self.admitted, strict=True)
            )
            or len(self.directions) != count
            or len(self.source_directions) != count
            or len(self.estimated_net_edge_ticks) != count
            or len(self.decision_dates) != count
            or len(self.entry_ns) != count
            or len(self.planned_exit_ns) != count
            or len(self.contracts) != count
            or any(
                isinstance(value, bool) or not isinstance(value, int)
                for value in (*self.entry_ns, *self.planned_exit_ns)
            )
            or any(
                isinstance(value, datetime) or not isinstance(value, date)
                for value in self.decision_dates
            )
            or any(not isinstance(value, str) or not value for value in self.contracts)
            or any(
                entry <= 0 or exit_ns <= entry
                for entry, exit_ns in zip(self.entry_ns, self.planned_exit_ns, strict=True)
            )
            or any(not math.isfinite(value) for value in self.scores)
            or any(
                value is not None and not math.isfinite(value)
                for value in self.estimated_net_edge_ticks
            )
            or any(not isinstance(value, TradeDirection) for value in self.directions)
            or any(not isinstance(value, TradeDirection) for value in self.source_directions)
            or len(self.fold_model_sha256) != len(SEARCH_OUTER_FOLD_KEYS)
            or any(len(value) != 64 for value in self.fold_model_sha256)
            or len(self.fold_admission_thresholds) != len(SEARCH_OUTER_FOLD_KEYS)
            or any(
                not math.isfinite(value) or value < 0 for value in self.fold_admission_thresholds
            )
            or len(self.fold_base_strategy_ids) not in {0, len(SEARCH_OUTER_FOLD_KEYS)}
            or len(self.fold_base_trigger_families) != len(self.fold_base_strategy_ids)
            or len(self.fold_symbolic_ranking_certificates) != len(self.fold_base_strategy_ids)
            or any(len(value) != 64 for value in self.fold_base_strategy_ids)
            or any(not value for value in self.fold_base_trigger_families)
            or any(
                len(values) != len(SEARCH_OUTER_FOLD_KEYS)
                for values in (
                    self.fold_outcome_lineage_sha256s,
                    self.fold_opportunity_lattice_sha256s,
                    self.fold_entry_schedule_sha256s,
                    self.fold_source_matrix_sha256s,
                    self.fold_outcome_values_sha256s,
                )
            )
            or any(
                len(value) != 64 or any(character not in "0123456789abcdef" for character in value)
                for values in (
                    self.fold_model_sha256,
                    self.fold_base_strategy_ids,
                    self.fold_outcome_lineage_sha256s,
                    self.fold_opportunity_lattice_sha256s,
                    self.fold_entry_schedule_sha256s,
                    self.fold_source_matrix_sha256s,
                    self.fold_outcome_values_sha256s,
                )
                for value in values
            )
            or any(type(value) is not bool for value in (*self.requested, *self.admitted))
            or any(type(value) is not bool for value in self.source_admitted)
            or any(
                admitted and not source
                for admitted, source in zip(self.admitted, self.source_admitted, strict=True)
            )
            or any(
                admitted and not requested
                for requested, admitted in zip(self.requested, self.source_admitted, strict=True)
            )
            or any(
                admitted != (direction is not TradeDirection.FLAT)
                for admitted, direction in zip(self.admitted, self.directions, strict=True)
            )
            or any(
                admitted != (direction is not TradeDirection.FLAT)
                for admitted, direction in zip(
                    self.source_admitted, self.source_directions, strict=True
                )
            )
            or any(
                admitted and direction is not source_direction
                for admitted, direction, source_direction in zip(
                    self.admitted,
                    self.directions,
                    self.source_directions,
                    strict=True,
                )
            )
            or tuple(sorted(self.row_indexes)) != self.row_indexes
            or tuple(zip(self.entry_ns, self.row_ids, strict=True))
            != tuple(sorted(zip(self.entry_ns, self.row_ids, strict=True)))
            or tuple(dict.fromkeys(self.fold_keys)) != SEARCH_OUTER_FOLD_KEYS
        ):
            raise AllCasesMLError("cross-fitted score artifact differs")
        meta_candidate = META_CANDIDATE_BY_ID.get(self.candidate_id)
        meta = bool(self.fold_symbolic_ranking_certificates)
        if (
            meta != (meta_candidate is not None)
            or not meta
            and self.candidate_id not in DIRECT_CANDIDATE_BY_ID
        ):
            raise AllCasesMLError("cross-fitted candidate kind differs")
        direct_candidate = DIRECT_CANDIDATE_BY_ID.get(self.candidate_id)
        if direct_candidate is not None:
            if (
                self.task_timeframe_seconds != direct_candidate.decision_timeframe_seconds
                or self.task_horizon_seconds != direct_candidate.horizon_seconds
                or any(value is None for value in self.estimated_net_edge_ticks)
            ):
                raise AllCasesMLError("direct cross-fitted task binding differs")
        elif (
            self.task_timeframe_seconds is not None
            or self.task_horizon_seconds is not None
            or any(value is not None for value in self.estimated_net_edge_ticks)
        ):
            raise AllCasesMLError("meta cross-fitted task binding differs")
        if meta_candidate is not None:
            for index, certificate in enumerate(self.fold_symbolic_ranking_certificates):
                strategy = certificate.strategy_at_rank(meta_candidate.symbolic_rank_slot)
                if (
                    strategy is None
                    or certificate.null_world != self.null_world
                    or certificate.fold_key != SEARCH_OUTER_FOLD_KEYS[index]
                    or strategy.strategy_id != self.fold_base_strategy_ids[index]
                    or strategy.trigger_family != self.fold_base_trigger_families[index]
                ):
                    raise AllCasesMLError("cross-fitted rank-slot certificate differs")

    @property
    def fold_symbolic_ranking_sha256(self) -> tuple[str, ...]:
        return tuple(item.artifact_sha256 for item in self.fold_symbolic_ranking_certificates)

    def definition_dict(self) -> dict[str, object]:
        meta = bool(self.fold_base_strategy_ids)
        return {
            "candidate_id": self.candidate_id,
            "folds": [
                {
                    "admission_threshold_hex": _float_hex(self.fold_admission_thresholds[index]),
                    "base_strategy_id": self.fold_base_strategy_ids[index] if meta else None,
                    "base_trigger_family": (
                        self.fold_base_trigger_families[index] if meta else None
                    ),
                    "fold_key": fold_key,
                    "entry_schedule_sha256": self.fold_entry_schedule_sha256s[index],
                    "model_sha256": self.fold_model_sha256[index],
                    "opportunity_lattice_sha256": self.fold_opportunity_lattice_sha256s[index],
                    "outcome_lineage_sha256": self.fold_outcome_lineage_sha256s[index],
                    "outcome_values_sha256": self.fold_outcome_values_sha256s[index],
                    "source_matrix_sha256": self.fold_source_matrix_sha256s[index],
                    "symbolic_ranking_certificate": (
                        self.fold_symbolic_ranking_certificates[index].as_dict() if meta else None
                    ),
                }
                for index, fold_key in enumerate(SEARCH_OUTER_FOLD_KEYS)
            ],
            "null_world": self.null_world,
            "task_horizon_seconds": self.task_horizon_seconds,
            "task_timeframe_seconds": self.task_timeframe_seconds,
            "rows": [
                {
                    "admitted": self.admitted[index],
                    "contract": self.contracts[index],
                    "decision_date": self.decision_dates[index].isoformat(),
                    "direction": self.directions[index].value,
                    "entry_ns": self.entry_ns[index],
                    "estimated_net_edge_ticks_hex": (
                        None
                        if self.estimated_net_edge_ticks[index] is None
                        else _float_hex(self.estimated_net_edge_ticks[index])
                    ),
                    "fold_key": self.fold_keys[index],
                    "planned_exit_ns": self.planned_exit_ns[index],
                    "requested": self.requested[index],
                    "source_admitted": self.source_admitted[index],
                    "source_direction": self.source_directions[index].value,
                    "row_id": self.row_ids[index],
                    "row_index": self.row_indexes[index],
                    "score_hex": _float_hex(self.scores[index]),
                }
                for index in range(len(self.row_ids))
            ],
            "schema": "systematic_fx.ai_all_cases_ml_crossfit.v1",
        }

    @property
    def artifact_sha256(self) -> str:
        return canonical_sha256(self.definition_dict())

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}

    @classmethod
    def from_dict(cls, value: object) -> CrossFittedScores:
        top_keys = {
            "artifact_sha256",
            "candidate_id",
            "folds",
            "null_world",
            "rows",
            "schema",
            "task_horizon_seconds",
            "task_timeframe_seconds",
        }
        if (
            not isinstance(value, dict)
            or set(value) != top_keys
            or value["schema"] != "systematic_fx.ai_all_cases_ml_crossfit.v1"
        ):
            raise AllCasesMLError("cross-fitted score document schema differs")
        rows = value["rows"]
        folds = value["folds"]
        row_keys = {
            "admitted",
            "contract",
            "decision_date",
            "direction",
            "entry_ns",
            "estimated_net_edge_ticks_hex",
            "fold_key",
            "planned_exit_ns",
            "requested",
            "row_id",
            "row_index",
            "score_hex",
            "source_admitted",
            "source_direction",
        }
        fold_keys = {
            "admission_threshold_hex",
            "base_strategy_id",
            "base_trigger_family",
            "entry_schedule_sha256",
            "fold_key",
            "model_sha256",
            "opportunity_lattice_sha256",
            "outcome_lineage_sha256",
            "outcome_values_sha256",
            "source_matrix_sha256",
            "symbolic_ranking_certificate",
        }
        if (
            not isinstance(rows, list)
            or not isinstance(folds, list)
            or len(folds) != len(SEARCH_OUTER_FOLD_KEYS)
            or any(not isinstance(item, dict) or set(item) != row_keys for item in rows)
            or any(not isinstance(item, dict) or set(item) != fold_keys for item in folds)
            or not isinstance(value["candidate_id"], str)
            or not isinstance(value["null_world"], str)
            or not isinstance(value["artifact_sha256"], str)
            or any(
                item is not None and (isinstance(item, bool) or not isinstance(item, int))
                for item in (
                    value["task_timeframe_seconds"],
                    value["task_horizon_seconds"],
                )
            )
            or any(
                not isinstance(item[key], str)
                for item in folds
                for key in (
                    "admission_threshold_hex",
                    "entry_schedule_sha256",
                    "fold_key",
                    "model_sha256",
                    "opportunity_lattice_sha256",
                    "outcome_lineage_sha256",
                    "outcome_values_sha256",
                    "source_matrix_sha256",
                )
            )
            or any(
                isinstance(item["row_index"], bool)
                or not isinstance(item["row_index"], int)
                or not isinstance(item["row_id"], str)
                or not isinstance(item["fold_key"], str)
                or not isinstance(item["score_hex"], str)
                or not isinstance(item["requested"], bool)
                or not isinstance(item["admitted"], bool)
                or not isinstance(item["source_admitted"], bool)
                or not isinstance(item["direction"], str)
                or not isinstance(item["source_direction"], str)
                or not isinstance(item["decision_date"], str)
                or isinstance(item["entry_ns"], bool)
                or not isinstance(item["entry_ns"], int)
                or isinstance(item["planned_exit_ns"], bool)
                or not isinstance(item["planned_exit_ns"], int)
                or not isinstance(item["contract"], str)
                or item["estimated_net_edge_ticks_hex"] is not None
                and not isinstance(item["estimated_net_edge_ticks_hex"], str)
                for item in rows
            )
        ):
            raise AllCasesMLError("cross-fitted score document rows differ")
        try:
            meta = all(item["base_strategy_id"] is not None for item in folds)
            if any(
                (item["base_strategy_id"] is None) != (not meta)
                or (item["base_trigger_family"] is None) != (not meta)
                or (item["symbolic_ranking_certificate"] is None) != (not meta)
                for item in folds
            ):
                raise AllCasesMLError("cross-fitted score meta binding differs")
            result = cls(
                value["candidate_id"],
                value["null_world"],
                tuple(item["row_index"] for item in rows),
                tuple(item["row_id"] for item in rows),
                tuple(item["fold_key"] for item in rows),
                tuple(
                    _decode_float_hex(item["score_hex"], label="crossfit score") for item in rows
                ),
                tuple(item["requested"] for item in rows),
                tuple(item["admitted"] for item in rows),
                tuple(TradeDirection(item["direction"]) for item in rows),
                tuple(
                    None
                    if item["estimated_net_edge_ticks_hex"] is None
                    else _decode_float_hex(
                        item["estimated_net_edge_ticks_hex"],
                        label="crossfit edge",
                    )
                    for item in rows
                ),
                tuple(date.fromisoformat(item["decision_date"]) for item in rows),
                tuple(item["entry_ns"] for item in rows),
                tuple(item["planned_exit_ns"] for item in rows),
                tuple(item["contract"] for item in rows),
                tuple(item["model_sha256"] for item in folds),
                tuple(
                    _decode_float_hex(
                        item["admission_threshold_hex"],
                        label="crossfit threshold",
                    )
                    for item in folds
                ),
                value["task_timeframe_seconds"],
                value["task_horizon_seconds"],
                tuple(item["outcome_lineage_sha256"] for item in folds),
                tuple(item["opportunity_lattice_sha256"] for item in folds),
                tuple(item["entry_schedule_sha256"] for item in folds),
                tuple(item["source_matrix_sha256"] for item in folds),
                tuple(item["outcome_values_sha256"] for item in folds),
                tuple(item["source_admitted"] for item in rows),
                tuple(TradeDirection(item["source_direction"]) for item in rows),
                tuple(item["base_strategy_id"] for item in folds) if meta else (),
                tuple(item["base_trigger_family"] for item in folds) if meta else (),
                tuple(
                    SymbolicRankingCertificate.from_dict(item["symbolic_ranking_certificate"])
                    for item in folds
                )
                if meta
                else (),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise AllCasesMLError("cross-fitted score document value differs") from error
        if result.as_dict() != value:
            raise AllCasesMLError("cross-fitted score document did not round trip")
        return result


def cross_fit_direct_candidate(
    candidate: DirectCandidate,
    matrix: TrainingMatrix,
    plan: SearchBlockPlan,
    *,
    world: NullWorld | str = NullWorld.REAL,
    cache: SharedFitCache | None = None,
) -> CrossFittedScores:
    """Generate B3-B8 predictions with prior-prefix-only fitting and purge."""

    normalized_world = _normalized_world(world)
    probe_null_world_feasibility(matrix, plan, candidate_id=candidate.candidate_id)
    indexes: list[int] = []
    row_ids: list[str] = []
    fold_keys: list[str] = []
    scores: list[float] = []
    requested: list[bool] = []
    directions: list[TradeDirection] = []
    edges: list[float | None] = []
    decision_dates: list[date] = []
    entry_times: list[int] = []
    exit_times: list[int] = []
    contracts: list[str] = []
    model_hashes: list[str] = []
    thresholds: list[float] = []
    for fold in plan.outer_folds:
        rows = purged_fold_rows(matrix, fold)
        train = matrix.take(rows.training_indexes)
        replacement_targets = (
            None
            if normalized_world is NullWorld.REAL
            else permuted_training_targets(
                matrix,
                rows.training_indexes,
                world=normalized_world,
                candidate_id=candidate.candidate_id,
                fold_key=fold.fold_key,
            )
        )
        model = fit_direct_model(
            candidate,
            train,
            world=normalized_world,
            fold_key=fold.fold_key,
            training_targets=replacement_targets,
            cache=cache,
        )
        validation_indexes = np.asarray(rows.validation_indexes, dtype=np.int64)
        validation_batch = predict_actions(
            model,
            matrix.values[validation_indexes],
            row_ids=tuple(matrix.row_ids[index] for index in rows.validation_indexes),
            atr_ticks=matrix.atr_ticks[validation_indexes],
        )
        indexes.extend(rows.validation_indexes)
        row_ids.extend(matrix.row_ids[index] for index in rows.validation_indexes)
        fold_keys.extend(fold.fold_key for _ in rows.validation_indexes)
        scores.extend(validation_batch.scores)
        requested.extend(validation_batch.requested)
        directions.extend(validation_batch.directions)
        edges.extend(validation_batch.estimated_net_edge_ticks)
        decision_dates.extend(matrix.decision_dates[index] for index in rows.validation_indexes)
        entry_times.extend(int(matrix.entry_ns[index]) for index in rows.validation_indexes)
        exit_times.extend(int(matrix.label_exit_ns[index]) for index in rows.validation_indexes)
        contracts.extend(matrix.contracts[index] for index in rows.validation_indexes)
        model_hashes.append(model.sha256)
        thresholds.append(model.admission_threshold)
    aggregate = PredictionBatch(
        canonical_sha256(model_hashes),
        candidate.candidate_id,
        tuple(row_ids),
        tuple(scores),
        tuple(requested),
        tuple(requested),
        tuple(directions),
        tuple(edges),
    )
    aggregate = apply_nonoverlap_occupancy(
        aggregate,
        entry_ns=np.asarray(entry_times, dtype=np.int64),
        planned_exit_ns=np.asarray(exit_times, dtype=np.int64),
        contracts=tuple(contracts),
    )
    return CrossFittedScores(
        candidate.candidate_id,
        normalized_world.value,
        tuple(indexes),
        tuple(row_ids),
        tuple(fold_keys),
        tuple(scores),
        aggregate.requested,
        aggregate.admitted,
        aggregate.directions,
        tuple(edges),
        tuple(decision_dates),
        tuple(entry_times),
        tuple(exit_times),
        tuple(contracts),
        tuple(model_hashes),
        tuple(thresholds),
        candidate.decision_timeframe_seconds,
        candidate.horizon_seconds,
        (matrix.outcome_lineage_sha256,) * len(SEARCH_OUTER_FOLD_KEYS),
        (matrix.opportunity_lattice_sha256,) * len(SEARCH_OUTER_FOLD_KEYS),
        (matrix.entry_schedule_sha256,) * len(SEARCH_OUTER_FOLD_KEYS),
        (training_rows_sha256(matrix),) * len(SEARCH_OUTER_FOLD_KEYS),
        (outcome_values_sha256(matrix),) * len(SEARCH_OUTER_FOLD_KEYS),
        aggregate.source_admitted,
        aggregate.source_directions,
    )


def cross_fit_meta_candidate(
    candidate: MetaCandidate,
    matrices_by_world_and_fold: Mapping[str, Mapping[str, TrainingMatrix]],
    plan: SearchBlockPlan,
    *,
    world: NullWorld | str = NullWorld.REAL,
    cache: SharedFitCache | None = None,
) -> CrossFittedScores:
    """Cross-fit a rank slot whose complete base strategy is reranked per prefix.

    The caller supplies a complete world x fold mapping.  Every target-control
    world must rerank the symbolic universe using that world's prior-prefix
    outcomes before the meta preprocessor and predictor are fitted.
    """

    normalized_world = _normalized_world(world)
    _validate_meta_world_fold_matrices(candidate, matrices_by_world_and_fold, plan)
    probe_meta_crossfit_null_feasibility(candidate, matrices_by_world_and_fold, plan)
    matrices_by_fold = matrices_by_world_and_fold[normalized_world.value]
    indexes: list[int] = []
    row_ids: list[str] = []
    fold_keys: list[str] = []
    scores: list[float] = []
    requested: list[bool] = []
    directions: list[TradeDirection] = []
    edges: list[float | None] = []
    decision_dates: list[date] = []
    entry_times: list[int] = []
    exit_times: list[int] = []
    contracts: list[str] = []
    model_hashes: list[str] = []
    thresholds: list[float] = []
    base_strategy_ids: list[str] = []
    base_trigger_families: list[str] = []
    ranking_certificates: list[SymbolicRankingCertificate] = []
    outcome_lineage_sha256s: list[str] = []
    opportunity_lattice_sha256s: list[str] = []
    entry_schedule_sha256s: list[str] = []
    source_matrix_sha256s: list[str] = []
    outcome_value_sha256s: list[str] = []
    synthetic_index = 0
    for fold in plan.outer_folds:
        matrix = matrices_by_fold[fold.fold_key]
        rows = purged_fold_rows(matrix, fold)
        train = matrix.take(rows.training_indexes)
        replacement_targets = (
            None
            if normalized_world is NullWorld.REAL
            else permuted_training_targets(
                matrix,
                rows.training_indexes,
                world=normalized_world,
                candidate_id=candidate.candidate_id,
                fold_key=fold.fold_key,
            )
        )
        model = fit_meta_model(
            candidate,
            train,
            ranking_training_dates=fold.training_dates,
            world=normalized_world,
            fold_key=fold.fold_key,
            training_targets=replacement_targets,
            cache=cache,
        )
        validation_indexes = np.asarray(rows.validation_indexes, dtype=np.int64)
        validation_batch = predict_actions(
            model,
            matrix.values[validation_indexes],
            row_ids=tuple(matrix.row_ids[index] for index in rows.validation_indexes),
            base_directions=matrix.base_directions[validation_indexes],
        )
        # Meta matrices can have different base strategies and therefore
        # different local row indexes.  The output index is a stable concat index.
        local_count = len(rows.validation_indexes)
        indexes.extend(range(synthetic_index, synthetic_index + local_count))
        synthetic_index += local_count
        row_ids.extend(matrix.row_ids[index] for index in rows.validation_indexes)
        fold_keys.extend(fold.fold_key for _ in rows.validation_indexes)
        scores.extend(validation_batch.scores)
        requested.extend(validation_batch.requested)
        directions.extend(validation_batch.directions)
        edges.extend(validation_batch.estimated_net_edge_ticks)
        decision_dates.extend(matrix.decision_dates[index] for index in rows.validation_indexes)
        entry_times.extend(int(matrix.entry_ns[index]) for index in rows.validation_indexes)
        exit_times.extend(int(matrix.label_exit_ns[index]) for index in rows.validation_indexes)
        contracts.extend(matrix.contracts[index] for index in rows.validation_indexes)
        model_hashes.append(model.sha256)
        thresholds.append(model.admission_threshold)
        if (
            model.base_strategy_id is None
            or model.base_trigger_family is None
            or model.symbolic_ranking_certificate is None
        ):
            raise AllCasesMLError("meta cross-fit model lost its symbolic binding")
        base_strategy_ids.append(model.base_strategy_id)
        base_trigger_families.append(model.base_trigger_family)
        ranking_certificates.append(model.symbolic_ranking_certificate)
        outcome_lineage_sha256s.append(matrix.outcome_lineage_sha256)
        opportunity_lattice_sha256s.append(matrix.opportunity_lattice_sha256)
        entry_schedule_sha256s.append(matrix.entry_schedule_sha256)
        source_matrix_sha256s.append(training_rows_sha256(matrix))
        outcome_value_sha256s.append(outcome_values_sha256(matrix))
    aggregate = PredictionBatch(
        canonical_sha256(model_hashes),
        candidate.candidate_id,
        tuple(row_ids),
        tuple(scores),
        tuple(requested),
        tuple(requested),
        tuple(directions),
        tuple(edges),
    )
    aggregate = apply_nonoverlap_occupancy(
        aggregate,
        entry_ns=np.asarray(entry_times, dtype=np.int64),
        planned_exit_ns=np.asarray(exit_times, dtype=np.int64),
        contracts=tuple(contracts),
    )
    return CrossFittedScores(
        candidate.candidate_id,
        normalized_world.value,
        tuple(indexes),
        tuple(row_ids),
        tuple(fold_keys),
        tuple(scores),
        aggregate.requested,
        aggregate.admitted,
        aggregate.directions,
        tuple(edges),
        tuple(decision_dates),
        tuple(entry_times),
        tuple(exit_times),
        tuple(contracts),
        tuple(model_hashes),
        tuple(thresholds),
        None,
        None,
        tuple(outcome_lineage_sha256s),
        tuple(opportunity_lattice_sha256s),
        tuple(entry_schedule_sha256s),
        tuple(source_matrix_sha256s),
        tuple(outcome_value_sha256s),
        aggregate.source_admitted,
        aggregate.source_directions,
        tuple(base_strategy_ids),
        tuple(base_trigger_families),
        tuple(ranking_certificates),
    )


@dataclass(frozen=True, slots=True)
class ControlAlignmentProof:
    """Exact date/direction cardinality proof for REAL and both target controls."""

    candidate_id: str
    scope_key: str
    target_count_records: tuple[tuple[str, str, int], ...]
    source_mask_sha256_by_world: tuple[tuple[str, str], ...]
    aligned_mask_sha256_by_world: tuple[tuple[str, str], ...]
    selected_row_ids_by_world: tuple[tuple[str, tuple[str, ...]], ...]
    artifact_sha256: str

    def definition_dict(self) -> dict[str, object]:
        return {
            "aligned_mask_sha256_by_world": [
                list(item) for item in self.aligned_mask_sha256_by_world
            ],
            "candidate_id": self.candidate_id,
            "schema": CONTROL_ALIGNMENT_SCHEMA,
            "scope_key": self.scope_key,
            "selected_row_ids_by_world": [
                [world, list(row_ids)] for world, row_ids in self.selected_row_ids_by_world
            ],
            "source_mask_sha256_by_world": [
                list(item) for item in self.source_mask_sha256_by_world
            ],
            "target_count_records": [list(item) for item in self.target_count_records],
        }

    def __post_init__(self) -> None:
        if (
            len(self.candidate_id) != 64
            or any(character not in "0123456789abcdef" for character in self.candidate_id)
            or not self.scope_key
            or tuple(world for world, _ in self.source_mask_sha256_by_world) != NULL_WORLD_ORDER
            or tuple(world for world, _ in self.aligned_mask_sha256_by_world) != NULL_WORLD_ORDER
            or tuple(world for world, _ in self.selected_row_ids_by_world) != NULL_WORLD_ORDER
            or any(
                len(value) != 64 or any(character not in "0123456789abcdef" for character in value)
                for _, value in self.source_mask_sha256_by_world
            )
            or any(
                len(value) != 64 or any(character not in "0123456789abcdef" for character in value)
                for _, value in self.aligned_mask_sha256_by_world
            )
            or self.target_count_records != tuple(sorted(self.target_count_records))
            or len({(day, direction) for day, direction, _ in self.target_count_records})
            != len(self.target_count_records)
            or any(
                not isinstance(day, str)
                or not day
                or direction not in {"LONG", "SHORT"}
                or isinstance(count, bool)
                or not isinstance(count, int)
                or count <= 0
                for day, direction, count in self.target_count_records
            )
            or any(
                len(set(row_ids)) != len(row_ids)
                or any(not isinstance(row_id, str) or not row_id for row_id in row_ids)
                for _, row_ids in self.selected_row_ids_by_world
            )
            or len(self.artifact_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.artifact_sha256)
            or canonical_sha256(self.definition_dict()) != self.artifact_sha256
        ):
            raise AllCasesMLError("control-alignment proof differs")

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


@dataclass(frozen=True, slots=True)
class AlignedCrossFitControls:
    real: CrossFittedScores
    circular_target: CrossFittedScores
    matched_target: CrossFittedScores
    proof: ControlAlignmentProof

    def __post_init__(self) -> None:
        values = (self.real, self.circular_target, self.matched_target)
        if (
            tuple(item.null_world for item in values) != NULL_WORLD_ORDER
            or len({item.candidate_id for item in values}) != 1
            or self.proof.candidate_id != self.real.candidate_id
            or self.proof.scope_key != "SEARCH_OOF_B3_B8"
        ):
            raise AllCasesMLError("aligned Search controls differ")
        _validate_control_alignment_attachment(
            {item.null_world: _prediction_batch_from_crossfit(item) for item in values},
            {item.null_world: item.decision_dates for item in values},
            self.proof,
            scope_key="SEARCH_OOF_B3_B8",
        )


def _prediction_batch_from_crossfit(result: CrossFittedScores) -> PredictionBatch:
    return PredictionBatch(
        canonical_sha256(list(result.fold_model_sha256)),
        result.candidate_id,
        result.row_ids,
        result.scores,
        result.requested,
        result.admitted,
        result.directions,
        result.estimated_net_edge_ticks,
        result.source_admitted,
        result.source_directions,
    )


def _source_prediction_batch(batch: PredictionBatch) -> PredictionBatch:
    return replace(
        batch,
        admitted=batch.source_admitted,
        directions=batch.source_directions,
    )


def _action_mask_sha256(
    predictions: PredictionBatch,
    decision_dates: Sequence[date],
) -> str:
    return canonical_sha256(
        [
            {
                "admitted": admitted,
                "decision_date": decision_date.isoformat(),
                "direction": direction.value,
                "requested": requested,
                "row_id": row_id,
            }
            for row_id, decision_date, requested, admitted, direction in zip(
                predictions.row_ids,
                decision_dates,
                predictions.requested,
                predictions.admitted,
                predictions.directions,
                strict=True,
            )
        ]
    )


def _validate_control_alignment_attachment(
    batches_by_world: Mapping[str, PredictionBatch],
    dates_by_world: Mapping[str, Sequence[date]],
    proof: ControlAlignmentProof,
    *,
    scope_key: str,
) -> None:
    """Recompute every proof semantic from its attached source and aligned masks."""

    if set(batches_by_world) != set(NULL_WORLD_ORDER) or set(dates_by_world) != set(
        NULL_WORLD_ORDER
    ):
        raise AllCasesMLError("control proof attachment worlds differ")
    batches = {world: batches_by_world[world] for world in NULL_WORLD_ORDER}
    dates = {world: tuple(dates_by_world[world]) for world in NULL_WORLD_ORDER}
    if (
        proof.scope_key != scope_key
        or any(len(dates[world]) != len(batches[world].row_ids) for world in NULL_WORLD_ORDER)
        or len({batch.candidate_id for batch in batches.values()}) != 1
        or proof.candidate_id != batches[NullWorld.REAL.value].candidate_id
    ):
        raise AllCasesMLError("control proof attachment identity differs")
    source_hashes = tuple(
        (
            world,
            _action_mask_sha256(_source_prediction_batch(batches[world]), dates[world]),
        )
        for world in NULL_WORLD_ORDER
    )
    aligned_hashes = tuple(
        (world, _action_mask_sha256(batches[world], dates[world])) for world in NULL_WORLD_ORDER
    )
    selected_rows = tuple(
        (
            world,
            tuple(
                row_id
                for row_id, admitted in zip(
                    batches[world].row_ids,
                    batches[world].admitted,
                    strict=True,
                )
                if admitted
            ),
        )
        for world in NULL_WORLD_ORDER
    )
    target_counts: dict[tuple[date, TradeDirection], int] = defaultdict(int)
    real = batches[NullWorld.REAL.value]
    for decision_date, admitted, direction in zip(
        dates[NullWorld.REAL.value], real.admitted, real.directions, strict=True
    ):
        if admitted:
            target_counts[decision_date, direction] += 1
    count_records = tuple(
        (decision_date.isoformat(), direction.value, count)
        for (decision_date, direction), count in sorted(
            target_counts.items(), key=lambda item: (item[0][0], item[0][1].value)
        )
    )
    for world in NULL_WORLD_ORDER:
        observed: dict[tuple[date, TradeDirection], int] = defaultdict(int)
        for decision_date, admitted, direction in zip(
            dates[world], batches[world].admitted, batches[world].directions, strict=True
        ):
            if admitted:
                observed[decision_date, direction] += 1
        if dict(observed) != dict(target_counts):
            raise AllCasesMLError("aligned control date/direction counts differ")
    if (
        proof.source_mask_sha256_by_world != source_hashes
        or proof.aligned_mask_sha256_by_world != aligned_hashes
        or proof.selected_row_ids_by_world != selected_rows
        or proof.target_count_records != count_records
    ):
        raise AllCasesMLError("control alignment proof is not paired to its masks")


def _aligned_prediction_batches(
    batches_by_world: Mapping[str, PredictionBatch],
    dates_by_world: Mapping[str, Sequence[date]],
    *,
    scope_key: str,
) -> tuple[dict[str, PredictionBatch], ControlAlignmentProof]:
    if set(batches_by_world) != set(NULL_WORLD_ORDER) or set(dates_by_world) != set(
        NULL_WORLD_ORDER
    ):
        raise AllCasesMLError("control alignment requires exactly REAL and both controls")
    batches = {world: batches_by_world[world] for world in NULL_WORLD_ORDER}
    dates = {world: tuple(dates_by_world[world]) for world in NULL_WORLD_ORDER}
    candidate_ids = {item.candidate_id for item in batches.values()}
    if len(candidate_ids) != 1 or any(
        len(dates[world]) != len(batches[world].row_ids) for world in NULL_WORLD_ORDER
    ):
        raise AllCasesMLError("control alignment candidate/date binding differs")

    real = batches[NullWorld.REAL.value]
    target_counts: dict[tuple[date, TradeDirection], int] = defaultdict(int)
    for decision_date, admitted, direction in zip(
        dates[NullWorld.REAL.value], real.admitted, real.directions, strict=True
    ):
        if admitted:
            if direction is TradeDirection.FLAT:
                raise AllCasesMLError("admitted REAL row has a flat direction")
            target_counts[decision_date, direction] += 1
    if not target_counts:
        raise MLCandidateIneligible(
            MLIneligibilityReason.REAL_MASK_EMPTY,
            "REAL mask has no admitted rows to align",
            candidate_id=real.candidate_id,
            scope_key=scope_key,
        )

    aligned: dict[str, PredictionBatch] = {NullWorld.REAL.value: real}
    for world in (NullWorld.CIRCULAR_TARGET.value, NullWorld.MATCHED_TARGET.value):
        source = batches[world]
        selected: set[int] = set()
        for (decision_date, direction), required in sorted(
            target_counts.items(), key=lambda item: (item[0][0], item[0][1].value)
        ):
            pool = [
                index
                for index, (candidate_date, admitted, candidate_direction) in enumerate(
                    zip(dates[world], source.admitted, source.directions, strict=True)
                )
                if admitted and candidate_date == decision_date and candidate_direction is direction
            ]
            direct_scores = any(
                source.estimated_net_edge_ticks[index] is not None for index in pool
            )
            pool.sort(
                key=lambda index: (
                    -(abs(source.scores[index]) if direct_scores else source.scores[index]),
                    source.row_ids[index],
                )
            )
            if len(pool) < required:
                raise MLCandidateIneligible(
                    MLIneligibilityReason.CONTROL_ALIGNMENT_INSUFFICIENT,
                    f"{world} has insufficient independently admitted date/direction rows",
                    candidate_id=real.candidate_id,
                    scope_key=scope_key,
                )
            selected.update(pool[:required])
        admissions = tuple(index in selected for index in range(len(source.row_ids)))
        directions = tuple(
            source.directions[index] if index in selected else TradeDirection.FLAT
            for index in range(len(source.row_ids))
        )
        aligned[world] = replace(source, admitted=admissions, directions=directions)

    count_records = tuple(
        (decision_date.isoformat(), direction.value, count)
        for (decision_date, direction), count in sorted(
            target_counts.items(), key=lambda item: (item[0][0], item[0][1].value)
        )
    )
    source_hashes = tuple(
        (
            world,
            _action_mask_sha256(_source_prediction_batch(batches[world]), dates[world]),
        )
        for world in NULL_WORLD_ORDER
    )
    aligned_hashes = tuple(
        (world, _action_mask_sha256(aligned[world], dates[world])) for world in NULL_WORLD_ORDER
    )
    if len({value for _, value in aligned_hashes}) != len(NULL_WORLD_ORDER):
        raise MLCandidateIneligible(
            MLIneligibilityReason.CONTROL_MASKS_NOT_DISTINCT,
            "aligned REAL/control masks are not pairwise distinct",
            candidate_id=real.candidate_id,
            scope_key=scope_key,
        )
    selected_rows = tuple(
        (
            world,
            tuple(
                row_id
                for row_id, admitted in zip(
                    aligned[world].row_ids, aligned[world].admitted, strict=True
                )
                if admitted
            ),
        )
        for world in NULL_WORLD_ORDER
    )
    definition = {
        "aligned_mask_sha256_by_world": [list(item) for item in aligned_hashes],
        "candidate_id": real.candidate_id,
        "schema": CONTROL_ALIGNMENT_SCHEMA,
        "scope_key": scope_key,
        "selected_row_ids_by_world": [[world, list(row_ids)] for world, row_ids in selected_rows],
        "source_mask_sha256_by_world": [list(item) for item in source_hashes],
        "target_count_records": [list(item) for item in count_records],
    }
    proof = ControlAlignmentProof(
        real.candidate_id,
        scope_key,
        count_records,
        source_hashes,
        aligned_hashes,
        selected_rows,
        canonical_sha256(definition),
    )
    return aligned, proof


def align_cross_fitted_null_controls(
    real: CrossFittedScores,
    circular_target: CrossFittedScores,
    matched_target: CrossFittedScores,
) -> AlignedCrossFitControls:
    """Match executed control counts to REAL within every date and direction."""

    inputs = (real, circular_target, matched_target)
    if tuple(item.null_world for item in inputs) != NULL_WORLD_ORDER:
        raise AllCasesMLError("cross-fit controls are not in frozen null-world order")
    batches, proof = _aligned_prediction_batches(
        {item.null_world: _prediction_batch_from_crossfit(item) for item in inputs},
        {item.null_world: item.decision_dates for item in inputs},
        scope_key="SEARCH_OOF_B3_B8",
    )
    aligned_results = tuple(
        replace(
            item,
            admitted=batches[item.null_world].admitted,
            directions=batches[item.null_world].directions,
        )
        for item in inputs
    )
    return AlignedCrossFitControls(*aligned_results, proof)


@dataclass(frozen=True, slots=True)
class MLGroupEconomicAggregate:
    group_key: str
    raw_signal_count: int
    active_signal_dates: tuple[date, ...]
    fill_count: int
    active_entry_dates: tuple[date, ...]
    net_ticks: int
    stress_net_ticks: int
    gross_profit_ticks: int
    gross_loss_ticks: int
    maximum_drawdown_ticks: int

    def __post_init__(self) -> None:
        numeric = (
            self.net_ticks,
            self.stress_net_ticks,
            self.gross_profit_ticks,
            self.gross_loss_ticks,
            self.maximum_drawdown_ticks,
        )
        if (
            not self.group_key
            or self.raw_signal_count < 0
            or self.fill_count < 0
            or self.fill_count > self.raw_signal_count
            or self.active_signal_dates != tuple(sorted(set(self.active_signal_dates)))
            or self.active_entry_dates != tuple(sorted(set(self.active_entry_dates)))
            or any(isinstance(value, bool) or not isinstance(value, int) for value in numeric)
            or any(value < 0 for value in numeric[2:])
        ):
            raise AllCasesMLError("ML group economics differ")

    def as_dict(self) -> dict[str, object]:
        return {
            "active_entry_dates": [value.isoformat() for value in self.active_entry_dates],
            "active_signal_dates": [value.isoformat() for value in self.active_signal_dates],
            "fill_count": self.fill_count,
            "gross_loss_ticks": self.gross_loss_ticks,
            "gross_profit_ticks": self.gross_profit_ticks,
            "group_key": self.group_key,
            "maximum_drawdown_ticks": self.maximum_drawdown_ticks,
            "net_ticks": self.net_ticks,
            "raw_signal_count": self.raw_signal_count,
            "stress_net_ticks": self.stress_net_ticks,
        }


def _integer_equity_shape(values: Sequence[int]) -> int:
    equity = 0
    peak = 0
    maximum_drawdown = 0
    for value in values:
        equity += int(value)
        peak = max(peak, equity)
        maximum_drawdown = max(maximum_drawdown, peak - equity)
    return maximum_drawdown


def _economic_group(
    result: CrossFittedScores,
    realized_net_ticks: np.ndarray,
    indexes: Sequence[int],
    *,
    group_key: str,
) -> MLGroupEconomicAggregate:
    selected = tuple(indexes)
    signals = tuple(index for index in selected if result.requested[index])
    fills = tuple(index for index in selected if result.admitted[index])
    nets = tuple(int(realized_net_ticks[index]) for index in fills)
    return MLGroupEconomicAggregate(
        group_key,
        len(signals),
        tuple(sorted({result.decision_dates[index] for index in signals})),
        len(fills),
        tuple(sorted({result.decision_dates[index] for index in fills})),
        sum(nets),
        sum(value - 4 for value in nets),
        sum(value for value in nets if value > 0),
        -sum(value for value in nets if value < 0),
        _integer_equity_shape(nets),
    )


@dataclass(frozen=True, slots=True)
class MLSearchEconomicEvaluation:
    candidate_id: str
    candidate_kind: Literal["DIRECT", "META"]
    family_key: str
    null_world: str
    alignment_proof_sha256: str
    raw_signal_count: int
    active_signal_days: int
    fill_count: int
    active_entry_days: int
    total_net_ticks: int
    total_stress_net_ticks: int
    gross_profit_ticks: int
    gross_loss_ticks: int
    maximum_drawdown_ticks: int
    action_identities: tuple[tuple[str, str], ...]
    daily_net_ticks: tuple[tuple[str, int], ...]
    reporting_groups: tuple[MLGroupEconomicAggregate, ...]
    outer_validations: tuple[MLGroupEconomicAggregate, ...]
    artifact_sha256: str

    @property
    def profit_factor(self) -> Fraction | None:
        if self.gross_loss_ticks == 0:
            return None if self.gross_profit_ticks == 0 else Fraction(10**18)
        return Fraction(self.gross_profit_ticks, self.gross_loss_ticks)

    def definition_dict(self) -> dict[str, object]:
        return {
            "active_entry_days": self.active_entry_days,
            "active_signal_days": self.active_signal_days,
            "action_identities": [list(value) for value in self.action_identities],
            "alignment_proof_sha256": self.alignment_proof_sha256,
            "candidate_id": self.candidate_id,
            "candidate_kind": self.candidate_kind,
            "family_key": self.family_key,
            "fill_count": self.fill_count,
            "gross_loss_ticks": self.gross_loss_ticks,
            "gross_profit_ticks": self.gross_profit_ticks,
            "maximum_drawdown_ticks": self.maximum_drawdown_ticks,
            "daily_net_ticks": [list(value) for value in self.daily_net_ticks],
            "null_world": self.null_world,
            "outer_validations": [value.as_dict() for value in self.outer_validations],
            "raw_signal_count": self.raw_signal_count,
            "reporting_groups": [value.as_dict() for value in self.reporting_groups],
            "schema": ML_SEARCH_EVALUATION_SCHEMA,
            "total_net_ticks": self.total_net_ticks,
            "total_stress_net_ticks": self.total_stress_net_ticks,
        }

    def __post_init__(self) -> None:
        if (
            len(self.candidate_id) != 64
            or self.candidate_kind not in {"DIRECT", "META"}
            or len(self.family_key) != 64
            or self.null_world not in NULL_WORLD_ORDER
            or len(self.alignment_proof_sha256) != 64
            or self.raw_signal_count < self.fill_count
            or self.active_signal_days < self.active_entry_days
            or len(self.action_identities) != self.fill_count
            or self.action_identities != tuple(sorted(set(self.action_identities)))
            or any(
                not row_id or direction not in {"LONG", "SHORT"}
                for row_id, direction in self.action_identities
            )
            or self.daily_net_ticks != tuple(sorted(self.daily_net_ticks))
            or len({value[0] for value in self.daily_net_ticks}) != len(self.daily_net_ticks)
            or any(
                not isinstance(day, str)
                or not day
                or isinstance(value, bool)
                or not isinstance(value, int)
                for day, value in self.daily_net_ticks
            )
            or sum(value for _, value in self.daily_net_ticks) != self.total_net_ticks
            or tuple(value.group_key for value in self.outer_validations) != SEARCH_OUTER_FOLD_KEYS
            or len(self.reporting_groups) < 1
            or any(
                isinstance(value, bool) or not isinstance(value, int)
                for value in (
                    self.total_net_ticks,
                    self.total_stress_net_ticks,
                    self.gross_profit_ticks,
                    self.gross_loss_ticks,
                    self.maximum_drawdown_ticks,
                )
            )
            or canonical_sha256(self.definition_dict()) != self.artifact_sha256
        ):
            raise AllCasesMLError("ML Search economic evaluation differs")

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def direct_crossfit_realized_net_ticks(
    result: CrossFittedScores,
    matrix: TrainingMatrix,
) -> np.ndarray:
    """Resolve aligned direct actions against untouched validation terminal moves."""

    candidate = DIRECT_CANDIDATE_BY_ID.get(result.candidate_id)
    expected_fold_count = len(SEARCH_OUTER_FOLD_KEYS)
    if (
        candidate is None
        or matrix.base_directions is not None
        or matrix.terminal_move_ticks is None
        or matrix.task_timeframe_seconds != candidate.decision_timeframe_seconds
        or matrix.task_horizon_seconds != candidate.horizon_seconds
        or result.task_timeframe_seconds != candidate.decision_timeframe_seconds
        or result.task_horizon_seconds != candidate.horizon_seconds
        or result.fold_outcome_lineage_sha256s
        != (matrix.outcome_lineage_sha256,) * expected_fold_count
        or result.fold_opportunity_lattice_sha256s
        != (matrix.opportunity_lattice_sha256,) * expected_fold_count
        or result.fold_entry_schedule_sha256s
        != (matrix.entry_schedule_sha256,) * expected_fold_count
        or result.fold_source_matrix_sha256s
        != (training_rows_sha256(matrix),) * expected_fold_count
        or result.fold_outcome_values_sha256s
        != (outcome_values_sha256(matrix),) * expected_fold_count
        or any(index < 0 or index >= matrix.row_count for index in result.row_indexes)
        or any(
            result.row_ids[output_index] != matrix.row_ids[matrix_index]
            or result.decision_dates[output_index] != matrix.decision_dates[matrix_index]
            or result.entry_ns[output_index] != int(matrix.entry_ns[matrix_index])
            or result.planned_exit_ns[output_index] != int(matrix.label_exit_ns[matrix_index])
            or result.contracts[output_index] != matrix.contracts[matrix_index]
            for output_index, matrix_index in enumerate(result.row_indexes)
        )
    ):
        raise AllCasesMLError("direct OOF outcome matrix differs")
    output = np.zeros(len(result.row_indexes), dtype=np.int64)
    for output_index, matrix_index in enumerate(result.row_indexes):
        if not result.admitted[output_index]:
            continue
        direction = result.directions[output_index]
        signed = 1 if direction is TradeDirection.LONG else -1
        if direction is TradeDirection.FLAT:
            raise AllCasesMLError("admitted direct OOF row is flat")
        gross_move_ticks = int(matrix.terminal_move_ticks[matrix_index])
        output[output_index] = int(signed) * gross_move_ticks - TOTAL_FRICTION_TICKS
    output.setflags(write=False)
    return output


def meta_crossfit_realized_net_ticks(
    result: CrossFittedScores,
    matrices_by_world_and_fold: Mapping[str, Mapping[str, TrainingMatrix]],
) -> np.ndarray:
    """Resolve aligned meta admissions against each world's reranked base trades."""

    candidate = META_CANDIDATE_BY_ID.get(result.candidate_id)
    world_matrices = matrices_by_world_and_fold.get(result.null_world)
    if (
        candidate is None
        or world_matrices is None
        or set(world_matrices) != set(SEARCH_OUTER_FOLD_KEYS)
        or len(result.fold_symbolic_ranking_certificates) != len(SEARCH_OUTER_FOLD_KEYS)
    ):
        raise AllCasesMLError("meta OOF outcome matrices differ")
    for fold_index, fold_key in enumerate(SEARCH_OUTER_FOLD_KEYS):
        matrix = world_matrices[fold_key]
        certificate = result.fold_symbolic_ranking_certificates[fold_index]
        ranked = certificate.strategy_at_rank(candidate.symbolic_rank_slot)
        if (
            ranked is None
            or matrix.symbolic_ranking_certificate != certificate
            or matrix.symbolic_ranking_world != result.null_world
            or matrix.base_strategy_id != result.fold_base_strategy_ids[fold_index]
            or matrix.base_strategy_id != ranked.strategy_id
            or matrix.base_trigger_family != result.fold_base_trigger_families[fold_index]
            or matrix.base_trigger_family != ranked.trigger_family
            or matrix.outcome_lineage_sha256 != result.fold_outcome_lineage_sha256s[fold_index]
            or matrix.opportunity_lattice_sha256
            != result.fold_opportunity_lattice_sha256s[fold_index]
            or matrix.entry_schedule_sha256 != result.fold_entry_schedule_sha256s[fold_index]
            or training_rows_sha256(matrix) != result.fold_source_matrix_sha256s[fold_index]
            or outcome_values_sha256(matrix) != result.fold_outcome_values_sha256s[fold_index]
        ):
            raise AllCasesMLError("meta OOF fold recipe or lineage binding differs")
    lookups = {
        fold_key: {row_id: index for index, row_id in enumerate(matrix.row_ids)}
        for fold_key, matrix in world_matrices.items()
    }
    output = np.zeros(len(result.row_ids), dtype=np.int64)
    for output_index, (fold_key, row_id) in enumerate(
        zip(result.fold_keys, result.row_ids, strict=True)
    ):
        matrix = world_matrices[fold_key]
        local_index = lookups[fold_key].get(row_id)
        if (
            local_index is None
            or matrix.realized_net_ticks is None
            or matrix.base_directions is None
            or result.decision_dates[output_index] != matrix.decision_dates[local_index]
            or result.entry_ns[output_index] != int(matrix.entry_ns[local_index])
            or result.planned_exit_ns[output_index] != int(matrix.label_exit_ns[local_index])
            or result.contracts[output_index] != matrix.contracts[local_index]
        ):
            raise AllCasesMLError("meta OOF row lost its world/fold base outcome")
        if not result.admitted[output_index]:
            continue
        expected_direction = (
            TradeDirection.LONG
            if int(matrix.base_directions[local_index]) == 1
            else TradeDirection.SHORT
        )
        if result.directions[output_index] is not expected_direction:
            raise AllCasesMLError("meta OOF gate changed the base trade direction")
        output[output_index] = int(matrix.realized_net_ticks[local_index])
    output.setflags(write=False)
    return output


def evaluate_ml_crossfit_economics(
    result: CrossFittedScores,
    realized_net_ticks: np.ndarray,
    *,
    reporting_group_by_date: Mapping[date, str],
    alignment_proof_sha256: str,
    family_key: str | None = None,
) -> MLSearchEconomicEvaluation:
    """Aggregate one aligned OOF world under the frozen 14/18-tick economics."""

    net = _exact_int64_array(
        realized_net_ticks,
        (len(result.row_ids),),
        "realized_net_ticks",
    )
    if (
        len(alignment_proof_sha256) != 64
        or not reporting_group_by_date
        or any(
            isinstance(key, datetime)
            or not isinstance(key, date)
            or not isinstance(value, str)
            or not value
            for key, value in reporting_group_by_date.items()
        )
        or any(value not in reporting_group_by_date for value in result.decision_dates)
    ):
        raise AllCasesMLError("ML OOF economics inputs differ")
    candidate: DirectCandidate | MetaCandidate | None = DIRECT_CANDIDATE_BY_ID.get(
        result.candidate_id
    )
    candidate_kind: Literal["DIRECT", "META"] = "DIRECT"
    if candidate is None:
        candidate = META_CANDIDATE_BY_ID.get(result.candidate_id)
        candidate_kind = "META"
    if candidate is None:
        raise AllCasesMLError("ML OOF candidate is outside the frozen catalogs")
    resolved_family_key = (
        ml_candidate_family_key(candidate) if isinstance(candidate, DirectCandidate) else family_key
    )
    if (
        resolved_family_key is None
        or len(resolved_family_key) != 64
        or any(character not in "0123456789abcdef" for character in resolved_family_key)
    ):
        raise AllCasesMLError("ML OOF family binding differs")

    reporting_keys = tuple(sorted(set(reporting_group_by_date.values())))
    reporting = tuple(
        _economic_group(
            result,
            net,
            tuple(
                index
                for index, value in enumerate(result.decision_dates)
                if reporting_group_by_date[value] == group_key
            ),
            group_key=group_key,
        )
        for group_key in reporting_keys
    )
    outer = tuple(
        _economic_group(
            result,
            net,
            tuple(index for index, value in enumerate(result.fold_keys) if value == fold_key),
            group_key=fold_key,
        )
        for fold_key in SEARCH_OUTER_FOLD_KEYS
    )
    fills = tuple(index for index, admitted in enumerate(result.admitted) if admitted)
    signals = tuple(index for index, requested in enumerate(result.requested) if requested)
    if any(result.directions[index] is TradeDirection.FLAT for index in fills):
        raise AllCasesMLError("admitted ML OOF action is flat")
    net_values = tuple(int(net[index]) for index in fills)
    action_identities = tuple(
        sorted((result.row_ids[index], result.directions[index].value) for index in fills)
    )
    daily_net = {value: 0 for value in sorted(reporting_group_by_date)}
    for index in fills:
        daily_net[result.decision_dates[index]] += int(net[index])
    daily_net_ticks = tuple((key.isoformat(), value) for key, value in daily_net.items())
    definition = {
        "active_entry_days": len({result.decision_dates[index] for index in fills}),
        "active_signal_days": len({result.decision_dates[index] for index in signals}),
        "action_identities": [list(value) for value in action_identities],
        "alignment_proof_sha256": alignment_proof_sha256,
        "candidate_id": result.candidate_id,
        "candidate_kind": candidate_kind,
        "family_key": resolved_family_key,
        "fill_count": len(fills),
        "gross_loss_ticks": -sum(value for value in net_values if value < 0),
        "gross_profit_ticks": sum(value for value in net_values if value > 0),
        "maximum_drawdown_ticks": _integer_equity_shape(net_values),
        "daily_net_ticks": [list(value) for value in daily_net_ticks],
        "null_world": result.null_world,
        "outer_validations": [value.as_dict() for value in outer],
        "raw_signal_count": len(signals),
        "reporting_groups": [value.as_dict() for value in reporting],
        "schema": ML_SEARCH_EVALUATION_SCHEMA,
        "total_net_ticks": sum(net_values),
        "total_stress_net_ticks": sum(value - 4 for value in net_values),
    }
    return MLSearchEconomicEvaluation(
        candidate_id=result.candidate_id,
        candidate_kind=candidate_kind,
        family_key=resolved_family_key,
        null_world=result.null_world,
        alignment_proof_sha256=alignment_proof_sha256,
        raw_signal_count=len(signals),
        active_signal_days=len({result.decision_dates[index] for index in signals}),
        fill_count=len(fills),
        active_entry_days=len({result.decision_dates[index] for index in fills}),
        total_net_ticks=sum(net_values),
        total_stress_net_ticks=sum(value - 4 for value in net_values),
        gross_profit_ticks=sum(value for value in net_values if value > 0),
        gross_loss_ticks=-sum(value for value in net_values if value < 0),
        maximum_drawdown_ticks=_integer_equity_shape(net_values),
        action_identities=action_identities,
        daily_net_ticks=daily_net_ticks,
        reporting_groups=reporting,
        outer_validations=outer,
        artifact_sha256=canonical_sha256(definition),
    )


@dataclass(frozen=True, slots=True)
class MLEvaluatedSearchControls:
    real: MLSearchEconomicEvaluation
    circular_target: MLSearchEconomicEvaluation
    matched_target: MLSearchEconomicEvaluation
    alignment_proof_sha256: str

    def __post_init__(self) -> None:
        values = (self.real, self.circular_target, self.matched_target)
        if (
            tuple(item.null_world for item in values) != NULL_WORLD_ORDER
            or len({item.candidate_id for item in values}) != 1
            or len({item.family_key for item in values}) != 1
            or any(item.alignment_proof_sha256 != self.alignment_proof_sha256 for item in values)
        ):
            raise AllCasesMLError("evaluated ML Search controls differ")


def evaluate_aligned_ml_search_controls(
    controls: AlignedCrossFitControls,
    *,
    reporting_group_by_date: Mapping[date, str],
    direct_matrix: TrainingMatrix | None = None,
    meta_matrices_by_world_and_fold: Mapping[str, Mapping[str, TrainingMatrix]] | None = None,
    meta_search_final_real_model: CanonicalMLModel | None = None,
) -> MLEvaluatedSearchControls:
    """Resolve and aggregate all three worlds only after exact count alignment."""

    if (direct_matrix is None) == (meta_matrices_by_world_and_fold is None):
        raise AllCasesMLError("choose exactly one direct or meta OOF outcome source")
    candidate = META_CANDIDATE_BY_ID.get(controls.real.candidate_id)
    family_key = None
    if meta_matrices_by_world_and_fold is not None:
        if (
            candidate is None
            or meta_search_final_real_model is None
            or meta_search_final_real_model.candidate_id != controls.real.candidate_id
            or meta_search_final_real_model.null_world != NullWorld.REAL.value
            or meta_search_final_real_model.fold_key != "SEARCH_FINAL"
        ):
            raise AllCasesMLError("meta OOF controls use a non-meta candidate")
        family_key = ml_candidate_family_key(
            candidate,
            base_trigger_family=meta_search_final_real_model.base_trigger_family,
        )
    elif meta_search_final_real_model is not None:
        raise AllCasesMLError("direct OOF controls cannot bind a meta final model")
    results = (controls.real, controls.circular_target, controls.matched_target)
    evaluations: list[MLSearchEconomicEvaluation] = []
    for result in results:
        net = (
            direct_crossfit_realized_net_ticks(result, direct_matrix)
            if direct_matrix is not None
            else meta_crossfit_realized_net_ticks(result, meta_matrices_by_world_and_fold or {})
        )
        evaluations.append(
            evaluate_ml_crossfit_economics(
                result,
                net,
                reporting_group_by_date=reporting_group_by_date,
                alignment_proof_sha256=controls.proof.artifact_sha256,
                family_key=family_key,
            )
        )
    return MLEvaluatedSearchControls(*evaluations, controls.proof.artifact_sha256)


@dataclass(frozen=True, slots=True)
class MLSearchGateResult:
    candidate_id: str
    candidate_kind: Literal["DIRECT", "META"]
    family_key: str
    eligible: bool
    rejection_reasons: tuple[str, ...]
    positive_reporting_group_count: int
    positive_outer_validation_count: int
    worst_outer_ev_numerator: int
    worst_outer_ev_denominator: int
    median_outer_ev_numerator: int
    median_outer_ev_denominator: int
    artifact_sha256: str

    @property
    def worst_outer_ev(self) -> Fraction:
        return Fraction(self.worst_outer_ev_numerator, self.worst_outer_ev_denominator)

    @property
    def median_outer_ev(self) -> Fraction:
        return Fraction(self.median_outer_ev_numerator, self.median_outer_ev_denominator)

    def definition_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "candidate_kind": self.candidate_kind,
            "eligible": self.eligible,
            "family_key": self.family_key,
            "median_outer_ev_denominator": self.median_outer_ev_denominator,
            "median_outer_ev_numerator": self.median_outer_ev_numerator,
            "positive_outer_validation_count": self.positive_outer_validation_count,
            "positive_reporting_group_count": self.positive_reporting_group_count,
            "rejection_reasons": list(self.rejection_reasons),
            "schema": ML_SEARCH_GATE_SCHEMA,
            "worst_outer_ev_denominator": self.worst_outer_ev_denominator,
            "worst_outer_ev_numerator": self.worst_outer_ev_numerator,
        }

    def __post_init__(self) -> None:
        if (
            len(self.candidate_id) != 64
            or self.candidate_kind not in {"DIRECT", "META"}
            or len(self.family_key) != 64
            or self.eligible == bool(self.rejection_reasons)
            or self.worst_outer_ev_denominator <= 0
            or self.median_outer_ev_denominator <= 0
            or canonical_sha256(self.definition_dict()) != self.artifact_sha256
        ):
            raise AllCasesMLError("ML Search gate result differs")

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def apply_ml_search_gates(controls: MLEvaluatedSearchControls) -> MLSearchGateResult:
    """Apply the shared sample/economic/null Search gates to aligned ML OOF evidence."""

    real = controls.real
    reasons: list[str] = []
    if real.raw_signal_count < 60:
        reasons.append("RAW_SIGNALS_LT_60")
    if real.active_signal_days < 40:
        reasons.append("ACTIVE_SIGNAL_DAYS_LT_40")
    if any(value.raw_signal_count < 6 for value in real.reporting_groups):
        reasons.append("REPORTING_RAW_SIGNALS_LT_6")
    if any(value.raw_signal_count < 6 for value in real.outer_validations):
        reasons.append("OUTER_RAW_SIGNALS_LT_6")
    if real.fill_count < 48:
        reasons.append("COMPLETE_FILLS_LT_48")
    if real.active_entry_days < 30:
        reasons.append("ACTIVE_ENTRY_DAYS_LT_30")
    if any(value.fill_count < 5 for value in real.outer_validations):
        reasons.append("OUTER_COMPLETE_FILLS_LT_5")
    positive_reporting = sum(value.net_ticks > 0 for value in real.reporting_groups)
    if positive_reporting < 3:
        reasons.append("POSITIVE_REPORTING_GROUPS_LT_3")
    positive_outer = sum(value.net_ticks > 0 for value in real.outer_validations)
    if positive_outer < 4:
        reasons.append("POSITIVE_OUTER_VALIDATIONS_LT_4")
    if real.total_net_ticks <= 0:
        reasons.append("NET_TICKS_NOT_POSITIVE")
    if real.total_stress_net_ticks <= 0:
        reasons.append("STRESS_18_TICK_NET_NOT_POSITIVE")
    if real.gross_profit_ticks == 0 or (
        real.gross_loss_ticks > 0 and 20 * real.gross_profit_ticks < 21 * real.gross_loss_ticks
    ):
        reasons.append("PROFIT_FACTOR_LT_21_OVER_20")
    if real.total_net_ticks <= controls.circular_target.total_net_ticks:
        reasons.append("REAL_NET_NOT_ABOVE_CIRCULAR_TARGET")
    if real.total_net_ticks <= controls.matched_target.total_net_ticks:
        reasons.append("REAL_NET_NOT_ABOVE_MATCHED_TARGET")
    outer_evs = sorted(
        Fraction(value.net_ticks, value.fill_count) if value.fill_count else Fraction(-(10**18))
        for value in real.outer_validations
    )
    worst = outer_evs[0]
    median = outer_evs[len(outer_evs) // 2]
    definition = {
        "candidate_id": real.candidate_id,
        "candidate_kind": real.candidate_kind,
        "eligible": not reasons,
        "family_key": real.family_key,
        "median_outer_ev_denominator": median.denominator,
        "median_outer_ev_numerator": median.numerator,
        "positive_outer_validation_count": positive_outer,
        "positive_reporting_group_count": positive_reporting,
        "rejection_reasons": reasons,
        "schema": ML_SEARCH_GATE_SCHEMA,
        "worst_outer_ev_denominator": worst.denominator,
        "worst_outer_ev_numerator": worst.numerator,
    }
    return MLSearchGateResult(
        real.candidate_id,
        real.candidate_kind,
        real.family_key,
        not reasons,
        tuple(reasons),
        positive_reporting,
        positive_outer,
        worst.numerator,
        worst.denominator,
        median.numerator,
        median.denominator,
        canonical_sha256(definition),
    )


@dataclass(frozen=True, slots=True)
class MLSearchSelection:
    classification: str
    selected_candidate_ids: tuple[str, ...]
    eligible_count: int
    artifact_sha256: str

    def definition_dict(self) -> dict[str, object]:
        return {
            "classification": self.classification,
            "eligible_count": self.eligible_count,
            "schema": ML_SEARCH_GATE_SCHEMA,
            "selected_candidate_ids": list(self.selected_candidate_ids),
        }

    def __post_init__(self) -> None:
        if (
            self.classification not in {"ML_CANDIDATES_SELECTED", "NO_ML_CANDIDATES"}
            or len(self.selected_candidate_ids) > 8
            or len(set(self.selected_candidate_ids)) != len(self.selected_candidate_ids)
            or canonical_sha256(self.definition_dict()) != self.artifact_sha256
        ):
            raise AllCasesMLError("ML Search selection differs")

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def _ml_search_pair_is_diverse(
    left: MLSearchEconomicEvaluation,
    right: MLSearchEconomicEvaluation,
) -> bool:
    """Apply the exact precommitted action-Jaccard and daily-PnL rules."""

    left_actions = set(left.action_identities)
    right_actions = set(right.action_identities)
    union_count = len(left_actions | right_actions)
    if union_count == 0 or 5 * len(left_actions & right_actions) >= 4 * union_count:
        return False

    left_daily = dict(left.daily_net_ticks)
    right_daily = dict(right.daily_net_ticks)
    if tuple(left_daily) != tuple(right_daily):
        raise AllCasesMLError("ML Search diversity daily domains differ")
    count = len(left_daily)
    if count < 2:
        return False
    left_values = tuple(left_daily[key] for key in left_daily)
    right_values = tuple(right_daily[key] for key in left_daily)
    covariance_numerator = count * sum(
        left_value * right_value
        for left_value, right_value in zip(
            left_values,
            right_values,
            strict=True,
        )
    ) - sum(left_values) * sum(right_values)
    left_variance_product_term = (
        count * sum(value * value for value in left_values) - sum(left_values) ** 2
    )
    right_variance_product_term = (
        count * sum(value * value for value in right_values) - sum(right_values) ** 2
    )
    if left_variance_product_term <= 0 or right_variance_product_term <= 0:
        return False
    return (
        25 * covariance_numerator * covariance_numerator
        < 16 * left_variance_product_term * right_variance_product_term
    )


def select_ml_search_candidates(
    evaluations: Sequence[MLSearchEconomicEvaluation],
    gate_results: Sequence[MLSearchGateResult],
) -> MLSearchSelection:
    """Rank then greedily diversify with direct<=4, meta<=4, family<=2."""

    evaluation_by_id = {
        value.candidate_id: value for value in evaluations if value.null_world == "REAL"
    }
    gates = tuple(gate_results)
    if len({value.candidate_id for value in gates}) != len(gates) or any(
        value.candidate_id not in evaluation_by_id
        or value.candidate_kind != evaluation_by_id[value.candidate_id].candidate_kind
        or value.family_key != evaluation_by_id[value.candidate_id].family_key
        for value in gates
    ):
        raise AllCasesMLError("ML Search selection evidence differs")
    eligible = [value for value in gates if value.eligible]
    eligible.sort(
        key=lambda value: (
            -value.positive_outer_validation_count,
            -value.worst_outer_ev,
            -evaluation_by_id[value.candidate_id].total_stress_net_ticks,
            -value.median_outer_ev,
            evaluation_by_id[value.candidate_id].maximum_drawdown_ticks,
            value.candidate_id,
        )
    )
    selected: list[str] = []
    kind_counts: dict[str, int] = defaultdict(int)
    family_counts: dict[str, int] = defaultdict(int)
    for value in eligible:
        if kind_counts[value.candidate_kind] >= 4 or family_counts[value.family_key] >= 2:
            continue
        evaluation = evaluation_by_id[value.candidate_id]
        if any(
            not _ml_search_pair_is_diverse(evaluation, evaluation_by_id[selected_id])
            for selected_id in selected
        ):
            continue
        selected.append(value.candidate_id)
        kind_counts[value.candidate_kind] += 1
        family_counts[value.family_key] += 1
    classification = "ML_CANDIDATES_SELECTED" if selected else "NO_ML_CANDIDATES"
    definition = {
        "classification": classification,
        "eligible_count": len(eligible),
        "schema": ML_SEARCH_GATE_SCHEMA,
        "selected_candidate_ids": selected,
    }
    return MLSearchSelection(
        classification,
        tuple(selected),
        len(eligible),
        canonical_sha256(definition),
    )


def _prediction_input_sha256(
    values: np.ndarray,
    row_ids: Sequence[str],
    auxiliary: np.ndarray,
    *,
    auxiliary_label: str,
) -> str:
    digest = hashlib.sha256()
    digest.update(auxiliary_label.encode("ascii") + b"\0")
    matrix = np.asarray(values, dtype=np.float64)
    side = np.asarray(auxiliary)
    for index, row_id in enumerate(row_ids):
        digest.update(row_id.encode("utf-8") + b"\0")
        for value in matrix[index]:
            encoded = "nan" if math.isnan(float(value)) else _float_hex(float(value))
            digest.update(encoded.encode("ascii") + b"\0")
        digest.update(str(side[index]).encode("ascii") + b"\0")
    return digest.hexdigest()


def stage_partition_date_plan_sha256(stage_key: str, decision_dates: Sequence[date]) -> str:
    """Canonical upstream-plan commitment for one complete OOS date domain."""

    dates = tuple(decision_dates)
    if (
        stage_key not in {"WF1", "WF2", "WF3", "WF4", "WF5", "HOLDOUT"}
        or not dates
        or any(isinstance(value, datetime) or not isinstance(value, date) for value in dates)
        or tuple(sorted(set(dates))) != dates
    ):
        raise AllCasesMLError("stage partition date plan differs")
    return canonical_sha256(
        {
            "decision_dates": [value.isoformat() for value in dates],
            "schema": "systematic_fx.ai_all_cases_stage_partition_date_plan.v1",
            "stage_key": stage_key,
        }
    )


@dataclass(frozen=True, slots=True)
class StagePartitionDateCertificate:
    """Typed proof of the authoritative full decision-date domain for one stage."""

    stage_key: str
    decision_dates: tuple[date, ...]
    upstream_plan_sha256: str
    artifact_sha256: str

    def definition_dict(self) -> dict[str, object]:
        return {
            "decision_dates": [value.isoformat() for value in self.decision_dates],
            "schema": STAGE_PARTITION_DATE_CERTIFICATE_SCHEMA,
            "stage_key": self.stage_key,
            "upstream_plan_sha256": self.upstream_plan_sha256,
        }

    def __post_init__(self) -> None:
        if (
            not isinstance(self.decision_dates, tuple)
            or self.upstream_plan_sha256
            != stage_partition_date_plan_sha256(self.stage_key, self.decision_dates)
            or not _is_sha256(self.artifact_sha256)
            or canonical_sha256(self.definition_dict()) != self.artifact_sha256
        ):
            raise AllCasesMLError("stage partition date certificate differs")

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}

    @classmethod
    def from_dict(cls, value: object) -> StagePartitionDateCertificate:
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "artifact_sha256",
                "decision_dates",
                "schema",
                "stage_key",
                "upstream_plan_sha256",
            }
            or value["schema"] != STAGE_PARTITION_DATE_CERTIFICATE_SCHEMA
            or not isinstance(value["artifact_sha256"], str)
            or not isinstance(value["stage_key"], str)
            or not isinstance(value["upstream_plan_sha256"], str)
            or not isinstance(value["decision_dates"], list)
            or any(not isinstance(item, str) for item in value["decision_dates"])
        ):
            raise AllCasesMLError("stage partition date certificate document differs")
        try:
            result = cls(
                value["stage_key"],
                tuple(date.fromisoformat(item) for item in value["decision_dates"]),
                value["upstream_plan_sha256"],
                value["artifact_sha256"],
            )
        except ValueError as error:
            raise AllCasesMLError("stage partition date certificate date differs") from error
        if result.as_dict() != value:
            raise AllCasesMLError("stage partition date certificate did not round trip")
        return result


def build_stage_partition_date_certificate(
    stage_key: str,
    decision_dates: Sequence[date],
    *,
    upstream_plan_sha256: str,
) -> StagePartitionDateCertificate:
    dates = tuple(decision_dates)
    definition = {
        "decision_dates": [value.isoformat() for value in dates],
        "schema": STAGE_PARTITION_DATE_CERTIFICATE_SCHEMA,
        "stage_key": stage_key,
        "upstream_plan_sha256": upstream_plan_sha256,
    }
    return StagePartitionDateCertificate(
        stage_key,
        dates,
        upstream_plan_sha256,
        canonical_sha256(definition),
    )


@dataclass(frozen=True, slots=True)
class OutcomeFreeExecutionSchedule:
    """Verified, outcome-free opportunity lineage used before any OOS scoring."""

    candidate_id: str
    feature_set_id: str
    task_timeframe_seconds: int
    task_horizon_seconds: int
    row_ids: tuple[str, ...]
    decision_dates: tuple[date, ...]
    partition_date_certificate: StagePartitionDateCertificate
    decision_ns: tuple[int, ...]
    entry_ns: tuple[int, ...]
    planned_exit_ns: tuple[int, ...]
    contracts: tuple[str, ...]
    outcome_span_ids: tuple[int, ...]
    segment_ids: tuple[int, ...]
    stage_key: str
    lineage_sha256: str
    opportunity_lattice_sha256: str
    entry_schedule_sha256: str
    feature_rows_sha256: str

    @property
    def partition_decision_dates(self) -> tuple[date, ...]:
        return self.partition_date_certificate.decision_dates

    def __post_init__(self) -> None:
        count = len(self.row_ids)
        candidate = DIRECT_CANDIDATE_BY_ID.get(self.candidate_id)
        if (
            candidate is None
            or self.feature_set_id != candidate.feature_set_id
            or self.task_timeframe_seconds != candidate.decision_timeframe_seconds
            or self.task_horizon_seconds != candidate.horizon_seconds
            or any(
                isinstance(value, bool) or not isinstance(value, int) or value <= 0
                for value in (self.task_timeframe_seconds, self.task_horizon_seconds)
            )
            or not count
            or any(not isinstance(value, str) or not value for value in self.row_ids)
            or len(set(self.row_ids)) != count
            or len(self.decision_dates) != count
            or len(self.decision_ns) != count
            or len(self.entry_ns) != count
            or len(self.planned_exit_ns) != count
            or len(self.contracts) != count
            or len(self.outcome_span_ids) != count
            or len(self.segment_ids) != count
            or any(
                isinstance(value, datetime) or not isinstance(value, date)
                for value in self.decision_dates
            )
            or not isinstance(self.partition_date_certificate, StagePartitionDateCertificate)
            or self.partition_date_certificate.stage_key != self.stage_key
            or not set(self.decision_dates).issubset(self.partition_decision_dates)
            or any(not isinstance(value, str) or not value for value in self.contracts)
            or any(
                not _is_exact_int64_scalar(value)
                for values in (
                    self.decision_ns,
                    self.entry_ns,
                    self.planned_exit_ns,
                    self.outcome_span_ids,
                    self.segment_ids,
                )
                for value in values
            )
            or any(value <= 0 for value in self.outcome_span_ids)
            or any(value <= 0 for value in self.segment_ids)
            or any(
                decision <= 0 or entry < decision or exit_ns <= entry
                for decision, entry, exit_ns in zip(
                    self.decision_ns,
                    self.entry_ns,
                    self.planned_exit_ns,
                    strict=True,
                )
            )
            or any(
                exit_ns != entry + self.task_horizon_seconds * 1_000_000_000
                for entry, exit_ns in zip(
                    self.entry_ns,
                    self.planned_exit_ns,
                    strict=True,
                )
            )
            or any(entry % 1_000_000_000 != 0 for entry in self.entry_ns)
            or tuple(zip(self.decision_ns, self.row_ids, strict=True))
            != tuple(sorted(zip(self.decision_ns, self.row_ids, strict=True)))
            or tuple(zip(self.entry_ns, self.row_ids, strict=True))
            != tuple(sorted(zip(self.entry_ns, self.row_ids, strict=True)))
            or not isinstance(self.stage_key, str)
            or self.stage_key not in {"WF1", "WF2", "WF3", "WF4", "WF5", "HOLDOUT"}
            or any(
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
                for value in (
                    self.lineage_sha256,
                    self.opportunity_lattice_sha256,
                    self.entry_schedule_sha256,
                    self.feature_rows_sha256,
                )
            )
        ):
            raise AllCasesMLError("outcome-free execution schedule differs")

    def definition_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "feature_set_id": self.feature_set_id,
            "lineage_sha256": self.lineage_sha256,
            "opportunity_lattice_sha256": self.opportunity_lattice_sha256,
            "entry_schedule_sha256": self.entry_schedule_sha256,
            "partition_date_certificate": self.partition_date_certificate.as_dict(),
            "feature_rows_sha256": self.feature_rows_sha256,
            "rows": [
                {
                    "contract": contract,
                    "decision_ns": decision,
                    "decision_date": decision_date.isoformat(),
                    "entry_ns": entry,
                    "outcome_span_id": span,
                    "planned_exit_ns": exit_ns,
                    "row_id": row_id,
                    "segment_id": segment,
                }
                for row_id, decision_date, decision, entry, exit_ns, contract, span, segment in zip(
                    self.row_ids,
                    self.decision_dates,
                    self.decision_ns,
                    self.entry_ns,
                    self.planned_exit_ns,
                    self.contracts,
                    self.outcome_span_ids,
                    self.segment_ids,
                    strict=True,
                )
            ],
            "schema": EXECUTION_SCHEDULE_SCHEMA,
            "stage_key": self.stage_key,
            "task_horizon_seconds": self.task_horizon_seconds,
            "task_timeframe_seconds": self.task_timeframe_seconds,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.definition_dict())

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "schedule_sha256": self.sha256}

    @classmethod
    def from_dict(cls, value: object) -> OutcomeFreeExecutionSchedule:
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "candidate_id",
                "entry_schedule_sha256",
                "feature_set_id",
                "lineage_sha256",
                "opportunity_lattice_sha256",
                "partition_date_certificate",
                "feature_rows_sha256",
                "rows",
                "schedule_sha256",
                "schema",
                "stage_key",
                "task_horizon_seconds",
                "task_timeframe_seconds",
            }
            or value["schema"] != EXECUTION_SCHEDULE_SCHEMA
        ):
            raise AllCasesMLError("outcome-free schedule document differs")
        rows = value["rows"]
        row_keys = {
            "contract",
            "decision_date",
            "decision_ns",
            "entry_ns",
            "outcome_span_id",
            "planned_exit_ns",
            "row_id",
            "segment_id",
        }
        string_keys = (
            "entry_schedule_sha256",
            "candidate_id",
            "feature_set_id",
            "lineage_sha256",
            "opportunity_lattice_sha256",
            "feature_rows_sha256",
            "schedule_sha256",
            "stage_key",
        )
        integer_keys = (
            "decision_ns",
            "entry_ns",
            "outcome_span_id",
            "planned_exit_ns",
            "segment_id",
        )
        if (
            any(not isinstance(value[key], str) for key in string_keys)
            or any(
                isinstance(value[key], bool) or not isinstance(value[key], int)
                for key in ("task_horizon_seconds", "task_timeframe_seconds")
            )
            or not isinstance(rows, list)
            or any(not isinstance(item, dict) or set(item) != row_keys for item in rows)
            or any(
                not isinstance(item["row_id"], str)
                or not isinstance(item["contract"], str)
                or not isinstance(item["decision_date"], str)
                or any(
                    isinstance(item[key], bool) or not isinstance(item[key], int)
                    for key in integer_keys
                )
                for item in rows
            )
        ):
            raise AllCasesMLError("outcome-free schedule document values differ")
        try:
            result = cls(
                value["candidate_id"],
                value["feature_set_id"],
                value["task_timeframe_seconds"],
                value["task_horizon_seconds"],
                tuple(item["row_id"] for item in rows),
                tuple(date.fromisoformat(item["decision_date"]) for item in rows),
                StagePartitionDateCertificate.from_dict(value["partition_date_certificate"]),
                tuple(item["decision_ns"] for item in rows),
                tuple(item["entry_ns"] for item in rows),
                tuple(item["planned_exit_ns"] for item in rows),
                tuple(item["contract"] for item in rows),
                tuple(item["outcome_span_id"] for item in rows),
                tuple(item["segment_id"] for item in rows),
                value["stage_key"],
                value["lineage_sha256"],
                value["opportunity_lattice_sha256"],
                value["entry_schedule_sha256"],
                value["feature_rows_sha256"],
            )
        except ValueError as error:
            raise AllCasesMLError("outcome-free schedule date differs") from error
        if result.as_dict() != value:
            raise AllCasesMLError("outcome-free schedule did not round trip")
        return result


def build_direct_outcome_free_execution_schedule(
    candidate: DirectCandidate,
    feature_rows: CausalFeatureRows,
    *,
    partition_key: Literal["WF1", "WF2", "WF3", "WF4", "WF5", "HOLDOUT"],
    partition_date_certificate: StagePartitionDateCertificate,
    outcome_lineage_sha256: str,
    opportunity_lattice: StructuralOpportunityLattice,
) -> OutcomeFreeExecutionSchedule:
    """Freeze direct decision/entry timing before any partition 1s outcome is opened."""

    if (
        DIRECT_CANDIDATE_BY_ID.get(candidate.candidate_id) != candidate
        or feature_rows.feature_set_id != candidate.feature_set_id
        or feature_rows.decision_timeframe_seconds != candidate.decision_timeframe_seconds
        or feature_rows.stage_key != partition_key
        or not isinstance(partition_date_certificate, StagePartitionDateCertificate)
        or partition_date_certificate.stage_key != partition_key
        or opportunity_lattice.stage_key != partition_key
        or feature_rows.entry_schedule_sha256 != opportunity_lattice.entry_schedule_sha256
        or not set(feature_rows.row_ids).issubset(opportunity_lattice.eligible_row_ids)
        or np.any(feature_rows.entry_ns >= feature_rows.decision_ns + 300 * 1_000_000_000)
    ):
        raise AllCasesMLError("direct outcome-free schedule binding differs")
    lattice_by_row_id = {
        row_id: (
            opportunity_lattice.eligible[index],
            opportunity_lattice.decision_ns[index],
            opportunity_lattice.entry_ns[index],
            opportunity_lattice.source_dates[index],
            opportunity_lattice.contracts[index],
            opportunity_lattice.outcome_span_ids[index],
            opportunity_lattice.segment_ids[index],
        )
        for index, row_id in enumerate(opportunity_lattice.row_ids)
    }
    if any(
        lattice_by_row_id.get(row_id)
        != (
            True,
            int(feature_rows.decision_ns[index]),
            int(feature_rows.entry_ns[index]),
            feature_rows.source_dates[index],
            feature_rows.contracts[index],
            int(feature_rows.outcome_span_ids[index]),
            int(feature_rows.segment_ids[index]),
        )
        for index, row_id in enumerate(feature_rows.row_ids)
    ):
        raise AllCasesMLError("direct feature/lattice row identity differs")
    return OutcomeFreeExecutionSchedule(
        candidate_id=candidate.candidate_id,
        feature_set_id=candidate.feature_set_id,
        task_timeframe_seconds=candidate.decision_timeframe_seconds,
        task_horizon_seconds=candidate.horizon_seconds,
        row_ids=feature_rows.row_ids,
        decision_dates=feature_rows.source_dates,
        partition_date_certificate=partition_date_certificate,
        decision_ns=tuple(int(value) for value in feature_rows.decision_ns),
        entry_ns=tuple(int(value) for value in feature_rows.entry_ns),
        planned_exit_ns=tuple(
            int(value) + candidate.horizon_seconds * 1_000_000_000
            for value in feature_rows.entry_ns
        ),
        contracts=feature_rows.contracts,
        outcome_span_ids=tuple(int(value) for value in feature_rows.outcome_span_ids),
        segment_ids=tuple(int(value) for value in feature_rows.segment_ids),
        stage_key=partition_key,
        lineage_sha256=outcome_lineage_sha256,
        opportunity_lattice_sha256=opportunity_lattice.artifact_sha256,
        entry_schedule_sha256=feature_rows.entry_schedule_sha256,
        feature_rows_sha256=feature_rows.artifact_sha256,
    )


@dataclass(frozen=True, slots=True)
class MetaAnchorGateSchedule:
    """Feature-only symbolic order identities scored at each anchor decision."""

    candidate_id: str
    base_strategy_id: str
    base_trigger_family: str
    symbolic_ranking_certificate: SymbolicRankingCertificate
    feature_set_id: str
    feature_row_ids: tuple[str, ...]
    order_ids: tuple[str, ...]
    anchor_keys: tuple[tuple[str, int, int, int, str], ...]
    decision_dates: tuple[date, ...]
    decision_ns: tuple[int, ...]
    base_directions: tuple[TradeDirection, ...]
    partition_key: str
    symbolic_order_batch_sha256: str
    feature_rows_sha256: str
    expert_artifact_sha256s: tuple[str, ...]
    expert_formula_sha256: str
    artifact_sha256: str

    def definition_dict(self) -> dict[str, object]:
        return {
            "base_strategy_id": self.base_strategy_id,
            "base_trigger_family": self.base_trigger_family,
            "candidate_id": self.candidate_id,
            "feature_set_id": self.feature_set_id,
            "feature_rows_sha256": self.feature_rows_sha256,
            "partition_key": self.partition_key,
            "rows": [
                {
                    "anchor_key": list(anchor_key),
                    "base_direction": direction.value,
                    "decision_date": decision_date.isoformat(),
                    "decision_ns": decision,
                    "feature_row_id": feature_row_id,
                    "order_id": order_id,
                }
                for feature_row_id, order_id, anchor_key, decision_date, decision, direction in zip(
                    self.feature_row_ids,
                    self.order_ids,
                    self.anchor_keys,
                    self.decision_dates,
                    self.decision_ns,
                    self.base_directions,
                    strict=True,
                )
            ],
            "schema": META_GATE_SCHEDULE_SCHEMA,
            "expert_artifact_sha256s": list(self.expert_artifact_sha256s),
            "expert_formula_sha256": self.expert_formula_sha256,
            "symbolic_order_batch_sha256": self.symbolic_order_batch_sha256,
            "symbolic_ranking_certificate": self.symbolic_ranking_certificate.as_dict(),
        }

    def __post_init__(self) -> None:
        from .symbolic import EXPERT_FEATURE_FORMULA_SHA256

        candidate = META_CANDIDATE_BY_ID.get(self.candidate_id)
        count = len(self.order_ids)
        ranked_strategy = (
            None
            if candidate is None
            else self.symbolic_ranking_certificate.strategy_at_rank(candidate.symbolic_rank_slot)
        )
        if (
            candidate is None
            or ranked_strategy is None
            or self.base_strategy_id != ranked_strategy.strategy_id
            or self.base_trigger_family != ranked_strategy.trigger_family
            or self.feature_set_id != candidate.feature_set_id
            or self.symbolic_ranking_certificate.fold_key != "SEARCH_FINAL"
            or not count
            or len(set(self.feature_row_ids)) != count
            or len(set(self.order_ids)) != count
            or any(
                len(values) != count
                for values in (
                    self.feature_row_ids,
                    self.anchor_keys,
                    self.decision_dates,
                    self.decision_ns,
                    self.base_directions,
                )
            )
            or any(
                isinstance(value, datetime) or not isinstance(value, date)
                for value in self.decision_dates
            )
            or any(
                len(order_id) != 64
                or any(character not in "0123456789abcdef" for character in order_id)
                for order_id in self.order_ids
            )
            or any(
                len(key) != 5
                or not isinstance(key[0], str)
                or not key[0]
                or any(
                    isinstance(key[index], bool) or not isinstance(key[index], int)
                    for index in (1, 2, 3)
                )
                or key[1] <= 0
                or key[2] <= 0
                or key[3] <= 0
                or key[4] not in {"LONG", "SHORT"}
                for key in self.anchor_keys
            )
            or tuple(key[3] for key in self.anchor_keys) != self.decision_ns
            or tuple(key[4] for key in self.anchor_keys)
            != tuple(direction.value for direction in self.base_directions)
            or tuple(zip(self.decision_ns, self.order_ids, strict=True))
            != tuple(sorted(zip(self.decision_ns, self.order_ids, strict=True)))
            or self.partition_key not in {"WF1", "WF2", "WF3", "WF4", "WF5", "HOLDOUT"}
            or len(self.symbolic_order_batch_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.symbolic_order_batch_sha256
            )
            or not _is_sha256(self.feature_rows_sha256)
            or len(self.expert_artifact_sha256s) != count
            or any(
                len(value) != 64 or any(character not in "0123456789abcdef" for character in value)
                for value in self.expert_artifact_sha256s
            )
            or self.expert_formula_sha256 != EXPERT_FEATURE_FORMULA_SHA256
            or len(self.artifact_sha256) != 64
            or canonical_sha256(self.definition_dict()) != self.artifact_sha256
        ):
            raise AllCasesMLError("meta anchor-gate schedule differs")

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}

    @classmethod
    def from_dict(cls, value: object) -> MetaAnchorGateSchedule:
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "artifact_sha256",
                "base_strategy_id",
                "base_trigger_family",
                "candidate_id",
                "expert_artifact_sha256s",
                "expert_formula_sha256",
                "feature_set_id",
                "feature_rows_sha256",
                "partition_key",
                "rows",
                "schema",
                "symbolic_order_batch_sha256",
                "symbolic_ranking_certificate",
            }
            or value["schema"] != META_GATE_SCHEDULE_SCHEMA
        ):
            raise AllCasesMLError("meta anchor-gate schedule document differs")
        string_keys = (
            "artifact_sha256",
            "base_strategy_id",
            "base_trigger_family",
            "candidate_id",
            "expert_formula_sha256",
            "feature_set_id",
            "feature_rows_sha256",
            "partition_key",
            "symbolic_order_batch_sha256",
        )
        rows = value["rows"]
        expert_sha256s = value["expert_artifact_sha256s"]
        row_keys = {
            "anchor_key",
            "base_direction",
            "decision_date",
            "decision_ns",
            "feature_row_id",
            "order_id",
        }
        if (
            any(not isinstance(value[key], str) for key in string_keys)
            or not isinstance(expert_sha256s, list)
            or any(not isinstance(item, str) for item in expert_sha256s)
            or not isinstance(rows, list)
            or any(not isinstance(item, dict) or set(item) != row_keys for item in rows)
            or any(
                not isinstance(item["anchor_key"], list)
                or len(item["anchor_key"]) != 5
                or not isinstance(item["base_direction"], str)
                or not isinstance(item["decision_date"], str)
                or isinstance(item["decision_ns"], bool)
                or not isinstance(item["decision_ns"], int)
                or not isinstance(item["feature_row_id"], str)
                or not isinstance(item["order_id"], str)
                for item in rows
            )
        ):
            raise AllCasesMLError("meta anchor-gate schedule values differ")
        try:
            anchor_keys = tuple(
                (
                    item["anchor_key"][0],
                    item["anchor_key"][1],
                    item["anchor_key"][2],
                    item["anchor_key"][3],
                    item["anchor_key"][4],
                )
                for item in rows
            )
            result = cls(
                value["candidate_id"],
                value["base_strategy_id"],
                value["base_trigger_family"],
                SymbolicRankingCertificate.from_dict(value["symbolic_ranking_certificate"]),
                value["feature_set_id"],
                tuple(item["feature_row_id"] for item in rows),
                tuple(item["order_id"] for item in rows),
                anchor_keys,
                tuple(date.fromisoformat(item["decision_date"]) for item in rows),
                tuple(item["decision_ns"] for item in rows),
                tuple(TradeDirection(item["base_direction"]) for item in rows),
                value["partition_key"],
                value["symbolic_order_batch_sha256"],
                value["feature_rows_sha256"],
                tuple(expert_sha256s),
                value["expert_formula_sha256"],
                value["artifact_sha256"],
            )
        except (TypeError, ValueError) as error:
            raise AllCasesMLError("meta anchor-gate schedule value differs") from error
        if result.as_dict() != value:
            raise AllCasesMLError("meta anchor-gate schedule did not round trip")
        return result


def build_meta_anchor_gate_schedule(
    candidate: MetaCandidate,
    feature_rows: CausalFeatureRows,
    *,
    base_order_batch: EntryOrderBatch,
    strategy_recipe: CompleteStrategyRecipe,
    expert_artifacts: Sequence[CausalExpertFeatureArtifact],
    partition_key: Literal["WF1", "WF2", "WF3", "WF4", "WF5", "HOLDOUT"],
    symbolic_ranking_certificate: SymbolicRankingCertificate,
) -> MetaAnchorGateSchedule:
    """Bind anchor-time features to frozen symbolic intents without fill/exit fields."""

    from .symbolic import EntryOrderBatch, FrozenEntryOrder

    if (
        META_CANDIDATE_BY_ID.get(candidate.candidate_id) != candidate
        or feature_rows.stage_key != partition_key
        or feature_rows.feature_set_id != candidate.feature_set_id
        or symbolic_ranking_certificate.fold_key != "SEARCH_FINAL"
        or not isinstance(base_order_batch, EntryOrderBatch)
    ):
        raise AllCasesMLError("meta anchor-gate request differs")
    ranked_strategy, typed_experts = _require_meta_recipe_inputs(
        candidate,
        feature_rows,
        symbolic_ranking_certificate=symbolic_ranking_certificate,
        strategy_recipe=strategy_recipe,
        base_order_batch=base_order_batch,
        expert_artifacts=expert_artifacts,
        scope_key=partition_key,
    )
    orders = base_order_batch.orders
    if not orders or any(not isinstance(item, FrozenEntryOrder) for item in orders):
        raise AllCasesMLError("meta anchor gate requires typed frozen symbolic orders")
    row_index_by_id = {row_id: index for index, row_id in enumerate(feature_rows.row_ids)}
    if len(row_index_by_id) != feature_rows.row_count or any(
        order.order_id not in row_index_by_id for order in orders
    ):
        raise AllCasesMLError("meta feature rows are not keyed by symbolic order_id")
    indexes = tuple(row_index_by_id[order.order_id] for order in orders)
    anchor_keys = tuple(
        (
            order.anchor.contract,
            int(order.anchor.outcome_span_id),
            int(order.anchor.segment_id),
            int(order.anchor.anchor_ns),
            str(order.anchor.direction),
        )
        for order in orders
    )
    directions = tuple(TradeDirection(key[4]) for key in anchor_keys)
    if any(
        feature_rows.source_dates[index] != order.anchor.source_date
        or feature_rows.contracts[index] != order.anchor.contract
        or int(feature_rows.outcome_span_ids[index]) != order.anchor.outcome_span_id
        or int(feature_rows.segment_ids[index]) != order.anchor.segment_id
        or int(feature_rows.decision_ns[index]) != order.anchor.anchor_ns
        for index, order in zip(indexes, orders, strict=True)
    ):
        raise AllCasesMLError("meta feature row/anchor identity differs")
    order_ids = tuple(order.order_id for order in orders)
    decisions = tuple(key[3] for key in anchor_keys)
    if tuple(zip(decisions, order_ids, strict=True)) != tuple(
        sorted(zip(decisions, order_ids, strict=True))
    ):
        raise AllCasesMLError("meta symbolic orders are not in anchor-time order")
    definition = {
        "base_strategy_id": ranked_strategy.strategy_id,
        "base_trigger_family": ranked_strategy.trigger_family,
        "candidate_id": candidate.candidate_id,
        "feature_set_id": candidate.feature_set_id,
        "feature_rows_sha256": feature_rows.artifact_sha256,
        "expert_artifact_sha256s": [item.artifact_sha256 for item in typed_experts],
        "expert_formula_sha256": typed_experts[0].formula_sha256,
        "partition_key": partition_key,
        "rows": [
            {
                "anchor_key": list(anchor_key),
                "base_direction": direction.value,
                "decision_date": feature_rows.source_dates[index].isoformat(),
                "decision_ns": decision,
                "feature_row_id": feature_rows.row_ids[index],
                "order_id": order_id,
            }
            for index, order_id, anchor_key, decision, direction in zip(
                indexes, order_ids, anchor_keys, decisions, directions, strict=True
            )
        ],
        "schema": META_GATE_SCHEDULE_SCHEMA,
        "symbolic_order_batch_sha256": base_order_batch.artifact_sha256,
        "symbolic_ranking_certificate": symbolic_ranking_certificate.as_dict(),
    }
    return MetaAnchorGateSchedule(
        candidate.candidate_id,
        ranked_strategy.strategy_id,
        ranked_strategy.trigger_family,
        symbolic_ranking_certificate,
        candidate.feature_set_id,
        tuple(feature_rows.row_ids[index] for index in indexes),
        order_ids,
        anchor_keys,
        tuple(feature_rows.source_dates[index] for index in indexes),
        decisions,
        directions,
        partition_key,
        base_order_batch.artifact_sha256,
        feature_rows.artifact_sha256,
        tuple(item.artifact_sha256 for item in typed_experts),
        typed_experts[0].formula_sha256,
        canonical_sha256(definition),
    )


@dataclass(frozen=True, slots=True)
class FrozenPredictionMask:
    """Direct-model WF/holdout mask made only from a Search-final artifact."""

    partition_key: str
    model_id: str
    model_sha256: str
    candidate_id: str
    base_trigger_family: str | None
    null_world: str
    prediction_input_sha256: str
    execution_schedule: OutcomeFreeExecutionSchedule
    predictions: PredictionBatch

    def __post_init__(self) -> None:
        if (
            self.partition_key not in {"WF1", "WF2", "WF3", "WF4", "WF5", "HOLDOUT"}
            or not _is_sha256(self.model_id)
            or not _is_sha256(self.model_sha256)
            or not _is_sha256(self.candidate_id)
            or self.null_world not in NULL_WORLD_ORDER
            or not _is_sha256(self.prediction_input_sha256)
            or self.predictions.model_sha256 != self.model_sha256
            or self.predictions.candidate_id != self.candidate_id
            or self.predictions.row_ids != self.execution_schedule.row_ids
            or self.execution_schedule.stage_key != self.partition_key
            or self.execution_schedule.candidate_id != self.candidate_id
            or self.candidate_id not in DIRECT_CANDIDATE_BY_ID
            or self.base_trigger_family is not None
        ):
            raise AllCasesMLError("frozen prediction-mask binding differs")

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "base_trigger_family": self.base_trigger_family,
            "model_id": self.model_id,
            "model_sha256": self.model_sha256,
            "null_world": self.null_world,
            "partition_key": self.partition_key,
            "prediction_input_sha256": self.prediction_input_sha256,
            "execution_schedule": self.execution_schedule.as_dict(),
            "predictions": self.predictions.as_dict(),
            "schema": FROZEN_MASK_SCHEMA,
        }

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.as_dict())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def from_dict(cls, value: object) -> FrozenPredictionMask:
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "base_trigger_family",
                "candidate_id",
                "execution_schedule",
                "model_id",
                "model_sha256",
                "null_world",
                "partition_key",
                "prediction_input_sha256",
                "predictions",
                "schema",
            }
            or value["schema"] != FROZEN_MASK_SCHEMA
        ):
            raise AllCasesMLError("frozen prediction-mask document differs")
        string_keys = (
            "candidate_id",
            "model_id",
            "model_sha256",
            "null_world",
            "partition_key",
            "prediction_input_sha256",
        )
        if (
            any(not isinstance(value[key], str) for key in string_keys)
            or value["base_trigger_family"] is not None
        ):
            raise AllCasesMLError("frozen prediction-mask document values differ")
        result = cls(
            value["partition_key"],
            value["model_id"],
            value["model_sha256"],
            value["candidate_id"],
            None,
            value["null_world"],
            value["prediction_input_sha256"],
            OutcomeFreeExecutionSchedule.from_dict(value["execution_schedule"]),
            PredictionBatch.from_dict(value["predictions"]),
        )
        if result.as_dict() != value:
            raise AllCasesMLError("frozen prediction-mask document did not round trip")
        return result


def freeze_prediction_mask(
    model: CanonicalMLModel,
    feature_rows: CausalFeatureRows,
    *,
    partition_key: str,
    execution_schedule: OutcomeFreeExecutionSchedule,
) -> FrozenPredictionMask:
    """Freeze a direct WF/holdout action mask; fitting/outcome inputs are impossible here."""

    candidate = DIRECT_CANDIDATE_BY_ID.get(model.candidate_id)
    if (
        model.fold_key != "SEARCH_FINAL"
        or candidate is None
        or not isinstance(feature_rows, CausalFeatureRows)
        or feature_rows.feature_set_id != candidate.feature_set_id
        or feature_rows.decision_timeframe_seconds != candidate.decision_timeframe_seconds
        or feature_rows.stage_key != partition_key
        or execution_schedule.stage_key != partition_key
        or execution_schedule.candidate_id != candidate.candidate_id
        or execution_schedule.feature_set_id != candidate.feature_set_id
        or execution_schedule.task_timeframe_seconds != candidate.decision_timeframe_seconds
        or execution_schedule.task_horizon_seconds != candidate.horizon_seconds
        or execution_schedule.row_ids != feature_rows.row_ids
        or execution_schedule.decision_dates != feature_rows.source_dates
        or execution_schedule.decision_ns != tuple(int(value) for value in feature_rows.decision_ns)
        or execution_schedule.entry_ns != tuple(int(value) for value in feature_rows.entry_ns)
        or execution_schedule.contracts != feature_rows.contracts
        or execution_schedule.outcome_span_ids
        != tuple(int(value) for value in feature_rows.outcome_span_ids)
        or execution_schedule.segment_ids != tuple(int(value) for value in feature_rows.segment_ids)
        or execution_schedule.entry_schedule_sha256 != feature_rows.entry_schedule_sha256
        or execution_schedule.feature_rows_sha256 != feature_rows.artifact_sha256
        or partition_key not in {"WF1", "WF2", "WF3", "WF4", "WF5", "HOLDOUT"}
    ):
        raise AllCasesMLError("direct OOS feature rows or SEARCH_FINAL schedule binding differs")
    timeframe_column = TF_ORDER.index(candidate.decision_timeframe_seconds)
    atr_ticks = feature_rows.atr_ticks_by_timeframe[:, timeframe_column]
    predictions = predict_actions(
        model,
        feature_rows.values,
        row_ids=execution_schedule.row_ids,
        atr_ticks=atr_ticks,
    )
    predictions = apply_nonoverlap_occupancy(
        predictions,
        entry_ns=np.asarray(execution_schedule.entry_ns, dtype=np.int64),
        planned_exit_ns=np.asarray(execution_schedule.planned_exit_ns, dtype=np.int64),
        contracts=execution_schedule.contracts,
    )
    return FrozenPredictionMask(
        partition_key,
        model.model_id,
        model.sha256,
        model.candidate_id,
        model.base_trigger_family,
        model.null_world,
        _prediction_input_sha256(
            feature_rows.values,
            execution_schedule.row_ids,
            atr_ticks,
            auxiliary_label="NATIVE_ATR_TICKS",
        ),
        execution_schedule,
        predictions,
    )


@dataclass(frozen=True, slots=True)
class FrozenMetaAnchorGate:
    """Search-final probability decisions over frozen symbolic order identities."""

    partition_key: str
    model_id: str
    model_sha256: str
    candidate_id: str
    null_world: str
    prediction_input_sha256: str
    schedule: MetaAnchorGateSchedule
    predictions: PredictionBatch

    def __post_init__(self) -> None:
        candidate = META_CANDIDATE_BY_ID.get(self.candidate_id)
        if (
            self.partition_key not in {"WF1", "WF2", "WF3", "WF4", "WF5", "HOLDOUT"}
            or candidate is None
            or self.schedule.partition_key != self.partition_key
            or self.schedule.candidate_id != self.candidate_id
            or self.schedule.symbolic_ranking_certificate.null_world != self.null_world
            or not _is_sha256(self.model_id)
            or not _is_sha256(self.model_sha256)
            or not _is_sha256(self.candidate_id)
            or self.null_world not in NULL_WORLD_ORDER
            or not _is_sha256(self.prediction_input_sha256)
            or self.predictions.model_sha256 != self.model_sha256
            or self.predictions.candidate_id != self.candidate_id
            or self.predictions.row_ids != self.schedule.order_ids
            or any(value is not None for value in self.predictions.estimated_net_edge_ticks)
            or any(
                admitted
                and direction is not base_direction
                or not admitted
                and direction is not TradeDirection.FLAT
                for admitted, direction, base_direction in zip(
                    self.predictions.admitted,
                    self.predictions.directions,
                    self.schedule.base_directions,
                    strict=True,
                )
            )
        ):
            raise AllCasesMLError("frozen meta anchor-gate binding differs")

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "model_id": self.model_id,
            "model_sha256": self.model_sha256,
            "null_world": self.null_world,
            "partition_key": self.partition_key,
            "prediction_input_sha256": self.prediction_input_sha256,
            "predictions": self.predictions.as_dict(),
            "schedule": self.schedule.as_dict(),
            "schema": META_GATE_MASK_SCHEMA,
        }

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.as_dict())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def from_dict(cls, value: object) -> FrozenMetaAnchorGate:
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "candidate_id",
                "model_id",
                "model_sha256",
                "null_world",
                "partition_key",
                "prediction_input_sha256",
                "predictions",
                "schedule",
                "schema",
            }
            or value["schema"] != META_GATE_MASK_SCHEMA
        ):
            raise AllCasesMLError("frozen meta anchor-gate document differs")
        string_keys = (
            "candidate_id",
            "model_id",
            "model_sha256",
            "null_world",
            "partition_key",
            "prediction_input_sha256",
        )
        if any(not isinstance(value[key], str) for key in string_keys):
            raise AllCasesMLError("frozen meta anchor-gate values differ")
        result = cls(
            value["partition_key"],
            value["model_id"],
            value["model_sha256"],
            value["candidate_id"],
            value["null_world"],
            value["prediction_input_sha256"],
            MetaAnchorGateSchedule.from_dict(value["schedule"]),
            PredictionBatch.from_dict(value["predictions"]),
        )
        if result.as_dict() != value:
            raise AllCasesMLError("frozen meta anchor-gate did not round trip")
        return result


def freeze_meta_anchor_gate(
    model: CanonicalMLModel,
    feature_rows: CausalFeatureRows,
    schedule: MetaAnchorGateSchedule,
) -> FrozenMetaAnchorGate:
    """Score only anchor-time features; no fill, price path, or exit is representable."""

    if (
        model.fold_key != "SEARCH_FINAL"
        or model.response_kind != "POSITIVE_NET_PROBABILITY"
        or model.candidate_id != schedule.candidate_id
        or model.symbolic_ranking_certificate != schedule.symbolic_ranking_certificate
        or model.base_strategy_id != schedule.base_strategy_id
        or model.base_trigger_family != schedule.base_trigger_family
        or model.null_world != schedule.symbolic_ranking_certificate.null_world
        or feature_rows.stage_key != schedule.partition_key
        or feature_rows.feature_set_id != model.feature_set_id
        or feature_rows.artifact_sha256 != schedule.feature_rows_sha256
    ):
        raise AllCasesMLError("meta anchor-gate model/schedule binding differs")
    index_by_id = {row_id: index for index, row_id in enumerate(feature_rows.row_ids)}
    if len(index_by_id) != feature_rows.row_count or any(
        row_id not in index_by_id for row_id in schedule.feature_row_ids
    ):
        raise AllCasesMLError("meta anchor-gate feature identities differ")
    indexes = np.asarray(
        tuple(index_by_id[row_id] for row_id in schedule.feature_row_ids), dtype=np.int64
    )
    if (
        tuple(int(feature_rows.decision_ns[index]) for index in indexes) != schedule.decision_ns
        or tuple(feature_rows.source_dates[index] for index in indexes) != schedule.decision_dates
    ):
        raise AllCasesMLError("meta anchor-gate feature chronology differs")
    values = feature_rows.values[indexes]
    base_directions = np.asarray(
        tuple(1 if value is TradeDirection.LONG else -1 for value in schedule.base_directions),
        dtype=np.int8,
    )
    predictions = predict_actions(
        model,
        values,
        row_ids=schedule.order_ids,
        base_directions=base_directions,
    )
    return FrozenMetaAnchorGate(
        schedule.partition_key,
        model.model_id,
        model.sha256,
        model.candidate_id,
        model.null_world,
        _prediction_input_sha256(
            values,
            schedule.order_ids,
            base_directions,
            auxiliary_label="ANCHOR_BASE_DIRECTIONS",
        ),
        schedule,
        predictions,
    )


def apply_meta_gate_to_symbolic_orders(
    gate: FrozenMetaAnchorGate,
    base_order_batch: EntryOrderBatch,
) -> tuple[FrozenEntryOrder, ...]:
    """Filter frozen intents by the gate; the symbolic engine still resolves all paths."""

    from .symbolic import EntryOrderBatch, FrozenEntryOrder

    if not isinstance(base_order_batch, EntryOrderBatch):
        raise AllCasesMLError("meta gate requires a typed symbolic order batch")
    orders = base_order_batch.orders
    if (
        base_order_batch.artifact_sha256 != gate.schedule.symbolic_order_batch_sha256
        or any(not isinstance(item, FrozenEntryOrder) for item in orders)
        or tuple(item.order_id for item in orders) != gate.schedule.order_ids
        or tuple(
            (
                item.anchor.contract,
                item.anchor.outcome_span_id,
                item.anchor.segment_id,
                item.anchor.anchor_ns,
                str(item.anchor.direction),
            )
            for item in orders
        )
        != gate.schedule.anchor_keys
    ):
        raise AllCasesMLError("meta gate/symbolic order binding differs")
    return tuple(
        order for order, admitted in zip(orders, gate.predictions.admitted, strict=True) if admitted
    )


@dataclass(frozen=True, slots=True)
class AlignedFrozenMetaAnchorGates:
    real: FrozenMetaAnchorGate
    circular_target: FrozenMetaAnchorGate
    matched_target: FrozenMetaAnchorGate
    proof: ControlAlignmentProof

    def __post_init__(self) -> None:
        gates = (self.real, self.circular_target, self.matched_target)
        if (
            tuple(item.null_world for item in gates) != NULL_WORLD_ORDER
            or len({item.candidate_id for item in gates}) != 1
            or len({item.partition_key for item in gates}) != 1
            or self.proof.candidate_id != self.real.candidate_id
            or self.proof.scope_key != self.real.partition_key
        ):
            raise AllCasesMLError("aligned frozen meta anchor gates differ")
        _validate_control_alignment_attachment(
            {item.null_world: item.predictions for item in gates},
            {item.null_world: item.schedule.decision_dates for item in gates},
            self.proof,
            scope_key=self.real.partition_key,
        )


def align_frozen_meta_anchor_gates(
    real: FrozenMetaAnchorGate,
    circular_target: FrozenMetaAnchorGate,
    matched_target: FrozenMetaAnchorGate,
) -> AlignedFrozenMetaAnchorGates:
    """Match date/base-direction gate counts before any world's symbolic execution."""

    gates = (real, circular_target, matched_target)
    if tuple(item.null_world for item in gates) != NULL_WORLD_ORDER:
        raise AllCasesMLError("meta gates are not in frozen null-world order")
    batches, proof = _aligned_prediction_batches(
        {item.null_world: item.predictions for item in gates},
        {item.null_world: item.schedule.decision_dates for item in gates},
        scope_key=real.partition_key,
    )
    aligned = tuple(replace(item, predictions=batches[item.null_world]) for item in gates)
    return AlignedFrozenMetaAnchorGates(*aligned, proof)


@dataclass(frozen=True, slots=True)
class AlignedFrozenPredictionMasks:
    real: FrozenPredictionMask
    circular_target: FrozenPredictionMask
    matched_target: FrozenPredictionMask
    proof: ControlAlignmentProof

    def __post_init__(self) -> None:
        values = (self.real, self.circular_target, self.matched_target)
        if (
            tuple(item.null_world for item in values) != NULL_WORLD_ORDER
            or len({item.candidate_id for item in values}) != 1
            or len({item.partition_key for item in values}) != 1
            or self.proof.candidate_id != self.real.candidate_id
            or self.proof.scope_key != self.real.partition_key
        ):
            raise AllCasesMLError("aligned frozen controls differ")
        _validate_control_alignment_attachment(
            {item.null_world: item.predictions for item in values},
            {item.null_world: item.execution_schedule.decision_dates for item in values},
            self.proof,
            scope_key=self.real.partition_key,
        )


def align_frozen_prediction_masks(
    real: FrozenPredictionMask,
    circular_target: FrozenPredictionMask,
    matched_target: FrozenPredictionMask,
) -> AlignedFrozenPredictionMasks:
    """Apply the same exact date/direction count match to one frozen OOS partition."""

    inputs = (real, circular_target, matched_target)
    if tuple(item.null_world for item in inputs) != NULL_WORLD_ORDER:
        raise AllCasesMLError("frozen controls are not in frozen null-world order")
    batches, proof = _aligned_prediction_batches(
        {item.null_world: item.predictions for item in inputs},
        {item.null_world: item.execution_schedule.decision_dates for item in inputs},
        scope_key=real.partition_key,
    )
    aligned_masks = tuple(replace(item, predictions=batches[item.null_world]) for item in inputs)
    return AlignedFrozenPredictionMasks(*aligned_masks, proof)


@dataclass(frozen=True, slots=True)
class FrozenResolvedOutcomeRows:
    """Post-freeze exact path outcomes; cardinality and lineage cannot change."""

    response_kind: Literal["DIRECT_TERMINAL_MOVE_TICKS"]
    row_ids: tuple[str, ...]
    execution_schedule_sha256: str
    outcome_lineage_sha256: str
    opportunity_lattice_sha256: str
    actual_exit_ns: tuple[int, ...]
    realized_values: tuple[int, ...]
    artifact_sha256: str

    def definition_dict(self) -> dict[str, object]:
        return {
            "actual_exit_ns": list(self.actual_exit_ns),
            "execution_schedule_sha256": self.execution_schedule_sha256,
            "opportunity_lattice_sha256": self.opportunity_lattice_sha256,
            "outcome_lineage_sha256": self.outcome_lineage_sha256,
            "realized_values": list(self.realized_values),
            "response_kind": self.response_kind,
            "row_ids": list(self.row_ids),
            "schema": "systematic_fx.ai_all_cases_ml_frozen_outcomes.v1",
        }

    def __post_init__(self) -> None:
        count = len(self.row_ids)
        if (
            self.response_kind != "DIRECT_TERMINAL_MOVE_TICKS"
            or not count
            or any(not isinstance(value, str) or not value for value in self.row_ids)
            or len(set(self.row_ids)) != count
            or len(self.actual_exit_ns) != count
            or len(self.realized_values) != count
            or any(not _is_exact_int64_scalar(value) for value in self.actual_exit_ns)
            or any(value <= 0 for value in self.actual_exit_ns)
            or any(not _is_exact_int64_scalar(value) for value in self.realized_values)
            or any(
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
                for value in (
                    self.execution_schedule_sha256,
                    self.outcome_lineage_sha256,
                    self.opportunity_lattice_sha256,
                    self.artifact_sha256,
                )
            )
            or canonical_sha256(self.definition_dict()) != self.artifact_sha256
        ):
            raise AllCasesMLError("frozen resolved outcomes differ")

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def build_frozen_resolved_outcome_rows(
    execution_schedule: OutcomeFreeExecutionSchedule,
    *,
    response_kind: Literal["DIRECT_TERMINAL_MOVE_TICKS"],
    row_ids: Sequence[str],
    actual_exit_ns: Sequence[int],
    realized_values: Sequence[int],
    valid_label_paths: Sequence[bool],
    outcome_contracts: Sequence[str],
    outcome_span_ids: Sequence[int],
    segment_ids: Sequence[int],
    outcome_lineage_sha256: str,
    opportunity_lattice_sha256: str,
) -> FrozenResolvedOutcomeRows:
    """Verify streamed 1s results against the pre-outcome lattice, without row dropping."""

    identifiers = tuple(row_ids)
    exits = _exact_int64_tuple(actual_exit_ns, label="actual_exit_ns")
    raw_values = tuple(realized_values)
    if any(
        isinstance(value, bool)
        or not isinstance(value, (int, np.integer))
        or int(value) < np.iinfo(np.int64).min
        or int(value) > np.iinfo(np.int64).max
        for value in raw_values
    ):
        raise AllCasesMLError("frozen realized values must be exact int64 ticks")
    values = tuple(int(value) for value in raw_values)
    valid = tuple(valid_label_paths)
    contracts = tuple(outcome_contracts)
    spans = _exact_int64_tuple(outcome_span_ids, label="outcome_span_ids")
    segments = _exact_int64_tuple(segment_ids, label="segment_ids")
    count = len(execution_schedule.row_ids)
    if (
        identifiers != execution_schedule.row_ids
        or len(exits) != count
        or len(values) != count
        or len(valid) != count
        or any(not isinstance(value, bool) for value in valid)
        or not all(valid)
        or contracts != execution_schedule.contracts
        or spans != execution_schedule.outcome_span_ids
        or segments != execution_schedule.segment_ids
        or exits != execution_schedule.planned_exit_ns
        or outcome_lineage_sha256 != execution_schedule.lineage_sha256
        or opportunity_lattice_sha256 != execution_schedule.opportunity_lattice_sha256
    ):
        raise AllCasesMLError(
            "post-freeze outcome path is missing, shortened, reordered, or cross-lineage"
        )
    definition = {
        "actual_exit_ns": list(exits),
        "execution_schedule_sha256": execution_schedule.sha256,
        "opportunity_lattice_sha256": opportunity_lattice_sha256,
        "outcome_lineage_sha256": outcome_lineage_sha256,
        "realized_values": list(values),
        "response_kind": response_kind,
        "row_ids": list(identifiers),
        "schema": "systematic_fx.ai_all_cases_ml_frozen_outcomes.v1",
    }
    return FrozenResolvedOutcomeRows(
        response_kind,
        identifiers,
        execution_schedule.sha256,
        outcome_lineage_sha256,
        opportunity_lattice_sha256,
        exits,
        values,
        canonical_sha256(definition),
    )


@dataclass(frozen=True, slots=True)
class OOSMLPartitionEvaluation:
    partition_key: str
    candidate_id: str
    candidate_kind: Literal["DIRECT"]
    family_key: str
    null_world: str
    alignment_proof_sha256: str
    outcome_artifact_sha256: str
    raw_signal_count: int
    active_signal_days: int
    fill_count: int
    active_entry_days: int
    total_net_ticks: int
    total_stress_net_ticks: int
    gross_profit_ticks: int
    gross_loss_ticks: int
    maximum_drawdown_ticks: int
    daily_net_ticks: tuple[tuple[str, int], ...]
    artifact_sha256: str

    def definition_dict(self) -> dict[str, object]:
        return {
            "active_entry_days": self.active_entry_days,
            "active_signal_days": self.active_signal_days,
            "alignment_proof_sha256": self.alignment_proof_sha256,
            "candidate_id": self.candidate_id,
            "candidate_kind": self.candidate_kind,
            "family_key": self.family_key,
            "fill_count": self.fill_count,
            "daily_net_ticks": [list(value) for value in self.daily_net_ticks],
            "gross_loss_ticks": self.gross_loss_ticks,
            "gross_profit_ticks": self.gross_profit_ticks,
            "maximum_drawdown_ticks": self.maximum_drawdown_ticks,
            "null_world": self.null_world,
            "outcome_artifact_sha256": self.outcome_artifact_sha256,
            "partition_key": self.partition_key,
            "raw_signal_count": self.raw_signal_count,
            "schema": "systematic_fx.ai_all_cases_ml_oos_evaluation.v1",
            "total_net_ticks": self.total_net_ticks,
            "total_stress_net_ticks": self.total_stress_net_ticks,
        }

    def __post_init__(self) -> None:
        if (
            self.partition_key not in {"WF1", "WF2", "WF3", "WF4", "WF5", "HOLDOUT"}
            or len(self.candidate_id) != 64
            or self.candidate_kind != "DIRECT"
            or len(self.family_key) != 64
            or self.null_world not in NULL_WORLD_ORDER
            or len(self.alignment_proof_sha256) != 64
            or len(self.outcome_artifact_sha256) != 64
            or self.raw_signal_count < self.fill_count
            or self.active_signal_days < self.active_entry_days
            or self.daily_net_ticks != tuple(sorted(self.daily_net_ticks))
            or len({value[0] for value in self.daily_net_ticks}) != len(self.daily_net_ticks)
            or any(
                isinstance(value, bool) or not isinstance(value, int)
                for value in (
                    self.total_net_ticks,
                    self.total_stress_net_ticks,
                    self.gross_profit_ticks,
                    self.gross_loss_ticks,
                    self.maximum_drawdown_ticks,
                )
            )
            or canonical_sha256(self.definition_dict()) != self.artifact_sha256
        ):
            raise AllCasesMLError("OOS ML partition evaluation differs")

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def evaluate_frozen_mask_economics(
    mask: FrozenPredictionMask,
    outcomes: FrozenResolvedOutcomeRows,
    *,
    alignment_proof_sha256: str,
) -> OOSMLPartitionEvaluation:
    """Apply a frozen aligned action mask to its exact post-freeze path outcomes."""

    if (
        outcomes.row_ids != mask.predictions.row_ids
        or outcomes.execution_schedule_sha256 != mask.execution_schedule.sha256
        or len(alignment_proof_sha256) != 64
    ):
        raise AllCasesMLError("frozen mask/outcome binding differs")
    candidate = DIRECT_CANDIDATE_BY_ID.get(mask.candidate_id)
    candidate_kind: Literal["DIRECT"] = "DIRECT"
    if candidate is None:
        raise AllCasesMLError("frozen direct OOS candidate is not in the catalog")
    if outcomes.response_kind != "DIRECT_TERMINAL_MOVE_TICKS":
        raise AllCasesMLError("frozen OOS response kind differs")
    net_values: list[int] = []
    active_dates: set[date] = set()
    daily_net = {value: 0 for value in mask.execution_schedule.partition_decision_dates}
    for index, admitted in enumerate(mask.predictions.admitted):
        if not admitted:
            continue
        direction = mask.predictions.directions[index]
        if direction is TradeDirection.FLAT:
            raise AllCasesMLError("admitted frozen OOS row is flat")
        signed = 1 if direction is TradeDirection.LONG else -1
        net_value = signed * outcomes.realized_values[index] - TOTAL_FRICTION_TICKS
        net_values.append(net_value)
        decision_date = mask.execution_schedule.decision_dates[index]
        active_dates.add(decision_date)
        daily_net[decision_date] += net_value
    signal_indexes = tuple(
        index for index, requested in enumerate(mask.predictions.requested) if requested
    )
    daily_items = tuple((key.isoformat(), value) for key, value in daily_net.items())
    family_key = ml_candidate_family_key(
        candidate,
        base_trigger_family=mask.base_trigger_family,
    )
    definition = {
        "active_entry_days": len(active_dates),
        "active_signal_days": len(
            {mask.execution_schedule.decision_dates[index] for index in signal_indexes}
        ),
        "alignment_proof_sha256": alignment_proof_sha256,
        "candidate_id": mask.candidate_id,
        "candidate_kind": candidate_kind,
        "family_key": family_key,
        "fill_count": len(net_values),
        "daily_net_ticks": [list(value) for value in daily_items],
        "gross_loss_ticks": -sum(value for value in net_values if value < 0),
        "gross_profit_ticks": sum(value for value in net_values if value > 0),
        "maximum_drawdown_ticks": _integer_equity_shape(net_values),
        "null_world": mask.null_world,
        "outcome_artifact_sha256": outcomes.artifact_sha256,
        "partition_key": mask.partition_key,
        "raw_signal_count": len(signal_indexes),
        "schema": "systematic_fx.ai_all_cases_ml_oos_evaluation.v1",
        "total_net_ticks": sum(net_values),
        "total_stress_net_ticks": sum(value - 4 for value in net_values),
    }
    return OOSMLPartitionEvaluation(
        mask.partition_key,
        mask.candidate_id,
        candidate_kind,
        family_key,
        mask.null_world,
        alignment_proof_sha256,
        outcomes.artifact_sha256,
        len(signal_indexes),
        len({mask.execution_schedule.decision_dates[index] for index in signal_indexes}),
        len(net_values),
        len(active_dates),
        sum(net_values),
        sum(value - 4 for value in net_values),
        sum(value for value in net_values if value > 0),
        -sum(value for value in net_values if value < 0),
        _integer_equity_shape(net_values),
        daily_items,
        canonical_sha256(definition),
    )


@dataclass(frozen=True, slots=True)
class EvaluatedFrozenControls:
    real: OOSMLPartitionEvaluation
    circular_target: OOSMLPartitionEvaluation
    matched_target: OOSMLPartitionEvaluation
    alignment_proof_sha256: str
    artifact_sha256: str

    def definition_dict(self) -> dict[str, object]:
        values = (self.real, self.circular_target, self.matched_target)
        return {
            "alignment_proof_sha256": self.alignment_proof_sha256,
            "candidate_id": self.real.candidate_id,
            "daily_net_ticks_by_world": {
                value.null_world: [list(item) for item in value.daily_net_ticks] for value in values
            },
            "evaluations": [value.as_dict() for value in values],
            "partition_key": self.real.partition_key,
            "schema": "systematic_fx.ai_all_cases_ml_oos_control_evaluation.v1",
        }

    def __post_init__(self) -> None:
        values = (self.real, self.circular_target, self.matched_target)
        if (
            tuple(item.null_world for item in values) != NULL_WORLD_ORDER
            or len({item.candidate_id for item in values}) != 1
            or len({item.partition_key for item in values}) != 1
            or any(item.alignment_proof_sha256 != self.alignment_proof_sha256 for item in values)
            or len({tuple(day for day, _ in item.daily_net_ticks) for item in values}) != 1
            or canonical_sha256(self.definition_dict()) != self.artifact_sha256
        ):
            raise AllCasesMLError("evaluated frozen controls differ")

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def evaluate_aligned_frozen_controls(
    controls: AlignedFrozenPredictionMasks,
    outcomes_by_world: Mapping[str, FrozenResolvedOutcomeRows],
) -> EvaluatedFrozenControls:
    """Evaluate one WF/holdout partition only after all three masks are frozen/aligned."""

    if set(outcomes_by_world) != set(NULL_WORLD_ORDER):
        raise AllCasesMLError("frozen control outcomes require all three worlds")
    masks = (controls.real, controls.circular_target, controls.matched_target)
    values = tuple(
        evaluate_frozen_mask_economics(
            mask,
            outcomes_by_world[mask.null_world],
            alignment_proof_sha256=controls.proof.artifact_sha256,
        )
        for mask in masks
    )
    definition = {
        "alignment_proof_sha256": controls.proof.artifact_sha256,
        "candidate_id": values[0].candidate_id,
        "daily_net_ticks_by_world": {
            value.null_world: [list(item) for item in value.daily_net_ticks] for value in values
        },
        "evaluations": [value.as_dict() for value in values],
        "partition_key": values[0].partition_key,
        "schema": "systematic_fx.ai_all_cases_ml_oos_control_evaluation.v1",
    }
    return EvaluatedFrozenControls(
        *values,
        controls.proof.artifact_sha256,
        canonical_sha256(definition),
    )
