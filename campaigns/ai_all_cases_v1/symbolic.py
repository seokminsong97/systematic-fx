"""Finite causal symbolic research language for all-cases v1.

This module is deliberately pure.  It has no filesystem, database, network,
clock, or outcome-loader access.  Completed 5-minute, 30-minute, and one-hour
trade bars are transformed into point-in-time event states and sparse entry
anchor masks.  Search outcomes are supplied only to the separate Stage-A
scoring functions after masks have been constructed and deduplicated.

The language is exhaustive only inside its frozen grammar: one of 1,740 base
events, one of 13 multi-timeframe contexts, one of 14 UTC filters, and one of
six delay/persistence policies.  The Cartesian policy count is 1,900,080, but
the implementation streams it in bounded batches and shares every indicator,
event, context, time, and delay computation.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict, deque
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from fractions import Fraction
from functools import lru_cache
from itertools import pairwise, product
from typing import Final, Literal

import numpy as np

from scripts.ai_pattern_holdout_engine import BarWithOutcomeSpan
from systematic_fx.features.bars import ONE_SECOND_NS
from systematic_fx.research.hypotheses import canonical_sha256

ENGINE_SCHEMA: Final = "systematic_fx.ai_all_cases_symbolic_engine.v1"
BASE_CATALOG_SCHEMA: Final = "systematic_fx.ai_all_cases_base_event_catalog.v1"
AXIS_CATALOG_SCHEMA: Final = "systematic_fx.ai_all_cases_symbolic_axis_catalog.v1"
POLICY_SCHEMA: Final = "systematic_fx.ai_all_cases_anchor_policy.v1"
MASK_SCHEMA: Final = "systematic_fx.ai_all_cases_sparse_mask.v1"
STRUCTURAL_ELIGIBILITY_SCHEMA: Final = "systematic_fx.ai_all_cases_structural_eligibility.v1"
DIRECT_OPPORTUNITY_SCHEMA: Final = "systematic_fx.ai_all_cases_direct_opportunity.v1"
STAGE_A_SCORE_SCHEMA: Final = "systematic_fx.ai_all_cases_stage_a_score.v1"
ENTRY_CATALOG_SCHEMA: Final = "systematic_fx.ai_all_cases_entry_catalog.v1"
EXIT_CATALOG_SCHEMA: Final = "systematic_fx.ai_all_cases_exit_catalog.v1"
COMPLETE_STRATEGY_SCHEMA: Final = "systematic_fx.ai_all_cases_complete_strategy.v1"
PATH_OUTCOME_SCHEMA: Final = "systematic_fx.ai_all_cases_path_outcome.v1"
COMPACT_STAGE_B_CHUNK_SCHEMA: Final = "systematic_fx.ai_all_cases_compact_stage_b_chunk.v1"
SEARCH_GATE_SCHEMA: Final = "systematic_fx.ai_all_cases_search_gate.v1"
EXPERT_FEATURE_SCHEMA: Final = "systematic_fx.ai_all_cases_causal_expert_features.v1"

FIVE_MINUTES: Final = 300
HALF_HOUR: Final = 1_800
ONE_HOUR: Final = 3_600
SUPPORTED_SIGNAL_TIMEFRAMES: Final = (FIVE_MINUTES, HALF_HOUR, ONE_HOUR)
EMA_SCALE: Final = 1_000_000
INDICATOR_SCALE: Final = 1_000_000
MAX_DAILY_BRIDGE_SECONDS: Final = 96 * ONE_HOUR
ATR_PERIOD: Final = 20

BASE_EVENT_COUNT: Final = 1_740
CONTEXT_COUNT: Final = 13
TIME_FILTER_COUNT: Final = 14
DELAY_COUNT: Final = 6
LOGICAL_ANCHOR_POLICY_COUNT: Final = (
    BASE_EVENT_COUNT * CONTEXT_COUNT * TIME_FILTER_COUNT * DELAY_COUNT
)
REFERENCE_HORIZONS_SECONDS: Final = (1_800, 3_600, 7_200, 10_800, 21_600)
REFERENCE_SCORE_CELL_COUNT: Final = LOGICAL_ANCHOR_POLICY_COUNT * len(REFERENCE_HORIZONS_SECONDS)
STAGE_A_MAXIMUM_SELECTION: Final = 256
STAGE_B_PAIR_BUDGET_MAXIMUM: Final = 100_000
STAGE_B_CONTROL_WORLD_COUNT: Final = 3
TOTAL_FRICTION_TICKS: Final = 14
ENTRY_ADVERSE_TICKS: Final = 2
EXIT_ADVERSE_TICKS: Final = 2
VARIABLE_COST_TICKS: Final = 5
ALLOCATED_FIXED_COST_TICKS: Final = 5
MASTER_NULL_SEED: Final = "ai-all-cases-v1"
CONTROL_VOLATILITY_WINDOW: Final = 12
CONTROL_VOLATILITY_HISTORY: Final = 20
CONTROL_REGIME_WINDOW: Final = 12
CONTROL_TIME_BUCKET_HOURS: Final = 4
SEARCH_OOF_BLOCK_KEYS: Final = ("B3", "B4", "B5", "B6", "B7", "B8")
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

ENTRY_POLICY_COUNT: Final = 9
EXIT_POLICY_COUNT: Final = 85
COMPLETE_STRATEGIES_PER_ANCHOR: Final = ENTRY_POLICY_COUNT * EXIT_POLICY_COUNT
COMPLETE_STRATEGY_MAXIMUM: Final = STAGE_A_MAXIMUM_SELECTION * COMPLETE_STRATEGIES_PER_ANCHOR

EMA_PAIRS: Final = ((5, 13), (8, 21), (12, 26), (20, 50))
MACD_PARAMETERS: Final = ((5, 13, 4), (8, 21, 5), (12, 26, 9), (19, 39, 9))
MTF_PAIRS: Final = (
    (FIVE_MINUTES, FIVE_MINUTES),
    (HALF_HOUR, FIVE_MINUTES),
    (HALF_HOUR, HALF_HOUR),
    (ONE_HOUR, FIVE_MINUTES),
    (ONE_HOUR, HALF_HOUR),
    (ONE_HOUR, ONE_HOUR),
)

Direction = Literal["LONG", "SHORT"]
EntryKind = Literal["MARKET", "STOP_SIGNAL_EXTREME", "LIMIT_ATR_RETRACE"]
ExitKind = Literal["TERMINAL", "BRACKET", "TRAILING", "BREAK_EVEN", "RULE"]
RuleExitKind = Literal["OPPOSITE_TRIGGER", "CONTEXT_INVALID"]
Family = Literal[
    "MOMENTUM_RETURN",
    "DONCHIAN_BREAKOUT",
    "EMA_TREND",
    "MACD_STATE",
    "RSI_STATE",
    "STOCHASTIC_STATE",
    "BOLLINGER_STATE",
    "RANGE_REVERSION",
    "COMPRESSION_BREAKOUT",
    "PULLBACK_CONTINUATION",
    "BODY_CONTINUATION",
    "WICK_REJECTION",
    "STRUCTURAL_PRICE_ACTION",
    "NBAR_PRICE_ACTION",
    "VOLATILITY_EXPANSION",
    "EFFICIENCY_RATIO",
    "VOLUME_FLOW",
    "ROLLING_VWAP",
    "SWING_FAILURE",
    "GAP_EVENT",
]


def expert_feature_formula_contract() -> dict[str, object]:
    """Return the exact outcome-blind rational Expert-8 formula contract."""

    return {
        "allowed_inputs": [
            "BaseEventCandidate",
            "ContextSpec",
            "AnchorPolicy",
            "AnchorRecord",
            "FrozenEntryOrder",
            "ExitPolicy",
        ],
        "decision_cutoff": "ANCHOR_NS_FEATURE_STATE_ONLY",
        "denominator_policy": "NORMALIZED_POSITIVE_EXACT_RATIONAL",
        "disallowed_inputs": [
            "EntryAttempt",
            "ExitOutcome",
            "OneSecondPath",
            "TradeBarOutcomeRows",
            "REALIZED_FILL_OR_EXIT",
        ],
        "features": [
            {
                "formula": (
                    "direction_sign*(trigger_close_ticks-trigger_open_ticks)*"
                    "atr_denominator/atr_sum_ticks;atr_sum_ticks=0=>0"
                ),
                "name": "expert_signal_strength",
            },
            {
                "formula": (
                    "(anchor_ns-trigger_end_ns)/(trigger_timeframe_seconds*1e9);require_nonnegative"
                ),
                "name": "expert_event_age_native_bars",
            },
            {
                "formula": (
                    "ANY=0;EMA_RELATION=relation;EFFICIENCY_TREND=1;"
                    "EFFICIENCY_RANGE=-1;VOLATILITY_EXPANDING=1;"
                    "VOLATILITY_CONTRACTING=-1"
                ),
                "name": "expert_context_relation",
            },
            {
                "formula": "atr_sum_ticks/atr_denominator",
                "name": "expert_atr_ticks",
            },
            {
                "formula": (
                    "(trigger_high_ticks-trigger_low_ticks)*atr_denominator/"
                    "atr_sum_ticks;atr_sum_ticks=0=>0"
                ),
                "name": "expert_signal_range_atr",
            },
            {
                "formula": "(expires_ns-valid_from_ns)/1e9;opportunity_window_not_fill_latency",
                "name": "expert_time_to_entry_seconds",
            },
            {
                "formula": (
                    "MARKET=0;otherwise direction_sign*(order_ticks-trigger_close_ticks)*"
                    "atr_denominator/atr_sum_ticks;atr_sum_ticks=0=>0"
                ),
                "name": "expert_planned_entry_distance_atr",
            },
            {
                "formula": (
                    "BRACKET=take_profit_atr/stop_loss_atr;TRAILING=activation_atr/"
                    "trail_atr;BREAK_EVEN=activation_atr/initial_stop_atr;"
                    "TERMINAL_OR_RULE=0"
                ),
                "name": "expert_reward_risk_ratio",
            },
        ],
        "feature_names": list(EXPERT_FEATURE_NAMES),
        "schema": EXPERT_FEATURE_SCHEMA,
        "value_representation": "NORMALIZED_INTEGER_NUMERATOR_DENOMINATOR",
    }


EXPERT_FEATURE_FORMULA_SHA256: Final = canonical_sha256(expert_feature_formula_contract())


class SymbolicEngineError(ValueError):
    """A symbolic definition, bar view, state, mask, or score is invalid."""


def _require_int(value: object, *, label: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SymbolicEngineError(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        raise SymbolicEngineError(f"{label} must be >= {minimum}")
    return value


def _require_sha(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise SymbolicEngineError(f"{label} must be a lowercase SHA-256")
    return value


def _round_half_even(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise SymbolicEngineError("rounding denominator must be positive")
    sign = -1 if numerator < 0 else 1
    quotient, remainder = divmod(abs(numerator), denominator)
    twice = remainder * 2
    if twice > denominator or (twice == denominator and quotient % 2):
        quotient += 1
    return sign * quotient


def _fraction_parameters(parameters: Mapping[str, int | Fraction]) -> tuple[tuple[str, int], ...]:
    flattened: dict[str, int] = {}
    for key, raw in parameters.items():
        if isinstance(raw, bool):
            raise SymbolicEngineError("candidate parameters cannot be boolean")
        value = raw if isinstance(raw, Fraction) else Fraction(raw)
        if value.denominator == 1:
            flattened[key] = value.numerator
        else:
            flattened[f"{key}_denominator"] = value.denominator
            flattened[f"{key}_numerator"] = value.numerator
    return tuple(sorted(flattened.items()))


@dataclass(frozen=True, slots=True)
class BaseEventCandidate:
    selection_rank: int
    candidate_id: str
    family: Family
    direction: Direction
    parameters: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        _require_int(self.selection_rank, label="selection_rank", minimum=1)
        _require_sha(self.candidate_id, label="candidate_id")
        if self.direction not in ("LONG", "SHORT"):
            raise SymbolicEngineError("candidate direction is invalid")
        if not self.parameters or self.parameters != tuple(sorted(self.parameters)):
            raise SymbolicEngineError("candidate parameters must be non-empty and sorted")
        if len({key for key, _ in self.parameters}) != len(self.parameters):
            raise SymbolicEngineError("candidate parameter names must be unique")
        if canonical_sha256(self.definition_dict()) != self.candidate_id:
            raise SymbolicEngineError("candidate id differs from its definition")

    def parameter(self, name: str) -> int:
        try:
            return dict(self.parameters)[name]
        except KeyError as error:
            raise SymbolicEngineError(f"candidate lacks parameter {name!r}") from error

    def fraction_parameter(self, name: str) -> Fraction:
        values = dict(self.parameters)
        numerator_key = f"{name}_numerator"
        denominator_key = f"{name}_denominator"
        if numerator_key in values or denominator_key in values:
            try:
                return Fraction(values[numerator_key], values[denominator_key])
            except KeyError as error:
                raise SymbolicEngineError(f"candidate has incomplete fraction {name!r}") from error
        return Fraction(self.parameter(name))

    @property
    def trigger_timeframe_seconds(self) -> int:
        return self.parameter("trigger_timeframe_seconds")

    def definition_dict(self) -> dict[str, object]:
        return {
            "direction": self.direction,
            "family": self.family,
            "parameters": dict(self.parameters),
            "schema": BASE_CATALOG_SCHEMA,
        }

    def as_dict(self) -> dict[str, object]:
        return {
            **self.definition_dict(),
            "candidate_id": self.candidate_id,
            "selection_rank": self.selection_rank,
        }


@dataclass(frozen=True, slots=True)
class BaseEventCatalog:
    candidates: tuple[BaseEventCandidate, ...]
    catalog_sha256: str

    def __post_init__(self) -> None:
        if len(self.candidates) != BASE_EVENT_COUNT:
            raise SymbolicEngineError("base-event catalog count differs")
        if tuple(item.selection_rank for item in self.candidates) != tuple(
            range(1, BASE_EVENT_COUNT + 1)
        ):
            raise SymbolicEngineError("base-event catalog ranks differ")
        if len({item.candidate_id for item in self.candidates}) != BASE_EVENT_COUNT:
            raise SymbolicEngineError("base-event catalog contains duplicate definitions")
        _require_sha(self.catalog_sha256, label="catalog_sha256")
        if canonical_sha256([item.as_dict() for item in self.candidates]) != self.catalog_sha256:
            raise SymbolicEngineError("base-event catalog hash differs")

    @property
    def candidate_ids(self) -> tuple[str, ...]:
        return tuple(item.candidate_id for item in self.candidates)

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_count": len(self.candidates),
            "candidate_ids": list(self.candidate_ids),
            "candidates": [item.as_dict() for item in self.candidates],
            "catalog_sha256": self.catalog_sha256,
            "schema": BASE_CATALOG_SCHEMA,
        }


def _base_candidate(
    rank: int,
    family: Family,
    direction: Direction,
    **parameters: int | Fraction,
) -> BaseEventCandidate:
    pairs = _fraction_parameters(parameters)
    definition = {
        "direction": direction,
        "family": family,
        "parameters": dict(pairs),
        "schema": BASE_CATALOG_SCHEMA,
    }
    return BaseEventCandidate(rank, canonical_sha256(definition), family, direction, pairs)


@lru_cache(maxsize=1)
def build_base_event_catalog() -> BaseEventCatalog:
    """Return the exact semantic-order 1,740-member event catalog."""

    rows: list[BaseEventCandidate] = []

    def add(family: Family, direction: Direction, **parameters: int | Fraction) -> None:
        rows.append(_base_candidate(len(rows) + 1, family, direction, **parameters))

    for timeframe, lookback, threshold, direction in product(
        SUPPORTED_SIGNAL_TIMEFRAMES,
        (1, 2, 3, 6, 12, 24),
        (Fraction(1, 4), Fraction(1, 2), Fraction(1), Fraction(3, 2)),
        ("LONG", "SHORT"),
    ):
        add(
            "MOMENTUM_RETURN",
            direction,
            trigger_timeframe_seconds=timeframe,
            lookback_bars=lookback,
            threshold_atr=threshold,
        )

    for timeframe, lookback, confirmation, buffer_atr, direction in product(
        SUPPORTED_SIGNAL_TIMEFRAMES,
        (3, 6, 12, 24, 48),
        (1, 2),
        (Fraction(0), Fraction(1, 4)),
        ("LONG", "SHORT"),
    ):
        add(
            "DONCHIAN_BREAKOUT",
            direction,
            trigger_timeframe_seconds=timeframe,
            lookback_bars=lookback,
            confirmation_bars=confirmation,
            buffer_atr=buffer_atr,
        )

    for timeframe, (fast, slow), mode, direction in product(
        SUPPORTED_SIGNAL_TIMEFRAMES,
        EMA_PAIRS,
        (1, 2),  # 1 fresh cross; 2 aligned state plus fast slope
        ("LONG", "SHORT"),
    ):
        add(
            "EMA_TREND",
            direction,
            trigger_timeframe_seconds=timeframe,
            fast_period=fast,
            slow_period=slow,
            mode=mode,
        )

    for timeframe, (fast, slow, signal), mode, direction in product(
        SUPPORTED_SIGNAL_TIMEFRAMES,
        MACD_PARAMETERS,
        (1, 2, 3),  # histogram cross; MACD-zero cross; positive/rising histogram
        ("LONG", "SHORT"),
    ):
        add(
            "MACD_STATE",
            direction,
            trigger_timeframe_seconds=timeframe,
            fast_period=fast,
            slow_period=slow,
            signal_period=signal,
            mode=mode,
        )

    for timeframe, period, lower, direction in product(
        SUPPORTED_SIGNAL_TIMEFRAMES,
        (7, 14, 21),
        (20, 30, 40),
        ("LONG", "SHORT"),
    ):
        add(
            "RSI_STATE",
            direction,
            trigger_timeframe_seconds=timeframe,
            period=period,
            lower_band=lower,
            upper_band=100 - lower,
            mode=1,
        )
    for timeframe, period, direction in product(
        SUPPORTED_SIGNAL_TIMEFRAMES, (7, 14, 21), ("LONG", "SHORT")
    ):
        add(
            "RSI_STATE",
            direction,
            trigger_timeframe_seconds=timeframe,
            period=period,
            lower_band=50,
            upper_band=50,
            mode=2,
        )

    for timeframe, period, lower, mode, direction in product(
        SUPPORTED_SIGNAL_TIMEFRAMES,
        (5, 9, 14),
        (20, 30),
        (1, 2),
        ("LONG", "SHORT"),
    ):
        add(
            "STOCHASTIC_STATE",
            direction,
            trigger_timeframe_seconds=timeframe,
            k_period=period,
            d_period=3,
            lower_band=lower,
            upper_band=100 - lower,
            mode=mode,
        )

    for timeframe, lookback, band, mode, direction in product(
        SUPPORTED_SIGNAL_TIMEFRAMES,
        (10, 20, 40),
        (Fraction(3, 2), Fraction(2), Fraction(5, 2)),
        (1, 2),  # breakout; re-entry fade
        ("LONG", "SHORT"),
    ):
        add(
            "BOLLINGER_STATE",
            direction,
            trigger_timeframe_seconds=timeframe,
            lookback_bars=lookback,
            band_sigma=band,
            mode=mode,
        )

    for timeframe, lookback, overshoot, direction in product(
        SUPPORTED_SIGNAL_TIMEFRAMES,
        (6, 12, 24, 48),
        (Fraction(0), Fraction(1, 4)),
        ("LONG", "SHORT"),
    ):
        add(
            "RANGE_REVERSION",
            direction,
            trigger_timeframe_seconds=timeframe,
            lookback_bars=lookback,
            overshoot_atr=overshoot,
        )

    for (context, trigger), compression, breakout, ratio, direction in product(
        MTF_PAIRS,
        (6, 12, 24),
        (3, 6, 12),
        (Fraction(1, 2), Fraction(3, 4)),
        ("LONG", "SHORT"),
    ):
        add(
            "COMPRESSION_BREAKOUT",
            direction,
            context_timeframe_seconds=context,
            trigger_timeframe_seconds=trigger,
            compression_window=compression,
            breakout_window=breakout,
            compression_ratio=ratio,
        )

    for (context, trigger), (fast, slow), pullback, direction in product(
        MTF_PAIRS,
        EMA_PAIRS,
        (1, 2, 3),
        ("LONG", "SHORT"),
    ):
        add(
            "PULLBACK_CONTINUATION",
            direction,
            context_timeframe_seconds=context,
            trigger_timeframe_seconds=trigger,
            fast_period=fast,
            slow_period=slow,
            pullback_bars=pullback,
        )

    for family in ("BODY_CONTINUATION", "WICK_REJECTION"):
        for timeframe, magnitude, close_location, direction in product(
            SUPPORTED_SIGNAL_TIMEFRAMES,
            (Fraction(1, 4), Fraction(1, 2), Fraction(3, 4)),
            (Fraction(3, 5), Fraction(3, 4), Fraction(9, 10)),
            ("LONG", "SHORT"),
        ):
            add(
                family,
                direction,
                trigger_timeframe_seconds=timeframe,
                magnitude_ratio=magnitude,
                close_location=close_location,
            )

    for timeframe, pattern, confirmation, direction in product(
        SUPPORTED_SIGNAL_TIMEFRAMES,
        (1, 2, 3, 4),  # inside, outside, engulfing, failed break
        (1, 2),
        ("LONG", "SHORT"),
    ):
        add(
            "STRUCTURAL_PRICE_ACTION",
            direction,
            trigger_timeframe_seconds=timeframe,
            pattern=pattern,
            confirmation_bars=confirmation,
        )

    for timeframe, length, pattern, direction in product(
        SUPPORTED_SIGNAL_TIMEFRAMES,
        (2, 3, 4),
        (1, 2, 3),  # directional bodies, staircase, reversal
        ("LONG", "SHORT"),
    ):
        add(
            "NBAR_PRICE_ACTION",
            direction,
            trigger_timeframe_seconds=timeframe,
            length_bars=length,
            pattern=pattern,
        )

    for timeframe, (short, long), ratio, direction in product(
        SUPPORTED_SIGNAL_TIMEFRAMES,
        ((3, 12), (6, 24), (12, 48)),
        (Fraction(5, 4), Fraction(3, 2), Fraction(2)),
        ("LONG", "SHORT"),
    ):
        add(
            "VOLATILITY_EXPANSION",
            direction,
            trigger_timeframe_seconds=timeframe,
            short_window=short,
            long_window=long,
            expansion_ratio=ratio,
        )

    for timeframe, lookback, threshold, mode, direction in product(
        SUPPORTED_SIGNAL_TIMEFRAMES,
        (10, 20, 40),
        (Fraction(1, 3), Fraction(1, 2), Fraction(2, 3)),
        (1, 2),  # efficient continuation; inefficient rolling-mean fade
        ("LONG", "SHORT"),
    ):
        add(
            "EFFICIENCY_RATIO",
            direction,
            trigger_timeframe_seconds=timeframe,
            lookback_bars=lookback,
            threshold=threshold,
            mode=mode,
        )

    for timeframe, lookback, z_score, mode, direction in product(
        SUPPORTED_SIGNAL_TIMEFRAMES,
        (12, 24, 48),
        (1, 2),
        (1, 2, 3),  # price continuation, imbalance continuation, absorption reversal
        ("LONG", "SHORT"),
    ):
        add(
            "VOLUME_FLOW",
            direction,
            trigger_timeframe_seconds=timeframe,
            lookback_bars=lookback,
            z_score=z_score,
            mode=mode,
        )

    for timeframe, lookback, band, mode, direction in product(
        SUPPORTED_SIGNAL_TIMEFRAMES,
        (12, 24, 48),
        (Fraction(0), Fraction(1, 2), Fraction(1)),
        (1, 2),  # continuation cross; outside-to-inside fade
        ("LONG", "SHORT"),
    ):
        add(
            "ROLLING_VWAP",
            direction,
            trigger_timeframe_seconds=timeframe,
            lookback_bars=lookback,
            band_atr=band,
            mode=mode,
        )

    for timeframe, lookback, failure, direction in product(
        SUPPORTED_SIGNAL_TIMEFRAMES,
        (6, 12, 24, 48),
        (1, 2, 3),
        ("LONG", "SHORT"),
    ):
        add(
            "SWING_FAILURE",
            direction,
            trigger_timeframe_seconds=timeframe,
            lookback_bars=lookback,
            failure_window=failure,
        )

    for timeframe, threshold, mode, direction in product(
        SUPPORTED_SIGNAL_TIMEFRAMES,
        (Fraction(1, 4), Fraction(1, 2), Fraction(1)),
        (1, 2),  # continuation; fade
        ("LONG", "SHORT"),
    ):
        add(
            "GAP_EVENT",
            direction,
            trigger_timeframe_seconds=timeframe,
            threshold_atr=threshold,
            mode=mode,
        )

    if len(rows) != BASE_EVENT_COUNT:
        raise SymbolicEngineError(f"base-event construction produced {len(rows)} rows")
    candidates = tuple(rows)
    return BaseEventCatalog(candidates, canonical_sha256([item.as_dict() for item in candidates]))


@dataclass(frozen=True, slots=True)
class EntryPolicy:
    """One causal order-placement policy in the frozen nine-member lattice."""

    selection_rank: int
    entry_id: str
    kind: EntryKind
    parameters: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        _require_int(self.selection_rank, label="entry selection_rank", minimum=1)
        _require_sha(self.entry_id, label="entry_id")
        if self.kind not in {"MARKET", "STOP_SIGNAL_EXTREME", "LIMIT_ATR_RETRACE"}:
            raise SymbolicEngineError("entry kind is invalid")
        if self.parameters != tuple(sorted(self.parameters)):
            raise SymbolicEngineError("entry parameters must be sorted")
        if len({key for key, _ in self.parameters}) != len(self.parameters):
            raise SymbolicEngineError("entry parameter names must be unique")
        names = {key for key, _ in self.parameters}
        expected = {
            "MARKET": set(),
            "STOP_SIGNAL_EXTREME": {"buffer_ticks", "time_in_force_seconds"},
            "LIMIT_ATR_RETRACE": {
                "retrace_atr_denominator",
                "retrace_atr_numerator",
                "time_in_force_seconds",
            },
        }[self.kind]
        if names != expected:
            raise SymbolicEngineError("entry parameters differ from the frozen kind")
        if canonical_sha256(self.definition_dict()) != self.entry_id:
            raise SymbolicEngineError("entry id differs from its definition")

    def parameter(self, name: str) -> int:
        try:
            return dict(self.parameters)[name]
        except KeyError as error:
            raise SymbolicEngineError(f"entry policy lacks parameter {name!r}") from error

    def fraction_parameter(self, name: str) -> Fraction:
        values = dict(self.parameters)
        try:
            return Fraction(values[f"{name}_numerator"], values[f"{name}_denominator"])
        except KeyError as error:
            raise SymbolicEngineError(f"entry policy lacks fraction {name!r}") from error

    def definition_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "parameters": dict(self.parameters),
            "schema": ENTRY_CATALOG_SCHEMA,
        }

    def as_dict(self) -> dict[str, object]:
        return {
            **self.definition_dict(),
            "entry_id": self.entry_id,
            "selection_rank": self.selection_rank,
        }


@dataclass(frozen=True, slots=True)
class ExitPolicy:
    """One causal liquidation policy in the frozen 85-member lattice."""

    selection_rank: int
    exit_id: str
    kind: ExitKind
    parameters: tuple[tuple[str, int], ...]
    rule_kind: RuleExitKind | None = None

    def __post_init__(self) -> None:
        _require_int(self.selection_rank, label="exit selection_rank", minimum=1)
        _require_sha(self.exit_id, label="exit_id")
        if self.kind not in {"TERMINAL", "BRACKET", "TRAILING", "BREAK_EVEN", "RULE"}:
            raise SymbolicEngineError("exit kind is invalid")
        if self.parameters != tuple(sorted(self.parameters)):
            raise SymbolicEngineError("exit parameters must be sorted")
        if len({key for key, _ in self.parameters}) != len(self.parameters):
            raise SymbolicEngineError("exit parameter names must be unique")
        names = {key for key, _ in self.parameters}
        expected = {
            "TERMINAL": {"horizon_seconds"},
            "BRACKET": {
                "cap_seconds",
                "stop_loss_atr_denominator",
                "stop_loss_atr_numerator",
                "take_profit_atr_denominator",
                "take_profit_atr_numerator",
            },
            "TRAILING": {
                "activation_atr_denominator",
                "activation_atr_numerator",
                "cap_seconds",
                "trail_atr_denominator",
                "trail_atr_numerator",
            },
            "BREAK_EVEN": {
                "activation_atr_denominator",
                "activation_atr_numerator",
                "cap_seconds",
                "initial_stop_atr_denominator",
                "initial_stop_atr_numerator",
            },
            "RULE": {"cap_seconds"},
        }[self.kind]
        if names != expected:
            raise SymbolicEngineError("exit parameters differ from the frozen kind")
        if self.kind == "RULE":
            if self.rule_kind not in {"OPPOSITE_TRIGGER", "CONTEXT_INVALID"}:
                raise SymbolicEngineError("rule exit lacks a valid rule kind")
        elif self.rule_kind is not None:
            raise SymbolicEngineError("non-rule exit cannot carry a rule kind")
        if canonical_sha256(self.definition_dict()) != self.exit_id:
            raise SymbolicEngineError("exit id differs from its definition")

    def parameter(self, name: str) -> int:
        try:
            return dict(self.parameters)[name]
        except KeyError as error:
            raise SymbolicEngineError(f"exit policy lacks parameter {name!r}") from error

    def fraction_parameter(self, name: str) -> Fraction:
        values = dict(self.parameters)
        try:
            return Fraction(values[f"{name}_numerator"], values[f"{name}_denominator"])
        except KeyError as error:
            raise SymbolicEngineError(f"exit policy lacks fraction {name!r}") from error

    @property
    def cap_seconds(self) -> int:
        return self.parameter("horizon_seconds" if self.kind == "TERMINAL" else "cap_seconds")

    def definition_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "parameters": dict(self.parameters),
            "rule_kind": self.rule_kind,
            "schema": EXIT_CATALOG_SCHEMA,
        }

    def as_dict(self) -> dict[str, object]:
        return {
            **self.definition_dict(),
            "exit_id": self.exit_id,
            "selection_rank": self.selection_rank,
        }


@dataclass(frozen=True, slots=True)
class EntryCatalog:
    candidates: tuple[EntryPolicy, ...]
    catalog_sha256: str

    def __post_init__(self) -> None:
        if len(self.candidates) != ENTRY_POLICY_COUNT:
            raise SymbolicEngineError("entry catalog count differs")
        if tuple(item.selection_rank for item in self.candidates) != tuple(
            range(1, ENTRY_POLICY_COUNT + 1)
        ):
            raise SymbolicEngineError("entry catalog ranks differ")
        if len({item.entry_id for item in self.candidates}) != ENTRY_POLICY_COUNT:
            raise SymbolicEngineError("entry catalog identities differ")
        _require_sha(self.catalog_sha256, label="entry catalog_sha256")
        if canonical_sha256([item.as_dict() for item in self.candidates]) != self.catalog_sha256:
            raise SymbolicEngineError("entry catalog hash differs")

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_count": len(self.candidates),
            "candidates": [item.as_dict() for item in self.candidates],
            "catalog_sha256": self.catalog_sha256,
            "schema": ENTRY_CATALOG_SCHEMA,
        }


@dataclass(frozen=True, slots=True)
class ExitCatalog:
    candidates: tuple[ExitPolicy, ...]
    catalog_sha256: str

    def __post_init__(self) -> None:
        if len(self.candidates) != EXIT_POLICY_COUNT:
            raise SymbolicEngineError("exit catalog count differs")
        if tuple(item.selection_rank for item in self.candidates) != tuple(
            range(1, EXIT_POLICY_COUNT + 1)
        ):
            raise SymbolicEngineError("exit catalog ranks differ")
        if len({item.exit_id for item in self.candidates}) != EXIT_POLICY_COUNT:
            raise SymbolicEngineError("exit catalog identities differ")
        _require_sha(self.catalog_sha256, label="exit catalog_sha256")
        if canonical_sha256([item.as_dict() for item in self.candidates]) != self.catalog_sha256:
            raise SymbolicEngineError("exit catalog hash differs")

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_count": len(self.candidates),
            "candidates": [item.as_dict() for item in self.candidates],
            "catalog_sha256": self.catalog_sha256,
            "schema": EXIT_CATALOG_SCHEMA,
        }


def _entry_policy(rank: int, kind: EntryKind, **parameters: int | Fraction) -> EntryPolicy:
    pairs = _execution_parameters(parameters)
    definition = {
        "kind": kind,
        "parameters": dict(pairs),
        "schema": ENTRY_CATALOG_SCHEMA,
    }
    return EntryPolicy(rank, canonical_sha256(definition), kind, pairs)


def _exit_policy(
    rank: int,
    kind: ExitKind,
    *,
    rule_kind: RuleExitKind | None = None,
    **parameters: int | Fraction,
) -> ExitPolicy:
    pairs = _execution_parameters(parameters)
    definition = {
        "kind": kind,
        "parameters": dict(pairs),
        "rule_kind": rule_kind,
        "schema": EXIT_CATALOG_SCHEMA,
    }
    return ExitPolicy(rank, canonical_sha256(definition), kind, pairs, rule_kind)


def _execution_parameters(
    parameters: Mapping[str, int | Fraction],
) -> tuple[tuple[str, int], ...]:
    flattened: dict[str, int] = {}
    for key, raw in parameters.items():
        if isinstance(raw, bool):
            raise SymbolicEngineError("execution parameters cannot be boolean")
        if isinstance(raw, Fraction):
            flattened[f"{key}_denominator"] = raw.denominator
            flattened[f"{key}_numerator"] = raw.numerator
        else:
            flattened[key] = raw
    return tuple(sorted(flattened.items()))


@lru_cache(maxsize=1)
def build_entry_catalog() -> EntryCatalog:
    rows = [_entry_policy(1, "MARKET")]
    for buffer_ticks, time_in_force_seconds in product((1, 4), (1_800, 3_600)):
        rows.append(
            _entry_policy(
                len(rows) + 1,
                "STOP_SIGNAL_EXTREME",
                buffer_ticks=buffer_ticks,
                time_in_force_seconds=time_in_force_seconds,
            )
        )
    for retrace_atr, time_in_force_seconds in product(
        (Fraction(1, 4), Fraction(1, 2)), (1_800, 3_600)
    ):
        rows.append(
            _entry_policy(
                len(rows) + 1,
                "LIMIT_ATR_RETRACE",
                retrace_atr=retrace_atr,
                time_in_force_seconds=time_in_force_seconds,
            )
        )
    candidates = tuple(rows)
    return EntryCatalog(candidates, canonical_sha256([item.as_dict() for item in candidates]))


@lru_cache(maxsize=1)
def build_exit_catalog() -> ExitCatalog:
    rows: list[ExitPolicy] = []
    for horizon_seconds in REFERENCE_HORIZONS_SECONDS:
        rows.append(
            _exit_policy(
                len(rows) + 1,
                "TERMINAL",
                horizon_seconds=horizon_seconds,
            )
        )
    for take_profit_atr, stop_loss_atr, cap_seconds in product(
        (Fraction(1, 2), Fraction(1), Fraction(3, 2), Fraction(2), Fraction(3)),
        (Fraction(1, 2), Fraction(1), Fraction(3, 2), Fraction(2)),
        (3_600, 10_800, 21_600),
    ):
        rows.append(
            _exit_policy(
                len(rows) + 1,
                "BRACKET",
                take_profit_atr=take_profit_atr,
                stop_loss_atr=stop_loss_atr,
                cap_seconds=cap_seconds,
            )
        )
    for activation_atr, trail_atr, cap_seconds in product(
        (Fraction(1, 2), Fraction(1)),
        (Fraction(1, 2), Fraction(1)),
        (10_800, 21_600),
    ):
        rows.append(
            _exit_policy(
                len(rows) + 1,
                "TRAILING",
                activation_atr=activation_atr,
                trail_atr=trail_atr,
                cap_seconds=cap_seconds,
            )
        )
    for activation_atr, initial_stop_atr, cap_seconds in product(
        (Fraction(1, 2), Fraction(1)),
        (Fraction(1, 2), Fraction(1)),
        (10_800, 21_600),
    ):
        rows.append(
            _exit_policy(
                len(rows) + 1,
                "BREAK_EVEN",
                activation_atr=activation_atr,
                initial_stop_atr=initial_stop_atr,
                cap_seconds=cap_seconds,
            )
        )
    for rule_kind, cap_seconds in product(
        ("OPPOSITE_TRIGGER", "CONTEXT_INVALID"), (10_800, 21_600)
    ):
        rows.append(
            _exit_policy(
                len(rows) + 1,
                "RULE",
                rule_kind=rule_kind,
                cap_seconds=cap_seconds,
            )
        )
    if len(rows) != EXIT_POLICY_COUNT:
        raise SymbolicEngineError("exit construction count differs")
    candidates = tuple(rows)
    return ExitCatalog(candidates, canonical_sha256([item.as_dict() for item in candidates]))


@dataclass(frozen=True, slots=True)
class ContextSpec:
    selection_rank: int
    context_id: str
    kind: str
    timeframe_seconds: int
    fast_period: int
    slow_period: int
    relation: int

    def __post_init__(self) -> None:
        _require_int(self.selection_rank, label="context selection_rank", minimum=1)
        _require_sha(self.context_id, label="context_id")
        if self.kind not in {
            "ANY",
            "EMA_RELATION",
            "EFFICIENCY_RANGE",
            "EFFICIENCY_TREND",
            "VOLATILITY_EXPANDING",
            "VOLATILITY_CONTRACTING",
        }:
            raise SymbolicEngineError("context kind is invalid")
        if canonical_sha256(self.definition_dict()) != self.context_id:
            raise SymbolicEngineError("context id differs from its definition")

    def definition_dict(self) -> dict[str, object]:
        return {
            "fast_period": self.fast_period,
            "kind": self.kind,
            "relation": self.relation,
            "schema": AXIS_CATALOG_SCHEMA,
            "slow_period": self.slow_period,
            "timeframe_seconds": self.timeframe_seconds,
        }

    def as_dict(self) -> dict[str, object]:
        return {
            **self.definition_dict(),
            "context_id": self.context_id,
            "selection_rank": self.selection_rank,
        }


@dataclass(frozen=True, slots=True)
class TimeFilterSpec:
    selection_rank: int
    time_filter_id: str
    kind: str
    value: int

    def __post_init__(self) -> None:
        _require_int(self.selection_rank, label="time-filter selection_rank", minimum=1)
        _require_sha(self.time_filter_id, label="time_filter_id")
        if self.kind not in {"ALL", "UTC_FOUR_HOUR", "UTC_WEEKDAY"}:
            raise SymbolicEngineError("time-filter kind is invalid")
        if canonical_sha256(self.definition_dict()) != self.time_filter_id:
            raise SymbolicEngineError("time-filter id differs from its definition")

    def definition_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "schema": AXIS_CATALOG_SCHEMA,
            "value": self.value,
        }

    def as_dict(self) -> dict[str, object]:
        return {
            **self.definition_dict(),
            "selection_rank": self.selection_rank,
            "time_filter_id": self.time_filter_id,
        }


@dataclass(frozen=True, slots=True)
class DelaySpec:
    selection_rank: int
    delay_id: str
    kind: str

    def __post_init__(self) -> None:
        _require_int(self.selection_rank, label="delay selection_rank", minimum=1)
        _require_sha(self.delay_id, label="delay_id")
        if self.kind not in {
            "IMMEDIATE",
            "NATIVE_PLUS_1_ALIVE",
            "NATIVE_PLUS_2_ALIVE",
            "NEXT_STRICT_UTC_30M_ALIVE",
            "NEXT_STRICT_UTC_1H_ALIVE",
            "NEXT_UTC_1H_AFTER_PLUS_1H_ALIVE",
        }:
            raise SymbolicEngineError("delay kind is invalid")
        if canonical_sha256(self.definition_dict()) != self.delay_id:
            raise SymbolicEngineError("delay id differs from its definition")

    def definition_dict(self) -> dict[str, object]:
        return {"kind": self.kind, "schema": AXIS_CATALOG_SCHEMA}

    def as_dict(self) -> dict[str, object]:
        return {
            **self.definition_dict(),
            "delay_id": self.delay_id,
            "selection_rank": self.selection_rank,
        }


def _axis_id(definition: Mapping[str, object]) -> str:
    return canonical_sha256(definition)


@lru_cache(maxsize=1)
def build_context_catalog() -> tuple[ContextSpec, ...]:
    raw: list[tuple[str, int, int, int, int]] = [("ANY", 0, 0, 0, 0)]
    for timeframe, (fast, slow), relation in product(
        (HALF_HOUR, ONE_HOUR), ((8, 21), (12, 26)), (1, -1)
    ):
        raw.append(("EMA_RELATION", timeframe, fast, slow, relation))
    raw.extend(
        (
            ("EFFICIENCY_RANGE", ONE_HOUR, 0, 24, 0),
            ("EFFICIENCY_TREND", ONE_HOUR, 0, 24, 0),
            ("VOLATILITY_EXPANDING", ONE_HOUR, 6, 24, 0),
            ("VOLATILITY_CONTRACTING", ONE_HOUR, 6, 24, 0),
        )
    )
    rows: list[ContextSpec] = []
    for rank, (kind, timeframe, fast, slow, relation) in enumerate(raw, start=1):
        definition = {
            "fast_period": fast,
            "kind": kind,
            "relation": relation,
            "schema": AXIS_CATALOG_SCHEMA,
            "slow_period": slow,
            "timeframe_seconds": timeframe,
        }
        rows.append(ContextSpec(rank, _axis_id(definition), kind, timeframe, fast, slow, relation))
    if len(rows) != CONTEXT_COUNT or len({item.context_id for item in rows}) != CONTEXT_COUNT:
        raise SymbolicEngineError("context catalog count or identity differs")
    return tuple(rows)


@lru_cache(maxsize=1)
def build_time_filter_catalog() -> tuple[TimeFilterSpec, ...]:
    raw = [("ALL", -1)]
    raw.extend(("UTC_FOUR_HOUR", hour) for hour in range(0, 24, 4))
    raw.extend(("UTC_WEEKDAY", weekday) for weekday in range(7))
    rows: list[TimeFilterSpec] = []
    for rank, (kind, value) in enumerate(raw, start=1):
        definition = {"kind": kind, "schema": AXIS_CATALOG_SCHEMA, "value": value}
        rows.append(TimeFilterSpec(rank, _axis_id(definition), kind, value))
    if len(rows) != TIME_FILTER_COUNT or len({item.time_filter_id for item in rows}) != len(rows):
        raise SymbolicEngineError("time-filter catalog count or identity differs")
    return tuple(rows)


@lru_cache(maxsize=1)
def build_delay_catalog() -> tuple[DelaySpec, ...]:
    kinds = (
        "IMMEDIATE",
        "NATIVE_PLUS_1_ALIVE",
        "NATIVE_PLUS_2_ALIVE",
        "NEXT_STRICT_UTC_30M_ALIVE",
        "NEXT_STRICT_UTC_1H_ALIVE",
        "NEXT_UTC_1H_AFTER_PLUS_1H_ALIVE",
    )
    rows = []
    for rank, kind in enumerate(kinds, start=1):
        definition = {"kind": kind, "schema": AXIS_CATALOG_SCHEMA}
        rows.append(DelaySpec(rank, _axis_id(definition), kind))
    if len(rows) != DELAY_COUNT or len({item.delay_id for item in rows}) != DELAY_COUNT:
        raise SymbolicEngineError("delay catalog count or identity differs")
    return tuple(rows)


@dataclass(frozen=True, slots=True)
class AnchorPolicy:
    policy_rank: int
    policy_id: str
    base_candidate_id: str
    context_id: str
    time_filter_id: str
    delay_id: str

    def __post_init__(self) -> None:
        _require_int(self.policy_rank, label="policy_rank", minimum=1)
        _require_sha(self.policy_id, label="policy_id")
        for label, value in (
            ("base_candidate_id", self.base_candidate_id),
            ("context_id", self.context_id),
            ("time_filter_id", self.time_filter_id),
            ("delay_id", self.delay_id),
        ):
            _require_sha(value, label=label)
        if canonical_sha256(self.definition_dict()) != self.policy_id:
            raise SymbolicEngineError("policy id differs from its definition")

    def definition_dict(self) -> dict[str, object]:
        return {
            "base_candidate_id": self.base_candidate_id,
            "context_id": self.context_id,
            "delay_id": self.delay_id,
            "schema": POLICY_SCHEMA,
            "time_filter_id": self.time_filter_id,
        }

    def as_dict(self) -> dict[str, object]:
        return {
            **self.definition_dict(),
            "policy_id": self.policy_id,
            "policy_rank": self.policy_rank,
        }


def iter_anchor_policies() -> Iterator[AnchorPolicy]:
    """Stream the exact 1,900,080-member policy catalog without retaining it."""

    rank = 0
    for candidate, context, time_filter, delay in product(
        build_base_event_catalog().candidates,
        build_context_catalog(),
        build_time_filter_catalog(),
        build_delay_catalog(),
    ):
        rank += 1
        definition = {
            "base_candidate_id": candidate.candidate_id,
            "context_id": context.context_id,
            "delay_id": delay.delay_id,
            "schema": POLICY_SCHEMA,
            "time_filter_id": time_filter.time_filter_id,
        }
        yield AnchorPolicy(
            rank,
            canonical_sha256(definition),
            candidate.candidate_id,
            context.context_id,
            time_filter.time_filter_id,
            delay.delay_id,
        )
    if rank != LOGICAL_ANCHOR_POLICY_COUNT:  # pragma: no cover - generator invariant
        raise SymbolicEngineError("anchor-policy catalog count differs")


def _validated_dates(values: Iterable[date]) -> tuple[date, ...]:
    dates = tuple(values)
    if (
        not dates
        or any(isinstance(item, datetime) or not isinstance(item, date) for item in dates)
        or dates != tuple(sorted(set(dates)))
    ):
        raise SymbolicEngineError("decision_dates must be unique increasing date values")
    return dates


def _lineage(item: BarWithOutcomeSpan) -> tuple[str, int, int]:
    return item.bar.contract, item.outcome_span_id, item.bar.segment_id


def _indicator_continues(
    previous: BarWithOutcomeSpan,
    current: BarWithOutcomeSpan,
    *,
    date_rank: Mapping[date, int],
) -> bool:
    left = previous.bar
    right = current.bar
    if left.timeframe_seconds != right.timeframe_seconds:
        return False
    if left.source_date == right.source_date:
        return left.end_ns == right.start_ns and _lineage(previous) == _lineage(current)
    gap_ns = right.start_ns - left.end_ns
    return (
        date_rank.get(right.source_date, -2) == date_rank.get(left.source_date, -1) + 1
        and left.contract == right.contract
        and previous.outcome_span_id == current.outcome_span_id
        and 0 <= gap_ns <= MAX_DAILY_BRIDGE_SECONDS * ONE_SECOND_NS
    )


@dataclass(frozen=True, slots=True)
class _Series:
    bars: tuple[BarWithOutcomeSpan, ...]
    continues: tuple[bool, ...]
    end_ns: tuple[int, ...]
    indices_by_contract_span: Mapping[tuple[str, int], tuple[int, ...]]
    ends_by_contract_span: Mapping[tuple[str, int], tuple[int, ...]]

    def __post_init__(self) -> None:
        if (
            not self.bars
            or len(self.bars) != len(self.continues)
            or len(self.bars) != len(self.end_ns)
            or self.continues[0]
        ):
            raise SymbolicEngineError("internal series shape differs")

    def latest_index(self, end_ns: int) -> int | None:
        index = bisect_right(self.end_ns, end_ns) - 1
        return None if index < 0 else index

    def latest_contract_span_index(
        self,
        end_ns: int,
        contract: str,
        outcome_span_id: int,
    ) -> int | None:
        key = contract, outcome_span_id
        indices = self.indices_by_contract_span.get(key, ())
        ends = self.ends_by_contract_span.get(key, ())
        position = bisect_right(ends, end_ns) - 1
        return None if position < 0 else indices[position]

    def window(self, end_index: int, length: int, *, offset: int = 0) -> tuple[int, int] | None:
        stop = end_index - offset + 1
        start = stop - length
        if length <= 0 or start < 0 or stop > len(self.bars):
            return None
        if any(not self.continues[index] for index in range(start + 1, stop)):
            return None
        return start, stop


def _validated_series(
    values: Sequence[BarWithOutcomeSpan],
    *,
    timeframe_seconds: int,
    decision_dates: tuple[date, ...],
) -> _Series:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence) or not values:
        raise SymbolicEngineError("each signal timeframe requires a non-empty bar sequence")
    allowed = frozenset(decision_dates)
    selected = tuple(item for item in values if item.bar.source_date in allowed)
    if not selected:
        raise SymbolicEngineError("a signal timeframe has no decision-date bars")
    prior: tuple[int, str, int, int] | None = None
    seen: set[tuple[int, str, int, int]] = set()
    for item in selected:
        if not isinstance(item, BarWithOutcomeSpan):
            raise SymbolicEngineError("bar sequences must contain BarWithOutcomeSpan")
        bar = item.bar
        if bar.timeframe_seconds != timeframe_seconds:
            raise SymbolicEngineError("bar sequence contains a wrong timeframe")
        identity = bar.start_ns, bar.contract, item.outcome_span_id, bar.segment_id
        if identity in seen or (prior is not None and identity <= prior):
            raise SymbolicEngineError("bar sequence is duplicate or non-canonical")
        seen.add(identity)
        prior = identity
    date_rank = {item: rank for rank, item in enumerate(decision_dates)}
    continues = [False]
    for previous, current in pairwise(selected):
        continues.append(_indicator_continues(previous, current, date_rank=date_rank))
    indices_by_contract_span: dict[tuple[str, int], list[int]] = defaultdict(list)
    for index, item in enumerate(selected):
        indices_by_contract_span[item.bar.contract, item.outcome_span_id].append(index)
    frozen_indices = {key: tuple(items) for key, items in indices_by_contract_span.items()}
    frozen_ends = {
        key: tuple(selected[index].bar.end_ns for index in items)
        for key, items in frozen_indices.items()
    }
    return _Series(
        selected,
        tuple(continues),
        tuple(item.bar.end_ns for item in selected),
        frozen_indices,
        frozen_ends,
    )


class _FeatureCache:
    """Shared stage-local fixed-point and exact-rational feature arrays."""

    def __init__(self, series_by_timeframe: Mapping[int, _Series]) -> None:
        self.series_by_timeframe = dict(series_by_timeframe)
        self._true_range: dict[int, tuple[int | None, ...]] = {}
        self._atr_sum: dict[tuple[int, int], tuple[int | None, ...]] = {}
        self._ema: dict[tuple[int, int], tuple[int | None, ...]] = {}
        self._macd: dict[
            tuple[int, int, int, int],
            tuple[tuple[int | None, ...], tuple[int | None, ...]],
        ] = {}
        self._rsi: dict[tuple[int, int], tuple[int | None, ...]] = {}
        self._stochastic: dict[
            tuple[int, int, int],
            tuple[tuple[int | None, ...], tuple[int | None, ...]],
        ] = {}

    def true_range(self, timeframe: int) -> tuple[int | None, ...]:
        cached = self._true_range.get(timeframe)
        if cached is not None:
            return cached
        series = self.series_by_timeframe[timeframe]
        output: list[int | None] = []
        for index, wrapped in enumerate(series.bars):
            bar = wrapped.bar
            if not series.continues[index]:
                output.append(None)
                continue
            previous_close = series.bars[index - 1].bar.close_ticks
            output.append(
                max(
                    bar.high_ticks - bar.low_ticks,
                    abs(bar.high_ticks - previous_close),
                    abs(bar.low_ticks - previous_close),
                )
            )
        result = tuple(output)
        self._true_range[timeframe] = result
        return result

    def atr_sum(self, timeframe: int, period: int = ATR_PERIOD) -> tuple[int | None, ...]:
        key = timeframe, period
        cached = self._atr_sum.get(key)
        if cached is not None:
            return cached
        series = self.series_by_timeframe[timeframe]
        true_ranges = self.true_range(timeframe)
        window: deque[int] = deque(maxlen=period)
        running = 0
        output: list[int | None] = []
        for index, value in enumerate(true_ranges):
            if not series.continues[index]:
                window.clear()
                running = 0
            if value is None:
                output.append(None)
                continue
            if len(window) == period:
                running -= window[0]
            window.append(value)
            running += value
            output.append(running if len(window) == period else None)
        result = tuple(output)
        self._atr_sum[key] = result
        return result

    def ema(self, timeframe: int, period: int) -> tuple[int | None, ...]:
        key = timeframe, period
        cached = self._ema.get(key)
        if cached is not None:
            return cached
        series = self.series_by_timeframe[timeframe]
        seed: deque[int] = deque(maxlen=period)
        value: int | None = None
        output: list[int | None] = []
        for index, wrapped in enumerate(series.bars):
            if not series.continues[index]:
                seed.clear()
                value = None
            close = wrapped.bar.close_ticks * EMA_SCALE
            if value is None:
                seed.append(close)
                if len(seed) == period:
                    value = _round_half_even(sum(seed), period)
            else:
                value = _round_half_even((period - 1) * value + 2 * close, period + 1)
            output.append(value)
        result = tuple(output)
        self._ema[key] = result
        return result

    def macd(
        self, timeframe: int, fast: int, slow: int, signal: int
    ) -> tuple[tuple[int | None, ...], tuple[int | None, ...]]:
        key = timeframe, fast, slow, signal
        cached = self._macd.get(key)
        if cached is not None:
            return cached
        series = self.series_by_timeframe[timeframe]
        fast_values = self.ema(timeframe, fast)
        slow_values = self.ema(timeframe, slow)
        signal_seed: deque[int] = deque(maxlen=signal)
        signal_value: int | None = None
        lines: list[int | None] = []
        histograms: list[int | None] = []
        for index, (fast_value, slow_value) in enumerate(
            zip(fast_values, slow_values, strict=True)
        ):
            if not series.continues[index]:
                signal_seed.clear()
                signal_value = None
            if fast_value is None or slow_value is None:
                lines.append(None)
                histograms.append(None)
                continue
            line = fast_value - slow_value
            lines.append(line)
            if signal_value is None:
                signal_seed.append(line)
                if len(signal_seed) == signal:
                    signal_value = _round_half_even(sum(signal_seed), signal)
            else:
                signal_value = _round_half_even(
                    (signal - 1) * signal_value + 2 * line,
                    signal + 1,
                )
            histograms.append(None if signal_value is None else line - signal_value)
        result = tuple(lines), tuple(histograms)
        self._macd[key] = result
        return result

    def rsi(self, timeframe: int, period: int) -> tuple[int | None, ...]:
        key = timeframe, period
        cached = self._rsi.get(key)
        if cached is not None:
            return cached
        series = self.series_by_timeframe[timeframe]
        gains: deque[int] = deque(maxlen=period)
        losses: deque[int] = deque(maxlen=period)
        average_gain: int | None = None
        average_loss: int | None = None
        output: list[int | None] = []
        for index, wrapped in enumerate(series.bars):
            if not series.continues[index]:
                gains.clear()
                losses.clear()
                average_gain = None
                average_loss = None
                output.append(None)
                continue
            delta = wrapped.bar.close_ticks - series.bars[index - 1].bar.close_ticks
            gain = max(delta, 0) * INDICATOR_SCALE
            loss = max(-delta, 0) * INDICATOR_SCALE
            if average_gain is None or average_loss is None:
                gains.append(gain)
                losses.append(loss)
                if len(gains) == period:
                    average_gain = _round_half_even(sum(gains), period)
                    average_loss = _round_half_even(sum(losses), period)
            else:
                average_gain = _round_half_even((period - 1) * average_gain + gain, period)
                average_loss = _round_half_even((period - 1) * average_loss + loss, period)
            if average_gain is None or average_loss is None:
                output.append(None)
            elif average_gain + average_loss == 0:
                output.append(50 * INDICATOR_SCALE)
            else:
                output.append(
                    _round_half_even(
                        100 * INDICATOR_SCALE * average_gain,
                        average_gain + average_loss,
                    )
                )
        result = tuple(output)
        self._rsi[key] = result
        return result

    def stochastic(
        self, timeframe: int, k_period: int, d_period: int
    ) -> tuple[tuple[int | None, ...], tuple[int | None, ...]]:
        key = timeframe, k_period, d_period
        cached = self._stochastic.get(key)
        if cached is not None:
            return cached
        series = self.series_by_timeframe[timeframe]
        k_values: list[int | None] = []
        d_values: list[int | None] = []
        d_seed: deque[int] = deque(maxlen=d_period)
        for index, wrapped in enumerate(series.bars):
            if not series.continues[index]:
                d_seed.clear()
            bounds = series.window(index, k_period)
            if bounds is None:
                k_values.append(None)
                d_values.append(None)
                continue
            start, stop = bounds
            high = max(item.bar.high_ticks for item in series.bars[start:stop])
            low = min(item.bar.low_ticks for item in series.bars[start:stop])
            k_value = (
                50 * INDICATOR_SCALE
                if high == low
                else _round_half_even(
                    100 * INDICATOR_SCALE * (wrapped.bar.close_ticks - low), high - low
                )
            )
            k_values.append(k_value)
            d_seed.append(k_value)
            d_values.append(
                _round_half_even(sum(d_seed), d_period) if len(d_seed) == d_period else None
            )
        result = tuple(k_values), tuple(d_values)
        self._stochastic[key] = result
        return result


@dataclass(frozen=True, slots=True)
class _SignalFrame:
    active: bool
    event: bool
    frozen: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True, slots=True)
class EventOccurrence:
    candidate_id: str
    direction: Direction
    trigger_timeframe_seconds: int
    series_index: int
    source_date: date
    contract: str
    outcome_span_id: int
    segment_id: int
    trigger_start_ns: int
    trigger_end_ns: int
    trigger_open_ticks: int
    trigger_high_ticks: int
    trigger_low_ticks: int
    trigger_close_ticks: int
    atr_sum_ticks: int
    frozen: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True, order=True)
class AnchorRecord:
    source_date: date
    contract: str
    outcome_span_id: int
    segment_id: int
    anchor_ns: int
    direction: Direction
    trigger_start_ns: int
    trigger_end_ns: int
    trigger_open_ticks: int
    trigger_high_ticks: int
    trigger_low_ticks: int
    trigger_close_ticks: int
    atr_sum_ticks: int
    atr_denominator: int
    frozen: tuple[tuple[str, int], ...]

    @property
    def outcome_key(self) -> tuple[str, int, int, int, Direction]:
        return (
            self.contract,
            self.outcome_span_id,
            self.segment_id,
            self.anchor_ns,
            self.direction,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "anchor_ns": self.anchor_ns,
            "atr_denominator": self.atr_denominator,
            "atr_sum_ticks": self.atr_sum_ticks,
            "contract": self.contract,
            "direction": self.direction,
            "frozen": dict(self.frozen),
            "outcome_span_id": self.outcome_span_id,
            "segment_id": self.segment_id,
            "source_date": self.source_date.isoformat(),
            "trigger_close_ticks": self.trigger_close_ticks,
            "trigger_end_ns": self.trigger_end_ns,
            "trigger_high_ticks": self.trigger_high_ticks,
            "trigger_low_ticks": self.trigger_low_ticks,
            "trigger_open_ticks": self.trigger_open_ticks,
            "trigger_start_ns": self.trigger_start_ns,
        }


def _direction_sign(direction: Direction) -> int:
    return 1 if direction == "LONG" else -1


def _atr_threshold_met(move_ticks: int, atr_sum_ticks: int, threshold: Fraction) -> bool:
    return (
        move_ticks >= 0
        and move_ticks * threshold.denominator * ATR_PERIOD >= threshold.numerator * atr_sum_ticks
    )


def _fraction_ge(numerator: int, denominator: int, threshold: Fraction) -> bool:
    return (
        denominator > 0 and numerator * threshold.denominator >= denominator * threshold.numerator
    )


def _fraction_le(numerator: int, denominator: int, threshold: Fraction) -> bool:
    return (
        denominator > 0 and numerator * threshold.denominator <= denominator * threshold.numerator
    )


def _rolling_extreme(
    series: _Series,
    index: int,
    length: int,
    *,
    offset: int = 0,
) -> tuple[int, int] | None:
    bounds = series.window(index, length, offset=offset)
    if bounds is None:
        return None
    start, stop = bounds
    return (
        max(item.bar.high_ticks for item in series.bars[start:stop]),
        min(item.bar.low_ticks for item in series.bars[start:stop]),
    )


def _rolling_sum(values: Sequence[int | None], bounds: tuple[int, int] | None) -> int | None:
    if bounds is None:
        return None
    start, stop = bounds
    selected = values[start:stop]
    if any(value is None for value in selected):
        return None
    return sum(value for value in selected if value is not None)


class SymbolicStage:
    """One stage-local, feature-only symbolic state with shared caches."""

    def __init__(
        self,
        series_by_timeframe: Mapping[int, _Series],
        decision_dates: tuple[date, ...],
    ) -> None:
        if tuple(sorted(series_by_timeframe)) != SUPPORTED_SIGNAL_TIMEFRAMES:
            raise SymbolicEngineError("symbolic stage requires exact 5m/30m/1h series")
        self.series_by_timeframe = dict(series_by_timeframe)
        self.decision_dates = decision_dates
        self.date_rank = {item: rank for rank, item in enumerate(decision_dates)}
        self.features = _FeatureCache(self.series_by_timeframe)
        self._frames: dict[str, tuple[_SignalFrame, ...]] = {}
        self._events: dict[str, tuple[EventOccurrence, ...]] = {}
        self._delayed: dict[tuple[str, str], tuple[AnchorRecord, ...]] = {}
        self._five_minute_end_index: dict[tuple[int, str, int], list[int]] = defaultdict(list)
        for index, item in enumerate(self.series_by_timeframe[FIVE_MINUTES].bars):
            key = item.bar.end_ns, item.bar.contract, item.outcome_span_id
            self._five_minute_end_index[key].append(index)

    def release_candidate_state(self, candidate: BaseEventCandidate) -> None:
        """Release candidate/mirror event state after a bounded cube is scored."""

        mirror = self._mirror_candidate(candidate)
        candidate_ids = {candidate.candidate_id, mirror.candidate_id}
        for candidate_id in candidate_ids:
            self._frames.pop(candidate_id, None)
            self._events.pop(candidate_id, None)
        for key in tuple(self._delayed):
            if key[0] in candidate_ids:
                self._delayed.pop(key, None)

    def _compatible_context(
        self,
        context: BarWithOutcomeSpan,
        trigger: BarWithOutcomeSpan,
    ) -> bool:
        left = context.bar
        right = trigger.bar
        if context.outcome_span_id != trigger.outcome_span_id or left.contract != right.contract:
            return False
        if left.end_ns > right.end_ns:
            return False
        if left.source_date == right.source_date:
            return left.segment_id == right.segment_id
        gap_ns = right.start_ns - left.end_ns
        return (
            self.date_rank.get(right.source_date, -2)
            == self.date_rank.get(left.source_date, -1) + 1
            and 0 <= gap_ns <= MAX_DAILY_BRIDGE_SECONDS * ONE_SECOND_NS
        )

    def _context_index(
        self,
        timeframe: int,
        trigger: BarWithOutcomeSpan,
        end_ns: int,
    ) -> int | None:
        series = self.series_by_timeframe[timeframe]
        index = series.latest_contract_span_index(
            end_ns,
            trigger.bar.contract,
            trigger.outcome_span_id,
        )
        if index is None or not self._compatible_context(series.bars[index], trigger):
            return None
        return index

    def _active_transition_frames(
        self,
        series: _Series,
        active: Sequence[bool],
        frozen: Sequence[tuple[tuple[str, int], ...]] | None = None,
        explicit_event: Sequence[bool] | None = None,
    ) -> tuple[_SignalFrame, ...]:
        if len(active) != len(series.bars):
            raise SymbolicEngineError("active-state array shape differs")
        frozen_values = frozen or ((),) * len(active)
        events = explicit_event
        output: list[_SignalFrame] = []
        for index, is_active in enumerate(active):
            prior_active = index > 0 and series.continues[index] and active[index - 1]
            event = (is_active and not prior_active) if events is None else events[index]
            output.append(_SignalFrame(is_active, bool(event), frozen_values[index]))
        return tuple(output)

    def frames(self, candidate: BaseEventCandidate) -> tuple[_SignalFrame, ...]:
        cached = self._frames.get(candidate.candidate_id)
        if cached is not None:
            return cached
        result = self._build_frames(candidate)
        self._frames[candidate.candidate_id] = result
        return result

    def _build_frames(self, candidate: BaseEventCandidate) -> tuple[_SignalFrame, ...]:
        timeframe = candidate.trigger_timeframe_seconds
        series = self.series_by_timeframe[timeframe]
        bars = series.bars
        sign = _direction_sign(candidate.direction)
        atr = self.features.atr_sum(timeframe)
        active = [False] * len(bars)
        explicit_event: list[bool] | None = None
        frozen: list[tuple[tuple[str, int], ...]] = [()] * len(bars)
        parameters = dict(candidate.parameters)

        if candidate.family == "MOMENTUM_RETURN":
            lookback = parameters["lookback_bars"]
            threshold = candidate.fraction_parameter("threshold_atr")
            for index, wrapped in enumerate(bars):
                bounds = series.window(index, lookback + 1)
                if bounds is None or atr[index] is None:
                    continue
                prior = bars[index - lookback].bar.close_ticks
                active[index] = _atr_threshold_met(
                    sign * (wrapped.bar.close_ticks - prior), atr[index], threshold
                )

        elif candidate.family == "DONCHIAN_BREAKOUT":
            lookback = parameters["lookback_bars"]
            confirmation = parameters["confirmation_bars"]
            buffer_atr = candidate.fraction_parameter("buffer_atr")
            for index in range(len(bars)):
                channel = _rolling_extreme(series, index, lookback, offset=confirmation)
                confirm_bounds = series.window(index, confirmation)
                if channel is None or confirm_bounds is None or atr[index] is None:
                    continue
                high, low = channel
                level = high if sign > 0 else low
                start, stop = confirm_bounds
                confirmed = all(
                    _atr_threshold_met(
                        sign * (item.bar.close_ticks - level), atr[index], buffer_atr
                    )
                    for item in bars[start:stop]
                )
                active[index] = confirmed
                frozen[index] = (("level_ticks", level), ("persistence_kind", 1))

        elif candidate.family == "EMA_TREND":
            fast = self.features.ema(timeframe, parameters["fast_period"])
            slow = self.features.ema(timeframe, parameters["slow_period"])
            mode = parameters["mode"]
            explicit_event = [False] * len(bars)
            for index in range(len(bars)):
                if fast[index] is None or slow[index] is None:
                    continue
                spread = sign * (fast[index] - slow[index])
                active[index] = spread > 0
                if mode == 2:
                    active[index] = (
                        active[index]
                        and index > 0
                        and series.continues[index]
                        and fast[index - 1] is not None
                        and sign * (fast[index] - fast[index - 1]) > 0
                    )
                previous_spread = (
                    None
                    if index == 0
                    or not series.continues[index]
                    or fast[index - 1] is None
                    or slow[index - 1] is None
                    else sign * (fast[index - 1] - slow[index - 1])
                )
                explicit_event[index] = active[index] and (
                    (mode == 1 and previous_spread is not None and previous_spread <= 0)
                    or (mode == 2 and (index == 0 or not active[index - 1]))
                )

        elif candidate.family == "MACD_STATE":
            line, histogram = self.features.macd(
                timeframe,
                parameters["fast_period"],
                parameters["slow_period"],
                parameters["signal_period"],
            )
            mode = parameters["mode"]
            values = histogram if mode != 2 else line
            explicit_event = [False] * len(bars)
            for index, value in enumerate(values):
                if value is None:
                    continue
                active[index] = sign * value > 0
                if mode == 3:
                    active[index] = (
                        active[index]
                        and index > 0
                        and series.continues[index]
                        and values[index - 1] is not None
                        and sign * (value - values[index - 1]) > 0
                    )
                previous = None if index == 0 or not series.continues[index] else values[index - 1]
                explicit_event[index] = active[index] and (
                    (mode in (1, 2) and previous is not None and sign * previous <= 0)
                    or (mode == 3 and (index == 0 or not active[index - 1]))
                )

        elif candidate.family == "RSI_STATE":
            values = self.features.rsi(timeframe, parameters["period"])
            lower = parameters["lower_band"] * INDICATOR_SCALE
            upper = parameters["upper_band"] * INDICATOR_SCALE
            mode = parameters["mode"]
            explicit_event = [False] * len(bars)
            for index, value in enumerate(values):
                if value is None:
                    continue
                threshold = lower if sign > 0 else upper
                active[index] = sign * (value - threshold) > 0
                previous = None if index == 0 or not series.continues[index] else values[index - 1]
                explicit_event[index] = (
                    previous is not None and active[index] and sign * (previous - threshold) <= 0
                )
                if mode == 1:
                    frozen[index] = (("rsi_exit_band", threshold),)

        elif candidate.family == "STOCHASTIC_STATE":
            k_values, d_values = self.features.stochastic(
                timeframe, parameters["k_period"], parameters["d_period"]
            )
            lower = parameters["lower_band"] * INDICATOR_SCALE
            upper = parameters["upper_band"] * INDICATOR_SCALE
            mode = parameters["mode"]
            explicit_event = [False] * len(bars)
            for index, (k_value, d_value) in enumerate(zip(k_values, d_values, strict=True)):
                if k_value is None or d_value is None:
                    continue
                if mode == 1:
                    active[index] = sign * (k_value - d_value) > 0
                    if index > 0 and series.continues[index]:
                        prior_k = k_values[index - 1]
                        prior_d = d_values[index - 1]
                        comparison_k = k_value if prior_k is None else prior_k
                        extreme = (
                            min(k_value, comparison_k) <= lower
                            if sign > 0
                            else max(k_value, comparison_k) >= upper
                        )
                        explicit_event[index] = (
                            prior_k is not None
                            and prior_d is not None
                            and extreme
                            and active[index]
                            and sign * (prior_k - prior_d) <= 0
                        )
                else:
                    threshold = lower if sign > 0 else upper
                    active[index] = sign * (k_value - threshold) > 0
                    prior_k = (
                        None if index == 0 or not series.continues[index] else k_values[index - 1]
                    )
                    explicit_event[index] = (
                        prior_k is not None and active[index] and sign * (prior_k - threshold) <= 0
                    )

        elif candidate.family == "BOLLINGER_STATE":
            lookback = parameters["lookback_bars"]
            band = candidate.fraction_parameter("band_sigma")
            mode = parameters["mode"]
            outside_direction = [False] * len(bars)
            outside_opposite = [False] * len(bars)
            for index, wrapped in enumerate(bars):
                bounds = series.window(index, lookback)
                if bounds is None:
                    continue
                start, stop = bounds
                closes = [item.bar.close_ticks for item in bars[start:stop]]
                total = sum(closes)
                square_total = sum(value * value for value in closes)
                variance_term = lookback * square_total - total * total
                delta = wrapped.bar.close_ticks * lookback - total
                outside = (
                    delta * delta * band.denominator * band.denominator
                    >= band.numerator * band.numerator * variance_term
                )
                outside_direction[index] = sign * delta > 0 and outside
                outside_opposite[index] = -sign * delta > 0 and outside
                frozen[index] = (("mean_numerator", total), ("mean_denominator", lookback))
            explicit_event = [False] * len(bars)
            for index in range(len(bars)):
                if mode == 1:
                    active[index] = outside_direction[index]
                    explicit_event[index] = active[index] and (
                        index > 0 and series.continues[index] and not outside_direction[index - 1]
                    )
                else:
                    active[index] = not outside_opposite[index]
                    explicit_event[index] = (
                        index > 0
                        and series.continues[index]
                        and outside_opposite[index - 1]
                        and active[index]
                    )

        elif candidate.family == "RANGE_REVERSION":
            lookback = parameters["lookback_bars"]
            overshoot = candidate.fraction_parameter("overshoot_atr")
            explicit_event = [False] * len(bars)
            for index, wrapped in enumerate(bars):
                channel = _rolling_extreme(series, index, lookback, offset=1)
                if channel is None or atr[index] is None:
                    continue
                high, low = channel
                level = low if sign > 0 else high
                excursion = (
                    level - wrapped.bar.low_ticks if sign > 0 else wrapped.bar.high_ticks - level
                )
                event = (
                    _atr_threshold_met(excursion, atr[index], overshoot)
                    and sign * (wrapped.bar.close_ticks - level) > 0
                )
                active[index] = low < wrapped.bar.close_ticks < high
                explicit_event[index] = event
                frozen[index] = (
                    ("channel_high_ticks", high),
                    ("channel_low_ticks", low),
                    ("persistence_kind", 2),
                )

        elif candidate.family == "COMPRESSION_BREAKOUT":
            context_tf = parameters["context_timeframe_seconds"]
            compression = parameters["compression_window"]
            breakout = parameters["breakout_window"]
            ratio = candidate.fraction_parameter("compression_ratio")
            context_series = self.series_by_timeframe[context_tf]
            explicit_event = [False] * len(bars)
            for index, wrapped in enumerate(bars):
                context_index = self._context_index(context_tf, wrapped, wrapped.bar.end_ns)
                channel = _rolling_extreme(series, index, breakout, offset=1)
                if context_index is None or channel is None:
                    continue
                recent_bounds = context_series.window(context_index, compression)
                prior_bounds = context_series.window(context_index, compression, offset=compression)
                if recent_bounds is None or prior_bounds is None:
                    continue
                recent = sum(
                    item.bar.high_ticks - item.bar.low_ticks
                    for item in context_series.bars[slice(*recent_bounds)]
                )
                prior = sum(
                    item.bar.high_ticks - item.bar.low_ticks
                    for item in context_series.bars[slice(*prior_bounds)]
                )
                high, low = channel
                level = high if sign > 0 else low
                compressed = recent * ratio.denominator <= prior * ratio.numerator
                active[index] = compressed and sign * (wrapped.bar.close_ticks - level) > 0
                explicit_event[index] = active[index]
                frozen[index] = (("level_ticks", level), ("persistence_kind", 1))

        elif candidate.family == "PULLBACK_CONTINUATION":
            context_tf = parameters["context_timeframe_seconds"]
            fast_values = self.features.ema(context_tf, parameters["fast_period"])
            slow_values = self.features.ema(context_tf, parameters["slow_period"])
            pullback = parameters["pullback_bars"]
            context_series = self.series_by_timeframe[context_tf]
            explicit_event = [False] * len(bars)
            for index, wrapped in enumerate(bars):
                context_index = self._context_index(context_tf, wrapped, wrapped.bar.end_ns)
                prior_bounds = series.window(index, pullback, offset=1)
                if context_index is None or prior_bounds is None or context_index == 0:
                    continue
                fast = fast_values[context_index]
                slow = slow_values[context_index]
                prior_fast = fast_values[context_index - 1]
                if fast is None or slow is None or prior_fast is None:
                    continue
                trend = sign * (fast - slow) > 0 and sign * (fast - prior_fast) > 0
                start, stop = prior_bounds
                pullback_seen = all(
                    sign * (item.bar.close_ticks * EMA_SCALE - fast) <= 0
                    for item in bars[start:stop]
                )
                current_side = sign * (wrapped.bar.close_ticks * EMA_SCALE - fast) > 0
                active[index] = trend and current_side
                explicit_event[index] = trend and pullback_seen and current_side
                frozen[index] = (
                    ("fast_ema_scaled", fast),
                    ("persistence_kind", 3),
                )

        elif candidate.family in ("BODY_CONTINUATION", "WICK_REJECTION"):
            magnitude = candidate.fraction_parameter("magnitude_ratio")
            close_threshold = candidate.fraction_parameter("close_location")
            for index, wrapped in enumerate(bars):
                bar = wrapped.bar
                bar_range = bar.high_ticks - bar.low_ticks
                if bar_range <= 0:
                    continue
                close_numerator = (
                    bar.close_ticks - bar.low_ticks
                    if sign > 0
                    else bar.high_ticks - bar.close_ticks
                )
                close_ok = _fraction_ge(close_numerator, bar_range, close_threshold)
                if candidate.family == "BODY_CONTINUATION":
                    magnitude_ticks = sign * (bar.close_ticks - bar.open_ticks)
                else:
                    magnitude_ticks = (
                        min(bar.open_ticks, bar.close_ticks) - bar.low_ticks
                        if sign > 0
                        else bar.high_ticks - max(bar.open_ticks, bar.close_ticks)
                    )
                active[index] = close_ok and _fraction_ge(magnitude_ticks, bar_range, magnitude)

        elif candidate.family == "STRUCTURAL_PRICE_ACTION":
            pattern = parameters["pattern"]
            confirmation = parameters["confirmation_bars"]
            explicit_event = [False] * len(bars)
            for index in range(len(bars)):
                setup_index = index - confirmation
                mother_index = setup_index - 1
                if mother_index < 0 or series.window(index, confirmation + 2) is None:
                    continue
                setup = bars[setup_index].bar
                mother = bars[mother_index].bar
                confirmations = [item.bar for item in bars[setup_index + 1 : index + 1]]
                level = mother.high_ticks if sign > 0 else mother.low_ticks
                if pattern == 1:
                    setup_ok = (
                        setup.high_ticks < mother.high_ticks and setup.low_ticks > mother.low_ticks
                    )
                    confirmed = all(sign * (item.close_ticks - level) > 0 for item in confirmations)
                elif pattern == 2:
                    setup_ok = (
                        setup.high_ticks > mother.high_ticks and setup.low_ticks < mother.low_ticks
                    )
                    level = setup.high_ticks if sign > 0 else setup.low_ticks
                    confirmed = sign * (setup.close_ticks - setup.open_ticks) > 0 and all(
                        sign * (item.close_ticks - level) > 0 for item in confirmations
                    )
                elif pattern == 3:
                    setup_ok = (
                        sign * (mother.close_ticks - mother.open_ticks) < 0
                        and sign * (setup.close_ticks - setup.open_ticks) > 0
                        and (
                            min(setup.open_ticks, setup.close_ticks)
                            < min(mother.open_ticks, mother.close_ticks)
                        )
                        and (
                            max(setup.open_ticks, setup.close_ticks)
                            > max(mother.open_ticks, mother.close_ticks)
                        )
                    )
                    level = setup.close_ticks
                    confirmed = all(
                        sign * (item.close_ticks - level) >= 0 for item in confirmations
                    )
                else:
                    breached = (
                        setup.low_ticks < mother.low_ticks
                        if sign > 0
                        else setup.high_ticks > mother.high_ticks
                    )
                    setup_ok = breached
                    level = mother.low_ticks if sign > 0 else mother.high_ticks
                    confirmed = all(sign * (item.close_ticks - level) > 0 for item in confirmations)
                active[index] = setup_ok and confirmed
                explicit_event[index] = active[index]
                frozen[index] = (("level_ticks", level), ("persistence_kind", 1))

        elif candidate.family == "NBAR_PRICE_ACTION":
            length = parameters["length_bars"]
            pattern = parameters["pattern"]
            for index in range(len(bars)):
                bounds = series.window(index, length)
                if bounds is None:
                    continue
                start, stop = bounds
                selected = [item.bar for item in bars[start:stop]]
                if pattern == 1:
                    active[index] = all(
                        sign * (item.close_ticks - item.open_ticks) > 0 for item in selected
                    )
                elif pattern == 2:
                    active[index] = all(
                        sign * (right.high_ticks - left.high_ticks) > 0
                        and sign * (right.low_ticks - left.low_ticks) > 0
                        for left, right in pairwise(selected)
                    )
                else:
                    prior = selected[:-1]
                    current = selected[-1]
                    opposite_staircase = all(
                        sign * (right.close_ticks - left.close_ticks) < 0
                        for left, right in pairwise(prior)
                    )
                    prior_move = abs(prior[-1].close_ticks - prior[0].open_ticks)
                    active[index] = (
                        opposite_staircase
                        and sign * (current.close_ticks - current.open_ticks) > prior_move
                    )

        elif candidate.family == "VOLATILITY_EXPANSION":
            true_range = self.features.true_range(timeframe)
            short = parameters["short_window"]
            long = parameters["long_window"]
            ratio = candidate.fraction_parameter("expansion_ratio")
            for index, wrapped in enumerate(bars):
                short_sum = _rolling_sum(true_range, series.window(index, short))
                long_sum = _rolling_sum(true_range, series.window(index, long))
                if short_sum is None or long_sum is None:
                    continue
                expanded = (
                    short_sum * long * ratio.denominator >= long_sum * short * ratio.numerator
                )
                active[index] = (
                    expanded and sign * (wrapped.bar.close_ticks - wrapped.bar.open_ticks) > 0
                )

        elif candidate.family == "EFFICIENCY_RATIO":
            lookback = parameters["lookback_bars"]
            threshold = candidate.fraction_parameter("threshold")
            mode = parameters["mode"]
            for index, wrapped in enumerate(bars):
                bounds = series.window(index, lookback + 1)
                if bounds is None:
                    continue
                start, stop = bounds
                closes = [item.bar.close_ticks for item in bars[start:stop]]
                displacement = abs(closes[-1] - closes[0])
                travel = sum(abs(right - left) for left, right in pairwise(closes))
                if travel == 0:
                    continue
                if mode == 1:
                    active[index] = (
                        _fraction_ge(displacement, travel, threshold)
                        and sign * (closes[-1] - closes[0]) > 0
                    )
                else:
                    mean_numerator = sum(closes[:-1])
                    mean_denominator = len(closes) - 1
                    prior_deviation = sign * (closes[-2] * mean_denominator - mean_numerator)
                    current_deviation = sign * (closes[-1] * mean_denominator - mean_numerator)
                    active[index] = (
                        _fraction_le(displacement, travel, threshold)
                        and prior_deviation < 0
                        and current_deviation < 0
                        and sign * (closes[-1] - closes[-2]) > 0
                    )

        elif candidate.family == "VOLUME_FLOW":
            lookback = parameters["lookback_bars"]
            z_score = parameters["z_score"]
            mode = parameters["mode"]
            for index, wrapped in enumerate(bars):
                bounds = series.window(index, lookback, offset=1)
                if bounds is None:
                    continue
                start, stop = bounds
                volumes = [item.bar.volume for item in bars[start:stop]]
                total = sum(volumes)
                square_total = sum(value * value for value in volumes)
                delta = wrapped.bar.volume * lookback - total
                variance_term = lookback * square_total - total * total
                high_volume = delta > 0 and delta * delta >= z_score * z_score * variance_term
                bar = wrapped.bar
                if mode == 1:
                    active[index] = high_volume and sign * (bar.close_ticks - bar.open_ticks) > 0
                else:
                    if bar.buy_volume is None or bar.sell_volume is None:
                        continue
                    imbalance = bar.buy_volume - bar.sell_volume
                    if mode == 2:
                        active[index] = high_volume and sign * imbalance > 0
                    else:
                        active[index] = (
                            high_volume
                            and sign * imbalance < 0
                            and sign * (bar.close_ticks - bar.open_ticks) > 0
                        )

        elif candidate.family == "ROLLING_VWAP":
            lookback = parameters["lookback_bars"]
            band = candidate.fraction_parameter("band_atr")
            mode = parameters["mode"]
            signed_side: list[int | None] = [None] * len(bars)
            for index, wrapped in enumerate(bars):
                bounds = series.window(index, lookback)
                if bounds is None or atr[index] is None:
                    continue
                start, stop = bounds
                denominator = 3 * sum(item.bar.volume for item in bars[start:stop])
                if denominator == 0:
                    continue
                numerator = sum(
                    item.bar.volume
                    * (item.bar.high_ticks + item.bar.low_ticks + item.bar.close_ticks)
                    for item in bars[start:stop]
                )
                deviation_numerator = sign * (wrapped.bar.close_ticks * denominator - numerator)
                threshold_numerator = band.numerator * atr[index] * denominator
                threshold_denominator = band.denominator * ATR_PERIOD
                side = (
                    deviation_numerator * threshold_denominator - threshold_numerator
                    if mode == 1
                    else deviation_numerator * threshold_denominator + threshold_numerator
                )
                signed_side[index] = side
                active[index] = side > 0 if mode == 1 else side >= 0
                frozen[index] = (
                    ("vwap_numerator", numerator),
                    ("vwap_denominator", denominator),
                )
            explicit_event = [False] * len(bars)
            for index, side in enumerate(signed_side):
                if side is None or index == 0 or not series.continues[index]:
                    continue
                prior_side = signed_side[index - 1]
                if prior_side is None:
                    continue
                if mode == 1:
                    explicit_event[index] = side > 0 and prior_side <= 0
                else:
                    explicit_event[index] = side >= 0 and prior_side < 0

        elif candidate.family == "SWING_FAILURE":
            lookback = parameters["lookback_bars"]
            failure = parameters["failure_window"]
            explicit_event = [False] * len(bars)
            for index, wrapped in enumerate(bars):
                channel = _rolling_extreme(series, index, lookback, offset=failure)
                breach_bounds = series.window(index, failure)
                if channel is None or breach_bounds is None:
                    continue
                high, low = channel
                start, stop = breach_bounds
                selected = [item.bar for item in bars[start:stop]]
                breached = (
                    any(item.low_ticks < low for item in selected)
                    if sign > 0
                    else any(item.high_ticks > high for item in selected)
                )
                level = low if sign > 0 else high
                active[index] = breached and sign * (wrapped.bar.close_ticks - level) > 0
                explicit_event[index] = active[index]
                frozen[index] = (("level_ticks", level), ("persistence_kind", 1))

        elif candidate.family == "GAP_EVENT":
            threshold = candidate.fraction_parameter("threshold_atr")
            mode = parameters["mode"]
            for index, wrapped in enumerate(bars):
                if index == 0 or not series.continues[index] or atr[index] is None:
                    continue
                previous_close = bars[index - 1].bar.close_ticks
                gap = wrapped.bar.open_ticks - previous_close
                directional_gap = sign * gap if mode == 1 else -sign * gap
                active[index] = (
                    _atr_threshold_met(directional_gap, atr[index], threshold)
                    and sign * (wrapped.bar.close_ticks - wrapped.bar.open_ticks) > 0
                )

        else:  # pragma: no cover - all canonical families are exhaustive
            raise SymbolicEngineError(f"unimplemented family {candidate.family}")

        return self._active_transition_frames(
            series,
            active,
            frozen,
            explicit_event,
        )

    def events(self, candidate: BaseEventCandidate) -> tuple[EventOccurrence, ...]:
        cached = self._events.get(candidate.candidate_id)
        if cached is not None:
            return cached
        series = self.series_by_timeframe[candidate.trigger_timeframe_seconds]
        atr = self.features.atr_sum(candidate.trigger_timeframe_seconds)
        output: list[EventOccurrence] = []
        for index, frame in enumerate(self.frames(candidate)):
            if not frame.event or atr[index] is None:
                continue
            wrapped = series.bars[index]
            bar = wrapped.bar
            output.append(
                EventOccurrence(
                    candidate.candidate_id,
                    candidate.direction,
                    candidate.trigger_timeframe_seconds,
                    index,
                    bar.source_date,
                    bar.contract,
                    wrapped.outcome_span_id,
                    bar.segment_id,
                    bar.start_ns,
                    bar.end_ns,
                    bar.open_ticks,
                    bar.high_ticks,
                    bar.low_ticks,
                    bar.close_ticks,
                    atr[index],
                    frame.frozen,
                )
            )
        result = tuple(output)
        self._events[candidate.candidate_id] = result
        return result

    def _mirror_candidate(self, candidate: BaseEventCandidate) -> BaseEventCandidate:
        direction: Direction = "SHORT" if candidate.direction == "LONG" else "LONG"
        definition = {
            "direction": direction,
            "family": candidate.family,
            "parameters": dict(candidate.parameters),
            "schema": BASE_CATALOG_SCHEMA,
        }
        candidate_id = canonical_sha256(definition)
        try:
            return _candidate_lookup()[candidate_id]
        except KeyError as error:  # pragma: no cover - catalog mirror invariant
            raise SymbolicEngineError("candidate catalog lacks direction mirror") from error

    def _same_event_lineage(
        self,
        left: EventOccurrence,
        right: EventOccurrence,
    ) -> bool:
        return (
            left.contract == right.contract
            and left.outcome_span_id == right.outcome_span_id
            and left.segment_id == right.segment_id
        )

    def _delayed_target(self, event: EventOccurrence, delay: DelaySpec) -> int | None:
        if delay.kind == "IMMEDIATE":
            return event.trigger_end_ns
        series = self.series_by_timeframe[event.trigger_timeframe_seconds]
        if delay.kind in {"NATIVE_PLUS_1_ALIVE", "NATIVE_PLUS_2_ALIVE"}:
            steps = 1 if delay.kind == "NATIVE_PLUS_1_ALIVE" else 2
            target_index = event.series_index + steps
            if target_index >= len(series.bars) or any(
                not series.continues[index]
                for index in range(event.series_index + 1, target_index + 1)
            ):
                return None
            return series.bars[target_index].bar.end_ns
        if delay.kind == "NEXT_STRICT_UTC_30M_ALIVE":
            width = HALF_HOUR * ONE_SECOND_NS
            return (event.trigger_end_ns // width + 1) * width
        if delay.kind == "NEXT_STRICT_UTC_1H_ALIVE":
            width = ONE_HOUR * ONE_SECOND_NS
            return (event.trigger_end_ns // width + 1) * width
        if delay.kind == "NEXT_UTC_1H_AFTER_PLUS_1H_ALIVE":
            width = ONE_HOUR * ONE_SECOND_NS
            minimum = event.trigger_end_ns + width
            return ((minimum + width - 1) // width) * width
        raise SymbolicEngineError("unknown delay kind")

    def _alive_at(
        self,
        candidate: BaseEventCandidate,
        event: EventOccurrence,
        target_ns: int,
        delay: DelaySpec,
    ) -> bool:
        if delay.kind == "IMMEDIATE":
            return True
        series = self.series_by_timeframe[candidate.trigger_timeframe_seconds]
        index = series.latest_contract_span_index(
            target_ns,
            event.contract,
            event.outcome_span_id,
        )
        if (
            index is None
            or index < event.series_index
            or _lineage(series.bars[index])
            != (event.contract, event.outcome_span_id, event.segment_id)
            or any(
                not series.continues[position]
                for position in range(event.series_index + 1, index + 1)
            )
        ):
            return False
        frozen = dict(event.frozen)
        sign = _direction_sign(candidate.direction)
        close = series.bars[index].bar.close_ticks
        persistence_kind = frozen.get("persistence_kind", 0)
        if persistence_kind == 1:
            return sign * (close - frozen["level_ticks"]) > 0
        if persistence_kind == 2:
            return frozen["channel_low_ticks"] < close < frozen["channel_high_ticks"]
        if persistence_kind == 3:
            fast = frozen["fast_ema_scaled"]
            if sign * (close * EMA_SCALE - fast) <= 0:
                return False
        return self.frames(candidate)[index].active

    def _execution_anchor(
        self,
        event: EventOccurrence,
        target_ns: int,
    ) -> BarWithOutcomeSpan | None:
        candidates = self._five_minute_end_index.get(
            (target_ns, event.contract, event.outcome_span_id), ()
        )
        series = self.series_by_timeframe[FIVE_MINUTES]
        matches = [
            series.bars[index]
            for index in candidates
            if series.bars[index].bar.segment_id == event.segment_id
        ]
        if len(matches) > 1:
            raise SymbolicEngineError("execution anchor is ambiguous")
        return matches[0] if matches else None

    def delayed_anchors(
        self,
        candidate: BaseEventCandidate,
        delay: DelaySpec,
    ) -> tuple[AnchorRecord, ...]:
        key = candidate.candidate_id, delay.delay_id
        cached = self._delayed.get(key)
        if cached is not None:
            return cached
        same_events = self.events(candidate)
        mirror_events = self.events(self._mirror_candidate(candidate))
        same_ends: dict[tuple[str, int, int], list[int]] = defaultdict(list)
        opposite_ends: dict[tuple[str, int, int], list[int]] = defaultdict(list)
        for item in same_events:
            same_ends[item.contract, item.outcome_span_id, item.segment_id].append(
                item.trigger_end_ns
            )
        for item in mirror_events:
            opposite_ends[item.contract, item.outcome_span_id, item.segment_id].append(
                item.trigger_end_ns
            )
        latest_by_target: dict[tuple[int, str, int, int], EventOccurrence] = {}
        for event in same_events:
            target = self._delayed_target(event, delay)
            if target is None or not self._alive_at(candidate, event, target, delay):
                continue
            lineage = event.contract, event.outcome_span_id, event.segment_id
            same_values = same_ends[lineage]
            next_same = bisect_right(same_values, event.trigger_end_ns)
            if next_same < len(same_values) and same_values[next_same] <= target:
                # A newer same-direction observation owns the pending state.
                continue
            opposite_values = opposite_ends[lineage]
            next_opposite = bisect_left(opposite_values, event.trigger_end_ns)
            if next_opposite < len(opposite_values) and opposite_values[next_opposite] <= target:
                continue
            target_key = target, event.contract, event.outcome_span_id, event.segment_id
            prior = latest_by_target.get(target_key)
            if prior is None or prior.trigger_end_ns < event.trigger_end_ns:
                latest_by_target[target_key] = event
        output: list[AnchorRecord] = []
        for (target, _contract, _span, _segment), event in sorted(latest_by_target.items()):
            anchor = self._execution_anchor(event, target)
            if anchor is None:
                continue
            bar = anchor.bar
            output.append(
                AnchorRecord(
                    bar.source_date,
                    bar.contract,
                    anchor.outcome_span_id,
                    bar.segment_id,
                    target,
                    candidate.direction,
                    event.trigger_start_ns,
                    event.trigger_end_ns,
                    event.trigger_open_ticks,
                    event.trigger_high_ticks,
                    event.trigger_low_ticks,
                    event.trigger_close_ticks,
                    event.atr_sum_ticks,
                    ATR_PERIOD,
                    event.frozen,
                )
            )
        result = tuple(output)
        self._delayed[key] = result
        return result

    def context_matches(
        self,
        candidate: BaseEventCandidate,
        context: ContextSpec,
        anchor: AnchorRecord,
    ) -> bool:
        if context.kind == "ANY":
            return True
        execution_matches = self._five_minute_end_index.get(
            (anchor.anchor_ns, anchor.contract, anchor.outcome_span_id), ()
        )
        execution = [
            self.series_by_timeframe[FIVE_MINUTES].bars[index]
            for index in execution_matches
            if self.series_by_timeframe[FIVE_MINUTES].bars[index].bar.segment_id
            == anchor.segment_id
        ]
        if len(execution) != 1:
            return False
        context_index = self._context_index(
            context.timeframe_seconds,
            execution[0],
            anchor.anchor_ns,
        )
        if context_index is None:
            return False
        return self._context_state(candidate, context, context_index)

    def _context_state(
        self,
        candidate: BaseEventCandidate,
        context: ContextSpec,
        context_index: int,
    ) -> bool:
        """Evaluate one already lineage-checked, completed context bar."""

        series = self.series_by_timeframe[context.timeframe_seconds]
        sign = _direction_sign(candidate.direction)
        if context.kind == "EMA_RELATION":
            fast_values = self.features.ema(context.timeframe_seconds, context.fast_period)
            slow_values = self.features.ema(context.timeframe_seconds, context.slow_period)
            if context_index == 0:
                return False
            fast = fast_values[context_index]
            slow = slow_values[context_index]
            prior_fast = fast_values[context_index - 1]
            return (
                fast is not None
                and slow is not None
                and prior_fast is not None
                and sign * context.relation * (fast - slow) > 0
                and sign * context.relation * (fast - prior_fast) > 0
            )
        if context.kind in {"EFFICIENCY_RANGE", "EFFICIENCY_TREND"}:
            bounds = series.window(context_index, 25)
            if bounds is None:
                return False
            start, stop = bounds
            closes = [item.bar.close_ticks for item in series.bars[start:stop]]
            displacement = abs(closes[-1] - closes[0])
            travel = sum(abs(right - left) for left, right in pairwise(closes))
            if context.kind == "EFFICIENCY_RANGE":
                return travel > 0 and 3 * displacement <= travel
            return travel > 0 and 3 * displacement >= 2 * travel
        if context.kind in {"VOLATILITY_EXPANDING", "VOLATILITY_CONTRACTING"}:
            values = self.features.true_range(context.timeframe_seconds)
            short_sum = _rolling_sum(values, series.window(context_index, 6))
            long_sum = _rolling_sum(values, series.window(context_index, 24))
            if short_sum is None or long_sum is None:
                return False
            if context.kind == "VOLATILITY_EXPANDING":
                return short_sum * 24 * 2 >= long_sum * 6 * 3
            return short_sum * 24 * 3 <= long_sum * 6 * 2
        raise SymbolicEngineError("unknown context kind")

    @staticmethod
    def time_filter_matches(time_filter: TimeFilterSpec, anchor_ns: int) -> bool:
        if time_filter.kind == "ALL":
            return True
        instant = datetime.fromtimestamp(anchor_ns // ONE_SECOND_NS, tz=UTC)
        if time_filter.kind == "UTC_FOUR_HOUR":
            return instant.hour // 4 * 4 == time_filter.value
        if time_filter.kind == "UTC_WEEKDAY":
            return instant.weekday() == time_filter.value
        raise SymbolicEngineError("unknown time-filter kind")

    def policy_mask(self, policy: AnchorPolicy) -> PolicyMask:
        candidate = _candidate_lookup().get(policy.base_candidate_id)
        context = _context_lookup().get(policy.context_id)
        time_filter = _time_filter_lookup().get(policy.time_filter_id)
        delay = _delay_lookup().get(policy.delay_id)
        if candidate is None or context is None or time_filter is None or delay is None:
            raise SymbolicEngineError("anchor policy refers to an unknown catalog member")
        records = tuple(
            anchor
            for anchor in self.delayed_anchors(candidate, delay)
            if self.context_matches(candidate, context, anchor)
            and self.time_filter_matches(time_filter, anchor.anchor_ns)
        )
        return PolicyMask.from_records(policy, candidate.family, candidate.direction, records)


def build_symbolic_stage(
    bars_by_timeframe: Mapping[int, Sequence[BarWithOutcomeSpan]],
    decision_dates: Iterable[date],
) -> SymbolicStage:
    """Build a stage-local feature view without opening any outcome payload."""

    dates = _validated_dates(decision_dates)
    if set(bars_by_timeframe) != set(SUPPORTED_SIGNAL_TIMEFRAMES):
        raise SymbolicEngineError("bars_by_timeframe must have exact 5m/30m/1h keys")
    series = {
        timeframe: _validated_series(
            bars_by_timeframe[timeframe],
            timeframe_seconds=timeframe,
            decision_dates=dates,
        )
        for timeframe in SUPPORTED_SIGNAL_TIMEFRAMES
    }
    return SymbolicStage(series, dates)


@lru_cache(maxsize=1)
def _candidate_lookup() -> Mapping[str, BaseEventCandidate]:
    return {item.candidate_id: item for item in build_base_event_catalog().candidates}


@lru_cache(maxsize=1)
def _context_lookup() -> Mapping[str, ContextSpec]:
    return {item.context_id: item for item in build_context_catalog()}


@lru_cache(maxsize=1)
def _time_filter_lookup() -> Mapping[str, TimeFilterSpec]:
    return {item.time_filter_id: item for item in build_time_filter_catalog()}


@lru_cache(maxsize=1)
def _delay_lookup() -> Mapping[str, DelaySpec]:
    return {item.delay_id: item for item in build_delay_catalog()}


@lru_cache(maxsize=1)
def _entry_lookup() -> Mapping[str, EntryPolicy]:
    return {item.entry_id: item for item in build_entry_catalog().candidates}


@lru_cache(maxsize=1)
def _exit_lookup() -> Mapping[str, ExitPolicy]:
    return {item.exit_id: item for item in build_exit_catalog().candidates}


@dataclass(frozen=True, slots=True)
class PolicyMask:
    policy: AnchorPolicy
    family: Family
    direction: Direction
    records: tuple[AnchorRecord, ...]
    mask_sha256: str
    support_day_count: int

    @classmethod
    def from_records(
        cls,
        policy: AnchorPolicy,
        family: Family,
        direction: Direction,
        records: Sequence[AnchorRecord],
    ) -> PolicyMask:
        canonical = tuple(sorted(set(records)))
        payload = [item.as_dict() for item in canonical]
        return cls(
            policy,
            family,
            direction,
            canonical,
            canonical_sha256(payload),
            len({item.source_date for item in canonical}),
        )

    @property
    def support_count(self) -> int:
        return len(self.records)

    def as_dict(self) -> dict[str, object]:
        return {
            "direction": self.direction,
            "family": self.family,
            "mask_sha256": self.mask_sha256,
            "policy": self.policy.as_dict(),
            "records": [item.as_dict() for item in self.records],
            "schema": MASK_SCHEMA,
            "support_count": self.support_count,
            "support_day_count": self.support_day_count,
        }


StructuralAnchorKey = tuple[str, int, int, int]


def _structural_anchor_key(record: AnchorRecord) -> StructuralAnchorKey:
    return (
        record.contract,
        record.outcome_span_id,
        record.segment_id,
        record.anchor_ns,
    )


@dataclass(frozen=True, slots=True)
class StructuralEligibilityLattice:
    """Feature-only 7h complete-case anchor set, frozen before any 1s row."""

    eligible_anchor_keys: tuple[StructuralAnchorKey, ...]
    maximum_path_seconds: int
    allowed_tail_end_ns: int
    artifact_sha256: str

    def __post_init__(self) -> None:
        if (
            not self.eligible_anchor_keys
            or self.eligible_anchor_keys != tuple(sorted(self.eligible_anchor_keys))
            or len(set(self.eligible_anchor_keys)) != len(self.eligible_anchor_keys)
        ):
            raise SymbolicEngineError("structural eligibility keys are non-canonical")
        if self.maximum_path_seconds != 25_200 or self.allowed_tail_end_ns <= 0:
            raise SymbolicEngineError("structural eligibility window differs")
        _require_sha(self.artifact_sha256, label="structural eligibility artifact_sha256")
        if canonical_sha256(self.definition_dict()) != self.artifact_sha256:
            raise SymbolicEngineError("structural eligibility commitment differs")

    @property
    def key_set(self) -> frozenset[StructuralAnchorKey]:
        return frozenset(self.eligible_anchor_keys)

    def definition_dict(self) -> dict[str, object]:
        return {
            "allowed_tail_end_ns": self.allowed_tail_end_ns,
            "eligible_anchor_keys": [list(item) for item in self.eligible_anchor_keys],
            "maximum_path_seconds": self.maximum_path_seconds,
            "schema": STRUCTURAL_ELIGIBILITY_SCHEMA,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def build_structural_eligibility_lattice(
    five_minute_bars: Sequence[BarWithOutcomeSpan],
    *,
    decision_dates: Iterable[date],
    allowed_tail_end_ns: int,
) -> StructuralEligibilityLattice:
    """Freeze anchors with one exact, gap-free same-lineage 7h future 5m path."""

    dates = _validated_dates(decision_dates)
    allowed_dates = frozenset(dates)
    values = tuple(five_minute_bars)
    if not values:
        raise SymbolicEngineError("structural eligibility requires 5m bars")
    prior: tuple[int, str, int, int] | None = None
    for wrapped in values:
        identity = (
            wrapped.bar.start_ns,
            wrapped.bar.contract,
            wrapped.outcome_span_id,
            wrapped.bar.segment_id,
        )
        if wrapped.bar.timeframe_seconds != FIVE_MINUTES or (
            prior is not None and identity <= prior
        ):
            raise SymbolicEngineError("structural eligibility 5m bars are non-canonical")
        prior = identity
    _require_int(allowed_tail_end_ns, label="allowed_tail_end_ns", minimum=1)
    future_bar_count = 25_200 // FIVE_MINUTES
    keys: list[StructuralAnchorKey] = []
    for index, wrapped in enumerate(values):
        bar = wrapped.bar
        terminal_index = index + future_bar_count
        if (
            bar.source_date not in allowed_dates
            or terminal_index >= len(values)
            or bar.end_ns + 25_200 * ONE_SECOND_NS > allowed_tail_end_ns
        ):
            continue
        lineage = _lineage(wrapped)
        expected_start = bar.end_ns
        for future in values[index + 1 : terminal_index + 1]:
            if future.bar.start_ns != expected_start or _lineage(future) != lineage:
                break
            expected_start = future.bar.end_ns
        else:
            if expected_start == bar.end_ns + 25_200 * ONE_SECOND_NS:
                keys.append((bar.contract, wrapped.outcome_span_id, bar.segment_id, bar.end_ns))
    canonical = tuple(sorted(keys))
    definition = {
        "allowed_tail_end_ns": allowed_tail_end_ns,
        "eligible_anchor_keys": [list(item) for item in canonical],
        "maximum_path_seconds": 25_200,
        "schema": STRUCTURAL_ELIGIBILITY_SCHEMA,
    }
    return StructuralEligibilityLattice(
        canonical,
        25_200,
        allowed_tail_end_ns,
        canonical_sha256(definition),
    )


@dataclass(frozen=True, slots=True)
class DirectOpportunity:
    """Feature-only direct-model entry scheduled from the next native 5m bar."""

    decision_source_date: date
    contract: str
    outcome_span_id: int
    segment_id: int
    decision_ns: int
    scheduled_entry_ns: int
    scheduled_entry_ticks: int

    def __post_init__(self) -> None:
        if not isinstance(self.decision_source_date, date):
            raise SymbolicEngineError("direct opportunity source date is invalid")
        if not self.contract:
            raise SymbolicEngineError("direct opportunity contract is empty")
        _require_int(self.outcome_span_id, label="direct outcome_span_id", minimum=1)
        _require_int(self.segment_id, label="direct segment_id", minimum=1)
        _require_int(self.decision_ns, label="direct decision_ns", minimum=0)
        _require_int(self.scheduled_entry_ns, label="direct scheduled_entry_ns", minimum=0)
        _require_int(
            self.scheduled_entry_ticks,
            label="direct scheduled_entry_ticks",
            minimum=1,
        )
        if (
            self.decision_ns % (FIVE_MINUTES * ONE_SECOND_NS)
            or self.scheduled_entry_ns % ONE_SECOND_NS
            or not self.decision_ns
            <= self.scheduled_entry_ns
            < self.decision_ns + FIVE_MINUTES * ONE_SECOND_NS
        ):
            raise SymbolicEngineError("direct scheduled entry is outside the exact next 5m bar")

    @property
    def structural_anchor_key(self) -> StructuralAnchorKey:
        return self.contract, self.outcome_span_id, self.segment_id, self.decision_ns

    def as_dict(self) -> dict[str, object]:
        return {
            "contract": self.contract,
            "decision_ns": self.decision_ns,
            "decision_source_date": self.decision_source_date.isoformat(),
            "outcome_span_id": self.outcome_span_id,
            "scheduled_entry_ns": self.scheduled_entry_ns,
            "scheduled_entry_ticks": self.scheduled_entry_ticks,
            "segment_id": self.segment_id,
        }


@dataclass(frozen=True, slots=True)
class DirectOpportunityLattice:
    """Committed decision-to-entry rows derived before any 1s outcome access."""

    structural_lattice_sha256: str
    structural_anchor_count: int
    structural_anchor_set_sha256: str
    opportunities: tuple[DirectOpportunity, ...]
    excluded_anchor_keys: tuple[StructuralAnchorKey, ...]
    artifact_sha256: str

    def __post_init__(self) -> None:
        _require_sha(self.structural_lattice_sha256, label="direct structural lattice SHA")
        _require_int(self.structural_anchor_count, label="direct structural count", minimum=1)
        _require_sha(self.structural_anchor_set_sha256, label="direct structural set SHA")
        opportunity_keys = tuple(item.structural_anchor_key for item in self.opportunities)
        if opportunity_keys != tuple(sorted(set(opportunity_keys))):
            raise SymbolicEngineError("direct opportunities are non-canonical")
        if self.excluded_anchor_keys != tuple(sorted(set(self.excluded_anchor_keys))):
            raise SymbolicEngineError("direct exclusions are non-canonical")
        combined = tuple(sorted((*opportunity_keys, *self.excluded_anchor_keys)))
        if (
            set(opportunity_keys) & set(self.excluded_anchor_keys)
            or len(combined) != self.structural_anchor_count
            or canonical_sha256([list(item) for item in combined])
            != self.structural_anchor_set_sha256
        ):
            raise SymbolicEngineError("direct opportunity partition differs")
        _require_sha(self.artifact_sha256, label="direct opportunity artifact_sha256")
        if canonical_sha256(self.definition_dict()) != self.artifact_sha256:
            raise SymbolicEngineError("direct opportunity commitment differs")

    @property
    def opportunity_count(self) -> int:
        return len(self.opportunities)

    def definition_dict(self) -> dict[str, object]:
        return {
            "excluded_anchor_keys": [list(item) for item in self.excluded_anchor_keys],
            "opportunities": [item.as_dict() for item in self.opportunities],
            "schema": DIRECT_OPPORTUNITY_SCHEMA,
            "structural_anchor_count": self.structural_anchor_count,
            "structural_anchor_set_sha256": self.structural_anchor_set_sha256,
            "structural_lattice_sha256": self.structural_lattice_sha256,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def build_direct_opportunity_lattice(
    five_minute_bars: Sequence[BarWithOutcomeSpan],
    structural_lattice: StructuralEligibilityLattice,
) -> DirectOpportunityLattice:
    """Freeze exact next-5m first-trade/open entries for direct model rows."""

    values = tuple(five_minute_bars)
    if not values:
        raise SymbolicEngineError("direct opportunity construction requires 5m bars")
    current_by_end: dict[StructuralAnchorKey, BarWithOutcomeSpan] = {}
    next_by_start: dict[StructuralAnchorKey, BarWithOutcomeSpan] = {}
    prior: tuple[int, str, int, int] | None = None
    for wrapped in values:
        bar = wrapped.bar
        identity = (bar.start_ns, bar.contract, wrapped.outcome_span_id, bar.segment_id)
        if bar.timeframe_seconds != FIVE_MINUTES or (prior is not None and identity <= prior):
            raise SymbolicEngineError("direct opportunity 5m bars are non-canonical")
        prior = identity
        lineage_start = (bar.contract, wrapped.outcome_span_id, bar.segment_id, bar.start_ns)
        lineage_end = (bar.contract, wrapped.outcome_span_id, bar.segment_id, bar.end_ns)
        if lineage_start in next_by_start or lineage_end in current_by_end:
            raise SymbolicEngineError("direct opportunity 5m lineage keys are duplicated")
        next_by_start[lineage_start] = wrapped
        current_by_end[lineage_end] = wrapped

    opportunities: list[DirectOpportunity] = []
    excluded: list[StructuralAnchorKey] = []
    for key in structural_lattice.eligible_anchor_keys:
        current = current_by_end.get(key)
        following = next_by_start.get(key)
        if current is None or following is None or _lineage(current) != _lineage(following):
            excluded.append(key)
            continue
        opportunities.append(
            DirectOpportunity(
                current.bar.source_date,
                key[0],
                key[1],
                key[2],
                key[3],
                following.bar.first_trade_ns // ONE_SECOND_NS * ONE_SECOND_NS,
                following.bar.open_ticks,
            )
        )
    opportunity_rows = tuple(opportunities)
    excluded_rows = tuple(excluded)
    structural_set_sha = canonical_sha256(
        [list(item) for item in structural_lattice.eligible_anchor_keys]
    )
    definition = {
        "excluded_anchor_keys": [list(item) for item in excluded_rows],
        "opportunities": [item.as_dict() for item in opportunity_rows],
        "schema": DIRECT_OPPORTUNITY_SCHEMA,
        "structural_anchor_count": len(structural_lattice.eligible_anchor_keys),
        "structural_anchor_set_sha256": structural_set_sha,
        "structural_lattice_sha256": structural_lattice.artifact_sha256,
    }
    return DirectOpportunityLattice(
        structural_lattice.artifact_sha256,
        len(structural_lattice.eligible_anchor_keys),
        structural_set_sha,
        opportunity_rows,
        excluded_rows,
        canonical_sha256(definition),
    )


@dataclass(frozen=True, slots=True)
class StructurallyEligiblePolicyMask:
    """Raw feature mask plus its pre-outcome complete-case evaluation mask."""

    raw_mask: PolicyMask
    evaluable_mask: PolicyMask
    excluded_anchor_keys: tuple[tuple[str, int, int, int, Direction], ...]
    lattice_sha256: str
    commitment_sha256: str

    def __post_init__(self) -> None:
        if (
            self.raw_mask.policy.policy_id != self.evaluable_mask.policy.policy_id
            or self.raw_mask.family != self.evaluable_mask.family
            or self.raw_mask.direction != self.evaluable_mask.direction
        ):
            raise SymbolicEngineError("structural masks refer to different policies")
        raw_keys = {item.outcome_key for item in self.raw_mask.records}
        evaluable_keys = {item.outcome_key for item in self.evaluable_mask.records}
        if not evaluable_keys <= raw_keys or self.excluded_anchor_keys != tuple(
            sorted(raw_keys - evaluable_keys)
        ):
            raise SymbolicEngineError("structural mask partition differs")
        _require_sha(self.lattice_sha256, label="structural lattice SHA")
        _require_sha(self.commitment_sha256, label="structural mask commitment SHA")
        if canonical_sha256(self.definition_dict()) != self.commitment_sha256:
            raise SymbolicEngineError("structural mask commitment differs")

    @property
    def raw_support_count(self) -> int:
        return self.raw_mask.support_count

    @property
    def evaluable_support_count(self) -> int:
        return self.evaluable_mask.support_count

    def definition_dict(self) -> dict[str, object]:
        return {
            "evaluable_mask": self.evaluable_mask.as_dict(),
            "excluded_anchor_keys": [list(item) for item in self.excluded_anchor_keys],
            "lattice_sha256": self.lattice_sha256,
            "raw_mask": self.raw_mask.as_dict(),
            "schema": STRUCTURAL_ELIGIBILITY_SCHEMA,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "commitment_sha256": self.commitment_sha256}


def freeze_structurally_eligible_policy_mask(
    raw_mask: PolicyMask,
    lattice: StructuralEligibilityLattice,
) -> StructurallyEligiblePolicyMask:
    """Apply the frozen 5m complete-case set without inspecting any 1s outcome."""

    eligible = lattice.key_set
    records = tuple(item for item in raw_mask.records if _structural_anchor_key(item) in eligible)
    evaluable = PolicyMask.from_records(
        raw_mask.policy,
        raw_mask.family,
        raw_mask.direction,
        records,
    )
    evaluable_keys = {item.outcome_key for item in records}
    excluded = tuple(
        sorted(
            item.outcome_key for item in raw_mask.records if item.outcome_key not in evaluable_keys
        )
    )
    definition = {
        "evaluable_mask": evaluable.as_dict(),
        "excluded_anchor_keys": [list(item) for item in excluded],
        "lattice_sha256": lattice.artifact_sha256,
        "raw_mask": raw_mask.as_dict(),
        "schema": STRUCTURAL_ELIGIBILITY_SCHEMA,
    }
    return StructurallyEligiblePolicyMask(
        raw_mask,
        evaluable,
        excluded,
        lattice.artifact_sha256,
        canonical_sha256(definition),
    )


@dataclass(frozen=True, slots=True)
class MaskAlias:
    alias_policy_id: str
    representative_policy_id: str
    mask_sha256: str

    def as_dict(self) -> dict[str, object]:
        return {
            "alias_policy_id": self.alias_policy_id,
            "mask_sha256": self.mask_sha256,
            "representative_policy_id": self.representative_policy_id,
        }


@dataclass(frozen=True, slots=True)
class MaskDeduplication:
    representatives: tuple[PolicyMask, ...]
    aliases: tuple[MaskAlias, ...]
    artifact_sha256: str

    def as_dict(self) -> dict[str, object]:
        return {
            "aliases": [item.as_dict() for item in self.aliases],
            "artifact_sha256": self.artifact_sha256,
            "representative_policy_ids": [item.policy.policy_id for item in self.representatives],
            "schema": MASK_SCHEMA,
        }


def deduplicate_feature_masks(masks: Iterable[PolicyMask]) -> MaskDeduplication:
    """Collapse identical feature-only masks using the earliest semantic rank."""

    ordered = tuple(sorted(masks, key=lambda item: item.policy.policy_rank))
    representatives: list[PolicyMask] = []
    aliases: list[MaskAlias] = []
    seen: dict[str, PolicyMask] = {}
    for mask in ordered:
        prior = seen.get(mask.mask_sha256)
        if prior is None:
            seen[mask.mask_sha256] = mask
            representatives.append(mask)
        else:
            aliases.append(
                MaskAlias(mask.policy.policy_id, prior.policy.policy_id, mask.mask_sha256)
            )
    payload = {
        "aliases": [item.as_dict() for item in aliases],
        "representative_policy_ids": [item.policy.policy_id for item in representatives],
        "schema": MASK_SCHEMA,
    }
    return MaskDeduplication(tuple(representatives), tuple(aliases), canonical_sha256(payload))


@dataclass(frozen=True, slots=True)
class EncodedSparseMask:
    policy_id: str
    mask_sha256: str
    bits_hex: str
    support_count: int
    support_day_count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "bits_hex": self.bits_hex,
            "mask_sha256": self.mask_sha256,
            "policy_id": self.policy_id,
            "support_count": self.support_count,
            "support_day_count": self.support_day_count,
        }


@dataclass(frozen=True, slots=True)
class AnchorMaskBatch:
    first_policy_rank: int
    last_policy_rank: int
    universe: tuple[AnchorRecord, ...]
    universe_sha256: str
    masks: tuple[EncodedSparseMask, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "first_policy_rank": self.first_policy_rank,
            "last_policy_rank": self.last_policy_rank,
            "masks": [item.as_dict() for item in self.masks],
            "schema": MASK_SCHEMA,
            "universe_count": len(self.universe),
            "universe_sha256": self.universe_sha256,
        }


def _encode_mask_batch(masks: Sequence[PolicyMask]) -> AnchorMaskBatch:
    if not masks:
        raise SymbolicEngineError("cannot encode an empty mask batch")
    universe = tuple(sorted({record for mask in masks for record in mask.records}))
    positions = {record: index for index, record in enumerate(universe)}
    encoded: list[EncodedSparseMask] = []
    for mask in masks:
        bits = 0
        for record in mask.records:
            bits |= 1 << positions[record]
        encoded.append(
            EncodedSparseMask(
                mask.policy.policy_id,
                mask.mask_sha256,
                format(bits, "x"),
                mask.support_count,
                mask.support_day_count,
            )
        )
    return AnchorMaskBatch(
        masks[0].policy.policy_rank,
        masks[-1].policy.policy_rank,
        universe,
        canonical_sha256([item.as_dict() for item in universe]),
        tuple(encoded),
    )


def iter_anchor_mask_batches(
    stage: SymbolicStage,
    *,
    batch_size: int = 64,
    policies: Iterable[AnchorPolicy] | None = None,
) -> Iterator[AnchorMaskBatch]:
    """Stream shared-universe sparse bitsets in semantic policy order."""

    if not isinstance(stage, SymbolicStage):
        raise SymbolicEngineError("mask batches require a SymbolicStage")
    _require_int(batch_size, label="batch_size", minimum=1)
    if batch_size > 256:
        raise SymbolicEngineError("batch_size must be <= 256")
    pending: list[PolicyMask] = []
    prior_rank = 0
    for policy in iter_anchor_policies() if policies is None else policies:
        if policy.policy_rank <= prior_rank:
            raise SymbolicEngineError("policies must use increasing semantic rank")
        prior_rank = policy.policy_rank
        pending.append(stage.policy_mask(policy))
        if len(pending) == batch_size:
            yield _encode_mask_batch(pending)
            pending.clear()
    if pending:
        yield _encode_mask_batch(pending)


STAGE_A_OUTER_CHUNK_COUNT: Final = 64
STAGE_A_POLICY_ROWS_PER_CHUNK_MAXIMUM: Final = (
    LOGICAL_ANCHOR_POLICY_COUNT + STAGE_A_OUTER_CHUNK_COUNT - 1
) // STAGE_A_OUTER_CHUNK_COUNT


@dataclass(frozen=True, slots=True)
class StageAChunkSpec:
    chunk_index: int
    first_policy_rank: int
    last_policy_rank: int
    policy_count: int
    chunk_id: str

    def __post_init__(self) -> None:
        _require_int(self.chunk_index, label="Stage-A chunk_index", minimum=0)
        if self.chunk_index >= STAGE_A_OUTER_CHUNK_COUNT:
            raise SymbolicEngineError("Stage-A chunk index exceeds the frozen plan")
        _require_int(self.first_policy_rank, label="first_policy_rank", minimum=1)
        _require_int(self.last_policy_rank, label="last_policy_rank", minimum=1)
        if (
            self.last_policy_rank < self.first_policy_rank
            or self.policy_count != self.last_policy_rank - self.first_policy_rank + 1
            or self.policy_count > STAGE_A_POLICY_ROWS_PER_CHUNK_MAXIMUM
        ):
            raise SymbolicEngineError("Stage-A chunk bounds differ")
        _require_sha(self.chunk_id, label="Stage-A chunk_id")
        if canonical_sha256(self.definition_dict()) != self.chunk_id:
            raise SymbolicEngineError("Stage-A chunk id differs")

    def definition_dict(self) -> dict[str, object]:
        return {
            "chunk_index": self.chunk_index,
            "first_policy_rank": self.first_policy_rank,
            "last_policy_rank": self.last_policy_rank,
            "policy_count": self.policy_count,
            "schema": STAGE_A_SCORE_SCHEMA,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "chunk_id": self.chunk_id}


@lru_cache(maxsize=1)
def build_stage_a_chunk_plan() -> tuple[StageAChunkSpec, ...]:
    rows = []
    for index in range(STAGE_A_OUTER_CHUNK_COUNT):
        first = index * STAGE_A_POLICY_ROWS_PER_CHUNK_MAXIMUM + 1
        last = min(
            LOGICAL_ANCHOR_POLICY_COUNT,
            first + STAGE_A_POLICY_ROWS_PER_CHUNK_MAXIMUM - 1,
        )
        definition = {
            "chunk_index": index,
            "first_policy_rank": first,
            "last_policy_rank": last,
            "policy_count": last - first + 1,
            "schema": STAGE_A_SCORE_SCHEMA,
        }
        rows.append(
            StageAChunkSpec(
                index,
                first,
                last,
                last - first + 1,
                canonical_sha256(definition),
            )
        )
    if (
        len(rows) != STAGE_A_OUTER_CHUNK_COUNT
        or rows[0].first_policy_rank != 1
        or rows[-1].last_policy_rank != LOGICAL_ANCHOR_POLICY_COUNT
        or any(
            left.last_policy_rank + 1 != right.first_policy_rank for left, right in pairwise(rows)
        )
    ):
        raise SymbolicEngineError("Stage-A chunk plan does not exactly partition policies")
    return tuple(rows)


@dataclass(frozen=True, slots=True)
class CubePolicyMask:
    policy: AnchorPolicy
    bits: int
    mask_sha256: str
    support_count: int
    support_day_count: int
    evaluable_bits: int
    evaluable_mask_sha256: str
    evaluable_support_count: int
    evaluable_support_day_count: int


class CandidatePolicyCube:
    """One candidate's 1,092 masks as shared delay/context/time integer bitsets."""

    def __init__(
        self,
        stage: SymbolicStage,
        candidate: BaseEventCandidate,
        structural_lattice: StructuralEligibilityLattice | None = None,
    ) -> None:
        self.stage = stage
        self.candidate = candidate
        delayed = {
            delay.delay_id: stage.delayed_anchors(candidate, delay)
            for delay in build_delay_catalog()
        }
        self.universe = tuple(
            sorted({record for records in delayed.values() for record in records})
        )
        positions = {record: index for index, record in enumerate(self.universe)}
        self.delay_bits = {
            delay_id: sum(1 << positions[record] for record in records)
            for delay_id, records in delayed.items()
        }
        self.context_bits = {
            context.context_id: sum(
                1 << index
                for index, record in enumerate(self.universe)
                if stage.context_matches(candidate, context, record)
            )
            for context in build_context_catalog()
        }
        self.time_bits = {
            time_filter.time_filter_id: sum(
                1 << index
                for index, record in enumerate(self.universe)
                if stage.time_filter_matches(time_filter, record.anchor_ns)
            )
            for time_filter in build_time_filter_catalog()
        }
        self.structural_lattice_sha256 = (
            None if structural_lattice is None else structural_lattice.artifact_sha256
        )
        if structural_lattice is None:
            self.structurally_eligible_bits = (1 << len(self.universe)) - 1
        else:
            eligible = structural_lattice.key_set
            self.structurally_eligible_bits = sum(
                1 << index
                for index, record in enumerate(self.universe)
                if _structural_anchor_key(record) in eligible
            )
        self._behavior: dict[int, tuple[str, int, int]] = {}

    def records_for_bits(self, bits: int) -> tuple[AnchorRecord, ...]:
        output: list[AnchorRecord] = []
        remaining = bits
        while remaining:
            lowest = remaining & -remaining
            index = lowest.bit_length() - 1
            output.append(self.universe[index])
            remaining ^= lowest
        return tuple(output)

    def behavior(self, bits: int) -> tuple[str, int, int]:
        cached = self._behavior.get(bits)
        if cached is not None:
            return cached
        records = self.records_for_bits(bits)
        result = (
            canonical_sha256([item.as_dict() for item in records]),
            len(records),
            len({item.source_date for item in records}),
        )
        self._behavior[bits] = result
        return result

    def iter_masks(
        self,
        *,
        first_policy_rank: int | None = None,
        last_policy_rank: int | None = None,
    ) -> Iterator[CubePolicyMask]:
        first = (
            self.candidate.selection_rank - 1
        ) * CONTEXT_COUNT * TIME_FILTER_COUNT * DELAY_COUNT + 1
        last = first + CONTEXT_COUNT * TIME_FILTER_COUNT * DELAY_COUNT - 1
        selected_first = first if first_policy_rank is None else max(first, first_policy_rank)
        selected_last = last if last_policy_rank is None else min(last, last_policy_rank)
        if selected_first > selected_last:
            return
        for context, time_filter, delay in product(
            build_context_catalog(),
            build_time_filter_catalog(),
            build_delay_catalog(),
        ):
            rank = (
                first
                + (
                    (context.selection_rank - 1) * TIME_FILTER_COUNT
                    + time_filter.selection_rank
                    - 1
                )
                * DELAY_COUNT
                + delay.selection_rank
                - 1
            )
            if rank < selected_first or rank > selected_last:
                continue
            definition = {
                "base_candidate_id": self.candidate.candidate_id,
                "context_id": context.context_id,
                "delay_id": delay.delay_id,
                "schema": POLICY_SCHEMA,
                "time_filter_id": time_filter.time_filter_id,
            }
            policy = AnchorPolicy(
                rank,
                canonical_sha256(definition),
                self.candidate.candidate_id,
                context.context_id,
                time_filter.time_filter_id,
                delay.delay_id,
            )
            bits = (
                self.delay_bits[delay.delay_id]
                & self.context_bits[context.context_id]
                & self.time_bits[time_filter.time_filter_id]
            )
            mask_sha, support, days = self.behavior(bits)
            evaluable_bits = bits & self.structurally_eligible_bits
            evaluable_sha, evaluable_support, evaluable_days = self.behavior(evaluable_bits)
            yield CubePolicyMask(
                policy,
                bits,
                mask_sha,
                support,
                days,
                evaluable_bits,
                evaluable_sha,
                evaluable_support,
                evaluable_days,
            )


def build_candidate_policy_cube(
    stage: SymbolicStage,
    candidate: BaseEventCandidate,
    structural_lattice: StructuralEligibilityLattice | None = None,
) -> CandidatePolicyCube:
    if not isinstance(stage, SymbolicStage):
        raise SymbolicEngineError("candidate policy cube requires a SymbolicStage")
    if candidate.candidate_id not in _candidate_lookup():
        raise SymbolicEngineError("candidate policy cube requires a catalog candidate")
    if structural_lattice is not None and not isinstance(
        structural_lattice, StructuralEligibilityLattice
    ):
        raise SymbolicEngineError("candidate cube structural lattice is invalid")
    return CandidatePolicyCube(stage, candidate, structural_lattice)


@dataclass(frozen=True, slots=True)
class CandidateCubeCommitment:
    candidate_id: str
    candidate_rank: int
    universe_count: int
    universe_sha256: str
    delay_cube_sha256: str
    context_cube_sha256: str
    time_cube_sha256: str
    structural_lattice_sha256: str | None
    structurally_eligible_universe_count: int
    structurally_eligible_universe_sha256: str
    cube_sha256: str

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "candidate_rank": self.candidate_rank,
            "context_cube_sha256": self.context_cube_sha256,
            "cube_sha256": self.cube_sha256,
            "delay_cube_sha256": self.delay_cube_sha256,
            "structural_lattice_sha256": self.structural_lattice_sha256,
            "structurally_eligible_universe_count": self.structurally_eligible_universe_count,
            "structurally_eligible_universe_sha256": self.structurally_eligible_universe_sha256,
            "time_cube_sha256": self.time_cube_sha256,
            "universe_count": self.universe_count,
            "universe_sha256": self.universe_sha256,
        }


@dataclass(frozen=True, slots=True)
class FeaturePolicyCommitment:
    policy_rank: int
    policy_id: str
    candidate_id: str
    mask_sha256: str
    support_count: int
    support_day_count: int
    evaluable_mask_sha256: str
    evaluable_support_count: int
    evaluable_support_day_count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "evaluable_mask_sha256": self.evaluable_mask_sha256,
            "evaluable_support_count": self.evaluable_support_count,
            "evaluable_support_day_count": self.evaluable_support_day_count,
            "mask_sha256": self.mask_sha256,
            "policy_id": self.policy_id,
            "policy_rank": self.policy_rank,
            "support_count": self.support_count,
            "support_day_count": self.support_day_count,
        }


@dataclass(frozen=True, slots=True)
class FeatureUniverseCommitmentChunk:
    chunk: StageAChunkSpec
    candidate_cubes: tuple[CandidateCubeCommitment, ...]
    policies: tuple[FeaturePolicyCommitment, ...]
    artifact_sha256: str

    def __post_init__(self) -> None:
        if len(self.policies) != self.chunk.policy_count:
            raise SymbolicEngineError("feature-universe chunk policy count differs")
        ranks = tuple(item.policy_rank for item in self.policies)
        if ranks != tuple(range(self.chunk.first_policy_rank, self.chunk.last_policy_rank + 1)):
            raise SymbolicEngineError("feature-universe chunk ranks differ")
        _require_sha(self.artifact_sha256, label="feature-universe chunk artifact_sha256")
        if canonical_sha256(self.definition_dict()) != self.artifact_sha256:
            raise SymbolicEngineError("feature-universe chunk hash differs")

    def definition_dict(self) -> dict[str, object]:
        return {
            "candidate_cubes": [item.as_dict() for item in self.candidate_cubes],
            "chunk": self.chunk.as_dict(),
            "policies": [item.as_dict() for item in self.policies],
            "schema": MASK_SCHEMA,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def _candidate_cube_commitment(cube: CandidatePolicyCube) -> CandidateCubeCommitment:
    universe_sha = canonical_sha256([item.as_dict() for item in cube.universe])
    delay_sha = canonical_sha256(
        [
            {"axis_id": key, "bits_hex": format(value, "x")}
            for key, value in sorted(cube.delay_bits.items())
        ]
    )
    context_sha = canonical_sha256(
        [
            {"axis_id": key, "bits_hex": format(value, "x")}
            for key, value in sorted(cube.context_bits.items())
        ]
    )
    time_sha = canonical_sha256(
        [
            {"axis_id": key, "bits_hex": format(value, "x")}
            for key, value in sorted(cube.time_bits.items())
        ]
    )
    eligible_records = cube.records_for_bits(cube.structurally_eligible_bits)
    eligible_sha = canonical_sha256([item.as_dict() for item in eligible_records])
    definition = {
        "candidate_id": cube.candidate.candidate_id,
        "candidate_rank": cube.candidate.selection_rank,
        "context_cube_sha256": context_sha,
        "delay_cube_sha256": delay_sha,
        "structural_lattice_sha256": cube.structural_lattice_sha256,
        "structurally_eligible_universe_count": len(eligible_records),
        "structurally_eligible_universe_sha256": eligible_sha,
        "time_cube_sha256": time_sha,
        "universe_count": len(cube.universe),
        "universe_sha256": universe_sha,
    }
    return CandidateCubeCommitment(
        cube.candidate.candidate_id,
        cube.candidate.selection_rank,
        len(cube.universe),
        universe_sha,
        delay_sha,
        context_sha,
        time_sha,
        cube.structural_lattice_sha256,
        len(eligible_records),
        eligible_sha,
        canonical_sha256(definition),
    )


def iter_feature_universe_commitment_chunks(
    stage: SymbolicStage,
    *,
    chunk_indices: Iterable[int] | None = None,
    structural_lattice: StructuralEligibilityLattice | None = None,
) -> Iterator[FeatureUniverseCommitmentChunk]:
    """Freeze raw 64-way cube evidence before any reference/outcome surface is supplied."""

    plan = build_stage_a_chunk_plan()
    indices = tuple(range(len(plan))) if chunk_indices is None else tuple(chunk_indices)
    if (
        any(isinstance(index, bool) or not isinstance(index, int) for index in indices)
        or indices != tuple(sorted(set(indices)))
        or any(index < 0 or index >= len(plan) for index in indices)
    ):
        raise SymbolicEngineError("feature-universe chunk indices must be unique increasing 0..63")
    policies_per_candidate = CONTEXT_COUNT * TIME_FILTER_COUNT * DELAY_COUNT
    catalog = build_base_event_catalog().candidates
    for chunk_index in indices:
        chunk = plan[chunk_index]
        first_candidate_rank = (chunk.first_policy_rank - 1) // policies_per_candidate + 1
        last_candidate_rank = (chunk.last_policy_rank - 1) // policies_per_candidate + 1
        cube_rows: list[CandidateCubeCommitment] = []
        policy_rows: list[FeaturePolicyCommitment] = []
        for candidate_rank in range(first_candidate_rank, last_candidate_rank + 1):
            candidate = catalog[candidate_rank - 1]
            cube = build_candidate_policy_cube(stage, candidate, structural_lattice)
            cube_rows.append(_candidate_cube_commitment(cube))
            policy_rows.extend(
                FeaturePolicyCommitment(
                    item.policy.policy_rank,
                    item.policy.policy_id,
                    candidate.candidate_id,
                    item.mask_sha256,
                    item.support_count,
                    item.support_day_count,
                    item.evaluable_mask_sha256,
                    item.evaluable_support_count,
                    item.evaluable_support_day_count,
                )
                for item in cube.iter_masks(
                    first_policy_rank=chunk.first_policy_rank,
                    last_policy_rank=chunk.last_policy_rank,
                )
            )
            stage.release_candidate_state(candidate)
        definition = {
            "candidate_cubes": [item.as_dict() for item in cube_rows],
            "chunk": chunk.as_dict(),
            "policies": [item.as_dict() for item in policy_rows],
            "schema": MASK_SCHEMA,
        }
        yield FeatureUniverseCommitmentChunk(
            chunk,
            tuple(cube_rows),
            tuple(policy_rows),
            canonical_sha256(definition),
        )


@dataclass(frozen=True, slots=True)
class ReferenceOutcomeSurface:
    """Direction-adjusted gross terminal ticks keyed by a frozen anchor."""

    horizon_seconds: int
    gross_ticks_by_anchor: Mapping[tuple[str, int, int, int, Direction], int]
    censored_anchor_keys: frozenset[tuple[str, int, int, int, Direction]] = frozenset()

    def __post_init__(self) -> None:
        if self.horizon_seconds not in REFERENCE_HORIZONS_SECONDS:
            raise SymbolicEngineError("reference surface horizon is outside the frozen grid")
        for key, value in self.gross_ticks_by_anchor.items():
            if (
                not isinstance(key, tuple)
                or len(key) != 5
                or key[4] not in ("LONG", "SHORT")
                or isinstance(value, bool)
                or not isinstance(value, int)
            ):
                raise SymbolicEngineError("reference outcome surface contains an invalid row")
        for key in self.censored_anchor_keys:
            if not isinstance(key, tuple) or len(key) != 5 or key[4] not in ("LONG", "SHORT"):
                raise SymbolicEngineError("reference outcome surface has an invalid censor key")
            if key in self.gross_ticks_by_anchor:
                raise SymbolicEngineError("a reference anchor cannot be both filled and censored")


@dataclass(frozen=True, slots=True)
class HorizonReferenceScore:
    horizon_seconds: int
    fill_count: int
    active_day_count: int
    net_ticks: int
    gross_profit_ticks: int
    gross_loss_ticks: int
    positive_group_count: int
    group_count: int
    worst_group_ev_numerator: int
    worst_group_ev_denominator: int
    censored_count: int = 0

    @property
    def ev(self) -> Fraction:
        return Fraction(self.net_ticks, self.fill_count) if self.fill_count else Fraction(-(10**18))

    @property
    def worst_group_ev(self) -> Fraction:
        return Fraction(self.worst_group_ev_numerator, self.worst_group_ev_denominator)

    @property
    def profit_factor(self) -> Fraction | None:
        if self.gross_loss_ticks == 0:
            return None if self.gross_profit_ticks == 0 else Fraction(10**18)
        return Fraction(self.gross_profit_ticks, self.gross_loss_ticks)

    def as_dict(self) -> dict[str, object]:
        profit_factor = self.profit_factor
        return {
            "active_day_count": self.active_day_count,
            "censored_count": self.censored_count,
            "ev_denominator": max(self.fill_count, 1),
            "ev_numerator": self.net_ticks if self.fill_count else -(10**18),
            "fill_count": self.fill_count,
            "gross_loss_ticks": self.gross_loss_ticks,
            "gross_profit_ticks": self.gross_profit_ticks,
            "group_count": self.group_count,
            "horizon_seconds": self.horizon_seconds,
            "net_ticks": self.net_ticks,
            "positive_group_count": self.positive_group_count,
            "profit_factor_denominator": None
            if profit_factor is None
            else profit_factor.denominator,
            "profit_factor_numerator": None if profit_factor is None else profit_factor.numerator,
            "worst_group_ev_denominator": self.worst_group_ev_denominator,
            "worst_group_ev_numerator": self.worst_group_ev_numerator,
        }


@dataclass(frozen=True, slots=True)
class StageAReferenceScore:
    policy_id: str
    policy_rank: int
    mask_sha256: str
    raw_mask_sha256: str
    base_candidate_id: str
    family: Family
    direction: Direction
    support_count: int
    support_day_count: int
    evaluable_support_count: int
    evaluable_support_day_count: int
    robust_horizon_count: int
    worst_group_ev_numerator: int
    worst_group_ev_denominator: int
    median_ev_numerator: int
    median_ev_denominator: int
    eligible: bool
    rejection_reasons: tuple[str, ...]
    horizons: tuple[HorizonReferenceScore, ...]

    @property
    def worst_group_ev(self) -> Fraction:
        return Fraction(self.worst_group_ev_numerator, self.worst_group_ev_denominator)

    @property
    def median_ev(self) -> Fraction:
        return Fraction(self.median_ev_numerator, self.median_ev_denominator)

    @property
    def raw_gate_rejection_reasons(self) -> tuple[str, ...]:
        return tuple(
            reason
            for reason in self.rejection_reasons
            if reason in {"RAW_SUPPORT_LT_60", "SUPPORT_DAYS_LT_40", "GROUP_SUPPORT_LT_6"}
        )

    @property
    def raw_gate_eligible(self) -> bool:
        return not self.raw_gate_rejection_reasons

    def as_dict(self) -> dict[str, object]:
        return {
            "base_candidate_id": self.base_candidate_id,
            "direction": self.direction,
            "eligible": self.eligible,
            "family": self.family,
            "horizons": [item.as_dict() for item in self.horizons],
            "median_ev_denominator": self.median_ev_denominator,
            "median_ev_numerator": self.median_ev_numerator,
            "mask_sha256": self.mask_sha256,
            "raw_mask_sha256": self.raw_mask_sha256,
            "raw_gate_eligible": self.raw_gate_eligible,
            "raw_gate_rejection_reasons": list(self.raw_gate_rejection_reasons),
            "policy_id": self.policy_id,
            "policy_rank": self.policy_rank,
            "rejection_reasons": list(self.rejection_reasons),
            "robust_horizon_count": self.robust_horizon_count,
            "schema": STAGE_A_SCORE_SCHEMA,
            "support_count": self.support_count,
            "support_day_count": self.support_day_count,
            "evaluable_support_count": self.evaluable_support_count,
            "evaluable_support_day_count": self.evaluable_support_day_count,
            "worst_group_ev_denominator": self.worst_group_ev_denominator,
            "worst_group_ev_numerator": self.worst_group_ev_numerator,
        }


def _score_one_horizon(
    mask: PolicyMask,
    surface: ReferenceOutcomeSurface,
    group_by_date: Mapping[date, str],
) -> HorizonReferenceScore:
    return _score_records_one_horizon(mask.records, surface, group_by_date)


def _score_records_one_horizon(
    records: Sequence[AnchorRecord],
    surface: ReferenceOutcomeSurface,
    group_by_date: Mapping[date, str],
) -> HorizonReferenceScore:
    nets: list[tuple[AnchorRecord, int]] = []
    censored_count = 0
    occupied_until_ns = -1
    horizon_ns = surface.horizon_seconds * ONE_SECOND_NS
    for anchor in sorted(
        records,
        key=lambda item: (
            item.anchor_ns,
            item.contract,
            item.outcome_span_id,
            item.segment_id,
            item.direction,
        ),
    ):
        if anchor.anchor_ns < occupied_until_ns:
            continue
        if anchor.outcome_key in surface.censored_anchor_keys:
            censored_count += 1
            continue
        gross = surface.gross_ticks_by_anchor.get(anchor.outcome_key)
        if gross is None:
            continue
        nets.append((anchor, gross - TOTAL_FRICTION_TICKS))
        occupied_until_ns = anchor.anchor_ns + horizon_ns
    net_ticks = sum(net for _, net in nets)
    gross_profit = sum(max(net, 0) for _, net in nets)
    gross_loss = sum(max(-net, 0) for _, net in nets)
    grouped: dict[str, list[int]] = defaultdict(list)
    for anchor, net in nets:
        group = group_by_date.get(anchor.source_date)
        if group is not None:
            grouped[group].append(net)
    expected_groups = tuple(sorted(set(group_by_date.values())))
    group_evs = [
        Fraction(sum(grouped[group]), len(grouped[group]))
        if grouped[group]
        else Fraction(-(10**18))
        for group in expected_groups
    ]
    worst = min(group_evs, default=Fraction(-(10**18)))
    return HorizonReferenceScore(
        surface.horizon_seconds,
        len(nets),
        len({anchor.source_date for anchor, _ in nets}),
        net_ticks,
        gross_profit,
        gross_loss,
        sum(1 for value in group_evs if value > 0),
        len(expected_groups),
        worst.numerator,
        worst.denominator,
        censored_count,
    )


def score_stage_a_reference_horizons(
    masks: Iterable[PolicyMask | StructurallyEligiblePolicyMask],
    outcome_surfaces: Sequence[ReferenceOutcomeSurface],
    group_by_date: Mapping[date, str],
) -> tuple[StageAReferenceScore, ...]:
    """Score frozen masks on five shared terminal-return surfaces.

    This interface accepts outcomes; all catalog, event, mask, and dedup APIs
    above it remain feature-only.  ``gross_ticks_by_anchor`` must already be
    direction adjusted and must contain no shortened or cross-lineage path.
    """

    surfaces = tuple(sorted(outcome_surfaces, key=lambda item: item.horizon_seconds))
    if tuple(item.horizon_seconds for item in surfaces) != REFERENCE_HORIZONS_SECONDS:
        raise SymbolicEngineError("Stage-A scoring requires all five exact horizons")
    if not group_by_date or any(not isinstance(key, date) for key in group_by_date):
        raise SymbolicEngineError("Stage-A scoring requires date-to-group mapping")
    expected_groups = tuple(sorted(set(group_by_date.values())))
    if len(expected_groups) < 3:
        raise SymbolicEngineError("Stage-A scoring requires at least three chronological groups")
    output: list[StageAReferenceScore] = []
    ordered_masks = sorted(
        masks,
        key=lambda item: (
            item.raw_mask.policy.policy_rank
            if isinstance(item, StructurallyEligiblePolicyMask)
            else item.policy.policy_rank
        ),
    )
    for item in ordered_masks:
        raw_mask = item.raw_mask if isinstance(item, StructurallyEligiblePolicyMask) else item
        mask = item.evaluable_mask if isinstance(item, StructurallyEligiblePolicyMask) else item
        horizon_scores = tuple(
            _score_one_horizon(mask, surface, group_by_date) for surface in surfaces
        )
        robust = sum(
            1
            for item in horizon_scores
            if item.censored_count == 0
            and item.net_ticks > 0
            and item.positive_group_count >= 2
            and item.fill_count >= 48
            and item.active_day_count >= 30
        )
        support_by_group: dict[str, int] = defaultdict(int)
        for record in raw_mask.records:
            group = group_by_date.get(record.source_date)
            if group is not None:
                support_by_group[group] += 1
        reasons: list[str] = []
        if raw_mask.support_count < 60:
            reasons.append("RAW_SUPPORT_LT_60")
        if raw_mask.support_day_count < 40:
            reasons.append("SUPPORT_DAYS_LT_40")
        if any(support_by_group[group] < 6 for group in expected_groups):
            reasons.append("GROUP_SUPPORT_LT_6")
        if any(item.censored_count != 0 for item in horizon_scores):
            reasons.append("CENSORED_COUNT_NONZERO")
        if robust < 3:
            reasons.append("ROBUST_POSITIVE_HORIZONS_LT_3")
        worst = min((item.worst_group_ev for item in horizon_scores), default=Fraction(-(10**18)))
        evs = sorted(item.ev for item in horizon_scores)
        median = evs[len(evs) // 2]
        output.append(
            StageAReferenceScore(
                mask.policy.policy_id,
                mask.policy.policy_rank,
                mask.mask_sha256,
                raw_mask.mask_sha256,
                mask.policy.base_candidate_id,
                mask.family,
                mask.direction,
                raw_mask.support_count,
                raw_mask.support_day_count,
                mask.support_count,
                mask.support_day_count,
                robust,
                worst.numerator,
                worst.denominator,
                median.numerator,
                median.denominator,
                not reasons,
                tuple(reasons),
                horizon_scores,
            )
        )
    return tuple(output)


@dataclass(frozen=True, slots=True)
class StageAScoreChunk:
    chunk: StageAChunkSpec
    scores: tuple[StageAReferenceScore, ...]
    unique_behavior_count: int
    artifact_sha256: str

    def __post_init__(self) -> None:
        if len(self.scores) != self.chunk.policy_count:
            raise SymbolicEngineError("Stage-A score chunk row count differs")
        ranks = tuple(item.policy_rank for item in self.scores)
        if ranks != tuple(range(self.chunk.first_policy_rank, self.chunk.last_policy_rank + 1)):
            raise SymbolicEngineError("Stage-A score chunk ranks differ")
        if self.unique_behavior_count != len({item.mask_sha256 for item in self.scores}):
            raise SymbolicEngineError("Stage-A score chunk behavior count differs")
        _require_sha(self.artifact_sha256, label="Stage-A score chunk artifact_sha256")
        if canonical_sha256(self.definition_dict()) != self.artifact_sha256:
            raise SymbolicEngineError("Stage-A score chunk hash differs")

    def definition_dict(self) -> dict[str, object]:
        return {
            "chunk": self.chunk.as_dict(),
            "schema": STAGE_A_SCORE_SCHEMA,
            "scores": [item.as_dict() for item in self.scores],
            "unique_behavior_count": self.unique_behavior_count,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def _validate_stage_a_surfaces(
    outcome_surfaces: Sequence[ReferenceOutcomeSurface],
    group_by_date: Mapping[date, str],
) -> tuple[tuple[ReferenceOutcomeSurface, ...], tuple[str, ...]]:
    surfaces = tuple(sorted(outcome_surfaces, key=lambda item: item.horizon_seconds))
    if tuple(item.horizon_seconds for item in surfaces) != REFERENCE_HORIZONS_SECONDS:
        raise SymbolicEngineError("Stage-A scoring requires all five exact horizons")
    if not group_by_date or any(not isinstance(key, date) for key in group_by_date):
        raise SymbolicEngineError("Stage-A scoring requires date-to-group mapping")
    expected_groups = tuple(sorted(set(group_by_date.values())))
    if len(expected_groups) < 3:
        raise SymbolicEngineError("Stage-A scoring requires at least three chronological groups")
    return surfaces, expected_groups


def score_stage_a_cube_chunk(
    stage: SymbolicStage,
    chunk: StageAChunkSpec,
    outcome_surfaces: Sequence[ReferenceOutcomeSurface],
    group_by_date: Mapping[date, str],
    *,
    release_candidate_state: bool = True,
    structural_lattice: StructuralEligibilityLattice | None = None,
) -> StageAScoreChunk:
    """Score one of 64 raw chunks using candidate-shared bitsets and score cubes."""

    if chunk != build_stage_a_chunk_plan()[chunk.chunk_index]:
        raise SymbolicEngineError("Stage-A score chunk is outside the frozen 64-way plan")
    surfaces, expected_groups = _validate_stage_a_surfaces(outcome_surfaces, group_by_date)
    policies_per_candidate = CONTEXT_COUNT * TIME_FILTER_COUNT * DELAY_COUNT
    first_candidate_rank = (chunk.first_policy_rank - 1) // policies_per_candidate + 1
    last_candidate_rank = (chunk.last_policy_rank - 1) // policies_per_candidate + 1
    catalog = build_base_event_catalog().candidates
    output: list[StageAReferenceScore] = []
    for candidate_rank in range(first_candidate_rank, last_candidate_rank + 1):
        candidate = catalog[candidate_rank - 1]
        cube = build_candidate_policy_cube(stage, candidate, structural_lattice)
        score_by_bits: dict[
            int,
            tuple[
                tuple[HorizonReferenceScore, ...],
                int,
                Fraction,
                Fraction,
            ],
        ] = {}
        for encoded in cube.iter_masks(
            first_policy_rank=chunk.first_policy_rank,
            last_policy_rank=chunk.last_policy_rank,
        ):
            cached = score_by_bits.get(encoded.evaluable_bits)
            if cached is None:
                records = cube.records_for_bits(encoded.evaluable_bits)
                horizons = tuple(
                    _score_records_one_horizon(records, surface, group_by_date)
                    for surface in surfaces
                )
                robust = sum(
                    1
                    for item in horizons
                    if item.censored_count == 0
                    and item.net_ticks > 0
                    and item.positive_group_count >= 2
                    and item.fill_count >= 48
                    and item.active_day_count >= 30
                )
                worst = min(
                    (item.worst_group_ev for item in horizons),
                    default=Fraction(-(10**18)),
                )
                evs = sorted(item.ev for item in horizons)
                median = evs[len(evs) // 2]
                cached = horizons, robust, worst, median
                score_by_bits[encoded.evaluable_bits] = cached
            horizons, robust, worst, median = cached
            raw_records = cube.records_for_bits(encoded.bits)
            support_by_group: dict[str, int] = defaultdict(int)
            for record in raw_records:
                group = group_by_date.get(record.source_date)
                if group is not None:
                    support_by_group[group] += 1
            reasons: list[str] = []
            if encoded.support_count < 60:
                reasons.append("RAW_SUPPORT_LT_60")
            if encoded.support_day_count < 40:
                reasons.append("SUPPORT_DAYS_LT_40")
            if any(support_by_group[group] < 6 for group in expected_groups):
                reasons.append("GROUP_SUPPORT_LT_6")
            if any(item.censored_count != 0 for item in horizons):
                reasons.append("CENSORED_COUNT_NONZERO")
            if robust < 3:
                reasons.append("ROBUST_POSITIVE_HORIZONS_LT_3")
            output.append(
                StageAReferenceScore(
                    encoded.policy.policy_id,
                    encoded.policy.policy_rank,
                    encoded.evaluable_mask_sha256,
                    encoded.mask_sha256,
                    encoded.policy.base_candidate_id,
                    candidate.family,
                    candidate.direction,
                    encoded.support_count,
                    encoded.support_day_count,
                    encoded.evaluable_support_count,
                    encoded.evaluable_support_day_count,
                    robust,
                    worst.numerator,
                    worst.denominator,
                    median.numerator,
                    median.denominator,
                    not reasons,
                    tuple(reasons),
                    horizons,
                )
            )
        if release_candidate_state:
            stage.release_candidate_state(candidate)
    scores = tuple(output)
    definition = {
        "chunk": chunk.as_dict(),
        "schema": STAGE_A_SCORE_SCHEMA,
        "scores": [item.as_dict() for item in scores],
        "unique_behavior_count": len({item.mask_sha256 for item in scores}),
    }
    return StageAScoreChunk(
        chunk,
        scores,
        len({item.mask_sha256 for item in scores}),
        canonical_sha256(definition),
    )


def iter_stage_a_cube_score_chunks(
    stage: SymbolicStage,
    outcome_surfaces: Sequence[ReferenceOutcomeSurface],
    group_by_date: Mapping[date, str],
    *,
    chunk_indices: Iterable[int] | None = None,
    structural_lattice: StructuralEligibilityLattice | None = None,
) -> Iterator[StageAScoreChunk]:
    """Run requested frozen chunks in increasing order; resume chooses the lowest missing."""

    plan = build_stage_a_chunk_plan()
    indices = tuple(range(len(plan))) if chunk_indices is None else tuple(chunk_indices)
    if (
        any(isinstance(index, bool) or not isinstance(index, int) for index in indices)
        or indices != tuple(sorted(set(indices)))
        or any(index < 0 or index >= len(plan) for index in indices)
    ):
        raise SymbolicEngineError("Stage-A chunk indices must be unique increasing values in 0..63")
    for index in indices:
        yield score_stage_a_cube_chunk(
            stage,
            plan[index],
            outcome_surfaces,
            group_by_date,
            structural_lattice=structural_lattice,
        )


@dataclass(frozen=True, slots=True)
class StageASelection:
    classification: str
    selected_policy_ids: tuple[str, ...]
    selected_scores: tuple[StageAReferenceScore, ...]
    eligible_count: int
    deduplicated_policy_count: int
    alias_count: int
    alias_chain_sha256: str
    stage_b_pair_budget_maximum: int
    stage_b_pair_budget_used: int
    budget_rejected_policy_count: int
    budget_decision_sha256: str
    artifact_sha256: str

    def __post_init__(self) -> None:
        if self.selected_policy_ids != tuple(item.policy_id for item in self.selected_scores):
            raise SymbolicEngineError("Stage-A selection ids and scores differ")
        if len(self.selected_scores) > STAGE_A_MAXIMUM_SELECTION:
            raise SymbolicEngineError("Stage-A selection exceeds its maximum")
        if self.stage_b_pair_budget_maximum != STAGE_B_PAIR_BUDGET_MAXIMUM:
            raise SymbolicEngineError("Stage-A Stage-B pair budget differs")
        expected_used = sum(
            item.evaluable_support_count * ENTRY_POLICY_COUNT * STAGE_B_CONTROL_WORLD_COUNT
            for item in self.selected_scores
        )
        if (
            self.stage_b_pair_budget_used != expected_used
            or not 0 <= self.stage_b_pair_budget_used <= self.stage_b_pair_budget_maximum
            or self.budget_rejected_policy_count < 0
        ):
            raise SymbolicEngineError("Stage-A Stage-B pair budget accounting differs")
        _require_sha(self.alias_chain_sha256, label="Stage-A alias chain SHA")
        _require_sha(self.budget_decision_sha256, label="Stage-A budget decision SHA")
        _require_sha(self.artifact_sha256, label="Stage-A selection artifact SHA")
        if canonical_sha256(self.definition_dict()) != self.artifact_sha256:
            raise SymbolicEngineError("Stage-A selection artifact differs")

    def definition_dict(self) -> dict[str, object]:
        return {
            "budget_decision_sha256": self.budget_decision_sha256,
            "budget_rejected_policy_count": self.budget_rejected_policy_count,
            "classification": self.classification,
            "deduplicated_policy_count": self.deduplicated_policy_count,
            "eligible_count": self.eligible_count,
            "alias_chain_sha256": self.alias_chain_sha256,
            "alias_count": self.alias_count,
            "schema": STAGE_A_SCORE_SCHEMA,
            "selected_policy_ids": list(self.selected_policy_ids),
            "selected_scores": [item.as_dict() for item in self.selected_scores],
            "stage_b_pair_budget_maximum": self.stage_b_pair_budget_maximum,
            "stage_b_pair_budget_used": self.stage_b_pair_budget_used,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def select_stage_a_top256(scores: Iterable[StageAReferenceScore]) -> StageASelection:
    """Deduplicate, rank, diversify, and fit the exact pre-outcome Stage-B budget."""

    def ordering(item: StageAReferenceScore) -> tuple[object, ...]:
        return (
            -item.robust_horizon_count,
            -item.worst_group_ev,
            -item.median_ev,
            -item.support_count,
            item.policy_rank,
        )

    prior_rank = 0
    scores_by_mask: dict[str, list[StageAReferenceScore]] = defaultdict(list)
    alias_chain = "0" * 64
    for item in scores:
        if item.policy_rank <= prior_rank:
            raise SymbolicEngineError("Stage-A scores must use increasing unique policy ranks")
        prior_rank = item.policy_rank
        _require_sha(item.mask_sha256, label="Stage-A mask_sha256")
        _require_sha(item.raw_mask_sha256, label="Stage-A raw_mask_sha256")
        scores_by_mask[item.mask_sha256].append(item)

    representatives: list[StageAReferenceScore] = []
    aliases: list[tuple[StageAReferenceScore, StageAReferenceScore]] = []
    for rows in scores_by_mask.values():
        representative = min(rows, key=lambda item: (not item.raw_gate_eligible, item.policy_rank))
        representatives.append(representative)
        aliases.extend((item, representative) for item in rows if item is not representative)
    for alias, representative in sorted(aliases, key=lambda item: item[0].policy_rank):
        alias_chain = canonical_sha256(
            {
                "alias_policy_id": alias.policy_id,
                "alias_policy_rank": alias.policy_rank,
                "alias_raw_gate_eligible": alias.raw_gate_eligible,
                "alias_raw_mask_sha256": alias.raw_mask_sha256,
                "mask_sha256": alias.mask_sha256,
                "predecessor_sha256": alias_chain,
                "representative_policy_id": representative.policy_id,
                "representative_policy_rank": representative.policy_rank,
                "representative_raw_gate_eligible": representative.raw_gate_eligible,
                "representative_raw_mask_sha256": representative.raw_mask_sha256,
                "schema": STAGE_A_SCORE_SCHEMA,
            }
        )
    eligible = [item for item in representatives if item.eligible]
    eligible.sort(key=ordering)
    selected: list[StageAReferenceScore] = []
    family_counts: dict[Family, int] = defaultdict(int)
    direction_counts: dict[tuple[Family, Direction], int] = defaultdict(int)
    pair_budget_used = 0
    budget_rejected = 0
    budget_decisions: list[dict[str, object]] = []
    for item in eligible:
        pair_cost = item.evaluable_support_count * ENTRY_POLICY_COUNT * STAGE_B_CONTROL_WORLD_COUNT
        if family_counts[item.family] >= 16:
            budget_decisions.append(
                {
                    "cumulative_pair_count": pair_budget_used,
                    "decision": "FAMILY_CAP_REJECTED",
                    "pair_cost": pair_cost,
                    "policy_id": item.policy_id,
                    "policy_rank": item.policy_rank,
                }
            )
            continue
        if direction_counts[item.family, item.direction] >= 8:
            budget_decisions.append(
                {
                    "cumulative_pair_count": pair_budget_used,
                    "decision": "FAMILY_DIRECTION_CAP_REJECTED",
                    "pair_cost": pair_cost,
                    "policy_id": item.policy_id,
                    "policy_rank": item.policy_rank,
                }
            )
            continue
        if len(selected) >= STAGE_A_MAXIMUM_SELECTION:
            budget_decisions.append(
                {
                    "cumulative_pair_count": pair_budget_used,
                    "decision": "SELECTION_MAXIMUM_REJECTED",
                    "pair_cost": pair_cost,
                    "policy_id": item.policy_id,
                    "policy_rank": item.policy_rank,
                }
            )
            continue
        if pair_budget_used + pair_cost > STAGE_B_PAIR_BUDGET_MAXIMUM:
            budget_rejected += 1
            budget_decisions.append(
                {
                    "cumulative_pair_count": pair_budget_used,
                    "decision": "STAGE_B_PAIR_BUDGET_REJECTED",
                    "pair_cost": pair_cost,
                    "policy_id": item.policy_id,
                    "policy_rank": item.policy_rank,
                }
            )
            continue
        selected.append(item)
        pair_budget_used += pair_cost
        family_counts[item.family] += 1
        direction_counts[item.family, item.direction] += 1
        budget_decisions.append(
            {
                "cumulative_pair_count": pair_budget_used,
                "decision": "SELECTED",
                "pair_cost": pair_cost,
                "policy_id": item.policy_id,
                "policy_rank": item.policy_rank,
            }
        )
    budget_decision_sha = canonical_sha256(
        {
            "decisions": budget_decisions,
            "entry_policy_count": ENTRY_POLICY_COUNT,
            "maximum_pair_count": STAGE_B_PAIR_BUDGET_MAXIMUM,
            "schema": STAGE_A_SCORE_SCHEMA,
            "world_count": STAGE_B_CONTROL_WORLD_COUNT,
        }
    )
    classification = "STAGE_A_ANCHORS_SELECTED" if selected else "NO_STAGE_A_ANCHORS"
    payload = {
        "budget_decision_sha256": budget_decision_sha,
        "budget_rejected_policy_count": budget_rejected,
        "classification": classification,
        "deduplicated_policy_count": len(scores_by_mask),
        "eligible_count": len(eligible),
        "alias_chain_sha256": alias_chain,
        "alias_count": len(aliases),
        "schema": STAGE_A_SCORE_SCHEMA,
        "selected_policy_ids": [item.policy_id for item in selected],
        "selected_scores": [item.as_dict() for item in selected],
        "stage_b_pair_budget_maximum": STAGE_B_PAIR_BUDGET_MAXIMUM,
        "stage_b_pair_budget_used": pair_budget_used,
    }
    return StageASelection(
        classification,
        tuple(item.policy_id for item in selected),
        tuple(selected),
        len(eligible),
        len(scores_by_mask),
        len(aliases),
        alias_chain,
        STAGE_B_PAIR_BUDGET_MAXIMUM,
        pair_budget_used,
        budget_rejected,
        budget_decision_sha,
        canonical_sha256(payload),
    )


@dataclass(frozen=True, slots=True)
class CompleteStrategyRecipe:
    """One selected anchor policy crossed with one entry and one exit policy."""

    strategy_rank: int
    strategy_id: str
    anchor_selection_rank: int
    anchor_policy_id: str
    entry_policy_id: str
    exit_policy_id: str

    def __post_init__(self) -> None:
        _require_int(self.anchor_selection_rank, label="anchor_selection_rank", minimum=1)
        if self.anchor_selection_rank > STAGE_A_MAXIMUM_SELECTION:
            raise SymbolicEngineError("anchor_selection_rank exceeds Stage-A maximum")
        _require_int(self.strategy_rank, label="strategy_rank", minimum=1)
        _require_sha(self.strategy_id, label="strategy_id")
        _require_sha(self.anchor_policy_id, label="anchor_policy_id")
        entry = _entry_lookup().get(self.entry_policy_id)
        exit_policy = _exit_lookup().get(self.exit_policy_id)
        if entry is None or exit_policy is None:
            raise SymbolicEngineError("strategy refers to an unknown entry or exit policy")
        expected_rank = (
            (self.anchor_selection_rank - 1) * COMPLETE_STRATEGIES_PER_ANCHOR
            + (entry.selection_rank - 1) * EXIT_POLICY_COUNT
            + exit_policy.selection_rank
        )
        if self.strategy_rank != expected_rank:
            raise SymbolicEngineError("complete strategy rank differs from nesting order")
        if canonical_sha256(self.definition_dict()) != self.strategy_id:
            raise SymbolicEngineError("complete strategy id differs from its definition")

    def definition_dict(self) -> dict[str, object]:
        return {
            "anchor_policy_id": self.anchor_policy_id,
            "entry_policy_id": self.entry_policy_id,
            "exit_policy_id": self.exit_policy_id,
            "schema": COMPLETE_STRATEGY_SCHEMA,
        }

    def as_dict(self) -> dict[str, object]:
        return {
            **self.definition_dict(),
            "anchor_selection_rank": self.anchor_selection_rank,
            "strategy_id": self.strategy_id,
            "strategy_rank": self.strategy_rank,
        }


STAGE_B_OUTER_CHUNK_COUNT: Final = 64
STAGE_B_RECIPE_ROWS_PER_CHUNK_MAXIMUM: Final = (
    COMPLETE_STRATEGY_MAXIMUM // STAGE_B_OUTER_CHUNK_COUNT
)


@dataclass(frozen=True, slots=True)
class StageBChunkSpec:
    chunk_index: int
    first_strategy_rank: int
    last_strategy_rank: int
    strategy_count: int
    chunk_id: str

    def __post_init__(self) -> None:
        _require_int(self.chunk_index, label="Stage-B chunk_index", minimum=0)
        if self.chunk_index >= STAGE_B_OUTER_CHUNK_COUNT:
            raise SymbolicEngineError("Stage-B chunk index exceeds frozen plan")
        _require_int(self.strategy_count, label="Stage-B strategy_count", minimum=0)
        if self.strategy_count == 0:
            if self.first_strategy_rank != 1 or self.last_strategy_rank != 0:
                raise SymbolicEngineError("empty Stage-B chunk bounds differ")
        elif (
            self.first_strategy_rank < 1
            or self.last_strategy_rank < self.first_strategy_rank
            or self.strategy_count != self.last_strategy_rank - self.first_strategy_rank + 1
        ):
            raise SymbolicEngineError("Stage-B chunk bounds differ")
        _require_sha(self.chunk_id, label="Stage-B chunk_id")
        if canonical_sha256(self.definition_dict()) != self.chunk_id:
            raise SymbolicEngineError("Stage-B chunk identity differs")

    def definition_dict(self) -> dict[str, object]:
        return {
            "chunk_index": self.chunk_index,
            "first_strategy_rank": self.first_strategy_rank,
            "last_strategy_rank": self.last_strategy_rank,
            "schema": PATH_OUTCOME_SCHEMA,
            "strategy_count": self.strategy_count,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "chunk_id": self.chunk_id}


def build_stage_b_chunk_plan(selected_anchor_count: int) -> tuple[StageBChunkSpec, ...]:
    """Partition a non-empty selected-anchor grid into exactly 64 balanced chunks."""

    _require_int(selected_anchor_count, label="selected_anchor_count", minimum=0)
    if selected_anchor_count > STAGE_A_MAXIMUM_SELECTION:
        raise SymbolicEngineError("selected anchor count exceeds 256")
    total = selected_anchor_count * COMPLETE_STRATEGIES_PER_ANCHOR
    quotient, remainder = divmod(total, STAGE_B_OUTER_CHUNK_COUNT)
    rows = []
    first = 1
    for index in range(STAGE_B_OUTER_CHUNK_COUNT):
        count = quotient + (1 if index < remainder else 0)
        last = 0 if total == 0 else first + count - 1
        definition = {
            "chunk_index": index,
            "first_strategy_rank": first,
            "last_strategy_rank": last,
            "schema": PATH_OUTCOME_SCHEMA,
            "strategy_count": count,
        }
        rows.append(
            StageBChunkSpec(
                index,
                first,
                last,
                count,
                canonical_sha256(definition),
            )
        )
        if total != 0:
            first = last + 1
    if first != total + 1 or max(item.strategy_count for item in rows) > 3_060:
        raise SymbolicEngineError("Stage-B 64-way chunk plan differs")
    return tuple(rows)


def iter_complete_strategy_recipes(
    selected_anchor_policy_ids: Sequence[str],
) -> Iterator[CompleteStrategyRecipe]:
    """Stream at most 195,840 recipes in selected-anchor/entry/exit order."""

    if isinstance(selected_anchor_policy_ids, (str, bytes)):
        raise SymbolicEngineError("selected anchor policy ids must be a sequence")
    policy_ids = tuple(selected_anchor_policy_ids)
    if len(policy_ids) > STAGE_A_MAXIMUM_SELECTION:
        raise SymbolicEngineError("selected anchor count must be between 0 and 256")
    if len(set(policy_ids)) != len(policy_ids):
        raise SymbolicEngineError("selected anchor policy ids must be unique")
    for policy_id in policy_ids:
        _require_sha(policy_id, label="selected anchor policy id")
    rank = 0
    for anchor_rank, anchor_policy_id in enumerate(policy_ids, start=1):
        for entry, exit_policy in product(
            build_entry_catalog().candidates,
            build_exit_catalog().candidates,
        ):
            rank += 1
            definition = {
                "anchor_policy_id": anchor_policy_id,
                "entry_policy_id": entry.entry_id,
                "exit_policy_id": exit_policy.exit_id,
                "schema": COMPLETE_STRATEGY_SCHEMA,
            }
            yield CompleteStrategyRecipe(
                rank,
                canonical_sha256(definition),
                anchor_rank,
                anchor_policy_id,
                entry.entry_id,
                exit_policy.exit_id,
            )
    expected = len(policy_ids) * COMPLETE_STRATEGIES_PER_ANCHOR
    if rank != expected:  # pragma: no cover - product invariant
        raise SymbolicEngineError("complete strategy construction count differs")


@dataclass(frozen=True, slots=True, order=True)
class RuleExitTimes:
    anchor_key: tuple[str, int, int, int, Direction]
    opposite_trigger_ns: int | None
    context_invalid_ns: int | None

    def __post_init__(self) -> None:
        if len(self.anchor_key) != 5 or self.anchor_key[4] not in ("LONG", "SHORT"):
            raise SymbolicEngineError("rule-exit anchor key is invalid")
        anchor_ns = self.anchor_key[3]
        for label, value in (
            ("opposite_trigger_ns", self.opposite_trigger_ns),
            ("context_invalid_ns", self.context_invalid_ns),
        ):
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value <= anchor_ns
            ):
                raise SymbolicEngineError(f"{label} must be strictly after the anchor")

    def as_dict(self) -> dict[str, object]:
        return {
            "anchor_key": list(self.anchor_key),
            "context_invalid_ns": self.context_invalid_ns,
            "opposite_trigger_ns": self.opposite_trigger_ns,
        }


@dataclass(frozen=True, slots=True)
class RuleExitSchedule:
    """Feature-only future completed-bar rule times, frozen before 1s outcomes."""

    policy_id: str
    rows: tuple[RuleExitTimes, ...]
    artifact_sha256: str

    def __post_init__(self) -> None:
        _require_sha(self.policy_id, label="rule-exit policy_id")
        if self.rows != tuple(sorted(self.rows)):
            raise SymbolicEngineError("rule-exit rows must use canonical anchor order")
        if len({item.anchor_key for item in self.rows}) != len(self.rows):
            raise SymbolicEngineError("rule-exit rows contain duplicate anchors")
        _require_sha(self.artifact_sha256, label="rule-exit artifact_sha256")
        if canonical_sha256(self.definition_dict()) != self.artifact_sha256:
            raise SymbolicEngineError("rule-exit schedule hash differs")

    @classmethod
    def from_rows(
        cls,
        policy_id: str,
        rows: Iterable[RuleExitTimes],
    ) -> RuleExitSchedule:
        canonical = tuple(sorted(rows))
        definition = {
            "policy_id": policy_id,
            "rows": [item.as_dict() for item in canonical],
            "schema": PATH_OUTCOME_SCHEMA,
        }
        return cls(policy_id, canonical, canonical_sha256(definition))

    def definition_dict(self) -> dict[str, object]:
        return {
            "policy_id": self.policy_id,
            "rows": [item.as_dict() for item in self.rows],
            "schema": PATH_OUTCOME_SCHEMA,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}

    def lookup(self) -> Mapping[tuple[str, int, int, int, Direction], RuleExitTimes]:
        return {item.anchor_key: item for item in self.rows}


def build_rule_exit_schedule(
    stage: SymbolicStage,
    policy: AnchorPolicy,
    anchors: Sequence[AnchorRecord],
) -> RuleExitSchedule:
    """Freeze opposite-event and context-invalidation times without 1s outcomes."""

    if not isinstance(stage, SymbolicStage):
        raise SymbolicEngineError("rule-exit schedule requires a SymbolicStage")
    candidate = _candidate_lookup().get(policy.base_candidate_id)
    context = _context_lookup().get(policy.context_id)
    if candidate is None or context is None:
        raise SymbolicEngineError("rule-exit policy refers to an unknown catalog member")
    canonical_anchors = tuple(sorted(set(anchors)))
    if any(anchor.direction != candidate.direction for anchor in canonical_anchors):
        raise SymbolicEngineError("rule-exit anchors differ from candidate direction")
    maximum_cap_ns = (
        max(item.cap_seconds for item in build_exit_catalog().candidates) * ONE_SECOND_NS
    )

    mirror = stage._mirror_candidate(candidate)
    opposite_by_lineage: dict[tuple[str, int, int], list[int]] = defaultdict(list)
    for event in stage.events(mirror):
        opposite_by_lineage[
            event.contract,
            event.outcome_span_id,
            event.segment_id,
        ].append(event.trigger_end_ns)

    invalid_by_lineage: dict[tuple[str, int, int], list[int]] = defaultdict(list)
    if context.kind != "ANY":
        for wrapped in stage.series_by_timeframe[FIVE_MINUTES].bars:
            context_index = stage._context_index(
                context.timeframe_seconds,
                wrapped,
                wrapped.bar.end_ns,
            )
            valid = context_index is not None and stage._context_state(
                candidate,
                context,
                context_index,
            )
            if not valid:
                invalid_by_lineage[_lineage(wrapped)].append(wrapped.bar.end_ns)

    rows: list[RuleExitTimes] = []
    for anchor in canonical_anchors:
        lineage = anchor.contract, anchor.outcome_span_id, anchor.segment_id
        limit = anchor.anchor_ns + maximum_cap_ns
        opposite_values = opposite_by_lineage[lineage]
        opposite_index = bisect_right(opposite_values, anchor.anchor_ns)
        opposite_ns = (
            opposite_values[opposite_index]
            if opposite_index < len(opposite_values) and opposite_values[opposite_index] <= limit
            else None
        )
        invalid_values = invalid_by_lineage[lineage]
        invalid_index = bisect_right(invalid_values, anchor.anchor_ns)
        invalid_ns = (
            invalid_values[invalid_index]
            if invalid_index < len(invalid_values) and invalid_values[invalid_index] <= limit
            else None
        )
        rows.append(RuleExitTimes(anchor.outcome_key, opposite_ns, invalid_ns))
    return RuleExitSchedule.from_rows(policy.policy_id, rows)


def _atr_distance(anchor: AnchorRecord, fraction: Fraction) -> int:
    if anchor.atr_sum_ticks < 0 or anchor.atr_denominator <= 0:
        raise SymbolicEngineError("anchor ATR state is invalid")
    return max(
        1,
        _round_half_even(
            anchor.atr_sum_ticks * fraction.numerator,
            anchor.atr_denominator * fraction.denominator,
        ),
    )


@dataclass(frozen=True, slots=True)
class FrozenEntryOrder:
    """Feature-only order intent; it contains no touch or fill information."""

    order_id: str
    anchor: AnchorRecord
    entry_policy_id: str
    kind: EntryKind
    order_ticks: int | None
    valid_from_ns: int
    expires_ns: int

    def __post_init__(self) -> None:
        _require_sha(self.order_id, label="order_id")
        entry = _entry_lookup().get(self.entry_policy_id)
        if entry is None or entry.kind != self.kind:
            raise SymbolicEngineError("frozen order refers to a different entry policy")
        if self.valid_from_ns != self.anchor.anchor_ns or self.expires_ns <= self.valid_from_ns:
            raise SymbolicEngineError("frozen order validity interval differs")
        if self.kind == "MARKET":
            if self.order_ticks is not None:
                raise SymbolicEngineError("market order cannot carry a limit price")
        elif (
            isinstance(self.order_ticks, bool)
            or not isinstance(self.order_ticks, int)
            or self.order_ticks <= 0
        ):
            raise SymbolicEngineError("priced entry order requires positive ticks")
        if canonical_sha256(self.definition_dict()) != self.order_id:
            raise SymbolicEngineError("frozen order id differs from its definition")

    def definition_dict(self) -> dict[str, object]:
        return {
            "anchor": self.anchor.as_dict(),
            "entry_policy_id": self.entry_policy_id,
            "expires_ns": self.expires_ns,
            "kind": self.kind,
            "order_ticks": self.order_ticks,
            "schema": PATH_OUTCOME_SCHEMA,
            "valid_from_ns": self.valid_from_ns,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "order_id": self.order_id}


@dataclass(frozen=True, slots=True)
class EntryOrderBatch:
    anchor_policy_id: str
    orders: tuple[FrozenEntryOrder, ...]
    artifact_sha256: str

    def __post_init__(self) -> None:
        _require_sha(self.anchor_policy_id, label="entry-order anchor_policy_id")
        if any(item.anchor is None for item in self.orders):  # pragma: no cover - typing guard
            raise SymbolicEngineError("entry-order batch contains an invalid row")
        keys = tuple(
            (item.anchor, _entry_lookup()[item.entry_policy_id].selection_rank)
            for item in self.orders
        )
        if keys != tuple(sorted(keys)) or len({item.order_id for item in self.orders}) != len(
            self.orders
        ):
            raise SymbolicEngineError("entry-order batch is non-canonical or duplicate")
        _require_sha(self.artifact_sha256, label="entry-order artifact_sha256")
        if canonical_sha256(self.definition_dict()) != self.artifact_sha256:
            raise SymbolicEngineError("entry-order batch hash differs")

    def definition_dict(self) -> dict[str, object]:
        return {
            "anchor_policy_id": self.anchor_policy_id,
            "orders": [item.as_dict() for item in self.orders],
            "schema": PATH_OUTCOME_SCHEMA,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def freeze_entry_orders(
    mask: PolicyMask,
    entry_policies: Sequence[EntryPolicy] | None = None,
) -> EntryOrderBatch:
    """Freeze all order prices/windows from feature-only anchor state."""

    entries = build_entry_catalog().candidates if entry_policies is None else tuple(entry_policies)
    if not entries or len({item.entry_id for item in entries}) != len(entries):
        raise SymbolicEngineError("entry policies must be non-empty and unique")
    if tuple(item.selection_rank for item in entries) != tuple(
        sorted(item.selection_rank for item in entries)
    ):
        raise SymbolicEngineError("entry policies must use catalog order")
    rows: list[FrozenEntryOrder] = []
    for anchor in mask.records:
        sign = _direction_sign(anchor.direction)
        for entry in entries:
            if entry.kind == "MARKET":
                order_ticks = None
                expires_ns = anchor.anchor_ns + FIVE_MINUTES * ONE_SECOND_NS
            elif entry.kind == "STOP_SIGNAL_EXTREME":
                buffer_ticks = entry.parameter("buffer_ticks")
                order_ticks = (
                    anchor.trigger_high_ticks + buffer_ticks
                    if sign > 0
                    else anchor.trigger_low_ticks - buffer_ticks
                )
                expires_ns = (
                    anchor.anchor_ns + entry.parameter("time_in_force_seconds") * ONE_SECOND_NS
                )
            else:
                distance = _atr_distance(anchor, entry.fraction_parameter("retrace_atr"))
                order_ticks = anchor.trigger_close_ticks - sign * distance
                expires_ns = (
                    anchor.anchor_ns + entry.parameter("time_in_force_seconds") * ONE_SECOND_NS
                )
            definition = {
                "anchor": anchor.as_dict(),
                "entry_policy_id": entry.entry_id,
                "expires_ns": expires_ns,
                "kind": entry.kind,
                "order_ticks": order_ticks,
                "schema": PATH_OUTCOME_SCHEMA,
                "valid_from_ns": anchor.anchor_ns,
            }
            rows.append(
                FrozenEntryOrder(
                    canonical_sha256(definition),
                    anchor,
                    entry.entry_id,
                    entry.kind,
                    order_ticks,
                    anchor.anchor_ns,
                    expires_ns,
                )
            )
    canonical = tuple(rows)
    definition = {
        "anchor_policy_id": mask.policy.policy_id,
        "orders": [item.as_dict() for item in canonical],
        "schema": PATH_OUTCOME_SCHEMA,
    }
    return EntryOrderBatch(
        mask.policy.policy_id,
        canonical,
        canonical_sha256(definition),
    )


@dataclass(frozen=True, slots=True)
class CausalExpertValue:
    feature_name: str
    numerator: int
    denominator: int

    def __post_init__(self) -> None:
        if self.feature_name not in EXPERT_FEATURE_NAMES:
            raise SymbolicEngineError("causal expert feature name is unknown")
        if (
            isinstance(self.numerator, bool)
            or not isinstance(self.numerator, int)
            or isinstance(self.denominator, bool)
            or not isinstance(self.denominator, int)
            or self.denominator <= 0
        ):
            raise SymbolicEngineError("causal expert value is not an exact rational")
        normalized = Fraction(self.numerator, self.denominator)
        if (normalized.numerator, normalized.denominator) != (
            self.numerator,
            self.denominator,
        ):
            raise SymbolicEngineError("causal expert value is not normalized")

    @classmethod
    def from_fraction(cls, feature_name: str, value: Fraction | int) -> CausalExpertValue:
        normalized = value if isinstance(value, Fraction) else Fraction(value)
        return cls(feature_name, normalized.numerator, normalized.denominator)

    @property
    def fraction(self) -> Fraction:
        return Fraction(self.numerator, self.denominator)

    def as_dict(self) -> dict[str, object]:
        return {
            "denominator": self.denominator,
            "feature_name": self.feature_name,
            "numerator": self.numerator,
        }


@dataclass(frozen=True, slots=True)
class CausalExpertFeatureArtifact:
    anchor_key: tuple[str, int, int, int, Direction]
    anchor_policy_id: str
    base_candidate_id: str
    context_id: str
    order_id: str
    exit_policy_id: str
    values: tuple[CausalExpertValue, ...]
    inputs_sha256: str
    values_sha256: str
    formula_sha256: str
    artifact_sha256: str

    def __post_init__(self) -> None:
        _validate_control_pair_keys(self.anchor_key, self.anchor_key, allow_identity=True)
        for label, value in (
            ("expert anchor policy id", self.anchor_policy_id),
            ("expert base candidate id", self.base_candidate_id),
            ("expert context id", self.context_id),
            ("expert order id", self.order_id),
            ("expert exit policy id", self.exit_policy_id),
            ("expert input SHA", self.inputs_sha256),
            ("expert values SHA", self.values_sha256),
            ("expert artifact SHA", self.artifact_sha256),
        ):
            _require_sha(value, label=label)
        if self.formula_sha256 != EXPERT_FEATURE_FORMULA_SHA256:
            raise SymbolicEngineError("causal expert formula SHA differs")
        if tuple(item.feature_name for item in self.values) != EXPERT_FEATURE_NAMES:
            raise SymbolicEngineError("causal expert values differ from the exact ordered eight")
        if canonical_sha256([item.as_dict() for item in self.values]) != self.values_sha256:
            raise SymbolicEngineError("causal expert values SHA differs")
        if canonical_sha256(self.definition_dict()) != self.artifact_sha256:
            raise SymbolicEngineError("causal expert artifact SHA differs")

    def definition_dict(self) -> dict[str, object]:
        return {
            "anchor_key": list(self.anchor_key),
            "anchor_policy_id": self.anchor_policy_id,
            "base_candidate_id": self.base_candidate_id,
            "context_id": self.context_id,
            "exit_policy_id": self.exit_policy_id,
            "formula_sha256": self.formula_sha256,
            "inputs_sha256": self.inputs_sha256,
            "order_id": self.order_id,
            "schema": EXPERT_FEATURE_SCHEMA,
            "values": [item.as_dict() for item in self.values],
            "values_sha256": self.values_sha256,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def build_causal_expert_feature_artifact(
    candidate: BaseEventCandidate,
    context: ContextSpec,
    policy: AnchorPolicy,
    anchor: AnchorRecord,
    order: FrozenEntryOrder,
    exit_policy: ExitPolicy,
) -> CausalExpertFeatureArtifact:
    """Build the recipe-aware exact Expert-8 using only decision-cutoff inputs."""

    if not all(
        (
            isinstance(candidate, BaseEventCandidate),
            isinstance(context, ContextSpec),
            isinstance(policy, AnchorPolicy),
            isinstance(anchor, AnchorRecord),
            isinstance(order, FrozenEntryOrder),
            isinstance(exit_policy, ExitPolicy),
        )
    ):
        raise SymbolicEngineError("causal expert builder received a non-causal input type")
    trigger_timeframe = candidate.trigger_timeframe_seconds
    if (
        candidate.direction != anchor.direction
        or policy.base_candidate_id != candidate.candidate_id
        or policy.context_id != context.context_id
        or order.anchor != anchor
        or anchor.trigger_end_ns < anchor.trigger_start_ns
        or anchor.trigger_end_ns - anchor.trigger_start_ns != trigger_timeframe * ONE_SECOND_NS
        or anchor.anchor_ns < anchor.trigger_end_ns
        or anchor.atr_sum_ticks < 0
        or anchor.atr_denominator <= 0
    ):
        raise SymbolicEngineError("causal expert inputs disagree at the decision cutoff")

    def atr_scaled(value: int) -> Fraction:
        return (
            Fraction(0)
            if anchor.atr_sum_ticks == 0
            else Fraction(value * anchor.atr_denominator, anchor.atr_sum_ticks)
        )

    context_relation = {
        "ANY": 0,
        "EFFICIENCY_TREND": 1,
        "EFFICIENCY_RANGE": -1,
        "VOLATILITY_EXPANDING": 1,
        "VOLATILITY_CONTRACTING": -1,
    }.get(context.kind)
    if context.kind == "EMA_RELATION":
        context_relation = context.relation
    if context_relation not in (-1, 0, 1):
        raise SymbolicEngineError("causal expert context relation is invalid")

    if order.kind == "MARKET":
        planned_entry_distance = Fraction(0)
    else:
        if order.order_ticks is None:  # pragma: no cover - FrozenEntryOrder invariant
            raise SymbolicEngineError("causal expert priced order lost its ticks")
        planned_entry_distance = atr_scaled(
            _direction_sign(anchor.direction) * (order.order_ticks - anchor.trigger_close_ticks)
        )

    if exit_policy.kind == "BRACKET":
        reward_risk = exit_policy.fraction_parameter(
            "take_profit_atr"
        ) / exit_policy.fraction_parameter("stop_loss_atr")
    elif exit_policy.kind == "TRAILING":
        reward_risk = exit_policy.fraction_parameter(
            "activation_atr"
        ) / exit_policy.fraction_parameter("trail_atr")
    elif exit_policy.kind == "BREAK_EVEN":
        reward_risk = exit_policy.fraction_parameter(
            "activation_atr"
        ) / exit_policy.fraction_parameter("initial_stop_atr")
    else:
        reward_risk = Fraction(0)

    exact = (
        atr_scaled(
            _direction_sign(anchor.direction)
            * (anchor.trigger_close_ticks - anchor.trigger_open_ticks)
        ),
        Fraction(
            anchor.anchor_ns - anchor.trigger_end_ns,
            trigger_timeframe * ONE_SECOND_NS,
        ),
        Fraction(context_relation),
        Fraction(anchor.atr_sum_ticks, anchor.atr_denominator),
        atr_scaled(anchor.trigger_high_ticks - anchor.trigger_low_ticks),
        Fraction(order.expires_ns - order.valid_from_ns, ONE_SECOND_NS),
        planned_entry_distance,
        reward_risk,
    )
    values = tuple(
        CausalExpertValue.from_fraction(name, value)
        for name, value in zip(EXPERT_FEATURE_NAMES, exact, strict=True)
    )
    inputs_definition = {
        "anchor": anchor.as_dict(),
        "base_candidate": candidate.as_dict(),
        "context": context.as_dict(),
        "exit_policy": exit_policy.as_dict(),
        "frozen_entry_order": order.as_dict(),
        "formula_sha256": EXPERT_FEATURE_FORMULA_SHA256,
        "anchor_policy": policy.as_dict(),
        "schema": EXPERT_FEATURE_SCHEMA,
    }
    inputs_sha = canonical_sha256(inputs_definition)
    values_sha = canonical_sha256([item.as_dict() for item in values])
    definition = {
        "anchor_key": list(anchor.outcome_key),
        "anchor_policy_id": policy.policy_id,
        "base_candidate_id": candidate.candidate_id,
        "context_id": context.context_id,
        "exit_policy_id": exit_policy.exit_id,
        "formula_sha256": EXPERT_FEATURE_FORMULA_SHA256,
        "inputs_sha256": inputs_sha,
        "order_id": order.order_id,
        "schema": EXPERT_FEATURE_SCHEMA,
        "values": [item.as_dict() for item in values],
        "values_sha256": values_sha,
    }
    return CausalExpertFeatureArtifact(
        anchor.outcome_key,
        policy.policy_id,
        candidate.candidate_id,
        context.context_id,
        order.order_id,
        exit_policy.exit_id,
        values,
        inputs_sha,
        values_sha,
        EXPERT_FEATURE_FORMULA_SHA256,
        canonical_sha256(definition),
    )


@dataclass(frozen=True, slots=True, order=True)
class ControlTriggerState:
    timeframe_seconds: int
    contract: str
    outcome_span_id: int
    segment_id: int
    start_ns: int
    end_ns: int
    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int
    atr_sum_ticks: int
    atr_denominator: int

    def __post_init__(self) -> None:
        if self.timeframe_seconds not in SUPPORTED_SIGNAL_TIMEFRAMES:
            raise SymbolicEngineError("control trigger timeframe is unsupported")
        if self.end_ns - self.start_ns != self.timeframe_seconds * ONE_SECOND_NS:
            raise SymbolicEngineError("control trigger interval differs")
        if self.atr_sum_ticks < 0 or self.atr_denominator != ATR_PERIOD:
            raise SymbolicEngineError("control trigger ATR differs")

    @property
    def key(self) -> tuple[int, int, str, int, int]:
        return (
            self.timeframe_seconds,
            self.end_ns,
            self.contract,
            self.outcome_span_id,
            self.segment_id,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "atr_denominator": self.atr_denominator,
            "atr_sum_ticks": self.atr_sum_ticks,
            "close_ticks": self.close_ticks,
            "contract": self.contract,
            "end_ns": self.end_ns,
            "high_ticks": self.high_ticks,
            "low_ticks": self.low_ticks,
            "open_ticks": self.open_ticks,
            "outcome_span_id": self.outcome_span_id,
            "segment_id": self.segment_id,
            "start_ns": self.start_ns,
            "timeframe_seconds": self.timeframe_seconds,
        }


@dataclass(frozen=True, slots=True, order=True)
class ControlOpportunity:
    source_date: date
    contract: str
    outcome_span_id: int
    segment_id: int
    anchor_ns: int
    trigger_start_ns: int
    trigger_open_ticks: int
    trigger_high_ticks: int
    trigger_low_ticks: int
    trigger_close_ticks: int
    atr_sum_ticks: int
    atr_denominator: int
    volatility_stratum: str
    regime_stratum: str
    utc_four_hour_bucket: int

    @property
    def lineage(self) -> tuple[str, int, int]:
        return self.contract, self.outcome_span_id, self.segment_id

    @property
    def group_key(self) -> tuple[date, str, int, int]:
        return self.source_date, self.contract, self.outcome_span_id, self.segment_id

    def anchor(
        self,
        direction: Direction,
        trigger_state: ControlTriggerState | None = None,
    ) -> AnchorRecord:
        trigger = trigger_state
        if trigger is not None and (
            trigger.contract != self.contract
            or trigger.outcome_span_id != self.outcome_span_id
            or trigger.segment_id != self.segment_id
            or trigger.end_ns > self.anchor_ns
        ):
            raise SymbolicEngineError("control trigger state and opportunity differ")
        return AnchorRecord(
            self.source_date,
            self.contract,
            self.outcome_span_id,
            self.segment_id,
            self.anchor_ns,
            direction,
            self.trigger_start_ns if trigger is None else trigger.start_ns,
            self.anchor_ns if trigger is None else trigger.end_ns,
            self.trigger_open_ticks if trigger is None else trigger.open_ticks,
            self.trigger_high_ticks if trigger is None else trigger.high_ticks,
            self.trigger_low_ticks if trigger is None else trigger.low_ticks,
            self.trigger_close_ticks if trigger is None else trigger.close_ticks,
            self.atr_sum_ticks if trigger is None else trigger.atr_sum_ticks,
            self.atr_denominator if trigger is None else trigger.atr_denominator,
            (
                ("control_opportunity", 1),
                (
                    "control_source_timeframe_seconds",
                    FIVE_MINUTES if trigger is None else trigger.timeframe_seconds,
                ),
            ),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "anchor_ns": self.anchor_ns,
            "atr_denominator": self.atr_denominator,
            "atr_sum_ticks": self.atr_sum_ticks,
            "contract": self.contract,
            "outcome_span_id": self.outcome_span_id,
            "regime_stratum": self.regime_stratum,
            "segment_id": self.segment_id,
            "source_date": self.source_date.isoformat(),
            "trigger_close_ticks": self.trigger_close_ticks,
            "trigger_high_ticks": self.trigger_high_ticks,
            "trigger_low_ticks": self.trigger_low_ticks,
            "trigger_open_ticks": self.trigger_open_ticks,
            "trigger_start_ns": self.trigger_start_ns,
            "utc_four_hour_bucket": self.utc_four_hour_bucket,
            "volatility_stratum": self.volatility_stratum,
        }


@dataclass(frozen=True, slots=True)
class ControlOpportunityLattice:
    opportunities: tuple[ControlOpportunity, ...]
    trigger_states: tuple[ControlTriggerState, ...]
    maximum_path_seconds: int
    artifact_sha256: str

    def __post_init__(self) -> None:
        if not self.opportunities:
            raise SymbolicEngineError("control opportunities must be non-empty")
        if self.opportunities != tuple(sorted(self.opportunities)):
            raise SymbolicEngineError("control opportunities are non-canonical")
        keys = [
            (item.contract, item.outcome_span_id, item.segment_id, item.anchor_ns)
            for item in self.opportunities
        ]
        if len(set(keys)) != len(keys) or self.maximum_path_seconds != 25_200:
            raise SymbolicEngineError("control opportunity lattice identity differs")
        trigger_keys = tuple(item.key for item in self.trigger_states)
        if (
            not self.trigger_states
            or trigger_keys != tuple(sorted(trigger_keys))
            or len(set(trigger_keys)) != len(trigger_keys)
        ):
            raise SymbolicEngineError("control trigger states are non-canonical")
        _require_sha(self.artifact_sha256, label="control opportunity artifact_sha256")
        if canonical_sha256(self.definition_dict()) != self.artifact_sha256:
            raise SymbolicEngineError("control opportunity lattice hash differs")

    def definition_dict(self) -> dict[str, object]:
        return {
            "maximum_path_seconds": self.maximum_path_seconds,
            "opportunities": [item.as_dict() for item in self.opportunities],
            "schema": MASK_SCHEMA,
            "trigger_states": [item.as_dict() for item in self.trigger_states],
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def _build_control_trigger_states(
    signal_bars_by_timeframe: Mapping[int, Sequence[BarWithOutcomeSpan]],
) -> tuple[ControlTriggerState, ...]:
    states: list[ControlTriggerState] = []
    for timeframe, rows in sorted(signal_bars_by_timeframe.items()):
        if timeframe not in SUPPORTED_SIGNAL_TIMEFRAMES:
            raise SymbolicEngineError("control trigger-state map has unsupported timeframe")
        values = tuple(rows)
        if not values:
            raise SymbolicEngineError("control trigger-state map has an empty timeframe")
        prior_identity: tuple[int, str, int, int] | None = None
        all_dates = tuple(sorted({item.bar.source_date for item in values}))
        date_rank = {item: rank for rank, item in enumerate(all_dates)}
        atr_window: deque[int] = deque(maxlen=ATR_PERIOD)
        running_atr = 0
        for index, wrapped in enumerate(values):
            identity = (
                wrapped.bar.start_ns,
                wrapped.bar.contract,
                wrapped.outcome_span_id,
                wrapped.bar.segment_id,
            )
            if wrapped.bar.timeframe_seconds != timeframe or (
                prior_identity is not None and identity <= prior_identity
            ):
                raise SymbolicEngineError("control trigger bars are non-canonical")
            prior_identity = identity
            continues = index > 0 and _indicator_continues(
                values[index - 1],
                wrapped,
                date_rank=date_rank,
            )
            if not continues:
                atr_window.clear()
                running_atr = 0
                continue
            prior_close = values[index - 1].bar.close_ticks
            bar = wrapped.bar
            true_range = max(
                bar.high_ticks - bar.low_ticks,
                abs(bar.high_ticks - prior_close),
                abs(bar.low_ticks - prior_close),
            )
            if len(atr_window) == ATR_PERIOD:
                running_atr -= atr_window[0]
            atr_window.append(true_range)
            running_atr += true_range
            if len(atr_window) != ATR_PERIOD:
                continue
            states.append(
                ControlTriggerState(
                    timeframe,
                    bar.contract,
                    wrapped.outcome_span_id,
                    bar.segment_id,
                    bar.start_ns,
                    bar.end_ns,
                    bar.open_ticks,
                    bar.high_ticks,
                    bar.low_ticks,
                    bar.close_ticks,
                    running_atr,
                    ATR_PERIOD,
                )
            )
    return tuple(sorted(states, key=lambda item: item.key))


def build_control_opportunity_lattice(
    five_minute_bars: Sequence[BarWithOutcomeSpan],
    *,
    decision_dates: Iterable[date],
    allowed_tail_end_ns: int,
    signal_bars_by_timeframe: Mapping[int, Sequence[BarWithOutcomeSpan]] | None = None,
) -> ControlOpportunityLattice:
    """Build the full-path, feature-only opportunity set used by both controls."""

    dates = _validated_dates(decision_dates)
    date_set = frozenset(dates)
    values = tuple(five_minute_bars)
    if not values:
        raise SymbolicEngineError("control opportunity lattice requires 5m bars")
    prior_identity: tuple[int, str, int, int] | None = None
    for wrapped in values:
        if (
            not isinstance(wrapped, BarWithOutcomeSpan)
            or wrapped.bar.timeframe_seconds != FIVE_MINUTES
        ):
            raise SymbolicEngineError("control opportunity lattice requires only 5m bars")
        identity = (
            wrapped.bar.start_ns,
            wrapped.bar.contract,
            wrapped.outcome_span_id,
            wrapped.bar.segment_id,
        )
        if prior_identity is not None and identity <= prior_identity:
            raise SymbolicEngineError("control opportunity 5m bars are non-canonical")
        prior_identity = identity
    _require_int(allowed_tail_end_ns, label="allowed_tail_end_ns", minimum=1)

    continuous = [False]
    for left, right in pairwise(values):
        continuous.append(
            left.bar.end_ns == right.bar.start_ns and _lineage(left) == _lineage(right)
        )
    true_ranges: list[int | None] = []
    atr_sums: list[int | None] = []
    atr_window: deque[int] = deque(maxlen=ATR_PERIOD)
    running_atr = 0
    volatility_history: list[int] = []
    volatility_strata: list[str] = []
    regime_strata: list[str] = []
    for index, wrapped in enumerate(values):
        if not continuous[index]:
            atr_window.clear()
            running_atr = 0
            volatility_history = []
            true_range = None
        else:
            prior_close = values[index - 1].bar.close_ticks
            bar = wrapped.bar
            true_range = max(
                bar.high_ticks - bar.low_ticks,
                abs(bar.high_ticks - prior_close),
                abs(bar.low_ticks - prior_close),
            )
        true_ranges.append(true_range)
        if true_range is None:
            atr_sums.append(None)
        else:
            if len(atr_window) == ATR_PERIOD:
                running_atr -= atr_window[0]
            atr_window.append(true_range)
            running_atr += true_range
            atr_sums.append(running_atr if len(atr_window) == ATR_PERIOD else None)

        recent_start = index - CONTROL_VOLATILITY_WINDOW + 1
        if recent_start < 0 or any(
            not continuous[position] for position in range(recent_start + 1, index + 1)
        ):
            volatility_strata.append("MISSING_BASE_VOLATILITY_HISTORY")
        else:
            volatility = sum(
                values[position].bar.high_ticks - values[position].bar.low_ticks
                for position in range(recent_start, index + 1)
            )
            if len(volatility_history) < CONTROL_VOLATILITY_HISTORY:
                volatility_strata.append("MISSING_PRIOR_20_HISTORY")
            else:
                reference = volatility_history[-CONTROL_VOLATILITY_HISTORY:]
                rank = sum(item <= volatility for item in reference)
                quartile = min(3, 4 * rank // (len(reference) + 1))
                volatility_strata.append(f"VOL_Q{quartile + 1}")
            volatility_history.append(volatility)

        regime_start = index - CONTROL_REGIME_WINDOW
        if regime_start < 0 or any(
            not continuous[position] for position in range(regime_start + 1, index + 1)
        ):
            regime_strata.append("MISSING_REGIME_HISTORY")
        else:
            closes = [
                values[position].bar.close_ticks for position in range(regime_start, index + 1)
            ]
            travelled = sum(abs(right - left) for left, right in pairwise(closes))
            delta = closes[-1] - closes[0]
            if travelled == 0 or 3 * abs(delta) <= travelled:
                regime_strata.append("RANGE")
            elif delta > 0:
                regime_strata.append("TREND_UP")
            else:
                regime_strata.append("TREND_DOWN")

    required_bars = 25_200 // FIVE_MINUTES
    opportunities: list[ControlOpportunity] = []
    for index, wrapped in enumerate(values):
        bar = wrapped.bar
        atr_sum = atr_sums[index]
        terminal_index = index + required_bars
        if (
            bar.source_date not in date_set
            or atr_sum is None
            or terminal_index >= len(values)
            or bar.end_ns + 25_200 * ONE_SECOND_NS > allowed_tail_end_ns
        ):
            continue
        lineage = _lineage(wrapped)
        expected_start = bar.end_ns
        structurally_complete = True
        for future_index in range(index + 1, terminal_index + 1):
            future = values[future_index]
            if future.bar.start_ns != expected_start or _lineage(future) != lineage:
                structurally_complete = False
                break
            expected_start = future.bar.end_ns
        if not structurally_complete or expected_start != bar.end_ns + 25_200 * ONE_SECOND_NS:
            continue
        hour = datetime.fromtimestamp(bar.end_ns // ONE_SECOND_NS, tz=UTC).hour
        opportunities.append(
            ControlOpportunity(
                bar.source_date,
                bar.contract,
                wrapped.outcome_span_id,
                bar.segment_id,
                bar.end_ns,
                bar.start_ns,
                bar.open_ticks,
                bar.high_ticks,
                bar.low_ticks,
                bar.close_ticks,
                atr_sum,
                ATR_PERIOD,
                volatility_strata[index],
                regime_strata[index],
                hour // CONTROL_TIME_BUCKET_HOURS,
            )
        )
    canonical = tuple(sorted(opportunities))
    source_bars = (
        {FIVE_MINUTES: values}
        if signal_bars_by_timeframe is None
        else dict(signal_bars_by_timeframe)
    )
    if FIVE_MINUTES not in source_bars or tuple(source_bars[FIVE_MINUTES]) != values:
        raise SymbolicEngineError("control trigger-state 5m bars differ from opportunity bars")
    trigger_states = _build_control_trigger_states(source_bars)
    definition = {
        "maximum_path_seconds": 25_200,
        "opportunities": [item.as_dict() for item in canonical],
        "schema": MASK_SCHEMA,
        "trigger_states": [item.as_dict() for item in trigger_states],
    }
    return ControlOpportunityLattice(
        canonical,
        trigger_states,
        25_200,
        canonical_sha256(definition),
    )


@dataclass(frozen=True, slots=True)
class CircularControlPair:
    real_anchor_key: tuple[str, int, int, int, Direction]
    control_anchor_key: tuple[str, int, int, int, Direction]
    preserved_lag_ns: int

    def __post_init__(self) -> None:
        _validate_control_pair_keys(
            self.real_anchor_key,
            self.control_anchor_key,
            allow_identity=True,
        )
        _require_int(self.preserved_lag_ns, label="circular preserved lag", minimum=0)

    def as_dict(self) -> dict[str, object]:
        return {
            "control_anchor_key": list(self.control_anchor_key),
            "preserved_lag_ns": self.preserved_lag_ns,
            "real_anchor_key": list(self.real_anchor_key),
        }


@dataclass(frozen=True, slots=True)
class MatchedControlPair:
    real_anchor_key: tuple[str, int, int, int, Direction]
    control_anchor_key: tuple[str, int, int, int, Direction]
    fallback_level: int
    preserved_lag_ns: int = 0

    def __post_init__(self) -> None:
        _validate_control_pair_keys(
            self.real_anchor_key,
            self.control_anchor_key,
            allow_identity=False,
        )
        _require_int(self.fallback_level, label="matched fallback level", minimum=0)
        if self.fallback_level > 6:
            raise SymbolicEngineError("matched fallback level must be in 0..6")
        _require_int(self.preserved_lag_ns, label="matched preserved lag", minimum=0)

    def as_dict(self) -> dict[str, object]:
        return {
            "control_anchor_key": list(self.control_anchor_key),
            "fallback_level": self.fallback_level,
            "preserved_lag_ns": self.preserved_lag_ns,
            "real_anchor_key": list(self.real_anchor_key),
        }


@dataclass(frozen=True, slots=True)
class FrozenControlMasks:
    stage_key: str
    real: PolicyMask
    circular: PolicyMask | None
    matched: PolicyMask | None
    circular_pairs: tuple[CircularControlPair, ...]
    matched_pairs: tuple[MatchedControlPair, ...]
    trigger_timeframe_seconds: int
    evaluable_daily_counts: tuple[tuple[date, int], ...]
    evaluable_reporting_group_counts: tuple[tuple[str, int], ...]
    sample_eligible: bool
    ineligibility_reason: str | None
    opportunity_lattice_sha256: str
    seed_sha256s: tuple[str, str]
    commitment_sha256: str

    def __post_init__(self) -> None:
        if not self.stage_key:
            raise SymbolicEngineError("control-mask stage key is empty")
        _require_sha(self.opportunity_lattice_sha256, label="control opportunity lattice SHA")
        for value in self.seed_sha256s:
            _require_sha(value, label="control seed SHA")
        if self.sample_eligible != (self.circular is not None and self.matched is not None):
            raise SymbolicEngineError("control-mask eligibility differs")
        if self.sample_eligible == (self.ineligibility_reason is not None):
            raise SymbolicEngineError("control-mask ineligibility reason differs")
        expected_daily: dict[date, int] = defaultdict(int)
        for record in self.real.records:
            expected_daily[record.source_date] += 1
        if (
            self.evaluable_daily_counts != tuple(sorted(expected_daily.items()))
            or self.evaluable_reporting_group_counts
            != tuple(sorted(self.evaluable_reporting_group_counts))
            or len({key for key, _count in self.evaluable_reporting_group_counts})
            != len(self.evaluable_reporting_group_counts)
            or any(
                not key or isinstance(count, bool) or not isinstance(count, int) or count < 1
                for key, count in self.evaluable_reporting_group_counts
            )
            or sum(count for _key, count in self.evaluable_reporting_group_counts)
            > self.real.support_count
        ):
            raise SymbolicEngineError("control evaluable count evidence differs")
        if self.sample_eligible and (
            self.circular.support_count != self.real.support_count
            or self.matched.support_count != self.real.support_count
        ):
            raise SymbolicEngineError("control masks are not cardinality preserving")
        candidate = _candidate_lookup().get(self.real.policy.base_candidate_id)
        if (
            candidate is None
            or self.trigger_timeframe_seconds != candidate.trigger_timeframe_seconds
        ):
            raise SymbolicEngineError("control trigger timeframe differs from policy")
        if self.sample_eligible and (
            len(self.circular_pairs) != self.real.support_count
            or len(self.matched_pairs) != self.real.support_count
        ):
            raise SymbolicEngineError("control pairing evidence count differs")
        if not self.sample_eligible and (
            self.circular is not None
            or self.matched is not None
            or self.circular_pairs
            or self.matched_pairs
        ):
            raise SymbolicEngineError("ineligible controls retain masks or pairing evidence")
        if self.sample_eligible:
            if self.circular is None or self.matched is None:  # pragma: no cover - guarded above
                raise SymbolicEngineError("eligible controls lost masks")
            if (
                self.circular.policy != self.real.policy
                or self.matched.policy != self.real.policy
                or self.circular.family != self.real.family
                or self.matched.family != self.real.family
                or self.circular.direction != self.real.direction
                or self.matched.direction != self.real.direction
            ):
                raise SymbolicEngineError("control masks do not preserve policy semantics")

            real_by_key = {item.outcome_key: item for item in self.real.records}
            circular_by_key = {item.outcome_key: item for item in self.circular.records}
            matched_by_key = {item.outcome_key: item for item in self.matched.records}

            def validate_pairs(
                pairs: Sequence[CircularControlPair | MatchedControlPair],
                control_by_key: Mapping[tuple[str, int, int, int, Direction], AnchorRecord],
            ) -> None:
                real_keys = tuple(item.real_anchor_key for item in pairs)
                control_keys = tuple(item.control_anchor_key for item in pairs)
                if (
                    real_keys != tuple(sorted(real_keys))
                    or len(set(real_keys)) != len(real_keys)
                    or len(set(control_keys)) != len(control_keys)
                    or set(real_keys) != set(real_by_key)
                    or set(control_keys) != set(control_by_key)
                ):
                    raise SymbolicEngineError("control pairing evidence is not a bijection")
                for pair in pairs:
                    real_record = real_by_key[pair.real_anchor_key]
                    control_record = control_by_key[pair.control_anchor_key]
                    if (
                        pair.preserved_lag_ns != real_record.anchor_ns - real_record.trigger_end_ns
                        or pair.preserved_lag_ns
                        != control_record.anchor_ns - control_record.trigger_end_ns
                    ):
                        raise SymbolicEngineError("control pair does not preserve trigger lag")

            validate_pairs(self.circular_pairs, circular_by_key)
            validate_pairs(self.matched_pairs, matched_by_key)

            def counts(mask: PolicyMask) -> Mapping[tuple[date, str, int, int], int]:
                output: dict[tuple[date, str, int, int], int] = defaultdict(int)
                for record in mask.records:
                    output[
                        record.source_date,
                        record.contract,
                        record.outcome_span_id,
                        record.segment_id,
                    ] += 1
                return output

            real_counts = counts(self.real)
            if counts(self.circular) != real_counts or counts(self.matched) != real_counts:
                raise SymbolicEngineError("controls do not preserve every causal opportunity group")
        _require_sha(self.commitment_sha256, label="control-mask commitment_sha256")
        if canonical_sha256(self.definition_dict()) != self.commitment_sha256:
            raise SymbolicEngineError("control-mask commitment differs")

    def definition_dict(self) -> dict[str, object]:
        return {
            "circular": None if self.circular is None else self.circular.as_dict(),
            "circular_pairs": [item.as_dict() for item in self.circular_pairs],
            "ineligibility_reason": self.ineligibility_reason,
            "matched": None if self.matched is None else self.matched.as_dict(),
            "matched_pairs": [item.as_dict() for item in self.matched_pairs],
            "opportunity_lattice_sha256": self.opportunity_lattice_sha256,
            "evaluable_daily_counts": [
                {"decision_date": day.isoformat(), "signal_count": count}
                for day, count in self.evaluable_daily_counts
            ],
            "evaluable_reporting_group_counts": [
                {"group_key": key, "signal_count": count}
                for key, count in self.evaluable_reporting_group_counts
            ],
            "real": self.real.as_dict(),
            "sample_eligible": self.sample_eligible,
            "schema": MASK_SCHEMA,
            "seed_sha256s": list(self.seed_sha256s),
            "stage_key": self.stage_key,
            "trigger_timeframe_seconds": self.trigger_timeframe_seconds,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "commitment_sha256": self.commitment_sha256}


def _validate_control_pair_keys(
    real_anchor_key: tuple[str, int, int, int, Direction],
    control_anchor_key: tuple[str, int, int, int, Direction],
    *,
    allow_identity: bool,
) -> None:
    for label, key in (("real", real_anchor_key), ("control", control_anchor_key)):
        if (
            not isinstance(key, tuple)
            or len(key) != 5
            or not isinstance(key[0], str)
            or not key[0]
            or isinstance(key[1], bool)
            or not isinstance(key[1], int)
            or key[1] < 1
            or isinstance(key[2], bool)
            or not isinstance(key[2], int)
            or key[2] < 1
            or isinstance(key[3], bool)
            or not isinstance(key[3], int)
            or key[3] < 0
            or key[4] not in ("LONG", "SHORT")
        ):
            raise SymbolicEngineError(f"{label} control pair anchor key is invalid")
    if real_anchor_key[4] != control_anchor_key[4] or (
        not allow_identity and real_anchor_key == control_anchor_key
    ):
        raise SymbolicEngineError("control pair identity/direction policy differs")


def _control_seed(stage_key: str, policy_id: str, role: str) -> str:
    return canonical_sha256(
        {
            "master_seed": MASTER_NULL_SEED,
            "policy_id": policy_id,
            "purpose": role,
            "stage_key": stage_key,
        }
    )


def _control_atr_scale_stratum(atr_sum_ticks: int, atr_denominator: int) -> str:
    average = max(1, _round_half_even(atr_sum_ticks, atr_denominator))
    return f"ATR_LOG2_{average.bit_length() - 1}"


def _control_signed_atr_bin(value_ticks: int, atr_sum_ticks: int) -> str:
    scaled = value_ticks * ATR_PERIOD
    boundaries = (
        (-2, "LE_NEG_2"),
        (-1, "NEG_2_TO_NEG_1"),
        (0, "NEG_1_TO_0"),
        (1, "ZERO_TO_1"),
        (2, "ONE_TO_2"),
    )
    for boundary, label in boundaries:
        if scaled <= boundary * atr_sum_ticks:
            return label
    return "GT_2"


def _control_geometry_stratum(anchor: AnchorRecord, execution_close_ticks: int) -> str:
    sign = _direction_sign(anchor.direction)
    extreme = anchor.trigger_high_ticks if sign > 0 else anchor.trigger_low_ticks
    stop_distance = sign * (extreme - execution_close_ticks)
    close_drift = sign * (execution_close_ticks - anchor.trigger_close_ticks)
    return "__".join(
        (
            _control_atr_scale_stratum(anchor.atr_sum_ticks, anchor.atr_denominator),
            f"STOP_{_control_signed_atr_bin(stop_distance, anchor.atr_sum_ticks)}",
            f"DRIFT_{_control_signed_atr_bin(close_drift, anchor.atr_sum_ticks)}",
        )
    )


def _deterministic_complete_maximum_matching(
    adjacency: Mapping[int, Sequence[int]],
    ordered_sources: Sequence[int],
) -> Mapping[int, int] | None:
    """Return a complete deterministic bipartite matching, or prove none exists."""

    sources = tuple(ordered_sources)
    if sources != tuple(dict.fromkeys(sources)) or set(sources) != set(adjacency):
        raise SymbolicEngineError("maximum-matching source order differs from adjacency")
    canonical_adjacency: dict[int, tuple[int, ...]] = {}
    for source in sources:
        targets = tuple(adjacency[source])
        if targets != tuple(dict.fromkeys(targets)):
            raise SymbolicEngineError("maximum-matching adjacency duplicates a target")
        canonical_adjacency[source] = targets
    control_to_real: dict[int, int] = {}
    real_to_control: dict[int, int] = {}

    def augment(root_real: int) -> bool:
        queue: deque[int] = deque((root_real,))
        visited_real = {root_real}
        parent_control: dict[int, int] = {}
        free_control: int | None = None
        while queue and free_control is None:
            current_real = queue.popleft()
            for control_index in canonical_adjacency[current_real]:
                if control_index in parent_control:
                    continue
                parent_control[control_index] = current_real
                prior_real = control_to_real.get(control_index)
                if prior_real is None:
                    free_control = control_index
                    break
                if prior_real not in visited_real:
                    visited_real.add(prior_real)
                    queue.append(prior_real)
        if free_control is None:
            return False
        control_index = free_control
        while True:
            current_real = parent_control[control_index]
            previous_control = real_to_control.get(current_real)
            real_to_control[current_real] = control_index
            control_to_real[control_index] = current_real
            if previous_control is None:
                break
            control_index = previous_control
        return True

    for source in sources:
        if not augment(source):
            return None
    return real_to_control


def freeze_feature_control_masks(
    stage_key: str,
    real: PolicyMask,
    opportunity_lattice: ControlOpportunityLattice,
    *,
    reporting_group_by_date: Mapping[date, str],
    master_seed: str = MASTER_NULL_SEED,
) -> FrozenControlMasks:
    """Freeze cardinality-preserving circular/matched controls before any 1s loader."""

    if not stage_key or master_seed != MASTER_NULL_SEED:
        raise SymbolicEngineError("control masks require the exact stage key and master seed")
    if not reporting_group_by_date:
        raise SymbolicEngineError("control masks require reporting groups")
    candidate = _candidate_lookup().get(real.policy.base_candidate_id)
    if candidate is None:
        raise SymbolicEngineError("control mask policy has no catalog candidate")
    trigger_timeframe = candidate.trigger_timeframe_seconds
    opportunities = opportunity_lattice.opportunities
    direction = real.direction
    opportunity_anchors = tuple(item.anchor(direction) for item in opportunities)
    position_by_key = {item.outcome_key: index for index, item in enumerate(opportunity_anchors)}
    try:
        real_positions = [position_by_key[item.outcome_key] for item in real.records]
    except KeyError:
        real_positions = []
        missing_real = True
    else:
        missing_real = len(set(real_positions)) != len(real_positions)
    circular_seed = _control_seed(stage_key, real.policy.policy_id, "CIRCULAR")
    matched_seed = _control_seed(stage_key, real.policy.policy_id, "MATCHED")
    daily_counts: dict[date, int] = defaultdict(int)
    reporting_counts: dict[str, int] = defaultdict(int)
    for record in real.records:
        daily_counts[record.source_date] += 1
        group = reporting_group_by_date.get(record.source_date)
        if group is not None:
            reporting_counts[group] += 1

    trigger_by_key = {item.key: item for item in opportunity_lattice.trigger_states}
    real_record_by_position = {
        position_by_key[item.outcome_key]: item
        for item in real.records
        if item.outcome_key in position_by_key
    }

    def pseudo_anchor(record: AnchorRecord, control_index: int) -> AnchorRecord | None:
        opportunity = opportunities[control_index]
        lag_ns = record.anchor_ns - record.trigger_end_ns
        if lag_ns < 0:
            return None
        trigger = trigger_by_key.get(
            (
                trigger_timeframe,
                opportunity.anchor_ns - lag_ns,
                opportunity.contract,
                opportunity.outcome_span_id,
                opportunity.segment_id,
            )
        )
        return None if trigger is None else opportunity.anchor(direction, trigger)

    circular_mask: PolicyMask | None = None
    matched_mask: PolicyMask | None = None
    circular_pair_rows: tuple[CircularControlPair, ...] = ()
    matched_pair_rows: tuple[MatchedControlPair, ...] = ()
    reason: str | None = None
    if missing_real:
        reason = "REAL_MASK_OUTSIDE_CONTROL_OPPORTUNITY_LATTICE"
    else:
        real_set = set(real_positions)
        by_group: dict[tuple[date, str, int, int], list[int]] = defaultdict(list)
        for index, opportunity in enumerate(opportunities):
            by_group[opportunity.group_key].append(index)
        circular_assignments: list[tuple[int, int, AnchorRecord]] = []
        for group, positions in sorted(by_group.items()):
            source_positions = [position for position in positions if position in real_set]
            if not source_positions:
                continue
            if len(source_positions) == len(positions) or len(positions) < 2:
                circular_assignments.extend(
                    (
                        source_position,
                        source_position,
                        real_record_by_position[source_position],
                    )
                    for source_position in source_positions
                )
                continue
            start = (
                int(
                    canonical_sha256(
                        {
                            "group": [str(item) for item in group],
                            "seed": circular_seed,
                        }
                    ),
                    16,
                )
                % (len(positions) - 1)
                + 1
            )
            local_index = {position: index for index, position in enumerate(positions)}
            selected: list[tuple[int, int, AnchorRecord]] | None = None
            for offset_step in range(len(positions) - 1):
                offset = (start - 1 + offset_step) % (len(positions) - 1) + 1
                proposed: list[tuple[int, int, AnchorRecord]] = []
                for source_position in source_positions:
                    target_position = positions[
                        (local_index[source_position] + offset) % len(positions)
                    ]
                    pseudo = pseudo_anchor(
                        real_record_by_position[source_position], target_position
                    )
                    if pseudo is None:
                        proposed = []
                        break
                    proposed.append((source_position, target_position, pseudo))
                if proposed:
                    selected = proposed
                    break
            if selected is None:
                reason = "CIRCULAR_NATIVE_TRIGGER_GEOMETRY_UNAVAILABLE"
                break
            circular_assignments.extend(selected)
        if reason is None:
            circular_records = [item[2] for item in circular_assignments]
            circular_mask = PolicyMask.from_records(
                real.policy,
                real.family,
                direction,
                circular_records,
            )
            if (
                circular_mask.support_count != real.support_count
                or circular_mask.mask_sha256 == real.mask_sha256
            ):
                reason = "NONIDENTITY_CIRCULAR_CONTROL_IMPOSSIBLE"
                circular_mask = None
            else:
                circular_pair_rows = tuple(
                    sorted(
                        (
                            CircularControlPair(
                                real_record_by_position[source].outcome_key,
                                pseudo.outcome_key,
                                real_record_by_position[source].anchor_ns
                                - real_record_by_position[source].trigger_end_ns,
                            )
                            for source, _target, pseudo in circular_assignments
                        ),
                        key=lambda item: item.real_anchor_key,
                    )
                )

    if reason is None:
        real_set = set(real_positions)
        available = set(range(len(opportunities))) - real_set
        available_by_group: dict[tuple[date, str, int, int], tuple[int, ...]] = {
            group: tuple(index for index in positions if index in available)
            for group, positions in by_group.items()
        }

        pseudo_cache: dict[tuple[int, int], AnchorRecord | None] = {}

        def cached_pseudo(real_index: int, control_index: int) -> AnchorRecord | None:
            key = real_index, control_index
            if key not in pseudo_cache:
                pseudo_cache[key] = pseudo_anchor(
                    real_record_by_position[real_index], control_index
                )
            return pseudo_cache[key]

        def candidate_levels(real_index: int) -> tuple[list[int], ...]:
            item = opportunities[real_index]
            group_pool = [
                index
                for index in available_by_group.get(item.group_key, ())
                if cached_pseudo(real_index, index) is not None
            ]

            def distance(index: int) -> int:
                direct = abs(opportunities[index].utc_four_hour_bucket - item.utc_four_hour_bucket)
                return min(direct, 6 - direct)

            state_exact = [
                index
                for index in group_pool
                if opportunities[index].volatility_stratum == item.volatility_stratum
                and opportunities[index].regime_stratum == item.regime_stratum
            ]
            real_geometry = _control_geometry_stratum(
                real_record_by_position[real_index], item.trigger_close_ticks
            )
            geometry_exact = [
                index
                for index in state_exact
                if _control_geometry_stratum(
                    cached_pseudo(real_index, index),
                    opportunities[index].trigger_close_ticks,
                )
                == real_geometry
            ]
            return (
                [index for index in geometry_exact if distance(index) == 0],
                [index for index in geometry_exact if distance(index) == 1],
                geometry_exact,
                [index for index in state_exact if distance(index) == 0],
                [index for index in state_exact if distance(index) <= 1],
                state_exact,
                group_pool,
            )

        edge_level_by_real: dict[int, dict[int, int]] = {}
        adjacency: dict[int, tuple[int, ...]] = {}
        for real_index in real_positions:
            edge_levels: dict[int, int] = {}
            for level, candidates in enumerate(candidate_levels(real_index)):
                for control_index in candidates:
                    edge_levels.setdefault(control_index, level)
            edge_level_by_real[real_index] = edge_levels
            adjacency[real_index] = tuple(
                sorted(
                    edge_levels,
                    key=lambda control_index: (
                        edge_levels[control_index],
                        canonical_sha256(
                            {
                                "control_index": control_index,
                                "fallback_level": edge_levels[control_index],
                                "real_index": real_index,
                                "seed": matched_seed,
                            }
                        ),
                    ),
                )
            )

        ordered_real = sorted(
            real_positions,
            key=lambda index: (
                len(adjacency[index]),
                canonical_sha256({"real_index": index, "seed": matched_seed}),
            ),
        )
        real_to_control = _deterministic_complete_maximum_matching(adjacency, ordered_real)
        if real_to_control is None:
            reason = "MATCHED_COMPLETE_MAXIMUM_MATCHING_IMPOSSIBLE"
        else:
            if len(real_to_control) != len(real_positions):  # pragma: no cover - helper invariant
                raise SymbolicEngineError("matched maximum matching cardinality differs")
            selected_pairs: list[MatchedControlPair] = []
            selected_matched_anchors: list[AnchorRecord] = []
            for real_index in real_positions:
                choice = real_to_control[real_index]
                selected_anchor = cached_pseudo(real_index, choice)
                if selected_anchor is None:  # pragma: no cover - adjacency filtered it
                    raise SymbolicEngineError("matched control lost native trigger geometry")
                selected_matched_anchors.append(selected_anchor)
                real_record = real_record_by_position[real_index]
                selected_pairs.append(
                    MatchedControlPair(
                        real_record.outcome_key,
                        selected_anchor.outcome_key,
                        edge_level_by_real[real_index][choice],
                        real_record.anchor_ns - real_record.trigger_end_ns,
                    )
                )
            matched_mask = PolicyMask.from_records(
                real.policy,
                real.family,
                direction,
                selected_matched_anchors,
            )
            matched_pair_rows = tuple(sorted(selected_pairs, key=lambda item: item.real_anchor_key))

    sample_eligible = reason is None
    if not sample_eligible:
        circular_mask = None
        matched_mask = None
        circular_pair_rows = ()
        matched_pair_rows = ()
    definition = {
        "circular": None if circular_mask is None else circular_mask.as_dict(),
        "circular_pairs": [item.as_dict() for item in circular_pair_rows],
        "ineligibility_reason": reason,
        "matched": None if matched_mask is None else matched_mask.as_dict(),
        "matched_pairs": [item.as_dict() for item in matched_pair_rows],
        "opportunity_lattice_sha256": opportunity_lattice.artifact_sha256,
        "evaluable_daily_counts": [
            {"decision_date": day.isoformat(), "signal_count": count}
            for day, count in sorted(daily_counts.items())
        ],
        "evaluable_reporting_group_counts": [
            {"group_key": key, "signal_count": count}
            for key, count in sorted(reporting_counts.items())
        ],
        "real": real.as_dict(),
        "sample_eligible": sample_eligible,
        "schema": MASK_SCHEMA,
        "seed_sha256s": [circular_seed, matched_seed],
        "stage_key": stage_key,
        "trigger_timeframe_seconds": trigger_timeframe,
    }
    return FrozenControlMasks(
        stage_key,
        real,
        circular_mask,
        matched_mask,
        circular_pair_rows,
        matched_pair_rows,
        trigger_timeframe,
        tuple(sorted(daily_counts.items())),
        tuple(sorted(reporting_counts.items())),
        sample_eligible,
        reason,
        opportunity_lattice.artifact_sha256,
        (circular_seed, matched_seed),
        canonical_sha256(definition),
    )


@dataclass(frozen=True, slots=True)
class FrozenControlRuleExitSchedules:
    real: RuleExitSchedule
    circular: RuleExitSchedule | None
    matched: RuleExitSchedule | None
    commitment_sha256: str

    def definition_dict(self) -> dict[str, object]:
        return {
            "circular": None if self.circular is None else self.circular.as_dict(),
            "matched": None if self.matched is None else self.matched.as_dict(),
            "real": self.real.as_dict(),
            "schema": PATH_OUTCOME_SCHEMA,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "commitment_sha256": self.commitment_sha256}


def build_control_rule_exit_schedules(
    stage: SymbolicStage,
    policy: AnchorPolicy,
    controls: FrozenControlMasks,
) -> FrozenControlRuleExitSchedules:
    """Build distinct feature-only RULE schedules for each frozen mask world."""

    if controls.real.policy.policy_id != policy.policy_id:
        raise SymbolicEngineError("control rule schedules received a different anchor policy")
    real = build_rule_exit_schedule(stage, policy, controls.real.records)
    circular = (
        None
        if controls.circular is None
        else build_rule_exit_schedule(stage, policy, controls.circular.records)
    )
    matched = (
        None
        if controls.matched is None
        else build_rule_exit_schedule(stage, policy, controls.matched.records)
    )
    definition = {
        "circular": None if circular is None else circular.as_dict(),
        "matched": None if matched is None else matched.as_dict(),
        "real": real.as_dict(),
        "schema": PATH_OUTCOME_SCHEMA,
    }
    return FrozenControlRuleExitSchedules(
        real,
        circular,
        matched,
        canonical_sha256(definition),
    )


@dataclass(frozen=True, slots=True)
class OneSecondPath:
    """Loader-verified local coverage runs for exactly one immutable lineage."""

    contract: str
    outcome_span_id: int
    segment_id: int
    coverage_start_ns: int
    coverage_end_ns: int
    structural_five_minute_bars: tuple[BarWithOutcomeSpan, ...]
    coverage_intervals: tuple[tuple[int, int], ...]
    rows: tuple[BarWithOutcomeSpan, ...]
    starts: tuple[int, ...]
    ends: tuple[int, ...]

    def __post_init__(self) -> None:
        if not self.contract:
            raise SymbolicEngineError("one-second path contract is empty")
        _require_int(self.outcome_span_id, label="path outcome_span_id", minimum=1)
        _require_int(self.segment_id, label="path segment_id", minimum=1)
        if (
            self.coverage_start_ns < 0
            or self.coverage_start_ns % ONE_SECOND_NS
            or self.coverage_end_ns <= self.coverage_start_ns
            or self.coverage_end_ns % ONE_SECOND_NS
        ):
            raise SymbolicEngineError("one-second coverage interval is invalid")
        if not self.structural_five_minute_bars:
            raise SymbolicEngineError("one-second path lacks structural 5m coverage proof")
        derived_intervals: list[tuple[int, int]] = []
        interval_start: int | None = None
        prior_end: int | None = None
        for wrapped in self.structural_five_minute_bars:
            if (
                wrapped.bar.timeframe_seconds != FIVE_MINUTES
                or _lineage(wrapped) != (self.contract, self.outcome_span_id, self.segment_id)
                or (prior_end is not None and wrapped.bar.start_ns < prior_end)
            ):
                raise SymbolicEngineError("structural 5m path overlaps or crosses lineage")
            if interval_start is None:
                interval_start = wrapped.bar.start_ns
            elif wrapped.bar.start_ns != prior_end:
                if prior_end is None:  # pragma: no cover - guarded by interval_start
                    raise SymbolicEngineError("structural interval state differs")
                derived_intervals.append((interval_start, prior_end))
                interval_start = wrapped.bar.start_ns
            prior_end = wrapped.bar.end_ns
        if interval_start is None or prior_end is None:  # pragma: no cover - non-empty guard
            raise SymbolicEngineError("structural interval state is empty")
        derived_intervals.append((interval_start, prior_end))
        if (
            tuple(derived_intervals) != self.coverage_intervals
            or self.coverage_start_ns != derived_intervals[0][0]
            or self.coverage_end_ns != derived_intervals[-1][1]
        ):
            raise SymbolicEngineError("structural 5m coverage intervals differ")
        if not self.rows or len(self.rows) != len(self.starts) or len(self.rows) != len(self.ends):
            raise SymbolicEngineError("one-second path shape differs")
        prior_start = -1
        interval_index = 0
        for row, start, end in zip(self.rows, self.starts, self.ends, strict=True):
            if not isinstance(row, BarWithOutcomeSpan) or row.bar.timeframe_seconds != 1:
                raise SymbolicEngineError("one-second path contains a non-1s bar")
            if _lineage(row) != (self.contract, self.outcome_span_id, self.segment_id):
                raise SymbolicEngineError("one-second path crosses lineage")
            if start != row.bar.start_ns or end != row.bar.end_ns:
                raise SymbolicEngineError("one-second path index differs from rows")
            while (
                interval_index < len(self.coverage_intervals)
                and start >= self.coverage_intervals[interval_index][1]
            ):
                interval_index += 1
            if (
                start <= prior_start
                or interval_index >= len(self.coverage_intervals)
                or start < self.coverage_intervals[interval_index][0]
                or end > self.coverage_intervals[interval_index][1]
            ):
                raise SymbolicEngineError("one-second rows are non-canonical or outside coverage")
            prior_start = start

    @classmethod
    def from_rows(
        cls,
        rows: Sequence[BarWithOutcomeSpan],
        *,
        coverage_start_ns: int,
        coverage_end_ns: int,
        structural_five_minute_bars: Sequence[BarWithOutcomeSpan],
    ) -> OneSecondPath:
        values = tuple(rows)
        if not values:
            raise SymbolicEngineError("cannot infer a one-second lineage from no rows")
        first = values[0]
        structural = tuple(structural_five_minute_bars)
        intervals: list[tuple[int, int]] = []
        for wrapped in structural:
            if not intervals or wrapped.bar.start_ns != intervals[-1][1]:
                intervals.append((wrapped.bar.start_ns, wrapped.bar.end_ns))
            else:
                intervals[-1] = intervals[-1][0], wrapped.bar.end_ns
        return cls(
            first.bar.contract,
            first.outcome_span_id,
            first.bar.segment_id,
            coverage_start_ns,
            coverage_end_ns,
            structural,
            tuple(intervals),
            values,
            tuple(item.bar.start_ns for item in values),
            tuple(item.bar.end_ns for item in values),
        )

    @property
    def lineage(self) -> tuple[str, int, int]:
        return self.contract, self.outcome_span_id, self.segment_id

    def coverage_interval_at(self, instant_ns: int) -> tuple[int, int] | None:
        """Return the half-open structural run containing an execution instant."""

        index = bisect_right(self.coverage_intervals, (instant_ns, 10**30)) - 1
        if index < 0:
            return None
        start_ns, end_ns = self.coverage_intervals[index]
        return (start_ns, end_ns) if start_ns <= instant_ns < end_ns else None

    def structurally_covers(self, start_ns: int, end_ns: int) -> bool:
        """Whether one local run covers the full half-open execution window."""

        interval = self.coverage_interval_at(start_ns)
        return interval is not None and start_ns <= end_ns <= interval[1]


def build_reference_outcome_surfaces(
    anchors: Iterable[AnchorRecord],
    paths: Sequence[OneSecondPath],
) -> tuple[ReferenceOutcomeSurface, ...]:
    """Build exact Stage-A market/terminal surfaces from verified sparse 1s paths."""

    path_by_lineage = {item.lineage: item for item in paths}
    if not path_by_lineage or len(path_by_lineage) != len(tuple(paths)):
        raise SymbolicEngineError("reference paths must be non-empty unique lineages")
    canonical_anchors = tuple(
        sorted(
            set(anchors),
            key=lambda item: (
                item.anchor_ns,
                item.contract,
                item.outcome_span_id,
                item.segment_id,
                item.direction,
            ),
        )
    )
    if not canonical_anchors:
        raise SymbolicEngineError("reference surfaces require at least one anchor")
    payloads: dict[
        int,
        dict[tuple[str, int, int, int, Direction], int],
    ] = {horizon: {} for horizon in REFERENCE_HORIZONS_SECONDS}
    censored: dict[int, set[tuple[str, int, int, int, Direction]]] = {
        horizon: set() for horizon in REFERENCE_HORIZONS_SECONDS
    }
    for anchor in canonical_anchors:
        lineage = anchor.contract, anchor.outcome_span_id, anchor.segment_id
        path = path_by_lineage.get(lineage)
        if path is None:
            for horizon in REFERENCE_HORIZONS_SECONDS:
                censored[horizon].add(anchor.outcome_key)
            continue
        interval = path.coverage_interval_at(anchor.anchor_ns)
        if interval is None:
            for horizon in REFERENCE_HORIZONS_SECONDS:
                censored[horizon].add(anchor.outcome_key)
            continue
        entry_index = bisect_left(path.starts, anchor.anchor_ns)
        entry_deadline_ns = anchor.anchor_ns + FIVE_MINUTES * ONE_SECOND_NS
        if entry_index >= len(path.rows) or path.rows[entry_index].bar.start_ns >= min(
            entry_deadline_ns, interval[1]
        ):
            for horizon in REFERENCE_HORIZONS_SECONDS:
                censored[horizon].add(anchor.outcome_key)
            continue
        entry_reference = path.rows[entry_index].bar.open_ticks
        sign = _direction_sign(anchor.direction)
        for horizon in REFERENCE_HORIZONS_SECONDS:
            terminal_ns = anchor.anchor_ns + horizon * ONE_SECOND_NS
            if terminal_ns > interval[1]:
                censored[horizon].add(anchor.outcome_key)
                continue
            terminal_index = _terminal_index(path, terminal_ns, entry_index)
            if terminal_index is None:
                censored[horizon].add(anchor.outcome_key)
                continue
            terminal_reference = path.rows[terminal_index].bar.close_ticks
            payloads[horizon][anchor.outcome_key] = sign * (terminal_reference - entry_reference)
    return tuple(
        ReferenceOutcomeSurface(horizon, payloads[horizon], frozenset(censored[horizon]))
        for horizon in REFERENCE_HORIZONS_SECONDS
    )


def merge_reference_outcome_surfaces(
    parts: Sequence[Sequence[ReferenceOutcomeSurface]],
) -> tuple[ReferenceOutcomeSurface, ...]:
    """Merge disjoint streamed outcome-span surfaces and reject conflicts."""

    values = tuple(tuple(item) for item in parts)
    if not values:
        raise SymbolicEngineError("cannot merge no reference surface parts")
    merged: dict[int, dict[tuple[str, int, int, int, Direction], int]] = {
        horizon: {} for horizon in REFERENCE_HORIZONS_SECONDS
    }
    censored: dict[int, set[tuple[str, int, int, int, Direction]]] = {
        horizon: set() for horizon in REFERENCE_HORIZONS_SECONDS
    }
    for part in values:
        ordered = tuple(sorted(part, key=lambda item: item.horizon_seconds))
        if tuple(item.horizon_seconds for item in ordered) != REFERENCE_HORIZONS_SECONDS:
            raise SymbolicEngineError("reference surface part lacks the exact five horizons")
        for surface in ordered:
            target = merged[surface.horizon_seconds]
            for key, gross in surface.gross_ticks_by_anchor.items():
                if key in censored[surface.horizon_seconds]:
                    raise SymbolicEngineError("reference surface parts fill a censored anchor")
                prior = target.get(key)
                if prior is not None and prior != gross:
                    raise SymbolicEngineError("reference surface parts conflict")
                target[key] = gross
            for key in surface.censored_anchor_keys:
                if key in target:
                    raise SymbolicEngineError("reference surface parts censor a filled anchor")
                censored[surface.horizon_seconds].add(key)
    return tuple(
        ReferenceOutcomeSurface(horizon, merged[horizon], frozenset(censored[horizon]))
        for horizon in REFERENCE_HORIZONS_SECONDS
    )


@dataclass(frozen=True, slots=True)
class EntryAttempt:
    order_id: str
    status: str
    reason: str
    occupied_until_ns: int
    entry_row_index: int | None
    entry_ns: int | None
    entry_reference_ticks: int | None
    entry_fill_ticks: int | None

    @property
    def filled(self) -> bool:
        return self.status == "FILLED"

    def as_dict(self) -> dict[str, object]:
        return {
            "entry_fill_ticks": self.entry_fill_ticks,
            "entry_ns": self.entry_ns,
            "entry_reference_ticks": self.entry_reference_ticks,
            "entry_row_index": self.entry_row_index,
            "occupied_until_ns": self.occupied_until_ns,
            "order_id": self.order_id,
            "reason": self.reason,
            "status": self.status,
        }


@dataclass(frozen=True, slots=True)
class ExitOutcome:
    status: str
    reason: str
    occupied_until_ns: int
    exit_row_index: int | None
    exit_ns: int | None
    exit_reference_ticks: int | None
    exit_fill_ticks: int | None
    reference_pnl_ticks: int | None
    gross_pnl_ticks: int | None
    net_pnl_ticks: int | None
    stress_net_pnl_ticks: int | None

    @property
    def filled(self) -> bool:
        return self.status == "FILLED"

    def as_dict(self) -> dict[str, object]:
        return {
            "exit_fill_ticks": self.exit_fill_ticks,
            "exit_ns": self.exit_ns,
            "exit_reference_ticks": self.exit_reference_ticks,
            "exit_row_index": self.exit_row_index,
            "gross_pnl_ticks": self.gross_pnl_ticks,
            "net_pnl_ticks": self.net_pnl_ticks,
            "occupied_until_ns": self.occupied_until_ns,
            "reason": self.reason,
            "reference_pnl_ticks": self.reference_pnl_ticks,
            "status": self.status,
            "stress_net_pnl_ticks": self.stress_net_pnl_ticks,
        }


def _entry_touch(order: FrozenEntryOrder, row: BarWithOutcomeSpan) -> bool:
    if order.kind == "MARKET":
        return True
    if order.order_ticks is None:  # pragma: no cover - FrozenEntryOrder invariant
        raise SymbolicEngineError("priced entry order lost its price")
    sign = _direction_sign(order.anchor.direction)
    if order.kind == "STOP_SIGNAL_EXTREME":
        return (
            row.bar.high_ticks >= order.order_ticks
            if sign > 0
            else row.bar.low_ticks <= order.order_ticks
        )
    return (
        row.bar.low_ticks <= order.order_ticks
        if sign > 0
        else row.bar.high_ticks >= order.order_ticks
    )


def _entry_reference(order: FrozenEntryOrder, row: BarWithOutcomeSpan) -> int:
    if order.kind == "MARKET":
        return row.bar.open_ticks
    if order.order_ticks is None:  # pragma: no cover - FrozenEntryOrder invariant
        raise SymbolicEngineError("priced entry order lost its price")
    sign = _direction_sign(order.anchor.direction)
    oriented_order = sign * order.order_ticks
    oriented_open = sign * row.bar.open_ticks
    if order.kind == "STOP_SIGNAL_EXTREME":
        return sign * max(oriented_order, oriented_open)
    return sign * min(oriented_order, oriented_open)


def _resolve_entry(order: FrozenEntryOrder, path: OneSecondPath) -> EntryAttempt:
    if path.lineage != (
        order.anchor.contract,
        order.anchor.outcome_span_id,
        order.anchor.segment_id,
    ):
        raise SymbolicEngineError("entry order and one-second path lineages differ")
    interval = path.coverage_interval_at(order.valid_from_ns)
    if interval is None:
        return EntryAttempt(
            order.order_id,
            "CENSORED",
            "ENTRY_WINDOW_START_NOT_COVERED",
            order.expires_ns,
            None,
            None,
            None,
            None,
        )
    start = bisect_left(path.starts, order.valid_from_ns)
    for index in range(start, len(path.rows)):
        row = path.rows[index]
        if row.bar.start_ns >= min(order.expires_ns, interval[1]):
            break
        if not _entry_touch(order, row):
            continue
        reference = _entry_reference(order, row)
        sign = _direction_sign(order.anchor.direction)
        return EntryAttempt(
            order.order_id,
            "FILLED",
            "ENTRY_FILLED",
            row.bar.end_ns,
            index,
            row.bar.start_ns,
            reference,
            reference + sign * ENTRY_ADVERSE_TICKS,
        )
    status = "UNFILLED" if interval[1] >= order.expires_ns else "CENSORED"
    reason = "ENTRY_TIF_EXPIRED" if status == "UNFILLED" else "ENTRY_WINDOW_END_NOT_COVERED"
    return EntryAttempt(
        order.order_id,
        status,
        reason,
        order.expires_ns,
        None,
        None,
        None,
        None,
    )


def _oriented_extremes(direction: Direction, row: BarWithOutcomeSpan) -> tuple[int, int]:
    if direction == "LONG":
        return row.bar.high_ticks, row.bar.low_ticks
    return -row.bar.low_ticks, -row.bar.high_ticks


def _terminal_index(path: OneSecondPath, cap_ns: int, entry_index: int) -> int | None:
    index = bisect_right(path.ends, cap_ns) - 1
    return (
        index
        if index >= entry_index and 0 <= cap_ns - path.ends[index] < FIVE_MINUTES * ONE_SECOND_NS
        else None
    )


def _filled_exit(
    order: FrozenEntryOrder,
    entry: EntryAttempt,
    *,
    reason: str,
    row_index: int,
    exit_ns: int,
    exit_reference_ticks: int,
    occupied_until_ns: int | None = None,
) -> ExitOutcome:
    if (
        entry.entry_reference_ticks is None
        or entry.entry_fill_ticks is None
        or entry.entry_ns is None
    ):
        raise SymbolicEngineError("filled exit requires a filled entry")
    sign = _direction_sign(order.anchor.direction)
    exit_fill = exit_reference_ticks - sign * EXIT_ADVERSE_TICKS
    reference_pnl = sign * (exit_reference_ticks - entry.entry_reference_ticks)
    gross_pnl = sign * (exit_fill - entry.entry_fill_ticks)
    net = gross_pnl - VARIABLE_COST_TICKS - ALLOCATED_FIXED_COST_TICKS
    return ExitOutcome(
        "FILLED",
        reason,
        exit_ns if occupied_until_ns is None else occupied_until_ns,
        row_index,
        exit_ns,
        exit_reference_ticks,
        exit_fill,
        reference_pnl,
        gross_pnl,
        net,
        net - (18 - TOTAL_FRICTION_TICKS),
    )


def _censored_exit(reason: str, occupied_until_ns: int) -> ExitOutcome:
    return ExitOutcome(
        "CENSORED",
        reason,
        occupied_until_ns,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )


def _terminal_exit(
    order: FrozenEntryOrder,
    entry: EntryAttempt,
    path: OneSecondPath,
    cap_ns: int,
    *,
    reason: str = "TERMINAL_CAP",
) -> ExitOutcome:
    if entry.entry_ns is None or not path.structurally_covers(entry.entry_ns, cap_ns):
        return _censored_exit("EXIT_WINDOW_END_NOT_COVERED", cap_ns)
    if entry.entry_row_index is None:
        raise SymbolicEngineError("terminal exit requires an entry row")
    index = _terminal_index(path, cap_ns, entry.entry_row_index)
    if index is None:
        return _censored_exit("MISSING_TERMINAL_TRADE", cap_ns)
    row = path.rows[index]
    return _filled_exit(
        order,
        entry,
        reason=reason,
        row_index=index,
        exit_ns=cap_ns,
        exit_reference_ticks=row.bar.close_ticks,
    )


def _stop_reference(
    direction: Direction,
    stop_oriented: int,
    row: BarWithOutcomeSpan,
    *,
    same_entry_second: bool,
) -> int:
    sign = _direction_sign(direction)
    reference_oriented = stop_oriented
    if not same_entry_second:
        reference_oriented = min(stop_oriented, sign * row.bar.open_ticks)
    return sign * reference_oriented


def _resolve_bracket_exit(
    order: FrozenEntryOrder,
    entry: EntryAttempt,
    exit_policy: ExitPolicy,
    path: OneSecondPath,
    cap_ns: int,
) -> ExitOutcome:
    if entry.entry_row_index is None or entry.entry_fill_ticks is None:
        raise SymbolicEngineError("bracket exit requires a filled entry")
    sign = _direction_sign(order.anchor.direction)
    entry_oriented = sign * entry.entry_fill_ticks
    take_profit = entry_oriented + _atr_distance(
        order.anchor, exit_policy.fraction_parameter("take_profit_atr")
    )
    stop_loss = entry_oriented - _atr_distance(
        order.anchor, exit_policy.fraction_parameter("stop_loss_atr")
    )
    if entry.entry_ns is None:  # pragma: no cover - filled-entry invariant
        raise SymbolicEngineError("bracket exit lost entry time")
    interval = path.coverage_interval_at(entry.entry_ns)
    if interval is None:  # pragma: no cover - entry resolver invariant
        return _censored_exit("EXIT_WINDOW_START_NOT_COVERED", cap_ns)
    for index in range(entry.entry_row_index, len(path.rows)):
        row = path.rows[index]
        if row.bar.start_ns >= min(cap_ns, interval[1]):
            break
        favorable, adverse = _oriented_extremes(order.anchor.direction, row)
        same_second = index == entry.entry_row_index
        stop_touched = adverse <= stop_loss
        target_touched = favorable >= take_profit and not same_second
        if stop_touched:
            reference = _stop_reference(
                order.anchor.direction,
                stop_loss,
                row,
                same_entry_second=same_second,
            )
            return _filled_exit(
                order,
                entry,
                reason="STOP_LOSS",
                row_index=index,
                exit_ns=row.bar.end_ns,
                exit_reference_ticks=reference,
            )
        if target_touched:
            return _filled_exit(
                order,
                entry,
                reason="TAKE_PROFIT",
                row_index=index,
                exit_ns=row.bar.end_ns,
                exit_reference_ticks=sign * take_profit,
            )
    return _terminal_exit(order, entry, path, cap_ns)


def _resolve_trailing_exit(
    order: FrozenEntryOrder,
    entry: EntryAttempt,
    exit_policy: ExitPolicy,
    path: OneSecondPath,
    cap_ns: int,
) -> ExitOutcome:
    if entry.entry_row_index is None or entry.entry_fill_ticks is None:
        raise SymbolicEngineError("trailing exit requires a filled entry")
    sign = _direction_sign(order.anchor.direction)
    entry_oriented = sign * entry.entry_fill_ticks
    activation = entry_oriented + _atr_distance(
        order.anchor, exit_policy.fraction_parameter("activation_atr")
    )
    trail_distance = _atr_distance(order.anchor, exit_policy.fraction_parameter("trail_atr"))
    if entry.entry_ns is None:  # pragma: no cover - filled-entry invariant
        raise SymbolicEngineError("trailing exit lost entry time")
    interval = path.coverage_interval_at(entry.entry_ns)
    if interval is None:  # pragma: no cover - entry resolver invariant
        return _censored_exit("EXIT_WINDOW_START_NOT_COVERED", cap_ns)
    high_water: int | None = None
    stop: int | None = None
    for index in range(entry.entry_row_index + 1, len(path.rows)):
        row = path.rows[index]
        if row.bar.start_ns >= min(cap_ns, interval[1]):
            break
        favorable, adverse = _oriented_extremes(order.anchor.direction, row)
        if stop is not None and adverse <= stop:
            return _filled_exit(
                order,
                entry,
                reason="TRAILING_STOP",
                row_index=index,
                exit_ns=row.bar.end_ns,
                exit_reference_ticks=_stop_reference(
                    order.anchor.direction,
                    stop,
                    row,
                    same_entry_second=False,
                ),
            )
        if high_water is None:
            if favorable < activation:
                continue
            high_water = favorable
        else:
            high_water = max(high_water, favorable)
        updated_stop = high_water - trail_distance
        if adverse <= updated_stop:
            return _filled_exit(
                order,
                entry,
                reason="TRAILING_STOP",
                row_index=index,
                exit_ns=row.bar.end_ns,
                exit_reference_ticks=sign * updated_stop,
            )
        stop = updated_stop
    return _terminal_exit(order, entry, path, cap_ns)


def _resolve_break_even_exit(
    order: FrozenEntryOrder,
    entry: EntryAttempt,
    exit_policy: ExitPolicy,
    path: OneSecondPath,
    cap_ns: int,
) -> ExitOutcome:
    if entry.entry_row_index is None or entry.entry_fill_ticks is None:
        raise SymbolicEngineError("break-even exit requires a filled entry")
    sign = _direction_sign(order.anchor.direction)
    entry_oriented = sign * entry.entry_fill_ticks
    activation = entry_oriented + _atr_distance(
        order.anchor, exit_policy.fraction_parameter("activation_atr")
    )
    initial_stop = entry_oriented - _atr_distance(
        order.anchor, exit_policy.fraction_parameter("initial_stop_atr")
    )
    if entry.entry_ns is None:  # pragma: no cover - filled-entry invariant
        raise SymbolicEngineError("break-even exit lost entry time")
    interval = path.coverage_interval_at(entry.entry_ns)
    if interval is None:  # pragma: no cover - entry resolver invariant
        return _censored_exit("EXIT_WINDOW_START_NOT_COVERED", cap_ns)
    activated = False
    for index in range(entry.entry_row_index, len(path.rows)):
        row = path.rows[index]
        if row.bar.start_ns >= min(cap_ns, interval[1]):
            break
        favorable, adverse = _oriented_extremes(order.anchor.direction, row)
        same_second = index == entry.entry_row_index
        active_stop = entry_oriented if activated else initial_stop
        if adverse <= active_stop:
            return _filled_exit(
                order,
                entry,
                reason="BREAK_EVEN_STOP" if activated else "INITIAL_STOP",
                row_index=index,
                exit_ns=row.bar.end_ns,
                exit_reference_ticks=_stop_reference(
                    order.anchor.direction,
                    active_stop,
                    row,
                    same_entry_second=same_second,
                ),
            )
        if not activated and not same_second and favorable >= activation:
            activated = True
            if adverse <= entry_oriented:
                return _filled_exit(
                    order,
                    entry,
                    reason="BREAK_EVEN_STOP",
                    row_index=index,
                    exit_ns=row.bar.end_ns,
                    exit_reference_ticks=sign * entry_oriented,
                )
    return _terminal_exit(order, entry, path, cap_ns)


def _resolve_rule_exit(
    order: FrozenEntryOrder,
    entry: EntryAttempt,
    exit_policy: ExitPolicy,
    path: OneSecondPath,
    cap_ns: int,
    rule_times: RuleExitTimes | None,
) -> ExitOutcome:
    if entry.entry_row_index is None or entry.entry_ns is None:
        raise SymbolicEngineError("rule exit requires a filled entry")
    if rule_times is None:
        return _censored_exit("MISSING_RULE_EXIT_SCHEDULE", cap_ns)
    trigger_ns = (
        rule_times.opposite_trigger_ns
        if exit_policy.rule_kind == "OPPOSITE_TRIGGER"
        else rule_times.context_invalid_ns
    )
    if trigger_ns is None or trigger_ns >= cap_ns:
        return _terminal_exit(order, entry, path, cap_ns)
    if trigger_ns <= entry.entry_ns:
        return ExitOutcome(
            "CANCELLED_BEFORE_ENTRY",
            "ENTRY_CANCELLED_BY_EXIT_RULE",
            trigger_ns,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    interval = path.coverage_interval_at(entry.entry_ns)
    if interval is None or trigger_ns >= interval[1]:
        return _censored_exit("RULE_EXIT_WINDOW_NOT_COVERED", cap_ns)
    index = bisect_left(path.starts, trigger_ns)
    if index >= len(path.rows) or path.rows[index].bar.start_ns >= cap_ns:
        return _terminal_exit(order, entry, path, cap_ns)
    row = path.rows[index]
    return _filled_exit(
        order,
        entry,
        reason=exit_policy.rule_kind or "RULE_EXIT",
        row_index=index,
        exit_ns=row.bar.start_ns,
        exit_reference_ticks=row.bar.open_ticks,
    )


def _resolve_exit(
    order: FrozenEntryOrder,
    entry: EntryAttempt,
    exit_policy: ExitPolicy,
    path: OneSecondPath,
    rule_times: RuleExitTimes | None,
) -> ExitOutcome:
    if not entry.filled or entry.entry_ns is None:
        raise SymbolicEngineError("exit resolution requires a filled entry")
    cap_ns = entry.entry_ns + exit_policy.cap_seconds * ONE_SECOND_NS
    if exit_policy.kind == "TERMINAL":
        return _terminal_exit(order, entry, path, cap_ns)
    if exit_policy.kind == "BRACKET":
        return _resolve_bracket_exit(order, entry, exit_policy, path, cap_ns)
    if exit_policy.kind == "TRAILING":
        return _resolve_trailing_exit(order, entry, exit_policy, path, cap_ns)
    if exit_policy.kind == "BREAK_EVEN":
        return _resolve_break_even_exit(order, entry, exit_policy, path, cap_ns)
    return _resolve_rule_exit(order, entry, exit_policy, path, cap_ns, rule_times)


@dataclass(frozen=True, slots=True)
class GroupStrategyAggregate:
    group_key: str
    raw_signal_count: int
    active_signal_dates: tuple[date, ...]
    fill_count: int
    active_entry_dates: tuple[date, ...]
    censored_count: int
    net_ticks: int
    stress_net_ticks: int
    gross_profit_ticks: int
    gross_loss_ticks: int
    maximum_drawdown_ticks: int
    maximum_prefix_equity_ticks: int
    minimum_prefix_equity_ticks: int

    def as_dict(self) -> dict[str, object]:
        return {
            "fill_count": self.fill_count,
            "active_entry_dates": [item.isoformat() for item in self.active_entry_dates],
            "active_signal_dates": [item.isoformat() for item in self.active_signal_dates],
            "censored_count": self.censored_count,
            "group_key": self.group_key,
            "gross_loss_ticks": self.gross_loss_ticks,
            "gross_profit_ticks": self.gross_profit_ticks,
            "maximum_drawdown_ticks": self.maximum_drawdown_ticks,
            "maximum_prefix_equity_ticks": self.maximum_prefix_equity_ticks,
            "minimum_prefix_equity_ticks": self.minimum_prefix_equity_ticks,
            "net_ticks": self.net_ticks,
            "raw_signal_count": self.raw_signal_count,
            "stress_net_ticks": self.stress_net_ticks,
        }


@dataclass(frozen=True, slots=True)
class CompleteStrategyEvaluation:
    """Compact, mergeable aggregate for one recipe over disjoint path lineages."""

    recipe: CompleteStrategyRecipe
    evaluated_lineages: tuple[tuple[str, int, int], ...]
    evaluation_start_ns: int
    evaluation_end_ns: int
    raw_signal_count: int
    active_signal_days: int
    active_signal_date_values: tuple[date, ...]
    skipped_occupied_count: int
    unfilled_entry_count: int
    cancelled_entry_count: int
    censored_count: int
    fill_count: int
    active_entry_days: int
    active_entry_date_values: tuple[date, ...]
    contract_count: int
    contract_values: tuple[str, ...]
    total_reference_pnl_ticks: int
    total_gross_pnl_ticks: int
    total_net_ticks: int
    total_stress_net_ticks: int
    gross_profit_ticks: int
    gross_loss_ticks: int
    maximum_drawdown_ticks: int
    maximum_prefix_equity_ticks: int
    minimum_prefix_equity_ticks: int
    reporting_groups: tuple[GroupStrategyAggregate, ...]
    outer_validations: tuple[GroupStrategyAggregate, ...]
    evaluated_anchor_counts: tuple[int, ...]
    evaluated_anchor_leaf_sha256s: tuple[str, ...]
    behavior_leaf_sha256s: tuple[str, ...]
    artifact_sha256: str

    def __post_init__(self) -> None:
        if self.active_signal_date_values != tuple(sorted(set(self.active_signal_date_values))):
            raise SymbolicEngineError("active signal dates are non-canonical")
        if self.active_entry_date_values != tuple(sorted(set(self.active_entry_date_values))):
            raise SymbolicEngineError("active entry dates are non-canonical")
        if self.contract_values != tuple(sorted(set(self.contract_values))):
            raise SymbolicEngineError("contract values are non-canonical")
        if (
            self.active_signal_days != len(self.active_signal_date_values)
            or self.active_entry_days != len(self.active_entry_date_values)
            or self.contract_count != len(self.contract_values)
            or len(self.evaluated_lineages) != len(self.evaluated_anchor_counts)
            or len(self.evaluated_lineages) != len(self.evaluated_anchor_leaf_sha256s)
            or len(self.evaluated_lineages) != len(self.behavior_leaf_sha256s)
            or sum(self.evaluated_anchor_counts) != self.raw_signal_count
        ):
            raise SymbolicEngineError("complete evaluation compact counts differ")
        _require_sha(self.artifact_sha256, label="complete evaluation artifact_sha256")
        if canonical_sha256(self.definition_dict()) != self.artifact_sha256:
            raise SymbolicEngineError("complete evaluation hash differs")

    @property
    def profit_factor(self) -> Fraction | None:
        if self.gross_loss_ticks == 0:
            return None if self.gross_profit_ticks == 0 else Fraction(10**18)
        return Fraction(self.gross_profit_ticks, self.gross_loss_ticks)

    def definition_dict(self) -> dict[str, object]:
        profit_factor = self.profit_factor
        return {
            "active_entry_days": self.active_entry_days,
            "active_entry_dates": [item.isoformat() for item in self.active_entry_date_values],
            "active_signal_days": self.active_signal_days,
            "active_signal_dates": [item.isoformat() for item in self.active_signal_date_values],
            "behavior_leaf_sha256s": list(self.behavior_leaf_sha256s),
            "cancelled_entry_count": self.cancelled_entry_count,
            "censored_count": self.censored_count,
            "contract_count": self.contract_count,
            "contracts": list(self.contract_values),
            "evaluated_lineages": [list(item) for item in self.evaluated_lineages],
            "evaluated_anchor_counts": list(self.evaluated_anchor_counts),
            "evaluated_anchor_leaf_sha256s": list(self.evaluated_anchor_leaf_sha256s),
            "evaluation_end_ns": self.evaluation_end_ns,
            "evaluation_start_ns": self.evaluation_start_ns,
            "fill_count": self.fill_count,
            "gross_loss_ticks": self.gross_loss_ticks,
            "gross_profit_ticks": self.gross_profit_ticks,
            "maximum_drawdown_ticks": self.maximum_drawdown_ticks,
            "maximum_prefix_equity_ticks": self.maximum_prefix_equity_ticks,
            "minimum_prefix_equity_ticks": self.minimum_prefix_equity_ticks,
            "outer_validations": [item.as_dict() for item in self.outer_validations],
            "profit_factor_denominator": (
                None if profit_factor is None else profit_factor.denominator
            ),
            "profit_factor_numerator": (None if profit_factor is None else profit_factor.numerator),
            "raw_signal_count": self.raw_signal_count,
            "recipe": self.recipe.as_dict(),
            "reporting_groups": [item.as_dict() for item in self.reporting_groups],
            "schema": PATH_OUTCOME_SCHEMA,
            "skipped_occupied_count": self.skipped_occupied_count,
            "total_gross_pnl_ticks": self.total_gross_pnl_ticks,
            "total_net_ticks": self.total_net_ticks,
            "total_reference_pnl_ticks": self.total_reference_pnl_ticks,
            "total_stress_net_ticks": self.total_stress_net_ticks,
            "unfilled_entry_count": self.unfilled_entry_count,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def _complete_evaluation_coverage_shape(
    evaluation: CompleteStrategyEvaluation,
) -> dict[str, object]:
    definition = {
        "evaluated_anchor_counts": list(evaluation.evaluated_anchor_counts),
        "evaluated_anchor_leaf_sha256s": list(evaluation.evaluated_anchor_leaf_sha256s),
        "evaluated_lineages": [list(item) for item in evaluation.evaluated_lineages],
        "schema": COMPACT_STAGE_B_CHUNK_SCHEMA,
    }
    return {**definition, "coverage_shape_sha256": canonical_sha256(definition)}


def _complete_evaluation_behavior_leaf_row(
    evaluation: CompleteStrategyEvaluation,
    coverage_shape_sha256: str,
) -> dict[str, object]:
    definition = {
        "behavior_leaf_sha256s": list(evaluation.behavior_leaf_sha256s),
        "coverage_shape_sha256": coverage_shape_sha256,
        "schema": COMPACT_STAGE_B_CHUNK_SCHEMA,
        "strategy_id": evaluation.recipe.strategy_id,
        "strategy_rank": evaluation.recipe.strategy_rank,
    }
    return {**definition, "behavior_leaf_row_sha256": canonical_sha256(definition)}


def _complete_evaluation_chunk_definition(
    evaluations: Sequence[CompleteStrategyEvaluation],
) -> dict[str, object]:
    values = tuple(evaluations)
    if not values:
        raise SymbolicEngineError("complete evaluation chunk cannot be empty")
    coverage_by_sha: dict[str, dict[str, object]] = {}
    behavior_rows: list[dict[str, object]] = []
    compact_evaluations: list[dict[str, object]] = []
    for evaluation in values:
        coverage = _complete_evaluation_coverage_shape(evaluation)
        coverage_sha = coverage["coverage_shape_sha256"]
        if not isinstance(coverage_sha, str):  # pragma: no cover - local canonical SHA
            raise SymbolicEngineError("compact coverage shape lost its SHA")
        coverage_by_sha.setdefault(coverage_sha, coverage)
        behavior = _complete_evaluation_behavior_leaf_row(evaluation, coverage_sha)
        behavior_rows.append(behavior)
        behavior_sha = behavior["behavior_leaf_row_sha256"]
        if not isinstance(behavior_sha, str):  # pragma: no cover - local canonical SHA
            raise SymbolicEngineError("compact behavior row lost its SHA")
        compact = evaluation.as_dict()
        for key in (
            "behavior_leaf_sha256s",
            "evaluated_anchor_counts",
            "evaluated_anchor_leaf_sha256s",
            "evaluated_lineages",
        ):
            del compact[key]
        compact["behavior_leaf_row_sha256"] = behavior_sha
        compact["coverage_shape_sha256"] = coverage_sha
        compact_evaluations.append(compact)
    return {
        "behavior_leaf_rows": behavior_rows,
        "coverage_shapes": [coverage_by_sha[key] for key in sorted(coverage_by_sha)],
        "evaluations": compact_evaluations,
        "first_strategy_rank": values[0].recipe.strategy_rank,
        "last_strategy_rank": values[-1].recipe.strategy_rank,
        "schema": COMPACT_STAGE_B_CHUNK_SCHEMA,
        "serialization": "FACTORED_COVERAGE_AND_BEHAVIOR_LEAVES",
    }


@dataclass(frozen=True, slots=True)
class CompleteEvaluationChunk:
    first_strategy_rank: int
    last_strategy_rank: int
    evaluations: tuple[CompleteStrategyEvaluation, ...]
    artifact_sha256: str

    @classmethod
    def from_evaluations(
        cls,
        evaluations: Sequence[CompleteStrategyEvaluation],
    ) -> CompleteEvaluationChunk:
        values = tuple(evaluations)
        definition = _complete_evaluation_chunk_definition(values)
        return cls(
            values[0].recipe.strategy_rank,
            values[-1].recipe.strategy_rank,
            values,
            canonical_sha256(definition),
        )

    def __post_init__(self) -> None:
        if not self.evaluations:
            raise SymbolicEngineError("complete evaluation chunk cannot be empty")
        ranks = tuple(item.recipe.strategy_rank for item in self.evaluations)
        if ranks != tuple(sorted(ranks)) or len(set(ranks)) != len(ranks):
            raise SymbolicEngineError("complete evaluation chunk ranks are non-canonical")
        if self.first_strategy_rank != ranks[0] or self.last_strategy_rank != ranks[-1]:
            raise SymbolicEngineError("complete evaluation chunk bounds differ")
        _require_sha(self.artifact_sha256, label="evaluation chunk artifact_sha256")
        if canonical_sha256(self.definition_dict()) != self.artifact_sha256:
            raise SymbolicEngineError("complete evaluation chunk hash differs")

    def definition_dict(self) -> dict[str, object]:
        return _complete_evaluation_chunk_definition(self.evaluations)

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


ControlWorld = Literal["REAL", "CIRCULAR", "MATCHED"]


@dataclass(frozen=True, slots=True)
class SelectedStrategyDetailRequest:
    selection_rank: int
    scope_key: str
    world: ControlWorld
    recipe: CompleteStrategyRecipe
    mask: PolicyMask
    rule_schedule: RuleExitSchedule | None

    def __post_init__(self) -> None:
        if not 1 <= self.selection_rank <= 24 or not self.scope_key:
            raise SymbolicEngineError("detail request must be one of bounded ranks 1..24")
        if self.world not in ("REAL", "CIRCULAR", "MATCHED"):
            raise SymbolicEngineError("detail request world differs")
        if self.recipe.anchor_policy_id != self.mask.policy.policy_id:
            raise SymbolicEngineError("detail request recipe and mask differ")


@dataclass(frozen=True, slots=True)
class StrategyTradeOutcomeRow:
    anchor_key: tuple[str, int, int, int, Direction]
    source_date: date
    direction: Direction
    trigger_start_ns: int
    trigger_end_ns: int
    atr_sum_ticks: int
    atr_denominator: int
    status: str
    reason: str
    structurally_valid: bool
    censored: bool
    entry_ns: int | None
    exit_ns: int | None
    entry_reference_ticks: int | None
    entry_fill_ticks: int | None
    exit_reference_ticks: int | None
    exit_fill_ticks: int | None
    reference_pnl_ticks: int | None
    gross_pnl_ticks: int | None
    net_pnl_ticks: int | None
    stress_net_pnl_ticks: int | None
    reporting_group: str | None
    outer_validation: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "anchor_key": list(self.anchor_key),
            "atr_denominator": self.atr_denominator,
            "atr_sum_ticks": self.atr_sum_ticks,
            "censored": self.censored,
            "direction": self.direction,
            "entry_fill_ticks": self.entry_fill_ticks,
            "entry_ns": self.entry_ns,
            "entry_reference_ticks": self.entry_reference_ticks,
            "exit_fill_ticks": self.exit_fill_ticks,
            "exit_ns": self.exit_ns,
            "exit_reference_ticks": self.exit_reference_ticks,
            "gross_pnl_ticks": self.gross_pnl_ticks,
            "net_pnl_ticks": self.net_pnl_ticks,
            "outer_validation": self.outer_validation,
            "reason": self.reason,
            "reference_pnl_ticks": self.reference_pnl_ticks,
            "reporting_group": self.reporting_group,
            "source_date": self.source_date.isoformat(),
            "status": self.status,
            "stress_net_pnl_ticks": self.stress_net_pnl_ticks,
            "structurally_valid": self.structurally_valid,
            "trigger_end_ns": self.trigger_end_ns,
            "trigger_start_ns": self.trigger_start_ns,
        }


@dataclass(frozen=True, slots=True)
class SelectedStrategyDetailedOutcome:
    selection_rank: int
    scope_key: str
    world: ControlWorld
    recipe: CompleteStrategyRecipe
    evaluation_artifact_sha256: str
    rows: tuple[StrategyTradeOutcomeRow, ...]
    artifact_sha256: str

    def __post_init__(self) -> None:
        if not 1 <= self.selection_rank <= 24 or not self.scope_key:
            raise SymbolicEngineError("detailed outcome rank/scope differs")
        _require_sha(
            self.evaluation_artifact_sha256,
            label="detailed outcome evaluation artifact SHA",
        )
        keys = tuple(item.anchor_key for item in self.rows)
        if len(set(keys)) != len(keys):
            raise SymbolicEngineError("detailed outcome rows duplicate an anchor")
        _require_sha(self.artifact_sha256, label="detailed outcome artifact SHA")
        if canonical_sha256(self.definition_dict()) != self.artifact_sha256:
            raise SymbolicEngineError("detailed outcome commitment differs")

    def definition_dict(self) -> dict[str, object]:
        return {
            "evaluation_artifact_sha256": self.evaluation_artifact_sha256,
            "recipe": self.recipe.as_dict(),
            "rows": [item.as_dict() for item in self.rows],
            "schema": PATH_OUTCOME_SCHEMA,
            "scope_key": self.scope_key,
            "selection_rank": self.selection_rank,
            "world": self.world,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def _equity_shape(values: Sequence[int]) -> tuple[int, int, int]:
    equity = 0
    peak = 0
    maximum_drawdown = 0
    maximum_prefix = 0
    minimum_prefix = 0
    for value in values:
        equity += value
        maximum_prefix = max(maximum_prefix, equity)
        minimum_prefix = min(minimum_prefix, equity)
        peak = max(peak, equity)
        maximum_drawdown = max(maximum_drawdown, peak - equity)
    return maximum_drawdown, maximum_prefix, minimum_prefix


class SharedPathEvaluator:
    """Share entry touches and path exits across a bounded strategy recipe batch."""

    def __init__(self, paths: Sequence[OneSecondPath]) -> None:
        values = tuple(paths)
        if not values or len({item.lineage for item in values}) != len(values):
            raise SymbolicEngineError("shared evaluator paths must be non-empty unique lineages")
        self.paths = {item.lineage: item for item in values}
        self._entry_attempts: dict[str, EntryAttempt] = {}
        self._exit_outcomes: dict[tuple[str, str, int | None, int | None], ExitOutcome] = {}
        self._order_batches: dict[tuple[str, str], Mapping[AnchorRecord, FrozenEntryOrder]] = {}
        self._path_tensors: dict[
            tuple[tuple[str, int, int], Direction],
            tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray],
        ] = {}

    def _orders(
        self, mask: PolicyMask, entry: EntryPolicy
    ) -> Mapping[AnchorRecord, FrozenEntryOrder]:
        key = mask.mask_sha256, entry.entry_id
        cached = self._order_batches.get(key)
        if cached is not None:
            return cached
        batch = freeze_entry_orders(mask, (entry,))
        result = {item.anchor: item for item in batch.orders}
        self._order_batches[key] = result
        return result

    def _entry(self, order: FrozenEntryOrder, path: OneSecondPath) -> EntryAttempt:
        cached = self._entry_attempts.get(order.order_id)
        if cached is None:
            cached = _resolve_entry(order, path)
            self._entry_attempts[order.order_id] = cached
        return cached

    def _exit(
        self,
        order: FrozenEntryOrder,
        entry: EntryAttempt,
        exit_policy: ExitPolicy,
        path: OneSecondPath,
        rule_times: RuleExitTimes | None,
    ) -> ExitOutcome:
        opposite_ns = None if rule_times is None else rule_times.opposite_trigger_ns
        invalid_ns = None if rule_times is None else rule_times.context_invalid_ns
        key = order.order_id, exit_policy.exit_id, opposite_ns, invalid_ns
        cached = self._exit_outcomes.get(key)
        if cached is None:
            if exit_policy.kind == "RULE":
                cached = _resolve_exit(order, entry, exit_policy, path, rule_times)
                self._exit_outcomes[key] = cached
            else:
                self._populate_fixed_exit_outcomes(order, entry, path)
                cached = self._exit_outcomes[key]
        return cached

    def fixed_exit_outcome_batch(
        self,
        order: FrozenEntryOrder,
        entry: EntryAttempt,
        path: OneSecondPath,
    ) -> tuple[tuple[str, ExitOutcome], ...]:
        """Return all 81 fixed exits after one shared vector-path population."""

        if not entry.filled:
            raise SymbolicEngineError("fixed exit batch requires a filled entry")
        self._populate_fixed_exit_outcomes(order, entry, path)
        return tuple(
            (item.exit_id, self._exit_outcomes[order.order_id, item.exit_id, None, None])
            for item in build_exit_catalog().candidates
            if item.kind != "RULE"
        )

    def release_entry_batch(self, mask: PolicyMask, entry_policy_id: str) -> None:
        """Bound cache RSS after the 85 recipes for one mask/entry are aggregated."""

        orders = self._order_batches.pop((mask.mask_sha256, entry_policy_id), None)
        if orders is None:
            return
        order_ids = {item.order_id for item in orders.values()}
        for order_id in order_ids:
            self._entry_attempts.pop(order_id, None)
        for key in tuple(self._exit_outcomes):
            if key[0] in order_ids:
                del self._exit_outcomes[key]

    def _path_tensor(
        self,
        path: OneSecondPath,
        direction: Direction,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        key = path.lineage, direction
        cached = self._path_tensors.get(key)
        if cached is not None:
            return cached
        sign = _direction_sign(direction)
        starts = np.fromiter(path.starts, dtype=np.int64, count=len(path.starts))
        opens = np.fromiter(
            (sign * item.bar.open_ticks for item in path.rows),
            dtype=np.int64,
            count=len(path.rows),
        )
        favorable = np.fromiter(
            (item.bar.high_ticks if sign > 0 else -item.bar.low_ticks for item in path.rows),
            dtype=np.int64,
            count=len(path.rows),
        )
        adverse = np.fromiter(
            (item.bar.low_ticks if sign > 0 else -item.bar.high_ticks for item in path.rows),
            dtype=np.int64,
            count=len(path.rows),
        )
        result = starts, opens, favorable, adverse
        self._path_tensors[key] = result
        return result

    @staticmethod
    def _first_true_index(values: np.ndarray, *, offset: int) -> int | None:
        positions = np.flatnonzero(values)
        return None if not len(positions) else offset + int(positions[0])

    def _populate_fixed_exit_outcomes(
        self,
        order: FrozenEntryOrder,
        entry: EntryAttempt,
        path: OneSecondPath,
    ) -> None:
        """Resolve all 81 non-RULE exits with shared vectorized path queries."""

        fixed = tuple(item for item in build_exit_catalog().candidates if item.kind != "RULE")
        if all((order.order_id, item.exit_id, None, None) in self._exit_outcomes for item in fixed):
            return
        if (
            entry.entry_row_index is None
            or entry.entry_ns is None
            or entry.entry_fill_ticks is None
        ):
            raise SymbolicEngineError("fixed exit family requires a filled entry")
        starts, _opens, favorable, adverse = self._path_tensor(path, order.anchor.direction)
        entry_index = entry.entry_row_index
        maximum_cap_ns = entry.entry_ns + 21_600 * ONE_SECOND_NS
        interval = path.coverage_interval_at(entry.entry_ns)
        local_end_ns = entry.entry_ns if interval is None else interval[1]
        stop_index = int(np.searchsorted(starts, min(maximum_cap_ns, local_end_ns), side="left"))
        sign = _direction_sign(order.anchor.direction)
        entry_oriented = sign * entry.entry_fill_ticks

        bracket_event_cache: dict[tuple[int, int], tuple[str, int, int] | None] = {}
        bracket_stop_cache: dict[int, int | None] = {}
        bracket_target_cache: dict[int, int | None] = {}
        trailing_event_cache: dict[tuple[int, int], tuple[str, int, int] | None] = {}
        break_even_event_cache: dict[tuple[int, int], tuple[str, int, int] | None] = {}

        def bracket_event(take_profit: int, stop_loss: int) -> tuple[str, int, int] | None:
            key = take_profit, stop_loss
            if key in bracket_event_cache:
                return bracket_event_cache[key]
            if stop_loss not in bracket_stop_cache:
                bracket_stop_cache[stop_loss] = self._first_true_index(
                    adverse[entry_index:stop_index] <= stop_loss,
                    offset=entry_index,
                )
            if take_profit not in bracket_target_cache:
                bracket_target_cache[take_profit] = self._first_true_index(
                    favorable[entry_index + 1 : stop_index] >= take_profit,
                    offset=entry_index + 1,
                )
            stop_hit = bracket_stop_cache[stop_loss]
            target_hit = bracket_target_cache[take_profit]
            if stop_hit is not None and (target_hit is None or stop_hit <= target_hit):
                row = path.rows[stop_hit]
                reference = _stop_reference(
                    order.anchor.direction,
                    stop_loss,
                    row,
                    same_entry_second=stop_hit == entry_index,
                )
                result = "STOP_LOSS", stop_hit, reference
            elif target_hit is not None:
                result = "TAKE_PROFIT", target_hit, sign * take_profit
            else:
                result = None
            bracket_event_cache[key] = result
            return result

        def trailing_event(activation: int, distance: int) -> tuple[str, int, int] | None:
            key = activation, distance
            if key in trailing_event_cache:
                return trailing_event_cache[key]
            activation_index = self._first_true_index(
                favorable[entry_index + 1 : stop_index] >= activation,
                offset=entry_index + 1,
            )
            if activation_index is None:
                trailing_event_cache[key] = None
                return None
            running_high = np.maximum.accumulate(favorable[activation_index:stop_index])
            updated_stops = running_high - distance
            prior_stops = np.empty_like(updated_stops)
            prior_stops[0] = np.iinfo(np.int64).min
            prior_stops[1:] = updated_stops[:-1]
            prior_hits = adverse[activation_index:stop_index] <= prior_stops
            updated_hits = adverse[activation_index:stop_index] <= updated_stops
            local = self._first_true_index(
                prior_hits | updated_hits,
                offset=activation_index,
            )
            if local is None:
                result = None
            else:
                position = local - activation_index
                if bool(prior_hits[position]):
                    oriented_stop = int(prior_stops[position])
                    reference = _stop_reference(
                        order.anchor.direction,
                        oriented_stop,
                        path.rows[local],
                        same_entry_second=False,
                    )
                else:
                    reference = sign * int(updated_stops[position])
                result = "TRAILING_STOP", local, reference
            trailing_event_cache[key] = result
            return result

        def break_even_event(activation: int, initial_stop: int) -> tuple[str, int, int] | None:
            key = activation, initial_stop
            if key in break_even_event_cache:
                return break_even_event_cache[key]
            initial_hit = self._first_true_index(
                adverse[entry_index:stop_index] <= initial_stop,
                offset=entry_index,
            )
            activation_index = self._first_true_index(
                favorable[entry_index + 1 : stop_index] >= activation,
                offset=entry_index + 1,
            )
            if initial_hit is not None and (
                activation_index is None or initial_hit <= activation_index
            ):
                reference = _stop_reference(
                    order.anchor.direction,
                    initial_stop,
                    path.rows[initial_hit],
                    same_entry_second=initial_hit == entry_index,
                )
                result = "INITIAL_STOP", initial_hit, reference
            elif activation_index is None:
                result = None
            else:
                break_even_hit = self._first_true_index(
                    adverse[activation_index:stop_index] <= entry_oriented,
                    offset=activation_index,
                )
                if break_even_hit is None:
                    result = None
                elif break_even_hit == activation_index:
                    result = "BREAK_EVEN_STOP", break_even_hit, sign * entry_oriented
                else:
                    result = (
                        "BREAK_EVEN_STOP",
                        break_even_hit,
                        _stop_reference(
                            order.anchor.direction,
                            entry_oriented,
                            path.rows[break_even_hit],
                            same_entry_second=False,
                        ),
                    )
            break_even_event_cache[key] = result
            return result

        for exit_policy in fixed:
            cap_ns = entry.entry_ns + exit_policy.cap_seconds * ONE_SECOND_NS
            event: tuple[str, int, int] | None = None
            if exit_policy.kind == "TERMINAL":
                outcome = _terminal_exit(order, entry, path, cap_ns)
            elif exit_policy.kind == "BRACKET":
                take_profit = entry_oriented + _atr_distance(
                    order.anchor,
                    exit_policy.fraction_parameter("take_profit_atr"),
                )
                stop_loss = entry_oriented - _atr_distance(
                    order.anchor,
                    exit_policy.fraction_parameter("stop_loss_atr"),
                )
                event = bracket_event(take_profit, stop_loss)
                outcome = _terminal_exit(order, entry, path, cap_ns)
            elif exit_policy.kind == "TRAILING":
                activation = entry_oriented + _atr_distance(
                    order.anchor,
                    exit_policy.fraction_parameter("activation_atr"),
                )
                distance = _atr_distance(
                    order.anchor,
                    exit_policy.fraction_parameter("trail_atr"),
                )
                event = trailing_event(activation, distance)
                outcome = _terminal_exit(order, entry, path, cap_ns)
            else:
                activation = entry_oriented + _atr_distance(
                    order.anchor,
                    exit_policy.fraction_parameter("activation_atr"),
                )
                initial_stop = entry_oriented - _atr_distance(
                    order.anchor,
                    exit_policy.fraction_parameter("initial_stop_atr"),
                )
                event = break_even_event(activation, initial_stop)
                outcome = _terminal_exit(order, entry, path, cap_ns)
            if event is not None:
                reason, row_index, reference = event
                row = path.rows[row_index]
                if row.bar.start_ns < cap_ns:
                    outcome = _filled_exit(
                        order,
                        entry,
                        reason=reason,
                        row_index=row_index,
                        exit_ns=row.bar.end_ns,
                        exit_reference_ticks=reference,
                    )
            self._exit_outcomes[order.order_id, exit_policy.exit_id, None, None] = outcome

    def evaluate(
        self,
        recipe: CompleteStrategyRecipe,
        mask: PolicyMask,
        *,
        rule_schedule: RuleExitSchedule | None,
        reporting_group_by_date: Mapping[date, str],
        outer_validation_by_date: Mapping[date, str],
    ) -> CompleteStrategyEvaluation:
        """Evaluate only mask anchors whose lineage is present in this path batch."""

        if recipe.anchor_policy_id != mask.policy.policy_id:
            raise SymbolicEngineError("recipe and mask anchor policy differ")
        entry_policy = _entry_lookup()[recipe.entry_policy_id]
        exit_policy = _exit_lookup()[recipe.exit_policy_id]
        if exit_policy.kind == "RULE":
            if rule_schedule is None or rule_schedule.policy_id != mask.policy.policy_id:
                raise SymbolicEngineError("rule strategy requires its exact feature-only schedule")
            rule_lookup = rule_schedule.lookup()
        else:
            rule_lookup = {}
        if not reporting_group_by_date or not outer_validation_by_date:
            raise SymbolicEngineError("complete evaluation requires both group mappings")
        reporting_keys = tuple(sorted(set(reporting_group_by_date.values())))
        outer_keys = tuple(sorted(set(outer_validation_by_date.values())))
        if not reporting_keys or not outer_keys:
            raise SymbolicEngineError("complete evaluation group mappings are empty")

        scoped = tuple(
            sorted(
                (
                    anchor
                    for anchor in mask.records
                    if (anchor.contract, anchor.outcome_span_id, anchor.segment_id) in self.paths
                ),
                key=lambda item: (
                    item.anchor_ns,
                    item.contract,
                    item.outcome_span_id,
                    item.segment_id,
                    item.direction,
                ),
            )
        )
        orders = self._orders(mask, entry_policy)
        reporting_raw: dict[str, int] = defaultdict(int)
        reporting_signal_dates: dict[str, set[date]] = defaultdict(set)
        reporting_fills: dict[str, int] = defaultdict(int)
        reporting_entry_dates: dict[str, set[date]] = defaultdict(set)
        reporting_censored: dict[str, int] = defaultdict(int)
        reporting_net: dict[str, int] = defaultdict(int)
        reporting_stress: dict[str, int] = defaultdict(int)
        reporting_net_values: dict[str, list[int]] = defaultdict(list)
        outer_raw: dict[str, int] = defaultdict(int)
        outer_signal_dates: dict[str, set[date]] = defaultdict(set)
        outer_fills: dict[str, int] = defaultdict(int)
        outer_entry_dates: dict[str, set[date]] = defaultdict(set)
        outer_censored: dict[str, int] = defaultdict(int)
        outer_net: dict[str, int] = defaultdict(int)
        outer_stress: dict[str, int] = defaultdict(int)
        outer_net_values: dict[str, list[int]] = defaultdict(list)
        active_signal_dates: set[date] = set()
        active_entry_dates: set[date] = set()
        contracts: set[str] = set()
        net_values: list[int] = []
        reference_total = 0
        gross_total = 0
        stress_total = 0
        skipped = 0
        unfilled = 0
        cancelled = 0
        censored = 0
        occupied_until_ns = -1
        unknown_occupancy = False
        behavior_by_lineage: dict[tuple[str, int, int], list[dict[str, object]]] = {
            lineage: [] for lineage in self.paths
        }
        anchor_keys_by_lineage: dict[
            tuple[str, int, int], list[tuple[str, int, int, int, Direction]]
        ] = {lineage: [] for lineage in self.paths}

        for anchor in scoped:
            lineage = anchor.contract, anchor.outcome_span_id, anchor.segment_id
            anchor_keys_by_lineage[lineage].append(anchor.outcome_key)
            reporting_group = reporting_group_by_date.get(anchor.source_date)
            outer_group = outer_validation_by_date.get(anchor.source_date)
            if reporting_group is not None:
                reporting_raw[reporting_group] += 1
                reporting_signal_dates[reporting_group].add(anchor.source_date)
            if outer_group is not None:
                outer_raw[outer_group] += 1
                outer_signal_dates[outer_group].add(anchor.source_date)
            active_signal_dates.add(anchor.source_date)
            if unknown_occupancy:
                censored += 1
                if reporting_group is not None:
                    reporting_censored[reporting_group] += 1
                if outer_group is not None:
                    outer_censored[outer_group] += 1
                behavior_by_lineage[lineage].append(
                    {
                        "anchor_key": list(anchor.outcome_key),
                        "status": "SKIPPED_AFTER_UNKNOWN_OCCUPANCY",
                    }
                )
                continue
            if anchor.anchor_ns < occupied_until_ns:
                skipped += 1
                behavior_by_lineage[lineage].append(
                    {"anchor_key": list(anchor.outcome_key), "status": "SKIPPED_OCCUPIED"}
                )
                continue
            order = orders[anchor]
            path = self.paths[lineage]
            attempt = self._entry(order, path)
            if attempt.status == "UNFILLED":
                unfilled += 1
                behavior_by_lineage[lineage].append(
                    {"anchor_key": list(anchor.outcome_key), "status": attempt.reason}
                )
                continue
            if attempt.status == "CENSORED":
                censored += 1
                if reporting_group is not None:
                    reporting_censored[reporting_group] += 1
                if outer_group is not None:
                    outer_censored[outer_group] += 1
                behavior_by_lineage[lineage].append(
                    {"anchor_key": list(anchor.outcome_key), "status": attempt.reason}
                )
                continue
            outcome = self._exit(
                order,
                attempt,
                exit_policy,
                path,
                rule_lookup.get(anchor.outcome_key),
            )
            if outcome.status == "CANCELLED_BEFORE_ENTRY":
                cancelled += 1
                behavior_by_lineage[lineage].append(
                    {"anchor_key": list(anchor.outcome_key), "status": outcome.reason}
                )
                continue
            if not outcome.filled:
                censored += 1
                if reporting_group is not None:
                    reporting_censored[reporting_group] += 1
                if outer_group is not None:
                    outer_censored[outer_group] += 1
                unknown_occupancy = True
                behavior_by_lineage[lineage].append(
                    {"anchor_key": list(anchor.outcome_key), "status": outcome.reason}
                )
                continue
            if (
                outcome.net_pnl_ticks is None
                or outcome.stress_net_pnl_ticks is None
                or outcome.reference_pnl_ticks is None
                or outcome.gross_pnl_ticks is None
                or outcome.exit_ns is None
            ):
                raise SymbolicEngineError("filled outcome lost required accounting fields")
            occupied_until_ns = outcome.occupied_until_ns
            active_entry_dates.add(anchor.source_date)
            contracts.add(anchor.contract)
            net_values.append(outcome.net_pnl_ticks)
            reference_total += outcome.reference_pnl_ticks
            gross_total += outcome.gross_pnl_ticks
            stress_total += outcome.stress_net_pnl_ticks
            if reporting_group is not None:
                reporting_fills[reporting_group] += 1
                reporting_entry_dates[reporting_group].add(anchor.source_date)
                reporting_net[reporting_group] += outcome.net_pnl_ticks
                reporting_stress[reporting_group] += outcome.stress_net_pnl_ticks
                reporting_net_values[reporting_group].append(outcome.net_pnl_ticks)
            if outer_group is not None:
                outer_fills[outer_group] += 1
                outer_entry_dates[outer_group].add(anchor.source_date)
                outer_net[outer_group] += outcome.net_pnl_ticks
                outer_stress[outer_group] += outcome.stress_net_pnl_ticks
                outer_net_values[outer_group].append(outcome.net_pnl_ticks)
            behavior_by_lineage[lineage].append(
                {
                    "anchor_key": list(anchor.outcome_key),
                    "entry_fill_ticks": attempt.entry_fill_ticks,
                    "entry_ns": attempt.entry_ns,
                    "exit_fill_ticks": outcome.exit_fill_ticks,
                    "exit_ns": outcome.exit_ns,
                    "net_ticks": outcome.net_pnl_ticks,
                    "reason": outcome.reason,
                    "status": "FILLED",
                }
            )

        maximum_drawdown, maximum_prefix, minimum_prefix = _equity_shape(net_values)
        lineages = tuple(
            sorted(
                self.paths,
                key=lambda key: (self.paths[key].coverage_start_ns, key),
            )
        )

        def group_row(
            key: str,
            raw: Mapping[str, int],
            signal_dates: Mapping[str, set[date]],
            fills: Mapping[str, int],
            entry_dates: Mapping[str, set[date]],
            censored_values: Mapping[str, int],
            nets: Mapping[str, int],
            stresses: Mapping[str, int],
            net_sequences: Mapping[str, list[int]],
        ) -> GroupStrategyAggregate:
            sequence = net_sequences.get(key, [])
            drawdown, maximum_prefix, minimum_prefix = _equity_shape(sequence)
            return GroupStrategyAggregate(
                key,
                raw.get(key, 0),
                tuple(sorted(signal_dates.get(key, set()))),
                fills.get(key, 0),
                tuple(sorted(entry_dates.get(key, set()))),
                censored_values.get(key, 0),
                nets.get(key, 0),
                stresses.get(key, 0),
                sum(value for value in sequence if value > 0),
                -sum(value for value in sequence if value < 0),
                drawdown,
                maximum_prefix,
                minimum_prefix,
            )

        reporting = tuple(
            group_row(
                key,
                reporting_raw,
                reporting_signal_dates,
                reporting_fills,
                reporting_entry_dates,
                reporting_censored,
                reporting_net,
                reporting_stress,
                reporting_net_values,
            )
            for key in reporting_keys
        )
        outer = tuple(
            group_row(
                key,
                outer_raw,
                outer_signal_dates,
                outer_fills,
                outer_entry_dates,
                outer_censored,
                outer_net,
                outer_stress,
                outer_net_values,
            )
            for key in outer_keys
        )
        leaves = tuple(canonical_sha256(behavior_by_lineage[key]) for key in lineages)
        anchor_counts = tuple(len(anchor_keys_by_lineage[key]) for key in lineages)
        anchor_leaves = tuple(
            canonical_sha256([list(item) for item in anchor_keys_by_lineage[key]])
            for key in lineages
        )
        definition: dict[str, object] = {
            "active_entry_days": len(active_entry_dates),
            "active_entry_dates": [item.isoformat() for item in sorted(active_entry_dates)],
            "active_signal_days": len(active_signal_dates),
            "active_signal_dates": [item.isoformat() for item in sorted(active_signal_dates)],
            "behavior_leaf_sha256s": list(leaves),
            "cancelled_entry_count": cancelled,
            "censored_count": censored,
            "contract_count": len(contracts),
            "contracts": sorted(contracts),
            "evaluated_lineages": [list(item) for item in lineages],
            "evaluated_anchor_counts": list(anchor_counts),
            "evaluated_anchor_leaf_sha256s": list(anchor_leaves),
            "evaluation_end_ns": max(item.coverage_end_ns for item in self.paths.values()),
            "evaluation_start_ns": min(item.coverage_start_ns for item in self.paths.values()),
            "fill_count": len(net_values),
            "gross_loss_ticks": -sum(value for value in net_values if value < 0),
            "gross_profit_ticks": sum(value for value in net_values if value > 0),
            "maximum_drawdown_ticks": maximum_drawdown,
            "maximum_prefix_equity_ticks": maximum_prefix,
            "minimum_prefix_equity_ticks": minimum_prefix,
            "outer_validations": [item.as_dict() for item in outer],
            "profit_factor_denominator": None,
            "profit_factor_numerator": None,
            "raw_signal_count": len(scoped),
            "recipe": recipe.as_dict(),
            "reporting_groups": [item.as_dict() for item in reporting],
            "schema": PATH_OUTCOME_SCHEMA,
            "skipped_occupied_count": skipped,
            "total_gross_pnl_ticks": gross_total,
            "total_net_ticks": sum(net_values),
            "total_reference_pnl_ticks": reference_total,
            "total_stress_net_ticks": stress_total,
            "unfilled_entry_count": unfilled,
        }
        gains = definition["gross_profit_ticks"]
        losses = definition["gross_loss_ticks"]
        if not isinstance(gains, int) or not isinstance(losses, int):  # pragma: no cover
            raise SymbolicEngineError("internal PnL aggregation type differs")
        profit_factor = (
            None
            if losses == 0 and gains == 0
            else (Fraction(10**18) if losses == 0 else Fraction(gains, losses))
        )
        definition["profit_factor_denominator"] = (
            None if profit_factor is None else profit_factor.denominator
        )
        definition["profit_factor_numerator"] = (
            None if profit_factor is None else profit_factor.numerator
        )
        return CompleteStrategyEvaluation(
            recipe,
            lineages,
            definition["evaluation_start_ns"],
            definition["evaluation_end_ns"],
            len(scoped),
            len(active_signal_dates),
            tuple(sorted(active_signal_dates)),
            skipped,
            unfilled,
            cancelled,
            censored,
            len(net_values),
            len(active_entry_dates),
            tuple(sorted(active_entry_dates)),
            len(contracts),
            tuple(sorted(contracts)),
            reference_total,
            gross_total,
            sum(net_values),
            stress_total,
            gains,
            losses,
            maximum_drawdown,
            maximum_prefix,
            minimum_prefix,
            reporting,
            outer,
            anchor_counts,
            anchor_leaves,
            leaves,
            canonical_sha256(definition),
        )


def evaluate_selected_strategy_details(
    evaluator: SharedPathEvaluator,
    requests: Sequence[SelectedStrategyDetailRequest],
    *,
    reporting_group_by_date: Mapping[date, str],
    outer_validation_by_date: Mapping[date, str],
) -> tuple[SelectedStrategyDetailedOutcome, ...]:
    """Replay at most 24 frozen selections into meta-ready per-signal rows."""

    values = tuple(requests)
    if not values or len(values) > 24:
        raise SymbolicEngineError("detailed evaluation requires between 1 and 24 selections")
    request_keys = tuple((item.scope_key, item.world, item.selection_rank) for item in values)
    if len(set(request_keys)) != len(request_keys):
        raise SymbolicEngineError("detailed evaluation requests duplicate a ranked slot")
    output: list[SelectedStrategyDetailedOutcome] = []
    for request in values:
        aggregate = evaluator.evaluate(
            request.recipe,
            request.mask,
            rule_schedule=request.rule_schedule,
            reporting_group_by_date=reporting_group_by_date,
            outer_validation_by_date=outer_validation_by_date,
        )
        entry_policy = _entry_lookup()[request.recipe.entry_policy_id]
        exit_policy = _exit_lookup()[request.recipe.exit_policy_id]
        if exit_policy.kind == "RULE":
            if (
                request.rule_schedule is None
                or request.rule_schedule.policy_id != request.mask.policy.policy_id
            ):
                raise SymbolicEngineError("detailed RULE replay requires exact schedule")
            rule_lookup = request.rule_schedule.lookup()
        else:
            rule_lookup = {}
        scoped = tuple(
            sorted(
                (
                    anchor
                    for anchor in request.mask.records
                    if (anchor.contract, anchor.outcome_span_id, anchor.segment_id)
                    in evaluator.paths
                ),
                key=lambda item: (
                    item.anchor_ns,
                    item.contract,
                    item.outcome_span_id,
                    item.segment_id,
                    item.direction,
                ),
            )
        )
        orders = evaluator._orders(request.mask, entry_policy)
        occupied_until_ns = -1
        unknown_occupancy = False
        rows: list[StrategyTradeOutcomeRow] = []
        for anchor in scoped:
            status = ""
            reason = ""
            censored = False
            attempt: EntryAttempt | None = None
            outcome: ExitOutcome | None = None
            if unknown_occupancy:
                status = "SKIPPED_AFTER_UNKNOWN_OCCUPANCY"
                reason = "PRIOR_FILLED_POSITION_EXIT_CENSORED"
                censored = True
            elif anchor.anchor_ns < occupied_until_ns:
                status = "SKIPPED_OCCUPIED"
                reason = "MAXIMUM_CONCURRENT_POSITIONS_ONE"
            else:
                order = orders[anchor]
                path = evaluator.paths[
                    anchor.contract,
                    anchor.outcome_span_id,
                    anchor.segment_id,
                ]
                attempt = evaluator._entry(order, path)
                if attempt.status == "UNFILLED":
                    status = "UNFILLED"
                    reason = attempt.reason
                elif attempt.status == "CENSORED":
                    status = "CENSORED"
                    reason = attempt.reason
                    censored = True
                else:
                    outcome = evaluator._exit(
                        order,
                        attempt,
                        exit_policy,
                        path,
                        rule_lookup.get(anchor.outcome_key),
                    )
                    if outcome.status == "CANCELLED_BEFORE_ENTRY":
                        status = "CANCELLED_BEFORE_ENTRY"
                        reason = outcome.reason
                    elif not outcome.filled:
                        status = "CENSORED"
                        reason = outcome.reason
                        censored = True
                        unknown_occupancy = True
                    else:
                        status = "FILLED"
                        reason = outcome.reason
                        occupied_until_ns = outcome.occupied_until_ns
            rows.append(
                StrategyTradeOutcomeRow(
                    anchor.outcome_key,
                    anchor.source_date,
                    anchor.direction,
                    anchor.trigger_start_ns,
                    anchor.trigger_end_ns,
                    anchor.atr_sum_ticks,
                    anchor.atr_denominator,
                    status,
                    reason,
                    not censored,
                    censored,
                    None if attempt is None else attempt.entry_ns,
                    None if outcome is None else outcome.exit_ns,
                    None if attempt is None else attempt.entry_reference_ticks,
                    None if attempt is None else attempt.entry_fill_ticks,
                    None if outcome is None else outcome.exit_reference_ticks,
                    None if outcome is None else outcome.exit_fill_ticks,
                    None if outcome is None else outcome.reference_pnl_ticks,
                    None if outcome is None else outcome.gross_pnl_ticks,
                    None if outcome is None else outcome.net_pnl_ticks,
                    None if outcome is None else outcome.stress_net_pnl_ticks,
                    reporting_group_by_date.get(anchor.source_date),
                    outer_validation_by_date.get(anchor.source_date),
                )
            )
        canonical_rows = tuple(rows)
        if (
            len(canonical_rows) != aggregate.raw_signal_count
            or sum(item.status == "FILLED" for item in canonical_rows) != aggregate.fill_count
            or sum(item.status == "UNFILLED" for item in canonical_rows)
            != aggregate.unfilled_entry_count
            or sum(item.status == "SKIPPED_OCCUPIED" for item in canonical_rows)
            != aggregate.skipped_occupied_count
            or sum(item.status == "CANCELLED_BEFORE_ENTRY" for item in canonical_rows)
            != aggregate.cancelled_entry_count
            or sum(item.censored for item in canonical_rows) != aggregate.censored_count
            or sum(item.net_pnl_ticks or 0 for item in canonical_rows) != aggregate.total_net_ticks
        ):
            raise SymbolicEngineError("detailed rows do not reproduce compact evaluation")
        definition = {
            "evaluation_artifact_sha256": aggregate.artifact_sha256,
            "recipe": request.recipe.as_dict(),
            "rows": [item.as_dict() for item in canonical_rows],
            "schema": PATH_OUTCOME_SCHEMA,
            "scope_key": request.scope_key,
            "selection_rank": request.selection_rank,
            "world": request.world,
        }
        output.append(
            SelectedStrategyDetailedOutcome(
                request.selection_rank,
                request.scope_key,
                request.world,
                request.recipe,
                aggregate.artifact_sha256,
                canonical_rows,
                canonical_sha256(definition),
            )
        )
    return tuple(output)


def stage_b_kernel_budget_projection(
    *,
    filled_anchor_entry_pairs: int = 100_000,
    maximum_observed_rows_per_path: int = 21_600,
    maximum_support_per_mask: int = 14_842,
    loaded_one_second_row_count: int = 7_573_041,
) -> dict[str, object]:
    """Conservative deterministic extrapolation for the shared Stage-B kernel."""

    _require_int(
        filled_anchor_entry_pairs,
        label="filled_anchor_entry_pairs",
        minimum=100_000,
    )
    _require_int(
        maximum_observed_rows_per_path,
        label="maximum_observed_rows_per_path",
        minimum=1,
    )
    _require_int(maximum_support_per_mask, label="maximum_support_per_mask", minimum=1)
    _require_int(
        loaded_one_second_row_count,
        label="loaded_one_second_row_count",
        minimum=1,
    )
    vector_passes_per_pair = 41
    scanned_int64_bytes = (
        filled_anchor_entry_pairs * maximum_observed_rows_per_path * vector_passes_per_pair * 8
    )
    conservative_numpy_bytes_per_second = 25_000_000
    fixed_outcome_rows = filled_anchor_entry_pairs * (EXIT_POLICY_COUNT - 4)
    conservative_fixed_rows_per_second = 1_000
    cached_exit_outcome_bytes_upper_bound = 768
    cached_entry_order_bytes_upper_bound = 512
    peak_shared_kernel_cache_bytes_upper_bound = (
        maximum_support_per_mask
        * (
            (EXIT_POLICY_COUNT - 4) * cached_exit_outcome_bytes_upper_bound
            + cached_entry_order_bytes_upper_bound
        )
        + loaded_one_second_row_count * 4 * 8 * 2
    )
    projected_seconds = (
        scanned_int64_bytes + conservative_numpy_bytes_per_second - 1
    ) // conservative_numpy_bytes_per_second + (
        fixed_outcome_rows + conservative_fixed_rows_per_second - 1
    ) // conservative_fixed_rows_per_second
    definition = {
        "conservative_fixed_rows_per_second": conservative_fixed_rows_per_second,
        "conservative_numpy_bytes_per_second": conservative_numpy_bytes_per_second,
        "filled_anchor_entry_pairs": filled_anchor_entry_pairs,
        "fixed_exit_outcome_rows": fixed_outcome_rows,
        "loaded_one_second_row_count": loaded_one_second_row_count,
        "maximum_support_per_mask": maximum_support_per_mask,
        "maximum_observed_rows_per_path": maximum_observed_rows_per_path,
        "peak_shared_kernel_cache_bytes_upper_bound": peak_shared_kernel_cache_bytes_upper_bound,
        "projected_seconds": projected_seconds,
        "python_full_path_iterations": 0,
        "scanned_int64_bytes_upper_bound": scanned_int64_bytes,
        "schema": PATH_OUTCOME_SCHEMA,
        "vector_passes_per_pair_upper_bound": vector_passes_per_pair,
        "cache_release_unit": "ONE_MASK_ONE_ENTRY_AFTER_85_RECIPES",
        "within_24h": projected_seconds < 86_400,
    }
    return {**definition, "artifact_sha256": canonical_sha256(definition)}


def _merge_groups(
    parts: Sequence[tuple[GroupStrategyAggregate, ...]],
) -> tuple[GroupStrategyAggregate, ...]:
    key_sets = [tuple(item.group_key for item in rows) for rows in parts]
    if not key_sets or any(keys != key_sets[0] for keys in key_sets[1:]):
        raise SymbolicEngineError("aggregate group keys differ across path chunks")
    output = []
    for index, key in enumerate(key_sets[0]):
        values = [rows[index] for rows in parts]
        total = 0
        maximum_prefix = 0
        minimum_prefix = 0
        maximum_drawdown = 0
        peak = 0
        for item in values:
            maximum_drawdown = max(
                maximum_drawdown,
                item.maximum_drawdown_ticks,
                peak - (total + item.minimum_prefix_equity_ticks),
            )
            maximum_prefix = max(
                maximum_prefix,
                total + item.maximum_prefix_equity_ticks,
            )
            minimum_prefix = min(
                minimum_prefix,
                total + item.minimum_prefix_equity_ticks,
            )
            total += item.net_ticks
            peak = maximum_prefix
        output.append(
            GroupStrategyAggregate(
                key,
                sum(item.raw_signal_count for item in values),
                tuple(sorted({day for item in values for day in item.active_signal_dates})),
                sum(item.fill_count for item in values),
                tuple(sorted({day for item in values for day in item.active_entry_dates})),
                sum(item.censored_count for item in values),
                total,
                sum(item.stress_net_ticks for item in values),
                sum(item.gross_profit_ticks for item in values),
                sum(item.gross_loss_ticks for item in values),
                maximum_drawdown,
                maximum_prefix,
                minimum_prefix,
            )
        )
    return tuple(output)


def merge_complete_strategy_evaluations(
    parts: Sequence[CompleteStrategyEvaluation],
) -> CompleteStrategyEvaluation:
    """Merge disjoint chronological loader chunks without retaining trade rows."""

    values = tuple(sorted(parts, key=lambda item: item.evaluation_start_ns))
    if not values:
        raise SymbolicEngineError("cannot merge no complete strategy evaluations")
    if len(values) == 1:
        return values[0]
    recipe = values[0].recipe
    if any(item.recipe != recipe for item in values[1:]):
        raise SymbolicEngineError("complete evaluation recipes differ")
    if any(left.evaluation_end_ns > right.evaluation_start_ns for left, right in pairwise(values)):
        raise SymbolicEngineError("complete evaluation path chunks overlap")
    all_lineages = [lineage for item in values for lineage in item.evaluated_lineages]
    if len(set(all_lineages)) != len(all_lineages):
        raise SymbolicEngineError("complete evaluation chunks repeat a lineage")

    total = 0
    maximum_prefix = 0
    minimum_prefix = 0
    maximum_drawdown = 0
    peak = 0
    for item in values:
        maximum_drawdown = max(
            maximum_drawdown,
            item.maximum_drawdown_ticks,
            peak - (total + item.minimum_prefix_equity_ticks),
        )
        maximum_prefix = max(maximum_prefix, total + item.maximum_prefix_equity_ticks)
        minimum_prefix = min(minimum_prefix, total + item.minimum_prefix_equity_ticks)
        total += item.total_net_ticks
        peak = maximum_prefix
    signal_dates = tuple(sorted({day for item in values for day in item.active_signal_date_values}))
    entry_dates = tuple(sorted({day for item in values for day in item.active_entry_date_values}))
    contracts = tuple(sorted({value for item in values for value in item.contract_values}))
    lineages = tuple(lineage for item in values for lineage in item.evaluated_lineages)
    anchor_counts = tuple(count for item in values for count in item.evaluated_anchor_counts)
    anchor_leaves = tuple(leaf for item in values for leaf in item.evaluated_anchor_leaf_sha256s)
    leaves = tuple(leaf for item in values for leaf in item.behavior_leaf_sha256s)
    reporting = _merge_groups([item.reporting_groups for item in values])
    outer = _merge_groups([item.outer_validations for item in values])
    gains = sum(item.gross_profit_ticks for item in values)
    losses = sum(item.gross_loss_ticks for item in values)
    profit_factor = (
        None
        if losses == 0 and gains == 0
        else (Fraction(10**18) if losses == 0 else Fraction(gains, losses))
    )
    definition = {
        "active_entry_days": len(entry_dates),
        "active_entry_dates": [item.isoformat() for item in entry_dates],
        "active_signal_days": len(signal_dates),
        "active_signal_dates": [item.isoformat() for item in signal_dates],
        "behavior_leaf_sha256s": list(leaves),
        "cancelled_entry_count": sum(item.cancelled_entry_count for item in values),
        "censored_count": sum(item.censored_count for item in values),
        "contract_count": len(contracts),
        "contracts": list(contracts),
        "evaluated_lineages": [list(item) for item in lineages],
        "evaluated_anchor_counts": list(anchor_counts),
        "evaluated_anchor_leaf_sha256s": list(anchor_leaves),
        "evaluation_end_ns": values[-1].evaluation_end_ns,
        "evaluation_start_ns": values[0].evaluation_start_ns,
        "fill_count": sum(item.fill_count for item in values),
        "gross_loss_ticks": losses,
        "gross_profit_ticks": gains,
        "maximum_drawdown_ticks": maximum_drawdown,
        "maximum_prefix_equity_ticks": maximum_prefix,
        "minimum_prefix_equity_ticks": minimum_prefix,
        "outer_validations": [item.as_dict() for item in outer],
        "profit_factor_denominator": None if profit_factor is None else profit_factor.denominator,
        "profit_factor_numerator": None if profit_factor is None else profit_factor.numerator,
        "raw_signal_count": sum(item.raw_signal_count for item in values),
        "recipe": recipe.as_dict(),
        "reporting_groups": [item.as_dict() for item in reporting],
        "schema": PATH_OUTCOME_SCHEMA,
        "skipped_occupied_count": sum(item.skipped_occupied_count for item in values),
        "total_gross_pnl_ticks": sum(item.total_gross_pnl_ticks for item in values),
        "total_net_ticks": total,
        "total_reference_pnl_ticks": sum(item.total_reference_pnl_ticks for item in values),
        "total_stress_net_ticks": sum(item.total_stress_net_ticks for item in values),
        "unfilled_entry_count": sum(item.unfilled_entry_count for item in values),
    }
    return CompleteStrategyEvaluation(
        recipe,
        lineages,
        values[0].evaluation_start_ns,
        values[-1].evaluation_end_ns,
        definition["raw_signal_count"],
        len(signal_dates),
        signal_dates,
        definition["skipped_occupied_count"],
        definition["unfilled_entry_count"],
        definition["cancelled_entry_count"],
        definition["censored_count"],
        definition["fill_count"],
        len(entry_dates),
        entry_dates,
        len(contracts),
        contracts,
        definition["total_reference_pnl_ticks"],
        definition["total_gross_pnl_ticks"],
        total,
        definition["total_stress_net_ticks"],
        gains,
        losses,
        maximum_drawdown,
        maximum_prefix,
        minimum_prefix,
        reporting,
        outer,
        anchor_counts,
        anchor_leaves,
        leaves,
        canonical_sha256(definition),
    )


@dataclass(frozen=True, slots=True)
class CompleteEvaluationCoverage:
    evaluation_artifact_sha256: str
    expected_mask_sha256: str
    expected_anchor_count: int
    expected_anchor_set_sha256: str
    artifact_sha256: str

    def definition_dict(self) -> dict[str, object]:
        return {
            "evaluation_artifact_sha256": self.evaluation_artifact_sha256,
            "expected_anchor_count": self.expected_anchor_count,
            "expected_anchor_set_sha256": self.expected_anchor_set_sha256,
            "expected_mask_sha256": self.expected_mask_sha256,
            "schema": PATH_OUTCOME_SCHEMA,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def verify_complete_evaluation_coverage(
    evaluation: CompleteStrategyEvaluation,
    expected_mask: PolicyMask,
) -> CompleteEvaluationCoverage:
    """Fail closed unless merged path chunks cover every frozen evaluable anchor once."""

    if evaluation.recipe.anchor_policy_id != expected_mask.policy.policy_id:
        raise SymbolicEngineError("coverage evaluation and expected mask policy differ")
    expected_by_lineage: dict[tuple[str, int, int], list[tuple[str, int, int, int, Direction]]] = (
        defaultdict(list)
    )
    for anchor in sorted(
        expected_mask.records,
        key=lambda item: (
            item.anchor_ns,
            item.contract,
            item.outcome_span_id,
            item.segment_id,
            item.direction,
        ),
    ):
        expected_by_lineage[
            anchor.contract,
            anchor.outcome_span_id,
            anchor.segment_id,
        ].append(anchor.outcome_key)
    expected = {
        lineage: (len(keys), canonical_sha256([list(item) for item in keys]))
        for lineage, keys in expected_by_lineage.items()
    }
    observed = {
        lineage: (count, leaf)
        for lineage, count, leaf in zip(
            evaluation.evaluated_lineages,
            evaluation.evaluated_anchor_counts,
            evaluation.evaluated_anchor_leaf_sha256s,
            strict=True,
        )
        if count != 0
    }
    if (
        observed != expected
        or evaluation.raw_signal_count != expected_mask.support_count
        or sum(count for count, _leaf in observed.values()) != expected_mask.support_count
    ):
        raise SymbolicEngineError("complete evaluation silently omitted or added anchors")
    expected_anchor_set_sha = canonical_sha256([item.as_dict() for item in expected_mask.records])
    definition = {
        "evaluation_artifact_sha256": evaluation.artifact_sha256,
        "expected_anchor_count": expected_mask.support_count,
        "expected_anchor_set_sha256": expected_anchor_set_sha,
        "expected_mask_sha256": expected_mask.mask_sha256,
        "schema": PATH_OUTCOME_SCHEMA,
    }
    return CompleteEvaluationCoverage(
        evaluation.artifact_sha256,
        expected_mask.mask_sha256,
        expected_mask.support_count,
        expected_anchor_set_sha,
        canonical_sha256(definition),
    )


def iter_complete_strategy_evaluation_chunks(
    recipes: Iterable[CompleteStrategyRecipe],
    *,
    masks_by_policy_id: Mapping[str, PolicyMask],
    paths: Sequence[OneSecondPath],
    rule_schedules_by_policy_id: Mapping[str, RuleExitSchedule],
    reporting_group_by_date: Mapping[date, str],
    outer_validation_by_date: Mapping[date, str],
    batch_size: int = 64,
) -> Iterator[CompleteEvaluationChunk]:
    """Stream compact, shared-path aggregates without materializing the strategy grid."""

    _require_int(batch_size, label="complete evaluation batch_size", minimum=1)
    if batch_size > 256:
        raise SymbolicEngineError("complete evaluation batch_size must be <= 256")
    evaluator = SharedPathEvaluator(paths)
    pending: list[CompleteStrategyEvaluation] = []
    prior_rank = 0
    active_mask: PolicyMask | None = None
    active_entry_policy_id: str | None = None
    try:
        for recipe in recipes:
            if recipe.strategy_rank <= prior_rank:
                raise SymbolicEngineError("complete recipes must use increasing rank")
            prior_rank = recipe.strategy_rank
            mask = masks_by_policy_id.get(recipe.anchor_policy_id)
            if mask is None:
                raise SymbolicEngineError("complete recipe lacks its selected feature mask")
            if active_mask is not None and (
                mask.mask_sha256 != active_mask.mask_sha256
                or recipe.entry_policy_id != active_entry_policy_id
            ):
                if active_entry_policy_id is None:  # pragma: no cover - paired assignment
                    raise SymbolicEngineError("active entry cache state differs")
                evaluator.release_entry_batch(active_mask, active_entry_policy_id)
            active_mask = mask
            active_entry_policy_id = recipe.entry_policy_id
            evaluation = evaluator.evaluate(
                recipe,
                mask,
                rule_schedule=rule_schedules_by_policy_id.get(recipe.anchor_policy_id),
                reporting_group_by_date=reporting_group_by_date,
                outer_validation_by_date=outer_validation_by_date,
            )
            pending.append(evaluation)
            if len(pending) == batch_size:
                yield CompleteEvaluationChunk.from_evaluations(pending)
                pending.clear()
    finally:
        if active_mask is not None and active_entry_policy_id is not None:
            evaluator.release_entry_batch(active_mask, active_entry_policy_id)
    if pending:
        yield CompleteEvaluationChunk.from_evaluations(pending)


@dataclass(frozen=True, slots=True)
class CompleteEvaluationAlias:
    alias_strategy_id: str
    representative_strategy_id: str
    behavior_sha256: str

    def as_dict(self) -> dict[str, object]:
        return {
            "alias_strategy_id": self.alias_strategy_id,
            "behavior_sha256": self.behavior_sha256,
            "representative_strategy_id": self.representative_strategy_id,
        }


@dataclass(frozen=True, slots=True)
class CompleteEvaluationDeduplication:
    representatives: tuple[CompleteStrategyEvaluation, ...]
    aliases: tuple[CompleteEvaluationAlias, ...]
    artifact_sha256: str

    def as_dict(self) -> dict[str, object]:
        return {
            "aliases": [item.as_dict() for item in self.aliases],
            "artifact_sha256": self.artifact_sha256,
            "representative_strategy_ids": [
                item.recipe.strategy_id for item in self.representatives
            ],
            "schema": PATH_OUTCOME_SCHEMA,
        }


def _complete_behavior_sha(evaluation: CompleteStrategyEvaluation) -> str:
    return canonical_sha256(
        {
            "evaluated_anchor_counts": list(evaluation.evaluated_anchor_counts),
            "evaluated_anchor_leaf_sha256s": list(evaluation.evaluated_anchor_leaf_sha256s),
            "behavior_leaf_sha256s": list(evaluation.behavior_leaf_sha256s),
            "evaluated_lineages": [list(item) for item in evaluation.evaluated_lineages],
            "schema": PATH_OUTCOME_SCHEMA,
        }
    )


def deduplicate_complete_evaluations(
    evaluations: Iterable[CompleteStrategyEvaluation],
) -> CompleteEvaluationDeduplication:
    """Collapse path-identical complete strategies after honest outcome evaluation."""

    ordered = tuple(sorted(evaluations, key=lambda item: item.recipe.strategy_rank))
    representatives: list[CompleteStrategyEvaluation] = []
    aliases: list[CompleteEvaluationAlias] = []
    seen: dict[str, CompleteStrategyEvaluation] = {}
    for evaluation in ordered:
        behavior_sha = _complete_behavior_sha(evaluation)
        prior = seen.get(behavior_sha)
        if prior is None:
            seen[behavior_sha] = evaluation
            representatives.append(evaluation)
        else:
            aliases.append(
                CompleteEvaluationAlias(
                    evaluation.recipe.strategy_id,
                    prior.recipe.strategy_id,
                    behavior_sha,
                )
            )
    definition = {
        "aliases": [item.as_dict() for item in aliases],
        "representative_strategy_ids": [item.recipe.strategy_id for item in representatives],
        "schema": PATH_OUTCOME_SCHEMA,
    }
    return CompleteEvaluationDeduplication(
        tuple(representatives),
        tuple(aliases),
        canonical_sha256(definition),
    )


@dataclass(frozen=True, slots=True)
class CompleteSearchGateResult:
    strategy_id: str
    eligible: bool
    rejection_reasons: tuple[str, ...]
    positive_reporting_group_count: int
    positive_outer_validation_count: int
    worst_outer_ev_numerator: int
    worst_outer_ev_denominator: int
    median_outer_ev_numerator: int
    median_outer_ev_denominator: int
    coverage_commitment_sha256s: tuple[str, str, str]
    artifact_sha256: str

    @property
    def worst_outer_ev(self) -> Fraction:
        return Fraction(self.worst_outer_ev_numerator, self.worst_outer_ev_denominator)

    @property
    def median_outer_ev(self) -> Fraction:
        return Fraction(self.median_outer_ev_numerator, self.median_outer_ev_denominator)

    def definition_dict(self) -> dict[str, object]:
        return {
            "eligible": self.eligible,
            "coverage_commitment_sha256s": list(self.coverage_commitment_sha256s),
            "median_outer_ev_denominator": self.median_outer_ev_denominator,
            "median_outer_ev_numerator": self.median_outer_ev_numerator,
            "positive_outer_validation_count": self.positive_outer_validation_count,
            "positive_reporting_group_count": self.positive_reporting_group_count,
            "rejection_reasons": list(self.rejection_reasons),
            "schema": SEARCH_GATE_SCHEMA,
            "strategy_id": self.strategy_id,
            "worst_outer_ev_denominator": self.worst_outer_ev_denominator,
            "worst_outer_ev_numerator": self.worst_outer_ev_numerator,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def apply_complete_search_gates(
    real: CompleteStrategyEvaluation,
    circular_target: CompleteStrategyEvaluation,
    matched_target: CompleteStrategyEvaluation,
    *,
    coverage_commitments: tuple[
        CompleteEvaluationCoverage,
        CompleteEvaluationCoverage,
        CompleteEvaluationCoverage,
    ],
) -> CompleteSearchGateResult:
    """Apply every predeclared Search sample/economic/null gate, without alpha testing."""

    if not (
        real.recipe.strategy_id
        == circular_target.recipe.strategy_id
        == matched_target.recipe.strategy_id
    ):
        raise SymbolicEngineError("Search control evaluations refer to different strategies")
    evaluations = (real, circular_target, matched_target)
    if len(coverage_commitments) != 3 or any(
        commitment.evaluation_artifact_sha256 != evaluation.artifact_sha256
        for commitment, evaluation in zip(coverage_commitments, evaluations, strict=True)
    ):
        raise SymbolicEngineError("Search gates require exact REAL/CIRCULAR/MATCHED coverage proof")
    oof_by_key = {item.group_key: item for item in real.outer_validations}
    if any(key not in oof_by_key for key in SEARCH_OOF_BLOCK_KEYS):
        raise SymbolicEngineError("Search evaluation lacks exact B3..B8 OOF blocks")
    oof_blocks = tuple(oof_by_key[key] for key in SEARCH_OOF_BLOCK_KEYS)
    reasons: list[str] = []
    if real.raw_signal_count < 60:
        reasons.append("RAW_SIGNALS_LT_60")
    if real.active_signal_days < 40:
        reasons.append("ACTIVE_SIGNAL_DAYS_LT_40")
    if any(item.raw_signal_count < 6 for item in real.reporting_groups):
        reasons.append("REPORTING_RAW_SIGNALS_LT_6")
    if any(item.raw_signal_count < 6 for item in oof_blocks):
        reasons.append("OUTER_RAW_SIGNALS_LT_6")
    if real.fill_count < 48:
        reasons.append("COMPLETE_FILLS_LT_48")
    if real.censored_count != 0:
        reasons.append("CENSORED_COUNT_NONZERO")
    if real.active_entry_days < 30:
        reasons.append("ACTIVE_ENTRY_DAYS_LT_30")
    if any(item.fill_count < 5 for item in oof_blocks):
        reasons.append("OUTER_COMPLETE_FILLS_LT_5")
    positive_reporting = sum(item.net_ticks > 0 for item in real.reporting_groups)
    if positive_reporting < 3:
        reasons.append("POSITIVE_REPORTING_GROUPS_LT_3")
    positive_outer = sum(item.net_ticks > 0 for item in oof_blocks)
    if positive_outer < 4:
        reasons.append("POSITIVE_OUTER_VALIDATIONS_LT_4")
    if real.total_net_ticks <= 0:
        reasons.append("NET_TICKS_NOT_POSITIVE")
    if real.total_stress_net_ticks <= 0:
        reasons.append("STRESS_18_TICK_NET_NOT_POSITIVE")
    profit_factor = real.profit_factor
    if profit_factor is None or profit_factor < Fraction(21, 20):
        reasons.append("PROFIT_FACTOR_LT_21_OVER_20")
    if real.total_net_ticks <= circular_target.total_net_ticks:
        reasons.append("REAL_NET_NOT_ABOVE_CIRCULAR_TARGET")
    if real.total_net_ticks <= matched_target.total_net_ticks:
        reasons.append("REAL_NET_NOT_ABOVE_MATCHED_TARGET")
    outer_evs = sorted(
        Fraction(item.net_ticks, item.fill_count) if item.fill_count else Fraction(-(10**18))
        for item in oof_blocks
    )
    worst = outer_evs[0] if outer_evs else Fraction(-(10**18))
    median = outer_evs[len(outer_evs) // 2] if outer_evs else Fraction(-(10**18))
    definition = {
        "coverage_commitment_sha256s": [item.artifact_sha256 for item in coverage_commitments],
        "eligible": not reasons,
        "median_outer_ev_denominator": median.denominator,
        "median_outer_ev_numerator": median.numerator,
        "positive_outer_validation_count": positive_outer,
        "positive_reporting_group_count": positive_reporting,
        "rejection_reasons": reasons,
        "schema": SEARCH_GATE_SCHEMA,
        "strategy_id": real.recipe.strategy_id,
        "worst_outer_ev_denominator": worst.denominator,
        "worst_outer_ev_numerator": worst.numerator,
    }
    return CompleteSearchGateResult(
        real.recipe.strategy_id,
        not reasons,
        tuple(reasons),
        positive_reporting,
        positive_outer,
        worst.numerator,
        worst.denominator,
        median.numerator,
        median.denominator,
        tuple(item.artifact_sha256 for item in coverage_commitments),
        canonical_sha256(definition),
    )


@dataclass(frozen=True, slots=True)
class CompleteSearchSelection:
    classification: str
    selected_strategy_ids: tuple[str, ...]
    eligible_count: int
    artifact_sha256: str

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_sha256": self.artifact_sha256,
            "classification": self.classification,
            "eligible_count": self.eligible_count,
            "schema": SEARCH_GATE_SCHEMA,
            "selected_strategy_ids": list(self.selected_strategy_ids),
        }


@dataclass(frozen=True, slots=True)
class SymbolicTop24Selection:
    scope_key: str
    selected_strategy_ids: tuple[str, ...]
    eligible_count: int
    artifact_sha256: str

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_sha256": self.artifact_sha256,
            "eligible_count": self.eligible_count,
            "schema": SEARCH_GATE_SCHEMA,
            "scope_key": self.scope_key,
            "selected_strategy_ids": list(self.selected_strategy_ids),
        }


def _select_symbolic_top24_from_complete_gates(
    scope_key: str,
    evaluations: Iterable[CompleteStrategyEvaluation],
    gate_results: Iterable[CompleteSearchGateResult],
) -> SymbolicTop24Selection:
    """Bind ranked slots 1..24 for one outer-training prefix or full Search."""

    if not scope_key:
        raise SymbolicEngineError("symbolic top24 scope key is empty")
    evaluation_by_id = {item.recipe.strategy_id: item for item in evaluations}
    gates = tuple(gate_results)
    if len({item.strategy_id for item in gates}) != len(gates):
        raise SymbolicEngineError("symbolic top24 gates contain duplicate strategies")
    eligible = [item for item in gates if item.eligible]
    if any(item.strategy_id not in evaluation_by_id for item in eligible):
        raise SymbolicEngineError("symbolic top24 gate lacks its real evaluation")
    eligible.sort(
        key=lambda item: (
            -item.positive_outer_validation_count,
            -item.worst_outer_ev,
            -evaluation_by_id[item.strategy_id].total_stress_net_ticks,
            -item.median_outer_ev,
            evaluation_by_id[item.strategy_id].maximum_drawdown_ticks,
            item.strategy_id,
        )
    )
    selected = tuple(item.strategy_id for item in eligible[:24])
    definition = {
        "eligible_count": len(eligible),
        "schema": SEARCH_GATE_SCHEMA,
        "scope_key": scope_key,
        "selected_strategy_ids": list(selected),
    }
    return SymbolicTop24Selection(
        scope_key,
        selected,
        len(eligible),
        canonical_sha256(definition),
    )


META_PREFIX_BLOCKS: Final = {
    "B3": ("B1", "B2"),
    "B4": ("B1", "B2", "B3"),
    "B5": ("B1", "B2", "B3", "B4"),
    "B6": ("B1", "B2", "B3", "B4", "B5"),
    "B7": ("B1", "B2", "B3", "B4", "B5", "B6"),
    "B8": ("B1", "B2", "B3", "B4", "B5", "B6", "B7"),
    "SEARCH_FINAL": ("B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8"),
}


@dataclass(frozen=True, slots=True)
class SymbolicMetaPrefixScore:
    scope_key: str
    strategy_id: str
    included_block_keys: tuple[str, ...]
    eligible: bool
    rejection_reasons: tuple[str, ...]
    positive_block_count: int
    worst_block_ev_numerator: int
    worst_block_ev_denominator: int
    median_block_ev_numerator: int
    median_block_ev_denominator: int
    stress_net_ticks: int
    maximum_drawdown_ticks: int
    artifact_sha256: str

    @property
    def worst_block_ev(self) -> Fraction:
        return Fraction(self.worst_block_ev_numerator, self.worst_block_ev_denominator)

    @property
    def median_block_ev(self) -> Fraction:
        return Fraction(self.median_block_ev_numerator, self.median_block_ev_denominator)

    def definition_dict(self) -> dict[str, object]:
        return {
            "eligible": self.eligible,
            "included_block_keys": list(self.included_block_keys),
            "maximum_drawdown_ticks": self.maximum_drawdown_ticks,
            "median_block_ev_denominator": self.median_block_ev_denominator,
            "median_block_ev_numerator": self.median_block_ev_numerator,
            "positive_block_count": self.positive_block_count,
            "rejection_reasons": list(self.rejection_reasons),
            "schema": SEARCH_GATE_SCHEMA,
            "scope_key": self.scope_key,
            "strategy_id": self.strategy_id,
            "stress_net_ticks": self.stress_net_ticks,
            "worst_block_ev_denominator": self.worst_block_ev_denominator,
            "worst_block_ev_numerator": self.worst_block_ev_numerator,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "artifact_sha256": self.artifact_sha256}


def score_symbolic_meta_prefix(
    scope_key: str,
    evaluation: CompleteStrategyEvaluation,
) -> SymbolicMetaPrefixScore:
    """Score only B1..B(k-1) for an outer fold, or B1..B8 for final Search."""

    included = META_PREFIX_BLOCKS.get(scope_key)
    if included is None:
        raise SymbolicEngineError("symbolic meta prefix scope is invalid")
    by_key = {item.group_key: item for item in evaluation.outer_validations}
    if any(key not in by_key for key in included):
        raise SymbolicEngineError("evaluation lacks a required prefix block aggregate")
    blocks = tuple(by_key[key] for key in included)
    reasons: list[str] = []
    if any(item.raw_signal_count < 6 for item in blocks):
        reasons.append("PREFIX_BLOCK_RAW_SIGNALS_LT_6")
    if any(item.fill_count < 5 for item in blocks):
        reasons.append("PREFIX_BLOCK_COMPLETE_FILLS_LT_5")
    if any(item.censored_count != 0 for item in blocks):
        reasons.append("PREFIX_BLOCK_CENSORED_COUNT_NONZERO")
    evs = sorted(
        Fraction(item.net_ticks, item.fill_count) if item.fill_count else Fraction(-(10**18))
        for item in blocks
    )
    worst = evs[0]
    median = evs[len(evs) // 2]
    total = 0
    peak = 0
    maximum_drawdown = 0
    for item in blocks:
        maximum_drawdown = max(
            maximum_drawdown,
            item.maximum_drawdown_ticks,
            peak - (total + item.minimum_prefix_equity_ticks),
        )
        peak = max(peak, total + item.maximum_prefix_equity_ticks)
        total += item.net_ticks
    definition = {
        "eligible": not reasons,
        "included_block_keys": list(included),
        "maximum_drawdown_ticks": maximum_drawdown,
        "median_block_ev_denominator": median.denominator,
        "median_block_ev_numerator": median.numerator,
        "positive_block_count": sum(item.net_ticks > 0 for item in blocks),
        "rejection_reasons": reasons,
        "schema": SEARCH_GATE_SCHEMA,
        "scope_key": scope_key,
        "strategy_id": evaluation.recipe.strategy_id,
        "stress_net_ticks": sum(item.stress_net_ticks for item in blocks),
        "worst_block_ev_denominator": worst.denominator,
        "worst_block_ev_numerator": worst.numerator,
    }
    return SymbolicMetaPrefixScore(
        scope_key,
        evaluation.recipe.strategy_id,
        included,
        not reasons,
        tuple(reasons),
        definition["positive_block_count"],
        worst.numerator,
        worst.denominator,
        median.numerator,
        median.denominator,
        definition["stress_net_ticks"],
        maximum_drawdown,
        canonical_sha256(definition),
    )


def select_symbolic_top24_for_meta(
    scope_key: str,
    evaluations: Iterable[CompleteStrategyEvaluation],
) -> SymbolicTop24Selection:
    """Select meta slots with no access to the current/future outer validation blocks."""

    scores = tuple(score_symbolic_meta_prefix(scope_key, item) for item in evaluations)
    if len({item.strategy_id for item in scores}) != len(scores):
        raise SymbolicEngineError("symbolic meta prefix contains duplicate strategies")
    eligible = [item for item in scores if item.eligible]
    eligible.sort(
        key=lambda item: (
            -item.positive_block_count,
            -item.worst_block_ev,
            -item.stress_net_ticks,
            -item.median_block_ev,
            item.maximum_drawdown_ticks,
            item.strategy_id,
        )
    )
    selected = tuple(item.strategy_id for item in eligible[:24])
    definition = {
        "eligible_count": len(eligible),
        "schema": SEARCH_GATE_SCHEMA,
        "scope_key": scope_key,
        "selected_strategy_ids": list(selected),
    }
    return SymbolicTop24Selection(
        scope_key,
        selected,
        len(eligible),
        canonical_sha256(definition),
    )


def select_complete_search_symbolic(
    evaluations: Iterable[CompleteStrategyEvaluation],
    gate_results: Iterable[CompleteSearchGateResult],
    family_by_anchor_policy_id: Mapping[str, Family],
) -> CompleteSearchSelection:
    """Select at most six symbolic candidates, with at most two per event family."""

    evaluation_by_id = {item.recipe.strategy_id: item for item in evaluations}
    gates = tuple(gate_results)
    if len({item.strategy_id for item in gates}) != len(gates):
        raise SymbolicEngineError("complete Search gates contain duplicate strategies")
    eligible = [item for item in gates if item.eligible]
    if any(item.strategy_id not in evaluation_by_id for item in eligible):
        raise SymbolicEngineError("eligible Search gate lacks its real evaluation")
    eligible.sort(
        key=lambda item: (
            -item.positive_outer_validation_count,
            -item.worst_outer_ev,
            -evaluation_by_id[item.strategy_id].total_stress_net_ticks,
            -item.median_outer_ev,
            evaluation_by_id[item.strategy_id].maximum_drawdown_ticks,
            item.strategy_id,
        )
    )
    selected: list[str] = []
    family_counts: dict[Family, int] = defaultdict(int)
    for gate in eligible:
        evaluation = evaluation_by_id[gate.strategy_id]
        family = family_by_anchor_policy_id.get(evaluation.recipe.anchor_policy_id)
        if family is None:
            raise SymbolicEngineError("complete strategy lacks its base event family")
        if family_counts[family] >= 2:
            continue
        selected.append(gate.strategy_id)
        family_counts[family] += 1
        if len(selected) == 6:
            break
    classification = "SYMBOLIC_SEARCH_SELECTED" if selected else "NO_SYMBOLIC_SEARCH_FINALISTS"
    definition = {
        "classification": classification,
        "eligible_count": len(eligible),
        "schema": SEARCH_GATE_SCHEMA,
        "selected_strategy_ids": selected,
    }
    return CompleteSearchSelection(
        classification,
        tuple(selected),
        len(eligible),
        canonical_sha256(definition),
    )


def _decode_mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise SymbolicEngineError(f"{label} must be a string-keyed mapping")
    return value


def _decode_date(value: object, *, label: str) -> date:
    if not isinstance(value, str):
        raise SymbolicEngineError(f"{label} must be an ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise SymbolicEngineError(f"{label} must be an ISO date") from error


def _decode_roundtrip(
    decoded: object,
    payload: Mapping[str, object],
    *,
    label: str,
) -> None:
    if not hasattr(decoded, "as_dict") or decoded.as_dict() != dict(payload):
        raise SymbolicEngineError(f"{label} canonical payload differs")
    for identity_key in ("artifact_sha256", "commitment_sha256"):
        if identity_key not in payload:
            continue
        identity = payload[identity_key]
        definition = {key: item for key, item in payload.items() if key != identity_key}
        if not isinstance(identity, str) or canonical_sha256(definition) != identity:
            raise SymbolicEngineError(f"{label} identity differs")


def _anchor_policy_from_dict(value: object) -> AnchorPolicy:
    row = _decode_mapping(value, label="anchor policy")
    try:
        decoded = AnchorPolicy(
            row["policy_rank"],
            row["policy_id"],
            row["base_candidate_id"],
            row["context_id"],
            row["time_filter_id"],
            row["delay_id"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("anchor policy payload is invalid") from error
    _decode_roundtrip(decoded, row, label="anchor policy")
    return decoded


def _anchor_record_from_dict(value: object) -> AnchorRecord:
    row = _decode_mapping(value, label="anchor record")
    frozen = _decode_mapping(row.get("frozen"), label="anchor frozen state")
    try:
        decoded = AnchorRecord(
            _decode_date(row["source_date"], label="anchor source_date"),
            row["contract"],
            row["outcome_span_id"],
            row["segment_id"],
            row["anchor_ns"],
            row["direction"],
            row["trigger_start_ns"],
            row["trigger_end_ns"],
            row["trigger_open_ticks"],
            row["trigger_high_ticks"],
            row["trigger_low_ticks"],
            row["trigger_close_ticks"],
            row["atr_sum_ticks"],
            row["atr_denominator"],
            tuple((key, item) for key, item in frozen.items()),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("anchor record payload is invalid") from error
    _decode_roundtrip(decoded, row, label="anchor record")
    return decoded


def policy_mask_from_dict(value: object) -> PolicyMask:
    """Strictly restore a canonical feature mask from persisted JSON data."""

    row = _decode_mapping(value, label="policy mask")
    records_value = row.get("records")
    if not isinstance(records_value, list):
        raise SymbolicEngineError("policy mask records must be a list")
    try:
        decoded = PolicyMask.from_records(
            _anchor_policy_from_dict(row["policy"]),
            row["family"],
            row["direction"],
            tuple(_anchor_record_from_dict(item) for item in records_value),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("policy mask payload is invalid") from error
    _decode_roundtrip(decoded, row, label="policy mask")
    return decoded


def structural_eligibility_lattice_from_dict(value: object) -> StructuralEligibilityLattice:
    row = _decode_mapping(value, label="structural eligibility lattice")
    keys = row.get("eligible_anchor_keys")
    if not isinstance(keys, list):
        raise SymbolicEngineError("structural eligibility keys must be a list")
    try:
        decoded = StructuralEligibilityLattice(
            tuple(tuple(item) for item in keys),
            row["maximum_path_seconds"],
            row["allowed_tail_end_ns"],
            row["artifact_sha256"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("structural eligibility payload is invalid") from error
    _decode_roundtrip(decoded, row, label="structural eligibility lattice")
    return decoded


def direct_opportunity_lattice_from_dict(value: object) -> DirectOpportunityLattice:
    """Strictly restore the feature-only direct decision/entry commitment."""

    row = _decode_mapping(value, label="direct opportunity lattice")
    opportunities = row.get("opportunities")
    excluded = row.get("excluded_anchor_keys")
    if not isinstance(opportunities, list) or not isinstance(excluded, list):
        raise SymbolicEngineError("direct opportunity lattice arrays differ")
    try:
        decoded_opportunities = tuple(
            DirectOpportunity(
                _decode_date(
                    _decode_mapping(item, label="direct opportunity")["decision_source_date"],
                    label="direct decision source date",
                ),
                _decode_mapping(item, label="direct opportunity")["contract"],
                _decode_mapping(item, label="direct opportunity")["outcome_span_id"],
                _decode_mapping(item, label="direct opportunity")["segment_id"],
                _decode_mapping(item, label="direct opportunity")["decision_ns"],
                _decode_mapping(item, label="direct opportunity")["scheduled_entry_ns"],
                _decode_mapping(item, label="direct opportunity")["scheduled_entry_ticks"],
            )
            for item in opportunities
        )
        decoded = DirectOpportunityLattice(
            row["structural_lattice_sha256"],
            row["structural_anchor_count"],
            row["structural_anchor_set_sha256"],
            decoded_opportunities,
            tuple(tuple(item) for item in excluded),
            row["artifact_sha256"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("direct opportunity lattice payload is invalid") from error
    _decode_roundtrip(decoded, row, label="direct opportunity lattice")
    return decoded


def causal_expert_feature_artifact_from_dict(value: object) -> CausalExpertFeatureArtifact:
    """Strictly restore an exact-rational causal Expert-8 artifact."""

    row = _decode_mapping(value, label="causal expert feature artifact")
    values = row.get("values")
    anchor_key = row.get("anchor_key")
    if not isinstance(values, list) or not isinstance(anchor_key, list):
        raise SymbolicEngineError("causal expert artifact arrays differ")
    try:
        decoded_values = tuple(
            CausalExpertValue(
                _decode_mapping(item, label="causal expert value")["feature_name"],
                _decode_mapping(item, label="causal expert value")["numerator"],
                _decode_mapping(item, label="causal expert value")["denominator"],
            )
            for item in values
        )
        decoded = CausalExpertFeatureArtifact(
            tuple(anchor_key),
            row["anchor_policy_id"],
            row["base_candidate_id"],
            row["context_id"],
            row["order_id"],
            row["exit_policy_id"],
            decoded_values,
            row["inputs_sha256"],
            row["values_sha256"],
            row["formula_sha256"],
            row["artifact_sha256"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("causal expert artifact payload is invalid") from error
    _decode_roundtrip(decoded, row, label="causal expert feature artifact")
    return decoded


def structurally_eligible_policy_mask_from_dict(
    value: object,
) -> StructurallyEligiblePolicyMask:
    row = _decode_mapping(value, label="structurally eligible policy mask")
    excluded = row.get("excluded_anchor_keys")
    if not isinstance(excluded, list):
        raise SymbolicEngineError("structural excluded anchors must be a list")
    try:
        decoded = StructurallyEligiblePolicyMask(
            policy_mask_from_dict(row["raw_mask"]),
            policy_mask_from_dict(row["evaluable_mask"]),
            tuple(tuple(item) for item in excluded),
            row["lattice_sha256"],
            row["commitment_sha256"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("structural policy payload is invalid") from error
    _decode_roundtrip(decoded, row, label="structurally eligible policy mask")
    return decoded


def frozen_control_masks_from_dict(value: object) -> FrozenControlMasks:
    """Strictly restore null masks with native-lag pairing/fallback evidence."""

    row = _decode_mapping(value, label="frozen control masks")
    circular_pairs = row.get("circular_pairs")
    matched_pairs = row.get("matched_pairs")
    daily = row.get("evaluable_daily_counts")
    reporting = row.get("evaluable_reporting_group_counts")
    seeds = row.get("seed_sha256s")
    if not all(
        isinstance(item, list) for item in (circular_pairs, matched_pairs, daily, reporting, seeds)
    ):
        raise SymbolicEngineError("frozen control arrays differ")
    try:
        decoded_circular_pairs = tuple(
            CircularControlPair(
                tuple(_decode_mapping(item, label="circular pair")["real_anchor_key"]),
                tuple(_decode_mapping(item, label="circular pair")["control_anchor_key"]),
                _decode_mapping(item, label="circular pair")["preserved_lag_ns"],
            )
            for item in circular_pairs
        )
        decoded_matched_pairs = tuple(
            MatchedControlPair(
                tuple(_decode_mapping(item, label="matched pair")["real_anchor_key"]),
                tuple(_decode_mapping(item, label="matched pair")["control_anchor_key"]),
                _decode_mapping(item, label="matched pair")["fallback_level"],
                _decode_mapping(item, label="matched pair")["preserved_lag_ns"],
            )
            for item in matched_pairs
        )
        decoded_daily = tuple(
            (
                _decode_date(
                    _decode_mapping(item, label="daily control count")["decision_date"],
                    label="daily control date",
                ),
                _decode_mapping(item, label="daily control count")["signal_count"],
            )
            for item in daily
        )
        decoded_reporting = tuple(
            (
                _decode_mapping(item, label="reporting control count")["group_key"],
                _decode_mapping(item, label="reporting control count")["signal_count"],
            )
            for item in reporting
        )
        decoded = FrozenControlMasks(
            row["stage_key"],
            policy_mask_from_dict(row["real"]),
            None if row["circular"] is None else policy_mask_from_dict(row["circular"]),
            None if row["matched"] is None else policy_mask_from_dict(row["matched"]),
            decoded_circular_pairs,
            decoded_matched_pairs,
            row["trigger_timeframe_seconds"],
            decoded_daily,
            decoded_reporting,
            row["sample_eligible"],
            row["ineligibility_reason"],
            row["opportunity_lattice_sha256"],
            tuple(seeds),
            row["commitment_sha256"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("frozen control payload is invalid") from error
    _decode_roundtrip(decoded, row, label="frozen control masks")
    return decoded


def _stage_a_chunk_from_dict(value: object) -> StageAChunkSpec:
    row = _decode_mapping(value, label="Stage-A chunk")
    try:
        decoded = StageAChunkSpec(
            row["chunk_index"],
            row["first_policy_rank"],
            row["last_policy_rank"],
            row["policy_count"],
            row["chunk_id"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("Stage-A chunk payload is invalid") from error
    _decode_roundtrip(decoded, row, label="Stage-A chunk")
    return decoded


def _horizon_score_from_dict(value: object) -> HorizonReferenceScore:
    row = _decode_mapping(value, label="horizon score")
    try:
        decoded = HorizonReferenceScore(
            row["horizon_seconds"],
            row["fill_count"],
            row["active_day_count"],
            row["net_ticks"],
            row["gross_profit_ticks"],
            row["gross_loss_ticks"],
            row["positive_group_count"],
            row["group_count"],
            row["worst_group_ev_numerator"],
            row["worst_group_ev_denominator"],
            row["censored_count"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("horizon score payload is invalid") from error
    _decode_roundtrip(decoded, row, label="horizon score")
    return decoded


def _stage_a_reference_score_from_dict(value: object) -> StageAReferenceScore:
    row = _decode_mapping(value, label="Stage-A reference score")
    horizons = row.get("horizons")
    reasons = row.get("rejection_reasons")
    if not isinstance(horizons, list) or not isinstance(reasons, list):
        raise SymbolicEngineError("Stage-A score arrays differ")
    try:
        decoded = StageAReferenceScore(
            row["policy_id"],
            row["policy_rank"],
            row["mask_sha256"],
            row["raw_mask_sha256"],
            row["base_candidate_id"],
            row["family"],
            row["direction"],
            row["support_count"],
            row["support_day_count"],
            row["evaluable_support_count"],
            row["evaluable_support_day_count"],
            row["robust_horizon_count"],
            row["worst_group_ev_numerator"],
            row["worst_group_ev_denominator"],
            row["median_ev_numerator"],
            row["median_ev_denominator"],
            row["eligible"],
            tuple(reasons),
            tuple(_horizon_score_from_dict(item) for item in horizons),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("Stage-A score payload is invalid") from error
    _decode_roundtrip(decoded, row, label="Stage-A reference score")
    return decoded


def stage_a_score_chunk_from_dict(value: object) -> StageAScoreChunk:
    row = _decode_mapping(value, label="Stage-A score chunk")
    scores = row.get("scores")
    if not isinstance(scores, list):
        raise SymbolicEngineError("Stage-A chunk scores must be a list")
    try:
        decoded = StageAScoreChunk(
            _stage_a_chunk_from_dict(row["chunk"]),
            tuple(_stage_a_reference_score_from_dict(item) for item in scores),
            row["unique_behavior_count"],
            row["artifact_sha256"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("Stage-A score chunk payload is invalid") from error
    _decode_roundtrip(decoded, row, label="Stage-A score chunk")
    return decoded


def stage_a_selection_from_dict(value: object) -> StageASelection:
    row = _decode_mapping(value, label="Stage-A selection")
    scores = row.get("selected_scores")
    policy_ids = row.get("selected_policy_ids")
    if not isinstance(scores, list) or not isinstance(policy_ids, list):
        raise SymbolicEngineError("Stage-A selection arrays differ")
    try:
        decoded = StageASelection(
            row["classification"],
            tuple(policy_ids),
            tuple(_stage_a_reference_score_from_dict(item) for item in scores),
            row["eligible_count"],
            row["deduplicated_policy_count"],
            row["alias_count"],
            row["alias_chain_sha256"],
            row["stage_b_pair_budget_maximum"],
            row["stage_b_pair_budget_used"],
            row["budget_rejected_policy_count"],
            row["budget_decision_sha256"],
            row["artifact_sha256"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("Stage-A selection payload is invalid") from error
    _decode_roundtrip(decoded, row, label="Stage-A selection")
    return decoded


def _complete_recipe_from_dict(value: object) -> CompleteStrategyRecipe:
    row = _decode_mapping(value, label="complete recipe")
    try:
        decoded = CompleteStrategyRecipe(
            row["strategy_rank"],
            row["strategy_id"],
            row["anchor_selection_rank"],
            row["anchor_policy_id"],
            row["entry_policy_id"],
            row["exit_policy_id"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("complete recipe payload is invalid") from error
    _decode_roundtrip(decoded, row, label="complete recipe")
    return decoded


def _group_aggregate_from_dict(value: object) -> GroupStrategyAggregate:
    row = _decode_mapping(value, label="group aggregate")
    signal_dates = row.get("active_signal_dates")
    entry_dates = row.get("active_entry_dates")
    if not isinstance(signal_dates, list) or not isinstance(entry_dates, list):
        raise SymbolicEngineError("group aggregate dates differ")
    try:
        decoded = GroupStrategyAggregate(
            row["group_key"],
            row["raw_signal_count"],
            tuple(_decode_date(item, label="active signal date") for item in signal_dates),
            row["fill_count"],
            tuple(_decode_date(item, label="active entry date") for item in entry_dates),
            row["censored_count"],
            row["net_ticks"],
            row["stress_net_ticks"],
            row["gross_profit_ticks"],
            row["gross_loss_ticks"],
            row["maximum_drawdown_ticks"],
            row["maximum_prefix_equity_ticks"],
            row["minimum_prefix_equity_ticks"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("group aggregate payload is invalid") from error
    _decode_roundtrip(decoded, row, label="group aggregate")
    return decoded


def complete_strategy_evaluation_from_dict(value: object) -> CompleteStrategyEvaluation:
    """Strictly restore a compact Stage-B aggregate for crash resume."""

    row = _decode_mapping(value, label="complete strategy evaluation")
    try:
        decoded = CompleteStrategyEvaluation(
            _complete_recipe_from_dict(row["recipe"]),
            tuple(tuple(item) for item in row["evaluated_lineages"]),
            row["evaluation_start_ns"],
            row["evaluation_end_ns"],
            row["raw_signal_count"],
            row["active_signal_days"],
            tuple(
                _decode_date(item, label="evaluation active signal date")
                for item in row["active_signal_dates"]
            ),
            row["skipped_occupied_count"],
            row["unfilled_entry_count"],
            row["cancelled_entry_count"],
            row["censored_count"],
            row["fill_count"],
            row["active_entry_days"],
            tuple(
                _decode_date(item, label="evaluation active entry date")
                for item in row["active_entry_dates"]
            ),
            row["contract_count"],
            tuple(row["contracts"]),
            row["total_reference_pnl_ticks"],
            row["total_gross_pnl_ticks"],
            row["total_net_ticks"],
            row["total_stress_net_ticks"],
            row["gross_profit_ticks"],
            row["gross_loss_ticks"],
            row["maximum_drawdown_ticks"],
            row["maximum_prefix_equity_ticks"],
            row["minimum_prefix_equity_ticks"],
            tuple(_group_aggregate_from_dict(item) for item in row["reporting_groups"]),
            tuple(_group_aggregate_from_dict(item) for item in row["outer_validations"]),
            tuple(row["evaluated_anchor_counts"]),
            tuple(row["evaluated_anchor_leaf_sha256s"]),
            tuple(row["behavior_leaf_sha256s"]),
            row["artifact_sha256"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("complete strategy evaluation payload is invalid") from error
    _decode_roundtrip(decoded, row, label="complete strategy evaluation")
    return decoded


def complete_evaluation_chunk_from_dict(value: object) -> CompleteEvaluationChunk:
    """Strictly restore a persisted Stage-B evaluation chunk for crash resume."""

    row = _decode_mapping(value, label="complete evaluation chunk")
    evaluations = row.get("evaluations")
    coverage_shapes = row.get("coverage_shapes")
    behavior_rows = row.get("behavior_leaf_rows")
    if not all(isinstance(item, list) for item in (evaluations, coverage_shapes, behavior_rows)):
        raise SymbolicEngineError("complete evaluation compact arrays differ")
    try:
        if (
            row["schema"] != COMPACT_STAGE_B_CHUNK_SCHEMA
            or row["serialization"] != "FACTORED_COVERAGE_AND_BEHAVIOR_LEAVES"
        ):
            raise SymbolicEngineError("complete evaluation compact schema differs")
        coverage_by_sha: dict[str, Mapping[str, object]] = {}
        for item in coverage_shapes:
            shape = _decode_mapping(item, label="compact coverage shape")
            shape_sha = shape["coverage_shape_sha256"]
            _require_sha(shape_sha, label="compact coverage shape SHA")
            shape_definition = {
                key: content for key, content in shape.items() if key != "coverage_shape_sha256"
            }
            if canonical_sha256(shape_definition) != shape_sha or shape_sha in coverage_by_sha:
                raise SymbolicEngineError("compact coverage shape identity differs")
            coverage_by_sha[shape_sha] = shape
        behavior_by_sha: dict[str, Mapping[str, object]] = {}
        for item in behavior_rows:
            behavior = _decode_mapping(item, label="compact behavior leaf row")
            behavior_sha = behavior["behavior_leaf_row_sha256"]
            _require_sha(behavior_sha, label="compact behavior row SHA")
            behavior_definition = {
                key: content
                for key, content in behavior.items()
                if key != "behavior_leaf_row_sha256"
            }
            if (
                canonical_sha256(behavior_definition) != behavior_sha
                or behavior_sha in behavior_by_sha
            ):
                raise SymbolicEngineError("compact behavior leaf identity differs")
            behavior_by_sha[behavior_sha] = behavior

        restored: list[CompleteStrategyEvaluation] = []
        used_coverage: set[str] = set()
        used_behavior: set[str] = set()
        for item in evaluations:
            compact = dict(_decode_mapping(item, label="compact strategy evaluation"))
            coverage_sha = compact.pop("coverage_shape_sha256")
            behavior_sha = compact.pop("behavior_leaf_row_sha256")
            coverage = coverage_by_sha[coverage_sha]
            behavior = behavior_by_sha[behavior_sha]
            recipe = _decode_mapping(compact["recipe"], label="compact recipe")
            if (
                behavior["coverage_shape_sha256"] != coverage_sha
                or behavior["strategy_id"] != recipe["strategy_id"]
                or behavior["strategy_rank"] != recipe["strategy_rank"]
            ):
                raise SymbolicEngineError("compact leaf references differ from strategy")
            compact["evaluated_lineages"] = coverage["evaluated_lineages"]
            compact["evaluated_anchor_counts"] = coverage["evaluated_anchor_counts"]
            compact["evaluated_anchor_leaf_sha256s"] = coverage["evaluated_anchor_leaf_sha256s"]
            compact["behavior_leaf_sha256s"] = behavior["behavior_leaf_sha256s"]
            restored.append(complete_strategy_evaluation_from_dict(compact))
            used_coverage.add(coverage_sha)
            used_behavior.add(behavior_sha)
        if used_coverage != set(coverage_by_sha) or used_behavior != set(behavior_by_sha):
            raise SymbolicEngineError("compact chunk contains unused or missing leaf rows")
        decoded = CompleteEvaluationChunk(
            row["first_strategy_rank"],
            row["last_strategy_rank"],
            tuple(restored),
            row["artifact_sha256"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("complete evaluation chunk payload is invalid") from error
    _decode_roundtrip(decoded, row, label="complete evaluation chunk")
    return decoded


def complete_evaluation_coverage_from_dict(value: object) -> CompleteEvaluationCoverage:
    row = _decode_mapping(value, label="complete evaluation coverage")
    try:
        decoded = CompleteEvaluationCoverage(
            row["evaluation_artifact_sha256"],
            row["expected_mask_sha256"],
            row["expected_anchor_count"],
            row["expected_anchor_set_sha256"],
            row["artifact_sha256"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("complete coverage payload is invalid") from error
    _decode_roundtrip(decoded, row, label="complete evaluation coverage")
    return decoded


def complete_search_gate_result_from_dict(value: object) -> CompleteSearchGateResult:
    row = _decode_mapping(value, label="complete Search gate")
    reasons = row.get("rejection_reasons")
    coverage = row.get("coverage_commitment_sha256s")
    if not isinstance(reasons, list) or not isinstance(coverage, list):
        raise SymbolicEngineError("complete gate arrays differ")
    try:
        decoded = CompleteSearchGateResult(
            row["strategy_id"],
            row["eligible"],
            tuple(reasons),
            row["positive_reporting_group_count"],
            row["positive_outer_validation_count"],
            row["worst_outer_ev_numerator"],
            row["worst_outer_ev_denominator"],
            row["median_outer_ev_numerator"],
            row["median_outer_ev_denominator"],
            tuple(coverage),
            row["artifact_sha256"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("complete Search gate payload is invalid") from error
    _decode_roundtrip(decoded, row, label="complete Search gate")
    return decoded


def complete_search_selection_from_dict(value: object) -> CompleteSearchSelection:
    row = _decode_mapping(value, label="complete Search selection")
    selected = row.get("selected_strategy_ids")
    if not isinstance(selected, list):
        raise SymbolicEngineError("complete selection ids must be a list")
    try:
        decoded = CompleteSearchSelection(
            row["classification"],
            tuple(selected),
            row["eligible_count"],
            row["artifact_sha256"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("complete Search selection payload is invalid") from error
    _decode_roundtrip(decoded, row, label="complete Search selection")
    return decoded


def symbolic_top24_selection_from_dict(value: object) -> SymbolicTop24Selection:
    row = _decode_mapping(value, label="symbolic top24 selection")
    selected = row.get("selected_strategy_ids")
    if not isinstance(selected, list):
        raise SymbolicEngineError("symbolic top24 ids must be a list")
    try:
        decoded = SymbolicTop24Selection(
            row["scope_key"],
            tuple(selected),
            row["eligible_count"],
            row["artifact_sha256"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SymbolicEngineError("symbolic top24 payload is invalid") from error
    _decode_roundtrip(decoded, row, label="symbolic top24 selection")
    return decoded


BASE_EVENT_CATALOG_SHA256: Final = build_base_event_catalog().catalog_sha256
ENTRY_CATALOG_SHA256: Final = build_entry_catalog().catalog_sha256
EXIT_CATALOG_SHA256: Final = build_exit_catalog().catalog_sha256
CONTEXT_CATALOG_SHA256: Final = canonical_sha256(
    [item.as_dict() for item in build_context_catalog()]
)
TIME_FILTER_CATALOG_SHA256: Final = canonical_sha256(
    [item.as_dict() for item in build_time_filter_catalog()]
)
DELAY_CATALOG_SHA256: Final = canonical_sha256([item.as_dict() for item in build_delay_catalog()])
ANCHOR_POLICY_RECIPE_SHA256: Final = canonical_sha256(
    {
        "base_event_catalog_sha256": BASE_EVENT_CATALOG_SHA256,
        "context_catalog_sha256": CONTEXT_CATALOG_SHA256,
        "delay_catalog_sha256": DELAY_CATALOG_SHA256,
        "nesting_order": ["BASE_EVENT", "CONTEXT", "TIME_FILTER", "DELAY"],
        "policy_count": LOGICAL_ANCHOR_POLICY_COUNT,
        "schema": POLICY_SCHEMA,
        "time_filter_catalog_sha256": TIME_FILTER_CATALOG_SHA256,
    }
)
STAGE_A_CHUNK_PLAN_SHA256: Final = canonical_sha256(
    [item.as_dict() for item in build_stage_a_chunk_plan()]
)
COMPLETE_STRATEGY_RECIPE_SHA256: Final = canonical_sha256(
    {
        "anchor_selection_maximum": STAGE_A_MAXIMUM_SELECTION,
        "complete_strategy_maximum": COMPLETE_STRATEGY_MAXIMUM,
        "entry_catalog_sha256": ENTRY_CATALOG_SHA256,
        "entry_count": ENTRY_POLICY_COUNT,
        "exit_catalog_sha256": EXIT_CATALOG_SHA256,
        "exit_count": EXIT_POLICY_COUNT,
        "nesting_order": ["SELECTED_ANCHOR", "ENTRY", "EXIT"],
        "schema": COMPLETE_STRATEGY_SCHEMA,
    }
)


def symbolic_engine_contract() -> dict[str, object]:
    """Return the exact non-provenance symbolic language contract."""

    return {
        "axes": {
            "anchor_policy_recipe_sha256": ANCHOR_POLICY_RECIPE_SHA256,
            "base_event_catalog_sha256": BASE_EVENT_CATALOG_SHA256,
            "base_event_count": BASE_EVENT_COUNT,
            "context_catalog_sha256": CONTEXT_CATALOG_SHA256,
            "context_count": CONTEXT_COUNT,
            "delay_catalog_sha256": DELAY_CATALOG_SHA256,
            "delay_count": DELAY_COUNT,
            "entry_catalog_sha256": ENTRY_CATALOG_SHA256,
            "entry_count": ENTRY_POLICY_COUNT,
            "exit_catalog_sha256": EXIT_CATALOG_SHA256,
            "exit_count": EXIT_POLICY_COUNT,
            "logical_anchor_policy_count": LOGICAL_ANCHOR_POLICY_COUNT,
            "complete_strategies_per_anchor": COMPLETE_STRATEGIES_PER_ANCHOR,
            "complete_strategy_maximum": COMPLETE_STRATEGY_MAXIMUM,
            "complete_strategy_recipe_sha256": COMPLETE_STRATEGY_RECIPE_SHA256,
            "reference_horizons_seconds": list(REFERENCE_HORIZONS_SECONDS),
            "reference_score_cell_count": REFERENCE_SCORE_CELL_COUNT,
            "time_filter_catalog_sha256": TIME_FILTER_CATALOG_SHA256,
            "time_filter_count": TIME_FILTER_COUNT,
        },
        "causality": {
            "completed_bars_only": True,
            "cross_date_carry": (
                "same contract/outcome span, adjacent allowlisted active dates, gap <= 96h"
            ),
            "cross_stage_warmup": "PROHIBITED",
            "intra_date_continuity": "exact timeframe adjacency and lineage",
            "stage_or_fold_start": "RESET",
        },
        "causal_expert_features": {
            "formula_contract": expert_feature_formula_contract(),
            "formula_sha256": EXPERT_FEATURE_FORMULA_SHA256,
            "realized_execution_inputs": "PROHIBITED_BY_BUILDER_SIGNATURE",
            "value_count": len(EXPERT_FEATURE_NAMES),
            "value_representation": "NORMALIZED_EXACT_RATIONAL",
        },
        "deduplication": {
            "stage_a_global_scope": "ALL_64_CHUNKS_BEFORE_OUTCOME_SELECTION",
            "stage_a_alias_evidence": "PREDECESSOR_HASH_CHAIN_IN_POLICY_RANK_ORDER",
            "behavioral_key": "canonical ordered AnchorRecord tuple",
            "complete_behavioral_key": "ordered lineage path-outcome leaf SHA-256 tuple",
            "empty_masks": "collapse to earliest semantic rank",
            "outcome_access": False,
            "representative": "raw-gate-eligible first, then lowest policy_rank",
            "raw_gate_precedence": (
                "RAW_GATE_ELIGIBLE_THEN_LOWEST_POLICY_RANK_WITH_RAW_MASK_SHA_BOUND"
            ),
        },
        "execution_boundary": {
            "direct_feature_entry_commitment": (
                "decision_ns mapped to floor(exact next same-lineage 5m first_trade_ns,1s)"
                "/open_ticks"
            ),
            "direct_missing_next_5m": "EXCLUDED_AND_COMMITTED_BEFORE_OUTCOMES",
            "preoutcome_structural_complete_case": (
                "candidate-independent exact same-lineage gap-free 7h 5m anchor lattice"
            ),
            "raw_and_evaluable_support_both_committed": True,
            "outcomes_in_signal_or_mask_api": False,
            "reference_friction_ticks": TOTAL_FRICTION_TICKS,
            "reference_scoring_is_separate": True,
            "same_execution_segment": True,
            "unexpected_censor_after_complete_case": "FATAL_CANDIDATE_INELIGIBLE",
        },
        "stage_a_chunking": {
            "chunk_count": STAGE_A_OUTER_CHUNK_COUNT,
            "chunk_plan_sha256": STAGE_A_CHUNK_PLAN_SHA256,
            "last_chunk_policy_count": build_stage_a_chunk_plan()[-1].policy_count,
            "policy_rows_per_chunk_maximum": STAGE_A_POLICY_ROWS_PER_CHUNK_MAXIMUM,
            "resume_order": "LOWEST_INCOMPLETE_CHUNK_INDEX",
            "runtime_representation": "CANDIDATE_SHARED_INTEGER_BITSET_CUBE",
            "structural_filter_timing": "BEFORE_ANY_1S_OUTCOME_LOAD",
        },
        "indicators": {
            "atr_period": ATR_PERIOD,
            "ema_rounding": "ROUND_HALF_EVEN_FIXED_POINT_1E6_SMA_SEED",
            "integer_or_rational_comparisons": True,
            "signal_timeframes_seconds": list(SUPPORTED_SIGNAL_TIMEFRAMES),
        },
        "schema": ENGINE_SCHEMA,
        "stage_a_selection": {
            "active_signal_days_minimum": 40,
            "family_direction_maximum": 8,
            "family_maximum": 16,
            "horizon_active_entry_days_minimum": 30,
            "horizon_fill_count_minimum": 48,
            "horizon_net_ticks_strictly_positive": True,
            "horizon_positive_reporting_groups_minimum": 2,
            "maximum": STAGE_A_MAXIMUM_SELECTION,
            "stage_b_pair_budget": {
                "application_order": "AFTER_GLOBAL_DEDUP_IN_FROZEN_MINIMAX_RANK_ORDER",
                "budget_decision_sha256_required": True,
                "maximum_real_circular_matched_anchor_entry_pairs": (STAGE_B_PAIR_BUDGET_MAXIMUM),
                "overflow": "REJECT_CANDIDATE_AND_CONTINUE_FROZEN_ORDER",
                "pair_cost_formula": "EVALUABLE_SUPPORT_COUNT*9_ENTRY_POLICIES*3_WORLDS",
                "world_count": STAGE_B_CONTROL_WORLD_COUNT,
            },
            "minimum_positive_horizons": 3,
            "ordering": [
                "ROBUST_HORIZON_COUNT_DESC",
                "WORST_GROUP_EV_DESC",
                "MEDIAN_HORIZON_EV_DESC",
                "SUPPORT_DESC",
                "POLICY_RANK_ASC",
            ],
            "raw_signals_each_reporting_group_minimum": 6,
            "raw_signals_minimum": 60,
            "gate_support_basis": "RAW_PRE_STRUCTURAL_FILTER",
            "outcome_and_behavior_basis": "STRUCTURALLY_EVALUABLE_MASK",
            "unexpected_censored_count_required": 0,
        },
        "stage_b_lattice": {
            "entry": {
                "limit_atr_retrace_fractions": ["1/4", "1/2"],
                "market_variant_count": 1,
                "stop_signal_extreme_buffer_ticks": [1, 4],
                "time_in_force_seconds": [1_800, 3_600],
                "variant_count": ENTRY_POLICY_COUNT,
            },
            "exit": {
                "bracket": {
                    "cap_seconds": [3_600, 10_800, 21_600],
                    "stop_loss_atr_fractions": ["1/2", "1", "3/2", "2"],
                    "take_profit_atr_fractions": ["1/2", "1", "3/2", "2", "3"],
                    "variant_count": 60,
                },
                "break_even": {
                    "activation_atr_fractions": ["1/2", "1"],
                    "cap_seconds": [10_800, 21_600],
                    "initial_stop_atr_fractions": ["1/2", "1"],
                    "variant_count": 8,
                },
                "rule": {
                    "cap_seconds": [10_800, 21_600],
                    "rules": ["OPPOSITE_TRIGGER", "CONTEXT_INVALID"],
                    "variant_count": 4,
                },
                "terminal_horizons_seconds": list(REFERENCE_HORIZONS_SECONDS),
                "trailing": {
                    "activation_atr_fractions": ["1/2", "1"],
                    "cap_seconds": [10_800, 21_600],
                    "trail_atr_fractions": ["1/2", "1"],
                    "variant_count": 8,
                },
                "variant_count": EXIT_POLICY_COUNT,
            },
        },
        "stage_b_chunking": {
            "chunk_count": STAGE_B_OUTER_CHUNK_COUNT,
            "empty_selection_chunk_bounds": {"first": 1, "last": 0, "count": 0},
            "evaluation_microbatch_size": 64,
            "recipe_rows_per_chunk_maximum": STAGE_B_RECIPE_ROWS_PER_CHUNK_MAXIMUM,
            "search_block_aggregates": [f"B{index}" for index in range(1, 9)],
            "serialization": (
                "FACTORED_COVERAGE_SHAPES_ONCE_PER_CHUNK;PER_STRATEGY_BEHAVIOR_LEAF_"
                "ROWS_HASH_REFERENCED;STRICT_FULL_EVALUATION_REPLAY"
            ),
            "streaming_reset_safety": (
                "MERGE_REQUIRES_NONOVERLAPPING_PATH_INTERVALS;EVERY_FROZEN_ANCHOR_HAS_"
                "SAME_LINEAGE_7H_COVERAGE_SO_OCCUPANCY_END_LTE_CHUNK_END"
            ),
        },
        "feature_only_controls": {
            "circular": (
                "deterministic rotation within date/contract/span/segment; saturated/singleton "
                "orbits copy identity rows with zero paired contrast; aggregate mask must differ"
            ),
            "circular_pair_evidence": "REAL_KEY_CONTROL_KEY_PRESERVED_LAG_NS_COMMITTED",
            "full_structural_path_seconds": 25_200,
            "master_seed": MASTER_NULL_SEED,
            "matched_fallback_levels": [
                "L0 same group exact volatility/regime/ATR/stop-drift geometry exact 4h bucket",
                "L1 same group exact volatility/regime/ATR/stop-drift geometry adjacent cyclic 4h bucket",
                "L2 same group exact volatility/regime/ATR/stop-drift geometry any bucket",
                "L3 same group exact volatility/regime exact 4h bucket",
                "L4 same group exact volatility/regime same-or-adjacent cyclic 4h bucket",
                "L5 same group exact volatility/regime any bucket",
                "L6 same date/contract/span/segment any causal stratum",
            ],
            "fallback_tier_counts": "DERIVED_EXACTLY_FROM_COMMITTED_MATCHED_PAIR_ROWS",
            "matched_sampling": "WITHOUT_REPLACEMENT_DETERMINISTIC_HASH_TIEBREAK",
            "matched_solver": (
                "DETERMINISTIC_COMPLETE_MAXIMUM_BIPARTITE_MATCHING;INELIGIBLE_ONLY_IF_"
                "MAXIMUM_CARDINALITY_LT_REAL_SUPPORT"
            ),
            "missing_or_nonidentity_impossible": "SAMPLE_INELIGIBLE",
            "per_group_cardinality": "EXACT",
            "native_trigger_geometry": "EXACT_CANDIDATE_TF_OHLC_AND_ATR20",
            "preserved_real_lag": "CONTROL_TRIGGER_END=CONTROL_ANCHOR_NS-REAL_LAG_NS",
            "rule_exit_schedules": "SEPARATE_REAL_CIRCULAR_MATCHED_FEATURE_ONLY_COMMITMENTS",
            "signal_count_basis": "STRUCTURALLY_EVALUABLE_REAL_MASK_NOT_RAW_PRE_FILTER_MASK",
        },
        "stage_b_execution": {
            "atr_distance_rounding": "ROUND_HALF_EVEN_MINIMUM_ONE_TICK",
            "break_even_same_second": (
                "INITIAL_STOP_BEFORE_ACTIVATION;ACTIVATION_ROW_EXACT_BREAK_EVEN;"
                "LATER_ROW_STOP_REFERENCE_APPLIES_ADVERSE_OPEN_GAP"
            ),
            "censored_or_unfilled_occupies": False,
            "entry_adverse_ticks": ENTRY_ADVERSE_TICKS,
            "entry_interval": "[ANCHOR_NS,TIF_EXPIRY_NS)",
            "exit_adverse_ticks": EXIT_ADVERSE_TICKS,
            "friction_algebra": (
                "direction*(exit_reference-entry_reference)-2_entry-2_exit-5_variable-5_fixed"
            ),
            "maximum_concurrent_positions_per_strategy": 1,
            "limit_fill": (
                "LONG touch low<=limit reference=min(limit,open); "
                "SHORT touch high>=limit reference=max(limit,open)"
            ),
            "market_fill": "first observed 1s open in [anchor,anchor+300s)",
            "maximum_required_path_seconds": 25_200,
            "no_shortened_horizon": True,
            "path_coverage": "LOADER_VERIFIED_FULL_INTERVAL_OR_CENSORED",
            "path_gap_handling": "LOCAL_CONTIGUOUS_INTERVAL_NO_BRIDGE",
            "rule_exit": "first observed 1s open at-or-after completed rule bar before cap",
            "same_entry_second": "ADVERSE_STOP_ELIGIBLE_FAVORABLE_EXIT_INELIGIBLE",
            "same_second_tp_sl": "STOP_FIRST",
            "standard_friction_ticks": TOTAL_FRICTION_TICKS,
            "stop_fill": (
                "LONG touch high>=stop reference=max(stop,open); "
                "SHORT touch low<=stop reference=min(stop,open)"
            ),
            "stress_friction_ticks": 18,
            "take_profit_fill": "EXACT_TARGET_NO_FAVORABLE_GAP_CREDIT",
            "terminal_trade_staleness_seconds_strictly_less_than": 300,
            "trailing_same_second": "PRIOR_STOP_FIRST_THEN_FAVORABLE_UPDATE_THEN_UPDATED_STOP",
            "variable_cost_ticks": VARIABLE_COST_TICKS,
            "allocated_fixed_cost_ticks": ALLOCATED_FIXED_COST_TICKS,
        },
        "stage_b_search_gates": {
            "active_entry_days_minimum": 30,
            "active_signal_days_minimum": 40,
            "complete_fills_each_outer_validation_minimum": 5,
            "complete_fills_minimum": 48,
            "censored_count_required": 0,
            "minimum_positive_outer_validations": 4,
            "minimum_positive_reporting_groups": 3,
            "net_strictly_above_both_controls": True,
            "net_ticks_strictly_positive": True,
            "profit_factor_minimum": "21/20",
            "raw_signals_each_outer_validation_minimum": 6,
            "raw_signals_each_reporting_group_minimum": 6,
            "raw_signals_minimum": 60,
            "stress_18_tick_net_strictly_positive": True,
            "oof_block_keys": list(SEARCH_OOF_BLOCK_KEYS),
            "full_evaluable_mask_coverage_commitment_required_for_each_world": True,
        },
        "stage_b_vector_kernel": {
            "budget_projection_100k": stage_b_kernel_budget_projection(),
            "budget_pair_scope": "AGGREGATE_FILLED_ANCHOR_ENTRY_PAIRS_ACROSS_ALL_WORLDS",
            "bounded_cache_release": "AFTER_EACH_MASK_ENTRY_85_RECIPE_BATCH",
            "fixed_exit_variants_shared_per_anchor_order": 81,
            "public_batch_api": "SharedPathEvaluator.fixed_exit_outcome_batch",
            "python_full_path_loop_per_exit": False,
            "path_tensor": "NUMPY_INT64_START_OPEN_FAVORABLE_ADVERSE_PER_LINEAGE_DIRECTION",
            "rule_exit_variants_logarithmic_query": 4,
        },
        "symbolic_meta_top24": {
            "final_symbolic_quota_applies": False,
            "rank_slot_count": 24,
            "scope_keys": ["B3", "B4", "B5", "B6", "B7", "B8", "SEARCH_FINAL"],
            "training_prefix_only": True,
        },
    }


__all__ = [
    "ANCHOR_POLICY_RECIPE_SHA256",
    "BASE_EVENT_CATALOG_SHA256",
    "BASE_EVENT_COUNT",
    "COMPLETE_STRATEGIES_PER_ANCHOR",
    "COMPLETE_STRATEGY_MAXIMUM",
    "COMPLETE_STRATEGY_RECIPE_SHA256",
    "CONTEXT_CATALOG_SHA256",
    "CONTEXT_COUNT",
    "DELAY_CATALOG_SHA256",
    "DELAY_COUNT",
    "ENTRY_CATALOG_SHA256",
    "ENTRY_POLICY_COUNT",
    "EXIT_CATALOG_SHA256",
    "EXIT_POLICY_COUNT",
    "EXPERT_FEATURE_FORMULA_SHA256",
    "EXPERT_FEATURE_NAMES",
    "LOGICAL_ANCHOR_POLICY_COUNT",
    "MASTER_NULL_SEED",
    "REFERENCE_HORIZONS_SECONDS",
    "REFERENCE_SCORE_CELL_COUNT",
    "SEARCH_OOF_BLOCK_KEYS",
    "STAGE_A_CHUNK_PLAN_SHA256",
    "STAGE_A_MAXIMUM_SELECTION",
    "STAGE_A_OUTER_CHUNK_COUNT",
    "STAGE_A_POLICY_ROWS_PER_CHUNK_MAXIMUM",
    "STAGE_B_CONTROL_WORLD_COUNT",
    "STAGE_B_OUTER_CHUNK_COUNT",
    "STAGE_B_PAIR_BUDGET_MAXIMUM",
    "STAGE_B_RECIPE_ROWS_PER_CHUNK_MAXIMUM",
    "TIME_FILTER_CATALOG_SHA256",
    "TIME_FILTER_COUNT",
    "TOTAL_FRICTION_TICKS",
    "AnchorMaskBatch",
    "AnchorPolicy",
    "AnchorRecord",
    "BaseEventCandidate",
    "BaseEventCatalog",
    "CandidateCubeCommitment",
    "CandidatePolicyCube",
    "CausalExpertFeatureArtifact",
    "CausalExpertValue",
    "CircularControlPair",
    "CompleteEvaluationChunk",
    "CompleteEvaluationCoverage",
    "CompleteEvaluationDeduplication",
    "CompleteSearchGateResult",
    "CompleteSearchSelection",
    "CompleteStrategyEvaluation",
    "CompleteStrategyRecipe",
    "ContextSpec",
    "ControlOpportunity",
    "ControlOpportunityLattice",
    "ControlTriggerState",
    "CubePolicyMask",
    "DelaySpec",
    "DirectOpportunity",
    "DirectOpportunityLattice",
    "EncodedSparseMask",
    "EntryAttempt",
    "EntryCatalog",
    "EntryOrderBatch",
    "EntryPolicy",
    "EventOccurrence",
    "ExitCatalog",
    "ExitOutcome",
    "ExitPolicy",
    "FeaturePolicyCommitment",
    "FeatureUniverseCommitmentChunk",
    "FrozenControlMasks",
    "FrozenControlRuleExitSchedules",
    "FrozenEntryOrder",
    "GroupStrategyAggregate",
    "HorizonReferenceScore",
    "MaskAlias",
    "MaskDeduplication",
    "MatchedControlPair",
    "OneSecondPath",
    "PolicyMask",
    "ReferenceOutcomeSurface",
    "RuleExitSchedule",
    "RuleExitTimes",
    "SelectedStrategyDetailRequest",
    "SelectedStrategyDetailedOutcome",
    "SharedPathEvaluator",
    "StageAChunkSpec",
    "StageAReferenceScore",
    "StageAScoreChunk",
    "StageASelection",
    "StageBChunkSpec",
    "StrategyTradeOutcomeRow",
    "StructuralEligibilityLattice",
    "StructurallyEligiblePolicyMask",
    "SymbolicEngineError",
    "SymbolicMetaPrefixScore",
    "SymbolicStage",
    "SymbolicTop24Selection",
    "TimeFilterSpec",
    "apply_complete_search_gates",
    "build_base_event_catalog",
    "build_candidate_policy_cube",
    "build_causal_expert_feature_artifact",
    "build_context_catalog",
    "build_control_opportunity_lattice",
    "build_control_rule_exit_schedules",
    "build_delay_catalog",
    "build_direct_opportunity_lattice",
    "build_entry_catalog",
    "build_exit_catalog",
    "build_reference_outcome_surfaces",
    "build_rule_exit_schedule",
    "build_stage_a_chunk_plan",
    "build_stage_b_chunk_plan",
    "build_structural_eligibility_lattice",
    "build_symbolic_stage",
    "build_time_filter_catalog",
    "causal_expert_feature_artifact_from_dict",
    "complete_evaluation_chunk_from_dict",
    "complete_evaluation_coverage_from_dict",
    "complete_search_gate_result_from_dict",
    "complete_search_selection_from_dict",
    "complete_strategy_evaluation_from_dict",
    "deduplicate_complete_evaluations",
    "deduplicate_feature_masks",
    "direct_opportunity_lattice_from_dict",
    "evaluate_selected_strategy_details",
    "expert_feature_formula_contract",
    "freeze_entry_orders",
    "freeze_feature_control_masks",
    "freeze_structurally_eligible_policy_mask",
    "frozen_control_masks_from_dict",
    "iter_anchor_mask_batches",
    "iter_anchor_policies",
    "iter_complete_strategy_evaluation_chunks",
    "iter_complete_strategy_recipes",
    "iter_feature_universe_commitment_chunks",
    "iter_stage_a_cube_score_chunks",
    "merge_complete_strategy_evaluations",
    "merge_reference_outcome_surfaces",
    "policy_mask_from_dict",
    "score_stage_a_cube_chunk",
    "score_stage_a_reference_horizons",
    "score_symbolic_meta_prefix",
    "select_complete_search_symbolic",
    "select_stage_a_top256",
    "select_symbolic_top24_for_meta",
    "stage_a_score_chunk_from_dict",
    "stage_a_selection_from_dict",
    "stage_b_kernel_budget_projection",
    "structural_eligibility_lattice_from_dict",
    "structurally_eligible_policy_mask_from_dict",
    "symbolic_engine_contract",
    "symbolic_top24_selection_from_dict",
    "verify_complete_evaluation_coverage",
]
