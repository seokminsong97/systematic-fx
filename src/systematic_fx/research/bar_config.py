"""Frozen configuration and bounded candidate catalog for bar-pattern research.

The v1 catalog is deliberately enumerated before any outcome is inspected:
three signal timeframes, six setup lengths, six fixed OHLC families, and two
directions produce exactly 216 strategy variants.  The remaining 24 entries in
the campaign budget are intentionally unused and cannot be filled after seeing
results.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from systematic_fx.backtest.barriers import BARRIER_TICKS, Direction
from systematic_fx.research.hypotheses import (
    HypothesisConfigError,
    canonical_sha256,
    load_toml_document,
)

BAR_PATTERN_CONFIG_RELATIVE_PATH: Final = Path("configs/research/bar_pattern_discovery_v1.toml")
BAR_PATTERN_CONFIG_SEMANTIC_SHA256: Final = (
    "34b84587e12af32f84bdcc3e66552c763feccbc55043d8514e188fb8895c7283"
)
BAR_PATTERN_CONFIG_ID: Final = "bar_pattern_discovery_v1"
BAR_PATTERN_CAMPAIGN_KEY: Final = "bar_pattern_discovery_v1"
BAR_PATTERN_SCHEMA_VERSION: Final = 1
BAR_PATTERN_SCREENING_ONLY: Final = True
BAR_PATTERN_QUALIFICATION_STATUS: Final = (
    "BLOCKED_MISSING_POINT_IN_TIME_DEFINITION_AND_TRADING_STATUS"
)
BAR_SOURCE_MANIFEST_SHA256: Final = (
    "14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de"
)
BAR_SOURCE_FILE_COUNT: Final = 1_434

SIGNAL_TIMEFRAMES_SECONDS: Final = (300, 1_800, 3_600)
SETUP_LOOKBACK_BARS: Final = (1, 2, 3, 4, 6, 12)
ATR_LOOKBACK_BARS: Final = 20
PATTERN_DIRECTIONS: Final = (Direction.LONG, Direction.SHORT)
CAMPAIGN_VARIANT_BUDGET: Final = 240
ALLOCATED_VARIANT_COUNT: Final = 216
UNALLOCATED_VARIANT_COUNT: Final = 24


class BarPatternConfigError(ValueError):
    """The frozen v1 bar-research definition is missing or has drifted."""


@dataclass(frozen=True, slots=True)
class PatternFamilySpec:
    """One fixed, mirrored OHLC pattern family and all of its gates."""

    family_id: str
    family_key: str
    title: str
    economic_rationale: str
    gates: tuple[tuple[str, str], ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "economic_rationale": self.economic_rationale,
            "family_id": self.family_id,
            "family_key": self.family_key,
            "gates": [
                {"expression": expression, "gate_id": gate_id} for gate_id, expression in self.gates
            ],
            "title": self.title,
        }


PATTERN_FAMILY_SPECS: Final = (
    PatternFamilySpec(
        family_id="F1",
        family_key="ordered_continuation",
        title="Ordered continuation",
        economic_rationale=(
            "An efficient setup move followed by a directional trigger may continue."
        ),
        gates=(
            ("R_GE_3_4", "R >= 3/4"),
            ("E_GE_7_20", "E >= 7/20"),
            ("X_GE_1_2", "X >= 1/2"),
            ("B_GE_1_2", "B >= 1/2"),
            ("Q_GE_3_4", "Q >= 3/4"),
        ),
    ),
    PatternFamilySpec(
        family_id="F2",
        family_key="pullback_resumption",
        title="Pullback resumption",
        economic_rationale=("A directional setup that rejects a trigger-bar pullback may resume."),
        gates=(
            ("R_GE_1_2", "R >= 1/2"),
            ("E_GE_1_4", "E >= 1/4"),
            ("P_GE_1_4", "P = (dC[t-1] - dL[t]) / A >= 1/4"),
            ("CLOSE_RECLAIMS_SETUP", "dC[t] >= dC[t-1]"),
            ("X_GE_1_2", "X >= 1/2"),
            ("B_GE_0", "B >= 0"),
            ("Q_GE_2_3", "Q >= 2/3"),
            ("K_GE_1_4", "K >= 1/4"),
        ),
    ),
    PatternFamilySpec(
        family_id="F3",
        family_key="exhaustion_rejection_reversal",
        title="Exhaustion-rejection reversal",
        economic_rationale=(
            "A large opposite setup followed by a strong rejection trigger may reverse."
        ),
        gates=(
            ("R_LE_NEG_1", "R <= -1"),
            ("E_LE_NEG_7_20", "E <= -7/20"),
            ("X_GE_3_4", "X >= 3/4"),
            ("B_GE_0", "B >= 0"),
            ("Q_GE_2_3", "Q >= 2/3"),
            ("K_GE_2_5", "K >= 2/5"),
        ),
    ),
    PatternFamilySpec(
        family_id="F4",
        family_key="body_engulfing_reversal",
        title="Body-engulfing reversal",
        economic_rationale=(
            "A trigger body that engulfs the prior opposite body may mark reversal."
        ),
        gates=(
            ("R_LE_NEG_1_2", "R <= -1/2"),
            ("PRIOR_BAR_OPPOSITE", "dC[t-1] < dO[t-1]"),
            ("TRIGGER_OPENS_BELOW_PRIOR_CLOSE", "dO[t] <= dC[t-1]"),
            ("TRIGGER_CLOSES_ABOVE_PRIOR_OPEN", "dC[t] >= dO[t-1]"),
            (
                "BODY_GE_3_4_PRIOR_BODY",
                "dC[t] - dO[t] >= 3/4 * abs(dC[t-1] - dO[t-1])",
            ),
            ("X_GE_1_2", "X >= 1/2"),
            ("Q_GE_2_3", "Q >= 2/3"),
        ),
    ),
    PatternFamilySpec(
        family_id="F5",
        family_key="compressed_range_breakout",
        title="Compressed-range breakout",
        economic_rationale=(
            "A low-width, low-median-range setup followed by a close outside its high "
            "may break out."
        ),
        gates=(
            ("W_LE_3_2", "W <= 3/2"),
            ("V_LE_3_4", "V <= 3/4"),
            ("J_GE_1_10", "J = (dC[t] - U) / A >= 1/10"),
            ("X_GE_3_4", "X >= 3/4"),
            ("B_GE_1_2", "B >= 1/2"),
            ("Q_GE_3_4", "Q >= 3/4"),
        ),
    ),
    PatternFamilySpec(
        family_id="F6",
        family_key="failed_breakout_reversal",
        title="Failed-breakout reversal",
        economic_rationale=(
            "A trigger that pierces the setup low and closes back inside may reverse."
        ),
        gates=(
            ("W_GE_1_2", "W >= 1/2"),
            ("N_GE_1_10", "N = (D - dL[t]) / A >= 1/10"),
            ("Z_GE_1_10", "Z = (dC[t] - D) / A >= 1/10"),
            ("CLOSE_NOT_ABOVE_SETUP_HIGH", "dC[t] <= U"),
            ("X_GE_3_4", "X >= 3/4"),
            ("B_GE_0", "B >= 0"),
            ("Q_GE_2_3", "Q >= 2/3"),
            ("K_GE_1_4", "K >= 1/4"),
        ),
    ),
)
PATTERN_FAMILY_BY_ID: Final = {item.family_id: item for item in PATTERN_FAMILY_SPECS}


@dataclass(frozen=True, slots=True)
class BarPatternCandidate:
    """One of the 216 preregistered, direction-specific strategy variants."""

    candidate_key: str
    timeframe_seconds: int
    setup_lookback_bars: int
    family: PatternFamilySpec
    direction: Direction

    def __post_init__(self) -> None:
        if self.timeframe_seconds not in SIGNAL_TIMEFRAMES_SECONDS:
            raise BarPatternConfigError("candidate timeframe is outside the frozen catalog")
        if self.setup_lookback_bars not in SETUP_LOOKBACK_BARS:
            raise BarPatternConfigError("candidate setup length is outside the frozen catalog")
        if PATTERN_FAMILY_BY_ID.get(self.family.family_id) != self.family:
            raise BarPatternConfigError("candidate family differs from the frozen definition")
        if not isinstance(self.direction, Direction):
            raise BarPatternConfigError("candidate direction must be LONG or SHORT")
        expected_key = (
            f"bpv1_tf{self.timeframe_seconds:04d}_lb{self.setup_lookback_bars:02d}_"
            f"{self.family.family_id.lower()}_{self.direction.value.lower()}"
        )
        if self.candidate_key != expected_key:
            raise BarPatternConfigError("candidate key differs from its canonical dimensions")

    def definition_payload(self) -> dict[str, object]:
        return {
            "atr": {
                "lookback_bars": ATR_LOOKBACK_BARS,
                "window_end": "t-L-1",
                "true_range": "max(H-L,abs(H-C_previous),abs(L-C_previous))",
            },
            "candidate_key": self.candidate_key,
            "direction": self.direction.value,
            "direction_transform": {
                "dC": "s*C",
                "dH": "max(s*H,s*L)",
                "dL": "min(s*H,s*L)",
                "dO": "s*O",
                "s": "+1_LONG_-1_SHORT",
            },
            "entry_index": "t+1",
            "family": self.family.as_dict(),
            "fitted_thresholds_allowed": False,
            "setup_indices": "t-L..t-1",
            "setup_lookback_bars": self.setup_lookback_bars,
            "time_filters_allowed": False,
            "timeframe_seconds": self.timeframe_seconds,
            "trigger_index": "t",
        }

    @property
    def definition_sha256(self) -> str:
        return canonical_sha256(self.definition_payload())


def _candidate_key(
    timeframe_seconds: int,
    setup_lookback_bars: int,
    family_id: str,
    direction: Direction,
) -> str:
    return (
        f"bpv1_tf{timeframe_seconds:04d}_lb{setup_lookback_bars:02d}_"
        f"{family_id.lower()}_{direction.value.lower()}"
    )


def frozen_bar_pattern_candidates() -> tuple[BarPatternCandidate, ...]:
    """Return the complete candidate catalog in its canonical stable order."""

    candidates = tuple(
        BarPatternCandidate(
            candidate_key=_candidate_key(timeframe, lookback, family.family_id, direction),
            timeframe_seconds=timeframe,
            setup_lookback_bars=lookback,
            family=family,
            direction=direction,
        )
        for timeframe in SIGNAL_TIMEFRAMES_SECONDS
        for lookback in SETUP_LOOKBACK_BARS
        for family in PATTERN_FAMILY_SPECS
        for direction in PATTERN_DIRECTIONS
    )
    if len(candidates) != ALLOCATED_VARIANT_COUNT:
        raise AssertionError("frozen bar-pattern candidate count drift")
    if len({candidate.candidate_key for candidate in candidates}) != len(candidates):
        raise AssertionError("frozen bar-pattern candidate key collision")
    return candidates


@dataclass(frozen=True, slots=True)
class BarExecutionScenarioSpec:
    scenario_id: str
    entry_adverse_ticks: int
    take_profit_trade_through_ticks: int
    stop_total_minimum_adverse_ticks: int
    terminal_exit_adverse_ticks: int
    variable_debit_ticks: int
    fixed_pool_multiplier_numerator: int
    fixed_pool_multiplier_denominator: int

    def as_dict(self) -> dict[str, object]:
        return {
            "entry_adverse_ticks": self.entry_adverse_ticks,
            "fixed_pool_multiplier_denominator": self.fixed_pool_multiplier_denominator,
            "fixed_pool_multiplier_numerator": self.fixed_pool_multiplier_numerator,
            "scenario_id": self.scenario_id,
            "stop_total_minimum_adverse_ticks": self.stop_total_minimum_adverse_ticks,
            "take_profit_trade_through_ticks": self.take_profit_trade_through_ticks,
            "terminal_exit_adverse_ticks": self.terminal_exit_adverse_ticks,
            "variable_debit_ticks": self.variable_debit_ticks,
        }


@dataclass(frozen=True, slots=True)
class BarPatternResearchConfig:
    """All frozen, non-secret inputs needed to identify the v1 campaign."""

    path: Path
    sha256: str
    semantic_sha256: str
    candidates: tuple[BarPatternCandidate, ...]
    split_policy: tuple[tuple[str, int | str], ...]
    discovery_support_gates: tuple[tuple[int, tuple[tuple[str, int], ...]], ...]
    discovery_economic_gates: tuple[tuple[str, int | str | bool], ...]
    execution_scenarios: tuple[BarExecutionScenarioSpec, ...]
    walk_forward_gates: tuple[tuple[str, int], ...]
    holdout_gates: tuple[tuple[str, int | bool], ...]

    @property
    def candidate_catalog_sha256(self) -> str:
        return canonical_sha256([candidate.definition_payload() for candidate in self.candidates])

    @property
    def definition_sha256(self) -> str:
        return canonical_sha256(self.canonical_parameters())

    def candidate(self, candidate_key: str) -> BarPatternCandidate:
        for candidate in self.candidates:
            if candidate.candidate_key == candidate_key:
                return candidate
        raise KeyError(f"unknown bar-pattern candidate: {candidate_key}")

    def canonical_parameters(self) -> dict[str, object]:
        """Record every frozen variable used by discovery or validation."""

        return {
            "barriers": {
                "axis_ticks": list(BARRIER_TICKS),
                "expected_cell_count": len(BARRIER_TICKS) ** 2,
            },
            "bars": {
                "alignment_policy": "HALF_OPEN_UTC_EPOCH",
                "atr_lookback_bars": ATR_LOOKBACK_BARS,
                "price_source": "SELECTED_CONTRACT_TRADE_OHLC",
                "require_contiguous_signal_bars": True,
                "reset_at_contract_change": True,
                "reset_at_quality_gap": True,
                "setup_lookback_bars": list(SETUP_LOOKBACK_BARS),
                "signal_timeframes_seconds": list(SIGNAL_TIMEFRAMES_SECONDS),
                "source_timeframe_seconds": 1,
            },
            "candidate_budget": {
                "allocated_variants": ALLOCATED_VARIANT_COUNT,
                "maximum_variants": CAMPAIGN_VARIANT_BUDGET,
                "result_driven_additions_allowed": False,
                "unallocated_variants": UNALLOCATED_VARIANT_COUNT,
            },
            "candidate_catalog_sha256": self.candidate_catalog_sha256,
            "campaign_key": BAR_PATTERN_CAMPAIGN_KEY,
            "config_id": BAR_PATTERN_CONFIG_ID,
            "config_semantic_sha256": self.semantic_sha256,
            "entry": {
                "decision_time_policy": "TRIGGER_BAR_END",
                "entry_index": "t+1",
                "entry_price_policy": "NEXT_SIGNAL_BAR_OPEN_PLUS_SCENARIO_ADVERSITY",
                "holding_limit_policy": "CONTRACT_QUALITY_OR_SPLIT_BOUNDARY",
                "normal_market_closure_policy": ("CONTINUE_WHILE_SAME_CONTRACT_AND_QUALIFIED"),
                "signal_context_gap_policy": "RESET_AFTER_3600_SECONDS",
                "terminal_boundary_types": [
                    "CONTRACT_CHANGE",
                    "QUALITY_BREAK",
                    "SPLIT_END",
                ],
                "same_second_first_touch_policy": "STOP_FIRST",
                "setup_indices": "t-L..t-1",
                "trigger_index": "t",
            },
            "execution_scenarios": [scenario.as_dict() for scenario in self.execution_scenarios],
            "families": [family.as_dict() for family in PATTERN_FAMILY_SPECS],
            "holdout_gates": dict(self.holdout_gates),
            "market": {"parent_symbol": "6E", "tick_size_raw": 50_000, "ticks_per_pip": 2},
            "patterns": {
                "directions": [direction.value for direction in PATTERN_DIRECTIONS],
                "exact_arithmetic": "INTEGER_CROSS_PRODUCTS_OR_RATIONALS",
                "family_ids": [family.family_id for family in PATTERN_FAMILY_SPECS],
                "fitted_thresholds_allowed": False,
                "time_filters_allowed": False,
            },
            "paths": {
                "bars_output_relative": "data/derived/trade_bars/version=trade_bar_v1",
                "checkpoint_output_relative": (
                    "data/derived/bar_patterns/checkpoints/bar_pattern_discovery_v1"
                ),
                "result_output_relative": ("data/derived/bar_patterns/bar_pattern_discovery_v1"),
            },
            "qualification_status": BAR_PATTERN_QUALIFICATION_STATUS,
            "schema_version": BAR_PATTERN_SCHEMA_VERSION,
            "screening_only": BAR_PATTERN_SCREENING_ONLY,
            "source": {
                "raw_root_relative": "data/mbp-10",
                "source_file_count": BAR_SOURCE_FILE_COUNT,
                "source_first_date": "2022-01-02",
                "source_last_date": "2026-07-31",
                "source_manifest_relative": ("data/derived/manifests/mbp10_source_sha256_v1.jsonl"),
                "source_manifest_sha256": BAR_SOURCE_MANIFEST_SHA256,
            },
            "split_policy": dict(self.split_policy),
            "discovery_support_gates": {
                str(timeframe): dict(gates) for timeframe, gates in self.discovery_support_gates
            },
            "discovery_economic_gates": dict(self.discovery_economic_gates),
            "walk_forward_gates": dict(self.walk_forward_gates),
        }


_SPLIT_POLICY: Final = (
    ("cross_boundary_position_policy", "TERMINAL_EXIT"),
    ("discovery_formula", "220+floor(2*(P-580)/5)"),
    ("discovery_no_entry_tail_days", 20),
    ("discovery_reporting_blocks", 4),
    ("holdout_visibility", "SEALED_UNTIL_FINALISTS_FROZEN"),
    ("minimum_eligible_active_days", 740),
    ("pre_holdout_symbol", "P=N-160"),
    ("remainder_assignment", "OLDEST_FIRST"),
    ("reserved_embargo_days", 20),
    ("sealed_holdout_decision_days", 120),
    ("sealed_holdout_outcome_tail_days", 20),
    ("walk_forward_folds", 5),
    ("walk_forward_minimum_active_days", 72),
    ("walk_forward_no_entry_tail_days", 20),
    ("walk_forward_visibility", "SEALED_UNTIL_ALL_FOLDS_COMPLETE"),
)

_DISCOVERY_SUPPORT_GATES: Final = (
    (
        300,
        (
            ("maximum_median_signals_per_active_day", 10),
            ("minimum_distinct_signal_days", 40),
            ("minimum_raw_signals", 160),
            ("minimum_signals_per_block", 25),
        ),
    ),
    (
        1_800,
        (
            ("maximum_median_signals_per_active_day", 6),
            ("minimum_distinct_signal_days", 35),
            ("minimum_raw_signals", 100),
            ("minimum_signals_per_block", 15),
        ),
    ),
    (
        3_600,
        (
            ("maximum_median_signals_per_active_day", 4),
            ("minimum_distinct_signal_days", 30),
            ("minimum_raw_signals", 80),
            ("minimum_signals_per_block", 12),
        ),
    ),
)

_DISCOVERY_ECONOMIC_GATES: Final = (
    ("baseline_net_ev_must_be_positive", True),
    ("component_connectivity", "ORTHOGONAL_4_NEIGHBOR"),
    ("maximum_single_positive_block_gross_share_denominator", 2),
    ("maximum_single_positive_block_gross_share_numerator", 1),
    ("minimum_filled_round_trips", 40),
    ("minimum_filled_round_trips_per_block", 8),
    ("minimum_neighbor_median_ev_ratio_denominator", 2),
    ("minimum_neighbor_median_ev_ratio_numerator", 1),
    ("minimum_positive_blocks", 3),
    ("minimum_positive_cells_in_3x3", 7),
    ("minimum_positive_component_cells", 9),
    ("minimum_worst_block_ev_ticks", -2),
    ("moderate_calendar_month_net_pnl_must_be_positive", True),
    ("moderate_minimum_profit_factor_denominator", 20),
    ("moderate_minimum_profit_factor_numerator", 21),
    ("moderate_net_pnl_must_be_positive", True),
    ("positive_block_gross_profit_definition", "SUM_POSITIVE_GROSS_TRADE_PNL_BEFORE_COSTS"),
    (
        "profit_factor_zero_loss_policy",
        "POSITIVE_GROSS_IS_POSITIVE_INFINITY_ZERO_GROSS_UNDEFINED",
    ),
    (
        "representative_policy",
        "COMPONENT_MEDOID_THEN_SMALLEST_SL_TP_WITHIN_10_PERCENT_MEDIAN_EV",
    ),
    ("required_reporting_blocks", 4),
)

_EXECUTION_SCENARIOS: Final = (
    BarExecutionScenarioSpec("BASELINE", 1, 1, 2, 1, 4, 1, 1),
    BarExecutionScenarioSpec("MODERATE_COMBINED", 2, 1, 4, 2, 5, 5, 4),
    BarExecutionScenarioSpec("SEVERE_DIAGNOSTIC", 3, 2, 6, 3, 6, 3, 2),
)

_WALK_FORWARD_GATES: Final = (
    ("benjamini_hochberg_q_denominator", 20),
    ("benjamini_hochberg_q_numerator", 1),
    ("bootstrap_confidence_denominator", 100),
    ("bootstrap_confidence_numerator", 95),
    ("bootstrap_replicates", 10_000),
    ("fold_count", 5),
    ("minimum_active_entry_days_aggregate", 150),
    ("minimum_active_entry_days_per_fold", 20),
    ("minimum_aggregate_profit_factor_denominator", 5),
    ("minimum_aggregate_profit_factor_numerator", 6),
    ("minimum_execution_contracts", 5),
    ("minimum_filled_trades_aggregate", 300),
    ("minimum_filled_trades_per_fold", 40),
    ("minimum_fold_profit_factor_denominator", 4),
    ("minimum_fold_profit_factor_numerator", 3),
    ("minimum_neighbor_median_ev_ratio_denominator", 2),
    ("minimum_neighbor_median_ev_ratio_numerator", 1),
    ("minimum_net_profit_to_drawdown_denominator", 2),
    ("minimum_net_profit_to_drawdown_numerator", 3),
    ("minimum_positive_folds", 4),
    ("minimum_positive_neighbor_cells", 7),
    ("moderate_minimum_profit_factor_denominator", 20),
    ("moderate_minimum_profit_factor_numerator", 21),
    ("neighbor_cell_count", 9),
)

_HOLDOUT_GATES: Final = (
    ("bootstrap_confidence_denominator", 100),
    ("bootstrap_confidence_numerator", 90),
    ("both_calendar_halves_must_be_positive", True),
    ("holm_alpha_denominator", 20),
    ("holm_alpha_numerator", 1),
    ("maximum_finalists", 10),
    ("minimum_active_entry_days", 40),
    ("minimum_execution_contracts", 2),
    ("minimum_filled_trades", 80),
    ("minimum_profit_factor_denominator", 20),
    ("minimum_profit_factor_numerator", 23),
)


def load_bar_pattern_config(
    project_root: Path,
    *,
    config_path: Path = BAR_PATTERN_CONFIG_RELATIVE_PATH,
) -> BarPatternResearchConfig:
    """Load the only accepted semantic v1 document and construct its catalog."""

    root = project_root.expanduser().resolve()
    requested = config_path if config_path.is_absolute() else root / config_path
    resolved = requested.expanduser().resolve()
    try:
        document = load_toml_document(resolved)
    except HypothesisConfigError as error:
        raise BarPatternConfigError(str(error)) from error
    semantic_sha256 = canonical_sha256(document)
    if semantic_sha256 != BAR_PATTERN_CONFIG_SEMANTIC_SHA256:
        raise BarPatternConfigError(
            "bar_pattern_discovery_v1.toml differs from the frozen semantic definition"
        )
    raw = resolved.read_bytes()
    return BarPatternResearchConfig(
        path=resolved,
        sha256=hashlib.sha256(raw).hexdigest(),
        semantic_sha256=semantic_sha256,
        candidates=frozen_bar_pattern_candidates(),
        split_policy=_SPLIT_POLICY,
        discovery_support_gates=_DISCOVERY_SUPPORT_GATES,
        discovery_economic_gates=_DISCOVERY_ECONOMIC_GATES,
        execution_scenarios=_EXECUTION_SCENARIOS,
        walk_forward_gates=_WALK_FORWARD_GATES,
        holdout_gates=_HOLDOUT_GATES,
    )
