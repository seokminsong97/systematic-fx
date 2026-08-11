"""Frozen State-Conditional Bar Model V2/V2A configurations and candidate catalogs.

The shared catalog is intentionally small: two signal clocks, two preregistered
feature sets, and three confidence margins produce exactly twelve candidates.
Each candidate emits one of ``LONG``, ``SHORT``, or ``NO_TRADE``; direction is
not doubled into separate variants.  V2A is an administrative successor whose
sole candidate-policy change is a larger optimizer iteration cap.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from systematic_fx.research.hypotheses import (
    HypothesisConfigError,
    canonical_sha256,
    load_toml_document,
)

BAR_STATE_CONFIG_RELATIVE_PATH: Final = Path("configs/research/bar_state_conditional_v2.toml")
BAR_STATE_CONFIG_FILE_SHA256: Final = (
    "8408a349ac2cd595e2104201185b361a5a58c7b24182babafe29e66f5c93a6e9"
)
BAR_STATE_CONFIG_SEMANTIC_SHA256: Final = (
    "7b2d5a1e70d59b97e699d0ee479670937975ba5bcd73bc003211a1bb856e84ba"
)
BAR_STATE_CONFIG_ID: Final = "bar_state_conditional_v2"
BAR_STATE_CAMPAIGN_KEY: Final = "bar_state_conditional_v2"
BAR_STATE_CANDIDATE_CATALOG_SHA256: Final = (
    "3e24dc08e9027ec604b5ab433368a54c4f7a4c89577599b79de372f62262120d"
)
BAR_STATE_CAMPAIGN_DEFINITION_SHA256: Final = (
    "4502e2ec1c40f344fce27066223a25e6b2f7456736e09fe0d96faab4171134f9"
)
BAR_STATE_V2A_CONFIG_RELATIVE_PATH: Final = Path("configs/research/bar_state_conditional_v2a.toml")
BAR_STATE_V2A_CONFIG_FILE_SHA256: Final = (
    "ecc4837c67e1c42ae69bfe0c74744e8aba9ba7cd99584b2dc0c091f6579f0a52"
)
BAR_STATE_V2A_CONFIG_SEMANTIC_SHA256: Final = (
    "2e2e3c6ee68af86fffa864ce736c24802eea7901a63d4ebda583327df06f156a"
)
BAR_STATE_V2A_CONFIG_ID: Final = "bar_state_conditional_v2a"
BAR_STATE_V2A_CAMPAIGN_KEY: Final = "bar_state_conditional_v2a"
BAR_STATE_V2A_CANDIDATE_CATALOG_SHA256: Final = (
    "97bbdacd0d655a1ca4e81085f3f25fb32da0bf31329bbd670ba89778611084d6"
)
BAR_STATE_V2A_CAMPAIGN_DEFINITION_SHA256: Final = (
    "8a332ad6998bb8bf48c3de94bc0ca660905a08acb848580ee5e31d9c42f8033c"
)
BAR_STATE_SCHEMA_VERSION: Final = 1
BAR_STATE_CANDIDATE_COUNT: Final = 12
BAR_STATE_MAXIMUM_FINALISTS: Final = 4
BAR_STATE_AUTHORIZED_STAGE: Final = "DISCOVERY_ONLY"
BAR_STATE_COST_MODEL_ID: Final = "BAR_TRADE_ONLY_COSTS_V1"

BAR_STATE_SIGNAL_TIMEFRAMES_SECONDS: Final = (300, 1_800)
BAR_STATE_LABEL_CLASSES: Final = ("UP_FIRST", "DOWN_FIRST", "CENSORED")
BAR_STATE_LABEL_HORIZON_ACTIVE_DAYS: Final = 20
BAR_STATE_LABEL_SIMULTANEOUS_POLICY: Final = "CENSORED_WITH_AMBIGUITY_COUNT"
BAR_STATE_LABEL_BOUNDARY_EVENT_ORDERING: Final = (
    "UNRESOLVED_AT_BOUNDARY_CENSORED_PRIOR_FIRST_TOUCH_PRESERVED"
)
BAR_STATE_OUTER_SPLIT_SHA256: Final = (
    "5594725f6769a706018d414a5b27e3903f1d7d1cc22c98e93b6e973ead1af043"
)
BAR_STATE_BAR_DATASET_MANIFEST_SHA256: Final = (
    "e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc"
)


class BarStateConfigError(ValueError):
    """A frozen bar-state campaign definition is missing or has drifted."""


@dataclass(frozen=True, slots=True)
class BarStateCampaignProfile:
    """One immutable administrative and optimizer identity for the shared engine."""

    version_id: str
    config_id: str
    campaign_key: str
    campaign_name: str
    engine_version: str
    config_relative_path: Path
    config_file_sha256: str
    config_semantic_sha256: str
    candidate_catalog_sha256: str
    campaign_definition_sha256: str
    model_max_iter: int
    amends_campaign_key: str | None = None
    predecessor_campaign_definition_sha256: str | None = None
    predecessor_code_commit: str | None = None
    predecessor_gate_policy: str | None = None

    def __post_init__(self) -> None:
        for field in (
            "version_id",
            "config_id",
            "campaign_key",
            "campaign_name",
            "engine_version",
        ):
            value = getattr(self, field)
            if not isinstance(value, str) or not value or value != value.strip():
                raise BarStateConfigError(f"campaign profile {field} must be canonical")
        if (
            not isinstance(self.config_relative_path, Path)
            or self.config_relative_path.is_absolute()
            or ".." in self.config_relative_path.parts
        ):
            raise BarStateConfigError("campaign config path must be relative and contained")
        for field in (
            "config_file_sha256",
            "config_semantic_sha256",
            "candidate_catalog_sha256",
            "campaign_definition_sha256",
        ):
            value = getattr(self, field)
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise BarStateConfigError(f"campaign profile {field} must be a SHA-256")
        if (
            isinstance(self.model_max_iter, bool)
            or not isinstance(self.model_max_iter, int)
            or self.model_max_iter <= 0
        ):
            raise BarStateConfigError("campaign model_max_iter must be a positive integer")
        if self.amends_campaign_key is not None and (
            not isinstance(self.amends_campaign_key, str)
            or not self.amends_campaign_key
            or self.amends_campaign_key == self.campaign_key
        ):
            raise BarStateConfigError("amended campaign identity is invalid")
        predecessor_fields = (
            self.predecessor_campaign_definition_sha256,
            self.predecessor_code_commit,
            self.predecessor_gate_policy,
        )
        if any(value is None for value in predecessor_fields) != all(
            value is None for value in predecessor_fields
        ):
            raise BarStateConfigError("predecessor gate identity must be complete or absent")
        if self.amends_campaign_key is None and any(
            value is not None for value in predecessor_fields
        ):
            raise BarStateConfigError("predecessor gate requires an amended campaign")
        if self.predecessor_campaign_definition_sha256 is not None and (
            len(self.predecessor_campaign_definition_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.predecessor_campaign_definition_sha256
            )
        ):
            raise BarStateConfigError("predecessor campaign definition SHA-256 is invalid")
        if self.predecessor_code_commit is not None and (
            len(self.predecessor_code_commit) != 40
            or any(
                character not in "0123456789abcdef" for character in self.predecessor_code_commit
            )
        ):
            raise BarStateConfigError("predecessor code commit is invalid")
        if self.predecessor_gate_policy is not None and (
            not self.predecessor_gate_policy
            or self.predecessor_gate_policy != self.predecessor_gate_policy.strip()
        ):
            raise BarStateConfigError("predecessor gate policy is invalid")

    @property
    def artifact_type(self) -> str:
        return self.campaign_key

    @property
    def experiment_key(self) -> str:
        return f"{self.campaign_key}:experiment:frozen_candidate_catalog:v1"


BAR_STATE_V2_PROFILE: Final = BarStateCampaignProfile(
    version_id="V2",
    config_id=BAR_STATE_CONFIG_ID,
    campaign_key=BAR_STATE_CAMPAIGN_KEY,
    campaign_name="Frozen conditional candle-state Discovery v2",
    engine_version="bar_state_conditional_discovery_v2",
    config_relative_path=BAR_STATE_CONFIG_RELATIVE_PATH,
    config_file_sha256=BAR_STATE_CONFIG_FILE_SHA256,
    config_semantic_sha256=BAR_STATE_CONFIG_SEMANTIC_SHA256,
    candidate_catalog_sha256=BAR_STATE_CANDIDATE_CATALOG_SHA256,
    campaign_definition_sha256=BAR_STATE_CAMPAIGN_DEFINITION_SHA256,
    model_max_iter=5_000,
)
BAR_STATE_V2A_PROFILE: Final = BarStateCampaignProfile(
    version_id="V2A",
    config_id=BAR_STATE_V2A_CONFIG_ID,
    campaign_key=BAR_STATE_V2A_CAMPAIGN_KEY,
    campaign_name="Frozen conditional candle-state Discovery v2a",
    engine_version="bar_state_conditional_discovery_v2a",
    config_relative_path=BAR_STATE_V2A_CONFIG_RELATIVE_PATH,
    config_file_sha256=BAR_STATE_V2A_CONFIG_FILE_SHA256,
    config_semantic_sha256=BAR_STATE_V2A_CONFIG_SEMANTIC_SHA256,
    candidate_catalog_sha256=BAR_STATE_V2A_CANDIDATE_CATALOG_SHA256,
    campaign_definition_sha256=BAR_STATE_V2A_CAMPAIGN_DEFINITION_SHA256,
    model_max_iter=50_000,
    amends_campaign_key=BAR_STATE_CAMPAIGN_KEY,
    predecessor_campaign_definition_sha256=BAR_STATE_CAMPAIGN_DEFINITION_SHA256,
    predecessor_code_commit="2ca2b0b6158c1d1e9d880c2ed65ec7d7582de189",
    predecessor_gate_policy="REQUIRE_EXACT_FAILED_PREDECESSOR_WITH_NO_OOS_EVIDENCE",
)
BAR_STATE_CAMPAIGN_PROFILES: Final = (
    BAR_STATE_V2_PROFILE,
    BAR_STATE_V2A_PROFILE,
)


def bar_state_campaign_profile(version_id: str = "V2") -> BarStateCampaignProfile:
    """Resolve one exact registered profile without accepting arbitrary campaigns."""

    matches = tuple(item for item in BAR_STATE_CAMPAIGN_PROFILES if item.version_id == version_id)
    if len(matches) != 1:
        raise BarStateConfigError(f"unknown bar-state campaign profile: {version_id!r}")
    return matches[0]


def require_bar_state_campaign_profile(
    profile: BarStateCampaignProfile,
) -> BarStateCampaignProfile:
    """Return the canonical member or reject a self-consistent unregistered profile."""

    if not isinstance(profile, BarStateCampaignProfile):
        raise BarStateConfigError("profile must be a BarStateCampaignProfile")
    matches = tuple(
        item for item in BAR_STATE_CAMPAIGN_PROFILES if item.version_id == profile.version_id
    )
    if len(matches) != 1 or matches[0] != profile:
        raise BarStateConfigError("bar-state campaign profile is not exactly registered")
    return matches[0]


@dataclass(frozen=True, slots=True)
class FrozenRatio:
    """One canonical positive rational without binary floating-point state."""

    numerator: int
    denominator: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.numerator, bool)
            or isinstance(self.denominator, bool)
            or not isinstance(self.numerator, int)
            or not isinstance(self.denominator, int)
            or self.numerator <= 0
            or self.denominator <= 0
        ):
            raise BarStateConfigError("frozen ratios must use positive integers")
        if math.gcd(self.numerator, self.denominator) != 1:
            raise BarStateConfigError("frozen ratios must be reduced")

    @property
    def text(self) -> str:
        return f"{self.numerator}/{self.denominator}"

    def as_dict(self) -> dict[str, int]:
        return {"denominator": self.denominator, "numerator": self.numerator}


MORPHOLOGY_FEATURE_IDS: Final = (
    "ret_1",
    "body_atr",
    "range_atr",
    "upper_wick_atr",
    "lower_wick_atr",
    "close_location",
)
STATE_FEATURE_IDS: Final = MORPHOLOGY_FEATURE_IDS + (
    "ret_3",
    "ret_6",
    "trend_6_atr",
    "realized_range_6",
    "atr_ratio_5_20",
    "volume_z20",
    "trade_count_z20",
    "buy_imbalance",
    "tod_sin",
    "tod_cos",
    "gap_from_prev_atr",
    "higher_tf_ret_1",
)

_FEATURE_FORMULAS: Final = (
    ("ret_1", "(C[t]-C[t-1])/ATR20[t]"),
    ("body_atr", "(C[t]-O[t])/ATR20[t]"),
    ("range_atr", "(H[t]-L[t])/ATR20[t]"),
    ("upper_wick_atr", "(H[t]-max(O[t],C[t]))/ATR20[t]"),
    ("lower_wick_atr", "(min(O[t],C[t])-L[t])/ATR20[t]"),
    ("close_location", "(C[t]-L[t])/(H[t]-L[t]);ZERO_RANGE=1/2"),
    ("ret_3", "(C[t]-C[t-3])/ATR20[t]"),
    ("ret_6", "(C[t]-C[t-6])/ATR20[t]"),
    ("trend_6_atr", "OLS_SLOPE_PER_BAR(C[t-5:t])/ATR20[t]"),
    ("realized_range_6", "(max(H[t-5:t])-min(L[t-5:t]))/ATR20[t]"),
    ("atr_ratio_5_20", "MEAN(TR[t-4:t])/MEAN(TR[t-19:t])"),
    ("volume_z20", "POPULATION_ZSCORE(VOLUME[t-19:t]);ZERO_STD=0"),
    ("trade_count_z20", "POPULATION_ZSCORE(TRADE_COUNT[t-19:t]);ZERO_STD=0"),
    (
        "buy_imbalance",
        "(BUY_VOLUME-SELL_VOLUME)/(BUY_VOLUME+SELL_VOLUME);MISSING_OR_ZERO=0",
    ),
    ("tod_sin", "sin(2*pi*UTC_BUCKET_START_SECOND/86400)"),
    ("tod_cos", "cos(2*pi*UTC_BUCKET_START_SECOND/86400)"),
    ("gap_from_prev_atr", "(O[t]-C[t-1])/ATR20[t]"),
    ("higher_tf_ret_1", "5M=(C30[k]-C30[k-1])/ATR20_30[k];30M=0"),
)
_FEATURE_FORMULA_BY_ID: Final = dict(_FEATURE_FORMULAS)


@dataclass(frozen=True, slots=True)
class BarStateFeatureSetSpec:
    feature_set_id: str
    version: str
    feature_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.feature_set_id not in {"MORPHOLOGY", "STATE"}:
            raise BarStateConfigError("unknown state-model feature set")
        if not self.version or not self.feature_ids:
            raise BarStateConfigError("feature-set version and features are required")
        if len(self.feature_ids) != len(set(self.feature_ids)):
            raise BarStateConfigError("feature-set IDs must be unique")
        if any(item not in _FEATURE_FORMULA_BY_ID for item in self.feature_ids):
            raise BarStateConfigError("feature set contains an undefined feature")

    def as_dict(self) -> dict[str, object]:
        return {
            "feature_definitions": [
                {"feature_id": item, "formula": _FEATURE_FORMULA_BY_ID[item]}
                for item in self.feature_ids
            ],
            "feature_ids": list(self.feature_ids),
            "feature_set_id": self.feature_set_id,
            "version": self.version,
        }


MORPHOLOGY_FEATURE_SET: Final = BarStateFeatureSetSpec(
    "MORPHOLOGY", "bar_state_morphology_v1", MORPHOLOGY_FEATURE_IDS
)
STATE_FEATURE_SET: Final = BarStateFeatureSetSpec("STATE", "bar_state_full_v1", STATE_FEATURE_IDS)
BAR_STATE_FEATURE_SETS: Final = (MORPHOLOGY_FEATURE_SET, STATE_FEATURE_SET)
BAR_STATE_FEATURE_SET_BY_ID: Final = {item.feature_set_id: item for item in BAR_STATE_FEATURE_SETS}

BAR_STATE_CONFIDENCE_MARGINS: Final = (
    FrozenRatio(1, 20),
    FrozenRatio(1, 10),
    FrozenRatio(3, 20),
)
BAR_STATE_ECONOMIC_MULTIPLIERS: Final = (
    FrozenRatio(1, 2),
    FrozenRatio(3, 4),
    FrozenRatio(1, 1),
    FrozenRatio(3, 2),
    FrozenRatio(2, 1),
    FrozenRatio(3, 1),
    FrozenRatio(4, 1),
)


@dataclass(frozen=True, slots=True)
class BarStateExecutionScenarioSpec:
    scenario_id: str
    entry_adverse_ticks: int
    take_profit_trade_through_ticks: int
    stop_total_minimum_adverse_ticks: int
    terminal_exit_adverse_ticks: int
    variable_debit_ticks: int
    fixed_pool_multiplier: FrozenRatio

    def as_dict(self) -> dict[str, object]:
        return {
            "entry_adverse_ticks": self.entry_adverse_ticks,
            "fixed_pool_multiplier": self.fixed_pool_multiplier.as_dict(),
            "scenario_id": self.scenario_id,
            "stop_total_minimum_adverse_ticks": self.stop_total_minimum_adverse_ticks,
            "take_profit_trade_through_ticks": self.take_profit_trade_through_ticks,
            "terminal_exit_adverse_ticks": self.terminal_exit_adverse_ticks,
            "variable_debit_ticks": self.variable_debit_ticks,
        }


BAR_STATE_EXECUTION_SCENARIOS: Final = (
    BarStateExecutionScenarioSpec("BASELINE", 1, 1, 2, 1, 4, FrozenRatio(1, 1)),
    BarStateExecutionScenarioSpec("MODERATE_COMBINED", 2, 1, 4, 2, 5, FrozenRatio(5, 4)),
    BarStateExecutionScenarioSpec("SEVERE_DIAGNOSTIC", 3, 2, 6, 3, 6, FrozenRatio(3, 2)),
)


def frozen_model_policy(*, max_iter: int = 5_000) -> dict[str, object]:
    """Return the shared sklearn policy with one explicit optimizer cap."""

    if isinstance(max_iter, bool) or not isinstance(max_iter, int) or max_iter <= 0:
        raise BarStateConfigError("model max_iter must be a positive integer")

    return {
        "coefficient_refit_policy": "EXPANDING_PRIOR_MATURED_ROWS_ONLY",
        "convergence_failure_policy": "HARD_FAIL",
        "family": "ELASTIC_NET_MULTINOMIAL_LOGISTIC",
        "hyperparameter_refit_allowed": False,
        "implementation": "sklearn.linear_model.LogisticRegression",
        "multiclass_policy": "MULTINOMIAL",
        "n_jobs_argument_policy": "OMIT_ON_SKLEARN_1_9_NO_EFFECT",
        "regularization": {
            "deprecated_penalty_argument_policy": "OMIT_ON_SKLEARN_1_9",
            "family": "ELASTIC_NET_VIA_L1_RATIO",
        },
        "scaler": {
            "actual_sklearn_arguments": {
                "copy": True,
                "with_mean": True,
                "with_std": True,
            },
            "fit_scope": "TRAIN_ONLY",
            "implementation": "sklearn.preprocessing.StandardScaler",
        },
        "sklearn_arguments": {
            "C_decimal": "0.1",
            "class_weight": "balanced",
            "fit_intercept": True,
            "l1_ratio_decimal": "0.5",
            "max_iter": max_iter,
            "random_state": 20_260_809,
            "solver": "saga",
            "tol_decimal": "0.00000001",
        },
    }


def frozen_label_policy() -> dict[str, object]:
    return {
        "artificial_split_boundary_policy": "DO_NOT_TERMINATE_LABEL",
        "boundary_event_ordering": BAR_STATE_LABEL_BOUNDARY_EVENT_ORDERING,
        "class_order": list(BAR_STATE_LABEL_CLASSES),
        "contract_boundary_policy": "CENSORED",
        "distance": {
            "maximum_ticks": 192,
            "minimum_ticks": 24,
            "rounding": "NEAREST_8_TICKS_HALF_UP",
            "symmetric_multiplier": FrozenRatio(1, 1).as_dict(),
            "volatility": "PRIOR_ATR20_TICKS",
        },
        "entry_reference": "NEXT_SIGNAL_BAR_FIRST_TRADE",
        "observation_horizon_active_days": BAR_STATE_LABEL_HORIZON_ACTIVE_DAYS,
        "quality_boundary_policy": "CENSORED",
        "simultaneous_touch_policy": BAR_STATE_LABEL_SIMULTANEOUS_POLICY,
        "split_policy": "SPLIT_INDEPENDENT_CHRONOLOGICAL_LABEL",
    }


def frozen_economic_barrier_policy() -> dict[str, object]:
    return {
        "axis_count": 7,
        "cell_count": 49,
        "distance_rounding": "NEAREST_8_TICKS_HALF_UP",
        "duplicate_realized_distances_recorded": True,
        "insufficient_distinct_distance_policy": "CANDIDATE_OOS_REJECT",
        "maximum_distance_ticks": 192,
        "minimum_distance_ticks": 24,
        "minimum_distinct_realized_distances_per_axis": 4,
        "selection_policy": "STABLE_CONNECTED_REGION_NOT_SINGLE_BEST_CELL",
        "stop_loss_multipliers": [item.as_dict() for item in BAR_STATE_ECONOMIC_MULTIPLIERS],
        "take_profit_multipliers": [item.as_dict() for item in BAR_STATE_ECONOMIC_MULTIPLIERS],
        "volatility_distance": "PRIOR_ATR20_TICKS",
    }


def frozen_entry_policy() -> dict[str, object]:
    return {
        "decision_time_policy": "SIGNAL_BAR_END",
        "entry_price_policy": "NEXT_SIGNAL_BAR_FIRST_TRADE_PLUS_SCENARIO_ADVERSITY",
        "missing_next_bar_policy": "ENTRY_NOT_FILLED",
        "occupied_signal_policy": "SKIPPED_OCCUPIED",
        "one_position_policy": "ONE_NET_POSITION_PER_CANDIDATE_SCENARIO_BARRIER_CELL",
        "portfolio_same_second_touch_policy": "DIRECTION_SPECIFIC_STOP_FIRST",
        "portfolio_terminal_policy": "CONTRACT_QUALITY_OR_EVALUATION_SPLIT_END",
        "successor_policy": (
            "IMMEDIATE_CHRONOLOGICAL_OBSERVED_SIGNAL_BAR_WITHIN_SAME_OUTCOME_SPAN"
        ),
    }


def frozen_discovery_selection_policy() -> dict[str, object]:
    """Return every State V2 Discovery gate; no V1 defaults are implicit."""

    return {
        "barrier_stability": {
            "component_connectivity": "ORTHOGONAL_4_NEIGHBOR",
            "core_positive_cell_definition": (
                "BASELINE_EV_POSITIVE_AND_MODERATE_EV_AND_NET_PNL_POSITIVE"
            ),
            "minimum_neighbor_median_ev_ratio": FrozenRatio(1, 2).as_dict(),
            "minimum_positive_cells_in_3x3": 7,
            "minimum_positive_component_cells": 9,
            "neighbor_median_scenario": "MODERATE_COMBINED",
            "post_bh_minimum_component_cells": 9,
        },
        "bootstrap": {
            "circular_blocks": True,
            "evaluation_calendar": "OOS_DECISIONS_PLUS_20_ACTIVE_DAY_OUTCOME_TAIL",
            "fold_calendar_lengths": [117, 117, 137],
            "generator": "NUMPY_GENERATOR_PCG64",
            "input": ("FOLD_LOCAL_EXIT_ACTIVE_DATE_ALIGNED_DAILY_NET_TICKS_AND_FILL_COUNTS"),
            "lower_bound_order_statistic": "ONE_INDEXED_500_OF_10000_UNCENTERED",
            "lower_bound_net_ev_must_be_positive": True,
            "mean_block_length_active_days": 10,
            "method": "POLITIS_ROMANO_STATIONARY_BLOCK_BOOTSTRAP",
            "null": "CENTERED_AT_OBSERVED_NET_EV",
            "one_sided_confidence": FrozenRatio(19, 20).as_dict(),
            "p_value": ("ONE_PLUS_NULL_REPLICATES_AT_LEAST_OBSERVED_DIVIDED_BY_10001"),
            "pnl_and_fill_attribution": (
                "CLOSED_TRADE_ASSIGNED_TO_EXIT_ACTIVE_DATE_WITH_EXPLICIT_ZERO_DAYS"
            ),
            "random_seed": 20_260_809,
            "replicates": 10_000,
            "restart_probability": FrozenRatio(1, 10).as_dict(),
            "shared_indices_across_cells": True,
            "statistic": "SUM_NET_TICKS_DIVIDED_BY_SUM_FILL_COUNT",
            "zero_fill_lower_bound_policy": "NEGATIVE_INFINITY",
            "zero_fill_p_value_policy": "COUNT_AS_EXCEEDANCE",
        },
        "concentration": {
            "maximum_single_contract_positive_gross_share": FrozenRatio(1, 2).as_dict(),
            "maximum_single_inner_oos_positive_gross_share": FrozenRatio(1, 2).as_dict(),
        },
        "economics": {
            "baseline_net_ev_must_be_positive": True,
            "minimum_filled_round_trips": 40,
            "minimum_filled_round_trips_per_inner_oos_fold": 8,
            "minimum_positive_inner_oos_folds": 2,
            "moderate_calendar_month_net_pnl_must_be_positive": True,
            "moderate_minimum_profit_factor": FrozenRatio(11, 10).as_dict(),
            "moderate_minimum_worst_inner_oos_ev_ticks": -2,
            "moderate_net_pnl_must_be_positive": True,
            "severe_net_ev_must_be_nonnegative": True,
        },
        "maximum_finalists": BAR_STATE_MAXIMUM_FINALISTS,
        "multiplicity": {
            "benjamini_hochberg_q": FrozenRatio(1, 20).as_dict(),
            "family_order": ("PREDECESSOR_THEN_CANONICAL_CANDIDATE_TP_INDEX_SL_INDEX"),
            "family_size": 804,
            "ineligible_state_cell_p_value_policy": "FIXED_AT_ONE",
            "post_bh_component_policy": ("ORTHOGONAL_COMPONENT_MUST_RETAIN_AT_LEAST_9_CELLS"),
            "predecessor_p_value_policy": "ALL_216_FIXED_AT_ONE",
            "predecessor_bar_pattern_variants": 216,
            "record_all_model_threshold_seed_and_barrier_trials": True,
            "state_model_barrier_cells": 588,
            "state_model_variants": BAR_STATE_CANDIDATE_COUNT,
        },
        "ranking": {
            "component_representative_policy": ("LARGEST_POST_BH_COMPONENT_MANHATTAN_MEDOID"),
            "component_tie_order": (
                "WORST_FOLD_MODERATE_EV_DESC_OVERALL_MODERATE_EV_DESC_"
                "BOOTSTRAP_LCB_DESC_MDD_ASC_SL_ASC_TP_ASC"
            ),
            "finalist_rank_order": (
                "POSITIVE_FOLDS_DESC_WORST_FOLD_MODERATE_EV_DESC_"
                "BOOTSTRAP_LCB_DESC_OVERALL_MODERATE_EV_DESC_MDD_ASC_"
                "SL_ASC_TP_ASC_CANDIDATE_KEY_ASC"
            ),
        },
    }


def _margin_code(margin: FrozenRatio) -> str:
    codes = {(1, 20): "005", (1, 10): "010", (3, 20): "015"}
    try:
        return codes[(margin.numerator, margin.denominator)]
    except KeyError as error:  # pragma: no cover - candidates use frozen margins
        raise BarStateConfigError("confidence margin is outside the frozen catalog") from error


@dataclass(frozen=True, slots=True)
class BarStateCandidate:
    """One complete state-model pipeline, including its decision margin."""

    candidate_key: str
    timeframe_seconds: int
    feature_set: BarStateFeatureSetSpec
    confidence_margin: FrozenRatio
    cost_model_id: str = BAR_STATE_COST_MODEL_ID
    model_max_iter: int = 5_000

    def __post_init__(self) -> None:
        if self.timeframe_seconds not in BAR_STATE_SIGNAL_TIMEFRAMES_SECONDS:
            raise BarStateConfigError("candidate timeframe is outside the frozen catalog")
        if BAR_STATE_FEATURE_SET_BY_ID.get(self.feature_set.feature_set_id) != self.feature_set:
            raise BarStateConfigError("candidate feature set differs from the frozen catalog")
        if self.confidence_margin not in BAR_STATE_CONFIDENCE_MARGINS:
            raise BarStateConfigError("candidate margin is outside the frozen catalog")
        if self.cost_model_id != BAR_STATE_COST_MODEL_ID:
            raise BarStateConfigError("candidate cost model differs from the frozen policy")
        if (
            isinstance(self.model_max_iter, bool)
            or not isinstance(self.model_max_iter, int)
            or self.model_max_iter <= 0
        ):
            raise BarStateConfigError("candidate model_max_iter must be a positive integer")
        expected = (
            f"bsv2_tf{self.timeframe_seconds:04d}_"
            f"fs{self.feature_set.feature_set_id.lower()}_cm{_margin_code(self.confidence_margin)}"
        )
        if self.candidate_key != expected:
            raise BarStateConfigError("candidate key differs from its canonical dimensions")

    def as_dict(self) -> dict[str, object]:
        margin = self.confidence_margin.as_dict()
        return {
            "candidate_key": self.candidate_key,
            "confidence_margin": margin,
            "cost_model": {
                "base_monthly_fixed_pool_usd": "500.00",
                "expected_monthly_round_trips": 20,
                "model_id": self.cost_model_id,
                "scenarios": [item.as_dict() for item in BAR_STATE_EXECUTION_SCENARIOS],
                "tick_value_usd": "6.25",
            },
            "economic_barrier_policy": frozen_economic_barrier_policy(),
            "entry_policy": frozen_entry_policy(),
            "feature_policy": {
                "atr_arithmetic": "EXACT_RATIONAL_MEAN",
                "atr_policy": "PRIOR_ATR20_TICKS",
                "atr_window": "TRUE_RANGE_T_MINUS_19_THROUGH_T_WITH_T_MINUS_20_CLOSE",
                "feature_availability": "POINT_IN_TIME_AT_SIGNAL_BAR_CLOSE",
                "feature_set": self.feature_set.as_dict(),
                "forward_fill_allowed": False,
                "higher_timeframe_30m": "ZERO_SENTINEL",
                "higher_timeframe_5m": "LATEST_CAUSALLY_COMPLETED_30M",
                "segment_policy": "RESET_ON_CONTRACT_QUALITY_OR_SOURCE_SEGMENT_BOUNDARY",
            },
            "label_policy": frozen_label_policy(),
            "model_policy": frozen_model_policy(max_iter=self.model_max_iter),
            "prediction_policy": {
                "long_rule": "SCORE_GREATER_THAN_OR_EQUAL_TO_MARGIN",
                "margin": margin,
                "otherwise": "NO_TRADE",
                "probability_class_order": list(BAR_STATE_LABEL_CLASSES),
                "score": "P_UP_FIRST_MINUS_P_DOWN_FIRST",
                "short_rule": "SCORE_LESS_THAN_OR_EQUAL_TO_NEGATIVE_MARGIN",
                "tie_policy": "NO_TRADE",
            },
            "selection_policy": frozen_discovery_selection_policy(),
            "timeframe_seconds": self.timeframe_seconds,
        }

    @property
    def definition_sha256(self) -> str:
        return canonical_sha256(self.as_dict())


def frozen_bar_state_candidates(
    *,
    profile: BarStateCampaignProfile = BAR_STATE_V2_PROFILE,
) -> tuple[BarStateCandidate, ...]:
    """Return one profile's twelve candidates in their shared stable order."""

    profile = require_bar_state_campaign_profile(profile)

    candidates = tuple(
        BarStateCandidate(
            candidate_key=(
                f"bsv2_tf{timeframe:04d}_fs{feature_set.feature_set_id.lower()}_"
                f"cm{_margin_code(margin)}"
            ),
            timeframe_seconds=timeframe,
            feature_set=feature_set,
            confidence_margin=margin,
            model_max_iter=profile.model_max_iter,
        )
        for timeframe in BAR_STATE_SIGNAL_TIMEFRAMES_SECONDS
        for feature_set in BAR_STATE_FEATURE_SETS
        for margin in BAR_STATE_CONFIDENCE_MARGINS
    )
    if len(candidates) != BAR_STATE_CANDIDATE_COUNT:
        raise AssertionError("frozen state-model candidate count drift")
    if len({item.candidate_key for item in candidates}) != len(candidates):
        raise AssertionError("frozen state-model candidate key collision")
    if len({item.definition_sha256 for item in candidates}) != len(candidates):
        raise AssertionError("frozen state-model candidate definition collision")
    return candidates


@dataclass(frozen=True, slots=True)
class BarStateResearchConfig:
    """Immutable identity and complete catalog for one Discovery campaign."""

    profile: BarStateCampaignProfile
    path: Path
    sha256: str
    semantic_sha256: str
    candidates: tuple[BarStateCandidate, ...]

    @property
    def candidate_catalog_sha256(self) -> str:
        return canonical_sha256([item.as_dict() for item in self.candidates])

    @property
    def definition_sha256(self) -> str:
        return canonical_sha256(self.as_dict())

    def candidate(self, candidate_key: str) -> BarStateCandidate:
        for item in self.candidates:
            if item.candidate_key == candidate_key:
                return item
        raise KeyError(f"unknown state-model candidate: {candidate_key}")

    def as_dict(self) -> dict[str, object]:
        document: dict[str, object] = {
            "authorized_stage": BAR_STATE_AUTHORIZED_STAGE,
            "bar_dataset_manifest_sha256": BAR_STATE_BAR_DATASET_MANIFEST_SHA256,
            "campaign_key": self.profile.campaign_key,
            "candidate_catalog_sha256": self.candidate_catalog_sha256,
            "candidate_count": len(self.candidates),
            "candidates": [item.as_dict() for item in self.candidates],
            "config_file_sha256": self.sha256,
            "config_id": self.profile.config_id,
            "config_semantic_sha256": self.semantic_sha256,
            "maximum_finalists": BAR_STATE_MAXIMUM_FINALISTS,
            "outer_split_plan_sha256": BAR_STATE_OUTER_SPLIT_SHA256,
            "schema_version": BAR_STATE_SCHEMA_VERSION,
        }
        if self.profile.amends_campaign_key is not None:
            document["optimizer_cap_amendment"] = {
                "amendment_scope": "OPTIMIZER_MAX_ITER_CAP_ONLY",
                "predecessor_campaign_definition_sha256": (
                    self.profile.predecessor_campaign_definition_sha256
                ),
                "predecessor_campaign_key": self.profile.amends_campaign_key,
                "predecessor_code_commit": self.profile.predecessor_code_commit,
                "predecessor_gate_policy": self.profile.predecessor_gate_policy,
            }
        return document


def load_bar_state_config(
    project_root: Path,
    *,
    config_path: Path | None = None,
    profile: BarStateCampaignProfile = BAR_STATE_V2_PROFILE,
) -> BarStateResearchConfig:
    """Load a byte- and semantic-exact profile; V2 remains the default."""

    profile = require_bar_state_campaign_profile(profile)
    root = project_root.expanduser().resolve()
    selected_path = profile.config_relative_path if config_path is None else config_path
    requested = selected_path if selected_path.is_absolute() else root / selected_path
    resolved = requested.expanduser().resolve()
    try:
        document = load_toml_document(resolved)
    except HypothesisConfigError as error:
        raise BarStateConfigError(str(error)) from error
    semantic_sha256 = canonical_sha256(document)
    if semantic_sha256 != profile.config_semantic_sha256:
        raise BarStateConfigError(
            f"{profile.config_relative_path.name} differs from the frozen semantic definition"
        )
    raw = resolved.read_bytes()
    file_sha256 = hashlib.sha256(raw).hexdigest()
    if file_sha256 != profile.config_file_sha256:
        raise BarStateConfigError(
            f"{profile.config_relative_path.name} differs from the frozen byte identity"
        )
    if (
        document.get("schema_version") != BAR_STATE_SCHEMA_VERSION
        or document.get("config_id") != profile.config_id
        or document.get("campaign_key") != profile.campaign_key
        or document.get("authorized_stage") != BAR_STATE_AUTHORIZED_STAGE
    ):
        raise BarStateConfigError("state-model root identity differs from the frozen campaign")
    config = BarStateResearchConfig(
        profile=profile,
        path=resolved,
        sha256=file_sha256,
        semantic_sha256=semantic_sha256,
        candidates=frozen_bar_state_candidates(profile=profile),
    )
    if config.candidate_catalog_sha256 != profile.candidate_catalog_sha256:
        raise BarStateConfigError("candidate catalog differs from the frozen campaign profile")
    if config.definition_sha256 != profile.campaign_definition_sha256:
        raise BarStateConfigError("campaign definition differs from the frozen campaign profile")
    return config


def load_bar_state_v2a_config(
    project_root: Path,
    *,
    config_path: Path | None = None,
) -> BarStateResearchConfig:
    """Load the explicit optimizer-cap amendment without changing the V2 default."""

    return load_bar_state_config(
        project_root,
        config_path=config_path,
        profile=BAR_STATE_V2A_PROFILE,
    )
