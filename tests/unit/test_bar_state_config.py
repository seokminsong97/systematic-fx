from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest

from systematic_fx.backtest.bar_replay import BAR_EXECUTION_SCENARIOS
from systematic_fx.research.bar_state_config import (
    BAR_STATE_CANDIDATE_COUNT,
    BAR_STATE_CONFIG_FILE_SHA256,
    BAR_STATE_CONFIG_RELATIVE_PATH,
    BAR_STATE_CONFIG_SEMANTIC_SHA256,
    BAR_STATE_ECONOMIC_MULTIPLIERS,
    BAR_STATE_EXECUTION_SCENARIOS,
    MORPHOLOGY_FEATURE_IDS,
    STATE_FEATURE_IDS,
    BarStateCandidate,
    BarStateConfigError,
    load_bar_state_config,
)
from systematic_fx.research.bar_state_features import BarStateFeatureSpec

ROOT = Path(__file__).resolve().parents[2]


def _contains_float(value: object) -> bool:
    if isinstance(value, float):
        return True
    if isinstance(value, dict):
        return any(_contains_float(key) or _contains_float(item) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return any(_contains_float(item) for item in value)
    return False


def test_loader_freezes_file_semantics_and_twelve_candidate_catalog() -> None:
    config = load_bar_state_config(ROOT)

    assert config.sha256 == BAR_STATE_CONFIG_FILE_SHA256
    assert config.semantic_sha256 == BAR_STATE_CONFIG_SEMANTIC_SHA256
    assert len(config.candidates) == BAR_STATE_CANDIDATE_COUNT == 12
    assert config.candidates[0].candidate_key == "bsv2_tf0300_fsmorphology_cm005"
    assert config.candidates[-1].candidate_key == "bsv2_tf1800_fsstate_cm015"
    assert len({item.candidate_key for item in config.candidates}) == 12
    assert len({item.definition_sha256 for item in config.candidates}) == 12
    assert Counter(item.timeframe_seconds for item in config.candidates) == {300: 6, 1_800: 6}
    assert Counter(item.feature_set.feature_set_id for item in config.candidates) == {
        "MORPHOLOGY": 6,
        "STATE": 6,
    }
    assert Counter(item.confidence_margin.text for item in config.candidates) == {
        "1/20": 4,
        "1/10": 4,
        "3/20": 4,
    }
    assert len(config.candidate_catalog_sha256) == 64
    assert len(config.definition_sha256) == 64


def test_candidate_payload_is_complete_and_has_no_binary_floats() -> None:
    candidate = load_bar_state_config(ROOT).candidates[-1]
    payload = candidate.as_dict()

    assert not _contains_float(payload)
    assert payload["feature_policy"]["feature_set"]["feature_ids"] == list(STATE_FEATURE_IDS)
    assert payload["model_policy"]["sklearn_arguments"] == {
        "C_decimal": "0.1",
        "class_weight": "balanced",
        "fit_intercept": True,
        "l1_ratio_decimal": "0.5",
        "max_iter": 5_000,
        "random_state": 20_260_809,
        "solver": "saga",
        "tol_decimal": "0.00000001",
    }
    assert payload["model_policy"]["n_jobs_argument_policy"] == ("OMIT_ON_SKLEARN_1_9_NO_EFFECT")
    assert payload["model_policy"]["scaler"] == {
        "actual_sklearn_arguments": {"copy": True, "with_mean": True, "with_std": True},
        "fit_scope": "TRAIN_ONLY",
        "implementation": "sklearn.preprocessing.StandardScaler",
    }
    assert payload["model_policy"]["regularization"] == {
        "deprecated_penalty_argument_policy": "OMIT_ON_SKLEARN_1_9",
        "family": "ELASTIC_NET_VIA_L1_RATIO",
    }
    assert payload["model_policy"]["convergence_failure_policy"] == "HARD_FAIL"
    assert payload["label_policy"]["class_order"] == [
        "UP_FIRST",
        "DOWN_FIRST",
        "CENSORED",
    ]
    assert payload["label_policy"]["simultaneous_touch_policy"] == ("CENSORED_WITH_AMBIGUITY_COUNT")
    assert payload["label_policy"]["boundary_event_ordering"] == (
        "UNRESOLVED_AT_BOUNDARY_CENSORED_PRIOR_FIRST_TOUCH_PRESERVED"
    )
    assert payload["label_policy"]["distance"] == {
        "maximum_ticks": 192,
        "minimum_ticks": 24,
        "rounding": "NEAREST_8_TICKS_HALF_UP",
        "symmetric_multiplier": {"denominator": 1, "numerator": 1},
        "volatility": "PRIOR_ATR20_TICKS",
    }
    assert payload["prediction_policy"]["score"] == "P_UP_FIRST_MINUS_P_DOWN_FIRST"
    assert payload["prediction_policy"]["margin"] == {"denominator": 20, "numerator": 3}
    barriers = payload["economic_barrier_policy"]
    assert barriers["cell_count"] == 49
    assert barriers["minimum_distinct_realized_distances_per_axis"] == 4
    assert barriers["insufficient_distinct_distance_policy"] == "CANDIDATE_OOS_REJECT"
    assert barriers["take_profit_multipliers"] == [
        item.as_dict() for item in BAR_STATE_ECONOMIC_MULTIPLIERS
    ]
    assert payload["entry_policy"]["one_position_policy"] == (
        "ONE_NET_POSITION_PER_CANDIDATE_SCENARIO_BARRIER_CELL"
    )
    assert payload["entry_policy"]["portfolio_same_second_touch_policy"] == (
        "DIRECTION_SPECIFIC_STOP_FIRST"
    )
    assert payload["entry_policy"]["successor_policy"] == (
        "IMMEDIATE_CHRONOLOGICAL_OBSERVED_SIGNAL_BAR_WITHIN_SAME_OUTCOME_SPAN"
    )
    selection = payload["selection_policy"]
    assert selection["economics"]["moderate_minimum_profit_factor"] == {
        "denominator": 10,
        "numerator": 11,
    }
    assert selection["economics"]["moderate_minimum_worst_inner_oos_ev_ticks"] == -2
    assert selection["economics"]["severe_net_ev_must_be_nonnegative"] is True
    assert selection["bootstrap"] == {
        "circular_blocks": True,
        "evaluation_calendar": "OOS_DECISIONS_PLUS_20_ACTIVE_DAY_OUTCOME_TAIL",
        "fold_calendar_lengths": [117, 117, 137],
        "generator": "NUMPY_GENERATOR_PCG64",
        "input": "FOLD_LOCAL_EXIT_ACTIVE_DATE_ALIGNED_DAILY_NET_TICKS_AND_FILL_COUNTS",
        "lower_bound_order_statistic": "ONE_INDEXED_500_OF_10000_UNCENTERED",
        "lower_bound_net_ev_must_be_positive": True,
        "mean_block_length_active_days": 10,
        "method": "POLITIS_ROMANO_STATIONARY_BLOCK_BOOTSTRAP",
        "null": "CENTERED_AT_OBSERVED_NET_EV",
        "one_sided_confidence": {"denominator": 20, "numerator": 19},
        "p_value": "ONE_PLUS_NULL_REPLICATES_AT_LEAST_OBSERVED_DIVIDED_BY_10001",
        "pnl_and_fill_attribution": (
            "CLOSED_TRADE_ASSIGNED_TO_EXIT_ACTIVE_DATE_WITH_EXPLICIT_ZERO_DAYS"
        ),
        "random_seed": 20_260_809,
        "replicates": 10_000,
        "restart_probability": {"denominator": 10, "numerator": 1},
        "shared_indices_across_cells": True,
        "statistic": "SUM_NET_TICKS_DIVIDED_BY_SUM_FILL_COUNT",
        "zero_fill_lower_bound_policy": "NEGATIVE_INFINITY",
        "zero_fill_p_value_policy": "COUNT_AS_EXCEEDANCE",
    }
    assert selection["multiplicity"]["benjamini_hochberg_q"] == {
        "denominator": 20,
        "numerator": 1,
    }
    assert selection["maximum_finalists"] == 4
    assert selection["barrier_stability"]["post_bh_minimum_component_cells"] == 9
    assert selection["multiplicity"]["family_size"] == 804
    assert selection["ranking"]["finalist_rank_order"].endswith("CANDIDATE_KEY_ASC")


def test_feature_sets_are_nested_and_bounded() -> None:
    assert MORPHOLOGY_FEATURE_IDS == STATE_FEATURE_IDS[: len(MORPHOLOGY_FEATURE_IDS)]
    assert len(MORPHOLOGY_FEATURE_IDS) == 6
    assert len(STATE_FEATURE_IDS) == 18
    assert len(set(STATE_FEATURE_IDS)) == len(STATE_FEATURE_IDS)


def test_every_candidate_binds_the_runtime_feature_spec_exactly() -> None:
    for candidate in load_bar_state_config(ROOT).candidates:
        runtime = BarStateFeatureSpec(
            timeframe_seconds=candidate.timeframe_seconds,
            feature_set_id=candidate.feature_set.feature_set_id,
            feature_names=candidate.feature_set.feature_ids,
        )
        assert runtime.feature_set_id == candidate.feature_set.feature_set_id
        assert runtime.feature_names == candidate.feature_set.feature_ids


def test_cost_scenarios_match_existing_bar_replay_engine() -> None:
    assert len(BAR_STATE_EXECUTION_SCENARIOS) == 3
    for spec in BAR_STATE_EXECUTION_SCENARIOS:
        engine = BAR_EXECUTION_SCENARIOS[spec.scenario_id]
        assert engine.entry_adverse_ticks == spec.entry_adverse_ticks
        assert engine.take_profit_trade_through_ticks == spec.take_profit_trade_through_ticks
        assert engine.stop_total_minimum_adverse_ticks == spec.stop_total_minimum_adverse_ticks
        assert engine.terminal_exit_adverse_ticks == spec.terminal_exit_adverse_ticks
        assert engine.variable_debit_ticks == spec.variable_debit_ticks
        assert engine.fixed_pool_multiplier.as_integer_ratio() == (
            spec.fixed_pool_multiplier.numerator,
            spec.fixed_pool_multiplier.denominator,
        )


def test_loader_rejects_semantic_and_byte_only_drift(tmp_path: Path) -> None:
    source = (ROOT / BAR_STATE_CONFIG_RELATIVE_PATH).read_text(encoding="utf-8")
    semantic_drift = tmp_path / "semantic.toml"
    semantic_drift.write_text(
        source.replace('authorized_stage = "DISCOVERY_ONLY"', 'authorized_stage = "HOLDOUT"'),
        encoding="utf-8",
    )
    with pytest.raises(BarStateConfigError, match="semantic definition"):
        load_bar_state_config(ROOT, config_path=semantic_drift)

    byte_drift = tmp_path / "bytes.toml"
    byte_drift.write_text(source + "\n", encoding="utf-8")
    with pytest.raises(BarStateConfigError, match="byte identity"):
        load_bar_state_config(ROOT, config_path=byte_drift)


def test_candidate_rejects_a_post_result_rename() -> None:
    template = load_bar_state_config(ROOT).candidates[0]

    with pytest.raises(BarStateConfigError, match="canonical dimensions"):
        BarStateCandidate(
            candidate_key="renamed_after_results",
            timeframe_seconds=template.timeframe_seconds,
            feature_set=template.feature_set,
            confidence_margin=template.confidence_margin,
        )
