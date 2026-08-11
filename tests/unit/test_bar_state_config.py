from __future__ import annotations

from collections import Counter
from dataclasses import replace
from pathlib import Path

import pytest

from systematic_fx.backtest.bar_replay import BAR_EXECUTION_SCENARIOS
from systematic_fx.research.bar_state_config import (
    BAR_STATE_CAMPAIGN_DEFINITION_SHA256,
    BAR_STATE_CAMPAIGN_KEY,
    BAR_STATE_CAMPAIGN_PROFILES,
    BAR_STATE_CANDIDATE_CATALOG_SHA256,
    BAR_STATE_CANDIDATE_COUNT,
    BAR_STATE_CONFIG_FILE_SHA256,
    BAR_STATE_CONFIG_RELATIVE_PATH,
    BAR_STATE_CONFIG_SEMANTIC_SHA256,
    BAR_STATE_ECONOMIC_MULTIPLIERS,
    BAR_STATE_EXECUTION_SCENARIOS,
    BAR_STATE_V2_PROFILE,
    BAR_STATE_V2A_CAMPAIGN_DEFINITION_SHA256,
    BAR_STATE_V2A_CAMPAIGN_KEY,
    BAR_STATE_V2A_CANDIDATE_CATALOG_SHA256,
    BAR_STATE_V2A_CONFIG_FILE_SHA256,
    BAR_STATE_V2A_CONFIG_RELATIVE_PATH,
    BAR_STATE_V2A_CONFIG_SEMANTIC_SHA256,
    BAR_STATE_V2A_PROFILE,
    MORPHOLOGY_FEATURE_IDS,
    STATE_FEATURE_IDS,
    BarStateCandidate,
    BarStateConfigError,
    bar_state_campaign_profile,
    frozen_bar_state_candidates,
    frozen_model_policy,
    load_bar_state_config,
    load_bar_state_v2a_config,
    require_bar_state_campaign_profile,
)
from systematic_fx.research.bar_state_features import BarStateFeatureSpec
from systematic_fx.research.hypotheses import canonical_sha256, load_toml_document

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

    assert config.profile is BAR_STATE_V2_PROFILE
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
    assert config.candidate_catalog_sha256 == BAR_STATE_CANDIDATE_CATALOG_SHA256
    assert config.definition_sha256 == BAR_STATE_CAMPAIGN_DEFINITION_SHA256


def test_v2a_loader_freezes_distinct_admin_identity_and_hashes() -> None:
    config = load_bar_state_v2a_config(ROOT)

    assert config.profile is BAR_STATE_V2A_PROFILE
    assert config.path == ROOT / BAR_STATE_V2A_CONFIG_RELATIVE_PATH
    assert config.sha256 == BAR_STATE_V2A_CONFIG_FILE_SHA256
    assert config.semantic_sha256 == BAR_STATE_V2A_CONFIG_SEMANTIC_SHA256
    assert config.candidate_catalog_sha256 == BAR_STATE_V2A_CANDIDATE_CATALOG_SHA256
    assert config.definition_sha256 == BAR_STATE_V2A_CAMPAIGN_DEFINITION_SHA256
    assert config.as_dict()["campaign_key"] == BAR_STATE_V2A_CAMPAIGN_KEY
    assert config.as_dict()["optimizer_cap_amendment"] == {
        "amendment_scope": "OPTIMIZER_MAX_ITER_CAP_ONLY",
        "predecessor_campaign_definition_sha256": (BAR_STATE_CAMPAIGN_DEFINITION_SHA256),
        "predecessor_campaign_key": BAR_STATE_CAMPAIGN_KEY,
        "predecessor_code_commit": "2ca2b0b6158c1d1e9d880c2ed65ec7d7582de189",
        "predecessor_gate_policy": ("REQUIRE_EXACT_FAILED_PREDECESSOR_WITH_NO_OOS_EVIDENCE"),
    }


def test_v2a_candidate_policy_diff_is_exactly_the_optimizer_cap() -> None:
    v2 = frozen_bar_state_candidates(profile=BAR_STATE_V2_PROFILE)
    v2a = frozen_bar_state_candidates(profile=BAR_STATE_V2A_PROFILE)

    assert tuple(item.candidate_key for item in v2a) == tuple(item.candidate_key for item in v2)
    for predecessor, successor in zip(v2, v2a, strict=True):
        predecessor_document = predecessor.as_dict()
        successor_document = successor.as_dict()
        predecessor_arguments = predecessor_document["model_policy"]["sklearn_arguments"]
        successor_arguments = successor_document["model_policy"]["sklearn_arguments"]
        assert predecessor_arguments.pop("max_iter") == 5_000
        assert successor_arguments.pop("max_iter") == 50_000
        assert successor_document == predecessor_document


def test_v2a_hashes_pin_model_policy_catalog_campaign_and_all_candidates() -> None:
    config = load_bar_state_v2a_config(ROOT)
    expected_candidates = {
        "bsv2_tf0300_fsmorphology_cm005": (
            "ef9d158d5909beaee7727aa5c71c99be2c44053399325c0438f508cfa0742eda"
        ),
        "bsv2_tf0300_fsmorphology_cm010": (
            "62fa347ad4d2824e3220df29834bf4bfedd58d9df16c161b95fbfd2ab36defb7"
        ),
        "bsv2_tf0300_fsmorphology_cm015": (
            "2620affd7d6bc99001b667d52173d90b24ee379134c6053ed27f6cad52ee4d6a"
        ),
        "bsv2_tf0300_fsstate_cm005": (
            "315c7ac44d828afe96f4a3ec2eb38e047fe7a2e7c9c268dabe01f557807383ac"
        ),
        "bsv2_tf0300_fsstate_cm010": (
            "6d8c80b71bccb9d25c69a173585c9dfe47a888a0fe5918240f0e95063d69035b"
        ),
        "bsv2_tf0300_fsstate_cm015": (
            "b8530e604700b64a8e39cee7e4c6719bfd1294c8f4c64e25345a731442301ec0"
        ),
        "bsv2_tf1800_fsmorphology_cm005": (
            "eb5404c6a507b05d243fdb1e81aa8ab9a93cb0a3bc958321b2a12a03600e44ee"
        ),
        "bsv2_tf1800_fsmorphology_cm010": (
            "375d9a388e1346b3557703beee061c408371683b1aa27c2d7b6fa8862ea298da"
        ),
        "bsv2_tf1800_fsmorphology_cm015": (
            "0367e3821e20fe2eb07ec278a3d3faff2bf90e15c8d1c2b1de241763ee5cf7d3"
        ),
        "bsv2_tf1800_fsstate_cm005": (
            "a98c2d8e60da3ffc8dbf84461d0873627dfbec47847891f23c44a6785685ae1e"
        ),
        "bsv2_tf1800_fsstate_cm010": (
            "57f4d5577456ff4ca3f30d82bb731b07c5638fa1b5f4a86b26d039d954bd19a3"
        ),
        "bsv2_tf1800_fsstate_cm015": (
            "696f5eac1caa452082cb51c0aef9c0f856daa96e31e89267b5d05f081242ef91"
        ),
    }

    assert canonical_sha256(frozen_model_policy(max_iter=50_000)) == (
        "844cd3964e2871fecd13b7f7a76f07016b150b853c290c4188e275cd2226874f"
    )
    assert {item.candidate_key: item.definition_sha256 for item in config.candidates} == (
        expected_candidates
    )
    assert config.candidate_catalog_sha256 == BAR_STATE_V2A_CANDIDATE_CATALOG_SHA256
    assert config.definition_sha256 == BAR_STATE_V2A_CAMPAIGN_DEFINITION_SHA256


def test_v2a_config_records_train_only_qualification_and_predecessor_gate() -> None:
    document = load_toml_document(ROOT / BAR_STATE_V2A_CONFIG_RELATIVE_PATH)
    qualification = document["optimizer_cap_amendment"]

    assert document["amendment_scope"] == "OPTIMIZER_MAX_ITER_CAP_ONLY"
    assert document["model"]["actual_sklearn_kwargs"]["max_iter"] == 50_000
    assert qualification["qualification_training_row_count"] == 26_735
    assert qualification["qualification_training_rows_sha256"] == (
        "d860672ce1f0496284596974d36f07f30897d9891751c2d8760e67328da6b3e0"
    )
    assert qualification["v2_n_iter"] == 5_000
    assert qualification["v2_convergence_warning"] is True
    assert qualification["diagnostic_n_iter"] == 25_000
    assert qualification["diagnostic_convergence_warning"] is True
    assert qualification["selected_n_iter"] == qualification["confirmation_n_iter"] == 33_542
    assert qualification["selected_convergence_warning"] is False
    assert qualification["confirmation_convergence_warning"] is False
    assert qualification["coefficient_sha256"] == (
        "22691a2e3a322cfaca78db45e01a44d63134f36d600ac73861d2ed6c8cf43a55"
    )
    assert qualification["intercept_sha256"] == (
        "dde5a31d4a64146b74f33d8b0cf3dde9a98945a1ae706c97c36f24adb9a96d99"
    )
    assert qualification["oos_economic_evidence_accessed"] is False
    assert qualification["sealed_walk_forward_accessed"] is False
    assert qualification["sealed_holdout_accessed"] is False


def test_campaign_profile_lookup_and_allowlist_reject_forged_profiles() -> None:
    assert BAR_STATE_CAMPAIGN_PROFILES == (BAR_STATE_V2_PROFILE, BAR_STATE_V2A_PROFILE)
    assert bar_state_campaign_profile() is BAR_STATE_V2_PROFILE
    assert bar_state_campaign_profile("V2A") is BAR_STATE_V2A_PROFILE
    assert require_bar_state_campaign_profile(BAR_STATE_V2A_PROFILE) is BAR_STATE_V2A_PROFILE
    assert BAR_STATE_V2A_PROFILE.artifact_type == BAR_STATE_V2A_CAMPAIGN_KEY
    assert BAR_STATE_V2A_PROFILE.experiment_key == (
        "bar_state_conditional_v2a:experiment:frozen_candidate_catalog:v1"
    )

    forged = replace(BAR_STATE_V2A_PROFILE, model_max_iter=50_001)
    with pytest.raises(BarStateConfigError, match="not exactly registered"):
        require_bar_state_campaign_profile(forged)
    with pytest.raises(BarStateConfigError, match="not exactly registered"):
        frozen_bar_state_candidates(profile=forged)
    with pytest.raises(BarStateConfigError, match="not exactly registered"):
        load_bar_state_config(ROOT, profile=forged)
    with pytest.raises(BarStateConfigError, match="unknown"):
        bar_state_campaign_profile("v2a")


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
