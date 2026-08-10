from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest

from systematic_fx.backtest.bar_replay import BAR_EXECUTION_SCENARIOS
from systematic_fx.research.bar_config import (
    ALLOCATED_VARIANT_COUNT,
    BAR_PATTERN_CONFIG_RELATIVE_PATH,
    BAR_PATTERN_CONFIG_SEMANTIC_SHA256,
    CAMPAIGN_VARIANT_BUDGET,
    PATTERN_FAMILY_SPECS,
    UNALLOCATED_VARIANT_COUNT,
    BarPatternCandidate,
    BarPatternConfigError,
    load_bar_pattern_config,
)

ROOT = Path(__file__).resolve().parents[2]


def _contains_float(value: object) -> bool:
    if isinstance(value, float):
        return True
    if isinstance(value, dict):
        return any(_contains_float(key) or _contains_float(item) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return any(_contains_float(item) for item in value)
    return False


def test_frozen_config_allocates_exactly_216_stable_candidate_keys() -> None:
    config = load_bar_pattern_config(ROOT)

    assert config.semantic_sha256 == BAR_PATTERN_CONFIG_SEMANTIC_SHA256
    assert len(config.candidates) == ALLOCATED_VARIANT_COUNT == 216
    assert CAMPAIGN_VARIANT_BUDGET == 240
    assert UNALLOCATED_VARIANT_COUNT == 24
    assert config.candidates[0].candidate_key == "bpv1_tf0300_lb01_f1_long"
    assert config.candidates[-1].candidate_key == "bpv1_tf3600_lb12_f6_short"
    assert len({candidate.candidate_key for candidate in config.candidates}) == 216
    assert len({candidate.definition_sha256 for candidate in config.candidates}) == 216

    assert Counter(candidate.timeframe_seconds for candidate in config.candidates) == {
        300: 72,
        1_800: 72,
        3_600: 72,
    }
    assert Counter(candidate.setup_lookback_bars for candidate in config.candidates) == {
        1: 36,
        2: 36,
        3: 36,
        4: 36,
        6: 36,
        12: 36,
    }
    assert Counter(candidate.family.family_id for candidate in config.candidates) == {
        family.family_id: 36 for family in PATTERN_FAMILY_SPECS
    }
    assert Counter(candidate.direction.value for candidate in config.candidates) == {
        "LONG": 108,
        "SHORT": 108,
    }


def test_canonical_parameters_record_split_policy_gates_costs_and_data_paths() -> None:
    config = load_bar_pattern_config(ROOT)
    parameters = config.canonical_parameters()

    assert not _contains_float(parameters)
    assert parameters["barriers"]["axis_ticks"] == list(range(24, 193, 8))
    assert parameters["barriers"]["expected_cell_count"] == 484
    assert parameters["candidate_budget"] == {
        "allocated_variants": 216,
        "maximum_variants": 240,
        "result_driven_additions_allowed": False,
        "unallocated_variants": 24,
    }
    assert parameters["source"] == {
        "raw_root_relative": "data/mbp-10",
        "source_file_count": 1434,
        "source_first_date": "2022-01-02",
        "source_last_date": "2026-07-31",
        "source_manifest_relative": "data/derived/manifests/mbp10_source_sha256_v1.jsonl",
        "source_manifest_sha256": (
            "14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de"
        ),
    }
    assert len(parameters["families"]) == 6
    assert parameters["split_policy"]["minimum_eligible_active_days"] == 740
    assert parameters["split_policy"]["walk_forward_folds"] == 5
    assert parameters["split_policy"]["sealed_holdout_decision_days"] == 120
    assert parameters["split_policy"]["holdout_visibility"] == ("SEALED_UNTIL_FINALISTS_FROZEN")
    assert parameters["entry"] == {
        "decision_time_policy": "TRIGGER_BAR_END",
        "entry_index": "t+1",
        "entry_price_policy": "NEXT_SIGNAL_BAR_OPEN_PLUS_SCENARIO_ADVERSITY",
        "holding_limit_policy": "CONTRACT_QUALITY_OR_SPLIT_BOUNDARY",
        "normal_market_closure_policy": "CONTINUE_WHILE_SAME_CONTRACT_AND_QUALIFIED",
        "same_second_first_touch_policy": "STOP_FIRST",
        "setup_indices": "t-L..t-1",
        "signal_context_gap_policy": "RESET_AFTER_3600_SECONDS",
        "terminal_boundary_types": ["CONTRACT_CHANGE", "QUALITY_BREAK", "SPLIT_END"],
        "trigger_index": "t",
    }
    assert parameters["discovery_support_gates"]["300"] == {
        "maximum_median_signals_per_active_day": 10,
        "minimum_distinct_signal_days": 40,
        "minimum_raw_signals": 160,
        "minimum_signals_per_block": 25,
    }
    assert (
        parameters["discovery_economic_gates"]["moderate_calendar_month_net_pnl_must_be_positive"]
        is True
    )
    assert parameters["discovery_economic_gates"]["positive_block_gross_profit_definition"] == (
        "SUM_POSITIVE_GROSS_TRADE_PNL_BEFORE_COSTS"
    )
    assert parameters["discovery_economic_gates"]["profit_factor_zero_loss_policy"] == (
        "POSITIVE_GROSS_IS_POSITIVE_INFINITY_ZERO_GROSS_UNDEFINED"
    )
    assert parameters["walk_forward_gates"]["minimum_positive_folds"] == 4
    assert parameters["walk_forward_gates"]["minimum_positive_neighbor_cells"] == 7
    assert parameters["paths"]["bars_output_relative"].startswith("data/derived/")
    assert parameters["paths"]["checkpoint_output_relative"].startswith("data/derived/")
    assert parameters["paths"]["result_output_relative"].startswith("data/derived/")
    assert len(config.candidate_catalog_sha256) == 64
    assert len(config.definition_sha256) == 64


def test_config_execution_scenarios_match_the_replay_engine() -> None:
    config = load_bar_pattern_config(ROOT)

    for spec in config.execution_scenarios:
        engine = BAR_EXECUTION_SCENARIOS[spec.scenario_id]
        assert engine.entry_adverse_ticks == spec.entry_adverse_ticks
        assert engine.take_profit_trade_through_ticks == spec.take_profit_trade_through_ticks
        assert engine.stop_total_minimum_adverse_ticks == spec.stop_total_minimum_adverse_ticks
        assert engine.terminal_exit_adverse_ticks == spec.terminal_exit_adverse_ticks
        assert engine.variable_debit_ticks == spec.variable_debit_ticks
        assert engine.fixed_pool_multiplier.as_integer_ratio() == (
            spec.fixed_pool_multiplier_numerator,
            spec.fixed_pool_multiplier_denominator,
        )


def test_loader_rejects_any_semantic_config_drift(tmp_path: Path) -> None:
    drifted = tmp_path / "drifted.toml"
    source = (ROOT / BAR_PATTERN_CONFIG_RELATIVE_PATH).read_text(encoding="utf-8")
    drifted.write_text(source.replace("allocated_variants = 216", "allocated_variants = 215"))

    with pytest.raises(BarPatternConfigError, match="frozen semantic definition"):
        load_bar_pattern_config(ROOT, config_path=drifted)


def test_candidate_object_rejects_noncanonical_identity() -> None:
    template = load_bar_pattern_config(ROOT).candidates[0]

    with pytest.raises(BarPatternConfigError, match="canonical dimensions"):
        BarPatternCandidate(
            candidate_key="renamed_after_results",
            timeframe_seconds=template.timeframe_seconds,
            setup_lookback_bars=template.setup_lookback_bars,
            family=template.family,
            direction=template.direction,
        )
