from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from scripts.ai_pattern_holdout_config import (
    AI_PATTERN_HOLDOUT_AUTHORITY,
    AI_PATTERN_HOLDOUT_CONFIG_RELATIVE_PATH,
    FINAL_HOLDOUT_STATUSES,
    AIPatternHoldoutConfigError,
    expected_ai_pattern_holdout_contract,
    load_ai_pattern_holdout_config,
    render_ai_pattern_holdout_toml_template,
)
from scripts.ai_pattern_holdout_engine import MATCHED_RELAXATION_LEVELS


def test_toml_template_has_exact_contract_and_deliberately_invalid_provenance() -> None:
    document = tomllib.loads(render_ai_pattern_holdout_toml_template())
    expected = expected_ai_pattern_holdout_contract()

    for key, value in expected.items():
        assert document[key] == value
    assert document["code_commit"].startswith("PENDING_")
    assert document["evaluator_implementation_sha256"].startswith("PENDING_")
    assert document["dependency_lock_sha256"].startswith("PENDING_")
    assert document["precommitted_at_utc"].startswith("PENDING_")


def test_contract_binds_execution_nulls_gates_and_bounded_authority() -> None:
    contract = expected_ai_pattern_holdout_contract()

    assert contract["authority"] == AI_PATTERN_HOLDOUT_AUTHORITY
    assert contract["execution"] == {
        "allocated_fixed_cost_ticks": 5,
        "entry_adverse_ticks": 2,
        "entry_policy": ("NEXT_CONTIGUOUS_SAME_CONTRACT_SIGNAL_SEGMENT_OUTCOME_SPAN_5M_OPEN"),
        "fully_loaded_round_trip_cost_ticks": 10,
        "holding_horizon_seconds": 3600,
        "maximum_concurrent_positions_per_pattern": 1,
        "maximum_drawdown_basis": (
            "CHRONOLOGICAL_SEQUENTIAL_FULLY_LOADED_TRADE_NET_TICKS_FROM_ZERO"
        ),
        "path_timeframe_seconds": 1,
        "profit_target_ticks": 32,
        "profit_target_trade_through_ticks": 1,
        "scenario_id": "MODERATE_COMBINED",
        "signal_timeframe_seconds": 300,
        "stop_loss_minimum_adverse_ticks": 4,
        "stop_loss_ticks": 24,
        "terminal_adverse_ticks": 2,
        "terminal_policy": "LAST_OBSERVED_1S_CLOSE_BY_HORIZON_OR_SPAN_END",
        "tie_policy": "SAME_SECOND_STOP_FIRST",
        "variable_cost_ticks": 5,
    }
    assert contract["nulls"]["shift_scope"] == [
        "SOURCE_DATE",
        "CONTRACT",
        "OUTCOME_SPAN",
        "SIGNAL_SEGMENT_ID",
    ]
    assert contract["nulls"]["matched_missing_history_policy"] == (
        "SEPARATE_CAUSAL_MISSING_PRIOR_20_HISTORY_STRATUM"
    )
    assert contract["nulls"]["matched_fallback_order"] == [
        ("SAME_DATE_CONTRACT_OUTCOME_SPAN_SIGNAL_SEGMENT_30M_UTC_BUCKET_CAUSAL_STRATUM"),
        (
            "SAME_DATE_CONTRACT_OUTCOME_SPAN_SIGNAL_SEGMENT_"
            "ADJACENT_PLUS_MINUS_1_30M_UTC_BUCKET_RETAIN_CAUSAL_STRATUM"
        ),
        ("SAME_DATE_CONTRACT_OUTCOME_SPAN_SIGNAL_SEGMENT_DROP_BUCKET_AND_CAUSAL_STRATUM"),
        "SAMPLE_INELIGIBLE",
    ]
    assert tuple(contract["nulls"]["matched_fallback_order"][:-1]) == (MATCHED_RELAXATION_LEVELS)
    assert contract["search_gates"]["maximum_finalists"] == 4
    assert contract["walk_forward_gates"]["maximum_finalists"] == 3
    assert contract["holdout_gates"]["holm_one_sided_alpha_denominator"] == 20
    assert contract["status"]["physical_holdout_isolation"] is False
    assert contract["status"]["paper_live_or_promotion_authority"] is False
    assert FINAL_HOLDOUT_STATUSES[:2] == (
        "NO_SEARCH_FINALISTS_HOLDOUT_NOT_OPENED",
        "NO_WALK_FORWARD_FINALISTS_HOLDOUT_NOT_OPENED",
    )


def test_loader_rejects_pending_template_before_git_or_data_access(tmp_path: Path) -> None:
    path = tmp_path / AI_PATTERN_HOLDOUT_CONFIG_RELATIVE_PATH
    path.parent.mkdir(parents=True)
    path.write_text(render_ai_pattern_holdout_toml_template(), encoding="utf-8")

    with pytest.raises(AIPatternHoldoutConfigError, match="code_commit"):
        load_ai_pattern_holdout_config(tmp_path)


def test_loader_rejects_symlinked_config(tmp_path: Path) -> None:
    target = tmp_path / "target.toml"
    target.write_text(render_ai_pattern_holdout_toml_template(), encoding="utf-8")
    path = tmp_path / AI_PATTERN_HOLDOUT_CONFIG_RELATIVE_PATH
    path.parent.mkdir(parents=True)
    path.symlink_to(target)

    with pytest.raises(AIPatternHoldoutConfigError, match="symbolic"):
        load_ai_pattern_holdout_config(tmp_path)
