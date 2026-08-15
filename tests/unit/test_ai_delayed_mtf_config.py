from __future__ import annotations

import json
import tomllib
from pathlib import Path

import pytest

from scripts.ai_delayed_mtf_config import (
    AI_DELAYED_MTF_CONFIG_RELATIVE_PATH,
    AIDelayedMTFConfigError,
    delayed_mtf_implementation_sha256,
    expected_ai_delayed_mtf_contract,
    load_ai_delayed_mtf_config,
    render_ai_delayed_mtf_toml_template,
)
from scripts.ai_delayed_mtf_engine import (
    CANDIDATE_CATALOG_SHA256,
    delayed_mtf_engine_contract,
)
from systematic_fx.research.hypotheses import canonical_sha256


def test_template_freezes_exact_symbolic_100_and_stage_access_barriers() -> None:
    document = tomllib.loads(render_ai_delayed_mtf_toml_template())
    expected = expected_ai_delayed_mtf_contract()

    for key, value in expected.items():
        assert document[key] == value
    assert document["catalog"] == {
        "candidate_catalog_sha256": CANDIDATE_CATALOG_SHA256,
        "candidate_count": 100,
        "compression_breakout_count": 24,
        "delayed_macd_count": 36,
        "family_count": 100,
        "global_multiplicity_family_count": 100,
        "meta_label_count": 0,
        "pullback_continuation_count": 24,
        "range_mean_reversion_count": 16,
        "stage_subset_order": "PRESERVE_CATALOG_SEMANTIC_SELECTION_RANK_ORDER",
    }
    lifecycle = document["lifecycle"]
    assert lifecycle["search_masks_before_search_one_second_loader"]
    assert lifecycle["walk_forward_masks_before_any_walk_forward_one_second_loader"]
    assert lifecycle["walk_forward_feature_open_barrier"] == ("SEARCH_FROZEN_SELECTION_MAXIMUM_8")
    assert lifecycle["holdout_feature_open_barrier"] == (
        "WF_MAXIMUM_3_FINALISTS_THEN_ONE_SHOT_AUTHORIZATION"
    )
    assert "WALK_FORWARD_SKIPPED" in lifecycle["stage_order_no_search_finalists"]
    assert lifecycle["failed_branch"].endswith("FAILED_TERMINAL")
    assert document["scope"]["search_claim"].startswith("RETROSPECTIVE_")
    assert document["scope"]["walk_forward_claim"].startswith("FIRST_OOS_")
    assert document["code_commit"].startswith("PENDING_")


def test_template_binds_full_new_engine_cost_null_warmup_contract() -> None:
    document = tomllib.loads(render_ai_delayed_mtf_toml_template())
    embedded = json.loads(document["engine"]["contract_canonical_json"])

    assert embedded == delayed_mtf_engine_contract()
    assert document["engine"]["contract_sha256"] == canonical_sha256(embedded)
    assert document["warmup"]["engine_contract_sha256"] == canonical_sha256(embedded)
    assert document["engine"]["existing_holdout_engine_reuse"] == (
        "OLD_EXECUTION_AND_SIGNAL_LOGIC_PROHIBITED_NEUTRAL_DATA_CONTAINERS_ONLY"
    )
    assert document["engine"]["allowed_legacy_container_symbols"] == [
        "BarWithOutcomeSpan",
        "SignalMask",
    ]
    assert document["engine"]["fixed_horizon_primary_seconds"] == 10_800
    assert document["engine"]["fixed_take_profit_or_stop_loss"] is False
    assert document["nulls"] == {
        "master_seed": "ai-delayed-mtf-v1",
        "seed_type": "EXACT_UTF8_STRING",
    }
    assert embedded["nulls"]["master_seed"] == "ai-delayed-mtf-v1"


def test_continuity_and_calendar_identity_are_explicit() -> None:
    document = tomllib.loads(render_ai_delayed_mtf_toml_template())

    assert document["continuity"] == {
        "active_calendar_sha256": (
            "b414eae72afdb1c149977ff0ea5b672069380997d91e74adf0407e35836e8ac1"
        ),
        "cross_date_carry_policy": (
            "SAME_CONTRACT_OUTCOME_SPAN_ADJACENT_ALLOWLISTED_ACTIVE_DATE_GAP_LE_96H"
        ),
        "execution_continuity": "EXACT_SIGNAL_SEGMENT_ONLY",
        "indicator_policy": "INDICATOR_CONTINUITY_V1",
        "intra_date_gap_policy": "RESET",
        "maximum_cross_date_wall_gap_hours": 96,
        "official_schedule_evidence": False,
        "segment_contract_or_outcome_span_change_policy": "RESET",
        "stage_or_fold_start_policy": "RESET",
        "weekend_carry_allowed_when_predicates_hold": True,
    }
    assert document["dataset"]["active_calendar_payload"] == ("BARE_ORDERED_ISO_DATE_STRING_LIST")
    assert document["dataset"]["timeframes_seconds"] == [1, 300, 1800, 3600]


def test_loader_rejects_unfilled_data_only_template(tmp_path: Path) -> None:
    path = tmp_path / AI_DELAYED_MTF_CONFIG_RELATIVE_PATH
    path.parent.mkdir(parents=True)
    path.write_text(render_ai_delayed_mtf_toml_template(), encoding="utf-8")

    with pytest.raises(AIDelayedMTFConfigError, match="code_commit"):
        load_ai_delayed_mtf_config(tmp_path)


def test_implementation_identity_covers_all_source_scripts_and_project_bytes(
    tmp_path: Path,
) -> None:
    (tmp_path / "src/package").mkdir(parents=True)
    (tmp_path / "scripts/nested").mkdir(parents=True)
    (tmp_path / "src/package/a.py").write_text("A = 1\n", encoding="utf-8")
    (tmp_path / "scripts/b.py").write_text("B = 2\n", encoding="utf-8")
    (tmp_path / "scripts/nested/c.py").write_text("C = 3\n", encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    (tmp_path / "uv.lock").write_text("version = 1\n", encoding="utf-8")

    original = delayed_mtf_implementation_sha256(tmp_path)
    (tmp_path / "scripts/nested/c.py").write_text("C = 4\n", encoding="utf-8")
    changed_script = delayed_mtf_implementation_sha256(tmp_path)
    (tmp_path / "scripts/nested/c.py").write_text("C = 3\n", encoding="utf-8")
    (tmp_path / "src/package/a.py").write_text("A = 9\n", encoding="utf-8")
    changed_source = delayed_mtf_implementation_sha256(tmp_path)
    (tmp_path / "src/package/a.py").write_text("A = 1\n", encoding="utf-8")
    (tmp_path / "uv.lock").write_text("version = 2\n", encoding="utf-8")
    changed_lock = delayed_mtf_implementation_sha256(tmp_path)

    assert len({original, changed_script, changed_source, changed_lock}) == 4
