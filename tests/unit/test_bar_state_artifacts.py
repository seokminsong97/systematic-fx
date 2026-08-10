from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pytest

from systematic_fx.research.bar_artifacts import verify_published_bar_artifact
from systematic_fx.research.bar_state_artifacts import (
    BAR_STATE_ARTIFACT_ROOT,
    BAR_STATE_ARTIFACT_SCHEMA_BY_KIND,
    BAR_STATE_SPLIT_PLAN_SHA256,
    BarStateArtifactError,
    BarStateArtifactLineage,
    BarStateDiscoveryScope,
    bar_state_global_result_projection,
    bar_state_model_package_projection,
    bar_state_price_policy_from_selection,
    bar_state_terminal_compact_summary,
    frozen_bar_state_discovery_scope,
    load_verified_bar_state_json,
    load_verified_bar_state_parquet,
    ordered_parent_artifacts,
    publish_bar_state_json,
    publish_bar_state_parquet,
    validate_bar_state_global_bootstrap,
)
from systematic_fx.research.hypotheses import canonical_json_bytes


def _lineage(**overrides: object) -> BarStateArtifactLineage:
    values: dict[str, object] = {
        "config_file_sha256": "1" * 64,
        "config_semantic_sha256": "2" * 64,
        "candidate_catalog_sha256": "3" * 64,
        "training_plan_sha256": "4" * 64,
        "code_snapshot_sha256": "5" * 64,
        "dependency_lock_sha256": "6" * 64,
        "runtime_environment_sha256": "7" * 64,
        "ordered_run_set_sha256": "8" * 64,
        "discovery_scope": frozen_bar_state_discovery_scope(),
    }
    values.update(overrides)
    return BarStateArtifactLineage(**values)  # type: ignore[arg-type]


def _terminal_document() -> dict[str, object]:
    candidate_key = "bsv2_tf0300_fsmorphology_cm005"
    selection = {
        "bootstrap_lower_bound_ev_ticks": None,
        "candidate_key": candidate_key,
        "final_label": "REJECTED",
        "maximum_drawdown_ticks": 0,
        "moderate_ev_ticks": None,
        "positive_component_size": 0,
        "positive_inner_fold_count": 0,
        "rejection_reasons": ["TEST_REJECT"],
        "selected_stop_loss_index": None,
        "selected_stop_loss_multiplier": None,
        "selected_take_profit_index": None,
        "selected_take_profit_multiplier": None,
        "worst_fold_moderate_ev_ticks": None,
    }
    price_policy = bar_state_price_policy_from_selection(selection)
    compact = {
        "candidate_key": candidate_key,
        "discovery_final_fit_model_sha256": None,
        "final_label": "REJECTED",
        "positive_component_size": 0,
        "price_policy": price_policy,
        "rejection_reasons": ["TEST_REJECT"],
        "selected_stop_loss_index": None,
        "selected_take_profit_index": None,
    }
    support = {
        "candidate_key": candidate_key,
        "distinct_signal_day_count": 0,
        "raw_directional_signal_count": 0,
        "raw_signal_count_by_fold": [
            {"fold_key": f"discovery_inner_{fold}", "signal_count": 0} for fold in (1, 2, 3)
        ],
        "timeframe_seconds": 300,
    }
    multiplicity_cells = [
        {
            "adjusted_p_value": {"denominator": 1, "numerator": 1},
            "bh_rejected": False,
            "bootstrap_lower_bound_ev_ticks": None,
            "candidate_key": candidate_key,
            "deterministic_gate_passed": False,
            "raw_p_value": {"denominator": 1, "numerator": 1},
            "rejection_reasons": ["TEST_REJECT"],
            "stop_loss_index": stop_loss_index,
            "take_profit_index": take_profit_index,
        }
        for take_profit_index in range(7)
        for stop_loss_index in range(7)
    ]
    return {
        "candidate_key": candidate_key,
        "compact_summary": compact,
        "decision_label": "DISCOVERY_REJECT",
        "result": {
            "candidate_selection": selection,
            "candidate_support": support,
            "discovery_final_fit_model": None,
            "multiplicity_cells": multiplicity_cells,
            "price_policy": price_policy,
            "schema": "systematic_fx.bar_state_candidate_result.v1",
        },
        "schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["TERMINAL_RESULT"],
        "trial_status": "REJECTED",
    }


def test_parquet_publication_is_campaign_scoped_content_addressed_and_idempotent(
    tmp_path: Path,
) -> None:
    table = pa.table(
        {
            "decision_ns": pa.array([1, 2], type=pa.int64()),
            "feature_ticks": pa.array([3, 4], type=pa.int64()),
        }
    )
    first = publish_bar_state_parquet(
        tmp_path,
        kind="FEATURE",
        artifact_key_suffix="tf0300_shard0001",
        table=table,
        lineage=_lineage(),
        logical_identity={"shard_ordinal": 1, "timeframe_seconds": 300},
    )
    second = publish_bar_state_parquet(
        tmp_path,
        kind="FEATURE",
        artifact_key_suffix="tf0300_shard0001",
        table=table,
        lineage=_lineage(),
        logical_identity={"shard_ordinal": 1, "timeframe_seconds": 300},
    )

    assert first == second
    assert first.path.is_relative_to(tmp_path / BAR_STATE_ARTIFACT_ROOT)
    assert first.descriptor.logical_identity["artifact_kind"] == "FEATURE"
    assert first.descriptor.record_count == 2
    verify_published_bar_artifact(tmp_path, first)
    assert load_verified_bar_state_parquet(tmp_path, first).equals(table)
    assert load_verified_bar_state_parquet(tmp_path, first, columns=("decision_ns",)).equals(
        table.select(("decision_ns",))
    )


def test_json_model_requires_canonical_non_executable_document(tmp_path: Path) -> None:
    document = {
        "candidate_key": "bsv2_tf0300_fsmorphology_cm005",
        "coefficients": ["0.125", "-0.25"],
        "schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["MODEL"],
    }
    artifact = publish_bar_state_json(
        tmp_path,
        kind="MODEL",
        artifact_key_suffix="tf0300_fsmorphology_cm005",
        document=document,
        record_count=1,
        lineage=_lineage(
            candidate_key="bsv2_tf0300_fsmorphology_cm005",
            candidate_definition_sha256="9" * 64,
            run_fingerprint="a" * 64,
        ),
        logical_identity={"candidate_key": "bsv2_tf0300_fsmorphology_cm005"},
    )

    assert artifact.path.suffix == ".json"
    assert artifact.path.read_bytes().startswith(b'{"candidate_key"')
    assert load_verified_bar_state_json(tmp_path, artifact) == document
    with pytest.raises(BarStateArtifactError, match="document schema"):
        publish_bar_state_json(
            tmp_path,
            kind="MODEL",
            artifact_key_suffix="wrong_schema",
            document={"schema": "systematic_fx.wrong.v1"},
            record_count=1,
            lineage=_lineage(),
            logical_identity={},
        )


def test_terminal_compact_summary_is_exactly_projected_from_immutable_result() -> None:
    document = _terminal_document()
    assert bar_state_terminal_compact_summary(document) == document["compact_summary"]

    forged = {
        **document,
        "result": {
            **document["result"],  # type: ignore[dict-item]
            "candidate_selection": {
                **document["result"]["candidate_selection"],  # type: ignore[index]
                "positive_component_size": 9,
            },
        },
    }
    with pytest.raises(BarStateArtifactError, match="differs from its result projection"):
        bar_state_terminal_compact_summary(forged)

    arbitrary_price = {
        **document,
        "compact_summary": {**document["compact_summary"], "price_policy": {}},  # type: ignore[dict-item]
        "result": {**document["result"], "price_policy": {}},  # type: ignore[dict-item]
    }
    with pytest.raises(BarStateArtifactError, match="price policy differs"):
        bar_state_terminal_compact_summary(arbitrary_price)

    missing_support = json.loads(json.dumps(document))
    missing_support["result"]["candidate_support"] = {}
    with pytest.raises(BarStateArtifactError, match="candidate support.*key set"):
        bar_state_terminal_compact_summary(missing_support)

    missing_multiplicity = json.loads(json.dumps(document))
    missing_multiplicity["result"]["multiplicity_cells"] = []
    with pytest.raises(BarStateArtifactError, match="exactly 49"):
        bar_state_terminal_compact_summary(missing_multiplicity)

    selected = dict(document["result"]["candidate_selection"])  # type: ignore[index]
    selected.update(
        {
            "final_label": "FINALIST",
            "positive_component_size": 9,
            "rejection_reasons": [],
            "selected_stop_loss_index": 0,
            "selected_stop_loss_multiplier": {"denominator": 4, "numerator": 3},
            "selected_take_profit_index": 0,
            "selected_take_profit_multiplier": {"denominator": 2, "numerator": 1},
        }
    )
    with pytest.raises(BarStateArtifactError, match="differs from its frozen 7-axis index"):
        bar_state_price_policy_from_selection(selected)

    capped_reject = dict(document["result"]["candidate_selection"])  # type: ignore[index]
    capped_reject.update(
        {
            "positive_component_size": 9,
            "rejection_reasons": ["MAXIMUM_FINALIST_LIMIT"],
            "selected_stop_loss_index": 4,
            "selected_stop_loss_multiplier": {"denominator": 1, "numerator": 2},
            "selected_take_profit_index": 3,
            "selected_take_profit_multiplier": {"denominator": 2, "numerator": 3},
        }
    )
    capped_policy = bar_state_price_policy_from_selection(capped_reject)
    assert capped_policy["selected_stop_loss_multiplier"] == {
        "denominator": 1,
        "numerator": 2,
    }


def test_model_package_projection_requires_exact_three_inner_and_optional_one_final() -> None:
    from tests.integration.test_bar_state_registry_postgres import (
        _gate_model_package_document,
    )

    candidate_key = "bsv2_tf0300_fsmorphology_cm005"
    document, binding = _gate_model_package_document(candidate_key, selected=True)
    projection = bar_state_model_package_projection(
        document,
        expected_candidate_key=candidate_key,
        expected_binding=binding,
    )
    assert projection.record_count == 4
    assert projection.projection["inner_model_count"] == 3
    assert projection.projection["final_fit_model_count"] == 1
    assert projection.projection["wrapper_count"] == 4

    missing = dict(document)
    del missing["fold_models"]
    with pytest.raises(BarStateArtifactError, match="schema or identity"):
        bar_state_model_package_projection(
            missing,
            expected_candidate_key=candidate_key,
            expected_binding=binding,
        )

    two_wrappers = json.loads(json.dumps(document))
    two_wrappers["fold_models"] = two_wrappers["fold_models"][:2]
    two_wrappers["fold_model_count"] = 2
    with pytest.raises(BarStateArtifactError, match="final-fit cardinality"):
        bar_state_model_package_projection(
            two_wrappers,
            expected_candidate_key=candidate_key,
            expected_binding=binding,
        )

    invalid_body = json.loads(json.dumps(document))
    invalid_body["fold_models"][0]["model"]["training_row_count"] = 4
    with pytest.raises(BarStateArtifactError, match="strict decoding|identity/content"):
        bar_state_model_package_projection(
            invalid_body,
            expected_candidate_key=candidate_key,
            expected_binding=binding,
        )


def test_global_projection_rejects_empty_duplicate_and_inconsistent_evidence() -> None:
    from systematic_fx.research.bar_state_run import load_prepared_bar_state_run
    from tests.integration.test_bar_state_registry_postgres import (
        _gate_full_global_document,
    )

    prepared = load_prepared_bar_state_run(Path.cwd())
    candidate_keys = prepared.candidate_keys
    valid = _gate_full_global_document(candidate_keys)
    projection = bar_state_global_result_projection(valid)
    assert (
        validate_bar_state_global_bootstrap(valid, split_plan=prepared.split_plan)
        == projection.bootstrap_validation_sha256
    )
    assert projection.evidence_projection["bh_family_size"] == 804
    assert projection.evidence_projection["cell_summary_count"] == 1764
    assert projection.evidence_projection["multiplicity_result_count"] == 588
    assert (
        sum(projection.candidate_oos_trade_record_count_by_key.values())
        == valid["discovery_result"]["portfolio_executed_trade_record_count"]
    )
    assert len(projection.candidate_evidence_slice_sha256_by_key) == 12
    assert (
        projection.evidence_projection["bootstrap_evaluation_calendar_sha256"]
        == "0f00faa36d08feebec1fce003268823ff02aa52b9817a84edbfcc8f863a324f1"
    )

    sunday_daily_evidence = json.loads(json.dumps(valid))
    sunday_cell = next(
        cell
        for cell in sunday_daily_evidence["discovery_result"]["cell_summaries"]
        if cell["scenario_id"] == "MODERATE_COMBINED"
        and cell["cell_id"] == "tpm3_2_slm3_2"
        and cell["entry_fill_count"] == 40
    )
    sunday_cell["daily_net_pnl_ticks"][0]["active_date"] = "2022-05-22"
    sunday_cell["daily_fill_count"][0]["active_date"] = "2022-05-22"
    with pytest.raises(BarStateArtifactError, match="exact fold blocks"):
        bar_state_global_result_projection(sunday_daily_evidence)

    forged_one_day_bootstrap = json.loads(json.dumps(valid))
    one_day_cell = next(
        cell
        for cell in forged_one_day_bootstrap["discovery_result"]["cell_summaries"]
        if cell["scenario_id"] == "MODERATE_COMBINED"
        and cell["cell_id"] == "tpm3_2_slm3_2"
        and cell["entry_fill_count"] == 40
    )
    one_day_cell["daily_net_pnl_ticks"] = [
        {
            "active_date": "2022-05-23",
            "net_pnl_ticks": one_day_cell["fully_loaded_net_pnl_ticks"],
        }
    ]
    one_day_cell["daily_fill_count"] = [
        {"active_date": "2022-05-23", "fill_count": one_day_cell["entry_fill_count"]}
    ]
    with pytest.raises(BarStateArtifactError, match="exact fold blocks"):
        bar_state_global_result_projection(forged_one_day_bootstrap)

    forged_three_day_bootstrap = json.loads(json.dumps(valid))
    three_day_cell = next(
        cell
        for cell in forged_three_day_bootstrap["discovery_result"]["cell_summaries"]
        if cell["scenario_id"] == "MODERATE_COMBINED"
        and cell["cell_id"] == "tpm3_2_slm3_2"
        and cell["entry_fill_count"] == 40
    )
    three_day_folds = forged_three_day_bootstrap["discovery_result"][
        "bootstrap_evaluation_calendar"
    ]["folds"]
    three_day_cell["daily_net_pnl_ticks"] = [
        {
            "active_date": fold["active_dates"][0],
            "net_pnl_ticks": net_ticks,
        }
        for fold, net_ticks in zip(three_day_folds, (560, 520, 520), strict=True)
    ]
    three_day_cell["daily_fill_count"] = [
        {"active_date": fold["active_dates"][0], "fill_count": fill_count}
        for fold, fill_count in zip(three_day_folds, (14, 13, 13), strict=True)
    ]
    with pytest.raises(BarStateArtifactError, match="frozen PCG64 replay"):
        bar_state_global_result_projection(forged_three_day_bootstrap)

    forged_fold_allocation = json.loads(json.dumps(valid))
    fold_cell = next(
        cell
        for cell in forged_fold_allocation["discovery_result"]["cell_summaries"]
        if cell["scenario_id"] == "MODERATE_COMBINED"
        and cell["cell_id"] == "tpm3_2_slm3_2"
        and cell["entry_fill_count"] == 40
    )
    first_fold_dates = forged_fold_allocation["discovery_result"]["bootstrap_evaluation_calendar"][
        "folds"
    ][0]["active_dates"]
    forged_dates = [first_fold_dates[index * len(first_fold_dates) // 40] for index in range(40)]
    fold_cell["daily_net_pnl_ticks"] = [
        {"active_date": active_date, "net_pnl_ticks": 40} for active_date in forged_dates
    ]
    fold_cell["daily_fill_count"] = [
        {"active_date": active_date, "fill_count": 1} for active_date in forged_dates
    ]
    with pytest.raises(BarStateArtifactError, match="exact fold blocks"):
        bar_state_global_result_projection(forged_fold_allocation)

    mutations: list[dict[str, object]] = []
    empty_support = json.loads(json.dumps(valid))
    empty_support["discovery_result"]["candidate_support"] = []
    mutations.append(empty_support)
    wrong_bh = json.loads(json.dumps(valid))
    wrong_bh["discovery_result"]["bh_family_size"] = 588
    mutations.append(wrong_bh)
    wrong_bootstrap = json.loads(json.dumps(valid))
    wrong_bootstrap["discovery_result"]["bootstrap_convention"] = {}
    mutations.append(wrong_bootstrap)
    duplicate_cell = json.loads(json.dumps(valid))
    duplicate_cell["discovery_result"]["cell_summaries"][1] = duplicate_cell["discovery_result"][
        "cell_summaries"
    ][0]
    mutations.append(duplicate_cell)
    bad_signal_total = json.loads(json.dumps(valid))
    bad_signal_total["discovery_result"]["signal_count"] += 1
    mutations.append(bad_signal_total)
    bad_multiplicity = json.loads(json.dumps(valid))
    bad_multiplicity["discovery_result"]["multiplicity_results"][0]["adjusted_p_value"] = {
        "denominator": 1,
        "numerator": 0,
    }
    mutations.append(bad_multiplicity)
    impossible_memory = json.loads(json.dumps(valid))
    impossible_memory["discovery_result"]["memory_plan"]["maximum_resident_one_second_rows"] = 1
    mutations.append(impossible_memory)
    wrong_fixed_cost = json.loads(json.dumps(valid))
    selected_cell = next(
        cell
        for cell in wrong_fixed_cost["discovery_result"]["cell_summaries"]
        if cell["entry_fill_count"] > 0
    )
    selected_cell["allocated_fixed_cost_ticks"] += selected_cell["entry_fill_count"]
    mutations.append(wrong_fixed_cost)
    forged_gate = json.loads(json.dumps(valid))
    rejected_cell = next(
        cell
        for cell in forged_gate["discovery_result"]["multiplicity_results"]
        if not cell["deterministic_gate_passed"] and cell["bootstrap_lower_bound_ev_ticks"] is None
    )
    rejected_cell["rejection_reasons"] = ["BASELINE_NET_EV"]
    mutations.append(forged_gate)
    forged_cell_rank = json.loads(json.dumps(valid))
    forged_cell_rank["discovery_result"]["candidate_results"][0]["selected_take_profit_index"] = 2
    forged_cell_rank["discovery_result"]["candidate_results"][0][
        "selected_take_profit_multiplier"
    ] = {"denominator": 1, "numerator": 1}
    mutations.append(forged_cell_rank)
    impossible_bootstrap_p = json.loads(json.dumps(valid))
    passed_cell = next(
        cell
        for cell in impossible_bootstrap_p["discovery_result"]["multiplicity_results"]
        if cell["deterministic_gate_passed"]
    )
    passed_cell["raw_p_value"] = {"denominator": 1_000_000, "numerator": 1}
    mutations.append(impossible_bootstrap_p)

    for mutation in mutations:
        with pytest.raises(BarStateArtifactError):
            bar_state_global_result_projection(mutation)


def test_code_snapshot_preserves_the_exact_provenance_preimage(tmp_path: Path) -> None:
    document = {
        "artifact_schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["CODE_SNAPSHOT"],
        "code_commit": "a" * 40,
        "file_count": 0,
        "files": [],
    }
    expected = hashlib.sha256(canonical_json_bytes(document)).hexdigest()
    artifact = publish_bar_state_json(
        tmp_path,
        kind="CODE_SNAPSHOT",
        artifact_key_suffix=expected,
        document=document,
        record_count=0,
        lineage=_lineage(code_snapshot_sha256=expected),
        logical_identity={
            "code_commit": "a" * 40,
            "code_snapshot_sha256": expected,
        },
    )

    assert artifact.sha256 == expected
    assert load_verified_bar_state_json(tmp_path, artifact) == document


def test_discovery_scope_rejects_walk_forward_or_outcome_overreach() -> None:
    values = {
        "split_plan_sha256": BAR_STATE_SPLIT_PLAN_SHA256,
        "split_key": "walk_forward_1",
        "result_visibility": "SEALED",
        "start_date": "2023-08-03",
        "decision_end_date": "2024-01-09",
        "outcome_end_date": "2024-02-01",
        "start_active_ordinal": 490,
        "decision_end_active_ordinal": 622,
        "outcome_end_active_ordinal": 642,
    }
    with pytest.raises(BarStateArtifactError, match="Discovery scope"):
        BarStateDiscoveryScope(**values)


def test_candidate_lineage_fields_are_all_or_none_and_parents_are_sorted(
    tmp_path: Path,
) -> None:
    with pytest.raises(BarStateArtifactError, match="must be supplied together"):
        _lineage(candidate_key="bsv2_tf0300_fsmorphology_cm005")

    document = {
        "schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["REGISTRATION"],
    }
    second = publish_bar_state_json(
        tmp_path,
        kind="REGISTRATION",
        artifact_key_suffix="second",
        document=document,
        record_count=12,
        lineage=_lineage(),
        logical_identity={"order": 2},
    )
    first = publish_bar_state_json(
        tmp_path,
        kind="REGISTRATION",
        artifact_key_suffix="first",
        document=document,
        record_count=12,
        lineage=_lineage(),
        logical_identity={"order": 1},
    )

    parents = ordered_parent_artifacts((second, first))
    assert tuple(item.artifact_key for item in parents) == tuple(
        sorted((first.descriptor.artifact_key, second.descriptor.artifact_key))
    )
