from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from systematic_fx.db.outcome_registry import (
    _EQUIVALENCE_COMPARISON_FIELDS,
    BARRIER_TICKS,
    CAMPAIGN_KEY,
    CHECKPOINT_ARTIFACT_SCHEMA,
    CHECKPOINT_ARTIFACT_TYPE,
    DIRECTION_IDS,
    EXPECTED_CELL_COUNT,
    EXPECTED_DETAIL_RECORD_COUNT,
    EXPECTED_DIRECTION_SIGNAL_COUNTS,
    EXPECTED_FINAL_SOURCE_DATE,
    EXPECTED_PLANNED_SOURCE_DATE_COUNT,
    EXPECTED_SOURCE_OCCURRENCE_COUNT,
    EXPECTED_SOURCE_SLICE_COUNT,
    EXPECTED_SUMMARY_COUNT,
    OUTCOME_CONFIG_ID,
    OUTCOME_ENGINE_VERSION,
    P1_05_EXPECTED_DETAIL_RECORD_COUNT,
    P1_05_EXPECTED_DIRECTION_SIGNAL_COUNTS,
    P1_05_EXPECTED_PLANNED_SOURCE_DATE_COUNT,
    P1_05_EXPECTED_SOURCE_OCCURRENCE_COUNT,
    P1_05_QUERY_ID,
    P4_01_EXPECTED_DIRECTION_SIGNAL_COUNTS,
    P4_01_EXPECTED_SOURCE_OCCURRENCE_COUNT,
    P4_01_QUERY_ID,
    P4_02_EXPECTED_DIRECTION_SIGNAL_COUNTS,
    P4_02_EXPECTED_SOURCE_OCCURRENCE_COUNT,
    P4_02_QUERY_ID,
    P4_PAIR_CONFIG_SHA256,
    P4_PAIR_ECONOMIC_CELL_COUNT,
    P4_PAIR_ID,
    P5_QUERY_ID,
    PHASE1A_CUMULATIVE_ECONOMIC_CELL_COUNT,
    SCENARIO_COST_TICKS_PER_FILL,
    SCENARIO_IDS,
    OutcomeCellSummary,
    OutcomePredecessorGate,
    OutcomeRegistryDatabaseError,
    OutcomeRegistryError,
    OutcomeReplayAuditCheckpoint,
    OutcomeReplayAuditSubject,
    OutcomeScreeningDecision,
    _open_held_immutable_file,
    _p4_pair_release_payload,
    _require_held_parent,
    _validate_checkpoint_artifact,
    _validate_equivalence_audit_artifact,
    _validate_source_artifact_document,
    _verify_held_file,
    complete_phase1a_outcome_replay,
    derive_phase1a_outcome_screening_decisions,
    load_latest_phase1a_outcome_checkpoint,
    outcome_query_profile,
    phase1a_outcome_parameters,
    phase1a_p5_outcome_parameters,
    reserve_phase1a_outcome_replay,
    validate_complete_cell_summaries,
)
from systematic_fx.research.run_spec import RUN_SPEC_SCHEMA, RUN_SPEC_SCHEMA_VERSION

ROOT = Path(__file__).resolve().parents[2]


def _cell(
    scenario: str,
    direction: str,
    take_profit_ticks: int,
    stop_loss_ticks: int,
    *,
    gross_pnl_ticks: int = 32,
) -> OutcomeCellSummary:
    signal_count = EXPECTED_DIRECTION_SIGNAL_COUNTS[direction]
    variable_cost_ticks, allocated_fixed_cost_ticks = SCENARIO_COST_TICKS_PER_FILL[scenario]
    net_ticks = gross_pnl_ticks - variable_cost_ticks - allocated_fixed_cost_ticks
    return OutcomeCellSummary(
        scenario_id=scenario,
        direction=direction,
        take_profit_ticks=take_profit_ticks,
        stop_loss_ticks=stop_loss_ticks,
        signal_count=signal_count,
        entry_fill_count=1,
        entry_not_filled_count=signal_count - 1,
        skipped_occupied_count=0,
        take_profit_first_count=1,
        stop_first_count=0,
        terminal_exit_count=0,
        censored_count=0,
        gross_pnl_ticks=gross_pnl_ticks,
        variable_cost_ticks=variable_cost_ticks,
        allocated_fixed_cost_ticks=allocated_fixed_cost_ticks,
        fully_loaded_net_pnl_ticks=net_ticks,
        fully_loaded_net_ev_ticks=Decimal(net_ticks),
        fully_loaded_net_pnl_usd=Decimal(net_ticks) * Decimal("6.25"),
        calendar_month_net_pnl_usd=Decimal(net_ticks) * Decimal("6.25"),
        profit_factor=None,
        maximum_drawdown_usd=Decimal(0),
        complete=True,
    )


def _complete_cells() -> tuple[OutcomeCellSummary, ...]:
    return tuple(
        _cell(scenario, direction, take_profit, stop_loss)
        for scenario in SCENARIO_IDS
        for direction in DIRECTION_IDS
        for take_profit in BARRIER_TICKS
        for stop_loss in BARRIER_TICKS
    )


def _publish_json(data_root: Path, subdirectory: str, document: dict[str, object]) -> Path:
    payload = (
        json.dumps(
            document,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )
    digest = hashlib.sha256(payload).hexdigest()
    target = data_root / "derived" / subdirectory / f"sha256={digest}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(payload)
    target.chmod(0o444)
    return target


def test_frozen_outcome_parameters_distinguish_shared_engine_and_p5_config() -> None:
    source_sha256 = "a" * 64
    parameters = phase1a_p5_outcome_parameters(source_sha256)

    assert OUTCOME_ENGINE_VERSION == "phase1a_shared_outcome_replay_v1"
    assert OUTCOME_CONFIG_ID == "phase1a_p5_outcome_replay_v1"
    assert parameters == {
        "cell_count_per_surface": 484,
        "direction_ids": ["LONG", "SHORT"],
        "expected_detail_record_count": 1613172,
        "expected_direction_signal_counts": {"LONG": 529, "SHORT": 582},
        "expected_summary_count": 2904,
        "final_source_date": "2023-08-31",
        "outcome_config_id": "phase1a_p5_outcome_replay_v1",
        "planned_source_date_count": 485,
        "query_id": "p5_01_range_expansion_flow_continuation",
        "scenario_ids": [
            "BASELINE",
            "MODERATE_COMBINED",
            "SEVERE_DIAGNOSTIC",
        ],
        "scenario_cost_ticks_per_fill": {
            "BASELINE": {"allocated_fixed": 4, "variable": 4},
            "MODERATE_COMBINED": {"allocated_fixed": 5, "variable": 5},
            "SEVERE_DIAGNOSTIC": {"allocated_fixed": 6, "variable": 6},
        },
        "source_artifact_manifest_sha256": source_sha256,
        "source_occurrence_count": 1111,
        "source_slice_count": 99,
        "stop_loss_ticks": list(BARRIER_TICKS),
        "take_profit_ticks": list(BARRIER_TICKS),
    }


def test_p1_profile_and_parameters_are_distinct_and_predecessor_bound() -> None:
    profile = outcome_query_profile(P1_05_QUERY_ID)
    gate = OutcomePredecessorGate(
        equivalence_audit_id=7,
        equivalence_audit_artifact_sha256="1" * 64,
        predecessor_outcome_replay_manifest_id=11,
        predecessor_run_fingerprint="2" * 64,
        predecessor_result_artifact_sha256="3" * 64,
        predecessor_input_lineage_sha256="4" * 64,
        predecessor_cell_summaries_sha256="5" * 64,
        predecessor_detail_shard_manifest_sha256="6" * 64,
        predecessor_final_checkpoint_sha256="7" * 64,
    )

    with pytest.raises(OutcomeRegistryError, match="predecessor gate"):
        phase1a_outcome_parameters("a" * 64, query_id=P1_05_QUERY_ID)
    parameters = phase1a_outcome_parameters(
        "a" * 64,
        query_id=P1_05_QUERY_ID,
        predecessor_gate=gate,
    )

    assert profile.source_occurrence_count == 943
    assert profile.direction_signal_counts == {"LONG": 446, "SHORT": 497}
    assert profile.planned_source_date_count == 478
    assert profile.detail_record_count == 1_369_236
    assert parameters["outcome_config_id"] == "phase1a_p1_05_outcome_replay_v1"
    assert parameters["predecessor_equivalence_audit_id"] == 7
    assert parameters["predecessor_result_artifact_sha256"] == "3" * 64


def test_p1_cell_surface_uses_its_own_frozen_direction_counts() -> None:
    p1_cells = tuple(
        replace(
            cell,
            signal_count=P1_05_EXPECTED_DIRECTION_SIGNAL_COUNTS[cell.direction],
            entry_not_filled_count=(P1_05_EXPECTED_DIRECTION_SIGNAL_COUNTS[cell.direction] - 1),
        )
        for cell in _complete_cells()
    )

    ordered, digest = validate_complete_cell_summaries(
        p1_cells,
        query_id=P1_05_QUERY_ID,
    )

    assert len(ordered) == 2_904
    assert len(digest) == 64
    with pytest.raises(OutcomeRegistryError, match="LONG cell signal_count"):
        validate_complete_cell_summaries(p1_cells)


@pytest.mark.parametrize(
    ("query_id", "signal_count", "direction_counts"),
    (
        (
            P4_01_QUERY_ID,
            P4_01_EXPECTED_SOURCE_OCCURRENCE_COUNT,
            P4_01_EXPECTED_DIRECTION_SIGNAL_COUNTS,
        ),
        (
            P4_02_QUERY_ID,
            P4_02_EXPECTED_SOURCE_OCCURRENCE_COUNT,
            P4_02_EXPECTED_DIRECTION_SIGNAL_COUNTS,
        ),
    ),
)
def test_p4_profiles_bind_the_exact_pair_and_cumulative_multiplicity(
    query_id: str,
    signal_count: int,
    direction_counts: dict[str, int],
) -> None:
    parameters = phase1a_outcome_parameters("a" * 64, query_id=query_id)

    assert parameters["pair_id"] == P4_PAIR_ID
    assert parameters["pair_config_sha256"] == P4_PAIR_CONFIG_SHA256
    assert parameters["paired_query_ids"] == [P4_01_QUERY_ID, P4_02_QUERY_ID]
    assert parameters["source_occurrence_count"] == signal_count
    assert parameters["expected_direction_signal_counts"] == direction_counts
    assert parameters["pair_economic_cell_count"] == P4_PAIR_ECONOMIC_CELL_COUNT
    assert parameters["cumulative_economic_cell_count"] == PHASE1A_CUMULATIVE_ECONOMIC_CELL_COUNT


def test_p4_release_payload_is_ordered_and_individual_completion_is_forbidden(
    tmp_path: Path,
) -> None:
    decisions = {
        P4_01_QUERY_ID: {"LONG": "1" * 64, "SHORT": "2" * 64},
        P4_02_QUERY_ID: {"LONG": "3" * 64, "SHORT": "4" * 64},
    }
    payload = _p4_pair_release_payload(
        p4_01_outcome_replay_manifest_id=11,
        p4_01_run_fingerprint="5" * 64,
        p4_01_result_artifact_sha256="6" * 64,
        p4_01_cell_summaries_sha256="7" * 64,
        p4_02_outcome_replay_manifest_id=12,
        p4_02_run_fingerprint="8" * 64,
        p4_02_result_artifact_sha256="9" * 64,
        p4_02_cell_summaries_sha256="a" * 64,
        decision_sha256s=decisions,
    )

    assert [member["query_id"] for member in payload["members"]] == [
        P4_01_QUERY_ID,
        P4_02_QUERY_ID,
    ]
    assert payload["expected_summary_count"] == 5_808
    assert payload["expected_detail_record_count"] == 978_648
    assert payload["decision_count"] == 4
    with pytest.raises(OutcomeRegistryError, match="atomic pair completion"):
        complete_phase1a_outcome_replay(
            "postgresql:///not-opened",
            outcome_replay_manifest_id=1,
            run_fingerprint="b" * 64,
            cell_summaries=(),
            result_artifact_path=tmp_path / "unused.json",
            data_root=tmp_path,
            query_id=P4_01_QUERY_ID,
        )


def test_p4_registry_boundary_rejects_signed_negative_zero_without_changing_p5() -> None:
    p5_cells = list(_complete_cells())
    p5_cells[0] = replace(p5_cells[0], maximum_drawdown_usd=Decimal("-0"))
    validate_complete_cell_summaries(tuple(p5_cells), query_id=P5_QUERY_ID)

    p4_cells = [
        replace(
            cell,
            signal_count=P4_01_EXPECTED_DIRECTION_SIGNAL_COUNTS[cell.direction],
            entry_not_filled_count=(P4_01_EXPECTED_DIRECTION_SIGNAL_COUNTS[cell.direction] - 1),
        )
        for cell in _complete_cells()
    ]
    p4_cells[0] = replace(p4_cells[0], maximum_drawdown_usd=Decimal("-0"))
    with pytest.raises(OutcomeRegistryError, match="signed negative zero"):
        validate_complete_cell_summaries(tuple(p4_cells), query_id=P4_01_QUERY_ID)


def test_audit_checkpoint_chain_identity_uses_progress_digest() -> None:
    checkpoint = OutcomeReplayAuditCheckpoint(
        checkpoint_sequence=1,
        checkpoint_artifact_sha256="a" * 64,
        checkpoint_artifact_byte_size=123,
        predecessor_checkpoint_sha256=None,
        last_completed_source_date=date(2022, 1, 3),
        source_event_count=456,
        progress_metadata={"replay_finished": False},
    )

    assert (
        checkpoint.identity_payload["progress_metadata_sha256"]
        == hashlib.sha256(b'{"replay_finished":false}').hexdigest()
    )
    assert "progress_metadata" not in checkpoint.identity_payload


def test_equivalence_audit_artifact_binds_every_subject_identity(tmp_path: Path) -> None:
    checkpoint = OutcomeReplayAuditCheckpoint(
        checkpoint_sequence=1,
        checkpoint_artifact_sha256="a" * 64,
        checkpoint_artifact_byte_size=123,
        predecessor_checkpoint_sha256=None,
        last_completed_source_date=date(2022, 1, 3),
        source_event_count=456,
        progress_metadata={"replay_finished": True},
    )
    subject = OutcomeReplayAuditSubject(
        outcome_replay_manifest_id=1,
        research_run_spec_id=2,
        research_run_attempt_id=3,
        run_fingerprint="b" * 64,
        status="SUCCEEDED",
        query_id=P5_QUERY_ID,
        source_artifact_manifest_sha256="c" * 64,
        result_artifact_id=4,
        result_artifact_path=tmp_path / "result.json",
        result_artifact_sha256="d" * 64,
        result_artifact_byte_size=789,
        cell_summaries_sha256="e" * 64,
        cache_manifest_sha256="f" * 64,
        detail_shard_manifest_sha256="1" * 64,
        input_lineage_sha256="2" * 64,
        final_checkpoint_sha256="a" * 64,
        final_checkpoint_sequence=EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        source_event_count=456,
        detail_record_count=1_613_172,
        summary_row_count=2_904,
        checkpoints=(checkpoint,) * EXPECTED_PLANNED_SOURCE_DATE_COUNT,
    )
    observed = {
        "cache_manifest_sha256": subject.cache_manifest_sha256,
        "cell_summaries_sha256": subject.cell_summaries_sha256,
        "checkpoint_chain_sha256": subject.checkpoint_chain_sha256,
        "checkpoint_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "checkpoint_load_count": 1,
        "checkpoint_publication_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "checkpoint_reused_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "complete_noop_count": 1,
        "detail_record_count": subject.detail_record_count,
        "detail_shard_manifest_sha256": subject.detail_shard_manifest_sha256,
        "detail_shard_publication_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "detail_shard_reused_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "final_checkpoint_sequence": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "final_checkpoint_sha256": subject.final_checkpoint_sha256,
        "final_result_disposition": "REUSED",
        "input_lineage_sha256": subject.input_lineage_sha256,
        "outcome_replay_manifest_id": 1,
        "result_artifact_byte_size": subject.result_artifact_byte_size,
        "result_artifact_sha256": subject.result_artifact_sha256,
        "run_fingerprint": subject.run_fingerprint,
        "source_event_count": subject.source_event_count,
        "start_noop_count": 1,
        "summary_row_count": subject.summary_row_count,
    }
    data_root = tmp_path / "data"
    (data_root / "derived").mkdir(parents=True)
    audit_path = _publish_json(
        data_root,
        "outcomes/audits/phase1a_p5_outcome_equivalence_v1",
        {
            "artifact_schema": "systematic_fx.phase1a_p5_outcome_equivalence_audit.v1",
            "audit_kind": "UNINTERRUPTED_VS_RESUMED_BYTE_EQUIVALENCE",
            "audit_version": "phase1a_p5_outcome_equivalence_v1",
            "comparisons": {key: True for key in _EQUIVALENCE_COMPARISON_FIELDS},
            "execution_policy": {
                "checkpoint_load": "FORCED_NONE",
                "control_plane_mutations": "NO_OP",
                "daily_artifact_policy": "MUST_REUSE_IDENTICAL_CONTENT",
                "replay_start": "FIRST_PLANNED_SOURCE_DATE",
            },
            "mismatches": [],
            "observed": observed,
            "passed": True,
            "query_id": P5_QUERY_ID,
            "subject": subject.subject_payload,
        },
    )
    held = _open_held_immutable_file(audit_path, data_root=data_root)
    try:
        document = _validate_equivalence_audit_artifact(held, subject=subject)
    finally:
        held.close()

    assert document["passed"] is True
    for section, field, forged_value in (
        ("observed", "checkpoint_load_count", 0),
        ("observed", "checkpoint_reused_count", 484),
        ("comparisons", "manifest_identity", False),
    ):
        forged = json.loads(audit_path.read_text(encoding="utf-8"))
        forged[section][field] = forged_value
        forged_path = _publish_json(
            data_root,
            f"outcomes/audits/forged-{section}-{field}",
            forged,
        )
        forged_held = _open_held_immutable_file(forged_path, data_root=data_root)
        try:
            with pytest.raises(OutcomeRegistryError):
                _validate_equivalence_audit_artifact(forged_held, subject=subject)
        finally:
            forged_held.close()


def test_held_artifact_rejects_ancestor_directory_swap(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    (data_root / "derived").mkdir(parents=True)
    path = _publish_json(data_root, "outcomes/swap-target", {"value": 1})
    held = _open_held_immutable_file(path, data_root=data_root)
    original_parent = path.parent
    moved_parent = original_parent.with_name("swap-held-original")
    try:
        original_parent.rename(moved_parent)
        original_parent.mkdir()
        replacement = original_parent / path.name
        replacement.write_bytes(held.content)
        replacement.chmod(0o444)

        with pytest.raises(OutcomeRegistryError, match="parent|path"):
            _verify_held_file(held)
    finally:
        held.close()


def test_held_artifact_rejects_cross_query_namespace(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    (data_root / "derived").mkdir(parents=True)
    path = _publish_json(
        data_root,
        "outcomes/checkpoints/phase1a_p5_outcome_replay_v1",
        {"value": 1},
    )
    held = _open_held_immutable_file(path, data_root=data_root)
    try:
        with pytest.raises(OutcomeRegistryError, match="frozen artifact directory"):
            _require_held_parent(
                held,
                data_root=data_root,
                expected_parent=outcome_query_profile(P1_05_QUERY_ID).checkpoint_directory,
                label="p1 checkpoint",
            )
    finally:
        held.close()


def test_screening_decision_requires_canonical_rejection_reasons() -> None:
    decision = OutcomeScreeningDecision(
        direction="SHORT",
        decision_label="SCREENING_REJECT",
        selected_take_profit_ticks=None,
        selected_stop_loss_ticks=None,
        positive_region_size=0,
        rejection_reasons=("BASELINE_PROFIT_FACTOR_BELOW_GATE",),
    )

    assert decision.payload(outcome_replay_manifest_id=1)["direction"] == "SHORT"
    with pytest.raises(OutcomeRegistryError, match="unique non-empty"):
        OutcomeScreeningDecision(
            direction="LONG",
            decision_label="SCREENING_REJECT",
            selected_take_profit_ticks=None,
            selected_stop_loss_ticks=None,
            positive_region_size=0,
            rejection_reasons=("Z", "Z"),
        )


def test_complete_surface_derives_both_frozen_screening_decisions() -> None:
    decisions = derive_phase1a_outcome_screening_decisions(_complete_cells())

    assert tuple(decision.direction for decision in decisions) == ("LONG", "SHORT")
    assert all(decision.decision_label == "SCREENING_SURVIVOR" for decision in decisions)
    assert all(decision.selected_take_profit_ticks is not None for decision in decisions)


def test_complete_cell_surface_is_canonical_and_rejects_missing_or_drifted_rows() -> None:
    cells = _complete_cells()
    ordered, first_sha256 = validate_complete_cell_summaries(tuple(reversed(cells)))
    second_ordered, second_sha256 = validate_complete_cell_summaries(cells)

    assert len(ordered) == EXPECTED_SUMMARY_COUNT
    assert ordered == second_ordered
    assert first_sha256 == second_sha256
    assert ordered[0].identity == ("BASELINE", "LONG", 24, 24)
    assert ordered[-1].identity == ("SEVERE_DIAGNOSTIC", "SHORT", 192, 192)

    with pytest.raises(OutcomeRegistryError, match="exactly 2904"):
        validate_complete_cell_summaries(cells[:-1])
    with pytest.raises(OutcomeRegistryError, match="duplicate cell"):
        validate_complete_cell_summaries(cells[:-1] + (cells[0],))
    with pytest.raises(OutcomeRegistryError, match="signal accounting"):
        replace(
            _cell("BASELINE", "LONG", 24, 24),
            signal_count=2,
        )
    wrong_direction_count = replace(
        cells[0],
        signal_count=530,
        entry_not_filled_count=cells[0].entry_not_filled_count + 1,
    )
    with pytest.raises(OutcomeRegistryError, match="LONG cell signal_count"):
        validate_complete_cell_summaries((wrong_direction_count,) + cells[1:])
    wrong_cost = replace(
        cells[0],
        variable_cost_ticks=cells[0].variable_cost_ticks + 1,
        fully_loaded_net_pnl_ticks=cells[0].fully_loaded_net_pnl_ticks - 1,
    )
    with pytest.raises(OutcomeRegistryError, match="variable cost"):
        validate_complete_cell_summaries((wrong_cost,) + cells[1:])


def test_cell_payload_round_trip_matches_economics_contract() -> None:
    original = _cell("MODERATE_COMBINED", "SHORT", 96, 144)
    payload = {"cell_id": "tp96_sl144", **original.payload}

    rebuilt = OutcomeCellSummary.from_mapping(payload)

    assert rebuilt == original
    assert rebuilt.summary_sha256 == original.summary_sha256
    with pytest.raises(OutcomeRegistryError, match="canonical decimal string"):
        OutcomeCellSummary.from_mapping({**payload, "fully_loaded_net_pnl_usd": 125.0})


def test_checkpoint_artifact_is_canonical_read_only_and_bound() -> None:
    fingerprint = "b" * 64
    metadata = {"checkpoint_kind": "SOURCE_DATE_COMPLETE"}
    metadata_sha256 = hashlib.sha256(
        json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()

    with tempfile.TemporaryDirectory() as directory_name:
        data_root = Path(directory_name).resolve() / "data"
        (data_root / "derived").mkdir(parents=True)
        checkpoint_path = _publish_json(
            data_root,
            "checkpoints",
            {
                "artifact_schema": CHECKPOINT_ARTIFACT_SCHEMA,
                "checkpoint_sequence": 1,
                "completed_source_date_count": 1,
                "last_completed_source_date": "2022-01-03",
                "outcome_config_id": OUTCOME_CONFIG_ID,
                "outcome_replay_manifest_id": 17,
                "predecessor_checkpoint_sha256": None,
                "progress_metadata_sha256": metadata_sha256,
                "query_id": P5_QUERY_ID,
                "run_fingerprint": fingerprint,
                "source_event_count": 123,
            },
        )

        checkpoint = _open_held_immutable_file(checkpoint_path, data_root=data_root)
        try:
            _validate_checkpoint_artifact(
                checkpoint,
                outcome_replay_manifest_id=17,
                run_fingerprint=fingerprint,
                checkpoint_sequence=1,
                completed_source_date_count=1,
                last_completed_source_date=date(2022, 1, 3),
                source_event_count=123,
                predecessor_checkpoint_sha256=None,
                progress_metadata_sha256=metadata_sha256,
            )
        finally:
            checkpoint.close()

        checkpoint_path.chmod(0o644)
        with pytest.raises(OutcomeRegistryError, match="read-only"):
            _open_held_immutable_file(checkpoint_path, data_root=data_root)


def test_latest_checkpoint_loader_is_read_only_and_returns_verified_exact_state() -> None:
    source_sha256 = "c" * 64
    canonical_spec = {
        "parameters": phase1a_p5_outcome_parameters(source_sha256),
    }
    fingerprint = hashlib.sha256(
        json.dumps(canonical_spec, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    manifest_id = 17
    progress_metadata = {
        "checkpoint_kind": "SOURCE_DATE_COMPLETE",
        "next_source_date_index": 1,
    }
    progress_sha256 = hashlib.sha256(
        json.dumps(progress_metadata, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()

    with tempfile.TemporaryDirectory() as directory_name:
        data_root = Path(directory_name).resolve() / "data"
        (data_root / "derived").mkdir(parents=True)
        checkpoint_document = {
            "artifact_schema": CHECKPOINT_ARTIFACT_SCHEMA,
            "checkpoint_sequence": 1,
            "completed_source_date_count": 1,
            "last_completed_source_date": "2022-01-03",
            "outcome_config_id": OUTCOME_CONFIG_ID,
            "outcome_replay_manifest_id": manifest_id,
            "predecessor_checkpoint_sha256": None,
            "progress_metadata_sha256": progress_sha256,
            "query_id": P5_QUERY_ID,
            "replay_state": {"next_source_date_index": 1},
            "run_fingerprint": fingerprint,
            "source_event_count": 123,
        }
        checkpoint_path = _publish_json(
            data_root,
            "outcomes/checkpoints/phase1a_p5_outcome_replay_v1",
            checkpoint_document,
        )
        checkpoint_sha256 = checkpoint_path.name.removeprefix("sha256=").removesuffix(".json")
        checkpoint_byte_size = checkpoint_path.stat().st_size
        manifest_row = {
            "attempt_number": 1,
            "attempt_status": "RUNNING",
            "barrier_axis_size": len(BARRIER_TICKS),
            "campaign_id": 5,
            "campaign_key": CAMPAIGN_KEY,
            "canonical_spec": canonical_spec,
            "canonicalization_schema": RUN_SPEC_SCHEMA,
            "canonicalization_version": RUN_SPEC_SCHEMA_VERSION,
            "cell_count_per_surface": EXPECTED_CELL_COUNT,
            "direction": "BOTH",
            "direction_count": len(DIRECTION_IDS),
            "engine_version": OUTCOME_ENGINE_VERSION,
            "expected_detail_record_count": EXPECTED_DETAIL_RECORD_COUNT,
            "expected_summary_count": EXPECTED_SUMMARY_COUNT,
            "experiment_id": None,
            "outcome_replay_manifest_id": manifest_id,
            "pattern_key": P5_QUERY_ID,
            "planned_source_date_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
            "final_source_date": EXPECTED_FINAL_SOURCE_DATE,
            "research_run_attempt_id": 31,
            "research_run_spec_id": 29,
            "run_fingerprint": fingerprint,
            "run_kind": "OUTCOME_BUILD",
            "scenario_count": len(SCENARIO_IDS),
            "source_artifact_manifest_sha256": source_sha256,
            "source_occurrence_count": EXPECTED_SOURCE_OCCURRENCE_COUNT,
            "source_slice_count": EXPECTED_SOURCE_SLICE_COUNT,
            "status": "RUNNING",
        }
        checkpoint_row = {
            "artifact_byte_size": checkpoint_byte_size,
            "artifact_key": (
                f"{CAMPAIGN_KEY}:outcome-checkpoint:{fingerprint}:manifest-{manifest_id}:000001"
            ),
            "artifact_metadata": {
                "campaign_key": CAMPAIGN_KEY,
                "checkpoint_sequence": 1,
                "last_completed_source_date": "2022-01-03",
                "outcome_config_id": OUTCOME_CONFIG_ID,
                "outcome_replay_manifest_id": manifest_id,
                "query_id": P5_QUERY_ID,
                "run_fingerprint": fingerprint,
            },
            "artifact_sha256": checkpoint_sha256,
            "artifact_type": CHECKPOINT_ARTIFACT_TYPE,
            "artifact_uri": checkpoint_path.as_uri(),
            "checkpoint_artifact_byte_size": checkpoint_byte_size,
            "checkpoint_artifact_id": 43,
            "checkpoint_artifact_sha256": checkpoint_sha256,
            "checkpoint_sequence": 1,
            "completed_source_date_count": 1,
            "last_completed_source_date": date(2022, 1, 3),
            "media_type": "application/json",
            "outcome_replay_manifest_id": manifest_id,
            "predecessor_checkpoint_sha256": None,
            "producer_job_id": None,
            "progress_metadata": progress_metadata,
            "run_fingerprint": fingerprint,
            "source_event_count": 123,
            "stored_artifact_id": 43,
        }
        manifest_result = MagicMock()
        manifest_result.fetchone.return_value = manifest_row
        checkpoint_result = MagicMock()
        checkpoint_result.fetchall.return_value = [checkpoint_row]
        connection = MagicMock()
        connection.execute.side_effect = [manifest_result, checkpoint_result]
        connect_result = MagicMock()
        connect_result.__enter__.return_value = connection

        with patch(
            "systematic_fx.db.outcome_registry.psycopg.connect",
            return_value=connect_result,
        ):
            loaded = load_latest_phase1a_outcome_checkpoint(
                "postgresql://test/outcome",
                outcome_replay_manifest_id=manifest_id,
                run_fingerprint=fingerprint,
                data_root=data_root,
            )

        assert loaded is not None
        assert loaded.checkpoint_sequence == 1
        assert loaded.checkpoint_artifact_uri == checkpoint_path.as_uri()
        assert loaded.checkpoint_artifact_path == checkpoint_path
        assert loaded.progress_metadata == progress_metadata
        assert loaded.progress_metadata_sha256 == progress_sha256
        assert loaded.checkpoint_document == checkpoint_document
        assert connection.read_only is True
        assert connection.isolation_level is psycopg.IsolationLevel.SERIALIZABLE
        assert connection.execute.call_count == 2
        assert all(
            call.args[0].lstrip().startswith("SELECT") for call in connection.execute.call_args_list
        )


def test_source_artifact_uses_the_canonical_discovery_definition_id() -> None:
    document = {
        "artifact_schema": "systematic_fx.phase1a_discovery_slice.v1",
        "requested_source_dates": [
            "2022-01-03",
            "2022-01-04",
            "2022-01-05",
            "2022-01-06",
            "2022-01-07",
        ],
        "run_fingerprint": "f" * 64,
        "query_results": [
            {
                "definition": {"id": P5_QUERY_ID},
                "direction_counts": {"LONG": 1, "SHORT": 0},
                "occurrences": [
                    {
                        "direction": "LONG",
                        "source_date": "2022-01-03",
                    }
                ],
                "support_count": 1,
            }
        ],
    }

    dates, count = _validate_source_artifact_document(
        document,
        run_fingerprint="f" * 64,
        slice_index=0,
    )

    assert dates[0] == "2022-01-03"
    assert count == 1
    document["query_results"][0]["definition"] = {"query_id": P5_QUERY_ID}
    with pytest.raises(OutcomeRegistryError, match="canonical p5 query"):
        _validate_source_artifact_document(
            document,
            run_fingerprint="f" * 64,
            slice_index=0,
        )


def test_serialization_failures_retry_the_whole_reservation_then_translate() -> None:
    failure = psycopg.errors.SerializationFailure("concurrent reservation")
    connect = MagicMock(side_effect=failure)
    with (
        patch("systematic_fx.db.outcome_registry.psycopg.connect", connect),
        patch("systematic_fx.db.postgres_retry.time.sleep"),
        pytest.raises(OutcomeRegistryDatabaseError) as raised,
    ):
        reserve_phase1a_outcome_replay(
            "postgresql://test/outcome",
            run_fingerprint="d" * 64,
            source_artifact_manifest_sha256="e" * 64,
        )

    assert connect.call_count == 4
    assert raised.value.__cause__ is failure


def test_migration_freezes_append_only_checkpoint_chain_and_complete_surface() -> None:
    sql = (ROOT / "migrations/0013_phase1a_outcome_replay.sql").read_text(encoding="utf-8")
    hardening_sql = (ROOT / "migrations/0014_phase1a_outcome_completion_hardening.sql").read_text(
        encoding="utf-8"
    )
    validation_sql = (ROOT / "migrations/0015_phase1a_outcome_constraints_validated.sql").read_text(
        encoding="utf-8"
    )
    ordered_sql = (ROOT / "migrations/0016_phase1a_ordered_outcome_candidates.sql").read_text(
        encoding="utf-8"
    )
    routing_sql = (ROOT / "migrations/0017_phase1a_ordered_trigger_routing.sql").read_text(
        encoding="utf-8"
    )
    decision_sql = (ROOT / "migrations/0018_phase1a_outcome_decision_atomicity.sql").read_text(
        encoding="utf-8"
    )
    lineage_sql = (ROOT / "migrations/0019_phase1a_outcome_audit_lineage_hardening.sql").read_text(
        encoding="utf-8"
    )

    assert "phase1a_outcome_replay_manifests" in sql
    assert "phase1a_outcome_replay_checkpoints" in sql
    assert "phase1a_outcome_cell_summaries" in sql
    assert "PRIMARY KEY (outcome_replay_manifest_id, checkpoint_sequence)" in sql
    assert "Phase 1A outcome replay checkpoints are append-only" in sql
    assert "last_completed_source_date <= previous_source_date" in sql
    assert "NEW.predecessor_checkpoint_sha256 <> previous_artifact_sha256" in sql
    assert "observed_summary_count <> NEW.expected_summary_count" in sql
    assert "expected_summary_count = 2904" in sql
    assert "phase1a_shared_outcome_replay_v1" in sql
    assert "phase1a_p5_outcome_replay_v1" in sql
    assert "p5_01_range_expansion_flow_continuation" in sql
    assert "phase1a_outcome_cells_frozen_signal_count" in hardening_sql
    assert "phase1a_outcome_cells_frozen_cost_accounting" in hardening_sql
    assert "expected_detail_record_count = 1613172" in hardening_sql
    assert "planned_source_date_count = 485" in hardening_sql
    assert "final_source_date = DATE '2023-08-31'" in hardening_sql
    assert "requires all 485 source-date checkpoints" in hardening_sql
    assert "replay_finished" in hardening_sql
    assert "VALIDATE CONSTRAINT phase1a_outcome_cells_frozen_signal_count" in (validation_sql)
    assert "VALIDATE CONSTRAINT phase1a_outcome_cells_frozen_cost_accounting" in (validation_sql)
    assert "p1_05_unconfirmed_move_reversal" in ordered_sql
    assert "source_occurrence_count = 943" in ordered_sql
    assert "expected_detail_record_count = 1369236" in ordered_sql
    assert "planned_source_date_count = 478" in ordered_sql
    assert "phase1a_outcome_replay_equivalence_audits" in ordered_sql
    assert "phase1a_outcome_screening_decisions" in ordered_sql
    assert "predecessor_equivalence_audit_id" in ordered_sql
    assert "phase1a_p5_outcome_manifests_preserve_and_validate" in routing_sql
    assert "phase1a_p1_05_outcome_manifests_preserve_and_validate" in routing_sql
    assert "successful Phase 1A outcome replay requires atomic" in decision_sql
    assert "phase1a_outcome_equivalence_one_per_subject" in lineage_sql
    assert "CREATE OR REPLACE FUNCTION systematic_fx.protect_phase1a_outcome_manifest" in (
        lineage_sql
    )
    assert "CREATE OR REPLACE FUNCTION systematic_fx.protect_phase1a_outcome_checkpoint" in (
        lineage_sql
    )
    assert "predecessor_detail_shard_manifest_sha256" in lineage_sql
    assert "IS DISTINCT FROM" in lineage_sql
    assert "<>" not in lineage_sql
    assert "CREATE TRIGGER" not in lineage_sql


def test_p4_migration_freezes_atomic_pair_release_and_semantic_hashes() -> None:
    sql = (ROOT / "migrations/0028_phase1a_p4_paired_outcomes.sql").read_text(encoding="utf-8")

    assert "phase1a_p4_outcome_pair_batches" in sql
    assert "phase1a_p4_outcome_pair_releases" in sql
    assert "WHERE status IN ('PREPARED', 'RELEASED')" in sql
    assert "new Phase 1A P4 pair batches must begin PREPARED" in sql
    assert "the singleton Phase 1A P4 pair is already released" in sql
    assert "phase1a_outcome_cell_summary_payload" in sql
    assert "outcome cell scenario-cost accounting drift" in sql
    assert "outcome cell decimal metrics must be finite" in sql
    assert "outcome cell summary SHA-256 drift" in sql
    assert "screening decision reasons must be unique nonblank strings" in sql
    assert "screening decision SHA-256 drift" in sql
    assert "ordered cell-summary aggregate SHA-256 drift" in sql
    assert "canonical_jsonb_text(expected_payload)" in sql
    assert "canonical_jsonb_sha256(expected_payload)" in sql
    assert "DEFERRABLE INITIALLY DEFERRED" in sql
    assert "P4 cell summaries cannot commit outside atomic pair release" in sql
    assert "P4 result artifacts cannot commit outside atomic pair release" in sql
    assert "VALUES (28, 'phase1a_p4_paired_outcomes', :'migration_checksum')" in sql


@pytest.mark.parametrize(
    "field_name",
    (
        "predecessor_equivalence_audit_id",
        "predecessor_equivalence_audit_artifact_sha256",
        "predecessor_outcome_replay_manifest_id",
        "predecessor_run_fingerprint",
        "predecessor_result_artifact_sha256",
        "predecessor_input_lineage_sha256",
        "predecessor_cell_summaries_sha256",
        "predecessor_detail_shard_manifest_sha256",
        "predecessor_final_checkpoint_sha256",
    ),
)
def test_migration_rejects_every_missing_p1_predecessor_key(field_name: str) -> None:
    sql = (ROOT / "migrations/0019_phase1a_outcome_audit_lineage_hardening.sql").read_text(
        encoding="utf-8"
    )

    assert f"{{{field_name}}}' IS NULL" in sql or (
        f"{{{field_name}}}'\n" in sql and "IS NULL" in sql
    )
    assert f"{{{field_name}}}' IS DISTINCT FROM" in sql or (
        f"{{{field_name}}}'\n" in sql and "IS DISTINCT FROM" in sql
    )


def test_constants_describe_three_two_by_484_surfaces() -> None:
    assert len(SCENARIO_IDS) == 3
    assert len(DIRECTION_IDS) == 2
    assert len(BARRIER_TICKS) == 22
    assert len(BARRIER_TICKS) ** 2 == EXPECTED_CELL_COUNT
    assert 3 * 2 * EXPECTED_CELL_COUNT == EXPECTED_SUMMARY_COUNT
    assert EXPECTED_SOURCE_SLICE_COUNT == 99
    assert EXPECTED_SOURCE_OCCURRENCE_COUNT == 1111
    assert EXPECTED_DIRECTION_SIGNAL_COUNTS == {"LONG": 529, "SHORT": 582}
    assert EXPECTED_PLANNED_SOURCE_DATE_COUNT == 485
    assert EXPECTED_FINAL_SOURCE_DATE == date(2023, 8, 31)
    assert EXPECTED_DETAIL_RECORD_COUNT == 1_613_172
    assert P1_05_EXPECTED_SOURCE_OCCURRENCE_COUNT == 943
    assert P1_05_EXPECTED_DIRECTION_SIGNAL_COUNTS == {"LONG": 446, "SHORT": 497}
    assert P1_05_EXPECTED_PLANNED_SOURCE_DATE_COUNT == 478
    assert P1_05_EXPECTED_DETAIL_RECORD_COUNT == 1_369_236
    assert os.path.basename("/data/derived") == "derived"
