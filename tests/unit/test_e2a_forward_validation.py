from __future__ import annotations

import hashlib
import json
import stat
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from campaigns.e2a_month_end_v1 import forward as forward_module
from campaigns.e2a_month_end_v1.config import frozen_config
from campaigns.e2a_month_end_v1.engine import E2AReproductionError
from campaigns.e2a_month_end_v1.forward import (
    FORWARD_AUTHORITY_SCOPE,
    FORWARD_LIFECYCLE_STATUS,
    FORWARD_OBSERVATION_SCHEMA,
    FORWARD_PLAN_ARTIFACT_TYPE,
    OBSERVE_UNAVAILABLE_CODE,
    E2AForwardError,
    E2AForwardPlan,
    E2AForwardUnavailable,
    E2AShadowObservation,
    build_e2a_forward_plan,
    observe_shadow_e2a_forward,
    precommit_e2a_forward,
    publish_forward_artifact,
    status_e2a_forward,
    verify_e2a_forward,
)
from scripts.run_e2a_forward_validation import build_parser, main
from systematic_fx.research.hypotheses import canonical_json_bytes


@pytest.fixture(autouse=True)
def _fixed_precommit_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        forward_module,
        "_now_utc",
        lambda: datetime(2026, 8, 20, 12, 0, tzinfo=UTC),
    )


def _artifact_path(state_root: Path, status: object) -> Path:
    identity = status.plan_artifact
    return state_root / identity.relative_uri


def _event_path(state_root: Path) -> Path:
    return state_root / "ledger/events/event-00000001.json"


def _contains_binary_float(value: object) -> bool:
    if isinstance(value, float):
        return True
    if isinstance(value, dict):
        return any(_contains_binary_float(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_binary_float(item) for item in value)
    return False


def test_plan_is_permanently_shadow_only_and_has_exact_dst_schedule() -> None:
    plan = build_e2a_forward_plan()
    document = plan.as_dict()

    assert canonical_json_bytes(document) == plan.canonical_bytes
    assert not _contains_binary_float(document)
    assert document["authority"] == {
        "allowed_execution_mode": "OFFLINE_SHADOW_ONLY",
        "lifecycle_status": "PLANNED_NOT_ARMABLE",
        "live_market_data_adapter": "ABSENT",
        "paper_broker_adapter": "ABSENT",
        "paper_order_authority": False,
        "scheduler_backend": "ABSENT",
    }
    opportunities = document["forward_window"]["opportunities"]
    assert [(row["candidate_date"], row["decision_utc"]) for row in opportunities] == [
        ("2026-08-31", "2026-08-31T14:00:00Z"),
        ("2026-09-30", "2026-09-30T14:00:00Z"),
        ("2026-10-30", "2026-10-30T15:00:00Z"),
        ("2026-11-30", "2026-11-30T15:00:00Z"),
        ("2026-12-31", "2026-12-31T15:00:00Z"),
        ("2027-01-29", "2027-01-29T15:00:00Z"),
        ("2027-02-26", "2027-02-26T15:00:00Z"),
        ("2027-03-31", "2027-03-31T14:00:00Z"),
        ("2027-04-30", "2027-04-30T14:00:00Z"),
        ("2027-05-31", "2027-05-31T14:00:00Z"),
        ("2027-06-30", "2027-06-30T14:00:00Z"),
        ("2027-07-30", "2027-07-30T14:00:00Z"),
    ]
    assert all(
        row["eligibility_status"] == "PROVISIONAL_NOT_ELIGIBILITY_DECISION" for row in opportunities
    )


def test_plan_retains_unresolved_blockers_decisions_and_frozen_rule() -> None:
    document = build_e2a_forward_plan().as_dict()
    config = frozen_config()

    decisions = document["user_decisions_required"]
    assert len(decisions) == 11
    assert all(
        decision["blocking"]
        and decision["resolution"] is None
        and decision["status"] == "USER_DECISION_REQUIRED"
        for decision in decisions
    )
    blocker_keys = {blocker["blocker_key"] for blocker in document["implementation_blockers"]}
    assert {
        "NO_FORWARD_SOURCE_OBSERVER_IMPLEMENTATION",
        "NO_LIVE_MARKET_DATA_ADAPTER",
        "NO_PAPER_BROKER_ORDER_OR_RECONCILIATION_ADAPTER",
        "NO_SCHEDULER_OR_SERVICE_STATE",
        "NO_EXTERNAL_TIMESTAMP_OR_COMMITTED_LEDGER_TAIL",
        "NO_GOVERNED_E2A_BOOK_RESET_AND_RECOVERY_POLICY",
        "NO_VERIFIED_ACTUAL_FEE_SCHEDULE",
    } <= blocker_keys
    rule = document["frozen_rule"]
    assert rule["time_anchor"] == "15:00:00_EUROPE_LONDON_DST_AWARE"
    assert rule["entry"]["decision_offset_seconds"] == 1
    assert rule["entry"]["wait_cap_seconds"] == 3
    assert rule["exit"]["holding_seconds"] == 86_400
    assert rule["position"]["take_profit"] is None
    assert rule["position"]["stop_loss"] is None
    assert document["gates"]["minimum_event_count"] == 12
    assert document["gates"]["minimum_win_count"] == 7
    assert document["gates"]["net_ticks_e6_alternative_strictly_greater_than"] == (120_000_000)
    assert document["gates"]["gate_expression"] == (
        "NET_AT_MEASURED_COST_GT_0 AND "
        "(WINS_GTE_7_OF_12 OR NET_TICKS_GT_120) AND "
        "MAX_EVENT_SHARE_OF_GROSS_POSITIVES_LTE_0_50 AND "
        "AVERAGE_SLIPPAGE_VS_SIMULATED_BBO_LTE_1_TICK_PER_SIDE"
    )
    assert document["gates"]["look_policy"] == (
        "ONE_LOOK_AFTER_FROZEN_HORIZON_NO_INTERIM_PASS_OR_RETUNING"
    )
    assert document["gates"]["stress_diagnostic_debit_ticks"] == [14, 18]
    assert document["candidate_registration"]["candidate_config_sha256"] == (config.semantic_sha256)
    assert len(config.semantic_sha256) == 64
    assert document["provenance"]["campaign_config_sha256"] == config.semantic_sha256
    assert document["evidence_disclosure"]["consumed_holdout_disclosure"] == (
        config.consumed_holdout_disclosure
    )
    assert document["evidence_disclosure"]["discovery_vs_preregistered"] == (
        config.discovery_preregistered_disclosure
    )
    evidence_files = document["provenance"]["evidence_files"]
    assert len(evidence_files) == 14
    assert all(len(item["sha256"]) == 64 and item["byte_size"] > 0 for item in evidence_files)
    audit_artifacts = document["provenance"]["audit_artifacts"]
    assert [item["artifact_schema"] for item in audit_artifacts] == [
        "systematic_fx.e2a_handover_raw_audit.v1",
        "systematic_fx.e2a_strict_physical_audit.v1",
    ]
    assert all(
        len(item["audit_body_sha256"]) == 64
        and len(item["canonical_artifact"]["content_sha256"]) == 64
        and len(item["repository_mirror"]["sha256"]) == 64
        for item in audit_artifacts
    )
    implementation_files = document["provenance"]["implementation_files"]
    assert [item["relative_path"] for item in implementation_files] == [
        "campaigns/e2a_month_end_v1/config.py",
        "campaigns/e2a_month_end_v1/engine.py",
        "campaigns/e2a_month_end_v1/strict.py",
        "campaigns/e2a_month_end_v1/forward.py",
        "scripts/audit_e2a_handover.py",
        "scripts/audit_e2a_strict_physical.py",
        "scripts/run_e2a_forward_validation.py",
    ]
    assert all(len(item["sha256"]) == 64 and item["byte_size"] > 0 for item in implementation_files)
    registration_files = document["provenance"]["registration_files"]
    assert [item["relative_path"] for item in registration_files] == [
        "configs/campaigns/e2a_month_end_v1.toml",
        "docs/research/E2A_MONTH_END_V1.md",
    ]
    assert all(len(item["sha256"]) == 64 and item["byte_size"] > 0 for item in registration_files)
    policy_files = document["provenance"]["policy_files"]
    assert [item["relative_path"] for item in policy_files] == [
        "docs/DESIGN.md",
        "docs/VALIDATION.md",
        "docs/phases/PHASE_1_DESIGN.md",
        "docs/phases/PHASE_2_DESIGN.md",
    ]
    assert all(len(item["sha256"]) == 64 and item["byte_size"] > 0 for item in policy_files)


def test_plan_type_rejects_an_armed_authority() -> None:
    document = build_e2a_forward_plan().as_dict()
    document["authority"]["lifecycle_status"] = "ARMED"

    with pytest.raises(E2AForwardError, match="authority drifted"):
        E2AForwardPlan(canonical_json_bytes(document))


def test_observation_schema_cannot_invent_broker_execution() -> None:
    plan = build_e2a_forward_plan()
    document = {
        "artifact_schema": FORWARD_OBSERVATION_SCHEMA,
        "calendar_eligibility": {},
        "opportunity": {},
        "paper_execution": {
            "fill_ids": [],
            "order_ids": [],
            "platform": None,
            "slippage_ticks_e6_per_side": None,
            "status": "NOT_OBSERVED_NO_BROKER_ADAPTER",
        },
        "plan_sha256": plan.sha256,
        "signal": {},
        "simulated_execution": {},
        "source_lineage": {},
    }

    observation = E2AShadowObservation(canonical_json_bytes(document))
    assert observation.as_dict() == document
    document["paper_execution"]["status"] = "FILLED"
    with pytest.raises(E2AForwardError, match="invented Paper execution"):
        E2AShadowObservation(canonical_json_bytes(document))


def test_status_before_precommit_is_read_only(tmp_path: Path) -> None:
    state_root = tmp_path / "forward-state"

    result = status_e2a_forward(REPO_ROOT, state_root=state_root)

    assert result.plan_registered is False
    assert result.plan_artifact_published is False
    assert result.event_count == 0
    assert result.as_dict()["lifecycle_status"] == FORWARD_LIFECYCLE_STATUS
    assert result.as_dict()["authority_scope"] == FORWARD_AUTHORITY_SCOPE
    assert result.as_dict()["paper_order_authority"] is False
    assert not state_root.exists()


def test_cross_checkout_project_root_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(E2AForwardError, match="differs from the checkout"):
        status_e2a_forward(tmp_path, state_root=tmp_path / "forward-state")


def test_evidence_drift_is_translated_to_forward_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_verified_open(*_args: object, **_kwargs: object) -> object:
        raise E2AReproductionError("fixture drift")

    monkeypatch.setattr(forward_module, "verified_readonly_file", fail_verified_open)
    with pytest.raises(E2AForwardError, match="evidence identity failed closed"):
        forward_module._verified_evidence_identity("fixture", "0" * 64)


def test_audit_mirror_recomputes_embedded_body_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = build_e2a_forward_plan().as_dict()["provenance"]["audit_artifacts"][0]
    document = forward_module._audit_document_from_repository_mirror(original)
    document["audit_sha256"] = "0" * 64
    payload = canonical_json_bytes(document) + b"\n"
    relative_path = "fixture/audit.json"
    path = tmp_path / relative_path
    path.parent.mkdir()
    path.write_bytes(payload)
    contract = dict(original)
    contract["audit_body_sha256"] = "0" * 64
    contract["repository_mirror"] = {
        "byte_size": len(payload),
        "relative_path": relative_path,
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    monkeypatch.setattr(forward_module, "_REPOSITORY_ROOT", tmp_path)

    with pytest.raises(E2AForwardError, match="body hash differs"):
        forward_module._audit_document_from_repository_mirror(contract)


def test_audit_lineage_is_bound_to_current_plan() -> None:
    plan = build_e2a_forward_plan()
    contract = plan.as_dict()["provenance"]["audit_artifacts"][0]
    document = forward_module._audit_document_from_repository_mirror(contract)
    document["campaign_config_sha256"] = "0" * 64

    with pytest.raises(E2AForwardError, match="campaign or dataset lineage differs"):
        forward_module._verify_audit_lineage(document, plan)


def test_precommit_publishes_canonical_content_address_and_one_event(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "forward-state"
    plan = build_e2a_forward_plan()

    status = precommit_e2a_forward(REPO_ROOT, state_root=state_root)

    artifact_path = _artifact_path(state_root, status)
    event_path = _event_path(state_root)
    assert status.plan_registered is True
    assert status.event_count == 1
    assert status.plan_sha256 == plan.sha256
    assert status.plan_artifact.artifact_type == FORWARD_PLAN_ARTIFACT_TYPE
    assert status.plan_artifact.content_sha256 == plan.sha256
    assert artifact_path.name == f"sha256={plan.sha256}.json"
    assert artifact_path.read_bytes() == plan.canonical_bytes
    assert stat.S_IMODE(artifact_path.stat().st_mode) == 0o444
    assert stat.S_IMODE(event_path.stat().st_mode) == 0o444
    event = json.loads(event_path.read_bytes())
    assert canonical_json_bytes(event) == event_path.read_bytes()
    assert event["event_type"] == "PLAN_REGISTERED"
    assert event["sequence"] == 1
    assert event["predecessor_sha256"] is None
    assert event["plan_sha256"] == plan.sha256
    assert event["payload"]["plan_artifact"] == status.plan_artifact.as_dict()
    for contract in plan.as_dict()["provenance"]["audit_artifacts"]:
        audit_path = state_root / contract["canonical_artifact"]["relative_uri"]
        assert audit_path.is_file()
        assert stat.S_IMODE(audit_path.stat().st_mode) == 0o444
    assert verify_e2a_forward(REPO_ROOT, state_root=state_root).as_dict() == status.as_dict()


def test_precommit_is_an_exact_idempotent_no_op(tmp_path: Path) -> None:
    state_root = tmp_path / "forward-state"
    first = precommit_e2a_forward(REPO_ROOT, state_root=state_root)
    event_before = _event_path(state_root).read_bytes()
    artifact_before = _artifact_path(state_root, first).read_bytes()

    second = precommit_e2a_forward(REPO_ROOT, state_root=state_root)

    assert second.as_dict() == first.as_dict()
    assert _event_path(state_root).read_bytes() == event_before
    assert _artifact_path(state_root, second).read_bytes() == artifact_before
    assert list((state_root / "ledger/events").iterdir()) == [_event_path(state_root)]


def test_precommit_recovers_partial_audit_publication(tmp_path: Path) -> None:
    state_root = tmp_path / "forward-state"
    plan = build_e2a_forward_plan()
    first_contract = plan.as_dict()["provenance"]["audit_artifacts"][0]
    first_document = forward_module._audit_document_from_repository_mirror(first_contract)
    publish_forward_artifact(
        state_root,
        artifact_type=first_contract["canonical_artifact"]["artifact_type"],
        document=first_document,
    )

    status = precommit_e2a_forward(REPO_ROOT, state_root=state_root)

    assert status.plan_registered is True
    assert status.event_count == 1
    assert verify_e2a_forward(REPO_ROOT, state_root=state_root).plan_registered is True


def test_generic_artifact_publication_is_content_addressed_and_rejects_float(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "forward-state"
    document = {"artifact_schema": "fixture.v1", "integer_ticks": 7}

    first = publish_forward_artifact(
        state_root,
        artifact_type="FIXTURE",
        document=document,
    )
    second = publish_forward_artifact(
        state_root,
        artifact_type="FIXTURE",
        document=document,
    )

    assert first == second
    assert (state_root / first.relative_uri).read_bytes() == canonical_json_bytes(document)
    with pytest.raises(E2AForwardError, match="strict canonical JSON"):
        publish_forward_artifact(
            state_root,
            artifact_type="FIXTURE",
            document={"artifact_schema": "fixture.v1", "unsafe": 0.5},
        )


def test_verify_detects_writable_plan_artifact(tmp_path: Path) -> None:
    state_root = tmp_path / "forward-state"
    status = precommit_e2a_forward(REPO_ROOT, state_root=state_root)
    artifact_path = _artifact_path(state_root, status)
    artifact_path.chmod(0o644)

    with pytest.raises(E2AForwardError, match="identity or mode differs"):
        verify_e2a_forward(REPO_ROOT, state_root=state_root)


def test_verify_detects_ledger_predecessor_tampering(tmp_path: Path) -> None:
    state_root = tmp_path / "forward-state"
    precommit_e2a_forward(REPO_ROOT, state_root=state_root)
    event_path = _event_path(state_root)
    event = json.loads(event_path.read_bytes())
    event["predecessor_sha256"] = "0" * 64
    event_path.chmod(0o644)
    event_path.write_bytes(canonical_json_bytes(event))
    event_path.chmod(0o444)

    with pytest.raises(E2AForwardError, match="cannot have a predecessor"):
        verify_e2a_forward(REPO_ROOT, state_root=state_root)


def test_verify_rejects_registration_at_or_after_first_decision(tmp_path: Path) -> None:
    state_root = tmp_path / "forward-state"
    precommit_e2a_forward(REPO_ROOT, state_root=state_root)
    event_path = _event_path(state_root)
    event = json.loads(event_path.read_bytes())
    event["recorded_at_utc"] = "2026-08-31T14:00:00.000000Z"
    event_path.chmod(0o644)
    event_path.write_bytes(canonical_json_bytes(event))
    event_path.chmod(0o444)

    with pytest.raises(E2AForwardError, match="not recorded before"):
        verify_e2a_forward(REPO_ROOT, state_root=state_root)


def test_late_precommit_fails_before_creating_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_root = tmp_path / "forward-state"
    monkeypatch.setattr(
        forward_module,
        "_now_utc",
        lambda: datetime(2026, 8, 31, 14, 0, tzinfo=UTC),
    )

    with pytest.raises(E2AForwardError, match="cannot be locally registered"):
        precommit_e2a_forward(REPO_ROOT, state_root=state_root)

    assert not state_root.exists()


def test_precommit_rejects_symbolic_artifact_directory(tmp_path: Path) -> None:
    state_root = tmp_path / "forward-state"
    outside = tmp_path / "outside"
    state_root.mkdir()
    outside.mkdir()
    (state_root / "artifacts").symlink_to(outside, target_is_directory=True)

    with pytest.raises(E2AForwardError, match="symbolic|escaped forward state"):
        precommit_e2a_forward(REPO_ROOT, state_root=state_root)

    assert tuple(outside.iterdir()) == ()


def test_observe_is_explicitly_unavailable_and_does_not_create_state(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "forward-state"

    with pytest.raises(E2AForwardUnavailable) as captured:
        observe_shadow_e2a_forward(REPO_ROOT, state_root=state_root)

    assert captured.value.code == OBSERVE_UNAVAILABLE_CODE
    assert not state_root.exists()


def test_cli_exposes_no_authority_commands_and_observe_exits_two(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    parser = build_parser()
    action = next(item for item in parser._actions if item.dest == "action")
    choices = set(action.choices)
    assert choices == {"precommit", "status", "verify", "observe-shadow"}
    assert not {"arm", "live", "paper", "submit"}.intersection(choices)
    state_root = tmp_path / "forward-state"

    exit_code = main(
        [
            "observe-shadow",
            "--project-root",
            str(REPO_ROOT),
            "--state-root",
            str(state_root),
            "--json",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    failure = json.loads(captured.err)
    assert failure == {
        "action": "observe-shadow",
        "authority_scope": "OFFLINE_SHADOW_ONLY",
        "error_code": OBSERVE_UNAVAILABLE_CODE,
        "lifecycle_status": "PLANNED_NOT_ARMABLE",
        "status": "UNAVAILABLE",
    }
    assert not state_root.exists()


def test_cli_precommit_status_and_verify_report_unarmable(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    state_root = tmp_path / "forward-state"
    common = [
        "--project-root",
        str(REPO_ROOT),
        "--state-root",
        str(state_root),
        "--json",
    ]

    for action in ("precommit", "status", "verify"):
        assert main([action, *common]) == 0
        captured = capsys.readouterr()
        assert captured.err == ""
        result = json.loads(captured.out)
        assert result["lifecycle_status"] == "PLANNED_NOT_ARMABLE"
        assert result["authority_scope"] == "OFFLINE_SHADOW_ONLY"
        assert result["paper_order_authority"] is False
        assert result["registration_status"] == "REGISTERED_APPEND_ONLY_LOCAL_LEDGER"
        assert result["registration_timing"] == (
            "LOCALLY_RECORDED_BEFORE_FIRST_PROVISIONAL_DECISION_NOT_EXTERNALLY_TIMESTAMPED"
        )
        assert result["event_count"] == 1
