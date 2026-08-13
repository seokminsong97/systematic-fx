from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass, replace
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from systematic_fx.db.bar_registry import (
    BAR_DATASET_MANIFEST_KEY,
    RAW_SOURCE_MANIFEST_KEY,
    abort_bar_run_attempt,
    validate_completed_bar_campaign,
    validate_reused_bar_attempts,
)
from systematic_fx.db.migrations import discover_migrations
from systematic_fx.research import bar_discovery as bar_discovery_module
from systematic_fx.research.bar_artifacts import BarArtifactDescriptor, PublishedBarArtifact
from systematic_fx.research.bar_config import load_bar_pattern_config
from systematic_fx.research.bar_discovery import (
    BarDiscoveryResult,
)
from systematic_fx.research.bar_pipeline import (
    BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
    LoadedBarDatasetManifest,
)
from systematic_fx.research.bar_research_run import (
    BAR_DATASET_KEY,
    BAR_DISCOVERY_LINEAGE_SCHEMA,
    BAR_RESEARCH_RUN_SCHEMA,
    SUPPORTED_MIGRATIONS,
    BarResearchRunError,
    BarResearchRunServices,
    BarRunProvenance,
    PreparedBarResearchRun,
    _default_services,
    _stage_global_discovery_result,
    _validate_discovery_result,
    build_bar_candidate_run_specs,
    execute_prepared_bar_research_run,
    publish_global_bar_discovery_result,
)
from systematic_fx.research.bar_selection import BarCandidateDecision, BarSupportEvidence
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.provenance import CODE_SNAPSHOT_SCHEMA, CodeSnapshot, SnapshotFile
from systematic_fx.validation.bar_splits import plan_bar_splits

ROOT = Path(__file__).resolve().parents[2]
DATASET_SHA256 = "e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc"
RAW_SHA256 = "14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de"
HANDOFF_SHA256 = "a" * 64
DEPENDENCY_SHA256 = "d" * 64
MIGRATION_SHA256 = "e" * 64
CODE_COMMIT = "1" * 40


def test_bar_research_run_supports_exact_p4_paired_migration_chain() -> None:
    migrations = discover_migrations(ROOT / "migrations")
    migration_by_version = {item.version: item for item in migrations}

    assert tuple(item.version for item in migrations) == SUPPORTED_MIGRATIONS
    assert SUPPORTED_MIGRATIONS == tuple(range(1, 30))
    assert migration_by_version[24].checksum == (
        "4aa845757f1a220c8d5595d4db6053f6374d99d067ab7e20c3e40ea22d610010"
    )
    assert migration_by_version[25].checksum == (
        "e08aa486bf9a65b2875e92866ae5e939fc56dc5d871010dfdb4b9085550749dd"
    )
    assert migration_by_version[26].checksum == (
        "232badda3e76fca79f93fcff059de6f3404fc797eb26a93c9483fd554cfe20bb"
    )
    assert migration_by_version[27].name == "bar_state_v2b_parquet_schema_amendment"
    assert migration_by_version[27].checksum == (
        "f0f69db031dc555b260da1fceef5f1fb4087f25717f1472ae4b006e77182cdb8"
    )
    assert migrations[-1].name == "m0b_governed_control_plane"
    assert len(migrations[-1].checksum) == 64


def _loaded_dataset(dates: tuple[date, ...]) -> LoadedBarDatasetManifest:
    # RunSpec/orchestration unit tests start after the hardened manifest loader;
    # constructing 7,065 physical artifact descriptors here would retest that
    # loader rather than this layer.
    result = object.__new__(LoadedBarDatasetManifest)
    object.__setattr__(result, "dataset_manifest_sha256", DATASET_SHA256)
    object.__setattr__(result, "source_manifest_sha256", RAW_SHA256)
    object.__setattr__(
        result,
        "outcome_span_policy_sha256",
        BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
    )
    object.__setattr__(result, "eligible_active_dates", dates)
    object.__setattr__(result, "partitions", ())
    return result


def _prepared(tmp_path: Path) -> PreparedBarResearchRun:
    dates = tuple(date(2022, 1, 1) + timedelta(days=index) for index in range(900))
    data = tmp_path / "data"
    data.mkdir(parents=True)
    return PreparedBarResearchRun(
        project_root=tmp_path.resolve(),
        data_root=data.resolve(),
        dataset=_loaded_dataset(dates),
        config=load_bar_pattern_config(ROOT),
        split_plan=plan_bar_splits(dates),
        dataset_handoff_sha256=HANDOFF_SHA256,
    )


def _snapshot(config_path: Path) -> CodeSnapshot:
    content = config_path.read_bytes()
    file = SnapshotFile(
        relative_path="configs/research/bar_pattern_discovery_v1.toml",
        byte_size=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        executable=False,
        content_base64=base64.b64encode(content).decode("ascii"),
    )
    payload = {
        "artifact_schema": CODE_SNAPSHOT_SCHEMA,
        "code_commit": CODE_COMMIT,
        "file_count": 1,
        "files": [file.payload],
    }
    canonical = canonical_json_bytes(payload)
    return CodeSnapshot(
        code_commit=CODE_COMMIT,
        files=(file,),
        canonical_bytes=canonical,
        sha256=hashlib.sha256(canonical).hexdigest(),
    )


def _artifact(
    tmp_path: Path,
    *,
    key: str,
    content_sha256: str,
    source_sha256: str = DATASET_SHA256,
    artifact_type: str = "test_artifact",
) -> PublishedBarArtifact:
    descriptor = BarArtifactDescriptor(
        artifact_key=key,
        artifact_type=artifact_type,
        artifact_schema="systematic_fx.test_artifact.v1",
        artifact_version=1,
        record_count=1,
        schema_sha256="f" * 64,
        source_manifest_sha256=source_sha256,
        logical_identity={"key": key},
        media_type="application/json",
        file_suffix=".json",
    )
    return PublishedBarArtifact(
        descriptor=descriptor,
        path=(tmp_path / f"sha256={content_sha256}.json").resolve(),
        sha256=content_sha256,
        byte_size=1,
    )


def _provenance(prepared: PreparedBarResearchRun) -> BarRunProvenance:
    snapshot = _snapshot(prepared.config.path)
    return BarRunProvenance(
        code_commit=CODE_COMMIT,
        snapshot=snapshot,
        snapshot_artifact=_artifact(
            prepared.project_root,
            key="test:code_snapshot",
            content_sha256=snapshot.sha256,
            artifact_type="bar_code_snapshot",
        ),
        dependency_lock_sha256=DEPENDENCY_SHA256,
        runtime_environment={
            "bar_research_run": {
                "code_snapshot_artifact_identity_sha256": "b" * 64,
                "dataset_handoff_sha256": HANDOFF_SHA256,
                "engine_version": "bar_pattern_streaming_discovery_v1",
                "orchestration": "REGISTER_AND_START_ALL_BEFORE_SINGLE_DISCOVERY_PASS",
            },
            "postgresql": {"schema_migrations_sha256": MIGRATION_SHA256},
            "python": "test",
        },
        postgres_migrations_sha256=MIGRATION_SHA256,
    )


def test_builds_exactly_216_candidate_specific_fully_bound_run_specs(tmp_path: Path) -> None:
    prepared = _prepared(tmp_path)
    specs = build_bar_candidate_run_specs(prepared, _provenance(prepared))

    assert len(specs) == 216
    assert len({item.fingerprint for item in specs}) == 216
    assert tuple(item.parameters["bar_candidate_key"] for item in specs) == (
        prepared.candidate_keys
    )
    assert all(
        dict(item.source_manifest_hashes)
        == {
            RAW_SOURCE_MANIFEST_KEY: RAW_SHA256,
            BAR_DATASET_MANIFEST_KEY: DATASET_SHA256,
        }
        for item in specs
    )
    assert all(
        item.parameters["bar_dataset_handoff_sha256"] == HANDOFF_SHA256
        and item.parameters["bar_outcome_span_policy_sha256"]
        == BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256
        and item.parameters["bar_config_file_sha256"] == prepared.config.sha256
        and item.parameters["bar_config_semantic_sha256"] == prepared.config.semantic_sha256
        and item.parameters["bar_candidate_catalog_sha256"]
        == prepared.config.candidate_catalog_sha256
        and item.parameters["bar_split_plan_sha256"] == prepared.split_plan.sha256
        and item.parameters["bar_entry_policy_sha256"] == canonical_sha256(item.entry_policy)
        and item.parameters["bar_barrier_policy_sha256"] == canonical_sha256(item.barrier_policy)
        for item in specs
    )
    assert {item.run_kind for item in specs} == {"SCREEN"}
    assert {item.direction for item in specs} == {"LONG", "SHORT"}
    cost_policy = json.loads(canonical_json_bytes(specs[0].parameters["bar_cost_policy"]))
    evidence_policy = json.loads(canonical_json_bytes(specs[0].parameters["bar_evidence_policy"]))
    assert cost_policy == {
        "base_monthly_fixed_pool_usd": "500.00",
        "execution_scenarios": [item.as_dict() for item in prepared.config.execution_scenarios],
        "expected_monthly_round_trips": 20,
        "fixed_cost_allocation": "MONTHLY_POOL_DIVIDED_BY_ROUND_TRIPS_CEILING_TICKS",
        "market": {"parent_symbol": "6E", "tick_size_raw": 50_000, "ticks_per_pip": 2},
        "schema": "systematic_fx.bar_cost_policy.v1",
        "tick_value_usd": "6.25",
    }
    assert evidence_policy["match_shard_max_records"] == 4_096
    assert evidence_policy["replay_shard_max_records"] == 256
    assert specs[0].parameters["bar_cost_policy_sha256"] == canonical_sha256(cost_policy)
    assert specs[0].parameters["bar_evidence_policy_sha256"] == canonical_sha256(evidence_policy)
    assert prepared.config.semantic_sha256 == (
        "34b84587e12af32f84bdcc3e66552c763feccbc55043d8514e188fb8895c7283"
    )
    assert prepared.config.definition_sha256 == (
        "8515c02921da6a1da31edb49a4809e048ae931a4ae32741559216eac1cc74081"
    )


def test_plan_only_has_no_provenance_artifact_or_database_side_effect(tmp_path: Path) -> None:
    prepared = _prepared(tmp_path)

    def forbidden(*args: object, **kwargs: object) -> Any:
        raise AssertionError("PLAN_ONLY called a side-effecting service")

    services = BarResearchRunServices(*([forbidden] * 23))
    progress = []
    report = execute_prepared_bar_research_run(
        prepared,
        mode="PLAN_ONLY",
        services=services,
        progress=progress.append,
    )

    assert report.disposition == "PLANNED"
    assert report.candidate_runs == ()
    assert report.as_dict()["schema"] == BAR_RESEARCH_RUN_SCHEMA
    assert progress[0].stage == "PLAN_READY"


def test_default_services_use_bar_specific_abort_reuse_and_completion_guards() -> None:
    services = _default_services()

    assert services.fail_attempt is abort_bar_run_attempt
    assert services.validate_reused_attempts is validate_reused_bar_attempts
    assert services.validate_completed_campaign is validate_completed_bar_campaign


def test_run_spec_build_rejects_discovery_evidence_buffer_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = _prepared(tmp_path)
    monkeypatch.setattr(bar_discovery_module, "_REPLAY_SHARD_MAX_RECORDS", 257)

    with pytest.raises(BarResearchRunError, match="evidence buffer constants drifted"):
        build_bar_candidate_run_specs(prepared, _provenance(prepared))


@dataclass(frozen=True)
class _CandidateResult:
    candidate: Any
    support: BarSupportEvidence
    decision: BarCandidateDecision
    final_label: str = "SUPPORT_REJECT"
    decision_trigger_count: int = 10
    evaluated_count: int = 0
    matched_signal_count: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_definition": self.candidate.definition_payload(),
            "candidate_definition_sha256": self.candidate.definition_sha256,
            "candidate_key": self.candidate.candidate_key,
            "decision": {"label": "SUPPORT_REJECT"},
            "economics": [],
            "final_label": self.final_label,
        }


@dataclass(frozen=True)
class _EvidenceManifest:
    artifact: PublishedBarArtifact
    evidence_identity_sha256: str = "5" * 64
    sha256: str = "6" * 64
    matched_record_count: int = 0
    replay_record_count: int = 0
    shards: tuple[object, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_identity_sha256": self.artifact.descriptor.identity_sha256,
            "evidence_identity_sha256": self.evidence_identity_sha256,
            "matched_record_count": self.matched_record_count,
            "non_ascii_regression_note": "한글 증거",
            "replay_record_count": self.replay_record_count,
            "sha256": self.sha256,
            "shards": [],
        }


def _fake_discovery_result(prepared: PreparedBarResearchRun, evidence: Any) -> Any:
    candidate_results = []
    for candidate in prepared.config.candidates:
        support = BarSupportEvidence(
            candidate_key=candidate.candidate_key,
            timeframe_seconds=candidate.timeframe_seconds,
            direction=candidate.direction,
            raw_signal_count=0,
            distinct_signal_day_count=0,
            block_signal_counts=(0, 0, 0, 0),
            median_signals_per_active_day_numerator=0,
            median_signals_per_active_day_denominator=1,
        )
        decision = BarCandidateDecision(
            candidate_key=candidate.candidate_key,
            direction=candidate.direction,
            label="SUPPORT_REJECT",
            selected_take_profit_ticks=None,
            selected_stop_loss_ticks=None,
            positive_component_size=0,
            rejection_reasons=("INSUFFICIENT_RAW_SIGNALS",),
            positive_block_count=0,
            worst_block_moderate_ev_ticks=None,
            overall_moderate_ev_ticks=None,
            moderate_maximum_drawdown_ticks=None,
        )
        candidate_results.append(_CandidateResult(candidate, support, decision))
    return SimpleNamespace(
        candidate_results=tuple(candidate_results),
        evidence_manifest=evidence,
        ranked_finalist_keys=(),
        budget_rejected_keys=(),
        sha256="7" * 64,
    )


@pytest.mark.parametrize("field", ["config_semantic_sha256", "candidate_catalog_sha256"])
def test_discovery_root_rejects_config_or_catalog_drift(
    tmp_path: Path,
    field: str,
) -> None:
    prepared = _prepared(tmp_path)
    result = BarDiscoveryResult(
        source_identity_sha256=DATASET_SHA256,
        dataset_build_sha256=DATASET_SHA256,
        outcome_span_policy_sha256=BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
        config_semantic_sha256=prepared.config.semantic_sha256,
        candidate_catalog_sha256=prepared.config.candidate_catalog_sha256,
        split_plan_sha256=prepared.split_plan.sha256,
        loaded_source_dates=(),
        decision_dates=(),
        loaded_bar_counts=(),
        replay_catalog=(),
        evidence_manifest=None,
        candidate_results=(),
        ranked_finalist_keys=(),
        budget_rejected_keys=(),
    )
    drifted = replace(result, **{field: "0" * 64})

    with pytest.raises(BarResearchRunError, match="lineage or visible date boundary drift"):
        _validate_discovery_result(drifted, prepared=prepared)


def test_global_result_stream_is_exact_canonical_preimage_and_publishes_once(
    tmp_path: Path,
) -> None:
    prepared = _prepared(tmp_path)
    evidence_artifact = _artifact(
        prepared.project_root,
        key="test:stream_evidence",
        content_sha256="6" * 64,
        artifact_type="bar_discovery_evidence_manifest",
    )
    evidence = _EvidenceManifest(artifact=evidence_artifact)
    candidate_results = []
    for candidate in prepared.config.candidates:
        support = BarSupportEvidence(
            candidate_key=candidate.candidate_key,
            timeframe_seconds=candidate.timeframe_seconds,
            direction=candidate.direction,
            raw_signal_count=0,
            distinct_signal_day_count=0,
            block_signal_counts=(0, 0, 0, 0),
            median_signals_per_active_day_numerator=0,
            median_signals_per_active_day_denominator=1,
        )
        decision = BarCandidateDecision(
            candidate_key=candidate.candidate_key,
            direction=candidate.direction,
            label="SUPPORT_REJECT",
            selected_take_profit_ticks=None,
            selected_stop_loss_ticks=None,
            positive_component_size=0,
            rejection_reasons=("INSUFFICIENT_RAW_SIGNALS",),
            positive_block_count=0,
            worst_block_moderate_ev_ticks=None,
            overall_moderate_ev_ticks=None,
            moderate_maximum_drawdown_ticks=None,
        )
        candidate_results.append(_CandidateResult(candidate, support, decision))
    result = BarDiscoveryResult(
        source_identity_sha256=DATASET_SHA256,
        dataset_build_sha256=DATASET_SHA256,
        outcome_span_policy_sha256=BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
        config_semantic_sha256=prepared.config.semantic_sha256,
        candidate_catalog_sha256=prepared.config.candidate_catalog_sha256,
        split_plan_sha256=prepared.split_plan.sha256,
        loaded_source_dates=(),
        decision_dates=(),
        loaded_bar_counts=((1, 0),),
        replay_catalog=(),
        evidence_manifest=evidence,  # type: ignore[arg-type]
        candidate_results=tuple(candidate_results),  # type: ignore[arg-type]
        ranked_finalist_keys=(),
        budget_rejected_keys=(),
    )

    staged, digest, byte_size = _stage_global_discovery_result(
        result,
        data_root=prepared.data_root,
    )
    try:
        staged.seek(0)
        staged_bytes = staged.read()
    finally:
        staged.close()
    assert staged_bytes == result.canonical_bytes
    assert b"\\ud55c\\uae00 \\uc99d\\uac70" in staged_bytes
    assert staged_bytes.endswith(b"}\n")
    assert digest == hashlib.sha256(staged_bytes).hexdigest()
    assert digest == result.sha256
    assert byte_size == len(staged_bytes)

    artifact = publish_global_bar_discovery_result(
        prepared.project_root,
        result,
        prepared=prepared,
    )
    assert artifact.path.read_bytes() == staged_bytes
    assert artifact.sha256 == digest
    assert artifact.sha256 == result.sha256
    assert artifact.descriptor.logical_identity["discovery_result_sha256"] == digest


def _governed_services(
    prepared: PreparedBarResearchRun,
    *,
    discovery_error: Exception | None = None,
    start_error_at: int | None = None,
    reservation_drift_at: int | None = None,
    duplicate_count: int = 0,
    reused_global_sha256: str = "7" * 64,
) -> tuple[BarResearchRunServices, list[tuple[Any, ...]]]:
    events: list[tuple[Any, ...]] = []
    snapshot = _snapshot(prepared.config.path)
    snapshot_artifact = _artifact(
        prepared.project_root,
        key="test:captured_snapshot",
        content_sha256=snapshot.sha256,
        artifact_type="bar_code_snapshot",
    )
    evidence_artifact = _artifact(
        prepared.project_root,
        key="test:evidence_manifest",
        content_sha256="6" * 64,
        artifact_type="bar_discovery_evidence_manifest",
    )
    evidence = SimpleNamespace(
        artifact=evidence_artifact,
        shards=(),
        evidence_identity_sha256="5" * 64,
        sha256=evidence_artifact.sha256,
        matched_record_count=0,
        replay_record_count=0,
    )
    result = _fake_discovery_result(prepared, evidence)
    global_artifact = _artifact(
        prepared.project_root,
        key="test:global_result",
        content_sha256=result.sha256,
        artifact_type="bar_global_discovery_result",
    )
    attempt_counter = 0
    spec_id_by_fingerprint: dict[str, int] = {}
    terminal_sha256_by_key: dict[str, str] = {}

    def publish_snapshot(*args: object, **kwargs: object) -> PublishedBarArtifact:
        events.append(("publish_snapshot",))
        return snapshot_artifact

    def publish_registration(*args: object, **kwargs: object) -> PublishedBarArtifact:
        events.append(("publish_registration",))
        return _artifact(
            prepared.project_root,
            key="test:registration",
            content_sha256="4" * 64,
            artifact_type="bar_registration",
        )

    def register_campaign(*args: object, **kwargs: object) -> object:
        assert kwargs["dataset_key"] == BAR_DATASET_KEY
        events.append(("register_campaign",))
        return object()

    def register_artifact(*args: object, **kwargs: object) -> object:
        events.append(("register_artifact", args[2].descriptor.artifact_type))
        return object()

    def register_spec(*args: object, **kwargs: object) -> object:
        events.append(("register_spec", kwargs["candidate_key"]))
        spec = args[1]
        identifier = len(spec_id_by_fingerprint) + 1
        spec_id_by_fingerprint[spec.fingerprint] = identifier
        return SimpleNamespace(
            research_run_spec_id=identifier,
            run_fingerprint=spec.fingerprint,
        )

    def reserve(*args: object, **kwargs: object) -> object:
        nonlocal attempt_counter
        attempt_counter += 1
        events.append(("reserve", attempt_counter))
        duplicate = attempt_counter <= duplicate_count
        return SimpleNamespace(
            execute=not duplicate,
            research_run_attempt_id=attempt_counter,
            research_run_spec_id=(
                spec_id_by_fingerprint[kwargs["run_fingerprint"]]
                + (1 if attempt_counter == reservation_drift_at else 0)
            ),
            attempt_number=(2 if duplicate else 1),
            reused_attempt_id=(1_000 + attempt_counter if duplicate else None),
            status=("SKIPPED_DUPLICATE" if duplicate else "QUEUED"),
        )

    def start(*args: object, **kwargs: object) -> object:
        events.append(("start", kwargs["research_run_attempt_id"]))
        if kwargs["research_run_attempt_id"] == start_error_at:
            raise RuntimeError("lost start response")
        attempt_id = kwargs["research_run_attempt_id"]
        return SimpleNamespace(
            status="RUNNING",
            research_run_attempt_id=attempt_id,
            research_run_spec_id=attempt_id,
            attempt_number=1,
        )

    def discovery(*args: object, **kwargs: object) -> object:
        assert sum(event[0] == "start" for event in events) == 216 - duplicate_count
        events.append(("discovery",))
        if discovery_error is not None:
            raise discovery_error
        return result

    terminal_counter = 0

    def publish_terminal(*args: object, **kwargs: object) -> PublishedBarArtifact:
        nonlocal terminal_counter
        terminal_counter += 1
        compact = kwargs["compact_result"]
        full = kwargs["candidate_result"]
        assert full["discovery_lineage"]["schema"] == BAR_DISCOVERY_LINEAGE_SCHEMA
        assert (
            compact["evidence_artifact_identity_sha256"]
            == (full["discovery_lineage"]["evidence_artifact_identity_sha256"])
        )
        assert (
            compact["global_result_artifact_identity_sha256"]
            == (full["discovery_lineage"]["global_result_artifact_identity_sha256"])
        )
        events.append(("publish_terminal", kwargs["candidate_key"]))
        artifact = _artifact(
            prepared.project_root,
            key=f"test:terminal:{terminal_counter}",
            content_sha256=f"{terminal_counter:064x}",
            artifact_type="bar_terminal_result",
        )
        terminal_sha256_by_key[kwargs["candidate_key"]] = artifact.sha256
        return artifact

    def publish_global(*args: object, **kwargs: object) -> PublishedBarArtifact:
        events.append(("publish_global",))
        return global_artifact

    def register_terminal(*args: object, **kwargs: object) -> object:
        terminal = kwargs["result"]
        assert terminal.trial_status == "REJECTED"
        assert terminal.decision_label == "SCREENING_REJECT"
        events.append(("register_terminal", terminal.research_run_attempt_id))
        return SimpleNamespace(
            research_run_attempt_id=terminal.research_run_attempt_id,
            research_run_spec_id=terminal.research_run_attempt_id,
            attempt_status="SUCCEEDED",
            trial_status=terminal.trial_status,
            decision_label=terminal.decision_label,
        )

    def fail(*args: object, **kwargs: object) -> object:
        events.append(("fail", kwargs["research_run_attempt_id"], "FAILED"))
        return object()

    def reuse_report(
        reservations: dict[str, tuple[Any, Any]],
        *,
        completion: bool,
    ) -> object:
        keys = (
            prepared.candidate_keys
            if completion
            else tuple(key for key in prepared.candidate_keys if not reservations[key][1].execute)
        )
        candidates = []
        for key in keys:
            spec, reservation = reservations[key]
            candidates.append(
                SimpleNamespace(
                    candidate_key=key,
                    run_fingerprint=spec.fingerprint,
                    duplicate_attempt_id=(
                        None if completion else reservation.research_run_attempt_id
                    ),
                    reused_attempt_id=(
                        reservation.reused_attempt_id
                        if reservation.reused_attempt_id is not None
                        else reservation.research_run_attempt_id
                    ),
                    trial_status="REJECTED",
                    final_label="SUPPORT_REJECT",
                    terminal_artifact_sha256=terminal_sha256_by_key.get(key, "8" * 64),
                )
            )
        return SimpleNamespace(
            candidates=tuple(candidates),
            global_result_artifact_sha256=reused_global_sha256,
            global_result_artifact_identity_sha256=global_artifact.descriptor.identity_sha256,
            evidence_manifest_sha256=evidence.sha256,
            evidence_artifact_identity_sha256=evidence.artifact.descriptor.identity_sha256,
            evidence_identity_sha256=evidence.evidence_identity_sha256,
            finalist_keys=(),
            final_label_counts=(("SUPPORT_REJECT", 216),),
        )

    def validate_reused(*args: object, **kwargs: object) -> object:
        events.append(("validate_reused",))
        return reuse_report(kwargs["reservations"], completion=False)

    def validate_completed(*args: object, **kwargs: object) -> object:
        events.append(("validate_completed",))
        reservations = {
            key: (
                kwargs["run_specs"][key],
                SimpleNamespace(
                    execute=(index > duplicate_count),
                    research_run_attempt_id=index,
                    reused_attempt_id=(1_000 + index if index <= duplicate_count else None),
                ),
            )
            for index, key in enumerate(prepared.candidate_keys, start=1)
        }
        return reuse_report(reservations, completion=True)

    postgres = {
        "schema_migrations": [],
        "schema_migrations_sha256": MIGRATION_SHA256,
        "server_version": "18.4",
        "server_version_num": "180004",
    }
    runtime = {"python": "test"}
    services = BarResearchRunServices(
        load_config=lambda *args, **kwargs: prepared.config,
        plan_splits=lambda *args, **kwargs: prepared.split_plan,
        git_head=lambda *args, **kwargs: CODE_COMMIT,
        build_snapshot=lambda *args, **kwargs: snapshot,
        publish_snapshot=publish_snapshot,
        dependency_hash=lambda *args, **kwargs: DEPENDENCY_SHA256,
        runtime=lambda: dict(runtime),
        postgres_runtime=lambda *args, **kwargs: dict(postgres),
        verify_artifact=lambda *args, **kwargs: None,
        publish_registration=publish_registration,
        register_campaign=register_campaign,
        register_artifact=register_artifact,
        register_spec=register_spec,
        reserve_attempt=reserve,
        start_attempt=start,
        validate_reused_attempts=validate_reused,
        validate_completed_campaign=validate_completed,
        run_discovery=discovery,
        validate_discovery=lambda *args, **kwargs: None,
        publish_global_result=publish_global,
        publish_terminal=publish_terminal,
        register_terminal=register_terminal,
        fail_attempt=fail,
    )
    return services, events


def test_governed_execution_starts_all_216_before_single_outcome_pass(
    tmp_path: Path,
) -> None:
    prepared = _prepared(tmp_path)
    services, events = _governed_services(prepared)

    report = execute_prepared_bar_research_run(
        prepared,
        mode="RUN",
        database_url="postgresql:///unused",
        services=services,
    )

    discovery_index = next(index for index, event in enumerate(events) if event[0] == "discovery")
    assert sum(event[0] == "register_spec" for event in events[:discovery_index]) == 216
    assert sum(event[0] == "reserve" for event in events[:discovery_index]) == 216
    assert sum(event[0] == "start" for event in events[:discovery_index]) == 216
    assert sum(event[0] == "publish_terminal" for event in events) == 216
    assert sum(event[0] == "register_terminal" for event in events) == 216
    assert not any(event[0] == "fail" for event in events)
    assert report.disposition == "COMPLETED"
    assert len(report.candidate_runs) == 216
    assert report.final_label_counts == (("SUPPORT_REJECT", 216),)
    assert report.evidence_artifact_count == 1
    assert report.discovery_result_sha256 == "7" * 64
    assert sum(event[0] == "publish_global" for event in events) == 1


def test_discovery_failure_terminalizes_every_running_attempt_as_failed(
    tmp_path: Path,
) -> None:
    prepared = _prepared(tmp_path)
    services, events = _governed_services(
        prepared,
        discovery_error=RuntimeError("deliberate outcome failure"),
    )

    with pytest.raises(BarResearchRunError, match="governed bar Discovery failed"):
        execute_prepared_bar_research_run(
            prepared,
            mode="RUN",
            database_url="postgresql:///unused",
            services=services,
        )

    assert sum(event[0] == "start" for event in events) == 216
    assert sum(event[0] == "fail" for event in events) == 216
    assert not any(event[0] == "publish_terminal" for event in events)
    assert all(event[2] == "FAILED" for event in events if event[0] == "fail")


def test_lost_start_response_cleans_reserved_queued_or_running_attempt(
    tmp_path: Path,
) -> None:
    prepared = _prepared(tmp_path)
    services, events = _governed_services(prepared, start_error_at=7)

    with pytest.raises(BarResearchRunError, match="governed bar Discovery failed"):
        execute_prepared_bar_research_run(
            prepared,
            mode="RUN",
            database_url="postgresql:///unused",
            services=services,
        )

    assert [event[1] for event in events if event[0] == "start"] == list(range(1, 8))
    assert [event[1] for event in events if event[0] == "fail"] == list(range(1, 8))
    assert not any(event[0] == "discovery" for event in events)


def test_reservation_contract_drift_still_cleans_the_queued_attempt(
    tmp_path: Path,
) -> None:
    prepared = _prepared(tmp_path)
    services, events = _governed_services(prepared, reservation_drift_at=7)

    with pytest.raises(BarResearchRunError, match="governed bar Discovery failed"):
        execute_prepared_bar_research_run(
            prepared,
            mode="RUN",
            database_url="postgresql:///unused",
            services=services,
        )

    assert [event[1] for event in events if event[0] == "start"] == list(range(1, 7))
    assert [event[1] for event in events if event[0] == "fail"] == list(range(1, 8))
    assert not any(event[0] == "discovery" for event in events)


def test_all_duplicates_are_live_validated_and_rehydrated_without_outcomes(
    tmp_path: Path,
) -> None:
    prepared = _prepared(tmp_path)
    services, events = _governed_services(prepared, duplicate_count=216)

    report = execute_prepared_bar_research_run(
        prepared,
        mode="RUN",
        database_url="postgresql:///unused",
        services=services,
    )

    assert report.disposition == "SKIPPED_DUPLICATE"
    assert len(report.candidate_runs) == 216
    assert all(item.disposition == "SKIPPED_DUPLICATE" for item in report.candidate_runs)
    assert report.discovery_result_sha256 == "7" * 64
    assert report.final_label_counts == (("SUPPORT_REJECT", 216),)
    assert sum(event[0] == "validate_reused" for event in events) == 1
    assert not any(event[0] in {"start", "discovery", "publish_global"} for event in events)


def test_partial_retry_requires_exact_persisted_global_and_evidence_consensus(
    tmp_path: Path,
) -> None:
    prepared = _prepared(tmp_path)
    services, events = _governed_services(
        prepared,
        duplicate_count=1,
        reused_global_sha256="9" * 64,
    )

    with pytest.raises(BarResearchRunError, match="governed bar Discovery failed"):
        execute_prepared_bar_research_run(
            prepared,
            mode="RUN",
            database_url="postgresql:///unused",
            services=services,
        )

    assert sum(event[0] == "start" for event in events) == 215
    assert sum(event[0] == "fail" for event in events) == 215
    assert sum(event[0] == "validate_reused" for event in events) == 1
    assert not any(event[0] in {"publish_terminal", "register_terminal"} for event in events)


def test_partial_retry_registers_only_missing_candidates_then_validates_aggregate(
    tmp_path: Path,
) -> None:
    prepared = _prepared(tmp_path)
    services, events = _governed_services(prepared, duplicate_count=1)

    report = execute_prepared_bar_research_run(
        prepared,
        mode="RUN",
        database_url="postgresql:///unused",
        services=services,
    )

    assert report.disposition == "COMPLETED"
    assert len(report.candidate_runs) == 216
    assert report.candidate_runs[0].disposition == "SKIPPED_DUPLICATE"
    assert sum(item.disposition == "TERMINAL_REGISTERED" for item in report.candidate_runs) == 215
    assert sum(event[0] == "publish_terminal" for event in events) == 215
    assert sum(event[0] == "register_terminal" for event in events) == 215
    assert sum(event[0] == "validate_completed" for event in events) == 1
