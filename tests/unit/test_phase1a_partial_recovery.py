from __future__ import annotations

import hashlib
import stat
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import systematic_fx.research.phase1a_pipeline as pipeline
from systematic_fx.db.research_registry import (
    Phase1ACurrentSlicePrefixReport,
    Phase1APartialRecoverySource,
    Phase1ARecoveryQuerySource,
)
from systematic_fx.research.hypotheses import canonical_json_bytes
from systematic_fx.research.phase1a_pipeline import (
    CAMPAIGN_ID,
    Phase1APipelineError,
    PipelineRunReport,
    ResolvedRunArtifact,
    _publish_recovery_manifest,
    _recover_phase1a_discovery_slice,
    _run_spec_from_recovery_source,
)
from systematic_fx.research.run_spec import RunSpec

_SHA_A = "a" * 64
_SHA_B = "b" * 64
_SHA_C = "c" * 64
_SHA_D = "d" * 64
_SHA_E = "e" * 64
_SHA_F = "f" * 64
_OLD_COMMIT = "1" * 40
_NEW_COMMIT = "2" * 40


def _ai_run_spec() -> RunSpec:
    return RunSpec(
        campaign_id=CAMPAIGN_ID,
        experiment_id=None,
        run_kind="AI_SLICE",
        engine_version="analysis-engine-v1",
        source_manifest_hashes={
            "mbp10_footer_manifest_v1": _SHA_A,
            "mbp10_source_sha256_v1": _SHA_B,
            "mbp10_structural_qc_v1": _SHA_C,
        },
        eligible_calendar_version="calendar-v7",
        eligible_calendar_sha256=_SHA_D,
        split_version="split-v9",
        split_sha256=_SHA_E,
        feature_version="feature-v3",
        feature_sha256=_SHA_F,
        outcome_version="outcome-v5",
        outcome_sha256="3" * 64,
        cost_version="cost-v4",
        cost_sha256="4" * 64,
        execution_version="execution-v6",
        execution_sha256="5" * 64,
        code_commit=_OLD_COMMIT,
        code_snapshot_sha256="6" * 64,
        dependency_lock_sha256="7" * 64,
        runtime_environment={
            "architecture": "arm64",
            "libraries": {"duckdb": "1.4.4", "pyarrow": "23.0.1"},
            "python": "3.12.12",
        },
        random_seed=97,
        direction="BOTH",
        signal_policy={"cadence_seconds": 300, "close_only": True},
        entry_policy={"order_type": "MARKETABLE_LIMIT", "timeout_ms": 250},
        barrier_policy={"grid_ticks": [24, 32, 40], "same_event": "LOSS_FIRST"},
        terminal_policy={"open_position": "MARK_UNRESOLVED", "tail_days": 5},
        parameters={
            "feature_inputs_by_date": {
                "2022-01-03": {"path": "research/part.parquet", "sha256": "8" * 64}
            },
            "frozen_toml_inputs": {"campaign": {"sha256": "9" * 64}},
            "no_entry_reason_by_date": {},
            "requested_source_dates": ["2022-01-03"],
            "slice_index": 0,
        },
    )


def test_recovery_run_spec_preserves_every_research_variable() -> None:
    parent = _ai_run_spec().payload()
    recovery_runtime = {
        "architecture": "arm64",
        "libraries": {"duckdb": "1.4.5", "pyarrow": "23.0.1"},
        "python": "3.12.13",
    }
    recovery_parameters = {
        "artifact_schema": "systematic_fx.phase1a_partial_recovery_control.v1",
        "no_research_recomputation": True,
        "slice_index": 0,
    }

    recovered = _run_spec_from_recovery_source(
        parent,
        run_kind="VALIDATION",
        engine_version="partial-recovery-v1",
        code_commit=_NEW_COMMIT,
        code_snapshot_sha256="a" * 64,
        dependency_sha256="b" * 64,
        runtime=recovery_runtime,
        parameters=recovery_parameters,
    ).payload()

    research_identity_fields = {
        "campaign_id",
        "experiment_id",
        "source_manifest_hashes",
        "eligible_calendar",
        "split",
        "feature",
        "outcome",
        "cost",
        "execution",
        "random_seed",
        "direction",
        "signal_policy",
        "entry_policy",
        "barrier_policy",
        "terminal_policy",
    }
    assert {key: recovered[key] for key in research_identity_fields} == {
        key: parent[key] for key in research_identity_fields
    }
    assert {key for key in parent if recovered[key] != parent[key]} == {
        "run_kind",
        "engine_version",
        "code_commit",
        "code_snapshot_sha256",
        "dependency_lock_sha256",
        "runtime_environment",
        "parameters",
    }
    assert recovered["parameters"] == recovery_parameters
    assert recovered["runtime_environment"] == recovery_runtime


def test_recovery_manifest_is_canonical_content_addressed_and_idempotent(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    (data_root / "derived").mkdir(parents=True)
    document = {
        "zeta": [3, 2, 1],
        "artifact_schema": "systematic_fx.phase1a_partial_recovery_manifest.v1",
        "nested": {"second": True, "first": "value"},
    }
    expected_content = canonical_json_bytes(document) + b"\n"
    expected_sha256 = hashlib.sha256(expected_content).hexdigest()

    first = _publish_recovery_manifest(data_root=data_root, document=document)
    first_inode = first.path.stat().st_ino
    second = _publish_recovery_manifest(
        data_root=data_root,
        document={
            "nested": {"first": "value", "second": True},
            "artifact_schema": "systematic_fx.phase1a_partial_recovery_manifest.v1",
            "zeta": [3, 2, 1],
        },
    )

    assert first.sha256 == second.sha256 == expected_sha256
    assert first.path == second.path
    assert first.path.name == f"sha256={expected_sha256}.json"
    assert (
        first.path.parent == (data_root / "derived/manifests/phase1a_partial_recovery_v1").resolve()
    )
    assert first.path.read_bytes() == expected_content
    assert first.path.stat().st_ino == first_inode
    assert first.artifact_type == "PHASE1A_SLICE_RECOVERY_MANIFEST"
    assert first.artifact_id == 0


def test_recovery_manifest_rejects_existing_content_tamper(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    (data_root / "derived").mkdir(parents=True)
    document = {
        "artifact_schema": "systematic_fx.phase1a_partial_recovery_manifest.v1",
        "slice_index": 0,
    }
    published = _publish_recovery_manifest(data_root=data_root, document=document)
    immutable_mode = stat.S_IMODE(published.path.stat().st_mode)
    assert immutable_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH) == 0
    published.path.chmod(immutable_mode | stat.S_IWUSR)
    try:
        published.path.write_bytes(b'{"artifact_schema":"tampered"}\n')
    finally:
        published.path.chmod(immutable_mode)
    assert stat.S_IMODE(published.path.stat().st_mode) == immutable_mode

    with pytest.raises(
        Phase1APipelineError,
        match="existing recovery manifest content drift",
    ):
        _publish_recovery_manifest(data_root=data_root, document=document)


def test_mid_recovery_missing_pattern_does_not_layer_a_second_registrar(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data"
    source_path = data_root / "derived/research_5m/source-discovery.json"
    source_path.parent.mkdir(parents=True)
    source_path.write_bytes(b"immutable discovery evidence")
    source_sha256 = hashlib.sha256(source_path.read_bytes()).hexdigest()
    source_date = date(2022, 1, 3)
    ai_spec = _ai_run_spec()
    ai_payload = ai_spec.payload()
    existing_projection = {
        "artifact_schema": "systematic_fx.phase1a_query_recovery_projection.v1",
        "owner": "first-recovery-control",
    }
    query_spec = _run_spec_from_recovery_source(
        ai_payload,
        run_kind="QUERY",
        engine_version="query-projection-v1",
        code_commit=_NEW_COMMIT,
        code_snapshot_sha256="a" * 64,
        dependency_sha256="b" * 64,
        runtime={"python": "3.12.13"},
        parameters={
            "frozen_toml_inputs": ai_payload["parameters"]["frozen_toml_inputs"],
            "recovery_projection": existing_projection,
        },
    )
    query_payload = query_spec.payload()
    query_result = {
        "definition": {"id": "Q01"},
        "source_date_count": 1,
        "support_count": 1,
    }
    discovery_document = {
        "code_snapshot_sha256": ai_payload["code_snapshot_sha256"],
        "query_results": [query_result],
        "summary": {"eligible_rows": 10, "nonzero_support_query_count": 1},
    }
    recovery_source = Phase1APartialRecoverySource(
        campaign_id=1,
        slice_index=0,
        feature_run_spec_id=11,
        feature_run_fingerprint="c" * 64,
        feature_success_attempt_id=12,
        feature_result_artifact_id=13,
        feature_canonical_spec={"kind": "feature"},
        ai_run_spec_id=21,
        ai_run_fingerprint=ai_spec.fingerprint,
        ai_success_attempt_id=22,
        ai_exposure_id=23,
        ai_canonical_spec=ai_payload,
        query_prefix=(
            Phase1ARecoveryQuerySource(
                query_id="Q01",
                research_run_spec_id=31,
                run_fingerprint=query_spec.fingerprint,
                success_attempt_id=32,
                discovery_exposure_id=33,
                canonical_spec=query_payload,
            ),
        ),
        pattern_ids=(),
        missing_pattern_query_id="Q01",
        result_artifact_id=41,
        result_artifact_uri=source_path.resolve().as_uri(),
        result_artifact_sha256=source_sha256,
        result_artifact_byte_size=source_path.stat().st_size,
    )
    prefix = Phase1ACurrentSlicePrefixReport(
        slice_index=0,
        state="RESUMABLE",
        feature_run_spec_id=11,
        ai_exposure_id=23,
        query_exposure_ids=(33,),
        pattern_ids=(),
        result_artifact_id=41,
        missing_pattern_query_id="Q01",
    )
    captured_derivations: list[dict[str, Any]] = []
    recorded_patterns: list[object] = []

    def fake_validate(*args: object, **kwargs: object) -> tuple[Path, dict[str, object]]:
        del args, kwargs
        return source_path.resolve(), discovery_document

    def fake_derive(**kwargs: Any) -> object:
        captured_derivations.append(kwargs)
        return SimpleNamespace(query_id="Q01")

    def fake_reserve_recovery(**kwargs: Any) -> tuple[PipelineRunReport, ResolvedRunArtifact]:
        manifest = kwargs["manifest"]
        run_spec = kwargs["run_spec"]
        return (
            PipelineRunReport(
                run_kind="VALIDATION",
                run_fingerprint=run_spec.fingerprint,
                research_run_spec_id=51,
                research_run_attempt_id=52,
                attempt_status="SUCCEEDED",
                executed=True,
                result_artifact_id=53,
            ),
            ResolvedRunArtifact(
                artifact_id=53,
                path=manifest.path,
                sha256=manifest.sha256,
                artifact_type="PHASE1A_SLICE_RECOVERY_MANIFEST",
            ),
        )

    monkeypatch.setattr(pipeline, "_validate_discovery_document", fake_validate)
    monkeypatch.setattr(pipeline, "derive_phase1a_pattern_observation", fake_derive)
    monkeypatch.setattr(pipeline, "_reserve_recovery_run", fake_reserve_recovery)

    final_prefix = Phase1ACurrentSlicePrefixReport(
        slice_index=0,
        state="COMPLETE",
        feature_run_spec_id=11,
        ai_exposure_id=23,
        query_exposure_ids=(33,),
        pattern_ids=(61,),
        result_artifact_id=41,
        missing_pattern_query_id=None,
    )
    services = SimpleNamespace(
        load_recovery_source=lambda *args, **kwargs: recovery_source,
        record_pattern=lambda database_url, observation: recorded_patterns.append(observation),
        verify_current_slice_prefix=lambda *args, **kwargs: final_prefix,
    )
    candidate_query = SimpleNamespace(
        query_id="Q01",
        as_dict=lambda: {"id": "Q01"},
    )

    report = _recover_phase1a_discovery_slice(
        database_url="postgresql://must-not-connect.invalid/research",
        data_root=data_root,
        slice_index=0,
        source_dates=(source_date,),
        interval_start=datetime(2022, 1, 3, tzinfo=UTC),
        interval_end=datetime(2022, 1, 3, tzinfo=UTC) + timedelta(days=1),
        prefix=prefix,
        services=services,
        campaign=SimpleNamespace(campaign_id=1, campaign_key=CAMPAIGN_ID),
        calendar=SimpleNamespace(sha256=_SHA_D),
        split=SimpleNamespace(sha256=_SHA_E),
        discovery_config=SimpleNamespace(
            sha256=_SHA_F,
            candidate_queries=(candidate_query,),
        ),
        code_commit="3" * 40,
        code_snapshot_sha256="a" * 64,
        code_snapshot_disposition="REGISTERED_REVISION",
        dependency_sha256="b" * 64,
        runtime={"python": "3.12.13"},
    )

    assert len(captured_derivations) == 1
    assert (
        captured_derivations[0]["query_run_spec"]["parameters"]["recovery_projection"]
        == existing_projection
    )
    assert captured_derivations[0]["rollup_registrar"] is None
    assert len(recorded_patterns) == 1
    assert report.pattern_observation_count == 1
    assert report.recovery_mode is True
