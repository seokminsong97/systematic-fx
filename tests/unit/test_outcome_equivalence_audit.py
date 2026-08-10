from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import systematic_fx.research.outcome_equivalence_audit as audit_module
from systematic_fx.cli import build_parser
from systematic_fx.db.outcome_registry import EXPECTED_SUMMARY_COUNT, P5_QUERY_ID
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.outcome_equivalence_audit import (
    AUDIT_DIRECTORY,
    AUDIT_SCHEMA,
    AUDIT_VERSION,
    OutcomeEquivalenceAuditError,
    OutcomeEquivalenceAuditServices,
    OutcomeEquivalenceCheckpoint,
    OutcomeEquivalenceObservation,
    OutcomeEquivalenceSubject,
    checkpoint_chain_sha256,
    compare_outcome_equivalence,
    execute_uninterrupted_p5_replay,
    outcome_equivalence_subject_from_registry,
    publish_outcome_equivalence_audit,
    run_phase1a_p5_outcome_equivalence_audit,
)
from systematic_fx.research.phase1a_outcome_pipeline import (
    OutcomeArtifactServices,
    _default_services,
)

_SHA = tuple(character * 64 for character in "abcdef0123456789")


def _checkpoints() -> tuple[OutcomeEquivalenceCheckpoint, ...]:
    first = OutcomeEquivalenceCheckpoint(
        checkpoint_sequence=1,
        checkpoint_artifact_sha256=_SHA[0],
        checkpoint_artifact_byte_size=101,
        predecessor_checkpoint_sha256=None,
        last_completed_source_date=date(2022, 1, 3),
        source_event_count=10,
        progress_metadata_sha256=canonical_sha256({"detail_record_count": 2}),
    )
    second = OutcomeEquivalenceCheckpoint(
        checkpoint_sequence=2,
        checkpoint_artifact_sha256=_SHA[1],
        checkpoint_artifact_byte_size=202,
        predecessor_checkpoint_sha256=first.checkpoint_artifact_sha256,
        last_completed_source_date=date(2022, 1, 4),
        source_event_count=25,
        progress_metadata_sha256=canonical_sha256({"detail_record_count": 4}),
    )
    return first, second


def _subject() -> OutcomeEquivalenceSubject:
    checkpoints = _checkpoints()
    return OutcomeEquivalenceSubject(
        outcome_replay_manifest_id=1,
        research_run_spec_id=2,
        research_run_attempt_id=3,
        status="SUCCEEDED",
        query_id=P5_QUERY_ID,
        outcome_config_id="phase1a_p5_outcome_replay_v1",
        run_fingerprint=_SHA[2],
        source_artifact_manifest_sha256=_SHA[3],
        cache_manifest_sha256=_SHA[4],
        result_artifact_sha256=_SHA[5],
        result_artifact_byte_size=303,
        cell_summaries_sha256=_SHA[6],
        detail_shard_manifest_sha256=_SHA[7],
        input_lineage_sha256=_SHA[8],
        final_checkpoint_sha256=checkpoints[-1].checkpoint_artifact_sha256,
        final_checkpoint_sequence=2,
        source_event_count=25,
        detail_record_count=4,
        summary_row_count=EXPECTED_SUMMARY_COUNT,
        checkpoints=checkpoints,
    )


def _observation(
    subject: OutcomeEquivalenceSubject,
    **changes: object,
) -> OutcomeEquivalenceObservation:
    values: dict[str, object] = {
        "outcome_replay_manifest_id": subject.outcome_replay_manifest_id,
        "run_fingerprint": subject.run_fingerprint,
        "cache_manifest_sha256": subject.cache_manifest_sha256,
        "result_artifact_sha256": subject.result_artifact_sha256,
        "result_artifact_byte_size": subject.result_artifact_byte_size,
        "cell_summaries_sha256": subject.cell_summaries_sha256,
        "detail_shard_manifest_sha256": subject.detail_shard_manifest_sha256,
        "input_lineage_sha256": subject.input_lineage_sha256,
        "final_checkpoint_sha256": subject.final_checkpoint_sha256,
        "final_checkpoint_sequence": subject.final_checkpoint_sequence,
        "source_event_count": subject.source_event_count,
        "detail_record_count": subject.detail_record_count,
        "summary_row_count": subject.summary_row_count,
        "checkpoints": subject.checkpoints,
        "detail_shard_publication_count": len(subject.checkpoints),
        "detail_shard_reused_count": len(subject.checkpoints),
        "checkpoint_publication_count": len(subject.checkpoints),
        "checkpoint_reused_count": len(subject.checkpoints),
        "final_result_disposition": "REUSED",
        "checkpoint_load_count": 1,
        "start_noop_count": 1,
        "complete_noop_count": 1,
    }
    values.update(changes)
    return OutcomeEquivalenceObservation(**values)  # type: ignore[arg-type]


def _data_layout(tmp_path: Path) -> Path:
    data = tmp_path / "data"
    (data / "derived").mkdir(parents=True)
    return data.resolve()


def test_checkpoint_chain_digest_requires_contiguous_hash_linked_dates() -> None:
    checkpoints = _checkpoints()

    assert checkpoint_chain_sha256(checkpoints) == canonical_sha256(
        [checkpoint.as_dict() for checkpoint in checkpoints]
    )
    broken = replace(checkpoints[1], predecessor_checkpoint_sha256=_SHA[9])
    with pytest.raises(OutcomeEquivalenceAuditError, match="predecessor chain"):
        checkpoint_chain_sha256((checkpoints[0], broken))


def test_registry_subject_adapter_derives_final_counts_and_progress_hashes() -> None:
    subject = _subject()
    raw_checkpoints = tuple(
        SimpleNamespace(
            checkpoint_sequence=item.checkpoint_sequence,
            checkpoint_artifact_sha256=item.checkpoint_artifact_sha256,
            checkpoint_artifact_byte_size=item.checkpoint_artifact_byte_size,
            predecessor_checkpoint_sha256=item.predecessor_checkpoint_sha256,
            last_completed_source_date=item.last_completed_source_date,
            source_event_count=item.source_event_count,
            progress_metadata={"detail_record_count": item.checkpoint_sequence * 2},
        )
        for item in subject.checkpoints
    )
    registry_value = SimpleNamespace(
        **{
            key: value
            for key, value in subject.as_dict().items()
            if key
            not in {
                "checkpoint_chain_sha256",
                "checkpoint_count",
                "detail_record_count",
                "source_event_count",
                "summary_row_count",
            }
        },
        checkpoints=raw_checkpoints,
    )

    adapted = outcome_equivalence_subject_from_registry(registry_value)

    assert adapted == subject
    assert adapted.checkpoint_chain_sha256 == subject.checkpoint_chain_sha256


def test_equivalence_report_is_passed_only_when_every_identity_and_reuse_matches() -> None:
    subject = _subject()

    passed = compare_outcome_equivalence(subject, _observation(subject))
    failed = compare_outcome_equivalence(
        subject,
        _observation(
            subject,
            result_artifact_sha256=_SHA[9],
            checkpoint_reused_count=1,
        ),
    )

    assert passed.passed is True
    assert passed.mismatches == ()
    assert failed.passed is False
    assert failed.mismatches == (
        "checkpoints_all_reused",
        "result_artifact_sha256",
    )


def test_audit_publication_is_canonical_content_addressed_and_idempotent(
    tmp_path: Path,
) -> None:
    data = _data_layout(tmp_path)
    subject = _subject()
    report = compare_outcome_equivalence(subject, _observation(subject))
    expected_content = canonical_json_bytes(report.as_dict()) + b"\n"
    expected_sha256 = hashlib.sha256(expected_content).hexdigest()

    first = publish_outcome_equivalence_audit(report, data_root=data)
    second = publish_outcome_equivalence_audit(report, data_root=data)

    assert first.disposition == "CREATED"
    assert second.disposition == "REUSED"
    assert first.path == second.path
    assert first.path.parent == data / "derived" / AUDIT_DIRECTORY
    assert first.path.name == f"sha256={expected_sha256}.json"
    assert first.sha256 == second.sha256 == expected_sha256
    assert first.byte_size == len(expected_content)
    assert first.path.read_bytes() == expected_content
    assert (
        stat.S_IMODE(first.path.stat().st_mode) & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH) == 0
    )
    document = json.loads(first.path.read_bytes())
    assert document["artifact_schema"] == AUDIT_SCHEMA
    assert document["audit_version"] == AUDIT_VERSION
    assert document["passed"] is True


def test_audit_publication_rejects_existing_content_or_permission_drift(
    tmp_path: Path,
) -> None:
    data = _data_layout(tmp_path)
    subject = _subject()
    report = compare_outcome_equivalence(subject, _observation(subject))
    published = publish_outcome_equivalence_audit(report, data_root=data)
    immutable_mode = stat.S_IMODE(published.path.stat().st_mode)
    published.path.chmod(immutable_mode | stat.S_IWUSR)
    try:
        with pytest.raises(
            OutcomeEquivalenceAuditError,
            match="identity or content",
        ):
            publish_outcome_equivalence_audit(report, data_root=data)
    finally:
        published.path.chmod(immutable_mode)


def test_registered_audit_loader_recomputes_comparisons_and_rejects_forged_pass(
    tmp_path: Path,
) -> None:
    data = _data_layout(tmp_path)
    subject = _subject()
    document = compare_outcome_equivalence(subject, _observation(subject)).as_dict()
    observed = document["observed"]
    assert isinstance(observed, dict)
    observed["checkpoint_load_count"] = 0
    # Deliberately leave the attacker-supplied comparison marked true.
    comparisons = document["comparisons"]
    assert isinstance(comparisons, dict)
    assert comparisons["forced_uninterrupted_start"] is True
    content = canonical_json_bytes(document) + b"\n"
    digest = hashlib.sha256(content).hexdigest()
    directory = data / "derived" / AUDIT_DIRECTORY
    directory.mkdir(parents=True)
    path = directory / f"sha256={digest}.json"
    path.write_bytes(content)
    path.chmod(0o444)

    with pytest.raises(OutcomeEquivalenceAuditError, match="semantic content drift"):
        audit_module.load_outcome_equivalence_audit(
            data_root=data,
            expected_sha256=digest,
            subject=subject,
        )


def test_audit_publication_rejects_symlinked_namespace_without_writing_outside(
    tmp_path: Path,
) -> None:
    data = _data_layout(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (data / "derived" / "outcomes").symlink_to(
        outside,
        target_is_directory=True,
    )
    subject = _subject()
    report = compare_outcome_equivalence(subject, _observation(subject))

    with pytest.raises(
        OutcomeEquivalenceAuditError,
        match="securely open the audit namespace",
    ):
        publish_outcome_equivalence_audit(report, data_root=data)

    assert tuple(outside.iterdir()) == ()


def test_registered_audit_loader_rejects_exact_content_through_leaf_symlink(
    tmp_path: Path,
) -> None:
    data = _data_layout(tmp_path)
    subject = _subject()
    report = compare_outcome_equivalence(subject, _observation(subject))
    published = publish_outcome_equivalence_audit(report, data_root=data)
    outside = tmp_path / "outside-audit.json"
    outside.write_bytes(published.path.read_bytes())
    outside.chmod(0o444)
    published.path.unlink()
    published.path.symlink_to(outside)

    with pytest.raises(
        OutcomeEquivalenceAuditError,
        match="cannot securely open the audit artifact",
    ):
        audit_module.load_outcome_equivalence_audit(
            data_root=data,
            expected_sha256=published.sha256,
            subject=subject,
        )

    assert outside.read_bytes() == canonical_json_bytes(report.as_dict()) + b"\n"


def test_audit_publication_fails_closed_when_namespace_is_swapped_after_link(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data = _data_layout(tmp_path)
    subject = _subject()
    report = compare_outcome_equivalence(subject, _observation(subject))
    audit_directory = data / "derived" / AUDIT_DIRECTORY
    moved_directory = audit_directory.with_name(f"{audit_directory.name}.held")
    outside = tmp_path / "outside"
    outside.mkdir()
    real_link = os.link
    swapped = False

    def swap_after_link(*args: object, **kwargs: object) -> None:
        nonlocal swapped
        real_link(*args, **kwargs)  # type: ignore[arg-type]
        audit_directory.rename(moved_directory)
        audit_directory.symlink_to(outside, target_is_directory=True)
        swapped = True

    monkeypatch.setattr(audit_module.os, "link", swap_after_link)
    try:
        with pytest.raises(
            OutcomeEquivalenceAuditError,
            match="path identity disappeared|directory identity changed",
        ):
            publish_outcome_equivalence_audit(report, data_root=data)
    finally:
        if swapped:
            audit_directory.unlink()
            moved_directory.rename(audit_directory)

    assert tuple(outside.iterdir()) == ()


def test_registered_audit_loader_detects_leaf_swap_after_descriptor_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data = _data_layout(tmp_path)
    subject = _subject()
    report = compare_outcome_equivalence(subject, _observation(subject))
    published = publish_outcome_equivalence_audit(report, data_root=data)
    backup = published.path.with_name(f".{published.path.name}.backup")
    os.link(published.path, backup)
    outside = tmp_path / "outside-audit.json"
    outside.write_bytes(published.path.read_bytes())
    outside.chmod(0o444)
    real_open = os.open
    swapped = False

    def swap_after_open(
        path: os.PathLike[str] | str,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal swapped
        descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
        if path == published.path.name and dir_fd is not None and not swapped:
            os.unlink(path, dir_fd=dir_fd)
            os.symlink(outside, path, dir_fd=dir_fd)
            swapped = True
        return descriptor

    monkeypatch.setattr(audit_module.os, "open", swap_after_open)
    try:
        with pytest.raises(
            OutcomeEquivalenceAuditError,
            match="identity or content changed while held",
        ):
            audit_module.load_outcome_equivalence_audit(
                data_root=data,
                expected_sha256=published.sha256,
                subject=subject,
            )
    finally:
        if swapped:
            published.path.unlink()
            backup.rename(published.path)

    assert outside.read_bytes() == canonical_json_bytes(report.as_dict()) + b"\n"


def test_uninterrupted_executor_forces_empty_checkpoint_and_reuses_every_artifact(
    tmp_path: Path,
) -> None:
    data = _data_layout(tmp_path)
    subject = _subject()
    progress_by_sequence = {
        1: {"detail_record_count": 2},
        2: {"detail_record_count": 4},
    }
    checkpoints = subject.checkpoints
    checkpoint_cursor = 0
    shard_cursor = 0

    def publish_shard(*args: object, **kwargs: object) -> object:
        nonlocal shard_cursor
        del args, kwargs
        shard_cursor += 1
        return SimpleNamespace(
            path=data / f"derived/shard-{shard_cursor}.parquet",
            sha256=_SHA[10 + shard_cursor],
            byte_size=50,
            disposition="REUSED",
            row_count=2,
        )

    def publish_checkpoint(*args: object, **kwargs: object) -> object:
        nonlocal checkpoint_cursor
        del args
        checkpoint_cursor += 1
        checkpoint = checkpoints[checkpoint_cursor - 1]
        assert kwargs["checkpoint_sequence"] == checkpoint_cursor
        return SimpleNamespace(
            path=(data / "derived" / f"sha256={checkpoint.checkpoint_artifact_sha256}.json"),
            sha256=checkpoint.checkpoint_artifact_sha256,
            byte_size=checkpoint.checkpoint_artifact_byte_size,
            disposition="REUSED",
            checkpoint_sequence=checkpoint.checkpoint_sequence,
            last_completed_source_date=checkpoint.last_completed_source_date,
            progress_metadata=progress_by_sequence[checkpoint_cursor],
        )

    final_artifact = SimpleNamespace(
        path=data / f"derived/sha256={subject.result_artifact_sha256}.json",
        sha256=subject.result_artifact_sha256,
        byte_size=subject.result_artifact_byte_size,
        disposition="REUSED",
    )

    def publish_result(*args: object, **kwargs: object) -> object:
        del args, kwargs
        return final_artifact

    artifacts = OutcomeArtifactServices(
        find_cache_manifest=lambda *args, **kwargs: None,
        publish_cache_manifest=lambda *args, **kwargs: None,
        publish_result_shard=publish_shard,
        read_result_shard=lambda *args, **kwargs: None,
        publish_checkpoint=publish_checkpoint,
        load_checkpoint_artifact=lambda *args, **kwargs: None,
        publish_result=publish_result,
        load_result=lambda *args, **kwargs: None,
    )
    pipeline_services = replace(_default_services(), artifacts=artifacts)
    prepared = SimpleNamespace(config=SimpleNamespace(query_id=P5_QUERY_ID))
    run_spec = SimpleNamespace(fingerprint=subject.run_fingerprint)

    def fake_run_replay(**kwargs: Any) -> tuple[object, object, int, int, int, int]:
        services = kwargs["services"]
        database_url = kwargs["database_url"]
        manifest_id = kwargs["reservation"].outcome_replay_manifest_id
        fingerprint = kwargs["run_spec"].fingerprint
        assert (
            services.start_replay(
                database_url,
                outcome_replay_manifest_id=manifest_id,
                run_fingerprint=fingerprint,
            )
            == subject
        )
        assert (
            services.load_checkpoint(
                database_url,
                outcome_replay_manifest_id=manifest_id,
                run_fingerprint=fingerprint,
                data_root=data,
            )
            is None
        )
        predecessor = None
        final_checkpoint: object | None = None
        for sequence, checkpoint in enumerate(checkpoints, start=1):
            services.artifacts.publish_result_shard(
                (),
                data_root=data,
                run_fingerprint=fingerprint,
                shard_sequence=sequence,
                source_date=checkpoint.last_completed_source_date,
            )
            final_checkpoint = services.artifacts.publish_checkpoint(
                data_root=data,
                outcome_replay_manifest_id=manifest_id,
                run_fingerprint=fingerprint,
                checkpoint_sequence=sequence,
                completed_source_date_count=sequence,
                last_completed_source_date=checkpoint.last_completed_source_date,
                source_event_count=checkpoint.source_event_count,
                predecessor_checkpoint_sha256=predecessor,
            )
            registered = services.register_checkpoint(
                database_url,
                outcome_replay_manifest_id=manifest_id,
                run_fingerprint=fingerprint,
                checkpoint_sequence=sequence,
                checkpoint_artifact_path=final_checkpoint.path,
            )
            predecessor = registered.checkpoint_artifact_sha256
        published_result = services.artifacts.publish_result(
            data_root=data,
            run_fingerprint=fingerprint,
        )
        result = SimpleNamespace(
            artifact=published_result,
            document={
                "cache_manifest": {"artifact_sha256": subject.cache_manifest_sha256},
                "cell_summaries_sha256": subject.cell_summaries_sha256,
                "detail_shard_manifest_sha256": subject.detail_shard_manifest_sha256,
                "input_lineage_sha256": subject.input_lineage_sha256,
            },
        )
        services.complete_replay(
            database_url,
            outcome_replay_manifest_id=manifest_id,
            run_fingerprint=fingerprint,
        )
        assert final_checkpoint is not None
        return result, final_checkpoint, 2, 25, 4, EXPECTED_SUMMARY_COUNT

    observed = execute_uninterrupted_p5_replay(
        subject=subject,
        prepared=prepared,  # type: ignore[arg-type]
        reports=(),
        terminal_resolution=SimpleNamespace(),
        cache_manifest=SimpleNamespace(sha256=subject.cache_manifest_sha256),
        run_spec=run_spec,
        database_url="postgresql://must-not-connect.invalid/research",
        data_root=data,
        pipeline_services=pipeline_services,
        run_replay=fake_run_replay,
    )
    report = compare_outcome_equivalence(subject, observed)

    assert report.passed is True
    assert observed.checkpoint_load_count == 1
    assert observed.start_noop_count == observed.complete_noop_count == 1
    assert observed.detail_shard_reused_count == 2
    assert observed.checkpoint_reused_count == 2
    assert observed.final_result_disposition == "REUSED"


@pytest.mark.parametrize(
    (
        "execute",
        "expected_disposition",
        "drift_after_replay",
        "database_drift_after_replay",
        "preflight_existing",
    ),
    (
        (True, "SUCCEEDED", False, False, False),
        (False, "SKIPPED_DUPLICATE", False, False, False),
        (True, None, True, False, False),
        pytest.param(
            True,
            None,
            False,
            True,
            False,
            id="forged-mid-run-db-migration",
        ),
        (True, "SKIPPED_DUPLICATE", False, False, True),
    ),
)
def test_governed_runner_binds_dirty_provenance_and_safely_reuses_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    execute: bool,
    expected_disposition: str | None,
    drift_after_replay: bool,
    database_drift_after_replay: bool,
    preflight_existing: bool,
) -> None:
    project = tmp_path / "project"
    data = project / "data"
    (data / "mbp-10").mkdir(parents=True)
    (data / "derived/manifests").mkdir(parents=True)
    subject = _subject()
    screening = SimpleNamespace(
        outcome_version="phase1a_barrier_outcome_v1",
        barrier_grid=SimpleNamespace(sha256=_SHA[9]),
        cost_version="phase1a_conservative_cost_v1",
        cost=SimpleNamespace(sha256=_SHA[10]),
        execution_version="phase1a_conservative_execution_v1",
        execution=SimpleNamespace(sha256=_SHA[11]),
    )
    prepared = SimpleNamespace(
        config=SimpleNamespace(
            query_id=P5_QUERY_ID,
            outcome_config_id="phase1a_p5_outcome_replay_v1",
            screening_bundle=screening,
        ),
        calendar=SimpleNamespace(sha256=_SHA[12]),
        split=SimpleNamespace(sha256=_SHA[13]),
        source_artifacts=SimpleNamespace(
            source_artifact_manifest_sha256=(subject.source_artifact_manifest_sha256)
        ),
        plan=SimpleNamespace(cache_plan_sha256=_SHA[14]),
        discovery=SimpleNamespace(input_manifest_sha256=_SHA[15]),
    )
    cache_manifest = SimpleNamespace(
        sha256=subject.cache_manifest_sha256,
        reports=(SimpleNamespace(), SimpleNamespace()),
    )
    provenance_calls: list[str] = []
    registered_specs: list[object] = []
    started_attempts: list[int] = []
    failed_attempts: list[int] = []
    replay_calls: list[object] = []
    reserve_calls: list[str] = []
    postgres_runtime_calls: list[int] = []

    def build_snapshot(path: Path, *, code_commit: str) -> object:
        assert path == project.resolve()
        assert code_commit == "1" * 40
        provenance_calls.append("snapshot")
        digest = _SHA[4] if drift_after_replay and len(provenance_calls) == 3 else _SHA[0]
        return SimpleNamespace(sha256=digest)

    defaults = _default_services()
    artifacts = replace(
        defaults.artifacts,
        find_cache_manifest=lambda *args, **kwargs: cache_manifest,
    )
    shared_postgres_runtime = {
        "schema_migrations": [
            {
                "checksum": _SHA[5],
                "name": "phase1a_outcome_audit_lineage_hardening",
                "version": 19,
            }
        ],
        "schema_migrations_sha256": _SHA[6],
        "server_version": "18.4-test",
        "server_version_num": "180004",
    }

    def postgres_runtime(*args: object, **kwargs: object) -> object:
        del args, kwargs
        postgres_runtime_calls.append(len(postgres_runtime_calls) + 1)
        if database_drift_after_replay and len(postgres_runtime_calls) == 3:
            # Return the same mutable object used by the initial capture.  A
            # shallow/reference capture would therefore accept this forged
            # mid-run migration identity instead of failing closed.
            migrations = shared_postgres_runtime["schema_migrations"]
            assert isinstance(migrations, list)
            migrations.append(
                {
                    "checksum": _SHA[7],
                    "name": "publication_run_progress",
                    "version": 20,
                }
            )
            shared_postgres_runtime["schema_migrations_sha256"] = _SHA[8]
        return shared_postgres_runtime

    pipeline_services = replace(
        defaults,
        git_head=lambda path: "1" * 40,
        build_snapshot=build_snapshot,
        publish_snapshot=lambda snapshot, **kwargs: SimpleNamespace(sha256=snapshot.sha256),
        dependency_hash=lambda path: _SHA[1],
        runtime=lambda: {"python": "3.12-test"},
        postgres_runtime=postgres_runtime,
        artifacts=artifacts,
    )
    monkeypatch.setattr(audit_module, "_prepare_inputs", lambda **kwargs: prepared)
    monkeypatch.setattr(
        audit_module,
        "_validate_cache_reports",
        lambda plan, reports: tuple(reports),
    )
    monkeypatch.setattr(
        audit_module,
        "_resolve_terminals",
        lambda plan, reports: SimpleNamespace(sha256=_SHA[2]),
    )
    monkeypatch.setattr(
        audit_module,
        "load_phase1a_screening_config",
        lambda path: SimpleNamespace(sha256=_SHA[3]),
    )

    def register_spec(database_url: str, run_spec: object) -> object:
        assert database_url == "postgresql://synthetic/research"
        registered_specs.append(run_spec)
        return SimpleNamespace(research_run_spec_id=12)

    reservation = SimpleNamespace(
        execute=execute,
        research_run_attempt_id=14 if execute else 15,
        reused_attempt_id=None if execute else 14,
    )
    existing = None
    if not execute or preflight_existing:
        existing = publish_outcome_equivalence_audit(
            compare_outcome_equivalence(subject, _observation(subject)),
            data_root=data,
        )
    gate = SimpleNamespace(
        equivalence_audit_id=77,
        equivalence_audit_artifact_sha256=(None if existing is None else existing.sha256),
        predecessor_outcome_replay_manifest_id=subject.outcome_replay_manifest_id,
        predecessor_run_fingerprint=subject.run_fingerprint,
        predecessor_result_artifact_sha256=subject.result_artifact_sha256,
        predecessor_input_lineage_sha256=subject.input_lineage_sha256,
        predecessor_cell_summaries_sha256=subject.cell_summaries_sha256,
        predecessor_detail_shard_manifest_sha256=(subject.detail_shard_manifest_sha256),
        predecessor_final_checkpoint_sha256=subject.final_checkpoint_sha256,
    )

    def loaded_audit(validation_run_fingerprint: str) -> object:
        assert existing is not None
        return SimpleNamespace(
            audit=SimpleNamespace(
                outcome_equivalence_audit_id=77,
                predecessor_outcome_replay_manifest_id=(subject.outcome_replay_manifest_id),
                validation_research_run_spec_id=12,
                validation_research_run_attempt_id=14,
                validation_run_fingerprint=validation_run_fingerprint,
                audit_artifact_sha256=existing.sha256,
                checkpoint_chain_sha256=subject.checkpoint_chain_sha256,
                passed=True,
            ),
            predecessor_gate=gate,
            audit_artifact_path=existing.path,
        )

    def reserve_attempt(database_url: str, *, run_fingerprint: str) -> object:
        assert database_url == "postgresql://synthetic/research"
        reserve_calls.append(run_fingerprint)
        return reservation

    def fake_execute(**kwargs: object) -> OutcomeEquivalenceObservation:
        replay_calls.append(kwargs["run_spec"])
        return _observation(subject)

    monkeypatch.setattr(
        audit_module,
        "execute_uninterrupted_p5_replay",
        fake_execute,
    )
    audit_services = OutcomeEquivalenceAuditServices(
        load_subject=lambda *args, **kwargs: subject,
        register_audit=lambda *args, **kwargs: SimpleNamespace(outcome_equivalence_audit_id=77),
        register_spec=register_spec,
        reserve_attempt=reserve_attempt,
        start_attempt=lambda database_url, *, research_run_attempt_id: started_attempts.append(
            research_run_attempt_id
        ),
        finish_attempt=lambda database_url, *, research_run_attempt_id, **kwargs: (
            failed_attempts.append(research_run_attempt_id)
        ),
        find_subject_audit=lambda *args, **kwargs: (
            loaded_audit(_SHA[9]) if preflight_existing else None
        ),
        load_attempt_audit=lambda *args, **kwargs: loaded_audit(registered_specs[0].fingerprint),
        publish_audit=publish_outcome_equivalence_audit,
        load_audit=audit_module.load_outcome_equivalence_audit,
    )

    arguments = {
        "project_root": project,
        "data_root": data,
        "database_url": "postgresql://synthetic/research",
        "pipeline_services": pipeline_services,
        "services": audit_services,
    }
    if drift_after_replay or database_drift_after_replay:
        expected_error = (
            "code snapshot, dependency lock, or Git identity changed"
            if drift_after_replay
            else "PostgreSQL runtime/schema_migrations identity changed"
        )
        with pytest.raises(
            OutcomeEquivalenceAuditError,
            match=expected_error,
        ):
            run_phase1a_p5_outcome_equivalence_audit(**arguments)  # type: ignore[arg-type]
        assert provenance_calls == ["snapshot", "snapshot", "snapshot"]
        assert postgres_runtime_calls == ([1, 2] if drift_after_replay else [1, 2, 3])
        assert started_attempts == [14]
        assert failed_attempts == [14]
        assert len(replay_calls) == 1
        assert not (data / "derived" / AUDIT_DIRECTORY).exists()
        return

    report = run_phase1a_p5_outcome_equivalence_audit(**arguments)  # type: ignore[arg-type]

    assert report.disposition == expected_disposition
    assert report.passed is True
    assert report.outcome_equivalence_audit_id == 77
    assert report.audit_artifact_path.is_file()
    if preflight_existing:
        assert report.validation_run_fingerprint == _SHA[9]
        assert report.validation_research_run_spec_id == 12
        assert report.validation_research_run_attempt_id == 14
        assert report.reused_validation_attempt_id == 14
        assert registered_specs == []
        assert reserve_calls == []
        assert provenance_calls == []
        assert postgres_runtime_calls == []
        assert started_attempts == []
        assert failed_attempts == []
        assert replay_calls == []
        return
    assert len(registered_specs) == 1
    validation_spec = registered_specs[0]
    assert validation_spec.run_kind == "VALIDATION"
    assert validation_spec.engine_version == "phase1a_outcome_equivalence_audit_v1"
    assert validation_spec.parameters["checkpoint_chain_sha256"] == (
        subject.checkpoint_chain_sha256
    )
    # Initial capture plus the immediate pre-registration recheck.  A real
    # execution performs the third post-replay recheck before PASSED.
    assert len(provenance_calls) == (3 if execute else 2)
    assert len(postgres_runtime_calls) == (3 if execute else 2)
    assert len(reserve_calls) == 1
    assert failed_attempts == []
    if execute:
        assert started_attempts == [14]
        assert len(replay_calls) == 1
        assert replay_calls[0].fingerprint == subject.run_fingerprint
    else:
        assert started_attempts == []
        assert replay_calls == []
        assert report.reused_validation_attempt_id == 14


def test_cli_exposes_governed_p5_equivalence_audit_arguments() -> None:
    args = build_parser().parse_args(
        [
            "research",
            "phase1a-p5-equivalence-audit",
            "--outcome-replay-manifest-id",
            "17",
            "--database-url",
            "postgresql://synthetic/research",
            "--json",
        ]
    )

    assert args.outcome_replay_manifest_id == 17
    assert args.database_url == "postgresql://synthetic/research"
    assert args.json is True
    assert args.handler.__name__ == "_phase1a_p5_equivalence_audit_command"


def test_cli_runs_governed_audit_and_emits_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}

    def fake_runner(**kwargs: object) -> object:
        captured.update(kwargs)
        return SimpleNamespace(
            passed=True,
            as_dict=lambda: {
                "disposition": "SKIPPED_DUPLICATE",
                "passed": True,
            },
        )

    monkeypatch.setattr(
        audit_module,
        "run_phase1a_p5_outcome_equivalence_audit",
        fake_runner,
    )
    monkeypatch.setenv("SYSTEMATIC_FX_DATA_ROOT", str(tmp_path / "data"))
    args = build_parser().parse_args(
        [
            "research",
            "phase1a-p5-equivalence-audit",
            "--outcome-replay-manifest-id",
            "17",
            "--database-url",
            "postgresql://synthetic/research",
            "--json",
        ]
    )

    assert args.handler(args) == 0
    assert json.loads(capsys.readouterr().out) == {
        "disposition": "SKIPPED_DUPLICATE",
        "passed": True,
    }
    assert captured["database_url"] == "postgresql://synthetic/research"
    assert captured["data_root"] == tmp_path / "data"
    assert captured["outcome_replay_manifest_id"] == 17
    assert callable(captured["progress_callback"])
