from __future__ import annotations

import hashlib
import json
import stat
import threading
import tomllib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import pytest

from systematic_fx import cli
from systematic_fx.db.m0b_registry import M0bRegistryError, _validate_work_binding
from systematic_fx.db.m0b_worker_registry import (
    M0bEpochRuntimeIdentity,
    M0bWorkerClaim,
    M0bWorkerRegistryError,
    load_m0b_epoch_runtime_identity,
)
from systematic_fx.research.hypotheses import canonical_json_bytes
from systematic_fx.research.m0b.first_passage_store import (
    FirstPassageStoreError,
    FirstPassageStoreSpec,
    _label_key,
    build_first_passage_store,
    load_first_passage_store,
)
from systematic_fx.research.m0b.model import ArtifactIdentity, RealSliceBuild
from systematic_fx.research.m0b.runner import (
    M0bControlPlaneReplayError,
    M0bRunnerError,
    M0bRuntimeCodeIdentityError,
    _database_identity_sha256,
    _runtime_project_root,
    _verify_runtime_code_identity,
    run_claimed_worker_cycle,
)
from systematic_fx.research.m0b.store_config import load_first_passage_store_config
from systematic_fx.research.m0b.worker import (
    CandidateJob,
    CandidateWorkSpec,
    M0bWorkerError,
    NumericAdmissionRules,
    VolatilityBarrierSpec,
    WorkerAttempt,
    _validate_executable_label,
    load_candidate_work_artifact,
    load_candidate_work_manifest,
    publish_candidate_work_manifest,
    publish_signal_artifact,
    run_bounded_daemon_cycle,
    run_candidate_work,
)
from systematic_fx.research.m0b.worker_db import (
    M0bCheckpointPublicationError,
    M0bTerminalPublicationError,
    PostgresWorkerObserver,
)
from systematic_fx.research.provenance import build_code_snapshot
from systematic_fx.research.run_spec import RunSpec

PROJECT = Path(__file__).resolve().parents[2]
STORE_CONFIG = PROJECT / "configs/research/m0b_first_passage_store_v1.toml"
WORKER_FIXTURE = PROJECT / "tests/fixtures/m0b_worker_precommit_v1.toml"
WORKER_FIXTURE_SHA256 = "656f6a38178ad35115a643865fd5a7b96ebc9205b6fe0b29709fd99e696ec38f"


def _fixture() -> dict[str, object]:
    payload = WORKER_FIXTURE.read_bytes()
    assert hashlib.sha256(payload).hexdigest() == WORKER_FIXTURE_SHA256
    return tomllib.loads(payload.decode("utf-8"))


def _label(
    *,
    event_ts_ns: int,
    session_id: str,
    outcome: str,
    entry_ts_ns: int,
    exit_ts_ns: int,
    net_pnl_ticks: int,
) -> dict[str, object]:
    return {
        "artifact_schema": "systematic_fx.m0b_quote_label.v1",
        "barrier_id": "tp3of4_sl2of4_h1800",
        "direction": "LONG",
        "entry_eligible": True,
        "entry_price_ticks": 100,
        "entry_ts_ns": entry_ts_ns,
        "event_ts_ns": event_ts_ns,
        "exit_price_ticks": 100 + net_pnl_ticks + 2,
        "exit_ts_ns": exit_ts_ns,
        "first_touch_ts_ns": exit_ts_ns,
        "first_touch_type": outcome,
        "instrument_id": 1,
        "k_sl_den": 4,
        "k_sl_num": 2,
        "k_tp_den": 4,
        "k_tp_num": 3,
        "label_version": "fixture_label_v1",
        "max_hold_seconds": 1800,
        "mechanical_outcome_valid": True,
        "net_pnl_ticks": net_pnl_ticks,
        "parent_feature_manifest_sha256": "f" * 64,
        "session_id": session_id,
        "timeout": False,
    }


def _store_and_work(root: Path):
    fixture = _fixture()
    identity = fixture["identity"]
    signal_expected = fixture["signal_artifact"]
    evaluation = fixture["evaluation"]
    rule_values = fixture["admission_rules"]
    signal_values = fixture["signals"]
    assert isinstance(identity, dict)
    assert isinstance(signal_expected, dict)
    assert isinstance(evaluation, dict)
    assert isinstance(rule_values, dict)
    assert isinstance(signal_values, list)

    labels = [
        _label(
            event_ts_ns=100,
            session_id="D1",
            outcome="TP_FIRST",
            entry_ts_ns=101,
            exit_ts_ns=130,
            net_pnl_ticks=3,
        ),
        _label(
            event_ts_ns=110,
            session_id="D1",
            outcome="TP_FIRST",
            entry_ts_ns=111,
            exit_ts_ns=150,
            net_pnl_ticks=3,
        ),
        _label(
            event_ts_ns=200,
            session_id="D2",
            outcome="SL_FIRST",
            entry_ts_ns=201,
            exit_ts_ns=210,
            net_pnl_ticks=-1,
        ),
        _label(
            event_ts_ns=300,
            session_id="D3",
            outcome="TP_FIRST",
            entry_ts_ns=301,
            exit_ts_ns=310,
            net_pnl_ticks=4,
        ),
    ]
    label_bytes = b"".join(canonical_json_bytes(row) + b"\n" for row in labels)
    label_sha256 = hashlib.sha256(label_bytes).hexdigest()
    label_uri = f"label-{label_sha256}.jsonl"
    root.mkdir()
    (root / label_uri).write_bytes(label_bytes)
    (root / label_uri).chmod(0o444)
    build = RealSliceBuild(
        slice_id="fixture-slice-v1",
        config_hash="d" * 64,
        source_manifest=ArtifactIdentity("SOURCE", 1, "a" * 64, None, "source.json"),
        quote_manifest=ArtifactIdentity("QUOTE_1S", 4, "b" * 64, "a" * 64, "quote.jsonl"),
        feature_manifest=ArtifactIdentity("FEATURE", 4, "f" * 64, "b" * 64, "feature.jsonl"),
        label_manifest=ArtifactIdentity("LABEL", 4, label_sha256, "f" * 64, label_uri),
        sessions=(),
    )
    store_spec = FirstPassageStoreSpec(
        slice_id=build.slice_id,
        real_slice_build_sha256=build.sha256,
        label_artifact_sha256=label_sha256,
        feature_artifact_sha256="f" * 64,
        label_row_count=4,
        label_version="fixture_label_v1",
        shard_row_target=2,
        max_rows=4,
    )
    store = build_first_passage_store(
        store_spec,
        build,
        staged_root=root,
        output_root=root,
    )
    manifest = root / f"first-passage-store-{store.sha256}.json"
    signal_rows = [
        {
            "artifact_schema": "systematic_fx.m0b_candidate_signal.v1",
            "candidate_sha256": identity["candidate_sha256"],
            "event_ts_ns": row["event_ts_ns"],
            "feature_sha256": identity["feature_sha256"],
            "instrument_id": row["instrument_id"],
            "search_fold": row["search_fold"],
            "session_id": row["session_id"],
        }
        for row in signal_values
    ]
    signals = publish_signal_artifact(
        root,
        candidate_sha256=identity["candidate_sha256"],
        feature_sha256=identity["feature_sha256"],
        rows=signal_rows,
        max_signals=evaluation["max_signals"],
        search_fold_count=evaluation["search_fold_count"],
    )
    assert signals.content_sha256 == signal_expected["content_sha256"]
    assert signals.byte_size == signal_expected["byte_size"]
    rules = NumericAdmissionRules(
        min_raw_events=rule_values["min_raw_events"],
        min_flat_trades=rule_values["min_flat_trades"],
        min_sequential_trades=rule_values["min_sequential_trades"],
        min_active_days=rule_values["min_active_days"],
        min_tp_probability_ppm=rule_values["min_tp_probability_ppm"],
        require_positive_net_ev=rule_values["require_positive_net_ev"],
        min_positive_search_folds=rule_values["min_positive_search_folds"],
        max_stressed_cost_ev_floor_ticks=rule_values["max_stressed_cost_ev_floor_ticks"],
    )
    work = CandidateWorkSpec(
        epoch_sha256=identity["epoch_sha256"],
        candidate_sha256=identity["candidate_sha256"],
        first_passage_store_sha256=store.sha256,
        signals=signals,
        candidate_kind=evaluation["candidate_kind"],
        direction=evaluation["direction"],
        barrier=VolatilityBarrierSpec(
            barrier_id=evaluation["barrier_id"],
            k_tp_num=evaluation["k_tp_num"],
            k_tp_den=evaluation["k_tp_den"],
            k_sl_num=evaluation["k_sl_num"],
            k_sl_den=evaluation["k_sl_den"],
            max_hold_seconds=evaluation["max_hold_seconds"],
        ),
        cooldown_ns=evaluation["cooldown_ns"],
        stress_extra_cost_ticks=evaluation["stress_extra_cost_ticks"],
        search_fold_count=evaluation["search_fold_count"],
        max_signals=evaluation["max_signals"],
        max_trades=evaluation["max_trades"],
        checkpoint_shard_interval=evaluation["checkpoint_shard_interval"],
        deterministic_seed=evaluation["deterministic_seed"],
        code_snapshot_sha256=identity["code_snapshot_sha256"],
        cost_sha256="4" * 64,
        execution_sha256="5" * 64,
        split_sha256="b" * 64,
        admission_rules=rules,
    )
    work_uri = publish_candidate_work_manifest(root, work)
    loaded_work = load_candidate_work_manifest(root / work_uri)
    assert loaded_work == work
    loaded_artifact = load_candidate_work_artifact(root / work_uri)
    assert loaded_artifact.work == work
    assert loaded_artifact.content_sha256 == work.sha256
    assert loaded_artifact.byte_size == len(canonical_json_bytes(work.as_dict()))
    return manifest, store, loaded_work


def _run_spec_for_work(artifact, *, work_sha256: str | None = None) -> RunSpec:
    work = artifact.work
    return RunSpec(
        campaign_id="m0b_fixture_campaign",
        experiment_id="m0b_fixture_experiment",
        run_kind="SCREEN",
        engine_version="m0b_fixture_engine_v1",
        source_manifest_hashes={"dataset": artifact.source_build_sha256},
        eligible_calendar_version="fixture_calendar_v1",
        eligible_calendar_sha256="a" * 64,
        split_version="fixture_split_v1",
        split_sha256="b" * 64,
        feature_version="fixture_feature_v1",
        feature_sha256=artifact.source_feature_sha256,
        outcome_version="fixture_label_v1",
        outcome_sha256=artifact.source_label_sha256,
        cost_version="fixture_cost_v1",
        cost_sha256="4" * 64,
        execution_version="fixture_execution_v1",
        execution_sha256="5" * 64,
        code_commit="6" * 40,
        code_snapshot_sha256=work.code_snapshot_sha256,
        dependency_lock_sha256="7" * 64,
        runtime_environment={"fixture": 1},
        random_seed=work.deterministic_seed,
        direction=work.direction,
        signal_policy={"family": "pullback_continuation_v1"},
        entry_policy={"latency": "next_quote"},
        barrier_policy=work.barrier.as_dict(),
        terminal_policy={"session_policy": "NO_CROSS_CLOSED_MARKET"},
        parameters={
            "data_role": "SEARCH",
            "split_role": "DISCOVERY",
            "m0b_epoch_sha256": work.epoch_sha256,
            "m0b_dataset_sha256": artifact.source_build_sha256,
            "m0b_contract_reference_sha256": "8" * 64,
            "m0b_candidate_sha256": work.candidate_sha256,
            "m0b_barrier_sha256": work.barrier.sha256,
            "m0b_evaluation_policy_sha256": work.evaluation_policy_sha256,
            "m0b_work_spec_sha256": work.sha256 if work_sha256 is None else work_sha256,
        },
    )


def test_checked_in_store_config_binds_exact_actual_lineage() -> None:
    config = load_first_passage_store_config(STORE_CONFIG)
    assert config.file_sha256 == "93a9120661b4c11a6a05e36c3d5ca24da3005918635c25a041179b61479da6d7"
    assert (
        config.semantic_sha256 == "78b9322ee81457f7af85f23a48974b26b9add4f09b68699ed463c7f2c24e432f"
    )
    assert config.store_spec.real_slice_build_sha256 == (
        "17f4ccdcb839c70bfdd95c9d00a2b37ca6d31fff89c34439a2adcaac4c32cf5f"
    )
    assert config.store_spec.label_artifact_sha256 == (
        "6c1f2df18eecaea8ec398b0ac44c4b2728333c15e23f1a1bbce7d38b6b145fb4"
    )
    assert config.store_spec.feature_artifact_sha256 == (
        "90cc332da98672d641233e23b66d8b7cc5b60d943c1b2479df8522b130ecba62"
    )
    assert config.store_spec.label_row_count == 7776
    assert config.store_spec.shard_row_target == 1080
    assert config.expected_store_sha256 == (
        "18670ad2e98e5a18fb3ad7c18f2768e60c9ca2560e6a3dd7ac0e7dc2fe6f5ab4"
    )
    config.verify_unchanged()


def test_first_passage_store_is_group_preserving_and_reconciles_bytes(tmp_path: Path) -> None:
    manifest, store, _ = _store_and_work(tmp_path / "store")
    assert [shard.row_count for shard in store.shards] == [2, 2]
    assert store.shards[0].last_event_key < store.shards[1].first_event_key
    assert load_first_passage_store(manifest) == store

    shard = tmp_path / "store" / store.shards[0].relative_uri
    shard.chmod(0o644)
    shard.write_bytes(shard.read_bytes() + b"{}\n")
    with pytest.raises(FirstPassageStoreError, match="shard bytes differ"):
        load_first_passage_store(manifest)


def test_first_passage_label_barrier_id_binds_exact_integer_fields() -> None:
    row = _label(
        event_ts_ns=100,
        session_id="D1",
        outcome="TP_FIRST",
        entry_ts_ns=101,
        exit_ts_ns=130,
        net_pnl_ticks=3,
    )
    assert _label_key(row)[-1] == "tp3of4_sl2of4_h1800"
    row["barrier_id"] = "tp3of4_sl1of2_h1800"
    with pytest.raises(FirstPassageStoreError, match="exact rational fields"):
        _label_key(row)
    row["barrier_id"] = "tp3of4_sl2of4_h31536001"
    row["max_hold_seconds"] = 31_536_001
    with pytest.raises(FirstPassageStoreError, match="governed maximum"):
        _label_key(row)


def test_store_loader_reconstructs_full_source_lineage_not_manifest_claim(
    tmp_path: Path,
) -> None:
    manifest, _, _ = _store_and_work(tmp_path / "forged")
    document = json.loads(manifest.read_bytes())
    document["source_label_sha256"] = "0" * 64
    payload = canonical_json_bytes(document)
    forged_sha256 = hashlib.sha256(payload).hexdigest()
    forged = manifest.parent / f"first-passage-store-{forged_sha256}.json"
    forged.write_bytes(payload)
    forged.chmod(0o444)
    with pytest.raises(FirstPassageStoreError, match="reconstruct the source label"):
        load_first_passage_store(forged)


def test_worker_resume_is_deterministic_and_metrics_match_db_contract(tmp_path: Path) -> None:
    manifest, _, work = _store_and_work(tmp_path / "resume")
    attempt = WorkerAttempt(m0b_candidate_id=11, research_run_attempt_id=23)
    partial = run_candidate_work(
        work,
        attempt,
        first_passage_manifest=manifest,
        worker_root=manifest.parent,
        max_checkpoints=1,
    )
    assert not partial.complete
    complete = run_candidate_work(
        work,
        attempt,
        first_passage_manifest=manifest,
        worker_root=manifest.parent,
    )
    assert complete.complete
    assert complete.classification == "REGISTERED"
    assert complete.metrics == {
        "active_days": 3,
        "flat_trades": 3,
        "net_pnl_ticks": 6,
        "positive_search_folds": 2,
        "raw_events": 4,
        "sequential_trades": 3,
        "stressed_net_pnl_ticks": 3,
        "tp_probability_ppm": 666667,
    }
    pointer = json.loads(next(manifest.parent.glob("*.pointer.json")).read_bytes())
    checkpoint = json.loads((manifest.parent / pointer["checkpoint_relative_uri"]).read_bytes())
    result_identity = checkpoint["state"]["result_artifact"]
    result_path = manifest.parent / result_identity["relative_uri"]
    assert result_identity["byte_size"] == result_path.stat().st_size

    fresh_manifest, _, fresh_work = _store_and_work(tmp_path / "fresh")
    fresh = run_candidate_work(
        fresh_work,
        attempt,
        first_passage_manifest=fresh_manifest,
        worker_root=fresh_manifest.parent,
    )
    assert fresh.result_sha256 == complete.result_sha256
    assert fresh.checkpoint_sha256 == complete.checkpoint_sha256


def test_resume_rejects_checkpoint_result_byte_size_drift(tmp_path: Path) -> None:
    manifest, _, work = _store_and_work(tmp_path / "result-size")
    attempt = WorkerAttempt(19, 29)
    completed = run_candidate_work(
        work,
        attempt,
        first_passage_manifest=manifest,
        worker_root=manifest.parent,
    )
    assert completed.complete
    pointer_path = next(manifest.parent.glob("*.pointer.json"))
    pointer = json.loads(pointer_path.read_bytes())
    checkpoint_path = manifest.parent / pointer["checkpoint_relative_uri"]
    checkpoint = json.loads(checkpoint_path.read_bytes())
    checkpoint["state"]["result_artifact"]["byte_size"] += 1
    checkpoint_payload = canonical_json_bytes(checkpoint)
    checkpoint_sha256 = hashlib.sha256(checkpoint_payload).hexdigest()
    forged_checkpoint = manifest.parent / f"checkpoint-{checkpoint_sha256}.json"
    forged_checkpoint.write_bytes(checkpoint_payload)
    forged_checkpoint.chmod(0o444)
    pointer["checkpoint_relative_uri"] = forged_checkpoint.name
    pointer["checkpoint_sha256"] = checkpoint_sha256
    pointer_path.write_bytes(canonical_json_bytes(pointer))
    with pytest.raises(M0bWorkerError, match="result bytes or semantics differ"):
        run_candidate_work(
            work,
            attempt,
            first_passage_manifest=manifest,
            worker_root=manifest.parent,
        )


def test_worker_replays_orphaned_immutable_artifacts_after_pointer_loss(tmp_path: Path) -> None:
    manifest, _, work = _store_and_work(tmp_path / "orphan")
    attempt = WorkerAttempt(m0b_candidate_id=5, research_run_attempt_id=8)
    partial = run_candidate_work(
        work,
        attempt,
        first_passage_manifest=manifest,
        worker_root=manifest.parent,
        max_checkpoints=1,
    )
    assert not partial.complete
    pointer = next(manifest.parent.glob("*.pointer.json"))
    pointer.unlink()
    replayed = run_candidate_work(
        work,
        attempt,
        first_passage_manifest=manifest,
        worker_root=manifest.parent,
    )
    assert replayed.complete
    assert replayed.classification == "REGISTERED"


def test_execution_contract_rejects_noncausal_tp_and_inexact_timeout(tmp_path: Path) -> None:
    _, _, work = _store_and_work(tmp_path / "execution")
    tp = _label(
        event_ts_ns=100,
        session_id="D1",
        outcome="TP_FIRST",
        entry_ts_ns=101,
        exit_ts_ns=101,
        net_pnl_ticks=3,
    )
    with pytest.raises(M0bWorkerError, match="post-entry trade-through"):
        _validate_executable_label(tp, work)

    timeout = _label(
        event_ts_ns=100,
        session_id="D1",
        outcome="TIMEOUT",
        entry_ts_ns=101,
        exit_ts_ns=100 + 1800 * 1_000_000_000 - 1,
        net_pnl_ticks=0,
    )
    timeout["timeout"] = True
    timeout["first_touch_ts_ns"] = None
    with pytest.raises(M0bWorkerError, match="exact precommitted horizon"):
        _validate_executable_label(timeout, work)


def test_bounded_daemon_cycle_isolates_job_failure(tmp_path: Path) -> None:
    manifest, _, work = _store_and_work(tmp_path / "daemon")
    good = CandidateJob(work, WorkerAttempt(1, 1), manifest, manifest.parent)
    bad = CandidateJob(work, WorkerAttempt(2, 2), manifest.parent / "missing.json", manifest.parent)
    # Duplicate work is rejected before any execution; use a distinct immutable
    # code identity for the intentionally bad job.
    bad_candidate = "8" * 64
    bad_work = replace(
        work,
        candidate_sha256=bad_candidate,
        code_snapshot_sha256="9" * 64,
        signals=replace(work.signals, candidate_sha256=bad_candidate),
    )
    bad = CandidateJob(bad_work, bad.attempt, bad.first_passage_manifest, bad.worker_root)
    results = run_bounded_daemon_cycle(
        (bad, good),
        max_jobs=2,
        max_checkpoints_per_job=2,
    )
    assert [item.status for item in results] == ["FAILED", "COMPLETED"]
    assert results[0].error is not None
    assert results[1].result is not None and results[1].result.complete


def test_null_control_can_never_register_and_duplicate_candidate_jobs_fail(tmp_path: Path) -> None:
    manifest, _, work = _store_and_work(tmp_path / "null")
    null_work = replace(work, candidate_kind="NULL")
    result = run_candidate_work(
        null_work,
        WorkerAttempt(7, 9),
        first_passage_manifest=manifest,
        worker_root=manifest.parent,
    )
    assert result.complete and result.classification == "SCREENED_OUT"
    duplicate_assumptions = replace(work, code_snapshot_sha256="9" * 64)
    with pytest.raises(M0bWorkerError, match="duplicate work or candidates"):
        run_bounded_daemon_cycle(
            (
                CandidateJob(work, WorkerAttempt(1, 1), manifest, manifest.parent),
                CandidateJob(
                    duplicate_assumptions,
                    WorkerAttempt(2, 2),
                    manifest,
                    manifest.parent,
                ),
            ),
            max_jobs=2,
            max_checkpoints_per_job=1,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("checkpoint_shard_interval", 100_001),
        ("cooldown_ns", 31_536_000_000_000_001),
        ("max_signals", 1_000_001),
        ("max_trades", 1_000_001),
        ("search_fold_count", 10_001),
        ("stress_extra_cost_ticks", 1_000_001),
    ],
)
def test_candidate_work_rejects_unbounded_resource_policy(
    tmp_path: Path,
    field: str,
    value: int,
) -> None:
    _, _, work = _store_and_work(tmp_path / field)
    values = {field: value}
    if field == "max_signals":
        values["max_trades"] = value
    with pytest.raises(M0bWorkerError, match="governed maximum"):
        replace(work, **values)


def test_candidate_work_rejects_unbounded_barrier_components() -> None:
    with pytest.raises(M0bWorkerError, match="governed maximum"):
        VolatilityBarrierSpec(
            barrier_id="tp1000001of1_sl1of1_h1800",
            k_tp_num=1_000_001,
            k_tp_den=1,
            k_sl_num=1,
            k_sl_den=1,
            max_hold_seconds=1800,
        )
    with pytest.raises(M0bWorkerError, match="governed maximum"):
        VolatilityBarrierSpec(
            barrier_id="tp1of1_sl1of1_h31536001",
            k_tp_num=1,
            k_tp_den=1,
            k_sl_num=1,
            k_sl_den=1,
            max_hold_seconds=31_536_001,
        )


def test_resume_verifies_complete_checkpoint_predecessor_chain(tmp_path: Path) -> None:
    manifest, _, work = _store_and_work(tmp_path / "chain")
    attempt = WorkerAttempt(3, 4)
    result = run_candidate_work(
        work,
        attempt,
        first_passage_manifest=manifest,
        worker_root=manifest.parent,
    )
    pointer_path = next(manifest.parent.glob("*.pointer.json"))
    pointer = __import__("json").loads(pointer_path.read_bytes())
    checkpoint = __import__("json").loads(
        (manifest.parent / pointer["checkpoint_relative_uri"]).read_bytes()
    )
    predecessor = manifest.parent / f"checkpoint-{checkpoint['predecessor_sha256']}.json"
    predecessor.unlink()
    with pytest.raises(M0bWorkerError, match="checkpoint predecessor is absent"):
        run_candidate_work(
            work,
            attempt,
            first_passage_manifest=manifest,
            worker_root=manifest.parent,
        )
    assert result.complete


def test_postgres_observer_replays_exact_checkpoint_and_db_classification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempt = WorkerAttempt(13, 17)
    calls: list[tuple[str, dict[str, object]]] = []

    def checkpoint(_url: str, **values: object):
        state = values["state"]
        document = {
            "artifact_schema": "systematic_fx.m0b_checkpoint.v1",
            "checkpoint_sequence": values["checkpoint_sequence"],
            "m0b_candidate_id": values["candidate_id"],
            "predecessor_sha256": values["predecessor_sha256"],
            "research_run_attempt_id": values["attempt_id"],
            "state": state,
        }
        calls.append(("checkpoint", document))
        return 1, hashlib.sha256(canonical_json_bytes(document)).hexdigest()

    def terminal(_url: str, **values: object):

        calls.append(("terminal", values))
        return SimpleNamespace(classification="REGISTERED")

    monkeypatch.setattr("systematic_fx.research.m0b.worker_db.checkpoint_m0b_work", checkpoint)
    monkeypatch.setattr("systematic_fx.research.m0b.worker_db.terminalize_m0b_work", terminal)
    observer = PostgresWorkerObserver("postgresql://research", attempt, "1" * 64)
    checkpoint_document = {
        "artifact_schema": "systematic_fx.m0b_checkpoint.v1",
        "checkpoint_sequence": 1,
        "m0b_candidate_id": 13,
        "predecessor_sha256": None,
        "research_run_attempt_id": 17,
        "state": {"state_schema": "systematic_fx.m0b_worker_state.v1"},
    }
    checkpoint_sha256 = hashlib.sha256(canonical_json_bytes(checkpoint_document)).hexdigest()
    observer.checkpoint_published(
        checkpoint_sha256=checkpoint_sha256,
        checkpoint=checkpoint_document,
        relative_uri=f"checkpoint-{checkpoint_sha256}.json",
    )
    metrics = {
        "active_days": 3,
        "flat_trades": 3,
        "net_pnl_ticks": 6,
        "positive_search_folds": 2,
        "raw_events": 4,
        "sequential_trades": 3,
        "stressed_net_pnl_ticks": 3,
        "tp_probability_ppm": 666667,
    }
    result = {
        "artifact_schema": "systematic_fx.m0b_candidate_result.v1",
        "classification": "REGISTERED",
        "metrics": metrics,
    }
    result_sha256 = hashlib.sha256(canonical_json_bytes(result)).hexdigest()
    observer.result_published(
        result_sha256=result_sha256,
        result=result,
        relative_uri=f"candidate-result-{result_sha256}.json",
    )
    assert [item[0] for item in calls] == ["checkpoint", "terminal"]
    assert calls[1][1]["result_byte_size"] == len(canonical_json_bytes(result))


def test_epoch_runtime_identity_loader_returns_only_governed_preclaim_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def call(_url: str, query: str, parameters: tuple[object, ...]):
        assert "holdout_revealed_at IS NULL" in query
        assert "campaign.closed_at IS NULL" in query
        assert parameters == ("fixture-epoch",)
        return {
            "epoch_key": "fixture-epoch",
            "code_commit": "a" * 40,
            "code_snapshot_sha256": "b" * 64,
            "dependency_lock_sha256": "c" * 64,
        }

    monkeypatch.setattr("systematic_fx.db.m0b_worker_registry._call", call)
    assert load_m0b_epoch_runtime_identity(
        "postgresql://worker",
        epoch_key="fixture-epoch",
    ) == M0bEpochRuntimeIdentity(
        epoch_key="fixture-epoch",
        code_commit="a" * 40,
        code_snapshot_sha256="b" * 64,
        dependency_lock_sha256="c" * 64,
    )


def test_postgres_observer_classifies_checkpoint_response_as_replayable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempt = WorkerAttempt(13, 17)

    def checkpoint(*_args: object, **_values: object):
        raise M0bWorkerRegistryError("response lost after commit")

    monkeypatch.setattr("systematic_fx.research.m0b.worker_db.checkpoint_m0b_work", checkpoint)
    observer = PostgresWorkerObserver("postgresql://research", attempt, "1" * 64)
    document = {
        "artifact_schema": "systematic_fx.m0b_checkpoint.v1",
        "checkpoint_sequence": 1,
        "m0b_candidate_id": 13,
        "predecessor_sha256": None,
        "research_run_attempt_id": 17,
        "state": {"state_schema": "systematic_fx.m0b_worker_state.v1"},
    }
    digest = hashlib.sha256(canonical_json_bytes(document)).hexdigest()
    with pytest.raises(M0bCheckpointPublicationError, match="remains pending"):
        observer.checkpoint_published(
            checkpoint_sha256=digest,
            checkpoint=document,
            relative_uri=f"checkpoint-{digest}.json",
        )


def test_registration_work_artifact_reconciles_all_precommitted_inputs(tmp_path: Path) -> None:
    _, store, work = _store_and_work(tmp_path / "binding")
    work_path = tmp_path / "binding" / f"candidate-work-{work.sha256}.json"
    artifact = load_candidate_work_artifact(work_path)
    assert artifact.canonical_bytes == canonical_json_bytes(work.as_dict())
    assert artifact.metadata() == {
        "admission_rules_sha256": work.admission_rules.sha256,
        "barrier": work.barrier.as_dict(),
        "barrier_sha256": work.barrier.sha256,
        "candidate_kind": "REAL",
        "candidate_sha256": work.candidate_sha256,
        "code_snapshot_sha256": work.code_snapshot_sha256,
        "cost_sha256": work.cost_sha256,
        "data_role": "SEARCH",
        "deterministic_seed": 7,
        "direction": "LONG",
        "epoch_sha256": work.epoch_sha256,
        "evaluation_policy": work.evaluation_policy,
        "evaluation_policy_sha256": work.evaluation_policy_sha256,
        "execution_sha256": work.execution_sha256,
        "first_passage_store_sha256": work.first_passage_store_sha256,
        "identity_schema": "systematic_fx.m0b.candidate_work.v2",
        "signal_artifact_sha256": work.signals.content_sha256,
        "source_build_sha256": artifact.source_build_sha256,
        "source_feature_sha256": "f" * 64,
        "source_label_sha256": artifact.source_label_sha256,
        "split_sha256": work.split_sha256,
        "work_spec_sha256": work.sha256,
    }
    assert artifact.artifact_key.endswith(f":{work.candidate_sha256}:{work.sha256}")
    assert artifact.artifact_uri.endswith(f"/sha256={work.sha256}.json")
    run_spec = _run_spec_for_work(artifact)
    candidate = {
        "candidate_kind": "REAL",
        "cost": {"sha256": work.cost_sha256, "version": "fixture_cost_v1"},
        "direction": work.direction,
        "random_seed": work.deterministic_seed,
        "barrier": {"k_tp": "0.75", "k_sl": "0.50", "max_hold_minutes": 30},
    }
    _validate_work_binding(
        work_artifact=artifact,
        run_spec=run_spec,
        candidate_kind="REAL",
        candidate_sha256=work.candidate_sha256,
        canonical_candidate=candidate,
    )
    work_path.chmod(0o644)
    with pytest.raises(M0bRegistryError, match="not immutable canonical work bytes"):
        _validate_work_binding(
            work_artifact=artifact,
            run_spec=run_spec,
            candidate_kind="REAL",
            candidate_sha256=work.candidate_sha256,
            canonical_candidate=candidate,
        )
    work_path.chmod(0o444)
    with pytest.raises(M0bRegistryError, match="identities differ"):
        _validate_work_binding(
            work_artifact=replace(artifact, source_feature_sha256="0" * 64),
            run_spec=run_spec,
            candidate_kind="REAL",
            candidate_sha256=work.candidate_sha256,
            canonical_candidate=candidate,
        )
    with pytest.raises(M0bRegistryError, match="identities differ"):
        _validate_work_binding(
            work_artifact=replace(artifact, source_build_sha256="0" * 64),
            run_spec=run_spec,
            candidate_kind="REAL",
            candidate_sha256=work.candidate_sha256,
            canonical_candidate=candidate,
        )
    with pytest.raises(M0bRegistryError, match="identities differ"):
        _validate_work_binding(
            work_artifact=artifact,
            run_spec=_run_spec_for_work(artifact, work_sha256="0" * 64),
            candidate_kind="REAL",
            candidate_sha256=work.candidate_sha256,
            canonical_candidate=candidate,
        )
    with pytest.raises(M0bRegistryError, match="identities differ"):
        _validate_work_binding(
            work_artifact=artifact,
            run_spec=run_spec,
            candidate_kind="REAL",
            candidate_sha256=work.candidate_sha256,
            canonical_candidate={
                **candidate,
                "barrier": {
                    "k_tp": "1.00",
                    "k_sl": "0.75",
                    "max_hold_minutes": 60,
                },
            },
        )
    with pytest.raises(M0bRegistryError, match="identities differ"):
        _validate_work_binding(
            work_artifact=artifact,
            run_spec=run_spec,
            candidate_kind="REAL",
            candidate_sha256=work.candidate_sha256,
            canonical_candidate={**candidate, "random_seed": 11},
        )

    # Rationally equivalent is not enough: label IDs preserve their exact
    # fixed-denominator representation, and Work must select one that exists.
    absent_barrier = VolatilityBarrierSpec(
        barrier_id="tp3of4_sl1of2_h1800",
        k_tp_num=3,
        k_tp_den=4,
        k_sl_num=1,
        k_sl_den=2,
        max_hold_seconds=1800,
    )
    absent_work = replace(work, barrier=absent_barrier)
    absent_uri = publish_candidate_work_manifest(tmp_path / "binding", absent_work)
    with pytest.raises(M0bWorkerError, match="representation is absent"):
        load_candidate_work_artifact(tmp_path / "binding" / absent_uri)

    signal_path = tmp_path / "binding" / work.signals.relative_uri
    signal_path.chmod(0o644)
    signal_path.write_bytes(signal_path.read_bytes() + b"{}\n")
    with pytest.raises(M0bWorkerError, match="signal artifact bytes differ"):
        load_candidate_work_artifact(work_path)

    signal_path.chmod(0o444)
    shard_path = tmp_path / "binding" / store.shards[0].relative_uri
    shard_path.chmod(0o644)
    shard_path.write_bytes(shard_path.read_bytes() + b"{}\n")
    with pytest.raises(FirstPassageStoreError, match="shard bytes differ"):
        load_candidate_work_artifact(work_path)


def _runner_work(root: Path):
    manifest, _, original = _store_and_work(root)
    canonical_candidate = {
        "artifact_schema": "systematic_fx.m0b_runner_test_candidate.v1",
        "direction": "LONG",
        "random_seed": 7,
    }
    candidate_sha256 = hashlib.sha256(canonical_json_bytes(canonical_candidate)).hexdigest()
    signal_rows = [
        {
            "artifact_schema": "systematic_fx.m0b_candidate_signal.v1",
            "candidate_sha256": candidate_sha256,
            "event_ts_ns": event_ts,
            "feature_sha256": original.signals.feature_sha256,
            "instrument_id": 1,
            "search_fold": fold,
            "session_id": session,
        }
        for event_ts, session, fold in (
            (100, "D1", 0),
            (110, "D1", 0),
            (200, "D2", 1),
            (300, "D3", 1),
        )
    ]
    signals = publish_signal_artifact(
        root,
        candidate_sha256=candidate_sha256,
        feature_sha256=original.signals.feature_sha256,
        rows=signal_rows,
        max_signals=4,
        search_fold_count=2,
    )
    work = replace(original, candidate_sha256=candidate_sha256, signals=signals)
    relative_uri = publish_candidate_work_manifest(root, work)
    artifact = load_candidate_work_artifact(root / relative_uri)
    claim = M0bWorkerClaim(
        m0b_candidate_id=31,
        research_run_attempt_id=41,
        attempt_number=1,
        candidate_sha256=candidate_sha256,
        candidate_kind="REAL",
        canonical_candidate=canonical_candidate,
        epoch_sha256=work.epoch_sha256,
        work_spec_sha256=work.sha256,
        work_spec_byte_size=artifact.byte_size,
        attempt_status="RUNNING",
        lease_status="ACTIVE",
        leased_until=datetime.now(UTC) + timedelta(hours=1),
    )
    return manifest, work, claim


class _RunnerObserver:
    checkpoints: ClassVar[list[str]] = []
    results: ClassVar[list[str]] = []
    failures: ClassVar[list[str]] = []
    interrupt_result_once: ClassVar[bool] = False
    interrupt_failure_once: ClassVar[bool] = False
    fail_terminal_once: ClassVar[bool] = False
    fail_checkpoint_once_on_sequence: ClassVar[int | None] = None

    def __init__(self, _database_url: str, _attempt: WorkerAttempt, _token: str) -> None:
        pass

    def checkpoint_published(
        self,
        *,
        checkpoint_sha256: str,
        checkpoint: dict[str, object],
        **_values: object,
    ) -> None:
        self.checkpoints.append(checkpoint_sha256)
        if self.fail_checkpoint_once_on_sequence == checkpoint["checkpoint_sequence"]:
            type(self).fail_checkpoint_once_on_sequence = None
            raise M0bCheckpointPublicationError("checkpoint capability response was ambiguous")

    def result_published(self, *, result_sha256: str, **_values: object) -> None:
        self.results.append(result_sha256)
        if self.interrupt_result_once:
            type(self).interrupt_result_once = False
            raise KeyboardInterrupt
        if self.fail_terminal_once:
            type(self).fail_terminal_once = False
            raise M0bTerminalPublicationError("terminal capability temporarily unavailable")

    def failure_published(self, *, error_message: str, retryable: bool) -> None:
        assert retryable
        self.failures.append(error_message)
        if self.interrupt_failure_once:
            type(self).interrupt_failure_once = False
            raise KeyboardInterrupt


def _reset_runner_observer() -> None:
    _RunnerObserver.checkpoints = []
    _RunnerObserver.results = []
    _RunnerObserver.failures = []
    _RunnerObserver.interrupt_result_once = False
    _RunnerObserver.interrupt_failure_once = False
    _RunnerObserver.fail_terminal_once = False
    _RunnerObserver.fail_checkpoint_once_on_sequence = None


@pytest.fixture
def stub_runner_code_preflight(monkeypatch: pytest.MonkeyPatch) -> None:
    commit = "a" * 40
    snapshot = "c" * 64
    dependency = "d" * 64
    monkeypatch.setattr(
        "systematic_fx.research.m0b.runner.load_m0b_epoch_runtime_identity",
        lambda _database_url, *, epoch_key: M0bEpochRuntimeIdentity(
            epoch_key=epoch_key,
            code_commit=commit,
            code_snapshot_sha256=snapshot,
            dependency_lock_sha256=dependency,
        ),
    )
    monkeypatch.setattr(
        "systematic_fx.research.m0b.runner._observed_runtime_code_identity",
        lambda: (commit, snapshot, dependency),
    )
    monkeypatch.setattr(
        "systematic_fx.research.m0b.runner._verify_runtime_code_identity",
        lambda _work: None,
    )


def test_runner_code_preflight_detects_workspace_byte_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "governed-workspace"
    for directory in ("configs", "docs", "migrations", "src/systematic_fx"):
        (workspace / directory).mkdir(parents=True, exist_ok=True)
    (workspace / ".git").mkdir()
    (workspace / "Makefile").write_text("all:\n\t@true\n", encoding="utf-8")
    (workspace / "pyproject.toml").write_text("[project]\nname='fixture'\n", encoding="utf-8")
    (workspace / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    source = workspace / "src/systematic_fx/runtime.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    commit = "a" * 40
    expected = build_code_snapshot(workspace, code_commit=commit)
    _, work, _ = _runner_work(tmp_path / "code-drift")
    bound_work = replace(work, code_snapshot_sha256=expected.sha256)
    monkeypatch.setattr(
        "systematic_fx.research.m0b.runner._runtime_project_root", lambda: workspace
    )
    monkeypatch.setattr("systematic_fx.research.m0b.runner._runtime_git_head", lambda _root: commit)
    _verify_runtime_code_identity(bound_work)
    source.write_text("VALUE = 2\n", encoding="utf-8")
    with pytest.raises(M0bRunnerError, match="runtime code snapshot differs"):
        _verify_runtime_code_identity(bound_work)


def test_runner_workspace_inspection_error_is_system_level_code_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "systematic_fx.research.m0b.runner.Path.resolve",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("storage unavailable")),
    )
    with pytest.raises(M0bRuntimeCodeIdentityError, match="cannot be inspected"):
        _runtime_project_root()


def test_runner_database_identity_excludes_rotatable_password() -> None:
    first = _database_identity_sha256("postgresql://worker:first-secret@db.example:5432/research")
    second = _database_identity_sha256("postgresql://worker:second-secret@db.example:5432/research")
    other_database = _database_identity_sha256(
        "postgresql://worker:second-secret@db.example:5432/other"
    )
    assert first == second
    assert first != other_database


def test_claimed_runner_rejects_epoch_runtime_drift_before_token_or_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claim_called = False

    def claim_work(*_args: object, **_kwargs: object):
        nonlocal claim_called
        claim_called = True
        raise AssertionError("runtime mismatch must precede claim")

    monkeypatch.setattr(
        "systematic_fx.research.m0b.runner.load_m0b_epoch_runtime_identity",
        lambda _database_url, *, epoch_key: M0bEpochRuntimeIdentity(
            epoch_key=epoch_key,
            code_commit="a" * 40,
            code_snapshot_sha256="b" * 64,
            dependency_lock_sha256="c" * 64,
        ),
    )
    monkeypatch.setattr(
        "systematic_fx.research.m0b.runner._observed_runtime_code_identity",
        lambda: ("a" * 40, "0" * 64, "c" * 64),
    )
    monkeypatch.setattr("systematic_fx.research.m0b.runner.claim_m0b_work", claim_work)
    worker_root = tmp_path / "preclaim-code-drift"
    with pytest.raises(M0bRuntimeCodeIdentityError, match="before claim"):
        run_claimed_worker_cycle(
            "postgresql://worker",
            epoch_key="fixture-epoch",
            worker_id="worker-preclaim-drift",
            worker_root=worker_root,
        )
    assert not claim_called
    assert not worker_root.exists()


def test_claimed_runner_reconciles_claimed_bytes_and_completes_one_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_runner_code_preflight: None,
) -> None:
    _, work, claim = _runner_work(tmp_path / "runner")
    _reset_runner_observer()
    calls: list[str] = []

    def claim_work(_database_url: str, **values: object):
        calls.append(str(values["lease_token_sha256"]))
        return claim

    monkeypatch.setattr("systematic_fx.research.m0b.runner.claim_m0b_work", claim_work)
    monkeypatch.setattr("systematic_fx.research.m0b.runner.PostgresWorkerObserver", _RunnerObserver)
    result = run_claimed_worker_cycle(
        "postgresql://worker",
        epoch_key="fixture-epoch",
        worker_id="worker-1",
        worker_root=tmp_path / "runner",
    )
    assert result.status == "COMPLETED"
    assert result.candidate_sha256 == work.candidate_sha256
    assert len(calls) == 1 and len(calls[0]) == 64
    assert _RunnerObserver.results
    assert not list((tmp_path / "runner").glob("m0b-worker-lease-*.json"))


def test_claimed_runner_code_drift_preserves_same_attempt_without_candidate_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_runner_code_preflight: None,
) -> None:
    _, _, claim = _runner_work(tmp_path / "runner-code-identity")
    _reset_runner_observer()
    claim_attempts: list[int] = []
    preflight_calls = 0

    def claim_work(_database_url: str, **_values: object):
        claim_attempts.append(claim.research_run_attempt_id)
        return claim

    def preflight(_work: CandidateWorkSpec) -> None:
        nonlocal preflight_calls
        preflight_calls += 1
        if preflight_calls == 1:
            raise M0bRuntimeCodeIdentityError("runtime code snapshot differs from CandidateWork")

    monkeypatch.setattr("systematic_fx.research.m0b.runner.claim_m0b_work", claim_work)
    monkeypatch.setattr("systematic_fx.research.m0b.runner.PostgresWorkerObserver", _RunnerObserver)
    monkeypatch.setattr(
        "systematic_fx.research.m0b.runner._verify_runtime_code_identity", preflight
    )
    with pytest.raises(M0bRuntimeCodeIdentityError, match="runtime code snapshot differs"):
        run_claimed_worker_cycle(
            "postgresql://worker",
            epoch_key="fixture-epoch",
            worker_id="worker-code-drift",
            worker_root=tmp_path / "runner-code-identity",
        )
    token_path = next((tmp_path / "runner-code-identity").glob("m0b-worker-lease-*.json"))
    token = json.loads(token_path.read_bytes())
    assert token["claim"]["research_run_attempt_id"] == claim.research_run_attempt_id
    assert token["pending_failure"] is None
    assert _RunnerObserver.failures == []

    corrected = run_claimed_worker_cycle(
        "postgresql://worker",
        epoch_key="fixture-epoch",
        worker_id="worker-code-drift",
        worker_root=tmp_path / "runner-code-identity",
    )
    assert corrected.status == "COMPLETED"
    assert corrected.research_run_attempt_id == claim.research_run_attempt_id
    assert claim_attempts == [claim.research_run_attempt_id, claim.research_run_attempt_id]
    assert _RunnerObserver.failures == []
    assert not token_path.exists()


def test_claimed_runner_heartbeats_during_slow_code_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_runner_code_preflight: None,
) -> None:
    _, _, claim = _runner_work(tmp_path / "runner-heartbeat")
    _reset_runner_observer()
    heartbeat_observed = threading.Event()
    claim_calls = 0

    def claim_work(_database_url: str, **_values: object):
        nonlocal claim_calls
        claim_calls += 1
        if claim_calls >= 2:
            heartbeat_observed.set()
        return claim

    def slow_preflight(_work: CandidateWorkSpec) -> None:
        assert heartbeat_observed.wait(timeout=1)

    monkeypatch.setattr("systematic_fx.research.m0b.runner.claim_m0b_work", claim_work)
    monkeypatch.setattr("systematic_fx.research.m0b.runner.PostgresWorkerObserver", _RunnerObserver)
    monkeypatch.setattr(
        "systematic_fx.research.m0b.runner._verify_runtime_code_identity", slow_preflight
    )
    monkeypatch.setattr("systematic_fx.research.m0b.runner._HEARTBEAT_SECONDS", 0.001)
    result = run_claimed_worker_cycle(
        "postgresql://worker",
        epoch_key="fixture-epoch",
        worker_id="worker-heartbeat",
        worker_root=tmp_path / "runner-heartbeat",
    )
    assert result.status == "COMPLETED"
    assert claim_calls >= 2
    assert _RunnerObserver.failures == []


def test_claimed_runner_terminal_failure_replays_complete_checkpoint_without_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_runner_code_preflight: None,
) -> None:
    _, _, claim = _runner_work(tmp_path / "runner-terminal-replay")
    _reset_runner_observer()
    _RunnerObserver.fail_terminal_once = True
    claim_attempts: list[int] = []

    def claim_work(_database_url: str, **_values: object):
        claim_attempts.append(claim.research_run_attempt_id)
        return claim

    monkeypatch.setattr("systematic_fx.research.m0b.runner.claim_m0b_work", claim_work)
    monkeypatch.setattr("systematic_fx.research.m0b.runner.PostgresWorkerObserver", _RunnerObserver)
    with pytest.raises(M0bTerminalPublicationError, match="temporarily unavailable"):
        run_claimed_worker_cycle(
            "postgresql://worker",
            epoch_key="fixture-epoch",
            worker_id="worker-terminal-replay",
            worker_root=tmp_path / "runner-terminal-replay",
        )
    token_path = next((tmp_path / "runner-terminal-replay").glob("m0b-worker-lease-*.json"))
    token = json.loads(token_path.read_bytes())
    assert token["pending_failure"] is None
    assert _RunnerObserver.failures == []

    replayed = run_claimed_worker_cycle(
        "postgresql://worker",
        epoch_key="fixture-epoch",
        worker_id="worker-terminal-replay",
        worker_root=tmp_path / "runner-terminal-replay",
    )
    assert replayed.status == "COMPLETED"
    assert claim_attempts == [claim.research_run_attempt_id, claim.research_run_attempt_id]
    assert len(_RunnerObserver.results) == 2
    assert _RunnerObserver.failures == []
    assert not token_path.exists()


def test_claimed_runner_reuses_same_token_when_expired_complete_lease_is_renewed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_runner_code_preflight: None,
) -> None:
    _, _, claim = _runner_work(tmp_path / "runner-expired-complete")
    _reset_runner_observer()
    _RunnerObserver.fail_terminal_once = True
    tokens: list[str] = []

    def claim_work(_database_url: str, **values: object):
        tokens.append(str(values["lease_token_sha256"]))
        # The DB exact-token path renews an expired ACTIVE lease when its
        # latest checkpoint is complete, so the runner must not rotate it.
        return replace(claim, leased_until=datetime.now(UTC) + timedelta(hours=1))

    monkeypatch.setattr("systematic_fx.research.m0b.runner.claim_m0b_work", claim_work)
    monkeypatch.setattr("systematic_fx.research.m0b.runner.PostgresWorkerObserver", _RunnerObserver)
    worker_root = tmp_path / "runner-expired-complete"
    with pytest.raises(M0bTerminalPublicationError):
        run_claimed_worker_cycle(
            "postgresql://worker",
            epoch_key="fixture-epoch",
            worker_id="worker-expired-complete",
            worker_root=worker_root,
        )
    replayed = run_claimed_worker_cycle(
        "postgresql://worker",
        epoch_key="fixture-epoch",
        worker_id="worker-expired-complete",
        worker_root=worker_root,
    )
    assert replayed.status == "COMPLETED"
    assert len(tokens) == 2 and tokens[0] == tokens[1]
    assert _RunnerObserver.failures == []


@pytest.mark.parametrize("checkpoint_sequence", [1, 2])
def test_claimed_runner_checkpoint_response_loss_replays_without_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_runner_code_preflight: None,
    checkpoint_sequence: int,
) -> None:
    _, _, claim = _runner_work(tmp_path / f"runner-checkpoint-{checkpoint_sequence}")
    _reset_runner_observer()
    _RunnerObserver.fail_checkpoint_once_on_sequence = checkpoint_sequence
    claims: list[int] = []

    def claim_work(_database_url: str, **_values: object):
        claims.append(claim.research_run_attempt_id)
        return claim

    monkeypatch.setattr("systematic_fx.research.m0b.runner.claim_m0b_work", claim_work)
    monkeypatch.setattr("systematic_fx.research.m0b.runner.PostgresWorkerObserver", _RunnerObserver)
    worker_root = tmp_path / f"runner-checkpoint-{checkpoint_sequence}"
    with pytest.raises(
        M0bCheckpointPublicationError,
        match="response was ambiguous",
    ):
        run_claimed_worker_cycle(
            "postgresql://worker",
            epoch_key="fixture-epoch",
            worker_id=f"worker-checkpoint-{checkpoint_sequence}",
            worker_root=worker_root,
        )
    token_path = next(worker_root.glob("m0b-worker-lease-*.json"))
    token = json.loads(token_path.read_bytes())
    assert token["pending_failure"] is None
    assert _RunnerObserver.failures == []

    replayed = run_claimed_worker_cycle(
        "postgresql://worker",
        epoch_key="fixture-epoch",
        worker_id=f"worker-checkpoint-{checkpoint_sequence}",
        worker_root=worker_root,
    )
    assert replayed.status == "COMPLETED"
    assert claims == [claim.research_run_attempt_id, claim.research_run_attempt_id]
    assert _RunnerObserver.failures == []
    assert not token_path.exists()


def test_claimed_runner_post_terminal_heartbeat_error_replays_without_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_runner_code_preflight: None,
) -> None:
    _, _, claim = _runner_work(tmp_path / "runner-heartbeat-terminal")
    _reset_runner_observer()
    claim_calls = 0

    def claim_work(_database_url: str, **_values: object):
        nonlocal claim_calls
        claim_calls += 1
        if claim_calls == 1:
            return claim
        return replace(claim, attempt_status="SUCCEEDED", lease_status="RELEASED")

    class HeartbeatFailsAfterTerminal:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_exc: object) -> None:
            assert _RunnerObserver.results
            raise M0bControlPlaneReplayError("lease heartbeat failed")

    monkeypatch.setattr("systematic_fx.research.m0b.runner.claim_m0b_work", claim_work)
    monkeypatch.setattr("systematic_fx.research.m0b.runner.PostgresWorkerObserver", _RunnerObserver)
    monkeypatch.setattr(
        "systematic_fx.research.m0b.runner._LeaseHeartbeat",
        HeartbeatFailsAfterTerminal,
    )
    worker_root = tmp_path / "runner-heartbeat-terminal"
    with pytest.raises(M0bControlPlaneReplayError, match="lease heartbeat failed"):
        run_claimed_worker_cycle(
            "postgresql://worker",
            epoch_key="fixture-epoch",
            worker_id="worker-heartbeat-terminal",
            worker_root=worker_root,
        )
    token_path = next(worker_root.glob("m0b-worker-lease-*.json"))
    assert json.loads(token_path.read_bytes())["pending_failure"] is None
    assert _RunnerObserver.failures == []

    replayed = run_claimed_worker_cycle(
        "postgresql://worker",
        epoch_key="fixture-epoch",
        worker_id="worker-heartbeat-terminal",
        worker_root=worker_root,
    )
    assert replayed.status == "COMPLETED"
    assert claim_calls == 2
    assert _RunnerObserver.failures == []
    assert not token_path.exists()


def test_claimed_runner_owner_only_token_resumes_completed_terminal_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_runner_code_preflight: None,
) -> None:
    _, work, claim = _runner_work(tmp_path / "replay")
    _reset_runner_observer()
    _RunnerObserver.interrupt_result_once = True
    claim_calls = 0

    def claim_work(_database_url: str, **_values: object):
        nonlocal claim_calls
        claim_calls += 1
        if claim_calls == 1:
            return claim
        return replace(claim, attempt_status="SUCCEEDED", lease_status="RELEASED")

    monkeypatch.setattr("systematic_fx.research.m0b.runner.claim_m0b_work", claim_work)
    monkeypatch.setattr("systematic_fx.research.m0b.runner.PostgresWorkerObserver", _RunnerObserver)
    with pytest.raises(KeyboardInterrupt):
        run_claimed_worker_cycle(
            "postgresql://worker",
            epoch_key="fixture-epoch",
            worker_id="worker-2",
            worker_root=tmp_path / "replay",
        )
    token_path = next((tmp_path / "replay").glob("m0b-worker-lease-*.json"))
    assert stat.S_IMODE(token_path.stat().st_mode) == 0o600
    token = json.loads(token_path.read_bytes())
    assert token["claim"]["work_spec_sha256"] == work.sha256

    replayed = run_claimed_worker_cycle(
        "postgresql://worker",
        epoch_key="fixture-epoch",
        worker_id="worker-2",
        worker_root=tmp_path / "replay",
    )
    assert replayed.status == "COMPLETED"
    assert claim_calls == 2
    assert len(_RunnerObserver.results) == 2
    assert not token_path.exists()


def test_claimed_runner_persists_failure_before_capability_call_for_exact_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_runner_code_preflight: None,
) -> None:
    _, _, claim = _runner_work(tmp_path / "failure")
    _reset_runner_observer()
    _RunnerObserver.interrupt_failure_once = True
    claim_calls = 0

    def claim_work(*_args: object, **_kwargs: object):
        nonlocal claim_calls
        claim_calls += 1
        if claim_calls == 1:
            return claim
        return replace(claim, attempt_status="FAILED", lease_status="RELEASED")

    monkeypatch.setattr("systematic_fx.research.m0b.runner.claim_m0b_work", claim_work)
    monkeypatch.setattr("systematic_fx.research.m0b.runner.PostgresWorkerObserver", _RunnerObserver)
    # Force a post-claim, deterministic local reconciliation failure.
    work_path = tmp_path / "failure" / f"candidate-work-{claim.work_spec_sha256}.json"
    work_path.chmod(0o644)
    with pytest.raises(KeyboardInterrupt):
        run_claimed_worker_cycle(
            "postgresql://worker",
            epoch_key="fixture-epoch",
            worker_id="worker-3",
            worker_root=tmp_path / "failure",
        )
    token_path = next((tmp_path / "failure").glob("m0b-worker-lease-*.json"))
    token = json.loads(token_path.read_bytes())
    assert token["pending_failure"]["retryable"] is True

    replayed = run_claimed_worker_cycle(
        "postgresql://worker",
        epoch_key="fixture-epoch",
        worker_id="worker-3",
        worker_root=tmp_path / "failure",
    )
    assert replayed.status == "FAILED"
    assert len(_RunnerObserver.failures) == 2
    assert not token_path.exists()


def test_claimed_runner_rotates_expired_token_and_uses_recovered_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_runner_code_preflight: None,
) -> None:
    _, _, claim = _runner_work(tmp_path / "expired")
    _reset_runner_observer()
    expired = replace(claim, leased_until=datetime.now(UTC) - timedelta(seconds=1))
    recovered = replace(
        claim,
        research_run_attempt_id=42,
        attempt_number=2,
        leased_until=datetime.now(UTC) + timedelta(hours=1),
    )
    tokens: list[str] = []

    def claim_work(_database_url: str, **values: object):
        tokens.append(str(values["lease_token_sha256"]))
        return expired if len(tokens) == 1 else recovered

    monkeypatch.setattr("systematic_fx.research.m0b.runner.claim_m0b_work", claim_work)
    monkeypatch.setattr("systematic_fx.research.m0b.runner.PostgresWorkerObserver", _RunnerObserver)
    result = run_claimed_worker_cycle(
        "postgresql://worker",
        epoch_key="fixture-epoch",
        worker_id="worker-expired",
        worker_root=tmp_path / "expired",
    )
    assert result.status == "COMPLETED"
    assert result.research_run_attempt_id == 42
    assert len(tokens) == 2 and tokens[0] != tokens[1]


def test_claimed_runner_idle_consumes_unused_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_runner_code_preflight: None,
) -> None:
    monkeypatch.setattr(
        "systematic_fx.research.m0b.runner.claim_m0b_work",
        lambda *_args, **_kwargs: None,
    )
    result = run_claimed_worker_cycle(
        "postgresql://worker",
        epoch_key="fixture-epoch",
        worker_id="worker-idle",
        worker_root=tmp_path / "idle",
    )
    assert result.status == "IDLE"
    assert not list((tmp_path / "idle").iterdir())


def test_first_passage_cli_missing_store_fails_without_traceback(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    status = cli.main(
        [
            "research",
            "m0b",
            "verify-first-passage-store",
            "--store",
            str(tmp_path / "missing" / "first-passage-store.json"),
        ]
    )
    assert status == 2
    assert "does not resolve to an existing path" in capsys.readouterr().err
