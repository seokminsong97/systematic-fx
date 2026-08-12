from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from systematic_fx import cli
from systematic_fx.research.hypotheses import canonical_sha256
from systematic_fx.research.m0a.config import load_epoch
from systematic_fx.research.m0a.controls import generate_null_candidates
from systematic_fx.research.m0a.daemon import (
    ForcedCrash,
    daemon_once,
    force_crash_after_claim,
    start_daemon,
)
from systematic_fx.research.m0a.family import generate_candidates
from systematic_fx.research.m0a.ledger import (
    EpochDriftError,
    LedgerInvariantError,
    LedgerStateError,
    M0aLedger,
)
from systematic_fx.research.m0a.pipeline import (
    build_feature_artifact,
    build_label_artifact,
    discover_persisted_epoch_inputs,
    load_epoch_evaluation,
    render_report_from_ledger,
    run_epoch_pipeline,
)

EPOCH_PATH = Path("epochs/m0a_fixture_v1.toml")


class ManualClock:
    def __init__(self) -> None:
        self.value = datetime(2026, 1, 1, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += timedelta(seconds=seconds)


def _digest(label: str) -> str:
    return canonical_sha256({"label": label})


def _epoch(*, real: int = 1, null: int = 1, suffix: str = "one") -> dict[str, object]:
    return {
        "code_commit": "0" * 40,
        "dataset_hash": _digest(f"dataset-{suffix}"),
        "dataset_version": "fixture-v1",
        "epoch_hash": _digest(f"epoch-{suffix}"),
        "epoch_id": f"epoch-{suffix}",
        "execution_model_version": "execution-v1",
        "family_id": "pullback_continuation_v1",
        "feature_version": "features-v1",
        "file_sha256": _digest(f"file-{suffix}"),
        "label_version": "labels-v1",
        "null_candidate_budget": null,
        "random_seeds": [1, 2, 3],
        "real_candidate_budget": real,
        "schema_version": 1,
    }


def _candidate(number: int) -> dict[str, object]:
    return {
        "family_id": "pullback_continuation_v1",
        "parameters": {"number": number},
    }


def _screened() -> dict[str, object]:
    return {
        "admitted": False,
        "controls": {},
        "flat_only_metrics": {},
        "fold_metrics": [],
        "raw_event_metrics": {},
        "sequential_metrics": {},
        "status": "SCREENED_OUT",
        "stressed_cost_metrics": {},
    }


def test_forced_crash_restart_appends_attempt_and_retries(tmp_path: Path) -> None:
    clock = ManualClock()
    ledger = M0aLedger(tmp_path / "ledger.sqlite3", clock=clock, default_lease_seconds=2)
    config = _epoch()
    ledger.ensure_epoch(config)
    ledger.register_candidate("epoch-one", _candidate(1))
    ledger.mark_generation_complete("epoch-one")

    with pytest.raises(ForcedCrash):
        daemon_once(
            ledger,
            epoch_id="epoch-one",
            worker_id="worker-a",
            evaluator=lambda _: _screened(),
            lease_seconds=2,
            crash_hook=force_crash_after_claim,
        )

    clock.advance(3)
    restarted = M0aLedger(tmp_path / "ledger.sqlite3", clock=clock, default_lease_seconds=2)
    step = daemon_once(
        restarted,
        epoch_id="epoch-one",
        worker_id="worker-b",
        evaluator=lambda _: _screened(),
        lease_seconds=2,
    )
    assert step.disposition == "COMPLETED"
    assert len(step.recovered) == 1
    assert restarted.report("epoch-one").attempt_status_counts == {
        "COMPLETED": 1,
        "CRASHED": 1,
    }
    assert restarted.load_epoch_evaluation("epoch-one").retry_count == 1


def test_duplicate_budget_drift_and_null_parent_are_fail_closed(tmp_path: Path) -> None:
    epoch = load_epoch(EPOCH_PATH)
    ledger = M0aLedger(tmp_path / "ledger.sqlite3")
    ledger.ensure_epoch(epoch)
    real = generate_candidates(
        budget=epoch.real_candidate_budget,
        seed=epoch.random_seeds[0],
        barriers=epoch.barrier_specs,
        family_id=epoch.family_id,
        search_space=epoch.family_search_space,
    )
    first = ledger.register_candidate(epoch.epoch_id, real[0])
    duplicate = ledger.register_candidate(epoch.epoch_id, real[0])
    assert first.created and not duplicate.created
    assert duplicate.candidate_id == first.candidate_id

    null = generate_null_candidates(real[:1], seed=epoch.random_seeds[1])[0]
    registered_null = ledger.register_candidate(epoch.epoch_id, null, candidate_kind="NULL")
    assert registered_null.parent_candidate_id == first.candidate_id
    orphan_ledger = M0aLedger(tmp_path / "orphan.sqlite3")
    orphan_ledger.ensure_epoch(epoch)
    with pytest.raises(LedgerStateError, match="exact earlier REAL parent"):
        orphan_ledger.register_candidate(epoch.epoch_id, null, candidate_kind="NULL")

    drift = dict(epoch.as_dict())
    drift["dataset_version"] = "changed"
    with pytest.raises(EpochDriftError):
        ledger.ensure_epoch(drift)


def test_budget_exhaustion_is_transactional_and_generation_stops(tmp_path: Path) -> None:
    ledger = M0aLedger(tmp_path / "ledger.sqlite3")
    config = _epoch()
    ledger.ensure_epoch(config)
    assert ledger.register_candidate("epoch-one", _candidate(1)).created
    overflow = ledger.register_candidate("epoch-one", _candidate(2))
    assert overflow.budget_exhausted and overflow.candidate_id is None
    ledger.mark_generation_complete("epoch-one")
    assert not ledger.register_candidate("epoch-one", _candidate(1)).created
    with pytest.raises(LedgerStateError, match="no longer accepts"):
        ledger.register_candidate("epoch-one", _candidate(3))


def test_candidate_failures_continue_but_system_threshold_halts(tmp_path: Path) -> None:
    ledger = M0aLedger(tmp_path / "candidate.sqlite3")
    ledger.ensure_epoch(_epoch(real=2, suffix="candidate"))
    ledger.register_candidate("epoch-candidate", _candidate(1))
    ledger.register_candidate("epoch-candidate", _candidate(2))
    ledger.mark_generation_complete("epoch-candidate")

    def candidate_evaluator(candidate: object) -> object:
        if candidate["parameters"]["number"] == 1:  # type: ignore[index]
            raise ValueError("deterministic bad candidate")
        return _screened()

    run = start_daemon(
        ledger,
        epoch_id="epoch-candidate",
        worker_id="worker",
        evaluator=candidate_evaluator,
    )
    assert run.epoch.status == "COMPLETED"
    assert run.epoch.consecutive_system_errors == 0
    assert run.epoch.candidate_status_counts == {"FAILED": 1, "SCREENED_OUT": 1}

    system = M0aLedger(tmp_path / "system.sqlite3", default_system_error_threshold=2)
    system.ensure_epoch(_epoch(real=3, suffix="system"), system_error_threshold=2)
    for number in range(3):
        system.register_candidate("epoch-system", _candidate(number))
    system.mark_generation_complete("epoch-system")
    halted = start_daemon(
        system,
        epoch_id="epoch-system",
        worker_id="worker",
        evaluator=lambda _: (_ for _ in ()).throw(RuntimeError("infra")),
    )
    assert halted.epoch.status == "HALTED"
    assert halted.epoch.consecutive_system_errors == 2
    assert halted.epoch.candidate_status_counts == {"FAILED": 2, "QUEUED": 1}


def test_artifact_and_event_tampering_are_detected(tmp_path: Path) -> None:
    ledger = M0aLedger(tmp_path / "ledger.sqlite3")
    ledger.ensure_epoch(_epoch())
    ledger.register_candidate("epoch-one", _candidate(1))
    ledger.mark_generation_complete("epoch-one")
    daemon_once(
        ledger,
        epoch_id="epoch-one",
        worker_id="worker",
        evaluator=lambda _: _screened(),
    )
    durable = ledger.load_epoch_evaluation("epoch-one")
    artifact = Path(str(durable.candidate_records[0]["artifact_path"]))
    artifact.chmod(0o644)
    artifact.write_text("{}\n", encoding="utf-8")
    with pytest.raises(LedgerInvariantError, match="artifact"):
        ledger.verify_invariants("epoch-one")

    second = M0aLedger(tmp_path / "events.sqlite3")
    second.ensure_epoch(_epoch(suffix="events"))
    with (
        sqlite3.connect(second.database_path) as connection,
        pytest.raises(sqlite3.IntegrityError),
    ):
        connection.execute("UPDATE events SET event_type = 'FORGED'")


def test_persisted_cli_sequence_and_completed_exact_rerun(tmp_path: Path) -> None:
    state = tmp_path / "state"
    features = build_feature_artifact(EPOCH_PATH, artifact_root=state)
    assert features.fixture_artifact.row_count == 1
    assert features.feature_artifact.row_count == 480
    labels = build_label_artifact(EPOCH_PATH, artifact_root=state)
    assert labels.row_count == 25_920
    assert discover_persisted_epoch_inputs(EPOCH_PATH, artifact_root=state) is not None

    ledger_path = tmp_path / "ledger.sqlite3"
    first = run_epoch_pipeline(EPOCH_PATH, ledger_path=ledger_path, artifact_root=state)
    second = run_epoch_pipeline(EPOCH_PATH, ledger_path=ledger_path, artifact_root=state)
    assert first.ledger.candidate_status_counts == {"REGISTERED": 1, "SCREENED_OUT": 35}
    assert {step.lease.lease_owner for step in first.daemon.steps if step.lease is not None} == {
        "m0a-worker-1-generation-1",
        "m0a-worker-1-generation-2",
        "m0a-worker-1-generation-3",
    }
    assert second.daemon.completed_count == 0
    assert second.invariants.valid
    durable = load_epoch_evaluation(ledger_path, first.epoch_id, artifact_root=state)
    assert len(durable.candidate_records) == 36
    assert durable.retry_count == 0
    markdown = render_report_from_ledger(
        ledger_path,
        first.epoch_id,
        artifact_root=state,
        epoch_path=EPOCH_PATH,
    )
    assert "Real budget used: 12" in markdown
    assert "Null budget used: 24" in markdown
    assert "Not paper eligible. Not live eligible." in markdown


def test_manifest_overrides_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(Exception, match="cannot override"):
        run_epoch_pipeline(
            EPOCH_PATH,
            ledger_path=tmp_path / "ledger.sqlite3",
            candidate_seed=999,
        )


def test_run_epoch_cli_does_not_call_a_live_partial_epoch_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class PartialResult:
        invariants = type("Invariants", (), {"valid": True})()
        ledger = type("Ledger", (), {"status": "RUNNING"})()

    monkeypatch.setattr(
        "systematic_fx.research.m0a.pipeline.run_epoch_pipeline",
        lambda *args, **kwargs: PartialResult(),
    )
    arguments = cli.build_parser().parse_args(
        [
            "research",
            "m0a",
            "run-epoch",
            "--state-root",
            str(tmp_path),
        ]
    )
    assert arguments.handler(arguments) == 3
