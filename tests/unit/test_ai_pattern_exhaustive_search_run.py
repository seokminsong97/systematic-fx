from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from scripts import ai_pattern_exhaustive_search_run as run_module
from scripts import ai_pattern_holdout_engine as engine_module
from scripts.ai_pattern_exhaustive_search_config import (
    AIPatternExhaustiveConfig,
    expected_ai_pattern_exhaustive_contract,
)
from scripts.ai_pattern_exhaustive_search_run import (
    AIPatternExhaustiveSearchError,
    ExhaustiveBatch,
    ExhaustiveCandidate,
    ExhaustiveLedger,
    ExhaustiveRunServices,
    ExhaustiveSearchPlan,
    _initial_evidence,
    _replay_completed_batch_data_with_services,
    _run_with_services,
    build_exhaustive_search_plan,
)
from scripts.ai_pattern_holdout_engine import (
    FrozenProposal,
    ProposalMaskSet,
    SignalMask,
    StageEvaluationResult,
    StageMaskBundle,
)
from systematic_fx.research.ai_pattern_discovery_v2 import deterministic_candidate_catalog_v2
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256


def _config(tmp_path: Path) -> AIPatternExhaustiveConfig:
    document = {
        **expected_ai_pattern_exhaustive_contract(),
        "code_commit": "1" * 40,
        "dependency_lock_sha256": "2" * 64,
        "evaluator_implementation_sha256": "3" * 64,
        "precommitted_at_utc": "2026-08-14T12:00:00Z",
    }
    canonical = canonical_json_bytes(document)
    return AIPatternExhaustiveConfig(
        path=tmp_path / "config.toml",
        file_sha256=hashlib.sha256(b"test config").hexdigest(),
        semantic_sha256=canonical_sha256(document),
        code_commit="1" * 40,
        evaluator_implementation_sha256="3" * 64,
        dependency_lock_sha256="2" * 64,
        precommitted_at_utc="2026-08-14T12:00:00Z",
        canonical_bytes=canonical,
    )


def _synthetic_plan() -> ExhaustiveSearchPlan:
    patterns = deterministic_candidate_catalog_v2()[:518]
    members = tuple(
        ExhaustiveCandidate(
            family_position=index,
            evaluation_id=f"{10_000 + index:064x}",
            pattern_sha256=patterns[index - 1].sha256,
            eligible_rank=index,
            pattern=patterns[index - 1],
            support_rows=500,
            session_support_count=80,
            stability_ppm=0,
            origin=(
                "IMPORTED_BATCH3_EXACT_SUMMARY" if index <= 12 else "EXHAUSTIVE_DOMAIN_SEPARATED_V1"
            ),
        )
        for index in range(1, 519)
    )
    initial = members[:12]
    remaining = members[12:]
    batches = tuple(
        ExhaustiveBatch(
            batch_number=offset // 12 + 1,
            batch_key=f"E{offset // 12 + 1:03d}",
            batch_sha256=f"{30_000 + offset // 12 + 1:064x}",
            members=remaining[offset : offset + 12],
        )
        for offset in range(0, len(remaining), 12)
    )
    family_sha256 = canonical_sha256([item.pattern_sha256 for item in members])
    return ExhaustiveSearchPlan(
        initial,
        remaining,
        batches,
        family_sha256,
        "4" * 64,
        "5" * 64,
        "6" * 64,
    )


def _mask_bundle(batch: ExhaustiveBatch) -> StageMaskBundle:
    masks = []
    for member in batch.members:
        proposal = FrozenProposal(
            member.family_position,
            member.evaluation_id,
            member.pattern.direction,
            member.pattern.rule,
        )
        real = SignalMask(
            f"{member.evaluation_id}:real",
            member.evaluation_id,
            "REAL",
            (False,),
        )
        masks.append(
            ProposalMaskSet(
                proposal,
                real,
                None,
                None,
                (),
                0,
                0,
                (),
                (),
                False,
                "REAL_SIGNAL_MASK_IS_EMPTY",
            )
        )
    return StageMaskBundle("SEARCH", "7" * 64, (True,), tuple(masks))


def test_real_discovery_only_plan_reproduces_all_independent_fingerprints() -> None:
    plan = build_exhaustive_search_plan(Path.cwd())

    assert len(plan.members) == 518
    assert len(plan.remaining_members) == 506
    assert [len(item.members) for item in plan.batches] == [*([12] * 42), 2]
    assert plan.family_ids == tuple(sorted(plan.family_ids))
    assert plan.family_sha256 == (
        "e269800244d62c346497dbbcdfdda540eb361f7273027f387fbc2efe27db4d59"
    )
    assert plan.remaining_pattern_sha_list_sha256 == (
        "f34c5b2e6189136e758cc6f441622d6b2e417046f580b42a75bf367432aa77d3"
    )
    assert plan.remaining_assessment_catalog_sha256 == (
        "088c35d2b6781b74e058aa1eef4be8a87a7818e3a5b42d1bd000fb3883d36c3b"
    )
    assert plan.batch_manifest_sha256 == (
        "022af03de649f829b5ae44f58c840bea05440bda36d7eeca9d5fc6d33fb0f322"
    )
    assert [item.eligible_rank for item in plan.initial_members] == [
        1,
        2,
        4,
        5,
        6,
        7,
        11,
        15,
        19,
        20,
        22,
        27,
    ]
    assert all(item.evaluation_id != item.hypothesis_id for item in plan.remaining_members)
    initial, eligibility = _initial_evidence(Path.cwd(), plan)
    assert len(initial) == 12
    assert all(eligibility.values())

    imported_batch = ExhaustiveBatch(1, "E001", "f" * 64, plan.initial_members)
    duplicate_row = initial[0].as_dict()
    with pytest.raises(AIPatternExhaustiveSearchError, match="duplicate or foreign"):
        run_module._decode_raw_batch_result(
            {
                "artifact_schema": ("systematic_fx.ai_pattern_exhaustive_raw_batch_result.v1"),
                "batch_summary": {},
                "raw_result": {
                    "candidates": [duplicate_row, duplicate_row],
                    "classification": "RAW_BATCH_EVALUATED_NO_SELECTION",
                    "stage_key": "SEARCH",
                },
                "sample_eligibility": [],
            },
            imported_batch,
        )


def test_ledger_partial_event_write_is_resumable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = ExhaustiveLedger(tmp_path / "ledger", create=True)
    original_write_all = run_module._write_all

    def interrupted_write(descriptor: int, _payload: bytes) -> None:
        assert run_module.os.write(descriptor, b"partial-event-123") == 17
        raise OSError("simulated interrupted ledger write")

    monkeypatch.setattr(run_module, "_write_all", interrupted_write)
    with pytest.raises(OSError, match="simulated interrupted"):
        ledger.append("PRECOMMITTED", "a" * 64, {})
    assert ledger.verify() == ()
    assert tuple(ledger.events_root.iterdir()) == ()

    # A SIGKILL may leave staging bytes, but they are outside the exact event
    # namespace and cannot poison verification or the next sequence.
    abandoned = ledger.staging_root / ".event-00000001-abandoned.tmp"
    abandoned.write_bytes(b"partial")
    assert ledger.verify() == ()

    monkeypatch.setattr(run_module, "_write_all", original_write_all)
    event = ledger.append("PRECOMMITTED", "a" * 64, {})
    assert event.sequence == 1
    assert ledger.verify() == (event,)


def test_fixed_run_mutation_lock_rejects_a_second_writer(tmp_path: Path) -> None:
    run_root = tmp_path / "run"
    run_root.mkdir()
    with (
        run_module._exclusive_run_lock(run_root),
        pytest.raises(OSError, match="another exhaustive Search writer"),
        run_module._exclusive_run_lock(run_root),
    ):
        raise AssertionError("contended writer entered the critical section")


def test_orphan_artifact_publisher_staging_is_reconciled(tmp_path: Path) -> None:
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    partial = artifacts_root / ".publish-partial.tmp"
    partial.write_bytes(b"partial")

    published = artifacts_root / "batch-result-deadbeef.json"
    published.write_bytes(b"published")
    published.chmod(0o444)
    linked_temp = artifacts_root / ".publish-linked.tmp"
    run_module.os.link(published, linked_temp)

    run_module._reconcile_artifact_publish_temps(artifacts_root)

    assert not partial.exists()
    assert not linked_temp.exists()
    assert published.read_bytes() == b"published"


def test_all_masks_barrier_crash_reconciliation_and_completed_corruption_rejection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _synthetic_plan()
    config = _config(tmp_path)
    run_root = tmp_path / "run"
    calls: list[str] = []
    evaluation_calls = 0
    selector_calls: list[tuple[str, ...]] = []
    frozen_batch_plan = tuple(item.as_dict() for item in plan.batches)

    original_selector = engine_module.select_stage_result

    def selector_spy(
        stage_key: str,
        raw_result: StageEvaluationResult,
        masks: object,
        family_ids: tuple[str, ...],
    ) -> StageEvaluationResult:
        assert stage_key == "SEARCH"
        assert tuple(family_ids) == plan.family_ids
        assert len(family_ids) == 518
        selector_calls.append(tuple(family_ids))
        return original_selector(stage_key, raw_result, masks, family_ids)  # type: ignore[arg-type]

    monkeypatch.setattr(engine_module, "select_stage_result", selector_spy)

    monkeypatch.setattr(
        run_module,
        "_initial_evidence",
        lambda _root, value: (
            (),
            {item.evaluation_id: False for item in value.initial_members},
        ),
    )

    def load_search_plan(_root: Path) -> object:
        calls.append("LOAD_PLAN")
        return object()

    def load_fives(_root: Path, _plan: object) -> tuple[object, ...]:
        calls.append("LOAD_5M")
        return (object(),)

    def freeze(
        _root: Path,
        batch: ExhaustiveBatch,
        _plan: object,
        _fives: tuple[object, ...],
    ) -> StageMaskBundle:
        calls.append(f"FREEZE_{batch.batch_key}")
        return _mask_bundle(batch)

    def evaluate(
        _root: Path,
        batch: ExhaustiveBatch,
        _plan: object,
        _fives: tuple[object, ...],
        _masks: object,
    ) -> StageEvaluationResult:
        nonlocal evaluation_calls
        evaluation_calls += 1
        if evaluation_calls <= 43:
            assert selector_calls == []
        calls.append(f"OPEN_1S_{batch.batch_key}")
        events = ExhaustiveLedger(run_root / "ledger", create=False).verify()
        assert sum(item.event_type == "BATCH_MASKS_FROZEN" for item in events) == 43
        assert any(item.event_type == "ALL_MASKS_FROZEN" for item in events)
        return StageEvaluationResult(
            "SEARCH",
            (),
            (),
            "RAW_BATCH_EVALUATED_NO_SELECTION",
        )

    services = ExhaustiveRunServices(load_search_plan, load_fives, freeze, evaluate)  # type: ignore[arg-type]
    original_append = ExhaustiveLedger.append
    crashed = False

    def crash_after_first_result_artifact(
        self: ExhaustiveLedger,
        event_type: str,
        request_sha256: str,
        payload: dict[str, object],
    ) -> object:
        nonlocal crashed
        if event_type == "BATCH_COMPLETED" and not crashed:
            crashed = True
            raise SystemExit("simulated crash after result publication")
        return original_append(self, event_type, request_sha256, payload)

    monkeypatch.setattr(ExhaustiveLedger, "append", crash_after_first_result_artifact)
    with pytest.raises(SystemExit, match="simulated crash"):
        _run_with_services(
            tmp_path,
            config,
            plan,
            run_root,
            services,
            emit_batch_summaries=False,
        )
    assert evaluation_calls == 1
    assert ExhaustiveLedger(run_root / "ledger", create=False).verify()[-1].event_type == (
        "ALL_MASKS_FROZEN"
    )

    monkeypatch.setattr(ExhaustiveLedger, "append", original_append)
    result = _run_with_services(
        tmp_path,
        config,
        plan,
        run_root,
        services,
        emit_batch_summaries=False,
    )

    assert result.status == "COMPLETED"
    assert result.masks_frozen == 43
    assert result.batches_completed == 43
    assert result.finalist_hypothesis_ids == ()
    assert len(selector_calls) == 1
    assert evaluation_calls == 43  # E001 was recovered and was not opened twice.
    assert calls.count("LOAD_5M") == 2  # once per process attempt, shared by all chunks
    assert tuple(item.as_dict() for item in plan.batches) == frozen_batch_plan
    first_open = next(index for index, value in enumerate(calls) if value.startswith("OPEN_1S"))
    assert sum(value.startswith("FREEZE_") for value in calls[:first_open]) == 44

    # A completed idempotent call replays family/report and does not load bars.
    before = calls.count("LOAD_5M")
    replay = _run_with_services(
        tmp_path,
        config,
        plan,
        run_root,
        services,
        emit_batch_summaries=False,
    )
    assert replay.as_dict() == result.as_dict()
    assert calls.count("LOAD_5M") == before
    assert len(selector_calls) == 2

    artifacts_root = run_root / "artifacts"
    completed_state = run_module._verify_ledger_state(
        ExhaustiveLedger(run_root / "ledger", create=False),
        artifacts_root,
        config,
        plan,
    )
    for identity in completed_state.result_artifacts:
        document = run_module._reopen_artifact(artifacts_root, identity)
        assert set(document["payload"]["raw_result"]) == {  # type: ignore[index]
            "candidates",
            "classification",
            "stage_key",
        }
    _replay_completed_batch_data_with_services(
        tmp_path,
        config,
        plan,
        completed_state,
        artifacts_root,
        services,
    )
    assert calls.count("LOAD_5M") == before + 1
    assert evaluation_calls == 86

    def divergent_freeze(
        root: Path,
        batch: ExhaustiveBatch,
        search_plan: object,
        fives: tuple[object, ...],
    ) -> StageMaskBundle:
        original = freeze(root, batch, search_plan, fives)
        return StageMaskBundle(
            original.stage_key,
            "8" * 64,
            original.eligible_positions,
            original.proposal_masks,
        )

    divergent_services = ExhaustiveRunServices(
        load_search_plan,
        load_fives,
        divergent_freeze,
        evaluate,
    )  # type: ignore[arg-type]
    with pytest.raises(AIPatternExhaustiveSearchError, match="exact replay"):
        _replay_completed_batch_data_with_services(
            tmp_path,
            config,
            plan,
            completed_state,
            artifacts_root,
            divergent_services,
        )

    assert result.report_artifact is not None
    report = run_root / "artifacts" / result.report_artifact.relative_uri
    report.unlink()
    with pytest.raises(AIPatternExhaustiveSearchError):
        _run_with_services(
            tmp_path,
            config,
            plan,
            run_root,
            services,
            emit_batch_summaries=False,
        )
