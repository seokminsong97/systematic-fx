from __future__ import annotations

import hashlib
import inspect
import stat
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from scripts import ai_pattern_holdout_run as holdout_run_module
from scripts import run_ai_pattern_holdout as holdout_cli
from scripts.ai_pattern_holdout_config import (
    AIPatternHoldoutConfig,
    expected_ai_pattern_holdout_contract,
)
from scripts.ai_pattern_holdout_engine import BarWithOutcomeSpan, StageEvaluationResult
from scripts.ai_pattern_holdout_run import (
    AIPatternHoldoutRunError,
    FrozenHoldoutBatch,
    FrozenHoldoutProposal,
    HoldoutEvaluationInputs,
    HoldoutLedger,
    HoldoutRunServices,
    HoldoutStageOutcome,
    HoldoutStagePlan,
    StageMaskBundle,
    _evaluate_stage_default,
    _fixed_run_root,
    _freeze_masks_default,
    _run_with_services,
    _verify_with_services,
    run_ai_pattern_holdout,
    verify_ai_pattern_holdout,
)
from systematic_fx.features.bars import TradeBar
from systematic_fx.research.ai_pattern_discovery import AndRule, RulePredicate
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256


@dataclass(frozen=True)
class _Partition:
    source_date: date
    outcome_span_id: int = 1


def _config(tmp_path: Path) -> AIPatternHoldoutConfig:
    document = {
        **expected_ai_pattern_holdout_contract(),
        "code_commit": "1" * 40,
        "dependency_lock_sha256": "2" * 64,
        "evaluator_implementation_sha256": "3" * 64,
        "precommitted_at_utc": "2026-08-13T23:00:00Z",
    }
    canonical = canonical_json_bytes(document)
    return AIPatternHoldoutConfig(
        path=tmp_path / "placeholder-ai-pattern-holdout-v1.toml",
        file_sha256=hashlib.sha256(b"placeholder config").hexdigest(),
        semantic_sha256=canonical_sha256(document),
        code_commit="1" * 40,
        evaluator_implementation_sha256="3" * 64,
        dependency_lock_sha256="2" * 64,
        precommitted_at_utc="2026-08-13T23:00:00Z",
        canonical_bytes=canonical,
    )


def _inputs() -> HoldoutEvaluationInputs:
    rule = AndRule((RulePredicate("range_ticks", "GE", 4),))
    proposals = tuple(
        FrozenHoldoutProposal(
            selection_rank=rank,
            proposal_sha256=f"{rank:064x}",
            direction="LONG",
            rule=rule,
            discovery_support_rows=100 + rank,
            discovery_session_support_count=50 + rank,
        )
        for rank in range(1, 13)
    )
    batch = FrozenHoldoutBatch(proposals)
    search = HoldoutStagePlan(
        "SEARCH",
        None,
        (date(2022, 1, 3),),
        (_Partition(date(2022, 1, 3)),),  # type: ignore[arg-type]
    )
    walk = tuple(
        HoldoutStagePlan(
            f"WALK_FORWARD_{fold}",
            fold,
            (date(2023, fold, 1),),
            (_Partition(date(2023, fold, 1)),),  # type: ignore[arg-type]
        )
        for fold in range(1, 6)
    )
    holdout = HoldoutStagePlan(
        "HOLDOUT",
        None,
        (date(2026, 2, 16),),
        (_Partition(date(2026, 2, 16)),),  # type: ignore[arg-type]
    )
    return HoldoutEvaluationInputs(
        batch=batch,
        dataset=object(),  # type: ignore[arg-type]
        split_plan=object(),  # type: ignore[arg-type]
        search_plan=search,
        walk_forward_plans=walk,
        holdout_plan=holdout,
    )


def _services(
    run_root: Path,
    inputs: HoldoutEvaluationInputs,
    *,
    search_finalists: tuple[str, ...],
    walk_finalists: tuple[str, ...] = (),
    holdout_classification: str = "ONE_SHOT_UNSEALED_BAR_HOLDOUT_DIAGNOSTIC_PASS",
    fail_stage: str | None = None,
    calls: list[str] | None = None,
) -> HoldoutRunServices:
    observed = [] if calls is None else calls

    def load_inputs(_: Path, __: AIPatternHoldoutConfig) -> HoldoutEvaluationInputs:
        observed.append("LOAD_INPUTS")
        event_types = [
            item.event_type for item in HoldoutLedger(run_root / "ledger", create=False).verify()
        ]
        if event_types[-1] != "COMPLETED":
            assert event_types == ["PRECOMMITTED"]
        return inputs

    def freeze_masks(
        _: Path,
        __: AIPatternHoldoutConfig,
        batch: FrozenHoldoutBatch,
        plans: tuple[HoldoutStagePlan, ...],
        candidate_ids: tuple[str, ...],
    ) -> StageMaskBundle:
        stage = (
            "WALK_FORWARD" if plans[0].stage_key.startswith("WALK_FORWARD") else plans[0].stage_key
        )
        observed.append(f"FREEZE_{stage}")
        latest = HoldoutLedger(run_root / "ledger", create=False).verify()[-1].event_type
        expected_latest = {
            "SEARCH": "PRECOMMITTED",
            "WALK_FORWARD": "SEARCH_COMPLETED",
            "HOLDOUT": "HOLDOUT_AUTHORIZED",
        }[stage]
        if latest != "COMPLETED":
            assert latest == expected_latest
        if fail_stage == f"FREEZE_{stage}":
            raise RuntimeError("deliberate freeze failure")
        by_id = {item.proposal_sha256: item for item in batch.proposals}
        counts = tuple(
            (
                key,
                by_id[key].discovery_support_rows if stage == "SEARCH" else 10,
            )
            for key in candidate_ids
        )
        days = tuple(
            (
                key,
                by_id[key].discovery_session_support_count if stage == "SEARCH" else 5,
            )
            for key in candidate_ids
        )
        return StageMaskBundle(stage, candidate_ids, counts, days, {"vectors": stage})

    def evaluate_stage(
        _: Path,
        __: AIPatternHoldoutConfig,
        ___: FrozenHoldoutBatch,
        plans: tuple[HoldoutStagePlan, ...],
        candidate_ids: tuple[str, ...],
        ____: StageMaskBundle,
    ) -> HoldoutStageOutcome:
        stage = (
            "WALK_FORWARD" if plans[0].stage_key.startswith("WALK_FORWARD") else plans[0].stage_key
        )
        observed.append(f"EVALUATE_{stage}")
        latest = HoldoutLedger(run_root / "ledger", create=False).verify()[-1].event_type
        if latest != "COMPLETED":
            assert (
                latest
                == {
                    "SEARCH": "SEARCH_MASKS_FROZEN",
                    "WALK_FORWARD": "WALK_FORWARD_MASKS_FROZEN",
                    "HOLDOUT": "HOLDOUT_MASKS_FROZEN",
                }[stage]
            )
        if fail_stage == f"EVALUATE_{stage}":
            raise RuntimeError("deliberate evaluation failure")
        if stage == "SEARCH":
            finalists = search_finalists
            classification = "SEARCH_FINALISTS_SELECTED" if finalists else "NO_SEARCH_FINALISTS"
        elif stage == "WALK_FORWARD":
            finalists = walk_finalists
            classification = (
                "WALK_FORWARD_FINALISTS_SELECTED" if finalists else "NO_WALK_FORWARD_FINALISTS"
            )
        else:
            finalists = (
                walk_finalists
                if holdout_classification == "ONE_SHOT_UNSEALED_BAR_HOLDOUT_DIAGNOSTIC_PASS"
                else ()
            )
            classification = holdout_classification
        assert set(finalists).issubset(candidate_ids)
        return HoldoutStageOutcome(stage, finalists, classification, {"metrics": stage})

    return HoldoutRunServices(load_inputs, freeze_masks, evaluate_stage)


def test_full_lifecycle_authorizes_holdout_before_open_and_replays_exactly(
    tmp_path: Path,
) -> None:
    inputs = _inputs()
    ids = tuple(item.proposal_sha256 for item in inputs.batch.proposals)
    run_root = tmp_path / "run"
    calls: list[str] = []
    services = _services(
        run_root,
        inputs,
        search_finalists=ids[:2],
        walk_finalists=ids[:1],
        calls=calls,
    )

    run = _run_with_services(tmp_path, _config(tmp_path), run_root, services)

    assert run.final_status == "ONE_SHOT_UNSEALED_BAR_HOLDOUT_DIAGNOSTIC_PASS"
    assert [
        item.event_type for item in HoldoutLedger(run_root / "ledger", create=False).verify()
    ] == [
        "PRECOMMITTED",
        "SEARCH_MASKS_FROZEN",
        "SEARCH_COMPLETED",
        "WALK_FORWARD_MASKS_FROZEN",
        "WALK_FORWARD_COMPLETED",
        "HOLDOUT_AUTHORIZED",
        "HOLDOUT_MASKS_FROZEN",
        "HOLDOUT_COMPLETED",
        "COMPLETED",
    ]
    assert calls.index("FREEZE_HOLDOUT") > calls.index("EVALUATE_WALK_FORWARD")
    assert all(
        not path.stat().st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
        for path in (run_root / "artifacts").iterdir()
    )
    replay_calls: list[str] = []
    replayed = _verify_with_services(
        tmp_path,
        _config(tmp_path),
        run_root,
        _services(
            run_root,
            inputs,
            search_finalists=ids[:2],
            walk_finalists=ids[:1],
            calls=replay_calls,
        ),
    )
    assert replayed.as_dict() == run.as_dict()


def test_no_search_finalists_never_calls_walk_forward_or_holdout(tmp_path: Path) -> None:
    inputs = _inputs()
    run_root = tmp_path / "run"
    calls: list[str] = []

    run = _run_with_services(
        tmp_path,
        _config(tmp_path),
        run_root,
        _services(run_root, inputs, search_finalists=(), calls=calls),
    )

    assert run.final_status == "NO_SEARCH_FINALISTS_HOLDOUT_NOT_OPENED"
    assert calls == ["LOAD_INPUTS", "FREEZE_SEARCH", "EVALUATE_SEARCH"]
    assert [
        item.event_type for item in HoldoutLedger(run_root / "ledger", create=False).verify()
    ] == [
        "PRECOMMITTED",
        "SEARCH_MASKS_FROZEN",
        "SEARCH_COMPLETED",
        "WALK_FORWARD_SKIPPED",
        "HOLDOUT_SKIPPED",
        "COMPLETED",
    ]


def test_no_walk_forward_finalists_never_opens_holdout(tmp_path: Path) -> None:
    inputs = _inputs()
    identity = inputs.batch.proposals[0].proposal_sha256
    run_root = tmp_path / "run"
    calls: list[str] = []

    run = _run_with_services(
        tmp_path,
        _config(tmp_path),
        run_root,
        _services(
            run_root,
            inputs,
            search_finalists=(identity,),
            walk_finalists=(),
            calls=calls,
        ),
    )

    assert run.final_status == "NO_WALK_FORWARD_FINALISTS_HOLDOUT_NOT_OPENED"
    assert "FREEZE_HOLDOUT" not in calls
    assert "EVALUATE_HOLDOUT" not in calls
    assert HoldoutLedger(run_root / "ledger", create=False).verify()[-2].event_type == (
        "HOLDOUT_SKIPPED"
    )


def test_post_precommit_failure_appends_terminal_failed_event(tmp_path: Path) -> None:
    inputs = _inputs()
    run_root = tmp_path / "run"

    with pytest.raises(RuntimeError, match="deliberate evaluation failure"):
        _run_with_services(
            tmp_path,
            _config(tmp_path),
            run_root,
            _services(
                run_root,
                inputs,
                search_finalists=(),
                fail_stage="EVALUATE_SEARCH",
            ),
        )

    assert [
        item.event_type for item in HoldoutLedger(run_root / "ledger", create=False).verify()
    ] == [
        "PRECOMMITTED",
        "SEARCH_MASKS_FROZEN",
        "FAILED",
    ]


def test_read_only_verifier_rejects_writable_event_before_replay(tmp_path: Path) -> None:
    inputs = _inputs()
    run_root = tmp_path / "run"
    _run_with_services(
        tmp_path,
        _config(tmp_path),
        run_root,
        _services(run_root, inputs, search_finalists=()),
    )
    event = run_root / "ledger/events/event-00000001.json"
    event.chmod(0o644)

    with pytest.raises(AIPatternHoldoutRunError, match="writable"):
        _verify_with_services(
            tmp_path,
            _config(tmp_path),
            run_root,
            _services(run_root, inputs, search_finalists=()),
        )


def test_fixed_verification_root_does_not_create_missing_directories(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text("[project]\nname='placeholder'\n", encoding="utf-8")

    with pytest.raises(AIPatternHoldoutRunError, match="does not exist"):
        _fixed_run_root(tmp_path, create=False)

    assert not (tmp_path / "data").exists()


def test_public_entry_points_do_not_allow_service_injection() -> None:
    assert tuple(inspect.signature(run_ai_pattern_holdout).parameters) == ("project_root",)
    assert tuple(inspect.signature(verify_ai_pattern_holdout).parameters) == ("project_root",)
    parsed = holdout_cli.build_parser().parse_args(["verify", "--json"])
    assert parsed.action == "verify"
    assert not hasattr(parsed, "services")
    assert not hasattr(parsed, "database_url")
    with pytest.raises(SystemExit):
        holdout_cli.build_parser().parse_args(["run", "--services", "fabricated"])


def _synthetic_bars() -> tuple[tuple[BarWithOutcomeSpan, ...], tuple[BarWithOutcomeSpan, ...]]:
    source_date = date(2024, 1, 2)
    base_ns = int(datetime(2024, 1, 2, tzinfo=UTC).timestamp() * 1_000_000_000)
    fives: list[BarWithOutcomeSpan] = []
    for index in range(48):
        start = base_ns + index * 300 * 1_000_000_000
        signal = index == 24
        bar = TradeBar(
            timeframe_seconds=300,
            segment_id=1,
            contract="6EH4",
            source_date=source_date,
            start_ns=start,
            end_ns=start + 300 * 1_000_000_000,
            first_trade_ns=start,
            last_trade_ns=start + 299 * 1_000_000_000,
            open_ticks=1_000,
            high_ticks=1_008 if signal else 1_004,
            low_ticks=1_000,
            close_ticks=1_008 if signal else 1_000,
            trade_count=300,
            volume=300,
            observed_subbars=300,
        )
        fives.append(BarWithOutcomeSpan(bar, 1))
    ones: list[BarWithOutcomeSpan] = []
    for index in range(48 * 300):
        start = base_ns + index * 1_000_000_000
        bar = TradeBar(
            timeframe_seconds=1,
            segment_id=1,
            contract="6EH4",
            source_date=source_date,
            start_ns=start,
            end_ns=start + 1_000_000_000,
            first_trade_ns=start,
            last_trade_ns=start,
            open_ticks=1_000,
            high_ticks=1_000,
            low_ticks=1_000,
            close_ticks=1_000,
            trade_count=1,
            volume=1,
            observed_subbars=1,
        )
        ones.append(BarWithOutcomeSpan(bar, 1))
    return tuple(fives), tuple(ones)


def test_real_default_adapter_runs_masks_outcomes_and_family_selection_on_synthetic_bars(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_values = _inputs()
    signal_rule = AndRule((RulePredicate("signed_body_ppm", "GE", 750_000),))
    first = input_values.batch.proposals[0]
    proposals = (
        FrozenHoldoutProposal(
            first.selection_rank,
            first.proposal_sha256,
            first.direction,
            signal_rule,
            1,
            1,
        ),
        *input_values.batch.proposals[1:],
    )
    batch = FrozenHoldoutBatch(proposals)
    partition = _Partition(date(2024, 1, 2))
    plan = HoldoutStagePlan(
        "SEARCH",
        None,
        (date(2024, 1, 2),),
        (partition,),  # type: ignore[arg-type]
        (("discovery_block_1", (date(2024, 1, 2),)),),
    )
    fives, ones = _synthetic_bars()
    loads: list[int] = []

    def load_bars(_: Path, __: object, timeframe_seconds: int) -> tuple[object, ...]:
        loads.append(timeframe_seconds)
        return fives if timeframe_seconds == 300 else ones

    monkeypatch.setattr(holdout_run_module, "_load_stage_bars", load_bars)
    identity = first.proposal_sha256
    masks = _freeze_masks_default(
        tmp_path,
        _config(tmp_path),
        batch,
        (plan,),
        (identity,),
    )
    result = _evaluate_stage_default(
        tmp_path,
        _config(tmp_path),
        batch,
        (plan,),
        (identity,),
        masks,
    )

    assert masks.raw_signal_counts == ((identity, 1),)
    assert masks.signal_day_counts == ((identity, 1),)
    assert result.classification == "NO_SEARCH_FINALISTS"
    assert result.finalist_proposal_sha256s == ()
    assert isinstance(result.payload, StageEvaluationResult)
    assert len(result.payload.multiplicity_decisions) == 1
    assert len(result.payload.gate_decisions) == 1
    assert result.payload.gate_decisions[0].failure_reasons
    assert loads == [300, 300, 1]
