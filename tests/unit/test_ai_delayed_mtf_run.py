from __future__ import annotations

import inspect
import json
from datetime import date
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.ai_delayed_mtf_config import AIDelayedMTFConfig
from scripts.ai_delayed_mtf_run import (
    DEFAULT_AI_DELAYED_MTF_ROOT,
    AIDelayedMTFRunError,
    _bh_rejections,
    _build_catalog_default,
    _candidate_ids,
    _default_services,
    _DelayedMTFServices,
    _evaluate_holdout_default,
    _evaluate_plan_default,
    _evaluate_search_default,
    _evaluate_walk_forward_default,
    _freeze_holdout_masks_default,
    _freeze_plan_default,
    _freeze_search_masks_default,
    _freeze_walk_forward_masks_default,
    _holm_rejections,
    _Ledger,
    _run_with_services,
    _select_holdout_stage,
    _select_search_stage,
    _select_walk_forward_stages,
    _verify_with_services,
    precommit_ai_delayed_mtf,
    run_ai_delayed_mtf,
    verify_ai_delayed_mtf,
)
from systematic_fx.research.ai_pattern_discovery import ImmutableArtifactError
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256


def _ids(count: int) -> tuple[str, ...]:
    return tuple(f"{index:064x}" for index in range(1, count + 1))


def _selector_item(
    candidate_id: str,
    rank: int,
    p_value: Fraction | None,
    *,
    net_ticks: int = 10,
    gate_pass: bool = True,
) -> SimpleNamespace:
    trade = SimpleNamespace(
        contract=f"CONTRACT_{rank % 7}",
        entry_ns=rank,
        fully_loaded_net_pnl_ticks=net_ticks,
        signal_index=rank,
    )
    summary = SimpleNamespace(
        group_summaries=(
            SimpleNamespace(
                group_key="group",
                mean_net_ticks=Fraction(net_ticks, 1),
                total_net_ticks=net_ticks,
                trade_count=1,
            ),
        ),
        mean_net_pnl_ticks=Fraction(net_ticks, 1),
    )
    return SimpleNamespace(
        candidate=SimpleNamespace(candidate_id=candidate_id, selection_rank=rank),
        conservative_p_value=p_value,
        gate_pass=gate_pass,
        real=SimpleNamespace(summary=summary, trades=(trade,)),
        test_p=p_value,
    )


def _fixture_config(tmp_path: Path) -> tuple[AIDelayedMTFConfig, dict[str, object]]:
    identifiers = _ids(100)
    candidates = [
        {"candidate_id": candidate_id, "selection_rank": ordinal}
        for ordinal, candidate_id in enumerate(identifiers, start=1)
    ]
    catalog = {
        "candidate_count": 100,
        "candidate_ids": list(identifiers),
        "candidates": candidates,
        "catalog_sha256": canonical_sha256(candidates),
    }
    document = {
        "catalog": {
            "candidate_catalog_sha256": catalog["catalog_sha256"],
            "candidate_count": 100,
        },
        "config_id": "unit_test",
    }
    raw = canonical_json_bytes(document)
    config = AIDelayedMTFConfig(
        path=tmp_path / "config.toml",
        file_sha256="a" * 64,
        semantic_sha256=canonical_sha256(document),
        code_commit="b" * 40,
        implementation_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
        precommitted_at_utc="2026-08-14T00:00:00Z",
        canonical_bytes=raw,
    )
    return config, catalog


def _services(
    catalog: dict[str, object],
    calls: list[str],
    *,
    search_selection_count: int,
    walk_finalist_count: int,
    search_error: Exception | None = None,
    holdout_error: Exception | None = None,
) -> _DelayedMTFServices:
    all_ids = tuple(catalog["candidate_ids"])

    def build(_root: Path, _config: AIDelayedMTFConfig) -> object:
        calls.append("catalog")
        return catalog

    def freeze_search(
        _root: Path, _config: AIDelayedMTFConfig, candidate_ids: tuple[str, ...]
    ) -> object:
        calls.append("freeze_search")
        assert candidate_ids == all_ids
        return {
            "candidate_ids": list(candidate_ids),
            "mask_commitment": canonical_sha256(list(candidate_ids)),
            "stage_key": "SEARCH",
        }

    def evaluate_search(
        _root: Path,
        _config: AIDelayedMTFConfig,
        candidate_ids: tuple[str, ...],
        masks: object,
    ) -> object:
        calls.append("evaluate_search_1s")
        assert isinstance(masks, dict) and masks["stage_key"] == "SEARCH"
        if search_error is not None:
            raise search_error
        return {
            "candidate_ids": list(candidate_ids),
            "result_commitment": canonical_sha256(list(candidate_ids)),
            "selected_candidate_ids": list(candidate_ids[:search_selection_count]),
        }

    def freeze_walk(
        _root: Path, _config: AIDelayedMTFConfig, candidate_ids: tuple[str, ...]
    ) -> object:
        calls.append("freeze_walk_forward")
        return {
            "candidate_ids": list(candidate_ids),
            "fold_keys": ["WF1", "WF2", "WF3", "WF4", "WF5"],
            "mask_commitment": canonical_sha256(list(candidate_ids)),
        }

    def evaluate_walk(
        _root: Path,
        _config: AIDelayedMTFConfig,
        candidate_ids: tuple[str, ...],
        masks: object,
        search: object,
    ) -> object:
        calls.append("evaluate_walk_forward_1s")
        assert isinstance(masks, dict) and isinstance(search, dict)
        return {
            "candidate_ids": list(candidate_ids),
            "finalist_candidate_ids": list(candidate_ids[:walk_finalist_count]),
            "fold_keys": ["WF1", "WF2", "WF3", "WF4", "WF5"],
            "result_commitment": canonical_sha256(list(candidate_ids)),
        }

    def freeze_holdout(
        _root: Path, _config: AIDelayedMTFConfig, candidate_ids: tuple[str, ...]
    ) -> object:
        calls.append("freeze_holdout")
        return {
            "candidate_ids": list(candidate_ids),
            "mask_commitment": canonical_sha256(list(candidate_ids)),
            "stage_key": "HOLDOUT",
        }

    def evaluate_holdout(
        _root: Path,
        _config: AIDelayedMTFConfig,
        candidate_ids: tuple[str, ...],
        masks: object,
    ) -> object:
        calls.append("evaluate_holdout_1s")
        assert isinstance(masks, dict) and masks["stage_key"] == "HOLDOUT"
        if holdout_error is not None:
            raise holdout_error
        return {
            "candidate_ids": list(candidate_ids),
            "classification": "ONE_SHOT_UNSEALED_DELAYED_MTF_HOLDOUT_DIAGNOSTIC_PASS",
            "result_commitment": canonical_sha256(list(candidate_ids)),
        }

    return _DelayedMTFServices(
        build_catalog=build,
        freeze_search_masks=freeze_search,
        evaluate_search=evaluate_search,
        freeze_walk_forward_masks=freeze_walk,
        evaluate_walk_forward=evaluate_walk,
        freeze_holdout_masks=freeze_holdout,
        evaluate_holdout=evaluate_holdout,
    )


def _event_types(root: Path) -> tuple[str, ...]:
    events = _Ledger(root / DEFAULT_AI_DELAYED_MTF_ROOT / "ledger", create=False).verify()
    return tuple(event.event_type for event in events)


def test_public_entry_points_accept_only_project_root() -> None:
    for function in (
        precommit_ai_delayed_mtf,
        run_ai_delayed_mtf,
        verify_ai_delayed_mtf,
    ):
        assert tuple(inspect.signature(function).parameters) == ("project_root",)


def test_semantic_catalog_order_is_preserved_through_bh_and_holm() -> None:
    identifiers = ("f" * 64, "0" * 63 + "1", "a" * 64)
    assert _candidate_ids(list(identifiers), label="semantic order") == identifiers
    p_values = {
        identifiers[0]: Fraction(1, 1000),
        identifiers[1]: Fraction(1, 1000),
        identifiers[2]: Fraction(1, 1),
    }

    assert _bh_rejections(identifiers, p_values, q=Fraction(1, 20)) == identifiers[:2]
    assert _holm_rejections(identifiers, p_values, alpha=Fraction(1, 20)) == (identifiers[:2])


def test_search_selector_uses_family_100_missing_p_all_gates_rank_and_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import ai_delayed_mtf_run as runner

    identifiers = _ids(100)
    items = tuple(
        _selector_item(
            candidate_id,
            rank,
            Fraction(rank, 100_000) if rank <= 12 else None,
            gate_pass=rank != 5,
        )
        for rank, candidate_id in enumerate(identifiers, start=1)
    )
    stage = SimpleNamespace(candidates=items, stage_key="SEARCH")
    monkeypatch.setattr(
        runner,
        "_search_gate",
        lambda item, _groups: (
            item.gate_pass,
            () if item.gate_pass else ("FORCED_GATE_FAIL",),
            {},
        ),
    )
    monkeypatch.setattr(
        runner,
        "_compact_stage_result",
        lambda value: {"stage_key": value.stage_key},
    )

    result = _select_search_stage(identifiers, stage, ("block_1",))

    expected = (*identifiers[:4], *identifiers[5:9])
    assert tuple(result["selected_candidate_ids"]) == expected
    assert len(result["selected_candidate_ids"]) == 8
    assert tuple(result["multiple_testing"]["rejected_candidate_ids"]) == identifiers[:12]
    assert result["gate_decisions"][4]["significant_bh_q_0_05"] is True
    assert result["gate_decisions"][4]["selected_before_budget"] is False
    assert result["gate_decisions"][12]["p_star"] == {
        "denominator": 1,
        "numerator": 1,
    }


def test_walk_forward_selector_requires_five_folds_bh_rank_and_cap_three(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import ai_delayed_mtf_run as runner

    identifiers = _ids(8)
    stages = tuple(
        SimpleNamespace(
            candidates=tuple(
                _selector_item(
                    candidate_id,
                    rank,
                    Fraction(rank, 10_000) if rank <= 5 else None,
                    gate_pass=rank != 2,
                )
                for rank, candidate_id in enumerate(identifiers, start=1)
            ),
            stage_key=stage_key,
        )
        for stage_key in ("WF1", "WF2", "WF3", "WF4", "WF5")
    )
    monkeypatch.setattr(
        runner,
        "_aggregate_p_star",
        lambda items: items[0].test_p or Fraction(1, 1),
    )
    monkeypatch.setattr(
        runner,
        "_walk_forward_gate",
        lambda items: (
            items[0].gate_pass,
            () if items[0].gate_pass else ("FORCED_GATE_FAIL",),
            {},
        ),
    )
    monkeypatch.setattr(
        runner,
        "_compact_stage_result",
        lambda value: {"stage_key": value.stage_key},
    )

    result = _select_walk_forward_stages(identifiers, stages)

    assert tuple(result["finalist_candidate_ids"]) == (
        identifiers[0],
        identifiers[2],
        identifiers[3],
    )
    assert tuple(result["multiple_testing"]["rejected_candidate_ids"]) == identifiers[:5]
    assert len(result["fold_results"]) == 5
    with pytest.raises(AIDelayedMTFRunError, match="all five folds"):
        _select_walk_forward_stages(identifiers, stages[:4])


def test_holdout_selector_publishes_all_holm_verdicts_and_passes_iff_all_gates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import ai_delayed_mtf_run as runner

    identifiers = _ids(3)
    items = tuple(
        _selector_item(candidate_id, rank, p_value, gate_pass=rank == 2)
        for rank, (candidate_id, p_value) in enumerate(
            zip(
                identifiers,
                (Fraction(1, 100), Fraction(1, 50), None),
                strict=True,
            ),
            start=1,
        )
    )
    stage = SimpleNamespace(candidates=items, stage_key="HOLDOUT")
    monkeypatch.setattr(
        runner,
        "_holdout_gate",
        lambda item, _groups: (
            item.gate_pass,
            () if item.gate_pass else ("FORCED_GATE_FAIL",),
            {},
        ),
    )
    monkeypatch.setattr(
        runner,
        "_compact_stage_result",
        lambda value: {"stage_key": value.stage_key},
    )

    passed = _select_holdout_stage(identifiers, stage, ("half_1", "half_2"))

    assert tuple(passed["multiple_testing"]["rejected_candidate_ids"]) == identifiers[:2]
    assert passed["passing_candidate_ids"] == [identifiers[1]]
    assert passed["classification"].endswith("_PASS")
    assert len(passed["gate_decisions"]) == 3
    for item in items:
        item.gate_pass = False
    failed = _select_holdout_stage(identifiers, stage, ("half_1", "half_2"))
    assert failed["passing_candidate_ids"] == []
    assert failed["classification"].endswith("_FAIL")


def test_default_services_bind_only_runner_adapters_and_new_engine_catalog(
    tmp_path: Path,
) -> None:
    config, _catalog = _fixture_config(tmp_path)
    services = _default_services()

    assert services.build_catalog is _build_catalog_default
    assert services.freeze_search_masks is _freeze_search_masks_default
    assert services.evaluate_search is _evaluate_search_default
    assert services.freeze_walk_forward_masks is _freeze_walk_forward_masks_default
    assert services.evaluate_walk_forward is _evaluate_walk_forward_default
    assert services.freeze_holdout_masks is _freeze_holdout_masks_default
    assert services.evaluate_holdout is _evaluate_holdout_default
    catalog = services.build_catalog(tmp_path, config)
    assert catalog.catalog_sha256 == (
        "04abb5a3820caf509c44b389c2ec6f0f9d070159c108a8de30ba08f86554bdf9"
    )
    assert len(catalog.candidate_ids) == 100


def test_runner_freeze_adapter_passes_exact_seed_groups_and_subset_to_pure_engine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import ai_delayed_mtf_engine as engine
    from scripts import ai_delayed_mtf_run as runner

    identifiers = (
        "f" * 64,
        "0" * 63 + "1",
    )
    plan = SimpleNamespace(
        stage_key="WF1",
        decision_dates=(date(2026, 1, 2),),
        group_by_date={date(2026, 1, 2): "walk_forward_1"},
    )
    loaded: list[int] = []

    def load_bars(_root: Path, _plan: object, timeframe: int) -> tuple[object, ...]:
        loaded.append(timeframe)
        return (SimpleNamespace(bar=SimpleNamespace(end_ns=10_800_000_000_000)),)

    captured: dict[str, object] = {}

    class Frozen:
        candidate_ids = identifiers
        commitment_sha256 = "a" * 64

        @staticmethod
        def as_dict() -> dict[str, object]:
            return {"candidate_ids": list(identifiers), "stage_key": "WF1"}

    def freeze(*args: object, **kwargs: object) -> object:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return Frozen()

    monkeypatch.setattr(runner, "_load_engine_stage_bars", load_bars)
    monkeypatch.setattr(engine, "freeze_delayed_mtf_stage_masks", freeze)

    frozen = _freeze_plan_default(tmp_path, plan, identifiers)

    assert isinstance(frozen, Frozen)
    assert loaded == [300, 1800, 3600]
    assert captured["kwargs"] == {
        "allowed_stage_tail_end_ns": 10_800_000_000_000,
        "candidate_ids": identifiers,
        "decision_dates": plan.decision_dates,
        "group_by_date": plan.group_by_date,
        "seed": "ai-delayed-mtf-v1",
    }


def test_runner_streaming_adapter_reopens_masks_before_constructing_one_second_parts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import ai_delayed_mtf_engine as engine
    from scripts import ai_delayed_mtf_run as runner

    identifier = "1" * 64
    plan = SimpleNamespace(
        stage_key="SEARCH",
        decision_dates=(date(2026, 1, 2),),
        group_by_date={date(2026, 1, 2): "discovery_block_1"},
    )
    calls: list[str] = []
    frozen = SimpleNamespace(commitment_sha256="b" * 64)

    def reopen(*_args: object, **_kwargs: object) -> object:
        calls.append("reopen_masks")
        return frozen

    def load_fives(*_args: object, **_kwargs: object) -> tuple[object, ...]:
        calls.append("load_fives")
        return (SimpleNamespace(bar=SimpleNamespace(end_ns=10_800_000_000_000)),)

    def parts(*_args: object, **_kwargs: object) -> object:
        calls.append("construct_1s_generator")

        def iterator() -> object:
            calls.append("yield_1s_span")
            yield (SimpleNamespace(),)

        return iterator()

    candidate = SimpleNamespace(candidate_id=identifier)
    stage_result = SimpleNamespace(
        candidates=(SimpleNamespace(candidate=candidate),),
        mask_commitment_sha256="b" * 64,
    )

    def evaluate(
        _stage: str,
        _fives: object,
        one_second_parts: object,
        _frozen: object,
        **_kwargs: object,
    ) -> object:
        calls.append("evaluate")
        tuple(one_second_parts)
        return stage_result

    monkeypatch.setattr(runner, "_reopen_frozen_plan_default", reopen)
    monkeypatch.setattr(runner, "_load_engine_stage_bars", load_fives)
    monkeypatch.setattr(runner, "_one_second_engine_parts", parts)
    monkeypatch.setattr(engine, "evaluate_delayed_mtf_stage_parts", evaluate)

    result = _evaluate_plan_default(
        tmp_path,
        plan,
        (identifier,),
        {"stage_key": "SEARCH"},
    )

    assert result is stage_result
    assert calls == [
        "reopen_masks",
        "load_fives",
        "construct_1s_generator",
        "evaluate",
        "yield_1s_span",
    ]


def test_default_walk_forward_adapter_invokes_selector_only_after_all_five_results(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import ai_delayed_mtf_run as runner

    config, _catalog = _fixture_config(tmp_path)
    identifier = "1" * 64
    plans = tuple(SimpleNamespace(stage_key=f"WF{index}") for index in range(1, 6))
    calls: list[str] = []
    monkeypatch.setattr(
        runner,
        "_load_evaluation_inputs_default",
        lambda _root, _config: SimpleNamespace(walk_forward=plans),
    )

    def evaluate(
        _root: Path,
        plan: SimpleNamespace,
        _candidate_ids: tuple[str, ...],
        _masks: object,
    ) -> SimpleNamespace:
        calls.append(f"evaluate_{plan.stage_key}")
        return SimpleNamespace(stage_key=plan.stage_key, candidates=())

    def select(
        _candidate_ids: tuple[str, ...],
        stages: object,
    ) -> dict[str, object]:
        calls.append("select")
        assert tuple(item.stage_key for item in stages) == (
            "WF1",
            "WF2",
            "WF3",
            "WF4",
            "WF5",
        )
        return {"finalist_candidate_ids": []}

    monkeypatch.setattr(runner, "_evaluate_plan_default", evaluate)
    monkeypatch.setattr(runner, "_select_walk_forward_stages", select)
    masks = {
        "folds": [{"stage_key": plan.stage_key} for plan in plans],
    }

    result = _evaluate_walk_forward_default(
        tmp_path,
        config,
        (identifier,),
        masks,
        {"selected_candidate_ids": [identifier]},
    )

    assert result == {"finalist_candidate_ids": []}
    assert calls == [
        "evaluate_WF1",
        "evaluate_WF2",
        "evaluate_WF3",
        "evaluate_WF4",
        "evaluate_WF5",
        "select",
    ]


def test_full_lifecycle_freezes_each_stage_before_its_one_second_loader(
    tmp_path: Path,
) -> None:
    config, catalog = _fixture_config(tmp_path)
    calls: list[str] = []
    services = _services(
        catalog,
        calls,
        search_selection_count=8,
        walk_finalist_count=3,
    )
    run_root = tmp_path / DEFAULT_AI_DELAYED_MTF_ROOT

    result = _run_with_services(tmp_path, config, run_root, services)

    assert result.status == "COMPLETED"
    assert len(result.finalist_candidate_ids) == 3
    assert calls.index("freeze_search") < calls.index("evaluate_search_1s")
    assert calls.index("evaluate_search_1s") < calls.index("freeze_walk_forward")
    assert calls.index("freeze_walk_forward") < calls.index("evaluate_walk_forward_1s")
    assert calls.index("evaluate_walk_forward_1s") < calls.index("freeze_holdout")
    assert calls.index("freeze_holdout") < calls.index("evaluate_holdout_1s")
    assert _event_types(tmp_path) == (
        "PRECOMMITTED",
        "SEARCH_MASKS_FROZEN",
        "SEARCH_RESULTS_RELEASED",
        "WALK_FORWARD_MASKS_FROZEN",
        "WALK_FORWARD_RESULTS_RELEASED",
        "HOLDOUT_AUTHORIZED",
        "HOLDOUT_MASKS_FROZEN",
        "HOLDOUT_RESULTS_RELEASED",
        "COMPLETED",
    )
    for path in (run_root / "artifacts").iterdir():
        assert path.stat().st_mode & 0o222 == 0


def test_no_search_finalists_never_open_walk_forward_or_holdout(tmp_path: Path) -> None:
    config, catalog = _fixture_config(tmp_path)
    calls: list[str] = []
    services = _services(
        catalog,
        calls,
        search_selection_count=0,
        walk_finalist_count=0,
    )

    result = _run_with_services(
        tmp_path,
        config,
        tmp_path / DEFAULT_AI_DELAYED_MTF_ROOT,
        services,
    )

    assert result.status == "COMPLETED"
    assert not any("walk_forward" in item for item in calls)
    assert not any("holdout" in item for item in calls)
    assert _event_types(tmp_path) == (
        "PRECOMMITTED",
        "SEARCH_MASKS_FROZEN",
        "SEARCH_RESULTS_RELEASED",
        "WALK_FORWARD_SKIPPED",
        "HOLDOUT_SKIPPED",
        "COMPLETED",
    )


def test_no_walk_forward_finalists_never_authorizes_holdout(tmp_path: Path) -> None:
    config, catalog = _fixture_config(tmp_path)
    calls: list[str] = []
    services = _services(
        catalog,
        calls,
        search_selection_count=8,
        walk_finalist_count=0,
    )

    _run_with_services(
        tmp_path,
        config,
        tmp_path / DEFAULT_AI_DELAYED_MTF_ROOT,
        services,
    )

    assert "freeze_walk_forward" in calls
    assert "evaluate_walk_forward_1s" in calls
    assert not any("holdout" in item for item in calls)
    assert _event_types(tmp_path)[-2:] == ("HOLDOUT_SKIPPED", "COMPLETED")


def test_search_selection_budget_is_enforced_before_walk_forward_access(
    tmp_path: Path,
) -> None:
    config, catalog = _fixture_config(tmp_path)
    calls: list[str] = []
    services = _services(
        catalog,
        calls,
        search_selection_count=9,
        walk_finalist_count=0,
    )

    with pytest.raises(AIDelayedMTFRunError, match="budget"):
        _run_with_services(
            tmp_path,
            config,
            tmp_path / DEFAULT_AI_DELAYED_MTF_ROOT,
            services,
        )

    assert "freeze_walk_forward" not in calls
    assert _event_types(tmp_path) == ("PRECOMMITTED", "SEARCH_MASKS_FROZEN")


def test_transient_crash_resumes_after_last_durable_event(tmp_path: Path) -> None:
    config, catalog = _fixture_config(tmp_path)
    first_calls: list[str] = []
    crashing = _services(
        catalog,
        first_calls,
        search_selection_count=8,
        walk_finalist_count=3,
        search_error=OSError("simulated crash"),
    )
    run_root = tmp_path / DEFAULT_AI_DELAYED_MTF_ROOT

    with pytest.raises(OSError, match="simulated crash"):
        _run_with_services(tmp_path, config, run_root, crashing)
    assert _event_types(tmp_path) == ("PRECOMMITTED", "SEARCH_MASKS_FROZEN")

    resumed_calls: list[str] = []
    resumed = _services(
        catalog,
        resumed_calls,
        search_selection_count=8,
        walk_finalist_count=3,
    )
    result = _run_with_services(tmp_path, config, run_root, resumed)

    assert result.status == "COMPLETED"
    assert "freeze_search" not in resumed_calls
    assert resumed_calls.index("evaluate_search_1s") < resumed_calls.index("freeze_walk_forward")


@pytest.mark.parametrize(
    "failure",
    [
        ValueError("engine integrity failure"),
        ImmutableArtifactError("artifact integrity failure"),
    ],
    ids=("engine_value_error", "artifact_runtime_error"),
)
def test_post_holdout_mask_integrity_failure_is_terminal_and_never_reopens_services(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
) -> None:
    from scripts import ai_delayed_mtf_run as runner

    config, catalog = _fixture_config(tmp_path)
    calls: list[str] = []
    services = _services(
        catalog,
        calls,
        search_selection_count=8,
        walk_finalist_count=3,
        holdout_error=failure,
    )
    monkeypatch.setattr(runner, "load_ai_delayed_mtf_config", lambda _root: config)
    monkeypatch.setattr(runner, "_default_services", lambda: services)

    with pytest.raises(
        AIDelayedMTFRunError,
        match="runtime engine, data, or artifact integrity failure",
    ) as captured:
        run_ai_delayed_mtf(tmp_path)

    assert captured.value.__cause__ is failure
    assert _event_types(tmp_path)[-2:] == ("HOLDOUT_MASKS_FROZEN", "FAILED")
    failed_event = _Ledger(
        tmp_path / DEFAULT_AI_DELAYED_MTF_ROOT / "ledger", create=False
    ).verify()[-1]
    assert failed_event.payload["failure_code"] == type(failure).__name__

    calls.clear()
    replay = run_ai_delayed_mtf(tmp_path)
    assert replay.status == "FAILED"
    assert calls == []


def test_fresh_verify_recomputes_without_changing_any_workspace_file(tmp_path: Path) -> None:
    config, catalog = _fixture_config(tmp_path)
    calls: list[str] = []
    services = _services(
        catalog,
        calls,
        search_selection_count=8,
        walk_finalist_count=3,
    )
    run_root = tmp_path / DEFAULT_AI_DELAYED_MTF_ROOT
    _run_with_services(tmp_path, config, run_root, services)
    before = {
        path.relative_to(run_root).as_posix(): (path.stat().st_mode, path.stat().st_mtime_ns)
        for path in run_root.rglob("*")
    }

    verify_calls: list[str] = []
    replay = _services(
        catalog,
        verify_calls,
        search_selection_count=8,
        walk_finalist_count=3,
    )
    verified = _verify_with_services(
        tmp_path,
        replay,
        recompute=True,
        config_override=config,
    )
    after = {
        path.relative_to(run_root).as_posix(): (path.stat().st_mode, path.stat().st_mtime_ns)
        for path in run_root.rglob("*")
    }

    assert verified.status == "COMPLETED"
    assert before == after
    assert "freeze_search" in verify_calls
    assert "evaluate_search_1s" in verify_calls
    assert "freeze_walk_forward" in verify_calls
    assert "evaluate_walk_forward_1s" in verify_calls
    assert "freeze_holdout" in verify_calls
    assert "evaluate_holdout_1s" in verify_calls


def test_ledger_rejects_writable_or_noncanonical_event(tmp_path: Path) -> None:
    config, catalog = _fixture_config(tmp_path)
    calls: list[str] = []
    services = _services(
        catalog,
        calls,
        search_selection_count=0,
        walk_finalist_count=0,
    )
    run_root = tmp_path / DEFAULT_AI_DELAYED_MTF_ROOT
    _run_with_services(tmp_path, config, run_root, services)
    event = run_root / "ledger/events/event-00000001.json"
    original = json.loads(event.read_bytes())
    event.chmod(0o644)
    event.write_text(json.dumps(original), encoding="utf-8")

    with pytest.raises(AIDelayedMTFRunError, match="unsafe event"):
        _Ledger(run_root / "ledger", create=False).verify()
