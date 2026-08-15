from __future__ import annotations

import inspect
import json
import os
import stat
from collections.abc import Mapping
from datetime import date, timedelta
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace

import pytest

from campaigns.ai_all_cases_v1 import pipeline as pipeline_module
from campaigns.ai_all_cases_v1 import run as run_module
from campaigns.ai_all_cases_v1.config import (
    AI_ALL_CASES_CONFIG_ID,
    AI_ALL_CASES_RUN_RELATIVE_ROOT,
    DETERMINISTIC_RUNTIME_ENV,
    AllCasesConfig,
)
from campaigns.ai_all_cases_v1.pipeline import (
    _SUBLEDGER_ARTIFACT_SCHEMA,
    AllCasesPipelineError,
    _benjamini_hochberg_rejections,
    _CandidatePartitionEvidence,
    _daily_p_star,
    _DirectSearchEvidence,
    _exact_one_sided_sign_test,
    _filled_trade_evidence,
    _finalize_search_result,
    _FrozenSearchCandidate,
    _holdout_result_payload,
    _holm_rejections,
    _lineage_groups,
    _MetaSearchEvidence,
    _publish_mode_0444,
    _SearchSubledger,
    _select_diverse_search_candidates,
    _validate_raw_chunk_payload,
    _walk_result_payload,
    _WorldPartitionEvidence,
)
from campaigns.ai_all_cases_v1.pipeline import (
    _canonical_json_bytes as _pipeline_json,
)
from campaigns.ai_all_cases_v1.run import (
    DEFAULT_AI_ALL_CASES_ROOT,
    AllCasesIntegrityError,
    _AllCasesServices,
    _canonical_sha256,
    _Ledger,
    _run_with_services,
    _validate_holdout_result,
    _validate_search_result,
    _validate_walk_result,
    _verify_with_services,
    precommit_ai_all_cases,
    run_ai_all_cases,
    verify_ai_all_cases,
)

ROOT = Path(__file__).resolve().parents[2]
RUN_REAL_SEARCH_FEATURE_SMOKE = (
    os.environ.get("SYSTEMATIC_FX_RUN_AI_ALL_CASES_REAL_SEARCH_FEATURE_SMOKE") == "1"
)

_PHASES = (
    "STAGE_A_SCORE_CHUNKS",
    "STAGE_A_TOP256",
    "STAGE_B_PLAN_FROZEN",
    "STAGE_B_RAW_CHUNKS",
    "SYMBOLIC_TOP24",
    "DIRECT_ML_CHUNKS",
    "META_PLAN_FROZEN",
    "META_ML_CHUNKS",
    "FINAL_MAX12",
)


@pytest.fixture(autouse=True)
def _deterministic_runtime_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in DETERMINISTIC_RUNTIME_ENV.items():
        monkeypatch.setenv(key, value)


def _ids(count: int) -> tuple[str, ...]:
    return tuple(f"{index:064x}" for index in range(1, count + 1))


def _fraction(numerator: int, denominator: int = 1) -> dict[str, int]:
    value = Fraction(numerator, denominator)
    return {"denominator": value.denominator, "numerator": value.numerator}


def _daily_worlds() -> dict[str, list[dict[str, object]]]:
    dates = tuple(f"2026-01-{index:02d}" for index in range(1, 21))

    def rows(value: int) -> list[dict[str, object]]:
        return [{"decision_date": day, "net_ticks": value} for day in dates]

    return {
        "CIRCULAR_TARGET": rows(1),
        "MATCHED_TARGET": rows(0),
        "REAL": rows(3),
    }


def _empty_fit_cache_aggregate(candidate_kind: str) -> dict[str, object]:
    from campaigns.ai_all_cases_v1 import ml

    maximum = 126 if candidate_kind == "DIRECT" else 84
    chunks = tuple(ml.SharedFitCacheEvidence(maximum, 0, 0, 0, 0, 0, 0, 0, 0) for _ in range(24))
    return ml.aggregate_shared_fit_cache_evidence(candidate_kind, chunks).as_dict()


def _economic_evidence(stage: str) -> dict[str, object]:
    common: dict[str, object] = {
        "active_entry_days": 20,
        "contract_count": 5,
        "fill_count": 100,
        "maximum_drawdown_ticks": 10,
        "net_ticks": 60,
        "profit_factor": _fraction(2),
    }
    if stage == "walk-forward":
        return {
            **common,
            "active_entry_days": 75,
            "fold_active_entry_days": [15] * 5,
            "fold_fill_counts": [20] * 5,
            "fold_net_ticks": [12] * 5,
            "worst_fold_ev_ticks": _fraction(3, 5),
            "worst_fold_profit_factor": _fraction(2),
            "worst_loss_over_median_positive": _fraction(0),
        }
    return {
        **common,
        "half_net_ticks": [30, 30],
        "net_over_maximum_drawdown": _fraction(6),
    }


def test_exact_daily_p_star_and_multiplicity_include_full_frozen_families() -> None:
    assert _exact_one_sided_sign_test((1, 2, 3, 4, 5)) == Fraction(1, 32)
    assert _exact_one_sided_sign_test((0, 0)) == 1
    dates = ("2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05")
    assert _daily_p_star(
        dates,
        {day: 3 for day in dates},
        {day: 1 for day in dates},
        {},
        eligible=True,
    ) == Fraction(1, 32)
    assert _daily_p_star(dates, {}, {}, {}, eligible=True) == 1
    assert _daily_p_star(dates, {}, None, {}, eligible=False) == 1

    family = _ids(3)
    p_values = {family[0]: Fraction(1, 100), family[1]: Fraction(1), family[2]: Fraction(1)}
    assert _benjamini_hochberg_rejections(family, p_values, q=Fraction(1, 20)) == family[:1]
    assert _holm_rejections(family, p_values, alpha=Fraction(1, 20)) == family[:1]


def test_filled_trade_segment_identity_accepts_exact_uint64_domain_only() -> None:
    arguments = {
        "candidate_id": "1" * 64,
        "stage_key": "WF1",
        "world": "REAL",
        "source_identity": {"row": 1},
        "decision_date": "2024-01-01",
        "decision_ns": 1,
        "entry_ns": 2,
        "exit_ns": 3,
        "contract": "6E",
        "outcome_span_id": 1,
        "direction": "LONG",
        "net_ticks": 1,
    }
    evidence = _filled_trade_evidence(segment_id=(1 << 64) - 1, **arguments)
    assert evidence.segment_id == (1 << 64) - 1
    with pytest.raises(AllCasesPipelineError, match="filled-trade evidence"):
        _filled_trade_evidence(segment_id=1 << 64, **arguments)


def test_production_result_assemblers_close_full_zero_filled_date_domains() -> None:
    candidate_id = _ids(1)[0]
    candidate = _FrozenSearchCandidate(
        candidate_id,
        "SYMBOLIC",
        "family",
        1,
        {},
    )

    def world(
        dates: tuple[str, ...],
        *,
        stage_key: str,
        world_key: str,
        net: int,
        trade_count: int,
    ) -> _WorldPartitionEvidence:
        daily = {day: 0 for day in dates}
        trades = []
        for index, day in enumerate(dates[:trade_count]):
            daily[day] += net
            decision_ns = date.fromisoformat(day).toordinal() * 1_000_000_000 + index
            trades.append(
                _filled_trade_evidence(
                    candidate_id=candidate_id,
                    stage_key=stage_key,
                    world=world_key,
                    source_identity={"index": index},
                    decision_date=day,
                    decision_ns=decision_ns,
                    entry_ns=decision_ns + 1,
                    exit_ns=decision_ns + 2,
                    contract=f"C{index % 5}",
                    outcome_span_id=1,
                    segment_id=1,
                    direction="LONG",
                    net_ticks=net,
                )
            )
        return _WorldPartitionEvidence(tuple(daily.items()), tuple(trades))

    folds = []
    fold_lengths = (133, 133, 133, 133, 132)
    walk_dates = tuple(
        (date(2023, 1, 1) + timedelta(days=index)).isoformat() for index in range(sum(fold_lengths))
    )
    cursor = 0
    for fold, length in enumerate(fold_lengths):
        dates = walk_dates[cursor : cursor + length]
        cursor += length
        folds.append(
            _CandidatePartitionEvidence(
                candidate,
                f"WF{fold + 1}",
                True,
                {
                    world_key: world(
                        dates,
                        stage_key=f"WF{fold + 1}",
                        world_key=world_key,
                        net=2 if world_key == "REAL" else 0,
                        trade_count=20,
                    )
                    for world_key in ("REAL", "CIRCULAR_TARGET", "MATCHED_TARGET")
                },
            )
        )
    walk = _walk_result_payload((candidate_id,), {candidate_id: folds})
    validated_walk, finalists = _validate_walk_result(
        walk,
        (candidate_id,),
        expected_decision_dates=walk_dates,
        expected_fold_lengths=fold_lengths,
    )
    assert finalists == (candidate_id,)
    assert validated_walk["all_folds_complete"] is True
    walk_real = walk["candidate_results"][0]["result_document"]["daily_net_ticks_by_world"]["REAL"]
    assert len(walk_real) == 664

    holdout_dates = tuple(
        (date(2026, 1, 1) + timedelta(days=index)).isoformat() for index in range(120)
    )
    holdout_value = _CandidatePartitionEvidence(
        candidate,
        "HOLDOUT",
        True,
        {
            world_key: world(
                holdout_dates,
                stage_key="HOLDOUT",
                world_key=world_key,
                net=3 if world_key == "REAL" else 0,
                trade_count=12,
            )
            for world_key in ("REAL", "CIRCULAR_TARGET", "MATCHED_TARGET")
        },
    )
    # Put an equal number of fills in the second frozen half.
    second_half = list(holdout_value.worlds["REAL"].filled_trades)  # type: ignore[union-attr]
    for index, day in enumerate(holdout_dates[60:72], start=60):
        decision_ns = date.fromisoformat(day).toordinal() * 1_000_000_000 + index
        second_half.append(
            _filled_trade_evidence(
                candidate_id=candidate_id,
                stage_key="HOLDOUT",
                world="REAL",
                source_identity={"index": index},
                decision_date=day,
                decision_ns=decision_ns,
                entry_ns=decision_ns + 1,
                exit_ns=decision_ns + 2,
                contract=f"C{index % 5}",
                outcome_span_id=1,
                segment_id=1,
                direction="LONG",
                net_ticks=3,
            )
        )
    real_daily = dict(holdout_value.worlds["REAL"].daily_net_ticks)  # type: ignore[union-attr]
    for day in holdout_dates[60:72]:
        real_daily[day] = 3
    holdout_value = _CandidatePartitionEvidence(
        candidate,
        "HOLDOUT",
        True,
        {
            **holdout_value.worlds,
            "REAL": _WorldPartitionEvidence(tuple(sorted(real_daily.items())), tuple(second_half)),
        },
    )
    holdout = _holdout_result_payload((candidate_id,), (holdout_value,))
    validated_holdout, classification = _validate_holdout_result(
        holdout,
        (candidate_id,),
        expected_decision_dates=holdout_dates,
    )
    assert classification.endswith("_PASS")
    assert validated_holdout["candidate_results"][0]["result_document"]["evidence"][
        "half_net_ticks"
    ] == [36, 36]

    omitted_walk = json.loads(json.dumps(walk))
    omitted_walk_document = omitted_walk["candidate_results"][0]["result_document"]
    for rows in omitted_walk_document["daily_net_ticks_by_world"].values():
        rows.pop(25)
    omitted_walk["candidate_results"][0]["result_sha256"] = _canonical_sha256(omitted_walk_document)
    omitted_walk["multiplicity_sha256"] = _canonical_sha256(
        {
            "candidate_results": omitted_walk["candidate_results"],
            "finalist_candidate_ids": omitted_walk["finalist_candidate_ids"],
            "method": "BENJAMINI_HOCHBERG",
        }
    )
    with pytest.raises(AllCasesIntegrityError, match="frozen calendar|trade-summary|economics"):
        _validate_walk_result(
            omitted_walk,
            (candidate_id,),
            expected_decision_dates=walk_dates,
            expected_fold_lengths=fold_lengths,
        )

    forged_walk = json.loads(json.dumps(walk))
    forged_walk_document = forged_walk["candidate_results"][0]["result_document"]
    forged_walk_document["evidence"]["fold_net_ticks"] = [39, 41, 40, 40, 40]
    forged_walk["candidate_results"][0]["result_sha256"] = _canonical_sha256(forged_walk_document)
    forged_walk["multiplicity_sha256"] = _canonical_sha256(
        {
            "candidate_results": forged_walk["candidate_results"],
            "finalist_candidate_ids": forged_walk["finalist_candidate_ids"],
            "method": "BENJAMINI_HOCHBERG",
        }
    )
    with pytest.raises(AllCasesIntegrityError, match="economics|fold-net"):
        _validate_walk_result(
            forged_walk,
            (candidate_id,),
            expected_decision_dates=walk_dates,
            expected_fold_lengths=fold_lengths,
        )

    omitted_holdout = json.loads(json.dumps(holdout))
    omitted_holdout_document = omitted_holdout["candidate_results"][0]["result_document"]
    for rows in omitted_holdout_document["daily_net_ticks_by_world"].values():
        rows.pop(30)
    omitted_holdout["candidate_results"][0]["result_sha256"] = _canonical_sha256(
        omitted_holdout_document
    )
    omitted_holdout["holm_sha256"] = _canonical_sha256(
        {
            "candidate_results": omitted_holdout["candidate_results"],
            "classification": omitted_holdout["classification"],
            "method": "HOLM_STEP_DOWN",
        }
    )
    with pytest.raises(AllCasesIntegrityError, match="frozen calendar|trade-summary|economics"):
        _validate_holdout_result(
            omitted_holdout,
            (candidate_id,),
            expected_decision_dates=holdout_dates,
        )

    forged_holdout = json.loads(json.dumps(holdout))
    forged_holdout_document = forged_holdout["candidate_results"][0]["result_document"]
    forged_holdout_document["evidence"]["half_net_ticks"] = [35, 37]
    forged_holdout["candidate_results"][0]["result_sha256"] = _canonical_sha256(
        forged_holdout_document
    )
    forged_holdout["holm_sha256"] = _canonical_sha256(
        {
            "candidate_results": forged_holdout["candidate_results"],
            "classification": forged_holdout["classification"],
            "method": "HOLM_STEP_DOWN",
        }
    )
    with pytest.raises(AllCasesIntegrityError, match="half-net|economics"):
        _validate_holdout_result(
            forged_holdout,
            (candidate_id,),
            expected_decision_dates=holdout_dates,
        )

    forged_aggregates = json.loads(json.dumps(holdout))
    forged_document = forged_aggregates["candidate_results"][0]["result_document"]
    forged_document["evidence"]["profit_factor"] = _fraction(2)
    forged_document["evidence"]["maximum_drawdown_ticks"] = 10
    forged_document["evidence"]["net_over_maximum_drawdown"] = _fraction(
        forged_document["evidence"]["net_ticks"], 10
    )
    forged_aggregates["candidate_results"][0]["result_sha256"] = _canonical_sha256(forged_document)
    forged_aggregates["holm_sha256"] = _canonical_sha256(
        {
            "candidate_results": forged_aggregates["candidate_results"],
            "classification": forged_aggregates["classification"],
            "method": "HOLM_STEP_DOWN",
        }
    )
    with pytest.raises(AllCasesIntegrityError, match="economics"):
        _validate_holdout_result(
            forged_aggregates,
            (candidate_id,),
            expected_decision_dates=holdout_dates,
        )


def test_holdout_terminal_taxonomy_distinguishes_inconclusive_and_mixed_failures() -> None:
    candidate_ids = _ids(2)

    def result(candidate_id: str, *, eligible: bool) -> dict[str, object]:
        worlds: dict[str, object] = _daily_worlds()
        evidence = _economic_evidence("holdout")
        if not eligible:
            worlds["CIRCULAR_TARGET"] = None
            worlds["MATCHED_TARGET"] = None
            reasons = ["NULL_SAMPLE_INELIGIBLE", "NULL_DELTA_NOT_POSITIVE"]
        else:
            for world in worlds.values():
                assert isinstance(world, list)
                for row in world:
                    row["net_ticks"] = 0
            evidence.update(
                {
                    "half_net_ticks": [0, 0],
                    "net_over_maximum_drawdown": _fraction(0),
                    "net_ticks": 0,
                    "profit_factor": None,
                }
            )
            reasons = [
                "BOTH_HOLDOUT_HALVES_NET_NOT_POSITIVE",
                "TOTAL_NET_NOT_POSITIVE",
                "PROFIT_FACTOR_LT_1_10",
                "NET_OVER_MAX_DRAWDOWN_LT_0_75",
                "NULL_DELTA_NOT_POSITIVE",
            ]
        document = {
            "candidate_kind": "SYMBOLIC",
            "catalog_selection_rank": 1,
            "daily_net_ticks_by_world": worlds,
            "economic_gate_pass": False,
            "evidence": evidence,
            "failure_reasons": reasons,
            "holm_rejected": False,
            "p_star": _fraction(1),
            "sample_eligible": eligible,
            "verdict_pass": False,
        }
        return {
            "candidate_id": candidate_id,
            "result_document": document,
            "result_sha256": _canonical_sha256(document),
        }

    def payload(eligibility: tuple[bool, bool], classification: str) -> dict[str, object]:
        results = [
            result(candidate_id, eligible=eligible)
            for candidate_id, eligible in zip(candidate_ids, eligibility, strict=True)
        ]
        return {
            "candidate_ids": list(candidate_ids),
            "candidate_results": results,
            "classification": classification,
            "holm_sha256": _canonical_sha256(
                {
                    "candidate_results": results,
                    "classification": classification,
                    "method": "HOLM_STEP_DOWN",
                }
            ),
            "schema": "systematic_fx.ai_all_cases_holdout_result_payload.v1",
        }

    inconclusive = "ONE_SHOT_UNSEALED_ALL_CASES_HOLDOUT_DIAGNOSTIC_INCONCLUSIVE"
    failed = "ONE_SHOT_UNSEALED_ALL_CASES_HOLDOUT_DIAGNOSTIC_FAIL"
    assert _validate_holdout_result(payload((False, False), inconclusive), candidate_ids)[1] == (
        inconclusive
    )
    assert _validate_holdout_result(payload((False, True), failed), candidate_ids)[1] == failed
    with pytest.raises(AllCasesIntegrityError, match="terminal classification"):
        _validate_holdout_result(payload((False, True), inconclusive), candidate_ids)


def test_result_validator_recomputes_economic_gate_and_walk_forward_rank() -> None:
    candidate_ids = _ids(2)

    holdout_document = {
        "candidate_kind": "SYMBOLIC",
        "catalog_selection_rank": 1,
        "daily_net_ticks_by_world": _daily_worlds(),
        "economic_gate_pass": False,
        "evidence": _economic_evidence("holdout"),
        "failure_reasons": [],
        "holm_rejected": True,
        "p_star": _fraction(1, 1_048_576),
        "sample_eligible": True,
        "verdict_pass": True,
    }
    holdout_result = {
        "candidate_id": candidate_ids[0],
        "result_document": holdout_document,
        "result_sha256": _canonical_sha256(holdout_document),
    }
    holdout_payload = {
        "candidate_ids": [candidate_ids[0]],
        "candidate_results": [holdout_result],
        "classification": "ONE_SHOT_UNSEALED_ALL_CASES_HOLDOUT_DIAGNOSTIC_PASS",
        "holm_sha256": _canonical_sha256(
            {
                "candidate_results": [holdout_result],
                "classification": ("ONE_SHOT_UNSEALED_ALL_CASES_HOLDOUT_DIAGNOSTIC_PASS"),
                "method": "HOLM_STEP_DOWN",
            }
        ),
        "schema": "systematic_fx.ai_all_cases_holdout_result_payload.v1",
    }
    with pytest.raises(AllCasesIntegrityError, match="economic gate"):
        _validate_holdout_result(holdout_payload, candidate_ids[:1])

    candidate_results = []
    for index, candidate_id in enumerate(candidate_ids, start=1):
        document = {
            "bh_rejected": True,
            "candidate_kind": "SYMBOLIC",
            "catalog_selection_rank": index,
            "daily_net_ticks_by_world": _daily_worlds(),
            "economic_gate_pass": True,
            "evidence": _economic_evidence("walk-forward"),
            "failure_reasons": [],
            "finalist_rank": 3 - index,
            "p_star": _fraction(1, 1_048_576),
            "sample_eligible": True,
            "selected_before_budget": True,
            "selected_finalist": True,
        }
        candidate_results.append(
            {
                "candidate_id": candidate_id,
                "result_document": document,
                "result_sha256": _canonical_sha256(document),
            }
        )
    reversed_finalists = list(reversed(candidate_ids))
    walk_payload = {
        "all_folds_complete": True,
        "candidate_ids": list(candidate_ids),
        "candidate_results": candidate_results,
        "finalist_candidate_ids": reversed_finalists,
        "fold_keys": ["WF1", "WF2", "WF3", "WF4", "WF5"],
        "multiplicity_sha256": _canonical_sha256(
            {
                "candidate_results": candidate_results,
                "finalist_candidate_ids": reversed_finalists,
                "method": "BENJAMINI_HOCHBERG",
            }
        ),
        "schema": "systematic_fx.ai_all_cases_walk_forward_result_payload.v1",
    }
    with pytest.raises(AllCasesIntegrityError, match="budget/rank"):
        _validate_walk_result(walk_payload, candidate_ids)


@pytest.mark.parametrize("stage", ("walk-forward", "holdout"))
def test_oos_results_bind_catalog_rank_to_immutable_search_descriptor(
    tmp_path: Path,
    stage: str,
) -> None:
    services = _services([], search_selection_count=4, walk_finalist_count=2)
    config = _config(tmp_path)
    universe = services.freeze_search_universe(tmp_path, config)
    search = services.train_select_search(tmp_path, config, universe)
    assert isinstance(search, dict)
    selected = tuple(search["selected_candidate_ids"])
    descriptors = run_module._search_candidate_descriptors(search, selected)
    masks = services.freeze_walk_forward_masks(tmp_path, config, selected, search)
    walk = services.evaluate_walk_forward(tmp_path, config, selected, masks, search)
    assert isinstance(walk, dict)
    if stage == "walk-forward":
        payload = json.loads(json.dumps(walk))
        candidate_ids = selected
    else:
        finalists = tuple(walk["finalist_candidate_ids"])
        holdout_masks = services.freeze_holdout_masks(tmp_path, config, finalists, walk)
        payload = json.loads(
            json.dumps(services.evaluate_holdout(tmp_path, config, finalists, holdout_masks))
        )
        candidate_ids = finalists
    first = payload["candidate_results"][0]["result_document"]
    second = payload["candidate_results"][1]["result_document"]
    first["catalog_selection_rank"], second["catalog_selection_rank"] = (
        second["catalog_selection_rank"],
        first["catalog_selection_rank"],
    )
    first["candidate_descriptor"]["catalog_selection_rank"] = first["catalog_selection_rank"]
    second["candidate_descriptor"]["catalog_selection_rank"] = second["catalog_selection_rank"]
    for row in payload["candidate_results"][:2]:
        row["result_sha256"] = _canonical_sha256(row["result_document"])
    if stage == "walk-forward":
        payload["multiplicity_sha256"] = _canonical_sha256(
            {
                "candidate_results": payload["candidate_results"],
                "finalist_candidate_ids": payload["finalist_candidate_ids"],
                "method": "BENJAMINI_HOCHBERG",
            }
        )
        validator = _validate_walk_result
    else:
        payload["holm_sha256"] = _canonical_sha256(
            {
                "candidate_results": payload["candidate_results"],
                "classification": payload["classification"],
                "method": "HOLM_STEP_DOWN",
            }
        )
        validator = _validate_holdout_result

    with pytest.raises(AllCasesIntegrityError, match="descriptor binding"):
        validator(
            payload,
            candidate_ids,
            expected_candidate_descriptors={
                candidate_id: descriptors[candidate_id] for candidate_id in candidate_ids
            },
        )


def _config(tmp_path: Path) -> AllCasesConfig:
    document = {
        "config_id": "unit-test",
        "compute_caps": {
            "artifact_bytes_maximum": 20 * 1024**3,
            "resident_set_bytes_maximum": 32 * 1024**3,
            "search_wall_seconds_maximum": 172_800,
            "verifier_wall_seconds_maximum": 172_800,
        },
        "search_design": {
            "search_internal_phase_order": list(_PHASES),
            "search_phase_chunk_counts_canonical_json": json.dumps(
                {phase: 1 for phase in _PHASES},
                sort_keys=True,
                separators=(",", ":"),
            ),
        },
        "selection": {"search_selection_maximum": 12},
    }
    raw = json.dumps(document, sort_keys=True, separators=(",", ":")).encode("ascii")
    return AllCasesConfig(
        path=tmp_path / "config.toml",
        file_sha256="a" * 64,
        semantic_sha256="b" * 64,
        code_commit="c" * 40,
        implementation_sha256="d" * 64,
        dependency_lock_sha256="e" * 64,
        precommitted_at_utc="2026-08-15T00:00:00Z",
        canonical_bytes=raw,
    )


def _production_identity_config(tmp_path: Path) -> AllCasesConfig:
    original = _config(tmp_path)
    document = original.as_dict()
    document["config_id"] = AI_ALL_CASES_CONFIG_ID
    raw = json.dumps(document, sort_keys=True, separators=(",", ":")).encode("ascii")
    return AllCasesConfig(
        path=original.path,
        file_sha256=original.file_sha256,
        semantic_sha256=original.semantic_sha256,
        code_commit=original.code_commit,
        implementation_sha256=original.implementation_sha256,
        dependency_lock_sha256=original.dependency_lock_sha256,
        precommitted_at_utc=original.precommitted_at_utc,
        canonical_bytes=raw,
    )


def _services(
    calls: list[str],
    *,
    search_selection_count: int,
    walk_finalist_count: int,
    search_error: Exception | None = None,
    holdout_error: Exception | None = None,
    invalid_search: bool = False,
) -> _AllCasesServices:
    identifiers = _ids(20)
    policy_id = "a" * 64

    def candidate_descriptor(candidate_id: str, rank: int) -> dict[str, object]:
        frozen_document = {
            "candidate_id": candidate_id,
            "catalog_selection_rank": rank,
            "recipe": "fixture",
        }
        return {
            "candidate_id": candidate_id,
            "candidate_kind": "SYMBOLIC",
            "catalog_selection_rank": rank,
            "family_key": "fixture-family",
            "frozen_artifact_sha256": _canonical_sha256(frozen_document),
        }

    def freeze_search(_root: Path, _config: AllCasesConfig) -> object:
        calls.append("freeze_search_features_events_catalog")
        leaves = [
            {
                "artifact_sha256": f"{index + 1:064x}",
                "chunk_index": index,
                "relative_path": f"universe-{index:03d}-{index + 1:064x}.json",
            }
            for index in range(64)
        ]
        identity = {
            "anchor_policy_count": 20,
            "anchor_policy_identity_root_sha256": "1" * 64,
            "anchor_policy_recipe_sha256": "2" * 64,
            "direct_catalog_sha256": "3" * 64,
            "direct_feature_universe_sha256": "9" * 64,
            "direct_opportunity_count": 10,
            "direct_opportunity_lattice_sha256": "8" * 64,
            "entry_exit_recipe_sha256": "4" * 64,
            "feature_event_universe_sha256": _canonical_sha256(leaves),
            "meta_catalog_sha256": "5" * 64,
            "stage_a_chunk_plan_sha256": "6" * 64,
            "stage_a_chunk_count": 64,
            "stage_a_policy_rows_per_chunk_maximum": 1,
            "structural_opportunity_count": 10,
            "structural_opportunity_lattice_lookahead_seconds": 25_200,
            "structural_opportunity_lattice_sha256": "7" * 64,
        }
        return {
            **identity,
            "feature_mask_chunk_artifacts": leaves,
            "schema": "systematic_fx.ai_all_cases_search_universe_payload.v1",
            "universe_root_sha256": _canonical_sha256(identity),
        }

    def search(_root: Path, _config: AllCasesConfig, universe: object) -> object:
        calls.append("open_search_outcomes_train_select")
        assert isinstance(universe, dict)
        if search_error is not None:
            raise search_error
        selected = identifiers[:search_selection_count]
        if invalid_search:
            selected = (*selected, "f" * 64)
        stage_a = (policy_id,)
        complete_root = "6" * 64
        stage_a_proof = _canonical_sha256(
            {
                "selected_policy_ids": list(stage_a),
                "universe_root_sha256": universe["universe_root_sha256"],
            }
        )
        complete_derivation = _canonical_sha256(
            {
                "complete_symbolic_candidate_count": len(identifiers),
                "complete_symbolic_candidate_root_sha256": complete_root,
                "entry_exit_recipe_sha256": universe["entry_exit_recipe_sha256"],
                "stage_a_selected_policy_ids": list(stage_a),
            }
        )
        chunk_artifacts = [
            {
                "artifact_sha256": f"{index + 1:x}" * 64,
                "chunk_index": 0,
                "phase": phase,
            }
            for index, phase in enumerate(_PHASES)
        ]
        return {
            "complete_symbolic_candidate_count": len(identifiers),
            "complete_symbolic_candidate_root_sha256": complete_root,
            "complete_symbolic_derivation_sha256": complete_derivation,
            "direct_candidate_count": 0,
            "direct_fit_cache_aggregate": _empty_fit_cache_aggregate("DIRECT"),
            "evaluated_candidate_ids": list(identifiers),
            "evaluated_family_sha256": _canonical_sha256(list(identifiers)),
            "meta_candidate_count": 0,
            "meta_fit_cache_aggregate": _empty_fit_cache_aggregate("META"),
            "meta_plan_sha256": "b" * 64,
            "model_artifacts": [],
            "search_chunk_artifacts": chunk_artifacts,
            "search_chunk_leaf_closure_sha256": _canonical_sha256(chunk_artifacts),
            "search_subledger_head_sha256": "c" * 64,
            "schema": "systematic_fx.ai_all_cases_search_result_payload.v1",
            "selected_candidate_ids": list(selected),
            "stage_a_selected_policy_ids": list(stage_a),
            "stage_a_selection_artifact_sha256": "7" * 64,
            "stage_a_selection_proof_sha256": stage_a_proof,
            "stage_b_plan_sha256": "d" * 64,
            "strategy_artifacts": [
                {
                    "candidate_id": candidate_id,
                    "candidate_kind": "SYMBOLIC",
                    "family_key": "fixture-family",
                    "strategy_document": {
                        "candidate_id": candidate_id,
                        "catalog_selection_rank": index,
                        "recipe": "fixture",
                    },
                    "strategy_sha256": _canonical_sha256(
                        {
                            "candidate_id": candidate_id,
                            "catalog_selection_rank": index,
                            "recipe": "fixture",
                        }
                    ),
                }
                for index, candidate_id in enumerate(selected, start=1)
            ],
            "symbolic_top24_artifact_sha256": "e" * 64,
            "universe_root_sha256": universe["universe_root_sha256"],
        }

    def freeze_walk(
        _root: Path,
        _config: AllCasesConfig,
        candidate_ids: tuple[str, ...],
        search_result: object,
    ) -> object:
        calls.append("freeze_all_five_walk_forward_masks")
        assert isinstance(search_result, dict)
        mask_documents = [
            {
                "candidate_id": candidate_id,
                "fold_key": fold_key,
                "mask_kind": "SYMBOLIC",
                "mask_sha256": _canonical_sha256([candidate_id, fold_key]),
            }
            for candidate_id in candidate_ids
            for fold_key in ("WF1", "WF2", "WF3", "WF4", "WF5")
        ]
        return {
            "candidate_ids": list(candidate_ids),
            "fold_keys": ["WF1", "WF2", "WF3", "WF4", "WF5"],
            "mask_commitment_sha256": _canonical_sha256(mask_documents),
            "mask_documents": mask_documents,
            "schema": "systematic_fx.ai_all_cases_walk_forward_masks_payload.v1",
        }

    def walk(
        _root: Path,
        _config: AllCasesConfig,
        candidate_ids: tuple[str, ...],
        masks: object,
        search_result: object,
    ) -> object:
        calls.append("open_all_walk_forward_outcomes_atomically")
        assert isinstance(masks, dict) and isinstance(search_result, dict)
        results = []
        for index, candidate_id in enumerate(candidate_ids):
            selected = index < walk_finalist_count
            evidence = _economic_evidence("walk-forward")
            if not selected:
                evidence["fill_count"] = 99
                evidence["fold_fill_counts"] = [19, 20, 20, 20, 20]
            result_document = {
                "bh_rejected": True,
                "candidate_descriptor": candidate_descriptor(candidate_id, index + 1),
                "candidate_kind": "SYMBOLIC",
                "catalog_selection_rank": index + 1,
                "daily_net_ticks_by_world": _daily_worlds(),
                "economic_gate_pass": selected,
                "evidence": evidence,
                "failure_reasons": [] if selected else ["FILLS_LT_100"],
                "finalist_rank": index + 1 if selected else None,
                "p_star": _fraction(1, 1_048_576),
                "sample_eligible": True,
                "selected_before_budget": selected,
                "selected_finalist": selected,
            }
            results.append(
                {
                    "candidate_id": candidate_id,
                    "result_document": result_document,
                    "result_sha256": _canonical_sha256(result_document),
                }
            )
        finalists = list(candidate_ids[:walk_finalist_count])
        return {
            "all_folds_complete": True,
            "candidate_ids": list(candidate_ids),
            "candidate_results": results,
            "finalist_candidate_ids": finalists,
            "fold_keys": ["WF1", "WF2", "WF3", "WF4", "WF5"],
            "multiplicity_sha256": _canonical_sha256(
                {
                    "candidate_results": results,
                    "finalist_candidate_ids": finalists,
                    "method": "BENJAMINI_HOCHBERG",
                }
            ),
            "schema": "systematic_fx.ai_all_cases_walk_forward_result_payload.v1",
        }

    def freeze_holdout(
        _root: Path,
        _config: AllCasesConfig,
        candidate_ids: tuple[str, ...],
        walk_result: object,
    ) -> object:
        calls.append("freeze_holdout_masks")
        assert isinstance(walk_result, dict)
        mask_documents = [
            {
                "candidate_id": candidate_id,
                "mask_kind": "SYMBOLIC",
                "mask_sha256": _canonical_sha256([candidate_id, "HOLDOUT"]),
            }
            for candidate_id in candidate_ids
        ]
        return {
            "candidate_ids": list(candidate_ids),
            "mask_commitment_sha256": _canonical_sha256(mask_documents),
            "mask_documents": mask_documents,
            "schema": "systematic_fx.ai_all_cases_holdout_masks_payload.v1",
        }

    def holdout(
        _root: Path,
        _config: AllCasesConfig,
        candidate_ids: tuple[str, ...],
        masks: object,
    ) -> object:
        calls.append("open_holdout_outcomes")
        assert isinstance(masks, dict)
        if holdout_error is not None:
            raise holdout_error
        results = []
        for index, candidate_id in enumerate(candidate_ids):
            result_document = {
                "candidate_descriptor": candidate_descriptor(candidate_id, index + 1),
                "candidate_kind": "SYMBOLIC",
                "catalog_selection_rank": index + 1,
                "daily_net_ticks_by_world": _daily_worlds(),
                "economic_gate_pass": True,
                "evidence": _economic_evidence("holdout"),
                "failure_reasons": [],
                "holm_rejected": True,
                "p_star": _fraction(1, 1_048_576),
                "sample_eligible": True,
                "verdict_pass": True,
            }
            results.append(
                {
                    "candidate_id": candidate_id,
                    "result_document": result_document,
                    "result_sha256": _canonical_sha256(result_document),
                }
            )
        classification = "ONE_SHOT_UNSEALED_ALL_CASES_HOLDOUT_DIAGNOSTIC_PASS"
        return {
            "candidate_ids": list(candidate_ids),
            "candidate_results": results,
            "classification": classification,
            "holm_sha256": _canonical_sha256(
                {
                    "candidate_results": results,
                    "classification": classification,
                    "method": "HOLM_STEP_DOWN",
                }
            ),
            "schema": "systematic_fx.ai_all_cases_holdout_result_payload.v1",
        }

    return _AllCasesServices(
        freeze_search_universe=freeze_search,
        train_select_search=search,
        freeze_walk_forward_masks=freeze_walk,
        evaluate_walk_forward=walk,
        freeze_holdout_masks=freeze_holdout,
        evaluate_holdout=holdout,
    )


def _root(tmp_path: Path) -> Path:
    run_root = tmp_path / DEFAULT_AI_ALL_CASES_ROOT
    run_root.mkdir(parents=True)
    return run_root


def _event_types(run_root: Path) -> tuple[str, ...]:
    events = _Ledger(run_root / "ledger", create=False).verify()
    return tuple(event.event_type for event in events)


def _rechain_enveloped_event(
    run_root: Path,
    event_type: str,
    *,
    mutate_payload: object,
    mutate_event_payload: Mapping[str, object] | None = None,
) -> None:
    """Rewrite one synthetic immutable envelope and the following hash chain."""

    event_paths = tuple(sorted((run_root / "ledger/events").glob("event-*.json")))
    documents = [json.loads(path.read_bytes()) for path in event_paths]
    index = next(
        offset for offset, document in enumerate(documents) if document["event_type"] == event_type
    )
    event = documents[index]
    role_field = run_module._ARTIFACT_ROLES[event_type][0]
    identity = event["payload"][role_field]
    old_path = run_root / "artifacts" / identity["relative_path"]
    envelope = json.loads(old_path.read_bytes())
    envelope["payload"] = mutate_payload
    raw = run_module._canonical_json_bytes(envelope)
    digest = __import__("hashlib").sha256(raw).hexdigest()
    prefix = identity["relative_path"].rpartition("-")[0]
    new_name = f"{prefix}-{digest}.json"
    old_path.unlink()
    replacement = run_root / "artifacts" / new_name
    replacement.write_bytes(raw)
    replacement.chmod(0o444)
    event["payload"][role_field] = {
        **identity,
        "byte_size": len(raw),
        "relative_path": new_name,
        "sha256": digest,
    }
    if mutate_event_payload is not None:
        event["payload"].update(mutate_event_payload)
    for offset in range(index, len(documents)):
        if offset > index:
            documents[offset]["predecessor_sha256"] = _canonical_sha256(documents[offset - 1])
        path = event_paths[offset]
        path.chmod(0o644)
        path.write_bytes(run_module._canonical_json_bytes(documents[offset]))
        path.chmod(0o444)


def _rechain_request_artifact(run_root: Path) -> None:
    event_paths = tuple(sorted((run_root / "ledger/events").glob("event-*.json")))
    documents = [json.loads(path.read_bytes()) for path in event_paths]
    identity = documents[0]["payload"]["request_artifact"]
    old_path = run_root / "artifacts" / identity["relative_path"]
    request = json.loads(old_path.read_bytes())
    request["limitations"].append("FORGED_RECHAINED_REQUEST")
    raw = run_module._canonical_json_bytes(request)
    digest = __import__("hashlib").sha256(raw).hexdigest()
    prefix = identity["relative_path"].rpartition("-")[0]
    new_name = f"{prefix}-{digest}.json"
    old_path.unlink()
    replacement = run_root / "artifacts" / new_name
    replacement.write_bytes(raw)
    replacement.chmod(0o444)
    documents[0]["payload"]["request_artifact"] = {
        **identity,
        "byte_size": len(raw),
        "relative_path": new_name,
        "sha256": digest,
    }
    for offset, document in enumerate(documents):
        document["request_sha256"] = digest
        if offset:
            document["predecessor_sha256"] = _canonical_sha256(documents[offset - 1])
        path = event_paths[offset]
        path.chmod(0o644)
        path.write_bytes(run_module._canonical_json_bytes(document))
        path.chmod(0o444)


def test_public_entry_points_are_root_only_and_have_no_service_injection() -> None:
    for function in (precommit_ai_all_cases, run_ai_all_cases, verify_ai_all_cases):
        assert tuple(inspect.signature(function).parameters) == ("project_root",)


def test_attempt2_identity_is_fixed_and_predecessor_guard_precedes_root_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert DEFAULT_AI_ALL_CASES_ROOT == AI_ALL_CASES_RUN_RELATIVE_ROOT
    assert DEFAULT_AI_ALL_CASES_ROOT.as_posix().endswith("ai_all_cases_v1_attempt2")
    config = _config(tmp_path)
    expected_root = tmp_path / DEFAULT_AI_ALL_CASES_ROOT
    calls: list[str] = []
    monkeypatch.setattr(run_module, "_project_root", lambda _value: tmp_path)
    monkeypatch.setattr(
        run_module,
        "_require_trusted_bootstrap_runtime",
        lambda _root: calls.append("bootstrap"),
    )
    monkeypatch.setattr(
        run_module,
        "load_ai_all_cases_config",
        lambda _root: calls.append("config") or config,
    )
    monkeypatch.setattr(
        run_module,
        "_load_validated_dataset_contract",
        lambda _root: calls.append("dataset"),
    )

    def predecessor(_root: Path) -> None:
        assert not expected_root.exists()
        calls.append("predecessor")

    def fixed(_root: Path, *, create: bool) -> Path:
        assert create is True
        calls.append("create_attempt2")
        return expected_root

    monkeypatch.setattr(run_module, "verify_failed_predecessor_attempt", predecessor)
    monkeypatch.setattr(run_module, "_fixed_run_root", fixed)

    assert run_module._prepare_mutation(tmp_path) == (tmp_path, config, expected_root)
    assert calls == ["bootstrap", "config", "dataset", "predecessor", "create_attempt2"]


def test_fresh_public_verify_guards_predecessor_before_attempt2_root_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    run_root = tmp_path / DEFAULT_AI_ALL_CASES_ROOT
    result = SimpleNamespace(status="VERIFIED")
    calls: list[str] = []
    monkeypatch.setattr(run_module, "_project_root", lambda _value: tmp_path)
    monkeypatch.setattr(
        run_module,
        "_require_trusted_bootstrap_runtime",
        lambda _root: calls.append("bootstrap"),
    )
    monkeypatch.setattr(
        run_module,
        "load_ai_all_cases_config",
        lambda _root: calls.append("config") or config,
    )
    monkeypatch.setattr(
        run_module,
        "_load_validated_dataset_contract",
        lambda _root: calls.append("dataset"),
    )
    monkeypatch.setattr(
        run_module,
        "verify_failed_predecessor_attempt",
        lambda _root: calls.append("predecessor"),
    )
    monkeypatch.setattr(
        run_module,
        "_fixed_run_root",
        lambda _root, *, create: calls.append(f"root:{create}") or run_root,
    )
    monkeypatch.setattr(
        run_module,
        "_default_services",
        lambda *, verify_only: calls.append(f"services:{verify_only}") or object(),
    )
    monkeypatch.setattr(
        run_module,
        "_verify_with_services",
        lambda *_args: calls.append("verify") or result,
    )

    assert verify_ai_all_cases(tmp_path) is result
    assert calls == [
        "bootstrap",
        "config",
        "dataset",
        "predecessor",
        "root:False",
        "services:True",
        "verify",
    ]


def test_full_lifecycle_freezes_every_stage_before_outcomes_and_publishes_0444(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    calls: list[str] = []
    result = _run_with_services(
        tmp_path,
        _config(tmp_path),
        run_root,
        _services(calls, search_selection_count=5, walk_finalist_count=2),
    )

    assert result.status == "COMPLETED"
    assert calls == [
        "freeze_search_features_events_catalog",
        "open_search_outcomes_train_select",
        "freeze_all_five_walk_forward_masks",
        "open_all_walk_forward_outcomes_atomically",
        "freeze_holdout_masks",
        "open_holdout_outcomes",
    ]
    assert _event_types(run_root) == (
        "PRECOMMITTED",
        "SEARCH_UNIVERSE_FROZEN",
        "SEARCH_RESULTS_RELEASED",
        "WALK_FORWARD_MASKS_FROZEN",
        "WALK_FORWARD_RESULTS_RELEASED",
        "HOLDOUT_AUTHORIZED",
        "HOLDOUT_MASKS_FROZEN",
        "HOLDOUT_RESULTS_RELEASED",
        "COMPLETED",
    )
    for path in (
        *list((run_root / "artifacts").glob("*.json")),
        *list((run_root / "ledger/events").glob("*.json")),
    ):
        assert stat.S_IMODE(path.stat().st_mode) == 0o444


def test_zero_search_finalists_skips_every_later_payload(tmp_path: Path) -> None:
    run_root = _root(tmp_path)
    calls: list[str] = []
    result = _run_with_services(
        tmp_path,
        _config(tmp_path),
        run_root,
        _services(calls, search_selection_count=0, walk_finalist_count=0),
    )

    assert result.status == "COMPLETED"
    assert calls == [
        "freeze_search_features_events_catalog",
        "open_search_outcomes_train_select",
    ]
    assert _event_types(run_root) == (
        "PRECOMMITTED",
        "SEARCH_UNIVERSE_FROZEN",
        "SEARCH_RESULTS_RELEASED",
        "WALK_FORWARD_SKIPPED",
        "HOLDOUT_SKIPPED",
        "COMPLETED",
    )


def test_zero_walk_forward_finalists_never_calls_holdout_services(tmp_path: Path) -> None:
    run_root = _root(tmp_path)
    calls: list[str] = []
    _run_with_services(
        tmp_path,
        _config(tmp_path),
        run_root,
        _services(calls, search_selection_count=4, walk_finalist_count=0),
    )

    assert "freeze_holdout_masks" not in calls
    assert "open_holdout_outcomes" not in calls
    assert _event_types(run_root)[-2:] == ("HOLDOUT_SKIPPED", "COMPLETED")


@pytest.mark.parametrize(
    ("stop_after", "search_count", "walk_count", "event_type", "forged_reason"),
    (
        ("WALK_FORWARD_SKIPPED", 0, 0, "WALK_FORWARD_SKIPPED", "FORGED_SEARCH_SKIP"),
        ("HOLDOUT_SKIPPED", 4, 0, "HOLDOUT_SKIPPED", "FORGED_HOLDOUT_SKIP"),
    ),
)
def test_resume_rejects_rechained_skip_before_completion(
    tmp_path: Path,
    stop_after: str,
    search_count: int,
    walk_count: int,
    event_type: str,
    forged_reason: str,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    _run_with_services(
        tmp_path,
        config,
        run_root,
        _services([], search_selection_count=search_count, walk_finalist_count=walk_count),
        stop_after=stop_after,
    )
    stage = "WALK_FORWARD" if event_type == "WALK_FORWARD_SKIPPED" else "HOLDOUT"
    _rechain_enveloped_event(
        run_root,
        event_type,
        mutate_payload=run_module._skip_document(stage, forged_reason),
        mutate_event_payload={"reason": forged_reason},
    )

    with pytest.raises(AllCasesIntegrityError, match="skip branch"):
        _run_with_services(
            tmp_path,
            config,
            run_root,
            _services([], search_selection_count=search_count, walk_finalist_count=walk_count),
        )
    assert "COMPLETED" not in _event_types(run_root)
    assert _event_types(run_root)[-1] == "FAILED"


def test_transient_oserror_leaves_prefix_and_resumes_without_rewriting(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    failed_calls: list[str] = []
    with pytest.raises(OSError, match="temporary"):
        _run_with_services(
            tmp_path,
            _config(tmp_path),
            run_root,
            _services(
                failed_calls,
                search_selection_count=0,
                walk_finalist_count=0,
                search_error=OSError("temporary read failure"),
            ),
        )
    before = tuple(
        (path.name, path.read_bytes()) for path in sorted((run_root / "ledger/events").iterdir())
    )
    assert _event_types(run_root) == ("PRECOMMITTED", "SEARCH_UNIVERSE_FROZEN")

    resumed_calls: list[str] = []
    result = _run_with_services(
        tmp_path,
        _config(tmp_path),
        run_root,
        _services(resumed_calls, search_selection_count=0, walk_finalist_count=0),
    )
    after_prefix = tuple(
        (path.name, path.read_bytes())
        for path in sorted((run_root / "ledger/events").iterdir())[:2]
    )
    assert result.status == "COMPLETED"
    assert after_prefix == before
    assert resumed_calls == [
        "freeze_search_features_events_catalog",
        "open_search_outcomes_train_select",
    ]


def test_hard_artifact_cap_rejects_projected_publication_before_any_leaf(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    original = _config(tmp_path)
    document = original.as_dict()
    document["compute_caps"]["artifact_bytes_maximum"] = 1
    raw = json.dumps(document, sort_keys=True, separators=(",", ":")).encode("ascii")
    config = AllCasesConfig(
        path=original.path,
        file_sha256=original.file_sha256,
        semantic_sha256=original.semantic_sha256,
        code_commit=original.code_commit,
        implementation_sha256=original.implementation_sha256,
        dependency_lock_sha256=original.dependency_lock_sha256,
        precommitted_at_utc=original.precommitted_at_utc,
        canonical_bytes=raw,
    )
    with pytest.raises(AllCasesIntegrityError, match="artifact-byte cap"):
        _run_with_services(
            tmp_path,
            config,
            run_root,
            _services([], search_selection_count=0, walk_finalist_count=0),
        )
    assert tuple((run_root / "artifacts").iterdir()) == ()
    assert tuple((run_root / "ledger/events").iterdir()) == ()


def test_mutation_resource_preflight_rejects_huge_leaf_before_reading_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_root = _root(tmp_path)
    artifacts = run_root / "artifacts"
    artifacts.mkdir()
    huge = artifacts / f"search-results-{'a' * 64}.json"
    huge.write_bytes(b"x" * 32_768)
    huge.chmod(0o444)
    original = _config(tmp_path)
    document = original.as_dict()
    document["compute_caps"]["artifact_bytes_maximum"] = 20_000
    raw = json.dumps(document, sort_keys=True, separators=(",", ":")).encode("ascii")
    config = AllCasesConfig(
        path=original.path,
        file_sha256=original.file_sha256,
        semantic_sha256=original.semantic_sha256,
        code_commit=original.code_commit,
        implementation_sha256=original.implementation_sha256,
        dependency_lock_sha256=original.dependency_lock_sha256,
        precommitted_at_utc=original.precommitted_at_utc,
        canonical_bytes=raw,
    )
    original_read_bytes = Path.read_bytes

    def guarded_read_bytes(path: Path) -> bytes:
        if path == huge:
            raise AssertionError("huge untrusted leaf was read before the hard cap")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)
    with pytest.raises(AllCasesIntegrityError, match="artifact-byte cap"):
        _run_with_services(
            tmp_path,
            config,
            run_root,
            _services([], search_selection_count=0, walk_finalist_count=0),
        )


def test_outer_and_pipeline_guards_enforce_distinct_wall_deadlines(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = _config(tmp_path)
    document = original.as_dict()
    document["compute_caps"] = {
        **document["compute_caps"],
        "artifact_bytes_maximum": 1 << 40,
        "resident_set_bytes_maximum": 1 << 40,
        "search_wall_seconds_maximum": 7,
        "verifier_wall_seconds_maximum": 3,
    }
    config = AllCasesConfig(
        path=original.path,
        file_sha256=original.file_sha256,
        semantic_sha256=original.semantic_sha256,
        code_commit=original.code_commit,
        implementation_sha256=original.implementation_sha256,
        dependency_lock_sha256=original.dependency_lock_sha256,
        precommitted_at_utc=original.precommitted_at_utc,
        canonical_bytes=json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii"),
    )
    clock = [100.0]
    monkeypatch.setattr(run_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(pipeline_module.time, "monotonic", lambda: clock[0])
    run_root = _root(tmp_path)
    outer_mutation = run_module._RunResourceGuard(config, run_root, verifier=False)
    outer_verifier = run_module._RunResourceGuard(config, run_root, verifier=True)
    pipeline_mutation = pipeline_module._PipelineResourceGuard(
        tmp_path,
        config,
        verifier=False,
    )
    pipeline_verifier = pipeline_module._PipelineResourceGuard(
        tmp_path,
        config,
        verifier=True,
    )

    clock[0] = 104.0
    outer_mutation.check("SEARCH_BOUNDARY")
    pipeline_mutation.check("SEARCH_CHUNK_BOUNDARY")
    with pytest.raises(AllCasesIntegrityError, match="wall-time cap"):
        outer_verifier.check("VERIFY_BOUNDARY")
    with pytest.raises(AllCasesPipelineError, match="wall-time cap"):
        pipeline_verifier.check("VERIFY_CHUNK_BOUNDARY")

    clock[0] = 108.0
    with pytest.raises(AllCasesIntegrityError, match="wall-time cap"):
        outer_mutation.check("SEARCH_BOUNDARY")
    with pytest.raises(AllCasesPipelineError, match="wall-time cap"):
        pipeline_mutation.check("SEARCH_CHUNK_BOUNDARY")


def test_deterministic_lineage_error_appends_failed_terminal(tmp_path: Path) -> None:
    run_root = _root(tmp_path)
    with pytest.raises(AllCasesIntegrityError, match="outside the frozen evaluated family"):
        _run_with_services(
            tmp_path,
            _config(tmp_path),
            run_root,
            _services(
                [],
                search_selection_count=2,
                walk_finalist_count=0,
                invalid_search=True,
            ),
        )
    assert _event_types(run_root) == (
        "PRECOMMITTED",
        "SEARCH_UNIVERSE_FROZEN",
        "FAILED",
    )


def test_non_oserror_search_failure_is_terminal_and_rerun_calls_no_services(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    with pytest.raises(AllCasesIntegrityError, match="ValueError"):
        _run_with_services(
            tmp_path,
            _config(tmp_path),
            run_root,
            _services(
                [],
                search_selection_count=2,
                walk_finalist_count=0,
                search_error=ValueError("bad deterministic matrix"),
            ),
        )
    assert _event_types(run_root) == (
        "PRECOMMITTED",
        "SEARCH_UNIVERSE_FROZEN",
        "FAILED",
    )

    rerun_calls: list[str] = []
    result = _run_with_services(
        tmp_path,
        _config(tmp_path),
        run_root,
        _services(rerun_calls, search_selection_count=2, walk_finalist_count=0),
    )
    assert result.status == "FAILED"
    assert rerun_calls == []

    verify_calls: list[str] = []
    verified = _verify_with_services(
        tmp_path,
        _config(tmp_path),
        run_root,
        _services(verify_calls, search_selection_count=2, walk_finalist_count=0),
    )
    assert verified.status == "FAILED"
    assert verify_calls == ["freeze_search_features_events_catalog"]


def test_failed_terminal_request_rechain_is_rejected_by_public_return_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    with pytest.raises(AllCasesIntegrityError):
        _run_with_services(
            tmp_path,
            config,
            run_root,
            _services(
                [],
                search_selection_count=1,
                walk_finalist_count=0,
                search_error=ValueError("terminal failure"),
            ),
        )
    _rechain_request_artifact(run_root)
    monkeypatch.setattr(
        run_module,
        "_prepare_mutation",
        lambda _project_root: (tmp_path, config, run_root),
    )

    with pytest.raises(AllCasesIntegrityError, match="request"):
        run_ai_all_cases(tmp_path)


@pytest.mark.parametrize("terminal", ("COMPLETED", "FAILED"))
def test_public_terminal_return_rejects_staging_temp_without_mutating_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    terminal: str,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    if terminal == "COMPLETED":
        _run_with_services(
            tmp_path,
            config,
            run_root,
            _services([], search_selection_count=0, walk_finalist_count=0),
        )
    else:
        with pytest.raises(AllCasesIntegrityError):
            _run_with_services(
                tmp_path,
                config,
                run_root,
                _services(
                    [],
                    search_selection_count=1,
                    walk_finalist_count=0,
                    search_error=ValueError("terminal failure"),
                ),
            )
    with run_module._exclusive_mutation(run_root):
        pass
    temporary = run_root / "staging/artifacts/.all-cases-report-crash.tmp"
    temporary.write_bytes(b"bounded-but-terminal-invalid")
    before = run_module._tree_snapshot(run_root)
    monkeypatch.setattr(
        run_module,
        "_prepare_mutation",
        lambda _project_root: (tmp_path, config, run_root),
    )

    with pytest.raises(AllCasesIntegrityError, match="ungoverned leaf|publisher temporary"):
        run_ai_all_cases(tmp_path)
    assert temporary.read_bytes() == b"bounded-but-terminal-invalid"
    assert run_module._tree_snapshot(run_root) == before


@pytest.mark.parametrize("terminal", ("COMPLETED", "FAILED"))
def test_public_terminal_return_does_not_launder_ledger_staging_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    terminal: str,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    if terminal == "COMPLETED":
        _run_with_services(
            tmp_path,
            config,
            run_root,
            _services([], search_selection_count=0, walk_finalist_count=0),
        )
    else:
        with pytest.raises(AllCasesIntegrityError):
            _run_with_services(
                tmp_path,
                config,
                run_root,
                _services(
                    [],
                    search_selection_count=1,
                    walk_finalist_count=0,
                    search_error=ValueError("terminal failure"),
                ),
            )
    with run_module._exclusive_mutation(run_root):
        pass
    temporary = run_root / "ledger/staging/.event-99999999-crash.tmp"
    temporary.write_bytes(b"bounded-but-terminal-invalid")
    before = run_module._tree_snapshot(run_root)
    monkeypatch.setattr(
        run_module,
        "_prepare_mutation",
        lambda _project_root: (tmp_path, config, run_root),
    )

    with pytest.raises(AllCasesIntegrityError, match="ledger verification.*temporary"):
        run_ai_all_cases(tmp_path)
    assert temporary.read_bytes() == b"bounded-but-terminal-invalid"
    assert run_module._tree_snapshot(run_root) == before


@pytest.mark.parametrize(
    ("publisher", "terminal"),
    (
        ("ARTIFACT", False),
        ("LEDGER", False),
        ("ARTIFACT", True),
        ("LEDGER", True),
    ),
)
def test_exact_linked_publisher_temp_is_closed_before_strict_resume_probe(
    tmp_path: Path,
    publisher: str,
    terminal: bool,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    services = _services([], search_selection_count=0, walk_finalist_count=0)
    _run_with_services(
        tmp_path,
        config,
        run_root,
        services,
        stop_after=None if terminal else "PRECOMMITTED",
    )
    events = _Ledger(run_root / "ledger", create=False).verify()
    if publisher == "LEDGER":
        destination = run_root / f"ledger/events/event-{len(events):08d}.json"
        temporary = run_root / f"ledger/staging/.event-{len(events):08d}-crash.tmp"
    else:
        event = events[-1] if terminal else events[0]
        identity = run_module._artifact_from_event(event)
        destination = run_root / "artifacts" / identity.relative_path
        prefix = destination.name.rpartition("-")[0]
        temporary = run_root / f"staging/artifacts/.{prefix}-crash.tmp"
    temporary.hardlink_to(destination)
    assert destination.stat().st_nlink == 2

    result = _run_with_services(tmp_path, config, run_root, services)

    assert result.status == "COMPLETED"
    assert not temporary.exists()
    assert destination.stat().st_nlink == 1


def test_linked_publisher_temp_with_wrong_destination_role_is_rejected_unchanged(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    _run_with_services(
        tmp_path,
        config,
        run_root,
        _services([], search_selection_count=0, walk_finalist_count=0),
        stop_after="PRECOMMITTED",
    )
    event = _Ledger(run_root / "ledger", create=False).verify()[0]
    destination = run_root / "artifacts" / run_module._artifact_from_event(event).relative_path
    temporary = run_root / "staging/artifacts/.wrong-role-crash.tmp"
    temporary.hardlink_to(destination)
    before = run_module._tree_snapshot(run_root)
    calls: list[str] = []

    with pytest.raises(AllCasesIntegrityError, match="destination name"):
        _run_with_services(
            tmp_path,
            config,
            run_root,
            _services(calls, search_selection_count=0, walk_finalist_count=0),
        )

    assert calls == []
    assert run_module._tree_snapshot(run_root) == before


def test_linked_ledger_temp_requires_exact_typed_event_before_cleanup(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    _run_with_services(
        tmp_path,
        config,
        run_root,
        _services([], search_selection_count=0, walk_finalist_count=0),
        stop_after="PRECOMMITTED",
    )
    destination = run_root / "ledger/events/event-00000002.json"
    destination.write_bytes(b"{}")
    destination.chmod(0o444)
    temporary = run_root / "ledger/staging/.event-00000002-crash.tmp"
    temporary.hardlink_to(destination)
    before = run_module._tree_snapshot(run_root)
    calls: list[str] = []

    with pytest.raises(AllCasesIntegrityError, match="event schema"):
        _run_with_services(
            tmp_path,
            config,
            run_root,
            _services(calls, search_selection_count=0, walk_finalist_count=0),
        )

    assert calls == []
    assert run_module._tree_snapshot(run_root) == before


def test_linked_ledger_temp_replays_the_entire_prefix_before_cleanup(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    _run_with_services(
        tmp_path,
        config,
        run_root,
        _services([], search_selection_count=0, walk_finalist_count=0),
        stop_after="PRECOMMITTED",
    )
    first = _Ledger(run_root / "ledger", create=False).verify()[0]
    second = run_module.AllCasesLedgerEvent(
        sequence=2,
        predecessor_sha256=first.sha256,
        event_type="PRECOMMITTED",
        request_sha256=first.request_sha256,
        recorded_at_utc=first.recorded_at_utc,
        payload=first.payload,
    )
    third = run_module.AllCasesLedgerEvent(
        sequence=3,
        predecessor_sha256=second.sha256,
        event_type="SEARCH_UNIVERSE_FROZEN",
        request_sha256=first.request_sha256,
        recorded_at_utc=first.recorded_at_utc,
        payload={
            "universe_artifact": {
                "artifact_type": "AI_ALL_CASES_SEARCH_FEATURE_EVENT_UNIVERSE",
                "byte_size": 0,
                "relative_path": "search-universe.json",
                "sha256": "a" * 64,
            },
            "universe_root_sha256": "b" * 64,
        },
    )
    events_root = run_root / "ledger/events"
    for sequence, event in ((2, second), (3, third)):
        path = events_root / f"event-{sequence:08d}.json"
        path.write_bytes(run_module._canonical_json_bytes(event.as_dict()))
        path.chmod(0o444)
    destination = events_root / "event-00000003.json"
    temporary = run_root / "ledger/staging/.event-00000003-crash.tmp"
    temporary.hardlink_to(destination)
    before = run_module._tree_snapshot(run_root)
    calls: list[str] = []

    with pytest.raises(AllCasesIntegrityError, match="transition"):
        _run_with_services(
            tmp_path,
            config,
            run_root,
            _services(calls, search_selection_count=0, walk_finalist_count=0),
        )

    assert calls == []
    assert run_module._tree_snapshot(run_root) == before


def test_public_terminal_probe_closes_exact_linked_final_ledger_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    services = _services([], search_selection_count=0, walk_finalist_count=0)
    _run_with_services(tmp_path, config, run_root, services)
    events = _Ledger(run_root / "ledger", create=False).verify()
    destination = run_root / f"ledger/events/event-{len(events):08d}.json"
    temporary = run_root / f"ledger/staging/.event-{len(events):08d}-crash.tmp"
    temporary.hardlink_to(destination)
    monkeypatch.setattr(
        run_module,
        "_prepare_mutation",
        lambda _project_root: (tmp_path, config, run_root),
    )
    monkeypatch.setattr(run_module, "_default_services", lambda **_kwargs: services)

    result = run_ai_all_cases(tmp_path)

    assert result.status == "COMPLETED"
    assert not temporary.exists()
    assert destination.stat().st_nlink == 1


def test_public_nonterminal_probe_closes_exact_linked_request_artifact_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    _run_with_services(
        tmp_path,
        config,
        run_root,
        _services([], search_selection_count=0, walk_finalist_count=0),
        stop_after="PRECOMMITTED",
    )
    event = _Ledger(run_root / "ledger", create=False).verify()[0]
    destination = run_root / "artifacts" / run_module._artifact_from_event(event).relative_path
    prefix = destination.name.rpartition("-")[0]
    temporary = run_root / f"staging/artifacts/.{prefix}-crash.tmp"
    temporary.hardlink_to(destination)
    services = _services([], search_selection_count=0, walk_finalist_count=0)
    monkeypatch.setattr(
        run_module,
        "_prepare_mutation",
        lambda _project_root: (tmp_path, config, run_root),
    )
    monkeypatch.setattr(run_module, "_default_services", lambda **_kwargs: services)

    result = run_ai_all_cases(tmp_path)

    assert result.status == "COMPLETED"
    assert not temporary.exists()
    assert destination.stat().st_nlink == 1


@pytest.mark.parametrize(
    ("stop_after", "search_count", "walk_count"),
    (
        ("SEARCH_UNIVERSE_FROZEN", 0, 0),
        ("SEARCH_RESULTS_RELEASED", 0, 0),
        ("WALK_FORWARD_SKIPPED", 0, 0),
        ("WALK_FORWARD_RESULTS_RELEASED", 4, 0),
        ("HOLDOUT_SKIPPED", 4, 0),
        ("HOLDOUT_AUTHORIZED", 4, 2),
        ("HOLDOUT_RESULTS_RELEASED", 4, 2),
        (None, 0, 0),
    ),
)
def test_precommit_public_surface_rejects_every_advanced_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stop_after: str | None,
    search_count: int,
    walk_count: int,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    _run_with_services(
        tmp_path,
        config,
        run_root,
        _services(
            [],
            search_selection_count=search_count,
            walk_finalist_count=walk_count,
        ),
        stop_after=stop_after,
    )
    monkeypatch.setattr(
        run_module,
        "_prepare_mutation",
        lambda _project_root: (tmp_path, config, run_root),
    )
    with pytest.raises(run_module.AllCasesRunError, match="use run or verify"):
        precommit_ai_all_cases(tmp_path)


def test_non_oserror_after_holdout_masks_appends_failed_without_result(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    with pytest.raises(AllCasesIntegrityError, match="RuntimeError"):
        _run_with_services(
            tmp_path,
            _config(tmp_path),
            run_root,
            _services(
                [],
                search_selection_count=4,
                walk_finalist_count=2,
                holdout_error=RuntimeError("deterministic evaluator failure"),
            ),
        )
    events = _event_types(run_root)
    assert events[-2:] == ("HOLDOUT_MASKS_FROZEN", "FAILED")
    assert "HOLDOUT_RESULTS_RELEASED" not in events


def test_oserror_after_holdout_masks_is_terminal_and_never_retries_holdout(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    with pytest.raises(AllCasesIntegrityError, match="one-shot holdout service failure"):
        _run_with_services(
            tmp_path,
            _config(tmp_path),
            run_root,
            _services(
                [],
                search_selection_count=4,
                walk_finalist_count=2,
                holdout_error=OSError("one-shot read interrupted"),
            ),
        )
    events = _event_types(run_root)
    assert events[-2:] == ("HOLDOUT_MASKS_FROZEN", "FAILED")
    assert "HOLDOUT_RESULTS_RELEASED" not in events

    rerun_calls: list[str] = []
    result = _run_with_services(
        tmp_path,
        _config(tmp_path),
        run_root,
        _services(rerun_calls, search_selection_count=4, walk_finalist_count=2),
    )
    assert result.status == "FAILED"
    assert rerun_calls == []


@pytest.mark.parametrize(
    "stop_after",
    ("HOLDOUT_AUTHORIZED", "HOLDOUT_MASKS_FROZEN"),
)
def test_preexisting_one_shot_holdout_prefix_is_failed_without_any_service_retry(
    tmp_path: Path,
    stop_after: str,
) -> None:
    run_root = _root(tmp_path)
    _run_with_services(
        tmp_path,
        _config(tmp_path),
        run_root,
        _services([], search_selection_count=4, walk_finalist_count=2),
        stop_after=stop_after,
    )
    resumed_calls: list[str] = []
    result = _run_with_services(
        tmp_path,
        _config(tmp_path),
        run_root,
        _services(resumed_calls, search_selection_count=4, walk_finalist_count=2),
    )
    assert result.status == "FAILED"
    assert resumed_calls == []
    assert _event_types(run_root)[-1] == "FAILED"


@pytest.mark.parametrize(
    ("field", "invalid"),
    (
        ("decision_ns", (1_000.0,)),
        ("label_exit_ns", (True,)),
        ("outcome_span_ids", (True,)),
        ("segment_ids", (True,)),
        ("segment_ids", (0,)),
        ("segment_ids", (2**64,)),
        ("valid_label_paths", (1,)),
    ),
)
def test_direct_oos_response_rejects_bool_integer_coercion(
    field: str,
    invalid: tuple[object, ...],
) -> None:
    day = date(2026, 1, 2)
    schedule = SimpleNamespace(
        contracts=("EURUSD",),
        decision_dates=(day,),
        decision_ns=(1_000,),
        entry_ns=(2_000,),
        entry_schedule_sha256="a" * 64,
        lineage_sha256="b" * 64,
        opportunity_lattice_sha256="c" * 64,
        outcome_span_ids=(7,),
        planned_exit_ns=(3_000,),
        row_ids=("row-1",),
        segment_ids=(9,),
    )
    controls = tuple(
        SimpleNamespace(null_world=world, execution_schedule=schedule)
        for world in ("REAL", "CIRCULAR_TARGET", "MATCHED_TARGET")
    )
    runtime = SimpleNamespace(
        direct_controls=controls,
        direct_response_coordinate=(300, 3_600),
    )
    values: dict[str, object] = {
        "decision_ns": (1_000,),
        "decision_timeframe_seconds": 300,
        "entry_schedule_sha256": "a" * 64,
        "entry_ticks": (100,),
        "fill_ns": (2_000,),
        "horizon_seconds": 3_600,
        "label_exit_ns": (3_000,),
        "opportunity_lattice_sha256": "c" * 64,
        "outcome_contracts": ("EURUSD",),
        "outcome_lineage_sha256": "b" * 64,
        "outcome_span_ids": (7,),
        "row_ids": ("row-1",),
        "segment_ids": (9,),
        "source_dates": (day,),
        "terminal_ticks": (120,),
        "valid_label_paths": (True,),
    }
    values[field] = invalid

    with pytest.raises(AllCasesPipelineError, match="non-exact integer|non-boolean"):
        pipeline_module._direct_partition_evidence(
            SimpleNamespace(), runtime, SimpleNamespace(**values)
        )


def test_pipeline_causal_bar_adapter_preserves_maximum_uint64_segment_id() -> None:
    import numpy as np

    maximum_segment_id = int(np.iinfo(np.uint64).max)
    source_date = date(2026, 1, 2)

    def wrapped_bars(timeframe_seconds: int) -> tuple[object, ...]:
        return tuple(
            SimpleNamespace(
                bar=SimpleNamespace(
                    buy_volume=None,
                    close_ticks=100,
                    contract="6EH6",
                    end_ns=(index + 1) * timeframe_seconds * 1_000_000_000,
                    high_ticks=101,
                    low_ticks=99,
                    open_ticks=100,
                    segment_id=maximum_segment_id,
                    sell_volume=None,
                    source_date=source_date,
                    trade_count=1,
                    volume=1,
                ),
                outcome_span_id=1,
            )
            for index in range(60)
        )

    state = SimpleNamespace(
        bars_by_timeframe={timeframe: wrapped_bars(timeframe) for timeframe in (300, 1_800, 3_600)},
        plan=SimpleNamespace(stage_key="SEARCH"),
    )

    series = pipeline_module._ml_bar_series(state)

    assert set(series) == {300, 1_800, 3_600}
    for timeframe in (300, 1_800, 3_600):
        assert series[timeframe].segment_ids.dtype == np.dtype(np.uint64)
        assert not series[timeframe].segment_ids.flags.writeable
        assert {int(value) for value in series[timeframe].segment_ids} == {maximum_segment_id}


def test_direct_oos_response_preserves_maximum_uint64_segment_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import numpy as np

    from campaigns.ai_all_cases_v1 import ml

    maximum_segment_id = int(np.iinfo(np.uint64).max)
    source_date = date(2026, 1, 2)
    schedule = SimpleNamespace(
        contracts=("EURUSD",),
        decision_dates=(source_date,),
        decision_ns=(1_000,),
        entry_ns=(2_000,),
        entry_schedule_sha256="a" * 64,
        lineage_sha256="b" * 64,
        opportunity_lattice_sha256="c" * 64,
        outcome_span_ids=(7,),
        planned_exit_ns=(3_000,),
        row_ids=("row-1",),
        segment_ids=(maximum_segment_id,),
    )
    controls = tuple(
        SimpleNamespace(
            execution_schedule=schedule,
            null_world=world,
            predictions=SimpleNamespace(admitted=(False,), directions=(ml.TradeDirection.FLAT,)),
        )
        for world in ml.NULL_WORLD_ORDER
    )
    runtime = SimpleNamespace(
        candidate=SimpleNamespace(candidate_id="d" * 64),
        direct_controls=controls,
        direct_response_coordinate=(300, 3_600),
        sample_eligible=False,
    )
    response = SimpleNamespace(
        decision_ns=(1_000,),
        decision_timeframe_seconds=300,
        entry_schedule_sha256="a" * 64,
        entry_ticks=(100,),
        fill_ns=(2_000,),
        horizon_seconds=3_600,
        label_exit_ns=(3_000,),
        opportunity_lattice_sha256="c" * 64,
        outcome_contracts=("EURUSD",),
        outcome_lineage_sha256="b" * 64,
        outcome_span_ids=(7,),
        row_ids=("row-1",),
        segment_ids=np.asarray([maximum_segment_id], dtype=np.uint64),
        source_dates=(source_date,),
        terminal_ticks=(120,),
        valid_label_paths=(True,),
    )
    observed_segments: list[tuple[int, ...]] = []

    def frozen_rows(*_args: object, **kwargs: object) -> object:
        observed_segments.append(tuple(int(value) for value in kwargs["segment_ids"]))
        return SimpleNamespace()

    monkeypatch.setattr(ml, "build_frozen_resolved_outcome_rows", frozen_rows)

    result = pipeline_module._direct_partition_evidence(
        SimpleNamespace(plan=SimpleNamespace(decision_dates=(source_date,))),
        runtime,
        response,
    )

    assert observed_segments == [(maximum_segment_id,)] * len(ml.NULL_WORLD_ORDER)
    assert result["REAL"] == _WorldPartitionEvidence(((source_date.isoformat(), 0),), ())
    assert result["CIRCULAR_TARGET"] is None
    assert result["MATCHED_TARGET"] is None


@pytest.mark.skipif(
    not RUN_REAL_SEARCH_FEATURE_SMOKE,
    reason=(
        "set SYSTEMATIC_FX_RUN_AI_ALL_CASES_REAL_SEARCH_FEATURE_SMOKE=1 for the "
        "real outcome-free Search feature gate"
    ),
)
def test_real_search_feature_adapters_preserve_uint64_lineage_without_opening_outcomes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import numpy as np

    monkeypatch.setattr(
        pipeline_module,
        "_one_second_path_parts",
        lambda *_args, **_kwargs: pytest.fail("feature-only smoke opened 1s outcomes"),
    )
    state = pipeline_module._search_feature_state(ROOT)
    expected_rows = {300: 111_297, 1_800: 18_808, 3_600: 9_418}
    expected_overflow_rows = {300: 57_820, 1_800: 9_764, 3_600: 4_889}
    maximum = 18_437_447_912_945_337_878
    for timeframe, wrapped in state.bars_by_timeframe.items():
        segment_ids = tuple(item.bar.segment_id for item in wrapped)
        assert len(segment_ids) == expected_rows[timeframe]
        assert (
            sum(value > np.iinfo(np.int64).max for value in segment_ids)
            == (expected_overflow_rows[timeframe])
        )
        assert max(segment_ids) == maximum

    bundles = pipeline_module._direct_feature_bundles(state)
    commitment, summaries = pipeline_module._direct_feature_universe_commitment(bundles)

    expected_bundle_rows = {300: 65_729, 1_800: 10_969, 3_600: 5_494}
    assert set(bundles) == {300, 1_800, 3_600}
    assert {
        timeframe: bundle.feature_rows.row_count for timeframe, bundle in bundles.items()
    } == expected_bundle_rows
    assert commitment == "922abd762e3d10f91f5307c1fdc2a0f3b9614a14623397adb262b082020d2801"
    assert commitment == pipeline_module._direct_feature_universe_commitment(bundles)[0]
    assert tuple(item["timeframe_seconds"] for item in summaries) == (300, 1_800, 3_600)
    assert {int(item["timeframe_seconds"]): int(item["row_count"]) for item in summaries} == (
        expected_bundle_rows
    )
    for bundle in bundles.values():
        assert bundle.feature_rows.segment_ids.dtype == np.dtype(np.uint64)
        assert max(int(value) for value in bundle.feature_rows.segment_ids) > np.iinfo(np.int64).max


def test_direct_only_stage_streams_once_and_builds_only_selected_coordinates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import numpy as np

    maximum_segment_id = int(np.iinfo(np.uint64).max)
    candidate_id = _ids(1)[0]
    candidate = _FrozenSearchCandidate(candidate_id, "DIRECT_ML", "direct", 1, {})
    coordinate = (300, 3_600)
    runtime = SimpleNamespace(
        candidate=candidate,
        direct_response_coordinate=coordinate,
        sample_eligible=True,
    )
    entry_ns = 1_000_000_000
    exit_ns = entry_ns + coordinate[1] * 1_000_000_000
    source_date = date(2026, 1, 2)
    feature_rows = SimpleNamespace(
        contracts=("EURUSD",),
        decision_ns=np.asarray([entry_ns - 1], dtype=np.int64),
        entry_ns=np.asarray([entry_ns], dtype=np.int64),
        entry_schedule_sha256="a" * 64,
        outcome_span_ids=np.asarray([1], dtype=np.int64),
        row_count=1,
        row_ids=("row-1",),
        segment_ids=np.asarray([maximum_segment_id], dtype=np.uint64),
        source_dates=(source_date,),
    )
    feature_bundle = pipeline_module._DirectFeatureBundle(
        feature_rows,
        (100,),
        SimpleNamespace(artifact_sha256="b" * 64),
    )
    path = SimpleNamespace(
        ends=(exit_ns,),
        lineage=("EURUSD", 1, maximum_segment_id),
        rows=(
            SimpleNamespace(
                bar=SimpleNamespace(
                    close_ticks=110,
                    open_ticks=100,
                    start_ns=entry_ns,
                )
            ),
        ),
        starts=(entry_ns,),
        structurally_covers=lambda start, end: (start, end) == (entry_ns, exit_ns),
    )
    stream_calls: list[object] = []

    def path_parts(_root: Path, plan: object) -> object:
        stream_calls.append(plan)
        yield (path,)

    requested_feature_coordinates: list[object] = []

    def feature_bundles(_state: object, *, required_coordinates: object) -> object:
        requested_feature_coordinates.append(required_coordinates)
        return {300: feature_bundle}

    observed_responses: list[object] = []

    def partition_evidence(_state: object, _runtime: object, response: object) -> object:
        observed_responses.append(response)
        return {}

    monkeypatch.setattr(pipeline_module, "_one_second_path_parts", path_parts)
    monkeypatch.setattr(pipeline_module, "_direct_feature_bundles", feature_bundles)
    monkeypatch.setattr(pipeline_module, "_direct_partition_evidence", partition_evidence)
    state = SimpleNamespace(plan=SimpleNamespace(stage_key="HOLDOUT"))

    result = pipeline_module._evaluate_stage_candidate_masks(
        tmp_path,
        state,
        (runtime,),
    )

    assert requested_feature_coordinates == [{coordinate}]
    assert len(stream_calls) == 1
    assert len(observed_responses) == 1
    assert observed_responses[0].decision_timeframe_seconds == coordinate[0]
    assert observed_responses[0].horizon_seconds == coordinate[1]
    assert observed_responses[0].segment_ids.dtype == np.dtype(np.uint64)
    assert tuple(int(value) for value in observed_responses[0].segment_ids) == (maximum_segment_id,)
    assert tuple(observed_responses[0].terminal_ticks) == (110,)
    assert result[0].candidate == candidate


def test_empty_symbolic_partition_skips_the_one_second_loader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        pipeline_module,
        "_one_second_path_parts",
        lambda *_args, **_kwargs: pytest.fail("empty symbolic partition opened 1s"),
    )
    assert (
        pipeline_module._symbolic_partition_evidence(
            tmp_path,
            SimpleNamespace(),
            (),
        )
        == {}
    )


def test_search_prefix_rejects_nonlist_final_model_hashes_before_source_replay() -> None:
    from campaigns.ai_all_cases_v1 import ml

    candidate = ml.build_direct_candidate_catalog()[0]
    row = {
        "candidate": candidate.as_dict(),
        "control_alignment": None,
        "crossfit_summaries": [],
        "final_model_sha256_by_world": True,
        "frozen_model_artifact": None,
        "gate": None,
        "ineligibility": {
            "candidate_id": candidate.candidate_id,
            "message": "candidate-local ineligibility",
            "reason": "REAL_MASK_EMPTY",
            "schema": "systematic_fx.ai_all_cases_ml_ineligibility.v1",
            "scope_key": None,
        },
        "null_feasibility": None,
        "schema": "systematic_fx.ai_all_cases_direct_search_candidate.v1",
        "search_controls": None,
        "training_rows_sha256": None,
    }
    with pytest.raises(AllCasesPipelineError, match="ineligible ML row"):
        pipeline_module._validate_ml_candidate_row(
            ml,
            row,
            candidate,
            candidate_kind="DIRECT_ML",
            candidate_schema="systematic_fx.ai_all_cases_direct_search_candidate.v1",
        )


@pytest.mark.parametrize("candidate_kind", ("DIRECT_ML", "META_ML"))
def test_search_prefix_rejects_boolean_catalog_integers_before_source_replay(
    candidate_kind: str,
) -> None:
    from campaigns.ai_all_cases_v1 import ml

    meta = candidate_kind == "META_ML"
    candidate = (
        ml.build_meta_candidate_catalog()[0] if meta else ml.build_direct_candidate_catalog()[0]
    )
    raw_candidate = json.loads(_pipeline_json(candidate.as_dict()))
    if meta:
        raw_candidate["symbolic_rank_slot"] = True
    else:
        raw_candidate["action_rate"]["numerator"] = True
    row = {
        "candidate": raw_candidate,
        "control_alignment": None,
        "crossfit_summaries": [],
        "final_model_sha256_by_world": [],
        "frozen_model_artifact": None,
        "gate": None,
        "ineligibility": {
            "candidate_id": candidate.candidate_id,
            "message": "candidate-local ineligibility",
            "reason": "REAL_MASK_EMPTY",
            "schema": "systematic_fx.ai_all_cases_ml_ineligibility.v1",
            "scope_key": None,
        },
        "null_feasibility": None,
        "schema": (
            "systematic_fx.ai_all_cases_meta_search_candidate.v1"
            if meta
            else "systematic_fx.ai_all_cases_direct_search_candidate.v1"
        ),
        "search_controls": None,
        ("training_rows_sha256_by_world_and_fold" if meta else "training_rows_sha256"): (
            {} if meta else None
        ),
    }
    with pytest.raises(AllCasesPipelineError, match="candidate row schema"):
        pipeline_module._validate_ml_candidate_row(
            ml,
            row,
            candidate,
            candidate_kind=candidate_kind,
            candidate_schema=row["schema"],
        )


def test_search_prefix_rejects_empty_symbolic_meta_plan_for_selected_strategy() -> None:
    from campaigns.ai_all_cases_v1 import ml

    strategy_id = _ids(1)[0]
    payload = {
        "certificates_by_world_and_scope": {world: {} for world in ml.NULL_WORLD_ORDER},
        "schema": "systematic_fx.ai_all_cases_meta_rank_slot_plan.v1",
        "source_symbolic_top24_sha256": pipeline_module._sha256({}),
        "symbolic_frozen_artifacts": [],
        "symbolic_selection_rows": [],
    }
    ledger = SimpleNamespace(_artifact_payload=lambda _event: payload)
    event = SimpleNamespace(phase="META_PLAN_FROZEN")
    evidence = SimpleNamespace(
        gate_results=(),
        symbolic_selection=SimpleNamespace(selected_strategy_ids=(strategy_id,)),
        top24_by_world_and_scope={},
        top24_document={},
    )
    feature_plan = SimpleNamespace(
        family_by_policy_id={},
        policies_by_id={},
        recipes=(),
    )
    with pytest.raises(AllCasesPipelineError, match="meta plan worlds"):
        pipeline_module._preflight_ml_search_prefix(
            ledger,
            (event,),
            feature_plan=feature_plan,
            stage_b_evidence=evidence,
            state=SimpleNamespace(plan=SimpleNamespace(decision_dates=())),
        )


def test_search_prefix_rejects_boolean_final_candidate_id() -> None:
    payload = {
        "eligible_candidate_evidence_sha256": "0" * 64,
        "fit_cache_aggregate_sha256s": [],
        "model_artifact_sha256s": [],
        "schema": "systematic_fx.ai_all_cases_search_final_selection.v1",
        "selected_candidate_ids": [True],
        "strategy_artifact_sha256s": [],
    }
    ledger = SimpleNamespace(_artifact_payload=lambda _event: payload)
    event = SimpleNamespace(phase="FINAL_MAX12")
    evidence = SimpleNamespace(
        gate_results=(),
        symbolic_selection=SimpleNamespace(selected_strategy_ids=()),
        top24_by_world_and_scope={},
        top24_document={},
    )
    feature_plan = SimpleNamespace(
        family_by_policy_id={},
        policies_by_id={},
        recipes=(),
    )
    with pytest.raises(AllCasesPipelineError, match="selected IDs"):
        pipeline_module._preflight_ml_search_prefix(
            ledger,
            (event,),
            feature_plan=feature_plan,
            stage_b_evidence=evidence,
            state=SimpleNamespace(plan=SimpleNamespace(decision_dates=())),
        )


@pytest.mark.parametrize("failed_terminal", (False, True))
def test_public_prefix_paths_run_outcome_free_search_semantics_before_any_service(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failed_terminal: bool,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    if failed_terminal:
        with pytest.raises(AllCasesIntegrityError):
            _run_with_services(
                tmp_path,
                config,
                run_root,
                _services(
                    [],
                    search_selection_count=1,
                    walk_finalist_count=0,
                    search_error=ValueError("terminal prefix fixture"),
                ),
            )
    else:
        _run_with_services(
            tmp_path,
            config,
            run_root,
            _services([], search_selection_count=0, walk_finalist_count=0),
            stop_after="SEARCH_UNIVERSE_FROZEN",
        )
    store = _SearchSubledger(tmp_path, config, create=True)
    store.ensure_phase(
        _PHASES[0],
        1,
        lambda _index: {
            "schema": "systematic_fx.ai_all_cases_stage_a_raw_chunk.v1",
            "score_chunk": {"artifact_sha256": "a" * 64},
            "structural_lattice_sha256": "b" * 64,
        },
        verify_only=False,
    )
    semantic_calls: list[str] = []

    def reject_semantics(
        _root: Path,
        _config: AllCasesConfig,
        _universe: Mapping[str, object],
    ) -> None:
        semantic_calls.append("semantic-prefix")
        raise AllCasesIntegrityError("synthetic nested Search prefix differs")

    monkeypatch.setattr(run_module, "_verify_search_prefix_semantics", reject_semantics)
    calls: list[str] = []
    with pytest.raises(AllCasesIntegrityError, match="nested Search prefix"):
        if failed_terminal:
            _run_with_services(
                tmp_path,
                config,
                run_root,
                _services(calls, search_selection_count=1, walk_finalist_count=0),
            )
        else:
            _verify_with_services(
                tmp_path,
                config,
                run_root,
                _services(calls, search_selection_count=0, walk_finalist_count=0),
            )
    assert semantic_calls == ["semantic-prefix"]
    assert calls == []


def test_one_shot_restart_recovers_bounded_internal_temp_before_failed_closure(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    _run_with_services(
        tmp_path,
        config,
        run_root,
        _services([], search_selection_count=4, walk_finalist_count=2),
        stop_after="HOLDOUT_AUTHORIZED",
    )
    store = _SearchSubledger(tmp_path, config, create=True)
    temporary = store.staging / ".chunk-sigkill.tmp"
    temporary.write_bytes(b"partial")

    calls: list[str] = []
    result = _run_with_services(
        tmp_path,
        config,
        run_root,
        _services(calls, search_selection_count=4, walk_finalist_count=2),
    )

    assert result.status == "FAILED"
    assert calls == []
    assert not temporary.exists()
    assert _event_types(run_root)[-1] == "FAILED"


def test_resume_uses_read_only_source_prefix_replay_before_later_outcomes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    _run_with_services(
        tmp_path,
        config,
        run_root,
        _services([], search_selection_count=4, walk_finalist_count=2),
        stop_after="SEARCH_RESULTS_RELEASED",
    )
    events = _Ledger(run_root / "ledger", create=False).verify()
    artifacts = run_root / "artifacts"
    universe = run_module._event_payload(
        artifacts,
        events,
        "SEARCH_UNIVERSE_FROZEN",
        schema="systematic_fx.ai_all_cases_search_universe.v1",
        config=config,
    )
    search = run_module._event_payload(
        artifacts,
        events,
        "SEARCH_RESULTS_RELEASED",
        schema="systematic_fx.ai_all_cases_search_results.v1",
        config=config,
    )
    monkeypatch.setattr(
        run_module,
        "_internal_source_prefix_presence",
        lambda _run_root: (True, True),
    )
    calls: list[str] = []
    base = _services(calls, search_selection_count=4, walk_finalist_count=2)

    def replay_universe(_root: Path, _config: AllCasesConfig) -> object:
        calls.append("replay_search_universe_prefix")
        return {
            "complete": True,
            "next_chunk_index": None,
            "next_phase": None,
            "payload": universe,
        }

    def replay_search(
        _root: Path,
        _config: AllCasesConfig,
        _universe: Mapping[str, object],
    ) -> object:
        calls.append("replay_search_prefix")
        return {
            "complete": True,
            "next_chunk_index": None,
            "next_phase": None,
            "payload": search,
        }

    services = _AllCasesServices(
        base.freeze_search_universe,
        base.train_select_search,
        base.freeze_walk_forward_masks,
        base.evaluate_walk_forward,
        base.freeze_holdout_masks,
        base.evaluate_holdout,
        replay_search_universe_prefix=replay_universe,
        replay_search_prefix=replay_search,
    )
    result = _run_with_services(tmp_path, config, run_root, services)

    assert result.status == "COMPLETED"
    assert calls == [
        "replay_search_universe_prefix",
        "replay_search_prefix",
        "freeze_all_five_walk_forward_masks",
        "open_all_walk_forward_outcomes_atomically",
        "freeze_holdout_masks",
        "open_holdout_outcomes",
    ]


def test_preexisting_walk_result_is_source_replayed_before_holdout_authorization(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    _run_with_services(
        tmp_path,
        config,
        run_root,
        _services([], search_selection_count=4, walk_finalist_count=2),
        stop_after="WALK_FORWARD_RESULTS_RELEASED",
    )
    calls: list[str] = []
    result = _run_with_services(
        tmp_path,
        config,
        run_root,
        _services(calls, search_selection_count=4, walk_finalist_count=2),
    )

    assert result.status == "COMPLETED"
    assert calls == [
        "freeze_search_features_events_catalog",
        "open_search_outcomes_train_select",
        "freeze_all_five_walk_forward_masks",
        "open_all_walk_forward_outcomes_atomically",
        "freeze_holdout_masks",
        "open_holdout_outcomes",
    ]


def test_partial_search_source_prefix_is_replayed_before_continuation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    _run_with_services(
        tmp_path,
        config,
        run_root,
        _services([], search_selection_count=0, walk_finalist_count=0),
        stop_after="SEARCH_UNIVERSE_FROZEN",
    )
    events = _Ledger(run_root / "ledger", create=False).verify()
    universe = run_module._event_payload(
        run_root / "artifacts",
        events,
        "SEARCH_UNIVERSE_FROZEN",
        schema="systematic_fx.ai_all_cases_search_universe.v1",
        config=config,
    )
    monkeypatch.setattr(
        run_module,
        "_internal_source_prefix_presence",
        lambda _run_root: (True, True),
    )
    calls: list[str] = []
    base = _services(calls, search_selection_count=0, walk_finalist_count=0)
    services = _AllCasesServices(
        base.freeze_search_universe,
        base.train_select_search,
        base.freeze_walk_forward_masks,
        base.evaluate_walk_forward,
        base.freeze_holdout_masks,
        base.evaluate_holdout,
        replay_search_universe_prefix=lambda _root, _config: (
            calls.append("replay_search_universe_prefix")
            or {
                "complete": True,
                "next_chunk_index": None,
                "next_phase": None,
                "payload": universe,
            }
        ),
        replay_search_prefix=lambda _root, _config, _universe: (
            calls.append("replay_search_prefix")
            or {
                "complete": False,
                "next_chunk_index": 17,
                "next_phase": "STAGE_A_SCORE_CHUNKS",
                "payload": None,
            }
        ),
    )
    result = _run_with_services(tmp_path, config, run_root, services)

    assert result.status == "COMPLETED"
    assert calls == [
        "replay_search_universe_prefix",
        "replay_search_prefix",
        "open_search_outcomes_train_select",
    ]


def test_preexisting_holdout_result_fails_without_reopening_or_reporting(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    first = _run_with_services(
        tmp_path,
        config,
        run_root,
        _services([], search_selection_count=4, walk_finalist_count=2),
        stop_after="HOLDOUT_RESULTS_RELEASED",
    )
    assert first.status == "HOLDOUT_RESULTS_RELEASED"
    verify_calls: list[str] = []
    with pytest.raises(AllCasesIntegrityError, match="terminal-invalid"):
        _verify_with_services(
            tmp_path,
            config,
            run_root,
            _services(verify_calls, search_selection_count=4, walk_finalist_count=2),
        )
    assert verify_calls == []
    resumed_calls: list[str] = []
    result = _run_with_services(
        tmp_path,
        config,
        run_root,
        _services(resumed_calls, search_selection_count=4, walk_finalist_count=2),
    )
    assert result.status == "FAILED"
    assert resumed_calls == []


def test_post_authorization_result_artifact_orphan_is_deleted_then_failed_without_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    original_append = _Ledger.append

    def interrupt_after_result_link(
        self: _Ledger,
        event_type: str,
        request_sha256: str,
        payload: dict[str, object],
        *,
        enforce_resources: bool = True,
    ) -> None:
        if event_type == "HOLDOUT_RESULTS_RELEASED":
            raise KeyboardInterrupt("result link durable before ledger event")
        original_append(
            self,
            event_type,
            request_sha256,
            payload,
            enforce_resources=enforce_resources,
        )

    monkeypatch.setattr(_Ledger, "append", interrupt_after_result_link)
    with pytest.raises(KeyboardInterrupt, match="result link"):
        _run_with_services(
            tmp_path,
            config,
            run_root,
            _services([], search_selection_count=4, walk_finalist_count=2),
        )
    assert _event_types(run_root)[-1] == "HOLDOUT_MASKS_FROZEN"
    referenced = run_module._artifact_relative_paths(
        _Ledger(run_root / "ledger", create=False).verify()
    )
    assert len({path.name for path in (run_root / "artifacts").iterdir()} - referenced) == 1

    monkeypatch.setattr(_Ledger, "append", original_append)
    calls: list[str] = []
    result = _run_with_services(
        tmp_path,
        config,
        run_root,
        _services(calls, search_selection_count=4, walk_finalist_count=2),
    )
    assert result.status == "FAILED"
    assert calls == []
    events = _Ledger(run_root / "ledger", create=False).verify()
    assert _event_types(run_root)[-1] == "FAILED"
    assert {path.name for path in (run_root / "artifacts").iterdir()} == set(
        run_module._artifact_relative_paths(events)
    )


def test_post_result_report_orphan_is_deleted_then_failed_without_conclusion(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    _run_with_services(
        tmp_path,
        config,
        run_root,
        _services([], search_selection_count=4, walk_finalist_count=2),
        stop_after="HOLDOUT_RESULTS_RELEASED",
    )
    events = _Ledger(run_root / "ledger", create=False).verify()
    classification = events[-1].payload["classification"]
    assert isinstance(classification, str)
    orphan = run_module._publish_envelope(
        run_root / "artifacts",
        config,
        artifact_type="AI_ALL_CASES_REPORT",
        filename_prefix="all-cases-report",
        schema="systematic_fx.ai_all_cases_report_envelope.v1",
        payload=run_module._report_document(events, classification, config),
        referenced_relative_paths=run_module._artifact_relative_paths(events),
    )
    calls: list[str] = []
    result = _run_with_services(
        tmp_path,
        config,
        run_root,
        _services(calls, search_selection_count=4, walk_finalist_count=2),
    )

    assert result.status == "FAILED"
    assert calls == []
    assert not (run_root / "artifacts" / orphan.relative_path).exists()
    assert "COMPLETED" not in _event_types(run_root)


def test_fresh_verify_recomputes_exactly_without_mutating_run_tree(tmp_path: Path) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    _run_with_services(
        tmp_path,
        config,
        run_root,
        _services([], search_selection_count=4, walk_finalist_count=2),
    )
    before = {
        path.relative_to(run_root).as_posix(): (path.stat().st_mode, path.read_bytes())
        for path in run_root.rglob("*")
        if path.is_file()
    }
    calls: list[str] = []
    verified = _verify_with_services(
        tmp_path,
        config,
        run_root,
        _services(calls, search_selection_count=4, walk_finalist_count=2),
    )
    after = {
        path.relative_to(run_root).as_posix(): (path.stat().st_mode, path.read_bytes())
        for path in run_root.rglob("*")
        if path.is_file()
    }
    assert verified.status == "COMPLETED"
    assert before == after
    assert calls[-1] == "open_holdout_outcomes"


def test_public_completed_return_uses_verify_only_full_source_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    _run_with_services(
        tmp_path,
        config,
        run_root,
        _services([], search_selection_count=4, walk_finalist_count=2),
    )
    with run_module._exclusive_mutation(run_root):
        pass
    before = run_module._tree_snapshot(run_root)
    calls: list[str] = []
    monkeypatch.setattr(
        run_module,
        "_prepare_mutation",
        lambda _project_root: (tmp_path, config, run_root),
    )

    def services(*, verify_only: bool = False) -> _AllCasesServices:
        assert verify_only is True
        return _services(calls, search_selection_count=4, walk_finalist_count=2)

    monkeypatch.setattr(run_module, "_default_services", services)
    result = run_ai_all_cases(tmp_path)

    assert result.status == "COMPLETED"
    assert calls == [
        "freeze_search_features_events_catalog",
        "open_search_outcomes_train_select",
        "freeze_all_five_walk_forward_masks",
        "open_all_walk_forward_outcomes_atomically",
        "freeze_holdout_masks",
        "open_holdout_outcomes",
    ]
    assert run_module._tree_snapshot(run_root) == before


def test_fresh_verify_of_resumable_prefix_calls_no_unreleased_service(tmp_path: Path) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    _run_with_services(
        tmp_path,
        config,
        run_root,
        _services([], search_selection_count=4, walk_finalist_count=2),
        stop_after="SEARCH_UNIVERSE_FROZEN",
    )
    calls: list[str] = []
    result = _verify_with_services(
        tmp_path,
        config,
        run_root,
        _services(calls, search_selection_count=4, walk_finalist_count=2),
    )
    assert result.status == "SEARCH_UNIVERSE_FROZEN"
    assert calls == ["freeze_search_features_events_catalog"]


def test_ledger_rejects_writable_or_tampered_event(tmp_path: Path) -> None:
    run_root = _root(tmp_path)
    _run_with_services(
        tmp_path,
        _config(tmp_path),
        run_root,
        _services([], search_selection_count=0, walk_finalist_count=0),
        stop_after="SEARCH_UNIVERSE_FROZEN",
    )
    first = run_root / "ledger/events/event-00000001.json"
    first.chmod(0o644)
    with pytest.raises(AllCasesIntegrityError, match="unsafe event"):
        _Ledger(run_root / "ledger", create=False).verify()


def test_ledger_rejects_artifact_identity_json_type_coercion(tmp_path: Path) -> None:
    run_root = _root(tmp_path)
    _run_with_services(
        tmp_path,
        _config(tmp_path),
        run_root,
        _services([], search_selection_count=0, walk_finalist_count=0),
        stop_after="PRECOMMITTED",
    )
    first = run_root / "ledger/events/event-00000001.json"
    document = json.loads(first.read_bytes())
    document["payload"]["request_artifact"]["byte_size"] = str(
        document["payload"]["request_artifact"]["byte_size"]
    )
    first.chmod(0o644)
    first.write_bytes(run_module._canonical_json_bytes(document))
    first.chmod(0o444)
    with pytest.raises(AllCasesIntegrityError, match="value types"):
        _Ledger(run_root / "ledger", create=False).verify()


def test_outer_publication_adopts_one_link_before_ledger_sigkill_orphan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_root = _root(tmp_path)
    original_append = _Ledger.append
    interrupted = False

    def append_then_sigkill_window(
        self: _Ledger,
        event_type: str,
        request_sha256: str,
        payload: dict[str, object],
    ) -> object:
        nonlocal interrupted
        if event_type == "SEARCH_UNIVERSE_FROZEN" and not interrupted:
            interrupted = True
            raise OSError("simulated link-before-ledger interruption")
        return original_append(self, event_type, request_sha256, payload)

    monkeypatch.setattr(_Ledger, "append", append_then_sigkill_window)
    with pytest.raises(OSError, match="link-before-ledger"):
        _run_with_services(
            tmp_path,
            _config(tmp_path),
            run_root,
            _services([], search_selection_count=0, walk_finalist_count=0),
        )
    assert _event_types(run_root) == ("PRECOMMITTED",)
    assert len(tuple((run_root / "artifacts").iterdir())) == 2

    monkeypatch.setattr(_Ledger, "append", original_append)
    result = _run_with_services(
        tmp_path,
        _config(tmp_path),
        run_root,
        _services([], search_selection_count=0, walk_finalist_count=0),
    )
    events = _Ledger(run_root / "ledger", create=False).verify()
    referenced = {
        run_module._artifact_from_event(event).relative_path
        for event in events
        if event.event_type in run_module._ARTIFACT_ROLES
    }
    assert result.status == "COMPLETED"
    assert {path.name for path in (run_root / "artifacts").iterdir()} == referenced


def test_outer_staging_recovers_only_bounded_temp_and_completed_leaf_set_is_exact(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    services = _services([], search_selection_count=0, walk_finalist_count=0)
    _run_with_services(tmp_path, config, run_root, services, stop_after="PRECOMMITTED")
    temporary = run_root / "staging/artifacts/.search-universe-sigkill.tmp"
    temporary.write_bytes(b"partial")
    result = _run_with_services(tmp_path, config, run_root, services)
    assert result.status == "COMPLETED"
    assert not temporary.exists()

    raw = run_module._canonical_json_bytes({"orphan": True})
    digest = run_module.hashlib.sha256(raw).hexdigest()
    orphan = run_root / f"artifacts/orphan-{digest}.json"
    orphan.write_bytes(raw)
    orphan.chmod(0o444)
    with pytest.raises(AllCasesIntegrityError, match="leaf set"):
        _verify_with_services(tmp_path, config, run_root, services)


def test_outer_staging_rejects_unbounded_or_symlink_orphan(tmp_path: Path) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    services = _services([], search_selection_count=0, walk_finalist_count=0)
    _run_with_services(tmp_path, config, run_root, services, stop_after="PRECOMMITTED")
    unexpected = run_root / "staging/artifacts/unbounded.tmp"
    unexpected.write_bytes(b"partial")
    with pytest.raises(AllCasesIntegrityError, match="unsafe orphan"):
        _run_with_services(tmp_path, config, run_root, services)


@pytest.mark.parametrize("staging_name", ("staging/artifacts", "ledger/staging"))
def test_outer_publishers_reject_multiple_crash_temps_without_deleting(
    tmp_path: Path,
    staging_name: str,
) -> None:
    staging = tmp_path / staging_name
    published = tmp_path / ("artifacts" if staging_name.startswith("staging") else "ledger/events")
    staging.mkdir(parents=True)
    published.mkdir(parents=True)
    paths = (staging / ".event-a.tmp", staging / ".event-b.tmp")
    for path in paths:
        path.write_bytes(b"partial")

    with pytest.raises(AllCasesIntegrityError, match="multiple crash orphans"):
        run_module._recover_temporary_files(staging, published)

    assert all(path.exists() for path in paths)


def test_internal_publisher_rejects_multiple_crash_temps_without_deleting(
    tmp_path: Path,
) -> None:
    staging = tmp_path / "staging"
    published = tmp_path / "published"
    staging.mkdir()
    published.mkdir()
    paths = (staging / ".chunk-a.tmp", staging / ".chunk-b.tmp")
    for path in paths:
        path.write_bytes(b"partial")

    with pytest.raises(AllCasesPipelineError, match="multiple crash orphans"):
        pipeline_module._recover_staging(staging, (published,))

    assert all(path.exists() for path in paths)


def test_publishers_durably_fchmod_before_file_fsync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[tuple[str, int]] = []
    real_fchmod = run_module.os.fchmod
    real_fsync = run_module.os.fsync
    real_close = run_module.os.close

    def fchmod(descriptor: int, mode: int) -> None:
        events.append(("fchmod", descriptor))
        real_fchmod(descriptor, mode)

    def fsync(descriptor: int) -> None:
        events.append(("fsync", descriptor))
        real_fsync(descriptor)

    def close(descriptor: int) -> None:
        events.append(("close", descriptor))
        real_close(descriptor)

    monkeypatch.setattr(run_module.os, "fchmod", fchmod)
    monkeypatch.setattr(run_module.os, "fsync", fsync)
    monkeypatch.setattr(run_module.os, "close", close)
    run_root = _root(tmp_path)
    _run_with_services(
        tmp_path,
        _config(tmp_path),
        run_root,
        _services([], search_selection_count=0, walk_finalist_count=0),
        stop_after="PRECOMMITTED",
    )
    internal_root = tmp_path / "internal-published"
    internal_staging = tmp_path / "internal-staging"
    internal_root.mkdir()
    internal_staging.mkdir()
    _publish_mode_0444(
        internal_root,
        "payload.json",
        b"{}",
        staging=internal_staging,
    )

    chmod_indexes = [index for index, item in enumerate(events) if item[0] == "fchmod"]
    assert len(chmod_indexes) >= 3
    for chmod_index in chmod_indexes:
        descriptor = events[chmod_index][1]
        later = events[chmod_index + 1 :]
        first_close = next(
            index for index, item in enumerate(later) if item == ("close", descriptor)
        )
        assert ("fsync", descriptor) in later[:first_close]


def test_crash_temp_unlink_is_followed_by_staging_directory_fsync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outer_staging = tmp_path / "outer-staging"
    outer_published = tmp_path / "outer-published"
    outer_staging.mkdir()
    outer_published.mkdir()
    outer_temp = outer_staging / ".artifact-crash.tmp"
    outer_temp.write_bytes(b"partial")
    outer_calls: list[Path] = []
    monkeypatch.setattr(run_module, "_fsync_directory", outer_calls.append)
    run_module._recover_temporary_files(outer_staging, outer_published)
    assert outer_calls == [outer_staging]

    internal_staging = tmp_path / "internal-staging"
    internal_published = tmp_path / "internal-published"
    internal_staging.mkdir()
    internal_published.mkdir()
    internal_temp = internal_staging / ".chunk-crash.tmp"
    internal_temp.write_bytes(b"partial")
    internal_calls: list[Path] = []
    monkeypatch.setattr(pipeline_module, "_fsync_directory", internal_calls.append)
    pipeline_module._recover_staging(internal_staging, (internal_published,))
    assert internal_calls == [internal_staging]


def test_fresh_verify_rejects_an_extra_run_root_leaf_before_any_service_call(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    services = _services([], search_selection_count=0, walk_finalist_count=0)
    _run_with_services(tmp_path, config, run_root, services)
    (run_root / "unguarded.bin").write_bytes(b"extra")
    calls: list[str] = []
    with pytest.raises(AllCasesIntegrityError, match="ungoverned leaf"):
        _verify_with_services(
            tmp_path,
            config,
            run_root,
            _services(calls, search_selection_count=0, walk_finalist_count=0),
        )
    assert calls == []


def test_mutation_rejects_an_extra_run_root_leaf_before_any_service_call(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    (run_root / "ungoverned.bin").write_bytes(b"extra")
    calls: list[str] = []
    with pytest.raises(AllCasesIntegrityError, match="ungoverned leaf"):
        _run_with_services(
            tmp_path,
            _config(tmp_path),
            run_root,
            _services(calls, search_selection_count=4, walk_finalist_count=2),
        )
    assert calls == []


def test_mutation_rejects_invalid_linked_search_orphan_before_any_service_call(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    store = _SearchSubledger(tmp_path, config, create=True)
    published = store.artifacts / "evil.json"
    published.write_bytes(b"not-json")
    published.chmod(0o444)
    (store.staging / ".chunk-evil.tmp").hardlink_to(published)

    calls: list[str] = []
    with pytest.raises(AllCasesIntegrityError, match="internal evidence predates"):
        _run_with_services(
            tmp_path,
            config,
            run_root,
            _services(calls, search_selection_count=4, walk_finalist_count=2),
        )
    assert calls == []


def test_mutation_rejects_cross_store_linked_orphan_before_any_service_call(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    universe = run_root / "internal/universe"
    universe.mkdir(parents=True)
    (run_root / "internal/universe-staging").mkdir()
    search_staging = run_root / "internal/search/staging"
    search_staging.mkdir(parents=True)
    (run_root / "internal/search/artifacts").mkdir()
    (run_root / "internal/search/events").mkdir()
    published = universe / "evil.json"
    published.write_bytes(b"{}")
    published.chmod(0o444)
    (search_staging / ".chunk-cross-store.tmp").hardlink_to(published)

    calls: list[str] = []
    with pytest.raises(AllCasesIntegrityError, match="hard-link companion"):
        _run_with_services(
            tmp_path,
            _config(tmp_path),
            run_root,
            _services(calls, search_selection_count=4, walk_finalist_count=2),
        )
    assert calls == []


def test_mutation_recovers_universe_root_only_directory_creation_prefix(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    universe = run_root / "internal/universe"
    universe.mkdir(parents=True)
    calls: list[str] = []

    result = _run_with_services(
        tmp_path,
        _config(tmp_path),
        run_root,
        _services(calls, search_selection_count=0, walk_finalist_count=0),
        stop_after="PRECOMMITTED",
    )

    assert result.status == "PRECOMMITTED"
    assert calls == []
    assert (run_root / "internal/universe-staging").is_dir()


def test_released_production_universe_requires_internal_leaves_before_search_outcomes(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    calls: list[str] = []
    base = _services(calls, search_selection_count=4, walk_finalist_count=2)

    def freeze(root: Path, config: AllCasesConfig) -> object:
        value = base.freeze_search_universe(root, config)
        assert isinstance(value, dict)
        return {
            **value,
            "schema": "systematic_fx.ai_all_cases_search_universe_payload.v1",
        }

    services = _AllCasesServices(
        freeze,
        base.train_select_search,
        base.freeze_walk_forward_masks,
        base.evaluate_walk_forward,
        base.freeze_holdout_masks,
        base.evaluate_holdout,
    )
    with pytest.raises(AllCasesIntegrityError, match="feature-universe release"):
        _run_with_services(tmp_path, _production_identity_config(tmp_path), run_root, services)

    assert calls == ["freeze_search_features_events_catalog"]
    verify_calls: list[str] = []
    with pytest.raises(AllCasesIntegrityError, match="feature-universe release"):
        _verify_with_services(
            tmp_path,
            _production_identity_config(tmp_path),
            run_root,
            _services(verify_calls, search_selection_count=4, walk_finalist_count=2),
        )
    assert verify_calls == []


def test_released_production_search_requires_internal_closure_before_walk_forward(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        run_module,
        "_verify_internal_universe_release",
        lambda *_args, **_kwargs: None,
    )
    run_root = _root(tmp_path)
    calls: list[str] = []
    base = _services(calls, search_selection_count=4, walk_finalist_count=2)

    def search(
        root: Path,
        config: AllCasesConfig,
        universe: Mapping[str, object],
    ) -> object:
        value = base.train_select_search(root, config, universe)
        assert isinstance(value, dict)
        return {
            **value,
            "schema": "systematic_fx.ai_all_cases_search_result_payload.v1",
        }

    services = _AllCasesServices(
        base.freeze_search_universe,
        search,
        base.freeze_walk_forward_masks,
        base.evaluate_walk_forward,
        base.freeze_holdout_masks,
        base.evaluate_holdout,
    )
    with pytest.raises(AllCasesIntegrityError, match="internal Search release"):
        _run_with_services(tmp_path, _production_identity_config(tmp_path), run_root, services)

    assert calls == [
        "freeze_search_features_events_catalog",
        "open_search_outcomes_train_select",
    ]
    verify_calls: list[str] = []
    with pytest.raises(AllCasesIntegrityError, match="internal Search release"):
        _verify_with_services(
            tmp_path,
            _production_identity_config(tmp_path),
            run_root,
            _services(verify_calls, search_selection_count=4, walk_finalist_count=2),
        )
    assert verify_calls == []


def test_fresh_verify_requires_the_exact_empty_mode_0600_mutation_lock(
    tmp_path: Path,
) -> None:
    run_root = _root(tmp_path)
    config = _config(tmp_path)
    _run_with_services(
        tmp_path,
        config,
        run_root,
        _services([], search_selection_count=0, walk_finalist_count=0),
    )
    lock = run_root / ".mutation.lock"
    lock.write_bytes(b"not-empty")
    lock.chmod(0o600)
    calls: list[str] = []
    with pytest.raises(AllCasesIntegrityError, match="mutation lock"):
        _verify_with_services(
            tmp_path,
            config,
            run_root,
            _services(calls, search_selection_count=0, walk_finalist_count=0),
        )
    assert calls == []


def test_final_search_diversity_is_strict_exact_and_input_order_invariant() -> None:
    def row(
        rank: int,
        actions: list[list[str]],
        daily: dict[str, int],
    ) -> dict[str, object]:
        return {
            "candidate_id": f"{rank:064x}",
            "candidate_kind": "DIRECT_ML",
            "family_key": f"family-{rank}",
            "maximum_drawdown_ticks": rank,
            "median_outer_ev_denominator": 1,
            "median_outer_ev_numerator": 10 - rank,
            "oof_actions": actions,
            "positive_outer_validation_count": 6,
            "stress_net_ticks": 100 - rank,
            "worst_outer_ev_denominator": 1,
            "worst_outer_ev_numerator": 10 - rank,
            "daily_net_ticks": daily,
        }

    first = row(
        1,
        [[f"{index + 100:064x}", "LONG"] for index in range(5)],
        {"2026-01-01": 1, "2026-01-02": -1, "2026-01-03": 1, "2026-01-04": -1},
    )
    exact_jaccard = row(
        2,
        [[f"{index + 100:064x}", "LONG"] for index in range(4)],
        {"2026-01-01": 1, "2026-01-02": 1, "2026-01-03": -1, "2026-01-04": -1},
    )
    diverse = row(
        3,
        [[f"{index + 200:064x}", "SHORT"] for index in range(4)],
        {"2026-01-01": 1, "2026-01-02": 1, "2026-01-03": -1, "2026-01-04": -1},
    )
    exact_correlation = row(
        4,
        [[f"{index + 300:064x}", "LONG"] for index in range(4)],
        {"2026-01-01": 2, "2026-01-02": -2, "2026-01-03": 2, "2026-01-04": -2},
    )
    candidates = [first, exact_jaccard, diverse, exact_correlation]
    expected = (first["candidate_id"], diverse["candidate_id"])
    assert _select_diverse_search_candidates(candidates) == expected
    assert _select_diverse_search_candidates(list(reversed(candidates))) == expected


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("positive_outer_validation_count", True),
        ("stress_net_ticks", "9"),
        ("median_outer_ev_denominator", 1.0),
        ("daily_net_ticks", {"2026-01-01": True}),
        ("oof_actions", [["1" * 64, 1]]),
    ),
)
def test_final_search_selection_rejects_typed_row_coercion(
    field: str,
    value: object,
) -> None:
    row: dict[str, object] = {
        "candidate_id": "a" * 64,
        "candidate_kind": "DIRECT_ML",
        "daily_net_ticks": {"2026-01-01": 1, "2026-01-02": -1},
        "family_key": "family",
        "maximum_drawdown_ticks": 1,
        "median_outer_ev_denominator": 1,
        "median_outer_ev_numerator": 2,
        "oof_actions": [["b" * 64, "LONG"]],
        "positive_outer_validation_count": 6,
        "stress_net_ticks": 3,
        "worst_outer_ev_denominator": 1,
        "worst_outer_ev_numerator": 2,
    }
    row[field] = value
    with pytest.raises(AllCasesPipelineError, match="Search selection"):
        _select_diverse_search_candidates((row,))


def test_lineage_grouping_rejects_a_noncontiguous_reappearance() -> None:
    def wrapped(contract: str, span: int, segment: int) -> object:
        return SimpleNamespace(
            bar=SimpleNamespace(contract=contract, segment_id=segment),
            outcome_span_id=span,
        )

    with pytest.raises(AllCasesPipelineError, match="noncontiguous"):
        _lineage_groups(
            (
                wrapped("A", 1, 1),
                wrapped("B", 1, 1),
                wrapped("A", 1, 1),
            )
        )


def test_search_subledger_adopts_one_sigkill_orphan_and_closes_exact_leaf_set(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    store = _SearchSubledger(tmp_path, config, create=True)
    payload = {
        "schema": "systematic_fx.ai_all_cases_stage_a_raw_chunk.v1",
        "score_chunk": {"artifact_sha256": "a" * 64},
        "structural_lattice_sha256": "b" * 64,
    }
    envelope = {
        "artifact_schema": _SUBLEDGER_ARTIFACT_SCHEMA,
        "chunk_index": 0,
        "config_semantic_sha256": config.semantic_sha256,
        "payload": payload,
        "phase": _PHASES[0],
    }
    raw = _pipeline_json(envelope)
    digest = __import__("hashlib").sha256(raw).hexdigest()
    relative = f"{_PHASES[0].lower()}-000000-{digest}.json"
    _publish_mode_0444(store.artifacts, relative, raw, staging=store.staging)

    assert store.verify() == ()
    assert store.ensure_phase(_PHASES[0], 1, lambda _index: payload, verify_only=False) == (
        payload,
    )

    def phase_payload(phase: str) -> dict[str, object]:
        if phase == "STAGE_B_RAW_CHUNKS":
            return {
                "chunk": {},
                "coverage_by_world": {
                    "CIRCULAR": [],
                    "MATCHED": [],
                    "REAL": [],
                },
                "evaluation_chunks_by_world": {
                    "CIRCULAR": None,
                    "MATCHED": None,
                    "REAL": None,
                },
                "schema": "systematic_fx.ai_all_cases_stage_b_raw_chunk.v1",
            }
        if phase in {"DIRECT_ML_CHUNKS", "META_ML_CHUNKS"}:
            return {
                "candidate_count": 0,
                "candidates": [],
                "fit_cache_evidence": {},
                "first_catalog_rank": 1,
                "last_catalog_rank": 0,
                "schema": f"fixture.{phase.lower()}.v1",
            }
        return {"barrier": phase}

    for phase in _PHASES[1:]:
        store.ensure_phase(
            phase,
            1,
            lambda _index, phase=phase: phase_payload(phase),
            verify_only=False,
        )
    consumed: list[tuple[int, object]] = []
    assert store.ensure_phase(
        _PHASES[0],
        1,
        lambda _index: payload,
        verify_only=False,
        resume_consumer=lambda index, value: consumed.append((index, value)),
    ) == (payload,)
    assert consumed == []
    store.assert_complete({phase: 1 for phase in _PHASES})
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o444 for path in store.events.iterdir())
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o444 for path in store.artifacts.iterdir())


def test_search_subledger_recovers_only_bounded_staging_temp_and_rejects_extra_event(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    store = _SearchSubledger(tmp_path, config, create=True)
    orphan = store.staging / ".chunk-sigkill.tmp"
    orphan.write_bytes(b"partial")
    recovered = _SearchSubledger(tmp_path, config, create=True)
    assert not orphan.exists()

    extra = recovered.events / "unexpected.json"
    extra.write_bytes(b"{}")
    extra.chmod(0o444)
    with pytest.raises(AllCasesPipelineError, match="sequence|schema"):
        recovered.verify()


def test_search_subledger_rejects_json_type_coercion_in_an_immutable_event(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    store = _SearchSubledger(tmp_path, config, create=True)
    payload = {
        "schema": "systematic_fx.ai_all_cases_stage_a_raw_chunk.v1",
        "score_chunk": {"artifact_sha256": "a" * 64},
        "structural_lattice_sha256": "b" * 64,
    }
    store.ensure_phase(_PHASES[0], 1, lambda _index: payload, verify_only=False)
    event_path = store.events / "event-00000001.json"
    document = json.loads(event_path.read_bytes())
    document["sequence"] = True
    event_path.chmod(0o644)
    event_path.write_bytes(_pipeline_json(document))
    event_path.chmod(0o444)
    with pytest.raises(AllCasesPipelineError, match="value types"):
        store.verify()


def test_outer_and_internal_ledgers_reject_variable_width_fractional_timestamp(
    tmp_path: Path,
) -> None:
    outer = run_module.AllCasesLedgerEvent(
        1,
        None,
        "FAILED",
        "a" * 64,
        "2026-08-15T00:00:00.1Z",
        {"failure_code": "INTEGRITY_0123456789ABCDEF01234567"},
    )
    with pytest.raises(AllCasesIntegrityError, match="timestamp is not canonical"):
        run_module._validate_event_payload(outer)

    config = _config(tmp_path)
    store = _SearchSubledger(tmp_path, config, create=True)
    payload = {
        "schema": "systematic_fx.ai_all_cases_stage_a_raw_chunk.v1",
        "score_chunk": {"artifact_sha256": "a" * 64},
        "structural_lattice_sha256": "b" * 64,
    }
    store.ensure_phase(_PHASES[0], 1, lambda _index: payload, verify_only=False)
    event_path = store.events / "event-00000001.json"
    document = json.loads(event_path.read_bytes())
    document["recorded_at_utc"] = "2026-08-15T00:00:00.1Z"
    event_path.chmod(0o644)
    event_path.write_bytes(_pipeline_json(document))
    event_path.chmod(0o444)
    with pytest.raises(AllCasesPipelineError, match="timestamp is not canonical"):
        store.verify()


def test_search_subledger_rejects_coerced_or_extra_artifact_envelope_fields(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    store = _SearchSubledger(tmp_path, config, create=True)
    payload = {
        "schema": "systematic_fx.ai_all_cases_stage_a_raw_chunk.v1",
        "score_chunk": {"artifact_sha256": "a" * 64},
        "structural_lattice_sha256": "b" * 64,
    }
    store.ensure_phase(_PHASES[0], 1, lambda _index: payload, verify_only=False)
    event_path = store.events / "event-00000001.json"
    event = json.loads(event_path.read_bytes())
    artifact_path = store.artifacts / event["artifact_relative_path"]
    envelope = json.loads(artifact_path.read_bytes())
    envelope["chunk_index"] = False
    envelope["unguarded"] = True
    raw = _pipeline_json(envelope)
    digest = __import__("hashlib").sha256(raw).hexdigest()
    replacement_name = f"{_PHASES[0].lower()}-000000-{digest}.json"
    artifact_path.unlink()
    replacement = store.artifacts / replacement_name
    replacement.write_bytes(raw)
    replacement.chmod(0o444)
    event["artifact_sha256"] = digest
    event["artifact_relative_path"] = replacement_name
    event_path.chmod(0o644)
    event_path.write_bytes(_pipeline_json(event))
    event_path.chmod(0o444)
    with pytest.raises(AllCasesPipelineError, match="envelope"):
        store.verify()


def test_search_subledger_rejects_advancing_before_declared_phase_completion(
    tmp_path: Path,
) -> None:
    original = _config(tmp_path)
    document = original.as_dict()
    counts = {phase: 1 for phase in _PHASES}
    counts[_PHASES[0]] = 2
    document["search_design"]["search_phase_chunk_counts_canonical_json"] = json.dumps(
        counts, sort_keys=True, separators=(",", ":")
    )
    raw = json.dumps(document, sort_keys=True, separators=(",", ":")).encode("ascii")
    config = AllCasesConfig(
        path=original.path,
        file_sha256=original.file_sha256,
        semantic_sha256=original.semantic_sha256,
        code_commit=original.code_commit,
        implementation_sha256=original.implementation_sha256,
        dependency_lock_sha256=original.dependency_lock_sha256,
        precommitted_at_utc=original.precommitted_at_utc,
        canonical_bytes=raw,
    )
    store = _SearchSubledger(tmp_path, config, create=True)
    first = {
        "schema": "systematic_fx.ai_all_cases_stage_a_raw_chunk.v1",
        "score_chunk": {"artifact_sha256": "a" * 64},
        "structural_lattice_sha256": "b" * 64,
    }
    store._append(_PHASES[0], 0, first)
    with pytest.raises(AllCasesPipelineError, match="advance"):
        store._append(_PHASES[1], 0, {"barrier": _PHASES[1]})


def test_raw_search_chunk_schema_rejects_adaptive_or_extra_fields() -> None:
    valid = {
        "schema": "systematic_fx.ai_all_cases_stage_a_raw_chunk.v1",
        "score_chunk": {"artifact_sha256": "a" * 64},
        "structural_lattice_sha256": "b" * 64,
    }
    _validate_raw_chunk_payload(valid, phase="STAGE_A_SCORE_CHUNKS")
    with pytest.raises(AllCasesPipelineError, match="adaptive fields"):
        _validate_raw_chunk_payload({**valid, "partial_metric": 1}, phase="STAGE_A_SCORE_CHUNKS")
    with pytest.raises(AllCasesPipelineError, match="selection or inferential"):
        _validate_raw_chunk_payload(
            {
                **valid,
                "score_chunk": {
                    "artifact_sha256": "a" * 64,
                    "selected_candidate_ids": [],
                },
            },
            phase="STAGE_A_SCORE_CHUNKS",
        )


def test_production_search_adapter_runs_every_frozen_phase_in_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    universe = {
        "schema": "systematic_fx.ai_all_cases_search_universe_payload.v1",
    }
    state = object()

    class TestLedger:
        def verify(self) -> tuple[object, ...]:
            calls.append("verify_ledger")
            return ()

    ledger = TestLedger()
    selection = object()
    stage_b_plan = object()
    stage_b_evidence = object()
    direct = object()
    meta_plan = object()
    meta = object()
    calls: list[str] = []

    monkeypatch.setattr(
        pipeline_module,
        "_search_feature_state",
        lambda _root: calls.append("feature_state") or state,
    )

    def ledger_factory(
        _root: Path,
        _config: AllCasesConfig,
        *,
        create: bool,
        allow_incomplete_verify: bool = False,
    ) -> object:
        assert create
        assert allow_incomplete_verify is False
        calls.append("ledger")
        return ledger

    monkeypatch.setattr(pipeline_module, "_SearchSubledger", ledger_factory)
    monkeypatch.setattr(
        pipeline_module,
        "_ensure_stage_a_search",
        lambda *_args, **_kwargs: calls.append("stage_a") or ((), selection, ()),
    )
    monkeypatch.setattr(
        pipeline_module,
        "_ensure_stage_b_feature_plan",
        lambda *_args, **_kwargs: calls.append("stage_b_plan") or stage_b_plan,
    )
    monkeypatch.setattr(
        pipeline_module,
        "_ensure_stage_b_search_evidence",
        lambda *_args, **_kwargs: calls.append("stage_b_raw_top24") or stage_b_evidence,
    )
    monkeypatch.setattr(
        pipeline_module,
        "_symbolic_search_candidate_rows",
        lambda *_args, **_kwargs: calls.append("symbolic_artifacts") or ((), ()),
    )
    monkeypatch.setattr(
        pipeline_module,
        "_ensure_direct_ml_search",
        lambda *_args, **_kwargs: calls.append("direct") or direct,
    )
    monkeypatch.setattr(
        pipeline_module,
        "_ensure_meta_feature_plan",
        lambda *_args, **_kwargs: calls.append("meta_plan") or meta_plan,
    )
    monkeypatch.setattr(
        pipeline_module,
        "_ensure_meta_ml_search",
        lambda *_args, **_kwargs: calls.append("meta") or meta,
    )

    def finalize(*args: object, **kwargs: object) -> dict[str, object]:
        assert args[2:] == (
            ledger,
            selection,
            stage_b_plan,
            stage_b_evidence,
            (),
            (),
            direct,
            meta,
        )
        assert kwargs == {"verify_only": False}
        calls.append("final")
        return {"complete": True}

    monkeypatch.setattr(pipeline_module, "_finalize_search_result", finalize)
    assert pipeline_module._train_select_search(
        tmp_path,
        config,
        universe,
        verify_only=False,
    ) == {"complete": True}
    assert calls == [
        "ledger",
        "verify_ledger",
        "feature_state",
        "stage_a",
        "stage_b_plan",
        "stage_b_raw_top24",
        "symbolic_artifacts",
        "direct",
        "meta_plan",
        "meta",
        "final",
    ]


def test_production_walk_adapter_replays_all_five_masks_before_first_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate_id = _ids(1)[0]
    candidate = _FrozenSearchCandidate(candidate_id, "SYMBOLIC", "family", 1, {})
    plans = tuple(SimpleNamespace(stage_key=f"WF{index}") for index in range(1, 6))
    calls: list[str] = []

    monkeypatch.setattr(
        pipeline_module,
        "_plans",
        lambda _root: SimpleNamespace(walk_forward=plans),
    )
    monkeypatch.setattr(
        pipeline_module,
        "_frozen_search_candidates",
        lambda _search, _ids: (candidate,),
    )

    def feature_state(_root: Path, plan: object) -> object:
        calls.append(f"features:{plan.stage_key}")
        return SimpleNamespace(plan=plan)

    def freeze_stage(
        _root: Path,
        _config_value: AllCasesConfig,
        state: object,
        _candidates: object,
        _search: object,
    ) -> tuple[object, ...]:
        calls.append(f"freeze:{state.plan.stage_key}")
        return (
            SimpleNamespace(
                candidate=candidate,
                mask_sha256=_canonical_sha256(
                    {"candidate_id": candidate_id, "stage_key": state.plan.stage_key}
                ),
            ),
        )

    monkeypatch.setattr(pipeline_module, "_stage_feature_state", feature_state)
    monkeypatch.setattr(pipeline_module, "_freeze_stage_candidate_masks", freeze_stage)
    config = _config(tmp_path)
    masks = pipeline_module._freeze_walk_forward_masks(tmp_path, config, (candidate_id,), {})
    calls.clear()

    def evaluate_stage(_root: Path, state: object, _frozen: object) -> tuple[object, ...]:
        calls.append(f"outcome:{state.plan.stage_key}")
        return (SimpleNamespace(candidate=candidate, stage_key=state.plan.stage_key),)

    monkeypatch.setattr(pipeline_module, "_evaluate_stage_candidate_masks", evaluate_stage)
    monkeypatch.setattr(
        pipeline_module,
        "_walk_result_payload",
        lambda ids, values: {
            "candidate_ids": list(ids),
            "observed": [item.stage_key for item in values[candidate_id]],
        },
    )
    result = pipeline_module._evaluate_walk_forward(
        tmp_path,
        config,
        (candidate_id,),
        masks,
        {},
    )
    first_outcome = next(index for index, value in enumerate(calls) if value.startswith("outcome:"))
    assert calls[:first_outcome] == [
        value for fold in range(1, 6) for value in (f"features:WF{fold}", f"freeze:WF{fold}")
    ]
    assert result["observed"] == ["WF1", "WF2", "WF3", "WF4", "WF5"]


def test_search_finalizer_closes_all_fixed_catalogs_before_atomic_release(
    tmp_path: Path,
) -> None:
    from campaigns.ai_all_cases_v1 import ml

    phase_counts = {
        "STAGE_A_SCORE_CHUNKS": 64,
        "STAGE_A_TOP256": 1,
        "STAGE_B_PLAN_FROZEN": 1,
        "STAGE_B_RAW_CHUNKS": 64,
        "SYMBOLIC_TOP24": 1,
        "DIRECT_ML_CHUNKS": 24,
        "META_PLAN_FROZEN": 1,
        "META_ML_CHUNKS": 24,
        "FINAL_MAX12": 1,
    }
    config_document = {
        "config_id": "unit-test",
        "search_design": {
            "search_internal_phase_order": list(_PHASES),
            "search_phase_chunk_counts_canonical_json": json.dumps(
                phase_counts, sort_keys=True, separators=(",", ":")
            ),
        },
    }
    config = AllCasesConfig(
        path=tmp_path / "config.toml",
        file_sha256="a" * 64,
        semantic_sha256="b" * 64,
        code_commit="c" * 40,
        implementation_sha256="d" * 64,
        dependency_lock_sha256="e" * 64,
        precommitted_at_utc="2026-08-15T00:00:00Z",
        canonical_bytes=json.dumps(config_document, sort_keys=True, separators=(",", ":")).encode(
            "ascii"
        ),
    )
    leaves = tuple(
        {
            "artifact_sha256": f"{leaf_rank + 1:064x}",
            "chunk_index": chunk_index,
            "phase": phase,
        }
        for leaf_rank, (phase, chunk_index) in enumerate(
            (phase, chunk_index) for phase in _PHASES for chunk_index in range(phase_counts[phase])
        )
    )

    class Ledger:
        final_payload: dict[str, object] | None = None

        def ensure_phase(
            self,
            phase: str,
            count: int,
            builder: object,
            *,
            verify_only: bool,
            resume_consumer: object,
        ) -> tuple[dict[str, object], ...]:
            assert phase == "FINAL_MAX12" and count == 1 and not verify_only
            payload = dict(builder(0))  # type: ignore[operator]
            self.final_payload = payload
            return (payload,)

        def assert_complete(self, counts: object) -> None:
            assert counts == phase_counts

        def leaf_closure(self) -> tuple[dict[str, object], ...]:
            return leaves

        @property
        def head_sha256(self) -> str:
            return "f" * 64

    direct_catalog = ml.build_direct_candidate_catalog()
    meta_catalog = ml.build_meta_candidate_catalog()
    direct = _DirectSearchEvidence(
        tuple({"candidate": item.as_dict()} for item in direct_catalog),
        (),
        {},
        _empty_fit_cache_aggregate("DIRECT"),
        0,
    )
    meta = _MetaSearchEvidence(
        tuple({"candidate": item.as_dict()} for item in meta_catalog),
        (),
        {},
        {"schema": "fixture.meta.plan.v1"},
        _empty_fit_cache_aggregate("META"),
        0,
    )
    recipe_root = _canonical_sha256([])
    feature_plan = SimpleNamespace(
        recipes=(),
        plan_document={
            "complete_recipe_root_sha256": recipe_root,
            "selected_anchor_policy_ids": [],
        },
    )
    stage_a = SimpleNamespace(
        artifact_sha256="a" * 64,
        selected_policy_ids=(),
    )
    stage_b = SimpleNamespace(top24_document={"schema": "fixture.top24.v1"})
    universe = {
        "entry_exit_recipe_sha256": "b" * 64,
        "universe_root_sha256": "c" * 64,
    }
    ledger = Ledger()
    result = _finalize_search_result(
        config,
        universe,
        ledger,  # type: ignore[arg-type]
        stage_a,
        feature_plan,  # type: ignore[arg-type]
        stage_b,  # type: ignore[arg-type]
        (),
        (),
        direct,
        meta,
        verify_only=False,
    )

    assert result["complete_symbolic_candidate_count"] == 0
    assert result["direct_candidate_count"] == 288
    assert result["meta_candidate_count"] == 192
    assert len(result["evaluated_candidate_ids"]) == 480
    assert result["selected_candidate_ids"] == []
    assert ledger.final_payload == {
        "eligible_candidate_evidence_sha256": _canonical_sha256([]),
        "fit_cache_aggregate_sha256s": [
            direct.fit_cache_aggregate["artifact_sha256"],
            meta.fit_cache_aggregate["artifact_sha256"],
        ],
        "model_artifact_sha256s": [],
        "schema": "systematic_fx.ai_all_cases_search_final_selection.v1",
        "selected_candidate_ids": [],
        "strategy_artifact_sha256s": [],
    }
    _validate_search_result(result, universe, config)


def test_search_release_registry_reaches_actual_top24_and_ml_cross_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from campaigns.ai_all_cases_v1 import ml, symbolic

    scopes = (*ml.SEARCH_OUTER_FOLD_KEYS, "SEARCH_FINAL")
    events = tuple(SimpleNamespace(phase=phase, chunk_index=0) for phase in _PHASES)
    structural_sha = "1" * 64
    top24_document = {
        "complete_search_gate_results": [],
        "ineligibility_by_world": {"REAL": {}, "CIRCULAR": {}, "MATCHED": {}},
        "schema": "systematic_fx.ai_all_cases_symbolic_top24.v1",
        "symbolic_search_selection": {"selection": "empty"},
        "top24_by_world_and_scope": {
            world: {scope: {"scope_key": scope} for scope in scopes}
            for world in ("REAL", "CIRCULAR", "MATCHED")
        },
    }
    payloads = {
        "STAGE_A_SCORE_CHUNKS": {
            "schema": "systematic_fx.ai_all_cases_stage_a_raw_chunk.v1",
            "score_chunk": {"chunk": 0},
            "structural_lattice_sha256": structural_sha,
        },
        "STAGE_A_TOP256": {
            "schema": "systematic_fx.ai_all_cases_stage_a_selection.v1",
            "selection": {"selection": "empty"},
            "source_chunk_artifact_sha256s": ["2" * 64],
        },
        "STAGE_B_PLAN_FROZEN": {
            "complete_recipe_count": 0,
            "complete_recipe_root_sha256": pipeline_module._sha256([]),
            "control_opportunity_lattice_sha256": "3" * 64,
            "policy_feature_commitments": [],
            "schema": "systematic_fx.ai_all_cases_stage_b_feature_plan.v1",
            "selected_anchor_policy_ids": [],
            "stage_b_chunks": [],
            "structural_lattice_sha256": structural_sha,
        },
        "SYMBOLIC_TOP24": top24_document,
    }
    leaves = tuple(
        {
            "artifact_sha256": f"{index + 1:064x}",
            "chunk_index": 0,
            "phase": phase,
        }
        for index, phase in enumerate(_PHASES)
    )

    class Ledger:
        head_sha256 = "4" * 64

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def assert_complete(self, _counts: object) -> None:
            pass

        def verify(self) -> tuple[object, ...]:
            return events

        def leaf_closure(self) -> tuple[dict[str, object], ...]:
            return leaves

        def _artifact_payload(self, event: object) -> dict[str, object]:
            return payloads.get(event.phase, {"barrier": event.phase})

    empty_selection = SimpleNamespace(
        artifact_sha256="5" * 64,
        selected_policy_ids=(),
        selected_scores=(),
        as_dict=lambda: {"selection": "empty"},
    )
    monkeypatch.setattr(pipeline_module, "_SearchSubledger", Ledger)
    monkeypatch.setattr(
        pipeline_module,
        "_search_phase_counts",
        lambda _config_value: {phase: 1 for phase in _PHASES},
    )
    monkeypatch.setattr(symbolic, "stage_a_selection_from_dict", lambda _value: empty_selection)
    expected_score_chunk = SimpleNamespace(chunk_index=0)
    monkeypatch.setattr(symbolic, "build_stage_a_chunk_plan", lambda: (expected_score_chunk,))
    monkeypatch.setattr(
        symbolic,
        "stage_a_score_chunk_from_dict",
        lambda _value: SimpleNamespace(
            chunk=expected_score_chunk, artifact_sha256="2" * 64, scores=()
        ),
    )
    monkeypatch.setattr(
        pipeline_module,
        "_select_stage_a_from_score_chunks",
        lambda _config_value, _chunks: empty_selection,
    )
    empty_stage_b_chunk = {
        "chunk_id": pipeline_module._sha256(
            {
                "chunk_index": 0,
                "first_strategy_rank": 1,
                "last_strategy_rank": 0,
                "schema": "systematic_fx.ai_all_cases_complete_path_outcome.v1",
                "strategy_count": 0,
            }
        ),
        "chunk_index": 0,
        "first_strategy_rank": 1,
        "last_strategy_rank": 0,
        "schema": "systematic_fx.ai_all_cases_complete_path_outcome.v1",
        "strategy_count": 0,
    }
    monkeypatch.setattr(
        symbolic,
        "build_stage_b_chunk_plan",
        lambda _count: (SimpleNamespace(as_dict=lambda: empty_stage_b_chunk),),
    )
    payloads["STAGE_B_PLAN_FROZEN"]["stage_b_chunks"] = [empty_stage_b_chunk]
    monkeypatch.setattr(
        pipeline_module, "_consume_stage_b_raw_chunk", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(symbolic, "iter_complete_strategy_recipes", lambda _ids: ())
    monkeypatch.setattr(symbolic, "complete_search_gate_result_from_dict", lambda value: value)
    symbolic_selection = SimpleNamespace(
        eligible_count=0,
        selected_strategy_ids=(),
        classification="NO_SYMBOLIC_SEARCH_FINALISTS",
    )
    monkeypatch.setattr(
        symbolic,
        "complete_search_selection_from_dict",
        lambda _value: symbolic_selection,
    )
    monkeypatch.setattr(
        symbolic,
        "symbolic_top24_selection_from_dict",
        lambda value: SimpleNamespace(scope_key=value["scope_key"], selected_strategy_ids=()),
    )
    replayed_top24 = {
        world: {
            scope: SimpleNamespace(scope_key=scope, selected_strategy_ids=()) for scope in scopes
        }
        for world in ("REAL", "CIRCULAR", "MATCHED")
    }
    monkeypatch.setattr(
        pipeline_module,
        "_aggregate_stage_b_search_evidence",
        lambda *_args, **_kwargs: SimpleNamespace(
            gate_results=(),
            symbolic_selection=symbolic_selection,
            top24_by_world_and_scope=replayed_top24,
            top24_document=top24_document,
        ),
    )
    search_dates = tuple(date(2024, 1, 1) + timedelta(days=index) for index in range(469))
    monkeypatch.setattr(
        pipeline_module,
        "_plans",
        lambda _root: SimpleNamespace(search=SimpleNamespace(decision_dates=search_dates)),
    )
    observed: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        pipeline_module,
        "_search_ml_artifact_registry",
        lambda *args: observed.append(args) or {},
    )
    stage_b_plan = payloads["STAGE_B_PLAN_FROZEN"]
    search = {
        "search_chunk_artifacts": list(leaves),
        "search_chunk_leaf_closure_sha256": pipeline_module._sha256(list(leaves)),
        "search_subledger_head_sha256": Ledger.head_sha256,
        "stage_a_selected_policy_ids": [],
        "stage_a_selection_artifact_sha256": empty_selection.artifact_sha256,
        "stage_b_plan_sha256": pipeline_module._sha256(stage_b_plan),
        "symbolic_top24_artifact_sha256": pipeline_module._sha256(top24_document),
    }
    recipes, policies, families = pipeline_module._search_recipe_registry(
        tmp_path, _config(tmp_path), search
    )
    assert recipes == policies == families == {}
    assert len(observed) == 1
    assert observed[0][2] is search
    assert observed[0][7] is symbolic_selection
