from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest

from systematic_fx.db.bar_state_registry import (
    BAR_STATE_ELIGIBLE_CALENDAR_SHA256,
    BarStateRegistryError,
    BarStateReusedArtifactEvidence,
    BarStateReuseValidationReport,
    _validate_bar_state_run_spec,
    register_terminal_bar_state_result,
)
from systematic_fx.features.bars import TradeBar
from systematic_fx.research import bar_state_run
from systematic_fx.research.bar_state_artifacts import bar_state_price_policy_from_selection
from systematic_fx.research.bar_state_features import (
    FEATURE_NAMES_BY_SET,
    BarStateFeatureRow,
)
from systematic_fx.research.bar_state_labels import BarStateLabel, StatePathLabel
from systematic_fx.research.bar_state_model import (
    STATE_MODEL_CLASSES,
    BarStatePrediction,
    CanonicalBarStateModel,
    StateTradeDecision,
)
from systematic_fx.research.bar_state_run import (
    BAR_STATE_DATASET_MANIFEST_RELATIVE_PATH,
    BAR_STATE_DISCOVERY_ONE_SECOND_ROW_COUNT,
    BAR_STATE_DISCOVERY_OUTCOME_SPAN_COUNT,
    BarStateCandidateEngineArtifacts,
    BarStateEngineResult,
    BarStateParquetPayload,
    BarStateResearchRunReport,
    BarStateRunProgress,
    BarStateRunProvenance,
    _feature_identity,
    _fit_discovery_finalist_models,
    _fit_models_and_build_signals,
    _load_partition_bars,
    _trade_arrow_schema,
    _validate_duplicate_consensus,
    _validate_engine_result,
    _validate_final_fit_bindings,
    _VerifiedOutcomeSpanSource,
    build_bar_state_run_specs,
    execute_prepared_bar_state_run,
    load_prepared_bar_state_run,
)
from systematic_fx.research.hypotheses import canonical_sha256
from systematic_fx.research.provenance import CodeSnapshot
from tests.integration.test_bar_state_registry_postgres import (
    _GATE_REJECTED_CANDIDATE_REASONS,
    _gate_global_evidence,
)

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def prepared():
    return load_prepared_bar_state_run(ROOT)


def _provenance() -> BarStateRunProvenance:
    runtime = {"artifact_schema": "systematic_fx.runtime_environment.v1"}
    return BarStateRunProvenance(
        code_commit="a" * 40,
        snapshot=CodeSnapshot(
            code_commit="a" * 40,
            files=(),
            canonical_bytes=b"{}\n",
            sha256="b" * 64,
        ),
        dependency_lock_sha256="c" * 64,
        runtime_environment=runtime,
        runtime_environment_sha256=canonical_sha256(runtime),
        postgres_migrations_sha256="d" * 64,
    )


def test_terminal_registry_rejects_decision_status_mismatch_before_database() -> None:
    with pytest.raises(BarStateRegistryError, match="decision_label differs"):
        register_terminal_bar_state_result(
            "postgresql://not-opened",
            ROOT,
            research_run_attempt_id=1,
            candidate_key="bsv2_tf0300_fsmorphology_cm005",
            trial_status="SUCCEEDED",
            decision_label="DISCOVERY_REJECT",
            compact_summary={},
        )


def _timestamp_ns(value: date) -> int:
    return int(datetime(value.year, value.month, value.day, tzinfo=UTC).timestamp()) * (
        1_000_000_000
    )


def _feature_row(
    timeframe: int,
    feature_set_id: str,
    source_date: date,
) -> BarStateFeatureRow:
    decision_ns = _timestamp_ns(source_date + timedelta(days=1))
    return BarStateFeatureRow(
        feature_set_id=feature_set_id,
        feature_names=FEATURE_NAMES_BY_SET[feature_set_id],
        timeframe_seconds=timeframe,
        segment_id=1,
        contract="6EH2",
        source_date=source_date,
        signal_start_ns=decision_ns - timeframe * 1_000_000_000,
        decision_ns=decision_ns,
        atr_true_range_sum_ticks=480,
        volatility_ticks=24,
        values=(0.0,) * len(FEATURE_NAMES_BY_SET[feature_set_id]),
    )


def _label_for_row(
    row: BarStateFeatureRow,
    *,
    path_id: int,
    terminal_date: date,
) -> BarStateLabel:
    entry_date = row.source_date + timedelta(days=1)
    return BarStateLabel(
        label=StatePathLabel.UP_FIRST,
        timeframe_seconds=row.timeframe_seconds,
        segment_id=row.segment_id,
        contract=row.contract,
        signal_start_ns=row.signal_start_ns,
        decision_ns=row.decision_ns,
        entry_path_id=path_id,
        entry_path_index=0,
        entry_signal_bar_start_ns=row.decision_ns,
        entry_signal_bar_end_ns=(row.decision_ns + row.timeframe_seconds * 1_000_000_000),
        entry_start_ns=_timestamp_ns(entry_date),
        entry_price_ticks=1_000,
        volatility_ticks=24,
        upper_barrier_ticks=1_024,
        lower_barrier_ticks=976,
        upper_hit_path_index=1,
        lower_hit_path_index=None,
        terminal_path_index=10,
        terminal_start_ns=_timestamp_ns(terminal_date),
        horizon_start_date=entry_date,
        horizon_terminal_date=terminal_date,
        path_truncated_before_horizon=False,
        censor_reason=None,
    )


class _AlwaysLongModel:
    def __init__(self, model_id: str) -> None:
        self.model_id = model_id
        self.sha256 = canonical_sha256({"model_id": model_id})

    def as_dict(self) -> dict[str, str]:
        return {"model_id": self.model_id}

    def predict(
        self,
        values: tuple[float, ...],
        *,
        margin,
    ) -> BarStatePrediction:
        del values
        return BarStatePrediction(
            censored_probability=0.1,
            down_first_probability=0.1,
            up_first_probability=0.8,
            directional_score=0.7,
            margin=margin,
            decision=StateTradeDecision.LONG,
        )


def _portable_model(
    model_id: str,
    timeframe_seconds: int,
    feature_set_id: str,
) -> CanonicalBarStateModel:
    names = FEATURE_NAMES_BY_SET[feature_set_id]
    width = len(names)
    return CanonicalBarStateModel(
        model_id=model_id,
        timeframe_seconds=timeframe_seconds,
        feature_set_id=feature_set_id,
        feature_names=names,
        classes=STATE_MODEL_CLASSES,
        scaler_mean=(0.0,) * width,
        scaler_scale=(1.0,) * width,
        coefficients=tuple((0.0,) * width for _ in STATE_MODEL_CLASSES),
        intercepts=(0.0,) * len(STATE_MODEL_CLASSES),
        training_row_count=3,
        training_class_counts=tuple((item, 1) for item in STATE_MODEL_CLASSES),
        training_rows_sha256="a" * 64,
        sklearn_version="test",
        numpy_version="test",
        python_version="test",
        optimizer_iterations=(1,) * len(STATE_MODEL_CLASSES),
    )


def _synthetic_feature_labels(prepared):
    dates = (
        prepared.discovery_active_dates[0],
        *(item.oos_decisions.start_date for item in prepared.split_plan.inner_folds),
    )
    groups: dict[tuple[int, str], tuple[BarStateFeatureRow, ...]] = {}
    lookups: dict[tuple[int, str], dict[tuple[str, int, int], BarStateLabel]] = {}
    path_ids = (90, 101, 102, 103)
    terminal_dates = (
        prepared.discovery_active_dates[19],
        *(item.outcome_tail.end_date for item in prepared.split_plan.inner_folds),
    )
    for group in ((300, "MORPHOLOGY"), (300, "STATE"), (1_800, "MORPHOLOGY"), (1_800, "STATE")):
        rows = tuple(_feature_row(*group, source_date=item) for item in dates)
        labels = tuple(
            _label_for_row(row, path_id=path_id, terminal_date=terminal_date)
            for row, path_id, terminal_date in zip(rows, path_ids, terminal_dates, strict=True)
        )
        groups[group] = rows
        lookups[group] = {
            (row.contract, row.signal_start_ns, row.decision_ns): label
            for row, label in zip(rows, labels, strict=True)
        }
    return groups, lookups


def test_real_manifest_default_prepares_only_discovery_and_tail(prepared) -> None:
    assert (ROOT / BAR_STATE_DATASET_MANIFEST_RELATIVE_PATH).is_file()
    assert prepared.candidate_keys[0] == "bsv2_tf0300_fsmorphology_cm005"
    assert len(prepared.candidate_keys) == 12
    assert len(prepared.discovery_partitions) == 489
    assert prepared.discovery_decision_dates[-1].isoformat() == "2023-07-10"
    assert prepared.discovery_active_dates[-1].isoformat() == "2023-08-02"


def test_plan_only_has_no_database_or_engine_dependency(prepared) -> None:
    progress: list[BarStateRunProgress] = []
    report = execute_prepared_bar_state_run(
        prepared,
        mode="PLAN_ONLY",
        progress=progress.append,
    )

    assert isinstance(report, BarStateResearchRunReport)
    assert report.disposition == "PLANNED"
    assert [item.as_dict() for item in progress] == [
        {
            "completed": 12,
            "rss_bytes": progress[0].rss_bytes,
            "stage": "PLAN_READY",
            "total": 12,
        }
    ]
    assert set(progress[0].as_dict()) == {"completed", "rss_bytes", "stage", "total"}


def test_all_twelve_run_specs_bind_exact_candidate_policies(prepared) -> None:
    specs = build_bar_state_run_specs(prepared, _provenance())

    assert len(specs) == 12
    assert len({item.fingerprint for item in specs}) == 12
    assert {item.eligible_calendar_sha256 for item in specs} == {BAR_STATE_ELIGIBLE_CALENDAR_SHA256}
    for candidate_key, spec in zip(prepared.candidate_keys, specs, strict=True):
        assert spec.terminal_policy["boundary_event_ordering"] == (
            "UNRESOLVED_AT_BOUNDARY_CENSORED_PRIOR_FIRST_TOUCH_PRESERVED"
        )
        _validate_bar_state_run_spec(
            spec,
            definition=prepared.registry_definition,
            split_plan=prepared.outer_split_plan,
            candidate_key=candidate_key,
        )

    with pytest.raises(BarStateRegistryError, match="random seed"):
        _validate_bar_state_run_spec(
            replace(specs[0], random_seed=1),
            definition=prepared.registry_definition,
            split_plan=prepared.outer_split_plan,
            candidate_key=prepared.candidate_keys[0],
        )


def test_registry_definition_rejects_self_consistent_but_unapproved_identity(
    prepared,
) -> None:
    with pytest.raises(BarStateRegistryError, match="approved v2 preregistration"):
        replace(prepared.registry_definition, config_file_sha256="f" * 64)


def test_real_manifest_memory_preflight_and_discovery_boundary(prepared) -> None:
    source = _VerifiedOutcomeSpanSource(
        prepared,
        progress=None,
        stage="TEST",
    )

    assert len(source.row_counts) == BAR_STATE_DISCOVERY_OUTCOME_SPAN_COUNT
    assert sum(source.row_counts.values()) == BAR_STATE_DISCOVERY_ONE_SECOND_ROW_COUNT
    assert prepared.discovery_partitions == prepared.dataset.partitions[:489]
    assert prepared.dataset.partitions[489].source_date > (
        prepared.discovery_partitions[-1].source_date
    )
    assert all(
        partition.source_date <= prepared.discovery_active_dates[-1]
        for partitions in source._partitions.values()
        for partition in partitions
    )
    with pytest.raises(bar_state_run.BarStateRunError, match="sealed WF/HOLDOUT"):
        _load_partition_bars(prepared, prepared.dataset.partitions[489], 300)


def test_one_second_source_rejects_two_resident_spans(monkeypatch) -> None:
    source = object.__new__(_VerifiedOutcomeSpanSource)
    source._prepared = object()
    source._partitions = {
        7: (SimpleNamespace(ordinal=1, source_date=date(2022, 1, 3)),),
        8: (SimpleNamespace(ordinal=2, source_date=date(2022, 1, 4)),),
    }
    source.row_counts = {7: 1, 8: 1}
    source._progress = None
    source._stage = "TEST"
    source._opened = 0
    source._active_path_id = None

    def fake_load(
        prepared: Any,
        partition: Any,
        timeframe_seconds: int,
    ) -> tuple[TradeBar, ...]:
        del prepared
        start_ns = _timestamp_ns(partition.source_date)
        return (
            TradeBar(
                timeframe_seconds=timeframe_seconds,
                segment_id=partition.ordinal,
                contract="6EH2",
                source_date=partition.source_date,
                start_ns=start_ns,
                end_ns=start_ns + 1_000_000_000,
                first_trade_ns=start_ns,
                last_trade_ns=start_ns,
                open_ticks=1_000,
                high_ticks=1_001,
                low_ticks=999,
                close_ticks=1_000,
                trade_count=1,
                volume=1,
                observed_subbars=1,
            ),
        )

    monkeypatch.setattr(bar_state_run, "_load_partition_bars", fake_load)
    with source.open_path(7) as first:
        assert first.path_id == 7
        with (
            pytest.raises(bar_state_run.BarStateRunError, match="two outcome spans"),
            source.open_path(8),
        ):
            pass
    assert source._active_path_id is None
    with source.open_path(8) as second:
        assert second.path_id == 8


def test_fold_models_build_chronological_oos_signals_with_separate_dates(
    prepared,
    monkeypatch,
) -> None:
    features, label_lookups = _synthetic_feature_labels(prepared)
    fit_calls: list[tuple[str, tuple[date, ...]]] = []

    def fake_fit(rows, labels, *, model_id):
        assert len(rows) == len(labels)
        fit_calls.append((model_id, tuple(item.source_date for item in rows)))
        return _AlwaysLongModel(model_id)

    monkeypatch.setattr(bar_state_run, "fit_bar_state_model", fake_fit)
    fold_terminals = {(100 + fold, fold): 10 for fold in (1, 2, 3)}
    _, documents, signals, _ = _fit_models_and_build_signals(
        prepared,
        features,
        label_lookups,
        fold_terminals,
        progress=None,
    )

    assert len(fit_calls) == 12
    assert all(len(items) == 3 for items in documents.values())
    assert len(signals) == 36
    assert {item.candidate_key for item in signals} == set(prepared.candidate_keys)
    assert all(item.signal_active_date < item.entry_active_date for item in signals)
    assert all(item.entry_utc_month is not None for item in signals)
    morphology_fold_2 = next(
        dates
        for model_id, dates in fit_calls
        if model_id == "bsv2_tf0300_fsmorphology_discovery_inner_2"
    )
    assert prepared.split_plan.inner_folds[0].oos_decisions.start_date in (morphology_fold_2)
    assert prepared.split_plan.inner_folds[1].oos_decisions.start_date not in (morphology_fold_2)


def test_final_fit_is_group_deduplicated_and_uses_469_with_489_maturity(
    prepared,
    monkeypatch,
) -> None:
    features, label_lookups = _synthetic_feature_labels(prepared)
    group = (300, "MORPHOLOGY")
    final_row = _feature_row(*group, prepared.split_plan.discovery_final_fit.end_date)
    tail_row = _feature_row(*group, prepared.split_plan.discovery_final_label_tail.start_date)
    features[group] = (*features[group], final_row, tail_row)
    label_lookups[group][_feature_identity(final_row)] = _label_for_row(
        final_row,
        path_id=200,
        terminal_date=prepared.split_plan.discovery_final_label_tail.end_date,
    )
    label_lookups[group][_feature_identity(tail_row)] = _label_for_row(
        tail_row,
        path_id=201,
        terminal_date=prepared.split_plan.discovery_final_label_tail.end_date,
    )
    fit_calls: list[tuple[date, ...]] = []

    def fake_fit(rows, labels, *, model_id):
        assert len(rows) == len(labels)
        assert model_id == "bsv2_tf0300_fsmorphology_discovery_final_fit"
        fit_calls.append(tuple(item.source_date for item in rows))
        return _AlwaysLongModel(model_id)

    monkeypatch.setattr(bar_state_run, "fit_bar_state_model", fake_fit)
    models, bindings = _fit_discovery_finalist_models(
        prepared,
        features,
        label_lookups,
        prepared.candidate_keys[:2],
        progress=None,
    )

    assert len(fit_calls) == 1
    assert fit_calls[0][-1] == prepared.split_plan.discovery_final_fit.end_date
    assert prepared.split_plan.discovery_final_label_tail.start_date not in fit_calls[0]
    assert tuple(models) == (group,)
    assert len(bindings) == 2
    assert len({item["model_sha256"] for item in bindings}) == 1
    empty_models, empty_bindings = _fit_discovery_finalist_models(
        prepared,
        features,
        label_lookups,
        (),
        progress=None,
    )
    assert empty_models == {}
    assert empty_bindings == ()


def test_oos_trade_schema_freezes_decision_entry_and_exit_dates() -> None:
    names = set(_trade_arrow_schema("bsv2_tf0300_fsmorphology_cm005").names)
    assert {
        "signal_active_date",
        "entry_active_date",
        "exit_active_date",
        "entry_utc_month",
        "exit_utc_month",
    } <= names
    assert "active_date" not in names
    assert "utc_month" not in names


def test_engine_final_fit_binding_is_exact_and_nonfinalists_cannot_claim_it(
    prepared,
) -> None:
    finalists = prepared.candidate_keys[:2]
    final_model = _portable_model(
        "bsv2_tf0300_fsmorphology_discovery_final_fit",
        300,
        "MORPHOLOGY",
    )
    model = final_model.as_dict()
    model_sha256 = final_model.sha256
    final_document = {
        "fit_key": "discovery_final_fit",
        "label_maturity_end_active_ordinal": 489,
        "model": model,
        "model_sha256": model_sha256,
        "schema": "systematic_fx.bar_state_final_fit_model.v1",
        "training_decision_end_active_ordinal": 469,
    }
    candidates = []
    for candidate_key in prepared.candidate_keys:
        selected = candidate_key in finalists
        spec = next(
            item for item in prepared.config.candidates if item.candidate_key == candidate_key
        )
        inner_documents = tuple(
            {
                "fold_key": f"discovery_inner_{fold}",
                "model": inner_model.as_dict(),
                "model_sha256": inner_model.sha256,
                "schema": "systematic_fx.bar_state_fold_model.v1",
            }
            for fold in (1, 2, 3)
            for inner_model in (
                _portable_model(
                    (
                        f"bsv2_tf{spec.timeframe_seconds:04d}_"
                        f"fs{spec.feature_set.feature_set_id.lower()}_"
                        f"discovery_inner_{fold}"
                    ),
                    spec.timeframe_seconds,
                    spec.feature_set.feature_set_id,
                ),
            )
        )
        binding = (
            {
                "candidate_key": candidate_key,
                "feature_set_id": "MORPHOLOGY",
                "model_sha256": model_sha256,
                "timeframe_seconds": 300,
            }
            if selected
            else None
        )
        rejection_reasons = [] if selected else list(_GATE_REJECTED_CANDIDATE_REASONS)
        selection = {
            "bootstrap_lower_bound_ev_ticks": (
                {"denominator": 1, "numerator": 40} if selected else None
            ),
            "candidate_key": candidate_key,
            "final_label": "FINALIST" if selected else "REJECTED",
            "maximum_drawdown_ticks": 0 if selected else None,
            "moderate_ev_ticks": "40" if selected else None,
            "positive_component_size": 9 if selected else 0,
            "positive_inner_fold_count": 3 if selected else 0,
            "rejection_reasons": rejection_reasons,
            "selected_stop_loss_index": 4 if selected else None,
            "selected_stop_loss_multiplier": (
                {"denominator": 1, "numerator": 2} if selected else None
            ),
            "selected_take_profit_index": 3 if selected else None,
            "selected_take_profit_multiplier": (
                {"denominator": 2, "numerator": 3} if selected else None
            ),
            "worst_fold_moderate_ev_ticks": "40" if selected else None,
        }
        price_policy = bar_state_price_policy_from_selection(selection)
        candidates.append(
            BarStateCandidateEngineArtifacts(
                candidate_key=candidate_key,
                model_documents=(
                    (*inner_documents, final_document) if selected else inner_documents
                ),
                oos_trade_tables=(),
                terminal_document={
                    "candidate_selection": selection,
                    "candidate_support": {},
                    "discovery_final_fit_model": binding,
                    "multiplicity_cells": [],
                    "price_policy": price_policy,
                    "schema": "systematic_fx.bar_state_candidate_result.v1",
                },
                decision_label=("DISCOVERY_FINALIST" if selected else "DISCOVERY_REJECT"),
                trial_status="SUCCEEDED" if selected else "REJECTED",
                compact_summary={
                    "candidate_key": candidate_key,
                    "discovery_final_fit_model_sha256": (model_sha256 if selected else None),
                    "final_label": "FINALIST" if selected else "REJECTED",
                    "positive_component_size": 9 if selected else 0,
                    "price_policy": price_policy,
                    "rejection_reasons": rejection_reasons,
                    "selected_stop_loss_index": 4 if selected else None,
                    "selected_take_profit_index": 3 if selected else None,
                },
            )
        )
    ranked_finalists = finalists
    binding_by_key = {
        candidates[index].candidate_key: candidates[index].terminal_document[
            "discovery_final_fit_model"
        ]
        for index in range(2)
    }
    bindings = [binding_by_key[key] for key in ranked_finalists]
    selection_documents = [item.terminal_document["candidate_selection"] for item in candidates]
    global_document = _gate_global_evidence(
        prepared.candidate_keys,
        selection_documents,
        qc_variant=False,
    )
    global_document.update(
        {
            "discovery_final_fit_models": [
                {
                    "feature_set_id": "MORPHOLOGY",
                    **final_document,
                    "timeframe_seconds": 300,
                }
            ],
            "discovery_finalist_model_bindings": bindings,
            "finalist_keys": list(ranked_finalists),
        }
    )
    support_by_key = {item["candidate_key"]: item for item in global_document["candidate_support"]}
    multiplicity_by_key = {
        candidate_key: [
            item
            for item in global_document["multiplicity_results"]
            if item["candidate_key"] == candidate_key
        ]
        for candidate_key in prepared.candidate_keys
    }
    for candidate in candidates:
        candidate.terminal_document["candidate_support"] = support_by_key[candidate.candidate_key]
        candidate.terminal_document["multiplicity_cells"] = multiplicity_by_key[
            candidate.candidate_key
        ]

    _validate_final_fit_bindings(prepared, tuple(candidates), global_document)

    with pytest.raises(bar_state_run.BarStateRunError, match="governance evidence"):
        _validate_final_fit_bindings(
            prepared,
            tuple(candidates),
            {**global_document, "discovery_final_fit_models": []},
        )
    with pytest.raises(bar_state_run.BarStateRunError, match="governance evidence"):
        _validate_final_fit_bindings(
            prepared,
            tuple(candidates),
            {
                **global_document,
                "discovery_final_fit_models": [
                    *global_document["discovery_final_fit_models"],
                    *global_document["discovery_final_fit_models"],
                ],
            },
        )

    def payload(suffix: str) -> BarStateParquetPayload:
        return BarStateParquetPayload(
            artifact_key_suffix=suffix,
            split_key="discovery",
            shard_ordinal=0,
            logical_identity={"suffix": suffix},
            table=pa.table({"value": [1]}),
        )

    engine_result = BarStateEngineResult(
        feature_tables=(payload("feature"),),
        label_tables=(payload("label"),),
        candidate_results=tuple(
            replace(item, oos_trade_tables=(payload(f"trade_{item.candidate_key}"),))
            for item in candidates
        ),
        global_document=global_document,
    )
    assert _validate_engine_result(prepared, engine_result) == ranked_finalists
    forged_model_document = {
        **final_document,
        "model": {"model_id": "self_hashed_but_not_canonical"},
        "model_sha256": canonical_sha256({"model_id": "self_hashed_but_not_canonical"}),
    }
    forged_model = replace(
        candidates[0],
        model_documents=(*candidates[0].model_documents[:3], forged_model_document),
    )
    with pytest.raises(bar_state_run.BarStateRunError, match="strict decoding"):
        _validate_final_fit_bindings(
            prepared,
            (forged_model, *candidates[1:]),
            global_document,
        )
    forged = replace(
        candidates[2],
        compact_summary={
            **candidates[2].compact_summary,
            "discovery_final_fit_model_sha256": model_sha256,
        },
    )
    with pytest.raises(bar_state_run.BarStateRunError, match="compact summary"):
        _validate_final_fit_bindings(
            prepared,
            (*candidates[:2], forged, *candidates[3:]),
            global_document,
        )


def test_duplicate_consensus_rehydrates_global_and_rejects_mixed_drift(
    prepared,
    monkeypatch,
) -> None:
    lineage = {"authorized_stage": "DISCOVERY_ONLY"}
    artifact = SimpleNamespace(
        descriptor=SimpleNamespace(
            identity_sha256="1" * 64,
            logical_identity={"lineage": lineage},
        ),
        sha256="2" * 64,
    )
    evidence = BarStateReusedArtifactEvidence(
        artifact_role="FEATURE",
        split_key="discovery",
        shard_ordinal=0,
        lineage_sha256=canonical_sha256(lineage),
        artifact=artifact,
    )
    global_document = {
        "discovery_result": {"finalist_keys": [prepared.candidate_keys[0]]},
        "schema": "systematic_fx.bar_state_global_result_artifact.v1",
    }

    def report(index: int) -> BarStateReuseValidationReport:
        selected = index == 0
        return BarStateReuseValidationReport(
            research_run_attempt_id=100 + index,
            reused_attempt_id=10 + index,
            candidate_key=prepared.candidate_keys[index],
            artifact_count=1,
            role_counts=(("FEATURE", 1),),
            artifacts=(evidence,),
            artifact_link_manifest_sha256="3" * 64,
            compact_summary={},
            candidate_evidence_slice_sha256=f"{index + 10:064x}",
            candidate_selection_sha256=f"{index + 20:064x}",
            candidate_selection_projection_sha256=f"{index + 40:064x}",
            decision_label=("DISCOVERY_FINALIST" if selected else "DISCOVERY_REJECT"),
            finalist_model_binding_sha256=("7" * 64 if selected else canonical_sha256(None)),
            global_evidence_projection_sha256="8" * 64,
            model_package_projection_sha256=f"{index + 60:064x}",
            trial_status="SUCCEEDED" if selected else "REJECTED",
            global_artifact_identity_sha256="6" * 64,
            global_artifact_sha256="4" * 64,
            global_document_sha256=canonical_sha256(global_document),
            finalist_keys=(prepared.candidate_keys[0],),
            terminal_artifact_sha256=f"{index + 5:064x}",
        )

    reports = {
        prepared.candidate_keys[index]: report(index)
        for index in range(len(prepared.candidate_keys))
    }
    global_sha256, finalists, terminals = _validate_duplicate_consensus(
        prepared,
        reports,
    )
    assert global_sha256 == "4" * 64
    assert finalists == (prepared.candidate_keys[0],)
    assert set(terminals) == set(reports)
    with pytest.raises(bar_state_run.BarStateRunError, match="all twelve"):
        _validate_duplicate_consensus(
            prepared,
            dict(tuple(reports.items())[:-1]),
        )
    forged_status = {
        **reports,
        prepared.candidate_keys[1]: replace(
            reports[prepared.candidate_keys[1]],
            decision_label="DISCOVERY_FINALIST",
        ),
    }
    with pytest.raises(bar_state_run.BarStateRunError, match="decision label"):
        _validate_duplicate_consensus(prepared, forged_status)
    forged_identity = {
        **reports,
        prepared.candidate_keys[1]: replace(
            reports[prepared.candidate_keys[1]],
            global_artifact_identity_sha256="7" * 64,
        ),
    }
    with pytest.raises(bar_state_run.BarStateRunError, match="global result"):
        _validate_duplicate_consensus(prepared, forged_identity)

    reused_identity = (
        "FEATURE",
        "discovery",
        0,
        "1" * 64,
        "2" * 64,
        canonical_sha256(lineage),
    )
    published = SimpleNamespace(
        global_artifact=SimpleNamespace(
            descriptor=SimpleNamespace(identity_sha256="6" * 64),
            sha256="4" * 64,
        )
    )
    monkeypatch.setattr(
        bar_state_run,
        "_published_candidate_evidence",
        lambda _published, _key: (reused_identity,),
    )
    _validate_duplicate_consensus(prepared, reports, published=published)
    monkeypatch.setattr(
        bar_state_run,
        "_published_candidate_evidence",
        lambda _published, _key: (),
    )
    with pytest.raises(bar_state_run.BarStateRunError, match="recomputed evidence"):
        _validate_duplicate_consensus(prepared, reports, published=published)


def _successor_bar(start: datetime, *, segment_id: int) -> TradeBar:
    start_ns = int(start.timestamp()) * 1_000_000_000
    return TradeBar(
        timeframe_seconds=300,
        segment_id=segment_id,
        contract="6EH2",
        source_date=start.date(),
        start_ns=start_ns,
        end_ns=start_ns + 300_000_000_000,
        first_trade_ns=start_ns + 123,
        last_trade_ns=start_ns + 456,
        open_ticks=20_000,
        high_ticks=20_001,
        low_ticks=19_999,
        close_ticks=20_000,
        trade_count=2,
        volume=2,
        observed_subbars=2,
    )


def test_entry_map_links_only_immediate_observed_bar_across_maintenance_gap() -> None:
    first = _successor_bar(datetime(2022, 1, 3, tzinfo=UTC), segment_id=7)
    immediate = _successor_bar(
        datetime(2022, 1, 3, 0, 10, tzinfo=UTC),
        segment_id=8,
    )
    later = _successor_bar(datetime(2022, 1, 3, 1, 0, tzinfo=UTC), segment_id=9)

    mapping = bar_state_run._entry_bar_maps(
        {300: (first, immediate, later)},
        outcome_span_by_date={date(2022, 1, 3): 9},
    )[300]
    linked = mapping[(first.contract, first.source_date, first.start_ns, first.end_ns)]

    assert linked.start_ns == immediate.start_ns
    assert linked.start_ns > first.end_ns
    assert linked.segment_id != first.segment_id
    assert linked.outcome_span_id == 9


def test_entry_map_never_links_across_manifest_outcome_spans() -> None:
    first = _successor_bar(
        datetime(2022, 1, 3, 23, 55, tzinfo=UTC),
        segment_id=7,
    )
    following = _successor_bar(
        datetime(2022, 1, 4, 0, 5, tzinfo=UTC),
        segment_id=8,
    )

    mapping = bar_state_run._entry_bar_maps(
        {300: (first, following)},
        outcome_span_by_date={date(2022, 1, 3): 9, date(2022, 1, 4): 10},
    )[300]

    assert mapping == {}


def test_entry_map_never_links_across_contracts() -> None:
    first = _successor_bar(datetime(2022, 1, 3, tzinfo=UTC), segment_id=7)
    following = replace(
        _successor_bar(
            datetime(2022, 1, 3, 0, 10, tzinfo=UTC),
            segment_id=8,
        ),
        contract="6EM2",
    )

    mapping = bar_state_run._entry_bar_maps(
        {300: (first, following)},
        outcome_span_by_date={date(2022, 1, 3): 9},
    )[300]

    assert mapping == {}
