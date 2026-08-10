from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Self
from unittest import mock

import pyarrow as pa
import pytest
from psycopg import IsolationLevel

from systematic_fx.db import bar_registry
from systematic_fx.db.bar_registry import (
    BAR_CATALOG_EXPERIMENT_KEY,
    BAR_DATASET_MANIFEST_KEY,
    BAR_REGISTRATION_SCHEMA,
    BAR_REGISTRY_SCHEMA_LIMITATIONS,
    BAR_TERMINAL_ARTIFACT_RECORD_COUNT,
    BAR_TERMINAL_ARTIFACT_SCHEMA,
    BAR_TERMINAL_ARTIFACT_SCHEMA_SHA256,
    BAR_TERMINAL_RESULT_SCHEMA,
    RAW_SOURCE_MANIFEST_KEY,
    BarRegistryDriftError,
    BarRegistryError,
    BarTerminalResult,
    abort_bar_run_attempt,
    build_bar_registration_document,
    candidate_trial_parameters,
    publish_bar_registration_artifact,
    publish_bar_terminal_result_artifact,
    register_bar_run_spec,
    register_published_bar_artifact,
    register_terminal_bar_result,
)
from systematic_fx.db.run_registry import RunSpecRegistration
from systematic_fx.research.bar_artifacts import (
    BarArtifactDescriptor,
    arrow_schema_sha256,
    open_verified_bar_artifact,
    publish_bar_artifact_bytes,
    publish_bar_json_artifact,
    publish_bar_parquet_table,
)
from systematic_fx.research.bar_config import BAR_SOURCE_MANIFEST_SHA256, load_bar_pattern_config
from systematic_fx.research.hypotheses import canonical_sha256
from systematic_fx.research.run_spec import RunSpec
from systematic_fx.validation.bar_splits import plan_bar_splits

ROOT = Path(__file__).resolve().parents[2]
SOURCE_MANIFEST_SHA256 = BAR_SOURCE_MANIFEST_SHA256
BAR_DATASET_MANIFEST_SHA256 = "d" * 64
RUN_FINGERPRINT = "b" * 64
DISCOVERY_SHA256 = "1" * 64
GLOBAL_IDENTITY_SHA256 = "2" * 64
EVIDENCE_MANIFEST_SHA256 = "3" * 64
EVIDENCE_ARTIFACT_IDENTITY_SHA256 = "4" * 64
EVIDENCE_IDENTITY_SHA256 = "5" * 64
SNAPSHOT_IDENTITY_SHA256 = "6" * 64
SNAPSHOT_SHA256 = "7" * 64
DATASET_HANDOFF_SHA256 = "8" * 64
MIGRATIONS_SHA256 = "9" * 64


class _Result:
    def __init__(self, value: dict[str, Any] | list[dict[str, Any]] | None) -> None:
        self.value = value

    def fetchone(self) -> dict[str, Any] | None:
        if isinstance(self.value, list):
            raise TypeError("fetchone called for a row collection")
        return self.value

    def fetchall(self) -> list[dict[str, Any]]:
        if not isinstance(self.value, list):
            raise TypeError("fetchall called for a scalar row")
        return self.value


class _Transaction:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    def __enter__(self) -> Self:
        self.connection.transaction_entries += 1
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Connection:
    def __init__(
        self,
        responses: Iterable[dict[str, Any] | list[dict[str, Any]] | None],
    ) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, object]] = []
        self.isolation_level: IsolationLevel | None = None
        self.transaction_entries = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def transaction(self) -> _Transaction:
        return _Transaction(self)

    def execute(self, sql: str, parameters: object = ()) -> _Result:
        self.calls.append((" ".join(sql.split()), parameters))
        if not self.responses:
            raise AssertionError(f"unexpected SQL: {sql}")
        return _Result(self.responses.pop(0))


def _split_plan():
    start = date(2022, 1, 2)
    return plan_bar_splits(tuple(start + timedelta(days=index) for index in range(1_500)))


def _artifact_row(artifact, *, artifact_id: int = 71) -> dict[str, object]:
    return {
        "artifact_id": artifact_id,
        "artifact_key": artifact.descriptor.artifact_key,
        "artifact_type": artifact.descriptor.artifact_type,
        "uri": artifact.uri,
        "sha256": artifact.sha256,
        "byte_size": artifact.byte_size,
        "media_type": artifact.descriptor.media_type,
        "producer_job_id": None,
        "metadata": artifact.database_metadata(),
    }


def _terminal_compact(candidate, *, trial_status: str = "SUCCEEDED") -> dict[str, object]:
    decision_label = {
        "SUCCEEDED": "DISCOVERY_FINALIST",
        "REJECTED": "SCREENING_REJECT",
    }[trial_status]
    final_label = {
        "SUCCEEDED": "DISCOVERY_FINALIST_SELECTED",
        "REJECTED": "SUPPORT_REJECT",
    }[trial_status]
    return {
        "candidate_definition_sha256": candidate.definition_sha256,
        "candidate_key": candidate.candidate_key,
        "decision_label": decision_label,
        "decision_trigger_count": 10,
        "discovery_result_sha256": DISCOVERY_SHA256,
        "distinct_signal_day_count": 5,
        "evidence_artifact_identity_sha256": EVIDENCE_ARTIFACT_IDENTITY_SHA256,
        "evidence_identity_sha256": EVIDENCE_IDENTITY_SHA256,
        "evidence_manifest_sha256": EVIDENCE_MANIFEST_SHA256,
        "final_label": final_label,
        "global_result_artifact_identity_sha256": GLOBAL_IDENTITY_SHA256,
        "global_result_artifact_sha256": DISCOVERY_SHA256,
        "matched_signal_count": 5,
        "moderate_ev_ticks": None,
        "positive_component_size": 0,
        "qualification_status": bar_registry.BAR_PATTERN_QUALIFICATION_STATUS,
        "raw_signal_count": 5,
        "rejection_reasons": ["INSUFFICIENT_RAW_SIGNALS"],
        "screening_only": True,
        "selected_buy_sell_loss_formula": None,
        "selected_stop_loss_ticks": None,
        "selected_take_profit_ticks": None,
    }


def _terminal_candidate_result(
    candidate,
    *,
    final_label: str,
    raw_source_manifest_sha256: str = SOURCE_MANIFEST_SHA256,
    bar_dataset_manifest_sha256: str = BAR_DATASET_MANIFEST_SHA256,
    split_plan_sha256: str | None = None,
) -> dict[str, object]:
    config = load_bar_pattern_config(ROOT)
    decision_label = {
        "SUPPORT_REJECT": "SUPPORT_REJECT",
        "ECONOMIC_REJECT": "ECONOMIC_REJECT",
        "DISCOVERY_FINALIST_SELECTED": "DISCOVERY_FINALIST",
        "DISCOVERY_FINALIST_BUDGET_REJECTED": "DISCOVERY_FINALIST",
    }[final_label]
    cells_by_scenario = [
        {
            "blocks": [{"block_key": f"b{index}"} for index in range(4)],
            "direction": candidate.direction.value,
            "stop_loss_ticks": stop_loss,
            "take_profit_ticks": take_profit,
        }
        for take_profit in bar_registry.BARRIER_TICKS
        for stop_loss in bar_registry.BARRIER_TICKS
    ]
    return {
        "candidate_definition": candidate.definition_payload(),
        "candidate_definition_sha256": candidate.definition_sha256,
        "candidate_key": candidate.candidate_key,
        "decision": {
            "candidate_key": candidate.candidate_key,
            "direction": candidate.direction.value,
            "label": decision_label,
            "overall_moderate_ev_ticks": None,
            "positive_component_size": 0,
            "rejection_reasons": ["INSUFFICIENT_RAW_SIGNALS"],
            "selected_buy_sell_loss_formula": None,
            "selected_stop_loss_ticks": None,
            "selected_take_profit_ticks": None,
        },
        "decision_trigger_count": 10,
        "discovery_lineage": {
            "candidate_catalog_sha256": config.candidate_catalog_sha256,
            "code_snapshot_artifact_identity_sha256": SNAPSHOT_IDENTITY_SHA256,
            "code_snapshot_sha256": SNAPSHOT_SHA256,
            "config_file_sha256": config.sha256,
            "config_semantic_sha256": config.semantic_sha256,
            "dataset_handoff_sha256": DATASET_HANDOFF_SHA256,
            "dataset_manifest_sha256": bar_dataset_manifest_sha256,
            "discovery_result_schema": bar_registry.DISCOVERY_RESULT_SCHEMA,
            "discovery_result_sha256": DISCOVERY_SHA256,
            "evidence_artifact_identity_sha256": EVIDENCE_ARTIFACT_IDENTITY_SHA256,
            "evidence_identity_sha256": EVIDENCE_IDENTITY_SHA256,
            "evidence_manifest_sha256": EVIDENCE_MANIFEST_SHA256,
            "evidence_matched_record_count": 5,
            "evidence_replay_record_count": 5,
            "evidence_shard_count": 2,
            "global_result_artifact_identity_sha256": GLOBAL_IDENTITY_SHA256,
            "global_result_artifact_sha256": DISCOVERY_SHA256,
            "outcome_span_policy_sha256": (bar_registry.BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256),
            "postgres_migrations_sha256": MIGRATIONS_SHA256,
            "raw_source_manifest_sha256": raw_source_manifest_sha256,
            "schema": bar_registry.BAR_DISCOVERY_LINEAGE_SCHEMA,
            "split_plan_sha256": split_plan_sha256 or _split_plan().sha256,
        },
        "economics": [
            {
                "cells": [
                    {**cell, "scenario_id": scenario.scenario_id} for cell in cells_by_scenario
                ],
                "scenario_id": scenario.scenario_id,
            }
            for scenario in config.execution_scenarios
        ],
        "final_label": final_label,
        "matched_signal_count": 5,
        "support": {
            "candidate_key": candidate.candidate_key,
            "direction": candidate.direction.value,
            "distinct_signal_day_count": 5,
            "raw_signal_count": 5,
            "timeframe_seconds": candidate.timeframe_seconds,
        },
    }


def _terminal_artifact(
    tmp_path: Path,
    candidate,
    *,
    trial_status: str = "SUCCEEDED",
    compact_result: dict[str, object] | None = None,
    run_fingerprint: str = RUN_FINGERPRINT,
    raw_source_manifest_sha256: str = SOURCE_MANIFEST_SHA256,
    bar_dataset_manifest_sha256: str = BAR_DATASET_MANIFEST_SHA256,
):
    config = load_bar_pattern_config(ROOT)
    compact = compact_result or _terminal_compact(candidate, trial_status=trial_status)
    decision_label = str(compact["decision_label"])
    final_label = str(compact["final_label"])
    return publish_bar_terminal_result_artifact(
        tmp_path,
        config,
        candidate_key=candidate.candidate_key,
        raw_source_manifest_sha256=raw_source_manifest_sha256,
        bar_dataset_manifest_sha256=bar_dataset_manifest_sha256,
        split_plan_sha256=_split_plan().sha256,
        run_fingerprint=run_fingerprint,
        trial_status=trial_status,
        decision_label=decision_label,
        compact_result=compact,
        candidate_result=_terminal_candidate_result(
            candidate,
            final_label=final_label,
            raw_source_manifest_sha256=raw_source_manifest_sha256,
            bar_dataset_manifest_sha256=bar_dataset_manifest_sha256,
            split_plan_sha256=_split_plan().sha256,
        ),
    )


def _valid_run_spec(config, split_plan, candidate) -> RunSpec:
    policies = bar_registry._expected_bar_policy_documents(config, candidate)
    trial_parameters = candidate_trial_parameters(
        config,
        split_plan,
        candidate,
        raw_source_manifest_sha256=SOURCE_MANIFEST_SHA256,
        bar_dataset_manifest_sha256=BAR_DATASET_MANIFEST_SHA256,
    )
    parameters = {
        "bar_cost_policy": policies["cost"],
        "bar_barrier_policy_sha256": canonical_sha256(policies["barrier"]),
        "bar_campaign_definition_sha256": config.definition_sha256,
        "bar_candidate_catalog_sha256": config.candidate_catalog_sha256,
        "bar_candidate_definition_sha256": candidate.definition_sha256,
        "bar_candidate_key": candidate.candidate_key,
        "bar_code_snapshot_artifact_identity_sha256": SNAPSHOT_IDENTITY_SHA256,
        "bar_config_file_sha256": config.sha256,
        "bar_config_semantic_sha256": config.semantic_sha256,
        "bar_cost_policy_sha256": canonical_sha256(policies["cost"]),
        "bar_dataset_handoff_sha256": DATASET_HANDOFF_SHA256,
        "bar_dataset_manifest_sha256": BAR_DATASET_MANIFEST_SHA256,
        "bar_entry_policy_sha256": canonical_sha256(policies["entry"]),
        "bar_evidence_policy": policies["evidence"],
        "bar_evidence_policy_sha256": canonical_sha256(policies["evidence"]),
        "bar_execution_policy": policies["execution"],
        "bar_outcome_policy": policies["outcome"],
        "bar_outcome_span_policy_sha256": (bar_registry.BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256),
        "bar_postgres_migrations_sha256": MIGRATIONS_SHA256,
        "bar_raw_source_manifest_sha256": SOURCE_MANIFEST_SHA256,
        "bar_screening_only": bar_registry.BAR_PATTERN_SCREENING_ONLY,
        "bar_selection_policy": policies["selection"],
        "bar_selection_policy_sha256": canonical_sha256(policies["selection"]),
        "bar_split_plan_sha256": split_plan.sha256,
        "bar_trial_parameters_sha256": canonical_sha256(trial_parameters),
        "qualification_status": bar_registry.BAR_PATTERN_QUALIFICATION_STATUS,
    }
    calendar = {
        "dataset_handoff_sha256": DATASET_HANDOFF_SHA256,
        "eligible_active_dates": [item.isoformat() for item in split_plan.eligible_dates],
        "schema": "systematic_fx.bar_eligible_calendar.v1",
    }
    return RunSpec(
        campaign_id=bar_registry.BAR_PATTERN_CAMPAIGN_KEY,
        experiment_id=BAR_CATALOG_EXPERIMENT_KEY,
        run_kind="SCREEN",
        engine_version=bar_registry.BAR_DISCOVERY_ENGINE_VERSION,
        source_manifest_hashes={
            RAW_SOURCE_MANIFEST_KEY: SOURCE_MANIFEST_SHA256,
            BAR_DATASET_MANIFEST_KEY: BAR_DATASET_MANIFEST_SHA256,
        },
        eligible_calendar_version=bar_registry.BAR_ELIGIBLE_CALENDAR_VERSION,
        eligible_calendar_sha256=canonical_sha256(calendar),
        split_version=bar_registry.BAR_SPLIT_VERSION,
        split_sha256=split_plan.sha256,
        feature_version=bar_registry.BAR_FEATURE_VERSION,
        feature_sha256=canonical_sha256(policies["signal"]),
        outcome_version=bar_registry.BAR_OUTCOME_VERSION,
        outcome_sha256=canonical_sha256(policies["outcome"]),
        cost_version=bar_registry.BAR_COST_VERSION,
        cost_sha256=canonical_sha256(policies["cost"]),
        execution_version=bar_registry.BAR_EXECUTION_VERSION,
        execution_sha256=canonical_sha256(policies["execution"]),
        code_commit="6" * 40,
        code_snapshot_sha256=SNAPSHOT_SHA256,
        dependency_lock_sha256="a" * 64,
        runtime_environment={
            "bar_research_run": {
                "code_snapshot_artifact_identity_sha256": SNAPSHOT_IDENTITY_SHA256,
                "dataset_handoff_sha256": DATASET_HANDOFF_SHA256,
                "engine_version": bar_registry.BAR_DISCOVERY_ENGINE_VERSION,
                "orchestration": "REGISTER_AND_START_ALL_BEFORE_SINGLE_DISCOVERY_PASS",
            },
            "postgresql": {"schema_migrations_sha256": MIGRATIONS_SHA256},
        },
        random_seed=bar_registry.BAR_RANDOM_SEED,
        direction=candidate.direction.value,
        signal_policy=policies["signal"],
        entry_policy=policies["entry"],
        barrier_policy=policies["barrier"],
        terminal_policy=policies["terminal"],
        parameters=parameters,
    )


def test_registration_document_contains_exactly_216_full_frozen_definitions() -> None:
    config = load_bar_pattern_config(ROOT)
    split_plan = _split_plan()
    document = build_bar_registration_document(
        config,
        split_plan,
        raw_source_manifest_sha256=SOURCE_MANIFEST_SHA256,
        bar_dataset_manifest_sha256=BAR_DATASET_MANIFEST_SHA256,
        code_commit="test-commit",
    )

    assert document["schema"] == BAR_REGISTRATION_SCHEMA
    assert document["campaign_definition"] == config.canonical_parameters()
    assert document["campaign_definition_sha256"] == config.definition_sha256
    assert document["candidate_catalog_sha256"] == config.candidate_catalog_sha256
    assert document["raw_source_manifest_sha256"] == SOURCE_MANIFEST_SHA256
    assert document["bar_dataset_manifest_sha256"] == BAR_DATASET_MANIFEST_SHA256
    assert document["split_plan"] == split_plan.as_dict()
    assert document["split_plan_sha256"] == split_plan.sha256
    assert document["schema_limitations"] == list(BAR_REGISTRY_SCHEMA_LIMITATIONS)
    candidates = document["candidate_catalog"]
    assert isinstance(candidates, list)
    assert len(candidates) == 216
    assert len({item["candidate_key"] for item in candidates}) == 216
    assert all(
        canonical_sha256(item["candidate_definition"]) == item["candidate_definition_sha256"]
        for item in candidates
    )


def test_every_candidate_trial_records_shared_and_candidate_specific_variables() -> None:
    config = load_bar_pattern_config(ROOT)
    split_plan = _split_plan()
    parameter_sets = [
        candidate_trial_parameters(
            config,
            split_plan,
            candidate,
            raw_source_manifest_sha256=SOURCE_MANIFEST_SHA256,
            bar_dataset_manifest_sha256=BAR_DATASET_MANIFEST_SHA256,
        )
        for candidate in config.candidates
    ]

    assert len(parameter_sets) == 216
    assert len({canonical_sha256(parameters) for parameters in parameter_sets}) == 216
    assert all(
        parameters["campaign_definition"] == config.canonical_parameters()
        and parameters["split_plan"] == split_plan.as_dict()
        and parameters["raw_source_manifest_sha256"] == SOURCE_MANIFEST_SHA256
        and parameters["bar_dataset_manifest_sha256"] == BAR_DATASET_MANIFEST_SHA256
        for parameters in parameter_sets
    )


def test_existing_complete_candidate_catalog_is_reused_without_insert_attempts() -> None:
    config = load_bar_pattern_config(ROOT)
    split_plan = _split_plan()
    key_rows: list[dict[str, object]] = []
    catalog_rows: list[dict[str, object]] = []
    for experiment_trial_id, candidate in enumerate(config.candidates, start=1):
        parameters = candidate_trial_parameters(
            config,
            split_plan,
            candidate,
            raw_source_manifest_sha256=SOURCE_MANIFEST_SHA256,
            bar_dataset_manifest_sha256=BAR_DATASET_MANIFEST_SHA256,
        )
        key_rows.append({"trial_key": candidate.candidate_key})
        catalog_rows.append(
            {
                "experiment_trial_id": experiment_trial_id,
                "trial_key": candidate.candidate_key,
                "trial_type": "STRATEGY_VARIANT",
                "status": "REGISTERED",
                "parameters": parameters,
                "parameters_sha256": canonical_sha256(parameters),
                "research_run_spec_id": None,
            }
        )
    connection = _Connection([key_rows, catalog_rows])

    trial_ids, created = bar_registry._ensure_candidate_trials(
        connection,
        experiment_id=11,
        config=config,
        split_plan=split_plan,
        raw_source_manifest_sha256=SOURCE_MANIFEST_SHA256,
        bar_dataset_manifest_sha256=BAR_DATASET_MANIFEST_SHA256,
    )

    assert created == 0
    assert trial_ids == tuple(range(1, 217))
    assert not any(
        "INSERT INTO systematic_fx.experiment_trials" in sql for sql, _ in connection.calls
    )


def test_registration_artifact_is_content_addressed_and_complete(tmp_path: Path) -> None:
    config = load_bar_pattern_config(ROOT)
    split_plan = _split_plan()
    artifact = publish_bar_registration_artifact(
        tmp_path,
        config,
        split_plan,
        raw_source_manifest_sha256=SOURCE_MANIFEST_SHA256,
        bar_dataset_manifest_sha256=BAR_DATASET_MANIFEST_SHA256,
        code_commit="test-commit",
    )

    assert artifact.descriptor.artifact_schema == BAR_REGISTRATION_SCHEMA
    assert artifact.descriptor.record_count == 216
    assert artifact.descriptor.logical_identity["candidate_catalog_sha256"] == (
        config.candidate_catalog_sha256
    )
    assert artifact.path.name == f"sha256={artifact.sha256}.json"


def test_discovery_reader_requires_ascii_escapes_and_final_lf(tmp_path: Path) -> None:
    document = {"artifact_schema": bar_registry.DISCOVERY_RESULT_SCHEMA, "note": "한글"}
    discovery_bytes = (
        json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        + b"\n"
    )
    descriptor = BarArtifactDescriptor(
        artifact_key="test:global-discovery",
        artifact_type=bar_registry.BAR_GLOBAL_DISCOVERY_ARTIFACT_TYPE,
        artifact_schema=bar_registry.DISCOVERY_RESULT_SCHEMA,
        artifact_version=1,
        record_count=216,
        schema_sha256="a" * 64,
        source_manifest_sha256=BAR_DATASET_MANIFEST_SHA256,
        logical_identity={"test": "discovery-canonical"},
        media_type="application/json",
        file_suffix=".json",
    )
    artifact = publish_bar_artifact_bytes(tmp_path, descriptor, discovery_bytes)
    with open_verified_bar_artifact(tmp_path, artifact) as opened:
        assert bar_registry._read_discovery_artifact_document(opened) == document

    non_discovery_bytes = json.dumps(
        document,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    non_discovery = publish_bar_artifact_bytes(
        tmp_path,
        replace(descriptor, artifact_key="test:wrong-global-canonical"),
        non_discovery_bytes,
    )
    with (
        open_verified_bar_artifact(tmp_path, non_discovery) as opened,
        pytest.raises(BarRegistryError, match="Discovery artifact is not exact canonical JSON"),
    ):
        bar_registry._read_discovery_artifact_document(opened)


def test_live_evidence_shard_revalidates_same_fd_parquet_schema_and_metadata(
    tmp_path: Path,
) -> None:
    config = load_bar_pattern_config(ROOT)
    split_plan = _split_plan()
    metadata = {
        b"systematic_fx.candidate_catalog_sha256": config.candidate_catalog_sha256.encode(),
        b"systematic_fx.config_semantic_sha256": config.semantic_sha256.encode(),
        b"systematic_fx.dataset_build_sha256": BAR_DATASET_MANIFEST_SHA256.encode(),
        b"systematic_fx.evidence_identity_sha256": EVIDENCE_IDENTITY_SHA256.encode(),
        b"systematic_fx.evidence_schema": bar_registry.DISCOVERY_EVIDENCE_SCHEMA.encode(),
        b"systematic_fx.outcome_span_policy_sha256": (
            bar_registry.BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256.encode()
        ),
        b"systematic_fx.source_identity_sha256": BAR_DATASET_MANIFEST_SHA256.encode(),
        b"systematic_fx.source_manifest_sha256": SOURCE_MANIFEST_SHA256.encode(),
        b"systematic_fx.split_plan_sha256": split_plan.sha256.encode(),
    }
    schema = bar_registry._EVIDENCE_REPLAY_BASE_SCHEMA.with_metadata(metadata)
    table = pa.Table.from_pylist(
        [{"replay_key": "r1", "decision_ns": 1, "bundle_json": "{}"}],
        schema=schema,
    )
    descriptor = BarArtifactDescriptor(
        artifact_key="test:evidence-replay",
        artifact_type=bar_registry.BAR_EVIDENCE_REPLAY_SHARD_ARTIFACT_TYPE,
        artifact_schema=bar_registry.DISCOVERY_EVIDENCE_SCHEMA,
        artifact_version=1,
        record_count=1,
        schema_sha256=arrow_schema_sha256(schema),
        source_manifest_sha256=SOURCE_MANIFEST_SHA256,
        logical_identity={
            "candidate_catalog_sha256": config.candidate_catalog_sha256,
            "config_semantic_sha256": config.semantic_sha256,
            "dataset_manifest_sha256": BAR_DATASET_MANIFEST_SHA256,
            "evidence_identity_sha256": EVIDENCE_IDENTITY_SHA256,
            "outcome_span_policy_sha256": (bar_registry.BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256),
            "record_kind": "replays",
            "row_count": 1,
            "schema_sha256": arrow_schema_sha256(schema),
            "source_identity_sha256": BAR_DATASET_MANIFEST_SHA256,
            "split_plan_sha256": split_plan.sha256,
        },
        media_type="application/vnd.apache.parquet",
        file_suffix=".parquet",
    )
    artifact = publish_bar_parquet_table(tmp_path, descriptor, table)
    bar_registry._validate_live_evidence_shard(
        tmp_path,
        artifact=artifact,
        record_kind="replays",
        config=config,
        split_plan=split_plan,
        raw_source_manifest_sha256=SOURCE_MANIFEST_SHA256,
        bar_dataset_manifest_sha256=BAR_DATASET_MANIFEST_SHA256,
        evidence_identity_sha256=EVIDENCE_IDENTITY_SHA256,
    )

    wrong_metadata = dict(metadata)
    wrong_metadata[b"systematic_fx.source_identity_sha256"] = ("0" * 64).encode()
    wrong_schema = bar_registry._EVIDENCE_REPLAY_BASE_SCHEMA.with_metadata(wrong_metadata)
    wrong_table = pa.Table.from_pylist(table.to_pylist(), schema=wrong_schema)
    wrong_descriptor = replace(
        descriptor,
        artifact_key="test:evidence-replay-wrong-metadata",
        schema_sha256=arrow_schema_sha256(wrong_schema),
        logical_identity={
            **descriptor.logical_identity,
            "schema_sha256": arrow_schema_sha256(wrong_schema),
        },
    )
    wrong_artifact = publish_bar_parquet_table(tmp_path, wrong_descriptor, wrong_table)
    with pytest.raises(BarRegistryDriftError, match="Parquet schema/row drift"):
        bar_registry._validate_live_evidence_shard(
            tmp_path,
            artifact=wrong_artifact,
            record_kind="replays",
            config=config,
            split_plan=split_plan,
            raw_source_manifest_sha256=SOURCE_MANIFEST_SHA256,
            bar_dataset_manifest_sha256=BAR_DATASET_MANIFEST_SHA256,
            evidence_identity_sha256=EVIDENCE_IDENTITY_SHA256,
        )


def test_bar_run_spec_requires_both_raw_and_final_bar_dataset_manifests() -> None:
    config = load_bar_pattern_config(ROOT)
    split_plan = _split_plan()
    candidate = config.candidates[0]
    parameters = candidate_trial_parameters(
        config,
        split_plan,
        candidate,
        raw_source_manifest_sha256=SOURCE_MANIFEST_SHA256,
        bar_dataset_manifest_sha256=BAR_DATASET_MANIFEST_SHA256,
    )
    valid = _valid_run_spec(config, split_plan, candidate)
    registration = RunSpecRegistration(
        research_run_spec_id=41,
        run_fingerprint=valid.fingerprint,
        campaign_id=11,
        experiment_id=22,
        parent_run_spec_id=None,
        created=True,
    )
    trial_row = {
        "experiment_trial_id": 31,
        "trial_key": candidate.candidate_key,
        "trial_type": "STRATEGY_VARIANT",
        "trial_status": "REGISTERED",
        "parameters": parameters,
        "parameters_sha256": canonical_sha256(parameters),
        "research_run_spec_id": None,
        "experiment_id": 22,
        "experiment_key": BAR_CATALOG_EXPERIMENT_KEY,
        "campaign_id": 11,
        "campaign_key": bar_registry.BAR_PATTERN_CAMPAIGN_KEY,
    }
    spec_row = {
        "research_run_spec_id": 41,
        "run_fingerprint": valid.fingerprint,
        "campaign_id": 11,
        "experiment_id": 22,
        "run_kind": valid.run_kind,
        "engine_version": valid.engine_version,
        "canonical_spec": json.loads(valid.canonical_json()),
        "source_manifest_hashes": dict(valid.source_manifest_hashes),
        "direction": valid.direction,
    }
    snapshot_row = {
        "artifact_id": 51,
        "artifact_type": "bar_code_snapshot",
        "sha256": SNAPSHOT_SHA256,
        "metadata": {
            "logical_identity": {
                "code_commit": valid.code_commit,
                "code_snapshot_sha256": SNAPSHOT_SHA256,
                "dataset_handoff_sha256": DATASET_HANDOFF_SHA256,
                "dataset_manifest_sha256": BAR_DATASET_MANIFEST_SHA256,
                "outcome_span_policy_sha256": (bar_registry.BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256),
                "raw_source_manifest_sha256": SOURCE_MANIFEST_SHA256,
            }
        },
    }
    connection = _Connection([trial_row, spec_row, [snapshot_row], {"experiment_trial_id": 31}])
    with (
        mock.patch.object(bar_registry, "register_run_spec", return_value=registration) as generic,
        mock.patch.object(bar_registry.psycopg, "connect", return_value=connection),
    ):
        report = register_bar_run_spec(
            "postgresql:///test",
            valid,
            config=config,
            split_plan=split_plan,
            candidate_key=candidate.candidate_key,
            raw_source_manifest_sha256=SOURCE_MANIFEST_SHA256,
            bar_dataset_manifest_sha256=BAR_DATASET_MANIFEST_SHA256,
        )
    assert report == registration
    generic.assert_called_once_with(
        "postgresql:///test",
        valid,
        parent_run_fingerprint=None,
    )

    assert any("SET research_run_spec_id" in sql for sql, _ in connection.calls)

    missing_final_bars = replace(
        valid,
        source_manifest_hashes={RAW_SOURCE_MANIFEST_KEY: SOURCE_MANIFEST_SHA256},
    )
    with pytest.raises(BarRegistryError, match="source_manifest_hashes"):
        register_bar_run_spec(
            "postgresql:///unused",
            missing_final_bars,
            config=config,
            split_plan=split_plan,
            candidate_key=candidate.candidate_key,
            raw_source_manifest_sha256=SOURCE_MANIFEST_SHA256,
            bar_dataset_manifest_sha256=BAR_DATASET_MANIFEST_SHA256,
        )


def test_generic_artifact_registration_is_serializable_and_exact(tmp_path: Path) -> None:
    config = load_bar_pattern_config(ROOT)
    artifact = publish_bar_registration_artifact(
        tmp_path,
        config,
        _split_plan(),
        raw_source_manifest_sha256=SOURCE_MANIFEST_SHA256,
        bar_dataset_manifest_sha256=BAR_DATASET_MANIFEST_SHA256,
        code_commit="test-commit",
    )
    connection = _Connection([{"artifact_id": 71}, [_artifact_row(artifact)]])

    with (
        mock.patch.object(bar_registry.psycopg, "connect", return_value=connection),
        mock.patch.object(bar_registry, "_registered_discovery_lineage"),
    ):
        report = register_published_bar_artifact(
            "postgresql:///test",
            tmp_path,
            artifact,
        )

    assert report.artifact_id == 71
    assert report.created
    assert connection.isolation_level is IsolationLevel.SERIALIZABLE
    assert connection.transaction_entries == 1
    assert "ON CONFLICT DO NOTHING" in connection.calls[0][0]
    assert connection.responses == []

    drifted_row = _artifact_row(artifact)
    drifted_row["metadata"] = {"drifted": True}
    drifted = _Connection([None, [drifted_row]])
    with (
        mock.patch.object(bar_registry.psycopg, "connect", return_value=drifted),
        pytest.raises(BarRegistryDriftError, match="metadata"),
    ):
        register_published_bar_artifact("postgresql:///test", tmp_path, artifact)


def test_screening_reject_attempt_succeeds_and_exact_retry_is_idempotent(
    tmp_path: Path,
) -> None:
    config = load_bar_pattern_config(ROOT)
    split_plan = _split_plan()
    candidate = config.candidates[0]
    parameters = candidate_trial_parameters(
        config,
        split_plan,
        candidate,
        raw_source_manifest_sha256=SOURCE_MANIFEST_SHA256,
        bar_dataset_manifest_sha256=BAR_DATASET_MANIFEST_SHA256,
    )
    parameters_sha256 = canonical_sha256(parameters)
    canonical_spec = _valid_run_spec(config, split_plan, candidate).payload()
    run_fingerprint = canonical_sha256(canonical_spec)
    compact_result = {
        **_terminal_compact(candidate, trial_status="REJECTED"),
        "filled_trade_count": 400,
        "reason": "NO_STABLE_CELL",
        "selected_cell_count": 0,
    }
    artifact = _terminal_artifact(
        tmp_path,
        candidate,
        trial_status="REJECTED",
        compact_result=compact_result,
        run_fingerprint=run_fingerprint,
    )
    result = BarTerminalResult(
        candidate_key=candidate.candidate_key,
        candidate_definition_sha256=candidate.definition_sha256,
        run_fingerprint=run_fingerprint,
        research_run_attempt_id=61,
        trial_status="REJECTED",
        decision_label="SCREENING_REJECT",
        compact_result=compact_result,
        artifact=artifact,
    )
    trial_row = {
        "experiment_trial_id": 31,
        "trial_key": candidate.candidate_key,
        "trial_type": "STRATEGY_VARIANT",
        "trial_status": "REGISTERED",
        "parameters": parameters,
        "parameters_sha256": parameters_sha256,
        "research_run_spec_id": 41,
        "trial_result_summary": {},
        "experiment_id": 22,
        "experiment_key": BAR_CATALOG_EXPERIMENT_KEY,
        "campaign_id": 11,
        "campaign_key": "bar_pattern_discovery_v1",
    }
    attempt_row = {
        "research_run_attempt_id": 61,
        "research_run_spec_id": 41,
        "attempt_status": "RUNNING",
        "started_at": datetime(2026, 8, 9, tzinfo=UTC),
        "finished_at": None,
        "result_artifact_id": None,
        "attempt_result_summary": {},
        "run_fingerprint": run_fingerprint,
        "campaign_id": 11,
        "experiment_id": 22,
        "canonical_spec": canonical_spec,
    }
    connection = _Connection(
        [
            trial_row,
            attempt_row,
            {"artifact_id": 71},
            [_artifact_row(artifact)],
            {"research_run_attempt_id": 61},
            {"experiment_trial_id": 31},
        ]
    )

    with (
        mock.patch.object(bar_registry.psycopg, "connect", return_value=connection),
        mock.patch.object(bar_registry, "_registered_discovery_lineage"),
    ):
        report = register_terminal_bar_result(
            "postgresql:///test",
            tmp_path,
            config=config,
            result=result,
        )

    assert report.experiment_trial_id == 31
    assert report.research_run_spec_id == 41
    assert report.result_artifact_id == 71
    assert report.attempt_status == "SUCCEEDED"
    assert report.trial_status == "REJECTED"
    assert report.decision_label == "SCREENING_REJECT"
    assert report.created_artifact
    assert report.transitioned_attempt
    assert report.transitioned_trial
    assert report.schema_limitations == BAR_REGISTRY_SCHEMA_LIMITATIONS
    assert connection.isolation_level is IsolationLevel.SERIALIZABLE
    assert connection.responses == []
    assert any("UPDATE systematic_fx.research_run_attempts" in sql for sql, _ in connection.calls)
    assert any("UPDATE systematic_fx.experiment_trials" in sql for sql, _ in connection.calls)

    summary = bar_registry._terminal_summary(result, result_artifact_id=71)
    retry_trial = {
        **trial_row,
        "trial_status": "REJECTED",
        "research_run_spec_id": 41,
        "trial_result_summary": summary,
    }
    retry_attempt = {
        **attempt_row,
        "attempt_status": "SUCCEEDED",
        "finished_at": datetime(2026, 8, 9, 1, tzinfo=UTC),
        "result_artifact_id": 71,
        "attempt_result_summary": summary,
    }
    retry_connection = _Connection([retry_trial, retry_attempt, None, [_artifact_row(artifact)]])
    with (
        mock.patch.object(
            bar_registry.psycopg,
            "connect",
            return_value=retry_connection,
        ),
        mock.patch.object(bar_registry, "_registered_discovery_lineage"),
    ):
        retried = register_terminal_bar_result(
            "postgresql:///test",
            tmp_path,
            config=config,
            result=result,
        )
    assert retried.attempt_status == "SUCCEEDED"
    assert retried.trial_status == "REJECTED"
    assert not retried.created_artifact
    assert not retried.transitioned_attempt
    assert not retried.transitioned_trial
    assert retry_connection.responses == []
    assert not any("UPDATE systematic_fx" in sql for sql, _ in retry_connection.calls)


@pytest.mark.parametrize("status", ["QUEUED", "RUNNING"])
def test_abort_bar_attempt_fails_exact_active_attempt_and_preserves_bound_trial(
    status: str,
) -> None:
    candidate_key = "pv1_tf0300_lb01_f1_long"
    identity = {
        "research_run_attempt_id": 61,
        "research_run_spec_id": 41,
        "run_fingerprint": RUN_FINGERPRINT,
        "candidate_key": candidate_key,
        "experiment_id": 22,
        "experiment_key": BAR_CATALOG_EXPERIMENT_KEY,
        "campaign_id": 11,
        "campaign_key": bar_registry.BAR_PATTERN_CAMPAIGN_KEY,
    }
    trial = {
        "experiment_trial_id": 31,
        "trial_status": "REGISTERED",
        "research_run_spec_id": 41,
    }
    attempt = {
        "research_run_attempt_id": 61,
        "research_run_spec_id": 41,
        "attempt_number": 2,
        "status": status,
        "result_artifact_id": None,
        "trade_ledger_artifact_id": None,
        "result_summary": {},
        "error_message": None,
        "finished_at": None,
    }
    updated = {
        "research_run_attempt_id": 61,
        "research_run_spec_id": 41,
        "attempt_number": 2,
        "status": "FAILED",
        "result_artifact_id": None,
        "trade_ledger_artifact_id": None,
    }
    connection = _Connection([identity, trial, attempt, updated])

    with mock.patch.object(bar_registry.psycopg, "connect", return_value=connection):
        state = abort_bar_run_attempt(
            "postgresql:///test",
            research_run_attempt_id=61,
            candidate_key=candidate_key,
            run_fingerprint=RUN_FINGERPRINT,
            result_summary={"cleanup_stage": "DISCOVERY"},
            error_message="discovery failed",
        )

    assert state.status == "FAILED"
    assert state.attempt_number == 2
    assert connection.isolation_level is IsolationLevel.SERIALIZABLE
    assert connection.transaction_entries == 1
    assert not any("UPDATE systematic_fx.experiment_trials" in sql for sql, _ in connection.calls)
    update_parameters = connection.calls[-1][1]
    assert isinstance(update_parameters, tuple)
    assert update_parameters[0].obj == {
        "candidate_key": candidate_key,
        "cleanup_stage": "DISCOVERY",
        "run_fingerprint": RUN_FINGERPRINT,
    }


def test_abort_bar_attempt_exact_failed_replay_is_idempotent() -> None:
    candidate_key = "pv1_tf0300_lb01_f1_long"
    summary = {
        "candidate_key": candidate_key,
        "cleanup_stage": "DISCOVERY",
        "run_fingerprint": RUN_FINGERPRINT,
    }
    identity = {
        "research_run_attempt_id": 61,
        "research_run_spec_id": 41,
        "run_fingerprint": RUN_FINGERPRINT,
        "candidate_key": candidate_key,
        "experiment_id": 22,
        "experiment_key": BAR_CATALOG_EXPERIMENT_KEY,
        "campaign_id": 11,
        "campaign_key": bar_registry.BAR_PATTERN_CAMPAIGN_KEY,
    }
    trial = {
        "experiment_trial_id": 31,
        "trial_status": "REGISTERED",
        "research_run_spec_id": 41,
    }
    attempt = {
        "research_run_attempt_id": 61,
        "research_run_spec_id": 41,
        "attempt_number": 2,
        "status": "FAILED",
        "result_artifact_id": None,
        "trade_ledger_artifact_id": None,
        "result_summary": summary,
        "error_message": "discovery failed",
        "finished_at": datetime(2026, 8, 9, tzinfo=UTC),
    }
    connection = _Connection([identity, trial, attempt])

    with mock.patch.object(bar_registry.psycopg, "connect", return_value=connection):
        state = abort_bar_run_attempt(
            "postgresql:///test",
            research_run_attempt_id=61,
            candidate_key=candidate_key,
            run_fingerprint=RUN_FINGERPRINT,
            result_summary={"cleanup_stage": "DISCOVERY"},
            error_message="discovery failed",
        )

    assert state.status == "FAILED"
    assert len(connection.calls) == 3


def test_abort_bar_attempt_rejects_unbound_trial_before_attempt_mutation() -> None:
    candidate_key = "pv1_tf0300_lb01_f1_long"
    connection = _Connection(
        [
            {
                "research_run_attempt_id": 61,
                "research_run_spec_id": 41,
                "run_fingerprint": RUN_FINGERPRINT,
                "candidate_key": candidate_key,
                "experiment_id": 22,
                "experiment_key": BAR_CATALOG_EXPERIMENT_KEY,
                "campaign_id": 11,
                "campaign_key": bar_registry.BAR_PATTERN_CAMPAIGN_KEY,
            },
            {
                "experiment_trial_id": 31,
                "trial_status": "REGISTERED",
                "research_run_spec_id": None,
            },
        ]
    )
    with (
        mock.patch.object(bar_registry.psycopg, "connect", return_value=connection),
        pytest.raises(BarRegistryError, match="prebound candidate trial"),
    ):
        abort_bar_run_attempt(
            "postgresql:///test",
            research_run_attempt_id=61,
            candidate_key=candidate_key,
            run_fingerprint=RUN_FINGERPRINT,
            result_summary={"cleanup_stage": "DISCOVERY"},
            error_message="discovery failed",
        )
    assert len(connection.calls) == 2


def test_terminal_registration_rejects_artifact_or_candidate_drift_before_database(
    tmp_path: Path,
) -> None:
    config = load_bar_pattern_config(ROOT)
    candidate = config.candidates[0]
    artifact = _terminal_artifact(tmp_path, candidate)
    result = BarTerminalResult(
        candidate_key=candidate.candidate_key,
        candidate_definition_sha256="f" * 64,
        run_fingerprint=RUN_FINGERPRINT,
        research_run_attempt_id=61,
        trial_status="REJECTED",
        decision_label="SCREENING_REJECT",
        compact_result={
            **_terminal_compact(candidate, trial_status="REJECTED"),
            "candidate_definition_sha256": "f" * 64,
            "reason": "NO_STABLE_CELL",
        },
        artifact=artifact,
    )

    with pytest.raises(BarRegistryError, match="definition SHA-256 drift"):
        register_terminal_bar_result(
            "postgresql:///unused",
            tmp_path,
            config=config,
            result=result,
        )


def test_terminal_summary_schema_and_compact_bound_are_frozen(tmp_path: Path) -> None:
    config = load_bar_pattern_config(ROOT)
    candidate = config.candidates[0]
    artifact = _terminal_artifact(tmp_path, candidate)

    with pytest.raises(BarRegistryError, match="64 KiB"):
        BarTerminalResult(
            candidate_key=candidate.candidate_key,
            candidate_definition_sha256=candidate.definition_sha256,
            run_fingerprint=RUN_FINGERPRINT,
            research_run_attempt_id=61,
            trial_status="SUCCEEDED",
            decision_label="DISCOVERY_FINALIST",
            compact_result={
                **_terminal_compact(candidate),
                "oversized": "x" * 70_000,
            },
            artifact=artifact,
        )

    rejected_artifact = _terminal_artifact(tmp_path, candidate, trial_status="REJECTED")
    valid = BarTerminalResult(
        candidate_key=candidate.candidate_key,
        candidate_definition_sha256=candidate.definition_sha256,
        run_fingerprint=RUN_FINGERPRINT,
        research_run_attempt_id=61,
        trial_status="REJECTED",
        decision_label="SCREENING_REJECT",
        compact_result={
            **_terminal_compact(candidate, trial_status="REJECTED"),
            "reason": "INSUFFICIENT_SUPPORT",
        },
        artifact=rejected_artifact,
    )
    summary = bar_registry._terminal_summary(valid, result_artifact_id=71)
    assert summary["schema"] == BAR_TERMINAL_RESULT_SCHEMA
    assert summary["compact_result_sha256"] == canonical_sha256(valid.compact_result)


@pytest.mark.parametrize(
    "drift",
    ["raw_source", "missing_dataset", "extra_source", "campaign_parameter", "split_parameter"],
)
def test_terminal_run_attempt_rebinds_exact_sources_config_and_split(
    tmp_path: Path,
    drift: str,
) -> None:
    config = load_bar_pattern_config(ROOT)
    split_plan = _split_plan()
    candidate = config.candidates[0]
    parameters = candidate_trial_parameters(
        config,
        split_plan,
        candidate,
        raw_source_manifest_sha256=SOURCE_MANIFEST_SHA256,
        bar_dataset_manifest_sha256=BAR_DATASET_MANIFEST_SHA256,
    )
    parameters_sha256 = canonical_sha256(parameters)
    canonical_spec: dict[str, object] = {
        "campaign_id": "bar_pattern_discovery_v1",
        "experiment_id": BAR_CATALOG_EXPERIMENT_KEY,
        "parameters": {
            "bar_campaign_definition_sha256": config.definition_sha256,
            "bar_candidate_definition_sha256": candidate.definition_sha256,
            "bar_candidate_key": candidate.candidate_key,
            "bar_split_plan_sha256": split_plan.sha256,
            "bar_trial_parameters_sha256": parameters_sha256,
        },
        "source_manifest_hashes": {
            RAW_SOURCE_MANIFEST_KEY: SOURCE_MANIFEST_SHA256,
            BAR_DATASET_MANIFEST_KEY: BAR_DATASET_MANIFEST_SHA256,
        },
        "split": {"sha256": split_plan.sha256},
    }
    source_hashes = canonical_spec["source_manifest_hashes"]
    run_parameters = canonical_spec["parameters"]
    assert isinstance(source_hashes, dict) and isinstance(run_parameters, dict)
    if drift == "raw_source":
        source_hashes[RAW_SOURCE_MANIFEST_KEY] = "0" * 64
    elif drift == "missing_dataset":
        del source_hashes[BAR_DATASET_MANIFEST_KEY]
    elif drift == "extra_source":
        source_hashes["unregistered_extra"] = "e" * 64
    elif drift == "campaign_parameter":
        run_parameters["bar_campaign_definition_sha256"] = "0" * 64
    else:
        run_parameters["bar_split_plan_sha256"] = "0" * 64
    run_fingerprint = canonical_sha256(canonical_spec)
    compact = _terminal_compact(candidate, trial_status="REJECTED")
    result = BarTerminalResult(
        candidate_key=candidate.candidate_key,
        candidate_definition_sha256=candidate.definition_sha256,
        run_fingerprint=run_fingerprint,
        research_run_attempt_id=61,
        trial_status="REJECTED",
        decision_label="SCREENING_REJECT",
        compact_result=compact,
        artifact=_terminal_artifact(tmp_path, candidate, trial_status="REJECTED"),
    )
    trial = {
        "campaign_id": 11,
        "experiment_id": 22,
        "parameters": parameters,
        "parameters_sha256": parameters_sha256,
        "research_run_spec_id": 41,
    }
    attempt = {
        "research_run_attempt_id": 61,
        "research_run_spec_id": 41,
        "attempt_status": "RUNNING",
        "started_at": datetime(2026, 8, 9, tzinfo=UTC),
        "finished_at": None,
        "result_artifact_id": None,
        "attempt_result_summary": {},
        "run_fingerprint": run_fingerprint,
        "campaign_id": 11,
        "experiment_id": 22,
        "canonical_spec": canonical_spec,
    }

    with pytest.raises(BarRegistryDriftError, match="RunSpec does not bind"):
        bar_registry._locked_run_attempt(
            _Connection([attempt]),
            trial=trial,
            result=result,
        )


@pytest.mark.parametrize("drift", ["campaign", "candidate", "split"])
def test_terminal_candidate_trial_rebinds_full_registered_parameters(
    tmp_path: Path,
    drift: str,
) -> None:
    config = load_bar_pattern_config(ROOT)
    split_plan = _split_plan()
    candidate = config.candidates[0]
    parameters = candidate_trial_parameters(
        config,
        split_plan,
        candidate,
        raw_source_manifest_sha256=SOURCE_MANIFEST_SHA256,
        bar_dataset_manifest_sha256=BAR_DATASET_MANIFEST_SHA256,
    )
    if drift == "campaign":
        parameters["campaign_definition_sha256"] = "0" * 64
    elif drift == "candidate":
        candidate_definition = dict(parameters["candidate_definition"])
        candidate_definition["entry_index"] = "t+2"
        parameters["candidate_definition"] = candidate_definition
    else:
        split_document = dict(parameters["split_plan"])
        split_document["unexpected"] = True
        parameters["split_plan"] = split_document
    compact = _terminal_compact(candidate, trial_status="REJECTED")
    result = BarTerminalResult(
        candidate_key=candidate.candidate_key,
        candidate_definition_sha256=candidate.definition_sha256,
        run_fingerprint=RUN_FINGERPRINT,
        research_run_attempt_id=61,
        trial_status="REJECTED",
        decision_label="SCREENING_REJECT",
        compact_result=compact,
        artifact=_terminal_artifact(tmp_path, candidate, trial_status="REJECTED"),
    )
    trial = {
        "experiment_trial_id": 31,
        "trial_key": candidate.candidate_key,
        "trial_type": "STRATEGY_VARIANT",
        "trial_status": "REGISTERED",
        "parameters": parameters,
        "parameters_sha256": canonical_sha256(parameters),
        "research_run_spec_id": None,
        "trial_result_summary": {},
        "experiment_id": 22,
        "experiment_key": BAR_CATALOG_EXPERIMENT_KEY,
        "campaign_id": 11,
        "campaign_key": "bar_pattern_discovery_v1",
    }

    with pytest.raises(BarRegistryDriftError, match="identity drift"):
        bar_registry._locked_candidate_trial(
            _Connection([trial]),
            result,
            config=config,
        )


def test_terminal_artifact_contract_binds_static_schema_lineage_and_document(
    tmp_path: Path,
) -> None:
    config = load_bar_pattern_config(ROOT)
    split_plan = _split_plan()
    candidate = config.candidates[0]
    compact = {
        **_terminal_compact(candidate, trial_status="REJECTED"),
        "reason": "INSUFFICIENT_SUPPORT",
    }
    artifact = _terminal_artifact(
        tmp_path,
        candidate,
        trial_status="REJECTED",
        compact_result=compact,
    )
    assert artifact.descriptor.artifact_schema == BAR_TERMINAL_ARTIFACT_SCHEMA
    assert artifact.descriptor.record_count == BAR_TERMINAL_ARTIFACT_RECORD_COUNT == 484
    assert artifact.descriptor.schema_sha256 == BAR_TERMINAL_ARTIFACT_SCHEMA_SHA256
    assert artifact.descriptor.source_manifest_sha256 == BAR_DATASET_MANIFEST_SHA256
    parameters = candidate_trial_parameters(
        config,
        split_plan,
        candidate,
        raw_source_manifest_sha256=SOURCE_MANIFEST_SHA256,
        bar_dataset_manifest_sha256=BAR_DATASET_MANIFEST_SHA256,
    )
    trial = {"parameters": parameters}
    canonical_spec = _valid_run_spec(config, split_plan, candidate).payload()
    result = BarTerminalResult(
        candidate_key=candidate.candidate_key,
        candidate_definition_sha256=candidate.definition_sha256,
        run_fingerprint=RUN_FINGERPRINT,
        research_run_attempt_id=61,
        trial_status="REJECTED",
        decision_label="SCREENING_REJECT",
        compact_result=compact,
        artifact=artifact,
    )
    with open_verified_bar_artifact(tmp_path, artifact) as opened:
        document = bar_registry._read_terminal_artifact_document(opened)
        bar_registry._validate_terminal_artifact_contract(
            config=config,
            trial=trial,
            canonical_spec=canonical_spec,
            result=result,
            document=document,
        )

        drifted_spec = json.loads(_valid_run_spec(config, split_plan, candidate).canonical_json())
        drifted_spec["parameters"]["bar_postgres_migrations_sha256"] = "0" * 64
        with pytest.raises(BarRegistryDriftError, match="bound RunSpec"):
            bar_registry._validate_terminal_artifact_contract(
                config=config,
                trial=trial,
                canonical_spec=drifted_spec,
                result=result,
                document=document,
            )

        drifted_result = replace(
            result,
            compact_result={**compact, "reason": "DIFFERENT_COMPACT_RESULT"},
        )
        with pytest.raises(BarRegistryError, match="descriptor differs"):
            bar_registry._validate_terminal_artifact_contract(
                config=config,
                trial=trial,
                canonical_spec=canonical_spec,
                result=drifted_result,
                document=document,
            )

    wrong_dataset_artifact = _terminal_artifact(
        tmp_path,
        candidate,
        trial_status="REJECTED",
        compact_result=compact,
        bar_dataset_manifest_sha256="e" * 64,
    )
    wrong_dataset_result = replace(result, artifact=wrong_dataset_artifact)
    with open_verified_bar_artifact(tmp_path, wrong_dataset_artifact) as opened:
        wrong_document = bar_registry._read_terminal_artifact_document(opened)
        with pytest.raises(BarRegistryError, match="lineage drift"):
            bar_registry._validate_terminal_artifact_contract(
                config=config,
                trial=trial,
                canonical_spec=canonical_spec,
                result=wrong_dataset_result,
                document=wrong_document,
            )

    drifted_descriptor = replace(
        artifact.descriptor,
        record_count=BAR_TERMINAL_ARTIFACT_RECORD_COUNT + 1,
    )
    drifted_artifact = publish_bar_json_artifact(
        tmp_path,
        drifted_descriptor,
        json.loads(artifact.path.read_bytes()),
    )
    with pytest.raises(BarRegistryError, match="static contract"):
        register_terminal_bar_result(
            "postgresql:///unused",
            tmp_path,
            config=config,
            result=replace(result, artifact=drifted_artifact),
        )
