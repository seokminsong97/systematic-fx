from __future__ import annotations

import json
import os
import shutil
import unittest
import uuid
from datetime import date
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

from systematic_fx.db.bar_registry import (
    BAR_CATALOG_EXPERIMENT_KEY,
    BAR_DATASET_MANIFEST_KEY,
    BAR_PATTERN_CAMPAIGN_KEY,
    RAW_SOURCE_MANIFEST_KEY,
    _expected_bar_policy_documents,
    bar_registration_artifact_descriptor,
    candidate_trial_parameters,
)
from systematic_fx.db.migrations import apply_migrations
from systematic_fx.research.bar_artifacts import BarArtifactDescriptor
from systematic_fx.research.bar_config import (
    BAR_PATTERN_QUALIFICATION_STATUS,
    BAR_SOURCE_MANIFEST_SHA256,
    load_bar_pattern_config,
)
from systematic_fx.research.bar_pipeline import BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256
from systematic_fx.research.hypotheses import canonical_sha256
from systematic_fx.research.run_spec import RunSpec
from systematic_fx.validation.bar_splits import BarDateRange, BarSplitPlan

APPROVED_ELIGIBLE_CALENDAR_SHA256 = (
    "92e2112c24463f3a9a2f59182a4ad6099e6a8fc740f3d8ddc771d31e61c1163d"
)
APPROVED_SPLIT_PLAN_SHA256 = "5594725f6769a706018d414a5b27e3903f1d7d1cc22c98e93b6e973ead1af043"


def _approved_split_plan() -> BarSplitPlan:
    """Return the compact, frozen split preimage without consulting local data."""

    document = {
        "active_day_count": 1413,
        "eligible_end_date": "2026-07-31",
        "eligible_start_date": "2022-01-03",
        "policy": {
            "boundary_tail_days": 20,
            "discovery_formula": "220+floor(2*(P-580)/5)",
            "discovery_reporting_blocks": 4,
            "embargo_days": 20,
            "holdout_decision_days": 120,
            "outcome_tail_days": 20,
            "walk_forward_folds": 5,
        },
        "ranges": [
            {
                "active_day_count": 489,
                "decision_end_date": "2023-07-10",
                "end_active_ordinal": 489,
                "end_date": "2023-08-02",
                "fold_number": None,
                "result_visibility": "VISIBLE",
                "role": "DISCOVERY",
                "split_key": "discovery",
                "start_active_ordinal": 1,
                "start_date": "2022-01-03",
            },
            {
                "active_day_count": 153,
                "decision_end_date": "2024-01-09",
                "end_active_ordinal": 642,
                "end_date": "2024-02-01",
                "fold_number": 1,
                "result_visibility": "SEALED",
                "role": "WALK_FORWARD",
                "split_key": "walk_forward_1",
                "start_active_ordinal": 490,
                "start_date": "2023-08-03",
            },
            {
                "active_day_count": 153,
                "decision_end_date": "2024-07-10",
                "end_active_ordinal": 795,
                "end_date": "2024-08-04",
                "fold_number": 2,
                "result_visibility": "SEALED",
                "role": "WALK_FORWARD",
                "split_key": "walk_forward_2",
                "start_active_ordinal": 643,
                "start_date": "2024-02-02",
            },
            {
                "active_day_count": 153,
                "decision_end_date": "2025-01-06",
                "end_active_ordinal": 948,
                "end_date": "2025-01-29",
                "fold_number": 3,
                "result_visibility": "SEALED",
                "role": "WALK_FORWARD",
                "split_key": "walk_forward_3",
                "start_active_ordinal": 796,
                "start_date": "2024-08-05",
            },
            {
                "active_day_count": 153,
                "decision_end_date": "2025-07-06",
                "end_active_ordinal": 1101,
                "end_date": "2025-07-29",
                "fold_number": 4,
                "result_visibility": "SEALED",
                "role": "WALK_FORWARD",
                "split_key": "walk_forward_4",
                "start_active_ordinal": 949,
                "start_date": "2025-01-30",
            },
            {
                "active_day_count": 152,
                "decision_end_date": "2025-12-30",
                "end_active_ordinal": 1253,
                "end_date": "2026-01-22",
                "fold_number": 5,
                "result_visibility": "SEALED",
                "role": "WALK_FORWARD",
                "split_key": "walk_forward_5",
                "start_active_ordinal": 1102,
                "start_date": "2025-07-30",
            },
            {
                "active_day_count": 20,
                "decision_end_date": None,
                "end_active_ordinal": 1273,
                "end_date": "2026-02-15",
                "fold_number": None,
                "result_visibility": "SEALED",
                "role": "EMBARGO",
                "split_key": "holdout_embargo",
                "start_active_ordinal": 1254,
                "start_date": "2026-01-23",
            },
            {
                "active_day_count": 120,
                "decision_end_date": "2026-07-08",
                "end_active_ordinal": 1393,
                "end_date": "2026-07-08",
                "fold_number": None,
                "result_visibility": "SEALED",
                "role": "HOLDOUT",
                "split_key": "sealed_holdout",
                "start_active_ordinal": 1274,
                "start_date": "2026-02-16",
            },
            {
                "active_day_count": 20,
                "decision_end_date": None,
                "end_active_ordinal": 1413,
                "end_date": "2026-07-31",
                "fold_number": None,
                "result_visibility": "SEALED",
                "role": "OUTCOME_TAIL",
                "split_key": "holdout_outcome_tail",
                "start_active_ordinal": 1394,
                "start_date": "2026-07-09",
            },
        ],
        "reporting_blocks": [
            {
                "active_day_count": 118,
                "decision_end_date": "2022-05-22",
                "end_active_ordinal": 118,
                "end_date": "2022-05-22",
                "fold_number": None,
                "result_visibility": "VISIBLE",
                "role": "DISCOVERY_REPORTING_BLOCK",
                "split_key": "discovery_block_1",
                "start_active_ordinal": 1,
                "start_date": "2022-01-03",
            },
            {
                "active_day_count": 117,
                "decision_end_date": "2022-10-05",
                "end_active_ordinal": 235,
                "end_date": "2022-10-05",
                "fold_number": None,
                "result_visibility": "VISIBLE",
                "role": "DISCOVERY_REPORTING_BLOCK",
                "split_key": "discovery_block_2",
                "start_active_ordinal": 119,
                "start_date": "2022-05-23",
            },
            {
                "active_day_count": 117,
                "decision_end_date": "2023-02-23",
                "end_active_ordinal": 352,
                "end_date": "2023-02-23",
                "fold_number": None,
                "result_visibility": "VISIBLE",
                "role": "DISCOVERY_REPORTING_BLOCK",
                "split_key": "discovery_block_3",
                "start_active_ordinal": 236,
                "start_date": "2022-10-06",
            },
            {
                "active_day_count": 117,
                "decision_end_date": "2023-07-10",
                "end_active_ordinal": 469,
                "end_date": "2023-07-10",
                "fold_number": None,
                "result_visibility": "VISIBLE",
                "role": "DISCOVERY_REPORTING_BLOCK",
                "split_key": "discovery_block_4",
                "start_active_ordinal": 353,
                "start_date": "2023-02-24",
            },
        ],
        "schema": "systematic_fx.bar_pattern_splits.v1",
    }
    canonical = json.dumps(
        document,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if canonical_sha256(document) != APPROVED_SPLIT_PLAN_SHA256:
        raise AssertionError("approved split fixture preimage drift")

    def decode_range(value: dict[str, object]) -> BarDateRange:
        decision_end = value["decision_end_date"]
        fold_number = value["fold_number"]
        return BarDateRange(
            split_key=str(value["split_key"]),
            role=str(value["role"]),
            start_date=date.fromisoformat(str(value["start_date"])),
            end_date=date.fromisoformat(str(value["end_date"])),
            start_active_ordinal=int(value["start_active_ordinal"]),
            end_active_ordinal=int(value["end_active_ordinal"]),
            decision_end_date=(
                None if decision_end is None else date.fromisoformat(str(decision_end))
            ),
            result_visibility=str(value["result_visibility"]),
            fold_number=None if fold_number is None else int(fold_number),
        )

    ranges = tuple(decode_range(value) for value in document["ranges"])
    blocks = tuple(decode_range(value) for value in document["reporting_blocks"])
    return BarSplitPlan(
        eligible_dates=(date(2022, 1, 3), date(2026, 7, 31)),
        discovery=ranges[0],
        discovery_reporting_blocks=blocks,
        walk_forward_folds=ranges[1:6],
        embargo=ranges[6],
        holdout=ranges[7],
        outcome_tail=ranges[8],
        canonical_bytes=canonical,
        sha256=APPROVED_SPLIT_PLAN_SHA256,
    )


def exercise_bar_registry_governance(connection: psycopg.Connection[object]) -> None:
    """Exercise migration 0022 entirely inside the caller's rollback-only transaction."""

    connection.execute(
        "CREATE TEMP TABLE bar_non_governed_attempt_probe "
        "(research_run_spec_id bigint NOT NULL) ON COMMIT DROP"
    )
    connection.execute(
        "CREATE TRIGGER bar_non_governed_attempt_probe_trigger "
        "BEFORE INSERT OR UPDATE ON bar_non_governed_attempt_probe "
        "FOR EACH ROW EXECUTE FUNCTION "
        "systematic_fx.enforce_bar_pattern_attempt_immediate()"
    )
    inserted_probe = connection.execute(
        "INSERT INTO bar_non_governed_attempt_probe VALUES (-1) RETURNING research_run_spec_id"
    ).fetchone()
    if inserted_probe != (-1,):
        raise AssertionError("non-governed attempt insert was suppressed by the bar trigger")
    updated_probe = connection.execute(
        "UPDATE bar_non_governed_attempt_probe SET research_run_spec_id = -2 "
        "RETURNING research_run_spec_id"
    ).fetchone()
    if updated_probe != (-2,):
        raise AssertionError("non-governed attempt update was suppressed by the bar trigger")

    suffix = uuid.uuid4().hex
    savepoint_number = 0

    def assert_rejected(
        sql: str,
        parameters: tuple[object, ...],
        message: str,
    ) -> None:
        nonlocal savepoint_number
        savepoint_number += 1
        savepoint = f"bar_governance_{savepoint_number}"
        connection.execute(f"SAVEPOINT {savepoint}")
        try:
            connection.execute(sql, parameters)
        except psycopg.errors.RaiseException as error:
            connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            connection.execute(f"RELEASE SAVEPOINT {savepoint}")
            if message not in str(error):
                raise AssertionError(f"unexpected governance error: {error}") from error
        else:
            connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            connection.execute(f"RELEASE SAVEPOINT {savepoint}")
            raise AssertionError(f"statement unexpectedly succeeded: {sql}")

    config = load_bar_pattern_config(Path.cwd())
    candidate = config.candidates[0]
    candidate_key = candidate.candidate_key
    split_plan = _approved_split_plan()
    dataset_manifest_sha256 = "e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc"
    dataset_handoff_sha256 = "26b1bb96f7323cae13bbe5d670c12f3e85615bbb9aab56932ce6523e67af7b00"
    migration_sha256 = "c" * 64
    snapshot_identity_sha256 = "d" * 64
    code_snapshot_sha256 = "e" * 64
    dependency_lock_sha256 = "f" * 64
    code_commit = "1" * 40
    campaign_parameters = config.canonical_parameters()
    candidate_definition_sha256 = candidate.definition_sha256
    dataset_id = connection.execute(
        """
        INSERT INTO systematic_fx.datasets
            (dataset_key, provider, feed, data_schema, root_uri, status, manifest_sha256)
        VALUES (%s, 'test', 'test', 'mbp-10', %s, 'VALIDATING', %s)
        RETURNING dataset_id
        """,
        (
            "glbx_mdp3_mbp_10_6e_fut_v1",
            f"data/test/{suffix}",
            BAR_SOURCE_MANIFEST_SHA256,
        ),
    ).fetchone()[0]
    registration_descriptor = bar_registration_artifact_descriptor(
        config,
        split_plan,
        raw_source_manifest_sha256=BAR_SOURCE_MANIFEST_SHA256,
        bar_dataset_manifest_sha256=dataset_manifest_sha256,
        code_commit=code_commit,
    )
    registration_sha256 = str(registration_descriptor.logical_identity["document_sha256"])
    registration_metadata = {
        **registration_descriptor.identity_document(),
        "artifact_identity_sha256": registration_descriptor.identity_sha256,
        "content_sha256": registration_sha256,
    }
    registration_artifact_id = connection.execute(
        """
        INSERT INTO systematic_fx.artifacts
            (artifact_key, artifact_type, uri, sha256, byte_size, media_type, metadata)
        VALUES (%s, %s, %s, %s, 1, %s, %s)
        RETURNING artifact_id
        """,
        (
            registration_descriptor.artifact_key,
            registration_descriptor.artifact_type,
            f"file:///tmp/{suffix}-bar-registration.json",
            registration_sha256,
            registration_descriptor.media_type,
            Jsonb(registration_metadata),
        ),
    ).fetchone()[0]
    split_policy = {
        "bar_dataset_manifest_sha256": dataset_manifest_sha256,
        "raw_source_manifest_sha256": BAR_SOURCE_MANIFEST_SHA256,
        "split_plan": split_plan.as_dict(),
        "split_plan_sha256": split_plan.sha256,
    }
    campaign_id = connection.execute(
        """
        INSERT INTO systematic_fx.campaigns
            (campaign_key, dataset_id, name, status, selected_start_date,
             selected_end_date, roll_cutoff_date, data_manifest_sha256,
             feature_version, outcome_version, cost_model_version,
             execution_model_version, code_commit, config_sha256, split_policy,
             trial_budget, finalist_budget, frozen_at)
        VALUES (%s, %s, 'Frozen multi-timeframe OHLC bar-pattern screening',
                'FROZEN', %s, %s, NULL, %s,
                'selected_contract_trade_ohlcv_bars_v1',
                'bar_first_touch_surface_v1', 'bar_conservative_combined_cost_v1',
                'bar_next_open_stop_first_v1', %s, %s, %s, 240, 10,
                statement_timestamp())
        RETURNING campaign_id
        """,
        (
            BAR_PATTERN_CAMPAIGN_KEY,
            dataset_id,
            split_plan.eligible_dates[0],
            split_plan.eligible_dates[-1],
            dataset_manifest_sha256,
            code_commit,
            config.definition_sha256,
            Jsonb(split_policy),
        ),
    ).fetchone()[0]
    feature_versions = {
        "bar_feature_version": "selected_contract_trade_ohlcv_bars_v1",
        "candidate_catalog_sha256": config.candidate_catalog_sha256,
    }
    search_boundary = {
        "allocated_candidate_count": 216,
        "bar_dataset_manifest_sha256": dataset_manifest_sha256,
        "campaign_definition_sha256": config.definition_sha256,
        "raw_source_manifest_sha256": BAR_SOURCE_MANIFEST_SHA256,
        "result_driven_additions_allowed": False,
        "split_plan_sha256": split_plan.sha256,
        "unallocated_campaign_budget": 24,
    }
    cost_assumptions = {"execution_scenarios": campaign_parameters["execution_scenarios"]}
    execution_assumptions = campaign_parameters["entry"]
    experiment_config_sha256 = canonical_sha256(
        {
            "cost_assumptions": cost_assumptions,
            "execution_assumptions": execution_assumptions,
            "feature_versions": feature_versions,
            "search_boundary": search_boundary,
        }
    )
    experiment_id = connection.execute(
        """
        INSERT INTO systematic_fx.experiments
            (experiment_key, campaign_id, primary_family, status, hypothesis,
             direction, model_family, tick_size, tick_value,
             feature_definition_versions, search_boundary, cost_assumptions,
             execution_assumptions, trial_budget, trials_registered,
             registration_artifact_id, code_commit, config_sha256, frozen_at)
        VALUES (
            %s, %s, 'FIXED_OHLC_BAR_PATTERN_CATALOG', 'FROZEN',
            'Fixed OHLC setup and trigger patterns have stable next-open first-touch economics',
            'BOTH', 'RULE_BASED_FIXED_OHLC',
            0.00005, 6.25, %s, %s, %s, %s,
            216, 216, %s, %s, %s, statement_timestamp()
        )
        RETURNING experiment_id
        """,
        (
            BAR_CATALOG_EXPERIMENT_KEY,
            campaign_id,
            Jsonb(feature_versions),
            Jsonb(search_boundary),
            Jsonb(cost_assumptions),
            Jsonb(execution_assumptions),
            registration_artifact_id,
            code_commit,
            experiment_config_sha256,
        ),
    ).fetchone()[0]
    assert_rejected(
        "UPDATE systematic_fx.campaigns SET holdout_revealed_at = statement_timestamp() "
        "WHERE campaign_id = %s",
        (campaign_id,),
        "campaign identity is immutable",
    )
    assert_rejected(
        "UPDATE systematic_fx.experiments SET search_boundary = '{}'::jsonb "
        "WHERE experiment_id = %s",
        (experiment_id,),
        "experiment identity is immutable",
    )
    assert_rejected(
        "INSERT INTO systematic_fx.experiment_trials "
        "(experiment_id, trial_key, trial_type, status, parameters, parameters_sha256, "
        "started_at, finished_at) VALUES (%s, %s, 'STRATEGY_VARIANT', 'REJECTED', "
        "'{}'::jsonb, %s, statement_timestamp(), statement_timestamp())",
        (experiment_id, f"terminal-poison-{suffix}", "0" * 64),
        "pristine unbound registrations",
    )
    trial_id = None
    for catalog_candidate in config.candidates:
        catalog_parameters = candidate_trial_parameters(
            config,
            split_plan,
            catalog_candidate,
            raw_source_manifest_sha256=BAR_SOURCE_MANIFEST_SHA256,
            bar_dataset_manifest_sha256=dataset_manifest_sha256,
        )
        inserted_trial_id = connection.execute(
            """
            INSERT INTO systematic_fx.experiment_trials
                (experiment_id, trial_key, trial_type, status, parameters,
                 parameters_sha256)
            VALUES (%s, %s, 'STRATEGY_VARIANT', 'REGISTERED', %s, %s)
            RETURNING experiment_trial_id
            """,
            (
                experiment_id,
                catalog_candidate.candidate_key,
                Jsonb(catalog_parameters),
                canonical_sha256(catalog_parameters),
            ),
        ).fetchone()[0]
        if catalog_candidate.candidate_key == candidate_key:
            trial_id = inserted_trial_id
    if trial_id is None:
        raise AssertionError("fixture failed to register the selected bar candidate")
    runtime = {
        "artifact_schema": "systematic_fx.runtime_environment.v1",
        "bar_research_run": {
            "code_snapshot_artifact_identity_sha256": snapshot_identity_sha256,
            "dataset_handoff_sha256": dataset_handoff_sha256,
            "engine_version": "bar_pattern_streaming_discovery_v1",
            "orchestration": "REGISTER_AND_START_ALL_BEFORE_SINGLE_DISCOVERY_PASS",
        },
        "cpu_count": 1,
        "locale": {"encoding": "UTF-8", "language": "en_US"},
        "numeric_environment": {},
        "packages": {},
        "platform": {"machine": "test", "release": "test", "system": "test"},
        "postgresql": {"schema_migrations_sha256": migration_sha256},
        "python": {"byteorder": "little", "implementation": "CPython", "version": "test"},
        "timezone": {"daylight": False, "names": ["UTC", "UTC"], "utc_offset_seconds": 0},
    }

    def candidate_run_spec(catalog_candidate: object) -> RunSpec:
        policies = _expected_bar_policy_documents(config, catalog_candidate)
        candidate_parameters = candidate_trial_parameters(
            config,
            split_plan,
            catalog_candidate,
            raw_source_manifest_sha256=BAR_SOURCE_MANIFEST_SHA256,
            bar_dataset_manifest_sha256=dataset_manifest_sha256,
        )
        parameters = {
            "bar_cost_policy": policies["cost"],
            "bar_barrier_policy_sha256": canonical_sha256(policies["barrier"]),
            "bar_campaign_definition_sha256": config.definition_sha256,
            "bar_candidate_catalog_sha256": config.candidate_catalog_sha256,
            "bar_candidate_definition_sha256": catalog_candidate.definition_sha256,
            "bar_candidate_key": catalog_candidate.candidate_key,
            "bar_code_snapshot_artifact_identity_sha256": snapshot_identity_sha256,
            "bar_config_file_sha256": config.sha256,
            "bar_config_semantic_sha256": config.semantic_sha256,
            "bar_cost_policy_sha256": canonical_sha256(policies["cost"]),
            "bar_dataset_handoff_sha256": dataset_handoff_sha256,
            "bar_dataset_manifest_sha256": dataset_manifest_sha256,
            "bar_entry_policy_sha256": canonical_sha256(policies["entry"]),
            "bar_evidence_policy": policies["evidence"],
            "bar_evidence_policy_sha256": canonical_sha256(policies["evidence"]),
            "bar_execution_policy": policies["execution"],
            "bar_outcome_policy": policies["outcome"],
            "bar_outcome_span_policy_sha256": BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
            "bar_postgres_migrations_sha256": migration_sha256,
            "bar_raw_source_manifest_sha256": BAR_SOURCE_MANIFEST_SHA256,
            "bar_screening_only": True,
            "bar_selection_policy": policies["selection"],
            "bar_selection_policy_sha256": canonical_sha256(policies["selection"]),
            "bar_split_plan_sha256": split_plan.sha256,
            "bar_trial_parameters_sha256": canonical_sha256(candidate_parameters),
            "qualification_status": BAR_PATTERN_QUALIFICATION_STATUS,
        }
        return RunSpec(
            campaign_id=BAR_PATTERN_CAMPAIGN_KEY,
            experiment_id=BAR_CATALOG_EXPERIMENT_KEY,
            run_kind="SCREEN",
            engine_version="bar_pattern_streaming_discovery_v1",
            source_manifest_hashes={
                RAW_SOURCE_MANIFEST_KEY: BAR_SOURCE_MANIFEST_SHA256,
                BAR_DATASET_MANIFEST_KEY: dataset_manifest_sha256,
            },
            eligible_calendar_version="bar_dataset_eligible_calendar_v1",
            eligible_calendar_sha256=APPROVED_ELIGIBLE_CALENDAR_SHA256,
            split_version="bar_pattern_splits_v1",
            split_sha256=split_plan.sha256,
            feature_version="selected_contract_trade_ohlcv_bars_v1",
            feature_sha256=canonical_sha256(policies["signal"]),
            outcome_version="bar_first_touch_surface_v1",
            outcome_sha256=canonical_sha256(policies["outcome"]),
            cost_version="bar_conservative_combined_cost_v1",
            cost_sha256=canonical_sha256(policies["cost"]),
            execution_version="bar_next_open_stop_first_v1",
            execution_sha256=canonical_sha256(policies["execution"]),
            code_commit=code_commit,
            code_snapshot_sha256=code_snapshot_sha256,
            dependency_lock_sha256=dependency_lock_sha256,
            runtime_environment=runtime,
            random_seed=0,
            direction=catalog_candidate.direction.value,
            signal_policy=policies["signal"],
            entry_policy=policies["entry"],
            barrier_policy=policies["barrier"],
            terminal_policy=policies["terminal"],
            parameters=parameters,
        )

    run_spec = candidate_run_spec(candidate)
    run_fingerprint = run_spec.fingerprint
    connection.execute(
        """
        INSERT INTO systematic_fx.artifacts
            (artifact_key, artifact_type, uri, sha256, byte_size, metadata)
        VALUES (%s, 'bar_code_snapshot', %s, %s, 1, %s)
        """,
        (
            f"bar-code-snapshot-{suffix}",
            f"data/code-snapshot/{suffix}.json",
            code_snapshot_sha256,
            Jsonb(
                {
                    "artifact_identity_sha256": snapshot_identity_sha256,
                    "logical_identity": {
                        "code_commit": code_commit,
                        "dataset_handoff_sha256": dataset_handoff_sha256,
                        "dataset_manifest_sha256": dataset_manifest_sha256,
                        "outcome_span_policy_sha256": BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
                        "raw_source_manifest_sha256": BAR_SOURCE_MANIFEST_SHA256,
                    },
                }
            ),
        ),
    )

    def insert_run_spec(catalog_run_spec: RunSpec) -> int:
        catalog_spec = json.loads(catalog_run_spec.canonical_json())
        return connection.execute(
            """
            INSERT INTO systematic_fx.research_run_specs
                (run_fingerprint, canonicalization_schema, canonicalization_version,
                 campaign_id, experiment_id, run_kind, engine_version, canonical_spec,
                 source_manifest_hashes, eligible_calendar_version,
                 eligible_calendar_sha256, split_version, split_sha256,
                 feature_version, feature_sha256, outcome_version, outcome_sha256,
                 cost_version, cost_sha256, execution_version, execution_sha256,
                 code_commit, code_snapshot_sha256, dependency_lock_sha256,
                 deterministic_seed, direction)
            VALUES (%s, 'systematic_fx.research_run_spec.v2', 2, %s, %s, 'SCREEN',
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, 0, %s)
            RETURNING research_run_spec_id
            """,
            (
                catalog_run_spec.fingerprint,
                campaign_id,
                experiment_id,
                catalog_run_spec.engine_version,
                Jsonb(catalog_spec),
                Jsonb(dict(catalog_run_spec.source_manifest_hashes)),
                catalog_run_spec.eligible_calendar_version,
                catalog_run_spec.eligible_calendar_sha256,
                catalog_run_spec.split_version,
                catalog_run_spec.split_sha256,
                catalog_run_spec.feature_version,
                catalog_run_spec.feature_sha256,
                catalog_run_spec.outcome_version,
                catalog_run_spec.outcome_sha256,
                catalog_run_spec.cost_version,
                catalog_run_spec.cost_sha256,
                catalog_run_spec.execution_version,
                catalog_run_spec.execution_sha256,
                code_commit,
                code_snapshot_sha256,
                dependency_lock_sha256,
                catalog_run_spec.direction,
            ),
        ).fetchone()[0]

    run_spec_ids: dict[str, int] = {}
    for catalog_candidate in config.candidates:
        catalog_run_spec = candidate_run_spec(catalog_candidate)
        catalog_run_spec_id = insert_run_spec(catalog_run_spec)
        bound_trial = connection.execute(
            "UPDATE systematic_fx.experiment_trials SET research_run_spec_id = %s "
            "WHERE experiment_id = %s AND trial_key = %s "
            "RETURNING experiment_trial_id",
            (
                catalog_run_spec_id,
                experiment_id,
                catalog_candidate.candidate_key,
            ),
        ).fetchone()
        if bound_trial is None:
            raise AssertionError("fixture failed to bind a catalog RunSpec")
        run_spec_ids[catalog_candidate.candidate_key] = catalog_run_spec_id
    run_spec_id = run_spec_ids[candidate_key]

    assert_rejected(
        "UPDATE systematic_fx.experiment_trials SET research_run_spec_id = NULL "
        "WHERE experiment_trial_id = %s",
        (trial_id,),
        "RunSpec binding is immutable",
    )
    assert_rejected(
        "UPDATE systematic_fx.experiment_trials SET status = 'FAILED', "
        "finished_at = statement_timestamp() WHERE experiment_trial_id = %s",
        (trial_id,),
        "aborts fail attempts",
    )

    attempt_id = connection.execute(
        """
        INSERT INTO systematic_fx.research_run_attempts
            (research_run_spec_id, attempt_number, status)
        VALUES (%s, 1, 'QUEUED')
        RETURNING research_run_attempt_id
        """,
        (run_spec_id,),
    ).fetchone()[0]
    connection.execute(
        """
        UPDATE systematic_fx.research_run_attempts
        SET status = 'FAILED', result_summary = %s, error_message = 'fixture abort',
            finished_at = statement_timestamp()
        WHERE research_run_attempt_id = %s
        """,
        (
            Jsonb({"candidate_key": candidate_key, "run_fingerprint": run_fingerprint}),
            attempt_id,
        ),
    )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.execute("SET CONSTRAINTS ALL DEFERRED")

    connection.execute("SAVEPOINT bar_governance_missing_candidate")
    try:
        forged_spec_id = connection.execute(
            """
            INSERT INTO systematic_fx.research_run_specs
                (run_fingerprint, canonicalization_schema, canonicalization_version,
                 campaign_id, experiment_id, parent_run_spec_id, run_kind,
                 engine_version, canonical_spec, source_manifest_hashes,
                 eligible_calendar_version, eligible_calendar_sha256,
                 split_version, split_sha256, feature_version, feature_sha256,
                 outcome_version, outcome_sha256, cost_version, cost_sha256,
                 execution_version, execution_sha256, code_commit,
                 code_snapshot_sha256, dependency_lock_sha256,
                 deterministic_seed, direction)
            SELECT %s, canonicalization_schema, canonicalization_version,
                   campaign_id, experiment_id, parent_run_spec_id, run_kind,
                   engine_version,
                   canonical_spec #- '{parameters,bar_candidate_key}',
                   source_manifest_hashes, eligible_calendar_version,
                   eligible_calendar_sha256, split_version, split_sha256,
                   feature_version, feature_sha256, outcome_version, outcome_sha256,
                   cost_version, cost_sha256, execution_version, execution_sha256,
                   code_commit, code_snapshot_sha256, dependency_lock_sha256,
                   deterministic_seed, direction
            FROM systematic_fx.research_run_specs
            WHERE research_run_spec_id = %s
            RETURNING research_run_spec_id
            """,
            ("0" * 64, run_spec_id),
        ).fetchone()[0]
        connection.execute(
            "INSERT INTO systematic_fx.research_run_attempts "
            "(research_run_spec_id, attempt_number, status) VALUES (%s, 1, 'QUEUED')",
            (forged_spec_id,),
        )
        connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    except psycopg.errors.RaiseException as error:
        connection.execute("ROLLBACK TO SAVEPOINT bar_governance_missing_candidate")
        connection.execute("RELEASE SAVEPOINT bar_governance_missing_candidate")
        if "lacks a canonical candidate key" not in str(error):
            raise AssertionError(f"unexpected missing-candidate error: {error}") from error
    else:
        connection.execute("ROLLBACK TO SAVEPOINT bar_governance_missing_candidate")
        connection.execute("RELEASE SAVEPOINT bar_governance_missing_candidate")
        raise AssertionError("governed RunSpec without a candidate key bypassed attempt guard")
    connection.execute("SET CONSTRAINTS ALL DEFERRED")

    compact_result = {
        "evidence_artifact_identity_sha256": "1" * 64,
        "evidence_manifest_sha256": "2" * 64,
        "global_result_artifact_identity_sha256": "3" * 64,
        "global_result_artifact_sha256": "4" * 64,
    }
    compact_result_sha256 = canonical_sha256(compact_result)
    terminal_logical_identity = {
        "bar_dataset_manifest_sha256": dataset_manifest_sha256,
        "campaign_definition_sha256": config.definition_sha256,
        "candidate_definition_sha256": candidate_definition_sha256,
        "candidate_key": candidate_key,
        "candidate_result_sha256": "5" * 64,
        "compact_result_sha256": compact_result_sha256,
        "decision_label": "SCREENING_REJECT",
        "final_label": "SUPPORT_REJECT",
        "raw_source_manifest_sha256": BAR_SOURCE_MANIFEST_SHA256,
        "run_fingerprint": run_fingerprint,
        "split_plan_sha256": split_plan.sha256,
        "trial_status": "REJECTED",
    }
    terminal_descriptor = BarArtifactDescriptor(
        artifact_key=(
            f"{BAR_PATTERN_CAMPAIGN_KEY}:terminal:{candidate_key}:"
            f"{run_fingerprint}:{terminal_logical_identity['candidate_result_sha256']}"
        ),
        artifact_type="bar_terminal_result",
        artifact_schema="systematic_fx.bar_terminal_result_artifact.v1",
        artifact_version=1,
        record_count=484,
        schema_sha256="200dd0663c0100459eced9dc01f6ca59689444b37181610c9753fe3d18aeea8b",
        source_manifest_sha256=dataset_manifest_sha256,
        logical_identity=terminal_logical_identity,
        media_type="application/json",
        file_suffix=".json",
    )
    terminal_sha256 = "a" * 64
    terminal_metadata = {
        **terminal_descriptor.identity_document(),
        "artifact_identity_sha256": terminal_descriptor.identity_sha256,
        "content_sha256": terminal_sha256,
    }
    terminal_artifact_id = connection.execute(
        """
        INSERT INTO systematic_fx.artifacts
            (artifact_key, artifact_type, uri, sha256, byte_size, media_type, metadata)
        VALUES (%s, 'bar_terminal_result', %s, %s, 1, 'application/json', %s)
        RETURNING artifact_id
        """,
        (
            terminal_descriptor.artifact_key,
            f"file:///tmp/{suffix}-bar-terminal.json",
            terminal_sha256,
            Jsonb(terminal_metadata),
        ),
    ).fetchone()[0]
    terminal_summary = {
        "artifact": {
            "artifact_id": terminal_artifact_id,
            "artifact_identity_sha256": terminal_descriptor.identity_sha256,
            "artifact_key": terminal_descriptor.artifact_key,
            "byte_size": 1,
            "sha256": terminal_sha256,
        },
        "attempt_status": "SUCCEEDED",
        "candidate_definition_sha256": candidate_definition_sha256,
        "candidate_key": candidate_key,
        "compact_result": compact_result,
        "compact_result_sha256": compact_result_sha256,
        "decision_label": "SCREENING_REJECT",
        "final_label": "SUPPORT_REJECT",
        "run_fingerprint": run_fingerprint,
        "schema": "systematic_fx.bar_pattern_terminal_result.v1",
        "trial_status": "REJECTED",
    }
    assert_rejected(
        "INSERT INTO systematic_fx.research_run_attempts "
        "(research_run_spec_id, attempt_number, status, result_artifact_id, "
        "result_summary, started_at, finished_at) "
        "VALUES (%s, 2, 'SUCCEEDED', %s, %s, statement_timestamp(), "
        "statement_timestamp())",
        (run_spec_id, terminal_artifact_id, Jsonb(terminal_summary)),
        "must be inserted as QUEUED or SKIPPED_DUPLICATE",
    )
    succeeded_attempt_id = connection.execute(
        """
        INSERT INTO systematic_fx.research_run_attempts
            (research_run_spec_id, attempt_number, status)
        VALUES (%s, 2, 'QUEUED')
        RETURNING research_run_attempt_id
        """,
        (run_spec_id,),
    ).fetchone()[0]
    connection.execute(
        "UPDATE systematic_fx.research_run_attempts "
        "SET status = 'RUNNING', started_at = statement_timestamp() "
        "WHERE research_run_attempt_id = %s",
        (succeeded_attempt_id,),
    )
    assert_rejected(
        "UPDATE systematic_fx.research_run_attempts "
        "SET status = 'SUCCEEDED', result_artifact_id = %s, result_summary = %s, "
        "finished_at = statement_timestamp() WHERE research_run_attempt_id = %s",
        (terminal_artifact_id, Jsonb(terminal_summary), succeeded_attempt_id),
        "invalid candidate lineage",
    )
    for catalog_key, catalog_run_spec_id in run_spec_ids.items():
        if catalog_key == candidate_key:
            continue
        catalog_attempt_id = connection.execute(
            "INSERT INTO systematic_fx.research_run_attempts "
            "(research_run_spec_id, attempt_number, status) "
            "VALUES (%s, 1, 'QUEUED') RETURNING research_run_attempt_id",
            (catalog_run_spec_id,),
        ).fetchone()[0]
        connection.execute(
            "UPDATE systematic_fx.research_run_attempts "
            "SET status = 'RUNNING', started_at = statement_timestamp() "
            "WHERE research_run_attempt_id = %s",
            (catalog_attempt_id,),
        )
    unrelated_terminal_artifact_id = connection.execute(
        """
        INSERT INTO systematic_fx.artifacts
            (artifact_key, artifact_type, uri, sha256, byte_size, metadata)
        VALUES (%s, 'unrelated_fixture', %s, %s, 1, '{}'::jsonb)
        RETURNING artifact_id
        """,
        (
            f"bar-unrelated-terminal-{suffix}",
            f"data/unrelated/{suffix}-terminal.json",
            "7" * 64,
        ),
    ).fetchone()[0]
    wrong_artifact_summary = {
        **terminal_summary,
        "artifact": {
            **terminal_summary["artifact"],
            "artifact_id": unrelated_terminal_artifact_id,
            "artifact_key": f"bar-unrelated-terminal-{suffix}",
            "sha256": "7" * 64,
        },
    }
    assert_rejected(
        "UPDATE systematic_fx.research_run_attempts "
        "SET status = 'SUCCEEDED', result_artifact_id = %s, result_summary = %s, "
        "finished_at = statement_timestamp() WHERE research_run_attempt_id = %s",
        (
            unrelated_terminal_artifact_id,
            Jsonb(wrong_artifact_summary),
            succeeded_attempt_id,
        ),
        "invalid candidate lineage",
    )
    assert_rejected(
        "UPDATE systematic_fx.research_run_attempts "
        "SET status = 'SUCCEEDED', result_artifact_id = %s, result_summary = %s, "
        "started_at = NULL, finished_at = statement_timestamp() "
        "WHERE research_run_attempt_id = %s",
        (terminal_artifact_id, Jsonb(terminal_summary), succeeded_attempt_id),
        "invalid candidate lineage",
    )
    assert_rejected(
        "UPDATE systematic_fx.research_run_attempts "
        "SET status = 'SUCCEEDED', result_artifact_id = %s, "
        "trade_ledger_artifact_id = %s, result_summary = %s, "
        "finished_at = statement_timestamp() WHERE research_run_attempt_id = %s",
        (
            terminal_artifact_id,
            terminal_artifact_id,
            Jsonb(terminal_summary),
            succeeded_attempt_id,
        ),
        "invalid candidate lineage",
    )
    connection.execute(
        """
        UPDATE systematic_fx.research_run_attempts
        SET status = 'SUCCEEDED', result_artifact_id = %s, result_summary = %s,
            finished_at = statement_timestamp()
        WHERE research_run_attempt_id = %s
        """,
        (terminal_artifact_id, Jsonb(terminal_summary), succeeded_attempt_id),
    )
    connection.execute(
        """
        UPDATE systematic_fx.experiment_trials
        SET status = 'REJECTED', result_summary = %s,
            started_at = statement_timestamp(), finished_at = statement_timestamp()
        WHERE experiment_trial_id = %s
        """,
        (Jsonb(terminal_summary), trial_id),
    )
    connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    connection.execute("SET CONSTRAINTS ALL DEFERRED")
    assert_rejected(
        "UPDATE systematic_fx.experiment_trials SET status = status WHERE experiment_trial_id = %s",
        (trial_id,),
        "terminal governed bar-pattern candidate trials are immutable",
    )
    assert_rejected(
        "DELETE FROM systematic_fx.experiment_trials WHERE experiment_trial_id = %s",
        (trial_id,),
        "append-preserved",
    )

    protected_artifact_id = connection.execute(
        """
        INSERT INTO systematic_fx.artifacts
            (artifact_key, artifact_type, uri, sha256, byte_size, metadata)
        VALUES (%s, 'bar_global_discovery_result', %s, %s, 1, '{}'::jsonb)
        RETURNING artifact_id
        """,
        (f"bar-protected-{suffix}", f"data/protected/{suffix}.json", "8" * 64),
    ).fetchone()[0]
    assert_rejected(
        "UPDATE systematic_fx.artifacts SET byte_size = 2 WHERE artifact_id = %s",
        (protected_artifact_id,),
        "immutable",
    )
    assert_rejected(
        "DELETE FROM systematic_fx.artifacts WHERE artifact_id = %s",
        (protected_artifact_id,),
        "immutable",
    )
    unrelated_artifact_id = connection.execute(
        """
        INSERT INTO systematic_fx.artifacts
            (artifact_key, artifact_type, uri, sha256, byte_size, metadata)
        VALUES (%s, 'unrelated_fixture', %s, %s, 1, '{}'::jsonb)
        RETURNING artifact_id
        """,
        (f"bar-unrelated-{suffix}", f"data/unrelated/{suffix}.json", "9" * 64),
    ).fetchone()[0]
    connection.execute(
        "UPDATE systematic_fx.artifacts SET byte_size = 2 WHERE artifact_id = %s",
        (unrelated_artifact_id,),
    )
    connection.execute(
        "DELETE FROM systematic_fx.artifacts WHERE artifact_id = %s",
        (unrelated_artifact_id,),
    )


class BarRegistryGovernancePostgreSQLTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.database_url = os.environ.get("SYSTEMATIC_FX_TEST_DATABASE_URL")
        if not cls.database_url:
            raise unittest.SkipTest("SYSTEMATIC_FX_TEST_DATABASE_URL is not set")
        cls.psql = shutil.which(os.environ.get("SYSTEMATIC_FX_PSQL", "psql"))
        if cls.psql is None:
            raise unittest.SkipTest("psql is not installed or is not on PATH")
        apply_migrations(cls.database_url, psql_binary=cls.psql)

    def test_bar_registry_governance_is_transactional_and_fail_closed(self) -> None:
        with psycopg.connect(self.database_url) as connection:
            try:
                exercise_bar_registry_governance(connection)
            finally:
                connection.rollback()


if __name__ == "__main__":
    unittest.main()
