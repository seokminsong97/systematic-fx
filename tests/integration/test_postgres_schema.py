import os
import shutil
import subprocess
import unittest
import uuid

import psycopg
from psycopg.types.json import Jsonb

from systematic_fx.db.migrations import apply_migrations

EXPECTED_TABLES = {
    "artifacts",
    "backtest_metrics",
    "backtest_runs",
    "campaign_days",
    "campaign_splits",
    "campaigns",
    "datasets",
    "derived_partition_sources",
    "derived_partitions",
    "discovery_exposures",
    "experiment_trials",
    "experiments",
    "instrument_mappings",
    "instruments",
    "jobs",
    "pattern_ledger",
    "publication_outbox",
    "quality_checks",
    "research_run_attempts",
    "research_run_specs",
    "schema_migrations",
    "source_files",
    "strategies",
}

EXPECTED_CONSTRAINTS = {
    "backtest_metrics_exactly_one_value",
    "campaign_splits_fold_valid",
    "discovery_exposures_interval_order",
    "discovery_exposures_campaign_run_spec_fk",
    "experiments_frozen_registration_required",
    "instrument_mappings_class_valid",
    "instrument_mappings_date_order",
    "instruments_execution_requires_outright",
    "quality_checks_exactly_one_target",
    "research_run_attempts_identity",
    "research_run_specs_code_snapshot_sha256_valid",
    "research_run_specs_experiment_ownership",
    "research_run_specs_fingerprint_valid",
    "source_files_dataset_fk",
    "strategies_take_profit_positive",
}

EXPECTED_TRIGGERS = {
    "artifacts_protect_phase1a_lineage",
    "campaigns_protect_phase1a_identity",
    "derived_partition_sources_protect_phase1a_lineage",
    "derived_partitions_protect_phase1a_lineage",
    "discovery_exposures_require_phase1a_success",
    "research_run_attempts_require_duplicate_success",
    "research_run_attempts_protect_phase1a_artifact_links",
    "source_files_protect_phase1a_lineage",
}

EXPECTED_INDEXES = {
    "research_run_attempts_one_active",
}


class PostgreSQLSchemaIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.database_url = os.environ.get("SYSTEMATIC_FX_TEST_DATABASE_URL")
        if not cls.database_url:
            raise unittest.SkipTest("SYSTEMATIC_FX_TEST_DATABASE_URL is not set")
        cls.psql = shutil.which(os.environ.get("SYSTEMATIC_FX_PSQL", "psql"))
        if cls.psql is None:
            raise unittest.SkipTest("psql is not installed or is not on PATH")

        apply_migrations(cls.database_url, psql_binary=cls.psql)

    @classmethod
    def _run_sql(cls, sql: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                cls.psql,
                "-X",
                "--no-password",
                "--set=ON_ERROR_STOP=1",
                "--tuples-only",
                "--no-align",
                "--quiet",
                "--dbname",
                cls.database_url,
                "--command",
                sql,
            ],
            check=False,
            capture_output=True,
            text=True,
        )

    def test_migration_is_repeatable_and_expected_tables_exist(self) -> None:
        report = apply_migrations(self.database_url, psql_binary=self.psql)
        self.assertEqual(report.applied, ())
        self.assertIn(1, report.skipped)
        self.assertIn(2, report.skipped)
        self.assertIn(3, report.skipped)
        self.assertIn(4, report.skipped)
        self.assertIn(5, report.skipped)
        self.assertIn(6, report.skipped)
        self.assertIn(7, report.skipped)
        self.assertIn(8, report.skipped)
        self.assertIn(9, report.skipped)
        self.assertIn(10, report.skipped)
        self.assertIn(11, report.skipped)

        result = self._run_sql(
            "SELECT tablename FROM pg_catalog.pg_tables "
            "WHERE schemaname = 'systematic_fx' ORDER BY tablename"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        actual_tables = set(result.stdout.splitlines())
        self.assertTrue(
            EXPECTED_TABLES <= actual_tables,
            f"missing tables: {sorted(EXPECTED_TABLES - actual_tables)}",
        )

    def test_expected_constraints_exist_and_reject_invalid_rows(self) -> None:
        result = self._run_sql(
            "SELECT conname FROM pg_catalog.pg_constraint c "
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.connamespace "
            "WHERE n.nspname = 'systematic_fx' ORDER BY conname"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        actual_constraints = set(result.stdout.splitlines())
        self.assertTrue(
            EXPECTED_CONSTRAINTS <= actual_constraints,
            f"missing constraints: {sorted(EXPECTED_CONSTRAINTS - actual_constraints)}",
        )

        invalid_hash_key = f"constraint-test-{uuid.uuid4()}"
        invalid_hash = self._run_sql(
            "INSERT INTO systematic_fx.datasets "
            "(dataset_key, provider, feed, data_schema, root_uri, manifest_sha256) "
            f"VALUES ('{invalid_hash_key}', 'test', 'test', 'test', '/tmp/test', 'bad-hash')"
        )
        self.assertNotEqual(invalid_hash.returncode, 0)
        self.assertIn("datasets_manifest_sha256_valid", invalid_hash.stderr)

        missing_parent = self._run_sql(
            "INSERT INTO systematic_fx.source_files "
            "(dataset_id, source_date, relative_uri, byte_size) "
            "VALUES (9223372036854775807, DATE '2026-01-01', 'missing.parquet', 1)"
        )
        self.assertNotEqual(missing_parent.returncode, 0)
        self.assertIn("source_files_dataset_fk", missing_parent.stderr)

    def test_phase1a_lineage_guard_triggers_exist(self) -> None:
        result = self._run_sql(
            "SELECT t.tgname FROM pg_catalog.pg_trigger t "
            "JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid "
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'systematic_fx' AND NOT t.tgisinternal "
            "ORDER BY t.tgname"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        actual_triggers = set(result.stdout.splitlines())
        self.assertTrue(
            EXPECTED_TRIGGERS <= actual_triggers,
            f"missing triggers: {sorted(EXPECTED_TRIGGERS - actual_triggers)}",
        )
        indexes = self._run_sql(
            "SELECT indexname FROM pg_catalog.pg_indexes "
            "WHERE schemaname = 'systematic_fx' ORDER BY indexname"
        )
        self.assertEqual(indexes.returncode, 0, indexes.stderr)
        actual_indexes = set(indexes.stdout.splitlines())
        self.assertTrue(
            EXPECTED_INDEXES <= actual_indexes,
            f"missing indexes: {sorted(EXPECTED_INDEXES - actual_indexes)}",
        )

    def test_phase1a_artifact_and_feature_lineage_is_immutable_but_other_rows_are_not(
        self,
    ) -> None:
        suffix = uuid.uuid4().hex
        fingerprint = suffix * 2
        phase_campaign_key = "phase1a_conservative_screening_v1"
        with psycopg.connect(self.database_url) as connection:
            try:
                phase_campaign = connection.execute(
                    "SELECT campaign_id, dataset_id FROM systematic_fx.campaigns "
                    "WHERE campaign_key = %s",
                    (phase_campaign_key,),
                ).fetchone()
                if phase_campaign is None:
                    phase_dataset_id = connection.execute(
                        """
                        INSERT INTO systematic_fx.datasets
                            (dataset_key, provider, feed, data_schema, root_uri,
                             status, manifest_sha256)
                        VALUES (%s, 'test', 'test', 'mbp-10', %s, 'VALIDATING', %s)
                        RETURNING dataset_id
                        """,
                        (
                            f"phase1a-lineage-dataset-{suffix}",
                            f"data/phase1a-lineage/{suffix}",
                            "0" * 64,
                        ),
                    ).fetchone()[0]
                    phase_campaign_id = connection.execute(
                        """
                        INSERT INTO systematic_fx.campaigns
                            (campaign_key, dataset_id, name, status,
                             data_manifest_sha256, feature_version, outcome_version,
                             cost_model_version, execution_model_version, code_commit,
                             config_sha256, split_policy, trial_budget, finalist_budget)
                        VALUES (%s, %s, 'Phase 1A lineage fixture', 'DRAFT', %s,
                                'phase1a_mbp10_screening_v1', 'outcome-v1', 'cost-v1',
                                'execution-v1', 'fixture', %s, '{}'::jsonb, 10, 1)
                        RETURNING campaign_id
                        """,
                        (phase_campaign_key, phase_dataset_id, "0" * 64, "1" * 64),
                    ).fetchone()[0]
                else:
                    phase_campaign_id, phase_dataset_id = phase_campaign

                run_spec_id = connection.execute(
                    """
                    INSERT INTO systematic_fx.research_run_specs
                        (run_fingerprint, canonicalization_schema,
                         canonicalization_version, campaign_id, experiment_id,
                         parent_run_spec_id, run_kind, engine_version, canonical_spec,
                         source_manifest_hashes, eligible_calendar_version,
                         eligible_calendar_sha256, split_version, split_sha256,
                         feature_version, feature_sha256, outcome_version,
                         outcome_sha256, cost_version, cost_sha256, execution_version,
                         execution_sha256, code_commit, code_snapshot_sha256,
                         dependency_lock_sha256, deterministic_seed, direction)
                    VALUES (%s, 'fixture.run_spec.v1', 1, %s, NULL, NULL, 'QUERY',
                            'fixture-engine-v1', %s, %s, 'calendar-v1', %s,
                            'split-v1', %s, 'phase1a_mbp10_screening_v1', %s,
                            'outcome-v1', %s, 'cost-v1', %s, 'execution-v1', %s,
                            'fixture', %s, %s, 1, 'BOTH')
                    RETURNING research_run_spec_id
                    """,
                    (
                        fingerprint,
                        phase_campaign_id,
                        Jsonb({"code_snapshot_sha256": "c" * 64}),
                        Jsonb({"mbp10": "a" * 64}),
                        "a" * 64,
                        "b" * 64,
                        "c" * 64,
                        "d" * 64,
                        "e" * 64,
                        "f" * 64,
                        "c" * 64,
                        "9" * 64,
                    ),
                ).fetchone()[0]

                def insert_artifact(
                    label: str,
                    artifact_type: str,
                    *,
                    phase1a_control: bool = False,
                ) -> int:
                    return connection.execute(
                        """
                        INSERT INTO systematic_fx.artifacts
                            (artifact_key, artifact_type, uri, sha256, byte_size,
                             media_type, metadata)
                        VALUES (%s, %s, %s, %s, 1, 'application/json', %s)
                        RETURNING artifact_id
                        """,
                        (
                            f"phase1a-lineage:{label}:{suffix}",
                            artifact_type,
                            f"artifacts/phase1a-lineage/{suffix}/{label}.json",
                            "a" * 64,
                            Jsonb(
                                {
                                    "fixture": label,
                                    **(
                                        {"campaign_key": ("phase1a_conservative_screening_v1")}
                                        if phase1a_control
                                        else {}
                                    ),
                                }
                            ),
                        ),
                    ).fetchone()[0]

                artifacts = {
                    "discovery": insert_artifact("discovery", "DISCOVERY_EXPOSURE_RESULT"),
                    "run_result": insert_artifact("run-result", "RUN_RESULT"),
                    "trade": insert_artifact("trade", "TRADE_LEDGER"),
                    "pattern": insert_artifact("pattern", "PATTERN_CONTEXT"),
                    "manifest": insert_artifact(
                        "manifest",
                        "PHASE1A_FEATURE_BUILD_MANIFEST",
                    ),
                    "calendar": insert_artifact(
                        "calendar",
                        "PHASE1A_ELIGIBLE_CALENDAR",
                        phase1a_control=True,
                    ),
                    "split": insert_artifact(
                        "split",
                        "PHASE1A_CAMPAIGN_SPLIT",
                        phase1a_control=True,
                    ),
                    "snapshot": insert_artifact(
                        "snapshot",
                        "PHASE1A_CODE_SNAPSHOT",
                        phase1a_control=True,
                    ),
                    "registry": insert_artifact(
                        "registry",
                        "PHASE1A_SCREENING_REGISTRY",
                        phase1a_control=True,
                    ),
                    "running": insert_artifact("running", "RUNNING_RESULT"),
                    "candidate": insert_artifact("candidate", "UNLINKED_CANDIDATE"),
                    "nonphase": insert_artifact("nonphase", "UNRELATED_RESULT"),
                }
                connection.execute(
                    """
                    INSERT INTO systematic_fx.research_run_attempts
                        (research_run_spec_id, attempt_number, status,
                         result_artifact_id, trade_ledger_artifact_id,
                         result_summary, finished_at)
                    VALUES (%s, 1, 'SUCCEEDED', %s, %s, '{}'::jsonb,
                            statement_timestamp())
                    """,
                    (run_spec_id, artifacts["discovery"], artifacts["trade"]),
                )
                connection.execute(
                    """
                    INSERT INTO systematic_fx.research_run_attempts
                        (research_run_spec_id, attempt_number, status,
                         result_artifact_id, result_summary, finished_at)
                    VALUES (%s, 2, 'REJECTED', %s, '{}'::jsonb,
                            statement_timestamp())
                    """,
                    (run_spec_id, artifacts["run_result"]),
                )
                connection.execute(
                    """
                    INSERT INTO systematic_fx.discovery_exposures
                        (exposure_key, campaign_id, exposure_type,
                         source_interval_start, source_interval_end, visible_to_ai,
                         research_eligible, query_spec, result_summary,
                         result_artifact_id, code_commit, config_sha256,
                         research_run_spec_id)
                    VALUES (%s, %s, 'QUERY', statement_timestamp(),
                            statement_timestamp(), true, false, '{}'::jsonb,
                            '{}'::jsonb, %s, 'fixture', %s, %s)
                    """,
                    (
                        f"phase1a-lineage:exposure:{suffix}",
                        phase_campaign_id,
                        artifacts["discovery"],
                        "2" * 64,
                        run_spec_id,
                    ),
                )
                running_attempt_id = connection.execute(
                    """
                    INSERT INTO systematic_fx.research_run_attempts
                        (research_run_spec_id, attempt_number, status,
                         result_artifact_id, started_at)
                    VALUES (%s, 3, 'RUNNING', %s, statement_timestamp())
                    RETURNING research_run_attempt_id
                    """,
                    (run_spec_id, artifacts["running"]),
                ).fetchone()[0]
                connection.execute(
                    """
                    INSERT INTO systematic_fx.pattern_ledger
                        (pattern_key, campaign_id, status, first_seen_from,
                         first_seen_to, last_updated_interval,
                         feature_definition_versions, direction, entry_condition,
                         economic_rationale, support_count,
                         forward_first_touch_summaries, cost_assumptions,
                         context_artifact_id)
                    VALUES (%s, %s, 'OPEN', statement_timestamp(),
                            statement_timestamp(), statement_timestamp(), %s, 'BOTH',
                            'fixture entry', 'fixture rationale', 1, %s,
                            '{}'::jsonb, %s)
                    """,
                    (
                        f"phase1a-lineage:pattern:{suffix}",
                        phase_campaign_id,
                        Jsonb({"rollup_schema": "systematic_fx.phase1a_pattern_rollup.v1"}),
                        Jsonb({"rollup_schema": "systematic_fx.phase1a_pattern_rollup.v1"}),
                        artifacts["pattern"],
                    ),
                )

                current_uri = f"raw/{suffix}/2022-01-04.parquet"
                previous_uri = f"raw/{suffix}/2022-01-03.parquet"
                current_source_id = connection.execute(
                    """
                    INSERT INTO systematic_fx.source_files
                        (dataset_id, source_date, relative_uri, byte_size, sha256,
                         row_count, status)
                    VALUES (%s, DATE '2022-01-04', %s, 1, %s, 1, 'HASHED')
                    RETURNING source_file_id
                    """,
                    (phase_dataset_id, current_uri, "1" * 64),
                ).fetchone()[0]
                previous_source_id = connection.execute(
                    """
                    INSERT INTO systematic_fx.source_files
                        (dataset_id, source_date, relative_uri, byte_size, sha256,
                         row_count, status)
                    VALUES (%s, DATE '2022-01-03', %s, 1, %s, 1, 'HASHED')
                    RETURNING source_file_id
                    """,
                    (phase_dataset_id, previous_uri, "2" * 64),
                ).fetchone()[0]
                unrelated_source_id = connection.execute(
                    """
                    INSERT INTO systematic_fx.source_files
                        (dataset_id, source_date, relative_uri, byte_size, sha256,
                         row_count, status)
                    VALUES (%s, DATE '2022-01-05', %s, 1, %s, 1, 'HASHED')
                    RETURNING source_file_id
                    """,
                    (phase_dataset_id, f"raw/{suffix}/unrelated.parquet", "3" * 64),
                ).fetchone()[0]
                build_job_id = connection.execute(
                    """
                    INSERT INTO systematic_fx.jobs
                        (job_key, dataset_id, job_type, status)
                    VALUES (%s, %s, 'BUILD_PHASE1A_SCREENING_FEATURES', 'QUEUED')
                    RETURNING job_id
                    """,
                    (f"phase1a-lineage:job:{suffix}", phase_dataset_id),
                ).fetchone()[0]
                partition_metadata = {
                    "provenance": {
                        "research_run_spec_id": run_spec_id,
                        "current_source": {
                            "relative_uri": current_uri,
                            "sha256": "1" * 64,
                            "source_date": "2022-01-04",
                        },
                        "previous_source": {
                            "relative_uri": previous_uri,
                            "sha256": "2" * 64,
                            "source_date": "2022-01-03",
                        },
                    }
                }
                phase_partition_id = connection.execute(
                    """
                    INSERT INTO systematic_fx.derived_partitions
                        (partition_key, dataset_id, partition_type,
                         definition_version, source_date, uri, sha256, row_count,
                         min_event_time_ns, max_event_time_ns,
                         source_manifest_sha256, code_commit, config_sha256,
                         manifest_artifact_id, build_job_id, status, metadata,
                         validated_at)
                    VALUES (%s, %s, 'FEATURES_1S',
                            'phase1a_mbp10_screening_v1', DATE '2022-01-04', %s,
                            %s, 1, 1, 2, %s, 'fixture', %s, %s, %s,
                            'VALIDATED', %s, statement_timestamp())
                    RETURNING derived_partition_id
                    """,
                    (
                        f"phase1a-feature:v1:features_1s:{suffix}",
                        phase_dataset_id,
                        f"derived/phase1a/{suffix}.parquet",
                        "4" * 64,
                        "5" * 64,
                        "6" * 64,
                        artifacts["manifest"],
                        build_job_id,
                        Jsonb(partition_metadata),
                    ),
                ).fetchone()[0]
                connection.execute(
                    """
                    INSERT INTO systematic_fx.derived_partition_sources
                        (derived_partition_id, source_file_id, source_sha256)
                    VALUES (%s, %s, %s), (%s, %s, %s)
                    """,
                    (
                        phase_partition_id,
                        current_source_id,
                        "1" * 64,
                        phase_partition_id,
                        previous_source_id,
                        "2" * 64,
                    ),
                )

                nonphase_partition_id = connection.execute(
                    """
                    INSERT INTO systematic_fx.derived_partitions
                        (partition_key, dataset_id, partition_type,
                         definition_version, source_date, uri, sha256, row_count,
                         source_manifest_sha256, code_commit, config_sha256,
                         status, metadata)
                    VALUES (%s, %s, 'OTHER', 'unrelated-v1', DATE '2022-01-05',
                            %s, %s, 1, %s, 'fixture', %s, 'BUILDING', '{}'::jsonb)
                    RETURNING derived_partition_id
                    """,
                    (
                        f"unrelated-partition:{suffix}",
                        phase_dataset_id,
                        f"derived/unrelated/{suffix}.parquet",
                        "7" * 64,
                        "8" * 64,
                        "9" * 64,
                    ),
                ).fetchone()[0]
                connection.execute(
                    """
                    INSERT INTO systematic_fx.derived_partition_sources
                        (derived_partition_id, source_file_id, source_sha256)
                    VALUES (%s, %s, %s)
                    """,
                    (nonphase_partition_id, unrelated_source_id, "3" * 64),
                )

                savepoint_number = 0

                def assert_rejected(
                    sql: str,
                    parameters: tuple[object, ...],
                    message: str,
                ) -> None:
                    nonlocal savepoint_number
                    savepoint_number += 1
                    savepoint = f"phase1a_guard_{savepoint_number}"
                    connection.execute(f"SAVEPOINT {savepoint}")
                    try:
                        connection.execute(sql, parameters)
                    except psycopg.errors.RaiseException as error:
                        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                        connection.execute(f"RELEASE SAVEPOINT {savepoint}")
                        self.assertIn(message, str(error))
                    else:
                        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                        connection.execute(f"RELEASE SAVEPOINT {savepoint}")
                        self.fail(f"statement unexpectedly succeeded: {sql}")

                assert_rejected(
                    "INSERT INTO systematic_fx.research_run_attempts "
                    "(research_run_spec_id, attempt_number, status, reused_attempt_id, "
                    "finished_at) VALUES (%s, 4, 'SKIPPED_DUPLICATE', %s, "
                    "statement_timestamp())",
                    (run_spec_id, running_attempt_id),
                    "SKIPPED_DUPLICATE must reuse a SUCCEEDED attempt",
                )
                assert_rejected(
                    "INSERT INTO systematic_fx.discovery_exposures "
                    "(exposure_key, campaign_id, exposure_type, source_interval_start, "
                    "source_interval_end, visible_to_ai, research_eligible, query_spec, "
                    "result_summary, result_artifact_id, code_commit, config_sha256, "
                    "research_run_spec_id) VALUES (%s, %s, 'QUERY', "
                    "statement_timestamp(), statement_timestamp(), true, false, "
                    "'{}'::jsonb, '{}'::jsonb, %s, 'fixture', %s, %s)",
                    (
                        f"phase1a-lineage:invalid-exposure:{suffix}",
                        phase_campaign_id,
                        artifacts["candidate"],
                        "2" * 64,
                        run_spec_id,
                    ),
                    "Phase 1A Discovery exposure requires exactly one matching SUCCEEDED attempt",
                )

                for label in (
                    "discovery",
                    "run_result",
                    "trade",
                    "pattern",
                    "manifest",
                    "calendar",
                    "split",
                    "snapshot",
                    "registry",
                    "running",
                ):
                    artifact_id = artifacts[label]
                    assert_rejected(
                        "UPDATE systematic_fx.artifacts "
                        "SET metadata = jsonb_build_object('drift', true) "
                        "WHERE artifact_id = %s",
                        (artifact_id,),
                        "Phase 1A result and lineage artifacts are immutable",
                    )
                    assert_rejected(
                        "DELETE FROM systematic_fx.artifacts WHERE artifact_id = %s",
                        (artifact_id,),
                        "Phase 1A result and lineage artifacts are immutable",
                    )

                assert_rejected(
                    "UPDATE systematic_fx.research_run_attempts "
                    "SET result_artifact_id = %s WHERE research_run_attempt_id = %s",
                    (artifacts["candidate"], running_attempt_id),
                    "Phase 1A run-attempt artifact links are immutable once assigned",
                )
                assert_rejected(
                    "UPDATE systematic_fx.campaigns SET campaign_key = %s WHERE campaign_id = %s",
                    (f"renamed-phase1a-{suffix}", phase_campaign_id),
                    "Phase 1A campaign identity is immutable",
                )
                assert_rejected(
                    "UPDATE systematic_fx.derived_partitions "
                    "SET metadata = '{}'::jsonb WHERE derived_partition_id = %s",
                    (phase_partition_id,),
                    "Phase 1A feature partitions are immutable",
                )
                assert_rejected(
                    "DELETE FROM systematic_fx.derived_partitions WHERE derived_partition_id = %s",
                    (phase_partition_id,),
                    "Phase 1A feature partitions are immutable",
                )
                assert_rejected(
                    "UPDATE systematic_fx.derived_partition_sources "
                    "SET source_sha256 = %s "
                    "WHERE derived_partition_id = %s AND source_file_id = %s",
                    ("f" * 64, phase_partition_id, current_source_id),
                    "Phase 1A feature source links are immutable",
                )
                assert_rejected(
                    "DELETE FROM systematic_fx.derived_partition_sources "
                    "WHERE derived_partition_id = %s AND source_file_id = %s",
                    (phase_partition_id, current_source_id),
                    "Phase 1A feature source links are immutable",
                )
                assert_rejected(
                    "INSERT INTO systematic_fx.derived_partition_sources "
                    "(derived_partition_id, source_file_id, source_sha256) "
                    "VALUES (%s, %s, %s)",
                    (phase_partition_id, unrelated_source_id, "3" * 64),
                    "Phase 1A feature source link differs from partition provenance",
                )
                assert_rejected(
                    "UPDATE systematic_fx.source_files SET relative_uri = %s "
                    "WHERE source_file_id = %s",
                    (f"raw/{suffix}/drifted.parquet", current_source_id),
                    "Phase 1A feature source-file identity is immutable",
                )
                assert_rejected(
                    "DELETE FROM systematic_fx.source_files WHERE source_file_id = %s",
                    (current_source_id,),
                    "Phase 1A feature source files cannot be deleted",
                )

                connection.execute(
                    "UPDATE systematic_fx.source_files "
                    "SET status = 'VALIDATED', validated_at = statement_timestamp() "
                    "WHERE source_file_id = %s",
                    (current_source_id,),
                )
                connection.execute(
                    "UPDATE systematic_fx.artifacts SET sha256 = %s, metadata = %s "
                    "WHERE artifact_id = %s",
                    ("f" * 64, Jsonb({"updated": True}), artifacts["nonphase"]),
                )
                connection.execute(
                    "DELETE FROM systematic_fx.artifacts WHERE artifact_id = %s",
                    (artifacts["nonphase"],),
                )
                connection.execute(
                    "UPDATE systematic_fx.derived_partition_sources "
                    "SET source_sha256 = %s WHERE derived_partition_id = %s",
                    ("e" * 64, nonphase_partition_id),
                )
                connection.execute(
                    "DELETE FROM systematic_fx.derived_partition_sources "
                    "WHERE derived_partition_id = %s",
                    (nonphase_partition_id,),
                )
                connection.execute(
                    "UPDATE systematic_fx.derived_partitions SET metadata = %s "
                    "WHERE derived_partition_id = %s",
                    (Jsonb({"updated": True}), nonphase_partition_id),
                )
                connection.execute(
                    "DELETE FROM systematic_fx.derived_partitions WHERE derived_partition_id = %s",
                    (nonphase_partition_id,),
                )
                connection.execute(
                    "UPDATE systematic_fx.source_files SET relative_uri = %s "
                    "WHERE source_file_id = %s",
                    (f"raw/{suffix}/unrelated-updated.parquet", unrelated_source_id),
                )
                connection.execute(
                    "DELETE FROM systematic_fx.source_files WHERE source_file_id = %s",
                    (unrelated_source_id,),
                )
            finally:
                connection.rollback()


if __name__ == "__main__":
    unittest.main()
