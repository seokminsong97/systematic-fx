import os
import shutil
import subprocess
import unittest
import uuid

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
    "phase1a_outcome_cell_summaries",
    "phase1a_outcome_replay_checkpoints",
    "phase1a_outcome_replay_equivalence_audits",
    "phase1a_outcome_replay_manifests",
    "phase1a_outcome_screening_decisions",
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
    "discovery_exposures_campaign_run_spec_fk",
    "discovery_exposures_interval_order",
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
        for version in range(1, 21):
            self.assertIn(version, report.skipped)

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

    def test_live_publication_progress_triggers_exist(self) -> None:
        result = self._run_sql(
            "SELECT t.tgname FROM pg_catalog.pg_trigger t "
            "JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid "
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'systematic_fx' AND NOT t.tgisinternal "
            "ORDER BY t.tgname"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        triggers = set(result.stdout.splitlines())
        self.assertTrue(
            {
                "research_run_specs_publication_refresh",
                "research_run_attempts_publication_refresh",
                "phase1a_outcome_checkpoints_publication_refresh",
            }
            <= triggers
        )


if __name__ == "__main__":
    unittest.main()
