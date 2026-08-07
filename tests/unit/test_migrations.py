import os
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

from systematic_fx.db.migrations import (
    MigrationError,
    _prepare_database_target,
    _run_psql,
    discover_migrations,
)

ROOT = Path(__file__).resolve().parents[2]


class DatabaseTargetSecurityTest(unittest.TestCase):
    def test_userinfo_password_is_removed_from_argv_and_percent_decoded(self) -> None:
        target = "postgresql://research:s3cr%40t%2Fvalue@[::1]:5432/systematic_fx"

        sanitized, password = _prepare_database_target(target)

        self.assertEqual(
            sanitized,
            "postgresql://research@[::1]:5432/systematic_fx",
        )
        self.assertEqual(password, "s3cr@t/value")

    def test_query_password_is_removed_without_rewriting_other_parameters(self) -> None:
        target = (
            "postgres://research@db.example/systematic_fx?"
            "sslmode=require&password=space%20and%2Bplus&application_name=research"
        )

        sanitized, password = _prepare_database_target(target)

        self.assertEqual(
            sanitized,
            "postgres://research@db.example/systematic_fx?"
            "sslmode=require&application_name=research",
        )
        self.assertEqual(password, "space and+plus")

    def test_empty_netloc_unix_socket_url_preserves_triple_slash(self) -> None:
        target = "postgresql:///postgres?host=%2Ftmp%2Fsystematic-fx-postgres&port=55432"

        sanitized, password = _prepare_database_target(target)

        self.assertEqual(sanitized, target)
        self.assertIsNone(password)

    def test_empty_netloc_url_strips_query_password_without_collapsing_slashes(self) -> None:
        target = (
            "postgresql:///postgres?host=%2Ftmp%2Fsystematic-fx-postgres&"
            "password=socket%20secret&port=55432"
        )

        sanitized, password = _prepare_database_target(target)

        self.assertEqual(
            sanitized,
            "postgresql:///postgres?host=%2Ftmp%2Fsystematic-fx-postgres&port=55432",
        )
        self.assertEqual(password, "socket secret")

    def test_keyword_conninfo_with_password_is_rejected_without_echoing_secret(self) -> None:
        secret = "do-not-expose"

        with self.assertRaises(MigrationError) as raised:
            _prepare_database_target(
                f"host=localhost dbname=systematic_fx user=research password = '{secret}'"
            )

        self.assertNotIn(secret, str(raised.exception))

    def test_more_than_one_url_password_source_is_rejected(self) -> None:
        with self.assertRaisesRegex(MigrationError, "more than one password source"):
            _prepare_database_target("postgresql://research:userinfo@localhost/db?password=query")

    def test_leading_whitespace_cannot_bypass_url_password_removal(self) -> None:
        sanitized, password = _prepare_database_target(
            "  postgresql://research:hidden@localhost/systematic_fx  "
        )

        self.assertEqual(
            sanitized,
            "postgresql://research@localhost/systematic_fx",
        )
        self.assertEqual(password, "hidden")

    @patch("systematic_fx.db.migrations.subprocess.run")
    def test_run_psql_passes_password_only_in_environment(self, run: unittest.mock.Mock) -> None:
        run.return_value = subprocess.CompletedProcess([], 0, stdout="1\n", stderr="")
        secret = "decoded password"
        target = "postgresql://research:decoded%20password@localhost/systematic_fx"

        with patch.dict(os.environ, {"PGPASSWORD": "stale-password"}):
            output = _run_psql(
                psql="/usr/bin/psql",
                database_url=target,
                command="SELECT 1",
            )

        self.assertEqual(output, "1")
        arguments = run.call_args.args[0]
        environment = run.call_args.kwargs["env"]
        self.assertNotIn("decoded%20password", " ".join(arguments))
        self.assertNotIn(secret, " ".join(arguments))
        self.assertIn(
            "postgresql://research@localhost/systematic_fx",
            arguments,
        )
        self.assertEqual(environment["PGPASSWORD"], secret)


class RepositoryMigrationContractTest(unittest.TestCase):
    def test_phase1a_artifact_lineage_guard_covers_every_owner(self) -> None:
        migrations = discover_migrations(ROOT / "migrations")
        migration = {item.version: item for item in migrations}[9]

        self.assertEqual(migration.name, "phase1a_artifact_lineage_integrity")
        sql = migration.path.read_text(encoding="utf-8")
        for trigger in (
            "artifacts_protect_phase1a_lineage",
            "campaigns_protect_phase1a_identity",
            "research_run_attempts_protect_phase1a_artifact_links",
            "derived_partitions_protect_phase1a_lineage",
            "derived_partition_sources_protect_phase1a_lineage",
            "source_files_protect_phase1a_lineage",
        ):
            self.assertIn(f"CREATE TRIGGER {trigger}", sql)
        for owner in (
            "discovery_exposures",
            "research_run_attempts",
            "pattern_ledger",
            "PHASE1A_FEATURE_BUILD_MANIFEST",
        ):
            self.assertIn(owner, sql)

        artifact_guard = sql.split(
            "CREATE FUNCTION systematic_fx.reject_phase1a_artifact_mutation()",
            maxsplit=1,
        )[1].split("CREATE TRIGGER artifacts_protect_phase1a_lineage", maxsplit=1)[0]
        self.assertIn("OLD.artifact_id", artifact_guard)
        self.assertIn("IF TG_OP = 'DELETE'", artifact_guard)
        self.assertIn("RETURN OLD", artifact_guard)

    def test_execution_atomicity_guards_active_runs_and_ai_visibility(self) -> None:
        migrations = {item.version: item for item in discover_migrations(ROOT / "migrations")}
        migration = migrations[10]
        self.assertEqual(migration.name, "research_execution_atomicity")
        sql = migration.path.read_text(encoding="utf-8")
        self.assertIn("CREATE UNIQUE INDEX research_run_attempts_one_active", sql)
        self.assertIn("WHERE status IN ('QUEUED', 'RUNNING')", sql)
        self.assertIn(
            "CREATE TRIGGER research_run_attempts_require_duplicate_success",
            sql,
        )
        self.assertIn(
            "CREATE TRIGGER discovery_exposures_require_phase1a_success",
            sql,
        )
        self.assertIn("status = 'SUCCEEDED'", sql)
        self.assertIn("result_artifact_id = NEW.result_artifact_id", sql)

    def test_phase1a_control_and_provenance_artifacts_are_immutable(self) -> None:
        migrations = {item.version: item for item in discover_migrations(ROOT / "migrations")}
        migration = migrations[11]
        self.assertEqual(migration.name, "phase1a_control_artifact_immutability")
        sql = migration.path.read_text(encoding="utf-8")
        self.assertIn(
            "CREATE OR REPLACE FUNCTION systematic_fx.phase1a_artifact_is_protected",
            sql,
        )
        for artifact_type in (
            "PHASE1A_ELIGIBLE_CALENDAR",
            "PHASE1A_CAMPAIGN_SPLIT",
            "PHASE1A_CODE_SNAPSHOT",
            "PHASE1A_SCREENING_REGISTRY",
        ):
            self.assertIn(artifact_type, sql)
        self.assertIn("experiment.registration_artifact_id", sql)
        self.assertIn("phase1a_conservative_screening_v1", sql)


if __name__ == "__main__":
    unittest.main()
