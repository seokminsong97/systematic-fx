import subprocess
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from systematic_fx import cli
from systematic_fx.db.bootstrap import (
    DatabaseBootstrapError,
    _validate_connection_targets,
    bootstrap_database,
    bootstrap_test_database,
)
from systematic_fx.db.migrations import MigrationReport

ADMIN_URL = "postgresql://admin:admin%20secret@localhost/postgres"
APPLICATION_URL = "postgresql://research:app%20secret@localhost/systematic_fx"
TEST_URL = "postgresql://research:app%20secret@localhost/systematic_fx_test"
PSQL = "/opt/postgresql/bin/psql"


def _completed(stdout: str = "", *, returncode: int = 0, stderr: str = ""):
    return subprocess.CompletedProcess([], returncode, stdout=stdout, stderr=stderr)


class DatabaseBootstrapTest(unittest.TestCase):
    @patch("systematic_fx.db.bootstrap.apply_migrations")
    @patch("systematic_fx.db.bootstrap._psql_binary", return_value=PSQL)
    @patch("systematic_fx.db.migrations.subprocess.run")
    def test_creates_with_explicit_existing_owner_then_applies_migrations(
        self,
        run: Mock,
        _psql_binary: Mock,
        apply_migrations: Mock,
    ) -> None:
        run.side_effect = [
            _completed(),
            _completed("1\n"),
            _completed(),
            _completed("research_owner\n"),
        ]
        migration_report = MigrationReport(applied=(1,), skipped=())
        apply_migrations.return_value = migration_report
        migrations_directory = Path("/checkout/migrations")

        report = bootstrap_database(
            ADMIN_URL,
            APPLICATION_URL,
            owner_role="research_owner",
            migrations_directory=migrations_directory,
        )

        self.assertTrue(report.created_database)
        self.assertEqual(report.database_name, "systematic_fx")
        self.assertEqual(report.database_owner, "research_owner")
        self.assertEqual(report.migrations, migration_report)
        apply_migrations.assert_called_once_with(
            APPLICATION_URL,
            directory=migrations_directory,
            psql_binary=PSQL,
        )

        create_arguments = run.call_args_list[2].args[0]
        create_command = create_arguments[create_arguments.index("--command") + 1]
        self.assertEqual(
            create_command,
            'CREATE DATABASE "systematic_fx" OWNER "research_owner"',
        )
        owner_lookup_arguments = run.call_args_list[0].args[0]
        owner_lookup_command = owner_lookup_arguments[owner_lookup_arguments.index("--command") + 1]
        self.assertEqual(
            owner_lookup_command,
            "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = 'systematic_fx'",
        )
        role_lookup_arguments = run.call_args_list[1].args[0]
        role_lookup_command = role_lookup_arguments[role_lookup_arguments.index("--command") + 1]
        self.assertEqual(
            role_lookup_command,
            "SELECT 1 FROM pg_roles WHERE rolname = 'research_owner'",
        )
        for lookup_arguments in (owner_lookup_arguments, role_lookup_arguments):
            self.assertFalse(
                any(
                    argument.startswith(("--set=database_name=", "--set=owner_role="))
                    for argument in lookup_arguments
                )
            )
            self.assertNotIn(":'", lookup_arguments[lookup_arguments.index("--command") + 1])
        for invocation in run.call_args_list:
            arguments = invocation.args[0]
            environment = invocation.kwargs["env"]
            self.assertNotIn("admin secret", " ".join(arguments))
            self.assertNotIn("admin%20secret", " ".join(arguments))
            self.assertEqual(environment["PGPASSWORD"], "admin secret")

    @patch("systematic_fx.db.bootstrap.apply_migrations")
    @patch("systematic_fx.db.bootstrap._psql_binary", return_value=PSQL)
    @patch("systematic_fx.db.migrations.subprocess.run")
    def test_omitted_owner_uses_authenticated_admin_role(
        self,
        run: Mock,
        _psql_binary: Mock,
        apply_migrations: Mock,
    ) -> None:
        run.side_effect = [
            _completed(),
            _completed(),
            _completed("admin\n"),
        ]
        apply_migrations.return_value = MigrationReport(applied=(1,), skipped=())

        report = bootstrap_database(ADMIN_URL, APPLICATION_URL)

        self.assertTrue(report.created_database)
        self.assertEqual(report.database_owner, "admin")
        create_arguments = run.call_args_list[1].args[0]
        create_command = create_arguments[create_arguments.index("--command") + 1]
        self.assertEqual(create_command, 'CREATE DATABASE "systematic_fx"')

    @patch("systematic_fx.db.bootstrap.apply_migrations")
    @patch("systematic_fx.db.bootstrap._psql_binary", return_value=PSQL)
    @patch("systematic_fx.db.migrations.subprocess.run")
    def test_existing_database_is_not_created_again(
        self,
        run: Mock,
        _psql_binary: Mock,
        apply_migrations: Mock,
    ) -> None:
        run.return_value = _completed("research_owner\n")
        apply_migrations.return_value = MigrationReport(applied=(), skipped=(1,))

        first = bootstrap_database(
            ADMIN_URL,
            APPLICATION_URL,
            owner_role="research_owner",
        )
        second = bootstrap_database(
            ADMIN_URL,
            APPLICATION_URL,
            owner_role="research_owner",
        )

        self.assertFalse(first.created_database)
        self.assertFalse(second.created_database)
        self.assertEqual(run.call_count, 2)
        self.assertEqual(apply_migrations.call_count, 2)
        for invocation in run.call_args_list:
            command_arguments = invocation.args[0]
            command = command_arguments[command_arguments.index("--command") + 1]
            self.assertNotIn("CREATE DATABASE", command)

    @patch("systematic_fx.db.bootstrap.apply_migrations")
    @patch("systematic_fx.db.bootstrap._psql_binary", return_value=PSQL)
    @patch("systematic_fx.db.migrations.subprocess.run")
    def test_create_race_rechecks_database_and_continues(
        self,
        run: Mock,
        _psql_binary: Mock,
        apply_migrations: Mock,
    ) -> None:
        run.side_effect = [
            _completed(),
            _completed(returncode=1, stderr="duplicate database"),
            _completed("admin\n"),
        ]
        apply_migrations.return_value = MigrationReport(applied=(), skipped=(1,))

        report = bootstrap_database(ADMIN_URL, APPLICATION_URL)

        self.assertFalse(report.created_database)
        self.assertEqual(report.database_owner, "admin")
        apply_migrations.assert_called_once()

    @patch("systematic_fx.db.bootstrap.apply_migrations")
    @patch("systematic_fx.db.bootstrap._psql_binary", return_value=PSQL)
    @patch("systematic_fx.db.migrations.subprocess.run")
    def test_missing_requested_role_stops_before_create_or_migration(
        self,
        run: Mock,
        _psql_binary: Mock,
        apply_migrations: Mock,
    ) -> None:
        run.side_effect = [_completed(), _completed()]

        with self.assertRaisesRegex(DatabaseBootstrapError, "never creates roles"):
            bootstrap_database(
                ADMIN_URL,
                APPLICATION_URL,
                owner_role="missing_owner",
            )

        self.assertEqual(run.call_count, 2)
        apply_migrations.assert_not_called()

    @patch("systematic_fx.db.bootstrap.apply_migrations")
    @patch("systematic_fx.db.bootstrap._psql_binary", return_value=PSQL)
    @patch("systematic_fx.db.migrations.subprocess.run")
    def test_existing_database_owner_mismatch_is_not_mutated(
        self,
        run: Mock,
        _psql_binary: Mock,
        apply_migrations: Mock,
    ) -> None:
        run.return_value = _completed("another_owner\n")

        with self.assertRaisesRegex(DatabaseBootstrapError, "ownership was not changed"):
            bootstrap_database(
                ADMIN_URL,
                APPLICATION_URL,
                owner_role="research_owner",
            )

        apply_migrations.assert_not_called()

    def test_rejects_unsafe_owner_identifiers_without_running_psql(self) -> None:
        unsafe_identifiers = (
            "",
            "9owner",
            "owner-role",
            'owner"; DROP DATABASE postgres; --',
            "a" * 64,
            "연구자",
        )

        with patch("systematic_fx.db.bootstrap._psql_binary") as psql_binary:
            for owner_role in unsafe_identifiers:
                with (
                    self.subTest(owner_role=owner_role),
                    self.assertRaises(DatabaseBootstrapError),
                ):
                    bootstrap_database(
                        ADMIN_URL,
                        APPLICATION_URL,
                        owner_role=owner_role,
                    )
            psql_binary.assert_not_called()

    def test_application_url_must_explicitly_target_research_database(self) -> None:
        invalid_application_urls = (
            "postgresql://research@localhost/postgres",
            "postgresql://research@localhost",
            "host=localhost dbname=systematic_fx user=research",
            "postgresql://research@localhost/systematic_fx?dbname=postgres",
        )

        for application_url in invalid_application_urls:
            with (
                self.subTest(application_url=application_url),
                self.assertRaises(DatabaseBootstrapError),
            ):
                bootstrap_database(ADMIN_URL, application_url)

    def test_admin_url_must_not_depend_on_database_being_created(self) -> None:
        with self.assertRaisesRegex(DatabaseBootstrapError, "maintenance database"):
            bootstrap_database(APPLICATION_URL, APPLICATION_URL)

    @patch("systematic_fx.db.bootstrap.apply_migrations")
    @patch("systematic_fx.db.bootstrap._psql_binary", return_value=PSQL)
    @patch("systematic_fx.db.migrations.subprocess.run")
    def test_test_bootstrap_uses_fixed_isolated_database_name(
        self,
        run: Mock,
        _psql_binary: Mock,
        apply_migrations: Mock,
    ) -> None:
        run.side_effect = [
            _completed(),
            _completed(),
            _completed("admin\n"),
        ]
        apply_migrations.return_value = MigrationReport(applied=(1, 2), skipped=())

        report = bootstrap_test_database(ADMIN_URL, TEST_URL)

        self.assertTrue(report.created_database)
        self.assertEqual(report.database_name, "systematic_fx_test")
        create_arguments = run.call_args_list[1].args[0]
        create_command = create_arguments[create_arguments.index("--command") + 1]
        self.assertEqual(create_command, 'CREATE DATABASE "systematic_fx_test"')
        apply_migrations.assert_called_once_with(
            TEST_URL,
            directory=None,
            psql_binary=PSQL,
        )

    def test_test_bootstrap_rejects_research_database_target(self) -> None:
        with self.assertRaisesRegex(DatabaseBootstrapError, "systematic_fx_test"):
            bootstrap_test_database(ADMIN_URL, APPLICATION_URL)

    def test_internal_bootstrap_target_is_allowlisted(self) -> None:
        with self.assertRaisesRegex(DatabaseBootstrapError, "fixed research/test"):
            _validate_connection_targets(
                admin_database_url=ADMIN_URL,
                application_database_url="postgresql://research@localhost/arbitrary",
                database_name="arbitrary",
            )

    def test_cli_exposes_separate_test_database_bootstrap(self) -> None:
        arguments = cli.build_parser().parse_args(["db", "bootstrap-test"])

        self.assertIs(arguments.handler, cli._bootstrap_test_command)

    def test_cli_test_bootstrap_never_falls_back_to_research_url(self) -> None:
        arguments = cli.build_parser().parse_args(["db", "bootstrap-test"])
        with (
            patch.object(cli.Settings, "from_env"),
            patch.dict(cli.os.environ, {}, clear=True),
            patch("builtins.print"),
        ):
            self.assertEqual(arguments.handler(arguments), 2)


if __name__ == "__main__":
    unittest.main()
