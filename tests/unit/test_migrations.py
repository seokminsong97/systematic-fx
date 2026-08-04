import os
import subprocess
import unittest
from unittest.mock import patch

from systematic_fx.db.migrations import (
    MigrationError,
    _prepare_database_target,
    _run_psql,
)


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


if __name__ == "__main__":
    unittest.main()
