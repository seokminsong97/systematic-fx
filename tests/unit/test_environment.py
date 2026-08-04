import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from systematic_fx.config import Settings
from systematic_fx.environment import EnvironmentCheck, EnvironmentReport, check_environment


class EnvironmentReportTest(unittest.TestCase):
    def test_report_classifies_failures_and_warnings(self) -> None:
        report = EnvironmentReport(
            checks=(
                EnvironmentCheck("ok", "pass", True, "ready"),
                EnvironmentCheck("required", "fail", True, "missing"),
                EnvironmentCheck("optional", "warning", False, "not configured"),
            )
        )

        self.assertFalse(report.passed)
        self.assertEqual([item.name for item in report.required_failures], ["required"])
        self.assertEqual([item.name for item in report.warnings], ["optional"])
        self.assertEqual(report.as_dict()["required_failure_count"], 1)
        self.assertEqual(report.as_dict()["warning_count"], 1)


class EnvironmentCheckTest(unittest.TestCase):
    def _settings(self, root: Path, database_url: str | None = None) -> Settings:
        data_root = root / "data"
        (data_root / "mbp-10").mkdir(parents=True)
        return Settings(
            data_root=data_root,
            artifacts_root=root / "artifacts",
            database_url=database_url,
        )

    @patch("systematic_fx.environment._dependency_check")
    @patch("systematic_fx.environment._python_check")
    @patch("systematic_fx.environment._run_command")
    @patch("systematic_fx.environment._find_psql", return_value="/usr/bin/psql")
    def test_happy_path_creates_and_checks_artifacts_directory(
        self,
        find_psql: Mock,
        run_command: Mock,
        python_check: Mock,
        dependency_check: Mock,
    ) -> None:
        del find_psql
        python_check.return_value = EnvironmentCheck("python", "pass", True, "ok", "3.12.8")
        dependency_check.side_effect = lambda module, distribution: EnvironmentCheck(
            f"dependency:{module}", "pass", True, distribution, "1.0"
        )
        run_command.return_value = subprocess.CompletedProcess(
            [], 0, stdout="psql (PostgreSQL) 18.4\n", stderr=""
        )

        with tempfile.TemporaryDirectory() as directory:
            settings = self._settings(Path(directory))
            report = check_environment(settings)

            self.assertTrue(report.passed)
            self.assertTrue(settings.artifacts_root.is_dir())
            self.assertEqual(report.warnings[0].name, "database-connectivity")
            self.assertEqual(report.warnings[0].detail, "database URL is not configured")

    @patch("systematic_fx.environment.importlib.import_module", return_value=SimpleNamespace())
    @patch("systematic_fx.environment.importlib.metadata.version", return_value="9.8.7")
    def test_dependency_records_imported_distribution_version(
        self, package_version: Mock, import_module: Mock
    ) -> None:
        from systematic_fx.environment import _dependency_check

        result = _dependency_check("sklearn", "scikit-learn")

        self.assertEqual(result.status, "pass")
        self.assertEqual(result.version, "9.8.7")
        import_module.assert_called_once_with("sklearn")
        package_version.assert_called_once_with("scikit-learn")

    @patch("systematic_fx.environment.importlib.import_module", side_effect=ImportError("missing"))
    def test_dependency_import_failure_is_required(self, import_module: Mock) -> None:
        del import_module
        from systematic_fx.environment import _dependency_check

        result = _dependency_check("numpy", "numpy")

        self.assertEqual(result.status, "fail")
        self.assertTrue(result.required)
        self.assertNotIn("missing", result.detail)

    @patch("systematic_fx.environment._dependency_check")
    @patch("systematic_fx.environment._python_check")
    @patch("systematic_fx.environment._run_command")
    @patch("systematic_fx.environment._find_psql", return_value="/usr/bin/psql")
    def test_database_password_and_original_url_never_appear_in_report_or_argv(
        self,
        find_psql: Mock,
        run_command: Mock,
        python_check: Mock,
        dependency_check: Mock,
    ) -> None:
        del find_psql
        python_check.return_value = EnvironmentCheck("python", "pass", True, "ok", "3.12.8")
        dependency_check.side_effect = lambda module, distribution: EnvironmentCheck(
            f"dependency:{module}", "pass", True, distribution, "1.0"
        )
        run_command.side_effect = (
            subprocess.CompletedProcess([], 0, stdout="psql (PostgreSQL) 18.4\n", stderr=""),
            subprocess.CompletedProcess([], 0, stdout="18.4\n", stderr=""),
        )
        secret = "secret value"
        database_url = "postgresql://research:secret%20value@localhost:5432/systematic_fx"

        with tempfile.TemporaryDirectory() as directory:
            settings = self._settings(Path(directory), database_url=database_url)
            report = check_environment(settings, require_database=True)

        self.assertTrue(report.passed)
        serialized = json.dumps(report.as_dict())
        self.assertNotIn(secret, serialized)
        self.assertNotIn(database_url, serialized)

        database_call = run_command.call_args_list[1]
        arguments = database_call.args[0]
        environment = database_call.kwargs["environment"]
        self.assertNotIn(secret, " ".join(arguments))
        self.assertNotIn("secret%20value", " ".join(arguments))
        self.assertEqual(environment["PGPASSWORD"], secret)

    @patch("systematic_fx.environment._dependency_check")
    @patch("systematic_fx.environment._python_check")
    @patch("systematic_fx.environment._run_command")
    @patch("systematic_fx.environment._find_psql", return_value="/usr/bin/psql")
    def test_database_error_does_not_copy_libpq_stderr(
        self,
        find_psql: Mock,
        run_command: Mock,
        python_check: Mock,
        dependency_check: Mock,
    ) -> None:
        del find_psql
        python_check.return_value = EnvironmentCheck("python", "pass", True, "ok", "3.12.8")
        dependency_check.side_effect = lambda module, distribution: EnvironmentCheck(
            f"dependency:{module}", "pass", True, distribution, "1.0"
        )
        secret = "never-print-this"
        run_command.side_effect = (
            subprocess.CompletedProcess([], 0, stdout="psql (PostgreSQL) 18.4\n", stderr=""),
            subprocess.CompletedProcess([], 2, stdout="", stderr=f"connection failed: {secret}"),
        )

        with tempfile.TemporaryDirectory() as directory:
            report = check_environment(
                self._settings(
                    Path(directory),
                    database_url=f"postgresql://research:{secret}@localhost/systematic_fx",
                ),
                require_database=True,
            )

        serialized = json.dumps(report.as_dict())
        self.assertFalse(report.passed)
        self.assertNotIn(secret, serialized)
        self.assertEqual(report.required_failures[-1].name, "database-connectivity")


if __name__ == "__main__":
    unittest.main()
