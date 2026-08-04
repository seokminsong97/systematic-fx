"""Deterministic readiness checks for the local research environment."""

from __future__ import annotations

import importlib
import importlib.metadata
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from systematic_fx.config import Settings
from systematic_fx.db.migrations import MigrationError, _prepare_database_target

_DEPENDENCIES = (
    ("numpy", "numpy"),
    ("polars", "polars"),
    ("pyarrow", "pyarrow"),
    ("psycopg", "psycopg"),
    ("scipy", "scipy"),
    ("sklearn", "scikit-learn"),
    ("statsmodels", "statsmodels"),
)
_PSQL_VERSION = re.compile(r"\b(?P<version>[0-9]+(?:\.[0-9]+)+)\b")


@dataclass(frozen=True)
class EnvironmentCheck:
    """One readiness assertion and its machine-readable severity."""

    name: str
    status: str
    required: bool
    detail: str
    version: str | None = None

    def as_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "name": self.name,
            "status": self.status,
            "required": self.required,
            "detail": self.detail,
        }
        if self.version is not None:
            result["version"] = self.version
        return result


@dataclass(frozen=True)
class EnvironmentReport:
    """Complete readiness report for one workstation and project checkout."""

    checks: tuple[EnvironmentCheck, ...]

    @property
    def required_failures(self) -> tuple[EnvironmentCheck, ...]:
        return tuple(check for check in self.checks if check.required and check.status == "fail")

    @property
    def warnings(self) -> tuple[EnvironmentCheck, ...]:
        return tuple(check for check in self.checks if check.status == "warning")

    @property
    def passed(self) -> bool:
        return not self.required_failures

    def as_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "required_failure_count": len(self.required_failures),
            "warning_count": len(self.warnings),
            "checks": [check.as_dict() for check in self.checks],
        }


def _result(
    name: str,
    *,
    passed: bool,
    required: bool,
    detail: str,
    version: str | None = None,
) -> EnvironmentCheck:
    if passed:
        status = "pass"
    else:
        status = "fail" if required else "warning"
    return EnvironmentCheck(
        name=name,
        status=status,
        required=required,
        detail=detail,
        version=version,
    )


def _python_check() -> EnvironmentCheck:
    version = ".".join(str(part) for part in sys.version_info[:3])
    supported = sys.version_info[:2] >= (3, 12)
    return _result(
        "python",
        passed=supported,
        required=True,
        detail=(
            "Python 3.12 or newer is available" if supported else "Python 3.12 or newer is required"
        ),
        version=version,
    )


def _dependency_check(module_name: str, distribution_name: str) -> EnvironmentCheck:
    name = f"dependency:{module_name}"
    try:
        module = importlib.import_module(module_name)
    # A dependency can raise an arbitrary runtime/ABI error while importing; the
    # doctor must report that state instead of crashing.
    except Exception as error:  # noqa: BLE001
        return _result(
            name,
            passed=False,
            required=True,
            detail=f"import failed ({type(error).__name__})",
        )

    try:
        version = importlib.metadata.version(distribution_name)
    except importlib.metadata.PackageNotFoundError:
        module_version = getattr(module, "__version__", None)
        if not isinstance(module_version, str) or not module_version.strip():
            return _result(
                name,
                passed=False,
                required=True,
                detail="import succeeded but package version is unavailable",
            )
        version = module_version.strip()

    return _result(
        name,
        passed=True,
        required=True,
        detail="import and version lookup succeeded",
        version=version,
    )


def _directory_check(name: str, path: Path) -> EnvironmentCheck:
    resolved = path.expanduser().resolve()
    exists = resolved.is_dir()
    return _result(
        name,
        passed=exists,
        required=True,
        detail=str(resolved) if exists else f"directory does not exist: {resolved}",
    )


def _artifacts_check(path: Path) -> EnvironmentCheck:
    resolved = path.expanduser().resolve()
    try:
        resolved.mkdir(parents=True, exist_ok=True)
        if not resolved.is_dir():
            raise NotADirectoryError(resolved)
        with tempfile.NamedTemporaryFile(prefix=".environment-check-", dir=resolved):
            pass
    except OSError as error:
        return _result(
            "artifacts-root",
            passed=False,
            required=True,
            detail=f"not writable: {resolved} ({type(error).__name__})",
        )
    return _result(
        "artifacts-root",
        passed=True,
        required=True,
        detail=f"writable directory: {resolved}",
    )


def _run_command(
    arguments: list[str],
    *,
    environment: dict[str, str] | None = None,
    timeout: float = 10.0,
) -> subprocess.CompletedProcess[str]:
    """Subprocess seam kept small so tests never invoke workstation programs."""

    return subprocess.run(
        arguments,
        check=False,
        capture_output=True,
        text=True,
        env=environment,
        timeout=timeout,
    )


def _find_psql() -> str | None:
    requested = os.environ.get("SYSTEMATIC_FX_PSQL") or "psql"
    return shutil.which(requested)


def _psql_check(psql: str | None) -> EnvironmentCheck:
    if psql is None:
        return _result(
            "psql",
            passed=False,
            required=True,
            detail="psql executable was not found",
        )

    try:
        completed = _run_command([psql, "--version"], timeout=5.0)
    except (OSError, subprocess.TimeoutExpired) as error:
        return _result(
            "psql",
            passed=False,
            required=True,
            detail=f"version check failed ({type(error).__name__})",
        )

    output = (completed.stdout or completed.stderr).strip()
    match = _PSQL_VERSION.search(output)
    if completed.returncode != 0 or match is None:
        return _result(
            "psql",
            passed=False,
            required=True,
            detail=f"version check returned exit code {completed.returncode}",
        )
    return _result(
        "psql",
        passed=True,
        required=True,
        detail="executable and version check succeeded",
        version=match.group("version"),
    )


def _database_check(
    database_url: str | None,
    *,
    psql: str | None,
    required: bool,
) -> EnvironmentCheck:
    if not database_url or not database_url.strip():
        return _result(
            "database-connectivity",
            passed=False,
            required=required,
            detail="database URL is not configured",
        )
    if not required:
        return _result(
            "database-connectivity",
            passed=True,
            required=False,
            detail="configured; connectivity check was not requested",
        )
    if psql is None:
        return _result(
            "database-connectivity",
            passed=False,
            required=True,
            detail="connectivity check requires psql",
        )

    try:
        sanitized_target, embedded_password = _prepare_database_target(database_url)
    except MigrationError:
        return _result(
            "database-connectivity",
            passed=False,
            required=True,
            detail="database URL is invalid or uses an unsupported password form",
        )

    environment = os.environ.copy()
    environment.setdefault("PGAPPNAME", "systematic-fx-environment-check")
    environment.setdefault("PGCONNECT_TIMEOUT", "5")
    if embedded_password is not None:
        environment["PGPASSWORD"] = embedded_password

    arguments = [
        psql,
        "-X",
        "--no-password",
        "--set=ON_ERROR_STOP=1",
        "--dbname",
        sanitized_target,
        "--tuples-only",
        "--no-align",
        "--quiet",
        "--command",
        "SHOW server_version",
    ]
    try:
        completed = _run_command(arguments, environment=environment, timeout=10.0)
    except (OSError, subprocess.TimeoutExpired) as error:
        return _result(
            "database-connectivity",
            passed=False,
            required=True,
            detail=f"connection check failed ({type(error).__name__})",
        )

    if completed.returncode != 0:
        # Do not copy libpq output into the report: it can repeat connection details.
        return _result(
            "database-connectivity",
            passed=False,
            required=True,
            detail=f"connection check returned exit code {completed.returncode}",
        )

    server_version = completed.stdout.strip() or None
    return _result(
        "database-connectivity",
        passed=True,
        required=True,
        detail="connection succeeded",
        version=server_version,
    )


def check_environment(
    settings: Settings,
    *,
    require_database: bool = False,
) -> EnvironmentReport:
    """Check all prerequisites without loading event data or mutating the database."""

    checks: list[EnvironmentCheck] = [_python_check()]
    checks.extend(
        _dependency_check(module_name, distribution_name)
        for module_name, distribution_name in _DEPENDENCIES
    )
    checks.extend(
        (
            _directory_check("data-root", Path(settings.data_root)),
            _directory_check("mbp10-root", Path(settings.mbp10_root)),
            _artifacts_check(Path(settings.artifacts_root)),
        )
    )
    psql = _find_psql()
    checks.append(_psql_check(psql))
    checks.append(
        _database_check(
            settings.database_url,
            psql=psql,
            required=require_database,
        )
    )
    return EnvironmentReport(checks=tuple(checks))


__all__ = ["EnvironmentCheck", "EnvironmentReport", "check_environment"]
