"""Checksum-verified PostgreSQL migration runner using the ``psql`` client."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit, urlunsplit

_MIGRATION_NAME = re.compile(r"^(?P<version>[0-9]{4})_(?P<name>[a-z0-9_]+)\.sql$")
_PASSWORD_KEYWORD = re.compile(r"(?:^|\s)password\s*=", re.IGNORECASE)
_POSTGRESQL_URL_SCHEMES = frozenset(("postgres", "postgresql"))


class MigrationError(RuntimeError):
    """A migration could not be discovered, verified, or applied."""


class MigrationDriftError(MigrationError):
    """An applied migration no longer matches the checked-in SQL file."""


@dataclass(frozen=True)
class Migration:
    """One immutable, numerically ordered SQL migration."""

    version: int
    name: str
    path: Path
    checksum: str


@dataclass(frozen=True)
class MigrationReport:
    """Versions applied and skipped during one migration run."""

    applied: tuple[int, ...]
    skipped: tuple[int, ...]


def default_migrations_directory() -> Path:
    """Resolve migrations for a source checkout or an explicit deployment path."""

    configured = os.environ.get("SYSTEMATIC_FX_MIGRATIONS_ROOT")
    if configured:
        return Path(configured).expanduser().resolve()

    checkout_candidate = Path(__file__).resolve().parents[3] / "migrations"
    if checkout_candidate.is_dir():
        return checkout_candidate

    working_directory_candidate = Path.cwd() / "migrations"
    if working_directory_candidate.is_dir():
        return working_directory_candidate.resolve()

    raise MigrationError("cannot find migrations directory; set SYSTEMATIC_FX_MIGRATIONS_ROOT")


def discover_migrations(directory: Path | None = None) -> tuple[Migration, ...]:
    """Return valid migration files in version order and reject ambiguous history."""

    migration_directory = (directory or default_migrations_directory()).resolve()
    if not migration_directory.is_dir():
        raise MigrationError(f"migrations directory does not exist: {migration_directory}")

    migrations: list[Migration] = []
    for path in sorted(migration_directory.glob("*.sql")):
        match = _MIGRATION_NAME.fullmatch(path.name)
        if match is None:
            raise MigrationError(
                f"invalid migration filename {path.name!r}; expected NNNN_name.sql"
            )
        migrations.append(
            Migration(
                version=int(match.group("version")),
                name=match.group("name"),
                path=path,
                checksum=hashlib.sha256(path.read_bytes()).hexdigest(),
            )
        )

    if not migrations:
        raise MigrationError(f"no SQL migrations found in {migration_directory}")

    versions = [migration.version for migration in migrations]
    if len(versions) != len(set(versions)):
        raise MigrationError("duplicate migration versions are not allowed")
    if versions != list(range(1, len(versions) + 1)):
        raise MigrationError(f"migration versions must be contiguous from 0001; found {versions}")
    return tuple(migrations)


def _psql_binary(requested: str | None = None) -> str:
    candidate = requested or os.environ.get("SYSTEMATIC_FX_PSQL") or "psql"
    resolved = shutil.which(candidate)
    if resolved is None:
        raise MigrationError(f"psql executable not found: {candidate}")
    return resolved


def _prepare_database_target(database_target: str) -> tuple[str, str | None]:
    """Remove a PostgreSQL URL password from argv and return it separately."""

    normalized_target = database_target.strip()
    try:
        parsed = urlsplit(normalized_target)
    except ValueError as error:
        raise MigrationError("invalid PostgreSQL connection target") from error

    if parsed.scheme.lower() in _POSTGRESQL_URL_SCHEMES:
        if parsed.fragment:
            raise MigrationError("PostgreSQL connection URLs must not contain fragments")
        userinfo, separator, hostinfo = parsed.netloc.rpartition("@")
        password_sources: list[str] = []
        sanitized_netloc = parsed.netloc
        if separator and ":" in userinfo:
            encoded_username, encoded_password = userinfo.split(":", maxsplit=1)
            password_sources.append(unquote(encoded_password))
            sanitized_netloc = f"{encoded_username}@{hostinfo}" if encoded_username else hostinfo

        sanitized_query_parts: list[str] = []
        for query_part in parsed.query.split("&") if parsed.query else ():
            encoded_key, equals, encoded_value = query_part.partition("=")
            if unquote(encoded_key).lower() == "password":
                password_sources.append(unquote(encoded_value) if equals else "")
            else:
                sanitized_query_parts.append(query_part)

        if len(password_sources) > 1:
            raise MigrationError("PostgreSQL URL contains more than one password source")
        password = password_sources[0] if password_sources else None
        if password is not None and "\x00" in password:
            raise MigrationError("PostgreSQL URL password contains an invalid null byte")

        sanitized_query = "&".join(sanitized_query_parts)
        if not parsed.netloc and normalized_target.lower().startswith(
            f"{parsed.scheme.lower()}://"
        ):
            # ``urlunsplit`` collapses ``postgresql:///database`` to the invalid
            # ``postgresql:/database`` when netloc is empty.
            sanitized_target = f"{parsed.scheme}://{parsed.path}"
            if sanitized_query:
                sanitized_target = f"{sanitized_target}?{sanitized_query}"
        else:
            sanitized_target = urlunsplit(
                parsed._replace(
                    netloc=sanitized_netloc,
                    query=sanitized_query,
                )
            )
        return sanitized_target, password

    if "://" in normalized_target:
        raise MigrationError("only postgres:// and postgresql:// URLs are supported")
    if _PASSWORD_KEYWORD.search(normalized_target):
        raise MigrationError(
            "password-bearing keyword conninfo is not supported; use a PostgreSQL URL "
            "or the PGPASSWORD environment variable"
        )
    return normalized_target, None


def _run_psql(
    *,
    psql: str,
    database_url: str,
    command: str | None = None,
    file: Path | None = None,
    variables: Mapping[str, str] | None = None,
) -> str:
    if (command is None) == (file is None):
        raise ValueError("provide exactly one of command or file")

    argv_database_target, embedded_password = _prepare_database_target(database_url)

    arguments = [
        psql,
        "-X",
        "--no-password",
        "--set=ON_ERROR_STOP=1",
        "--dbname",
        argv_database_target,
    ]
    for name, value in sorted((variables or {}).items()):
        arguments.append(f"--set={name}={value}")
    if command is not None:
        arguments.extend(("--tuples-only", "--no-align", "--quiet", "--command", command))
    else:
        arguments.extend(("--file", str(file)))

    environment = os.environ.copy()
    environment.setdefault("PGAPPNAME", "systematic-fx-migrations")
    if embedded_password is not None:
        environment["PGPASSWORD"] = embedded_password
    completed = subprocess.run(
        arguments,
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "unknown psql error"
        raise MigrationError(detail)
    return completed.stdout.strip()


def _load_applied_migrations(*, psql: str, database_url: str) -> dict[int, tuple[str, str]]:
    relation = _run_psql(
        psql=psql,
        database_url=database_url,
        command="SELECT to_regclass('systematic_fx.schema_migrations') IS NOT NULL",
    )
    if relation != "t":
        return {}

    rows = _run_psql(
        psql=psql,
        database_url=database_url,
        command=(
            "SELECT version::text || '|' || name || '|' || checksum "
            "FROM systematic_fx.schema_migrations ORDER BY version"
        ),
    )
    applied: dict[int, tuple[str, str]] = {}
    for row in rows.splitlines():
        if not row:
            continue
        version_text, name, checksum = row.split("|", maxsplit=2)
        applied[int(version_text)] = (name, checksum)
    return applied


def apply_migrations(
    database_url: str,
    *,
    directory: Path | None = None,
    psql_binary: str | None = None,
) -> MigrationReport:
    """Apply pending migrations and reject missing or modified migration history."""

    if not database_url.strip():
        raise MigrationError("database URL must not be empty")

    migrations = discover_migrations(directory)
    psql = _psql_binary(psql_binary)
    applied_records = _load_applied_migrations(psql=psql, database_url=database_url)
    local_by_version = {migration.version: migration for migration in migrations}

    unknown_versions = sorted(set(applied_records) - set(local_by_version))
    if unknown_versions:
        raise MigrationDriftError(
            f"database contains migration versions absent from this checkout: {unknown_versions}"
        )

    applied_now: list[int] = []
    skipped: list[int] = []
    for migration in migrations:
        applied = applied_records.get(migration.version)
        if applied is not None:
            applied_name, applied_checksum = applied
            if applied_name != migration.name or applied_checksum != migration.checksum:
                raise MigrationDriftError(
                    f"migration {migration.version:04d} differs from applied history"
                )
            skipped.append(migration.version)
            continue

        _run_psql(
            psql=psql,
            database_url=database_url,
            file=migration.path,
            variables={"migration_checksum": migration.checksum},
        )
        applied_now.append(migration.version)

    final_records = _load_applied_migrations(psql=psql, database_url=database_url)
    for migration in migrations:
        if final_records.get(migration.version) != (migration.name, migration.checksum):
            raise MigrationError(
                f"migration {migration.version:04d} was not recorded exactly after application"
            )

    return MigrationReport(applied=tuple(applied_now), skipped=tuple(skipped))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=os.environ.get("SYSTEMATIC_FX_DATABASE_URL"),
        help="PostgreSQL URL; defaults to SYSTEMATIC_FX_DATABASE_URL",
    )
    parser.add_argument(
        "--migrations-dir",
        type=Path,
        help="migration directory; defaults to the checkout migrations/ directory",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for automation and local administration."""

    arguments = _parser().parse_args(argv)
    if arguments.database_url is None:
        raise SystemExit(
            "database URL is required via --database-url or SYSTEMATIC_FX_DATABASE_URL"
        )
    report = apply_migrations(arguments.database_url, directory=arguments.migrations_dir)
    print(f"migrations complete: applied={list(report.applied)} skipped={list(report.skipped)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
