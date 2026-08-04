"""Create the research database safely and apply its checked-in migrations."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlsplit

from systematic_fx.db.migrations import (
    MigrationError,
    MigrationReport,
    _prepare_database_target,
    _psql_binary,
    _run_psql,
    apply_migrations,
)

DATABASE_NAME = "systematic_fx"
TEST_DATABASE_NAME = "systematic_fx_test"
_ALLOWED_DATABASE_NAMES = frozenset({DATABASE_NAME, TEST_DATABASE_NAME})
_POSTGRESQL_URL_SCHEMES = frozenset(("postgres", "postgresql"))
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


class DatabaseBootstrapError(MigrationError):
    """The research database could not be validated, created, or prepared."""


@dataclass(frozen=True)
class BootstrapReport:
    """Result of creating (if necessary) and migrating the research database."""

    database_name: str
    database_owner: str
    created_database: bool
    migrations: MigrationReport


def _validate_identifier(identifier: str, *, label: str) -> str:
    """Accept a deliberately narrow, unquoted PostgreSQL identifier subset."""

    if not _SAFE_IDENTIFIER.fullmatch(identifier):
        raise DatabaseBootstrapError(
            f"{label} must be 1-63 ASCII letters, digits, or underscores "
            "and must start with a letter or underscore"
        )
    return identifier


def _quote_identifier(identifier: str, *, label: str) -> str:
    validated = _validate_identifier(identifier, label=label)
    return f'"{validated}"'


def _quote_string_literal(identifier: str, *, label: str) -> str:
    """Quote a strictly validated identifier for equality checks in ``--command`` SQL."""

    validated = _validate_identifier(identifier, label=label)
    return "'" + validated.replace("'", "''") + "'"


def _url_database_name(database_url: str, *, label: str) -> str | None:
    """Return an explicit URI database name, or ``None`` for keyword conninfo."""

    if not database_url.strip():
        raise DatabaseBootstrapError(f"{label} must not be empty")

    try:
        sanitized_target, _ = _prepare_database_target(database_url)
        parsed = urlsplit(sanitized_target)
    except (MigrationError, ValueError) as error:
        raise DatabaseBootstrapError(f"{label} is not a valid PostgreSQL target") from error

    if parsed.scheme.lower() not in _POSTGRESQL_URL_SCHEMES:
        return None

    try:
        query_parameters = parse_qsl(
            parsed.query,
            keep_blank_values=True,
            strict_parsing=False,
            errors="strict",
        )
    except (UnicodeDecodeError, ValueError) as error:
        raise DatabaseBootstrapError(f"{label} has an invalid query string") from error
    if any(key.lower() == "dbname" for key, _ in query_parameters):
        raise DatabaseBootstrapError(
            f"{label} must select its database in the URL path, not a dbname query parameter"
        )

    if not parsed.path or parsed.path == "/":
        return ""
    if not parsed.path.startswith("/"):
        raise DatabaseBootstrapError(f"{label} has an invalid database path")
    return unquote(parsed.path[1:])


def _validate_connection_targets(
    *,
    admin_database_url: str,
    application_database_url: str,
    database_name: str = DATABASE_NAME,
) -> None:
    _validate_identifier(database_name, label="database_name")
    if database_name not in _ALLOWED_DATABASE_NAMES:
        raise DatabaseBootstrapError(
            "database_name must be one of the fixed research/test database names"
        )
    admin_database_name = _url_database_name(
        admin_database_url,
        label="admin_database_url",
    )
    application_database_name = _url_database_name(
        application_database_url,
        label="application_database_url",
    )

    if application_database_name is None:
        raise DatabaseBootstrapError(
            "application_database_url must be a postgres:// or postgresql:// URL "
            f"with /{database_name} as its database path"
        )
    if application_database_name != database_name:
        raise DatabaseBootstrapError(
            f"application_database_url must target database {database_name!r}"
        )
    if admin_database_name == database_name:
        raise DatabaseBootstrapError(
            "admin_database_url must target an existing maintenance database, "
            f"not {database_name!r}"
        )


def _database_owner(
    *,
    psql: str,
    admin_database_url: str,
    database_name: str,
) -> str | None:
    database_literal = _quote_string_literal(database_name, label="database name")
    owner = _run_psql(
        psql=psql,
        database_url=admin_database_url,
        command=(
            f"SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = {database_literal}"
        ),
    )
    return owner or None


def _role_exists(*, psql: str, admin_database_url: str, owner_role: str) -> bool:
    owner_literal = _quote_string_literal(owner_role, label="owner_role")
    result = _run_psql(
        psql=psql,
        database_url=admin_database_url,
        command=f"SELECT 1 FROM pg_roles WHERE rolname = {owner_literal}",
    )
    return result == "1"


def _create_database(
    *,
    psql: str,
    admin_database_url: str,
    database_name: str,
    owner_role: str | None,
) -> None:
    command = f"CREATE DATABASE {_quote_identifier(database_name, label='database name')}"
    if owner_role is not None:
        command += f" OWNER {_quote_identifier(owner_role, label='owner_role')}"
    _run_psql(
        psql=psql,
        database_url=admin_database_url,
        command=command,
    )


def _bootstrap_database(
    admin_database_url: str,
    application_database_url: str,
    *,
    database_name: str,
    owner_role: str | None = None,
    migrations_directory: Path | None = None,
    psql_binary: str | None = None,
) -> BootstrapReport:
    """Create one explicitly named database, then apply checksum-verified migrations.

    ``admin_database_url`` must connect to an already existing maintenance database.
    ``application_database_url`` must explicitly target ``database_name`` and is used only
    after the database exists. Passwords embedded in either URL are delegated to the
    migration runner's environment-only password handling and never enter argv.

    When ``owner_role`` is omitted, PostgreSQL assigns ownership to the authenticated
    admin role running ``CREATE DATABASE``. The function never creates or alters roles.
    """

    _validate_connection_targets(
        admin_database_url=admin_database_url,
        application_database_url=application_database_url,
        database_name=database_name,
    )
    if owner_role is not None:
        _validate_identifier(owner_role, label="owner_role")

    psql = _psql_binary(psql_binary)
    database_owner = _database_owner(
        psql=psql,
        admin_database_url=admin_database_url,
        database_name=database_name,
    )
    created_database = False

    if database_owner is None:
        if owner_role is not None and not _role_exists(
            psql=psql,
            admin_database_url=admin_database_url,
            owner_role=owner_role,
        ):
            raise DatabaseBootstrapError(
                f"owner role {owner_role!r} does not exist; bootstrap never creates roles"
            )

        try:
            _create_database(
                psql=psql,
                admin_database_url=admin_database_url,
                database_name=database_name,
                owner_role=owner_role,
            )
            created_database = True
        except MigrationError as error:
            # Another bootstrap process may have created the database after our check.
            database_owner = _database_owner(
                psql=psql,
                admin_database_url=admin_database_url,
                database_name=database_name,
            )
            if database_owner is None:
                raise DatabaseBootstrapError(
                    f"failed to create database {database_name!r}"
                ) from error

        if database_owner is None:
            database_owner = _database_owner(
                psql=psql,
                admin_database_url=admin_database_url,
                database_name=database_name,
            )
        if database_owner is None:
            raise DatabaseBootstrapError(
                f"database {database_name!r} was not visible after creation"
            )

    if owner_role is not None and database_owner != owner_role:
        raise DatabaseBootstrapError(
            f"database {database_name!r} is owned by {database_owner!r}, "
            f"not requested owner {owner_role!r}; ownership was not changed"
        )

    migration_report = apply_migrations(
        application_database_url,
        directory=migrations_directory,
        psql_binary=psql,
    )
    return BootstrapReport(
        database_name=database_name,
        database_owner=database_owner,
        created_database=created_database,
        migrations=migration_report,
    )


def bootstrap_database(
    admin_database_url: str,
    application_database_url: str,
    *,
    owner_role: str | None = None,
    migrations_directory: Path | None = None,
    psql_binary: str | None = None,
) -> BootstrapReport:
    """Create ``systematic_fx`` if absent, then apply checked migrations."""

    return _bootstrap_database(
        admin_database_url,
        application_database_url,
        database_name=DATABASE_NAME,
        owner_role=owner_role,
        migrations_directory=migrations_directory,
        psql_binary=psql_binary,
    )


def bootstrap_test_database(
    admin_database_url: str,
    test_database_url: str,
    *,
    owner_role: str | None = None,
    migrations_directory: Path | None = None,
    psql_binary: str | None = None,
) -> BootstrapReport:
    """Create isolated ``systematic_fx_test`` and apply checked migrations.

    The fixed name prevents a test bootstrap from being redirected to the research
    control database or to an arbitrary PostgreSQL database.
    """

    return _bootstrap_database(
        admin_database_url,
        test_database_url,
        database_name=TEST_DATABASE_NAME,
        owner_role=owner_role,
        migrations_directory=migrations_directory,
        psql_binary=psql_binary,
    )
