"""Provision the isolated public projection database and least-privilege roles."""

from __future__ import annotations

import os
import re
import secrets
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

import psycopg
from dotenv import dotenv_values
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

from systematic_fx.publication.public_store import bootstrap_public_database

PUBLIC_DATABASE_NAME = "systematic_fx_public"
PUBLIC_OWNER_ROLE = "systematic_fx_public_owner"
PUBLIC_WRITER_ROLE = "systematic_fx_public_writer"
PUBLIC_READER_ROLE = "systematic_fx_public_reader"
PUBLIC_SCHEMA = "systematic_fx_public"
_ENV_KEY = re.compile(r"^[A-Z][A-Z0-9_]*$")
_POSTGRESQL_SCHEMES = frozenset({"postgres", "postgresql"})


class PublicProvisionError(RuntimeError):
    """The public database or its local runtime configuration could not be prepared."""


@dataclass(frozen=True)
class PublicProvisionReport:
    database_created: bool
    owner_created: bool
    writer_created: bool
    reader_created: bool
    writer_url: str = field(repr=False)
    reader_url: str = field(repr=False)


def _database_target(database_url: str, database_name: str) -> str:
    parameters = conninfo_to_dict(database_url)
    parameters["dbname"] = database_name
    return make_conninfo(**parameters)


def _application_url(
    admin_database_url: str,
    *,
    database_name: str,
    role: str,
    password: str,
) -> str:
    parsed = urlsplit(admin_database_url)
    if parsed.scheme.lower() not in _POSTGRESQL_SCHEMES:
        raise PublicProvisionError("admin database URL must use postgres:// or postgresql://")
    if parsed.fragment:
        raise PublicProvisionError("admin database URL must not contain a fragment")

    host = parsed.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"
    userinfo = f"{quote(role, safe='')}:{quote(password, safe='')}@"
    query = urlencode(
        [
            (key, value)
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if key.lower() not in {"dbname", "password", "user"}
        ],
        doseq=True,
    )
    return urlunsplit(
        (
            parsed.scheme,
            f"{userinfo}{host}",
            f"/{quote(database_name, safe='')}",
            query,
            "",
        )
    )


def _ensure_role(
    connection: psycopg.Connection,
    *,
    role: str,
    login: bool,
    password: str | None = None,
) -> bool:
    exists = connection.execute(
        "SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = %s",
        (role,),
    ).fetchone()
    action = sql.SQL("ALTER ROLE") if exists else sql.SQL("CREATE ROLE")
    attributes = (
        sql.SQL(
            "LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION "
            "NOBYPASSRLS CONNECTION LIMIT 20 PASSWORD {}"
        ).format(sql.Literal(password))
        if login
        else sql.SQL(
            "NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
        )
    )
    connection.execute(sql.SQL("{} {} WITH {}").format(action, sql.Identifier(role), attributes))
    return exists is None


def _ensure_database(connection: psycopg.Connection) -> bool:
    row = connection.execute(
        "SELECT pg_get_userbyid(datdba) AS owner FROM pg_catalog.pg_database WHERE datname = %s",
        (PUBLIC_DATABASE_NAME,),
    ).fetchone()
    if row is not None:
        if row["owner"] != PUBLIC_OWNER_ROLE:
            raise PublicProvisionError(
                "existing systematic_fx_public database has an unexpected owner; refusing takeover"
            )
        return False
    connection.execute(
        sql.SQL("CREATE DATABASE {} OWNER {}").format(
            sql.Identifier(PUBLIC_DATABASE_NAME),
            sql.Identifier(PUBLIC_OWNER_ROLE),
        )
    )
    return True


def _apply_least_privilege_grants(admin_public_url: str) -> None:
    with psycopg.connect(admin_public_url, autocommit=True) as connection:
        connection.execute(
            sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM PUBLIC").format(
                sql.Identifier(PUBLIC_DATABASE_NAME)
            )
        )
        for role in (PUBLIC_OWNER_ROLE, PUBLIC_WRITER_ROLE, PUBLIC_READER_ROLE):
            connection.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    sql.Identifier(PUBLIC_DATABASE_NAME),
                    sql.Identifier(role),
                )
            )
        connection.execute("REVOKE ALL ON SCHEMA public FROM PUBLIC")
        connection.execute(
            sql.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC").format(sql.Identifier(PUBLIC_SCHEMA))
        )
        for role in (PUBLIC_WRITER_ROLE, PUBLIC_READER_ROLE):
            connection.execute(
                sql.SQL("REVOKE ALL ON SCHEMA {} FROM {}").format(
                    sql.Identifier(PUBLIC_SCHEMA),
                    sql.Identifier(role),
                )
            )
            connection.execute(
                sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                    sql.Identifier(PUBLIC_SCHEMA),
                    sql.Identifier(role),
                )
            )
            connection.execute(
                sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA {} FROM {}").format(
                    sql.Identifier(PUBLIC_SCHEMA),
                    sql.Identifier(role),
                )
            )
            connection.execute(
                sql.SQL("REVOKE ALL ON ALL SEQUENCES IN SCHEMA {} FROM {}").format(
                    sql.Identifier(PUBLIC_SCHEMA),
                    sql.Identifier(role),
                )
            )
        connection.execute(
            sql.SQL("GRANT SELECT, INSERT ON TABLE {}.{} TO {}").format(
                sql.Identifier(PUBLIC_SCHEMA),
                sql.Identifier("research_publications"),
                sql.Identifier(PUBLIC_WRITER_ROLE),
            )
        )
        connection.execute(
            sql.SQL("GRANT USAGE, SELECT ON SEQUENCE {}.{} TO {}").format(
                sql.Identifier(PUBLIC_SCHEMA),
                sql.Identifier("research_publications_publication_id_seq"),
                sql.Identifier(PUBLIC_WRITER_ROLE),
            )
        )
        connection.execute(
            sql.SQL("GRANT SELECT ON TABLE {}.{} TO {}").format(
                sql.Identifier(PUBLIC_SCHEMA),
                sql.Identifier("current_research_publications"),
                sql.Identifier(PUBLIC_READER_ROLE),
            )
        )
        connection.execute(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {} REVOKE ALL ON TABLES FROM PUBLIC"
            ).format(
                sql.Identifier(PUBLIC_OWNER_ROLE),
                sql.Identifier(PUBLIC_SCHEMA),
            )
        )


def provision_public_database(
    *,
    admin_database_url: str,
    public_migration_path: Path,
) -> PublicProvisionReport:
    writer_password = secrets.token_urlsafe(36)
    reader_password = secrets.token_urlsafe(36)
    with psycopg.connect(
        admin_database_url,
        autocommit=True,
        row_factory=dict_row,
    ) as connection:
        owner_created = _ensure_role(
            connection,
            role=PUBLIC_OWNER_ROLE,
            login=False,
        )
        writer_created = _ensure_role(
            connection,
            role=PUBLIC_WRITER_ROLE,
            login=True,
            password=writer_password,
        )
        reader_created = _ensure_role(
            connection,
            role=PUBLIC_READER_ROLE,
            login=True,
            password=reader_password,
        )
        database_created = _ensure_database(connection)

    admin_public_url = _database_target(admin_database_url, PUBLIC_DATABASE_NAME)
    bootstrap_public_database(
        admin_public_url,
        public_migration_path,
        owner_role=PUBLIC_OWNER_ROLE,
    )
    _apply_least_privilege_grants(admin_public_url)
    return PublicProvisionReport(
        database_created=database_created,
        owner_created=owner_created,
        writer_created=writer_created,
        reader_created=reader_created,
        writer_url=_application_url(
            admin_database_url,
            database_name=PUBLIC_DATABASE_NAME,
            role=PUBLIC_WRITER_ROLE,
            password=writer_password,
        ),
        reader_url=_application_url(
            admin_database_url,
            database_name=PUBLIC_DATABASE_NAME,
            role=PUBLIC_READER_ROLE,
            password=reader_password,
        ),
    )


def _upsert_env_values(text: str, updates: dict[str, str]) -> str:
    if any(not _ENV_KEY.fullmatch(key) for key in updates):
        raise ValueError("environment keys must be uppercase identifiers")
    remaining = dict(updates)
    output: list[str] = []
    for line in text.splitlines():
        key, separator, _ = line.partition("=")
        if separator and key in updates:
            if key in remaining:
                output.append(f"{key}={remaining.pop(key)}")
            continue
        output.append(line)
    if remaining:
        if output and output[-1]:
            output.append("")
        output.extend(f"{key}={value}" for key, value in remaining.items())
    return "\n".join(output).rstrip() + "\n"


def _atomic_private_write(path: Path, text: str) -> None:
    target = path.expanduser().resolve(strict=False)
    if target.is_symlink():
        raise PublicProvisionError(f"refusing to replace symlinked environment file: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=target.parent,
            prefix=f".{target.name}.",
            delete=False,
        ) as handle:
            temporary_name = handle.name
            handle.write(text)
        os.chmod(temporary_name, 0o600)
        os.replace(temporary_name, target)
    finally:
        if temporary_name and os.path.exists(temporary_name):
            os.unlink(temporary_name)


def write_public_runtime_environment(
    *,
    source_env_path: Path,
    runtime_env_path: Path,
    web_env_path: Path,
    report: PublicProvisionReport,
) -> None:
    source = source_env_path.expanduser().resolve()
    source_values = dotenv_values(source)
    if not source_values.get("SYSTEMATIC_FX_DATABASE_URL"):
        raise PublicProvisionError("source environment is missing SYSTEMATIC_FX_DATABASE_URL")
    runtime_text = _upsert_env_values(
        source.read_text(encoding="utf-8"),
        {"SYSTEMATIC_FX_PUBLIC_DATABASE_URL": report.writer_url},
    )
    _atomic_private_write(runtime_env_path, runtime_text)

    existing_web = web_env_path.read_text(encoding="utf-8") if web_env_path.is_file() else ""
    web_text = _upsert_env_values(
        existing_web,
        {
            "SITE_DATABASE_URL": report.reader_url,
            "SITE_CAMPAIGN_KEY": "phase1a_conservative_screening_v1",
            "NEXT_PUBLIC_REFRESH_INTERVAL_MS": "15000",
        },
    )
    _atomic_private_write(web_env_path, web_text)
