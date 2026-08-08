from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.publication.contract import payload_sha256


def bootstrap_public_database(
    database_url: str,
    migration_path: Path,
    *,
    owner_role: str | None = None,
) -> None:
    sql_text = migration_path.read_text(encoding="utf-8")
    checksum = hashlib.sha256(sql_text.encode()).hexdigest()
    with (
        psycopg.connect(database_url, row_factory=dict_row) as connection,
        connection.transaction(),
    ):
        if owner_role is not None:
            connection.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(owner_role)))
        connection.execute("CREATE SCHEMA IF NOT EXISTS systematic_fx_public")
        migration_table_exists = connection.execute(
            "SELECT to_regclass('systematic_fx_public.schema_migrations') IS NOT NULL AS exists"
        ).fetchone()["exists"]
        existing = None
        if migration_table_exists:
            existing = connection.execute(
                "SELECT checksum FROM systematic_fx_public.schema_migrations WHERE version = 1"
            ).fetchone()
        if existing is not None:
            if existing["checksum"] != checksum:
                raise RuntimeError("public migration 1 checksum drift detected")
            return
        if migration_table_exists:
            raise RuntimeError("public schema has an unrecorded migration state")
        connection.execute(sql_text)
        connection.execute(
            """
            INSERT INTO systematic_fx_public.schema_migrations (version, name, checksum)
            VALUES (1, 'public_projection', %s)
            """,
            (checksum,),
        )


def publish_snapshot(
    connection: psycopg.Connection,
    *,
    campaign_key: str,
    revision: int,
    payload: dict[str, Any],
) -> str:
    digest = payload_sha256(payload)
    metadata = payload["metadata"]
    with connection.transaction():
        inserted = connection.execute(
            """
            INSERT INTO systematic_fx_public.research_publications (
                campaign_key, revision, schema_version, payload, payload_sha256,
                source_commit, generated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (campaign_key, revision) DO NOTHING
            RETURNING publication_id
            """,
            (
                campaign_key,
                revision,
                metadata["schemaVersion"],
                Jsonb(payload),
                digest,
                metadata["sourceRevision"],
                metadata["publishedAt"],
            ),
        ).fetchone()
        if inserted is None:
            existing = connection.execute(
                """
                SELECT payload_sha256
                FROM systematic_fx_public.research_publications
                WHERE campaign_key = %s AND revision = %s
                """,
                (campaign_key, revision),
            ).fetchone()
            if existing is None or existing["payload_sha256"] != digest:
                raise RuntimeError("public revision collision with different payload")
    return digest


def connect_public(database_url: str) -> psycopg.Connection:
    return psycopg.connect(database_url, row_factory=dict_row)
