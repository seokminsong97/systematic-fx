from __future__ import annotations

from dataclasses import dataclass

import psycopg
from psycopg.rows import dict_row


@dataclass(frozen=True)
class RefreshRequest:
    scope_key: str
    revision: int
    attempts: int


def claim_refresh(
    connection: psycopg.Connection,
    *,
    scope_key: str,
    worker_id: str,
    lease_seconds: int = 300,
) -> RefreshRequest | None:
    with connection.transaction():
        row = connection.execute(
            """
            WITH candidate AS (
                SELECT scope_key
                FROM systematic_fx.publication_outbox
                WHERE scope_key = %s
                  AND delivered_version < request_version
                  AND (claimed_at IS NULL
                       OR claimed_at < statement_timestamp() - (%s * interval '1 second'))
                FOR UPDATE SKIP LOCKED
            )
            UPDATE systematic_fx.publication_outbox AS outbox
            SET claimed_at = statement_timestamp(),
                claimed_by = %s,
                attempts = attempts + 1
            FROM candidate
            WHERE outbox.scope_key = candidate.scope_key
            RETURNING outbox.scope_key, outbox.request_version, outbox.attempts
            """,
            (scope_key, lease_seconds, worker_id),
        ).fetchone()
    if row is None:
        return None
    return RefreshRequest(
        scope_key=row["scope_key"],
        revision=row["request_version"],
        attempts=row["attempts"],
    )


def acknowledge_refresh(
    connection: psycopg.Connection,
    *,
    request: RefreshRequest,
    worker_id: str,
) -> None:
    with connection.transaction():
        row = connection.execute(
            """
            UPDATE systematic_fx.publication_outbox
            SET delivered_version = GREATEST(delivered_version, %s),
                claimed_at = NULL,
                claimed_by = NULL,
                last_error = NULL
            WHERE scope_key = %s AND claimed_by = %s
            RETURNING scope_key
            """,
            (request.revision, request.scope_key, worker_id),
        ).fetchone()
    if row is None:
        raise RuntimeError("publication refresh lease was lost before acknowledgement")


def fail_refresh(
    connection: psycopg.Connection,
    *,
    request: RefreshRequest,
    worker_id: str,
    error: BaseException,
) -> None:
    message = f"{type(error).__name__}: {error}"[:2000]
    with connection.transaction():
        connection.execute(
            """
            UPDATE systematic_fx.publication_outbox
            SET claimed_at = NULL, claimed_by = NULL, last_error = %s
            WHERE scope_key = %s AND claimed_by = %s
            """,
            (message, request.scope_key, worker_id),
        )


def connect_research(database_url: str) -> psycopg.Connection:
    return psycopg.connect(database_url, row_factory=dict_row)
