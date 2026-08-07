"""Serializable, drift-rejecting persistence for governed research runs."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from functools import wraps
from typing import Any, Literal, ParamSpec, TypeVar

import psycopg
from psycopg import IsolationLevel
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.db.postgres_retry import retry_serialization_failures
from systematic_fx.research.run_spec import (
    RUN_SPEC_SCHEMA,
    RUN_SPEC_SCHEMA_VERSION,
    RunSpec,
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_TERMINAL_STATUSES = frozenset({"SUCCEEDED", "REJECTED", "FAILED", "CANCELLED"})
_MAX_ATTEMPT_NUMBER = 2**31 - 1
_P = ParamSpec("_P")
_R = TypeVar("_R")

TerminalStatus = Literal["SUCCEEDED", "REJECTED", "FAILED", "CANCELLED"]


class RunRegistryError(RuntimeError):
    """A research run could not be registered or transitioned safely."""


class RunRegistryDriftError(RunRegistryError):
    """A deterministic identity already exists with different immutable content."""


class RunRegistryStateError(RunRegistryError):
    """A run attempt was asked to make an invalid state transition."""


class RunRegistryDatabaseError(RunRegistryError):
    """PostgreSQL rejected or could not complete a registry operation."""


def _translate_psycopg_errors(
    operation: str,
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    """Expose one stable error API while preserving the driver exception as cause."""

    def decorate(function: Callable[_P, _R]) -> Callable[_P, _R]:
        @wraps(function)
        def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            try:
                return retry_serialization_failures(function, *args, **kwargs)
            except RunRegistryError:
                raise
            except psycopg.Error as error:
                raise RunRegistryDatabaseError(f"PostgreSQL {operation} failed") from error

        return wrapped

    return decorate


@dataclass(frozen=True, slots=True)
class RunSpecRegistration:
    """Resolved database identity for one immutable :class:`RunSpec`."""

    research_run_spec_id: int
    run_fingerprint: str
    campaign_id: int
    experiment_id: int | None
    parent_run_spec_id: int | None
    created: bool


@dataclass(frozen=True, slots=True)
class RunAttemptReservation:
    """A queued execution or an append-preserved duplicate skip."""

    research_run_attempt_id: int
    research_run_spec_id: int
    attempt_number: int
    status: Literal["QUEUED", "SKIPPED_DUPLICATE"]
    execute: bool
    reused_attempt_id: int | None


@dataclass(frozen=True, slots=True)
class RunAttemptState:
    """State returned after a validated attempt transition."""

    research_run_attempt_id: int
    research_run_spec_id: int
    attempt_number: int
    status: str
    result_artifact_id: int | None
    trade_ledger_artifact_id: int | None


def _nonempty(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RunRegistryError(f"{label} must be a non-empty string")
    if value != value.strip():
        raise RunRegistryError(f"{label} must not have leading or trailing whitespace")
    return value


def _sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise RunRegistryError(f"{label} must be a lowercase SHA-256")
    return value


def _positive_identifier(value: object, *, label: str, optional: bool = False) -> int | None:
    if optional and value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise RunRegistryError(f"{label} must be a positive integer")
    return value


def _database_url(value: object) -> str:
    return _nonempty(value, label="database_url")


def _row_or_error(row: dict[str, Any] | None, *, label: str) -> dict[str, Any]:
    if row is None:
        raise RunRegistryError(f"{label} does not exist")
    return row


def _assert_fields(
    *,
    label: str,
    row: Mapping[str, Any],
    expected: Mapping[str, object],
) -> None:
    mismatches = [key for key, value in expected.items() if row.get(key) != value]
    if mismatches:
        raise RunRegistryDriftError(
            f"{label} immutable content drift in fields: {', '.join(sorted(mismatches))}"
        )


def _plain_canonical_spec(run_spec: RunSpec) -> dict[str, object]:
    """Decode the already validated canonical bytes into JSONB-compatible values."""

    value = json.loads(run_spec.canonical_json())
    if not isinstance(value, dict):  # defensive: RunSpec always emits an object
        raise RunRegistryError("RunSpec canonical JSON must be an object")
    return value


def _assert_canonical_parent_identity(
    canonical_spec: Mapping[str, object],
    *,
    parent_run_fingerprint: str | None,
) -> None:
    """Require every database parent edge to be part of the child fingerprint."""

    parameters = canonical_spec.get("parameters")
    if not isinstance(parameters, Mapping):  # RunSpec already enforces an object
        raise RunRegistryError("RunSpec parameters must be a canonical object")
    recorded_parent = parameters.get("parent_run_fingerprint")
    if parent_run_fingerprint is None:
        if recorded_parent is not None:
            raise RunRegistryError(
                "RunSpec records parent_run_fingerprint but no database parent was requested"
            )
        return
    fingerprint = _sha256(parent_run_fingerprint, label="parent_run_fingerprint")
    if recorded_parent != fingerprint:
        raise RunRegistryError(
            "RunSpec parameters.parent_run_fingerprint must equal the database parent edge"
        )


def _set_serializable(connection: psycopg.Connection[dict[str, Any]]) -> None:
    connection.isolation_level = IsolationLevel.SERIALIZABLE


def _resolve_campaign_and_experiment(
    connection: psycopg.Connection[dict[str, Any]],
    run_spec: RunSpec,
) -> tuple[int, int | None]:
    campaign = connection.execute(
        """
        SELECT campaign_id, campaign_key
        FROM systematic_fx.campaigns
        WHERE campaign_key = %s
        FOR SHARE
        """,
        (run_spec.campaign_id,),
    ).fetchone()
    campaign = _row_or_error(campaign, label=f"campaign {run_spec.campaign_id}")
    campaign_id = int(campaign["campaign_id"])

    if run_spec.experiment_id is None:
        return campaign_id, None

    experiment = connection.execute(
        """
        SELECT experiment_id, experiment_key, campaign_id
        FROM systematic_fx.experiments
        WHERE experiment_key = %s AND campaign_id = %s
        FOR SHARE
        """,
        (run_spec.experiment_id, campaign_id),
    ).fetchone()
    experiment = _row_or_error(
        experiment,
        label=f"experiment {run_spec.experiment_id} in campaign {run_spec.campaign_id}",
    )
    return campaign_id, int(experiment["experiment_id"])


def _resolve_parent_run_spec(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    campaign_id: int,
    parent_run_fingerprint: str | None,
) -> int | None:
    if parent_run_fingerprint is None:
        return None
    fingerprint = _sha256(parent_run_fingerprint, label="parent_run_fingerprint")
    row = connection.execute(
        """
        SELECT research_run_spec_id, campaign_id
        FROM systematic_fx.research_run_specs
        WHERE run_fingerprint = %s
        FOR SHARE
        """,
        (fingerprint,),
    ).fetchone()
    row = _row_or_error(row, label=f"parent run specification {fingerprint}")
    if int(row["campaign_id"]) != campaign_id:
        raise RunRegistryError("parent run specification belongs to a different campaign")
    return int(row["research_run_spec_id"])


@_translate_psycopg_errors("run-spec registration")
def register_run_spec(
    database_url: str,
    run_spec: RunSpec,
    *,
    parent_run_fingerprint: str | None = None,
) -> RunSpecRegistration:
    """Atomically register a canonical run or verify an exact existing fingerprint."""

    database_url = _database_url(database_url)
    if not isinstance(run_spec, RunSpec):
        raise RunRegistryError("run_spec must be a RunSpec")

    canonical_spec = _plain_canonical_spec(run_spec)
    _assert_canonical_parent_identity(
        canonical_spec,
        parent_run_fingerprint=parent_run_fingerprint,
    )
    fingerprint = run_spec.fingerprint
    source_manifest_hashes = dict(run_spec.source_manifest_hashes)
    deterministic_seed = Decimal(run_spec.random_seed)

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with connection.transaction():
            campaign_id, experiment_id = _resolve_campaign_and_experiment(connection, run_spec)
            parent_run_spec_id = _resolve_parent_run_spec(
                connection,
                campaign_id=campaign_id,
                parent_run_fingerprint=parent_run_fingerprint,
            )
            inserted = connection.execute(
                """
                INSERT INTO systematic_fx.research_run_specs
                    (run_fingerprint, canonicalization_schema, canonicalization_version,
                     campaign_id, experiment_id, parent_run_spec_id, run_kind,
                     engine_version, canonical_spec, source_manifest_hashes,
                     eligible_calendar_version, eligible_calendar_sha256,
                     split_version, split_sha256, feature_version, feature_sha256,
                     outcome_version, outcome_sha256, cost_version, cost_sha256,
                     execution_version, execution_sha256, code_commit,
                     code_snapshot_sha256, dependency_lock_sha256,
                     deterministic_seed, direction)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_fingerprint) DO NOTHING
                RETURNING research_run_spec_id
                """,
                (
                    fingerprint,
                    RUN_SPEC_SCHEMA,
                    RUN_SPEC_SCHEMA_VERSION,
                    campaign_id,
                    experiment_id,
                    parent_run_spec_id,
                    run_spec.run_kind,
                    run_spec.engine_version,
                    Jsonb(canonical_spec),
                    Jsonb(source_manifest_hashes),
                    run_spec.eligible_calendar_version,
                    run_spec.eligible_calendar_sha256,
                    run_spec.split_version,
                    run_spec.split_sha256,
                    run_spec.feature_version,
                    run_spec.feature_sha256,
                    run_spec.outcome_version,
                    run_spec.outcome_sha256,
                    run_spec.cost_version,
                    run_spec.cost_sha256,
                    run_spec.execution_version,
                    run_spec.execution_sha256,
                    run_spec.code_commit,
                    run_spec.code_snapshot_sha256,
                    run_spec.dependency_lock_sha256,
                    deterministic_seed,
                    run_spec.direction,
                ),
            ).fetchone()
            created = inserted is not None
            row = connection.execute(
                """
                SELECT research_run_spec_id, run_fingerprint, canonicalization_schema,
                       canonicalization_version, campaign_id, experiment_id,
                       parent_run_spec_id, run_kind, engine_version, canonical_spec,
                       source_manifest_hashes, eligible_calendar_version,
                       eligible_calendar_sha256, split_version, split_sha256,
                       feature_version, feature_sha256, outcome_version, outcome_sha256,
                       cost_version, cost_sha256, execution_version, execution_sha256,
                       code_commit, code_snapshot_sha256, dependency_lock_sha256,
                       deterministic_seed, direction
                FROM systematic_fx.research_run_specs
                WHERE run_fingerprint = %s
                FOR SHARE
                """,
                (fingerprint,),
            ).fetchone()
            row = _row_or_error(row, label=f"run specification {fingerprint}")
            _assert_fields(
                label=f"run specification {fingerprint}",
                row=row,
                expected={
                    "run_fingerprint": fingerprint,
                    "canonicalization_schema": RUN_SPEC_SCHEMA,
                    "canonicalization_version": RUN_SPEC_SCHEMA_VERSION,
                    "campaign_id": campaign_id,
                    "experiment_id": experiment_id,
                    "parent_run_spec_id": parent_run_spec_id,
                    "run_kind": run_spec.run_kind,
                    "engine_version": run_spec.engine_version,
                    "canonical_spec": canonical_spec,
                    "source_manifest_hashes": source_manifest_hashes,
                    "eligible_calendar_version": run_spec.eligible_calendar_version,
                    "eligible_calendar_sha256": run_spec.eligible_calendar_sha256,
                    "split_version": run_spec.split_version,
                    "split_sha256": run_spec.split_sha256,
                    "feature_version": run_spec.feature_version,
                    "feature_sha256": run_spec.feature_sha256,
                    "outcome_version": run_spec.outcome_version,
                    "outcome_sha256": run_spec.outcome_sha256,
                    "cost_version": run_spec.cost_version,
                    "cost_sha256": run_spec.cost_sha256,
                    "execution_version": run_spec.execution_version,
                    "execution_sha256": run_spec.execution_sha256,
                    "code_commit": run_spec.code_commit,
                    "code_snapshot_sha256": run_spec.code_snapshot_sha256,
                    "dependency_lock_sha256": run_spec.dependency_lock_sha256,
                    "deterministic_seed": deterministic_seed,
                    "direction": run_spec.direction,
                },
            )

    return RunSpecRegistration(
        research_run_spec_id=int(row["research_run_spec_id"]),
        run_fingerprint=fingerprint,
        campaign_id=campaign_id,
        experiment_id=experiment_id,
        parent_run_spec_id=parent_run_spec_id,
        created=created,
    )


def _attempt_number(
    connection: psycopg.Connection[dict[str, Any]],
    research_run_spec_id: int,
) -> int:
    row = connection.execute(
        """
        SELECT COALESCE(max(attempt_number), 0)::integer AS last_attempt_number
        FROM systematic_fx.research_run_attempts
        WHERE research_run_spec_id = %s
        """,
        (research_run_spec_id,),
    ).fetchone()
    row = _row_or_error(row, label="attempt-number query")
    last = int(row["last_attempt_number"])
    if last >= _MAX_ATTEMPT_NUMBER:
        raise RunRegistryError("research run attempt number is exhausted")
    return last + 1


@_translate_psycopg_errors("run-attempt reservation")
def reserve_run_attempt(
    database_url: str,
    *,
    run_fingerprint: str,
    job_id: int | None = None,
) -> RunAttemptReservation:
    """Append one QUEUED attempt only when no active or successful attempt exists."""

    database_url = _database_url(database_url)
    fingerprint = _sha256(run_fingerprint, label="run_fingerprint")
    job_id = _positive_identifier(job_id, label="job_id", optional=True)

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with connection.transaction():
            spec_row = connection.execute(
                """
                SELECT research_run_spec_id, run_fingerprint
                FROM systematic_fx.research_run_specs
                WHERE run_fingerprint = %s
                FOR UPDATE
                """,
                (fingerprint,),
            ).fetchone()
            spec_row = _row_or_error(spec_row, label=f"run specification {fingerprint}")
            research_run_spec_id = int(spec_row["research_run_spec_id"])

            active = connection.execute(
                """
                SELECT research_run_attempt_id, attempt_number, status
                FROM systematic_fx.research_run_attempts
                WHERE research_run_spec_id = %s AND status IN ('QUEUED', 'RUNNING')
                ORDER BY attempt_number
                LIMIT 1
                FOR UPDATE
                """,
                (research_run_spec_id,),
            ).fetchone()
            if active is not None:
                raise RunRegistryStateError(
                    "run specification "
                    f"{fingerprint} already has active attempt "
                    f"{active['research_run_attempt_id']} ({active['status']}); "
                    "terminalize that attempt before retrying"
                )

            succeeded = connection.execute(
                """
                SELECT research_run_attempt_id, attempt_number, result_artifact_id
                FROM systematic_fx.research_run_attempts
                WHERE research_run_spec_id = %s AND status = 'SUCCEEDED'
                ORDER BY attempt_number
                LIMIT 1
                FOR SHARE
                """,
                (research_run_spec_id,),
            ).fetchone()
            attempt_number = _attempt_number(connection, research_run_spec_id)

            if succeeded is not None:
                reused_attempt_id = int(succeeded["research_run_attempt_id"])
                result_summary = {
                    "reason": "EXACT_FINGERPRINT_ALREADY_SUCCEEDED",
                    "reused_attempt_id": reused_attempt_id,
                }
                row = connection.execute(
                    """
                    INSERT INTO systematic_fx.research_run_attempts
                        (research_run_spec_id, attempt_number, status, job_id,
                         reused_attempt_id, result_summary, finished_at)
                    VALUES (%s, %s, 'SKIPPED_DUPLICATE', %s, %s, %s,
                            statement_timestamp())
                    RETURNING research_run_attempt_id, research_run_spec_id,
                              attempt_number, status, reused_attempt_id
                    """,
                    (
                        research_run_spec_id,
                        attempt_number,
                        job_id,
                        reused_attempt_id,
                        Jsonb(result_summary),
                    ),
                ).fetchone()
                execute = False
            else:
                row = connection.execute(
                    """
                    INSERT INTO systematic_fx.research_run_attempts
                        (research_run_spec_id, attempt_number, status, job_id)
                    VALUES (%s, %s, 'QUEUED', %s)
                    RETURNING research_run_attempt_id, research_run_spec_id,
                              attempt_number, status, reused_attempt_id
                    """,
                    (research_run_spec_id, attempt_number, job_id),
                ).fetchone()
                reused_attempt_id = None
                execute = True
            row = _row_or_error(row, label="reserved research run attempt")
            expected_status: Literal["QUEUED", "SKIPPED_DUPLICATE"] = (
                "QUEUED" if execute else "SKIPPED_DUPLICATE"
            )
            _assert_fields(
                label=f"research run attempt {row['research_run_attempt_id']}",
                row=row,
                expected={
                    "research_run_spec_id": research_run_spec_id,
                    "attempt_number": attempt_number,
                    "status": expected_status,
                    "reused_attempt_id": reused_attempt_id,
                },
            )

    return RunAttemptReservation(
        research_run_attempt_id=int(row["research_run_attempt_id"]),
        research_run_spec_id=research_run_spec_id,
        attempt_number=attempt_number,
        status=expected_status,
        execute=execute,
        reused_attempt_id=reused_attempt_id,
    )


def _locked_attempt(
    connection: psycopg.Connection[dict[str, Any]],
    research_run_attempt_id: int,
) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT research_run_attempt_id, research_run_spec_id, attempt_number, status,
               result_artifact_id, trade_ledger_artifact_id, started_at, finished_at
        FROM systematic_fx.research_run_attempts
        WHERE research_run_attempt_id = %s
        FOR UPDATE
        """,
        (research_run_attempt_id,),
    ).fetchone()
    return _row_or_error(row, label=f"research run attempt {research_run_attempt_id}")


def _attempt_state(row: Mapping[str, Any]) -> RunAttemptState:
    return RunAttemptState(
        research_run_attempt_id=int(row["research_run_attempt_id"]),
        research_run_spec_id=int(row["research_run_spec_id"]),
        attempt_number=int(row["attempt_number"]),
        status=str(row["status"]),
        result_artifact_id=(
            int(row["result_artifact_id"]) if row.get("result_artifact_id") is not None else None
        ),
        trade_ledger_artifact_id=(
            int(row["trade_ledger_artifact_id"])
            if row.get("trade_ledger_artifact_id") is not None
            else None
        ),
    )


@_translate_psycopg_errors("run-attempt start")
def start_run_attempt(
    database_url: str,
    *,
    research_run_attempt_id: int,
) -> RunAttemptState:
    """Transition exactly one QUEUED attempt to RUNNING."""

    database_url = _database_url(database_url)
    attempt_id = _positive_identifier(
        research_run_attempt_id,
        label="research_run_attempt_id",
    )
    assert attempt_id is not None

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with connection.transaction():
            current = _locked_attempt(connection, attempt_id)
            if current["status"] != "QUEUED":
                raise RunRegistryStateError(
                    f"attempt {attempt_id} cannot transition {current['status']} -> RUNNING"
                )
            row = connection.execute(
                """
                UPDATE systematic_fx.research_run_attempts
                SET status = 'RUNNING', started_at = statement_timestamp()
                WHERE research_run_attempt_id = %s AND status = 'QUEUED'
                RETURNING research_run_attempt_id, research_run_spec_id, attempt_number,
                          status, result_artifact_id, trade_ledger_artifact_id,
                          started_at, finished_at
                """,
                (attempt_id,),
            ).fetchone()
            if row is None:
                raise RunRegistryStateError(
                    f"attempt {attempt_id} lost its QUEUED state before transition"
                )
            if row["status"] != "RUNNING" or row["started_at"] is None:
                raise RunRegistryDriftError(f"attempt {attempt_id} has invalid RUNNING state")

    return _attempt_state(row)


@_translate_psycopg_errors("run-attempt finish")
def finish_run_attempt(
    database_url: str,
    *,
    research_run_attempt_id: int,
    status: TerminalStatus,
    result_summary: Mapping[str, object] | None = None,
    result_artifact_id: int | None = None,
    trade_ledger_artifact_id: int | None = None,
    error_message: str | None = None,
) -> RunAttemptState:
    """Transition one RUNNING attempt to an allowed immutable terminal state."""

    database_url = _database_url(database_url)
    attempt_id = _positive_identifier(
        research_run_attempt_id,
        label="research_run_attempt_id",
    )
    assert attempt_id is not None
    if not isinstance(status, str) or status not in _TERMINAL_STATUSES:
        raise RunRegistryStateError(f"terminal status must be one of {sorted(_TERMINAL_STATUSES)}")
    if result_summary is None:
        summary: dict[str, object] = {}
    elif isinstance(result_summary, Mapping):
        summary = dict(result_summary)
    else:
        raise RunRegistryError("result_summary must be a mapping")
    result_artifact_id = _positive_identifier(
        result_artifact_id,
        label="result_artifact_id",
        optional=True,
    )
    trade_ledger_artifact_id = _positive_identifier(
        trade_ledger_artifact_id,
        label="trade_ledger_artifact_id",
        optional=True,
    )
    if status == "SUCCEEDED" and result_artifact_id is None:
        raise RunRegistryStateError("SUCCEEDED requires result_artifact_id")
    if status == "FAILED":
        if not isinstance(error_message, str) or not error_message.strip():
            raise RunRegistryStateError("FAILED requires a non-empty error_message")
        error_message = _nonempty(error_message, label="error_message")
    elif error_message is not None:
        error_message = _nonempty(error_message, label="error_message")

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with connection.transaction():
            current = _locked_attempt(connection, attempt_id)
            if current["status"] != "RUNNING":
                raise RunRegistryStateError(
                    f"attempt {attempt_id} cannot transition {current['status']} -> {status}"
                )
            if current["started_at"] is None:
                raise RunRegistryDriftError(f"attempt {attempt_id} is RUNNING without started_at")
            row = connection.execute(
                """
                UPDATE systematic_fx.research_run_attempts
                SET status = %s,
                    result_artifact_id = %s,
                    trade_ledger_artifact_id = %s,
                    result_summary = %s,
                    error_message = %s,
                    finished_at = statement_timestamp()
                WHERE research_run_attempt_id = %s AND status = 'RUNNING'
                RETURNING research_run_attempt_id, research_run_spec_id, attempt_number,
                          status, result_artifact_id, trade_ledger_artifact_id,
                          started_at, finished_at
                """,
                (
                    status,
                    result_artifact_id,
                    trade_ledger_artifact_id,
                    Jsonb(summary),
                    error_message,
                    attempt_id,
                ),
            ).fetchone()
            if row is None:
                raise RunRegistryStateError(
                    f"attempt {attempt_id} lost its RUNNING state before transition"
                )
            if row["status"] != status or row["finished_at"] is None:
                raise RunRegistryDriftError(f"attempt {attempt_id} has invalid terminal state")

    return _attempt_state(row)
