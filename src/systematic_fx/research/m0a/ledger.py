"""Durable local SQLite control ledger for bounded M0a research epochs.

The ledger is deliberately local: PostgreSQL remains untouched and no result in
this module grants walk-forward, holdout, paper, or live authority.  SQLite owns
only crash recovery, finite candidate budgets, leases, and content-addressed
local result evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
import tempfile
import time
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Final, Literal

from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256

_SCHEMA_VERSION: Final = 1
_SHA256 = re.compile(r"[0-9a-f]{64}")
_WRITE_BITS: Final = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH

CandidateKind = Literal["REAL", "NULL"]
CandidateStatus = Literal[
    "QUEUED",
    "RUNNING",
    "SCREENED_OUT",
    "SEQUENTIAL_TEST",
    "WALK_FORWARD",
    "REGISTERED",
    "FAILED",
    "CRASHED",
]
AttemptStatus = Literal["QUEUED", "RUNNING", "COMPLETED", "FAILED", "CRASHED"]


class M0aLedgerError(RuntimeError):
    """A durable M0a operation could not be completed safely."""


class EpochDriftError(M0aLedgerError):
    """An existing epoch differs from the requested immutable identity."""


class LedgerStateError(M0aLedgerError):
    """A ledger row is in an invalid lifecycle state."""


class LedgerInvariantError(M0aLedgerError):
    """Durable rows or local artifact bytes fail exact reconstruction."""


@dataclass(frozen=True, slots=True)
class EpochRegistration:
    epoch_id: str
    epoch_hash: str
    identity_sha256: str
    created: bool


@dataclass(frozen=True, slots=True)
class CandidateRegistration:
    epoch_id: str
    candidate_id: int | None
    candidate_sha256: str
    candidate_kind: CandidateKind
    attempt_id: int | None
    created: bool
    budget_exhausted: bool
    parent_candidate_id: int | None = None


@dataclass(frozen=True, slots=True)
class CandidateSnapshot:
    candidate_id: int
    epoch_id: str
    candidate_sha256: str
    candidate_kind: CandidateKind
    candidate_ordinal: int
    parent_candidate_id: int | None
    candidate_payload: Mapping[str, Any]
    status: CandidateStatus
    registered_at: datetime | None


@dataclass(frozen=True, slots=True)
class DurableEpochEvaluation:
    """Verified report-only reconstruction; it never reruns candidate economics."""

    epoch_record: Mapping[str, Any]
    candidate_records: tuple[Mapping[str, Any], ...]
    retry_count: int
    failure_count: int


@dataclass(frozen=True, slots=True)
class AttemptLease:
    epoch_id: str
    candidate_id: int
    candidate_sha256: str
    candidate_kind: CandidateKind
    candidate_payload: Mapping[str, Any]
    attempt_id: int
    attempt_number: int
    attempt_key: str
    lease_owner: str
    lease_expires_at: datetime
    resumed: bool = False


@dataclass(frozen=True, slots=True)
class LocalArtifact:
    artifact_sha256: str
    byte_size: int
    path: Path
    canonical_json: str
    created: bool


@dataclass(frozen=True, slots=True)
class AttemptCompletion:
    attempt_id: int
    candidate_id: int
    candidate_sha256: str
    candidate_status: CandidateStatus
    artifact: LocalArtifact


@dataclass(frozen=True, slots=True)
class StaleRecovery:
    crashed_attempt_id: int
    retry_attempt_id: int | None
    candidate_id: int
    attempt_number: int
    epoch_halted: bool


@dataclass(frozen=True, slots=True)
class EpochReport:
    epoch_id: str
    epoch_hash: str
    status: str
    generation_complete: bool
    real_candidate_budget: int
    null_candidate_budget: int
    real_candidate_count: int
    null_candidate_count: int
    candidate_status_counts: Mapping[str, int]
    attempt_status_counts: Mapping[str, int]
    registered_candidate_sha256s: tuple[str, ...]
    consecutive_system_errors: int
    system_error_threshold: int
    halted_reason: str | None
    event_count: int
    event_tail_sha256: str | None


@dataclass(frozen=True, slots=True)
class InvariantReport:
    epoch_id: str
    candidate_count: int
    attempt_count: int
    artifact_count: int
    event_count: int
    event_tail_sha256: str | None
    valid: bool = True


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _as_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise M0aLedgerError("timestamps must be timezone-aware datetimes")
    return value.astimezone(UTC)


def _timestamp_us(value: datetime) -> int:
    return int(_as_utc(value).timestamp() * 1_000_000)


def _from_timestamp_us(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1_000_000, tz=UTC)


def _nonempty(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise M0aLedgerError(f"{label} must be a non-empty string")
    return value.strip()


def _sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise M0aLedgerError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _plain_mapping(value: object, *, label: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        result = dict(value)
    elif hasattr(value, "as_dict") and callable(value.as_dict):
        result = dict(value.as_dict())
    elif is_dataclass(value) and not isinstance(value, type):
        result = asdict(value)
    else:
        raise M0aLedgerError(f"{label} must be a mapping or expose as_dict()")
    try:
        return json.loads(canonical_json_bytes(result))
    except (TypeError, ValueError) as error:
        raise M0aLedgerError(f"{label} is not canonical research JSON: {error}") from error


def _canonical_text(value: object) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def _epoch_identity(config: object) -> tuple[str, str, str, dict[str, Any]]:
    document = _plain_mapping(config, label="epoch config")
    epoch_id = _nonempty(document.get("epoch_id"), label="epoch_id")
    epoch_hash = _sha256(document.get("epoch_hash"), label="epoch_hash")
    file_sha256 = _sha256(document.get("file_sha256"), label="file_sha256")
    required = {
        "code_commit",
        "dataset_hash",
        "dataset_version",
        "execution_model_version",
        "family_id",
        "feature_version",
        "label_version",
        "null_candidate_budget",
        "random_seeds",
        "real_candidate_budget",
        "schema_version",
    }
    missing = sorted(required - document.keys())
    if missing:
        raise M0aLedgerError(f"epoch config is missing immutable fields: {missing}")
    for key in ("real_candidate_budget", "null_candidate_budget"):
        value = document[key]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise M0aLedgerError(f"{key} must be a positive integer")
    return epoch_id, epoch_hash, file_sha256, document


def candidate_identity(candidate: object) -> tuple[str, str, dict[str, Any]]:
    """Return ``(family_id, sha256, complete canonical payload)`` for a candidate."""

    document = _plain_mapping(candidate, label="candidate")
    claimed = document.pop("candidate_hash", None)
    if claimed is None:
        claimed = document.pop("candidate_sha256", None)
    family_id = _nonempty(document.get("family_id"), label="candidate family_id")
    if "parameters" not in document or not isinstance(document["parameters"], Mapping):
        raise M0aLedgerError("candidate parameters must be a JSON object")
    if hasattr(candidate, "identity_payload") and callable(candidate.identity_payload):
        identity = _plain_mapping(candidate.identity_payload(), label="candidate identity")
    elif "null_family_id" in document:
        identity = {
            key: document[key]
            for key in (
                "barrier",
                "control_id",
                "direction",
                "family_id",
                "feature_tier",
                "method",
                "null_family_id",
                "parameters",
                "parent_candidate_hash",
                "random_seed",
            )
        }
    elif {"barrier", "direction", "feature_tier"} <= document.keys():
        identity = {
            "barrier": document["barrier"],
            "direction": document["direction"],
            "family_id": family_id,
            "feature_tier": document["feature_tier"],
            "parameters": document["parameters"],
        }
    else:
        identity = {"family_id": family_id, "parameters": document["parameters"]}
    observed = canonical_sha256(identity)
    if claimed is not None and claimed != observed:
        raise M0aLedgerError("candidate claimed hash differs from its canonical parameters")
    return family_id, observed, document


def _evaluation_payload(evaluation: object) -> dict[str, Any]:
    return _plain_mapping(evaluation, label="candidate evaluation")


class LocalArtifactStore:
    """Publish immutable canonical JSON result files below one local root."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def publish(self, *, epoch_id: str, payload: object) -> LocalArtifact:
        safe_epoch = _nonempty(epoch_id, label="epoch_id")
        if safe_epoch in {".", ".."} or "/" in safe_epoch or "\\" in safe_epoch:
            raise M0aLedgerError("epoch_id is not safe for an artifact namespace")
        canonical_json = _canonical_text(payload)
        content = canonical_json.encode("utf-8") + b"\n"
        sha256 = hashlib.sha256(content).hexdigest()
        directory = self.root / safe_epoch / "results"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"sha256={sha256}.json"
        if path.exists():
            self._verify_existing(path, content)
            return LocalArtifact(sha256, len(content), path, canonical_json, False)

        descriptor, temporary_name = tempfile.mkstemp(prefix=".m0a-result-", dir=directory)
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            temporary.chmod(0o444)
            try:
                os.link(temporary, path)
                created = True
            except FileExistsError:
                created = False
            if created:
                directory_fd = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            self._verify_existing(path, content)
            return LocalArtifact(sha256, len(content), path, canonical_json, created)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    @staticmethod
    def _verify_existing(path: Path, expected: bytes) -> None:
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or path.is_symlink():
            raise LedgerInvariantError("M0a artifact path is not a regular non-symlink file")
        observed = path.read_bytes()
        if observed != expected:
            raise LedgerInvariantError("existing M0a result artifact content drift")
        if metadata.st_mode & _WRITE_BITS:
            path.chmod(metadata.st_mode & ~_WRITE_BITS)


class M0aLedger:
    """SQLite-backed M0a epoch, candidate, attempt, artifact, and event ledger."""

    def __init__(
        self,
        database_path: str | Path,
        *,
        artifact_root: str | Path | None = None,
        clock: Callable[[], datetime] = _utc_now,
        default_lease_seconds: int = 60,
        default_system_error_threshold: int = 3,
    ) -> None:
        self.database_path = Path(database_path).expanduser().resolve()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.artifacts = LocalArtifactStore(
            artifact_root or self.database_path.parent / f"{self.database_path.stem}-artifacts"
        )
        self._clock = clock
        if default_lease_seconds <= 0:
            raise M0aLedgerError("default_lease_seconds must be positive")
        if default_system_error_threshold <= 0:
            raise M0aLedgerError("default_system_error_threshold must be positive")
        self.default_lease_seconds = default_lease_seconds
        self.default_system_error_threshold = default_system_error_threshold
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.database_path,
            timeout=30,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    @contextmanager
    def _transaction(self) -> Any:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connect() as connection:
            mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                raise M0aLedgerError("M0a SQLite ledger could not enable WAL mode")
            connection.executescript(_SCHEMA_SQL)
            row = connection.execute(
                "SELECT value FROM schema_metadata WHERE key = 'schema_version'"
            ).fetchone()
            if row is None or int(row["value"]) != _SCHEMA_VERSION:
                raise M0aLedgerError("unsupported M0a SQLite schema version")

    def ensure_epoch(
        self,
        config: object,
        *,
        system_error_threshold: int | None = None,
    ) -> EpochRegistration:
        """Insert an immutable epoch or verify every identity field exactly."""

        if hasattr(config, "verify_unchanged") and callable(config.verify_unchanged):
            config.verify_unchanged()
        epoch_id, epoch_hash, file_sha256, document = _epoch_identity(config)
        threshold = system_error_threshold or self.default_system_error_threshold
        if threshold <= 0:
            raise M0aLedgerError("system_error_threshold must be positive")
        canonical = _canonical_text(document)
        identity_sha256 = canonical_sha256(document)
        now_us = _timestamp_us(self._clock())
        expected = {
            "config_file_sha256": file_sha256,
            "config_json": canonical,
            "dataset_sha256": document["dataset_hash"],
            "dataset_version": document["dataset_version"],
            "epoch_hash": epoch_hash,
            "execution_version": document["execution_model_version"],
            "family_id": document["family_id"],
            "feature_version": document["feature_version"],
            "identity_sha256": identity_sha256,
            "label_version": document["label_version"],
            "null_candidate_budget": document["null_candidate_budget"],
            "real_candidate_budget": document["real_candidate_budget"],
            "code_version": document["code_commit"],
            "system_error_threshold": threshold,
        }
        with self._transaction() as connection:
            inserted = connection.execute(
                """
                INSERT OR IGNORE INTO epochs
                    (epoch_id, epoch_hash, identity_sha256, config_file_sha256,
                     config_json, dataset_version, dataset_sha256, feature_version,
                     label_version, execution_version, code_version, family_id,
                     real_candidate_budget, null_candidate_budget,
                     system_error_threshold, created_at_us)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    epoch_id,
                    epoch_hash,
                    identity_sha256,
                    file_sha256,
                    canonical,
                    document["dataset_version"],
                    document["dataset_hash"],
                    document["feature_version"],
                    document["label_version"],
                    document["execution_model_version"],
                    document["code_commit"],
                    document["family_id"],
                    document["real_candidate_budget"],
                    document["null_candidate_budget"],
                    threshold,
                    now_us,
                ),
            ).rowcount
            row = connection.execute(
                "SELECT * FROM epochs WHERE epoch_id = ?", (epoch_id,)
            ).fetchone()
            if row is None:  # pragma: no cover - protected by the insert/select transaction
                raise M0aLedgerError("epoch registration returned no row")
            mismatches = [key for key, value in expected.items() if row[key] != value]
            if mismatches:
                raise EpochDriftError(
                    f"epoch {epoch_id} immutable identity drift: {', '.join(sorted(mismatches))}"
                )
            if inserted:
                self._append_event(
                    connection,
                    epoch_id=epoch_id,
                    event_type="EPOCH_REGISTERED",
                    payload={
                        "epoch_hash": epoch_hash,
                        "identity_sha256": identity_sha256,
                        "null_candidate_budget": document["null_candidate_budget"],
                        "real_candidate_budget": document["real_candidate_budget"],
                    },
                    created_at_us=now_us,
                )
        return EpochRegistration(epoch_id, epoch_hash, identity_sha256, bool(inserted))

    register_epoch = ensure_epoch

    def register_candidate(
        self,
        epoch_id: str,
        candidate: object,
        *,
        candidate_kind: CandidateKind = "REAL",
    ) -> CandidateRegistration:
        """Register one canonical candidate and its deterministic first attempt."""

        epoch_id = _nonempty(epoch_id, label="epoch_id")
        if candidate_kind not in {"REAL", "NULL"}:
            raise M0aLedgerError("candidate_kind must be REAL or NULL")
        family_id, candidate_sha256, identity = candidate_identity(candidate)
        candidate_json = _canonical_text(identity)
        parent_candidate_sha256: str | None = None
        if candidate_kind == "NULL":
            parent_candidate_sha256 = _sha256(
                identity.get("parent_candidate_hash"),
                label="null parent_candidate_hash",
            )
        elif "parent_candidate_hash" in identity:
            raise M0aLedgerError("REAL candidates cannot declare a parent candidate")
        now_us = _timestamp_us(self._clock())
        with self._transaction() as connection:
            epoch = connection.execute(
                "SELECT * FROM epochs WHERE epoch_id = ?", (epoch_id,)
            ).fetchone()
            if epoch is None:
                raise LedgerStateError(f"epoch {epoch_id} is not registered")
            if epoch["family_id"] != family_id:
                raise EpochDriftError("candidate family differs from the immutable epoch family")
            existing = connection.execute(
                "SELECT * FROM candidates WHERE epoch_id = ? AND candidate_sha256 = ?",
                (epoch_id, candidate_sha256),
            ).fetchone()
            if existing is not None:
                parent_id = (
                    int(existing["parent_candidate_id"])
                    if existing["parent_candidate_id"] is not None
                    else None
                )
                if (
                    existing["candidate_kind"] != candidate_kind
                    or existing["family_id"] != family_id
                    or existing["candidate_json"] != candidate_json
                ):
                    raise EpochDriftError("candidate hash resolves to different immutable identity")
                if candidate_kind == "NULL":
                    parent = connection.execute(
                        "SELECT candidate_sha256 FROM candidates WHERE candidate_id = ?",
                        (parent_id,),
                    ).fetchone()
                    if parent is None or parent["candidate_sha256"] != parent_candidate_sha256:
                        raise EpochDriftError("null candidate parent identity drift")
                attempt = connection.execute(
                    "SELECT attempt_id FROM attempts WHERE candidate_id = ? ORDER BY attempt_number LIMIT 1",
                    (existing["candidate_id"],),
                ).fetchone()
                return CandidateRegistration(
                    epoch_id,
                    int(existing["candidate_id"]),
                    candidate_sha256,
                    candidate_kind,
                    int(attempt["attempt_id"]) if attempt is not None else None,
                    False,
                    False,
                    parent_id,
                )
            if epoch["status"] in {"COMPLETED", "HALTED"} or epoch["generation_complete"]:
                raise LedgerStateError(f"epoch {epoch_id} no longer accepts candidates")
            budget_column = (
                "real_candidate_budget" if candidate_kind == "REAL" else "null_candidate_budget"
            )
            count = connection.execute(
                "SELECT count(*) AS count FROM candidates WHERE epoch_id = ? AND candidate_kind = ?",
                (epoch_id, candidate_kind),
            ).fetchone()["count"]
            if int(count) >= int(epoch[budget_column]):
                return CandidateRegistration(
                    epoch_id, None, candidate_sha256, candidate_kind, None, False, True, None
                )
            parent_candidate_id: int | None = None
            if candidate_kind == "NULL":
                parent = connection.execute(
                    """
                    SELECT candidate_id FROM candidates
                    WHERE epoch_id = ? AND candidate_sha256 = ?
                      AND candidate_kind = 'REAL' AND family_id = ?
                    """,
                    (epoch_id, parent_candidate_sha256, family_id),
                ).fetchone()
                if parent is None:
                    raise LedgerStateError(
                        "null candidate requires its exact earlier REAL parent in this epoch"
                    )
                parent_candidate_id = int(parent["candidate_id"])
            ordinal = int(count) + 1
            cursor = connection.execute(
                """
                INSERT INTO candidates
                    (epoch_id, candidate_sha256, candidate_kind, family_id,
                     candidate_ordinal, parent_candidate_id, candidate_json, created_at_us)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    epoch_id,
                    candidate_sha256,
                    candidate_kind,
                    family_id,
                    ordinal,
                    parent_candidate_id,
                    candidate_json,
                    now_us,
                ),
            )
            candidate_id = int(cursor.lastrowid)
            attempt_id = self._insert_attempt(
                connection,
                epoch_id=epoch_id,
                candidate_id=candidate_id,
                candidate_sha256=candidate_sha256,
                attempt_number=1,
                now_us=now_us,
            )
            self._append_event(
                connection,
                epoch_id=epoch_id,
                candidate_id=candidate_id,
                attempt_id=attempt_id,
                event_type="CANDIDATE_QUEUED",
                payload={
                    "candidate_kind": candidate_kind,
                    "candidate_ordinal": ordinal,
                    "candidate_sha256": candidate_sha256,
                },
                created_at_us=now_us,
            )
            counts = connection.execute(
                """
                SELECT count(*) FILTER (WHERE candidate_kind = 'REAL') AS real_count,
                       count(*) FILTER (WHERE candidate_kind = 'NULL') AS null_count
                FROM candidates WHERE epoch_id = ?
                """,
                (epoch_id,),
            ).fetchone()
            if int(counts["real_count"]) == int(epoch["real_candidate_budget"]) and int(
                counts["null_count"]
            ) == int(epoch["null_candidate_budget"]):
                connection.execute(
                    "UPDATE epochs SET generation_complete = 1 WHERE epoch_id = ?",
                    (epoch_id,),
                )
                self._append_event(
                    connection,
                    epoch_id=epoch_id,
                    event_type="GENERATION_BUDGET_EXHAUSTED",
                    payload={
                        "null_candidate_count": counts["null_count"],
                        "real_candidate_count": counts["real_count"],
                    },
                    created_at_us=now_us,
                )
        return CandidateRegistration(
            epoch_id,
            candidate_id,
            candidate_sha256,
            candidate_kind,
            attempt_id,
            True,
            False,
            parent_candidate_id,
        )

    enqueue_candidate = register_candidate

    def mark_generation_complete(self, epoch_id: str) -> None:
        epoch_id = _nonempty(epoch_id, label="epoch_id")
        now_us = _timestamp_us(self._clock())
        with self._transaction() as connection:
            epoch = connection.execute(
                "SELECT status, generation_complete FROM epochs WHERE epoch_id = ?", (epoch_id,)
            ).fetchone()
            if epoch is None:
                raise LedgerStateError(f"epoch {epoch_id} is not registered")
            if epoch["status"] == "HALTED":
                raise LedgerStateError(f"epoch {epoch_id} is halted")
            if not epoch["generation_complete"]:
                connection.execute(
                    "UPDATE epochs SET generation_complete = 1 WHERE epoch_id = ?", (epoch_id,)
                )
                self._append_event(
                    connection,
                    epoch_id=epoch_id,
                    event_type="GENERATION_COMPLETED",
                    payload={},
                    created_at_us=now_us,
                )
            self._complete_epoch_if_idle(connection, epoch_id=epoch_id, now_us=now_us)

    def claim_next_attempt(
        self,
        epoch_id: str,
        *,
        lease_owner: str,
        lease_seconds: int | None = None,
        now: datetime | None = None,
    ) -> AttemptLease | None:
        """Claim one QUEUED attempt or exactly resume this owner's live lease."""

        epoch_id = _nonempty(epoch_id, label="epoch_id")
        owner = _nonempty(lease_owner, label="lease_owner")
        duration = lease_seconds or self.default_lease_seconds
        if duration <= 0:
            raise M0aLedgerError("lease_seconds must be positive")
        current = _as_utc(now or self._clock())
        now_us = _timestamp_us(current)
        expires_us = _timestamp_us(current + timedelta(seconds=duration))
        with self._transaction() as connection:
            epoch = connection.execute(
                "SELECT status FROM epochs WHERE epoch_id = ?", (epoch_id,)
            ).fetchone()
            if epoch is None:
                raise LedgerStateError(f"epoch {epoch_id} is not registered")
            if epoch["status"] in {"COMPLETED", "HALTED"}:
                return None
            resumed = connection.execute(
                """
                SELECT attempt.*, candidate.candidate_sha256, candidate.candidate_kind,
                       candidate.candidate_json
                FROM attempts AS attempt
                JOIN candidates AS candidate ON candidate.candidate_id = attempt.candidate_id
                WHERE attempt.epoch_id = ? AND attempt.status = 'RUNNING'
                  AND attempt.lease_owner = ? AND attempt.lease_expires_at_us > ?
                ORDER BY candidate.candidate_kind, candidate.candidate_ordinal
                LIMIT 1
                """,
                (epoch_id, owner, now_us),
            ).fetchone()
            if resumed is not None:
                connection.execute(
                    "UPDATE attempts SET heartbeat_at_us = ?, lease_expires_at_us = ? "
                    "WHERE attempt_id = ?",
                    (now_us, expires_us, resumed["attempt_id"]),
                )
                return self._lease_from_row(
                    resumed, owner=owner, expires_us=expires_us, resumed=True
                )
            row = connection.execute(
                """
                SELECT attempt.*, candidate.candidate_sha256, candidate.candidate_kind,
                       candidate.candidate_json
                FROM attempts AS attempt
                JOIN candidates AS candidate ON candidate.candidate_id = attempt.candidate_id
                WHERE attempt.epoch_id = ? AND attempt.status = 'QUEUED'
                  AND candidate.status = 'QUEUED'
                ORDER BY CASE candidate.candidate_kind WHEN 'REAL' THEN 0 ELSE 1 END,
                         candidate.candidate_ordinal, attempt.attempt_number
                LIMIT 1
                """,
                (epoch_id,),
            ).fetchone()
            if row is None:
                self._complete_epoch_if_idle(connection, epoch_id=epoch_id, now_us=now_us)
                return None
            connection.execute(
                """
                UPDATE attempts
                SET status = 'RUNNING', lease_owner = ?, lease_expires_at_us = ?,
                    heartbeat_at_us = ?, started_at_us = ?
                WHERE attempt_id = ? AND status = 'QUEUED'
                """,
                (owner, expires_us, now_us, now_us, row["attempt_id"]),
            )
            connection.execute(
                "UPDATE candidates SET status = 'RUNNING' WHERE candidate_id = ? AND status = 'QUEUED'",
                (row["candidate_id"],),
            )
            connection.execute(
                "UPDATE epochs SET status = 'RUNNING' WHERE epoch_id = ? AND status = 'QUEUED'",
                (epoch_id,),
            )
            self._append_event(
                connection,
                epoch_id=epoch_id,
                candidate_id=int(row["candidate_id"]),
                attempt_id=int(row["attempt_id"]),
                event_type="ATTEMPT_STARTED",
                payload={
                    "attempt_key": row["attempt_key"],
                    "attempt_number": row["attempt_number"],
                    "lease_expires_at_us": expires_us,
                    "lease_owner": owner,
                },
                created_at_us=now_us,
            )
            return self._lease_from_row(row, owner=owner, expires_us=expires_us, resumed=False)

    claim = claim_next_attempt

    def heartbeat(
        self,
        lease: AttemptLease,
        *,
        lease_seconds: int | None = None,
        now: datetime | None = None,
    ) -> AttemptLease:
        duration = lease_seconds or self.default_lease_seconds
        if duration <= 0:
            raise M0aLedgerError("lease_seconds must be positive")
        current = _as_utc(now or self._clock())
        now_us = _timestamp_us(current)
        expires_us = _timestamp_us(current + timedelta(seconds=duration))
        with self._transaction() as connection:
            row = self._owned_running_attempt(
                connection, lease=lease, now_us=now_us, require_unexpired=True
            )
            connection.execute(
                "UPDATE attempts SET heartbeat_at_us = ?, lease_expires_at_us = ? "
                "WHERE attempt_id = ?",
                (now_us, expires_us, lease.attempt_id),
            )
        return self._lease_from_row(
            row, owner=lease.lease_owner, expires_us=expires_us, resumed=True
        )

    def recover_stale_attempts(
        self,
        epoch_id: str,
        *,
        now: datetime | None = None,
    ) -> tuple[StaleRecovery, ...]:
        """Mark expired RUNNING attempts CRASHED and append deterministic retries."""

        epoch_id = _nonempty(epoch_id, label="epoch_id")
        now_us = _timestamp_us(now or self._clock())
        reports: list[StaleRecovery] = []
        with self._transaction() as connection:
            epoch = connection.execute(
                "SELECT * FROM epochs WHERE epoch_id = ?", (epoch_id,)
            ).fetchone()
            if epoch is None:
                raise LedgerStateError(f"epoch {epoch_id} is not registered")
            if epoch["status"] == "HALTED":
                return ()
            stale = connection.execute(
                """
                SELECT attempt.*, candidate.candidate_sha256
                FROM attempts AS attempt
                JOIN candidates AS candidate ON candidate.candidate_id = attempt.candidate_id
                WHERE attempt.epoch_id = ? AND attempt.status = 'RUNNING'
                  AND attempt.lease_expires_at_us <= ?
                ORDER BY attempt.attempt_id
                """,
                (epoch_id, now_us),
            ).fetchall()
            consecutive = int(epoch["consecutive_system_errors"])
            threshold = int(epoch["system_error_threshold"])
            for row in stale:
                connection.execute(
                    """
                UPDATE attempts
                SET status = 'CRASHED', finished_at_us = ?, error_class = 'STALE_LEASE',
                        error_message = 'RUNNING lease expired before terminal commit',
                        system_error = 1, lease_owner = NULL,
                        lease_expires_at_us = NULL, heartbeat_at_us = NULL
                    WHERE attempt_id = ? AND status = 'RUNNING'
                    """,
                    (now_us, row["attempt_id"]),
                )
                connection.execute(
                    "UPDATE candidates SET status = 'CRASHED' "
                    "WHERE candidate_id = ? AND status = 'RUNNING'",
                    (row["candidate_id"],),
                )
                consecutive += 1
                self._append_event(
                    connection,
                    epoch_id=epoch_id,
                    candidate_id=int(row["candidate_id"]),
                    attempt_id=int(row["attempt_id"]),
                    event_type="ATTEMPT_CRASHED",
                    payload={
                        "attempt_number": row["attempt_number"],
                        "reason": "STALE_LEASE",
                    },
                    created_at_us=now_us,
                )
                retry_attempt_id: int | None = None
                halted = consecutive >= threshold
                if halted:
                    connection.execute(
                        """
                        UPDATE epochs
                        SET status = 'HALTED', consecutive_system_errors = ?,
                            halted_reason = 'CONSECUTIVE_SYSTEM_ERROR_THRESHOLD',
                            completed_at_us = ?
                        WHERE epoch_id = ?
                        """,
                        (consecutive, now_us, epoch_id),
                    )
                else:
                    next_number = int(row["attempt_number"]) + 1
                    retry_attempt_id = self._insert_attempt(
                        connection,
                        epoch_id=epoch_id,
                        candidate_id=int(row["candidate_id"]),
                        candidate_sha256=str(row["candidate_sha256"]),
                        attempt_number=next_number,
                        now_us=now_us,
                    )
                    connection.execute(
                        "UPDATE candidates SET status = 'QUEUED' WHERE candidate_id = ?",
                        (row["candidate_id"],),
                    )
                    connection.execute(
                        "UPDATE epochs SET consecutive_system_errors = ? WHERE epoch_id = ?",
                        (consecutive, epoch_id),
                    )
                    self._append_event(
                        connection,
                        epoch_id=epoch_id,
                        candidate_id=int(row["candidate_id"]),
                        attempt_id=retry_attempt_id,
                        event_type="ATTEMPT_RETRY_QUEUED",
                        payload={"attempt_number": next_number},
                        created_at_us=now_us,
                    )
                reports.append(
                    StaleRecovery(
                        int(row["attempt_id"]),
                        retry_attempt_id,
                        int(row["candidate_id"]),
                        int(row["attempt_number"]),
                        halted,
                    )
                )
                if halted:
                    break
        return tuple(reports)

    recover_stale = recover_stale_attempts

    def publish_result_artifact(
        self,
        lease: AttemptLease,
        evaluation: object,
    ) -> LocalArtifact:
        payload = {
            "artifact_schema": "systematic_fx.m0a_candidate_result.v1",
            "attempt_key": lease.attempt_key,
            "attempt_number": lease.attempt_number,
            "candidate_sha256": lease.candidate_sha256,
            "epoch_id": lease.epoch_id,
            "evaluation": _evaluation_payload(evaluation),
        }
        return self.artifacts.publish(epoch_id=lease.epoch_id, payload=payload)

    def complete_attempt(
        self,
        lease: AttemptLease,
        evaluation: object,
        *,
        artifact: LocalArtifact | None = None,
        final_status: Literal["SCREENED_OUT", "REGISTERED"] | None = None,
        now: datetime | None = None,
    ) -> AttemptCompletion:
        """Atomically bind one verified result artifact and terminal evaluation."""

        evaluation_payload = _evaluation_payload(evaluation)
        admitted = evaluation_payload.get("admitted")
        evaluation_status = evaluation_payload.get("status")
        if final_status is None:
            if admitted is True or evaluation_status in {"SEARCH_DATA_SURVIVOR", "REGISTERED"}:
                final_status = "REGISTERED"
            elif admitted is False or evaluation_status in {"SCREENED_OUT", "REJECTED"}:
                final_status = "SCREENED_OUT"
            else:
                raise M0aLedgerError(
                    "candidate evaluation must declare admitted or an exact terminal status"
                )
        if final_status == "REGISTERED" and lease.candidate_kind == "NULL":
            raise LedgerStateError("null-control candidates cannot be REGISTERED")
        result = artifact or self.publish_result_artifact(lease, evaluation_payload)
        self._verify_artifact(result)
        now_us = _timestamp_us(now or self._clock())
        metrics = {
            "raw_event_metrics_json": _canonical_text(
                evaluation_payload.get("raw_event_metrics", {})
            ),
            "flat_only_metrics_json": _canonical_text(
                evaluation_payload.get("flat_only_metrics", {})
            ),
            "sequential_metrics_json": _canonical_text(
                evaluation_payload.get("sequential_metrics", {})
            ),
            "stressed_cost_metrics_json": _canonical_text(
                evaluation_payload.get("stressed_cost_metrics", {})
            ),
            "fold_metrics_json": _canonical_text(evaluation_payload.get("fold_metrics", {})),
            "controls_json": _canonical_text(
                evaluation_payload.get(
                    "controls",
                    {
                        "circular_shift_control": evaluation_payload.get(
                            "circular_shift_control", {}
                        ),
                        "matched_random_control": evaluation_payload.get(
                            "matched_random_control", {}
                        ),
                    },
                )
            ),
        }
        evaluation_json = _canonical_text(evaluation_payload)
        evaluation_sha256 = canonical_sha256(evaluation_payload)
        with self._transaction() as connection:
            self._owned_running_attempt(
                connection, lease=lease, now_us=now_us, require_unexpired=True
            )
            artifact_id = self._ensure_artifact(
                connection,
                lease=lease,
                artifact=result,
                now_us=now_us,
            )
            connection.execute(
                """
                UPDATE attempts
                SET status = 'COMPLETED', finished_at_us = ?, lease_owner = NULL,
                    lease_expires_at_us = NULL, heartbeat_at_us = NULL,
                    result_artifact_id = ?, evaluation_json = ?,
                    evaluation_sha256 = ?, system_error = 0
                WHERE attempt_id = ? AND status = 'RUNNING'
                """,
                (now_us, artifact_id, evaluation_json, evaluation_sha256, lease.attempt_id),
            )
            if final_status == "REGISTERED":
                for status, event_type in (
                    ("SEQUENTIAL_TEST", "SEQUENTIAL_TEST_COMPLETED"),
                    ("WALK_FORWARD", "WALK_FORWARD_COMPLETED"),
                ):
                    connection.execute(
                        f"UPDATE candidates SET status = '{status}' WHERE candidate_id = ?",
                        (lease.candidate_id,),
                    )
                    self._append_event(
                        connection,
                        epoch_id=lease.epoch_id,
                        candidate_id=lease.candidate_id,
                        attempt_id=lease.attempt_id,
                        event_type=event_type,
                        payload={"evaluation_sha256": evaluation_sha256},
                        created_at_us=now_us,
                    )
                registered_at_us: int | None = now_us
            else:
                registered_at_us = None
            connection.execute(
                """
                UPDATE candidates
                SET status = ?, registered_at_us = ?, result_artifact_id = ?,
                    evaluation_json = ?, evaluation_sha256 = ?,
                    raw_event_metrics_json = ?, flat_only_metrics_json = ?,
                    sequential_metrics_json = ?, stressed_cost_metrics_json = ?,
                    fold_metrics_json = ?, controls_json = ?
                WHERE candidate_id = ?
                """,
                (
                    final_status,
                    registered_at_us,
                    artifact_id,
                    evaluation_json,
                    evaluation_sha256,
                    metrics["raw_event_metrics_json"],
                    metrics["flat_only_metrics_json"],
                    metrics["sequential_metrics_json"],
                    metrics["stressed_cost_metrics_json"],
                    metrics["fold_metrics_json"],
                    metrics["controls_json"],
                    lease.candidate_id,
                ),
            )
            connection.execute(
                "UPDATE epochs SET consecutive_system_errors = 0 WHERE epoch_id = ?",
                (lease.epoch_id,),
            )
            self._append_event(
                connection,
                epoch_id=lease.epoch_id,
                candidate_id=lease.candidate_id,
                attempt_id=lease.attempt_id,
                event_type="CANDIDATE_REGISTERED"
                if final_status == "REGISTERED"
                else "CANDIDATE_SCREENED_OUT",
                payload={
                    "artifact_sha256": result.artifact_sha256,
                    "evaluation_sha256": evaluation_sha256,
                    "status": final_status,
                },
                created_at_us=now_us,
            )
            self._complete_epoch_if_idle(connection, epoch_id=lease.epoch_id, now_us=now_us)
        return AttemptCompletion(
            lease.attempt_id,
            lease.candidate_id,
            lease.candidate_sha256,
            final_status,
            result,
        )

    finish_attempt = complete_attempt

    def fail_attempt(
        self,
        lease: AttemptLease,
        *,
        error: BaseException | str,
        system_error: bool = True,
        now: datetime | None = None,
    ) -> bool:
        """Fail one candidate while preserving the rest of the epoch queue.

        Returns ``True`` when the consecutive system-error threshold halted the
        epoch.  A successful later candidate resets the counter.
        """

        now_us = _timestamp_us(now or self._clock())
        error_class = type(error).__name__ if isinstance(error, BaseException) else "ERROR"
        message = _nonempty(str(error), label="error message")
        with self._transaction() as connection:
            self._owned_running_attempt(
                connection, lease=lease, now_us=now_us, require_unexpired=False
            )
            epoch = connection.execute(
                "SELECT consecutive_system_errors, system_error_threshold "
                "FROM epochs WHERE epoch_id = ?",
                (lease.epoch_id,),
            ).fetchone()
            consecutive = int(epoch["consecutive_system_errors"]) + (1 if system_error else 0)
            halted = system_error and consecutive >= int(epoch["system_error_threshold"])
            connection.execute(
                """
                UPDATE attempts
                SET status = 'FAILED', finished_at_us = ?, lease_owner = NULL,
                    lease_expires_at_us = NULL, heartbeat_at_us = NULL,
                    error_class = ?, error_message = ?, system_error = ?
                WHERE attempt_id = ? AND status = 'RUNNING'
                """,
                (now_us, error_class, message, int(system_error), lease.attempt_id),
            )
            connection.execute(
                "UPDATE candidates SET status = 'FAILED' WHERE candidate_id = ?",
                (lease.candidate_id,),
            )
            if halted:
                connection.execute(
                    """
                    UPDATE epochs
                    SET status = 'HALTED', consecutive_system_errors = ?,
                        halted_reason = 'CONSECUTIVE_SYSTEM_ERROR_THRESHOLD',
                        completed_at_us = ?
                    WHERE epoch_id = ?
                    """,
                    (consecutive, now_us, lease.epoch_id),
                )
            else:
                connection.execute(
                    "UPDATE epochs SET consecutive_system_errors = ? WHERE epoch_id = ?",
                    (consecutive, lease.epoch_id),
                )
            self._append_event(
                connection,
                epoch_id=lease.epoch_id,
                candidate_id=lease.candidate_id,
                attempt_id=lease.attempt_id,
                event_type="CANDIDATE_FAILED",
                payload={
                    "error_class": error_class,
                    "error_message": message,
                    "epoch_halted": halted,
                    "system_error": system_error,
                },
                created_at_us=now_us,
            )
            if not halted:
                self._complete_epoch_if_idle(connection, epoch_id=lease.epoch_id, now_us=now_us)
        return halted

    def report(self, epoch_id: str) -> EpochReport:
        epoch_id = _nonempty(epoch_id, label="epoch_id")
        with self._connect() as connection:
            epoch = connection.execute(
                "SELECT * FROM epochs WHERE epoch_id = ?", (epoch_id,)
            ).fetchone()
            if epoch is None:
                raise LedgerStateError(f"epoch {epoch_id} is not registered")
            kind_counts = {
                row["candidate_kind"]: int(row["count"])
                for row in connection.execute(
                    "SELECT candidate_kind, count(*) AS count FROM candidates "
                    "WHERE epoch_id = ? GROUP BY candidate_kind",
                    (epoch_id,),
                ).fetchall()
            }
            candidate_counts = {
                row["status"]: int(row["count"])
                for row in connection.execute(
                    "SELECT status, count(*) AS count FROM candidates "
                    "WHERE epoch_id = ? GROUP BY status",
                    (epoch_id,),
                ).fetchall()
            }
            attempt_counts = {
                row["status"]: int(row["count"])
                for row in connection.execute(
                    "SELECT status, count(*) AS count FROM attempts "
                    "WHERE epoch_id = ? GROUP BY status",
                    (epoch_id,),
                ).fetchall()
            }
            registered = tuple(
                row["candidate_sha256"]
                for row in connection.execute(
                    "SELECT candidate_sha256 FROM candidates "
                    "WHERE epoch_id = ? AND status = 'REGISTERED' "
                    "ORDER BY candidate_sha256",
                    (epoch_id,),
                ).fetchall()
            )
            tail = connection.execute(
                "SELECT event_sequence, event_sha256 FROM events WHERE epoch_id = ? "
                "ORDER BY event_sequence DESC LIMIT 1",
                (epoch_id,),
            ).fetchone()
        return EpochReport(
            epoch_id=epoch_id,
            epoch_hash=str(epoch["epoch_hash"]),
            status=str(epoch["status"]),
            generation_complete=bool(epoch["generation_complete"]),
            real_candidate_budget=int(epoch["real_candidate_budget"]),
            null_candidate_budget=int(epoch["null_candidate_budget"]),
            real_candidate_count=kind_counts.get("REAL", 0),
            null_candidate_count=kind_counts.get("NULL", 0),
            candidate_status_counts=candidate_counts,
            attempt_status_counts=attempt_counts,
            registered_candidate_sha256s=registered,
            consecutive_system_errors=int(epoch["consecutive_system_errors"]),
            system_error_threshold=int(epoch["system_error_threshold"]),
            halted_reason=epoch["halted_reason"],
            event_count=int(tail["event_sequence"]) if tail is not None else 0,
            event_tail_sha256=str(tail["event_sha256"]) if tail is not None else None,
        )

    epoch_report = report

    def list_candidates(self, epoch_id: str) -> tuple[CandidateSnapshot, ...]:
        """Return immutable candidate snapshots in generation order."""

        epoch_id = _nonempty(epoch_id, label="epoch_id")
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM candidates WHERE epoch_id = ? "
                "ORDER BY CASE candidate_kind WHEN 'REAL' THEN 0 ELSE 1 END, "
                "candidate_ordinal",
                (epoch_id,),
            ).fetchall()
        return tuple(
            CandidateSnapshot(
                candidate_id=int(row["candidate_id"]),
                epoch_id=epoch_id,
                candidate_sha256=str(row["candidate_sha256"]),
                candidate_kind=str(row["candidate_kind"]),  # type: ignore[arg-type]
                candidate_ordinal=int(row["candidate_ordinal"]),
                parent_candidate_id=(
                    int(row["parent_candidate_id"])
                    if row["parent_candidate_id"] is not None
                    else None
                ),
                candidate_payload=json.loads(row["candidate_json"]),
                status=str(row["status"]),  # type: ignore[arg-type]
                registered_at=(
                    _from_timestamp_us(int(row["registered_at_us"]))
                    if row["registered_at_us"] is not None
                    else None
                ),
            )
            for row in rows
        )

    def load_epoch_evaluation(self, epoch_id: str) -> DurableEpochEvaluation:
        """Load all durable evaluations after exact DB/file/hash verification."""

        self.verify_invariants(epoch_id)
        with self._connect() as connection:
            epoch = connection.execute(
                "SELECT * FROM epochs WHERE epoch_id = ?", (epoch_id,)
            ).fetchone()
            if epoch is None:
                raise LedgerStateError(f"epoch {epoch_id} is not registered")
            rows = connection.execute(
                """
                SELECT candidate.*, artifact.canonical_json AS artifact_json,
                       artifact.artifact_sha256, artifact.path AS artifact_path,
                       count(attempt.attempt_id) AS attempt_count,
                       sum(CASE WHEN attempt.status IN ('FAILED', 'CRASHED') THEN 1 ELSE 0 END)
                           AS failed_attempt_count
                FROM candidates AS candidate
                LEFT JOIN artifacts AS artifact
                  ON artifact.artifact_id = candidate.result_artifact_id
                JOIN attempts AS attempt ON attempt.candidate_id = candidate.candidate_id
                WHERE candidate.epoch_id = ?
                GROUP BY candidate.candidate_id
                ORDER BY CASE candidate.candidate_kind WHEN 'REAL' THEN 0 ELSE 1 END,
                         candidate.candidate_ordinal
                """,
                (epoch_id,),
            ).fetchall()
            retry_count = sum(max(int(row["attempt_count"]) - 1, 0) for row in rows)
            failures = sum(int(row["status"] == "FAILED") for row in rows)
            attempt_status_counts = {
                str(row["status"]): int(row["count"])
                for row in connection.execute(
                    "SELECT status, count(*) AS count FROM attempts "
                    "WHERE epoch_id = ? GROUP BY status",
                    (epoch_id,),
                ).fetchall()
            }
            records: list[Mapping[str, Any]] = []
            for row in rows:
                latest_attempt = connection.execute(
                    "SELECT * FROM attempts WHERE candidate_id = ? "
                    "ORDER BY attempt_number DESC LIMIT 1",
                    (row["candidate_id"],),
                ).fetchone()
                evaluation = (
                    json.loads(row["evaluation_json"])
                    if row["evaluation_json"] is not None
                    else None
                )
                records.append(
                    {
                        "artifact_path": row["artifact_path"],
                        "artifact_sha256": row["artifact_sha256"],
                        "attempt_count": int(row["attempt_count"]),
                        "candidate_kind": row["candidate_kind"],
                        "candidate_payload": json.loads(row["candidate_json"]),
                        "candidate_sha256": row["candidate_sha256"],
                        "evaluation": evaluation,
                        "error_class": latest_attempt["error_class"],
                        "error_message": latest_attempt["error_message"],
                        "failed_attempt_count": int(row["failed_attempt_count"]),
                        "parent_candidate_id": row["parent_candidate_id"],
                        "registered_at": (
                            _from_timestamp_us(int(row["registered_at_us"])).isoformat()
                            if row["registered_at_us"] is not None
                            else None
                        ),
                        "status": row["status"],
                    }
                )
        config = json.loads(epoch["config_json"])
        epoch_record = {
            **config,
            "attempt_count": sum(int(row["attempt_count"]) for row in rows),
            "attempt_status_counts": attempt_status_counts,
            "candidate_status_counts": dict(self.report(epoch_id).candidate_status_counts),
            "failure_count": failures,
            "retry_count": retry_count,
            "status": epoch["status"],
        }
        return DurableEpochEvaluation(epoch_record, tuple(records), retry_count, failures)

    def verify_invariants(self, epoch_id: str) -> InvariantReport:
        """Recompute hashes, chains, budgets, attempts, and artifact bytes."""

        epoch_id = _nonempty(epoch_id, label="epoch_id")
        with self._connect() as connection:
            epoch = connection.execute(
                "SELECT * FROM epochs WHERE epoch_id = ?", (epoch_id,)
            ).fetchone()
            if epoch is None:
                raise LedgerStateError(f"epoch {epoch_id} is not registered")
            config = json.loads(epoch["config_json"])
            if canonical_sha256(config) != epoch["identity_sha256"]:
                raise LedgerInvariantError("epoch canonical identity hash drift")
            if _canonical_text(config) != epoch["config_json"]:
                raise LedgerInvariantError("epoch config JSON is not canonical")
            candidates = connection.execute(
                "SELECT * FROM candidates WHERE epoch_id = ? ORDER BY candidate_id",
                (epoch_id,),
            ).fetchall()
            kind_counts = {"REAL": 0, "NULL": 0}
            for candidate in candidates:
                payload = json.loads(candidate["candidate_json"])
                if _canonical_text(payload) != candidate["candidate_json"]:
                    raise LedgerInvariantError("candidate JSON is not canonical")
                _, reconstructed_sha256, _ = candidate_identity(payload)
                if reconstructed_sha256 != candidate["candidate_sha256"]:
                    raise LedgerInvariantError("candidate canonical hash drift")
                kind_counts[str(candidate["candidate_kind"])] += 1
                if candidate["candidate_kind"] == "REAL":
                    if candidate["parent_candidate_id"] is not None:
                        raise LedgerInvariantError("REAL candidate unexpectedly has a parent")
                else:
                    parent = connection.execute(
                        "SELECT * FROM candidates WHERE candidate_id = ?",
                        (candidate["parent_candidate_id"],),
                    ).fetchone()
                    if (
                        parent is None
                        or parent["candidate_kind"] != "REAL"
                        or parent["epoch_id"] != epoch_id
                        or parent["family_id"] != candidate["family_id"]
                        or int(parent["candidate_id"]) >= int(candidate["candidate_id"])
                        or parent["candidate_sha256"] != payload.get("parent_candidate_hash")
                    ):
                        raise LedgerInvariantError("NULL candidate parent lineage drift")
                attempts = connection.execute(
                    "SELECT * FROM attempts WHERE candidate_id = ? ORDER BY attempt_number",
                    (candidate["candidate_id"],),
                ).fetchall()
                if [int(row["attempt_number"]) for row in attempts] != list(
                    range(1, len(attempts) + 1)
                ):
                    raise LedgerInvariantError("candidate attempt numbers are not contiguous")
                active = [row for row in attempts if row["status"] in {"QUEUED", "RUNNING"}]
                if len(active) > 1:
                    raise LedgerInvariantError("candidate has more than one active attempt")
                if candidate["status"] in {"REGISTERED", "SCREENED_OUT"}:
                    completed = [row for row in attempts if row["status"] == "COMPLETED"]
                    if len(completed) != 1 or candidate["result_artifact_id"] is None:
                        raise LedgerInvariantError("terminal evaluated candidate lacks one result")
                    evaluation = json.loads(candidate["evaluation_json"])
                    evaluation_sha256 = canonical_sha256(evaluation)
                    completed_attempt = completed[0]
                    if (
                        _canonical_text(evaluation) != candidate["evaluation_json"]
                        or evaluation_sha256 != candidate["evaluation_sha256"]
                        or completed_attempt["evaluation_json"] != candidate["evaluation_json"]
                        or completed_attempt["evaluation_sha256"] != evaluation_sha256
                        or completed_attempt["result_artifact_id"]
                        != candidate["result_artifact_id"]
                    ):
                        raise LedgerInvariantError("candidate/attempt evaluation identity drift")
                    artifact = connection.execute(
                        "SELECT * FROM artifacts WHERE artifact_id = ?",
                        (candidate["result_artifact_id"],),
                    ).fetchone()
                    if artifact is None:
                        raise LedgerInvariantError("terminal candidate artifact is missing")
                    artifact_payload = json.loads(artifact["canonical_json"])
                    expected_artifact = {
                        "artifact_schema": "systematic_fx.m0a_candidate_result.v1",
                        "attempt_key": completed_attempt["attempt_key"],
                        "attempt_number": completed_attempt["attempt_number"],
                        "candidate_sha256": candidate["candidate_sha256"],
                        "epoch_id": epoch_id,
                        "evaluation": evaluation,
                    }
                    if artifact_payload != expected_artifact:
                        raise LedgerInvariantError("candidate result artifact lineage drift")
                if candidate["status"] == "REGISTERED" and candidate["registered_at_us"] is None:
                    raise LedgerInvariantError("REGISTERED candidate lacks registered_at")
                if candidate["candidate_kind"] == "NULL" and candidate["status"] == "REGISTERED":
                    raise LedgerInvariantError("null-control candidate was REGISTERED")
            if kind_counts["REAL"] > int(epoch["real_candidate_budget"]):
                raise LedgerInvariantError("real candidate budget exceeded")
            if kind_counts["NULL"] > int(epoch["null_candidate_budget"]):
                raise LedgerInvariantError("null candidate budget exceeded")

            artifacts = connection.execute(
                "SELECT * FROM artifacts WHERE epoch_id = ? ORDER BY artifact_id",
                (epoch_id,),
            ).fetchall()
            for row in artifacts:
                payload = json.loads(row["canonical_json"])
                expected = _canonical_text(payload)
                content = expected.encode("utf-8") + b"\n"
                if expected != row["canonical_json"]:
                    raise LedgerInvariantError("artifact JSON is not canonical")
                if hashlib.sha256(content).hexdigest() != row["artifact_sha256"]:
                    raise LedgerInvariantError("artifact DB hash drift")
                if len(content) != int(row["byte_size"]):
                    raise LedgerInvariantError("artifact DB byte-size drift")
                path = Path(row["path"])
                if path.read_bytes() != content or path.is_symlink():
                    raise LedgerInvariantError("artifact file bytes drift")

            events = connection.execute(
                "SELECT * FROM events WHERE epoch_id = ? ORDER BY event_sequence",
                (epoch_id,),
            ).fetchall()
            predecessor: str | None = None
            for sequence, row in enumerate(events, start=1):
                if int(row["event_sequence"]) != sequence:
                    raise LedgerInvariantError("event sequence has a gap")
                payload = json.loads(row["payload_json"])
                if _canonical_text(payload) != row["payload_json"]:
                    raise LedgerInvariantError("event payload JSON is not canonical")
                payload_sha = canonical_sha256(payload)
                if payload_sha != row["payload_sha256"]:
                    raise LedgerInvariantError("event payload hash drift")
                if row["predecessor_event_sha256"] != predecessor:
                    raise LedgerInvariantError("event predecessor chain drift")
                expected_event = self._event_sha256(
                    epoch_id=epoch_id,
                    sequence=sequence,
                    event_type=str(row["event_type"]),
                    payload_sha256=payload_sha,
                    predecessor_event_sha256=predecessor,
                    candidate_id=row["candidate_id"],
                    attempt_id=row["attempt_id"],
                    created_at_us=int(row["created_at_us"]),
                )
                if expected_event != row["event_sha256"]:
                    raise LedgerInvariantError("event hash chain drift")
                predecessor = expected_event
            attempt_count = connection.execute(
                "SELECT count(*) AS count FROM attempts WHERE epoch_id = ?", (epoch_id,)
            ).fetchone()["count"]
        return InvariantReport(
            epoch_id,
            len(candidates),
            int(attempt_count),
            len(artifacts),
            len(events),
            predecessor,
        )

    verify = verify_invariants

    def _insert_attempt(
        self,
        connection: sqlite3.Connection,
        *,
        epoch_id: str,
        candidate_id: int,
        candidate_sha256: str,
        attempt_number: int,
        now_us: int,
    ) -> int:
        attempt_key = canonical_sha256(
            {
                "attempt_number": attempt_number,
                "candidate_sha256": candidate_sha256,
                "epoch_id": epoch_id,
            }
        )
        cursor = connection.execute(
            """
            INSERT INTO attempts
                (epoch_id, candidate_id, attempt_number, attempt_key, queued_at_us)
            VALUES (?, ?, ?, ?, ?)
            """,
            (epoch_id, candidate_id, attempt_number, attempt_key, now_us),
        )
        return int(cursor.lastrowid)

    @staticmethod
    def _lease_from_row(
        row: sqlite3.Row,
        *,
        owner: str,
        expires_us: int,
        resumed: bool,
    ) -> AttemptLease:
        return AttemptLease(
            epoch_id=str(row["epoch_id"]),
            candidate_id=int(row["candidate_id"]),
            candidate_sha256=str(row["candidate_sha256"]),
            candidate_kind=str(row["candidate_kind"]),  # type: ignore[arg-type]
            candidate_payload=json.loads(row["candidate_json"]),
            attempt_id=int(row["attempt_id"]),
            attempt_number=int(row["attempt_number"]),
            attempt_key=str(row["attempt_key"]),
            lease_owner=owner,
            lease_expires_at=_from_timestamp_us(expires_us),
            resumed=resumed,
        )

    @staticmethod
    def _owned_running_attempt(
        connection: sqlite3.Connection,
        *,
        lease: AttemptLease,
        now_us: int,
        require_unexpired: bool,
    ) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT attempt.*, candidate.candidate_sha256, candidate.candidate_kind,
                   candidate.candidate_json
            FROM attempts AS attempt
            JOIN candidates AS candidate ON candidate.candidate_id = attempt.candidate_id
            WHERE attempt.attempt_id = ?
            """,
            (lease.attempt_id,),
        ).fetchone()
        if row is None:
            raise LedgerStateError(f"attempt {lease.attempt_id} does not exist")
        if (
            row["epoch_id"] != lease.epoch_id
            or int(row["candidate_id"]) != lease.candidate_id
            or row["candidate_sha256"] != lease.candidate_sha256
            or row["attempt_key"] != lease.attempt_key
            or row["status"] != "RUNNING"
            or row["lease_owner"] != lease.lease_owner
        ):
            raise LedgerStateError("attempt lease identity or ownership drift")
        if require_unexpired and int(row["lease_expires_at_us"] or 0) <= now_us:
            raise LedgerStateError("attempt lease expired before terminal commit")
        return row

    @staticmethod
    def _verify_artifact(artifact: LocalArtifact) -> None:
        content = artifact.canonical_json.encode("utf-8") + b"\n"
        if (
            hashlib.sha256(content).hexdigest() != artifact.artifact_sha256
            or len(content) != artifact.byte_size
            or artifact.path.read_bytes() != content
            or artifact.path.is_symlink()
        ):
            raise LedgerInvariantError("local result artifact differs from its canonical identity")

    @staticmethod
    def _ensure_artifact(
        connection: sqlite3.Connection,
        *,
        lease: AttemptLease,
        artifact: LocalArtifact,
        now_us: int,
    ) -> int:
        connection.execute(
            """
            INSERT OR IGNORE INTO artifacts
                (epoch_id, candidate_id, attempt_id, artifact_type,
                 artifact_sha256, byte_size, path, canonical_json, created_at_us)
            VALUES (?, ?, ?, 'CANDIDATE_RESULT', ?, ?, ?, ?, ?)
            """,
            (
                lease.epoch_id,
                lease.candidate_id,
                lease.attempt_id,
                artifact.artifact_sha256,
                artifact.byte_size,
                str(artifact.path),
                artifact.canonical_json,
                now_us,
            ),
        )
        rows = connection.execute(
            "SELECT * FROM artifacts WHERE artifact_sha256 = ? OR path = ?",
            (artifact.artifact_sha256, str(artifact.path)),
        ).fetchall()
        if len(rows) != 1:
            raise LedgerInvariantError("artifact hash and path do not resolve to one row")
        row = rows[0]
        expected = {
            "artifact_sha256": artifact.artifact_sha256,
            "artifact_type": "CANDIDATE_RESULT",
            "attempt_id": lease.attempt_id,
            "byte_size": artifact.byte_size,
            "candidate_id": lease.candidate_id,
            "canonical_json": artifact.canonical_json,
            "epoch_id": lease.epoch_id,
            "path": str(artifact.path),
        }
        mismatches = [key for key, value in expected.items() if row[key] != value]
        if mismatches:
            raise LedgerInvariantError(
                "artifact immutable identity drift: " + ", ".join(sorted(mismatches))
            )
        return int(row["artifact_id"])

    def _complete_epoch_if_idle(
        self,
        connection: sqlite3.Connection,
        *,
        epoch_id: str,
        now_us: int,
    ) -> bool:
        epoch = connection.execute(
            "SELECT status, generation_complete FROM epochs WHERE epoch_id = ?", (epoch_id,)
        ).fetchone()
        if epoch is None or epoch["status"] in {"COMPLETED", "HALTED"}:
            return False
        active = connection.execute(
            "SELECT count(*) AS count FROM attempts "
            "WHERE epoch_id = ? AND status IN ('QUEUED', 'RUNNING')",
            (epoch_id,),
        ).fetchone()["count"]
        if epoch["generation_complete"] and int(active) == 0:
            connection.execute(
                "UPDATE epochs SET status = 'COMPLETED', completed_at_us = ? WHERE epoch_id = ?",
                (now_us, epoch_id),
            )
            self._append_event(
                connection,
                epoch_id=epoch_id,
                event_type="EPOCH_COMPLETED",
                payload={},
                created_at_us=now_us,
            )
            return True
        return False

    def _append_event(
        self,
        connection: sqlite3.Connection,
        *,
        epoch_id: str,
        event_type: str,
        payload: object,
        created_at_us: int,
        candidate_id: int | None = None,
        attempt_id: int | None = None,
    ) -> str:
        prior = connection.execute(
            "SELECT event_sequence, event_sha256 FROM events WHERE epoch_id = ? "
            "ORDER BY event_sequence DESC LIMIT 1",
            (epoch_id,),
        ).fetchone()
        sequence = int(prior["event_sequence"]) + 1 if prior is not None else 1
        predecessor = str(prior["event_sha256"]) if prior is not None else None
        payload_json = _canonical_text(payload)
        payload_sha256 = canonical_sha256(payload)
        event_sha256 = self._event_sha256(
            epoch_id=epoch_id,
            sequence=sequence,
            event_type=event_type,
            payload_sha256=payload_sha256,
            predecessor_event_sha256=predecessor,
            candidate_id=candidate_id,
            attempt_id=attempt_id,
            created_at_us=created_at_us,
        )
        connection.execute(
            """
            INSERT INTO events
                (epoch_id, event_sequence, candidate_id, attempt_id, event_type,
                 payload_json, payload_sha256, predecessor_event_sha256,
                 event_sha256, created_at_us)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                epoch_id,
                sequence,
                candidate_id,
                attempt_id,
                event_type,
                payload_json,
                payload_sha256,
                predecessor,
                event_sha256,
                created_at_us,
            ),
        )
        return event_sha256

    @staticmethod
    def _event_sha256(
        *,
        epoch_id: str,
        sequence: int,
        event_type: str,
        payload_sha256: str,
        predecessor_event_sha256: str | None,
        candidate_id: int | None,
        attempt_id: int | None,
        created_at_us: int,
    ) -> str:
        return canonical_sha256(
            {
                "attempt_id": attempt_id,
                "candidate_id": candidate_id,
                "created_at_us": created_at_us,
                "epoch_id": epoch_id,
                "event_sequence": sequence,
                "event_type": event_type,
                "payload_sha256": payload_sha256,
                "predecessor_event_sha256": predecessor_event_sha256,
            }
        )


_SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS schema_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;
INSERT OR IGNORE INTO schema_metadata (key, value)
VALUES ('schema_version', '{_SCHEMA_VERSION}');

CREATE TABLE IF NOT EXISTS epochs (
    epoch_id TEXT PRIMARY KEY,
    epoch_hash TEXT NOT NULL UNIQUE,
    identity_sha256 TEXT NOT NULL UNIQUE,
    config_file_sha256 TEXT NOT NULL,
    config_json TEXT NOT NULL,
    dataset_version TEXT NOT NULL,
    dataset_sha256 TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    label_version TEXT NOT NULL,
    execution_version TEXT NOT NULL,
    code_version TEXT NOT NULL,
    family_id TEXT NOT NULL,
    real_candidate_budget INTEGER NOT NULL,
    null_candidate_budget INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'QUEUED'
        CHECK (status IN ('QUEUED', 'RUNNING', 'COMPLETED', 'HALTED')),
    generation_complete INTEGER NOT NULL DEFAULT 0 CHECK (generation_complete IN (0, 1)),
    consecutive_system_errors INTEGER NOT NULL DEFAULT 0
        CHECK (consecutive_system_errors >= 0),
    system_error_threshold INTEGER NOT NULL CHECK (system_error_threshold > 0),
    halted_reason TEXT,
    created_at_us INTEGER NOT NULL,
    completed_at_us INTEGER,
    CHECK (length(epoch_hash) = 64 AND epoch_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK (length(identity_sha256) = 64 AND identity_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK (length(config_file_sha256) = 64 AND config_file_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK (length(dataset_sha256) = 64 AND dataset_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK (real_candidate_budget > 0 AND null_candidate_budget > 0),
    CHECK ((status = 'HALTED') = (halted_reason IS NOT NULL)),
    CHECK (status NOT IN ('COMPLETED', 'HALTED') OR completed_at_us IS NOT NULL)
) STRICT;

CREATE TABLE IF NOT EXISTS candidates (
    candidate_id INTEGER PRIMARY KEY,
    epoch_id TEXT NOT NULL REFERENCES epochs(epoch_id),
    candidate_sha256 TEXT NOT NULL,
    candidate_kind TEXT NOT NULL CHECK (candidate_kind IN ('REAL', 'NULL')),
    family_id TEXT NOT NULL,
    candidate_ordinal INTEGER NOT NULL CHECK (candidate_ordinal > 0),
    parent_candidate_id INTEGER REFERENCES candidates(candidate_id),
    candidate_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'QUEUED' CHECK (status IN (
        'QUEUED', 'RUNNING', 'SCREENED_OUT', 'SEQUENTIAL_TEST', 'WALK_FORWARD',
        'REGISTERED', 'FAILED', 'CRASHED'
    )),
    result_artifact_id INTEGER,
    evaluation_json TEXT,
    evaluation_sha256 TEXT,
    raw_event_metrics_json TEXT,
    flat_only_metrics_json TEXT,
    sequential_metrics_json TEXT,
    stressed_cost_metrics_json TEXT,
    fold_metrics_json TEXT,
    controls_json TEXT,
    created_at_us INTEGER NOT NULL,
    registered_at_us INTEGER,
    UNIQUE (epoch_id, candidate_sha256),
    UNIQUE (epoch_id, candidate_kind, candidate_ordinal),
    CHECK (length(candidate_sha256) = 64 AND candidate_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK (evaluation_sha256 IS NULL OR
           (length(evaluation_sha256) = 64 AND evaluation_sha256 NOT GLOB '*[^0-9a-f]*')),
    CHECK ((status = 'REGISTERED') = (registered_at_us IS NOT NULL)),
    CHECK (candidate_kind <> 'NULL' OR status <> 'REGISTERED'),
    CHECK ((candidate_kind = 'REAL') = (parent_candidate_id IS NULL))
) STRICT;

CREATE TABLE IF NOT EXISTS attempts (
    attempt_id INTEGER PRIMARY KEY,
    epoch_id TEXT NOT NULL REFERENCES epochs(epoch_id),
    candidate_id INTEGER NOT NULL REFERENCES candidates(candidate_id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    attempt_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'QUEUED'
        CHECK (status IN ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'CRASHED')),
    lease_owner TEXT,
    lease_expires_at_us INTEGER,
    heartbeat_at_us INTEGER,
    result_artifact_id INTEGER,
    evaluation_json TEXT,
    evaluation_sha256 TEXT,
    queued_at_us INTEGER NOT NULL,
    started_at_us INTEGER,
    finished_at_us INTEGER,
    error_class TEXT,
    error_message TEXT,
    system_error INTEGER NOT NULL DEFAULT 0 CHECK (system_error IN (0, 1)),
    UNIQUE (candidate_id, attempt_number),
    CHECK (length(attempt_key) = 64 AND attempt_key NOT GLOB '*[^0-9a-f]*'),
    CHECK ((status = 'RUNNING') =
           (lease_owner IS NOT NULL AND lease_expires_at_us IS NOT NULL AND
            heartbeat_at_us IS NOT NULL)),
    CHECK (status <> 'RUNNING' OR started_at_us IS NOT NULL),
    CHECK (status NOT IN ('COMPLETED', 'FAILED', 'CRASHED') OR finished_at_us IS NOT NULL),
    CHECK (status <> 'COMPLETED' OR
           (result_artifact_id IS NOT NULL AND evaluation_sha256 IS NOT NULL)),
    CHECK (status NOT IN ('FAILED', 'CRASHED') OR
           (error_class IS NOT NULL AND error_message IS NOT NULL))
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS attempts_one_active
    ON attempts(candidate_id) WHERE status IN ('QUEUED', 'RUNNING');

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id INTEGER PRIMARY KEY,
    epoch_id TEXT NOT NULL REFERENCES epochs(epoch_id),
    candidate_id INTEGER NOT NULL REFERENCES candidates(candidate_id),
    attempt_id INTEGER NOT NULL UNIQUE REFERENCES attempts(attempt_id),
    artifact_type TEXT NOT NULL CHECK (artifact_type = 'CANDIDATE_RESULT'),
    artifact_sha256 TEXT NOT NULL UNIQUE,
    byte_size INTEGER NOT NULL CHECK (byte_size > 0),
    path TEXT NOT NULL UNIQUE,
    canonical_json TEXT NOT NULL,
    created_at_us INTEGER NOT NULL,
    CHECK (length(artifact_sha256) = 64 AND artifact_sha256 NOT GLOB '*[^0-9a-f]*')
) STRICT;

CREATE TABLE IF NOT EXISTS events (
    event_id INTEGER PRIMARY KEY,
    epoch_id TEXT NOT NULL REFERENCES epochs(epoch_id),
    event_sequence INTEGER NOT NULL CHECK (event_sequence > 0),
    candidate_id INTEGER REFERENCES candidates(candidate_id),
    attempt_id INTEGER REFERENCES attempts(attempt_id),
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    predecessor_event_sha256 TEXT,
    event_sha256 TEXT NOT NULL UNIQUE,
    created_at_us INTEGER NOT NULL,
    UNIQUE (epoch_id, event_sequence),
    CHECK (length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
    CHECK (predecessor_event_sha256 IS NULL OR
           (length(predecessor_event_sha256) = 64 AND
            predecessor_event_sha256 NOT GLOB '*[^0-9a-f]*')),
    CHECK (length(event_sha256) = 64 AND event_sha256 NOT GLOB '*[^0-9a-f]*')
) STRICT;

CREATE TRIGGER IF NOT EXISTS epochs_identity_immutable
BEFORE UPDATE ON epochs
WHEN NEW.epoch_id <> OLD.epoch_id
  OR NEW.epoch_hash <> OLD.epoch_hash
  OR NEW.identity_sha256 <> OLD.identity_sha256
  OR NEW.config_file_sha256 <> OLD.config_file_sha256
  OR NEW.config_json <> OLD.config_json
  OR NEW.dataset_version <> OLD.dataset_version
  OR NEW.dataset_sha256 <> OLD.dataset_sha256
  OR NEW.feature_version <> OLD.feature_version
  OR NEW.label_version <> OLD.label_version
  OR NEW.execution_version <> OLD.execution_version
  OR NEW.code_version <> OLD.code_version
  OR NEW.family_id <> OLD.family_id
  OR NEW.real_candidate_budget <> OLD.real_candidate_budget
  OR NEW.null_candidate_budget <> OLD.null_candidate_budget
  OR NEW.system_error_threshold <> OLD.system_error_threshold
  OR NEW.created_at_us <> OLD.created_at_us
BEGIN SELECT RAISE(ABORT, 'M0a epoch identity is immutable'); END;
CREATE TRIGGER IF NOT EXISTS epochs_append_preserved
BEFORE DELETE ON epochs BEGIN SELECT RAISE(ABORT, 'M0a epochs are append-preserved'); END;

CREATE TRIGGER IF NOT EXISTS candidates_budget_real
BEFORE INSERT ON candidates WHEN NEW.candidate_kind = 'REAL' AND (
    SELECT count(*) FROM candidates
    WHERE epoch_id = NEW.epoch_id AND candidate_kind = 'REAL'
) >= (SELECT real_candidate_budget FROM epochs WHERE epoch_id = NEW.epoch_id)
BEGIN SELECT RAISE(ABORT, 'M0a real candidate budget exhausted'); END;
CREATE TRIGGER IF NOT EXISTS candidates_budget_null
BEFORE INSERT ON candidates WHEN NEW.candidate_kind = 'NULL' AND (
    SELECT count(*) FROM candidates
    WHERE epoch_id = NEW.epoch_id AND candidate_kind = 'NULL'
) >= (SELECT null_candidate_budget FROM epochs WHERE epoch_id = NEW.epoch_id)
BEGIN SELECT RAISE(ABORT, 'M0a null candidate budget exhausted'); END;
CREATE TRIGGER IF NOT EXISTS candidates_null_parent_lineage
BEFORE INSERT ON candidates WHEN NEW.candidate_kind = 'NULL' AND NOT EXISTS (
    SELECT 1 FROM candidates AS parent
    WHERE parent.candidate_id = NEW.parent_candidate_id
      AND parent.epoch_id = NEW.epoch_id
      AND parent.candidate_kind = 'REAL'
      AND parent.family_id = NEW.family_id
      AND parent.created_at_us <= NEW.created_at_us
      AND parent.candidate_sha256 = json_extract(
          NEW.candidate_json, '$.parent_candidate_hash'
      )
)
BEGIN SELECT RAISE(ABORT, 'M0a NULL candidate parent lineage is invalid'); END;
CREATE TRIGGER IF NOT EXISTS candidates_identity_immutable
BEFORE UPDATE ON candidates
WHEN NEW.candidate_id <> OLD.candidate_id
  OR NEW.epoch_id <> OLD.epoch_id
  OR NEW.candidate_sha256 <> OLD.candidate_sha256
  OR NEW.candidate_kind <> OLD.candidate_kind
  OR NEW.family_id <> OLD.family_id
  OR NEW.candidate_ordinal <> OLD.candidate_ordinal
  OR NEW.parent_candidate_id IS NOT OLD.parent_candidate_id
  OR NEW.candidate_json <> OLD.candidate_json
  OR NEW.created_at_us <> OLD.created_at_us
BEGIN SELECT RAISE(ABORT, 'M0a candidate identity is immutable'); END;
CREATE TRIGGER IF NOT EXISTS candidates_lifecycle
BEFORE UPDATE OF status ON candidates
WHEN NOT (
    (OLD.status = 'QUEUED' AND NEW.status IN ('RUNNING', 'FAILED')) OR
    (OLD.status = 'RUNNING' AND NEW.status IN
        ('SCREENED_OUT', 'SEQUENTIAL_TEST', 'FAILED', 'CRASHED')) OR
    (OLD.status = 'SEQUENTIAL_TEST' AND NEW.status IN ('WALK_FORWARD', 'SCREENED_OUT')) OR
    (OLD.status = 'WALK_FORWARD' AND NEW.status IN ('REGISTERED', 'SCREENED_OUT')) OR
    (OLD.status = 'CRASHED' AND NEW.status IN ('QUEUED', 'FAILED'))
)
BEGIN SELECT RAISE(ABORT, 'invalid M0a candidate lifecycle transition'); END;
CREATE TRIGGER IF NOT EXISTS candidates_terminal_immutable
BEFORE UPDATE ON candidates
WHEN OLD.status IN ('SCREENED_OUT', 'REGISTERED', 'FAILED')
BEGIN SELECT RAISE(ABORT, 'terminal M0a candidates are immutable'); END;
CREATE TRIGGER IF NOT EXISTS candidates_append_preserved
BEFORE DELETE ON candidates
BEGIN SELECT RAISE(ABORT, 'M0a candidates are append-preserved'); END;

CREATE TRIGGER IF NOT EXISTS attempts_identity_immutable
BEFORE UPDATE ON attempts
WHEN NEW.attempt_id <> OLD.attempt_id
  OR NEW.epoch_id <> OLD.epoch_id
  OR NEW.candidate_id <> OLD.candidate_id
  OR NEW.attempt_number <> OLD.attempt_number
  OR NEW.attempt_key <> OLD.attempt_key
  OR NEW.queued_at_us <> OLD.queued_at_us
BEGIN SELECT RAISE(ABORT, 'M0a attempt identity is immutable'); END;
CREATE TRIGGER IF NOT EXISTS attempts_lifecycle
BEFORE UPDATE OF status ON attempts
WHEN NOT (
    (OLD.status = 'QUEUED' AND NEW.status IN ('RUNNING', 'FAILED')) OR
    (OLD.status = 'RUNNING' AND NEW.status IN ('COMPLETED', 'FAILED', 'CRASHED'))
)
BEGIN SELECT RAISE(ABORT, 'invalid M0a attempt lifecycle transition'); END;
CREATE TRIGGER IF NOT EXISTS attempts_terminal_immutable
BEFORE UPDATE ON attempts
WHEN OLD.status IN ('COMPLETED', 'FAILED', 'CRASHED')
BEGIN SELECT RAISE(ABORT, 'terminal M0a attempts are immutable'); END;
CREATE TRIGGER IF NOT EXISTS attempts_append_preserved
BEFORE DELETE ON attempts
BEGIN SELECT RAISE(ABORT, 'M0a attempts are append-preserved'); END;

CREATE TRIGGER IF NOT EXISTS artifacts_immutable_update
BEFORE UPDATE ON artifacts BEGIN SELECT RAISE(ABORT, 'M0a artifacts are immutable'); END;
CREATE TRIGGER IF NOT EXISTS artifacts_immutable_delete
BEFORE DELETE ON artifacts BEGIN SELECT RAISE(ABORT, 'M0a artifacts are append-preserved'); END;
CREATE TRIGGER IF NOT EXISTS events_immutable_update
BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT, 'M0a events are immutable'); END;
CREATE TRIGGER IF NOT EXISTS events_immutable_delete
BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT, 'M0a events are append-preserved'); END;
"""


def wait_for_lease_expiry(seconds: float) -> None:
    """Small explicit helper for integration harnesses; production uses heartbeats."""

    if seconds < 0:
        raise M0aLedgerError("lease wait cannot be negative")
    time.sleep(seconds)
