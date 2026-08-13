"""One finite, crash-replayable M0b worker control-plane cycle.

The runner claims only work that was already registered by the control plane.
Its lease capability is persisted in one owner-only file before the claim call,
so losing a response can only replay the same opaque token.  Candidate work is
then reopened by the byte identity returned by PostgreSQL and fully reconciled
against its signal and first-passage inputs before evaluation.
"""

from __future__ import annotations

import json
import os
import secrets
import stat
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, Self

from psycopg import ProgrammingError
from psycopg.conninfo import conninfo_to_dict

from systematic_fx.db.m0b_worker_registry import (
    M0bWorkerClaim,
    M0bWorkerRegistryError,
    claim_m0b_work,
    load_m0b_epoch_runtime_identity,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.m0b.first_passage_store import _fsync_directory, _sha256
from systematic_fx.research.m0b.materialize import _safe_root
from systematic_fx.research.m0b.worker import (
    CandidateWorkSpec,
    M0bWorkerError,
    WorkerAttempt,
    WorkerRunResult,
    load_candidate_work_artifact,
    run_candidate_work,
)
from systematic_fx.research.m0b.worker_db import (
    M0bCheckpointPublicationError,
    M0bTerminalPublicationError,
    PostgresWorkerObserver,
)
from systematic_fx.research.provenance import (
    ProvenanceError,
    build_code_snapshot,
    dependency_lock_sha256,
)

TOKEN_SCHEMA: Final = "systematic_fx.m0b_worker_lease_token.v1"
_LEASE_SECONDS: Final = 3600
_HEARTBEAT_SECONDS: Final = 60
_MAX_TOKEN_BYTES: Final = 8192
_TOKEN_KEYS: Final = {
    "artifact_schema",
    "claim",
    "database_identity_sha256",
    "epoch_key",
    "lease_token_sha256",
    "pending_failure",
    "worker_id",
}
_CLAIM_KEYS: Final = {
    "attempt_number",
    "attempt_status",
    "candidate_kind",
    "candidate_sha256",
    "epoch_sha256",
    "m0b_candidate_id",
    "lease_status",
    "research_run_attempt_id",
    "work_spec_byte_size",
    "work_spec_sha256",
}


class M0bRunnerError(M0bWorkerError):
    """A claimed worker cycle could not preserve its durable capability state."""


class M0bRuntimeCodeIdentityError(M0bRunnerError):
    """The deployed worker bytes differ from the claimed immutable precommitment."""


class M0bControlPlaneReplayError(M0bRunnerError):
    """A transient lease/control-plane publication needs exact token replay."""


@dataclass(frozen=True, slots=True)
class ClaimedWorkerCycleResult:
    status: str
    candidate_sha256: str | None
    research_run_attempt_id: int | None
    work_spec_sha256: str | None
    result: WorkerRunResult | None
    error: str | None

    def __post_init__(self) -> None:
        if self.status not in {"IDLE", "RUNNING", "COMPLETED", "FAILED"}:
            raise M0bRunnerError("claimed worker cycle status differs")


def _canonical_text(value: object, *, label: str, maximum: int = 128) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or value != value.strip()
        or len(value) > maximum
        or any(character in value for character in "\x00\r\n")
    ):
        raise M0bRunnerError(f"{label} is not a bounded canonical string")
    return value


def _token_path(
    root: Path,
    *,
    database_identity_sha256: str,
    epoch_key: str,
    worker_id: str,
) -> Path:
    identity = canonical_sha256(
        {
            "artifact_schema": "systematic_fx.m0b_worker_identity.v1",
            "database_identity_sha256": database_identity_sha256,
            "epoch_key": epoch_key,
            "worker_id": worker_id,
        }
    )
    return root / f"m0b-worker-lease-{identity}.json"


def _database_identity_sha256(database_url: str) -> str:
    """Bind durable tokens to a DB principal/endpoint without hashing secrets."""

    try:
        parsed = conninfo_to_dict(database_url)
    except ProgrammingError as error:
        raise M0bRunnerError("database_url is not valid PostgreSQL conninfo") from error
    identity = {
        key: parsed.get(key) for key in ("dbname", "host", "hostaddr", "port", "service", "user")
    }
    return canonical_sha256(
        {
            "artifact_schema": "systematic_fx.m0b_database_identity.v1",
            "connection": identity,
        }
    )


def _token_document(
    *, database_identity_sha256: str, epoch_key: str, worker_id: str
) -> dict[str, object]:
    return {
        "artifact_schema": TOKEN_SCHEMA,
        "claim": None,
        "database_identity_sha256": database_identity_sha256,
        "epoch_key": epoch_key,
        "lease_token_sha256": secrets.token_hex(32),
        "pending_failure": None,
        "worker_id": worker_id,
    }


def _validate_token(
    document: object,
    *,
    database_identity_sha256: str,
    epoch_key: str,
    worker_id: str,
) -> dict[str, object]:
    if (
        not isinstance(document, dict)
        or set(document) != _TOKEN_KEYS
        or document.get("artifact_schema") != TOKEN_SCHEMA
        or document.get("database_identity_sha256") != database_identity_sha256
        or document.get("epoch_key") != epoch_key
        or document.get("worker_id") != worker_id
    ):
        raise M0bRunnerError("durable lease token identity differs")
    _sha256(document.get("lease_token_sha256"), label="lease token SHA-256")
    _sha256(document.get("database_identity_sha256"), label="database identity SHA-256")
    claim = document.get("claim")
    if claim is not None:
        if not isinstance(claim, dict) or set(claim) != _CLAIM_KEYS:
            raise M0bRunnerError("durable claim identity differs")
        for key in ("candidate_sha256", "epoch_sha256", "work_spec_sha256"):
            _sha256(claim.get(key), label=f"claim {key}")
        for key in (
            "attempt_number",
            "m0b_candidate_id",
            "research_run_attempt_id",
            "work_spec_byte_size",
        ):
            value = claim.get(key)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise M0bRunnerError(f"claim {key} must be a positive integer")
        if claim.get("candidate_kind") not in {"REAL", "NULL"}:
            raise M0bRunnerError("claim candidate_kind differs")
        if claim.get("attempt_status") not in {"RUNNING", "SUCCEEDED", "FAILED"}:
            raise M0bRunnerError("claim attempt_status differs")
        if claim.get("lease_status") not in {"ACTIVE", "RELEASED"}:
            raise M0bRunnerError("claim lease_status differs")
    pending_failure = document.get("pending_failure")
    if pending_failure is not None and (
        not isinstance(pending_failure, dict)
        or set(pending_failure) != {"error_message", "retryable"}
        or not isinstance(pending_failure.get("error_message"), str)
        or not str(pending_failure["error_message"]).strip()
        or len(str(pending_failure["error_message"])) > 4000
        or not isinstance(pending_failure.get("retryable"), bool)
        or claim is None
    ):
        raise M0bRunnerError("durable pending failure identity differs")
    return document


def _read_token(
    path: Path,
    *,
    database_identity_sha256: str,
    epoch_key: str,
    worker_id: str,
) -> dict[str, object]:
    details = path.lstat()
    if (
        not stat.S_ISREG(details.st_mode)
        or stat.S_IMODE(details.st_mode) != 0o600
        or details.st_uid != os.getuid()
        or details.st_size <= 0
        or details.st_size > _MAX_TOKEN_BYTES
    ):
        raise M0bRunnerError("durable lease token must be an owner-only regular file")
    payload = path.read_bytes()
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise M0bRunnerError("durable lease token is invalid JSON") from error
    if not isinstance(document, dict) or canonical_json_bytes(document) != payload:
        raise M0bRunnerError("durable lease token is not canonical")
    return _validate_token(
        document,
        database_identity_sha256=database_identity_sha256,
        epoch_key=epoch_key,
        worker_id=worker_id,
    )


def _create_token(path: Path, document: dict[str, object]) -> None:
    payload = canonical_json_bytes(document)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)
    _fsync_directory(path.parent)


def _replace_token(path: Path, document: dict[str, object]) -> None:
    payload = canonical_json_bytes(document)
    descriptor, temporary_name = tempfile.mkstemp(prefix=".m0b-lease-", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _load_or_create_token(
    root: Path,
    *,
    database_identity_sha256: str,
    epoch_key: str,
    worker_id: str,
) -> tuple[Path, dict[str, object]]:
    path = _token_path(
        root,
        database_identity_sha256=database_identity_sha256,
        epoch_key=epoch_key,
        worker_id=worker_id,
    )
    try:
        _create_token(
            path,
            _token_document(
                database_identity_sha256=database_identity_sha256,
                epoch_key=epoch_key,
                worker_id=worker_id,
            ),
        )
    except FileExistsError:
        pass
    return path, _read_token(
        path,
        database_identity_sha256=database_identity_sha256,
        epoch_key=epoch_key,
        worker_id=worker_id,
    )


def _claim_dict(claim: M0bWorkerClaim) -> dict[str, object]:
    if canonical_sha256(claim.canonical_candidate) != claim.candidate_sha256:
        raise M0bRunnerError("claimed canonical candidate hash differs")
    return {
        "attempt_number": claim.attempt_number,
        "attempt_status": claim.attempt_status,
        "candidate_kind": claim.candidate_kind,
        "candidate_sha256": claim.candidate_sha256,
        "epoch_sha256": claim.epoch_sha256,
        "lease_status": claim.lease_status,
        "m0b_candidate_id": claim.m0b_candidate_id,
        "research_run_attempt_id": claim.research_run_attempt_id,
        "work_spec_byte_size": claim.work_spec_byte_size,
        "work_spec_sha256": claim.work_spec_sha256,
    }


def _same_claim_identity(left: dict[str, object], right: dict[str, object]) -> bool:
    return all(
        left.get(key) == right.get(key) for key in _CLAIM_KEYS - {"attempt_status", "lease_status"}
    )


def _lease_expired(claim: M0bWorkerClaim) -> bool:
    leased_until = claim.leased_until
    if not isinstance(leased_until, datetime) or leased_until.tzinfo is None:
        raise M0bRunnerError("claimed lease expiry is not timezone-aware")
    return leased_until <= datetime.now(UTC)


class _LeaseHeartbeat:
    """Refresh one active lease without widening the worker API surface."""

    def __init__(
        self,
        database_url: str,
        *,
        epoch_key: str,
        worker_id: str,
        lease_token_sha256: str,
        expected_claim: dict[str, object],
    ) -> None:
        self._database_url = database_url
        self._epoch_key = epoch_key
        self._worker_id = worker_id
        self._lease_token_sha256 = lease_token_sha256
        self._expected_claim = expected_claim
        self._stop = threading.Event()
        self._error: BaseException | None = None
        self._thread = threading.Thread(
            target=self._run,
            name="m0b-lease-heartbeat",
            daemon=True,
        )

    def _run(self) -> None:
        while not self._stop.wait(_HEARTBEAT_SECONDS):
            try:
                observed = claim_m0b_work(
                    self._database_url,
                    epoch_key=self._epoch_key,
                    worker_id=self._worker_id,
                    lease_token_sha256=self._lease_token_sha256,
                    lease_seconds=_LEASE_SECONDS,
                )
                if observed is None or not _same_claim_identity(
                    self._expected_claim, _claim_dict(observed)
                ):
                    raise M0bRunnerError("heartbeat claim identity differs")
                if (observed.attempt_status, observed.lease_status) not in {
                    ("RUNNING", "ACTIVE"),
                    ("SUCCEEDED", "RELEASED"),
                    ("FAILED", "RELEASED"),
                }:
                    raise M0bRunnerError("heartbeat claim lifecycle differs")
                if observed.lease_status == "RELEASED":
                    return
            except (M0bWorkerError, M0bWorkerRegistryError, OSError, ValueError) as error:
                self._error = error
                return

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._stop.set()
        self._thread.join()
        if self._error is not None and _exc[0] is None:
            raise M0bControlPlaneReplayError("lease heartbeat failed") from self._error


def _fresh_token(
    path: Path,
    *,
    database_identity_sha256: str,
    epoch_key: str,
    worker_id: str,
) -> dict[str, object]:
    document = _token_document(
        database_identity_sha256=database_identity_sha256,
        epoch_key=epoch_key,
        worker_id=worker_id,
    )
    _replace_token(path, document)
    return document


def _consume_token(path: Path) -> None:
    path.unlink()
    _fsync_directory(path.parent)


def _attempt(claim: dict[str, object]) -> WorkerAttempt:
    return WorkerAttempt(
        m0b_candidate_id=int(claim["m0b_candidate_id"]),
        research_run_attempt_id=int(claim["research_run_attempt_id"]),
    )


def _runtime_project_root() -> Path:
    """Locate the governed source workspace without accepting runtime authority."""

    try:
        module = Path(__file__).resolve(strict=True)
        for candidate in module.parents:
            if (
                (candidate / "pyproject.toml").is_file()
                and (candidate / "uv.lock").is_file()
                and (candidate / "src" / "systematic_fx").is_dir()
                and (candidate / ".git").exists()
            ):
                return candidate
    except OSError as error:
        raise M0bRuntimeCodeIdentityError(
            "governed source workspace cannot be inspected"
        ) from error
    raise M0bRuntimeCodeIdentityError("governed source workspace cannot be located")


def _runtime_git_head(project_root: Path) -> str:
    try:
        completed = subprocess.run(
            ("git", "-C", os.fspath(project_root), "rev-parse", "--verify", "HEAD"),
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise M0bRuntimeCodeIdentityError("governed Git HEAD cannot be resolved") from error
    commit = completed.stdout.strip()
    if (
        completed.returncode != 0
        or len(commit) not in {40, 64}
        or any(character not in "0123456789abcdef" for character in commit)
    ):
        raise M0bRuntimeCodeIdentityError("governed Git HEAD is not one full lowercase object ID")
    return commit


def _observed_runtime_code_identity() -> tuple[str, str, str]:
    project_root = _runtime_project_root()
    commit = _runtime_git_head(project_root)
    try:
        observed = build_code_snapshot(project_root, code_commit=commit)
        dependency = dependency_lock_sha256(project_root)
    except (OSError, ProvenanceError) as error:
        raise M0bRuntimeCodeIdentityError(
            "governed code snapshot cannot be reconstructed"
        ) from error
    return commit, observed.sha256, dependency


def _verify_runtime_code_identity(work: object) -> None:
    """Fail closed unless current executable/config bytes equal the precommitment."""

    if not isinstance(work, CandidateWorkSpec):
        raise M0bRuntimeCodeIdentityError("runtime code preflight requires CandidateWorkSpec")
    _commit, snapshot_sha256, _dependency = _observed_runtime_code_identity()
    if snapshot_sha256 != work.code_snapshot_sha256:
        raise M0bRuntimeCodeIdentityError("runtime code snapshot differs from CandidateWork")


def run_claimed_worker_cycle(
    database_url: str,
    *,
    epoch_key: str,
    worker_id: str,
    worker_root: str | Path,
) -> ClaimedWorkerCycleResult:
    """Claim and advance at most one pre-registered candidate to completion.

    There is no candidate-generation, holdout, promotion, LLM, or runtime search
    configuration surface here.  All evaluation bounds and assumptions come
    from the immutable CandidateWork document returned by byte identity.
    """

    database_url = _canonical_text(database_url, label="database_url", maximum=8192)
    epoch_key = _canonical_text(epoch_key, label="epoch_key")
    worker_id = _canonical_text(worker_id, label="worker_id")
    epoch_runtime = load_m0b_epoch_runtime_identity(database_url, epoch_key=epoch_key)
    observed_commit, observed_snapshot, observed_dependency = _observed_runtime_code_identity()
    if (
        epoch_runtime.epoch_key != epoch_key
        or epoch_runtime.code_commit != observed_commit
        or epoch_runtime.code_snapshot_sha256 != observed_snapshot
        or epoch_runtime.dependency_lock_sha256 != observed_dependency
    ):
        raise M0bRuntimeCodeIdentityError("deployed runtime differs from active epoch before claim")
    database_identity_sha256 = _database_identity_sha256(database_url)
    root = _safe_root(worker_root, label="m0b_worker_root", create=True)
    token_path, token = _load_or_create_token(
        root,
        database_identity_sha256=database_identity_sha256,
        epoch_key=epoch_key,
        worker_id=worker_id,
    )

    # Reconcile the token with PostgreSQL even when a prior claim response was
    # already persisted.  The capability returns the exact prior identity for
    # ACTIVE or RELEASED leases, and renews ACTIVE work even after wall-clock
    # expiry when the exact durable token is replayed.  The defensive expired
    # branch supports an older/discordant server response by rotating once so
    # a fresh token can invoke bounded stale-attempt recovery.
    for recovery_round in range(2):
        lease_token = str(token["lease_token_sha256"])
        claimed = claim_m0b_work(
            database_url,
            epoch_key=epoch_key,
            worker_id=worker_id,
            lease_token_sha256=lease_token,
            lease_seconds=_LEASE_SECONDS,
        )
        if claimed is None:
            if token["claim"] is not None:
                raise M0bRunnerError("persisted claim disappeared from PostgreSQL")
            _consume_token(token_path)
            return ClaimedWorkerCycleResult("IDLE", None, None, None, None, None)
        observed_claim = _claim_dict(claimed)
        prior_claim = token["claim"]
        if isinstance(prior_claim, dict) and not _same_claim_identity(prior_claim, observed_claim):
            raise M0bRunnerError("persisted and PostgreSQL claim identities differ")
        if (
            claimed.lease_status == "ACTIVE"
            and claimed.attempt_status == "RUNNING"
            and _lease_expired(claimed)
        ):
            if recovery_round != 0:
                raise M0bRunnerError("replacement worker lease was already expired")
            token = _fresh_token(
                token_path,
                database_identity_sha256=database_identity_sha256,
                epoch_key=epoch_key,
                worker_id=worker_id,
            )
            continue
        token = {**token, "claim": observed_claim}
        _replace_token(token_path, token)
        break
    claim_value = token["claim"]
    if not isinstance(claim_value, dict):
        raise M0bRunnerError("durable claim is not an object")
    claim = claim_value
    attempt = _attempt(claim)
    observer = PostgresWorkerObserver(database_url, attempt, lease_token)

    pending_failure = token["pending_failure"]
    if isinstance(pending_failure, dict):
        if (claim["attempt_status"], claim["lease_status"]) not in {
            ("RUNNING", "ACTIVE"),
            ("FAILED", "RELEASED"),
        }:
            raise M0bRunnerError("pending failure and PostgreSQL lifecycle differ")
        observer.failure_published(
            error_message=str(pending_failure["error_message"]),
            retryable=bool(pending_failure["retryable"]),
        )
        _consume_token(token_path)
        return ClaimedWorkerCycleResult(
            "FAILED",
            str(claim["candidate_sha256"]),
            attempt.research_run_attempt_id,
            str(claim["work_spec_sha256"]),
            None,
            str(pending_failure["error_message"]),
        )
    if (claim["attempt_status"], claim["lease_status"]) not in {
        ("RUNNING", "ACTIVE"),
        ("SUCCEEDED", "RELEASED"),
    }:
        raise M0bRunnerError("claimed attempt lifecycle cannot execute or replay")

    try:
        heartbeat_context = (
            _LeaseHeartbeat(
                database_url,
                epoch_key=epoch_key,
                worker_id=worker_id,
                lease_token_sha256=lease_token,
                expected_claim=claim,
            )
            if claim["lease_status"] == "ACTIVE"
            else None
        )

        def execute() -> WorkerRunResult:
            work_path = root / f"candidate-work-{claim['work_spec_sha256']}.json"
            artifact = load_candidate_work_artifact(work_path, reconcile_inputs=True)
            work = artifact.work
            if (
                artifact.content_sha256 != claim["work_spec_sha256"]
                or artifact.byte_size != claim["work_spec_byte_size"]
                or work.epoch_sha256 != claim["epoch_sha256"]
                or work.candidate_sha256 != claim["candidate_sha256"]
                or work.candidate_kind != claim["candidate_kind"]
            ):
                raise M0bRunnerError("claimed CandidateWork byte or semantic identity differs")
            _verify_runtime_code_identity(work)
            return run_candidate_work(
                work,
                attempt,
                first_passage_manifest=(
                    root / f"first-passage-store-{work.first_passage_store_sha256}.json"
                ),
                worker_root=root,
                observer=observer,
            )

        if heartbeat_context is None:
            result = execute()
        else:
            with heartbeat_context:
                result = execute()
    except (
        M0bCheckpointPublicationError,
        M0bControlPlaneReplayError,
        M0bRuntimeCodeIdentityError,
        M0bTerminalPublicationError,
    ):
        # A worker deployed from the wrong governed snapshot must not consume a
        # candidate retry.  Likewise, a terminal API outage after the complete
        # checkpoint must replay that exact result, never convert it to a
        # failed candidate.  Checkpoint response ambiguity and heartbeat
        # faults likewise preserve the same durable capability and local
        # cursor for reconciliation on the next cycle.
        raise
    except (M0bWorkerError, M0bWorkerRegistryError, OSError, ValueError) as error:
        error_message = f"{type(error).__name__}: {error}"[:4000]
        failure = {"error_message": error_message, "retryable": True}
        token = {**token, "pending_failure": failure}
        _replace_token(token_path, token)
        try:
            observer.failure_published(error_message=error_message, retryable=True)
        except (M0bWorkerError, M0bWorkerRegistryError, OSError, ValueError) as failure_error:
            raise M0bRunnerError(
                "claimed work failed and durable failure replay remains pending"
            ) from failure_error
        _consume_token(token_path)
        return ClaimedWorkerCycleResult(
            "FAILED",
            str(claim["candidate_sha256"]),
            attempt.research_run_attempt_id,
            str(claim["work_spec_sha256"]),
            None,
            error_message,
        )

    if result.complete:
        _consume_token(token_path)
    return ClaimedWorkerCycleResult(
        "COMPLETED" if result.complete else "RUNNING",
        str(claim["candidate_sha256"]),
        attempt.research_run_attempt_id,
        str(claim["work_spec_sha256"]),
        result,
        None,
    )
