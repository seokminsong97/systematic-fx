"""Adapter from durable M0b worker publications to least-privilege DB APIs."""

from __future__ import annotations

from dataclasses import dataclass

from systematic_fx.db.m0b_worker_registry import (
    M0bWorkerRegistryError,
    checkpoint_m0b_work,
    fail_m0b_work,
    terminalize_m0b_work,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.m0b.first_passage_store import _sha256
from systematic_fx.research.m0b.worker import (
    CHECKPOINT_SCHEMA,
    RESULT_SCHEMA,
    M0bWorkerError,
    WorkerAttempt,
)


class M0bTerminalPublicationError(M0bWorkerError):
    """A complete local/DB checkpoint still needs exact terminal replay."""


class M0bCheckpointPublicationError(M0bWorkerError):
    """A local checkpoint needs exact DB publication replay."""


@dataclass(frozen=True, slots=True)
class PostgresWorkerObserver:
    """Mirror local publications through migration-0030 capability functions.

    Both callbacks are safe to replay: the DB capability verifies the exact
    canonical identity on conflict.  PostgreSQL independently derives the
    terminal classification and this adapter rejects a local/DB disagreement.
    """

    database_url: str
    attempt: WorkerAttempt
    lease_token_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.database_url, str) or not self.database_url.strip():
            raise M0bWorkerError("PostgreSQL observer requires a database URL")
        if not isinstance(self.attempt, WorkerAttempt):
            raise M0bWorkerError("PostgreSQL observer attempt identity differs")
        _sha256(self.lease_token_sha256, label="lease token SHA-256")

    def checkpoint_published(
        self,
        *,
        checkpoint_sha256: str,
        checkpoint: dict[str, object],
        relative_uri: str,
    ) -> None:
        del relative_uri  # DB stores the canonical cursor, not local path authority.
        if (
            checkpoint.get("artifact_schema") != CHECKPOINT_SCHEMA
            or checkpoint.get("m0b_candidate_id") != self.attempt.m0b_candidate_id
            or checkpoint.get("research_run_attempt_id") != self.attempt.research_run_attempt_id
            or canonical_sha256(checkpoint) != checkpoint_sha256
            or not isinstance(checkpoint.get("state"), dict)
        ):
            raise M0bWorkerError("published checkpoint differs from PostgreSQL identity")
        try:
            _, observed_sha256 = checkpoint_m0b_work(
                self.database_url,
                candidate_id=self.attempt.m0b_candidate_id,
                attempt_id=self.attempt.research_run_attempt_id,
                lease_token_sha256=self.lease_token_sha256,
                checkpoint_sequence=int(checkpoint["checkpoint_sequence"]),
                predecessor_sha256=checkpoint["predecessor_sha256"],  # type: ignore[arg-type]
                state=checkpoint["state"],  # type: ignore[arg-type]
            )
        except M0bWorkerRegistryError as error:
            raise M0bCheckpointPublicationError(
                "local checkpoint publication remains pending"
            ) from error
        if observed_sha256 != checkpoint_sha256:
            raise M0bCheckpointPublicationError("PostgreSQL checkpoint canonicalization differs")

    def result_published(
        self,
        *,
        result_sha256: str,
        result: dict[str, object],
        relative_uri: str,
    ) -> None:
        del relative_uri
        payload = canonical_json_bytes(result)
        if (
            result.get("artifact_schema") != RESULT_SCHEMA
            or canonical_sha256(result) != result_sha256
            or not isinstance(result.get("metrics"), dict)
            or result.get("classification") not in {"REGISTERED", "SCREENED_OUT"}
        ):
            raise M0bWorkerError("published result differs from PostgreSQL identity")
        try:
            terminal = terminalize_m0b_work(
                self.database_url,
                candidate_id=self.attempt.m0b_candidate_id,
                attempt_id=self.attempt.research_run_attempt_id,
                lease_token_sha256=self.lease_token_sha256,
                result_sha256=result_sha256,
                result_byte_size=len(payload),
                metrics=result["metrics"],  # type: ignore[arg-type]
            )
        except M0bWorkerRegistryError as error:
            raise M0bTerminalPublicationError(
                "complete checkpoint terminal publication remains pending"
            ) from error
        if terminal.classification != result["classification"]:
            raise M0bTerminalPublicationError(
                "PostgreSQL-derived classification differs from local result"
            )

    def failure_published(self, *, error_message: str, retryable: bool) -> None:
        """Durably close the bound attempt without affecting later jobs."""

        if not isinstance(error_message, str) or not error_message.strip():
            raise M0bWorkerError("worker failure message must be non-empty")
        if len(error_message) > 4000:
            raise M0bWorkerError("worker failure message exceeds the durable DB bound")
        if not isinstance(retryable, bool):
            raise M0bWorkerError("worker failure retryability must be boolean")
        fail_m0b_work(
            self.database_url,
            candidate_id=self.attempt.m0b_candidate_id,
            attempt_id=self.attempt.research_run_attempt_id,
            lease_token_sha256=self.lease_token_sha256,
            error_message=error_message,
            retryable=retryable,
        )
