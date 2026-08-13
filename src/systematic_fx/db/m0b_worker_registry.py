"""Client boundary for the least-privilege M0b worker capabilities."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.research.hypotheses import canonical_sha256


class M0bWorkerRegistryError(RuntimeError):
    """A worker capability rejected an invalid identity or lifecycle transition."""


@dataclass(frozen=True, slots=True)
class M0bEpochRuntimeIdentity:
    epoch_key: str
    code_commit: str
    code_snapshot_sha256: str
    dependency_lock_sha256: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.epoch_key, str)
            or not self.epoch_key.strip()
            or self.epoch_key != self.epoch_key.strip()
        ):
            raise M0bWorkerRegistryError("epoch runtime key is not canonical")
        if (
            not isinstance(self.code_commit, str)
            or len(self.code_commit) not in {40, 64}
            or any(character not in "0123456789abcdef" for character in self.code_commit)
        ):
            raise M0bWorkerRegistryError("epoch runtime code commit is invalid")
        for label, value in (
            ("code snapshot", self.code_snapshot_sha256),
            ("dependency lock", self.dependency_lock_sha256),
        ):
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise M0bWorkerRegistryError(f"epoch runtime {label} SHA-256 is invalid")


@dataclass(frozen=True, slots=True)
class M0bWorkerClaim:
    m0b_candidate_id: int
    research_run_attempt_id: int
    attempt_number: int
    candidate_sha256: str
    candidate_kind: str
    canonical_candidate: dict[str, object]
    epoch_sha256: str
    work_spec_sha256: str
    work_spec_byte_size: int
    attempt_status: str
    lease_status: str
    leased_until: object


@dataclass(frozen=True, slots=True)
class M0bWorkerTerminalResult:
    artifact_id: int
    classification: str
    registered_at: object | None


def _call(database_url: str, query: str, parameters: tuple[object, ...]) -> dict | None:
    try:
        with (
            psycopg.connect(database_url, row_factory=dict_row) as connection,
            connection.transaction(),
        ):
            return connection.execute(query, parameters).fetchone()
    except psycopg.Error as error:
        primary = error.diag.message_primary or error.__class__.__name__
        context = error.diag.context or ""
        raise M0bWorkerRegistryError(
            f"PostgreSQL M0b worker capability failed: {primary[:500]} {context[:1000]}".rstrip()
        ) from error


def load_m0b_epoch_runtime_identity(
    database_url: str,
    *,
    epoch_key: str,
) -> M0bEpochRuntimeIdentity:
    """Read only the active epoch's governed runtime identity before claiming."""

    row = _call(
        database_url,
        """
        SELECT epoch.epoch_key, epoch.code_commit,
               epoch.code_snapshot_sha256, epoch.dependency_lock_sha256
          FROM systematic_fx.m0b_epochs AS epoch
          JOIN systematic_fx.campaigns AS campaign USING (campaign_id)
         WHERE epoch.epoch_key = %s
           AND epoch.status = 'RUNNING'
           AND campaign.status = 'RUNNING'
           AND campaign.holdout_revealed_at IS NULL
           AND campaign.closed_at IS NULL
        """,
        (epoch_key,),
    )
    if row is None:
        raise M0bWorkerRegistryError("active unrevealed M0b epoch runtime identity is absent")
    return M0bEpochRuntimeIdentity(
        epoch_key=str(row["epoch_key"]),
        code_commit=str(row["code_commit"]),
        code_snapshot_sha256=str(row["code_snapshot_sha256"]),
        dependency_lock_sha256=str(row["dependency_lock_sha256"]),
    )


def claim_m0b_work(
    database_url: str,
    *,
    epoch_key: str,
    worker_id: str,
    lease_token_sha256: str,
    lease_seconds: int = 300,
) -> M0bWorkerClaim | None:
    """Claim the next already-registered candidate; never generate a candidate."""

    row = _call(
        database_url,
        "SELECT * FROM systematic_fx.m0b_worker_claim_next(%s, %s, %s, %s)",
        (epoch_key, worker_id, lease_token_sha256, lease_seconds),
    )
    if row is None:
        return None
    return M0bWorkerClaim(
        m0b_candidate_id=int(row["m0b_candidate_id"]),
        research_run_attempt_id=int(row["research_run_attempt_id"]),
        attempt_number=int(row["attempt_number"]),
        candidate_sha256=str(row["candidate_sha256"]),
        candidate_kind=str(row["candidate_kind"]),
        canonical_candidate=dict(row["canonical_candidate"]),
        epoch_sha256=str(row["epoch_sha256"]),
        work_spec_sha256=str(row["work_spec_sha256"]),
        work_spec_byte_size=int(row["work_spec_byte_size"]),
        attempt_status=str(row["attempt_status"]),
        lease_status=str(row["lease_status"]),
        leased_until=row["leased_until"],
    )


def checkpoint_m0b_work(
    database_url: str,
    *,
    candidate_id: int,
    attempt_id: int,
    lease_token_sha256: str,
    checkpoint_sequence: int,
    predecessor_sha256: str | None,
    state: Mapping[str, object],
) -> tuple[int, str]:
    """Append one canonical checkpoint under the active opaque lease."""

    cursor = {
        "artifact_schema": "systematic_fx.m0b_checkpoint.v1",
        "checkpoint_sequence": checkpoint_sequence,
        "m0b_candidate_id": candidate_id,
        "predecessor_sha256": predecessor_sha256,
        "research_run_attempt_id": attempt_id,
        "state": dict(state),
    }
    checkpoint_sha256 = canonical_sha256(cursor)
    row = _call(
        database_url,
        """
        SELECT systematic_fx.m0b_worker_checkpoint(
            %s, %s, %s, %s, %s, %s, %s) AS checkpoint_id
        """,
        (
            candidate_id,
            attempt_id,
            lease_token_sha256,
            checkpoint_sequence,
            checkpoint_sha256,
            predecessor_sha256,
            Jsonb(cursor),
        ),
    )
    if row is None:
        raise M0bWorkerRegistryError("M0b checkpoint capability returned no identity")
    return int(row["checkpoint_id"]), checkpoint_sha256


def terminalize_m0b_work(
    database_url: str,
    *,
    candidate_id: int,
    attempt_id: int,
    lease_token_sha256: str,
    result_sha256: str,
    result_byte_size: int,
    metrics: Mapping[str, object],
) -> M0bWorkerTerminalResult:
    """Commit result identity; PostgreSQL alone derives REGISTERED/SCREENED_OUT."""

    row = _call(
        database_url,
        "SELECT * FROM systematic_fx.m0b_worker_terminalize(%s, %s, %s, %s, %s, %s)",
        (
            candidate_id,
            attempt_id,
            lease_token_sha256,
            result_sha256,
            result_byte_size,
            Jsonb(dict(metrics)),
        ),
    )
    if row is None:
        raise M0bWorkerRegistryError("M0b terminal capability returned no identity")
    return M0bWorkerTerminalResult(
        artifact_id=int(row["artifact_id"]),
        classification=str(row["classification"]),
        registered_at=row["registered_at"],
    )


def fail_m0b_work(
    database_url: str,
    *,
    candidate_id: int,
    attempt_id: int,
    lease_token_sha256: str,
    error_message: str,
    retryable: bool = True,
) -> str:
    """Finish one attempt as failed and retain retry state only within budget."""

    row = _call(
        database_url,
        "SELECT systematic_fx.m0b_worker_fail(%s, %s, %s, %s, %s) AS status",
        (candidate_id, attempt_id, lease_token_sha256, error_message, retryable),
    )
    if row is None:
        raise M0bWorkerRegistryError("M0b failure capability returned no state")
    return str(row["status"])
