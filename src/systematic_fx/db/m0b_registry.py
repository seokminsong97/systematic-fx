"""Atomic registration boundary for one finite-budget M0b candidate."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from typing import Literal

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.db.postgres_retry import retry_serialization_failures
from systematic_fx.research.hypotheses import canonical_sha256
from systematic_fx.research.m0b.first_passage_store import FirstPassageStoreError
from systematic_fx.research.m0b.worker import (
    CandidateWorkArtifact,
    load_candidate_work_artifact,
    load_candidate_work_manifest,
)
from systematic_fx.research.run_spec import (
    RUN_SPEC_SCHEMA,
    RUN_SPEC_SCHEMA_VERSION,
    RunSpec,
)


class M0bRegistryError(RuntimeError):
    """M0b registration could not satisfy its atomic identity contract."""


@dataclass(frozen=True, slots=True)
class M0bCandidateRegistration:
    m0b_candidate_id: int
    research_run_spec_id: int
    run_fingerprint: str
    candidate_sha256: str
    created: bool
    work_artifact_id: int
    work_spec_sha256: str


_BARRIER_DECIMAL = re.compile(r"(?:0|[1-9][0-9]*)\.[0-9]{2}")


def _candidate_barrier_matches(
    candidate: Mapping[str, object],
    work_artifact: CandidateWorkArtifact,
) -> bool:
    barrier = candidate.get("barrier")
    if not isinstance(barrier, Mapping) or set(barrier) != {
        "k_tp",
        "k_sl",
        "max_hold_minutes",
    }:
        return False
    k_tp = barrier.get("k_tp")
    k_sl = barrier.get("k_sl")
    minutes = barrier.get("max_hold_minutes")
    if (
        not isinstance(k_tp, str)
        or _BARRIER_DECIMAL.fullmatch(k_tp) is None
        or not isinstance(k_sl, str)
        or _BARRIER_DECIMAL.fullmatch(k_sl) is None
        or isinstance(minutes, bool)
        or not isinstance(minutes, int)
        or minutes <= 0
    ):
        return False
    try:
        candidate_tp = Fraction(Decimal(k_tp))
        candidate_sl = Fraction(Decimal(k_sl))
    except (InvalidOperation, ValueError, ZeroDivisionError):
        return False
    work = work_artifact.work.barrier
    return (
        candidate_tp > 0
        and candidate_sl > 0
        and candidate_tp == Fraction(work.k_tp_num, work.k_tp_den)
        and candidate_sl == Fraction(work.k_sl_num, work.k_sl_den)
        and minutes * 60 == work.max_hold_seconds
    )


def _candidate_core_matches(
    candidate: Mapping[str, object],
    work_artifact: CandidateWorkArtifact,
    candidate_kind: Literal["REAL", "NULL"],
) -> bool:
    cost = candidate.get("cost")
    seed = candidate.get("random_seed")
    return (
        candidate.get("candidate_kind") == candidate_kind
        and candidate.get("direction") == work_artifact.work.direction
        and not isinstance(seed, bool)
        and isinstance(seed, int)
        and seed == work_artifact.work.deterministic_seed
        and isinstance(cost, Mapping)
        and set(cost) == {"sha256", "version"}
        and isinstance(cost.get("version"), str)
        and bool(str(cost["version"]).strip())
        and cost.get("sha256") == work_artifact.work.cost_sha256
    )


def _validate_work_binding(
    *,
    work_artifact: CandidateWorkArtifact,
    run_spec: RunSpec,
    candidate_kind: Literal["REAL", "NULL"],
    candidate_sha256: str,
    canonical_candidate: Mapping[str, object],
) -> None:
    if not isinstance(work_artifact, CandidateWorkArtifact):
        raise M0bRegistryError("work_artifact must be a loaded CandidateWorkArtifact")
    try:
        reopened_artifact = load_candidate_work_artifact(
            work_artifact.path,
            reconcile_inputs=True,
        )
        reopened_work = load_candidate_work_manifest(work_artifact.path)
    except (FirstPassageStoreError, OSError) as error:
        raise M0bRegistryError(
            "CandidateWork artifact path is not immutable canonical work bytes"
        ) from error
    try:
        reopened_bytes = work_artifact.path.read_bytes()
    except OSError as error:
        raise M0bRegistryError("CandidateWork artifact bytes cannot be reopened") from error
    work = work_artifact.work
    if (
        reopened_artifact != work_artifact
        or reopened_artifact.canonical_bytes != work_artifact.canonical_bytes
        or reopened_work != work
        or reopened_bytes != work_artifact.canonical_bytes
        or work_artifact.path.stat().st_size != work_artifact.byte_size
        or work_artifact.path.stat().st_mode & 0o222
        or work_artifact.path.is_symlink()
        or work_artifact.content_sha256 != work.sha256
        or work.candidate_sha256 != candidate_sha256
        or work.candidate_kind != candidate_kind
        or run_spec.parameters.get("m0b_work_spec_sha256") != work_artifact.content_sha256
        or run_spec.parameters.get("m0b_epoch_sha256") != work.epoch_sha256
        or work.code_snapshot_sha256 != run_spec.code_snapshot_sha256
        or work.deterministic_seed != run_spec.random_seed
        or work.direction != run_spec.direction
        or work_artifact.source_build_sha256 != run_spec.parameters.get("m0b_dataset_sha256")
        or work_artifact.source_feature_sha256 != run_spec.feature_sha256
        or work_artifact.source_label_sha256 != run_spec.outcome_sha256
        or work.signals.feature_sha256 != run_spec.feature_sha256
        or work.cost_sha256 != run_spec.cost_sha256
        or work.execution_sha256 != run_spec.execution_sha256
        or work.split_sha256 != run_spec.split_sha256
        or dict(run_spec.barrier_policy) != work.barrier.as_dict()
        or run_spec.parameters.get("m0b_barrier_sha256") != work.barrier.sha256
        or run_spec.parameters.get("m0b_evaluation_policy_sha256") != work.evaluation_policy_sha256
        or not _candidate_core_matches(
            canonical_candidate,
            work_artifact,
            candidate_kind,
        )
        or not _candidate_barrier_matches(canonical_candidate, work_artifact)
    ):
        raise M0bRegistryError("CandidateWork artifact, candidate, and RunSpec identities differ")


def _register_work_artifact(
    connection: psycopg.Connection,
    work_artifact: CandidateWorkArtifact,
) -> tuple[int, bool]:
    rows = connection.execute(
        """
        SELECT artifact_id, artifact_key, artifact_type, uri, sha256, byte_size,
               media_type, producer_job_id, metadata
          FROM systematic_fx.artifacts
         WHERE artifact_key = %s OR uri = %s
         FOR UPDATE
        """,
        (work_artifact.artifact_key, work_artifact.artifact_uri),
    ).fetchall()
    metadata = work_artifact.metadata()
    if not rows:
        inserted = connection.execute(
            """
            INSERT INTO systematic_fx.artifacts
                (artifact_key, artifact_type, uri, sha256, byte_size,
                 media_type, metadata)
            VALUES (%s, 'M0B_CANDIDATE_WORK', %s, %s, %s,
                    'application/json', %s)
            RETURNING artifact_id
            """,
            (
                work_artifact.artifact_key,
                work_artifact.artifact_uri,
                work_artifact.content_sha256,
                work_artifact.byte_size,
                Jsonb(metadata),
            ),
        ).fetchone()
        if inserted is None:  # pragma: no cover - PostgreSQL RETURNING is mandatory
            raise M0bRegistryError("M0b CandidateWork artifact registration disappeared")
        return int(inserted["artifact_id"]), True
    if len(rows) != 1:
        raise M0bRegistryError("M0b CandidateWork artifact key and URI collide")
    row = rows[0]
    expected = {
        "artifact_key": work_artifact.artifact_key,
        "artifact_type": "M0B_CANDIDATE_WORK",
        "byte_size": work_artifact.byte_size,
        "media_type": work_artifact.media_type,
        "metadata": metadata,
        "producer_job_id": None,
        "sha256": work_artifact.content_sha256,
        "uri": work_artifact.artifact_uri,
    }
    if any(row.get(key) != value for key, value in expected.items()):
        raise M0bRegistryError("existing M0b CandidateWork artifact identity differs")
    return int(row["artifact_id"]), False


def _plain(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_plain(item) for item in value]
    return value


def _register(
    database_url: str,
    *,
    epoch_key: str,
    run_spec: RunSpec,
    candidate_kind: Literal["REAL", "NULL"],
    ordinal: int,
    canonical_candidate: Mapping[str, object],
    parent_candidate_sha256: str | None,
    work_artifact: CandidateWorkArtifact,
) -> M0bCandidateRegistration:
    if not isinstance(run_spec, RunSpec) or not epoch_key.strip():
        raise M0bRegistryError("epoch_key and RunSpec are required")
    if candidate_kind not in ("REAL", "NULL"):
        raise M0bRegistryError("candidate_kind must be REAL or NULL")
    if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal <= 0:
        raise M0bRegistryError("ordinal must be a positive integer")
    if candidate_kind == "REAL" and parent_candidate_sha256 is not None:
        raise M0bRegistryError("REAL candidates cannot bind a parent")
    if candidate_kind == "NULL" and (
        not isinstance(parent_candidate_sha256, str)
        or len(parent_candidate_sha256) != 64
        or any(character not in "0123456789abcdef" for character in parent_candidate_sha256)
    ):
        raise M0bRegistryError("NULL candidates require a lowercase parent SHA-256")
    candidate_document = dict(canonical_candidate)
    candidate_sha256 = canonical_sha256(candidate_document)
    if run_spec.parameters.get("m0b_candidate_sha256") != candidate_sha256:
        raise M0bRegistryError("RunSpec and candidate SHA-256 differ")
    _validate_work_binding(
        work_artifact=work_artifact,
        run_spec=run_spec,
        candidate_kind=candidate_kind,
        candidate_sha256=candidate_sha256,
        canonical_candidate=candidate_document,
    )
    canonical_spec = _plain(run_spec.payload())
    source_hashes = dict(run_spec.source_manifest_hashes)
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        connection.isolation_level = psycopg.IsolationLevel.SERIALIZABLE
        with connection.transaction():
            epoch_identity = connection.execute(
                """
                SELECT epoch.m0b_epoch_id, epoch.campaign_id
                  FROM systematic_fx.m0b_epochs AS epoch
                 WHERE epoch.epoch_key = %s
                """,
                (epoch_key,),
            ).fetchone()
            if epoch_identity is None:
                raise M0bRegistryError("M0b epoch does not exist")
            campaign = connection.execute(
                """
                SELECT campaign_id, campaign_key
                  FROM systematic_fx.campaigns
                 WHERE campaign_id = %s
                 FOR UPDATE
                """,
                (epoch_identity["campaign_id"],),
            ).fetchone()
            epoch = connection.execute(
                """
                SELECT m0b_epoch_id, campaign_id
                  FROM systematic_fx.m0b_epochs
                 WHERE m0b_epoch_id = %s AND campaign_id = %s
                 FOR UPDATE
                """,
                (epoch_identity["m0b_epoch_id"], epoch_identity["campaign_id"]),
            ).fetchone()
            if campaign is None or epoch is None:
                raise M0bRegistryError("M0b epoch namespace disappeared")
            epoch = {**epoch, "campaign_key": campaign["campaign_key"]}
            if epoch is None or run_spec.campaign_id != epoch["campaign_key"]:
                raise M0bRegistryError("M0b epoch and RunSpec campaign differ")
            if run_spec.parameters.get("m0b_epoch_sha256") != work_artifact.work.epoch_sha256:
                raise M0bRegistryError("CandidateWork artifact belongs to another epoch")
            work_artifact_id, work_artifact_created = _register_work_artifact(
                connection,
                work_artifact,
            )
            experiment = connection.execute(
                """
                SELECT experiment_id FROM systematic_fx.experiments
                 WHERE campaign_id = %s AND experiment_key = %s
                """,
                (epoch["campaign_id"], run_spec.experiment_id),
            ).fetchone()
            if experiment is None:
                raise M0bRegistryError("M0b experiment does not exist")
            parent_candidate_id = None
            parent_run_spec_id = None
            if candidate_kind == "NULL":
                parent = connection.execute(
                    """
                    SELECT m0b_candidate_id, research_run_spec_id
                      FROM systematic_fx.m0b_candidates
                     WHERE m0b_epoch_id = %s AND candidate_sha256 = %s
                     FOR SHARE
                    """,
                    (epoch["m0b_epoch_id"], parent_candidate_sha256),
                ).fetchone()
                if parent is None:
                    raise M0bRegistryError("M0b NULL parent is absent")
                parent_candidate_id = int(parent["m0b_candidate_id"])
                parent_run_spec_id = int(parent["research_run_spec_id"])
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
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s)
                ON CONFLICT (run_fingerprint) DO NOTHING
                RETURNING research_run_spec_id
                """,
                (
                    run_spec.fingerprint,
                    RUN_SPEC_SCHEMA,
                    RUN_SPEC_SCHEMA_VERSION,
                    epoch["campaign_id"],
                    experiment["experiment_id"],
                    parent_run_spec_id,
                    run_spec.run_kind,
                    run_spec.engine_version,
                    Jsonb(canonical_spec),
                    Jsonb(source_hashes),
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
                    Decimal(run_spec.random_seed),
                    run_spec.direction,
                ),
            ).fetchone()
            run_spec_created = inserted is not None
            run_row = (
                inserted
                or connection.execute(
                    """
                SELECT research_run_spec_id FROM systematic_fx.research_run_specs
                 WHERE run_fingerprint = %s FOR SHARE
                """,
                    (run_spec.fingerprint,),
                ).fetchone()
            )
            if run_row is None:
                raise M0bRegistryError("M0b RunSpec registration disappeared")
            run_spec_id = int(run_row["research_run_spec_id"])
            candidate = connection.execute(
                """
                INSERT INTO systematic_fx.m0b_candidates
                    (m0b_epoch_id, parent_candidate_id, research_run_spec_id,
                     work_artifact_id, candidate_kind, ordinal,
                     candidate_sha256, canonical_candidate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (m0b_epoch_id, candidate_sha256) DO NOTHING
                RETURNING m0b_candidate_id, research_run_spec_id, work_artifact_id
                """,
                (
                    epoch["m0b_epoch_id"],
                    parent_candidate_id,
                    run_spec_id,
                    work_artifact_id,
                    candidate_kind,
                    ordinal,
                    candidate_sha256,
                    Jsonb(candidate_document),
                ),
            ).fetchone()
            candidate_created = candidate is not None
            if candidate is None:
                candidate = connection.execute(
                    """
                    SELECT m0b_candidate_id, research_run_spec_id, work_artifact_id
                      FROM systematic_fx.m0b_candidates
                     WHERE m0b_epoch_id = %s AND candidate_sha256 = %s FOR SHARE
                    """,
                    (epoch["m0b_epoch_id"], candidate_sha256),
                ).fetchone()
            if candidate is None or int(candidate["research_run_spec_id"]) != run_spec_id:
                raise M0bRegistryError("existing M0b candidate identity differs")
            if candidate.get("work_artifact_id") != work_artifact_id:
                raise M0bRegistryError("existing M0b candidate work identity differs")
    return M0bCandidateRegistration(
        m0b_candidate_id=int(candidate["m0b_candidate_id"]),
        research_run_spec_id=run_spec_id,
        run_fingerprint=run_spec.fingerprint,
        candidate_sha256=candidate_sha256,
        created=work_artifact_created and run_spec_created and candidate_created,
        work_artifact_id=work_artifact_id,
        work_spec_sha256=work_artifact.content_sha256,
    )


def register_m0b_candidate(
    database_url: str,
    *,
    epoch_key: str,
    run_spec: RunSpec,
    candidate_kind: Literal["REAL", "NULL"],
    ordinal: int,
    canonical_candidate: Mapping[str, object],
    work_artifact: CandidateWorkArtifact,
    parent_candidate_sha256: str | None = None,
) -> M0bCandidateRegistration:
    """Register required work bytes, RunSpec, and budgeted candidate atomically."""

    try:
        return retry_serialization_failures(
            _register,
            database_url,
            epoch_key=epoch_key,
            run_spec=run_spec,
            candidate_kind=candidate_kind,
            ordinal=ordinal,
            canonical_candidate=canonical_candidate,
            parent_candidate_sha256=parent_candidate_sha256,
            work_artifact=work_artifact,
        )
    except M0bRegistryError:
        raise
    except psycopg.Error as error:
        raise M0bRegistryError("PostgreSQL M0b candidate registration failed") from error
