"""Atomic registration boundary for one finite-budget M0b candidate."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.db.postgres_retry import retry_serialization_failures
from systematic_fx.research.hypotheses import canonical_sha256
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
                     candidate_kind, ordinal, candidate_sha256, canonical_candidate)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (m0b_epoch_id, candidate_sha256) DO NOTHING
                RETURNING m0b_candidate_id, research_run_spec_id
                """,
                (
                    epoch["m0b_epoch_id"],
                    parent_candidate_id,
                    run_spec_id,
                    candidate_kind,
                    ordinal,
                    candidate_sha256,
                    Jsonb(candidate_document),
                ),
            ).fetchone()
            candidate_created = candidate is not None
            candidate = (
                candidate
                or connection.execute(
                    """
                SELECT m0b_candidate_id, research_run_spec_id
                  FROM systematic_fx.m0b_candidates
                 WHERE m0b_epoch_id = %s AND candidate_sha256 = %s FOR SHARE
                """,
                    (epoch["m0b_epoch_id"], candidate_sha256),
                ).fetchone()
            )
            if candidate is None or int(candidate["research_run_spec_id"]) != run_spec_id:
                raise M0bRegistryError("existing M0b candidate identity differs")
    return M0bCandidateRegistration(
        m0b_candidate_id=int(candidate["m0b_candidate_id"]),
        research_run_spec_id=run_spec_id,
        run_fingerprint=run_spec.fingerprint,
        candidate_sha256=candidate_sha256,
        created=run_spec_created and candidate_created,
    )


def register_m0b_candidate(
    database_url: str,
    *,
    epoch_key: str,
    run_spec: RunSpec,
    candidate_kind: Literal["REAL", "NULL"],
    ordinal: int,
    canonical_candidate: Mapping[str, object],
    parent_candidate_sha256: str | None = None,
) -> M0bCandidateRegistration:
    """Register the immutable RunSpec and budgeted candidate in one transaction."""

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
        )
    except M0bRegistryError:
        raise
    except psycopg.Error as error:
        raise M0bRegistryError("PostgreSQL M0b candidate registration failed") from error
