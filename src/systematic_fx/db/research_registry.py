"""Transactional, drift-rejecting persistence for Phase 1 research registration."""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from functools import wraps
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.research.hypotheses import (
    EXPECTED_CAMPAIGN_VARIANT_BUDGET,
    EXPECTED_PARENT_COUNT,
    EXPECTED_PARENTS_PER_FAMILY,
    HypothesisBundle,
    HypothesisConfigError,
    HypothesisSpec,
    canonical_json_bytes,
    canonical_sha256,
    family_counts,
    load_hypothesis_bundle,
    load_toml_document,
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_KEY_PART = re.compile(r"[^a-z0-9]+")
_EXPOSURE_TYPES = frozenset({"AI_SLICE", "QUERY", "SUMMARY", "EVENT_WINDOW", "PIPELINE_PILOT"})
_P = ParamSpec("_P")
_R = TypeVar("_R")


class ResearchRegistryError(RuntimeError):
    """Research control state could not be safely registered."""


class ResearchRegistryDriftError(ResearchRegistryError):
    """A deterministic identity already exists with different immutable content."""


def _translate_psycopg_errors(
    operation: str,
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    """Keep driver details chained while presenting one stable registry error API."""

    def decorate(function: Callable[_P, _R]) -> Callable[_P, _R]:
        @wraps(function)
        def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            try:
                return function(*args, **kwargs)
            except ResearchRegistryError:
                raise
            except psycopg.Error as error:
                raise ResearchRegistryError(f"PostgreSQL {operation} failed") from error

        return wrapped

    return decorate


@dataclass(frozen=True)
class PreparedParentRegistration:
    """Validated config state and its deterministic registration artifact."""

    campaign_key: str
    dataset_key: str
    dataset_document: Mapping[str, object]
    campaign_document: Mapping[str, object]
    cost_assumptions: Mapping[str, object]
    execution_assumptions: Mapping[str, object]
    hypothesis_bundle: HypothesisBundle
    registration_document: Mapping[str, object]
    registration_bytes: bytes
    registration_sha256: str
    campaign_config_sha256: str


@dataclass(frozen=True)
class ParentRegistrationReport:
    """Identities created or verified by one parent-hypothesis registration."""

    dataset_id: int
    dataset_key: str
    campaign_id: int
    campaign_key: str
    job_id: int
    artifact_id: int
    artifact_path: Path
    artifact_sha256: str
    experiment_ids: tuple[int, ...]
    created_dataset: bool
    created_campaign: bool
    created_job: bool
    created_artifact: bool
    created_experiments: int


@dataclass(frozen=True)
class DiscoveryExposureReport:
    """One AI-visible or pilot exposure identity and optional result artifact."""

    discovery_exposure_id: int
    exposure_key: str
    campaign_id: int
    result_artifact_id: int | None
    created_exposure: bool
    created_artifact: bool


def _table(document: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = document.get(name)
    if not isinstance(value, dict):
        raise ResearchRegistryError(f"{name} must be a TOML table")
    return value


def _nonempty(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ResearchRegistryError(f"{label} must be a non-empty string")
    return value.strip()


def _boolean(value: object, *, label: str) -> bool:
    if not isinstance(value, bool):
        raise ResearchRegistryError(f"{label} must be boolean")
    return value


def _positive_integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ResearchRegistryError(f"{label} must be a positive integer")
    return value


def _safe_key_part(value: str) -> str:
    normalized = _SAFE_KEY_PART.sub("_", value.lower()).strip("_")
    if not normalized:
        raise ResearchRegistryError("cannot derive a database key from an empty value")
    return normalized


def _campaign_inputs(
    document: Mapping[str, Any],
    bundle: HypothesisBundle,
    cost_document: Mapping[str, Any],
    execution_document: Mapping[str, Any],
) -> tuple[dict[str, object], dict[str, object]]:
    campaign = _table(document, "campaign")
    budget = _table(document, "budget")
    data_gate = _table(document, "data_gate")
    data_gate_policy = _table(data_gate, "policy")
    split_policy = _table(document, "split_policy")
    visibility = _table(document, "visibility")
    provenance = _table(document, "provenance")
    cost_model = _table(cost_document, "cost_model")
    execution_model = _table(execution_document, "execution_model")

    campaign_key = _nonempty(campaign.get("id"), label="campaign.id")
    if campaign.get("status") != "DRAFT":
        raise ResearchRegistryError("campaign.status must be DRAFT")
    if _boolean(campaign.get("research_eligible"), label="campaign.research_eligible"):
        raise ResearchRegistryError("the pending-gate campaign must not be research eligible")
    if not _boolean(
        data_gate_policy.get("block_strategy_performance"),
        label="data_gate.policy.block_strategy_performance",
    ):
        raise ResearchRegistryError("pending data gates must block strategy performance")
    if not _boolean(
        data_gate_policy.get("allow_a_priori_hypothesis_registration"),
        label="data_gate.policy.allow_a_priori_hypothesis_registration",
    ):
        raise ResearchRegistryError("campaign must explicitly allow a-priori registration")
    if data_gate_policy.get("derived_root") != "data/derived":
        raise ResearchRegistryError("data_gate.policy.derived_root must be data/derived")
    if data_gate.get("full_content_sha256") is not True:
        raise ResearchRegistryError("data_gate.full_content_sha256 must pass before registration")
    source_manifest_sha256 = provenance.get("source_manifest_sha256")
    if not isinstance(source_manifest_sha256, str) or not _SHA256.fullmatch(source_manifest_sha256):
        raise ResearchRegistryError("provenance.source_manifest_sha256 must be verified SHA-256")

    expected_budgets = {
        "primary_families": 6,
        "parents_per_family": EXPECTED_PARENTS_PER_FAMILY,
        "parent_hypotheses": EXPECTED_PARENT_COUNT,
        "descendants_per_parent": bundle.descendants_per_parent,
        "strategy_variants": EXPECTED_CAMPAIGN_VARIANT_BUDGET,
        "sealed_holdout_finalists": 10,
    }
    for key, expected in expected_budgets.items():
        actual = _positive_integer(budget.get(key), label=f"budget.{key}")
        if actual != expected:
            raise ResearchRegistryError(f"budget.{key} must equal {expected}")
    if bundle.campaign_strategy_variant_budget != budget["strategy_variants"]:
        raise ResearchRegistryError("campaign and hypothesis strategy-variant budgets differ")

    if campaign.get("parent_symbol") != bundle.parent_symbol:
        raise ResearchRegistryError("campaign and hypothesis parent symbols differ")
    if campaign.get("feature_version") != bundle.feature_definition_versions["research_5m"]:
        raise ResearchRegistryError("campaign and hypothesis feature versions differ")
    if campaign.get("outcome_version") != bundle.feature_definition_versions["outcomes"]:
        raise ResearchRegistryError("campaign and hypothesis outcome versions differ")

    cost_id = _nonempty(cost_model.get("id"), label="cost_model.id")
    execution_id = _nonempty(execution_model.get("id"), label="execution_model.id")
    if campaign.get("cost_model_version") != cost_id or cost_id != "cost_pending_v1":
        raise ResearchRegistryError("campaign must reference cost_pending_v1")
    if campaign.get("execution_model_version") != execution_id or execution_id != (
        "execution_pending_v1"
    ):
        raise ResearchRegistryError("campaign must reference execution_pending_v1")
    for label, model in (("cost_model", cost_model), ("execution_model", execution_model)):
        if model.get("status") != "UNRESOLVED":
            raise ResearchRegistryError(f"{label}.status must be UNRESOLVED")
        if _boolean(model.get("numeric_verified"), label=f"{label}.numeric_verified"):
            raise ResearchRegistryError(f"{label}.numeric_verified must remain false")
        if _boolean(model.get("research_eligible"), label=f"{label}.research_eligible"):
            raise ResearchRegistryError(f"{label}.research_eligible must remain false")
        gate = _table(model, "gate")
        if not _boolean(
            gate.get("block_economic_screening"),
            label=f"{label}.gate.block_economic_screening",
        ):
            raise ResearchRegistryError(f"{label} must block economic screening")
        _nonempty(gate.get("reason"), label=f"{label}.gate.reason")

    source_start = campaign.get("source_start")
    source_end = campaign.get("source_end")
    if not isinstance(source_start, date) or not isinstance(source_end, date):
        raise ResearchRegistryError("campaign source dates must be TOML local dates")
    if source_start > source_end:
        raise ResearchRegistryError("campaign source_start must not exceed source_end")

    feed = _nonempty(campaign.get("dataset"), label="campaign.dataset")
    data_schema = _nonempty(campaign.get("schema"), label="campaign.schema")
    parent_symbol = _nonempty(campaign.get("parent_symbol"), label="campaign.parent_symbol")
    dataset_key = "_".join(
        (
            _safe_key_part(feed),
            _safe_key_part(data_schema),
            _safe_key_part(parent_symbol),
            "v1",
        )
    )
    dataset_document: dict[str, object] = {
        "dataset_key": dataset_key,
        "provider": "Databento",
        "feed": feed,
        "data_schema": data_schema,
        "price_scale_exponent": -9,
        "status": "VALIDATING",
        "expected_start_date": source_start,
        "expected_end_date": source_end,
        "manifest_sha256": source_manifest_sha256,
        "metadata": {
            "parent_symbol": parent_symbol,
            "source_manifest_kind": "full_content_sha256_v1",
        },
    }
    campaign_document: dict[str, object] = {
        "campaign_key": campaign_key,
        "name": _nonempty(campaign.get("name"), label="campaign.name"),
        "status": "DRAFT",
        "selected_start_date": None,
        "selected_end_date": None,
        "roll_cutoff_date": None,
        "data_manifest_sha256": source_manifest_sha256,
        "feature_version": _nonempty(
            campaign.get("feature_version"), label="campaign.feature_version"
        ),
        "outcome_version": _nonempty(
            campaign.get("outcome_version"), label="campaign.outcome_version"
        ),
        "cost_model_version": cost_id,
        "execution_model_version": execution_id,
        "split_policy": {
            "status": "PENDING_ELIGIBLE_CALENDAR",
            "research_eligible": False,
            "data_gate": dict(data_gate),
            "policy": dict(split_policy),
            "visibility": dict(visibility),
            "provenance": dict(provenance),
        },
        "trial_budget": EXPECTED_CAMPAIGN_VARIANT_BUDGET,
        "finalist_budget": 10,
    }
    return dataset_document, campaign_document


def _model_assumptions(document: Mapping[str, Any], table_name: str) -> dict[str, object]:
    model = dict(_table(document, table_name))
    gate = _table(model, "gate")
    return {
        "version": model["id"],
        "status": "UNVERIFIED",
        "numeric_verified": False,
        "research_eligible": False,
        "execution_blocked": True,
        "unresolved_reason": gate["reason"],
        "config": model,
    }


def prepare_parent_hypothesis_registration(
    *,
    campaign_config_path: Path,
    hypothesis_config_path: Path,
    cost_config_path: Path,
    execution_config_path: Path,
    code_commit: str,
) -> PreparedParentRegistration:
    """Validate four configs and build the exact immutable registration bytes."""

    code_commit = _nonempty(code_commit, label="code_commit")
    try:
        hypothesis_bundle = load_hypothesis_bundle(hypothesis_config_path)
        campaign_config = load_toml_document(campaign_config_path)
        cost_config = load_toml_document(cost_config_path)
        execution_config = load_toml_document(execution_config_path)
    except HypothesisConfigError as error:
        raise ResearchRegistryError(str(error)) from error

    dataset_document, campaign_document = _campaign_inputs(
        campaign_config,
        hypothesis_bundle,
        cost_config,
        execution_config,
    )
    cost_assumptions = _model_assumptions(cost_config, "cost_model")
    execution_assumptions = _model_assumptions(execution_config, "execution_model")
    config_documents = {
        "campaign": campaign_config,
        "hypotheses": hypothesis_bundle.registration_payload(),
        "cost": cost_config,
        "execution": execution_config,
    }
    source_descriptors = {
        "campaign": {
            "name": campaign_config_path.name,
            "canonical_sha256": canonical_sha256(campaign_config),
        },
        "hypotheses": {
            "name": hypothesis_config_path.name,
            "canonical_sha256": hypothesis_bundle.config_sha256,
        },
        "cost": {
            "name": cost_config_path.name,
            "canonical_sha256": canonical_sha256(cost_config),
        },
        "execution": {
            "name": execution_config_path.name,
            "canonical_sha256": canonical_sha256(execution_config),
        },
    }
    registration_document: dict[str, object] = {
        "artifact_schema": "systematic_fx.parent_hypothesis_registration.v1",
        "campaign_key": campaign_document["campaign_key"],
        "dataset_key": dataset_document["dataset_key"],
        "code_commit": code_commit,
        "source_configs": source_descriptors,
        "configs": config_documents,
        "registration_policy": {
            "a_priori_parent_hypotheses": EXPECTED_PARENT_COUNT,
            "family_counts": family_counts(hypothesis_bundle.hypotheses),
            "pattern_rows_created": 0,
            "performance_execution_blocked": True,
            "selected_dates_are_null": True,
            "split_boundaries_are_unassigned": True,
            "campaign_budget_counts_only": "STRATEGY_VARIANT",
            "experiment_local_budget_counts": "ALL_EXPERIMENT_TRIAL_ROWS",
        },
    }
    registration_bytes = canonical_json_bytes(registration_document) + b"\n"
    registration_sha256 = hashlib.sha256(registration_bytes).hexdigest()
    campaign_config_sha256 = canonical_sha256(
        {
            "campaign": campaign_config,
            "cost": cost_config,
            "execution": execution_config,
        }
    )
    return PreparedParentRegistration(
        campaign_key=str(campaign_document["campaign_key"]),
        dataset_key=str(dataset_document["dataset_key"]),
        dataset_document=dataset_document,
        campaign_document=campaign_document,
        cost_assumptions=cost_assumptions,
        execution_assumptions=execution_assumptions,
        hypothesis_bundle=hypothesis_bundle,
        registration_document=registration_document,
        registration_bytes=registration_bytes,
        registration_sha256=registration_sha256,
        campaign_config_sha256=campaign_config_sha256,
    )


def _contained_path(path: Path, root: Path, *, label: str) -> Path:
    resolved_root = root.expanduser().resolve()
    resolved = path.expanduser().resolve()
    if not resolved.is_relative_to(resolved_root):
        raise ResearchRegistryError(f"{label} must be contained by {resolved_root}")
    return resolved


def _write_registration_artifact(
    prepared: PreparedParentRegistration,
    artifacts_root: Path,
) -> Path:
    resolved_root = artifacts_root.expanduser().resolve()
    resolved_root.mkdir(parents=True, exist_ok=True)
    registration_directory = resolved_root / "registration"
    registration_directory.mkdir(parents=True, exist_ok=True)
    registration_directory = _contained_path(
        registration_directory,
        resolved_root,
        label="registration artifact directory",
    )
    destination = registration_directory / (
        f"{_safe_key_part(prepared.campaign_key)}-{prepared.registration_sha256}.json"
    )
    if destination.exists():
        if destination.read_bytes() != prepared.registration_bytes:
            raise ResearchRegistryDriftError(
                f"registration artifact content drift at {destination}"
            )
        return destination

    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=registration_directory,
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            os.fchmod(handle.fileno(), 0o600)
            handle.write(prepared.registration_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, destination)
        temporary_path = None
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()
    return destination


def _row_or_error(row: dict[str, Any] | None, *, label: str) -> dict[str, Any]:
    if row is None:
        raise ResearchRegistryError(f"{label} was not visible after registration")
    return row


def _assert_fields(
    *,
    label: str,
    row: Mapping[str, Any],
    expected: Mapping[str, object],
) -> None:
    mismatches = [key for key, value in expected.items() if row.get(key) != value]
    if mismatches:
        raise ResearchRegistryDriftError(
            f"{label} immutable content drift in fields: {', '.join(sorted(mismatches))}"
        )


def _ensure_dataset(
    connection: psycopg.Connection[dict[str, Any]],
    prepared: PreparedParentRegistration,
    source_root_uri: str,
) -> tuple[int, bool]:
    spec = prepared.dataset_document
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.datasets
            (dataset_key, provider, feed, data_schema, root_uri, price_scale_exponent,
             status, expected_start_date, expected_end_date, manifest_sha256, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (dataset_key) DO NOTHING
        RETURNING dataset_id
        """,
        (
            spec["dataset_key"],
            spec["provider"],
            spec["feed"],
            spec["data_schema"],
            source_root_uri,
            spec["price_scale_exponent"],
            spec["status"],
            spec["expected_start_date"],
            spec["expected_end_date"],
            spec["manifest_sha256"],
            Jsonb(spec["metadata"]),
        ),
    ).fetchone()
    created = inserted is not None
    row = connection.execute(
        """
        SELECT dataset_id, dataset_key, provider, feed, data_schema, root_uri,
               price_scale_exponent, status, expected_start_date, expected_end_date,
               manifest_sha256, metadata
        FROM systematic_fx.datasets WHERE dataset_key = %s FOR UPDATE
        """,
        (spec["dataset_key"],),
    ).fetchone()
    row = _row_or_error(row, label=f"dataset {spec['dataset_key']}")
    _assert_fields(
        label=f"dataset {spec['dataset_key']}",
        row=row,
        expected={
            "dataset_key": spec["dataset_key"],
            "provider": spec["provider"],
            "feed": spec["feed"],
            "data_schema": spec["data_schema"],
            "root_uri": source_root_uri,
            "price_scale_exponent": spec["price_scale_exponent"],
            "expected_start_date": spec["expected_start_date"],
            "expected_end_date": spec["expected_end_date"],
        },
    )
    if row["manifest_sha256"] not in {None, spec["manifest_sha256"]}:
        raise ResearchRegistryDriftError(
            f"dataset {spec['dataset_key']} source manifest SHA-256 drift"
        )
    actual_metadata = row["metadata"]
    for key, value in spec["metadata"].items():
        if key in actual_metadata and actual_metadata[key] != value:
            raise ResearchRegistryDriftError(
                f"dataset {spec['dataset_key']} metadata drift in field {key}"
            )
    if row["manifest_sha256"] is None or any(
        key not in actual_metadata for key in spec["metadata"]
    ):
        connection.execute(
            """
            UPDATE systematic_fx.datasets
            SET manifest_sha256 = COALESCE(manifest_sha256, %s),
                metadata = metadata || %s,
                updated_at = statement_timestamp()
            WHERE dataset_id = %s
            """,
            (spec["manifest_sha256"], Jsonb(spec["metadata"]), row["dataset_id"]),
        )
    if row["status"] in {"REJECTED", "RETIRED"}:
        raise ResearchRegistryDriftError(
            f"dataset {spec['dataset_key']} is in terminal status {row['status']}"
        )
    return int(row["dataset_id"]), created


def _ensure_campaign(
    connection: psycopg.Connection[dict[str, Any]],
    prepared: PreparedParentRegistration,
    dataset_id: int,
    code_commit: str,
) -> tuple[int, bool]:
    spec = prepared.campaign_document
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.campaigns
            (campaign_key, dataset_id, name, status, selected_start_date, selected_end_date,
             roll_cutoff_date, data_manifest_sha256, feature_version, outcome_version,
             cost_model_version, execution_model_version, code_commit, config_sha256,
             split_policy, trial_budget, finalist_budget)
        VALUES (%s, %s, %s, 'DRAFT', NULL, NULL, NULL, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s)
        ON CONFLICT (campaign_key) DO NOTHING
        RETURNING campaign_id
        """,
        (
            spec["campaign_key"],
            dataset_id,
            spec["name"],
            spec["data_manifest_sha256"],
            spec["feature_version"],
            spec["outcome_version"],
            spec["cost_model_version"],
            spec["execution_model_version"],
            code_commit,
            prepared.campaign_config_sha256,
            Jsonb(spec["split_policy"]),
            spec["trial_budget"],
            spec["finalist_budget"],
        ),
    ).fetchone()
    created = inserted is not None
    row = connection.execute(
        """
        SELECT campaign_id, campaign_key, dataset_id, name, status,
               selected_start_date, selected_end_date, roll_cutoff_date,
               data_manifest_sha256, feature_version, outcome_version,
               cost_model_version, execution_model_version, code_commit,
               config_sha256, split_policy, trial_budget, finalist_budget
        FROM systematic_fx.campaigns WHERE campaign_key = %s FOR UPDATE
        """,
        (spec["campaign_key"],),
    ).fetchone()
    row = _row_or_error(row, label=f"campaign {spec['campaign_key']}")
    _assert_fields(
        label=f"campaign {spec['campaign_key']}",
        row=row,
        expected={
            "campaign_key": spec["campaign_key"],
            "dataset_id": dataset_id,
            "name": spec["name"],
            "status": "DRAFT",
            "selected_start_date": None,
            "selected_end_date": None,
            "roll_cutoff_date": None,
            "data_manifest_sha256": spec["data_manifest_sha256"],
            "feature_version": spec["feature_version"],
            "outcome_version": spec["outcome_version"],
            "cost_model_version": spec["cost_model_version"],
            "execution_model_version": spec["execution_model_version"],
            "code_commit": code_commit,
            "config_sha256": prepared.campaign_config_sha256,
            "split_policy": spec["split_policy"],
            "trial_budget": spec["trial_budget"],
            "finalist_budget": spec["finalist_budget"],
        },
    )
    return int(row["campaign_id"]), created


def _ensure_registration_job(
    connection: psycopg.Connection[dict[str, Any]],
    prepared: PreparedParentRegistration,
    dataset_id: int,
) -> tuple[int, bool]:
    job_key = f"{prepared.campaign_key}:register:{prepared.registration_sha256}"
    idempotency_key = (
        f"parent-hypothesis-registration:v1:{prepared.campaign_key}:{prepared.registration_sha256}"
    )
    payload = {
        "campaign_key": prepared.campaign_key,
        "dataset_key": prepared.dataset_key,
        "registration_sha256": prepared.registration_sha256,
        "parent_hypothesis_count": EXPECTED_PARENT_COUNT,
        "family_counts": family_counts(prepared.hypothesis_bundle.hypotheses),
        "performance_execution_blocked": True,
    }
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.jobs
            (job_key, dataset_id, job_type, status, idempotency_key, payload,
             attempts, max_attempts, started_at)
        VALUES (%s, %s, 'REGISTER_PARENT_HYPOTHESES', 'RUNNING', %s, %s, 1, 1,
                statement_timestamp())
        ON CONFLICT DO NOTHING
        RETURNING job_id
        """,
        (job_key, dataset_id, idempotency_key, Jsonb(payload)),
    ).fetchone()
    created = inserted is not None
    row = connection.execute(
        """
        SELECT job_id, job_key, dataset_id, job_type, status, idempotency_key, payload
        FROM systematic_fx.jobs WHERE idempotency_key = %s FOR UPDATE
        """,
        (idempotency_key,),
    ).fetchone()
    row = _row_or_error(row, label=f"registration job {idempotency_key}")
    _assert_fields(
        label=f"registration job {idempotency_key}",
        row=row,
        expected={
            "job_key": job_key,
            "dataset_id": dataset_id,
            "job_type": "REGISTER_PARENT_HYPOTHESES",
            "idempotency_key": idempotency_key,
            "payload": payload,
        },
    )
    if not created and row["status"] != "SUCCEEDED":
        raise ResearchRegistryDriftError(
            f"existing registration job is not SUCCEEDED: {row['status']}"
        )
    return int(row["job_id"]), created


def _file_sha256(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            byte_size += len(chunk)
    return digest.hexdigest(), byte_size


def _ensure_artifact(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    artifact_key: str,
    artifact_type: str,
    path: Path,
    sha256: str,
    byte_size: int,
    media_type: str,
    producer_job_id: int | None,
    metadata: Mapping[str, object],
    permit_uri_reuse: bool = False,
) -> tuple[int, bool]:
    uri = path.resolve().as_uri()
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.artifacts
            (artifact_key, artifact_type, uri, sha256, byte_size, media_type,
             producer_job_id, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING artifact_id
        """,
        (
            artifact_key,
            artifact_type,
            uri,
            sha256,
            byte_size,
            media_type,
            producer_job_id,
            Jsonb(metadata),
        ),
    ).fetchone()
    created = inserted is not None
    rows = connection.execute(
        """
        SELECT artifact_id, artifact_key, artifact_type, uri, sha256, byte_size,
               media_type, producer_job_id, metadata
        FROM systematic_fx.artifacts
        WHERE artifact_key = %s OR uri = %s
        FOR SHARE
        """,
        (artifact_key, uri),
    ).fetchall()
    if len(rows) != 1:
        raise ResearchRegistryDriftError(
            f"artifact key and URI resolve to {len(rows)} rows instead of one"
        )
    row = rows[0]
    expected = {
        "artifact_type": artifact_type,
        "uri": uri,
        "sha256": sha256,
        "byte_size": byte_size,
        "media_type": media_type,
    }
    if not permit_uri_reuse or row["artifact_key"] == artifact_key:
        expected.update(
            {
                "artifact_key": artifact_key,
                "producer_job_id": producer_job_id,
                "metadata": metadata,
            }
        )
    _assert_fields(label=f"artifact {artifact_key}", row=row, expected=expected)
    return int(row["artifact_id"]), created


def _experiment_spec(
    prepared: PreparedParentRegistration,
    hypothesis: HypothesisSpec,
) -> dict[str, object]:
    common_search = prepared.hypothesis_bundle.registration_payload()["search_boundary"]
    search_boundary = {
        **dict(common_search),
        "execution_blocked": True,
        "hypothesis_id": hypothesis.hypothesis_id,
        "title": hypothesis.title,
        "entry_condition": hypothesis.entry_condition,
        "economic_rationale": hypothesis.economic_rationale,
        "features": list(hypothesis.features),
        "interaction_family": hypothesis.interaction_family,
        "trial_budget_semantics": {
            "campaign_limit": EXPECTED_CAMPAIGN_VARIANT_BUDGET,
            "campaign_counts_only": "STRATEGY_VARIANT",
            "experiment_local_limit": prepared.hypothesis_bundle.local_trial_budget,
            "experiment_counts": "ALL_EXPERIMENT_TRIAL_ROWS",
            "local_breakdown": dict(prepared.hypothesis_bundle.local_trial_budget_breakdown),
        },
    }
    return {
        "primary_family": hypothesis.family,
        "hypothesis": hypothesis.hypothesis,
        "direction": hypothesis.direction,
        "model_family": hypothesis.model_family,
        "tick_size": prepared.hypothesis_bundle.tick_size,
        "tick_value": prepared.hypothesis_bundle.tick_value,
        "feature_definition_versions": dict(prepared.hypothesis_bundle.feature_definition_versions),
        "search_boundary": search_boundary,
        "cost_assumptions": dict(prepared.cost_assumptions),
        "execution_assumptions": dict(prepared.execution_assumptions),
        "trial_budget": prepared.hypothesis_bundle.local_trial_budget,
    }


def _ensure_experiment(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    prepared: PreparedParentRegistration,
    hypothesis: HypothesisSpec,
    campaign_id: int,
    registration_artifact_id: int,
    code_commit: str,
) -> tuple[int, bool]:
    experiment_key = f"{prepared.campaign_key}:experiment:{hypothesis.hypothesis_id}:v1"
    spec = _experiment_spec(prepared, hypothesis)
    config_sha256 = canonical_sha256(spec)
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.experiments
            (experiment_key, campaign_id, pattern_id, parent_experiment_id,
             primary_family, status, hypothesis, direction, model_family,
             tick_size, tick_value, feature_definition_versions, search_boundary,
             cost_assumptions, execution_assumptions, trial_budget,
             trials_registered, registration_artifact_id, code_commit, config_sha256)
        VALUES (%s, %s, NULL, NULL, %s, 'REGISTERED', %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, 0, %s, %s, %s)
        ON CONFLICT (experiment_key) DO NOTHING
        RETURNING experiment_id
        """,
        (
            experiment_key,
            campaign_id,
            spec["primary_family"],
            spec["hypothesis"],
            spec["direction"],
            spec["model_family"],
            Decimal(str(spec["tick_size"])),
            Decimal(str(spec["tick_value"])),
            Jsonb(spec["feature_definition_versions"]),
            Jsonb(spec["search_boundary"]),
            Jsonb(spec["cost_assumptions"]),
            Jsonb(spec["execution_assumptions"]),
            spec["trial_budget"],
            registration_artifact_id,
            code_commit,
            config_sha256,
        ),
    ).fetchone()
    created = inserted is not None
    row = connection.execute(
        """
        SELECT experiment_id, experiment_key, campaign_id, pattern_id,
               parent_experiment_id, primary_family, status, hypothesis, direction,
               model_family, tick_size, tick_value, feature_definition_versions,
               search_boundary, cost_assumptions, execution_assumptions, trial_budget,
               registration_artifact_id, code_commit, config_sha256
        FROM systematic_fx.experiments WHERE experiment_key = %s FOR SHARE
        """,
        (experiment_key,),
    ).fetchone()
    row = _row_or_error(row, label=f"experiment {experiment_key}")
    _assert_fields(
        label=f"experiment {experiment_key}",
        row=row,
        expected={
            "experiment_key": experiment_key,
            "campaign_id": campaign_id,
            "pattern_id": None,
            "parent_experiment_id": None,
            "primary_family": spec["primary_family"],
            "status": "REGISTERED",
            "hypothesis": spec["hypothesis"],
            "direction": spec["direction"],
            "model_family": spec["model_family"],
            "tick_size": Decimal(str(spec["tick_size"])),
            "tick_value": Decimal(str(spec["tick_value"])),
            "feature_definition_versions": spec["feature_definition_versions"],
            "search_boundary": spec["search_boundary"],
            "cost_assumptions": spec["cost_assumptions"],
            "execution_assumptions": spec["execution_assumptions"],
            "trial_budget": spec["trial_budget"],
            "registration_artifact_id": registration_artifact_id,
            "code_commit": code_commit,
            "config_sha256": config_sha256,
        },
    )
    return int(row["experiment_id"]), created


def _verify_campaign_budgets(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    campaign_id: int,
    experiment_ids: tuple[int, ...],
    bundle: HypothesisBundle,
) -> None:
    family_rows = connection.execute(
        """
        SELECT primary_family, count(*)::integer AS parent_count
        FROM systematic_fx.experiments
        WHERE campaign_id = %s AND parent_experiment_id IS NULL
        GROUP BY primary_family
        """,
        (campaign_id,),
    ).fetchall()
    counts = {row["primary_family"]: row["parent_count"] for row in family_rows}
    expected = {
        family: EXPECTED_PARENTS_PER_FAMILY for family in sorted(family_counts(bundle.hypotheses))
    }
    if counts != expected:
        raise ResearchRegistryError(
            f"campaign parent-family counts differ from the frozen 10-per-family budget: {counts}"
        )

    descendants = connection.execute(
        """
        SELECT parent_experiment_id, count(*)::integer AS descendant_count
        FROM systematic_fx.experiments
        WHERE campaign_id = %s AND parent_experiment_id IS NOT NULL
        GROUP BY parent_experiment_id HAVING count(*) > %s
        """,
        (campaign_id, bundle.descendants_per_parent),
    ).fetchall()
    if descendants:
        raise ResearchRegistryError("one or more parent experiments exceed descendant budget")

    strategy_variants = connection.execute(
        """
        SELECT count(*)::integer AS variant_count
        FROM systematic_fx.experiment_trials t
        JOIN systematic_fx.experiments e ON e.experiment_id = t.experiment_id
        WHERE e.campaign_id = %s AND t.trial_type = 'STRATEGY_VARIANT'
        """,
        (campaign_id,),
    ).fetchone()
    strategy_variants = _row_or_error(strategy_variants, label="campaign strategy-variant count")
    if strategy_variants["variant_count"] > bundle.campaign_strategy_variant_budget:
        raise ResearchRegistryError("campaign strategy-variant budget is exceeded")

    local_counts = connection.execute(
        """
        SELECT e.experiment_id, e.trial_budget, count(t.experiment_trial_id)::integer AS actual
        FROM systematic_fx.experiments e
        LEFT JOIN systematic_fx.experiment_trials t ON t.experiment_id = e.experiment_id
        WHERE e.experiment_id = ANY(%s)
        GROUP BY e.experiment_id, e.trial_budget
        """,
        (list(experiment_ids),),
    ).fetchall()
    if len(local_counts) != len(experiment_ids):
        raise ResearchRegistryError("not every registered experiment was present for budget audit")
    for row in local_counts:
        if row["actual"] > row["trial_budget"]:
            raise ResearchRegistryError(
                f"experiment {row['experiment_id']} exceeds its local multiplicity budget"
            )
        connection.execute(
            "UPDATE systematic_fx.experiments SET trials_registered = %s WHERE experiment_id = %s",
            (row["actual"], row["experiment_id"]),
        )

    finalists = connection.execute(
        """
        SELECT count(*)::integer AS finalist_count
        FROM systematic_fx.strategies
        WHERE campaign_id = %s AND status IN ('FROZEN', 'VALIDATED', 'PAPER_ELIGIBLE')
        """,
        (campaign_id,),
    ).fetchone()
    finalists = _row_or_error(finalists, label="campaign finalist count")
    if finalists["finalist_count"] > 10:
        raise ResearchRegistryError("campaign frozen-finalist budget is exceeded")


@_translate_psycopg_errors("parent-hypothesis registration")
def register_parent_hypothesis_bundle(
    database_url: str,
    *,
    campaign_config_path: Path,
    hypothesis_config_path: Path,
    cost_config_path: Path,
    execution_config_path: Path,
    data_root: Path,
    artifacts_root: Path,
    code_commit: str,
) -> ParentRegistrationReport:
    """Register the pending dataset, DRAFT campaign, and 60 a-priori parents.

    The canonical artifact is durably written before the database transaction. If
    the transaction fails, that content-addressed file is a harmless orphan and is
    reused byte-for-byte on retry. PostgreSQL rows are committed atomically.
    """

    if not isinstance(database_url, str) or not database_url.strip():
        raise ResearchRegistryError("database_url must be a non-empty string")
    code_commit = _nonempty(code_commit, label="code_commit")
    prepared = prepare_parent_hypothesis_registration(
        campaign_config_path=campaign_config_path,
        hypothesis_config_path=hypothesis_config_path,
        cost_config_path=cost_config_path,
        execution_config_path=execution_config_path,
        code_commit=code_commit,
    )
    source_root = data_root.expanduser().resolve() / "mbp-10"
    if not source_root.is_dir():
        raise ResearchRegistryError(f"MBP-10 source root does not exist: {source_root}")
    artifact_path = _write_registration_artifact(prepared, artifacts_root)

    with psycopg.connect(database_url, row_factory=dict_row) as connection:  # noqa: SIM117
        with connection.transaction():
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (prepared.dataset_key,),
            )
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (prepared.campaign_key,),
            )
            dataset_id, created_dataset = _ensure_dataset(
                connection,
                prepared,
                source_root.resolve().as_uri(),
            )
            campaign_id, created_campaign = _ensure_campaign(
                connection,
                prepared,
                dataset_id,
                code_commit,
            )
            job_id, created_job = _ensure_registration_job(
                connection,
                prepared,
                dataset_id,
            )
            artifact_key = (
                f"{prepared.campaign_key}:parent-hypothesis-registration:"
                f"{prepared.registration_sha256}"
            )
            artifact_metadata = {
                "campaign_key": prepared.campaign_key,
                "dataset_key": prepared.dataset_key,
                "artifact_schema": "systematic_fx.parent_hypothesis_registration.v1",
                "hypothesis_count": EXPECTED_PARENT_COUNT,
                "performance_execution_blocked": True,
            }
            artifact_id, created_artifact = _ensure_artifact(
                connection,
                artifact_key=artifact_key,
                artifact_type="PARENT_HYPOTHESIS_REGISTRATION",
                path=artifact_path,
                sha256=prepared.registration_sha256,
                byte_size=len(prepared.registration_bytes),
                media_type="application/json",
                producer_job_id=job_id,
                metadata=artifact_metadata,
            )

            experiment_ids: list[int] = []
            created_experiments = 0
            for hypothesis in prepared.hypothesis_bundle.hypotheses:
                experiment_id, created = _ensure_experiment(
                    connection,
                    prepared=prepared,
                    hypothesis=hypothesis,
                    campaign_id=campaign_id,
                    registration_artifact_id=artifact_id,
                    code_commit=code_commit,
                )
                experiment_ids.append(experiment_id)
                created_experiments += int(created)
            experiment_ids_tuple = tuple(experiment_ids)
            _verify_campaign_budgets(
                connection,
                campaign_id=campaign_id,
                experiment_ids=experiment_ids_tuple,
                bundle=prepared.hypothesis_bundle,
            )

            if created_job:
                result = {
                    "campaign_id": campaign_id,
                    "artifact_id": artifact_id,
                    "experiment_count": len(experiment_ids_tuple),
                    "created_experiments": created_experiments,
                    "pattern_rows_created": 0,
                    "performance_execution_blocked": True,
                }
                connection.execute(
                    """
                    UPDATE systematic_fx.jobs
                    SET status = 'SUCCEEDED', result = %s, finished_at = statement_timestamp()
                    WHERE job_id = %s
                    """,
                    (Jsonb(result), job_id),
                )

    return ParentRegistrationReport(
        dataset_id=dataset_id,
        dataset_key=prepared.dataset_key,
        campaign_id=campaign_id,
        campaign_key=prepared.campaign_key,
        job_id=job_id,
        artifact_id=artifact_id,
        artifact_path=artifact_path,
        artifact_sha256=prepared.registration_sha256,
        experiment_ids=experiment_ids_tuple,
        created_dataset=created_dataset,
        created_campaign=created_campaign,
        created_job=created_job,
        created_artifact=created_artifact,
        created_experiments=created_experiments,
    )


def _aware_utc(value: datetime, *, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ResearchRegistryError(f"{label} must be a timezone-aware datetime")
    return value.astimezone(UTC)


@_translate_psycopg_errors("Discovery exposure registration")
def record_discovery_exposure(
    database_url: str,
    *,
    campaign_key: str,
    exposure_key: str,
    exposure_type: str,
    source_interval_start: datetime,
    source_interval_end: datetime,
    query_spec: Mapping[str, object],
    result_summary: Mapping[str, object],
    visible_to_ai: bool,
    research_eligible: bool,
    code_commit: str,
    config_sha256: str,
    result_artifact_path: Path | None = None,
    artifacts_root: Path | None = None,
) -> DiscoveryExposureReport:
    """Record one deterministic Discovery/pilot exposure without content updates."""

    if not isinstance(database_url, str) or not database_url.strip():
        raise ResearchRegistryError("database_url must be a non-empty string")
    campaign_key = _nonempty(campaign_key, label="campaign_key")
    exposure_key = _nonempty(exposure_key, label="exposure_key")
    exposure_type = _nonempty(exposure_type, label="exposure_type").upper()
    if exposure_type not in _EXPOSURE_TYPES:
        raise ResearchRegistryError(f"unsupported exposure_type: {exposure_type}")
    start = _aware_utc(source_interval_start, label="source_interval_start")
    end = _aware_utc(source_interval_end, label="source_interval_end")
    if start > end:
        raise ResearchRegistryError("source exposure interval is reversed")
    if not isinstance(query_spec, Mapping) or not isinstance(result_summary, Mapping):
        raise ResearchRegistryError("query_spec and result_summary must be mappings")
    code_commit = _nonempty(code_commit, label="code_commit")
    if not isinstance(config_sha256, str) or not _SHA256.fullmatch(config_sha256):
        raise ResearchRegistryError("config_sha256 must be lowercase SHA-256")
    if not isinstance(visible_to_ai, bool) or not isinstance(research_eligible, bool):
        raise ResearchRegistryError("exposure visibility flags must be boolean")

    artifact_spec: tuple[Path, str, int] | None = None
    if result_artifact_path is not None:
        if artifacts_root is None:
            raise ResearchRegistryError("artifacts_root is required with result_artifact_path")
        artifact_path = _contained_path(
            result_artifact_path,
            artifacts_root,
            label="Discovery result artifact",
        )
        if not artifact_path.is_file():
            raise ResearchRegistryError(
                f"Discovery result artifact does not exist: {artifact_path}"
            )
        sha256, byte_size = _file_sha256(artifact_path)
        artifact_spec = (artifact_path, sha256, byte_size)

    with psycopg.connect(database_url, row_factory=dict_row) as connection:  # noqa: SIM117
        with connection.transaction():
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (campaign_key,),
            )
            campaign = connection.execute(
                """
                SELECT campaign_id, status FROM systematic_fx.campaigns
                WHERE campaign_key = %s FOR SHARE
                """,
                (campaign_key,),
            ).fetchone()
            campaign = _row_or_error(campaign, label=f"campaign {campaign_key}")
            if campaign["status"] in {"CLOSED", "ABORTED"}:
                raise ResearchRegistryError(
                    f"cannot add Discovery exposure to {campaign['status']} campaign"
                )
            campaign_id = int(campaign["campaign_id"])

            result_artifact_id: int | None = None
            created_artifact = False
            if artifact_spec is not None:
                artifact_path, artifact_sha256, byte_size = artifact_spec
                result_artifact_id, created_artifact = _ensure_artifact(
                    connection,
                    artifact_key=(
                        f"{campaign_key}:discovery-exposure:{exposure_key}:{artifact_sha256}"
                    ),
                    artifact_type="DISCOVERY_EXPOSURE_RESULT",
                    path=artifact_path,
                    sha256=artifact_sha256,
                    byte_size=byte_size,
                    media_type="application/json",
                    producer_job_id=None,
                    metadata={
                        "campaign_key": campaign_key,
                        "exposure_key": exposure_key,
                        "exposure_type": exposure_type,
                    },
                    permit_uri_reuse=True,
                )

            inserted = connection.execute(
                """
                INSERT INTO systematic_fx.discovery_exposures
                    (exposure_key, campaign_id, exposure_type, source_interval_start,
                     source_interval_end, visible_to_ai, research_eligible, query_spec,
                     result_summary, result_artifact_id, code_commit, config_sha256)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (exposure_key) DO NOTHING
                RETURNING discovery_exposure_id
                """,
                (
                    exposure_key,
                    campaign_id,
                    exposure_type,
                    start,
                    end,
                    visible_to_ai,
                    research_eligible,
                    Jsonb(dict(query_spec)),
                    Jsonb(dict(result_summary)),
                    result_artifact_id,
                    code_commit,
                    config_sha256,
                ),
            ).fetchone()
            created_exposure = inserted is not None
            row = connection.execute(
                """
                SELECT discovery_exposure_id, exposure_key, campaign_id, exposure_type,
                       source_interval_start, source_interval_end, visible_to_ai,
                       research_eligible, query_spec, result_summary, result_artifact_id,
                       code_commit, config_sha256
                FROM systematic_fx.discovery_exposures
                WHERE exposure_key = %s FOR SHARE
                """,
                (exposure_key,),
            ).fetchone()
            row = _row_or_error(row, label=f"Discovery exposure {exposure_key}")
            _assert_fields(
                label=f"Discovery exposure {exposure_key}",
                row=row,
                expected={
                    "exposure_key": exposure_key,
                    "campaign_id": campaign_id,
                    "exposure_type": exposure_type,
                    "source_interval_start": start,
                    "source_interval_end": end,
                    "visible_to_ai": visible_to_ai,
                    "research_eligible": research_eligible,
                    "query_spec": dict(query_spec),
                    "result_summary": dict(result_summary),
                    "result_artifact_id": result_artifact_id,
                    "code_commit": code_commit,
                    "config_sha256": config_sha256,
                },
            )

    return DiscoveryExposureReport(
        discovery_exposure_id=int(row["discovery_exposure_id"]),
        exposure_key=exposure_key,
        campaign_id=campaign_id,
        result_artifact_id=result_artifact_id,
        created_exposure=created_exposure,
        created_artifact=created_artifact,
    )
