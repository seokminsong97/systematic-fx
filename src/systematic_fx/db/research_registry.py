"""Transactional, drift-rejecting persistence for Phase 1 research registration."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from functools import wraps
from itertools import pairwise
from pathlib import Path
from typing import Any, Literal, ParamSpec, TypeVar
from urllib.parse import unquote, urlparse

import psycopg
from psycopg import IsolationLevel
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.db.postgres_retry import retry_serialization_failures
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
_PHASE1A_CAMPAIGN_KEY = "phase1a_conservative_screening_v1"
_PHASE1A_QUERY_COUNT = 11
_PHASE1A_SLICE_SOURCE_DATE_COUNT = 5
_PHASE1A_PATTERN_ROLLUP_SCHEMA = "systematic_fx.phase1a_pattern_rollup.v1"
_PHASE1A_RECOVERY_CONTROL_SCHEMA = "systematic_fx.phase1a_partial_recovery_control.v1"
_PHASE1A_RECOVERY_MANIFEST_SCHEMA = "systematic_fx.phase1a_partial_recovery_manifest.v1"
_PHASE1A_RECOVERY_ENGINE = "phase1a_partial_recovery_control_v1"
_PHASE1A_QUERY_ENGINE = "phase1a_fixed_query_projection_v1"
_PHASE1A_RECOVERY_PROJECTION_SCHEMA = "systematic_fx.phase1a_query_recovery_projection.v1"
_PHASE1A_RECOVERY_REGISTRAR_SCHEMA = "systematic_fx.phase1a_pattern_recovery_registrar.v1"
_PHASE1A_DISCOVERY_ARTIFACT_SCHEMA = "systematic_fx.phase1a_discovery_slice.v1"
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
                return retry_serialization_failures(function, *args, **kwargs)
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
    research_run_spec_id: int | None
    result_artifact_id: int | None
    created_exposure: bool
    created_artifact: bool


@dataclass(frozen=True, slots=True)
class Phase1APredecessorSliceReport:
    """Exact immutable identities proving one prior Discovery slice is complete."""

    prior_slice_index: int
    ai_exposure_id: int
    query_exposure_ids: tuple[int, ...]
    pattern_ids: tuple[int, ...]
    result_artifact_id: int


@dataclass(frozen=True, slots=True)
class Phase1ACurrentSlicePrefixReport:
    """A clean, failed-feature-retryable, or exact resumable slice boundary."""

    slice_index: int
    state: Literal["EMPTY", "FAILED_FEATURE_RETRYABLE", "RESUMABLE"]
    feature_run_spec_id: int | None
    ai_exposure_id: int | None
    query_exposure_ids: tuple[int, ...]
    pattern_ids: tuple[int, ...]
    result_artifact_id: int | None
    missing_pattern_query_id: str | None


@dataclass(frozen=True, slots=True)
class Phase1ARecoveryQuerySource:
    """One already-published QUERY in a validated partial slice prefix."""

    query_id: str
    research_run_spec_id: int
    run_fingerprint: str
    success_attempt_id: int
    discovery_exposure_id: int
    canonical_spec: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class Phase1APartialRecoverySource:
    """Immutable source identities used to recover registration after code revision."""

    campaign_id: int
    slice_index: int
    feature_run_spec_id: int
    feature_run_fingerprint: str
    feature_success_attempt_id: int
    feature_result_artifact_id: int
    feature_canonical_spec: Mapping[str, Any]
    ai_run_spec_id: int
    ai_run_fingerprint: str
    ai_success_attempt_id: int
    ai_exposure_id: int
    ai_canonical_spec: Mapping[str, Any]
    query_prefix: tuple[Phase1ARecoveryQuerySource, ...]
    pattern_ids: tuple[int, ...]
    missing_pattern_query_id: str | None
    result_artifact_id: int
    result_artifact_uri: str
    result_artifact_sha256: str
    result_artifact_byte_size: int


@dataclass(frozen=True, slots=True)
class Phase1ARecoveryManifestReport:
    """Immutable artifact owned by one successful partial-recovery control run."""

    research_run_spec_id: int
    research_run_attempt_id: int
    result_artifact_id: int
    run_fingerprint: str
    manifest_sha256: str
    manifest_uri: str
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


def _file_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _open_hashed_regular_file(path: Path) -> tuple[int, str, int, tuple[int, ...]]:
    """Open one non-symlink inode and hash bytes without releasing its identity."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ResearchRegistryError(f"cannot open immutable artifact: {path}") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ResearchRegistryError(f"immutable artifact is not a regular file: {path}")
        identity = _file_identity(before)
        digest = hashlib.sha256()
        byte_size = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
            byte_size += len(chunk)
        if _file_identity(os.fstat(descriptor)) != identity or byte_size != before.st_size:
            raise ResearchRegistryDriftError(
                f"immutable artifact changed while it was hashed: {path}"
            )
        return descriptor, digest.hexdigest(), byte_size, identity
    except Exception:
        os.close(descriptor)
        raise


def _verify_open_file_binding(
    descriptor: int,
    path: Path,
    identity: tuple[int, ...],
) -> None:
    """Prove both the open inode and its content-addressed path still identify one file."""

    if _file_identity(os.fstat(descriptor)) != identity:
        raise ResearchRegistryDriftError(
            f"immutable artifact inode changed before database commit: {path}"
        )
    try:
        path_identity = _file_identity(path.lstat())
    except OSError as exc:
        raise ResearchRegistryDriftError(
            f"immutable artifact path disappeared before database commit: {path}"
        ) from exc
    if path_identity != identity or not stat.S_ISREG(path_identity[2]):
        raise ResearchRegistryDriftError(
            f"immutable artifact path changed before database commit: {path}"
        )


def _file_sha256(path: Path) -> tuple[str, int]:
    descriptor, sha256, byte_size, _ = _open_hashed_regular_file(path)
    try:
        return sha256, byte_size
    finally:
        os.close(descriptor)


def _verify_phase1a_artifact_file(row: Mapping[str, Any]) -> None:
    """Fail closed unless a registered Discovery artifact is still byte-reachable."""

    uri = row.get("artifact_uri")
    if not isinstance(uri, str):
        raise ResearchRegistryDriftError("Phase 1A result artifact URI is missing")
    parsed = urlparse(uri)
    if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
        raise ResearchRegistryDriftError("Phase 1A result artifact must use a local file URI")
    path = Path(unquote(parsed.path))
    expected_sha256 = _phase1a_sha256(
        row.get("artifact_sha256"),
        label="Phase 1A result artifact",
    )
    expected_size = row.get("artifact_byte_size")
    if isinstance(expected_size, bool) or not isinstance(expected_size, int) or expected_size <= 0:
        raise ResearchRegistryDriftError("Phase 1A result artifact byte size is invalid")
    if not path.is_absolute() or path.name != f"sha256={expected_sha256}.json":
        raise ResearchRegistryDriftError("Phase 1A result artifact URI is not content-addressed")
    parts = path.parts
    if not any(parts[index : index + 2] == ("data", "derived") for index in range(len(parts) - 1)):
        raise ResearchRegistryDriftError("Phase 1A result artifact is outside data/derived")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise ResearchRegistryDriftError("Phase 1A result artifact is no longer reachable") from exc
    if resolved != path or path.is_symlink():
        raise ResearchRegistryDriftError("Phase 1A result artifact path contains a symlink")
    descriptor, observed_sha256, observed_size, identity = _open_hashed_regular_file(path)
    try:
        _verify_open_file_binding(descriptor, path, identity)
    finally:
        os.close(descriptor)
    if observed_sha256 != expected_sha256 or observed_size != expected_size:
        raise ResearchRegistryDriftError("Phase 1A result artifact content drift")


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
    run_fingerprint: str | None = None,
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
    if run_fingerprint is not None and (
        not isinstance(run_fingerprint, str) or not _SHA256.fullmatch(run_fingerprint)
    ):
        raise ResearchRegistryError("run_fingerprint must be a lowercase SHA-256")
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

            research_run_spec_id: int | None = None
            if run_fingerprint is not None:
                run_spec = connection.execute(
                    """
                    SELECT research_run_spec_id, campaign_id, run_kind, code_commit
                    FROM systematic_fx.research_run_specs
                    WHERE run_fingerprint = %s
                    FOR SHARE
                    """,
                    (run_fingerprint,),
                ).fetchone()
                run_spec = _row_or_error(
                    run_spec,
                    label=f"research run specification {run_fingerprint}",
                )
                expected_run_kind = "AI_SLICE" if exposure_type == "AI_SLICE" else "QUERY"
                _assert_fields(
                    label=f"research run specification {run_fingerprint}",
                    row=run_spec,
                    expected={
                        "campaign_id": campaign_id,
                        "code_commit": code_commit,
                        "run_kind": expected_run_kind,
                    },
                )
                research_run_spec_id = int(run_spec["research_run_spec_id"])

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
                     result_summary, result_artifact_id, code_commit, config_sha256,
                     research_run_spec_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    research_run_spec_id,
                ),
            ).fetchone()
            created_exposure = inserted is not None
            row = connection.execute(
                """
                SELECT discovery_exposure_id, exposure_key, campaign_id, exposure_type,
                       source_interval_start, source_interval_end, visible_to_ai,
                       research_eligible, query_spec, result_summary, result_artifact_id,
                       code_commit, config_sha256, research_run_spec_id
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
                    "research_run_spec_id": research_run_spec_id,
                },
            )

    return DiscoveryExposureReport(
        discovery_exposure_id=int(row["discovery_exposure_id"]),
        exposure_key=exposure_key,
        campaign_id=campaign_id,
        research_run_spec_id=research_run_spec_id,
        result_artifact_id=result_artifact_id,
        created_exposure=created_exposure,
        created_artifact=created_artifact,
    )


def _verify_phase1a_child_parent_success(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    child_run_spec: Mapping[str, Any],
    exposure_type: str,
    exposure_result_summary: Mapping[str, object] | None = None,
) -> tuple[int, str, int, str]:
    """Prove a Discovery child names its parent's one immutable success artifact."""

    parent_run_spec_id = child_run_spec.get("parent_run_spec_id")
    if (
        isinstance(parent_run_spec_id, bool)
        or not isinstance(parent_run_spec_id, int)
        or parent_run_spec_id <= 0
    ):
        raise ResearchRegistryDriftError(
            f"{exposure_type} RunSpec requires an exact parent_run_spec_id"
        )
    canonical_spec = _phase1a_json_object(
        child_run_spec.get("canonical_spec"),
        label=f"{exposure_type} child RunSpec",
    )
    parameters = _phase1a_json_object(
        canonical_spec.get("parameters"),
        label=f"{exposure_type} child RunSpec parameters",
    )
    parent_rows = connection.execute(
        """
        SELECT parent.research_run_spec_id, parent.run_fingerprint, parent.run_kind,
               attempt.result_artifact_id, artifact.sha256 AS artifact_sha256,
               artifact.artifact_type
        FROM systematic_fx.research_run_specs AS parent
        JOIN systematic_fx.research_run_attempts AS attempt
          ON attempt.research_run_spec_id = parent.research_run_spec_id
         AND attempt.status = 'SUCCEEDED'
        JOIN systematic_fx.artifacts AS artifact
          ON artifact.artifact_id = attempt.result_artifact_id
        WHERE parent.research_run_spec_id = %s
          AND parent.campaign_id = %s
        FOR SHARE OF parent, attempt, artifact
        """,
        (parent_run_spec_id, child_run_spec.get("campaign_id")),
    ).fetchall()
    if len(parent_rows) != 1:
        raise ResearchRegistryDriftError(
            f"{exposure_type} parent must have exactly one SUCCEEDED artifact"
        )
    parent = parent_rows[0]
    parent_fingerprint = _phase1a_sha256(
        parent.get("run_fingerprint"),
        label=f"{exposure_type} parent run fingerprint",
    )
    artifact_sha256 = _phase1a_sha256(
        parent.get("artifact_sha256"),
        label=f"{exposure_type} parent artifact",
    )
    expected_parent_kind = "FEATURE_BUILD" if exposure_type == "AI_SLICE" else "AI_SLICE"
    expected_artifact_type = (
        "PHASE1A_FEATURE_BUILD_MANIFEST"
        if exposure_type == "AI_SLICE"
        else "DISCOVERY_EXPOSURE_RESULT"
    )
    parameter_sha_key = (
        "feature_manifest_sha256" if exposure_type == "AI_SLICE" else "discovery_artifact_sha256"
    )
    if (
        parent.get("run_kind") != expected_parent_kind
        or parent.get("artifact_type") != expected_artifact_type
        or parameters.get("parent_run_fingerprint") != parent_fingerprint
        or parameters.get(parameter_sha_key) != artifact_sha256
    ):
        raise ResearchRegistryDriftError(f"{exposure_type} parent artifact lineage drift")
    if exposure_result_summary is not None:
        summary_sha_key = (
            "feature_manifest_sha256" if exposure_type == "AI_SLICE" else "artifact_sha256"
        )
        if exposure_result_summary.get(summary_sha_key) != artifact_sha256:
            raise ResearchRegistryDriftError(
                f"{exposure_type} result summary parent artifact SHA-256 drift"
            )
    result_artifact_id = parent.get("result_artifact_id")
    if (
        isinstance(result_artifact_id, bool)
        or not isinstance(result_artifact_id, int)
        or result_artifact_id <= 0
    ):
        raise ResearchRegistryDriftError(f"{exposure_type} parent artifact ID is invalid")
    return parent_run_spec_id, parent_fingerprint, result_artifact_id, artifact_sha256


@_translate_psycopg_errors("atomic Phase 1A Discovery run completion")
def complete_discovery_run_success(
    database_url: str,
    *,
    research_run_attempt_id: int,
    campaign_key: str,
    exposure_key: str,
    exposure_type: str,
    source_interval_start: datetime,
    source_interval_end: datetime,
    query_spec: Mapping[str, object],
    exposure_result_summary: Mapping[str, object],
    attempt_result_summary: Mapping[str, object],
    code_commit: str,
    config_sha256: str,
    run_fingerprint: str,
    expected_artifact_sha256: str,
    result_artifact_path: Path,
    artifacts_root: Path,
) -> DiscoveryExposureReport:
    """Atomically publish one governed result, terminal attempt, and AI exposure.

    The Phase 1A exposure is append-preserved by the database.  It therefore may
    not become visible in an earlier transaction than the successful attempt that
    owns its exact result artifact.
    """

    if not isinstance(database_url, str) or not database_url.strip():
        raise ResearchRegistryError("database_url must be a non-empty string")
    attempt_id = _positive_integer(
        research_run_attempt_id,
        label="research_run_attempt_id",
    )
    campaign_key = _nonempty(campaign_key, label="campaign_key")
    if campaign_key != _PHASE1A_CAMPAIGN_KEY:
        raise ResearchRegistryError(
            "atomic Discovery completion is restricted to the Phase 1A campaign"
        )
    exposure_key = _nonempty(exposure_key, label="exposure_key")
    exposure_type = _nonempty(exposure_type, label="exposure_type").upper()
    if exposure_type not in {"AI_SLICE", "QUERY"}:
        raise ResearchRegistryError("Phase 1A completion requires AI_SLICE or QUERY")
    start = _aware_utc(source_interval_start, label="source_interval_start")
    end = _aware_utc(source_interval_end, label="source_interval_end")
    if start >= end:
        raise ResearchRegistryError("Phase 1A source exposure interval must be positive")
    if not isinstance(query_spec, Mapping):
        raise ResearchRegistryError("query_spec must be a mapping")
    if not isinstance(exposure_result_summary, Mapping):
        raise ResearchRegistryError("exposure_result_summary must be a mapping")
    if not isinstance(attempt_result_summary, Mapping):
        raise ResearchRegistryError("attempt_result_summary must be a mapping")
    query_document = dict(query_spec)
    exposure_summary = dict(exposure_result_summary)
    attempt_summary = dict(attempt_result_summary)
    code_commit = _nonempty(code_commit, label="code_commit")
    if not isinstance(config_sha256, str) or not _SHA256.fullmatch(config_sha256):
        raise ResearchRegistryError("config_sha256 must be lowercase SHA-256")
    if not isinstance(run_fingerprint, str) or not _SHA256.fullmatch(run_fingerprint):
        raise ResearchRegistryError("run_fingerprint must be a lowercase SHA-256")
    expected_artifact_sha256 = _phase1a_sha256(
        expected_artifact_sha256,
        label="expected_artifact_sha256",
    )
    if attempt_summary.get("artifact_sha256") != expected_artifact_sha256:
        raise ResearchRegistryError(
            "attempt_result_summary artifact_sha256 must equal the expected artifact"
        )

    artifact_path = _contained_path(
        result_artifact_path,
        artifacts_root,
        label="Discovery result artifact",
    )
    if artifact_path.is_symlink() or not artifact_path.is_file():
        raise ResearchRegistryError(
            f"Discovery result artifact is not a regular file: {artifact_path}"
        )
    if artifact_path.name != f"sha256={expected_artifact_sha256}.json":
        raise ResearchRegistryError(
            "Discovery result artifact filename must contain its expected SHA-256"
        )
    artifact_descriptor, artifact_sha256, artifact_byte_size, artifact_identity = (
        _open_hashed_regular_file(artifact_path)
    )
    if artifact_sha256 != expected_artifact_sha256:
        os.close(artifact_descriptor)
        raise ResearchRegistryDriftError(
            "Discovery result artifact differs from its expected SHA-256"
        )

    with (
        os.fdopen(artifact_descriptor, "rb", closefd=True),
        psycopg.connect(
            database_url,
            row_factory=dict_row,
        ) as connection,
    ):
        connection.isolation_level = IsolationLevel.SERIALIZABLE
        with connection.transaction():
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (campaign_key,),
            )
            campaign = connection.execute(
                """
                SELECT campaign_id, status
                FROM systematic_fx.campaigns
                WHERE campaign_key = %s
                FOR SHARE
                """,
                (campaign_key,),
            ).fetchone()
            campaign = _row_or_error(campaign, label=f"campaign {campaign_key}")
            if campaign["status"] in {"CLOSED", "ABORTED"}:
                raise ResearchRegistryError(
                    f"cannot complete Discovery run in {campaign['status']} campaign"
                )
            campaign_id = int(campaign["campaign_id"])

            run_spec = connection.execute(
                """
                SELECT research_run_spec_id, campaign_id, parent_run_spec_id,
                       run_kind, code_commit, canonical_spec
                FROM systematic_fx.research_run_specs
                WHERE run_fingerprint = %s
                FOR SHARE
                """,
                (run_fingerprint,),
            ).fetchone()
            run_spec = _row_or_error(
                run_spec,
                label=f"research run specification {run_fingerprint}",
            )
            expected_run_kind = "AI_SLICE" if exposure_type == "AI_SLICE" else "QUERY"
            _assert_fields(
                label=f"research run specification {run_fingerprint}",
                row=run_spec,
                expected={
                    "campaign_id": campaign_id,
                    "code_commit": code_commit,
                    "run_kind": expected_run_kind,
                },
            )
            research_run_spec_id = int(run_spec["research_run_spec_id"])
            _, _, parent_result_artifact_id, _ = _verify_phase1a_child_parent_success(
                connection,
                child_run_spec=run_spec,
                exposure_type=exposure_type,
                exposure_result_summary=exposure_summary,
            )

            attempt = connection.execute(
                """
                SELECT research_run_attempt_id, research_run_spec_id, status, started_at
                FROM systematic_fx.research_run_attempts
                WHERE research_run_attempt_id = %s
                FOR UPDATE
                """,
                (attempt_id,),
            ).fetchone()
            attempt = _row_or_error(attempt, label=f"research run attempt {attempt_id}")
            _assert_fields(
                label=f"research run attempt {attempt_id}",
                row=attempt,
                expected={
                    "research_run_spec_id": research_run_spec_id,
                    "status": "RUNNING",
                },
            )
            if attempt["started_at"] is None:
                raise ResearchRegistryDriftError(
                    f"research run attempt {attempt_id} is RUNNING without started_at"
                )

            result_artifact_id, created_artifact = _ensure_artifact(
                connection,
                artifact_key=(
                    f"{campaign_key}:discovery-exposure:{exposure_key}:{artifact_sha256}"
                ),
                artifact_type="DISCOVERY_EXPOSURE_RESULT",
                path=artifact_path,
                sha256=artifact_sha256,
                byte_size=artifact_byte_size,
                media_type="application/json",
                producer_job_id=None,
                metadata={
                    "campaign_key": campaign_key,
                    "exposure_key": exposure_key,
                    "exposure_type": exposure_type,
                    "run_fingerprint": run_fingerprint,
                },
                permit_uri_reuse=True,
            )
            if exposure_type == "QUERY" and result_artifact_id != parent_result_artifact_id:
                raise ResearchRegistryDriftError(
                    "QUERY result artifact must be its AI_SLICE parent result artifact"
                )

            transitioned = connection.execute(
                """
                UPDATE systematic_fx.research_run_attempts
                SET status = 'SUCCEEDED', result_artifact_id = %s,
                    result_summary = %s, error_message = NULL,
                    finished_at = statement_timestamp()
                WHERE research_run_attempt_id = %s AND status = 'RUNNING'
                RETURNING research_run_attempt_id
                """,
                (result_artifact_id, Jsonb(attempt_summary), attempt_id),
            ).fetchone()
            if transitioned is None:
                raise ResearchRegistryDriftError(
                    f"research run attempt {attempt_id} lost RUNNING state"
                )

            inserted = connection.execute(
                """
                INSERT INTO systematic_fx.discovery_exposures
                    (exposure_key, campaign_id, exposure_type, source_interval_start,
                     source_interval_end, visible_to_ai, research_eligible, query_spec,
                     result_summary, result_artifact_id, code_commit, config_sha256,
                     research_run_spec_id)
                VALUES (%s, %s, %s, %s, %s, true, false, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (exposure_key) DO NOTHING
                RETURNING discovery_exposure_id
                """,
                (
                    exposure_key,
                    campaign_id,
                    exposure_type,
                    start,
                    end,
                    Jsonb(query_document),
                    Jsonb(exposure_summary),
                    result_artifact_id,
                    code_commit,
                    config_sha256,
                    research_run_spec_id,
                ),
            ).fetchone()
            created_exposure = inserted is not None
            row = connection.execute(
                """
                SELECT discovery_exposure_id, exposure_key, campaign_id, exposure_type,
                       source_interval_start, source_interval_end, visible_to_ai,
                       research_eligible, query_spec, result_summary, result_artifact_id,
                       code_commit, config_sha256, research_run_spec_id
                FROM systematic_fx.discovery_exposures
                WHERE exposure_key = %s
                FOR SHARE
                """,
                (exposure_key,),
            ).fetchone()
            row = _row_or_error(row, label=f"Discovery exposure {exposure_key}")
            _assert_fields(
                label=f"Discovery exposure {exposure_key}",
                row=row,
                expected={
                    "campaign_id": campaign_id,
                    "code_commit": code_commit,
                    "config_sha256": config_sha256,
                    "exposure_key": exposure_key,
                    "exposure_type": exposure_type,
                    "query_spec": query_document,
                    "research_eligible": False,
                    "research_run_spec_id": research_run_spec_id,
                    "result_artifact_id": result_artifact_id,
                    "result_summary": exposure_summary,
                    "source_interval_end": end,
                    "source_interval_start": start,
                    "visible_to_ai": True,
                },
            )
            _verify_open_file_binding(
                artifact_descriptor,
                artifact_path,
                artifact_identity,
            )

    return DiscoveryExposureReport(
        discovery_exposure_id=int(row["discovery_exposure_id"]),
        exposure_key=exposure_key,
        campaign_id=campaign_id,
        research_run_spec_id=research_run_spec_id,
        result_artifact_id=result_artifact_id,
        created_exposure=created_exposure,
        created_artifact=created_artifact,
    )


def _read_open_descriptor(descriptor: int) -> bytes:
    try:
        os.lseek(descriptor, 0, os.SEEK_SET)
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 1024 * 1024):
            chunks.append(chunk)
        return b"".join(chunks)
    except OSError as exc:
        raise ResearchRegistryError("cannot read immutable recovery manifest") from exc


def _validate_phase1a_recovery_manifest_document(
    content: bytes,
    *,
    campaign_key: str,
    run_spec: Mapping[str, Any],
    parent_spec: Mapping[str, Any],
    parent_run_spec_id: int,
    parent_run_fingerprint: str,
    parent_success_attempt_id: int,
    parent_ai_exposure_id: int,
    parent_result_artifact_id: int,
    parent_result_artifact_sha256: str,
    parent_result_artifact_byte_size: int,
    parent_result_artifact_relative_path: str,
    feature_run_spec_id: int,
    feature_run_fingerprint: str,
    feature_success_attempt_id: int,
    feature_result_artifact_id: int,
    feature_spec: Mapping[str, Any],
    manifest_sha256: str,
    manifest_relative_path: str,
) -> dict[str, Any]:
    try:
        document = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ResearchRegistryDriftError("partial-recovery manifest is not valid JSON") from exc
    if not isinstance(document, dict) or canonical_json_bytes(document) + b"\n" != content:
        raise ResearchRegistryDriftError(
            "partial-recovery manifest is not canonical newline-terminated JSON"
        )
    _phase1a_exact_keys(
        document,
        expected={
            "artifact_schema",
            "campaign_key",
            "no_research_recomputation",
            "pipeline_version",
            "planned_actions",
            "query_evidence",
            "recovery_execution",
            "requested_source_dates",
            "slice_index",
            "source_prefix",
        },
        label="partial-recovery manifest",
    )
    if (
        document.get("artifact_schema") != _PHASE1A_RECOVERY_MANIFEST_SCHEMA
        or document.get("campaign_key") != campaign_key
        or document.get("no_research_recomputation") is not True
    ):
        raise ResearchRegistryDriftError("partial-recovery manifest identity drift")

    parameters = _phase1a_json_object(
        run_spec.get("parameters"),
        label="partial-recovery control parameters",
    )
    _phase1a_exact_keys(
        parameters,
        expected={
            "artifact_schema",
            "discovery_artifact_sha256",
            "no_research_recomputation",
            "parent_run_fingerprint",
            "pipeline_version",
            "recovery_manifest_relative_path",
            "recovery_manifest_sha256",
            "requested_source_dates",
            "slice_index",
            "source_ai_canonical_sha256",
            "source_ai_code_snapshot_sha256",
            "source_artifact_id",
            "source_artifact_relative_path",
        },
        label="partial-recovery control parameters",
    )
    expected_parameters = {
        "artifact_schema": _PHASE1A_RECOVERY_CONTROL_SCHEMA,
        "discovery_artifact_sha256": parent_result_artifact_sha256,
        "no_research_recomputation": True,
        "parent_run_fingerprint": parent_run_fingerprint,
        "pipeline_version": document.get("pipeline_version"),
        "recovery_manifest_relative_path": manifest_relative_path,
        "recovery_manifest_sha256": manifest_sha256,
        "requested_source_dates": document.get("requested_source_dates"),
        "slice_index": document.get("slice_index"),
        "source_ai_canonical_sha256": canonical_sha256(parent_spec),
        "source_ai_code_snapshot_sha256": parent_spec.get("code_snapshot_sha256"),
        "source_artifact_id": parent_result_artifact_id,
        "source_artifact_relative_path": parent_result_artifact_relative_path,
    }
    if parameters != expected_parameters:
        raise ResearchRegistryDriftError("partial-recovery control parameter drift")
    parent_parameters = _phase1a_json_object(
        parent_spec.get("parameters"),
        label="partial-recovery source AI parameters",
    )
    if (
        document.get("pipeline_version") != parent_parameters.get("pipeline_version")
        or document.get("requested_source_dates") != parent_parameters.get("requested_source_dates")
        or document.get("slice_index") != parent_parameters.get("slice_index")
    ):
        raise ResearchRegistryDriftError(
            "partial-recovery manifest differs from source AI slice identity"
        )

    source_prefix = _phase1a_json_object(
        document.get("source_prefix"),
        label="partial-recovery source_prefix",
    )
    _phase1a_exact_keys(
        source_prefix,
        expected={
            "ai",
            "discovery_artifact",
            "existing_pattern_ids",
            "existing_queries",
            "feature",
            "missing_pattern_query_id",
        },
        label="partial-recovery source_prefix",
    )
    ai_source = _phase1a_json_object(
        source_prefix.get("ai"),
        label="partial-recovery source AI",
    )
    _phase1a_exact_keys(
        ai_source,
        expected={
            "canonical_sha256",
            "code_commit",
            "code_snapshot_sha256",
            "discovery_exposure_id",
            "research_run_spec_id",
            "run_fingerprint",
            "success_attempt_id",
        },
        label="partial-recovery source AI",
    )
    if (
        ai_source.get("research_run_spec_id") != parent_run_spec_id
        or ai_source.get("success_attempt_id") != parent_success_attempt_id
        or ai_source.get("discovery_exposure_id") != parent_ai_exposure_id
        or ai_source.get("run_fingerprint") != parent_run_fingerprint
        or ai_source.get("canonical_sha256") != canonical_sha256(parent_spec)
        or ai_source.get("code_commit") != parent_spec.get("code_commit")
        or ai_source.get("code_snapshot_sha256") != parent_spec.get("code_snapshot_sha256")
    ):
        raise ResearchRegistryDriftError("partial-recovery source AI identity drift")
    source_artifact = _phase1a_json_object(
        source_prefix.get("discovery_artifact"),
        label="partial-recovery source artifact",
    )
    _phase1a_exact_keys(
        source_artifact,
        expected={"artifact_id", "byte_size", "relative_path", "sha256"},
        label="partial-recovery source artifact",
    )
    if (
        source_artifact.get("artifact_id") != parent_result_artifact_id
        or source_artifact.get("sha256") != parent_result_artifact_sha256
        or source_artifact.get("byte_size") != parent_result_artifact_byte_size
        or source_artifact.get("relative_path") != parent_result_artifact_relative_path
        or source_artifact.get("relative_path") == manifest_relative_path
    ):
        # The final disjunct rejects accidentally self-referencing the recovery
        # manifest while permitting any validated content-addressed Discovery path.
        raise ResearchRegistryDriftError("partial-recovery source artifact identity drift")

    feature_source = _phase1a_json_object(
        source_prefix.get("feature"),
        label="partial-recovery source FEATURE",
    )
    if feature_source != {
        "canonical_sha256": canonical_sha256(feature_spec),
        "research_run_spec_id": feature_run_spec_id,
        "result_artifact_id": feature_result_artifact_id,
        "run_fingerprint": feature_run_fingerprint,
        "success_attempt_id": feature_success_attempt_id,
    }:
        raise ResearchRegistryDriftError("partial-recovery source FEATURE identity drift")
    _phase1a_exact_keys(
        feature_source,
        expected={
            "canonical_sha256",
            "research_run_spec_id",
            "result_artifact_id",
            "run_fingerprint",
            "success_attempt_id",
        },
        label="partial-recovery source FEATURE",
    )
    for label, value in (
        ("source AI RunSpec ID", ai_source.get("research_run_spec_id")),
        ("source AI attempt ID", ai_source.get("success_attempt_id")),
        ("source AI exposure ID", ai_source.get("discovery_exposure_id")),
        ("source FEATURE RunSpec ID", feature_source.get("research_run_spec_id")),
        ("source FEATURE attempt ID", feature_source.get("success_attempt_id")),
        ("source FEATURE artifact ID", feature_source.get("result_artifact_id")),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ResearchRegistryDriftError(f"partial-recovery manifest {label} is invalid")
    for label, value in (
        ("source AI run fingerprint", ai_source.get("run_fingerprint")),
        ("source AI canonical SHA-256", ai_source.get("canonical_sha256")),
        ("source AI code snapshot", ai_source.get("code_snapshot_sha256")),
        ("source FEATURE run fingerprint", feature_source.get("run_fingerprint")),
        ("source FEATURE canonical SHA-256", feature_source.get("canonical_sha256")),
    ):
        _phase1a_sha256(value, label=f"partial-recovery manifest {label}")

    query_evidence = document.get("query_evidence")
    existing_queries = source_prefix.get("existing_queries")
    existing_pattern_ids = source_prefix.get("existing_pattern_ids")
    if (
        not isinstance(query_evidence, list)
        or not isinstance(existing_queries, list)
        or not isinstance(existing_pattern_ids, list)
    ):
        raise ResearchRegistryDriftError(
            "partial-recovery query/pattern prefix must use ordered arrays"
        )
    if len(query_evidence) != _PHASE1A_QUERY_COUNT:
        raise ResearchRegistryDriftError(
            "partial-recovery manifest must record all eleven query results"
        )
    evidence_ids: list[str] = []
    for index, value in enumerate(query_evidence):
        evidence = _phase1a_json_object(
            value,
            label=f"partial-recovery query_evidence[{index}]",
        )
        _phase1a_exact_keys(
            evidence,
            expected={
                "definition_sha256",
                "query_id",
                "query_result_sha256",
                "source_date_count",
                "support_count",
            },
            label=f"partial-recovery query_evidence[{index}]",
        )
        query_id = evidence.get("query_id")
        if not isinstance(query_id, str) or not query_id or query_id in evidence_ids:
            raise ResearchRegistryDriftError("partial-recovery query evidence identity drift")
        evidence_ids.append(query_id)
        _phase1a_sha256(
            evidence.get("definition_sha256"),
            label=f"partial-recovery {query_id} definition",
        )
        _phase1a_sha256(
            evidence.get("query_result_sha256"),
            label=f"partial-recovery {query_id} result",
        )
        _phase1a_nonnegative_integer(
            evidence.get("source_date_count"),
            label=f"partial-recovery {query_id} source-date count",
        )
        _phase1a_nonnegative_integer(
            evidence.get("support_count"),
            label=f"partial-recovery {query_id} support count",
        )

    existing_query_ids: list[str] = []
    recorded_pattern_count = 0
    for index, value in enumerate(existing_queries):
        existing = _phase1a_json_object(
            value,
            label=f"partial-recovery existing_queries[{index}]",
        )
        _phase1a_exact_keys(
            existing,
            expected={
                "canonical_sha256",
                "discovery_exposure_id",
                "pattern_recorded",
                "query_id",
                "research_run_spec_id",
                "run_fingerprint",
                "success_attempt_id",
            },
            label=f"partial-recovery existing_queries[{index}]",
        )
        query_id = existing.get("query_id")
        if not isinstance(query_id, str):
            raise ResearchRegistryDriftError("partial-recovery existing QUERY ID is invalid")
        existing_query_ids.append(query_id)
        for key in (
            "discovery_exposure_id",
            "research_run_spec_id",
            "success_attempt_id",
        ):
            value_id = existing.get(key)
            if isinstance(value_id, bool) or not isinstance(value_id, int) or value_id <= 0:
                raise ResearchRegistryDriftError(
                    f"partial-recovery existing QUERY {key} is invalid"
                )
        _phase1a_sha256(
            existing.get("canonical_sha256"),
            label="partial-recovery existing QUERY canonical SHA-256",
        )
        _phase1a_sha256(
            existing.get("run_fingerprint"),
            label="partial-recovery existing QUERY fingerprint",
        )
        pattern_recorded = existing.get("pattern_recorded")
        if not isinstance(pattern_recorded, bool):
            raise ResearchRegistryDriftError(
                "partial-recovery existing QUERY pattern flag is invalid"
            )
        if pattern_recorded:
            if index != recorded_pattern_count:
                raise ResearchRegistryDriftError(
                    "partial-recovery patterns are not an ordered prefix"
                )
            recorded_pattern_count += 1
    if existing_query_ids != evidence_ids[: len(existing_query_ids)]:
        raise ResearchRegistryDriftError(
            "partial-recovery existing queries are not an ordered evidence prefix"
        )
    if len(existing_pattern_ids) != recorded_pattern_count or any(
        isinstance(value, bool) or not isinstance(value, int) or value <= 0
        for value in existing_pattern_ids
    ):
        raise ResearchRegistryDriftError("partial-recovery existing pattern identities drift")
    missing_pattern_query_id = source_prefix.get("missing_pattern_query_id")
    expected_missing = (
        existing_query_ids[-1] if recorded_pattern_count < len(existing_query_ids) else None
    )
    if missing_pattern_query_id != expected_missing:
        raise ResearchRegistryDriftError("partial-recovery missing-pattern identity drift")
    planned = _phase1a_json_object(
        document.get("planned_actions"),
        label="partial-recovery planned actions",
    )
    _phase1a_exact_keys(
        planned,
        expected={
            "project_missing_query_ids",
            "repair_existing_query_pattern_ids",
            "total_query_count",
        },
        label="partial-recovery planned actions",
    )
    if planned != {
        "project_missing_query_ids": evidence_ids[len(existing_query_ids) :],
        "repair_existing_query_pattern_ids": (
            [expected_missing] if expected_missing is not None else []
        ),
        "total_query_count": _PHASE1A_QUERY_COUNT,
    }:
        raise ResearchRegistryDriftError("partial-recovery planned actions drift")

    recovery_execution = _phase1a_json_object(
        document.get("recovery_execution"),
        label="partial-recovery execution",
    )
    _phase1a_exact_keys(
        recovery_execution,
        expected={
            "code_commit",
            "code_snapshot_sha256",
            "dependency_lock_sha256",
            "runtime_environment",
            "runtime_environment_sha256",
        },
        label="partial-recovery execution",
    )
    runtime_environment = _phase1a_json_object(
        run_spec.get("runtime_environment"),
        label="partial-recovery control runtime",
    )
    if recovery_execution != {
        "code_commit": run_spec.get("code_commit"),
        "code_snapshot_sha256": run_spec.get("code_snapshot_sha256"),
        "dependency_lock_sha256": run_spec.get("dependency_lock_sha256"),
        "runtime_environment": runtime_environment,
        "runtime_environment_sha256": canonical_sha256(runtime_environment),
    }:
        raise ResearchRegistryDriftError("partial-recovery execution identity drift")
    return document


@_translate_psycopg_errors("atomic Phase 1A partial-recovery control completion")
def complete_phase1a_recovery_run_success(
    database_url: str,
    *,
    research_run_attempt_id: int,
    campaign_key: str,
    run_fingerprint: str,
    code_commit: str,
    expected_manifest_sha256: str,
    recovery_manifest_path: Path,
    artifacts_root: Path,
) -> Phase1ARecoveryManifestReport:
    """Atomically bind a canonical recovery manifest to a successful VALIDATION."""

    if not isinstance(database_url, str) or not database_url.strip():
        raise ResearchRegistryError("database_url must be a non-empty string")
    attempt_id = _positive_integer(
        research_run_attempt_id,
        label="research_run_attempt_id",
    )
    campaign = _nonempty(campaign_key, label="campaign_key")
    if campaign != _PHASE1A_CAMPAIGN_KEY:
        raise ResearchRegistryError("partial recovery is restricted to the Phase 1A campaign")
    fingerprint = _phase1a_sha256(run_fingerprint, label="recovery run fingerprint")
    commit = _nonempty(code_commit, label="code_commit")
    manifest_sha256 = _phase1a_sha256(
        expected_manifest_sha256,
        label="recovery manifest",
    )
    manifest_path = _contained_path(
        recovery_manifest_path,
        artifacts_root,
        label="partial-recovery manifest",
    )
    if (
        manifest_path.is_symlink()
        or not manifest_path.is_file()
        or manifest_path.name != f"sha256={manifest_sha256}.json"
        or manifest_path.stat().st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
    ):
        raise ResearchRegistryError(
            "partial-recovery manifest must be a content-addressed regular file"
        )
    descriptor, observed_sha256, byte_size, identity = _open_hashed_regular_file(manifest_path)
    if observed_sha256 != manifest_sha256:
        os.close(descriptor)
        raise ResearchRegistryDriftError("partial-recovery manifest SHA-256 drift")
    content = _read_open_descriptor(descriptor)
    relative_path = manifest_path.relative_to(artifacts_root.resolve()).as_posix()

    try:
        with psycopg.connect(database_url, row_factory=dict_row) as connection:
            connection.isolation_level = IsolationLevel.SERIALIZABLE
            with connection.transaction():
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (campaign,),
                )
                run_rows = connection.execute(
                    """
                    SELECT child.research_run_spec_id, child.campaign_id,
                           child.parent_run_spec_id, child.run_fingerprint,
                           child.run_kind, child.engine_version, child.code_commit,
                           child.canonical_spec,
                           parent.run_fingerprint AS parent_run_fingerprint,
                           parent.run_kind AS parent_run_kind,
                           parent.canonical_spec AS parent_canonical_spec,
                           parent_attempt.research_run_attempt_id
                               AS parent_success_attempt_id,
                           parent_attempt.result_artifact_id
                               AS parent_result_artifact_id,
                           ai_exposure.discovery_exposure_id AS parent_ai_exposure_id,
                           parent_artifact.uri AS parent_result_artifact_uri,
                           parent_artifact.sha256 AS parent_result_artifact_sha256,
                           parent_artifact.byte_size AS parent_result_artifact_byte_size,
                           feature.research_run_spec_id AS feature_run_spec_id,
                           feature.run_fingerprint AS feature_run_fingerprint,
                           feature.canonical_spec AS feature_canonical_spec,
                           feature_attempt.research_run_attempt_id
                               AS feature_success_attempt_id,
                           feature_attempt.result_artifact_id
                               AS feature_result_artifact_id
                    FROM systematic_fx.research_run_specs AS child
                    JOIN systematic_fx.research_run_specs AS parent
                      ON parent.research_run_spec_id = child.parent_run_spec_id
                     AND parent.campaign_id = child.campaign_id
                    JOIN systematic_fx.research_run_attempts AS parent_attempt
                      ON parent_attempt.research_run_spec_id = parent.research_run_spec_id
                     AND parent_attempt.status = 'SUCCEEDED'
                    JOIN systematic_fx.artifacts AS parent_artifact
                      ON parent_artifact.artifact_id = parent_attempt.result_artifact_id
                    JOIN systematic_fx.discovery_exposures AS ai_exposure
                      ON ai_exposure.research_run_spec_id = parent.research_run_spec_id
                     AND ai_exposure.exposure_type = 'AI_SLICE'
                     AND ai_exposure.result_artifact_id = parent_attempt.result_artifact_id
                    JOIN systematic_fx.research_run_specs AS feature
                      ON feature.research_run_spec_id = parent.parent_run_spec_id
                     AND feature.campaign_id = parent.campaign_id
                     AND feature.run_kind = 'FEATURE_BUILD'
                    JOIN systematic_fx.research_run_attempts AS feature_attempt
                      ON feature_attempt.research_run_spec_id = feature.research_run_spec_id
                     AND feature_attempt.status = 'SUCCEEDED'
                    WHERE child.run_fingerprint = %s
                    FOR SHARE OF child, parent, parent_attempt, parent_artifact,
                                 ai_exposure, feature, feature_attempt
                    """,
                    (fingerprint,),
                ).fetchall()
                if len(run_rows) != 1:
                    raise ResearchRegistryDriftError(
                        "partial-recovery control requires one exact successful AI parent"
                    )
                run = run_rows[0]
                campaign_row = connection.execute(
                    """
                    SELECT campaign_id
                    FROM systematic_fx.campaigns
                    WHERE campaign_key = %s
                    FOR SHARE
                    """,
                    (campaign,),
                ).fetchone()
                campaign_row = _row_or_error(campaign_row, label=f"campaign {campaign}")
                _assert_fields(
                    label="partial-recovery control RunSpec",
                    row=run,
                    expected={
                        "campaign_id": int(campaign_row["campaign_id"]),
                        "code_commit": commit,
                        "engine_version": _PHASE1A_RECOVERY_ENGINE,
                        "parent_run_kind": "AI_SLICE",
                        "run_fingerprint": fingerprint,
                        "run_kind": "VALIDATION",
                    },
                )
                run_spec = _phase1a_json_object(
                    run.get("canonical_spec"),
                    label="partial-recovery control RunSpec",
                )
                parent_spec = _phase1a_json_object(
                    run.get("parent_canonical_spec"),
                    label="partial-recovery source AI RunSpec",
                )
                if canonical_sha256(run_spec) != fingerprint:
                    raise ResearchRegistryDriftError(
                        "partial-recovery control canonical fingerprint drift"
                    )
                shared_fields = (
                    "campaign_id",
                    "source_manifest_hashes",
                    "eligible_calendar",
                    "split",
                    "feature",
                    "outcome",
                    "cost",
                    "execution",
                    "random_seed",
                    "direction",
                    "signal_policy",
                    "entry_policy",
                    "barrier_policy",
                    "terminal_policy",
                )
                if any(run_spec.get(key) != parent_spec.get(key) for key in shared_fields):
                    raise ResearchRegistryDriftError(
                        "partial-recovery control changed source research variables"
                    )
                parent_fingerprint = _phase1a_sha256(
                    run.get("parent_run_fingerprint"),
                    label="partial-recovery source AI fingerprint",
                )
                parent_artifact_sha256 = _phase1a_sha256(
                    run.get("parent_result_artifact_sha256"),
                    label="partial-recovery source artifact",
                )
                parent_run_spec_id = int(run["parent_run_spec_id"])
                parent_artifact_id = int(run["parent_result_artifact_id"])
                parent_artifact_size = int(run["parent_result_artifact_byte_size"])
                parent_uri = run.get("parent_result_artifact_uri")
                if not isinstance(parent_uri, str):
                    raise ResearchRegistryDriftError(
                        "partial-recovery source artifact URI is missing"
                    )
                parsed_parent_uri = urlparse(parent_uri)
                if parsed_parent_uri.scheme != "file" or parsed_parent_uri.netloc not in {
                    "",
                    "localhost",
                }:
                    raise ResearchRegistryDriftError(
                        "partial-recovery source artifact URI is invalid"
                    )
                parent_artifact_path = Path(unquote(parsed_parent_uri.path)).resolve(strict=True)
                resolved_artifacts_root = artifacts_root.resolve(strict=True)
                if (
                    not parent_artifact_path.is_relative_to(resolved_artifacts_root)
                    or parent_artifact_path.is_symlink()
                ):
                    raise ResearchRegistryDriftError(
                        "partial-recovery source artifact is outside data/derived"
                    )
                _verify_phase1a_artifact_file(
                    {
                        "artifact_uri": parent_uri,
                        "artifact_sha256": parent_artifact_sha256,
                        "artifact_byte_size": parent_artifact_size,
                    }
                )
                parent_artifact_relative_path = parent_artifact_path.relative_to(
                    resolved_artifacts_root
                ).as_posix()
                feature_spec = _phase1a_json_object(
                    run.get("feature_canonical_spec"),
                    label="partial-recovery source FEATURE RunSpec",
                )
                feature_fingerprint = _phase1a_sha256(
                    run.get("feature_run_fingerprint"),
                    label="partial-recovery source FEATURE fingerprint",
                )
                if canonical_sha256(feature_spec) != feature_fingerprint:
                    raise ResearchRegistryDriftError(
                        "partial-recovery source FEATURE canonical fingerprint drift"
                    )
                document = _validate_phase1a_recovery_manifest_document(
                    content,
                    campaign_key=campaign,
                    run_spec=run_spec,
                    parent_spec=parent_spec,
                    parent_run_spec_id=parent_run_spec_id,
                    parent_run_fingerprint=parent_fingerprint,
                    parent_success_attempt_id=int(run["parent_success_attempt_id"]),
                    parent_ai_exposure_id=int(run["parent_ai_exposure_id"]),
                    parent_result_artifact_id=parent_artifact_id,
                    parent_result_artifact_sha256=parent_artifact_sha256,
                    parent_result_artifact_byte_size=parent_artifact_size,
                    parent_result_artifact_relative_path=parent_artifact_relative_path,
                    feature_run_spec_id=int(run["feature_run_spec_id"]),
                    feature_run_fingerprint=feature_fingerprint,
                    feature_success_attempt_id=int(run["feature_success_attempt_id"]),
                    feature_result_artifact_id=int(run["feature_result_artifact_id"]),
                    feature_spec=feature_spec,
                    manifest_sha256=manifest_sha256,
                    manifest_relative_path=relative_path,
                )

                attempt = connection.execute(
                    """
                    SELECT research_run_spec_id, status, started_at
                    FROM systematic_fx.research_run_attempts
                    WHERE research_run_attempt_id = %s
                    FOR UPDATE
                    """,
                    (attempt_id,),
                ).fetchone()
                attempt = _row_or_error(attempt, label=f"recovery attempt {attempt_id}")
                _assert_fields(
                    label=f"recovery attempt {attempt_id}",
                    row=attempt,
                    expected={
                        "research_run_spec_id": int(run["research_run_spec_id"]),
                        "status": "RUNNING",
                    },
                )
                if attempt.get("started_at") is None:
                    raise ResearchRegistryDriftError(
                        "partial-recovery RUNNING attempt lacks started_at"
                    )
                metadata = {
                    "artifact_schema": _PHASE1A_RECOVERY_MANIFEST_SCHEMA,
                    "campaign_key": campaign,
                    "no_research_recomputation": True,
                    "run_fingerprint": fingerprint,
                    "source_ai_run_fingerprint": parent_fingerprint,
                    "source_artifact_id": parent_artifact_id,
                }
                artifact_id, created_artifact = _ensure_artifact(
                    connection,
                    artifact_key=f"{campaign}:partial-recovery:{fingerprint}:{manifest_sha256}",
                    artifact_type="PHASE1A_SLICE_RECOVERY_MANIFEST",
                    path=manifest_path,
                    sha256=manifest_sha256,
                    byte_size=byte_size,
                    media_type="application/json",
                    producer_job_id=None,
                    metadata=metadata,
                )
                result_summary = {
                    "manifest_sha256": manifest_sha256,
                    "no_research_recomputation": True,
                    "pipeline_version": document["pipeline_version"],
                    "run_fingerprint": fingerprint,
                    "slice_index": document["slice_index"],
                    "source_ai_run_fingerprint": parent_fingerprint,
                    "source_artifact_sha256": parent_artifact_sha256,
                }
                transitioned = connection.execute(
                    """
                    UPDATE systematic_fx.research_run_attempts
                    SET status = 'SUCCEEDED', result_artifact_id = %s,
                        result_summary = %s, error_message = NULL,
                        finished_at = statement_timestamp()
                    WHERE research_run_attempt_id = %s AND status = 'RUNNING'
                    RETURNING research_run_attempt_id
                    """,
                    (artifact_id, Jsonb(result_summary), attempt_id),
                ).fetchone()
                if transitioned is None:
                    raise ResearchRegistryDriftError("partial-recovery attempt lost RUNNING state")
                _verify_open_file_binding(descriptor, manifest_path, identity)
    finally:
        os.close(descriptor)

    return Phase1ARecoveryManifestReport(
        research_run_spec_id=int(run["research_run_spec_id"]),
        research_run_attempt_id=attempt_id,
        result_artifact_id=artifact_id,
        run_fingerprint=fingerprint,
        manifest_sha256=manifest_sha256,
        manifest_uri=manifest_path.as_uri(),
        created_artifact=created_artifact,
    )


@_translate_psycopg_errors("Phase 1A Discovery success verification")
def verify_discovery_run_success(
    database_url: str,
    *,
    campaign_key: str,
    exposure_key: str,
    exposure_type: str,
    source_interval_start: datetime,
    source_interval_end: datetime,
    query_spec: Mapping[str, object],
    exposure_result_summary: Mapping[str, object],
    code_commit: str,
    config_sha256: str,
    run_fingerprint: str,
    result_artifact_id: int,
) -> DiscoveryExposureReport:
    """Prove that a duplicate success still has its exact atomic exposure."""

    if not isinstance(database_url, str) or not database_url.strip():
        raise ResearchRegistryError("database_url must be a non-empty string")
    campaign_key = _nonempty(campaign_key, label="campaign_key")
    if campaign_key != _PHASE1A_CAMPAIGN_KEY:
        raise ResearchRegistryError(
            "Discovery success verification is restricted to the Phase 1A campaign"
        )
    exposure_key = _nonempty(exposure_key, label="exposure_key")
    exposure_type = _nonempty(exposure_type, label="exposure_type").upper()
    if exposure_type not in {"AI_SLICE", "QUERY"}:
        raise ResearchRegistryError("Phase 1A verification requires AI_SLICE or QUERY")
    start = _aware_utc(source_interval_start, label="source_interval_start")
    end = _aware_utc(source_interval_end, label="source_interval_end")
    if start >= end:
        raise ResearchRegistryError("Phase 1A source exposure interval must be positive")
    if not isinstance(query_spec, Mapping) or not isinstance(
        exposure_result_summary,
        Mapping,
    ):
        raise ResearchRegistryError("query_spec and exposure_result_summary must be mappings")
    query_document = dict(query_spec)
    exposure_summary = dict(exposure_result_summary)
    code_commit = _nonempty(code_commit, label="code_commit")
    if not isinstance(config_sha256, str) or not _SHA256.fullmatch(config_sha256):
        raise ResearchRegistryError("config_sha256 must be lowercase SHA-256")
    if not isinstance(run_fingerprint, str) or not _SHA256.fullmatch(run_fingerprint):
        raise ResearchRegistryError("run_fingerprint must be a lowercase SHA-256")
    artifact_id = _positive_integer(result_artifact_id, label="result_artifact_id")

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        connection.isolation_level = IsolationLevel.SERIALIZABLE
        with connection.transaction():
            row = connection.execute(
                """
                SELECT exposure.discovery_exposure_id, exposure.exposure_key,
                       exposure.campaign_id, exposure.exposure_type,
                       exposure.source_interval_start, exposure.source_interval_end,
                       exposure.visible_to_ai, exposure.research_eligible,
                       exposure.query_spec, exposure.result_summary,
                       exposure.result_artifact_id, exposure.code_commit,
                       exposure.config_sha256, exposure.research_run_spec_id,
                       run_spec.run_fingerprint, run_spec.parent_run_spec_id,
                       run_spec.canonical_spec
                FROM systematic_fx.discovery_exposures AS exposure
                JOIN systematic_fx.campaigns AS campaign
                  ON campaign.campaign_id = exposure.campaign_id
                JOIN systematic_fx.research_run_specs AS run_spec
                  ON run_spec.research_run_spec_id = exposure.research_run_spec_id
                WHERE campaign.campaign_key = %s
                  AND exposure.exposure_key = %s
                FOR SHARE OF exposure, campaign, run_spec
                """,
                (campaign_key, exposure_key),
            ).fetchone()
            row = _row_or_error(row, label=f"Discovery exposure {exposure_key}")
            research_run_spec_id = int(row["research_run_spec_id"])
            _assert_fields(
                label=f"Discovery exposure {exposure_key}",
                row=row,
                expected={
                    "code_commit": code_commit,
                    "config_sha256": config_sha256,
                    "exposure_key": exposure_key,
                    "exposure_type": exposure_type,
                    "query_spec": query_document,
                    "research_eligible": False,
                    "result_artifact_id": artifact_id,
                    "result_summary": exposure_summary,
                    "run_fingerprint": run_fingerprint,
                    "source_interval_end": end,
                    "source_interval_start": start,
                    "visible_to_ai": True,
                },
            )
            _, _, parent_result_artifact_id, _ = _verify_phase1a_child_parent_success(
                connection,
                child_run_spec=row,
                exposure_type=exposure_type,
                exposure_result_summary=exposure_summary,
            )
            if exposure_type == "QUERY" and artifact_id != parent_result_artifact_id:
                raise ResearchRegistryDriftError(
                    "QUERY result artifact must be its AI_SLICE parent result artifact"
                )
            attempts = connection.execute(
                """
                SELECT research_run_attempt_id
                FROM systematic_fx.research_run_attempts
                WHERE research_run_spec_id = %s
                  AND status = 'SUCCEEDED'
                  AND result_artifact_id = %s
                FOR SHARE
                """,
                (research_run_spec_id, artifact_id),
            ).fetchall()
            if len(attempts) != 1:
                raise ResearchRegistryDriftError(
                    "Phase 1A exposure does not have exactly one matching successful attempt"
                )

    return DiscoveryExposureReport(
        discovery_exposure_id=int(row["discovery_exposure_id"]),
        exposure_key=exposure_key,
        campaign_id=int(row["campaign_id"]),
        research_run_spec_id=research_run_spec_id,
        result_artifact_id=artifact_id,
        created_exposure=False,
        created_artifact=False,
    )


def _phase1a_json_object(value: object, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ResearchRegistryDriftError(f"{label} must be a JSON object")
    return value


def _phase1a_exact_keys(
    value: Mapping[str, object],
    *,
    expected: set[str],
    label: str,
) -> None:
    if set(value) != expected:
        raise ResearchRegistryDriftError(f"{label} fields drift")


def _phase1a_sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ResearchRegistryDriftError(f"{label} must be a lowercase SHA-256")
    return value


def _phase1a_nonnegative_integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ResearchRegistryDriftError(f"{label} must be a non-negative integer")
    return value


def _phase1a_predecessor_inputs(
    database_url: str,
    *,
    campaign_key: str,
    prior_slice_index: int,
    source_interval_start: datetime,
    source_interval_end: datetime,
    requested_source_dates: Sequence[date],
    query_definition_sha256_by_id: Mapping[str, str],
) -> tuple[
    str,
    int,
    datetime,
    datetime,
    tuple[date, ...],
    tuple[tuple[str, str], ...],
]:
    if not isinstance(database_url, str) or not database_url.strip():
        raise ResearchRegistryError("database_url must be a non-empty string")
    campaign = _nonempty(campaign_key, label="campaign_key")
    if campaign != _PHASE1A_CAMPAIGN_KEY:
        raise ResearchRegistryError(
            "predecessor verification is restricted to the Phase 1A campaign"
        )
    if (
        isinstance(prior_slice_index, bool)
        or not isinstance(prior_slice_index, int)
        or not 0 <= prior_slice_index <= 98
    ):
        raise ResearchRegistryError("prior_slice_index must be an integer from 0 through 98")
    start = _aware_utc(source_interval_start, label="source_interval_start")
    end = _aware_utc(source_interval_end, label="source_interval_end")
    if isinstance(requested_source_dates, (str, bytes)) or not isinstance(
        requested_source_dates,
        Sequence,
    ):
        raise ResearchRegistryError("requested_source_dates must be an ordered sequence")
    source_dates = tuple(requested_source_dates)
    if len(source_dates) != _PHASE1A_SLICE_SOURCE_DATE_COUNT:
        raise ResearchRegistryError("a Phase 1A predecessor slice requires exactly five dates")
    if any(not isinstance(day, date) or isinstance(day, datetime) for day in source_dates):
        raise ResearchRegistryError("requested_source_dates must contain date values")
    if any(left >= right for left, right in pairwise(source_dates)):
        raise ResearchRegistryError("requested_source_dates must be unique and increasing")
    expected_start = datetime.combine(source_dates[0], time.min, tzinfo=UTC)
    expected_end = datetime.combine(source_dates[-1] + timedelta(days=1), time.min, tzinfo=UTC)
    if start != expected_start or end != expected_end:
        raise ResearchRegistryError(
            "source interval must exactly span the requested predecessor dates"
        )
    if not isinstance(query_definition_sha256_by_id, Mapping):
        raise ResearchRegistryError("query_definition_sha256_by_id must be an ordered mapping")
    query_definitions = tuple(query_definition_sha256_by_id.items())
    if len(query_definitions) != _PHASE1A_QUERY_COUNT:
        raise ResearchRegistryError("Phase 1A predecessor verification requires 11 queries")
    seen_query_ids: set[str] = set()
    for query_id, definition_sha256 in query_definitions:
        if (
            not isinstance(query_id, str)
            or not query_id
            or query_id != query_id.strip()
            or query_id in seen_query_ids
        ):
            raise ResearchRegistryError("query IDs must be unique canonical non-empty strings")
        if not isinstance(definition_sha256, str) or _SHA256.fullmatch(definition_sha256) is None:
            raise ResearchRegistryError("query definition values must be lowercase SHA-256")
        seen_query_ids.add(query_id)
    return campaign, prior_slice_index, start, end, source_dates, query_definitions


def _phase1a_exposure_parameters(row: Mapping[str, Any], *, label: str) -> dict[str, Any]:
    canonical_spec = _phase1a_json_object(row.get("canonical_spec"), label=f"{label} RunSpec")
    return _phase1a_json_object(
        canonical_spec.get("parameters"),
        label=f"{label} RunSpec parameters",
    )


def _validate_phase1a_ai_predecessor(
    row: Mapping[str, Any],
    *,
    campaign_key: str,
    prior_slice_index: int,
    requested_date_strings: list[str],
    query_definitions: tuple[tuple[str, str], ...],
) -> tuple[tuple[dict[str, Any], ...], int, str, int, str]:
    label = "Phase 1A predecessor AI_SLICE"
    run_fingerprint = _phase1a_sha256(row.get("run_fingerprint"), label="AI run fingerprint")
    if row.get("run_kind") != "AI_SLICE" or row.get("engine_version") != (
        "phase1a_fixed_query_discovery_v1"
    ):
        raise ResearchRegistryDriftError(f"{label} is not backed by an AI_SLICE RunSpec")
    canonical_spec = _phase1a_json_object(
        row.get("canonical_spec"),
        label="AI_SLICE canonical RunSpec",
    )
    if canonical_sha256(canonical_spec) != run_fingerprint:
        raise ResearchRegistryDriftError("AI_SLICE canonical RunSpec fingerprint drift")
    raw_query_spec = _phase1a_json_object(row.get("query_spec"), label="AI query_spec")
    _phase1a_exact_keys(
        raw_query_spec,
        expected={"candidate_queries", "definition_sha256", "run_fingerprint"},
        label="AI query_spec",
    )
    raw_candidates = raw_query_spec.get("candidate_queries")
    if not isinstance(raw_candidates, list) or len(raw_candidates) != len(query_definitions):
        raise ResearchRegistryDriftError("AI_SLICE candidate query cardinality drift")
    candidates: list[dict[str, Any]] = []
    observed_ids: list[str] = []
    for index, (candidate, (expected_query_id, expected_sha256)) in enumerate(
        zip(raw_candidates, query_definitions, strict=True)
    ):
        definition = _phase1a_json_object(candidate, label=f"AI candidate query {index}")
        if definition.get("id") != expected_query_id:
            raise ResearchRegistryDriftError("AI_SLICE candidate query ID/order drift")
        if canonical_sha256(definition) != expected_sha256:
            raise ResearchRegistryDriftError(
                f"AI_SLICE candidate query definition drift for {expected_query_id}"
            )
        observed_ids.append(expected_query_id)
        candidates.append(definition)
    if len(set(observed_ids)) != len(observed_ids):
        raise ResearchRegistryDriftError("AI_SLICE repeats a candidate query ID")
    definition_sha256 = canonical_sha256(candidates)
    if raw_query_spec != {
        "candidate_queries": candidates,
        "definition_sha256": definition_sha256,
        "run_fingerprint": run_fingerprint,
    }:
        raise ResearchRegistryDriftError("AI_SLICE exact query specification drift")

    parameters = _phase1a_exposure_parameters(row, label="AI_SLICE")
    _phase1a_exact_keys(
        parameters,
        expected={
            "analysis_authority",
            "candidate_queries",
            "candidate_query_definition_sha256",
            "feature_inputs_by_date",
            "feature_manifest_relative_path",
            "feature_manifest_sha256",
            "frozen_toml_inputs",
            "no_entry_reason_by_date",
            "parent_run_fingerprint",
            "pipeline_version",
            "requested_source_dates",
            "research_eligible",
            "screening_only",
            "slice_index",
        },
        label="AI_SLICE RunSpec parameters",
    )
    expected_parameter_values = {
        "analysis_authority": "OPEN_OBSERVATION",
        "candidate_queries": candidates,
        "candidate_query_definition_sha256": definition_sha256,
        "pipeline_version": "phase1a_discovery_pipeline_v1",
        "requested_source_dates": requested_date_strings,
        "research_eligible": False,
        "screening_only": True,
        "slice_index": prior_slice_index,
    }
    if any(parameters.get(key) != value for key, value in expected_parameter_values.items()):
        raise ResearchRegistryDriftError("AI_SLICE RunSpec predecessor identity drift")

    result_summary = _phase1a_json_object(row.get("result_summary"), label="AI result_summary")
    _phase1a_exact_keys(
        result_summary,
        expected={
            "candidate_query_count",
            "feature_manifest_sha256",
            "requested_source_dates",
            "screening_only",
        },
        label="AI result_summary",
    )
    if (
        result_summary.get("candidate_query_count") != len(query_definitions)
        or result_summary.get("requested_source_dates") != requested_date_strings
        or result_summary.get("screening_only") is not True
    ):
        raise ResearchRegistryDriftError("AI_SLICE result summary predecessor identity drift")
    _phase1a_sha256(
        result_summary.get("feature_manifest_sha256"),
        label="AI feature manifest SHA-256",
    )

    artifact_id = row.get("result_artifact_id")
    run_spec_id = row.get("research_run_spec_id")
    if (
        isinstance(artifact_id, bool)
        or not isinstance(artifact_id, int)
        or artifact_id <= 0
        or isinstance(run_spec_id, bool)
        or not isinstance(run_spec_id, int)
        or run_spec_id <= 0
    ):
        raise ResearchRegistryDriftError("AI_SLICE lacks immutable RunSpec/artifact identities")
    artifact_sha256 = _phase1a_sha256(row.get("artifact_sha256"), label="result artifact")
    exposure_key = f"{campaign_key}:ai-slice:{prior_slice_index:02d}"
    expected_metadata = {
        "campaign_key": campaign_key,
        "exposure_key": exposure_key,
        "exposure_type": "AI_SLICE",
        "run_fingerprint": run_fingerprint,
    }
    if (
        row.get("exposure_key") != exposure_key
        or row.get("artifact_type") != "DISCOVERY_EXPOSURE_RESULT"
        or row.get("artifact_media_type") != "application/json"
        or row.get("artifact_metadata") != expected_metadata
        or row.get("artifact_key")
        != f"{campaign_key}:discovery-exposure:{exposure_key}:{artifact_sha256}"
    ):
        raise ResearchRegistryDriftError("AI_SLICE result artifact lineage drift")
    byte_size = row.get("artifact_byte_size")
    if isinstance(byte_size, bool) or not isinstance(byte_size, int) or byte_size <= 0:
        raise ResearchRegistryDriftError("AI_SLICE result artifact must be non-empty")
    _verify_phase1a_artifact_file(row)
    return tuple(candidates), artifact_id, artifact_sha256, run_spec_id, run_fingerprint


def _phase1a_read_immutable_artifact(
    row: Mapping[str, Any],
    *,
    uri_key: str,
    sha256_key: str,
    byte_size_key: str,
    label: str,
) -> tuple[bytes, Path, Path]:
    uri = row.get(uri_key)
    expected_sha256 = _phase1a_sha256(row.get(sha256_key), label=label)
    expected_size = row.get(byte_size_key)
    if not isinstance(uri, str) or (
        isinstance(expected_size, bool) or not isinstance(expected_size, int) or expected_size <= 0
    ):
        raise ResearchRegistryDriftError(f"{label} database identity is invalid")
    parsed = urlparse(uri)
    if (
        parsed.scheme != "file"
        or parsed.netloc not in {"", "localhost"}
        or parsed.query
        or parsed.fragment
    ):
        raise ResearchRegistryDriftError(f"{label} must use a plain local file URI")
    path = Path(unquote(parsed.path))
    matches = [
        index
        for index in range(len(path.parts) - 1)
        if path.parts[index : index + 2] == ("data", "derived")
    ]
    if not path.is_absolute() or len(matches) != 1 or path.name != f"sha256={expected_sha256}.json":
        raise ResearchRegistryDriftError(f"{label} is not content-addressed below data/derived")
    derived_root = Path(*path.parts[: matches[0] + 2])
    try:
        resolved_path = path.resolve(strict=True)
        resolved_derived = derived_root.resolve(strict=True)
    except OSError as exc:
        raise ResearchRegistryDriftError(f"{label} is no longer reachable") from exc
    if (
        resolved_path != path
        or resolved_derived != derived_root
        or not path.is_relative_to(derived_root)
        or path.is_symlink()
    ):
        raise ResearchRegistryDriftError(f"{label} path contains a symlink")
    if path.stat().st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH):
        raise ResearchRegistryDriftError(f"{label} is writable")
    descriptor, observed_sha256, observed_size, identity = _open_hashed_regular_file(path)
    try:
        content = _read_open_descriptor(descriptor)
        _verify_open_file_binding(descriptor, path, identity)
    finally:
        os.close(descriptor)
    if observed_sha256 != expected_sha256 or observed_size != expected_size:
        raise ResearchRegistryDriftError(f"{label} content differs from its database identity")
    return content, path, derived_root


def _phase1a_load_discovery_query_evidence(
    row: Mapping[str, Any],
    *,
    candidates: Sequence[Mapping[str, object]],
    query_definitions: tuple[tuple[str, str], ...],
    requested_date_strings: list[str],
    ai_run_fingerprint: str,
    ai_canonical_spec: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    """Bind every QUERY summary and digest to the held Discovery artifact bytes."""

    content, _, _ = _phase1a_read_immutable_artifact(
        row,
        uri_key="artifact_uri",
        sha256_key="artifact_sha256",
        byte_size_key="artifact_byte_size",
        label="Discovery artifact",
    )
    try:
        document_value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ResearchRegistryDriftError("Discovery artifact is not valid JSON") from exc
    document = _phase1a_json_object(document_value, label="Discovery artifact")
    if canonical_json_bytes(document) + b"\n" != content:
        raise ResearchRegistryDriftError("Discovery artifact bytes are not canonical")
    authority = _phase1a_json_object(
        document.get("authority"),
        label="Discovery artifact authority",
    )
    expected_authority = {
        "maximum_authority": "OPEN_OBSERVATION",
        "pass_backtest_allowed": False,
        "screening_only": True,
        "screening_survivor_allowed": False,
    }
    if (
        document.get("artifact_schema") != _PHASE1A_DISCOVERY_ARTIFACT_SCHEMA
        or document.get("artifact_version") != "phase1a_discovery_slice_v1"
        or document.get("run_fingerprint") != ai_run_fingerprint
        or document.get("code_snapshot_sha256") != ai_canonical_spec.get("code_snapshot_sha256")
        or document.get("requested_source_dates") != requested_date_strings
        or authority != expected_authority
    ):
        raise ResearchRegistryDriftError("Discovery artifact authority/RunSpec identity drift")
    raw_results = document.get("query_results")
    if not isinstance(raw_results, list) or len(raw_results) != len(query_definitions):
        raise ResearchRegistryDriftError("Discovery artifact query result cardinality drift")

    evidence_by_id: dict[str, dict[str, Any]] = {}
    nonzero_count = 0
    for index, (value, candidate, (query_id, definition_sha256)) in enumerate(
        zip(raw_results, candidates, query_definitions, strict=True)
    ):
        result = _phase1a_json_object(value, label=f"Discovery query result {index}")
        _phase1a_exact_keys(
            result,
            expected={
                "definition",
                "direction_counts",
                "forward",
                "occurrences",
                "source_date_count",
                "support_count",
            },
            label=f"Discovery query result {query_id}",
        )
        if result.get("definition") != dict(candidate) or canonical_sha256(candidate) != (
            definition_sha256
        ):
            raise ResearchRegistryDriftError(f"Discovery query definition drift for {query_id}")
        direction_counts = _phase1a_json_object(
            result.get("direction_counts"),
            label=f"Discovery query {query_id} direction counts",
        )
        _phase1a_exact_keys(
            direction_counts,
            expected={"LONG", "SHORT"},
            label=f"Discovery query {query_id} direction counts",
        )
        long_count = _phase1a_nonnegative_integer(
            direction_counts.get("LONG"),
            label=f"Discovery query {query_id} LONG count",
        )
        short_count = _phase1a_nonnegative_integer(
            direction_counts.get("SHORT"),
            label=f"Discovery query {query_id} SHORT count",
        )
        support_count = _phase1a_nonnegative_integer(
            result.get("support_count"),
            label=f"Discovery query {query_id} support count",
        )
        source_date_count = _phase1a_nonnegative_integer(
            result.get("source_date_count"),
            label=f"Discovery query {query_id} source-date count",
        )
        occurrences = result.get("occurrences")
        if (
            not isinstance(occurrences, list)
            or len(occurrences) != support_count
            or long_count + short_count != support_count
            or source_date_count > len(requested_date_strings)
        ):
            raise ResearchRegistryDriftError(f"Discovery query {query_id} count invariants drift")
        nonzero_count += int(support_count > 0)
        evidence_by_id[query_id] = {
            "definition_sha256": definition_sha256,
            "query_result_sha256": canonical_sha256(result),
            "result_summary": {
                "artifact_sha256": row.get("artifact_sha256"),
                "direction_counts": direction_counts,
                "source_date_count": source_date_count,
                "support_count": support_count,
            },
        }
    summary = _phase1a_json_object(document.get("summary"), label="Discovery artifact summary")
    if (
        summary.get("candidate_query_count") != len(query_definitions)
        or summary.get("nonzero_support_query_count") != nonzero_count
        or summary.get("zero_support_query_count") != len(query_definitions) - nonzero_count
    ):
        raise ResearchRegistryDriftError("Discovery artifact aggregate query summary drift")
    return evidence_by_id


def _validate_phase1a_recovery_projection_control(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    query_spec: Mapping[str, Any],
    parameters: Mapping[str, Any],
    ai_spec: Mapping[str, Any],
    ai_run_spec_id: int,
    ai_run_fingerprint: str,
    ai_artifact_id: int,
    ai_artifact_sha256: str,
    expected_query_id: str,
    artifact_query_evidence: Mapping[str, Any],
    recovery_identity: Mapping[str, Any] | None = None,
    identity_schema: str = _PHASE1A_RECOVERY_PROJECTION_SCHEMA,
    identity_mode: str = "IMMUTABLE_AI_ARTIFACT_PROJECTION",
    required_action: str = "project_missing_query_ids",
    require_query_execution_identity: bool = True,
) -> None:
    identity_label = (
        "recovery projection" if recovery_identity is None else "pattern recovery registrar"
    )
    recovery = _phase1a_json_object(
        parameters.get("recovery_projection") if recovery_identity is None else recovery_identity,
        label=f"{identity_label} {expected_query_id}",
    )
    _phase1a_exact_keys(
        recovery,
        expected={
            "artifact_schema",
            "mode",
            "no_research_recomputation",
            "recovery_code_commit",
            "recovery_code_snapshot_sha256",
            "recovery_control_run_fingerprint",
            "recovery_manifest_artifact_id",
            "recovery_manifest_relative_path",
            "recovery_manifest_sha256",
            "recovery_runtime_sha256",
            "source_ai_canonical_sha256",
            "source_ai_code_snapshot_sha256",
            "source_ai_run_fingerprint",
            "source_artifact_id",
            "source_artifact_sha256",
        },
        label=f"{identity_label} {expected_query_id}",
    )
    control_fingerprint = _phase1a_sha256(
        recovery.get("recovery_control_run_fingerprint"),
        label="recovery control fingerprint",
    )
    for key in (
        "recovery_code_snapshot_sha256",
        "recovery_manifest_sha256",
        "recovery_runtime_sha256",
        "source_ai_canonical_sha256",
        "source_ai_code_snapshot_sha256",
        "source_ai_run_fingerprint",
        "source_artifact_sha256",
    ):
        _phase1a_sha256(recovery.get(key), label=f"{identity_label} {key}")
    for key in ("recovery_manifest_artifact_id", "source_artifact_id"):
        value = recovery.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ResearchRegistryDriftError(f"{identity_label} {key} is invalid")
    if (
        recovery.get("artifact_schema") != identity_schema
        or recovery.get("mode") != identity_mode
        or recovery.get("no_research_recomputation") is not True
        or recovery.get("source_ai_canonical_sha256") != canonical_sha256(ai_spec)
        or recovery.get("source_ai_code_snapshot_sha256") != ai_spec.get("code_snapshot_sha256")
        or recovery.get("source_ai_run_fingerprint") != ai_run_fingerprint
        or recovery.get("source_artifact_id") != ai_artifact_id
        or recovery.get("source_artifact_sha256") != ai_artifact_sha256
    ):
        raise ResearchRegistryDriftError(f"{identity_label} source identity drift")

    rows = connection.execute(
        """
        SELECT control.research_run_spec_id, control.campaign_id,
               control.parent_run_spec_id, control.run_fingerprint,
               control.run_kind, control.engine_version, control.canonical_spec,
               parent.run_fingerprint AS parent_run_fingerprint,
               parent.canonical_spec AS parent_canonical_spec,
               parent_attempt.research_run_attempt_id AS parent_success_attempt_id,
               parent_attempt.result_artifact_id AS source_artifact_id,
               ai_exposure.discovery_exposure_id AS parent_ai_exposure_id,
               source_artifact.uri AS source_artifact_uri,
               source_artifact.sha256 AS source_artifact_sha256,
               source_artifact.byte_size AS source_artifact_byte_size,
               feature.research_run_spec_id AS feature_run_spec_id,
               feature.run_fingerprint AS feature_run_fingerprint,
               feature.canonical_spec AS feature_canonical_spec,
               feature_attempt.research_run_attempt_id AS feature_success_attempt_id,
               feature_attempt.result_artifact_id AS feature_result_artifact_id,
               control_attempt.research_run_attempt_id AS control_success_attempt_id,
               control_attempt.result_artifact_id AS recovery_artifact_id,
               recovery_artifact.artifact_key AS recovery_artifact_key,
               recovery_artifact.artifact_type AS recovery_artifact_type,
               recovery_artifact.uri AS recovery_artifact_uri,
               recovery_artifact.sha256 AS recovery_artifact_sha256,
               recovery_artifact.byte_size AS recovery_artifact_byte_size,
               recovery_artifact.media_type AS recovery_artifact_media_type,
               recovery_artifact.metadata AS recovery_artifact_metadata
        FROM systematic_fx.research_run_specs AS control
        JOIN systematic_fx.research_run_specs AS parent
          ON parent.research_run_spec_id = control.parent_run_spec_id
         AND parent.campaign_id = control.campaign_id
        JOIN systematic_fx.research_run_attempts AS parent_attempt
          ON parent_attempt.research_run_spec_id = parent.research_run_spec_id
         AND parent_attempt.status = 'SUCCEEDED'
        JOIN systematic_fx.artifacts AS source_artifact
          ON source_artifact.artifact_id = parent_attempt.result_artifact_id
        JOIN systematic_fx.discovery_exposures AS ai_exposure
          ON ai_exposure.research_run_spec_id = parent.research_run_spec_id
         AND ai_exposure.exposure_type = 'AI_SLICE'
         AND ai_exposure.result_artifact_id = parent_attempt.result_artifact_id
        JOIN systematic_fx.research_run_specs AS feature
          ON feature.research_run_spec_id = parent.parent_run_spec_id
         AND feature.campaign_id = parent.campaign_id
         AND feature.run_kind = 'FEATURE_BUILD'
        JOIN systematic_fx.research_run_attempts AS feature_attempt
          ON feature_attempt.research_run_spec_id = feature.research_run_spec_id
         AND feature_attempt.status = 'SUCCEEDED'
        JOIN systematic_fx.research_run_attempts AS control_attempt
          ON control_attempt.research_run_spec_id = control.research_run_spec_id
         AND control_attempt.status = 'SUCCEEDED'
        JOIN systematic_fx.artifacts AS recovery_artifact
          ON recovery_artifact.artifact_id = control_attempt.result_artifact_id
        WHERE control.run_fingerprint = %s
        FOR SHARE OF control, parent, parent_attempt, source_artifact,
                     ai_exposure, feature, feature_attempt, control_attempt,
                     recovery_artifact
        """,
        (control_fingerprint,),
    ).fetchall()
    if len(rows) != 1:
        raise ResearchRegistryDriftError(
            "recovery projection lacks one successful control/source chain"
        )
    control = rows[0]
    if (
        control.get("parent_run_spec_id") != ai_run_spec_id
        or control.get("run_kind") != "VALIDATION"
        or control.get("engine_version") != _PHASE1A_RECOVERY_ENGINE
        or control.get("run_fingerprint") != control_fingerprint
        or control.get("parent_run_fingerprint") != ai_run_fingerprint
        or control.get("parent_canonical_spec") != ai_spec
        or control.get("source_artifact_id") != ai_artifact_id
        or control.get("source_artifact_sha256") != ai_artifact_sha256
    ):
        raise ResearchRegistryDriftError("recovery control source chain drift")
    control_spec = _phase1a_json_object(
        control.get("canonical_spec"),
        label="recovery control RunSpec",
    )
    if canonical_sha256(control_spec) != control_fingerprint:
        raise ResearchRegistryDriftError("recovery control canonical fingerprint drift")
    shared_research_fields = (
        "campaign_id",
        "source_manifest_hashes",
        "eligible_calendar",
        "split",
        "feature",
        "outcome",
        "cost",
        "execution",
        "random_seed",
        "direction",
        "signal_policy",
        "entry_policy",
        "barrier_policy",
        "terminal_policy",
    )
    if any(control_spec.get(key) != ai_spec.get(key) for key in shared_research_fields):
        raise ResearchRegistryDriftError("recovery control changed source research variables")
    control_runtime = _phase1a_json_object(
        control_spec.get("runtime_environment"),
        label="recovery control runtime",
    )
    if (
        recovery.get("recovery_code_commit") != control_spec.get("code_commit")
        or recovery.get("recovery_code_snapshot_sha256") != control_spec.get("code_snapshot_sha256")
        or recovery.get("recovery_runtime_sha256") != canonical_sha256(control_runtime)
    ):
        raise ResearchRegistryDriftError(f"{identity_label}/control execution identity drift")
    if require_query_execution_identity and (
        query_spec.get("code_commit") != control_spec.get("code_commit")
        or query_spec.get("code_snapshot_sha256") != control_spec.get("code_snapshot_sha256")
        or query_spec.get("dependency_lock_sha256") != control_spec.get("dependency_lock_sha256")
        or query_spec.get("runtime_environment") != control_runtime
    ):
        raise ResearchRegistryDriftError("recovery QUERY/control execution identity drift")
    recovery_artifact_id = control.get("recovery_artifact_id")
    expected_metadata = {
        "artifact_schema": _PHASE1A_RECOVERY_MANIFEST_SCHEMA,
        "campaign_key": _PHASE1A_CAMPAIGN_KEY,
        "no_research_recomputation": True,
        "run_fingerprint": control_fingerprint,
        "source_ai_run_fingerprint": ai_run_fingerprint,
        "source_artifact_id": ai_artifact_id,
    }
    if (
        recovery_artifact_id != recovery.get("recovery_manifest_artifact_id")
        or control.get("recovery_artifact_type") != "PHASE1A_SLICE_RECOVERY_MANIFEST"
        or control.get("recovery_artifact_media_type") != "application/json"
        or control.get("recovery_artifact_sha256") != recovery.get("recovery_manifest_sha256")
        or control.get("recovery_artifact_metadata") != expected_metadata
        or control.get("recovery_artifact_key")
        != (
            f"{_PHASE1A_CAMPAIGN_KEY}:partial-recovery:"
            f"{control_fingerprint}:{recovery.get('recovery_manifest_sha256')}"
        )
    ):
        raise ResearchRegistryDriftError("recovery manifest database lineage drift")
    content, manifest_path, derived_root = _phase1a_read_immutable_artifact(
        control,
        uri_key="recovery_artifact_uri",
        sha256_key="recovery_artifact_sha256",
        byte_size_key="recovery_artifact_byte_size",
        label="recovery manifest",
    )
    if manifest_path.relative_to(derived_root).as_posix() != recovery.get(
        "recovery_manifest_relative_path"
    ):
        raise ResearchRegistryDriftError("recovery manifest relative path drift")
    source_uri = control.get("source_artifact_uri")
    if not isinstance(source_uri, str):
        raise ResearchRegistryDriftError("recovery source artifact URI is missing")
    source_path = Path(unquote(urlparse(source_uri).path)).resolve(strict=True)
    feature_spec = _phase1a_json_object(
        control.get("feature_canonical_spec"),
        label="recovery source FEATURE RunSpec",
    )
    manifest = _validate_phase1a_recovery_manifest_document(
        content,
        campaign_key=_PHASE1A_CAMPAIGN_KEY,
        run_spec=control_spec,
        parent_spec=ai_spec,
        parent_run_spec_id=ai_run_spec_id,
        parent_run_fingerprint=ai_run_fingerprint,
        parent_success_attempt_id=int(control["parent_success_attempt_id"]),
        parent_ai_exposure_id=int(control["parent_ai_exposure_id"]),
        parent_result_artifact_id=ai_artifact_id,
        parent_result_artifact_sha256=ai_artifact_sha256,
        parent_result_artifact_byte_size=int(control["source_artifact_byte_size"]),
        parent_result_artifact_relative_path=source_path.relative_to(derived_root).as_posix(),
        feature_run_spec_id=int(control["feature_run_spec_id"]),
        feature_run_fingerprint=str(control["feature_run_fingerprint"]),
        feature_success_attempt_id=int(control["feature_success_attempt_id"]),
        feature_result_artifact_id=int(control["feature_result_artifact_id"]),
        feature_spec=feature_spec,
        manifest_sha256=str(control["recovery_artifact_sha256"]),
        manifest_relative_path=manifest_path.relative_to(derived_root).as_posix(),
    )
    query_evidence = manifest.get("query_evidence")
    planned = _phase1a_json_object(
        manifest.get("planned_actions"),
        label="recovery planned actions",
    )
    matches = (
        [
            value
            for value in query_evidence
            if isinstance(value, dict) and value.get("query_id") == expected_query_id
        ]
        if isinstance(query_evidence, list)
        else []
    )
    expected_evidence = _phase1a_json_object(
        artifact_query_evidence,
        label=f"Discovery evidence for {expected_query_id}",
    )
    expected_result_summary = _phase1a_json_object(
        expected_evidence.get("result_summary"),
        label=f"Discovery summary for {expected_query_id}",
    )
    if (
        len(matches) != 1
        or matches[0]
        != {
            "definition_sha256": expected_evidence.get("definition_sha256"),
            "query_id": expected_query_id,
            "query_result_sha256": expected_evidence.get("query_result_sha256"),
            "source_date_count": expected_result_summary.get("source_date_count"),
            "support_count": expected_result_summary.get("support_count"),
        }
        or expected_query_id not in planned.get(required_action, [])
    ):
        raise ResearchRegistryDriftError(f"recovery manifest does not authorize {identity_label}")


def _validate_phase1a_query_predecessor(
    connection: psycopg.Connection[dict[str, Any]],
    row: Mapping[str, Any],
    *,
    campaign_key: str,
    prior_slice_index: int,
    requested_date_strings: list[str],
    expected_query_id: str,
    expected_definition_sha256: str,
    expected_definition: Mapping[str, object],
    ai_artifact_id: int,
    ai_artifact_sha256: str,
    ai_run_spec_id: int,
    ai_run_fingerprint: str,
    ai_canonical_spec: Mapping[str, Any],
    artifact_query_evidence: Mapping[str, Any],
) -> None:
    label = f"Phase 1A predecessor query {expected_query_id}"
    run_fingerprint = _phase1a_sha256(
        row.get("run_fingerprint"),
        label=f"{expected_query_id} run fingerprint",
    )
    if (
        row.get("run_kind") != "QUERY"
        or row.get("engine_version") != _PHASE1A_QUERY_ENGINE
        or row.get("parent_run_spec_id") != ai_run_spec_id
    ):
        raise ResearchRegistryDriftError(f"{label} has invalid RunSpec parentage")
    canonical_spec = _phase1a_json_object(
        row.get("canonical_spec"),
        label=f"{label} canonical RunSpec",
    )
    if canonical_sha256(canonical_spec) != run_fingerprint:
        raise ResearchRegistryDriftError(f"{label} canonical RunSpec fingerprint drift")
    query_spec = _phase1a_json_object(row.get("query_spec"), label=f"{label} query_spec")
    expected_query_spec = {
        "candidate_query": dict(expected_definition),
        "query_definition_sha256": expected_definition_sha256,
        "run_fingerprint": run_fingerprint,
    }
    if query_spec != expected_query_spec:
        raise ResearchRegistryDriftError(f"{label} exact query specification drift")
    parameters = _phase1a_exposure_parameters(row, label=label)
    standard_parameter_keys = {
        "candidate_query",
        "discovery_artifact_relative_path",
        "discovery_artifact_sha256",
        "frozen_toml_inputs",
        "parent_run_fingerprint",
        "pipeline_version",
        "query_definition_sha256",
        "query_result_sha256",
        "requested_source_dates",
        "research_eligible",
        "screening_only",
        "slice_index",
    }
    has_recovery_projection = "recovery_projection" in parameters
    _phase1a_exact_keys(
        parameters,
        expected=(
            standard_parameter_keys
            if not has_recovery_projection
            else standard_parameter_keys | {"recovery_projection"}
        ),
        label=f"{label} RunSpec parameters",
    )
    expected_parameter_values = {
        "candidate_query": dict(expected_definition),
        "discovery_artifact_sha256": ai_artifact_sha256,
        "parent_run_fingerprint": ai_run_fingerprint,
        "query_definition_sha256": expected_definition_sha256,
        "requested_source_dates": requested_date_strings,
        "research_eligible": False,
        "screening_only": True,
        "slice_index": prior_slice_index,
    }
    if any(parameters.get(key) != value for key, value in expected_parameter_values.items()):
        raise ResearchRegistryDriftError(f"{label} RunSpec predecessor identity drift")
    ai_parameters = _phase1a_json_object(
        ai_canonical_spec.get("parameters"),
        label="predecessor AI_SLICE parameters",
    )
    if (
        parameters.get("frozen_toml_inputs") != ai_parameters.get("frozen_toml_inputs")
        or parameters.get("pipeline_version") != ai_parameters.get("pipeline_version")
        or parameters.get("pipeline_version") != "phase1a_discovery_pipeline_v1"
    ):
        raise ResearchRegistryDriftError(f"{label} frozen/orchestration identity drift")
    expected_evidence = _phase1a_json_object(
        artifact_query_evidence,
        label=f"Discovery evidence for {expected_query_id}",
    )
    expected_result_summary = _phase1a_json_object(
        expected_evidence.get("result_summary"),
        label=f"Discovery summary for {expected_query_id}",
    )
    if expected_evidence.get("definition_sha256") != expected_definition_sha256 or parameters.get(
        "query_result_sha256"
    ) != expected_evidence.get("query_result_sha256"):
        raise ResearchRegistryDriftError(f"{label} result digest differs from artifact")
    artifact_uri = row.get("artifact_uri")
    if not isinstance(artifact_uri, str):
        raise ResearchRegistryDriftError(f"{label} artifact URI is missing")
    artifact_path = Path(unquote(urlparse(artifact_uri).path)).resolve(strict=True)
    derived_matches = [
        index
        for index in range(len(artifact_path.parts) - 1)
        if artifact_path.parts[index : index + 2] == ("data", "derived")
    ]
    if len(derived_matches) != 1:
        raise ResearchRegistryDriftError(f"{label} artifact path is outside data/derived")
    data_root = Path(*artifact_path.parts[: derived_matches[0] + 1])
    if (
        parameters.get("discovery_artifact_relative_path")
        != artifact_path.relative_to(data_root).as_posix()
    ):
        raise ResearchRegistryDriftError(f"{label} artifact relative path drift")
    shared_research_fields = (
        "campaign_id",
        "source_manifest_hashes",
        "eligible_calendar",
        "split",
        "feature",
        "outcome",
        "cost",
        "execution",
        "random_seed",
        "direction",
        "signal_policy",
        "entry_policy",
        "barrier_policy",
        "terminal_policy",
    )
    if any(canonical_spec.get(key) != ai_canonical_spec.get(key) for key in shared_research_fields):
        raise ResearchRegistryDriftError(f"{label} changed source research variables")
    execution_fields = (
        "code_commit",
        "code_snapshot_sha256",
        "dependency_lock_sha256",
        "runtime_environment",
    )
    if not has_recovery_projection:
        if any(canonical_spec.get(key) != ai_canonical_spec.get(key) for key in execution_fields):
            raise ResearchRegistryDriftError(f"{label} normal execution identity drift")
    else:
        _validate_phase1a_recovery_projection_control(
            connection,
            query_spec=canonical_spec,
            parameters=parameters,
            ai_spec=ai_canonical_spec,
            ai_run_spec_id=ai_run_spec_id,
            ai_run_fingerprint=ai_run_fingerprint,
            ai_artifact_id=ai_artifact_id,
            ai_artifact_sha256=ai_artifact_sha256,
            expected_query_id=expected_query_id,
            artifact_query_evidence=expected_evidence,
        )
    result_summary = _phase1a_json_object(
        row.get("result_summary"),
        label=f"{label} result_summary",
    )
    _phase1a_exact_keys(
        result_summary,
        expected={
            "artifact_sha256",
            "direction_counts",
            "source_date_count",
            "support_count",
        },
        label=f"{label} result_summary",
    )
    if result_summary != expected_result_summary:
        raise ResearchRegistryDriftError(f"{label} result summary differs from artifact")

    exposure_key = f"{campaign_key}:query:{prior_slice_index:02d}:{expected_query_id}"
    if (
        row.get("exposure_key") != exposure_key
        or row.get("result_artifact_id") != ai_artifact_id
        or row.get("artifact_sha256") != ai_artifact_sha256
        or row.get("artifact_type") != "DISCOVERY_EXPOSURE_RESULT"
    ):
        raise ResearchRegistryDriftError(f"{label} result artifact lineage drift")


def _phase1a_slice_rollup_items(
    row: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    pattern_key = row.get("pattern_key")
    features = _phase1a_json_object(
        row.get("feature_definition_versions"),
        label=f"pattern {pattern_key} feature roll-up",
    )
    summaries = _phase1a_json_object(
        row.get("forward_first_touch_summaries"),
        label=f"pattern {pattern_key} summary roll-up",
    )
    if (
        features.get("rollup_schema") != _PHASE1A_PATTERN_ROLLUP_SCHEMA
        or summaries.get("rollup_schema") != _PHASE1A_PATTERN_ROLLUP_SCHEMA
    ):
        raise ResearchRegistryDriftError(f"pattern {pattern_key} roll-up schema drift")
    raw_features = features.get("slice_identities")
    raw_summaries = summaries.get("slice_observations")
    if not isinstance(raw_features, list) or not isinstance(raw_summaries, list):
        raise ResearchRegistryDriftError(f"pattern {pattern_key} slice roll-up is malformed")
    feature_items = [
        _phase1a_json_object(value, label=f"pattern {pattern_key} feature item")
        for value in raw_features
    ]
    summary_items = [
        _phase1a_json_object(value, label=f"pattern {pattern_key} summary item")
        for value in raw_summaries
    ]
    return feature_items, summary_items


def _validate_phase1a_pattern_predecessor(
    pattern_rows: Sequence[Mapping[str, Any]],
    *,
    campaign_key: str,
    source_interval_start: datetime,
    source_interval_end: datetime,
    query_rows_by_id: Mapping[str, Mapping[str, Any]],
    query_definitions: tuple[tuple[str, str], ...],
    definitions_by_id: Mapping[str, Mapping[str, object]],
    allow_missing_final: bool = False,
    connection: psycopg.Connection[dict[str, Any]] | None = None,
    ai_canonical_spec: Mapping[str, Any] | None = None,
    ai_run_spec_id: int | None = None,
    ai_run_fingerprint: str | None = None,
    ai_artifact_id: int | None = None,
    ai_artifact_sha256: str | None = None,
    artifact_query_evidence_by_id: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[int, ...]:
    expected_start = source_interval_start.isoformat()
    expected_end = source_interval_end.isoformat()
    expected_exposure_ids = {
        int(query_rows_by_id[query_id]["discovery_exposure_id"])
        for query_id, _ in query_definitions
    }
    summaries_by_exposure: dict[int, list[tuple[Mapping[str, Any], dict[str, Any]]]] = {}
    features_by_exposure: dict[int, list[tuple[Mapping[str, Any], dict[str, Any]]]] = {}
    for pattern_row in pattern_rows:
        feature_items, summary_items = _phase1a_slice_rollup_items(pattern_row)
        for item in feature_items:
            exposure_id = item.get("discovery_exposure_id")
            if exposure_id in expected_exposure_ids:
                features_by_exposure.setdefault(int(exposure_id), []).append((pattern_row, item))
        for item in summary_items:
            exposure_id = item.get("discovery_exposure_id")
            at_expected_interval = (
                item.get("source_interval_start") == expected_start
                and item.get("source_interval_end") == expected_end
            )
            if at_expected_interval and exposure_id not in expected_exposure_ids:
                raise ResearchRegistryDriftError(
                    "unexpected pattern observation exists in the predecessor interval"
                )
            if exposure_id in expected_exposure_ids:
                summaries_by_exposure.setdefault(int(exposure_id), []).append((pattern_row, item))

    pattern_ids: list[int] = []
    for query_index, (query_id, definition_sha256) in enumerate(query_definitions):
        query_row = query_rows_by_id[query_id]
        exposure_id = int(query_row["discovery_exposure_id"])
        summary_matches = summaries_by_exposure.get(exposure_id, [])
        feature_matches = features_by_exposure.get(exposure_id, [])
        if not summary_matches and not feature_matches:
            if allow_missing_final and query_index == len(query_definitions) - 1:
                continue
            raise ResearchRegistryDriftError(
                f"query {query_id} does not have exactly one predecessor pattern observation"
            )
        if len(summary_matches) != 1 or len(feature_matches) != 1:
            raise ResearchRegistryDriftError(
                f"query {query_id} does not have exactly one predecessor pattern observation"
            )
        summary_pattern, summary = summary_matches[0]
        feature_pattern, feature = feature_matches[0]
        expected_pattern_key = f"{campaign_key}:{query_id}"
        if (
            summary_pattern.get("pattern_id") != feature_pattern.get("pattern_id")
            or summary_pattern.get("pattern_key") != expected_pattern_key
            or feature_pattern.get("pattern_key") != expected_pattern_key
        ):
            raise ResearchRegistryDriftError(f"query {query_id} pattern identity drift")
        _phase1a_exact_keys(
            summary,
            expected={
                "counterexamples",
                "discovery_exposure_id",
                "exposure_key",
                "forward_first_touch_summary",
                "query_definition_sha256",
                "research_run_spec_id",
                "result_artifact_id",
                "run_fingerprint",
                "source_interval_end",
                "source_interval_start",
                "support_count",
            },
            label=f"query {query_id} pattern observation",
        )
        if not isinstance(summary.get("counterexamples"), list) or not isinstance(
            summary.get("forward_first_touch_summary"),
            dict,
        ):
            raise ResearchRegistryDriftError(f"query {query_id} pattern evidence is malformed")
        expected_summary_identity = {
            "discovery_exposure_id": exposure_id,
            "exposure_key": query_row["exposure_key"],
            "query_definition_sha256": definition_sha256,
            "research_run_spec_id": query_row["research_run_spec_id"],
            "result_artifact_id": query_row["result_artifact_id"],
            "run_fingerprint": query_row["run_fingerprint"],
            "source_interval_end": expected_end,
            "source_interval_start": expected_start,
            "support_count": query_row["result_summary"]["support_count"],
        }
        if any(summary.get(key) != value for key, value in expected_summary_identity.items()):
            raise ResearchRegistryDriftError(f"query {query_id} pattern observation drift")

        _phase1a_exact_keys(
            feature,
            expected={
                "discovery_exposure_id",
                "feature_identity",
                "query_definition",
                "query_definition_sha256",
                "run_fingerprint",
            },
            label=f"query {query_id} pattern feature identity",
        )
        expected_feature_identity = {
            "discovery_exposure_id": exposure_id,
            "query_definition": dict(definitions_by_id[query_id]),
            "query_definition_sha256": definition_sha256,
            "run_fingerprint": query_row["run_fingerprint"],
        }
        feature_identity = feature.get("feature_identity")
        if (
            not isinstance(feature_identity, dict)
            or not feature_identity
            or any(feature.get(key) != value for key, value in expected_feature_identity.items())
        ):
            raise ResearchRegistryDriftError(f"query {query_id} pattern feature identity drift")
        registrar_value = feature_identity.get("rollup_registrar")
        if registrar_value is not None:
            if (
                connection is None
                or ai_canonical_spec is None
                or ai_run_spec_id is None
                or ai_run_fingerprint is None
                or ai_artifact_id is None
                or ai_artifact_sha256 is None
                or artifact_query_evidence_by_id is None
                or query_id not in artifact_query_evidence_by_id
            ):
                raise ResearchRegistryDriftError(
                    f"query {query_id} pattern recovery context is incomplete"
                )
            query_canonical_spec = _phase1a_json_object(
                query_row.get("canonical_spec"),
                label=f"query {query_id} canonical RunSpec",
            )
            query_parameters = _phase1a_json_object(
                query_canonical_spec.get("parameters"),
                label=f"query {query_id} RunSpec parameters",
            )
            registrar = _phase1a_json_object(
                registrar_value,
                label=f"query {query_id} pattern recovery registrar",
            )
            _validate_phase1a_recovery_projection_control(
                connection,
                query_spec=query_canonical_spec,
                parameters=query_parameters,
                ai_spec=ai_canonical_spec,
                ai_run_spec_id=ai_run_spec_id,
                ai_run_fingerprint=ai_run_fingerprint,
                ai_artifact_id=ai_artifact_id,
                ai_artifact_sha256=ai_artifact_sha256,
                expected_query_id=query_id,
                artifact_query_evidence=artifact_query_evidence_by_id[query_id],
                recovery_identity=registrar,
                identity_schema=_PHASE1A_RECOVERY_REGISTRAR_SCHEMA,
                identity_mode="IMMUTABLE_AI_ARTIFACT_PATTERN_RECOVERY",
                required_action="repair_existing_query_pattern_ids",
                require_query_execution_identity=False,
            )
        pattern_id = summary_pattern.get("pattern_id")
        if isinstance(pattern_id, bool) or not isinstance(pattern_id, int) or pattern_id <= 0:
            raise ResearchRegistryDriftError(f"query {query_id} pattern ID is invalid")
        pattern_ids.append(pattern_id)
    return tuple(pattern_ids)


@_translate_psycopg_errors("Phase 1A predecessor slice verification")
def verify_phase1a_predecessor_slice(
    database_url: str,
    *,
    campaign_key: str,
    prior_slice_index: int,
    source_interval_start: datetime,
    source_interval_end: datetime,
    requested_source_dates: Sequence[date],
    query_definition_sha256_by_id: Mapping[str, str],
) -> Phase1APredecessorSliceReport:
    """Fail closed unless one prior five-date Discovery slice is exactly complete."""

    (
        campaign,
        slice_index,
        interval_start,
        interval_end,
        source_dates,
        query_definitions,
    ) = _phase1a_predecessor_inputs(
        database_url,
        campaign_key=campaign_key,
        prior_slice_index=prior_slice_index,
        source_interval_start=source_interval_start,
        source_interval_end=source_interval_end,
        requested_source_dates=requested_source_dates,
        query_definition_sha256_by_id=query_definition_sha256_by_id,
    )
    requested_date_strings = [day.isoformat() for day in source_dates]
    ai_key = f"{campaign}:ai-slice:{slice_index:02d}"
    query_key_pattern = f"{campaign}:query:{slice_index:02d}:%"

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        connection.isolation_level = IsolationLevel.SERIALIZABLE
        with connection.transaction():
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (campaign,),
            )
            campaign_row = connection.execute(
                """
                SELECT campaign_id, campaign_key
                FROM systematic_fx.campaigns
                WHERE campaign_key = %s
                FOR SHARE
                """,
                (campaign,),
            ).fetchone()
            campaign_row = _row_or_error(campaign_row, label=f"campaign {campaign}")
            campaign_id = int(campaign_row["campaign_id"])

            exposure_rows = connection.execute(
                """
                SELECT exposure.discovery_exposure_id, exposure.campaign_id,
                       exposure.exposure_key, exposure.exposure_type,
                       exposure.source_interval_start,
                       exposure.source_interval_end, exposure.visible_to_ai,
                       exposure.research_eligible, exposure.query_spec,
                       exposure.result_summary, exposure.result_artifact_id,
                       exposure.research_run_spec_id,
                       run_spec.run_fingerprint, run_spec.run_kind,
                       run_spec.engine_version,
                       run_spec.parent_run_spec_id, run_spec.canonical_spec,
                       artifact.artifact_key, artifact.artifact_type,
                       artifact.uri AS artifact_uri,
                       artifact.sha256 AS artifact_sha256,
                       artifact.byte_size AS artifact_byte_size,
                       artifact.media_type AS artifact_media_type,
                       artifact.metadata AS artifact_metadata
                FROM systematic_fx.discovery_exposures AS exposure
                JOIN systematic_fx.research_run_specs AS run_spec
                  ON run_spec.research_run_spec_id = exposure.research_run_spec_id
                 AND run_spec.campaign_id = exposure.campaign_id
                JOIN systematic_fx.artifacts AS artifact
                  ON artifact.artifact_id = exposure.result_artifact_id
                WHERE exposure.campaign_id = %s
                  AND exposure.exposure_type IN ('AI_SLICE', 'QUERY')
                  AND (
                    (exposure.source_interval_start = %s
                     AND exposure.source_interval_end = %s)
                    OR run_spec.canonical_spec #>> '{parameters,slice_index}' = %s
                    OR run_spec.canonical_spec #> '{parameters,requested_source_dates}' = %s
                    OR exposure.exposure_key = %s
                    OR exposure.exposure_key LIKE %s
                  )
                ORDER BY exposure.discovery_exposure_id
                FOR SHARE OF exposure, run_spec, artifact
                """,
                (
                    campaign_id,
                    interval_start,
                    interval_end,
                    str(slice_index),
                    Jsonb(requested_date_strings),
                    ai_key,
                    query_key_pattern,
                ),
            ).fetchall()
            if not exposure_rows:
                raise ResearchRegistryDriftError("Phase 1A predecessor slice is missing")
            for row in exposure_rows:
                _assert_fields(
                    label=f"predecessor exposure {row['exposure_key']}",
                    row=row,
                    expected={
                        "research_eligible": False,
                        "source_interval_end": interval_end,
                        "source_interval_start": interval_start,
                        "visible_to_ai": True,
                    },
                )
            ai_rows = [row for row in exposure_rows if row["exposure_type"] == "AI_SLICE"]
            query_rows = [row for row in exposure_rows if row["exposure_type"] == "QUERY"]
            if len(ai_rows) != 1:
                raise ResearchRegistryDriftError(
                    "Phase 1A predecessor requires exactly one AI_SLICE exposure"
                )
            if len(query_rows) != len(query_definitions):
                raise ResearchRegistryDriftError(
                    "Phase 1A predecessor QUERY exposure cardinality drift"
                )
            (
                candidate_definitions,
                artifact_id,
                artifact_sha256,
                ai_run_spec_id,
                ai_run_fingerprint,
            ) = _validate_phase1a_ai_predecessor(
                ai_rows[0],
                campaign_key=campaign,
                prior_slice_index=slice_index,
                requested_date_strings=requested_date_strings,
                query_definitions=query_definitions,
            )
            _verify_phase1a_child_parent_success(
                connection,
                child_run_spec=ai_rows[0],
                exposure_type="AI_SLICE",
                exposure_result_summary=ai_rows[0]["result_summary"],
            )
            definitions_by_id = {
                query_id: definition
                for (query_id, _), definition in zip(
                    query_definitions,
                    candidate_definitions,
                    strict=True,
                )
            }
            query_rows_by_id: dict[str, Mapping[str, Any]] = {}
            for row in query_rows:
                query_spec = _phase1a_json_object(
                    row.get("query_spec"),
                    label="predecessor QUERY query_spec",
                )
                candidate = _phase1a_json_object(
                    query_spec.get("candidate_query"),
                    label="predecessor QUERY candidate_query",
                )
                query_id = candidate.get("id")
                if not isinstance(query_id, str) or query_id in query_rows_by_id:
                    raise ResearchRegistryDriftError(
                        "Phase 1A predecessor has duplicate or invalid QUERY IDs"
                    )
                query_rows_by_id[query_id] = row
            expected_query_ids = {query_id for query_id, _ in query_definitions}
            if set(query_rows_by_id) != expected_query_ids:
                raise ResearchRegistryDriftError(
                    "Phase 1A predecessor has missing or unexpected QUERY IDs"
                )
            query_exposure_ids: list[int] = []
            run_spec_ids = {ai_run_spec_id}
            ai_canonical_spec = _phase1a_json_object(
                ai_rows[0].get("canonical_spec"),
                label="predecessor AI_SLICE canonical RunSpec",
            )
            artifact_query_evidence_by_id = _phase1a_load_discovery_query_evidence(
                ai_rows[0],
                candidates=candidate_definitions,
                query_definitions=query_definitions,
                requested_date_strings=requested_date_strings,
                ai_run_fingerprint=ai_run_fingerprint,
                ai_canonical_spec=ai_canonical_spec,
            )
            for query_id, definition_sha256 in query_definitions:
                row = query_rows_by_id[query_id]
                _validate_phase1a_query_predecessor(
                    connection,
                    row,
                    campaign_key=campaign,
                    prior_slice_index=slice_index,
                    requested_date_strings=requested_date_strings,
                    expected_query_id=query_id,
                    expected_definition_sha256=definition_sha256,
                    expected_definition=definitions_by_id[query_id],
                    ai_artifact_id=artifact_id,
                    ai_artifact_sha256=artifact_sha256,
                    ai_run_spec_id=ai_run_spec_id,
                    ai_run_fingerprint=ai_run_fingerprint,
                    ai_canonical_spec=ai_canonical_spec,
                    artifact_query_evidence=artifact_query_evidence_by_id[query_id],
                )
                run_spec_id = row.get("research_run_spec_id")
                exposure_id = row.get("discovery_exposure_id")
                if (
                    isinstance(run_spec_id, bool)
                    or not isinstance(run_spec_id, int)
                    or run_spec_id <= 0
                    or run_spec_id in run_spec_ids
                    or isinstance(exposure_id, bool)
                    or not isinstance(exposure_id, int)
                    or exposure_id <= 0
                ):
                    raise ResearchRegistryDriftError(
                        "Phase 1A predecessor QUERY RunSpec/exposure identity drift"
                    )
                run_spec_ids.add(run_spec_id)
                query_exposure_ids.append(exposure_id)

            success_rows = connection.execute(
                """
                SELECT research_run_spec_id, result_artifact_id
                FROM systematic_fx.research_run_attempts
                WHERE research_run_spec_id = ANY(%s::bigint[])
                  AND status = 'SUCCEEDED'
                ORDER BY research_run_spec_id
                FOR SHARE
                """,
                (list(run_spec_ids),),
            ).fetchall()
            successes_by_spec: dict[int, list[Mapping[str, Any]]] = {}
            for success in success_rows:
                successes_by_spec.setdefault(int(success["research_run_spec_id"]), []).append(
                    success
                )
            if any(
                len(successes_by_spec.get(run_spec_id, [])) != 1
                or successes_by_spec[run_spec_id][0]["result_artifact_id"] != artifact_id
                for run_spec_id in run_spec_ids
            ):
                raise ResearchRegistryDriftError(
                    "Phase 1A predecessor lacks an exact successful RunSpec/artifact attempt"
                )

            pattern_rows = connection.execute(
                """
                SELECT pattern_id, pattern_key, feature_definition_versions,
                       forward_first_touch_summaries
                FROM systematic_fx.pattern_ledger
                WHERE campaign_id = %s
                ORDER BY pattern_id
                FOR SHARE
                """,
                (campaign_id,),
            ).fetchall()
            pattern_ids = _validate_phase1a_pattern_predecessor(
                pattern_rows,
                campaign_key=campaign,
                source_interval_start=interval_start,
                source_interval_end=interval_end,
                query_rows_by_id=query_rows_by_id,
                query_definitions=query_definitions,
                definitions_by_id=definitions_by_id,
                connection=connection,
                ai_canonical_spec=ai_canonical_spec,
                ai_run_spec_id=ai_run_spec_id,
                ai_run_fingerprint=ai_run_fingerprint,
                ai_artifact_id=artifact_id,
                ai_artifact_sha256=artifact_sha256,
                artifact_query_evidence_by_id=artifact_query_evidence_by_id,
            )

    return Phase1APredecessorSliceReport(
        prior_slice_index=slice_index,
        ai_exposure_id=int(ai_rows[0]["discovery_exposure_id"]),
        query_exposure_ids=tuple(query_exposure_ids),
        pattern_ids=pattern_ids,
        result_artifact_id=artifact_id,
    )


@_translate_psycopg_errors("Phase 1A current slice prefix verification")
def verify_phase1a_current_slice_prefix(
    database_url: str,
    *,
    campaign_key: str,
    slice_index: int,
    source_interval_start: datetime,
    source_interval_end: datetime,
    requested_source_dates: Sequence[date],
    expected_feature_run_fingerprint: str | None,
    query_definition_sha256_by_id: Mapping[str, str],
) -> Phase1ACurrentSlicePrefixReport:
    """Allow only an empty current slice or one coherent, ordered resume prefix."""

    (
        campaign,
        normalized_slice_index,
        interval_start,
        interval_end,
        source_dates,
        query_definitions,
    ) = _phase1a_predecessor_inputs(
        database_url,
        campaign_key=campaign_key,
        prior_slice_index=slice_index,
        source_interval_start=source_interval_start,
        source_interval_end=source_interval_end,
        requested_source_dates=requested_source_dates,
        query_definition_sha256_by_id=query_definition_sha256_by_id,
    )
    if expected_feature_run_fingerprint is not None and (
        not isinstance(expected_feature_run_fingerprint, str)
        or _SHA256.fullmatch(expected_feature_run_fingerprint) is None
    ):
        raise ResearchRegistryError(
            "expected_feature_run_fingerprint must be null or a lowercase SHA-256"
        )
    requested_date_strings = [day.isoformat() for day in source_dates]
    ai_key = f"{campaign}:ai-slice:{normalized_slice_index:02d}"
    query_key_pattern = f"{campaign}:query:{normalized_slice_index:02d}:%"

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        connection.isolation_level = IsolationLevel.SERIALIZABLE
        with connection.transaction():
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (campaign,),
            )
            campaign_row = connection.execute(
                """
                SELECT campaign_id, campaign_key
                FROM systematic_fx.campaigns
                WHERE campaign_key = %s
                FOR SHARE
                """,
                (campaign,),
            ).fetchone()
            campaign_row = _row_or_error(campaign_row, label=f"campaign {campaign}")
            campaign_id = int(campaign_row["campaign_id"])
            exposure_rows = connection.execute(
                """
                SELECT exposure.discovery_exposure_id, exposure.campaign_id,
                       exposure.exposure_key, exposure.exposure_type,
                       exposure.source_interval_start, exposure.source_interval_end,
                       exposure.visible_to_ai, exposure.research_eligible,
                       exposure.query_spec, exposure.result_summary,
                       exposure.result_artifact_id, exposure.research_run_spec_id,
                       run_spec.run_fingerprint, run_spec.run_kind,
                       run_spec.engine_version,
                       run_spec.parent_run_spec_id, run_spec.canonical_spec,
                       artifact.artifact_key, artifact.artifact_type,
                       artifact.uri AS artifact_uri,
                       artifact.sha256 AS artifact_sha256,
                       artifact.byte_size AS artifact_byte_size,
                       artifact.media_type AS artifact_media_type,
                       artifact.metadata AS artifact_metadata
                FROM systematic_fx.discovery_exposures AS exposure
                JOIN systematic_fx.research_run_specs AS run_spec
                  ON run_spec.research_run_spec_id = exposure.research_run_spec_id
                 AND run_spec.campaign_id = exposure.campaign_id
                JOIN systematic_fx.artifacts AS artifact
                  ON artifact.artifact_id = exposure.result_artifact_id
                WHERE exposure.campaign_id = %s
                  AND exposure.exposure_type IN ('AI_SLICE', 'QUERY')
                  AND (
                    (exposure.source_interval_start = %s
                     AND exposure.source_interval_end = %s)
                    OR run_spec.canonical_spec #>> '{parameters,slice_index}' = %s
                    OR run_spec.canonical_spec #> '{parameters,requested_source_dates}' = %s
                    OR exposure.exposure_key = %s
                    OR exposure.exposure_key LIKE %s
                  )
                ORDER BY exposure.discovery_exposure_id
                FOR SHARE OF exposure, run_spec, artifact
                """,
                (
                    campaign_id,
                    interval_start,
                    interval_end,
                    str(normalized_slice_index),
                    Jsonb(requested_date_strings),
                    ai_key,
                    query_key_pattern,
                ),
            ).fetchall()
            pattern_rows = connection.execute(
                """
                SELECT pattern_id, pattern_key, feature_definition_versions,
                       forward_first_touch_summaries
                FROM systematic_fx.pattern_ledger
                WHERE campaign_id = %s
                ORDER BY pattern_id
                FOR SHARE
                """,
                (campaign_id,),
            ).fetchall()
            if not exposure_rows:
                slice_run_rows = connection.execute(
                    """
                    SELECT run_spec.research_run_spec_id, run_spec.run_kind,
                           attempt.research_run_attempt_id, attempt.status,
                           attempt.result_artifact_id, attempt.reused_attempt_id,
                           attempt.trade_ledger_artifact_id
                    FROM systematic_fx.research_run_specs AS run_spec
                    LEFT JOIN systematic_fx.research_run_attempts AS attempt
                      ON attempt.research_run_spec_id = run_spec.research_run_spec_id
                    WHERE run_spec.campaign_id = %s
                      AND run_spec.run_kind IN
                          ('FEATURE_BUILD', 'AI_SLICE', 'QUERY', 'VALIDATION')
                      AND (
                        run_spec.canonical_spec #>> '{parameters,slice_index}' = %s
                        OR run_spec.canonical_spec #> '{parameters,requested_source_dates}' = %s
                      )
                    ORDER BY run_spec.research_run_spec_id,
                             attempt.research_run_attempt_id
                    FOR SHARE OF run_spec
                    """,
                    (
                        campaign_id,
                        str(normalized_slice_index),
                        Jsonb(requested_date_strings),
                    ),
                ).fetchall()
                _validate_phase1a_pattern_predecessor(
                    pattern_rows,
                    campaign_key=campaign,
                    source_interval_start=interval_start,
                    source_interval_end=interval_end,
                    query_rows_by_id={},
                    query_definitions=(),
                    definitions_by_id={},
                )
                if slice_run_rows:
                    identities = ", ".join(
                        f"{row['run_kind']}#{row['research_run_spec_id']}:{row['status'] or 'NO_ATTEMPT'}"
                        for row in slice_run_rows
                    )
                    if any(row.get("run_kind") != "FEATURE_BUILD" for row in slice_run_rows):
                        raise ResearchRegistryDriftError(
                            "current Phase 1A slice has non-FEATURE RunSpec state without a "
                            f"governed AI exposure: {identities}"
                        )
                    if any(
                        row.get("research_run_attempt_id") is None or row.get("status") != "FAILED"
                        for row in slice_run_rows
                    ):
                        raise ResearchRegistryDriftError(
                            "current Phase 1A feature-only state is not composed solely of "
                            f"terminal FAILED attempts: {identities}"
                        )
                    artifact_link_fields = (
                        "result_artifact_id",
                        "reused_attempt_id",
                        "trade_ledger_artifact_id",
                    )
                    if any(
                        row.get(field) is not None
                        for row in slice_run_rows
                        for field in artifact_link_fields
                    ):
                        raise ResearchRegistryDriftError(
                            "current Phase 1A FAILED feature attempt has artifact, reuse, "
                            "or trade-ledger linkage"
                        )
                    return Phase1ACurrentSlicePrefixReport(
                        slice_index=normalized_slice_index,
                        state="FAILED_FEATURE_RETRYABLE",
                        feature_run_spec_id=None,
                        ai_exposure_id=None,
                        query_exposure_ids=(),
                        pattern_ids=(),
                        result_artifact_id=None,
                        missing_pattern_query_id=None,
                    )
                return Phase1ACurrentSlicePrefixReport(
                    slice_index=normalized_slice_index,
                    state="EMPTY",
                    feature_run_spec_id=None,
                    ai_exposure_id=None,
                    query_exposure_ids=(),
                    pattern_ids=(),
                    result_artifact_id=None,
                    missing_pattern_query_id=None,
                )

            for row in exposure_rows:
                _assert_fields(
                    label=f"current-slice exposure {row['exposure_key']}",
                    row=row,
                    expected={
                        "research_eligible": False,
                        "source_interval_end": interval_end,
                        "source_interval_start": interval_start,
                        "visible_to_ai": True,
                    },
                )
            ai_rows = [row for row in exposure_rows if row["exposure_type"] == "AI_SLICE"]
            query_rows = [row for row in exposure_rows if row["exposure_type"] == "QUERY"]
            if len(ai_rows) != 1:
                raise ResearchRegistryDriftError(
                    "current Phase 1A slice requires one AI_SLICE before any QUERY"
                )
            if len(query_rows) > len(query_definitions):
                raise ResearchRegistryDriftError("current Phase 1A QUERY prefix exceeds its budget")
            (
                candidate_definitions,
                artifact_id,
                artifact_sha256,
                ai_run_spec_id,
                ai_run_fingerprint,
            ) = _validate_phase1a_ai_predecessor(
                ai_rows[0],
                campaign_key=campaign,
                prior_slice_index=normalized_slice_index,
                requested_date_strings=requested_date_strings,
                query_definitions=query_definitions,
            )
            (
                feature_run_spec_id,
                feature_run_fingerprint,
                _,
                _,
            ) = _verify_phase1a_child_parent_success(
                connection,
                child_run_spec=ai_rows[0],
                exposure_type="AI_SLICE",
                exposure_result_summary=ai_rows[0]["result_summary"],
            )
            if (
                expected_feature_run_fingerprint is not None
                and feature_run_fingerprint != expected_feature_run_fingerprint
            ):
                raise ResearchRegistryDriftError(
                    "current Phase 1A AI_SLICE belongs to a different FEATURE_BUILD"
                )

            definitions_by_id = {
                query_id: definition
                for (query_id, _), definition in zip(
                    query_definitions,
                    candidate_definitions,
                    strict=True,
                )
            }
            query_rows_by_id: dict[str, Mapping[str, Any]] = {}
            for row in query_rows:
                query_spec = _phase1a_json_object(
                    row.get("query_spec"),
                    label="current-slice QUERY query_spec",
                )
                candidate = _phase1a_json_object(
                    query_spec.get("candidate_query"),
                    label="current-slice QUERY candidate_query",
                )
                query_id = candidate.get("id")
                if not isinstance(query_id, str) or query_id in query_rows_by_id:
                    raise ResearchRegistryDriftError(
                        "current Phase 1A slice has duplicate or invalid QUERY IDs"
                    )
                query_rows_by_id[query_id] = row
            completed_query_definitions = query_definitions[: len(query_rows)]
            expected_completed_ids = {query_id for query_id, _ in completed_query_definitions}
            if set(query_rows_by_id) != expected_completed_ids:
                raise ResearchRegistryDriftError(
                    "current Phase 1A QUERY exposures are not an ordered config prefix"
                )

            query_exposure_ids: list[int] = []
            run_spec_ids = {ai_run_spec_id}
            ai_canonical_spec = _phase1a_json_object(
                ai_rows[0].get("canonical_spec"),
                label="current AI_SLICE canonical RunSpec",
            )
            artifact_query_evidence_by_id = _phase1a_load_discovery_query_evidence(
                ai_rows[0],
                candidates=candidate_definitions,
                query_definitions=query_definitions,
                requested_date_strings=requested_date_strings,
                ai_run_fingerprint=ai_run_fingerprint,
                ai_canonical_spec=ai_canonical_spec,
            )
            for query_id, definition_sha256 in completed_query_definitions:
                row = query_rows_by_id[query_id]
                _validate_phase1a_query_predecessor(
                    connection,
                    row,
                    campaign_key=campaign,
                    prior_slice_index=normalized_slice_index,
                    requested_date_strings=requested_date_strings,
                    expected_query_id=query_id,
                    expected_definition_sha256=definition_sha256,
                    expected_definition=definitions_by_id[query_id],
                    ai_artifact_id=artifact_id,
                    ai_artifact_sha256=artifact_sha256,
                    ai_run_spec_id=ai_run_spec_id,
                    ai_run_fingerprint=ai_run_fingerprint,
                    ai_canonical_spec=ai_canonical_spec,
                    artifact_query_evidence=artifact_query_evidence_by_id[query_id],
                )
                run_spec_id = row.get("research_run_spec_id")
                exposure_id = row.get("discovery_exposure_id")
                if (
                    isinstance(run_spec_id, bool)
                    or not isinstance(run_spec_id, int)
                    or run_spec_id <= 0
                    or run_spec_id in run_spec_ids
                    or isinstance(exposure_id, bool)
                    or not isinstance(exposure_id, int)
                    or exposure_id <= 0
                ):
                    raise ResearchRegistryDriftError(
                        "current Phase 1A QUERY RunSpec/exposure identity drift"
                    )
                run_spec_ids.add(run_spec_id)
                query_exposure_ids.append(exposure_id)
            success_rows = connection.execute(
                """
                SELECT research_run_spec_id, result_artifact_id
                FROM systematic_fx.research_run_attempts
                WHERE research_run_spec_id = ANY(%s::bigint[])
                  AND status = 'SUCCEEDED'
                ORDER BY research_run_spec_id
                FOR SHARE
                """,
                (list(run_spec_ids),),
            ).fetchall()
            successes_by_spec: dict[int, list[Mapping[str, Any]]] = {}
            for success in success_rows:
                successes_by_spec.setdefault(int(success["research_run_spec_id"]), []).append(
                    success
                )
            if any(
                len(successes_by_spec.get(run_spec_id, [])) != 1
                or successes_by_spec[run_spec_id][0]["result_artifact_id"] != artifact_id
                for run_spec_id in run_spec_ids
            ):
                raise ResearchRegistryDriftError(
                    "current Phase 1A prefix lacks an exact successful Discovery attempt"
                )
            pattern_ids = _validate_phase1a_pattern_predecessor(
                pattern_rows,
                campaign_key=campaign,
                source_interval_start=interval_start,
                source_interval_end=interval_end,
                query_rows_by_id=query_rows_by_id,
                query_definitions=completed_query_definitions,
                definitions_by_id=definitions_by_id,
                allow_missing_final=True,
                connection=connection,
                ai_canonical_spec=ai_canonical_spec,
                ai_run_spec_id=ai_run_spec_id,
                ai_run_fingerprint=ai_run_fingerprint,
                ai_artifact_id=artifact_id,
                ai_artifact_sha256=artifact_sha256,
                artifact_query_evidence_by_id=artifact_query_evidence_by_id,
            )
            missing_pattern_query_id = (
                completed_query_definitions[-1][0]
                if len(pattern_ids) < len(completed_query_definitions)
                else None
            )

    return Phase1ACurrentSlicePrefixReport(
        slice_index=normalized_slice_index,
        state="RESUMABLE",
        feature_run_spec_id=feature_run_spec_id,
        ai_exposure_id=int(ai_rows[0]["discovery_exposure_id"]),
        query_exposure_ids=tuple(query_exposure_ids),
        pattern_ids=pattern_ids,
        result_artifact_id=artifact_id,
        missing_pattern_query_id=missing_pattern_query_id,
    )


@_translate_psycopg_errors("Phase 1A partial-recovery source loading")
def load_phase1a_partial_recovery_source(
    database_url: str,
    *,
    campaign_key: str,
    prefix: Phase1ACurrentSlicePrefixReport,
) -> Phase1APartialRecoverySource:
    """Load the exact immutable FEATURE/AI/QUERY prefix without reading raw data."""

    if not isinstance(database_url, str) or not database_url.strip():
        raise ResearchRegistryError("database_url must be a non-empty string")
    campaign = _nonempty(campaign_key, label="campaign_key")
    if campaign != _PHASE1A_CAMPAIGN_KEY:
        raise ResearchRegistryError("partial recovery is restricted to the Phase 1A campaign")
    if not isinstance(prefix, Phase1ACurrentSlicePrefixReport) or prefix.state != "RESUMABLE":
        raise ResearchRegistryError("partial recovery requires a validated RESUMABLE prefix")
    if (
        prefix.feature_run_spec_id is None
        or prefix.ai_exposure_id is None
        or prefix.result_artifact_id is None
    ):
        raise ResearchRegistryError("partial recovery prefix identities are incomplete")

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        connection.isolation_level = IsolationLevel.SERIALIZABLE
        with connection.transaction():
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (campaign,),
            )
            source_rows = connection.execute(
                """
                SELECT campaign.campaign_id,
                       feature.research_run_spec_id AS feature_run_spec_id,
                       feature.run_fingerprint AS feature_run_fingerprint,
                       feature.run_kind AS feature_run_kind,
                       feature.canonical_spec AS feature_canonical_spec,
                       feature_attempt.research_run_attempt_id
                           AS feature_success_attempt_id,
                       feature_attempt.result_artifact_id
                           AS feature_result_artifact_id,
                       ai.research_run_spec_id AS ai_run_spec_id,
                       ai.run_fingerprint AS ai_run_fingerprint,
                       ai.run_kind AS ai_run_kind,
                       ai.canonical_spec AS ai_canonical_spec,
                       ai_attempt.research_run_attempt_id AS ai_success_attempt_id,
                       exposure.discovery_exposure_id AS ai_exposure_id,
                       exposure.result_artifact_id,
                       artifact.uri AS artifact_uri,
                       artifact.sha256 AS artifact_sha256,
                       artifact.byte_size AS artifact_byte_size,
                       artifact.artifact_type,
                       artifact.metadata AS artifact_metadata
                FROM systematic_fx.campaigns AS campaign
                JOIN systematic_fx.discovery_exposures AS exposure
                  ON exposure.campaign_id = campaign.campaign_id
                 AND exposure.discovery_exposure_id = %s
                 AND exposure.exposure_type = 'AI_SLICE'
                JOIN systematic_fx.research_run_specs AS ai
                  ON ai.research_run_spec_id = exposure.research_run_spec_id
                 AND ai.campaign_id = campaign.campaign_id
                JOIN systematic_fx.research_run_attempts AS ai_attempt
                  ON ai_attempt.research_run_spec_id = ai.research_run_spec_id
                 AND ai_attempt.status = 'SUCCEEDED'
                 AND ai_attempt.result_artifact_id = exposure.result_artifact_id
                JOIN systematic_fx.research_run_specs AS feature
                  ON feature.research_run_spec_id = ai.parent_run_spec_id
                 AND feature.campaign_id = campaign.campaign_id
                JOIN systematic_fx.research_run_attempts AS feature_attempt
                  ON feature_attempt.research_run_spec_id = feature.research_run_spec_id
                 AND feature_attempt.status = 'SUCCEEDED'
                JOIN systematic_fx.artifacts AS artifact
                  ON artifact.artifact_id = exposure.result_artifact_id
                WHERE campaign.campaign_key = %s
                FOR SHARE OF campaign, exposure, ai, ai_attempt, feature,
                             feature_attempt, artifact
                """,
                (prefix.ai_exposure_id, campaign),
            ).fetchall()
            if len(source_rows) != 1:
                raise ResearchRegistryDriftError(
                    "partial recovery source does not have one exact FEATURE/AI success chain"
                )
            source = source_rows[0]
            _assert_fields(
                label="partial recovery source",
                row=source,
                expected={
                    "ai_exposure_id": prefix.ai_exposure_id,
                    "ai_run_kind": "AI_SLICE",
                    "artifact_type": "DISCOVERY_EXPOSURE_RESULT",
                    "feature_run_kind": "FEATURE_BUILD",
                    "feature_run_spec_id": prefix.feature_run_spec_id,
                    "result_artifact_id": prefix.result_artifact_id,
                },
            )
            feature_spec = _phase1a_json_object(
                source.get("feature_canonical_spec"),
                label="partial recovery FEATURE RunSpec",
            )
            ai_spec = _phase1a_json_object(
                source.get("ai_canonical_spec"),
                label="partial recovery AI_SLICE RunSpec",
            )
            feature_fingerprint = _phase1a_sha256(
                source.get("feature_run_fingerprint"),
                label="partial recovery FEATURE fingerprint",
            )
            ai_fingerprint = _phase1a_sha256(
                source.get("ai_run_fingerprint"),
                label="partial recovery AI_SLICE fingerprint",
            )
            if (
                canonical_sha256(feature_spec) != feature_fingerprint
                or canonical_sha256(ai_spec) != ai_fingerprint
            ):
                raise ResearchRegistryDriftError(
                    "partial recovery source canonical RunSpec fingerprint drift"
                )
            ai_parameters = _phase1a_json_object(
                ai_spec.get("parameters"),
                label="partial recovery AI_SLICE parameters",
            )
            if ai_parameters.get("parent_run_fingerprint") != feature_fingerprint:
                raise ResearchRegistryDriftError(
                    "partial recovery AI_SLICE parent fingerprint drift"
                )
            artifact_sha256 = _phase1a_sha256(
                source.get("artifact_sha256"),
                label="partial recovery Discovery artifact",
            )
            artifact_size = source.get("artifact_byte_size")
            if (
                isinstance(artifact_size, bool)
                or not isinstance(artifact_size, int)
                or artifact_size <= 0
            ):
                raise ResearchRegistryDriftError(
                    "partial recovery Discovery artifact byte size is invalid"
                )
            _verify_phase1a_artifact_file(source)

            query_exposure_ids = list(prefix.query_exposure_ids)
            query_rows: list[Mapping[str, Any]] = []
            if query_exposure_ids:
                query_rows = connection.execute(
                    """
                    SELECT exposure.discovery_exposure_id,
                           exposure.result_artifact_id,
                           exposure.query_spec,
                           run_spec.research_run_spec_id,
                           run_spec.parent_run_spec_id,
                           run_spec.run_fingerprint,
                           run_spec.run_kind,
                           run_spec.canonical_spec,
                           attempt.research_run_attempt_id AS success_attempt_id,
                           attempt.result_artifact_id AS success_artifact_id
                    FROM systematic_fx.discovery_exposures AS exposure
                    JOIN systematic_fx.research_run_specs AS run_spec
                      ON run_spec.research_run_spec_id = exposure.research_run_spec_id
                     AND run_spec.campaign_id = exposure.campaign_id
                    JOIN systematic_fx.research_run_attempts AS attempt
                      ON attempt.research_run_spec_id = run_spec.research_run_spec_id
                     AND attempt.status = 'SUCCEEDED'
                    WHERE exposure.discovery_exposure_id = ANY(%s::bigint[])
                      AND exposure.exposure_type = 'QUERY'
                    ORDER BY array_position(%s::bigint[], exposure.discovery_exposure_id)
                    FOR SHARE OF exposure, run_spec, attempt
                    """,
                    (query_exposure_ids, query_exposure_ids),
                ).fetchall()
            if [int(row["discovery_exposure_id"]) for row in query_rows] != query_exposure_ids:
                raise ResearchRegistryDriftError(
                    "partial recovery QUERY prefix changed while it was loaded"
                )

            query_sources: list[Phase1ARecoveryQuerySource] = []
            for row in query_rows:
                run_spec = _phase1a_json_object(
                    row.get("canonical_spec"),
                    label="partial recovery QUERY RunSpec",
                )
                fingerprint = _phase1a_sha256(
                    row.get("run_fingerprint"),
                    label="partial recovery QUERY fingerprint",
                )
                parameters = _phase1a_json_object(
                    run_spec.get("parameters"),
                    label="partial recovery QUERY parameters",
                )
                candidate = _phase1a_json_object(
                    parameters.get("candidate_query"),
                    label="partial recovery QUERY candidate",
                )
                query_id = candidate.get("id")
                if not isinstance(query_id, str) or not query_id:
                    raise ResearchRegistryDriftError("partial recovery QUERY ID is invalid")
                _assert_fields(
                    label=f"partial recovery QUERY {query_id}",
                    row=row,
                    expected={
                        "parent_run_spec_id": int(source["ai_run_spec_id"]),
                        "result_artifact_id": prefix.result_artifact_id,
                        "run_kind": "QUERY",
                        "success_artifact_id": prefix.result_artifact_id,
                    },
                )
                if (
                    canonical_sha256(run_spec) != fingerprint
                    or parameters.get("parent_run_fingerprint") != ai_fingerprint
                    or parameters.get("discovery_artifact_sha256") != artifact_sha256
                ):
                    raise ResearchRegistryDriftError(
                        f"partial recovery QUERY {query_id} canonical lineage drift"
                    )
                query_sources.append(
                    Phase1ARecoveryQuerySource(
                        query_id=query_id,
                        research_run_spec_id=int(row["research_run_spec_id"]),
                        run_fingerprint=fingerprint,
                        success_attempt_id=int(row["success_attempt_id"]),
                        discovery_exposure_id=int(row["discovery_exposure_id"]),
                        canonical_spec=run_spec,
                    )
                )

    return Phase1APartialRecoverySource(
        campaign_id=int(source["campaign_id"]),
        slice_index=prefix.slice_index,
        feature_run_spec_id=int(source["feature_run_spec_id"]),
        feature_run_fingerprint=feature_fingerprint,
        feature_success_attempt_id=int(source["feature_success_attempt_id"]),
        feature_result_artifact_id=int(source["feature_result_artifact_id"]),
        feature_canonical_spec=feature_spec,
        ai_run_spec_id=int(source["ai_run_spec_id"]),
        ai_run_fingerprint=ai_fingerprint,
        ai_success_attempt_id=int(source["ai_success_attempt_id"]),
        ai_exposure_id=int(source["ai_exposure_id"]),
        ai_canonical_spec=ai_spec,
        query_prefix=tuple(query_sources),
        pattern_ids=prefix.pattern_ids,
        missing_pattern_query_id=prefix.missing_pattern_query_id,
        result_artifact_id=int(source["result_artifact_id"]),
        result_artifact_uri=str(source["artifact_uri"]),
        result_artifact_sha256=artifact_sha256,
        result_artifact_byte_size=artifact_size,
    )
