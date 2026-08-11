"""Database governance for conditional bar-state Discovery v2.

PostgreSQL is the compact control plane.  Feature, label, model, OOS-trade,
global, and terminal bytes stay in content-addressed files below ``data/``;
this module stores their exact hashes and immutable RunSpec/attempt/trial
edges.  No API in this module can register walk-forward or holdout evidence.
"""

from __future__ import annotations

import json
import os
import re
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from functools import wraps
from pathlib import Path
from typing import Any, Final, Literal, ParamSpec, TypeVar

import psycopg
from psycopg import IsolationLevel
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.db.bar_registry import (
    _artifact_from_database_row,
    _ensure_artifact,
    _ensure_campaign_days,
    _ensure_splits,
)
from systematic_fx.db.postgres_retry import retry_serialization_failures
from systematic_fx.db.run_registry import (
    RunAttemptReservation,
    RunAttemptState,
    RunSpecRegistration,
    register_run_spec,
)
from systematic_fx.research.bar_artifacts import (
    PublishedBarArtifact,
    open_verified_bar_artifact,
)
from systematic_fx.research.bar_state_artifacts import (
    BAR_STATE_ARTIFACT_SCHEMA_BY_KIND,
    BAR_STATE_BAR_DATASET_MANIFEST_SHA256,
    BAR_STATE_RAW_SOURCE_MANIFEST_SHA256,
    BAR_STATE_SPLIT_PLAN_SHA256,
    BarStateArtifactError,
    BarStateArtifactKind,
    bar_state_candidate_selection_projection,
    bar_state_global_result_projection,
    bar_state_lineage_matches_profile,
    bar_state_model_package_projection,
    bar_state_terminal_compact_summary,
    frozen_bar_state_discovery_scope,
    load_verified_bar_state_json,
)
from systematic_fx.research.bar_state_config import (
    BAR_STATE_V2_PROFILE,
    BAR_STATE_V2A_PROFILE,
    BAR_STATE_V2B_PROFILE,
    BarStateCampaignProfile,
    require_bar_state_campaign_profile,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.run_spec import RunSpec
from systematic_fx.validation.bar_splits import BarSplitPlan
from systematic_fx.validation.bar_state_splits import BAR_STATE_FROZEN_SPLIT_SHA256

BAR_STATE_CANDIDATE_COUNT: Final = 12
BAR_STATE_FINALIST_BUDGET: Final = 4
BAR_STATE_DATASET_KEY: Final = "glbx_mdp3_mbp_10_6e_fut_v1"
BAR_STATE_CAMPAIGN_KEY: Final = BAR_STATE_V2_PROFILE.campaign_key
BAR_STATE_ARTIFACT_TYPE: Final = BAR_STATE_V2_PROFILE.artifact_type
BAR_STATE_EXPERIMENT_KEY: Final = BAR_STATE_V2_PROFILE.experiment_key
BAR_STATE_REGISTRATION_SCHEMA: Final = BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["REGISTRATION"]
BAR_STATE_CAMPAIGN_DEFINITION_SCHEMA: Final = "systematic_fx.bar_state_campaign_definition.v1"
BAR_STATE_TRIAL_PARAMETERS_SCHEMA: Final = "systematic_fx.bar_state_trial_parameters.v1"
BAR_STATE_TERMINAL_SUMMARY_SCHEMA: Final = "systematic_fx.bar_state_terminal_summary.v1"
BAR_STATE_ENGINE_VERSION: Final = BAR_STATE_V2_PROFILE.engine_version
BAR_STATE_FEATURE_VERSION: Final = "bar_state_features_v1"
BAR_STATE_OUTCOME_VERSION: Final = "bar_state_twenty_day_first_touch_labels_v1"
BAR_STATE_COST_VERSION: Final = "BAR_TRADE_ONLY_COSTS_V1"
BAR_STATE_EXECUTION_VERSION: Final = "bar_state_next_open_49_cell_replay_v1"
BAR_STATE_SPLIT_VERSION: Final = "bar_state_discovery_inner_oos_v1"
BAR_STATE_ELIGIBLE_CALENDAR_VERSION: Final = "bar_dataset_eligible_calendar_v1"
BAR_STATE_ELIGIBLE_CALENDAR_SHA256: Final = (
    "a8b57ad2ffcb68accc0e792c08082cf51090b87bf963800178f88dd27af9da14"
)
BAR_STATE_RANDOM_SEED: Final = 20_260_809
BAR_STATE_CANDIDATE_CATALOG_SHA256: Final = BAR_STATE_V2_PROFILE.candidate_catalog_sha256
BAR_STATE_CAMPAIGN_DEFINITION_SHA256: Final = BAR_STATE_V2_PROFILE.campaign_definition_sha256

BarStateArtifactRole = Literal[
    "FEATURE",
    "LABEL",
    "MODEL",
    "OOS_TRADE",
    "GLOBAL_RESULT",
    "TERMINAL_RESULT",
]
BarStateTrialStatus = Literal["SUCCEEDED", "REJECTED"]

_ARTIFACT_ROLE_TO_KIND: Final[dict[BarStateArtifactRole, BarStateArtifactKind]] = {
    "FEATURE": "FEATURE",
    "LABEL": "LABEL",
    "MODEL": "MODEL",
    "OOS_TRADE": "OOS_TRADE",
    "GLOBAL_RESULT": "GLOBAL_RESULT",
    "TERMINAL_RESULT": "TERMINAL_RESULT",
}
_SHA256 = re.compile(r"[0-9a-f]{64}")
_GIT_OBJECT_ID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")
_CANDIDATE_KEY = re.compile(r"bsv2_tf(?:0300|1800)_fs(?:morphology|state)_cm(?:005|010|015)")
_DISCOVERY_SPLIT_KEYS = frozenset(
    {"discovery", "discovery_inner_1", "discovery_inner_2", "discovery_inner_3"}
)
_P = ParamSpec("_P")
_R = TypeVar("_R")


class BarStateRegistryError(RuntimeError):
    """A v2 registry request is invalid or incomplete."""


class BarStateRegistryDriftError(BarStateRegistryError):
    """Existing database state differs from an immutable request."""


class BarStateRegistryStateError(BarStateRegistryError):
    """A requested lifecycle transition is not currently allowed."""


class BarStateRegistryDatabaseError(BarStateRegistryError):
    """PostgreSQL could not complete a v2 registry operation."""


def _translate_psycopg_errors(
    operation: str,
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    def decorate(function: Callable[_P, _R]) -> Callable[_P, _R]:
        @wraps(function)
        def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            try:
                return retry_serialization_failures(function, *args, **kwargs)
            except BarStateRegistryError:
                raise
            except psycopg.Error as error:
                raise BarStateRegistryDatabaseError(f"{operation} failed") from error

        return wrapped

    return decorate


def _set_serializable(connection: psycopg.Connection[dict[str, Any]]) -> None:
    connection.set_isolation_level(IsolationLevel.SERIALIZABLE)


def _nonempty(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise BarStateRegistryError(f"{label} must be a canonical non-empty string")
    return value


def _sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise BarStateRegistryError(f"{label} must be a lowercase SHA-256")
    return value


def _positive_integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise BarStateRegistryError(f"{label} must be a positive integer")
    return value


def _canonical_mapping(value: object, *, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise BarStateRegistryError(f"{label} must be a mapping")
    try:
        encoded = canonical_json_bytes(value)
    except (TypeError, ValueError) as error:
        raise BarStateRegistryError(f"{label} must be strict canonical JSON") from error
    decoded = json.loads(encoded)
    if not isinstance(decoded, dict):  # pragma: no cover
        raise BarStateRegistryError(f"{label} must encode an object")
    return decoded


def _assert_fields(
    *,
    label: str,
    row: Mapping[str, Any],
    expected: Mapping[str, object],
) -> None:
    for field, value in expected.items():
        if row.get(field) != value:
            raise BarStateRegistryDriftError(f"{label} field {field!r} drifted")


def _row_or_error(row: Mapping[str, Any] | None, *, label: str) -> Mapping[str, Any]:
    if row is None:
        raise BarStateRegistryStateError(f"{label} does not exist")
    return row


def _candidate_key(value: object) -> str:
    if not isinstance(value, str) or _CANDIDATE_KEY.fullmatch(value) is None:
        raise BarStateRegistryError("candidate_key is outside the frozen 12-candidate catalog")
    return value


@dataclass(frozen=True, slots=True)
class BarStateRegistryDefinition:
    """Config-independent, canonical handoff from the v2 planning layer."""

    config_file_sha256: str
    config_semantic_sha256: str
    campaign_definition: Mapping[str, object]
    campaign_definition_sha256: str
    candidate_catalog_sha256: str
    training_plan: Mapping[str, object]
    training_plan_sha256: str
    candidates: tuple[Mapping[str, object], ...]
    profile: BarStateCampaignProfile = BAR_STATE_V2_PROFILE

    def __post_init__(self) -> None:
        profile = require_bar_state_campaign_profile(self.profile)
        for field in (
            "config_file_sha256",
            "config_semantic_sha256",
            "campaign_definition_sha256",
            "candidate_catalog_sha256",
            "training_plan_sha256",
        ):
            _sha256(getattr(self, field), label=field)
        approved_identities = {
            "campaign_definition_sha256": profile.campaign_definition_sha256,
            "candidate_catalog_sha256": profile.candidate_catalog_sha256,
            "config_file_sha256": profile.config_file_sha256,
            "config_semantic_sha256": profile.config_semantic_sha256,
            "training_plan_sha256": BAR_STATE_FROZEN_SPLIT_SHA256,
        }
        for field, expected in approved_identities.items():
            if getattr(self, field) != expected:
                raise BarStateRegistryError(
                    f"{field} differs from the approved {profile.version_id.lower()} "
                    "preregistration"
                )
        campaign = _canonical_mapping(self.campaign_definition, label="campaign_definition")
        training = _canonical_mapping(self.training_plan, label="training_plan")
        if canonical_sha256(campaign) != self.campaign_definition_sha256:
            raise BarStateRegistryError("campaign_definition_sha256 mismatch")
        if canonical_sha256(training) != self.training_plan_sha256:
            raise BarStateRegistryError("training_plan_sha256 mismatch")
        if len(self.candidates) != BAR_STATE_CANDIDATE_COUNT:
            raise BarStateRegistryError("bar-state v2 requires exactly 12 candidates")
        canonical_candidates = tuple(
            _canonical_mapping(item, label=f"candidates[{index}]")
            for index, item in enumerate(self.candidates)
        )
        keys = tuple(_candidate_key(item.get("candidate_key")) for item in canonical_candidates)
        if keys != tuple(sorted(set(keys))):
            raise BarStateRegistryError("candidate definitions must be unique and key-sorted")
        for item in canonical_candidates:
            definition_sha = item.get("definition_sha256")
            if definition_sha is not None:
                raise BarStateRegistryError(
                    "candidate as_dict must not embed definition_sha256; it is derived"
                )
        if canonical_sha256(list(canonical_candidates)) != self.candidate_catalog_sha256:
            raise BarStateRegistryError("candidate_catalog_sha256 mismatch")
        object.__setattr__(self, "campaign_definition", campaign)
        object.__setattr__(self, "training_plan", training)
        object.__setattr__(self, "candidates", canonical_candidates)
        object.__setattr__(self, "profile", profile)

    @property
    def candidate_keys(self) -> tuple[str, ...]:
        return tuple(str(item["candidate_key"]) for item in self.candidates)

    def candidate(self, candidate_key: str) -> Mapping[str, object]:
        key = _candidate_key(candidate_key)
        matches = tuple(item for item in self.candidates if item["candidate_key"] == key)
        if len(matches) != 1:  # pragma: no cover - protected at construction
            raise BarStateRegistryError(f"unknown candidate {key}")
        return matches[0]


@dataclass(frozen=True, slots=True)
class BarStateCampaignRegistrationReport:
    dataset_id: int
    campaign_id: int
    experiment_id: int
    registration_artifact_id: int
    candidate_trial_ids: tuple[int, ...]
    created_campaign: bool
    created_experiment: bool
    created_trials: int


@dataclass(frozen=True, slots=True)
class BarStatePredecessorGateReport:
    """Metadata-only proof that an amendment follows its exact failed predecessor."""

    predecessor_campaign_id: int
    predecessor_experiment_id: int
    candidate_count: int
    failed_attempt_count: int
    linked_artifact_count: int


@dataclass(frozen=True, slots=True)
class BarStateArtifactLinkReport:
    bar_state_artifact_link_id: int
    artifact_id: int
    research_run_attempt_id: int
    artifact_role: BarStateArtifactRole
    shard_ordinal: int
    created: bool


@dataclass(frozen=True, slots=True)
class BarStateTerminalReport:
    candidate_key: str
    research_run_attempt_id: int
    research_run_spec_id: int
    trial_status: BarStateTrialStatus
    result_artifact_id: int


@dataclass(frozen=True, slots=True)
class BarStateReuseValidationReport:
    research_run_attempt_id: int
    reused_attempt_id: int
    candidate_key: str
    artifact_count: int
    role_counts: tuple[tuple[str, int], ...]
    artifacts: tuple[BarStateReusedArtifactEvidence, ...]
    artifact_link_manifest_sha256: str
    compact_summary: Mapping[str, object]
    candidate_evidence_slice_sha256: str
    candidate_selection_sha256: str
    candidate_selection_projection_sha256: str
    decision_label: str
    finalist_model_binding_sha256: str
    global_evidence_projection_sha256: str
    model_package_projection_sha256: str
    trial_status: BarStateTrialStatus
    global_artifact_identity_sha256: str
    global_artifact_sha256: str
    global_document_sha256: str
    finalist_keys: tuple[str, ...]
    terminal_artifact_sha256: str


@dataclass(frozen=True, slots=True)
class BarStateReusedArtifactEvidence:
    artifact_role: BarStateArtifactRole
    split_key: str
    shard_ordinal: int
    lineage_sha256: str
    artifact: PublishedBarArtifact


def candidate_trial_parameters(
    definition: BarStateRegistryDefinition,
    candidate_key: str,
    *,
    split_plan: BarSplitPlan,
) -> dict[str, object]:
    """Return the exact append-preserved parameter document for one candidate."""

    if split_plan.sha256 != BAR_STATE_SPLIT_PLAN_SHA256:
        raise BarStateRegistryError("split_plan differs from the frozen outer split")
    candidate = definition.candidate(candidate_key)
    candidate_definition_sha256 = canonical_sha256(candidate)
    return {
        "bar_dataset_manifest_sha256": BAR_STATE_BAR_DATASET_MANIFEST_SHA256,
        "campaign_definition": dict(definition.campaign_definition),
        "campaign_definition_sha256": definition.campaign_definition_sha256,
        "candidate_catalog_sha256": definition.candidate_catalog_sha256,
        "candidate_definition": dict(candidate),
        "candidate_definition_sha256": candidate_definition_sha256,
        "candidate_key": candidate_key,
        "config_file_sha256": definition.config_file_sha256,
        "config_semantic_sha256": definition.config_semantic_sha256,
        "discovery_scope": frozen_bar_state_discovery_scope().as_dict(),
        "discovery_scope_sha256": frozen_bar_state_discovery_scope().sha256,
        "raw_source_manifest_sha256": BAR_STATE_RAW_SOURCE_MANIFEST_SHA256,
        "schema": BAR_STATE_TRIAL_PARAMETERS_SCHEMA,
        "split_plan": split_plan.as_dict(),
        "split_plan_sha256": split_plan.sha256,
        "training_plan": dict(definition.training_plan),
        "training_plan_sha256": definition.training_plan_sha256,
    }


def build_bar_state_registration_document(
    definition: BarStateRegistryDefinition,
    *,
    split_plan: BarSplitPlan,
    code_commit: str,
    code_snapshot_sha256: str,
    dependency_lock_sha256: str,
    runtime_environment: Mapping[str, object],
    ordered_run_fingerprints: Sequence[str],
) -> dict[str, object]:
    """Build the catalog document that must exist before any outcome is computed."""

    commit = _nonempty(code_commit, label="code_commit")
    if _GIT_OBJECT_ID.fullmatch(commit) is None:
        raise BarStateRegistryError("code_commit must be a full lowercase Git object ID")
    snapshot = _sha256(code_snapshot_sha256, label="code_snapshot_sha256")
    dependency = _sha256(dependency_lock_sha256, label="dependency_lock_sha256")
    runtime = _canonical_mapping(runtime_environment, label="runtime_environment")
    fingerprints = tuple(
        _sha256(item, label="ordered_run_fingerprints item") for item in ordered_run_fingerprints
    )
    if len(fingerprints) != BAR_STATE_CANDIDATE_COUNT or len(set(fingerprints)) != len(
        fingerprints
    ):
        raise BarStateRegistryError("registration requires 12 unique run fingerprints")
    if split_plan.sha256 != BAR_STATE_SPLIT_PLAN_SHA256:
        raise BarStateRegistryError("registration cannot use a non-frozen split")
    return {
        "bar_dataset_manifest_sha256": BAR_STATE_BAR_DATASET_MANIFEST_SHA256,
        "campaign_definition": dict(definition.campaign_definition),
        "campaign_definition_sha256": definition.campaign_definition_sha256,
        "candidate_catalog": [dict(item) for item in definition.candidates],
        "candidate_catalog_sha256": definition.candidate_catalog_sha256,
        "code_commit": commit,
        "code_snapshot_sha256": snapshot,
        "config_file_sha256": definition.config_file_sha256,
        "config_semantic_sha256": definition.config_semantic_sha256,
        "dependency_lock_sha256": dependency,
        "discovery_scope": frozen_bar_state_discovery_scope().as_dict(),
        "ordered_run_fingerprints": list(fingerprints),
        "ordered_run_set_sha256": canonical_sha256(list(fingerprints)),
        "raw_source_manifest_sha256": BAR_STATE_RAW_SOURCE_MANIFEST_SHA256,
        "runtime_environment": runtime,
        "runtime_environment_sha256": canonical_sha256(runtime),
        "schema": BAR_STATE_REGISTRATION_SCHEMA,
        "split_plan": split_plan.as_dict(),
        "split_plan_sha256": split_plan.sha256,
        "training_plan": dict(definition.training_plan),
        "training_plan_sha256": definition.training_plan_sha256,
    }


def _read_json_artifact(
    held_descriptor: int,
    *,
    maximum_bytes: int = 4 * 1024 * 1024,
) -> dict[str, object]:
    details = os.fstat(held_descriptor)
    if details.st_size > maximum_bytes:
        raise BarStateRegistryError("JSON artifact exceeds the registry validation limit")
    os.lseek(held_descriptor, 0, os.SEEK_SET)
    payload = os.read(held_descriptor, maximum_bytes + 1)
    if len(payload) != details.st_size:
        raise BarStateRegistryDriftError("JSON artifact changed while held open")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BarStateRegistryError("JSON artifact is not strict JSON") from error
    return _canonical_mapping(value, label="artifact document")


def _campaign_split_policy(
    definition: BarStateRegistryDefinition,
    split_plan: BarSplitPlan,
) -> dict[str, object]:
    return {
        "authorized_stage": "DISCOVERY_ONLY",
        "bar_dataset_manifest_sha256": BAR_STATE_BAR_DATASET_MANIFEST_SHA256,
        "discovery_scope": frozen_bar_state_discovery_scope().as_dict(),
        "raw_source_manifest_sha256": BAR_STATE_RAW_SOURCE_MANIFEST_SHA256,
        "split_plan": split_plan.as_dict(),
        "split_plan_sha256": split_plan.sha256,
        "training_plan": dict(definition.training_plan),
        "training_plan_sha256": definition.training_plan_sha256,
    }


def _experiment_documents(
    definition: BarStateRegistryDefinition,
) -> tuple[dict[str, object], dict[str, object], dict[str, object], dict[str, object], str]:
    first_candidate = definition.candidates[0]
    feature_versions = {
        "candidate_catalog_sha256": definition.candidate_catalog_sha256,
        "feature_policy_sha256s": sorted(
            {canonical_sha256(item["feature_policy"]) for item in definition.candidates}
        ),
        "feature_version": BAR_STATE_FEATURE_VERSION,
    }
    search_boundary = {
        "allocated_candidate_count": BAR_STATE_CANDIDATE_COUNT,
        "authorized_stage": "DISCOVERY_ONLY",
        "candidate_catalog_sha256": definition.candidate_catalog_sha256,
        "result_driven_additions_allowed": False,
        "split_plan_sha256": BAR_STATE_SPLIT_PLAN_SHA256,
        "training_plan_sha256": definition.training_plan_sha256,
    }
    cost_assumptions = _canonical_mapping(
        first_candidate.get("cost_model"),
        label="candidate cost model",
    )
    execution_assumptions = {
        "economic_barriers": first_candidate.get("economic_barrier_policy"),
        "entry": first_candidate.get("entry_policy"),
        "label_policy": first_candidate.get("label_policy"),
        "prediction_policy_sha256s": sorted(
            {canonical_sha256(item["prediction_policy"]) for item in definition.candidates}
        ),
    }
    experiment_config_sha256 = canonical_sha256(
        {
            "cost_assumptions": cost_assumptions,
            "execution_assumptions": execution_assumptions,
            "feature_versions": feature_versions,
            "search_boundary": search_boundary,
        }
    )
    return (
        feature_versions,
        search_boundary,
        cost_assumptions,
        execution_assumptions,
        experiment_config_sha256,
    )


def _require_clean_bar_state_predecessor_connection(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    successor_profile: BarStateCampaignProfile,
) -> BarStatePredecessorGateReport:
    successor = require_bar_state_campaign_profile(successor_profile)
    if successor == BAR_STATE_V2A_PROFILE:
        predecessor = BAR_STATE_V2_PROFILE
        expected_attempt_numbers = (1,)
        expected_gate_policy = "REQUIRE_EXACT_FAILED_PREDECESSOR_WITH_NO_OOS_EVIDENCE"
    elif successor == BAR_STATE_V2B_PROFILE:
        predecessor = BAR_STATE_V2A_PROFILE
        expected_attempt_numbers = (1, 2)
        expected_gate_policy = (
            "REQUIRE_EXACT_FAILED_PREDECESSOR_ATTEMPTS_1_AND_2_WITH_NO_GOVERNED_EVIDENCE"
        )
    else:
        raise BarStateRegistryError("campaign profile lacks an exact predecessor gate")
    if (
        successor.amends_campaign_key != predecessor.campaign_key
        or successor.predecessor_campaign_definition_sha256
        != predecessor.campaign_definition_sha256
        or successor.predecessor_code_commit is None
        or successor.predecessor_gate_policy != expected_gate_policy
    ):
        raise BarStateRegistryError("campaign profile lacks an exact predecessor gate")

    predecessor_label = f"{predecessor.version_id} predecessor"
    expected_attempt_count = BAR_STATE_CANDIDATE_COUNT * len(expected_attempt_numbers)

    identity = connection.execute(
        """
        SELECT c.campaign_id, c.campaign_key, c.name, c.status, c.data_manifest_sha256,
               c.feature_version, c.outcome_version, c.cost_model_version,
               c.execution_model_version, c.code_commit, c.config_sha256,
               c.trial_budget, c.finalist_budget, c.frozen_at,
               c.holdout_revealed_at, c.closed_at,
               d.dataset_key, d.manifest_sha256 AS raw_manifest_sha256,
               d.status AS dataset_status,
               e.experiment_id, e.experiment_key, e.status AS experiment_status,
               e.primary_family, e.model_family, e.direction,
               e.trial_budget AS experiment_trial_budget,
               e.trials_registered, e.frozen_at AS experiment_frozen_at,
               e.completed_at,
               registration.artifact_type AS registration_artifact_type,
               registration.metadata #>> '{artifact_schema}' AS registration_schema,
               registration.metadata #>> '{logical_identity,artifact_kind}'
                   AS registration_kind,
               registration.metadata #>> '{logical_identity,campaign_key}'
                   AS registration_campaign_key
        FROM systematic_fx.campaigns AS c
        JOIN systematic_fx.datasets AS d ON d.dataset_id = c.dataset_id
        JOIN systematic_fx.experiments AS e ON e.campaign_id = c.campaign_id
        JOIN systematic_fx.artifacts AS registration
          ON registration.artifact_id = e.registration_artifact_id
        WHERE c.campaign_key = %s AND e.experiment_key = %s
          AND (
              SELECT count(*)
              FROM systematic_fx.experiments AS campaign_experiment
              WHERE campaign_experiment.campaign_id = c.campaign_id
          ) = 1
        FOR SHARE OF c, e
        """,
        (predecessor.campaign_key, predecessor.experiment_key),
    ).fetchone()
    identity = _row_or_error(identity, label=f"{predecessor_label} campaign")
    _assert_fields(
        label=f"{predecessor_label} campaign",
        row=identity,
        expected={
            "campaign_key": predecessor.campaign_key,
            "name": predecessor.campaign_name,
            "status": "FROZEN",
            "data_manifest_sha256": BAR_STATE_BAR_DATASET_MANIFEST_SHA256,
            "feature_version": BAR_STATE_FEATURE_VERSION,
            "outcome_version": BAR_STATE_OUTCOME_VERSION,
            "cost_model_version": BAR_STATE_COST_VERSION,
            "execution_model_version": BAR_STATE_EXECUTION_VERSION,
            "code_commit": successor.predecessor_code_commit,
            "config_sha256": successor.predecessor_campaign_definition_sha256,
            "trial_budget": BAR_STATE_CANDIDATE_COUNT,
            "finalist_budget": BAR_STATE_FINALIST_BUDGET,
            "holdout_revealed_at": None,
            "closed_at": None,
            "dataset_key": BAR_STATE_DATASET_KEY,
            "raw_manifest_sha256": BAR_STATE_RAW_SOURCE_MANIFEST_SHA256,
            "experiment_key": predecessor.experiment_key,
            "experiment_status": "FROZEN",
            "primary_family": "CONDITIONAL_BAR_STATE_MODEL",
            "model_family": "ELASTIC_NET_MULTINOMIAL_LOGISTIC",
            "direction": "BOTH",
            "experiment_trial_budget": BAR_STATE_CANDIDATE_COUNT,
            "trials_registered": BAR_STATE_CANDIDATE_COUNT,
            "completed_at": None,
            "registration_artifact_type": predecessor.artifact_type,
            "registration_schema": BAR_STATE_REGISTRATION_SCHEMA,
            "registration_kind": "REGISTRATION",
            "registration_campaign_key": predecessor.campaign_key,
        },
    )
    if (
        identity["frozen_at"] is None
        or identity["experiment_frozen_at"] is None
        or identity["dataset_status"] in {"REJECTED", "RETIRED"}
    ):
        raise BarStateRegistryStateError(
            f"{predecessor_label} is not frozen and available"
        )

    experiment_id = int(identity["experiment_id"])
    catalog = connection.execute(
        """
        SELECT count(*)::integer AS trial_count,
               count(*) FILTER (WHERE t.status = 'REGISTERED')::integer
                   AS registered_count,
               count(*) FILTER (WHERE t.research_run_spec_id IS NOT NULL)::integer
                   AS bound_count,
               count(*) FILTER (
                   WHERE t.research_run_spec_id IS NULL
                      OR NOT systematic_fx.bar_state_run_spec_matches_trial(
                          t.research_run_spec_id, t.experiment_trial_id
                      )
               )::integer AS invalid_binding_count,
               count(DISTINCT t.trial_key)::integer AS distinct_candidate_count,
               count(DISTINCT r.research_run_spec_id)::integer AS distinct_spec_count,
               (
                   SELECT count(*)::integer
                   FROM systematic_fx.research_run_specs AS all_specs
                   WHERE all_specs.experiment_id = %s
               ) AS total_experiment_spec_count
        FROM systematic_fx.experiment_trials AS t
        LEFT JOIN systematic_fx.research_run_specs AS r
          ON r.research_run_spec_id = t.research_run_spec_id
        WHERE t.experiment_id = %s
        """,
        (experiment_id, experiment_id),
    ).fetchone()
    catalog = _row_or_error(catalog, label=f"{predecessor_label} candidate catalog")
    _assert_fields(
        label=f"{predecessor_label} candidate catalog",
        row=catalog,
        expected={
            "trial_count": BAR_STATE_CANDIDATE_COUNT,
            "registered_count": BAR_STATE_CANDIDATE_COUNT,
            "bound_count": BAR_STATE_CANDIDATE_COUNT,
            "invalid_binding_count": 0,
            "distinct_candidate_count": BAR_STATE_CANDIDATE_COUNT,
            "distinct_spec_count": BAR_STATE_CANDIDATE_COUNT,
            "total_experiment_spec_count": BAR_STATE_CANDIDATE_COUNT,
        },
    )

    attempts = connection.execute(
        """
        SELECT count(*)::integer AS attempt_count,
               count(*) FILTER (WHERE a.status = 'FAILED')::integer AS failed_count,
               count(*) FILTER (
                   WHERE a.attempt_number = ANY(%s::integer[])
                     AND a.result_artifact_id IS NULL
                     AND a.trade_ledger_artifact_id IS NULL
                     AND a.reused_attempt_id IS NULL
                     AND a.started_at IS NOT NULL
                     AND a.finished_at IS NOT NULL
                     AND btrim(COALESCE(a.error_message, '')) <> ''
                     AND a.result_summary = jsonb_build_object(
                         'candidate_key',
                             r.canonical_spec #>>
                                 '{parameters,bar_state_candidate_key}',
                         'run_fingerprint', r.run_fingerprint
                     )
               )::integer AS exact_failed_count,
               count(DISTINCT a.research_run_spec_id)::integer AS distinct_spec_count,
               count(DISTINCT (a.research_run_spec_id, a.attempt_number))::integer
                   AS distinct_spec_attempt_count
        FROM systematic_fx.research_run_attempts AS a
        JOIN systematic_fx.research_run_specs AS r
          ON r.research_run_spec_id = a.research_run_spec_id
        WHERE r.campaign_id = %s AND r.experiment_id = %s
        """,
        (list(expected_attempt_numbers), identity["campaign_id"], experiment_id),
    ).fetchone()
    attempts = _row_or_error(attempts, label=f"{predecessor_label} failed attempts")
    _assert_fields(
        label=f"{predecessor_label} failed attempts",
        row=attempts,
        expected={
            "attempt_count": expected_attempt_count,
            "failed_count": expected_attempt_count,
            "exact_failed_count": expected_attempt_count,
            "distinct_spec_count": BAR_STATE_CANDIDATE_COUNT,
            "distinct_spec_attempt_count": expected_attempt_count,
        },
    )

    link_row = connection.execute(
        """
        SELECT count(*)::integer AS linked_artifact_count
        FROM systematic_fx.bar_state_artifact_links
        WHERE campaign_id = %s
        """,
        (identity["campaign_id"],),
    ).fetchone()
    link_row = _row_or_error(link_row, label=f"{predecessor_label} artifact links")
    if link_row["linked_artifact_count"] != 0:
        raise BarStateRegistryStateError(
            f"{predecessor_label} has linked governed evidence"
        )

    return BarStatePredecessorGateReport(
        predecessor_campaign_id=int(identity["campaign_id"]),
        predecessor_experiment_id=experiment_id,
        candidate_count=BAR_STATE_CANDIDATE_COUNT,
        failed_attempt_count=expected_attempt_count,
        linked_artifact_count=0,
    )


@_translate_psycopg_errors("bar-state predecessor gate")
def require_clean_bar_state_predecessor(
    database_url: str,
    *,
    successor_profile: BarStateCampaignProfile = BAR_STATE_V2A_PROFILE,
) -> BarStatePredecessorGateReport:
    """Prove one amendment follows only its exact failed governed predecessor."""

    url = _nonempty(database_url, label="database_url")
    selected = require_bar_state_campaign_profile(successor_profile)
    with psycopg.connect(url, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with connection.transaction():
            return _require_clean_bar_state_predecessor_connection(
                connection,
                successor_profile=selected,
            )


@_translate_psycopg_errors("bar-state campaign registration")
def register_bar_state_campaign(
    database_url: str,
    project_root: Path,
    *,
    definition: BarStateRegistryDefinition,
    split_plan: BarSplitPlan,
    code_commit: str,
    registration_artifact: PublishedBarArtifact,
    expected_registration_document: Mapping[str, object],
    dataset_key: str = BAR_STATE_DATASET_KEY,
) -> BarStateCampaignRegistrationReport:
    """Atomically freeze the campaign, split calendar, experiment, and 12 trials."""

    profile = require_bar_state_campaign_profile(definition.profile)
    url = _nonempty(database_url, label="database_url")
    commit = _nonempty(code_commit, label="code_commit")
    if _GIT_OBJECT_ID.fullmatch(commit) is None:
        raise BarStateRegistryError("code_commit must be a full lowercase Git object ID")
    if split_plan.sha256 != BAR_STATE_SPLIT_PLAN_SHA256:
        raise BarStateRegistryError("bar-state campaign requires the frozen split")
    if registration_artifact.descriptor.artifact_type != profile.artifact_type:
        raise BarStateRegistryError("registration artifact is outside the campaign root")
    logical = registration_artifact.descriptor.logical_identity
    registration_lineage = logical.get("lineage")
    if (
        logical.get("artifact_kind") != "REGISTRATION"
        or logical.get("campaign_key") != profile.campaign_key
        or not bar_state_lineage_matches_profile(registration_lineage, profile=profile)
        or registration_artifact.descriptor.artifact_schema
        != BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["REGISTRATION"]
    ):
        raise BarStateRegistryError("registration artifact has the wrong role")
    expected_document = _canonical_mapping(
        expected_registration_document,
        label="expected_registration_document",
    )
    split_policy = _campaign_split_policy(definition, split_plan)
    feature, search, costs, execution, experiment_config_sha = _experiment_documents(definition)

    with psycopg.connect(url, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with (
            connection.transaction(),
            open_verified_bar_artifact(project_root, registration_artifact) as held,
        ):
            if profile.amends_campaign_key is not None:
                _require_clean_bar_state_predecessor_connection(
                    connection,
                    successor_profile=profile,
                )
            if _read_json_artifact(held.descriptor) != expected_document:
                raise BarStateRegistryDriftError("registration artifact document drift")
            dataset = connection.execute(
                """
                SELECT dataset_id, dataset_key, manifest_sha256, status
                FROM systematic_fx.datasets
                WHERE dataset_key = %s
                FOR SHARE
                """,
                (dataset_key,),
            ).fetchone()
            dataset = _row_or_error(dataset, label=f"dataset {dataset_key}")
            _assert_fields(
                label=f"dataset {dataset_key}",
                row=dataset,
                expected={
                    "dataset_key": dataset_key,
                    "manifest_sha256": BAR_STATE_RAW_SOURCE_MANIFEST_SHA256,
                },
            )
            if dataset["status"] in {"REJECTED", "RETIRED"}:
                raise BarStateRegistryStateError("bar dataset is terminally unavailable")
            dataset_id = int(dataset["dataset_id"])
            registration_artifact_id, _ = _ensure_artifact(
                connection,
                registration_artifact,
            )
            inserted_campaign = connection.execute(
                """
                INSERT INTO systematic_fx.campaigns
                    (campaign_key, dataset_id, name, status, selected_start_date,
                     selected_end_date, roll_cutoff_date, data_manifest_sha256,
                     feature_version, outcome_version, cost_model_version,
                     execution_model_version, code_commit, config_sha256,
                     split_policy, trial_budget, finalist_budget, frozen_at)
                VALUES (%s, %s, %s, 'FROZEN', %s, %s, NULL, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, statement_timestamp())
                ON CONFLICT (campaign_key) DO NOTHING
                RETURNING campaign_id
                """,
                (
                    profile.campaign_key,
                    dataset_id,
                    profile.campaign_name,
                    split_plan.eligible_dates[0],
                    split_plan.eligible_dates[-1],
                    BAR_STATE_BAR_DATASET_MANIFEST_SHA256,
                    BAR_STATE_FEATURE_VERSION,
                    BAR_STATE_OUTCOME_VERSION,
                    BAR_STATE_COST_VERSION,
                    BAR_STATE_EXECUTION_VERSION,
                    commit,
                    definition.campaign_definition_sha256,
                    Jsonb(split_policy),
                    BAR_STATE_CANDIDATE_COUNT,
                    BAR_STATE_FINALIST_BUDGET,
                ),
            ).fetchone()
            campaign = connection.execute(
                """
                SELECT campaign_id, campaign_key, dataset_id, name, status,
                       selected_start_date, selected_end_date, roll_cutoff_date,
                       data_manifest_sha256, feature_version, outcome_version,
                       cost_model_version, execution_model_version, code_commit,
                       config_sha256, split_policy, trial_budget, finalist_budget,
                       frozen_at, holdout_revealed_at, closed_at
                FROM systematic_fx.campaigns
                WHERE campaign_key = %s
                FOR UPDATE
                """,
                (profile.campaign_key,),
            ).fetchone()
            campaign = _row_or_error(campaign, label="bar-state campaign")
            _assert_fields(
                label="bar-state campaign",
                row=campaign,
                expected={
                    "campaign_key": profile.campaign_key,
                    "dataset_id": dataset_id,
                    "name": profile.campaign_name,
                    "status": "FROZEN",
                    "selected_start_date": split_plan.eligible_dates[0],
                    "selected_end_date": split_plan.eligible_dates[-1],
                    "roll_cutoff_date": None,
                    "data_manifest_sha256": BAR_STATE_BAR_DATASET_MANIFEST_SHA256,
                    "feature_version": BAR_STATE_FEATURE_VERSION,
                    "outcome_version": BAR_STATE_OUTCOME_VERSION,
                    "cost_model_version": BAR_STATE_COST_VERSION,
                    "execution_model_version": BAR_STATE_EXECUTION_VERSION,
                    "code_commit": commit,
                    "config_sha256": definition.campaign_definition_sha256,
                    "split_policy": split_policy,
                    "trial_budget": BAR_STATE_CANDIDATE_COUNT,
                    "finalist_budget": BAR_STATE_FINALIST_BUDGET,
                    "holdout_revealed_at": None,
                    "closed_at": None,
                },
            )
            if campaign["frozen_at"] is None:
                raise BarStateRegistryDriftError("bar-state campaign is not frozen")
            campaign_id = int(campaign["campaign_id"])
            split_ids, _ = _ensure_splits(
                connection,
                campaign_id=campaign_id,
                ranges=split_plan.ranges,
            )
            _ensure_campaign_days(
                connection,
                dataset_id=dataset_id,
                campaign_id=campaign_id,
                split_plan=split_plan,
                split_ids=split_ids,
            )
            inserted_experiment = connection.execute(
                """
                INSERT INTO systematic_fx.experiments
                    (experiment_key, campaign_id, pattern_id, parent_experiment_id,
                     primary_family, status, hypothesis, direction, model_family,
                     tick_size, tick_value, feature_definition_versions,
                     search_boundary, cost_assumptions, execution_assumptions,
                     trial_budget, trials_registered, registration_artifact_id,
                     code_commit, config_sha256, frozen_at)
                VALUES (%s, %s, NULL, NULL, %s, 'FROZEN', %s, 'BOTH', %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        statement_timestamp())
                ON CONFLICT (experiment_key) DO NOTHING
                RETURNING experiment_id
                """,
                (
                    profile.experiment_key,
                    campaign_id,
                    "CONDITIONAL_BAR_STATE_MODEL",
                    "Completed candle state predicts next-open 20-day first-touch direction",
                    "ELASTIC_NET_MULTINOMIAL_LOGISTIC",
                    Decimal("0.00005"),
                    Decimal("6.25"),
                    Jsonb(feature),
                    Jsonb(search),
                    Jsonb(costs),
                    Jsonb(execution),
                    BAR_STATE_CANDIDATE_COUNT,
                    BAR_STATE_CANDIDATE_COUNT,
                    registration_artifact_id,
                    commit,
                    experiment_config_sha,
                ),
            ).fetchone()
            experiment = connection.execute(
                """
                SELECT experiment_id, experiment_key, campaign_id, pattern_id,
                       parent_experiment_id, primary_family, status, hypothesis,
                       direction, model_family, tick_size, tick_value,
                       feature_definition_versions, search_boundary,
                       cost_assumptions, execution_assumptions, trial_budget,
                       trials_registered, registration_artifact_id, code_commit,
                       config_sha256, frozen_at, completed_at
                FROM systematic_fx.experiments
                WHERE experiment_key = %s
                FOR UPDATE
                """,
                (profile.experiment_key,),
            ).fetchone()
            experiment = _row_or_error(experiment, label="bar-state experiment")
            _assert_fields(
                label="bar-state experiment",
                row=experiment,
                expected={
                    "experiment_key": profile.experiment_key,
                    "campaign_id": campaign_id,
                    "pattern_id": None,
                    "parent_experiment_id": None,
                    "primary_family": "CONDITIONAL_BAR_STATE_MODEL",
                    "status": "FROZEN",
                    "hypothesis": (
                        "Completed candle state predicts next-open 20-day first-touch direction"
                    ),
                    "direction": "BOTH",
                    "model_family": "ELASTIC_NET_MULTINOMIAL_LOGISTIC",
                    "tick_size": Decimal("0.00005"),
                    "tick_value": Decimal("6.25"),
                    "feature_definition_versions": feature,
                    "search_boundary": search,
                    "cost_assumptions": costs,
                    "execution_assumptions": execution,
                    "trial_budget": BAR_STATE_CANDIDATE_COUNT,
                    "trials_registered": BAR_STATE_CANDIDATE_COUNT,
                    "registration_artifact_id": registration_artifact_id,
                    "code_commit": commit,
                    "config_sha256": experiment_config_sha,
                    "completed_at": None,
                },
            )
            if experiment["frozen_at"] is None:
                raise BarStateRegistryDriftError("bar-state experiment is not frozen")
            experiment_id = int(experiment["experiment_id"])

            expected_trials = {
                key: candidate_trial_parameters(definition, key, split_plan=split_plan)
                for key in definition.candidate_keys
            }
            existing = connection.execute(
                """
                SELECT trial_key
                FROM systematic_fx.experiment_trials
                WHERE experiment_id = %s
                FOR SHARE
                """,
                (experiment_id,),
            ).fetchall()
            if not {str(row["trial_key"]) for row in existing} <= set(expected_trials):
                raise BarStateRegistryDriftError("catalog contains an unknown candidate trial")
            created_trials = 0
            for key in definition.candidate_keys:
                parameters = expected_trials[key]
                inserted = connection.execute(
                    """
                    INSERT INTO systematic_fx.experiment_trials
                        (experiment_id, trial_key, trial_type, status, parameters,
                         parameters_sha256, result_summary)
                    VALUES (%s, %s, 'MODEL_FIT', 'REGISTERED', %s, %s, '{}'::jsonb)
                    ON CONFLICT (experiment_id, trial_key) DO NOTHING
                    RETURNING experiment_trial_id
                    """,
                    (
                        experiment_id,
                        key,
                        Jsonb(parameters),
                        canonical_sha256(parameters),
                    ),
                ).fetchone()
                created_trials += inserted is not None
            rows = connection.execute(
                """
                SELECT experiment_trial_id, trial_key, trial_type, status,
                       parameters, parameters_sha256, research_run_spec_id
                FROM systematic_fx.experiment_trials
                WHERE experiment_id = %s
                ORDER BY trial_key
                FOR SHARE
                """,
                (experiment_id,),
            ).fetchall()
            if len(rows) != BAR_STATE_CANDIDATE_COUNT:
                raise BarStateRegistryDriftError("catalog must contain exactly 12 trials")
            trial_ids: list[int] = []
            for row in rows:
                key = str(row["trial_key"])
                parameters = expected_trials.get(key)
                if parameters is None:
                    raise BarStateRegistryDriftError("catalog contains an unknown trial")
                _assert_fields(
                    label=f"candidate trial {key}",
                    row=row,
                    expected={
                        "trial_key": key,
                        "trial_type": "MODEL_FIT",
                        "parameters": parameters,
                        "parameters_sha256": canonical_sha256(parameters),
                    },
                )
                if row["status"] not in {
                    "REGISTERED",
                    "RUNNING",
                    "SUCCEEDED",
                    "REJECTED",
                }:
                    raise BarStateRegistryStateError(f"candidate {key} has invalid status")
                trial_ids.append(int(row["experiment_trial_id"]))

    return BarStateCampaignRegistrationReport(
        dataset_id=dataset_id,
        campaign_id=campaign_id,
        experiment_id=experiment_id,
        registration_artifact_id=registration_artifact_id,
        candidate_trial_ids=tuple(trial_ids),
        created_campaign=inserted_campaign is not None,
        created_experiment=inserted_experiment is not None,
        created_trials=created_trials,
    )


def _validate_bar_state_run_spec(
    run_spec: RunSpec,
    *,
    definition: BarStateRegistryDefinition,
    split_plan: BarSplitPlan,
    candidate_key: str,
) -> dict[str, object]:
    profile = require_bar_state_campaign_profile(definition.profile)
    key = _candidate_key(candidate_key)
    if run_spec.campaign_id != profile.campaign_key:
        raise BarStateRegistryError("RunSpec belongs to a different campaign")
    if run_spec.experiment_id != profile.experiment_key:
        raise BarStateRegistryError("RunSpec belongs to a different experiment")
    if run_spec.run_kind != "MODEL_FIT" or run_spec.direction != "BOTH":
        raise BarStateRegistryError("bar-state candidate RunSpec must be BOTH MODEL_FIT")
    if run_spec.engine_version != profile.engine_version:
        raise BarStateRegistryError("bar-state engine version drift")
    if run_spec.split_sha256 != definition.training_plan_sha256:
        raise BarStateRegistryError("RunSpec nested training split drift")
    if (
        run_spec.eligible_calendar_version != BAR_STATE_ELIGIBLE_CALENDAR_VERSION
        or run_spec.eligible_calendar_sha256 != BAR_STATE_ELIGIBLE_CALENDAR_SHA256
    ):
        raise BarStateRegistryError("RunSpec eligible calendar drift")
    if dict(run_spec.source_manifest_hashes) != {
        "raw_mbp10_source_manifest_v1": BAR_STATE_RAW_SOURCE_MANIFEST_SHA256,
        "selected_trade_bar_dataset_manifest_v1": BAR_STATE_BAR_DATASET_MANIFEST_SHA256,
    }:
        raise BarStateRegistryError("RunSpec source manifest lineage drift")
    parameters = dict(run_spec.payload()["parameters"])
    trial_parameters = candidate_trial_parameters(
        definition,
        key,
        split_plan=split_plan,
    )
    expected_parameters = {
        "authorized_stage": "DISCOVERY_ONLY",
        "bar_state_candidate_key": key,
        "bar_state_candidate_definition_sha256": trial_parameters["candidate_definition_sha256"],
        "bar_state_candidate_catalog_sha256": definition.candidate_catalog_sha256,
        "bar_state_config_file_sha256": definition.config_file_sha256,
        "bar_state_config_semantic_sha256": definition.config_semantic_sha256,
        "bar_state_discovery_scope_sha256": frozen_bar_state_discovery_scope().sha256,
        "bar_state_training_plan_sha256": definition.training_plan_sha256,
        "bar_state_trial_parameters_sha256": canonical_sha256(trial_parameters),
    }
    if parameters != expected_parameters:
        raise BarStateRegistryError("RunSpec candidate parameters drift")
    if run_spec.feature_version != BAR_STATE_FEATURE_VERSION:
        raise BarStateRegistryError("RunSpec feature version drift")
    if run_spec.outcome_version != BAR_STATE_OUTCOME_VERSION:
        raise BarStateRegistryError("RunSpec outcome version drift")
    if run_spec.cost_version != BAR_STATE_COST_VERSION:
        raise BarStateRegistryError("RunSpec cost version drift")
    if run_spec.execution_version != BAR_STATE_EXECUTION_VERSION:
        raise BarStateRegistryError("RunSpec execution version drift")
    candidate = definition.candidate(key)
    feature = _canonical_mapping(candidate.get("feature_policy"), label="feature policy")
    label = _canonical_mapping(candidate.get("label_policy"), label="label policy")
    if (
        label.get("boundary_event_ordering")
        != "UNRESOLVED_AT_BOUNDARY_CENSORED_PRIOR_FIRST_TOUCH_PRESERVED"
    ):
        raise BarStateRegistryError("RunSpec boundary event ordering drift")
    cost = _canonical_mapping(candidate.get("cost_model"), label="cost model")
    entry = _canonical_mapping(candidate.get("entry_policy"), label="entry policy")
    barrier = _canonical_mapping(
        candidate.get("economic_barrier_policy"),
        label="economic barrier policy",
    )
    prediction = _canonical_mapping(
        candidate.get("prediction_policy"),
        label="prediction policy",
    )
    signal = {
        "authorized_stage": "DISCOVERY_ONLY",
        "candidate_key": key,
        "feature_policy": feature,
        "prediction_policy": prediction,
        "schema": "systematic_fx.bar_state_signal_policy.v1",
    }
    execution = {
        "economic_barrier_policy": barrier,
        "entry_policy": entry,
        "prediction_policy": prediction,
    }
    terminal = {
        "authorized_stage": "DISCOVERY_ONLY",
        "boundary_event_ordering": label["boundary_event_ordering"],
        "contract_boundary_policy": label["contract_boundary_policy"],
        "observation_horizon_active_days": label["observation_horizon_active_days"],
        "quality_boundary_policy": label["quality_boundary_policy"],
        "split_boundary_policy": "DISCOVERY_OUTCOME_TAIL_END",
    }
    expected_hashes = {
        "feature_sha256": canonical_sha256(feature),
        "outcome_sha256": canonical_sha256(label),
        "cost_sha256": canonical_sha256(cost),
        "execution_sha256": canonical_sha256(execution),
    }
    if any(getattr(run_spec, field) != value for field, value in expected_hashes.items()):
        raise BarStateRegistryError("RunSpec feature/outcome/cost/execution hash drift")
    expected_policies = {
        "signal_policy": signal,
        "entry_policy": entry,
        "barrier_policy": barrier,
        "terminal_policy": terminal,
    }
    if any(
        canonical_sha256(dict(getattr(run_spec, field))) != canonical_sha256(value)
        for field, value in expected_policies.items()
    ):
        raise BarStateRegistryError("RunSpec signal/entry/barrier/terminal policy drift")
    if run_spec.random_seed != BAR_STATE_RANDOM_SEED:
        raise BarStateRegistryError("RunSpec random seed drift")
    return trial_parameters


@_translate_psycopg_errors("bar-state RunSpec registration")
def register_bar_state_run_spec(
    database_url: str,
    run_spec: RunSpec,
    *,
    definition: BarStateRegistryDefinition,
    split_plan: BarSplitPlan,
    candidate_key: str,
) -> RunSpecRegistration:
    """Register and atomically bind one exact candidate before outcomes."""

    profile = require_bar_state_campaign_profile(definition.profile)
    key = _candidate_key(candidate_key)
    trial_parameters = _validate_bar_state_run_spec(
        run_spec,
        definition=definition,
        split_plan=split_plan,
        candidate_key=key,
    )
    registration = register_run_spec(database_url, run_spec)
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with connection.transaction():
            trial = connection.execute(
                """
                SELECT t.experiment_trial_id, t.trial_key, t.trial_type,
                       t.status, t.parameters, t.parameters_sha256,
                       t.research_run_spec_id, e.experiment_id, e.experiment_key,
                       c.campaign_id, c.campaign_key
                FROM systematic_fx.experiment_trials AS t
                JOIN systematic_fx.experiments AS e
                  ON e.experiment_id = t.experiment_id
                JOIN systematic_fx.campaigns AS c
                  ON c.campaign_id = e.campaign_id
                WHERE e.experiment_key = %s AND t.trial_key = %s
                FOR UPDATE OF t
                """,
                (profile.experiment_key, key),
            ).fetchone()
            trial = _row_or_error(trial, label=f"candidate trial {key}")
            _assert_fields(
                label=f"candidate trial {key}",
                row=trial,
                expected={
                    "trial_key": key,
                    "trial_type": "MODEL_FIT",
                    "parameters": trial_parameters,
                    "parameters_sha256": canonical_sha256(trial_parameters),
                    "experiment_id": registration.experiment_id,
                    "experiment_key": profile.experiment_key,
                    "campaign_id": registration.campaign_id,
                    "campaign_key": profile.campaign_key,
                },
            )
            if trial["status"] not in {"REGISTERED", "RUNNING", "SUCCEEDED", "REJECTED"}:
                raise BarStateRegistryStateError("candidate trial cannot bind a RunSpec")
            existing = trial["research_run_spec_id"]
            if existing is None:
                updated = connection.execute(
                    """
                    UPDATE systematic_fx.experiment_trials
                    SET research_run_spec_id = %s
                    WHERE experiment_trial_id = %s AND research_run_spec_id IS NULL
                    RETURNING experiment_trial_id
                    """,
                    (
                        registration.research_run_spec_id,
                        trial["experiment_trial_id"],
                    ),
                ).fetchone()
                if updated is None:
                    raise BarStateRegistryStateError("candidate lost its unbound state")
            elif int(existing) != registration.research_run_spec_id:
                raise BarStateRegistryDriftError("candidate is bound to another RunSpec")
    return registration


def _artifact_role(value: object) -> BarStateArtifactRole:
    if value not in _ARTIFACT_ROLE_TO_KIND:
        raise BarStateRegistryError(
            f"artifact_role must be one of {sorted(_ARTIFACT_ROLE_TO_KIND)}"
        )
    return value  # type: ignore[return-value]


@_translate_psycopg_errors("bar-state artifact link registration")
def register_bar_state_artifact_link(
    database_url: str,
    project_root: Path,
    *,
    research_run_attempt_id: int,
    candidate_key: str,
    artifact_role: BarStateArtifactRole,
    split_key: str,
    shard_ordinal: int,
    artifact: PublishedBarArtifact,
    profile: BarStateCampaignProfile = BAR_STATE_V2_PROFILE,
) -> BarStateArtifactLinkReport:
    """Register one exact Discovery artifact against a RUNNING candidate attempt."""

    selected_profile = require_bar_state_campaign_profile(profile)
    attempt_id = _positive_integer(
        research_run_attempt_id,
        label="research_run_attempt_id",
    )
    key = _candidate_key(candidate_key)
    role = _artifact_role(artifact_role)
    if split_key not in _DISCOVERY_SPLIT_KEYS:
        raise BarStateRegistryError("bar-state artifacts may reference only Discovery splits")
    if isinstance(shard_ordinal, bool) or not isinstance(shard_ordinal, int):
        raise BarStateRegistryError("shard_ordinal must be an integer")
    if shard_ordinal < 0:
        raise BarStateRegistryError("shard_ordinal cannot be negative")
    expected_kind = _ARTIFACT_ROLE_TO_KIND[role]
    logical = artifact.descriptor.logical_identity
    lineage = logical.get("lineage")
    if not isinstance(lineage, Mapping):
        raise BarStateRegistryError("artifact lacks bar-state lineage")
    if (
        artifact.descriptor.artifact_type != selected_profile.artifact_type
        or artifact.descriptor.artifact_schema != BAR_STATE_ARTIFACT_SCHEMA_BY_KIND[expected_kind]
        or logical.get("artifact_kind") != expected_kind
        or logical.get("campaign_key") != selected_profile.campaign_key
        or not bar_state_lineage_matches_profile(lineage, profile=selected_profile)
        or lineage.get("discovery_scope_sha256") != frozen_bar_state_discovery_scope().sha256
    ):
        raise BarStateRegistryError("artifact role or Discovery lineage drift")
    if role in {"MODEL", "OOS_TRADE", "TERMINAL_RESULT"} and (
        logical.get("candidate_key") != key or lineage.get("candidate_key") != key
    ):
        raise BarStateRegistryError("candidate-specific artifact belongs to another candidate")
    if role == "OOS_TRADE" and (
        isinstance(logical.get("row_count"), bool)
        or not isinstance(logical.get("row_count"), int)
        or logical.get("row_count") != artifact.descriptor.record_count
    ):
        raise BarStateRegistryError("OOS trade artifact row-count identity drift")
    if role == "MODEL":
        model_document = load_verified_bar_state_json(
            project_root,
            artifact,
            profile=selected_profile,
        )
        raw_binding = logical.get("finalist_model_binding")
        if raw_binding is not None and not isinstance(raw_binding, Mapping):
            raise BarStateRegistryError("MODEL finalist binding must be an object or null")
        model_projection = bar_state_model_package_projection(
            model_document,
            expected_candidate_key=key,
            expected_binding=raw_binding,
            profile=selected_profile,
        )
        if (
            artifact.descriptor.record_count != model_projection.record_count
            or _SHA256.fullmatch(str(logical.get("candidate_selection_sha256"))) is None
            or _SHA256.fullmatch(str(logical.get("candidate_selection_projection_sha256"))) is None
            or _SHA256.fullmatch(str(logical.get("global_evidence_projection_sha256"))) is None
            or logical.get("finalist_model_binding_sha256")
            != canonical_sha256(model_projection.binding)
            or logical.get("model_package_projection") != dict(model_projection.projection)
            or logical.get("model_package_projection_sha256") != model_projection.sha256
        ):
            raise BarStateRegistryError("MODEL artifact semantic identity drift")
    global_projection = None
    if role == "GLOBAL_RESULT":
        global_document = load_verified_bar_state_json(
            project_root,
            artifact,
            profile=selected_profile,
        )
        global_projection = bar_state_global_result_projection(
            global_document,
            profile=selected_profile,
        )
        if (
            logical.get("candidate_evidence_slice_sha256_by_key")
            != dict(global_projection.candidate_evidence_slice_sha256_by_key)
            or logical.get("candidate_oos_trade_record_count_by_key")
            != dict(global_projection.candidate_oos_trade_record_count_by_key)
            or logical.get("candidate_selection_sha256_by_key")
            != dict(global_projection.candidate_selection_sha256_by_key)
            or logical.get("candidate_selection_projection_sha256_by_key")
            != dict(global_projection.candidate_selection_projection_sha256_by_key)
            or logical.get("global_evidence_projection_sha256")
            != global_projection.evidence_projection_sha256
            or not isinstance(logical.get("model_package_projection_sha256_by_key"), Mapping)
            or set(logical["model_package_projection_sha256_by_key"])
            != set(global_projection.candidate_selections)
            or any(
                _SHA256.fullmatch(str(value)) is None
                for value in logical["model_package_projection_sha256_by_key"].values()
            )
            or logical.get("finalist_model_binding_sha256_by_key")
            != dict(global_projection.finalist_model_binding_sha256_by_key)
            or logical.get("finalist_model_binding_by_key")
            != {
                candidate: (
                    None
                    if (binding := global_projection.finalist_bindings.get(candidate)) is None
                    else dict(binding)
                )
                for candidate in global_projection.candidate_selections
            }
        ):
            raise BarStateRegistryError("global artifact semantic hash catalog drift")
    if role == "TERMINAL_RESULT":
        terminal_document = load_verified_bar_state_json(
            project_root,
            artifact,
            profile=selected_profile,
        )
        terminal_compact = bar_state_terminal_compact_summary(terminal_document)
        terminal_result = _canonical_mapping(
            terminal_document.get("result"), label="terminal result"
        )
        terminal_selection = _canonical_mapping(
            terminal_result.get("candidate_selection"), label="terminal selection"
        )
        terminal_evidence_slice = {
            "candidate_support": terminal_result.get("candidate_support"),
            "multiplicity_cells": terminal_result.get("multiplicity_cells"),
        }
        if (
            terminal_document.get("candidate_key") != key
            or terminal_document.get("decision_label") != logical.get("decision_label")
            or terminal_document.get("trial_status") != logical.get("trial_status")
            or logical.get("compact_summary_sha256") != canonical_sha256(terminal_compact)
            or logical.get("candidate_evidence_slice_sha256")
            != canonical_sha256(terminal_evidence_slice)
            or logical.get("candidate_selection_sha256") != canonical_sha256(terminal_selection)
            or logical.get("candidate_selection_projection_sha256")
            != canonical_sha256(bar_state_candidate_selection_projection(terminal_selection))
            or _SHA256.fullmatch(str(logical.get("global_evidence_projection_sha256"))) is None
            or _SHA256.fullmatch(str(logical.get("model_package_projection_sha256"))) is None
            or logical.get("finalist_model_binding")
            != terminal_result.get("discovery_final_fit_model")
            or logical.get("finalist_model_binding_sha256")
            != canonical_sha256(terminal_result.get("discovery_final_fit_model"))
        ):
            raise BarStateRegistryError("terminal artifact summary or identity drift")
    lineage_sha256 = canonical_sha256(lineage)

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with (
            connection.transaction(),
            open_verified_bar_artifact(project_root, artifact),
        ):
            identity = connection.execute(
                """
                SELECT a.research_run_attempt_id, a.research_run_spec_id,
                       a.status AS attempt_status, r.run_fingerprint,
                       r.campaign_id, r.experiment_id,
                       r.canonical_spec #>> '{parameters,bar_state_candidate_key}'
                           AS candidate_key,
                       t.experiment_trial_id, t.research_run_spec_id AS trial_spec_id
                FROM systematic_fx.research_run_attempts AS a
                JOIN systematic_fx.research_run_specs AS r
                  ON r.research_run_spec_id = a.research_run_spec_id
                JOIN systematic_fx.experiment_trials AS t
                  ON t.experiment_id = r.experiment_id
                 AND t.trial_key =
                     r.canonical_spec #>> '{parameters,bar_state_candidate_key}'
                JOIN systematic_fx.campaigns AS c
                  ON c.campaign_id = r.campaign_id
                WHERE a.research_run_attempt_id = %s
                  AND c.campaign_key = %s
                FOR SHARE OF a, r, t, c
                """,
                (attempt_id, selected_profile.campaign_key),
            ).fetchone()
            identity = _row_or_error(identity, label=f"bar-state attempt {attempt_id}")
            _assert_fields(
                label=f"bar-state attempt {attempt_id}",
                row=identity,
                expected={
                    "research_run_attempt_id": attempt_id,
                    "attempt_status": "RUNNING",
                    "candidate_key": key,
                    "trial_spec_id": identity["research_run_spec_id"],
                },
            )
            if (
                role in {"MODEL", "OOS_TRADE", "TERMINAL_RESULT"}
                and lineage.get("run_fingerprint") != identity["run_fingerprint"]
            ):
                raise BarStateRegistryDriftError("artifact RunSpec fingerprint drift")
            if role == "TERMINAL_RESULT":
                semantic_metadata = connection.execute(
                    """
                    SELECT global_artifact.metadata AS global_metadata,
                           model_artifact.metadata AS model_metadata
                    FROM systematic_fx.bar_state_artifact_links AS global_link
                    JOIN systematic_fx.artifacts AS global_artifact
                      ON global_artifact.artifact_id = global_link.artifact_id
                    JOIN systematic_fx.bar_state_artifact_links AS model_link
                      ON model_link.research_run_attempt_id =
                         global_link.research_run_attempt_id
                     AND model_link.artifact_role = 'MODEL'
                     AND model_link.split_key = 'discovery'
                     AND model_link.shard_ordinal = 0
                    JOIN systematic_fx.artifacts AS model_artifact
                      ON model_artifact.artifact_id = model_link.artifact_id
                    WHERE global_link.research_run_attempt_id = %s
                      AND global_link.artifact_role = 'GLOBAL_RESULT'
                      AND global_link.split_key = 'discovery'
                      AND global_link.shard_ordinal = 0
                    FOR SHARE OF global_link, global_artifact
                    """,
                    (attempt_id,),
                ).fetchone()
                if semantic_metadata is None:
                    raise BarStateRegistryStateError(
                        "terminal artifact requires its GLOBAL/MODEL semantic binding"
                    )
                global_logical = _canonical_mapping(
                    semantic_metadata["global_metadata"].get("logical_identity"),
                    label="GLOBAL logical identity",
                )
                model_logical = _canonical_mapping(
                    semantic_metadata["model_metadata"].get("logical_identity"),
                    label="MODEL logical identity",
                )
                evidence_slice_hashes = _canonical_mapping(
                    global_logical.get("candidate_evidence_slice_sha256_by_key"),
                    label="GLOBAL candidate-evidence-slice hash catalog",
                )
                selection_hashes = _canonical_mapping(
                    global_logical.get("candidate_selection_sha256_by_key"),
                    label="GLOBAL candidate-selection hash catalog",
                )
                selection_projection_hashes = _canonical_mapping(
                    global_logical.get("candidate_selection_projection_sha256_by_key"),
                    label="GLOBAL candidate-selection projection hash catalog",
                )
                model_package_hashes = _canonical_mapping(
                    global_logical.get("model_package_projection_sha256_by_key"),
                    label="GLOBAL MODEL package projection hash catalog",
                )
                binding_hashes = _canonical_mapping(
                    global_logical.get("finalist_model_binding_sha256_by_key"),
                    label="GLOBAL finalist-binding hash catalog",
                )
                bindings = _canonical_mapping(
                    global_logical.get("finalist_model_binding_by_key"),
                    label="GLOBAL finalist-binding catalog",
                )
                if (
                    evidence_slice_hashes.get(key) != logical.get("candidate_evidence_slice_sha256")
                    or selection_hashes.get(key) != logical.get("candidate_selection_sha256")
                    or selection_projection_hashes.get(key)
                    != logical.get("candidate_selection_projection_sha256")
                    or model_package_hashes.get(key)
                    != logical.get("model_package_projection_sha256")
                    or global_logical.get("global_evidence_projection_sha256")
                    != logical.get("global_evidence_projection_sha256")
                    or binding_hashes.get(key) != logical.get("finalist_model_binding_sha256")
                    or bindings.get(key) != logical.get("finalist_model_binding")
                    or model_logical.get("candidate_selection_sha256")
                    != logical.get("candidate_selection_sha256")
                    or model_logical.get("candidate_selection_projection_sha256")
                    != logical.get("candidate_selection_projection_sha256")
                    or model_logical.get("global_evidence_projection_sha256")
                    != logical.get("global_evidence_projection_sha256")
                    or model_logical.get("model_package_projection_sha256")
                    != logical.get("model_package_projection_sha256")
                    or model_logical.get("finalist_model_binding")
                    != logical.get("finalist_model_binding")
                    or model_logical.get("finalist_model_binding_sha256")
                    != logical.get("finalist_model_binding_sha256")
                ):
                    raise BarStateRegistryDriftError(
                        "terminal artifact differs from its GLOBAL semantic binding"
                    )
            artifact_id, _ = _ensure_artifact(connection, artifact)
            inserted = connection.execute(
                """
                INSERT INTO systematic_fx.bar_state_artifact_links
                    (campaign_id, experiment_trial_id, research_run_spec_id,
                     research_run_attempt_id, artifact_id, artifact_role,
                     split_key, shard_ordinal, artifact_identity_sha256,
                     content_sha256, lineage_sha256)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (research_run_attempt_id, artifact_role,
                             split_key, shard_ordinal) DO NOTHING
                RETURNING bar_state_artifact_link_id
                """,
                (
                    identity["campaign_id"],
                    identity["experiment_trial_id"],
                    identity["research_run_spec_id"],
                    attempt_id,
                    artifact_id,
                    role,
                    split_key,
                    shard_ordinal,
                    artifact.descriptor.identity_sha256,
                    artifact.sha256,
                    lineage_sha256,
                ),
            ).fetchone()
            created = inserted is not None
            row = connection.execute(
                """
                SELECT bar_state_artifact_link_id, campaign_id,
                       experiment_trial_id, research_run_spec_id,
                       research_run_attempt_id, artifact_id, artifact_role,
                       split_key, shard_ordinal, artifact_identity_sha256,
                       content_sha256, lineage_sha256
                FROM systematic_fx.bar_state_artifact_links
                WHERE research_run_attempt_id = %s
                  AND artifact_role = %s AND split_key = %s
                  AND shard_ordinal = %s
                FOR SHARE
                """,
                (attempt_id, role, split_key, shard_ordinal),
            ).fetchone()
            row = _row_or_error(row, label="bar-state artifact link")
            _assert_fields(
                label="bar-state artifact link",
                row=row,
                expected={
                    "campaign_id": identity["campaign_id"],
                    "experiment_trial_id": identity["experiment_trial_id"],
                    "research_run_spec_id": identity["research_run_spec_id"],
                    "research_run_attempt_id": attempt_id,
                    "artifact_id": artifact_id,
                    "artifact_role": role,
                    "split_key": split_key,
                    "shard_ordinal": shard_ordinal,
                    "artifact_identity_sha256": artifact.descriptor.identity_sha256,
                    "content_sha256": artifact.sha256,
                    "lineage_sha256": lineage_sha256,
                },
            )
    return BarStateArtifactLinkReport(
        bar_state_artifact_link_id=int(row["bar_state_artifact_link_id"]),
        artifact_id=artifact_id,
        research_run_attempt_id=attempt_id,
        artifact_role=role,
        shard_ordinal=shard_ordinal,
        created=created,
    )


def _link_manifest(rows: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, object]], str]:
    links = [
        {
            "artifact_id": int(row["artifact_id"]),
            "artifact_identity_sha256": str(row["artifact_identity_sha256"]),
            "artifact_role": str(row["artifact_role"]),
            "content_sha256": str(row["content_sha256"]),
            "lineage_sha256": str(row["lineage_sha256"]),
            "shard_ordinal": int(row["shard_ordinal"]),
            "split_key": str(row["split_key"]),
        }
        for row in rows
    ]
    return links, canonical_sha256(links)


def _global_candidate_selection(
    document: Mapping[str, object],
    candidate_key: str,
    *,
    profile: BarStateCampaignProfile = BAR_STATE_V2_PROFILE,
) -> tuple[Mapping[str, object], Mapping[str, object] | None, tuple[str, ...], str, str]:
    """Extract one exact candidate selection from the immutable global result."""

    try:
        projection = bar_state_global_result_projection(document, profile=profile)
    except BarStateArtifactError as error:
        raise BarStateRegistryDriftError("global result semantic contract drift") from error
    try:
        selection = projection.candidate_selections[candidate_key]
        selection_sha256 = projection.candidate_selection_sha256_by_key[candidate_key]
        binding_sha256 = projection.finalist_model_binding_sha256_by_key[candidate_key]
    except KeyError as error:
        raise BarStateRegistryDriftError(
            "global result lacks one exact candidate selection"
        ) from error
    return (
        selection,
        projection.finalist_bindings.get(candidate_key),
        projection.finalist_keys,
        selection_sha256,
        binding_sha256,
    )


@_translate_psycopg_errors("bar-state candidate terminal registration")
def register_terminal_bar_state_result(
    database_url: str,
    project_root: Path,
    *,
    research_run_attempt_id: int,
    candidate_key: str,
    trial_status: BarStateTrialStatus,
    decision_label: str,
    compact_summary: Mapping[str, object],
    profile: BarStateCampaignProfile = BAR_STATE_V2_PROFILE,
) -> BarStateTerminalReport:
    """Atomically pair one successful computation with its terminal trial."""

    selected_profile = require_bar_state_campaign_profile(profile)
    attempt_id = _positive_integer(
        research_run_attempt_id,
        label="research_run_attempt_id",
    )
    key = _candidate_key(candidate_key)
    if trial_status not in {"SUCCEEDED", "REJECTED"}:
        raise BarStateRegistryError("trial_status must be SUCCEEDED or REJECTED")
    label = _nonempty(decision_label, label="decision_label")
    expected_label = "DISCOVERY_FINALIST" if trial_status == "SUCCEEDED" else "DISCOVERY_REJECT"
    if label != expected_label:
        raise BarStateRegistryError("decision_label differs from trial_status")
    compact = _canonical_mapping(compact_summary, label="compact_summary")
    if len(canonical_json_bytes(compact)) > 32_768:
        raise BarStateRegistryError("compact_summary exceeds 32 KiB")

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with connection.transaction():
            identity = connection.execute(
                """
                SELECT a.research_run_attempt_id, a.research_run_spec_id,
                       a.attempt_number, a.status AS attempt_status,
                       a.started_at, r.run_fingerprint, r.experiment_id,
                       r.canonical_spec #>> '{parameters,bar_state_candidate_key}'
                           AS candidate_key,
                       t.experiment_trial_id, t.status AS trial_status,
                       t.research_run_spec_id AS trial_spec_id
                FROM systematic_fx.research_run_attempts AS a
                JOIN systematic_fx.research_run_specs AS r
                  ON r.research_run_spec_id = a.research_run_spec_id
                JOIN systematic_fx.experiment_trials AS t
                  ON t.experiment_id = r.experiment_id
                 AND t.trial_key =
                     r.canonical_spec #>> '{parameters,bar_state_candidate_key}'
                JOIN systematic_fx.campaigns AS c
                  ON c.campaign_id = r.campaign_id
                WHERE a.research_run_attempt_id = %s
                  AND c.campaign_key = %s
                FOR UPDATE OF a, t
                """,
                (attempt_id, selected_profile.campaign_key),
            ).fetchone()
            identity = _row_or_error(identity, label=f"bar-state attempt {attempt_id}")
            _assert_fields(
                label=f"bar-state attempt {attempt_id}",
                row=identity,
                expected={
                    "research_run_attempt_id": attempt_id,
                    "attempt_status": "RUNNING",
                    "candidate_key": key,
                    "trial_spec_id": identity["research_run_spec_id"],
                },
            )
            if identity["started_at"] is None or identity["trial_status"] not in {
                "REGISTERED",
                "RUNNING",
            }:
                raise BarStateRegistryStateError("candidate is not active")
            rows = connection.execute(
                """
                SELECT l.artifact_role, l.split_key,
                       l.shard_ordinal, l.artifact_identity_sha256,
                       l.content_sha256, l.lineage_sha256, artifact.*
                FROM systematic_fx.bar_state_artifact_links AS l
                JOIN systematic_fx.artifacts AS artifact
                  ON artifact.artifact_id = l.artifact_id
                WHERE l.research_run_attempt_id = %s
                ORDER BY l.artifact_role, l.split_key, l.shard_ordinal
                FOR SHARE
                """,
                (attempt_id,),
            ).fetchall()
            role_counts = Counter(str(row["artifact_role"]) for row in rows)
            expected_coordinates = {
                *(("FEATURE", "discovery", shard) for shard in range(4)),
                *(("LABEL", "discovery", shard) for shard in range(4)),
                ("MODEL", "discovery", 0),
                ("OOS_TRADE", "discovery", 0),
                ("GLOBAL_RESULT", "discovery", 0),
                ("TERMINAL_RESULT", "discovery", 0),
            }
            observed_coordinates = {
                (
                    str(row["artifact_role"]),
                    str(row["split_key"]),
                    int(row["shard_ordinal"]),
                )
                for row in rows
            }
            if observed_coordinates != expected_coordinates or len(rows) != len(
                expected_coordinates
            ):
                raise BarStateRegistryStateError(
                    "terminal candidate requires the exact 12-shard evidence manifest"
                )
            terminal_row = next(row for row in rows if row["artifact_role"] == "TERMINAL_RESULT")
            global_row = next(row for row in rows if row["artifact_role"] == "GLOBAL_RESULT")
            model_row = next(row for row in rows if row["artifact_role"] == "MODEL")
            oos_row = next(row for row in rows if row["artifact_role"] == "OOS_TRADE")
            terminal_artifact = _artifact_from_database_row(terminal_row)
            global_artifact = _artifact_from_database_row(global_row)
            model_artifact = _artifact_from_database_row(model_row)
            oos_artifact = _artifact_from_database_row(oos_row)
            terminal_document = load_verified_bar_state_json(
                project_root,
                terminal_artifact,
                profile=selected_profile,
            )
            global_document = load_verified_bar_state_json(
                project_root,
                global_artifact,
                profile=selected_profile,
            )
            model_document = load_verified_bar_state_json(
                project_root,
                model_artifact,
                profile=selected_profile,
            )
            projected_compact = bar_state_terminal_compact_summary(terminal_document)
            (
                global_selection,
                global_binding,
                global_finalists,
                selection_sha256,
                binding_sha256,
            ) = _global_candidate_selection(
                global_document,
                key,
                profile=selected_profile,
            )
            global_projection = bar_state_global_result_projection(
                global_document,
                profile=selected_profile,
            )
            selection_projection_sha256 = (
                global_projection.candidate_selection_projection_sha256_by_key[key]
            )
            evidence_projection_sha256 = global_projection.evidence_projection_sha256
            candidate_evidence_slice = dict(global_projection.candidate_evidence_slice_by_key[key])
            candidate_evidence_slice_sha256 = (
                global_projection.candidate_evidence_slice_sha256_by_key[key]
            )
            expected_oos_trade_record_count = (
                global_projection.candidate_oos_trade_record_count_by_key[key]
            )
            terminal_result = _canonical_mapping(
                terminal_document.get("result"),
                label="terminal result",
            )
            terminal_logical = terminal_artifact.descriptor.logical_identity
            global_logical = global_artifact.descriptor.logical_identity
            model_logical = model_artifact.descriptor.logical_identity
            model_projection = bar_state_model_package_projection(
                model_document,
                expected_candidate_key=key,
                expected_binding=global_binding,
                profile=selected_profile,
            )
            model_package_projection_sha256 = model_projection.sha256
            terminal_evidence_slice = {
                "candidate_support": terminal_result.get("candidate_support"),
                "multiplicity_cells": terminal_result.get("multiplicity_cells"),
            }
            if (
                terminal_document.get("candidate_key") != key
                or projected_compact != compact
                or terminal_document.get("decision_label") != label
                or terminal_document.get("trial_status") != trial_status
                or terminal_evidence_slice != candidate_evidence_slice
                or canonical_sha256(terminal_evidence_slice) != candidate_evidence_slice_sha256
                or terminal_result.get("candidate_selection") != global_selection
                or terminal_result.get("discovery_final_fit_model") != global_binding
                or model_projection.binding != global_binding
                or model_artifact.descriptor.record_count != model_projection.record_count
                or (key in global_finalists) != (trial_status == "SUCCEEDED")
                or terminal_logical.get("compact_summary_sha256") != canonical_sha256(compact)
                or terminal_logical.get("candidate_evidence_slice_sha256")
                != candidate_evidence_slice_sha256
                or terminal_logical.get("candidate_selection_sha256") != selection_sha256
                or terminal_logical.get("candidate_selection_projection_sha256")
                != selection_projection_sha256
                or terminal_logical.get("global_evidence_projection_sha256")
                != evidence_projection_sha256
                or terminal_logical.get("model_package_projection_sha256")
                != model_package_projection_sha256
                or terminal_logical.get("finalist_model_binding") != global_binding
                or terminal_logical.get("finalist_model_binding_sha256") != binding_sha256
                or model_logical.get("candidate_selection_sha256") != selection_sha256
                or model_logical.get("candidate_selection_projection_sha256")
                != selection_projection_sha256
                or model_logical.get("global_evidence_projection_sha256")
                != evidence_projection_sha256
                or model_logical.get("model_package_projection")
                != dict(model_projection.projection)
                or model_logical.get("model_package_projection_sha256")
                != model_package_projection_sha256
                or model_logical.get("finalist_model_binding") != global_binding
                or model_logical.get("finalist_model_binding_sha256") != binding_sha256
                or global_logical.get("candidate_selection_sha256_by_key")
                != dict(global_projection.candidate_selection_sha256_by_key)
                or global_logical.get("candidate_evidence_slice_sha256_by_key")
                != dict(global_projection.candidate_evidence_slice_sha256_by_key)
                or global_logical.get("candidate_oos_trade_record_count_by_key")
                != dict(global_projection.candidate_oos_trade_record_count_by_key)
                or oos_artifact.descriptor.record_count != expected_oos_trade_record_count
                or oos_artifact.descriptor.logical_identity.get("row_count")
                != expected_oos_trade_record_count
                or global_logical.get("candidate_selection_projection_sha256_by_key")
                != dict(global_projection.candidate_selection_projection_sha256_by_key)
                or global_logical.get("global_evidence_projection_sha256")
                != evidence_projection_sha256
                or global_logical.get("model_package_projection_sha256_by_key", {}).get(key)
                != model_package_projection_sha256
                or global_logical.get("finalist_model_binding_sha256_by_key")
                != dict(global_projection.finalist_model_binding_sha256_by_key)
                or global_logical.get("finalist_model_binding_by_key")
                != {
                    candidate: (
                        None
                        if (binding := global_projection.finalist_bindings.get(candidate)) is None
                        else dict(binding)
                    )
                    for candidate in global_projection.candidate_selections
                }
            ):
                raise BarStateRegistryDriftError(
                    "terminal artifact differs from the compact terminal summary"
                )
            links, links_sha256 = _link_manifest(rows)
            summary = {
                "artifact_link_manifest_sha256": links_sha256,
                "artifact_role_counts": dict(sorted(role_counts.items())),
                "attempt_status": "SUCCEEDED",
                "candidate_key": key,
                "candidate_evidence_slice_sha256": candidate_evidence_slice_sha256,
                "candidate_selection_sha256": selection_sha256,
                "candidate_selection_projection_sha256": selection_projection_sha256,
                "compact_summary": compact,
                "decision_label": label,
                "finalist_model_binding_sha256": binding_sha256,
                "global_evidence_projection_sha256": evidence_projection_sha256,
                "model_package_projection_sha256": model_package_projection_sha256,
                "result_artifact_id": int(terminal_row["artifact_id"]),
                "run_fingerprint": str(identity["run_fingerprint"]),
                "schema": BAR_STATE_TERMINAL_SUMMARY_SCHEMA,
                "trial_status": trial_status,
            }
            if len(canonical_json_bytes(summary)) > 65_536:
                raise BarStateRegistryError("terminal result summary exceeds 64 KiB")
            attempt = connection.execute(
                """
                UPDATE systematic_fx.research_run_attempts
                SET status = 'SUCCEEDED', result_artifact_id = %s,
                    trade_ledger_artifact_id = NULL, result_summary = %s,
                    error_message = NULL, finished_at = statement_timestamp()
                WHERE research_run_attempt_id = %s AND status = 'RUNNING'
                RETURNING research_run_spec_id
                """,
                (int(terminal_row["artifact_id"]), Jsonb(summary), attempt_id),
            ).fetchone()
            if attempt is None:
                raise BarStateRegistryStateError("attempt lost its RUNNING state")
            trial = connection.execute(
                """
                UPDATE systematic_fx.experiment_trials
                SET status = %s, result_summary = %s,
                    started_at = COALESCE(started_at, %s),
                    finished_at = statement_timestamp()
                WHERE experiment_trial_id = %s
                  AND status IN ('REGISTERED', 'RUNNING')
                RETURNING experiment_trial_id
                """,
                (
                    trial_status,
                    Jsonb(summary),
                    identity["started_at"],
                    identity["experiment_trial_id"],
                ),
            ).fetchone()
            if trial is None:
                raise BarStateRegistryStateError("candidate trial lost its active state")
            # Keep the compact manifest in SQL and every row in content-addressed files.
            if canonical_sha256(links) != summary["artifact_link_manifest_sha256"]:
                raise BarStateRegistryDriftError("artifact link manifest changed in transaction")

    return BarStateTerminalReport(
        candidate_key=key,
        research_run_attempt_id=attempt_id,
        research_run_spec_id=int(attempt["research_run_spec_id"]),
        trial_status=trial_status,
        result_artifact_id=int(terminal_row["artifact_id"]),
    )


@_translate_psycopg_errors("bar-state run-attempt abort")
def abort_bar_state_run_attempt(
    database_url: str,
    *,
    research_run_attempt_id: int,
    candidate_key: str,
    run_fingerprint: str,
    error_message: str,
    profile: BarStateCampaignProfile = BAR_STATE_V2_PROFILE,
) -> RunAttemptState:
    """Fail one active attempt without terminalizing its reusable candidate trial."""

    selected_profile = require_bar_state_campaign_profile(profile)
    attempt_id = _positive_integer(
        research_run_attempt_id,
        label="research_run_attempt_id",
    )
    key = _candidate_key(candidate_key)
    fingerprint = _sha256(run_fingerprint, label="run_fingerprint")
    message = _nonempty(error_message, label="error_message")
    if len(message) > 2_000:
        raise BarStateRegistryError("error_message exceeds 2,000 characters")
    summary = {"candidate_key": key, "run_fingerprint": fingerprint}
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with connection.transaction():
            row = connection.execute(
                """
                SELECT a.research_run_attempt_id, a.research_run_spec_id,
                       a.attempt_number, a.status, r.run_fingerprint,
                       r.canonical_spec #>> '{parameters,bar_state_candidate_key}'
                           AS candidate_key
                FROM systematic_fx.research_run_attempts AS a
                JOIN systematic_fx.research_run_specs AS r
                  ON r.research_run_spec_id = a.research_run_spec_id
                JOIN systematic_fx.campaigns AS c
                  ON c.campaign_id = r.campaign_id
                WHERE a.research_run_attempt_id = %s
                  AND c.campaign_key = %s
                FOR UPDATE OF a
                """,
                (attempt_id, selected_profile.campaign_key),
            ).fetchone()
            row = _row_or_error(row, label=f"bar-state attempt {attempt_id}")
            _assert_fields(
                label=f"bar-state attempt {attempt_id}",
                row=row,
                expected={"candidate_key": key, "run_fingerprint": fingerprint},
            )
            if row["status"] not in {"QUEUED", "RUNNING"}:
                raise BarStateRegistryStateError("only an active attempt can be failed")
            updated = connection.execute(
                """
                UPDATE systematic_fx.research_run_attempts
                SET status = 'FAILED', result_summary = %s,
                    error_message = %s, finished_at = statement_timestamp()
                WHERE research_run_attempt_id = %s
                  AND status IN ('QUEUED', 'RUNNING')
                RETURNING research_run_attempt_id, research_run_spec_id,
                          attempt_number, status, result_artifact_id,
                          trade_ledger_artifact_id
                """,
                (Jsonb(summary), message, attempt_id),
            ).fetchone()
            if updated is None:
                raise BarStateRegistryStateError("attempt lost its active state")
    return RunAttemptState(
        research_run_attempt_id=attempt_id,
        research_run_spec_id=int(updated["research_run_spec_id"]),
        attempt_number=int(updated["attempt_number"]),
        status="FAILED",
        result_artifact_id=None,
        trade_ledger_artifact_id=None,
    )


@_translate_psycopg_errors("bar-state duplicate validation")
def validate_reused_bar_state_attempt(
    database_url: str,
    project_root: Path,
    *,
    reservation: RunAttemptReservation,
    candidate_key: str,
    profile: BarStateCampaignProfile = BAR_STATE_V2_PROFILE,
) -> BarStateReuseValidationReport:
    """Reopen and rehash every artifact before accepting an exact duplicate."""

    selected_profile = require_bar_state_campaign_profile(profile)
    if reservation.execute or reservation.status != "SKIPPED_DUPLICATE":
        raise BarStateRegistryError("reservation is not an exact duplicate")
    attempt_id = _positive_integer(
        reservation.research_run_attempt_id,
        label="research_run_attempt_id",
    )
    reused_id = _positive_integer(reservation.reused_attempt_id, label="reused_attempt_id")
    key = _candidate_key(candidate_key)
    with (
        psycopg.connect(database_url, row_factory=dict_row) as connection,
        connection.transaction(),
    ):
        identity = connection.execute(
            """
            SELECT duplicate.status, duplicate.reused_attempt_id,
                   source.status AS source_status,
                   source.result_artifact_id AS source_result_artifact_id,
                   source.result_summary AS source_result_summary,
                   trial.status AS source_trial_status,
                   trial.result_summary AS source_trial_result_summary,
                   r.canonical_spec #>> '{parameters,bar_state_candidate_key}'
                       AS candidate_key
            FROM systematic_fx.research_run_attempts AS duplicate
            JOIN systematic_fx.research_run_attempts AS source
              ON source.research_run_attempt_id = duplicate.reused_attempt_id
            JOIN systematic_fx.research_run_specs AS r
              ON r.research_run_spec_id = duplicate.research_run_spec_id
             AND r.research_run_spec_id = source.research_run_spec_id
            JOIN systematic_fx.campaigns AS c
              ON c.campaign_id = r.campaign_id
            JOIN systematic_fx.experiment_trials AS trial
              ON trial.experiment_id = r.experiment_id
             AND trial.trial_key =
                 r.canonical_spec #>> '{parameters,bar_state_candidate_key}'
             AND trial.research_run_spec_id = r.research_run_spec_id
            WHERE duplicate.research_run_attempt_id = %s
              AND c.campaign_key = %s
            FOR SHARE OF duplicate, source, r, c, trial
            """,
            (attempt_id, selected_profile.campaign_key),
        ).fetchone()
        identity = _row_or_error(identity, label=f"duplicate attempt {attempt_id}")
        _assert_fields(
            label=f"duplicate attempt {attempt_id}",
            row=identity,
            expected={
                "status": "SKIPPED_DUPLICATE",
                "reused_attempt_id": reused_id,
                "source_status": "SUCCEEDED",
                "candidate_key": key,
            },
        )
        summary = _canonical_mapping(
            identity["source_result_summary"],
            label="reused source result_summary",
        )
        if (
            identity["source_trial_status"] not in {"SUCCEEDED", "REJECTED"}
            or identity["source_trial_result_summary"] != identity["source_result_summary"]
        ):
            raise BarStateRegistryDriftError("reused source attempt/trial terminal summary drift")
        rows = connection.execute(
            """
            SELECT l.artifact_role, l.split_key, l.shard_ordinal,
                   l.artifact_identity_sha256, l.content_sha256,
                   l.lineage_sha256, a.*
            FROM systematic_fx.bar_state_artifact_links AS l
            JOIN systematic_fx.artifacts AS a
              ON a.artifact_id = l.artifact_id
            WHERE l.research_run_attempt_id = %s
            ORDER BY l.artifact_role, l.split_key, l.shard_ordinal
            FOR SHARE OF l, a
            """,
            (reused_id,),
        ).fetchall()
        if not rows:
            raise BarStateRegistryDriftError("reused attempt has no artifact evidence")
        evidence: list[BarStateReusedArtifactEvidence] = []
        for row in rows:
            artifact = _artifact_from_database_row(row)
            lineage = artifact.descriptor.logical_identity.get("lineage")
            if (
                artifact.descriptor.identity_sha256 != row["artifact_identity_sha256"]
                or artifact.sha256 != row["content_sha256"]
                or artifact.descriptor.artifact_type != selected_profile.artifact_type
                or artifact.descriptor.logical_identity.get("campaign_key")
                != selected_profile.campaign_key
                or not isinstance(lineage, Mapping)
                or not bar_state_lineage_matches_profile(lineage, profile=selected_profile)
                or canonical_sha256(lineage) != row["lineage_sha256"]
            ):
                raise BarStateRegistryDriftError("reused artifact link identity drift")
            evidence.append(
                BarStateReusedArtifactEvidence(
                    artifact_role=_artifact_role(row["artifact_role"]),
                    split_key=str(row["split_key"]),
                    shard_ordinal=int(row["shard_ordinal"]),
                    lineage_sha256=str(row["lineage_sha256"]),
                    artifact=artifact,
                )
            )
        role_counts = Counter(item.artifact_role for item in evidence)
        expected_coordinates = {
            *(("FEATURE", "discovery", shard) for shard in range(4)),
            *(("LABEL", "discovery", shard) for shard in range(4)),
            ("MODEL", "discovery", 0),
            ("OOS_TRADE", "discovery", 0),
            ("GLOBAL_RESULT", "discovery", 0),
            ("TERMINAL_RESULT", "discovery", 0),
        }
        observed_coordinates = {
            (item.artifact_role, item.split_key, item.shard_ordinal) for item in evidence
        }
        if observed_coordinates != expected_coordinates or len(evidence) != len(
            expected_coordinates
        ):
            raise BarStateRegistryDriftError(
                "reused attempt differs from the exact 12-shard evidence manifest"
            )
        _links, links_sha256 = _link_manifest(rows)
        if summary.get("artifact_link_manifest_sha256") != links_sha256 or summary.get(
            "artifact_role_counts"
        ) != dict(sorted(role_counts.items())):
            raise BarStateRegistryDriftError("reused artifact manifest hash drift")

    for item in evidence:
        with open_verified_bar_artifact(project_root, item.artifact):
            pass
    global_evidence = next(item for item in evidence if item.artifact_role == "GLOBAL_RESULT")
    terminal_evidence = next(item for item in evidence if item.artifact_role == "TERMINAL_RESULT")
    model_evidence = next(item for item in evidence if item.artifact_role == "MODEL")
    oos_evidence = next(item for item in evidence if item.artifact_role == "OOS_TRADE")
    global_document = load_verified_bar_state_json(
        project_root,
        global_evidence.artifact,
        profile=selected_profile,
    )
    terminal_document = load_verified_bar_state_json(
        project_root,
        terminal_evidence.artifact,
        profile=selected_profile,
    )
    model_document = load_verified_bar_state_json(
        project_root,
        model_evidence.artifact,
        profile=selected_profile,
    )
    projected_compact = bar_state_terminal_compact_summary(terminal_document)
    compact = _canonical_mapping(summary.get("compact_summary"), label="compact_summary")
    decision_label = _nonempty(summary.get("decision_label"), label="decision_label")
    trial_status = summary.get("trial_status")
    if trial_status not in {"SUCCEEDED", "REJECTED"}:
        raise BarStateRegistryDriftError("reused trial status is invalid")
    if decision_label != (
        "DISCOVERY_FINALIST" if trial_status == "SUCCEEDED" else "DISCOVERY_REJECT"
    ):
        raise BarStateRegistryDriftError("reused decision label differs from terminal trial status")
    (
        global_selection,
        global_binding,
        frozen_finalists,
        selection_sha256,
        binding_sha256,
    ) = _global_candidate_selection(
        global_document,
        key,
        profile=selected_profile,
    )
    global_projection = bar_state_global_result_projection(
        global_document,
        profile=selected_profile,
    )
    selection_projection_sha256 = global_projection.candidate_selection_projection_sha256_by_key[
        key
    ]
    evidence_projection_sha256 = global_projection.evidence_projection_sha256
    candidate_evidence_slice = dict(global_projection.candidate_evidence_slice_by_key[key])
    candidate_evidence_slice_sha256 = global_projection.candidate_evidence_slice_sha256_by_key[key]
    expected_oos_trade_record_count = global_projection.candidate_oos_trade_record_count_by_key[key]
    model_projection = bar_state_model_package_projection(
        model_document,
        expected_candidate_key=key,
        expected_binding=global_binding,
        profile=selected_profile,
    )
    model_package_projection_sha256 = model_projection.sha256
    model_logical = model_evidence.artifact.descriptor.logical_identity
    terminal_result = _canonical_mapping(
        terminal_document.get("result"),
        label="reused terminal result",
    )
    terminal_evidence_slice = {
        "candidate_support": terminal_result.get("candidate_support"),
        "multiplicity_cells": terminal_result.get("multiplicity_cells"),
    }
    if (
        summary.get("candidate_key") != key
        or identity["source_result_artifact_id"]
        != next(
            int(row["artifact_id"]) for row in rows if row["artifact_role"] == "TERMINAL_RESULT"
        )
        or terminal_document.get("candidate_key") != key
        or summary.get("candidate_evidence_slice_sha256") != candidate_evidence_slice_sha256
        or summary.get("candidate_selection_sha256") != selection_sha256
        or summary.get("candidate_selection_projection_sha256") != selection_projection_sha256
        or projected_compact != compact
        or terminal_document.get("decision_label") != decision_label
        or terminal_document.get("trial_status") != trial_status
        or identity["source_trial_status"] != trial_status
        or summary.get("finalist_model_binding_sha256") != binding_sha256
        or summary.get("global_evidence_projection_sha256") != evidence_projection_sha256
        or summary.get("model_package_projection_sha256") != model_package_projection_sha256
        or terminal_evidence_slice != candidate_evidence_slice
        or canonical_sha256(terminal_evidence_slice) != candidate_evidence_slice_sha256
        or terminal_result.get("candidate_selection") != global_selection
        or terminal_result.get("discovery_final_fit_model") != global_binding
        or model_projection.binding != global_binding
        or model_evidence.artifact.descriptor.record_count != model_projection.record_count
        or (key in frozen_finalists) != (trial_status == "SUCCEEDED")
        or terminal_evidence.artifact.descriptor.logical_identity.get("compact_summary_sha256")
        != canonical_sha256(compact)
        or terminal_evidence.artifact.descriptor.logical_identity.get(
            "candidate_evidence_slice_sha256"
        )
        != candidate_evidence_slice_sha256
        or terminal_evidence.artifact.descriptor.logical_identity.get("candidate_selection_sha256")
        != selection_sha256
        or terminal_evidence.artifact.descriptor.logical_identity.get(
            "candidate_selection_projection_sha256"
        )
        != selection_projection_sha256
        or terminal_evidence.artifact.descriptor.logical_identity.get(
            "global_evidence_projection_sha256"
        )
        != evidence_projection_sha256
        or terminal_evidence.artifact.descriptor.logical_identity.get(
            "model_package_projection_sha256"
        )
        != model_package_projection_sha256
        or terminal_evidence.artifact.descriptor.logical_identity.get("finalist_model_binding")
        != global_binding
        or terminal_evidence.artifact.descriptor.logical_identity.get(
            "finalist_model_binding_sha256"
        )
        != binding_sha256
        or model_logical.get("candidate_selection_sha256") != selection_sha256
        or model_logical.get("candidate_selection_projection_sha256") != selection_projection_sha256
        or model_logical.get("global_evidence_projection_sha256") != evidence_projection_sha256
        or model_logical.get("model_package_projection") != dict(model_projection.projection)
        or model_logical.get("model_package_projection_sha256") != model_package_projection_sha256
        or model_logical.get("finalist_model_binding") != global_binding
        or model_logical.get("finalist_model_binding_sha256") != binding_sha256
        or global_evidence.artifact.descriptor.logical_identity.get(
            "candidate_selection_sha256_by_key"
        )
        != dict(global_projection.candidate_selection_sha256_by_key)
        or global_evidence.artifact.descriptor.logical_identity.get(
            "candidate_evidence_slice_sha256_by_key"
        )
        != dict(global_projection.candidate_evidence_slice_sha256_by_key)
        or global_evidence.artifact.descriptor.logical_identity.get(
            "candidate_oos_trade_record_count_by_key"
        )
        != dict(global_projection.candidate_oos_trade_record_count_by_key)
        or oos_evidence.artifact.descriptor.record_count != expected_oos_trade_record_count
        or oos_evidence.artifact.descriptor.logical_identity.get("row_count")
        != expected_oos_trade_record_count
        or global_evidence.artifact.descriptor.logical_identity.get(
            "candidate_selection_projection_sha256_by_key"
        )
        != dict(global_projection.candidate_selection_projection_sha256_by_key)
        or global_evidence.artifact.descriptor.logical_identity.get(
            "global_evidence_projection_sha256"
        )
        != evidence_projection_sha256
        or global_evidence.artifact.descriptor.logical_identity.get(
            "model_package_projection_sha256_by_key", {}
        ).get(key)
        != model_package_projection_sha256
        or global_evidence.artifact.descriptor.logical_identity.get("finalist_model_binding_by_key")
        != {
            candidate: (
                None
                if (binding := global_projection.finalist_bindings.get(candidate)) is None
                else dict(binding)
            )
            for candidate in global_projection.candidate_selections
        }
        or global_evidence.artifact.descriptor.logical_identity.get(
            "finalist_model_binding_sha256_by_key"
        )
        != dict(global_projection.finalist_model_binding_sha256_by_key)
        or canonical_sha256(global_document) != global_evidence.artifact.sha256
    ):
        raise BarStateRegistryDriftError("reused terminal document/summary drift")
    return BarStateReuseValidationReport(
        research_run_attempt_id=attempt_id,
        reused_attempt_id=reused_id,
        candidate_key=key,
        artifact_count=len(evidence),
        role_counts=tuple(sorted(role_counts.items())),
        artifacts=tuple(evidence),
        artifact_link_manifest_sha256=links_sha256,
        compact_summary=compact,
        candidate_evidence_slice_sha256=candidate_evidence_slice_sha256,
        candidate_selection_sha256=selection_sha256,
        candidate_selection_projection_sha256=selection_projection_sha256,
        decision_label=decision_label,
        finalist_model_binding_sha256=binding_sha256,
        global_evidence_projection_sha256=evidence_projection_sha256,
        model_package_projection_sha256=model_package_projection_sha256,
        trial_status=trial_status,
        global_artifact_identity_sha256=(global_evidence.artifact.descriptor.identity_sha256),
        global_artifact_sha256=global_evidence.artifact.sha256,
        global_document_sha256=canonical_sha256(global_document),
        finalist_keys=frozen_finalists,
        terminal_artifact_sha256=terminal_evidence.artifact.sha256,
    )
