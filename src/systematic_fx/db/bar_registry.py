"""Governed generic-ledger registration for the isolated bar-pattern v1 campaign.

The frozen campaign is stored as one generic ``campaign``, one catalog
``experiment``, and exactly 216 ``STRATEGY_VARIANT`` trials.  Run execution uses
the generic ``RunSpec`` and ``research_run_attempts`` ledger, with migration
0022 adding bar-specific immutability and cross-ledger consistency triggers.

The module deliberately reports, rather than hides, the residual limitations
of using the generic schema.  In particular, the trial-to-artifact relationship
is represented in JSONB; the authoritative foreign-key edge is therefore the
one from ``research_run_attempts.result_artifact_id`` to ``artifacts``.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from functools import wraps
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final, Literal, ParamSpec, TypeVar
from urllib.parse import unquote, urlsplit

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg import IsolationLevel
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.backtest.economics import (
    BASE_MONTHLY_FIXED_POOL_USD,
    EXPECTED_MONTHLY_ROUND_TRIPS,
    TICK_VALUE_USD,
)
from systematic_fx.db.postgres_retry import retry_serialization_failures
from systematic_fx.db.run_registry import (
    RunAttemptReservation,
    RunAttemptState,
    RunSpecRegistration,
    register_run_spec,
)
from systematic_fx.research.bar_artifacts import (
    BarArtifactDescriptor,
    OpenVerifiedBarArtifact,
    PublishedBarArtifact,
    arrow_schema_sha256,
    open_verified_bar_artifact,
    publish_bar_json_artifact,
)
from systematic_fx.research.bar_config import (
    ALLOCATED_VARIANT_COUNT,
    BAR_PATTERN_CAMPAIGN_KEY,
    BAR_PATTERN_QUALIFICATION_STATUS,
    BAR_PATTERN_SCREENING_ONLY,
    BAR_SOURCE_MANIFEST_SHA256,
    BARRIER_TICKS,
    CAMPAIGN_VARIANT_BUDGET,
    BarPatternCandidate,
    BarPatternResearchConfig,
)
from systematic_fx.research.bar_discovery import (
    DISCOVERY_EVIDENCE_SCHEMA,
    DISCOVERY_RESULT_SCHEMA,
    DISCOVERY_SPOOL_VERSION,
)
from systematic_fx.research.bar_pipeline import BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.run_spec import RunSpec
from systematic_fx.validation.bar_splits import BAR_SPLIT_SCHEMA, BarDateRange, BarSplitPlan

BAR_REGISTRATION_SCHEMA: Final = "systematic_fx.bar_pattern_registration.v1"
BAR_TRIAL_PARAMETERS_SCHEMA: Final = "systematic_fx.bar_pattern_trial_parameters.v1"
BAR_TERMINAL_RESULT_SCHEMA: Final = "systematic_fx.bar_pattern_terminal_result.v1"
BAR_TERMINAL_ARTIFACT_SCHEMA: Final = "systematic_fx.bar_terminal_result_artifact.v1"
BAR_TERMINAL_ARTIFACT_TYPE: Final = "bar_terminal_result"
BAR_TERMINAL_ARTIFACT_RECORD_COUNT: Final = len(BARRIER_TICKS) ** 2
BAR_CATALOG_EXPERIMENT_KEY: Final = (
    f"{BAR_PATTERN_CAMPAIGN_KEY}:experiment:frozen_candidate_catalog:v1"
)
BAR_FEATURE_VERSION: Final = "selected_contract_trade_ohlcv_bars_v1"
BAR_OUTCOME_VERSION: Final = "bar_first_touch_surface_v1"
BAR_COST_VERSION: Final = "bar_conservative_combined_cost_v1"
BAR_EXECUTION_VERSION: Final = "bar_next_open_stop_first_v1"
BAR_DISCOVERY_ENGINE_VERSION: Final = "bar_pattern_streaming_discovery_v1"
BAR_ELIGIBLE_CALENDAR_VERSION: Final = "bar_dataset_eligible_calendar_v1"
BAR_SPLIT_VERSION: Final = "bar_pattern_splits_v1"
BAR_RANDOM_SEED: Final = 0
BAR_DISCOVERY_LINEAGE_SCHEMA: Final = "systematic_fx.bar_discovery_lineage.v1"
BAR_GLOBAL_DISCOVERY_ARTIFACT_TYPE: Final = "bar_global_discovery_result"
BAR_EVIDENCE_MANIFEST_ARTIFACT_TYPE: Final = "bar_discovery_evidence_manifest"
BAR_EVIDENCE_MATCH_SHARD_ARTIFACT_TYPE: Final = "bar_discovery_matches_shard"
BAR_EVIDENCE_REPLAY_SHARD_ARTIFACT_TYPE: Final = "bar_discovery_replays_shard"
BAR_EVIDENCE_MATCH_SHARD_MAX_RECORDS: Final = 4_096
BAR_EVIDENCE_REPLAY_SHARD_MAX_RECORDS: Final = 256
RAW_SOURCE_MANIFEST_KEY: Final = "raw_mbp10_source_manifest_v1"
BAR_DATASET_MANIFEST_KEY: Final = "selected_trade_bar_dataset_manifest_v1"
_EVIDENCE_MATCH_BASE_SCHEMA: Final = pa.schema(
    (
        pa.field("candidate_key", pa.string(), nullable=False),
        pa.field("signal_id", pa.string(), nullable=False),
        pa.field("signal_date", pa.date32(), nullable=False),
        pa.field("decision_ns", pa.int64(), nullable=False),
        pa.field("block_key", pa.string(), nullable=False),
        pa.field("outcome_span_id", pa.int64(), nullable=False),
        pa.field("entry_status", pa.string(), nullable=False),
        pa.field("no_fill_reason", pa.string(), nullable=True),
        pa.field("entry_path_index", pa.int64(), nullable=True),
        pa.field("entry_1s_start_ns", pa.int64(), nullable=True),
        pa.field("replay_key", pa.string(), nullable=True),
        pa.field("evaluation_json", pa.large_string(), nullable=False),
    )
)
_EVIDENCE_REPLAY_BASE_SCHEMA: Final = pa.schema(
    (
        pa.field("replay_key", pa.string(), nullable=False),
        pa.field("decision_ns", pa.int64(), nullable=False),
        pa.field("bundle_json", pa.large_string(), nullable=False),
    )
)
BAR_REGISTRY_SCHEMA_LIMITATIONS: Final = (
    (
        "experiment_trials has no direct result_artifact_id foreign key; the authoritative "
        "artifact edge is research_run_attempts.result_artifact_id and the trial stores a "
        "compact JSON pointer"
    ),
    (
        "candidate definition hashes are application-enforced JSON identities rather than "
        "database-generated columns or CHECK constraints"
    ),
    (
        "the generic split-role enum cannot represent nested Discovery reporting blocks; "
        "those blocks remain in the immutable split artifact"
    ),
    "direct SQL can bypass the application-level exactly-216 catalog guard",
)

_SHA256 = re.compile(r"[0-9a-f]{64}")
_TERMINAL_STATUSES = frozenset({"SUCCEEDED", "REJECTED"})
_TERMINAL_FINAL_LABELS: Final = {
    "SUCCEEDED": frozenset({"DISCOVERY_FINALIST_SELECTED"}),
    "REJECTED": frozenset(
        {
            "SUPPORT_REJECT",
            "ECONOMIC_REJECT",
            "DISCOVERY_FINALIST_BUDGET_REJECTED",
        }
    ),
}
BAR_TERMINAL_ARTIFACT_SCHEMA_SHA256: Final = canonical_sha256(
    {
        "artifact_schema": BAR_TERMINAL_ARTIFACT_SCHEMA,
        "candidate_result_contract": "bar_candidate_discovery_result_v1",
        "record_count": BAR_TERMINAL_ARTIFACT_RECORD_COUNT,
        "record_count_semantics": "frozen_take_profit_by_stop_loss_cell_count",
        "required_fields": [
            "bar_dataset_manifest_sha256",
            "campaign_definition_sha256",
            "candidate_definition_sha256",
            "candidate_key",
            "candidate_result",
            "candidate_result_sha256",
            "compact_result",
            "compact_result_sha256",
            "decision_label",
            "final_label",
            "raw_source_manifest_sha256",
            "run_fingerprint",
            "schema",
            "split_plan_sha256",
            "trial_status",
        ],
    }
)
_REGISTRATION_SCHEMA_SHA256: Final = canonical_sha256(
    {
        "candidate_count": ALLOCATED_VARIANT_COUNT,
        "required": [
            "campaign_definition",
            "candidate_catalog",
            "code_commit",
            "schema",
            "schema_limitations",
            "bar_dataset_manifest_sha256",
            "raw_source_manifest_sha256",
            "split_plan",
        ],
        "schema": BAR_REGISTRATION_SCHEMA,
    }
)
_P = ParamSpec("_P")
_R = TypeVar("_R")

BarTrialStatus = Literal["SUCCEEDED", "REJECTED"]
BarDecisionLabel = Literal["DISCOVERY_FINALIST", "SCREENING_REJECT"]


class BarRegistryError(RuntimeError):
    """Bar research state could not be registered without ambiguity."""


class BarRegistryDriftError(BarRegistryError):
    """An existing generic-schema identity differs from the frozen request."""


class BarRegistryStateError(BarRegistryError):
    """A run or candidate trial was asked to make an invalid transition."""


class BarRegistryDatabaseError(BarRegistryError):
    """PostgreSQL rejected or could not complete a registry operation."""


def _translate_psycopg_errors(
    operation: str,
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    def decorate(function: Callable[_P, _R]) -> Callable[_P, _R]:
        @wraps(function)
        def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            try:
                return retry_serialization_failures(function, *args, **kwargs)
            except BarRegistryError:
                raise
            except psycopg.Error as error:
                raise BarRegistryDatabaseError(f"PostgreSQL {operation} failed") from error

        return wrapped

    return decorate


def _nonempty(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise BarRegistryError(f"{label} must be a canonical non-empty string")
    return value


def _sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise BarRegistryError(f"{label} must be a lowercase SHA-256")
    return value


def _raw_source_sha256(value: object) -> str:
    digest = _sha256(value, label="raw_source_manifest_sha256")
    if digest != BAR_SOURCE_MANIFEST_SHA256:
        raise BarRegistryError("raw_source_manifest_sha256 differs from the frozen v1 source")
    return digest


def _positive_integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise BarRegistryError(f"{label} must be a positive integer")
    return value


def _canonical_mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise BarRegistryError(f"{label} must be a mapping")
    try:
        detached = json.loads(canonical_json_bytes(value))
    except (TypeError, ValueError) as error:
        raise BarRegistryError(f"{label} must be strict canonical JSON") from error
    if not isinstance(detached, dict):
        raise BarRegistryError(f"{label} must encode a JSON object")
    return MappingProxyType(detached)


def _terminal_compact_result(
    value: object,
    *,
    candidate_key: str,
    candidate_definition_sha256: str,
    trial_status: str,
    decision_label: str,
) -> Mapping[str, object]:
    if trial_status not in _TERMINAL_STATUSES:
        raise BarRegistryError(f"trial_status must be one of {sorted(_TERMINAL_STATUSES)}")
    expected_decision = {
        "SUCCEEDED": "DISCOVERY_FINALIST",
        "REJECTED": "SCREENING_REJECT",
    }[trial_status]
    if decision_label != expected_decision:
        raise BarRegistryError(f"{trial_status} requires decision_label {expected_decision}")
    compact = _canonical_mapping(value, label="compact_result")
    if not compact:
        raise BarRegistryError("compact_result must not be empty")
    required = {
        "candidate_definition_sha256": candidate_definition_sha256,
        "candidate_key": candidate_key,
        "decision_label": decision_label,
    }
    mismatches = [key for key, expected in required.items() if compact.get(key) != expected]
    if mismatches:
        raise BarRegistryError(
            "compact_result does not bind terminal identity fields: "
            + ", ".join(sorted(mismatches))
        )
    final_label = compact.get("final_label")
    if final_label not in _TERMINAL_FINAL_LABELS[trial_status]:
        raise BarRegistryError(
            f"compact_result.final_label is invalid for trial_status {trial_status}"
        )
    for key in (
        "discovery_result_sha256",
        "evidence_artifact_identity_sha256",
        "evidence_identity_sha256",
        "evidence_manifest_sha256",
        "global_result_artifact_identity_sha256",
        "global_result_artifact_sha256",
    ):
        _sha256(compact.get(key), label=f"compact_result.{key}")
    if len(canonical_json_bytes(compact)) > 65_536:
        raise BarRegistryError("compact_result exceeds the 64 KiB registry limit")
    return compact


def _row_or_error(row: Mapping[str, Any] | None, *, label: str) -> Mapping[str, Any]:
    if row is None:
        raise BarRegistryError(f"{label} does not exist")
    return row


def _assert_fields(
    *,
    label: str,
    row: Mapping[str, Any],
    expected: Mapping[str, object],
) -> None:
    mismatches = [field for field, value in expected.items() if row.get(field) != value]
    if mismatches:
        raise BarRegistryDriftError(
            f"{label} immutable content drift in fields: {', '.join(sorted(mismatches))}"
        )


def _set_serializable(connection: psycopg.Connection[dict[str, Any]]) -> None:
    connection.isolation_level = IsolationLevel.SERIALIZABLE


@dataclass(frozen=True, slots=True)
class BarArtifactRegistrationReport:
    artifact_id: int
    artifact_key: str
    artifact_sha256: str
    created: bool


@dataclass(frozen=True, slots=True)
class BarCampaignRegistrationReport:
    dataset_id: int
    campaign_id: int
    experiment_id: int
    registration_artifact_id: int
    candidate_trial_ids: tuple[int, ...]
    created_campaign: bool
    created_experiment: bool
    created_trials: int
    created_splits: int
    created_days: int
    schema_limitations: tuple[str, ...] = BAR_REGISTRY_SCHEMA_LIMITATIONS


@dataclass(frozen=True, slots=True)
class BarTerminalResult:
    """One final, compact candidate result backed by an immutable full artifact."""

    candidate_key: str
    candidate_definition_sha256: str
    run_fingerprint: str
    research_run_attempt_id: int
    trial_status: BarTrialStatus
    decision_label: BarDecisionLabel
    compact_result: Mapping[str, object]
    artifact: PublishedBarArtifact

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "candidate_key", _nonempty(self.candidate_key, label="candidate_key")
        )
        object.__setattr__(
            self,
            "candidate_definition_sha256",
            _sha256(self.candidate_definition_sha256, label="candidate_definition_sha256"),
        )
        object.__setattr__(
            self,
            "run_fingerprint",
            _sha256(self.run_fingerprint, label="run_fingerprint"),
        )
        object.__setattr__(
            self,
            "research_run_attempt_id",
            _positive_integer(
                self.research_run_attempt_id,
                label="research_run_attempt_id",
            ),
        )
        compact = _terminal_compact_result(
            self.compact_result,
            candidate_key=self.candidate_key,
            candidate_definition_sha256=self.candidate_definition_sha256,
            trial_status=self.trial_status,
            decision_label=self.decision_label,
        )
        object.__setattr__(self, "compact_result", compact)
        if not isinstance(self.artifact, PublishedBarArtifact):
            raise BarRegistryError("artifact must be a PublishedBarArtifact")

    @property
    def final_label(self) -> str:
        return str(self.compact_result["final_label"])


@dataclass(frozen=True, slots=True)
class BarTerminalRegistrationReport:
    experiment_trial_id: int
    research_run_spec_id: int
    research_run_attempt_id: int
    result_artifact_id: int
    attempt_status: Literal["SUCCEEDED"]
    trial_status: BarTrialStatus
    decision_label: BarDecisionLabel
    created_artifact: bool
    transitioned_attempt: bool
    transitioned_trial: bool
    schema_limitations: tuple[str, ...] = BAR_REGISTRY_SCHEMA_LIMITATIONS


@dataclass(frozen=True, slots=True)
class BarReusedCandidateReport:
    candidate_key: str
    run_fingerprint: str
    duplicate_attempt_id: int | None
    reused_attempt_id: int
    trial_status: BarTrialStatus
    final_label: str
    terminal_artifact_sha256: str


@dataclass(frozen=True, slots=True)
class BarReuseValidationReport:
    candidates: tuple[BarReusedCandidateReport, ...]
    global_result_artifact_sha256: str
    global_result_artifact_identity_sha256: str
    evidence_manifest_sha256: str
    evidence_artifact_identity_sha256: str
    evidence_identity_sha256: str
    finalist_keys: tuple[str, ...]
    final_label_counts: tuple[tuple[str, int], ...]


def build_bar_registration_document(
    config: BarPatternResearchConfig,
    split_plan: BarSplitPlan,
    *,
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
    code_commit: str,
) -> dict[str, object]:
    """Build the complete preregistration evidence without reading outcomes."""

    if not isinstance(config, BarPatternResearchConfig):
        raise BarRegistryError("config must be a BarPatternResearchConfig")
    if not isinstance(split_plan, BarSplitPlan):
        raise BarRegistryError("split_plan must be a BarSplitPlan")
    raw_source_hash = _raw_source_sha256(raw_source_manifest_sha256)
    bar_dataset_hash = _sha256(
        bar_dataset_manifest_sha256,
        label="bar_dataset_manifest_sha256",
    )
    commit = _nonempty(code_commit, label="code_commit")
    candidates = [
        {
            "candidate_definition": candidate.definition_payload(),
            "candidate_definition_sha256": candidate.definition_sha256,
            "candidate_key": candidate.candidate_key,
        }
        for candidate in config.candidates
    ]
    if (
        len(candidates) != ALLOCATED_VARIANT_COUNT
        or len({item["candidate_key"] for item in candidates}) != ALLOCATED_VARIANT_COUNT
    ):
        raise BarRegistryError("the frozen candidate catalog must contain exactly 216 identities")
    return {
        "campaign_definition": config.canonical_parameters(),
        "campaign_definition_sha256": config.definition_sha256,
        "candidate_catalog": candidates,
        "candidate_catalog_sha256": config.candidate_catalog_sha256,
        "code_commit": commit,
        "schema": BAR_REGISTRATION_SCHEMA,
        "schema_limitations": list(BAR_REGISTRY_SCHEMA_LIMITATIONS),
        "bar_dataset_manifest_sha256": bar_dataset_hash,
        "raw_source_manifest_sha256": raw_source_hash,
        "split_plan": split_plan.as_dict(),
        "split_plan_sha256": split_plan.sha256,
    }


def bar_registration_artifact_descriptor(
    config: BarPatternResearchConfig,
    split_plan: BarSplitPlan,
    *,
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
    code_commit: str,
) -> BarArtifactDescriptor:
    document = build_bar_registration_document(
        config,
        split_plan,
        raw_source_manifest_sha256=raw_source_manifest_sha256,
        bar_dataset_manifest_sha256=bar_dataset_manifest_sha256,
        code_commit=code_commit,
    )
    document_sha256 = canonical_sha256(document)
    return BarArtifactDescriptor(
        artifact_key=f"{BAR_PATTERN_CAMPAIGN_KEY}:registration:{document_sha256}",
        artifact_type="bar_registration",
        artifact_schema=BAR_REGISTRATION_SCHEMA,
        artifact_version=1,
        record_count=ALLOCATED_VARIANT_COUNT,
        schema_sha256=_REGISTRATION_SCHEMA_SHA256,
        source_manifest_sha256=bar_dataset_manifest_sha256,
        logical_identity={
            "bar_dataset_manifest_sha256": bar_dataset_manifest_sha256,
            "campaign_definition_sha256": config.definition_sha256,
            "candidate_catalog_sha256": config.candidate_catalog_sha256,
            "code_commit": code_commit,
            "document_sha256": document_sha256,
            "raw_source_manifest_sha256": raw_source_manifest_sha256,
            "split_plan_sha256": split_plan.sha256,
        },
        media_type="application/json",
        file_suffix=".json",
    )


def publish_bar_registration_artifact(
    project_root: Path,
    config: BarPatternResearchConfig,
    split_plan: BarSplitPlan,
    *,
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
    code_commit: str,
) -> PublishedBarArtifact:
    document = build_bar_registration_document(
        config,
        split_plan,
        raw_source_manifest_sha256=raw_source_manifest_sha256,
        bar_dataset_manifest_sha256=bar_dataset_manifest_sha256,
        code_commit=code_commit,
    )
    descriptor = bar_registration_artifact_descriptor(
        config,
        split_plan,
        raw_source_manifest_sha256=raw_source_manifest_sha256,
        bar_dataset_manifest_sha256=bar_dataset_manifest_sha256,
        code_commit=code_commit,
    )
    return publish_bar_json_artifact(project_root, descriptor, document)


def candidate_trial_parameters(
    config: BarPatternResearchConfig,
    split_plan: BarSplitPlan,
    candidate: BarPatternCandidate,
    *,
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
) -> dict[str, object]:
    """Record every candidate-specific and shared variable in one trial row."""

    if candidate not in config.candidates:
        raise BarRegistryError("candidate is not part of the frozen config catalog")
    return {
        "campaign_definition": config.canonical_parameters(),
        "campaign_definition_sha256": config.definition_sha256,
        "candidate_definition": candidate.definition_payload(),
        "candidate_definition_sha256": candidate.definition_sha256,
        "candidate_key": candidate.candidate_key,
        "candidate_catalog_sha256": config.candidate_catalog_sha256,
        "schema": BAR_TRIAL_PARAMETERS_SCHEMA,
        "bar_dataset_manifest_sha256": _sha256(
            bar_dataset_manifest_sha256,
            label="bar_dataset_manifest_sha256",
        ),
        "raw_source_manifest_sha256": _raw_source_sha256(raw_source_manifest_sha256),
        "split_plan": split_plan.as_dict(),
        "split_plan_schema": BAR_SPLIT_SCHEMA,
        "split_plan_sha256": split_plan.sha256,
    }


def _terminal_candidate_result(
    value: object,
    *,
    config: BarPatternResearchConfig,
    candidate: BarPatternCandidate,
    final_label: str,
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
    split_plan_sha256: str,
    compact_result: Mapping[str, object],
) -> Mapping[str, object]:
    document = _canonical_mapping(value, label="candidate_result")
    required = {
        "candidate_definition": candidate.definition_payload(),
        "candidate_definition_sha256": candidate.definition_sha256,
        "candidate_key": candidate.candidate_key,
        "final_label": final_label,
    }
    mismatches = [key for key, expected in required.items() if document.get(key) != expected]
    decision = document.get("decision")
    expected_discovery_decision = {
        "SUPPORT_REJECT": "SUPPORT_REJECT",
        "ECONOMIC_REJECT": "ECONOMIC_REJECT",
        "DISCOVERY_FINALIST_SELECTED": "DISCOVERY_FINALIST",
        "DISCOVERY_FINALIST_BUDGET_REJECTED": "DISCOVERY_FINALIST",
    }[final_label]
    if not isinstance(decision, Mapping) or decision.get("label") != expected_discovery_decision:
        mismatches.append("decision.label")
    if mismatches:
        raise BarRegistryError(
            "candidate_result does not bind the frozen candidate/final decision: "
            + ", ".join(sorted(mismatches))
        )
    support = document.get("support")
    if not isinstance(support, Mapping) or any(
        support.get(key) != expected
        for key, expected in {
            "candidate_key": candidate.candidate_key,
            "direction": candidate.direction.value,
            "timeframe_seconds": candidate.timeframe_seconds,
        }.items()
    ):
        raise BarRegistryError("candidate_result support identity drift")
    if any(
        decision.get(key) != expected
        for key, expected in {
            "candidate_key": candidate.candidate_key,
            "direction": candidate.direction.value,
        }.items()
    ):
        raise BarRegistryError("candidate_result decision identity drift")

    scenario_ids = tuple(item.scenario_id for item in config.execution_scenarios)
    economics = document.get("economics")
    if not isinstance(economics, Sequence) or isinstance(economics, (str, bytes)):
        raise BarRegistryError("candidate_result economics must be an array")
    if (
        tuple(item.get("scenario_id") if isinstance(item, Mapping) else None for item in economics)
        != scenario_ids
    ):
        raise BarRegistryError("candidate_result economics scenarios are incomplete or unordered")
    expected_cells = {
        (take_profit, stop_loss) for take_profit in BARRIER_TICKS for stop_loss in BARRIER_TICKS
    }
    for scenario_id, scenario in zip(scenario_ids, economics, strict=True):
        if not isinstance(scenario, Mapping):  # pragma: no cover - guarded above
            raise BarRegistryError("candidate_result scenario must be an object")
        cells = scenario.get("cells")
        if not isinstance(cells, Sequence) or isinstance(cells, (str, bytes)):
            raise BarRegistryError("candidate_result scenario cells must be an array")
        identities: set[tuple[object, object]] = set()
        for cell in cells:
            if not isinstance(cell, Mapping):
                raise BarRegistryError("candidate_result cells must be objects")
            identity = (cell.get("take_profit_ticks"), cell.get("stop_loss_ticks"))
            identities.add(identity)
            blocks = cell.get("blocks")
            if (
                cell.get("scenario_id") != scenario_id
                or cell.get("direction") != candidate.direction.value
                or not isinstance(blocks, Sequence)
                or isinstance(blocks, (str, bytes))
                or len(blocks) != 4
            ):
                raise BarRegistryError("candidate_result cell identity or block surface drift")
        if len(cells) != BAR_TERMINAL_ARTIFACT_RECORD_COUNT or identities != expected_cells:
            raise BarRegistryError("candidate_result must contain each frozen 484-cell surface")

    lineage = document.get("discovery_lineage")
    if not isinstance(lineage, Mapping):
        raise BarRegistryError("candidate_result.discovery_lineage must be an object")
    expected_lineage = {
        "candidate_catalog_sha256": config.candidate_catalog_sha256,
        "config_file_sha256": config.sha256,
        "config_semantic_sha256": config.semantic_sha256,
        "dataset_manifest_sha256": bar_dataset_manifest_sha256,
        "discovery_result_schema": DISCOVERY_RESULT_SCHEMA,
        "outcome_span_policy_sha256": BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
        "raw_source_manifest_sha256": raw_source_manifest_sha256,
        "schema": BAR_DISCOVERY_LINEAGE_SCHEMA,
        "split_plan_sha256": split_plan_sha256,
    }
    lineage_mismatches = [
        key for key, expected in expected_lineage.items() if lineage.get(key) != expected
    ]
    for key in (
        "code_snapshot_artifact_identity_sha256",
        "code_snapshot_sha256",
        "dataset_handoff_sha256",
        "discovery_result_sha256",
        "evidence_artifact_identity_sha256",
        "evidence_identity_sha256",
        "evidence_manifest_sha256",
        "global_result_artifact_identity_sha256",
        "global_result_artifact_sha256",
        "postgres_migrations_sha256",
    ):
        try:
            _sha256(lineage.get(key), label=f"discovery_lineage.{key}")
        except BarRegistryError:
            lineage_mismatches.append(key)
    for key in (
        "evidence_matched_record_count",
        "evidence_replay_record_count",
        "evidence_shard_count",
    ):
        value = lineage.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            lineage_mismatches.append(key)
    if lineage.get("global_result_artifact_sha256") != lineage.get("discovery_result_sha256"):
        lineage_mismatches.append("global_result_artifact_sha256")
    if lineage_mismatches:
        raise BarRegistryError(
            "candidate_result discovery lineage drift: "
            + ", ".join(sorted(set(lineage_mismatches)))
        )

    compact_projection = {
        "candidate_definition_sha256": candidate.definition_sha256,
        "candidate_key": candidate.candidate_key,
        "decision_trigger_count": document.get("decision_trigger_count"),
        "discovery_result_sha256": lineage["discovery_result_sha256"],
        "distinct_signal_day_count": support.get("distinct_signal_day_count"),
        "evidence_artifact_identity_sha256": lineage["evidence_artifact_identity_sha256"],
        "evidence_identity_sha256": lineage["evidence_identity_sha256"],
        "evidence_manifest_sha256": lineage["evidence_manifest_sha256"],
        "final_label": final_label,
        "global_result_artifact_identity_sha256": lineage["global_result_artifact_identity_sha256"],
        "global_result_artifact_sha256": lineage["global_result_artifact_sha256"],
        "matched_signal_count": document.get("matched_signal_count"),
        "moderate_ev_ticks": decision.get("overall_moderate_ev_ticks"),
        "positive_component_size": decision.get("positive_component_size"),
        "qualification_status": BAR_PATTERN_QUALIFICATION_STATUS,
        "raw_signal_count": support.get("raw_signal_count"),
        "rejection_reasons": decision.get("rejection_reasons"),
        "screening_only": BAR_PATTERN_SCREENING_ONLY,
        "selected_buy_sell_loss_formula": decision.get("selected_buy_sell_loss_formula"),
        "selected_stop_loss_ticks": decision.get("selected_stop_loss_ticks"),
        "selected_take_profit_ticks": decision.get("selected_take_profit_ticks"),
    }
    projection_mismatches = [
        key for key, expected in compact_projection.items() if compact_result.get(key) != expected
    ]
    if projection_mismatches:
        raise BarRegistryError(
            "compact_result is not the exact candidate_result projection: "
            + ", ".join(sorted(projection_mismatches))
        )
    return document


def _bar_terminal_artifact_contract(
    config: BarPatternResearchConfig,
    *,
    candidate_key: str,
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
    split_plan_sha256: str,
    run_fingerprint: str,
    trial_status: BarTrialStatus,
    decision_label: BarDecisionLabel,
    compact_result: Mapping[str, object],
    candidate_result: Mapping[str, object],
) -> tuple[BarArtifactDescriptor, dict[str, object]]:
    if not isinstance(config, BarPatternResearchConfig):
        raise BarRegistryError("config must be a BarPatternResearchConfig")
    try:
        candidate = config.candidate(candidate_key)
    except KeyError as error:
        raise BarRegistryError(str(error)) from error
    raw_hash = _raw_source_sha256(raw_source_manifest_sha256)
    dataset_hash = _sha256(
        bar_dataset_manifest_sha256,
        label="bar_dataset_manifest_sha256",
    )
    split_hash = _sha256(split_plan_sha256, label="split_plan_sha256")
    fingerprint = _sha256(run_fingerprint, label="run_fingerprint")
    compact = _terminal_compact_result(
        compact_result,
        candidate_key=candidate.candidate_key,
        candidate_definition_sha256=candidate.definition_sha256,
        trial_status=trial_status,
        decision_label=decision_label,
    )
    final_label = str(compact["final_label"])
    full_result = _terminal_candidate_result(
        candidate_result,
        config=config,
        candidate=candidate,
        final_label=final_label,
        raw_source_manifest_sha256=raw_hash,
        bar_dataset_manifest_sha256=dataset_hash,
        split_plan_sha256=split_hash,
        compact_result=compact,
    )
    compact_sha256 = canonical_sha256(compact)
    candidate_result_sha256 = canonical_sha256(full_result)
    logical_identity = {
        "bar_dataset_manifest_sha256": dataset_hash,
        "campaign_definition_sha256": config.definition_sha256,
        "candidate_definition_sha256": candidate.definition_sha256,
        "candidate_key": candidate.candidate_key,
        "candidate_result_sha256": candidate_result_sha256,
        "compact_result_sha256": compact_sha256,
        "decision_label": decision_label,
        "final_label": final_label,
        "raw_source_manifest_sha256": raw_hash,
        "run_fingerprint": fingerprint,
        "split_plan_sha256": split_hash,
        "trial_status": trial_status,
    }
    descriptor = BarArtifactDescriptor(
        artifact_key=(
            f"{BAR_PATTERN_CAMPAIGN_KEY}:terminal:{candidate.candidate_key}:"
            f"{fingerprint}:{candidate_result_sha256}"
        ),
        artifact_type=BAR_TERMINAL_ARTIFACT_TYPE,
        artifact_schema=BAR_TERMINAL_ARTIFACT_SCHEMA,
        artifact_version=1,
        record_count=BAR_TERMINAL_ARTIFACT_RECORD_COUNT,
        schema_sha256=BAR_TERMINAL_ARTIFACT_SCHEMA_SHA256,
        source_manifest_sha256=dataset_hash,
        logical_identity=logical_identity,
        media_type="application/json",
        file_suffix=".json",
    )
    document = {
        "bar_dataset_manifest_sha256": dataset_hash,
        "campaign_definition_sha256": config.definition_sha256,
        "candidate_definition_sha256": candidate.definition_sha256,
        "candidate_key": candidate.candidate_key,
        "candidate_result": dict(full_result),
        "candidate_result_sha256": candidate_result_sha256,
        "compact_result": dict(compact),
        "compact_result_sha256": compact_sha256,
        "decision_label": decision_label,
        "final_label": final_label,
        "raw_source_manifest_sha256": raw_hash,
        "run_fingerprint": fingerprint,
        "schema": BAR_TERMINAL_ARTIFACT_SCHEMA,
        "split_plan_sha256": split_hash,
        "trial_status": trial_status,
    }
    return descriptor, document


def bar_terminal_result_artifact_descriptor(
    config: BarPatternResearchConfig,
    *,
    candidate_key: str,
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
    split_plan_sha256: str,
    run_fingerprint: str,
    trial_status: BarTrialStatus,
    decision_label: BarDecisionLabel,
    compact_result: Mapping[str, object],
    candidate_result: Mapping[str, object],
) -> BarArtifactDescriptor:
    """Return the exact full-result artifact descriptor for one terminal candidate."""

    descriptor, _ = _bar_terminal_artifact_contract(
        config,
        candidate_key=candidate_key,
        raw_source_manifest_sha256=raw_source_manifest_sha256,
        bar_dataset_manifest_sha256=bar_dataset_manifest_sha256,
        split_plan_sha256=split_plan_sha256,
        run_fingerprint=run_fingerprint,
        trial_status=trial_status,
        decision_label=decision_label,
        compact_result=compact_result,
        candidate_result=candidate_result,
    )
    return descriptor


def publish_bar_terminal_result_artifact(
    project_root: Path,
    config: BarPatternResearchConfig,
    *,
    candidate_key: str,
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
    split_plan_sha256: str,
    run_fingerprint: str,
    trial_status: BarTrialStatus,
    decision_label: BarDecisionLabel,
    compact_result: Mapping[str, object],
    candidate_result: Mapping[str, object],
) -> PublishedBarArtifact:
    """Publish the only JSON shape accepted by terminal candidate registration."""

    descriptor, document = _bar_terminal_artifact_contract(
        config,
        candidate_key=candidate_key,
        raw_source_manifest_sha256=raw_source_manifest_sha256,
        bar_dataset_manifest_sha256=bar_dataset_manifest_sha256,
        split_plan_sha256=split_plan_sha256,
        run_fingerprint=run_fingerprint,
        trial_status=trial_status,
        decision_label=decision_label,
        compact_result=compact_result,
        candidate_result=candidate_result,
    )
    return publish_bar_json_artifact(project_root, descriptor, document)


def _ensure_artifact(
    connection: psycopg.Connection[dict[str, Any]],
    artifact: PublishedBarArtifact,
    *,
    producer_job_id: int | None = None,
) -> tuple[int, bool]:
    metadata = artifact.database_metadata()
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
            artifact.descriptor.artifact_key,
            artifact.descriptor.artifact_type,
            artifact.uri,
            artifact.sha256,
            artifact.byte_size,
            artifact.descriptor.media_type,
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
        (artifact.descriptor.artifact_key, artifact.uri),
    ).fetchall()
    if len(rows) != 1:
        raise BarRegistryDriftError("artifact key and URI do not resolve to exactly one row")
    row = rows[0]
    _assert_fields(
        label=f"artifact {artifact.descriptor.artifact_key}",
        row=row,
        expected={
            "artifact_key": artifact.descriptor.artifact_key,
            "artifact_type": artifact.descriptor.artifact_type,
            "uri": artifact.uri,
            "sha256": artifact.sha256,
            "byte_size": artifact.byte_size,
            "media_type": artifact.descriptor.media_type,
            "producer_job_id": producer_job_id,
            "metadata": metadata,
        },
    )
    return int(row["artifact_id"]), created


@_translate_psycopg_errors("bar-artifact registration")
def register_published_bar_artifact(
    database_url: str,
    project_root: Path,
    artifact: PublishedBarArtifact,
    *,
    producer_job_id: int | None = None,
) -> BarArtifactRegistrationReport:
    """Register or exactly verify one safely opened immutable artifact."""

    url = _nonempty(database_url, label="database_url")
    if producer_job_id is not None:
        producer_job_id = _positive_integer(producer_job_id, label="producer_job_id")
    with psycopg.connect(url, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with connection.transaction(), open_verified_bar_artifact(project_root, artifact):
            artifact_id, created = _ensure_artifact(
                connection,
                artifact,
                producer_job_id=producer_job_id,
            )
    return BarArtifactRegistrationReport(
        artifact_id=artifact_id,
        artifact_key=artifact.descriptor.artifact_key,
        artifact_sha256=artifact.sha256,
        created=created,
    )


def _ensure_campaign(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    dataset_id: int,
    config: BarPatternResearchConfig,
    split_plan: BarSplitPlan,
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
    code_commit: str,
) -> tuple[int, bool]:
    split_policy = {
        "bar_dataset_manifest_sha256": bar_dataset_manifest_sha256,
        "raw_source_manifest_sha256": raw_source_manifest_sha256,
        "split_plan": split_plan.as_dict(),
        "split_plan_sha256": split_plan.sha256,
    }
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.campaigns
            (campaign_key, dataset_id, name, status, selected_start_date,
             selected_end_date, roll_cutoff_date, data_manifest_sha256,
             feature_version, outcome_version, cost_model_version,
             execution_model_version, code_commit, config_sha256, split_policy,
             trial_budget, finalist_budget, frozen_at)
        VALUES (%s, %s, %s, 'FROZEN', %s, %s, NULL, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, 10, statement_timestamp())
        ON CONFLICT (campaign_key) DO NOTHING
        RETURNING campaign_id
        """,
        (
            BAR_PATTERN_CAMPAIGN_KEY,
            dataset_id,
            "Frozen multi-timeframe OHLC bar-pattern screening",
            split_plan.eligible_dates[0],
            split_plan.eligible_dates[-1],
            bar_dataset_manifest_sha256,
            BAR_FEATURE_VERSION,
            BAR_OUTCOME_VERSION,
            BAR_COST_VERSION,
            BAR_EXECUTION_VERSION,
            code_commit,
            config.definition_sha256,
            Jsonb(split_policy),
            CAMPAIGN_VARIANT_BUDGET,
        ),
    ).fetchone()
    created = inserted is not None
    row = connection.execute(
        """
        SELECT campaign_id, campaign_key, dataset_id, name, status,
               selected_start_date, selected_end_date, roll_cutoff_date,
               data_manifest_sha256, feature_version, outcome_version,
               cost_model_version, execution_model_version, code_commit,
               config_sha256, split_policy, trial_budget, finalist_budget,
               frozen_at
        FROM systematic_fx.campaigns
        WHERE campaign_key = %s
        FOR UPDATE
        """,
        (BAR_PATTERN_CAMPAIGN_KEY,),
    ).fetchone()
    row = _row_or_error(row, label=f"campaign {BAR_PATTERN_CAMPAIGN_KEY}")
    _assert_fields(
        label=f"campaign {BAR_PATTERN_CAMPAIGN_KEY}",
        row=row,
        expected={
            "campaign_key": BAR_PATTERN_CAMPAIGN_KEY,
            "dataset_id": dataset_id,
            "name": "Frozen multi-timeframe OHLC bar-pattern screening",
            "selected_start_date": split_plan.eligible_dates[0],
            "selected_end_date": split_plan.eligible_dates[-1],
            "roll_cutoff_date": None,
            "data_manifest_sha256": bar_dataset_manifest_sha256,
            "feature_version": BAR_FEATURE_VERSION,
            "outcome_version": BAR_OUTCOME_VERSION,
            "cost_model_version": BAR_COST_VERSION,
            "execution_model_version": BAR_EXECUTION_VERSION,
            "code_commit": code_commit,
            "config_sha256": config.definition_sha256,
            "split_policy": split_policy,
            "trial_budget": CAMPAIGN_VARIANT_BUDGET,
            "finalist_budget": 10,
        },
    )
    if row["status"] not in {"FROZEN", "RUNNING", "CLOSED"} or row["frozen_at"] is None:
        raise BarRegistryStateError("bar campaign is not in a valid frozen lifecycle state")
    return int(row["campaign_id"]), created


def _ensure_splits(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    campaign_id: int,
    ranges: Sequence[BarDateRange],
) -> tuple[dict[str, int], int]:
    created = 0
    for item in ranges:
        inserted = connection.execute(
            """
            INSERT INTO systematic_fx.campaign_splits
                (campaign_id, split_key, split_role, fold_number, start_date,
                 end_date, start_active_ordinal, end_active_ordinal,
                 purge_before_days, purge_after_days, result_visibility)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0, 0, %s)
            ON CONFLICT (campaign_id, split_key) DO NOTHING
            RETURNING campaign_split_id
            """,
            (
                campaign_id,
                item.split_key,
                item.role,
                item.fold_number,
                item.start_date,
                item.end_date,
                item.start_active_ordinal,
                item.end_active_ordinal,
                item.result_visibility,
            ),
        ).fetchone()
        created += inserted is not None
    rows = connection.execute(
        """
        SELECT campaign_split_id, split_key, split_role, fold_number, start_date,
               end_date, start_active_ordinal, end_active_ordinal,
               purge_before_days, purge_after_days, result_visibility, revealed_at
        FROM systematic_fx.campaign_splits
        WHERE campaign_id = %s
        ORDER BY start_active_ordinal
        FOR SHARE
        """,
        (campaign_id,),
    ).fetchall()
    if len(rows) != len(ranges):
        raise BarRegistryDriftError("campaign split count differs from the frozen split plan")
    by_key = {str(row["split_key"]): row for row in rows}
    if len(by_key) != len(rows):
        raise BarRegistryDriftError("campaign split keys are not unique")
    identities: dict[str, int] = {}
    for item in ranges:
        row = _row_or_error(by_key.get(item.split_key), label=f"split {item.split_key}")
        _assert_fields(
            label=f"split {item.split_key}",
            row=row,
            expected={
                "split_key": item.split_key,
                "split_role": item.role,
                "fold_number": item.fold_number,
                "start_date": item.start_date,
                "end_date": item.end_date,
                "start_active_ordinal": item.start_active_ordinal,
                "end_active_ordinal": item.end_active_ordinal,
                "purge_before_days": 0,
                "purge_after_days": 0,
                "result_visibility": item.result_visibility,
            },
        )
        if item.result_visibility == "SEALED" and row["revealed_at"] is not None:
            raise BarRegistryDriftError(f"sealed split {item.split_key} was already revealed")
        identities[item.split_key] = int(row["campaign_split_id"])
    return identities, created


def _split_for_ordinal(ranges: Sequence[BarDateRange], ordinal: int) -> BarDateRange:
    matches = [
        item for item in ranges if item.start_active_ordinal <= ordinal <= item.end_active_ordinal
    ]
    if len(matches) != 1:
        raise BarRegistryError(f"active ordinal {ordinal} is not covered by exactly one split")
    return matches[0]


def _ensure_campaign_days(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    dataset_id: int,
    campaign_id: int,
    split_plan: BarSplitPlan,
    split_ids: Mapping[str, int],
) -> int:
    created = 0
    expected: dict[date, tuple[int, int, Mapping[str, object]]] = {}
    for ordinal, calendar_date in enumerate(split_plan.eligible_dates, start=1):
        split = _split_for_ordinal(split_plan.ranges, ordinal)
        split_id = split_ids[split.split_key]
        metadata = {
            "calendar_schema": BAR_SPLIT_SCHEMA,
            "split_plan_sha256": split_plan.sha256,
        }
        inserted = connection.execute(
            """
            INSERT INTO systematic_fx.campaign_days
                (dataset_id, campaign_id, calendar_date, active_day_ordinal,
                 eligibility_status, exclusion_reason, campaign_split_id,
                 source_file_id, execution_instrument_id, is_roll_cutoff, metadata)
            VALUES (%s, %s, %s, %s, 'ELIGIBLE', NULL, %s, NULL, NULL, false, %s)
            ON CONFLICT (campaign_id, calendar_date) DO NOTHING
            RETURNING campaign_day_id
            """,
            (
                dataset_id,
                campaign_id,
                calendar_date,
                ordinal,
                split_id,
                Jsonb(metadata),
            ),
        ).fetchone()
        created += inserted is not None
        expected[calendar_date] = (ordinal, split_id, metadata)
    rows = connection.execute(
        """
        SELECT calendar_date, active_day_ordinal, eligibility_status,
               exclusion_reason, campaign_split_id, source_file_id,
               execution_instrument_id, is_roll_cutoff, metadata
        FROM systematic_fx.campaign_days
        WHERE campaign_id = %s
        ORDER BY active_day_ordinal
        FOR SHARE
        """,
        (campaign_id,),
    ).fetchall()
    if len(rows) != len(expected):
        raise BarRegistryDriftError("campaign day count differs from the eligible calendar")
    for row in rows:
        calendar_date = row["calendar_date"]
        identity = expected.get(calendar_date)
        if identity is None:
            raise BarRegistryDriftError("campaign contains a non-preregistered calendar day")
        ordinal, split_id, metadata = identity
        _assert_fields(
            label=f"campaign day {calendar_date}",
            row=row,
            expected={
                "active_day_ordinal": ordinal,
                "eligibility_status": "ELIGIBLE",
                "exclusion_reason": None,
                "campaign_split_id": split_id,
                "source_file_id": None,
                "execution_instrument_id": None,
                "is_roll_cutoff": False,
                "metadata": metadata,
            },
        )
    return created


def _ensure_experiment(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    campaign_id: int,
    config: BarPatternResearchConfig,
    split_plan: BarSplitPlan,
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
    registration_artifact_id: int,
    code_commit: str,
) -> tuple[int, bool]:
    parameters = config.canonical_parameters()
    feature_versions = {
        "bar_feature_version": BAR_FEATURE_VERSION,
        "candidate_catalog_sha256": config.candidate_catalog_sha256,
    }
    search_boundary = {
        "allocated_candidate_count": ALLOCATED_VARIANT_COUNT,
        "bar_dataset_manifest_sha256": bar_dataset_manifest_sha256,
        "campaign_definition_sha256": config.definition_sha256,
        "raw_source_manifest_sha256": raw_source_manifest_sha256,
        "result_driven_additions_allowed": False,
        "split_plan_sha256": split_plan.sha256,
        "unallocated_campaign_budget": CAMPAIGN_VARIANT_BUDGET - ALLOCATED_VARIANT_COUNT,
    }
    cost_assumptions = {"execution_scenarios": parameters["execution_scenarios"]}
    execution_assumptions = parameters["entry"]
    config_sha256 = canonical_sha256(
        {
            "cost_assumptions": cost_assumptions,
            "execution_assumptions": execution_assumptions,
            "feature_versions": feature_versions,
            "search_boundary": search_boundary,
        }
    )
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.experiments
            (experiment_key, campaign_id, pattern_id, parent_experiment_id,
             primary_family, status, hypothesis, direction, model_family,
             tick_size, tick_value, feature_definition_versions, search_boundary,
             cost_assumptions, execution_assumptions, trial_budget,
             trials_registered, registration_artifact_id, code_commit,
             config_sha256, frozen_at)
        VALUES (%s, %s, NULL, NULL, %s, 'FROZEN', %s, 'BOTH', %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, statement_timestamp())
        ON CONFLICT (experiment_key) DO NOTHING
        RETURNING experiment_id
        """,
        (
            BAR_CATALOG_EXPERIMENT_KEY,
            campaign_id,
            "FIXED_OHLC_BAR_PATTERN_CATALOG",
            "Fixed OHLC setup and trigger patterns have stable next-open first-touch economics",
            "RULE_BASED_FIXED_OHLC",
            Decimal("0.00005"),
            Decimal("6.25"),
            Jsonb(feature_versions),
            Jsonb(search_boundary),
            Jsonb(cost_assumptions),
            Jsonb(execution_assumptions),
            ALLOCATED_VARIANT_COUNT,
            ALLOCATED_VARIANT_COUNT,
            registration_artifact_id,
            code_commit,
            config_sha256,
        ),
    ).fetchone()
    created = inserted is not None
    row = connection.execute(
        """
        SELECT experiment_id, experiment_key, campaign_id, pattern_id,
               parent_experiment_id, primary_family, status, hypothesis,
               direction, model_family, tick_size, tick_value,
               feature_definition_versions, search_boundary, cost_assumptions,
               execution_assumptions, trial_budget, trials_registered,
               registration_artifact_id, code_commit, config_sha256, frozen_at
        FROM systematic_fx.experiments
        WHERE experiment_key = %s
        FOR UPDATE
        """,
        (BAR_CATALOG_EXPERIMENT_KEY,),
    ).fetchone()
    row = _row_or_error(row, label=f"experiment {BAR_CATALOG_EXPERIMENT_KEY}")
    _assert_fields(
        label=f"experiment {BAR_CATALOG_EXPERIMENT_KEY}",
        row=row,
        expected={
            "experiment_key": BAR_CATALOG_EXPERIMENT_KEY,
            "campaign_id": campaign_id,
            "pattern_id": None,
            "parent_experiment_id": None,
            "primary_family": "FIXED_OHLC_BAR_PATTERN_CATALOG",
            "hypothesis": (
                "Fixed OHLC setup and trigger patterns have stable next-open first-touch economics"
            ),
            "direction": "BOTH",
            "model_family": "RULE_BASED_FIXED_OHLC",
            "tick_size": Decimal("0.00005"),
            "tick_value": Decimal("6.25"),
            "feature_definition_versions": feature_versions,
            "search_boundary": search_boundary,
            "cost_assumptions": cost_assumptions,
            "execution_assumptions": execution_assumptions,
            "trial_budget": ALLOCATED_VARIANT_COUNT,
            "trials_registered": ALLOCATED_VARIANT_COUNT,
            "registration_artifact_id": registration_artifact_id,
            "code_commit": code_commit,
            "config_sha256": config_sha256,
        },
    )
    if row["status"] not in {"FROZEN", "RUNNING", "REJECTED", "RETAINED", "FAILED"}:
        raise BarRegistryStateError("catalog experiment has an invalid lifecycle status")
    if row["frozen_at"] is None:
        raise BarRegistryDriftError("catalog experiment is not frozen")
    return int(row["experiment_id"]), created


def _ensure_candidate_trials(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    experiment_id: int,
    config: BarPatternResearchConfig,
    split_plan: BarSplitPlan,
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
) -> tuple[tuple[int, ...], int]:
    expected: dict[str, tuple[dict[str, object], str]] = {}
    for candidate in config.candidates:
        parameters = candidate_trial_parameters(
            config,
            split_plan,
            candidate,
            raw_source_manifest_sha256=raw_source_manifest_sha256,
            bar_dataset_manifest_sha256=bar_dataset_manifest_sha256,
        )
        parameters_sha256 = canonical_sha256(parameters)
        expected[candidate.candidate_key] = (parameters, parameters_sha256)

    existing_rows = connection.execute(
        """
        SELECT trial_key
        FROM systematic_fx.experiment_trials
        WHERE experiment_id = %s
        ORDER BY trial_key
        FOR SHARE
        """,
        (experiment_id,),
    ).fetchall()
    existing_keys = [str(row["trial_key"]) for row in existing_rows]
    if len(existing_keys) != len(set(existing_keys)) or not set(existing_keys) <= set(expected):
        raise BarRegistryDriftError("catalog contains an unknown or duplicate candidate trial")

    created = 0
    existing_key_set = set(existing_keys)
    for candidate in config.candidates:
        if candidate.candidate_key in existing_key_set:
            continue
        parameters, parameters_sha256 = expected[candidate.candidate_key]
        inserted = connection.execute(
            """
            INSERT INTO systematic_fx.experiment_trials
                (experiment_id, trial_key, trial_type, status, parameters,
                 parameters_sha256, result_summary)
            VALUES (%s, %s, 'STRATEGY_VARIANT', 'REGISTERED', %s, %s, '{}'::jsonb)
            ON CONFLICT (experiment_id, trial_key) DO NOTHING
            RETURNING experiment_trial_id
            """,
            (
                experiment_id,
                candidate.candidate_key,
                Jsonb(parameters),
                parameters_sha256,
            ),
        ).fetchone()
        created += inserted is not None
    rows = connection.execute(
        """
        SELECT experiment_trial_id, trial_key, trial_type, status, parameters,
               parameters_sha256, research_run_spec_id
        FROM systematic_fx.experiment_trials
        WHERE experiment_id = %s
        ORDER BY trial_key
        FOR SHARE
        """,
        (experiment_id,),
    ).fetchall()
    if len(rows) != ALLOCATED_VARIANT_COUNT:
        raise BarRegistryDriftError("catalog experiment must contain exactly 216 candidate trials")
    trial_ids: list[int] = []
    observed_keys: set[str] = set()
    for row in rows:
        trial_key = str(row["trial_key"])
        identity = expected.get(trial_key)
        if identity is None or trial_key in observed_keys:
            raise BarRegistryDriftError("catalog contains an unknown or duplicate candidate trial")
        observed_keys.add(trial_key)
        parameters, parameters_sha256 = identity
        _assert_fields(
            label=f"candidate trial {trial_key}",
            row=row,
            expected={
                "trial_key": trial_key,
                "trial_type": "STRATEGY_VARIANT",
                "parameters": parameters,
                "parameters_sha256": parameters_sha256,
            },
        )
        if row["status"] not in {
            "REGISTERED",
            "RUNNING",
            "SUCCEEDED",
            "REJECTED",
            "FAILED",
            "CANCELLED",
        }:
            raise BarRegistryStateError(f"candidate trial {trial_key} has an invalid status")
        trial_ids.append(int(row["experiment_trial_id"]))
    if observed_keys != set(expected):
        raise BarRegistryDriftError("not every frozen candidate trial was registered")
    return tuple(trial_ids), created


@_translate_psycopg_errors("bar-campaign registration")
def register_bar_campaign(
    database_url: str,
    project_root: Path,
    *,
    dataset_key: str,
    config: BarPatternResearchConfig,
    split_plan: BarSplitPlan,
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
    code_commit: str,
    registration_artifact: PublishedBarArtifact,
) -> BarCampaignRegistrationReport:
    """Atomically freeze the campaign, split calendar, and all 216 variables."""

    url = _nonempty(database_url, label="database_url")
    dataset_key = _nonempty(dataset_key, label="dataset_key")
    raw_source_hash = _raw_source_sha256(raw_source_manifest_sha256)
    bar_dataset_hash = _sha256(
        bar_dataset_manifest_sha256,
        label="bar_dataset_manifest_sha256",
    )
    commit = _nonempty(code_commit, label="code_commit")
    expected_descriptor = bar_registration_artifact_descriptor(
        config,
        split_plan,
        raw_source_manifest_sha256=raw_source_hash,
        bar_dataset_manifest_sha256=bar_dataset_hash,
        code_commit=commit,
    )
    if registration_artifact.descriptor != expected_descriptor:
        raise BarRegistryError("registration_artifact descriptor differs from the frozen inputs")

    with psycopg.connect(url, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with (
            connection.transaction(),
            open_verified_bar_artifact(project_root, registration_artifact),
        ):
            dataset = connection.execute(
                """
                SELECT dataset_id, dataset_key, status
                FROM systematic_fx.datasets
                WHERE dataset_key = %s
                FOR SHARE
                """,
                (dataset_key,),
            ).fetchone()
            dataset = _row_or_error(dataset, label=f"dataset {dataset_key}")
            if dataset["status"] in {"REJECTED", "RETIRED"}:
                raise BarRegistryStateError(
                    f"dataset {dataset_key} is in terminal state {dataset['status']}"
                )
            dataset_id = int(dataset["dataset_id"])
            registration_artifact_id, _ = _ensure_artifact(
                connection,
                registration_artifact,
            )
            campaign_id, created_campaign = _ensure_campaign(
                connection,
                dataset_id=dataset_id,
                config=config,
                split_plan=split_plan,
                raw_source_manifest_sha256=raw_source_hash,
                bar_dataset_manifest_sha256=bar_dataset_hash,
                code_commit=commit,
            )
            split_ids, created_splits = _ensure_splits(
                connection,
                campaign_id=campaign_id,
                ranges=split_plan.ranges,
            )
            created_days = _ensure_campaign_days(
                connection,
                dataset_id=dataset_id,
                campaign_id=campaign_id,
                split_plan=split_plan,
                split_ids=split_ids,
            )
            experiment_id, created_experiment = _ensure_experiment(
                connection,
                campaign_id=campaign_id,
                config=config,
                split_plan=split_plan,
                raw_source_manifest_sha256=raw_source_hash,
                bar_dataset_manifest_sha256=bar_dataset_hash,
                registration_artifact_id=registration_artifact_id,
                code_commit=commit,
            )
            trial_ids, created_trials = _ensure_candidate_trials(
                connection,
                experiment_id=experiment_id,
                config=config,
                split_plan=split_plan,
                raw_source_manifest_sha256=raw_source_hash,
                bar_dataset_manifest_sha256=bar_dataset_hash,
            )
    return BarCampaignRegistrationReport(
        dataset_id=dataset_id,
        campaign_id=campaign_id,
        experiment_id=experiment_id,
        registration_artifact_id=registration_artifact_id,
        candidate_trial_ids=trial_ids,
        created_campaign=created_campaign,
        created_experiment=created_experiment,
        created_trials=created_trials,
        created_splits=created_splits,
        created_days=created_days,
    )


def _plain_json_object(value: object, *, label: str) -> dict[str, object]:
    return dict(_canonical_mapping(value, label=label))


def _expected_bar_policy_documents(
    config: BarPatternResearchConfig,
    candidate: BarPatternCandidate,
) -> dict[str, dict[str, object]]:
    campaign = config.canonical_parameters()
    entry = _plain_json_object(campaign["entry"], label="campaign entry policy")
    barriers = _plain_json_object(campaign["barriers"], label="campaign barrier policy")
    bars = _plain_json_object(campaign["bars"], label="campaign bar policy")
    market = _plain_json_object(campaign["market"], label="campaign market policy")
    scenarios = list(campaign["execution_scenarios"])
    signal = {
        "bars": bars,
        "candidate_definition": candidate.definition_payload(),
        "candidate_definition_sha256": candidate.definition_sha256,
        "candidate_key": candidate.candidate_key,
        "decision_visibility": "DISCOVERY_DECISION_DATES_ONLY",
        "schema": "systematic_fx.bar_signal_policy.v1",
    }
    entry_policy = {
        **entry,
        "candidate_timeframe_seconds": candidate.timeframe_seconds,
        "schema": "systematic_fx.bar_entry_policy.v1",
    }
    barrier = {
        **barriers,
        "grid_evaluation": "ALL_CELLS_NO_EARLY_PRUNING",
        "ticks_per_pip": market["ticks_per_pip"],
        "schema": "systematic_fx.bar_barrier_policy.v1",
    }
    terminal = {
        "holding_limit_policy": entry["holding_limit_policy"],
        "normal_market_closure_policy": entry["normal_market_closure_policy"],
        "outcome_span_policy_sha256": BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
        "split_boundary_policy": "TERMINAL_EXIT_AT_DISCOVERY_END",
        "terminal_boundary_types": entry["terminal_boundary_types"],
        "schema": "systematic_fx.bar_terminal_policy.v1",
    }
    cost = {
        "base_monthly_fixed_pool_usd": format(BASE_MONTHLY_FIXED_POOL_USD, "f"),
        "execution_scenarios": scenarios,
        "expected_monthly_round_trips": EXPECTED_MONTHLY_ROUND_TRIPS,
        "fixed_cost_allocation": "MONTHLY_POOL_DIVIDED_BY_ROUND_TRIPS_CEILING_TICKS",
        "market": market,
        "schema": "systematic_fx.bar_cost_policy.v1",
        "tick_value_usd": format(TICK_VALUE_USD, "f"),
    }
    selection = {
        "candidate_budget": campaign["candidate_budget"],
        "discovery_economic_gates": campaign["discovery_economic_gates"],
        "discovery_support_gates": campaign["discovery_support_gates"],
        "finalist_limit": campaign["holdout_gates"]["maximum_finalists"],
        "ranking_order": [
            "positive_block_count_desc",
            "worst_block_moderate_ev_desc",
            "overall_moderate_ev_desc",
            "moderate_maximum_drawdown_asc",
            "selected_stop_loss_ticks_asc",
            "selected_take_profit_ticks_asc",
            "candidate_key_asc",
        ],
        "schema": "systematic_fx.bar_selection_policy.v1",
    }
    evidence = {
        "candidate_summary": "FULL_3_SCENARIO_484_CELL_SURFACES",
        "evidence_schema": DISCOVERY_EVIDENCE_SCHEMA,
        "match_shard_max_records": BAR_EVIDENCE_MATCH_SHARD_MAX_RECORDS,
        "publication": "CONTENT_ADDRESSED_HELD_DIRFD",
        "record_kinds": ["matches", "replays"],
        "replay_shard_max_records": BAR_EVIDENCE_REPLAY_SHARD_MAX_RECORDS,
        "spool_version": DISCOVERY_SPOOL_VERSION,
        "schema": "systematic_fx.bar_evidence_policy.v1",
    }
    execution = {
        "barrier_policy": barrier,
        "entry_policy": entry_policy,
        "one_position_policy": "INDEPENDENT_PER_CANDIDATE_SCENARIO_TP_SL_CELL",
        "same_second_first_touch_policy": entry["same_second_first_touch_policy"],
        "terminal_policy": terminal,
        "schema": "systematic_fx.bar_execution_policy.v1",
    }
    outcome = {
        "barrier_policy": barrier,
        "one_second_path_source": "VERIFIED_SELECTED_CONTRACT_TRADE_BARS",
        "outcome_span_policy_sha256": BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
        "same_second_first_touch_policy": entry["same_second_first_touch_policy"],
        "terminal_policy": terminal,
        "schema": "systematic_fx.bar_outcome_policy.v1",
    }
    return {
        "barrier": barrier,
        "cost": cost,
        "entry": entry_policy,
        "evidence": evidence,
        "execution": execution,
        "outcome": outcome,
        "selection": selection,
        "signal": signal,
        "terminal": terminal,
    }


def _validate_expected_bar_run_spec(
    run_spec: RunSpec,
    *,
    config: BarPatternResearchConfig,
    split_plan: BarSplitPlan,
    candidate: BarPatternCandidate,
    trial_parameters: Mapping[str, object],
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
) -> dict[str, object]:
    policies = _expected_bar_policy_documents(config, candidate)
    parameters = _plain_json_object(run_spec.parameters, label="RunSpec parameters")
    dataset_handoff_sha256 = _sha256(
        parameters.get("bar_dataset_handoff_sha256"),
        label="RunSpec bar_dataset_handoff_sha256",
    )
    snapshot_identity_sha256 = _sha256(
        parameters.get("bar_code_snapshot_artifact_identity_sha256"),
        label="RunSpec bar_code_snapshot_artifact_identity_sha256",
    )
    migrations_sha256 = _sha256(
        parameters.get("bar_postgres_migrations_sha256"),
        label="RunSpec bar_postgres_migrations_sha256",
    )
    runtime = _plain_json_object(run_spec.runtime_environment, label="RunSpec runtime")
    postgres_runtime = runtime.get("postgresql")
    bar_runtime = runtime.get("bar_research_run")
    if (
        not isinstance(postgres_runtime, Mapping)
        or postgres_runtime.get("schema_migrations_sha256") != migrations_sha256
        or not isinstance(bar_runtime, Mapping)
        or dict(bar_runtime)
        != {
            "code_snapshot_artifact_identity_sha256": snapshot_identity_sha256,
            "dataset_handoff_sha256": dataset_handoff_sha256,
            "engine_version": BAR_DISCOVERY_ENGINE_VERSION,
            "orchestration": "REGISTER_AND_START_ALL_BEFORE_SINGLE_DISCOVERY_PASS",
        }
    ):
        raise BarRegistryError("RunSpec runtime does not bind frozen bar provenance")
    expected_parameters = {
        "bar_cost_policy": policies["cost"],
        "bar_barrier_policy_sha256": canonical_sha256(policies["barrier"]),
        "bar_campaign_definition_sha256": config.definition_sha256,
        "bar_candidate_catalog_sha256": config.candidate_catalog_sha256,
        "bar_candidate_definition_sha256": candidate.definition_sha256,
        "bar_candidate_key": candidate.candidate_key,
        "bar_code_snapshot_artifact_identity_sha256": snapshot_identity_sha256,
        "bar_config_file_sha256": config.sha256,
        "bar_config_semantic_sha256": config.semantic_sha256,
        "bar_cost_policy_sha256": canonical_sha256(policies["cost"]),
        "bar_dataset_handoff_sha256": dataset_handoff_sha256,
        "bar_dataset_manifest_sha256": bar_dataset_manifest_sha256,
        "bar_entry_policy_sha256": canonical_sha256(policies["entry"]),
        "bar_evidence_policy": policies["evidence"],
        "bar_evidence_policy_sha256": canonical_sha256(policies["evidence"]),
        "bar_execution_policy": policies["execution"],
        "bar_outcome_policy": policies["outcome"],
        "bar_outcome_span_policy_sha256": BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
        "bar_postgres_migrations_sha256": migrations_sha256,
        "bar_raw_source_manifest_sha256": raw_source_manifest_sha256,
        "bar_screening_only": BAR_PATTERN_SCREENING_ONLY,
        "bar_selection_policy": policies["selection"],
        "bar_selection_policy_sha256": canonical_sha256(policies["selection"]),
        "bar_split_plan_sha256": split_plan.sha256,
        "bar_trial_parameters_sha256": canonical_sha256(trial_parameters),
        "qualification_status": BAR_PATTERN_QUALIFICATION_STATUS,
    }
    calendar = {
        "dataset_handoff_sha256": dataset_handoff_sha256,
        "eligible_active_dates": [item.isoformat() for item in split_plan.eligible_dates],
        "schema": "systematic_fx.bar_eligible_calendar.v1",
    }
    fixed_mismatches: list[str] = []
    fixed_expected = {
        "run_kind": "SCREEN",
        "engine_version": BAR_DISCOVERY_ENGINE_VERSION,
        "eligible_calendar_version": BAR_ELIGIBLE_CALENDAR_VERSION,
        "eligible_calendar_sha256": canonical_sha256(calendar),
        "split_version": BAR_SPLIT_VERSION,
        "split_sha256": split_plan.sha256,
        "feature_version": BAR_FEATURE_VERSION,
        "feature_sha256": canonical_sha256(policies["signal"]),
        "outcome_version": BAR_OUTCOME_VERSION,
        "outcome_sha256": canonical_sha256(policies["outcome"]),
        "cost_version": BAR_COST_VERSION,
        "cost_sha256": canonical_sha256(policies["cost"]),
        "execution_version": BAR_EXECUTION_VERSION,
        "execution_sha256": canonical_sha256(policies["execution"]),
        "random_seed": BAR_RANDOM_SEED,
        "direction": candidate.direction.value,
        "signal_policy": policies["signal"],
        "entry_policy": policies["entry"],
        "barrier_policy": policies["barrier"],
        "terminal_policy": policies["terminal"],
        "parameters": expected_parameters,
    }
    for field_name, expected in fixed_expected.items():
        actual = parameters if field_name == "parameters" else getattr(run_spec, field_name)
        if (
            isinstance(expected, Mapping)
            and isinstance(actual, Mapping)
            and canonical_sha256(actual) != canonical_sha256(expected)
        ) or (not isinstance(expected, Mapping) and actual != expected):
            fixed_mismatches.append(field_name)
    expected_sources = {
        RAW_SOURCE_MANIFEST_KEY: raw_source_manifest_sha256,
        BAR_DATASET_MANIFEST_KEY: bar_dataset_manifest_sha256,
    }
    if dict(run_spec.source_manifest_hashes) != expected_sources:
        fixed_mismatches.append("source_manifest_hashes")
    if fixed_mismatches:
        raise BarRegistryError(
            "RunSpec differs from the complete frozen bar contract: "
            + ", ".join(sorted(fixed_mismatches))
        )
    canonical_spec = json.loads(run_spec.canonical_json())
    if not isinstance(canonical_spec, dict):  # pragma: no cover - RunSpec invariant
        raise BarRegistryError("RunSpec canonical payload is not an object")
    return canonical_spec


def _validate_registered_code_snapshot(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    run_spec: RunSpec,
    canonical_spec: Mapping[str, object],
) -> None:
    parameters = canonical_spec["parameters"]
    if not isinstance(parameters, Mapping):  # pragma: no cover - validated above
        raise BarRegistryError("RunSpec parameters are not an object")
    identity_sha256 = parameters["bar_code_snapshot_artifact_identity_sha256"]
    rows = connection.execute(
        """
        SELECT artifact_id, artifact_type, sha256, metadata
        FROM systematic_fx.artifacts
        WHERE artifact_type = 'bar_code_snapshot'
          AND metadata #>> '{artifact_identity_sha256}' = %s
        FOR SHARE
        """,
        (identity_sha256,),
    ).fetchall()
    if len(rows) != 1:
        raise BarRegistryDriftError("RunSpec code snapshot is not exactly registered")
    row = rows[0]
    metadata = row.get("metadata")
    logical = metadata.get("logical_identity") if isinstance(metadata, Mapping) else None
    expected_logical = {
        "code_commit": run_spec.code_commit,
        "code_snapshot_sha256": run_spec.code_snapshot_sha256,
        "dataset_handoff_sha256": parameters["bar_dataset_handoff_sha256"],
        "dataset_manifest_sha256": parameters["bar_dataset_manifest_sha256"],
        "outcome_span_policy_sha256": parameters["bar_outcome_span_policy_sha256"],
        "raw_source_manifest_sha256": parameters["bar_raw_source_manifest_sha256"],
    }
    if (
        row.get("sha256") != run_spec.code_snapshot_sha256
        or not isinstance(logical, Mapping)
        or any(logical.get(key) != value for key, value in expected_logical.items())
    ):
        raise BarRegistryDriftError("RunSpec code snapshot artifact lineage drift")


@_translate_psycopg_errors("bar RunSpec registration and candidate binding")
def register_bar_run_spec(
    database_url: str,
    run_spec: RunSpec,
    *,
    config: BarPatternResearchConfig,
    split_plan: BarSplitPlan,
    candidate_key: str,
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
    parent_run_fingerprint: str | None = None,
) -> RunSpecRegistration:
    """Register the full frozen spec and atomically bind its candidate before outcomes."""

    if not isinstance(run_spec, RunSpec):
        raise BarRegistryError("run_spec must be a RunSpec")
    if run_spec.campaign_id != BAR_PATTERN_CAMPAIGN_KEY:
        raise BarRegistryError("RunSpec campaign_id is not the bar-pattern campaign")
    if run_spec.experiment_id != BAR_CATALOG_EXPERIMENT_KEY:
        raise BarRegistryError("RunSpec experiment_id is not the frozen catalog experiment")
    try:
        candidate = config.candidate(candidate_key)
    except KeyError as error:
        raise BarRegistryError(str(error)) from error
    trial_parameters = candidate_trial_parameters(
        config,
        split_plan,
        candidate,
        raw_source_manifest_sha256=raw_source_manifest_sha256,
        bar_dataset_manifest_sha256=bar_dataset_manifest_sha256,
    )
    raw_hash = _raw_source_sha256(raw_source_manifest_sha256)
    dataset_hash = _sha256(
        bar_dataset_manifest_sha256,
        label="bar_dataset_manifest_sha256",
    )
    canonical_spec = _validate_expected_bar_run_spec(
        run_spec,
        config=config,
        split_plan=split_plan,
        candidate=candidate,
        trial_parameters=trial_parameters,
        raw_source_manifest_sha256=raw_hash,
        bar_dataset_manifest_sha256=dataset_hash,
    )
    url = _nonempty(database_url, label="database_url")
    registration = register_run_spec(
        url,
        run_spec,
        parent_run_fingerprint=parent_run_fingerprint,
    )
    with psycopg.connect(url, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with connection.transaction():
            trial = connection.execute(
                """
                SELECT t.experiment_trial_id, t.trial_key, t.trial_type,
                       t.status AS trial_status, t.parameters, t.parameters_sha256,
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
                (BAR_CATALOG_EXPERIMENT_KEY, candidate.candidate_key),
            ).fetchone()
            trial = _row_or_error(trial, label=f"candidate trial {candidate.candidate_key}")
            _assert_fields(
                label=f"candidate trial {candidate.candidate_key}",
                row=trial,
                expected={
                    "trial_key": candidate.candidate_key,
                    "trial_type": "STRATEGY_VARIANT",
                    "parameters": trial_parameters,
                    "parameters_sha256": canonical_sha256(trial_parameters),
                    "experiment_id": registration.experiment_id,
                    "experiment_key": BAR_CATALOG_EXPERIMENT_KEY,
                    "campaign_id": registration.campaign_id,
                    "campaign_key": BAR_PATTERN_CAMPAIGN_KEY,
                },
            )
            spec_row = connection.execute(
                """
                SELECT research_run_spec_id, run_fingerprint, campaign_id,
                       experiment_id, run_kind, engine_version, canonical_spec,
                       source_manifest_hashes, direction
                FROM systematic_fx.research_run_specs
                WHERE research_run_spec_id = %s
                FOR SHARE
                """,
                (registration.research_run_spec_id,),
            ).fetchone()
            spec_row = _row_or_error(spec_row, label=f"RunSpec {run_spec.fingerprint}")
            _assert_fields(
                label=f"RunSpec {run_spec.fingerprint}",
                row=spec_row,
                expected={
                    "research_run_spec_id": registration.research_run_spec_id,
                    "run_fingerprint": run_spec.fingerprint,
                    "campaign_id": registration.campaign_id,
                    "experiment_id": registration.experiment_id,
                    "run_kind": run_spec.run_kind,
                    "engine_version": run_spec.engine_version,
                    "canonical_spec": canonical_spec,
                    "source_manifest_hashes": dict(run_spec.source_manifest_hashes),
                    "direction": run_spec.direction,
                },
            )
            _validate_registered_code_snapshot(
                connection,
                run_spec=run_spec,
                canonical_spec=canonical_spec,
            )
            existing_spec_id = trial["research_run_spec_id"]
            if existing_spec_id is None:
                updated = connection.execute(
                    """
                    UPDATE systematic_fx.experiment_trials
                    SET research_run_spec_id = %s
                    WHERE experiment_trial_id = %s
                      AND research_run_spec_id IS NULL
                    RETURNING experiment_trial_id
                    """,
                    (
                        registration.research_run_spec_id,
                        trial["experiment_trial_id"],
                    ),
                ).fetchone()
                if updated is None:
                    raise BarRegistryStateError("candidate trial lost its unbound state")
            elif int(existing_spec_id) != registration.research_run_spec_id:
                raise BarRegistryDriftError(
                    "candidate trial is already bound to a different immutable RunSpec"
                )
    return registration


@_translate_psycopg_errors("bar run-attempt abort")
def abort_bar_run_attempt(
    database_url: str,
    *,
    research_run_attempt_id: int,
    candidate_key: str,
    run_fingerprint: str,
    result_summary: Mapping[str, object],
    error_message: str,
) -> RunAttemptState:
    """Fail one exact active bar attempt while preserving its pre-outcome spec binding."""

    url = _nonempty(database_url, label="database_url")
    attempt_id = _positive_integer(
        research_run_attempt_id,
        label="research_run_attempt_id",
    )
    key = _nonempty(candidate_key, label="candidate_key")
    fingerprint = _sha256(run_fingerprint, label="run_fingerprint")
    summary = dict(_canonical_mapping(result_summary, label="result_summary"))
    for field, expected in {
        "candidate_key": key,
        "run_fingerprint": fingerprint,
    }.items():
        present = summary.get(field)
        if present is not None and present != expected:
            raise BarRegistryError(f"result_summary.{field} differs from the abort identity")
        summary[field] = expected
    if len(canonical_json_bytes(summary)) > 65_536:
        raise BarRegistryError("result_summary exceeds the 64 KiB registry limit")
    message = _nonempty(error_message, label="error_message")
    if len(message) > 2_000:
        raise BarRegistryError("error_message exceeds 2,000 characters")

    with psycopg.connect(url, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with connection.transaction():
            identity = connection.execute(
                """
                SELECT a.research_run_attempt_id, a.research_run_spec_id,
                       r.run_fingerprint,
                       r.canonical_spec #>> '{parameters,bar_candidate_key}' AS candidate_key,
                       e.experiment_id, e.experiment_key,
                       c.campaign_id, c.campaign_key
                FROM systematic_fx.research_run_attempts AS a
                JOIN systematic_fx.research_run_specs AS r
                  ON r.research_run_spec_id = a.research_run_spec_id
                JOIN systematic_fx.experiments AS e
                  ON e.experiment_id = r.experiment_id
                JOIN systematic_fx.campaigns AS c
                  ON c.campaign_id = r.campaign_id
                WHERE a.research_run_attempt_id = %s
                """,
                (attempt_id,),
            ).fetchone()
            identity = _row_or_error(identity, label=f"research run attempt {attempt_id}")
            _assert_fields(
                label=f"bar research run attempt {attempt_id}",
                row=identity,
                expected={
                    "research_run_attempt_id": attempt_id,
                    "run_fingerprint": fingerprint,
                    "candidate_key": key,
                    "experiment_key": BAR_CATALOG_EXPERIMENT_KEY,
                    "campaign_key": BAR_PATTERN_CAMPAIGN_KEY,
                },
            )
            trial = connection.execute(
                """
                SELECT experiment_trial_id, status AS trial_status,
                       research_run_spec_id
                FROM systematic_fx.experiment_trials
                WHERE experiment_id = %s AND trial_key = %s
                FOR UPDATE
                """,
                (identity["experiment_id"], key),
            ).fetchone()
            trial = _row_or_error(trial, label=f"candidate trial {key}")
            if trial["trial_status"] not in {"REGISTERED", "RUNNING"}:
                raise BarRegistryStateError("terminal candidate trial cannot be aborted")
            bound_spec_id = trial["research_run_spec_id"]
            if bound_spec_id is None:
                raise BarRegistryStateError(
                    "reserved bar attempt requires its prebound candidate trial"
                )
            if int(bound_spec_id) != int(identity["research_run_spec_id"]):
                raise BarRegistryDriftError("candidate trial is bound to a different RunSpec")

            attempt = connection.execute(
                """
                SELECT research_run_attempt_id, research_run_spec_id,
                       attempt_number, status, result_artifact_id,
                       trade_ledger_artifact_id, result_summary,
                       error_message, finished_at
                FROM systematic_fx.research_run_attempts
                WHERE research_run_attempt_id = %s
                FOR UPDATE
                """,
                (attempt_id,),
            ).fetchone()
            attempt = _row_or_error(attempt, label=f"research run attempt {attempt_id}")
            _assert_fields(
                label=f"bar research run attempt {attempt_id}",
                row=attempt,
                expected={"research_run_spec_id": identity["research_run_spec_id"]},
            )
            status = str(attempt["status"])
            if status == "FAILED":
                _assert_fields(
                    label=f"failed bar research run attempt {attempt_id}",
                    row=attempt,
                    expected={
                        "result_artifact_id": None,
                        "trade_ledger_artifact_id": None,
                        "result_summary": summary,
                        "error_message": message,
                    },
                )
                if attempt["finished_at"] is None:
                    raise BarRegistryDriftError("failed bar attempt lacks finished_at")
            elif status in {"QUEUED", "RUNNING"}:
                if (
                    attempt["result_artifact_id"] is not None
                    or attempt["trade_ledger_artifact_id"] is not None
                ):
                    raise BarRegistryDriftError("active bar attempt already has artifact links")
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
                    raise BarRegistryStateError("bar attempt lost its active state")
                attempt = updated
            else:
                raise BarRegistryStateError(f"bar attempt cannot transition {status} -> FAILED")

    return RunAttemptState(
        research_run_attempt_id=attempt_id,
        research_run_spec_id=int(attempt["research_run_spec_id"]),
        attempt_number=int(attempt["attempt_number"]),
        status="FAILED",
        result_artifact_id=None,
        trade_ledger_artifact_id=None,
    )


def _terminal_summary(
    result: BarTerminalResult,
    *,
    result_artifact_id: int,
) -> dict[str, object]:
    return {
        "artifact": {
            "artifact_id": result_artifact_id,
            "artifact_identity_sha256": result.artifact.descriptor.identity_sha256,
            "artifact_key": result.artifact.descriptor.artifact_key,
            "byte_size": result.artifact.byte_size,
            "sha256": result.artifact.sha256,
        },
        "candidate_definition_sha256": result.candidate_definition_sha256,
        "candidate_key": result.candidate_key,
        "compact_result": dict(result.compact_result),
        "compact_result_sha256": canonical_sha256(result.compact_result),
        "decision_label": result.decision_label,
        "final_label": result.final_label,
        "run_fingerprint": result.run_fingerprint,
        "schema": BAR_TERMINAL_RESULT_SCHEMA,
        "attempt_status": "SUCCEEDED",
        "trial_status": result.trial_status,
    }


def _read_exact_json_artifact_document(
    opened: OpenVerifiedBarArtifact,
    *,
    discovery_canonical: bool,
) -> Mapping[str, object]:
    os.lseek(opened.descriptor, 0, os.SEEK_SET)
    raw = bytearray()
    while chunk := os.read(opened.descriptor, 1024 * 1024):
        raw.extend(chunk)
    os.lseek(opened.descriptor, 0, os.SEEK_SET)
    raw_length = len(raw)
    raw_digest = hashlib.sha256(raw).digest()
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BarRegistryError("bar artifact is not valid UTF-8 JSON") from error
    if discovery_canonical:
        if not isinstance(value, dict):
            raise BarRegistryError("Discovery artifact document must be an object")
        document: Mapping[str, object] = value
        raw.clear()
        try:
            encoder = json.JSONEncoder(
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
            canonical_digest = hashlib.sha256()
            canonical_length = 0
            for fragment in encoder.iterencode(document):
                encoded = fragment.encode("ascii")
                canonical_digest.update(encoded)
                canonical_length += len(encoded)
            canonical_digest.update(b"\n")
            canonical_length += 1
        except (TypeError, ValueError) as error:  # pragma: no cover - canonical mapping guarded
            raise BarRegistryError("Discovery artifact is not strict canonical JSON") from error
        exact = canonical_length == raw_length and canonical_digest.digest() == raw_digest
    else:
        document = _canonical_mapping(value, label="bar artifact document")
        expected = canonical_json_bytes(document)
        exact = expected == raw
    if not exact:
        artifact_kind = "Discovery" if discovery_canonical else "terminal/evidence"
        raise BarRegistryError(f"{artifact_kind} artifact is not exact canonical JSON")
    return document


def _read_terminal_artifact_document(
    opened: OpenVerifiedBarArtifact,
) -> Mapping[str, object]:
    return _read_exact_json_artifact_document(opened, discovery_canonical=False)


def _read_discovery_artifact_document(
    opened: OpenVerifiedBarArtifact,
) -> Mapping[str, object]:
    """Read the Discovery engine's ASCII-escaped, LF-terminated canonical JSON."""

    return _read_exact_json_artifact_document(opened, discovery_canonical=True)


def _artifact_from_database_row(row: Mapping[str, Any]) -> PublishedBarArtifact:
    metadata = row.get("metadata")
    if not isinstance(metadata, Mapping):
        raise BarRegistryDriftError("bar artifact metadata is not an object")
    try:
        descriptor = BarArtifactDescriptor(
            artifact_key=metadata["artifact_key"],
            artifact_type=metadata["artifact_type"],
            artifact_schema=metadata["artifact_schema"],
            artifact_version=metadata["artifact_version"],
            record_count=metadata["record_count"],
            schema_sha256=metadata["schema_sha256"],
            source_manifest_sha256=metadata["source_manifest_sha256"],
            logical_identity=metadata["logical_identity"],
            media_type=metadata["media_type"],
            file_suffix=metadata["file_suffix"],
            root_kind=metadata["root_kind"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise BarRegistryDriftError("bar artifact descriptor metadata is incomplete") from error
    uri = row.get("uri")
    if not isinstance(uri, str):
        raise BarRegistryDriftError("bar artifact URI is not a string")
    parsed = urlsplit(uri)
    if (
        parsed.scheme != "file"
        or parsed.netloc not in {"", "localhost"}
        or parsed.query
        or parsed.fragment
    ):
        raise BarRegistryDriftError("bar artifact URI is not a canonical local file URI")
    path = Path(unquote(parsed.path))
    sha256 = row.get("sha256")
    byte_size = row.get("byte_size")
    if not isinstance(sha256, str) or _SHA256.fullmatch(sha256) is None:
        raise BarRegistryDriftError("registered bar artifact SHA-256 is invalid")
    if isinstance(byte_size, bool) or not isinstance(byte_size, int) or byte_size < 0:
        raise BarRegistryDriftError("registered bar artifact byte_size is invalid")
    artifact = PublishedBarArtifact(
        descriptor=descriptor,
        path=path,
        sha256=sha256,
        byte_size=byte_size,
    )
    expected_row = {
        "artifact_key": descriptor.artifact_key,
        "artifact_type": descriptor.artifact_type,
        "uri": artifact.uri,
        "sha256": artifact.sha256,
        "byte_size": artifact.byte_size,
        "media_type": descriptor.media_type,
        "metadata": artifact.database_metadata(),
    }
    if any(row.get(key) != expected for key, expected in expected_row.items()):
        raise BarRegistryDriftError("registered bar artifact row/descriptor identity drift")
    return artifact


def _registered_lineage_artifact(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    artifact_type: str,
    artifact_identity_sha256: str,
) -> PublishedBarArtifact:
    rows = connection.execute(
        """
        SELECT artifact_id, artifact_key, artifact_type, uri, sha256,
               byte_size, media_type, metadata
        FROM systematic_fx.artifacts
        WHERE artifact_type = %s
          AND metadata #>> '{artifact_identity_sha256}' = %s
        FOR SHARE
        """,
        (artifact_type, artifact_identity_sha256),
    ).fetchall()
    if len(rows) != 1:
        raise BarRegistryDriftError(
            f"{artifact_type} identity does not resolve to exactly one registered artifact"
        )
    return _artifact_from_database_row(rows[0])


def _registered_discovery_lineage(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    config: BarPatternResearchConfig,
    trial: Mapping[str, Any],
    candidate_result: Mapping[str, object],
) -> tuple[PublishedBarArtifact, PublishedBarArtifact]:
    lineage = candidate_result.get("discovery_lineage")
    parameters = trial.get("parameters")
    if not isinstance(lineage, Mapping) or not isinstance(parameters, Mapping):
        raise BarRegistryError("terminal discovery lineage is incomplete")
    global_artifact = _registered_lineage_artifact(
        connection,
        artifact_type=BAR_GLOBAL_DISCOVERY_ARTIFACT_TYPE,
        artifact_identity_sha256=str(lineage["global_result_artifact_identity_sha256"]),
    )
    evidence_artifact = _registered_lineage_artifact(
        connection,
        artifact_type=BAR_EVIDENCE_MANIFEST_ARTIFACT_TYPE,
        artifact_identity_sha256=str(lineage["evidence_artifact_identity_sha256"]),
    )
    global_logical = global_artifact.descriptor.logical_identity
    evidence_logical = evidence_artifact.descriptor.logical_identity
    expected_global = {
        "candidate_catalog_sha256": config.candidate_catalog_sha256,
        "config_semantic_sha256": config.semantic_sha256,
        "dataset_handoff_sha256": lineage["dataset_handoff_sha256"],
        "dataset_manifest_sha256": parameters["bar_dataset_manifest_sha256"],
        "discovery_result_sha256": lineage["discovery_result_sha256"],
        "evidence_artifact_identity_sha256": lineage["evidence_artifact_identity_sha256"],
        "evidence_identity_sha256": lineage["evidence_identity_sha256"],
        "evidence_manifest_sha256": lineage["evidence_manifest_sha256"],
        "outcome_span_policy_sha256": BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
        "raw_source_manifest_sha256": parameters["raw_source_manifest_sha256"],
        "split_plan_sha256": parameters["split_plan_sha256"],
    }
    expected_evidence = {
        "candidate_catalog_sha256": config.candidate_catalog_sha256,
        "config_semantic_sha256": config.semantic_sha256,
        "dataset_manifest_sha256": parameters["bar_dataset_manifest_sha256"],
        "evidence_identity_sha256": lineage["evidence_identity_sha256"],
        "outcome_span_policy_sha256": BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
        "record_count": lineage["evidence_shard_count"],
        "source_identity_sha256": parameters["bar_dataset_manifest_sha256"],
        "split_plan_sha256": parameters["split_plan_sha256"],
    }
    if (
        global_artifact.descriptor.artifact_schema != DISCOVERY_RESULT_SCHEMA
        or global_artifact.descriptor.record_count != ALLOCATED_VARIANT_COUNT
        or global_artifact.descriptor.source_manifest_sha256
        != parameters["bar_dataset_manifest_sha256"]
        or global_artifact.sha256 != lineage["global_result_artifact_sha256"]
        or global_artifact.descriptor.identity_sha256
        != lineage["global_result_artifact_identity_sha256"]
        or any(global_logical.get(key) != value for key, value in expected_global.items())
    ):
        raise BarRegistryDriftError("registered global Discovery artifact lineage drift")
    if (
        evidence_artifact.descriptor.artifact_schema != DISCOVERY_EVIDENCE_SCHEMA
        or evidence_artifact.descriptor.record_count != lineage["evidence_shard_count"]
        or evidence_artifact.descriptor.source_manifest_sha256
        != parameters["raw_source_manifest_sha256"]
        or evidence_artifact.sha256 != lineage["evidence_manifest_sha256"]
        or evidence_artifact.descriptor.identity_sha256
        != lineage["evidence_artifact_identity_sha256"]
        or any(evidence_logical.get(key) != value for key, value in expected_evidence.items())
    ):
        raise BarRegistryDriftError("registered Discovery evidence artifact lineage drift")
    return global_artifact, evidence_artifact


def _validate_discovery_lineage_against_run_spec(
    *,
    candidate_result: Mapping[str, object],
    canonical_spec: Mapping[str, object],
) -> None:
    """Bind outcome-produced lineage to every corresponding frozen RunSpec input."""

    lineage = candidate_result.get("discovery_lineage")
    parameters = canonical_spec.get("parameters")
    sources = canonical_spec.get("source_manifest_hashes")
    split = canonical_spec.get("split")
    if (
        not isinstance(lineage, Mapping)
        or not isinstance(parameters, Mapping)
        or not isinstance(sources, Mapping)
        or not isinstance(split, Mapping)
    ):
        raise BarRegistryDriftError("terminal lineage/RunSpec structure is incomplete")
    expected = {
        "candidate_catalog_sha256": parameters.get("bar_candidate_catalog_sha256"),
        "code_snapshot_artifact_identity_sha256": parameters.get(
            "bar_code_snapshot_artifact_identity_sha256"
        ),
        "code_snapshot_sha256": canonical_spec.get("code_snapshot_sha256"),
        "config_file_sha256": parameters.get("bar_config_file_sha256"),
        "config_semantic_sha256": parameters.get("bar_config_semantic_sha256"),
        "dataset_handoff_sha256": parameters.get("bar_dataset_handoff_sha256"),
        "dataset_manifest_sha256": parameters.get("bar_dataset_manifest_sha256"),
        "outcome_span_policy_sha256": parameters.get("bar_outcome_span_policy_sha256"),
        "postgres_migrations_sha256": parameters.get("bar_postgres_migrations_sha256"),
        "raw_source_manifest_sha256": parameters.get("bar_raw_source_manifest_sha256"),
        "split_plan_sha256": parameters.get("bar_split_plan_sha256"),
    }
    mismatches = [key for key, value in expected.items() if lineage.get(key) != value]
    if (
        sources.get(RAW_SOURCE_MANIFEST_KEY) != expected["raw_source_manifest_sha256"]
        or sources.get(BAR_DATASET_MANIFEST_KEY) != expected["dataset_manifest_sha256"]
    ):
        mismatches.append("source_manifest_hashes")
    if split.get("sha256") != expected["split_plan_sha256"]:
        mismatches.append("split")
    if mismatches:
        raise BarRegistryDriftError(
            "terminal discovery lineage differs from bound RunSpec: "
            + ", ".join(sorted(set(mismatches)))
        )


def _validate_terminal_artifact_contract(
    *,
    config: BarPatternResearchConfig,
    trial: Mapping[str, Any],
    canonical_spec: Mapping[str, object],
    result: BarTerminalResult,
    document: Mapping[str, object],
) -> None:
    parameters = trial.get("parameters")
    if not isinstance(parameters, Mapping):  # pragma: no cover - locked above
        raise BarRegistryDriftError("candidate trial parameters are not an object")
    candidate_result = document.get("candidate_result")
    if not isinstance(candidate_result, Mapping):
        raise BarRegistryError("terminal artifact candidate_result must be an object")
    expected_descriptor, expected_document = _bar_terminal_artifact_contract(
        config,
        candidate_key=result.candidate_key,
        raw_source_manifest_sha256=str(parameters["raw_source_manifest_sha256"]),
        bar_dataset_manifest_sha256=str(parameters["bar_dataset_manifest_sha256"]),
        split_plan_sha256=str(parameters["split_plan_sha256"]),
        run_fingerprint=result.run_fingerprint,
        trial_status=result.trial_status,
        decision_label=result.decision_label,
        compact_result=result.compact_result,
        candidate_result=candidate_result,
    )
    if result.artifact.descriptor != expected_descriptor:
        raise BarRegistryError(
            "terminal artifact descriptor differs from the candidate result contract"
        )
    if dict(document) != expected_document:
        raise BarRegistryError("terminal artifact document differs from its bound result contract")
    _validate_discovery_lineage_against_run_spec(
        candidate_result=candidate_result,
        canonical_spec=canonical_spec,
    )


def _locked_candidate_trial(
    connection: psycopg.Connection[dict[str, Any]],
    result: BarTerminalResult,
    *,
    config: BarPatternResearchConfig,
) -> Mapping[str, Any]:
    trial = connection.execute(
        """
        SELECT t.experiment_trial_id, t.trial_key, t.trial_type,
               t.status AS trial_status, t.parameters,
               t.parameters_sha256, t.research_run_spec_id,
               t.result_summary AS trial_result_summary,
               e.experiment_id, e.experiment_key, e.campaign_id,
               c.campaign_key
        FROM systematic_fx.experiment_trials t
        JOIN systematic_fx.experiments e ON e.experiment_id = t.experiment_id
        JOIN systematic_fx.campaigns c ON c.campaign_id = e.campaign_id
        WHERE e.experiment_key = %s AND t.trial_key = %s
        FOR UPDATE OF t
        """,
        (BAR_CATALOG_EXPERIMENT_KEY, result.candidate_key),
    ).fetchone()
    trial = _row_or_error(trial, label=f"candidate trial {result.candidate_key}")
    _assert_fields(
        label=f"candidate trial {result.candidate_key}",
        row=trial,
        expected={
            "trial_key": result.candidate_key,
            "trial_type": "STRATEGY_VARIANT",
            "experiment_key": BAR_CATALOG_EXPERIMENT_KEY,
            "campaign_key": BAR_PATTERN_CAMPAIGN_KEY,
        },
    )
    parameters = trial["parameters"]
    if not isinstance(parameters, Mapping):
        raise BarRegistryDriftError("candidate trial parameters are not an object")
    candidate = config.candidate(result.candidate_key)
    expected_parameters = {
        "campaign_definition": config.canonical_parameters(),
        "campaign_definition_sha256": config.definition_sha256,
        "candidate_catalog_sha256": config.candidate_catalog_sha256,
        "candidate_definition": candidate.definition_payload(),
        "candidate_definition_sha256": result.candidate_definition_sha256,
        "candidate_key": result.candidate_key,
        "schema": BAR_TRIAL_PARAMETERS_SCHEMA,
        "split_plan_schema": BAR_SPLIT_SCHEMA,
    }
    if any(parameters.get(key) != value for key, value in expected_parameters.items()) or (
        canonical_sha256(parameters) != trial["parameters_sha256"]
    ):
        raise BarRegistryDriftError("candidate trial parameter identity drift")
    try:
        raw_hash = _raw_source_sha256(parameters.get("raw_source_manifest_sha256"))
        dataset_hash = _sha256(
            parameters.get("bar_dataset_manifest_sha256"),
            label="candidate trial bar_dataset_manifest_sha256",
        )
        split_hash = _sha256(
            parameters.get("split_plan_sha256"),
            label="candidate trial split_plan_sha256",
        )
    except BarRegistryError as error:
        raise BarRegistryDriftError("candidate trial manifest/split identity drift") from error
    split_document = parameters.get("split_plan")
    if not isinstance(split_document, Mapping) or canonical_sha256(split_document) != split_hash:
        raise BarRegistryDriftError("candidate trial split-plan identity drift")
    # Normalize these values once so all downstream checks compare the same
    # application-verified trial lineage rather than trusting loose JSON keys.
    if (
        raw_hash != parameters["raw_source_manifest_sha256"]
        or dataset_hash != parameters["bar_dataset_manifest_sha256"]
    ):
        raise BarRegistryDriftError("candidate trial source-manifest identity drift")
    if trial["research_run_spec_id"] is None:
        raise BarRegistryStateError("candidate trial must be bound to its RunSpec before outcomes")
    return trial


def _locked_run_attempt(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    trial: Mapping[str, Any],
    result: BarTerminalResult,
) -> Mapping[str, Any]:
    attempt = connection.execute(
        """
        SELECT a.research_run_attempt_id, a.research_run_spec_id,
               a.status AS attempt_status, a.started_at, a.finished_at,
               a.result_artifact_id, a.result_summary AS attempt_result_summary,
               r.run_fingerprint, r.campaign_id, r.experiment_id,
               r.canonical_spec
        FROM systematic_fx.research_run_attempts a
        JOIN systematic_fx.research_run_specs r
          ON r.research_run_spec_id = a.research_run_spec_id
        WHERE a.research_run_attempt_id = %s
        FOR UPDATE OF a
        """,
        (result.research_run_attempt_id,),
    ).fetchone()
    attempt = _row_or_error(
        attempt,
        label=f"research run attempt {result.research_run_attempt_id}",
    )
    _assert_fields(
        label=f"research run attempt {result.research_run_attempt_id}",
        row=attempt,
        expected={
            "run_fingerprint": result.run_fingerprint,
            "campaign_id": trial["campaign_id"],
            "experiment_id": trial["experiment_id"],
        },
    )
    if int(attempt["research_run_spec_id"]) != int(trial["research_run_spec_id"]):
        raise BarRegistryDriftError("attempt RunSpec differs from the prebound candidate trial")
    canonical_spec = attempt["canonical_spec"]
    if not isinstance(canonical_spec, Mapping):
        raise BarRegistryDriftError("RunSpec canonical_spec is not an object")
    run_parameters = canonical_spec.get("parameters")
    trial_parameters = trial["parameters"]
    if not isinstance(trial_parameters, Mapping):  # pragma: no cover - locked above
        raise BarRegistryDriftError("candidate trial parameters are not an object")
    required_run_parameters = {
        "bar_campaign_definition_sha256": trial_parameters["campaign_definition_sha256"],
        "bar_candidate_definition_sha256": result.candidate_definition_sha256,
        "bar_candidate_key": result.candidate_key,
        "bar_split_plan_sha256": trial_parameters["split_plan_sha256"],
        "bar_trial_parameters_sha256": trial["parameters_sha256"],
    }
    source_hashes = canonical_spec.get("source_manifest_hashes")
    required_source_hashes = {
        RAW_SOURCE_MANIFEST_KEY: trial_parameters["raw_source_manifest_sha256"],
        BAR_DATASET_MANIFEST_KEY: trial_parameters["bar_dataset_manifest_sha256"],
    }
    split_identity = canonical_spec.get("split")
    if (
        not isinstance(run_parameters, Mapping)
        or any(run_parameters.get(key) != value for key, value in required_run_parameters.items())
        or not isinstance(source_hashes, Mapping)
        or dict(source_hashes) != required_source_hashes
        or not isinstance(split_identity, Mapping)
        or split_identity.get("sha256") != trial_parameters["split_plan_sha256"]
        or canonical_spec.get("campaign_id") != BAR_PATTERN_CAMPAIGN_KEY
        or canonical_spec.get("experiment_id") != BAR_CATALOG_EXPERIMENT_KEY
        or canonical_sha256(canonical_spec) != result.run_fingerprint
    ):
        raise BarRegistryDriftError("RunSpec does not bind the registered candidate")
    return attempt


def _terminalize_attempt(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    attempt: Mapping[str, Any],
    result: BarTerminalResult,
    result_artifact_id: int,
    summary: Mapping[str, object],
) -> bool:
    current_status = str(attempt["attempt_status"])
    if current_status == "RUNNING":
        if attempt["started_at"] is None:
            raise BarRegistryDriftError("RUNNING attempt has no started_at")
        updated = connection.execute(
            """
            UPDATE systematic_fx.research_run_attempts
            SET status = %s, result_artifact_id = %s,
                result_summary = %s, finished_at = statement_timestamp()
            WHERE research_run_attempt_id = %s AND status = 'RUNNING'
            RETURNING research_run_attempt_id
            """,
            (
                "SUCCEEDED",
                result_artifact_id,
                Jsonb(summary),
                result.research_run_attempt_id,
            ),
        ).fetchone()
        if updated is None:
            raise BarRegistryStateError("run attempt lost RUNNING state")
        return True
    if current_status == "SUCCEEDED":
        _assert_fields(
            label="terminal research run attempt",
            row=attempt,
            expected={
                "result_artifact_id": result_artifact_id,
                "attempt_result_summary": summary,
            },
        )
        if attempt["finished_at"] is None:
            raise BarRegistryDriftError("terminal attempt has no finished_at")
        return False
    raise BarRegistryStateError(f"attempt cannot transition {current_status} -> SUCCEEDED")


def _terminalize_trial(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    trial: Mapping[str, Any],
    attempt: Mapping[str, Any],
    result: BarTerminalResult,
    summary: Mapping[str, object],
) -> bool:
    current_status = str(trial["trial_status"])
    if current_status in {"REGISTERED", "RUNNING"}:
        updated = connection.execute(
            """
            UPDATE systematic_fx.experiment_trials
            SET status = %s, result_summary = %s,
                started_at = COALESCE(started_at, statement_timestamp()),
                finished_at = statement_timestamp()
            WHERE experiment_trial_id = %s
              AND research_run_spec_id = %s
              AND status IN ('REGISTERED', 'RUNNING')
            RETURNING experiment_trial_id
            """,
            (
                result.trial_status,
                Jsonb(summary),
                trial["experiment_trial_id"],
                attempt["research_run_spec_id"],
            ),
        ).fetchone()
        if updated is None:
            raise BarRegistryStateError("candidate trial lost its active state")
        return True
    if current_status == result.trial_status:
        _assert_fields(
            label="terminal candidate trial",
            row=trial,
            expected={
                "research_run_spec_id": attempt["research_run_spec_id"],
                "trial_result_summary": summary,
            },
        )
        return False
    raise BarRegistryStateError(
        f"candidate trial cannot transition {current_status} -> {result.trial_status}"
    )


@_translate_psycopg_errors("terminal bar-result registration")
def register_terminal_bar_result(
    database_url: str,
    project_root: Path,
    *,
    config: BarPatternResearchConfig,
    result: BarTerminalResult,
) -> BarTerminalRegistrationReport:
    """Atomically bind artifact, run attempt, and candidate terminal summary."""

    url = _nonempty(database_url, label="database_url")
    try:
        candidate = config.candidate(result.candidate_key)
    except KeyError as error:
        raise BarRegistryError(str(error)) from error
    if candidate.definition_sha256 != result.candidate_definition_sha256:
        raise BarRegistryError("terminal result candidate definition SHA-256 drift")
    descriptor = result.artifact.descriptor
    static_contract = {
        "artifact_type": BAR_TERMINAL_ARTIFACT_TYPE,
        "artifact_schema": BAR_TERMINAL_ARTIFACT_SCHEMA,
        "artifact_version": 1,
        "record_count": BAR_TERMINAL_ARTIFACT_RECORD_COUNT,
        "schema_sha256": BAR_TERMINAL_ARTIFACT_SCHEMA_SHA256,
        "media_type": "application/json",
        "file_suffix": ".json",
        "root_kind": "bar_patterns",
    }
    if any(getattr(descriptor, key) != value for key, value in static_contract.items()):
        raise BarRegistryError("terminal artifact descriptor violates the frozen static contract")

    with psycopg.connect(url, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with (
            connection.transaction(),
            open_verified_bar_artifact(project_root, result.artifact) as opened,
        ):
            document = _read_terminal_artifact_document(opened)
            trial = _locked_candidate_trial(connection, result, config=config)
            attempt = _locked_run_attempt(connection, trial=trial, result=result)
            _validate_terminal_artifact_contract(
                config=config,
                trial=trial,
                canonical_spec=attempt["canonical_spec"],
                result=result,
                document=document,
            )
            candidate_result = document["candidate_result"]
            if not isinstance(candidate_result, Mapping):  # pragma: no cover - validated above
                raise BarRegistryError("terminal artifact candidate_result must be an object")
            _registered_discovery_lineage(
                connection,
                config=config,
                trial=trial,
                candidate_result=candidate_result,
            )
            result_artifact_id, created_artifact = _ensure_artifact(
                connection,
                result.artifact,
            )
            summary = _terminal_summary(result, result_artifact_id=result_artifact_id)
            transitioned_attempt = _terminalize_attempt(
                connection,
                attempt=attempt,
                result=result,
                result_artifact_id=result_artifact_id,
                summary=summary,
            )
            transitioned_trial = _terminalize_trial(
                connection,
                trial=trial,
                attempt=attempt,
                result=result,
                summary=summary,
            )

    return BarTerminalRegistrationReport(
        experiment_trial_id=int(trial["experiment_trial_id"]),
        research_run_spec_id=int(attempt["research_run_spec_id"]),
        research_run_attempt_id=result.research_run_attempt_id,
        result_artifact_id=result_artifact_id,
        attempt_status="SUCCEEDED",
        trial_status=result.trial_status,
        decision_label=result.decision_label,
        created_artifact=created_artifact,
        transitioned_attempt=transitioned_attempt,
        transitioned_trial=transitioned_trial,
    )


def _artifact_row_from_completed_candidate(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "artifact_id": row["result_artifact_id"],
        "artifact_key": row["artifact_key"],
        "artifact_type": row["artifact_type"],
        "uri": row["artifact_uri"],
        "sha256": row["artifact_sha256"],
        "byte_size": row["artifact_byte_size"],
        "media_type": row["artifact_media_type"],
        "metadata": row["artifact_metadata"],
    }


def _completed_candidate_row(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    candidate_key: str,
) -> Mapping[str, Any]:
    rows = connection.execute(
        """
        SELECT t.experiment_trial_id, t.trial_key, t.trial_type,
               t.status AS trial_status, t.parameters, t.parameters_sha256,
               t.research_run_spec_id, t.result_summary AS trial_result_summary,
               t.started_at AS trial_started_at, t.finished_at AS trial_finished_at,
               r.run_fingerprint, r.canonical_spec,
               r.campaign_id, r.experiment_id,
               e.experiment_key, c.campaign_key,
               a.research_run_attempt_id AS succeeded_attempt_id,
               a.status AS succeeded_attempt_status,
               a.result_artifact_id,
               a.trade_ledger_artifact_id,
               a.result_summary AS attempt_result_summary,
               a.started_at AS attempt_started_at,
               a.finished_at AS attempt_finished_at,
               a.error_message AS attempt_error_message,
               artifact.artifact_key, artifact.artifact_type,
               artifact.uri AS artifact_uri,
               artifact.sha256 AS artifact_sha256,
               artifact.byte_size AS artifact_byte_size,
               artifact.media_type AS artifact_media_type,
               artifact.metadata AS artifact_metadata
        FROM systematic_fx.experiment_trials AS t
        JOIN systematic_fx.experiments AS e
          ON e.experiment_id = t.experiment_id
        JOIN systematic_fx.campaigns AS c
          ON c.campaign_id = e.campaign_id
        JOIN systematic_fx.research_run_specs AS r
          ON r.research_run_spec_id = t.research_run_spec_id
        JOIN systematic_fx.research_run_attempts AS a
          ON a.research_run_spec_id = r.research_run_spec_id
         AND a.status = 'SUCCEEDED'
        JOIN systematic_fx.artifacts AS artifact
          ON artifact.artifact_id = a.result_artifact_id
        WHERE e.experiment_key = %s AND c.campaign_key = %s
          AND t.trial_key = %s
        FOR SHARE OF t, r, a, artifact
        """,
        (BAR_CATALOG_EXPERIMENT_KEY, BAR_PATTERN_CAMPAIGN_KEY, candidate_key),
    ).fetchall()
    if len(rows) != 1:
        raise BarRegistryDriftError(
            f"candidate {candidate_key} does not have exactly one terminal success"
        )
    return rows[0]


def _validate_duplicate_attempt(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    reservation: RunAttemptReservation,
    reused_attempt_id: int,
) -> None:
    row = connection.execute(
        """
        SELECT research_run_attempt_id, research_run_spec_id, attempt_number,
               status, reused_attempt_id, result_artifact_id,
               trade_ledger_artifact_id, result_summary, started_at,
               finished_at, error_message
        FROM systematic_fx.research_run_attempts
        WHERE research_run_attempt_id = %s
        FOR SHARE
        """,
        (reservation.research_run_attempt_id,),
    ).fetchone()
    row = _row_or_error(
        row,
        label=f"duplicate attempt {reservation.research_run_attempt_id}",
    )
    _assert_fields(
        label=f"duplicate attempt {reservation.research_run_attempt_id}",
        row=row,
        expected={
            "research_run_attempt_id": reservation.research_run_attempt_id,
            "research_run_spec_id": reservation.research_run_spec_id,
            "attempt_number": reservation.attempt_number,
            "status": "SKIPPED_DUPLICATE",
            "reused_attempt_id": reused_attempt_id,
            "result_artifact_id": None,
            "trade_ledger_artifact_id": None,
            "result_summary": {
                "reason": "EXACT_FINGERPRINT_ALREADY_SUCCEEDED",
                "reused_attempt_id": reused_attempt_id,
            },
            "started_at": None,
            "error_message": None,
        },
    )
    if row["finished_at"] is None:
        raise BarRegistryDriftError("duplicate attempt lacks finished_at")


def _validate_live_evidence_shard(
    project_root: Path,
    *,
    artifact: PublishedBarArtifact,
    record_kind: str,
    config: BarPatternResearchConfig,
    split_plan: BarSplitPlan,
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
    evidence_identity_sha256: str,
) -> None:
    base_schema = {
        "matches": _EVIDENCE_MATCH_BASE_SCHEMA,
        "replays": _EVIDENCE_REPLAY_BASE_SCHEMA,
    }[record_kind]
    expected_metadata = {
        b"systematic_fx.candidate_catalog_sha256": config.candidate_catalog_sha256.encode("ascii"),
        b"systematic_fx.config_semantic_sha256": config.semantic_sha256.encode("ascii"),
        b"systematic_fx.dataset_build_sha256": bar_dataset_manifest_sha256.encode("ascii"),
        b"systematic_fx.evidence_identity_sha256": evidence_identity_sha256.encode("ascii"),
        b"systematic_fx.evidence_schema": DISCOVERY_EVIDENCE_SCHEMA.encode("ascii"),
        b"systematic_fx.outcome_span_policy_sha256": (
            BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256.encode("ascii")
        ),
        b"systematic_fx.source_identity_sha256": bar_dataset_manifest_sha256.encode("ascii"),
        b"systematic_fx.source_manifest_sha256": raw_source_manifest_sha256.encode("ascii"),
        b"systematic_fx.split_plan_sha256": split_plan.sha256.encode("ascii"),
    }
    logical = artifact.descriptor.logical_identity
    expected_logical = {
        "candidate_catalog_sha256": config.candidate_catalog_sha256,
        "config_semantic_sha256": config.semantic_sha256,
        "dataset_manifest_sha256": bar_dataset_manifest_sha256,
        "evidence_identity_sha256": evidence_identity_sha256,
        "outcome_span_policy_sha256": BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
        "record_kind": record_kind,
        "row_count": artifact.descriptor.record_count,
        "schema_sha256": artifact.descriptor.schema_sha256,
        "source_identity_sha256": bar_dataset_manifest_sha256,
        "split_plan_sha256": split_plan.sha256,
    }
    if artifact.descriptor.source_manifest_sha256 != raw_source_manifest_sha256 or any(
        logical.get(key) != value for key, value in expected_logical.items()
    ):
        raise BarRegistryDriftError("registered evidence shard logical lineage drift")
    with (
        open_verified_bar_artifact(project_root, artifact) as opened,
        os.fdopen(os.dup(opened.descriptor), "rb") as source,
    ):
        parquet = pq.ParquetFile(source)
        schema = parquet.schema_arrow
        table = parquet.read()
        if (
            parquet.metadata.num_rows != artifact.descriptor.record_count
            or table.num_rows != artifact.descriptor.record_count
            or schema.remove_metadata() != base_schema
            or schema.metadata != expected_metadata
            or table.schema != schema
            or arrow_schema_sha256(schema) != artifact.descriptor.schema_sha256
        ):
            raise BarRegistryDriftError("live evidence shard Parquet schema/row drift")


def _validate_live_discovery_artifacts(
    connection: psycopg.Connection[dict[str, Any]],
    project_root: Path,
    *,
    config: BarPatternResearchConfig,
    split_plan: BarSplitPlan,
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
    global_artifact: PublishedBarArtifact,
    evidence_artifact: PublishedBarArtifact,
    lineage: Mapping[str, object],
    terminal_candidates: Mapping[str, Mapping[str, object]],
) -> tuple[tuple[str, ...], tuple[tuple[str, int], ...]]:
    with (
        open_verified_bar_artifact(project_root, global_artifact) as opened_global,
        open_verified_bar_artifact(project_root, evidence_artifact) as opened_evidence,
    ):
        global_document = _read_discovery_artifact_document(opened_global)
        evidence_document = _read_terminal_artifact_document(opened_evidence)
    expected_global = {
        "artifact_schema": DISCOVERY_RESULT_SCHEMA,
        "candidate_catalog_sha256": config.candidate_catalog_sha256,
        "candidate_count": ALLOCATED_VARIANT_COUNT,
        "config_semantic_sha256": config.semantic_sha256,
        "dataset_build_sha256": bar_dataset_manifest_sha256,
        "outcome_span_policy_sha256": BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
        "replay_catalog": [],
        "replay_catalog_count": 0,
        "source_identity_sha256": bar_dataset_manifest_sha256,
        "split_plan_sha256": split_plan.sha256,
    }
    if any(global_document.get(key) != expected for key, expected in expected_global.items()):
        raise BarRegistryDriftError("live global Discovery document lineage drift")
    evidence_embedded = global_document.get("evidence_manifest")
    if not isinstance(evidence_embedded, Mapping):
        raise BarRegistryDriftError("global Discovery document lacks evidence manifest")
    expected_evidence_document = {
        "candidate_catalog_sha256": config.candidate_catalog_sha256,
        "config_semantic_sha256": config.semantic_sha256,
        "dataset_build_sha256": bar_dataset_manifest_sha256,
        "evidence_identity_sha256": lineage["evidence_identity_sha256"],
        "evidence_schema": DISCOVERY_EVIDENCE_SCHEMA,
        "outcome_span_policy_sha256": BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
        "schema": DISCOVERY_EVIDENCE_SCHEMA,
        "source_identity_sha256": bar_dataset_manifest_sha256,
        "source_manifest_sha256": raw_source_manifest_sha256,
        "split_plan_sha256": split_plan.sha256,
        "spool_version": DISCOVERY_SPOOL_VERSION,
    }
    if any(
        evidence_document.get(key) != expected
        for key, expected in expected_evidence_document.items()
    ):
        raise BarRegistryDriftError("live Discovery evidence manifest lineage drift")
    shards = evidence_document.get("shards")
    if not isinstance(shards, Sequence) or isinstance(shards, (str, bytes)):
        raise BarRegistryDriftError("Discovery evidence manifest shards are not an array")
    if (
        len(shards) != lineage["evidence_shard_count"]
        or evidence_embedded.get("shards") != list(shards)
        or evidence_embedded.get("sha256") != evidence_artifact.sha256
        or evidence_embedded.get("artifact_identity_sha256")
        != evidence_artifact.descriptor.identity_sha256
        or evidence_embedded.get("evidence_identity_sha256") != lineage["evidence_identity_sha256"]
    ):
        raise BarRegistryDriftError("global/evidence manifest content disagreement")
    matched_count = 0
    replay_count = 0
    for shard in shards:
        if not isinstance(shard, Mapping):
            raise BarRegistryDriftError("evidence shard descriptor is not an object")
        record_kind = shard.get("record_kind")
        artifact_type = {
            "matches": BAR_EVIDENCE_MATCH_SHARD_ARTIFACT_TYPE,
            "replays": BAR_EVIDENCE_REPLAY_SHARD_ARTIFACT_TYPE,
        }.get(record_kind)
        if artifact_type is None:
            raise BarRegistryDriftError("evidence shard record kind drift")
        identity_sha256 = _sha256(
            shard.get("artifact_identity_sha256"),
            label="evidence shard artifact_identity_sha256",
        )
        artifact = _registered_lineage_artifact(
            connection,
            artifact_type=artifact_type,
            artifact_identity_sha256=identity_sha256,
        )
        relative_uri = shard.get("relative_uri")
        try:
            observed_relative_uri = artifact.path.relative_to(project_root).as_posix()
        except ValueError as error:
            raise BarRegistryDriftError("evidence shard is outside project_root") from error
        if (
            shard.get("artifact_descriptor") != artifact.descriptor.identity_document()
            or shard.get("sha256") != artifact.sha256
            or shard.get("byte_size") != artifact.byte_size
            or relative_uri != observed_relative_uri
            or shard.get("row_count") != artifact.descriptor.record_count
        ):
            raise BarRegistryDriftError("registered evidence shard descriptor drift")
        _validate_live_evidence_shard(
            project_root,
            artifact=artifact,
            record_kind=record_kind,
            config=config,
            split_plan=split_plan,
            raw_source_manifest_sha256=raw_source_manifest_sha256,
            bar_dataset_manifest_sha256=bar_dataset_manifest_sha256,
            evidence_identity_sha256=str(lineage["evidence_identity_sha256"]),
        )
        row_count = artifact.descriptor.record_count
        if record_kind == "matches":
            matched_count += row_count
        else:
            replay_count += row_count
    if (
        matched_count != lineage["evidence_matched_record_count"]
        or replay_count != lineage["evidence_replay_record_count"]
        or evidence_embedded.get("matched_record_count") != matched_count
        or evidence_embedded.get("replay_record_count") != replay_count
    ):
        raise BarRegistryDriftError("Discovery evidence record counts drift")

    global_candidates = global_document.get("candidate_results")
    if not isinstance(global_candidates, Sequence) or isinstance(global_candidates, (str, bytes)):
        raise BarRegistryDriftError("global candidate_results is not an array")
    if len(global_candidates) != ALLOCATED_VARIANT_COUNT:
        raise BarRegistryDriftError("global Discovery does not contain exactly 216 candidates")
    candidate_map: dict[str, Mapping[str, object]] = {}
    labels: Counter[str] = Counter()
    observed_order: list[str] = []
    evaluated_count = 0
    matched_signal_count = 0
    allowed_final_labels = {
        "SUPPORT_REJECT",
        "ECONOMIC_REJECT",
        "DISCOVERY_FINALIST_SELECTED",
        "DISCOVERY_FINALIST_BUDGET_REJECTED",
    }
    for candidate_result in global_candidates:
        if not isinstance(candidate_result, Mapping):
            raise BarRegistryDriftError("global candidate result is not an object")
        candidate_key = candidate_result.get("candidate_key")
        final_label = candidate_result.get("final_label")
        candidate_evaluated = candidate_result.get("evaluated_count")
        candidate_matched = candidate_result.get("matched_signal_count")
        if (
            not isinstance(candidate_key, str)
            or not isinstance(final_label, str)
            or final_label not in allowed_final_labels
            or isinstance(candidate_evaluated, bool)
            or not isinstance(candidate_evaluated, int)
            or candidate_evaluated < 0
            or isinstance(candidate_matched, bool)
            or not isinstance(candidate_matched, int)
            or candidate_matched < 0
        ):
            raise BarRegistryDriftError("global candidate result identity drift")
        if candidate_key in candidate_map:
            raise BarRegistryDriftError("global candidate result is duplicated")
        candidate_map[candidate_key] = candidate_result
        observed_order.append(candidate_key)
        labels[final_label] += 1
        evaluated_count += candidate_evaluated
        matched_signal_count += candidate_matched
    expected_order = [item.candidate_key for item in config.candidates]
    if observed_order != expected_order:
        raise BarRegistryDriftError("global candidate catalog order drift")
    if (
        global_document.get("evaluated_count") != evaluated_count
        or global_document.get("matched_signal_count") != matched_signal_count
    ):
        raise BarRegistryDriftError("global Discovery aggregate counts drift")
    for candidate_key, terminal_candidate in terminal_candidates.items():
        projected = dict(terminal_candidate)
        projected.pop("discovery_lineage", None)
        if candidate_map.get(candidate_key) != projected:
            raise BarRegistryDriftError(
                f"terminal/global candidate result disagreement for {candidate_key}"
            )
    finalist_keys = global_document.get("ranked_finalist_keys")
    budget_rejected_keys = global_document.get("budget_rejected_keys")
    if (
        not isinstance(finalist_keys, Sequence)
        or isinstance(finalist_keys, (str, bytes))
        or not all(isinstance(item, str) for item in finalist_keys)
        or len(set(finalist_keys)) != len(finalist_keys)
        or not isinstance(budget_rejected_keys, Sequence)
        or isinstance(budget_rejected_keys, (str, bytes))
        or not all(isinstance(item, str) for item in budget_rejected_keys)
    ):
        raise BarRegistryDriftError("global finalist keys are not an array")
    selected_set = {
        key
        for key, value in candidate_map.items()
        if value.get("final_label") == "DISCOVERY_FINALIST_SELECTED"
    }
    expected_budget_rejected = [
        key
        for key in observed_order
        if candidate_map[key].get("final_label") == "DISCOVERY_FINALIST_BUDGET_REJECTED"
    ]
    if (
        set(finalist_keys) != selected_set
        or len(finalist_keys) > int(dict(config.holdout_gates)["maximum_finalists"])
        or list(budget_rejected_keys) != expected_budget_rejected
    ):
        raise BarRegistryDriftError("global finalist/budget decision projection drift")
    return tuple(str(item) for item in finalist_keys), tuple(sorted(labels.items()))


def _validate_bar_campaign_results(
    database_url: str,
    project_root: Path,
    *,
    config: BarPatternResearchConfig,
    split_plan: BarSplitPlan,
    run_specs: Mapping[str, RunSpec],
    reservations: Mapping[str, RunAttemptReservation] | None,
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
) -> BarReuseValidationReport:
    url = _nonempty(database_url, label="database_url")
    raw_hash = _raw_source_sha256(raw_source_manifest_sha256)
    dataset_hash = _sha256(
        bar_dataset_manifest_sha256,
        label="bar_dataset_manifest_sha256",
    )
    expected_keys = tuple(item.candidate_key for item in config.candidates)
    if set(run_specs) != set(expected_keys):
        raise BarRegistryError("run_specs must contain the exact 216 candidate keys")
    if reservations is not None and set(reservations) != set(expected_keys):
        raise BarRegistryError("reservations must contain the exact 216 candidate keys")

    candidate_reports: list[BarReusedCandidateReport] = []
    terminal_candidates: dict[str, Mapping[str, object]] = {}
    consensus: dict[str, object] | None = None
    global_artifact: PublishedBarArtifact | None = None
    evidence_artifact: PublishedBarArtifact | None = None
    with psycopg.connect(url, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with connection.transaction():
            for candidate_key in expected_keys:
                candidate = config.candidate(candidate_key)
                run_spec = run_specs[candidate_key]
                reservation = None if reservations is None else reservations[candidate_key]
                if reservation is not None and reservation.execute:
                    continue
                if reservation is not None and (
                    reservation.status != "SKIPPED_DUPLICATE"
                    or reservation.reused_attempt_id is None
                ):
                    raise BarRegistryStateError("reused reservation is not SKIPPED_DUPLICATE")
                trial_parameters = candidate_trial_parameters(
                    config,
                    split_plan,
                    candidate,
                    raw_source_manifest_sha256=raw_hash,
                    bar_dataset_manifest_sha256=dataset_hash,
                )
                canonical_spec = _validate_expected_bar_run_spec(
                    run_spec,
                    config=config,
                    split_plan=split_plan,
                    candidate=candidate,
                    trial_parameters=trial_parameters,
                    raw_source_manifest_sha256=raw_hash,
                    bar_dataset_manifest_sha256=dataset_hash,
                )
                row = _completed_candidate_row(connection, candidate_key=candidate_key)
                expected_spec_id = (
                    int(row["research_run_spec_id"])
                    if reservation is None
                    else reservation.research_run_spec_id
                )
                _assert_fields(
                    label=f"completed candidate {candidate_key}",
                    row=row,
                    expected={
                        "trial_key": candidate_key,
                        "trial_type": "STRATEGY_VARIANT",
                        "parameters": trial_parameters,
                        "parameters_sha256": canonical_sha256(trial_parameters),
                        "research_run_spec_id": expected_spec_id,
                        "run_fingerprint": run_spec.fingerprint,
                        "canonical_spec": canonical_spec,
                        "experiment_key": BAR_CATALOG_EXPERIMENT_KEY,
                        "campaign_key": BAR_PATTERN_CAMPAIGN_KEY,
                        "succeeded_attempt_status": "SUCCEEDED",
                    },
                )
                if row["trial_status"] not in _TERMINAL_STATUSES:
                    raise BarRegistryStateError("completed candidate trial is not terminal")
                if (
                    row["trial_started_at"] is None
                    or row["trial_finished_at"] is None
                    or row["attempt_started_at"] is None
                    or row["attempt_finished_at"] is None
                    or row["trade_ledger_artifact_id"] is not None
                    or row["attempt_error_message"] is not None
                ):
                    raise BarRegistryDriftError(
                        "completed candidate terminal timestamps/artifact links drift"
                    )
                if reservation is not None:
                    reused_attempt_id = int(reservation.reused_attempt_id)
                    if int(row["succeeded_attempt_id"]) != reused_attempt_id:
                        raise BarRegistryDriftError("reservation reuses a different success")
                    _validate_duplicate_attempt(
                        connection,
                        reservation=reservation,
                        reused_attempt_id=reused_attempt_id,
                    )
                    duplicate_attempt_id: int | None = reservation.research_run_attempt_id
                else:
                    reused_attempt_id = int(row["succeeded_attempt_id"])
                    duplicate_attempt_id = None
                artifact = _artifact_from_database_row(_artifact_row_from_completed_candidate(row))
                summary = row["attempt_result_summary"]
                if not isinstance(summary, Mapping) or summary != row["trial_result_summary"]:
                    raise BarRegistryDriftError("trial and attempt terminal summaries disagree")
                compact = summary.get("compact_result")
                if not isinstance(compact, Mapping):
                    raise BarRegistryDriftError("terminal summary compact_result is absent")
                result = BarTerminalResult(
                    candidate_key=candidate_key,
                    candidate_definition_sha256=candidate.definition_sha256,
                    run_fingerprint=run_spec.fingerprint,
                    research_run_attempt_id=reused_attempt_id,
                    trial_status=str(row["trial_status"]),
                    decision_label=str(summary.get("decision_label")),
                    compact_result=compact,
                    artifact=artifact,
                )
                expected_summary = _terminal_summary(
                    result,
                    result_artifact_id=int(row["result_artifact_id"]),
                )
                if summary != expected_summary:
                    raise BarRegistryDriftError(
                        "terminal attempt/trial summary is not the exact compact projection"
                    )
                with open_verified_bar_artifact(project_root, artifact) as opened:
                    document = _read_terminal_artifact_document(opened)
                trial = {"parameters": trial_parameters}
                _validate_terminal_artifact_contract(
                    config=config,
                    trial=trial,
                    canonical_spec=row["canonical_spec"],
                    result=result,
                    document=document,
                )
                candidate_result = document["candidate_result"]
                if not isinstance(candidate_result, Mapping):  # pragma: no cover
                    raise BarRegistryDriftError("terminal candidate result is not an object")
                lineage = candidate_result["discovery_lineage"]
                if not isinstance(lineage, Mapping):  # pragma: no cover - contract validated
                    raise BarRegistryDriftError("terminal candidate lineage is not an object")
                lineage_consensus = {
                    key: lineage[key]
                    for key in (
                        "discovery_result_sha256",
                        "evidence_artifact_identity_sha256",
                        "evidence_identity_sha256",
                        "evidence_manifest_sha256",
                        "evidence_matched_record_count",
                        "evidence_replay_record_count",
                        "evidence_shard_count",
                        "global_result_artifact_identity_sha256",
                        "global_result_artifact_sha256",
                    )
                }
                if consensus is None:
                    consensus = lineage_consensus
                    global_artifact, evidence_artifact = _registered_discovery_lineage(
                        connection,
                        config=config,
                        trial={"parameters": trial_parameters},
                        candidate_result=candidate_result,
                    )
                elif lineage_consensus != consensus:
                    raise BarRegistryDriftError("terminal candidates disagree on Discovery lineage")
                terminal_candidates[candidate_key] = candidate_result
                candidate_reports.append(
                    BarReusedCandidateReport(
                        candidate_key=candidate_key,
                        run_fingerprint=run_spec.fingerprint,
                        duplicate_attempt_id=duplicate_attempt_id,
                        reused_attempt_id=reused_attempt_id,
                        trial_status=str(row["trial_status"]),
                        final_label=str(summary.get("final_label")),
                        terminal_artifact_sha256=artifact.sha256,
                    )
                )
            if not candidate_reports or consensus is None:
                raise BarRegistryStateError("there are no completed candidate results to validate")
            if global_artifact is None or evidence_artifact is None:  # pragma: no cover
                raise BarRegistryDriftError("Discovery lineage artifacts were not resolved")
            finalist_keys, final_label_counts = _validate_live_discovery_artifacts(
                connection,
                project_root,
                config=config,
                split_plan=split_plan,
                raw_source_manifest_sha256=raw_hash,
                bar_dataset_manifest_sha256=dataset_hash,
                global_artifact=global_artifact,
                evidence_artifact=evidence_artifact,
                lineage=consensus,
                terminal_candidates=terminal_candidates,
            )
    return BarReuseValidationReport(
        candidates=tuple(candidate_reports),
        global_result_artifact_sha256=str(consensus["global_result_artifact_sha256"]),
        global_result_artifact_identity_sha256=str(
            consensus["global_result_artifact_identity_sha256"]
        ),
        evidence_manifest_sha256=str(consensus["evidence_manifest_sha256"]),
        evidence_artifact_identity_sha256=str(consensus["evidence_artifact_identity_sha256"]),
        evidence_identity_sha256=str(consensus["evidence_identity_sha256"]),
        finalist_keys=finalist_keys,
        final_label_counts=final_label_counts,
    )


@_translate_psycopg_errors("reused bar-attempt validation")
def validate_reused_bar_attempts(
    database_url: str,
    project_root: Path,
    *,
    config: BarPatternResearchConfig,
    split_plan: BarSplitPlan,
    reservations: Mapping[str, tuple[RunSpec, RunAttemptReservation]],
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
) -> BarReuseValidationReport:
    """Validate every duplicate subset member and one shared live Discovery lineage."""

    if not isinstance(reservations, Mapping):
        raise BarRegistryError("reservations must be a mapping")
    run_specs: dict[str, RunSpec] = {}
    attempts: dict[str, RunAttemptReservation] = {}
    for candidate_key, pair in reservations.items():
        if not isinstance(pair, tuple) or len(pair) != 2:
            raise BarRegistryError("reservation values must be (RunSpec, RunAttemptReservation)")
        run_spec, reservation = pair
        if not isinstance(run_spec, RunSpec) or not isinstance(reservation, RunAttemptReservation):
            raise BarRegistryError("reservation values have unsupported types")
        run_specs[candidate_key] = run_spec
        attempts[candidate_key] = reservation
    return _validate_bar_campaign_results(
        database_url,
        project_root,
        config=config,
        split_plan=split_plan,
        run_specs=run_specs,
        reservations=attempts,
        raw_source_manifest_sha256=raw_source_manifest_sha256,
        bar_dataset_manifest_sha256=bar_dataset_manifest_sha256,
    )


@_translate_psycopg_errors("completed bar-campaign validation")
def validate_completed_bar_campaign(
    database_url: str,
    project_root: Path,
    *,
    config: BarPatternResearchConfig,
    split_plan: BarSplitPlan,
    run_specs: Mapping[str, RunSpec],
    raw_source_manifest_sha256: str,
    bar_dataset_manifest_sha256: str,
) -> BarReuseValidationReport:
    """Read-only validate all 216 terminal candidates and their live shared artifacts."""

    return _validate_bar_campaign_results(
        database_url,
        project_root,
        config=config,
        split_plan=split_plan,
        run_specs=run_specs,
        reservations=None,
        raw_source_manifest_sha256=raw_source_manifest_sha256,
        bar_dataset_manifest_sha256=bar_dataset_manifest_sha256,
    )
