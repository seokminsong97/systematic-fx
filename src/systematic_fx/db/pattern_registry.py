"""Append-tracked Phase 1A pattern observations backed by governed QUERY runs.

The current pattern ledger is a compact roll-up.  The append-preserved source of
truth for each slice is its ``discovery_exposures`` row and immutable result
artifact.  This module only appends a new slice pointer to the roll-up after it
has proved that the exposure belongs to an all-variable QUERY ``RunSpec`` whose
canonical parameters contain the exact candidate-query definition.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from functools import wraps
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final, ParamSpec, TypeVar
from urllib.parse import unquote, urlparse

import psycopg
from psycopg import IsolationLevel
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.db.postgres_retry import retry_serialization_failures
from systematic_fx.features.screening import FEATURE_VERSION, FORMULA_SHA256
from systematic_fx.research.discovery_slice import (
    DISCOVERY_SLICE_SCHEMA,
    DISCOVERY_SLICE_VERSION,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256

_SHA256: Final = re.compile(r"[0-9a-f]{64}")
_DIRECTIONS: Final = frozenset({"LONG", "SHORT", "BOTH", "NONE"})
_ROLLUP_SCHEMA: Final = "systematic_fx.phase1a_pattern_rollup.v1"
_QUERY_RESULT_LIMIT: Final = 20
_SOURCE_MANIFEST_KEYS: Final = {
    "mbp10_footer_manifest_v1",
    "mbp10_source_sha256_v1",
    "mbp10_structural_qc_v1",
}
_RECOVERY_CONTROL_SCHEMA: Final = "systematic_fx.phase1a_partial_recovery_control.v1"
_RECOVERY_MANIFEST_SCHEMA: Final = "systematic_fx.phase1a_partial_recovery_manifest.v1"
_RECOVERY_PROJECTION_SCHEMA: Final = "systematic_fx.phase1a_query_recovery_projection.v1"
_RECOVERY_REGISTRAR_SCHEMA: Final = "systematic_fx.phase1a_pattern_recovery_registrar.v1"
_RECOVERY_CONTROL_ENGINE: Final = "phase1a_partial_recovery_control_v1"
_P = ParamSpec("_P")
_R = TypeVar("_R")


class PatternRegistryError(RuntimeError):
    """A pattern observation is incomplete or cannot be persisted safely."""


class PatternRegistryDriftError(PatternRegistryError):
    """Existing immutable pattern or exposure content differs from the request."""


class PatternRegistryDatabaseError(PatternRegistryError):
    """PostgreSQL rejected the pattern registration transaction."""


def _translate_psycopg_errors(
    operation: str,
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    """Retry serialization conflicts before exposing the stable pattern API."""

    def decorate(function: Callable[_P, _R]) -> Callable[_P, _R]:
        @wraps(function)
        def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            try:
                return retry_serialization_failures(function, *args, **kwargs)
            except PatternRegistryError:
                raise
            except psycopg.Error as exc:
                raise PatternRegistryDatabaseError(f"PostgreSQL {operation} failed") from exc

        return wrapped

    return decorate


def _nonempty(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise PatternRegistryError(f"{label} must be a canonical non-empty string")
    return value


def _sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise PatternRegistryError(f"{label} must be a lowercase SHA-256")
    return value


def _canonical_mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise PatternRegistryError(f"{label} must be a mapping")
    try:
        detached = json.loads(canonical_json_bytes(value))
    except (TypeError, ValueError) as exc:
        raise PatternRegistryError(f"{label} must be strict canonical JSON") from exc
    if not isinstance(detached, dict):  # pragma: no cover - Mapping always encodes an object
        raise PatternRegistryError(f"{label} must encode a JSON object")
    return MappingProxyType(detached)


def _canonical_sequence(
    value: object,
    *,
    label: str,
) -> tuple[Mapping[str, object], ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise PatternRegistryError(f"{label} must be an ordered sequence")
    return tuple(
        _canonical_mapping(item, label=f"{label}[{index}]") for index, item in enumerate(value)
    )


@dataclass(frozen=True, slots=True)
class PatternSliceObservation:
    """One query result exposed from one non-overlapping Discovery slice."""

    campaign_key: str
    pattern_key: str
    query_id: str
    run_fingerprint: str
    exposure_key: str
    query_definition: Mapping[str, object]
    feature_identity: Mapping[str, object]
    direction: str
    entry_condition: str
    economic_rationale: str
    applicable_regime: Mapping[str, object]
    counterexamples: Sequence[Mapping[str, object]]
    support_count: int
    candidate_barrier_region: Mapping[str, object]
    forward_first_touch_summary: Mapping[str, object]
    cost_assumptions: Mapping[str, object]

    def __post_init__(self) -> None:
        for field_name in (
            "campaign_key",
            "pattern_key",
            "query_id",
            "exposure_key",
            "entry_condition",
            "economic_rationale",
        ):
            object.__setattr__(
                self,
                field_name,
                _nonempty(getattr(self, field_name), label=field_name),
            )
        object.__setattr__(
            self,
            "run_fingerprint",
            _sha256(self.run_fingerprint, label="run_fingerprint"),
        )
        if self.direction not in _DIRECTIONS:
            raise PatternRegistryError(f"direction must be one of {sorted(_DIRECTIONS)}")
        if (
            isinstance(self.support_count, bool)
            or not isinstance(self.support_count, int)
            or self.support_count < 0
        ):
            raise PatternRegistryError("support_count must be a non-negative integer")
        for field_name in (
            "query_definition",
            "feature_identity",
            "applicable_regime",
            "candidate_barrier_region",
            "forward_first_touch_summary",
            "cost_assumptions",
        ):
            object.__setattr__(
                self,
                field_name,
                _canonical_mapping(getattr(self, field_name), label=field_name),
            )
        object.__setattr__(
            self,
            "counterexamples",
            _canonical_sequence(self.counterexamples, label="counterexamples"),
        )
        query_id = self.query_definition.get("id")
        if query_id != self.query_id:
            raise PatternRegistryError("query_definition.id must equal query_id")

    @property
    def query_definition_sha256(self) -> str:
        return canonical_sha256(self.query_definition)


@dataclass(frozen=True, slots=True)
class PatternObservationReport:
    """Database identity and whether a new slice pointer changed the roll-up."""

    pattern_id: int
    pattern_key: str
    campaign_id: int
    discovery_exposure_id: int
    research_run_spec_id: int
    result_artifact_id: int
    created_pattern: bool
    appended_observation: bool


@dataclass(frozen=True, slots=True)
class _ExposureIdentity:
    discovery_exposure_id: int
    research_run_spec_id: int
    result_artifact_id: int
    source_interval_start: datetime
    source_interval_end: datetime


@dataclass(frozen=True, slots=True)
class _OpenedDiscoveryEvidence:
    descriptor: int
    path: Path
    identity: tuple[int, int, int, int, int, int]
    query_result: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class _OpenedRecoveryEvidence:
    descriptor: int
    path: Path
    identity: tuple[int, int, int, int, int, int]


def _file_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _drift_sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise PatternRegistryDriftError(f"{label} must be a lowercase SHA-256")
    return value


def _required_array(value: object, *, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise PatternRegistryDriftError(f"{label} must be a JSON array")
    return value


def _artifact_path(
    exposure_row: Mapping[str, Any],
) -> tuple[Path, Path, str, int]:
    uri = exposure_row.get("artifact_uri")
    if not isinstance(uri, str):
        raise PatternRegistryDriftError("Discovery result artifact URI is missing")
    parsed = urlparse(uri)
    if (
        parsed.scheme != "file"
        or parsed.netloc not in {"", "localhost"}
        or parsed.query
        or parsed.fragment
    ):
        raise PatternRegistryDriftError("Discovery result artifact must use a plain local file URI")
    path = Path(unquote(parsed.path))
    expected_sha256 = _drift_sha256(
        exposure_row.get("artifact_sha256"),
        label="Discovery result artifact",
    )
    expected_size = exposure_row.get("artifact_byte_size")
    if isinstance(expected_size, bool) or not isinstance(expected_size, int) or expected_size <= 0:
        raise PatternRegistryDriftError("Discovery result artifact byte size is invalid")
    if not path.is_absolute() or path.name != f"sha256={expected_sha256}.json":
        raise PatternRegistryDriftError("Discovery result artifact path is not content-addressed")
    matches = [
        index
        for index in range(len(path.parts) - 1)
        if path.parts[index : index + 2] == ("data", "derived")
    ]
    if len(matches) != 1:
        raise PatternRegistryDriftError("Discovery result artifact must be below one data/derived")
    derived_root = Path(*path.parts[: matches[0] + 2])
    try:
        resolved = path.resolve(strict=True)
        resolved_derived = derived_root.resolve(strict=True)
    except OSError as exc:
        raise PatternRegistryDriftError("Discovery result artifact is no longer reachable") from exc
    if (
        resolved != path
        or resolved_derived != derived_root
        or not path.is_relative_to(derived_root)
    ):
        raise PatternRegistryDriftError("Discovery result artifact path contains a symlink")
    return path, derived_root, expected_sha256, expected_size


def _open_stable_artifact(
    exposure_row: Mapping[str, Any],
) -> tuple[int, Path, Path, tuple[int, int, int, int, int, int], bytes, str]:
    path, derived_root, expected_sha256, expected_size = _artifact_path(exposure_row)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise PatternRegistryDriftError("cannot open Discovery result artifact safely") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise PatternRegistryDriftError("Discovery result artifact is not a regular file")
        identity = _file_identity(before)
        digest = hashlib.sha256()
        chunks: list[bytes] = []
        byte_size = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
            chunks.append(chunk)
            byte_size += len(chunk)
        raw = b"".join(chunks)
        if (
            _file_identity(os.fstat(descriptor)) != identity
            or byte_size != before.st_size
            or byte_size != expected_size
            or digest.hexdigest() != expected_sha256
        ):
            raise PatternRegistryDriftError(
                "Discovery result artifact changed or differs from its registered identity"
            )
        return descriptor, path, derived_root, identity, raw, expected_sha256
    except Exception:
        os.close(descriptor)
        raise


def _verify_open_artifact_binding(evidence: _OpenedDiscoveryEvidence) -> None:
    try:
        descriptor_identity = _file_identity(os.fstat(evidence.descriptor))
    except OSError as exc:
        raise PatternRegistryDriftError(
            "Discovery result artifact descriptor disappeared before pattern commit"
        ) from exc
    if descriptor_identity != evidence.identity:
        raise PatternRegistryDriftError(
            "Discovery result artifact inode changed before pattern commit"
        )
    try:
        path_identity = _file_identity(evidence.path.lstat())
    except OSError as exc:
        raise PatternRegistryDriftError(
            "Discovery result artifact path disappeared before pattern commit"
        ) from exc
    if path_identity != evidence.identity or not stat.S_ISREG(path_identity[2]):
        raise PatternRegistryDriftError(
            "Discovery result artifact path changed before pattern commit"
        )


def _plain(value: Mapping[str, object]) -> dict[str, object]:
    decoded = json.loads(canonical_json_bytes(value))
    if not isinstance(decoded, dict):  # pragma: no cover - Mapping encodes an object
        raise PatternRegistryError("canonical mapping did not encode an object")
    return decoded


def _required_object(value: object, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PatternRegistryDriftError(f"{label} must be a JSON object")
    return value


def _exact_keys(value: Mapping[str, object], *, expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise PatternRegistryDriftError(f"{label} field schema drift")


def _aware_utc(value: object, *, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise PatternRegistryDriftError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _validate_governed_query(
    run_row: Mapping[str, Any],
    exposure_row: Mapping[str, Any],
    observation: PatternSliceObservation,
) -> _ExposureIdentity:
    if run_row.get("run_kind") != "QUERY":
        raise PatternRegistryDriftError("pattern observations require a QUERY RunSpec")
    if run_row.get("run_fingerprint") != observation.run_fingerprint:
        raise PatternRegistryDriftError("QUERY RunSpec fingerprint differs from the observation")
    canonical_spec = _required_object(run_row.get("canonical_spec"), label="canonical_spec")
    if canonical_sha256(canonical_spec) != observation.run_fingerprint:
        raise PatternRegistryDriftError("QUERY RunSpec canonical content fingerprint drift")
    parameters = _required_object(canonical_spec.get("parameters"), label="RunSpec.parameters")
    definition = _plain(observation.query_definition)
    if parameters.get("candidate_query") != definition:
        raise PatternRegistryDriftError("RunSpec candidate_query differs from the observation")
    if parameters.get("query_definition_sha256") != observation.query_definition_sha256:
        raise PatternRegistryDriftError("RunSpec query definition SHA-256 differs")

    if exposure_row.get("research_run_spec_id") != run_row.get("research_run_spec_id"):
        raise PatternRegistryDriftError("Discovery exposure belongs to a different RunSpec")
    if exposure_row.get("exposure_key") != observation.exposure_key:
        raise PatternRegistryDriftError("Discovery exposure key differs from the observation")
    if exposure_row.get("exposure_type") != "QUERY":
        raise PatternRegistryDriftError("pattern observations require a QUERY exposure")
    if exposure_row.get("visible_to_ai") is not True:
        raise PatternRegistryDriftError("pattern observation exposure must be AI-visible")
    if exposure_row.get("research_eligible") is not False:
        raise PatternRegistryDriftError("Phase1A query exposure cannot claim research eligibility")
    result_artifact_id = exposure_row.get("result_artifact_id")
    if isinstance(result_artifact_id, bool) or not isinstance(result_artifact_id, int):
        raise PatternRegistryDriftError("query exposure requires an immutable result artifact")
    query_spec = _required_object(exposure_row.get("query_spec"), label="exposure.query_spec")
    expected_query_spec = {
        "candidate_query": definition,
        "query_definition_sha256": observation.query_definition_sha256,
        "run_fingerprint": observation.run_fingerprint,
    }
    if query_spec != expected_query_spec:
        raise PatternRegistryDriftError("Discovery exposure query_spec is not the exact query")
    start = _aware_utc(
        exposure_row.get("source_interval_start"),
        label="source_interval_start",
    )
    end = _aware_utc(exposure_row.get("source_interval_end"), label="source_interval_end")
    if start > end:
        raise PatternRegistryDriftError("Discovery exposure interval is reversed")
    return _ExposureIdentity(
        discovery_exposure_id=int(exposure_row["discovery_exposure_id"]),
        research_run_spec_id=int(run_row["research_run_spec_id"]),
        result_artifact_id=result_artifact_id,
        source_interval_start=start,
        source_interval_end=end,
    )


def _expected_counterexamples(query_result: Mapping[str, object]) -> list[dict[str, object]]:
    occurrences = _required_array(query_result.get("occurrences"), label="query occurrences")
    selected: list[dict[str, object]] = []
    for index, occurrence_value in enumerate(occurrences):
        occurrence = _required_object(
            occurrence_value,
            label=f"query occurrences[{index}]",
        )
        forward = _required_object(
            occurrence.get("forward"),
            label=f"query occurrences[{index}].forward",
        )
        horizon = forward.get("12")
        if horizon is None:
            continue
        horizon_result = _required_object(
            horizon,
            label=f"query occurrences[{index}].forward.12",
        )
        aligned = horizon_result.get("aligned_close_x2_ticks")
        if isinstance(aligned, bool) or not isinstance(aligned, int):
            raise PatternRegistryDriftError(
                "query occurrence horizon 12 aligned close must be an integer"
            )
        if aligned <= 0:
            selected.append(occurrence)
            if len(selected) == _QUERY_RESULT_LIMIT:
                break
    return selected


def _economic_rationale(
    definition: Mapping[str, Any],
    frozen_inputs: Mapping[str, Any],
) -> str:
    parent_ids = _required_array(
        definition.get("parent_hypothesis_ids"),
        label="candidate query parent_hypothesis_ids",
    )
    if not parent_ids or not all(isinstance(value, str) and value for value in parent_ids):
        raise PatternRegistryDriftError("candidate query parent hypotheses are invalid")
    hypotheses_input = _required_object(
        frozen_inputs.get("parent_hypotheses"),
        label="frozen parent hypotheses",
    )
    hypotheses_document = _required_object(
        hypotheses_input.get("document"),
        label="frozen parent hypotheses document",
    )
    hypotheses = _required_array(
        hypotheses_document.get("hypotheses"),
        label="frozen parent hypotheses list",
    )
    by_id: dict[str, Mapping[str, Any]] = {}
    for index, value in enumerate(hypotheses):
        hypothesis = _required_object(value, label=f"frozen hypothesis[{index}]")
        hypothesis_id = hypothesis.get("id")
        if not isinstance(hypothesis_id, str) or hypothesis_id in by_id:
            raise PatternRegistryDriftError("frozen parent hypothesis identity drift")
        by_id[hypothesis_id] = hypothesis
    rationales: list[str] = []
    for parent_id in parent_ids:
        parent = by_id.get(parent_id)
        if parent is None:
            raise PatternRegistryDriftError("candidate query names an unknown parent hypothesis")
        rationale = parent.get("economic_rationale")
        if not isinstance(rationale, str) or not rationale:
            raise PatternRegistryDriftError("parent hypothesis economic rationale is missing")
        rationales.append(f"{parent_id}: {rationale}")
    return " | ".join(rationales)


def derive_phase1a_pattern_observation(
    *,
    campaign_key: str,
    query_run_fingerprint: str,
    exposure_key: str,
    query_run_spec: Mapping[str, Any],
    ai_run_spec: Mapping[str, Any],
    artifact_sha256: str,
    artifact_document: Mapping[str, Any],
    query_result: Mapping[str, Any],
    rollup_registrar: Mapping[str, object] | None = None,
) -> PatternSliceObservation:
    """Derive one pattern observation only from immutable governed evidence."""

    campaign = _nonempty(campaign_key, label="campaign_key")
    run_fingerprint = _sha256(query_run_fingerprint, label="query_run_fingerprint")
    artifact_digest = _sha256(artifact_sha256, label="artifact_sha256")
    definition = _required_object(query_result.get("definition"), label="query definition")
    query_id = _nonempty(definition.get("id"), label="query_id")
    conditions = _required_array(definition.get("conditions"), label="query conditions")
    direction_rule = _nonempty(definition.get("direction_rule"), label="direction_rule")
    if not all(isinstance(condition, str) for condition in conditions):
        raise PatternRegistryError("query conditions must contain strings")
    query_parameters = _required_object(
        query_run_spec.get("parameters"),
        label="QUERY parameters",
    )
    ai_parameters = _required_object(
        ai_run_spec.get("parameters"),
        label="AI_SLICE parameters",
    )
    frozen_inputs = _required_object(
        query_parameters.get("frozen_toml_inputs"),
        label="QUERY frozen_toml_inputs",
    )
    if ai_parameters.get("frozen_toml_inputs") != frozen_inputs:
        raise PatternRegistryDriftError("QUERY and AI_SLICE frozen inputs differ")
    source_hashes = _required_object(
        ai_run_spec.get("source_manifest_hashes"),
        label="AI_SLICE source_manifest_hashes",
    )
    calendar = _required_object(
        ai_run_spec.get("eligible_calendar"),
        label="AI_SLICE eligible_calendar",
    )
    feature = _required_object(ai_run_spec.get("feature"), label="AI_SLICE feature")
    config = _required_object(artifact_document.get("config"), label="Discovery config")
    feature_inputs = _required_array(
        artifact_document.get("feature_inputs"),
        label="Discovery feature_inputs",
    )
    feature_identity: dict[str, object] = {
        "calendar_sha256": calendar.get("sha256"),
        "code_snapshot_sha256": ai_run_spec.get("code_snapshot_sha256"),
        "discovery_artifact_sha256": artifact_digest,
        "discovery_config_sha256": config.get("sha256"),
        "feature_config_sha256": feature.get("sha256"),
        "feature_inputs": feature_inputs,
        "feature_manifest_sha256": ai_parameters.get("feature_manifest_sha256"),
        "feature_version": feature.get("version"),
        "footer_manifest_sha256": source_hashes.get("mbp10_footer_manifest_v1"),
        "formula_sha256": FORMULA_SHA256,
        "qc_manifest_sha256": source_hashes.get("mbp10_structural_qc_v1"),
        "source_manifest_sha256": source_hashes.get("mbp10_source_sha256_v1"),
    }
    if rollup_registrar is not None:
        feature_identity["rollup_registrar"] = dict(rollup_registrar)

    barrier_input = _required_object(
        frozen_inputs.get("barrier_grid"),
        label="frozen barrier grid",
    )
    barrier_document = _required_object(
        barrier_input.get("document"),
        label="frozen barrier grid document",
    )
    barrier_grid = _required_object(
        barrier_document.get("barrier_grid"),
        label="frozen barrier_grid table",
    )
    cost = _required_object(ai_run_spec.get("cost"), label="AI_SLICE cost")
    cost_input = _required_object(frozen_inputs.get("cost"), label="frozen cost")
    cost_document = _required_object(cost_input.get("document"), label="frozen cost document")
    variable_cost = _required_object(
        cost_document.get("variable_cost"),
        label="frozen variable cost",
    )
    fixed_cost = _required_object(
        cost_document.get("fully_loaded_fixed_allocation"),
        label="frozen fixed cost",
    )
    economic_floor = _required_object(
        cost_document.get("economic_floor"),
        label="frozen economic floor",
    )
    signal_policy = _required_object(
        ai_run_spec.get("signal_policy"),
        label="AI_SLICE signal policy",
    )
    support_count = query_result.get("support_count")
    if isinstance(support_count, bool) or not isinstance(support_count, int):
        raise PatternRegistryDriftError("query support_count is invalid")
    return PatternSliceObservation(
        campaign_key=campaign,
        pattern_key=f"{campaign}:{query_id}",
        query_id=query_id,
        run_fingerprint=run_fingerprint,
        exposure_key=_nonempty(exposure_key, label="exposure_key"),
        query_definition=definition,
        feature_identity=feature_identity,
        direction=str(query_run_spec.get("direction")),
        entry_condition=f"direction_rule={direction_rule}; " + "; ".join(conditions),
        economic_rationale=_economic_rationale(definition, frozen_inputs),
        applicable_regime={
            "authority": "OPEN_OBSERVATION",
            "definition_status_available": False,
            "parent_hypothesis_ids": definition.get("parent_hypothesis_ids"),
            "research_eligible": False,
            "screening_only": True,
            "signal_cadence_seconds": signal_policy.get("signal_cadence_seconds"),
        },
        counterexamples=_expected_counterexamples(query_result),
        support_count=support_count,
        candidate_barrier_region={
            "cell_count": barrier_grid.get("expected_cell_count"),
            "stop_loss_pips": barrier_grid.get("stop_loss_pips"),
            "stop_loss_ticks": barrier_grid.get("stop_loss_ticks"),
            "status": "NOT_EVALUATED_IN_DISCOVERY_SLICE",
            "take_profit_pips": barrier_grid.get("take_profit_pips"),
            "take_profit_ticks": barrier_grid.get("take_profit_ticks"),
        },
        forward_first_touch_summary={
            "direction_counts": query_result.get("direction_counts"),
            "forward_close_and_excursion_proxy": query_result.get("forward"),
            "first_touch_status": "NOT_COMPUTED",
            "reason": "DISCOVERY_SLICE_HAS_NO_EVENT_LEVEL_FIRST_TOUCH_OUTCOME",
            "source_date_count": query_result.get("source_date_count"),
        },
        cost_assumptions={
            "allocated_fixed_cost_ticks": fixed_cost.get(
                "allocated_fixed_cost_ticks_per_round_trip"
            ),
            "baseline_cost_floor_ticks": economic_floor.get("baseline_minimum_take_profit_ticks"),
            "cost_config_sha256": cost.get("sha256"),
            "cost_model_version": cost.get("version"),
            "status": "RECORDED_NOT_APPLIED_TO_FORWARD_PROXY",
            "variable_cost_ticks": variable_cost.get("round_trip_debit_ticks"),
        },
    )


def _validate_observation_evidence(
    *,
    run_spec: Mapping[str, Any],
    parent_spec: Mapping[str, Any],
    artifact_document: Mapping[str, Any],
    artifact_sha256: str,
    query_result: Mapping[str, Any],
    observation: PatternSliceObservation,
) -> None:
    expected_pattern_key = f"{observation.campaign_key}:{observation.query_id}"
    if observation.pattern_key != expected_pattern_key:
        raise PatternRegistryDriftError("pattern_key is not the canonical campaign/query identity")
    parameters = _required_object(run_spec.get("parameters"), label="QUERY parameters")
    recovery_projection = parameters.get("recovery_projection")
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
    if any(parent_spec.get(key) != run_spec.get(key) for key in shared_research_fields):
        raise PatternRegistryDriftError("QUERY and AI_SLICE shared research identity drift")
    shared_execution_fields = (
        "code_commit",
        "code_snapshot_sha256",
        "dependency_lock_sha256",
        "runtime_environment",
    )
    if recovery_projection is None:
        if any(parent_spec.get(key) != run_spec.get(key) for key in shared_execution_fields):
            raise PatternRegistryDriftError("QUERY and AI_SLICE shared execution identity drift")
    else:
        recovery = _required_object(
            recovery_projection,
            label="QUERY recovery_projection",
        )
        _exact_keys(
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
            label="QUERY recovery_projection",
        )
        expected_recovery_source = {
            "artifact_schema": _RECOVERY_PROJECTION_SCHEMA,
            "mode": "IMMUTABLE_AI_ARTIFACT_PROJECTION",
            "no_research_recomputation": True,
            "source_ai_canonical_sha256": canonical_sha256(parent_spec),
            "source_ai_code_snapshot_sha256": parent_spec.get("code_snapshot_sha256"),
            "source_ai_run_fingerprint": canonical_sha256(parent_spec),
            "recovery_code_commit": run_spec.get("code_commit"),
            "recovery_code_snapshot_sha256": run_spec.get("code_snapshot_sha256"),
        }
        if any(recovery.get(key) != value for key, value in expected_recovery_source.items()):
            raise PatternRegistryDriftError("QUERY recovery projection provenance drift")
        for key in (
            "recovery_manifest_sha256",
            "recovery_runtime_sha256",
            "recovery_control_run_fingerprint",
            "source_artifact_sha256",
        ):
            _drift_sha256(recovery.get(key), label=f"QUERY recovery projection {key}")
        for key in ("recovery_manifest_artifact_id", "source_artifact_id"):
            value = recovery.get(key)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise PatternRegistryDriftError(
                    f"QUERY recovery projection {key} must be a positive integer"
                )
        if not isinstance(recovery.get("recovery_manifest_relative_path"), str):
            raise PatternRegistryDriftError("QUERY recovery projection manifest path is invalid")
        if recovery.get("source_artifact_sha256") != artifact_sha256:
            raise PatternRegistryDriftError("QUERY recovery source artifact SHA-256 drift")
    parent_parameters = _required_object(
        parent_spec.get("parameters"),
        label="AI_SLICE parent parameters",
    )
    frozen_inputs = _required_object(
        parameters.get("frozen_toml_inputs"),
        label="QUERY frozen_toml_inputs",
    )
    if parent_parameters.get("frozen_toml_inputs") != frozen_inputs:
        raise PatternRegistryDriftError("QUERY and AI_SLICE frozen inputs differ")

    eligible_calendar = _required_object(
        run_spec.get("eligible_calendar"),
        label="QUERY eligible_calendar",
    )
    feature = _required_object(run_spec.get("feature"), label="QUERY feature")
    source_hashes = _required_object(
        run_spec.get("source_manifest_hashes"),
        label="QUERY source_manifest_hashes",
    )
    if set(source_hashes) != _SOURCE_MANIFEST_KEYS:
        raise PatternRegistryDriftError("QUERY source manifest identity set drift")
    for key in _SOURCE_MANIFEST_KEYS:
        _drift_sha256(source_hashes[key], label=f"QUERY source manifest {key}")
    calendar_sha256 = _drift_sha256(
        eligible_calendar.get("sha256"),
        label="QUERY eligible calendar",
    )
    feature_sha256 = _drift_sha256(feature.get("sha256"), label="QUERY feature config")
    analysis_code_snapshot_sha256 = _drift_sha256(
        parent_spec.get("code_snapshot_sha256"),
        label="AI_SLICE analysis code snapshot",
    )
    if artifact_document.get("code_snapshot_sha256") != analysis_code_snapshot_sha256:
        raise PatternRegistryDriftError("Discovery artifact code snapshot differs from RunSpecs")
    if feature.get("version") != FEATURE_VERSION:
        raise PatternRegistryDriftError("QUERY feature version drift")

    config = _required_object(
        artifact_document.get("config"),
        label="Discovery artifact config",
    )
    discovery_config_sha256 = _drift_sha256(
        config.get("sha256"),
        label="Discovery query config",
    )
    discovery_frozen = _required_object(
        frozen_inputs.get("discovery_query"),
        label="frozen Discovery query config",
    )
    if discovery_frozen.get("sha256") != discovery_config_sha256:
        raise PatternRegistryDriftError("Discovery artifact and frozen query config differ")
    feature_inputs = _required_array(
        artifact_document.get("feature_inputs"),
        label="Discovery artifact feature_inputs",
    )
    feature_inputs_by_date: dict[str, dict[str, object]] = {}
    for index, value in enumerate(feature_inputs):
        feature_input = _required_object(value, label=f"feature_inputs[{index}]")
        source_date = feature_input.get("source_date")
        relative_path = feature_input.get("path")
        digest = feature_input.get("sha256")
        if (
            not isinstance(source_date, str)
            or source_date in feature_inputs_by_date
            or not isinstance(relative_path, str)
        ):
            raise PatternRegistryDriftError("Discovery feature input identity drift")
        feature_inputs_by_date[source_date] = {
            "relative_path": relative_path,
            "sha256": _drift_sha256(digest, label="Discovery feature input"),
        }
    if parent_parameters.get("feature_inputs_by_date") != feature_inputs_by_date:
        raise PatternRegistryDriftError("AI_SLICE feature input identities differ from artifact")

    feature_manifest_sha256 = _drift_sha256(
        parent_parameters.get("feature_manifest_sha256"),
        label="AI_SLICE feature manifest",
    )
    expected_feature_identity = {
        "calendar_sha256": calendar_sha256,
        "code_snapshot_sha256": analysis_code_snapshot_sha256,
        "discovery_artifact_sha256": artifact_sha256,
        "discovery_config_sha256": discovery_config_sha256,
        "feature_config_sha256": feature_sha256,
        "feature_inputs": feature_inputs,
        "feature_manifest_sha256": feature_manifest_sha256,
        "feature_version": FEATURE_VERSION,
        "footer_manifest_sha256": source_hashes["mbp10_footer_manifest_v1"],
        "formula_sha256": FORMULA_SHA256,
        "qc_manifest_sha256": source_hashes["mbp10_structural_qc_v1"],
        "source_manifest_sha256": source_hashes["mbp10_source_sha256_v1"],
    }
    observed_feature_identity = _plain(observation.feature_identity)
    rollup_registrar = observed_feature_identity.pop("rollup_registrar", None)
    if observed_feature_identity != expected_feature_identity:
        raise PatternRegistryDriftError("pattern feature_identity differs from governed evidence")
    if rollup_registrar is not None:
        registrar = _required_object(rollup_registrar, label="pattern rollup_registrar")
        _exact_keys(
            registrar,
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
            label="pattern rollup_registrar",
        )
        expected_registrar_source = {
            "artifact_schema": _RECOVERY_REGISTRAR_SCHEMA,
            "mode": "IMMUTABLE_AI_ARTIFACT_PATTERN_RECOVERY",
            "no_research_recomputation": True,
            "source_ai_canonical_sha256": canonical_sha256(parent_spec),
            "source_ai_code_snapshot_sha256": parent_spec.get("code_snapshot_sha256"),
            "source_ai_run_fingerprint": canonical_sha256(parent_spec),
            "source_artifact_sha256": artifact_sha256,
        }
        if any(registrar.get(key) != value for key, value in expected_registrar_source.items()):
            raise PatternRegistryDriftError("pattern recovery registrar source provenance drift")
        for key in (
            "recovery_code_snapshot_sha256",
            "recovery_control_run_fingerprint",
            "recovery_manifest_sha256",
            "recovery_runtime_sha256",
        ):
            _drift_sha256(registrar.get(key), label=f"pattern registrar {key}")
        if recovery_projection is not None and any(
            registrar.get(key) != recovery_projection.get(key)
            for key in (
                "recovery_code_snapshot_sha256",
                "recovery_code_commit",
                "recovery_control_run_fingerprint",
                "recovery_manifest_artifact_id",
                "recovery_manifest_relative_path",
                "recovery_manifest_sha256",
                "recovery_runtime_sha256",
            )
        ):
            raise PatternRegistryDriftError(
                "QUERY and pattern recovery registrar identities differ"
            )

    support_count = query_result.get("support_count")
    occurrences = _required_array(query_result.get("occurrences"), label="query occurrences")
    if (
        isinstance(support_count, bool)
        or not isinstance(support_count, int)
        or support_count < 0
        or support_count != len(occurrences)
        or observation.support_count != support_count
    ):
        raise PatternRegistryDriftError("pattern support_count differs from query evidence")
    expected_counterexamples = _expected_counterexamples(query_result)
    if [_plain(value) for value in observation.counterexamples] != expected_counterexamples:
        raise PatternRegistryDriftError("pattern counterexamples differ from query evidence")
    expected_forward_summary = {
        "direction_counts": query_result.get("direction_counts"),
        "forward_close_and_excursion_proxy": query_result.get("forward"),
        "first_touch_status": "NOT_COMPUTED",
        "reason": "DISCOVERY_SLICE_HAS_NO_EVENT_LEVEL_FIRST_TOUCH_OUTCOME",
        "source_date_count": query_result.get("source_date_count"),
    }
    if _plain(observation.forward_first_touch_summary) != expected_forward_summary:
        raise PatternRegistryDriftError(
            "pattern forward_first_touch_summary differs from query evidence"
        )

    definition = _required_object(query_result.get("definition"), label="query definition")
    conditions = _required_array(definition.get("conditions"), label="query conditions")
    direction_rule = definition.get("direction_rule")
    if not isinstance(direction_rule, str) or not all(
        isinstance(condition, str) for condition in conditions
    ):
        raise PatternRegistryDriftError("query entry definition is invalid")
    expected_entry = f"direction_rule={direction_rule}; " + "; ".join(conditions)
    if observation.entry_condition != expected_entry:
        raise PatternRegistryDriftError("pattern entry condition differs from query definition")
    if observation.direction != run_spec.get("direction"):
        raise PatternRegistryDriftError("pattern direction differs from QUERY RunSpec")
    if observation.economic_rationale != _economic_rationale(definition, frozen_inputs):
        raise PatternRegistryDriftError("pattern economic rationale differs from frozen evidence")

    signal_policy = _required_object(run_spec.get("signal_policy"), label="QUERY signal_policy")
    expected_regime = {
        "authority": "OPEN_OBSERVATION",
        "definition_status_available": False,
        "parent_hypothesis_ids": definition.get("parent_hypothesis_ids"),
        "research_eligible": False,
        "screening_only": True,
        "signal_cadence_seconds": signal_policy.get("signal_cadence_seconds"),
    }
    if _plain(observation.applicable_regime) != expected_regime:
        raise PatternRegistryDriftError("pattern applicable regime differs from governed evidence")

    barrier_input = _required_object(
        frozen_inputs.get("barrier_grid"),
        label="frozen barrier grid",
    )
    outcome = _required_object(run_spec.get("outcome"), label="QUERY outcome")
    if barrier_input.get("sha256") != outcome.get("sha256"):
        raise PatternRegistryDriftError("QUERY outcome and frozen barrier grid differ")
    barrier_document = _required_object(
        barrier_input.get("document"),
        label="frozen barrier grid document",
    )
    barrier_grid = _required_object(
        barrier_document.get("barrier_grid"),
        label="frozen barrier_grid table",
    )
    expected_barrier_region = {
        "cell_count": barrier_grid.get("expected_cell_count"),
        "stop_loss_pips": barrier_grid.get("stop_loss_pips"),
        "stop_loss_ticks": barrier_grid.get("stop_loss_ticks"),
        "status": "NOT_EVALUATED_IN_DISCOVERY_SLICE",
        "take_profit_pips": barrier_grid.get("take_profit_pips"),
        "take_profit_ticks": barrier_grid.get("take_profit_ticks"),
    }
    if _plain(observation.candidate_barrier_region) != expected_barrier_region:
        raise PatternRegistryDriftError("pattern barrier region differs from frozen evidence")

    cost = _required_object(run_spec.get("cost"), label="QUERY cost")
    cost_input = _required_object(frozen_inputs.get("cost"), label="frozen cost")
    if cost_input.get("sha256") != cost.get("sha256"):
        raise PatternRegistryDriftError("QUERY cost and frozen cost input differ")
    cost_document = _required_object(
        cost_input.get("document"),
        label="frozen cost document",
    )
    variable_cost = _required_object(
        cost_document.get("variable_cost"),
        label="frozen variable cost",
    )
    fixed_cost = _required_object(
        cost_document.get("fully_loaded_fixed_allocation"),
        label="frozen fixed cost",
    )
    economic_floor = _required_object(
        cost_document.get("economic_floor"),
        label="frozen economic floor",
    )
    expected_cost = {
        "allocated_fixed_cost_ticks": fixed_cost.get("allocated_fixed_cost_ticks_per_round_trip"),
        "baseline_cost_floor_ticks": economic_floor.get("baseline_minimum_take_profit_ticks"),
        "cost_config_sha256": cost.get("sha256"),
        "cost_model_version": cost.get("version"),
        "status": "RECORDED_NOT_APPLIED_TO_FORWARD_PROXY",
        "variable_cost_ticks": variable_cost.get("round_trip_debit_ticks"),
    }
    if _plain(observation.cost_assumptions) != expected_cost:
        raise PatternRegistryDriftError("pattern cost assumptions differ from frozen evidence")


def _open_validated_discovery_evidence(
    run_row: Mapping[str, Any],
    exposure_row: Mapping[str, Any],
    observation: PatternSliceObservation,
    exposure: _ExposureIdentity,
) -> _OpenedDiscoveryEvidence:
    run_spec = _required_object(run_row.get("canonical_spec"), label="QUERY canonical_spec")
    parameters = _required_object(run_spec.get("parameters"), label="QUERY parameters")
    parent_run_spec_id = run_row.get("parent_run_spec_id")
    if (
        isinstance(parent_run_spec_id, bool)
        or not isinstance(parent_run_spec_id, int)
        or parent_run_spec_id <= 0
        or run_row.get("parent_research_run_spec_id") != parent_run_spec_id
        or run_row.get("parent_run_kind") != "AI_SLICE"
    ):
        raise PatternRegistryDriftError("QUERY RunSpec lacks its exact AI_SLICE parent")
    parent_fingerprint = _drift_sha256(
        run_row.get("parent_run_fingerprint"),
        label="AI_SLICE parent fingerprint",
    )
    parent_spec = _required_object(
        run_row.get("parent_canonical_spec"),
        label="AI_SLICE parent canonical_spec",
    )
    if (
        canonical_sha256(parent_spec) != parent_fingerprint
        or parameters.get("parent_run_fingerprint") != parent_fingerprint
    ):
        raise PatternRegistryDriftError("AI_SLICE parent RunSpec content drift")

    artifact_id = exposure_row.get("artifact_id")
    if artifact_id != exposure.result_artifact_id:
        raise PatternRegistryDriftError("QUERY exposure and artifact IDs differ")
    if (
        exposure_row.get("artifact_type") != "DISCOVERY_EXPOSURE_RESULT"
        or exposure_row.get("artifact_media_type") != "application/json"
    ):
        raise PatternRegistryDriftError("QUERY result artifact type or media type drift")

    descriptor, path, derived_root, identity, raw, artifact_sha256 = _open_stable_artifact(
        exposure_row
    )
    try:
        data_root = derived_root.parent
        relative_path = path.relative_to(data_root).as_posix()
        try:
            document_value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PatternRegistryDriftError(
                "Discovery result artifact is not valid UTF-8 JSON"
            ) from exc
        document = _required_object(document_value, label="Discovery result artifact")
        try:
            canonical_payload = canonical_json_bytes(document) + b"\n"
        except (TypeError, ValueError) as exc:
            raise PatternRegistryDriftError(
                "Discovery result artifact is not strict canonical JSON"
            ) from exc
        if raw != canonical_payload:
            raise PatternRegistryDriftError("Discovery result artifact bytes are not canonical")
        if set(document) != {
            "artifact_schema",
            "artifact_version",
            "authority",
            "code_snapshot_sha256",
            "config",
            "coverage",
            "feature_distributions",
            "feature_inputs",
            "no_entry_reasons",
            "query_results",
            "requested_source_dates",
            "run_fingerprint",
            "summary",
        }:
            raise PatternRegistryDriftError("Discovery result artifact field schema drift")
        if (
            document.get("artifact_schema") != DISCOVERY_SLICE_SCHEMA
            or document.get("artifact_version") != DISCOVERY_SLICE_VERSION
            or document.get("run_fingerprint") != parent_fingerprint
        ):
            raise PatternRegistryDriftError("Discovery result artifact lineage drift")
        if document.get("authority") != {
            "maximum_authority": "OPEN_OBSERVATION",
            "pass_backtest_allowed": False,
            "screening_survivor_allowed": False,
            "screening_only": True,
        }:
            raise PatternRegistryDriftError("Discovery result artifact authority drift")
        _required_array(document.get("coverage"), label="Discovery coverage")
        _required_object(
            document.get("feature_distributions"),
            label="Discovery feature_distributions",
        )

        requested_dates = _required_array(
            document.get("requested_source_dates"),
            label="Discovery requested_source_dates",
        )
        try:
            parsed_dates = tuple(date.fromisoformat(value) for value in requested_dates)
        except (TypeError, ValueError) as exc:
            raise PatternRegistryDriftError("Discovery requested source dates are invalid") from exc
        if (
            len(parsed_dates) != 5
            or tuple(sorted(set(parsed_dates))) != parsed_dates
            or parameters.get("requested_source_dates") != requested_dates
        ):
            raise PatternRegistryDriftError("Discovery source-date slice identity drift")
        parent_parameters = _required_object(
            parent_spec.get("parameters"),
            label="AI_SLICE parent parameters",
        )
        if parent_parameters.get("requested_source_dates") != requested_dates:
            raise PatternRegistryDriftError("QUERY and AI_SLICE source-date slices differ")
        expected_start = datetime.combine(parsed_dates[0], time.min, tzinfo=UTC)
        expected_end = datetime.combine(parsed_dates[-1] + timedelta(days=1), time.min, tzinfo=UTC)
        if (
            exposure.source_interval_start != expected_start
            or exposure.source_interval_end != expected_end
        ):
            raise PatternRegistryDriftError("QUERY exposure interval differs from artifact dates")

        query_results = _required_array(
            document.get("query_results"),
            label="Discovery query_results",
        )
        normalized_results: list[dict[str, Any]] = []
        query_ids: set[str] = set()
        for index, value in enumerate(query_results):
            result = _required_object(value, label=f"query_results[{index}]")
            definition = _required_object(
                result.get("definition"),
                label=f"query_results[{index}].definition",
            )
            query_id = definition.get("id")
            if not isinstance(query_id, str) or not query_id or query_id in query_ids:
                raise PatternRegistryDriftError("Discovery query result identity drift")
            query_ids.add(query_id)
            normalized_results.append(result)
        candidate_queries = parent_parameters.get("candidate_queries")
        if candidate_queries != [result["definition"] for result in normalized_results]:
            raise PatternRegistryDriftError(
                "Discovery query definitions differ from AI_SLICE RunSpec"
            )
        config = _required_object(document.get("config"), label="Discovery config")
        if parent_parameters.get("candidate_query_definition_sha256") != config.get(
            "definition_sha256"
        ):
            raise PatternRegistryDriftError("Discovery query definition bundle SHA-256 drift")
        matches = [
            result
            for result in normalized_results
            if result["definition"].get("id") == observation.query_id
        ]
        if len(matches) != 1:
            raise PatternRegistryDriftError("Discovery artifact lacks one exact query result")
        query_result = matches[0]
        if set(query_result) != {
            "definition",
            "direction_counts",
            "forward",
            "occurrences",
            "source_date_count",
            "support_count",
        }:
            raise PatternRegistryDriftError("Discovery query result field schema drift")
        if query_result.get("definition") != _plain(observation.query_definition):
            raise PatternRegistryDriftError("Discovery query definition differs from observation")
        query_result_sha256 = _drift_sha256(
            parameters.get("query_result_sha256"),
            label="QUERY query_result_sha256",
        )
        if canonical_sha256(query_result) != query_result_sha256:
            raise PatternRegistryDriftError(
                "QUERY query_result_sha256 differs from Discovery artifact bytes"
            )
        if (
            parameters.get("discovery_artifact_sha256") != artifact_sha256
            or parameters.get("discovery_artifact_relative_path") != relative_path
        ):
            raise PatternRegistryDriftError("QUERY RunSpec result artifact identity drift")

        slice_index = parameters.get("slice_index")
        if isinstance(slice_index, bool) or not isinstance(slice_index, int) or slice_index < 0:
            raise PatternRegistryDriftError("QUERY slice_index is invalid")
        ai_exposure_key = f"{observation.campaign_key}:ai-slice:{slice_index:02d}"
        expected_query_exposure_key = (
            f"{observation.campaign_key}:query:{slice_index:02d}:{observation.query_id}"
        )
        if observation.exposure_key != expected_query_exposure_key:
            raise PatternRegistryDriftError(
                "QUERY exposure key is not the canonical slice/query key"
            )
        expected_metadata = {
            "campaign_key": observation.campaign_key,
            "exposure_key": ai_exposure_key,
            "exposure_type": "AI_SLICE",
            "run_fingerprint": parent_fingerprint,
        }
        if exposure_row.get("artifact_metadata") != expected_metadata or exposure_row.get(
            "artifact_key"
        ) != (f"{observation.campaign_key}:discovery-exposure:{ai_exposure_key}:{artifact_sha256}"):
            raise PatternRegistryDriftError("Discovery result artifact database lineage drift")

        if exposure_row.get("config_sha256") != config.get("sha256"):
            raise PatternRegistryDriftError("QUERY exposure config differs from artifact")
        expected_result_summary = {
            "artifact_sha256": artifact_sha256,
            "direction_counts": query_result.get("direction_counts"),
            "source_date_count": query_result.get("source_date_count"),
            "support_count": query_result.get("support_count"),
        }
        if exposure_row.get("result_summary") != expected_result_summary:
            raise PatternRegistryDriftError("QUERY exposure result summary differs from artifact")

        _validate_observation_evidence(
            run_spec=run_spec,
            parent_spec=parent_spec,
            artifact_document=document,
            artifact_sha256=artifact_sha256,
            query_result=query_result,
            observation=observation,
        )
        evidence = _OpenedDiscoveryEvidence(
            descriptor=descriptor,
            path=path,
            identity=identity,
            query_result=MappingProxyType(query_result),
        )
        _verify_open_artifact_binding(evidence)
        return evidence
    except Exception:
        os.close(descriptor)
        raise


def _recovery_identity(
    run_spec: Mapping[str, Any],
    observation: PatternSliceObservation,
) -> tuple[dict[str, Any] | None, str | None]:
    parameters = _required_object(run_spec.get("parameters"), label="QUERY parameters")
    projection_value = parameters.get("recovery_projection")
    projection = (
        _required_object(projection_value, label="QUERY recovery_projection")
        if projection_value is not None
        else None
    )
    feature_identity = _plain(observation.feature_identity)
    registrar_value = feature_identity.get("rollup_registrar")
    registrar = (
        _required_object(registrar_value, label="pattern rollup_registrar")
        if registrar_value is not None
        else None
    )
    if projection is None and registrar is None:
        return None, None
    if projection is not None and registrar is not None:
        shared_projection = {
            key: value
            for key, value in projection.items()
            if key not in {"artifact_schema", "mode"}
        }
        shared_registrar = {
            key: value for key, value in registrar.items() if key not in {"artifact_schema", "mode"}
        }
        if shared_projection != shared_registrar:
            raise PatternRegistryDriftError(
                "QUERY projection and pattern registrar recovery identities differ"
            )
    if projection is not None:
        return projection, "PROJECTION"
    return registrar, "REGISTRAR"


def _verify_open_recovery_binding(evidence: _OpenedRecoveryEvidence) -> None:
    try:
        descriptor_identity = _file_identity(os.fstat(evidence.descriptor))
        path_identity = _file_identity(evidence.path.lstat())
    except OSError as exc:
        raise PatternRegistryDriftError(
            "recovery manifest disappeared before pattern commit"
        ) from exc
    if (
        descriptor_identity != evidence.identity
        or path_identity != evidence.identity
        or not stat.S_ISREG(path_identity[2])
    ):
        raise PatternRegistryDriftError("recovery manifest changed before pattern commit")


def _open_validated_recovery_evidence(
    connection: psycopg.Connection[dict[str, Any]],
    run_row: Mapping[str, Any],
    exposure_row: Mapping[str, Any],
    observation: PatternSliceObservation,
    query_result: Mapping[str, object],
) -> _OpenedRecoveryEvidence | None:
    run_spec = _required_object(run_row.get("canonical_spec"), label="QUERY canonical_spec")
    recovery, recovery_use = _recovery_identity(run_spec, observation)
    if recovery is None or recovery_use is None:
        return None
    control_fingerprint = _drift_sha256(
        recovery.get("recovery_control_run_fingerprint"),
        label="recovery control fingerprint",
    )
    rows = connection.execute(
        """
        SELECT control.research_run_spec_id, control.campaign_id,
               control.parent_run_spec_id, control.run_fingerprint,
               control.run_kind, control.engine_version,
               control.canonical_spec,
               parent.run_fingerprint AS parent_run_fingerprint,
               parent.canonical_spec AS parent_canonical_spec,
               control_attempt.research_run_attempt_id,
               control_attempt.result_artifact_id,
               recovery_artifact.artifact_id,
               recovery_artifact.artifact_key,
               recovery_artifact.artifact_type,
               recovery_artifact.uri AS artifact_uri,
               recovery_artifact.sha256 AS artifact_sha256,
               recovery_artifact.byte_size AS artifact_byte_size,
               recovery_artifact.media_type AS artifact_media_type,
               recovery_artifact.metadata AS artifact_metadata,
               source_attempt.result_artifact_id AS source_artifact_id,
               source_artifact.sha256 AS source_artifact_sha256,
               source_artifact.byte_size AS source_artifact_byte_size,
               source_artifact.uri AS source_artifact_uri
        FROM systematic_fx.research_run_specs AS control
        JOIN systematic_fx.research_run_specs AS parent
          ON parent.research_run_spec_id = control.parent_run_spec_id
         AND parent.campaign_id = control.campaign_id
        JOIN systematic_fx.research_run_attempts AS control_attempt
          ON control_attempt.research_run_spec_id = control.research_run_spec_id
         AND control_attempt.status = 'SUCCEEDED'
        JOIN systematic_fx.artifacts AS recovery_artifact
          ON recovery_artifact.artifact_id = control_attempt.result_artifact_id
        JOIN systematic_fx.research_run_attempts AS source_attempt
          ON source_attempt.research_run_spec_id = parent.research_run_spec_id
         AND source_attempt.status = 'SUCCEEDED'
        JOIN systematic_fx.artifacts AS source_artifact
          ON source_artifact.artifact_id = source_attempt.result_artifact_id
        WHERE control.run_fingerprint = %s
        FOR SHARE OF control, parent, control_attempt, recovery_artifact,
                     source_attempt, source_artifact
        """,
        (control_fingerprint,),
    ).fetchall()
    if len(rows) != 1:
        raise PatternRegistryDriftError(
            "recovery control does not have one exact successful manifest/source chain"
        )
    row = rows[0]
    parent_run_spec_id = run_row.get("parent_research_run_spec_id")
    if (
        row.get("campaign_id") != run_row.get("campaign_id")
        or row.get("parent_run_spec_id") != parent_run_spec_id
        or row.get("run_kind") != "VALIDATION"
        or row.get("engine_version") != _RECOVERY_CONTROL_ENGINE
        or row.get("run_fingerprint") != control_fingerprint
        or row.get("parent_run_fingerprint") != run_row.get("parent_run_fingerprint")
        or row.get("parent_canonical_spec") != run_row.get("parent_canonical_spec")
    ):
        raise PatternRegistryDriftError("recovery control RunSpec parentage drift")
    control_spec = _required_object(
        row.get("canonical_spec"),
        label="recovery control canonical_spec",
    )
    if canonical_sha256(control_spec) != control_fingerprint:
        raise PatternRegistryDriftError("recovery control canonical fingerprint drift")
    control_parameters = _required_object(
        control_spec.get("parameters"),
        label="recovery control parameters",
    )
    _exact_keys(
        control_parameters,
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
        label="recovery control parameters",
    )
    parent_spec = _required_object(
        row.get("parent_canonical_spec"),
        label="recovery source AI canonical_spec",
    )
    runtime = _required_object(
        control_spec.get("runtime_environment"),
        label="recovery control runtime",
    )
    source_artifact_id = row.get("source_artifact_id")
    expected_control_parameters = {
        "artifact_schema": _RECOVERY_CONTROL_SCHEMA,
        "discovery_artifact_sha256": row.get("source_artifact_sha256"),
        "no_research_recomputation": True,
        "parent_run_fingerprint": row.get("parent_run_fingerprint"),
        "pipeline_version": control_parameters.get("pipeline_version"),
        "recovery_manifest_relative_path": recovery.get("recovery_manifest_relative_path"),
        "recovery_manifest_sha256": recovery.get("recovery_manifest_sha256"),
        "requested_source_dates": _required_object(
            parent_spec.get("parameters"),
            label="recovery source AI parameters",
        ).get("requested_source_dates"),
        "slice_index": _required_object(
            parent_spec.get("parameters"),
            label="recovery source AI parameters",
        ).get("slice_index"),
        "source_ai_canonical_sha256": canonical_sha256(parent_spec),
        "source_ai_code_snapshot_sha256": parent_spec.get("code_snapshot_sha256"),
        "source_artifact_id": source_artifact_id,
        "source_artifact_relative_path": control_parameters.get("source_artifact_relative_path"),
    }
    if control_parameters != expected_control_parameters:
        raise PatternRegistryDriftError("recovery control parameter lineage drift")
    recovery_artifact_id = row.get("artifact_id")
    if (
        recovery_artifact_id != recovery.get("recovery_manifest_artifact_id")
        or row.get("result_artifact_id") != recovery_artifact_id
        or row.get("artifact_type") != "PHASE1A_SLICE_RECOVERY_MANIFEST"
        or row.get("artifact_media_type") != "application/json"
        or row.get("artifact_sha256") != recovery.get("recovery_manifest_sha256")
        or source_artifact_id != exposure_row.get("result_artifact_id")
        or row.get("source_artifact_sha256") != exposure_row.get("artifact_sha256")
        or recovery.get("source_artifact_id") != source_artifact_id
        or recovery.get("source_artifact_sha256") != row.get("source_artifact_sha256")
        or recovery.get("source_ai_run_fingerprint") != row.get("parent_run_fingerprint")
        or recovery.get("source_ai_canonical_sha256") != canonical_sha256(parent_spec)
        or recovery.get("source_ai_code_snapshot_sha256") != parent_spec.get("code_snapshot_sha256")
        or recovery.get("recovery_code_commit") != control_spec.get("code_commit")
        or recovery.get("recovery_code_snapshot_sha256") != control_spec.get("code_snapshot_sha256")
        or recovery.get("recovery_runtime_sha256") != canonical_sha256(runtime)
    ):
        raise PatternRegistryDriftError("recovery control/artifact identity drift")
    expected_metadata = {
        "artifact_schema": _RECOVERY_MANIFEST_SCHEMA,
        "campaign_key": observation.campaign_key,
        "no_research_recomputation": True,
        "run_fingerprint": control_fingerprint,
        "source_ai_run_fingerprint": row.get("parent_run_fingerprint"),
        "source_artifact_id": source_artifact_id,
    }
    if row.get("artifact_metadata") != expected_metadata or row.get("artifact_key") != (
        f"{observation.campaign_key}:partial-recovery:"
        f"{control_fingerprint}:{row.get('artifact_sha256')}"
    ):
        raise PatternRegistryDriftError("recovery manifest database metadata drift")

    descriptor, path, derived_root, identity, raw, manifest_sha256 = _open_stable_artifact(row)
    try:
        if identity[2] & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH):
            raise PatternRegistryDriftError("recovery manifest is writable")
        if path.relative_to(derived_root).as_posix() != recovery.get(
            "recovery_manifest_relative_path"
        ):
            raise PatternRegistryDriftError("recovery manifest relative path drift")
        try:
            document_value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PatternRegistryDriftError("recovery manifest is not valid JSON") from exc
        document = _required_object(document_value, label="recovery manifest")
        if canonical_json_bytes(document) + b"\n" != raw:
            raise PatternRegistryDriftError("recovery manifest bytes are not canonical")
        _exact_keys(
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
            label="recovery manifest",
        )
        if (
            document.get("artifact_schema") != _RECOVERY_MANIFEST_SCHEMA
            or document.get("campaign_key") != observation.campaign_key
            or document.get("no_research_recomputation") is not True
            or document.get("requested_source_dates")
            != control_parameters.get("requested_source_dates")
            or document.get("slice_index") != control_parameters.get("slice_index")
            or manifest_sha256 != control_parameters.get("recovery_manifest_sha256")
        ):
            raise PatternRegistryDriftError("recovery manifest control identity drift")
        execution = _required_object(
            document.get("recovery_execution"),
            label="recovery manifest execution",
        )
        if execution != {
            "code_commit": control_spec.get("code_commit"),
            "code_snapshot_sha256": control_spec.get("code_snapshot_sha256"),
            "dependency_lock_sha256": control_spec.get("dependency_lock_sha256"),
            "runtime_environment": runtime,
            "runtime_environment_sha256": canonical_sha256(runtime),
        }:
            raise PatternRegistryDriftError("recovery manifest execution drift")
        source_prefix = _required_object(
            document.get("source_prefix"),
            label="recovery manifest source prefix",
        )
        source_ai = _required_object(source_prefix.get("ai"), label="manifest source AI")
        source_artifact = _required_object(
            source_prefix.get("discovery_artifact"),
            label="manifest source artifact",
        )
        source_uri = row.get("source_artifact_uri")
        if not isinstance(source_uri, str):
            raise PatternRegistryDriftError("source artifact URI is invalid")
        parsed_source = urlparse(source_uri)
        source_path = Path(unquote(parsed_source.path)).resolve(strict=True)
        if (
            source_ai.get("research_run_spec_id") != parent_run_spec_id
            or source_ai.get("run_fingerprint") != row.get("parent_run_fingerprint")
            or source_ai.get("canonical_sha256") != canonical_sha256(parent_spec)
            or source_artifact
            != {
                "artifact_id": source_artifact_id,
                "byte_size": row.get("source_artifact_byte_size"),
                "relative_path": source_path.relative_to(derived_root).as_posix(),
                "sha256": row.get("source_artifact_sha256"),
            }
        ):
            raise PatternRegistryDriftError("recovery manifest source identity drift")
        query_evidence = document.get("query_evidence")
        if not isinstance(query_evidence, list):
            raise PatternRegistryDriftError("recovery manifest query evidence is invalid")
        matches = [
            item
            for item in query_evidence
            if isinstance(item, dict) and item.get("query_id") == observation.query_id
        ]
        if len(matches) != 1 or matches[0].get("query_result_sha256") != canonical_sha256(
            query_result
        ):
            raise PatternRegistryDriftError("recovery manifest query evidence drift")
        planned = _required_object(
            document.get("planned_actions"),
            label="recovery manifest planned actions",
        )
        expected_list = (
            planned.get("project_missing_query_ids")
            if recovery_use == "PROJECTION"
            else planned.get("repair_existing_query_pattern_ids")
        )
        if not isinstance(expected_list, list) or observation.query_id not in expected_list:
            raise PatternRegistryDriftError("recovery manifest does not authorize this action")
        evidence = _OpenedRecoveryEvidence(
            descriptor=descriptor,
            path=path,
            identity=identity,
        )
        _verify_open_recovery_binding(evidence)
        return evidence
    except Exception:
        os.close(descriptor)
        raise


def _slice_record(
    observation: PatternSliceObservation,
    exposure: _ExposureIdentity,
) -> dict[str, object]:
    return {
        "counterexamples": [_plain(value) for value in observation.counterexamples],
        "discovery_exposure_id": exposure.discovery_exposure_id,
        "exposure_key": observation.exposure_key,
        "forward_first_touch_summary": _plain(observation.forward_first_touch_summary),
        "query_definition_sha256": observation.query_definition_sha256,
        "research_run_spec_id": exposure.research_run_spec_id,
        "result_artifact_id": exposure.result_artifact_id,
        "run_fingerprint": observation.run_fingerprint,
        "source_interval_end": exposure.source_interval_end.isoformat(),
        "source_interval_start": exposure.source_interval_start.isoformat(),
        "support_count": observation.support_count,
    }


def _feature_record(
    observation: PatternSliceObservation,
    exposure: _ExposureIdentity,
) -> dict[str, object]:
    return {
        "discovery_exposure_id": exposure.discovery_exposure_id,
        "feature_identity": _plain(observation.feature_identity),
        "query_definition": _plain(observation.query_definition),
        "query_definition_sha256": observation.query_definition_sha256,
        "run_fingerprint": observation.run_fingerprint,
    }


def _initial_documents(
    observation: PatternSliceObservation,
    exposure: _ExposureIdentity,
) -> tuple[dict[str, object], list[dict[str, object]], dict[str, object]]:
    feature_versions = {
        "rollup_schema": _ROLLUP_SCHEMA,
        "slice_identities": [_feature_record(observation, exposure)],
    }
    counterexamples = [
        {
            **_plain(value),
            "discovery_exposure_id": exposure.discovery_exposure_id,
            "run_fingerprint": observation.run_fingerprint,
        }
        for value in observation.counterexamples
    ]
    summaries = {
        "rollup_schema": _ROLLUP_SCHEMA,
        "slice_observations": [_slice_record(observation, exposure)],
    }
    return feature_versions, counterexamples, summaries


def _append_documents(
    row: Mapping[str, Any],
    observation: PatternSliceObservation,
    exposure: _ExposureIdentity,
) -> tuple[dict[str, object], list[dict[str, object]], dict[str, object], bool]:
    features = _required_object(
        row.get("feature_definition_versions"),
        label="pattern feature_definition_versions",
    )
    summaries = _required_object(
        row.get("forward_first_touch_summaries"),
        label="pattern forward_first_touch_summaries",
    )
    raw_counterexamples = row.get("counterexamples")
    if not isinstance(raw_counterexamples, list):
        raise PatternRegistryDriftError("pattern counterexamples must be a JSON array")
    if features.get("rollup_schema") != _ROLLUP_SCHEMA:
        raise PatternRegistryDriftError("pattern feature roll-up schema drift")
    if summaries.get("rollup_schema") != _ROLLUP_SCHEMA:
        raise PatternRegistryDriftError("pattern summary roll-up schema drift")
    feature_items = features.get("slice_identities")
    summary_items = summaries.get("slice_observations")
    if not isinstance(feature_items, list) or not isinstance(summary_items, list):
        raise PatternRegistryDriftError("pattern slice roll-up must use ordered arrays")

    candidate_summary = _slice_record(observation, exposure)
    candidate_feature = _feature_record(observation, exposure)
    matches = [
        item
        for item in summary_items
        if isinstance(item, dict)
        and item.get("discovery_exposure_id") == exposure.discovery_exposure_id
    ]
    if matches:
        if len(matches) != 1 or matches[0] != candidate_summary:
            raise PatternRegistryDriftError("reused pattern exposure has different slice content")
        feature_matches = [
            item
            for item in feature_items
            if isinstance(item, dict)
            and item.get("discovery_exposure_id") == exposure.discovery_exposure_id
        ]
        if feature_matches != [candidate_feature]:
            raise PatternRegistryDriftError(
                "reused pattern exposure has different feature identity"
            )
        return dict(features), list(raw_counterexamples), dict(summaries), False

    if summary_items:
        final = summary_items[-1]
        if not isinstance(final, dict) or not isinstance(final.get("source_interval_end"), str):
            raise PatternRegistryDriftError("last pattern slice interval is invalid")
        try:
            prior_end = datetime.fromisoformat(final["source_interval_end"]).astimezone(UTC)
        except (ValueError, TypeError) as exc:
            raise PatternRegistryDriftError("last pattern slice interval is invalid") from exc
        if exposure.source_interval_start < prior_end:
            raise PatternRegistryDriftError("new pattern slice overlaps or predates its roll-up")

    appended_features = dict(features)
    appended_features["slice_identities"] = [*feature_items, candidate_feature]
    appended_summaries = dict(summaries)
    appended_summaries["slice_observations"] = [*summary_items, candidate_summary]
    appended_counterexamples = [
        *raw_counterexamples,
        *(
            {
                **_plain(value),
                "discovery_exposure_id": exposure.discovery_exposure_id,
                "run_fingerprint": observation.run_fingerprint,
            }
            for value in observation.counterexamples
        ),
    ]
    return appended_features, appended_counterexamples, appended_summaries, True


def _assert_pattern_identity(
    row: Mapping[str, Any],
    observation: PatternSliceObservation,
) -> None:
    expected = {
        "pattern_key": observation.pattern_key,
        "parent_pattern_id": None,
        "direction": observation.direction,
        "entry_condition": observation.entry_condition,
        "economic_rationale": observation.economic_rationale,
        "applicable_regime": _plain(observation.applicable_regime),
        "candidate_barrier_region": _plain(observation.candidate_barrier_region),
        "cost_assumptions": _plain(observation.cost_assumptions),
    }
    drift = [key for key, value in expected.items() if row.get(key) != value]
    if drift:
        raise PatternRegistryDriftError(
            f"pattern immutable identity drift: {', '.join(sorted(drift))}"
        )


def _row_or_error(row: dict[str, Any] | None, *, label: str) -> dict[str, Any]:
    if row is None:
        raise PatternRegistryError(f"{label} does not exist")
    return row


def _register_pattern(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    campaign_id: int,
    observation: PatternSliceObservation,
    exposure: _ExposureIdentity,
) -> tuple[dict[str, Any], bool, bool]:
    feature_versions, counterexamples, summaries = _initial_documents(observation, exposure)
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.pattern_ledger
            (pattern_key, campaign_id, parent_pattern_id, status,
             first_seen_from, first_seen_to, last_updated_interval,
             feature_definition_versions, direction, entry_condition,
             economic_rationale, applicable_regime, counterexamples,
             support_count, candidate_barrier_region,
             forward_first_touch_summaries, cost_assumptions, context_artifact_id)
        VALUES (%s, %s, NULL, 'OPEN', %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s)
        ON CONFLICT (pattern_key) DO NOTHING
        RETURNING pattern_id
        """,
        (
            observation.pattern_key,
            campaign_id,
            exposure.source_interval_start,
            exposure.source_interval_end,
            exposure.source_interval_end,
            Jsonb(feature_versions),
            observation.direction,
            observation.entry_condition,
            observation.economic_rationale,
            Jsonb(_plain(observation.applicable_regime)),
            Jsonb(counterexamples),
            observation.support_count,
            Jsonb(_plain(observation.candidate_barrier_region)),
            Jsonb(summaries),
            Jsonb(_plain(observation.cost_assumptions)),
            exposure.result_artifact_id,
        ),
    ).fetchone()
    created = inserted is not None
    row = connection.execute(
        """
        SELECT pattern_id, pattern_key, campaign_id, parent_pattern_id, status,
               first_seen_from, first_seen_to, last_updated_interval,
               feature_definition_versions, direction, entry_condition,
               economic_rationale, applicable_regime, counterexamples,
               support_count, candidate_barrier_region,
               forward_first_touch_summaries, cost_assumptions, context_artifact_id
        FROM systematic_fx.pattern_ledger
        WHERE pattern_key = %s
        FOR UPDATE
        """,
        (observation.pattern_key,),
    ).fetchone()
    row = _row_or_error(row, label=f"pattern {observation.pattern_key}")
    if int(row["campaign_id"]) != campaign_id:
        raise PatternRegistryDriftError("pattern key belongs to another campaign")
    _assert_pattern_identity(row, observation)
    if created:
        return row, True, True

    features, next_counterexamples, next_summaries, appended = _append_documents(
        row,
        observation,
        exposure,
    )
    if not appended:
        return row, False, False
    if row.get("last_updated_interval") is not None:
        prior_update = _aware_utc(row["last_updated_interval"], label="last_updated_interval")
        if exposure.source_interval_end < prior_update:
            raise PatternRegistryDriftError("pattern last_updated_interval would move backward")
    expected_support = int(row["support_count"]) + observation.support_count
    updated = connection.execute(
        """
        UPDATE systematic_fx.pattern_ledger
        SET last_updated_interval = %s,
            feature_definition_versions = %s,
            counterexamples = %s,
            support_count = %s,
            forward_first_touch_summaries = %s,
            context_artifact_id = %s,
            updated_at = statement_timestamp()
        WHERE pattern_id = %s
        RETURNING pattern_id, pattern_key, campaign_id, support_count,
                  last_updated_interval, context_artifact_id
        """,
        (
            exposure.source_interval_end,
            Jsonb(features),
            Jsonb(next_counterexamples),
            expected_support,
            Jsonb(next_summaries),
            exposure.result_artifact_id,
            row["pattern_id"],
        ),
    ).fetchone()
    row = _row_or_error(updated, label=f"updated pattern {observation.pattern_key}")
    if int(row["support_count"]) != expected_support:
        raise PatternRegistryDriftError("pattern support roll-up update drifted")
    return row, False, True


@_translate_psycopg_errors("pattern registration")
def record_pattern_slice_observation(
    database_url: str,
    observation: PatternSliceObservation,
) -> PatternObservationReport:
    """Append one governed query exposure to a compact pattern roll-up.

    Repeating the exact exposure is idempotent and never increments support a
    second time.  A different exposure must be chronological and non-overlapping.
    """

    database_url = _nonempty(database_url, label="database_url")
    if not isinstance(observation, PatternSliceObservation):
        raise PatternRegistryError("observation must be a PatternSliceObservation")
    opened_evidence: _OpenedDiscoveryEvidence | None = None
    opened_recovery: _OpenedRecoveryEvidence | None = None
    try:
        with psycopg.connect(database_url, row_factory=dict_row) as connection:
            connection.isolation_level = IsolationLevel.SERIALIZABLE
            with connection.transaction():
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (observation.pattern_key,),
                )
                campaign = connection.execute(
                    """
                    SELECT campaign_id, campaign_key
                    FROM systematic_fx.campaigns
                    WHERE campaign_key = %s
                    FOR SHARE
                    """,
                    (observation.campaign_key,),
                ).fetchone()
                campaign = _row_or_error(
                    campaign,
                    label=f"campaign {observation.campaign_key}",
                )
                campaign_id = int(campaign["campaign_id"])
                run = connection.execute(
                    """
                    SELECT child.research_run_spec_id, child.campaign_id,
                           child.parent_run_spec_id, child.run_fingerprint,
                           child.run_kind, child.canonical_spec,
                           parent.research_run_spec_id AS parent_research_run_spec_id,
                           parent.run_fingerprint AS parent_run_fingerprint,
                           parent.run_kind AS parent_run_kind,
                           parent.canonical_spec AS parent_canonical_spec
                    FROM systematic_fx.research_run_specs AS child
                    JOIN systematic_fx.research_run_specs AS parent
                      ON parent.research_run_spec_id = child.parent_run_spec_id
                     AND parent.campaign_id = child.campaign_id
                    WHERE child.run_fingerprint = %s
                    FOR SHARE OF child, parent
                    """,
                    (observation.run_fingerprint,),
                ).fetchone()
                run = _row_or_error(run, label="pattern QUERY RunSpec")
                if int(run["campaign_id"]) != campaign_id:
                    raise PatternRegistryDriftError("QUERY RunSpec belongs to another campaign")
                exposure = connection.execute(
                    """
                    SELECT exposure.discovery_exposure_id, exposure.campaign_id,
                           exposure.exposure_key, exposure.exposure_type,
                           exposure.source_interval_start, exposure.source_interval_end,
                           exposure.visible_to_ai, exposure.research_eligible,
                           exposure.query_spec, exposure.result_summary,
                           exposure.result_artifact_id, exposure.research_run_spec_id,
                           exposure.config_sha256,
                           artifact.artifact_id, artifact.artifact_key,
                           artifact.artifact_type, artifact.uri AS artifact_uri,
                           artifact.sha256 AS artifact_sha256,
                           artifact.byte_size AS artifact_byte_size,
                           artifact.media_type AS artifact_media_type,
                           artifact.metadata AS artifact_metadata
                    FROM systematic_fx.discovery_exposures AS exposure
                    JOIN systematic_fx.artifacts AS artifact
                      ON artifact.artifact_id = exposure.result_artifact_id
                    WHERE exposure.exposure_key = %s
                    FOR SHARE OF exposure, artifact
                    """,
                    (observation.exposure_key,),
                ).fetchone()
                exposure = _row_or_error(exposure, label="pattern QUERY exposure")
                if int(exposure["campaign_id"]) != campaign_id:
                    raise PatternRegistryDriftError("QUERY exposure belongs to another campaign")
                identity = _validate_governed_query(run, exposure, observation)
                opened_evidence = _open_validated_discovery_evidence(
                    run,
                    exposure,
                    observation,
                    identity,
                )
                opened_recovery = _open_validated_recovery_evidence(
                    connection,
                    run,
                    exposure,
                    observation,
                    opened_evidence.query_result,
                )
                row, created_pattern, appended = _register_pattern(
                    connection,
                    campaign_id=campaign_id,
                    observation=observation,
                    exposure=identity,
                )
                _verify_open_artifact_binding(opened_evidence)
                if opened_recovery is not None:
                    _verify_open_recovery_binding(opened_recovery)
    finally:
        if opened_evidence is not None:
            os.close(opened_evidence.descriptor)
        if opened_recovery is not None:
            os.close(opened_recovery.descriptor)

    return PatternObservationReport(
        pattern_id=int(row["pattern_id"]),
        pattern_key=observation.pattern_key,
        campaign_id=campaign_id,
        discovery_exposure_id=identity.discovery_exposure_id,
        research_run_spec_id=identity.research_run_spec_id,
        result_artifact_id=identity.result_artifact_id,
        created_pattern=created_pattern,
        appended_observation=appended,
    )
