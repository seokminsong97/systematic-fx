"""Bounded, outcome-blind AI pattern proposals on Discovery features only.

This module deliberately stops before candidate evaluation.  It does not import
the database, labels, first-passage outcomes, holdout code, or any execution
worker.  A run can only publish immutable hypothesis proposals with status
``HYPOTHESES_GENERATED_AWAITING_ELIGIBLE_DATA``.

The durable lifecycle is intentionally split into two stages:

1. publish a PRECOMMITTED request in a separate append-only local ledger;
2. build and publish a compact feature-only context, then either run the local
   deterministic proposer or parse/replay one already-recorded model response.

Recorded responses are data, never executable instructions.  The accepted DSL
is an AND of a small finite set of integer predicates.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Final, Literal

from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256

AUTHORITY: Final = "PROPOSE_CANDIDATES_ONLY"
FINAL_STATUS: Final = "HYPOTHESES_GENERATED_AWAITING_ELIGIBLE_DATA"

REQUEST_SCHEMA: Final = "systematic_fx.ai_pattern_proposal_request.v1"
CONTEXT_SCHEMA: Final = "systematic_fx.ai_pattern_discovery_context.v1"
RULE_SCHEMA: Final = "systematic_fx.ai_pattern_and_rule.v1"
PROPOSAL_SCHEMA: Final = "systematic_fx.ai_pattern_proposal.v1"
BATCH_SCHEMA: Final = "systematic_fx.ai_pattern_proposal_batch.v1"
RECORDED_RESPONSE_SCHEMA: Final = "systematic_fx.ai_pattern_recorded_response.v1"
REPORT_SCHEMA: Final = "systematic_fx.ai_pattern_discovery_report.v1"
LEDGER_EVENT_SCHEMA: Final = "systematic_fx.ai_pattern_proposal_ledger_event.v1"

_SHA256 = re.compile(r"[0-9a-f]{64}")
_SAFE_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:@+-]{0,127}")
_LEDGER_NAME = re.compile(r"event-([0-9]{8})\.json")
_WRITE_BITS: Final = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
_PPM: Final = 1_000_000
_UINT64_MAX: Final = 2**64 - 1

ProposerMode = Literal["DETERMINISTIC_OUTCOME_BLIND_V1", "RECORDED_RESPONSE_V1"]
RuleOperator = Literal["GE", "LE"]
Direction = Literal["LONG", "SHORT"]

ALLOWED_FEATURES: Final = (
    "range_ticks",
    "signed_body_ppm",
    "absolute_body_ppm",
    "close_location_ppm",
    "upper_wick_ppm",
    "lower_wick_ppm",
)

ALLOWED_PREDICATES: Final[Mapping[str, Mapping[str, tuple[int, ...]]]] = {
    "range_ticks": {"GE": (1, 2, 4, 8, 12, 16, 24, 32)},
    "signed_body_ppm": {
        "GE": (100_000, 250_000, 500_000, 750_000),
        "LE": (-900_000, -750_000, -500_000, -250_000, -100_000),
    },
    "absolute_body_ppm": {"GE": (100_000, 250_000, 500_000, 750_000, 900_000)},
    "close_location_ppm": {
        "GE": (600_000, 700_000, 800_000, 900_000),
        "LE": (100_000, 200_000, 300_000, 400_000),
    },
    "upper_wick_ppm": {"GE": (100_000, 250_000, 500_000, 750_000)},
    "lower_wick_ppm": {"GE": (100_000, 250_000, 500_000, 750_000)},
}

_FEATURE_BOUNDS: Final[Mapping[str, tuple[int, int]]] = {
    "range_ticks": (0, 1_000_000),
    "signed_body_ppm": (-1_000_000, 1_000_000),
    "absolute_body_ppm": (0, 1_000_000),
    "close_location_ppm": (0, 1_000_000),
    "upper_wick_ppm": (0, 1_000_000),
    "lower_wick_ppm": (0, 1_000_000),
}

_RATIONALE_BY_FAMILY: Final[Mapping[str, str]] = {
    "BODY_CLOSE_CONFIRMATION": "DIRECTIONAL_BODY_CLOSE_CONFIRMATION",
    "RANGE_EXPANSION_CONTINUATION": "RANGE_BODY_CONTINUATION",
    "WICK_REJECTION_REVERSAL": "INTRABAR_WICK_REJECTION",
}
ALLOWED_FAMILIES: Final = tuple(sorted(_RATIONALE_BY_FAMILY))
ALLOWED_RATIONALE_CODES: Final = tuple(sorted(_RATIONALE_BY_FAMILY.values()))

LIMITATIONS: Final = (
    "DISCOVERY_FEATURES_ONLY",
    "NO_LABEL_OR_OUTCOME_ACCESS",
    "NO_PERFORMANCE_EVALUATION",
    "NO_M0B_EPOCH_OR_DATABASE_REGISTRATION",
    "NO_WALK_FORWARD_OR_HOLDOUT_ACCESS",
    "NO_PAPER_LIVE_OR_PROMOTION_AUTHORITY",
)

RULE_SPACE_DOCUMENT: Final[Mapping[str, object]] = {
    "artifact_schema": "systematic_fx.ai_pattern_rule_space.v1",
    "families": list(ALLOWED_FAMILIES),
    "features": [
        {
            "feature": feature,
            "operators": [
                {"operator": operator, "thresholds": list(thresholds)}
                for operator, thresholds in sorted(ALLOWED_PREDICATES[feature].items())
            ],
        }
        for feature in ALLOWED_FEATURES
    ],
    "maximum_and_predicates": 3,
    "rule_form": "AND_ONLY",
}
RULE_SPACE_SHA256: Final = canonical_sha256(RULE_SPACE_DOCUMENT)

RECORDED_RESPONSE_CONTRACT: Final[Mapping[str, object]] = {
    "artifact_schema": RECORDED_RESPONSE_SCHEMA,
    "exact_top_level_keys": [
        "artifact_schema",
        "context_sha256",
        "proposals",
        "request_sha256",
    ],
    "proposal_exact_keys": ["direction", "family", "rationale_code", "rule"],
    "rule_exact_keys": ["all", "artifact_schema"],
    "predicate_exact_keys": ["feature", "operator", "threshold"],
    "rule_space_sha256": RULE_SPACE_SHA256,
}
RECORDED_RESPONSE_CONTRACT_SHA256: Final = canonical_sha256(RECORDED_RESPONSE_CONTRACT)
DETERMINISTIC_PROMPT_SHA256: Final = canonical_sha256(
    {
        "contract": "outcome-blind support/stability/diversity proposer",
        "rule_space_sha256": RULE_SPACE_SHA256,
        "version": 1,
    }
)

AI_DISCOVERY_CONTEXT_SCHEMA: Final = "systematic_fx.ai_discovery_context.v1"
_AI_CONTEXT_TOP_KEYS: Final = {
    "authority",
    "bar_columns",
    "bars",
    "block_summaries",
    "block_summary_columns",
    "daily_summaries",
    "daily_summary_columns",
    "morphology",
    "schema",
    "source",
    "threshold_lattice",
}
_AI_BAR_COLUMNS: Final = (
    "source_date",
    "start_ns",
    "end_ns",
    "block_number",
    "decision_eligible",
    "range_ticks",
    "signed_body_ppm",
    "close_location_ppm",
    "upper_wick_ppm",
    "lower_wick_ppm",
)
_AI_CONTEXT_SOURCE_KEYS: Final = {
    "active_date_count",
    "bar_row_count",
    "bar_version",
    "dataset_handoff_sha256",
    "dataset_manifest_sha256",
    "decision_date_count",
    "decision_end_date",
    "discovery_calendar_sha256",
    "discovery_end_date",
    "discovery_start_date",
    "raw_source_manifest_sha256",
    "reporting_block_count",
    "source_artifact_byte_count",
    "split_plan_sha256",
    "timeframe_seconds",
}
_AI_CONTEXT_MORPHOLOGY_KEYS: Final = {
    "availability",
    "feature_version",
    "integer_ratio_scale",
    "integer_rounding",
    "lattice_sha256",
    "maximum_bar_rows",
    "maximum_source_artifact_bytes",
    "zero_range_policy",
}
_AI_CONTEXT_LATTICE_KEYS: Final = {
    "axes",
    "maximum_candidates",
    "maximum_conditions_per_candidate",
    "minimum_active_date_options",
    "support_columns",
    "supports",
}
_EXPECTED_AI_AXES: Final = (
    {"feature": "range_ticks", "operator": "GE", "thresholds": [1, 2, 4, 8, 12, 16, 24, 32]},
    {
        "feature": "absolute_body_ppm",
        "operator": "GE",
        "thresholds": [100_000, 250_000, 500_000, 750_000, 900_000],
    },
    {
        "feature": "signed_body_ppm",
        "operator": "GE",
        "thresholds": [100_000, 250_000, 500_000, 750_000],
    },
    {
        "feature": "signed_body_ppm",
        "operator": "LE",
        "thresholds": [-900_000, -750_000, -500_000, -250_000, -100_000],
    },
    {
        "feature": "close_location_ppm",
        "operator": "GE",
        "thresholds": [600_000, 700_000, 800_000, 900_000],
    },
    {
        "feature": "close_location_ppm",
        "operator": "LE",
        "thresholds": [100_000, 200_000, 300_000, 400_000],
    },
    {
        "feature": "upper_wick_ppm",
        "operator": "GE",
        "thresholds": [100_000, 250_000, 500_000, 750_000],
    },
    {
        "feature": "lower_wick_ppm",
        "operator": "GE",
        "thresholds": [100_000, 250_000, 500_000, 750_000],
    },
)


class PatternDiscoveryError(RuntimeError):
    """A bounded proposal run is invalid or cannot be reproduced safely."""


class UnsafeRecordedResponseError(PatternDiscoveryError):
    """A recorded model response escaped its exact non-executable schema."""


class ProposalLedgerError(PatternDiscoveryError):
    """The append-only proposal ledger is incomplete, conflicting, or corrupt."""


class ImmutableArtifactError(PatternDiscoveryError):
    """A content-addressed local artifact differs from its claimed identity."""


def _required_sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise PatternDiscoveryError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _required_identifier(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or _SAFE_IDENTIFIER.fullmatch(value) is None
        or "://" in value
        or ".." in value
    ):
        raise PatternDiscoveryError(f"{label} must be a bounded non-path identifier")
    return value


def _integer(
    value: object,
    *,
    label: str,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PatternDiscoveryError(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        raise PatternDiscoveryError(f"{label} is below its finite minimum")
    if maximum is not None and value > maximum:
        raise PatternDiscoveryError(f"{label} exceeds its finite maximum")
    return value


def _iso_date(value: object, *, label: str) -> str:
    if not isinstance(value, str):
        raise PatternDiscoveryError(f"{label} must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise PatternDiscoveryError(f"{label} must be an ISO date") from error
    if parsed.isoformat() != value:
        raise PatternDiscoveryError(f"{label} must use canonical ISO date form")
    return value


def _utc_timestamp(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise PatternDiscoveryError(f"{label} must be an explicit UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise PatternDiscoveryError(f"{label} must be an explicit UTC timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() != datetime.now(UTC).utcoffset():
        raise PatternDiscoveryError(f"{label} must be UTC")
    return value


def _now_utc() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _exact_keys(value: Mapping[str, object], expected: set[str], *, label: str) -> None:
    if set(value) != expected:
        raise PatternDiscoveryError(f"{label} keys differ from the exact schema")


def _safe_root(path: str | Path, *, create: bool) -> Path:
    requested = Path(path).expanduser()
    if requested.is_symlink():
        raise ImmutableArtifactError("artifact root cannot be symbolic")
    if create:
        requested.mkdir(parents=True, exist_ok=True)
    resolved = requested.resolve(strict=True)
    if not resolved.is_dir():
        raise ImmutableArtifactError("artifact root must be a directory")
    return resolved


def _leaf(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or Path(value).name != value
        or value in {".", ".."}
    ):
        raise ImmutableArtifactError(f"{label} must be one bounded filename")
    return value


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _write_all(fd: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise ImmutableArtifactError("immutable artifact write made no progress")
        view = view[written:]


def _publish_immutable_bytes(root: Path, leaf: str, payload: bytes) -> Path:
    """Atomically publish bytes without ever replacing an existing pathname."""

    destination = root / _leaf(leaf, label="artifact filename")
    if destination.exists() or destination.is_symlink():
        if (
            destination.is_symlink()
            or not destination.is_file()
            or destination.stat().st_mode & _WRITE_BITS
            or destination.read_bytes() != payload
        ):
            raise ImmutableArtifactError("existing immutable artifact bytes or mode differ")
        return destination

    fd, temporary_name = tempfile.mkstemp(prefix=".publish-", suffix=".tmp", dir=root)
    temporary = Path(temporary_name)
    try:
        _write_all(fd, payload)
        os.fsync(fd)
        os.fchmod(fd, 0o444)
        os.close(fd)
        fd = -1
        try:
            os.link(temporary, destination, follow_symlinks=False)
        except FileExistsError:
            if (
                destination.is_symlink()
                or not destination.is_file()
                or destination.stat().st_mode & _WRITE_BITS
                or destination.read_bytes() != payload
            ):
                raise ImmutableArtifactError("concurrent immutable publication conflicts")
        directory_fd = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        temporary.unlink(missing_ok=True)
    return destination


@dataclass(frozen=True, slots=True)
class ArtifactIdentity:
    artifact_type: str
    content_sha256: str
    byte_size: int
    relative_uri: str
    media_type: str = "application/json"

    def __post_init__(self) -> None:
        _required_identifier(self.artifact_type, label="artifact_type")
        _required_sha256(self.content_sha256, label="content_sha256")
        _integer(self.byte_size, label="byte_size", minimum=1, maximum=64 * 1024 * 1024)
        _leaf(self.relative_uri, label="relative_uri")
        if self.media_type not in {"application/json", "application/octet-stream"}:
            raise ImmutableArtifactError("unsupported artifact media_type")

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_type": self.artifact_type,
            "byte_size": self.byte_size,
            "content_sha256": self.content_sha256,
            "media_type": self.media_type,
            "relative_uri": self.relative_uri,
        }

    @classmethod
    def from_dict(cls, value: object) -> ArtifactIdentity:
        if not isinstance(value, dict):
            raise ImmutableArtifactError("artifact identity must be an object")
        _exact_keys(
            value,
            {"artifact_type", "byte_size", "content_sha256", "media_type", "relative_uri"},
            label="artifact identity",
        )
        return cls(
            artifact_type=value["artifact_type"],
            content_sha256=value["content_sha256"],
            byte_size=value["byte_size"],
            relative_uri=value["relative_uri"],
            media_type=value["media_type"],
        )


def publish_canonical_artifact(
    root: str | Path,
    *,
    artifact_type: str,
    filename_prefix: str,
    document: Mapping[str, object],
) -> ArtifactIdentity:
    """Publish one canonical JSON document under its SHA-256 identity."""

    _required_identifier(artifact_type, label="artifact_type")
    _required_identifier(filename_prefix, label="filename_prefix")
    payload = canonical_json_bytes(document)
    digest = hashlib.sha256(payload).hexdigest()
    relative_uri = f"{filename_prefix}-{digest}.json"
    bounded_root = _safe_root(root, create=True)
    _publish_immutable_bytes(bounded_root, relative_uri, payload)
    return ArtifactIdentity(artifact_type, digest, len(payload), relative_uri)


def publish_recorded_response_artifact(
    root: str | Path,
    raw_response: bytes,
    *,
    maximum_byte_size: int,
) -> ArtifactIdentity:
    """Retain exact provider bytes before strict parsing or rejection."""

    if not isinstance(raw_response, bytes) or not raw_response:
        raise UnsafeRecordedResponseError("recorded response must be non-empty bytes")
    _integer(maximum_byte_size, label="maximum_byte_size", minimum=1, maximum=16 * 1024 * 1024)
    if len(raw_response) > maximum_byte_size:
        raise UnsafeRecordedResponseError("recorded response exceeds its precommitted byte budget")
    digest = hashlib.sha256(raw_response).hexdigest()
    relative_uri = f"recorded-response-{digest}.json"
    bounded_root = _safe_root(root, create=True)
    _publish_immutable_bytes(bounded_root, relative_uri, raw_response)
    return ArtifactIdentity(
        "AI_PATTERN_RECORDED_RESPONSE",
        digest,
        len(raw_response),
        relative_uri,
        media_type="application/octet-stream",
    )


def verify_immutable_artifact(
    root: str | Path,
    identity: ArtifactIdentity,
    *,
    expected_bytes: bytes | None = None,
) -> bytes:
    """Reopen one artifact and verify path, mode, size, digest, and optional bytes."""

    bounded_root = _safe_root(root, create=False)
    path = bounded_root / _leaf(identity.relative_uri, label="artifact relative_uri")
    if (
        path.is_symlink()
        or not path.is_file()
        or not path.resolve(strict=True).is_relative_to(bounded_root)
        or path.stat().st_mode & _WRITE_BITS
        or path.stat().st_size != identity.byte_size
        or _file_sha256(path) != identity.content_sha256
    ):
        raise ImmutableArtifactError("immutable artifact identity verification failed")
    payload = path.read_bytes()
    if expected_bytes is not None and payload != expected_bytes:
        raise ImmutableArtifactError("immutable artifact content differs from reconstruction")
    return payload


@dataclass(frozen=True, slots=True, order=True)
class RulePredicate:
    feature: str
    operator: RuleOperator
    threshold: int

    def __post_init__(self) -> None:
        if self.feature not in ALLOWED_PREDICATES:
            raise PatternDiscoveryError("rule predicate uses a feature outside the finite DSL")
        if self.operator not in ALLOWED_PREDICATES[self.feature]:
            raise PatternDiscoveryError("rule predicate feature/operator pair is outside the finite DSL")
        _integer(self.threshold, label="rule threshold")
        if self.threshold not in ALLOWED_PREDICATES[self.feature][self.operator]:
            raise PatternDiscoveryError("rule predicate threshold is outside the finite lattice")

    def as_dict(self) -> dict[str, object]:
        return {
            "feature": self.feature,
            "operator": self.operator,
            "threshold": self.threshold,
        }

    @classmethod
    def from_dict(cls, value: object) -> RulePredicate:
        if not isinstance(value, dict):
            raise PatternDiscoveryError("rule predicate must be an object")
        _exact_keys(value, {"feature", "operator", "threshold"}, label="rule predicate")
        return cls(value["feature"], value["operator"], value["threshold"])

    def matches(self, feature_value: int) -> bool:
        return feature_value >= self.threshold if self.operator == "GE" else feature_value <= self.threshold


@dataclass(frozen=True, slots=True)
class AndRule:
    predicates: tuple[RulePredicate, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.predicates, tuple) or not 1 <= len(self.predicates) <= 3:
            raise PatternDiscoveryError("rule must contain between one and three AND predicates")
        if not all(isinstance(item, RulePredicate) for item in self.predicates):
            raise PatternDiscoveryError("rule predicates must be canonical RulePredicate values")
        ordered = tuple(sorted(self.predicates))
        if len(set(ordered)) != len(ordered):
            raise PatternDiscoveryError("rule cannot contain duplicate predicates")
        by_feature_operator: set[tuple[str, str]] = set()
        lower: dict[str, int] = {}
        upper: dict[str, int] = {}
        for predicate in ordered:
            key = (predicate.feature, predicate.operator)
            if key in by_feature_operator:
                raise PatternDiscoveryError("rule cannot contain redundant same-side predicates")
            by_feature_operator.add(key)
            if predicate.operator == "GE":
                lower[predicate.feature] = predicate.threshold
            else:
                upper[predicate.feature] = predicate.threshold
        if any(lower[name] > upper[name] for name in lower.keys() & upper.keys()):
            raise PatternDiscoveryError("rule contains contradictory predicates")
        object.__setattr__(self, "predicates", ordered)

    def as_dict(self) -> dict[str, object]:
        return {
            "all": [predicate.as_dict() for predicate in self.predicates],
            "artifact_schema": RULE_SCHEMA,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())

    @classmethod
    def from_dict(cls, value: object) -> AndRule:
        if not isinstance(value, dict):
            raise PatternDiscoveryError("rule must be an object")
        _exact_keys(value, {"all", "artifact_schema"}, label="rule")
        if value.get("artifact_schema") != RULE_SCHEMA or not isinstance(value.get("all"), list):
            raise PatternDiscoveryError("rule schema differs from the finite AND DSL")
        return cls(tuple(RulePredicate.from_dict(item) for item in value["all"]))

    def matches(self, values: tuple[int, ...]) -> bool:
        by_name = dict(zip(ALLOWED_FEATURES, values, strict=True))
        return all(predicate.matches(by_name[predicate.feature]) for predicate in self.predicates)


@dataclass(frozen=True, slots=True, order=True)
class DiscoveryVectorBin:
    session_id: str
    values: tuple[int, ...]
    row_count: int

    def __post_init__(self) -> None:
        _required_identifier(self.session_id, label="session_id")
        if not isinstance(self.values, tuple) or len(self.values) != len(ALLOWED_FEATURES):
            raise PatternDiscoveryError("feature vector differs from the fixed feature order")
        for feature, value in zip(ALLOWED_FEATURES, self.values, strict=True):
            lower, upper = _FEATURE_BOUNDS[feature]
            _integer(value, label=feature, minimum=lower, maximum=upper)
        _integer(self.row_count, label="row_count", minimum=1, maximum=10_000_000)

    def as_dict(self) -> dict[str, object]:
        return {
            "row_count": self.row_count,
            "session_id": self.session_id,
            "values": list(self.values),
        }


@dataclass(frozen=True, slots=True)
class ProposalRequest:
    request_key: str
    proposer_mode: ProposerMode
    provider_id: str
    model_id: str
    model_version: str
    prompt_sha256: str
    source_feature_sha256: str
    source_feature_version: str
    discovery_split_sha256: str
    source_interval_start: str
    source_interval_end: str
    max_source_rows: int
    max_context_bins: int
    proposal_budget: int
    max_predicates_per_rule: int
    minimum_support_rows: int
    minimum_session_count: int
    minimum_stability_ppm: int
    maximum_pairwise_overlap_ppm: int
    max_model_calls: int
    max_input_tokens: int
    max_output_tokens: int
    max_response_bytes: int
    deterministic_seed: int
    precommitted_at_utc: str
    candidate_evaluation_budget: int
    candidate_catalog_sha256: str
    code_commit: str
    proposer_implementation_sha256: str
    dependency_lock_sha256: str

    def __post_init__(self) -> None:
        for name in (
            "request_key",
            "provider_id",
            "model_id",
            "model_version",
            "source_feature_version",
        ):
            _required_identifier(getattr(self, name), label=name)
        if self.proposer_mode not in {"DETERMINISTIC_OUTCOME_BLIND_V1", "RECORDED_RESPONSE_V1"}:
            raise PatternDiscoveryError("proposer_mode is outside the fixed proposer contract")
        for name in ("prompt_sha256", "source_feature_sha256", "discovery_split_sha256"):
            _required_sha256(getattr(self, name), label=name)
        if any(
            not set(getattr(self, name)) - {"0"}
            for name in (
                "candidate_catalog_sha256",
                "proposer_implementation_sha256",
                "dependency_lock_sha256",
            )
        ):
            raise PatternDiscoveryError("proposal provenance cannot use an all-zero sentinel")
        start = _iso_date(self.source_interval_start, label="source_interval_start")
        end = _iso_date(self.source_interval_end, label="source_interval_end")
        if start > end:
            raise PatternDiscoveryError("Discovery source interval is reversed")
        _integer(self.max_source_rows, label="max_source_rows", minimum=1, maximum=1_000_000)
        _integer(self.max_context_bins, label="max_context_bins", minimum=1, maximum=1_000_000)
        if self.max_context_bins > self.max_source_rows:
            raise PatternDiscoveryError("context bin budget cannot exceed source row budget")
        _integer(self.proposal_budget, label="proposal_budget", minimum=1, maximum=100)
        _integer(self.max_predicates_per_rule, label="max_predicates_per_rule", minimum=1, maximum=3)
        _integer(
            self.minimum_support_rows,
            label="minimum_support_rows",
            minimum=1,
            maximum=self.max_source_rows,
        )
        _integer(self.minimum_session_count, label="minimum_session_count", minimum=1, maximum=10_000)
        _integer(self.minimum_stability_ppm, label="minimum_stability_ppm", minimum=0, maximum=_PPM)
        _integer(
            self.maximum_pairwise_overlap_ppm,
            label="maximum_pairwise_overlap_ppm",
            minimum=0,
            maximum=_PPM,
        )
        _integer(self.deterministic_seed, label="deterministic_seed", minimum=0, maximum=_UINT64_MAX)
        _utc_timestamp(self.precommitted_at_utc, label="precommitted_at_utc")
        _integer(
            self.candidate_evaluation_budget,
            label="candidate_evaluation_budget",
            minimum=1,
            maximum=100_000,
        )
        for name in (
            "candidate_catalog_sha256",
            "proposer_implementation_sha256",
            "dependency_lock_sha256",
        ):
            _required_sha256(getattr(self, name), label=name)
        if (
            not isinstance(self.code_commit, str)
            or len(self.code_commit) not in {40, 64}
            or _SHA256.fullmatch(self.code_commit.zfill(64)) is None
            or not set(self.code_commit) - {"0"}
        ):
            raise PatternDiscoveryError("code_commit must be a full lowercase Git object ID")
        if self.proposer_mode == "DETERMINISTIC_OUTCOME_BLIND_V1":
            if (
                self.provider_id != "SYSTEMATIC_FX_LOCAL"
                or self.model_id != "OUTCOME_BLIND_SUPPORT_STABILITY_DIVERSITY"
                or self.model_version != "v1"
                or self.prompt_sha256 != DETERMINISTIC_PROMPT_SHA256
                or any(
                    value != 0
                    for value in (
                        self.max_model_calls,
                        self.max_input_tokens,
                        self.max_output_tokens,
                        self.max_response_bytes,
                    )
                )
            ):
                raise PatternDiscoveryError("deterministic proposer identity or zero model budget differs")
        else:
            _integer(self.max_model_calls, label="max_model_calls", minimum=1, maximum=1)
            _integer(self.max_input_tokens, label="max_input_tokens", minimum=1, maximum=1_000_000)
            _integer(self.max_output_tokens, label="max_output_tokens", minimum=1, maximum=100_000)
            _integer(self.max_response_bytes, label="max_response_bytes", minimum=1, maximum=16 * 1024 * 1024)

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": REQUEST_SCHEMA,
            "authority": AUTHORITY,
            "budgets": {
                "candidate_evaluation_budget": self.candidate_evaluation_budget,
                "deterministic_seed": self.deterministic_seed,
                "max_context_bins": self.max_context_bins,
                "max_input_tokens": self.max_input_tokens,
                "max_model_calls": self.max_model_calls,
                "max_output_tokens": self.max_output_tokens,
                "max_predicates_per_rule": self.max_predicates_per_rule,
                "max_response_bytes": self.max_response_bytes,
                "max_source_rows": self.max_source_rows,
                "proposal_budget": self.proposal_budget,
            },
            "data_boundary": {
                "data_role": "SEARCH",
                "discovery_split_sha256": self.discovery_split_sha256,
                "label_access": False,
                "outcome_access": False,
                "sealed_holdout_untouched": True,
                "source_feature_sha256": self.source_feature_sha256,
                "source_feature_version": self.source_feature_version,
                "source_interval_end": self.source_interval_end,
                "source_interval_start": self.source_interval_start,
                "split_role": "DISCOVERY",
            },
            "execution_prohibited": {
                "database_mutation": True,
                "m0b_epoch_registration": True,
                "paper_live_or_promotion": True,
                "performance_evaluation": True,
            },
            "precommitted_at_utc": self.precommitted_at_utc,
            "provenance": {
                "candidate_catalog_sha256": self.candidate_catalog_sha256,
                "code_commit": self.code_commit,
                "proposer_implementation_sha256": self.proposer_implementation_sha256,
                "dependency_lock_sha256": self.dependency_lock_sha256,
            },
            "proposer": {
                "mode": self.proposer_mode,
                "model_id": self.model_id,
                "model_version": self.model_version,
                "prompt_sha256": self.prompt_sha256,
                "provider_id": self.provider_id,
                "recorded_response_contract_sha256": RECORDED_RESPONSE_CONTRACT_SHA256,
            },
            "request_key": self.request_key,
            "rule_space_sha256": RULE_SPACE_SHA256,
            "selection": {
                "maximum_pairwise_overlap_ppm": self.maximum_pairwise_overlap_ppm,
                "minimum_session_count": self.minimum_session_count,
                "minimum_stability_ppm": self.minimum_stability_ppm,
                "minimum_support_rows": self.minimum_support_rows,
                "ranking_inputs": ["SUPPORT", "SESSION_STABILITY", "SIGNAL_DIVERSITY"],
            },
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


@dataclass(frozen=True, slots=True)
class DiscoveryContext:
    request_sha256: str
    source_feature_sha256: str
    source_feature_version: str
    discovery_split_sha256: str
    source_interval_start: str
    source_interval_end: str
    source_row_count: int
    session_row_counts: tuple[tuple[str, int], ...]
    bins: tuple[DiscoveryVectorBin, ...]

    def __post_init__(self) -> None:
        for name in ("request_sha256", "source_feature_sha256", "discovery_split_sha256"):
            _required_sha256(getattr(self, name), label=name)
        _required_identifier(self.source_feature_version, label="source_feature_version")
        if _iso_date(self.source_interval_start, label="source_interval_start") > _iso_date(
            self.source_interval_end, label="source_interval_end"
        ):
            raise PatternDiscoveryError("context source interval is reversed")
        _integer(self.source_row_count, label="source_row_count", minimum=1, maximum=1_000_000)
        if not self.bins or tuple(sorted(self.bins)) != self.bins:
            raise PatternDiscoveryError("context bins must be non-empty, unique, and canonical")
        if len({(item.session_id, item.values) for item in self.bins}) != len(self.bins):
            raise PatternDiscoveryError("context bins contain duplicate feature vectors")
        expected_counts: Counter[str] = Counter()
        for item in self.bins:
            expected_counts[item.session_id] += item.row_count
        observed_counts = tuple(sorted(self.session_row_counts))
        if observed_counts != self.session_row_counts or observed_counts != tuple(sorted(expected_counts.items())):
            raise PatternDiscoveryError("context session row accounting differs")
        if sum(expected_counts.values()) != self.source_row_count:
            raise PatternDiscoveryError("context source row accounting differs")

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": CONTEXT_SCHEMA,
            "authority": AUTHORITY,
            "bins": [item.as_dict() for item in self.bins],
            "data_boundary": {
                "data_role": "SEARCH",
                "discovery_split_sha256": self.discovery_split_sha256,
                "feature_order": list(ALLOWED_FEATURES),
                "label_access": False,
                "outcome_access": False,
                "sealed_holdout_untouched": True,
                "source_feature_sha256": self.source_feature_sha256,
                "source_feature_version": self.source_feature_version,
                "source_interval_end": self.source_interval_end,
                "source_interval_start": self.source_interval_start,
                "split_role": "DISCOVERY",
            },
            "request_sha256": self.request_sha256,
            "rule_space_sha256": RULE_SPACE_SHA256,
            "session_row_counts": [
                {"row_count": count, "session_id": session}
                for session, count in self.session_row_counts
            ],
            "source_row_count": self.source_row_count,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


def build_discovery_context(
    request: ProposalRequest,
    feature_rows: Iterable[Mapping[str, object]],
) -> DiscoveryContext:
    """Build a bounded, order-independent projection containing no outcome fields."""

    if not isinstance(request, ProposalRequest):
        raise PatternDiscoveryError("context build requires a canonical ProposalRequest")
    expected_keys = {"session_id", *ALLOWED_FEATURES}
    counts: Counter[tuple[str, tuple[int, ...]]] = Counter()
    row_count = 0
    for index, row in enumerate(feature_rows):
        if not isinstance(row, Mapping) or set(row) != expected_keys:
            raise PatternDiscoveryError(
                f"feature_rows[{index}] must be an exact feature-only projection; extra label, "
                "outcome, path, query, and code fields are prohibited"
            )
        session_id = _required_identifier(row["session_id"], label=f"feature_rows[{index}].session_id")
        values: list[int] = []
        for feature in ALLOWED_FEATURES:
            lower, upper = _FEATURE_BOUNDS[feature]
            values.append(
                _integer(row[feature], label=f"feature_rows[{index}].{feature}", minimum=lower, maximum=upper)
            )
        row_count += 1
        if row_count > request.max_source_rows:
            raise PatternDiscoveryError("Discovery feature rows exceed the precommitted source budget")
        counts[(session_id, tuple(values))] += 1
        if len(counts) > request.max_context_bins:
            raise PatternDiscoveryError("Discovery context bins exceed the precommitted compactness budget")
    if row_count == 0:
        raise PatternDiscoveryError("Discovery context cannot be empty")
    bins = tuple(
        DiscoveryVectorBin(session, values, weight)
        for (session, values), weight in sorted(counts.items())
    )
    sessions: Counter[str] = Counter()
    for item in bins:
        sessions[item.session_id] += item.row_count
    if len(sessions) < request.minimum_session_count:
        raise PatternDiscoveryError("Discovery context has fewer sessions than the precommitted minimum")
    return DiscoveryContext(
        request_sha256=request.sha256,
        source_feature_sha256=request.source_feature_sha256,
        source_feature_version=request.source_feature_version,
        discovery_split_sha256=request.discovery_split_sha256,
        source_interval_start=request.source_interval_start,
        source_interval_end=request.source_interval_end,
        source_row_count=row_count,
        session_row_counts=tuple(sorted(sessions.items())),
        bins=bins,
    )


def context_from_ai_discovery_document(
    request: ProposalRequest,
    document: Mapping[str, object],
    *,
    expected_context_sha256: str,
) -> DiscoveryContext:
    """Project one verified bar-morphology context into the finite proposer input.

    The source document is the exact schema published by
    :mod:`systematic_fx.research.ai_discovery_context`.  Only rows explicitly
    marked decision-eligible are ranked; the visible non-decision tail remains
    excluded from support and stability calculations.  Each source date is a
    stability session, so the proposer cannot call one dense reporting block
    broad coverage.
    """

    if not isinstance(request, ProposalRequest):
        raise PatternDiscoveryError("AI context projection requires a canonical request")
    _required_sha256(expected_context_sha256, label="expected_context_sha256")
    if not isinstance(document, Mapping):
        raise PatternDiscoveryError("AI Discovery context must be an object")
    try:
        observed_context_sha256 = canonical_sha256(document)
    except (TypeError, ValueError) as error:
        raise PatternDiscoveryError("AI Discovery context is not canonical research JSON") from error
    if (
        observed_context_sha256 != expected_context_sha256
        or request.source_feature_sha256 != expected_context_sha256
    ):
        raise PatternDiscoveryError("AI Discovery context content identity differs")
    _exact_keys(document, _AI_CONTEXT_TOP_KEYS, label="AI Discovery context")
    if document.get("schema") != AI_DISCOVERY_CONTEXT_SCHEMA:
        raise PatternDiscoveryError("AI Discovery context schema differs")
    authority = document.get("authority")
    if authority != {
        "content_policy": "COMPLETED_5M_BAR_MORPHOLOGY_ONLY",
        "data_role": "DISCOVERY",
        "maximum_status": "HYPOTHESIS_CONTEXT_ONLY",
        "visibility": "VISIBLE",
    }:
        raise PatternDiscoveryError("AI Discovery context authority differs")

    source = document.get("source")
    morphology = document.get("morphology")
    lattice = document.get("threshold_lattice")
    if not isinstance(source, dict):
        raise PatternDiscoveryError("AI Discovery context source must be an object")
    if not isinstance(morphology, dict):
        raise PatternDiscoveryError("AI Discovery context morphology must be an object")
    if not isinstance(lattice, dict):
        raise PatternDiscoveryError("AI Discovery threshold lattice must be an object")
    _exact_keys(source, _AI_CONTEXT_SOURCE_KEYS, label="AI Discovery source")
    _exact_keys(morphology, _AI_CONTEXT_MORPHOLOGY_KEYS, label="AI Discovery morphology")
    _exact_keys(lattice, _AI_CONTEXT_LATTICE_KEYS, label="AI Discovery threshold lattice")
    if (
        morphology.get("availability") != "AFTER_BAR_CLOSE"
        or morphology.get("integer_ratio_scale") != _PPM
        or morphology.get("integer_rounding") != "TRUNCATE_TOWARD_ZERO"
        or morphology.get("zero_range_policy")
        != "MIDPOINT_LOCATION_AND_ZERO_BODY_WICKS"
        or morphology.get("feature_version") != request.source_feature_version
        or source.get("split_plan_sha256") != request.discovery_split_sha256
        or source.get("discovery_start_date") != request.source_interval_start
        or source.get("decision_end_date") != request.source_interval_end
        or source.get("timeframe_seconds") != 300
    ):
        raise PatternDiscoveryError("AI Discovery source or morphology identity differs")
    if (
        lattice.get("axes") != list(_EXPECTED_AI_AXES)
        or lattice.get("maximum_conditions_per_candidate") != 3
        or not isinstance(lattice.get("maximum_candidates"), int)
        or lattice["maximum_candidates"] < request.proposal_budget
    ):
        raise PatternDiscoveryError("AI Discovery finite threshold lattice differs")
    if document.get("bar_columns") != list(_AI_BAR_COLUMNS):
        raise PatternDiscoveryError("AI Discovery bar columns differ")
    bars = document.get("bars")
    if not isinstance(bars, list):
        raise PatternDiscoveryError("AI Discovery bars must be an array")
    expected_row_count = _integer(
        source.get("bar_row_count"),
        label="AI Discovery source bar_row_count",
        minimum=1,
        maximum=request.max_source_rows,
    )
    if len(bars) != expected_row_count:
        raise PatternDiscoveryError("AI Discovery bar row count differs")

    def decision_feature_rows() -> Iterable[Mapping[str, object]]:
        decision_dates: set[str] = set()
        for index, raw in enumerate(bars):
            if not isinstance(raw, list) or len(raw) != len(_AI_BAR_COLUMNS):
                raise PatternDiscoveryError(f"AI Discovery bar {index} schema differs")
            source_date = _iso_date(raw[0], label=f"AI Discovery bar {index} source_date")
            _integer(raw[1], label=f"AI Discovery bar {index} start_ns", minimum=0)
            _integer(raw[2], label=f"AI Discovery bar {index} end_ns", minimum=1)
            if not isinstance(raw[4], bool):
                raise PatternDiscoveryError(
                    f"AI Discovery bar {index} decision_eligible must be boolean"
                )
            block_number = raw[3]
            if raw[4]:
                _integer(
                    block_number,
                    label=f"AI Discovery bar {index} block_number",
                    minimum=1,
                    maximum=4,
                )
                if source_date > request.source_interval_end:
                    raise PatternDiscoveryError("decision-eligible bar exceeds the frozen prefix")
            elif block_number is not None:
                raise PatternDiscoveryError("non-decision bar cannot have a reporting block")
            range_ticks = _integer(
                raw[5], label=f"AI Discovery bar {index} range_ticks", minimum=0
            )
            signed_body = _integer(
                raw[6],
                label=f"AI Discovery bar {index} signed_body_ppm",
                minimum=-_PPM,
                maximum=_PPM,
            )
            close_location = _integer(
                raw[7],
                label=f"AI Discovery bar {index} close_location_ppm",
                minimum=0,
                maximum=_PPM,
            )
            upper_wick = _integer(
                raw[8],
                label=f"AI Discovery bar {index} upper_wick_ppm",
                minimum=0,
                maximum=_PPM,
            )
            lower_wick = _integer(
                raw[9],
                label=f"AI Discovery bar {index} lower_wick_ppm",
                minimum=0,
                maximum=_PPM,
            )
            if not raw[4]:
                continue
            decision_dates.add(source_date)
            yield {
                "absolute_body_ppm": abs(signed_body),
                "close_location_ppm": close_location,
                "lower_wick_ppm": lower_wick,
                "range_ticks": range_ticks,
                "session_id": source_date,
                "signed_body_ppm": signed_body,
                "upper_wick_ppm": upper_wick,
            }
        expected_decision_dates = _integer(
            source.get("decision_date_count"),
            label="AI Discovery decision_date_count",
            minimum=1,
            maximum=10_000,
        )
        if len(decision_dates) != expected_decision_dates:
            raise PatternDiscoveryError("AI Discovery decision-date coverage differs")

    return build_discovery_context(request, decision_feature_rows())


@dataclass(frozen=True, slots=True)
class ProposedPattern:
    family: str
    direction: Direction
    rationale_code: str
    rule: AndRule

    def __post_init__(self) -> None:
        if self.family not in _RATIONALE_BY_FAMILY:
            raise PatternDiscoveryError("proposal family is outside the finite rule universe")
        if self.direction not in {"LONG", "SHORT"}:
            raise PatternDiscoveryError("proposal direction must be LONG or SHORT")
        if self.rationale_code != _RATIONALE_BY_FAMILY[self.family]:
            raise PatternDiscoveryError("proposal rationale code differs from its finite family")
        if not isinstance(self.rule, AndRule):
            raise PatternDiscoveryError("proposal rule must be a canonical AndRule")

    def as_dict(self) -> dict[str, object]:
        return {
            "direction": self.direction,
            "family": self.family,
            "rationale_code": self.rationale_code,
            "rule": self.rule.as_dict(),
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


@dataclass(frozen=True, slots=True)
class PatternProposal:
    request_sha256: str
    context_sha256: str
    pattern: ProposedPattern
    selection_rank: int
    support_rows: int
    support_ppm: int
    session_support_ppm: tuple[tuple[str, int], ...]
    session_support_count: int
    stability_ppm: int
    max_overlap_with_prior_ppm: int

    def __post_init__(self) -> None:
        _required_sha256(self.request_sha256, label="request_sha256")
        _required_sha256(self.context_sha256, label="context_sha256")
        if not isinstance(self.pattern, ProposedPattern):
            raise PatternDiscoveryError("pattern must be a canonical ProposedPattern")
        _integer(self.selection_rank, label="selection_rank", minimum=1, maximum=100)
        _integer(self.support_rows, label="support_rows", minimum=1, maximum=1_000_000)
        for name in ("support_ppm", "stability_ppm", "max_overlap_with_prior_ppm"):
            _integer(getattr(self, name), label=name, minimum=0, maximum=_PPM)
        if tuple(sorted(self.session_support_ppm)) != self.session_support_ppm:
            raise PatternDiscoveryError("session support must use canonical session order")
        if any(value < 0 or value > _PPM for _, value in self.session_support_ppm):
            raise PatternDiscoveryError("session support is outside ppm bounds")
        expected_count = sum(value > 0 for _, value in self.session_support_ppm)
        if self.session_support_count != expected_count:
            raise PatternDiscoveryError("session support count differs from its exact rates")

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": PROPOSAL_SCHEMA,
            "authority": AUTHORITY,
            "context_sha256": self.context_sha256,
            "direction": self.pattern.direction,
            "family": self.pattern.family,
            "max_overlap_with_prior_ppm": self.max_overlap_with_prior_ppm,
            "outcome_metrics": None,
            "rationale_code": self.pattern.rationale_code,
            "request_sha256": self.request_sha256,
            "rule": self.pattern.rule.as_dict(),
            "selection_rank": self.selection_rank,
            "session_support_count": self.session_support_count,
            "session_support_ppm": [
                {"session_id": session, "support_ppm": value}
                for session, value in self.session_support_ppm
            ],
            "stability_ppm": self.stability_ppm,
            "status": "HYPOTHESIS_PROPOSED_NOT_EVALUATED",
            "support_ppm": self.support_ppm,
            "support_rows": self.support_rows,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


@dataclass(frozen=True, slots=True)
class ProposalBatch:
    request_sha256: str
    context_sha256: str
    proposer_mode: ProposerMode
    recorded_response_sha256: str | None
    proposal_budget: int
    candidate_universe_count: int
    support_eligible_count: int
    diversity_rejected_count: int
    proposals: tuple[PatternProposal, ...]

    def __post_init__(self) -> None:
        _required_sha256(self.request_sha256, label="request_sha256")
        _required_sha256(self.context_sha256, label="context_sha256")
        if self.proposer_mode not in {"DETERMINISTIC_OUTCOME_BLIND_V1", "RECORDED_RESPONSE_V1"}:
            raise PatternDiscoveryError("batch proposer_mode differs")
        if self.proposer_mode == "RECORDED_RESPONSE_V1":
            _required_sha256(self.recorded_response_sha256, label="recorded_response_sha256")
        elif self.recorded_response_sha256 is not None:
            raise PatternDiscoveryError("deterministic batch cannot bind a model response")
        _integer(self.proposal_budget, label="proposal_budget", minimum=1, maximum=100)
        for name in ("candidate_universe_count", "support_eligible_count", "diversity_rejected_count"):
            _integer(getattr(self, name), label=name, minimum=0, maximum=1_000_000)
        if len(self.proposals) != self.proposal_budget:
            raise PatternDiscoveryError("proposal batch must spend its exact precommitted budget")
        if tuple(item.selection_rank for item in self.proposals) != tuple(
            range(1, self.proposal_budget + 1)
        ):
            raise PatternDiscoveryError("proposal selection ranks must be contiguous")
        if len({item.pattern.sha256 for item in self.proposals}) != len(self.proposals):
            raise PatternDiscoveryError("proposal batch contains duplicate rule identities")
        if any(
            item.request_sha256 != self.request_sha256 or item.context_sha256 != self.context_sha256
            for item in self.proposals
        ):
            raise PatternDiscoveryError("proposal batch lineage differs")

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": BATCH_SCHEMA,
            "authority": AUTHORITY,
            "candidate_universe_count": self.candidate_universe_count,
            "context_sha256": self.context_sha256,
            "diversity_rejected_count": self.diversity_rejected_count,
            "limitations": list(LIMITATIONS),
            "proposal_budget": self.proposal_budget,
            "proposals": [
                {"proposal": item.as_dict(), "proposal_sha256": item.sha256}
                for item in self.proposals
            ],
            "proposer_mode": self.proposer_mode,
            "recorded_response_sha256": self.recorded_response_sha256,
            "request_sha256": self.request_sha256,
            "status": FINAL_STATUS,
            "support_eligible_count": self.support_eligible_count,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


@dataclass(frozen=True, slots=True)
class _Assessment:
    pattern: ProposedPattern
    support_rows: int
    support_ppm: int
    session_support_ppm: tuple[tuple[str, int], ...]
    stability_ppm: int
    matching_bins: frozenset[int]

    @property
    def session_support_count(self) -> int:
        return sum(value > 0 for _, value in self.session_support_ppm)


def _assess_pattern(context: DiscoveryContext, pattern: ProposedPattern) -> _Assessment:
    matched_by_session: Counter[str] = Counter()
    matching_bins: set[int] = set()
    support = 0
    for ordinal, item in enumerate(context.bins):
        if pattern.rule.matches(item.values):
            matching_bins.add(ordinal)
            support += item.row_count
            matched_by_session[item.session_id] += item.row_count
    session_rates = tuple(
        (session, matched_by_session[session] * _PPM // total)
        for session, total in context.session_row_counts
    )
    rates = [value for _, value in session_rates]
    stability = _PPM - (max(rates) - min(rates))
    return _Assessment(
        pattern=pattern,
        support_rows=support,
        support_ppm=support * _PPM // context.source_row_count,
        session_support_ppm=session_rates,
        stability_ppm=stability,
        matching_bins=frozenset(matching_bins),
    )


def _weighted_overlap_ppm(
    context: DiscoveryContext,
    left: frozenset[int],
    right: frozenset[int],
) -> int:
    union = left | right
    if not union:
        return 0
    intersection_weight = sum(context.bins[index].row_count for index in left & right)
    union_weight = sum(context.bins[index].row_count for index in union)
    return intersection_weight * _PPM // union_weight


def _predicate(feature: str, operator: RuleOperator, threshold: int) -> RulePredicate:
    return RulePredicate(feature, operator, threshold)


def _deterministic_universe(max_predicates: int) -> tuple[ProposedPattern, ...]:
    specs: dict[str, ProposedPattern] = {}

    def add(family: str, direction: Direction, predicates: tuple[RulePredicate, ...]) -> None:
        if len(predicates) > max_predicates:
            return
        pattern = ProposedPattern(
            family,
            direction,
            _RATIONALE_BY_FAMILY[family],
            AndRule(predicates),
        )
        specs[pattern.sha256] = pattern

    for direction in ("LONG", "SHORT"):
        if direction == "LONG":
            body_operator: RuleOperator = "GE"
            body_values = (100_000, 250_000, 500_000, 750_000)
            close_operator: RuleOperator = "GE"
            close_values = (600_000, 700_000, 800_000, 900_000)
            wick_feature = "lower_wick_ppm"
        else:
            body_operator = "LE"
            body_values = (-100_000, -250_000, -500_000, -750_000)
            close_operator = "LE"
            close_values = (400_000, 300_000, 200_000, 100_000)
            wick_feature = "upper_wick_ppm"

        for body in body_values:
            for close in close_values:
                add(
                    "BODY_CLOSE_CONFIRMATION",
                    direction,
                    (
                        _predicate("signed_body_ppm", body_operator, body),
                        _predicate("close_location_ppm", close_operator, close),
                    ),
                )
                for range_ticks in (4, 8, 12, 16, 24):
                    add(
                        "BODY_CLOSE_CONFIRMATION",
                        direction,
                        (
                            _predicate("signed_body_ppm", body_operator, body),
                            _predicate("close_location_ppm", close_operator, close),
                            _predicate("range_ticks", "GE", range_ticks),
                        ),
                    )

        for wick in (100_000, 250_000, 500_000, 750_000):
            for close in close_values:
                add(
                    "WICK_REJECTION_REVERSAL",
                    direction,
                    (
                        _predicate(wick_feature, "GE", wick),
                        _predicate("close_location_ppm", close_operator, close),
                    ),
                )
                for body in (100_000, 250_000, 500_000):
                    add(
                        "WICK_REJECTION_REVERSAL",
                        direction,
                        (
                            _predicate(wick_feature, "GE", wick),
                            _predicate("close_location_ppm", close_operator, close),
                            _predicate("absolute_body_ppm", "GE", body),
                        ),
                    )

        for range_ticks in (4, 8, 12, 16, 24, 32):
            for absolute_body in (100_000, 250_000, 500_000, 750_000, 900_000):
                add(
                    "RANGE_EXPANSION_CONTINUATION",
                    direction,
                    (
                        _predicate("range_ticks", "GE", range_ticks),
                        _predicate("absolute_body_ppm", "GE", absolute_body),
                    ),
                )
                for body in body_values:
                    add(
                        "RANGE_EXPANSION_CONTINUATION",
                        direction,
                        (
                            _predicate("range_ticks", "GE", range_ticks),
                            _predicate("absolute_body_ppm", "GE", absolute_body),
                            _predicate("signed_body_ppm", body_operator, body),
                        ),
                    )
    return tuple(specs[key] for key in sorted(specs))


def _select_proposals(
    request: ProposalRequest,
    context: DiscoveryContext,
    patterns: tuple[ProposedPattern, ...],
    *,
    recorded_response_sha256: str | None,
) -> ProposalBatch:
    if context.request_sha256 != request.sha256:
        raise PatternDiscoveryError("proposal context belongs to another precommitted request")
    if len({item.sha256 for item in patterns}) != len(patterns):
        raise PatternDiscoveryError("proposal input contains duplicate canonical rules")
    assessments = tuple(_assess_pattern(context, pattern) for pattern in patterns)
    eligible = [
        item
        for item in assessments
        if item.support_rows >= request.minimum_support_rows
        and item.session_support_count >= request.minimum_session_count
        and item.stability_ppm >= request.minimum_stability_ppm
    ]
    eligible.sort(
        key=lambda item: (
            -item.stability_ppm,
            -item.support_rows,
            item.pattern.sha256,
        )
    )
    selected: list[tuple[_Assessment, int]] = []
    diversity_rejected = 0
    for item in eligible:
        overlaps = [
            _weighted_overlap_ppm(context, item.matching_bins, prior.matching_bins)
            for prior, _ in selected
        ]
        maximum_overlap = max(overlaps, default=0)
        if maximum_overlap > request.maximum_pairwise_overlap_ppm:
            diversity_rejected += 1
            continue
        selected.append((item, maximum_overlap))
        if len(selected) == request.proposal_budget:
            break
    if len(selected) != request.proposal_budget:
        raise PatternDiscoveryError(
            "eligible diverse hypotheses cannot spend the exact precommitted proposal budget"
        )
    proposals = tuple(
        PatternProposal(
            request_sha256=request.sha256,
            context_sha256=context.sha256,
            pattern=item.pattern,
            selection_rank=rank,
            support_rows=item.support_rows,
            support_ppm=item.support_ppm,
            session_support_ppm=item.session_support_ppm,
            session_support_count=item.session_support_count,
            stability_ppm=item.stability_ppm,
            max_overlap_with_prior_ppm=maximum_overlap,
        )
        for rank, (item, maximum_overlap) in enumerate(selected, start=1)
    )
    return ProposalBatch(
        request_sha256=request.sha256,
        context_sha256=context.sha256,
        proposer_mode=request.proposer_mode,
        recorded_response_sha256=recorded_response_sha256,
        proposal_budget=request.proposal_budget,
        candidate_universe_count=len(patterns),
        support_eligible_count=len(eligible),
        diversity_rejected_count=diversity_rejected,
        proposals=proposals,
    )


def propose_deterministically(request: ProposalRequest, context: DiscoveryContext) -> ProposalBatch:
    """Rank a finite rule universe using only support, stability, and diversity."""

    if request.proposer_mode != "DETERMINISTIC_OUTCOME_BLIND_V1":
        raise PatternDiscoveryError("deterministic proposer requires deterministic request mode")
    universe = _deterministic_universe(request.max_predicates_per_rule)
    if (
        len(universe) != request.candidate_evaluation_budget
        or canonical_sha256([item.as_dict() for item in universe])
        != request.candidate_catalog_sha256
    ):
        raise PatternDiscoveryError("precommitted candidate catalog identity differs")
    return _select_proposals(
        request,
        context,
        universe,
        recorded_response_sha256=None,
    )


class _DuplicateJsonKey(ValueError):
    pass


def _reject_float(_: str) -> object:
    raise UnsafeRecordedResponseError("recorded response cannot contain binary floats")


def _object_without_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateJsonKey(key)
        value[key] = item
    return value


def parse_recorded_response(
    request: ProposalRequest,
    context: DiscoveryContext,
    raw_response: bytes,
) -> tuple[ProposedPattern, ...]:
    """Parse exact recorded JSON into inert finite-DSL proposals."""

    if request.proposer_mode != "RECORDED_RESPONSE_V1":
        raise UnsafeRecordedResponseError("recorded response requires recorded-response request mode")
    if not isinstance(raw_response, bytes) or not raw_response:
        raise UnsafeRecordedResponseError("recorded response must be non-empty bytes")
    if len(raw_response) > request.max_response_bytes:
        raise UnsafeRecordedResponseError("recorded response exceeds its precommitted byte budget")
    try:
        document = json.loads(
            raw_response.decode("utf-8"),
            object_pairs_hook=_object_without_duplicate_keys,
            parse_float=_reject_float,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, _DuplicateJsonKey) as error:
        raise UnsafeRecordedResponseError("recorded response is not strict unique-key UTF-8 JSON") from error
    if not isinstance(document, dict):
        raise UnsafeRecordedResponseError("recorded response must be one JSON object")
    try:
        _exact_keys(
            document,
            {"artifact_schema", "context_sha256", "proposals", "request_sha256"},
            label="recorded response",
        )
        if (
            document["artifact_schema"] != RECORDED_RESPONSE_SCHEMA
            or document["request_sha256"] != request.sha256
            or document["context_sha256"] != context.sha256
            or not isinstance(document["proposals"], list)
            or len(document["proposals"]) != request.proposal_budget
        ):
            raise UnsafeRecordedResponseError("recorded response lineage or exact budget differs")
        patterns: list[ProposedPattern] = []
        for index, raw in enumerate(document["proposals"]):
            if not isinstance(raw, dict):
                raise UnsafeRecordedResponseError(f"recorded proposal {index} must be an object")
            _exact_keys(
                raw,
                {"direction", "family", "rationale_code", "rule"},
                label=f"recorded proposal {index}",
            )
            pattern = ProposedPattern(
                family=raw["family"],
                direction=raw["direction"],
                rationale_code=raw["rationale_code"],
                rule=AndRule.from_dict(raw["rule"]),
            )
            if len(pattern.rule.predicates) > request.max_predicates_per_rule:
                raise UnsafeRecordedResponseError("recorded rule exceeds its predicate budget")
            patterns.append(pattern)
    except PatternDiscoveryError as error:
        if isinstance(error, UnsafeRecordedResponseError):
            raise
        raise UnsafeRecordedResponseError(str(error)) from error
    if len({item.sha256 for item in patterns}) != len(patterns):
        raise UnsafeRecordedResponseError("recorded response contains duplicate canonical proposals")
    return tuple(sorted(patterns, key=lambda item: item.sha256))


def replay_recorded_response(
    request: ProposalRequest,
    context: DiscoveryContext,
    raw_response: bytes,
) -> ProposalBatch:
    """Replay saved bytes; deterministic replay never calls a model again."""

    patterns = parse_recorded_response(request, context, raw_response)
    return _select_proposals(
        request,
        context,
        patterns,
        recorded_response_sha256=hashlib.sha256(raw_response).hexdigest(),
    )


def recorded_response_for_patterns(
    request: ProposalRequest,
    context: DiscoveryContext,
    patterns: Iterable[ProposedPattern],
) -> bytes:
    """Serialize inert proposals into the exact recorded-provider response schema."""

    if request.proposer_mode != "RECORDED_RESPONSE_V1":
        raise PatternDiscoveryError("recorded response serialization requires recorded mode")
    values = tuple(patterns)
    if len(values) != request.proposal_budget or not all(
        isinstance(item, ProposedPattern) for item in values
    ):
        raise PatternDiscoveryError("recorded patterns must spend the exact proposal budget")
    ordered = tuple(sorted(values, key=lambda item: item.sha256))
    if len({item.sha256 for item in ordered}) != len(ordered):
        raise PatternDiscoveryError("recorded patterns contain duplicate identities")
    return canonical_json_bytes(
        {
            "artifact_schema": RECORDED_RESPONSE_SCHEMA,
            "context_sha256": context.sha256,
            "proposals": [item.as_dict() for item in ordered],
            "request_sha256": request.sha256,
        }
    )


@dataclass(frozen=True, slots=True)
class LedgerEvent:
    sequence: int
    predecessor_sha256: str | None
    event_type: str
    request_sha256: str
    recorded_at_utc: str
    payload: Mapping[str, object]

    def __post_init__(self) -> None:
        _integer(self.sequence, label="ledger sequence", minimum=1, maximum=99_999_999)
        if self.sequence == 1:
            if self.predecessor_sha256 is not None:
                raise ProposalLedgerError("first ledger event cannot have a predecessor")
        else:
            _required_sha256(self.predecessor_sha256, label="predecessor_sha256")
        if self.event_type not in {
            "PRECOMMITTED",
            "CONTEXT_PUBLISHED",
            "RESPONSE_RECORDED",
            "COMPLETED",
            "FAILED",
        }:
            raise ProposalLedgerError("ledger event type differs")
        _required_sha256(self.request_sha256, label="request_sha256")
        _utc_timestamp(self.recorded_at_utc, label="recorded_at_utc")
        if not isinstance(self.payload, Mapping):
            raise ProposalLedgerError("ledger event payload must be an object")
        try:
            canonical_json_bytes(self.payload)
        except (TypeError, ValueError) as error:
            raise ProposalLedgerError("ledger event payload is not canonical JSON") from error

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": LEDGER_EVENT_SCHEMA,
            "event_type": self.event_type,
            "payload": dict(self.payload),
            "predecessor_sha256": self.predecessor_sha256,
            "recorded_at_utc": self.recorded_at_utc,
            "request_sha256": self.request_sha256,
            "sequence": self.sequence,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


class ProposalLedger:
    """A local append-only directory of canonical, predecessor-hashed events."""

    def __init__(self, root: str | Path) -> None:
        self.root = _safe_root(root, create=True)
        self.events_root = _safe_root(self.root / "events", create=True)

    def verify(self) -> tuple[LedgerEvent, ...]:
        by_sequence: dict[int, Path] = {}
        for path in self.events_root.iterdir():
            if path.is_symlink() or not path.is_file():
                raise ProposalLedgerError("proposal ledger contains a non-file event entry")
            match = _LEDGER_NAME.fullmatch(path.name)
            if match is None:
                raise ProposalLedgerError("proposal ledger contains an unexpected filename")
            sequence = int(match.group(1))
            if sequence in by_sequence:
                raise ProposalLedgerError("proposal ledger contains a duplicate sequence")
            by_sequence[sequence] = path
        events: list[LedgerEvent] = []
        predecessor: str | None = None
        states: dict[str, str] = {}
        for expected_sequence, sequence in enumerate(sorted(by_sequence), start=1):
            if sequence != expected_sequence:
                raise ProposalLedgerError("proposal ledger sequence is not contiguous")
            path = by_sequence[sequence]
            if path.stat().st_mode & _WRITE_BITS:
                raise ProposalLedgerError("proposal ledger event must be read-only")
            payload = path.read_bytes()
            try:
                document = json.loads(
                    payload.decode("utf-8"), object_pairs_hook=_object_without_duplicate_keys
                )
            except (UnicodeDecodeError, json.JSONDecodeError, _DuplicateJsonKey) as error:
                raise ProposalLedgerError("proposal ledger event is invalid JSON") from error
            if not isinstance(document, dict) or canonical_json_bytes(document) != payload:
                raise ProposalLedgerError("proposal ledger event is not canonical JSON")
            _exact_keys(
                document,
                {
                    "artifact_schema",
                    "event_type",
                    "payload",
                    "predecessor_sha256",
                    "recorded_at_utc",
                    "request_sha256",
                    "sequence",
                },
                label="ledger event",
            )
            if document["artifact_schema"] != LEDGER_EVENT_SCHEMA:
                raise ProposalLedgerError("proposal ledger event schema differs")
            event = LedgerEvent(
                sequence=document["sequence"],
                predecessor_sha256=document["predecessor_sha256"],
                event_type=document["event_type"],
                request_sha256=document["request_sha256"],
                recorded_at_utc=document["recorded_at_utc"],
                payload=document["payload"],
            )
            if event.predecessor_sha256 != predecessor:
                raise ProposalLedgerError("proposal ledger predecessor hash chain differs")
            state = states.get(event.request_sha256)
            if event.event_type == "PRECOMMITTED":
                if state is not None:
                    raise ProposalLedgerError("proposal request was precommitted more than once")
                states[event.request_sha256] = "PRECOMMITTED"
            elif event.event_type == "CONTEXT_PUBLISHED":
                if state != "PRECOMMITTED":
                    raise ProposalLedgerError("context was not preceded by PRECOMMITTED")
                states[event.request_sha256] = "CONTEXT_PUBLISHED"
            elif event.event_type == "RESPONSE_RECORDED":
                if state != "CONTEXT_PUBLISHED":
                    raise ProposalLedgerError("response was not preceded by a published context")
                states[event.request_sha256] = "RESPONSE_RECORDED"
            elif event.event_type == "COMPLETED":
                if state not in {"CONTEXT_PUBLISHED", "RESPONSE_RECORDED"}:
                    raise ProposalLedgerError("completion lacks its precommitted context lifecycle")
                states[event.request_sha256] = "COMPLETED"
            else:
                if state not in {"PRECOMMITTED", "CONTEXT_PUBLISHED", "RESPONSE_RECORDED"}:
                    raise ProposalLedgerError("failure lacks a live precommitted lifecycle")
                states[event.request_sha256] = "FAILED"
            events.append(event)
            predecessor = event.sha256
        return tuple(events)

    def _append(
        self,
        event_type: str,
        request_sha256: str,
        payload: Mapping[str, object],
    ) -> LedgerEvent:
        events = self.verify()
        event = LedgerEvent(
            sequence=len(events) + 1,
            predecessor_sha256=events[-1].sha256 if events else None,
            event_type=event_type,
            request_sha256=request_sha256,
            recorded_at_utc=_now_utc(),
            payload=payload,
        )
        # Validate lifecycle before publication by reconstructing the same state rules.
        prior_for_request = [item for item in events if item.request_sha256 == request_sha256]
        prior_type = prior_for_request[-1].event_type if prior_for_request else None
        required_prior = {
            "PRECOMMITTED": {None},
            "CONTEXT_PUBLISHED": {"PRECOMMITTED"},
            "RESPONSE_RECORDED": {"CONTEXT_PUBLISHED"},
            "COMPLETED": {"CONTEXT_PUBLISHED", "RESPONSE_RECORDED"},
            "FAILED": {"PRECOMMITTED", "CONTEXT_PUBLISHED", "RESPONSE_RECORDED"},
        }
        if prior_type not in required_prior[event_type]:
            raise ProposalLedgerError("proposal ledger lifecycle transition is invalid")
        leaf = f"event-{event.sequence:08d}.json"
        destination = self.events_root / leaf
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(destination, flags, 0o444)
        except FileExistsError as error:
            raise ProposalLedgerError("concurrent proposal ledger append conflict") from error
        try:
            _write_all(fd, canonical_json_bytes(event.as_dict()))
            os.fsync(fd)
            os.fchmod(fd, 0o444)
        finally:
            os.close(fd)
        directory_fd = os.open(self.events_root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        verified = self.verify()
        if verified[-1].sha256 != event.sha256:
            raise ProposalLedgerError("proposal ledger append did not replay exactly")
        return event

    def append_precommit(
        self, request: ProposalRequest, request_artifact: ArtifactIdentity
    ) -> LedgerEvent:
        if request_artifact.artifact_type != "AI_PATTERN_PROPOSAL_REQUEST":
            raise ProposalLedgerError("precommit requires a proposal request artifact")
        if request_artifact.content_sha256 != request.sha256:
            raise ProposalLedgerError("precommit request artifact identity differs")
        return self._append(
            "PRECOMMITTED",
            request.sha256,
            {"request_artifact": request_artifact.as_dict()},
        )

    def append_context(
        self,
        request: ProposalRequest,
        context: DiscoveryContext,
        context_artifact: ArtifactIdentity,
    ) -> LedgerEvent:
        if (
            context.request_sha256 != request.sha256
            or context_artifact.artifact_type != "AI_PATTERN_DISCOVERY_CONTEXT"
            or context_artifact.content_sha256 != context.sha256
        ):
            raise ProposalLedgerError("published context identity differs")
        return self._append(
            "CONTEXT_PUBLISHED",
            request.sha256,
            {"context_artifact": context_artifact.as_dict()},
        )

    def append_response(
        self, request: ProposalRequest, response_artifact: ArtifactIdentity
    ) -> LedgerEvent:
        if response_artifact.artifact_type != "AI_PATTERN_RECORDED_RESPONSE":
            raise ProposalLedgerError("recorded response artifact type differs")
        return self._append(
            "RESPONSE_RECORDED",
            request.sha256,
            {"response_artifact": response_artifact.as_dict()},
        )

    def append_completed(
        self,
        request: ProposalRequest,
        batch_artifact: ArtifactIdentity,
        report_artifact: ArtifactIdentity,
    ) -> LedgerEvent:
        return self._append(
            "COMPLETED",
            request.sha256,
            {
                "batch_artifact": batch_artifact.as_dict(),
                "report_artifact": report_artifact.as_dict(),
                "status": FINAL_STATUS,
            },
        )

    def append_failed(self, request: ProposalRequest, *, failure_code: str) -> LedgerEvent:
        return self._append(
            "FAILED",
            request.sha256,
            {"failure_code": _required_identifier(failure_code, label="failure_code")},
        )


@dataclass(frozen=True, slots=True)
class ProposalRunStart:
    request: ProposalRequest
    request_artifact: ArtifactIdentity
    context: DiscoveryContext
    context_artifact: ArtifactIdentity


@dataclass(frozen=True, slots=True)
class ProposalRunReport:
    request_sha256: str
    request_artifact: ArtifactIdentity
    context_sha256: str
    context_artifact: ArtifactIdentity
    batch_sha256: str
    batch_artifact: ArtifactIdentity
    recorded_response_artifact: ArtifactIdentity | None

    def __post_init__(self) -> None:
        for name in ("request_sha256", "context_sha256", "batch_sha256"):
            _required_sha256(getattr(self, name), label=name)
        if self.request_artifact.content_sha256 != self.request_sha256:
            raise PatternDiscoveryError("report request artifact differs")
        if self.context_artifact.content_sha256 != self.context_sha256:
            raise PatternDiscoveryError("report context artifact differs")
        if self.batch_artifact.content_sha256 != self.batch_sha256:
            raise PatternDiscoveryError("report batch artifact differs")

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": REPORT_SCHEMA,
            "authority": AUTHORITY,
            "batch_artifact": self.batch_artifact.as_dict(),
            "batch_sha256": self.batch_sha256,
            "context_artifact": self.context_artifact.as_dict(),
            "context_sha256": self.context_sha256,
            "database_mutated": False,
            "limitations": list(LIMITATIONS),
            "m0b_epoch_registered": False,
            "performance_evaluated": False,
            "recorded_response_artifact": (
                None
                if self.recorded_response_artifact is None
                else self.recorded_response_artifact.as_dict()
            ),
            "request_artifact": self.request_artifact.as_dict(),
            "request_sha256": self.request_sha256,
            "sealed_holdout_untouched": True,
            "status": FINAL_STATUS,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


@dataclass(frozen=True, slots=True)
class ProposalRunResult:
    start: ProposalRunStart
    batch: ProposalBatch
    batch_artifact: ArtifactIdentity
    report: ProposalRunReport
    report_artifact: ArtifactIdentity
    recorded_response_artifact: ArtifactIdentity | None


def begin_pattern_discovery(
    *,
    ledger_root: str | Path,
    artifact_root: str | Path,
    request: ProposalRequest,
    feature_rows: Iterable[Mapping[str, object]],
) -> ProposalRunStart:
    """Precommit first, then consume feature rows and publish the context."""

    ledger = ProposalLedger(ledger_root)
    request_artifact = publish_canonical_artifact(
        artifact_root,
        artifact_type="AI_PATTERN_PROPOSAL_REQUEST",
        filename_prefix="proposal-request",
        document=request.as_dict(),
    )
    ledger.append_precommit(request, request_artifact)
    try:
        context = build_discovery_context(request, feature_rows)
        context_artifact = publish_canonical_artifact(
            artifact_root,
            artifact_type="AI_PATTERN_DISCOVERY_CONTEXT",
            filename_prefix="discovery-context",
            document=context.as_dict(),
        )
        ledger.append_context(request, context, context_artifact)
    except Exception as error:
        ledger.append_failed(request, failure_code=type(error).__name__)
        raise
    return ProposalRunStart(request, request_artifact, context, context_artifact)


def begin_pattern_discovery_with_context(
    *,
    ledger_root: str | Path,
    artifact_root: str | Path,
    request: ProposalRequest,
    context: DiscoveryContext,
) -> ProposalRunStart:
    """Precommit, then publish an already verified/projected Discovery context.

    This is the integration hook for ``ai_discovery_context``.  Callers must
    first reopen and verify its content-addressed source artifact, then use
    :func:`context_from_ai_discovery_document`.  The request is durably appended
    before this function inspects or publishes any context bytes.
    """

    ledger = ProposalLedger(ledger_root)
    request_artifact = publish_canonical_artifact(
        artifact_root,
        artifact_type="AI_PATTERN_PROPOSAL_REQUEST",
        filename_prefix="proposal-request",
        document=request.as_dict(),
    )
    ledger.append_precommit(request, request_artifact)
    try:
        if not isinstance(context, DiscoveryContext) or context.request_sha256 != request.sha256:
            raise PatternDiscoveryError("verified Discovery context belongs to another request")
        if (
            context.source_feature_sha256 != request.source_feature_sha256
            or context.source_feature_version != request.source_feature_version
            or context.discovery_split_sha256 != request.discovery_split_sha256
            or context.source_interval_start != request.source_interval_start
            or context.source_interval_end != request.source_interval_end
            or context.source_row_count > request.max_source_rows
            or len(context.bins) > request.max_context_bins
        ):
            raise PatternDiscoveryError("verified Discovery context exceeds or differs from precommit")
        context_artifact = publish_canonical_artifact(
            artifact_root,
            artifact_type="AI_PATTERN_DISCOVERY_CONTEXT",
            filename_prefix="discovery-context",
            document=context.as_dict(),
        )
        ledger.append_context(request, context, context_artifact)
    except Exception as error:
        ledger.append_failed(request, failure_code=type(error).__name__)
        raise
    return ProposalRunStart(request, request_artifact, context, context_artifact)


def _verify_start(
    ledger: ProposalLedger,
    artifact_root: str | Path,
    start: ProposalRunStart,
) -> None:
    verify_immutable_artifact(
        artifact_root,
        start.request_artifact,
        expected_bytes=canonical_json_bytes(start.request.as_dict()),
    )
    verify_immutable_artifact(
        artifact_root,
        start.context_artifact,
        expected_bytes=canonical_json_bytes(start.context.as_dict()),
    )
    relevant = [
        event for event in ledger.verify() if event.request_sha256 == start.request.sha256
    ]
    if [event.event_type for event in relevant] != ["PRECOMMITTED", "CONTEXT_PUBLISHED"]:
        raise ProposalLedgerError("proposal run start lifecycle differs")


def _complete(
    *,
    ledger: ProposalLedger,
    artifact_root: str | Path,
    start: ProposalRunStart,
    batch: ProposalBatch,
    recorded_response_artifact: ArtifactIdentity | None,
) -> ProposalRunResult:
    batch_artifact = publish_canonical_artifact(
        artifact_root,
        artifact_type="AI_PATTERN_PROPOSAL_BATCH",
        filename_prefix="proposal-batch",
        document=batch.as_dict(),
    )
    report = ProposalRunReport(
        request_sha256=start.request.sha256,
        request_artifact=start.request_artifact,
        context_sha256=start.context.sha256,
        context_artifact=start.context_artifact,
        batch_sha256=batch.sha256,
        batch_artifact=batch_artifact,
        recorded_response_artifact=recorded_response_artifact,
    )
    report_artifact = publish_canonical_artifact(
        artifact_root,
        artifact_type="AI_PATTERN_DISCOVERY_REPORT",
        filename_prefix="proposal-report",
        document=report.as_dict(),
    )
    ledger.append_completed(start.request, batch_artifact, report_artifact)
    return ProposalRunResult(
        start=start,
        batch=batch,
        batch_artifact=batch_artifact,
        report=report,
        report_artifact=report_artifact,
        recorded_response_artifact=recorded_response_artifact,
    )


def complete_deterministic_pattern_discovery(
    *,
    ledger_root: str | Path,
    artifact_root: str | Path,
    start: ProposalRunStart,
) -> ProposalRunResult:
    """Complete a precommitted context with the local outcome-blind proposer."""

    ledger = ProposalLedger(ledger_root)
    _verify_start(ledger, artifact_root, start)
    try:
        batch = propose_deterministically(start.request, start.context)
        return _complete(
            ledger=ledger,
            artifact_root=artifact_root,
            start=start,
            batch=batch,
            recorded_response_artifact=None,
        )
    except Exception as error:
        ledger.append_failed(start.request, failure_code=type(error).__name__)
        raise


def complete_recorded_pattern_discovery(
    *,
    ledger_root: str | Path,
    artifact_root: str | Path,
    start: ProposalRunStart,
    raw_response: bytes,
) -> ProposalRunResult:
    """Record exact model bytes, then strictly parse and replay them once."""

    ledger = ProposalLedger(ledger_root)
    _verify_start(ledger, artifact_root, start)
    try:
        response_artifact = publish_recorded_response_artifact(
            artifact_root,
            raw_response,
            maximum_byte_size=start.request.max_response_bytes,
        )
        ledger.append_response(start.request, response_artifact)
        batch = replay_recorded_response(start.request, start.context, raw_response)
        return _complete(
            ledger=ledger,
            artifact_root=artifact_root,
            start=start,
            batch=batch,
            recorded_response_artifact=response_artifact,
        )
    except Exception as error:
        current = [
            event
            for event in ledger.verify()
            if event.request_sha256 == start.request.sha256
        ]
        if current and current[-1].event_type in {
            "PRECOMMITTED",
            "CONTEXT_PUBLISHED",
            "RESPONSE_RECORDED",
        }:
            ledger.append_failed(start.request, failure_code=type(error).__name__)
        raise


def run_deterministic_pattern_discovery(
    *,
    ledger_root: str | Path,
    artifact_root: str | Path,
    request: ProposalRequest,
    feature_rows: Iterable[Mapping[str, object]],
) -> ProposalRunResult:
    """Convenience API preserving precommit-before-context ordering."""

    start = begin_pattern_discovery(
        ledger_root=ledger_root,
        artifact_root=artifact_root,
        request=request,
        feature_rows=feature_rows,
    )
    return complete_deterministic_pattern_discovery(
        ledger_root=ledger_root,
        artifact_root=artifact_root,
        start=start,
    )


__all__ = [
    "AI_DISCOVERY_CONTEXT_SCHEMA",
    "ALLOWED_FAMILIES",
    "ALLOWED_FEATURES",
    "ALLOWED_PREDICATES",
    "ALLOWED_RATIONALE_CODES",
    "AUTHORITY",
    "DETERMINISTIC_PROMPT_SHA256",
    "FINAL_STATUS",
    "LIMITATIONS",
    "RECORDED_RESPONSE_CONTRACT",
    "RECORDED_RESPONSE_CONTRACT_SHA256",
    "RECORDED_RESPONSE_SCHEMA",
    "RULE_SCHEMA",
    "RULE_SPACE_DOCUMENT",
    "RULE_SPACE_SHA256",
    "AndRule",
    "ArtifactIdentity",
    "DiscoveryContext",
    "DiscoveryVectorBin",
    "ImmutableArtifactError",
    "LedgerEvent",
    "PatternDiscoveryError",
    "PatternProposal",
    "ProposalBatch",
    "ProposalLedger",
    "ProposalLedgerError",
    "ProposalRequest",
    "ProposalRunReport",
    "ProposalRunResult",
    "ProposalRunStart",
    "ProposedPattern",
    "RulePredicate",
    "UnsafeRecordedResponseError",
    "begin_pattern_discovery",
    "begin_pattern_discovery_with_context",
    "build_discovery_context",
    "complete_deterministic_pattern_discovery",
    "complete_recorded_pattern_discovery",
    "context_from_ai_discovery_document",
    "parse_recorded_response",
    "propose_deterministically",
    "publish_canonical_artifact",
    "publish_recorded_response_artifact",
    "recorded_response_for_patterns",
    "replay_recorded_response",
    "run_deterministic_pattern_discovery",
    "verify_immutable_artifact",
]
