"""Immutable, canonical identity for one governed research run."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import PurePath
from types import MappingProxyType
from typing import Final, cast

RUN_SPEC_SCHEMA: Final = "systematic_fx.research_run_spec.v2"
RUN_SPEC_SCHEMA_VERSION: Final = 2

_SHA256 = re.compile(r"[0-9a-f]{64}")
_GIT_OBJECT_ID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")
_DIRECTIONS = frozenset({"LONG", "SHORT", "BOTH"})
_RUN_KINDS = frozenset(
    {
        "FEATURE_BUILD",
        "OUTCOME_BUILD",
        "AI_SLICE",
        "QUERY",
        "SCREEN",
        "BARRIER_SURFACE",
        "MODEL_FIT",
        "BACKTEST",
        "WALK_FORWARD",
        "HOLDOUT",
        "STRESS",
        "VALIDATION",
    }
)
_CAMPAIGN_LEVEL_RUN_KINDS = frozenset(
    {
        "FEATURE_BUILD",
        "OUTCOME_BUILD",
        "AI_SLICE",
        "QUERY",
        "VALIDATION",
    }
)
_UINT64_MAX = 2**64 - 1

type CanonicalScalar = str | int | bool | None
type CanonicalValue = (
    CanonicalScalar | tuple["CanonicalValue", ...] | Mapping[str, "CanonicalValue"]
)


class RunSpecError(ValueError):
    """A run specification is incomplete or cannot be represented canonically."""


def _required_string(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RunSpecError(f"{label} must be a non-empty string")
    if value != value.strip():
        raise RunSpecError(f"{label} must not have leading or trailing whitespace")
    return value


def _required_sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise RunSpecError(f"{label} must be a lowercase 64-character SHA-256")
    return value


def _freeze_canonical(
    value: object,
    *,
    label: str,
    ancestors: frozenset[int] = frozenset(),
) -> CanonicalValue:
    """Validate and detach one strict JSON value into immutable containers."""

    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        raise RunSpecError(
            f"{label} cannot contain binary floats, including NaN or Infinity; "
            "use an exact decimal string"
        )
    if isinstance(value, PurePath):
        raise RunSpecError(f"{label} cannot contain Path values; use a canonical URI string")
    if isinstance(value, (datetime, date)):
        raise RunSpecError(
            f"{label} cannot contain date or datetime values; use an explicit UTC string"
        )
    if isinstance(value, (set, frozenset)):
        raise RunSpecError(f"{label} cannot contain unordered set values; use an ordered array")

    if isinstance(value, Mapping):
        identity = id(value)
        if identity in ancestors:
            raise RunSpecError(f"{label} cannot contain cyclic containers")
        child_ancestors = ancestors | {identity}
        frozen: dict[str, CanonicalValue] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise RunSpecError(f"{label} mappings require non-empty string keys")
            frozen[key] = _freeze_canonical(
                item,
                label=f"{label}.{key}",
                ancestors=child_ancestors,
            )
        return MappingProxyType(frozen)

    if isinstance(value, (list, tuple)):
        identity = id(value)
        if identity in ancestors:
            raise RunSpecError(f"{label} cannot contain cyclic containers")
        child_ancestors = ancestors | {identity}
        return tuple(
            _freeze_canonical(
                item,
                label=f"{label}[{index}]",
                ancestors=child_ancestors,
            )
            for index, item in enumerate(value)
        )

    raise RunSpecError(f"{label} contains unsupported canonical JSON type {type(value).__name__}")


def _required_mapping(
    value: object,
    *,
    label: str,
    allow_empty: bool = False,
) -> Mapping[str, CanonicalValue]:
    if not isinstance(value, Mapping):
        raise RunSpecError(f"{label} must be a mapping")
    frozen = cast(
        Mapping[str, CanonicalValue],
        _freeze_canonical(value, label=label),
    )
    if not frozen and not allow_empty:
        raise RunSpecError(f"{label} must not be empty")
    return frozen


def _json_value(value: CanonicalValue) -> object:
    if isinstance(value, Mapping):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    return value


def canonical_json_bytes(value: Mapping[str, CanonicalValue]) -> bytes:
    """Return strict canonical JSON with sorted keys and no insignificant space."""

    return json.dumps(
        _json_value(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class RunSpec:
    """Complete immutable inputs whose fingerprint identifies one research run."""

    campaign_id: str
    experiment_id: str | None
    run_kind: str
    engine_version: str
    source_manifest_hashes: Mapping[str, str]
    eligible_calendar_version: str
    eligible_calendar_sha256: str
    split_version: str
    split_sha256: str
    feature_version: str
    feature_sha256: str
    outcome_version: str
    outcome_sha256: str
    cost_version: str
    cost_sha256: str
    execution_version: str
    execution_sha256: str
    code_commit: str
    code_snapshot_sha256: str
    dependency_lock_sha256: str
    runtime_environment: Mapping[str, CanonicalValue]
    random_seed: int
    direction: str
    signal_policy: Mapping[str, CanonicalValue]
    entry_policy: Mapping[str, CanonicalValue]
    barrier_policy: Mapping[str, CanonicalValue]
    terminal_policy: Mapping[str, CanonicalValue]
    parameters: Mapping[str, CanonicalValue]

    def __post_init__(self) -> None:
        for field_name in (
            "campaign_id",
            "engine_version",
            "eligible_calendar_version",
            "split_version",
            "feature_version",
            "outcome_version",
            "cost_version",
            "execution_version",
            "code_commit",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_string(getattr(self, field_name), label=field_name),
            )

        if self.run_kind not in _RUN_KINDS:
            raise RunSpecError(f"run_kind must be one of {sorted(_RUN_KINDS)}")
        if self.experiment_id is None:
            if self.run_kind not in _CAMPAIGN_LEVEL_RUN_KINDS:
                raise RunSpecError(
                    "experiment_id may be null only for campaign-level run kinds "
                    f"{sorted(_CAMPAIGN_LEVEL_RUN_KINDS)}"
                )
        else:
            object.__setattr__(
                self,
                "experiment_id",
                _required_string(self.experiment_id, label="experiment_id"),
            )
        if _GIT_OBJECT_ID.fullmatch(self.code_commit) is None:
            raise RunSpecError("code_commit must be a full lowercase Git object ID")

        for field_name in (
            "eligible_calendar_sha256",
            "split_sha256",
            "feature_sha256",
            "outcome_sha256",
            "cost_sha256",
            "execution_sha256",
            "code_snapshot_sha256",
            "dependency_lock_sha256",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_sha256(getattr(self, field_name), label=field_name),
            )

        if not isinstance(self.source_manifest_hashes, Mapping) or not self.source_manifest_hashes:
            raise RunSpecError("source_manifest_hashes must be a non-empty mapping")
        source_hashes: dict[str, str] = {}
        for name, digest in self.source_manifest_hashes.items():
            source_name = _required_string(name, label="source_manifest_hashes key")
            source_hashes[source_name] = _required_sha256(
                digest,
                label=f"source_manifest_hashes.{source_name}",
            )
        object.__setattr__(self, "source_manifest_hashes", MappingProxyType(source_hashes))

        if isinstance(self.random_seed, bool) or not isinstance(self.random_seed, int):
            raise RunSpecError("random_seed must be an unsigned 64-bit integer")
        if not 0 <= self.random_seed <= _UINT64_MAX:
            raise RunSpecError("random_seed must be between 0 and 2^64 - 1")
        if self.direction not in _DIRECTIONS:
            raise RunSpecError(f"direction must be one of {sorted(_DIRECTIONS)}")

        for field_name in (
            "signal_policy",
            "entry_policy",
            "barrier_policy",
            "terminal_policy",
            "runtime_environment",
            "parameters",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_mapping(getattr(self, field_name), label=field_name),
            )

    def payload(self) -> dict[str, CanonicalValue]:
        """Return a detached canonical payload suitable for persistence."""

        return {
            "artifact_schema": RUN_SPEC_SCHEMA,
            "schema_version": RUN_SPEC_SCHEMA_VERSION,
            "campaign_id": self.campaign_id,
            "experiment_id": self.experiment_id,
            "run_kind": self.run_kind,
            "engine_version": self.engine_version,
            "source_manifest_hashes": dict(self.source_manifest_hashes),
            "eligible_calendar": {
                "version": self.eligible_calendar_version,
                "sha256": self.eligible_calendar_sha256,
            },
            "split": {"version": self.split_version, "sha256": self.split_sha256},
            "feature": {"version": self.feature_version, "sha256": self.feature_sha256},
            "outcome": {"version": self.outcome_version, "sha256": self.outcome_sha256},
            "cost": {"version": self.cost_version, "sha256": self.cost_sha256},
            "execution": {
                "version": self.execution_version,
                "sha256": self.execution_sha256,
            },
            "code_commit": self.code_commit,
            "code_snapshot_sha256": self.code_snapshot_sha256,
            "dependency_lock_sha256": self.dependency_lock_sha256,
            "runtime_environment": self.runtime_environment,
            "random_seed": self.random_seed,
            "direction": self.direction,
            "signal_policy": self.signal_policy,
            "entry_policy": self.entry_policy,
            "barrier_policy": self.barrier_policy,
            "terminal_policy": self.terminal_policy,
            "parameters": self.parameters,
        }

    def canonical_json(self) -> bytes:
        """Serialize every run input to deterministic UTF-8 JSON bytes."""

        return canonical_json_bytes(self.payload())

    @property
    def fingerprint(self) -> str:
        """Return the SHA-256 identity of :meth:`canonical_json`."""

        return hashlib.sha256(self.canonical_json()).hexdigest()
