"""Append-only orchestration for the governed Batch 3 performance evaluation.

This module owns stage authorization and filesystem access ordering.  The
statistical engine remains pure and is imported from
``scripts.ai_pattern_holdout_engine`` by the default service adapter.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from itertools import groupby
from pathlib import Path
from typing import Final

from scripts.ai_pattern_holdout_config import (
    AI_PATTERN_BATCH3_BATCH_RELATIVE_PATH,
    AI_PATTERN_BATCH3_REPORT_RELATIVE_PATH,
    AI_PATTERN_BATCH3_REQUEST_RELATIVE_PATH,
    AI_PATTERN_HOLDOUT_AUTHORITY,
    DATASET_MANIFEST_RELATIVE_PATH,
    EXPECTED_BATCH3_GOVERNED_REQUEST_SHA256,
    EXPECTED_BATCH3_PROPOSAL_BATCH_SHA256,
    EXPECTED_BATCH3_PROPOSAL_REPORT_SHA256,
    EXPECTED_DATASET_HANDOFF_SHA256,
    EXPECTED_DATASET_MANIFEST_SHA256,
    EXPECTED_SOURCE_MANIFEST_SHA256,
    EXPECTED_SPLIT_PLAN_SHA256,
    FINAL_HOLDOUT_STATUSES,
    AIPatternHoldoutConfig,
    load_ai_pattern_holdout_config,
)
from systematic_fx.features.bars import load_trade_bar_artifact
from systematic_fx.research.ai_pattern_discovery import (
    AUTHORITY as PROPOSAL_AUTHORITY,
)
from systematic_fx.research.ai_pattern_discovery import (
    FINAL_STATUS as PROPOSAL_FINAL_STATUS,
)
from systematic_fx.research.ai_pattern_discovery import (
    AndRule,
    ArtifactIdentity,
    ProposedPattern,
    publish_canonical_artifact,
    verify_immutable_artifact,
)
from systematic_fx.research.ai_pattern_discovery_v2 import (
    DIRECTIONAL_PROPOSAL_BATCH_SCHEMA,
    V2_PROPOSER_MODE,
    V2_SEMANTIC_POLICY_SHA256,
    validate_pattern_semantics_v2,
)
from systematic_fx.research.bar_config import BAR_SOURCE_MANIFEST_SHA256
from systematic_fx.research.bar_pipeline import (
    BarDatasetPartition,
    LoadedBarDatasetManifest,
    load_bar_dataset_manifest,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.validation.bar_splits import BarDateRange, BarSplitPlan, plan_bar_splits

AI_PATTERN_HOLDOUT_RUN_SCHEMA: Final = "systematic_fx.ai_pattern_holdout_run.v1"
AI_PATTERN_HOLDOUT_EVENT_SCHEMA: Final = "systematic_fx.ai_pattern_holdout_event.v1"
DEFAULT_AI_PATTERN_HOLDOUT_ROOT: Final = Path("data/derived/bar_patterns/ai_pattern_holdout_v1")
_WRITE_BITS: Final = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
_EVENT_TYPES: Final = frozenset(
    {
        "PRECOMMITTED",
        "SEARCH_MASKS_FROZEN",
        "SEARCH_COMPLETED",
        "WALK_FORWARD_MASKS_FROZEN",
        "WALK_FORWARD_COMPLETED",
        "WALK_FORWARD_SKIPPED",
        "HOLDOUT_AUTHORIZED",
        "HOLDOUT_MASKS_FROZEN",
        "HOLDOUT_COMPLETED",
        "HOLDOUT_SKIPPED",
        "COMPLETED",
        "FAILED",
    }
)
_NONTERMINAL_EVENT_TYPES: Final = _EVENT_TYPES - {"COMPLETED", "FAILED"}
_EVENT_PAYLOAD_KEYS: Final = {
    "PRECOMMITTED": {"request_artifact"},
    "SEARCH_MASKS_FROZEN": {"candidate_sha256s", "masks_artifact"},
    "SEARCH_COMPLETED": {"classification", "finalist_sha256s", "result_artifact"},
    "WALK_FORWARD_MASKS_FROZEN": {"candidate_sha256s", "masks_artifact"},
    "WALK_FORWARD_COMPLETED": {"classification", "finalist_sha256s", "result_artifact"},
    "WALK_FORWARD_SKIPPED": {"reason", "skip_artifact"},
    "HOLDOUT_AUTHORIZED": {
        "authorization_artifact",
        "finalist_sha256s",
        "holm_family_sha256",
    },
    "HOLDOUT_MASKS_FROZEN": {"candidate_sha256s", "masks_artifact"},
    "HOLDOUT_COMPLETED": {"classification", "final_status", "result_artifact"},
    "HOLDOUT_SKIPPED": {"reason", "skip_artifact"},
    "COMPLETED": {"final_status", "report_artifact"},
    "FAILED": {"failure_code"},
}
_EVENT_ARTIFACT_ROLES: Final = {
    "PRECOMMITTED": ("request_artifact", "AI_PATTERN_HOLDOUT_REQUEST"),
    "SEARCH_MASKS_FROZEN": ("masks_artifact", "AI_PATTERN_SEARCH_MASKS"),
    "SEARCH_COMPLETED": ("result_artifact", "AI_PATTERN_SEARCH_RESULT"),
    "WALK_FORWARD_MASKS_FROZEN": (
        "masks_artifact",
        "AI_PATTERN_WALK_FORWARD_MASKS",
    ),
    "WALK_FORWARD_COMPLETED": (
        "result_artifact",
        "AI_PATTERN_WALK_FORWARD_RESULT",
    ),
    "WALK_FORWARD_SKIPPED": (
        "skip_artifact",
        "AI_PATTERN_WALK_FORWARD_SKIPPED",
    ),
    "HOLDOUT_AUTHORIZED": (
        "authorization_artifact",
        "AI_PATTERN_HOLDOUT_AUTHORIZATION",
    ),
    "HOLDOUT_MASKS_FROZEN": ("masks_artifact", "AI_PATTERN_HOLDOUT_MASKS"),
    "HOLDOUT_COMPLETED": ("result_artifact", "AI_PATTERN_HOLDOUT_RESULT"),
    "HOLDOUT_SKIPPED": ("skip_artifact", "AI_PATTERN_HOLDOUT_SKIPPED"),
    "COMPLETED": ("report_artifact", "AI_PATTERN_HOLDOUT_REPORT"),
}


class AIPatternHoldoutRunError(RuntimeError):
    """The performance evaluation violated its frozen lifecycle or evidence."""


@dataclass(frozen=True, slots=True)
class FrozenHoldoutProposal:
    selection_rank: int
    proposal_sha256: str
    direction: str
    rule: AndRule
    discovery_support_rows: int
    discovery_session_support_count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "direction": self.direction,
            "discovery_session_support_count": self.discovery_session_support_count,
            "discovery_support_rows": self.discovery_support_rows,
            "proposal_sha256": self.proposal_sha256,
            "rule": self.rule.as_dict(),
            "selection_rank": self.selection_rank,
        }


@dataclass(frozen=True, slots=True)
class FrozenHoldoutBatch:
    proposals: tuple[FrozenHoldoutProposal, ...]
    governed_request_sha256: str = EXPECTED_BATCH3_GOVERNED_REQUEST_SHA256
    proposal_batch_sha256: str = EXPECTED_BATCH3_PROPOSAL_BATCH_SHA256
    proposal_report_sha256: str = EXPECTED_BATCH3_PROPOSAL_REPORT_SHA256

    def __post_init__(self) -> None:
        if tuple(item.selection_rank for item in self.proposals) != tuple(range(1, 13)):
            raise AIPatternHoldoutRunError("Batch 3 proposal ranks are not exactly 1 through 12")
        identities = tuple(item.proposal_sha256 for item in self.proposals)
        if len(set(identities)) != 12:
            raise AIPatternHoldoutRunError("Batch 3 proposal identities are not unique")

    def as_dict(self) -> dict[str, object]:
        return {
            "governed_request_sha256": self.governed_request_sha256,
            "proposal_batch_sha256": self.proposal_batch_sha256,
            "proposal_report_sha256": self.proposal_report_sha256,
            "proposals": [item.as_dict() for item in self.proposals],
        }


@dataclass(frozen=True, slots=True)
class HoldoutStagePlan:
    stage_key: str
    fold_number: int | None
    decision_dates: tuple[date, ...]
    partitions: tuple[BarDatasetPartition, ...]
    reporting_groups: tuple[tuple[str, tuple[date, ...]], ...] = ()

    def __post_init__(self) -> None:
        partition_dates = tuple(item.source_date for item in self.partitions)
        if (
            not self.stage_key
            or not self.decision_dates
            or not self.partitions
            or partition_dates != tuple(sorted(set(partition_dates)))
            or self.decision_dates != tuple(sorted(set(self.decision_dates)))
            or not set(self.decision_dates).issubset(partition_dates)
        ):
            raise AIPatternHoldoutRunError("evaluation stage plan is incomplete or unordered")

    @property
    def data_dates(self) -> tuple[date, ...]:
        return tuple(item.source_date for item in self.partitions)

    def identity_dict(self) -> dict[str, object]:
        return {
            "data_end_date": self.data_dates[-1].isoformat(),
            "data_start_date": self.data_dates[0].isoformat(),
            "decision_dates": [item.isoformat() for item in self.decision_dates],
            "fold_number": self.fold_number,
            "outcome_span_ids": sorted({item.outcome_span_id for item in self.partitions}),
            "reporting_groups": [
                {"dates": [item.isoformat() for item in dates], "group_key": key}
                for key, dates in self.reporting_groups
            ],
            "stage_key": self.stage_key,
        }


@dataclass(frozen=True, slots=True)
class HoldoutEvaluationInputs:
    batch: FrozenHoldoutBatch
    dataset: LoadedBarDatasetManifest
    split_plan: BarSplitPlan
    search_plan: HoldoutStagePlan
    walk_forward_plans: tuple[HoldoutStagePlan, ...]
    holdout_plan: HoldoutStagePlan


@dataclass(frozen=True, slots=True)
class StageMaskBundle:
    stage_key: str
    proposal_sha256s: tuple[str, ...]
    raw_signal_counts: tuple[tuple[str, int], ...]
    signal_day_counts: tuple[tuple[str, int], ...]
    payload: object

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": "systematic_fx.ai_pattern_stage_masks.v1",
            "payload": _document(self.payload),
            "proposal_sha256s": list(self.proposal_sha256s),
            "raw_signal_counts": [
                {"proposal_sha256": key, "value": value} for key, value in self.raw_signal_counts
            ],
            "signal_day_counts": [
                {"proposal_sha256": key, "value": value} for key, value in self.signal_day_counts
            ],
            "stage_key": self.stage_key,
        }


@dataclass(frozen=True, slots=True)
class HoldoutStageOutcome:
    stage_key: str
    finalist_proposal_sha256s: tuple[str, ...]
    classification: str
    payload: object

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": "systematic_fx.ai_pattern_stage_result.v1",
            "classification": self.classification,
            "finalist_proposal_sha256s": list(self.finalist_proposal_sha256s),
            "payload": _document(self.payload),
            "stage_key": self.stage_key,
        }


@dataclass(frozen=True, slots=True)
class HoldoutRunServices:
    load_inputs: Callable[[Path, AIPatternHoldoutConfig], HoldoutEvaluationInputs]
    freeze_masks: Callable[
        [
            Path,
            AIPatternHoldoutConfig,
            FrozenHoldoutBatch,
            tuple[HoldoutStagePlan, ...],
            tuple[str, ...],
        ],
        StageMaskBundle,
    ]
    evaluate_stage: Callable[
        [
            Path,
            AIPatternHoldoutConfig,
            FrozenHoldoutBatch,
            tuple[HoldoutStagePlan, ...],
            tuple[str, ...],
            StageMaskBundle,
        ],
        HoldoutStageOutcome,
    ]


@dataclass(frozen=True, slots=True)
class HoldoutLedgerEvent:
    sequence: int
    predecessor_sha256: str | None
    event_type: str
    request_sha256: str
    recorded_at_utc: str
    payload: Mapping[str, object]

    def __post_init__(self) -> None:
        if (
            isinstance(self.sequence, bool)
            or not isinstance(self.sequence, int)
            or self.sequence < 1
        ):
            raise AIPatternHoldoutRunError("holdout ledger sequence is invalid")
        if self.event_type not in _EVENT_TYPES:
            raise AIPatternHoldoutRunError("holdout ledger event type differs")
        _sha256(self.request_sha256, label="holdout ledger request_sha256")
        if self.sequence == 1:
            if self.predecessor_sha256 is not None:
                raise AIPatternHoldoutRunError("first holdout event cannot have a predecessor")
        else:
            _sha256(self.predecessor_sha256, label="holdout ledger predecessor_sha256")
        if not isinstance(self.recorded_at_utc, str) or not self.recorded_at_utc.endswith("Z"):
            raise AIPatternHoldoutRunError("holdout ledger timestamp is not explicit UTC")
        if not isinstance(self.payload, Mapping):
            raise AIPatternHoldoutRunError("holdout ledger payload is not an object")
        canonical_json_bytes(self.payload)

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": AI_PATTERN_HOLDOUT_EVENT_SCHEMA,
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


class HoldoutLedger:
    """A canonical, mode-0444, predecessor-hashed evaluation ledger."""

    def __init__(self, root: Path, *, create: bool) -> None:
        self.root = _safe_directory(root, create=create)
        self.events_root = _safe_directory(self.root / "events", create=create)

    def verify(self) -> tuple[HoldoutLedgerEvent, ...]:
        paths: dict[int, Path] = {}
        for path in self.events_root.iterdir():
            if path.is_symlink() or not path.is_file() or not path.name.startswith("event-"):
                raise AIPatternHoldoutRunError("holdout ledger contains an unsafe entry")
            suffix = path.name.removeprefix("event-").removesuffix(".json")
            if path.name != f"event-{suffix}.json" or len(suffix) != 8 or not suffix.isdigit():
                raise AIPatternHoldoutRunError("holdout ledger event filename differs")
            sequence = int(suffix)
            if sequence in paths:
                raise AIPatternHoldoutRunError("holdout ledger contains a duplicate sequence")
            paths[sequence] = path
        events: list[HoldoutLedgerEvent] = []
        predecessor: str | None = None
        prior_type: str | None = None
        request_sha256: str | None = None
        for expected_sequence, sequence in enumerate(sorted(paths), start=1):
            if sequence != expected_sequence:
                raise AIPatternHoldoutRunError("holdout ledger sequence is not contiguous")
            path = paths[sequence]
            raw = path.read_bytes()
            if path.stat().st_mode & _WRITE_BITS:
                raise AIPatternHoldoutRunError("holdout ledger event is writable")
            try:
                document = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise AIPatternHoldoutRunError("holdout ledger event is invalid JSON") from error
            if not isinstance(document, dict) or canonical_json_bytes(document) != raw:
                raise AIPatternHoldoutRunError("holdout ledger event is not canonical JSON")
            if (
                set(document)
                != {
                    "artifact_schema",
                    "event_type",
                    "payload",
                    "predecessor_sha256",
                    "recorded_at_utc",
                    "request_sha256",
                    "sequence",
                }
                or document["artifact_schema"] != AI_PATTERN_HOLDOUT_EVENT_SCHEMA
            ):
                raise AIPatternHoldoutRunError("holdout ledger event schema differs")
            event = HoldoutLedgerEvent(
                sequence=document["sequence"],
                predecessor_sha256=document["predecessor_sha256"],
                event_type=document["event_type"],
                request_sha256=document["request_sha256"],
                recorded_at_utc=document["recorded_at_utc"],
                payload=document["payload"],
            )
            if event.predecessor_sha256 != predecessor:
                raise AIPatternHoldoutRunError("holdout ledger predecessor chain differs")
            _validate_event_payload(event)
            if request_sha256 is None:
                request_sha256 = event.request_sha256
            if event.request_sha256 != request_sha256:
                raise AIPatternHoldoutRunError("holdout ledger contains multiple requests")
            _require_transition(prior_type, event.event_type)
            events.append(event)
            predecessor = event.sha256
            prior_type = event.event_type
        return tuple(events)

    def append(
        self,
        event_type: str,
        request_sha256: str,
        payload: Mapping[str, object],
    ) -> HoldoutLedgerEvent:
        events = self.verify()
        prior_type = events[-1].event_type if events else None
        _require_transition(prior_type, event_type)
        event = HoldoutLedgerEvent(
            sequence=len(events) + 1,
            predecessor_sha256=events[-1].sha256 if events else None,
            event_type=event_type,
            request_sha256=request_sha256,
            recorded_at_utc=datetime.now(UTC)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z"),
            payload=payload,
        )
        destination = self.events_root / f"event-{event.sequence:08d}.json"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(destination, flags, 0o444)
        except FileExistsError as error:
            raise AIPatternHoldoutRunError("concurrent holdout ledger append conflict") from error
        try:
            _write_all(descriptor, canonical_json_bytes(event.as_dict()))
            os.fsync(descriptor)
            os.fchmod(descriptor, 0o444)
        finally:
            os.close(descriptor)
        directory = os.open(self.events_root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        verified = self.verify()
        if verified[-1].sha256 != event.sha256:
            raise AIPatternHoldoutRunError("holdout ledger append did not replay exactly")
        return event


def _validate_sha_list(value: object, *, label: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise AIPatternHoldoutRunError(f"{label} must be a list")
    parsed = tuple(_sha256(item, label=label) for item in value)
    if len(set(parsed)) != len(parsed):
        raise AIPatternHoldoutRunError(f"{label} contains duplicate identities")
    return parsed


def _validate_event_payload(event: HoldoutLedgerEvent) -> None:
    expected = _EVENT_PAYLOAD_KEYS[event.event_type]
    if set(event.payload) != expected:
        raise AIPatternHoldoutRunError("holdout ledger payload keys differ")
    role = _EVENT_ARTIFACT_ROLES.get(event.event_type)
    if role is not None:
        field, artifact_type = role
        try:
            identity = ArtifactIdentity.from_dict(event.payload[field])
        except (TypeError, ValueError) as error:
            raise AIPatternHoldoutRunError("holdout ledger artifact identity differs") from error
        if identity.artifact_type != artifact_type:
            raise AIPatternHoldoutRunError("holdout ledger artifact role differs")
    for field in ("candidate_sha256s", "finalist_sha256s"):
        if field in event.payload:
            _validate_sha_list(event.payload[field], label=field)
    if "holm_family_sha256" in event.payload:
        _sha256(event.payload["holm_family_sha256"], label="Holm family SHA-256")
    if "final_status" in event.payload and event.payload["final_status"] not in (
        FINAL_HOLDOUT_STATUSES
    ):
        raise AIPatternHoldoutRunError("holdout ledger final status differs")
    for field in ("classification", "reason", "failure_code"):
        if field in event.payload and (
            not isinstance(event.payload[field], str) or not event.payload[field]
        ):
            raise AIPatternHoldoutRunError(f"holdout ledger {field} differs")


def _require_transition(prior: str | None, event_type: str) -> None:
    allowed: dict[str, set[str | None]] = {
        "PRECOMMITTED": {None},
        "SEARCH_MASKS_FROZEN": {"PRECOMMITTED"},
        "SEARCH_COMPLETED": {"SEARCH_MASKS_FROZEN"},
        "WALK_FORWARD_MASKS_FROZEN": {"SEARCH_COMPLETED"},
        "WALK_FORWARD_COMPLETED": {"WALK_FORWARD_MASKS_FROZEN"},
        "WALK_FORWARD_SKIPPED": {"SEARCH_COMPLETED"},
        "HOLDOUT_AUTHORIZED": {"WALK_FORWARD_COMPLETED"},
        "HOLDOUT_MASKS_FROZEN": {"HOLDOUT_AUTHORIZED"},
        "HOLDOUT_COMPLETED": {"HOLDOUT_MASKS_FROZEN"},
        "HOLDOUT_SKIPPED": {"WALK_FORWARD_COMPLETED", "WALK_FORWARD_SKIPPED"},
        "COMPLETED": {"HOLDOUT_COMPLETED", "HOLDOUT_SKIPPED"},
        "FAILED": set(_NONTERMINAL_EVENT_TYPES),
    }
    if event_type not in allowed or prior not in allowed[event_type]:
        raise AIPatternHoldoutRunError(
            f"holdout ledger transition {prior!r} -> {event_type!r} is invalid"
        )


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise AIPatternHoldoutRunError("holdout ledger write made no progress")
        view = view[written:]


def _sha256(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise AIPatternHoldoutRunError(f"{label} is not a lowercase SHA-256")
    return value


def _document(value: object) -> dict[str, object]:
    candidate = value.as_dict() if hasattr(value, "as_dict") else value
    try:
        decoded = json.loads(canonical_json_bytes(candidate))
    except (TypeError, ValueError) as error:
        raise AIPatternHoldoutRunError("stage evidence is not canonical JSON") from error
    if not isinstance(decoded, dict):
        raise AIPatternHoldoutRunError("stage evidence must be a JSON object")
    return decoded


def _safe_directory(path: Path, *, create: bool) -> Path:
    if path.is_symlink():
        raise AIPatternHoldoutRunError("holdout directory cannot be symbolic")
    if create:
        path.mkdir(parents=True, exist_ok=True)
    try:
        resolved = path.resolve(strict=True)
    except FileNotFoundError as error:
        raise AIPatternHoldoutRunError("holdout directory does not exist") from error
    if not resolved.is_dir() or resolved != path.absolute():
        raise AIPatternHoldoutRunError("holdout directory is unsafe")
    return resolved


def _project_root(value: Path | str) -> Path:
    requested = Path(value).expanduser()
    if requested.is_symlink():
        raise AIPatternHoldoutRunError("project root cannot be symbolic")
    root = requested.resolve(strict=True)
    if not root.is_dir() or not (root / "pyproject.toml").is_file():
        raise AIPatternHoldoutRunError("project root is not a systematic-fx checkout")
    return root


def _fixed_run_root(project_root: Path, *, create: bool) -> Path:
    current = project_root
    for part in DEFAULT_AI_PATTERN_HOLDOUT_ROOT.parts:
        current = current / part
        if current.is_symlink():
            raise AIPatternHoldoutRunError("holdout run root has a symbolic-link component")
    return _safe_directory(project_root / DEFAULT_AI_PATTERN_HOLDOUT_ROOT, create=create)


def _read_frozen_file(project_root: Path, relative: Path, expected_sha256: str) -> bytes:
    current = project_root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise AIPatternHoldoutRunError("frozen input has a symbolic-link component")
    path = project_root / relative
    if not path.is_file() or not path.resolve(strict=True).is_relative_to(project_root):
        raise AIPatternHoldoutRunError("frozen input is missing, writable, or unsafe")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_mode & _WRITE_BITS:
            raise AIPatternHoldoutRunError("frozen input is not an immutable regular file")
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 1024 * 1024):
            chunks.append(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    lexical = path.stat(follow_symlinks=False)
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ) or (after.st_dev, after.st_ino, after.st_size) != (
        lexical.st_dev,
        lexical.st_ino,
        lexical.st_size,
    ):
        raise AIPatternHoldoutRunError("frozen input changed while it was opened")
    payload = b"".join(chunks)
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise AIPatternHoldoutRunError("frozen input content identity differs")
    return payload


def _json_object(payload: bytes, *, label: str) -> dict[str, object]:
    try:
        document = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AIPatternHoldoutRunError(f"{label} is invalid JSON") from error
    if not isinstance(document, dict) or canonical_json_bytes(document) != payload:
        raise AIPatternHoldoutRunError(f"{label} is not canonical JSON")
    return document


def _integer(value: object, *, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise AIPatternHoldoutRunError(f"{label} is not a bounded integer")
    return value


def _load_frozen_batch3(project_root: Path) -> FrozenHoldoutBatch:
    """Load Batch 3 directly by immutable artifact identity, without historical code replay."""

    _read_frozen_file(
        project_root,
        AI_PATTERN_BATCH3_REQUEST_RELATIVE_PATH,
        EXPECTED_BATCH3_GOVERNED_REQUEST_SHA256,
    )
    _read_frozen_file(
        project_root,
        AI_PATTERN_BATCH3_REPORT_RELATIVE_PATH,
        EXPECTED_BATCH3_PROPOSAL_REPORT_SHA256,
    )
    raw = _read_frozen_file(
        project_root,
        AI_PATTERN_BATCH3_BATCH_RELATIVE_PATH,
        EXPECTED_BATCH3_PROPOSAL_BATCH_SHA256,
    )
    document = _json_object(raw, label="Batch 3 proposal artifact")
    if set(document) != {
        "artifact_schema",
        "authority",
        "base_batch",
        "base_batch_sha256",
        "directional_envelope_sha256",
        "proposer_mode",
        "semantic_policy_sha256",
    }:
        raise AIPatternHoldoutRunError("Batch 3 directional envelope schema differs")
    if (
        document["artifact_schema"] != DIRECTIONAL_PROPOSAL_BATCH_SCHEMA
        or document["authority"] != PROPOSAL_AUTHORITY
        or document["proposer_mode"] != V2_PROPOSER_MODE
        or document["semantic_policy_sha256"] != V2_SEMANTIC_POLICY_SHA256
    ):
        raise AIPatternHoldoutRunError("Batch 3 directional policy identity differs")
    _sha256(document["directional_envelope_sha256"], label="directional envelope SHA-256")
    base = document["base_batch"]
    if not isinstance(base, dict) or set(base) != {
        "artifact_schema",
        "authority",
        "candidate_universe_count",
        "context_sha256",
        "diversity_rejected_count",
        "limitations",
        "proposal_budget",
        "proposals",
        "proposer_mode",
        "recorded_response_sha256",
        "request_sha256",
        "status",
        "support_eligible_count",
    }:
        raise AIPatternHoldoutRunError("Batch 3 base batch schema differs")
    if (
        canonical_sha256(base) != document["base_batch_sha256"]
        or base["authority"] != PROPOSAL_AUTHORITY
        or base["status"] != PROPOSAL_FINAL_STATUS
        or base["proposal_budget"] != 12
        or base["candidate_universe_count"] != 560
        or not isinstance(base["proposals"], list)
        or len(base["proposals"]) != 12
    ):
        raise AIPatternHoldoutRunError("Batch 3 base batch identity differs")
    proposals: list[FrozenHoldoutProposal] = []
    expected_request = _sha256(base["request_sha256"], label="base request SHA-256")
    expected_context = _sha256(base["context_sha256"], label="base context SHA-256")
    for expected_rank, wrapper in enumerate(base["proposals"], start=1):
        if not isinstance(wrapper, dict) or set(wrapper) != {"proposal", "proposal_sha256"}:
            raise AIPatternHoldoutRunError("Batch 3 proposal wrapper differs")
        proposal = wrapper["proposal"]
        if not isinstance(proposal, dict) or set(proposal) != {
            "artifact_schema",
            "authority",
            "context_sha256",
            "direction",
            "family",
            "max_overlap_with_prior_ppm",
            "outcome_metrics",
            "rationale_code",
            "request_sha256",
            "rule",
            "selection_rank",
            "session_support_count",
            "session_support_ppm",
            "stability_ppm",
            "status",
            "support_ppm",
            "support_rows",
        }:
            raise AIPatternHoldoutRunError("Batch 3 proposal schema differs")
        proposal_sha256 = _sha256(wrapper["proposal_sha256"], label="proposal SHA-256")
        if (
            canonical_sha256(proposal) != proposal_sha256
            or proposal["authority"] != PROPOSAL_AUTHORITY
            or proposal["request_sha256"] != expected_request
            or proposal["context_sha256"] != expected_context
            or proposal["selection_rank"] != expected_rank
            or proposal["outcome_metrics"] is not None
            or proposal["status"] != "HYPOTHESIS_PROPOSED_NOT_EVALUATED"
        ):
            raise AIPatternHoldoutRunError("Batch 3 proposal lineage differs")
        rule = AndRule.from_dict(proposal["rule"])
        pattern = ProposedPattern(
            family=proposal["family"],
            direction=proposal["direction"],
            rationale_code=proposal["rationale_code"],
            rule=rule,
        )
        validate_pattern_semantics_v2(pattern)
        support_rows = _integer(proposal["support_rows"], label="proposal support_rows", minimum=1)
        support_days = _integer(
            proposal["session_support_count"],
            label="proposal session_support_count",
            minimum=1,
        )
        sessions = proposal["session_support_ppm"]
        if (
            not isinstance(sessions, list)
            or sum(
                isinstance(item, dict)
                and set(item) == {"session_id", "support_ppm"}
                and isinstance(item["support_ppm"], int)
                and not isinstance(item["support_ppm"], bool)
                and item["support_ppm"] > 0
                for item in sessions
            )
            != support_days
        ):
            raise AIPatternHoldoutRunError("Batch 3 proposal session support differs")
        proposals.append(
            FrozenHoldoutProposal(
                selection_rank=expected_rank,
                proposal_sha256=proposal_sha256,
                direction=pattern.direction,
                rule=rule,
                discovery_support_rows=support_rows,
                discovery_session_support_count=support_days,
            )
        )
    return FrozenHoldoutBatch(tuple(proposals))


def _dates_for_range(eligible: tuple[date, ...], value: BarDateRange) -> tuple[date, ...]:
    return eligible[value.start_active_ordinal - 1 : value.end_active_ordinal]


def _decision_dates(eligible: tuple[date, ...], value: BarDateRange) -> tuple[date, ...]:
    if value.decision_end_date is None:
        raise AIPatternHoldoutRunError("evaluation range has no decision dates")
    end = eligible.index(value.decision_end_date) + 1
    return eligible[value.start_active_ordinal - 1 : end]


def _partitions_for_range(
    dataset: LoadedBarDatasetManifest,
    start_ordinal: int,
    end_ordinal: int,
) -> tuple[BarDatasetPartition, ...]:
    return dataset.partitions[start_ordinal - 1 : end_ordinal]


def _build_evaluation_inputs(
    project_root: Path,
    config: AIPatternHoldoutConfig,
) -> HoldoutEvaluationInputs:
    del config
    batch = _load_frozen_batch3(project_root)
    dataset = load_bar_dataset_manifest(
        project_root / DATASET_MANIFEST_RELATIVE_PATH,
        expected_sha256=EXPECTED_DATASET_MANIFEST_SHA256,
    )
    if (
        dataset.dataset_manifest_sha256 != EXPECTED_DATASET_MANIFEST_SHA256
        or dataset.handoff_sha256 != EXPECTED_DATASET_HANDOFF_SHA256
        or dataset.source_manifest_sha256 != EXPECTED_SOURCE_MANIFEST_SHA256
        or dataset.source_manifest_sha256 != BAR_SOURCE_MANIFEST_SHA256
    ):
        raise AIPatternHoldoutRunError("bar dataset lineage differs from the precommit")
    split = plan_bar_splits(dataset.eligible_active_dates)
    if split.sha256 != EXPECTED_SPLIT_PLAN_SHA256:
        raise AIPatternHoldoutRunError("bar split plan differs from the precommit")
    reporting_groups = tuple(
        (
            block.split_key,
            _dates_for_range(dataset.eligible_active_dates, block),
        )
        for block in split.discovery_reporting_blocks
    )
    search = HoldoutStagePlan(
        stage_key="SEARCH",
        fold_number=None,
        decision_dates=_decision_dates(dataset.eligible_active_dates, split.discovery),
        partitions=_partitions_for_range(
            dataset,
            split.discovery.start_active_ordinal,
            split.discovery.end_active_ordinal,
        ),
        reporting_groups=reporting_groups,
    )
    walk_forward = tuple(
        HoldoutStagePlan(
            stage_key=f"WALK_FORWARD_{fold.fold_number}",
            fold_number=fold.fold_number,
            decision_dates=_decision_dates(dataset.eligible_active_dates, fold),
            partitions=_partitions_for_range(
                dataset,
                fold.start_active_ordinal,
                fold.end_active_ordinal,
            ),
            reporting_groups=(
                (fold.split_key, _decision_dates(dataset.eligible_active_dates, fold)),
            ),
        )
        for fold in split.walk_forward_folds
    )
    holdout_decisions = _decision_dates(dataset.eligible_active_dates, split.holdout)
    if len(holdout_decisions) != 120:
        raise AIPatternHoldoutRunError("holdout must contain exactly 120 decision dates")
    holdout = HoldoutStagePlan(
        stage_key="HOLDOUT",
        fold_number=None,
        decision_dates=holdout_decisions,
        partitions=_partitions_for_range(
            dataset,
            split.holdout.start_active_ordinal,
            split.outcome_tail.end_active_ordinal,
        ),
        reporting_groups=(
            ("HOLDOUT_HALF_1", holdout_decisions[:60]),
            ("HOLDOUT_HALF_2", holdout_decisions[60:]),
        ),
    )
    embargo_dates = set(_dates_for_range(dataset.eligible_active_dates, split.embargo))
    if any(
        embargo_dates.intersection(plan.data_dates) for plan in (search, *walk_forward, holdout)
    ):
        raise AIPatternHoldoutRunError("embargo partition leaked into an evaluation stage")
    return HoldoutEvaluationInputs(batch, dataset, split, search, walk_forward, holdout)


@dataclass(frozen=True, slots=True)
class AIPatternHoldoutRun:
    config: AIPatternHoldoutConfig
    batch: FrozenHoldoutBatch
    final_status: str
    request_artifact: ArtifactIdentity
    report_artifact: ArtifactIdentity
    evidence_artifacts: tuple[ArtifactIdentity, ...]
    root: Path

    def as_dict(self) -> dict[str, object]:
        return {
            "authority": AI_PATTERN_HOLDOUT_AUTHORITY,
            "batch3_governed_request_sha256": self.batch.governed_request_sha256,
            "batch3_proposal_batch_sha256": self.batch.proposal_batch_sha256,
            "config_file_sha256": self.config.file_sha256,
            "config_semantic_sha256": self.config.semantic_sha256,
            "database_mutated": False,
            "evidence_artifacts": [item.as_dict() for item in self.evidence_artifacts],
            "final_status": self.final_status,
            "network_accessed": False,
            "paper_live_or_promotion_authority": False,
            "physical_holdout_isolation": False,
            "report_artifact": self.report_artifact.as_dict(),
            "request_artifact": self.request_artifact.as_dict(),
            "schema": AI_PATTERN_HOLDOUT_RUN_SCHEMA,
            "strict_backtest_claim": False,
            "strict_sealed_holdout_claim": False,
        }


def _precommit_document(config: AIPatternHoldoutConfig) -> dict[str, object]:
    return {
        "artifact_schema": "systematic_fx.ai_pattern_holdout_request.v1",
        "authority": AI_PATTERN_HOLDOUT_AUTHORITY,
        "config": config.as_dict(),
        "config_file_sha256": config.file_sha256,
        "config_semantic_sha256": config.semantic_sha256,
        "limitations": [
            "LOCAL_FILESYSTEM_BYTES_ARE_NOT_PHYSICALLY_SEALED",
            "BAR_SCREENING_DIAGNOSTIC_ONLY",
            "NO_BID_ASK_FILL_PROOF",
            "NO_PAPER_LIVE_OR_PROMOTION_AUTHORITY",
            "NO_DATABASE_OR_NETWORK_MUTATION",
        ],
    }


def _artifact_document(
    config: AIPatternHoldoutConfig,
    *,
    stage: str,
    kind: str,
    payload: object,
) -> dict[str, object]:
    return {
        "artifact_schema": "systematic_fx.ai_pattern_holdout_stage_artifact.v1",
        "authority": AI_PATTERN_HOLDOUT_AUTHORITY,
        "config_semantic_sha256": config.semantic_sha256,
        "kind": kind,
        "payload": _document(payload),
        "stage": stage,
    }


def _skip_document(
    config: AIPatternHoldoutConfig,
    *,
    stage: str,
    reason: str,
) -> dict[str, object]:
    return {
        "artifact_schema": "systematic_fx.ai_pattern_holdout_stage_skip.v1",
        "authority": AI_PATTERN_HOLDOUT_AUTHORITY,
        "config_semantic_sha256": config.semantic_sha256,
        "holdout_bytes_opened": False if stage == "HOLDOUT" else None,
        "reason": reason,
        "stage": stage,
    }


def _holdout_authorization_document(
    config: AIPatternHoldoutConfig,
    finalists: tuple[str, ...],
) -> dict[str, object]:
    contract = config.as_dict()
    return {
        "artifact_schema": "systematic_fx.ai_pattern_holdout_authorization.v1",
        "authority": AI_PATTERN_HOLDOUT_AUTHORITY,
        "authorized_proposal_sha256s": list(finalists),
        "config_semantic_sha256": config.semantic_sha256,
        "execution": contract["execution"],
        "holdout_gates": contract["holdout_gates"],
        "holm_family": {
            "member_count": len(finalists),
            "members": list(finalists),
            "method": "HOLM_STEP_DOWN",
            "missing_or_error_p_value": 1,
        },
        "nulls": contract["nulls"],
        "open_order": "AUTHORIZATION_BEFORE_ANY_HOLDOUT_5M_OR_1S_BYTES",
    }


def _report_document(
    config: AIPatternHoldoutConfig,
    batch: FrozenHoldoutBatch,
    *,
    final_status: str,
    evidence_artifacts: Sequence[ArtifactIdentity],
    holdout_opened: bool,
) -> dict[str, object]:
    if final_status not in FINAL_HOLDOUT_STATUSES:
        raise AIPatternHoldoutRunError("terminal holdout status is outside the frozen vocabulary")
    if final_status.startswith("NO_") and holdout_opened:
        raise AIPatternHoldoutRunError("early-exit status cannot claim opened holdout bytes")
    return {
        "artifact_schema": "systematic_fx.ai_pattern_holdout_report.v1",
        "authority": AI_PATTERN_HOLDOUT_AUTHORITY,
        "batch3": {
            "governed_request_sha256": batch.governed_request_sha256,
            "proposal_batch_sha256": batch.proposal_batch_sha256,
            "proposal_report_sha256": batch.proposal_report_sha256,
        },
        "config_semantic_sha256": config.semantic_sha256,
        "database_mutated": False,
        "evidence_artifacts": [item.as_dict() for item in evidence_artifacts],
        "final_status": final_status,
        "holdout_bytes_opened": holdout_opened,
        "limitations": [
            "UNSEALED_LOCAL_BAR_SCREENING_HOLDOUT",
            "NO_PHYSICAL_HOLDOUT_ISOLATION",
            "NO_BID_ASK_FILL_PROOF",
            "NO_STRICT_BACKTEST_CLAIM",
            "NO_PAPER_LIVE_OR_PROMOTION_AUTHORITY",
        ],
        "network_accessed": False,
        "paper_live_or_promotion_authority": False,
        "physical_holdout_isolation": False,
        "strict_backtest_claim": False,
        "strict_sealed_holdout_claim": False,
    }


def _publish(
    artifacts_root: Path,
    *,
    artifact_type: str,
    filename_prefix: str,
    document: Mapping[str, object],
) -> ArtifactIdentity:
    identity = publish_canonical_artifact(
        artifacts_root,
        artifact_type=artifact_type,
        filename_prefix=filename_prefix,
        document=document,
    )
    verify_immutable_artifact(
        artifacts_root,
        identity,
        expected_bytes=canonical_json_bytes(document),
    )
    return identity


def _proposal_ids(batch: FrozenHoldoutBatch) -> tuple[str, ...]:
    return tuple(item.proposal_sha256 for item in batch.proposals)


def _validate_masks(
    masks: StageMaskBundle,
    *,
    stage_key: str,
    candidate_ids: tuple[str, ...],
    batch: FrozenHoldoutBatch,
) -> None:
    if masks.stage_key != stage_key or masks.proposal_sha256s != candidate_ids:
        raise AIPatternHoldoutRunError("stage mask candidate lineage differs")
    count_map = dict(masks.raw_signal_counts)
    day_map = dict(masks.signal_day_counts)
    if (
        len(count_map) != len(masks.raw_signal_counts)
        or len(day_map) != len(masks.signal_day_counts)
        or set(count_map) != set(candidate_ids)
        or set(day_map) != set(candidate_ids)
        or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in count_map.values()
        )
        or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in day_map.values()
        )
    ):
        raise AIPatternHoldoutRunError("stage mask accounting differs")
    if stage_key == "SEARCH":
        expected = {item.proposal_sha256: item for item in batch.proposals}
        if any(
            count_map[key] != expected[key].discovery_support_rows
            or day_map[key] != expected[key].discovery_session_support_count
            for key in candidate_ids
        ):
            raise AIPatternHoldoutRunError(
                "Search signal masks differ from frozen Batch 3 support evidence"
            )
    canonical_json_bytes(masks.as_dict())


def _validate_outcome(
    outcome: HoldoutStageOutcome,
    *,
    stage_key: str,
    candidate_ids: tuple[str, ...],
    maximum_finalists: int,
) -> None:
    finalists = outcome.finalist_proposal_sha256s
    expected_classifications = {
        "SEARCH": {"SEARCH_FINALISTS_SELECTED", "NO_SEARCH_FINALISTS"},
        "WALK_FORWARD": {
            "WALK_FORWARD_FINALISTS_SELECTED",
            "NO_WALK_FORWARD_FINALISTS",
        },
        "HOLDOUT": set(FINAL_HOLDOUT_STATUSES[2:]),
    }
    expected = expected_classifications.get(stage_key)
    if (
        outcome.stage_key != stage_key
        or len(finalists) > maximum_finalists
        or len(set(finalists)) != len(finalists)
        or not set(finalists).issubset(candidate_ids)
        or not isinstance(outcome.classification, str)
        or not outcome.classification
        or expected is None
        or outcome.classification not in expected
    ):
        raise AIPatternHoldoutRunError("stage outcome lineage or finalist budget differs")
    selected_classification = {
        "SEARCH": "SEARCH_FINALISTS_SELECTED",
        "WALK_FORWARD": "WALK_FORWARD_FINALISTS_SELECTED",
        "HOLDOUT": "ONE_SHOT_UNSEALED_BAR_HOLDOUT_DIAGNOSTIC_PASS",
    }[stage_key]
    if bool(finalists) != (outcome.classification == selected_classification):
        raise AIPatternHoldoutRunError("stage finalists and classification disagree")
    canonical_json_bytes(outcome.as_dict())


def _holdout_final_status(classification: str) -> str:
    if classification in FINAL_HOLDOUT_STATUSES[2:]:
        return classification
    raise AIPatternHoldoutRunError("holdout classification is outside the frozen vocabulary")


def _append_failed(
    ledger: HoldoutLedger,
    request_sha256: str,
    error: Exception,
) -> None:
    try:
        ledger.append(
            "FAILED",
            request_sha256,
            {"failure_code": type(error).__name__},
        )
    except Exception as ledger_error:
        raise AIPatternHoldoutRunError(
            "evaluation failed and its FAILED event could not be appended"
        ) from ledger_error


def _run_with_services(
    root: Path,
    config: AIPatternHoldoutConfig,
    run_root: Path,
    services: HoldoutRunServices,
) -> AIPatternHoldoutRun:
    ledger = HoldoutLedger(run_root / "ledger", create=True)
    if ledger.verify():
        raise AIPatternHoldoutRunError(
            "holdout evaluation already exists; verify instead of reopening a research budget"
        )
    artifacts_root = _safe_directory(run_root / "artifacts", create=True)
    precommit = _precommit_document(config)
    request_sha256 = canonical_sha256(precommit)
    request_artifact = _publish(
        artifacts_root,
        artifact_type="AI_PATTERN_HOLDOUT_REQUEST",
        filename_prefix="holdout-request",
        document=precommit,
    )
    ledger.append(
        "PRECOMMITTED",
        request_sha256,
        {"request_artifact": request_artifact.as_dict()},
    )
    evidence: list[ArtifactIdentity] = []
    try:
        inputs = services.load_inputs(root, config)
        all_ids = _proposal_ids(inputs.batch)

        search_masks = services.freeze_masks(
            root, config, inputs.batch, (inputs.search_plan,), all_ids
        )
        _validate_masks(
            search_masks,
            stage_key="SEARCH",
            candidate_ids=all_ids,
            batch=inputs.batch,
        )
        search_masks_artifact = _publish(
            artifacts_root,
            artifact_type="AI_PATTERN_SEARCH_MASKS",
            filename_prefix="search-masks",
            document=_artifact_document(config, stage="SEARCH", kind="MASKS", payload=search_masks),
        )
        evidence.append(search_masks_artifact)
        ledger.append(
            "SEARCH_MASKS_FROZEN",
            request_sha256,
            {
                "candidate_sha256s": list(all_ids),
                "masks_artifact": search_masks_artifact.as_dict(),
            },
        )

        search = services.evaluate_stage(
            root,
            config,
            inputs.batch,
            (inputs.search_plan,),
            all_ids,
            search_masks,
        )
        _validate_outcome(
            search,
            stage_key="SEARCH",
            candidate_ids=all_ids,
            maximum_finalists=4,
        )
        search_artifact = _publish(
            artifacts_root,
            artifact_type="AI_PATTERN_SEARCH_RESULT",
            filename_prefix="search-result",
            document=_artifact_document(config, stage="SEARCH", kind="RESULT", payload=search),
        )
        evidence.append(search_artifact)
        ledger.append(
            "SEARCH_COMPLETED",
            request_sha256,
            {
                "classification": search.classification,
                "finalist_sha256s": list(search.finalist_proposal_sha256s),
                "result_artifact": search_artifact.as_dict(),
            },
        )

        if not search.finalist_proposal_sha256s:
            walk_skip = _skip_document(
                config,
                stage="WALK_FORWARD",
                reason="NO_SEARCH_FINALISTS_OR_INCONCLUSIVE_SEARCH",
            )
            walk_artifact = _publish(
                artifacts_root,
                artifact_type="AI_PATTERN_WALK_FORWARD_SKIPPED",
                filename_prefix="walk-forward-skipped",
                document=walk_skip,
            )
            evidence.append(walk_artifact)
            ledger.append(
                "WALK_FORWARD_SKIPPED",
                request_sha256,
                {"reason": walk_skip["reason"], "skip_artifact": walk_artifact.as_dict()},
            )
            holdout_skip = _skip_document(
                config,
                stage="HOLDOUT",
                reason="NO_SEARCH_FINALISTS_HOLDOUT_NOT_OPENED",
            )
            holdout_artifact = _publish(
                artifacts_root,
                artifact_type="AI_PATTERN_HOLDOUT_SKIPPED",
                filename_prefix="holdout-skipped",
                document=holdout_skip,
            )
            evidence.append(holdout_artifact)
            ledger.append(
                "HOLDOUT_SKIPPED",
                request_sha256,
                {"reason": holdout_skip["reason"], "skip_artifact": holdout_artifact.as_dict()},
            )
            final_status = "NO_SEARCH_FINALISTS_HOLDOUT_NOT_OPENED"
            holdout_opened = False
        else:
            search_ids = search.finalist_proposal_sha256s
            walk_masks = services.freeze_masks(
                root, config, inputs.batch, inputs.walk_forward_plans, search_ids
            )
            _validate_masks(
                walk_masks,
                stage_key="WALK_FORWARD",
                candidate_ids=search_ids,
                batch=inputs.batch,
            )
            walk_masks_artifact = _publish(
                artifacts_root,
                artifact_type="AI_PATTERN_WALK_FORWARD_MASKS",
                filename_prefix="walk-forward-masks",
                document=_artifact_document(
                    config, stage="WALK_FORWARD", kind="MASKS", payload=walk_masks
                ),
            )
            evidence.append(walk_masks_artifact)
            ledger.append(
                "WALK_FORWARD_MASKS_FROZEN",
                request_sha256,
                {
                    "candidate_sha256s": list(search_ids),
                    "masks_artifact": walk_masks_artifact.as_dict(),
                },
            )
            walk = services.evaluate_stage(
                root,
                config,
                inputs.batch,
                inputs.walk_forward_plans,
                search_ids,
                walk_masks,
            )
            _validate_outcome(
                walk,
                stage_key="WALK_FORWARD",
                candidate_ids=search_ids,
                maximum_finalists=3,
            )
            walk_result_artifact = _publish(
                artifacts_root,
                artifact_type="AI_PATTERN_WALK_FORWARD_RESULT",
                filename_prefix="walk-forward-result",
                document=_artifact_document(
                    config, stage="WALK_FORWARD", kind="RESULT", payload=walk
                ),
            )
            evidence.append(walk_result_artifact)
            ledger.append(
                "WALK_FORWARD_COMPLETED",
                request_sha256,
                {
                    "classification": walk.classification,
                    "finalist_sha256s": list(walk.finalist_proposal_sha256s),
                    "result_artifact": walk_result_artifact.as_dict(),
                },
            )
            if not walk.finalist_proposal_sha256s:
                holdout_skip = _skip_document(
                    config,
                    stage="HOLDOUT",
                    reason="NO_WALK_FORWARD_FINALISTS_HOLDOUT_NOT_OPENED",
                )
                holdout_artifact = _publish(
                    artifacts_root,
                    artifact_type="AI_PATTERN_HOLDOUT_SKIPPED",
                    filename_prefix="holdout-skipped",
                    document=holdout_skip,
                )
                evidence.append(holdout_artifact)
                ledger.append(
                    "HOLDOUT_SKIPPED",
                    request_sha256,
                    {
                        "reason": holdout_skip["reason"],
                        "skip_artifact": holdout_artifact.as_dict(),
                    },
                )
                final_status = "NO_WALK_FORWARD_FINALISTS_HOLDOUT_NOT_OPENED"
                holdout_opened = False
            else:
                walk_ids = walk.finalist_proposal_sha256s
                authorization = _holdout_authorization_document(config, walk_ids)
                authorization_artifact = _publish(
                    artifacts_root,
                    artifact_type="AI_PATTERN_HOLDOUT_AUTHORIZATION",
                    filename_prefix="holdout-authorization",
                    document=authorization,
                )
                evidence.append(authorization_artifact)
                holm_family_sha256 = canonical_sha256(authorization["holm_family"])
                ledger.append(
                    "HOLDOUT_AUTHORIZED",
                    request_sha256,
                    {
                        "authorization_artifact": authorization_artifact.as_dict(),
                        "finalist_sha256s": list(walk_ids),
                        "holm_family_sha256": holm_family_sha256,
                    },
                )
                holdout_masks = services.freeze_masks(
                    root, config, inputs.batch, (inputs.holdout_plan,), walk_ids
                )
                _validate_masks(
                    holdout_masks,
                    stage_key="HOLDOUT",
                    candidate_ids=walk_ids,
                    batch=inputs.batch,
                )
                holdout_masks_artifact = _publish(
                    artifacts_root,
                    artifact_type="AI_PATTERN_HOLDOUT_MASKS",
                    filename_prefix="holdout-masks",
                    document=_artifact_document(
                        config, stage="HOLDOUT", kind="MASKS", payload=holdout_masks
                    ),
                )
                evidence.append(holdout_masks_artifact)
                ledger.append(
                    "HOLDOUT_MASKS_FROZEN",
                    request_sha256,
                    {
                        "candidate_sha256s": list(walk_ids),
                        "masks_artifact": holdout_masks_artifact.as_dict(),
                    },
                )
                holdout = services.evaluate_stage(
                    root,
                    config,
                    inputs.batch,
                    (inputs.holdout_plan,),
                    walk_ids,
                    holdout_masks,
                )
                _validate_outcome(
                    holdout,
                    stage_key="HOLDOUT",
                    candidate_ids=walk_ids,
                    maximum_finalists=3,
                )
                holdout_result_artifact = _publish(
                    artifacts_root,
                    artifact_type="AI_PATTERN_HOLDOUT_RESULT",
                    filename_prefix="holdout-result",
                    document=_artifact_document(
                        config, stage="HOLDOUT", kind="RESULT", payload=holdout
                    ),
                )
                evidence.append(holdout_result_artifact)
                final_status = _holdout_final_status(holdout.classification)
                ledger.append(
                    "HOLDOUT_COMPLETED",
                    request_sha256,
                    {
                        "classification": holdout.classification,
                        "final_status": final_status,
                        "result_artifact": holdout_result_artifact.as_dict(),
                    },
                )
                holdout_opened = True
        report = _report_document(
            config,
            inputs.batch,
            final_status=final_status,
            evidence_artifacts=evidence,
            holdout_opened=holdout_opened,
        )
        report_artifact = _publish(
            artifacts_root,
            artifact_type="AI_PATTERN_HOLDOUT_REPORT",
            filename_prefix="holdout-report",
            document=report,
        )
        ledger.append(
            "COMPLETED",
            request_sha256,
            {"final_status": final_status, "report_artifact": report_artifact.as_dict()},
        )
    except Exception as error:
        _append_failed(ledger, request_sha256, error)
        raise
    return AIPatternHoldoutRun(
        config=config,
        batch=inputs.batch,
        final_status=final_status,
        request_artifact=request_artifact,
        report_artifact=report_artifact,
        evidence_artifacts=tuple(evidence),
        root=run_root,
    )


def _identity_from_event(event: HoldoutLedgerEvent, field: str) -> ArtifactIdentity:
    try:
        return ArtifactIdentity.from_dict(event.payload[field])
    except (KeyError, TypeError, ValueError) as error:
        raise AIPatternHoldoutRunError(
            "ledger artifact identity cannot be reconstructed"
        ) from error


def _verify_expected_artifact(
    artifacts_root: Path,
    event: HoldoutLedgerEvent,
    field: str,
    document: Mapping[str, object],
) -> ArtifactIdentity:
    identity = _identity_from_event(event, field)
    verify_immutable_artifact(
        artifacts_root,
        identity,
        expected_bytes=canonical_json_bytes(document),
    )
    return identity


def _expect_event(
    events: tuple[HoldoutLedgerEvent, ...], index: int, event_type: str
) -> HoldoutLedgerEvent:
    if index >= len(events) or events[index].event_type != event_type:
        raise AIPatternHoldoutRunError(f"expected exact {event_type} lifecycle event")
    return events[index]


def _verify_with_services(
    root: Path,
    config: AIPatternHoldoutConfig,
    run_root: Path,
    services: HoldoutRunServices,
) -> AIPatternHoldoutRun:
    ledger_root = _safe_directory(run_root / "ledger", create=False)
    artifacts_root = _safe_directory(run_root / "artifacts", create=False)
    events_root = _safe_directory(ledger_root / "events", create=False)
    if events_root.parent != ledger_root:
        raise AIPatternHoldoutRunError("holdout ledger root lineage differs")
    events = HoldoutLedger(ledger_root, create=False).verify()
    if not events or events[-1].event_type == "FAILED":
        raise AIPatternHoldoutRunError("holdout evaluation is failed or incomplete")
    if events[-1].event_type != "COMPLETED":
        raise AIPatternHoldoutRunError("holdout evaluation lacks a terminal completion")
    precommit = _precommit_document(config)
    request_sha256 = canonical_sha256(precommit)
    if any(event.request_sha256 != request_sha256 for event in events):
        raise AIPatternHoldoutRunError("holdout ledger belongs to another config precommit")
    cursor = 0
    event = _expect_event(events, cursor, "PRECOMMITTED")
    request_artifact = _verify_expected_artifact(
        artifacts_root, event, "request_artifact", precommit
    )
    referenced = [request_artifact]
    cursor += 1

    inputs = services.load_inputs(root, config)
    all_ids = _proposal_ids(inputs.batch)
    search_masks = services.freeze_masks(root, config, inputs.batch, (inputs.search_plan,), all_ids)
    _validate_masks(
        search_masks,
        stage_key="SEARCH",
        candidate_ids=all_ids,
        batch=inputs.batch,
    )
    event = _expect_event(events, cursor, "SEARCH_MASKS_FROZEN")
    if tuple(event.payload["candidate_sha256s"]) != all_ids:
        raise AIPatternHoldoutRunError("Search ledger candidate family differs")
    identity = _verify_expected_artifact(
        artifacts_root,
        event,
        "masks_artifact",
        _artifact_document(config, stage="SEARCH", kind="MASKS", payload=search_masks),
    )
    referenced.append(identity)
    cursor += 1

    search = services.evaluate_stage(
        root, config, inputs.batch, (inputs.search_plan,), all_ids, search_masks
    )
    _validate_outcome(
        search,
        stage_key="SEARCH",
        candidate_ids=all_ids,
        maximum_finalists=4,
    )
    event = _expect_event(events, cursor, "SEARCH_COMPLETED")
    if (
        event.payload["classification"] != search.classification
        or tuple(event.payload["finalist_sha256s"]) != search.finalist_proposal_sha256s
    ):
        raise AIPatternHoldoutRunError("Search result ledger lineage differs")
    identity = _verify_expected_artifact(
        artifacts_root,
        event,
        "result_artifact",
        _artifact_document(config, stage="SEARCH", kind="RESULT", payload=search),
    )
    referenced.append(identity)
    cursor += 1

    if not search.finalist_proposal_sha256s:
        walk_skip = _skip_document(
            config,
            stage="WALK_FORWARD",
            reason="NO_SEARCH_FINALISTS_OR_INCONCLUSIVE_SEARCH",
        )
        event = _expect_event(events, cursor, "WALK_FORWARD_SKIPPED")
        if event.payload["reason"] != walk_skip["reason"]:
            raise AIPatternHoldoutRunError("walk-forward skip reason differs")
        identity = _verify_expected_artifact(artifacts_root, event, "skip_artifact", walk_skip)
        referenced.append(identity)
        cursor += 1
        holdout_skip = _skip_document(
            config,
            stage="HOLDOUT",
            reason="NO_SEARCH_FINALISTS_HOLDOUT_NOT_OPENED",
        )
        event = _expect_event(events, cursor, "HOLDOUT_SKIPPED")
        if event.payload["reason"] != holdout_skip["reason"]:
            raise AIPatternHoldoutRunError("holdout skip reason differs")
        identity = _verify_expected_artifact(artifacts_root, event, "skip_artifact", holdout_skip)
        referenced.append(identity)
        cursor += 1
        final_status = "NO_SEARCH_FINALISTS_HOLDOUT_NOT_OPENED"
        holdout_opened = False
    else:
        search_ids = search.finalist_proposal_sha256s
        walk_masks = services.freeze_masks(
            root, config, inputs.batch, inputs.walk_forward_plans, search_ids
        )
        _validate_masks(
            walk_masks,
            stage_key="WALK_FORWARD",
            candidate_ids=search_ids,
            batch=inputs.batch,
        )
        event = _expect_event(events, cursor, "WALK_FORWARD_MASKS_FROZEN")
        if tuple(event.payload["candidate_sha256s"]) != search_ids:
            raise AIPatternHoldoutRunError("walk-forward candidate ledger differs")
        identity = _verify_expected_artifact(
            artifacts_root,
            event,
            "masks_artifact",
            _artifact_document(config, stage="WALK_FORWARD", kind="MASKS", payload=walk_masks),
        )
        referenced.append(identity)
        cursor += 1
        walk = services.evaluate_stage(
            root,
            config,
            inputs.batch,
            inputs.walk_forward_plans,
            search_ids,
            walk_masks,
        )
        _validate_outcome(
            walk,
            stage_key="WALK_FORWARD",
            candidate_ids=search_ids,
            maximum_finalists=3,
        )
        event = _expect_event(events, cursor, "WALK_FORWARD_COMPLETED")
        if (
            event.payload["classification"] != walk.classification
            or tuple(event.payload["finalist_sha256s"]) != walk.finalist_proposal_sha256s
        ):
            raise AIPatternHoldoutRunError("walk-forward result ledger differs")
        identity = _verify_expected_artifact(
            artifacts_root,
            event,
            "result_artifact",
            _artifact_document(config, stage="WALK_FORWARD", kind="RESULT", payload=walk),
        )
        referenced.append(identity)
        cursor += 1
        if not walk.finalist_proposal_sha256s:
            holdout_skip = _skip_document(
                config,
                stage="HOLDOUT",
                reason="NO_WALK_FORWARD_FINALISTS_HOLDOUT_NOT_OPENED",
            )
            event = _expect_event(events, cursor, "HOLDOUT_SKIPPED")
            if event.payload["reason"] != holdout_skip["reason"]:
                raise AIPatternHoldoutRunError("holdout skip reason differs")
            identity = _verify_expected_artifact(
                artifacts_root, event, "skip_artifact", holdout_skip
            )
            referenced.append(identity)
            cursor += 1
            final_status = "NO_WALK_FORWARD_FINALISTS_HOLDOUT_NOT_OPENED"
            holdout_opened = False
        else:
            walk_ids = walk.finalist_proposal_sha256s
            authorization = _holdout_authorization_document(config, walk_ids)
            event = _expect_event(events, cursor, "HOLDOUT_AUTHORIZED")
            if tuple(event.payload["finalist_sha256s"]) != walk_ids or event.payload[
                "holm_family_sha256"
            ] != canonical_sha256(authorization["holm_family"]):
                raise AIPatternHoldoutRunError("holdout authorization family differs")
            identity = _verify_expected_artifact(
                artifacts_root,
                event,
                "authorization_artifact",
                authorization,
            )
            referenced.append(identity)
            cursor += 1
            # The default freezer is the first function allowed to open holdout 5m bytes.
            holdout_masks = services.freeze_masks(
                root, config, inputs.batch, (inputs.holdout_plan,), walk_ids
            )
            _validate_masks(
                holdout_masks,
                stage_key="HOLDOUT",
                candidate_ids=walk_ids,
                batch=inputs.batch,
            )
            event = _expect_event(events, cursor, "HOLDOUT_MASKS_FROZEN")
            if tuple(event.payload["candidate_sha256s"]) != walk_ids:
                raise AIPatternHoldoutRunError("holdout mask family differs")
            identity = _verify_expected_artifact(
                artifacts_root,
                event,
                "masks_artifact",
                _artifact_document(config, stage="HOLDOUT", kind="MASKS", payload=holdout_masks),
            )
            referenced.append(identity)
            cursor += 1
            holdout = services.evaluate_stage(
                root,
                config,
                inputs.batch,
                (inputs.holdout_plan,),
                walk_ids,
                holdout_masks,
            )
            _validate_outcome(
                holdout,
                stage_key="HOLDOUT",
                candidate_ids=walk_ids,
                maximum_finalists=3,
            )
            final_status = _holdout_final_status(holdout.classification)
            event = _expect_event(events, cursor, "HOLDOUT_COMPLETED")
            if (
                event.payload["classification"] != holdout.classification
                or event.payload["final_status"] != final_status
            ):
                raise AIPatternHoldoutRunError("holdout terminal classification differs")
            identity = _verify_expected_artifact(
                artifacts_root,
                event,
                "result_artifact",
                _artifact_document(config, stage="HOLDOUT", kind="RESULT", payload=holdout),
            )
            referenced.append(identity)
            cursor += 1
            holdout_opened = True
    report = _report_document(
        config,
        inputs.batch,
        final_status=final_status,
        evidence_artifacts=referenced[1:],
        holdout_opened=holdout_opened,
    )
    event = _expect_event(events, cursor, "COMPLETED")
    if event.payload["final_status"] != final_status:
        raise AIPatternHoldoutRunError("terminal report status differs")
    report_artifact = _verify_expected_artifact(artifacts_root, event, "report_artifact", report)
    cursor += 1
    if cursor != len(events):
        raise AIPatternHoldoutRunError("holdout ledger contains trailing lifecycle events")
    expected_leaves = {item.relative_uri for item in (*referenced, report_artifact)}
    observed_leaves: set[str] = set()
    for path in artifacts_root.iterdir():
        if path.is_symlink() or not path.is_file() or path.stat().st_mode & _WRITE_BITS:
            raise AIPatternHoldoutRunError("holdout artifact directory contains an unsafe entry")
        observed_leaves.add(path.name)
    if observed_leaves != expected_leaves:
        raise AIPatternHoldoutRunError("holdout artifact set differs from its ledger")
    return AIPatternHoldoutRun(
        config=config,
        batch=inputs.batch,
        final_status=final_status,
        request_artifact=request_artifact,
        report_artifact=report_artifact,
        evidence_artifacts=tuple(referenced[1:]),
        root=run_root,
    )


def _normalized_stage_key(plans: tuple[HoldoutStagePlan, ...]) -> str:
    if not plans:
        raise AIPatternHoldoutRunError("stage cannot have an empty allowlist")
    if all(item.stage_key.startswith("WALK_FORWARD_") for item in plans):
        if tuple(item.fold_number for item in plans) != (1, 2, 3, 4, 5):
            raise AIPatternHoldoutRunError("walk-forward stage must contain all five folds")
        return "WALK_FORWARD"
    if len(plans) == 1 and plans[0].stage_key in {"SEARCH", "HOLDOUT"}:
        return plans[0].stage_key
    raise AIPatternHoldoutRunError("stage plan collection differs from the frozen lifecycle")


def _stage_groups(plans: tuple[HoldoutStagePlan, ...]) -> dict[date, str]:
    output: dict[date, str] = {}
    for plan in plans:
        assigned: set[date] = set()
        for key, dates in plan.reporting_groups:
            for source_date in dates:
                if source_date in output:
                    raise AIPatternHoldoutRunError(
                        "stage date belongs to multiple reporting groups"
                    )
                output[source_date] = key
                assigned.add(source_date)
        if assigned != set(plan.decision_dates):
            raise AIPatternHoldoutRunError("reporting groups do not cover exact decision dates")
    return output


def _stage_partitions(
    plans: tuple[HoldoutStagePlan, ...],
) -> tuple[BarDatasetPartition, ...]:
    partitions = tuple(partition for plan in plans for partition in plan.partitions)
    dates = tuple(item.source_date for item in partitions)
    if dates != tuple(sorted(set(dates))):
        raise AIPatternHoldoutRunError("stage partition allowlist is duplicated or unordered")
    return partitions


def _load_stage_bars(
    project_root: Path,
    partitions: Sequence[BarDatasetPartition],
    timeframe_seconds: int,
) -> tuple[object, ...]:
    from scripts.ai_pattern_holdout_engine import BarWithOutcomeSpan

    if timeframe_seconds not in {1, 300}:
        raise AIPatternHoldoutRunError("holdout adapter may load only 1s or 5m bars")
    data_root = project_root / "data"
    if data_root.is_symlink() or not data_root.is_dir():
        raise AIPatternHoldoutRunError("bar data root is missing or symbolic")
    output: list[BarWithOutcomeSpan] = []
    for partition in partitions:
        matches = tuple(
            item for item in partition.artifacts if item.timeframe_seconds == timeframe_seconds
        )
        if len(matches) != 1:
            raise AIPatternHoldoutRunError("stage partition lacks one exact timeframe artifact")
        bars = load_trade_bar_artifact(
            data_root,
            matches[0],
            expected_plan_sha256=partition.plan_sha256,
            expected_source_sha256=partition.source_sha256,
            expected_source_date=partition.source_date,
        )
        if any(
            item.source_date != partition.source_date or item.contract != partition.contract
            for item in bars
        ):
            raise AIPatternHoldoutRunError("loaded bar partition differs from its manifest")
        output.extend(BarWithOutcomeSpan(item, partition.outcome_span_id) for item in bars)
    return tuple(output)


def _one_second_outcome_parts(
    project_root: Path,
    partitions: tuple[BarDatasetPartition, ...],
) -> Iterator[tuple[object, ...]]:
    """Yield one manifest outcome span at a time; never retain all WF 1s rows."""

    for _span, grouped in groupby(partitions, key=lambda item: item.outcome_span_id):
        yield _load_stage_bars(project_root, tuple(grouped), 1)


def _freeze_masks_default(
    project_root: Path,
    config: AIPatternHoldoutConfig,
    batch: FrozenHoldoutBatch,
    plans: tuple[HoldoutStagePlan, ...],
    candidate_ids: tuple[str, ...],
) -> StageMaskBundle:
    from scripts.ai_pattern_holdout_engine import (
        MATCHED_RELAXATION_LEVELS,
        ExecutionSpec,
        build_stage_masks,
    )

    stage_key = _normalized_stage_key(plans)
    five_minute_bars = _load_stage_bars(project_root, _stage_partitions(plans), 300)
    contract = config.as_dict()
    execution = contract["execution"]
    nulls = contract["nulls"]
    spec = ExecutionSpec()
    if (
        not isinstance(execution, dict)
        or execution["profit_target_ticks"] != spec.take_profit_ticks
        or execution["stop_loss_ticks"] != spec.stop_loss_ticks
        or execution["holding_horizon_seconds"] != spec.horizon_seconds
        or execution["entry_adverse_ticks"] != spec.entry_adverse_ticks
        or execution["profit_target_trade_through_ticks"] != spec.take_profit_trade_through_ticks
        or execution["stop_loss_minimum_adverse_ticks"] != spec.stop_total_minimum_adverse_ticks
        or execution["terminal_adverse_ticks"] != spec.terminal_exit_adverse_ticks
        or execution["variable_cost_ticks"] != spec.variable_debit_ticks
        or execution["allocated_fixed_cost_ticks"] != spec.allocated_fixed_cost_ticks
        or execution["fully_loaded_round_trip_cost_ticks"] != spec.fully_loaded_cost_ticks
        or not isinstance(nulls, dict)
        or not isinstance(nulls["master_seed"], int)
        or tuple(nulls["matched_fallback_order"][:-1]) != MATCHED_RELAXATION_LEVELS
        or nulls["matched_fallback_order"][-1] != "SAMPLE_INELIGIBLE"
    ):
        raise AIPatternHoldoutRunError("engine execution/null contract differs from config")
    engine_bundle = build_stage_masks(
        stage_key,
        five_minute_bars,
        batch.proposals,
        candidate_ids,
        spec,
        nulls["master_seed"],
        _stage_groups(plans),
    )
    mask_by_sha = {item.proposal.proposal_sha256: item for item in engine_bundle.proposal_masks}
    if set(mask_by_sha) != set(candidate_ids):
        raise AIPatternHoldoutRunError("engine mask family differs from requested candidates")
    return StageMaskBundle(
        stage_key=stage_key,
        proposal_sha256s=candidate_ids,
        raw_signal_counts=tuple(
            (key, mask_by_sha[key].rule_support_count) for key in candidate_ids
        ),
        signal_day_counts=tuple(
            (key, mask_by_sha[key].rule_support_day_count) for key in candidate_ids
        ),
        payload=engine_bundle,
    )


def _evaluate_stage_default(
    project_root: Path,
    config: AIPatternHoldoutConfig,
    batch: FrozenHoldoutBatch,
    plans: tuple[HoldoutStagePlan, ...],
    candidate_ids: tuple[str, ...],
    masks: StageMaskBundle,
) -> HoldoutStageOutcome:
    from scripts import ai_pattern_holdout_engine as engine

    stage_key = _normalized_stage_key(plans)
    partitions = _stage_partitions(plans)
    fives = _load_stage_bars(project_root, partitions, 300)
    if not isinstance(masks.payload, engine.StageMaskBundle):
        raise AIPatternHoldoutRunError("default evaluator received a non-engine mask bundle")
    raw = engine.evaluate_stage_parts(
        stage_key,
        fives,
        _one_second_outcome_parts(project_root, partitions),
        masks.payload,
        batch.proposals,
        engine.ExecutionSpec(),
        lambda _candidate: True,
        group_by_date=_stage_groups(plans),
        classification_pass="RAW_STAGE_EVALUATED",
        classification_fail="RAW_STAGE_NO_SAMPLE_ELIGIBLE_CANDIDATES",
    )
    selected = engine.select_stage_result(stage_key, raw, masks.payload, candidate_ids)
    if not isinstance(selected, engine.StageEvaluationResult):
        raise AIPatternHoldoutRunError("engine family selection returned an invalid result")
    return HoldoutStageOutcome(
        stage_key=stage_key,
        finalist_proposal_sha256s=selected.finalist_proposal_sha256s,
        classification=selected.classification,
        payload=selected,
    )


def _default_services() -> HoldoutRunServices:
    return HoldoutRunServices(
        load_inputs=_build_evaluation_inputs,
        freeze_masks=_freeze_masks_default,
        evaluate_stage=_evaluate_stage_default,
    )


def run_ai_pattern_holdout(
    project_root: Path | str,
) -> AIPatternHoldoutRun:
    """Execute the finite Search/WF/one-shot holdout lifecycle exactly once."""

    root = _project_root(project_root)
    config = load_ai_pattern_holdout_config(root)
    run_root = _fixed_run_root(root, create=True)
    return _run_with_services(root, config, run_root, _default_services())


def verify_ai_pattern_holdout(
    project_root: Path | str,
) -> AIPatternHoldoutRun:
    """Read-only replay every decision and immutable artifact from committed inputs."""

    root = _project_root(project_root)
    config = load_ai_pattern_holdout_config(root)
    run_root = _fixed_run_root(root, create=False)
    return _verify_with_services(root, config, run_root, _default_services())


def render_ai_pattern_holdout_report(run: AIPatternHoldoutRun) -> str:
    """Render the bounded result without sealed-backtest or promotion language."""

    data = run.as_dict()
    holdout_opened = not str(data["final_status"]).startswith("NO_")
    return "\n".join(
        [
            "# AI Pattern Batch 3 Performance Evaluation",
            "",
            f"- Final status: `{data['final_status']}`",
            f"- Authority: `{AI_PATTERN_HOLDOUT_AUTHORITY}`",
            f"- Batch 3 SHA-256: `{data['batch3_proposal_batch_sha256']}`",
            f"- Config SHA-256: `{data['config_semantic_sha256']}`",
            f"- Holdout bytes opened: {'yes' if holdout_opened else 'no'}",
            "- Persistent database/network mutation: no",
            "- Strict sealed holdout/backtest claim: no",
            "- Paper, live, or promotion authority: no",
            "",
            (
                "This is a one-shot local bar-screening diagnostic. The local filesystem does "
                "not provide physical holdout isolation or bid/ask fill proof."
            ),
            "",
        ]
    )


def publish_ai_pattern_holdout_report(
    project_root: Path | str,
    run: AIPatternHoldoutRun,
) -> Path:
    """Publish a non-governed convenience Markdown rendering after completion."""

    root = _project_root(project_root)
    output = root / "reports/generated/ai_pattern_holdout_batch_3.md"
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.is_symlink():
        raise AIPatternHoldoutRunError("holdout Markdown report cannot be symbolic")
    temporary = output.with_suffix(".md.tmp")
    temporary.write_text(render_ai_pattern_holdout_report(run), encoding="utf-8")
    os.replace(temporary, output)
    return output


__all__ = [
    "AI_PATTERN_HOLDOUT_EVENT_SCHEMA",
    "AI_PATTERN_HOLDOUT_RUN_SCHEMA",
    "DEFAULT_AI_PATTERN_HOLDOUT_ROOT",
    "AIPatternHoldoutRun",
    "AIPatternHoldoutRunError",
    "FrozenHoldoutBatch",
    "FrozenHoldoutProposal",
    "HoldoutEvaluationInputs",
    "HoldoutLedger",
    "HoldoutLedgerEvent",
    "HoldoutRunServices",
    "HoldoutStageOutcome",
    "HoldoutStagePlan",
    "StageMaskBundle",
    "publish_ai_pattern_holdout_report",
    "render_ai_pattern_holdout_report",
    "run_ai_pattern_holdout",
    "verify_ai_pattern_holdout",
]
