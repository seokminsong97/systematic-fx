"""Governed, crash-resumable orchestration for delayed multi-timeframe research.

The statistical implementation lives in :mod:`scripts.ai_delayed_mtf_engine`.
This module owns only authorization, access ordering, immutable publication,
and replay.  Search masks are committed before Search outcomes; only its frozen
selection may then open and freeze all five walk-forward folds before any WF
outcome; holdout feature and outcome bytes remain behind their own authorization.

There are deliberately no public dependency-injection arguments.  The small
``_DelayedMTFServices`` seam is private and exists solely so unit tests can
prove lifecycle ordering without opening the research data.
"""

from __future__ import annotations

import fcntl
import json
import os
import stat
import tempfile
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from fractions import Fraction
from itertools import groupby
from pathlib import Path
from typing import Final

from scripts.ai_delayed_mtf_config import (
    AI_DELAYED_MTF_AUTHORITY,
    AIDelayedMTFConfig,
    load_ai_delayed_mtf_config,
)
from scripts.ai_pattern_holdout_config import (
    DATASET_MANIFEST_RELATIVE_PATH,
    EXPECTED_DATASET_HANDOFF_SHA256,
    EXPECTED_DATASET_MANIFEST_SHA256,
    EXPECTED_SOURCE_MANIFEST_SHA256,
    EXPECTED_SPLIT_PLAN_SHA256,
)
from systematic_fx.features.bars import TradeBar, load_trade_bar_artifact
from systematic_fx.research.ai_pattern_discovery import (
    ArtifactIdentity,
    publish_canonical_artifact,
    verify_immutable_artifact,
)
from systematic_fx.research.bar_config import BAR_SOURCE_MANIFEST_SHA256
from systematic_fx.research.bar_pipeline import (
    BarDatasetPartition,
    LoadedBarDatasetManifest,
    load_bar_dataset_manifest,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.validation.bar_splits import BarDateRange, BarSplitPlan, plan_bar_splits

AI_DELAYED_MTF_RUN_SCHEMA: Final = "systematic_fx.ai_delayed_mtf_run.v1"
AI_DELAYED_MTF_EVENT_SCHEMA: Final = "systematic_fx.ai_delayed_mtf_event.v1"
DEFAULT_AI_DELAYED_MTF_ROOT: Final = Path("data/derived/bar_patterns/ai_delayed_mtf_v1")
WALK_FORWARD_STAGE_KEYS: Final = ("WF1", "WF2", "WF3", "WF4", "WF5")
MAXIMUM_SEARCH_SELECTION: Final = 8
MAXIMUM_HOLDOUT_FINALISTS: Final = 3
HOLDOUT_CLASSIFICATIONS: Final = (
    "ONE_SHOT_UNSEALED_DELAYED_MTF_HOLDOUT_DIAGNOSTIC_PASS",
    "ONE_SHOT_UNSEALED_DELAYED_MTF_HOLDOUT_DIAGNOSTIC_FAIL",
)
FINAL_STATUSES: Final = (
    "NO_SEARCH_FINALISTS_HOLDOUT_NOT_OPENED",
    "NO_WALK_FORWARD_FINALISTS_HOLDOUT_NOT_OPENED",
    *HOLDOUT_CLASSIFICATIONS,
)
_WRITE_BITS: Final = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
_SHA256_LENGTH: Final = 64
_EVENT_TYPES: Final = (
    "PRECOMMITTED",
    "SEARCH_MASKS_FROZEN",
    "SEARCH_RESULTS_RELEASED",
    "WALK_FORWARD_SKIPPED",
    "WALK_FORWARD_MASKS_FROZEN",
    "WALK_FORWARD_RESULTS_RELEASED",
    "HOLDOUT_SKIPPED",
    "HOLDOUT_AUTHORIZED",
    "HOLDOUT_MASKS_FROZEN",
    "HOLDOUT_RESULTS_RELEASED",
    "COMPLETED",
    "FAILED",
)
_TRANSITIONS: Final = {
    "PRECOMMITTED": {None},
    "SEARCH_MASKS_FROZEN": {"PRECOMMITTED"},
    "SEARCH_RESULTS_RELEASED": {"SEARCH_MASKS_FROZEN"},
    "WALK_FORWARD_SKIPPED": {"SEARCH_RESULTS_RELEASED"},
    "WALK_FORWARD_MASKS_FROZEN": {"SEARCH_RESULTS_RELEASED"},
    "WALK_FORWARD_RESULTS_RELEASED": {"WALK_FORWARD_MASKS_FROZEN"},
    "HOLDOUT_SKIPPED": {"WALK_FORWARD_SKIPPED", "WALK_FORWARD_RESULTS_RELEASED"},
    "HOLDOUT_AUTHORIZED": {"WALK_FORWARD_RESULTS_RELEASED"},
    "HOLDOUT_MASKS_FROZEN": {"HOLDOUT_AUTHORIZED"},
    "HOLDOUT_RESULTS_RELEASED": {"HOLDOUT_MASKS_FROZEN"},
    "COMPLETED": {"HOLDOUT_SKIPPED", "HOLDOUT_RESULTS_RELEASED"},
    "FAILED": set(_EVENT_TYPES) - {"COMPLETED", "FAILED"},
}
_EVENT_PAYLOAD_KEYS: Final = {
    "PRECOMMITTED": {"request_artifact"},
    "SEARCH_MASKS_FROZEN": {"candidate_ids", "masks_artifact"},
    "SEARCH_RESULTS_RELEASED": {
        "result_artifact",
        "selected_candidate_ids",
    },
    "WALK_FORWARD_SKIPPED": {"reason", "skip_artifact"},
    "WALK_FORWARD_MASKS_FROZEN": {
        "candidate_ids",
        "fold_keys",
        "masks_artifact",
    },
    "WALK_FORWARD_RESULTS_RELEASED": {
        "finalist_candidate_ids",
        "result_artifact",
    },
    "HOLDOUT_SKIPPED": {"final_status", "reason", "skip_artifact"},
    "HOLDOUT_AUTHORIZED": {
        "authorization_artifact",
        "family_sha256",
        "finalist_candidate_ids",
    },
    "HOLDOUT_MASKS_FROZEN": {"candidate_ids", "masks_artifact"},
    "HOLDOUT_RESULTS_RELEASED": {
        "classification",
        "result_artifact",
    },
    "COMPLETED": {"final_status", "report_artifact"},
    "FAILED": {"failure_code"},
}
_ARTIFACT_ROLES: Final = {
    "PRECOMMITTED": ("request_artifact", "AI_DELAYED_MTF_REQUEST"),
    "SEARCH_MASKS_FROZEN": (
        "masks_artifact",
        "AI_DELAYED_MTF_SEARCH_MASKS",
    ),
    "SEARCH_RESULTS_RELEASED": (
        "result_artifact",
        "AI_DELAYED_MTF_SEARCH_RESULTS",
    ),
    "WALK_FORWARD_SKIPPED": (
        "skip_artifact",
        "AI_DELAYED_MTF_WALK_FORWARD_SKIPPED",
    ),
    "WALK_FORWARD_MASKS_FROZEN": (
        "masks_artifact",
        "AI_DELAYED_MTF_WALK_FORWARD_MASKS",
    ),
    "WALK_FORWARD_RESULTS_RELEASED": (
        "result_artifact",
        "AI_DELAYED_MTF_WALK_FORWARD_RESULTS",
    ),
    "HOLDOUT_SKIPPED": ("skip_artifact", "AI_DELAYED_MTF_HOLDOUT_SKIPPED"),
    "HOLDOUT_AUTHORIZED": (
        "authorization_artifact",
        "AI_DELAYED_MTF_HOLDOUT_AUTHORIZATION",
    ),
    "HOLDOUT_MASKS_FROZEN": (
        "masks_artifact",
        "AI_DELAYED_MTF_HOLDOUT_MASKS",
    ),
    "HOLDOUT_RESULTS_RELEASED": (
        "result_artifact",
        "AI_DELAYED_MTF_HOLDOUT_RESULTS",
    ),
    "COMPLETED": ("report_artifact", "AI_DELAYED_MTF_REPORT"),
}


class AIDelayedMTFRunError(RuntimeError):
    """The delayed multi-timeframe lifecycle or evidence failed closed."""


def _sha256(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _SHA256_LENGTH
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise AIDelayedMTFRunError(f"{label} is not a lowercase SHA-256")
    return value


def _json_document(value: object, *, label: str) -> dict[str, object]:
    candidate = value.as_dict() if hasattr(value, "as_dict") else value
    try:
        decoded = json.loads(canonical_json_bytes(candidate))
    except (TypeError, ValueError) as error:
        raise AIDelayedMTFRunError(f"{label} is not canonical JSON") from error
    if not isinstance(decoded, dict):
        raise AIDelayedMTFRunError(f"{label} must be a JSON object")
    return decoded


def _candidate_ids(value: object, *, label: str, maximum: int | None = None) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise AIDelayedMTFRunError(f"{label} must be an ordered list")
    parsed = tuple(_sha256(item, label=label) for item in value)
    if len(set(parsed)) != len(parsed):
        raise AIDelayedMTFRunError(f"{label} contains duplicate identities")
    if maximum is not None and len(parsed) > maximum:
        raise AIDelayedMTFRunError(f"{label} exceeds its frozen budget")
    return parsed


def _is_ordered_subsequence(
    child: Sequence[str],
    parent: Sequence[str],
) -> bool:
    """Require stage subsets to retain the frozen semantic catalog order."""

    positions = {candidate_id: index for index, candidate_id in enumerate(parent)}
    try:
        indexes = tuple(positions[candidate_id] for candidate_id in child)
    except KeyError:
        return False
    return indexes == tuple(sorted(indexes))


def _artifact_identity(value: object, *, role: str) -> ArtifactIdentity:
    try:
        identity = ArtifactIdentity.from_dict(value)
    except (TypeError, ValueError) as error:
        raise AIDelayedMTFRunError("artifact identity differs") from error
    if identity.artifact_type != role:
        raise AIDelayedMTFRunError("artifact role differs")
    return identity


@dataclass(frozen=True, slots=True)
class DelayedMTFLedgerEvent:
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
            raise AIDelayedMTFRunError("ledger sequence is invalid")
        if self.event_type not in _EVENT_TYPES:
            raise AIDelayedMTFRunError("ledger event type differs")
        _sha256(self.request_sha256, label="ledger request_sha256")
        if self.sequence == 1:
            if self.predecessor_sha256 is not None:
                raise AIDelayedMTFRunError("first event cannot have a predecessor")
        else:
            _sha256(self.predecessor_sha256, label="ledger predecessor_sha256")
        if not isinstance(self.recorded_at_utc, str) or not self.recorded_at_utc.endswith("Z"):
            raise AIDelayedMTFRunError("ledger timestamp is not explicit UTC")
        try:
            parsed_timestamp = datetime.fromisoformat(self.recorded_at_utc)
        except ValueError as error:
            raise AIDelayedMTFRunError("ledger timestamp is invalid") from error
        if parsed_timestamp.tzinfo is None or parsed_timestamp.utcoffset() != UTC.utcoffset(None):
            raise AIDelayedMTFRunError("ledger timestamp is not UTC")
        _json_document(dict(self.payload), label="ledger payload")

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": AI_DELAYED_MTF_EVENT_SCHEMA,
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


def _validate_event_payload(event: DelayedMTFLedgerEvent) -> None:
    expected = _EVENT_PAYLOAD_KEYS[event.event_type]
    if set(event.payload) != expected:
        raise AIDelayedMTFRunError("ledger event payload keys differ")
    artifact_role = _ARTIFACT_ROLES.get(event.event_type)
    if artifact_role is not None:
        artifact_field, role = artifact_role
        _artifact_identity(event.payload[artifact_field], role=role)
    for field in ("candidate_ids", "selected_candidate_ids", "finalist_candidate_ids"):
        if field in event.payload:
            maximum = {
                "selected_candidate_ids": MAXIMUM_SEARCH_SELECTION,
                "finalist_candidate_ids": MAXIMUM_HOLDOUT_FINALISTS,
            }.get(field)
            _candidate_ids(event.payload[field], label=field, maximum=maximum)
    if "fold_keys" in event.payload and tuple(event.payload["fold_keys"]) != (
        WALK_FORWARD_STAGE_KEYS
    ):
        raise AIDelayedMTFRunError("walk-forward fold order differs")
    if "family_sha256" in event.payload:
        _sha256(event.payload["family_sha256"], label="family_sha256")
    for field in ("classification", "final_status", "reason", "failure_code"):
        if field in event.payload and (
            not isinstance(event.payload[field], str) or not event.payload[field]
        ):
            raise AIDelayedMTFRunError(f"ledger {field} differs")
    if "classification" in event.payload and event.payload["classification"] not in (
        HOLDOUT_CLASSIFICATIONS
    ):
        raise AIDelayedMTFRunError("holdout classification is outside frozen vocabulary")
    if "final_status" in event.payload and event.payload["final_status"] not in FINAL_STATUSES:
        raise AIDelayedMTFRunError("terminal status is outside frozen vocabulary")


def _require_transition(prior: str | None, event_type: str) -> None:
    if event_type not in _TRANSITIONS or prior not in _TRANSITIONS[event_type]:
        raise AIDelayedMTFRunError(f"ledger transition {prior!r} -> {event_type!r} is invalid")


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise AIDelayedMTFRunError("immutable write made no progress")
        view = view[written:]


def _safe_directory(path: Path, *, create: bool) -> Path:
    if path.is_symlink():
        raise AIDelayedMTFRunError("run directory cannot be symbolic")
    if create:
        path.mkdir(parents=True, exist_ok=True)
    try:
        resolved = path.resolve(strict=True)
    except FileNotFoundError as error:
        raise AIDelayedMTFRunError("run directory does not exist") from error
    if not resolved.is_dir() or resolved != path.absolute():
        raise AIDelayedMTFRunError("run directory is unsafe")
    return resolved


def _project_root(value: Path | str) -> Path:
    requested = Path(value).expanduser()
    if requested.is_symlink():
        raise AIDelayedMTFRunError("project root cannot be symbolic")
    try:
        root = requested.resolve(strict=True)
    except FileNotFoundError as error:
        raise AIDelayedMTFRunError("project root does not exist") from error
    if not root.is_dir():
        raise AIDelayedMTFRunError("project root is not a directory")
    return root


def _fixed_run_root(project_root: Path, *, create: bool) -> Path:
    current = project_root
    for part in DEFAULT_AI_DELAYED_MTF_ROOT.parts:
        current = current / part
        if current.is_symlink():
            raise AIDelayedMTFRunError("run root has a symbolic component")
    return _safe_directory(project_root / DEFAULT_AI_DELAYED_MTF_ROOT, create=create)


@contextmanager
def _exclusive_mutation(run_root: Path) -> Iterator[None]:
    """Serialize every public mutation of the one fixed research budget."""

    path = run_root / ".mutation.lock"
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        metadata = os.fstat(descriptor)
        visible = path.stat(follow_symlinks=False)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or (metadata.st_dev, metadata.st_ino) != (visible.st_dev, visible.st_ino)
        ):
            raise AIDelayedMTFRunError("mutation lock is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise AIDelayedMTFRunError("another delayed-MTF writer is active") from error
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


class _Ledger:
    """Canonical predecessor-hashed, append-only, mode-0444 event ledger."""

    def __init__(self, root: Path, *, create: bool) -> None:
        self.root = _safe_directory(root, create=create)
        self.events_root = _safe_directory(self.root / "events", create=create)
        self.staging_root = _safe_directory(self.root / "staging", create=create)

    def verify(self) -> tuple[DelayedMTFLedgerEvent, ...]:
        paths: dict[int, Path] = {}
        for path in self.events_root.iterdir():
            suffix = path.name.removeprefix("event-").removesuffix(".json")
            if (
                path.is_symlink()
                or not path.is_file()
                or path.name != f"event-{suffix}.json"
                or len(suffix) != 8
                or not suffix.isdigit()
                or path.stat().st_mode & _WRITE_BITS
            ):
                raise AIDelayedMTFRunError("ledger contains an unsafe event")
            sequence = int(suffix)
            if sequence in paths:
                raise AIDelayedMTFRunError("ledger sequence is duplicated")
            paths[sequence] = path
        events: list[DelayedMTFLedgerEvent] = []
        predecessor: str | None = None
        request_sha256: str | None = None
        prior_type: str | None = None
        for expected, sequence in enumerate(sorted(paths), start=1):
            if sequence != expected:
                raise AIDelayedMTFRunError("ledger sequence is not contiguous")
            raw = paths[sequence].read_bytes()
            try:
                document = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise AIDelayedMTFRunError("ledger event is invalid JSON") from error
            if (
                not isinstance(document, dict)
                or canonical_json_bytes(document) != raw
                or set(document)
                != {
                    "artifact_schema",
                    "event_type",
                    "payload",
                    "predecessor_sha256",
                    "recorded_at_utc",
                    "request_sha256",
                    "sequence",
                }
                or document["artifact_schema"] != AI_DELAYED_MTF_EVENT_SCHEMA
            ):
                raise AIDelayedMTFRunError("ledger event schema or canonical bytes differ")
            event = DelayedMTFLedgerEvent(
                sequence=document["sequence"],
                predecessor_sha256=document["predecessor_sha256"],
                event_type=document["event_type"],
                request_sha256=document["request_sha256"],
                recorded_at_utc=document["recorded_at_utc"],
                payload=document["payload"],
            )
            _require_transition(prior_type, event.event_type)
            _validate_event_payload(event)
            if event.predecessor_sha256 != predecessor:
                raise AIDelayedMTFRunError("ledger predecessor chain differs")
            if request_sha256 is None:
                request_sha256 = event.request_sha256
            elif event.request_sha256 != request_sha256:
                raise AIDelayedMTFRunError("ledger contains multiple research requests")
            events.append(event)
            predecessor = event.sha256
            prior_type = event.event_type
        return tuple(events)

    def append(
        self,
        event_type: str,
        request_sha256: str,
        payload: Mapping[str, object],
    ) -> DelayedMTFLedgerEvent:
        events = self.verify()
        prior = events[-1].event_type if events else None
        _require_transition(prior, event_type)
        event = DelayedMTFLedgerEvent(
            sequence=len(events) + 1,
            predecessor_sha256=events[-1].sha256 if events else None,
            event_type=event_type,
            request_sha256=request_sha256,
            recorded_at_utc=datetime.now(UTC)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z"),
            payload=dict(payload),
        )
        _validate_event_payload(event)
        destination = self.events_root / f"event-{event.sequence:08d}.json"
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".event-{event.sequence:08d}-",
            suffix=".tmp",
            dir=self.staging_root,
        )
        temporary = Path(temporary_name)
        try:
            _write_all(descriptor, canonical_json_bytes(event.as_dict()))
            os.fsync(descriptor)
            os.fchmod(descriptor, 0o444)
            os.close(descriptor)
            descriptor = -1
            try:
                os.link(temporary, destination, follow_symlinks=False)
            except FileExistsError as error:
                raise AIDelayedMTFRunError("concurrent ledger append conflict") from error
            directory = os.open(self.events_root, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            temporary.unlink(missing_ok=True)
        verified = self.verify()
        if not verified or verified[-1].sha256 != event.sha256:
            raise AIDelayedMTFRunError("ledger append did not replay exactly")
        return event


@dataclass(frozen=True, slots=True)
class _StagePlan:
    stage_key: str
    decision_dates: tuple[date, ...]
    partitions: tuple[BarDatasetPartition, ...]
    reporting_groups: tuple[tuple[str, tuple[date, ...]], ...]

    def __post_init__(self) -> None:
        partition_dates = tuple(item.source_date for item in self.partitions)
        grouped_dates = tuple(
            source_date for _key, dates in self.reporting_groups for source_date in dates
        )
        if (
            self.stage_key not in {"SEARCH", *WALK_FORWARD_STAGE_KEYS, "HOLDOUT"}
            or not self.decision_dates
            or not self.partitions
            or partition_dates != tuple(sorted(set(partition_dates)))
            or self.decision_dates != tuple(sorted(set(self.decision_dates)))
            or not set(self.decision_dates).issubset(partition_dates)
            or tuple(sorted(grouped_dates)) != self.decision_dates
            or len(set(grouped_dates)) != len(grouped_dates)
        ):
            raise AIDelayedMTFRunError("stage plan is incomplete, overlapping, or unordered")

    @property
    def data_dates(self) -> tuple[date, ...]:
        return tuple(item.source_date for item in self.partitions)

    @property
    def group_by_date(self) -> dict[date, str]:
        return {source_date: key for key, dates in self.reporting_groups for source_date in dates}

    def identity_dict(self) -> dict[str, object]:
        return {
            "data_dates": [item.isoformat() for item in self.data_dates],
            "decision_dates": [item.isoformat() for item in self.decision_dates],
            "outcome_span_ids": sorted({item.outcome_span_id for item in self.partitions}),
            "reporting_groups": [
                {"dates": [item.isoformat() for item in dates], "group_key": key}
                for key, dates in self.reporting_groups
            ],
            "stage_key": self.stage_key,
        }


@dataclass(frozen=True, slots=True)
class _EvaluationInputs:
    dataset: LoadedBarDatasetManifest
    split: BarSplitPlan
    search: _StagePlan
    walk_forward: tuple[_StagePlan, ...]
    holdout: _StagePlan


def _dates_for_range(
    eligible: tuple[date, ...],
    value: BarDateRange,
) -> tuple[date, ...]:
    return eligible[value.start_active_ordinal - 1 : value.end_active_ordinal]


def _decision_dates(
    eligible: tuple[date, ...],
    value: BarDateRange,
) -> tuple[date, ...]:
    if value.decision_end_date is None:
        raise AIDelayedMTFRunError("evaluation range has no decision dates")
    end = eligible.index(value.decision_end_date) + 1
    return eligible[value.start_active_ordinal - 1 : end]


def _partitions_for_range(
    dataset: LoadedBarDatasetManifest,
    start_ordinal: int,
    end_ordinal: int,
) -> tuple[BarDatasetPartition, ...]:
    return dataset.partitions[start_ordinal - 1 : end_ordinal]


def _load_evaluation_inputs_default(
    project_root: Path,
    config: AIDelayedMTFConfig,
) -> _EvaluationInputs:
    """Reopen and verify the exact manifest/calendar/split before bar access."""

    contract = config.as_dict()
    dataset_contract = contract.get("dataset")
    if not isinstance(dataset_contract, dict):
        raise AIDelayedMTFRunError("config dataset contract is missing")
    dataset = load_bar_dataset_manifest(
        project_root / DATASET_MANIFEST_RELATIVE_PATH,
        expected_sha256=EXPECTED_DATASET_MANIFEST_SHA256,
    )
    eligible = dataset.eligible_active_dates
    calendar_sha256 = canonical_sha256([item.isoformat() for item in eligible])
    if (
        dataset.dataset_manifest_sha256 != EXPECTED_DATASET_MANIFEST_SHA256
        or dataset.handoff_sha256 != EXPECTED_DATASET_HANDOFF_SHA256
        or dataset.source_manifest_sha256 != EXPECTED_SOURCE_MANIFEST_SHA256
        or dataset.source_manifest_sha256 != BAR_SOURCE_MANIFEST_SHA256
        or dataset_contract.get("dataset_manifest_sha256") != EXPECTED_DATASET_MANIFEST_SHA256
        or dataset_contract.get("dataset_handoff_sha256") != EXPECTED_DATASET_HANDOFF_SHA256
        or dataset_contract.get("source_manifest_sha256") != EXPECTED_SOURCE_MANIFEST_SHA256
        or len(eligible) != 1413
        or eligible[0].isoformat() != "2022-01-03"
        or eligible[-1].isoformat() != "2026-07-31"
        or calendar_sha256 != "b414eae72afdb1c149977ff0ea5b672069380997d91e74adf0407e35836e8ac1"
    ):
        raise AIDelayedMTFRunError("bar manifest or bare active-calendar identity differs")
    split = plan_bar_splits(eligible)
    if (
        split.sha256 != EXPECTED_SPLIT_PLAN_SHA256
        or dataset_contract.get("split_plan_sha256") != EXPECTED_SPLIT_PLAN_SHA256
    ):
        raise AIDelayedMTFRunError("bar split plan differs from precommit")

    search = _StagePlan(
        stage_key="SEARCH",
        decision_dates=_decision_dates(eligible, split.discovery),
        partitions=_partitions_for_range(
            dataset,
            split.discovery.start_active_ordinal,
            split.discovery.end_active_ordinal,
        ),
        reporting_groups=tuple(
            (block.split_key, _dates_for_range(eligible, block))
            for block in split.discovery_reporting_blocks
        ),
    )
    walk_forward = tuple(
        _StagePlan(
            stage_key=f"WF{fold.fold_number}",
            decision_dates=_decision_dates(eligible, fold),
            partitions=_partitions_for_range(
                dataset,
                fold.start_active_ordinal,
                fold.end_active_ordinal,
            ),
            reporting_groups=((fold.split_key, _decision_dates(eligible, fold)),),
        )
        for fold in split.walk_forward_folds
    )
    if tuple(item.stage_key for item in walk_forward) != WALK_FORWARD_STAGE_KEYS:
        raise AIDelayedMTFRunError("walk-forward plan does not contain exact folds 1 through 5")
    holdout_decisions = _decision_dates(eligible, split.holdout)
    if len(holdout_decisions) != 120:
        raise AIDelayedMTFRunError("holdout decision calendar does not contain 120 dates")
    holdout = _StagePlan(
        stage_key="HOLDOUT",
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
    embargo_dates = set(_dates_for_range(eligible, split.embargo))
    if any(
        embargo_dates.intersection(plan.data_dates) for plan in (search, *walk_forward, holdout)
    ):
        raise AIDelayedMTFRunError("embargo partition leaked into an authorized stage")
    return _EvaluationInputs(dataset, split, search, walk_forward, holdout)


def _load_raw_stage_bars(
    project_root: Path,
    partitions: Sequence[BarDatasetPartition],
    timeframe_seconds: int,
) -> tuple[tuple[TradeBar, int], ...]:
    if timeframe_seconds not in {1, 300, 1800, 3600}:
        raise AIDelayedMTFRunError("adapter requested a timeframe outside the precommit")
    data_root = project_root / "data"
    if data_root.is_symlink() or not data_root.is_dir():
        raise AIDelayedMTFRunError("bar data root is missing or symbolic")
    output: list[tuple[TradeBar, int]] = []
    for partition in partitions:
        matches = tuple(
            artifact
            for artifact in partition.artifacts
            if artifact.timeframe_seconds == timeframe_seconds
        )
        if len(matches) != 1:
            raise AIDelayedMTFRunError("stage partition lacks one exact timeframe artifact")
        bars = load_trade_bar_artifact(
            data_root,
            matches[0],
            expected_plan_sha256=partition.plan_sha256,
            expected_source_sha256=partition.source_sha256,
            expected_source_date=partition.source_date,
        )
        if any(
            item.source_date != partition.source_date
            or item.contract != partition.contract
            or item.timeframe_seconds != timeframe_seconds
            for item in bars
        ):
            raise AIDelayedMTFRunError("loaded bars differ from manifest partition")
        output.extend((item, partition.outcome_span_id) for item in bars)
    return tuple(output)


def _raw_one_second_parts(
    project_root: Path,
    partitions: tuple[BarDatasetPartition, ...],
) -> Iterator[tuple[tuple[TradeBar, int], ...]]:
    """Yield one outcome span at a time; never retain a whole stage of 1s rows."""

    for _span, grouped in groupby(partitions, key=lambda item: item.outcome_span_id):
        yield _load_raw_stage_bars(project_root, tuple(grouped), 1)


def _engine_bars(
    raw: Sequence[tuple[TradeBar, int]],
) -> tuple[object, ...]:
    """Construct the new engine's lineage wrapper without importing its old evaluator."""

    from scripts import ai_delayed_mtf_engine as engine

    return tuple(engine.BarWithOutcomeSpan(bar, outcome_span_id) for bar, outcome_span_id in raw)


def _load_engine_stage_bars(
    project_root: Path,
    plan: _StagePlan,
    timeframe_seconds: int,
) -> tuple[object, ...]:
    return _engine_bars(_load_raw_stage_bars(project_root, plan.partitions, timeframe_seconds))


def _one_second_engine_parts(
    project_root: Path,
    plan: _StagePlan,
) -> Iterator[tuple[object, ...]]:
    for raw in _raw_one_second_parts(project_root, plan.partitions):
        yield _engine_bars(raw)


def _freeze_plan_default(
    project_root: Path,
    plan: _StagePlan,
    candidate_ids: tuple[str, ...],
) -> object:
    from scripts import ai_delayed_mtf_engine as engine

    fives = _load_engine_stage_bars(project_root, plan, 300)
    halves = _load_engine_stage_bars(project_root, plan, 1800)
    hours = _load_engine_stage_bars(project_root, plan, 3600)
    tail_end = max(item.bar.end_ns for item in fives)
    frozen = engine.freeze_delayed_mtf_stage_masks(
        plan.stage_key,
        fives,
        halves,
        hours,
        decision_dates=plan.decision_dates,
        allowed_stage_tail_end_ns=tail_end,
        seed="ai-delayed-mtf-v1",
        candidate_ids=candidate_ids,
        group_by_date=plan.group_by_date,
    )
    if tuple(frozen.candidate_ids) != candidate_ids:
        raise AIDelayedMTFRunError("engine mask candidates differ from requested semantic order")
    return frozen


def _frozen_stage_document(plan: _StagePlan, frozen: object) -> dict[str, object]:
    if not hasattr(frozen, "as_dict"):
        raise AIDelayedMTFRunError("engine frozen masks lack canonical serialization")
    return {
        "candidate_ids": list(frozen.candidate_ids),
        "engine_masks": _json_document(frozen.as_dict(), label="engine frozen masks"),
        "stage_key": plan.stage_key,
        "stage_plan": plan.identity_dict(),
    }


def _build_catalog_default(
    _project_root: Path,
    _config: AIDelayedMTFConfig,
) -> object:
    from scripts import ai_delayed_mtf_engine as engine

    return engine.build_delayed_mtf_candidate_catalog()


def _freeze_search_masks_default(
    project_root: Path,
    config: AIDelayedMTFConfig,
    candidate_ids: tuple[str, ...],
) -> object:
    inputs = _load_evaluation_inputs_default(project_root, config)
    frozen = _freeze_plan_default(project_root, inputs.search, candidate_ids)
    return _frozen_stage_document(inputs.search, frozen)


def _freeze_walk_forward_masks_default(
    project_root: Path,
    config: AIDelayedMTFConfig,
    candidate_ids: tuple[str, ...],
) -> object:
    inputs = _load_evaluation_inputs_default(project_root, config)
    folds = []
    for plan in inputs.walk_forward:
        frozen = _freeze_plan_default(project_root, plan, candidate_ids)
        folds.append(_frozen_stage_document(plan, frozen))
    return {
        "candidate_ids": list(candidate_ids),
        "fold_keys": list(WALK_FORWARD_STAGE_KEYS),
        "folds": folds,
        "schema": "systematic_fx.ai_delayed_mtf_walk_forward_mask_bundle.v1",
    }


def _freeze_holdout_masks_default(
    project_root: Path,
    config: AIDelayedMTFConfig,
    candidate_ids: tuple[str, ...],
) -> object:
    inputs = _load_evaluation_inputs_default(project_root, config)
    frozen = _freeze_plan_default(project_root, inputs.holdout, candidate_ids)
    return _frozen_stage_document(inputs.holdout, frozen)


def _reopen_frozen_plan_default(
    project_root: Path,
    plan: _StagePlan,
    candidate_ids: tuple[str, ...],
    recorded: Mapping[str, object],
) -> object:
    """Re-freeze feature-only masks and compare before constructing a 1s iterator."""

    frozen = _freeze_plan_default(project_root, plan, candidate_ids)
    rebuilt = _frozen_stage_document(plan, frozen)
    if canonical_json_bytes(rebuilt) != canonical_json_bytes(recorded):
        raise AIDelayedMTFRunError("persisted stage masks do not re-freeze exactly")
    return frozen


def _evaluate_plan_default(
    project_root: Path,
    plan: _StagePlan,
    candidate_ids: tuple[str, ...],
    recorded_masks: Mapping[str, object],
) -> object:
    from scripts import ai_delayed_mtf_engine as engine

    frozen = _reopen_frozen_plan_default(
        project_root,
        plan,
        candidate_ids,
        recorded_masks,
    )
    fives = _load_engine_stage_bars(project_root, plan, 300)
    tail_end = max(item.bar.end_ns for item in fives)
    # The generator is created only after the exact persisted mask comparison
    # above.  It yields one outcome span and releases it before loading the next.
    result = engine.evaluate_delayed_mtf_stage_parts(
        plan.stage_key,
        fives,
        _one_second_engine_parts(project_root, plan),
        frozen,
        allowed_stage_tail_end_ns=tail_end,
        reporting_dates=plan.decision_dates,
        group_by_date=plan.group_by_date,
    )
    result_ids = tuple(item.candidate.candidate_id for item in result.candidates)
    if result_ids != candidate_ids or result.mask_commitment_sha256 != frozen.commitment_sha256:
        raise AIDelayedMTFRunError("engine stage result lineage differs from frozen masks")
    return result


def _fraction_document(value: Fraction | None) -> dict[str, int] | None:
    if value is None:
        return None
    return {"denominator": value.denominator, "numerator": value.numerator}


def _compact_mask_evaluation(value: object | None) -> dict[str, object] | None:
    if value is None:
        return None
    trade_rows = [item.as_dict() for item in value.trades]
    censored_rows = [item.as_dict() for item in value.censored_signals]
    return {
        "candidate_id": value.candidate_id,
        "censored_signal_count": len(censored_rows),
        "censored_signals_sha256": canonical_sha256(censored_rows),
        "direction": value.direction,
        "mask_key": value.mask_key,
        "mask_role": value.mask_role,
        "summary": value.summary.as_dict(),
        "trade_count": len(trade_rows),
        "trades_sha256": canonical_sha256(trade_rows),
    }


def _compact_stage_result(value: object) -> dict[str, object]:
    candidates = []
    for item in value.candidates:
        candidates.append(
            {
                "candidate": item.candidate.as_dict(),
                "circular_shift": _compact_mask_evaluation(item.circular_shift),
                "conservative_p_value": _fraction_document(item.conservative_p_value),
                "ineligibility_reason": item.ineligibility_reason,
                "matched_random": _compact_mask_evaluation(item.matched_random),
                "p_vs_circular_shift": _fraction_document(item.p_vs_circular_shift),
                "p_vs_matched_random": _fraction_document(item.p_vs_matched_random),
                "p_vs_zero": _fraction_document(item.p_vs_zero),
                "raw_signal_count": item.raw_signal_count,
                "raw_signal_daily_counts": [
                    {"decision_date": day.isoformat(), "signal_count": count}
                    for day, count in item.raw_signal_daily_counts
                ],
                "raw_signal_group_counts": [
                    {"group_key": key, "signal_count": count}
                    for key, count in item.raw_signal_group_counts
                ],
                "real": _compact_mask_evaluation(item.real),
                "sample_eligible": item.sample_eligible,
            }
        )
    document = {
        "candidates": candidates,
        "mask_commitment_sha256": value.mask_commitment_sha256,
        "schema": "systematic_fx.ai_delayed_mtf_compact_stage_result.v1",
        "stage_key": value.stage_key,
    }
    document["compact_result_sha256"] = canonical_sha256(document)
    return document


def _maximum_drawdown(values: Sequence[int]) -> int:
    equity = 0
    peak = 0
    maximum = 0
    for value in values:
        equity += value
        peak = max(peak, equity)
        maximum = max(maximum, peak - equity)
    return maximum


def _profit_factor(values: Sequence[int]) -> Fraction | None:
    gains = sum(item for item in values if item > 0)
    losses = -sum(item for item in values if item < 0)
    return None if losses == 0 else Fraction(gains, losses)


def _profit_factor_at_least(
    values: Sequence[int],
    threshold: Fraction,
) -> bool:
    value = _profit_factor(values)
    return (value is None and bool(values) and sum(values) > 0) or (
        value is not None and value >= threshold
    )


def _candidate_result_map(stage_result: object) -> dict[str, object]:
    output = {item.candidate.candidate_id: item for item in stage_result.candidates}
    if len(output) != len(stage_result.candidates):
        raise AIDelayedMTFRunError("engine result candidate identity is duplicated")
    return output


def _bh_rejections(
    candidate_ids: tuple[str, ...],
    p_values: Mapping[str, Fraction],
    *,
    q: Fraction,
) -> tuple[str, ...]:
    ordered = sorted(
        candidate_ids,
        key=lambda candidate_id: (p_values[candidate_id], candidate_ids.index(candidate_id)),
    )
    rejection_count = 0
    family_count = len(candidate_ids)
    for rank, candidate_id in enumerate(ordered, start=1):
        if p_values[candidate_id] <= Fraction(rank, family_count) * q:
            rejection_count = rank
    rejected = set(ordered[:rejection_count])
    return tuple(candidate_id for candidate_id in candidate_ids if candidate_id in rejected)


def _holm_rejections(
    candidate_ids: tuple[str, ...],
    p_values: Mapping[str, Fraction],
    *,
    alpha: Fraction,
) -> tuple[str, ...]:
    ordered = sorted(
        candidate_ids,
        key=lambda candidate_id: (p_values[candidate_id], candidate_ids.index(candidate_id)),
    )
    rejected: set[str] = set()
    family_count = len(candidate_ids)
    for index, candidate_id in enumerate(ordered):
        if p_values[candidate_id] > alpha / (family_count - index):
            break
        rejected.add(candidate_id)
    return tuple(candidate_id for candidate_id in candidate_ids if candidate_id in rejected)


def _search_gate(
    item: object,
    expected_groups: tuple[str, ...],
) -> tuple[bool, tuple[str, ...], dict[str, object]]:
    reasons: list[str] = []
    raw_groups = dict(item.raw_signal_group_counts)
    raw_days = sum(count > 0 for _day, count in item.raw_signal_daily_counts)
    real = item.real.summary
    fill_groups = {summary.group_key: summary for summary in real.group_summaries}
    active_days = sum(count > 0 for _day, count in real.daily_trade_counts)
    net_values = [trade.fully_loaded_net_pnl_ticks for trade in item.real.trades]
    block_evs = tuple(
        (
            None
            if key not in fill_groups or fill_groups[key].trade_count == 0
            else fill_groups[key].mean_net_ticks
        )
        for key in expected_groups
    )
    positive_blocks = sum(
        key in fill_groups and fill_groups[key].total_net_ticks > 0 for key in expected_groups
    )
    if not item.sample_eligible:
        reasons.append("NULL_SAMPLE_INELIGIBLE")
    if item.raw_signal_count < 48:
        reasons.append("RAW_SIGNALS_LT_48")
    if raw_days < 30:
        reasons.append("RAW_SIGNAL_DAYS_LT_30")
    if any(raw_groups.get(key, 0) < 6 for key in expected_groups):
        reasons.append("REPORTING_BLOCK_RAW_SIGNALS_LT_6")
    if real.trade_count < 36:
        reasons.append("FILLS_LT_36")
    if active_days < 24:
        reasons.append("ACTIVE_ENTRY_DAYS_LT_24")
    if any(key not in fill_groups or fill_groups[key].trade_count < 4 for key in expected_groups):
        reasons.append("REPORTING_BLOCK_FILLS_LT_4")
    if positive_blocks < 3:
        reasons.append("POSITIVE_REPORTING_BLOCKS_LT_3")
    if any(value is None or value < -14 for value in block_evs):
        reasons.append("WORST_REPORTING_BLOCK_EV_LT_MINUS_14")
    if real.total_net_pnl_ticks <= 0:
        reasons.append("TOTAL_NET_NOT_POSITIVE")
    if not _profit_factor_at_least(net_values, Fraction(21, 20)):
        reasons.append("PROFIT_FACTOR_LT_1_05")
    if (
        item.circular_shift is None
        or item.matched_random is None
        or real.total_net_pnl_ticks <= item.circular_shift.summary.total_net_pnl_ticks
        or real.total_net_pnl_ticks <= item.matched_random.summary.total_net_pnl_ticks
    ):
        reasons.append("NULL_DELTA_NOT_POSITIVE")
    evidence = {
        "active_entry_days": active_days,
        "positive_reporting_blocks": positive_blocks,
        "profit_factor": _fraction_document(_profit_factor(net_values)),
        "raw_signal_days": raw_days,
        "worst_reporting_block_ev_ticks": _fraction_document(
            None if any(value is None for value in block_evs) else min(block_evs)
        ),
    }
    return not reasons, tuple(reasons), evidence


def _evaluate_search_default(
    project_root: Path,
    config: AIDelayedMTFConfig,
    candidate_ids: tuple[str, ...],
    masks: Mapping[str, object],
) -> object:
    inputs = _load_evaluation_inputs_default(project_root, config)
    stage = _evaluate_plan_default(
        project_root,
        inputs.search,
        candidate_ids,
        masks,
    )
    expected_groups = tuple(key for key, _dates in inputs.search.reporting_groups)
    return _select_search_stage(candidate_ids, stage, expected_groups)


def _select_search_stage(
    candidate_ids: tuple[str, ...],
    stage: object,
    expected_groups: tuple[str, ...],
) -> dict[str, object]:
    """Apply the frozen family-100 Search screen after full stage release."""

    if len(candidate_ids) != 100:
        raise AIDelayedMTFRunError("Search selector requires the exact family of 100")
    by_id = _candidate_result_map(stage)
    if tuple(item.candidate.candidate_id for item in stage.candidates) != candidate_ids:
        raise AIDelayedMTFRunError("Search result candidate order differs")
    p_values = {
        candidate_id: (
            Fraction(1, 1)
            if by_id[candidate_id].conservative_p_value is None
            else by_id[candidate_id].conservative_p_value
        )
        for candidate_id in candidate_ids
    }
    rejected = _bh_rejections(candidate_ids, p_values, q=Fraction(1, 20))
    rejected_set = set(rejected)
    decisions = []
    passing: list[object] = []
    for candidate_id in candidate_ids:
        item = by_id[candidate_id]
        economic_pass, reasons, evidence = _search_gate(item, expected_groups)
        significant = candidate_id in rejected_set
        if economic_pass and significant:
            passing.append(item)
        decisions.append(
            {
                "candidate_id": candidate_id,
                "economic_gate_pass": economic_pass,
                "evidence": evidence,
                "failure_reasons": list(reasons),
                "p_star": _fraction_document(p_values[candidate_id]),
                "selected_before_budget": economic_pass and significant,
                "significant_bh_q_0_05": significant,
            }
        )

    def ranking_key(item: object) -> tuple[object, ...]:
        real = item.real.summary
        group_evs = tuple(
            summary.mean_net_ticks
            for summary in real.group_summaries
            if summary.mean_net_ticks is not None
        )
        worst = min(group_evs) if group_evs else Fraction(-(10**18), 1)
        mean = real.mean_net_pnl_ticks or Fraction(-(10**18), 1)
        net_values = [trade.fully_loaded_net_pnl_ticks for trade in item.real.trades]
        profit_factor = _profit_factor(net_values)
        infinite_profit = profit_factor is None and sum(net_values) > 0
        return (
            p_values[item.candidate.candidate_id],
            -worst,
            -mean,
            0 if infinite_profit else 1,
            -(profit_factor or Fraction(0, 1)),
            item.candidate.selection_rank,
        )

    ranked = tuple(sorted(passing, key=ranking_key))
    budgeted = {item.candidate.candidate_id for item in ranked[:MAXIMUM_SEARCH_SELECTION]}
    selected = tuple(candidate_id for candidate_id in candidate_ids if candidate_id in budgeted)
    return {
        "candidate_ids": list(candidate_ids),
        "gate_decisions": decisions,
        "multiple_testing": {
            "family_count": len(candidate_ids),
            "method": "BENJAMINI_HOCHBERG",
            "q_denominator": 20,
            "q_numerator": 1,
            "rejected_candidate_ids": list(rejected),
        },
        "ranking_candidate_ids": [item.candidate.candidate_id for item in ranked],
        "schema": "systematic_fx.ai_delayed_mtf_search_selection.v1",
        "selected_candidate_ids": list(selected),
        "stage_result": _compact_stage_result(stage),
    }


def _median_fraction(values: Sequence[int]) -> Fraction | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return Fraction(ordered[middle], 1)
    return Fraction(ordered[middle - 1] + ordered[middle], 2)


def _daily_totals(evaluations: Sequence[object]) -> dict[date, int]:
    output: dict[date, int] = {}
    for evaluation in evaluations:
        for day, value in evaluation.summary.daily_net_ticks:
            if day in output:
                raise AIDelayedMTFRunError("walk-forward daily outcome date overlaps folds")
            output[day] = value
    return output


def _aggregate_p_star(items: Sequence[object]) -> Fraction:
    from scripts import ai_delayed_mtf_engine as engine

    real = _daily_totals([item.real for item in items])
    if any(item.circular_shift is None or item.matched_random is None for item in items):
        return Fraction(1, 1)
    circular = _daily_totals([item.circular_shift for item in items])
    matched = _daily_totals([item.matched_random for item in items])
    dates = tuple(sorted(set(real) | set(circular) | set(matched)))
    p_zero = engine.exact_one_sided_sign_test(real.get(day, 0) for day in dates)
    p_circular = engine.exact_one_sided_sign_test(
        real.get(day, 0) - circular.get(day, 0) for day in dates
    )
    p_matched = engine.exact_one_sided_sign_test(
        real.get(day, 0) - matched.get(day, 0) for day in dates
    )
    return max(p_zero, p_circular, p_matched)


def _walk_forward_gate(
    items: Sequence[object],
) -> tuple[bool, tuple[str, ...], dict[str, object]]:
    reasons: list[str] = []
    fold_trades = [tuple(item.real.trades) for item in items]
    all_trades = tuple(
        sorted(
            (trade for trades in fold_trades for trade in trades),
            key=lambda trade: (trade.entry_ns, trade.signal_index),
        )
    )
    net_values = [trade.fully_loaded_net_pnl_ticks for trade in all_trades]
    fold_nets = [
        sum(trade.fully_loaded_net_pnl_ticks for trade in trades) for trades in fold_trades
    ]
    fold_days = [
        sum(count > 0 for _day, count in item.real.summary.daily_trade_counts) for item in items
    ]
    active_days = sum(fold_days)
    contracts = {trade.contract for trade in all_trades}
    positive_folds = sum(value > 0 for value in fold_nets)
    drawdown = _maximum_drawdown(net_values)
    positive_net_folds = [value for value in fold_nets if value > 0]
    losing_net_folds = [-value for value in fold_nets if value < 0]
    median_positive = _median_fraction(positive_net_folds)
    if any(not item.sample_eligible for item in items):
        reasons.append("NULL_SAMPLE_INELIGIBLE")
    if len(all_trades) < 100:
        reasons.append("FILLS_LT_100")
    if active_days < 75:
        reasons.append("ACTIVE_ENTRY_DAYS_LT_75")
    if len(contracts) < 5:
        reasons.append("CONTRACTS_LT_5")
    if any(len(trades) < 12 for trades in fold_trades):
        reasons.append("FOLD_FILLS_LT_12")
    if any(value < 10 for value in fold_days):
        reasons.append("FOLD_ACTIVE_ENTRY_DAYS_LT_10")
    if positive_folds < 4:
        reasons.append("POSITIVE_FOLDS_LT_4")
    if sum(net_values) <= 0:
        reasons.append("TOTAL_NET_NOT_POSITIVE")
    if not _profit_factor_at_least(net_values, Fraction(11, 10)):
        reasons.append("PROFIT_FACTOR_LT_1_10")
    if drawdown <= 0:
        if sum(net_values) <= 0:
            reasons.append("NET_OVER_MAX_DRAWDOWN_LT_1")
    elif Fraction(sum(net_values), drawdown) < 1:
        reasons.append("NET_OVER_MAX_DRAWDOWN_LT_1")
    if any(
        not _profit_factor_at_least(
            [trade.fully_loaded_net_pnl_ticks for trade in trades], Fraction(7, 10)
        )
        for trades in fold_trades
    ):
        reasons.append("WORST_FOLD_PROFIT_FACTOR_LT_0_70")
    if losing_net_folds and (
        median_positive is None
        or Fraction(max(losing_net_folds), 1) > Fraction(3, 2) * median_positive
    ):
        reasons.append("WORST_FOLD_LOSS_GT_1_5_MEDIAN_POSITIVE")
    circular_net = sum(
        item.circular_shift.summary.total_net_pnl_ticks
        for item in items
        if item.circular_shift is not None
    )
    matched_net = sum(
        item.matched_random.summary.total_net_pnl_ticks
        for item in items
        if item.matched_random is not None
    )
    if (
        any(item.circular_shift is None or item.matched_random is None for item in items)
        or sum(net_values) <= circular_net
        or sum(net_values) <= matched_net
    ):
        reasons.append("NULL_DELTA_NOT_POSITIVE")
    fold_evs = tuple(
        Fraction(value, len(trades)) if trades else None
        for value, trades in zip(fold_nets, fold_trades, strict=True)
    )
    evidence = {
        "active_entry_days": active_days,
        "contract_count": len(contracts),
        "maximum_drawdown_ticks": drawdown,
        "positive_fold_count": positive_folds,
        "profit_factor": _fraction_document(_profit_factor(net_values)),
        "total_net_ticks": sum(net_values),
        "worst_fold_ev_ticks": _fraction_document(
            None if any(value is None for value in fold_evs) else min(fold_evs)
        ),
    }
    return not reasons, tuple(reasons), evidence


def _walk_forward_mask_documents(
    masks: Mapping[str, object],
) -> dict[str, Mapping[str, object]]:
    folds = masks.get("folds")
    if not isinstance(folds, list) or len(folds) != len(WALK_FORWARD_STAGE_KEYS):
        raise AIDelayedMTFRunError("persisted walk-forward mask bundle differs")
    output: dict[str, Mapping[str, object]] = {}
    for value in folds:
        if not isinstance(value, dict) or value.get("stage_key") not in WALK_FORWARD_STAGE_KEYS:
            raise AIDelayedMTFRunError("walk-forward mask fold is invalid")
        output[str(value["stage_key"])] = value
    if tuple(output) != WALK_FORWARD_STAGE_KEYS:
        raise AIDelayedMTFRunError("walk-forward mask fold order differs")
    return output


def _evaluate_walk_forward_default(
    project_root: Path,
    config: AIDelayedMTFConfig,
    candidate_ids: tuple[str, ...],
    masks: Mapping[str, object],
    search: Mapping[str, object],
) -> object:
    if tuple(search.get("selected_candidate_ids", ())) != candidate_ids:
        raise AIDelayedMTFRunError("Search selection differs before WF evaluation")
    inputs = _load_evaluation_inputs_default(project_root, config)
    mask_documents = _walk_forward_mask_documents(masks)
    stages = tuple(
        _evaluate_plan_default(
            project_root,
            plan,
            candidate_ids,
            mask_documents[plan.stage_key],
        )
        for plan in inputs.walk_forward
    )
    return _select_walk_forward_stages(candidate_ids, stages)


def _select_walk_forward_stages(
    candidate_ids: tuple[str, ...],
    stages: Sequence[object],
) -> dict[str, object]:
    """Select at most three finalists only after all five WF folds exist."""

    if not 1 <= len(candidate_ids) <= MAXIMUM_SEARCH_SELECTION:
        raise AIDelayedMTFRunError("WF selector family must contain one through eight rows")
    if (
        len(stages) != len(WALK_FORWARD_STAGE_KEYS)
        or tuple(stage.stage_key for stage in stages) != WALK_FORWARD_STAGE_KEYS
    ):
        raise AIDelayedMTFRunError("WF selector requires all five folds in frozen order")
    if any(
        tuple(item.candidate.candidate_id for item in stage.candidates) != candidate_ids
        for stage in stages
    ):
        raise AIDelayedMTFRunError("WF result candidate order differs")
    maps = tuple(_candidate_result_map(stage) for stage in stages)
    p_values = {
        candidate_id: _aggregate_p_star([candidate_map[candidate_id] for candidate_map in maps])
        for candidate_id in candidate_ids
    }
    rejected = _bh_rejections(candidate_ids, p_values, q=Fraction(1, 20))
    rejected_set = set(rejected)
    decisions = []
    passing: list[tuple[str, Sequence[object]]] = []
    for candidate_id in candidate_ids:
        items = [candidate_map[candidate_id] for candidate_map in maps]
        economic_pass, reasons, evidence = _walk_forward_gate(items)
        significant = candidate_id in rejected_set
        if economic_pass and significant:
            passing.append((candidate_id, items))
        decisions.append(
            {
                "candidate_id": candidate_id,
                "economic_gate_pass": economic_pass,
                "evidence": evidence,
                "failure_reasons": list(reasons),
                "p_star": _fraction_document(p_values[candidate_id]),
                "selected_before_budget": economic_pass and significant,
                "significant_bh_q_0_05": significant,
            }
        )

    def ranking_key(value: tuple[str, Sequence[object]]) -> tuple[object, ...]:
        candidate_id, items = value
        trades_by_fold = [tuple(item.real.trades) for item in items]
        values_by_fold = [
            [trade.fully_loaded_net_pnl_ticks for trade in trades] for trades in trades_by_fold
        ]
        fold_evs = [
            Fraction(sum(values), len(values)) if values else Fraction(-(10**18), 1)
            for values in values_by_fold
        ]
        net_values = [value for values in values_by_fold for value in values]
        aggregate_ev = (
            Fraction(sum(net_values), len(net_values)) if net_values else Fraction(-(10**18), 1)
        )
        profit_factor = _profit_factor(net_values)
        infinite_profit = profit_factor is None and sum(net_values) > 0
        semantic_rank = items[0].candidate.selection_rank
        return (
            p_values[candidate_id],
            -min(fold_evs),
            -aggregate_ev,
            0 if infinite_profit else 1,
            -(profit_factor or Fraction(0, 1)),
            semantic_rank,
        )

    ranked = tuple(sorted(passing, key=ranking_key))
    budgeted = {candidate_id for candidate_id, _items in ranked[:MAXIMUM_HOLDOUT_FINALISTS]}
    finalists = tuple(candidate_id for candidate_id in candidate_ids if candidate_id in budgeted)
    return {
        "candidate_ids": list(candidate_ids),
        "finalist_candidate_ids": list(finalists),
        "fold_keys": list(WALK_FORWARD_STAGE_KEYS),
        "fold_results": [_compact_stage_result(stage) for stage in stages],
        "gate_decisions": decisions,
        "multiple_testing": {
            "family_count": len(candidate_ids),
            "method": "BENJAMINI_HOCHBERG",
            "q_denominator": 20,
            "q_numerator": 1,
            "rejected_candidate_ids": list(rejected),
        },
        "ranking_candidate_ids": [candidate_id for candidate_id, _items in ranked],
        "schema": "systematic_fx.ai_delayed_mtf_walk_forward_selection.v1",
    }


def _holdout_gate(
    item: object,
    expected_halves: tuple[str, ...],
) -> tuple[bool, tuple[str, ...], dict[str, object]]:
    reasons: list[str] = []
    trades = tuple(item.real.trades)
    net_values = [trade.fully_loaded_net_pnl_ticks for trade in trades]
    active_days = sum(count > 0 for _day, count in item.real.summary.daily_trade_counts)
    contracts = {trade.contract for trade in trades}
    group_map = {summary.group_key: summary for summary in item.real.summary.group_summaries}
    drawdown = _maximum_drawdown(net_values)
    if not item.sample_eligible:
        reasons.append("NULL_SAMPLE_INELIGIBLE")
    if len(trades) < 24:
        reasons.append("FILLS_LT_24")
    if active_days < 18:
        reasons.append("ACTIVE_ENTRY_DAYS_LT_18")
    if len(contracts) < 2:
        reasons.append("CONTRACTS_LT_2")
    if any(key not in group_map or group_map[key].total_net_ticks <= 0 for key in expected_halves):
        reasons.append("BOTH_HOLDOUT_HALVES_NET_NOT_POSITIVE")
    if sum(net_values) <= 0:
        reasons.append("TOTAL_NET_NOT_POSITIVE")
    if not _profit_factor_at_least(net_values, Fraction(11, 10)):
        reasons.append("PROFIT_FACTOR_LT_1_10")
    if drawdown <= 0:
        if sum(net_values) <= 0:
            reasons.append("NET_OVER_MAX_DRAWDOWN_LT_0_75")
    elif Fraction(sum(net_values), drawdown) < Fraction(3, 4):
        reasons.append("NET_OVER_MAX_DRAWDOWN_LT_0_75")
    if (
        item.circular_shift is None
        or item.matched_random is None
        or sum(net_values) <= item.circular_shift.summary.total_net_pnl_ticks
        or sum(net_values) <= item.matched_random.summary.total_net_pnl_ticks
    ):
        reasons.append("NULL_DELTA_NOT_POSITIVE")
    evidence = {
        "active_entry_days": active_days,
        "contract_count": len(contracts),
        "maximum_drawdown_ticks": drawdown,
        "profit_factor": _fraction_document(_profit_factor(net_values)),
        "total_net_ticks": sum(net_values),
    }
    return not reasons, tuple(reasons), evidence


def _evaluate_holdout_default(
    project_root: Path,
    config: AIDelayedMTFConfig,
    candidate_ids: tuple[str, ...],
    masks: Mapping[str, object],
) -> object:
    inputs = _load_evaluation_inputs_default(project_root, config)
    stage = _evaluate_plan_default(
        project_root,
        inputs.holdout,
        candidate_ids,
        masks,
    )
    expected_halves = tuple(key for key, _dates in inputs.holdout.reporting_groups)
    return _select_holdout_stage(candidate_ids, stage, expected_halves)


def _select_holdout_stage(
    candidate_ids: tuple[str, ...],
    stage: object,
    expected_halves: tuple[str, ...],
) -> dict[str, object]:
    """Publish every Holm verdict; PASS requires significance and all gates."""

    if not 1 <= len(candidate_ids) <= MAXIMUM_HOLDOUT_FINALISTS:
        raise AIDelayedMTFRunError("holdout selector family must contain one through three rows")
    by_id = _candidate_result_map(stage)
    if tuple(item.candidate.candidate_id for item in stage.candidates) != candidate_ids:
        raise AIDelayedMTFRunError("holdout result candidate order differs")
    p_values = {
        candidate_id: (
            Fraction(1, 1)
            if by_id[candidate_id].conservative_p_value is None
            else by_id[candidate_id].conservative_p_value
        )
        for candidate_id in candidate_ids
    }
    rejected = _holm_rejections(candidate_ids, p_values, alpha=Fraction(1, 20))
    rejected_set = set(rejected)
    decisions = []
    passing: list[str] = []
    for candidate_id in candidate_ids:
        item = by_id[candidate_id]
        economic_pass, reasons, evidence = _holdout_gate(item, expected_halves)
        significant = candidate_id in rejected_set
        if economic_pass and significant:
            passing.append(candidate_id)
        decisions.append(
            {
                "candidate_id": candidate_id,
                "economic_gate_pass": economic_pass,
                "evidence": evidence,
                "failure_reasons": list(reasons),
                "holm_rejected_alpha_0_05": significant,
                "p_star": _fraction_document(p_values[candidate_id]),
                "verdict_pass": economic_pass and significant,
            }
        )
    classification = (
        "ONE_SHOT_UNSEALED_DELAYED_MTF_HOLDOUT_DIAGNOSTIC_PASS"
        if passing
        else "ONE_SHOT_UNSEALED_DELAYED_MTF_HOLDOUT_DIAGNOSTIC_FAIL"
    )
    return {
        "candidate_ids": list(candidate_ids),
        "classification": classification,
        "gate_decisions": decisions,
        "multiple_testing": {
            "alpha_denominator": 20,
            "alpha_numerator": 1,
            "family_count": len(candidate_ids),
            "method": "HOLM_STEP_DOWN",
            "rejected_candidate_ids": list(rejected),
        },
        "passing_candidate_ids": passing,
        "schema": "systematic_fx.ai_delayed_mtf_holdout_selection.v1",
        "stage_result": _compact_stage_result(stage),
    }


@dataclass(frozen=True, slots=True)
class _DelayedMTFServices:
    """Private seam; production services are fixed by ``_default_services``."""

    build_catalog: Callable[[Path, AIDelayedMTFConfig], object]
    freeze_search_masks: Callable[[Path, AIDelayedMTFConfig, tuple[str, ...]], object]
    evaluate_search: Callable[
        [Path, AIDelayedMTFConfig, tuple[str, ...], Mapping[str, object]], object
    ]
    freeze_walk_forward_masks: Callable[[Path, AIDelayedMTFConfig, tuple[str, ...]], object]
    evaluate_walk_forward: Callable[
        [
            Path,
            AIDelayedMTFConfig,
            tuple[str, ...],
            Mapping[str, object],
            Mapping[str, object],
        ],
        object,
    ]
    freeze_holdout_masks: Callable[[Path, AIDelayedMTFConfig, tuple[str, ...]], object]
    evaluate_holdout: Callable[
        [Path, AIDelayedMTFConfig, tuple[str, ...], Mapping[str, object]], object
    ]


@dataclass(frozen=True, slots=True)
class AIDelayedMTFRun:
    config: AIDelayedMTFConfig
    status: str
    request_artifact: ArtifactIdentity
    evidence_artifacts: tuple[ArtifactIdentity, ...]
    finalist_candidate_ids: tuple[str, ...]
    root: Path
    event_count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "authority": AI_DELAYED_MTF_AUTHORITY,
            "config_file_sha256": self.config.file_sha256,
            "config_semantic_sha256": self.config.semantic_sha256,
            "database_mutated": False,
            "event_count": self.event_count,
            "evidence_artifacts": [item.as_dict() for item in self.evidence_artifacts],
            "finalist_candidate_ids": list(self.finalist_candidate_ids),
            "network_accessed": False,
            "paper_live_or_promotion_authority": False,
            "physical_holdout_isolation": False,
            "request_artifact": self.request_artifact.as_dict(),
            "schema": AI_DELAYED_MTF_RUN_SCHEMA,
            "status": self.status,
            "strict_backtest_claim": False,
            "strict_sealed_holdout_claim": False,
        }


def _catalog_identity(
    config: AIDelayedMTFConfig,
    services: _DelayedMTFServices,
    root: Path,
) -> tuple[dict[str, object], tuple[str, ...]]:
    catalog = _json_document(services.build_catalog(root, config), label="candidate catalog")
    if not {"candidate_ids", "catalog_sha256", "candidates"}.issubset(catalog):
        raise AIDelayedMTFRunError("candidate catalog identity fields are missing")
    identifiers = _candidate_ids(catalog["candidate_ids"], label="candidate catalog IDs")
    candidates = catalog["candidates"]
    if (
        not isinstance(candidates, list)
        or any(not isinstance(item, dict) for item in candidates)
        or tuple(item.get("candidate_id") for item in candidates) != identifiers
        or tuple(item.get("selection_rank") for item in candidates)
        != tuple(range(1, len(identifiers) + 1))
    ):
        raise AIDelayedMTFRunError("candidate catalog semantic rank lineage differs")
    expected = config.as_dict()["catalog"]
    if not isinstance(expected, dict):  # pragma: no cover - config validation guarantees it
        raise AIDelayedMTFRunError("config catalog is not an object")
    if (
        len(identifiers) != expected["candidate_count"]
        or catalog["catalog_sha256"] != expected["candidate_catalog_sha256"]
        or canonical_sha256(candidates) != catalog["catalog_sha256"]
    ):
        raise AIDelayedMTFRunError("engine candidate catalog differs from precommit")
    return catalog, identifiers


def _request_document(
    config: AIDelayedMTFConfig,
    catalog: Mapping[str, object],
) -> dict[str, object]:
    return {
        "artifact_schema": "systematic_fx.ai_delayed_mtf_request.v1",
        "authority": AI_DELAYED_MTF_AUTHORITY,
        "catalog": dict(catalog),
        "config": config.as_dict(),
        "config_file_sha256": config.file_sha256,
        "config_semantic_sha256": config.semantic_sha256,
        "limitations": [
            "SEARCH_IS_RETROSPECTIVE_DEVELOPMENT_DIAGNOSTIC",
            "WALK_FORWARD_IS_FIRST_OOS_EVIDENCE",
            "LOCAL_HOLDOUT_BYTES_ARE_NOT_PHYSICALLY_SEALED",
            "NO_BID_ASK_FILL_PROOF",
            "NO_LEGACY_NEXT_5M_TP_SL_EXECUTION_LOGIC",
            "NO_PAPER_LIVE_OR_PROMOTION_AUTHORITY",
        ],
    }


def _envelope(
    config: AIDelayedMTFConfig,
    *,
    schema: str,
    payload: object,
) -> dict[str, object]:
    return {
        "artifact_schema": schema,
        "authority": AI_DELAYED_MTF_AUTHORITY,
        "config_semantic_sha256": config.semantic_sha256,
        "payload": _json_document(payload, label=schema),
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


def _payload_from_artifact(
    artifacts_root: Path,
    identity: ArtifactIdentity,
    *,
    expected_schema: str,
    config: AIDelayedMTFConfig,
) -> dict[str, object]:
    raw = verify_immutable_artifact(artifacts_root, identity)
    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AIDelayedMTFRunError("immutable artifact is invalid JSON") from error
    if (
        not isinstance(document, dict)
        or canonical_json_bytes(document) != raw
        or set(document) != {"artifact_schema", "authority", "config_semantic_sha256", "payload"}
        or document["artifact_schema"] != expected_schema
        or document["authority"] != AI_DELAYED_MTF_AUTHORITY
        or document["config_semantic_sha256"] != config.semantic_sha256
        or not isinstance(document["payload"], dict)
    ):
        raise AIDelayedMTFRunError("immutable stage artifact envelope differs")
    return document["payload"]


def _artifact_from_event(event: DelayedMTFLedgerEvent) -> ArtifactIdentity:
    artifact_role = _ARTIFACT_ROLES.get(event.event_type)
    if artifact_role is None:
        raise AIDelayedMTFRunError("ledger event has no artifact identity")
    field, role = artifact_role
    return _artifact_identity(event.payload[field], role=role)


def _status(events: Sequence[DelayedMTFLedgerEvent]) -> str:
    return events[-1].event_type if events else "NOT_PRECOMMITTED"


def _run_value(
    config: AIDelayedMTFConfig,
    run_root: Path,
    events: Sequence[DelayedMTFLedgerEvent],
) -> AIDelayedMTFRun:
    if not events:
        raise AIDelayedMTFRunError("run has not been precommitted")
    artifacts = tuple(
        _artifact_from_event(event) for event in events if event.event_type in _ARTIFACT_ROLES
    )
    finalists: tuple[str, ...] = ()
    for event in events:
        if "finalist_candidate_ids" in event.payload:
            finalists = _candidate_ids(
                event.payload["finalist_candidate_ids"],
                label="finalist_candidate_ids",
                maximum=MAXIMUM_HOLDOUT_FINALISTS,
            )
    return AIDelayedMTFRun(
        config=config,
        status=_status(events),
        request_artifact=artifacts[0],
        evidence_artifacts=artifacts,
        finalist_candidate_ids=finalists,
        root=run_root,
        event_count=len(events),
    )


def _terminal_run_value_without_services(
    config: AIDelayedMTFConfig,
    run_root: Path,
    artifacts_root: Path,
    events: Sequence[DelayedMTFLedgerEvent],
) -> AIDelayedMTFRun | None:
    """Replay an already terminal campaign without reopening any engine input."""

    if not events or events[-1].event_type not in {"COMPLETED", "FAILED"}:
        return None
    identity = _artifact_from_event(events[0])
    raw = verify_immutable_artifact(artifacts_root, identity)
    try:
        request = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AIDelayedMTFRunError("terminal request artifact is invalid JSON") from error
    if (
        not isinstance(request, dict)
        or canonical_json_bytes(request) != raw
        or request.get("artifact_schema") != "systematic_fx.ai_delayed_mtf_request.v1"
        or request.get("authority") != AI_DELAYED_MTF_AUTHORITY
        or request.get("config_file_sha256") != config.file_sha256
        or request.get("config_semantic_sha256") != config.semantic_sha256
        or canonical_sha256(request) != events[0].request_sha256
    ):
        raise AIDelayedMTFRunError("terminal request identity differs")
    return _run_value(config, run_root, events)


def _find_event(
    events: Sequence[DelayedMTFLedgerEvent], event_type: str
) -> DelayedMTFLedgerEvent | None:
    return next((event for event in events if event.event_type == event_type), None)


def _artifact_payload_for_event(
    artifacts_root: Path,
    events: Sequence[DelayedMTFLedgerEvent],
    event_type: str,
    *,
    schema: str,
    config: AIDelayedMTFConfig,
) -> dict[str, object]:
    event = _find_event(events, event_type)
    if event is None:
        raise AIDelayedMTFRunError(f"{event_type} evidence is missing")
    return _payload_from_artifact(
        artifacts_root,
        _artifact_from_event(event),
        expected_schema=schema,
        config=config,
    )


def _append_precommit(
    root: Path,
    config: AIDelayedMTFConfig,
    run_root: Path,
    ledger: _Ledger,
    artifacts_root: Path,
    services: _DelayedMTFServices,
) -> tuple[DelayedMTFLedgerEvent, ...]:
    events = ledger.verify()
    catalog, _ = _catalog_identity(config, services, root)
    request = _request_document(config, catalog)
    request_sha256 = canonical_sha256(request)
    if events:
        if events[0].request_sha256 != request_sha256:
            raise AIDelayedMTFRunError("existing run belongs to another precommit")
        identity = _artifact_from_event(events[0])
        verify_immutable_artifact(
            artifacts_root,
            identity,
            expected_bytes=canonical_json_bytes(request),
        )
        return events
    identity = _publish(
        artifacts_root,
        artifact_type="AI_DELAYED_MTF_REQUEST",
        filename_prefix="delayed-mtf-request",
        document=request,
    )
    ledger.append("PRECOMMITTED", request_sha256, {"request_artifact": identity.as_dict()})
    return ledger.verify()


def _run_with_services(
    root: Path,
    config: AIDelayedMTFConfig,
    run_root: Path,
    services: _DelayedMTFServices,
    *,
    stop_after: str | None = None,
) -> AIDelayedMTFRun:
    if stop_after is not None and stop_after not in _EVENT_TYPES:
        raise AIDelayedMTFRunError("private stop_after event differs")
    artifacts_root = _safe_directory(run_root / "artifacts", create=True)
    ledger = _Ledger(run_root / "ledger", create=True)
    terminal = _terminal_run_value_without_services(
        config,
        run_root,
        artifacts_root,
        ledger.verify(),
    )
    if terminal is not None:
        return terminal
    events = _append_precommit(root, config, run_root, ledger, artifacts_root, services)
    request_sha256 = events[0].request_sha256
    if events[-1].event_type in {"COMPLETED", "FAILED"}:
        return _run_value(config, run_root, events)
    if stop_after == events[-1].event_type:
        return _run_value(config, run_root, events)

    catalog, all_candidates = _catalog_identity(config, services, root)
    del catalog

    if _find_event(events, "SEARCH_MASKS_FROZEN") is None:
        search_masks = _json_document(
            services.freeze_search_masks(root, config, all_candidates),
            label="Search masks",
        )
        if (
            search_masks.get("stage_key") != "SEARCH"
            or _candidate_ids(search_masks.get("candidate_ids"), label="Search mask candidate IDs")
            != all_candidates
        ):
            raise AIDelayedMTFRunError("all 100 Search masks were not frozen together")
        envelope = _envelope(
            config,
            schema="systematic_fx.ai_delayed_mtf_search_masks.v1",
            payload=search_masks,
        )
        identity = _publish(
            artifacts_root,
            artifact_type="AI_DELAYED_MTF_SEARCH_MASKS",
            filename_prefix="search-masks",
            document=envelope,
        )
        ledger.append(
            "SEARCH_MASKS_FROZEN",
            request_sha256,
            {"candidate_ids": list(all_candidates), "masks_artifact": identity.as_dict()},
        )
        events = ledger.verify()
    search_masks = _artifact_payload_for_event(
        artifacts_root,
        events,
        "SEARCH_MASKS_FROZEN",
        schema="systematic_fx.ai_delayed_mtf_search_masks.v1",
        config=config,
    )
    if stop_after == events[-1].event_type:
        return _run_value(config, run_root, events)

    if _find_event(events, "SEARCH_RESULTS_RELEASED") is None:
        # No Search one-second path may be opened before the immutable mask
        # event above.  Search is development-only and cannot establish OOS.
        search_payload = _json_document(
            services.evaluate_search(root, config, all_candidates, search_masks),
            label="Search results",
        )
        result_candidates = _candidate_ids(
            search_payload.get("candidate_ids"), label="Search result candidate IDs"
        )
        selected = _candidate_ids(
            search_payload.get("selected_candidate_ids", ()),
            label="Search selected candidate IDs",
            maximum=MAXIMUM_SEARCH_SELECTION,
        )
        if result_candidates != all_candidates or not _is_ordered_subsequence(
            selected, all_candidates
        ):
            raise AIDelayedMTFRunError("Search all-results release differs from frozen catalog")
        envelope = _envelope(
            config,
            schema="systematic_fx.ai_delayed_mtf_search_results.v1",
            payload=search_payload,
        )
        identity = _publish(
            artifacts_root,
            artifact_type="AI_DELAYED_MTF_SEARCH_RESULTS",
            filename_prefix="search-results",
            document=envelope,
        )
        ledger.append(
            "SEARCH_RESULTS_RELEASED",
            request_sha256,
            {
                "result_artifact": identity.as_dict(),
                "selected_candidate_ids": list(selected),
            },
        )
        events = ledger.verify()
    search_payload = _artifact_payload_for_event(
        artifacts_root,
        events,
        "SEARCH_RESULTS_RELEASED",
        schema="systematic_fx.ai_delayed_mtf_search_results.v1",
        config=config,
    )
    selected = _candidate_ids(
        search_payload.get("selected_candidate_ids", ()),
        label="Search selected candidate IDs",
        maximum=MAXIMUM_SEARCH_SELECTION,
    )
    if stop_after == events[-1].event_type:
        return _run_value(config, run_root, events)

    if not selected:
        if _find_event(events, "WALK_FORWARD_SKIPPED") is None:
            skip = _envelope(
                config,
                schema="systematic_fx.ai_delayed_mtf_walk_forward_skip.v1",
                payload={"reason": "NO_SEARCH_FINALISTS", "walk_forward_bytes_opened": False},
            )
            identity = _publish(
                artifacts_root,
                artifact_type="AI_DELAYED_MTF_WALK_FORWARD_SKIPPED",
                filename_prefix="walk-forward-skipped",
                document=skip,
            )
            ledger.append(
                "WALK_FORWARD_SKIPPED",
                request_sha256,
                {"reason": "NO_SEARCH_FINALISTS", "skip_artifact": identity.as_dict()},
            )
            events = ledger.verify()
        if stop_after == events[-1].event_type:
            return _run_value(config, run_root, events)
        finalists: tuple[str, ...] = ()
        terminal_status = "NO_SEARCH_FINALISTS_HOLDOUT_NOT_OPENED"
    else:
        if _find_event(events, "WALK_FORWARD_MASKS_FROZEN") is None:
            # This is the first authorized access to unopened WF feature bars.
            # All five folds are frozen together before any WF one-second path.
            walk_masks = _json_document(
                services.freeze_walk_forward_masks(root, config, selected),
                label="walk-forward masks",
            )
            if (
                tuple(walk_masks.get("fold_keys", ())) != WALK_FORWARD_STAGE_KEYS
                or _candidate_ids(
                    walk_masks.get("candidate_ids"), label="walk-forward mask candidate IDs"
                )
                != selected
            ):
                raise AIDelayedMTFRunError("five-fold masks differ from frozen Search selection")
            envelope = _envelope(
                config,
                schema="systematic_fx.ai_delayed_mtf_walk_forward_masks.v1",
                payload=walk_masks,
            )
            identity = _publish(
                artifacts_root,
                artifact_type="AI_DELAYED_MTF_WALK_FORWARD_MASKS",
                filename_prefix="walk-forward-masks",
                document=envelope,
            )
            ledger.append(
                "WALK_FORWARD_MASKS_FROZEN",
                request_sha256,
                {
                    "candidate_ids": list(selected),
                    "fold_keys": list(WALK_FORWARD_STAGE_KEYS),
                    "masks_artifact": identity.as_dict(),
                },
            )
            events = ledger.verify()
        walk_masks = _artifact_payload_for_event(
            artifacts_root,
            events,
            "WALK_FORWARD_MASKS_FROZEN",
            schema="systematic_fx.ai_delayed_mtf_walk_forward_masks.v1",
            config=config,
        )
        if stop_after == events[-1].event_type:
            return _run_value(config, run_root, events)

        if _find_event(events, "WALK_FORWARD_RESULTS_RELEASED") is None:
            walk_payload = _json_document(
                services.evaluate_walk_forward(
                    root,
                    config,
                    selected,
                    walk_masks,
                    search_payload,
                ),
                label="walk-forward results",
            )
            if tuple(walk_payload.get("fold_keys", ())) != WALK_FORWARD_STAGE_KEYS:
                raise AIDelayedMTFRunError("five walk-forward folds were not atomically released")
            evaluated = _candidate_ids(
                walk_payload.get("candidate_ids"), label="walk-forward candidate IDs"
            )
            finalists = _candidate_ids(
                walk_payload.get("finalist_candidate_ids", ()),
                label="walk-forward finalists",
                maximum=MAXIMUM_HOLDOUT_FINALISTS,
            )
            if evaluated != selected or not _is_ordered_subsequence(finalists, selected):
                raise AIDelayedMTFRunError("walk-forward candidates changed after mask freeze")
            envelope = _envelope(
                config,
                schema="systematic_fx.ai_delayed_mtf_walk_forward_results.v1",
                payload=walk_payload,
            )
            identity = _publish(
                artifacts_root,
                artifact_type="AI_DELAYED_MTF_WALK_FORWARD_RESULTS",
                filename_prefix="walk-forward-results",
                document=envelope,
            )
            ledger.append(
                "WALK_FORWARD_RESULTS_RELEASED",
                request_sha256,
                {
                    "finalist_candidate_ids": list(finalists),
                    "result_artifact": identity.as_dict(),
                },
            )
            events = ledger.verify()
        walk_payload = _artifact_payload_for_event(
            artifacts_root,
            events,
            "WALK_FORWARD_RESULTS_RELEASED",
            schema="systematic_fx.ai_delayed_mtf_walk_forward_results.v1",
            config=config,
        )
        finalists = _candidate_ids(
            walk_payload.get("finalist_candidate_ids", ()),
            label="walk-forward finalists",
            maximum=MAXIMUM_HOLDOUT_FINALISTS,
        )
        if stop_after == events[-1].event_type:
            return _run_value(config, run_root, events)
        terminal_status = "NO_WALK_FORWARD_FINALISTS_HOLDOUT_NOT_OPENED"

    if not finalists:
        if _find_event(events, "HOLDOUT_SKIPPED") is None:
            reason = "NO_SEARCH_FINALISTS" if not selected else "NO_WALK_FORWARD_FINALISTS"
            skip = _envelope(
                config,
                schema="systematic_fx.ai_delayed_mtf_holdout_skip.v1",
                payload={
                    "final_status": terminal_status,
                    "holdout_bytes_opened": False,
                    "reason": reason,
                },
            )
            identity = _publish(
                artifacts_root,
                artifact_type="AI_DELAYED_MTF_HOLDOUT_SKIPPED",
                filename_prefix="holdout-skipped",
                document=skip,
            )
            ledger.append(
                "HOLDOUT_SKIPPED",
                request_sha256,
                {
                    "final_status": terminal_status,
                    "reason": reason,
                    "skip_artifact": identity.as_dict(),
                },
            )
            events = ledger.verify()
        if stop_after == events[-1].event_type:
            return _run_value(config, run_root, events)
    else:
        if _find_event(events, "HOLDOUT_AUTHORIZED") is None:
            authorization = {
                "artifact_schema": "systematic_fx.ai_delayed_mtf_holdout_authorization.v1",
                "authority": AI_DELAYED_MTF_AUTHORITY,
                "config_semantic_sha256": config.semantic_sha256,
                "family": {
                    "candidate_count": len(finalists),
                    "candidate_ids": list(finalists),
                    "family_sha256": canonical_sha256(list(finalists)),
                    "maximum_candidate_count": MAXIMUM_HOLDOUT_FINALISTS,
                },
                "open_order": "AUTHORIZATION_BEFORE_ANY_HOLDOUT_BAR_OR_OUTCOME_BYTES",
                "one_shot": True,
            }
            identity = _publish(
                artifacts_root,
                artifact_type="AI_DELAYED_MTF_HOLDOUT_AUTHORIZATION",
                filename_prefix="holdout-authorization",
                document=authorization,
            )
            ledger.append(
                "HOLDOUT_AUTHORIZED",
                request_sha256,
                {
                    "authorization_artifact": identity.as_dict(),
                    "family_sha256": canonical_sha256(list(finalists)),
                    "finalist_candidate_ids": list(finalists),
                },
            )
            events = ledger.verify()
        if stop_after == events[-1].event_type:
            return _run_value(config, run_root, events)

        if _find_event(events, "HOLDOUT_MASKS_FROZEN") is None:
            holdout_masks = _json_document(
                services.freeze_holdout_masks(root, config, finalists),
                label="holdout masks",
            )
            mask_candidates = _candidate_ids(
                holdout_masks.get("candidate_ids"), label="holdout mask candidate IDs"
            )
            if mask_candidates != finalists:
                raise AIDelayedMTFRunError("holdout masks differ from one-shot authorization")
            envelope = _envelope(
                config,
                schema="systematic_fx.ai_delayed_mtf_holdout_masks.v1",
                payload=holdout_masks,
            )
            identity = _publish(
                artifacts_root,
                artifact_type="AI_DELAYED_MTF_HOLDOUT_MASKS",
                filename_prefix="holdout-masks",
                document=envelope,
            )
            ledger.append(
                "HOLDOUT_MASKS_FROZEN",
                request_sha256,
                {"candidate_ids": list(finalists), "masks_artifact": identity.as_dict()},
            )
            events = ledger.verify()
        holdout_masks = _artifact_payload_for_event(
            artifacts_root,
            events,
            "HOLDOUT_MASKS_FROZEN",
            schema="systematic_fx.ai_delayed_mtf_holdout_masks.v1",
            config=config,
        )
        if stop_after == events[-1].event_type:
            return _run_value(config, run_root, events)

        if _find_event(events, "HOLDOUT_RESULTS_RELEASED") is None:
            holdout_payload = _json_document(
                services.evaluate_holdout(root, config, finalists, holdout_masks),
                label="holdout results",
            )
            evaluated = _candidate_ids(
                holdout_payload.get("candidate_ids"), label="holdout result candidate IDs"
            )
            classification = holdout_payload.get("classification")
            if evaluated != finalists or classification not in HOLDOUT_CLASSIFICATIONS:
                raise AIDelayedMTFRunError("holdout result differs from authorization")
            envelope = _envelope(
                config,
                schema="systematic_fx.ai_delayed_mtf_holdout_results.v1",
                payload=holdout_payload,
            )
            identity = _publish(
                artifacts_root,
                artifact_type="AI_DELAYED_MTF_HOLDOUT_RESULTS",
                filename_prefix="holdout-results",
                document=envelope,
            )
            ledger.append(
                "HOLDOUT_RESULTS_RELEASED",
                request_sha256,
                {"classification": classification, "result_artifact": identity.as_dict()},
            )
            events = ledger.verify()
        holdout_payload = _artifact_payload_for_event(
            artifacts_root,
            events,
            "HOLDOUT_RESULTS_RELEASED",
            schema="systematic_fx.ai_delayed_mtf_holdout_results.v1",
            config=config,
        )
        terminal_status = str(holdout_payload["classification"])
        if stop_after == events[-1].event_type:
            return _run_value(config, run_root, events)

    if _find_event(events, "COMPLETED") is None:
        evidence = [
            _artifact_from_event(event).as_dict()
            for event in events
            if event.event_type in _ARTIFACT_ROLES
        ]
        report = {
            "artifact_schema": "systematic_fx.ai_delayed_mtf_report.v1",
            "authority": AI_DELAYED_MTF_AUTHORITY,
            "config_semantic_sha256": config.semantic_sha256,
            "evidence_artifacts": evidence,
            "final_status": terminal_status,
            "finalist_candidate_ids": list(finalists),
            "holdout_bytes_opened": bool(finalists),
            "limitations": [
                "SEARCH_IS_RETROSPECTIVE_DEVELOPMENT_DIAGNOSTIC",
                "WALK_FORWARD_IS_FIRST_OOS_EVIDENCE",
                "UNSEALED_LOCAL_BAR_HOLDOUT_DIAGNOSTIC",
                "NO_LEGACY_NEXT_5M_TP_SL_EXECUTION_LOGIC",
                "NO_STRICT_BACKTEST_OR_PROMOTION_CLAIM",
            ],
        }
        identity = _publish(
            artifacts_root,
            artifact_type="AI_DELAYED_MTF_REPORT",
            filename_prefix="delayed-mtf-report",
            document=report,
        )
        ledger.append(
            "COMPLETED",
            request_sha256,
            {"final_status": terminal_status, "report_artifact": identity.as_dict()},
        )
        events = ledger.verify()
    return _run_value(config, run_root, events)


def _default_services() -> _DelayedMTFServices:
    """Bind production orchestration only to the new delayed-MTF engine API."""

    return _DelayedMTFServices(
        build_catalog=_build_catalog_default,
        freeze_search_masks=_freeze_search_masks_default,
        evaluate_search=_evaluate_search_default,
        freeze_walk_forward_masks=_freeze_walk_forward_masks_default,
        evaluate_walk_forward=_evaluate_walk_forward_default,
        freeze_holdout_masks=_freeze_holdout_masks_default,
        evaluate_holdout=_evaluate_holdout_default,
    )


def _precommit_with_services(
    project_root: Path | str,
    services: _DelayedMTFServices,
) -> AIDelayedMTFRun:
    root = _project_root(project_root)
    config = load_ai_delayed_mtf_config(root)
    run_root = _fixed_run_root(root, create=True)
    with _exclusive_mutation(run_root):
        artifacts_root = _safe_directory(run_root / "artifacts", create=True)
        ledger = _Ledger(run_root / "ledger", create=True)
        events = _append_precommit(
            root,
            config,
            run_root,
            ledger,
            artifacts_root,
            services,
        )
        return _run_value(config, run_root, events)


def precommit_ai_delayed_mtf(project_root: Path | str) -> AIDelayedMTFRun:
    """Create or replay only the immutable PRECOMMITTED event."""

    return _precommit_with_services(project_root, _default_services())


def run_ai_delayed_mtf(project_root: Path | str) -> AIDelayedMTFRun:
    """Resume the one fixed campaign through its one-shot terminal report."""

    root = _project_root(project_root)
    config = load_ai_delayed_mtf_config(root)
    run_root = _fixed_run_root(root, create=True)
    with _exclusive_mutation(run_root):
        try:
            return _run_with_services(root, config, run_root, _default_services())
        except OSError:
            # A transient filesystem failure leaves the last durable event
            # resumable and must not consume the campaign.
            raise
        except Exception as error:
            # Once PRECOMMITTED exists, every non-OS engine, data, artifact, or
            # orchestration failure is terminal.  Config/root/lock failures
            # occur outside this protected mutation boundary.
            try:
                ledger = _Ledger(run_root / "ledger", create=False)
                events = ledger.verify()
                if events and events[-1].event_type not in {"COMPLETED", "FAILED"}:
                    ledger.append(
                        "FAILED",
                        events[0].request_sha256,
                        {"failure_code": type(error).__name__},
                    )
            except Exception as ledger_error:
                raise AIDelayedMTFRunError(
                    "integrity failure could not be recorded safely"
                ) from ledger_error
            if not events:
                raise
            if isinstance(error, AIDelayedMTFRunError):
                raise
            raise AIDelayedMTFRunError(
                "runtime engine, data, or artifact integrity failure"
            ) from error


def _verify_with_services(
    project_root: Path | str,
    services: _DelayedMTFServices,
    *,
    recompute: bool,
    config_override: AIDelayedMTFConfig | None = None,
) -> AIDelayedMTFRun:
    """Read-only replay; optionally recompute every frozen mask and result."""

    root = _project_root(project_root)
    config = load_ai_delayed_mtf_config(root) if config_override is None else config_override
    run_root = _fixed_run_root(root, create=False)
    artifacts_root = _safe_directory(run_root / "artifacts", create=False)
    ledger = _Ledger(run_root / "ledger", create=False)
    events = ledger.verify()
    if not events:
        raise AIDelayedMTFRunError("run has not been precommitted")

    catalog, all_candidates = _catalog_identity(config, services, root)
    request = _request_document(config, catalog)
    if events[0].request_sha256 != canonical_sha256(request):
        raise AIDelayedMTFRunError("request identity differs on fresh replay")
    verify_immutable_artifact(
        artifacts_root,
        _artifact_from_event(events[0]),
        expected_bytes=canonical_json_bytes(request),
    )
    for event in events[1:]:
        if event.event_type in _ARTIFACT_ROLES:
            verify_immutable_artifact(artifacts_root, _artifact_from_event(event))

    search_masks: dict[str, object] = {}
    if _find_event(events, "SEARCH_MASKS_FROZEN") is not None:
        search_masks = _artifact_payload_for_event(
            artifacts_root,
            events,
            "SEARCH_MASKS_FROZEN",
            schema="systematic_fx.ai_delayed_mtf_search_masks.v1",
            config=config,
        )
        if (
            search_masks.get("stage_key") != "SEARCH"
            or _candidate_ids(search_masks.get("candidate_ids"), label="Search mask replay")
            != all_candidates
        ):
            raise AIDelayedMTFRunError("Search mask catalog differs on replay")
        if recompute:
            rebuilt = _json_document(
                services.freeze_search_masks(root, config, all_candidates),
                label="recomputed Search masks",
            )
            if canonical_json_bytes(rebuilt) != canonical_json_bytes(search_masks):
                raise AIDelayedMTFRunError("Search masks do not recompute exactly")

    recorded_search: dict[str, object] = {}
    if _find_event(events, "SEARCH_RESULTS_RELEASED") is not None:
        recorded_search = _artifact_payload_for_event(
            artifacts_root,
            events,
            "SEARCH_RESULTS_RELEASED",
            schema="systematic_fx.ai_delayed_mtf_search_results.v1",
            config=config,
        )
        selected = _candidate_ids(
            recorded_search.get("selected_candidate_ids", ()),
            label="Search selection replay",
            maximum=MAXIMUM_SEARCH_SELECTION,
        )
        if recompute:
            rebuilt = _json_document(
                services.evaluate_search(root, config, all_candidates, search_masks),
                label="recomputed Search results",
            )
            if canonical_json_bytes(rebuilt) != canonical_json_bytes(recorded_search):
                raise AIDelayedMTFRunError("Search results do not recompute exactly")
    else:
        selected = ()

    walk_masks: dict[str, object] = {}
    if _find_event(events, "WALK_FORWARD_MASKS_FROZEN") is not None:
        walk_masks = _artifact_payload_for_event(
            artifacts_root,
            events,
            "WALK_FORWARD_MASKS_FROZEN",
            schema="systematic_fx.ai_delayed_mtf_walk_forward_masks.v1",
            config=config,
        )
        if (
            tuple(walk_masks.get("fold_keys", ())) != WALK_FORWARD_STAGE_KEYS
            or _candidate_ids(walk_masks.get("candidate_ids"), label="walk-forward mask replay")
            != selected
        ):
            raise AIDelayedMTFRunError("walk-forward masks differ on replay")
        if recompute:
            rebuilt = _json_document(
                services.freeze_walk_forward_masks(root, config, selected),
                label="recomputed walk-forward masks",
            )
            if canonical_json_bytes(rebuilt) != canonical_json_bytes(walk_masks):
                raise AIDelayedMTFRunError("walk-forward masks do not recompute exactly")

    recorded_walk: dict[str, object] = {}
    if _find_event(events, "WALK_FORWARD_RESULTS_RELEASED") is not None:
        recorded_walk = _artifact_payload_for_event(
            artifacts_root,
            events,
            "WALK_FORWARD_RESULTS_RELEASED",
            schema="systematic_fx.ai_delayed_mtf_walk_forward_results.v1",
            config=config,
        )
        finalists = _candidate_ids(
            recorded_walk.get("finalist_candidate_ids", ()),
            label="walk-forward finalist replay",
            maximum=MAXIMUM_HOLDOUT_FINALISTS,
        )
        if recompute:
            rebuilt = _json_document(
                services.evaluate_walk_forward(
                    root,
                    config,
                    selected,
                    walk_masks,
                    recorded_search,
                ),
                label="recomputed walk-forward results",
            )
            if canonical_json_bytes(rebuilt) != canonical_json_bytes(recorded_walk):
                raise AIDelayedMTFRunError("walk-forward results do not recompute exactly")
    else:
        finalists = ()

    authorization_event = _find_event(events, "HOLDOUT_AUTHORIZED")
    if authorization_event is not None:
        authorization = {
            "artifact_schema": "systematic_fx.ai_delayed_mtf_holdout_authorization.v1",
            "authority": AI_DELAYED_MTF_AUTHORITY,
            "config_semantic_sha256": config.semantic_sha256,
            "family": {
                "candidate_count": len(finalists),
                "candidate_ids": list(finalists),
                "family_sha256": canonical_sha256(list(finalists)),
                "maximum_candidate_count": MAXIMUM_HOLDOUT_FINALISTS,
            },
            "open_order": "AUTHORIZATION_BEFORE_ANY_HOLDOUT_BAR_OR_OUTCOME_BYTES",
            "one_shot": True,
        }
        verify_immutable_artifact(
            artifacts_root,
            _artifact_from_event(authorization_event),
            expected_bytes=canonical_json_bytes(authorization),
        )

    holdout_masks: dict[str, object] = {}
    if _find_event(events, "HOLDOUT_MASKS_FROZEN") is not None:
        holdout_masks = _artifact_payload_for_event(
            artifacts_root,
            events,
            "HOLDOUT_MASKS_FROZEN",
            schema="systematic_fx.ai_delayed_mtf_holdout_masks.v1",
            config=config,
        )
        if (
            _candidate_ids(holdout_masks.get("candidate_ids"), label="holdout mask replay")
            != finalists
        ):
            raise AIDelayedMTFRunError("holdout masks differ on replay")
        if recompute:
            rebuilt = _json_document(
                services.freeze_holdout_masks(root, config, finalists),
                label="recomputed holdout masks",
            )
            if canonical_json_bytes(rebuilt) != canonical_json_bytes(holdout_masks):
                raise AIDelayedMTFRunError("holdout masks do not recompute exactly")

    recorded_holdout: dict[str, object] = {}
    if _find_event(events, "HOLDOUT_RESULTS_RELEASED") is not None:
        recorded_holdout = _artifact_payload_for_event(
            artifacts_root,
            events,
            "HOLDOUT_RESULTS_RELEASED",
            schema="systematic_fx.ai_delayed_mtf_holdout_results.v1",
            config=config,
        )
        if recompute:
            rebuilt = _json_document(
                services.evaluate_holdout(root, config, finalists, holdout_masks),
                label="recomputed holdout results",
            )
            if canonical_json_bytes(rebuilt) != canonical_json_bytes(recorded_holdout):
                raise AIDelayedMTFRunError("holdout results do not recompute exactly")

    completed = _find_event(events, "COMPLETED")
    if completed is not None:
        final_status = str(completed.payload["final_status"])
        evidence = [
            _artifact_from_event(event).as_dict()
            for event in events
            if event.event_type in _ARTIFACT_ROLES and event.event_type != "COMPLETED"
        ]
        expected_report = {
            "artifact_schema": "systematic_fx.ai_delayed_mtf_report.v1",
            "authority": AI_DELAYED_MTF_AUTHORITY,
            "config_semantic_sha256": config.semantic_sha256,
            "evidence_artifacts": evidence,
            "final_status": final_status,
            "finalist_candidate_ids": list(finalists),
            "holdout_bytes_opened": bool(recorded_holdout),
            "limitations": [
                "SEARCH_IS_RETROSPECTIVE_DEVELOPMENT_DIAGNOSTIC",
                "WALK_FORWARD_IS_FIRST_OOS_EVIDENCE",
                "UNSEALED_LOCAL_BAR_HOLDOUT_DIAGNOSTIC",
                "NO_LEGACY_NEXT_5M_TP_SL_EXECUTION_LOGIC",
                "NO_STRICT_BACKTEST_OR_PROMOTION_CLAIM",
            ],
        }
        verify_immutable_artifact(
            artifacts_root,
            _artifact_from_event(completed),
            expected_bytes=canonical_json_bytes(expected_report),
        )
    return _run_value(config, run_root, events)


def verify_ai_delayed_mtf(project_root: Path | str) -> AIDelayedMTFRun:
    """Fresh, read-only verification with deterministic result recomputation."""

    return _verify_with_services(project_root, _default_services(), recompute=True)


__all__ = [
    "AI_DELAYED_MTF_EVENT_SCHEMA",
    "AI_DELAYED_MTF_RUN_SCHEMA",
    "DEFAULT_AI_DELAYED_MTF_ROOT",
    "FINAL_STATUSES",
    "HOLDOUT_CLASSIFICATIONS",
    "MAXIMUM_HOLDOUT_FINALISTS",
    "MAXIMUM_SEARCH_SELECTION",
    "WALK_FORWARD_STAGE_KEYS",
    "AIDelayedMTFRun",
    "AIDelayedMTFRunError",
    "DelayedMTFLedgerEvent",
    "precommit_ai_delayed_mtf",
    "run_ai_delayed_mtf",
    "verify_ai_delayed_mtf",
]
