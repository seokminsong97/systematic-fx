"""Crash-resumable lifecycle for the all-cases v1 research campaign.

This module owns authorization and evidence publication, not statistical
logic.  Production services are fixed privately.  The injectable seam is
private and exists only so lifecycle tests can prove that outcome-bearing
payloads cannot be opened before their feature/event commitments exist.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import resource
import stat
import sys
import tempfile
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from fractions import Fraction
from math import comb
from pathlib import Path
from typing import Final

from .config import (
    AI_ALL_CASES_AUTHORITY,
    AI_ALL_CASES_CONFIG_ID,
    AI_ALL_CASES_CONFIG_RELATIVE_PATH,
    AI_ALL_CASES_RUN_RELATIVE_ROOT,
    AllCasesConfig,
    _load_validated_dataset_contract,
    _require_trusted_bootstrap_runtime,
    _runtime_identity_document,
    load_ai_all_cases_config,
    verify_failed_attempt2_predecessor,
    verify_failed_attempt3_predecessor,
    verify_failed_attempt4_predecessor,
    verify_failed_predecessor_attempt,
)

AI_ALL_CASES_RUN_SCHEMA: Final = "systematic_fx.ai_all_cases_run.v1"
AI_ALL_CASES_EVENT_SCHEMA: Final = "systematic_fx.ai_all_cases_event.v1"
DEFAULT_AI_ALL_CASES_ROOT: Final = AI_ALL_CASES_RUN_RELATIVE_ROOT
WALK_FORWARD_FOLD_KEYS: Final = ("WF1", "WF2", "WF3", "WF4", "WF5")
MAXIMUM_SEARCH_SELECTION: Final = 12
MAXIMUM_HOLDOUT_FINALISTS: Final = 3
HOLDOUT_CLASSIFICATIONS: Final = (
    "ONE_SHOT_UNSEALED_ALL_CASES_HOLDOUT_DIAGNOSTIC_PASS",
    "ONE_SHOT_UNSEALED_ALL_CASES_HOLDOUT_DIAGNOSTIC_FAIL",
    "ONE_SHOT_UNSEALED_ALL_CASES_HOLDOUT_DIAGNOSTIC_INCONCLUSIVE",
)
FINAL_STATUSES: Final = (
    "NO_SEARCH_FINALISTS_HOLDOUT_NOT_OPENED",
    "NO_WALK_FORWARD_FINALISTS_HOLDOUT_NOT_OPENED",
    *HOLDOUT_CLASSIFICATIONS,
)
_WRITE_BITS: Final = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
_SHA256_LENGTH: Final = 64
_OUTER_EVENT_BYTES_MAXIMUM: Final = 16_384
_FAILURE_CODE: Final = re.compile(r"INTEGRITY_[0-9A-F]{24}")
_SEARCH_UNIVERSE_PAYLOAD_SCHEMA: Final = "systematic_fx.ai_all_cases_search_universe_payload.v1"
_SEARCH_RESULT_PAYLOAD_SCHEMA: Final = "systematic_fx.ai_all_cases_search_result_payload.v1"
_WALK_MASKS_PAYLOAD_SCHEMA: Final = "systematic_fx.ai_all_cases_walk_forward_masks_payload.v1"
_WALK_RESULT_PAYLOAD_SCHEMA: Final = "systematic_fx.ai_all_cases_walk_forward_result_payload.v1"
_HOLDOUT_MASKS_PAYLOAD_SCHEMA: Final = "systematic_fx.ai_all_cases_holdout_masks_payload.v1"
_HOLDOUT_RESULT_PAYLOAD_SCHEMA: Final = "systematic_fx.ai_all_cases_holdout_result_payload.v1"
_EVENT_TYPES: Final = (
    "PRECOMMITTED",
    "SEARCH_UNIVERSE_FROZEN",
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
    "SEARCH_UNIVERSE_FROZEN": {"PRECOMMITTED"},
    "SEARCH_RESULTS_RELEASED": {"SEARCH_UNIVERSE_FROZEN"},
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
    "SEARCH_UNIVERSE_FROZEN": {"universe_artifact", "universe_root_sha256"},
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
    "HOLDOUT_RESULTS_RELEASED": {"classification", "result_artifact"},
    "COMPLETED": {"final_status", "report_artifact"},
    "FAILED": {"failure_code"},
}
_ARTIFACT_ROLES: Final = {
    "PRECOMMITTED": ("request_artifact", "AI_ALL_CASES_REQUEST"),
    "SEARCH_UNIVERSE_FROZEN": (
        "universe_artifact",
        "AI_ALL_CASES_SEARCH_FEATURE_EVENT_UNIVERSE",
    ),
    "SEARCH_RESULTS_RELEASED": ("result_artifact", "AI_ALL_CASES_SEARCH_RESULTS"),
    "WALK_FORWARD_SKIPPED": ("skip_artifact", "AI_ALL_CASES_WALK_FORWARD_SKIPPED"),
    "WALK_FORWARD_MASKS_FROZEN": (
        "masks_artifact",
        "AI_ALL_CASES_WALK_FORWARD_MASKS",
    ),
    "WALK_FORWARD_RESULTS_RELEASED": (
        "result_artifact",
        "AI_ALL_CASES_WALK_FORWARD_RESULTS",
    ),
    "HOLDOUT_SKIPPED": ("skip_artifact", "AI_ALL_CASES_HOLDOUT_SKIPPED"),
    "HOLDOUT_AUTHORIZED": (
        "authorization_artifact",
        "AI_ALL_CASES_HOLDOUT_AUTHORIZATION",
    ),
    "HOLDOUT_MASKS_FROZEN": ("masks_artifact", "AI_ALL_CASES_HOLDOUT_MASKS"),
    "HOLDOUT_RESULTS_RELEASED": ("result_artifact", "AI_ALL_CASES_HOLDOUT_RESULTS"),
    "COMPLETED": ("report_artifact", "AI_ALL_CASES_REPORT"),
}
_OUTER_ARTIFACT_FILENAME_PREFIXES: Final = frozenset(
    {
        "all-cases-report",
        "all-cases-request",
        "holdout-authorization",
        "holdout-masks",
        "holdout-results",
        "holdout-skipped",
        "search-results",
        "search-universe",
        "walk-forward-masks",
        "walk-forward-results",
        "walk-forward-skipped",
    }
)


class AllCasesRunError(RuntimeError):
    """The governed all-cases run cannot safely proceed."""


class AllCasesIntegrityError(AllCasesRunError):
    """A deterministic contract or lineage invariant failed."""


class _RunResourceGuard:
    """Fail closed at every governed phase boundary on frozen resource caps."""

    def __init__(
        self,
        config: AllCasesConfig,
        run_root: Path,
        *,
        verifier: bool,
    ) -> None:
        caps = config.as_dict().get("compute_caps")
        if not isinstance(caps, dict):
            raise AllCasesIntegrityError("compute-cap contract is absent")
        wall_key = "verifier_wall_seconds_maximum" if verifier else "search_wall_seconds_maximum"
        try:
            self.artifact_bytes_maximum = int(caps["artifact_bytes_maximum"])
            self.resident_set_bytes_maximum = int(caps["resident_set_bytes_maximum"])
            self.wall_seconds_maximum = int(caps[wall_key])
        except (KeyError, TypeError, ValueError) as error:
            raise AllCasesIntegrityError("compute-cap values differ") from error
        if (
            min(
                self.artifact_bytes_maximum,
                self.resident_set_bytes_maximum,
                self.wall_seconds_maximum,
            )
            <= 0
        ):
            raise AllCasesIntegrityError("compute caps must be positive")
        self.run_root = run_root
        self.started = time.monotonic()
        self.failure_event_reserve = 0 if verifier else _OUTER_EVENT_BYTES_MAXIMUM

    @staticmethod
    def _resident_bytes() -> int:
        observed = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        return observed if sys.platform == "darwin" else observed * 1024

    def _regular_bytes(self) -> int:
        if not self.run_root.exists():
            return 0
        total = 0
        seen: set[tuple[int, int]] = set()
        for path in self.run_root.rglob("*"):
            metadata = path.lstat()
            if stat.S_ISLNK(metadata.st_mode):
                raise AllCasesIntegrityError("run-root resource scan found a symlink")
            if stat.S_ISDIR(metadata.st_mode):
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise AllCasesIntegrityError("run-root resource scan found a special file")
            identity = metadata.st_dev, metadata.st_ino
            if identity not in seen:
                total += metadata.st_size
                seen.add(identity)
        return total

    def check(self, boundary: str) -> None:
        if not boundary:
            raise AllCasesIntegrityError("resource boundary label is empty")
        if time.monotonic() - self.started > self.wall_seconds_maximum:
            raise AllCasesIntegrityError(f"wall-time cap exceeded at {boundary}")
        if self._resident_bytes() > self.resident_set_bytes_maximum:
            raise AllCasesIntegrityError(f"resident-set cap exceeded at {boundary}")
        if self._regular_bytes() + self.failure_event_reserve > self.artifact_bytes_maximum:
            raise AllCasesIntegrityError(f"artifact-byte cap exceeded at {boundary}")

    def ensure_additional_bytes(self, byte_count: int, boundary: str) -> None:
        """Reject a publication before it could make the hard byte cap false."""

        if isinstance(byte_count, bool) or not isinstance(byte_count, int) or byte_count < 0:
            raise AllCasesIntegrityError("projected artifact byte count differs")
        self.check(boundary)
        if (
            self._regular_bytes() + byte_count + self.failure_event_reserve
            > self.artifact_bytes_maximum
        ):
            raise AllCasesIntegrityError(f"projected artifact-byte cap exceeded at {boundary}")

    def check_terminal_bytes(self, boundary: str) -> None:
        if self._regular_bytes() > self.artifact_bytes_maximum:
            raise AllCasesIntegrityError(f"terminal artifact-byte cap exceeded at {boundary}")


def _canonical_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError) as error:
        raise AllCasesIntegrityError("value is not canonical JSON") from error


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _json_document(value: object, *, label: str) -> dict[str, object]:
    candidate = value.as_dict() if hasattr(value, "as_dict") else value
    try:
        decoded = json.loads(_canonical_json_bytes(candidate))
    except (TypeError, ValueError) as error:  # pragma: no cover - helper normalizes these
        raise AllCasesIntegrityError(f"{label} is not canonical JSON") from error
    if not isinstance(decoded, dict):
        raise AllCasesIntegrityError(f"{label} must be a JSON object")
    return decoded


def _sha256(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _SHA256_LENGTH
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise AllCasesIntegrityError(f"{label} is not a lowercase SHA-256")
    return value


def _candidate_ids(
    value: object,
    *,
    label: str,
    maximum: int | None = None,
    allow_empty: bool = True,
) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise AllCasesIntegrityError(f"{label} must be an ordered list")
    parsed = tuple(_sha256(item, label=label) for item in value)
    if (not allow_empty and not parsed) or len(set(parsed)) != len(parsed):
        raise AllCasesIntegrityError(f"{label} is empty or contains duplicate identities")
    if maximum is not None and len(parsed) > maximum:
        raise AllCasesIntegrityError(f"{label} exceeds its frozen budget")
    return parsed


def _ordered_subset(child: Sequence[str], parent: Sequence[str]) -> bool:
    positions = {candidate_id: index for index, candidate_id in enumerate(parent)}
    try:
        indexes = tuple(positions[candidate_id] for candidate_id in child)
    except KeyError:
        return False
    return indexes == tuple(sorted(indexes))


@dataclass(frozen=True, slots=True)
class ArtifactIdentity:
    artifact_type: str
    relative_path: str
    sha256: str
    byte_size: int

    def __post_init__(self) -> None:
        path = Path(self.relative_path)
        if (
            not self.artifact_type
            or path.is_absolute()
            or ".." in path.parts
            or path.as_posix() != self.relative_path
            or self.byte_size < 0
        ):
            raise AllCasesIntegrityError("artifact identity is unsafe")
        _sha256(self.sha256, label="artifact sha256")

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_type": self.artifact_type,
            "byte_size": self.byte_size,
            "relative_path": self.relative_path,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, value: object) -> ArtifactIdentity:
        if not isinstance(value, dict) or set(value) != {
            "artifact_type",
            "byte_size",
            "relative_path",
            "sha256",
        }:
            raise AllCasesIntegrityError("artifact identity schema differs")
        if (
            not isinstance(value["artifact_type"], str)
            or not isinstance(value["relative_path"], str)
            or not isinstance(value["sha256"], str)
            or type(value["byte_size"]) is not int
        ):
            raise AllCasesIntegrityError("artifact identity value types differ")
        return cls(
            artifact_type=value["artifact_type"],
            relative_path=value["relative_path"],
            sha256=value["sha256"],
            byte_size=value["byte_size"],
        )


@dataclass(frozen=True, slots=True)
class AllCasesLedgerEvent:
    sequence: int
    predecessor_sha256: str | None
    event_type: str
    request_sha256: str
    recorded_at_utc: str
    payload: dict[str, object]

    @property
    def sha256(self) -> str:
        return _canonical_sha256(self.as_dict())

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": AI_ALL_CASES_EVENT_SCHEMA,
            "event_type": self.event_type,
            "payload": self.payload,
            "predecessor_sha256": self.predecessor_sha256,
            "recorded_at_utc": self.recorded_at_utc,
            "request_sha256": self.request_sha256,
            "sequence": self.sequence,
        }


def _require_transition(prior: str | None, event_type: str) -> None:
    if event_type not in _TRANSITIONS or prior not in _TRANSITIONS[event_type]:
        raise AllCasesIntegrityError(f"ledger transition {prior!r} -> {event_type!r} is invalid")


def _artifact_identity(value: object, *, role: str) -> ArtifactIdentity:
    identity = ArtifactIdentity.from_dict(value)
    if identity.artifact_type != role:
        raise AllCasesIntegrityError("ledger artifact role differs")
    return identity


def _validate_event_payload(event: AllCasesLedgerEvent) -> None:
    if (
        not isinstance(event.sequence, int)
        or isinstance(event.sequence, bool)
        or event.sequence < 1
        or event.event_type not in _EVENT_PAYLOAD_KEYS
        or set(event.payload) != _EVENT_PAYLOAD_KEYS[event.event_type]
    ):
        raise AllCasesIntegrityError("ledger event payload schema differs")
    _sha256(event.request_sha256, label="request_sha256")
    if event.predecessor_sha256 is not None:
        _sha256(event.predecessor_sha256, label="predecessor_sha256")
    try:
        parsed_timestamp = datetime.strptime(
            event.recorded_at_utc, "%Y-%m-%dT%H:%M:%S.%fZ"
        ).replace(tzinfo=UTC)
    except (TypeError, ValueError) as error:
        raise AllCasesIntegrityError("ledger timestamp differs") from error
    if parsed_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ") != event.recorded_at_utc:
        raise AllCasesIntegrityError("ledger timestamp is not canonical")
    role = _ARTIFACT_ROLES.get(event.event_type)
    if role is not None:
        field, artifact_type = role
        _artifact_identity(event.payload[field], role=artifact_type)
    for field in ("candidate_ids", "selected_candidate_ids", "finalist_candidate_ids"):
        if field in event.payload:
            maximum = {
                "selected_candidate_ids": MAXIMUM_SEARCH_SELECTION,
                "finalist_candidate_ids": MAXIMUM_HOLDOUT_FINALISTS,
            }.get(field)
            _candidate_ids(event.payload[field], label=field, maximum=maximum)
    if "fold_keys" in event.payload and (
        not isinstance(event.payload["fold_keys"], list)
        or tuple(event.payload["fold_keys"]) != WALK_FORWARD_FOLD_KEYS
    ):
        raise AllCasesIntegrityError("walk-forward fold order differs")
    if "family_sha256" in event.payload:
        _sha256(event.payload["family_sha256"], label="family_sha256")
    if "universe_root_sha256" in event.payload:
        _sha256(event.payload["universe_root_sha256"], label="universe_root_sha256")
    for field in ("classification", "final_status", "reason", "failure_code"):
        if field in event.payload and (
            not isinstance(event.payload[field], str) or not event.payload[field]
        ):
            raise AllCasesIntegrityError(f"ledger {field} differs")
    if (
        "classification" in event.payload
        and event.payload["classification"] not in HOLDOUT_CLASSIFICATIONS
    ):
        raise AllCasesIntegrityError("holdout classification differs")
    if "final_status" in event.payload and event.payload["final_status"] not in FINAL_STATUSES:
        raise AllCasesIntegrityError("terminal status differs")
    if (
        event.event_type == "FAILED"
        and _FAILURE_CODE.fullmatch(str(event.payload["failure_code"])) is None
    ):
        raise AllCasesIntegrityError("failure code format differs")


def _ledger_event_from_raw(
    raw: bytes,
    *,
    expected_sequence: int,
) -> AllCasesLedgerEvent:
    """Strictly decode one canonical event before chain-level validation."""

    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AllCasesIntegrityError("ledger event is invalid JSON") from error
    if (
        not isinstance(document, dict)
        or _canonical_json_bytes(document) != raw
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
        or document["artifact_schema"] != AI_ALL_CASES_EVENT_SCHEMA
    ):
        raise AllCasesIntegrityError("ledger event schema or bytes differ")
    if (
        type(document["sequence"]) is not int
        or document["sequence"] != expected_sequence
        or not isinstance(document["event_type"], str)
        or not isinstance(document["request_sha256"], str)
        or not isinstance(document["recorded_at_utc"], str)
        or not isinstance(document["payload"], dict)
        or (
            document["predecessor_sha256"] is not None
            and not isinstance(document["predecessor_sha256"], str)
        )
    ):
        raise AllCasesIntegrityError("ledger event value types or sequence differ")
    event = AllCasesLedgerEvent(
        sequence=document["sequence"],
        predecessor_sha256=document["predecessor_sha256"],
        event_type=document["event_type"],
        request_sha256=document["request_sha256"],
        recorded_at_utc=document["recorded_at_utc"],
        payload=document["payload"],
    )
    if _canonical_json_bytes(event.as_dict()) != raw:
        raise AllCasesIntegrityError("ledger event canonical values differ")
    _validate_event_payload(event)
    return event


def _ledger_events_from_root(
    events_root: Path,
    *,
    linked_final_sequence: int | None = None,
) -> tuple[AllCasesLedgerEvent, ...]:
    """Replay an exact complete ledger prefix, optionally admitting its linked final leaf."""

    paths: dict[int, Path] = {}
    for path in events_root.iterdir():
        suffix = path.name.removeprefix("event-").removesuffix(".json")
        metadata = path.lstat()
        sequence = int(suffix) if len(suffix) == 8 and suffix.isdigit() else None
        allowed_link_count = (
            2 if linked_final_sequence is not None and sequence == linked_final_sequence else 1
        )
        if (
            path.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != allowed_link_count
            or path.name != f"event-{suffix}.json"
            or sequence is None
            or metadata.st_mode & _WRITE_BITS
        ):
            raise AllCasesIntegrityError("ledger contains an unsafe event")
        if sequence in paths:
            raise AllCasesIntegrityError("ledger sequence is duplicated")
        paths[sequence] = path
    if linked_final_sequence is not None and (not paths or max(paths) != linked_final_sequence):
        raise AllCasesIntegrityError("linked publisher event is not the final ledger leaf")
    events: list[AllCasesLedgerEvent] = []
    predecessor: str | None = None
    request_sha256: str | None = None
    prior_type: str | None = None
    for expected, sequence in enumerate(sorted(paths), start=1):
        if sequence != expected:
            raise AllCasesIntegrityError("ledger sequence is not contiguous")
        event = _ledger_event_from_raw(
            paths[sequence].read_bytes(),
            expected_sequence=expected,
        )
        _require_transition(prior_type, event.event_type)
        if event.predecessor_sha256 != predecessor:
            raise AllCasesIntegrityError("ledger predecessor chain differs")
        if request_sha256 is None:
            request_sha256 = event.request_sha256
        elif event.request_sha256 != request_sha256:
            raise AllCasesIntegrityError("ledger contains multiple requests")
        events.append(event)
        predecessor = event.sha256
        prior_type = event.event_type
    return tuple(events)


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError("atomic write made no progress")
        view = view[written:]


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _recover_temporary_files(
    staging_root: Path,
    published_root: Path,
    *,
    remove: bool = True,
    linked_only: bool = False,
    validate_content: bool = True,
) -> frozenset[tuple[int, int]]:
    """Remove only bounded publisher leaves left by an interrupted hard-link publish."""

    paths = tuple(staging_root.iterdir())
    if len(paths) > 1:
        raise AllCasesIntegrityError("publisher staging contains multiple crash orphans")
    removed = False
    linked_inodes: set[tuple[int, int]] = set()
    for path in paths:
        metadata = path.lstat()
        stem, separator, suffix = path.name[1:].rpartition("-")
        if (
            path.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or metadata.st_nlink not in {1, 2}
            or not path.name.startswith(".")
            or not path.name.endswith(".tmp")
            or not separator
            or not stem
            or not suffix.removesuffix(".tmp")
            or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789-" for character in stem)
        ):
            raise AllCasesIntegrityError("publisher staging contains an unsafe orphan")
        if metadata.st_nlink == 2:
            companions: list[Path] = []
            for candidate in published_root.iterdir():
                if candidate.is_symlink() or not candidate.is_file():
                    continue
                candidate_metadata = candidate.stat()
                if (candidate_metadata.st_dev, candidate_metadata.st_ino) == (
                    metadata.st_dev,
                    metadata.st_ino,
                ):
                    companions.append(candidate)
            if (
                len(companions) != 1
                or companions[0].suffix != ".json"
                or companions[0].stat().st_mode & _WRITE_BITS
            ):
                raise AllCasesIntegrityError("linked publisher staging orphan is unsafe")
            companion = companions[0]
            companion_prefix, companion_separator, digest_with_suffix = companion.name.rpartition(
                "-"
            )
            digest = digest_with_suffix.removesuffix(".json")
            event_sequence_text = stem.removeprefix("event-")
            is_event = (
                companion.name == f"{stem}.json"
                and stem == f"event-{event_sequence_text}"
                and len(event_sequence_text) == 8
                and event_sequence_text.isdigit()
                and int(event_sequence_text) >= 1
            )
            is_artifact = (
                companion_separator == "-"
                and companion_prefix == stem
                and len(digest) == 64
                and all(character in "0123456789abcdef" for character in digest)
            )
            if not (is_event or is_artifact):
                raise AllCasesIntegrityError("linked publisher destination name differs")
            if validate_content:
                raw = path.read_bytes()
                try:
                    document = json.loads(raw)
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise AllCasesIntegrityError(
                        "linked publisher destination is not JSON"
                    ) from error
                if _canonical_json_bytes(document) != raw or (
                    is_artifact and hashlib.sha256(raw).hexdigest() != digest
                ):
                    raise AllCasesIntegrityError("linked publisher destination bytes differ")
                if is_event:
                    sequence = int(event_sequence_text)
                    events = _ledger_events_from_root(
                        published_root,
                        linked_final_sequence=sequence,
                    )
                    if not events or _canonical_json_bytes(events[-1].as_dict()) != raw:
                        raise AllCasesIntegrityError("linked publisher final event differs")
            linked_inodes.add((metadata.st_dev, metadata.st_ino))
        if remove and (metadata.st_nlink == 2 or not linked_only):
            path.unlink()
            removed = True
    if removed:
        _fsync_directory(staging_root)
    return frozenset(linked_inodes)


def _recover_linked_publisher_temporaries(run_root: Path) -> None:
    """Finish only destination-durable hard-link publishes after cap preflight."""

    for staging_relative, published_relative in (
        ("staging/artifacts", "artifacts"),
        ("ledger/staging", "ledger/events"),
    ):
        staging = run_root / staging_relative
        published = run_root / published_relative
        if staging.is_dir() and published.is_dir():
            _recover_temporary_files(
                staging,
                published,
                linked_only=True,
            )


def _safe_directory(path: Path, *, create: bool) -> Path:
    absolute = path.absolute()
    for candidate in (absolute, *absolute.parents):
        if not candidate.exists() and not candidate.is_symlink():
            continue
        metadata = candidate.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise AllCasesIntegrityError("directory has an unsafe ancestor")
    if create:
        absolute.mkdir(parents=True, exist_ok=True)
    try:
        resolved = path.resolve(strict=True)
    except FileNotFoundError as error:
        raise AllCasesIntegrityError("directory does not exist") from error
    if not resolved.is_dir() or resolved != absolute:
        raise AllCasesIntegrityError("directory is unsafe")
    return resolved


def _project_root(value: Path | str) -> Path:
    requested = Path(value).expanduser()
    if requested.is_symlink():
        raise AllCasesIntegrityError("project root cannot be symbolic")
    try:
        root = requested.resolve(strict=True)
    except FileNotFoundError as error:
        raise AllCasesIntegrityError("project root does not exist") from error
    if not root.is_dir():
        raise AllCasesIntegrityError("project root is not a directory")
    return root


def _fixed_run_root(project_root: Path, *, create: bool) -> Path:
    current = project_root
    for part in DEFAULT_AI_ALL_CASES_ROOT.parts:
        current = current / part
        if current.is_symlink():
            raise AllCasesIntegrityError("run root has a symbolic component")
    return _safe_directory(project_root / DEFAULT_AI_ALL_CASES_ROOT, create=create)


def _outer_artifact_staging(run_root: Path, artifacts_root: Path, *, create: bool) -> Path:
    staging = _safe_directory(run_root / "staging/artifacts", create=create)
    if create:
        _recover_temporary_files(staging, artifacts_root)
    elif any(staging.iterdir()):
        raise AllCasesIntegrityError("read-only verification found a publisher temporary")
    return staging


@contextmanager
def _exclusive_mutation(run_root: Path) -> Iterator[None]:
    path = run_root / ".mutation.lock"
    descriptor = os.open(
        path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        metadata = os.fstat(descriptor)
        visible = path.stat(follow_symlinks=False)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != os.geteuid()
            or metadata.st_size != 0
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or (metadata.st_dev, metadata.st_ino) != (visible.st_dev, visible.st_ino)
        ):
            raise AllCasesIntegrityError("mutation lock is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise AllCasesRunError("another all-cases writer is active") from error
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _publish(
    artifacts_root: Path,
    *,
    artifact_type: str,
    filename_prefix: str,
    document: Mapping[str, object],
    referenced_relative_paths: frozenset[str],
    resources: _RunResourceGuard | None = None,
) -> ArtifactIdentity:
    staging_root = _safe_directory(artifacts_root.parent / "staging/artifacts", create=True)
    _recover_temporary_files(staging_root, artifacts_root)
    raw = _canonical_json_bytes(document)
    digest = hashlib.sha256(raw).hexdigest()
    relative_path = f"{filename_prefix}-{digest}.json"
    destination = artifacts_root / relative_path
    observed = _observed_artifact_leaves(artifacts_root)
    if not referenced_relative_paths.issubset(observed):
        raise AllCasesIntegrityError("artifact directory omits a ledger leaf")
    orphaned = observed - referenced_relative_paths
    if orphaned and orphaned != {relative_path}:
        raise AllCasesIntegrityError("orphan artifact differs from the next publication")
    if destination.exists():
        identity = ArtifactIdentity(artifact_type, relative_path, digest, len(raw))
        _verify_artifact(artifacts_root, identity, expected_bytes=raw)
        return identity
    if resources is not None:
        resources.ensure_additional_bytes(
            len(raw) + _OUTER_EVENT_BYTES_MAXIMUM,
            f"PUBLISH_{filename_prefix}",
        )
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{filename_prefix}-", suffix=".tmp", dir=staging_root
    )
    temporary = Path(temporary_name)
    try:
        _write_all(descriptor, raw)
        os.fchmod(descriptor, 0o444)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, destination, follow_symlinks=False)
        except FileExistsError:
            pass
        directory = os.open(artifacts_root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        else:
            _fsync_directory(staging_root)
    identity = ArtifactIdentity(artifact_type, relative_path, digest, len(raw))
    _verify_artifact(artifacts_root, identity, expected_bytes=raw)
    return identity


def _verify_artifact(
    artifacts_root: Path,
    identity: ArtifactIdentity,
    *,
    expected_bytes: bytes | None = None,
) -> bytes:
    path = artifacts_root / identity.relative_path
    if (
        path.is_symlink()
        or not path.is_file()
        or path.stat().st_nlink != 1
        or path.resolve(strict=True).parent != artifacts_root
        or path.stat().st_mode & _WRITE_BITS
    ):
        raise AllCasesIntegrityError("immutable artifact is missing, writable, or unsafe")
    raw = path.read_bytes()
    if (
        len(raw) != identity.byte_size
        or hashlib.sha256(raw).hexdigest() != identity.sha256
        or (expected_bytes is not None and raw != expected_bytes)
    ):
        raise AllCasesIntegrityError("immutable artifact bytes differ")
    return raw


def _artifact_relative_paths(
    events: Sequence[AllCasesLedgerEvent],
) -> frozenset[str]:
    return frozenset(
        _artifact_from_event(event).relative_path
        for event in events
        if event.event_type in _ARTIFACT_ROLES
    )


def _observed_artifact_leaves(artifacts_root: Path) -> frozenset[str]:
    observed: set[str] = set()
    for path in artifacts_root.iterdir():
        metadata = path.lstat()
        prefix, separator, digest_with_suffix = path.name.rpartition("-")
        digest = digest_with_suffix.removesuffix(".json")
        if (
            path.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_mode & _WRITE_BITS
            or path.name != f"{prefix}-{digest}.json"
            or not separator
            or not prefix
            or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789-" for character in prefix)
        ):
            raise AllCasesIntegrityError("artifact directory contains an unsafe leaf")
        _sha256(digest, label="artifact filename digest")
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != digest:
            raise AllCasesIntegrityError("artifact filename differs from its bytes")
        try:
            document = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise AllCasesIntegrityError("artifact leaf is not JSON") from error
        if _canonical_json_bytes(document) != raw:
            raise AllCasesIntegrityError("artifact leaf is not canonical JSON")
        observed.add(path.name)
    return frozenset(observed)


def _verify_outer_artifact_leaf_set(
    artifacts_root: Path,
    events: Sequence[AllCasesLedgerEvent],
    *,
    allow_one_orphan: bool,
) -> str | None:
    expected = _artifact_relative_paths(events)
    for event in events:
        if event.event_type in _ARTIFACT_ROLES:
            _verify_artifact(artifacts_root, _artifact_from_event(event))
    observed = _observed_artifact_leaves(artifacts_root)
    if not expected.issubset(observed):
        raise AllCasesIntegrityError("artifact directory omits a ledger leaf")
    orphaned = observed - expected
    if len(orphaned) > (1 if allow_one_orphan else 0):
        raise AllCasesIntegrityError("artifact leaf set differs from the ledger closure")
    return next(iter(orphaned), None)


def _discard_bounded_outer_publisher_orphan(
    artifacts_root: Path,
    events: Sequence[AllCasesLedgerEvent],
) -> None:
    """Discard only one validated artifact left before its ledger publication."""

    orphan = _verify_outer_artifact_leaf_set(artifacts_root, events, allow_one_orphan=True)
    if orphan is None:
        return
    prefix, separator, digest_with_suffix = orphan.rpartition("-")
    digest = digest_with_suffix.removesuffix(".json")
    if (
        not separator
        or prefix not in _OUTER_ARTIFACT_FILENAME_PREFIXES
        or orphan != f"{prefix}-{digest}.json"
    ):
        raise AllCasesIntegrityError("outer orphan is not a bounded publisher leaf")
    _sha256(digest, label="outer orphan digest")
    path = artifacts_root / orphan
    metadata = path.lstat()
    if (
        path.is_symlink()
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_mode & _WRITE_BITS
        or hashlib.sha256(path.read_bytes()).hexdigest() != digest
    ):
        raise AllCasesIntegrityError("outer orphan changed before bounded recovery")
    path.unlink()
    descriptor = os.open(artifacts_root, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _preflight_outer_artifact_orphan(
    artifacts_root: Path,
    events: Sequence[AllCasesLedgerEvent],
    config: AllCasesConfig,
) -> None:
    """Validate the sole crash orphan against the exact next lifecycle role."""

    orphan = _verify_outer_artifact_leaf_set(artifacts_root, events, allow_one_orphan=True)
    if orphan is None:
        return
    if not events:
        raise AllCasesIntegrityError("outer artifact predates PRECOMMITTED")
    prior = events[-1]
    next_role: tuple[str, str, str | None]
    if prior.event_type == "PRECOMMITTED":
        next_role = (
            "search-universe",
            "systematic_fx.ai_all_cases_search_universe.v1",
            _SEARCH_UNIVERSE_PAYLOAD_SCHEMA,
        )
    elif prior.event_type == "SEARCH_UNIVERSE_FROZEN":
        next_role = (
            "search-results",
            "systematic_fx.ai_all_cases_search_results.v1",
            _SEARCH_RESULT_PAYLOAD_SCHEMA,
        )
    elif prior.event_type == "SEARCH_RESULTS_RELEASED":
        selected = _candidate_ids(
            prior.payload["selected_candidate_ids"],
            label="orphan-preflight Search selection",
            maximum=MAXIMUM_SEARCH_SELECTION,
        )
        next_role = (
            ("walk-forward-masks" if selected else "walk-forward-skipped"),
            (
                "systematic_fx.ai_all_cases_walk_forward_masks.v1"
                if selected
                else "systematic_fx.ai_all_cases_stage_skip.v1"
            ),
            (
                _WALK_MASKS_PAYLOAD_SCHEMA
                if selected
                else "systematic_fx.ai_all_cases_stage_skip.v1"
            ),
        )
    elif prior.event_type == "WALK_FORWARD_SKIPPED":
        next_role = (
            "holdout-skipped",
            "systematic_fx.ai_all_cases_stage_skip.v1",
            "systematic_fx.ai_all_cases_stage_skip.v1",
        )
    elif prior.event_type == "WALK_FORWARD_MASKS_FROZEN":
        next_role = (
            "walk-forward-results",
            "systematic_fx.ai_all_cases_walk_forward_results.v1",
            _WALK_RESULT_PAYLOAD_SCHEMA,
        )
    elif prior.event_type == "WALK_FORWARD_RESULTS_RELEASED":
        finalists = _candidate_ids(
            prior.payload["finalist_candidate_ids"],
            label="orphan-preflight WF finalists",
            maximum=MAXIMUM_HOLDOUT_FINALISTS,
        )
        next_role = (
            ("holdout-authorization" if finalists else "holdout-skipped"),
            (
                "systematic_fx.ai_all_cases_holdout_authorization.v1"
                if finalists
                else "systematic_fx.ai_all_cases_stage_skip.v1"
            ),
            (None if finalists else "systematic_fx.ai_all_cases_stage_skip.v1"),
        )
    elif prior.event_type == "HOLDOUT_AUTHORIZED":
        next_role = (
            "holdout-masks",
            "systematic_fx.ai_all_cases_holdout_masks.v1",
            _HOLDOUT_MASKS_PAYLOAD_SCHEMA,
        )
    elif prior.event_type == "HOLDOUT_MASKS_FROZEN":
        next_role = (
            "holdout-results",
            "systematic_fx.ai_all_cases_holdout_results.v1",
            _HOLDOUT_RESULT_PAYLOAD_SCHEMA,
        )
    elif prior.event_type in {"HOLDOUT_SKIPPED", "HOLDOUT_RESULTS_RELEASED"}:
        next_role = (
            "all-cases-report",
            "systematic_fx.ai_all_cases_report.v1",
            None,
        )
    else:
        raise AllCasesIntegrityError("terminal lifecycle contains an outer artifact orphan")
    prefix, envelope_schema, payload_schema = next_role
    observed_prefix, separator, digest_with_suffix = orphan.rpartition("-")
    if not separator or observed_prefix != prefix:
        raise AllCasesIntegrityError("outer orphan role differs from the next lifecycle coordinate")
    _sha256(digest_with_suffix.removesuffix(".json"), label="outer orphan digest")
    path = artifacts_root / orphan
    try:
        document = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AllCasesIntegrityError("outer orphan is invalid JSON") from error
    if not isinstance(document, dict):
        raise AllCasesIntegrityError("outer orphan document differs")
    if prefix == "holdout-authorization":
        finalists = tuple(prior.payload["finalist_candidate_ids"])
        if _canonical_json_bytes(document) != _canonical_json_bytes(
            _authorization_document(config, finalists)
        ):
            raise AllCasesIntegrityError("holdout-authorization orphan differs")
    elif prefix == "all-cases-report":
        final_status = (
            prior.payload.get("final_status")
            if prior.event_type == "HOLDOUT_SKIPPED"
            else prior.payload.get("classification")
        )
        if (
            not isinstance(final_status, str)
            or document.get("schema") != envelope_schema
            or _canonical_json_bytes(document)
            != _canonical_json_bytes(_report_document(events, final_status, config))
        ):
            raise AllCasesIntegrityError("report orphan schema differs")
    elif (
        set(document) != {"artifact_schema", "authority", "config_semantic_sha256", "payload"}
        or document.get("artifact_schema") != envelope_schema
        or document.get("authority") != AI_ALL_CASES_AUTHORITY
        or document.get("config_semantic_sha256") != config.semantic_sha256
        or not isinstance(document.get("payload"), dict)
        or document["payload"].get("schema") != payload_schema
    ):
        raise AllCasesIntegrityError("outer orphan envelope differs from its next role")
    else:
        payload = document["payload"]
        if prefix == "search-universe":
            _validate_search_universe(payload, config)
        elif prefix == "search-results":
            universe = _event_payload(
                artifacts_root,
                events,
                "SEARCH_UNIVERSE_FROZEN",
                schema="systematic_fx.ai_all_cases_search_universe.v1",
                config=config,
            )
            _validate_search_result(payload, _validate_search_universe(universe, config), config)
        elif prefix == "walk-forward-masks":
            _validate_walk_masks(
                payload,
                tuple(prior.payload["selected_candidate_ids"]),
            )
        elif prefix == "walk-forward-results":
            search_event = _find_event(events, "SEARCH_RESULTS_RELEASED")
            if search_event is None:
                raise AllCasesIntegrityError("WF-result orphan lacks its Search family")
            selected = tuple(search_event.payload["selected_candidate_ids"])
            _validate_walk_result(payload, selected)
        elif prefix == "walk-forward-skipped":
            if _canonical_json_bytes(payload) != _canonical_json_bytes(
                _skip_document("WALK_FORWARD", "NO_SEARCH_FINALISTS")
            ):
                raise AllCasesIntegrityError("WF-skip orphan differs")
        elif prefix == "holdout-skipped":
            reason = (
                "NO_SEARCH_FINALISTS_HOLDOUT_NOT_OPENED"
                if prior.event_type == "WALK_FORWARD_SKIPPED"
                else "NO_WALK_FORWARD_FINALISTS_HOLDOUT_NOT_OPENED"
            )
            if _canonical_json_bytes(payload) != _canonical_json_bytes(
                _skip_document("HOLDOUT", reason)
            ):
                raise AllCasesIntegrityError("holdout-skip orphan differs")
        elif prefix == "holdout-masks":
            _validate_holdout_masks(payload, tuple(prior.payload["finalist_candidate_ids"]))
        elif prefix == "holdout-results":
            authorization = _find_event(events, "HOLDOUT_AUTHORIZED")
            if authorization is None:
                raise AllCasesIntegrityError("holdout-result orphan lacks authorization")
            _validate_holdout_result(
                payload, tuple(authorization.payload["finalist_candidate_ids"])
            )
    # Strong source replay makes adoption unnecessary.  Remove the validated
    # pre-event leaf, fsync, and let the exact stage service rebuild it.
    path.unlink()
    descriptor = os.open(artifacts_root, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class _Ledger:
    """Canonical predecessor-hashed, append-only, mode-0444 event ledger."""

    def __init__(
        self,
        root: Path,
        *,
        create: bool,
        resources: _RunResourceGuard | None = None,
        allow_bounded_staging: bool = False,
    ) -> None:
        self.resources = resources
        self.root = _safe_directory(root, create=create)
        self.events_root = _safe_directory(self.root / "events", create=create)
        self.staging_root = _safe_directory(self.root / "staging", create=create)
        if create:
            _recover_temporary_files(self.staging_root, self.events_root)
        elif allow_bounded_staging:
            _recover_temporary_files(
                self.staging_root,
                self.events_root,
                remove=False,
            )
        elif any(self.staging_root.iterdir()):
            raise AllCasesIntegrityError("read-only ledger verification found a temporary file")

    def verify(self) -> tuple[AllCasesLedgerEvent, ...]:
        return _ledger_events_from_root(self.events_root)

    def append(
        self,
        event_type: str,
        request_sha256: str,
        payload: Mapping[str, object],
        *,
        enforce_resources: bool = True,
        verify_after_append: bool = True,
    ) -> AllCasesLedgerEvent:
        if type(verify_after_append) is not bool or (
            not verify_after_append and event_type != "COMPLETED"
        ):
            raise AllCasesIntegrityError("ledger replay bypass is reserved for final COMPLETED")
        events = self.verify()
        _require_transition(events[-1].event_type if events else None, event_type)
        event = AllCasesLedgerEvent(
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
        raw = _canonical_json_bytes(event.as_dict())
        if len(raw) > _OUTER_EVENT_BYTES_MAXIMUM:
            raise AllCasesIntegrityError("ledger event exceeds its frozen byte reserve")
        if enforce_resources and self.resources is not None:
            self.resources.ensure_additional_bytes(len(raw), f"LEDGER_{event_type}")
        destination = self.events_root / f"event-{event.sequence:08d}.json"
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".event-{event.sequence:08d}-", suffix=".tmp", dir=self.staging_root
        )
        temporary = Path(temporary_name)
        try:
            _write_all(descriptor, raw)
            os.fchmod(descriptor, 0o444)
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            try:
                os.link(temporary, destination, follow_symlinks=False)
            except FileExistsError as error:
                raise AllCasesIntegrityError("concurrent ledger append conflict") from error
            directory = os.open(self.events_root, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
            else:
                _fsync_directory(self.staging_root)
        if verify_after_append and self.verify()[-1].sha256 != event.sha256:
            raise AllCasesIntegrityError("ledger append did not replay exactly")
        return event


@dataclass(frozen=True, slots=True)
class _AllCasesServices:
    """Private service seam; public entry points never accept replacements."""

    freeze_search_universe: Callable[[Path, AllCasesConfig], object]
    train_select_search: Callable[[Path, AllCasesConfig, Mapping[str, object]], object]
    freeze_walk_forward_masks: Callable[
        [Path, AllCasesConfig, tuple[str, ...], Mapping[str, object]], object
    ]
    evaluate_walk_forward: Callable[
        [
            Path,
            AllCasesConfig,
            tuple[str, ...],
            Mapping[str, object],
            Mapping[str, object],
        ],
        object,
    ]
    freeze_holdout_masks: Callable[
        [Path, AllCasesConfig, tuple[str, ...], Mapping[str, object]], object
    ]
    evaluate_holdout: Callable[
        [Path, AllCasesConfig, tuple[str, ...], Mapping[str, object]], object
    ]
    replay_search_universe_prefix: Callable[[Path, AllCasesConfig], object] | None = None
    replay_search_prefix: Callable[[Path, AllCasesConfig, Mapping[str, object]], object] | None = (
        None
    )


@dataclass(frozen=True, slots=True)
class AllCasesRun:
    config: AllCasesConfig
    status: str
    request_artifact: ArtifactIdentity
    evidence_artifacts: tuple[ArtifactIdentity, ...]
    finalist_candidate_ids: tuple[str, ...]
    root: Path
    event_count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "authority": AI_ALL_CASES_AUTHORITY,
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
            "schema": AI_ALL_CASES_RUN_SCHEMA,
            "status": self.status,
            "strict_backtest_claim": False,
            "strict_sealed_holdout_claim": False,
        }


def _envelope(config: AllCasesConfig, *, schema: str, payload: object) -> dict[str, object]:
    return {
        "artifact_schema": schema,
        "authority": AI_ALL_CASES_AUTHORITY,
        "config_semantic_sha256": config.semantic_sha256,
        "payload": _json_document(payload, label=schema),
    }


def _payload_from_artifact(
    artifacts_root: Path,
    identity: ArtifactIdentity,
    *,
    expected_schema: str,
    config: AllCasesConfig,
) -> dict[str, object]:
    raw = _verify_artifact(artifacts_root, identity)
    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AllCasesIntegrityError("artifact is invalid JSON") from error
    if (
        not isinstance(document, dict)
        or _canonical_json_bytes(document) != raw
        or set(document) != {"artifact_schema", "authority", "config_semantic_sha256", "payload"}
        or document["artifact_schema"] != expected_schema
        or document["authority"] != AI_ALL_CASES_AUTHORITY
        or document["config_semantic_sha256"] != config.semantic_sha256
        or not isinstance(document["payload"], dict)
    ):
        raise AllCasesIntegrityError("artifact envelope differs")
    return document["payload"]


def _artifact_from_event(event: AllCasesLedgerEvent) -> ArtifactIdentity:
    role = _ARTIFACT_ROLES.get(event.event_type)
    if role is None:
        raise AllCasesIntegrityError("ledger event has no artifact")
    field, artifact_type = role
    return _artifact_identity(event.payload[field], role=artifact_type)


def _find_event(
    events: Sequence[AllCasesLedgerEvent], event_type: str
) -> AllCasesLedgerEvent | None:
    return next((event for event in events if event.event_type == event_type), None)


def _event_payload(
    artifacts_root: Path,
    events: Sequence[AllCasesLedgerEvent],
    event_type: str,
    *,
    schema: str,
    config: AllCasesConfig,
) -> dict[str, object]:
    event = _find_event(events, event_type)
    if event is None:
        raise AllCasesIntegrityError(f"{event_type} evidence is missing")
    return _payload_from_artifact(
        artifacts_root,
        _artifact_from_event(event),
        expected_schema=schema,
        config=config,
    )


def _publish_envelope(
    artifacts_root: Path,
    config: AllCasesConfig,
    *,
    artifact_type: str,
    filename_prefix: str,
    schema: str,
    payload: object,
    referenced_relative_paths: frozenset[str],
    resources: _RunResourceGuard | None = None,
) -> ArtifactIdentity:
    return _publish(
        artifacts_root,
        artifact_type=artifact_type,
        filename_prefix=filename_prefix,
        document=_envelope(config, schema=schema, payload=payload),
        referenced_relative_paths=referenced_relative_paths,
        resources=resources,
    )


def _runtime_identity_for_config(config: AllCasesConfig) -> dict[str, object]:
    expected_suffix = AI_ALL_CASES_CONFIG_RELATIVE_PATH
    parts = config.path.parts
    if tuple(parts[-len(expected_suffix.parts) :]) == expected_suffix.parts:
        return _runtime_identity_document(config.path.parents[2])
    # Private unit fixtures do not represent a public checkout.  Their runtime
    # identity still binds the checkout containing this implementation.
    return _runtime_identity_document()


def _request_document(config: AllCasesConfig) -> dict[str, object]:
    runtime_identity = _runtime_identity_for_config(config)
    return {
        "artifact_schema": "systematic_fx.ai_all_cases_request.v1",
        "authority": AI_ALL_CASES_AUTHORITY,
        "config": config.as_dict(),
        "config_file_sha256": config.file_sha256,
        "config_semantic_sha256": config.semantic_sha256,
        "limitations": [
            "SEARCH_IS_RETROSPECTIVE_DEVELOPMENT_AND_TRAINING",
            "WALK_FORWARD_IS_FIRST_OOS_EVIDENCE",
            "LOCAL_HOLDOUT_BYTES_ARE_NOT_PHYSICALLY_SEALED",
            "NO_PAPER_LIVE_OR_PROMOTION_AUTHORITY",
        ],
        "runtime_identity": runtime_identity,
        "runtime_identity_sha256": _canonical_sha256(runtime_identity),
    }


_ALLOWED_RUN_DIRECTORIES: Final = frozenset(
    {
        "artifacts",
        "internal",
        "internal/search",
        "internal/search/artifacts",
        "internal/search/events",
        "internal/search/staging",
        "internal/universe",
        "internal/universe-staging",
        "ledger",
        "ledger/events",
        "ledger/staging",
        "staging",
        "staging/artifacts",
    }
)
_ALLOWED_JSON_LEAF_PARENTS: Final = frozenset(
    {
        "artifacts",
        "internal/search/artifacts",
        "internal/search/events",
        "internal/universe",
        "ledger/events",
    }
)


def _verify_run_root_tree(
    run_root: Path,
    *,
    allow_bounded_internal_staging: bool = False,
    allow_bounded_publisher_staging: bool = False,
) -> None:
    """Reject every ungoverned directory or leaf in the fixed run namespace."""

    recoverable_linked_inodes: set[tuple[int, int]] = set()
    if allow_bounded_publisher_staging:
        for staging_relative, published_relative in (
            ("staging/artifacts", "artifacts"),
            ("ledger/staging", "ledger/events"),
        ):
            staging = run_root / staging_relative
            published = run_root / published_relative
            if staging.is_dir() and published.is_dir():
                recoverable_linked_inodes.update(
                    _recover_temporary_files(
                        staging,
                        published,
                        remove=False,
                        validate_content=False,
                    )
                )
    if allow_bounded_internal_staging:
        staging_destinations = {
            "internal/search/staging": {
                "internal/search/artifacts",
                "internal/search/events",
            },
            "internal/universe-staging": {"internal/universe"},
        }
        for staging_relative, allowed_destinations in staging_destinations.items():
            staging = run_root / staging_relative
            if not staging.is_dir():
                continue
            temporaries = tuple(staging.iterdir())
            if len(temporaries) > 1:
                raise AllCasesIntegrityError("internal staging contains multiple crash orphans")
            for temporary in temporaries:
                metadata = temporary.lstat()
                if metadata.st_nlink != 2:
                    continue
                identity = metadata.st_dev, metadata.st_ino
                companions = []
                for candidate in run_root.rglob("*"):
                    candidate_metadata = candidate.lstat()
                    if (
                        stat.S_ISREG(candidate_metadata.st_mode)
                        and (candidate_metadata.st_dev, candidate_metadata.st_ino) == identity
                    ):
                        companions.append(candidate)
                published = [item for item in companions if item != temporary]
                if (
                    len(companions) != 2
                    or len(published) != 1
                    or published[0].parent.relative_to(run_root).as_posix()
                    not in allowed_destinations
                    or published[0].suffix != ".json"
                    or published[0].lstat().st_mode & _WRITE_BITS
                ):
                    raise AllCasesIntegrityError("internal staging hard-link companion is unsafe")
                recoverable_linked_inodes.add(identity)
    for path in run_root.rglob("*"):
        relative = path.relative_to(run_root).as_posix()
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            raise AllCasesIntegrityError("run tree contains a symbolic path")
        if stat.S_ISDIR(metadata.st_mode):
            if relative not in _ALLOWED_RUN_DIRECTORIES:
                raise AllCasesIntegrityError("run tree contains an ungoverned directory")
            continue
        parent = path.parent.relative_to(run_root).as_posix()
        if allow_bounded_publisher_staging and parent in {
            "ledger/staging",
            "staging/artifacts",
        }:
            continue
        if allow_bounded_internal_staging and parent in {
            "internal/search/staging",
            "internal/universe-staging",
        }:
            if (
                not stat.S_ISREG(metadata.st_mode)
                or not path.name.startswith(".chunk-")
                or not path.name.endswith(".tmp")
                or metadata.st_uid != os.geteuid()
                or metadata.st_nlink not in {1, 2}
            ):
                raise AllCasesIntegrityError("internal staging contains an unsafe orphan")
            continue
        if not stat.S_ISREG(metadata.st_mode) or (
            metadata.st_nlink != 1
            and (metadata.st_dev, metadata.st_ino) not in recoverable_linked_inodes
        ):
            raise AllCasesIntegrityError("run tree contains an unsafe leaf")
        if relative == ".mutation.lock":
            if (
                metadata.st_uid != os.geteuid()
                or metadata.st_size != 0
                or stat.S_IMODE(metadata.st_mode) != 0o600
            ):
                raise AllCasesIntegrityError("mutation lock bytes or mode differ")
            continue
        if (
            parent not in _ALLOWED_JSON_LEAF_PARENTS
            or path.suffix != ".json"
            or metadata.st_mode & _WRITE_BITS
        ):
            raise AllCasesIntegrityError("run tree contains an ungoverned leaf")
    for relative in (
        "internal/search/staging",
        "internal/universe-staging",
        "ledger/staging",
        "staging/artifacts",
    ):
        directory = run_root / relative
        if (
            directory.is_dir()
            and any(directory.iterdir())
            and not (
                allow_bounded_internal_staging
                and relative in {"internal/search/staging", "internal/universe-staging"}
                or allow_bounded_publisher_staging
                and relative in {"ledger/staging", "staging/artifacts"}
            )
        ):
            raise AllCasesIntegrityError("run tree contains publisher staging bytes")


def _recover_and_verify_internal_prefix(
    project_root: Path,
    config: AllCasesConfig,
) -> None:
    """Recover only typed internal publisher remnants before any service call."""

    try:
        from .pipeline import recover_and_verify_internal_prefix

        recover_and_verify_internal_prefix(project_root, config)
    except (OSError, RuntimeError, ValueError) as error:
        raise AllCasesIntegrityError("internal crash prefix failed bounded recovery") from error


def _verify_no_internal_evidence_before_precommit(project_root: Path) -> None:
    try:
        from .pipeline import verify_no_internal_evidence_before_precommit

        verify_no_internal_evidence_before_precommit(project_root)
    except (OSError, RuntimeError, ValueError) as error:
        raise AllCasesIntegrityError("internal evidence predates PRECOMMITTED") from error


def _verify_search_store_empty_before_universe_release(project_root: Path) -> None:
    try:
        from .pipeline import verify_search_store_empty_before_universe_release

        verify_search_store_empty_before_universe_release(project_root)
    except (OSError, RuntimeError, ValueError) as error:
        raise AllCasesIntegrityError("Search evidence predates the universe release") from error


def _verify_internal_universe_release(
    project_root: Path,
    config: AllCasesConfig,
    universe: Mapping[str, object],
) -> None:
    try:
        from .pipeline import verify_internal_universe_release

        verify_internal_universe_release(project_root, config, universe)
    except (OSError, RuntimeError, ValueError) as error:
        raise AllCasesIntegrityError("internal feature-universe release differs") from error


def _verify_internal_search_release(
    project_root: Path,
    config: AllCasesConfig,
    search: Mapping[str, object],
) -> None:
    try:
        from .pipeline import verify_internal_search_release

        verify_internal_search_release(project_root, config, search)
    except (OSError, RuntimeError, ValueError) as error:
        raise AllCasesIntegrityError("internal Search release differs") from error


def _verify_search_prefix_semantics(
    project_root: Path,
    config: AllCasesConfig,
    universe: Mapping[str, object],
) -> None:
    try:
        from .pipeline import verify_search_prefix_semantics

        verify_search_prefix_semantics(project_root, config, universe)
    except (OSError, RuntimeError, ValueError) as error:
        raise AllCasesIntegrityError("observed Search prefix semantic replay differs") from error


def _verify_observed_internal_prefixes(
    project_root: Path,
    config: AllCasesConfig,
) -> None:
    try:
        from .pipeline import verify_observed_internal_prefixes

        verify_observed_internal_prefixes(project_root, config)
    except (OSError, RuntimeError, ValueError) as error:
        raise AllCasesIntegrityError("observed internal prefix differs") from error


def _close_internal_prefix_for_terminal_failure(
    project_root: Path,
    config: AllCasesConfig,
) -> None:
    try:
        from .pipeline import close_internal_prefix_for_terminal_failure

        close_internal_prefix_for_terminal_failure(project_root, config)
    except (OSError, RuntimeError, ValueError) as error:
        raise AllCasesIntegrityError("terminal internal prefix failed bounded closure") from error


def _verify_terminal_internal_prefixes(
    project_root: Path,
    config: AllCasesConfig,
) -> None:
    try:
        from .pipeline import verify_terminal_internal_prefixes

        verify_terminal_internal_prefixes(project_root, config)
    except (OSError, RuntimeError, ValueError) as error:
        raise AllCasesIntegrityError("terminal internal prefix differs") from error


def _project_root_from_run_root(run_root: Path) -> Path:
    project_root = run_root
    for _part in DEFAULT_AI_ALL_CASES_ROOT.parts:
        project_root = project_root.parent
    if project_root / DEFAULT_AI_ALL_CASES_ROOT != run_root:
        raise AllCasesIntegrityError("run root is outside the fixed campaign namespace")
    return project_root


def _verify_released_internal_closures(
    config: AllCasesConfig,
    run_root: Path,
    events: Sequence[AllCasesLedgerEvent],
) -> None:
    project_root = _project_root_from_run_root(run_root)
    if events and events[-1].event_type in {"COMPLETED", "FAILED"}:
        _verify_terminal_internal_prefixes(project_root, config)
    else:
        _verify_observed_internal_prefixes(project_root, config)
    artifacts_root = _safe_directory(run_root / "artifacts", create=False)
    universe_event = _find_event(events, "SEARCH_UNIVERSE_FROZEN")
    search_events_root = run_root / "internal/search/events"
    if universe_event is None and search_events_root.is_dir() and any(search_events_root.iterdir()):
        raise AllCasesIntegrityError("Search evidence exists without a released universe")
    if universe_event is not None:
        universe = _event_payload(
            artifacts_root,
            events,
            "SEARCH_UNIVERSE_FROZEN",
            schema="systematic_fx.ai_all_cases_search_universe.v1",
            config=config,
        )
        universe = _validate_search_universe(universe, config)
        _verify_internal_universe_release(project_root, config, universe)
        _verify_search_prefix_semantics(project_root, config, universe)
    if _find_event(events, "SEARCH_RESULTS_RELEASED") is not None:
        search = _event_payload(
            artifacts_root,
            events,
            "SEARCH_RESULTS_RELEASED",
            schema="systematic_fx.ai_all_cases_search_results.v1",
            config=config,
        )
        _verify_internal_search_release(project_root, config, search)


def _run_value(
    config: AllCasesConfig,
    run_root: Path,
    events: Sequence[AllCasesLedgerEvent],
) -> AllCasesRun:
    if not events:
        raise AllCasesIntegrityError("run has not been precommitted")
    _verify_run_root_tree(run_root)
    _verify_released_internal_closures(config, run_root, events)
    expected_request = _request_document(config)
    if events[0].event_type != "PRECOMMITTED" or events[0].request_sha256 != _canonical_sha256(
        expected_request
    ):
        raise AllCasesIntegrityError("terminal request differs from the current campaign")
    _verify_artifact(
        _safe_directory(run_root / "artifacts", create=False),
        _artifact_from_event(events[0]),
        expected_bytes=_canonical_json_bytes(expected_request),
    )
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
    return AllCasesRun(
        config=config,
        status=events[-1].event_type,
        request_artifact=artifacts[0],
        evidence_artifacts=artifacts,
        finalist_candidate_ids=finalists,
        root=run_root,
        event_count=len(events),
    )


def _append_precommit(
    config: AllCasesConfig,
    ledger: _Ledger,
    artifacts_root: Path,
    resources: _RunResourceGuard | None = None,
) -> tuple[AllCasesLedgerEvent, ...]:
    resources = resources if resources is not None else ledger.resources
    events = ledger.verify()
    request = _request_document(config)
    request_sha256 = _canonical_sha256(request)
    if events:
        if events[0].request_sha256 != request_sha256:
            raise AllCasesIntegrityError("existing run belongs to another precommit")
        _verify_artifact(
            artifacts_root,
            _artifact_from_event(events[0]),
            expected_bytes=_canonical_json_bytes(request),
        )
        _verify_outer_artifact_leaf_set(artifacts_root, events, allow_one_orphan=True)
        return events
    identity = _publish(
        artifacts_root,
        artifact_type="AI_ALL_CASES_REQUEST",
        filename_prefix="all-cases-request",
        document=request,
        referenced_relative_paths=frozenset(),
        resources=resources,
    )
    ledger.append("PRECOMMITTED", request_sha256, {"request_artifact": identity.as_dict()})
    events = ledger.verify()
    _verify_outer_artifact_leaf_set(artifacts_root, events, allow_one_orphan=False)
    return events


def _validate_search_universe(value: object, config: AllCasesConfig) -> dict[str, object]:
    document = _json_document(value, label="Search feature/event universe")
    required = {
        "anchor_policy_count",
        "anchor_policy_identity_root_sha256",
        "anchor_policy_recipe_sha256",
        "direct_catalog_sha256",
        "direct_feature_universe_sha256",
        "direct_opportunity_count",
        "direct_opportunity_lattice_sha256",
        "entry_exit_recipe_sha256",
        "feature_event_universe_sha256",
        "feature_mask_chunk_artifacts",
        "meta_catalog_sha256",
        "schema",
        "stage_a_chunk_plan_sha256",
        "stage_a_chunk_count",
        "stage_a_policy_rows_per_chunk_maximum",
        "structural_opportunity_count",
        "structural_opportunity_lattice_lookahead_seconds",
        "structural_opportunity_lattice_sha256",
        "universe_root_sha256",
    }
    if set(document) != required or document.get("schema") != _SEARCH_UNIVERSE_PAYLOAD_SCHEMA:
        raise AllCasesIntegrityError("Search universe identity fields are missing")
    for field in required - {
        "anchor_policy_count",
        "schema",
        "feature_mask_chunk_artifacts",
        "stage_a_chunk_count",
        "stage_a_policy_rows_per_chunk_maximum",
        "direct_opportunity_count",
        "structural_opportunity_count",
        "structural_opportunity_lattice_lookahead_seconds",
    }:
        _sha256(document[field], label=field)
    leaves = document["feature_mask_chunk_artifacts"]
    if not isinstance(leaves, list) or len(leaves) != 64:
        raise AllCasesIntegrityError("Search feature-universe leaf plan differs")
    for index, leaf in enumerate(leaves):
        if (
            not isinstance(leaf, dict)
            or set(leaf) != {"artifact_sha256", "chunk_index", "relative_path"}
            or type(leaf["chunk_index"]) is not int
            or leaf["chunk_index"] != index
            or not isinstance(leaf["relative_path"], str)
            or leaf["relative_path"] != f"universe-{index:03d}-{leaf['artifact_sha256']}.json"
        ):
            raise AllCasesIntegrityError("Search feature-universe leaf schema differs")
        _sha256(leaf["artifact_sha256"], label="feature-universe leaf SHA")
    if _canonical_sha256(leaves) != document["feature_event_universe_sha256"]:
        raise AllCasesIntegrityError("Search feature-universe leaf closure differs")
    if (
        not isinstance(document["anchor_policy_count"], int)
        or isinstance(document["anchor_policy_count"], bool)
        or document["anchor_policy_count"] <= 0
    ):
        raise AllCasesIntegrityError("anchor policy count differs")
    if (
        document["stage_a_chunk_count"] != 64
        or document["stage_a_policy_rows_per_chunk_maximum"]
        != (document["anchor_policy_count"] + 63) // 64
    ):
        raise AllCasesIntegrityError("Stage-A chunk plan dimensions differ")
    if (
        not isinstance(document["direct_opportunity_count"], int)
        or isinstance(document["direct_opportunity_count"], bool)
        or document["direct_opportunity_count"] <= 0
        or document["direct_opportunity_count"] > document["structural_opportunity_count"]
    ):
        raise AllCasesIntegrityError("direct opportunity lattice dimensions differ")
    if (
        not isinstance(document["structural_opportunity_count"], int)
        or isinstance(document["structural_opportunity_count"], bool)
        or document["structural_opportunity_count"] <= 0
        or document["structural_opportunity_lattice_lookahead_seconds"] != 25_200
    ):
        raise AllCasesIntegrityError("structural opportunity lattice dimensions differ")
    contract = config.as_dict()
    counts = contract.get("universe_counts")
    execution = contract.get("execution")
    bindings = contract.get("bindings")
    if isinstance(counts, dict) and document["anchor_policy_count"] != counts.get(
        "logical_anchor_policy_count"
    ):
        raise AllCasesIntegrityError("anchor policy count differs from precommit")
    if isinstance(execution, dict) and document["entry_exit_recipe_sha256"] != execution.get(
        "entry_exit_recipe_sha256"
    ):
        raise AllCasesIntegrityError("entry/exit recipe differs from precommit")
    if isinstance(execution, dict) and document[
        "structural_opportunity_lattice_lookahead_seconds"
    ] != execution.get("structural_complete_case_lookahead_seconds"):
        raise AllCasesIntegrityError("structural opportunity lattice differs from precommit")
    if isinstance(bindings, dict):
        expected_bindings = {
            "anchor_policy_recipe_sha256": "anchor_policy_recipe_sha256",
            "direct_catalog_sha256": "direct_catalog_sha256",
            "meta_catalog_sha256": "meta_catalog_sha256",
            "stage_a_chunk_plan_sha256": "stage_a_chunk_plan_sha256",
        }
        if any(
            document[document_field] != bindings.get(binding_field)
            for document_field, binding_field in expected_bindings.items()
        ):
            raise AllCasesIntegrityError("Search universe recipe/catalog binding differs")
    identity = {
        key: document[key]
        for key in sorted(
            required - {"schema", "universe_root_sha256", "feature_mask_chunk_artifacts"}
        )
    }
    if _canonical_sha256(identity) != document["universe_root_sha256"]:
        raise AllCasesIntegrityError("Search universe derivation root differs")
    return document


def _validate_search_result(
    value: object,
    universe: Mapping[str, object],
    config: AllCasesConfig,
) -> tuple[dict[str, object], tuple[str, ...], tuple[str, ...]]:
    document = _json_document(value, label="Search training/selection result")
    required = {
        "complete_symbolic_candidate_count",
        "complete_symbolic_candidate_root_sha256",
        "complete_symbolic_derivation_sha256",
        "direct_candidate_count",
        "direct_fit_cache_aggregate",
        "evaluated_candidate_ids",
        "evaluated_family_sha256",
        "meta_candidate_count",
        "meta_fit_cache_aggregate",
        "model_artifacts",
        "meta_plan_sha256",
        "search_chunk_artifacts",
        "search_chunk_leaf_closure_sha256",
        "search_subledger_head_sha256",
        "schema",
        "selected_candidate_ids",
        "stage_a_selected_policy_ids",
        "stage_a_selection_artifact_sha256",
        "stage_a_selection_proof_sha256",
        "stage_b_plan_sha256",
        "strategy_artifacts",
        "symbolic_top24_artifact_sha256",
        "universe_root_sha256",
    }
    if set(document) != required or document.get("schema") != _SEARCH_RESULT_PAYLOAD_SCHEMA:
        raise AllCasesIntegrityError("Search result identity fields are missing")
    if document["universe_root_sha256"] != universe.get("universe_root_sha256"):
        raise AllCasesIntegrityError("Search result differs from frozen universe")
    for field in (
        "complete_symbolic_candidate_root_sha256",
        "complete_symbolic_derivation_sha256",
        "evaluated_family_sha256",
        "stage_a_selection_artifact_sha256",
        "stage_a_selection_proof_sha256",
        "stage_b_plan_sha256",
        "symbolic_top24_artifact_sha256",
        "meta_plan_sha256",
        "search_chunk_leaf_closure_sha256",
        "search_subledger_head_sha256",
        "universe_root_sha256",
    ):
        _sha256(document[field], label=field)
    stage_a = _candidate_ids(
        document["stage_a_selected_policy_ids"],
        label="Stage-A selected policy IDs",
        maximum=256,
    )
    evaluated = _candidate_ids(
        document["evaluated_candidate_ids"],
        label="Search evaluated candidate IDs",
        allow_empty=False,
    )
    if _canonical_sha256(list(evaluated)) != document["evaluated_family_sha256"]:
        raise AllCasesIntegrityError("Search evaluated family commitment differs")
    expected_stage_a_proof = _canonical_sha256(
        {
            "selected_policy_ids": list(stage_a),
            "universe_root_sha256": universe["universe_root_sha256"],
        }
    )
    if document["stage_a_selection_proof_sha256"] != expected_stage_a_proof:
        raise AllCasesIntegrityError("Stage-A selection proof differs")
    chunk_artifacts = document["search_chunk_artifacts"]
    if not isinstance(chunk_artifacts, list) or not chunk_artifacts:
        raise AllCasesIntegrityError("Search chunk artifact closure is empty")
    phase_order = config.as_dict().get("search_design", {}).get("search_internal_phase_order", [])
    if not isinstance(phase_order, list):
        raise AllCasesIntegrityError("Search phase order is absent from precommit")
    positions = {phase: index for index, phase in enumerate(phase_order)}
    prior_key: tuple[int, int] | None = None
    seen: set[tuple[str, int]] = set()
    next_index_by_phase: dict[str, int] = {}
    for leaf in chunk_artifacts:
        if not isinstance(leaf, dict) or set(leaf) != {
            "artifact_sha256",
            "chunk_index",
            "phase",
        }:
            raise AllCasesIntegrityError("Search chunk leaf schema differs")
        phase = leaf["phase"]
        index = leaf["chunk_index"]
        if (
            phase not in positions
            or not isinstance(index, int)
            or isinstance(index, bool)
            or index < 0
        ):
            raise AllCasesIntegrityError("Search chunk leaf coordinate differs")
        _sha256(leaf["artifact_sha256"], label="Search chunk artifact SHA")
        coordinate = (str(phase), index)
        key = (positions[str(phase)], index)
        expected_index = next_index_by_phase.get(str(phase), 0)
        if (
            coordinate in seen
            or (prior_key is not None and key <= prior_key)
            or index != expected_index
        ):
            raise AllCasesIntegrityError("Search chunk leaves are duplicated or unordered")
        seen.add(coordinate)
        next_index_by_phase[str(phase)] = expected_index + 1
        prior_key = key
    if tuple(next_index_by_phase) != tuple(phase_order):
        raise AllCasesIntegrityError("Search chunk closure omits a deterministic phase")
    counts_json = (
        config.as_dict().get("search_design", {}).get("search_phase_chunk_counts_canonical_json")
    )
    if not isinstance(counts_json, str):
        raise AllCasesIntegrityError("Search phase count contract is absent")
    try:
        expected_counts = json.loads(counts_json)
    except json.JSONDecodeError as error:
        raise AllCasesIntegrityError("Search phase count contract is invalid") from error
    actual_counts = {phase: next_index_by_phase[phase] for phase in phase_order}
    if not isinstance(expected_counts, dict) or expected_counts != actual_counts:
        raise AllCasesIntegrityError("Search chunk phase counts differ from precommit")
    if _canonical_sha256(chunk_artifacts) != document["search_chunk_leaf_closure_sha256"]:
        raise AllCasesIntegrityError("Search chunk leaf closure differs")
    counts = {
        field: document[field]
        for field in (
            "complete_symbolic_candidate_count",
            "direct_candidate_count",
            "meta_candidate_count",
        )
    }
    if any(
        not isinstance(item, int) or isinstance(item, bool) or item < 0 for item in counts.values()
    ):
        raise AllCasesIntegrityError("Search family counts differ")
    if counts["complete_symbolic_candidate_count"] > len(stage_a) * 9 * 85 or sum(
        counts.values()
    ) != len(evaluated):
        raise AllCasesIntegrityError("Search derived family size differs")
    expected_complete_derivation = _canonical_sha256(
        {
            "complete_symbolic_candidate_count": counts["complete_symbolic_candidate_count"],
            "complete_symbolic_candidate_root_sha256": document[
                "complete_symbolic_candidate_root_sha256"
            ],
            "entry_exit_recipe_sha256": universe["entry_exit_recipe_sha256"],
            "stage_a_selected_policy_ids": list(stage_a),
        }
    )
    if document["complete_symbolic_derivation_sha256"] != expected_complete_derivation:
        raise AllCasesIntegrityError("complete symbolic derivation proof differs")
    from . import ml

    cache_aggregates = (
        ("direct_fit_cache_aggregate", "DIRECT"),
        ("meta_fit_cache_aggregate", "META"),
    )
    for field, candidate_kind in cache_aggregates:
        raw_aggregate = document[field]
        if not isinstance(raw_aggregate, dict):
            raise AllCasesIntegrityError("Search fit-cache aggregate schema differs")
        try:
            aggregate = ml.SharedFitCacheAggregateEvidence.from_dict(raw_aggregate)
        except ml.AllCasesMLError as error:
            raise AllCasesIntegrityError("Search fit-cache aggregate differs") from error
        if aggregate.candidate_kind != candidate_kind:
            raise AllCasesIntegrityError("Search fit-cache candidate kind differs")
    precommit_counts = config.as_dict().get("universe_counts")
    if isinstance(precommit_counts, dict) and (
        counts["direct_candidate_count"] != precommit_counts.get("direct_ml_count")
        or counts["meta_candidate_count"] != precommit_counts.get("meta_ml_count")
    ):
        raise AllCasesIntegrityError("fixed ML family size differs from precommit")
    selected = _candidate_ids(
        document["selected_candidate_ids"],
        label="Search selected candidate IDs",
        maximum=MAXIMUM_SEARCH_SELECTION,
    )
    if not set(selected).issubset(evaluated):
        raise AllCasesIntegrityError("Search selection is outside the frozen evaluated family")
    frozen_candidates: set[str] = set()
    for collection, digest_field, document_field, allowed_kinds in (
        (
            document["model_artifacts"],
            "model_sha256",
            "model_document",
            {"DIRECT_ML", "META_ML"},
        ),
        (
            document["strategy_artifacts"],
            "strategy_sha256",
            "strategy_document",
            {"SYMBOLIC"},
        ),
    ):
        if not isinstance(collection, list) or any(
            not isinstance(item, dict) for item in collection
        ):
            raise AllCasesIntegrityError("Search frozen artifact list differs")
        for artifact in collection:
            if set(artifact) != {
                "candidate_id",
                "candidate_kind",
                "family_key",
                digest_field,
                document_field,
            }:
                raise AllCasesIntegrityError("Search frozen artifact identity is incomplete")
            candidate_id = _sha256(artifact["candidate_id"], label="artifact candidate_id")
            _sha256(artifact[digest_field], label=digest_field)
            if (
                candidate_id not in evaluated
                or artifact["candidate_kind"] not in allowed_kinds
                or not isinstance(artifact["family_key"], str)
                or not artifact["family_key"]
                or not isinstance(artifact[document_field], dict)
                or _canonical_sha256(artifact[document_field]) != artifact[digest_field]
            ):
                raise AllCasesIntegrityError("Search artifact is outside evaluated family")
            frozen_candidates.add(candidate_id)
    if not set(selected).issubset(frozen_candidates):
        raise AllCasesIntegrityError("selected Search candidate lacks a frozen strategy/model")
    return document, evaluated, selected


def _search_candidate_descriptors(
    search: Mapping[str, object],
    candidate_ids: Sequence[str],
) -> dict[str, dict[str, object]]:
    """Restore the exact immutable Search identity/rank for every OOS candidate."""

    expected = tuple(candidate_ids)
    found: dict[str, dict[str, object]] = {}
    for collection_key, document_key, digest_key in (
        ("strategy_artifacts", "strategy_document", "strategy_sha256"),
        ("model_artifacts", "model_document", "model_sha256"),
    ):
        collection = search.get(collection_key)
        if not isinstance(collection, list):
            raise AllCasesIntegrityError("Search candidate descriptor family differs")
        for artifact in collection:
            if not isinstance(artifact, dict) or not isinstance(artifact.get(document_key), dict):
                raise AllCasesIntegrityError("Search candidate descriptor row differs")
            candidate_id = artifact.get("candidate_id")
            if candidate_id not in expected:
                continue
            document = artifact[document_key]
            candidate = document.get("candidate")
            recipe = document.get("recipe")
            rank = (
                candidate.get("selection_rank")
                if isinstance(candidate, dict)
                else recipe.get("strategy_rank")
                if isinstance(recipe, dict)
                else document.get("catalog_selection_rank")
            )
            if (
                candidate_id in found
                or isinstance(rank, bool)
                or not isinstance(rank, int)
                or rank < 1
                or artifact.get(digest_key) != _canonical_sha256(document)
                or not isinstance(artifact.get("family_key"), str)
                or not artifact["family_key"]
            ):
                raise AllCasesIntegrityError("Search candidate descriptor binding differs")
            found[candidate_id] = {
                "candidate_id": candidate_id,
                "candidate_kind": artifact["candidate_kind"],
                "catalog_selection_rank": rank,
                "family_key": artifact["family_key"],
                "frozen_artifact_sha256": artifact[digest_key],
            }
    if set(found) != set(expected):
        raise AllCasesIntegrityError("Search candidate descriptor family differs")
    return {candidate_id: found[candidate_id] for candidate_id in expected}


def _exact_fraction_document(value: object, *, label: str) -> Fraction:
    if not isinstance(value, dict) or set(value) != {"denominator", "numerator"}:
        raise AllCasesIntegrityError(f"{label} fraction schema differs")
    numerator = value["numerator"]
    denominator = value["denominator"]
    if (
        isinstance(numerator, bool)
        or not isinstance(numerator, int)
        or isinstance(denominator, bool)
        or not isinstance(denominator, int)
        or denominator <= 0
    ):
        raise AllCasesIntegrityError(f"{label} fraction differs")
    fraction = Fraction(numerator, denominator)
    if fraction.numerator != numerator or fraction.denominator != denominator:
        raise AllCasesIntegrityError(f"{label} fraction is not canonical")
    return fraction


def _exact_sign_p(values: Sequence[int]) -> Fraction:
    nonzero = tuple(value for value in values if value != 0)
    if not nonzero:
        return Fraction(1, 1)
    positives = sum(value > 0 for value in nonzero)
    return Fraction(
        sum(comb(len(nonzero), index) for index in range(positives, len(nonzero) + 1)),
        2 ** len(nonzero),
    )


def _production_decision_date_domains(
    project_root: Path,
    config: AllCasesConfig,
) -> tuple[tuple[str, ...], tuple[int, ...], tuple[str, ...]] | None:
    """Derive the exact OOS calendars from the committed manifest/split metadata."""

    if config.as_dict().get("config_id") != AI_ALL_CASES_CONFIG_ID:
        return None
    dataset, split = _load_validated_dataset_contract(project_root)
    eligible = dataset.eligible_active_dates
    walk_parts = tuple(
        tuple(
            value.isoformat()
            for value in eligible[fold.start_active_ordinal - 1 : fold.end_active_ordinal - 20]
        )
        for fold in split.walk_forward_folds
    )
    lengths = tuple(len(part) for part in walk_parts)
    holdout = tuple(
        value.isoformat()
        for value in eligible[
            split.holdout.start_active_ordinal - 1 : split.holdout.end_active_ordinal
        ]
    )
    walk = tuple(value for part in walk_parts for value in part)
    if lengths != (133, 133, 133, 133, 132) or len(walk) != 664 or len(holdout) != 120:
        raise AllCasesIntegrityError("committed OOS decision-date domains differ")
    if tuple(sorted(set(walk))) != walk or tuple(sorted(set(holdout))) != holdout:
        raise AllCasesIntegrityError("committed OOS decision dates are not canonical")
    return walk, lengths, holdout


def _daily_world_p_star(
    value: object,
    *,
    sample_eligible: bool,
    label: str,
    expected_decision_dates: Sequence[str] | None = None,
) -> Fraction:
    if not isinstance(value, dict) or set(value) != {
        "CIRCULAR_TARGET",
        "MATCHED_TARGET",
        "REAL",
    }:
        raise AllCasesIntegrityError(f"{label} daily-world schema differs")

    def vector(rows: object, *, world: str) -> tuple[tuple[str, int], ...] | None:
        if rows is None:
            return None
        if not isinstance(rows, list) or not rows:
            raise AllCasesIntegrityError(f"{label} {world} daily vector differs")
        output = []
        for row in rows:
            if not isinstance(row, dict) or set(row) != {"decision_date", "net_ticks"}:
                raise AllCasesIntegrityError(f"{label} {world} daily row schema differs")
            day = row["decision_date"]
            net = row["net_ticks"]
            try:
                canonical_day = date.fromisoformat(str(day)).isoformat()
            except ValueError as error:
                raise AllCasesIntegrityError(f"{label} daily date differs") from error
            if day != canonical_day or isinstance(net, bool) or not isinstance(net, int):
                raise AllCasesIntegrityError(f"{label} daily value differs")
            output.append((canonical_day, net))
        result = tuple(output)
        if result != tuple(sorted(result)) or len({day for day, _ in result}) != len(result):
            raise AllCasesIntegrityError(f"{label} daily vector is not canonical")
        return result

    real = vector(value["REAL"], world="REAL")
    circular = vector(value["CIRCULAR_TARGET"], world="CIRCULAR_TARGET")
    matched = vector(value["MATCHED_TARGET"], world="MATCHED_TARGET")
    if real is None:
        raise AllCasesIntegrityError(f"{label} REAL daily vector is absent")
    real_dates = tuple(day for day, _ in real)
    if expected_decision_dates is not None and real_dates != tuple(expected_decision_dates):
        raise AllCasesIntegrityError(f"{label} daily vector differs from its frozen calendar")
    if not sample_eligible:
        if circular is not None or matched is not None:
            raise AllCasesIntegrityError(f"{label} ineligible controls must be explicitly absent")
        return Fraction(1, 1)
    if circular is None or matched is None:
        raise AllCasesIntegrityError(f"{label} eligible controls are absent")
    if real_dates != tuple(day for day, _ in circular) or real_dates != tuple(
        day for day, _ in matched
    ):
        raise AllCasesIntegrityError(f"{label} daily vectors omit frozen decision dates")
    real_values = tuple(net for _, net in real)
    circular_values = tuple(net for _, net in circular)
    matched_values = tuple(net for _, net in matched)
    return max(
        _exact_sign_p(real_values),
        _exact_sign_p(
            tuple(left - right for left, right in zip(real_values, circular_values, strict=True))
        ),
        _exact_sign_p(
            tuple(left - right for left, right in zip(real_values, matched_values, strict=True))
        ),
    )


def _validate_economic_evidence(value: object, *, label: str) -> None:
    common = {
        "active_entry_days",
        "contract_count",
        "fill_count",
        "maximum_drawdown_ticks",
        "net_ticks",
        "profit_factor",
    }
    stage_fields = (
        {
            "fold_active_entry_days",
            "fold_fill_counts",
            "fold_net_ticks",
            "worst_fold_ev_ticks",
            "worst_fold_profit_factor",
            "worst_loss_over_median_positive",
        }
        if label == "walk-forward"
        else {"half_net_ticks", "net_over_maximum_drawdown"}
    )
    if not isinstance(value, dict) or set(value) != common | stage_fields:
        raise AllCasesIntegrityError(f"{label} economic evidence schema differs")
    for field in common - {"profit_factor"}:
        item = value[field]
        if isinstance(item, bool) or not isinstance(item, int) or item < 0 and field != "net_ticks":
            raise AllCasesIntegrityError(f"{label} economic evidence value differs")
    for field in stage_fields:
        item = value[field]
        if field.startswith("fold_") or field == "half_net_ticks":
            expected_length = 5 if field.startswith("fold_") else 2
            if (
                not isinstance(item, list)
                or len(item) != expected_length
                or any(isinstance(row, bool) or not isinstance(row, int) for row in item)
            ):
                raise AllCasesIntegrityError(f"{label} grouped economic evidence differs")
        elif item is not None:
            _exact_fraction_document(item, label=f"{label} {field}")
    if value["profit_factor"] is not None:
        _exact_fraction_document(value["profit_factor"], label=f"{label} profit factor")


def _median_fraction(values: Sequence[int]) -> Fraction | None:
    ordered = tuple(sorted(values))
    if not ordered:
        return None
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return Fraction(ordered[middle], 1)
    return Fraction(ordered[middle - 1] + ordered[middle], 2)


def _world_daily_total(value: Mapping[str, object], world: str) -> int | None:
    rows = value[world]
    if rows is None:
        return None
    if not isinstance(rows, list):  # schema is checked before this helper
        raise AllCasesIntegrityError("daily world rows differ")
    return sum(int(row["net_ticks"]) for row in rows)


def _fraction_or_none(value: object, *, label: str) -> Fraction | None:
    return None if value is None else _exact_fraction_document(value, label=label)


def _expected_walk_economics(
    result: Mapping[str, object],
    *,
    expected_decision_dates: Sequence[str] | None = None,
    expected_fold_lengths: Sequence[int] | None = None,
) -> tuple[bool, tuple[str, ...]]:
    evidence = result["evidence"]
    daily = result["daily_net_ticks_by_world"]
    if not isinstance(evidence, dict) or not isinstance(daily, dict):
        raise AllCasesIntegrityError("walk-forward economics are invalid")
    net = int(evidence["net_ticks"])
    fold_nets = tuple(int(value) for value in evidence["fold_net_ticks"])
    fold_fills = tuple(int(value) for value in evidence["fold_fill_counts"])
    if expected_decision_dates is not None:
        if tuple(expected_fold_lengths or ()) != (133, 133, 133, 133, 132):
            raise AllCasesIntegrityError("walk-forward fold calendar dimensions differ")
        real_rows = daily.get("REAL")
        if not isinstance(real_rows, list):
            raise AllCasesIntegrityError("walk-forward REAL daily vector is absent")
        values = tuple(row["net_ticks"] for row in real_rows)
        cursor = 0
        recomputed = []
        for length in expected_fold_lengths or ():
            recomputed.append(sum(values[cursor : cursor + length]))
            cursor += length
        if cursor != len(expected_decision_dates) or tuple(recomputed) != fold_nets:
            raise AllCasesIntegrityError("walk-forward fold-net evidence differs")
    if sum(fold_nets) != net or _world_daily_total(daily, "REAL") != net:
        raise AllCasesIntegrityError("walk-forward net evidence does not close")
    expected_worst_ev = min(
        (
            Fraction(value, fills) if fills else Fraction(-(10**18), 1)
            for value, fills in zip(fold_nets, fold_fills, strict=True)
        ),
        default=Fraction(-(10**18), 1),
    )
    if (
        _fraction_or_none(evidence["worst_fold_ev_ticks"], label="walk-forward worst fold EV")
        != expected_worst_ev
    ):
        raise AllCasesIntegrityError("walk-forward worst-fold EV differs")
    positive = [value for value in fold_nets if value > 0]
    losses = [-value for value in fold_nets if value < 0]
    median_positive = _median_fraction(positive)
    expected_loss_ratio = (
        Fraction(0, 1)
        if not losses
        else None
        if median_positive is None
        else Fraction(max(losses), 1) / median_positive
    )
    if (
        _fraction_or_none(
            evidence["worst_loss_over_median_positive"],
            label="walk-forward worst loss ratio",
        )
        != expected_loss_ratio
    ):
        raise AllCasesIntegrityError("walk-forward loss/positive-fold ratio differs")

    reasons: list[str] = []
    if not result["sample_eligible"]:
        reasons.append("NULL_SAMPLE_INELIGIBLE")
    if evidence["fill_count"] < 100:
        reasons.append("FILLS_LT_100")
    if evidence["active_entry_days"] < 75:
        reasons.append("ACTIVE_ENTRY_DAYS_LT_75")
    if evidence["contract_count"] < 5:
        reasons.append("CONTRACTS_LT_5")
    if any(value < 12 for value in fold_fills):
        reasons.append("FOLD_FILLS_LT_12")
    if any(value < 10 for value in evidence["fold_active_entry_days"]):
        reasons.append("FOLD_ACTIVE_ENTRY_DAYS_LT_10")
    if sum(value > 0 for value in fold_nets) < 4:
        reasons.append("POSITIVE_FOLDS_LT_4")
    if net <= 0:
        reasons.append("TOTAL_NET_NOT_POSITIVE")
    profit_factor = _fraction_or_none(evidence["profit_factor"], label="walk-forward profit factor")
    if (profit_factor is None and net <= 0) or (
        profit_factor is not None and profit_factor < Fraction(11, 10)
    ):
        reasons.append("PROFIT_FACTOR_LT_1_10")
    drawdown = int(evidence["maximum_drawdown_ticks"])
    if net <= 0 or (drawdown > 0 and Fraction(net, drawdown) < 1):
        reasons.append("NET_OVER_MAX_DRAWDOWN_LT_1")
    worst_profit_factor = _fraction_or_none(
        evidence["worst_fold_profit_factor"],
        label="walk-forward worst fold profit factor",
    )
    if worst_profit_factor is None or worst_profit_factor < Fraction(7, 10):
        reasons.append("WORST_FOLD_PROFIT_FACTOR_LT_0_70")
    if expected_loss_ratio is None or expected_loss_ratio > Fraction(3, 2):
        reasons.append("WORST_FOLD_LOSS_GT_1_5_MEDIAN_POSITIVE")
    circular = _world_daily_total(daily, "CIRCULAR_TARGET")
    matched = _world_daily_total(daily, "MATCHED_TARGET")
    if circular is None or matched is None or net <= circular or net <= matched:
        reasons.append("NULL_DELTA_NOT_POSITIVE")
    return not reasons, tuple(reasons)


def _expected_holdout_economics(
    result: Mapping[str, object],
    *,
    expected_decision_dates: Sequence[str] | None = None,
) -> tuple[bool, tuple[str, ...]]:
    evidence = result["evidence"]
    daily = result["daily_net_ticks_by_world"]
    if not isinstance(evidence, dict) or not isinstance(daily, dict):
        raise AllCasesIntegrityError("holdout economics are invalid")
    net = int(evidence["net_ticks"])
    half_nets = tuple(int(value) for value in evidence["half_net_ticks"])
    if expected_decision_dates is not None:
        if len(expected_decision_dates) != 120:
            raise AllCasesIntegrityError("holdout calendar dimensions differ")
        real_rows = daily.get("REAL")
        if not isinstance(real_rows, list):
            raise AllCasesIntegrityError("holdout REAL daily vector is absent")
        values = tuple(row["net_ticks"] for row in real_rows)
        if half_nets != (sum(values[:60]), sum(values[60:120])):
            raise AllCasesIntegrityError("holdout half-net evidence differs")
    if sum(half_nets) != net or _world_daily_total(daily, "REAL") != net:
        raise AllCasesIntegrityError("holdout net evidence does not close")
    reasons: list[str] = []
    if not result["sample_eligible"]:
        reasons.append("NULL_SAMPLE_INELIGIBLE")
    if evidence["fill_count"] < 24:
        reasons.append("FILLS_LT_24")
    if evidence["active_entry_days"] < 18:
        reasons.append("ACTIVE_ENTRY_DAYS_LT_18")
    if evidence["contract_count"] < 2:
        reasons.append("CONTRACTS_LT_2")
    if any(value <= 0 for value in half_nets):
        reasons.append("BOTH_HOLDOUT_HALVES_NET_NOT_POSITIVE")
    if net <= 0:
        reasons.append("TOTAL_NET_NOT_POSITIVE")
    profit_factor = _fraction_or_none(evidence["profit_factor"], label="holdout profit factor")
    if (
        profit_factor is None
        and net <= 0
        or (profit_factor is not None and profit_factor < Fraction(11, 10))
    ):
        reasons.append("PROFIT_FACTOR_LT_1_10")
    drawdown = int(evidence["maximum_drawdown_ticks"])
    expected_net_drawdown = None if drawdown == 0 else Fraction(net, drawdown)
    if (
        _fraction_or_none(evidence["net_over_maximum_drawdown"], label="holdout net/drawdown")
        != expected_net_drawdown
    ):
        raise AllCasesIntegrityError("holdout net/drawdown evidence differs")
    if net <= 0 or (drawdown > 0 and Fraction(net, drawdown) < Fraction(3, 4)):
        reasons.append("NET_OVER_MAX_DRAWDOWN_LT_0_75")
    circular = _world_daily_total(daily, "CIRCULAR_TARGET")
    matched = _world_daily_total(daily, "MATCHED_TARGET")
    if circular is None or matched is None or net <= circular or net <= matched:
        reasons.append("NULL_DELTA_NOT_POSITIVE")
    return not reasons, tuple(reasons)


def _fraction_document_or_none(value: Fraction | None) -> dict[str, int] | None:
    return (
        None if value is None else {"denominator": value.denominator, "numerator": value.numerator}
    )


def _replay_filled_trade_evidence(
    document: Mapping[str, object],
    candidate_id: str,
    *,
    label: str,
    expected_decision_dates: Sequence[str] | None,
    expected_fold_lengths: Sequence[int] | None,
) -> None:
    """Recompute OOS economics from bounded per-date trade monoids."""

    raw_worlds = document.get("filled_trade_summaries_by_world")
    daily_worlds = document.get("daily_net_ticks_by_world")
    evidence = document.get("evidence")
    worlds = ("REAL", "CIRCULAR_TARGET", "MATCHED_TARGET")
    if (
        not isinstance(raw_worlds, dict)
        or set(raw_worlds) != set(worlds)
        or not isinstance(daily_worlds, dict)
        or set(daily_worlds) != set(worlds)
        or not isinstance(evidence, dict)
    ):
        raise AllCasesIntegrityError(f"{label} filled-trade summary family differs")
    row_keys = {
        "contract_ids",
        "decision_date",
        "equity_maximum_prefix_ticks",
        "equity_minimum_prefix_ticks",
        "equity_total_ticks",
        "fill_count",
        "gross_gain_ticks",
        "gross_loss_ticks",
        "maximum_drawdown_ticks",
        "trade_identity_root_sha256",
    }
    fold_keys = WALK_FORWARD_FOLD_KEYS if label == "walk-forward" else ("HOLDOUT",)
    fold_dates: dict[str, tuple[str, ...]] = {}
    if expected_decision_dates is None:
        real_daily = daily_worlds.get("REAL")
        if not isinstance(real_daily, list) or any(not isinstance(row, dict) for row in real_daily):
            raise AllCasesIntegrityError(f"{label} REAL daily calendar differs")
        expected_decision_dates = tuple(row.get("decision_date") for row in real_daily)
        if any(not isinstance(day, str) for day in expected_decision_dates):
            raise AllCasesIntegrityError(f"{label} REAL daily dates differ")
        expected_fold_lengths = (133, 133, 133, 133, 132) if label == "walk-forward" else None
    if expected_decision_dates is not None:
        if label == "walk-forward":
            if expected_fold_lengths is None or len(expected_fold_lengths) != 5:
                raise AllCasesIntegrityError("walk-forward trade-summary fold dimensions differ")
            cursor = 0
            for fold_key, length in zip(fold_keys, expected_fold_lengths, strict=True):
                fold_dates[fold_key] = tuple(expected_decision_dates[cursor : cursor + length])
                cursor += length
            if cursor != len(expected_decision_dates):
                raise AllCasesIntegrityError("walk-forward trade-summary calendar differs")
        else:
            fold_dates["HOLDOUT"] = tuple(expected_decision_dates)

    decoded: dict[str, tuple[dict[str, object], ...] | None] = {}
    for world in worlds:
        raw_rows = raw_worlds[world]
        if raw_rows is None:
            if world == "REAL" or document["sample_eligible"] or daily_worlds[world] is not None:
                raise AllCasesIntegrityError(f"{label} trade-summary world is absent")
            decoded[world] = None
            continue
        if not isinstance(raw_rows, list):
            raise AllCasesIntegrityError(f"{label} trade-summary rows differ")
        daily_rows = daily_worlds[world]
        if not isinstance(daily_rows, list):
            raise AllCasesIntegrityError(f"{label} daily world is absent")
        daily_dates = tuple(row.get("decision_date") for row in daily_rows if isinstance(row, dict))
        if len(daily_dates) != len(daily_rows):
            raise AllCasesIntegrityError(f"{label} daily rows differ")
        if expected_decision_dates is not None and daily_dates != tuple(expected_decision_dates):
            raise AllCasesIntegrityError(f"{label} trade-summary calendar differs")
        if len(raw_rows) != len(daily_rows):
            raise AllCasesIntegrityError(f"{label} trade-summary date closure differs")
        rows: list[dict[str, object]] = []
        for raw, daily_row in zip(raw_rows, daily_rows, strict=True):
            if not isinstance(raw, dict) or set(raw) != row_keys:
                raise AllCasesIntegrityError(f"{label} trade-summary row schema differs")
            integers = (
                raw["equity_maximum_prefix_ticks"],
                raw["equity_minimum_prefix_ticks"],
                raw["equity_total_ticks"],
                raw["fill_count"],
                raw["gross_gain_ticks"],
                raw["gross_loss_ticks"],
                raw["maximum_drawdown_ticks"],
            )
            if any(isinstance(item, bool) or not isinstance(item, int) for item in integers):
                raise AllCasesIntegrityError(f"{label} trade-summary integer differs")
            try:
                canonical_day = date.fromisoformat(raw["decision_date"]).isoformat()
            except (TypeError, ValueError) as error:
                raise AllCasesIntegrityError(f"{label} trade-summary date differs") from error
            contracts = raw["contract_ids"]
            total = raw["equity_total_ticks"]
            fills = raw["fill_count"]
            gains = raw["gross_gain_ticks"]
            losses = raw["gross_loss_ticks"]
            maximum_prefix = raw["equity_maximum_prefix_ticks"]
            minimum_prefix = raw["equity_minimum_prefix_ticks"]
            maximum_drawdown = raw["maximum_drawdown_ticks"]
            if (
                canonical_day != raw["decision_date"]
                or not isinstance(contracts, list)
                or any(not isinstance(contract, str) or not contract for contract in contracts)
            ):
                raise AllCasesIntegrityError(f"{label} trade-summary identity differs")
            if (
                contracts != sorted(set(contracts))
                or fills < 0
                or gains < 0
                or losses < 0
                or maximum_prefix < 0
                or minimum_prefix > 0
                or maximum_drawdown < 0
                or total != gains - losses
                or total < minimum_prefix
                or total > maximum_prefix
                or maximum_drawdown > maximum_prefix - minimum_prefix
                or maximum_drawdown < max(-minimum_prefix, maximum_prefix - total)
                or (fills == 0 and (contracts or gains or losses or total or maximum_drawdown))
                or (fills == 0 and (maximum_prefix != 0 or minimum_prefix != 0))
                or (fills > 0 and not contracts)
                or len(contracts) > fills
                or int(gains > 0) + int(losses > 0) > fills
                or _sha256(
                    raw["trade_identity_root_sha256"],
                    label=f"{label} trade identity root",
                )
                != raw["trade_identity_root_sha256"]
                or raw["decision_date"] != daily_row.get("decision_date")
                or total != daily_row.get("net_ticks")
            ):
                raise AllCasesIntegrityError(f"{label} trade-summary identity differs")
            if fills == 1:
                if gains > 0:
                    expected_single = (gains, 0, gains, gains, 0, 0)
                elif losses > 0:
                    expected_single = (0, losses, -losses, 0, -losses, losses)
                else:
                    expected_single = (0, 0, 0, 0, 0, 0)
                if (
                    gains,
                    losses,
                    total,
                    maximum_prefix,
                    minimum_prefix,
                    maximum_drawdown,
                ) != expected_single:
                    raise AllCasesIntegrityError(
                        f"{label} single-fill trade-summary monoid differs"
                    )
            if fills == 0 and raw["trade_identity_root_sha256"] != _canonical_sha256([]):
                raise AllCasesIntegrityError(f"{label} empty trade-summary root differs")
            rows.append(raw)
        canonical = tuple(rows)
        if tuple(row["decision_date"] for row in canonical) != daily_dates:
            raise AllCasesIntegrityError(f"{label} trade-summary order differs")
        decoded[world] = canonical

    real = decoded["REAL"]
    if real is None:  # pragma: no cover - guarded above
        raise AllCasesIntegrityError(f"{label} REAL trade summaries are absent")
    equity = 0
    peak = 0
    maximum_drawdown = 0
    for row in real:
        maximum_drawdown = max(
            maximum_drawdown,
            row["maximum_drawdown_ticks"],
            peak - (equity + row["equity_minimum_prefix_ticks"]),
        )
        peak = max(peak, equity + row["equity_maximum_prefix_ticks"])
        equity += row["equity_total_ticks"]
    gains = sum(row["gross_gain_ticks"] for row in real)
    losses = sum(row["gross_loss_ticks"] for row in real)
    profit_factor = None if losses == 0 else Fraction(gains, losses)
    expected_evidence: dict[str, object] = {
        "active_entry_days": sum(row["fill_count"] > 0 for row in real),
        "contract_count": len({contract for row in real for contract in row["contract_ids"]}),
        "fill_count": sum(row["fill_count"] for row in real),
        "maximum_drawdown_ticks": maximum_drawdown,
        "net_ticks": equity,
        "profit_factor": _fraction_document_or_none(profit_factor),
    }
    if label == "walk-forward":
        by_date = {row["decision_date"]: row for row in real}
        fold_rows = tuple(tuple(by_date[day] for day in fold_dates[key]) for key in fold_keys)
        fold_nets = tuple(sum(row["equity_total_ticks"] for row in rows) for rows in fold_rows)
        fold_fills = tuple(sum(row["fill_count"] for row in rows) for rows in fold_rows)
        positive = tuple(value for value in fold_nets if value > 0)
        losses_by_fold = tuple(-value for value in fold_nets if value < 0)
        median_positive = _median_fraction(positive)
        loss_ratio = (
            Fraction(0, 1)
            if not losses_by_fold
            else None
            if median_positive is None
            else Fraction(max(losses_by_fold), 1) / median_positive
        )
        fold_profit_factors = []
        for rows in fold_rows:
            fold_gains = sum(row["gross_gain_ticks"] for row in rows)
            fold_losses = sum(row["gross_loss_ticks"] for row in rows)
            fold_profit_factors.append(
                Fraction(10**18, 1)
                if fold_losses == 0 and fold_gains > 0
                else Fraction(0, 1)
                if fold_losses == 0
                else Fraction(fold_gains, fold_losses)
            )
        expected_evidence.update(
            {
                "fold_active_entry_days": [
                    sum(row["fill_count"] > 0 for row in rows) for rows in fold_rows
                ],
                "fold_fill_counts": list(fold_fills),
                "fold_net_ticks": list(fold_nets),
                "worst_fold_ev_ticks": _fraction_document_or_none(
                    min(
                        (
                            Fraction(net, fills) if fills else Fraction(-(10**18), 1)
                            for net, fills in zip(fold_nets, fold_fills, strict=True)
                        )
                    )
                ),
                "worst_fold_profit_factor": _fraction_document_or_none(min(fold_profit_factors)),
                "worst_loss_over_median_positive": _fraction_document_or_none(loss_ratio),
            }
        )
    else:
        real_daily = daily_worlds["REAL"]
        half_nets = [
            sum(row["net_ticks"] for row in real_daily[:60]),
            sum(row["net_ticks"] for row in real_daily[60:120]),
        ]
        expected_evidence.update(
            {
                "half_net_ticks": half_nets,
                "net_over_maximum_drawdown": _fraction_document_or_none(
                    None if maximum_drawdown == 0 else Fraction(equity, maximum_drawdown)
                ),
            }
        )
    if _canonical_json_bytes(evidence) != _canonical_json_bytes(expected_evidence):
        raise AllCasesIntegrityError(f"{label} economics differ from trade summaries")


def _validate_candidate_result_documents(
    value: object,
    candidate_ids: tuple[str, ...],
    *,
    label: str,
    expected_decision_dates: Sequence[str] | None = None,
    expected_fold_lengths: Sequence[int] | None = None,
    expected_candidate_descriptors: Mapping[str, Mapping[str, object]] | None = None,
) -> dict[str, dict[str, object]]:
    if not isinstance(value, list) or len(value) != len(candidate_ids):
        raise AllCasesIntegrityError(f"{label} candidate-result count differs")
    output: dict[str, dict[str, object]] = {}
    for candidate_id, item in zip(candidate_ids, value, strict=True):
        if not isinstance(item, dict) or set(item) != {
            "candidate_id",
            "result_document",
            "result_sha256",
        }:
            raise AllCasesIntegrityError(f"{label} candidate-result schema differs")
        if item["candidate_id"] != candidate_id or not isinstance(item["result_document"], dict):
            raise AllCasesIntegrityError(f"{label} candidate-result identity differs")
        _sha256(item["result_sha256"], label=f"{label} candidate-result SHA")
        if _canonical_sha256(item["result_document"]) != item["result_sha256"]:
            raise AllCasesIntegrityError(f"{label} candidate-result bytes differ")
        document = item["result_document"]
        common = {
            "candidate_kind",
            "catalog_selection_rank",
            "daily_net_ticks_by_world",
            "economic_gate_pass",
            "evidence",
            "failure_reasons",
            "p_star",
            "sample_eligible",
        }
        has_descriptor = "candidate_descriptor" in document
        has_filled_trades = "filled_trade_summaries_by_world" in document
        if expected_candidate_descriptors is not None and not has_descriptor:
            raise AllCasesIntegrityError(f"{label} candidate descriptor is absent")
        if expected_decision_dates is not None and not has_filled_trades:
            raise AllCasesIntegrityError(f"{label} filled-trade evidence is absent")
        if has_descriptor:
            common.add("candidate_descriptor")
        if has_filled_trades:
            common.add("filled_trade_summaries_by_world")
        stage_fields = (
            {"bh_rejected", "finalist_rank", "selected_before_budget", "selected_finalist"}
            if label == "walk-forward"
            else {"holm_rejected", "verdict_pass"}
        )
        if set(document) != common | stage_fields:
            raise AllCasesIntegrityError(f"{label} result-document schema differs")
        if document["candidate_kind"] not in {"SYMBOLIC", "DIRECT_ML", "META_ML"}:
            raise AllCasesIntegrityError(f"{label} candidate kind differs")
        rank = document["catalog_selection_rank"]
        if isinstance(rank, bool) or not isinstance(rank, int) or rank < 1:
            raise AllCasesIntegrityError(f"{label} catalog rank differs")
        if has_descriptor:
            descriptor = document["candidate_descriptor"]
            if not isinstance(descriptor, dict) or set(descriptor) != {
                "candidate_id",
                "candidate_kind",
                "catalog_selection_rank",
                "family_key",
                "frozen_artifact_sha256",
            }:
                raise AllCasesIntegrityError(f"{label} candidate descriptor differs")
            _sha256(descriptor["candidate_id"], label=f"{label} descriptor candidate ID")
            _sha256(
                descriptor["frozen_artifact_sha256"],
                label=f"{label} descriptor frozen artifact SHA",
            )
            if (
                descriptor["candidate_id"] != candidate_id
                or descriptor["candidate_kind"] != document["candidate_kind"]
                or descriptor["catalog_selection_rank"] != rank
                or not isinstance(descriptor["family_key"], str)
                or not descriptor["family_key"]
                or (
                    expected_candidate_descriptors is not None
                    and _canonical_json_bytes(descriptor)
                    != _canonical_json_bytes(expected_candidate_descriptors.get(candidate_id))
                )
            ):
                raise AllCasesIntegrityError(f"{label} candidate descriptor binding differs")
        boolean_fields = ("economic_gate_pass", "sample_eligible") + (
            ("bh_rejected", "selected_before_budget", "selected_finalist")
            if label == "walk-forward"
            else ("holm_rejected", "verdict_pass")
        )
        for field in boolean_fields:
            if not isinstance(document[field], bool):
                raise AllCasesIntegrityError(f"{label} Boolean decision differs")
        reasons = document["failure_reasons"]
        if (
            not isinstance(reasons, list)
            or any(not isinstance(reason, str) or not reason for reason in reasons)
            or len(set(reasons)) != len(reasons)
        ):
            raise AllCasesIntegrityError(f"{label} failure reasons differ")
        _validate_economic_evidence(document["evidence"], label=label)
        if has_filled_trades:
            _replay_filled_trade_evidence(
                document,
                candidate_id,
                label=label,
                expected_decision_dates=expected_decision_dates,
                expected_fold_lengths=expected_fold_lengths,
            )
        expected_p = _daily_world_p_star(
            document["daily_net_ticks_by_world"],
            sample_eligible=document["sample_eligible"],
            label=label,
            expected_decision_dates=expected_decision_dates,
        )
        if _exact_fraction_document(document["p_star"], label=f"{label} p_star") != expected_p:
            raise AllCasesIntegrityError(f"{label} p_star differs")
        expected_gate, expected_reasons = (
            _expected_walk_economics(
                document,
                expected_decision_dates=expected_decision_dates,
                expected_fold_lengths=expected_fold_lengths,
            )
            if label == "walk-forward"
            else _expected_holdout_economics(
                document,
                expected_decision_dates=expected_decision_dates,
            )
        )
        if (
            document["economic_gate_pass"] != expected_gate
            or tuple(document["failure_reasons"]) != expected_reasons
        ):
            raise AllCasesIntegrityError(f"{label} economic gate decision differs")
        if label == "walk-forward":
            finalist_rank = document["finalist_rank"]
            if finalist_rank is not None and (
                isinstance(finalist_rank, bool)
                or not isinstance(finalist_rank, int)
                or finalist_rank < 1
                or not document["selected_finalist"]
            ):
                raise AllCasesIntegrityError("walk-forward finalist rank differs")
            if document["selected_finalist"] != (finalist_rank is not None):
                raise AllCasesIntegrityError("walk-forward finalist decision differs")
        output[candidate_id] = document
    return output


def _bh_rejected_ids(
    candidate_ids: tuple[str, ...], documents: Mapping[str, Mapping[str, object]]
) -> frozenset[str]:
    ordered = sorted(
        candidate_ids,
        key=lambda candidate_id: (
            _exact_fraction_document(
                documents[candidate_id]["p_star"], label="walk-forward p_star"
            ),
            candidate_id,
        ),
    )
    largest = 0
    for rank, candidate_id in enumerate(ordered, start=1):
        value = _exact_fraction_document(
            documents[candidate_id]["p_star"], label="walk-forward p_star"
        )
        if value <= Fraction(1, 20) * rank / len(ordered):
            largest = rank
    return frozenset(ordered[:largest])


def _holm_rejected_ids(
    candidate_ids: tuple[str, ...], documents: Mapping[str, Mapping[str, object]]
) -> frozenset[str]:
    ordered = sorted(
        candidate_ids,
        key=lambda candidate_id: (
            _exact_fraction_document(documents[candidate_id]["p_star"], label="holdout p_star"),
            candidate_id,
        ),
    )
    rejected: set[str] = set()
    for index, candidate_id in enumerate(ordered):
        value = _exact_fraction_document(documents[candidate_id]["p_star"], label="holdout p_star")
        if value > Fraction(1, 20) / (len(ordered) - index):
            break
        rejected.add(candidate_id)
    return frozenset(rejected)


def _validate_walk_masks(
    value: object,
    candidate_ids: tuple[str, ...],
) -> dict[str, object]:
    document = _json_document(value, label="walk-forward masks")
    required = {
        "candidate_ids",
        "fold_keys",
        "mask_commitment_sha256",
        "mask_documents",
        "schema",
    }
    if set(document) != required or document.get("schema") != _WALK_MASKS_PAYLOAD_SCHEMA:
        raise AllCasesIntegrityError("walk-forward mask identity fields are missing")
    if (
        _candidate_ids(document["candidate_ids"], label="WF mask candidate IDs") != candidate_ids
        or tuple(document["fold_keys"]) != WALK_FORWARD_FOLD_KEYS
    ):
        raise AllCasesIntegrityError("walk-forward masks differ from the selected family")
    _sha256(document["mask_commitment_sha256"], label="WF mask commitment")
    mask_documents = document["mask_documents"]
    if not isinstance(mask_documents, list) or len(mask_documents) != len(candidate_ids) * 5:
        raise AllCasesIntegrityError("walk-forward mask document count differs")
    expected_coordinates = [
        (candidate_id, fold_key)
        for candidate_id in candidate_ids
        for fold_key in WALK_FORWARD_FOLD_KEYS
    ]
    actual_coordinates = []
    for item in mask_documents:
        if not isinstance(item, dict) or set(item) != {
            "candidate_id",
            "fold_key",
            "mask_kind",
            "mask_sha256",
        }:
            raise AllCasesIntegrityError("walk-forward mask document schema differs")
        actual_coordinates.append((item["candidate_id"], item["fold_key"]))
        _sha256(item["candidate_id"], label="WF mask candidate_id")
        _sha256(item["mask_sha256"], label="WF mask SHA")
        if item["mask_kind"] not in {"SYMBOLIC", "DIRECT_ML", "META_ML"}:
            raise AllCasesIntegrityError("walk-forward mask kind differs")
    if actual_coordinates != expected_coordinates:
        raise AllCasesIntegrityError("walk-forward mask coordinates differ")
    if _canonical_sha256(mask_documents) != document["mask_commitment_sha256"]:
        raise AllCasesIntegrityError("walk-forward mask closure differs")
    return document


def _validate_walk_result(
    value: object,
    candidate_ids: tuple[str, ...],
    *,
    expected_decision_dates: Sequence[str] | None = None,
    expected_fold_lengths: Sequence[int] | None = None,
    expected_candidate_descriptors: Mapping[str, Mapping[str, object]] | None = None,
) -> tuple[dict[str, object], tuple[str, ...]]:
    document = _json_document(value, label="walk-forward result")
    required = {
        "all_folds_complete",
        "candidate_ids",
        "candidate_results",
        "finalist_candidate_ids",
        "fold_keys",
        "multiplicity_sha256",
        "schema",
    }
    if set(document) != required or document.get("schema") != _WALK_RESULT_PAYLOAD_SCHEMA:
        raise AllCasesIntegrityError("walk-forward result identity fields are missing")
    if (
        document["all_folds_complete"] is not True
        or tuple(document["fold_keys"]) != WALK_FORWARD_FOLD_KEYS
        or _candidate_ids(document["candidate_ids"], label="WF result candidate IDs")
        != candidate_ids
    ):
        raise AllCasesIntegrityError("walk-forward result is partial or from another family")
    finalists = _candidate_ids(
        document["finalist_candidate_ids"],
        label="WF finalist candidate IDs",
        maximum=MAXIMUM_HOLDOUT_FINALISTS,
    )
    if not set(finalists).issubset(candidate_ids):
        raise AllCasesIntegrityError("WF finalists are not a frozen subset")
    result_documents = _validate_candidate_result_documents(
        document["candidate_results"],
        candidate_ids,
        label="walk-forward",
        expected_decision_dates=expected_decision_dates,
        expected_fold_lengths=expected_fold_lengths,
        expected_candidate_descriptors=expected_candidate_descriptors,
    )
    rejected = _bh_rejected_ids(candidate_ids, result_documents)
    for candidate_id in candidate_ids:
        result = result_documents[candidate_id]
        expected_rejected = candidate_id in rejected
        expected_before_budget = (
            expected_rejected
            and bool(result["sample_eligible"])
            and bool(result["economic_gate_pass"])
        )
        if (
            result["bh_rejected"] != expected_rejected
            or result["selected_before_budget"] != expected_before_budget
        ):
            raise AllCasesIntegrityError("WF BH or gate decision differs")
    ranked_finalists = tuple(
        candidate_id
        for candidate_id, _rank in sorted(
            (
                (candidate_id, result_documents[candidate_id]["finalist_rank"])
                for candidate_id in candidate_ids
                if result_documents[candidate_id]["selected_finalist"]
            ),
            key=lambda item: int(item[1]),
        )
    )
    if ranked_finalists != finalists or tuple(
        int(result_documents[candidate_id]["finalist_rank"]) for candidate_id in finalists
    ) != tuple(range(1, len(finalists) + 1)):
        raise AllCasesIntegrityError("WF finalist rank closure differs")
    eligible_for_budget = tuple(
        candidate_id
        for candidate_id in candidate_ids
        if result_documents[candidate_id]["selected_before_budget"]
    )

    def ranking_key(candidate_id: str) -> tuple[object, ...]:
        result = result_documents[candidate_id]
        evidence = result["evidence"]
        if not isinstance(evidence, dict):  # validated above
            raise AllCasesIntegrityError("WF ranking evidence differs")
        net = int(evidence["net_ticks"])
        fills = int(evidence["fill_count"])
        aggregate_ev = Fraction(net, fills) if fills else Fraction(-(10**18), 1)
        profit_factor = _fraction_or_none(
            evidence["profit_factor"], label="walk-forward ranking profit factor"
        )
        infinite_profit = profit_factor is None and net > 0
        return (
            _exact_fraction_document(result["p_star"], label="walk-forward p_star"),
            -_exact_fraction_document(
                evidence["worst_fold_ev_ticks"], label="walk-forward worst fold EV"
            ),
            -aggregate_ev,
            0 if infinite_profit else 1,
            -(profit_factor or Fraction(0, 1)),
            int(result["catalog_selection_rank"]),
            candidate_id,
        )

    expected_finalists = tuple(sorted(eligible_for_budget, key=ranking_key)[:3])
    if finalists != expected_finalists:
        raise AllCasesIntegrityError("WF finalist budget/rank differs")
    _sha256(document["multiplicity_sha256"], label="WF multiplicity SHA")
    if document["multiplicity_sha256"] != _canonical_sha256(
        {
            "candidate_results": document["candidate_results"],
            "finalist_candidate_ids": list(finalists),
            "method": "BENJAMINI_HOCHBERG",
        }
    ):
        raise AllCasesIntegrityError("WF multiplicity closure differs")
    return document, finalists


def _validate_holdout_masks(
    value: object,
    candidate_ids: tuple[str, ...],
) -> dict[str, object]:
    document = _json_document(value, label="holdout masks")
    required = {"candidate_ids", "mask_commitment_sha256", "mask_documents", "schema"}
    if set(document) != required or document.get("schema") != _HOLDOUT_MASKS_PAYLOAD_SCHEMA:
        raise AllCasesIntegrityError("holdout mask identity fields are missing")
    if _candidate_ids(document["candidate_ids"], label="holdout mask IDs") != candidate_ids:
        raise AllCasesIntegrityError("holdout masks differ from authorized family")
    _sha256(document["mask_commitment_sha256"], label="holdout mask commitment")
    mask_documents = document["mask_documents"]
    if not isinstance(mask_documents, list) or len(mask_documents) != len(candidate_ids):
        raise AllCasesIntegrityError("holdout mask document count differs")
    for candidate_id, item in zip(candidate_ids, mask_documents, strict=True):
        if not isinstance(item, dict) or set(item) != {
            "candidate_id",
            "mask_kind",
            "mask_sha256",
        }:
            raise AllCasesIntegrityError("holdout mask document schema differs")
        if item["candidate_id"] != candidate_id:
            raise AllCasesIntegrityError("holdout mask coordinates differ")
        _sha256(item["mask_sha256"], label="holdout mask SHA")
        if item["mask_kind"] not in {"SYMBOLIC", "DIRECT_ML", "META_ML"}:
            raise AllCasesIntegrityError("holdout mask kind differs")
    if _canonical_sha256(mask_documents) != document["mask_commitment_sha256"]:
        raise AllCasesIntegrityError("holdout mask closure differs")
    return document


def _validate_holdout_result(
    value: object,
    candidate_ids: tuple[str, ...],
    *,
    expected_decision_dates: Sequence[str] | None = None,
    expected_candidate_descriptors: Mapping[str, Mapping[str, object]] | None = None,
) -> tuple[dict[str, object], str]:
    document = _json_document(value, label="holdout result")
    required = {
        "candidate_ids",
        "candidate_results",
        "classification",
        "holm_sha256",
        "schema",
    }
    if set(document) != required or document.get("schema") != _HOLDOUT_RESULT_PAYLOAD_SCHEMA:
        raise AllCasesIntegrityError("holdout result identity fields are missing")
    if _candidate_ids(document["candidate_ids"], label="holdout result IDs") != candidate_ids:
        raise AllCasesIntegrityError("holdout result differs from authorized family")
    classification = document["classification"]
    if classification not in HOLDOUT_CLASSIFICATIONS:
        raise AllCasesIntegrityError("holdout result classification differs")
    result_documents = _validate_candidate_result_documents(
        document["candidate_results"],
        candidate_ids,
        label="holdout",
        expected_decision_dates=expected_decision_dates,
        expected_candidate_descriptors=expected_candidate_descriptors,
    )
    rejected = _holm_rejected_ids(candidate_ids, result_documents)
    for candidate_id in candidate_ids:
        result = result_documents[candidate_id]
        expected_rejected = candidate_id in rejected
        expected_pass = (
            expected_rejected
            and bool(result["sample_eligible"])
            and bool(result["economic_gate_pass"])
        )
        if result["holm_rejected"] != expected_rejected or result["verdict_pass"] != expected_pass:
            raise AllCasesIntegrityError("holdout Holm or gate decision differs")
    any_pass = any(result["verdict_pass"] for result in result_documents.values())
    every_ineligible = all(not result["sample_eligible"] for result in result_documents.values())
    expected_classification = (
        "ONE_SHOT_UNSEALED_ALL_CASES_HOLDOUT_DIAGNOSTIC_PASS"
        if any_pass
        else "ONE_SHOT_UNSEALED_ALL_CASES_HOLDOUT_DIAGNOSTIC_INCONCLUSIVE"
        if every_ineligible
        else "ONE_SHOT_UNSEALED_ALL_CASES_HOLDOUT_DIAGNOSTIC_FAIL"
    )
    if classification != expected_classification:
        raise AllCasesIntegrityError("holdout terminal classification differs from decisions")
    _sha256(document["holm_sha256"], label="holdout Holm SHA")
    if document["holm_sha256"] != _canonical_sha256(
        {
            "candidate_results": document["candidate_results"],
            "classification": classification,
            "method": "HOLM_STEP_DOWN",
        }
    ):
        raise AllCasesIntegrityError("holdout multiplicity closure differs")
    return document, str(classification)


def _authorization_document(
    config: AllCasesConfig, candidate_ids: tuple[str, ...]
) -> dict[str, object]:
    family_sha256 = _canonical_sha256(list(candidate_ids))
    return {
        "artifact_schema": "systematic_fx.ai_all_cases_holdout_authorization.v1",
        "candidate_ids": list(candidate_ids),
        "config_semantic_sha256": config.semantic_sha256,
        "family_sha256": family_sha256,
        "one_shot": True,
    }


def _skip_document(stage: str, reason: str) -> dict[str, object]:
    return {
        "reason": reason,
        "schema": "systematic_fx.ai_all_cases_stage_skip.v1",
        "stage": stage,
    }


def _validate_existing_skip_event(
    artifacts_root: Path,
    events: Sequence[AllCasesLedgerEvent],
    config: AllCasesConfig,
    event_type: str,
    *,
    stage: str,
    reason: str,
    final_status: str | None = None,
) -> None:
    event = _find_event(events, event_type)
    if event is None:
        raise AllCasesIntegrityError("expected skip event is absent")
    stored = _event_payload(
        artifacts_root,
        events,
        event_type,
        schema="systematic_fx.ai_all_cases_stage_skip.v1",
        config=config,
    )
    if (
        _canonical_json_bytes(stored) != _canonical_json_bytes(_skip_document(stage, reason))
        or event.payload.get("reason") != reason
        or (final_status is not None and event.payload.get("final_status") != final_status)
    ):
        raise AllCasesIntegrityError("persisted skip branch differs from source replay")


def _report_document(
    events: Sequence[AllCasesLedgerEvent],
    final_status: str,
    config: AllCasesConfig,
) -> dict[str, object]:
    runtime_identity = _runtime_identity_for_config(config)
    return {
        "event_chain_head_sha256": events[-1].sha256,
        "event_count_before_completion": len(events),
        "final_status": final_status,
        "runtime_identity": runtime_identity,
        "runtime_identity_sha256": _canonical_sha256(runtime_identity),
        "schema": "systematic_fx.ai_all_cases_report.v1",
    }


def _append_enveloped_event(
    ledger: _Ledger,
    artifacts_root: Path,
    config: AllCasesConfig,
    request_sha256: str,
    *,
    event_type: str,
    artifact_field: str,
    artifact_type: str,
    filename_prefix: str,
    schema: str,
    payload: object,
    event_payload: Mapping[str, object],
    resources: _RunResourceGuard | None = None,
) -> tuple[AllCasesLedgerEvent, ...]:
    resources = resources if resources is not None else ledger.resources
    events = ledger.verify()
    _verify_outer_artifact_leaf_set(artifacts_root, events, allow_one_orphan=True)
    identity = _publish_envelope(
        artifacts_root,
        config,
        artifact_type=artifact_type,
        filename_prefix=filename_prefix,
        schema=schema,
        payload=payload,
        referenced_relative_paths=_artifact_relative_paths(events),
        resources=resources,
    )
    ledger.append(
        event_type,
        request_sha256,
        {**event_payload, artifact_field: identity.as_dict()},
    )
    events = ledger.verify()
    _verify_outer_artifact_leaf_set(artifacts_root, events, allow_one_orphan=False)
    return events


def _append_failed(
    ledger: _Ledger,
    request_sha256: str,
    error: AllCasesIntegrityError,
) -> None:
    events = ledger.verify()
    if events and events[-1].event_type not in {"COMPLETED", "FAILED"}:
        code = hashlib.sha256(str(error).encode("utf-8")).hexdigest().upper()[:24]
        ledger.append(
            "FAILED",
            request_sha256,
            {"failure_code": f"INTEGRITY_{code}"},
            enforce_resources=False,
        )


def _append_failed_and_close_artifact_set(
    ledger: _Ledger,
    artifacts_root: Path,
    config: AllCasesConfig,
    run_root: Path,
    request_sha256: str,
    error: AllCasesIntegrityError,
) -> None:
    events = ledger.verify()
    _discard_bounded_outer_publisher_orphan(artifacts_root, events)
    project_root = _project_root_from_run_root(run_root)
    _verify_run_root_tree(run_root, allow_bounded_internal_staging=True)
    _close_internal_prefix_for_terminal_failure(project_root, config)
    _verify_run_root_tree(run_root)
    _verify_terminal_internal_prefixes(project_root, config)
    _append_failed(ledger, request_sha256, error)
    _verify_outer_artifact_leaf_set(artifacts_root, ledger.verify(), allow_one_orphan=False)
    if ledger.resources is not None:
        ledger.resources.check_terminal_bytes("FAILED_TERMINAL")


def _run_with_services(
    root: Path,
    config: AllCasesConfig,
    run_root: Path,
    services: _AllCasesServices,
    *,
    stop_after: str | None = None,
) -> AllCasesRun:
    if stop_after is not None and stop_after not in _EVENT_TYPES:
        raise AllCasesIntegrityError("private stop_after event differs")
    artifacts_root = _safe_directory(run_root / "artifacts", create=True)
    _safe_directory(run_root / "staging/artifacts", create=True)
    ledger_root = _safe_directory(run_root / "ledger", create=True)
    _safe_directory(ledger_root / "events", create=True)
    _safe_directory(ledger_root / "staging", create=True)
    resources = _RunResourceGuard(config, run_root, verifier=False)
    _verify_run_root_tree(
        run_root,
        allow_bounded_internal_staging=True,
        allow_bounded_publisher_staging=True,
    )
    resources.check("MUTATION_PREFLIGHT")
    _recover_linked_publisher_temporaries(run_root)
    probed_events = _Ledger(
        ledger_root,
        create=False,
        allow_bounded_staging=True,
    ).verify()
    if probed_events and probed_events[-1].event_type in {"COMPLETED", "FAILED"}:
        _outer_artifact_staging(run_root, artifacts_root, create=False)
        ledger = _Ledger(ledger_root, create=False, resources=resources)
    else:
        _outer_artifact_staging(run_root, artifacts_root, create=True)
        ledger = _Ledger(ledger_root, create=True, resources=resources)
    _verify_run_root_tree(run_root, allow_bounded_internal_staging=True)
    existing_events = ledger.verify()
    if not existing_events:
        _verify_no_internal_evidence_before_precommit(root)
    elif _find_event(existing_events, "SEARCH_UNIVERSE_FROZEN") is None:
        _verify_search_store_empty_before_universe_release(root)
    _recover_and_verify_internal_prefix(root, config)
    _verify_run_root_tree(run_root)
    preexisting_universe_prefix, preexisting_search_prefix = _internal_source_prefix_presence(
        run_root
    )
    events = _append_precommit(config, ledger, artifacts_root, resources)
    preexisting_event_types = frozenset(event.event_type for event in existing_events)
    _verify_outer_artifact_leaf_set(artifacts_root, events, allow_one_orphan=True)
    request_sha256 = events[0].request_sha256
    if events[-1].event_type not in {"COMPLETED", "FAILED"}:
        try:
            orphan = _verify_outer_artifact_leaf_set(artifacts_root, events, allow_one_orphan=True)
            if orphan is not None:
                _discard_bounded_outer_publisher_orphan(artifacts_root, events)
                if _find_event(events, "HOLDOUT_AUTHORIZED") is not None:
                    integrity = AllCasesIntegrityError(
                        "post-authorization outer publisher orphan is terminal-invalid"
                    )
                    _append_failed_and_close_artifact_set(
                        ledger,
                        artifacts_root,
                        config,
                        run_root,
                        request_sha256,
                        integrity,
                    )
                    return _run_value(config, run_root, ledger.verify())
        except AllCasesIntegrityError as error:
            _append_failed_and_close_artifact_set(
                ledger, artifacts_root, config, run_root, request_sha256, error
            )
            raise
    if events[-1].event_type == "COMPLETED":
        return _verify_with_services(root, config, run_root, services)
    if events[-1].event_type == "FAILED" or stop_after == events[-1].event_type:
        _verify_outer_artifact_leaf_set(
            artifacts_root,
            events,
            allow_one_orphan=events[-1].event_type not in {"COMPLETED", "FAILED"},
        )
        return _run_value(config, run_root, events)
    _verify_released_internal_closures(config, run_root, events)

    # A holdout result and its report/completion are one non-resumable terminal
    # transaction.  A process that observes result-only bytes cannot authenticate
    # them without reopening the one-shot payload and therefore records no
    # conclusion.
    if _find_event(events, "HOLDOUT_RESULTS_RELEASED") is not None:
        integrity = AllCasesIntegrityError(
            "pre-existing holdout result cannot be finalized after restart"
        )
        _append_failed_and_close_artifact_set(
            ledger, artifacts_root, config, run_root, request_sha256, integrity
        )
        return _run_value(config, run_root, ledger.verify())

    # Authorization is the durable one-shot boundary.  A process that did not
    # itself publish that event may never resume feature freezing or reopen the
    # holdout payload.  A durable result is different: only its report may be
    # finalized, using the already-published bytes below and no service call.
    if (
        _find_event(events, "HOLDOUT_AUTHORIZED") is not None
        and _find_event(events, "HOLDOUT_RESULTS_RELEASED") is None
    ):
        integrity = AllCasesIntegrityError(
            "pre-existing one-shot holdout authorization cannot be resumed"
        )
        _append_failed_and_close_artifact_set(
            ledger, artifacts_root, config, run_root, request_sha256, integrity
        )
        return _run_value(config, run_root, ledger.verify())

    holdout_one_shot_boundary_crossed = _find_event(events, "HOLDOUT_AUTHORIZED") is not None
    try:
        walk_decision_dates: tuple[str, ...] | None = None
        walk_fold_lengths: tuple[int, ...] | None = None
        holdout_decision_dates: tuple[str, ...] | None = None
        resources.check("RUN_START")
        universe_prefix_replay: (
            tuple[bool, str | None, int | None, dict[str, object] | None] | None
        ) = None
        if preexisting_universe_prefix and services.replay_search_universe_prefix is not None:
            resources.check("BEFORE_SEARCH_UNIVERSE_PREFIX_SOURCE_REPLAY")
            universe_prefix_replay = _validated_prefix_replay(
                services.replay_search_universe_prefix(root, config),
                label="Search-universe prefix replay",
            )
            resources.check("AFTER_SEARCH_UNIVERSE_PREFIX_SOURCE_REPLAY")
            complete, next_phase, next_index, _payload = universe_prefix_replay
            if not complete and (
                next_phase != "SEARCH_UNIVERSE" or next_index is None or next_index >= 64
            ):
                raise AllCasesIntegrityError("Search-universe prefix replay coordinate differs")
        if _find_event(events, "SEARCH_UNIVERSE_FROZEN") is None:
            resources.check("BEFORE_SEARCH_UNIVERSE")
            if universe_prefix_replay is not None and universe_prefix_replay[0]:
                replay_payload = universe_prefix_replay[3]
                if replay_payload is None:  # pragma: no cover - tuple invariant above
                    raise AllCasesIntegrityError("Search-universe prefix replay omitted payload")
                universe = _validate_search_universe(replay_payload, config)
            else:
                universe = _validate_search_universe(
                    services.freeze_search_universe(root, config), config
                )
            resources.check("AFTER_SEARCH_UNIVERSE")
            events = _append_enveloped_event(
                ledger,
                artifacts_root,
                config,
                request_sha256,
                event_type="SEARCH_UNIVERSE_FROZEN",
                artifact_field="universe_artifact",
                artifact_type="AI_ALL_CASES_SEARCH_FEATURE_EVENT_UNIVERSE",
                filename_prefix="search-universe",
                schema="systematic_fx.ai_all_cases_search_universe.v1",
                payload=universe,
                event_payload={"universe_root_sha256": universe["universe_root_sha256"]},
            )
        if stop_after == events[-1].event_type:
            return _run_value(config, run_root, events)

        universe = _event_payload(
            artifacts_root,
            events,
            "SEARCH_UNIVERSE_FROZEN",
            schema="systematic_fx.ai_all_cases_search_universe.v1",
            config=config,
        )
        universe = _validate_search_universe(universe, config)
        _verify_internal_universe_release(root, config, universe)
        if "SEARCH_UNIVERSE_FROZEN" in preexisting_event_types:
            if universe_prefix_replay is not None:
                if not universe_prefix_replay[0] or universe_prefix_replay[3] is None:
                    raise AllCasesIntegrityError(
                        "released Search universe has an incomplete source prefix"
                    )
                replayed_universe = _validate_search_universe(universe_prefix_replay[3], config)
            else:
                resources.check("BEFORE_SEARCH_UNIVERSE_SOURCE_REPLAY")
                replayed_universe = _validate_search_universe(
                    services.freeze_search_universe(root, config), config
                )
                resources.check("AFTER_SEARCH_UNIVERSE_SOURCE_REPLAY")
            _compare_payload(universe, replayed_universe, label="Search universe resume")
            universe = replayed_universe
        _verify_search_prefix_semantics(root, config, universe)
        search_prefix_replay: (
            tuple[bool, str | None, int | None, dict[str, object] | None] | None
        ) = None
        if preexisting_search_prefix and services.replay_search_prefix is not None:
            resources.check("BEFORE_SEARCH_PREFIX_SOURCE_REPLAY")
            search_prefix_replay = _validated_prefix_replay(
                services.replay_search_prefix(root, config, universe),
                label="Search prefix replay",
            )
            resources.check("AFTER_SEARCH_PREFIX_SOURCE_REPLAY")
        if _find_event(events, "SEARCH_RESULTS_RELEASED") is None:
            resources.check("BEFORE_SEARCH_RESULTS")
            if search_prefix_replay is not None and search_prefix_replay[0]:
                replay_payload = search_prefix_replay[3]
                if replay_payload is None:  # pragma: no cover - tuple invariant above
                    raise AllCasesIntegrityError("Search prefix replay omitted payload")
                search, _evaluated, selected = _validate_search_result(
                    replay_payload, universe, config
                )
            else:
                search, _evaluated, selected = _validate_search_result(
                    services.train_select_search(root, config, universe), universe, config
                )
            # The complete internal Search prefix now exists, but no public
            # outcome-bearing Search event does.  Authenticate both its
            # scientific semantics and its exact release closure before the
            # outer ledger can cross SEARCH_RESULTS_RELEASED.
            _verify_search_prefix_semantics(root, config, universe)
            _verify_internal_search_release(root, config, search)
            resources.check("AFTER_SEARCH_RESULTS")
            events = _append_enveloped_event(
                ledger,
                artifacts_root,
                config,
                request_sha256,
                event_type="SEARCH_RESULTS_RELEASED",
                artifact_field="result_artifact",
                artifact_type="AI_ALL_CASES_SEARCH_RESULTS",
                filename_prefix="search-results",
                schema="systematic_fx.ai_all_cases_search_results.v1",
                payload=search,
                event_payload={"selected_candidate_ids": list(selected)},
            )
        if stop_after == events[-1].event_type:
            return _run_value(config, run_root, events)

        search = _event_payload(
            artifacts_root,
            events,
            "SEARCH_RESULTS_RELEASED",
            schema="systematic_fx.ai_all_cases_search_results.v1",
            config=config,
        )
        search, _evaluated, selected = _validate_search_result(search, universe, config)
        if "SEARCH_RESULTS_RELEASED" in preexisting_event_types:
            if search_prefix_replay is not None:
                if not search_prefix_replay[0] or search_prefix_replay[3] is None:
                    raise AllCasesIntegrityError(
                        "released Search result has an incomplete source prefix"
                    )
                replayed_search, _replayed_evaluated, replayed_selected = _validate_search_result(
                    search_prefix_replay[3], universe, config
                )
            else:
                resources.check("BEFORE_SEARCH_RESULTS_SOURCE_REPLAY")
                replayed_search, _replayed_evaluated, replayed_selected = _validate_search_result(
                    services.train_select_search(root, config, universe), universe, config
                )
                resources.check("AFTER_SEARCH_RESULTS_SOURCE_REPLAY")
            _compare_payload(search, replayed_search, label="Search result resume")
            if replayed_selected != selected:
                raise AllCasesIntegrityError("Search resume selection differs")
            search = replayed_search
        _verify_internal_search_release(root, config, search)
        candidate_descriptors = _search_candidate_descriptors(search, selected)
        if not selected:
            final_status = "NO_SEARCH_FINALISTS_HOLDOUT_NOT_OPENED"
            if _find_event(events, "WALK_FORWARD_SKIPPED") is None:
                skip = _skip_document("WALK_FORWARD", "NO_SEARCH_FINALISTS")
                events = _append_enveloped_event(
                    ledger,
                    artifacts_root,
                    config,
                    request_sha256,
                    event_type="WALK_FORWARD_SKIPPED",
                    artifact_field="skip_artifact",
                    artifact_type="AI_ALL_CASES_WALK_FORWARD_SKIPPED",
                    filename_prefix="walk-forward-skipped",
                    schema="systematic_fx.ai_all_cases_stage_skip.v1",
                    payload=skip,
                    event_payload={"reason": "NO_SEARCH_FINALISTS"},
                )
            else:
                _validate_existing_skip_event(
                    artifacts_root,
                    events,
                    config,
                    "WALK_FORWARD_SKIPPED",
                    stage="WALK_FORWARD",
                    reason="NO_SEARCH_FINALISTS",
                )
            if stop_after == events[-1].event_type:
                return _run_value(config, run_root, events)
            if _find_event(events, "HOLDOUT_SKIPPED") is None:
                skip = _skip_document("HOLDOUT", final_status)
                events = _append_enveloped_event(
                    ledger,
                    artifacts_root,
                    config,
                    request_sha256,
                    event_type="HOLDOUT_SKIPPED",
                    artifact_field="skip_artifact",
                    artifact_type="AI_ALL_CASES_HOLDOUT_SKIPPED",
                    filename_prefix="holdout-skipped",
                    schema="systematic_fx.ai_all_cases_stage_skip.v1",
                    payload=skip,
                    event_payload={"final_status": final_status, "reason": final_status},
                )
            else:
                _validate_existing_skip_event(
                    artifacts_root,
                    events,
                    config,
                    "HOLDOUT_SKIPPED",
                    stage="HOLDOUT",
                    reason=final_status,
                    final_status=final_status,
                )
            if stop_after == events[-1].event_type:
                return _run_value(config, run_root, events)
            return _complete(
                ledger,
                artifacts_root,
                config,
                request_sha256,
                events,
                final_status,
            )

        oos_domains = _production_decision_date_domains(root, config)
        if oos_domains is not None:
            walk_decision_dates, walk_fold_lengths, holdout_decision_dates = oos_domains

        if _find_event(events, "WALK_FORWARD_MASKS_FROZEN") is None:
            resources.check("BEFORE_WALK_FORWARD_MASKS")
            masks = _validate_walk_masks(
                services.freeze_walk_forward_masks(root, config, selected, search), selected
            )
            resources.check("AFTER_WALK_FORWARD_MASKS")
            events = _append_enveloped_event(
                ledger,
                artifacts_root,
                config,
                request_sha256,
                event_type="WALK_FORWARD_MASKS_FROZEN",
                artifact_field="masks_artifact",
                artifact_type="AI_ALL_CASES_WALK_FORWARD_MASKS",
                filename_prefix="walk-forward-masks",
                schema="systematic_fx.ai_all_cases_walk_forward_masks.v1",
                payload=masks,
                event_payload={
                    "candidate_ids": list(selected),
                    "fold_keys": list(WALK_FORWARD_FOLD_KEYS),
                },
            )
        if stop_after == events[-1].event_type:
            return _run_value(config, run_root, events)
        masks = _event_payload(
            artifacts_root,
            events,
            "WALK_FORWARD_MASKS_FROZEN",
            schema="systematic_fx.ai_all_cases_walk_forward_masks.v1",
            config=config,
        )
        masks = _validate_walk_masks(masks, selected)
        if "WALK_FORWARD_MASKS_FROZEN" in preexisting_event_types:
            resources.check("BEFORE_WALK_FORWARD_MASK_SOURCE_REPLAY")
            replayed_masks = _validate_walk_masks(
                services.freeze_walk_forward_masks(root, config, selected, search), selected
            )
            resources.check("AFTER_WALK_FORWARD_MASK_SOURCE_REPLAY")
            _compare_payload(masks, replayed_masks, label="walk-forward masks resume")
            masks = replayed_masks
        if _find_event(events, "WALK_FORWARD_RESULTS_RELEASED") is None:
            resources.check("BEFORE_WALK_FORWARD_RESULTS")
            walk, finalists = _validate_walk_result(
                services.evaluate_walk_forward(root, config, selected, masks, search),
                selected,
                expected_decision_dates=walk_decision_dates,
                expected_fold_lengths=walk_fold_lengths,
                expected_candidate_descriptors=candidate_descriptors,
            )
            resources.check("AFTER_WALK_FORWARD_RESULTS")
            events = _append_enveloped_event(
                ledger,
                artifacts_root,
                config,
                request_sha256,
                event_type="WALK_FORWARD_RESULTS_RELEASED",
                artifact_field="result_artifact",
                artifact_type="AI_ALL_CASES_WALK_FORWARD_RESULTS",
                filename_prefix="walk-forward-results",
                schema="systematic_fx.ai_all_cases_walk_forward_results.v1",
                payload=walk,
                event_payload={"finalist_candidate_ids": list(finalists)},
            )
        if stop_after == events[-1].event_type:
            return _run_value(config, run_root, events)
        walk = _event_payload(
            artifacts_root,
            events,
            "WALK_FORWARD_RESULTS_RELEASED",
            schema="systematic_fx.ai_all_cases_walk_forward_results.v1",
            config=config,
        )
        walk, finalists = _validate_walk_result(
            walk,
            selected,
            expected_decision_dates=walk_decision_dates,
            expected_fold_lengths=walk_fold_lengths,
            expected_candidate_descriptors=candidate_descriptors,
        )
        if "WALK_FORWARD_RESULTS_RELEASED" in preexisting_event_types:
            resources.check("BEFORE_WALK_FORWARD_RESULTS_SOURCE_REPLAY")
            replayed_walk, replayed_finalists = _validate_walk_result(
                services.evaluate_walk_forward(root, config, selected, masks, search),
                selected,
                expected_decision_dates=walk_decision_dates,
                expected_fold_lengths=walk_fold_lengths,
                expected_candidate_descriptors=candidate_descriptors,
            )
            resources.check("AFTER_WALK_FORWARD_RESULTS_SOURCE_REPLAY")
            _compare_payload(walk, replayed_walk, label="walk-forward result resume")
            if replayed_finalists != finalists:
                raise AllCasesIntegrityError("walk-forward resume finalists differ")
            walk = replayed_walk
        if not finalists:
            final_status = "NO_WALK_FORWARD_FINALISTS_HOLDOUT_NOT_OPENED"
            if _find_event(events, "HOLDOUT_SKIPPED") is None:
                skip = _skip_document("HOLDOUT", final_status)
                events = _append_enveloped_event(
                    ledger,
                    artifacts_root,
                    config,
                    request_sha256,
                    event_type="HOLDOUT_SKIPPED",
                    artifact_field="skip_artifact",
                    artifact_type="AI_ALL_CASES_HOLDOUT_SKIPPED",
                    filename_prefix="holdout-skipped",
                    schema="systematic_fx.ai_all_cases_stage_skip.v1",
                    payload=skip,
                    event_payload={"final_status": final_status, "reason": final_status},
                )
            else:
                _validate_existing_skip_event(
                    artifacts_root,
                    events,
                    config,
                    "HOLDOUT_SKIPPED",
                    stage="HOLDOUT",
                    reason=final_status,
                    final_status=final_status,
                )
            if stop_after == events[-1].event_type:
                return _run_value(config, run_root, events)
            return _complete(ledger, artifacts_root, config, request_sha256, events, final_status)

        authorization = _authorization_document(config, finalists)
        if _find_event(events, "HOLDOUT_AUTHORIZED") is None:
            resources.check("BEFORE_HOLDOUT_AUTHORIZATION")
            identity = _publish(
                artifacts_root,
                artifact_type="AI_ALL_CASES_HOLDOUT_AUTHORIZATION",
                filename_prefix="holdout-authorization",
                document=authorization,
                referenced_relative_paths=_artifact_relative_paths(events),
                resources=resources,
            )
            ledger.append(
                "HOLDOUT_AUTHORIZED",
                request_sha256,
                {
                    "authorization_artifact": identity.as_dict(),
                    "family_sha256": authorization["family_sha256"],
                    "finalist_candidate_ids": list(finalists),
                },
            )
            events = ledger.verify()
            holdout_one_shot_boundary_crossed = True
            _verify_outer_artifact_leaf_set(artifacts_root, events, allow_one_orphan=False)
            resources.check("AFTER_HOLDOUT_AUTHORIZATION")
        else:
            holdout_one_shot_boundary_crossed = True
            _verify_artifact(
                artifacts_root,
                _artifact_from_event(_find_event(events, "HOLDOUT_AUTHORIZED")),  # type: ignore[arg-type]
                expected_bytes=_canonical_json_bytes(authorization),
            )
        if stop_after == events[-1].event_type:
            return _run_value(config, run_root, events)

        if _find_event(events, "HOLDOUT_MASKS_FROZEN") is None:
            resources.check("BEFORE_HOLDOUT_MASKS")
            holdout_masks = _validate_holdout_masks(
                services.freeze_holdout_masks(root, config, finalists, walk), finalists
            )
            resources.check("AFTER_HOLDOUT_MASKS")
            events = _append_enveloped_event(
                ledger,
                artifacts_root,
                config,
                request_sha256,
                event_type="HOLDOUT_MASKS_FROZEN",
                artifact_field="masks_artifact",
                artifact_type="AI_ALL_CASES_HOLDOUT_MASKS",
                filename_prefix="holdout-masks",
                schema="systematic_fx.ai_all_cases_holdout_masks.v1",
                payload=holdout_masks,
                event_payload={"candidate_ids": list(finalists)},
            )
        if stop_after == events[-1].event_type:
            return _run_value(config, run_root, events)
        holdout_masks = _event_payload(
            artifacts_root,
            events,
            "HOLDOUT_MASKS_FROZEN",
            schema="systematic_fx.ai_all_cases_holdout_masks.v1",
            config=config,
        )
        holdout_masks = _validate_holdout_masks(holdout_masks, finalists)
        if _find_event(events, "HOLDOUT_RESULTS_RELEASED") is None:
            resources.check("BEFORE_HOLDOUT_RESULTS")
            holdout, classification = _validate_holdout_result(
                services.evaluate_holdout(root, config, finalists, holdout_masks),
                finalists,
                expected_decision_dates=holdout_decision_dates,
                expected_candidate_descriptors={
                    candidate_id: candidate_descriptors[candidate_id] for candidate_id in finalists
                },
            )
            resources.check("AFTER_HOLDOUT_RESULTS")
            events = _append_enveloped_event(
                ledger,
                artifacts_root,
                config,
                request_sha256,
                event_type="HOLDOUT_RESULTS_RELEASED",
                artifact_field="result_artifact",
                artifact_type="AI_ALL_CASES_HOLDOUT_RESULTS",
                filename_prefix="holdout-results",
                schema="systematic_fx.ai_all_cases_holdout_results.v1",
                payload=holdout,
                event_payload={"classification": classification},
            )
        if stop_after == events[-1].event_type:
            return _run_value(config, run_root, events)
        holdout = _event_payload(
            artifacts_root,
            events,
            "HOLDOUT_RESULTS_RELEASED",
            schema="systematic_fx.ai_all_cases_holdout_results.v1",
            config=config,
        )
        _holdout, classification = _validate_holdout_result(
            holdout,
            finalists,
            expected_decision_dates=holdout_decision_dates,
            expected_candidate_descriptors={
                candidate_id: candidate_descriptors[candidate_id] for candidate_id in finalists
            },
        )
        return _complete(ledger, artifacts_root, config, request_sha256, events, classification)
    except AllCasesIntegrityError as error:
        _append_failed_and_close_artifact_set(
            ledger, artifacts_root, config, run_root, request_sha256, error
        )
        raise
    except OSError as error:
        if (
            holdout_one_shot_boundary_crossed
            or _find_event(ledger.verify(), "HOLDOUT_AUTHORIZED") is not None
        ):
            integrity = AllCasesIntegrityError(
                f"one-shot holdout service failure {type(error).__name__}: {error}"
            )
            _append_failed_and_close_artifact_set(
                ledger, artifacts_root, config, run_root, request_sha256, integrity
            )
            raise integrity from error
        raise
    except Exception as error:
        integrity = AllCasesIntegrityError(
            f"deterministic service failure {type(error).__name__}: {error}"
        )
        _append_failed_and_close_artifact_set(
            ledger, artifacts_root, config, run_root, request_sha256, integrity
        )
        raise integrity from error


def _complete(
    ledger: _Ledger,
    artifacts_root: Path,
    config: AllCasesConfig,
    request_sha256: str,
    events: tuple[AllCasesLedgerEvent, ...],
    final_status: str,
    resources: _RunResourceGuard | None = None,
) -> AllCasesRun:
    resources = resources if resources is not None else ledger.resources
    existing = _find_event(events, "COMPLETED")
    if existing is None:
        run_root = ledger.root.parent
        # This is the terminal validation barrier.  Every replay, source
        # semantic check, request/artifact closure, and terminal-internal
        # assertion that can fail runs while FAILED is still a legal next
        # event.  COMPLETED is never used as a promise to validate later.
        prefix_result = _run_value(config, run_root, events)
        _verify_terminal_internal_prefixes(_project_root_from_run_root(run_root), config)
        _verify_outer_artifact_leaf_set(artifacts_root, events, allow_one_orphan=False)
        if resources is not None:
            resources.check("BEFORE_COMPLETION_REPORT")
        report = _report_document(events, final_status, config)
        identity = _publish_envelope(
            artifacts_root,
            config,
            artifact_type="AI_ALL_CASES_REPORT",
            filename_prefix="all-cases-report",
            schema="systematic_fx.ai_all_cases_report_envelope.v1",
            payload=report,
            referenced_relative_paths=_artifact_relative_paths(events),
            resources=resources,
        )
        expected_report_bytes = _canonical_json_bytes(
            _envelope(
                config,
                schema="systematic_fx.ai_all_cases_report_envelope.v1",
                payload=report,
            )
        )
        _verify_artifact(artifacts_root, identity, expected_bytes=expected_report_bytes)
        orphan = _verify_outer_artifact_leaf_set(
            artifacts_root,
            events,
            allow_one_orphan=True,
        )
        if orphan != identity.relative_path:
            raise AllCasesIntegrityError("completion report is not the sole bounded orphan")
        if resources is not None:
            resources.check("BEFORE_COMPLETED")
            resources.check_terminal_bytes("BEFORE_COMPLETED_TERMINAL")
        completed_result = AllCasesRun(
            config=config,
            status="COMPLETED",
            request_artifact=prefix_result.request_artifact,
            evidence_artifacts=(*prefix_result.evidence_artifacts, identity),
            finalist_candidate_ids=prefix_result.finalist_candidate_ids,
            root=run_root,
            event_count=len(events) + 1,
        )
        ledger.append(
            "COMPLETED",
            request_sha256,
            {"final_status": final_status, "report_artifact": identity.as_dict()},
            verify_after_append=False,
        )
        # No semantic, source, request, artifact, report, resource, or ledger
        # replay follows the final durable lifecycle publication.
        return completed_result
    elif existing.payload["final_status"] != final_status:
        raise AllCasesIntegrityError("completed status differs from replay")
    _verify_outer_artifact_leaf_set(artifacts_root, events, allow_one_orphan=False)
    return _run_value(config, ledger.root.parent, events)


def _default_services(*, verify_only: bool = False) -> _AllCasesServices:
    """Return the fixed production adapter once the campaign pipeline lands.

    Keeping this import private prevents callers from injecting loaders through
    any public interface.  The pipeline module is part of the same provenance
    closure and may only be introduced before the data-only precommit.
    """

    try:
        from .pipeline import production_services
    except ImportError as error:
        raise AllCasesRunError("all-cases production services are not linked yet") from error
    services = production_services(verify_only=verify_only)
    if not isinstance(services, _AllCasesServices):
        raise AllCasesIntegrityError("production service bundle type differs")
    return services


def _prepare_mutation(project_root: Path | str) -> tuple[Path, AllCasesConfig, Path]:
    root = _project_root(project_root)
    _require_trusted_bootstrap_runtime(root)
    config = load_ai_all_cases_config(root)
    _load_validated_dataset_contract(root)
    verify_failed_predecessor_attempt(root)
    verify_failed_attempt2_predecessor(root)
    verify_failed_attempt3_predecessor(root)
    verify_failed_attempt4_predecessor(root)
    run_root = _fixed_run_root(root, create=True)
    return root, config, run_root


def precommit_ai_all_cases(project_root: Path | str) -> AllCasesRun:
    """Create only the immutable request and PRECOMMITTED event."""

    _root, config, run_root = _prepare_mutation(project_root)
    with _exclusive_mutation(run_root):
        resources = _RunResourceGuard(config, run_root, verifier=False)
        _verify_run_root_tree(
            run_root,
            allow_bounded_internal_staging=True,
            allow_bounded_publisher_staging=True,
        )
        resources.check("PRECOMMIT_MUTATION_PREFLIGHT")
        _recover_linked_publisher_temporaries(run_root)
        _verify_run_root_tree(
            run_root,
            allow_bounded_internal_staging=True,
            allow_bounded_publisher_staging=True,
        )
        ledger_path = run_root / "ledger"
        if ledger_path.is_dir():
            existing = _Ledger(
                ledger_path,
                create=False,
                allow_bounded_staging=True,
            ).verify()
            if len(existing) > 1 or (existing and existing[0].event_type != "PRECOMMITTED"):
                raise AllCasesRunError(
                    "campaign already advanced beyond PRECOMMITTED; use run or verify"
                )
        artifacts_root = _safe_directory(run_root / "artifacts", create=True)
        _outer_artifact_staging(run_root, artifacts_root, create=True)
        ledger = _Ledger(run_root / "ledger", create=True, resources=resources)
        _verify_run_root_tree(run_root, allow_bounded_internal_staging=True)
        existing_events = ledger.verify()
        if not existing_events:
            _verify_no_internal_evidence_before_precommit(_root)
        elif _find_event(existing_events, "SEARCH_UNIVERSE_FROZEN") is None:
            _verify_search_store_empty_before_universe_release(_root)
        _recover_and_verify_internal_prefix(_root, config)
        _verify_run_root_tree(run_root)
        resources.check("PRECOMMIT_START")
        events = _append_precommit(config, ledger, artifacts_root, resources)
        _verify_outer_artifact_leaf_set(artifacts_root, events, allow_one_orphan=False)
        resources.check("PRECOMMIT_END")
        return _run_value(config, run_root, events)


def run_ai_all_cases(project_root: Path | str) -> AllCasesRun:
    """Resume the sole governed budget using fixed production services."""

    root, config, run_root = _prepare_mutation(project_root)
    with _exclusive_mutation(run_root):
        resources = _RunResourceGuard(config, run_root, verifier=False)
        _verify_run_root_tree(
            run_root,
            allow_bounded_internal_staging=True,
            allow_bounded_publisher_staging=True,
        )
        resources.check("RUN_PUBLIC_MUTATION_PREFLIGHT")
        _recover_linked_publisher_temporaries(run_root)
        _verify_run_root_tree(
            run_root,
            allow_bounded_internal_staging=True,
            allow_bounded_publisher_staging=True,
        )
        ledger_path = run_root / "ledger"
        if ledger_path.is_dir():
            events = _Ledger(
                ledger_path,
                create=False,
                allow_bounded_staging=True,
            ).verify()
            if events and events[-1].event_type in {"COMPLETED", "FAILED"}:
                artifacts_root = _safe_directory(run_root / "artifacts", create=False)
                _outer_artifact_staging(run_root, artifacts_root, create=False)
                _Ledger(ledger_path, create=False)
                _verify_outer_artifact_leaf_set(artifacts_root, events, allow_one_orphan=False)
                if events[-1].event_type == "COMPLETED":
                    return _verify_with_services(
                        root,
                        config,
                        run_root,
                        _default_services(verify_only=True),
                    )
                return _run_value(config, run_root, events)
        return _run_with_services(root, config, run_root, _default_services())


def _tree_snapshot(root: Path) -> tuple[tuple[str, int, int, str], ...]:
    rows = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise AllCasesIntegrityError("verification tree contains a symbolic path")
        relative = path.relative_to(root).as_posix()
        metadata = path.stat()
        digest = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else "DIRECTORY"
        rows.append((relative, stat.S_IMODE(metadata.st_mode), metadata.st_size, digest))
    return tuple(rows)


def _compare_payload(expected: object, actual: object, *, label: str) -> None:
    if _canonical_json_bytes(_json_document(actual, label=label)) != _canonical_json_bytes(
        expected
    ):
        raise AllCasesIntegrityError(f"fresh {label} recomputation differs")


def _validated_prefix_replay(
    value: object,
    *,
    label: str,
) -> tuple[bool, str | None, int | None, dict[str, object] | None]:
    """Decode the fixed read-only source-prefix replay result without coercion."""

    document = _json_document(value, label=label)
    if set(document) != {"complete", "next_chunk_index", "next_phase", "payload"}:
        raise AllCasesIntegrityError(f"{label} schema differs")
    complete = document["complete"]
    if type(complete) is not bool:
        raise AllCasesIntegrityError(f"{label} completion flag differs")
    next_phase = document["next_phase"]
    next_chunk_index = document["next_chunk_index"]
    payload = document["payload"]
    if complete:
        if next_phase is not None or next_chunk_index is not None or not isinstance(payload, dict):
            raise AllCasesIntegrityError(f"{label} complete coordinate differs")
        return True, None, None, payload
    if (
        not isinstance(next_phase, str)
        or not next_phase
        or type(next_chunk_index) is not int
        or next_chunk_index < 0
        or payload is not None
    ):
        raise AllCasesIntegrityError(f"{label} incomplete coordinate differs")
    return False, next_phase, next_chunk_index, None


def _internal_source_prefix_presence(run_root: Path) -> tuple[bool, bool]:
    """Return whether immutable universe/Search source coordinates predate this invocation."""

    universe_root = run_root / "internal/universe"
    search_events = run_root / "internal/search/events"
    universe_present = universe_root.is_dir() and any(universe_root.iterdir())
    search_present = search_events.is_dir() and any(search_events.iterdir())
    return universe_present, search_present


def _verify_with_services(
    root: Path,
    config: AllCasesConfig,
    run_root: Path,
    services: _AllCasesServices,
) -> AllCasesRun:
    """Read-only replay plus exact fresh recomputation of every released stage."""

    resources = _RunResourceGuard(config, run_root, verifier=True)
    _verify_run_root_tree(run_root)
    resources.check("VERIFY_START")
    before = _tree_snapshot(run_root)
    artifacts_root = _safe_directory(run_root / "artifacts", create=False)
    _outer_artifact_staging(run_root, artifacts_root, create=False)
    ledger = _Ledger(run_root / "ledger", create=False)
    events = ledger.verify()
    if not events:
        raise AllCasesIntegrityError("run has not been precommitted")
    if (
        _find_event(events, "HOLDOUT_RESULTS_RELEASED") is not None
        and _find_event(events, "COMPLETED") is None
    ):
        raise AllCasesIntegrityError("holdout result without atomic completion is terminal-invalid")
    _verify_outer_artifact_leaf_set(artifacts_root, events, allow_one_orphan=False)
    _verify_released_internal_closures(config, run_root, events)
    request = _request_document(config)
    if events[0].request_sha256 != _canonical_sha256(request):
        raise AllCasesIntegrityError("request differs during fresh verification")
    _verify_artifact(
        artifacts_root,
        _artifact_from_event(events[0]),
        expected_bytes=_canonical_json_bytes(request),
    )
    walk_decision_dates: tuple[str, ...] | None = None
    walk_fold_lengths: tuple[int, ...] | None = None
    holdout_decision_dates: tuple[str, ...] | None = None

    def finish_prefix() -> AllCasesRun:
        resources.check("VERIFY_FINISH")
        result = _run_value(config, run_root, events)
        if _tree_snapshot(run_root) != before:
            raise AllCasesIntegrityError("verification mutated the run tree")
        return result

    def fresh_service(label: str, operation: Callable[[], object]) -> object:
        try:
            return operation()
        except AllCasesIntegrityError:
            raise
        except Exception as error:
            raise AllCasesIntegrityError(
                f"fresh {label} service failed: {type(error).__name__}: {error}"
            ) from error

    failed_terminal = events[-1].event_type == "FAILED"
    lifecycle_events = events[:-1] if failed_terminal else events
    if failed_terminal and _find_event(lifecycle_events, "COMPLETED") is not None:
        raise AllCasesIntegrityError("FAILED ledger also contains completion")
    actual_types = tuple(item.event_type for item in lifecycle_events)

    def require_prefix(expected: Sequence[str]) -> None:
        expected_types = tuple(expected)
        if actual_types != expected_types[: len(actual_types)]:
            raise AllCasesIntegrityError("ledger branch differs from fresh lifecycle replay")

    def validate_completion(expected_status: str, expected_types: Sequence[str]) -> None:
        require_prefix(expected_types)
        completed = _find_event(events, "COMPLETED")
        if completed is None:
            return
        if completed is not events[-1] or completed.payload["final_status"] != expected_status:
            raise AllCasesIntegrityError("completion status differs from fresh lifecycle replay")
        stored_report = _event_payload(
            artifacts_root,
            events,
            "COMPLETED",
            schema="systematic_fx.ai_all_cases_report_envelope.v1",
            config=config,
        )
        _compare_payload(
            stored_report,
            _report_document(events[:-1], expected_status, config),
            label="completion report",
        )

    def validate_skip(
        event_type: str,
        *,
        stage: str,
        reason: str,
        final_status: str | None = None,
    ) -> None:
        event = _find_event(events, event_type)
        if event is None:
            return
        expected_payload = _skip_document(stage, reason)
        stored = _event_payload(
            artifacts_root,
            events,
            event_type,
            schema="systematic_fx.ai_all_cases_stage_skip.v1",
            config=config,
        )
        _compare_payload(stored, expected_payload, label=f"{stage} skip")
        if event.payload["reason"] != reason or (
            final_status is not None and event.payload["final_status"] != final_status
        ):
            raise AllCasesIntegrityError("skip event metadata differs from fresh replay")

    if _find_event(events, "SEARCH_UNIVERSE_FROZEN") is None:
        require_prefix(("PRECOMMITTED",))
        return finish_prefix()

    stored_universe = _event_payload(
        artifacts_root,
        events,
        "SEARCH_UNIVERSE_FROZEN",
        schema="systematic_fx.ai_all_cases_search_universe.v1",
        config=config,
    )
    resources.check("VERIFY_BEFORE_SEARCH_UNIVERSE")
    fresh_universe = _validate_search_universe(
        fresh_service("Search universe", lambda: services.freeze_search_universe(root, config)),
        config,
    )
    resources.check("VERIFY_AFTER_SEARCH_UNIVERSE")
    _compare_payload(stored_universe, fresh_universe, label="Search universe")
    universe_event = _find_event(events, "SEARCH_UNIVERSE_FROZEN")
    if (
        universe_event is None
        or universe_event.payload["universe_root_sha256"] != fresh_universe["universe_root_sha256"]
    ):
        raise AllCasesIntegrityError("Search-universe event metadata differs")
    if _find_event(events, "SEARCH_RESULTS_RELEASED") is None:
        require_prefix(("PRECOMMITTED", "SEARCH_UNIVERSE_FROZEN"))
        return finish_prefix()
    stored_search = _event_payload(
        artifacts_root,
        events,
        "SEARCH_RESULTS_RELEASED",
        schema="systematic_fx.ai_all_cases_search_results.v1",
        config=config,
    )
    resources.check("VERIFY_BEFORE_SEARCH_RESULTS")
    fresh_search, _evaluated, selected = _validate_search_result(
        fresh_service(
            "Search result",
            lambda: services.train_select_search(root, config, fresh_universe),
        ),
        fresh_universe,
        config,
    )
    resources.check("VERIFY_AFTER_SEARCH_RESULTS")
    _compare_payload(stored_search, fresh_search, label="Search result")
    search_event = _find_event(events, "SEARCH_RESULTS_RELEASED")
    if search_event is None or tuple(search_event.payload["selected_candidate_ids"]) != selected:
        raise AllCasesIntegrityError("Search-result event metadata differs")
    candidate_descriptors = _search_candidate_descriptors(fresh_search, selected)
    base = ("PRECOMMITTED", "SEARCH_UNIVERSE_FROZEN", "SEARCH_RESULTS_RELEASED")
    if not selected:
        final_status = "NO_SEARCH_FINALISTS_HOLDOUT_NOT_OPENED"
        expected = (
            *base,
            "WALK_FORWARD_SKIPPED",
            "HOLDOUT_SKIPPED",
            "COMPLETED",
        )
        require_prefix(expected)
        validate_skip(
            "WALK_FORWARD_SKIPPED",
            stage="WALK_FORWARD",
            reason="NO_SEARCH_FINALISTS",
        )
        validate_skip(
            "HOLDOUT_SKIPPED",
            stage="HOLDOUT",
            reason=final_status,
            final_status=final_status,
        )
        validate_completion(final_status, expected)
        return finish_prefix()

    oos_domains = _production_decision_date_domains(root, config)
    if oos_domains is not None:
        walk_decision_dates, walk_fold_lengths, holdout_decision_dates = oos_domains

    if _find_event(events, "WALK_FORWARD_MASKS_FROZEN") is None:
        require_prefix(base)
        return finish_prefix()
    stored_masks = _event_payload(
        artifacts_root,
        events,
        "WALK_FORWARD_MASKS_FROZEN",
        schema="systematic_fx.ai_all_cases_walk_forward_masks.v1",
        config=config,
    )
    resources.check("VERIFY_BEFORE_WALK_FORWARD_MASKS")
    fresh_masks = _validate_walk_masks(
        fresh_service(
            "walk-forward masks",
            lambda: services.freeze_walk_forward_masks(root, config, selected, fresh_search),
        ),
        selected,
    )
    resources.check("VERIFY_AFTER_WALK_FORWARD_MASKS")
    _compare_payload(stored_masks, fresh_masks, label="walk-forward masks")
    mask_event = _find_event(events, "WALK_FORWARD_MASKS_FROZEN")
    if mask_event is None or (
        tuple(mask_event.payload["candidate_ids"]) != selected
        or tuple(mask_event.payload["fold_keys"]) != WALK_FORWARD_FOLD_KEYS
    ):
        raise AllCasesIntegrityError("walk-forward mask event metadata differs")
    with_masks = (*base, "WALK_FORWARD_MASKS_FROZEN")
    if _find_event(events, "WALK_FORWARD_RESULTS_RELEASED") is None:
        require_prefix(with_masks)
        return finish_prefix()
    stored_walk = _event_payload(
        artifacts_root,
        events,
        "WALK_FORWARD_RESULTS_RELEASED",
        schema="systematic_fx.ai_all_cases_walk_forward_results.v1",
        config=config,
    )
    resources.check("VERIFY_BEFORE_WALK_FORWARD_RESULTS")
    fresh_walk, finalists = _validate_walk_result(
        fresh_service(
            "walk-forward result",
            lambda: services.evaluate_walk_forward(
                root, config, selected, fresh_masks, fresh_search
            ),
        ),
        selected,
        expected_decision_dates=walk_decision_dates,
        expected_fold_lengths=walk_fold_lengths,
        expected_candidate_descriptors=candidate_descriptors,
    )
    resources.check("VERIFY_AFTER_WALK_FORWARD_RESULTS")
    _compare_payload(stored_walk, fresh_walk, label="walk-forward result")
    walk_event = _find_event(events, "WALK_FORWARD_RESULTS_RELEASED")
    if walk_event is None or tuple(walk_event.payload["finalist_candidate_ids"]) != finalists:
        raise AllCasesIntegrityError("walk-forward result event metadata differs")
    with_walk = (*with_masks, "WALK_FORWARD_RESULTS_RELEASED")
    if not finalists:
        final_status = "NO_WALK_FORWARD_FINALISTS_HOLDOUT_NOT_OPENED"
        expected = (*with_walk, "HOLDOUT_SKIPPED", "COMPLETED")
        require_prefix(expected)
        validate_skip(
            "HOLDOUT_SKIPPED",
            stage="HOLDOUT",
            reason=final_status,
            final_status=final_status,
        )
        validate_completion(final_status, expected)
        return finish_prefix()

    expected_full = (
        *with_walk,
        "HOLDOUT_AUTHORIZED",
        "HOLDOUT_MASKS_FROZEN",
        "HOLDOUT_RESULTS_RELEASED",
        "COMPLETED",
    )
    require_prefix(expected_full)
    authorization_event = _find_event(events, "HOLDOUT_AUTHORIZED")
    if authorization_event is None:
        return finish_prefix()
    authorization = _authorization_document(config, finalists)
    _verify_artifact(
        artifacts_root,
        _artifact_from_event(authorization_event),
        expected_bytes=_canonical_json_bytes(authorization),
    )
    if (
        tuple(authorization_event.payload["finalist_candidate_ids"]) != finalists
        or authorization_event.payload["family_sha256"] != authorization["family_sha256"]
    ):
        raise AllCasesIntegrityError("holdout authorization event metadata differs")
    if _find_event(events, "HOLDOUT_MASKS_FROZEN") is None:
        return finish_prefix()
    stored_holdout_masks = _event_payload(
        artifacts_root,
        events,
        "HOLDOUT_MASKS_FROZEN",
        schema="systematic_fx.ai_all_cases_holdout_masks.v1",
        config=config,
    )
    resources.check("VERIFY_BEFORE_HOLDOUT_MASKS")
    fresh_holdout_masks = _validate_holdout_masks(
        fresh_service(
            "holdout masks",
            lambda: services.freeze_holdout_masks(root, config, finalists, fresh_walk),
        ),
        finalists,
    )
    resources.check("VERIFY_AFTER_HOLDOUT_MASKS")
    _compare_payload(stored_holdout_masks, fresh_holdout_masks, label="holdout masks")
    holdout_mask_event = _find_event(events, "HOLDOUT_MASKS_FROZEN")
    if (
        holdout_mask_event is None
        or tuple(holdout_mask_event.payload["candidate_ids"]) != finalists
    ):
        raise AllCasesIntegrityError("holdout mask event metadata differs")
    if _find_event(events, "HOLDOUT_RESULTS_RELEASED") is None:
        return finish_prefix()
    stored_holdout = _event_payload(
        artifacts_root,
        events,
        "HOLDOUT_RESULTS_RELEASED",
        schema="systematic_fx.ai_all_cases_holdout_results.v1",
        config=config,
    )
    resources.check("VERIFY_BEFORE_HOLDOUT_RESULTS")
    fresh_holdout, classification = _validate_holdout_result(
        fresh_service(
            "holdout result",
            lambda: services.evaluate_holdout(root, config, finalists, fresh_holdout_masks),
        ),
        finalists,
        expected_decision_dates=holdout_decision_dates,
        expected_candidate_descriptors={
            candidate_id: candidate_descriptors[candidate_id] for candidate_id in finalists
        },
    )
    resources.check("VERIFY_AFTER_HOLDOUT_RESULTS")
    _compare_payload(stored_holdout, fresh_holdout, label="holdout result")
    holdout_event = _find_event(events, "HOLDOUT_RESULTS_RELEASED")
    if holdout_event is None or holdout_event.payload["classification"] != classification:
        raise AllCasesIntegrityError("holdout result event classification differs")
    validate_completion(classification, expected_full)
    return finish_prefix()


def verify_ai_all_cases(project_root: Path | str) -> AllCasesRun:
    """Fresh, read-only recomputation using only fixed production services."""

    root = _project_root(project_root)
    _require_trusted_bootstrap_runtime(root)
    config = load_ai_all_cases_config(root)
    _load_validated_dataset_contract(root)
    verify_failed_predecessor_attempt(root)
    verify_failed_attempt2_predecessor(root)
    verify_failed_attempt3_predecessor(root)
    verify_failed_attempt4_predecessor(root)
    run_root = _fixed_run_root(root, create=False)
    return _verify_with_services(
        root,
        config,
        run_root,
        _default_services(verify_only=True),
    )


__all__ = [
    "AI_ALL_CASES_EVENT_SCHEMA",
    "AI_ALL_CASES_RUN_SCHEMA",
    "DEFAULT_AI_ALL_CASES_ROOT",
    "AllCasesIntegrityError",
    "AllCasesRun",
    "AllCasesRunError",
    "precommit_ai_all_cases",
    "run_ai_all_cases",
    "verify_ai_all_cases",
]
