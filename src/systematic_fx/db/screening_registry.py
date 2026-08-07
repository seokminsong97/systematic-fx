"""Bounded, serializable control-plane registration for Phase 1A screening.

Preparation is deliberately database-free and does not publish files.  The public
registration entry point first revalidates every immutable input, then publishes one
content-addressed control artifact beneath the explicitly supplied data root, and
finally registers or verifies all PostgreSQL control rows in one SERIALIZABLE
transaction.  Split dates are not copied into the control artifact: their exact hash
is recorded while sealed boundaries remain represented only by SEALED database rows.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import stat
import tempfile
from collections.abc import Callable, Mapping, Sequence
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from functools import wraps
from pathlib import Path, PurePosixPath
from typing import Any, Final, ParamSpec, TypeVar

import psycopg
from psycopg import IsolationLevel
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.db.postgres_retry import retry_serialization_failures
from systematic_fx.research.hypotheses import (
    EXPECTED_CAMPAIGN_VARIANT_BUDGET,
    EXPECTED_PARENT_COUNT,
    HypothesisBundle,
    HypothesisConfigError,
    HypothesisSpec,
    canonical_json_bytes,
    canonical_sha256,
    family_counts,
    load_hypothesis_bundle,
    load_toml_document,
)
from systematic_fx.research.provenance import CODE_SNAPSHOT_SCHEMA
from systematic_fx.research.screening_config import (
    ConservativeScreeningBundle,
    ScreeningConfigError,
    load_conservative_screening_bundle,
)
from systematic_fx.validation.splits import (
    CALENDAR_SCHEMA,
    CALENDAR_VERSION,
    CAMPAIGN_ID,
    PHASE1A_EXCLUDED_SOURCE_DATES,
    SPLIT_SCHEMA,
    SPLIT_VERSION,
    Phase1AScreeningCalendar,
    Phase1AScreeningSplit,
)

REGISTRATION_SCHEMA: Final = "systematic_fx.phase1a_screening_registry.v1"
REGISTRATION_VERSION: Final = "phase1a_screening_registry_v1"
CONTROL_ARTIFACT_SUBDIRECTORY: Final = PurePosixPath(
    "derived/manifests/phase1a_screening_registry_v1"
)
DEFAULT_DATASET_KEY: Final = "glbx_mdp3_mbp_10_6e_fut_v1"
EXPECTED_BARRIER_TICKS: Final = tuple(range(24, 193, 8))
EXPECTED_BARRIER_CELL_COUNT: Final = 484

_VISIBLE_SPLIT = "VISIBLE"
_SEALED_SPLIT = "SEALED"
_INTERNAL_PROVENANCE = "INTERNAL"
_SHA256 = re.compile(r"[0-9a-f]{64}")
_GIT_OBJECT_ID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")
_PRE_HOLDOUT_SPLIT_COUNT = 6  # Discovery plus five walk-forward folds.
_STRESS_SCENARIO_COUNT = 3
_FULL_QC_JOB_TYPE = "RECORD_FULL_MBP10_STRUCTURAL_SCAN"
_FULL_QC_SOURCE_CHECK = "FULL_MBP10_STRUCTURAL_SCAN_FILE"
_P = ParamSpec("_P")
_R = TypeVar("_R")


class ScreeningRegistryError(RuntimeError):
    """Phase 1A control state could not be prepared or registered safely."""


class ScreeningRegistryDriftError(ScreeningRegistryError):
    """An immutable file or database identity differs from the requested state."""


class ScreeningRegistryDatabaseError(ScreeningRegistryError):
    """PostgreSQL rejected or could not complete the atomic registration."""


def _translate_psycopg_errors(
    operation: str,
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    def decorate(function: Callable[_P, _R]) -> Callable[_P, _R]:
        @wraps(function)
        def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            try:
                return retry_serialization_failures(function, *args, **kwargs)
            except ScreeningRegistryError:
                raise
            except psycopg.Error as error:
                raise ScreeningRegistryDatabaseError(f"PostgreSQL {operation} failed") from error

        return wrapped

    return decorate


@dataclass(frozen=True, slots=True)
class VerifiedInputArtifact:
    """Exact canonical calendar or split artifact already present under data root."""

    artifact_kind: str
    path: Path
    relative_uri: str
    sha256: str
    byte_size: int
    artifact_schema: str
    visibility: str


@dataclass(frozen=True, slots=True)
class _HeldArtifactFile:
    """One verified inode kept open through the database transaction."""

    path: Path
    descriptor: int
    sha256: str
    byte_size: int
    identity: tuple[int, int, int, int, int, int]


@dataclass(frozen=True, slots=True)
class _RegistrationBaseline:
    """Original campaign/control identities preserved across code-only revisions."""

    campaign_id: int
    code_commit: str
    control_artifact_id: int
    control_artifact_sha256: str
    control_document: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class CampaignSplitSpec:
    """One exact row requested in ``campaign_splits``."""

    split_key: str
    split_role: str
    fold_number: int | None
    start_date: date
    end_date: date
    start_active_ordinal: int
    end_active_ordinal: int
    purge_before_days: int
    purge_after_days: int
    result_visibility: str


@dataclass(frozen=True, slots=True)
class CampaignDaySpec:
    """One exact source-date proxy calendar row before database IDs are resolved."""

    calendar_date: date
    active_day_ordinal: int | None
    eligibility_status: str
    exclusion_reason: str | None
    split_key: str | None
    metadata: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class Phase1AExperimentSpec:
    """Immutable cloned parent experiment content, excluding database foreign keys."""

    experiment_key: str
    hypothesis: HypothesisSpec
    feature_definition_versions: Mapping[str, object]
    search_boundary: Mapping[str, object]
    cost_assumptions: Mapping[str, object]
    execution_assumptions: Mapping[str, object]
    trial_budget: int
    config_sha256: str


@dataclass(frozen=True, slots=True)
class PreparedScreeningRegistration:
    """All validated, deterministic state needed by one atomic registration."""

    project_root: Path
    data_root: Path
    dataset_key: str
    code_commit: str
    code_snapshot_sha256: str
    cost_input_manifest_sha256: str
    screening_bundle: ConservativeScreeningBundle
    hypothesis_bundle: HypothesisBundle
    calendar: Phase1AScreeningCalendar
    split: Phase1AScreeningSplit
    calendar_artifact: VerifiedInputArtifact
    split_artifact: VerifiedInputArtifact
    code_snapshot_artifact: VerifiedInputArtifact
    campaign_document: Mapping[str, object]
    split_specs: tuple[CampaignSplitSpec, ...]
    day_specs: tuple[CampaignDaySpec, ...]
    experiment_specs: tuple[Phase1AExperimentSpec, ...]
    registration_document: Mapping[str, object]
    registration_bytes: bytes
    registration_sha256: str

    @property
    def control_artifact_directory(self) -> Path:
        return self.data_root.joinpath(*CONTROL_ARTIFACT_SUBDIRECTORY.parts)

    @property
    def control_artifact_path(self) -> Path:
        return self.control_artifact_directory / f"{self.registration_sha256}.json"


@dataclass(frozen=True, slots=True)
class _DatabaseRegistration:
    dataset_id: int
    campaign_id: int
    calendar_artifact_id: int
    split_artifact_id: int
    code_snapshot_artifact_id: int
    control_artifact_id: int
    split_ids: tuple[int, ...]
    experiment_ids: tuple[int, ...]
    created_campaign: bool
    created_artifacts: int
    created_splits: int
    created_days: int
    created_experiments: int


@dataclass(frozen=True, slots=True)
class ScreeningRegistrationReport:
    """Database and filesystem identities created or exactly reused."""

    dataset_id: int
    dataset_key: str
    campaign_id: int
    campaign_key: str
    calendar_artifact_id: int
    split_artifact_id: int
    code_snapshot_artifact_id: int
    control_artifact_id: int
    control_artifact_path: Path
    control_artifact_sha256: str
    split_ids: tuple[int, ...]
    experiment_ids: tuple[int, ...]
    created_control_artifact: bool
    created_campaign: bool
    created_artifacts: int
    created_splits: int
    created_days: int
    created_experiments: int


def _nonempty(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ScreeningRegistryError(f"{label} must be a non-empty string")
    if value != value.strip():
        raise ScreeningRegistryError(f"{label} must not have surrounding whitespace")
    return value


def _sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ScreeningRegistryError(f"{label} must be a lowercase 64-character SHA-256")
    return value


def _git_object_id(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _GIT_OBJECT_ID.fullmatch(value) is None:
        raise ScreeningRegistryError(f"{label} must be a full lowercase Git object ID")
    return value


def _table(document: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = document.get(key)
    if not isinstance(value, dict):
        raise ScreeningRegistryError(f"{key} must be a TOML table")
    return value


def _resolved_directory(value: Path | str, *, label: str) -> Path:
    requested = Path(value).expanduser()
    try:
        resolved = requested.resolve(strict=True)
    except FileNotFoundError as error:
        raise ScreeningRegistryError(f"{label} does not exist: {requested}") from error
    if not resolved.is_dir():
        raise ScreeningRegistryError(f"{label} must be a directory: {resolved}")
    return resolved


def _assert_no_symlink_components(root: Path, path: Path, *, label: str) -> None:
    relative = path.relative_to(root)
    current = root
    for component in relative.parts:
        current /= component
        if current.is_symlink():
            raise ScreeningRegistryDriftError(f"{label} cannot use a symbolic link: {current}")


def _read_exact_regular_file(path: Path, *, label: str) -> bytes:
    if path.is_symlink():
        raise ScreeningRegistryDriftError(f"{label} cannot be a symbolic link: {path}")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except (FileNotFoundError, OSError) as error:
        raise ScreeningRegistryError(f"{label} is not readable: {path}") from error
    try:
        before = os.fstat(descriptor)
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            content = handle.read()
        after = os.fstat(descriptor)
        current = path.lstat()
    finally:
        os.close(descriptor)
    before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    path_identity = (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns)
    if (
        not stat.S_ISREG(before.st_mode)
        or before_identity != after_identity
        or after_identity != path_identity
    ):
        raise ScreeningRegistryDriftError(f"{label} changed while it was read: {path}")
    return content


def _file_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _open_verified_artifact_file(
    path: Path,
    *,
    expected_sha256: str,
    expected_byte_size: int,
    label: str,
) -> _HeldArtifactFile:
    """Hash a non-symlink file once and retain its descriptor for commit binding."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ScreeningRegistryError(f"{label} is not readable: {path}") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ScreeningRegistryError(f"{label} is not a regular file: {path}")
        identity = _file_identity(before)
        digest = hashlib.sha256()
        byte_size = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
            byte_size += len(chunk)
        if _file_identity(os.fstat(descriptor)) != identity or byte_size != before.st_size:
            raise ScreeningRegistryDriftError(f"{label} changed while it was hashed: {path}")
        observed_sha256 = digest.hexdigest()
        if observed_sha256 != expected_sha256 or byte_size != expected_byte_size:
            raise ScreeningRegistryDriftError(f"{label} bytes differ from the prepared identity")
        held = _HeldArtifactFile(
            path=path,
            descriptor=descriptor,
            sha256=observed_sha256,
            byte_size=byte_size,
            identity=identity,
        )
        _verify_held_artifact_binding(held, label=label)
        return held
    except Exception:
        os.close(descriptor)
        raise


def _verify_held_artifact_binding(
    held: _HeldArtifactFile,
    *,
    label: str,
) -> None:
    """Verify both the held inode and its durable URI path still identify one file."""

    try:
        descriptor_identity = _file_identity(os.fstat(held.descriptor))
        path_identity = _file_identity(held.path.lstat())
    except OSError as exc:
        raise ScreeningRegistryDriftError(
            f"{label} disappeared before database commit: {held.path}"
        ) from exc
    if (
        descriptor_identity != held.identity
        or path_identity != held.identity
        or not stat.S_ISREG(path_identity[2])
    ):
        raise ScreeningRegistryDriftError(
            f"{label} path or inode changed before database commit: {held.path}"
        )


def _read_held_artifact_bytes(
    held: _HeldArtifactFile,
    *,
    label: str,
) -> bytes:
    """Rehash/read one already-held inode without reopening its durable path."""

    _verify_held_artifact_binding(held, label=label)
    try:
        os.lseek(held.descriptor, 0, os.SEEK_SET)
        digest = hashlib.sha256()
        chunks: list[bytes] = []
        byte_size = 0
        while chunk := os.read(held.descriptor, 1024 * 1024):
            digest.update(chunk)
            chunks.append(chunk)
            byte_size += len(chunk)
    except OSError as exc:
        raise ScreeningRegistryError(f"{label} could not be read from its held inode") from exc
    _verify_held_artifact_binding(held, label=label)
    if byte_size != held.byte_size or digest.hexdigest() != held.sha256:
        raise ScreeningRegistryDriftError(f"{label} held bytes changed while they were read")
    return b"".join(chunks)


def _registration_invariant_projection(
    value: object,
    *,
    label: str,
) -> dict[str, object]:
    """Replace only code-revision fields while preserving every research invariant."""

    if not isinstance(value, Mapping):
        raise ScreeningRegistryDriftError(f"{label} must be a JSON object")
    try:
        detached = json.loads(canonical_json_bytes(value))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ScreeningRegistryDriftError(f"{label} is not strict canonical JSON") from exc
    if (
        not isinstance(detached, dict)
        or detached.get("artifact_schema") != REGISTRATION_SCHEMA
        or detached.get("artifact_version") != REGISTRATION_VERSION
    ):
        raise ScreeningRegistryDriftError(f"{label} registration schema drift")
    code = detached.get("code")
    provenance = detached.get("provenance_inputs")
    if not isinstance(code, dict) or set(code) != {"artifact", "commit", "snapshot_sha256"}:
        raise ScreeningRegistryDriftError(f"{label} code identity schema drift")
    code_artifact = code.get("artifact")
    if not isinstance(code_artifact, dict) or set(code_artifact) != {
        "artifact_schema",
        "byte_size",
        "relative_uri",
        "sha256",
    }:
        raise ScreeningRegistryDriftError(f"{label} code artifact schema drift")
    if not isinstance(provenance, dict) or "code_snapshot_sha256" not in provenance:
        raise ScreeningRegistryDriftError(f"{label} code provenance schema drift")
    snapshot_sha256 = _sha256(code.get("snapshot_sha256"), label=f"{label} snapshot SHA-256")
    artifact_sha256 = _sha256(
        code_artifact.get("sha256"),
        label=f"{label} code artifact SHA-256",
    )
    provenance_sha256 = _sha256(
        provenance.get("code_snapshot_sha256"),
        label=f"{label} provenance code SHA-256",
    )
    _git_object_id(code.get("commit"), label=f"{label} code commit")
    byte_size = code_artifact.get("byte_size")
    relative_uri = code_artifact.get("relative_uri")
    if (
        code_artifact.get("artifact_schema") != CODE_SNAPSHOT_SCHEMA
        or snapshot_sha256 != artifact_sha256
        or snapshot_sha256 != provenance_sha256
        or isinstance(byte_size, bool)
        or not isinstance(byte_size, int)
        or byte_size <= 0
        or not isinstance(relative_uri, str)
        or not relative_uri
    ):
        raise ScreeningRegistryDriftError(f"{label} code snapshot lineage drift")
    code["commit"] = "<CODE_COMMIT>"
    code["snapshot_sha256"] = "<CODE_SNAPSHOT_SHA256>"
    code_artifact["byte_size"] = "<CODE_ARTIFACT_BYTE_SIZE>"
    code_artifact["relative_uri"] = "<CODE_ARTIFACT_RELATIVE_URI>"
    code_artifact["sha256"] = "<CODE_SNAPSHOT_SHA256>"
    provenance["code_snapshot_sha256"] = "<CODE_SNAPSHOT_SHA256>"
    return detached


def _assert_registration_revision_invariants(
    baseline_document: Mapping[str, object],
    current_document: Mapping[str, object],
) -> None:
    baseline = _registration_invariant_projection(
        baseline_document,
        label="baseline Phase 1A control artifact",
    )
    current = _registration_invariant_projection(
        current_document,
        label="current Phase 1A control artifact",
    )
    if baseline != current:
        raise ScreeningRegistryDriftError(
            "Phase 1A control revision changes non-code research invariants"
        )


def _held_artifact_for_path(
    held_artifacts: Mapping[Path, _HeldArtifactFile],
    path: Path,
) -> _HeldArtifactFile:
    try:
        held = held_artifacts[path]
    except KeyError as exc:
        raise ScreeningRegistryError(f"artifact descriptor is missing for {path}") from exc
    _verify_held_artifact_binding(held, label="registration artifact")
    return held


def _verify_input_artifact(
    path: Path | str,
    *,
    data_root: Path,
    expected_bytes: bytes,
    expected_sha256: str,
    artifact_kind: str,
    artifact_schema: str,
    visibility: str,
) -> VerifiedInputArtifact:
    requested = Path(path).expanduser()
    if requested.is_symlink():
        raise ScreeningRegistryDriftError(
            f"{artifact_kind} artifact cannot be a symbolic link: {requested}"
        )
    try:
        resolved = requested.resolve(strict=True)
    except FileNotFoundError as error:
        raise ScreeningRegistryError(
            f"{artifact_kind} artifact does not exist: {requested}"
        ) from error
    if not resolved.is_relative_to(data_root):
        raise ScreeningRegistryError(
            f"{artifact_kind} artifact must be contained by data_root {data_root}"
        )
    _assert_no_symlink_components(data_root, resolved, label=f"{artifact_kind} artifact")
    relative = resolved.relative_to(data_root)
    if relative.parts[:2] != ("derived", "manifests"):
        raise ScreeningRegistryError(
            f"{artifact_kind} artifact must be under data_root/derived/manifests"
        )
    content = _read_exact_regular_file(resolved, label=f"{artifact_kind} artifact")
    actual_sha256 = hashlib.sha256(content).hexdigest()
    if content != expected_bytes or actual_sha256 != expected_sha256:
        raise ScreeningRegistryDriftError(
            f"{artifact_kind} artifact bytes differ from its canonical object"
        )
    return VerifiedInputArtifact(
        artifact_kind=artifact_kind,
        path=resolved,
        relative_uri=relative.as_posix(),
        sha256=actual_sha256,
        byte_size=len(content),
        artifact_schema=artifact_schema,
        visibility=visibility,
    )


def _verify_code_snapshot_artifact(
    path: Path | str,
    *,
    data_root: Path,
    expected_sha256: str,
    expected_code_commit: str,
) -> VerifiedInputArtifact:
    requested = Path(path).expanduser()
    if requested.is_symlink():
        raise ScreeningRegistryDriftError("code snapshot artifact cannot be a symbolic link")
    try:
        resolved = requested.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ScreeningRegistryError("code snapshot artifact does not exist") from exc
    if not resolved.is_relative_to(data_root):
        raise ScreeningRegistryError("code snapshot artifact must be contained by data_root")
    _assert_no_symlink_components(data_root, resolved, label="code snapshot artifact")
    relative = resolved.relative_to(data_root)
    if relative.parts[:3] != ("derived", "manifests", "code_snapshot_v2"):
        raise ScreeningRegistryError(
            "code snapshot artifact must use data/derived/manifests/code_snapshot_v2"
        )
    content = _read_exact_regular_file(resolved, label="code snapshot artifact")
    observed_sha256 = hashlib.sha256(content).hexdigest()
    if observed_sha256 != expected_sha256 or resolved.name != f"sha256={expected_sha256}.json":
        raise ScreeningRegistryDriftError("code snapshot artifact SHA/path identity drift")
    try:
        document = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ScreeningRegistryDriftError("code snapshot artifact is not valid JSON") from exc
    if (
        not isinstance(document, dict)
        or document.get("artifact_schema") != CODE_SNAPSHOT_SCHEMA
        or document.get("code_commit") != expected_code_commit
        or not isinstance(document.get("file_count"), int)
        or isinstance(document.get("file_count"), bool)
        or not isinstance(document.get("files"), list)
        or document["file_count"] <= 0
        or document["file_count"] != len(document["files"])
        or canonical_json_bytes(document) != content
    ):
        raise ScreeningRegistryDriftError("code snapshot artifact root schema drift")
    observed_paths: list[str] = []
    for index, item in enumerate(document["files"]):
        if not isinstance(item, dict) or set(item) != {
            "byte_size",
            "content_base64",
            "content_encoding",
            "executable",
            "relative_path",
            "sha256",
        }:
            raise ScreeningRegistryDriftError(f"code snapshot file {index} schema drift")
        relative_path = item.get("relative_path")
        byte_size = item.get("byte_size")
        sha256 = item.get("sha256")
        if (
            not isinstance(relative_path, str)
            or not relative_path
            or PurePosixPath(relative_path).is_absolute()
            or ".." in PurePosixPath(relative_path).parts
            or isinstance(byte_size, bool)
            or not isinstance(byte_size, int)
            or byte_size < 0
            or not isinstance(sha256, str)
            or _SHA256.fullmatch(sha256) is None
            or item.get("content_encoding") != "base64"
            or not isinstance(item.get("content_base64"), str)
            or not isinstance(item.get("executable"), bool)
        ):
            raise ScreeningRegistryDriftError(f"code snapshot file {index} identity drift")
        try:
            restored = base64.b64decode(item["content_base64"], validate=True)
        except (ValueError, TypeError) as exc:
            raise ScreeningRegistryDriftError(
                f"code snapshot file {index} has invalid base64"
            ) from exc
        if len(restored) != byte_size or hashlib.sha256(restored).hexdigest() != sha256:
            raise ScreeningRegistryDriftError(
                f"code snapshot file {index} bytes differ from its identity"
            )
        observed_paths.append(relative_path)
    if observed_paths != sorted(set(observed_paths)):
        raise ScreeningRegistryDriftError("code snapshot paths are not unique and ordered")
    return VerifiedInputArtifact(
        artifact_kind="code_snapshot",
        path=resolved,
        relative_uri=relative.as_posix(),
        sha256=observed_sha256,
        byte_size=len(content),
        artifact_schema=CODE_SNAPSHOT_SCHEMA,
        visibility=_INTERNAL_PROVENANCE,
    )


def _validate_calendar_and_split(
    bundle: ConservativeScreeningBundle,
    calendar: Phase1AScreeningCalendar,
    split: Phase1AScreeningSplit,
) -> None:
    if not isinstance(calendar, Phase1AScreeningCalendar):
        raise ScreeningRegistryError("calendar must be a Phase1AScreeningCalendar")
    if not isinstance(split, Phase1AScreeningSplit):
        raise ScreeningRegistryError("split must be a Phase1AScreeningSplit")
    if bundle.calendar_version != CALENDAR_VERSION or bundle.split_version != SPLIT_VERSION:
        raise ScreeningRegistryDriftError("screening bundle calendar/split version drift")
    if split.calendar_sha256 != calendar.sha256:
        raise ScreeningRegistryDriftError("split is bound to a different calendar SHA-256")
    if split.eligible_source_date_count != len(calendar.source_dates):
        raise ScreeningRegistryDriftError("split eligible-date count differs from calendar")
    combined = (
        split.discovery
        + tuple(day for fold in split.walk_forward_folds for day in fold)
        + split.embargo
        + split.sealed_holdout
        + split.outcome_tail
    )
    if combined != calendar.source_dates:
        raise ScreeningRegistryDriftError("split does not exactly cover the calendar in order")
    excluded_text = tuple(day.isoformat() for day in calendar.excluded_source_dates)
    if excluded_text != bundle.excluded_dates:
        raise ScreeningRegistryDriftError("calendar exclusions differ from screening policy")
    all_dates = tuple(sorted((*calendar.source_dates, *calendar.excluded_source_dates)))
    if len(all_dates) != calendar.source_record_count:
        raise ScreeningRegistryDriftError("calendar source identity count is inconsistent")
    if (
        all_dates[0].isoformat() != bundle.source_start
        or all_dates[-1].isoformat() != bundle.source_end
    ):
        raise ScreeningRegistryDriftError("calendar boundaries differ from screening policy")
    if tuple(bundle.barrier_ticks) != EXPECTED_BARRIER_TICKS:
        raise ScreeningRegistryDriftError("screening bundle barrier axes drifted")


def _split_specs(split: Phase1AScreeningSplit) -> tuple[CampaignSplitSpec, ...]:
    definitions: list[tuple[str, str, int | None, tuple[date, ...], str]] = [
        ("DISCOVERY", "DISCOVERY", None, split.discovery, _VISIBLE_SPLIT),
    ]
    definitions.extend(
        (
            f"WALK_FORWARD_{index}",
            "WALK_FORWARD",
            index,
            fold,
            _SEALED_SPLIT,
        )
        for index, fold in enumerate(split.walk_forward_folds, start=1)
    )
    definitions.extend(
        (
            ("EMBARGO", "EMBARGO", None, split.embargo, _SEALED_SPLIT),
            ("HOLDOUT", "HOLDOUT", None, split.sealed_holdout, _SEALED_SPLIT),
            ("OUTCOME_TAIL", "OUTCOME_TAIL", None, split.outcome_tail, _SEALED_SPLIT),
        )
    )

    specs: list[CampaignSplitSpec] = []
    ordinal = 1
    for suffix, role, fold_number, days, visibility in definitions:
        specs.append(
            CampaignSplitSpec(
                split_key=f"{CAMPAIGN_ID}:{suffix.lower()}",
                split_role=role,
                fold_number=fold_number,
                start_date=days[0],
                end_date=days[-1],
                start_active_ordinal=ordinal,
                end_active_ordinal=ordinal + len(days) - 1,
                purge_before_days=0,
                purge_after_days=0,
                result_visibility=visibility,
            )
        )
        ordinal += len(days)
    if ordinal - 1 != split.eligible_source_date_count:
        raise ScreeningRegistryDriftError("split ordinals do not cover the calendar")
    return tuple(specs)


def _day_specs(
    calendar: Phase1AScreeningCalendar,
    split_specs: Sequence[CampaignSplitSpec],
) -> tuple[CampaignDaySpec, ...]:
    split_for_date: dict[date, CampaignSplitSpec] = {}
    for spec in split_specs:
        for ordinal in range(spec.start_active_ordinal, spec.end_active_ordinal + 1):
            source_date = calendar.source_dates[ordinal - 1]
            if source_date in split_for_date:
                raise ScreeningRegistryDriftError("eligible date is assigned to duplicate splits")
            split_for_date[source_date] = spec
    if set(split_for_date) != set(calendar.source_dates):
        raise ScreeningRegistryDriftError("not every eligible calendar date has one split")

    eligible_ordinals = {day: index for index, day in enumerate(calendar.source_dates, start=1)}
    all_dates = sorted((*calendar.source_dates, *calendar.excluded_source_dates))
    specs: list[CampaignDaySpec] = []
    for source_date in all_dates:
        ordinal = eligible_ordinals.get(source_date)
        if ordinal is None:
            metadata: dict[str, object] = {
                "calendar_sha256": calendar.sha256,
                "calendar_version": CALENDAR_VERSION,
                "exclusion_scope": "ENTIRE_SOURCE_DATE_ALL_CONTRACTS_ALL_SESSIONS",
                "raw_qc_reclassification_allowed": False,
                "raw_qc_status": "FAIL",
                "source_date_proxy": True,
                "split_sha256": None,
                "split_version": SPLIT_VERSION,
            }
            specs.append(
                CampaignDaySpec(
                    calendar_date=source_date,
                    active_day_ordinal=None,
                    eligibility_status="INELIGIBLE",
                    exclusion_reason="FROZEN_RAW_STRUCTURAL_QC_FAIL_SOURCE_DATE",
                    split_key=None,
                    metadata=metadata,
                )
            )
            continue
        split_spec = split_for_date[source_date]
        specs.append(
            CampaignDaySpec(
                calendar_date=source_date,
                active_day_ordinal=ordinal,
                eligibility_status="ELIGIBLE",
                exclusion_reason=None,
                split_key=split_spec.split_key,
                metadata={
                    "calendar_sha256": calendar.sha256,
                    "calendar_version": CALENDAR_VERSION,
                    "raw_qc_status": "PASS",
                    "result_visibility": split_spec.result_visibility,
                    "source_date_proxy": True,
                    "split_sha256": None,  # Filled after exact split validation below.
                    "split_version": SPLIT_VERSION,
                },
            )
        )
    return tuple(specs)


def _experiment_trial_budget(bundle: HypothesisBundle) -> int:
    variants = bundle.strategy_variants_per_parent
    barrier_trials = (
        variants * _PRE_HOLDOUT_SPLIT_COUNT * _STRESS_SCENARIO_COUNT * EXPECTED_BARRIER_CELL_COUNT
    )
    screen_trials = int(bundle.local_trial_budget_breakdown.get("screen_or_model_fit", 0))
    return variants + barrier_trials + screen_trials


def _experiment_specs(
    *,
    screening: ConservativeScreeningBundle,
    hypotheses: HypothesisBundle,
    calendar: Phase1AScreeningCalendar,
    split: Phase1AScreeningSplit,
    cost_document: Mapping[str, object],
    execution_document: Mapping[str, object],
    cost_input_manifest_sha256: str,
) -> tuple[Phase1AExperimentSpec, ...]:
    feature_versions: dict[str, object] = {
        "eligible_calendar": screening.calendar_version,
        "features_1s": screening.feature_version,
        "outcomes": screening.outcome_version,
        "research_5m": screening.feature_version,
        "split": screening.split_version,
    }
    cost_assumptions: dict[str, object] = {
        "allocated_fixed_cost_ticks": screening.allocated_fixed_cost_ticks,
        "baseline_cost_floor_ticks": screening.baseline_cost_floor_ticks,
        "config": dict(cost_document),
        "config_sha256": screening.cost.sha256,
        "input_manifest_sha256": cost_input_manifest_sha256,
        "variable_cost_ticks": screening.variable_cost_ticks,
        "version": screening.cost_version,
    }
    execution_assumptions: dict[str, object] = {
        "config": dict(execution_document),
        "config_sha256": screening.execution.sha256,
        "routing_delay_ms": screening.routing_delay_ms,
        "same_timestamp_tie_break": "STOP_FIRST",
        "stop_adverse_ticks": screening.stop_adverse_ticks,
        "take_profit_trade_through_ticks": screening.take_profit_trade_through_ticks,
        "version": screening.execution_version,
    }
    trial_budget = _experiment_trial_budget(hypotheses)
    specs: list[Phase1AExperimentSpec] = []
    for hypothesis in hypotheses.hypotheses:
        search_boundary: dict[str, object] = {
            "authority_ceiling": "SCREENING_SURVIVOR",
            "barrier_cell_count": EXPECTED_BARRIER_CELL_COUNT,
            "barrier_grid_sha256": screening.barrier_grid.sha256,
            "barrier_grid_version": screening.barrier_grid_version,
            "calendar_sha256": calendar.sha256,
            "candidate_variant_is_separate_dimension": True,
            "cartesian_product_required": True,
            "direction": hypothesis.direction,
            "direction_is_separate_trial_dimension": True,
            "economic_rationale": hypothesis.economic_rationale,
            "entry_condition": hypothesis.entry_condition,
            "features": list(hypothesis.features),
            "hypothesis_id": hypothesis.hypothesis_id,
            "interaction_family": hypothesis.interaction_family,
            "lookback_bars": list(hypotheses.lookback_bars),
            "observation_active_sessions": hypotheses.observation_active_sessions,
            "source_hypothesis_feature_versions": dict(hypotheses.feature_definition_versions),
            "portfolio_occupancy_scope": "UPSTREAM_NOT_BARRIER_ENGINE",
            "preselection_pruning_allowed": False,
            "sealed_holdout_access_allowed": False,
            "signal_cadence_seconds": hypotheses.signal_cadence_seconds,
            "split_sha256": split.sha256,
            "stop_loss_ticks": list(EXPECTED_BARRIER_TICKS),
            "take_profit_ticks": list(EXPECTED_BARRIER_TICKS),
            "terminal_states": ["TP_FIRST", "STOP_FIRST", "TERMINAL_EXIT", "CENSORED"],
            "trial_budget_semantics": {
                "barrier_cells_per_surface": EXPECTED_BARRIER_CELL_COUNT,
                "campaign_counts_only": "STRATEGY_VARIANT",
                "campaign_strategy_variant_limit": EXPECTED_CAMPAIGN_VARIANT_BUDGET,
                "experiment_counts": "ALL_EXPERIMENT_TRIAL_ROWS",
                "pre_holdout_splits": _PRE_HOLDOUT_SPLIT_COUNT,
                "stress_scenarios": _STRESS_SCENARIO_COUNT,
                "strategy_variants": hypotheses.strategy_variants_per_parent,
                "total": trial_budget,
            },
        }
        identity_payload = {
            "cost_assumptions": cost_assumptions,
            "execution_assumptions": execution_assumptions,
            "feature_definition_versions": feature_versions,
            "hypothesis": hypothesis.registration_payload(),
            "search_boundary": search_boundary,
            "trial_budget": trial_budget,
        }
        specs.append(
            Phase1AExperimentSpec(
                experiment_key=(f"{CAMPAIGN_ID}:experiment:{hypothesis.hypothesis_id}:v1"),
                hypothesis=hypothesis,
                feature_definition_versions=dict(feature_versions),
                search_boundary=search_boundary,
                cost_assumptions=dict(cost_assumptions),
                execution_assumptions=dict(execution_assumptions),
                trial_budget=trial_budget,
                config_sha256=canonical_sha256(identity_payload),
            )
        )
    return tuple(specs)


def prepare_phase1a_screening_registration(
    *,
    project_root: Path | str,
    data_root: Path | str,
    calendar: Phase1AScreeningCalendar,
    split: Phase1AScreeningSplit,
    calendar_artifact_path: Path | str,
    split_artifact_path: Path | str,
    code_snapshot_artifact_path: Path | str,
    code_commit: str,
    code_snapshot_sha256: str,
    cost_input_manifest_sha256: str,
    dataset_key: str = DEFAULT_DATASET_KEY,
) -> PreparedScreeningRegistration:
    """Validate every bounded input and prepare exact registration bytes only."""

    root = _resolved_directory(project_root, label="project_root")
    resolved_data_root = _resolved_directory(data_root, label="data_root")
    source_root = resolved_data_root / "mbp-10"
    if not source_root.is_dir():
        raise ScreeningRegistryError(f"MBP-10 source root does not exist: {source_root}")
    dataset_key = _nonempty(dataset_key, label="dataset_key")
    code_commit = _git_object_id(code_commit, label="code_commit")
    code_snapshot_sha256 = _sha256(code_snapshot_sha256, label="code_snapshot_sha256")
    cost_input_manifest_sha256 = _sha256(
        cost_input_manifest_sha256,
        label="cost_input_manifest_sha256",
    )

    try:
        screening = load_conservative_screening_bundle(root)
        hypotheses = load_hypothesis_bundle(
            root / "configs/research/phase1_parent_hypotheses_v1.toml"
        )
    except (ScreeningConfigError, HypothesisConfigError, FileNotFoundError) as error:
        raise ScreeningRegistryError(str(error)) from error
    _validate_calendar_and_split(screening, calendar, split)
    if len(hypotheses.hypotheses) != EXPECTED_PARENT_COUNT:
        raise ScreeningRegistryDriftError("parent hypothesis bundle must contain exactly 60 rows")

    calendar_artifact = _verify_input_artifact(
        calendar_artifact_path,
        data_root=resolved_data_root,
        expected_bytes=calendar.canonical_json(),
        expected_sha256=calendar.sha256,
        artifact_kind="calendar",
        artifact_schema=CALENDAR_SCHEMA,
        visibility=_VISIBLE_SPLIT,
    )
    split_artifact = _verify_input_artifact(
        split_artifact_path,
        data_root=resolved_data_root,
        expected_bytes=split.canonical_json(),
        expected_sha256=split.sha256,
        artifact_kind="split",
        artifact_schema=SPLIT_SCHEMA,
        visibility=_SEALED_SPLIT,
    )
    code_snapshot_artifact = _verify_code_snapshot_artifact(
        code_snapshot_artifact_path,
        data_root=resolved_data_root,
        expected_sha256=code_snapshot_sha256,
        expected_code_commit=code_commit,
    )

    try:
        campaign_config = load_toml_document(screening.campaign.path)
        cost_config = load_toml_document(screening.cost.path)
        execution_config = load_toml_document(screening.execution.path)
        grid_config = load_toml_document(screening.barrier_grid.path)
    except (HypothesisConfigError, FileNotFoundError) as error:
        raise ScreeningRegistryError(str(error)) from error
    reloaded_config_hashes = {
        "barrier_grid": canonical_sha256(grid_config),
        "campaign": canonical_sha256(campaign_config),
        "cost": canonical_sha256(cost_config),
        "execution": canonical_sha256(execution_config),
    }
    if reloaded_config_hashes != screening.config_hashes:
        raise ScreeningRegistryDriftError(
            "screening configuration changed during registration preparation"
        )
    source_start = date.fromisoformat(screening.source_start)
    source_end = date.fromisoformat(screening.source_end)
    split_specs = _split_specs(split)
    raw_day_specs = _day_specs(calendar, split_specs)
    day_specs = tuple(
        CampaignDaySpec(
            calendar_date=spec.calendar_date,
            active_day_ordinal=spec.active_day_ordinal,
            eligibility_status=spec.eligibility_status,
            exclusion_reason=spec.exclusion_reason,
            split_key=spec.split_key,
            metadata={
                **dict(spec.metadata),
                "split_sha256": split.sha256 if spec.active_day_ordinal is not None else None,
            },
        )
        for spec in raw_day_specs
    )
    experiment_specs = _experiment_specs(
        screening=screening,
        hypotheses=hypotheses,
        calendar=calendar,
        split=split,
        cost_document=cost_config,
        execution_document=execution_config,
        cost_input_manifest_sha256=cost_input_manifest_sha256,
    )

    visibility = {
        "discovery": _VISIBLE_SPLIT,
        "walk_forward_until_all_folds_complete": _SEALED_SPLIT,
        "embargo": _SEALED_SPLIT,
        "holdout": _SEALED_SPLIT,
        "outcome_tail": _SEALED_SPLIT,
    }
    split_counts = {
        "discovery": len(split.discovery),
        "walk_forward": [len(fold) for fold in split.walk_forward_folds],
        "embargo": len(split.embargo),
        "holdout": len(split.sealed_holdout),
        "outcome_tail": len(split.outcome_tail),
    }
    campaign_document: dict[str, object] = {
        "campaign_key": CAMPAIGN_ID,
        "name": "Phase 1A conservative MBP-10 screening",
        "status": "DRAFT",
        "selected_start_date": source_start,
        "selected_end_date": source_end,
        "roll_cutoff_date": None,
        "data_manifest_sha256": calendar.source_manifest_sha256,
        "feature_version": screening.feature_version,
        "outcome_version": screening.outcome_version,
        "cost_model_version": screening.cost_version,
        "execution_model_version": screening.execution_version,
        "trial_budget": hypotheses.campaign_strategy_variant_budget,
        "finalist_budget": 10,
        "split_policy": {
            "authority_ceiling": "SCREENING_SURVIVOR",
            "calendar_sha256": calendar.sha256,
            "calendar_version": CALENDAR_VERSION,
            "definition_status_available": False,
            "pass_backtest_allowed": False,
            "screening_only": True,
            "sealed_boundaries_revealed": False,
            "split_sha256": split.sha256,
            "split_version": SPLIT_VERSION,
            "visibility": visibility,
        },
    }

    registration_document: dict[str, object] = {
        "artifact_schema": REGISTRATION_SCHEMA,
        "artifact_version": REGISTRATION_VERSION,
        "authority": {
            "campaign_status": "DRAFT",
            "maximum_positive_label": "SCREENING_SURVIVOR",
            "pass_backtest_allowed": False,
            "paper_allowed": False,
            "sealed_boundaries_embedded_in_control_artifact": False,
        },
        "barrier_surface": {
            "axis_order": ["take_profit_ticks", "stop_loss_ticks"],
            "cartesian_product_required": True,
            "cell_count": EXPECTED_BARRIER_CELL_COUNT,
            "cell_id_format": "tp{take_profit_ticks}_sl{stop_loss_ticks}",
            "preselection_pruning_allowed": False,
            "stop_loss_ticks": list(EXPECTED_BARRIER_TICKS),
            "take_profit_ticks": list(EXPECTED_BARRIER_TICKS),
        },
        "calendar_identity": {
            "artifact": {
                "byte_size": calendar_artifact.byte_size,
                "relative_uri": calendar_artifact.relative_uri,
                "sha256": calendar_artifact.sha256,
            },
            "artifact_schema": CALENDAR_SCHEMA,
            "calendar_sha256": calendar.sha256,
            "calendar_version": CALENDAR_VERSION,
            "eligible_source_date_count": len(calendar.source_dates),
            "excluded_source_dates": [day.isoformat() for day in PHASE1A_EXCLUDED_SOURCE_DATES],
            "first_source_date": source_start.isoformat(),
            "last_source_date": source_end.isoformat(),
            "raw_qc_fail_count": len(PHASE1A_EXCLUDED_SOURCE_DATES),
        },
        "campaign": {
            "campaign_key": CAMPAIGN_ID,
            "dataset_key": dataset_key,
            "feature_version": screening.feature_version,
            "outcome_version": screening.outcome_version,
            "trial_budget": hypotheses.campaign_strategy_variant_budget,
        },
        "code": {
            "artifact": {
                "artifact_schema": code_snapshot_artifact.artifact_schema,
                "byte_size": code_snapshot_artifact.byte_size,
                "relative_uri": code_snapshot_artifact.relative_uri,
                "sha256": code_snapshot_artifact.sha256,
            },
            "commit": code_commit,
            "snapshot_sha256": code_snapshot_sha256,
        },
        "config_inputs": {
            "barrier_grid": screening.barrier_grid.sha256,
            "bundle": screening.bundle_sha256,
            "campaign": screening.campaign.sha256,
            "cost": screening.cost.sha256,
            "execution": screening.execution.sha256,
            "parent_hypotheses": hypotheses.config_sha256,
        },
        "cost_assumptions": {
            "allocated_fixed_cost_ticks": screening.allocated_fixed_cost_ticks,
            "baseline_cost_floor_ticks": screening.baseline_cost_floor_ticks,
            "config": cost_config,
            "config_sha256": screening.cost.sha256,
            "input_manifest_sha256": cost_input_manifest_sha256,
            "variable_cost_ticks": screening.variable_cost_ticks,
        },
        "execution_assumptions": {
            "config": execution_config,
            "config_sha256": screening.execution.sha256,
            "routing_delay_ms": screening.routing_delay_ms,
            "same_timestamp_tie_break": "STOP_FIRST",
            "stop_adverse_ticks": screening.stop_adverse_ticks,
            "take_profit_trade_through_ticks": screening.take_profit_trade_through_ticks,
        },
        "frozen_configs": {
            "barrier_grid": grid_config,
            "campaign": campaign_config,
        },
        "parent_hypotheses": {
            "bundle_sha256": hypotheses.config_sha256,
            "family_counts": family_counts(hypotheses.hypotheses),
            "parent_count": len(hypotheses.hypotheses),
            "payload": hypotheses.registration_payload(),
        },
        "provenance_inputs": {
            "code_snapshot_sha256": code_snapshot_sha256,
            "cost_input_manifest_sha256": cost_input_manifest_sha256,
            "full_qc_config_sha256": calendar.qc_config_sha256,
            "full_qc_manifest_sha256": calendar.qc_manifest_sha256,
            "raw_schema_fingerprint": calendar.schema_fingerprint,
            "source_manifest_sha256": calendar.source_manifest_sha256,
        },
        "registration_policy": {
            "all_calendar_days_registered": True,
            "all_excluded_raw_qc_fail_days_ineligible": True,
            "all_parents_cloned": EXPECTED_PARENT_COUNT,
            "all_splits_registered": len(split_specs),
            "immutable_mismatch_behavior": "REJECT",
            "serializable_transaction_required": True,
        },
        "split_identity": {
            "artifact": {
                "byte_size": split_artifact.byte_size,
                "relative_uri": split_artifact.relative_uri,
                "sha256": split_artifact.sha256,
                "visibility": _SEALED_SPLIT,
            },
            "artifact_schema": SPLIT_SCHEMA,
            "partition_counts": split_counts,
            "sealed_boundaries_embedded": False,
            "split_sha256": split.sha256,
            "split_version": SPLIT_VERSION,
            "visibility": visibility,
        },
    }
    registration_bytes = canonical_json_bytes(registration_document) + b"\n"
    registration_sha256 = hashlib.sha256(registration_bytes).hexdigest()
    return PreparedScreeningRegistration(
        project_root=root,
        data_root=resolved_data_root,
        dataset_key=dataset_key,
        code_commit=code_commit,
        code_snapshot_sha256=code_snapshot_sha256,
        cost_input_manifest_sha256=cost_input_manifest_sha256,
        screening_bundle=screening,
        hypothesis_bundle=hypotheses,
        calendar=calendar,
        split=split,
        calendar_artifact=calendar_artifact,
        split_artifact=split_artifact,
        code_snapshot_artifact=code_snapshot_artifact,
        campaign_document=campaign_document,
        split_specs=split_specs,
        day_specs=day_specs,
        experiment_specs=experiment_specs,
        registration_document=registration_document,
        registration_bytes=registration_bytes,
        registration_sha256=registration_sha256,
    )


def _verify_existing_bytes(path: Path, expected: bytes, *, label: str) -> None:
    actual = _read_exact_regular_file(path, label=label)
    if actual != expected:
        raise ScreeningRegistryDriftError(f"{label} content drift at {path}")


def _publish_control_artifact(
    prepared: PreparedScreeningRegistration,
) -> tuple[Path, bool]:
    """Atomically publish or exactly reuse the one content-addressed control file."""

    root = prepared.data_root
    directory = prepared.control_artifact_directory
    current = root
    for component in CONTROL_ARTIFACT_SUBDIRECTORY.parts:
        current /= component
        if current.is_symlink():
            raise ScreeningRegistryDriftError(
                f"control artifact directory cannot be a symbolic link: {current}"
            )
        if current.exists() and not current.is_dir():
            raise ScreeningRegistryDriftError(
                f"control artifact directory is not a directory: {current}"
            )
        current.mkdir(exist_ok=True, mode=0o700)
    resolved_directory = directory.resolve(strict=True)
    if resolved_directory != directory or not resolved_directory.is_relative_to(root):
        raise ScreeningRegistryDriftError("control artifact directory escaped data_root")
    destination = prepared.control_artifact_path
    if destination.exists() or destination.is_symlink():
        _verify_existing_bytes(
            destination,
            prepared.registration_bytes,
            label="Phase 1A control artifact",
        )
        return destination, False

    handle = tempfile.NamedTemporaryFile(  # noqa: SIM115
        mode="w+b",
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=directory,
        delete=False,
    )
    temporary_path = Path(handle.name)
    try:
        os.fchmod(handle.fileno(), 0o600)
        handle.write(prepared.registration_bytes)
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        try:
            os.link(temporary_path, destination)
            created = True
        except FileExistsError:
            _verify_existing_bytes(
                destination,
                prepared.registration_bytes,
                label="Phase 1A control artifact",
            )
            created = False
        temporary_path.unlink()
        directory_descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except BaseException:
        handle.close()
        temporary_path.unlink(missing_ok=True)
        raise
    return destination, created


def _row_or_error(row: dict[str, Any] | None, *, label: str) -> dict[str, Any]:
    if row is None:
        raise ScreeningRegistryError(f"{label} does not exist")
    return row


def _assert_fields(
    *,
    label: str,
    row: Mapping[str, Any],
    expected: Mapping[str, object],
) -> None:
    mismatches = [key for key, value in expected.items() if row.get(key) != value]
    if mismatches:
        raise ScreeningRegistryDriftError(
            f"{label} immutable content drift in fields: {', '.join(sorted(mismatches))}"
        )


def _load_registration_baseline(
    connection: psycopg.Connection[dict[str, Any]],
    prepared: PreparedScreeningRegistration,
    *,
    artifact_stack: ExitStack,
    held_artifacts: dict[Path, _HeldArtifactFile],
) -> _RegistrationBaseline | None:
    """Load and compare the original immutable control document for a code revision."""

    campaign = connection.execute(
        """
        SELECT campaign_id, code_commit, config_sha256
        FROM systematic_fx.campaigns
        WHERE campaign_key = %s
        FOR SHARE
        """,
        (CAMPAIGN_ID,),
    ).fetchone()
    if campaign is None:
        return None
    campaign_id = campaign.get("campaign_id")
    if isinstance(campaign_id, bool) or not isinstance(campaign_id, int) or campaign_id <= 0:
        raise ScreeningRegistryDriftError("baseline Phase 1A campaign ID is invalid")
    baseline_commit = _git_object_id(
        campaign.get("code_commit"),
        label="baseline Phase 1A campaign code_commit",
    )
    baseline_sha256 = _sha256(
        campaign.get("config_sha256"),
        label="baseline Phase 1A campaign config_sha256",
    )
    artifact_key = f"phase1a-screening-registry:{baseline_sha256}"
    artifact_path = prepared.control_artifact_directory / f"{baseline_sha256}.json"
    rows = connection.execute(
        """
        SELECT artifact_id, artifact_key, artifact_type, uri, sha256, byte_size,
               media_type, producer_job_id, metadata
        FROM systematic_fx.artifacts
        WHERE artifact_key = %s
        FOR SHARE
        """,
        (artifact_key,),
    ).fetchall()
    if len(rows) != 1:
        raise ScreeningRegistryDriftError(
            "baseline Phase 1A control artifact must resolve to exactly one row"
        )
    artifact = rows[0]
    byte_size = artifact.get("byte_size")
    artifact_id = artifact.get("artifact_id")
    metadata = artifact.get("metadata")
    if (
        isinstance(byte_size, bool)
        or not isinstance(byte_size, int)
        or byte_size <= 0
        or isinstance(artifact_id, bool)
        or not isinstance(artifact_id, int)
        or artifact_id <= 0
        or not isinstance(metadata, Mapping)
        or set(metadata)
        != {
            "artifact_schema",
            "calendar_artifact_id",
            "campaign_key",
            "code_snapshot_artifact_id",
            "holdout_boundaries_embedded",
            "result_visibility",
            "split_artifact_id",
        }
    ):
        raise ScreeningRegistryDriftError("baseline Phase 1A control artifact row is malformed")
    _assert_fields(
        label="baseline Phase 1A control artifact",
        row=artifact,
        expected={
            "artifact_key": artifact_key,
            "artifact_type": "PHASE1A_SCREENING_REGISTRY",
            "uri": artifact_path.as_uri(),
            "sha256": baseline_sha256,
            "media_type": "application/json",
            "producer_job_id": None,
        },
    )
    _assert_fields(
        label="baseline Phase 1A control artifact metadata",
        row=metadata,
        expected={
            "artifact_schema": REGISTRATION_SCHEMA,
            "campaign_key": CAMPAIGN_ID,
            "holdout_boundaries_embedded": False,
            "result_visibility": _SEALED_SPLIT,
        },
    )
    for field in (
        "calendar_artifact_id",
        "code_snapshot_artifact_id",
        "split_artifact_id",
    ):
        value = metadata.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ScreeningRegistryDriftError(
                f"baseline Phase 1A control artifact metadata {field} is invalid"
            )
    held = held_artifacts.get(artifact_path)
    if held is None:
        held = _open_verified_artifact_file(
            artifact_path,
            expected_sha256=baseline_sha256,
            expected_byte_size=byte_size,
            label="baseline Phase 1A control artifact",
        )
        artifact_stack.callback(os.close, held.descriptor)
        held_artifacts[artifact_path] = held
    elif held.sha256 != baseline_sha256 or held.byte_size != byte_size:
        raise ScreeningRegistryDriftError(
            "held baseline Phase 1A control artifact differs from its database row"
        )
    content = _read_held_artifact_bytes(
        held,
        label="baseline Phase 1A control artifact",
    )
    try:
        document = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ScreeningRegistryDriftError(
            "baseline Phase 1A control artifact is not valid JSON"
        ) from exc
    baseline_code = document.get("code") if isinstance(document, dict) else None
    if (
        not isinstance(document, dict)
        or not isinstance(baseline_code, dict)
        or canonical_json_bytes(document) + b"\n" != content
        or baseline_code.get("commit") != baseline_commit
    ):
        raise ScreeningRegistryDriftError(
            "baseline Phase 1A control artifact canonical identity drift"
        )
    _assert_registration_revision_invariants(document, prepared.registration_document)
    return _RegistrationBaseline(
        campaign_id=campaign_id,
        code_commit=baseline_commit,
        control_artifact_id=artifact_id,
        control_artifact_sha256=baseline_sha256,
        control_document=document,
    )


def _verify_dataset_and_sources(
    connection: psycopg.Connection[dict[str, Any]],
    prepared: PreparedScreeningRegistration,
) -> tuple[int, dict[date, int]]:
    row = connection.execute(
        """
        SELECT dataset_id, dataset_key, provider, feed, data_schema, root_uri,
               price_scale_exponent, status, expected_start_date, expected_end_date,
               manifest_sha256
        FROM systematic_fx.datasets
        WHERE dataset_key = %s
        FOR UPDATE
        """,
        (prepared.dataset_key,),
    ).fetchone()
    row = _row_or_error(row, label=f"source-registered dataset {prepared.dataset_key}")
    screening = prepared.screening_bundle
    expected = {
        "dataset_key": prepared.dataset_key,
        "provider": "Databento",
        "feed": "GLBX.MDP3",
        "data_schema": "mbp-10",
        "root_uri": (prepared.data_root / "mbp-10").resolve().as_uri(),
        "price_scale_exponent": -9,
        "expected_start_date": date.fromisoformat(screening.source_start),
        "expected_end_date": date.fromisoformat(screening.source_end),
        "manifest_sha256": prepared.calendar.source_manifest_sha256,
    }
    _assert_fields(label=f"dataset {prepared.dataset_key}", row=row, expected=expected)
    if row["status"] not in {"VALIDATING", "READY"}:
        raise ScreeningRegistryDriftError(
            f"dataset {prepared.dataset_key} status is not usable: {row['status']}"
        )
    dataset_id = int(row["dataset_id"])

    rows = connection.execute(
        """
        SELECT source_file_id, source_date, status, sha256
        FROM systematic_fx.source_files
        WHERE dataset_id = %s
        ORDER BY source_date
        FOR SHARE
        """,
        (dataset_id,),
    ).fetchall()
    expected_dates = tuple(
        sorted((*prepared.calendar.source_dates, *prepared.calendar.excluded_source_dates))
    )
    if len(rows) != len(expected_dates):
        raise ScreeningRegistryDriftError("dataset source-file count differs from calendar")
    source_ids: dict[date, int] = {}
    for expected_date, source in zip(expected_dates, rows, strict=True):
        if source.get("source_date") != expected_date:
            raise ScreeningRegistryDriftError("dataset source dates differ from calendar")
        if source.get("status") not in {"HASHED", "VALIDATED"}:
            raise ScreeningRegistryDriftError(
                f"source {expected_date.isoformat()} is not content-hashed"
            )
        _sha256(source.get("sha256"), label=f"source {expected_date.isoformat()} sha256")
        source_file_id = int(source["source_file_id"])
        if expected_date in source_ids:
            raise ScreeningRegistryDriftError("duplicate source date in database")
        source_ids[expected_date] = source_file_id
    _verify_full_qc_evidence(
        connection,
        prepared=prepared,
        dataset_id=dataset_id,
        source_ids=source_ids,
    )
    return dataset_id, source_ids


def _verify_full_qc_evidence(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    prepared: PreparedScreeningRegistration,
    dataset_id: int,
    source_ids: Mapping[date, int],
) -> None:
    """Bind campaign eligibility to the already-registered exact full-QC job."""

    jobs = connection.execute(
        """
        SELECT job_id, status, payload, result
        FROM systematic_fx.jobs
        WHERE dataset_id = %s
          AND job_type = %s
          AND payload ->> 'scan_manifest_sha256' = %s
        FOR SHARE
        """,
        (dataset_id, _FULL_QC_JOB_TYPE, prepared.calendar.qc_manifest_sha256),
    ).fetchall()
    if len(jobs) != 1:
        raise ScreeningRegistryDriftError(
            "exact full-QC manifest must resolve to one registered job"
        )
    job = jobs[0]
    _assert_fields(
        label="full-QC registration job",
        row=job,
        expected={"status": "SUCCEEDED"},
    )
    payload = job.get("payload")
    result = job.get("result")
    if not isinstance(payload, Mapping) or not isinstance(result, Mapping):
        raise ScreeningRegistryDriftError("full-QC job payload/result must be objects")
    _assert_fields(
        label="full-QC registration payload",
        row=payload,
        expected={
            "config_sha256": prepared.calendar.qc_config_sha256,
            "file_count": prepared.calendar.source_record_count,
            "scan_manifest_sha256": prepared.calendar.qc_manifest_sha256,
            "source_manifest_sha256": prepared.calendar.source_manifest_sha256,
        },
    )
    expected_result_counts = {
        "ERROR": 0,
        "FAIL": prepared.calendar.qc_fail_record_count,
        "PASS": prepared.calendar.qc_pass_record_count,
        "WARN": 0,
    }
    _assert_fields(
        label="full-QC registration result",
        row=result,
        expected={"result_counts": expected_result_counts},
    )

    checks = connection.execute(
        """
        SELECT source_file_id, result, observed
        FROM systematic_fx.quality_checks
        WHERE job_id = %s AND check_name = %s
        ORDER BY source_file_id
        FOR SHARE
        """,
        (int(job["job_id"]), _FULL_QC_SOURCE_CHECK),
    ).fetchall()
    if len(checks) != prepared.calendar.source_record_count:
        raise ScreeningRegistryDriftError("full-QC per-source evidence count drift")
    date_by_source_id = {source_file_id: day for day, source_file_id in source_ids.items()}
    seen_dates: set[date] = set()
    excluded = frozenset(prepared.calendar.excluded_source_dates)
    for check in checks:
        source_file_id = int(check["source_file_id"])
        source_date = date_by_source_id.get(source_file_id)
        if source_date is None or source_date in seen_dates:
            raise ScreeningRegistryDriftError("full-QC source identity drift")
        seen_dates.add(source_date)
        expected_result = "FAIL" if source_date in excluded else "PASS"
        observed = check.get("observed")
        if not isinstance(observed, Mapping):
            raise ScreeningRegistryDriftError("full-QC source observation must be an object")
        _assert_fields(
            label=f"full-QC source {source_date.isoformat()}",
            row=check,
            expected={"result": expected_result},
        )
        _assert_fields(
            label=f"full-QC observation {source_date.isoformat()}",
            row=observed,
            expected={
                "coverage_complete": True,
                "result": expected_result,
                "schema_fingerprint": prepared.calendar.schema_fingerprint,
                "source_date": source_date.isoformat(),
                "source_manifest_sha256": prepared.calendar.source_manifest_sha256,
            },
        )
    if seen_dates != set(source_ids):
        raise ScreeningRegistryDriftError("full-QC evidence does not cover every source date")


def _ensure_artifact(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    artifact_key: str,
    artifact_type: str,
    path: Path,
    sha256: str,
    byte_size: int,
    metadata: Mapping[str, object],
    held_file: _HeldArtifactFile,
    allow_create: bool,
) -> tuple[int, bool]:
    _verify_held_artifact_binding(held_file, label=f"artifact {artifact_key}")
    if held_file.path != path or held_file.byte_size != byte_size or held_file.sha256 != sha256:
        raise ScreeningRegistryDriftError(
            f"artifact {artifact_key} descriptor differs from the registered identity"
        )
    uri = path.as_uri()
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.artifacts
            (artifact_key, artifact_type, uri, sha256, byte_size, media_type,
             producer_job_id, metadata)
        VALUES (%s, %s, %s, %s, %s, 'application/json', NULL, %s)
        ON CONFLICT DO NOTHING
        RETURNING artifact_id
        """,
        (artifact_key, artifact_type, uri, sha256, byte_size, Jsonb(dict(metadata))),
    ).fetchone()
    if inserted is not None and not allow_create:
        raise ScreeningRegistryDriftError(
            f"code-only revision cannot create missing baseline artifact {artifact_key}"
        )
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
        raise ScreeningRegistryDriftError(
            f"artifact {artifact_key} key/URI resolves to {len(rows)} rows"
        )
    row = rows[0]
    _assert_fields(
        label=f"artifact {artifact_key}",
        row=row,
        expected={
            "artifact_key": artifact_key,
            "artifact_type": artifact_type,
            "uri": uri,
            "sha256": sha256,
            "byte_size": byte_size,
            "media_type": "application/json",
            "producer_job_id": None,
            "metadata": dict(metadata),
        },
    )
    return int(row["artifact_id"]), inserted is not None


def _ensure_campaign(
    connection: psycopg.Connection[dict[str, Any]],
    prepared: PreparedScreeningRegistration,
    dataset_id: int,
    *,
    baseline: _RegistrationBaseline | None,
) -> tuple[int, bool]:
    spec = prepared.campaign_document
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.campaigns
            (campaign_key, dataset_id, name, status, selected_start_date,
             selected_end_date, roll_cutoff_date, data_manifest_sha256,
             feature_version, outcome_version, cost_model_version,
             execution_model_version, code_commit, config_sha256, split_policy,
             trial_budget, finalist_budget)
        VALUES (%s, %s, %s, 'DRAFT', %s, %s, NULL, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s)
        ON CONFLICT (campaign_key) DO NOTHING
        RETURNING campaign_id
        """,
        (
            spec["campaign_key"],
            dataset_id,
            spec["name"],
            spec["selected_start_date"],
            spec["selected_end_date"],
            spec["data_manifest_sha256"],
            spec["feature_version"],
            spec["outcome_version"],
            spec["cost_model_version"],
            spec["execution_model_version"],
            prepared.code_commit,
            prepared.registration_sha256,
            Jsonb(dict(spec["split_policy"])),
            spec["trial_budget"],
            spec["finalist_budget"],
        ),
    ).fetchone()
    row = connection.execute(
        """
        SELECT campaign_id, campaign_key, dataset_id, name, status,
               selected_start_date, selected_end_date, roll_cutoff_date,
               data_manifest_sha256, feature_version, outcome_version,
               cost_model_version, execution_model_version, code_commit,
               config_sha256, split_policy, trial_budget, finalist_budget
        FROM systematic_fx.campaigns
        WHERE campaign_key = %s
        FOR UPDATE
        """,
        (spec["campaign_key"],),
    ).fetchone()
    row = _row_or_error(row, label=f"campaign {CAMPAIGN_ID}")
    created = inserted is not None
    if created == (baseline is not None):
        raise ScreeningRegistryDriftError(
            "Phase 1A campaign existence changed during revision registration"
        )
    expected_code_commit = prepared.code_commit if baseline is None else baseline.code_commit
    expected_config_sha256 = (
        prepared.registration_sha256 if baseline is None else baseline.control_artifact_sha256
    )
    _assert_fields(
        label=f"campaign {CAMPAIGN_ID}",
        row=row,
        expected={
            "campaign_key": CAMPAIGN_ID,
            "dataset_id": dataset_id,
            "name": spec["name"],
            "status": "DRAFT",
            "selected_start_date": spec["selected_start_date"],
            "selected_end_date": spec["selected_end_date"],
            "roll_cutoff_date": None,
            "data_manifest_sha256": spec["data_manifest_sha256"],
            "feature_version": spec["feature_version"],
            "outcome_version": spec["outcome_version"],
            "cost_model_version": spec["cost_model_version"],
            "execution_model_version": spec["execution_model_version"],
            "code_commit": expected_code_commit,
            "config_sha256": expected_config_sha256,
            "split_policy": spec["split_policy"],
            "trial_budget": spec["trial_budget"],
            "finalist_budget": spec["finalist_budget"],
        },
    )
    campaign_id = int(row["campaign_id"])
    if baseline is not None and campaign_id != baseline.campaign_id:
        raise ScreeningRegistryDriftError("Phase 1A baseline campaign identity changed")
    return campaign_id, created


def _ensure_splits(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    campaign_id: int,
    specs: Sequence[CampaignSplitSpec],
    allow_create: bool,
) -> tuple[dict[str, int], int]:
    split_ids: dict[str, int] = {}
    created_count = 0
    for spec in specs:
        inserted = connection.execute(
            """
            INSERT INTO systematic_fx.campaign_splits
                (campaign_id, split_key, split_role, fold_number, start_date,
                 end_date, start_active_ordinal, end_active_ordinal,
                 purge_before_days, purge_after_days, result_visibility, revealed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)
            ON CONFLICT (campaign_id, split_key) DO NOTHING
            RETURNING campaign_split_id
            """,
            (
                campaign_id,
                spec.split_key,
                spec.split_role,
                spec.fold_number,
                spec.start_date,
                spec.end_date,
                spec.start_active_ordinal,
                spec.end_active_ordinal,
                spec.purge_before_days,
                spec.purge_after_days,
                spec.result_visibility,
            ),
        ).fetchone()
        if inserted is not None and not allow_create:
            raise ScreeningRegistryDriftError(
                "code-only revision cannot create a missing baseline campaign split"
            )
        row = connection.execute(
            """
            SELECT campaign_split_id, campaign_id, split_key, split_role,
                   fold_number, start_date, end_date, start_active_ordinal,
                   end_active_ordinal, purge_before_days, purge_after_days,
                   result_visibility, revealed_at
            FROM systematic_fx.campaign_splits
            WHERE campaign_id = %s AND split_key = %s
            FOR UPDATE
            """,
            (campaign_id, spec.split_key),
        ).fetchone()
        row = _row_or_error(row, label=f"campaign split {spec.split_key}")
        _assert_fields(
            label=f"campaign split {spec.split_key}",
            row=row,
            expected={
                "campaign_id": campaign_id,
                "split_key": spec.split_key,
                "split_role": spec.split_role,
                "fold_number": spec.fold_number,
                "start_date": spec.start_date,
                "end_date": spec.end_date,
                "start_active_ordinal": spec.start_active_ordinal,
                "end_active_ordinal": spec.end_active_ordinal,
                "purge_before_days": spec.purge_before_days,
                "purge_after_days": spec.purge_after_days,
                "result_visibility": spec.result_visibility,
                "revealed_at": None,
            },
        )
        split_ids[spec.split_key] = int(row["campaign_split_id"])
        created_count += int(inserted is not None)
    if len(split_ids) != len(specs):
        raise ScreeningRegistryDriftError("not every campaign split has one identity")
    rows = connection.execute(
        """
        SELECT split_key
        FROM systematic_fx.campaign_splits
        WHERE campaign_id = %s
        ORDER BY split_key
        FOR UPDATE
        """,
        (campaign_id,),
    ).fetchall()
    expected_keys = {spec.split_key for spec in specs}
    observed_keys = {row.get("split_key") for row in rows}
    if len(rows) != len(expected_keys) or observed_keys != expected_keys:
        raise ScreeningRegistryDriftError(
            "campaign split key set differs from the registered baseline"
        )
    return split_ids, created_count


def _ensure_days(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    prepared: PreparedScreeningRegistration,
    dataset_id: int,
    campaign_id: int,
    source_ids: Mapping[date, int],
    split_ids: Mapping[str, int],
    allow_create: bool,
) -> int:
    requested: list[dict[str, object]] = []
    for spec in prepared.day_specs:
        requested.append(
            {
                "active_day_ordinal": spec.active_day_ordinal,
                "calendar_date": spec.calendar_date.isoformat(),
                "campaign_split_id": (
                    split_ids[spec.split_key] if spec.split_key is not None else None
                ),
                "eligibility_status": spec.eligibility_status,
                "exclusion_reason": spec.exclusion_reason,
                "metadata": dict(spec.metadata),
                "source_file_id": source_ids[spec.calendar_date],
            }
        )
    inserted = connection.execute(
        """
        WITH requested AS (
            SELECT *
            FROM jsonb_to_recordset(%s::jsonb) AS item(
                calendar_date date,
                active_day_ordinal integer,
                eligibility_status text,
                exclusion_reason text,
                campaign_split_id bigint,
                source_file_id bigint,
                metadata jsonb
            )
        )
        INSERT INTO systematic_fx.campaign_days
            (dataset_id, campaign_id, calendar_date, active_day_ordinal,
             eligibility_status, exclusion_reason, campaign_split_id,
             source_file_id, execution_instrument_id, is_roll_cutoff, metadata)
        SELECT %s, %s, calendar_date, active_day_ordinal, eligibility_status,
               exclusion_reason, campaign_split_id, source_file_id, NULL, false, metadata
        FROM requested
        ON CONFLICT (campaign_id, calendar_date) DO NOTHING
        RETURNING campaign_day_id
        """,
        (Jsonb(requested), dataset_id, campaign_id),
    ).fetchall()
    if inserted and not allow_create:
        raise ScreeningRegistryDriftError(
            "code-only revision cannot create missing baseline campaign days"
        )
    rows = connection.execute(
        """
        SELECT dataset_id, campaign_id, calendar_date, active_day_ordinal,
               eligibility_status, exclusion_reason, campaign_split_id,
               source_file_id, execution_instrument_id, is_roll_cutoff, metadata
        FROM systematic_fx.campaign_days
        WHERE campaign_id = %s
        ORDER BY calendar_date
        FOR UPDATE
        """,
        (campaign_id,),
    ).fetchall()
    if len(rows) != len(requested):
        raise ScreeningRegistryDriftError("campaign day count differs from calendar")
    for expected, row in zip(requested, rows, strict=True):
        _assert_fields(
            label=f"campaign day {expected['calendar_date']}",
            row=row,
            expected={
                "dataset_id": dataset_id,
                "campaign_id": campaign_id,
                "calendar_date": date.fromisoformat(str(expected["calendar_date"])),
                "active_day_ordinal": expected["active_day_ordinal"],
                "eligibility_status": expected["eligibility_status"],
                "exclusion_reason": expected["exclusion_reason"],
                "campaign_split_id": expected["campaign_split_id"],
                "source_file_id": expected["source_file_id"],
                "execution_instrument_id": None,
                "is_roll_cutoff": False,
                "metadata": expected["metadata"],
            },
        )
    excluded = [row for row in rows if row["eligibility_status"] == "INELIGIBLE"]
    if {row["calendar_date"] for row in excluded} != set(PHASE1A_EXCLUDED_SOURCE_DATES):
        raise ScreeningRegistryDriftError("database INELIGIBLE days differ from six QC FAIL days")
    return len(inserted)


def _ensure_experiments(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    prepared: PreparedScreeningRegistration,
    campaign_id: int,
    registration_artifact_id: int,
    baseline: _RegistrationBaseline | None,
) -> tuple[tuple[int, ...], int]:
    experiment_ids: list[int] = []
    created_count = 0
    for spec in prepared.experiment_specs:
        hypothesis = spec.hypothesis
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
                spec.experiment_key,
                campaign_id,
                hypothesis.family,
                hypothesis.hypothesis,
                hypothesis.direction,
                hypothesis.model_family,
                Decimal(prepared.hypothesis_bundle.tick_size),
                Decimal(prepared.hypothesis_bundle.tick_value),
                Jsonb(dict(spec.feature_definition_versions)),
                Jsonb(dict(spec.search_boundary)),
                Jsonb(dict(spec.cost_assumptions)),
                Jsonb(dict(spec.execution_assumptions)),
                spec.trial_budget,
                registration_artifact_id,
                prepared.code_commit,
                spec.config_sha256,
            ),
        ).fetchone()
        if baseline is not None and inserted is not None:
            raise ScreeningRegistryDriftError(
                "code-only revision cannot create a missing baseline experiment"
            )
        row = connection.execute(
            """
            SELECT experiment_id, experiment_key, campaign_id, pattern_id,
                   parent_experiment_id, primary_family, status, hypothesis,
                   direction, model_family, tick_size, tick_value,
                   feature_definition_versions, search_boundary, cost_assumptions,
                   execution_assumptions, trial_budget, trials_registered,
                   registration_artifact_id, code_commit, config_sha256
            FROM systematic_fx.experiments
            WHERE experiment_key = %s
            FOR SHARE
            """,
            (spec.experiment_key,),
        ).fetchone()
        row = _row_or_error(row, label=f"experiment {spec.experiment_key}")
        expected_registration_artifact_id = (
            registration_artifact_id if baseline is None else baseline.control_artifact_id
        )
        expected_code_commit = prepared.code_commit if baseline is None else baseline.code_commit
        _assert_fields(
            label=f"experiment {spec.experiment_key}",
            row=row,
            expected={
                "experiment_key": spec.experiment_key,
                "campaign_id": campaign_id,
                "pattern_id": None,
                "parent_experiment_id": None,
                "primary_family": hypothesis.family,
                "status": "REGISTERED",
                "hypothesis": hypothesis.hypothesis,
                "direction": hypothesis.direction,
                "model_family": hypothesis.model_family,
                "tick_size": Decimal(prepared.hypothesis_bundle.tick_size),
                "tick_value": Decimal(prepared.hypothesis_bundle.tick_value),
                "feature_definition_versions": spec.feature_definition_versions,
                "search_boundary": spec.search_boundary,
                "cost_assumptions": spec.cost_assumptions,
                "execution_assumptions": spec.execution_assumptions,
                "trial_budget": spec.trial_budget,
                "trials_registered": 0,
                "registration_artifact_id": expected_registration_artifact_id,
                "code_commit": expected_code_commit,
                "config_sha256": spec.config_sha256,
            },
        )
        experiment_ids.append(int(row["experiment_id"]))
        created_count += int(inserted is not None)
    if len(experiment_ids) != EXPECTED_PARENT_COUNT or len(set(experiment_ids)) != (
        EXPECTED_PARENT_COUNT
    ):
        raise ScreeningRegistryDriftError("campaign must resolve exactly 60 parent experiments")
    family_rows = connection.execute(
        """
        SELECT primary_family, count(*)::integer AS parent_count
        FROM systematic_fx.experiments
        WHERE campaign_id = %s AND parent_experiment_id IS NULL
        GROUP BY primary_family
        """,
        (campaign_id,),
    ).fetchall()
    observed = {str(row["primary_family"]): int(row["parent_count"]) for row in family_rows}
    expected = family_counts(prepared.hypothesis_bundle.hypotheses)
    if observed != expected:
        raise ScreeningRegistryDriftError(
            f"campaign parent-family counts differ from source bundle: {observed}"
        )
    return tuple(experiment_ids), created_count


def _register_prepared(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    prepared: PreparedScreeningRegistration,
    control_artifact_path: Path,
    held_artifacts: dict[Path, _HeldArtifactFile],
    artifact_stack: ExitStack,
) -> _DatabaseRegistration:
    baseline = _load_registration_baseline(
        connection,
        prepared,
        artifact_stack=artifact_stack,
        held_artifacts=held_artifacts,
    )
    dataset_id, source_ids = _verify_dataset_and_sources(connection, prepared)
    calendar_artifact_id, created_calendar = _ensure_artifact(
        connection,
        artifact_key=f"phase1a-calendar:{prepared.calendar.sha256}",
        artifact_type="PHASE1A_ELIGIBLE_CALENDAR",
        path=prepared.calendar_artifact.path,
        sha256=prepared.calendar_artifact.sha256,
        byte_size=prepared.calendar_artifact.byte_size,
        metadata={
            "artifact_schema": CALENDAR_SCHEMA,
            "calendar_version": CALENDAR_VERSION,
            "campaign_key": CAMPAIGN_ID,
            "result_visibility": _VISIBLE_SPLIT,
            "source_manifest_sha256": prepared.calendar.source_manifest_sha256,
        },
        held_file=_held_artifact_for_path(
            held_artifacts,
            prepared.calendar_artifact.path,
        ),
        allow_create=baseline is None,
    )
    split_artifact_id, created_split_artifact = _ensure_artifact(
        connection,
        artifact_key=f"phase1a-split:{prepared.split.sha256}",
        artifact_type="PHASE1A_CAMPAIGN_SPLIT",
        path=prepared.split_artifact.path,
        sha256=prepared.split_artifact.sha256,
        byte_size=prepared.split_artifact.byte_size,
        metadata={
            "artifact_schema": SPLIT_SCHEMA,
            "campaign_key": CAMPAIGN_ID,
            "holdout_revealed": False,
            "result_visibility": _SEALED_SPLIT,
            "split_version": SPLIT_VERSION,
        },
        held_file=_held_artifact_for_path(
            held_artifacts,
            prepared.split_artifact.path,
        ),
        allow_create=baseline is None,
    )
    code_snapshot_artifact_id, created_code_snapshot = _ensure_artifact(
        connection,
        artifact_key=f"phase1a-code-snapshot:{prepared.code_snapshot_artifact.sha256}",
        artifact_type="PHASE1A_CODE_SNAPSHOT",
        path=prepared.code_snapshot_artifact.path,
        sha256=prepared.code_snapshot_artifact.sha256,
        byte_size=prepared.code_snapshot_artifact.byte_size,
        metadata={
            "artifact_schema": CODE_SNAPSHOT_SCHEMA,
            "campaign_key": CAMPAIGN_ID,
            "code_commit": prepared.code_commit,
            "result_visibility": _INTERNAL_PROVENANCE,
        },
        held_file=_held_artifact_for_path(
            held_artifacts,
            prepared.code_snapshot_artifact.path,
        ),
        allow_create=True,
    )
    control_artifact_id, created_control = _ensure_artifact(
        connection,
        artifact_key=f"phase1a-screening-registry:{prepared.registration_sha256}",
        artifact_type="PHASE1A_SCREENING_REGISTRY",
        path=control_artifact_path,
        sha256=prepared.registration_sha256,
        byte_size=len(prepared.registration_bytes),
        metadata={
            "artifact_schema": REGISTRATION_SCHEMA,
            "campaign_key": CAMPAIGN_ID,
            "calendar_artifact_id": calendar_artifact_id,
            "code_snapshot_artifact_id": code_snapshot_artifact_id,
            "holdout_boundaries_embedded": False,
            "result_visibility": _SEALED_SPLIT,
            "split_artifact_id": split_artifact_id,
        },
        held_file=_held_artifact_for_path(held_artifacts, control_artifact_path),
        allow_create=True,
    )
    campaign_id, created_campaign = _ensure_campaign(
        connection,
        prepared,
        dataset_id,
        baseline=baseline,
    )
    split_ids_by_key, created_splits = _ensure_splits(
        connection,
        campaign_id=campaign_id,
        specs=prepared.split_specs,
        allow_create=baseline is None,
    )
    created_days = _ensure_days(
        connection,
        prepared=prepared,
        dataset_id=dataset_id,
        campaign_id=campaign_id,
        source_ids=source_ids,
        split_ids=split_ids_by_key,
        allow_create=baseline is None,
    )
    experiment_ids, created_experiments = _ensure_experiments(
        connection,
        prepared=prepared,
        campaign_id=campaign_id,
        registration_artifact_id=control_artifact_id,
        baseline=baseline,
    )
    if baseline is not None and any(
        (
            created_calendar,
            created_split_artifact,
            created_campaign,
            created_splits,
            created_days,
            created_experiments,
        )
    ):
        raise ScreeningRegistryDriftError(
            "code-only revision changed baseline campaign relational state"
        )
    return _DatabaseRegistration(
        dataset_id=dataset_id,
        campaign_id=campaign_id,
        calendar_artifact_id=calendar_artifact_id,
        split_artifact_id=split_artifact_id,
        code_snapshot_artifact_id=code_snapshot_artifact_id,
        control_artifact_id=control_artifact_id,
        split_ids=tuple(split_ids_by_key[spec.split_key] for spec in prepared.split_specs),
        experiment_ids=experiment_ids,
        created_campaign=created_campaign,
        created_artifacts=sum(
            (
                created_calendar,
                created_split_artifact,
                created_code_snapshot,
                created_control,
            )
        ),
        created_splits=created_splits,
        created_days=created_days,
        created_experiments=created_experiments,
    )


@_translate_psycopg_errors("Phase 1A screening registration")
def register_phase1a_screening_campaign(
    database_url: str,
    *,
    project_root: Path | str,
    data_root: Path | str,
    calendar: Phase1AScreeningCalendar,
    split: Phase1AScreeningSplit,
    calendar_artifact_path: Path | str,
    split_artifact_path: Path | str,
    code_snapshot_artifact_path: Path | str,
    code_commit: str,
    code_snapshot_sha256: str,
    cost_input_manifest_sha256: str,
    dataset_key: str = DEFAULT_DATASET_KEY,
) -> ScreeningRegistrationReport:
    """Publish the control artifact and atomically register/verify Phase 1A state."""

    database_url = _nonempty(database_url, label="database_url")
    prepared = prepare_phase1a_screening_registration(
        project_root=project_root,
        data_root=data_root,
        calendar=calendar,
        split=split,
        calendar_artifact_path=calendar_artifact_path,
        split_artifact_path=split_artifact_path,
        code_snapshot_artifact_path=code_snapshot_artifact_path,
        code_commit=code_commit,
        code_snapshot_sha256=code_snapshot_sha256,
        cost_input_manifest_sha256=cost_input_manifest_sha256,
        dataset_key=dataset_key,
    )
    control_artifact_path, created_control_artifact = _publish_control_artifact(prepared)

    artifact_specs = (
        (
            prepared.calendar_artifact.path,
            prepared.calendar_artifact.sha256,
            prepared.calendar_artifact.byte_size,
            "calendar artifact",
        ),
        (
            prepared.split_artifact.path,
            prepared.split_artifact.sha256,
            prepared.split_artifact.byte_size,
            "split artifact",
        ),
        (
            prepared.code_snapshot_artifact.path,
            prepared.code_snapshot_artifact.sha256,
            prepared.code_snapshot_artifact.byte_size,
            "code snapshot artifact",
        ),
        (
            control_artifact_path,
            prepared.registration_sha256,
            len(prepared.registration_bytes),
            "screening registration artifact",
        ),
    )
    with ExitStack() as artifact_stack:
        held_artifacts: dict[Path, _HeldArtifactFile] = {}
        for path, sha256, byte_size, label in artifact_specs:
            held = _open_verified_artifact_file(
                path,
                expected_sha256=sha256,
                expected_byte_size=byte_size,
                label=label,
            )
            artifact_stack.callback(os.close, held.descriptor)
            held_artifacts[path] = held
        if len(held_artifacts) != len(artifact_specs):
            raise ScreeningRegistryDriftError(
                "registration artifacts do not resolve to four distinct paths"
            )

        with psycopg.connect(database_url, row_factory=dict_row) as connection:
            connection.isolation_level = IsolationLevel.SERIALIZABLE
            with connection.transaction():
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (prepared.dataset_key,),
                )
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (CAMPAIGN_ID,),
                )
                registered = _register_prepared(
                    connection,
                    prepared=prepared,
                    control_artifact_path=control_artifact_path,
                    held_artifacts=held_artifacts,
                    artifact_stack=artifact_stack,
                )
                for held in held_artifacts.values():
                    _verify_held_artifact_binding(held, label="registration artifact")
            for held in held_artifacts.values():
                _verify_held_artifact_binding(held, label="registration artifact")

    return ScreeningRegistrationReport(
        dataset_id=registered.dataset_id,
        dataset_key=prepared.dataset_key,
        campaign_id=registered.campaign_id,
        campaign_key=CAMPAIGN_ID,
        calendar_artifact_id=registered.calendar_artifact_id,
        split_artifact_id=registered.split_artifact_id,
        code_snapshot_artifact_id=registered.code_snapshot_artifact_id,
        control_artifact_id=registered.control_artifact_id,
        control_artifact_path=control_artifact_path,
        control_artifact_sha256=prepared.registration_sha256,
        split_ids=registered.split_ids,
        experiment_ids=registered.experiment_ids,
        created_control_artifact=created_control_artifact,
        created_campaign=registered.created_campaign,
        created_artifacts=registered.created_artifacts,
        created_splits=registered.created_splits,
        created_days=registered.created_days,
        created_experiments=registered.created_experiments,
    )
