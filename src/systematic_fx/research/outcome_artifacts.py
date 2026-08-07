"""Immutable artifacts for the Phase 1A shared p5 outcome replay.

All publications are canonical, content addressed, read-only, and confined to
``data/derived``.  Daily Parquet shards retain the complete
:class:`ReplayResultRecord` contract, including first-class Buying, Selling,
and Loss prices and lossless nested event references.  JSON cache, checkpoint,
and final manifests form the portable lineage consumed by the append-only
outcome registry.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Final, Literal

import pyarrow as pa
import pyarrow.parquet as pq

from systematic_fx.backtest.barriers import BARRIER_TICKS
from systematic_fx.backtest.event_cache import (
    CACHE_SCHEMA,
    CACHE_VERSION,
    DailyCacheReport,
    EventCacheError,
    read_daily_executable_cache,
)
from systematic_fx.backtest.shared_replay import (
    ReplayResultRecord,
    SharedReplay,
    SharedReplayError,
)
from systematic_fx.db.outcome_registry import (
    CHECKPOINT_ARTIFACT_SCHEMA,
    DIRECTION_IDS,
    EXPECTED_SOURCE_OCCURRENCE_COUNT,
    EXPECTED_SOURCE_SLICE_COUNT,
    EXPECTED_SUMMARY_COUNT,
    OUTCOME_ARTIFACT_SCHEMA,
    OUTCOME_CONFIG_ID,
    P5_QUERY_ID,
    SCENARIO_IDS,
    OutcomeCellSummary,
    OutcomeRegistryError,
    validate_complete_cell_summaries,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256

DETAIL_SHARD_SCHEMA: Final = "systematic_fx.phase1a_outcome_detail_shard.v1"
DETAIL_SHARD_VERSION: Final = "phase1a_outcome_detail_shard_v1"
CACHE_MANIFEST_SCHEMA: Final = "systematic_fx.phase1a_outcome_cache_manifest.v1"
CACHE_MANIFEST_VERSION: Final = "phase1a_outcome_cache_manifest_v1"
CHECKPOINT_PROGRESS_SCHEMA: Final = "systematic_fx.phase1a_outcome_progress.v1"

_DETAIL_DIRECTORY: Final = Path("outcomes/phase1a_p5_outcome_replay_v1/detail_shards")
_CACHE_MANIFEST_DIRECTORY: Final = Path(
    "backtest_event_cache/phase1a_daily_executable_cache_v1/manifests"
)
_CHECKPOINT_DIRECTORY: Final = Path("outcomes/checkpoints/phase1a_p5_outcome_replay_v1")
_RESULT_DIRECTORY: Final = Path("outcomes/phase1a_p5_outcome_replay_v1")
_SHA256: Final = re.compile(r"[0-9a-f]{64}")
_WRITE_BITS: Final = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
_SCENARIO_RANK: Final = {value: index for index, value in enumerate(SCENARIO_IDS)}
_DIRECTION_RANK: Final = {value: index for index, value in enumerate(DIRECTION_IDS)}
_REFERENCE_FIELDS: Final = (
    "decision_ref",
    "eligibility_ref",
    "attempt_ref",
    "entry_ref",
    "trigger_ref",
    "fill_ref",
    "first_touch_censor_ref",
    "terminal_ref",
    "failure_ref",
)
_DETAIL_FIELDS: Final = (
    "signal_id",
    "decision_ts_recv_ns",
    "utc_month",
    "scenario_id",
    "direction",
    "contract_key",
    "cell_id",
    "take_profit_ticks",
    "stop_loss_ticks",
    "entry_status",
    "entry_eligibility_ts_recv_ns",
    "entry_fill_price_ticks",
    "buying_price_ticks",
    "selling_price_ticks",
    "loss_price_ticks",
    "take_profit_target_price_ticks",
    "stop_trigger_price_ticks",
    "first_touch_outcome",
    "portfolio_outcome",
    "exit_fill_price_ticks",
    "decision_ref",
    "eligibility_ref",
    "attempt_ref",
    "entry_ref",
    "trigger_ref",
    "fill_ref",
    "first_touch_censor_ref",
    "terminal_ref",
    "entry_limit_price_ticks",
    "route_event_count",
    "maximum_route_quote_gap_ns",
    "failure_ref",
    "occupying_signal_id",
    "no_fill_reason",
    "completion_ts_recv_ns",
)
_CACHE_MANIFEST_FIELDS: Final = {
    "artifact_schema",
    "artifact_version",
    "cache_count",
    "cache_entries",
    "cache_entries_sha256",
    "cache_plan_sha256",
    "cache_schema",
    "cache_version",
    "input_manifest_sha256",
    "partition_key",
}
_CHECKPOINT_FIELDS: Final = {
    "artifact_schema",
    "cache_manifest",
    "checkpoint_sequence",
    "completed_source_date_count",
    "detail_record_count",
    "detail_shard_manifest_sha256",
    "detail_shards",
    "input_lineage",
    "input_lineage_sha256",
    "last_completed_source_date",
    "outcome_config_id",
    "outcome_replay_manifest_id",
    "predecessor_checkpoint_sha256",
    "progress_metadata",
    "progress_metadata_sha256",
    "query_id",
    "replay_state",
    "replay_state_sha256",
    "run_fingerprint",
    "source_event_count",
}
_FINAL_RESULT_FIELDS: Final = {
    "artifact_schema",
    "cache_manifest",
    "cell_summaries",
    "cell_summaries_sha256",
    "detail_record_count",
    "detail_shard_count",
    "detail_shard_manifest_sha256",
    "detail_shards",
    "direction_ids",
    "input_lineage",
    "input_lineage_sha256",
    "final_checkpoint",
    "outcome_config_id",
    "query_id",
    "run_fingerprint",
    "scenario_ids",
    "source_artifact_manifest_sha256",
    "source_occurrence_count",
    "source_slice_count",
    "summary_row_count",
}
_REFERENCE_TYPE: Final = pa.struct(
    [
        pa.field("contract_key", pa.string(), nullable=False),
        pa.field("source_date", pa.date32(), nullable=False),
        pa.field("session_ordinal", pa.int32(), nullable=False),
        pa.field("event_index", pa.int64(), nullable=False),
        pa.field("ts_recv_ns", pa.int64(), nullable=False),
        pa.field("best_bid_ticks", pa.int64(), nullable=True),
        pa.field("best_ask_ticks", pa.int64(), nullable=True),
        pa.field("valid", pa.bool_(), nullable=False),
    ]
)
_DETAIL_SCHEMA: Final = pa.schema(
    [
        pa.field("signal_id", pa.string(), nullable=False),
        pa.field("decision_ts_recv_ns", pa.int64(), nullable=False),
        pa.field("utc_month", pa.string(), nullable=False),
        pa.field("scenario_id", pa.string(), nullable=False),
        pa.field("direction", pa.string(), nullable=False),
        pa.field("contract_key", pa.string(), nullable=False),
        pa.field("cell_id", pa.string(), nullable=False),
        pa.field("take_profit_ticks", pa.int32(), nullable=False),
        pa.field("stop_loss_ticks", pa.int32(), nullable=False),
        pa.field("entry_status", pa.string(), nullable=False),
        pa.field("entry_eligibility_ts_recv_ns", pa.int64(), nullable=False),
        pa.field("entry_fill_price_ticks", pa.int64(), nullable=True),
        pa.field("buying_price_ticks", pa.int64(), nullable=True),
        pa.field("selling_price_ticks", pa.int64(), nullable=True),
        pa.field("loss_price_ticks", pa.int64(), nullable=True),
        pa.field("take_profit_target_price_ticks", pa.int64(), nullable=True),
        pa.field("stop_trigger_price_ticks", pa.int64(), nullable=True),
        pa.field("first_touch_outcome", pa.string(), nullable=True),
        pa.field("portfolio_outcome", pa.string(), nullable=True),
        pa.field("exit_fill_price_ticks", pa.int64(), nullable=True),
        *(pa.field(name, _REFERENCE_TYPE, nullable=True) for name in _REFERENCE_FIELDS[:8]),
        pa.field("entry_limit_price_ticks", pa.int64(), nullable=True),
        pa.field("route_event_count", pa.int64(), nullable=False),
        pa.field("maximum_route_quote_gap_ns", pa.int64(), nullable=False),
        pa.field("failure_ref", _REFERENCE_TYPE, nullable=True),
        pa.field("occupying_signal_id", pa.string(), nullable=True),
        pa.field("no_fill_reason", pa.string(), nullable=True),
        pa.field("completion_ts_recv_ns", pa.int64(), nullable=True),
    ]
)


class OutcomeArtifactError(ValueError):
    """An immutable outcome artifact or lineage chain is invalid."""


@dataclass(frozen=True, slots=True)
class _FileIdentity:
    device: int
    inode: int
    mode: int
    size: int
    modified_ns: int
    changed_ns: int


@dataclass(frozen=True, slots=True)
class _ArtifactLocation:
    candidate: Path
    data_root: Path
    derived: Path
    relative: Path
    data_root_identity: _FileIdentity
    derived_identity: _FileIdentity
    component_identities: tuple[_FileIdentity, ...]

    @property
    def relative_uri(self) -> str:
        return self.relative.as_posix()


@dataclass(slots=True)
class _HeldArtifact:
    path: Path
    descriptor: int
    parent_path: Path
    parent_descriptor: int
    parent_identity: _FileIdentity
    filename: str
    relative_uri: str
    identity: _FileIdentity
    sha256: str
    byte_size: int

    def close(self) -> None:
        if self.descriptor >= 0:
            os.close(self.descriptor)
            self.descriptor = -1
        if self.parent_descriptor >= 0:
            os.close(self.parent_descriptor)
            self.parent_descriptor = -1


@dataclass(frozen=True, slots=True)
class DetailShardArtifact:
    path: Path
    relative_uri: str
    sha256: str
    byte_size: int
    disposition: Literal["CREATED", "REUSED"]
    shard_sequence: int
    source_date: date
    row_count: int
    record_manifest_sha256: str
    run_fingerprint: str

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_relative_uri": self.relative_uri,
            "artifact_sha256": self.sha256,
            "byte_size": self.byte_size,
            "record_manifest_sha256": self.record_manifest_sha256,
            "row_count": self.row_count,
            "run_fingerprint": self.run_fingerprint,
            "shard_sequence": self.shard_sequence,
            "source_date": self.source_date.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class LoadedDetailShard:
    artifact: DetailShardArtifact
    records: tuple[ReplayResultRecord, ...]


@dataclass(frozen=True, slots=True)
class CacheManifestArtifact:
    path: Path
    relative_uri: str
    sha256: str
    byte_size: int
    disposition: Literal["CREATED", "REUSED"]
    cache_count: int
    cache_entries_sha256: str
    cache_plan_sha256: str
    input_manifest_sha256: str

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_relative_uri": self.relative_uri,
            "artifact_sha256": self.sha256,
            "byte_size": self.byte_size,
            "cache_count": self.cache_count,
            "cache_entries_sha256": self.cache_entries_sha256,
            "cache_plan_sha256": self.cache_plan_sha256,
            "input_manifest_sha256": self.input_manifest_sha256,
        }


@dataclass(frozen=True, slots=True)
class LoadedCacheManifest:
    artifact: CacheManifestArtifact
    reports: tuple[DailyCacheReport, ...]
    document: dict[str, Any]

    @property
    def path(self) -> Path:
        return self.artifact.path

    @property
    def relative_uri(self) -> str:
        return self.artifact.relative_uri

    @property
    def sha256(self) -> str:
        return self.artifact.sha256

    @property
    def byte_size(self) -> int:
        return self.artifact.byte_size

    @property
    def cache_plan_sha256(self) -> str:
        return self.artifact.cache_plan_sha256

    @property
    def input_manifest_sha256(self) -> str:
        return self.artifact.input_manifest_sha256


@dataclass(frozen=True, slots=True)
class CheckpointArtifact:
    path: Path
    relative_uri: str
    sha256: str
    byte_size: int
    disposition: Literal["CREATED", "REUSED"]
    checkpoint_sequence: int
    last_completed_source_date: date
    progress_metadata: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_relative_uri": self.relative_uri,
            "artifact_sha256": self.sha256,
            "byte_size": self.byte_size,
            "checkpoint_sequence": self.checkpoint_sequence,
            "last_completed_source_date": self.last_completed_source_date.isoformat(),
            "progress_metadata": self.progress_metadata,
            "progress_metadata_sha256": canonical_sha256(self.progress_metadata),
        }


@dataclass(frozen=True, slots=True)
class LoadedCheckpoint:
    artifact: CheckpointArtifact
    document: dict[str, Any]
    replay: SharedReplay
    replay_state: dict[str, object]
    detail_shards: tuple[DetailShardArtifact, ...]
    loaded_detail_shards: tuple[LoadedDetailShard, ...]
    cache_manifest: CacheManifestArtifact
    input_lineage: dict[str, object]
    progress_metadata: dict[str, object]


@dataclass(frozen=True, slots=True)
class FinalResultArtifact:
    path: Path
    relative_uri: str
    sha256: str
    byte_size: int
    disposition: Literal["CREATED", "REUSED"]
    cell_summaries_sha256: str
    summary_row_count: int
    detail_shard_manifest_sha256: str
    detail_record_count: int
    final_checkpoint_sha256: str
    final_checkpoint_sequence: int

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_relative_uri": self.relative_uri,
            "artifact_sha256": self.sha256,
            "byte_size": self.byte_size,
            "cell_summaries_sha256": self.cell_summaries_sha256,
            "detail_record_count": self.detail_record_count,
            "detail_shard_manifest_sha256": self.detail_shard_manifest_sha256,
            "final_checkpoint_sequence": self.final_checkpoint_sequence,
            "final_checkpoint_sha256": self.final_checkpoint_sha256,
            "summary_row_count": self.summary_row_count,
        }


@dataclass(frozen=True, slots=True)
class LoadedFinalResult:
    artifact: FinalResultArtifact
    document: dict[str, Any]
    cell_summaries: tuple[OutcomeCellSummary, ...]
    detail_shards: tuple[DetailShardArtifact, ...]
    cache_manifest: CacheManifestArtifact
    input_lineage: dict[str, object]
    final_checkpoint: LoadedCheckpoint


def _sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise OutcomeArtifactError(f"{label} must be a lowercase SHA-256")
    return value


def _integer(value: object, *, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise OutcomeArtifactError(f"{label} must be an integer >= {minimum}")
    return value


def _text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise OutcomeArtifactError(f"{label} must be a canonical non-empty string")
    return value


def _day(value: object, *, label: str) -> date:
    if isinstance(value, datetime):
        raise OutcomeArtifactError(f"{label} must not be a datetime")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise OutcomeArtifactError(f"{label} must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise OutcomeArtifactError(f"{label} must be an ISO date") from error
    if parsed.isoformat() != value:
        raise OutcomeArtifactError(f"{label} must be a canonical ISO date")
    return parsed


def _canonical_mapping(value: object, *, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise OutcomeArtifactError(f"{label} must be a mapping")
    try:
        payload = canonical_json_bytes(value)
    except TypeError as error:
        raise OutcomeArtifactError(f"{label} contains a float or unsupported value") from error
    decoded = json.loads(payload)
    if not isinstance(decoded, dict):  # pragma: no cover - mapping normalizes to object
        raise OutcomeArtifactError(f"{label} did not remain a mapping")
    return decoded


def _identity(value: os.stat_result) -> _FileIdentity:
    return _FileIdentity(
        device=value.st_dev,
        inode=value.st_ino,
        mode=value.st_mode,
        size=value.st_size,
        modified_ns=value.st_mtime_ns,
        changed_ns=value.st_ctime_ns,
    )


def _data_layout(data_root: Path | str) -> tuple[Path, Path]:
    requested = Path(data_root)
    if not requested.is_absolute():
        requested = Path.cwd() / requested
    lexical = Path(os.path.abspath(os.fspath(requested)))
    try:
        resolved = lexical.resolve(strict=True)
    except OSError as error:
        raise OutcomeArtifactError("data_root does not exist") from error
    try:
        root_mode = lexical.lstat().st_mode
    except OSError as error:  # pragma: no cover - resolve above already proved existence
        raise OutcomeArtifactError("cannot inspect data_root") from error
    if (
        lexical != resolved
        or not stat.S_ISDIR(root_mode)
        or stat.S_ISLNK(root_mode)
        or lexical.name != "data"
    ):
        raise OutcomeArtifactError("data_root must be the real non-symlink data directory")
    derived = lexical / "derived"
    try:
        mode = derived.lstat().st_mode
    except OSError as error:
        raise OutcomeArtifactError("data/derived does not exist") from error
    if not stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
        raise OutcomeArtifactError("data/derived must be a real directory")
    return lexical, derived


def _ensure_directory(derived: Path, relative: Path) -> Path:
    if relative.is_absolute() or ".." in relative.parts:
        raise OutcomeArtifactError("artifact output directory must remain below data/derived")
    current = derived
    for part in relative.parts:
        current /= part
        try:
            current.mkdir(mode=0o755)
        except FileExistsError:
            pass
        try:
            mode = current.lstat().st_mode
        except OSError as error:
            raise OutcomeArtifactError(f"cannot inspect artifact directory: {current}") from error
        if not stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
            raise OutcomeArtifactError(f"artifact directory is not a real directory: {current}")
    return current


def _artifact_path(path: Path, *, data_root: Path | str) -> _ArtifactLocation:
    root, derived = _data_layout(data_root)
    candidate = path if path.is_absolute() else Path.cwd() / path
    candidate = Path(os.path.abspath(os.fspath(candidate)))
    try:
        relative = candidate.relative_to(derived)
    except ValueError as error:
        raise OutcomeArtifactError("artifact is outside data/derived") from error
    if not relative.parts:
        raise OutcomeArtifactError("artifact path must name a file below data/derived")
    try:
        root_identity = _identity(root.lstat())
        derived_identity = _identity(derived.lstat())
    except OSError as error:  # pragma: no cover - _data_layout already inspected both
        raise OutcomeArtifactError("artifact data layout disappeared") from error
    cursor = derived
    identities: list[_FileIdentity] = []
    for index, part in enumerate(relative.parts):
        cursor /= part
        try:
            state = cursor.lstat()
        except OSError as error:
            raise OutcomeArtifactError(f"artifact path is not reachable: {candidate}") from error
        if stat.S_ISLNK(state.st_mode):
            raise OutcomeArtifactError(f"artifact path contains a symlink: {candidate}")
        if index < len(relative.parts) - 1 and not stat.S_ISDIR(state.st_mode):
            raise OutcomeArtifactError(f"artifact parent is not a directory: {candidate}")
        identities.append(_identity(state))
    try:
        if candidate.resolve(strict=True) != candidate:
            raise OutcomeArtifactError("artifact path is not canonical")
    except OSError as error:
        raise OutcomeArtifactError(f"artifact path is not reachable: {candidate}") from error
    return _ArtifactLocation(
        candidate=candidate,
        data_root=root,
        derived=derived,
        relative=relative,
        data_root_identity=root_identity,
        derived_identity=derived_identity,
        component_identities=tuple(identities),
    )


def _same_node(left: _FileIdentity, right: _FileIdentity) -> bool:
    """Compare the stable identity of an opened directory or directory entry."""

    return (
        left.device,
        left.inode,
        stat.S_IFMT(left.mode),
    ) == (
        right.device,
        right.inode,
        stat.S_IFMT(right.mode),
    )


def _directory_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _open_root_directory(path: Path, expected: _FileIdentity) -> int:
    """Open a pathname directory and prove it is the lstat-inspected inode."""

    try:
        before = _identity(path.lstat())
        descriptor = os.open(path, _directory_flags())
    except OSError as error:
        raise OutcomeArtifactError(f"cannot securely open artifact directory: {path}") from error
    try:
        opened = _identity(os.fstat(descriptor))
        after = _identity(path.lstat())
        if (
            not stat.S_ISDIR(opened.mode)
            or stat.S_ISLNK(before.mode)
            or not _same_node(before, expected)
            or not _same_node(opened, expected)
            or not _same_node(after, expected)
        ):
            raise OutcomeArtifactError("artifact directory identity changed while opening")
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _openat_directory(parent_descriptor: int, name: str, expected: _FileIdentity) -> int:
    """Open one child directory relative to a held parent without following links."""

    try:
        before = _identity(os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False))
        descriptor = os.open(
            name,
            _directory_flags(),
            dir_fd=parent_descriptor,
        )
    except OSError as error:
        raise OutcomeArtifactError(
            f"cannot securely open artifact directory component: {name}"
        ) from error
    try:
        opened = _identity(os.fstat(descriptor))
        after = _identity(os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False))
        if (
            not stat.S_ISDIR(opened.mode)
            or stat.S_ISLNK(before.mode)
            or not _same_node(before, expected)
            or not _same_node(opened, expected)
            or not _same_node(after, expected)
        ):
            raise OutcomeArtifactError("artifact directory component changed while opening")
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _open_artifact_descriptor(
    location: _ArtifactLocation,
) -> tuple[int, int, Path]:
    """Open an artifact through held ``openat`` directory descriptors.

    Every directory component must still be the non-symlink inode observed by
    :func:`_artifact_path`; the final regular-file entry is compared before,
    during, and after ``openat(O_NOFOLLOW)``.  The returned parent descriptor
    remains open so callers can later prove that the pathname still names the
    inode whose bytes they hashed.
    """

    current_descriptor = _open_root_directory(
        location.data_root,
        location.data_root_identity,
    )
    current_path = location.data_root
    try:
        child_descriptor = _openat_directory(
            current_descriptor,
            "derived",
            location.derived_identity,
        )
        os.close(current_descriptor)
        current_descriptor = child_descriptor
        current_path = location.derived
        for part, identity in zip(
            location.relative.parts[:-1],
            location.component_identities[:-1],
            strict=True,
        ):
            child_descriptor = _openat_directory(
                current_descriptor,
                part,
                identity,
            )
            os.close(current_descriptor)
            current_descriptor = child_descriptor
            current_path /= part

        filename = location.relative.parts[-1]
        expected_file = location.component_identities[-1]
        try:
            before = _identity(os.stat(filename, dir_fd=current_descriptor, follow_symlinks=False))
            descriptor = os.open(
                filename,
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=current_descriptor,
            )
        except OSError as error:
            raise OutcomeArtifactError(
                f"cannot securely open immutable artifact: {location.candidate}"
            ) from error
        try:
            opened = _identity(os.fstat(descriptor))
            after = _identity(os.stat(filename, dir_fd=current_descriptor, follow_symlinks=False))
            if (
                not stat.S_ISREG(opened.mode)
                or before != expected_file
                or opened != expected_file
                or after != expected_file
            ):
                raise OutcomeArtifactError("artifact file identity changed while opening")
        except Exception:
            os.close(descriptor)
            raise
        return descriptor, current_descriptor, current_path
    except Exception:
        os.close(current_descriptor)
        raise


def _descriptor_sha256(descriptor: int) -> tuple[str, int]:
    digest = hashlib.sha256()
    offset = 0
    while chunk := os.pread(descriptor, 1024 * 1024, offset):
        digest.update(chunk)
        offset += len(chunk)
    return digest.hexdigest(), offset


def _descriptor_bytes(descriptor: int, byte_size: int) -> bytes:
    chunks: list[bytes] = []
    offset = 0
    while offset < byte_size:
        chunk = os.pread(descriptor, min(1024 * 1024, byte_size - offset), offset)
        if not chunk:
            break
        chunks.append(chunk)
        offset += len(chunk)
    if offset != byte_size:
        raise OutcomeArtifactError("artifact became truncated while reading")
    return b"".join(chunks)


def _open_held(
    path: Path,
    *,
    data_root: Path | str,
    expected_sha256: str | None = None,
    expected_byte_size: int | None = None,
    suffix: str | None = None,
) -> _HeldArtifact:
    location = _artifact_path(path, data_root=data_root)
    descriptor, parent_descriptor, parent_path = _open_artifact_descriptor(location)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_mode & _WRITE_BITS:
            raise OutcomeArtifactError("artifact must be a read-only regular file")
        identity = _identity(before)
        digest, byte_size = _descriptor_sha256(descriptor)
        if _identity(os.fstat(descriptor)) != identity or byte_size != identity.size:
            raise OutcomeArtifactError("artifact changed while hashing")
        if expected_sha256 is not None and digest != _sha256(
            expected_sha256, label="expected artifact sha256"
        ):
            raise OutcomeArtifactError("artifact SHA-256 drift")
        if expected_byte_size is not None and byte_size != _integer(
            expected_byte_size, label="expected artifact byte_size", minimum=1
        ):
            raise OutcomeArtifactError("artifact byte-size drift")
        if suffix is not None and location.candidate.name != f"sha256={digest}{suffix}":
            raise OutcomeArtifactError("artifact filename differs from its content identity")
        held = _HeldArtifact(
            path=location.candidate,
            descriptor=descriptor,
            parent_path=parent_path,
            parent_descriptor=parent_descriptor,
            parent_identity=_identity(os.fstat(parent_descriptor)),
            filename=location.candidate.name,
            relative_uri=location.relative_uri,
            identity=identity,
            sha256=digest,
            byte_size=byte_size,
        )
        _verify_held(held)
        return held
    except Exception:
        os.close(descriptor)
        os.close(parent_descriptor)
        raise


def _verify_held(held: _HeldArtifact) -> None:
    if held.descriptor < 0 or _identity(os.fstat(held.descriptor)) != held.identity:
        raise OutcomeArtifactError("open artifact inode changed")
    if held.parent_descriptor < 0 or not _same_node(
        _identity(os.fstat(held.parent_descriptor)),
        held.parent_identity,
    ):
        raise OutcomeArtifactError("open artifact parent directory changed")
    try:
        entry_identity = _identity(
            os.stat(
                held.filename,
                dir_fd=held.parent_descriptor,
                follow_symlinks=False,
            )
        )
        parent_path_identity = _identity(held.parent_path.lstat())
        path_identity = _identity(held.path.lstat())
    except OSError as error:
        raise OutcomeArtifactError("artifact path disappeared") from error
    if (
        entry_identity != held.identity
        or path_identity != held.identity
        or not _same_node(parent_path_identity, held.parent_identity)
    ):
        raise OutcomeArtifactError("artifact path identity changed")


def _publish_temporary(
    temporary: Path,
    *,
    data_root: Path | str,
    output_relative: Path,
    suffix: str,
) -> tuple[Path, str, int, Literal["CREATED", "REUSED"], str]:
    _, derived = _data_layout(data_root)
    output = _ensure_directory(derived, output_relative)
    with temporary.open("rb") as handle:
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o444)
    temporary_held = _open_held(temporary, data_root=data_root)
    try:
        if temporary_held.parent_path != output:
            raise OutcomeArtifactError("temporary artifact parent directory drift")
        digest = temporary_held.sha256
        byte_size = temporary_held.byte_size
        target = output / f"sha256={digest}{suffix}"
        disposition: Literal["CREATED", "REUSED"] = "CREATED"
        try:
            os.link(
                temporary_held.filename,
                target.name,
                src_dir_fd=temporary_held.parent_descriptor,
                dst_dir_fd=temporary_held.parent_descriptor,
                follow_symlinks=False,
            )
        except FileExistsError:
            disposition = "REUSED"
        os.fsync(temporary_held.parent_descriptor)
    finally:
        temporary_held.close()
    # Resolve the published pathname afresh after the descriptor-relative link.
    # If any directory component was swapped, publication fails closed instead
    # of returning lineage for an entry reached through a different directory.
    published = _open_held(
        target,
        data_root=data_root,
        expected_sha256=digest,
        expected_byte_size=byte_size,
        suffix=suffix,
    )
    try:
        relative_uri = published.relative_uri
    finally:
        published.close()
    return target, digest, byte_size, disposition, relative_uri


def _publish_json(
    document: Mapping[str, object],
    *,
    data_root: Path | str,
    output_relative: Path,
) -> tuple[Path, str, int, Literal["CREATED", "REUSED"], str]:
    normalized = _canonical_mapping(document, label="artifact document")
    payload = canonical_json_bytes(normalized) + b"\n"
    _, derived = _data_layout(data_root)
    output = _ensure_directory(derived, output_relative)
    descriptor, name = tempfile.mkstemp(dir=output, prefix=".publish.", suffix=".json.tmp")
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        return _publish_temporary(
            temporary,
            data_root=data_root,
            output_relative=output_relative,
            suffix=".json",
        )
    finally:
        temporary.unlink(missing_ok=True)


def _read_json(
    path: Path,
    *,
    data_root: Path | str,
    expected_sha256: str | None = None,
    expected_byte_size: int | None = None,
) -> tuple[_HeldArtifact, dict[str, Any]]:
    held = _open_held(
        path,
        data_root=data_root,
        expected_sha256=expected_sha256,
        expected_byte_size=expected_byte_size,
        suffix=".json",
    )
    try:
        content = _descriptor_bytes(held.descriptor, held.byte_size)
        try:
            document = json.loads(content)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise OutcomeArtifactError("artifact is not valid UTF-8 JSON") from error
        try:
            canonical = canonical_json_bytes(document) + b"\n"
        except TypeError as error:
            raise OutcomeArtifactError(
                "artifact contains a float or unsupported JSON value"
            ) from error
        if not isinstance(document, dict) or canonical != content:
            raise OutcomeArtifactError("artifact is not canonical JSON plus newline")
        _verify_held(held)
        return held, document
    except Exception:
        held.close()
        raise


def _record_sort_key(record: ReplayResultRecord) -> tuple[object, ...]:
    return (
        record.decision_ts_recv_ns,
        record.signal_id,
        _SCENARIO_RANK.get(record.scenario_id, len(_SCENARIO_RANK)),
        _DIRECTION_RANK.get(record.direction.value, len(_DIRECTION_RANK)),
        record.contract_key,
        record.take_profit_ticks,
        record.stop_loss_ticks,
    )


def _validated_record(value: object) -> ReplayResultRecord:
    if not isinstance(value, ReplayResultRecord):
        raise OutcomeArtifactError("detail shard values must be ReplayResultRecord objects")
    document = value.as_dict()
    if tuple(document) != _DETAIL_FIELDS:
        raise OutcomeArtifactError("ReplayResultRecord field schema drift")
    try:
        parsed = SharedReplay.record_from_dict(document)
    except SharedReplayError as error:
        raise OutcomeArtifactError("ReplayResultRecord is not losslessly serializable") from error
    if parsed.as_dict() != document:
        raise OutcomeArtifactError("ReplayResultRecord round-trip drift")
    if value.scenario_id not in SCENARIO_IDS or value.direction.value not in DIRECTION_IDS:
        raise OutcomeArtifactError("ReplayResultRecord scenario/direction drift")
    if value.take_profit_ticks not in BARRIER_TICKS or value.stop_loss_ticks not in BARRIER_TICKS:
        raise OutcomeArtifactError("ReplayResultRecord is outside the frozen barrier grid")
    if value.cell_id != f"tp{value.take_profit_ticks}_sl{value.stop_loss_ticks}":
        raise OutcomeArtifactError("ReplayResultRecord cell_id differs from its barriers")
    return parsed


def _parquet_record(value: ReplayResultRecord) -> dict[str, object]:
    document = value.as_dict()
    for name in _REFERENCE_FIELDS:
        reference = document[name]
        if reference is not None:
            assert isinstance(reference, dict)
            reference = dict(reference)
            reference["source_date"] = date.fromisoformat(str(reference["source_date"]))
            document[name] = reference
    return document


def _mapping_record(value: Mapping[str, object]) -> dict[str, object]:
    document = dict(value)
    for name in _REFERENCE_FIELDS:
        reference = document.get(name)
        if reference is not None:
            if not isinstance(reference, dict):
                raise OutcomeArtifactError("Parquet event reference is not a struct")
            reference = dict(reference)
            source_date = reference.get("source_date")
            if not isinstance(source_date, date) or isinstance(source_date, datetime):
                raise OutcomeArtifactError("Parquet event reference date drift")
            reference["source_date"] = source_date.isoformat()
            document[name] = reference
    return document


def publish_detail_shard(
    records: Sequence[ReplayResultRecord],
    *,
    data_root: Path | str,
    run_fingerprint: str,
    shard_sequence: int,
    source_date: date,
) -> DetailShardArtifact:
    """Publish one deterministic daily result shard, including empty days."""

    if isinstance(records, (str, bytes)) or not isinstance(records, Sequence):
        raise OutcomeArtifactError("records must be a sequence")
    fingerprint = _sha256(run_fingerprint, label="run_fingerprint")
    sequence = _integer(shard_sequence, label="shard_sequence", minimum=1)
    day = _day(source_date, label="source_date")
    ordered = tuple(sorted((_validated_record(value) for value in records), key=_record_sort_key))
    identities = [
        (record.signal_id, record.scenario_id, record.take_profit_ticks, record.stop_loss_ticks)
        for record in ordered
    ]
    if len(identities) != len(set(identities)):
        raise OutcomeArtifactError("detail shard contains duplicate result identities")
    documents = [record.as_dict() for record in ordered]
    record_manifest_sha256 = canonical_sha256(documents)
    metadata = {
        "artifact_schema": DETAIL_SHARD_SCHEMA,
        "artifact_version": DETAIL_SHARD_VERSION,
        "record_manifest_sha256": record_manifest_sha256,
        "row_count": len(ordered),
        "run_fingerprint": fingerprint,
        "shard_sequence": sequence,
        "source_date": day.isoformat(),
    }
    schema = _DETAIL_SCHEMA.with_metadata(
        {b"systematic_fx.outcome_detail": canonical_json_bytes(metadata)}
    )
    table = pa.Table.from_pylist([_parquet_record(record) for record in ordered], schema=schema)
    _, derived = _data_layout(data_root)
    output = _ensure_directory(derived, _DETAIL_DIRECTORY)
    descriptor, name = tempfile.mkstemp(dir=output, prefix=".detail.", suffix=".parquet.tmp")
    os.close(descriptor)
    temporary = Path(name)
    try:
        pq.write_table(
            table,
            temporary,
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
            version="2.6",
            data_page_version="1.0",
            row_group_size=65_536,
        )
        path, digest, byte_size, disposition, relative_uri = _publish_temporary(
            temporary,
            data_root=data_root,
            output_relative=_DETAIL_DIRECTORY,
            suffix=".parquet",
        )
    finally:
        temporary.unlink(missing_ok=True)
    return DetailShardArtifact(
        path=path,
        relative_uri=relative_uri,
        sha256=digest,
        byte_size=byte_size,
        disposition=disposition,
        shard_sequence=sequence,
        source_date=day,
        row_count=len(ordered),
        record_manifest_sha256=record_manifest_sha256,
        run_fingerprint=fingerprint,
    )


def load_detail_shard(
    artifact: DetailShardArtifact,
    *,
    data_root: Path | str,
) -> LoadedDetailShard:
    """Hash, schema, metadata, ordering, and lossless-record validate one shard."""

    if not isinstance(artifact, DetailShardArtifact):
        raise OutcomeArtifactError("artifact must be a DetailShardArtifact")
    held = _open_held(
        artifact.path,
        data_root=data_root,
        expected_sha256=artifact.sha256,
        expected_byte_size=artifact.byte_size,
        suffix=".parquet",
    )
    try:
        observed_relative_uri = _artifact_path(
            held.path,
            data_root=data_root,
        ).relative_uri
        if observed_relative_uri != artifact.relative_uri:
            raise OutcomeArtifactError("detail shard relative URI drift")
        with os.fdopen(os.dup(held.descriptor), "rb") as handle:
            parquet = pq.ParquetFile(handle)
            if parquet.schema_arrow.remove_metadata() != _DETAIL_SCHEMA:
                raise OutcomeArtifactError("detail shard Arrow schema drift")
            raw_metadata = (parquet.schema_arrow.metadata or {}).get(
                b"systematic_fx.outcome_detail"
            )
            if raw_metadata is None:
                raise OutcomeArtifactError("detail shard metadata is missing")
            try:
                metadata = json.loads(raw_metadata)
            except (TypeError, json.JSONDecodeError) as error:
                raise OutcomeArtifactError("detail shard metadata is invalid") from error
            if canonical_json_bytes(metadata) != raw_metadata:
                raise OutcomeArtifactError("detail shard metadata is not canonical JSON")
            expected_metadata = {
                "artifact_schema": DETAIL_SHARD_SCHEMA,
                "artifact_version": DETAIL_SHARD_VERSION,
                "record_manifest_sha256": artifact.record_manifest_sha256,
                "row_count": artifact.row_count,
                "run_fingerprint": artifact.run_fingerprint,
                "shard_sequence": artifact.shard_sequence,
                "source_date": artifact.source_date.isoformat(),
            }
            _sha256(artifact.run_fingerprint, label="detail run_fingerprint")
            if metadata != expected_metadata:
                raise OutcomeArtifactError("detail shard metadata drift")
            rows: list[dict[str, object]] = []
            for row_group_index in range(parquet.metadata.num_row_groups):
                table = parquet.read_row_group(row_group_index, use_threads=False)
                rows.extend(_mapping_record(value) for value in table.to_pylist())
        if len(rows) != artifact.row_count:
            raise OutcomeArtifactError("detail shard row count drift")
        records: list[ReplayResultRecord] = []
        for document in rows:
            if tuple(document) != _DETAIL_FIELDS:
                raise OutcomeArtifactError("detail shard field order/schema drift")
            try:
                record = SharedReplay.record_from_dict(document)
            except SharedReplayError as error:
                raise OutcomeArtifactError("detail shard record is invalid") from error
            records.append(_validated_record(record))
        if records != sorted(records, key=_record_sort_key):
            raise OutcomeArtifactError("detail shard records are not canonically ordered")
        identities = {
            (record.signal_id, record.scenario_id, record.take_profit_ticks, record.stop_loss_ticks)
            for record in records
        }
        if len(identities) != len(records):
            raise OutcomeArtifactError("detail shard contains duplicate result identities")
        if (
            canonical_sha256([record.as_dict() for record in records])
            != artifact.record_manifest_sha256
        ):
            raise OutcomeArtifactError("detail shard semantic manifest drift")
        _verify_held(held)
        return LoadedDetailShard(artifact=artifact, records=tuple(records))
    finally:
        held.close()


def _data_relative(path: Path | str, *, data_root: Path, label: str) -> str:
    requested = Path(path)
    if not requested.is_absolute():
        requested = Path.cwd() / requested
    candidate = Path(os.path.abspath(os.fspath(requested)))
    try:
        resolved = candidate.resolve(strict=True)
        relative = resolved.relative_to(data_root)
    except (OSError, ValueError) as error:
        raise OutcomeArtifactError(f"{label} must be an existing data-relative path") from error
    if candidate != resolved:
        raise OutcomeArtifactError(f"{label} must be canonical and non-symlinked")
    return relative.as_posix()


def _cache_entry(report: DailyCacheReport, *, data_root: Path) -> dict[str, object]:
    if not isinstance(report, DailyCacheReport):
        raise OutcomeArtifactError("cache reports must contain DailyCacheReport values")
    held = _open_held(
        report.path,
        data_root=data_root,
        expected_sha256=report.sha256,
        expected_byte_size=report.byte_size,
        suffix=".parquet",
    )
    held.close()
    source_relative_uri = _data_relative(
        report.source_path,
        data_root=data_root,
        label="cache raw source path",
    )
    for field_name in (
        "instrument_id",
        "event_index_offset",
        "source_row_count",
        "cached_quote_count",
        "valid_quote_count",
        "first_event_index",
        "last_event_index",
        "first_ts_recv_ns",
        "last_ts_recv_ns",
    ):
        _integer(getattr(report, field_name), label=f"cache {field_name}")
    if report.cached_quote_count <= 0:
        raise OutcomeArtifactError("cache report must contain at least one quote")
    if report.valid_quote_count > report.cached_quote_count:
        raise OutcomeArtifactError("cache valid_quote_count exceeds cached_quote_count")
    if report.cached_quote_count > report.source_row_count:
        raise OutcomeArtifactError("cache quote count exceeds source row count")
    if report.first_event_index > report.last_event_index:
        raise OutcomeArtifactError("cache event bounds are reversed")
    if report.first_ts_recv_ns > report.last_ts_recv_ns:
        raise OutcomeArtifactError("cache receive-time bounds are reversed")
    if (report.last_valid_event_index is None) != (report.last_valid_ts_recv_ns is None):
        raise OutcomeArtifactError("cache last-valid event/time bounds disagree")
    if (report.valid_quote_count == 0) != (report.last_valid_event_index is None):
        raise OutcomeArtifactError("cache valid count and last-valid bounds disagree")
    if not (
        report.event_index_offset
        <= report.first_event_index
        <= report.last_event_index
        < report.event_index_offset + report.source_row_count
    ):
        raise OutcomeArtifactError("cache event bounds escape source-row lineage")
    if report.last_valid_event_index is not None:
        if not report.first_event_index <= report.last_valid_event_index <= report.last_event_index:
            raise OutcomeArtifactError("cache last-valid event lies outside event bounds")
        assert report.last_valid_ts_recv_ns is not None
        if not report.first_ts_recv_ns <= report.last_valid_ts_recv_ns <= report.last_ts_recv_ns:
            raise OutcomeArtifactError("cache last-valid time lies outside time bounds")
    cache_relative_uri = _artifact_path(
        report.path,
        data_root=data_root,
    ).relative_uri
    return {
        "artifact_relative_uri": cache_relative_uri,
        "artifact_sha256": _sha256(report.sha256, label="cache artifact sha256"),
        "byte_size": _integer(report.byte_size, label="cache byte_size", minimum=1),
        "cached_quote_count": report.cached_quote_count,
        "event_index_offset": report.event_index_offset,
        "first_event_index": report.first_event_index,
        "first_ts_recv_ns": report.first_ts_recv_ns,
        "instrument_id": report.instrument_id,
        "last_event_index": report.last_event_index,
        "last_ts_recv_ns": report.last_ts_recv_ns,
        "last_valid_event_index": report.last_valid_event_index,
        "last_valid_ts_recv_ns": report.last_valid_ts_recv_ns,
        "raw_symbol": _text(report.raw_symbol, label="cache raw_symbol"),
        "source_date": _day(report.source_date, label="cache source_date").isoformat(),
        "source_relative_uri": source_relative_uri,
        "source_row_count": report.source_row_count,
        "source_sha256": _sha256(report.source_sha256, label="cache source sha256"),
        "valid_quote_count": report.valid_quote_count,
    }


def _report_from_cache_entry(
    value: object,
    *,
    data_root: Path,
) -> DailyCacheReport:
    expected_fields = {
        "artifact_relative_uri",
        "artifact_sha256",
        "byte_size",
        "cached_quote_count",
        "event_index_offset",
        "first_event_index",
        "first_ts_recv_ns",
        "instrument_id",
        "last_event_index",
        "last_ts_recv_ns",
        "last_valid_event_index",
        "last_valid_ts_recv_ns",
        "raw_symbol",
        "source_date",
        "source_relative_uri",
        "source_row_count",
        "source_sha256",
        "valid_quote_count",
    }
    if not isinstance(value, dict) or set(value) != expected_fields:
        raise OutcomeArtifactError("cache manifest entry schema drift")
    artifact_relative = _text(
        value.get("artifact_relative_uri"), label="cache artifact_relative_uri"
    )
    source_relative = _text(value.get("source_relative_uri"), label="cache source_relative_uri")
    artifact_path = data_root / "derived" / artifact_relative
    source_path = data_root / source_relative
    report = DailyCacheReport(
        path=artifact_path,
        sha256=_sha256(value.get("artifact_sha256"), label="cache artifact_sha256"),
        byte_size=_integer(value.get("byte_size"), label="cache byte_size", minimum=1),
        disposition="REUSED",
        source_date=_day(value.get("source_date"), label="cache source_date"),
        source_path=str(source_path),
        source_sha256=_sha256(value.get("source_sha256"), label="cache source_sha256"),
        raw_symbol=_text(value.get("raw_symbol"), label="cache raw_symbol"),
        instrument_id=_integer(value.get("instrument_id"), label="cache instrument_id"),
        event_index_offset=_integer(
            value.get("event_index_offset"), label="cache event_index_offset"
        ),
        source_row_count=_integer(value.get("source_row_count"), label="cache source_row_count"),
        cached_quote_count=_integer(
            value.get("cached_quote_count"), label="cache cached_quote_count", minimum=1
        ),
        valid_quote_count=_integer(value.get("valid_quote_count"), label="cache valid_quote_count"),
        first_event_index=_integer(value.get("first_event_index"), label="cache first_event_index"),
        last_event_index=_integer(value.get("last_event_index"), label="cache last_event_index"),
        first_ts_recv_ns=_integer(value.get("first_ts_recv_ns"), label="cache first_ts_recv_ns"),
        last_ts_recv_ns=_integer(value.get("last_ts_recv_ns"), label="cache last_ts_recv_ns"),
        last_valid_event_index=(
            None
            if value.get("last_valid_event_index") is None
            else _integer(
                value.get("last_valid_event_index"),
                label="cache last_valid_event_index",
            )
        ),
        last_valid_ts_recv_ns=(
            None
            if value.get("last_valid_ts_recv_ns") is None
            else _integer(
                value.get("last_valid_ts_recv_ns"),
                label="cache last_valid_ts_recv_ns",
            )
        ),
    )
    expected = _cache_entry(report, data_root=data_root)
    if expected != value:
        raise OutcomeArtifactError("cache manifest entry semantic drift")
    return report


def publish_cache_manifest(
    reports: Sequence[DailyCacheReport],
    *,
    data_root: Path | str,
    cache_plan_sha256: str,
    input_manifest_sha256: str,
) -> CacheManifestArtifact:
    """Publish the run-independent complete daily executable-cache lineage."""

    if isinstance(reports, (str, bytes)) or not isinstance(reports, Sequence) or not reports:
        raise OutcomeArtifactError("cache reports must be a non-empty sequence")
    root, _ = _data_layout(data_root)
    ordered = tuple(reports)
    keys = tuple((report.source_date, report.raw_symbol) for report in ordered)
    if keys != tuple(sorted(set(keys))):
        raise OutcomeArtifactError("cache reports must have unique sorted date/contract keys")
    entries = [_cache_entry(report, data_root=root) for report in ordered]
    entries_sha256 = canonical_sha256(entries)
    plan_sha256 = _sha256(cache_plan_sha256, label="cache_plan_sha256")
    input_sha256 = _sha256(input_manifest_sha256, label="input_manifest_sha256")
    document = {
        "artifact_schema": CACHE_MANIFEST_SCHEMA,
        "artifact_version": CACHE_MANIFEST_VERSION,
        "cache_count": len(entries),
        "cache_entries": entries,
        "cache_entries_sha256": entries_sha256,
        "cache_plan_sha256": plan_sha256,
        "cache_schema": CACHE_SCHEMA,
        "cache_version": CACHE_VERSION,
        "input_manifest_sha256": input_sha256,
        "partition_key": ["source_date", "raw_symbol"],
    }
    path, digest, byte_size, disposition, relative_uri = _publish_json(
        document,
        data_root=data_root,
        output_relative=_CACHE_MANIFEST_DIRECTORY,
    )
    return CacheManifestArtifact(
        path=path,
        relative_uri=relative_uri,
        sha256=digest,
        byte_size=byte_size,
        disposition=disposition,
        cache_count=len(entries),
        cache_entries_sha256=entries_sha256,
        cache_plan_sha256=plan_sha256,
        input_manifest_sha256=input_sha256,
    )


def load_cache_manifest(
    artifact: CacheManifestArtifact | Path,
    *,
    data_root: Path | str,
    expected_sha256: str | None = None,
    expected_byte_size: int | None = None,
    verify_cache_content: bool = True,
) -> LoadedCacheManifest:
    """Load a manifest and optionally stream-validate every referenced cache."""

    if isinstance(artifact, CacheManifestArtifact):
        path = artifact.path
        expected_sha256 = artifact.sha256
        expected_byte_size = artifact.byte_size
    elif isinstance(artifact, Path):
        path = artifact
        if expected_sha256 is None or expected_byte_size is None:
            raise OutcomeArtifactError("Path cache loads require expected SHA-256 and byte size")
    else:
        raise OutcomeArtifactError("cache artifact must be CacheManifestArtifact or Path")
    held, document = _read_json(
        path,
        data_root=data_root,
        expected_sha256=expected_sha256,
        expected_byte_size=expected_byte_size,
    )
    try:
        if set(document) != _CACHE_MANIFEST_FIELDS:
            raise OutcomeArtifactError("cache manifest top-level schema drift")
        if (
            document.get("artifact_schema") != CACHE_MANIFEST_SCHEMA
            or document.get("artifact_version") != CACHE_MANIFEST_VERSION
        ):
            raise OutcomeArtifactError("cache manifest schema/version drift")
        if (
            document.get("cache_schema") != CACHE_SCHEMA
            or document.get("cache_version") != CACHE_VERSION
        ):
            raise OutcomeArtifactError("cache artifact schema/version lineage drift")
        if document.get("partition_key") != ["source_date", "raw_symbol"]:
            raise OutcomeArtifactError("cache partition key drift")
        entries = document.get("cache_entries")
        count = _integer(document.get("cache_count"), label="cache_count", minimum=1)
        if not isinstance(entries, list) or len(entries) != count:
            raise OutcomeArtifactError("cache manifest entry count drift")
        entries_sha256 = _sha256(document.get("cache_entries_sha256"), label="cache_entries_sha256")
        if canonical_sha256(entries) != entries_sha256:
            raise OutcomeArtifactError("cache entries semantic hash drift")
        plan_sha256 = _sha256(document.get("cache_plan_sha256"), label="cache_plan_sha256")
        input_sha256 = _sha256(document.get("input_manifest_sha256"), label="input_manifest_sha256")
        root, derived = _data_layout(data_root)
        reports = tuple(_report_from_cache_entry(entry, data_root=root) for entry in entries)
        keys = tuple((report.source_date, report.raw_symbol) for report in reports)
        if keys != tuple(sorted(set(keys))):
            raise OutcomeArtifactError("cache manifest keys are duplicated or unordered")
        if verify_cache_content:
            try:
                for report in reports:
                    for _ in read_daily_executable_cache(report):
                        pass
            except EventCacheError as error:
                raise OutcomeArtifactError("referenced cache content failed validation") from error
        relative_uri = held.path.relative_to(derived).as_posix()
        result_artifact = CacheManifestArtifact(
            path=held.path,
            relative_uri=relative_uri,
            sha256=held.sha256,
            byte_size=held.byte_size,
            disposition="REUSED",
            cache_count=count,
            cache_entries_sha256=entries_sha256,
            cache_plan_sha256=plan_sha256,
            input_manifest_sha256=input_sha256,
        )
        _verify_held(held)
        return LoadedCacheManifest(result_artifact, reports, document)
    finally:
        held.close()


def find_cache_manifest(
    *,
    data_root: Path | str,
    cache_plan_sha256: str,
    input_manifest_sha256: str,
    verify_cache_content: bool = True,
) -> LoadedCacheManifest | None:
    """Resolve one exact reusable manifest before any raw cache rebuild."""

    plan_sha256 = _sha256(cache_plan_sha256, label="cache_plan_sha256")
    input_sha256 = _sha256(input_manifest_sha256, label="input_manifest_sha256")
    _, derived = _data_layout(data_root)
    directory = derived / _CACHE_MANIFEST_DIRECTORY
    if not directory.exists():
        return None
    if directory.is_symlink() or not directory.is_dir():
        raise OutcomeArtifactError("cache manifest directory is not a real directory")
    matches: list[LoadedCacheManifest] = []
    for path in sorted(directory.glob("sha256=*.json")):
        held, document = _read_json(path, data_root=data_root)
        try:
            if document.get("artifact_schema") != CACHE_MANIFEST_SCHEMA:
                raise OutcomeArtifactError("cache manifest directory contains unknown JSON")
            if (
                document.get("cache_plan_sha256") == plan_sha256
                and document.get("input_manifest_sha256") == input_sha256
            ):
                matches.append(
                    load_cache_manifest(
                        path,
                        data_root=data_root,
                        expected_sha256=held.sha256,
                        expected_byte_size=held.byte_size,
                        verify_cache_content=verify_cache_content,
                    )
                )
        finally:
            held.close()
    if len(matches) > 1:
        identities = {(item.sha256, item.byte_size) for item in matches}
        if len(identities) > 1:
            raise OutcomeArtifactError("multiple cache manifests claim one semantic plan")
        return matches[0]
    return None if not matches else matches[0]


def _quick_artifact_check(
    *,
    path: Path,
    sha256: str,
    byte_size: int,
    suffix: str,
    data_root: Path | str,
) -> str:
    held = _open_held(
        path,
        data_root=data_root,
        expected_sha256=_sha256(sha256, label="artifact sha256"),
        expected_byte_size=_integer(byte_size, label="artifact byte_size", minimum=1),
        suffix=suffix,
    )
    try:
        # `_open_held` streams the complete artifact through SHA-256 using
        # constant memory, then binds those bytes to both the held file
        # descriptor and its held parent-directory entry.  This intentionally
        # remains a semantic-light check, but it is never a content-light one.
        _verify_held(held)
        return held.relative_uri
    finally:
        held.close()


def _cache_artifact(value: CacheManifestArtifact | LoadedCacheManifest) -> CacheManifestArtifact:
    if isinstance(value, LoadedCacheManifest):
        return value.artifact
    if isinstance(value, CacheManifestArtifact):
        return value
    raise OutcomeArtifactError("cache_manifest must be a published or loaded cache manifest")


def _checkpoint_artifact(
    value: CheckpointArtifact | LoadedCheckpoint,
) -> CheckpointArtifact:
    if isinstance(value, LoadedCheckpoint):
        return value.artifact
    if isinstance(value, CheckpointArtifact):
        return value
    raise OutcomeArtifactError("final_checkpoint must be a published or loaded checkpoint")


def _checkpoint_artifact_from_mapping(
    value: object,
    *,
    data_root: Path,
) -> CheckpointArtifact:
    expected = {
        "artifact_relative_uri",
        "artifact_sha256",
        "byte_size",
        "checkpoint_sequence",
        "last_completed_source_date",
        "progress_metadata",
        "progress_metadata_sha256",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise OutcomeArtifactError("final checkpoint lineage schema drift")
    relative_uri = _text(value.get("artifact_relative_uri"), label="final checkpoint relative URI")
    progress = _canonical_mapping(
        value.get("progress_metadata"), label="final checkpoint progress metadata"
    )
    if canonical_sha256(progress) != _sha256(
        value.get("progress_metadata_sha256"),
        label="final checkpoint progress metadata sha256",
    ):
        raise OutcomeArtifactError("final checkpoint progress metadata hash drift")
    return CheckpointArtifact(
        path=data_root / "derived" / relative_uri,
        relative_uri=relative_uri,
        sha256=_sha256(value.get("artifact_sha256"), label="final checkpoint sha256"),
        byte_size=_integer(value.get("byte_size"), label="final checkpoint byte_size", minimum=1),
        disposition="REUSED",
        checkpoint_sequence=_integer(
            value.get("checkpoint_sequence"),
            label="final checkpoint sequence",
            minimum=1,
        ),
        last_completed_source_date=_day(
            value.get("last_completed_source_date"),
            label="final checkpoint last completed source date",
        ),
        progress_metadata=progress,
    )


def _cache_artifact_from_mapping(
    value: object,
    *,
    data_root: Path,
) -> CacheManifestArtifact:
    expected = {
        "artifact_relative_uri",
        "artifact_sha256",
        "byte_size",
        "cache_count",
        "cache_entries_sha256",
        "cache_plan_sha256",
        "input_manifest_sha256",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise OutcomeArtifactError("cache manifest lineage schema drift")
    relative_uri = _text(value.get("artifact_relative_uri"), label="cache manifest relative URI")
    return CacheManifestArtifact(
        path=data_root / "derived" / relative_uri,
        relative_uri=relative_uri,
        sha256=_sha256(value.get("artifact_sha256"), label="cache manifest sha256"),
        byte_size=_integer(value.get("byte_size"), label="cache manifest byte_size", minimum=1),
        disposition="REUSED",
        cache_count=_integer(value.get("cache_count"), label="cache_count", minimum=1),
        cache_entries_sha256=_sha256(
            value.get("cache_entries_sha256"), label="cache_entries_sha256"
        ),
        cache_plan_sha256=_sha256(value.get("cache_plan_sha256"), label="cache_plan_sha256"),
        input_manifest_sha256=_sha256(
            value.get("input_manifest_sha256"), label="input_manifest_sha256"
        ),
    )


def _shard_from_mapping(value: object, *, data_root: Path) -> DetailShardArtifact:
    expected = {
        "artifact_relative_uri",
        "artifact_sha256",
        "byte_size",
        "record_manifest_sha256",
        "row_count",
        "run_fingerprint",
        "shard_sequence",
        "source_date",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise OutcomeArtifactError("detail shard lineage schema drift")
    relative_uri = _text(value.get("artifact_relative_uri"), label="shard relative URI")
    return DetailShardArtifact(
        path=data_root / "derived" / relative_uri,
        relative_uri=relative_uri,
        sha256=_sha256(value.get("artifact_sha256"), label="shard sha256"),
        byte_size=_integer(value.get("byte_size"), label="shard byte_size", minimum=1),
        disposition="REUSED",
        shard_sequence=_integer(value.get("shard_sequence"), label="shard_sequence", minimum=1),
        source_date=_day(value.get("source_date"), label="shard source_date"),
        row_count=_integer(value.get("row_count"), label="shard row_count"),
        record_manifest_sha256=_sha256(
            value.get("record_manifest_sha256"), label="record_manifest_sha256"
        ),
        run_fingerprint=_sha256(value.get("run_fingerprint"), label="shard run_fingerprint"),
    )


def _validated_shards(
    shards: Sequence[DetailShardArtifact],
    *,
    run_fingerprint: str,
    data_root: Path | str,
) -> tuple[DetailShardArtifact, ...]:
    if isinstance(shards, (str, bytes)) or not isinstance(shards, Sequence) or not shards:
        raise OutcomeArtifactError("detail_shards must be a non-empty cumulative sequence")
    fingerprint = _sha256(run_fingerprint, label="run_fingerprint")
    ordered = tuple(shards)
    prior_date: date | None = None
    identities: set[str] = set()
    for expected_sequence, shard in enumerate(ordered, start=1):
        if not isinstance(shard, DetailShardArtifact):
            raise OutcomeArtifactError("detail_shards must contain DetailShardArtifact values")
        if shard.shard_sequence != expected_sequence:
            raise OutcomeArtifactError("detail shard sequence must be contiguous from one")
        if shard.run_fingerprint != fingerprint:
            raise OutcomeArtifactError("detail shard run fingerprint drift")
        if prior_date is not None and shard.source_date <= prior_date:
            raise OutcomeArtifactError("detail shard source dates must be strictly increasing")
        relative_uri = _quick_artifact_check(
            path=shard.path,
            sha256=shard.sha256,
            byte_size=shard.byte_size,
            suffix=".parquet",
            data_root=data_root,
        )
        if relative_uri != shard.relative_uri:
            raise OutcomeArtifactError("detail shard relative URI drift")
        if shard.sha256 in identities:
            raise OutcomeArtifactError("detail shard lineage repeats an artifact")
        identities.add(shard.sha256)
        prior_date = shard.source_date
    return ordered


def _progress_metadata(
    *,
    replay_state_sha256: str,
    detail_shard_manifest_sha256: str,
    detail_shard_count: int,
    detail_record_count: int,
    cache_manifest_sha256: str,
    input_lineage_sha256: str,
    source_event_count: int,
    replay_finished: bool,
) -> dict[str, object]:
    return {
        "artifact_schema": CHECKPOINT_PROGRESS_SCHEMA,
        "cache_manifest_sha256": cache_manifest_sha256,
        "detail_record_count": detail_record_count,
        "detail_shard_count": detail_shard_count,
        "detail_shard_manifest_sha256": detail_shard_manifest_sha256,
        "input_lineage_sha256": input_lineage_sha256,
        "replay_state_sha256": replay_state_sha256,
        "replay_finished": replay_finished,
        "source_event_count": source_event_count,
    }


def _validate_replay_boundary(
    replay_state: Mapping[str, object],
    *,
    last_completed_source_date: date,
    source_event_count: int,
    detail_record_count: int,
    expected_finished: bool | None = None,
) -> tuple[dict[str, object], SharedReplay, bool]:
    state = _canonical_mapping(replay_state, label="replay_state")
    if state.get("completed_source_date") != last_completed_source_date.isoformat():
        raise OutcomeArtifactError("replay state completed source-date drift")
    if state.get("source_event_count") != source_event_count:
        raise OutcomeArtifactError("replay state source event count drift")
    finished = state.get("finished")
    if not isinstance(finished, bool):
        raise OutcomeArtifactError("replay state finished flag must be boolean")
    if expected_finished is not None and finished is not expected_finished:
        raise OutcomeArtifactError("replay state finished flag drift")
    if state.get("records") != []:
        raise OutcomeArtifactError("replay state must be drained before artifact publication")
    if state.get("drained_record_count") != detail_record_count:
        raise OutcomeArtifactError("replay drained count differs from detail shard lineage")
    try:
        replay = SharedReplay.from_checkpoint(state)
    except SharedReplayError as error:
        raise OutcomeArtifactError("replay checkpoint state is not resumable") from error
    if replay.finished is not finished:
        raise OutcomeArtifactError("restored replay finished flag drift")
    if finished and (
        replay.active_position_count != 0
        or replay.active_owner_group_count != 0
        or replay.pending_entry_count != 0
    ):
        raise OutcomeArtifactError("finished replay retains live positions or entries")
    return state, replay, finished


def publish_outcome_checkpoint(
    *,
    data_root: Path | str,
    outcome_replay_manifest_id: int,
    run_fingerprint: str,
    checkpoint_sequence: int,
    completed_source_date_count: int,
    last_completed_source_date: date,
    source_event_count: int,
    predecessor_checkpoint_sha256: str | None,
    replay_state: Mapping[str, object],
    detail_shards: Sequence[DetailShardArtifact],
    cache_manifest: CacheManifestArtifact | LoadedCacheManifest,
    input_lineage: Mapping[str, object],
) -> CheckpointArtifact:
    """Publish one registry-compatible SOURCE_DATE_COMPLETE checkpoint."""

    manifest_id = _integer(
        outcome_replay_manifest_id, label="outcome_replay_manifest_id", minimum=1
    )
    fingerprint = _sha256(run_fingerprint, label="run_fingerprint")
    sequence = _integer(checkpoint_sequence, label="checkpoint_sequence", minimum=1)
    completed_count = _integer(
        completed_source_date_count,
        label="completed_source_date_count",
        minimum=1,
    )
    if completed_count != sequence:
        raise OutcomeArtifactError("completed_source_date_count must equal checkpoint_sequence")
    day = _day(last_completed_source_date, label="last_completed_source_date")
    event_count = _integer(source_event_count, label="source_event_count")
    if sequence == 1:
        if predecessor_checkpoint_sha256 is not None:
            raise OutcomeArtifactError("checkpoint one cannot have a predecessor")
        predecessor = None
    else:
        predecessor = _sha256(predecessor_checkpoint_sha256, label="predecessor_checkpoint_sha256")
    shards = _validated_shards(
        detail_shards,
        run_fingerprint=fingerprint,
        data_root=data_root,
    )
    if len(shards) != completed_count or shards[-1].source_date != day:
        raise OutcomeArtifactError("checkpoint count/date differs from cumulative shards")
    detail_rows = sum(shard.row_count for shard in shards)
    state, _, replay_finished = _validate_replay_boundary(
        replay_state,
        last_completed_source_date=day,
        source_event_count=event_count,
        detail_record_count=detail_rows,
    )
    cache = _cache_artifact(cache_manifest)
    relative_uri = _quick_artifact_check(
        path=cache.path,
        sha256=cache.sha256,
        byte_size=cache.byte_size,
        suffix=".json",
        data_root=data_root,
    )
    if relative_uri != cache.relative_uri:
        raise OutcomeArtifactError("cache manifest relative URI drift")
    inputs = _canonical_mapping(input_lineage, label="input_lineage")
    shard_lineage = [shard.as_dict() for shard in shards]
    shard_manifest_sha256 = canonical_sha256(shard_lineage)
    state_sha256 = canonical_sha256(state)
    input_sha256 = canonical_sha256(inputs)
    progress = _progress_metadata(
        replay_state_sha256=state_sha256,
        detail_shard_manifest_sha256=shard_manifest_sha256,
        detail_shard_count=len(shards),
        detail_record_count=detail_rows,
        cache_manifest_sha256=cache.sha256,
        input_lineage_sha256=input_sha256,
        source_event_count=event_count,
        replay_finished=replay_finished,
    )
    document = {
        "artifact_schema": CHECKPOINT_ARTIFACT_SCHEMA,
        "cache_manifest": cache.as_dict(),
        "checkpoint_sequence": sequence,
        "completed_source_date_count": completed_count,
        "detail_record_count": detail_rows,
        "detail_shard_manifest_sha256": shard_manifest_sha256,
        "detail_shards": shard_lineage,
        "input_lineage": inputs,
        "input_lineage_sha256": input_sha256,
        "last_completed_source_date": day.isoformat(),
        "outcome_config_id": OUTCOME_CONFIG_ID,
        "outcome_replay_manifest_id": manifest_id,
        "predecessor_checkpoint_sha256": predecessor,
        "progress_metadata": progress,
        "progress_metadata_sha256": canonical_sha256(progress),
        "query_id": P5_QUERY_ID,
        "replay_state": state,
        "replay_state_sha256": state_sha256,
        "run_fingerprint": fingerprint,
        "source_event_count": event_count,
    }
    path, digest, byte_size, disposition, relative = _publish_json(
        document,
        data_root=data_root,
        output_relative=_CHECKPOINT_DIRECTORY,
    )
    return CheckpointArtifact(
        path=path,
        relative_uri=relative,
        sha256=digest,
        byte_size=byte_size,
        disposition=disposition,
        checkpoint_sequence=sequence,
        last_completed_source_date=day,
        progress_metadata=progress,
    )


def load_outcome_checkpoint(
    artifact: CheckpointArtifact | Path,
    *,
    data_root: Path | str,
    expected_sha256: str | None = None,
    expected_byte_size: int | None = None,
    expected_progress_metadata: Mapping[str, object] | None = None,
    verify_cache_manifest: bool = True,
    verify_cache_content: bool = True,
    verify_detail_content: bool = True,
    retain_detail_records: bool = True,
) -> LoadedCheckpoint:
    """Strictly restore replay state and validate cumulative lineage.

    Detail shards are validated one at a time.  ``retain_detail_records=False``
    keeps peak memory bounded to one daily shard while preserving full content
    validation.  Callers that consume the shards separately (for example the
    replay-resume economics accumulator) may also set
    ``verify_detail_content=False`` to avoid reading every shard twice.

    ``verify_cache_manifest=False`` is reserved for a same-process lineage
    binding where the cache manifest was already validated.  It still checks
    the immutable descriptor and content-addressed path, but does not reopen
    the manifest or any referenced cache.  Cache-content verification requires
    cache-manifest verification.
    """

    for value, label in (
        (verify_cache_manifest, "verify_cache_manifest"),
        (verify_cache_content, "verify_cache_content"),
        (verify_detail_content, "verify_detail_content"),
        (retain_detail_records, "retain_detail_records"),
    ):
        if not isinstance(value, bool):
            raise OutcomeArtifactError(f"{label} must be a boolean")
    if verify_cache_content and not verify_cache_manifest:
        raise OutcomeArtifactError(
            "cache-content verification requires cache-manifest verification"
        )

    if isinstance(artifact, CheckpointArtifact):
        path = artifact.path
        expected_sha256 = artifact.sha256
        expected_byte_size = artifact.byte_size
    elif isinstance(artifact, Path):
        path = artifact
        if expected_sha256 is None or expected_byte_size is None:
            raise OutcomeArtifactError("Path checkpoint loads require expected SHA and byte size")
    else:
        raise OutcomeArtifactError("checkpoint artifact must be CheckpointArtifact or Path")
    held, document = _read_json(
        path,
        data_root=data_root,
        expected_sha256=expected_sha256,
        expected_byte_size=expected_byte_size,
    )
    try:
        if set(document) != _CHECKPOINT_FIELDS:
            raise OutcomeArtifactError("checkpoint top-level schema drift")
        if document.get("artifact_schema") != CHECKPOINT_ARTIFACT_SCHEMA:
            raise OutcomeArtifactError("checkpoint artifact schema drift")
        if document.get("outcome_config_id") != OUTCOME_CONFIG_ID:
            raise OutcomeArtifactError("checkpoint outcome config drift")
        if document.get("query_id") != P5_QUERY_ID:
            raise OutcomeArtifactError("checkpoint query drift")
        sequence = _integer(
            document.get("checkpoint_sequence"), label="checkpoint_sequence", minimum=1
        )
        completed_count = _integer(
            document.get("completed_source_date_count"),
            label="completed_source_date_count",
            minimum=1,
        )
        if sequence != completed_count:
            raise OutcomeArtifactError("checkpoint sequence/count drift")
        day = _day(
            document.get("last_completed_source_date"),
            label="last_completed_source_date",
        )
        event_count = _integer(document.get("source_event_count"), label="source_event_count")
        fingerprint = _sha256(document.get("run_fingerprint"), label="run_fingerprint")
        _integer(
            document.get("outcome_replay_manifest_id"),
            label="outcome_replay_manifest_id",
            minimum=1,
        )
        predecessor = document.get("predecessor_checkpoint_sha256")
        if sequence == 1:
            if predecessor is not None:
                raise OutcomeArtifactError("checkpoint one has a predecessor")
        else:
            _sha256(predecessor, label="predecessor_checkpoint_sha256")
        progress = _canonical_mapping(document.get("progress_metadata"), label="progress_metadata")
        if canonical_sha256(progress) != _sha256(
            document.get("progress_metadata_sha256"),
            label="progress_metadata_sha256",
        ):
            raise OutcomeArtifactError("checkpoint progress metadata hash drift")
        if expected_progress_metadata is not None and progress != _canonical_mapping(
            expected_progress_metadata, label="expected_progress_metadata"
        ):
            raise OutcomeArtifactError("checkpoint progress metadata differs from registry")
        root, derived = _data_layout(data_root)
        raw_shards = document.get("detail_shards")
        if not isinstance(raw_shards, list):
            raise OutcomeArtifactError("checkpoint detail_shards must be a list")
        shards = tuple(_shard_from_mapping(value, data_root=root) for value in raw_shards)
        shards = _validated_shards(
            shards,
            run_fingerprint=fingerprint,
            data_root=data_root,
        )
        if len(shards) != completed_count or shards[-1].source_date != day:
            raise OutcomeArtifactError("checkpoint shard count/date drift")
        shard_lineage = [shard.as_dict() for shard in shards]
        shard_sha256 = canonical_sha256(shard_lineage)
        if shard_sha256 != _sha256(
            document.get("detail_shard_manifest_sha256"),
            label="detail_shard_manifest_sha256",
        ):
            raise OutcomeArtifactError("checkpoint detail shard manifest hash drift")
        loaded_shard_values: list[LoadedDetailShard] = []
        if verify_detail_content:
            detail_rows = 0
            for shard in shards:
                loaded_shard = load_detail_shard(shard, data_root=data_root)
                detail_rows += len(loaded_shard.records)
                if retain_detail_records:
                    loaded_shard_values.append(loaded_shard)
                else:
                    # Do not retain a reference to a completed daily shard.  The
                    # next iteration is therefore the only possible live record
                    # materialization owned by this loader.
                    del loaded_shard
        else:
            detail_rows = sum(shard.row_count for shard in shards)
        loaded_shards = tuple(loaded_shard_values)
        if detail_rows != _integer(
            document.get("detail_record_count"), label="detail_record_count"
        ):
            raise OutcomeArtifactError("checkpoint detail record count drift")
        state = _canonical_mapping(document.get("replay_state"), label="replay_state")
        if canonical_sha256(state) != _sha256(
            document.get("replay_state_sha256"), label="replay_state_sha256"
        ):
            raise OutcomeArtifactError("checkpoint replay state hash drift")
        state, replay, replay_finished = _validate_replay_boundary(
            state,
            last_completed_source_date=day,
            source_event_count=event_count,
            detail_record_count=detail_rows,
        )
        inputs = _canonical_mapping(document.get("input_lineage"), label="input_lineage")
        if canonical_sha256(inputs) != _sha256(
            document.get("input_lineage_sha256"), label="input_lineage_sha256"
        ):
            raise OutcomeArtifactError("checkpoint input lineage hash drift")
        cache = _cache_artifact_from_mapping(document.get("cache_manifest"), data_root=root)
        if verify_cache_manifest:
            loaded_cache_artifact = load_cache_manifest(
                cache,
                data_root=data_root,
                verify_cache_content=verify_cache_content,
            ).artifact
        else:
            cache_relative_uri = _quick_artifact_check(
                path=cache.path,
                sha256=cache.sha256,
                byte_size=cache.byte_size,
                suffix=".json",
                data_root=data_root,
            )
            if cache_relative_uri != cache.relative_uri:
                raise OutcomeArtifactError("checkpoint cache manifest relative URI drift")
            loaded_cache_artifact = cache
        expected_progress = _progress_metadata(
            replay_state_sha256=canonical_sha256(state),
            detail_shard_manifest_sha256=shard_sha256,
            detail_shard_count=len(shards),
            detail_record_count=detail_rows,
            cache_manifest_sha256=loaded_cache_artifact.sha256,
            input_lineage_sha256=canonical_sha256(inputs),
            source_event_count=event_count,
            replay_finished=replay_finished,
        )
        if progress != expected_progress:
            raise OutcomeArtifactError("checkpoint progress metadata content drift")
        relative = held.path.relative_to(derived).as_posix()
        result_artifact = CheckpointArtifact(
            path=held.path,
            relative_uri=relative,
            sha256=held.sha256,
            byte_size=held.byte_size,
            disposition="REUSED",
            checkpoint_sequence=sequence,
            last_completed_source_date=day,
            progress_metadata=progress,
        )
        _verify_held(held)
        return LoadedCheckpoint(
            artifact=result_artifact,
            document=document,
            replay=replay,
            replay_state=state,
            detail_shards=shards,
            loaded_detail_shards=loaded_shards,
            cache_manifest=loaded_cache_artifact,
            input_lineage=inputs,
            progress_metadata=progress,
        )
    finally:
        held.close()


def publish_final_result_manifest(
    *,
    data_root: Path | str,
    run_fingerprint: str,
    source_artifact_manifest_sha256: str,
    cell_summaries: Sequence[OutcomeCellSummary],
    detail_shards: Sequence[DetailShardArtifact],
    cache_manifest: CacheManifestArtifact | LoadedCacheManifest,
    input_lineage: Mapping[str, object],
    final_checkpoint: CheckpointArtifact | LoadedCheckpoint,
) -> FinalResultArtifact:
    """Publish all 2,904 summaries bound to one finished replay checkpoint."""

    fingerprint = _sha256(run_fingerprint, label="run_fingerprint")
    source_manifest_sha256 = _sha256(
        source_artifact_manifest_sha256,
        label="source_artifact_manifest_sha256",
    )
    try:
        summaries, summaries_sha256 = validate_complete_cell_summaries(cell_summaries)
    except OutcomeRegistryError as error:
        raise OutcomeArtifactError("final cell summaries are not a complete frozen grid") from error
    shards = _validated_shards(
        detail_shards,
        run_fingerprint=fingerprint,
        data_root=data_root,
    )
    shard_lineage = [shard.as_dict() for shard in shards]
    shard_manifest_sha256 = canonical_sha256(shard_lineage)
    detail_record_count = sum(shard.row_count for shard in shards)
    cache = _cache_artifact(cache_manifest)
    relative_uri = _quick_artifact_check(
        path=cache.path,
        sha256=cache.sha256,
        byte_size=cache.byte_size,
        suffix=".json",
        data_root=data_root,
    )
    if relative_uri != cache.relative_uri:
        raise OutcomeArtifactError("final cache manifest relative URI drift")
    inputs = _canonical_mapping(input_lineage, label="input_lineage")
    input_lineage_sha256 = canonical_sha256(inputs)
    checkpoint = _checkpoint_artifact(final_checkpoint)
    checkpoint_relative_uri = _quick_artifact_check(
        path=checkpoint.path,
        sha256=checkpoint.sha256,
        byte_size=checkpoint.byte_size,
        suffix=".json",
        data_root=data_root,
    )
    if checkpoint_relative_uri != checkpoint.relative_uri:
        raise OutcomeArtifactError("final checkpoint relative URI drift")
    loaded_checkpoint = load_outcome_checkpoint(
        checkpoint,
        data_root=data_root,
        expected_progress_metadata=checkpoint.progress_metadata,
        verify_cache_manifest=False,
        verify_cache_content=False,
        verify_detail_content=False,
        retain_detail_records=False,
    )
    if loaded_checkpoint.document.get("run_fingerprint") != fingerprint:
        raise OutcomeArtifactError("final checkpoint run fingerprint drift")
    if not loaded_checkpoint.replay.finished:
        raise OutcomeArtifactError("final result requires a finished checkpoint")
    if (
        loaded_checkpoint.artifact.checkpoint_sequence != len(shards)
        or loaded_checkpoint.artifact.last_completed_source_date != shards[-1].source_date
    ):
        raise OutcomeArtifactError("final checkpoint sequence/date differs from detail lineage")
    if [item.as_dict() for item in loaded_checkpoint.detail_shards] != shard_lineage:
        raise OutcomeArtifactError("final checkpoint detail lineage drift")
    if loaded_checkpoint.cache_manifest.as_dict() != cache.as_dict():
        raise OutcomeArtifactError("final checkpoint cache lineage drift")
    if loaded_checkpoint.input_lineage != inputs:
        raise OutcomeArtifactError("final checkpoint input lineage drift")
    expected_progress_bindings = {
        "cache_manifest_sha256": cache.sha256,
        "detail_record_count": detail_record_count,
        "detail_shard_count": len(shards),
        "detail_shard_manifest_sha256": shard_manifest_sha256,
        "input_lineage_sha256": input_lineage_sha256,
        "replay_finished": True,
    }
    if any(
        loaded_checkpoint.progress_metadata.get(key) != value
        for key, value in expected_progress_bindings.items()
    ):
        raise OutcomeArtifactError("final checkpoint progress lineage drift")
    checkpoint = loaded_checkpoint.artifact
    document = {
        "artifact_schema": OUTCOME_ARTIFACT_SCHEMA,
        "cache_manifest": cache.as_dict(),
        "cell_summaries": [summary.payload for summary in summaries],
        "cell_summaries_sha256": summaries_sha256,
        "detail_record_count": detail_record_count,
        "detail_shard_count": len(shards),
        "detail_shard_manifest_sha256": shard_manifest_sha256,
        "detail_shards": shard_lineage,
        "direction_ids": list(DIRECTION_IDS),
        "final_checkpoint": checkpoint.as_dict(),
        "input_lineage": inputs,
        "input_lineage_sha256": input_lineage_sha256,
        "outcome_config_id": OUTCOME_CONFIG_ID,
        "query_id": P5_QUERY_ID,
        "run_fingerprint": fingerprint,
        "scenario_ids": list(SCENARIO_IDS),
        "source_artifact_manifest_sha256": source_manifest_sha256,
        "source_occurrence_count": EXPECTED_SOURCE_OCCURRENCE_COUNT,
        "source_slice_count": EXPECTED_SOURCE_SLICE_COUNT,
        "summary_row_count": EXPECTED_SUMMARY_COUNT,
    }
    path, digest, byte_size, disposition, relative = _publish_json(
        document,
        data_root=data_root,
        output_relative=_RESULT_DIRECTORY,
    )
    return FinalResultArtifact(
        path=path,
        relative_uri=relative,
        sha256=digest,
        byte_size=byte_size,
        disposition=disposition,
        cell_summaries_sha256=summaries_sha256,
        summary_row_count=len(summaries),
        detail_shard_manifest_sha256=shard_manifest_sha256,
        detail_record_count=detail_record_count,
        final_checkpoint_sha256=checkpoint.sha256,
        final_checkpoint_sequence=checkpoint.checkpoint_sequence,
    )


def load_final_result_manifest(
    artifact: FinalResultArtifact | Path,
    *,
    data_root: Path | str,
    expected_sha256: str | None = None,
    expected_byte_size: int | None = None,
    verify_cache_content: bool = True,
    verify_detail_content: bool = True,
) -> LoadedFinalResult:
    """Strictly validate a registry-compatible final result and all its lineage."""

    if isinstance(artifact, FinalResultArtifact):
        path = artifact.path
        expected_sha256 = artifact.sha256
        expected_byte_size = artifact.byte_size
    elif isinstance(artifact, Path):
        path = artifact
        if expected_sha256 is None or expected_byte_size is None:
            raise OutcomeArtifactError("Path final loads require expected SHA and byte size")
    else:
        raise OutcomeArtifactError("final artifact must be FinalResultArtifact or Path")
    held, document = _read_json(
        path,
        data_root=data_root,
        expected_sha256=expected_sha256,
        expected_byte_size=expected_byte_size,
    )
    try:
        if set(document) != _FINAL_RESULT_FIELDS:
            raise OutcomeArtifactError("final result top-level schema drift")
        expected_static = {
            "artifact_schema": OUTCOME_ARTIFACT_SCHEMA,
            "direction_ids": list(DIRECTION_IDS),
            "outcome_config_id": OUTCOME_CONFIG_ID,
            "query_id": P5_QUERY_ID,
            "scenario_ids": list(SCENARIO_IDS),
            "source_occurrence_count": EXPECTED_SOURCE_OCCURRENCE_COUNT,
            "source_slice_count": EXPECTED_SOURCE_SLICE_COUNT,
            "summary_row_count": EXPECTED_SUMMARY_COUNT,
        }
        if any(document.get(key) != value for key, value in expected_static.items()):
            raise OutcomeArtifactError("final result registry field drift")
        _sha256(document.get("run_fingerprint"), label="run_fingerprint")
        _sha256(
            document.get("source_artifact_manifest_sha256"),
            label="source_artifact_manifest_sha256",
        )
        raw_summaries = document.get("cell_summaries")
        if not isinstance(raw_summaries, list):
            raise OutcomeArtifactError("final cell_summaries must be a list")
        try:
            parsed_summaries = tuple(
                OutcomeCellSummary.from_mapping(value)
                for value in raw_summaries
                if isinstance(value, Mapping)
            )
            if len(parsed_summaries) != len(raw_summaries):
                raise OutcomeArtifactError("final cell summary is not an object")
            summaries, summaries_sha256 = validate_complete_cell_summaries(parsed_summaries)
        except OutcomeRegistryError as error:
            raise OutcomeArtifactError("final cell summary grid validation failed") from error
        if summaries_sha256 != _sha256(
            document.get("cell_summaries_sha256"), label="cell_summaries_sha256"
        ):
            raise OutcomeArtifactError("final cell summaries semantic hash drift")
        fingerprint = str(document["run_fingerprint"])
        root, derived = _data_layout(data_root)
        raw_shards = document.get("detail_shards")
        if not isinstance(raw_shards, list):
            raise OutcomeArtifactError("final detail_shards must be a list")
        shards = tuple(_shard_from_mapping(value, data_root=root) for value in raw_shards)
        shards = _validated_shards(
            shards,
            run_fingerprint=fingerprint,
            data_root=data_root,
        )
        if document.get("detail_shard_count") != len(shards):
            raise OutcomeArtifactError("final detail shard count drift")
        shard_sha256 = canonical_sha256([shard.as_dict() for shard in shards])
        if shard_sha256 != _sha256(
            document.get("detail_shard_manifest_sha256"),
            label="detail_shard_manifest_sha256",
        ):
            raise OutcomeArtifactError("final detail shard manifest hash drift")
        inputs = _canonical_mapping(document.get("input_lineage"), label="input_lineage")
        if canonical_sha256(inputs) != _sha256(
            document.get("input_lineage_sha256"), label="input_lineage_sha256"
        ):
            raise OutcomeArtifactError("final input lineage hash drift")
        cache = _cache_artifact_from_mapping(document.get("cache_manifest"), data_root=root)
        checkpoint = _checkpoint_artifact_from_mapping(
            document.get("final_checkpoint"),
            data_root=root,
        )
        loaded_checkpoint = load_outcome_checkpoint(
            checkpoint,
            data_root=data_root,
            expected_progress_metadata=checkpoint.progress_metadata,
            verify_cache_content=verify_cache_content,
            verify_detail_content=verify_detail_content,
            retain_detail_records=False,
        )
        if loaded_checkpoint.artifact.as_dict() != checkpoint.as_dict():
            raise OutcomeArtifactError("final checkpoint artifact identity drift")
        if (
            loaded_checkpoint.document.get("run_fingerprint") != fingerprint
            or not loaded_checkpoint.replay.finished
        ):
            raise OutcomeArtifactError("final checkpoint completion identity drift")
        if (
            loaded_checkpoint.artifact.checkpoint_sequence != len(shards)
            or loaded_checkpoint.artifact.last_completed_source_date != shards[-1].source_date
            or [item.as_dict() for item in loaded_checkpoint.detail_shards]
            != [item.as_dict() for item in shards]
        ):
            raise OutcomeArtifactError("final checkpoint detail lineage drift")
        if loaded_checkpoint.cache_manifest.as_dict() != cache.as_dict():
            raise OutcomeArtifactError("final checkpoint cache lineage drift")
        if loaded_checkpoint.input_lineage != inputs:
            raise OutcomeArtifactError("final checkpoint input lineage drift")
        # The checkpoint loader validates detail content one daily shard at a
        # time when requested, but deliberately does not retain those records.
        # Row cardinality is already cross-checked against every loaded shard's
        # Parquet metadata and the checkpoint replay counters.
        detail_record_count = sum(shard.row_count for shard in shards)
        if detail_record_count != _integer(
            document.get("detail_record_count"), label="detail_record_count"
        ):
            raise OutcomeArtifactError("final detail record count drift")
        relative = held.path.relative_to(derived).as_posix()
        result_artifact = FinalResultArtifact(
            path=held.path,
            relative_uri=relative,
            sha256=held.sha256,
            byte_size=held.byte_size,
            disposition="REUSED",
            cell_summaries_sha256=summaries_sha256,
            summary_row_count=len(summaries),
            detail_shard_manifest_sha256=shard_sha256,
            detail_record_count=detail_record_count,
            final_checkpoint_sha256=loaded_checkpoint.artifact.sha256,
            final_checkpoint_sequence=loaded_checkpoint.artifact.checkpoint_sequence,
        )
        _verify_held(held)
        return LoadedFinalResult(
            artifact=result_artifact,
            document=document,
            cell_summaries=summaries,
            detail_shards=shards,
            cache_manifest=loaded_checkpoint.cache_manifest,
            input_lineage=inputs,
            final_checkpoint=loaded_checkpoint,
        )
    finally:
        held.close()
