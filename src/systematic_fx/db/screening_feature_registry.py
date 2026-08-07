"""Governed registration for one five-date Phase 1A screening-feature batch.

Preparation verifies the builder reports and both Parquet artifacts from open
file descriptors, constructs immutable content-addressed identities, and does
not access PostgreSQL.  Registration republishes verified bytes to canonical
snapshots, publishes one canonical batch manifest below ``data/derived``, then
atomically records or exactly reuses the job, manifest artifact, partitions,
and their current/previous raw-source links in a SERIALIZABLE transaction.

``VALIDATED`` here means byte/schema/provenance validation only.  Every
manifest, job, and partition remains explicitly screening-only and not
research eligible.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from functools import wraps
from itertools import pairwise
from pathlib import Path, PurePosixPath
from typing import Any, Final, ParamSpec, TypeVar

import psycopg
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from psycopg import IsolationLevel
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.data.contract_selection import (
    CONTRACT_SELECTION_POLICY_VERSION,
    CONTRACT_SELECTION_SCHEMA,
    ContractSelectionResult,
)
from systematic_fx.db.data_registry import (
    ManifestValidationError,
    load_source_manifest_bundle,
)
from systematic_fx.db.postgres_retry import retry_serialization_failures
from systematic_fx.features.screening import (
    DEFAULT_CONFIG_PATH,
    FEATURE_VERSION,
    FIVE_MINUTE_SCHEMA,
    FORMULA_SHA256,
    ONE_SECOND_SCHEMA,
    PRICE_SCALE,
    TICK_SIZE_RAW,
    ScreeningArtifactReport,
    ScreeningFeatureBuildReport,
    load_phase1a_screening_config,
)
from systematic_fx.research.run_spec import (
    RUN_SPEC_SCHEMA,
    RUN_SPEC_SCHEMA_VERSION,
    RunSpec,
)
from systematic_fx.validation.splits import (
    CALENDAR_VERSION,
    Phase1AScreeningCalendar,
)

FEATURE_BATCH_MANIFEST_SCHEMA: Final = "systematic_fx.phase1a_feature_build_batch.v1"
FEATURE_BATCH_REGISTRY_VERSION: Final = "phase1a_feature_build_v1"
FEATURE_BATCH_SIZE: Final = 5
DEFAULT_DATASET_KEY: Final = "glbx_mdp3_mbp_10_6e_fut_v1"
MANIFEST_SUBDIRECTORY: Final = PurePosixPath("derived/manifests/phase1a_feature_build_v1")
SNAPSHOT_SUBDIRECTORY: Final = PurePosixPath("derived/registry/phase1a_feature_build_v1")

_SHA256 = re.compile(r"[0-9a-f]{64}")
_SAFE_SYMBOL = re.compile(r"6E[FGHJKMNQUVXZ][0-9]{1,2}")
_NO_ENTRY_REASON = re.compile(r"[A-Z][A-Z0-9_]*")
_SOURCE_URI = re.compile(
    r"(?P<year>[0-9]{4})/(?P<month>[0-9]{2})/(?P<day>[0-9]{2})/"
    r"glbx-mdp3-(?P<stamp>[0-9]{8})\.mbp-10\.parquet"
)
_VALID_SOURCE_STATUSES = frozenset({"HASHED", "VALIDATED"})
_VALID_DATASET_STATUSES = frozenset({"VALIDATING", "READY"})
_COPY_CHUNK_SIZE: Final = 1024 * 1024
_FOOTER_MANIFEST_KEY: Final = "mbp10_footer_manifest_v1"
_SOURCE_MANIFEST_KEY: Final = "mbp10_source_sha256_v1"
_QC_MANIFEST_KEY: Final = "mbp10_structural_qc_v1"
_FOOTER_MANIFEST_RELATIVE_URI: Final = PurePosixPath(
    "derived/manifests/mbp10_footer_manifest_v1.jsonl"
)
_SOURCE_MANIFEST_RELATIVE_URI: Final = PurePosixPath(
    "derived/manifests/mbp10_source_sha256_v1.jsonl"
)
_SOURCE_MANIFEST_FIELDS: Final = frozenset({"byte_size", "relative_uri", "sha256", "source_date"})
_JOB_TYPE: Final = "BUILD_PHASE1A_SCREENING_FEATURES"
_PARTITION_TYPES: Final = ("FEATURES_1S", "RESEARCH_5M")
_P = ParamSpec("_P")
_R = TypeVar("_R")


class ScreeningFeatureRegistryError(RuntimeError):
    """A screening feature batch could not be prepared or registered."""


class ScreeningFeatureArtifactError(ScreeningFeatureRegistryError):
    """A report, source identity, path, or Parquet artifact is invalid."""


class ScreeningFeatureRegistryDriftError(ScreeningFeatureRegistryError):
    """An immutable file or PostgreSQL identity has conflicting content."""


class ScreeningFeatureRegistryDatabaseError(ScreeningFeatureRegistryError):
    """PostgreSQL rejected the atomic screening-feature registration."""


class BatchEntryStatus(StrEnum):
    """Whether a batch date supplies feature artifacts or only prior-volume context."""

    BUILT = "BUILT"
    RECORDED_NO_ENTRY = "RECORDED_NO_ENTRY"


@dataclass(frozen=True, slots=True)
class RawSourceReference:
    """Manifest-bound identity for one raw MBP-10 source."""

    source_date: date
    relative_uri: str
    sha256: str


@dataclass(frozen=True, slots=True)
class ScreeningFeatureBatchEntry:
    """One ordered date in the fixed five-date feature-build batch."""

    source: RawSourceReference
    status: BatchEntryStatus
    report: ScreeningFeatureBuildReport | None = None
    selection: ContractSelectionResult | None = None
    previous_source: RawSourceReference | None = None
    no_entry_reason: str | None = None


@dataclass(frozen=True, slots=True)
class VerifiedScreeningArtifact:
    """One descriptor-verified 1s or 5m Parquet artifact."""

    source_date: date
    partition_type: str
    granularity: str
    original_path: Path
    original_relative_uri: str
    canonical_path: Path
    canonical_relative_uri: str
    sha256: str
    byte_size: int
    row_count: int
    schema_sha256: str
    min_event_time_ns: int
    max_event_time_ns: int


@dataclass(frozen=True, slots=True)
class PreparedBatchEntry:
    """Validated normalized input retained by the canonical manifest."""

    source: RawSourceReference
    status: BatchEntryStatus
    previous_source: RawSourceReference | None
    report: ScreeningFeatureBuildReport | None
    selection: ContractSelectionResult | None
    no_entry_reason: str | None
    artifacts: tuple[VerifiedScreeningArtifact, ...]


@dataclass(frozen=True, slots=True)
class _FileIdentity:
    device: int
    inode: int
    byte_size: int
    mtime_ns: int
    ctime_ns: int


@dataclass(frozen=True, slots=True)
class _PreparedRawSource:
    reference: RawSourceReference
    path: Path
    identity: _FileIdentity


@dataclass(frozen=True, slots=True)
class _SourceManifestRecord:
    reference: RawSourceReference
    byte_size: int


@dataclass(frozen=True, slots=True)
class _PreparedSourceManifest:
    path: Path
    identity: _FileIdentity
    records: tuple[_SourceManifestRecord, ...]
    sha256: str


@dataclass(frozen=True, slots=True)
class _PreparedFooterManifest:
    path: Path
    identity: _FileIdentity
    sha256: str


@dataclass(frozen=True, slots=True)
class PreparedScreeningFeatureBatch:
    """Fully verified file and provenance inputs, ready for publication/DB use."""

    data_root: Path
    dataset_key: str
    calendar: Phase1AScreeningCalendar
    run_spec: RunSpec
    footer_manifest: _PreparedFooterManifest
    source_manifest: _PreparedSourceManifest
    entries: tuple[PreparedBatchEntry, ...]
    raw_sources: tuple[_PreparedRawSource, ...]
    artifacts: tuple[VerifiedScreeningArtifact, ...]
    config_sha256: str
    formula_sha256: str
    code_snapshot_sha256: str
    manifest_document: dict[str, object]
    manifest_bytes: bytes
    manifest_sha256: str
    manifest_path: Path

    @property
    def footer_manifest_sha256(self) -> str:
        return self.footer_manifest.sha256


@dataclass(frozen=True, slots=True)
class ScreeningFeatureRegistrationReport:
    """Committed identities for one exact five-date batch."""

    dataset_id: int
    campaign_id: int
    research_run_spec_id: int
    build_job_id: int
    manifest_artifact_id: int
    partition_ids: tuple[int, ...]
    source_file_ids: tuple[tuple[str, int], ...]
    manifest_path: Path
    manifest_sha256: str
    created_job: bool
    created_manifest_artifact: bool
    created_partitions: int

    def as_dict(self) -> dict[str, object]:
        return {
            "build_job_id": self.build_job_id,
            "campaign_id": self.campaign_id,
            "created_job": self.created_job,
            "created_manifest_artifact": self.created_manifest_artifact,
            "created_partitions": self.created_partitions,
            "dataset_id": self.dataset_id,
            "manifest_artifact_id": self.manifest_artifact_id,
            "manifest_path": str(self.manifest_path),
            "manifest_sha256": self.manifest_sha256,
            "partition_ids": list(self.partition_ids),
            "research_run_spec_id": self.research_run_spec_id,
            "source_file_ids": dict(self.source_file_ids),
        }


def _translate_psycopg_errors(
    operation: str,
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    def decorate(function: Callable[_P, _R]) -> Callable[_P, _R]:
        @wraps(function)
        def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            try:
                return retry_serialization_failures(function, *args, **kwargs)
            except ScreeningFeatureRegistryError:
                raise
            except psycopg.Error as error:
                raise ScreeningFeatureRegistryDatabaseError(
                    f"PostgreSQL {operation} failed"
                ) from error

        return wrapped

    return decorate


def _require_nonempty(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ScreeningFeatureArtifactError(f"{label} must be a trimmed non-empty string")
    return value


def _require_sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ScreeningFeatureArtifactError(f"{label} must be a lowercase SHA-256")
    return value


def _normalize_no_entry_reason(value: object) -> str:
    reason = _require_nonempty(value, label="RECORDED_NO_ENTRY no_entry_reason")
    if _NO_ENTRY_REASON.fullmatch(reason) is None:
        raise ScreeningFeatureArtifactError(
            "RECORDED_NO_ENTRY no_entry_reason must be canonical UPPER_SNAKE_CASE"
        )
    return reason


def _parse_date(value: object, *, label: str) -> date:
    if isinstance(value, datetime):
        raise ScreeningFeatureArtifactError(f"{label} must not be a datetime")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise ScreeningFeatureArtifactError(f"{label} must be an ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ScreeningFeatureArtifactError(f"{label} must be an ISO date") from error


def _safe_relative_uri(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ScreeningFeatureArtifactError(f"{label} must be a safe relative URI")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ScreeningFeatureArtifactError(f"{label} must be a safe relative URI")
    return value


def _relative_to(path: Path, root: Path, *, label: str) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError as error:
        raise ScreeningFeatureArtifactError(f"{label} must remain below {root}") from error


def _file_identity(value: os.stat_result) -> _FileIdentity:
    return _FileIdentity(
        device=value.st_dev,
        inode=value.st_ino,
        byte_size=value.st_size,
        mtime_ns=value.st_mtime_ns,
        ctime_ns=value.st_ctime_ns,
    )


def _ensure_no_symlink(root: Path, path: Path, *, label: str) -> None:
    relative = path.relative_to(root)
    cursor = root
    for part in relative.parts:
        cursor /= part
        if cursor.is_symlink():
            raise ScreeningFeatureArtifactError(f"{label} contains a symbolic link: {cursor}")


def _resolve_data_root(value: Path | str) -> tuple[Path, Path]:
    requested = Path(value).expanduser()
    if requested.is_symlink():
        raise ScreeningFeatureArtifactError("data_root cannot be a symbolic link")
    try:
        root = requested.resolve(strict=True)
    except FileNotFoundError as error:
        raise ScreeningFeatureArtifactError(f"data_root does not exist: {requested}") from error
    if not root.is_dir() or root.name != "data":
        raise ScreeningFeatureArtifactError("data_root must be an existing directory named data")
    derived = root / "derived"
    if derived.is_symlink() or not derived.is_dir():
        raise ScreeningFeatureArtifactError("data/derived must be an existing real directory")
    return root, derived.resolve(strict=True)


def _normalize_source(reference: RawSourceReference) -> RawSourceReference:
    if not isinstance(reference, RawSourceReference):
        raise ScreeningFeatureArtifactError("source references must be RawSourceReference values")
    source_date = _parse_date(reference.source_date, label="source_date")
    relative_uri = _safe_relative_uri(reference.relative_uri, label="source relative_uri")
    match = _SOURCE_URI.fullmatch(relative_uri)
    stamp = source_date.strftime("%Y%m%d")
    if match is None or (
        match.group("year"),
        match.group("month"),
        match.group("day"),
        match.group("stamp"),
    ) != (
        f"{source_date.year:04d}",
        f"{source_date.month:02d}",
        f"{source_date.day:02d}",
        stamp,
    ):
        raise ScreeningFeatureArtifactError("source relative_uri date identity drift")
    return RawSourceReference(
        source_date=source_date,
        relative_uri=relative_uri,
        sha256=_require_sha256(reference.sha256, label="source sha256"),
    )


def _prepare_raw_source(
    data_root: Path,
    reference: RawSourceReference,
    *,
    expected_byte_size: int,
) -> _PreparedRawSource:
    raw_root = data_root / "mbp-10"
    requested = raw_root.joinpath(*PurePosixPath(reference.relative_uri).parts)
    if requested.is_symlink():
        raise ScreeningFeatureArtifactError(f"raw source cannot be a symlink: {requested}")
    try:
        path = requested.resolve(strict=True)
    except FileNotFoundError as error:
        raise ScreeningFeatureArtifactError(f"raw source does not exist: {requested}") from error
    resolved_raw_root = raw_root.resolve(strict=True)
    _relative_to(path, resolved_raw_root, label="raw source")
    _ensure_no_symlink(resolved_raw_root, path, label="raw source")
    descriptor, identity = _open_descriptor(path, label="raw source")
    try:
        if identity.byte_size != expected_byte_size:
            raise ScreeningFeatureArtifactError(
                f"raw source byte size differs from canonical source manifest: {path}"
            )
        if _descriptor_sha256(descriptor) != reference.sha256:
            raise ScreeningFeatureArtifactError(
                f"raw source bytes differ from canonical source manifest SHA-256: {path}"
            )
        if _file_identity(os.fstat(descriptor)) != identity:
            raise ScreeningFeatureArtifactError(
                f"raw source changed while its SHA-256 was verified: {path}"
            )
    finally:
        os.close(descriptor)
    return _PreparedRawSource(
        reference=reference,
        path=path,
        identity=identity,
    )


def _open_descriptor(path: Path, *, label: str) -> tuple[int, _FileIdentity]:
    if path.is_symlink():
        raise ScreeningFeatureArtifactError(f"{label} cannot be a symbolic link")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ScreeningFeatureArtifactError(f"cannot open {label}: {path}") from error
    stat_result = os.fstat(descriptor)
    if not stat.S_ISREG(stat_result.st_mode):
        os.close(descriptor)
        raise ScreeningFeatureArtifactError(f"{label} must be a regular file")
    return descriptor, _file_identity(stat_result)


def _descriptor_sha256(descriptor: int) -> str:
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    while chunk := os.read(descriptor, _COPY_CHUNK_SIZE):
        digest.update(chunk)
    return digest.hexdigest()


def _prepare_footer_manifest(
    data_root: Path,
    source_manifest: _PreparedSourceManifest,
) -> _PreparedFooterManifest:
    """Pair, parse, and rehash the exact manifests used by contract selection."""

    path = data_root.joinpath(*_FOOTER_MANIFEST_RELATIVE_URI.parts)
    if path.is_symlink():
        raise ScreeningFeatureArtifactError("footer manifest cannot be a symbolic link")
    try:
        resolved = path.resolve(strict=True)
    except FileNotFoundError as error:
        raise ScreeningFeatureArtifactError("canonical footer manifest does not exist") from error
    expected = data_root.joinpath(*_FOOTER_MANIFEST_RELATIVE_URI.parts)
    if resolved != expected:
        raise ScreeningFeatureArtifactError("footer manifest path escaped data_root")
    source_path = data_root.joinpath(*_SOURCE_MANIFEST_RELATIVE_URI.parts)
    try:
        bundle = load_source_manifest_bundle(resolved, source_path)
    except ManifestValidationError as error:
        raise ScreeningFeatureArtifactError(
            "footer/source manifests do not form one canonical paired bundle"
        ) from error
    expected_records = tuple(
        (
            record.reference.relative_uri,
            record.reference.source_date,
            record.byte_size,
            record.reference.sha256,
        )
        for record in source_manifest.records
    )
    paired_records = tuple(
        (record.relative_uri, record.source_date, record.byte_size, record.sha256)
        for record in bundle.records
    )
    if bundle.hash_manifest_sha256 != source_manifest.sha256 or paired_records != expected_records:
        raise ScreeningFeatureArtifactError("paired footer/source manifest identity drift")

    descriptor, identity = _open_descriptor(resolved, label="footer manifest")
    try:
        if identity.byte_size <= 0:
            raise ScreeningFeatureArtifactError("footer manifest cannot be empty")
        digest = _descriptor_sha256(descriptor)
        if _file_identity(os.fstat(descriptor)) != identity:
            raise ScreeningFeatureArtifactError("footer manifest changed while being hashed")
    finally:
        os.close(descriptor)
    if digest != bundle.footer_manifest_sha256:
        raise ScreeningFeatureArtifactError("footer manifest changed after paired validation")
    return _PreparedFooterManifest(path=resolved, identity=identity, sha256=digest)


def _canonical_json_line(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _prepare_source_manifest(
    data_root: Path,
    calendar: Phase1AScreeningCalendar,
) -> _PreparedSourceManifest:
    path = data_root.joinpath(*_SOURCE_MANIFEST_RELATIVE_URI.parts)
    if path.is_symlink():
        raise ScreeningFeatureArtifactError("source SHA manifest cannot be a symbolic link")
    try:
        resolved = path.resolve(strict=True)
    except FileNotFoundError as error:
        raise ScreeningFeatureArtifactError(
            f"canonical source SHA manifest does not exist: {path}"
        ) from error
    if resolved != path:
        raise ScreeningFeatureArtifactError("canonical source SHA manifest path drift")
    _ensure_no_symlink(data_root, resolved, label="source SHA manifest")

    descriptor, identity = _open_descriptor(resolved, label="source SHA manifest")
    records: list[_SourceManifestRecord] = []
    digest = hashlib.sha256()
    previous_reference: RawSourceReference | None = None
    try:
        with os.fdopen(os.dup(descriptor), "rb") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                digest.update(raw_line)
                try:
                    document = json.loads(raw_line)
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise ScreeningFeatureArtifactError(
                        f"source SHA manifest line {line_number} is invalid JSON"
                    ) from error
                if not isinstance(document, dict) or set(document) != _SOURCE_MANIFEST_FIELDS:
                    raise ScreeningFeatureArtifactError(
                        f"source SHA manifest line {line_number} fields drift"
                    )
                if raw_line != _canonical_json_line(document):
                    raise ScreeningFeatureArtifactError(
                        f"source SHA manifest line {line_number} is not canonical JSONL"
                    )
                source_date = _parse_date(
                    document["source_date"],
                    label=f"source SHA manifest line {line_number} source_date",
                )
                reference = _normalize_source(
                    RawSourceReference(
                        source_date=source_date,
                        relative_uri=document["relative_uri"],
                        sha256=document["sha256"],
                    )
                )
                byte_size = document["byte_size"]
                if isinstance(byte_size, bool) or not isinstance(byte_size, int) or byte_size < 0:
                    raise ScreeningFeatureArtifactError(
                        f"source SHA manifest line {line_number} byte_size is invalid"
                    )
                if previous_reference is not None and (
                    reference.source_date <= previous_reference.source_date
                    or reference.relative_uri <= previous_reference.relative_uri
                ):
                    raise ScreeningFeatureArtifactError(
                        "source SHA manifest records must be unique and strictly ordered"
                    )
                records.append(_SourceManifestRecord(reference=reference, byte_size=byte_size))
                previous_reference = reference
        if _file_identity(os.fstat(descriptor)) != identity:
            raise ScreeningFeatureArtifactError("source SHA manifest changed while being verified")
    except OSError as error:
        raise ScreeningFeatureArtifactError(
            f"cannot verify source SHA manifest: {resolved}"
        ) from error
    finally:
        os.close(descriptor)

    actual_sha256 = digest.hexdigest()
    if actual_sha256 != calendar.source_manifest_sha256:
        raise ScreeningFeatureArtifactError(
            "source SHA manifest bytes differ from calendar identity"
        )
    expected_dates = tuple(sorted((*calendar.source_dates, *calendar.excluded_source_dates)))
    actual_dates = tuple(record.reference.source_date for record in records)
    if len(records) != calendar.source_record_count or actual_dates != expected_dates:
        raise ScreeningFeatureArtifactError(
            "source SHA manifest coverage differs from the screening calendar"
        )
    return _PreparedSourceManifest(
        path=resolved,
        identity=identity,
        records=tuple(records),
        sha256=actual_sha256,
    )


def _schema_sha256(schema: pa.Schema) -> str:
    metadata = {
        key.decode(): value.decode() for key, value in sorted((schema.metadata or {}).items())
    }
    document = {
        "fields": [
            {"name": field.name, "nullable": field.nullable, "type": str(field.type)}
            for field in schema
        ],
        "metadata": metadata,
    }
    return hashlib.sha256(
        json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _datetime_to_ns(value: datetime, *, label: str) -> int:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ScreeningFeatureArtifactError(f"{label} must be timezone-aware")
    utc = value.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = utc - epoch
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


def _all_equal(column: pa.ChunkedArray, expected: object) -> bool:
    return pc.all(pc.equal(column, pa.scalar(expected, type=column.type))).as_py() is True


def _expected_artifact_metadata(
    report: ScreeningFeatureBuildReport,
    *,
    granularity: str,
) -> dict[bytes, bytes]:
    return {
        b"systematic_fx.feature_version": FEATURE_VERSION.encode(),
        b"systematic_fx.formula_sha256": report.formula_sha256.encode(),
        b"systematic_fx.granularity": granularity.encode(),
        b"systematic_fx.price_scale": PRICE_SCALE.encode(),
        b"systematic_fx.tick_size_raw": str(TICK_SIZE_RAW).encode(),
        b"systematic_fx.screening_only": b"true",
        b"systematic_fx.research_eligible": b"false",
        b"systematic_fx.definition_status_available": b"false",
        b"systematic_fx.source_date": report.source_date.encode(),
        b"systematic_fx.source_sha256": report.source_sha256.encode(),
        b"systematic_fx.source_schema_sha256": report.source_schema_sha256.encode(),
        b"systematic_fx.source_manifest_sha256": report.source_manifest_sha256.encode(),
        b"systematic_fx.qc_manifest_sha256": report.qc_manifest_sha256.encode(),
        b"systematic_fx.qc_config_sha256": report.qc_config_sha256.encode(),
        b"systematic_fx.calendar_sha256": report.calendar_sha256.encode(),
        b"systematic_fx.code_snapshot_sha256": report.code_snapshot_sha256.encode(),
        b"systematic_fx.config_sha256": report.config_sha256.encode(),
        b"systematic_fx.source_start_boundary_policy": b"EXCLUDE_PARTIAL_RIGHT_CLOSED",
        b"systematic_fx.source_end_boundary_policy": b"UNPROVEN_CLOSED_BOUNDARY",
        b"systematic_fx.contract_selection_sha256": report.contract_selection_sha256.encode(),
        b"systematic_fx.previous_volume_sha256": report.previous_volume_sha256.encode(),
        b"systematic_fx.previous_source_date": report.previous_source_date.encode(),
        b"systematic_fx.instrument_id": str(report.instrument_id).encode(),
        b"systematic_fx.contract": report.contract.encode(),
        b"systematic_fx.contract_month": report.contract_month.encode(),
        b"systematic_fx.previous_trade_rows": str(report.previous_trade_rows).encode(),
        b"systematic_fx.previous_trade_volume": str(report.previous_trade_volume).encode(),
    }


def _expected_artifact_relative_uri(
    *,
    directory_name: str,
    report: ScreeningFeatureBuildReport,
) -> str:
    return (
        f"derived/{directory_name}/version={FEATURE_VERSION}/contract={report.contract}/"
        f"source_date={report.source_date}/part-000.parquet"
    )


def _validate_artifact(
    *,
    data_root: Path,
    derived_root: Path,
    report: ScreeningFeatureBuildReport,
    artifact_report: ScreeningArtifactReport,
    partition_type: str,
    directory_name: str,
    granularity: str,
    base_schema: pa.Schema,
) -> VerifiedScreeningArtifact:
    label = f"{report.source_date} {partition_type} artifact"
    if not isinstance(artifact_report, ScreeningArtifactReport):
        raise ScreeningFeatureArtifactError(f"{label} report type is invalid")
    if artifact_report.disposition not in {"CREATED", "REUSED"}:
        raise ScreeningFeatureArtifactError(f"{label} disposition is invalid")
    report_sha = _require_sha256(artifact_report.sha256, label=f"{label} sha256")
    report_schema_sha = _require_sha256(
        artifact_report.schema_sha256,
        label=f"{label} schema_sha256",
    )
    if (
        isinstance(artifact_report.rows, bool)
        or not isinstance(artifact_report.rows, int)
        or artifact_report.rows <= 0
    ):
        raise ScreeningFeatureArtifactError(f"{label} rows must be a positive integer")

    requested = Path(artifact_report.path).expanduser()
    if requested.is_symlink():
        raise ScreeningFeatureArtifactError(f"{label} cannot be a symbolic link")
    try:
        path = requested.resolve(strict=True)
    except FileNotFoundError as error:
        raise ScreeningFeatureArtifactError(f"{label} does not exist: {requested}") from error
    relative_uri = _relative_to(path, data_root, label=label)
    _ensure_no_symlink(data_root, path, label=label)
    expected_relative_uri = _expected_artifact_relative_uri(
        directory_name=directory_name,
        report=report,
    )
    if relative_uri != expected_relative_uri:
        raise ScreeningFeatureArtifactError(
            f"{label} path must be exactly data/{expected_relative_uri}"
        )

    descriptor, identity = _open_descriptor(path, label=label)
    minimum: datetime
    maximum: datetime
    try:
        actual_sha = _descriptor_sha256(descriptor)
        os.lseek(descriptor, 0, os.SEEK_SET)
        with os.fdopen(os.dup(descriptor), "rb") as handle:
            parquet = pq.ParquetFile(handle)
            schema = parquet.schema_arrow
            if schema.remove_metadata() != base_schema.remove_metadata():
                raise ScreeningFeatureArtifactError(f"{label} fields differ from frozen schema")
            expected_metadata = _expected_artifact_metadata(report, granularity=granularity)
            if (schema.metadata or {}) != expected_metadata:
                raise ScreeningFeatureArtifactError(f"{label} schema metadata drift")
            if _schema_sha256(schema) != report_schema_sha:
                raise ScreeningFeatureArtifactError(f"{label} schema SHA-256 differs from report")
            if parquet.metadata.num_rows != artifact_report.rows:
                raise ScreeningFeatureArtifactError(f"{label} row count differs from report")
            identity_table = parquet.read(
                columns=(
                    "feature_version",
                    "screening_only",
                    "definition_status_available",
                    "source_date",
                    "contract",
                    "instrument_id",
                    "bucket_end",
                    "signal_input_valid" if granularity == "5m" else "valid_second",
                )
            )
        expected_identity: tuple[tuple[str, object], ...] = (
            ("feature_version", FEATURE_VERSION),
            ("screening_only", True),
            ("definition_status_available", False),
            ("source_date", date.fromisoformat(report.source_date)),
            ("contract", report.contract),
            ("instrument_id", report.instrument_id),
        )
        for column_name, expected in expected_identity:
            if not _all_equal(identity_table[column_name], expected):
                raise ScreeningFeatureArtifactError(
                    f"{label} contains inconsistent {column_name} values"
                )
        if granularity == "5m" and not _all_equal(identity_table["signal_input_valid"], False):
            raise ScreeningFeatureArtifactError(
                f"{label} must not claim signal_input_valid without definition/status data"
            )
        minimum_value = pc.min(identity_table["bucket_end"]).as_py()
        maximum_value = pc.max(identity_table["bucket_end"]).as_py()
        if not isinstance(minimum_value, datetime) or not isinstance(maximum_value, datetime):
            raise ScreeningFeatureArtifactError(f"{label} bucket_end bounds are invalid")
        minimum = minimum_value
        maximum = maximum_value
        if minimum.isoformat() != artifact_report.min_bucket_end:
            raise ScreeningFeatureArtifactError(f"{label} minimum bucket differs from report")
        if maximum.isoformat() != artifact_report.max_bucket_end:
            raise ScreeningFeatureArtifactError(f"{label} maximum bucket differs from report")
        if actual_sha != report_sha:
            raise ScreeningFeatureArtifactError(f"{label} bytes differ from report SHA-256")
        if _file_identity(os.fstat(descriptor)) != identity:
            raise ScreeningFeatureArtifactError(f"{label} changed while being verified")
    except (OSError, pa.ArrowException) as error:
        raise ScreeningFeatureArtifactError(f"cannot verify {label}: {path}") from error
    finally:
        os.close(descriptor)

    source_date = date.fromisoformat(report.source_date)
    start = datetime.combine(source_date, datetime.min.time(), tzinfo=UTC)
    end = start + timedelta(days=1)
    if not (start < minimum <= maximum < end):
        raise ScreeningFeatureArtifactError(f"{label} bucket range escapes source date")
    canonical_path = (
        derived_root
        / "registry"
        / FEATURE_BATCH_REGISTRY_VERSION
        / directory_name
        / "sha256"
        / report_sha[:2]
        / f"{report_sha}.parquet"
    )
    return VerifiedScreeningArtifact(
        source_date=source_date,
        partition_type=partition_type,
        granularity=granularity,
        original_path=path,
        original_relative_uri=relative_uri,
        canonical_path=canonical_path,
        canonical_relative_uri=_relative_to(canonical_path, data_root, label=label),
        sha256=report_sha,
        byte_size=identity.byte_size,
        row_count=artifact_report.rows,
        schema_sha256=report_schema_sha,
        min_event_time_ns=_datetime_to_ns(minimum, label=f"{label} minimum"),
        max_event_time_ns=_datetime_to_ns(maximum, label=f"{label} maximum"),
    )


def _validate_selection(
    selection: ContractSelectionResult,
    *,
    source: RawSourceReference,
    previous_source: RawSourceReference,
    report: ScreeningFeatureBuildReport | None,
) -> None:
    if not isinstance(selection, ContractSelectionResult):
        raise ScreeningFeatureArtifactError("BUILT entry selection must be ContractSelectionResult")
    if hashlib.sha256(selection.canonical_bytes).hexdigest() != selection.sha256:
        raise ScreeningFeatureArtifactError("contract selection canonical SHA-256 drift")
    if (
        hashlib.sha256(selection.previous_volume.canonical_bytes).hexdigest()
        != selection.previous_volume.sha256
    ):
        raise ScreeningFeatureArtifactError("previous-volume canonical SHA-256 drift")
    if selection.selected not in selection.candidates:
        raise ScreeningFeatureArtifactError("selected contract is absent from candidates")
    if (
        selection.eligible_source_date != source.source_date
        or selection.previous_source_date != previous_source.source_date
        or selection.previous_volume.source_date != previous_source.source_date
        or selection.eligible_source_sha256 != source.sha256
        or selection.previous_source_sha256 != previous_source.sha256
        or selection.previous_volume.source_sha256 != previous_source.sha256
        or selection.previous_source_date >= selection.eligible_source_date
    ):
        raise ScreeningFeatureArtifactError("selection source-date lineage drift")
    selected = selection.selected
    if selected.instrument_id < 0 or selected.instrument_id > 2**32 - 1:
        raise ScreeningFeatureArtifactError("selection provider instrument id is outside uint32")
    if report is not None and (
        selected.previous_trade_rows <= 0 or selected.previous_trade_volume <= 0
    ):
        raise ScreeningFeatureArtifactError("built selection requires positive prior volume")
    selection_document = selection.as_dict()
    previous_document = selection.previous_volume.as_dict()
    if (
        selection_document.get("artifact_schema") != CONTRACT_SELECTION_SCHEMA
        or selection_document.get("policy_version") != CONTRACT_SELECTION_POLICY_VERSION
        or selection_document.get("eligible_source_sha256") != source.sha256
        or selection_document.get("previous_source_sha256") != previous_source.sha256
        or selection_document.get("selected") != selected.as_dict()
        or selection_document.get("previous_volume_sha256") != selection.previous_volume.sha256
    ):
        raise ScreeningFeatureArtifactError("contract selection canonical document drift")
    if (
        previous_document.get("artifact_schema") != f"{CONTRACT_SELECTION_SCHEMA}.previous_volume"
        or previous_document.get("policy_version") != CONTRACT_SELECTION_POLICY_VERSION
        or previous_document.get("source_date") != previous_source.source_date.isoformat()
        or previous_document.get("source_sha256") != previous_source.sha256
    ):
        raise ScreeningFeatureArtifactError("previous-volume canonical document drift")
    if report is not None and (
        report.contract_selection_sha256 != selection.sha256
        or report.previous_volume_sha256 != selection.previous_volume.sha256
        or report.previous_source_date != previous_source.source_date.isoformat()
        or report.instrument_id != selected.instrument_id
        or report.contract != selected.raw_symbol
        or report.contract_month != selected.contract_month.isoformat()
        or report.previous_trade_rows != selected.previous_trade_rows
        or report.previous_trade_volume != selected.previous_trade_volume
    ):
        raise ScreeningFeatureArtifactError("builder report and contract selection differ")


def _validate_report(
    report: ScreeningFeatureBuildReport,
    *,
    source: RawSourceReference,
    previous_source: RawSourceReference,
    calendar: Phase1AScreeningCalendar,
    config_path: Path,
    config_sha256: str,
    code_snapshot_sha256: str,
) -> None:
    if not isinstance(report, ScreeningFeatureBuildReport):
        raise ScreeningFeatureArtifactError("BUILT entry report type is invalid")
    if (
        report.feature_version != FEATURE_VERSION
        or report.screening_only is not True
        or report.research_eligible is not False
        or report.definition_status_available is not False
    ):
        raise ScreeningFeatureArtifactError("builder report overstates screening authority")
    if (
        report.source_date != source.source_date.isoformat()
        or report.source_sha256 != source.sha256
        or report.previous_source_date != previous_source.source_date.isoformat()
    ):
        raise ScreeningFeatureArtifactError("builder report raw-source identity drift")
    if (
        report.source_schema_sha256 != calendar.schema_fingerprint
        or report.source_manifest_sha256 != calendar.source_manifest_sha256
        or report.qc_manifest_sha256 != calendar.qc_manifest_sha256
        or report.qc_config_sha256 != calendar.qc_config_sha256
        or report.calendar_sha256 != calendar.sha256
    ):
        raise ScreeningFeatureArtifactError("builder report calendar/source/QC provenance drift")
    if (
        report.code_snapshot_sha256 != code_snapshot_sha256
        or report.config_sha256 != config_sha256
        or report.formula_sha256 != FORMULA_SHA256
    ):
        raise ScreeningFeatureArtifactError("builder report code/config/formula provenance drift")
    try:
        actual_config_path = Path(report.config_path).expanduser().resolve(strict=True)
    except FileNotFoundError as error:
        raise ScreeningFeatureArtifactError("builder report config_path does not exist") from error
    if actual_config_path != config_path:
        raise ScreeningFeatureArtifactError("builder report config_path is not the frozen config")
    if not isinstance(report.contract, str) or _SAFE_SYMBOL.fullmatch(report.contract) is None:
        raise ScreeningFeatureArtifactError("builder report contract is not a safe 6E outright")
    contract_month = _parse_date(report.contract_month, label="report contract_month")
    if contract_month.day != 1:
        raise ScreeningFeatureArtifactError("report contract_month must be month-normalized")
    integer_fields = (
        "source_rows",
        "selected_rows",
        "late_rows_ignored",
        "source_start_partial_one_second_excluded",
        "unproven_closed_boundary_one_second_excluded",
        "unproven_closed_boundary_five_minute_excluded",
        "previous_trade_rows",
        "previous_trade_volume",
        "instrument_id",
    )
    for field_name in integer_fields:
        value = getattr(report, field_name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ScreeningFeatureArtifactError(f"report {field_name} must be non-negative")
    if (
        report.source_rows <= 0
        or report.selected_rows > report.source_rows
        or report.late_rows_ignored > report.selected_rows
        or report.previous_trade_rows <= 0
        or report.previous_trade_volume <= 0
        or report.instrument_id > 2**32 - 1
    ):
        raise ScreeningFeatureArtifactError("builder report row/volume/instrument counts drift")


def _normalize_report_for_manifest(report: ScreeningFeatureBuildReport) -> dict[str, object]:
    document = report.as_dict()
    for key in ("one_second", "five_minute"):
        artifact = document.get(key)
        if not isinstance(artifact, dict):  # pragma: no cover - dataclass invariant
            raise ScreeningFeatureArtifactError("builder artifact report must be an object")
        artifact.pop("disposition", None)
        artifact["publication_disposition_identity"] = "NON_SEMANTIC_VERIFIED_INPUT"
    return document


def _plain_run_spec(run_spec: RunSpec) -> dict[str, object]:
    value = json.loads(run_spec.canonical_json())
    if not isinstance(value, dict):  # pragma: no cover - RunSpec invariant
        raise ScreeningFeatureArtifactError("RunSpec canonical JSON must be an object")
    return value


def _validate_run_spec(
    run_spec: RunSpec,
    *,
    calendar: Phase1AScreeningCalendar,
    entries: Sequence[PreparedBatchEntry],
    config_sha256: str,
    footer_manifest_sha256: str,
) -> dict[str, object]:
    if not isinstance(run_spec, RunSpec):
        raise ScreeningFeatureArtifactError("run_spec must be a RunSpec")
    if run_spec.run_kind != "FEATURE_BUILD" or run_spec.experiment_id is not None:
        raise ScreeningFeatureArtifactError(
            "screening features require a campaign-level FEATURE_BUILD RunSpec"
        )
    if run_spec.direction != "BOTH":
        raise ScreeningFeatureArtifactError("feature build direction must be BOTH")
    if (
        run_spec.eligible_calendar_version != CALENDAR_VERSION
        or run_spec.eligible_calendar_sha256 != calendar.sha256
        or run_spec.feature_version != FEATURE_VERSION
        or run_spec.feature_sha256 != config_sha256
    ):
        raise ScreeningFeatureArtifactError("RunSpec calendar/feature identity drift")
    expected_manifests = {
        _FOOTER_MANIFEST_KEY: footer_manifest_sha256,
        _SOURCE_MANIFEST_KEY: calendar.source_manifest_sha256,
        _QC_MANIFEST_KEY: calendar.qc_manifest_sha256,
    }
    if dict(run_spec.source_manifest_hashes) != expected_manifests:
        raise ScreeningFeatureArtifactError("RunSpec footer/source/QC manifest identities drift")

    canonical = _plain_run_spec(run_spec)
    parameters = canonical.get("parameters")
    if not isinstance(parameters, dict):  # pragma: no cover - RunSpec validates mappings
        raise ScreeningFeatureArtifactError("RunSpec parameters must be an object")
    batch_parameters = [
        {
            "current_source": _source_document(entry.source),
            "no_entry_reason": entry.no_entry_reason,
            "previous_source": (
                _source_document(entry.previous_source)
                if entry.previous_source is not None
                else None
            ),
            "previous_volume_sha256": (
                entry.selection.previous_volume.sha256 if entry.selection is not None else None
            ),
            "previous_volume_document": (
                entry.selection.previous_volume.as_dict() if entry.selection is not None else None
            ),
            "selection_sha256": (entry.selection.sha256 if entry.selection is not None else None),
            "selection_document": (
                entry.selection.as_dict() if entry.selection is not None else None
            ),
            "status": entry.status.value,
        }
        for entry in entries
    ]
    expected_parameters: dict[str, object] = {
        "batch_entries": batch_parameters,
        "batch_source_dates": [entry.source.source_date.isoformat() for entry in entries],
        "batch_status_by_date": {
            entry.source.source_date.isoformat(): entry.status.value for entry in entries
        },
        "config_sha256": config_sha256,
        "definition_status_available": False,
        "formula_sha256": FORMULA_SHA256,
        "previous_volume_sha256_by_date": {
            entry.source.source_date.isoformat(): entry.selection.previous_volume.sha256
            for entry in entries
            if entry.selection is not None
        },
        "research_eligible": False,
        "screening_only": True,
        "selection_sha256_by_date": {
            entry.source.source_date.isoformat(): entry.selection.sha256
            for entry in entries
            if entry.selection is not None
        },
        "no_entry_reason_by_date": {
            entry.source.source_date.isoformat(): entry.no_entry_reason
            for entry in entries
            if entry.status is BatchEntryStatus.RECORDED_NO_ENTRY
        },
    }
    mismatches = [
        key for key, expected in expected_parameters.items() if parameters.get(key) != expected
    ]
    if mismatches:
        raise ScreeningFeatureArtifactError(
            "RunSpec feature-build parameters drift: " + ", ".join(sorted(mismatches))
        )
    return canonical


def _source_document(reference: RawSourceReference) -> dict[str, object]:
    return {
        "relative_uri": reference.relative_uri,
        "sha256": reference.sha256,
        "source_date": reference.source_date.isoformat(),
    }


def prepare_phase1a_screening_feature_batch(
    *,
    data_root: Path | str,
    calendar: Phase1AScreeningCalendar,
    run_spec: RunSpec,
    entries: Sequence[ScreeningFeatureBatchEntry],
    dataset_key: str = DEFAULT_DATASET_KEY,
) -> PreparedScreeningFeatureBatch:
    """Verify one five-date batch and construct its canonical manifest."""

    root, derived_root = _resolve_data_root(data_root)
    dataset_key = _require_nonempty(dataset_key, label="dataset_key")
    if not isinstance(calendar, Phase1AScreeningCalendar):
        raise ScreeningFeatureArtifactError("calendar must be Phase1AScreeningCalendar")
    if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
        raise ScreeningFeatureArtifactError("entries must be an ordered sequence")
    if len(entries) != FEATURE_BATCH_SIZE:
        raise ScreeningFeatureArtifactError(
            f"feature batch must contain exactly {FEATURE_BATCH_SIZE} source dates"
        )
    if any(not isinstance(entry, ScreeningFeatureBatchEntry) for entry in entries):
        raise ScreeningFeatureArtifactError("entries contain an invalid batch entry")

    source_manifest = _prepare_source_manifest(root, calendar)
    footer_manifest = _prepare_footer_manifest(root, source_manifest)
    footer_manifest_sha256 = footer_manifest.sha256
    manifest_positions = {
        record.reference.source_date: index for index, record in enumerate(source_manifest.records)
    }
    normalized_sources = tuple(_normalize_source(entry.source) for entry in entries)
    source_dates = tuple(source.source_date for source in normalized_sources)
    if any(left >= right for left, right in pairwise(source_dates)):
        raise ScreeningFeatureArtifactError("batch source dates must be strictly increasing")
    positions = [
        calendar.source_dates.index(day) if day in calendar.source_dates else -1
        for day in source_dates
    ]
    if positions[0] < 0 or positions != list(
        range(positions[0], positions[0] + FEATURE_BATCH_SIZE)
    ):
        raise ScreeningFeatureArtifactError("batch dates must be one contiguous calendar slice")
    for source in normalized_sources:
        manifest_position = manifest_positions.get(source.source_date)
        if (
            manifest_position is None
            or source_manifest.records[manifest_position].reference != source
        ):
            raise ScreeningFeatureArtifactError(
                "batch current source differs from canonical source SHA manifest"
            )

    frozen_config = load_phase1a_screening_config(DEFAULT_CONFIG_PATH)
    config_path = frozen_config.path.resolve(strict=True)
    config_sha256 = frozen_config.sha256
    raw_references: dict[date, RawSourceReference] = {}
    normalized_entries: list[PreparedBatchEntry] = []
    artifacts: list[VerifiedScreeningArtifact] = []
    code_snapshot_sha256 = run_spec.code_snapshot_sha256

    for input_entry, source in zip(entries, normalized_sources, strict=True):
        try:
            status = BatchEntryStatus(input_entry.status)
        except (TypeError, ValueError) as error:
            raise ScreeningFeatureArtifactError("batch entry status is invalid") from error
        manifest_position = manifest_positions[source.source_date]
        expected_previous_source = (
            source_manifest.records[manifest_position - 1].reference
            if manifest_position > 0
            else None
        )
        previous_source = (
            _normalize_source(input_entry.previous_source)
            if input_entry.previous_source is not None
            else None
        )
        if previous_source != expected_previous_source:
            raise ScreeningFeatureArtifactError(
                "previous_source must be the exact preceding canonical source manifest record"
            )
        existing_source = raw_references.get(source.source_date)
        if existing_source is not None and existing_source != source:
            raise ScreeningFeatureArtifactError("current raw-source identity conflicts")
        raw_references[source.source_date] = source
        if status is BatchEntryStatus.RECORDED_NO_ENTRY:
            reason = _normalize_no_entry_reason(input_entry.no_entry_reason)
            if input_entry.report is not None:
                raise ScreeningFeatureArtifactError(
                    "RECORDED_NO_ENTRY cannot carry a feature build report"
                )
            if previous_source is None:
                if reason != "MISSING_PREVIOUS_COMPLETED_SESSION":
                    raise ScreeningFeatureArtifactError(
                        "no-entry without previous source must be "
                        "MISSING_PREVIOUS_COMPLETED_SESSION"
                    )
                if input_entry.selection is not None:
                    raise ScreeningFeatureArtifactError(
                        "selection audit requires its previous raw source"
                    )
            else:
                if reason == "MISSING_PREVIOUS_COMPLETED_SESSION":
                    raise ScreeningFeatureArtifactError(
                        "MISSING_PREVIOUS_COMPLETED_SESSION is valid only for the first "
                        "canonical source manifest record"
                    )
                prior_identity = raw_references.get(previous_source.source_date)
                if prior_identity is not None and prior_identity != previous_source:
                    raise ScreeningFeatureArtifactError("previous raw-source identity conflicts")
                raw_references[previous_source.source_date] = previous_source
                if input_entry.selection is not None:
                    _validate_selection(
                        input_entry.selection,
                        source=source,
                        previous_source=previous_source,
                        report=None,
                    )
            normalized_entries.append(
                PreparedBatchEntry(
                    source=source,
                    status=status,
                    previous_source=previous_source,
                    report=None,
                    selection=input_entry.selection,
                    no_entry_reason=reason,
                    artifacts=(),
                )
            )
            continue

        if input_entry.no_entry_reason is not None:
            raise ScreeningFeatureArtifactError("BUILT entry no_entry_reason must be None")
        if input_entry.report is None or input_entry.selection is None:
            raise ScreeningFeatureArtifactError("BUILT entry requires report and selection")
        if previous_source is None:
            raise ScreeningFeatureArtifactError(
                "the first canonical source manifest record must be RECORDED_NO_ENTRY"
            )
        existing_previous = raw_references.get(previous_source.source_date)
        if existing_previous is not None and existing_previous != previous_source:
            raise ScreeningFeatureArtifactError("previous raw-source identity conflicts")
        raw_references[previous_source.source_date] = previous_source
        report = input_entry.report
        _validate_report(
            report,
            source=source,
            previous_source=previous_source,
            calendar=calendar,
            config_path=config_path,
            config_sha256=config_sha256,
            code_snapshot_sha256=code_snapshot_sha256,
        )
        _validate_selection(
            input_entry.selection,
            source=source,
            previous_source=previous_source,
            report=report,
        )
        raw_path = root.joinpath("mbp-10", *PurePosixPath(source.relative_uri).parts)
        try:
            actual_report_source = Path(report.source_path).expanduser().resolve(strict=True)
            expected_report_source = raw_path.resolve(strict=True)
        except FileNotFoundError as error:
            raise ScreeningFeatureArtifactError("builder report raw source disappeared") from error
        if actual_report_source != expected_report_source:
            raise ScreeningFeatureArtifactError("builder report source_path drift")
        entry_artifacts = (
            _validate_artifact(
                data_root=root,
                derived_root=derived_root,
                report=report,
                artifact_report=report.one_second,
                partition_type="FEATURES_1S",
                directory_name="features_1s",
                granularity="1s",
                base_schema=ONE_SECOND_SCHEMA,
            ),
            _validate_artifact(
                data_root=root,
                derived_root=derived_root,
                report=report,
                artifact_report=report.five_minute,
                partition_type="RESEARCH_5M",
                directory_name="research_5m",
                granularity="5m",
                base_schema=FIVE_MINUTE_SCHEMA,
            ),
        )
        artifacts.extend(entry_artifacts)
        normalized_entries.append(
            PreparedBatchEntry(
                source=source,
                status=status,
                previous_source=previous_source,
                report=report,
                selection=input_entry.selection,
                no_entry_reason=None,
                artifacts=entry_artifacts,
            )
        )

    prepared_entries = tuple(normalized_entries)
    canonical_run_spec = _validate_run_spec(
        run_spec,
        calendar=calendar,
        entries=prepared_entries,
        config_sha256=config_sha256,
        footer_manifest_sha256=footer_manifest_sha256,
    )
    manifest_records_by_date = {
        record.reference.source_date: record for record in source_manifest.records
    }
    prepared_raw_sources = tuple(
        _prepare_raw_source(
            root,
            reference,
            expected_byte_size=manifest_records_by_date[reference.source_date].byte_size,
        )
        for reference in sorted(
            raw_references.values(),
            key=lambda value: (value.source_date, value.relative_uri),
        )
    )

    entry_documents: list[dict[str, object]] = []
    for entry in prepared_entries:
        if entry.status is BatchEntryStatus.RECORDED_NO_ENTRY:
            entry_documents.append(
                {
                    "current_source": _source_document(entry.source),
                    "no_entry_reason": entry.no_entry_reason,
                    "previous_source": (
                        _source_document(entry.previous_source)
                        if entry.previous_source is not None
                        else None
                    ),
                    "selection_audit": (
                        {
                            "contract_selection_sha256": entry.selection.sha256,
                            "previous_volume_document": (entry.selection.previous_volume.as_dict()),
                            "previous_volume_sha256": entry.selection.previous_volume.sha256,
                            "policy_version": CONTRACT_SELECTION_POLICY_VERSION,
                            "selected": entry.selection.selected.as_dict(),
                            "selection_document": entry.selection.as_dict(),
                        }
                        if entry.selection is not None
                        else None
                    ),
                    "status": entry.status.value,
                }
            )
            continue
        assert entry.report is not None
        assert entry.selection is not None
        assert entry.previous_source is not None
        entry_documents.append(
            {
                "artifacts": [
                    {
                        "byte_size": artifact.byte_size,
                        "canonical_relative_uri": artifact.canonical_relative_uri,
                        "granularity": artifact.granularity,
                        "max_event_time_ns": artifact.max_event_time_ns,
                        "min_event_time_ns": artifact.min_event_time_ns,
                        "original_relative_uri": artifact.original_relative_uri,
                        "partition_type": artifact.partition_type,
                        "row_count": artifact.row_count,
                        "schema_sha256": artifact.schema_sha256,
                        "sha256": artifact.sha256,
                    }
                    for artifact in entry.artifacts
                ],
                "builder_report": _normalize_report_for_manifest(entry.report),
                "current_source": _source_document(entry.source),
                "previous_source": _source_document(entry.previous_source),
                "selection": {
                    "contract_selection_sha256": entry.selection.sha256,
                    "previous_volume_document": entry.selection.previous_volume.as_dict(),
                    "previous_volume_sha256": entry.selection.previous_volume.sha256,
                    "policy_version": CONTRACT_SELECTION_POLICY_VERSION,
                    "selected": entry.selection.selected.as_dict(),
                    "selection_document": entry.selection.as_dict(),
                },
                "status": entry.status.value,
            }
        )
    manifest_document: dict[str, object] = {
        "artifact_schema": FEATURE_BATCH_MANIFEST_SCHEMA,
        "authority": {
            "definition_status_available": False,
            "research_eligible": False,
            "screening_only": True,
            "validation_scope": "BYTE_SCHEMA_METADATA_AND_LINEAGE_ONLY",
        },
        "batch": {
            "entry_count": FEATURE_BATCH_SIZE,
            "entries": entry_documents,
            "source_dates": [day.isoformat() for day in source_dates],
        },
        "dataset_key": dataset_key,
        "provenance": {
            "calendar_sha256": calendar.sha256,
            "calendar_version": CALENDAR_VERSION,
            "code_snapshot_sha256": code_snapshot_sha256,
            "config_sha256": config_sha256,
            "formula_sha256": FORMULA_SHA256,
            "footer_manifest": {
                "relative_uri": _FOOTER_MANIFEST_RELATIVE_URI.as_posix(),
                "sha256": footer_manifest_sha256,
            },
            "footer_manifest_sha256": footer_manifest_sha256,
            "qc_config_sha256": calendar.qc_config_sha256,
            "qc_manifest_sha256": calendar.qc_manifest_sha256,
            "source_manifest": {
                "byte_size": source_manifest.identity.byte_size,
                "relative_uri": _SOURCE_MANIFEST_RELATIVE_URI.as_posix(),
                "sha256": source_manifest.sha256,
            },
            "source_manifest_sha256": calendar.source_manifest_sha256,
        },
        "run_spec": {
            "canonical_spec": canonical_run_spec,
            "run_fingerprint": run_spec.fingerprint,
        },
    }
    manifest_bytes = _canonical_json_line(manifest_document)
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    manifest_path = (
        derived_root
        / "manifests"
        / FEATURE_BATCH_REGISTRY_VERSION
        / f"sha256={manifest_sha256}.json"
    )
    return PreparedScreeningFeatureBatch(
        data_root=root,
        dataset_key=dataset_key,
        calendar=calendar,
        run_spec=run_spec,
        footer_manifest=footer_manifest,
        source_manifest=source_manifest,
        entries=prepared_entries,
        raw_sources=prepared_raw_sources,
        artifacts=tuple(artifacts),
        config_sha256=config_sha256,
        formula_sha256=FORMULA_SHA256,
        code_snapshot_sha256=code_snapshot_sha256,
        manifest_document=manifest_document,
        manifest_bytes=manifest_bytes,
        manifest_sha256=manifest_sha256,
        manifest_path=manifest_path,
    )


def _ensure_publish_directory(data_root: Path, directory: Path, *, label: str) -> None:
    relative = directory.relative_to(data_root)
    cursor = data_root
    for part in relative.parts:
        cursor /= part
        if cursor.is_symlink():
            raise ScreeningFeatureRegistryDriftError(
                f"{label} directory cannot traverse a symbolic link: {cursor}"
            )
        if cursor.exists() and not cursor.is_dir():
            raise ScreeningFeatureRegistryDriftError(
                f"{label} directory component is not a directory: {cursor}"
            )
        cursor.mkdir(exist_ok=True, mode=0o700)
    if directory.resolve(strict=True) != directory or not directory.is_relative_to(data_root):
        raise ScreeningFeatureRegistryDriftError(f"{label} directory escaped data_root")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _verify_existing_content(
    path: Path,
    *,
    expected_sha256: str,
    expected_size: int,
    label: str,
) -> None:
    descriptor, identity = _open_descriptor(path, label=label)
    try:
        if identity.byte_size != expected_size or _descriptor_sha256(descriptor) != expected_sha256:
            raise ScreeningFeatureRegistryDriftError(f"{label} content drift: {path}")
    finally:
        os.close(descriptor)


def _publish_snapshot(
    artifact: VerifiedScreeningArtifact,
    *,
    data_root: Path,
) -> None:
    target = artifact.canonical_path
    _ensure_publish_directory(data_root, target.parent, label="feature snapshot")
    if target.exists() or target.is_symlink():
        _verify_existing_content(
            target,
            expected_sha256=artifact.sha256,
            expected_size=artifact.byte_size,
            label="content-addressed feature snapshot",
        )
        return

    source_descriptor, source_identity = _open_descriptor(
        artifact.original_path,
        label="verified feature artifact",
    )
    temporary_descriptor = -1
    temporary = target.parent / f".{target.name}.uninitialized"
    try:
        if (
            source_identity.byte_size != artifact.byte_size
            or _descriptor_sha256(source_descriptor) != artifact.sha256
        ):
            raise ScreeningFeatureRegistryDriftError(
                f"feature artifact changed after preparation: {artifact.original_path}"
            )
        temporary_descriptor, temporary_name = tempfile.mkstemp(
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
        )
        temporary = Path(temporary_name)
        os.fchmod(temporary_descriptor, 0o400)
        os.lseek(source_descriptor, 0, os.SEEK_SET)
        while chunk := os.read(source_descriptor, _COPY_CHUNK_SIZE):
            remaining = memoryview(chunk)
            while remaining:
                written = os.write(temporary_descriptor, remaining)
                if written <= 0:
                    raise OSError("short write while publishing feature snapshot")
                remaining = remaining[written:]
        os.fsync(temporary_descriptor)
        if _file_identity(os.fstat(source_descriptor)) != source_identity:
            raise ScreeningFeatureRegistryDriftError(
                f"feature artifact changed during publication: {artifact.original_path}"
            )
        os.close(temporary_descriptor)
        temporary_descriptor = -1
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError:
            _verify_existing_content(
                target,
                expected_sha256=artifact.sha256,
                expected_size=artifact.byte_size,
                label="concurrent content-addressed feature snapshot",
            )
        _fsync_directory(target.parent)
    except OSError as error:
        raise ScreeningFeatureRegistryDriftError(
            f"cannot publish feature snapshot: {target}"
        ) from error
    finally:
        os.close(source_descriptor)
        if temporary_descriptor >= 0:
            os.close(temporary_descriptor)
        temporary.unlink(missing_ok=True)


def _publish_manifest(prepared: PreparedScreeningFeatureBatch) -> None:
    target = prepared.manifest_path
    _ensure_publish_directory(prepared.data_root, target.parent, label="feature manifest")
    if target.exists() or target.is_symlink():
        _verify_existing_content(
            target,
            expected_sha256=prepared.manifest_sha256,
            expected_size=len(prepared.manifest_bytes),
            label="content-addressed feature manifest",
        )
        return
    descriptor, temporary_name = tempfile.mkstemp(
        dir=target.parent,
        prefix=f".{target.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o400)
        content = memoryview(prepared.manifest_bytes)
        while content:
            written = os.write(descriptor, content)
            if written <= 0:
                raise OSError("short write while publishing feature manifest")
            content = content[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError:
            _verify_existing_content(
                target,
                expected_sha256=prepared.manifest_sha256,
                expected_size=len(prepared.manifest_bytes),
                label="concurrent content-addressed feature manifest",
            )
        _fsync_directory(target.parent)
    except OSError as error:
        raise ScreeningFeatureRegistryDriftError(
            f"cannot publish feature manifest: {target}"
        ) from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _publish_prepared(prepared: PreparedScreeningFeatureBatch) -> None:
    footer_manifest = prepared.footer_manifest
    descriptor, identity = _open_descriptor(
        footer_manifest.path,
        label="canonical footer manifest",
    )
    try:
        if (
            identity != footer_manifest.identity
            or _descriptor_sha256(descriptor) != footer_manifest.sha256
            or _file_identity(os.fstat(descriptor)) != identity
        ):
            raise ScreeningFeatureRegistryDriftError(
                "canonical footer manifest changed after preparation"
            )
    finally:
        os.close(descriptor)
    source_manifest = prepared.source_manifest
    descriptor, identity = _open_descriptor(
        source_manifest.path,
        label="canonical source SHA manifest",
    )
    try:
        if (
            identity != source_manifest.identity
            or _descriptor_sha256(descriptor) != source_manifest.sha256
            or _file_identity(os.fstat(descriptor)) != identity
        ):
            raise ScreeningFeatureRegistryDriftError(
                "canonical source SHA manifest changed after preparation"
            )
    finally:
        os.close(descriptor)
    for raw_source in prepared.raw_sources:
        try:
            current = _file_identity(raw_source.path.stat())
        except OSError as error:
            raise ScreeningFeatureRegistryDriftError(
                f"raw source disappeared before registration: {raw_source.path}"
            ) from error
        if current != raw_source.identity:
            raise ScreeningFeatureRegistryDriftError(
                f"raw source identity changed after preparation: {raw_source.path}"
            )
    for artifact in prepared.artifacts:
        _publish_snapshot(artifact, data_root=prepared.data_root)
    _publish_manifest(prepared)


def _assert_fields(
    *,
    label: str,
    row: Mapping[str, Any],
    expected: Mapping[str, object],
) -> None:
    mismatches = [key for key, value in expected.items() if row.get(key) != value]
    if mismatches:
        raise ScreeningFeatureRegistryDriftError(
            f"{label} immutable drift in fields: {', '.join(sorted(mismatches))}"
        )


def _row_or_error(row: dict[str, Any] | None, *, label: str) -> dict[str, Any]:
    if row is None:
        raise ScreeningFeatureRegistryDriftError(f"{label} does not exist")
    return row


def _require_single_row(rows: list[dict[str, Any]], *, label: str) -> dict[str, Any]:
    if len(rows) != 1:
        raise ScreeningFeatureRegistryDriftError(
            f"{label} resolved to {len(rows)} rows instead of one"
        )
    return rows[0]


@dataclass(frozen=True, slots=True)
class _ControlPlane:
    dataset_id: int
    campaign_id: int
    research_run_spec_id: int
    source_file_ids: Mapping[str, int]


def _verify_run_spec_row(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    prepared: PreparedScreeningFeatureBatch,
    campaign_id: int,
) -> int:
    spec = prepared.run_spec
    row = connection.execute(
        """
        SELECT research_run_spec_id, run_fingerprint, canonicalization_schema,
               canonicalization_version, campaign_id, experiment_id,
               parent_run_spec_id, run_kind, engine_version, canonical_spec,
               source_manifest_hashes, eligible_calendar_version,
               eligible_calendar_sha256, split_version, split_sha256,
               feature_version, feature_sha256, outcome_version, outcome_sha256,
               cost_version, cost_sha256, execution_version, execution_sha256,
               code_commit, code_snapshot_sha256, dependency_lock_sha256,
               deterministic_seed, direction
        FROM systematic_fx.research_run_specs
        WHERE run_fingerprint = %s
        FOR SHARE
        """,
        (spec.fingerprint,),
    ).fetchone()
    row = _row_or_error(row, label=f"FEATURE_BUILD RunSpec {spec.fingerprint}")
    _assert_fields(
        label=f"FEATURE_BUILD RunSpec {spec.fingerprint}",
        row=row,
        expected={
            "campaign_id": campaign_id,
            "canonical_spec": _plain_run_spec(spec),
            "canonicalization_schema": RUN_SPEC_SCHEMA,
            "canonicalization_version": RUN_SPEC_SCHEMA_VERSION,
            "code_commit": spec.code_commit,
            "code_snapshot_sha256": spec.code_snapshot_sha256,
            "cost_sha256": spec.cost_sha256,
            "cost_version": spec.cost_version,
            "dependency_lock_sha256": spec.dependency_lock_sha256,
            "deterministic_seed": Decimal(spec.random_seed),
            "direction": spec.direction,
            "eligible_calendar_sha256": spec.eligible_calendar_sha256,
            "eligible_calendar_version": spec.eligible_calendar_version,
            "engine_version": spec.engine_version,
            "execution_sha256": spec.execution_sha256,
            "execution_version": spec.execution_version,
            "experiment_id": None,
            "feature_sha256": spec.feature_sha256,
            "feature_version": spec.feature_version,
            "outcome_sha256": spec.outcome_sha256,
            "outcome_version": spec.outcome_version,
            "parent_run_spec_id": None,
            "run_fingerprint": spec.fingerprint,
            "run_kind": "FEATURE_BUILD",
            "source_manifest_hashes": dict(spec.source_manifest_hashes),
            "split_sha256": spec.split_sha256,
            "split_version": spec.split_version,
        },
    )
    return int(row["research_run_spec_id"])


def _load_control_plane(
    connection: psycopg.Connection[dict[str, Any]],
    prepared: PreparedScreeningFeatureBatch,
) -> _ControlPlane:
    dataset = connection.execute(
        """
        SELECT dataset_id, dataset_key, status, manifest_sha256
        FROM systematic_fx.datasets
        WHERE dataset_key = %s
        FOR SHARE
        """,
        (prepared.dataset_key,),
    ).fetchone()
    dataset = _row_or_error(dataset, label=f"dataset {prepared.dataset_key}")
    _assert_fields(
        label=f"dataset {prepared.dataset_key}",
        row=dataset,
        expected={
            "dataset_key": prepared.dataset_key,
            "manifest_sha256": prepared.calendar.source_manifest_sha256,
        },
    )
    if dataset["status"] not in _VALID_DATASET_STATUSES:
        raise ScreeningFeatureRegistryDriftError(
            f"dataset status is not usable: {dataset['status']}"
        )
    dataset_id = int(dataset["dataset_id"])

    campaign = connection.execute(
        """
        SELECT campaign_id, campaign_key, dataset_id, status,
               data_manifest_sha256, feature_version, outcome_version,
               cost_model_version, execution_model_version, code_commit,
               split_policy
        FROM systematic_fx.campaigns
        WHERE campaign_key = %s
        FOR SHARE
        """,
        (prepared.run_spec.campaign_id,),
    ).fetchone()
    campaign = _row_or_error(campaign, label=f"campaign {prepared.run_spec.campaign_id}")
    _assert_fields(
        label=f"campaign {prepared.run_spec.campaign_id}",
        row=campaign,
        expected={
            "campaign_key": prepared.run_spec.campaign_id,
            "code_commit": prepared.run_spec.code_commit,
            "cost_model_version": prepared.run_spec.cost_version,
            "data_manifest_sha256": prepared.calendar.source_manifest_sha256,
            "dataset_id": dataset_id,
            "execution_model_version": prepared.run_spec.execution_version,
            "feature_version": FEATURE_VERSION,
            "outcome_version": prepared.run_spec.outcome_version,
            "status": "DRAFT",
        },
    )
    split_policy = campaign.get("split_policy")
    if not isinstance(split_policy, Mapping):
        raise ScreeningFeatureRegistryDriftError("campaign split_policy must be an object")
    _assert_fields(
        label="campaign split policy",
        row=split_policy,
        expected={
            "calendar_sha256": prepared.calendar.sha256,
            "calendar_version": CALENDAR_VERSION,
            "definition_status_available": False,
            "pass_backtest_allowed": False,
            "screening_only": True,
            "split_sha256": prepared.run_spec.split_sha256,
            "split_version": prepared.run_spec.split_version,
        },
    )
    campaign_id = int(campaign["campaign_id"])
    research_run_spec_id = _verify_run_spec_row(
        connection,
        prepared=prepared,
        campaign_id=campaign_id,
    )

    current_reports = {
        entry.source.relative_uri: entry.report
        for entry in prepared.entries
        if entry.report is not None
    }
    source_file_ids: dict[str, int] = {}
    for raw_source in prepared.raw_sources:
        reference = raw_source.reference
        row = connection.execute(
            """
            SELECT source_file_id, dataset_id, source_date, relative_uri,
                   byte_size, sha256, row_count, parquet_schema_fingerprint, status
            FROM systematic_fx.source_files
            WHERE dataset_id = %s AND relative_uri = %s
            FOR SHARE
            """,
            (dataset_id, reference.relative_uri),
        ).fetchone()
        row = _row_or_error(row, label=f"source file {reference.relative_uri}")
        _assert_fields(
            label=f"source file {reference.relative_uri}",
            row=row,
            expected={
                "byte_size": raw_source.identity.byte_size,
                "dataset_id": dataset_id,
                "relative_uri": reference.relative_uri,
                "sha256": reference.sha256,
                "source_date": reference.source_date,
            },
        )
        if row["status"] not in _VALID_SOURCE_STATUSES:
            raise ScreeningFeatureRegistryDriftError(
                f"source status is not HASHED/VALIDATED: {reference.relative_uri}"
            )
        report = current_reports.get(reference.relative_uri)
        if report is not None:
            _assert_fields(
                label=f"current source evidence {reference.relative_uri}",
                row=row,
                expected={
                    "parquet_schema_fingerprint": report.source_schema_sha256,
                    "row_count": report.source_rows,
                },
            )
        source_file_ids[reference.relative_uri] = int(row["source_file_id"])
    if len(source_file_ids) != len(prepared.raw_sources):
        raise ScreeningFeatureRegistryDriftError("raw sources do not resolve one-to-one")
    return _ControlPlane(
        dataset_id=dataset_id,
        campaign_id=campaign_id,
        research_run_spec_id=research_run_spec_id,
        source_file_ids=source_file_ids,
    )


def _job_payload(
    prepared: PreparedScreeningFeatureBatch,
    *,
    research_run_spec_id: int,
) -> dict[str, object]:
    parameters = _plain_run_spec(prepared.run_spec)["parameters"]
    if not isinstance(parameters, dict):  # pragma: no cover - preparation invariant
        raise ScreeningFeatureRegistryDriftError("RunSpec parameters lost object identity")
    return {
        "artifact_schema": FEATURE_BATCH_MANIFEST_SCHEMA,
        "batch_entries": parameters["batch_entries"],
        "batch_source_dates": [entry.source.source_date.isoformat() for entry in prepared.entries],
        "batch_status_by_date": parameters["batch_status_by_date"],
        "calendar_sha256": prepared.calendar.sha256,
        "code_snapshot_sha256": prepared.code_snapshot_sha256,
        "config_sha256": prepared.config_sha256,
        "dataset_key": prepared.dataset_key,
        "definition_status_available": False,
        "formula_sha256": prepared.formula_sha256,
        "footer_manifest_sha256": prepared.footer_manifest_sha256,
        "manifest_sha256": prepared.manifest_sha256,
        "no_entry_reason_by_date": parameters["no_entry_reason_by_date"],
        "qc_config_sha256": prepared.calendar.qc_config_sha256,
        "qc_manifest_sha256": prepared.calendar.qc_manifest_sha256,
        "research_eligible": False,
        "research_run_spec_id": research_run_spec_id,
        "run_fingerprint": prepared.run_spec.fingerprint,
        "screening_only": True,
        "source_manifest_sha256": prepared.calendar.source_manifest_sha256,
    }


def _ensure_job(
    connection: psycopg.Connection[dict[str, Any]],
    prepared: PreparedScreeningFeatureBatch,
    *,
    control: _ControlPlane,
) -> tuple[int, bool]:
    job_key = f"phase1a-feature-build:v1:{prepared.manifest_sha256}"
    payload = _job_payload(
        prepared,
        research_run_spec_id=control.research_run_spec_id,
    )
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.jobs
            (job_key, dataset_id, job_type, status, idempotency_key, payload,
             attempts, max_attempts, started_at)
        VALUES (%s, %s, %s, 'RUNNING', %s, %s, 1, 1, statement_timestamp())
        ON CONFLICT DO NOTHING
        RETURNING job_id
        """,
        (
            job_key,
            control.dataset_id,
            _JOB_TYPE,
            job_key,
            Jsonb(payload),
        ),
    ).fetchone()
    rows = connection.execute(
        """
        SELECT job_id, job_key, dataset_id, job_type, status,
               idempotency_key, payload, result
        FROM systematic_fx.jobs
        WHERE job_key = %s OR idempotency_key = %s
        FOR UPDATE
        """,
        (job_key, job_key),
    ).fetchall()
    row = _require_single_row(rows, label=f"feature-build job {job_key}")
    _assert_fields(
        label=f"feature-build job {job_key}",
        row=row,
        expected={
            "dataset_id": control.dataset_id,
            "idempotency_key": job_key,
            "job_key": job_key,
            "job_type": _JOB_TYPE,
            "payload": payload,
        },
    )
    created = inserted is not None
    if created:
        if row["status"] != "RUNNING" or row["result"] != {}:
            raise ScreeningFeatureRegistryDriftError("new feature-build job state drift")
    elif row["status"] != "SUCCEEDED":
        raise ScreeningFeatureRegistryDriftError(
            f"existing feature-build job is not SUCCEEDED: {row['status']}"
        )
    return int(row["job_id"]), created


def _ensure_manifest_artifact(
    connection: psycopg.Connection[dict[str, Any]],
    prepared: PreparedScreeningFeatureBatch,
    *,
    job_id: int,
) -> tuple[int, bool]:
    artifact_key = f"phase1a-feature-build-manifest:v1:{prepared.manifest_sha256}"
    uri = prepared.manifest_path.as_uri()
    metadata: dict[str, object] = {
        "artifact_schema": FEATURE_BATCH_MANIFEST_SCHEMA,
        "batch_entry_count": FEATURE_BATCH_SIZE,
        "built_date_count": sum(
            entry.status is BatchEntryStatus.BUILT for entry in prepared.entries
        ),
        "calendar_sha256": prepared.calendar.sha256,
        "footer_manifest_sha256": prepared.footer_manifest_sha256,
        "partition_count": len(prepared.artifacts),
        "research_eligible": False,
        "run_fingerprint": prepared.run_spec.fingerprint,
        "screening_only": True,
        "validation_scope": "BYTE_SCHEMA_METADATA_AND_LINEAGE_ONLY",
    }
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.artifacts
            (artifact_key, artifact_type, uri, sha256, byte_size, media_type,
             producer_job_id, metadata)
        VALUES (%s, 'PHASE1A_FEATURE_BUILD_MANIFEST', %s, %s, %s,
                'application/json', %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING artifact_id
        """,
        (
            artifact_key,
            uri,
            prepared.manifest_sha256,
            len(prepared.manifest_bytes),
            job_id,
            Jsonb(metadata),
        ),
    ).fetchone()
    rows = connection.execute(
        """
        SELECT artifact_id, artifact_key, artifact_type, uri, sha256,
               byte_size, media_type, producer_job_id, metadata
        FROM systematic_fx.artifacts
        WHERE artifact_key = %s OR uri = %s
        FOR SHARE
        """,
        (artifact_key, uri),
    ).fetchall()
    row = _require_single_row(rows, label=f"manifest artifact {artifact_key}")
    _assert_fields(
        label=f"manifest artifact {artifact_key}",
        row=row,
        expected={
            "artifact_key": artifact_key,
            "artifact_type": "PHASE1A_FEATURE_BUILD_MANIFEST",
            "byte_size": len(prepared.manifest_bytes),
            "media_type": "application/json",
            "metadata": metadata,
            "producer_job_id": job_id,
            "sha256": prepared.manifest_sha256,
            "uri": uri,
        },
    )
    return int(row["artifact_id"]), inserted is not None


def _entry_for_artifact(
    prepared: PreparedScreeningFeatureBatch,
    artifact: VerifiedScreeningArtifact,
) -> PreparedBatchEntry:
    matches = [
        entry
        for entry in prepared.entries
        if entry.source.source_date == artifact.source_date and artifact in entry.artifacts
    ]
    if len(matches) != 1:  # pragma: no cover - preparation invariant
        raise ScreeningFeatureRegistryDriftError("artifact lost its unique batch entry")
    return matches[0]


def _partition_key(
    prepared: PreparedScreeningFeatureBatch,
    artifact: VerifiedScreeningArtifact,
) -> str:
    digest = hashlib.sha256(
        (
            f"{prepared.manifest_sha256}:{artifact.source_date.isoformat()}:"
            f"{artifact.partition_type}:{artifact.sha256}"
        ).encode("ascii")
    ).hexdigest()
    return f"phase1a-feature:v1:{artifact.partition_type.lower()}:{digest}"


def _partition_metadata(
    prepared: PreparedScreeningFeatureBatch,
    artifact: VerifiedScreeningArtifact,
    *,
    research_run_spec_id: int,
) -> dict[str, object]:
    entry = _entry_for_artifact(prepared, artifact)
    if entry.report is None or entry.selection is None or entry.previous_source is None:
        raise ScreeningFeatureRegistryDriftError("built artifact lost report/selection lineage")
    report = entry.report
    return {
        "artifact": {
            "byte_size": artifact.byte_size,
            "canonical_relative_uri": artifact.canonical_relative_uri,
            "granularity": artifact.granularity,
            "original_relative_uri": artifact.original_relative_uri,
            "schema_sha256": artifact.schema_sha256,
        },
        "authority": {
            "definition_status_available": False,
            "research_eligible": False,
            "screening_only": True,
            "validation_scope": "BYTE_SCHEMA_METADATA_AND_LINEAGE_ONLY",
        },
        "build_audit": {
            "late_rows_ignored": report.late_rows_ignored,
            "selected_rows": report.selected_rows,
            "source_rows": report.source_rows,
            "source_start_partial_one_second_excluded": (
                report.source_start_partial_one_second_excluded
            ),
            "unproven_closed_boundary_five_minute_excluded": (
                report.unproven_closed_boundary_five_minute_excluded
            ),
            "unproven_closed_boundary_one_second_excluded": (
                report.unproven_closed_boundary_one_second_excluded
            ),
        },
        "contract": {
            "contract_month": report.contract_month,
            "instrument_fk": None,
            "instrument_fk_reason": "PROVIDER_ID_RETAINED_IN_METADATA",
            "provider_instrument_id": report.instrument_id,
            "raw_symbol": report.contract,
        },
        "provenance": {
            "calendar_sha256": prepared.calendar.sha256,
            "code_snapshot_sha256": report.code_snapshot_sha256,
            "config_sha256": report.config_sha256,
            "contract_selection_sha256": entry.selection.sha256,
            "current_source": _source_document(entry.source),
            "formula_sha256": report.formula_sha256,
            "footer_manifest_sha256": prepared.footer_manifest_sha256,
            "previous_source": _source_document(entry.previous_source),
            "previous_trade_rows": report.previous_trade_rows,
            "previous_trade_volume": report.previous_trade_volume,
            "previous_volume_sha256": entry.selection.previous_volume.sha256,
            "qc_config_sha256": report.qc_config_sha256,
            "qc_manifest_sha256": report.qc_manifest_sha256,
            "research_run_spec_id": research_run_spec_id,
            "run_fingerprint": prepared.run_spec.fingerprint,
            "source_manifest_sha256": report.source_manifest_sha256,
            "source_schema_sha256": report.source_schema_sha256,
        },
    }


def _ensure_partition(
    connection: psycopg.Connection[dict[str, Any]],
    prepared: PreparedScreeningFeatureBatch,
    artifact: VerifiedScreeningArtifact,
    *,
    control: _ControlPlane,
    manifest_artifact_id: int,
    build_job_id: int,
) -> tuple[int, bool]:
    entry = _entry_for_artifact(prepared, artifact)
    if entry.previous_source is None:
        raise ScreeningFeatureRegistryDriftError("built partition lacks previous source")
    partition_key = _partition_key(prepared, artifact)
    uri = artifact.canonical_path.as_uri()
    metadata = _partition_metadata(
        prepared,
        artifact,
        research_run_spec_id=control.research_run_spec_id,
    )
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.derived_partitions
            (partition_key, dataset_id, instrument_id, partition_type,
             definition_version, source_date, uri, sha256, row_count,
             min_event_time_ns, max_event_time_ns, source_manifest_sha256,
             code_commit, config_sha256, manifest_artifact_id, build_job_id,
             status, metadata, validated_at)
        VALUES (%s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, 'VALIDATED', %s, statement_timestamp())
        ON CONFLICT DO NOTHING
        RETURNING derived_partition_id
        """,
        (
            partition_key,
            control.dataset_id,
            artifact.partition_type,
            FEATURE_VERSION,
            artifact.source_date,
            uri,
            artifact.sha256,
            artifact.row_count,
            artifact.min_event_time_ns,
            artifact.max_event_time_ns,
            prepared.calendar.source_manifest_sha256,
            prepared.run_spec.code_commit,
            prepared.config_sha256,
            manifest_artifact_id,
            build_job_id,
            Jsonb(metadata),
        ),
    ).fetchone()
    rows = connection.execute(
        """
        SELECT derived_partition_id, partition_key, dataset_id, instrument_id,
               partition_type, definition_version, source_date, uri, sha256,
               row_count, min_event_time_ns, max_event_time_ns,
               source_manifest_sha256, code_commit, config_sha256,
               manifest_artifact_id, build_job_id, status, metadata, validated_at
        FROM systematic_fx.derived_partitions
        WHERE partition_key = %s OR uri = %s
        FOR UPDATE
        """,
        (partition_key, uri),
    ).fetchall()
    row = _require_single_row(rows, label=f"derived partition {partition_key}")
    _assert_fields(
        label=f"derived partition {partition_key}",
        row=row,
        expected={
            "build_job_id": build_job_id,
            "code_commit": prepared.run_spec.code_commit,
            "config_sha256": prepared.config_sha256,
            "dataset_id": control.dataset_id,
            "definition_version": FEATURE_VERSION,
            "instrument_id": None,
            "manifest_artifact_id": manifest_artifact_id,
            "max_event_time_ns": artifact.max_event_time_ns,
            "metadata": metadata,
            "min_event_time_ns": artifact.min_event_time_ns,
            "partition_key": partition_key,
            "partition_type": artifact.partition_type,
            "row_count": artifact.row_count,
            "sha256": artifact.sha256,
            "source_date": artifact.source_date,
            "source_manifest_sha256": prepared.calendar.source_manifest_sha256,
            "status": "VALIDATED",
            "uri": uri,
        },
    )
    if row["validated_at"] is None:
        raise ScreeningFeatureRegistryDriftError(
            f"VALIDATED partition lacks validated_at: {partition_key}"
        )
    partition_id = int(row["derived_partition_id"])
    source_links = (
        (
            control.source_file_ids[entry.source.relative_uri],
            entry.source.sha256,
        ),
        (
            control.source_file_ids[entry.previous_source.relative_uri],
            entry.previous_source.sha256,
        ),
    )
    for source_file_id, source_sha256 in source_links:
        connection.execute(
            """
            INSERT INTO systematic_fx.derived_partition_sources
                (derived_partition_id, source_file_id, source_sha256)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (partition_id, source_file_id, source_sha256),
        )
    stored_links = connection.execute(
        """
        SELECT source_file_id, source_sha256
        FROM systematic_fx.derived_partition_sources
        WHERE derived_partition_id = %s
        ORDER BY source_file_id
        FOR SHARE
        """,
        (partition_id,),
    ).fetchall()
    expected_links = [
        {"source_file_id": source_file_id, "source_sha256": source_sha256}
        for source_file_id, source_sha256 in sorted(source_links)
    ]
    if stored_links != expected_links:
        raise ScreeningFeatureRegistryDriftError(f"derived source-link drift: {partition_key}")
    return partition_id, inserted is not None


def _complete_job(
    connection: psycopg.Connection[dict[str, Any]],
    prepared: PreparedScreeningFeatureBatch,
    *,
    job_id: int,
    created: bool,
    manifest_artifact_id: int,
    partition_ids: tuple[int, ...],
    research_run_spec_id: int,
) -> None:
    result: dict[str, object] = {
        "definition_status_available": False,
        "manifest_artifact_id": manifest_artifact_id,
        "manifest_sha256": prepared.manifest_sha256,
        "partition_ids": list(partition_ids),
        "research_eligible": False,
        "research_run_spec_id": research_run_spec_id,
        "run_fingerprint": prepared.run_spec.fingerprint,
        "screening_only": True,
        "validation_scope": "BYTE_SCHEMA_METADATA_AND_LINEAGE_ONLY",
    }
    if created:
        updated = connection.execute(
            """
            UPDATE systematic_fx.jobs
            SET status = 'SUCCEEDED', result = %s,
                finished_at = statement_timestamp()
            WHERE job_id = %s AND status = 'RUNNING'
            RETURNING job_id
            """,
            (Jsonb(result), job_id),
        ).fetchone()
        if updated is None:
            raise ScreeningFeatureRegistryDriftError("new feature-build job could not be completed")
    row = connection.execute(
        """
        SELECT status, result, started_at, finished_at
        FROM systematic_fx.jobs
        WHERE job_id = %s
        FOR SHARE
        """,
        (job_id,),
    ).fetchone()
    row = _row_or_error(row, label=f"feature-build job {job_id}")
    _assert_fields(
        label=f"feature-build job {job_id}",
        row=row,
        expected={"result": result, "status": "SUCCEEDED"},
    )
    if row["started_at"] is None or row["finished_at"] is None:
        raise ScreeningFeatureRegistryDriftError(
            "succeeded feature-build job lacks start/finish timestamps"
        )


def _register_prepared(
    connection: psycopg.Connection[dict[str, Any]],
    prepared: PreparedScreeningFeatureBatch,
) -> ScreeningFeatureRegistrationReport:
    connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (f"phase1a-feature:{prepared.dataset_key}:{prepared.manifest_sha256}",),
    )
    control = _load_control_plane(connection, prepared)
    job_id, created_job = _ensure_job(connection, prepared, control=control)
    manifest_artifact_id, created_manifest = _ensure_manifest_artifact(
        connection,
        prepared,
        job_id=job_id,
    )
    partition_results = tuple(
        _ensure_partition(
            connection,
            prepared,
            artifact,
            control=control,
            manifest_artifact_id=manifest_artifact_id,
            build_job_id=job_id,
        )
        for artifact in prepared.artifacts
    )
    partition_ids = tuple(partition_id for partition_id, _ in partition_results)
    _complete_job(
        connection,
        prepared,
        job_id=job_id,
        created=created_job,
        manifest_artifact_id=manifest_artifact_id,
        partition_ids=partition_ids,
        research_run_spec_id=control.research_run_spec_id,
    )
    return ScreeningFeatureRegistrationReport(
        dataset_id=control.dataset_id,
        campaign_id=control.campaign_id,
        research_run_spec_id=control.research_run_spec_id,
        build_job_id=job_id,
        manifest_artifact_id=manifest_artifact_id,
        partition_ids=partition_ids,
        source_file_ids=tuple(sorted(control.source_file_ids.items())),
        manifest_path=prepared.manifest_path,
        manifest_sha256=prepared.manifest_sha256,
        created_job=created_job,
        created_manifest_artifact=created_manifest,
        created_partitions=sum(created for _, created in partition_results),
    )


@_translate_psycopg_errors("Phase 1A screening-feature registration")
def register_phase1a_screening_feature_batch(
    database_url: str,
    *,
    data_root: Path | str,
    calendar: Phase1AScreeningCalendar,
    run_spec: RunSpec,
    entries: Sequence[ScreeningFeatureBatchEntry],
    dataset_key: str = DEFAULT_DATASET_KEY,
) -> ScreeningFeatureRegistrationReport:
    """Verify, publish, and atomically register one five-date batch.

    Campaign and the canonical FEATURE_BUILD RunSpec are immutable prerequisites.
    This function does not create or promote either, and it never changes dataset,
    source-file, campaign, or research-eligibility state.
    """

    _require_nonempty(database_url, label="database_url")
    prepared = prepare_phase1a_screening_feature_batch(
        data_root=data_root,
        calendar=calendar,
        run_spec=run_spec,
        entries=entries,
        dataset_key=dataset_key,
    )
    _publish_prepared(prepared)
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        connection.isolation_level = IsolationLevel.SERIALIZABLE
        with connection.transaction():
            return _register_prepared(connection, prepared)


__all__ = [
    "DEFAULT_DATASET_KEY",
    "FEATURE_BATCH_MANIFEST_SCHEMA",
    "FEATURE_BATCH_REGISTRY_VERSION",
    "FEATURE_BATCH_SIZE",
    "BatchEntryStatus",
    "PreparedBatchEntry",
    "PreparedScreeningFeatureBatch",
    "RawSourceReference",
    "ScreeningFeatureArtifactError",
    "ScreeningFeatureBatchEntry",
    "ScreeningFeatureRegistrationReport",
    "ScreeningFeatureRegistryDatabaseError",
    "ScreeningFeatureRegistryDriftError",
    "ScreeningFeatureRegistryError",
    "VerifiedScreeningArtifact",
    "prepare_phase1a_screening_feature_batch",
    "register_phase1a_screening_feature_batch",
]
