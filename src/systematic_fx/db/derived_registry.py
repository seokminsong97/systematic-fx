"""Content-addressed lineage registration for non-research pilot features.

The pilot builder publishes convenient working paths.  This registry verifies
those Parquet files before opening a database transaction, snapshots their
bytes to immutable content-addressed paths below ``data/derived``, writes one
canonical lineage manifest, and atomically records the build job, artifact,
partitions, and source links.

``VALIDATED`` in this module means that the artifact's structure and lineage
match the frozen pilot contract.  It deliberately does not mean that the
partition is eligible for strategy research; every registered partition has
``research_eligible=false`` in its metadata.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import psycopg
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.features.pilot import (
    FEATURE_VERSION,
    FIVE_MINUTE_SCHEMA,
    FIVE_MINUTE_SCHEMA_SHA256,
    FORMULA_SHA256,
    ONE_SECOND_SCHEMA,
    ONE_SECOND_SCHEMA_SHA256,
    ArtifactReport,
    PilotBuildReport,
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_SYMBOL = re.compile(r"^[A-Z0-9]+$")
_VALID_SOURCE_STATUSES = frozenset({"HASHED", "VALIDATED"})
_VALID_DATASET_STATUSES = frozenset({"VALIDATING", "READY"})
_COPY_CHUNK_SIZE = 1024 * 1024


class DerivedRegistryError(RuntimeError):
    """Pilot-derived artifacts could not be safely registered."""


class PilotArtifactValidationError(DerivedRegistryError):
    """An input, report field, path, or Parquet artifact is invalid."""


class DerivedRegistryDriftError(DerivedRegistryError):
    """An immutable content identity conflicts with existing state."""


class DerivedRegistryDatabaseError(DerivedRegistryError):
    """PostgreSQL rejected or could not complete the atomic registration."""


@dataclass(frozen=True)
class VerifiedPilotArtifact:
    """One physically verified pilot artifact before database access."""

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


@dataclass(frozen=True)
class PreparedPilotDerivedRegistration:
    """File-verified, content-addressed material ready for one transaction."""

    data_root: Path
    dataset_key: str
    source_relative_uri: str
    source_sha256: str
    source_manifest_sha256: str
    feature_version: str
    provider_instrument_id: int
    raw_symbol: str
    source_date: date
    formula_sha256: str
    config_sha256: str
    code_commit: str
    artifacts: tuple[VerifiedPilotArtifact, VerifiedPilotArtifact]
    manifest_document: dict[str, object]
    manifest_bytes: bytes
    manifest_sha256: str
    manifest_path: Path


@dataclass(frozen=True)
class PilotDerivedRegistrationReport:
    """Committed identities for one 1-second/5-minute pilot build."""

    dataset_id: int
    source_file_id: int
    build_job_id: int
    manifest_artifact_id: int
    features_1s_partition_id: int
    research_5m_partition_id: int
    manifest_path: Path
    manifest_sha256: str
    created_job: bool
    created_manifest_artifact: bool
    created_partitions: int

    def as_dict(self) -> dict[str, object]:
        return {
            "build_job_id": self.build_job_id,
            "created_job": self.created_job,
            "created_manifest_artifact": self.created_manifest_artifact,
            "created_partitions": self.created_partitions,
            "dataset_id": self.dataset_id,
            "features_1s_partition_id": self.features_1s_partition_id,
            "manifest_artifact_id": self.manifest_artifact_id,
            "manifest_path": str(self.manifest_path),
            "manifest_sha256": self.manifest_sha256,
            "research_5m_partition_id": self.research_5m_partition_id,
            "source_file_id": self.source_file_id,
        }


@dataclass(frozen=True)
class _FileIdentity:
    device: int
    inode: int
    byte_size: int
    mtime_ns: int
    ctime_ns: int


def _file_identity(value: os.stat_result) -> _FileIdentity:
    return _FileIdentity(
        device=value.st_dev,
        inode=value.st_ino,
        byte_size=value.st_size,
        mtime_ns=value.st_mtime_ns,
        ctime_ns=value.st_ctime_ns,
    )


def _require_nonempty(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PilotArtifactValidationError(f"{label} must be a non-empty string")
    return value.strip()


def _require_sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise PilotArtifactValidationError(f"{label} must be a lowercase SHA-256")
    return value


def _parse_source_date(value: date | str) -> date:
    if isinstance(value, datetime):
        raise PilotArtifactValidationError("source_date must not be a datetime")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise PilotArtifactValidationError("source_date must be an ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise PilotArtifactValidationError("source_date must be an ISO date") from exc


def _safe_relative_uri(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise PilotArtifactValidationError(f"{label} must be a safe relative URI")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise PilotArtifactValidationError(f"{label} must be a safe relative URI")
    return value


def _relative_to(path: Path, root: Path, *, label: str) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError as exc:
        raise PilotArtifactValidationError(f"{label} must remain below {root}") from exc


def _ensure_no_symlink(root: Path, path: Path, *, label: str) -> None:
    relative = path.relative_to(root)
    cursor = root
    for part in relative.parts:
        cursor = cursor / part
        if cursor.is_symlink():
            raise PilotArtifactValidationError(f"{label} contains a symbolic link: {cursor}")


def _resolve_data_root(value: Path | str) -> tuple[Path, Path]:
    requested = Path(value).expanduser()
    if requested.is_symlink():
        raise PilotArtifactValidationError("data_root must not be a symbolic link")
    try:
        root = requested.resolve(strict=True)
    except FileNotFoundError as exc:
        raise PilotArtifactValidationError(f"data_root does not exist: {requested}") from exc
    if not root.is_dir() or root.name != "data":
        raise PilotArtifactValidationError("data_root must be an existing directory named data")
    derived = root / "derived"
    if derived.is_symlink():
        raise PilotArtifactValidationError("data/derived must not be a symbolic link")
    if not derived.is_dir():
        raise PilotArtifactValidationError("data/derived must be an existing directory")
    return root, derived.resolve(strict=True)


def _datetime_to_ns(value: datetime) -> int:
    if value.tzinfo is None or value.utcoffset() is None:
        raise PilotArtifactValidationError("bucket_end must decode as timezone-aware UTC")
    utc = value.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = utc - epoch
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


def _all_equal(column: pa.ChunkedArray, value: object) -> bool:
    result = pc.all(pc.equal(column, pa.scalar(value, type=column.type))).as_py()
    return result is True


def _open_verified_descriptor(path: Path, *, label: str) -> tuple[int, _FileIdentity]:
    if path.is_symlink():
        raise PilotArtifactValidationError(f"{label} must not be a symbolic link")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise PilotArtifactValidationError(f"cannot open {label}: {path}") from exc
    identity = _file_identity(os.fstat(descriptor))
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise PilotArtifactValidationError(f"{label} must be a regular file")
    return descriptor, identity


def _descriptor_sha256(descriptor: int) -> str:
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    while chunk := os.read(descriptor, _COPY_CHUNK_SIZE):
        digest.update(chunk)
    return digest.hexdigest()


def _write_descriptor(descriptor: int, content: bytes) -> None:
    remaining = memoryview(content)
    while remaining:
        written = os.write(descriptor, remaining)
        if written <= 0:
            raise OSError("short write while publishing a content-addressed snapshot")
        remaining = remaining[written:]


def _validate_artifact(
    *,
    data_root: Path,
    derived_root: Path,
    artifact_report: ArtifactReport,
    partition_type: str,
    directory_name: str,
    granularity: str,
    expected_schema: pa.Schema,
    expected_schema_sha256: str,
    feature_version: str,
    provider_instrument_id: int,
    raw_symbol: str,
    source_date: date,
) -> VerifiedPilotArtifact:
    label = f"{partition_type} artifact"
    requested = Path(artifact_report.path).expanduser()
    try:
        path = requested.resolve(strict=True)
    except FileNotFoundError as exc:
        raise PilotArtifactValidationError(f"{label} does not exist: {requested}") from exc
    relative_uri = _relative_to(path, data_root, label=label)
    _ensure_no_symlink(data_root, path, label=label)
    expected_relative_uri = (
        f"derived/{directory_name}/version={feature_version}/contract={raw_symbol}/"
        f"source_date={source_date.isoformat()}/part-000.parquet"
    )
    if relative_uri != expected_relative_uri:
        raise PilotArtifactValidationError(
            f"{label} path must be exactly data/{expected_relative_uri}"
        )

    report_sha256 = _require_sha256(artifact_report.sha256, label=f"{label} report SHA-256")
    if artifact_report.schema_sha256 != expected_schema_sha256:
        raise PilotArtifactValidationError(f"{label} report schema SHA-256 is invalid")
    if isinstance(artifact_report.rows, bool) or not isinstance(artifact_report.rows, int):
        raise PilotArtifactValidationError(f"{label} report rows must be an integer")
    if artifact_report.rows <= 0:
        raise PilotArtifactValidationError(f"{label} must contain at least one row")

    descriptor, identity = _open_verified_descriptor(path, label=label)
    try:
        actual_sha256 = _descriptor_sha256(descriptor)
        os.lseek(descriptor, 0, os.SEEK_SET)
        with os.fdopen(os.dup(descriptor), "rb") as handle:
            parquet = pq.ParquetFile(handle)
            if parquet.schema_arrow != expected_schema:
                raise PilotArtifactValidationError(f"{label} schema does not match the pilot")
            if parquet.metadata.num_rows != artifact_report.rows:
                raise PilotArtifactValidationError(f"{label} row count differs from its report")
            identity_table = parquet.read(
                columns=(
                    "feature_version",
                    "research_eligible",
                    "source_date",
                    "contract",
                    "instrument_id",
                    "bucket_end",
                )
            )
        expected_identity: tuple[tuple[str, object], ...] = (
            ("feature_version", feature_version),
            ("research_eligible", False),
            ("source_date", source_date),
            ("contract", raw_symbol),
            ("instrument_id", provider_instrument_id),
        )
        for column_name, expected_value in expected_identity:
            if not _all_equal(identity_table[column_name], expected_value):
                raise PilotArtifactValidationError(
                    f"{label} contains inconsistent {column_name} values"
                )
        minimum = pc.min(identity_table["bucket_end"]).as_py()
        maximum = pc.max(identity_table["bucket_end"]).as_py()
        if not isinstance(minimum, datetime) or not isinstance(maximum, datetime):
            raise PilotArtifactValidationError(f"{label} has invalid bucket_end values")
        if minimum.isoformat() != artifact_report.min_bucket_end:
            raise PilotArtifactValidationError(f"{label} minimum time differs from its report")
        if maximum.isoformat() != artifact_report.max_bucket_end:
            raise PilotArtifactValidationError(f"{label} maximum time differs from its report")
        if actual_sha256 != report_sha256:
            raise PilotArtifactValidationError(f"{label} SHA-256 differs from its report")
        if _file_identity(os.fstat(descriptor)) != identity:
            raise PilotArtifactValidationError(f"{label} changed while it was verified")
    except (pa.ArrowException, OSError) as exc:
        raise PilotArtifactValidationError(f"cannot verify {label}: {path}") from exc
    finally:
        os.close(descriptor)

    canonical_path = (
        derived_root
        / "registry"
        / "pilot_v1"
        / directory_name
        / "sha256"
        / report_sha256[:2]
        / f"{report_sha256}.parquet"
    )
    return VerifiedPilotArtifact(
        partition_type=partition_type,
        granularity=granularity,
        original_path=path,
        original_relative_uri=relative_uri,
        canonical_path=canonical_path,
        canonical_relative_uri=_relative_to(canonical_path, data_root, label=label),
        sha256=report_sha256,
        byte_size=identity.byte_size,
        row_count=artifact_report.rows,
        schema_sha256=artifact_report.schema_sha256,
        min_event_time_ns=_datetime_to_ns(minimum),
        max_event_time_ns=_datetime_to_ns(maximum),
    )


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def prepare_pilot_derived_registration(
    *,
    data_root: Path | str,
    dataset_key: str,
    source_relative_uri: str,
    source_sha256: str,
    feature_version: str,
    provider_instrument_id: int,
    raw_symbol: str,
    source_date: date | str,
    formula_sha256: str,
    config_sha256: str,
    code_commit: str,
    source_manifest_sha256: str,
    one_second_artifact: ArtifactReport,
    five_minute_artifact: ArtifactReport,
) -> PreparedPilotDerivedRegistration:
    """Verify all files and construct immutable lineage before database access."""

    root, derived_root = _resolve_data_root(data_root)
    parsed_date = _parse_source_date(source_date)
    dataset_key = _require_nonempty(dataset_key, label="dataset_key")
    source_relative_uri = _safe_relative_uri(
        source_relative_uri,
        label="source_relative_uri",
    )
    source_sha256 = _require_sha256(source_sha256, label="source_sha256")
    source_manifest_sha256 = _require_sha256(
        source_manifest_sha256,
        label="source_manifest_sha256",
    )
    config_sha256 = _require_sha256(config_sha256, label="config_sha256")
    formula_sha256 = _require_sha256(formula_sha256, label="formula_sha256")
    code_commit = _require_nonempty(code_commit, label="code_commit")
    if feature_version != FEATURE_VERSION:
        raise PilotArtifactValidationError(
            f"feature_version must be the implemented pilot version {FEATURE_VERSION}"
        )
    if formula_sha256 != FORMULA_SHA256:
        raise PilotArtifactValidationError("formula_sha256 does not match the pilot formulas")
    if (
        isinstance(provider_instrument_id, bool)
        or not isinstance(provider_instrument_id, int)
        or not 0 <= provider_instrument_id <= 2**32 - 1
    ):
        raise PilotArtifactValidationError("provider_instrument_id must be uint32")
    if not isinstance(raw_symbol, str) or _SAFE_SYMBOL.fullmatch(raw_symbol) is None:
        raise PilotArtifactValidationError("raw_symbol must be a safe uppercase symbol")

    artifacts = (
        _validate_artifact(
            data_root=root,
            derived_root=derived_root,
            artifact_report=one_second_artifact,
            partition_type="FEATURES_1S",
            directory_name="features_1s",
            granularity="1s",
            expected_schema=ONE_SECOND_SCHEMA,
            expected_schema_sha256=ONE_SECOND_SCHEMA_SHA256,
            feature_version=feature_version,
            provider_instrument_id=provider_instrument_id,
            raw_symbol=raw_symbol,
            source_date=parsed_date,
        ),
        _validate_artifact(
            data_root=root,
            derived_root=derived_root,
            artifact_report=five_minute_artifact,
            partition_type="RESEARCH_5M",
            directory_name="research_5m",
            granularity="5m",
            expected_schema=FIVE_MINUTE_SCHEMA,
            expected_schema_sha256=FIVE_MINUTE_SCHEMA_SHA256,
            feature_version=feature_version,
            provider_instrument_id=provider_instrument_id,
            raw_symbol=raw_symbol,
            source_date=parsed_date,
        ),
    )
    manifest_document: dict[str, object] = {
        "artifact_schema": "systematic_fx.pilot_derived_lineage.v1",
        "dataset_key": dataset_key,
        "source": {
            "relative_uri": source_relative_uri,
            "sha256": source_sha256,
            "source_date": parsed_date.isoformat(),
            "source_manifest_sha256": source_manifest_sha256,
        },
        "definition": {
            "code_commit": code_commit,
            "config_sha256": config_sha256,
            "feature_version": feature_version,
            "formula_sha256": formula_sha256,
            "research_eligible": False,
            "validation_scope": "STRUCTURE_AND_LINEAGE_ONLY",
        },
        "instrument": {
            "instrument_fk": None,
            "provider_instrument_id": provider_instrument_id,
            "raw_symbol": raw_symbol,
        },
        "partitions": [
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
            for artifact in artifacts
        ],
    }
    manifest_bytes = _canonical_json_bytes(manifest_document)
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    manifest_path = (
        derived_root / "manifests" / "pilot_derived_registry_v1" / f"sha256={manifest_sha256}.json"
    )
    return PreparedPilotDerivedRegistration(
        data_root=root,
        dataset_key=dataset_key,
        source_relative_uri=source_relative_uri,
        source_sha256=source_sha256,
        source_manifest_sha256=source_manifest_sha256,
        feature_version=feature_version,
        provider_instrument_id=provider_instrument_id,
        raw_symbol=raw_symbol,
        source_date=parsed_date,
        formula_sha256=formula_sha256,
        config_sha256=config_sha256,
        code_commit=code_commit,
        artifacts=artifacts,
        manifest_document=manifest_document,
        manifest_bytes=manifest_bytes,
        manifest_sha256=manifest_sha256,
        manifest_path=manifest_path,
    )


def _verify_existing_bytes(path: Path, expected: bytes, *, label: str) -> None:
    if path.is_symlink() or not path.is_file():
        raise DerivedRegistryDriftError(f"{label} is not a regular immutable file: {path}")
    if path.read_bytes() != expected:
        raise DerivedRegistryDriftError(f"{label} content drift: {path}")


def _publish_bytes(path: Path, content: bytes, *, data_root: Path, label: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _ensure_no_symlink(data_root, path.parent, label=label)
    if path.exists() or path.is_symlink():
        _verify_existing_bytes(path, content, label=label)
        return
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o444)
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            _verify_existing_bytes(path, content, label=label)
    finally:
        temporary.unlink(missing_ok=True)


def _publish_snapshot(
    artifact: VerifiedPilotArtifact,
    *,
    data_root: Path,
) -> None:
    target = artifact.canonical_path
    target.parent.mkdir(parents=True, exist_ok=True)
    _ensure_no_symlink(data_root, target.parent, label="content-addressed snapshot")
    if target.exists() or target.is_symlink():
        descriptor, identity = _open_verified_descriptor(
            target,
            label="content-addressed snapshot",
        )
        try:
            if identity.byte_size != artifact.byte_size or (
                _descriptor_sha256(descriptor) != artifact.sha256
            ):
                raise DerivedRegistryDriftError(f"content-addressed snapshot drift: {target}")
        finally:
            os.close(descriptor)
        return

    source_descriptor, source_identity = _open_verified_descriptor(
        artifact.original_path,
        label=f"{artifact.partition_type} source artifact",
    )
    temporary_descriptor, temporary_name = tempfile.mkstemp(
        dir=target.parent,
        prefix=f".{target.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    digest = hashlib.sha256()
    byte_size = 0
    try:
        os.lseek(source_descriptor, 0, os.SEEK_SET)
        while chunk := os.read(source_descriptor, _COPY_CHUNK_SIZE):
            digest.update(chunk)
            byte_size += len(chunk)
            _write_descriptor(temporary_descriptor, chunk)
        os.fsync(temporary_descriptor)
        if _file_identity(os.fstat(source_descriptor)) != source_identity:
            raise PilotArtifactValidationError(
                f"{artifact.partition_type} artifact changed while it was snapshotted"
            )
        if byte_size != artifact.byte_size or digest.hexdigest() != artifact.sha256:
            raise PilotArtifactValidationError(
                f"{artifact.partition_type} artifact changed after verification"
            )
        os.fchmod(temporary_descriptor, 0o444)
        os.close(temporary_descriptor)
        temporary_descriptor = -1
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError:
            existing_descriptor, existing_identity = _open_verified_descriptor(
                target,
                label="content-addressed snapshot",
            )
            try:
                if existing_identity.byte_size != artifact.byte_size or (
                    _descriptor_sha256(existing_descriptor) != artifact.sha256
                ):
                    raise DerivedRegistryDriftError(f"content-addressed snapshot drift: {target}")
            finally:
                os.close(existing_descriptor)
    finally:
        os.close(source_descriptor)
        if temporary_descriptor >= 0:
            os.close(temporary_descriptor)
        temporary.unlink(missing_ok=True)


def _publish_prepared(prepared: PreparedPilotDerivedRegistration) -> None:
    for artifact in prepared.artifacts:
        _publish_snapshot(artifact, data_root=prepared.data_root)
    _publish_bytes(
        prepared.manifest_path,
        prepared.manifest_bytes,
        data_root=prepared.data_root,
        label="pilot lineage manifest",
    )


def _assert_fields(
    *,
    label: str,
    row: dict[str, Any],
    expected: dict[str, object],
) -> None:
    mismatches = [key for key, value in expected.items() if row.get(key) != value]
    if mismatches:
        raise DerivedRegistryDriftError(
            f"{label} immutable drift in fields: {', '.join(sorted(mismatches))}"
        )


def _require_single_row(rows: list[dict[str, Any]], *, label: str) -> dict[str, Any]:
    if len(rows) != 1:
        raise DerivedRegistryDriftError(f"{label} resolved to {len(rows)} rows instead of one")
    return rows[0]


def _load_dataset_and_source(
    connection: psycopg.Connection[dict[str, Any]],
    prepared: PreparedPilotDerivedRegistration,
) -> tuple[int, int]:
    dataset = connection.execute(
        """
        SELECT dataset_id, dataset_key, status, manifest_sha256
        FROM systematic_fx.datasets
        WHERE dataset_key = %s
        FOR SHARE
        """,
        (prepared.dataset_key,),
    ).fetchone()
    if dataset is None:
        raise DerivedRegistryDriftError(
            f"dataset must already be source-registered: {prepared.dataset_key}"
        )
    _assert_fields(
        label=f"dataset {prepared.dataset_key}",
        row=dataset,
        expected={
            "dataset_key": prepared.dataset_key,
            "manifest_sha256": prepared.source_manifest_sha256,
        },
    )
    if dataset["status"] not in _VALID_DATASET_STATUSES:
        raise DerivedRegistryDriftError(
            f"dataset status must already be VALIDATING or READY: {dataset['status']}"
        )
    dataset_id = int(dataset["dataset_id"])

    source = connection.execute(
        """
        SELECT source_file_id, dataset_id, source_date, relative_uri, sha256, status
        FROM systematic_fx.source_files
        WHERE dataset_id = %s AND relative_uri = %s
        FOR SHARE
        """,
        (dataset_id, prepared.source_relative_uri),
    ).fetchone()
    if source is None:
        raise DerivedRegistryDriftError(
            f"source file must already be HASHED: {prepared.source_relative_uri}"
        )
    _assert_fields(
        label=f"source file {prepared.source_relative_uri}",
        row=source,
        expected={
            "dataset_id": dataset_id,
            "source_date": prepared.source_date,
            "relative_uri": prepared.source_relative_uri,
            "sha256": prepared.source_sha256,
        },
    )
    if source["status"] not in _VALID_SOURCE_STATUSES:
        raise DerivedRegistryDriftError(
            f"source status must already be HASHED or VALIDATED: {source['status']}"
        )
    return dataset_id, int(source["source_file_id"])


def _ensure_job(
    connection: psycopg.Connection[dict[str, Any]],
    prepared: PreparedPilotDerivedRegistration,
    dataset_id: int,
) -> tuple[int, bool, dict[str, object]]:
    job_key = f"pilot-derived-build:v1:{prepared.manifest_sha256}"
    idempotency_key = f"pilot-derived-build:v1:{prepared.manifest_sha256}"
    payload: dict[str, object] = {
        "dataset_key": prepared.dataset_key,
        "feature_version": prepared.feature_version,
        "manifest_sha256": prepared.manifest_sha256,
        "provider_instrument_id": prepared.provider_instrument_id,
        "raw_symbol": prepared.raw_symbol,
        "research_eligible": False,
        "source_date": prepared.source_date.isoformat(),
        "source_relative_uri": prepared.source_relative_uri,
        "validation_scope": "STRUCTURE_AND_LINEAGE_ONLY",
    }
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.jobs
            (job_key, dataset_id, job_type, status, idempotency_key, payload,
             attempts, max_attempts, started_at)
        VALUES (%s, %s, 'BUILD_PILOT_DERIVED', 'RUNNING', %s, %s, 1, 1,
                statement_timestamp())
        ON CONFLICT DO NOTHING
        RETURNING job_id
        """,
        (job_key, dataset_id, idempotency_key, Jsonb(payload)),
    ).fetchone()
    rows = connection.execute(
        """
        SELECT job_id, job_key, dataset_id, job_type, status, idempotency_key,
               payload, result
        FROM systematic_fx.jobs
        WHERE job_key = %s OR idempotency_key = %s
        FOR UPDATE
        """,
        (job_key, idempotency_key),
    ).fetchall()
    row = _require_single_row(rows, label=f"build job {job_key}")
    _assert_fields(
        label=f"build job {job_key}",
        row=row,
        expected={
            "dataset_id": dataset_id,
            "idempotency_key": idempotency_key,
            "job_key": job_key,
            "job_type": "BUILD_PILOT_DERIVED",
            "payload": payload,
        },
    )
    created = inserted is not None
    if created:
        if row["status"] != "RUNNING" or row["result"] != {}:
            raise DerivedRegistryDriftError("new pilot build job has invalid state")
    elif row["status"] != "SUCCEEDED":
        raise DerivedRegistryDriftError(
            f"existing pilot build job is not SUCCEEDED: {row['status']}"
        )
    return int(row["job_id"]), created, payload


def _ensure_manifest_artifact(
    connection: psycopg.Connection[dict[str, Any]],
    prepared: PreparedPilotDerivedRegistration,
    job_id: int,
) -> tuple[int, bool]:
    artifact_key = f"pilot-derived-lineage:v1:{prepared.manifest_sha256}"
    uri = prepared.manifest_path.as_uri()
    metadata: dict[str, object] = {
        "dataset_key": prepared.dataset_key,
        "feature_version": prepared.feature_version,
        "partition_count": 2,
        "research_eligible": False,
        "validation_scope": "STRUCTURE_AND_LINEAGE_ONLY",
    }
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.artifacts
            (artifact_key, artifact_type, uri, sha256, byte_size, media_type,
             producer_job_id, metadata)
        VALUES (%s, 'PILOT_DERIVED_LINEAGE_MANIFEST', %s, %s, %s,
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
        SELECT artifact_id, artifact_key, artifact_type, uri, sha256, byte_size,
               media_type, producer_job_id, metadata
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
            "artifact_type": "PILOT_DERIVED_LINEAGE_MANIFEST",
            "byte_size": len(prepared.manifest_bytes),
            "media_type": "application/json",
            "metadata": metadata,
            "producer_job_id": job_id,
            "sha256": prepared.manifest_sha256,
            "uri": uri,
        },
    )
    return int(row["artifact_id"]), inserted is not None


def _partition_key(prepared: PreparedPilotDerivedRegistration, partition_type: str) -> str:
    digest = hashlib.sha256(
        f"{prepared.manifest_sha256}:{partition_type}".encode("ascii")
    ).hexdigest()
    return f"pilot-derived:v1:{partition_type.lower()}:{digest}"


def _ensure_partition(
    connection: psycopg.Connection[dict[str, Any]],
    prepared: PreparedPilotDerivedRegistration,
    artifact: VerifiedPilotArtifact,
    *,
    dataset_id: int,
    source_file_id: int,
    manifest_artifact_id: int,
    build_job_id: int,
) -> tuple[int, bool]:
    partition_key = _partition_key(prepared, artifact.partition_type)
    uri = artifact.canonical_path.as_uri()
    metadata: dict[str, object] = {
        "byte_size": artifact.byte_size,
        "canonical_relative_uri": artifact.canonical_relative_uri,
        "formula_sha256": prepared.formula_sha256,
        "instrument_fk_reason": "PROVIDER_MAPPING_NOT_REGISTERED",
        "original_artifact_uri": artifact.original_path.as_uri(),
        "original_relative_uri": artifact.original_relative_uri,
        "provider_instrument_id": prepared.provider_instrument_id,
        "raw_symbol": prepared.raw_symbol,
        "research_eligible": False,
        "schema_sha256": artifact.schema_sha256,
        "source_relative_uri": prepared.source_relative_uri,
        "source_sha256": prepared.source_sha256,
        "validation_scope": "STRUCTURE_AND_LINEAGE_ONLY",
    }
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.derived_partitions
            (partition_key, dataset_id, instrument_id, partition_type,
             definition_version, source_date, uri, sha256, row_count,
             min_event_time_ns, max_event_time_ns, source_manifest_sha256,
             code_commit, config_sha256, manifest_artifact_id, build_job_id,
             status, metadata, validated_at)
        VALUES (%s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, 'VALIDATED', %s, statement_timestamp())
        ON CONFLICT DO NOTHING
        RETURNING derived_partition_id
        """,
        (
            partition_key,
            dataset_id,
            artifact.partition_type,
            prepared.feature_version,
            prepared.source_date,
            uri,
            artifact.sha256,
            artifact.row_count,
            artifact.min_event_time_ns,
            artifact.max_event_time_ns,
            prepared.source_manifest_sha256,
            prepared.code_commit,
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
            "code_commit": prepared.code_commit,
            "config_sha256": prepared.config_sha256,
            "dataset_id": dataset_id,
            "definition_version": prepared.feature_version,
            "instrument_id": None,
            "manifest_artifact_id": manifest_artifact_id,
            "max_event_time_ns": artifact.max_event_time_ns,
            "metadata": metadata,
            "min_event_time_ns": artifact.min_event_time_ns,
            "partition_key": partition_key,
            "partition_type": artifact.partition_type,
            "row_count": artifact.row_count,
            "sha256": artifact.sha256,
            "source_date": prepared.source_date,
            "source_manifest_sha256": prepared.source_manifest_sha256,
            "status": "VALIDATED",
            "uri": uri,
        },
    )
    if row["validated_at"] is None:
        raise DerivedRegistryDriftError(f"validated partition lacks validated_at: {partition_key}")
    partition_id = int(row["derived_partition_id"])
    connection.execute(
        """
        INSERT INTO systematic_fx.derived_partition_sources
            (derived_partition_id, source_file_id, source_sha256)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (partition_id, source_file_id, prepared.source_sha256),
    )
    source_rows = connection.execute(
        """
        SELECT source_file_id, source_sha256
        FROM systematic_fx.derived_partition_sources
        WHERE derived_partition_id = %s
        ORDER BY source_file_id
        FOR SHARE
        """,
        (partition_id,),
    ).fetchall()
    if source_rows != [{"source_file_id": source_file_id, "source_sha256": prepared.source_sha256}]:
        raise DerivedRegistryDriftError(f"derived source-link drift: {partition_key}")
    return partition_id, inserted is not None


def _complete_job(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    job_id: int,
    created: bool,
    manifest_artifact_id: int,
    partition_ids: tuple[int, int],
) -> None:
    result: dict[str, object] = {
        "manifest_artifact_id": manifest_artifact_id,
        "partition_ids": list(partition_ids),
        "research_eligible": False,
        "validation_scope": "STRUCTURE_AND_LINEAGE_ONLY",
    }
    if created:
        updated = connection.execute(
            """
            UPDATE systematic_fx.jobs
            SET status = 'SUCCEEDED', result = %s, finished_at = statement_timestamp()
            WHERE job_id = %s AND status = 'RUNNING'
            RETURNING job_id
            """,
            (Jsonb(result), job_id),
        ).fetchone()
        if updated is None:
            raise DerivedRegistryDriftError("new pilot build job could not be completed")
    row = connection.execute(
        """
        SELECT status, result, started_at, finished_at
        FROM systematic_fx.jobs WHERE job_id = %s
        FOR SHARE
        """,
        (job_id,),
    ).fetchone()
    if row is None:
        raise DerivedRegistryDriftError("pilot build job disappeared")
    _assert_fields(
        label=f"build job {job_id}",
        row=row,
        expected={"result": result, "status": "SUCCEEDED"},
    )
    if row["started_at"] is None or row["finished_at"] is None:
        raise DerivedRegistryDriftError("succeeded pilot build job lacks timestamps")


def _register_prepared(
    connection: psycopg.Connection[dict[str, Any]],
    prepared: PreparedPilotDerivedRegistration,
) -> PilotDerivedRegistrationReport:
    connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (f"pilot-derived:{prepared.dataset_key}:{prepared.manifest_sha256}",),
    )
    dataset_id, source_file_id = _load_dataset_and_source(connection, prepared)
    job_id, created_job, _ = _ensure_job(connection, prepared, dataset_id)
    manifest_artifact_id, created_manifest = _ensure_manifest_artifact(
        connection,
        prepared,
        job_id,
    )
    partition_results = tuple(
        _ensure_partition(
            connection,
            prepared,
            artifact,
            dataset_id=dataset_id,
            source_file_id=source_file_id,
            manifest_artifact_id=manifest_artifact_id,
            build_job_id=job_id,
        )
        for artifact in prepared.artifacts
    )
    partition_ids = (partition_results[0][0], partition_results[1][0])
    _complete_job(
        connection,
        job_id=job_id,
        created=created_job,
        manifest_artifact_id=manifest_artifact_id,
        partition_ids=partition_ids,
    )
    return PilotDerivedRegistrationReport(
        dataset_id=dataset_id,
        source_file_id=source_file_id,
        build_job_id=job_id,
        manifest_artifact_id=manifest_artifact_id,
        features_1s_partition_id=partition_ids[0],
        research_5m_partition_id=partition_ids[1],
        manifest_path=prepared.manifest_path,
        manifest_sha256=prepared.manifest_sha256,
        created_job=created_job,
        created_manifest_artifact=created_manifest,
        created_partitions=sum(created for _, created in partition_results),
    )


def register_pilot_derived_partitions(
    database_url: str,
    *,
    data_root: Path | str,
    dataset_key: str,
    source_relative_uri: str,
    source_sha256: str,
    feature_version: str,
    provider_instrument_id: int,
    raw_symbol: str,
    source_date: date | str,
    formula_sha256: str,
    config_sha256: str,
    code_commit: str,
    source_manifest_sha256: str,
    one_second_artifact: ArtifactReport,
    five_minute_artifact: ArtifactReport,
) -> PilotDerivedRegistrationReport:
    """Verify, snapshot, and atomically register both pilot partitions.

    File verification and content-addressed publication complete before the
    PostgreSQL transaction begins.  Source dataset and file rows must already
    exist with verified checksums.  The function never changes dataset or
    source-file status, and an identical rerun creates no duplicate rows.
    """

    if not isinstance(database_url, str) or not database_url.strip():
        raise PilotArtifactValidationError("database_url must be a non-empty string")
    prepared = prepare_pilot_derived_registration(
        data_root=data_root,
        dataset_key=dataset_key,
        source_relative_uri=source_relative_uri,
        source_sha256=source_sha256,
        feature_version=feature_version,
        provider_instrument_id=provider_instrument_id,
        raw_symbol=raw_symbol,
        source_date=source_date,
        formula_sha256=formula_sha256,
        config_sha256=config_sha256,
        code_commit=code_commit,
        source_manifest_sha256=source_manifest_sha256,
        one_second_artifact=one_second_artifact,
        five_minute_artifact=five_minute_artifact,
    )
    _publish_prepared(prepared)
    try:
        with psycopg.connect(database_url, row_factory=dict_row) as connection:
            return _register_prepared(connection, prepared)
    except DerivedRegistryError:
        raise
    except psycopg.Error as exc:
        raise DerivedRegistryDatabaseError("PostgreSQL pilot-derived registration failed") from exc


def register_pilot_build_report(
    database_url: str,
    *,
    data_root: Path | str,
    dataset_key: str,
    source_relative_uri: str,
    report: PilotBuildReport,
    config_sha256: str,
    code_commit: str,
    source_manifest_sha256: str,
) -> PilotDerivedRegistrationReport:
    """Convenience wrapper that registers a complete pilot builder report."""

    return register_pilot_derived_partitions(
        database_url,
        data_root=data_root,
        dataset_key=dataset_key,
        source_relative_uri=source_relative_uri,
        source_sha256=report.source_sha256,
        feature_version=report.feature_version,
        provider_instrument_id=report.instrument_id,
        raw_symbol=report.contract,
        source_date=report.source_date,
        formula_sha256=report.formula_sha256,
        config_sha256=config_sha256,
        code_commit=code_commit,
        source_manifest_sha256=source_manifest_sha256,
        one_second_artifact=report.one_second,
        five_minute_artifact=report.five_minute,
    )
