"""Atomic registration of verified footer and full-content source manifests."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import date
from itertools import zip_longest
from pathlib import Path, PurePosixPath
from typing import BinaryIO

import psycopg
from psycopg.types.json import Jsonb

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_HASH_RECORD_KEYS = frozenset({"byte_size", "relative_uri", "sha256", "source_date"})
_FOOTER_DATABASE_COLUMNS = frozenset(
    {
        "file_size_bytes",
        "instrument_mappings",
        "path",
        "row_count",
        "schema_fingerprint",
        "source_date",
    }
)
_REGISTRABLE_DATASET_STATUSES = frozenset({"REGISTERED", "VALIDATING", "READY"})
_REGISTRABLE_SOURCE_STATUSES = frozenset({"DISCOVERED", "HASHED", "VALIDATED"})


class DataRegistryError(RuntimeError):
    """Base error for manifest validation and control-plane registration."""


class ManifestValidationError(DataRegistryError):
    """The footer and content-hash manifests are not the same source set."""


class RegistryDriftError(DataRegistryError):
    """Existing immutable control-plane identity conflicts with the manifests."""


class DataRegistryDatabaseError(DataRegistryError):
    """PostgreSQL rejected or could not complete the atomic registration."""


@dataclass(frozen=True)
class DatasetRegistration:
    """Stable database identity for one immutable source dataset version."""

    dataset_key: str
    root_uri: str
    provider: str = "Databento"
    feed: str = "GLBX.MDP3"
    data_schema: str = "mbp-10"
    price_scale_exponent: int = -9

    def validate(self) -> None:
        fields = {
            "dataset_key": self.dataset_key,
            "root_uri": self.root_uri,
            "provider": self.provider,
            "feed": self.feed,
            "data_schema": self.data_schema,
        }
        for name, value in fields.items():
            if not isinstance(value, str) or not value.strip():
                raise ManifestValidationError(f"{name} must be a non-empty string")
        if (
            isinstance(self.price_scale_exponent, bool)
            or not isinstance(self.price_scale_exponent, int)
            or not -18 <= self.price_scale_exponent <= 18
        ):
            raise ManifestValidationError("price_scale_exponent must be an integer from -18 to 18")


@dataclass(frozen=True)
class SourceFileRegistration:
    """One source identity after footer/hash 1:1 verification."""

    relative_uri: str
    source_date: date
    byte_size: int
    sha256: str
    row_count: int
    schema_fingerprint: str
    provider_dataset: str
    data_schema: str
    price_scale: str
    footer_metadata: dict[str, object]


@dataclass(frozen=True)
class SourceManifestBundle:
    """Fully paired manifests, safe to register as one database transaction."""

    footer_manifest_path: Path
    hash_manifest_path: Path
    footer_manifest_sha256: str
    hash_manifest_sha256: str
    records: tuple[SourceFileRegistration, ...]
    total_source_bytes: int
    first_source_date: date
    last_source_date: date

    @property
    def file_count(self) -> int:
        return len(self.records)


@dataclass(frozen=True)
class DataRegistryReport:
    """Committed control-plane identity and source-file coverage."""

    dataset_id: int
    dataset_key: str
    dataset_status: str
    source_file_count: int
    total_source_bytes: int
    preexisting_source_file_count: int
    inserted_source_file_count: int
    footer_manifest_sha256: str
    hash_manifest_sha256: str

    def as_dict(self) -> dict[str, object]:
        return {
            "dataset_id": self.dataset_id,
            "dataset_key": self.dataset_key,
            "dataset_status": self.dataset_status,
            "footer_manifest_sha256": self.footer_manifest_sha256,
            "hash_manifest_sha256": self.hash_manifest_sha256,
            "inserted_source_file_count": self.inserted_source_file_count,
            "preexisting_source_file_count": self.preexisting_source_file_count,
            "source_file_count": self.source_file_count,
            "total_source_bytes": self.total_source_bytes,
        }


@dataclass(frozen=True)
class _FileIdentity:
    byte_size: int
    mtime_ns: int
    ctime_ns: int
    device: int
    inode: int


@dataclass
class _ManifestStream:
    path: Path
    descriptor: int
    handle: BinaryIO
    identity: _FileIdentity
    digest: object

    def lines(self):  # type: ignore[no-untyped-def]
        for line in self.handle:
            self.digest.update(line)
            yield line

    def finish(self) -> str:
        current = _identity(os.fstat(self.descriptor))
        if current != self.identity:
            raise ManifestValidationError(f"manifest changed while being read: {self.path}")
        return self.digest.hexdigest()  # type: ignore[no-any-return]


def _identity(file_stat: os.stat_result) -> _FileIdentity:
    return _FileIdentity(
        byte_size=file_stat.st_size,
        mtime_ns=file_stat.st_mtime_ns,
        ctime_ns=file_stat.st_ctime_ns,
        device=file_stat.st_dev,
        inode=file_stat.st_ino,
    )


def _open_manifest(stack: ExitStack, path: Path | str, *, label: str) -> _ManifestStream:
    requested = Path(path).expanduser()
    if requested.is_symlink():
        raise ManifestValidationError(f"{label} cannot be a symbolic link: {requested}")
    try:
        resolved = requested.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ManifestValidationError(f"{label} does not exist: {requested}") from exc
    mode = resolved.lstat().st_mode
    if not stat.S_ISREG(mode):
        raise ManifestValidationError(f"{label} must be a regular file: {resolved}")

    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(resolved, flags)
    stack.callback(os.close, descriptor)
    identity = _identity(os.fstat(descriptor))
    handle = stack.enter_context(os.fdopen(descriptor, "rb", closefd=False))
    return _ManifestStream(
        path=resolved,
        descriptor=descriptor,
        handle=handle,
        identity=identity,
        digest=hashlib.sha256(),
    )


def _canonical_json_line(record: dict[str, object]) -> bytes:
    return (
        json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _json_record(raw_line: bytes, *, label: str, line_number: int) -> dict[str, object]:
    if not raw_line.endswith(b"\n"):
        raise ManifestValidationError(f"{label} line {line_number} is not newline-terminated")
    try:
        record = json.loads(raw_line)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ManifestValidationError(f"invalid JSON in {label} line {line_number}") from exc
    if not isinstance(record, dict):
        raise ManifestValidationError(f"{label} line {line_number} must be a JSON object")
    if raw_line != _canonical_json_line(record):
        raise ManifestValidationError(f"{label} line {line_number} is not canonical JSONL")
    return record


def _nonnegative_int(value: object, *, field: str, label: str, line_number: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ManifestValidationError(
            f"{label} line {line_number} field {field!r} must be a non-negative integer"
        )
    return value


def _iso_date(value: object, *, field: str, label: str, line_number: int) -> date:
    if not isinstance(value, str):
        raise ManifestValidationError(
            f"{label} line {line_number} field {field!r} must be an ISO date"
        )
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ManifestValidationError(f"invalid {field!r} in {label} line {line_number}") from exc


def _relative_uri(value: object, *, label: str, line_number: int) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ManifestValidationError(f"{label} line {line_number} has an invalid relative URI")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
        or path.as_posix() != value
    ):
        raise ManifestValidationError(
            f"{label} line {line_number} has an unsafe relative URI: {value!r}"
        )
    return value


def _sha256(value: object, *, field: str, label: str, line_number: int) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise ManifestValidationError(
            f"{label} line {line_number} field {field!r} must be lowercase SHA-256"
        )
    return value


def _footer_fields(
    record: dict[str, object],
    *,
    line_number: int,
) -> tuple[str, date, int, int, str, str, str, str, dict[str, object]]:
    label = "footer manifest"
    relative_uri = _relative_uri(record.get("path"), label=label, line_number=line_number)
    source_date = _iso_date(
        record.get("source_date"),
        field="source_date",
        label=label,
        line_number=line_number,
    )
    byte_size = _nonnegative_int(
        record.get("file_size_bytes"),
        field="file_size_bytes",
        label=label,
        line_number=line_number,
    )
    row_count = _nonnegative_int(
        record.get("row_count"),
        field="row_count",
        label=label,
        line_number=line_number,
    )
    schema_fingerprint = _sha256(
        record.get("schema_fingerprint"),
        field="schema_fingerprint",
        label=label,
        line_number=line_number,
    )
    contract = record.get("contract")
    if not isinstance(contract, dict):
        raise ManifestValidationError(
            f"footer manifest line {line_number} contract must be an object"
        )
    provider_dataset = contract.get("dataset")
    data_schema = contract.get("schema")
    price_scale = contract.get("price_scale")
    if not all(isinstance(value, str) and value for value in (provider_dataset, data_schema)):
        raise ManifestValidationError(
            f"footer manifest line {line_number} has invalid contract dataset/schema"
        )
    if not isinstance(price_scale, str) or not price_scale:
        raise ManifestValidationError(
            f"footer manifest line {line_number} has invalid contract price_scale"
        )
    footer_metadata = {
        key: value for key, value in record.items() if key not in _FOOTER_DATABASE_COLUMNS
    }
    return (
        relative_uri,
        source_date,
        byte_size,
        row_count,
        schema_fingerprint,
        provider_dataset,
        data_schema,
        price_scale,
        footer_metadata,
    )


def _hash_fields(
    record: dict[str, object],
    *,
    line_number: int,
) -> tuple[str, date, int, str]:
    label = "hash manifest"
    if set(record) != _HASH_RECORD_KEYS:
        raise ManifestValidationError(
            f"hash manifest line {line_number} must contain exactly "
            "byte_size, relative_uri, sha256, source_date"
        )
    return (
        _relative_uri(record.get("relative_uri"), label=label, line_number=line_number),
        _iso_date(
            record.get("source_date"),
            field="source_date",
            label=label,
            line_number=line_number,
        ),
        _nonnegative_int(
            record.get("byte_size"),
            field="byte_size",
            label=label,
            line_number=line_number,
        ),
        _sha256(
            record.get("sha256"),
            field="sha256",
            label=label,
            line_number=line_number,
        ),
    )


def load_source_manifest_bundle(
    footer_manifest_path: Path | str,
    hash_manifest_path: Path | str,
) -> SourceManifestBundle:
    """Load two canonical JSONL manifests and enforce exact 1:1 source identity."""

    with ExitStack() as stack:
        footer_stream = _open_manifest(
            stack,
            footer_manifest_path,
            label="footer manifest",
        )
        hash_stream = _open_manifest(stack, hash_manifest_path, label="hash manifest")
        if footer_stream.path == hash_stream.path:
            raise ManifestValidationError("footer and hash manifests must be different files")

        records: list[SourceFileRegistration] = []
        previous_uri: str | None = None
        for line_number, pair in enumerate(
            zip_longest(footer_stream.lines(), hash_stream.lines()),
            start=1,
        ):
            footer_line, hash_line = pair
            if footer_line is None or hash_line is None:
                raise ManifestValidationError(
                    f"footer/hash manifest cardinality mismatch at line {line_number}"
                )
            footer_record = _json_record(
                footer_line,
                label="footer manifest",
                line_number=line_number,
            )
            hash_record = _json_record(
                hash_line,
                label="hash manifest",
                line_number=line_number,
            )
            (
                footer_uri,
                footer_date,
                footer_size,
                row_count,
                schema_fingerprint,
                provider_dataset,
                data_schema,
                price_scale,
                footer_metadata,
            ) = _footer_fields(footer_record, line_number=line_number)
            hash_uri, hash_date, hash_size, content_sha256 = _hash_fields(
                hash_record,
                line_number=line_number,
            )
            if (footer_uri, footer_date, footer_size) != (hash_uri, hash_date, hash_size):
                raise ManifestValidationError(
                    f"footer/hash identity mismatch at line {line_number}: "
                    f"footer=({footer_uri!r}, {footer_date}, {footer_size}), "
                    f"hash=({hash_uri!r}, {hash_date}, {hash_size})"
                )
            if previous_uri is not None and footer_uri <= previous_uri:
                kind = "duplicate" if footer_uri == previous_uri else "path-order drift"
                raise ManifestValidationError(
                    f"{kind} in source manifests at line {line_number}: {footer_uri!r}"
                )
            previous_uri = footer_uri
            records.append(
                SourceFileRegistration(
                    relative_uri=footer_uri,
                    source_date=footer_date,
                    byte_size=footer_size,
                    sha256=content_sha256,
                    row_count=row_count,
                    schema_fingerprint=schema_fingerprint,
                    provider_dataset=provider_dataset,
                    data_schema=data_schema,
                    price_scale=price_scale,
                    footer_metadata=footer_metadata,
                )
            )

        footer_sha256 = footer_stream.finish()
        hash_sha256 = hash_stream.finish()

    if not records:
        raise ManifestValidationError("source manifests must contain at least one file")
    return SourceManifestBundle(
        footer_manifest_path=footer_stream.path,
        hash_manifest_path=hash_stream.path,
        footer_manifest_sha256=footer_sha256,
        hash_manifest_sha256=hash_sha256,
        records=tuple(records),
        total_source_bytes=sum(record.byte_size for record in records),
        first_source_date=min(record.source_date for record in records),
        last_source_date=max(record.source_date for record in records),
    )


def _validate_bundle_contract(
    dataset: DatasetRegistration,
    bundle: SourceManifestBundle,
) -> None:
    expected_price_scale = f"1e{dataset.price_scale_exponent}"
    for record in bundle.records:
        if record.provider_dataset != dataset.feed:
            raise ManifestValidationError(
                f"footer contract dataset {record.provider_dataset!r} does not match "
                f"registered feed {dataset.feed!r} for {record.relative_uri}"
            )
        if record.data_schema != dataset.data_schema:
            raise ManifestValidationError(
                f"footer contract schema {record.data_schema!r} does not match "
                f"registered schema {dataset.data_schema!r} for {record.relative_uri}"
            )
        if record.price_scale != expected_price_scale:
            raise ManifestValidationError(
                f"footer price scale {record.price_scale!r} does not match "
                f"registered exponent {dataset.price_scale_exponent} for {record.relative_uri}"
            )


def _validate_existing_dataset(
    row: tuple[object, ...],
    dataset: DatasetRegistration,
    bundle: SourceManifestBundle,
) -> int:
    (
        dataset_id,
        provider,
        feed,
        data_schema,
        root_uri,
        price_scale_exponent,
        status,
        expected_start_date,
        expected_end_date,
        manifest_sha256,
    ) = row
    expected_identity = (
        dataset.provider,
        dataset.feed,
        dataset.data_schema,
        dataset.root_uri,
        dataset.price_scale_exponent,
    )
    if (provider, feed, data_schema, root_uri, price_scale_exponent) != expected_identity:
        raise RegistryDriftError(
            f"dataset {dataset.dataset_key!r} already has a different immutable identity"
        )
    if status not in _REGISTRABLE_DATASET_STATUSES:
        raise RegistryDriftError(
            f"dataset {dataset.dataset_key!r} cannot be registered from status {status!r}"
        )
    if expected_start_date is not None and expected_start_date != bundle.first_source_date:
        raise RegistryDriftError(f"dataset {dataset.dataset_key!r} start-date drift")
    if expected_end_date is not None and expected_end_date != bundle.last_source_date:
        raise RegistryDriftError(f"dataset {dataset.dataset_key!r} end-date drift")
    if manifest_sha256 is not None and manifest_sha256 != bundle.hash_manifest_sha256:
        raise RegistryDriftError(f"dataset {dataset.dataset_key!r} manifest SHA-256 drift")
    if isinstance(dataset_id, bool) or not isinstance(dataset_id, int):
        raise DataRegistryDatabaseError("database returned an invalid dataset_id")
    return dataset_id


def _validate_existing_sources(
    rows: list[tuple[object, ...]],
    bundle: SourceManifestBundle,
    *,
    dataset_ready: bool,
) -> None:
    expected_by_uri = {record.relative_uri: record for record in bundle.records}
    seen: set[str] = set()
    for row in rows:
        (
            relative_uri,
            source_date,
            byte_size,
            sha256,
            row_count,
            schema_fingerprint,
            status,
        ) = row
        if not isinstance(relative_uri, str) or relative_uri in seen:
            raise RegistryDriftError("database contains duplicate or invalid source URI state")
        seen.add(relative_uri)
        expected = expected_by_uri.get(relative_uri)
        if expected is None:
            raise RegistryDriftError(
                f"database source {relative_uri!r} is absent from the immutable manifests"
            )
        if source_date != expected.source_date or byte_size != expected.byte_size:
            raise RegistryDriftError(f"database source identity drift for {relative_uri}")
        if sha256 is not None and sha256 != expected.sha256:
            raise RegistryDriftError(f"database source SHA-256 drift for {relative_uri}")
        if row_count is not None and row_count != expected.row_count:
            raise RegistryDriftError(f"database source row-count drift for {relative_uri}")
        if schema_fingerprint is not None and schema_fingerprint != expected.schema_fingerprint:
            raise RegistryDriftError(f"database source schema drift for {relative_uri}")
        if status not in _REGISTRABLE_SOURCE_STATUSES:
            raise RegistryDriftError(
                f"database source {relative_uri!r} cannot be registered from status {status!r}"
            )
        if dataset_ready and status != "VALIDATED":
            raise RegistryDriftError(f"READY dataset source {relative_uri!r} is not VALIDATED")
    if dataset_ready and len(rows) != bundle.file_count:
        raise RegistryDriftError(
            "READY dataset does not contain the complete validated source manifest"
        )


_SELECT_DATASET_SQL = """
SELECT dataset_id, provider, feed, data_schema, root_uri, price_scale_exponent,
       status, expected_start_date, expected_end_date, manifest_sha256
FROM systematic_fx.datasets
WHERE dataset_key = %s
FOR UPDATE
"""

_UPSERT_DATASET_SQL = """
INSERT INTO systematic_fx.datasets
    (dataset_key, provider, feed, data_schema, root_uri, price_scale_exponent,
     status, expected_start_date, expected_end_date, manifest_sha256)
VALUES (%s, %s, %s, %s, %s, %s, 'VALIDATING', %s, %s, %s)
ON CONFLICT (dataset_key) DO UPDATE SET
    status = 'VALIDATING',
    manifest_sha256 = EXCLUDED.manifest_sha256,
    updated_at = statement_timestamp()
RETURNING dataset_id
"""

_SELECT_SOURCES_SQL = """
SELECT relative_uri, source_date, byte_size, sha256, row_count,
       parquet_schema_fingerprint, status
FROM systematic_fx.source_files
WHERE dataset_id = %s
ORDER BY relative_uri
FOR UPDATE
"""

_UPSERT_SOURCE_SQL = """
INSERT INTO systematic_fx.source_files AS existing
    (dataset_id, source_date, relative_uri, byte_size, sha256, row_count,
     parquet_schema_fingerprint, status, footer_metadata, validated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, 'HASHED', %s, NULL)
ON CONFLICT (dataset_id, relative_uri) DO UPDATE SET
    source_date = EXCLUDED.source_date,
    byte_size = EXCLUDED.byte_size,
    sha256 = EXCLUDED.sha256,
    row_count = EXCLUDED.row_count,
    parquet_schema_fingerprint = EXCLUDED.parquet_schema_fingerprint,
    status = 'HASHED',
    footer_metadata = EXCLUDED.footer_metadata,
    validated_at = NULL
WHERE existing.status <> 'VALIDATED'
"""

_VERIFY_SOURCES_SQL = """
SELECT count(*)::bigint,
       count(*) FILTER (
           WHERE status IN ('HASHED', 'VALIDATED') AND sha256 IS NOT NULL
       )::bigint
FROM systematic_fx.source_files
WHERE dataset_id = %s
"""


def _register_bundle(
    connection: psycopg.Connection,
    dataset: DatasetRegistration,
    bundle: SourceManifestBundle,
) -> DataRegistryReport:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (dataset.dataset_key,),
        )
        cursor.execute(_SELECT_DATASET_SQL, (dataset.dataset_key,))
        existing_dataset = cursor.fetchone()
        existing_dataset_status: str | None = None
        existing_dataset_id: int | None = None
        if existing_dataset is not None:
            existing_dataset_id = _validate_existing_dataset(existing_dataset, dataset, bundle)
            status_value = existing_dataset[6]
            if not isinstance(status_value, str):
                raise DataRegistryDatabaseError("database returned an invalid dataset status")
            existing_dataset_status = status_value

        if existing_dataset_status == "READY":
            dataset_id = existing_dataset_id
            if dataset_id is None:
                raise DataRegistryDatabaseError("READY dataset has no valid dataset_id")
        else:
            cursor.execute(
                _UPSERT_DATASET_SQL,
                (
                    dataset.dataset_key,
                    dataset.provider,
                    dataset.feed,
                    dataset.data_schema,
                    dataset.root_uri,
                    dataset.price_scale_exponent,
                    bundle.first_source_date,
                    bundle.last_source_date,
                    bundle.hash_manifest_sha256,
                ),
            )
            returned_dataset = cursor.fetchone()
            if returned_dataset is None:
                raise DataRegistryDatabaseError("dataset upsert returned no dataset_id")
            dataset_id = returned_dataset[0]
            if isinstance(dataset_id, bool) or not isinstance(dataset_id, int):
                raise DataRegistryDatabaseError("dataset upsert returned an invalid dataset_id")

        cursor.execute(_SELECT_SOURCES_SQL, (dataset_id,))
        existing_sources = cursor.fetchall()
        _validate_existing_sources(
            existing_sources,
            bundle,
            dataset_ready=existing_dataset_status == "READY",
        )
        cursor.executemany(
            _UPSERT_SOURCE_SQL,
            [
                (
                    dataset_id,
                    record.source_date,
                    record.relative_uri,
                    record.byte_size,
                    record.sha256,
                    record.row_count,
                    record.schema_fingerprint,
                    Jsonb(record.footer_metadata),
                )
                for record in bundle.records
            ],
        )
        cursor.execute(_VERIFY_SOURCES_SQL, (dataset_id,))
        verified = cursor.fetchone()
        if verified != (bundle.file_count, bundle.file_count):
            raise DataRegistryDatabaseError(
                "source-file verification did not match the complete hashed manifest"
            )

    preexisting_count = len(existing_sources)
    return DataRegistryReport(
        dataset_id=dataset_id,
        dataset_key=dataset.dataset_key,
        dataset_status="READY" if existing_dataset_status == "READY" else "VALIDATING",
        source_file_count=bundle.file_count,
        total_source_bytes=bundle.total_source_bytes,
        preexisting_source_file_count=preexisting_count,
        inserted_source_file_count=bundle.file_count - preexisting_count,
        footer_manifest_sha256=bundle.footer_manifest_sha256,
        hash_manifest_sha256=bundle.hash_manifest_sha256,
    )


def register_source_manifests(
    database_url: str,
    *,
    footer_manifest_path: Path | str,
    hash_manifest_path: Path | str,
    dataset: DatasetRegistration,
) -> DataRegistryReport:
    """Validate both manifests, then atomically upsert dataset and source files.

    This stage intentionally stops at ``source_files``.  A successful registry
    transaction sets every source to ``HASHED`` and the dataset to
    ``VALIDATING``; only later quality gates may promote it to ``READY``.
    """

    if not isinstance(database_url, str) or not database_url.strip():
        raise ManifestValidationError("database_url must be a non-empty string")
    dataset.validate()
    bundle = load_source_manifest_bundle(footer_manifest_path, hash_manifest_path)
    _validate_bundle_contract(dataset, bundle)
    try:
        with psycopg.connect(database_url) as connection:
            return _register_bundle(connection, dataset, bundle)
    except DataRegistryError:
        raise
    except psycopg.Error as exc:
        raise DataRegistryDatabaseError("PostgreSQL data registration failed") from exc
