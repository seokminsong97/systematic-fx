"""Register complete MBP-10 structural scans without changing data eligibility."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path, PurePosixPath
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.data.quality import (
    CHECKER_VERSION as SCANNER_CHECKER_VERSION,
)
from systematic_fx.data.quality import (
    DIAGNOSTIC_CHECKS,
    HARD_CHECKS,
)
from systematic_fx.data.quality import (
    FILE_ARTIFACT_SCHEMA as SCAN_ARTIFACT_SCHEMA,
)

EVIDENCE_ARTIFACT_SCHEMA = "systematic_fx.full_qc_registration_evidence.v1"
REGISTRY_CHECKER_VERSION = "full_qc_registry_v1"
SOURCE_CHECK_NAME = "FULL_MBP10_STRUCTURAL_SCAN_FILE"
DATASET_CHECK_NAME = "FULL_MBP10_STRUCTURAL_SCAN_AGGREGATE"
DIAGNOSTICS_CHECK_NAME = "FULL_MBP10_STRUCTURAL_DIAGNOSTICS"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_RESULTS = frozenset({"PASS", "WARN", "FAIL", "ERROR"})
_SCANNER_RESULTS = frozenset({"PASS", "FAIL"})
_SOURCE_STATUSES = frozenset({"HASHED", "VALIDATED"})
_DATASET_STATUSES = frozenset({"VALIDATING", "READY"})
_HASH_MANIFEST_KEYS = frozenset({"byte_size", "relative_uri", "sha256", "source_date"})
_SCAN_MANIFEST_KEYS = frozenset(
    {
        "artifact_schema",
        "checker_version",
        "config_sha256",
        "coverage_complete",
        "diagnostic_counts",
        "expected_row_count",
        "expected_row_group_count",
        "first_ts_recv_ns",
        "hard_violation_count",
        "hard_violation_counts",
        "last_ts_recv_ns",
        "relative_uri",
        "research_eligible",
        "result",
        "scanned_row_count",
        "scanned_row_group_count",
        "schema_fingerprint",
        "source_byte_size",
        "source_date",
        "source_manifest_sha256",
        "source_sha256",
    }
)


class FullQcRegistryError(RuntimeError):
    """Full structural-scan evidence could not be registered safely."""


class FullQcManifestError(FullQcRegistryError):
    """The final scanner manifest is incomplete, malformed, or inconsistent."""


class FullQcRegistryDriftError(FullQcRegistryError):
    """Immutable scanner, source, or database identity has drifted."""


class FullQcRegistryDatabaseError(FullQcRegistryError):
    """PostgreSQL could not commit the full-QC registration transaction."""


@dataclass(frozen=True)
class SourceHashIdentity:
    relative_uri: str
    source_date: date
    byte_size: int
    sha256: str


@dataclass(frozen=True)
class VerifiedFileScan:
    relative_uri: str
    source_date: date
    source_sha256: str
    source_byte_size: int
    schema_fingerprint: str
    expected_rows: int
    scanned_rows: int
    expected_row_groups: int
    scanned_row_groups: int
    first_ts_recv_ns: int | None
    last_ts_recv_ns: int | None
    scanner_result: str
    hard_violation_counts: Mapping[str, int]
    diagnostic_counts: Mapping[str, int]

    @property
    def result(self) -> str:
        return self.scanner_result

    def as_dict(self) -> dict[str, object]:
        return {
            "diagnostic_counts": dict(sorted(self.diagnostic_counts.items())),
            "expected_row_groups": self.expected_row_groups,
            "expected_rows": self.expected_rows,
            "first_ts_recv_ns": self.first_ts_recv_ns,
            "hard_violation_counts": dict(sorted(self.hard_violation_counts.items())),
            "last_ts_recv_ns": self.last_ts_recv_ns,
            "relative_uri": self.relative_uri,
            "result": self.result,
            "scanner_result": self.scanner_result,
            "scanned_row_groups": self.scanned_row_groups,
            "scanned_rows": self.scanned_rows,
            "schema_fingerprint": self.schema_fingerprint,
            "source_byte_size": self.source_byte_size,
            "source_date": self.source_date.isoformat(),
            "source_sha256": self.source_sha256,
        }


@dataclass(frozen=True)
class PreparedFullQcRegistration:
    data_root: Path
    dataset_key: str
    scan_manifest_path: Path
    scan_manifest_sha256: str
    source_manifest_path: Path
    source_manifest_sha256: str
    scanner_version: str
    config_sha256: str
    files: tuple[VerifiedFileScan, ...]
    aggregate_result: str
    diagnostic_result: str
    result_counts: Mapping[str, int]
    scanner_result_counts: Mapping[str, int]
    hard_violation_counts: Mapping[str, int]
    diagnostic_counts: Mapping[str, int]
    expected_rows: int
    scanned_rows: int
    expected_row_groups: int
    scanned_row_groups: int
    evidence_document: Mapping[str, object]
    evidence_bytes: bytes
    evidence_sha256: str
    evidence_path: Path
    created_evidence: bool


@dataclass(frozen=True)
class FullQcRegistrationReport:
    dataset_id: int
    dataset_key: str
    job_id: int
    artifact_id: int
    dataset_quality_check_id: int
    diagnostics_quality_check_id: int
    source_quality_check_ids: tuple[int, ...]
    evidence_path: Path
    evidence_sha256: str
    aggregate_result: str
    diagnostic_result: str
    result_counts: Mapping[str, int]
    created_evidence: bool
    created_job: bool
    created_artifact: bool
    created_quality_checks: int
    dataset_status: str
    source_status_counts: Mapping[str, int]
    status_effect: str = "NONE"
    research_eligible: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "aggregate_result": self.aggregate_result,
            "artifact_id": self.artifact_id,
            "created_artifact": self.created_artifact,
            "created_evidence": self.created_evidence,
            "created_job": self.created_job,
            "created_quality_checks": self.created_quality_checks,
            "dataset_id": self.dataset_id,
            "dataset_key": self.dataset_key,
            "dataset_quality_check_id": self.dataset_quality_check_id,
            "diagnostic_result": self.diagnostic_result,
            "diagnostics_quality_check_id": self.diagnostics_quality_check_id,
            "dataset_status": self.dataset_status,
            "evidence_path": self.evidence_path.as_posix(),
            "evidence_sha256": self.evidence_sha256,
            "job_id": self.job_id,
            "research_eligible": self.research_eligible,
            "result_counts": dict(self.result_counts),
            "source_quality_check_count": len(self.source_quality_check_ids),
            "source_status_counts": dict(self.source_status_counts),
            "status_effect": self.status_effect,
        }


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _require_sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise FullQcManifestError(f"{label} must be a lowercase SHA-256")
    return value


def _require_nonempty(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FullQcManifestError(f"{label} must be a non-empty string")
    return value.strip()


def _require_int(value: object, *, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise FullQcManifestError(f"{label} must be an integer >= {minimum}")
    return value


def _require_date(value: object, *, label: str) -> date:
    if not isinstance(value, str):
        raise FullQcManifestError(f"{label} must be an ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise FullQcManifestError(f"{label} must be an ISO date") from exc


def _require_relative_uri(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise FullQcManifestError(f"{label} must be a canonical relative URI")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise FullQcManifestError(f"{label} must be a canonical relative URI")
    return value


def _require_counts(
    value: object,
    *,
    label: str,
    expected_keys: Sequence[str],
) -> dict[str, int]:
    if not isinstance(value, dict) or set(value) != set(expected_keys):
        raise FullQcManifestError(f"{label} fields do not match checker v1")
    output: dict[str, int] = {}
    for key, count in value.items():
        if not isinstance(key, str) or not key:
            raise FullQcManifestError(f"{label} keys must be non-empty strings")
        output[key] = _require_int(count, label=f"{label}.{key}")
    return dict(sorted(output.items()))


def _require_optional_int(value: object, *, label: str) -> int | None:
    if value is None:
        return None
    return _require_int(value, label=label)


def _resolve_data_root(value: Path | str) -> tuple[Path, Path]:
    requested = Path(value).expanduser()
    if requested.is_symlink():
        raise FullQcManifestError("data_root cannot be a symbolic link")
    try:
        root = requested.resolve(strict=True)
    except FileNotFoundError as exc:
        raise FullQcManifestError(f"data_root does not exist: {requested}") from exc
    if not root.is_dir():
        raise FullQcManifestError("data_root must be a directory")
    manifests = root
    for component in ("derived", "manifests"):
        manifests /= component
        if manifests.is_symlink():
            raise FullQcManifestError(f"evidence directory cannot be a symbolic link: {manifests}")
        if manifests.exists() and not manifests.is_dir():
            raise FullQcManifestError(f"evidence directory is not a directory: {manifests}")
        manifests.mkdir(exist_ok=True, mode=0o700)
    manifests = manifests.resolve(strict=True)
    try:
        manifests.relative_to(root)
    except ValueError as exc:
        raise FullQcManifestError("manifest directory escaped data_root") from exc
    return root, manifests


def _read_contained_file(path: Path | str, *, root: Path, label: str) -> tuple[Path, bytes, str]:
    requested = Path(path).expanduser()
    if requested.is_symlink():
        raise FullQcManifestError(f"{label} cannot be a symbolic link")
    try:
        resolved = requested.resolve(strict=True)
    except FileNotFoundError as exc:
        raise FullQcManifestError(f"{label} does not exist: {requested}") from exc
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise FullQcManifestError(f"{label} must be contained by data_root") from exc
    if not resolved.is_file():
        raise FullQcManifestError(f"{label} must be a regular file")
    before = resolved.stat()
    payload = resolved.read_bytes()
    after = resolved.stat()
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if identity_before != identity_after or len(payload) != after.st_size:
        raise FullQcManifestError(f"{label} changed while being read")
    return resolved, payload, hashlib.sha256(payload).hexdigest()


def _load_source_manifest(payload: bytes) -> tuple[SourceHashIdentity, ...]:
    records: list[SourceHashIdentity] = []
    previous_uri: str | None = None
    for line_number, raw_line in enumerate(payload.splitlines(keepends=True), start=1):
        if not raw_line.endswith(b"\n"):
            raise FullQcManifestError(
                f"source manifest line {line_number} is not newline-terminated"
            )
        try:
            record = json.loads(raw_line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FullQcManifestError(
                f"invalid source manifest JSON at line {line_number}"
            ) from exc
        if not isinstance(record, dict) or set(record) != _HASH_MANIFEST_KEYS:
            raise FullQcManifestError(f"invalid source manifest fields at line {line_number}")
        if raw_line != _canonical_bytes(record):
            raise FullQcManifestError(f"source manifest line {line_number} is not canonical")
        relative_uri = _require_relative_uri(
            record.get("relative_uri"),
            label=f"source manifest line {line_number} relative_uri",
        )
        if previous_uri is not None and relative_uri <= previous_uri:
            raise FullQcManifestError("source manifest must be unique and path ordered")
        previous_uri = relative_uri
        records.append(
            SourceHashIdentity(
                relative_uri=relative_uri,
                source_date=_require_date(
                    record.get("source_date"),
                    label=f"source manifest line {line_number} source_date",
                ),
                byte_size=_require_int(
                    record.get("byte_size"),
                    label=f"source manifest line {line_number} byte_size",
                ),
                sha256=_require_sha256(
                    record.get("sha256"),
                    label=f"source manifest line {line_number} sha256",
                ),
            )
        )
    if not records:
        raise FullQcManifestError("source manifest must not be empty")
    return tuple(records)


def _parse_file_scan(value: object, *, index: int) -> VerifiedFileScan:
    if not isinstance(value, dict):
        raise FullQcManifestError(f"scan files[{index}] must be an object")
    if set(value) != _SCAN_MANIFEST_KEYS:
        raise FullQcManifestError(f"scan files[{index}] fields do not match the final contract")
    scanner_result = _require_nonempty(
        value.get("result"),
        label=f"scan files[{index}].result",
    )
    if scanner_result not in _SCANNER_RESULTS:
        raise FullQcManifestError(f"scan files[{index}].result is invalid")
    expected_rows = _require_int(
        value.get("expected_row_count"),
        label=f"scan files[{index}].expected_row_count",
    )
    scanned_rows = _require_int(
        value.get("scanned_row_count"),
        label=f"scan files[{index}].scanned_row_count",
    )
    expected_row_groups = _require_int(
        value.get("expected_row_group_count"),
        label=f"scan files[{index}].expected_row_group_count",
        minimum=1,
    )
    scanned_row_groups = _require_int(
        value.get("scanned_row_group_count"),
        label=f"scan files[{index}].scanned_row_group_count",
    )
    coverage_complete = value.get("coverage_complete")
    if coverage_complete is not True:
        raise FullQcManifestError(f"scan files[{index}] coverage_complete must be true")
    first_ts_recv_ns = _require_optional_int(
        value.get("first_ts_recv_ns"),
        label=f"scan files[{index}].first_ts_recv_ns",
    )
    last_ts_recv_ns = _require_optional_int(
        value.get("last_ts_recv_ns"),
        label=f"scan files[{index}].last_ts_recv_ns",
    )
    if scanned_rows != expected_rows or scanned_row_groups != expected_row_groups:
        raise FullQcManifestError(f"scan files[{index}] does not prove complete coverage")
    if expected_rows == 0:
        if first_ts_recv_ns is not None or last_ts_recv_ns is not None:
            raise FullQcManifestError(f"scan files[{index}] empty-file timestamps must be null")
    elif first_ts_recv_ns is None or last_ts_recv_ns is None:
        raise FullQcManifestError(f"scan files[{index}] timestamp coverage is incomplete")
    hard_counts = _require_counts(
        value.get("hard_violation_counts"),
        label=f"scan files[{index}].hard_violation_counts",
        expected_keys=HARD_CHECKS,
    )
    hard_total = _require_int(
        value.get("hard_violation_count"),
        label=f"scan files[{index}].hard_violation_count",
    )
    if hard_total != sum(hard_counts.values()):
        raise FullQcManifestError(f"scan files[{index}] hard-violation total drift")
    expected_scanner_result = "FAIL" if hard_total else "PASS"
    if scanner_result != expected_scanner_result:
        raise FullQcManifestError(f"scan files[{index}] result contradicts hard violations")
    if value.get("research_eligible") is not False:
        raise FullQcManifestError(f"scan files[{index}] research_eligible must be false")
    return VerifiedFileScan(
        relative_uri=_require_relative_uri(
            value.get("relative_uri"),
            label=f"scan files[{index}].relative_uri",
        ),
        source_date=_require_date(
            value.get("source_date"),
            label=f"scan files[{index}].source_date",
        ),
        source_sha256=_require_sha256(
            value.get("source_sha256"),
            label=f"scan files[{index}].source_sha256",
        ),
        source_byte_size=_require_int(
            value.get("source_byte_size"),
            label=f"scan files[{index}].source_byte_size",
        ),
        schema_fingerprint=_require_sha256(
            value.get("schema_fingerprint"),
            label=f"scan files[{index}].schema_fingerprint",
        ),
        expected_rows=expected_rows,
        scanned_rows=scanned_rows,
        expected_row_groups=expected_row_groups,
        scanned_row_groups=scanned_row_groups,
        first_ts_recv_ns=first_ts_recv_ns,
        last_ts_recv_ns=last_ts_recv_ns,
        scanner_result=scanner_result,
        hard_violation_counts=hard_counts,
        diagnostic_counts=_require_counts(
            value.get("diagnostic_counts"),
            label=f"scan files[{index}].diagnostic_counts",
            expected_keys=DIAGNOSTIC_CHECKS,
        ),
    )


def _load_scan_manifest(
    payload: bytes,
    *,
    source_manifest_sha256: str,
) -> tuple[tuple[VerifiedFileScan, ...], str, str]:
    files: list[VerifiedFileScan] = []
    previous_uri: str | None = None
    config_sha256: str | None = None
    for line_number, raw_line in enumerate(payload.splitlines(keepends=True), start=1):
        if not raw_line.endswith(b"\n"):
            raise FullQcManifestError(
                f"full-QC scan manifest line {line_number} is not newline-terminated"
            )
        try:
            record = json.loads(raw_line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FullQcManifestError(f"invalid full-QC scan JSON at line {line_number}") from exc
        if not isinstance(record, dict) or set(record) != _SCAN_MANIFEST_KEYS:
            raise FullQcManifestError(f"invalid full-QC scan fields at line {line_number}")
        if raw_line != _canonical_bytes(record):
            raise FullQcManifestError(f"full-QC scan manifest line {line_number} is not canonical")
        if record.get("artifact_schema") != SCAN_ARTIFACT_SCHEMA:
            raise FullQcManifestError(
                f"full-QC scan artifact_schema is invalid at line {line_number}"
            )
        if record.get("checker_version") != SCANNER_CHECKER_VERSION:
            raise FullQcManifestError(
                f"full-QC scan checker_version is invalid at line {line_number}"
            )
        if record.get("source_manifest_sha256") != source_manifest_sha256:
            raise FullQcManifestError(f"scan source_manifest_sha256 drift at line {line_number}")
        line_config_sha256 = _require_sha256(
            record.get("config_sha256"),
            label=f"scan line {line_number} config_sha256",
        )
        if config_sha256 is None:
            config_sha256 = line_config_sha256
        elif config_sha256 != line_config_sha256:
            raise FullQcManifestError("scan config_sha256 is not constant across files")
        file_scan = _parse_file_scan(record, index=line_number - 1)
        if previous_uri is not None and file_scan.relative_uri <= previous_uri:
            raise FullQcManifestError("full-QC scan manifest must be unique and path ordered")
        previous_uri = file_scan.relative_uri
        files.append(file_scan)
    if not files or config_sha256 is None:
        raise FullQcManifestError("full-QC scan manifest must not be empty")
    return tuple(files), SCANNER_CHECKER_VERSION, config_sha256


def _sum_named_counts(files: Sequence[VerifiedFileScan], attribute: str) -> dict[str, int]:
    totals: Counter[str] = Counter()
    for file in files:
        totals.update(getattr(file, attribute))
    return dict(sorted(totals.items()))


def _aggregate_result(counts: Mapping[str, int]) -> str:
    if counts.get("ERROR", 0):
        return "ERROR"
    if counts.get("FAIL", 0):
        return "FAIL"
    if counts.get("WARN", 0):
        return "WARN"
    return "PASS"


def _verify_existing_evidence(path: Path, payload: bytes) -> None:
    if path.is_symlink():
        raise FullQcRegistryDriftError(f"full-QC evidence drift at {path}")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except (FileNotFoundError, OSError) as exc:
        raise FullQcRegistryDriftError(f"full-QC evidence drift at {path}") from exc
    try:
        before = os.fstat(descriptor)
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            actual = handle.read()
        after = os.fstat(descriptor)
        current = path.lstat()
    finally:
        os.close(descriptor)
    identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    path_identity = (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns)
    if (
        not stat.S_ISREG(before.st_mode)
        or identity_before != identity_after
        or identity_after != path_identity
        or actual != payload
    ):
        raise FullQcRegistryDriftError(f"full-QC evidence drift at {path}")


def _publish_evidence(path: Path, payload: bytes, *, root: Path) -> bool:
    current = root
    for component in path.parent.relative_to(root).parts:
        current /= component
        if current.is_symlink():
            raise FullQcRegistryDriftError(
                f"full-QC evidence directory cannot be a symbolic link: {current}"
            )
        if current.exists() and not current.is_dir():
            raise FullQcRegistryDriftError(
                f"full-QC evidence directory is not a directory: {current}"
            )
        current.mkdir(exist_ok=True, mode=0o700)
    if path.exists() or path.is_symlink():
        _verify_existing_evidence(path, payload)
        return False
    handle = tempfile.NamedTemporaryFile(  # noqa: SIM115
        mode="w+b",
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        delete=False,
    )
    temporary_path = Path(handle.name)
    try:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        try:
            os.link(temporary_path, path)
            created = True
        except FileExistsError:
            _verify_existing_evidence(path, payload)
            created = False
        temporary_path.unlink()
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except BaseException:
        handle.close()
        temporary_path.unlink(missing_ok=True)
        raise
    return created


def prepare_full_qc_registration(
    *,
    data_root: Path | str,
    dataset_key: str,
    scan_manifest_path: Path | str,
    source_manifest_path: Path | str,
) -> PreparedFullQcRegistration:
    """Verify final scan/source manifests and publish immutable canonical evidence."""

    if not isinstance(dataset_key, str) or not dataset_key.strip():
        raise FullQcManifestError("dataset_key must be a non-empty string")
    root, manifests_root = _resolve_data_root(data_root)
    source_path, source_bytes, source_sha256 = _read_contained_file(
        source_manifest_path,
        root=root,
        label="source hash manifest",
    )
    source_identities = _load_source_manifest(source_bytes)
    scan_path, scan_bytes, scan_sha256 = _read_contained_file(
        scan_manifest_path,
        root=root,
        label="full-QC scan manifest",
    )
    if scan_path.name.endswith(".checkpoint.jsonl"):
        raise FullQcManifestError("a row-group checkpoint cannot be registered as final evidence")
    files, scanner_version, config_sha256 = _load_scan_manifest(
        scan_bytes,
        source_manifest_sha256=source_sha256,
    )
    if len(files) != len(source_identities):
        raise FullQcManifestError("scan/source manifest file counts differ")
    for source, scanned in zip(source_identities, files, strict=True):
        if (
            source.relative_uri,
            source.source_date,
            source.byte_size,
            source.sha256,
        ) != (
            scanned.relative_uri,
            scanned.source_date,
            scanned.source_byte_size,
            scanned.source_sha256,
        ):
            raise FullQcManifestError(f"scan/source identity drift for {source.relative_uri}")

    sparse_result_counts = Counter(file.result for file in files)
    result_counts = {result: sparse_result_counts.get(result, 0) for result in sorted(_RESULTS)}
    scanner_counts = Counter(file.scanner_result for file in files)
    scanner_result_counts = {
        result: scanner_counts.get(result, 0) for result in sorted(_SCANNER_RESULTS)
    }
    aggregate_result = _aggregate_result(result_counts)
    hard_violation_counts = _sum_named_counts(files, "hard_violation_counts")
    diagnostic_counts = _sum_named_counts(files, "diagnostic_counts")
    diagnostic_result = "WARN" if sum(diagnostic_counts.values()) else "PASS"
    expected_rows = sum(file.expected_rows for file in files)
    scanned_rows = sum(file.scanned_rows for file in files)
    expected_row_groups = sum(file.expected_row_groups for file in files)
    scanned_row_groups = sum(file.scanned_row_groups for file in files)
    expected_summary = {
        "expected_row_groups": expected_row_groups,
        "expected_rows": expected_rows,
        "file_count": len(files),
        "hard_violation_count": sum(hard_violation_counts.values()),
        "hard_violation_counts": hard_violation_counts,
        "diagnostic_count": sum(diagnostic_counts.values()),
        "diagnostic_counts": diagnostic_counts,
        "diagnostic_result": diagnostic_result,
        "result": aggregate_result,
        "result_counts": result_counts,
        "scanner_result_counts": scanner_result_counts,
        "scanned_row_groups": scanned_row_groups,
        "scanned_rows": scanned_rows,
    }

    evidence_document: dict[str, object] = {
        "artifact_schema": EVIDENCE_ARTIFACT_SCHEMA,
        "config_sha256": config_sha256,
        "dataset_key": dataset_key,
        "files": [file.as_dict() for file in files],
        "research_eligible": False,
        "scan_manifest": {
            "relative_uri": scan_path.relative_to(root).as_posix(),
            "sha256": scan_sha256,
        },
        "scanner_version": scanner_version,
        "source_manifest": {
            "relative_uri": source_path.relative_to(root).as_posix(),
            "sha256": source_sha256,
        },
        "status_effect": "NONE",
        "summary": expected_summary,
    }
    evidence_bytes = _canonical_bytes(evidence_document)
    evidence_sha256 = hashlib.sha256(evidence_bytes).hexdigest()
    evidence_path = (
        manifests_root
        / "full_qc_registry_v1"
        / "sha256"
        / evidence_sha256[:2]
        / f"{evidence_sha256}.json"
    )
    created_evidence = _publish_evidence(
        evidence_path,
        evidence_bytes,
        root=manifests_root,
    )
    return PreparedFullQcRegistration(
        data_root=root,
        dataset_key=dataset_key,
        scan_manifest_path=scan_path,
        scan_manifest_sha256=scan_sha256,
        source_manifest_path=source_path,
        source_manifest_sha256=source_sha256,
        scanner_version=scanner_version,
        config_sha256=config_sha256,
        files=files,
        aggregate_result=aggregate_result,
        diagnostic_result=diagnostic_result,
        result_counts=result_counts,
        scanner_result_counts=scanner_result_counts,
        hard_violation_counts=hard_violation_counts,
        diagnostic_counts=diagnostic_counts,
        expected_rows=expected_rows,
        scanned_rows=scanned_rows,
        expected_row_groups=expected_row_groups,
        scanned_row_groups=scanned_row_groups,
        evidence_document=evidence_document,
        evidence_bytes=evidence_bytes,
        evidence_sha256=evidence_sha256,
        evidence_path=evidence_path,
        created_evidence=created_evidence,
    )


def _single_row(row: Mapping[str, Any] | None, *, label: str) -> Mapping[str, Any]:
    if row is None:
        raise FullQcRegistryDriftError(f"{label} is missing")
    return row


def _database_id(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FullQcRegistryDatabaseError(f"database returned an invalid {label}")
    return value


def _assert_fields(
    *,
    label: str,
    row: Mapping[str, Any],
    expected: Mapping[str, object],
) -> None:
    drift = sorted(key for key, value in expected.items() if row.get(key) != value)
    if drift:
        raise FullQcRegistryDriftError(f"{label} drift: {', '.join(drift)}")


def _verify_database_sources(
    connection: psycopg.Connection[dict[str, Any]],
    prepared: PreparedFullQcRegistration,
) -> tuple[int, str, list[Mapping[str, Any]], dict[int, str]]:
    connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (prepared.dataset_key,),
    )
    dataset = _single_row(
        connection.execute(
            """
            SELECT dataset_id, dataset_key, provider, feed, data_schema, root_uri,
                   price_scale_exponent, status, expected_start_date,
                   expected_end_date, manifest_sha256
            FROM systematic_fx.datasets
            WHERE dataset_key = %s
            FOR UPDATE
            """,
            (prepared.dataset_key,),
        ).fetchone(),
        label=f"dataset {prepared.dataset_key}",
    )
    first_source_date = min(file.source_date for file in prepared.files)
    last_source_date = max(file.source_date for file in prepared.files)
    _assert_fields(
        label=f"dataset {prepared.dataset_key}",
        row=dataset,
        expected={
            "data_schema": "mbp-10",
            "dataset_key": prepared.dataset_key,
            "expected_end_date": last_source_date,
            "expected_start_date": first_source_date,
            "feed": "GLBX.MDP3",
            "manifest_sha256": prepared.source_manifest_sha256,
            "price_scale_exponent": -9,
            "provider": "Databento",
            "root_uri": (prepared.data_root / "mbp-10").resolve().as_uri(),
        },
    )
    dataset_status = dataset.get("status")
    if dataset_status not in _DATASET_STATUSES:
        raise FullQcRegistryDriftError(
            "dataset status must already be VALIDATING or READY and is never changed by full QC"
        )
    dataset_id = _database_id(dataset.get("dataset_id"), label="dataset_id")

    source_rows = connection.execute(
        """
        SELECT source_file_id, source_date, relative_uri, byte_size, sha256,
               row_count, parquet_schema_fingerprint, status, validated_at
        FROM systematic_fx.source_files
        WHERE dataset_id = %s
        ORDER BY relative_uri
        FOR SHARE
        """,
        (dataset_id,),
    ).fetchall()
    if len(source_rows) != len(prepared.files):
        raise FullQcRegistryDriftError(
            f"database source count {len(source_rows)} != scan count {len(prepared.files)}"
        )
    status_by_id: dict[int, str] = {}
    for file, row in zip(prepared.files, source_rows, strict=True):
        _assert_fields(
            label=f"source file {file.relative_uri}",
            row=row,
            expected={
                "byte_size": file.source_byte_size,
                "parquet_schema_fingerprint": file.schema_fingerprint,
                "relative_uri": file.relative_uri,
                "row_count": file.expected_rows,
                "sha256": file.source_sha256,
                "source_date": file.source_date,
            },
        )
        status = row.get("status")
        if status not in _SOURCE_STATUSES:
            raise FullQcRegistryDriftError(
                f"source status must already be HASHED or VALIDATED: {file.relative_uri}"
            )
        source_file_id = _database_id(row.get("source_file_id"), label="source_file_id")
        status_by_id[source_file_id] = str(status)
    return dataset_id, str(dataset_status), source_rows, status_by_id


def _job_payload(prepared: PreparedFullQcRegistration) -> dict[str, object]:
    return {
        "artifact_schema": EVIDENCE_ARTIFACT_SCHEMA,
        "config_sha256": prepared.config_sha256,
        "dataset_key": prepared.dataset_key,
        "evidence_sha256": prepared.evidence_sha256,
        "file_count": len(prepared.files),
        "registry_checker_version": REGISTRY_CHECKER_VERSION,
        "research_eligible": False,
        "scan_manifest_sha256": prepared.scan_manifest_sha256,
        "scanner_version": prepared.scanner_version,
        "source_manifest_sha256": prepared.source_manifest_sha256,
        "status_effect": "NONE",
    }


def _job_result(prepared: PreparedFullQcRegistration) -> dict[str, object]:
    return {
        "diagnostic_counts": dict(prepared.diagnostic_counts),
        "diagnostic_result": prepared.diagnostic_result,
        "expected_row_groups": prepared.expected_row_groups,
        "expected_rows": prepared.expected_rows,
        "hard_violation_counts": dict(prepared.hard_violation_counts),
        "quality_result": prepared.aggregate_result,
        "research_eligible": False,
        "result_counts": dict(prepared.result_counts),
        "scan_process_status": "SUCCEEDED",
        "scanned_row_groups": prepared.scanned_row_groups,
        "scanned_rows": prepared.scanned_rows,
        "status_effect": "NONE",
    }


def _ensure_job(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    prepared: PreparedFullQcRegistration,
    dataset_id: int,
) -> tuple[int, bool]:
    key = f"full-qc-registration:v1:{prepared.dataset_key}:{prepared.evidence_sha256}"
    payload = _job_payload(prepared)
    result = _job_result(prepared)
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.jobs
            (job_key, dataset_id, job_type, status, idempotency_key, payload,
             result, attempts, max_attempts, started_at, finished_at)
        VALUES (%s, %s, 'RECORD_FULL_MBP10_STRUCTURAL_SCAN', 'SUCCEEDED', %s,
                %s, %s, 1, 1, statement_timestamp(), statement_timestamp())
        ON CONFLICT DO NOTHING
        RETURNING job_id
        """,
        (key, dataset_id, key, Jsonb(payload), Jsonb(result)),
    ).fetchone()
    rows = connection.execute(
        """
        SELECT job_id, job_key, parent_job_id, dataset_id, job_type, status,
               priority, idempotency_key, payload, result, attempts, max_attempts,
               worker_id, leased_until, error_message
        FROM systematic_fx.jobs
        WHERE job_key = %s OR idempotency_key = %s
        FOR UPDATE
        """,
        (key, key),
    ).fetchall()
    if len(rows) != 1:
        raise FullQcRegistryDriftError(f"full-QC job key resolved to {len(rows)} rows")
    row = rows[0]
    _assert_fields(
        label=f"full-QC job {key}",
        row=row,
        expected={
            "attempts": 1,
            "dataset_id": dataset_id,
            "error_message": None,
            "idempotency_key": key,
            "job_key": key,
            "job_type": "RECORD_FULL_MBP10_STRUCTURAL_SCAN",
            "leased_until": None,
            "max_attempts": 1,
            "parent_job_id": None,
            "payload": payload,
            "priority": 0,
            "result": result,
            "status": "SUCCEEDED",
            "worker_id": None,
        },
    )
    return _database_id(row.get("job_id"), label="job_id"), inserted is not None


def _artifact_metadata(prepared: PreparedFullQcRegistration) -> dict[str, object]:
    return {
        "aggregate_result": prepared.aggregate_result,
        "config_sha256": prepared.config_sha256,
        "dataset_key": prepared.dataset_key,
        "diagnostic_result": prepared.diagnostic_result,
        "file_count": len(prepared.files),
        "quality_check_count": len(prepared.files) + 2,
        "registry_checker_version": REGISTRY_CHECKER_VERSION,
        "research_eligible": False,
        "scan_manifest_sha256": prepared.scan_manifest_sha256,
        "scanner_version": prepared.scanner_version,
        "source_manifest_sha256": prepared.source_manifest_sha256,
        "status_effect": "NONE",
    }


def _ensure_artifact(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    prepared: PreparedFullQcRegistration,
    job_id: int,
) -> tuple[int, bool]:
    artifact_key = f"full-qc-evidence:v1:{prepared.dataset_key}:{prepared.evidence_sha256}"
    uri = prepared.evidence_path.as_uri()
    metadata = _artifact_metadata(prepared)
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.artifacts
            (artifact_key, artifact_type, uri, sha256, byte_size, media_type,
             producer_job_id, metadata)
        VALUES (%s, 'FULL_MBP10_STRUCTURAL_SCAN_EVIDENCE', %s, %s, %s,
                'application/json', %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING artifact_id
        """,
        (
            artifact_key,
            uri,
            prepared.evidence_sha256,
            len(prepared.evidence_bytes),
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
    if len(rows) != 1:
        raise FullQcRegistryDriftError(f"full-QC artifact key/URI resolved to {len(rows)} rows")
    row = rows[0]
    _assert_fields(
        label=f"full-QC artifact {artifact_key}",
        row=row,
        expected={
            "artifact_key": artifact_key,
            "artifact_type": "FULL_MBP10_STRUCTURAL_SCAN_EVIDENCE",
            "byte_size": len(prepared.evidence_bytes),
            "media_type": "application/json",
            "metadata": metadata,
            "producer_job_id": job_id,
            "sha256": prepared.evidence_sha256,
            "uri": uri,
        },
    )
    return _database_id(row.get("artifact_id"), label="artifact_id"), inserted is not None


def _quality_check_key(
    prepared: PreparedFullQcRegistration,
    *,
    suffix: str,
) -> str:
    return (
        f"full-mbp10-structural-scan:v1:{prepared.dataset_key}:{prepared.evidence_sha256}:{suffix}"
    )


def _ensure_quality_check(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    quality_check_key: str,
    dataset_id: int | None,
    source_file_id: int | None,
    job_id: int,
    check_name: str,
    result: str,
    observed: Mapping[str, object],
    expected: Mapping[str, object],
    details: str,
) -> tuple[int, bool]:
    if result not in _RESULTS:
        raise FullQcRegistryError(f"invalid quality-check result: {result}")
    if (dataset_id is None) == (source_file_id is None):
        raise FullQcRegistryError("a full-QC check must have exactly one target")
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.quality_checks
            (quality_check_key, dataset_id, source_file_id, job_id, check_name,
             checker_version, result, observed, expected, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING quality_check_id
        """,
        (
            quality_check_key,
            dataset_id,
            source_file_id,
            job_id,
            check_name,
            REGISTRY_CHECKER_VERSION,
            result,
            Jsonb(dict(observed)),
            Jsonb(dict(expected)),
            details,
        ),
    ).fetchone()
    row = _single_row(
        connection.execute(
            """
            SELECT quality_check_id, quality_check_key, dataset_id,
                   source_file_id, derived_partition_id, job_id, check_name,
                   checker_version, result, observed, expected, details
            FROM systematic_fx.quality_checks
            WHERE quality_check_key = %s
            FOR SHARE
            """,
            (quality_check_key,),
        ).fetchone(),
        label=f"quality check {quality_check_key}",
    )
    _assert_fields(
        label=f"quality check {quality_check_key}",
        row=row,
        expected={
            "check_name": check_name,
            "checker_version": REGISTRY_CHECKER_VERSION,
            "dataset_id": dataset_id,
            "derived_partition_id": None,
            "details": details,
            "expected": dict(expected),
            "job_id": job_id,
            "observed": dict(observed),
            "quality_check_key": quality_check_key,
            "result": result,
            "source_file_id": source_file_id,
        },
    )
    return (
        _database_id(row.get("quality_check_id"), label="quality_check_id"),
        inserted is not None,
    )


def _source_check_payloads(
    prepared: PreparedFullQcRegistration,
    file: VerifiedFileScan,
) -> tuple[dict[str, object], dict[str, object]]:
    observed: dict[str, object] = {
        "coverage_complete": True,
        "diagnostic_counts": dict(file.diagnostic_counts),
        "evidence_report_sha256": prepared.evidence_sha256,
        "evidence_report_uri": prepared.evidence_path.as_uri(),
        "expected_row_count": file.expected_rows,
        "expected_row_group_count": file.expected_row_groups,
        "first_ts_recv_ns": file.first_ts_recv_ns,
        "hard_violation_count": sum(file.hard_violation_counts.values()),
        "hard_violation_counts": dict(file.hard_violation_counts),
        "last_ts_recv_ns": file.last_ts_recv_ns,
        "research_eligible": False,
        "result": file.result,
        "scanned_row_count": file.scanned_rows,
        "scanned_row_group_count": file.scanned_row_groups,
        "schema_fingerprint": file.schema_fingerprint,
        "source_byte_size": file.source_byte_size,
        "source_date": file.source_date.isoformat(),
        "source_manifest_sha256": prepared.source_manifest_sha256,
        "source_relative_uri": file.relative_uri,
        "source_sha256": file.source_sha256,
        "status_effect": "NONE",
    }
    expected: dict[str, object] = {
        "coverage_complete": True,
        "expected_row_count": file.expected_rows,
        "expected_row_group_count": file.expected_row_groups,
        "hard_violation_count": 0,
        "scanned_row_count": file.expected_rows,
        "scanned_row_group_count": file.expected_row_groups,
        "schema_fingerprint": file.schema_fingerprint,
        "source_byte_size": file.source_byte_size,
        "source_sha256": file.source_sha256,
    }
    return observed, expected


def _aggregate_check_payloads(
    prepared: PreparedFullQcRegistration,
) -> tuple[dict[str, object], dict[str, object]]:
    observed: dict[str, object] = {
        "coverage_complete": True,
        "evidence_report_sha256": prepared.evidence_sha256,
        "evidence_report_uri": prepared.evidence_path.as_uri(),
        "expected_row_count": prepared.expected_rows,
        "expected_row_group_count": prepared.expected_row_groups,
        "file_count": len(prepared.files),
        "hard_violation_count": sum(prepared.hard_violation_counts.values()),
        "hard_violation_counts": dict(prepared.hard_violation_counts),
        "research_eligible": False,
        "result": prepared.aggregate_result,
        "result_counts": dict(prepared.result_counts),
        "scan_manifest_sha256": prepared.scan_manifest_sha256,
        "scanned_row_count": prepared.scanned_rows,
        "scanned_row_group_count": prepared.scanned_row_groups,
        "source_manifest_sha256": prepared.source_manifest_sha256,
        "status_effect": "NONE",
    }
    expected: dict[str, object] = {
        "coverage_complete": True,
        "file_count": len(prepared.files),
        "hard_violation_count": 0,
        "scanned_row_count": prepared.expected_rows,
        "scanned_row_group_count": prepared.expected_row_groups,
        "source_manifest_sha256": prepared.source_manifest_sha256,
    }
    return observed, expected


def _diagnostics_check_payloads(
    prepared: PreparedFullQcRegistration,
) -> tuple[dict[str, object], dict[str, object]]:
    observed: dict[str, object] = {
        "diagnostic_count": sum(prepared.diagnostic_counts.values()),
        "diagnostic_counts": dict(prepared.diagnostic_counts),
        "evidence_report_sha256": prepared.evidence_sha256,
        "evidence_report_uri": prepared.evidence_path.as_uri(),
        "file_count": len(prepared.files),
        "research_eligible": False,
        "result": prepared.diagnostic_result,
        "scan_manifest_sha256": prepared.scan_manifest_sha256,
        "source_manifest_sha256": prepared.source_manifest_sha256,
        "status_effect": "NONE",
    }
    expected: dict[str, object] = {
        "diagnostic_count": 0,
        "source_manifest_sha256": prepared.source_manifest_sha256,
    }
    return observed, expected


def register_full_qc_scan(
    database_url: str,
    *,
    data_root: Path | str,
    dataset_key: str,
    scan_manifest_path: Path | str,
    source_manifest_path: Path | str,
) -> FullQcRegistrationReport:
    """Register complete full-QC evidence without changing source/dataset status."""

    if not isinstance(database_url, str) or not database_url.strip():
        raise FullQcRegistryError("database_url must be a non-empty string")
    prepared = prepare_full_qc_registration(
        data_root=data_root,
        dataset_key=dataset_key,
        scan_manifest_path=scan_manifest_path,
        source_manifest_path=source_manifest_path,
    )
    try:
        with psycopg.connect(database_url, row_factory=dict_row) as connection:  # noqa: SIM117
            with connection.transaction():
                dataset_id, dataset_status, source_rows, initial_status_by_id = (
                    _verify_database_sources(connection, prepared)
                )
                job_id, created_job = _ensure_job(
                    connection,
                    prepared=prepared,
                    dataset_id=dataset_id,
                )
                artifact_id, created_artifact = _ensure_artifact(
                    connection,
                    prepared=prepared,
                    job_id=job_id,
                )

                source_check_ids: list[int] = []
                created_quality_checks = 0
                details = (
                    "Complete source-file structural scan evidence; diagnostics and "
                    "research eligibility are recorded separately and no status is changed."
                )
                for file, source_row in zip(prepared.files, source_rows, strict=True):
                    source_file_id = _database_id(
                        source_row.get("source_file_id"),
                        label="source_file_id",
                    )
                    uri_digest = hashlib.sha256(file.relative_uri.encode("utf-8")).hexdigest()
                    observed, expected = _source_check_payloads(prepared, file)
                    check_id, created = _ensure_quality_check(
                        connection,
                        quality_check_key=_quality_check_key(
                            prepared,
                            suffix=f"source:{uri_digest}",
                        ),
                        dataset_id=None,
                        source_file_id=source_file_id,
                        job_id=job_id,
                        check_name=SOURCE_CHECK_NAME,
                        result=file.result,
                        observed=observed,
                        expected=expected,
                        details=details,
                    )
                    source_check_ids.append(check_id)
                    created_quality_checks += int(created)

                aggregate_observed, aggregate_expected = _aggregate_check_payloads(prepared)
                dataset_check_id, created = _ensure_quality_check(
                    connection,
                    quality_check_key=_quality_check_key(prepared, suffix="dataset:structural"),
                    dataset_id=dataset_id,
                    source_file_id=None,
                    job_id=job_id,
                    check_name=DATASET_CHECK_NAME,
                    result=prepared.aggregate_result,
                    observed=aggregate_observed,
                    expected=aggregate_expected,
                    details=(
                        "Dataset aggregate of complete per-source structural scans; the "
                        "registration job succeeds independently of this quality result."
                    ),
                )
                created_quality_checks += int(created)

                diagnostics_observed, diagnostics_expected = _diagnostics_check_payloads(prepared)
                diagnostics_check_id, created = _ensure_quality_check(
                    connection,
                    quality_check_key=_quality_check_key(prepared, suffix="dataset:diagnostics"),
                    dataset_id=dataset_id,
                    source_file_id=None,
                    job_id=job_id,
                    check_name=DIAGNOSTICS_CHECK_NAME,
                    result=prepared.diagnostic_result,
                    observed=diagnostics_observed,
                    expected=diagnostics_expected,
                    details=(
                        "Non-gating structural diagnostics are preserved separately from "
                        "hard structural quality and do not change research eligibility."
                    ),
                )
                created_quality_checks += int(created)

                final_dataset = _single_row(
                    connection.execute(
                        "SELECT status FROM systematic_fx.datasets WHERE dataset_id = %s",
                        (dataset_id,),
                    ).fetchone(),
                    label=f"dataset {dataset_id} final status",
                )
                if final_dataset.get("status") != dataset_status:
                    raise FullQcRegistryDriftError("full-QC registration changed dataset status")
                final_sources = connection.execute(
                    """
                    SELECT source_file_id, status
                    FROM systematic_fx.source_files
                    WHERE dataset_id = %s
                    ORDER BY relative_uri
                    FOR SHARE
                    """,
                    (dataset_id,),
                ).fetchall()
                final_status_by_id = {
                    _database_id(row.get("source_file_id"), label="source_file_id"): str(
                        row.get("status")
                    )
                    for row in final_sources
                }
                if final_status_by_id != initial_status_by_id:
                    raise FullQcRegistryDriftError("full-QC registration changed source status")
                source_status_counts = Counter(initial_status_by_id.values())

                return FullQcRegistrationReport(
                    dataset_id=dataset_id,
                    dataset_key=prepared.dataset_key,
                    job_id=job_id,
                    artifact_id=artifact_id,
                    dataset_quality_check_id=dataset_check_id,
                    diagnostics_quality_check_id=diagnostics_check_id,
                    source_quality_check_ids=tuple(source_check_ids),
                    evidence_path=prepared.evidence_path,
                    evidence_sha256=prepared.evidence_sha256,
                    aggregate_result=prepared.aggregate_result,
                    diagnostic_result=prepared.diagnostic_result,
                    result_counts=dict(prepared.result_counts),
                    created_evidence=prepared.created_evidence,
                    created_job=created_job,
                    created_artifact=created_artifact,
                    created_quality_checks=created_quality_checks,
                    dataset_status=dataset_status,
                    source_status_counts=dict(sorted(source_status_counts.items())),
                )
    except FullQcRegistryError:
        raise
    except psycopg.Error as exc:
        raise FullQcRegistryDatabaseError("PostgreSQL full-QC registration failed") from exc
