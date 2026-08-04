"""Dataset-level source qualification evidence and control-plane registration."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.data.contracts import (
    DATASET_NAME,
    EXPECTED_COLUMN_COUNT,
    PRICE_ENCODING,
    PRICE_SCALE_TEXT,
    SCHEMA_NAME,
    UNDEFINED_PRICE,
)
from systematic_fx.db.data_registry import (
    DataRegistryError,
    SourceFileRegistration,
    load_source_manifest_bundle,
)

CHECKER_VERSION = "source_qualification_v1"
DEFAULT_REPORT_NAME = "mbp10_source_qualification_v1.json"
EXPECTED_SCHEMA_FINGERPRINT = "57c7cc404aec87845b9e3872a4b2abcc651bd07858810324b4c9e3aa636ef5ea"

_RESULTS = frozenset({"PASS", "WARN", "FAIL", "ERROR"})


class QualificationRegistryError(RuntimeError):
    """Source qualification could not be prepared or registered safely."""


class QualificationDriftError(QualificationRegistryError):
    """Existing evidence or database identity differs from the frozen inputs."""


class QualificationDatabaseError(QualificationRegistryError):
    """PostgreSQL could not complete the qualification transaction."""


@dataclass(frozen=True)
class QualificationCheck:
    """One dataset-target result backed by canonical evidence."""

    check_name: str
    result: Literal["PASS", "WARN", "FAIL", "ERROR"]
    observed: Mapping[str, object]
    expected: Mapping[str, object]
    details: str

    def as_dict(self) -> dict[str, object]:
        return {
            "check_name": self.check_name,
            "details": self.details,
            "expected": dict(self.expected),
            "observed": dict(self.observed),
            "result": self.result,
        }


@dataclass(frozen=True)
class QualificationRegistrationReport:
    """Committed dataset-level checks and their canonical evidence artifact."""

    dataset_id: int
    dataset_key: str
    dataset_status: str
    source_status_counts: Mapping[str, int]
    evidence_path: Path
    evidence_sha256: str
    evidence_byte_size: int
    artifact_id: int
    quality_check_ids: tuple[int, ...]
    created_evidence_file: bool
    created_artifact: bool
    created_quality_checks: int
    overall_status: str
    research_eligible: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_id": self.artifact_id,
            "created_artifact": self.created_artifact,
            "created_evidence_file": self.created_evidence_file,
            "created_quality_checks": self.created_quality_checks,
            "dataset_id": self.dataset_id,
            "dataset_key": self.dataset_key,
            "dataset_status": self.dataset_status,
            "evidence_byte_size": self.evidence_byte_size,
            "evidence_path": self.evidence_path.as_posix(),
            "evidence_sha256": self.evidence_sha256,
            "overall_status": self.overall_status,
            "quality_check_ids": list(self.quality_check_ids),
            "research_eligible": self.research_eligible,
            "source_status_counts": dict(self.source_status_counts),
        }


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _nonnegative_int(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise QualificationRegistryError(f"{label} must be a non-negative integer")
    return value


def _string_list(value: object, *, label: str) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise QualificationRegistryError(f"{label} must be a list of strings")
    return value


def _manifest_checks(
    records: Sequence[SourceFileRegistration],
    *,
    footer_manifest_sha256: str,
) -> tuple[QualificationCheck, QualificationCheck, QualificationCheck, dict[str, object]]:
    expected_contract = {
        "dataset": DATASET_NAME,
        "dbn_version": 3,
        "price_encoding": PRICE_ENCODING,
        "price_scale": PRICE_SCALE_TEXT,
        "schema": SCHEMA_NAME,
        "undefined_price": UNDEFINED_PRICE,
    }
    schema_fingerprints: set[str] = set()
    unknown_mappings = 0
    outright_mappings = 0
    spread_mappings = 0
    mapping_intervals = 0
    partial_symbols = 0
    files_with_partial = 0
    not_found_symbols = 0

    for record in records:
        metadata = record.footer_metadata
        if metadata.get("contract") != expected_contract:
            raise QualificationRegistryError(f"footer contract drift for {record.relative_uri}")
        if metadata.get("column_count") != EXPECTED_COLUMN_COUNT:
            raise QualificationRegistryError(f"footer column-count drift for {record.relative_uri}")
        if record.schema_fingerprint != EXPECTED_SCHEMA_FINGERPRINT:
            raise QualificationRegistryError(
                f"footer schema fingerprint drift for {record.relative_uri}"
            )
        schema_fingerprints.add(record.schema_fingerprint)

        counts = metadata.get("instrument_kind_counts")
        if not isinstance(counts, dict):
            raise QualificationRegistryError(
                f"instrument_kind_counts missing for {record.relative_uri}"
            )
        outright = _nonnegative_int(counts.get("outright"), label="outright mapping count")
        spreads = _nonnegative_int(
            counts.get("calendar_spread"),
            label="calendar-spread mapping count",
        )
        unknown = _nonnegative_int(counts.get("unknown"), label="unknown mapping count")
        intervals = _nonnegative_int(
            metadata.get("mapping_interval_count"),
            label="mapping interval count",
        )
        if outright + spreads + unknown != intervals:
            raise QualificationRegistryError(
                f"mapping classification total drift for {record.relative_uri}"
            )
        outright_mappings += outright
        spread_mappings += spreads
        unknown_mappings += unknown
        mapping_intervals += intervals

        partial = _string_list(metadata.get("partial"), label="partial metadata")
        not_found = _string_list(metadata.get("not_found"), label="not_found metadata")
        partial_symbols += len(partial)
        files_with_partial += bool(partial)
        not_found_symbols += len(not_found)

    footer_check = QualificationCheck(
        check_name="footer_exact_contract_identity",
        result="PASS" if not_found_symbols == 0 else "FAIL",
        observed={
            "column_count": EXPECTED_COLUMN_COUNT,
            "file_count": len(records),
            "footer_manifest_sha256": footer_manifest_sha256,
            "not_found_symbol_count": not_found_symbols,
            "schema_fingerprints": sorted(schema_fingerprints),
        },
        expected={
            "contract": expected_contract,
            "not_found_symbol_count": 0,
            "schema_fingerprint": EXPECTED_SCHEMA_FINGERPRINT,
        },
        details=(
            "Every canonical footer record carries the frozen 73-column MBP-10 contract and "
            "schema identity; URI/date/size pairing is validated against the hash manifest."
        ),
    )
    mapping_check = QualificationCheck(
        check_name="mapping_classification",
        result="PASS" if unknown_mappings == 0 else "FAIL",
        observed={
            "calendar_spread_mapping_count": spread_mappings,
            "mapping_interval_count": mapping_intervals,
            "outright_mapping_count": outright_mappings,
            "unknown_mapping_count": unknown_mappings,
        },
        expected={"unknown_mapping_count": 0},
        details="Every footer mapping interval is classified; unknown mappings block research.",
    )
    partial_check = QualificationCheck(
        check_name="provider_partial_metadata",
        result="WARN" if partial_symbols else "PASS",
        observed={
            "files_with_partial": files_with_partial,
            "partial_symbol_count": partial_symbols,
        },
        expected={"partial_symbol_count": 0},
        details=(
            "Provider partial-request metadata is retained as a warning; execution-contract "
            "eligibility still requires separate roll/reference and row-quality checks."
        ),
    )
    summary = {
        "calendar_spread_mapping_count": spread_mappings,
        "files_with_partial": files_with_partial,
        "mapping_interval_count": mapping_intervals,
        "not_found_symbol_count": not_found_symbols,
        "outright_mapping_count": outright_mappings,
        "partial_symbol_count": partial_symbols,
        "schema_fingerprints": sorted(schema_fingerprints),
        "unknown_mapping_count": unknown_mappings,
    }
    return footer_check, mapping_check, partial_check, summary


def _source_identity_payload(record: SourceFileRegistration) -> dict[str, object]:
    return {
        "byte_size": record.byte_size,
        "parquet_schema_fingerprint": record.schema_fingerprint,
        "relative_uri": record.relative_uri,
        "row_count": record.row_count,
        "sha256": record.sha256,
        "source_date": record.source_date.isoformat(),
    }


def _identity_sha256(records: Sequence[SourceFileRegistration]) -> str:
    digest = hashlib.sha256()
    for record in records:
        digest.update(_canonical_bytes(_source_identity_payload(record)))
    return digest.hexdigest()


def _blocker_checks() -> tuple[QualificationCheck, ...]:
    return (
        QualificationCheck(
            check_name="eligible_day_calendar_definition",
            result="FAIL",
            observed={"definition_provided": False, "status": "MISSING"},
            expected={"definition_provided": True, "status": "VALIDATED"},
            details="No eligible-day calendar definition/status was supplied; split freeze is blocked.",
        ),
        QualificationCheck(
            check_name="point_in_time_instrument_definitions",
            result="FAIL",
            observed={"definition_provided": False, "status": "MISSING"},
            expected={"definition_provided": True, "status": "VALIDATED"},
            details=(
                "No canonical expiry, notice, last-trade, roll-cutoff, and terminal-exit "
                "reference definition/status was supplied."
            ),
        ),
        QualificationCheck(
            check_name="point_in_time_trading_status",
            result="FAIL",
            observed={"definition_provided": False, "status": "MISSING"},
            expected={"definition_provided": True, "status": "VALIDATED"},
            details=(
                "MBP-10 alone does not provide a separately verified point-in-time trading-status "
                "reference; eligible sessions cannot be inferred or fabricated."
            ),
        ),
        QualificationCheck(
            check_name="full_row_group_quality",
            result="FAIL",
            observed={"all_row_groups_scanned": False, "status": "PENDING"},
            expected={"all_row_groups_scanned": True, "status": "VALIDATED"},
            details=(
                "Footer and content hashes do not prove event ordering or book validity; "
                "dataset/source status must remain VALIDATING/HASHED."
            ),
        ),
    )


def _require_data_path(path: Path, data_root: Path, *, label: str) -> str:
    resolved = path.resolve(strict=True)
    try:
        relative = resolved.relative_to(data_root)
    except ValueError as exc:
        raise QualificationRegistryError(f"{label} must be contained by data_root") from exc
    return relative.as_posix()


def _report_destination(data_root: Path | str, report_name: str) -> tuple[Path, Path]:
    root = Path(data_root).expanduser()
    if root.is_symlink():
        raise QualificationRegistryError("data_root cannot be a symbolic link")
    root = root.resolve(strict=True)
    if not root.is_dir():
        raise QualificationRegistryError("data_root must be a directory")
    if (
        not report_name
        or Path(report_name).name != report_name
        or not report_name.endswith(".json")
    ):
        raise QualificationRegistryError("report_name must be one .json filename")

    current = root
    for part in ("derived", "manifests"):
        current /= part
        if current.is_symlink():
            raise QualificationRegistryError(f"evidence directory cannot be a symlink: {current}")
        if current.exists() and not current.is_dir():
            raise QualificationRegistryError(f"evidence directory is not a directory: {current}")
        current.mkdir(mode=0o700, exist_ok=True)
    directory = current.resolve(strict=True)
    try:
        directory.relative_to(root)
    except ValueError as exc:
        raise QualificationRegistryError("evidence directory escaped data_root") from exc
    destination = directory / report_name
    if destination.is_symlink():
        raise QualificationRegistryError("evidence report cannot be a symbolic link")
    return root, destination


def _write_evidence(path: Path, payload: bytes) -> bool:
    if path.exists():
        if path.is_symlink() or not path.is_file():
            raise QualificationDriftError(f"evidence destination is unsafe: {path}")
        if path.read_bytes() != payload:
            raise QualificationDriftError(f"evidence report content drift at {path}")
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
        os.replace(temporary_path, path)
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except BaseException:
        handle.close()
        temporary_path.unlink(missing_ok=True)
        raise
    return True


def _single_row(row: Mapping[str, Any] | None, *, label: str) -> Mapping[str, Any]:
    if row is None:
        raise QualificationDriftError(f"{label} is missing")
    return row


def _verify_database_sources(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    dataset_key: str,
    expected_root_uri: str,
    records: Sequence[SourceFileRegistration],
    hash_manifest_sha256: str,
    first_source_date: object,
    last_source_date: object,
) -> tuple[int, str, list[Mapping[str, Any]], str]:
    connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (dataset_key,),
    )
    dataset = _single_row(
        connection.execute(
            """
            SELECT dataset_id, dataset_key, provider, feed, data_schema, root_uri,
                   status, expected_start_date, expected_end_date, manifest_sha256
            FROM systematic_fx.datasets WHERE dataset_key = %s FOR UPDATE
            """,
            (dataset_key,),
        ).fetchone(),
        label=f"dataset {dataset_key}",
    )
    expected_dataset = {
        "dataset_key": dataset_key,
        "provider": "Databento",
        "feed": DATASET_NAME,
        "data_schema": SCHEMA_NAME,
        "root_uri": expected_root_uri,
        "status": "VALIDATING",
        "expected_start_date": first_source_date,
        "expected_end_date": last_source_date,
        "manifest_sha256": hash_manifest_sha256,
    }
    drift_fields = [key for key, value in expected_dataset.items() if dataset.get(key) != value]
    if drift_fields:
        raise QualificationDriftError(
            f"dataset {dataset_key} qualification drift: {', '.join(sorted(drift_fields))}"
        )
    dataset_id = dataset.get("dataset_id")
    if isinstance(dataset_id, bool) or not isinstance(dataset_id, int):
        raise QualificationDatabaseError("database returned an invalid dataset_id")

    source_rows = connection.execute(
        """
        SELECT source_file_id, source_date, relative_uri, byte_size, sha256, row_count,
               parquet_schema_fingerprint, status
        FROM systematic_fx.source_files
        WHERE dataset_id = %s
        ORDER BY relative_uri
        FOR SHARE
        """,
        (dataset_id,),
    ).fetchall()
    if len(source_rows) != len(records):
        raise QualificationDriftError(
            f"database source count {len(source_rows)} != manifest count {len(records)}"
        )
    for record, row in zip(records, source_rows, strict=True):
        expected = {
            **_source_identity_payload(record),
            "status": "HASHED",
        }
        row_identity = {
            "byte_size": row.get("byte_size"),
            "parquet_schema_fingerprint": row.get("parquet_schema_fingerprint"),
            "relative_uri": row.get("relative_uri"),
            "row_count": row.get("row_count"),
            "sha256": row.get("sha256"),
            "source_date": (
                row["source_date"].isoformat()
                if hasattr(row.get("source_date"), "isoformat")
                else row.get("source_date")
            ),
            "status": row.get("status"),
        }
        if row_identity != expected:
            raise QualificationDriftError(
                f"database source identity/status drift for {record.relative_uri}"
            )
    return dataset_id, str(dataset["status"]), source_rows, _identity_sha256(records)


def _ensure_artifact(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    dataset_key: str,
    path: Path,
    sha256: str,
    byte_size: int,
    metadata: Mapping[str, object],
) -> tuple[int, bool]:
    artifact_key = f"source-qualification:v1:{dataset_key}"
    uri = path.as_uri()
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.artifacts
            (artifact_key, artifact_type, uri, sha256, byte_size, media_type, metadata)
        VALUES (%s, 'SOURCE_QUALIFICATION_EVIDENCE', %s, %s, %s,
                'application/json', %s)
        ON CONFLICT DO NOTHING
        RETURNING artifact_id
        """,
        (artifact_key, uri, sha256, byte_size, Jsonb(metadata)),
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
        raise QualificationDriftError(
            f"qualification artifact key/URI resolved to {len(rows)} rows"
        )
    row = rows[0]
    expected = {
        "artifact_key": artifact_key,
        "artifact_type": "SOURCE_QUALIFICATION_EVIDENCE",
        "byte_size": byte_size,
        "media_type": "application/json",
        "metadata": metadata,
        "producer_job_id": None,
        "sha256": sha256,
        "uri": uri,
    }
    drift = [key for key, value in expected.items() if row.get(key) != value]
    if drift:
        raise QualificationDriftError(f"qualification artifact drift: {', '.join(sorted(drift))}")
    artifact_id = row.get("artifact_id")
    if isinstance(artifact_id, bool) or not isinstance(artifact_id, int):
        raise QualificationDatabaseError("database returned an invalid artifact_id")
    return artifact_id, inserted is not None


def _ensure_quality_check(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    dataset_id: int,
    dataset_key: str,
    check: QualificationCheck,
    evidence_sha256: str,
    evidence_uri: str,
) -> tuple[int, bool]:
    if check.result not in _RESULTS:
        raise QualificationRegistryError(f"invalid check result: {check.result}")
    quality_check_key = f"source-qualification:v1:{dataset_key}:{check.check_name}"
    observed = {
        **dict(check.observed),
        "evidence_report_sha256": evidence_sha256,
        "evidence_report_uri": evidence_uri,
    }
    inserted = connection.execute(
        """
        INSERT INTO systematic_fx.quality_checks
            (quality_check_key, dataset_id, check_name, checker_version, result,
             observed, expected, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING quality_check_id
        """,
        (
            quality_check_key,
            dataset_id,
            check.check_name,
            CHECKER_VERSION,
            check.result,
            Jsonb(observed),
            Jsonb(dict(check.expected)),
            check.details,
        ),
    ).fetchone()
    row = _single_row(
        connection.execute(
            """
            SELECT quality_check_id, quality_check_key, dataset_id, source_file_id,
                   derived_partition_id, job_id, check_name, checker_version, result,
                   observed, expected, details
            FROM systematic_fx.quality_checks
            WHERE quality_check_key = %s
            FOR SHARE
            """,
            (quality_check_key,),
        ).fetchone(),
        label=f"quality check {quality_check_key}",
    )
    expected_row = {
        "dataset_id": dataset_id,
        "derived_partition_id": None,
        "details": check.details,
        "check_name": check.check_name,
        "checker_version": CHECKER_VERSION,
        "expected": dict(check.expected),
        "job_id": None,
        "observed": observed,
        "quality_check_key": quality_check_key,
        "result": check.result,
        "source_file_id": None,
    }
    drift = [key for key, value in expected_row.items() if row.get(key) != value]
    if drift:
        raise QualificationDriftError(
            f"quality check {check.check_name} drift: {', '.join(sorted(drift))}"
        )
    quality_check_id = row.get("quality_check_id")
    if isinstance(quality_check_id, bool) or not isinstance(quality_check_id, int):
        raise QualificationDatabaseError("database returned an invalid quality_check_id")
    return quality_check_id, inserted is not None


def register_source_qualification(
    database_url: str,
    *,
    data_root: Path | str,
    dataset_key: str,
    footer_manifest_path: Path | str,
    hash_manifest_path: Path | str,
    report_name: str = DEFAULT_REPORT_NAME,
) -> QualificationRegistrationReport:
    """Register bounded source evidence without promoting dataset/source status."""

    if not isinstance(database_url, str) or not database_url.strip():
        raise QualificationRegistryError("database_url must be a non-empty string")
    if not isinstance(dataset_key, str) or not dataset_key.strip():
        raise QualificationRegistryError("dataset_key must be a non-empty string")
    root, evidence_path = _report_destination(data_root, report_name)
    try:
        bundle = load_source_manifest_bundle(footer_manifest_path, hash_manifest_path)
    except DataRegistryError as exc:
        raise QualificationRegistryError(str(exc)) from exc
    footer_uri = _require_data_path(
        bundle.footer_manifest_path,
        root,
        label="footer manifest",
    )
    hash_uri = _require_data_path(
        bundle.hash_manifest_path,
        root,
        label="hash manifest",
    )
    footer_check, mapping_check, partial_check, mapping_summary = _manifest_checks(
        bundle.records,
        footer_manifest_sha256=bundle.footer_manifest_sha256,
    )
    manifest_identity_sha256 = _identity_sha256(bundle.records)

    try:
        with psycopg.connect(database_url, row_factory=dict_row) as connection:  # noqa: SIM117
            with connection.transaction():
                dataset_id, dataset_status, source_rows, database_identity_sha256 = (
                    _verify_database_sources(
                        connection,
                        dataset_key=dataset_key,
                        expected_root_uri=(root / "mbp-10").resolve().as_uri(),
                        records=bundle.records,
                        hash_manifest_sha256=bundle.hash_manifest_sha256,
                        first_source_date=bundle.first_source_date,
                        last_source_date=bundle.last_source_date,
                    )
                )
                hash_check = QualificationCheck(
                    check_name="full_content_hash_database_identity",
                    result=(
                        "PASS" if database_identity_sha256 == manifest_identity_sha256 else "FAIL"
                    ),
                    observed={
                        "database_identity_sha256": database_identity_sha256,
                        "file_count": bundle.file_count,
                        "hash_manifest_sha256": bundle.hash_manifest_sha256,
                        "manifest_identity_sha256": manifest_identity_sha256,
                        "total_source_bytes": bundle.total_source_bytes,
                    },
                    expected={
                        "database_manifest_one_to_one": True,
                        "file_count": bundle.file_count,
                        "hash_manifest_sha256": bundle.hash_manifest_sha256,
                    },
                    details=(
                        "Canonical URI/date/size/content hash/row/schema identities match "
                        "PostgreSQL source_files exactly 1:1."
                    ),
                )
                checks = (
                    footer_check,
                    hash_check,
                    mapping_check,
                    partial_check,
                    *_blocker_checks(),
                )
                result_counts = Counter(check.result for check in checks)
                overall_status = "BLOCKED" if result_counts["FAIL"] else "QUALIFIED"
                source_status_counts = Counter(str(row["status"]) for row in source_rows)
                report_document: dict[str, object] = {
                    "artifact_schema": "systematic_fx.source_qualification_evidence.v1",
                    "checker_version": CHECKER_VERSION,
                    "checks": [check.as_dict() for check in checks],
                    "dataset": {
                        "dataset_key": dataset_key,
                        "status": dataset_status,
                    },
                    "manifests": {
                        "footer": {
                            "relative_uri": footer_uri,
                            "sha256": bundle.footer_manifest_sha256,
                        },
                        "full_content": {
                            "relative_uri": hash_uri,
                            "sha256": bundle.hash_manifest_sha256,
                        },
                    },
                    "mapping_summary": mapping_summary,
                    "overall_status": overall_status,
                    "research_eligible": False,
                    "source_summary": {
                        "file_count": bundle.file_count,
                        "first_source_date": bundle.first_source_date.isoformat(),
                        "identity_sha256": manifest_identity_sha256,
                        "last_source_date": bundle.last_source_date.isoformat(),
                        "status_counts": dict(sorted(source_status_counts.items())),
                        "total_source_bytes": bundle.total_source_bytes,
                    },
                }
                evidence_bytes = _canonical_bytes(report_document)
                evidence_sha256 = hashlib.sha256(evidence_bytes).hexdigest()
                created_evidence = _write_evidence(evidence_path, evidence_bytes)
                artifact_metadata = {
                    "checker_version": CHECKER_VERSION,
                    "dataset_key": dataset_key,
                    "overall_status": overall_status,
                    "quality_check_count": len(checks),
                    "research_eligible": False,
                }
                artifact_id, created_artifact = _ensure_artifact(
                    connection,
                    dataset_key=dataset_key,
                    path=evidence_path,
                    sha256=evidence_sha256,
                    byte_size=len(evidence_bytes),
                    metadata=artifact_metadata,
                )
                quality_check_ids: list[int] = []
                created_quality_checks = 0
                for check in checks:
                    quality_check_id, created = _ensure_quality_check(
                        connection,
                        dataset_id=dataset_id,
                        dataset_key=dataset_key,
                        check=check,
                        evidence_sha256=evidence_sha256,
                        evidence_uri=evidence_path.as_uri(),
                    )
                    quality_check_ids.append(quality_check_id)
                    created_quality_checks += created

                final_dataset_status = connection.execute(
                    "SELECT status FROM systematic_fx.datasets WHERE dataset_id = %s",
                    (dataset_id,),
                ).fetchone()
                final_source_counts = connection.execute(
                    """
                    SELECT status, count(*)::integer AS count
                    FROM systematic_fx.source_files WHERE dataset_id = %s
                    GROUP BY status ORDER BY status
                    """,
                    (dataset_id,),
                ).fetchall()
                if final_dataset_status != {"status": "VALIDATING"}:
                    raise QualificationDriftError("qualification changed dataset status")
                observed_final_counts = {
                    str(row["status"]): int(row["count"]) for row in final_source_counts
                }
                if observed_final_counts != dict(source_status_counts):
                    raise QualificationDriftError("qualification changed source status counts")

                return QualificationRegistrationReport(
                    dataset_id=dataset_id,
                    dataset_key=dataset_key,
                    dataset_status=dataset_status,
                    source_status_counts=dict(sorted(source_status_counts.items())),
                    evidence_path=evidence_path,
                    evidence_sha256=evidence_sha256,
                    evidence_byte_size=len(evidence_bytes),
                    artifact_id=artifact_id,
                    quality_check_ids=tuple(quality_check_ids),
                    created_evidence_file=created_evidence,
                    created_artifact=created_artifact,
                    created_quality_checks=created_quality_checks,
                    overall_status=overall_status,
                    research_eligible=False,
                )
    except QualificationRegistryError:
        raise
    except psycopg.Error as exc:
        raise QualificationDatabaseError("PostgreSQL source qualification failed") from exc
