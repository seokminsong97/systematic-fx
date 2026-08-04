import copy
import hashlib
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

from systematic_fx.db import full_qc_registry
from systematic_fx.db.full_qc_registry import (
    FullQcManifestError,
    FullQcRegistryDriftError,
    prepare_full_qc_registration,
    register_full_qc_scan,
)


def _canonical(record: dict[str, object]) -> str:
    return json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"


def _source(day: date, byte_size: int, digest: str) -> dict[str, object]:
    return {
        "byte_size": byte_size,
        "relative_uri": (
            f"{day.year:04d}/{day.month:02d}/{day.day:02d}/glbx-mdp3-{day:%Y%m%d}.mbp-10.parquet"
        ),
        "sha256": digest,
        "source_date": day.isoformat(),
    }


def _scan(
    source: dict[str, object],
    *,
    source_manifest_sha256: str,
    rows: int,
    row_groups: int,
    diagnostic_counts: dict[str, int] | None = None,
    hard_violation_counts: dict[str, int] | None = None,
) -> dict[str, object]:
    hard = dict.fromkeys(full_qc_registry.HARD_CHECKS, 0)
    hard.update(hard_violation_counts or {})
    diagnostics = dict.fromkeys(full_qc_registry.DIAGNOSTIC_CHECKS, 0)
    diagnostics.update(diagnostic_counts or {})
    return {
        "artifact_schema": full_qc_registry.SCAN_ARTIFACT_SCHEMA,
        "checker_version": full_qc_registry.SCANNER_CHECKER_VERSION,
        "config_sha256": "c" * 64,
        "coverage_complete": True,
        "diagnostic_counts": diagnostics,
        "expected_row_count": rows,
        "expected_row_group_count": row_groups,
        "first_ts_recv_ns": 1_700_000_000_000_000_000,
        "hard_violation_count": sum(hard.values()),
        "hard_violation_counts": hard,
        "last_ts_recv_ns": 1_700_000_001_000_000_000,
        "relative_uri": source["relative_uri"],
        "research_eligible": False,
        "result": "FAIL" if sum(hard.values()) else "PASS",
        "scanned_row_count": rows,
        "scanned_row_group_count": row_groups,
        "schema_fingerprint": "1" * 64,
        "source_byte_size": source["byte_size"],
        "source_date": source["source_date"],
        "source_manifest_sha256": source_manifest_sha256,
        "source_sha256": source["sha256"],
    }


def _write_inputs(
    root: Path,
    *,
    hard_failure: bool = False,
    diagnostic: bool = True,
) -> tuple[Path, Path, list[dict[str, object]]]:
    manifests = root / "derived" / "manifests"
    manifests.mkdir(parents=True, exist_ok=True)
    (root / "mbp-10").mkdir(exist_ok=True)
    sources = [
        _source(date(2024, 1, 2), 10, "a" * 64),
        _source(date(2024, 1, 3), 20, "b" * 64),
    ]
    source_path = manifests / "mbp10_source_sha256_v1.jsonl"
    source_path.write_text("".join(_canonical(record) for record in sources))
    source_sha256 = hashlib.sha256(source_path.read_bytes()).hexdigest()
    scans = [
        _scan(
            sources[0],
            source_manifest_sha256=source_sha256,
            rows=100,
            row_groups=2,
            diagnostic_counts={"maybe_bad_book_flag_rows": 2} if diagnostic else {},
            hard_violation_counts={"unexpected_rtype": 1} if hard_failure else {},
        ),
        _scan(
            sources[1],
            source_manifest_sha256=source_sha256,
            rows=200,
            row_groups=3,
        ),
    ]
    scan_path = manifests / "mbp10_structural_qc_v1.jsonl"
    scan_path.write_text("".join(_canonical(record) for record in scans))
    return source_path, scan_path, scans


class _Result:
    def __init__(self, *, one=None, many=None):  # type: ignore[no-untyped-def]
        self.one = one
        self.many = many or []

    def fetchone(self):  # type: ignore[no-untyped-def]
        return self.one

    def fetchall(self):  # type: ignore[no-untyped-def]
        return self.many


class _Transaction:
    def __init__(self, connection):  # type: ignore[no-untyped-def]
        self.connection = connection
        self.snapshot = None

    def __enter__(self):  # type: ignore[no-untyped-def]
        self.snapshot = copy.deepcopy(
            (self.connection.jobs, self.connection.artifacts, self.connection.checks)
        )
        return self

    def __exit__(self, exception_type, _exception, _traceback):  # type: ignore[no-untyped-def]
        self.connection.transaction_exit_types.append(exception_type)
        if exception_type is not None:
            self.connection.jobs, self.connection.artifacts, self.connection.checks = self.snapshot
        return False


class _FakeConnection:
    def __init__(self, root: Path, source_manifest_sha256: str) -> None:
        self.dataset = {
            "dataset_id": 7,
            "dataset_key": "dataset-v1",
            "provider": "Databento",
            "feed": "GLBX.MDP3",
            "data_schema": "mbp-10",
            "root_uri": (root / "mbp-10").resolve().as_uri(),
            "price_scale_exponent": -9,
            "status": "VALIDATING",
            "expected_start_date": date(2024, 1, 2),
            "expected_end_date": date(2024, 1, 3),
            "manifest_sha256": source_manifest_sha256,
        }
        self.sources = [
            {
                "source_file_id": 1,
                "source_date": date(2024, 1, 2),
                "relative_uri": "2024/01/02/glbx-mdp3-20240102.mbp-10.parquet",
                "byte_size": 10,
                "sha256": "a" * 64,
                "row_count": 100,
                "parquet_schema_fingerprint": "1" * 64,
                "status": "HASHED",
                "validated_at": None,
            },
            {
                "source_file_id": 2,
                "source_date": date(2024, 1, 3),
                "relative_uri": "2024/01/03/glbx-mdp3-20240103.mbp-10.parquet",
                "byte_size": 20,
                "sha256": "b" * 64,
                "row_count": 200,
                "parquet_schema_fingerprint": "1" * 64,
                "status": "HASHED",
                "validated_at": None,
            },
        ]
        self.jobs: dict[str, dict[str, object]] = {}
        self.artifacts: dict[str, dict[str, object]] = {}
        self.checks: dict[str, dict[str, object]] = {
            "source-qualification:v1:dataset-v1:full_row_group_quality": {
                "quality_check_id": 50,
                "result": "FAIL",
            }
        }
        self.executed_sql: list[str] = []
        self.transaction_exit_types: list[object] = []
        self.fail_on_quality_insert = False

    def __enter__(self):
        return self

    def __exit__(self, _exception_type, _exception, _traceback):
        return False

    def transaction(self):
        return _Transaction(self)

    def execute(self, sql, parameters=()):  # type: ignore[no-untyped-def]
        normalized = " ".join(sql.split())
        self.executed_sql.append(normalized)
        if "pg_advisory_xact_lock" in normalized:
            return _Result()
        if "FROM systematic_fx.datasets" in normalized and "dataset_key = %s" in normalized:
            return _Result(one=self.dataset)
        if (
            "source_date, relative_uri" in normalized
            and "FROM systematic_fx.source_files" in normalized
        ):
            return _Result(many=self.sources)
        if normalized.startswith("INSERT INTO systematic_fx.jobs"):
            key, dataset_id, idempotency_key, payload, result = parameters
            created = key not in self.jobs
            if created:
                self.jobs[key] = {
                    "job_id": 20,
                    "job_key": key,
                    "parent_job_id": None,
                    "dataset_id": dataset_id,
                    "job_type": "RECORD_FULL_MBP10_STRUCTURAL_SCAN",
                    "status": "SUCCEEDED",
                    "priority": 0,
                    "idempotency_key": idempotency_key,
                    "payload": payload.obj,
                    "result": result.obj,
                    "attempts": 1,
                    "max_attempts": 1,
                    "worker_id": None,
                    "leased_until": None,
                    "error_message": None,
                }
            return _Result(one={"job_id": 20} if created else None)
        if "FROM systematic_fx.jobs" in normalized:
            rows = [row for row in self.jobs.values() if row["job_key"] == parameters[0]]
            return _Result(many=rows)
        if normalized.startswith("INSERT INTO systematic_fx.artifacts"):
            key, uri, sha256, byte_size, job_id, metadata = parameters
            created = key not in self.artifacts
            if created:
                self.artifacts[key] = {
                    "artifact_id": 30,
                    "artifact_key": key,
                    "artifact_type": "FULL_MBP10_STRUCTURAL_SCAN_EVIDENCE",
                    "uri": uri,
                    "sha256": sha256,
                    "byte_size": byte_size,
                    "media_type": "application/json",
                    "producer_job_id": job_id,
                    "metadata": metadata.obj,
                }
            return _Result(one={"artifact_id": 30} if created else None)
        if "FROM systematic_fx.artifacts" in normalized:
            rows = [
                row
                for row in self.artifacts.values()
                if row["artifact_key"] == parameters[0] or row["uri"] == parameters[1]
            ]
            return _Result(many=rows)
        if normalized.startswith("INSERT INTO systematic_fx.quality_checks"):
            if self.fail_on_quality_insert:
                self.fail_on_quality_insert = False
                raise FullQcRegistryDriftError("injected quality-check failure")
            (
                key,
                dataset_id,
                source_file_id,
                job_id,
                check_name,
                checker_version,
                result,
                observed,
                expected,
                details,
            ) = parameters
            created = key not in self.checks
            if created:
                self.checks[key] = {
                    "quality_check_id": 100 + len(self.checks),
                    "quality_check_key": key,
                    "dataset_id": dataset_id,
                    "source_file_id": source_file_id,
                    "derived_partition_id": None,
                    "job_id": job_id,
                    "check_name": check_name,
                    "checker_version": checker_version,
                    "result": result,
                    "observed": observed.obj,
                    "expected": expected.obj,
                    "details": details,
                }
            return _Result(
                one={"quality_check_id": self.checks[key]["quality_check_id"]} if created else None
            )
        if "FROM systematic_fx.quality_checks" in normalized:
            return _Result(one=self.checks.get(parameters[0]))
        if normalized.startswith("SELECT status FROM systematic_fx.datasets"):
            return _Result(one={"status": self.dataset["status"]})
        if "SELECT source_file_id, status" in normalized:
            return _Result(
                many=[
                    {"source_file_id": row["source_file_id"], "status": row["status"]}
                    for row in self.sources
                ]
            )
        raise AssertionError(f"unexpected SQL: {normalized}")


class FullQcPreparationTest(unittest.TestCase):
    def test_prepares_canonical_content_addressed_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            root.mkdir()
            source_path, scan_path, _ = _write_inputs(root)

            first = prepare_full_qc_registration(
                data_root=root,
                dataset_key="dataset-v1",
                scan_manifest_path=scan_path,
                source_manifest_path=source_path,
            )
            second = prepare_full_qc_registration(
                data_root=root,
                dataset_key="dataset-v1",
                scan_manifest_path=scan_path,
                source_manifest_path=source_path,
            )

            self.assertEqual(first.aggregate_result, "PASS")
            self.assertEqual(first.diagnostic_result, "WARN")
            self.assertEqual(first.result_counts, {"ERROR": 0, "FAIL": 0, "PASS": 2, "WARN": 0})
            self.assertEqual(first.diagnostic_counts["maybe_bad_book_flag_rows"], 2)
            self.assertEqual(sum(first.diagnostic_counts.values()), 2)
            self.assertTrue(first.created_evidence)
            self.assertFalse(second.created_evidence)
            self.assertEqual(first.evidence_sha256, second.evidence_sha256)
            self.assertIn(
                "data/derived/manifests/full_qc_registry_v1/sha256", first.evidence_path.as_posix()
            )
            evidence = json.loads(first.evidence_bytes)
            self.assertEqual(first.evidence_bytes, _canonical(evidence).encode())
            self.assertFalse(evidence["research_eligible"])
            self.assertEqual(evidence["status_effect"], "NONE")

    def test_rejects_noncanonical_incomplete_and_identity_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            root.mkdir()
            source_path, scan_path, scans = _write_inputs(root)
            cases = []

            noncanonical = "".join(json.dumps(record) + "\n" for record in scans)
            cases.append(("not canonical", noncanonical))
            incomplete = copy.deepcopy(scans)
            incomplete[0]["coverage_complete"] = False
            cases.append(
                ("coverage_complete", "".join(_canonical(record) for record in incomplete))
            )
            identity_drift = copy.deepcopy(scans)
            identity_drift[0]["source_sha256"] = "f" * 64
            cases.append(
                ("identity drift", "".join(_canonical(record) for record in identity_drift))
            )
            reversed_order = list(reversed(scans))
            cases.append(("path ordered", "".join(_canonical(record) for record in reversed_order)))

            for expected_message, payload in cases:
                with self.subTest(expected_message=expected_message):
                    scan_path.write_text(payload)
                    with self.assertRaisesRegex(FullQcManifestError, expected_message):
                        prepare_full_qc_registration(
                            data_root=root,
                            dataset_key="dataset-v1",
                            scan_manifest_path=scan_path,
                            source_manifest_path=source_path,
                        )

    def test_rejects_checkpoint_filename(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            root.mkdir()
            source_path, scan_path, _ = _write_inputs(root)
            checkpoint = scan_path.with_name("mbp10_structural_qc_v1.checkpoint.jsonl")
            checkpoint.write_bytes(scan_path.read_bytes())

            with self.assertRaisesRegex(FullQcManifestError, "checkpoint"):
                prepare_full_qc_registration(
                    data_root=root,
                    dataset_key="dataset-v1",
                    scan_manifest_path=checkpoint,
                    source_manifest_path=source_path,
                )

    def test_global_timestamp_reversal_remains_non_gating_diagnostic(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            root.mkdir()
            source_path, scan_path, scans = _write_inputs(root)
            scans[0]["first_ts_recv_ns"] = 1_700_000_001_000_000_000
            scans[0]["last_ts_recv_ns"] = 1_700_000_000_000_000_000
            scans[0]["diagnostic_counts"]["global_ts_recv_regression"] = 1
            scan_path.write_text("".join(_canonical(record) for record in scans))

            prepared = prepare_full_qc_registration(
                data_root=root,
                dataset_key="dataset-v1",
                scan_manifest_path=scan_path,
                source_manifest_path=source_path,
            )

            self.assertEqual(prepared.aggregate_result, "PASS")
            self.assertEqual(prepared.diagnostic_result, "WARN")


class FullQcRegistryTest(unittest.TestCase):
    def _register(self, root: Path, connection: _FakeConnection):
        source_path = root / "derived" / "manifests" / "mbp10_source_sha256_v1.jsonl"
        scan_path = root / "derived" / "manifests" / "mbp10_structural_qc_v1.jsonl"
        with mock.patch.object(full_qc_registry.psycopg, "connect", return_value=connection):
            return register_full_qc_scan(
                "postgresql:///systematic_fx",
                data_root=root,
                dataset_key="dataset-v1",
                scan_manifest_path=scan_path,
                source_manifest_path=source_path,
            )

    def test_registers_append_only_checks_and_replays_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            root.mkdir()
            source_path, _, _ = _write_inputs(root)
            connection = _FakeConnection(
                root,
                hashlib.sha256(source_path.read_bytes()).hexdigest(),
            )
            qualification_before = copy.deepcopy(connection.checks)

            first = self._register(root, connection)
            second = self._register(root, connection)

            self.assertTrue(first.created_job)
            self.assertTrue(first.created_artifact)
            self.assertEqual(first.created_quality_checks, 4)
            self.assertFalse(second.created_job)
            self.assertFalse(second.created_artifact)
            self.assertEqual(second.created_quality_checks, 0)
            self.assertEqual(first.source_quality_check_ids, second.source_quality_check_ids)
            self.assertEqual(connection.dataset["status"], "VALIDATING")
            self.assertEqual({row["status"] for row in connection.sources}, {"HASHED"})
            self.assertEqual(first.aggregate_result, "PASS")
            self.assertEqual(first.diagnostic_result, "WARN")
            self.assertEqual(len(first.source_quality_check_ids), 2)
            self.assertEqual(
                connection.checks["source-qualification:v1:dataset-v1:full_row_group_quality"],
                qualification_before["source-qualification:v1:dataset-v1:full_row_group_quality"],
            )
            source_checks = [
                row
                for row in connection.checks.values()
                if row.get("check_name") == full_qc_registry.SOURCE_CHECK_NAME
            ]
            self.assertEqual({row["result"] for row in source_checks}, {"PASS"})
            self.assertEqual(
                sum(
                    row["observed"]["diagnostic_counts"].get("maybe_bad_book_flag_rows", 0)
                    for row in source_checks
                ),
                2,
            )
            self.assertFalse(any(sql.startswith("UPDATE ") for sql in connection.executed_sql))

    def test_hard_failure_is_quality_fail_but_job_succeeds_without_status_change(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            root.mkdir()
            source_path, _, _ = _write_inputs(root, hard_failure=True)
            connection = _FakeConnection(
                root,
                hashlib.sha256(source_path.read_bytes()).hexdigest(),
            )

            result = self._register(root, connection)

            self.assertEqual(result.aggregate_result, "FAIL")
            self.assertEqual(next(iter(connection.jobs.values()))["status"], "SUCCEEDED")
            self.assertEqual(connection.dataset["status"], "VALIDATING")
            self.assertEqual({row["status"] for row in connection.sources}, {"HASHED"})

    def test_database_schema_drift_stops_before_inserts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            root.mkdir()
            source_path, _, _ = _write_inputs(root)
            connection = _FakeConnection(
                root,
                hashlib.sha256(source_path.read_bytes()).hexdigest(),
            )
            connection.sources[0]["parquet_schema_fingerprint"] = "9" * 64

            with self.assertRaisesRegex(FullQcRegistryDriftError, "parquet_schema_fingerprint"):
                self._register(root, connection)

            self.assertEqual(connection.jobs, {})
            self.assertEqual(connection.artifacts, {})

    def test_existing_quality_check_drift_aborts_replay(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            root.mkdir()
            source_path, _, _ = _write_inputs(root)
            connection = _FakeConnection(
                root,
                hashlib.sha256(source_path.read_bytes()).hexdigest(),
            )
            first = self._register(root, connection)
            source_key = next(
                key
                for key, row in connection.checks.items()
                if row.get("check_name") == full_qc_registry.SOURCE_CHECK_NAME
            )
            connection.checks[source_key]["result"] = "FAIL"
            before = copy.deepcopy((connection.jobs, connection.artifacts, connection.checks))

            with self.assertRaisesRegex(FullQcRegistryDriftError, "quality check"):
                self._register(root, connection)

            self.assertEqual((connection.jobs, connection.artifacts, connection.checks), before)
            self.assertEqual(first.created_quality_checks, 4)

    def test_injected_failure_rolls_back_all_database_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            root.mkdir()
            source_path, _, _ = _write_inputs(root)
            connection = _FakeConnection(
                root,
                hashlib.sha256(source_path.read_bytes()).hexdigest(),
            )
            original_checks = copy.deepcopy(connection.checks)
            connection.fail_on_quality_insert = True

            with self.assertRaisesRegex(FullQcRegistryDriftError, "injected"):
                self._register(root, connection)

            self.assertEqual(connection.jobs, {})
            self.assertEqual(connection.artifacts, {})
            self.assertEqual(connection.checks, original_checks)
            evidence_files = list(
                (root / "derived" / "manifests" / "full_qc_registry_v1").rglob("*.json")
            )
            self.assertEqual(len(evidence_files), 1)


if __name__ == "__main__":
    unittest.main()
