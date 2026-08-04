import copy
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

from systematic_fx import cli
from systematic_fx.config import Settings
from systematic_fx.db import qualification_registry
from systematic_fx.db.qualification_registry import (
    QualificationDriftError,
    register_source_qualification,
)


def _canonical(record: dict[str, object]) -> str:
    return json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"


def _manifest_records(day: date, size: int, rows: int, digest: str, partial: bool):
    uri = f"{day.year:04d}/{day.month:02d}/{day.day:02d}/glbx-mdp3-{day:%Y%m%d}.mbp-10.parquet"
    footer = {
        "column_count": 73,
        "contract": {
            "dataset": "GLBX.MDP3",
            "dbn_version": 3,
            "price_encoding": "fixed",
            "price_scale": "1e-9",
            "schema": "mbp-10",
            "undefined_price": 9223372036854775807,
        },
        "file_size_bytes": size,
        "instrument_kind_counts": {
            "calendar_spread": 2,
            "outright": 1,
            "unknown": 0,
        },
        "instrument_mappings": [],
        "mapping_interval_count": 3,
        "not_found": [],
        "partial": ["6EZ4"] if partial else [],
        "path": uri,
        "row_count": rows,
        "schema_fingerprint": qualification_registry.EXPECTED_SCHEMA_FINGERPRINT,
        "source_date": day.isoformat(),
    }
    hashed = {
        "byte_size": size,
        "relative_uri": uri,
        "sha256": digest,
        "source_date": day.isoformat(),
    }
    return footer, hashed


def _write_inputs(root: Path) -> tuple[Path, Path]:
    manifests = root / "derived" / "manifests"
    manifests.mkdir(parents=True, exist_ok=True)
    (root / "mbp-10").mkdir(exist_ok=True)
    pairs = (
        _manifest_records(date(2024, 1, 2), 10, 100, "a" * 64, True),
        _manifest_records(date(2024, 1, 3), 20, 200, "b" * 64, False),
    )
    footer = manifests / "mbp10_footer_manifest_v1.jsonl"
    hashed = manifests / "mbp10_source_sha256_v1.jsonl"
    footer.write_text("".join(_canonical(pair[0]) for pair in pairs))
    hashed.write_text("".join(_canonical(pair[1]) for pair in pairs))
    return footer, hashed


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
        self.snapshot = (
            copy.deepcopy(self.connection.artifact),
            copy.deepcopy(self.connection.checks),
        )
        return self

    def __exit__(self, exception_type, _exception, _traceback):  # type: ignore[no-untyped-def]
        self.connection.transaction_exit_types.append(exception_type)
        if exception_type is not None:
            self.connection.artifact, self.connection.checks = self.snapshot
        return False


class _FakeConnection:
    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root
        self.dataset = {
            "dataset_id": 7,
            "dataset_key": "dataset-v1",
            "provider": "Databento",
            "feed": "GLBX.MDP3",
            "data_schema": "mbp-10",
            "root_uri": (data_root / "mbp-10").resolve().as_uri(),
            "status": "VALIDATING",
            "expected_start_date": date(2024, 1, 2),
            "expected_end_date": date(2024, 1, 3),
            "manifest_sha256": None,
        }
        self.sources = [
            {
                "source_file_id": 1,
                "source_date": date(2024, 1, 2),
                "relative_uri": "2024/01/02/glbx-mdp3-20240102.mbp-10.parquet",
                "byte_size": 10,
                "sha256": "a" * 64,
                "row_count": 100,
                "parquet_schema_fingerprint": qualification_registry.EXPECTED_SCHEMA_FINGERPRINT,
                "status": "HASHED",
            },
            {
                "source_file_id": 2,
                "source_date": date(2024, 1, 3),
                "relative_uri": "2024/01/03/glbx-mdp3-20240103.mbp-10.parquet",
                "byte_size": 20,
                "sha256": "b" * 64,
                "row_count": 200,
                "parquet_schema_fingerprint": qualification_registry.EXPECTED_SCHEMA_FINGERPRINT,
                "status": "HASHED",
            },
        ]
        self.artifact = None
        self.checks = {}
        self.transaction_exit_types = []
        self.fail_on_first_check = False

    def __enter__(self):
        return self

    def __exit__(self, _exception_type, _exception, _traceback):
        return False

    def transaction(self):
        return _Transaction(self)

    def execute(self, sql, parameters=()):  # type: ignore[no-untyped-def]
        normalized = " ".join(sql.split())
        if "pg_advisory_xact_lock" in normalized:
            return _Result()
        if "FROM systematic_fx.datasets WHERE dataset_key" in normalized:
            return _Result(one=self.dataset)
        if "FROM systematic_fx.source_files" in normalized and "FOR SHARE" in normalized:
            return _Result(many=self.sources)
        if normalized.startswith("INSERT INTO systematic_fx.artifacts"):
            artifact_key, uri, sha256, byte_size, metadata = parameters
            created = self.artifact is None
            if created:
                self.artifact = {
                    "artifact_id": 11,
                    "artifact_key": artifact_key,
                    "artifact_type": "SOURCE_QUALIFICATION_EVIDENCE",
                    "uri": uri,
                    "sha256": sha256,
                    "byte_size": byte_size,
                    "media_type": "application/json",
                    "producer_job_id": None,
                    "metadata": metadata.obj,
                }
            return _Result(one={"artifact_id": 11} if created else None)
        if "FROM systematic_fx.artifacts" in normalized:
            return _Result(many=[self.artifact] if self.artifact else [])
        if normalized.startswith("INSERT INTO systematic_fx.quality_checks"):
            if self.fail_on_first_check:
                self.fail_on_first_check = False
                raise QualificationDriftError("injected quality-check failure")
            (
                key,
                dataset_id,
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
                    "source_file_id": None,
                    "derived_partition_id": None,
                    "job_id": None,
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
        if "GROUP BY status ORDER BY status" in normalized:
            counts = {}
            for row in self.sources:
                counts[row["status"]] = counts.get(row["status"], 0) + 1
            return _Result(
                many=[
                    {"status": status, "count": count} for status, count in sorted(counts.items())
                ]
            )
        raise AssertionError(f"unexpected SQL: {normalized}")


class QualificationRegistryTest(unittest.TestCase):
    def _register(self, root: Path, connection: _FakeConnection):
        footer, hashed = _write_inputs(root)
        from systematic_fx.db.data_registry import load_source_manifest_bundle

        connection.dataset["manifest_sha256"] = load_source_manifest_bundle(
            footer, hashed
        ).hash_manifest_sha256
        with mock.patch.object(
            qualification_registry.psycopg,
            "connect",
            return_value=connection,
        ):
            return register_source_qualification(
                "postgresql:///systematic_fx",
                data_root=root,
                dataset_key="dataset-v1",
                footer_manifest_path=footer,
                hash_manifest_path=hashed,
            )

    def test_registers_canonical_blocked_evidence_and_replays_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            root.mkdir()
            connection = _FakeConnection(root)

            first = self._register(root, connection)
            second = self._register(root, connection)

            self.assertEqual(first.overall_status, "BLOCKED")
            self.assertFalse(first.research_eligible)
            self.assertEqual(first.dataset_status, "VALIDATING")
            self.assertEqual(first.source_status_counts, {"HASHED": 2})
            self.assertTrue(first.created_evidence_file)
            self.assertTrue(first.created_artifact)
            self.assertEqual(first.created_quality_checks, 8)
            self.assertFalse(second.created_evidence_file)
            self.assertFalse(second.created_artifact)
            self.assertEqual(second.created_quality_checks, 0)
            self.assertEqual(first.quality_check_ids, second.quality_check_ids)
            evidence_bytes = first.evidence_path.read_bytes()
            evidence = json.loads(evidence_bytes)
            self.assertEqual(evidence_bytes, _canonical(evidence).encode())
            results = {check["check_name"]: check["result"] for check in evidence["checks"]}
            self.assertEqual(results["footer_exact_contract_identity"], "PASS")
            self.assertEqual(results["full_content_hash_database_identity"], "PASS")
            self.assertEqual(results["mapping_classification"], "PASS")
            self.assertEqual(results["provider_partial_metadata"], "WARN")
            self.assertEqual(results["eligible_day_calendar_definition"], "FAIL")
            self.assertEqual(results["point_in_time_instrument_definitions"], "FAIL")
            self.assertEqual(results["point_in_time_trading_status"], "FAIL")
            self.assertEqual(results["full_row_group_quality"], "FAIL")
            self.assertEqual(connection.dataset["status"], "VALIDATING")
            self.assertEqual({row["status"] for row in connection.sources}, {"HASHED"})
            self.assertEqual(connection.transaction_exit_types, [None, None])

    def test_existing_quality_check_drift_aborts_replay_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            root.mkdir()
            connection = _FakeConnection(root)
            self._register(root, connection)
            key = "source-qualification:v1:dataset-v1:mapping_classification"
            connection.checks[key]["result"] = "WARN"
            artifact_before = copy.deepcopy(connection.artifact)
            checks_before = copy.deepcopy(connection.checks)

            with self.assertRaisesRegex(QualificationDriftError, "mapping_classification drift"):
                self._register(root, connection)

            self.assertEqual(connection.artifact, artifact_before)
            self.assertEqual(connection.checks, checks_before)
            self.assertIs(connection.transaction_exit_types[-1], QualificationDriftError)

    def test_failure_after_artifact_insert_rolls_back_database_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            root.mkdir()
            connection = _FakeConnection(root)
            connection.fail_on_first_check = True

            with self.assertRaisesRegex(QualificationDriftError, "injected"):
                self._register(root, connection)

            self.assertIsNone(connection.artifact)
            self.assertEqual(connection.checks, {})
            self.assertIs(connection.transaction_exit_types[-1], QualificationDriftError)
            self.assertTrue(
                (root / "derived" / "manifests" / "mbp10_source_qualification_v1.json").is_file()
            )

    def test_existing_evidence_file_content_drift_stops_before_database_writes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            root.mkdir()
            connection = _FakeConnection(root)
            first = self._register(root, connection)
            first.evidence_path.write_text('{"drift":true}\n')
            artifact_before = copy.deepcopy(connection.artifact)

            with self.assertRaisesRegex(QualificationDriftError, "evidence report content drift"):
                self._register(root, connection)

            self.assertEqual(connection.artifact, artifact_before)

    def test_cli_parser_exposes_bounded_qualification_command(self) -> None:
        args = cli.build_parser().parse_args(
            [
                "data",
                "qualify",
                "--dataset-key",
                "dataset-v1",
                "--database-url",
                "postgresql:///systematic_fx",
                "--json",
            ]
        )

        self.assertEqual(args.dataset_key, "dataset-v1")
        self.assertEqual(args.report_name, "mbp10_source_qualification_v1.json")
        self.assertIs(args.handler, cli._qualify_data_command)

    def test_cli_returns_blocked_without_treating_it_as_execution_error(self) -> None:
        fake_result = mock.MagicMock(overall_status="BLOCKED")
        fake_result.as_dict.return_value = {"overall_status": "BLOCKED"}
        settings = Settings(
            data_root=Path("/tmp/systematic-fx-test-data"),
            artifacts_root=Path("/tmp/systematic-fx-test-artifacts"),
            database_url="postgresql:///systematic_fx",
        )
        args = cli.build_parser().parse_args(["data", "qualify", "--json"])
        with (
            mock.patch.object(cli.Settings, "from_env", return_value=settings),
            mock.patch.object(
                qualification_registry,
                "register_source_qualification",
                return_value=fake_result,
            ) as register,
            mock.patch("builtins.print"),
        ):
            exit_code = args.handler(args)

        self.assertEqual(exit_code, 1)
        register.assert_called_once()


if __name__ == "__main__":
    unittest.main()
