import hashlib
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

from systematic_fx.db import data_registry
from systematic_fx.db.data_registry import (
    DataRegistryDatabaseError,
    DatasetRegistration,
    ManifestValidationError,
    RegistryDriftError,
    load_source_manifest_bundle,
    register_source_manifests,
)

SCHEMA_FINGERPRINT = "1" * 64


def _canonical(record: dict[str, object]) -> str:
    return json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"


def _footer_record(day: date, byte_size: int, row_count: int) -> dict[str, object]:
    relative_uri = (
        f"{day.year:04d}/{day.month:02d}/{day.day:02d}/glbx-mdp3-{day:%Y%m%d}.mbp-10.parquet"
    )
    return {
        "column_count": 73,
        "contract": {
            "dataset": "GLBX.MDP3",
            "price_scale": "1e-9",
            "schema": "mbp-10",
        },
        "dbn_end_ns": 2,
        "dbn_start_ns": 1,
        "file_size_bytes": byte_size,
        "instrument_mappings": [{"instrument_id": 123, "raw_symbol": "6EH4"}],
        "mapping_interval_count": 1,
        "path": relative_uri,
        "row_count": row_count,
        "schema_fingerprint": SCHEMA_FINGERPRINT,
        "source_date": day.isoformat(),
    }


def _hash_record(day: date, byte_size: int, digit: str) -> dict[str, object]:
    relative_uri = (
        f"{day.year:04d}/{day.month:02d}/{day.day:02d}/glbx-mdp3-{day:%Y%m%d}.mbp-10.parquet"
    )
    return {
        "byte_size": byte_size,
        "relative_uri": relative_uri,
        "sha256": digit * 64,
        "source_date": day.isoformat(),
    }


def _write_manifests(
    directory: Path,
    *,
    footer_records: list[dict[str, object]] | None = None,
    hash_records: list[dict[str, object]] | None = None,
) -> tuple[Path, Path]:
    footer_records = footer_records or [
        _footer_record(date(2024, 1, 2), 10, 100),
        _footer_record(date(2024, 1, 3), 20, 200),
    ]
    hash_records = hash_records or [
        _hash_record(date(2024, 1, 2), 10, "a"),
        _hash_record(date(2024, 1, 3), 20, "b"),
    ]
    footer_path = directory / "footer.jsonl"
    hash_path = directory / "hash.jsonl"
    footer_path.write_text("".join(_canonical(record) for record in footer_records))
    hash_path.write_text("".join(_canonical(record) for record in hash_records))
    return footer_path, hash_path


def _dataset() -> DatasetRegistration:
    return DatasetRegistration(
        dataset_key="test_glbx_mdp3_mbp10_v1",
        root_uri="/data/mbp-10",
    )


class SourceManifestBundleTest(unittest.TestCase):
    def test_loads_exact_pair_and_excludes_mapping_payload_from_footer_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            footer_path, hash_path = _write_manifests(Path(directory))

            bundle = load_source_manifest_bundle(footer_path, hash_path)

            self.assertEqual(bundle.file_count, 2)
            self.assertEqual(bundle.total_source_bytes, 30)
            self.assertEqual(bundle.first_source_date, date(2024, 1, 2))
            self.assertEqual(bundle.last_source_date, date(2024, 1, 3))
            self.assertEqual(
                bundle.footer_manifest_sha256,
                hashlib.sha256(footer_path.read_bytes()).hexdigest(),
            )
            self.assertEqual(
                bundle.hash_manifest_sha256,
                hashlib.sha256(hash_path.read_bytes()).hexdigest(),
            )
            first = bundle.records[0]
            self.assertEqual(first.sha256, "a" * 64)
            self.assertEqual(first.row_count, 100)
            self.assertEqual(first.schema_fingerprint, SCHEMA_FINGERPRINT)
            self.assertNotIn("instrument_mappings", first.footer_metadata)
            self.assertEqual(first.footer_metadata["mapping_interval_count"], 1)

    def test_rejects_uri_size_date_and_cardinality_mismatches(self) -> None:
        mutations = {
            "identity mismatch": lambda record: record.update(relative_uri="2024/01/03/x.parquet"),
            "size mismatch": lambda record: record.update(byte_size=999),
            "date mismatch": lambda record: record.update(source_date="2024-01-04"),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                hash_records = [
                    _hash_record(date(2024, 1, 2), 10, "a"),
                    _hash_record(date(2024, 1, 3), 20, "b"),
                ]
                mutate(hash_records[0])
                footer_path, hash_path = _write_manifests(
                    Path(directory),
                    hash_records=hash_records,
                )
                with self.assertRaisesRegex(ManifestValidationError, "identity mismatch"):
                    load_source_manifest_bundle(footer_path, hash_path)

        with tempfile.TemporaryDirectory() as directory:
            footer_path, hash_path = _write_manifests(
                Path(directory),
                hash_records=[_hash_record(date(2024, 1, 2), 10, "a")],
            )
            with self.assertRaisesRegex(ManifestValidationError, "cardinality mismatch"):
                load_source_manifest_bundle(footer_path, hash_path)

    def test_rejects_duplicate_order_drift_noncanonical_and_unsafe_uri(self) -> None:
        day = date(2024, 1, 2)
        for label, footer_records, hash_records, expected in (
            (
                "duplicate",
                [_footer_record(day, 10, 100), _footer_record(day, 10, 100)],
                [_hash_record(day, 10, "a"), _hash_record(day, 10, "a")],
                "duplicate",
            ),
            (
                "unsafe",
                [
                    {
                        **_footer_record(day, 10, 100),
                        "path": "../2024/01/02/glbx-mdp3-20240102.mbp-10.parquet",
                    }
                ],
                [
                    {
                        **_hash_record(day, 10, "a"),
                        "relative_uri": "../2024/01/02/glbx-mdp3-20240102.mbp-10.parquet",
                    }
                ],
                "unsafe relative URI",
            ),
        ):
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                footer_path, hash_path = _write_manifests(
                    Path(directory),
                    footer_records=footer_records,
                    hash_records=hash_records,
                )
                with self.assertRaisesRegex(ManifestValidationError, expected):
                    load_source_manifest_bundle(footer_path, hash_path)

        with tempfile.TemporaryDirectory() as directory:
            footer_path, hash_path = _write_manifests(Path(directory))
            hash_path.write_text(hash_path.read_text().replace(":", ": ", 1))
            with self.assertRaisesRegex(ManifestValidationError, "not canonical"):
                load_source_manifest_bundle(footer_path, hash_path)


class DataRegistryTest(unittest.TestCase):
    def _connection_mocks(self):  # type: ignore[no-untyped-def]
        connection_context = mock.MagicMock()
        connection = connection_context.__enter__.return_value
        cursor_context = mock.MagicMock()
        cursor = cursor_context.__enter__.return_value
        connection.cursor.return_value = cursor_context
        return connection_context, connection, cursor

    def test_registers_dataset_and_sources_as_hash_validating_in_one_connection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            footer_path, hash_path = _write_manifests(Path(directory))
            connection_context, _, cursor = self._connection_mocks()
            cursor.fetchone.side_effect = [None, (42,), (2, 2)]
            cursor.fetchall.return_value = []

            with mock.patch.object(
                data_registry.psycopg,
                "connect",
                return_value=connection_context,
            ) as connect:
                report = register_source_manifests(
                    "postgresql:///systematic_fx",
                    footer_manifest_path=footer_path,
                    hash_manifest_path=hash_path,
                    dataset=_dataset(),
                )

            connect.assert_called_once_with("postgresql:///systematic_fx")
            self.assertEqual(report.dataset_id, 42)
            self.assertEqual(report.dataset_status, "VALIDATING")
            self.assertEqual(report.inserted_source_file_count, 2)
            self.assertEqual(report.preexisting_source_file_count, 0)
            source_sql = cursor.executemany.call_args.args[0]
            source_rows = cursor.executemany.call_args.args[1]
            self.assertIn("'HASHED'", source_sql)
            self.assertEqual(len(source_rows), 2)
            self.assertEqual(source_rows[0][4], "a" * 64)
            self.assertNotIn("instrument_mappings", source_rows[0][-1].obj)
            dataset_sql = cursor.execute.call_args_list[2].args[0]
            self.assertIn("'VALIDATING'", dataset_sql)

    def test_identical_existing_rows_are_idempotently_upserted_without_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            footer_path, hash_path = _write_manifests(Path(directory))
            bundle = load_source_manifest_bundle(footer_path, hash_path)
            existing_dataset = (
                7,
                "Databento",
                "GLBX.MDP3",
                "mbp-10",
                "/data/mbp-10",
                -9,
                "VALIDATING",
                bundle.first_source_date,
                bundle.last_source_date,
                bundle.hash_manifest_sha256,
            )
            existing_sources = [
                (
                    record.relative_uri,
                    record.source_date,
                    record.byte_size,
                    record.sha256,
                    record.row_count,
                    record.schema_fingerprint,
                    "HASHED",
                )
                for record in bundle.records
            ]
            connection_context, _, cursor = self._connection_mocks()
            cursor.fetchone.side_effect = [existing_dataset, (7,), (2, 2)]
            cursor.fetchall.return_value = existing_sources

            with mock.patch.object(
                data_registry.psycopg,
                "connect",
                return_value=connection_context,
            ):
                report = register_source_manifests(
                    "postgresql:///systematic_fx",
                    footer_manifest_path=footer_path,
                    hash_manifest_path=hash_path,
                    dataset=_dataset(),
                )

            self.assertEqual(report.dataset_id, 7)
            self.assertEqual(report.preexisting_source_file_count, 2)
            self.assertEqual(report.inserted_source_file_count, 0)

    def test_identical_ready_validated_registration_is_a_no_demotion_verification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            footer_path, hash_path = _write_manifests(Path(directory))
            bundle = load_source_manifest_bundle(footer_path, hash_path)
            ready_dataset = (
                7,
                "Databento",
                "GLBX.MDP3",
                "mbp-10",
                "/data/mbp-10",
                -9,
                "READY",
                bundle.first_source_date,
                bundle.last_source_date,
                bundle.hash_manifest_sha256,
            )
            validated_sources = [
                (
                    record.relative_uri,
                    record.source_date,
                    record.byte_size,
                    record.sha256,
                    record.row_count,
                    record.schema_fingerprint,
                    "VALIDATED",
                )
                for record in bundle.records
            ]
            connection_context, _, cursor = self._connection_mocks()
            cursor.fetchone.side_effect = [ready_dataset, (2, 2)]
            cursor.fetchall.return_value = validated_sources

            with mock.patch.object(
                data_registry.psycopg,
                "connect",
                return_value=connection_context,
            ):
                report = register_source_manifests(
                    "postgresql:///systematic_fx",
                    footer_manifest_path=footer_path,
                    hash_manifest_path=hash_path,
                    dataset=_dataset(),
                )

            self.assertEqual(report.dataset_status, "READY")
            self.assertEqual(report.inserted_source_file_count, 0)
            executed_sql = [call.args[0] for call in cursor.execute.call_args_list]
            self.assertFalse(
                any("INSERT INTO systematic_fx.datasets" in sql for sql in executed_sql)
            )
            source_sql = cursor.executemany.call_args.args[0]
            self.assertIn("WHERE existing.status <> 'VALIDATED'", source_sql)
            verification_sql = executed_sql[-1]
            self.assertIn("'HASHED', 'VALIDATED'", verification_sql)

    def test_terminal_dataset_status_is_rejected_before_any_upsert(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            footer_path, hash_path = _write_manifests(Path(directory))
            bundle = load_source_manifest_bundle(footer_path, hash_path)
            rejected_dataset = (
                7,
                "Databento",
                "GLBX.MDP3",
                "mbp-10",
                "/data/mbp-10",
                -9,
                "REJECTED",
                bundle.first_source_date,
                bundle.last_source_date,
                bundle.hash_manifest_sha256,
            )
            connection_context, _, cursor = self._connection_mocks()
            cursor.fetchone.return_value = rejected_dataset

            with (
                mock.patch.object(
                    data_registry.psycopg,
                    "connect",
                    return_value=connection_context,
                ),
                self.assertRaisesRegex(RegistryDriftError, "status 'REJECTED'"),
            ):
                register_source_manifests(
                    "postgresql:///systematic_fx",
                    footer_manifest_path=footer_path,
                    hash_manifest_path=hash_path,
                    dataset=_dataset(),
                )

            cursor.executemany.assert_not_called()

    def test_database_drift_raises_inside_transaction_before_source_upsert(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            footer_path, hash_path = _write_manifests(Path(directory))
            bundle = load_source_manifest_bundle(footer_path, hash_path)
            existing_dataset = (
                7,
                "Databento",
                "GLBX.MDP3",
                "mbp-10",
                "/data/mbp-10",
                -9,
                "VALIDATING",
                bundle.first_source_date,
                bundle.last_source_date,
                bundle.hash_manifest_sha256,
            )
            record = bundle.records[0]
            conflicting_source = (
                record.relative_uri,
                record.source_date,
                record.byte_size,
                "f" * 64,
                record.row_count,
                record.schema_fingerprint,
                "HASHED",
            )
            connection_context, _, cursor = self._connection_mocks()
            cursor.fetchone.side_effect = [existing_dataset, (7,)]
            cursor.fetchall.return_value = [conflicting_source]

            with (
                mock.patch.object(
                    data_registry.psycopg,
                    "connect",
                    return_value=connection_context,
                ),
                self.assertRaisesRegex(RegistryDriftError, "SHA-256 drift"),
            ):
                register_source_manifests(
                    "postgresql:///systematic_fx",
                    footer_manifest_path=footer_path,
                    hash_manifest_path=hash_path,
                    dataset=_dataset(),
                )

            cursor.executemany.assert_not_called()
            exception_type = connection_context.__exit__.call_args.args[0]
            self.assertIs(exception_type, RegistryDriftError)

    def test_manifest_or_contract_failure_never_opens_database_connection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            footer_path, hash_path = _write_manifests(
                Path(directory),
                hash_records=[_hash_record(date(2024, 1, 2), 999, "a")],
            )
            with (
                mock.patch.object(data_registry.psycopg, "connect") as connect,
                self.assertRaises(ManifestValidationError),
            ):
                register_source_manifests(
                    "postgresql:///systematic_fx",
                    footer_manifest_path=footer_path,
                    hash_manifest_path=hash_path,
                    dataset=_dataset(),
                )
            connect.assert_not_called()

        with tempfile.TemporaryDirectory() as directory:
            footer_path, hash_path = _write_manifests(Path(directory))
            wrong_contract = DatasetRegistration(
                dataset_key="wrong",
                root_uri="/data/mbp-10",
                feed="OTHER.FEED",
            )
            with (
                mock.patch.object(data_registry.psycopg, "connect") as connect,
                self.assertRaisesRegex(ManifestValidationError, "does not match"),
            ):
                register_source_manifests(
                    "postgresql:///systematic_fx",
                    footer_manifest_path=footer_path,
                    hash_manifest_path=hash_path,
                    dataset=wrong_contract,
                )
            connect.assert_not_called()

    def test_post_upsert_count_mismatch_aborts_registration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            footer_path, hash_path = _write_manifests(Path(directory))
            connection_context, _, cursor = self._connection_mocks()
            cursor.fetchone.side_effect = [None, (42,), (1, 1)]
            cursor.fetchall.return_value = []

            with (
                mock.patch.object(
                    data_registry.psycopg,
                    "connect",
                    return_value=connection_context,
                ),
                self.assertRaisesRegex(DataRegistryDatabaseError, "verification"),
            ):
                register_source_manifests(
                    "postgresql:///systematic_fx",
                    footer_manifest_path=footer_path,
                    hash_manifest_path=hash_path,
                    dataset=_dataset(),
                )

            exception_type = connection_context.__exit__.call_args.args[0]
            self.assertIs(exception_type, DataRegistryDatabaseError)


if __name__ == "__main__":
    unittest.main()
