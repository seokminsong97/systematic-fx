import hashlib
import json
import os
import shutil
import tempfile
import unittest
import uuid
from datetime import date
from pathlib import Path

import psycopg

from systematic_fx.db.data_registry import DatasetRegistration, register_source_manifests
from systematic_fx.db.migrations import apply_migrations


def _canonical(record: dict[str, object]) -> str:
    return json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"


def _records(day: date, byte_size: int, row_count: int, digest: str):  # type: ignore[no-untyped-def]
    relative_uri = (
        f"{day.year:04d}/{day.month:02d}/{day.day:02d}/glbx-mdp3-{day:%Y%m%d}.mbp-10.parquet"
    )
    footer = {
        "contract": {
            "dataset": "GLBX.MDP3",
            "price_scale": "1e-9",
            "schema": "mbp-10",
        },
        "file_size_bytes": byte_size,
        "instrument_mappings": [{"instrument_id": 123, "raw_symbol": "6EH4"}],
        "mapping_interval_count": 1,
        "path": relative_uri,
        "row_count": row_count,
        "schema_fingerprint": "1" * 64,
        "source_date": day.isoformat(),
    }
    hashed = {
        "byte_size": byte_size,
        "relative_uri": relative_uri,
        "sha256": digest,
        "source_date": day.isoformat(),
    }
    return footer, hashed


class DataRegistryPostgreSQLIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.database_url = os.environ.get("SYSTEMATIC_FX_TEST_DATABASE_URL")
        if not cls.database_url:
            raise unittest.SkipTest("SYSTEMATIC_FX_TEST_DATABASE_URL is not set")
        cls.psql = shutil.which(os.environ.get("SYSTEMATIC_FX_PSQL", "psql"))
        if cls.psql is None:
            raise unittest.SkipTest("psql is not installed or is not on PATH")
        apply_migrations(cls.database_url, psql_binary=cls.psql)

    def test_atomic_idempotent_dataset_and_hashed_source_registration(self) -> None:
        dataset_key = f"registry_integration_{uuid.uuid4().hex}"
        with tempfile.TemporaryDirectory() as directory:
            manifest_root = Path(directory)
            footer_path = manifest_root / "footer.jsonl"
            hash_path = manifest_root / "hash.jsonl"
            pairs = (
                _records(date(2024, 1, 2), 10, 100, "a" * 64),
                _records(date(2024, 1, 3), 20, 200, "b" * 64),
            )
            footer_path.write_text("".join(_canonical(pair[0]) for pair in pairs))
            hash_path.write_text("".join(_canonical(pair[1]) for pair in pairs))
            dataset = DatasetRegistration(
                dataset_key=dataset_key,
                root_uri="/integration/mbp-10",
            )

            try:
                first = register_source_manifests(
                    self.database_url,
                    footer_manifest_path=footer_path,
                    hash_manifest_path=hash_path,
                    dataset=dataset,
                )
                second = register_source_manifests(
                    self.database_url,
                    footer_manifest_path=footer_path,
                    hash_manifest_path=hash_path,
                    dataset=dataset,
                )

                self.assertEqual(first.dataset_id, second.dataset_id)
                self.assertEqual(first.dataset_status, "VALIDATING")
                self.assertEqual(first.inserted_source_file_count, 2)
                self.assertEqual(second.inserted_source_file_count, 0)
                self.assertEqual(second.preexisting_source_file_count, 2)
                self.assertEqual(
                    first.hash_manifest_sha256,
                    hashlib.sha256(hash_path.read_bytes()).hexdigest(),
                )

                with psycopg.connect(self.database_url) as connection:
                    connection.execute(
                        "UPDATE systematic_fx.source_files "
                        "SET status = 'VALIDATED', validated_at = statement_timestamp() "
                        "WHERE dataset_id = %s",
                        (first.dataset_id,),
                    )
                    connection.execute(
                        "UPDATE systematic_fx.datasets SET status = 'READY' WHERE dataset_id = %s",
                        (first.dataset_id,),
                    )

                third = register_source_manifests(
                    self.database_url,
                    footer_manifest_path=footer_path,
                    hash_manifest_path=hash_path,
                    dataset=dataset,
                )
                self.assertEqual(third.dataset_status, "READY")
                self.assertEqual(third.inserted_source_file_count, 0)

                with (
                    psycopg.connect(self.database_url) as connection,
                    connection.cursor() as cursor,
                ):
                    cursor.execute(
                        "SELECT status, manifest_sha256, expected_start_date, "
                        "expected_end_date, metadata "
                        "FROM systematic_fx.datasets WHERE dataset_key = %s",
                        (dataset_key,),
                    )
                    self.assertEqual(
                        cursor.fetchone(),
                        (
                            "READY",
                            first.hash_manifest_sha256,
                            date(2024, 1, 2),
                            date(2024, 1, 3),
                            {},
                        ),
                    )
                    cursor.execute(
                        "SELECT count(*)::integer, "
                        "count(*) FILTER (WHERE status = 'VALIDATED')::integer, "
                        "count(*) FILTER (WHERE sha256 IS NOT NULL)::integer, "
                        "bool_and(NOT footer_metadata ? 'instrument_mappings') "
                        "FROM systematic_fx.source_files WHERE dataset_id = %s",
                        (first.dataset_id,),
                    )
                    self.assertEqual(cursor.fetchone(), (2, 2, 2, True))
                    cursor.execute(
                        "SELECT relative_uri, byte_size, row_count, sha256 "
                        "FROM systematic_fx.source_files WHERE dataset_id = %s "
                        "ORDER BY relative_uri",
                        (first.dataset_id,),
                    )
                    self.assertEqual(
                        cursor.fetchall(),
                        [
                            (
                                "2024/01/02/glbx-mdp3-20240102.mbp-10.parquet",
                                10,
                                100,
                                "a" * 64,
                            ),
                            (
                                "2024/01/03/glbx-mdp3-20240103.mbp-10.parquet",
                                20,
                                200,
                                "b" * 64,
                            ),
                        ],
                    )
            finally:
                with (
                    psycopg.connect(self.database_url) as connection,
                    connection.cursor() as cursor,
                ):
                    cursor.execute(
                        "DELETE FROM systematic_fx.source_files "
                        "WHERE dataset_id IN (SELECT dataset_id FROM systematic_fx.datasets "
                        "WHERE dataset_key = %s)",
                        (dataset_key,),
                    )
                    cursor.execute(
                        "DELETE FROM systematic_fx.datasets WHERE dataset_key = %s",
                        (dataset_key,),
                    )


if __name__ == "__main__":
    unittest.main()
