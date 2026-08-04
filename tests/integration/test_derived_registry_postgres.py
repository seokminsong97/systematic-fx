import hashlib
import os
import shutil
import tempfile
import unittest
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

from systematic_fx.db.derived_registry import (
    DerivedRegistryDriftError,
    register_pilot_derived_partitions,
)
from systematic_fx.db.migrations import apply_migrations
from systematic_fx.features.pilot import (
    FEATURE_VERSION,
    FIVE_MINUTE_SCHEMA,
    FIVE_MINUTE_SCHEMA_SHA256,
    FORMULA_SHA256,
    ONE_SECOND_SCHEMA,
    ONE_SECOND_SCHEMA_SHA256,
    ArtifactReport,
)

SOURCE_DATE = date(2022, 1, 3)
INSTRUMENT_ID = 28727
RAW_SYMBOL = "6EH2"
SOURCE_SHA256 = "a" * 64
SOURCE_MANIFEST_SHA256 = "b" * 64
CONFIG_SHA256 = "c" * 64


def _default_value(field: pa.Field, bucket_end: datetime) -> object:
    if field.name == "feature_version":
        return FEATURE_VERSION
    if field.name == "research_eligible":
        return False
    if field.name == "source_date":
        return SOURCE_DATE
    if field.name == "contract":
        return RAW_SYMBOL
    if field.name == "instrument_id":
        return INSTRUMENT_ID
    if pa.types.is_timestamp(field.type):
        return bucket_end
    if pa.types.is_string(field.type):
        return "N"
    if pa.types.is_boolean(field.type):
        return False
    if pa.types.is_floating(field.type):
        return 0.0
    if pa.types.is_integer(field.type):
        return 0
    raise AssertionError(f"unhandled test field: {field}")


def _write_artifact(
    path: Path,
    schema: pa.Schema,
    schema_sha256: str,
    start: datetime,
) -> ArtifactReport:
    rows = [
        {field.name: _default_value(field, start + timedelta(seconds=offset)) for field in schema}
        for offset in (0, 1)
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return ArtifactReport(
        path=str(path),
        sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        rows=len(rows),
        schema_sha256=schema_sha256,
        min_bucket_end=start.isoformat(),
        max_bucket_end=(start + timedelta(seconds=1)).isoformat(),
    )


def _artifacts(root: Path) -> tuple[Path, ArtifactReport, ArtifactReport]:
    data_root = root / "data"
    prefix = "version=mbp10_pilot_v1/contract=6EH2/source_date=2022-01-03"
    start = datetime(2022, 1, 3, 0, 0, 1, tzinfo=UTC)
    one_second = _write_artifact(
        data_root / "derived/features_1s" / prefix / "part-000.parquet",
        ONE_SECOND_SCHEMA,
        ONE_SECOND_SCHEMA_SHA256,
        start,
    )
    five_minute = _write_artifact(
        data_root / "derived/research_5m" / prefix / "part-000.parquet",
        FIVE_MINUTE_SCHEMA,
        FIVE_MINUTE_SCHEMA_SHA256,
        start,
    )
    return data_root, one_second, five_minute


class DerivedRegistryPostgreSQLIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.database_url = os.environ.get("SYSTEMATIC_FX_TEST_DATABASE_URL")
        if not cls.database_url:
            raise unittest.SkipTest("SYSTEMATIC_FX_TEST_DATABASE_URL is not set")
        cls.psql = shutil.which(os.environ.get("SYSTEMATIC_FX_PSQL", "psql"))
        if cls.psql is None:
            raise unittest.SkipTest("psql is not installed or is not on PATH")
        apply_migrations(cls.database_url, psql_binary=cls.psql)

    def test_atomic_idempotent_validated_lineage_and_drift_rejection(self) -> None:
        dataset_key = f"derived_registry_integration_{uuid.uuid4().hex}"
        source_relative_uri = "2022/01/03/glbx-mdp3-20220103.mbp-10.parquet"
        with tempfile.TemporaryDirectory() as directory:
            data_root, one_second, five_minute = _artifacts(Path(directory))
            with (
                psycopg.connect(self.database_url) as connection,
                connection.cursor() as cursor,
            ):
                cursor.execute(
                    """
                    INSERT INTO systematic_fx.datasets
                        (dataset_key, provider, feed, data_schema, root_uri, status,
                         expected_start_date, expected_end_date, manifest_sha256)
                    VALUES (%s, 'Databento', 'GLBX.MDP3', 'mbp-10', %s, 'VALIDATING',
                            %s, %s, %s)
                    RETURNING dataset_id
                    """,
                    (
                        dataset_key,
                        (data_root / "raw").resolve().as_uri(),
                        SOURCE_DATE,
                        SOURCE_DATE,
                        SOURCE_MANIFEST_SHA256,
                    ),
                )
                dataset_id = cursor.fetchone()[0]
                cursor.execute(
                    """
                    INSERT INTO systematic_fx.source_files
                        (dataset_id, source_date, relative_uri, byte_size, sha256,
                         row_count, status)
                    VALUES (%s, %s, %s, 100, %s, 10, 'HASHED')
                    RETURNING source_file_id
                    """,
                    (dataset_id, SOURCE_DATE, source_relative_uri, SOURCE_SHA256),
                )
                source_file_id = cursor.fetchone()[0]

            arguments = {
                "data_root": data_root,
                "dataset_key": dataset_key,
                "source_relative_uri": source_relative_uri,
                "source_sha256": SOURCE_SHA256,
                "feature_version": FEATURE_VERSION,
                "provider_instrument_id": INSTRUMENT_ID,
                "raw_symbol": RAW_SYMBOL,
                "source_date": SOURCE_DATE,
                "formula_sha256": FORMULA_SHA256,
                "config_sha256": CONFIG_SHA256,
                "code_commit": "integration-test-worktree",
                "source_manifest_sha256": SOURCE_MANIFEST_SHA256,
                "one_second_artifact": one_second,
                "five_minute_artifact": five_minute,
            }
            try:
                first = register_pilot_derived_partitions(
                    self.database_url,
                    **arguments,
                )
                with (
                    psycopg.connect(self.database_url) as connection,
                    connection.cursor() as cursor,
                ):
                    cursor.execute(
                        "UPDATE systematic_fx.datasets SET status = 'READY' WHERE dataset_id = %s",
                        (dataset_id,),
                    )
                    cursor.execute(
                        "UPDATE systematic_fx.source_files "
                        "SET status = 'VALIDATED', validated_at = statement_timestamp() "
                        "WHERE source_file_id = %s",
                        (source_file_id,),
                    )

                second = register_pilot_derived_partitions(
                    self.database_url,
                    **arguments,
                )
                self.assertEqual(first.dataset_id, dataset_id)
                self.assertEqual(first.source_file_id, source_file_id)
                self.assertEqual(first.build_job_id, second.build_job_id)
                self.assertEqual(first.manifest_artifact_id, second.manifest_artifact_id)
                self.assertEqual(first.features_1s_partition_id, second.features_1s_partition_id)
                self.assertEqual(first.research_5m_partition_id, second.research_5m_partition_id)
                self.assertTrue(first.created_job)
                self.assertTrue(first.created_manifest_artifact)
                self.assertEqual(first.created_partitions, 2)
                self.assertFalse(second.created_job)
                self.assertFalse(second.created_manifest_artifact)
                self.assertEqual(second.created_partitions, 0)

                with (
                    psycopg.connect(self.database_url) as connection,
                    connection.cursor() as cursor,
                ):
                    cursor.execute(
                        "SELECT status FROM systematic_fx.datasets WHERE dataset_id = %s",
                        (dataset_id,),
                    )
                    self.assertEqual(cursor.fetchone(), ("READY",))
                    cursor.execute(
                        "SELECT status FROM systematic_fx.source_files WHERE source_file_id = %s",
                        (source_file_id,),
                    )
                    self.assertEqual(cursor.fetchone(), ("VALIDATED",))
                    cursor.execute(
                        """
                        SELECT count(*)::integer,
                               count(*) FILTER (WHERE status = 'VALIDATED')::integer,
                               count(*) FILTER (WHERE instrument_id IS NULL)::integer,
                               count(*) FILTER (WHERE validated_at IS NOT NULL
                                                AND manifest_artifact_id IS NOT NULL
                                                AND build_job_id IS NOT NULL)::integer,
                               bool_and((metadata ->> 'research_eligible')::boolean = false)
                        FROM systematic_fx.derived_partitions
                        WHERE dataset_id = %s
                        """,
                        (dataset_id,),
                    )
                    self.assertEqual(cursor.fetchone(), (2, 2, 2, 2, True))
                    cursor.execute(
                        """
                        SELECT count(*)::integer, bool_and(dps.source_sha256 = %s)
                        FROM systematic_fx.derived_partition_sources dps
                        JOIN systematic_fx.derived_partitions dp
                          ON dp.derived_partition_id = dps.derived_partition_id
                        WHERE dp.dataset_id = %s
                        """,
                        (SOURCE_SHA256, dataset_id),
                    )
                    self.assertEqual(cursor.fetchone(), (2, True))
                    cursor.execute(
                        "SELECT count(*)::integer FROM systematic_fx.jobs "
                        "WHERE dataset_id = %s AND job_type = 'BUILD_PILOT_DERIVED'",
                        (dataset_id,),
                    )
                    self.assertEqual(cursor.fetchone(), (1,))
                    cursor.execute(
                        "SELECT count(*)::integer FROM systematic_fx.artifacts "
                        "WHERE producer_job_id = %s",
                        (first.build_job_id,),
                    )
                    self.assertEqual(cursor.fetchone(), (1,))
                    cursor.execute(
                        """
                        UPDATE systematic_fx.derived_partitions
                        SET metadata = jsonb_set(metadata, '{research_eligible}', 'true')
                        WHERE derived_partition_id = %s
                        """,
                        (first.features_1s_partition_id,),
                    )

                with self.assertRaisesRegex(
                    DerivedRegistryDriftError,
                    "immutable drift.*metadata",
                ):
                    register_pilot_derived_partitions(
                        self.database_url,
                        **arguments,
                    )
            finally:
                with (
                    psycopg.connect(self.database_url) as connection,
                    connection.cursor() as cursor,
                ):
                    cursor.execute(
                        """
                        DELETE FROM systematic_fx.derived_partition_sources
                        WHERE derived_partition_id IN
                              (SELECT derived_partition_id
                               FROM systematic_fx.derived_partitions
                               WHERE dataset_id = %s)
                        """,
                        (dataset_id,),
                    )
                    cursor.execute(
                        "DELETE FROM systematic_fx.derived_partitions WHERE dataset_id = %s",
                        (dataset_id,),
                    )
                    cursor.execute(
                        "DELETE FROM systematic_fx.artifacts "
                        "WHERE producer_job_id IN "
                        "(SELECT job_id FROM systematic_fx.jobs WHERE dataset_id = %s)",
                        (dataset_id,),
                    )
                    cursor.execute(
                        "DELETE FROM systematic_fx.jobs WHERE dataset_id = %s",
                        (dataset_id,),
                    )
                    cursor.execute(
                        "DELETE FROM systematic_fx.source_files WHERE dataset_id = %s",
                        (dataset_id,),
                    )
                    cursor.execute(
                        "DELETE FROM systematic_fx.datasets WHERE dataset_id = %s",
                        (dataset_id,),
                    )


if __name__ == "__main__":
    unittest.main()
