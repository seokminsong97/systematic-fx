import hashlib
import tempfile
import unittest
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

from systematic_fx.db import derived_registry
from systematic_fx.db.derived_registry import (
    PilotArtifactValidationError,
    prepare_pilot_derived_registration,
    register_pilot_derived_partitions,
)
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
    *,
    start: datetime,
) -> ArtifactReport:
    rows = [
        {field.name: _default_value(field, start + timedelta(seconds=offset)) for field in schema}
        for offset in (0, 1)
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    content = path.read_bytes()
    return ArtifactReport(
        path=str(path),
        sha256=hashlib.sha256(content).hexdigest(),
        rows=2,
        schema_sha256=schema_sha256,
        min_bucket_end=start.isoformat(),
        max_bucket_end=(start + timedelta(seconds=1)).isoformat(),
    )


def _fixture(root: Path) -> tuple[Path, ArtifactReport, ArtifactReport]:
    data_root = root / "data"
    one_second_path = (
        data_root
        / "derived/features_1s/version=mbp10_pilot_v1/contract=6EH2"
        / "source_date=2022-01-03/part-000.parquet"
    )
    five_minute_path = (
        data_root
        / "derived/research_5m/version=mbp10_pilot_v1/contract=6EH2"
        / "source_date=2022-01-03/part-000.parquet"
    )
    start = datetime(2022, 1, 3, 0, 0, 1, tzinfo=UTC)
    one_second = _write_artifact(
        one_second_path,
        ONE_SECOND_SCHEMA,
        ONE_SECOND_SCHEMA_SHA256,
        start=start,
    )
    five_minute = _write_artifact(
        five_minute_path,
        FIVE_MINUTE_SCHEMA,
        FIVE_MINUTE_SCHEMA_SHA256,
        start=start,
    )
    return data_root, one_second, five_minute


def _prepare(data_root: Path, one_second: ArtifactReport, five_minute: ArtifactReport):  # type: ignore[no-untyped-def]
    return prepare_pilot_derived_registration(
        data_root=data_root,
        dataset_key="glbx_mbp10_test_v1",
        source_relative_uri="2022/01/03/glbx-mdp3-20220103.mbp-10.parquet",
        source_sha256="a" * 64,
        feature_version=FEATURE_VERSION,
        provider_instrument_id=INSTRUMENT_ID,
        raw_symbol=RAW_SYMBOL,
        source_date=SOURCE_DATE,
        formula_sha256=FORMULA_SHA256,
        config_sha256="b" * 64,
        code_commit="test-worktree",
        source_manifest_sha256="c" * 64,
        one_second_artifact=one_second,
        five_minute_artifact=five_minute,
    )


class PilotDerivedPreparationTest(unittest.TestCase):
    def test_verifies_artifacts_and_builds_deterministic_nonresearch_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root, one_second, five_minute = _fixture(Path(directory))

            first = _prepare(data_root, one_second, five_minute)
            second = _prepare(data_root, one_second, five_minute)

            self.assertEqual(first.manifest_bytes, second.manifest_bytes)
            self.assertEqual(first.manifest_sha256, second.manifest_sha256)
            self.assertEqual(
                first.manifest_sha256,
                hashlib.sha256(first.manifest_bytes).hexdigest(),
            )
            self.assertTrue(first.manifest_path.is_relative_to(data_root.resolve() / "derived"))
            self.assertFalse(first.manifest_document["definition"]["research_eligible"])
            self.assertEqual(
                [artifact.partition_type for artifact in first.artifacts],
                ["FEATURES_1S", "RESEARCH_5M"],
            )
            for artifact in first.artifacts:
                self.assertIn(
                    f"sha256/{artifact.sha256[:2]}/{artifact.sha256}",
                    artifact.canonical_relative_uri,
                )
                self.assertEqual(artifact.row_count, 2)

            derived_registry._publish_prepared(first)
            derived_registry._publish_prepared(second)
            self.assertEqual(first.manifest_path.read_bytes(), first.manifest_bytes)
            for artifact in first.artifacts:
                self.assertEqual(
                    hashlib.sha256(artifact.canonical_path.read_bytes()).hexdigest(),
                    artifact.sha256,
                )

    def test_rejects_report_content_path_schema_and_identity_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root, one_second, five_minute = _fixture(root)

            wrong_hash = ArtifactReport(**{**one_second.as_dict(), "sha256": "d" * 64})
            with self.assertRaisesRegex(PilotArtifactValidationError, "SHA-256 differs"):
                _prepare(data_root, wrong_hash, five_minute)

            outside = root / "outside.parquet"
            outside.write_bytes(Path(one_second.path).read_bytes())
            outside_report = ArtifactReport(**{**one_second.as_dict(), "path": str(outside)})
            with self.assertRaisesRegex(PilotArtifactValidationError, "must remain below"):
                _prepare(data_root, outside_report, five_minute)

            wrong_schema_path = Path(one_second.path)
            pq.write_table(pa.table({"bucket_end": [datetime.now(UTC)]}), wrong_schema_path)
            wrong_schema_report = ArtifactReport(
                **{
                    **one_second.as_dict(),
                    "sha256": hashlib.sha256(wrong_schema_path.read_bytes()).hexdigest(),
                }
            )
            with self.assertRaisesRegex(PilotArtifactValidationError, "schema does not match"):
                _prepare(data_root, wrong_schema_report, five_minute)

    def test_file_failure_occurs_before_database_connection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root, one_second, five_minute = _fixture(Path(directory))
            missing = ArtifactReport(
                **{**one_second.as_dict(), "path": str(data_root / "derived/missing.parquet")}
            )
            with (
                mock.patch.object(derived_registry.psycopg, "connect") as connect,
                self.assertRaisesRegex(PilotArtifactValidationError, "does not exist"),
            ):
                register_pilot_derived_partitions(
                    "postgresql:///systematic_fx",
                    data_root=data_root,
                    dataset_key="glbx_mbp10_test_v1",
                    source_relative_uri="2022/01/03/source.parquet",
                    source_sha256="a" * 64,
                    feature_version=FEATURE_VERSION,
                    provider_instrument_id=INSTRUMENT_ID,
                    raw_symbol=RAW_SYMBOL,
                    source_date=SOURCE_DATE,
                    formula_sha256=FORMULA_SHA256,
                    config_sha256="b" * 64,
                    code_commit="test-worktree",
                    source_manifest_sha256="c" * 64,
                    one_second_artifact=missing,
                    five_minute_artifact=five_minute,
                )
            connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
