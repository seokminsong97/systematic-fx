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

from systematic_fx.data.quality import DIAGNOSTIC_CHECKS, HARD_CHECKS
from systematic_fx.db.full_qc_registry import (
    DIAGNOSTICS_CHECK_NAME,
    SCAN_ARTIFACT_SCHEMA,
    SCANNER_CHECKER_VERSION,
    SOURCE_CHECK_NAME,
    FullQcRegistryDriftError,
    register_full_qc_scan,
)
from systematic_fx.db.migrations import apply_migrations


def _canonical(record: dict[str, object]) -> str:
    return json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"


def _write_manifests(root: Path) -> tuple[Path, Path, list[dict[str, object]]]:
    manifests = root / "derived" / "manifests"
    manifests.mkdir(parents=True)
    (root / "mbp-10").mkdir()
    sources: list[dict[str, object]] = []
    for day, size, digest in (
        (date(2024, 1, 2), 10, "a" * 64),
        (date(2024, 1, 3), 20, "b" * 64),
    ):
        sources.append(
            {
                "byte_size": size,
                "relative_uri": (
                    f"{day.year:04d}/{day.month:02d}/{day.day:02d}/"
                    f"glbx-mdp3-{day:%Y%m%d}.mbp-10.parquet"
                ),
                "sha256": digest,
                "source_date": day.isoformat(),
            }
        )
    source_path = manifests / "mbp10_source_sha256_v1.jsonl"
    source_path.write_text("".join(_canonical(source) for source in sources))
    source_manifest_sha256 = hashlib.sha256(source_path.read_bytes()).hexdigest()
    scans: list[dict[str, object]] = []
    for index, source in enumerate(sources):
        diagnostics = dict.fromkeys(DIAGNOSTIC_CHECKS, 0)
        if index == 0:
            diagnostics["maybe_bad_book_flag_rows"] = 2
        scans.append(
            {
                "artifact_schema": SCAN_ARTIFACT_SCHEMA,
                "checker_version": SCANNER_CHECKER_VERSION,
                "config_sha256": "c" * 64,
                "coverage_complete": True,
                "diagnostic_counts": diagnostics,
                "expected_row_count": 100 * (index + 1),
                "expected_row_group_count": index + 2,
                "first_ts_recv_ns": 1_700_000_000_000_000_000 + index,
                "hard_violation_count": 0,
                "hard_violation_counts": dict.fromkeys(HARD_CHECKS, 0),
                "last_ts_recv_ns": 1_700_000_001_000_000_000 + index,
                "relative_uri": source["relative_uri"],
                "research_eligible": False,
                "result": "PASS",
                "scanned_row_count": 100 * (index + 1),
                "scanned_row_group_count": index + 2,
                "schema_fingerprint": "1" * 64,
                "source_byte_size": source["byte_size"],
                "source_date": source["source_date"],
                "source_manifest_sha256": source_manifest_sha256,
                "source_sha256": source["sha256"],
            }
        )
    scan_path = manifests / "mbp10_structural_qc_v1.jsonl"
    scan_path.write_text("".join(_canonical(scan) for scan in scans))
    return source_path, scan_path, sources


class FullQcRegistryPostgreSQLIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.database_url = os.environ.get("SYSTEMATIC_FX_TEST_DATABASE_URL")
        if not cls.database_url:
            raise unittest.SkipTest("SYSTEMATIC_FX_TEST_DATABASE_URL is not set")
        cls.psql = shutil.which(os.environ.get("SYSTEMATIC_FX_PSQL", "psql"))
        if cls.psql is None:
            raise unittest.SkipTest("psql is not installed or is not on PATH")
        apply_migrations(cls.database_url, psql_binary=cls.psql)

    def test_atomic_append_only_idempotent_full_qc_registration(self) -> None:
        dataset_key = f"full_qc_integration_{uuid.uuid4().hex}"
        dataset_id: int | None = None
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            root.mkdir()
            source_path, scan_path, sources = _write_manifests(root)
            source_manifest_sha256 = hashlib.sha256(source_path.read_bytes()).hexdigest()
            try:
                with psycopg.connect(self.database_url) as connection:
                    dataset_id = connection.execute(
                        """
                        INSERT INTO systematic_fx.datasets
                            (dataset_key, provider, feed, data_schema, root_uri,
                             price_scale_exponent, status, expected_start_date,
                             expected_end_date, manifest_sha256)
                        VALUES (%s, 'Databento', 'GLBX.MDP3', 'mbp-10', %s, -9,
                                'VALIDATING', DATE '2024-01-02', DATE '2024-01-03', %s)
                        RETURNING dataset_id
                        """,
                        (
                            dataset_key,
                            (root / "mbp-10").resolve().as_uri(),
                            source_manifest_sha256,
                        ),
                    ).fetchone()[0]
                    source_ids: list[int] = []
                    for index, source in enumerate(sources):
                        source_ids.append(
                            connection.execute(
                                """
                                INSERT INTO systematic_fx.source_files
                                    (dataset_id, source_date, relative_uri, byte_size,
                                     sha256, row_count, parquet_schema_fingerprint, status)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, 'HASHED')
                                RETURNING source_file_id
                                """,
                                (
                                    dataset_id,
                                    date.fromisoformat(str(source["source_date"])),
                                    source["relative_uri"],
                                    source["byte_size"],
                                    source["sha256"],
                                    100 * (index + 1),
                                    "1" * 64,
                                ),
                            ).fetchone()[0]
                        )
                    connection.execute(
                        """
                        INSERT INTO systematic_fx.quality_checks
                            (quality_check_key, dataset_id, check_name,
                             checker_version, result, observed, expected, details)
                        VALUES (%s, %s, 'full_row_group_quality',
                                'source_qualification_v1', 'FAIL', '{}'::jsonb,
                                '{}'::jsonb, 'historical blocker')
                        """,
                        (
                            f"source-qualification:v1:{dataset_key}:full_row_group_quality",
                            dataset_id,
                        ),
                    )

                first = register_full_qc_scan(
                    self.database_url,
                    data_root=root,
                    dataset_key=dataset_key,
                    scan_manifest_path=scan_path,
                    source_manifest_path=source_path,
                )
                second = register_full_qc_scan(
                    self.database_url,
                    data_root=root,
                    dataset_key=dataset_key,
                    scan_manifest_path=scan_path,
                    source_manifest_path=source_path,
                )

                self.assertEqual(first.job_id, second.job_id)
                self.assertEqual(first.created_quality_checks, 4)
                self.assertEqual(second.created_quality_checks, 0)
                self.assertEqual(first.aggregate_result, "PASS")
                self.assertEqual(first.diagnostic_result, "WARN")
                self.assertEqual(first.dataset_status, "VALIDATING")
                self.assertEqual(first.source_status_counts, {"HASHED": 2})

                with psycopg.connect(self.database_url) as connection:
                    job = connection.execute(
                        "SELECT status, result->>'quality_result' FROM systematic_fx.jobs "
                        "WHERE job_id = %s",
                        (first.job_id,),
                    ).fetchone()
                    self.assertEqual(job, ("SUCCEEDED", "PASS"))
                    counts = connection.execute(
                        """
                        SELECT count(*)::integer,
                               count(*) FILTER (WHERE source_file_id IS NOT NULL)::integer,
                               count(*) FILTER (WHERE dataset_id IS NOT NULL)::integer
                        FROM systematic_fx.quality_checks WHERE job_id = %s
                        """,
                        (first.job_id,),
                    ).fetchone()
                    self.assertEqual(counts, (4, 2, 2))
                    diagnostic = connection.execute(
                        "SELECT result FROM systematic_fx.quality_checks "
                        "WHERE job_id = %s AND check_name = %s",
                        (first.job_id, DIAGNOSTICS_CHECK_NAME),
                    ).fetchone()
                    self.assertEqual(diagnostic, ("WARN",))
                    source_results = connection.execute(
                        "SELECT result FROM systematic_fx.quality_checks "
                        "WHERE job_id = %s AND check_name = %s ORDER BY source_file_id",
                        (first.job_id, SOURCE_CHECK_NAME),
                    ).fetchall()
                    self.assertEqual(source_results, [("PASS",), ("PASS",)])
                    historical = connection.execute(
                        "SELECT result FROM systematic_fx.quality_checks "
                        "WHERE quality_check_key = %s",
                        (f"source-qualification:v1:{dataset_key}:full_row_group_quality",),
                    ).fetchone()
                    self.assertEqual(historical, ("FAIL",))
                    statuses = connection.execute(
                        "SELECT status FROM systematic_fx.source_files "
                        "WHERE dataset_id = %s ORDER BY relative_uri",
                        (dataset_id,),
                    ).fetchall()
                    self.assertEqual(statuses, [("HASHED",), ("HASHED",)])

                    drift_key = connection.execute(
                        "SELECT quality_check_key FROM systematic_fx.quality_checks "
                        "WHERE job_id = %s AND source_file_id = %s",
                        (first.job_id, source_ids[0]),
                    ).fetchone()[0]
                    connection.execute(
                        "UPDATE systematic_fx.quality_checks SET result = 'FAIL' "
                        "WHERE quality_check_key = %s",
                        (drift_key,),
                    )

                with self.assertRaises(FullQcRegistryDriftError):
                    register_full_qc_scan(
                        self.database_url,
                        data_root=root,
                        dataset_key=dataset_key,
                        scan_manifest_path=scan_path,
                        source_manifest_path=source_path,
                    )
            finally:
                if dataset_id is not None:
                    with psycopg.connect(self.database_url) as connection:
                        connection.execute(
                            "DELETE FROM systematic_fx.quality_checks "
                            "WHERE dataset_id = %s OR source_file_id IN "
                            "(SELECT source_file_id FROM systematic_fx.source_files "
                            "WHERE dataset_id = %s)",
                            (dataset_id, dataset_id),
                        )
                        connection.execute(
                            "DELETE FROM systematic_fx.artifacts WHERE producer_job_id IN "
                            "(SELECT job_id FROM systematic_fx.jobs WHERE dataset_id = %s)",
                            (dataset_id,),
                        )
                        connection.execute(
                            "DELETE FROM systematic_fx.jobs WHERE dataset_id = %s",
                            (dataset_id,),
                        )
                        connection.execute(
                            "DELETE FROM systematic_fx.source_files WHERE dataset_id = %s",
                            (dataset_id,),
                        )
                        connection.execute(
                            "DELETE FROM systematic_fx.datasets WHERE dataset_id = %s",
                            (dataset_id,),
                        )


if __name__ == "__main__":
    unittest.main()
