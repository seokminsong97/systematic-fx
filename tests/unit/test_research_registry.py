import hashlib
import tempfile
import unittest
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Self
from unittest.mock import patch

import psycopg
from psycopg import IsolationLevel

from systematic_fx.db.research_registry import (
    ResearchRegistryDriftError,
    ResearchRegistryError,
    _verify_phase1a_artifact_file,
    _write_registration_artifact,
    complete_discovery_run_success,
    prepare_parent_hypothesis_registration,
    record_discovery_exposure,
    register_parent_hypothesis_bundle,
)

ROOT = Path(__file__).resolve().parents[2]
CAMPAIGN = ROOT / "configs" / "campaigns" / "phase1_discovery_v1.toml"
HYPOTHESES = ROOT / "configs" / "research" / "phase1_parent_hypotheses_v1.toml"
COST = ROOT / "configs" / "costs" / "cost_pending_v1.toml"
EXECUTION = ROOT / "configs" / "execution" / "execution_pending_v1.toml"
SOURCE_MANIFEST_SHA256 = "14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de"


class _DbResult:
    def __init__(self, value: object) -> None:
        self.value = value

    def fetchone(self) -> dict[str, Any] | None:
        if self.value is None or isinstance(self.value, dict):
            return self.value
        raise AssertionError("result is not a single row")

    def fetchall(self) -> list[dict[str, Any]]:
        if isinstance(self.value, list):
            return self.value
        raise AssertionError("result is not a row list")


class _Transaction:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _DbConnection:
    def __init__(self, responses: Iterable[object]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, object]] = []
        self.isolation_level: IsolationLevel | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def transaction(self) -> _Transaction:
        return _Transaction()

    def execute(self, sql: str, parameters: object = ()) -> _DbResult:
        self.calls.append((" ".join(sql.split()), parameters))
        if not self.responses:
            raise AssertionError(f"unexpected SQL: {sql}")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return _DbResult(response)


class ResearchRegistryPreparationTest(unittest.TestCase):
    def _prepare(self):
        return prepare_parent_hypothesis_registration(
            campaign_config_path=CAMPAIGN,
            hypothesis_config_path=HYPOTHESES,
            cost_config_path=COST,
            execution_config_path=EXECUTION,
            code_commit="unit-test-code-commit",
        )

    def test_prepared_bundle_preserves_pending_gates_and_null_boundaries(self) -> None:
        prepared = self._prepare()

        self.assertEqual(prepared.campaign_key, "phase1_discovery_v1")
        self.assertEqual(len(prepared.hypothesis_bundle.hypotheses), 60)
        self.assertIsNone(prepared.campaign_document["selected_start_date"])
        self.assertIsNone(prepared.campaign_document["selected_end_date"])
        self.assertEqual(
            prepared.campaign_document["data_manifest_sha256"],
            SOURCE_MANIFEST_SHA256,
        )
        self.assertEqual(
            prepared.dataset_document["manifest_sha256"],
            SOURCE_MANIFEST_SHA256,
        )
        self.assertFalse(prepared.campaign_document["split_policy"]["research_eligible"])
        self.assertTrue(
            prepared.campaign_document["split_policy"]["data_gate"]["policy"][
                "block_strategy_performance"
            ]
        )
        self.assertFalse(prepared.cost_assumptions["numeric_verified"])
        self.assertFalse(prepared.execution_assumptions["numeric_verified"])
        self.assertTrue(prepared.cost_assumptions["execution_blocked"])
        self.assertTrue(prepared.execution_assumptions["execution_blocked"])
        self.assertEqual(len(prepared.registration_sha256), 64)

    def test_registration_artifact_is_content_addressed_atomic_and_repeatable(self) -> None:
        prepared = self._prepare()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "artifacts"

            first = _write_registration_artifact(prepared, root)
            second = _write_registration_artifact(prepared, root)

            self.assertEqual(first, second)
            self.assertEqual(first.parent, (root / "registration").resolve())
            self.assertEqual(first.read_bytes(), prepared.registration_bytes)
            self.assertIn(prepared.registration_sha256, first.name)
            self.assertEqual(list(first.parent.glob("*.tmp")), [])

    def test_discovery_exposure_rejects_naive_time_before_connecting(self) -> None:
        with self.assertRaisesRegex(ResearchRegistryError, "timezone-aware"):
            record_discovery_exposure(
                "postgresql:///unused",
                campaign_key="campaign",
                exposure_key="exposure",
                exposure_type="PIPELINE_PILOT",
                source_interval_start=datetime(2022, 1, 3, tzinfo=UTC).replace(tzinfo=None),
                source_interval_end=datetime(2022, 1, 3, tzinfo=UTC),
                query_spec={},
                result_summary={},
                visible_to_ai=True,
                research_eligible=False,
                code_commit="test",
                config_sha256="0" * 64,
            )

    def test_discovery_exposure_rejects_invalid_run_fingerprint_before_connecting(self) -> None:
        with self.assertRaisesRegex(ResearchRegistryError, "run_fingerprint"):
            record_discovery_exposure(
                "postgresql:///unused",
                campaign_key="campaign",
                exposure_key="exposure",
                exposure_type="AI_SLICE",
                source_interval_start=datetime(2022, 1, 3, tzinfo=UTC),
                source_interval_end=datetime(2022, 1, 4, tzinfo=UTC),
                query_spec={},
                result_summary={},
                visible_to_ai=True,
                research_eligible=True,
                code_commit="test",
                config_sha256="0" * 64,
                run_fingerprint="not-a-sha",
            )

    def test_discovery_artifact_must_stay_under_artifacts_root(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifacts = root / "artifacts"
            artifacts.mkdir()
            outside = root / "outside.json"
            outside.write_text("{}")

            with self.assertRaisesRegex(ResearchRegistryError, "must be contained"):
                record_discovery_exposure(
                    "postgresql:///unused",
                    campaign_key="campaign",
                    exposure_key="exposure",
                    exposure_type="PIPELINE_PILOT",
                    source_interval_start=datetime(2022, 1, 3, tzinfo=UTC),
                    source_interval_end=datetime(2022, 1, 3, 1, tzinfo=UTC),
                    query_spec={},
                    result_summary={},
                    visible_to_ai=True,
                    research_eligible=False,
                    code_commit="test",
                    config_sha256="0" * 64,
                    result_artifact_path=outside,
                    artifacts_root=artifacts,
                )

    def test_phase1a_success_attempt_and_exposure_share_one_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            artifact_directory = data_root / "derived"
            artifact_directory.mkdir(parents=True)
            payload = b'{"artifact_schema":"test"}\n'
            digest = hashlib.sha256(payload).hexdigest()
            artifact = artifact_directory / f"sha256={digest}.json"
            artifact.write_bytes(payload)
            campaign_key = "phase1a_conservative_screening_v1"
            exposure_key = "phase1a:slice:0:ai"
            fingerprint = "a" * 64
            parent_fingerprint = "c" * 64
            parent_artifact_sha256 = "d" * 64
            start = datetime(2022, 1, 2, tzinfo=UTC)
            end = datetime(2022, 1, 7, tzinfo=UTC)
            artifact_row = {
                "artifact_id": 77,
                "artifact_key": (f"{campaign_key}:discovery-exposure:{exposure_key}:{digest}"),
                "artifact_type": "DISCOVERY_EXPOSURE_RESULT",
                "uri": artifact.resolve().as_uri(),
                "sha256": digest,
                "byte_size": len(artifact.read_bytes()),
                "media_type": "application/json",
                "producer_job_id": None,
                "metadata": {
                    "campaign_key": campaign_key,
                    "exposure_key": exposure_key,
                    "exposure_type": "AI_SLICE",
                    "run_fingerprint": fingerprint,
                },
            }
            exposure_row = {
                "discovery_exposure_id": 88,
                "exposure_key": exposure_key,
                "campaign_id": 11,
                "exposure_type": "AI_SLICE",
                "source_interval_start": start,
                "source_interval_end": end,
                "visible_to_ai": True,
                "research_eligible": False,
                "query_spec": {"slice_index": 0},
                "result_summary": {
                    "eligible_rows": 4,
                    "feature_manifest_sha256": parent_artifact_sha256,
                },
                "result_artifact_id": 77,
                "code_commit": "1" * 40,
                "config_sha256": "b" * 64,
                "research_run_spec_id": 31,
            }
            connection = _DbConnection(
                [
                    None,
                    {"campaign_id": 11, "status": "DRAFT"},
                    {
                        "research_run_spec_id": 31,
                        "campaign_id": 11,
                        "parent_run_spec_id": 30,
                        "run_kind": "AI_SLICE",
                        "code_commit": "1" * 40,
                        "canonical_spec": {
                            "parameters": {
                                "feature_manifest_sha256": parent_artifact_sha256,
                                "parent_run_fingerprint": parent_fingerprint,
                            }
                        },
                    },
                    [
                        {
                            "research_run_spec_id": 30,
                            "run_fingerprint": parent_fingerprint,
                            "run_kind": "FEATURE_BUILD",
                            "result_artifact_id": 66,
                            "artifact_sha256": parent_artifact_sha256,
                            "artifact_type": "PHASE1A_FEATURE_BUILD_MANIFEST",
                        }
                    ],
                    {
                        "research_run_attempt_id": 55,
                        "research_run_spec_id": 31,
                        "status": "RUNNING",
                        "started_at": start,
                    },
                    {"artifact_id": 77},
                    [artifact_row],
                    {"research_run_attempt_id": 55},
                    {"discovery_exposure_id": 88},
                    exposure_row,
                ]
            )

            with patch(
                "systematic_fx.db.research_registry.psycopg.connect",
                return_value=connection,
            ):
                report = complete_discovery_run_success(
                    "postgresql:///test",
                    research_run_attempt_id=55,
                    campaign_key=campaign_key,
                    exposure_key=exposure_key,
                    exposure_type="AI_SLICE",
                    source_interval_start=start,
                    source_interval_end=end,
                    query_spec={"slice_index": 0},
                    exposure_result_summary={
                        "eligible_rows": 4,
                        "feature_manifest_sha256": parent_artifact_sha256,
                    },
                    attempt_result_summary={
                        "artifact_sha256": digest,
                        "pipeline_version": "v1",
                    },
                    code_commit="1" * 40,
                    config_sha256="b" * 64,
                    run_fingerprint=fingerprint,
                    expected_artifact_sha256=digest,
                    result_artifact_path=artifact,
                    artifacts_root=data_root / "derived",
                )

            self.assertEqual(report.result_artifact_id, 77)
            self.assertTrue(report.created_exposure)
            self.assertTrue(report.created_artifact)
            self.assertEqual(connection.isolation_level, IsolationLevel.SERIALIZABLE)
            success_index = next(
                index
                for index, (sql, _) in enumerate(connection.calls)
                if "SET status = 'SUCCEEDED'" in sql
            )
            exposure_index = next(
                index
                for index, (sql, _) in enumerate(connection.calls)
                if "INSERT INTO systematic_fx.discovery_exposures" in sql
            )
            self.assertLess(success_index, exposure_index)
            self.assertFalse(connection.responses)

    def test_phase1a_artifact_file_must_remain_content_addressed_and_reachable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            artifact_directory = Path(directory) / "data" / "derived" / "manifests"
            artifact_directory.mkdir(parents=True)
            payload = b'{"artifact_schema":"test"}\n'
            digest = hashlib.sha256(payload).hexdigest()
            artifact = artifact_directory / f"sha256={digest}.json"
            artifact.write_bytes(payload)
            row = {
                "artifact_uri": artifact.resolve().as_uri(),
                "artifact_sha256": digest,
                "artifact_byte_size": len(payload),
            }

            _verify_phase1a_artifact_file(row)
            artifact.write_bytes(b"x" * len(payload))
            with self.assertRaisesRegex(ResearchRegistryDriftError, "content drift"):
                _verify_phase1a_artifact_file(row)

    def test_public_registration_wraps_driver_errors(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "data"
            (data_root / "mbp-10").mkdir(parents=True)

            with (
                patch(
                    "systematic_fx.db.research_registry.psycopg.connect",
                    side_effect=psycopg.OperationalError("database unavailable"),
                ),
                self.assertRaisesRegex(
                    ResearchRegistryError,
                    "PostgreSQL parent-hypothesis registration failed",
                ) as raised,
            ):
                register_parent_hypothesis_bundle(
                    "postgresql:///unavailable",
                    campaign_config_path=CAMPAIGN,
                    hypothesis_config_path=HYPOTHESES,
                    cost_config_path=COST,
                    execution_config_path=EXECUTION,
                    data_root=data_root,
                    artifacts_root=root / "artifacts",
                    code_commit="unit-test-code-commit",
                )

            self.assertIsInstance(raised.exception.__cause__, psycopg.OperationalError)


if __name__ == "__main__":
    unittest.main()
