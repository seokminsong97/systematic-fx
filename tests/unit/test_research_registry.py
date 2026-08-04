import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import psycopg

from systematic_fx.db.research_registry import (
    ResearchRegistryError,
    _write_registration_artifact,
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
