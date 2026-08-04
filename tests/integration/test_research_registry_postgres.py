import os
import tempfile
import unittest
import uuid
from datetime import UTC, date, datetime
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from systematic_fx.db.migrations import apply_migrations
from systematic_fx.db.research_registry import (
    ResearchRegistryDriftError,
    record_discovery_exposure,
    register_parent_hypothesis_bundle,
)
from systematic_fx.research.hypotheses import canonical_sha256

ROOT = Path(__file__).resolve().parents[2]
CAMPAIGN = ROOT / "configs" / "campaigns" / "phase1_discovery_v1.toml"
HYPOTHESES = ROOT / "configs" / "research" / "phase1_parent_hypotheses_v1.toml"
COST = ROOT / "configs" / "costs" / "cost_pending_v1.toml"
EXECUTION = ROOT / "configs" / "execution" / "execution_pending_v1.toml"
SOURCE_MANIFEST_SHA256 = "14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de"


class ResearchRegistryIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.database_url = os.environ.get("SYSTEMATIC_FX_TEST_DATABASE_URL")
        if not cls.database_url:
            raise unittest.SkipTest("SYSTEMATIC_FX_TEST_DATABASE_URL is not set")
        apply_migrations(
            cls.database_url,
            psql_binary=os.environ.get("SYSTEMATIC_FX_PSQL"),
        )

    def _cleanup(self, campaign_key: str, dataset_key: str) -> None:
        with psycopg.connect(self.database_url) as connection:  # noqa: SIM117
            with connection.transaction():
                campaign = connection.execute(
                    "SELECT campaign_id FROM systematic_fx.campaigns WHERE campaign_key = %s",
                    (campaign_key,),
                ).fetchone()
                if campaign is not None:
                    campaign_id = campaign[0]
                    connection.execute(
                        "DELETE FROM systematic_fx.experiments WHERE campaign_id = %s",
                        (campaign_id,),
                    )
                    connection.execute(
                        "DELETE FROM systematic_fx.discovery_exposures WHERE campaign_id = %s",
                        (campaign_id,),
                    )
                connection.execute(
                    "DELETE FROM systematic_fx.artifacts WHERE metadata->>'campaign_key' = %s",
                    (campaign_key,),
                )
                connection.execute(
                    "DELETE FROM systematic_fx.jobs WHERE payload->>'campaign_key' = %s",
                    (campaign_key,),
                )
                connection.execute(
                    "DELETE FROM systematic_fx.campaigns WHERE campaign_key = %s",
                    (campaign_key,),
                )
                connection.execute(
                    "DELETE FROM systematic_fx.datasets WHERE dataset_key = %s",
                    (dataset_key,),
                )

    def test_bundle_and_exposure_are_transactional_idempotent_and_drift_rejecting(
        self,
    ) -> None:
        suffix = uuid.uuid4().hex
        campaign_key = f"phase1_discovery_test_{suffix}"
        feed = f"TEST.{suffix}"
        dataset_key = f"test_{suffix}_mbp_10_6e_fut_v1"
        code_commit = f"integration-test-{suffix}"

        with tempfile.TemporaryDirectory() as directory:
            temporary_root = Path(directory)
            data_root = temporary_root / "data"
            (data_root / "mbp-10").mkdir(parents=True)
            artifacts_root = temporary_root / "artifacts"
            campaign_config = temporary_root / "campaign.toml"
            campaign_config.write_text(
                CAMPAIGN.read_text()
                .replace('id = "phase1_discovery_v1"', f'id = "{campaign_key}"', 1)
                .replace('dataset = "GLBX.MDP3"', f'dataset = "{feed}"', 1)
            )
            hypothesis_config = temporary_root / "hypotheses.toml"
            hypothesis_config.write_bytes(HYPOTHESES.read_bytes())

            try:
                # Simulate the source-data registry winning the creation race. Parent
                # registration must preserve the row and merge only its owned metadata.
                with psycopg.connect(self.database_url) as connection:
                    connection.execute(
                        """
                        INSERT INTO systematic_fx.datasets
                            (dataset_key, provider, feed, data_schema, root_uri,
                             price_scale_exponent, status, expected_start_date,
                             expected_end_date, manifest_sha256, metadata)
                        VALUES (%s, 'Databento', %s, 'mbp-10', %s, -9, 'VALIDATING',
                                %s, %s, %s, '{}'::jsonb)
                        """,
                        (
                            dataset_key,
                            feed,
                            (data_root / "mbp-10").resolve().as_uri(),
                            date(2022, 1, 2),
                            date(2026, 7, 31),
                            SOURCE_MANIFEST_SHA256,
                        ),
                    )

                first = register_parent_hypothesis_bundle(
                    self.database_url,
                    campaign_config_path=campaign_config,
                    hypothesis_config_path=hypothesis_config,
                    cost_config_path=COST,
                    execution_config_path=EXECUTION,
                    data_root=data_root,
                    artifacts_root=artifacts_root,
                    code_commit=code_commit,
                )
                second = register_parent_hypothesis_bundle(
                    self.database_url,
                    campaign_config_path=campaign_config,
                    hypothesis_config_path=hypothesis_config,
                    cost_config_path=COST,
                    execution_config_path=EXECUTION,
                    data_root=data_root,
                    artifacts_root=artifacts_root,
                    code_commit=code_commit,
                )

                self.assertEqual(first.dataset_key, dataset_key)
                self.assertEqual(len(first.experiment_ids), 60)
                self.assertEqual(first.created_experiments, 60)
                self.assertFalse(first.created_dataset)
                self.assertEqual(second.created_experiments, 0)
                self.assertFalse(second.created_dataset)
                self.assertFalse(second.created_campaign)
                self.assertFalse(second.created_job)
                self.assertFalse(second.created_artifact)
                self.assertEqual(first.experiment_ids, second.experiment_ids)

                with psycopg.connect(self.database_url, row_factory=dict_row) as connection:
                    dataset_row = connection.execute(
                        """
                        SELECT manifest_sha256, metadata
                        FROM systematic_fx.datasets WHERE dataset_id = %s
                        """,
                        (first.dataset_id,),
                    ).fetchone()
                    campaign_row = connection.execute(
                        """
                        SELECT status, selected_start_date, selected_end_date,
                               roll_cutoff_date, data_manifest_sha256, split_policy
                        FROM systematic_fx.campaigns WHERE campaign_id = %s
                        """,
                        (first.campaign_id,),
                    ).fetchone()
                    experiment_rows = connection.execute(
                        """
                        SELECT primary_family, pattern_id, status, trials_registered,
                               trial_budget, cost_assumptions, execution_assumptions,
                               search_boundary
                        FROM systematic_fx.experiments WHERE campaign_id = %s
                        """,
                        (first.campaign_id,),
                    ).fetchall()
                    pattern_row = connection.execute(
                        """
                        SELECT count(*) AS pattern_count
                        FROM systematic_fx.pattern_ledger
                        WHERE campaign_id = %s
                        """,
                        (first.campaign_id,),
                    ).fetchone()

                self.assertEqual(dataset_row["manifest_sha256"], SOURCE_MANIFEST_SHA256)
                self.assertEqual(
                    dataset_row["metadata"],
                    {
                        "parent_symbol": "6E.FUT",
                        "source_manifest_kind": "full_content_sha256_v1",
                    },
                )
                self.assertEqual(campaign_row["status"], "DRAFT")
                self.assertIsNone(campaign_row["selected_start_date"])
                self.assertIsNone(campaign_row["selected_end_date"])
                self.assertIsNone(campaign_row["roll_cutoff_date"])
                self.assertEqual(campaign_row["data_manifest_sha256"], SOURCE_MANIFEST_SHA256)
                self.assertFalse(campaign_row["split_policy"]["research_eligible"])
                self.assertEqual(len(experiment_rows), 60)
                self.assertEqual(
                    {
                        family: sum(row["primary_family"] == family for row in experiment_rows)
                        for family in (f"P{i}" for i in range(1, 7))
                    },
                    {f"P{i}": 10 for i in range(1, 7)},
                )
                self.assertTrue(all(row["pattern_id"] is None for row in experiment_rows))
                self.assertTrue(all(row["status"] == "REGISTERED" for row in experiment_rows))
                self.assertTrue(all(row["trials_registered"] == 0 for row in experiment_rows))
                self.assertTrue(all(row["trial_budget"] == 272 for row in experiment_rows))
                self.assertTrue(
                    all(not row["cost_assumptions"]["numeric_verified"] for row in experiment_rows)
                )
                self.assertTrue(
                    all(
                        not row["execution_assumptions"]["numeric_verified"]
                        for row in experiment_rows
                    )
                )
                self.assertTrue(
                    all(row["search_boundary"]["execution_blocked"] for row in experiment_rows)
                )
                self.assertEqual(pattern_row["pattern_count"], 0)

                drifted = hypothesis_config.read_text().replace(
                    "A directional return accompanied by stable spread",
                    "A drifted directional return accompanied by stable spread",
                    1,
                )
                hypothesis_config.write_text(drifted)
                with self.assertRaises(ResearchRegistryDriftError):
                    register_parent_hypothesis_bundle(
                        self.database_url,
                        campaign_config_path=campaign_config,
                        hypothesis_config_path=hypothesis_config,
                        cost_config_path=COST,
                        execution_config_path=EXECUTION,
                        data_root=data_root,
                        artifacts_root=artifacts_root,
                        code_commit=code_commit,
                    )

                exposure_artifact = artifacts_root / "discovery" / "pilot.json"
                exposure_artifact.parent.mkdir(parents=True)
                exposure_artifact.write_text('{"rows":1}\n')
                exposure_key = f"{campaign_key}:pilot:2022-01-03"
                exposure_config_sha = canonical_sha256({"pilot": "2022-01-03", "row_groups": 1})
                exposure_arguments = {
                    "campaign_key": campaign_key,
                    "exposure_key": exposure_key,
                    "exposure_type": "PIPELINE_PILOT",
                    "source_interval_start": datetime(2022, 1, 3, tzinfo=UTC),
                    "source_interval_end": datetime(2022, 1, 4, tzinfo=UTC),
                    "query_spec": {"row_groups": 1},
                    "result_summary": {"rows": 1, "decision": "PIPELINE_ONLY"},
                    "visible_to_ai": True,
                    "research_eligible": False,
                    "code_commit": code_commit,
                    "config_sha256": exposure_config_sha,
                    "result_artifact_path": exposure_artifact,
                    "artifacts_root": artifacts_root,
                }
                exposure_first = record_discovery_exposure(
                    self.database_url,
                    **exposure_arguments,
                )
                exposure_second = record_discovery_exposure(
                    self.database_url,
                    **exposure_arguments,
                )
                self.assertTrue(exposure_first.created_exposure)
                self.assertTrue(exposure_first.created_artifact)
                self.assertFalse(exposure_second.created_exposure)
                self.assertFalse(exposure_second.created_artifact)
                self.assertEqual(
                    exposure_first.discovery_exposure_id,
                    exposure_second.discovery_exposure_id,
                )

                exposure_arguments["result_summary"] = {
                    "rows": 2,
                    "decision": "PIPELINE_ONLY",
                }
                with self.assertRaises(ResearchRegistryDriftError):
                    record_discovery_exposure(
                        self.database_url,
                        **exposure_arguments,
                    )
            finally:
                self._cleanup(campaign_key, dataset_key)


if __name__ == "__main__":
    unittest.main()
