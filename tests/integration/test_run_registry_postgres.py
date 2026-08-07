from __future__ import annotations

import os
import unittest
import uuid
from dataclasses import replace

import psycopg

from systematic_fx.db.migrations import apply_migrations
from systematic_fx.db.run_registry import (
    finish_run_attempt,
    register_run_spec,
    reserve_run_attempt,
    start_run_attempt,
)
from systematic_fx.research.run_spec import RunSpec


class RunRegistryPostgreSQLIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        database_url = os.environ.get("SYSTEMATIC_FX_TEST_DATABASE_URL")
        if not database_url:
            raise unittest.SkipTest("SYSTEMATIC_FX_TEST_DATABASE_URL is not set")
        cls.database_url = database_url
        apply_migrations(database_url, psql_binary=os.environ.get("SYSTEMATIC_FX_PSQL"))

        suffix = uuid.uuid4().hex
        cls.campaign_key = f"run-ledger-test-campaign-{suffix}"
        cls.experiment_key = f"run-ledger-test-experiment-{suffix}"
        dataset_key = f"run-ledger-test-dataset-{suffix}"
        artifact_key = f"run-ledger-test-artifact-{suffix}"
        with psycopg.connect(database_url) as connection, connection.transaction():
            dataset_id = connection.execute(
                """
                INSERT INTO systematic_fx.datasets
                    (dataset_key, provider, feed, data_schema, root_uri,
                     status, manifest_sha256)
                VALUES (%s, 'test', 'test', 'mbp-10', %s, 'VALIDATING', %s)
                RETURNING dataset_id
                """,
                (dataset_key, f"data/test/{suffix}", "a" * 64),
            ).fetchone()[0]
            campaign_id = connection.execute(
                """
                INSERT INTO systematic_fx.campaigns
                    (campaign_key, dataset_id, name, status, data_manifest_sha256,
                     feature_version, outcome_version, cost_model_version,
                     execution_model_version, code_commit, config_sha256,
                     split_policy, trial_budget, finalist_budget)
                VALUES (%s, %s, %s, 'DRAFT', %s, 'feature-v1', 'outcome-v1',
                        'cost-v1', 'execution-v1', %s, %s, '{}'::jsonb, 10, 1)
                RETURNING campaign_id
                """,
                (
                    cls.campaign_key,
                    dataset_id,
                    cls.campaign_key,
                    "a" * 64,
                    "1" * 40,
                    "b" * 64,
                ),
            ).fetchone()[0]
            connection.execute(
                """
                INSERT INTO systematic_fx.experiments
                    (experiment_key, campaign_id, primary_family, hypothesis,
                     direction, model_family, tick_size, tick_value,
                     feature_definition_versions, search_boundary,
                     cost_assumptions, execution_assumptions, trial_budget,
                     code_commit, config_sha256)
                VALUES (%s, %s, 'TEST', 'test hypothesis', 'LONG', 'TEST_MODEL',
                        0.00005, 6.25, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
                        '{}'::jsonb, 10, %s, %s)
                """,
                (cls.experiment_key, campaign_id, "1" * 40, "c" * 64),
            )
            cls.result_artifact_id = connection.execute(
                """
                INSERT INTO systematic_fx.artifacts
                    (artifact_key, artifact_type, uri, sha256, byte_size, media_type)
                VALUES (%s, 'TEST_RESULT', %s, %s, 2, 'application/json')
                RETURNING artifact_id
                """,
                (artifact_key, f"artifacts/test/{suffix}.json", "d" * 64),
            ).fetchone()[0]

    def _spec(self) -> RunSpec:
        return RunSpec(
            campaign_id=self.campaign_key,
            experiment_id=self.experiment_key,
            run_kind="BARRIER_SURFACE",
            engine_version="test-engine-v1",
            source_manifest_hashes={"mbp10": "a" * 64},
            eligible_calendar_version="calendar-v1",
            eligible_calendar_sha256="b" * 64,
            split_version="split-v1",
            split_sha256="c" * 64,
            feature_version="feature-v1",
            feature_sha256="d" * 64,
            outcome_version="outcome-v1",
            outcome_sha256="e" * 64,
            cost_version="cost-v1",
            cost_sha256="f" * 64,
            execution_version="execution-v1",
            execution_sha256="0" * 64,
            code_commit="1" * 40,
            code_snapshot_sha256="3" * 64,
            dependency_lock_sha256="2" * 64,
            runtime_environment={"python": "3.12.13", "postgresql": "18.4"},
            random_seed=2**64 - 1,
            direction="LONG",
            signal_policy={"rule": "test"},
            entry_policy={"type": "MARKETABLE_LIMIT"},
            barrier_policy={"grid": [24, 32, 40]},
            terminal_policy={"rule": "expiry_month_start"},
            parameters={"threshold": "0.75"},
        )

    def test_exact_success_is_reused_and_history_is_immutable(self) -> None:
        spec = self._spec()
        first = register_run_spec(self.database_url, spec)
        second = register_run_spec(self.database_url, spec)
        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(first.research_run_spec_id, second.research_run_spec_id)

        queued = reserve_run_attempt(self.database_url, run_fingerprint=spec.fingerprint)
        self.assertTrue(queued.execute)
        running = start_run_attempt(
            self.database_url,
            research_run_attempt_id=queued.research_run_attempt_id,
        )
        self.assertEqual(running.status, "RUNNING")
        succeeded = finish_run_attempt(
            self.database_url,
            research_run_attempt_id=queued.research_run_attempt_id,
            status="SUCCEEDED",
            result_summary={"cell_count": 484},
            result_artifact_id=self.result_artifact_id,
        )
        self.assertEqual(succeeded.status, "SUCCEEDED")

        duplicate = reserve_run_attempt(self.database_url, run_fingerprint=spec.fingerprint)
        self.assertFalse(duplicate.execute)
        self.assertEqual(duplicate.status, "SKIPPED_DUPLICATE")
        self.assertEqual(duplicate.reused_attempt_id, queued.research_run_attempt_id)

        with psycopg.connect(self.database_url) as connection:
            statuses = connection.execute(
                """
                SELECT status
                FROM systematic_fx.research_run_attempts
                WHERE research_run_spec_id = %s
                ORDER BY attempt_number
                """,
                (first.research_run_spec_id,),
            ).fetchall()
            self.assertEqual(statuses, [("SUCCEEDED",), ("SKIPPED_DUPLICATE",)])

            with self.assertRaises(psycopg.errors.RaiseException):
                connection.execute(
                    "DELETE FROM systematic_fx.research_run_specs WHERE research_run_spec_id = %s",
                    (first.research_run_spec_id,),
                )
            connection.rollback()
            with self.assertRaises(psycopg.errors.RaiseException):
                connection.execute(
                    "UPDATE systematic_fx.research_run_attempts "
                    "SET result_summary = '{}'::jsonb "
                    "WHERE research_run_attempt_id = %s",
                    (queued.research_run_attempt_id,),
                )

    def test_campaign_level_feature_build_has_no_experiment_foreign_key(self) -> None:
        campaign_level = replace(
            self._spec(),
            experiment_id=None,
            run_kind="FEATURE_BUILD",
            parameters={"source_dates": ["2022-01-03"]},
        )
        registration = register_run_spec(self.database_url, campaign_level)

        self.assertIsNone(registration.experiment_id)
        with psycopg.connect(self.database_url) as connection:
            stored = connection.execute(
                "SELECT experiment_id FROM systematic_fx.research_run_specs "
                "WHERE research_run_spec_id = %s",
                (registration.research_run_spec_id,),
            ).fetchone()
        self.assertEqual(stored, (None,))


if __name__ == "__main__":
    unittest.main()
