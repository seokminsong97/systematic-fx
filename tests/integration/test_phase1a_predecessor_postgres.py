from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
import unittest
import uuid
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Self
from unittest.mock import patch

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.db.migrations import apply_migrations
from systematic_fx.db.research_registry import (
    ResearchRegistryDriftError,
    verify_phase1a_current_slice_prefix,
    verify_phase1a_predecessor_slice,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.run_spec import (
    RUN_SPEC_SCHEMA,
    RUN_SPEC_SCHEMA_VERSION,
    RunSpec,
)

CAMPAIGN = "phase1a_conservative_screening_v1"
ROLLUP_SCHEMA = "systematic_fx.phase1a_pattern_rollup.v1"
AI_ENGINE_VERSION = "phase1a_fixed_query_discovery_v1"
QUERY_ENGINE_VERSION = "phase1a_fixed_query_projection_v1"


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


class _BorrowedConnection:
    """Let the public verifier use a real connection without owning/committing it."""

    def __init__(self, connection: psycopg.Connection[dict[str, object]]) -> None:
        self.connection = connection

    @property
    def isolation_level(self) -> object:
        return self.connection.isolation_level

    @isolation_level.setter
    def isolation_level(self, value: object) -> None:
        del value  # The outer fixture transaction already owns its isolation boundary.

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def transaction(self) -> object:
        return self.connection.transaction()

    def execute(self, query: object, params: object = ()) -> object:
        return self.connection.execute(query, params)


class Phase1APredecessorPostgreSQLIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        database_url = os.environ.get("SYSTEMATIC_FX_TEST_DATABASE_URL")
        if not database_url:
            raise unittest.SkipTest("SYSTEMATIC_FX_TEST_DATABASE_URL is not set")
        cls.database_url = database_url
        apply_migrations(database_url, psql_binary=os.environ.get("SYSTEMATIC_FX_PSQL"))

    @staticmethod
    def _campaign_id(
        connection: psycopg.Connection[dict[str, object]],
        *,
        suffix: str,
    ) -> int:
        row = connection.execute(
            "SELECT campaign_id FROM systematic_fx.campaigns WHERE campaign_key = %s",
            (CAMPAIGN,),
        ).fetchone()
        if row is not None:
            return int(row["campaign_id"])
        dataset_id = connection.execute(
            """
            INSERT INTO systematic_fx.datasets
                (dataset_key, provider, feed, data_schema, root_uri,
                 status, manifest_sha256)
            VALUES (%s, 'test', 'test', 'mbp-10', %s, 'VALIDATING', %s)
            RETURNING dataset_id
            """,
            (f"predecessor-dataset-{suffix}", f"data/test/{suffix}", "0" * 64),
        ).fetchone()["dataset_id"]
        return int(
            connection.execute(
                """
                INSERT INTO systematic_fx.campaigns
                    (campaign_key, dataset_id, name, status, data_manifest_sha256,
                     feature_version, outcome_version, cost_model_version,
                     execution_model_version, code_commit, config_sha256,
                     split_policy, trial_budget, finalist_budget)
                VALUES (%s, %s, 'Phase 1A predecessor fixture', 'DRAFT', %s,
                        'feature-v1', 'outcome-v1', 'cost-v1', 'execution-v1',
                        'fixture', %s, '{}'::jsonb, 272, 10)
                RETURNING campaign_id
                """,
                (CAMPAIGN, dataset_id, "0" * 64, "1" * 64),
            ).fetchone()["campaign_id"]
        )

    @staticmethod
    def _unused_slice_index(
        connection: psycopg.Connection[dict[str, object]],
        *,
        campaign_id: int,
    ) -> int:
        rows = connection.execute(
            """
            SELECT DISTINCT run_spec.canonical_spec #>> '{parameters,slice_index}' AS value
            FROM systematic_fx.discovery_exposures AS exposure
            JOIN systematic_fx.research_run_specs AS run_spec
              ON run_spec.research_run_spec_id = exposure.research_run_spec_id
            WHERE exposure.campaign_id = %s
            """,
            (campaign_id,),
        ).fetchall()
        used = {
            int(row["value"])
            for row in rows
            if isinstance(row["value"], str) and row["value"].isdigit()
        }
        available = sorted(set(range(99)) - used, reverse=True)
        if not available:
            raise unittest.SkipTest("the Phase 1A test campaign has no unused slice index")
        return available[0]

    @staticmethod
    def _empty_slice_index(
        connection: psycopg.Connection[dict[str, object]],
        *,
        campaign_id: int,
        date_strings: list[str],
        interval_start: datetime,
        interval_end: datetime,
    ) -> int:
        for slice_index in range(98, -1, -1):
            ai_key = f"{CAMPAIGN}:ai-slice:{slice_index:02d}"
            query_key_pattern = f"{CAMPAIGN}:query:{slice_index:02d}:%"
            row = connection.execute(
                """
                SELECT
                    (
                        SELECT count(*)
                        FROM systematic_fx.research_run_specs AS run_spec
                        WHERE run_spec.campaign_id = %s
                          AND run_spec.run_kind IN
                              ('FEATURE_BUILD', 'AI_SLICE', 'QUERY', 'VALIDATION')
                          AND (
                            run_spec.canonical_spec #>> '{parameters,slice_index}' = %s
                            OR run_spec.canonical_spec
                                 #> '{parameters,requested_source_dates}' = %s
                          )
                    ) AS run_state_count,
                    (
                        SELECT count(*)
                        FROM systematic_fx.discovery_exposures AS exposure
                        JOIN systematic_fx.research_run_specs AS run_spec
                          ON run_spec.research_run_spec_id = exposure.research_run_spec_id
                         AND run_spec.campaign_id = exposure.campaign_id
                        WHERE exposure.campaign_id = %s
                          AND exposure.exposure_type IN ('AI_SLICE', 'QUERY')
                          AND (
                            (exposure.source_interval_start = %s
                             AND exposure.source_interval_end = %s)
                            OR run_spec.canonical_spec
                                 #>> '{parameters,slice_index}' = %s
                            OR run_spec.canonical_spec
                                 #> '{parameters,requested_source_dates}' = %s
                            OR exposure.exposure_key = %s
                            OR exposure.exposure_key LIKE %s
                          )
                    ) AS exposure_count
                """,
                (
                    campaign_id,
                    str(slice_index),
                    Jsonb(date_strings),
                    campaign_id,
                    interval_start,
                    interval_end,
                    str(slice_index),
                    Jsonb(date_strings),
                    ai_key,
                    query_key_pattern,
                ),
            ).fetchone()
            if int(row["run_state_count"]) == 0 and int(row["exposure_count"]) == 0:
                return slice_index
        raise unittest.SkipTest("the Phase 1A test campaign has no truly empty slice")

    @staticmethod
    def _campaign_ledger_snapshot(
        connection: psycopg.Connection[dict[str, object]],
        *,
        campaign_id: int,
    ) -> tuple[int, int, int, int]:
        row = connection.execute(
            """
            SELECT
                (SELECT count(*)
                 FROM systematic_fx.research_run_specs
                 WHERE campaign_id = %s) AS run_spec_count,
                (SELECT count(*)
                 FROM systematic_fx.research_run_attempts AS attempt
                 JOIN systematic_fx.research_run_specs AS run_spec
                   ON run_spec.research_run_spec_id = attempt.research_run_spec_id
                 WHERE run_spec.campaign_id = %s) AS attempt_count,
                (SELECT count(*)
                 FROM systematic_fx.discovery_exposures
                 WHERE campaign_id = %s) AS exposure_count,
                (SELECT count(*)
                 FROM systematic_fx.pattern_ledger
                 WHERE campaign_id = %s) AS pattern_count
            """,
            (campaign_id, campaign_id, campaign_id, campaign_id),
        ).fetchone()
        return (
            int(row["run_spec_count"]),
            int(row["attempt_count"]),
            int(row["exposure_count"]),
            int(row["pattern_count"]),
        )

    @staticmethod
    def _insert_run_spec(
        connection: psycopg.Connection[dict[str, object]],
        *,
        campaign_id: int,
        run_spec: RunSpec,
        parent_run_spec_id: int | None,
    ) -> int:
        canonical_spec = json.loads(run_spec.canonical_json())
        return int(
            connection.execute(
                """
                INSERT INTO systematic_fx.research_run_specs
                    (run_fingerprint, canonicalization_schema,
                     canonicalization_version, campaign_id, experiment_id,
                     parent_run_spec_id, run_kind, engine_version, canonical_spec,
                     source_manifest_hashes, eligible_calendar_version,
                     eligible_calendar_sha256, split_version, split_sha256,
                     feature_version, feature_sha256, outcome_version,
                     outcome_sha256, cost_version, cost_sha256, execution_version,
                     execution_sha256, code_commit, code_snapshot_sha256,
                     dependency_lock_sha256, deterministic_seed, direction)
                VALUES (%s, %s, %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s)
                RETURNING research_run_spec_id
                """,
                (
                    run_spec.fingerprint,
                    RUN_SPEC_SCHEMA,
                    RUN_SPEC_SCHEMA_VERSION,
                    campaign_id,
                    parent_run_spec_id,
                    run_spec.run_kind,
                    run_spec.engine_version,
                    Jsonb(canonical_spec),
                    Jsonb(dict(run_spec.source_manifest_hashes)),
                    run_spec.eligible_calendar_version,
                    run_spec.eligible_calendar_sha256,
                    run_spec.split_version,
                    run_spec.split_sha256,
                    run_spec.feature_version,
                    run_spec.feature_sha256,
                    run_spec.outcome_version,
                    run_spec.outcome_sha256,
                    run_spec.cost_version,
                    run_spec.cost_sha256,
                    run_spec.execution_version,
                    run_spec.execution_sha256,
                    run_spec.code_commit,
                    run_spec.code_snapshot_sha256,
                    run_spec.dependency_lock_sha256,
                    run_spec.random_seed,
                    run_spec.direction,
                ),
            ).fetchone()["research_run_spec_id"]
        )

    @staticmethod
    def _base_run_spec(
        *,
        run_kind: str,
        engine_version: str,
        parameters: dict[str, object],
    ) -> RunSpec:
        return RunSpec(
            campaign_id=CAMPAIGN,
            experiment_id=None,
            run_kind=run_kind,
            engine_version=engine_version,
            source_manifest_hashes={
                "mbp10_footer_manifest_v1": "1" * 64,
                "mbp10_source_sha256_v1": "2" * 64,
                "mbp10_structural_qc_v1": "3" * 64,
            },
            eligible_calendar_version="calendar-v1",
            eligible_calendar_sha256="4" * 64,
            split_version="split-v1",
            split_sha256="5" * 64,
            feature_version="feature-v1",
            feature_sha256="6" * 64,
            outcome_version="outcome-v1",
            outcome_sha256="7" * 64,
            cost_version="cost-v1",
            cost_sha256="8" * 64,
            execution_version="execution-v1",
            execution_sha256="9" * 64,
            code_commit="a" * 40,
            code_snapshot_sha256="b" * 64,
            dependency_lock_sha256="c" * 64,
            runtime_environment={"python": "3.12.13", "test_fixture": True},
            random_seed=1,
            direction="BOTH",
            signal_policy={"signal_cadence_seconds": 300},
            entry_policy={"entry": "NEXT_EVENT"},
            barrier_policy={"same_event": "LOSS_FIRST"},
            terminal_policy={"open_position": "UNRESOLVED"},
            parameters=parameters,
        )

    def test_truly_empty_slice_is_read_only_under_postgresql_18(self) -> None:
        suffix = uuid.uuid4().hex
        source_dates = tuple(date(2199, 7, day) for day in (1, 2, 3, 4, 5))
        date_strings = [day.isoformat() for day in source_dates]
        interval_start = datetime(2199, 7, 1, tzinfo=UTC)
        interval_end = datetime(2199, 7, 6, tzinfo=UTC)
        definitions = [
            {"id": f"empty_{suffix}_q{index:02d}", "rule": f"empty_{index} = true"}
            for index in range(11)
        ]
        definition_hashes = {
            str(definition["id"]): canonical_sha256(definition) for definition in definitions
        }

        with psycopg.connect(self.database_url, row_factory=dict_row) as connection:
            try:
                version_row = connection.execute(
                    "SELECT current_setting('server_version_num') AS version_num"
                ).fetchone()
                self.assertEqual(int(version_row["version_num"]) // 10000, 18)
                campaign_id = self._campaign_id(connection, suffix=suffix)
                slice_index = self._empty_slice_index(
                    connection,
                    campaign_id=campaign_id,
                    date_strings=date_strings,
                    interval_start=interval_start,
                    interval_end=interval_end,
                )
                before = self._campaign_ledger_snapshot(
                    connection,
                    campaign_id=campaign_id,
                )
                borrowed = _BorrowedConnection(connection)
                with patch(
                    "systematic_fx.db.research_registry.psycopg.connect",
                    return_value=borrowed,
                ):
                    report = verify_phase1a_current_slice_prefix(
                        self.database_url,
                        campaign_key=CAMPAIGN,
                        slice_index=slice_index,
                        source_interval_start=interval_start,
                        source_interval_end=interval_end,
                        requested_source_dates=source_dates,
                        expected_feature_run_fingerprint=None,
                        query_definition_sha256_by_id=definition_hashes,
                    )
                after = self._campaign_ledger_snapshot(
                    connection,
                    campaign_id=campaign_id,
                )

                self.assertEqual(report.slice_index, slice_index)
                self.assertEqual(report.state, "EMPTY")
                self.assertIsNone(report.feature_run_spec_id)
                self.assertIsNone(report.ai_exposure_id)
                self.assertEqual(report.query_exposure_ids, ())
                self.assertEqual(report.pattern_ids, ())
                self.assertIsNone(report.result_artifact_id)
                self.assertIsNone(report.missing_pattern_query_id)
                self.assertEqual(after, before)
            finally:
                connection.rollback()

    def test_failed_feature_only_slice_is_retryable_but_artifact_link_fails_closed(
        self,
    ) -> None:
        suffix = uuid.uuid4().hex
        source_dates = tuple(date(2199, 8, day) for day in (1, 2, 3, 4, 5))
        date_strings = [day.isoformat() for day in source_dates]
        interval_start = datetime(2199, 8, 1, tzinfo=UTC)
        interval_end = datetime(2199, 8, 6, tzinfo=UTC)
        definitions = [
            {"id": f"failed_{suffix}_q{index:02d}", "rule": f"failed_{index} = true"}
            for index in range(11)
        ]
        definition_hashes = {
            str(definition["id"]): canonical_sha256(definition) for definition in definitions
        }

        with psycopg.connect(self.database_url, row_factory=dict_row) as connection:
            try:
                campaign_id = self._campaign_id(connection, suffix=suffix)
                slice_index = self._empty_slice_index(
                    connection,
                    campaign_id=campaign_id,
                    date_strings=date_strings,
                    interval_start=interval_start,
                    interval_end=interval_end,
                )
                feature_spec = self._base_run_spec(
                    run_kind="FEATURE_BUILD",
                    engine_version="phase1a_failed_feature_fixture_v1",
                    parameters={
                        "fixture": suffix,
                        "requested_source_dates": date_strings,
                        "slice_index": slice_index,
                    },
                )
                feature_run_spec_id = self._insert_run_spec(
                    connection,
                    campaign_id=campaign_id,
                    run_spec=feature_spec,
                    parent_run_spec_id=None,
                )
                connection.execute(
                    """
                    INSERT INTO systematic_fx.research_run_attempts
                        (research_run_spec_id, attempt_number, status,
                         result_summary, started_at, finished_at, error_message)
                    VALUES (%s, 1, 'FAILED', '{}'::jsonb, statement_timestamp(),
                            statement_timestamp(), 'fixture feature failure')
                    """,
                    (feature_run_spec_id,),
                )

                before = self._campaign_ledger_snapshot(
                    connection,
                    campaign_id=campaign_id,
                )
                borrowed = _BorrowedConnection(connection)
                with patch(
                    "systematic_fx.db.research_registry.psycopg.connect",
                    return_value=borrowed,
                ):
                    report = verify_phase1a_current_slice_prefix(
                        self.database_url,
                        campaign_key=CAMPAIGN,
                        slice_index=slice_index,
                        source_interval_start=interval_start,
                        source_interval_end=interval_end,
                        requested_source_dates=source_dates,
                        expected_feature_run_fingerprint=feature_spec.fingerprint,
                        query_definition_sha256_by_id=definition_hashes,
                    )
                after = self._campaign_ledger_snapshot(
                    connection,
                    campaign_id=campaign_id,
                )

                self.assertEqual(report.slice_index, slice_index)
                self.assertEqual(report.state, "FAILED_FEATURE_RETRYABLE")
                self.assertIsNone(report.feature_run_spec_id)
                self.assertIsNone(report.ai_exposure_id)
                self.assertEqual(report.query_exposure_ids, ())
                self.assertEqual(report.pattern_ids, ())
                self.assertIsNone(report.result_artifact_id)
                self.assertIsNone(report.missing_pattern_query_id)
                self.assertEqual(after, before)

                artifact_id = connection.execute(
                    """
                    INSERT INTO systematic_fx.artifacts
                        (artifact_key, artifact_type, uri, sha256, byte_size,
                         media_type, metadata)
                    VALUES (%s, 'FAILED_FEATURE_FIXTURE', %s, %s, 1,
                            'application/json', '{}'::jsonb)
                    RETURNING artifact_id
                    """,
                    (
                        f"failed-feature-artifact:{suffix}",
                        f"data/test/failed-feature-artifact/{suffix}.json",
                        _digest(f"failed-feature-artifact:{suffix}"),
                    ),
                ).fetchone()["artifact_id"]
                connection.execute(
                    """
                    INSERT INTO systematic_fx.research_run_attempts
                        (research_run_spec_id, attempt_number, status,
                         result_artifact_id, result_summary, started_at,
                         finished_at, error_message)
                    VALUES (%s, 2, 'FAILED', %s, '{}'::jsonb,
                            statement_timestamp(), statement_timestamp(),
                            'fixture linked feature failure')
                    """,
                    (feature_run_spec_id, artifact_id),
                )

                with (
                    patch(
                        "systematic_fx.db.research_registry.psycopg.connect",
                        return_value=borrowed,
                    ),
                    self.assertRaisesRegex(
                        ResearchRegistryDriftError,
                        "artifact, reuse, or trade-ledger linkage",
                    ),
                ):
                    verify_phase1a_current_slice_prefix(
                        self.database_url,
                        campaign_key=CAMPAIGN,
                        slice_index=slice_index,
                        source_interval_start=interval_start,
                        source_interval_end=interval_end,
                        requested_source_dates=source_dates,
                        expected_feature_run_fingerprint=feature_spec.fingerprint,
                        query_definition_sha256_by_id=definition_hashes,
                    )
            finally:
                connection.rollback()

    def test_exact_postgresql_slice_passes_and_partial_rollup_fails_closed(self) -> None:
        suffix = uuid.uuid4().hex
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        artifact_directory = Path(temporary.name) / "data" / "derived" / "test"
        artifact_directory.mkdir(parents=True)
        source_dates = tuple(date(2099, 1, day) for day in (5, 6, 7, 8, 9))
        date_strings = [day.isoformat() for day in source_dates]
        interval_start = datetime(2099, 1, 5, tzinfo=UTC)
        interval_end = datetime(2099, 1, 10, tzinfo=UTC)
        definitions = [
            {"id": f"fixture_{suffix}_q{index:02d}", "rule": f"feature_{index} >= 1"}
            for index in range(11)
        ]
        definition_hashes = {
            str(definition["id"]): canonical_sha256(definition) for definition in definitions
        }
        query_results = [
            {
                "definition": definition,
                "direction_counts": {"LONG": index, "SHORT": 0},
                "forward": {},
                "occurrences": [{"fixture_occurrence": occurrence} for occurrence in range(index)],
                "source_date_count": min(index, len(source_dates)),
                "support_count": index,
            }
            for index, definition in enumerate(definitions)
        ]

        with psycopg.connect(self.database_url, row_factory=dict_row) as connection:
            try:
                campaign_id = self._campaign_id(connection, suffix=suffix)
                slice_index = self._unused_slice_index(connection, campaign_id=campaign_id)
                feature_artifact_sha256 = _digest(f"{suffix}:feature-artifact")
                feature_spec = self._base_run_spec(
                    run_kind="FEATURE_BUILD",
                    engine_version="phase1a_screening_feature_builder_v1",
                    parameters={
                        "requested_source_dates": date_strings,
                        "slice_index": slice_index,
                    },
                )
                feature_fingerprint = feature_spec.fingerprint
                feature_run_spec_id = self._insert_run_spec(
                    connection,
                    campaign_id=campaign_id,
                    run_spec=feature_spec,
                    parent_run_spec_id=None,
                )
                feature_artifact_id = int(
                    connection.execute(
                        """
                        INSERT INTO systematic_fx.artifacts
                            (artifact_key, artifact_type, uri, sha256, byte_size,
                             media_type, metadata)
                        VALUES (%s, 'PHASE1A_FEATURE_BUILD_MANIFEST', %s, %s, 100,
                                'application/json', '{}'::jsonb)
                        RETURNING artifact_id
                        """,
                        (
                            f"phase1a-feature-fixture:{suffix}",
                            f"data/derived/test/{suffix}-feature.json",
                            feature_artifact_sha256,
                        ),
                    ).fetchone()["artifact_id"]
                )
                ai_parameters = {
                    "analysis_authority": "OPEN_OBSERVATION",
                    "candidate_queries": definitions,
                    "candidate_query_definition_sha256": canonical_sha256(definitions),
                    "feature_inputs_by_date": {
                        day.isoformat(): {
                            "relative_path": f"derived/research_5m/{day.isoformat()}.parquet",
                            "sha256": _digest(f"{suffix}:feature:{day.isoformat()}"),
                        }
                        for day in source_dates
                    },
                    "feature_manifest_relative_path": (f"derived/manifests/{suffix}-feature.json"),
                    "feature_manifest_sha256": feature_artifact_sha256,
                    "frozen_toml_inputs": {
                        "campaign": {"sha256": _digest(f"{suffix}:campaign")},
                        "discovery_query": {"sha256": canonical_sha256(definitions)},
                    },
                    "no_entry_reason_by_date": {},
                    "parent_run_fingerprint": feature_fingerprint,
                    "pipeline_version": "phase1a_discovery_pipeline_v1",
                    "requested_source_dates": date_strings,
                    "research_eligible": False,
                    "screening_only": True,
                    "slice_index": slice_index,
                }
                ai_spec = replace(
                    feature_spec,
                    run_kind="AI_SLICE",
                    engine_version=AI_ENGINE_VERSION,
                    parameters=ai_parameters,
                )
                ai_fingerprint = ai_spec.fingerprint
                ai_exposure_key = f"{CAMPAIGN}:ai-slice:{slice_index:02d}"
                ai_run_spec_id = self._insert_run_spec(
                    connection,
                    campaign_id=campaign_id,
                    run_spec=ai_spec,
                    parent_run_spec_id=feature_run_spec_id,
                )
                artifact_document = {
                    "artifact_schema": "systematic_fx.phase1a_discovery_slice.v1",
                    "artifact_version": "phase1a_discovery_slice_v1",
                    "authority": {
                        "maximum_authority": "OPEN_OBSERVATION",
                        "pass_backtest_allowed": False,
                        "screening_only": True,
                        "screening_survivor_allowed": False,
                    },
                    "code_snapshot_sha256": ai_spec.code_snapshot_sha256,
                    "query_results": query_results,
                    "requested_source_dates": date_strings,
                    "run_fingerprint": ai_fingerprint,
                    "summary": {
                        "candidate_query_count": len(definitions),
                        "nonzero_support_query_count": len(definitions) - 1,
                        "zero_support_query_count": 1,
                    },
                }
                artifact_payload = canonical_json_bytes(artifact_document) + b"\n"
                artifact_sha256 = hashlib.sha256(artifact_payload).hexdigest()
                artifact_path = artifact_directory / f"sha256={artifact_sha256}.json"
                artifact_path.write_bytes(artifact_payload)
                artifact_path.chmod(0o444)
                self.assertEqual(stat.S_IMODE(artifact_path.stat().st_mode), 0o444)
                artifact_relative_path = artifact_path.relative_to(
                    Path(temporary.name) / "data"
                ).as_posix()
                artifact_id = int(
                    connection.execute(
                        """
                        INSERT INTO systematic_fx.artifacts
                            (artifact_key, artifact_type, uri, sha256, byte_size,
                             media_type, metadata)
                        VALUES (%s, 'DISCOVERY_EXPOSURE_RESULT', %s, %s, %s,
                                'application/json', %s)
                        RETURNING artifact_id
                        """,
                        (
                            f"{CAMPAIGN}:discovery-exposure:{ai_exposure_key}:{artifact_sha256}",
                            artifact_path.resolve().as_uri(),
                            artifact_sha256,
                            len(artifact_payload),
                            Jsonb(
                                {
                                    "campaign_key": CAMPAIGN,
                                    "exposure_key": ai_exposure_key,
                                    "exposure_type": "AI_SLICE",
                                    "run_fingerprint": ai_fingerprint,
                                }
                            ),
                        ),
                    ).fetchone()["artifact_id"]
                )

                def insert_success(run_spec_id: int, result_artifact_id: int) -> None:
                    connection.execute(
                        """
                        INSERT INTO systematic_fx.research_run_attempts
                            (research_run_spec_id, attempt_number, status,
                             result_artifact_id, result_summary, finished_at)
                        VALUES (%s, 1, 'SUCCEEDED', %s, '{}'::jsonb,
                                statement_timestamp())
                        """,
                        (run_spec_id, result_artifact_id),
                    )

                insert_success(feature_run_spec_id, feature_artifact_id)
                insert_success(ai_run_spec_id, artifact_id)
                ai_exposure_id = int(
                    connection.execute(
                        """
                        INSERT INTO systematic_fx.discovery_exposures
                            (exposure_key, campaign_id, exposure_type,
                             source_interval_start, source_interval_end,
                             visible_to_ai, research_eligible, query_spec,
                             result_summary, result_artifact_id, code_commit,
                             config_sha256, research_run_spec_id)
                        VALUES (%s, %s, 'AI_SLICE', %s, %s, true, false, %s,
                                %s, %s, 'fixture', %s, %s)
                        RETURNING discovery_exposure_id
                        """,
                        (
                            ai_exposure_key,
                            campaign_id,
                            interval_start,
                            interval_end,
                            Jsonb(
                                {
                                    "candidate_queries": definitions,
                                    "definition_sha256": canonical_sha256(definitions),
                                    "run_fingerprint": ai_fingerprint,
                                }
                            ),
                            Jsonb(
                                {
                                    "candidate_query_count": 11,
                                    "feature_manifest_sha256": feature_artifact_sha256,
                                    "requested_source_dates": date_strings,
                                    "screening_only": True,
                                }
                            ),
                            artifact_id,
                            "f" * 64,
                            ai_run_spec_id,
                        ),
                    ).fetchone()["discovery_exposure_id"]
                )
                query_exposure_ids: list[int] = []
                pattern_ids: list[int] = []
                for index, definition in enumerate(definitions):
                    query_id = str(definition["id"])
                    definition_sha256 = definition_hashes[query_id]
                    query_result = query_results[index]
                    query_exposure_key = f"{CAMPAIGN}:query:{slice_index:02d}:{query_id}"
                    query_parameters = {
                        "candidate_query": definition,
                        "discovery_artifact_relative_path": artifact_relative_path,
                        "discovery_artifact_sha256": artifact_sha256,
                        "frozen_toml_inputs": ai_parameters["frozen_toml_inputs"],
                        "parent_run_fingerprint": ai_fingerprint,
                        "pipeline_version": "phase1a_discovery_pipeline_v1",
                        "query_definition_sha256": definition_sha256,
                        "query_result_sha256": canonical_sha256(query_result),
                        "requested_source_dates": date_strings,
                        "research_eligible": False,
                        "screening_only": True,
                        "slice_index": slice_index,
                    }
                    query_spec = replace(
                        ai_spec,
                        run_kind="QUERY",
                        engine_version=QUERY_ENGINE_VERSION,
                        parameters=query_parameters,
                    )
                    query_fingerprint = query_spec.fingerprint
                    query_run_spec_id = self._insert_run_spec(
                        connection,
                        campaign_id=campaign_id,
                        run_spec=query_spec,
                        parent_run_spec_id=ai_run_spec_id,
                    )
                    insert_success(query_run_spec_id, artifact_id)
                    query_exposure_id = int(
                        connection.execute(
                            """
                            INSERT INTO systematic_fx.discovery_exposures
                                (exposure_key, campaign_id, exposure_type,
                                 source_interval_start, source_interval_end,
                                 visible_to_ai, research_eligible, query_spec,
                                 result_summary, result_artifact_id, code_commit,
                                 config_sha256, research_run_spec_id)
                            VALUES (%s, %s, 'QUERY', %s, %s, true, false, %s,
                                    %s, %s, 'fixture', %s, %s)
                            RETURNING discovery_exposure_id
                            """,
                            (
                                query_exposure_key,
                                campaign_id,
                                interval_start,
                                interval_end,
                                Jsonb(
                                    {
                                        "candidate_query": definition,
                                        "query_definition_sha256": definition_sha256,
                                        "run_fingerprint": query_fingerprint,
                                    }
                                ),
                                Jsonb(
                                    {
                                        "artifact_sha256": artifact_sha256,
                                        "direction_counts": query_result["direction_counts"],
                                        "source_date_count": query_result["source_date_count"],
                                        "support_count": query_result["support_count"],
                                    }
                                ),
                                artifact_id,
                                "f" * 64,
                                query_run_spec_id,
                            ),
                        ).fetchone()["discovery_exposure_id"]
                    )
                    query_exposure_ids.append(query_exposure_id)
                    feature_versions = {
                        "rollup_schema": ROLLUP_SCHEMA,
                        "slice_identities": [
                            {
                                "discovery_exposure_id": query_exposure_id,
                                "feature_identity": {"manifest_sha256": "e" * 64},
                                "query_definition": definition,
                                "query_definition_sha256": definition_sha256,
                                "run_fingerprint": query_fingerprint,
                            }
                        ],
                    }
                    summaries = {
                        "rollup_schema": ROLLUP_SCHEMA,
                        "slice_observations": [
                            {
                                "counterexamples": [],
                                "discovery_exposure_id": query_exposure_id,
                                "exposure_key": query_exposure_key,
                                "forward_first_touch_summary": {"12": {"resolved": index}},
                                "query_definition_sha256": definition_sha256,
                                "research_run_spec_id": query_run_spec_id,
                                "result_artifact_id": artifact_id,
                                "run_fingerprint": query_fingerprint,
                                "source_interval_end": interval_end.isoformat(),
                                "source_interval_start": interval_start.isoformat(),
                                "support_count": index,
                            }
                        ],
                    }
                    pattern_ids.append(
                        int(
                            connection.execute(
                                """
                                INSERT INTO systematic_fx.pattern_ledger
                                    (pattern_key, campaign_id, status, first_seen_from,
                                     first_seen_to, last_updated_interval,
                                     feature_definition_versions, direction,
                                     entry_condition, economic_rationale,
                                     support_count, forward_first_touch_summaries,
                                     cost_assumptions, context_artifact_id)
                                VALUES (%s, %s, 'OPEN', %s, %s, %s, %s, 'BOTH',
                                        'fixture entry', 'fixture rationale', %s, %s,
                                        '{}'::jsonb, %s)
                                RETURNING pattern_id
                                """,
                                (
                                    f"{CAMPAIGN}:{query_id}",
                                    campaign_id,
                                    interval_start,
                                    interval_end,
                                    interval_end,
                                    Jsonb(feature_versions),
                                    index,
                                    Jsonb(summaries),
                                    artifact_id,
                                ),
                            ).fetchone()["pattern_id"]
                        )
                    )

                borrowed = _BorrowedConnection(connection)
                with patch(
                    "systematic_fx.db.research_registry.psycopg.connect",
                    return_value=borrowed,
                ):
                    report = verify_phase1a_predecessor_slice(
                        self.database_url,
                        campaign_key=CAMPAIGN,
                        prior_slice_index=slice_index,
                        source_interval_start=interval_start,
                        source_interval_end=interval_end,
                        requested_source_dates=source_dates,
                        query_definition_sha256_by_id=definition_hashes,
                    )
                    prefix = verify_phase1a_current_slice_prefix(
                        self.database_url,
                        campaign_key=CAMPAIGN,
                        slice_index=slice_index,
                        source_interval_start=interval_start,
                        source_interval_end=interval_end,
                        requested_source_dates=source_dates,
                        expected_feature_run_fingerprint=feature_fingerprint,
                        query_definition_sha256_by_id=definition_hashes,
                    )
                self.assertEqual(report.ai_exposure_id, ai_exposure_id)
                self.assertEqual(report.query_exposure_ids, tuple(query_exposure_ids))
                self.assertEqual(report.pattern_ids, tuple(pattern_ids))
                self.assertEqual(report.result_artifact_id, artifact_id)
                self.assertEqual(prefix.state, "RESUMABLE")
                self.assertEqual(prefix.feature_run_spec_id, feature_run_spec_id)
                self.assertEqual(prefix.query_exposure_ids, tuple(query_exposure_ids))

                connection.execute(
                    """
                    UPDATE systematic_fx.pattern_ledger
                    SET forward_first_touch_summaries = %s
                    WHERE pattern_id = %s
                    """,
                    (
                        Jsonb(
                            {
                                "rollup_schema": ROLLUP_SCHEMA,
                                "slice_observations": [],
                            }
                        ),
                        pattern_ids[0],
                    ),
                )
                with (
                    patch(
                        "systematic_fx.db.research_registry.psycopg.connect",
                        return_value=borrowed,
                    ),
                    self.assertRaises(ResearchRegistryDriftError),
                ):
                    verify_phase1a_predecessor_slice(
                        self.database_url,
                        campaign_key=CAMPAIGN,
                        prior_slice_index=slice_index,
                        source_interval_start=interval_start,
                        source_interval_end=interval_end,
                        requested_source_dates=source_dates,
                        query_definition_sha256_by_id=definition_hashes,
                    )
            finally:
                connection.rollback()


if __name__ == "__main__":
    unittest.main()
