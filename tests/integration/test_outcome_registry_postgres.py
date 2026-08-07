from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
import uuid
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

from systematic_fx.db.migrations import apply_migrations
from systematic_fx.db.outcome_registry import (
    BARRIER_TICKS,
    CAMPAIGN_KEY,
    CHECKPOINT_ARTIFACT_SCHEMA,
    DIRECTION_IDS,
    EXPECTED_DETAIL_RECORD_COUNT,
    EXPECTED_DIRECTION_SIGNAL_COUNTS,
    EXPECTED_FINAL_SOURCE_DATE,
    EXPECTED_PLANNED_SOURCE_DATE_COUNT,
    EXPECTED_SOURCE_OCCURRENCE_COUNT,
    EXPECTED_SOURCE_SLICE_COUNT,
    EXPECTED_SUMMARY_COUNT,
    OUTCOME_ARTIFACT_SCHEMA,
    OUTCOME_CONFIG_ID,
    OUTCOME_ENGINE_VERSION,
    P5_QUERY_ID,
    SCENARIO_COST_TICKS_PER_FILL,
    SCENARIO_IDS,
    OutcomeCellSummary,
    OutcomeRegistryError,
    complete_phase1a_outcome_replay,
    load_latest_phase1a_outcome_checkpoint,
    phase1a_p5_outcome_parameters,
    register_phase1a_outcome_checkpoint,
    reserve_phase1a_outcome_replay,
    start_phase1a_outcome_replay,
    validate_complete_cell_summaries,
)
from systematic_fx.db.run_registry import register_run_spec
from systematic_fx.research.run_spec import RunSpec


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _terminal_resolution() -> dict[str, object]:
    return {
        "contracts": [
            {
                "contract_key": "6E.FUT",
                "eligible_partition_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
                "terminal_event_index": EXPECTED_PLANNED_SOURCE_DATE_COUNT - 1,
                "terminal_source_date": EXPECTED_FINAL_SOURCE_DATE.isoformat(),
                "terminal_ts_recv_ns": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
                "trailing_non_executable_partition_count": 0,
            }
        ],
        "partition_resolution_policy": ("REVERSE_SCAN_LAST_VALID_EXECUTABLE_QUOTE_PARTITION_V1"),
        "terminal_exit_policy": ("LAST_VALID_EXECUTABLE_QUOTE_BEFORE_EXPIRY_MONTH_START"),
    }


def _cell(
    scenario: str,
    direction: str,
    take_profit_ticks: int,
    stop_loss_ticks: int,
) -> OutcomeCellSummary:
    signal_count = EXPECTED_DIRECTION_SIGNAL_COUNTS[direction]
    gross = take_profit_ticks
    variable, fixed = SCENARIO_COST_TICKS_PER_FILL[scenario]
    net = gross - variable - fixed
    return OutcomeCellSummary(
        scenario_id=scenario,
        direction=direction,
        take_profit_ticks=take_profit_ticks,
        stop_loss_ticks=stop_loss_ticks,
        signal_count=signal_count,
        entry_fill_count=1,
        entry_not_filled_count=signal_count - 1,
        skipped_occupied_count=0,
        take_profit_first_count=1,
        stop_first_count=0,
        terminal_exit_count=0,
        censored_count=0,
        gross_pnl_ticks=gross,
        variable_cost_ticks=variable,
        allocated_fixed_cost_ticks=fixed,
        fully_loaded_net_pnl_ticks=net,
        fully_loaded_net_ev_ticks=Decimal(net),
        fully_loaded_net_pnl_usd=Decimal(net) * Decimal("6.25"),
        calendar_month_net_pnl_usd=Decimal(net) * Decimal("6.25"),
        profit_factor=None,
        maximum_drawdown_usd=Decimal(0),
        complete=True,
    )


def _complete_cells() -> tuple[OutcomeCellSummary, ...]:
    return tuple(
        _cell(scenario, direction, take_profit, stop_loss)
        for scenario in SCENARIO_IDS
        for direction in DIRECTION_IDS
        for take_profit in BARRIER_TICKS
        for stop_loss in BARRIER_TICKS
    )


def _publish(data_root: Path, subdirectory: str, document: dict[str, object]) -> Path:
    payload = (
        json.dumps(
            document,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )
    digest = hashlib.sha256(payload).hexdigest()
    path = data_root / "derived" / subdirectory / f"sha256={digest}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(0o444)
    return path


def _publish_blob(
    data_root: Path,
    subdirectory: str,
    payload: bytes,
    *,
    suffix: str,
) -> Path:
    digest = hashlib.sha256(payload).hexdigest()
    path = data_root / "derived" / subdirectory / f"sha256={digest}{suffix}"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(0o444)
    return path


def _completion_input_fixture(
    data_root: Path,
    *,
    nonce: str,
    source_artifact_manifest_sha256: str,
) -> tuple[
    tuple[date, ...],
    dict[str, object],
    dict[str, object],
    dict[str, object],
]:
    dates = tuple(
        EXPECTED_FINAL_SOURCE_DATE - timedelta(days=EXPECTED_PLANNED_SOURCE_DATE_COUNT - sequence)
        for sequence in range(1, EXPECTED_PLANNED_SOURCE_DATE_COUNT + 1)
    )
    cache_plan_sha256 = _digest(f"cache-plan:{nonce}")
    discovery_input_manifest_sha256 = _digest(f"discovery-input:{nonce}")
    entries: list[dict[str, object]] = []
    for event_index, source_date in enumerate(dates):
        cache_sha256 = _digest(f"cache:{nonce}:{source_date.isoformat()}")
        entries.append(
            {
                "artifact_relative_uri": (
                    "backtest_event_cache/phase1a_daily_executable_cache_v1/"
                    f"sha256={cache_sha256}.parquet"
                ),
                "artifact_sha256": cache_sha256,
                "byte_size": 1,
                "cached_quote_count": 1,
                "event_index_offset": event_index,
                "first_event_index": event_index,
                "first_ts_recv_ns": event_index + 1,
                "instrument_id": 1,
                "last_event_index": event_index,
                "last_ts_recv_ns": event_index + 1,
                "last_valid_event_index": event_index,
                "last_valid_ts_recv_ns": event_index + 1,
                "raw_symbol": "6E.FUT",
                "source_date": source_date.isoformat(),
                "source_relative_uri": (f"mbp-10/{source_date.isoformat()}/6E.FUT.parquet"),
                "source_row_count": 1,
                "source_sha256": _digest(f"source:{nonce}:{source_date.isoformat()}"),
                "valid_quote_count": 1,
            }
        )
    entries_sha256 = _canonical_sha256(entries)
    cache_document = {
        "artifact_schema": "systematic_fx.phase1a_outcome_cache_manifest.v1",
        "artifact_version": "phase1a_outcome_cache_manifest_v1",
        "cache_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "cache_entries": entries,
        "cache_entries_sha256": entries_sha256,
        "cache_plan_sha256": cache_plan_sha256,
        "cache_schema": "systematic_fx.phase1a_daily_executable_cache.v1",
        "cache_version": "phase1a_daily_executable_cache_v1",
        "input_manifest_sha256": discovery_input_manifest_sha256,
        "partition_key": ["source_date", "raw_symbol"],
    }
    cache_path = _publish(
        data_root,
        "backtest_event_cache/phase1a_daily_executable_cache_v1/manifests",
        cache_document,
    )
    cache_manifest_sha256 = cache_path.name.removeprefix("sha256=").removesuffix(".json")
    cache_reference = {
        "artifact_relative_uri": cache_path.relative_to(data_root / "derived").as_posix(),
        "artifact_sha256": cache_manifest_sha256,
        "byte_size": cache_path.stat().st_size,
        "cache_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "cache_entries_sha256": entries_sha256,
        "cache_plan_sha256": cache_plan_sha256,
        "input_manifest_sha256": discovery_input_manifest_sha256,
    }
    input_lineage = {
        "cache_plan_sha256": cache_plan_sha256,
        "calendar_sha256": _digest(f"calendar:{nonce}"),
        "discovery_input_manifest_sha256": discovery_input_manifest_sha256,
        "expected_completed_source_date_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "expected_last_completed_source_date": EXPECTED_FINAL_SOURCE_DATE.isoformat(),
        "footer_manifest_sha256": _digest(f"footers:{nonce}"),
        "input_plan_sha256": _digest(f"input-plan:{nonce}"),
        "portable_artifact_manifest_sha256": _digest(f"portable-artifacts:{nonce}"),
        "rich_source_artifact_manifest_sha256": source_artifact_manifest_sha256,
        "signal_manifest_sha256": _digest(f"signals:{nonce}"),
        "source_hash_manifest_sha256": _digest(f"source-hashes:{nonce}"),
        "source_record_manifest_sha256": _digest(f"source-records:{nonce}"),
        "split_sha256": _digest(f"split:{nonce}"),
        "terminal_resolution_sha256": _canonical_sha256(_terminal_resolution()),
    }
    completion_lineage = {
        "cache_manifest_sha256": cache_manifest_sha256,
        "cache_partition_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "input_plan_sha256": input_lineage["input_plan_sha256"],
        "portable_discovery_artifact_manifest_sha256": input_lineage[
            "portable_artifact_manifest_sha256"
        ],
        "portable_discovery_input_manifest_sha256": discovery_input_manifest_sha256,
        "portable_signal_manifest_sha256": input_lineage["signal_manifest_sha256"],
        "source_record_manifest_sha256": input_lineage["source_record_manifest_sha256"],
        "terminal_resolution": _terminal_resolution(),
        "terminal_resolution_sha256": input_lineage["terminal_resolution_sha256"],
    }
    return dates, cache_reference, input_lineage, completion_lineage


def _detail_fixture(
    data_root: Path,
    *,
    nonce: str,
    run_fingerprint: str,
    dates: tuple[date, ...],
) -> tuple[list[dict[str, object]], str]:
    base_rows, remainder = divmod(EXPECTED_DETAIL_RECORD_COUNT, len(dates))
    shards: list[dict[str, object]] = []
    for sequence, source_date in enumerate(dates, start=1):
        path = _publish_blob(
            data_root,
            "outcomes/phase1a_p5_outcome_replay_v1/detail_shards",
            f"PARQUET-FIXTURE:{nonce}:{sequence}".encode("ascii"),
            suffix=".parquet",
        )
        artifact_sha256 = path.name.removeprefix("sha256=").removesuffix(".parquet")
        shards.append(
            {
                "artifact_relative_uri": path.relative_to(data_root / "derived").as_posix(),
                "artifact_sha256": artifact_sha256,
                "byte_size": path.stat().st_size,
                "record_manifest_sha256": _digest(f"detail-records:{nonce}:{sequence}"),
                "row_count": base_rows + (1 if sequence <= remainder else 0),
                "run_fingerprint": run_fingerprint,
                "shard_sequence": sequence,
                "source_date": source_date.isoformat(),
            }
        )
    return shards, _canonical_sha256(shards)


class OutcomeRegistryPostgreSQLIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        database_url = os.environ.get("SYSTEMATIC_FX_TEST_DATABASE_URL")
        if not database_url:
            raise unittest.SkipTest("SYSTEMATIC_FX_TEST_DATABASE_URL is not set")
        cls.database_url = database_url
        apply_migrations(
            database_url,
            psql_binary=os.environ.get("SYSTEMATIC_FX_PSQL"),
        )
        cls.temporary_directory = tempfile.TemporaryDirectory()
        cls.data_root = Path(cls.temporary_directory.name).resolve() / "data"
        (cls.data_root / "derived").mkdir(parents=True)

        with psycopg.connect(database_url) as connection, connection.transaction():
            campaign = connection.execute(
                "SELECT campaign_id FROM systematic_fx.campaigns WHERE campaign_key = %s",
                (CAMPAIGN_KEY,),
            ).fetchone()
            if campaign is None:
                suffix = uuid.uuid4().hex
                dataset_id = connection.execute(
                    """
                    INSERT INTO systematic_fx.datasets
                        (dataset_key, provider, feed, data_schema, root_uri,
                         status, manifest_sha256)
                    VALUES (%s, 'test', 'test', 'mbp-10', %s, 'VALIDATING', %s)
                    RETURNING dataset_id
                    """,
                    (
                        f"outcome-registry-dataset-{suffix}",
                        f"data/test/outcome-registry/{suffix}",
                        _digest(f"dataset:{suffix}"),
                    ),
                ).fetchone()[0]
                connection.execute(
                    """
                    INSERT INTO systematic_fx.campaigns
                        (campaign_key, dataset_id, name, status,
                         data_manifest_sha256, feature_version, outcome_version,
                         cost_model_version, execution_model_version, code_commit,
                         config_sha256, split_policy, trial_budget, finalist_budget)
                    VALUES (%s, %s, 'Phase 1A outcome registry fixture', 'DRAFT',
                            %s, 'phase1a_mbp10_screening_v1', 'outcome-v1',
                            'cost-v1', 'execution-v1', %s, %s, '{}'::jsonb, 10, 1)
                    """,
                    (
                        CAMPAIGN_KEY,
                        dataset_id,
                        _digest(f"campaign-data:{suffix}"),
                        "1" * 40,
                        _digest(f"campaign-config:{suffix}"),
                    ),
                )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary_directory.cleanup()

    def _run_spec(
        self,
        *,
        source_sha256: str,
        nonce: str,
        completion_lineage: Mapping[str, object] | None = None,
    ) -> RunSpec:
        lineage = {
            "cache_manifest_sha256": _digest(f"cache-manifest:{nonce}"),
            "cache_partition_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
            "input_plan_sha256": _digest(f"input-plan:{nonce}"),
            "portable_discovery_artifact_manifest_sha256": _digest(f"portable-artifacts:{nonce}"),
            "portable_discovery_input_manifest_sha256": _digest(f"portable-inputs:{nonce}"),
            "portable_signal_manifest_sha256": _digest(f"signals:{nonce}"),
            "source_record_manifest_sha256": _digest(f"source-records:{nonce}"),
            "terminal_resolution": _terminal_resolution(),
            "terminal_resolution_sha256": _canonical_sha256(_terminal_resolution()),
        }
        if completion_lineage is not None:
            lineage.update(completion_lineage)
        return RunSpec(
            campaign_id=CAMPAIGN_KEY,
            experiment_id=None,
            run_kind="OUTCOME_BUILD",
            engine_version=OUTCOME_ENGINE_VERSION,
            source_manifest_hashes={"phase1a_ai_slices": source_sha256},
            eligible_calendar_version="phase1a_calendar_v1",
            eligible_calendar_sha256="1" * 64,
            split_version="phase1a_split_v1",
            split_sha256="2" * 64,
            feature_version="phase1a_mbp10_screening_v1",
            feature_sha256="3" * 64,
            outcome_version=OUTCOME_CONFIG_ID,
            outcome_sha256="4" * 64,
            cost_version="phase1a_conservative_cost_v1",
            cost_sha256="5" * 64,
            execution_version="phase1a_conservative_execution_v1",
            execution_sha256="6" * 64,
            code_commit="7" * 40,
            code_snapshot_sha256="8" * 64,
            dependency_lock_sha256="9" * 64,
            runtime_environment={"postgresql": "18.4", "python": "3.12"},
            random_seed=int(nonce[:16], 16),
            direction="BOTH",
            signal_policy={"query_id": P5_QUERY_ID},
            entry_policy={"type": "MARKETABLE_LIMIT_IOC"},
            barrier_policy={"ticks": list(BARRIER_TICKS)},
            terminal_policy={
                "terminal_exit": ("LAST_VALID_EXECUTABLE_QUOTE_BEFORE_EXPIRY_MONTH_START"),
                "terminal_partition_resolution": (
                    "REVERSE_SCAN_LAST_VALID_EXECUTABLE_QUOTE_PARTITION_V1"
                ),
                "terminal_resolution_sha256": lineage["terminal_resolution_sha256"],
            },
            parameters={
                **phase1a_p5_outcome_parameters(source_sha256),
                "expected_completed_source_date_count": (EXPECTED_PLANNED_SOURCE_DATE_COUNT),
                "expected_last_completed_source_date": (EXPECTED_FINAL_SOURCE_DATE.isoformat()),
                "fixture_nonce": nonce,
                **lineage,
            },
        )

    def test_full_lifecycle_is_atomic_idempotent_and_append_only(self) -> None:
        nonce = uuid.uuid4().hex
        source_sha256 = _digest(f"source:{nonce}")
        dates, cache_reference, input_lineage, completion_lineage = _completion_input_fixture(
            self.data_root,
            nonce=nonce,
            source_artifact_manifest_sha256=source_sha256,
        )
        run_spec = self._run_spec(
            source_sha256=source_sha256,
            nonce=nonce,
            completion_lineage=completion_lineage,
        )
        register_run_spec(self.database_url, run_spec)
        detail_shards, detail_shard_manifest_sha256 = _detail_fixture(
            self.data_root,
            nonce=nonce,
            run_fingerprint=run_spec.fingerprint,
            dates=dates,
        )
        input_lineage_sha256 = _canonical_sha256(input_lineage)

        reservation = reserve_phase1a_outcome_replay(
            self.database_url,
            run_fingerprint=run_spec.fingerprint,
            source_artifact_manifest_sha256=source_sha256,
        )
        self.assertTrue(reservation.execute)
        self.assertTrue(reservation.created_manifest)
        running = start_phase1a_outcome_replay(
            self.database_url,
            outcome_replay_manifest_id=reservation.outcome_replay_manifest_id,
            run_fingerprint=run_spec.fingerprint,
        )
        self.assertEqual(running.status, "RUNNING")
        self.assertIsNone(
            load_latest_phase1a_outcome_checkpoint(
                self.database_url,
                outcome_replay_manifest_id=reservation.outcome_replay_manifest_id,
                run_fingerprint=run_spec.fingerprint,
                data_root=self.data_root,
            )
        )

        progress_metadata = {"checkpoint_kind": "SOURCE_DATE_COMPLETE"}
        progress_sha256 = _canonical_sha256(progress_metadata)
        checkpoint_path = _publish(
            self.data_root,
            "outcomes/checkpoints/phase1a_p5_outcome_replay_v1",
            {
                "artifact_schema": CHECKPOINT_ARTIFACT_SCHEMA,
                "checkpoint_sequence": 1,
                "completed_source_date_count": 1,
                "last_completed_source_date": dates[0].isoformat(),
                "outcome_config_id": OUTCOME_CONFIG_ID,
                "outcome_replay_manifest_id": reservation.outcome_replay_manifest_id,
                "predecessor_checkpoint_sha256": None,
                "progress_metadata_sha256": progress_sha256,
                "query_id": P5_QUERY_ID,
                "replay_state": {"next_source_date_index": 1},
                "run_fingerprint": run_spec.fingerprint,
                "source_event_count": 1,
            },
        )
        checkpoint = register_phase1a_outcome_checkpoint(
            self.database_url,
            outcome_replay_manifest_id=reservation.outcome_replay_manifest_id,
            run_fingerprint=run_spec.fingerprint,
            checkpoint_sequence=1,
            completed_source_date_count=1,
            last_completed_source_date=dates[0],
            source_event_count=1,
            predecessor_checkpoint_sha256=None,
            progress_metadata=progress_metadata,
            checkpoint_artifact_path=checkpoint_path,
            data_root=self.data_root,
        )
        repeated_checkpoint = register_phase1a_outcome_checkpoint(
            self.database_url,
            outcome_replay_manifest_id=reservation.outcome_replay_manifest_id,
            run_fingerprint=run_spec.fingerprint,
            checkpoint_sequence=1,
            completed_source_date_count=1,
            last_completed_source_date=dates[0],
            source_event_count=1,
            predecessor_checkpoint_sha256=None,
            progress_metadata=progress_metadata,
            checkpoint_artifact_path=checkpoint_path,
            data_root=self.data_root,
        )
        self.assertTrue(checkpoint.created)
        self.assertFalse(repeated_checkpoint.created)
        self.assertEqual(
            checkpoint.checkpoint_artifact_id,
            repeated_checkpoint.checkpoint_artifact_id,
        )
        loaded_checkpoint = load_latest_phase1a_outcome_checkpoint(
            self.database_url,
            outcome_replay_manifest_id=reservation.outcome_replay_manifest_id,
            run_fingerprint=run_spec.fingerprint,
            data_root=self.data_root,
        )
        self.assertIsNotNone(loaded_checkpoint)
        assert loaded_checkpoint is not None
        self.assertEqual(loaded_checkpoint.checkpoint_sequence, 1)
        self.assertEqual(loaded_checkpoint.completed_source_date_count, 1)
        self.assertEqual(loaded_checkpoint.last_completed_source_date, dates[0])
        self.assertEqual(loaded_checkpoint.source_event_count, 1)
        self.assertEqual(
            loaded_checkpoint.checkpoint_artifact_id,
            checkpoint.checkpoint_artifact_id,
        )
        self.assertEqual(
            loaded_checkpoint.checkpoint_artifact_uri,
            checkpoint_path.as_uri(),
        )
        self.assertEqual(loaded_checkpoint.progress_metadata, progress_metadata)
        self.assertEqual(
            loaded_checkpoint.checkpoint_document["replay_state"],
            {"next_source_date_index": 1},
        )
        checkpoint_path.chmod(0o644)
        try:
            with self.assertRaisesRegex(OutcomeRegistryError, "read-only"):
                load_latest_phase1a_outcome_checkpoint(
                    self.database_url,
                    outcome_replay_manifest_id=reservation.outcome_replay_manifest_id,
                    run_fingerprint=run_spec.fingerprint,
                    data_root=self.data_root,
                )
        finally:
            checkpoint_path.chmod(0o444)

        predecessor_sha256 = checkpoint.checkpoint_artifact_sha256
        for sequence, source_date in enumerate(dates[1:-1], start=2):
            checkpoint_path = _publish(
                self.data_root,
                "outcomes/checkpoints/phase1a_p5_outcome_replay_v1",
                {
                    "artifact_schema": CHECKPOINT_ARTIFACT_SCHEMA,
                    "checkpoint_sequence": sequence,
                    "completed_source_date_count": sequence,
                    "last_completed_source_date": source_date.isoformat(),
                    "outcome_config_id": OUTCOME_CONFIG_ID,
                    "outcome_replay_manifest_id": (reservation.outcome_replay_manifest_id),
                    "predecessor_checkpoint_sha256": predecessor_sha256,
                    "progress_metadata_sha256": progress_sha256,
                    "query_id": P5_QUERY_ID,
                    "replay_state": {"next_source_date_index": sequence},
                    "run_fingerprint": run_spec.fingerprint,
                    "source_event_count": sequence,
                },
            )
            intermediate = register_phase1a_outcome_checkpoint(
                self.database_url,
                outcome_replay_manifest_id=reservation.outcome_replay_manifest_id,
                run_fingerprint=run_spec.fingerprint,
                checkpoint_sequence=sequence,
                completed_source_date_count=sequence,
                last_completed_source_date=source_date,
                source_event_count=sequence,
                predecessor_checkpoint_sha256=predecessor_sha256,
                progress_metadata=progress_metadata,
                checkpoint_artifact_path=checkpoint_path,
                data_root=self.data_root,
            )
            predecessor_sha256 = intermediate.checkpoint_artifact_sha256

        final_replay_state = {
            "buffer": [],
            "completed_source_date": dates[-1].isoformat(),
            "drained_record_count": EXPECTED_DETAIL_RECORD_COUNT,
            "finished": True,
            "occupancy": [],
            "pending_entries": [],
            "position_groups": [],
            "records": [],
            "result_record_count": EXPECTED_DETAIL_RECORD_COUNT,
            "signal_cursor": EXPECTED_SOURCE_OCCURRENCE_COUNT,
            "signals": [{} for _ in range(EXPECTED_SOURCE_OCCURRENCE_COUNT)],
            "source_event_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        }
        replay_state_sha256 = _canonical_sha256(final_replay_state)
        final_progress = {
            "artifact_schema": "systematic_fx.phase1a_outcome_progress.v1",
            "cache_manifest_sha256": cache_reference["artifact_sha256"],
            "detail_record_count": EXPECTED_DETAIL_RECORD_COUNT,
            "detail_shard_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
            "detail_shard_manifest_sha256": detail_shard_manifest_sha256,
            "input_lineage_sha256": input_lineage_sha256,
            "replay_finished": True,
            "replay_state_sha256": replay_state_sha256,
            "source_event_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        }
        final_checkpoint_path = _publish(
            self.data_root,
            "outcomes/checkpoints/phase1a_p5_outcome_replay_v1",
            {
                "artifact_schema": CHECKPOINT_ARTIFACT_SCHEMA,
                "cache_manifest": cache_reference,
                "checkpoint_sequence": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
                "completed_source_date_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
                "detail_record_count": EXPECTED_DETAIL_RECORD_COUNT,
                "detail_shard_manifest_sha256": detail_shard_manifest_sha256,
                "detail_shards": detail_shards,
                "input_lineage": input_lineage,
                "input_lineage_sha256": input_lineage_sha256,
                "last_completed_source_date": dates[-1].isoformat(),
                "outcome_config_id": OUTCOME_CONFIG_ID,
                "outcome_replay_manifest_id": reservation.outcome_replay_manifest_id,
                "predecessor_checkpoint_sha256": predecessor_sha256,
                "progress_metadata": final_progress,
                "progress_metadata_sha256": _canonical_sha256(final_progress),
                "query_id": P5_QUERY_ID,
                "replay_state": final_replay_state,
                "replay_state_sha256": replay_state_sha256,
                "run_fingerprint": run_spec.fingerprint,
                "source_event_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
            },
        )
        final_checkpoint = register_phase1a_outcome_checkpoint(
            self.database_url,
            outcome_replay_manifest_id=reservation.outcome_replay_manifest_id,
            run_fingerprint=run_spec.fingerprint,
            checkpoint_sequence=EXPECTED_PLANNED_SOURCE_DATE_COUNT,
            completed_source_date_count=EXPECTED_PLANNED_SOURCE_DATE_COUNT,
            last_completed_source_date=dates[-1],
            source_event_count=EXPECTED_PLANNED_SOURCE_DATE_COUNT,
            predecessor_checkpoint_sha256=predecessor_sha256,
            progress_metadata=final_progress,
            checkpoint_artifact_path=final_checkpoint_path,
            data_root=self.data_root,
        )

        cells, cells_sha256 = validate_complete_cell_summaries(_complete_cells())
        final_checkpoint_reference = {
            "artifact_relative_uri": final_checkpoint_path.relative_to(
                self.data_root / "derived"
            ).as_posix(),
            "artifact_sha256": final_checkpoint.checkpoint_artifact_sha256,
            "byte_size": final_checkpoint.checkpoint_artifact_byte_size,
            "checkpoint_sequence": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
            "last_completed_source_date": dates[-1].isoformat(),
            "progress_metadata": final_progress,
            "progress_metadata_sha256": _canonical_sha256(final_progress),
        }
        result_path = _publish(
            self.data_root,
            "outcomes/phase1a_p5_outcome_replay_v1",
            {
                "artifact_schema": OUTCOME_ARTIFACT_SCHEMA,
                "cache_manifest": cache_reference,
                "cell_summaries": [cell.payload for cell in cells],
                "cell_summaries_sha256": cells_sha256,
                "detail_record_count": EXPECTED_DETAIL_RECORD_COUNT,
                "detail_shard_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
                "detail_shard_manifest_sha256": detail_shard_manifest_sha256,
                "detail_shards": detail_shards,
                "direction_ids": list(DIRECTION_IDS),
                "final_checkpoint": final_checkpoint_reference,
                "input_lineage": input_lineage,
                "input_lineage_sha256": input_lineage_sha256,
                "outcome_config_id": OUTCOME_CONFIG_ID,
                "query_id": P5_QUERY_ID,
                "run_fingerprint": run_spec.fingerprint,
                "scenario_ids": list(SCENARIO_IDS),
                "source_artifact_manifest_sha256": source_sha256,
                "source_occurrence_count": EXPECTED_SOURCE_OCCURRENCE_COUNT,
                "source_slice_count": EXPECTED_SOURCE_SLICE_COUNT,
                "summary_row_count": EXPECTED_SUMMARY_COUNT,
            },
        )
        completed = complete_phase1a_outcome_replay(
            self.database_url,
            outcome_replay_manifest_id=reservation.outcome_replay_manifest_id,
            run_fingerprint=run_spec.fingerprint,
            cell_summaries=cells,
            result_artifact_path=result_path,
            data_root=self.data_root,
        )
        repeated = complete_phase1a_outcome_replay(
            self.database_url,
            outcome_replay_manifest_id=reservation.outcome_replay_manifest_id,
            run_fingerprint=run_spec.fingerprint,
            cell_summaries=cells,
            result_artifact_path=result_path,
            data_root=self.data_root,
        )
        duplicate = reserve_phase1a_outcome_replay(
            self.database_url,
            run_fingerprint=run_spec.fingerprint,
            source_artifact_manifest_sha256=source_sha256,
        )

        self.assertTrue(completed.completed)
        self.assertFalse(repeated.completed)
        self.assertEqual(completed.result_artifact_id, repeated.result_artifact_id)
        self.assertFalse(duplicate.execute)
        self.assertEqual(duplicate.attempt_status, "SKIPPED_DUPLICATE")
        with psycopg.connect(self.database_url) as connection:
            row = connection.execute(
                """
                SELECT manifest.status, attempt.status,
                       (SELECT count(*)
                        FROM systematic_fx.phase1a_outcome_cell_summaries AS cell
                        WHERE cell.outcome_replay_manifest_id =
                              manifest.outcome_replay_manifest_id) AS cell_count
                FROM systematic_fx.phase1a_outcome_replay_manifests AS manifest
                JOIN systematic_fx.research_run_attempts AS attempt
                  ON attempt.research_run_attempt_id = manifest.research_run_attempt_id
                WHERE manifest.outcome_replay_manifest_id = %s
                """,
                (reservation.outcome_replay_manifest_id,),
            ).fetchone()
            self.assertEqual(row, ("SUCCEEDED", "SUCCEEDED", EXPECTED_SUMMARY_COUNT))

        with psycopg.connect(self.database_url) as connection:  # noqa: SIM117
            with self.assertRaises(psycopg.errors.RaiseException):
                connection.execute(
                    "UPDATE systematic_fx.phase1a_outcome_replay_checkpoints "
                    "SET source_event_count = source_event_count + 1 "
                    "WHERE outcome_replay_manifest_id = %s AND checkpoint_sequence = 1",
                    (reservation.outcome_replay_manifest_id,),
                )

    def test_database_rejects_success_without_complete_replay_evidence(self) -> None:
        nonce = uuid.uuid4().hex
        source_sha256 = _digest(f"source:{nonce}")
        run_spec = self._run_spec(source_sha256=source_sha256, nonce=nonce)
        register_run_spec(self.database_url, run_spec)
        reservation = reserve_phase1a_outcome_replay(
            self.database_url,
            run_fingerprint=run_spec.fingerprint,
            source_artifact_manifest_sha256=source_sha256,
        )
        start_phase1a_outcome_replay(
            self.database_url,
            outcome_replay_manifest_id=reservation.outcome_replay_manifest_id,
            run_fingerprint=run_spec.fingerprint,
        )

        artifact_sha256 = _digest(f"partial-artifact:{nonce}")
        cells_sha256 = _digest(f"partial-cells:{nonce}")
        finished_at = datetime.now(UTC)
        with psycopg.connect(self.database_url) as connection:  # noqa: SIM117
            with (
                self.assertRaisesRegex(
                    psycopg.errors.RaiseException,
                    "result completion lineage drift",
                ),
                connection.transaction(),
            ):
                artifact_id = connection.execute(
                    """
                    INSERT INTO systematic_fx.artifacts
                        (artifact_key, artifact_type, uri, sha256, byte_size,
                         media_type, metadata)
                    VALUES (%s, 'PHASE1A_OUTCOME_REPLAY_RESULT', %s, %s, 1,
                            'application/json', %s)
                    RETURNING artifact_id
                    """,
                    (
                        f"partial-outcome:{nonce}",
                        (f"file://{self.data_root}/derived/partial/sha256={artifact_sha256}.json"),
                        artifact_sha256,
                        Jsonb(
                            {
                                "campaign_key": CAMPAIGN_KEY,
                                "cell_summaries_sha256": cells_sha256,
                                "outcome_config_id": OUTCOME_CONFIG_ID,
                                "query_id": P5_QUERY_ID,
                                "run_fingerprint": run_spec.fingerprint,
                                "source_artifact_manifest_sha256": source_sha256,
                                "summary_row_count": EXPECTED_SUMMARY_COUNT,
                            }
                        ),
                    ),
                ).fetchone()[0]
                result_summary = {
                    "artifact_sha256": artifact_sha256,
                    "cell_summaries_sha256": cells_sha256,
                    "source_artifact_manifest_sha256": source_sha256,
                    "summary_row_count": EXPECTED_SUMMARY_COUNT,
                }
                connection.execute(
                    """
                    UPDATE systematic_fx.research_run_attempts
                    SET status = 'SUCCEEDED', result_artifact_id = %s,
                        result_summary = %s, finished_at = %s
                    WHERE research_run_attempt_id = %s
                    """,
                    (
                        artifact_id,
                        Jsonb(result_summary),
                        finished_at,
                        reservation.research_run_attempt_id,
                    ),
                )
                connection.execute(
                    """
                    UPDATE systematic_fx.phase1a_outcome_replay_manifests
                    SET status = 'SUCCEEDED', result_artifact_id = %s,
                        result_artifact_sha256 = %s,
                        result_artifact_byte_size = 1,
                        cell_summaries_sha256 = %s, finished_at = %s
                    WHERE outcome_replay_manifest_id = %s
                    """,
                    (
                        artifact_id,
                        artifact_sha256,
                        cells_sha256,
                        finished_at,
                        reservation.outcome_replay_manifest_id,
                    ),
                )

        with psycopg.connect(self.database_url) as connection:
            states = connection.execute(
                """
                SELECT manifest.status, attempt.status
                FROM systematic_fx.phase1a_outcome_replay_manifests AS manifest
                JOIN systematic_fx.research_run_attempts AS attempt
                  ON attempt.research_run_attempt_id = manifest.research_run_attempt_id
                WHERE manifest.outcome_replay_manifest_id = %s
                """,
                (reservation.outcome_replay_manifest_id,),
            ).fetchone()
        self.assertEqual(states, ("RUNNING", "RUNNING"))


if __name__ == "__main__":
    unittest.main()
