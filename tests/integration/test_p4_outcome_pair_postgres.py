from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.config.settings import Settings
from systematic_fx.db.migrations import apply_migrations
from systematic_fx.db.outcome_registry import (
    BARRIER_TICKS,
    CAMPAIGN_KEY,
    CHECKPOINT_ARTIFACT_SCHEMA,
    DIRECTION_IDS,
    EXPECTED_SUMMARY_COUNT,
    OUTCOME_ARTIFACT_TYPE,
    OUTCOME_ENGINE_VERSION,
    P4_01_QUERY_ID,
    P4_02_QUERY_ID,
    P4_PAIR_CONFIG_SHA256,
    P4_PAIR_ECONOMIC_CELL_COUNT,
    P4_PAIR_ID,
    P4_PAIR_PRIOR_LINEAGE,
    P4_PAIR_PRIOR_LINEAGE_SHA256,
    PHASE1A_CUMULATIVE_ECONOMIC_CELL_COUNT,
    SCENARIO_COST_TICKS_PER_FILL,
    SCENARIO_IDS,
    OutcomeCellSummary,
    OutcomeRegistryError,
    P4OutcomePairMember,
    _canonical_json_bytes,
    _canonical_sha256,
    _cell_insert_parameters,
    _ensure_result_artifact,
    _load_manifest_for_update,
    _open_held_immutable_file,
    _p4_pair_release_payload,
    _register_cells,
    _register_screening_decisions,
    _validate_final_checkpoint,
    _validate_p4_prior_outcome_lineage,
    _validate_result_artifact,
    _validate_run_spec_completion_lineage,
    complete_phase1a_outcome_replay,
    complete_phase1a_p4_outcome_pair,
    derive_phase1a_outcome_screening_decisions,
    fail_phase1a_outcome_replay,
    fail_phase1a_p4_outcome_pair,
    fail_unpaired_phase1a_p4_outcome_replay,
    load_phase1a_p4_outcome_pair_release,
    outcome_query_profile,
    phase1a_outcome_parameters,
    register_phase1a_outcome_checkpoint,
    reserve_phase1a_outcome_replay,
    reserve_phase1a_p4_outcome_pair,
    start_phase1a_outcome_replay,
    validate_complete_cell_summaries,
)
from systematic_fx.db.run_registry import register_run_spec
from systematic_fx.research.run_spec import RunSpec


def digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def publish(data_root: Path, relative_parent: str, document: object) -> Path:
    payload = _canonical_json_bytes(document) + b"\n"
    sha256 = hashlib.sha256(payload).hexdigest()
    path = data_root / "derived" / relative_parent / f"sha256={sha256}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(0o444)
    return path


def publish_blob(
    data_root: Path,
    relative_parent: str,
    payload: bytes,
    *,
    suffix: str,
) -> Path:
    sha256 = hashlib.sha256(payload).hexdigest()
    path = data_root / "derived" / relative_parent / f"sha256={sha256}{suffix}"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(0o444)
    return path


def terminal_resolution() -> dict[str, object]:
    return {
        "partition_resolution_policy": ("REVERSE_SCAN_LAST_VALID_EXECUTABLE_QUOTE_PARTITION_V1"),
        "terminal_exit_policy": "LAST_VALID_EXECUTABLE_QUOTE_BEFORE_EXPIRY_MONTH_START",
    }


def build_cells(query_id: str) -> tuple[tuple[OutcomeCellSummary, ...], str]:
    profile = outcome_query_profile(query_id)
    cells = tuple(
        OutcomeCellSummary(
            scenario_id=scenario,
            direction=direction,
            take_profit_ticks=take_profit,
            stop_loss_ticks=stop_loss,
            signal_count=profile.direction_signal_counts[direction],
            entry_fill_count=1,
            entry_not_filled_count=(profile.direction_signal_counts[direction] - 1),
            skipped_occupied_count=0,
            take_profit_first_count=1,
            stop_first_count=0,
            terminal_exit_count=0,
            censored_count=0,
            gross_pnl_ticks=take_profit,
            variable_cost_ticks=SCENARIO_COST_TICKS_PER_FILL[scenario][0],
            allocated_fixed_cost_ticks=SCENARIO_COST_TICKS_PER_FILL[scenario][1],
            fully_loaded_net_pnl_ticks=(
                take_profit
                - SCENARIO_COST_TICKS_PER_FILL[scenario][0]
                - SCENARIO_COST_TICKS_PER_FILL[scenario][1]
            ),
            fully_loaded_net_ev_ticks=Decimal(
                take_profit
                - SCENARIO_COST_TICKS_PER_FILL[scenario][0]
                - SCENARIO_COST_TICKS_PER_FILL[scenario][1]
            ),
            fully_loaded_net_pnl_usd=(
                Decimal(
                    take_profit
                    - SCENARIO_COST_TICKS_PER_FILL[scenario][0]
                    - SCENARIO_COST_TICKS_PER_FILL[scenario][1]
                )
                * Decimal("6.25")
            ),
            calendar_month_net_pnl_usd=(
                Decimal(
                    take_profit
                    - SCENARIO_COST_TICKS_PER_FILL[scenario][0]
                    - SCENARIO_COST_TICKS_PER_FILL[scenario][1]
                )
                * Decimal("6.25")
            ),
            profit_factor=None,
            maximum_drawdown_usd=Decimal(0),
            complete=True,
        )
        for scenario in SCENARIO_IDS
        for direction in DIRECTION_IDS
        for take_profit in BARRIER_TICKS
        for stop_loss in BARRIER_TICKS
    )
    return validate_complete_cell_summaries(cells, query_id=query_id)


def build_input_fixture(
    data_root: Path,
    *,
    query_id: str,
    source_sha256: str,
    nonce: str,
) -> dict[str, Any]:
    profile = outcome_query_profile(query_id)
    dates = tuple(
        profile.final_source_date - timedelta(days=profile.planned_source_date_count - sequence)
        for sequence in range(1, profile.planned_source_date_count + 1)
    )
    cache_plan_sha256 = digest(f"cache-plan:{nonce}")
    input_manifest_sha256 = digest(f"discovery-input:{nonce}")
    entries: list[dict[str, object]] = []
    for event_index, source_date in enumerate(dates):
        cache_sha256 = digest(f"cache:{nonce}:{source_date.isoformat()}")
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
                "source_sha256": digest(f"source:{nonce}:{source_date.isoformat()}"),
                "valid_quote_count": 1,
            }
        )
    entries_sha256 = _canonical_sha256(entries)
    cache_path = publish(
        data_root,
        "backtest_event_cache/phase1a_daily_executable_cache_v1/manifests",
        {
            "artifact_schema": "systematic_fx.phase1a_outcome_cache_manifest.v1",
            "artifact_version": "phase1a_outcome_cache_manifest_v1",
            "cache_count": profile.cache_partition_count,
            "cache_entries": entries,
            "cache_entries_sha256": entries_sha256,
            "cache_plan_sha256": cache_plan_sha256,
            "cache_schema": "systematic_fx.phase1a_daily_executable_cache.v1",
            "cache_version": "phase1a_daily_executable_cache_v1",
            "input_manifest_sha256": input_manifest_sha256,
            "partition_key": ["source_date", "raw_symbol"],
        },
    )
    cache_sha256 = cache_path.name.removeprefix("sha256=").removesuffix(".json")
    cache_reference = {
        "artifact_relative_uri": cache_path.relative_to(data_root / "derived").as_posix(),
        "artifact_sha256": cache_sha256,
        "byte_size": cache_path.stat().st_size,
        "cache_count": profile.cache_partition_count,
        "cache_entries_sha256": entries_sha256,
        "cache_plan_sha256": cache_plan_sha256,
        "input_manifest_sha256": input_manifest_sha256,
    }
    resolution = terminal_resolution()
    input_lineage = {
        "cache_plan_sha256": cache_plan_sha256,
        "calendar_sha256": digest(f"calendar:{nonce}"),
        "discovery_input_manifest_sha256": input_manifest_sha256,
        "expected_completed_source_date_count": profile.planned_source_date_count,
        "expected_last_completed_source_date": profile.final_source_date.isoformat(),
        "footer_manifest_sha256": digest(f"footers:{nonce}"),
        "input_plan_sha256": profile.input_plan_sha256,
        "portable_artifact_manifest_sha256": digest(f"portable-artifacts:{nonce}"),
        "rich_source_artifact_manifest_sha256": source_sha256,
        "signal_manifest_sha256": profile.signal_manifest_sha256,
        "source_hash_manifest_sha256": digest(f"source-hashes:{nonce}"),
        "source_record_manifest_sha256": digest(f"source-records:{nonce}"),
        "split_sha256": digest(f"split:{nonce}"),
        "terminal_resolution_sha256": _canonical_sha256(resolution),
    }
    completion_lineage = {
        "cache_manifest_sha256": cache_sha256,
        "cache_partition_count": profile.cache_partition_count,
        "expected_completed_source_date_count": profile.planned_source_date_count,
        "expected_last_completed_source_date": profile.final_source_date.isoformat(),
        "input_plan_sha256": profile.input_plan_sha256,
        "portable_discovery_artifact_manifest_sha256": input_lineage[
            "portable_artifact_manifest_sha256"
        ],
        "portable_discovery_input_manifest_sha256": input_manifest_sha256,
        "portable_signal_manifest_sha256": profile.signal_manifest_sha256,
        "source_record_manifest_sha256": input_lineage["source_record_manifest_sha256"],
        "terminal_resolution": resolution,
        "terminal_resolution_sha256": input_lineage["terminal_resolution_sha256"],
    }
    return {
        "cache_reference": cache_reference,
        "completion_lineage": completion_lineage,
        "dates": dates,
        "input_lineage": input_lineage,
        "input_lineage_sha256": _canonical_sha256(input_lineage),
        "source_sha256": source_sha256,
    }


def build_run_spec(query_id: str, fixture: dict[str, Any], nonce: str) -> RunSpec:
    profile = outcome_query_profile(query_id)
    resolution = terminal_resolution()
    return RunSpec(
        campaign_id=CAMPAIGN_KEY,
        experiment_id=None,
        run_kind="OUTCOME_BUILD",
        engine_version=OUTCOME_ENGINE_VERSION,
        source_manifest_hashes={"phase1a_ai_slices": fixture["source_sha256"]},
        eligible_calendar_version="phase1a_calendar_v1",
        eligible_calendar_sha256=digest(f"eligible-calendar:{nonce}"),
        split_version="phase1a_split_v1",
        split_sha256=digest(f"eligible-split:{nonce}"),
        feature_version="phase1a_mbp10_screening_v1",
        feature_sha256=digest(f"feature:{nonce}"),
        outcome_version=profile.outcome_config_id,
        outcome_sha256=profile.outcome_config_sha256,
        cost_version="phase1a_conservative_cost_v1",
        cost_sha256=digest(f"cost:{nonce}"),
        execution_version="phase1a_conservative_execution_v1",
        execution_sha256=digest(f"execution:{nonce}"),
        code_commit="7" * 40,
        code_snapshot_sha256=digest(f"code:{nonce}"),
        dependency_lock_sha256=digest(f"lock:{nonce}"),
        runtime_environment={"gate": "p4_full_completion", "query_id": query_id},
        random_seed=0,
        direction="BOTH",
        signal_policy={"query_id": query_id},
        entry_policy={"type": "MARKETABLE_LIMIT_IOC"},
        barrier_policy={"ticks": list(BARRIER_TICKS)},
        terminal_policy={
            "terminal_exit": resolution["terminal_exit_policy"],
            "terminal_partition_resolution": resolution["partition_resolution_policy"],
            "terminal_resolution_sha256": fixture["input_lineage"]["terminal_resolution_sha256"],
        },
        parameters={
            **phase1a_outcome_parameters(fixture["source_sha256"], query_id=query_id),
            **fixture["completion_lineage"],
            "fixture_nonce": nonce,
        },
    )


def build_detail_and_completion_artifacts(
    database_url: str,
    data_root: Path,
    *,
    fixture: dict[str, Any],
) -> None:
    profile = fixture["profile"]
    manifest_id = fixture["manifest_id"]
    fingerprint = fixture["run_spec"].fingerprint
    dates = fixture["dates"]
    base_rows, remainder = divmod(profile.detail_record_count, len(dates))
    shards: list[dict[str, object]] = []
    for sequence, source_date in enumerate(dates, start=1):
        path = publish_blob(
            data_root,
            profile.detail_shard_directory.as_posix(),
            f"PARQUET-P4-GATE:{profile.query_id}:{sequence}".encode("ascii"),
            suffix=".parquet",
        )
        sha256 = path.name.removeprefix("sha256=").removesuffix(".parquet")
        shards.append(
            {
                "artifact_relative_uri": path.relative_to(data_root / "derived").as_posix(),
                "artifact_sha256": sha256,
                "byte_size": path.stat().st_size,
                "record_manifest_sha256": digest(f"detail-records:{profile.query_id}:{sequence}"),
                "row_count": base_rows + (1 if sequence <= remainder else 0),
                "run_fingerprint": fingerprint,
                "shard_sequence": sequence,
                "source_date": source_date.isoformat(),
            }
        )
    detail_sha256 = _canonical_sha256(shards)
    fixture["detail_shards"] = shards
    fixture["detail_shard_manifest_sha256"] = detail_sha256

    progress = {"checkpoint_kind": "SOURCE_DATE_COMPLETE"}
    progress_sha256 = _canonical_sha256(progress)
    predecessor_sha256 = None
    for sequence, source_date in enumerate(dates[:-1], start=1):
        checkpoint_path = publish(
            data_root,
            profile.checkpoint_directory.as_posix(),
            {
                "artifact_schema": CHECKPOINT_ARTIFACT_SCHEMA,
                "checkpoint_sequence": sequence,
                "completed_source_date_count": sequence,
                "last_completed_source_date": source_date.isoformat(),
                "outcome_config_id": profile.outcome_config_id,
                "outcome_replay_manifest_id": manifest_id,
                "predecessor_checkpoint_sha256": predecessor_sha256,
                "progress_metadata_sha256": progress_sha256,
                "query_id": profile.query_id,
                "replay_state": {"next_source_date_index": sequence},
                "run_fingerprint": fingerprint,
                "source_event_count": sequence,
            },
        )
        checkpoint = register_phase1a_outcome_checkpoint(
            database_url,
            outcome_replay_manifest_id=manifest_id,
            run_fingerprint=fingerprint,
            checkpoint_sequence=sequence,
            completed_source_date_count=sequence,
            last_completed_source_date=source_date,
            source_event_count=sequence,
            predecessor_checkpoint_sha256=predecessor_sha256,
            progress_metadata=progress,
            checkpoint_artifact_path=checkpoint_path,
            data_root=data_root,
            query_id=profile.query_id,
        )
        predecessor_sha256 = checkpoint.checkpoint_artifact_sha256

    final_sequence = profile.planned_source_date_count
    final_replay_state = {
        "buffer": [],
        "completed_source_date": dates[-1].isoformat(),
        "drained_record_count": profile.detail_record_count,
        "finished": True,
        "occupancy": [],
        "pending_entries": [],
        "position_groups": [],
        "records": [],
        "result_record_count": profile.detail_record_count,
        "signal_cursor": profile.source_occurrence_count,
        "signals": [{} for _ in range(profile.source_occurrence_count)],
        "source_event_count": final_sequence,
    }
    replay_state_sha256 = _canonical_sha256(final_replay_state)
    final_progress = {
        "artifact_schema": "systematic_fx.phase1a_outcome_progress.v1",
        "cache_manifest_sha256": fixture["cache_reference"]["artifact_sha256"],
        "detail_record_count": profile.detail_record_count,
        "detail_shard_count": profile.planned_source_date_count,
        "detail_shard_manifest_sha256": detail_sha256,
        "input_lineage_sha256": fixture["input_lineage_sha256"],
        "replay_finished": True,
        "replay_state_sha256": replay_state_sha256,
        "source_event_count": final_sequence,
    }
    final_checkpoint_path = publish(
        data_root,
        profile.checkpoint_directory.as_posix(),
        {
            "artifact_schema": CHECKPOINT_ARTIFACT_SCHEMA,
            "cache_manifest": fixture["cache_reference"],
            "checkpoint_sequence": final_sequence,
            "completed_source_date_count": final_sequence,
            "detail_record_count": profile.detail_record_count,
            "detail_shard_manifest_sha256": detail_sha256,
            "detail_shards": shards,
            "input_lineage": fixture["input_lineage"],
            "input_lineage_sha256": fixture["input_lineage_sha256"],
            "last_completed_source_date": dates[-1].isoformat(),
            "outcome_config_id": profile.outcome_config_id,
            "outcome_replay_manifest_id": manifest_id,
            "predecessor_checkpoint_sha256": predecessor_sha256,
            "progress_metadata": final_progress,
            "progress_metadata_sha256": _canonical_sha256(final_progress),
            "query_id": profile.query_id,
            "replay_state": final_replay_state,
            "replay_state_sha256": replay_state_sha256,
            "run_fingerprint": fingerprint,
            "source_event_count": final_sequence,
        },
    )
    final_checkpoint = register_phase1a_outcome_checkpoint(
        database_url,
        outcome_replay_manifest_id=manifest_id,
        run_fingerprint=fingerprint,
        checkpoint_sequence=final_sequence,
        completed_source_date_count=final_sequence,
        last_completed_source_date=dates[-1],
        source_event_count=final_sequence,
        predecessor_checkpoint_sha256=predecessor_sha256,
        progress_metadata=final_progress,
        checkpoint_artifact_path=final_checkpoint_path,
        data_root=data_root,
        query_id=profile.query_id,
    )
    fixture["final_checkpoint_sha256"] = final_checkpoint.checkpoint_artifact_sha256
    final_reference = {
        "artifact_relative_uri": final_checkpoint_path.relative_to(
            data_root / "derived"
        ).as_posix(),
        "artifact_sha256": final_checkpoint.checkpoint_artifact_sha256,
        "byte_size": final_checkpoint.checkpoint_artifact_byte_size,
        "checkpoint_sequence": final_sequence,
        "last_completed_source_date": dates[-1].isoformat(),
        "progress_metadata": final_progress,
        "progress_metadata_sha256": _canonical_sha256(final_progress),
    }
    fixture["result_path"] = publish(
        data_root,
        f"outcomes/{profile.outcome_config_id}",
        {
            "artifact_schema": profile.outcome_artifact_schema,
            "cache_manifest": fixture["cache_reference"],
            "cell_summaries": [cell.payload for cell in fixture["cells"]],
            "cell_summaries_sha256": fixture["cells_sha256"],
            "detail_record_count": profile.detail_record_count,
            "detail_shard_count": profile.planned_source_date_count,
            "detail_shard_manifest_sha256": detail_sha256,
            "detail_shards": shards,
            "direction_ids": list(DIRECTION_IDS),
            "final_checkpoint": final_reference,
            "input_lineage": fixture["input_lineage"],
            "input_lineage_sha256": fixture["input_lineage_sha256"],
            "outcome_config_id": profile.outcome_config_id,
            "query_id": profile.query_id,
            "run_fingerprint": fingerprint,
            "scenario_ids": list(SCENARIO_IDS),
            "source_artifact_manifest_sha256": fixture["source_sha256"],
            "source_occurrence_count": profile.source_occurrence_count,
            "source_slice_count": profile.source_slice_count,
            "summary_row_count": EXPECTED_SUMMARY_COUNT,
        },
    )


CELL_INSERT_SQL = """
    INSERT INTO systematic_fx.phase1a_outcome_cell_summaries
        (outcome_replay_manifest_id, run_fingerprint, scenario_id, direction,
         take_profit_ticks, stop_loss_ticks, signal_count, entry_fill_count,
         entry_not_filled_count, skipped_occupied_count,
         take_profit_first_count, stop_first_count, terminal_exit_count,
         censored_count, gross_pnl_ticks, variable_cost_ticks,
         allocated_fixed_cost_ticks, fully_loaded_net_pnl_ticks,
         fully_loaded_net_ev_ticks, fully_loaded_net_pnl_usd,
         calendar_month_net_pnl_usd, profit_factor, maximum_drawdown_usd,
         complete, summary_sha256)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def expect_statement_rejection(
    connection: psycopg.Connection[Any],
    label: str,
    operation: Any,
    expected_text: str,
) -> None:
    savepoint = "p4_negative_gate"
    connection.execute(f"SAVEPOINT {savepoint}")
    try:
        operation()
    except psycopg.Error as error:
        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")
        if expected_text not in str(error):
            raise AssertionError(f"{label} rejected for wrong reason: {error}") from error
        print(f"NEGATIVE {label}: {type(error).__name__}: {error}")
    else:
        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")
        raise AssertionError(f"{label} unexpectedly succeeded")


def exercise_cell_negatives(connection: psycopg.Connection[Any], fixture: dict[str, Any]) -> None:
    parameters = list(
        _cell_insert_parameters(
            fixture["manifest_id"],
            fixture["run_spec"].fingerprint,
            fixture["cells"][0],
        )
    )
    forged_sha = list(parameters)
    forged_sha[-1] = "f" * 64
    expect_statement_rejection(
        connection,
        "forged cell summary SHA",
        lambda: connection.execute(CELL_INSERT_SQL, forged_sha),
        "summary SHA-256 drift",
    )
    forged_cost = list(parameters)
    forged_cost[15] = int(forged_cost[15]) + 1
    forged_cost[17] = int(forged_cost[17]) - 1
    expect_statement_rejection(
        connection,
        "forged scenario cost",
        lambda: connection.execute(CELL_INSERT_SQL, forged_cost),
        "scenario-cost accounting drift",
    )
    forged_infinity = list(parameters)
    forged_infinity[19] = Decimal("Infinity")
    expect_statement_rejection(
        connection,
        "nonfinite cell metric",
        lambda: connection.execute(CELL_INSERT_SQL, forged_infinity),
        "decimal metrics must be finite",
    )
    negative_zero = replace(fixture["cells"][0], fully_loaded_net_pnl_usd=Decimal("-0"))
    negative_zero_parameters = _cell_insert_parameters(
        fixture["manifest_id"],
        fixture["run_spec"].fingerprint,
        negative_zero,
    )
    expect_statement_rejection(
        connection,
        "negative-zero canonicalization",
        lambda: connection.execute(CELL_INSERT_SQL, negative_zero_parameters),
        "summary SHA-256 drift",
    )


def prepare_member_success(
    connection: psycopg.Connection[dict[str, Any]],
    fixture: dict[str, Any],
    *,
    claimed_cells_sha256: str,
    register_decisions: bool = True,
) -> None:
    profile = fixture["profile"]
    manifest_id = fixture["manifest_id"]
    fingerprint = fixture["run_spec"].fingerprint
    manifest = _load_manifest_for_update(
        connection,
        outcome_replay_manifest_id=manifest_id,
    )
    held = _open_held_immutable_file(fixture["result_path"], data_root=fixture["data_root"])
    try:
        lineage = _validate_result_artifact(
            held,
            run_fingerprint=fingerprint,
            source_artifact_manifest_sha256=fixture["source_sha256"],
            cell_summaries_sha256=fixture["cells_sha256"],
            cell_summaries=fixture["cells"],
            data_root=fixture["data_root"],
            profile=profile,
        )
        _validate_run_spec_completion_lineage(manifest, lineage, profile=profile)
        final_checkpoint_sha256, planned_count = _validate_final_checkpoint(
            connection,
            manifest_id=manifest_id,
            run_fingerprint=fingerprint,
            lineage=lineage,
            data_root=fixture["data_root"],
            profile=profile,
        )
        _register_cells(
            connection,
            manifest_id=manifest_id,
            run_fingerprint=fingerprint,
            cells=fixture["cells"],
        )
        observed_sha256 = connection.execute(
            """
            SELECT systematic_fx.canonical_jsonb_sha256(
                       jsonb_agg(
                           systematic_fx.phase1a_outcome_cell_summary_payload(cell)
                           ORDER BY
                               CASE cell.scenario_id
                                   WHEN 'BASELINE' THEN 1
                                   WHEN 'MODERATE_COMBINED' THEN 2
                                   WHEN 'SEVERE_DIAGNOSTIC' THEN 3
                               END,
                               CASE cell.direction WHEN 'LONG' THEN 1 ELSE 2 END,
                               cell.take_profit_ticks,
                               cell.stop_loss_ticks
                       )
                   ) AS digest
            FROM systematic_fx.phase1a_outcome_cell_summaries AS cell
            WHERE outcome_replay_manifest_id = %s
            """,
            (manifest_id,),
        ).fetchone()["digest"]
        if observed_sha256 != fixture["cells_sha256"]:
            raise AssertionError(
                f"SQL/Python aggregate digest mismatch for {profile.query_id}: "
                f"{observed_sha256} != {fixture['cells_sha256']}"
            )
        artifact_id, _, _ = _ensure_result_artifact(
            connection,
            run_fingerprint=fingerprint,
            source_artifact_manifest_sha256=fixture["source_sha256"],
            cells_sha256=claimed_cells_sha256,
            lineage=lineage,
            final_checkpoint_sha256=final_checkpoint_sha256,
            held=held,
            profile=profile,
        )
        finished_at = datetime.now(UTC)
        result_summary = {
            "artifact_sha256": held.sha256,
            "cache_manifest_sha256": lineage.cache_manifest_sha256,
            "cell_summaries_sha256": claimed_cells_sha256,
            "cumulative_economic_cell_count": PHASE1A_CUMULATIVE_ECONOMIC_CELL_COUNT,
            "detail_record_count": lineage.detail_record_count,
            "detail_shard_count": len(lineage.detail_shards),
            "detail_shard_manifest_sha256": lineage.detail_shard_manifest_sha256,
            "final_checkpoint_sha256": final_checkpoint_sha256,
            "input_lineage_sha256": lineage.input_lineage_sha256,
            "outcome_config_id": profile.outcome_config_id,
            "pair_config_sha256": P4_PAIR_CONFIG_SHA256,
            "pair_economic_cell_count": P4_PAIR_ECONOMIC_CELL_COUNT,
            "pair_id": P4_PAIR_ID,
            "paired_query_ids": [P4_01_QUERY_ID, P4_02_QUERY_ID],
            "planned_source_date_count": planned_count,
            "prior_outcome_lineage_sha256": P4_PAIR_PRIOR_LINEAGE_SHA256,
            "query_id": profile.query_id,
            "source_artifact_manifest_sha256": fixture["source_sha256"],
            "summary_row_count": EXPECTED_SUMMARY_COUNT,
        }
        updated_attempt = connection.execute(
            """
            UPDATE systematic_fx.research_run_attempts
            SET status = 'SUCCEEDED', result_artifact_id = %s,
                result_summary = %s, finished_at = %s
            WHERE research_run_attempt_id = %s AND status = 'RUNNING'
            RETURNING research_run_attempt_id
            """,
            (
                artifact_id,
                Jsonb(result_summary),
                finished_at,
                manifest["research_run_attempt_id"],
            ),
        ).fetchone()
        if updated_attempt is None:
            raise AssertionError("direct negative setup failed to update attempt")
        updated_manifest = connection.execute(
            """
            UPDATE systematic_fx.phase1a_outcome_replay_manifests
            SET status = 'SUCCEEDED', result_artifact_id = %s,
                result_artifact_sha256 = %s,
                result_artifact_byte_size = %s,
                cell_summaries_sha256 = %s,
                finished_at = %s
            WHERE outcome_replay_manifest_id = %s AND status = 'RUNNING'
            RETURNING outcome_replay_manifest_id
            """,
            (
                artifact_id,
                held.sha256,
                held.byte_size,
                claimed_cells_sha256,
                finished_at,
                manifest_id,
            ),
        ).fetchone()
        if updated_manifest is None:
            raise AssertionError("direct negative setup failed to update manifest")
        if register_decisions:
            _register_screening_decisions(
                connection,
                manifest_id=manifest_id,
                decisions=fixture["decisions"],
            )
    finally:
        held.close()


def decision_sha256s(fixtures: dict[str, dict[str, Any]]) -> dict[str, dict[str, str]]:
    return {
        query_id: {
            decision.direction: _canonical_sha256(
                decision.payload(outcome_replay_manifest_id=fixtures[query_id]["manifest_id"])
            )
            for decision in fixtures[query_id]["decisions"]
        }
        for query_id in (P4_01_QUERY_ID, P4_02_QUERY_ID)
    }


def insert_pair_release(
    connection: psycopg.Connection[Any],
    fixtures: dict[str, dict[str, Any]],
    *,
    batch_id: int,
    claimed_cells_sha256s: dict[str, str],
    noncanonical: bool = False,
) -> None:
    p4_01 = fixtures[P4_01_QUERY_ID]
    p4_02 = fixtures[P4_02_QUERY_ID]
    decisions = decision_sha256s(fixtures)
    payload = _p4_pair_release_payload(
        p4_01_outcome_replay_manifest_id=p4_01["manifest_id"],
        p4_01_run_fingerprint=p4_01["run_spec"].fingerprint,
        p4_01_result_artifact_sha256=digest_file(p4_01["result_path"]),
        p4_01_cell_summaries_sha256=claimed_cells_sha256s[P4_01_QUERY_ID],
        p4_02_outcome_replay_manifest_id=p4_02["manifest_id"],
        p4_02_run_fingerprint=p4_02["run_spec"].fingerprint,
        p4_02_result_artifact_sha256=digest_file(p4_02["result_path"]),
        p4_02_cell_summaries_sha256=claimed_cells_sha256s[P4_02_QUERY_ID],
        decision_sha256s=decisions,
    )
    canonical_text = _canonical_json_bytes(payload).decode("utf-8")
    release_text = (
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False)
        if noncanonical
        else canonical_text
    )
    release_sha256 = hashlib.sha256(release_text.encode("utf-8")).hexdigest()
    connection.execute(
        """
        INSERT INTO systematic_fx.phase1a_p4_outcome_pair_releases
            (p4_pair_batch_id, pair_id,
             p4_01_outcome_replay_manifest_id,
             p4_02_outcome_replay_manifest_id,
             p4_01_run_fingerprint, p4_02_run_fingerprint,
             p4_01_result_artifact_sha256,
             p4_02_result_artifact_sha256,
             p4_01_cell_summaries_sha256,
             p4_02_cell_summaries_sha256,
             decision_sha256s, pair_config_sha256,
             prior_outcome_lineage_sha256,
             pair_economic_cell_count,
             cumulative_economic_cell_count,
             canonical_release_json, pair_release_sha256)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s)
        """,
        (
            batch_id,
            P4_PAIR_ID,
            p4_01["manifest_id"],
            p4_02["manifest_id"],
            p4_01["run_spec"].fingerprint,
            p4_02["run_spec"].fingerprint,
            digest_file(p4_01["result_path"]),
            digest_file(p4_02["result_path"]),
            claimed_cells_sha256s[P4_01_QUERY_ID],
            claimed_cells_sha256s[P4_02_QUERY_ID],
            Jsonb(decisions),
            P4_PAIR_CONFIG_SHA256,
            P4_PAIR_PRIOR_LINEAGE_SHA256,
            P4_PAIR_ECONOMIC_CELL_COUNT,
            PHASE1A_CUMULATIVE_ECONOMIC_CELL_COUNT,
            release_text,
            release_sha256,
        ),
    )


def digest_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def exercise_direct_sql_negatives(
    database_url: str,
    fixtures: dict[str, dict[str, Any]],
    *,
    batch_id: int,
) -> None:
    first = fixtures[P4_01_QUERY_ID]
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        exercise_cell_negatives(connection, first)
        connection.rollback()

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        prepare_member_success(
            connection,
            first,
            claimed_cells_sha256=first["cells_sha256"],
        )
        try:
            connection.commit()
        except psycopg.Error as error:
            if "P4" not in str(error):
                raise AssertionError(
                    f"one-member success rejected for wrong reason: {error}"
                ) from error
            print(f"NEGATIVE one-member success commit: {type(error).__name__}: {error}")
            connection.rollback()
        else:
            raise AssertionError("one-member P4 success unexpectedly committed")

    wrong_claims = {P4_01_QUERY_ID: "0" * 64, P4_02_QUERY_ID: "1" * 64}
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        for query_id in (P4_01_QUERY_ID, P4_02_QUERY_ID):
            prepare_member_success(
                connection,
                fixtures[query_id],
                claimed_cells_sha256=wrong_claims[query_id],
            )
        expect_statement_rejection(
            connection,
            "wrong aggregate release",
            lambda: insert_pair_release(
                connection,
                fixtures,
                batch_id=batch_id,
                claimed_cells_sha256s=wrong_claims,
            ),
            "aggregate SHA-256 drift",
        )
        connection.rollback()

    true_claims = {
        query_id: fixtures[query_id]["cells_sha256"]
        for query_id in (P4_01_QUERY_ID, P4_02_QUERY_ID)
    }
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        for query_id in (P4_01_QUERY_ID, P4_02_QUERY_ID):
            prepare_member_success(
                connection,
                fixtures[query_id],
                claimed_cells_sha256=true_claims[query_id],
                register_decisions=False,
            )
        decision = first["decisions"][0]
        expect_statement_rejection(
            connection,
            "forged decision SHA",
            lambda: connection.execute(
                """
                INSERT INTO systematic_fx.phase1a_outcome_screening_decisions
                    (outcome_replay_manifest_id, direction, decision_label,
                     selected_take_profit_ticks, selected_stop_loss_ticks,
                     positive_region_size, rejection_reasons, decision_sha256)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    first["manifest_id"],
                    decision.direction,
                    decision.decision_label,
                    decision.selected_take_profit_ticks,
                    decision.selected_stop_loss_ticks,
                    decision.positive_region_size,
                    Jsonb(list(decision.rejection_reasons)),
                    "f" * 64,
                ),
            ),
            "decision SHA-256 drift",
        )
        tab_payload = {
            "decision_label": "SCREENING_REJECT",
            "direction": "LONG",
            "outcome_replay_manifest_id": first["manifest_id"],
            "positive_region_size": 0,
            "rejection_reasons": ["\t"],
            "selected_stop_loss_ticks": None,
            "selected_take_profit_ticks": None,
        }
        expect_statement_rejection(
            connection,
            "tab-only decision reason",
            lambda: connection.execute(
                """
                INSERT INTO systematic_fx.phase1a_outcome_screening_decisions
                    (outcome_replay_manifest_id, direction, decision_label,
                     selected_take_profit_ticks, selected_stop_loss_ticks,
                     positive_region_size, rejection_reasons, decision_sha256)
                VALUES (%s, 'LONG', 'SCREENING_REJECT', NULL, NULL, 0, %s, %s)
                """,
                (
                    first["manifest_id"],
                    Jsonb(["\t"]),
                    _canonical_sha256(tab_payload),
                ),
            ),
            "unique nonblank strings",
        )
        for query_id in (P4_01_QUERY_ID, P4_02_QUERY_ID):
            _register_screening_decisions(
                connection,
                manifest_id=fixtures[query_id]["manifest_id"],
                decisions=fixtures[query_id]["decisions"],
            )
        expect_statement_rejection(
            connection,
            "noncanonical release JSON",
            lambda: insert_pair_release(
                connection,
                fixtures,
                batch_id=batch_id,
                claimed_cells_sha256s=true_claims,
                noncanonical=True,
            ),
            "canonical release payload",
        )
        connection.rollback()


def verify_fresh_migration_chain(base: dict[str, str], admin_url: str) -> None:
    database_name = f"systematic_fx_p4_fresh_migrations_{os.getpid()}"
    database_url = make_conninfo(**{**base, "dbname": database_name})
    with psycopg.connect(admin_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
    try:
        first = apply_migrations(database_url)
        repeated = apply_migrations(database_url)
        if first.applied != tuple(range(1, 31)) or first.skipped:
            raise AssertionError(f"fresh P4 migration chain drift: {first}")
        if repeated.applied or repeated.skipped != tuple(range(1, 31)):
            raise AssertionError(f"repeated P4 migration chain drift: {repeated}")
        with psycopg.connect(database_url) as connection:
            versions = tuple(
                row[0]
                for row in connection.execute(
                    "SELECT version FROM systematic_fx.schema_migrations ORDER BY version"
                ).fetchall()
            )
        if versions != tuple(range(1, 31)):
            raise AssertionError(f"stored P4 migration versions drift: {versions}")
        print("MIGRATIONS fresh=1..30 repeated=all-skipped")
    finally:
        with psycopg.connect(admin_url, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(database_name)))
    with psycopg.connect(admin_url) as connection:
        remaining = connection.execute(
            "SELECT count(*) FROM pg_database WHERE datname = %s",
            (database_name,),
        ).fetchone()[0]
    if remaining != 0:
        raise AssertionError("fresh migration disposable database was not dropped")


def sanitize_cloned_p4_state(database_url: str) -> None:
    """Remove inherited P4 rows from the disposable clone, never the source DB."""

    with psycopg.connect(  # noqa: SIM117 - transaction uses this connection
        database_url, row_factory=dict_row
    ) as connection:
        with connection.transaction():
            connection.execute("SET LOCAL session_replication_role = replica")
            connection.execute(
                """
                CREATE TEMP TABLE p4_gate_manifest_ids ON COMMIT DROP AS
                SELECT outcome_replay_manifest_id, research_run_spec_id,
                       research_run_attempt_id, result_artifact_id
                FROM systematic_fx.phase1a_outcome_replay_manifests
                WHERE pattern_key IN (%s, %s)
                """,
                (P4_01_QUERY_ID, P4_02_QUERY_ID),
            )
            connection.execute(
                """
                CREATE TEMP TABLE p4_gate_spec_ids ON COMMIT DROP AS
                SELECT DISTINCT research_run_spec_id
                FROM (
                    SELECT research_run_spec_id
                    FROM p4_gate_manifest_ids
                    UNION ALL
                    SELECT research_run_spec_id
                    FROM systematic_fx.research_run_specs
                    WHERE canonical_spec #>> '{parameters,query_id}' IN (%s, %s)
                ) AS governed_specs
                """,
                (P4_01_QUERY_ID, P4_02_QUERY_ID),
            )
            connection.execute(
                """
                CREATE TEMP TABLE p4_gate_attempt_ids ON COMMIT DROP AS
                SELECT DISTINCT research_run_attempt_id
                FROM (
                    SELECT research_run_attempt_id
                    FROM p4_gate_manifest_ids
                    UNION ALL
                    SELECT attempt.research_run_attempt_id
                    FROM systematic_fx.research_run_attempts AS attempt
                    JOIN p4_gate_spec_ids AS governed_spec
                      USING (research_run_spec_id)
                ) AS governed_attempts
                """
            )
            connection.execute(
                """
                CREATE TEMP TABLE p4_gate_artifact_ids ON COMMIT DROP AS
                SELECT DISTINCT artifact_id
                FROM (
                    SELECT result_artifact_id AS artifact_id
                    FROM p4_gate_manifest_ids
                    WHERE result_artifact_id IS NOT NULL
                    UNION ALL
                    SELECT checkpoint.checkpoint_artifact_id
                    FROM systematic_fx.phase1a_outcome_replay_checkpoints AS checkpoint
                    JOIN p4_gate_manifest_ids AS governed_manifest
                      USING (outcome_replay_manifest_id)
                    UNION ALL
                    SELECT attempt.result_artifact_id
                    FROM systematic_fx.research_run_attempts AS attempt
                    JOIN p4_gate_attempt_ids AS governed_attempt
                      USING (research_run_attempt_id)
                    WHERE attempt.result_artifact_id IS NOT NULL
                    UNION ALL
                    SELECT attempt.trade_ledger_artifact_id
                    FROM systematic_fx.research_run_attempts AS attempt
                    JOIN p4_gate_attempt_ids AS governed_attempt
                      USING (research_run_attempt_id)
                    WHERE attempt.trade_ledger_artifact_id IS NOT NULL
                    UNION ALL
                    SELECT artifact.artifact_id
                    FROM systematic_fx.artifacts AS artifact
                    WHERE artifact.metadata ->> 'query_id' IN (%s, %s)
                ) AS governed_artifacts
                """,
                (P4_01_QUERY_ID, P4_02_QUERY_ID),
            )
            deleted: dict[str, int] = {}
            for label, statement in (
                (
                    "releases",
                    """DELETE FROM systematic_fx.phase1a_p4_outcome_pair_releases
                       WHERE pair_id = %s""",
                ),
                (
                    "batches",
                    """DELETE FROM systematic_fx.phase1a_p4_outcome_pair_batches
                       WHERE pair_id = %s""",
                ),
                (
                    "decisions",
                    """DELETE FROM systematic_fx.phase1a_outcome_screening_decisions
                       WHERE outcome_replay_manifest_id IN
                             (SELECT outcome_replay_manifest_id
                              FROM p4_gate_manifest_ids)""",
                ),
                (
                    "cells",
                    """DELETE FROM systematic_fx.phase1a_outcome_cell_summaries
                       WHERE outcome_replay_manifest_id IN
                             (SELECT outcome_replay_manifest_id
                              FROM p4_gate_manifest_ids)""",
                ),
                (
                    "checkpoints",
                    """DELETE FROM systematic_fx.phase1a_outcome_replay_checkpoints
                       WHERE outcome_replay_manifest_id IN
                             (SELECT outcome_replay_manifest_id
                              FROM p4_gate_manifest_ids)""",
                ),
                (
                    "manifests",
                    """DELETE FROM systematic_fx.phase1a_outcome_replay_manifests
                       WHERE outcome_replay_manifest_id IN
                             (SELECT outcome_replay_manifest_id
                              FROM p4_gate_manifest_ids)""",
                ),
                (
                    "attempts",
                    """DELETE FROM systematic_fx.research_run_attempts
                       WHERE research_run_attempt_id IN
                             (SELECT research_run_attempt_id
                              FROM p4_gate_attempt_ids)""",
                ),
                (
                    "specs",
                    """DELETE FROM systematic_fx.research_run_specs
                       WHERE research_run_spec_id IN
                             (SELECT research_run_spec_id FROM p4_gate_spec_ids)""",
                ),
                (
                    "artifacts",
                    """DELETE FROM systematic_fx.artifacts
                       WHERE artifact_id IN
                             (SELECT artifact_id FROM p4_gate_artifact_ids)""",
                ),
            ):
                parameters = (P4_PAIR_ID,) if label in {"releases", "batches"} else ()
                deleted[label] = connection.execute(statement, parameters).rowcount
            governed_counts = connection.execute(
                """
                SELECT
                    (SELECT count(*)
                     FROM systematic_fx.phase1a_p4_outcome_pair_releases
                     WHERE pair_id = %s) AS releases,
                    (SELECT count(*)
                     FROM systematic_fx.phase1a_p4_outcome_pair_batches
                     WHERE pair_id = %s) AS batches,
                    (SELECT count(*)
                     FROM systematic_fx.phase1a_outcome_screening_decisions
                     WHERE outcome_replay_manifest_id IN
                           (SELECT outcome_replay_manifest_id
                            FROM p4_gate_manifest_ids)) AS decisions,
                    (SELECT count(*)
                     FROM systematic_fx.phase1a_outcome_cell_summaries
                     WHERE outcome_replay_manifest_id IN
                           (SELECT outcome_replay_manifest_id
                            FROM p4_gate_manifest_ids)) AS cells,
                    (SELECT count(*)
                     FROM systematic_fx.phase1a_outcome_replay_checkpoints
                     WHERE outcome_replay_manifest_id IN
                           (SELECT outcome_replay_manifest_id
                            FROM p4_gate_manifest_ids)) AS checkpoints,
                    (SELECT count(*)
                     FROM systematic_fx.phase1a_outcome_replay_manifests
                     WHERE outcome_replay_manifest_id IN
                           (SELECT outcome_replay_manifest_id
                            FROM p4_gate_manifest_ids)) AS manifests,
                    (SELECT count(*)
                     FROM systematic_fx.research_run_attempts
                     WHERE research_run_attempt_id IN
                           (SELECT research_run_attempt_id
                            FROM p4_gate_attempt_ids)) AS attempts,
                    (SELECT count(*)
                     FROM systematic_fx.research_run_specs
                     WHERE research_run_spec_id IN
                           (SELECT research_run_spec_id
                            FROM p4_gate_spec_ids)) AS specs,
                    (SELECT count(*)
                     FROM systematic_fx.artifacts
                     WHERE artifact_id IN
                           (SELECT artifact_id
                            FROM p4_gate_artifact_ids)) AS artifacts
                """,
                (P4_PAIR_ID, P4_PAIR_ID),
            ).fetchone()
            if any(governed_counts.values()):
                raise AssertionError(
                    f"disposable P4 sanitization left inherited child rows: {governed_counts}"
                )

    with psycopg.connect(  # noqa: SIM117 - transaction uses this connection
        database_url, row_factory=dict_row
    ) as connection:
        with connection.transaction():
            prior = _validate_p4_prior_outcome_lineage(connection)
            counts = connection.execute(
                """
                SELECT
                    (SELECT count(*)
                     FROM systematic_fx.phase1a_p4_outcome_pair_releases
                     WHERE pair_id = %s) AS releases,
                    (SELECT count(*)
                     FROM systematic_fx.phase1a_p4_outcome_pair_batches
                     WHERE pair_id = %s) AS batches,
                    (SELECT count(*)
                     FROM systematic_fx.phase1a_outcome_replay_manifests
                     WHERE pattern_key IN (%s, %s)) AS manifests,
                    (SELECT count(*)
                     FROM systematic_fx.research_run_specs
                     WHERE canonical_spec #>> '{parameters,query_id}' IN (%s, %s))
                        AS specs,
                    (SELECT count(*)
                     FROM systematic_fx.research_run_attempts AS attempt
                     JOIN systematic_fx.research_run_specs AS spec
                       USING (research_run_spec_id)
                     WHERE spec.canonical_spec #>> '{parameters,query_id}' IN (%s, %s))
                        AS attempts,
                    (SELECT count(*)
                     FROM systematic_fx.artifacts
                     WHERE metadata ->> 'query_id' IN (%s, %s)) AS artifacts
                """,
                (
                    P4_PAIR_ID,
                    P4_PAIR_ID,
                    P4_01_QUERY_ID,
                    P4_02_QUERY_ID,
                    P4_01_QUERY_ID,
                    P4_02_QUERY_ID,
                    P4_01_QUERY_ID,
                    P4_02_QUERY_ID,
                    P4_01_QUERY_ID,
                    P4_02_QUERY_ID,
                ),
            ).fetchone()
    if prior != P4_PAIR_PRIOR_LINEAGE:
        raise AssertionError("disposable P4 sanitization altered frozen P5/P1 lineage")
    if any(counts.values()):
        raise AssertionError(f"disposable P4 sanitization left governed rows: {counts}")
    print(
        f"SANITIZE disposable_clone_deleted={deleted} "
        f"p4_remaining={governed_counts} rediscovered={counts}"
    )


def main() -> None:
    project = Path.cwd()
    settings = Settings.from_env(working_directory=project)
    base = conninfo_to_dict(settings.database_url)
    source_database = base["dbname"]
    database_name = f"systematic_fx_p4_full_gate_{os.getpid()}"
    admin_url = make_conninfo(**{**base, "dbname": "postgres"})
    database_url = make_conninfo(**{**base, "dbname": database_name})
    verify_fresh_migration_chain(base, admin_url)
    with psycopg.connect(admin_url, autocommit=True) as connection:
        connection.execute(
            sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                sql.Identifier(database_name),
                sql.Identifier(source_database),
            )
        )
    try:
        report = apply_migrations(database_url)
        print(f"MIGRATIONS {report}")
        sanitize_cloned_p4_state(database_url)
        with tempfile.TemporaryDirectory(prefix="p4-full-gate-") as temporary:
            data_root = Path(temporary).resolve() / "data"
            (data_root / "derived").mkdir(parents=True)
            fixtures: dict[str, dict[str, Any]] = {}
            for query_id in (P4_01_QUERY_ID, P4_02_QUERY_ID):
                nonce = f"full-gate:{query_id}:{os.getpid()}"
                source_sha256 = digest(f"source:{nonce}")
                fixture = build_input_fixture(
                    data_root,
                    query_id=query_id,
                    source_sha256=source_sha256,
                    nonce=nonce,
                )
                fixture["data_root"] = data_root
                fixture["profile"] = outcome_query_profile(query_id)
                fixture["run_spec"] = build_run_spec(query_id, fixture, nonce)
                fixture["cells"], fixture["cells_sha256"] = build_cells(query_id)
                fixture["decisions"] = derive_phase1a_outcome_screening_decisions(
                    fixture["cells"], query_id=query_id
                )
                register_run_spec(database_url, fixture["run_spec"])
                reservation = reserve_phase1a_outcome_replay(
                    database_url,
                    run_fingerprint=fixture["run_spec"].fingerprint,
                    source_artifact_manifest_sha256=source_sha256,
                    query_id=query_id,
                )
                if not reservation.execute or not reservation.created_manifest:
                    raise AssertionError(f"{query_id} initial reservation was not new")
                fixture["manifest_id"] = reservation.outcome_replay_manifest_id
                fixtures[query_id] = fixture

            p4_01 = fixtures[P4_01_QUERY_ID]
            p4_02 = fixtures[P4_02_QUERY_ID]
            try:
                reserve_phase1a_p4_outcome_pair(
                    database_url,
                    p4_01_outcome_replay_manifest_id=p4_01["manifest_id"],
                    p4_01_run_fingerprint=p4_01["run_spec"].fingerprint,
                    p4_02_outcome_replay_manifest_id=2**62,
                    p4_02_run_fingerprint=p4_02["run_spec"].fingerprint,
                )
            except OutcomeRegistryError as error:
                print(f"NEGATIVE missing pair member: {type(error).__name__}: {error}")
            else:
                raise AssertionError("missing P4 pair member unexpectedly reserved")

            pair = reserve_phase1a_p4_outcome_pair(
                database_url,
                p4_01_outcome_replay_manifest_id=p4_01["manifest_id"],
                p4_01_run_fingerprint=p4_01["run_spec"].fingerprint,
                p4_02_outcome_replay_manifest_id=p4_02["manifest_id"],
                p4_02_run_fingerprint=p4_02["run_spec"].fingerprint,
            )
            with psycopg.connect(database_url) as connection:
                connection.execute("SAVEPOINT invalid_batch")
                try:
                    connection.execute(
                        """
                        INSERT INTO systematic_fx.phase1a_p4_outcome_pair_batches
                            (pair_id,
                             p4_01_outcome_replay_manifest_id,
                             p4_02_outcome_replay_manifest_id,
                             p4_01_run_fingerprint,
                             p4_02_run_fingerprint,
                             pair_config_sha256,
                             p4_01_outcome_config_sha256,
                             p4_02_outcome_config_sha256,
                             p4_01_query_definition_sha256,
                             p4_02_query_definition_sha256,
                             p4_01_signal_manifest_sha256,
                             p4_02_signal_manifest_sha256,
                             p4_01_input_plan_sha256,
                             p4_02_input_plan_sha256,
                             prior_outcome_lineage,
                             prior_outcome_lineage_sha256,
                             status, finished_at, error_message)
                        SELECT pair_id,
                               p4_01_outcome_replay_manifest_id,
                               p4_02_outcome_replay_manifest_id,
                               p4_01_run_fingerprint,
                               p4_02_run_fingerprint,
                               pair_config_sha256,
                               p4_01_outcome_config_sha256,
                               p4_02_outcome_config_sha256,
                               p4_01_query_definition_sha256,
                               p4_02_query_definition_sha256,
                               p4_01_signal_manifest_sha256,
                               p4_02_signal_manifest_sha256,
                               p4_01_input_plan_sha256,
                               p4_02_input_plan_sha256,
                               prior_outcome_lineage,
                               prior_outcome_lineage_sha256,
                               'FAILED', statement_timestamp(), 'forged terminal insert'
                        FROM systematic_fx.phase1a_p4_outcome_pair_batches
                        WHERE p4_pair_batch_id = %s
                        """,
                        (pair.p4_pair_batch_id,),
                    )
                except psycopg.Error as error:
                    connection.execute("ROLLBACK TO SAVEPOINT invalid_batch")
                    if "must begin PREPARED" not in str(error):
                        raise
                    print(f"NEGATIVE terminal batch insert: {type(error).__name__}: {error}")
                else:
                    raise AssertionError("terminal P4 batch INSERT unexpectedly succeeded")
                connection.rollback()

            start_phase1a_outcome_replay(
                database_url,
                outcome_replay_manifest_id=p4_01["manifest_id"],
                run_fingerprint=p4_01["run_spec"].fingerprint,
            )
            resumed_mixed = reserve_phase1a_p4_outcome_pair(
                database_url,
                p4_01_outcome_replay_manifest_id=p4_01["manifest_id"],
                p4_01_run_fingerprint=p4_01["run_spec"].fingerprint,
                p4_02_outcome_replay_manifest_id=p4_02["manifest_id"],
                p4_02_run_fingerprint=p4_02["run_spec"].fingerprint,
            )
            if resumed_mixed.created or resumed_mixed.p4_pair_batch_id != pair.p4_pair_batch_id:
                raise AssertionError("RUNNING+QUEUED P4 pair did not resume exact batch")
            start_phase1a_outcome_replay(
                database_url,
                outcome_replay_manifest_id=p4_02["manifest_id"],
                run_fingerprint=p4_02["run_spec"].fingerprint,
            )
            resumed_running = reserve_phase1a_p4_outcome_pair(
                database_url,
                p4_01_outcome_replay_manifest_id=p4_01["manifest_id"],
                p4_01_run_fingerprint=p4_01["run_spec"].fingerprint,
                p4_02_outcome_replay_manifest_id=p4_02["manifest_id"],
                p4_02_run_fingerprint=p4_02["run_spec"].fingerprint,
            )
            if resumed_running.created or resumed_running.p4_pair_batch_id != pair.p4_pair_batch_id:
                raise AssertionError("RUNNING+RUNNING P4 pair did not resume exact batch")
            print("POSITIVE resume states=RUNNING+QUEUED,RUNNING+RUNNING")
            try:
                fail_phase1a_outcome_replay(
                    database_url,
                    outcome_replay_manifest_id=p4_01["manifest_id"],
                    run_fingerprint=p4_01["run_spec"].fingerprint,
                    error_message="forbidden individual P4 failure",
                )
            except OutcomeRegistryError as error:
                print(f"NEGATIVE individual fail: {type(error).__name__}: {error}")
            else:
                raise AssertionError("individual P4 fail unexpectedly succeeded")
            try:
                complete_phase1a_outcome_replay(
                    database_url,
                    outcome_replay_manifest_id=p4_01["manifest_id"],
                    run_fingerprint=p4_01["run_spec"].fingerprint,
                    cell_summaries=(),
                    result_artifact_path=data_root / "missing.json",
                    data_root=data_root,
                    query_id=P4_01_QUERY_ID,
                )
            except OutcomeRegistryError as error:
                print(f"NEGATIVE individual complete: {type(error).__name__}: {error}")
            else:
                raise AssertionError("individual P4 complete unexpectedly succeeded")

            failure_message = "tracked atomic P4 pair retry"
            failed_pair = fail_phase1a_p4_outcome_pair(
                database_url,
                p4_pair_batch_id=pair.p4_pair_batch_id,
                p4_01_run_fingerprint=p4_01["run_spec"].fingerprint,
                p4_02_run_fingerprint=p4_02["run_spec"].fingerprint,
                error_message=failure_message,
            )
            repeated_failure = fail_phase1a_p4_outcome_pair(
                database_url,
                p4_pair_batch_id=pair.p4_pair_batch_id,
                p4_01_run_fingerprint=p4_01["run_spec"].fingerprint,
                p4_02_run_fingerprint=p4_02["run_spec"].fingerprint,
                error_message=failure_message,
            )
            if (
                failed_pair.status != "FAILED"
                or repeated_failure.status != "FAILED"
                or any(state.status != "FAILED" for state in failed_pair.states)
            ):
                raise AssertionError("P4 pair failure did not terminalize both members")
            first_batch_id = pair.p4_pair_batch_id
            first_manifest_ids = {
                query_id: fixtures[query_id]["manifest_id"]
                for query_id in (P4_01_QUERY_ID, P4_02_QUERY_ID)
            }
            with psycopg.connect(database_url, row_factory=dict_row) as connection:
                failed_rows = connection.execute(
                    """
                    SELECT manifest.outcome_replay_manifest_id,
                           manifest.status AS manifest_status,
                           manifest.error_message AS manifest_error,
                           attempt.status AS attempt_status,
                           attempt.error_message AS attempt_error
                    FROM systematic_fx.phase1a_outcome_replay_manifests AS manifest
                    JOIN systematic_fx.research_run_attempts AS attempt
                      ON attempt.research_run_attempt_id =
                         manifest.research_run_attempt_id
                    WHERE manifest.outcome_replay_manifest_id IN (%s, %s)
                    ORDER BY manifest.outcome_replay_manifest_id
                    """,
                    tuple(first_manifest_ids.values()),
                ).fetchall()
            if len(failed_rows) != 2 or any(
                row["manifest_status"] != "FAILED"
                or row["attempt_status"] != "FAILED"
                or row["manifest_error"] != failure_message
                or row["attempt_error"] != failure_message
                for row in failed_rows
            ):
                raise AssertionError(f"atomic P4 failure state drift: {failed_rows}")

            for query_id in (P4_01_QUERY_ID, P4_02_QUERY_ID):
                fixture = fixtures[query_id]
                retry = reserve_phase1a_outcome_replay(
                    database_url,
                    run_fingerprint=fixture["run_spec"].fingerprint,
                    source_artifact_manifest_sha256=fixture["source_sha256"],
                    query_id=query_id,
                )
                if (
                    not retry.execute
                    or not retry.created_manifest
                    or retry.outcome_replay_manifest_id == first_manifest_ids[query_id]
                ):
                    raise AssertionError(f"{query_id} failed pair did not create a retry")
                fixture["manifest_id"] = retry.outcome_replay_manifest_id
            pair = reserve_phase1a_p4_outcome_pair(
                database_url,
                p4_01_outcome_replay_manifest_id=p4_01["manifest_id"],
                p4_01_run_fingerprint=p4_01["run_spec"].fingerprint,
                p4_02_outcome_replay_manifest_id=p4_02["manifest_id"],
                p4_02_run_fingerprint=p4_02["run_spec"].fingerprint,
            )
            if not pair.created or pair.p4_pair_batch_id == first_batch_id:
                raise AssertionError("failed P4 batch did not bind a new retry batch")
            for query_id in (P4_01_QUERY_ID, P4_02_QUERY_ID):
                fixture = fixtures[query_id]
                start_phase1a_outcome_replay(
                    database_url,
                    outcome_replay_manifest_id=fixture["manifest_id"],
                    run_fingerprint=fixture["run_spec"].fingerprint,
                )
            print(
                "POSITIVE atomic pair failure=idempotent, "
                f"retry_batch={first_batch_id}->{pair.p4_pair_batch_id}"
            )

            for query_id in (P4_01_QUERY_ID, P4_02_QUERY_ID):
                print(f"BUILDING {query_id} checkpoint/detail fixture")
                build_detail_and_completion_artifacts(
                    database_url,
                    data_root,
                    fixture=fixtures[query_id],
                )
            exercise_direct_sql_negatives(
                database_url,
                fixtures,
                batch_id=pair.p4_pair_batch_id,
            )

            members = tuple(
                P4OutcomePairMember(
                    query_id=query_id,
                    outcome_replay_manifest_id=fixtures[query_id]["manifest_id"],
                    run_fingerprint=fixtures[query_id]["run_spec"].fingerprint,
                    cell_summaries=fixtures[query_id]["cells"],
                    result_artifact_path=fixtures[query_id]["result_path"],
                )
                for query_id in (P4_01_QUERY_ID, P4_02_QUERY_ID)
            )
            completed = complete_phase1a_p4_outcome_pair(
                database_url,
                p4_pair_batch_id=pair.p4_pair_batch_id,
                members=members,
                data_root=data_root,
            )
            if not completed.completed:
                raise AssertionError("initial P4 pair completion did not publish")
            loaded = load_phase1a_p4_outcome_pair_release(
                database_url,
                p4_01_outcome_replay_manifest_id=p4_01["manifest_id"],
                p4_01_run_fingerprint=p4_01["run_spec"].fingerprint,
                p4_02_outcome_replay_manifest_id=p4_02["manifest_id"],
                p4_02_run_fingerprint=p4_02["run_spec"].fingerprint,
                data_root=data_root,
            )
            repeated_completion = complete_phase1a_p4_outcome_pair(
                database_url,
                p4_pair_batch_id=pair.p4_pair_batch_id,
                members=members,
                data_root=data_root,
            )
            if repeated_completion.completed:
                raise AssertionError("repeated P4 completion was not idempotent")

            duplicate_reservations = {}
            for query_id in (P4_01_QUERY_ID, P4_02_QUERY_ID):
                fixture = fixtures[query_id]
                duplicate = reserve_phase1a_outcome_replay(
                    database_url,
                    run_fingerprint=fixture["run_spec"].fingerprint,
                    source_artifact_manifest_sha256=fixture["source_sha256"],
                    query_id=query_id,
                )
                if duplicate.execute:
                    raise AssertionError(f"{query_id} released duplicate tried to execute")
                duplicate_reservations[query_id] = duplicate
            duplicate_pair = reserve_phase1a_p4_outcome_pair(
                database_url,
                p4_01_outcome_replay_manifest_id=duplicate_reservations[
                    P4_01_QUERY_ID
                ].outcome_replay_manifest_id,
                p4_01_run_fingerprint=p4_01["run_spec"].fingerprint,
                p4_02_outcome_replay_manifest_id=duplicate_reservations[
                    P4_02_QUERY_ID
                ].outcome_replay_manifest_id,
                p4_02_run_fingerprint=p4_02["run_spec"].fingerprint,
            )
            if duplicate_pair.created or duplicate_pair.status != "RELEASED":
                raise AssertionError("released duplicate pair was not rehydrated")
            duplicate_loaded = load_phase1a_p4_outcome_pair_release(
                database_url,
                p4_01_outcome_replay_manifest_id=p4_01["manifest_id"],
                p4_01_run_fingerprint=p4_01["run_spec"].fingerprint,
                p4_02_outcome_replay_manifest_id=p4_02["manifest_id"],
                p4_02_run_fingerprint=p4_02["run_spec"].fingerprint,
                data_root=data_root,
            )
            late_fixtures: dict[str, dict[str, Any]] = {}
            for query_id in (P4_01_QUERY_ID, P4_02_QUERY_ID):
                nonce = f"post-release-mismatch:{query_id}:{os.getpid()}"
                source_sha256 = digest(f"source:{nonce}")
                late_fixture = build_input_fixture(
                    data_root,
                    query_id=query_id,
                    source_sha256=source_sha256,
                    nonce=nonce,
                )
                late_fixture["run_spec"] = build_run_spec(query_id, late_fixture, nonce)
                register_run_spec(database_url, late_fixture["run_spec"])
                late_reservation = reserve_phase1a_outcome_replay(
                    database_url,
                    run_fingerprint=late_fixture["run_spec"].fingerprint,
                    source_artifact_manifest_sha256=source_sha256,
                    query_id=query_id,
                )
                late_fixture["manifest_id"] = late_reservation.outcome_replay_manifest_id
                late_fixtures[query_id] = late_fixture
            late_p4_01 = late_fixtures[P4_01_QUERY_ID]
            late_p4_02 = late_fixtures[P4_02_QUERY_ID]
            try:
                reserve_phase1a_p4_outcome_pair(
                    database_url,
                    p4_01_outcome_replay_manifest_id=late_p4_01["manifest_id"],
                    p4_01_run_fingerprint=late_p4_01["run_spec"].fingerprint,
                    p4_02_outcome_replay_manifest_id=late_p4_02["manifest_id"],
                    p4_02_run_fingerprint=late_p4_02["run_spec"].fingerprint,
                )
            except OutcomeRegistryError as error:
                if "already released" not in str(error):
                    raise
                print(
                    f"NEGATIVE post-release different fingerprints: {type(error).__name__}: {error}"
                )
            else:
                raise AssertionError("post-release different P4 pair unexpectedly prepared")
            with psycopg.connect(database_url) as connection:
                connection.execute("SAVEPOINT released_batch")
                try:
                    connection.execute(
                        """
                        INSERT INTO systematic_fx.phase1a_p4_outcome_pair_batches
                            (pair_id,
                             p4_01_outcome_replay_manifest_id,
                             p4_02_outcome_replay_manifest_id,
                             p4_01_run_fingerprint,
                             p4_02_run_fingerprint,
                             pair_config_sha256,
                             p4_01_outcome_config_sha256,
                             p4_02_outcome_config_sha256,
                             p4_01_query_definition_sha256,
                             p4_02_query_definition_sha256,
                             p4_01_signal_manifest_sha256,
                             p4_02_signal_manifest_sha256,
                             p4_01_input_plan_sha256,
                             p4_02_input_plan_sha256,
                             prior_outcome_lineage,
                             prior_outcome_lineage_sha256)
                        SELECT pair_id, %s, %s, %s, %s,
                               pair_config_sha256,
                               p4_01_outcome_config_sha256,
                               p4_02_outcome_config_sha256,
                               p4_01_query_definition_sha256,
                               p4_02_query_definition_sha256,
                               p4_01_signal_manifest_sha256,
                               p4_02_signal_manifest_sha256,
                               p4_01_input_plan_sha256,
                               p4_02_input_plan_sha256,
                               prior_outcome_lineage,
                               prior_outcome_lineage_sha256
                        FROM systematic_fx.phase1a_p4_outcome_pair_batches
                        WHERE p4_pair_batch_id = %s
                        """,
                        (
                            late_p4_01["manifest_id"],
                            late_p4_02["manifest_id"],
                            late_p4_01["run_spec"].fingerprint,
                            late_p4_02["run_spec"].fingerprint,
                            pair.p4_pair_batch_id,
                        ),
                    )
                except psycopg.Error as error:
                    connection.execute("ROLLBACK TO SAVEPOINT released_batch")
                    if "already released" not in str(error):
                        raise
                    print(
                        f"NEGATIVE direct SQL post-release batch: {type(error).__name__}: {error}"
                    )
                else:
                    raise AssertionError("direct SQL post-release batch unexpectedly inserted")
                connection.rollback()
            for query_id in (P4_01_QUERY_ID, P4_02_QUERY_ID):
                late_fixture = late_fixtures[query_id]
                failed = fail_unpaired_phase1a_p4_outcome_replay(
                    database_url,
                    outcome_replay_manifest_id=late_fixture["manifest_id"],
                    run_fingerprint=late_fixture["run_spec"].fingerprint,
                    error_message="post-release singleton orphan cleanup",
                )
                if failed.status != "FAILED":
                    raise AssertionError("post-release unpaired cleanup failed")
            with psycopg.connect(database_url, row_factory=dict_row) as connection:
                counts = connection.execute(
                    """
                    SELECT
                        (SELECT count(*) FROM systematic_fx.phase1a_outcome_cell_summaries
                         WHERE outcome_replay_manifest_id IN (%s, %s)) AS cells,
                        (SELECT count(*) FROM systematic_fx.phase1a_outcome_screening_decisions
                         WHERE outcome_replay_manifest_id IN (%s, %s)) AS decisions,
                        (SELECT count(*) FROM systematic_fx.phase1a_p4_outcome_pair_releases
                         WHERE p4_pair_batch_id = %s) AS releases,
                        (SELECT count(*) FROM systematic_fx.phase1a_outcome_replay_manifests
                         WHERE outcome_replay_manifest_id IN (%s, %s)
                           AND status = 'SUCCEEDED') AS successes,
                        (SELECT count(*) FROM systematic_fx.artifacts AS artifact
                         JOIN systematic_fx.phase1a_outcome_replay_manifests AS manifest
                           ON manifest.result_artifact_id = artifact.artifact_id
                         WHERE manifest.outcome_replay_manifest_id IN (%s, %s)
                           AND artifact.artifact_type = %s) AS result_artifacts
                    """,
                    (
                        p4_01["manifest_id"],
                        p4_02["manifest_id"],
                        p4_01["manifest_id"],
                        p4_02["manifest_id"],
                        pair.p4_pair_batch_id,
                        p4_01["manifest_id"],
                        p4_02["manifest_id"],
                        p4_01["manifest_id"],
                        p4_02["manifest_id"],
                        OUTCOME_ARTIFACT_TYPE,
                    ),
                ).fetchone()
            expected_counts = {
                "cells": 5808,
                "decisions": 4,
                "releases": 1,
                "successes": 2,
                "result_artifacts": 2,
            }
            if counts != expected_counts:
                raise AssertionError(f"P4 release counts drift: {counts}")
            if not (
                loaded.pair_release_sha256
                == duplicate_loaded.pair_release_sha256
                == completed.release.pair_release_sha256
            ):
                raise AssertionError("P4 duplicate release identity drift")
            print(f"POSITIVE counts={counts}")
            print(
                "POSITIVE release="
                f"{completed.release.pair_release_sha256} duplicate={duplicate_pair}"
            )
    finally:
        with psycopg.connect(admin_url, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(database_name)))
    with psycopg.connect(admin_url) as connection:
        remaining = connection.execute(
            "SELECT count(*) FROM pg_database WHERE datname = %s",
            (database_name,),
        ).fetchone()[0]
    if remaining != 0:
        raise AssertionError("P4 lifecycle disposable database was not dropped")
    print("CLEANUP disposable_databases_remaining=0")


if __name__ == "__main__":
    main()


def test_p4_pair_full_atomic_release_gate() -> None:
    if os.environ.get("SYSTEMATIC_FX_RUN_P4_FULL_GATE") != "1":
        pytest.skip("set SYSTEMATIC_FX_RUN_P4_FULL_GATE=1 for the disposable P4 gate")
    main()
