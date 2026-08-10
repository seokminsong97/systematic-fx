from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from systematic_fx.backtest.barriers import BARRIER_TICKS, BarrierOutcome, Direction
from systematic_fx.backtest.economics import EntryStatus
from systematic_fx.backtest.event_cache import (
    _CACHE_FIELDS,
    CACHE_SCHEMA,
    CACHE_VERSION,
    DailyCacheReport,
)
from systematic_fx.backtest.shared_replay import (
    ReplayEventReference,
    ReplayResultRecord,
    SharedReplay,
)
from systematic_fx.db.outcome_registry import (
    CHECKPOINT_ARTIFACT_SCHEMA,
    OUTCOME_ARTIFACT_SCHEMA,
    OutcomeCellSummary,
)
from systematic_fx.research import outcome_artifacts as artifact_module
from systematic_fx.research.outcome_artifacts import (
    P5_OUTCOME_ARTIFACT_IDENTITY,
    OutcomeArtifactError,
    OutcomeArtifactIdentity,
    find_cache_manifest,
    load_cache_manifest,
    load_detail_shard,
    load_final_result_manifest,
    load_outcome_checkpoint,
    publish_cache_manifest,
    publish_detail_shard,
    publish_final_result_manifest,
    publish_outcome_checkpoint,
)

RUN_FINGERPRINT = "a" * 64
PLAN_SHA256 = "b" * 64
INPUT_SHA256 = "c" * 64
SOURCE_MANIFEST_SHA256 = "d" * 64
DAY = date(2024, 1, 2)
P1_IDENTITY = OutcomeArtifactIdentity(
    query_id="p1_05_unconfirmed_move_reversal",
    outcome_config_id="phase1a_p1_05_outcome_replay_v1",
    outcome_artifact_schema="systematic_fx.phase1a_p1_05_outcome_replay.v1",
    source_slice_count=99,
    source_occurrence_count=943,
    summary_row_count=2_904,
)


def _layout(root: Path) -> Path:
    data = root / "data"
    (data / "derived").mkdir(parents=True)
    (data / "mbp-10").mkdir()
    return data.resolve()


def _write_cache(data: Path) -> DailyCacheReport:
    source = data / "mbp-10/source.parquet"
    source.write_bytes(b"raw-source")
    source_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    metadata = {
        "cache_schema": CACHE_SCHEMA,
        "cache_version": CACHE_VERSION,
        "event_index_offset": 10,
        "instrument_id": 7,
        "raw_symbol": "6EH4",
        "source_date": DAY.isoformat(),
        "source_relative_uri": "mbp-10/source.parquet",
        "source_row_count": 1,
        "source_sha256": source_sha256,
    }
    schema = pa.schema(
        _CACHE_FIELDS,
        metadata={
            b"systematic_fx.cache": json.dumps(
                metadata,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        },
    )
    table = pa.Table.from_pylist(
        [
            {
                "event_index": 10,
                "ts_recv_ns": 1_704_153_600_000_000_000,
                "best_bid_ticks": 100,
                "best_ask_ticks": 101,
                "valid": True,
                "sequence": 1,
                "source_row_index": 0,
                "row_group_index": 0,
                "row_index": 0,
                "invalid_reason": None,
            }
        ],
        schema=schema,
    )
    directory = data / "derived/backtest_event_cache/phase1a_daily_executable_cache_v1"
    directory.mkdir(parents=True)
    temporary = directory / "temporary.parquet"
    pq.write_table(table, temporary, compression="zstd", version="2.6")
    payload = temporary.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    path = directory / f"sha256={digest}.parquet"
    temporary.rename(path)
    path.chmod(0o444)
    timestamp = 1_704_153_600_000_000_000
    return DailyCacheReport(
        path=path,
        sha256=digest,
        byte_size=len(payload),
        disposition="CREATED",
        source_date=DAY,
        source_path=str(source.resolve()),
        source_sha256=source_sha256,
        raw_symbol="6EH4",
        instrument_id=7,
        event_index_offset=10,
        source_row_count=1,
        cached_quote_count=1,
        valid_quote_count=1,
        first_event_index=10,
        last_event_index=10,
        first_ts_recv_ns=timestamp,
        last_ts_recv_ns=timestamp,
        last_valid_event_index=10,
        last_valid_ts_recv_ns=timestamp,
    )


def _reference() -> ReplayEventReference:
    return ReplayEventReference(
        contract_key="6EH4",
        source_date=DAY,
        session_ordinal=0,
        event_index=10,
        ts_recv_ns=1_704_153_600_000_000_000,
        best_bid_ticks=100,
        best_ask_ticks=101,
        valid=True,
    )


def _record() -> ReplayResultRecord:
    reference = _reference()
    return ReplayResultRecord(
        signal_id="signal-1",
        decision_ts_recv_ns=reference.ts_recv_ns,
        utc_month="2024-01",
        scenario_id="BASELINE",
        direction=Direction.LONG,
        contract_key="6EH4",
        cell_id="tp24_sl24",
        take_profit_ticks=24,
        stop_loss_ticks=24,
        entry_status=EntryStatus.ENTRY_FILLED,
        entry_eligibility_ts_recv_ns=reference.ts_recv_ns + 1_000_000_000,
        entry_fill_price_ticks=101,
        buying_price_ticks=101,
        selling_price_ticks=125,
        loss_price_ticks=77,
        take_profit_target_price_ticks=125,
        stop_trigger_price_ticks=77,
        first_touch_outcome=BarrierOutcome.TP_FIRST,
        portfolio_outcome=BarrierOutcome.TP_FIRST,
        exit_fill_price_ticks=125,
        decision_ref=reference,
        eligibility_ref=reference,
        attempt_ref=reference,
        entry_ref=reference,
        trigger_ref=reference,
        fill_ref=reference,
        first_touch_censor_ref=None,
        terminal_ref=None,
        entry_limit_price_ticks=101,
        route_event_count=1,
        maximum_route_quote_gap_ns=0,
        failure_ref=None,
        occupying_signal_id=None,
        no_fill_reason=None,
        completion_ts_recv_ns=reference.ts_recv_ns + 2_000_000_000,
    )


def _summaries(
    *,
    long_signal_count: int = 529,
    short_signal_count: int = 582,
) -> tuple[OutcomeCellSummary, ...]:
    return tuple(
        OutcomeCellSummary(
            scenario_id=scenario,
            direction=direction,
            take_profit_ticks=take_profit,
            stop_loss_ticks=stop_loss,
            signal_count=(long_signal_count if direction == "LONG" else short_signal_count),
            entry_fill_count=0,
            entry_not_filled_count=(
                long_signal_count if direction == "LONG" else short_signal_count
            ),
            skipped_occupied_count=0,
            take_profit_first_count=0,
            stop_first_count=0,
            terminal_exit_count=0,
            censored_count=0,
            gross_pnl_ticks=0,
            variable_cost_ticks=0,
            allocated_fixed_cost_ticks=0,
            fully_loaded_net_pnl_ticks=0,
            fully_loaded_net_ev_ticks=None,
            fully_loaded_net_pnl_usd=Decimal(0),
            calendar_month_net_pnl_usd=Decimal(0),
            profit_factor=None,
            maximum_drawdown_usd=Decimal(0),
            complete=True,
        )
        for scenario in ("BASELINE", "MODERATE_COMBINED", "SEVERE_DIAGNOSTIC")
        for direction in ("LONG", "SHORT")
        for take_profit in BARRIER_TICKS
        for stop_loss in BARRIER_TICKS
    )


class OutcomeDetailArtifactTests(unittest.TestCase):
    def test_populated_and_empty_detail_shards_are_deterministic_and_lossless(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data = _layout(Path(directory))
            populated = publish_detail_shard(
                [_record()],
                data_root=data,
                run_fingerprint=RUN_FINGERPRINT,
                shard_sequence=1,
                source_date=DAY,
            )
            reused = publish_detail_shard(
                [_record()],
                data_root=data,
                run_fingerprint=RUN_FINGERPRINT,
                shard_sequence=1,
                source_date=DAY,
            )
            empty = publish_detail_shard(
                [],
                data_root=data,
                run_fingerprint=RUN_FINGERPRINT,
                shard_sequence=2,
                source_date=date(2024, 1, 3),
            )

            loaded = load_detail_shard(populated, data_root=data)
            loaded_empty = load_detail_shard(empty, data_root=data)

            self.assertEqual(loaded.records, (_record(),))
            self.assertEqual(loaded.records[0].buying_price_ticks, 101)
            self.assertEqual(loaded.records[0].selling_price_ticks, 125)
            self.assertEqual(loaded.records[0].loss_price_ticks, 77)
            self.assertEqual(loaded.records[0].decision_ref, _reference())
            self.assertEqual(loaded_empty.records, ())
            self.assertEqual(populated.sha256, reused.sha256)
            self.assertEqual(reused.disposition, "REUSED")
            self.assertTrue(populated.path.is_relative_to(data / "derived"))
            self.assertEqual(populated.path.stat().st_mode & 0o222, 0)

    def test_candidate_identity_is_registry_bound_and_uses_a_distinct_namespace(self) -> None:
        with self.assertRaisesRegex(OutcomeArtifactError, "registry query profile"):
            OutcomeArtifactIdentity(
                query_id=P1_IDENTITY.query_id,
                outcome_config_id=P5_OUTCOME_ARTIFACT_IDENTITY.outcome_config_id,
                outcome_artifact_schema=P1_IDENTITY.outcome_artifact_schema,
                source_slice_count=P1_IDENTITY.source_slice_count,
                source_occurrence_count=P1_IDENTITY.source_occurrence_count,
                summary_row_count=P1_IDENTITY.summary_row_count,
            )

        with tempfile.TemporaryDirectory() as directory:
            data = _layout(Path(directory))
            p5 = publish_detail_shard(
                [],
                data_root=data,
                run_fingerprint=RUN_FINGERPRINT,
                shard_sequence=1,
                source_date=DAY,
            )
            p1 = publish_detail_shard(
                [],
                data_root=data,
                run_fingerprint=RUN_FINGERPRINT,
                shard_sequence=1,
                source_date=DAY,
                identity=P1_IDENTITY,
            )

            self.assertEqual(p1.sha256, p5.sha256)
            self.assertEqual(
                p1.path.parent.relative_to(data / "derived").as_posix(),
                "outcomes/phase1a_p1_05_outcome_replay_v1/detail_shards",
            )
            self.assertNotEqual(p1.path, p5.path)
            self.assertEqual(
                load_detail_shard(p1, data_root=data, identity=P1_IDENTITY).records,
                (),
            )
            with self.assertRaisesRegex(OutcomeArtifactError, "candidate namespace"):
                load_detail_shard(p1, data_root=data)

    def test_detail_load_rejects_permission_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data = _layout(Path(directory))
            artifact = publish_detail_shard(
                [_record()],
                data_root=data,
                run_fingerprint=RUN_FINGERPRINT,
                shard_sequence=1,
                source_date=DAY,
            )
            artifact.path.chmod(0o644)
            with self.assertRaisesRegex(OutcomeArtifactError, "read-only"):
                load_detail_shard(artifact, data_root=data)

    def test_publish_link_fails_closed_when_output_path_is_swapped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = _layout(root)
            output = data / "derived/outcomes/phase1a_p5_outcome_replay_v1/detail_shards"
            original_output = output.with_name("detail_shards.original")
            attacker_output = root / "attacker-publish-directory"
            attacker_output.mkdir()
            real_link = artifact_module.os.link
            swapped = False

            def swap_then_link(source: str, target: str, **kwargs: object) -> None:
                nonlocal swapped
                output.rename(original_output)
                output.symlink_to(attacker_output, target_is_directory=True)
                swapped = True
                real_link(source, target, **kwargs)

            with (
                patch.object(artifact_module.os, "link", side_effect=swap_then_link),
                self.assertRaisesRegex(OutcomeArtifactError, "contains a symlink"),
            ):
                publish_detail_shard(
                    [_record()],
                    data_root=data,
                    run_fingerprint=RUN_FINGERPRINT,
                    shard_sequence=1,
                    source_date=DAY,
                )
            self.assertTrue(swapped)
            self.assertEqual(tuple(attacker_output.iterdir()), ())


class OutcomeManifestTests(unittest.TestCase):
    def test_p1_checkpoint_and_final_manifest_roundtrip_use_p1_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data = _layout(Path(directory))
            report = _write_cache(data)
            cache = publish_cache_manifest(
                [report],
                data_root=data,
                cache_plan_sha256=PLAN_SHA256,
                input_manifest_sha256=INPUT_SHA256,
            )
            shard = publish_detail_shard(
                [],
                data_root=data,
                run_fingerprint=RUN_FINGERPRINT,
                shard_sequence=1,
                source_date=DAY,
                identity=P1_IDENTITY,
            )
            replay = SharedReplay([])
            replay.complete_source_date(DAY)
            replay.finish()
            self.assertEqual(replay.drain_result_records(), ())
            checkpoint = publish_outcome_checkpoint(
                data_root=data,
                outcome_replay_manifest_id=8,
                run_fingerprint=RUN_FINGERPRINT,
                checkpoint_sequence=1,
                completed_source_date_count=1,
                last_completed_source_date=DAY,
                source_event_count=0,
                predecessor_checkpoint_sha256=None,
                replay_state=replay.checkpoint(),
                detail_shards=[shard],
                cache_manifest=cache,
                input_lineage={"input_manifest_sha256": INPUT_SHA256},
                identity=P1_IDENTITY,
            )
            loaded_checkpoint = load_outcome_checkpoint(
                checkpoint,
                data_root=data,
                identity=P1_IDENTITY,
            )
            final = publish_final_result_manifest(
                data_root=data,
                run_fingerprint=RUN_FINGERPRINT,
                source_artifact_manifest_sha256=SOURCE_MANIFEST_SHA256,
                cell_summaries=_summaries(
                    long_signal_count=446,
                    short_signal_count=497,
                ),
                detail_shards=[shard],
                cache_manifest=cache,
                input_lineage={"input_manifest_sha256": INPUT_SHA256},
                final_checkpoint=loaded_checkpoint,
                identity=P1_IDENTITY,
            )
            loaded_final = load_final_result_manifest(
                final,
                data_root=data,
                verify_cache_content=False,
                identity=P1_IDENTITY,
            )

            self.assertEqual(
                checkpoint.path.parent.relative_to(data / "derived").as_posix(),
                "outcomes/checkpoints/phase1a_p1_05_outcome_replay_v1",
            )
            self.assertEqual(
                final.path.parent.relative_to(data / "derived").as_posix(),
                "outcomes/phase1a_p1_05_outcome_replay_v1",
            )
            self.assertEqual(loaded_final.document["query_id"], P1_IDENTITY.query_id)
            self.assertEqual(
                loaded_final.document["outcome_config_id"],
                P1_IDENTITY.outcome_config_id,
            )
            self.assertEqual(
                loaded_final.document["artifact_schema"],
                P1_IDENTITY.outcome_artifact_schema,
            )
            self.assertEqual(loaded_final.document["source_occurrence_count"], 943)
            self.assertEqual(loaded_final.document["source_slice_count"], 99)
            with self.assertRaisesRegex(OutcomeArtifactError, "outcome config drift"):
                load_outcome_checkpoint(checkpoint, data_root=data)
            with self.assertRaisesRegex(OutcomeArtifactError, "registry field drift"):
                load_final_result_manifest(final, data_root=data)

    def test_lineage_only_checkpoint_rejects_same_size_detail_bit_flip(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data = _layout(Path(directory))
            report = _write_cache(data)
            cache = publish_cache_manifest(
                [report],
                data_root=data,
                cache_plan_sha256=PLAN_SHA256,
                input_manifest_sha256=INPUT_SHA256,
            )
            shard = publish_detail_shard(
                [],
                data_root=data,
                run_fingerprint=RUN_FINGERPRINT,
                shard_sequence=1,
                source_date=DAY,
            )
            replay = SharedReplay([])
            replay.complete_source_date(DAY)
            replay.finish()
            self.assertEqual(replay.drain_result_records(), ())
            checkpoint = publish_outcome_checkpoint(
                data_root=data,
                outcome_replay_manifest_id=7,
                run_fingerprint=RUN_FINGERPRINT,
                checkpoint_sequence=1,
                completed_source_date_count=1,
                last_completed_source_date=DAY,
                source_event_count=0,
                predecessor_checkpoint_sha256=None,
                replay_state=replay.checkpoint(),
                detail_shards=[shard],
                cache_manifest=cache,
                input_lineage={"input_manifest_sha256": INPUT_SHA256},
            )

            payload = bytearray(shard.path.read_bytes())
            payload[len(payload) // 2] ^= 0x01
            replacement = shard.path.with_name(".same-size-tampered.parquet")
            replacement.write_bytes(payload)
            replacement.chmod(0o444)
            replacement.replace(shard.path)
            self.assertEqual(shard.path.stat().st_size, shard.byte_size)

            with self.assertRaisesRegex(OutcomeArtifactError, "SHA-256 drift"):
                load_outcome_checkpoint(
                    checkpoint,
                    data_root=data,
                    expected_progress_metadata=checkpoint.progress_metadata,
                    verify_detail_content=False,
                    retain_detail_records=False,
                )
            with self.assertRaisesRegex(OutcomeArtifactError, "SHA-256 drift"):
                publish_final_result_manifest(
                    data_root=data,
                    run_fingerprint=RUN_FINGERPRINT,
                    source_artifact_manifest_sha256=SOURCE_MANIFEST_SHA256,
                    cell_summaries=_summaries(),
                    detail_shards=[shard],
                    cache_manifest=cache,
                    input_lineage={"input_manifest_sha256": INPUT_SHA256},
                    final_checkpoint=checkpoint,
                )

    def test_artifact_open_rejects_parent_symlink_swap_after_path_inspection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = _layout(root)
            artifact = publish_detail_shard(
                [_record()],
                data_root=data,
                run_fingerprint=RUN_FINGERPRINT,
                shard_sequence=1,
                source_date=DAY,
            )
            victim_directory = artifact.path.parent
            original_directory = victim_directory.with_name("detail_shards.original")
            attacker_directory = root / "attacker-detail-shards"
            attacker_directory.mkdir()
            attacker_artifact = attacker_directory / artifact.path.name
            attacker_artifact.write_bytes(artifact.path.read_bytes())
            attacker_artifact.chmod(0o444)
            original_artifact_path = artifact_module._artifact_path
            swapped = False

            def inspect_then_swap(path: Path, *, data_root: Path | str):
                nonlocal swapped
                location = original_artifact_path(path, data_root=data_root)
                if not swapped:
                    victim_directory.rename(original_directory)
                    victim_directory.symlink_to(attacker_directory, target_is_directory=True)
                    swapped = True
                return location

            with (
                patch.object(
                    artifact_module,
                    "_artifact_path",
                    side_effect=inspect_then_swap,
                ),
                self.assertRaisesRegex(
                    OutcomeArtifactError,
                    "securely open artifact directory component|identity changed",
                ),
            ):
                load_detail_shard(artifact, data_root=data)
            self.assertTrue(swapped)

    def test_cache_publish_find_load_roundtrip_retains_every_source_bound(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data = _layout(Path(directory))
            report = _write_cache(data)
            published = publish_cache_manifest(
                [report],
                data_root=data,
                cache_plan_sha256=PLAN_SHA256,
                input_manifest_sha256=INPUT_SHA256,
            )

            loaded = load_cache_manifest(published, data_root=data)
            found = find_cache_manifest(
                data_root=data,
                cache_plan_sha256=PLAN_SHA256,
                input_manifest_sha256=INPUT_SHA256,
            )

            self.assertIsNotNone(found)
            assert found is not None
            self.assertEqual(loaded.reports, found.reports)
            self.assertEqual(found.path, published.path)
            self.assertEqual(found.sha256, published.sha256)
            self.assertEqual(found.reports[0].first_event_index, 10)
            self.assertEqual(found.reports[0].last_event_index, 10)
            self.assertEqual(found.reports[0].first_ts_recv_ns, report.first_ts_recv_ns)
            self.assertEqual(found.reports[0].last_ts_recv_ns, report.last_ts_recv_ns)
            self.assertEqual(found.reports[0].last_valid_event_index, 10)
            self.assertEqual(found.reports[0].last_valid_ts_recv_ns, report.last_ts_recv_ns)
            self.assertEqual(
                found.document["cache_entries"][0]["artifact_relative_uri"],
                report.path.relative_to(data / "derived").as_posix(),
            )

    def test_finished_checkpoint_and_final_manifest_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data = _layout(Path(directory))
            report = _write_cache(data)
            cache = publish_cache_manifest(
                [report],
                data_root=data,
                cache_plan_sha256=PLAN_SHA256,
                input_manifest_sha256=INPUT_SHA256,
            )
            shard = publish_detail_shard(
                [],
                data_root=data,
                run_fingerprint=RUN_FINGERPRINT,
                shard_sequence=1,
                source_date=DAY,
            )
            replay = SharedReplay([])
            replay.complete_source_date(DAY)
            replay.finish()
            self.assertEqual(replay.drain_result_records(), ())
            checkpoint = publish_outcome_checkpoint(
                data_root=data,
                outcome_replay_manifest_id=7,
                run_fingerprint=RUN_FINGERPRINT,
                checkpoint_sequence=1,
                completed_source_date_count=1,
                last_completed_source_date=DAY,
                source_event_count=0,
                predecessor_checkpoint_sha256=None,
                replay_state=replay.checkpoint(),
                detail_shards=[shard],
                cache_manifest=cache,
                input_lineage={"input_manifest_sha256": INPUT_SHA256},
            )

            loaded_checkpoint = load_outcome_checkpoint(
                checkpoint,
                data_root=data,
                expected_progress_metadata=checkpoint.progress_metadata,
            )

            self.assertTrue(loaded_checkpoint.replay.finished)
            self.assertEqual(len(loaded_checkpoint.loaded_detail_shards), 1)
            self.assertEqual(loaded_checkpoint.loaded_detail_shards[0].records, ())
            self.assertEqual(
                loaded_checkpoint.document["artifact_schema"],
                CHECKPOINT_ARTIFACT_SCHEMA,
            )
            self.assertTrue(
                checkpoint.path.is_relative_to(
                    data / "derived/outcomes/checkpoints/phase1a_p5_outcome_replay_v1"
                )
            )

            with patch(
                "systematic_fx.research.outcome_artifacts.load_detail_shard",
                side_effect=AssertionError("detail content must not be loaded"),
            ) as detail_loader:
                lineage_only = load_outcome_checkpoint(
                    checkpoint,
                    data_root=data,
                    expected_progress_metadata=checkpoint.progress_metadata,
                    verify_detail_content=False,
                    retain_detail_records=False,
                )
            detail_loader.assert_not_called()
            self.assertEqual(lineage_only.loaded_detail_shards, ())

            with patch(
                "systematic_fx.research.outcome_artifacts.load_detail_shard",
                wraps=load_detail_shard,
            ) as bounded_detail_loader:
                bounded = load_outcome_checkpoint(
                    checkpoint,
                    data_root=data,
                    expected_progress_metadata=checkpoint.progress_metadata,
                    retain_detail_records=False,
                )
            self.assertEqual(bounded_detail_loader.call_count, 1)
            self.assertEqual(bounded.loaded_detail_shards, ())

            with (
                patch(
                    "systematic_fx.research.outcome_artifacts.load_detail_shard",
                    side_effect=AssertionError("final publication must not rescan detail"),
                ) as publish_detail_loader,
                patch(
                    "systematic_fx.research.outcome_artifacts.load_cache_manifest",
                    side_effect=AssertionError("final publication must not rescan cache"),
                ) as publish_cache_loader,
            ):
                final = publish_final_result_manifest(
                    data_root=data,
                    run_fingerprint=RUN_FINGERPRINT,
                    source_artifact_manifest_sha256=SOURCE_MANIFEST_SHA256,
                    cell_summaries=_summaries(),
                    detail_shards=[shard],
                    cache_manifest=cache,
                    input_lineage={"input_manifest_sha256": INPUT_SHA256},
                    final_checkpoint=loaded_checkpoint,
                )
            publish_detail_loader.assert_not_called()
            publish_cache_loader.assert_not_called()

            with (
                patch(
                    "systematic_fx.research.outcome_artifacts.load_detail_shard",
                    wraps=load_detail_shard,
                ) as final_detail_loader,
                patch(
                    "systematic_fx.research.outcome_artifacts.read_daily_executable_cache",
                    side_effect=AssertionError("cache rows must not be decoded again"),
                ) as final_cache_reader,
            ):
                loaded_final = load_final_result_manifest(
                    final,
                    data_root=data,
                    verify_cache_content=False,
                )
            self.assertEqual(final_detail_loader.call_count, 1)
            final_cache_reader.assert_not_called()
            self.assertEqual(loaded_final.final_checkpoint.loaded_detail_shards, ())

            self.assertEqual(len(loaded_final.cell_summaries), 2_904)
            self.assertEqual(loaded_final.document["artifact_schema"], OUTCOME_ARTIFACT_SCHEMA)
            self.assertEqual(loaded_final.detail_shards[0].as_dict(), shard.as_dict())
            self.assertTrue(loaded_final.final_checkpoint.replay.finished)
            self.assertEqual(final.final_checkpoint_sha256, checkpoint.sha256)
            self.assertEqual(final.final_checkpoint_sequence, 1)
            self.assertEqual(
                loaded_final.document["final_checkpoint"]["artifact_sha256"],
                checkpoint.sha256,
            )
            self.assertEqual(final.summary_row_count, 2_904)
            self.assertTrue(final.path.is_relative_to(data / "derived"))


if __name__ == "__main__":
    unittest.main()
