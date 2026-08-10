from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import date, timedelta
from pathlib import Path

import pytest

import systematic_fx.research.bar_discovery as discovery_module
from systematic_fx.features.bars import TradeBarArtifactDescriptor
from systematic_fx.research.bar_artifacts import BarArtifactDriftError
from systematic_fx.research.bar_config import load_bar_pattern_config
from systematic_fx.research.bar_discovery import (
    ENTRY_FILLED,
    ENTRY_NOT_FILLED,
    BarDiscoveryPartition,
    iter_bar_discovery_cell_ledger,
    iter_bar_discovery_evidence_records,
    run_bar_pattern_discovery,
    run_streaming_bar_pattern_discovery,
)
from systematic_fx.validation.bar_splits import BarDateRange, BarSplitPlan


@dataclass(frozen=True, slots=True)
class TinyBar:
    timeframe_seconds: int
    segment_id: int
    contract: str
    source_date: date
    start_ns: int
    end_ns: int
    first_trade_ns: int
    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def _date_range(
    dates: tuple[date, ...],
    *,
    key: str,
    role: str,
    start: int,
    end: int,
    decision_end: int | None,
) -> BarDateRange:
    return BarDateRange(
        split_key=key,
        role=role,
        start_date=dates[start - 1],
        end_date=dates[end - 1],
        start_active_ordinal=start,
        end_active_ordinal=end,
        decision_end_date=None if decision_end is None else dates[decision_end - 1],
        result_visibility="VISIBLE" if role.startswith("DISCOVERY") else "SEALED",
    )


def _split_plan() -> BarSplitPlan:
    dates = tuple(date(2022, 1, 2) + timedelta(days=index) for index in range(8))
    discovery = _date_range(
        dates,
        key="discovery",
        role="DISCOVERY",
        start=1,
        end=4,
        decision_end=4,
    )
    blocks = tuple(
        _date_range(
            dates,
            key=f"discovery_block_{index}",
            role="DISCOVERY_REPORTING_BLOCK",
            start=index,
            end=index,
            decision_end=index,
        )
        for index in range(1, 5)
    )
    return BarSplitPlan(
        eligible_dates=dates,
        discovery=discovery,
        discovery_reporting_blocks=blocks,
        walk_forward_folds=(),
        embargo=_date_range(
            dates,
            key="embargo",
            role="EMBARGO",
            start=5,
            end=5,
            decision_end=None,
        ),
        holdout=_date_range(
            dates,
            key="holdout",
            role="HOLDOUT",
            start=6,
            end=6,
            decision_end=6,
        ),
        outcome_tail=_date_range(
            dates,
            key="outcome_tail",
            role="OUTCOME_TAIL",
            start=7,
            end=8,
            decision_end=None,
        ),
        canonical_bytes=b"tiny-split",
        sha256=_digest("tiny-split"),
    )


def _bar(
    *,
    timeframe: int,
    segment: int,
    source_date: date,
    start_seconds: int,
    open_ticks: int,
    bullish: bool,
) -> TinyBar:
    start_ns = start_seconds * 1_000_000_000
    close_ticks = open_ticks + 2 if bullish else open_ticks
    return TinyBar(
        timeframe_seconds=timeframe,
        segment_id=segment,
        contract="6EH2",
        source_date=source_date,
        start_ns=start_ns,
        end_ns=start_ns + timeframe * 1_000_000_000,
        first_trade_ns=start_ns,
        open_ticks=open_ticks,
        high_ticks=open_ticks + (2 if bullish else 1),
        low_ticks=open_ticks - (0 if bullish else 1),
        close_ticks=close_ticks,
    )


def _partitions_and_rows(split: BarSplitPlan):
    rows: dict[str, tuple[TinyBar, ...]] = {}
    partitions: list[BarDiscoveryPartition] = []
    required = (1, 60, 300, 1_800, 3_600)
    for ordinal, source_date in enumerate(split.eligible_dates[:5], start=1):
        by_timeframe: dict[int, list[TinyBar]] = {item: [] for item in required}
        one_second_by_start: dict[int, TinyBar] = {}
        signal_segment = ordinal * 10_000 + 1
        day_base = 1_641_081_600 + (ordinal - 1) * 86_400
        by_timeframe[60].append(
            _bar(
                timeframe=60,
                segment=signal_segment,
                source_date=source_date,
                start_seconds=day_base,
                open_ticks=20_000,
                bullish=False,
            )
        )
        one_second_by_start[day_base * 1_000_000_000] = _bar(
            timeframe=1,
            segment=signal_segment,
            source_date=source_date,
            start_seconds=day_base,
            open_ticks=20_000,
            bullish=False,
        )
        for timeframe in (300, 1_800, 3_600):
            if ordinal < 4:
                signal_bars = [
                    _bar(
                        timeframe=timeframe,
                        segment=signal_segment,
                        source_date=source_date,
                        start_seconds=day_base,
                        open_ticks=20_000,
                        bullish=False,
                    )
                ]
            else:
                offset = day_base + timeframe * 10
                signal_bars = [
                    _bar(
                        timeframe=timeframe,
                        segment=signal_segment,
                        source_date=source_date,
                        start_seconds=offset + index * timeframe,
                        open_ticks=20_000 + 2 * index,
                        bullish=index >= 33,
                    )
                    for index in range(36)
                ]
            by_timeframe[timeframe].extend(signal_bars)
            for bar in signal_bars:
                one_second_by_start.setdefault(
                    bar.start_ns,
                    _bar(
                        timeframe=1,
                        segment=signal_segment,
                        source_date=source_date,
                        start_seconds=bar.start_ns // 1_000_000_000,
                        open_ticks=bar.open_ticks,
                        bullish=bar.close_ticks > bar.open_ticks,
                    ),
                )
        by_timeframe[1] = [one_second_by_start[key] for key in sorted(one_second_by_start)]
        descriptors: list[TradeBarArtifactDescriptor] = []
        for timeframe in required:
            digest = _digest(f"{source_date}:{timeframe}")
            descriptors.append(
                TradeBarArtifactDescriptor(
                    timeframe_seconds=timeframe,
                    relative_uri=f"mock/{digest}.parquet",
                    sha256=digest,
                    byte_size=1,
                    row_count=len(by_timeframe[timeframe]),
                )
            )
            rows[digest] = tuple(by_timeframe[timeframe])
        partitions.append(
            BarDiscoveryPartition(
                source_date=source_date,
                contract="6EH2",
                outcome_span_id=ordinal,
                plan_sha256=_digest(f"plan:{source_date}"),
                source_sha256=_digest(f"source:{source_date}"),
                artifacts=tuple(descriptors),
            )
        )
    return tuple(partitions), rows


def test_discovery_runs_all_candidates_with_occupancy_and_never_loads_sealed_data(
    tmp_path: Path,
) -> None:
    split = _split_plan()
    partitions, rows = _partitions_and_rows(split)
    memory_plan = discovery_module._memory_plan_from_partitions(
        partitions,
        discovery_dates=split.eligible_dates[:4],
    )
    assert memory_plan.concurrent_cell_accumulator_count == 216 * 3 * 484
    assert memory_plan.maximum_buffered_match_record_count == 4_096
    assert memory_plan.maximum_buffered_replay_record_count == 256
    assert memory_plan.as_dict()["whole_discovery_rows_retained"] is False
    loaded_dates: list[date] = []

    def loader(_root, artifact, *, expected_source_date, **_kwargs):
        loaded_dates.append(expected_source_date)
        if expected_source_date > split.discovery.end_date:
            raise AssertionError("sealed data was opened")
        return rows[artifact.sha256]

    result = run_bar_pattern_discovery(
        partitions,
        split_plan=split,
        data_root=tmp_path,
        artifact_loader=loader,
    )

    assert len(result.candidate_results) == 216
    assert result.loaded_source_dates == split.eligible_dates[:4]
    assert set(loaded_dates) == set(split.eligible_dates[:4])
    assert split.eligible_dates[4] not in loaded_dates
    assert len(result.ranked_finalist_keys) <= 10
    target = next(
        item
        for item in result.candidate_results
        if item.candidate.candidate_key == "bpv1_tf0300_lb03_f1_long"
    )
    assert target.decision_trigger_count == (
        target.evaluated_count + target.context_not_evaluable_count
    )
    assert target.evaluated_count > 0
    assert target.failed_gate_counts
    assert [item.entry_status for item in target.matched_signals[-3:]] == [
        ENTRY_FILLED,
        ENTRY_FILLED,
        ENTRY_NOT_FILLED,
    ]
    filled = next(item for item in target.matched_signals if item.entry_status == ENTRY_FILLED)
    third_one_second = next(item for item in partitions[2].artifacts if item.timeframe_seconds == 1)
    entry_on_third_date = replace(
        filled,
        entry_1s_start_ns=rows[third_one_second.sha256][0].start_ns,
    )
    economic_block, economic_month = discovery_module._economic_attribution(
        entry_on_third_date,
        block_by_date={
            item.start_date: item.split_key for item in split.discovery_reporting_blocks
        },
    )
    assert economic_block == "discovery_block_3"
    assert economic_month == "2022-01"
    baseline = target.economics[0].cells[0]
    assert baseline.signal_count >= 3
    assert baseline.entry_fill_count >= 1
    assert baseline.skipped_occupied_count >= 1
    assert baseline.entry_not_filled_count >= 1
    assert any(cell.terminal_exit_count >= 1 for cell in target.economics[0].cells)
    assert len(target.economics) == 3
    assert all(len(item.cells) == 484 for item in target.economics)

    dispositions = [
        item.disposition
        for item in iter_bar_discovery_cell_ledger(
            result,
            candidate_key=target.candidate.candidate_key,
        )
        if item.scenario_id == "BASELINE"
        and item.take_profit_ticks == 24
        and item.stop_loss_ticks == 24
    ]
    assert dispositions[-3:] == [ENTRY_FILLED, "SKIPPED_OCCUPIED", ENTRY_NOT_FILLED]


def test_compact_replay_catalog_reconstructs_cells_without_embedding_484_rows(
    tmp_path: Path,
) -> None:
    split = _split_plan()
    partitions, rows = _partitions_and_rows(split)

    def loader(_root, artifact, **_kwargs):
        return rows[artifact.sha256]

    result = run_bar_pattern_discovery(
        partitions,
        split_plan=split,
        data_root=tmp_path,
        artifact_loader=loader,
    )

    bundle = result.replay_catalog[0]
    payload = bundle.as_dict()
    assert "cells" not in json.dumps(payload, sort_keys=True)
    assert len(json.dumps(payload, sort_keys=True)) < 100_000
    compact = bundle.scenarios[0]
    reconstructed = compact.to_surface()
    assert len(reconstructed.cells) == 484
    assert reconstructed.cell(24, 24).entry_fill_price_ticks == (compact.entry_fill_price_ticks)


def test_streaming_discovery_spools_contexts_and_compact_replays_under_data(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    split = _split_plan()
    partitions, rows = _partitions_and_rows(split)
    loaded_dates: list[date] = []
    progress_events = []

    def loader(_root, artifact, *, expected_source_date, **_kwargs):
        loaded_dates.append(expected_source_date)
        if expected_source_date > split.discovery.end_date:
            raise AssertionError("sealed data was opened")
        return rows[artifact.sha256]

    with pytest.raises(
        discovery_module.BarDiscoveryError,
        match="requires LoadedBarDatasetManifest",
    ):
        run_streaming_bar_pattern_discovery(
            partitions,
            split_plan=split,
            data_root=data_root,
            artifact_loader=loader,
        )

    result = discovery_module._run_streaming_bar_pattern_discovery(
        partitions,
        split_plan=split,
        data_root=data_root,
        artifact_loader=loader,
        progress=progress_events.append,
    )

    assert len(result.candidate_results) == 216
    assert result.replay_catalog == ()
    assert result.evidence_manifest is not None
    manifest = result.evidence_manifest
    assert (
        result.candidate_catalog_sha256
        == load_bar_pattern_config(Path.cwd()).candidate_catalog_sha256
    )
    assert manifest.candidate_catalog_sha256 == result.candidate_catalog_sha256
    assert manifest.matched_record_count == result.matched_signal_count
    assert manifest.replay_record_count > 0
    assert manifest.relative_uri.startswith("data/derived/bar_patterns/")
    assert set(loaded_dates) == set(split.eligible_dates[:4])
    assert progress_events[-1].stage == "COMPLETE"
    assert progress_events[-1].completed_active_dates == 4
    assert progress_events[-1].matched_signal_count == result.matched_signal_count
    assert not hasattr(progress_events[-1], "economics")
    target = next(
        item
        for item in result.candidate_results
        if item.candidate.candidate_key == "bpv1_tf0300_lb03_f1_long"
    )
    assert target.matched_signal_count >= 3
    assert target.matched_signals == ()
    baseline = target.economics[0].cells[0]
    assert baseline.skipped_occupied_count >= 1
    records = tuple(
        iter_bar_discovery_evidence_records(
            data_root,
            manifest,
            record_kind="matches",
            candidate_key=target.candidate.candidate_key,
        )
    )
    assert len(records) == target.matched_signal_count
    assert all(record["candidate_key"] == target.candidate.candidate_key for record in records)
    assert all("context" in record["evaluation"] for record in records)
    replay_records = tuple(
        iter_bar_discovery_evidence_records(
            data_root,
            manifest,
            record_kind="replays",
        )
    )
    fourth_one_second = next(
        item for item in partitions[3].artifacts if item.timeframe_seconds == 1
    )
    discovery_terminal_ns = max(item.start_ns for item in rows[fourth_one_second.sha256])
    assert replay_records
    assert all(
        scenario["terminal_1s_start_ns"] == discovery_terminal_ns
        for record in replay_records
        for scenario in record["bundle"]["scenarios"]
    )

    rerun = discovery_module._run_streaming_bar_pattern_discovery(
        partitions,
        split_plan=split,
        data_root=data_root,
        artifact_loader=loader,
    )
    assert rerun.evidence_manifest == manifest
    assert rerun.ranked_finalist_keys == result.ranked_finalist_keys

    manifest_path = manifest.artifact.path
    original_manifest_path = manifest_path.with_name(f"original-{manifest_path.name}")
    manifest_path.rename(original_manifest_path)
    manifest_path.symlink_to(original_manifest_path.name)
    with pytest.raises(BarArtifactDriftError):
        tuple(
            iter_bar_discovery_evidence_records(
                data_root,
                manifest,
                record_kind="matches",
            )
        )
    manifest_path.unlink()
    original_manifest_path.rename(manifest_path)

    shard_path = manifest.shards[0].artifact.path
    original_path = shard_path.with_name(f"original-{shard_path.name}")
    shard_path.rename(original_path)
    shard_path.symlink_to(original_path.name)
    with pytest.raises(BarArtifactDriftError):
        tuple(
            iter_bar_discovery_evidence_records(
                data_root,
                manifest,
                record_kind=manifest.shards[0].record_kind,
            )
        )


def test_streaming_waits_for_next_partition_before_flushing_a_continued_segment(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    split = _split_plan()
    partitions, original_rows = _partitions_and_rows(split)
    rows = dict(original_rows)
    third_date = split.eligible_dates[2]
    partitions = tuple(
        replace(item, outcome_span_id=3) if index == 3 else item
        for index, item in enumerate(partitions)
    )
    observed: list[tuple[tuple[int, str], set[date]]] = []

    def scan_spy(*, outcome_span_key, bars_by_timeframe, **_kwargs):
        observed.append(
            (
                outcome_span_key,
                {item.source_date for item in bars_by_timeframe[1]},
            )
        )
        return [], []

    monkeypatch.setattr(discovery_module, "_scan_streaming_outcome_span", scan_spy)

    def loader(_root, artifact, **_kwargs):
        return rows[artifact.sha256]

    discovery_module._run_streaming_bar_pattern_discovery(
        partitions,
        split_plan=split,
        data_root=data_root,
        artifact_loader=loader,
    )

    carried = [dates for key, dates in observed if key[0] == 3]
    assert carried == [{third_date, split.eligible_dates[3]}]
