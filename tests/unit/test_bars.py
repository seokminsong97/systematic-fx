from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from systematic_fx.data.contracts import UNDEFINED_PRICE
from systematic_fx.data.instruments import InstrumentKind, InstrumentMapping
from systematic_fx.features import bars as bars_module
from systematic_fx.features.bars import (
    BAR_VERSION,
    QC_EXCLUDED_SOURCE_DATES,
    TICK_SIZE_RAW,
    BarArtifact,
    DailyPlanStatus,
    NextBarLinkStatus,
    TradeBar,
    TradeBarArtifactDescriptor,
    TradeBarError,
    TradePrint,
    build_daily_trade_bar_artifacts,
    build_one_second_trade_bars,
    link_next_bars,
    load_trade_bar_artifact,
    make_daily_volume_summary,
    plan_daily_trade_bars,
    resample_trade_bars,
    segment_tail,
)


def _sha(character: str) -> str:
    return character * 64


def _ns(day: date, seconds: float = 0.0) -> int:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    return int(start.timestamp() * 1_000_000_000 + seconds * 1_000_000_000)


def _mapping(
    instrument_id: int,
    symbol: str,
    *,
    interval_start: date = date(2022, 1, 1),
    interval_end: date = date(2022, 12, 31),
) -> InstrumentMapping:
    return InstrumentMapping(
        instrument_id=instrument_id,
        raw_symbol=symbol,
        kind=InstrumentKind.OUTRIGHT,
        interval_start=interval_start,
        interval_end=interval_end,
    )


def _previous_summary(*, zero: bool = False):
    mappings = (_mapping(1, "6EH2"), _mapping(2, "6EM2"))
    return make_daily_volume_summary(
        source_date=date(2022, 1, 31),
        source_sha256=_sha("a"),
        mappings=mappings,
        totals_by_instrument={1: (0, 0) if zero else (10, 100), 2: (5, 50)},
    )


def _print(
    day: date,
    seconds: float,
    ticks: int,
    *,
    sequence: int,
    ordinal: int,
    size: int = 1,
    side: str = "N",
) -> TradePrint:
    return TradePrint(
        ts_recv_ns=_ns(day, seconds),
        sequence=sequence,
        physical_ordinal=ordinal,
        price_raw=ticks * TICK_SIZE_RAW,
        size=size,
        side=side,
    )


def test_plan_uses_only_previous_eligible_volume_and_has_stable_hash() -> None:
    mappings = (_mapping(2, "6EM2"), _mapping(1, "6EH2"))
    previous = _previous_summary()

    plan = plan_daily_trade_bars(
        source_date=date(2022, 2, 1),
        source_sha256=_sha("b"),
        mappings=mappings,
        previous_volume_summary=previous,
    )
    same = plan_daily_trade_bars(
        source_date=date(2022, 2, 1),
        source_sha256=_sha("b"),
        mappings=tuple(reversed(mappings)),
        previous_volume_summary=previous,
    )

    assert plan.status is DailyPlanStatus.SELECTED
    assert plan.selected_instrument_id == 1
    assert plan.selected_contract == "6EH2"
    assert plan.selected_previous_trade_count == 10
    assert plan.selected_previous_volume == 100
    assert plan.previous_source_date == date(2022, 1, 31)
    assert plan.sha256 == same.sha256
    assert plan.as_dict()["sha256"] == plan.sha256


def test_plan_fail_closed_qc_seed_and_zero_previous_volume() -> None:
    mappings = (_mapping(1, "6EH2"), _mapping(2, "6EM2"))

    seed = plan_daily_trade_bars(
        source_date=date(2022, 2, 1),
        source_sha256=_sha("b"),
        mappings=mappings,
        previous_volume_summary=None,
    )
    zero = plan_daily_trade_bars(
        source_date=date(2022, 2, 1),
        source_sha256=_sha("b"),
        mappings=mappings,
        previous_volume_summary=_previous_summary(zero=True),
    )
    excluded = plan_daily_trade_bars(
        source_date=date(2024, 6, 30),
        source_sha256=_sha("c"),
        mappings=(),
        previous_volume_summary=None,
    )

    assert seed.status is DailyPlanStatus.NO_PREVIOUS_ELIGIBLE_SOURCE
    # June still has positive evidence, so zero March evidence must not force a
    # zero-volume nearest-contract tie.
    assert zero.status is DailyPlanStatus.SELECTED
    assert zero.selected_contract == "6EM2"
    assert excluded.status is DailyPlanStatus.QC_EXCLUDED
    assert date(2024, 6, 30) in QC_EXCLUDED_SOURCE_DATES

    all_zero = make_daily_volume_summary(
        source_date=date(2022, 1, 31),
        source_sha256=_sha("d"),
        mappings=mappings,
        totals_by_instrument={1: (0, 0), 2: (0, 0)},
    )
    rejected = plan_daily_trade_bars(
        source_date=date(2022, 2, 1),
        source_sha256=_sha("b"),
        mappings=mappings,
        previous_volume_summary=all_zero,
    )
    assert rejected.status is DailyPlanStatus.NO_POSITIVE_PREVIOUS_VOLUME
    assert rejected.selected_contract is None


def test_one_second_bars_use_canonical_order_and_half_open_intervals() -> None:
    day = date(2022, 2, 1)
    prints = (
        _print(day, 0.9, 101, sequence=3, ordinal=2, size=5, side="N"),
        _print(day, 0.1, 100, sequence=2, ordinal=1, size=3, side="B"),
        _print(day, 0.1, 99, sequence=1, ordinal=0, size=2, side="A"),
        _print(day, 1.0, 102, sequence=4, ordinal=3, size=7, side="B"),
    )

    bars = build_one_second_trade_bars(prints, contract="6EH2", source_date=day)

    assert len(bars) == 2
    first, second = bars
    assert (first.start_ns, first.end_ns) == (_ns(day), _ns(day, 1))
    assert (first.open_ticks, first.high_ticks, first.low_ticks, first.close_ticks) == (
        99,
        101,
        99,
        101,
    )
    assert (first.trade_count, first.volume) == (3, 10)
    assert (first.buy_volume, first.sell_volume) == (3, 2)
    assert second.start_ns == first.end_ns
    assert first.segment_id == second.segment_id

    links = link_next_bars(bars)
    assert links[0].status is NextBarLinkStatus.EXACT_NEXT_BAR
    assert links[0].next_bar_start_ns == second.start_ns
    assert links[0].next_first_trade_ns == second.first_trade_ns
    assert links[1].status is NextBarLinkStatus.PARTITION_END


def test_gap_segments_and_linkage_are_explicit() -> None:
    day = date(2022, 2, 1)
    bars = build_one_second_trade_bars(
        (
            _print(day, 0.1, 100, sequence=1, ordinal=0),
            _print(day, 3_601.1, 101, sequence=2, ordinal=1),
        ),
        contract="6EH2",
        source_date=day,
    )

    assert bars[0].segment_id != bars[1].segment_id
    links = link_next_bars(bars)
    assert links[0].status is NextBarLinkStatus.SEGMENT_BOUNDARY
    assert links[0].next_bar_start_ns == bars[1].start_ns

    short_gap = build_one_second_trade_bars(
        (
            _print(day, 0.1, 100, sequence=1, ordinal=0),
            _print(day, 2.1, 101, sequence=2, ordinal=1),
        ),
        contract="6EH2",
        source_date=day,
    )
    assert short_gap[0].segment_id == short_gap[1].segment_id
    assert link_next_bars(short_gap)[0].status is NextBarLinkStatus.GAP


def test_segment_continues_across_source_dates_and_breaks_on_contract() -> None:
    from systematic_fx.backtest.bar_replay import BarPathIndex

    first_day = date(2022, 2, 1)
    second_day = date(2022, 2, 2)
    prior = build_one_second_trade_bars(
        (_print(first_day, 86_399.1, 100, sequence=1, ordinal=0),),
        contract="6EH2",
        source_date=first_day,
    )
    tail = segment_tail(prior)
    assert tail is not None

    continued = build_one_second_trade_bars(
        (_print(second_day, 0.1, 101, sequence=1, ordinal=0),),
        contract="6EH2",
        source_date=second_day,
        previous_segment_tail=tail,
    )
    switched = build_one_second_trade_bars(
        (_print(second_day, 0.1, 101, sequence=1, ordinal=0),),
        contract="6EM2",
        source_date=second_day,
        previous_segment_tail=tail,
    )

    assert isinstance(prior[0].segment_id, int)
    assert continued[0].segment_id == prior[0].segment_id
    assert switched[0].segment_id != prior[0].segment_id
    cross_partition_links = link_next_bars((*prior, *continued))
    assert cross_partition_links[0].status is NextBarLinkStatus.EXACT_NEXT_BAR
    assert cross_partition_links[1].status is NextBarLinkStatus.PARTITION_END
    assert BarPathIndex((*prior, *continued)).segment_id == prior[0].segment_id


def test_resampling_is_associative_and_preserves_observed_seconds() -> None:
    day = date(2022, 2, 1)
    seconds = build_one_second_trade_bars(
        (
            _print(day, 0.1, 100, sequence=1, ordinal=0, size=2, side="B"),
            _print(day, 59.9, 101, sequence=2, ordinal=1, size=3, side="A"),
            _print(day, 60.1, 99, sequence=3, ordinal=2, size=5, side="B"),
            _print(day, 299.9, 105, sequence=4, ordinal=3, size=7, side="N"),
        ),
        contract="6EH2",
        source_date=day,
    )
    direct = resample_trade_bars(seconds, timeframe_seconds=300)
    minutes = resample_trade_bars(seconds, timeframe_seconds=60)
    staged = resample_trade_bars(minutes, timeframe_seconds=300)

    assert [bar.as_dict() for bar in direct] == [bar.as_dict() for bar in staged]
    assert len(direct) == 1
    assert direct[0].observed_subbars == 4
    assert direct[0].trade_count == 4
    assert direct[0].volume == 17
    assert (direct[0].open_ticks, direct[0].high_ticks) == (100, 105)
    assert (direct[0].low_ticks, direct[0].close_ticks) == (99, 105)


def test_trade_and_bar_validation_rejects_undefined_off_tick_null_and_duplicates() -> None:
    day = date(2022, 2, 1)
    common = {
        "ts_recv_ns": _ns(day, 0.1),
        "sequence": 1,
        "physical_ordinal": 0,
        "size": 1,
        "side": "B",
    }
    with pytest.raises(TradeBarError, match="undefined-price"):
        TradePrint(price_raw=UNDEFINED_PRICE, **common)
    with pytest.raises(TradeBarError, match="off the 6E tick grid"):
        TradePrint(price_raw=100 * TICK_SIZE_RAW + 1, **common)
    with pytest.raises(TradeBarError, match="price_raw must be an integer"):
        TradePrint(price_raw=None, **common)  # type: ignore[arg-type]

    left = _print(day, 0.1, 100, sequence=1, ordinal=0)
    right = _print(day, 0.1, 101, sequence=1, ordinal=0)
    with pytest.raises(TradeBarError, match="duplicate trade ordering key"):
        build_one_second_trade_bars((left, right), contract="6EH2", source_date=day)

    with pytest.raises(TradeBarError, match="OHLC tick ordering"):
        TradeBar(
            timeframe_seconds=1,
            segment_id=1,
            contract="6EH2",
            source_date=day,
            start_ns=_ns(day),
            end_ns=_ns(day, 1),
            first_trade_ns=_ns(day, 0.1),
            last_trade_ns=_ns(day, 0.2),
            open_ticks=100,
            high_ticks=99,
            low_ticks=98,
            close_ticks=99,
            trade_count=1,
            volume=1,
            observed_subbars=1,
        )


def _write_daily_source(path: Path, source_date: date) -> None:
    start_ns = _ns(source_date)
    mappings = [
        {
            "raw_symbol": "6EH2",
            "intervals": [{"start": "2022-01-01", "end": "2022-04-01", "symbol": "1"}],
        },
        {
            "raw_symbol": "6EM2",
            "intervals": [{"start": "2022-01-01", "end": "2022-07-01", "symbol": "2"}],
        },
    ]
    schema = pa.schema(
        [
            pa.field("ts_recv", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("instrument_id", pa.uint32(), nullable=False),
            pa.field("action", pa.string(), nullable=False),
            pa.field("side", pa.string(), nullable=False),
            pa.field("price", pa.int64(), nullable=False),
            pa.field("size", pa.uint32(), nullable=False),
            pa.field("flags", pa.uint8(), nullable=False),
            pa.field("sequence", pa.uint32(), nullable=False),
        ],
        metadata={
            b"dbn.metadata": json.dumps(
                {"start": start_ns, "mappings": mappings},
                separators=(",", ":"),
            ).encode()
        },
    )
    rows = [
        {
            "ts_recv": start_ns + 100_000_000,
            "instrument_id": 1,
            "action": "T",
            "side": "B",
            "price": 100 * TICK_SIZE_RAW,
            "size": 2,
            "flags": 0,
            "sequence": 1,
        },
        {
            "ts_recv": start_ns + 200_000_000,
            "instrument_id": 1,
            "action": "T",
            "side": "A",
            "price": 101 * TICK_SIZE_RAW,
            "size": 3,
            "flags": 0,
            "sequence": 2,
        },
        {
            "ts_recv": start_ns + 1_100_000_000,
            "instrument_id": 1,
            "action": "T",
            "side": "N",
            "price": 99 * TICK_SIZE_RAW,
            "size": 4,
            "flags": 0,
            "sequence": 3,
        },
        {
            "ts_recv": start_ns + 2_100_000_000,
            "instrument_id": 1,
            "action": "T",
            "side": "B",
            "price": 100 * TICK_SIZE_RAW,
            "size": 1,
            "flags": 8,
            "sequence": 4,
        },
        {
            "ts_recv": start_ns + 300_000_000,
            "instrument_id": 2,
            "action": "T",
            "side": "B",
            "price": 200 * TICK_SIZE_RAW,
            "size": 5,
            "flags": 0,
            "sequence": 5,
        },
        {
            "ts_recv": start_ns + 400_000_000,
            "instrument_id": 1,
            "action": "A",
            "side": "B",
            "price": 102 * TICK_SIZE_RAW,
            "size": 1,
            "flags": 0,
            "sequence": 6,
        },
    ]
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path, compression="zstd", row_group_size=3)


def _daily_source_path(data_root: Path, source_date: date) -> Path:
    path = (
        data_root
        / "mbp-10"
        / f"{source_date.year:04d}"
        / f"{source_date.month:02d}"
        / f"{source_date.day:02d}"
        / f"glbx-mdp3-{source_date:%Y%m%d}.mbp-10.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _publication_table() -> pa.Table:
    source_date = date(2022, 2, 1)
    bars = build_one_second_trade_bars(
        [_print(source_date, 0.1, 100, sequence=1, ordinal=0)],
        contract="6EH2",
        source_date=source_date,
    )
    return bars_module.trade_bars_to_table(
        bars,
        plan_sha256=_sha("a"),
        source_sha256=_sha("b"),
    )


def _forged_publication_table(table: pa.Table) -> pa.Table:
    records = table.to_pylist()
    records[0]["volume"] = int(records[0]["volume"]) + 1
    return pa.Table.from_pylist(records, schema=table.schema)


def _publish_test_bar_table(
    data_root: Path,
    table: pa.Table,
    *,
    timeframe_seconds: int,
) -> BarArtifact:
    data_root.mkdir()
    staged = data_root / "staged.parquet"
    pq.write_table(table, staged, compression="zstd")
    digest = hashlib.sha256(staged.read_bytes()).hexdigest()
    labels = {1: "1s", 60: "1m", 300: "5m", 1_800: "30m", 3_600: "1h"}
    relative = Path(
        f"derived/trade_bars/version={BAR_VERSION}/"
        f"timeframe={labels[timeframe_seconds]}/sha256={digest}.parquet"
    )
    target = data_root / relative
    target.parent.mkdir(parents=True)
    staged.replace(target)
    return BarArtifact(
        timeframe_seconds=timeframe_seconds,
        relative_uri=relative.as_posix(),
        sha256=digest,
        byte_size=target.stat().st_size,
        row_count=table.num_rows,
        disposition="CREATED",
    )


def _loader_fixture(data_root: Path):
    data_root.mkdir(exist_ok=True)
    source_date = date(2022, 2, 1)
    raw = _daily_source_path(data_root, source_date)
    _write_daily_source(raw, source_date)
    report = build_daily_trade_bar_artifacts(
        raw,
        data_root=data_root,
        source_date=source_date,
        verified_source_sha256=_file_sha256(raw),
        previous_volume_summary=_previous_summary(),
    )
    return report.artifacts[0], report


def test_daily_primitive_projects_once_and_publishes_content_addressed_bars(
    tmp_path: Path,
) -> None:
    source_date = date(2022, 2, 1)
    raw = _daily_source_path(tmp_path, source_date)
    _write_daily_source(raw, source_date)
    source_sha256 = _file_sha256(raw)

    first = build_daily_trade_bar_artifacts(
        raw,
        data_root=tmp_path,
        source_date=source_date,
        verified_source_sha256=source_sha256,
        previous_volume_summary=_previous_summary(),
    )
    second = build_daily_trade_bar_artifacts(
        raw,
        data_root=tmp_path,
        source_date=source_date,
        verified_source_sha256=source_sha256,
        previous_volume_summary=_previous_summary(),
    )

    assert first.plan.status is DailyPlanStatus.SELECTED
    assert first.plan.selected_contract == "6EH2"
    assert first.source_row_count == 6
    assert first.source_trade_count == 5
    assert first.as_dict()["source_trade_count"] == 5
    assert first.selected_trade_count == 3
    assert first.bad_ts_recv_trades_excluded == 1
    assert len(first.artifacts) == 5
    assert [item.timeframe_seconds for item in first.artifacts] == [1, 60, 300, 1_800, 3_600]
    assert [item.row_count for item in first.artifacts] == [2, 1, 1, 1, 1]
    assert {item.disposition for item in first.artifacts} == {"CREATED"}
    assert {item.disposition for item in second.artifacts} == {"REUSED"}
    assert first.sha256 == second.sha256
    assert [item.sha256 for item in first.artifacts] == [item.sha256 for item in second.artifacts]
    assert [item.semantic_dict() for item in first.artifacts] == [
        item.semantic_dict() for item in second.artifacts
    ]

    loaded = {
        item.timeframe_seconds: load_trade_bar_artifact(
            tmp_path,
            item,
            expected_plan_sha256=first.plan.sha256,
            expected_source_sha256=source_sha256,
            expected_source_date=source_date,
        )
        for item in first.artifacts
    }
    assert {timeframe: len(bars) for timeframe, bars in loaded.items()} == {
        1: 2,
        60: 1,
        300: 1,
        1_800: 1,
        3_600: 1,
    }
    manifest_descriptor = TradeBarArtifactDescriptor.from_mapping(
        first.artifacts[0].semantic_dict()
    )
    assert manifest_descriptor.as_dict() == first.artifacts[0].semantic_dict()
    assert (
        load_trade_bar_artifact(
            tmp_path,
            manifest_descriptor,
            expected_plan_sha256=first.plan.sha256,
            expected_source_sha256=source_sha256,
            expected_source_date=source_date,
        )
        == loaded[1]
    )
    with pytest.raises(TradeBarError, match="fields are not canonical"):
        TradeBarArtifactDescriptor.from_mapping(first.artifacts[0].as_dict())
    with pytest.raises(TradeBarError, match="byte size differs"):
        load_trade_bar_artifact(
            tmp_path,
            replace(first.artifacts[0], byte_size=first.artifacts[0].byte_size + 1),
        )
    with pytest.raises(TradeBarError, match="plan identity differs"):
        load_trade_bar_artifact(
            tmp_path,
            first.artifacts[0],
            expected_plan_sha256=_sha("0"),
        )

    assert first.current_volume_summary is not None
    by_symbol = {
        item.raw_symbols[0]: (item.trade_count, item.volume)
        for item in first.current_volume_summary.contracts
    }
    # BAD_TS_RECV is excluded from price bars but remains valid prior-date
    # contract-volume evidence because that selection uses no timestamp order.
    assert by_symbol == {"6EH2": (4, 10), "6EM2": (1, 5)}

    one_second_artifact = first.artifacts[0]
    output = tmp_path / one_second_artifact.relative_uri
    assert output.is_file()
    assert f"version={BAR_VERSION}" in one_second_artifact.relative_uri
    assert f"sha256={one_second_artifact.sha256}.parquet" in one_second_artifact.relative_uri
    table = pq.read_table(output)
    artifact_metadata = pq.ParquetFile(output).schema_arrow.metadata
    assert artifact_metadata is not None
    assert artifact_metadata[b"systematic_fx.plan_sha256"].decode() == first.plan.sha256
    assert artifact_metadata[b"systematic_fx.source_date"] == b"2022-02-01"
    assert artifact_metadata[b"systematic_fx.source_sha256"].decode() == source_sha256
    forbidden = {
        "action",
        "flags",
        "instrument_id",
        "physical_ordinal",
        "price",
        "price_raw",
        "sequence",
        "side",
    }
    assert forbidden.isdisjoint(table.column_names)
    assert {
        "open_ticks",
        "high_ticks",
        "low_ticks",
        "close_ticks",
        "next_link_status",
        "next_bar_start_ns",
        "next_first_trade_ns",
    }.issubset(table.column_names)
    assert table["next_link_status"].to_pylist() == [
        NextBarLinkStatus.EXACT_NEXT_BAR.value,
        NextBarLinkStatus.PARTITION_END.value,
    ]
    assert table["open_ticks"].to_pylist() == [100, 99]
    assert table["close_ticks"].to_pylist() == [101, 99]

    bad_metadata = _publish_test_bar_table(
        tmp_path / "bad-metadata",
        pq.ParquetFile(output).read().replace_schema_metadata({}),
        timeframe_seconds=1,
    )
    with pytest.raises(TradeBarError, match="metadata keys differ"):
        load_trade_bar_artifact(tmp_path / "bad-metadata", bad_metadata)

    bad_link_table = pq.ParquetFile(output).read()
    link_index = bad_link_table.schema.get_field_index("next_link_status")
    bad_link_table = bad_link_table.set_column(
        link_index,
        bad_link_table.schema.field(link_index),
        pa.array(
            [NextBarLinkStatus.GAP.value, NextBarLinkStatus.PARTITION_END.value],
            type=pa.string(),
        ),
    )
    bad_link = _publish_test_bar_table(
        tmp_path / "bad-link",
        bad_link_table,
        timeframe_seconds=1,
    )
    with pytest.raises(TradeBarError, match="invalid next-link metadata"):
        load_trade_bar_artifact(tmp_path / "bad-link", bad_link)

    real_parent = output.parent.with_name(f"{output.parent.name}-real")
    output.parent.rename(real_parent)
    output.parent.symlink_to(real_parent, target_is_directory=True)
    with pytest.raises(TradeBarError, match="symbolic link"):
        load_trade_bar_artifact(tmp_path, first.artifacts[0])


def test_loader_hashes_and_reads_parquet_through_the_same_held_fd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact, report = _loader_fixture(tmp_path)
    hash_descriptors: list[int] = []
    parquet_descriptors: list[int] = []
    original_hash = bars_module._sha256_descriptor
    original_parquet_file = bars_module.pq.ParquetFile

    def hash_spy(descriptor: int) -> str:
        hash_descriptors.append(descriptor)
        return original_hash(descriptor)

    def parquet_spy(source, *args, **kwargs):
        assert not isinstance(source, (str, Path))
        parquet_descriptors.append(source.fileno())
        return original_parquet_file(source, *args, **kwargs)

    monkeypatch.setattr(bars_module, "_sha256_descriptor", hash_spy)
    monkeypatch.setattr(bars_module.pq, "ParquetFile", parquet_spy)

    loaded = load_trade_bar_artifact(
        tmp_path,
        artifact,
        expected_plan_sha256=report.plan.sha256,
        expected_source_sha256=report.plan.source_sha256,
        expected_source_date=date(2022, 2, 1),
    )

    assert len(loaded) == artifact.row_count
    assert len(hash_descriptors) == len(parquet_descriptors) == 1
    assert hash_descriptors == parquet_descriptors


def test_daily_source_hashes_and_scans_parquet_through_the_same_held_fd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_date = date(2022, 2, 1)
    raw = _daily_source_path(tmp_path, source_date)
    _write_daily_source(raw, source_date)
    source_sha256 = _file_sha256(raw)
    hash_descriptors: list[int] = []
    parquet_descriptors: list[int] = []
    original_hash = bars_module._sha256_descriptor
    original_parquet_file = bars_module.pq.ParquetFile

    def hash_spy(descriptor: int) -> str:
        hash_descriptors.append(descriptor)
        return original_hash(descriptor)

    def parquet_spy(source, *args, **kwargs):
        if not isinstance(source, (str, Path)):
            parquet_descriptors.append(source.fileno())
        return original_parquet_file(source, *args, **kwargs)

    monkeypatch.setattr(bars_module, "_sha256_descriptor", hash_spy)
    monkeypatch.setattr(bars_module.pq, "ParquetFile", parquet_spy)

    report = build_daily_trade_bar_artifacts(
        raw,
        data_root=tmp_path,
        source_date=source_date,
        verified_source_sha256=source_sha256,
        previous_volume_summary=_previous_summary(),
    )

    assert report.source_scanned
    assert hash_descriptors[0] == parquet_descriptors[0]


@pytest.mark.parametrize("swap_target", ["data_root", "ancestor", "parent", "leaf"])
def test_daily_source_detects_path_swap_while_held_fd_is_being_scanned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    swap_target: str,
) -> None:
    data_root = tmp_path / "data-root"
    data_root.mkdir()
    source_date = date(2022, 2, 1)
    raw = _daily_source_path(data_root, source_date)
    _write_daily_source(raw, source_date)
    source_sha256 = _file_sha256(raw)
    original_scan = bars_module._scan_daily_trades
    swapped = False

    def scan_spy(*args, **kwargs):
        nonlocal swapped
        scan = original_scan(*args, **kwargs)
        if not swapped:
            swapped = True
            if swap_target == "data_root":
                displaced = data_root.with_name("data-root-displaced")
                data_root.rename(displaced)
                data_root.mkdir()
            elif swap_target == "ancestor":
                ancestor = data_root / "mbp-10"
                displaced = data_root / "mbp-10-displaced"
                ancestor.rename(displaced)
                ancestor.mkdir()
            elif swap_target == "parent":
                parent = raw.parent
                displaced = parent.with_name(f"{parent.name}-displaced")
                parent.rename(displaced)
                parent.symlink_to(displaced, target_is_directory=True)
            else:
                displaced = raw.with_name("displaced.parquet")
                raw.rename(displaced)
                raw.symlink_to(displaced)
        return scan

    monkeypatch.setattr(bars_module, "_scan_daily_trades", scan_spy)

    with pytest.raises(TradeBarError, match="changed during source projection"):
        build_daily_trade_bar_artifacts(
            raw,
            data_root=data_root,
            source_date=source_date,
            verified_source_sha256=source_sha256,
            previous_volume_summary=_previous_summary(),
        )


def test_daily_source_rejects_wrong_hash_noncanonical_path_and_symlink(
    tmp_path: Path,
) -> None:
    source_date = date(2022, 2, 1)
    data_root = tmp_path / "data-root"
    data_root.mkdir()
    raw = _daily_source_path(data_root, source_date)
    _write_daily_source(raw, source_date)
    source_sha256 = _file_sha256(raw)

    with pytest.raises(TradeBarError, match="SHA-256 differs"):
        build_daily_trade_bar_artifacts(
            raw,
            data_root=data_root,
            source_date=source_date,
            verified_source_sha256="0" * 64,
            previous_volume_summary=_previous_summary(),
        )

    noncanonical = data_root / "copy.parquet"
    noncanonical.write_bytes(raw.read_bytes())
    with pytest.raises(TradeBarError, match="path is not canonical"):
        build_daily_trade_bar_artifacts(
            noncanonical,
            data_root=data_root,
            source_date=source_date,
            verified_source_sha256=source_sha256,
            previous_volume_summary=_previous_summary(),
        )


@pytest.mark.parametrize("symlink_target", ["mbp_root", "year", "month", "day", "leaf"])
def test_daily_source_rejects_preexisting_symlink_components(
    tmp_path: Path,
    symlink_target: str,
) -> None:
    source_date = date(2022, 2, 1)
    data_root = tmp_path / "data-root"
    data_root.mkdir()
    raw = _daily_source_path(data_root, source_date)
    _write_daily_source(raw, source_date)
    source_sha256 = _file_sha256(raw)
    components = {
        "mbp_root": data_root / "mbp-10",
        "year": data_root / "mbp-10" / "2022",
        "month": data_root / "mbp-10" / "2022" / "02",
        "day": raw.parent,
        "leaf": raw,
    }
    target = components[symlink_target]
    is_directory = target.is_dir()
    displaced = target.with_name(f"{target.name}-real")
    target.rename(displaced)
    target.symlink_to(displaced, target_is_directory=is_directory)

    with pytest.raises(TradeBarError, match="symbolic link"):
        build_daily_trade_bar_artifacts(
            raw,
            data_root=data_root,
            source_date=source_date,
            verified_source_sha256=source_sha256,
            previous_volume_summary=_previous_summary(),
        )


def test_trade_bar_publisher_writes_validates_and_hashes_one_held_fd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = _publication_table()
    legacy_path = tmp_path / "legacy-path-writer.parquet"
    pq.write_table(
        table,
        legacy_path,
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
        version="2.6",
        row_group_size=65_536,
    )
    legacy_sha256 = _file_sha256(legacy_path)
    writer_descriptors: list[int] = []
    parquet_descriptors: list[int] = []
    hash_descriptors: list[int] = []
    original_write = bars_module.pq.write_table
    original_parquet = bars_module.pq.ParquetFile
    original_hash = bars_module._sha256_descriptor

    def write_spy(value, destination, **kwargs):
        writer_descriptors.append(destination.fileno())
        return original_write(value, destination, **kwargs)

    def parquet_spy(source, *args, **kwargs):
        parquet_descriptors.append(source.fileno())
        return original_parquet(source, *args, **kwargs)

    def hash_spy(descriptor: int) -> str:
        hash_descriptors.append(descriptor)
        return original_hash(descriptor)

    monkeypatch.setattr(bars_module.pq, "write_table", write_spy)
    monkeypatch.setattr(bars_module.pq, "ParquetFile", parquet_spy)
    monkeypatch.setattr(bars_module, "_sha256_descriptor", hash_spy)

    artifact = bars_module._publish_table(
        table,
        data_root=tmp_path,
        timeframe_seconds=1,
    )

    assert writer_descriptors == parquet_descriptors == hash_descriptors[:1]
    assert artifact.disposition == "CREATED"
    assert artifact.sha256 == legacy_sha256
    assert load_trade_bar_artifact(tmp_path, artifact)


@pytest.mark.parametrize("swap_target", ["ancestor", "temporary", "target"])
def test_trade_bar_publisher_rejects_path_swap_before_target_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    swap_target: str,
) -> None:
    table = _publication_table()
    forged = _forged_publication_table(table)
    relative_parent = Path("derived/trade_bars/version=trade_bar_v1/timeframe=1s")
    parent = tmp_path / relative_parent
    original_link = bars_module.os.link
    swapped = False

    def link_spy(source_name, target_name, *args, **kwargs):
        nonlocal swapped
        if swap_target == "temporary":
            staged = parent / source_name
            staged.rename(parent / f"{source_name}.displaced")
            pq.write_table(forged, staged)
            staged.chmod(0o444)
        result = original_link(source_name, target_name, *args, **kwargs)
        if not swapped and swap_target == "target":
            swapped = True
            target = parent / target_name
            target.rename(parent / f"{target_name}.displaced")
            pq.write_table(forged, target)
            target.chmod(0o444)
        elif not swapped and swap_target == "ancestor":
            swapped = True
            ancestor = tmp_path / "derived"
            ancestor.rename(tmp_path / "derived-displaced")
            ancestor.mkdir()
        return result

    monkeypatch.setattr(bars_module.os, "link", link_spy)

    with pytest.raises(TradeBarError, match="differs|changed"):
        bars_module._publish_table(
            table,
            data_root=tmp_path,
            timeframe_seconds=1,
        )


@pytest.mark.parametrize("swap_target", ["data_root", "ancestor", "parent", "leaf"])
def test_loader_detects_path_swap_while_held_fd_is_being_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    swap_target: str,
) -> None:
    data_root = tmp_path / "data-root"
    artifact, _ = _loader_fixture(data_root)
    output = data_root / artifact.relative_uri
    original_parquet_file = bars_module.pq.ParquetFile
    swapped = False

    def parquet_spy(source, *args, **kwargs):
        nonlocal swapped
        parquet = original_parquet_file(source, *args, **kwargs)
        if not swapped:
            swapped = True
            if swap_target == "data_root":
                displaced = data_root.with_name("data-root-displaced")
                data_root.rename(displaced)
                data_root.mkdir()
            elif swap_target == "ancestor":
                ancestor = data_root / "derived"
                displaced = data_root / "derived-displaced"
                ancestor.rename(displaced)
                ancestor.mkdir()
            elif swap_target == "parent":
                parent = output.parent
                displaced = parent.with_name(f"{parent.name}-displaced")
                parent.rename(displaced)
                parent.symlink_to(displaced, target_is_directory=True)
            else:
                displaced = output.with_name("displaced.parquet")
                output.rename(displaced)
                output.symlink_to(displaced)
        return parquet

    monkeypatch.setattr(bars_module.pq, "ParquetFile", parquet_spy)

    with pytest.raises(TradeBarError, match="changed during verified loading"):
        load_trade_bar_artifact(data_root, artifact)


def test_loader_rejects_preexisting_leaf_symlink_and_noncanonical_leaf(
    tmp_path: Path,
) -> None:
    artifact, _ = _loader_fixture(tmp_path)
    output = tmp_path / artifact.relative_uri
    displaced = output.with_name("real.parquet")
    output.rename(displaced)
    output.symlink_to(displaced)

    with pytest.raises(TradeBarError, match="leaf cannot be a symbolic link"):
        load_trade_bar_artifact(tmp_path, artifact)

    with pytest.raises(TradeBarError, match="URI is not canonical"):
        load_trade_bar_artifact(
            tmp_path,
            replace(artifact, relative_uri="derived/trade_bars/wrong.parquet"),
        )


def test_daily_primitive_does_not_scan_qc_excluded_source(tmp_path: Path) -> None:
    source_date = date(2024, 6, 30)
    raw = _daily_source_path(tmp_path, source_date)
    # A valid minimal footer/schema is enough: the hard exclusion must happen
    # before any event-row scan or publication.
    schema = pa.schema(
        [
            pa.field("ts_recv", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("instrument_id", pa.uint32(), nullable=False),
            pa.field("action", pa.string(), nullable=False),
            pa.field("side", pa.string(), nullable=False),
            pa.field("price", pa.int64(), nullable=False),
            pa.field("size", pa.uint32(), nullable=False),
            pa.field("flags", pa.uint8(), nullable=False),
            pa.field("sequence", pa.uint32(), nullable=False),
        ],
        metadata={
            b"dbn.metadata": json.dumps(
                {"start": _ns(source_date), "mappings": []},
                separators=(",", ":"),
            ).encode()
        },
    )
    pq.write_table(pa.Table.from_pylist([], schema=schema), raw)

    report = build_daily_trade_bar_artifacts(
        raw,
        data_root=tmp_path,
        source_date=source_date,
        verified_source_sha256=_file_sha256(raw),
        previous_volume_summary=None,
    )

    assert report.plan.status is DailyPlanStatus.QC_EXCLUDED
    assert report.source_scanned is False
    assert report.current_volume_summary is None
    assert report.artifacts == ()
    assert not (tmp_path / "derived").exists()
