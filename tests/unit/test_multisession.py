from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import UTC, date, datetime, time, timedelta
from itertools import pairwise
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from systematic_fx.backtest.barriers import (
    BarrierOutcome,
    Direction,
    ExecutableQuote,
    replay_barrier_surface,
)
from systematic_fx.backtest.economics import EntryStatus
from systematic_fx.backtest.entry import (
    BboInvalidReason,
    EntryAudit,
    EntryReason,
    EntryReplayResult,
    ExecutableQuotePath,
)
from systematic_fx.backtest.multisession import (
    CutoffReason,
    LineagedExecutableQuote,
    MultisessionReplayError,
    SourceSession,
    build_multisession_path,
)
from systematic_fx.data.contracts import (
    DATASET_NAME,
    PRICE_ENCODING,
    PRICE_SCALE_TEXT,
    SCHEMA_NAME,
    UNDEFINED_PRICE,
    expected_mbp10_schema,
)

SECOND = 1_000_000_000
TICK_RAW = 50_000
BASE_TICKS = 20_000
MANIFEST_SHA = "f" * 64

type MappingSpec = tuple[str, int, date, date]


def _ns(value: datetime) -> int:
    return int(value.timestamp()) * SECOND + value.microsecond * 1_000


def _at(day: date, hour: int, minute: int, second: float = 0) -> int:
    whole = int(second)
    microsecond = round((second - whole) * 1_000_000)
    return _ns(datetime.combine(day, time(hour, minute, whole, microsecond), tzinfo=UTC))


def _raw(ticks: int) -> int:
    return ticks * TICK_RAW


def _mapping(
    raw_symbol: str,
    instrument_id: int,
    start: date = date(2022, 1, 1),
    end: date = date(2022, 4, 1),
) -> MappingSpec:
    return raw_symbol, instrument_id, start, end


def _metadata(day: date, mappings: list[MappingSpec]) -> dict[bytes, bytes]:
    document = {
        "dataset": DATASET_NAME,
        "schema": SCHEMA_NAME,
        "version": 3,
        "stype_out": "instrument_id",
        "start": _at(day, 0, 0),
        "end": _at(day + timedelta(days=1), 0, 0),
        "mappings": [
            {
                "raw_symbol": raw_symbol,
                "intervals": [
                    {
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                        "symbol": str(instrument_id),
                    }
                ],
            }
            for raw_symbol, instrument_id, start, end in mappings
        ],
    }
    return {
        b"dbn.dataset": DATASET_NAME.encode(),
        b"dbn.schema": SCHEMA_NAME.encode(),
        b"dbn.version": b"3",
        b"mbo_mbp10.price_encoding": PRICE_ENCODING.encode(),
        b"mbo_mbp10.price_scale": PRICE_SCALE_TEXT.encode(),
        b"mbo_mbp10.undefined_price": str(UNDEFINED_PRICE).encode(),
        b"dbn.metadata": json.dumps(document, sort_keys=True).encode(),
    }


def _event(
    timestamp_ns: int,
    *,
    instrument_id: int,
    action: str = "A",
    flags: int = 0,
    sequence: int = 1,
    bid_ticks: int | None = BASE_TICKS,
    ask_ticks: int | None = BASE_TICKS + 1,
    bid_raw: int | None = None,
    ask_raw: int | None = None,
    bid_size: int = 2,
    ask_size: int = 2,
) -> dict[str, object]:
    return {
        "ts_recv": timestamp_ns,
        "instrument_id": instrument_id,
        "action": action,
        "flags": flags,
        "sequence": sequence,
        "bid_px_00": (
            bid_raw
            if bid_raw is not None
            else UNDEFINED_PRICE
            if bid_ticks is None
            else _raw(bid_ticks)
        ),
        "ask_px_00": (
            ask_raw
            if ask_raw is not None
            else UNDEFINED_PRICE
            if ask_ticks is None
            else _raw(ask_ticks)
        ),
        "bid_sz_00": bid_size,
        "ask_sz_00": ask_size,
    }


def _write_source(
    path: Path,
    *,
    day: date,
    mappings: list[MappingSpec],
    events: list[dict[str, object]],
    row_group_size: int = 2,
) -> None:
    schema = expected_mbp10_schema(metadata=_metadata(day, mappings))
    columns: dict[str, list[object]] = {field.name: [] for field in schema}
    for ordinal, event in enumerate(events):
        timestamp_ns = int(event["ts_recv"])
        for field in schema:
            name = field.name
            if name in {"ts_recv", "ts_event"}:
                value: object = timestamp_ns
            elif name == "rtype":
                value = 10
            elif name == "publisher_id":
                value = 1
            elif name == "instrument_id":
                value = event[name]
            elif name == "action":
                value = event.get(name, "A")
            elif name == "side":
                value = "N" if event.get("action") == "R" else "B"
            elif name == "depth":
                value = 0
            elif name == "price":
                value = UNDEFINED_PRICE
            elif name == "size":
                value = 1
            elif name == "flags":
                value = event.get(name, 0)
            elif name == "ts_in_delta":
                value = 0
            elif name == "sequence":
                value = event.get(name, ordinal + 1)
            elif name.startswith(("bid_px_", "ask_px_")):
                value = event.get(name, UNDEFINED_PRICE)
            elif name.startswith(("bid_sz_", "ask_sz_", "bid_ct_", "ask_ct_")):
                value = event.get(name, 0)
            else:  # pragma: no cover - exact schema is deliberately exhaustive
                raise AssertionError(name)
            columns[name].append(value)
    pq.write_table(
        pa.Table.from_pydict(columns, schema=schema),
        path,
        row_group_size=row_group_size,
    )


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _entry(
    *,
    eligible_day: date = date(2022, 1, 4),
    raw_symbol: str = "6EH2",
    contract_month: date = date(2022, 3, 1),
    footer_rows: int = 10,
    events: tuple[ExecutableQuote, ...] | None = None,
) -> EntryReplayResult:
    entry_events = events or (
        ExecutableQuote(
            event_index=8,
            ts_recv_ns=_at(eligible_day, 23, 0),
            best_bid_ticks=BASE_TICKS - 1,
            best_ask_ticks=BASE_TICKS,
        ),
    )
    audit = EntryAudit(
        source_path="entry.parquet",
        source_schema_sha256="a" * 64,
        source_dbn_metadata_sha256="b" * 64,
        source_footer_rows=footer_rows,
        source_footer_row_groups=1,
        source_rows_examined=footer_rows,
        source_row_groups_read=1,
        selected_instrument_id=101,
        selected_raw_symbol=raw_symbol,
        selected_contract_month=contract_month,
        previous_source_date=eligible_day - timedelta(days=1),
        eligible_source_date=eligible_day,
        selection_sha256="c" * 64,
        previous_volume_sha256="d" * 64,
        contract_selection_policy_version="previous_source_trade_volume_v1",
        execution_policy_id="phase1a_conservative_execution_v1",
        execution_policy_sha256="e" * 64,
        screening_bundle_sha256="f" * 64,
        decision_ts_recv_ns=_at(eligible_day, 10, 0),
        entry_eligibility_ts_recv_ns=_at(eligible_day, 10, 0, 1),
        decision_event=None,
        eligibility_snapshot=None,
        attempt_event=None,
        entry_limit_side="BEST_ASK",
        entry_limit_price_raw=_raw(BASE_TICKS),
        entry_limit_price_ticks=BASE_TICKS,
        failure_event=None,
        route_event_count=1,
        maximum_route_quote_gap_ns=0,
    )
    canonical = b'{"filled":true}'
    return EntryReplayResult(
        status=EntryStatus.ENTRY_FILLED,
        reason=EntryReason.FILLED_AT_DELAYED_OPPOSITE_BBO,
        direction=Direction.LONG,
        fill_price_ticks=BASE_TICKS,
        fill_price_raw=_raw(BASE_TICKS),
        fill_quantity_contracts=1,
        executable_path=ExecutableQuotePath(
            entry_events,
            terminal_quote=None,
            terminal_reference=None,
        ),
        audit=audit,
        canonical_bytes=canonical,
        sha256=hashlib.sha256(canonical).hexdigest(),
    )


class MultisessionPathTests(unittest.TestCase):
    def test_fixed_symbol_continues_across_id_changes_and_integrates_with_barriers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first_day = date(2022, 1, 5)
            second_day = date(2022, 1, 6)
            first = root / "first.parquet"
            second = root / "second.parquet"
            _write_source(
                first,
                day=first_day,
                mappings=[_mapping("6EH2", 201), _mapping("6EM2", 999)],
                events=[
                    _event(
                        _at(first_day, 10, 0),
                        instrument_id=999,
                        sequence=1,
                        bid_ticks=BASE_TICKS + 200,
                        ask_ticks=BASE_TICKS + 201,
                    ),
                    _event(
                        _at(first_day, 10, 0, 0.1),
                        instrument_id=201,
                        sequence=2,
                        bid_ticks=BASE_TICKS + 25,
                        ask_ticks=BASE_TICKS + 26,
                    ),
                    _event(
                        _at(first_day, 10, 0, 0.2),
                        instrument_id=201,
                        sequence=3,
                        bid_ticks=BASE_TICKS + 5,
                        ask_ticks=BASE_TICKS + 5,
                    ),
                ],
            )
            _write_source(
                second,
                day=second_day,
                mappings=[_mapping("6EH2", 301), _mapping("6EM2", 201)],
                events=[
                    _event(
                        _at(second_day, 10, 0),
                        instrument_id=301,
                        sequence=4,
                        bid_ticks=BASE_TICKS + 3,
                        ask_ticks=BASE_TICKS + 4,
                    ),
                    _event(
                        _at(second_day, 10, 0, 0.1),
                        instrument_id=301,
                        sequence=5,
                        bid_ticks=None,
                        ask_ticks=None,
                    ),
                ],
            )
            entry = _entry()
            path = build_multisession_path(
                entry=entry,
                sources=[SourceSession(first_day, first), SourceSession(second_day, second)],
                source_sha256_by_date={first_day: _sha(first), second_day: _sha(second)},
                source_manifest_sha256=MANIFEST_SHA,
            )

            self.assertFalse(path.consumed)
            self.assertEqual(path.source_files_streamed, 0)
            self.assertIsInstance(path.terminal_event, LineagedExecutableQuote)
            assert path.terminal_event is not None
            self.assertEqual(path.terminal_event.instrument_id, 301)
            self.assertEqual(path.terminal_event.source_date, second_day)
            self.assertEqual(path.terminal_event.event_index, 13)
            self.assertEqual([item.instrument_id for item in path.audit.sources], [201, 301])

            surface = replay_barrier_surface(
                direction=entry.direction,
                entry_fill_price_ticks=entry.fill_price_ticks,
                events=path,
                terminal_event=path.terminal_event,
            )

            self.assertTrue(path.consumed)
            self.assertEqual(path.source_files_streamed, 2)
            self.assertEqual(surface.thresholds.source_event_count, 3)
            self.assertEqual(surface.cell(24, 24).outcome, BarrierOutcome.TP_FIRST)
            self.assertEqual(surface.cell(24, 24).take_profit_fill_event_index, 11)
            with self.assertRaisesRegex(MultisessionReplayError, "only once"):
                iter(path)

    def test_reset_locked_crossed_and_missing_depth_match_executable_quote_rules(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            day = date(2022, 1, 5)
            source = root / "reset.parquet"
            start = _at(day, 10, 0)
            _write_source(
                source,
                day=day,
                mappings=[_mapping("6EH2", 201)],
                events=[
                    _event(start, instrument_id=201, action="R", sequence=1),
                    _event(start + 100_000_000, instrument_id=201, sequence=2),
                    _event(
                        start + 500_000_000,
                        instrument_id=201,
                        sequence=3,
                        bid_ticks=BASE_TICKS,
                        ask_ticks=BASE_TICKS,
                    ),
                    _event(start + 600_000_000, instrument_id=201, sequence=4),
                    _event(start + 1_600_000_000, instrument_id=201, sequence=5),
                    _event(
                        start + 1_700_000_000,
                        instrument_id=201,
                        sequence=6,
                        bid_size=0,
                    ),
                    _event(
                        start + 1_800_000_000,
                        instrument_id=201,
                        sequence=7,
                        bid_ticks=BASE_TICKS + 2,
                        ask_ticks=BASE_TICKS + 1,
                    ),
                    _event(start + 1_900_000_000, instrument_id=201, sequence=8),
                ],
                row_group_size=3,
            )
            path = build_multisession_path(
                entry=_entry(),
                sources=[SourceSession(day, source)],
                source_sha256_by_date={day: _sha(source)},
                source_manifest_sha256=MANIFEST_SHA,
            )

            events = list(path)
            later = [event for event in events if isinstance(event, LineagedExecutableQuote)]
            self.assertEqual(
                [event.invalid_reason for event in later],
                [
                    BboInvalidReason.RESET,
                    BboInvalidReason.RESET_NOT_REARMED,
                    BboInvalidReason.LOCKED_BOOK,
                    BboInvalidReason.RESET_NOT_REARMED,
                    None,
                    BboInvalidReason.MISSING_DEPTH,
                    BboInvalidReason.CROSSED_BOOK,
                ],
            )
            self.assertEqual([event.source_row_index for event in later], list(range(7)))
            self.assertTrue(
                all(left.event_index < right.event_index for left, right in pairwise(events))
            )
            assert path.terminal_event is not None
            self.assertEqual(path.terminal_event.source_row_index, 7)
            self.assertTrue(path.terminal_event.valid)

    def test_expiry_source_and_everything_after_are_not_opened(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            before_day = date(2022, 2, 28)
            expiry_day = date(2022, 3, 1)
            after_day = date(2022, 3, 2)
            before = root / "before.parquet"
            _write_source(
                before,
                day=before_day,
                mappings=[_mapping("6EH2", 201)],
                events=[_event(_at(before_day, 10, 0), instrument_id=201)],
            )
            path = build_multisession_path(
                entry=_entry(),
                sources=[
                    SourceSession(before_day, before),
                    SourceSession(expiry_day, root / "does-not-exist.parquet"),
                    SourceSession(after_day, root / "also-does-not-exist.parquet"),
                ],
                source_sha256_by_date={
                    before_day: _sha(before),
                    expiry_day: "1" * 64,
                    after_day: "2" * 64,
                },
                source_manifest_sha256=MANIFEST_SHA,
            )

            self.assertEqual(path.audit.cutoff_reason, CutoffReason.EXPIRY_MONTH)
            self.assertEqual(path.audit.expiry_cutoff_source_date, expiry_day)
            self.assertEqual(path.audit.excluded_source_dates, (expiry_day, after_day))
            self.assertEqual(len(path.audit.sources), 1)
            assert path.terminal_event is not None
            self.assertLess(path.terminal_event.source_date, date(2022, 3, 1))

    def test_rejects_hash_mapping_order_window_and_off_grid_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            day = date(2022, 1, 5)
            valid = root / "valid.parquet"
            wrong_mapping = root / "wrong-mapping.parquet"
            off_grid = root / "off-grid.parquet"
            _write_source(
                valid,
                day=day,
                mappings=[_mapping("6EH2", 201)],
                events=[_event(_at(day, 10, 0), instrument_id=201)],
            )
            _write_source(
                wrong_mapping,
                day=day,
                mappings=[_mapping("6EM2", 999)],
                events=[_event(_at(day, 10, 0), instrument_id=999)],
            )
            _write_source(
                off_grid,
                day=day,
                mappings=[_mapping("6EH2", 201)],
                events=[
                    _event(
                        _at(day, 10, 0),
                        instrument_id=201,
                        bid_raw=_raw(BASE_TICKS) + 1,
                    )
                ],
            )

            with self.assertRaisesRegex(MultisessionReplayError, "SHA-256 differs"):
                build_multisession_path(
                    entry=_entry(),
                    sources=[SourceSession(day, valid)],
                    source_sha256_by_date={day: "0" * 64},
                    source_manifest_sha256=MANIFEST_SHA,
                )
            with self.assertRaisesRegex(MultisessionReplayError, "fixed raw symbol"):
                build_multisession_path(
                    entry=_entry(),
                    sources=[SourceSession(day, wrong_mapping)],
                    source_sha256_by_date={day: _sha(wrong_mapping)},
                    source_manifest_sha256=MANIFEST_SHA,
                )
            with self.assertRaisesRegex(MultisessionReplayError, "off-grid"):
                build_multisession_path(
                    entry=_entry(),
                    sources=[SourceSession(day, off_grid)],
                    source_sha256_by_date={day: _sha(off_grid)},
                    source_manifest_sha256=MANIFEST_SHA,
                )
            with self.assertRaisesRegex(MultisessionReplayError, "strictly increasing"):
                build_multisession_path(
                    entry=_entry(),
                    sources=[SourceSession(day, valid), SourceSession(day, valid)],
                    source_sha256_by_date={day: _sha(valid)},
                    source_manifest_sha256=MANIFEST_SHA,
                )

            many_days = tuple(day + timedelta(days=index) for index in range(21))
            with self.assertRaisesRegex(MultisessionReplayError, "exceeds 20"):
                build_multisession_path(
                    entry=_entry(raw_symbol="6EZ2", contract_month=date(2022, 12, 1)),
                    sources=[
                        SourceSession(value, root / f"missing-{index}.parquet")
                        for index, value in enumerate(many_days)
                    ],
                    source_sha256_by_date={
                        value: f"{index + 1:064x}" for index, value in enumerate(many_days)
                    },
                    source_manifest_sha256=MANIFEST_SHA,
                )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
