from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from systematic_fx.backtest.barriers import BarrierOutcome, Direction, replay_barrier_surface
from systematic_fx.backtest.economics import EntryStatus as EconomicsEntryStatus
from systematic_fx.backtest.entry import (
    BboInvalidReason,
    EntryReason,
    EntryReplayError,
    EntryStatus,
    raw_6e_price_to_ticks,
    read_entry_path,
)
from systematic_fx.data.contract_selection import (
    ContractSelectionResult,
    select_next_eligible_contract,
)
from systematic_fx.data.contracts import (
    DATASET_NAME,
    PRICE_ENCODING,
    PRICE_SCALE_TEXT,
    SCHEMA_NAME,
    UNDEFINED_PRICE,
    expected_mbp10_schema,
)
from systematic_fx.research.screening_config import load_conservative_screening_bundle

ROOT = Path(__file__).resolve().parents[2]
SECOND = 1_000_000_000
TICK_RAW = 50_000
BASE_TICKS = 20_000

type MappingSpec = tuple[str, int, date, date]


def _ns(value: datetime) -> int:
    return int(value.timestamp()) * SECOND + value.microsecond * 1_000


def _at(day: date, hour: int, minute: int, second: float = 0) -> int:
    whole = int(second)
    microsecond = round((second - whole) * 1_000_000)
    return _ns(datetime.combine(day, time(hour, minute, whole, microsecond), tzinfo=UTC))


def _raw(ticks: int) -> int:
    return ticks * TICK_RAW


def _schema_metadata(day: date, mappings: list[MappingSpec]) -> dict[bytes, bytes]:
    start = _at(day, 0, 0)
    end = _at(day + timedelta(days=1), 0, 0)
    document = {
        "dataset": DATASET_NAME,
        "schema": SCHEMA_NAME,
        "version": 3,
        "stype_out": "instrument_id",
        "start": start,
        "end": end,
        "mappings": [
            {
                "raw_symbol": raw_symbol,
                "intervals": [
                    {
                        "start": interval_start.isoformat(),
                        "end": interval_end.isoformat(),
                        "symbol": str(instrument_id),
                    }
                ],
            }
            for raw_symbol, instrument_id, interval_start, interval_end in mappings
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


def _mapping(raw_symbol: str = "6EH2", instrument_id: int = 101) -> MappingSpec:
    return raw_symbol, instrument_id, date(2022, 1, 1), date(2023, 1, 1)


def _event(
    timestamp_ns: int,
    *,
    instrument_id: int = 101,
    action: str = "A",
    side: str | None = None,
    flags: int = 0,
    sequence: int = 1,
    bid_ticks: int | None = BASE_TICKS,
    ask_ticks: int | None = BASE_TICKS + 1,
    bid_raw: int | None = None,
    ask_raw: int | None = None,
    bid_size: int = 2,
    ask_size: int = 2,
    event_size: int = 1,
) -> dict[str, object]:
    bid_price = (
        bid_raw
        if bid_raw is not None
        else UNDEFINED_PRICE
        if bid_ticks is None
        else _raw(bid_ticks)
    )
    ask_price = (
        ask_raw
        if ask_raw is not None
        else UNDEFINED_PRICE
        if ask_ticks is None
        else _raw(ask_ticks)
    )
    return {
        "ts_recv": timestamp_ns,
        "instrument_id": instrument_id,
        "action": action,
        "side": side if side is not None else "N" if action == "R" else "B",
        "flags": flags,
        "sequence": sequence,
        "bid_px_00": bid_price,
        "ask_px_00": ask_price,
        "bid_sz_00": bid_size,
        "ask_sz_00": ask_size,
        "bid_ct_00": 1 if bid_price != UNDEFINED_PRICE and bid_size > 0 else 0,
        "ask_ct_00": 1 if ask_price != UNDEFINED_PRICE and ask_size > 0 else 0,
        "size": event_size,
    }


def _write_source(
    path: Path,
    *,
    day: date,
    events: list[dict[str, object]],
    mappings: list[MappingSpec] | None = None,
    row_group_size: int = 3,
) -> None:
    schema = expected_mbp10_schema(metadata=_schema_metadata(day, mappings or [_mapping()]))
    columns: dict[str, list[object]] = {field.name: [] for field in schema}
    for ordinal, event in enumerate(events):
        timestamp_ns = int(event["ts_recv"])
        for field in schema:
            name = field.name
            if name == "ts_recv" or name == "ts_event":
                value: object = timestamp_ns
            elif name == "rtype":
                value = 10
            elif name == "publisher_id":
                value = 1
            elif name == "instrument_id":
                value = event.get(name, 101)
            elif name == "action":
                value = event.get(name, "A")
            elif name == "side":
                value = event.get(name, "N" if event.get("action") == "R" else "B")
            elif name == "depth":
                value = 0
            elif name == "price":
                value = UNDEFINED_PRICE
            elif name == "size":
                value = event.get(name, 1)
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
            else:  # pragma: no cover - exact schema makes this defensive only
                raise AssertionError(name)
            columns[name].append(value)
    table = pa.Table.from_pydict(columns, schema=schema)
    pq.write_table(table, path, row_group_size=row_group_size)


def _prepare_selection(
    root: Path,
    *,
    eligible_events: list[dict[str, object]],
    eligible_mappings: list[MappingSpec] | None = None,
    seed_recovery: bool = True,
) -> tuple[ContractSelectionResult, Path, date, int]:
    previous_day = date(2022, 1, 3)
    eligible_day = date(2022, 1, 4)
    previous_path = root / "previous.parquet"
    eligible_path = root / "eligible.parquet"
    _write_source(
        previous_path,
        day=previous_day,
        events=[
            _event(
                _at(previous_day, 12, 0),
                action="T",
                event_size=100,
            )
        ],
    )
    decision = _at(eligible_day, 10, 0)
    recovery_prefix = (
        [
            _event(decision - 3 * SECOND, flags=32, sequence=900_001),
            _event(decision - 2 * SECOND, sequence=900_002),
        ]
        if seed_recovery
        else []
    )
    _write_source(
        eligible_path,
        day=eligible_day,
        events=[*recovery_prefix, *eligible_events],
        mappings=eligible_mappings,
    )
    selection = select_next_eligible_contract(
        previous_path,
        eligible_path,
        previous_source_date=previous_day,
        eligible_source_date=eligible_day,
    )
    return selection, eligible_path, eligible_day, decision


class EntryPathTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = load_conservative_screening_bundle(ROOT)

    def test_long_fill_integrates_selection_entry_path_terminal_and_barriers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            day = date(2022, 1, 4)
            decision = _at(day, 10, 0)
            events = [
                _event(decision - 500_000_000, sequence=10),
                _event(decision + 200_000_000, sequence=11),
                _event(decision + 800_000_000, sequence=12),
                _event(decision + SECOND, sequence=13, ask_size=3),
                _event(
                    decision + SECOND + 100_000_000,
                    sequence=14,
                    bid_ticks=BASE_TICKS + 26,
                    ask_ticks=BASE_TICKS + 27,
                ),
                _event(decision + 2 * SECOND, sequence=15),
                _event(decision + 2_500_000_000, sequence=16),
            ]
            selection, source, _, decision = _prepare_selection(
                root,
                eligible_events=events,
            )

            result = read_entry_path(
                selection=selection,
                source_parquet_path=source,
                decision_ts_recv_ns=decision,
                direction=Direction.LONG,
                policy=self.policy,
                terminal_cutoff_ts_recv_ns=decision + 3 * SECOND,
            )

        self.assertEqual(result.status, EntryStatus.ENTRY_FILLED)
        self.assertIs(result.status, EconomicsEntryStatus.ENTRY_FILLED)
        self.assertEqual(result.reason, EntryReason.FILLED_AT_DELAYED_OPPOSITE_BBO)
        self.assertEqual(result.fill_price_ticks, BASE_TICKS + 1)
        self.assertEqual(result.fill_price_raw, _raw(BASE_TICKS + 1))
        self.assertEqual(result.fill_quantity_contracts, 1)
        self.assertIsNotNone(result.executable_path)
        path = result.executable_path
        assert path is not None
        self.assertEqual(path.source_path_passes, 1)
        self.assertEqual(path.event_count, 2)
        self.assertEqual(path.terminal_quote.event_index, 8)  # type: ignore[union-attr]
        surface = replay_barrier_surface(
            direction=result.direction,
            entry_fill_price_ticks=result.fill_price_ticks,
            events=path,
            terminal_event=path.terminal_quote,
        )
        self.assertEqual(surface.cell(24, 24).outcome, BarrierOutcome.TP_FIRST)
        self.assertEqual(surface.thresholds.source_path_passes, 1)
        with self.assertRaisesRegex(EntryReplayError, "only once"):
            list(path)

        self.assertEqual(result.audit.selection_sha256, selection.sha256)
        self.assertEqual(
            result.audit.previous_volume_sha256,
            selection.previous_volume.sha256,
        )
        self.assertEqual(result.audit.eligibility_snapshot.sequence, 13)  # type: ignore[union-attr]
        self.assertEqual(result.audit.attempt_event.sequence, 13)  # type: ignore[union-attr]
        self.assertEqual(result.sha256, hashlib.sha256(result.canonical_bytes).hexdigest())
        self.assertEqual(result.as_dict()["status"], "ENTRY_FILLED")

    def test_short_fills_and_post_entry_reset_rearms_after_adjacent_clean_second(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            day = date(2022, 1, 4)
            decision = _at(day, 10, 0)
            events = [
                _event(decision, sequence=20),
                _event(decision + SECOND, sequence=21, bid_size=4),
                _event(
                    decision + SECOND + 100_000_000,
                    action="R",
                    sequence=22,
                    bid_ticks=None,
                    ask_ticks=None,
                    bid_size=0,
                    ask_size=0,
                ),
                _event(decision + 3 * SECOND, sequence=23),
                _event(decision + 3_200_000_000, sequence=24),
            ]
            selection, source, _, decision = _prepare_selection(root, eligible_events=events)
            result = read_entry_path(
                selection=selection,
                source_parquet_path=source,
                decision_ts_recv_ns=decision,
                direction="SHORT",
                policy=self.policy,
            )

        self.assertEqual(result.status, EntryStatus.ENTRY_FILLED)
        self.assertEqual(result.fill_price_ticks, BASE_TICKS)
        path = result.executable_path
        assert path is not None
        quotes = list(path)
        self.assertEqual([quote.valid for quote in quotes], [False, False, True])
        self.assertEqual(quotes[0].best_bid_ticks, None)

    def test_worse_attempt_price_is_outside_frozen_long_and_short_limits(self) -> None:
        day = date(2022, 1, 4)
        decision = _at(day, 10, 0)
        cases = (
            (
                Direction.LONG,
                _event(decision, sequence=40),
                _event(
                    decision + SECOND + 100_000_000,
                    sequence=41,
                    bid_ticks=BASE_TICKS + 1,
                    ask_ticks=BASE_TICKS + 2,
                ),
                BASE_TICKS + 1,
            ),
            (
                Direction.SHORT,
                _event(decision, sequence=40),
                _event(
                    decision + SECOND + 100_000_000,
                    sequence=41,
                    bid_ticks=BASE_TICKS - 1,
                    ask_ticks=BASE_TICKS,
                ),
                BASE_TICKS,
            ),
        )
        for direction, snapshot, attempt, expected_limit in cases:
            with self.subTest(direction=direction), tempfile.TemporaryDirectory() as directory:
                selection, source, _, boundary = _prepare_selection(
                    Path(directory),
                    eligible_events=[
                        snapshot,
                        attempt,
                        _event(decision + SECOND + 200_000_000, sequence=42),
                    ],
                )
                result = read_entry_path(
                    selection=selection,
                    source_parquet_path=source,
                    decision_ts_recv_ns=boundary,
                    direction=direction,
                    policy=self.policy,
                )

            self.assertEqual(result.status, EntryStatus.ENTRY_NOT_FILLED)
            self.assertEqual(result.reason, EntryReason.PRICE_OUTSIDE_LIMIT)
            self.assertEqual(result.audit.entry_limit_price_ticks, expected_limit)
            self.assertEqual(result.audit.eligibility_snapshot.sequence, 40)  # type: ignore[union-attr]
            self.assertEqual(result.audit.attempt_event.sequence, 41)  # type: ignore[union-attr]
            self.assertEqual(result.audit.source_rows_examined, 4)
            self.assertIsNone(result.executable_path)

    def test_improved_attempt_price_fills_at_current_bbo_not_frozen_cap(self) -> None:
        day = date(2022, 1, 4)
        decision = _at(day, 10, 0)
        cases = (
            (
                Direction.LONG,
                _event(
                    decision,
                    sequence=50,
                    bid_ticks=BASE_TICKS,
                    ask_ticks=BASE_TICKS + 2,
                ),
                _event(
                    decision + SECOND + 100_000_000,
                    sequence=51,
                    bid_ticks=BASE_TICKS,
                    ask_ticks=BASE_TICKS + 1,
                ),
                BASE_TICKS + 2,
                BASE_TICKS + 1,
            ),
            (
                Direction.SHORT,
                _event(
                    decision,
                    sequence=50,
                    bid_ticks=BASE_TICKS,
                    ask_ticks=BASE_TICKS + 2,
                ),
                _event(
                    decision + SECOND + 100_000_000,
                    sequence=51,
                    bid_ticks=BASE_TICKS + 1,
                    ask_ticks=BASE_TICKS + 2,
                ),
                BASE_TICKS,
                BASE_TICKS + 1,
            ),
        )
        for direction, snapshot, attempt, expected_limit, expected_fill in cases:
            with self.subTest(direction=direction), tempfile.TemporaryDirectory() as directory:
                selection, source, _, boundary = _prepare_selection(
                    Path(directory),
                    eligible_events=[snapshot, attempt],
                )
                result = read_entry_path(
                    selection=selection,
                    source_parquet_path=source,
                    decision_ts_recv_ns=boundary,
                    direction=direction,
                    policy=self.policy,
                )

            self.assertEqual(result.status, EntryStatus.ENTRY_FILLED)
            self.assertEqual(result.audit.entry_limit_price_ticks, expected_limit)
            self.assertEqual(result.fill_price_ticks, expected_fill)
            self.assertNotEqual(result.fill_price_ticks, result.audit.entry_limit_price_ticks)
            self.assertEqual(result.audit.eligibility_snapshot.sequence, 50)  # type: ignore[union-attr]
            self.assertEqual(result.audit.attempt_event.sequence, 51)  # type: ignore[union-attr]

    def test_event_exactly_at_eligibility_is_both_limit_snapshot_and_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            day = date(2022, 1, 4)
            decision = _at(day, 10, 0)
            selection, source, _, boundary = _prepare_selection(
                root,
                eligible_events=[
                    _event(decision, sequence=60),
                    _event(
                        decision + SECOND,
                        sequence=61,
                        bid_ticks=BASE_TICKS + 2,
                        ask_ticks=BASE_TICKS + 3,
                    ),
                ],
            )
            result = read_entry_path(
                selection=selection,
                source_parquet_path=source,
                decision_ts_recv_ns=boundary,
                direction=Direction.LONG,
                policy=self.policy,
            )

        self.assertEqual(result.status, EntryStatus.ENTRY_FILLED)
        self.assertEqual(result.audit.entry_limit_price_ticks, BASE_TICKS + 3)
        self.assertEqual(result.fill_price_ticks, BASE_TICKS + 3)
        self.assertEqual(result.audit.eligibility_snapshot.event_index, 3)  # type: ignore[union-attr]
        self.assertEqual(result.audit.attempt_event.event_index, 3)  # type: ignore[union-attr]
        self.assertEqual(
            result.as_dict()["audit"]["entry_gate"]["entry_limit"]["price_ticks"],
            BASE_TICKS + 3,
        )

    def test_missing_and_stale_decision_quotes_are_explicit_no_entry_states(self) -> None:
        day = date(2022, 1, 4)
        decision = _at(day, 10, 0)
        cases = (
            (
                [_event(decision + SECOND)],
                EntryReason.NO_DECISION_QUOTE,
                False,
            ),
            (
                [
                    _event(decision - SECOND - 1),
                    _event(decision + SECOND),
                ],
                EntryReason.STALE_DECISION_QUOTE,
                True,
            ),
        )
        for events, expected_reason, seed_recovery in cases:
            with self.subTest(reason=expected_reason), tempfile.TemporaryDirectory() as directory:
                selection, source, _, boundary = _prepare_selection(
                    Path(directory),
                    eligible_events=events,
                    seed_recovery=seed_recovery,
                )
                result = read_entry_path(
                    selection=selection,
                    source_parquet_path=source,
                    decision_ts_recv_ns=boundary,
                    direction="LONG",
                    policy=self.policy,
                )
            self.assertEqual(result.status, EntryStatus.ENTRY_NOT_FILLED)
            self.assertEqual(result.reason, expected_reason)
            self.assertEqual(result.fill_quantity_contracts, 0)
            self.assertIsNone(result.executable_path)

    def test_invalid_decision_route_and_reset_each_block_entry(self) -> None:
        day = date(2022, 1, 4)
        decision = _at(day, 10, 0)
        cases = (
            (
                [
                    _event(decision, bid_ticks=BASE_TICKS, ask_ticks=BASE_TICKS),
                    _event(decision + SECOND),
                ],
                EntryReason.INVALID_DECISION_BBO,
                BboInvalidReason.LOCKED_BOOK,
            ),
            (
                [
                    _event(decision),
                    _event(
                        decision + 500_000_000,
                        bid_ticks=BASE_TICKS + 2,
                        ask_ticks=BASE_TICKS + 1,
                    ),
                    _event(decision + SECOND),
                ],
                EntryReason.INVALID_BBO_DURING_ROUTE,
                BboInvalidReason.CROSSED_BOOK,
            ),
            (
                [
                    _event(decision),
                    _event(
                        decision + 500_000_000,
                        action="R",
                        bid_ticks=None,
                        ask_ticks=None,
                        bid_size=0,
                        ask_size=0,
                    ),
                    _event(decision + SECOND),
                ],
                EntryReason.RESET_DURING_ROUTE,
                BboInvalidReason.RESET,
            ),
        )
        for events, expected_reason, invalid_reason in cases:
            with self.subTest(reason=expected_reason), tempfile.TemporaryDirectory() as directory:
                selection, source, _, boundary = _prepare_selection(
                    Path(directory),
                    eligible_events=events,
                )
                result = read_entry_path(
                    selection=selection,
                    source_parquet_path=source,
                    decision_ts_recv_ns=boundary,
                    direction="LONG",
                    policy=self.policy,
                )
            self.assertEqual(result.status, EntryStatus.ENTRY_NOT_FILLED)
            self.assertEqual(result.reason, expected_reason)
            self.assertEqual(result.audit.failure_event.invalid_reason, invalid_reason)  # type: ignore[union-attr]

    def test_predecision_reset_requires_one_complete_fresh_second_to_rearm(self) -> None:
        day = date(2022, 1, 4)
        decision = _at(day, 10, 0)
        reset = _event(
            decision - 2 * SECOND,
            action="R",
            bid_ticks=None,
            ask_ticks=None,
            bid_size=0,
            ask_size=0,
        )
        cases = (
            (
                [
                    reset,
                    _event(decision - 500_000_000),
                    _event(decision),
                    _event(decision + SECOND),
                ],
                EntryStatus.ENTRY_NOT_FILLED,
            ),
            (
                [
                    reset,
                    _event(decision - SECOND),
                    _event(decision),
                    _event(decision + SECOND),
                ],
                EntryStatus.ENTRY_FILLED,
            ),
        )
        for events, expected_status in cases:
            with self.subTest(status=expected_status), tempfile.TemporaryDirectory() as directory:
                selection, source, _, boundary = _prepare_selection(
                    Path(directory),
                    eligible_events=events,
                )
                result = read_entry_path(
                    selection=selection,
                    source_parquet_path=source,
                    decision_ts_recv_ns=boundary,
                    direction="LONG",
                    policy=self.policy,
                )
            self.assertEqual(result.status, expected_status)

    def test_first_eligible_event_is_single_ioc_attempt_without_retry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            day = date(2022, 1, 4)
            decision = _at(day, 10, 0)
            selection, source, _, boundary = _prepare_selection(
                root,
                eligible_events=[
                    _event(decision),
                    _event(decision + SECOND, ask_size=0, sequence=31),
                    _event(decision + SECOND + 1, ask_size=10, sequence=32),
                ],
            )
            result = read_entry_path(
                selection=selection,
                source_parquet_path=source,
                decision_ts_recv_ns=boundary,
                direction="LONG",
                policy=self.policy,
            )

        self.assertEqual(result.status, EntryStatus.ENTRY_NOT_FILLED)
        self.assertEqual(result.reason, EntryReason.INSUFFICIENT_EXECUTABLE_SIZE)
        self.assertEqual(result.audit.attempt_event.sequence, 31)  # type: ignore[union-attr]
        self.assertEqual(result.audit.source_rows_examined, 4)

    def test_route_staleness_and_missing_eligibility_event_do_not_fill(self) -> None:
        day = date(2022, 1, 4)
        decision = _at(day, 10, 0)
        cases = (
            (
                [
                    _event(decision - 500_000_000),
                    _event(decision + 700_000_000),
                    _event(decision + SECOND),
                ],
                EntryReason.STALE_BBO_DURING_ROUTE,
            ),
            (
                [_event(decision), _event(decision + 800_000_000)],
                EntryReason.NO_ENTRY_ELIGIBILITY_EVENT,
            ),
        )
        for events, expected_reason in cases:
            with self.subTest(reason=expected_reason), tempfile.TemporaryDirectory() as directory:
                selection, source, _, boundary = _prepare_selection(
                    Path(directory),
                    eligible_events=events,
                )
                result = read_entry_path(
                    selection=selection,
                    source_parquet_path=source,
                    decision_ts_recv_ns=boundary,
                    direction="LONG",
                    policy=self.policy,
                )
            self.assertEqual(result.reason, expected_reason)

    def test_source_starts_unknown_and_long_gap_requires_a_new_marker(self) -> None:
        day = date(2022, 1, 4)
        decision = _at(day, 10, 0)
        cases = (
            (
                [_event(decision), _event(decision + SECOND)],
                BboInvalidReason.SOURCE_STATE_UNKNOWN,
            ),
            (
                [
                    _event(decision - 2 * SECOND, flags=32),
                    _event(decision),
                    _event(decision + SECOND),
                ],
                BboInvalidReason.RECOVERY_GAP,
            ),
        )
        for events, expected_reason in cases:
            with self.subTest(reason=expected_reason), tempfile.TemporaryDirectory() as directory:
                selection, source, _, boundary = _prepare_selection(
                    Path(directory),
                    eligible_events=events,
                    seed_recovery=False,
                )
                result = read_entry_path(
                    selection=selection,
                    source_parquet_path=source,
                    decision_ts_recv_ns=boundary,
                    direction="LONG",
                    policy=self.policy,
                )

            self.assertEqual(result.reason, EntryReason.INVALID_DECISION_BBO)
            failure = result.audit.failure_event
            assert failure is not None
            self.assertIn(expected_reason, failure.invalid_reasons)
            self.assertIn(BboInvalidReason.RECOVERY_REQUIRED, failure.invalid_reasons)
            self.assertIn(expected_reason, result.audit.observed_invalid_reasons)

    def test_maybe_bad_book_persists_until_valid_marker_and_adjacent_clean_second(
        self,
    ) -> None:
        day = date(2022, 1, 4)
        decision = _at(day, 10, 0)
        cases = (
            (
                [
                    _event(decision - SECOND, flags=4),
                    _event(decision),
                    _event(decision + SECOND),
                ],
                EntryStatus.ENTRY_NOT_FILLED,
            ),
            (
                [
                    _event(decision - 1_800_000_000, flags=4),
                    _event(decision - 1_500_000_000),
                    _event(decision - SECOND, flags=32),
                    _event(decision),
                    _event(decision + SECOND),
                ],
                EntryStatus.ENTRY_FILLED,
            ),
        )
        for events, expected_status in cases:
            with self.subTest(status=expected_status), tempfile.TemporaryDirectory() as directory:
                selection, source, _, boundary = _prepare_selection(
                    Path(directory),
                    eligible_events=events,
                )
                result = read_entry_path(
                    selection=selection,
                    source_parquet_path=source,
                    decision_ts_recv_ns=boundary,
                    direction="LONG",
                    policy=self.policy,
                )

            self.assertEqual(result.status, expected_status)
            self.assertIn(
                BboInvalidReason.MAYBE_BAD_BOOK,
                result.audit.observed_invalid_reasons,
            )
            if expected_status is EntryStatus.ENTRY_NOT_FILLED:
                failure = result.audit.failure_event
                assert failure is not None
                self.assertIn(BboInvalidReason.MAYBE_BAD_BOOK, failure.invalid_reasons)

    def test_empty_reset_rearms_but_invalid_reset_and_deep_structure_do_not(self) -> None:
        day = date(2022, 1, 4)
        decision = _at(day, 10, 0)
        empty_reset = _event(
            decision - 2 * SECOND,
            action="R",
            bid_ticks=None,
            ask_ticks=None,
            bid_size=0,
            ask_size=0,
        )
        invalid_reset = _event(decision - 2 * SECOND, action="R")
        invalid_snapshot = _event(decision - 2 * SECOND, flags=32)
        invalid_snapshot["bid_px_01"] = _raw(BASE_TICKS - 1)
        invalid_snapshot["bid_sz_01"] = 1
        invalid_snapshot["bid_ct_01"] = 0
        cases = (
            (empty_reset, EntryStatus.ENTRY_FILLED, BboInvalidReason.RESET),
            (
                invalid_reset,
                EntryStatus.ENTRY_NOT_FILLED,
                BboInvalidReason.INVALID_RECOVERY_MARKER,
            ),
            (
                invalid_snapshot,
                EntryStatus.ENTRY_NOT_FILLED,
                BboInvalidReason.INVALID_BOOK_STRUCTURE,
            ),
        )
        for marker, expected_status, audit_reason in cases:
            with self.subTest(reason=audit_reason), tempfile.TemporaryDirectory() as directory:
                selection, source, _, boundary = _prepare_selection(
                    Path(directory),
                    eligible_events=[
                        marker,
                        _event(decision - SECOND),
                        _event(decision),
                        _event(decision + SECOND),
                    ],
                    seed_recovery=False,
                )
                result = read_entry_path(
                    selection=selection,
                    source_parquet_path=source,
                    decision_ts_recv_ns=boundary,
                    direction="LONG",
                    policy=self.policy,
                )

            self.assertEqual(result.status, expected_status)
            self.assertIn(audit_reason, result.audit.observed_invalid_reasons)
            if expected_status is EntryStatus.ENTRY_NOT_FILLED:
                self.assertIn(
                    BboInvalidReason.INVALID_RECOVERY_MARKER,
                    result.audit.observed_invalid_reasons,
                )

    def test_marker_second_is_invalid_and_audit_retains_every_row_reason(self) -> None:
        day = date(2022, 1, 4)
        decision = _at(day, 10, 0)
        cases = (
            (
                _event(decision, flags=32),
                {BboInvalidReason.RECOVERY_MARKER, BboInvalidReason.RECOVERY_REQUIRED},
            ),
            (
                _event(
                    decision,
                    flags=4 | 8 | 32,
                    bid_ticks=BASE_TICKS,
                    ask_ticks=BASE_TICKS,
                    ask_size=0,
                ),
                {
                    BboInvalidReason.MAYBE_BAD_BOOK,
                    BboInvalidReason.BAD_TS_RECV,
                    BboInvalidReason.LOCKED_BOOK,
                    BboInvalidReason.MISSING_DEPTH,
                    BboInvalidReason.INVALID_BOOK_STRUCTURE,
                    BboInvalidReason.INVALID_RECOVERY_MARKER,
                },
            ),
        )
        for marker, expected_reasons in cases:
            with self.subTest(reasons=expected_reasons), tempfile.TemporaryDirectory() as directory:
                selection, source, _, boundary = _prepare_selection(
                    Path(directory),
                    eligible_events=[marker, _event(decision + SECOND)],
                )
                result = read_entry_path(
                    selection=selection,
                    source_parquet_path=source,
                    decision_ts_recv_ns=boundary,
                    direction="LONG",
                    policy=self.policy,
                )

            self.assertEqual(result.reason, EntryReason.INVALID_DECISION_BBO)
            failure = result.audit.failure_event
            assert failure is not None
            self.assertTrue(expected_reasons.issubset(failure.invalid_reasons))
            self.assertTrue(expected_reasons.issubset(result.audit.observed_invalid_reasons))

    def test_off_tick_price_and_non_five_minute_decision_are_rejected(self) -> None:
        self.assertEqual(raw_6e_price_to_ticks(_raw(BASE_TICKS)), BASE_TICKS)
        with self.assertRaisesRegex(EntryReplayError, "off the exact 6E tick grid"):
            raw_6e_price_to_ticks(_raw(BASE_TICKS) + 1)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            day = date(2022, 1, 4)
            decision = _at(day, 10, 0)
            selection, source, _, boundary = _prepare_selection(
                root,
                eligible_events=[
                    _event(decision, bid_raw=_raw(BASE_TICKS) + 1),
                    _event(decision + SECOND),
                ],
            )
            with self.assertRaisesRegex(EntryReplayError, "off the exact 6E tick grid"):
                read_entry_path(
                    selection=selection,
                    source_parquet_path=source,
                    decision_ts_recv_ns=boundary,
                    direction="LONG",
                    policy=self.policy,
                )
            with self.assertRaisesRegex(EntryReplayError, "right-closed five-minute"):
                read_entry_path(
                    selection=selection,
                    source_parquet_path=source,
                    decision_ts_recv_ns=boundary + 1,
                    direction="LONG",
                    policy=self.policy,
                )

    def test_selection_sha_and_source_date_boundaries_are_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            day = date(2022, 1, 4)
            decision = _at(day, 10, 0)
            selection, source, _, boundary = _prepare_selection(
                root,
                eligible_events=[_event(decision), _event(decision + SECOND)],
            )
            tampered = replace(selection, sha256="0" * 64)
            with self.assertRaisesRegex(EntryReplayError, "canonical SHA-256 mismatch"):
                read_entry_path(
                    selection=tampered,
                    source_parquet_path=source,
                    decision_ts_recv_ns=boundary,
                    direction="LONG",
                    policy=self.policy,
                )

            wrong_day = day + timedelta(days=1)
            wrong_source = root / "wrong-day.parquet"
            _write_source(
                wrong_source,
                day=wrong_day,
                events=[
                    _event(_at(wrong_day, 10, 0)),
                    _event(_at(wrong_day, 10, 0) + SECOND),
                ],
            )
            with self.assertRaisesRegex(EntryReplayError, "differs from selection eligible date"):
                read_entry_path(
                    selection=selection,
                    source_parquet_path=wrong_source,
                    decision_ts_recv_ns=boundary,
                    direction="LONG",
                    policy=self.policy,
                )


if __name__ == "__main__":
    unittest.main()
