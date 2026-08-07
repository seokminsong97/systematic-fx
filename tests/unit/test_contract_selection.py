from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from systematic_fx.data.contract_selection import (
    CONTRACT_SELECTION_POLICY_VERSION,
    ContractSelectionError,
    resolve_6e_contract_month,
    resolve_active_6e_outrights,
    select_next_eligible_contract,
    summarize_previous_trade_volume,
)

type MappingSpec = tuple[str, int, date, date]
type Event = tuple[int, str, int]


def _midnight_ns(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp()) * 1_000_000_000


def _metadata(day: date, mappings: list[MappingSpec]) -> dict[bytes, bytes]:
    document = {
        "start": _midnight_ns(day),
        "end": _midnight_ns(day + timedelta(days=1)),
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
    return {b"dbn.metadata": json.dumps(document, sort_keys=True).encode()}


def _write_daily(
    path: Path,
    *,
    day: date,
    mappings: list[MappingSpec],
    events: list[Event],
    row_group_size: int = 2,
) -> None:
    schema = pa.schema(
        [
            pa.field("instrument_id", pa.uint32(), nullable=False),
            pa.field("action", pa.string(), nullable=False),
            pa.field("size", pa.uint32(), nullable=False),
        ],
        metadata=_metadata(day, mappings),
    )
    table = pa.Table.from_pydict(
        {
            "instrument_id": [event[0] for event in events],
            "action": [event[1] for event in events],
            "size": [event[2] for event in events],
        },
        schema=schema,
    )
    pq.write_table(table, path, row_group_size=row_group_size)


def _mapping(
    raw_symbol: str,
    instrument_id: int,
    *,
    start: date = date(2022, 1, 1),
    end: date = date(2023, 1, 1),
) -> MappingSpec:
    return raw_symbol, instrument_id, start, end


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _same_size_mutation(path: Path) -> None:
    payload = bytearray(path.read_bytes())
    payload[len(payload) // 2] ^= 1
    path.write_bytes(payload)


class ContractMonthTests(unittest.TestCase):
    def test_one_and_two_digit_years_use_the_source_date_era(self) -> None:
        cases = (
            ("6EH2", date(2022, 2, 1), date(2022, 3, 1)),
            ("6EZ9", date(2025, 6, 1), date(2029, 12, 1)),
            ("6EH22", date(2026, 1, 1), date(2022, 3, 1)),
            ("6EM7", date(2031, 1, 1), date(2027, 6, 1)),
        )
        for raw_symbol, source_date, expected in cases:
            with self.subTest(raw_symbol=raw_symbol, source_date=source_date):
                self.assertEqual(
                    resolve_6e_contract_month(raw_symbol, source_date=source_date),
                    expected,
                )

    def test_non_6e_spread_and_unparseable_symbols_are_rejected(self) -> None:
        for raw_symbol in ("6EH2-6EM2", "ESH2", "6E", "6EA2"):
            with self.subTest(raw_symbol=raw_symbol), self.assertRaises(ContractSelectionError):
                resolve_6e_contract_month(raw_symbol, source_date=date(2022, 2, 1))


class FooterResolutionTests(unittest.TestCase):
    def test_only_source_date_active_parseable_6e_outrights_are_returned(self) -> None:
        day = date(2022, 2, 28)
        mappings = [
            _mapping("6EH2", 101),
            _mapping("6EH2-6EM2", 201),
            _mapping("ESH2", 301),
            _mapping("BROKEN", 401),
            _mapping("6EM2", 501, start=date(2022, 1, 1), end=day),
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.parquet"
            _write_daily(path, day=day, mappings=mappings, events=[(101, "A", 1)])

            contracts = resolve_active_6e_outrights(path, source_date=day)

        self.assertEqual(
            [(item.instrument_id, item.raw_symbol, item.contract_month) for item in contracts],
            [(101, "6EH2", date(2022, 3, 1))],
        )

    def test_ambiguous_active_mapping_is_rejected(self) -> None:
        day = date(2022, 2, 28)
        mappings = [_mapping("6EH2", 101), _mapping("6EM2", 101)]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.parquet"
            _write_daily(path, day=day, mappings=mappings, events=[(101, "A", 1)])
            with self.assertRaisesRegex(ContractSelectionError, "maps to both"):
                resolve_active_6e_outrights(path, source_date=day)


class PreviousVolumeTests(unittest.TestCase):
    def test_streams_row_groups_sums_only_trades_and_hashes_canonical_summary(self) -> None:
        day = date(2022, 2, 28)
        mappings = [
            _mapping("6EH2", 101),
            _mapping("6EM2", 202),
            _mapping("6EU2", 303),
            _mapping("6EH2-6EM2", 404),
            _mapping("BROKEN", 505),
        ]
        events = [
            (101, "T", 100),
            (101, "A", 999),
            (202, "T", 20),
            (202, "T", 30),
            (404, "T", 1000),
            (505, "T", 7),
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.parquet"
            _write_daily(
                path,
                day=day,
                mappings=mappings,
                events=events,
                row_group_size=2,
            )

            source_sha256 = _sha256(path)
            first = summarize_previous_trade_volume(
                path,
                source_date=day,
                source_sha256=source_sha256,
            )
            second = summarize_previous_trade_volume(
                path,
                source_date=day,
                source_sha256=source_sha256,
            )

        self.assertEqual(first.row_groups_scanned, 3)
        self.assertEqual(first.rows_scanned, 6)
        self.assertEqual(first.trade_rows, 5)
        self.assertEqual(first.trade_volume, 1157)
        self.assertEqual(first.excluded_trade_rows, 2)
        self.assertEqual(first.excluded_trade_volume, 1007)
        volumes = {item.contract_month: item.trade_volume for item in first.contracts}
        self.assertEqual(
            volumes,
            {
                date(2022, 3, 1): 100,
                date(2022, 6, 1): 50,
                date(2022, 9, 1): 0,
            },
        )
        self.assertEqual(first.canonical_bytes, second.canonical_bytes)
        self.assertEqual(first.sha256, second.sha256)
        self.assertEqual(first.sha256, hashlib.sha256(first.canonical_bytes).hexdigest())
        self.assertEqual(first.as_dict()["policy_version"], CONTRACT_SELECTION_POLICY_VERSION)
        self.assertEqual(first.source_sha256, source_sha256)
        self.assertEqual(first.as_dict()["source_sha256"], source_sha256)

    def test_trade_with_no_active_footer_mapping_is_rejected(self) -> None:
        day = date(2022, 2, 28)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.parquet"
            _write_daily(
                path,
                day=day,
                mappings=[_mapping("6EH2", 101)],
                events=[(999, "T", 1)],
            )
            with self.assertRaisesRegex(ContractSelectionError, "no source-date-active mapping"):
                summarize_previous_trade_volume(path, source_date=day)


class SelectionTests(unittest.TestCase):
    def test_uses_only_previous_rows_excludes_expiry_month_and_ranks_deterministically(
        self,
    ) -> None:
        previous_day = date(2022, 2, 28)
        eligible_day = date(2022, 3, 1)
        previous_mappings = [
            _mapping("6EH2", 101),
            _mapping("6EM2", 202),
            _mapping("6EU2", 303),
        ]
        eligible_mappings = [
            _mapping("6EH2", 111),
            _mapping("6EM2", 222),
            _mapping("6EM22", 221),
            _mapping("6EU2", 333),
            _mapping("6EM2-6EU2", 444),
            _mapping("BROKEN", 555),
        ]
        previous_events = [
            (101, "T", 10_000),
            (202, "T", 50),
            (303, "T", 50),
        ]
        current_events = [
            (111, "T", 1),
            (222, "T", 1),
            (221, "T", 2),
            (333, "T", 999_999),
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            previous_path = root / "previous.parquet"
            eligible_path = root / "eligible.parquet"
            _write_daily(
                previous_path,
                day=previous_day,
                mappings=previous_mappings,
                events=previous_events,
                row_group_size=1,
            )
            _write_daily(
                eligible_path,
                day=eligible_day,
                mappings=eligible_mappings,
                events=current_events,
                row_group_size=1,
            )
            previous_sha256 = _sha256(previous_path)
            eligible_sha256 = _sha256(eligible_path)

            first = select_next_eligible_contract(
                previous_path,
                eligible_path,
                previous_source_date=previous_day,
                eligible_source_date=eligible_day,
                previous_source_sha256=previous_sha256,
                eligible_source_sha256=eligible_sha256,
            )
            second = select_next_eligible_contract(
                previous_path,
                eligible_path,
                previous_source_date=previous_day,
                eligible_source_date=eligible_day,
                previous_source_sha256=previous_sha256,
                eligible_source_sha256=eligible_sha256,
            )

        # H2 has the greatest prior volume but is excluded on entering March.
        self.assertEqual(
            [(item.instrument_id, item.raw_symbol) for item in first.expiry_exclusions],
            [(111, "6EH2")],
        )
        # M2 and U2 tie on prior volume: nearest later month wins.  The two M2
        # aliases then tie on month, so the lower current-day mapping ID wins.
        self.assertEqual(first.selected.instrument_id, 221)
        self.assertEqual(first.selected.raw_symbol, "6EM22")
        self.assertEqual(first.selected.previous_trade_volume, 50)
        # The eligible file gives U2 enormous current-day volume; it is ignored.
        self.assertEqual(first.candidates[-1].raw_symbol, "6EU2")
        self.assertEqual(first.candidates[-1].previous_trade_volume, 50)
        self.assertEqual(first.canonical_bytes, second.canonical_bytes)
        self.assertEqual(first.sha256, hashlib.sha256(first.canonical_bytes).hexdigest())
        self.assertFalse(first.as_dict()["information_boundary"]["eligible_source_rows_read"])
        self.assertEqual(first.previous_source_sha256, previous_sha256)
        self.assertEqual(first.eligible_source_sha256, eligible_sha256)
        self.assertEqual(first.as_dict()["previous_source_sha256"], previous_sha256)
        self.assertEqual(first.as_dict()["eligible_source_sha256"], eligible_sha256)

    def test_rejects_same_size_previous_and_current_source_mutations(self) -> None:
        previous_day = date(2022, 2, 28)
        eligible_day = date(2022, 3, 1)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            previous_path = root / "previous.parquet"
            eligible_path = root / "eligible.parquet"
            for path, day, instrument_id, symbol in (
                (previous_path, previous_day, 101, "6EM2"),
                (eligible_path, eligible_day, 201, "6EM2"),
            ):
                _write_daily(
                    path,
                    day=day,
                    mappings=[_mapping(symbol, instrument_id)],
                    events=[(instrument_id, "T", 10)],
                )
            previous_sha256 = _sha256(previous_path)
            eligible_sha256 = _sha256(eligible_path)
            previous_size = previous_path.stat().st_size
            eligible_size = eligible_path.stat().st_size

            _same_size_mutation(previous_path)
            self.assertEqual(previous_path.stat().st_size, previous_size)
            with self.assertRaisesRegex(ContractSelectionError, "SHA-256 differs"):
                select_next_eligible_contract(
                    previous_path,
                    eligible_path,
                    previous_source_date=previous_day,
                    eligible_source_date=eligible_day,
                    previous_source_sha256=previous_sha256,
                    eligible_source_sha256=eligible_sha256,
                )

            _write_daily(
                previous_path,
                day=previous_day,
                mappings=[_mapping("6EM2", 101)],
                events=[(101, "T", 10)],
            )
            _same_size_mutation(eligible_path)
            self.assertEqual(eligible_path.stat().st_size, eligible_size)
            with self.assertRaisesRegex(ContractSelectionError, "SHA-256 differs"):
                select_next_eligible_contract(
                    previous_path,
                    eligible_path,
                    previous_source_date=previous_day,
                    eligible_source_date=eligible_day,
                    previous_source_sha256=previous_sha256,
                    eligible_source_sha256=eligible_sha256,
                )

    def test_date_and_path_boundaries_prevent_current_or_future_row_use(self) -> None:
        day = date(2022, 2, 28)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.parquet"
            _write_daily(
                path,
                day=day,
                mappings=[_mapping("6EH2", 101)],
                events=[(101, "T", 1)],
            )
            with self.assertRaisesRegex(ContractSelectionError, "must precede"):
                select_next_eligible_contract(
                    path,
                    path,
                    previous_source_date=day,
                    eligible_source_date=day,
                )
            with self.assertRaisesRegex(ContractSelectionError, "different files"):
                select_next_eligible_contract(
                    path,
                    path,
                    previous_source_date=day - timedelta(days=1),
                    eligible_source_date=day,
                )


if __name__ == "__main__":
    unittest.main()
