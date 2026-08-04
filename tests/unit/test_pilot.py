import hashlib
import json
import tempfile
import tomllib
import unittest
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from systematic_fx.data.contracts import UNDEFINED_PRICE, expected_mbp10_schema
from systematic_fx.features.pilot import (
    FEATURE_VERSION,
    FIVE_MINUTE_NS,
    FIVE_MINUTE_SCHEMA,
    ONE_SECOND_NS,
    ONE_SECOND_SCHEMA,
    PilotBuildError,
    PilotPathError,
    _right_closed_bucket_end_ns,
    build_pilot_features,
)

SOURCE_DATE = date(2022, 1, 3)
DAY_START_NS = 1_641_168_000 * ONE_SECOND_NS


def _metadata(mappings: list[tuple[str, int]] | None = None) -> dict[bytes, bytes]:
    mapping_rows = [
        {
            "raw_symbol": symbol,
            "intervals": [{"start": "2022-01-01", "end": "2022-02-01", "symbol": str(identifier)}],
        }
        for symbol, identifier in (mappings or [("6EH2", 101), ("6EM2", 202)])
    ]
    dbn = {
        "dataset": "GLBX.MDP3",
        "schema": "mbp-10",
        "version": 3,
        "stype_out": "instrument_id",
        "mappings": mapping_rows,
    }
    return {
        b"dbn.dataset": b"GLBX.MDP3",
        b"dbn.schema": b"mbp-10",
        b"dbn.version": b"3",
        b"dbn.metadata": json.dumps(dbn, sort_keys=True).encode(),
        b"mbo_mbp10.price_encoding": b"fixed",
        b"mbo_mbp10.price_scale": b"1e-9",
        b"mbo_mbp10.undefined_price": str(UNDEFINED_PRICE).encode(),
    }


def _event(
    offset_ns: int,
    *,
    instrument_id: int = 101,
    action: str = "A",
    side: str = "B",
    size: int = 1,
    flags: int = 0,
    bid: int = 100,
    ask: int = 102,
) -> dict[str, object]:
    return {
        "ts_recv": DAY_START_NS + offset_ns,
        "instrument_id": instrument_id,
        "action": action,
        "side": side,
        "size": size,
        "flags": flags,
        "bid": bid,
        "ask": ask,
    }


def _write_source(
    path: Path,
    events: list[dict[str, object]],
    *,
    row_group_size: int = 2,
    mappings: list[tuple[str, int]] | None = None,
) -> None:
    schema = expected_mbp10_schema(metadata=_metadata(mappings))
    columns: dict[str, list[object]] = {field.name: [] for field in schema}
    for sequence, event in enumerate(events, start=1):
        timestamp = int(event["ts_recv"])
        bid = int(event["bid"])
        ask = int(event["ask"])
        columns["ts_recv"].append(timestamp)
        columns["ts_event"].append(timestamp - 1)
        columns["rtype"].append(10)
        columns["publisher_id"].append(1)
        columns["instrument_id"].append(int(event["instrument_id"]))
        columns["action"].append(str(event["action"]))
        columns["side"].append(str(event["side"]))
        columns["depth"].append(0)
        columns["price"].append(bid)
        columns["size"].append(int(event["size"]))
        columns["flags"].append(int(event["flags"]))
        columns["ts_in_delta"].append(1)
        columns["sequence"].append(sequence)
        for level in range(10):
            suffix = f"{level:02d}"
            columns[f"bid_px_{suffix}"].append(
                UNDEFINED_PRICE if bid == UNDEFINED_PRICE else bid - level
            )
            columns[f"ask_px_{suffix}"].append(
                UNDEFINED_PRICE if ask == UNDEFINED_PRICE else ask + level
            )
            columns[f"bid_sz_{suffix}"].append(10 + level)
            columns[f"ask_sz_{suffix}"].append(20 + level)
            columns[f"bid_ct_{suffix}"].append(1)
            columns[f"ask_ct_{suffix}"].append(2)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pydict(columns, schema=schema),
        path,
        row_group_size=row_group_size,
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


class PilotBucketTest(unittest.TestCase):
    def test_frozen_config_lists_the_implemented_schema_only(self) -> None:
        config_path = Path(__file__).resolve().parents[2] / "configs/features/mbp10_pilot_v1.toml"
        with config_path.open("rb") as handle:
            config = tomllib.load(handle)

        self.assertEqual(config["feature_set"]["id"], FEATURE_VERSION)
        self.assertFalse(config["feature_set"]["research_eligible"])
        self.assertFalse(config["scope"]["automatic_active_contract_selection"])
        self.assertFalse(config["scope"]["automatic_roll_selection"])
        one_second_names = [
            item.split(":", maxsplit=1)[0] for item in config["one_second"]["schema"]["fields"]
        ]
        five_minute_names = [
            item.split(":", maxsplit=1)[0] for item in config["five_minute"]["schema"]["fields"]
        ]
        self.assertEqual(one_second_names, ONE_SECOND_SCHEMA.names)
        self.assertEqual(five_minute_names, FIVE_MINUTE_SCHEMA.names)

    def test_right_closed_boundaries(self) -> None:
        self.assertEqual(_right_closed_bucket_end_ns(0, ONE_SECOND_NS), 0)
        self.assertEqual(_right_closed_bucket_end_ns(1, ONE_SECOND_NS), ONE_SECOND_NS)
        self.assertEqual(
            _right_closed_bucket_end_ns(ONE_SECOND_NS, ONE_SECOND_NS),
            ONE_SECOND_NS,
        )
        self.assertEqual(
            _right_closed_bucket_end_ns(ONE_SECOND_NS + 1, ONE_SECOND_NS),
            2 * ONE_SECOND_NS,
        )
        self.assertEqual(
            _right_closed_bucket_end_ns(FIVE_MINUTE_NS, FIVE_MINUTE_NS),
            FIVE_MINUTE_NS,
        )

    def test_non_positive_bucket_width_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "positive"):
            _right_closed_bucket_end_ns(1, 0)


class PilotBuildTest(unittest.TestCase):
    def test_build_preserves_row_group_state_masks_prices_and_reports_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            source = root / "raw" / "source.parquet"
            _write_source(
                source,
                [
                    _event(100_000_000, action="A", bid=100, ask=102),
                    _event(500_000_000, instrument_id=202, bid=200, ask=202),
                    # Same observed second as row zero, but in the next row group.
                    _event(800_000_000, action="T", side="B", size=7, bid=101, ask=103),
                    _event(1_200_000_000, action="T", side="A", size=3, bid=102, ask=104),
                    _event(2_200_000_000, action="M", bid=105, ask=105),
                    _event(
                        10_200_000_000,
                        action="T",
                        side="N",
                        size=5,
                        bid=UNDEFINED_PRICE,
                        ask=UNDEFINED_PRICE,
                    ),
                ],
                row_group_size=2,
            )

            report = build_pilot_features(
                source,
                data_root=root,
                instrument_id=101,
                symbol="6EH2",
                source_date=SOURCE_DATE,
            )

            expected_1s = (
                root
                / "derived/features_1s/version=mbp10_pilot_v1"
                / "contract=6EH2/source_date=2022-01-03/part-000.parquet"
            )
            expected_5m = (
                root
                / "derived/research_5m/version=mbp10_pilot_v1"
                / "contract=6EH2/source_date=2022-01-03/part-000.parquet"
            )
            self.assertEqual(Path(report.one_second.path), expected_1s)
            self.assertEqual(Path(report.five_minute.path), expected_5m)
            self.assertFalse(report.research_eligible)
            self.assertEqual(report.feature_version, FEATURE_VERSION)
            self.assertEqual(report.selected_rows, 5)
            self.assertEqual(report.late_rows_ignored, 0)
            self.assertEqual(report.one_second.sha256, _sha256(expected_1s))
            self.assertEqual(report.five_minute.sha256, _sha256(expected_5m))
            self.assertEqual(len(list((root / "derived").rglob("*.parquet"))), 2)

            one_second_file = pq.ParquetFile(expected_1s)
            self.assertEqual(one_second_file.schema_arrow, ONE_SECOND_SCHEMA)
            rows = one_second_file.read().to_pylist()
            self.assertEqual(len(rows), 4)

            first = rows[0]
            self.assertEqual(first["event_count"], 2)
            self.assertEqual(first["action_a_count"], 1)
            self.assertEqual(first["action_t_count"], 1)
            self.assertEqual(first["source_last_row"], 2)
            self.assertEqual(first["bid_px_00_raw"], 101)
            self.assertEqual(first["ask_px_00_raw"], 103)
            self.assertEqual(first["mid_px_x2_raw"], 204)
            self.assertEqual(first["spread_raw"], 2)
            self.assertEqual(first["trade_volume"], 7)
            self.assertEqual(first["aggressor_buy_volume"], 7)
            self.assertEqual(first["signed_trade_volume"], 7)
            self.assertTrue(first["observed_second"])
            self.assertFalse(first["missing_second"])
            self.assertTrue(first["valid_second"])
            self.assertEqual(first["bid_cum_size_l3"], 33)
            self.assertEqual(first["ask_cum_size_l3"], 63)

            second = rows[1]
            self.assertEqual(second["aggressor_sell_volume"], 3)
            self.assertEqual(second["signed_trade_volume"], -3)
            self.assertTrue(second["valid_second"])
            self.assertTrue(rows[2]["locked_book"])
            self.assertFalse(rows[2]["valid_second"])
            self.assertTrue(rows[3]["book_missing"])
            self.assertIsNone(rows[3]["bid_px_00_raw"])
            self.assertIsNone(rows[3]["mid_px_x2_raw"])
            self.assertEqual(rows[3]["unknown_side_trade_volume"], 5)

            five_minute_file = pq.ParquetFile(expected_5m)
            self.assertEqual(five_minute_file.schema_arrow, FIVE_MINUTE_SCHEMA)
            summary = five_minute_file.read().to_pylist()[0]
            self.assertEqual(summary["observed_seconds"], 4)
            self.assertEqual(summary["missing_seconds"], 296)
            self.assertEqual(summary["valid_seconds"], 2)
            self.assertEqual(summary["invalid_seconds"], 2)
            self.assertEqual(summary["trade_volume"], 15)
            self.assertEqual(summary["signed_trade_volume"], 4)
            self.assertEqual(summary["mid_px_x2_raw_open"], 204)
            self.assertEqual(summary["mid_px_x2_raw_close"], 206)
            self.assertTrue(summary["source_window_complete"])
            self.assertTrue(summary["closed_bucket"])
            self.assertFalse(summary["valid_window"])
            self.assertNotIn("trading_status", five_minute_file.schema_arrow.names)

            self.assertEqual(json.loads(report.to_json()), report.as_dict())

            with self.assertRaisesRegex(PilotPathError, "overwrite"):
                build_pilot_features(
                    source,
                    data_root=root,
                    instrument_id=101,
                    symbol="6EH2",
                    source_date=SOURCE_DATE,
                )

    def test_future_and_late_rows_do_not_rewrite_closed_seconds(self) -> None:
        base_events = [
            _event(100_000_000, action="A", bid=100, ask=102),
            _event(1_100_000_000, action="M", bid=101, ask=103),
        ]
        extended_events = [
            *base_events,
            # This opens the next five-minute bucket as well as a future second.
            _event(301_100_000_000, action="M", bid=105, ask=107),
            # Arrives physically after the future row but belongs to the first bucket.
            _event(200_000_000, action="T", side="B", size=999, bid=900, ask=902),
        ]
        with (
            tempfile.TemporaryDirectory() as first_directory,
            tempfile.TemporaryDirectory() as second_directory,
        ):
            first_root = Path(first_directory)
            second_root = Path(second_directory)
            first_source = first_root / "raw/source.parquet"
            second_source = second_root / "raw/source.parquet"
            _write_source(first_source, base_events, row_group_size=1)
            _write_source(second_source, extended_events, row_group_size=1)

            first_report = build_pilot_features(
                first_source,
                data_root=first_root,
                instrument_id=101,
                symbol="6EH2",
                source_date=SOURCE_DATE,
            )
            second_report = build_pilot_features(
                second_source,
                data_root=second_root,
                instrument_id=101,
                symbol="6EH2",
                source_date=SOURCE_DATE,
            )
            first_rows = pq.read_table(first_report.one_second.path).to_pylist()
            second_rows = pq.read_table(second_report.one_second.path).to_pylist()
            first_five_minute = pq.read_table(first_report.five_minute.path).to_pylist()
            second_five_minute = pq.read_table(second_report.five_minute.path).to_pylist()

            self.assertEqual(second_report.late_rows_ignored, 1)
            self.assertEqual(first_rows, second_rows[:2])
            self.assertEqual(first_five_minute, second_five_minute[:1])
            self.assertEqual(second_rows[0]["trade_volume"], 0)
            self.assertEqual(len(second_rows), 3)

    def test_selection_and_output_path_guards(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "raw/source.parquet"
            _write_source(source, [_event(100_000_000)])

            with self.assertRaisesRegex(PilotBuildError, "safe outright"):
                build_pilot_features(
                    source,
                    data_root=root,
                    instrument_id=101,
                    symbol="../6EH2",
                    source_date=SOURCE_DATE,
                )
            with self.assertRaisesRegex(PilotBuildError, "exactly one"):
                build_pilot_features(
                    source,
                    data_root=root,
                    instrument_id=202,
                    symbol="6EH2",
                    source_date=SOURCE_DATE,
                )

            derived_source = root / "derived/raw.parquet"
            _write_source(derived_source, [_event(100_000_000)])
            with self.assertRaisesRegex(PilotPathError, "must not be inside"):
                build_pilot_features(
                    derived_source,
                    data_root=root,
                    instrument_id=101,
                    symbol="6EH2",
                    source_date=SOURCE_DATE,
                )

    def test_symlinked_derived_directory_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as outside:
            root = Path(directory)
            source = root / "raw/source.parquet"
            _write_source(source, [_event(100_000_000)])
            (root / "derived").symlink_to(Path(outside), target_is_directory=True)

            with self.assertRaisesRegex(PilotPathError, "must not be a symlink"):
                build_pilot_features(
                    source,
                    data_root=root,
                    instrument_id=101,
                    symbol="6EH2",
                    source_date=SOURCE_DATE,
                )


if __name__ == "__main__":
    unittest.main()
