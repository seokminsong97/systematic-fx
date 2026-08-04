import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from systematic_fx.data.contracts import UNDEFINED_PRICE, expected_mbp10_schema
from systematic_fx.features.pilot import (
    FIVE_MINUTE_SCHEMA,
    ONE_SECOND_SCHEMA,
    build_pilot_features,
)


def _write_five_minute_source(path: Path) -> None:
    dbn = {
        "dataset": "GLBX.MDP3",
        "schema": "mbp-10",
        "version": 3,
        "stype_out": "instrument_id",
        "mappings": [
            {
                "raw_symbol": "6EH2",
                "intervals": [{"start": "2022-01-01", "end": "2022-02-01", "symbol": "101"}],
            }
        ],
    }
    metadata = {
        b"dbn.dataset": b"GLBX.MDP3",
        b"dbn.schema": b"mbp-10",
        b"dbn.version": b"3",
        b"dbn.metadata": json.dumps(dbn, sort_keys=True).encode(),
        b"mbo_mbp10.price_encoding": b"fixed",
        b"mbo_mbp10.price_scale": b"1e-9",
        b"mbo_mbp10.undefined_price": str(UNDEFINED_PRICE).encode(),
    }
    schema = expected_mbp10_schema(metadata=metadata)
    columns: dict[str, list[object]] = {field.name: [] for field in schema}
    day_start_ns = 1_641_168_000_000_000_000

    for second in range(1, 301):
        timestamp = day_start_ns + second * 1_000_000_000
        action = "T" if second in (1, 300) else "M"
        side = "B" if second == 1 else "A" if second == 300 else "N"
        size = 4 if second == 1 else 1
        bid = 100_000 + second
        ask = bid + 2
        fixed = {
            "ts_recv": timestamp,
            "ts_event": timestamp - 1,
            "rtype": 10,
            "publisher_id": 1,
            "instrument_id": 101,
            "action": action,
            "side": side,
            "depth": 0,
            "price": bid,
            "size": size,
            "flags": 0,
            "ts_in_delta": 1,
            "sequence": second,
        }
        for name, value in fixed.items():
            columns[name].append(value)
        for level in range(10):
            suffix = f"{level:02d}"
            columns[f"bid_px_{suffix}"].append(bid - level)
            columns[f"ask_px_{suffix}"].append(ask + level)
            columns[f"bid_sz_{suffix}"].append(10 + level)
            columns[f"ask_sz_{suffix}"].append(20 + level)
            columns[f"bid_ct_{suffix}"].append(1)
            columns[f"ask_ct_{suffix}"].append(1)

    path.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pydict(columns, schema=schema),
        path,
        row_group_size=17,
    )


class PilotPipelineIntegrationTest(unittest.TestCase):
    def test_full_observed_window_is_valid_and_build_is_deterministic(self) -> None:
        with (
            tempfile.TemporaryDirectory() as source_directory,
            tempfile.TemporaryDirectory() as first_directory,
            tempfile.TemporaryDirectory() as second_directory,
        ):
            source = Path(source_directory).resolve() / "raw/source.parquet"
            first_root = Path(first_directory).resolve()
            second_root = Path(second_directory).resolve()
            _write_five_minute_source(source)

            first = build_pilot_features(
                source,
                data_root=first_root,
                instrument_id=101,
                symbol="6EH2",
                source_date=date(2022, 1, 3),
            )
            second = build_pilot_features(
                source,
                data_root=second_root,
                instrument_id=101,
                symbol="6EH2",
                source_date=date(2022, 1, 3),
            )

            self.assertEqual(first.source_sha256, second.source_sha256)
            self.assertEqual(first.one_second.sha256, second.one_second.sha256)
            self.assertEqual(first.five_minute.sha256, second.five_minute.sha256)
            self.assertEqual(first.one_second.rows, 300)
            self.assertEqual(first.five_minute.rows, 1)

            one_second_file = pq.ParquetFile(first.one_second.path)
            five_minute_file = pq.ParquetFile(first.five_minute.path)
            self.assertEqual(one_second_file.schema_arrow, ONE_SECOND_SCHEMA)
            self.assertEqual(five_minute_file.schema_arrow, FIVE_MINUTE_SCHEMA)
            self.assertEqual(
                one_second_file.schema_arrow.metadata[b"systematic_fx.research_eligible"],
                b"false",
            )

            summary = five_minute_file.read().to_pylist()[0]
            self.assertEqual(summary["observed_seconds"], 300)
            self.assertEqual(summary["missing_seconds"], 0)
            self.assertEqual(summary["valid_seconds"], 300)
            self.assertEqual(summary["invalid_seconds"], 0)
            self.assertEqual(summary["event_count"], 300)
            self.assertEqual(summary["trade_count"], 2)
            self.assertEqual(summary["trade_volume"], 5)
            self.assertEqual(summary["signed_trade_volume"], 3)
            self.assertEqual(summary["mid_px_x2_raw_open"], 200_004)
            self.assertEqual(summary["mid_px_x2_raw_close"], 200_602)
            self.assertTrue(summary["source_window_complete"])
            self.assertTrue(summary["closed_bucket"])
            self.assertTrue(summary["valid_window"])

            forbidden = {"trading_status", "label", "return", "pnl", "stale_seconds"}
            self.assertTrue(forbidden.isdisjoint(five_minute_file.schema_arrow.names))


if __name__ == "__main__":
    unittest.main()
