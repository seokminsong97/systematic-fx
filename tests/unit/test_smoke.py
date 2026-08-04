import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from systematic_fx.data.smoke import (
    DEFAULT_UNDEFINED_PRICE,
    SmokeCheckError,
    smoke_check_parquet,
)


def _write_mbp10(
    path: Path,
    *,
    rows: dict[str, list[object]],
    mapped_ids: tuple[int, ...] = (101,),
    row_group_size: int = 2,
) -> None:
    dbn_metadata = {
        "mappings": [
            {
                "raw_symbol": f"6E-{instrument_id}",
                "intervals": [{"symbol": str(instrument_id)}],
            }
            for instrument_id in mapped_ids
        ]
    }
    schema = pa.schema(
        [
            pa.field("ts_recv", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("ts_event", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("instrument_id", pa.uint32(), nullable=False),
            pa.field("action", pa.string(), nullable=False),
            pa.field("side", pa.string(), nullable=False),
            pa.field("depth", pa.uint8(), nullable=False),
            pa.field("bid_px_00", pa.int64(), nullable=False),
            pa.field("ask_px_00", pa.int64(), nullable=False),
        ],
        metadata={
            b"dbn.metadata": json.dumps(dbn_metadata).encode(),
            b"mbo_mbp10.undefined_price": str(DEFAULT_UNDEFINED_PRICE).encode(),
        },
    )
    pq.write_table(pa.Table.from_pydict(rows, schema=schema), path, row_group_size=row_group_size)


class EventSmokeTest(unittest.TestCase):
    def test_bounded_scan_reports_bbo_masks_without_failing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.parquet"
            _write_mbp10(
                path,
                rows={
                    "ts_recv": [10, 20, 30, 40, 50, 60],
                    "ts_event": [9, 19, 29, 39, 49, 59],
                    "instrument_id": [101] * 6,
                    "action": ["A", "C", "N", "M", "N", "R"],
                    "side": ["A", "B", "N", "A", "B", "N"],
                    "depth": [0, 9, 1, 2, 3, 4],
                    # Zero and negative spread prices remain valid. Only the
                    # encoded sentinel makes the second BBO invalid.
                    "bid_px_00": [-2, 100, -1, 0, 100, 100],
                    "ask_px_00": [0, DEFAULT_UNDEFINED_PRICE, -1, -1, 101, 101],
                },
            )

            result = smoke_check_parquet(path, max_row_groups=2)

            self.assertEqual(result.row_groups_available, 3)
            self.assertEqual(result.row_groups_scanned, 2)
            self.assertEqual(result.rows_scanned, 4)
            self.assertEqual(result.invalid_bbo, 1)
            self.assertEqual(result.locked_bbo, 1)
            self.assertEqual(result.crossed_bbo, 1)
            self.assertEqual(result.structural_violations, 0)
            self.assertTrue(result.passed)
            self.assertEqual(json.loads(result.to_json()), result.as_dict())

    def test_structural_violations_include_row_group_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "violations.parquet"
            _write_mbp10(
                path,
                mapped_ids=(101,),
                rows={
                    "ts_recv": [10, 20, 15, 40],
                    "ts_event": [9, 19, 16, 39],
                    "instrument_id": [101, 101, 999, 101],
                    "action": ["A", "C", "Z", "T"],
                    "side": ["A", "B", "Q", "N"],
                    "depth": [0, 9, 10, 1],
                    "bid_px_00": [100, 100, 100, 100],
                    "ask_px_00": [101, 101, 101, 101],
                },
            )

            result = smoke_check_parquet(path, max_row_groups=2)

            self.assertEqual(result.ts_recv_regressions, 1)
            self.assertEqual(result.ts_recv_before_ts_event, 1)
            self.assertEqual(result.depth_out_of_range, 1)
            self.assertEqual(result.unknown_actions, 1)
            self.assertEqual(result.unknown_sides, 1)
            self.assertEqual(result.unknown_instrument_ids, 1)
            # Publisher and capture clocks are not guaranteed to be synchronized,
            # so ts_recv preceding ts_event remains diagnostic rather than fatal.
            self.assertEqual(result.structural_violations, 5)
            self.assertFalse(result.passed)

    def test_missing_dbn_mapping_metadata_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing-metadata.parquet"
            table = pa.table(
                {
                    "ts_recv": pa.array([], type=pa.timestamp("ns", tz="UTC")),
                    "ts_event": pa.array([], type=pa.timestamp("ns", tz="UTC")),
                    "instrument_id": pa.array([], type=pa.uint32()),
                    "action": pa.array([], type=pa.string()),
                    "side": pa.array([], type=pa.string()),
                    "depth": pa.array([], type=pa.uint8()),
                    "bid_px_00": pa.array([], type=pa.int64()),
                    "ask_px_00": pa.array([], type=pa.int64()),
                }
            )
            pq.write_table(table, path)

            with self.assertRaisesRegex(SmokeCheckError, "dbn.metadata"):
                smoke_check_parquet(path)

    def test_non_positive_row_group_limit_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "at least 1"):
            smoke_check_parquet(Path("unused.parquet"), max_row_groups=0)


if __name__ == "__main__":
    unittest.main()
