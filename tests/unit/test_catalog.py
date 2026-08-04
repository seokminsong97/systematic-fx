import json
import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

from systematic_fx.data.catalog import scan_catalog
from systematic_fx.data.contracts import expected_mbp10_schema


def _timestamp_ns(day: date) -> int:
    moment = datetime(day.year, day.month, day.day, tzinfo=UTC)
    return int(moment.timestamp()) * 1_000_000_000


def _write_valid_daily_file(root: Path, day: date, instrument_id: int) -> Path:
    next_day_ns = _timestamp_ns(day) + 86_400 * 1_000_000_000
    dbn = {
        "dataset": "GLBX.MDP3",
        "end": next_day_ns,
        "mappings": [
            {
                "intervals": [
                    {
                        "end": date.fromordinal(day.toordinal() + 1).isoformat(),
                        "start": day.isoformat(),
                        "symbol": str(instrument_id),
                    }
                ],
                "raw_symbol": "6EH7",
            },
            {
                "intervals": [
                    {
                        "end": date.fromordinal(day.toordinal() + 1).isoformat(),
                        "start": day.isoformat(),
                        "symbol": str(instrument_id + 1),
                    }
                ],
                "raw_symbol": "6EM7-6EH7",
            },
        ],
        "not_found": [],
        "partial": ["6EZ4"],
        "schema": "mbp-10",
        "start": _timestamp_ns(day),
        "stype_out": "instrument_id",
        "symbols": ["6E.FUT"],
        "version": 3,
    }
    metadata = {
        b"dbn.dataset": b"GLBX.MDP3",
        b"dbn.metadata": json.dumps(dbn, sort_keys=True, separators=(",", ":")).encode(),
        b"dbn.schema": b"mbp-10",
        b"dbn.version": b"3",
        b"mbo_mbp10.price_encoding": b"fixed",
        b"mbo_mbp10.price_scale": b"1e-9",
        b"mbo_mbp10.undefined_price": b"9223372036854775807",
    }
    schema = expected_mbp10_schema(metadata=metadata)
    table = pa.Table.from_batches([], schema=schema)
    path = (
        root
        / f"{day.year:04d}"
        / f"{day.month:02d}"
        / f"{day.day:02d}"
        / f"glbx-mdp3-{day:%Y%m%d}.mbp-10.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return path


class CatalogScanTest(unittest.TestCase):
    def test_scan_uses_footers_and_writes_deterministic_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "mbp-10"
            _write_valid_daily_file(root, date(2024, 1, 3), 30)
            _write_valid_daily_file(root, date(2024, 1, 2), 10)
            manifest = Path(directory) / "catalog.jsonl"

            with mock.patch("pyarrow.parquet.read_table", side_effect=AssertionError("row read")):
                summary = scan_catalog(root, manifest_path=manifest)

            self.assertEqual(summary.file_count, 2)
            self.assertEqual(summary.total_rows, 0)
            self.assertEqual(summary.mapping_interval_count, 4)
            self.assertEqual(summary.unique_instrument_count, 4)
            self.assertEqual(summary.outright_mapping_count, 2)
            self.assertEqual(summary.calendar_spread_mapping_count, 2)
            self.assertEqual(summary.unknown_mapping_count, 0)
            self.assertEqual(len(summary.schema_fingerprints), 1)
            self.assertEqual(summary.as_dict()["schema_fingerprint_count"], 1)
            self.assertEqual(summary.request_symbols, ("6E.FUT",))
            self.assertEqual(summary.partial_symbol_count, 2)
            self.assertEqual(summary.not_found_symbol_count, 0)
            self.assertEqual(summary.files_with_partial, 2)
            self.assertEqual(summary.first_source_date, date(2024, 1, 2))
            self.assertEqual(summary.last_source_date, date(2024, 1, 3))

            first_output = manifest.read_bytes()
            records = [json.loads(line) for line in first_output.splitlines()]
            self.assertEqual(records[0]["path"], "2024/01/02/glbx-mdp3-20240102.mbp-10.parquet")
            self.assertEqual(records[0]["contract"]["price_scale"], "1e-9")
            self.assertEqual(records[0]["schema_fingerprint"], summary.schema_fingerprints[0])
            self.assertEqual(records[0]["symbols"], ["6E.FUT"])
            self.assertEqual(records[0]["partial"], ["6EZ4"])
            self.assertEqual(records[0]["not_found"], [])
            self.assertEqual(len(records[0]["instrument_mappings"]), 2)

            scan_catalog(root, manifest_path=manifest)
            self.assertEqual(manifest.read_bytes(), first_output)

    def test_date_filters_and_limit_bound_the_pilot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "mbp-10"
            _write_valid_daily_file(root, date(2024, 1, 2), 10)
            _write_valid_daily_file(root, date(2024, 1, 3), 30)
            _write_valid_daily_file(root, date(2024, 1, 4), 50)
            manifest = Path(directory) / "pilot.jsonl"

            summary = scan_catalog(
                root,
                start_date=date(2024, 1, 3),
                end_date=date(2024, 1, 4),
                limit=1,
                manifest_path=manifest,
                include_mappings=False,
            )

            self.assertEqual(summary.file_count, 1)
            self.assertEqual(summary.first_source_date, date(2024, 1, 3))
            record = json.loads(manifest.read_text().strip())
            self.assertNotIn("instrument_mappings", record)
            self.assertEqual(record["source_date"], "2024-01-03")

    def test_zero_limit_creates_an_empty_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "mbp-10"
            _write_valid_daily_file(root, date(2024, 1, 2), 10)
            manifest = Path(directory) / "empty.jsonl"

            summary = scan_catalog(root, limit=0, manifest_path=manifest)

            self.assertEqual(summary.file_count, 0)
            self.assertEqual(manifest.read_text(), "")

    def test_partition_components_must_match_filename_date(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "mbp-10"
            original = _write_valid_daily_file(root, date(2024, 1, 2), 10)
            wrong = root / "2024" / "01" / "03" / original.name
            wrong.parent.mkdir(parents=True, exist_ok=True)
            original.rename(wrong)

            with self.assertRaisesRegex(ValueError, "source partition mismatch"):
                scan_catalog(root)


if __name__ == "__main__":
    unittest.main()
