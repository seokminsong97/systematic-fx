import hashlib
import json
import tempfile
import unittest
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest import mock

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from systematic_fx.data import qc_mutations, quality
from systematic_fx.data.contracts import UNDEFINED_PRICE, expected_mbp10_schema
from systematic_fx.data.quality import (
    StructuralQcError,
    load_structural_qc_config,
    scan_structural_quality,
)

CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs/data/mbp10_structural_qc_v1.toml"


def _timestamp_ns(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp()) * 1_000_000_000


def _metadata(day: date, instrument_id: int | tuple[int, ...]) -> dict[bytes, bytes]:
    start_ns = _timestamp_ns(day)
    end_day = day + timedelta(days=1)
    instrument_ids = (instrument_id,) if isinstance(instrument_id, int) else instrument_id
    dbn = {
        "dataset": "GLBX.MDP3",
        "end": start_ns + 86_400 * 1_000_000_000,
        "mappings": [
            {
                "intervals": [
                    {
                        "end": end_day.isoformat(),
                        "start": day.isoformat(),
                        "symbol": str(mapped_id),
                    }
                ],
                "raw_symbol": f"6E{'HMUZ'[index % 4]}4",
            }
            for index, mapped_id in enumerate(instrument_ids)
        ],
        "not_found": [],
        "partial": [],
        "schema": "mbp-10",
        "start": start_ns,
        "stype_out": "instrument_id",
        "symbols": ["6E.FUT"],
        "version": 3,
    }
    return {
        b"dbn.dataset": b"GLBX.MDP3",
        b"dbn.metadata": json.dumps(dbn, sort_keys=True, separators=(",", ":")).encode(),
        b"dbn.schema": b"mbp-10",
        b"dbn.version": b"3",
        b"mbo_mbp10.price_encoding": b"fixed",
        b"mbo_mbp10.price_scale": b"1e-9",
        b"mbo_mbp10.undefined_price": str(UNDEFINED_PRICE).encode(),
    }


def _book(*, bid: int, ask: int, empty: bool = False) -> dict[str, int]:
    output: dict[str, int] = {}
    for level in range(10):
        suffix = f"{level:02d}"
        output[f"bid_px_{suffix}"] = UNDEFINED_PRICE if empty else bid - level
        output[f"ask_px_{suffix}"] = UNDEFINED_PRICE if empty else ask + level
        output[f"bid_sz_{suffix}"] = 0 if empty else 5
        output[f"ask_sz_{suffix}"] = 0 if empty else 5
        output[f"bid_ct_{suffix}"] = 0 if empty else 1
        output[f"ask_ct_{suffix}"] = 0 if empty else 1
    return output


def _valid_rows(day: date, instrument_id: int) -> list[dict[str, object]]:
    start = _timestamp_ns(day)
    specifications = (
        (100, 90, 10, "A", "B", 0, 100, 101, False, 0, 1),
        (200, 190, 20, "M", "B", 0, 100, 100, False, 0, 2),
        (150, 140, 30, "R", "N", 0, 0, 0, True, 40, 1),
        (300, 290, 40, "A", "A", 0, 102, 101, False, 0, 4),
    )
    rows: list[dict[str, object]] = []
    for recv, event, price, action, side, depth, bid, ask, empty, flags, sequence in specifications:
        row: dict[str, object] = {
            "action": action,
            "depth": depth,
            "flags": flags,
            "instrument_id": instrument_id,
            "price": UNDEFINED_PRICE if action == "R" else price,
            "publisher_id": 1,
            "rtype": 10,
            "sequence": sequence,
            "side": side,
            "size": 1,
            "ts_event": start + event,
            "ts_in_delta": -1 if sequence == 2 else 1,
            "ts_recv": start + recv,
        }
        row.update(_book(bid=bid, ask=ask, empty=empty))
        rows.append(row)
    return rows


def _structural_row(
    day: date,
    *,
    instrument_id: int,
    offset_ns: int,
    sequence: int,
    action: str,
    bid: int = 100,
    ask: int = 101,
    flags: int = 0,
    empty: bool = False,
) -> dict[str, object]:
    start = _timestamp_ns(day)
    row: dict[str, object] = {
        "action": action,
        "depth": 0,
        "flags": flags,
        "instrument_id": instrument_id,
        "price": UNDEFINED_PRICE if action == "R" else bid,
        "publisher_id": 1,
        "rtype": 10,
        "sequence": sequence,
        "side": "N" if action == "R" else "B",
        "size": 1,
        "ts_event": start + offset_ns - 1,
        "ts_in_delta": 1,
        "ts_recv": start + offset_ns,
    }
    row.update(_book(bid=bid, ask=ask, empty=empty))
    return row


def _source_path(data_root: Path, day: date) -> Path:
    return (
        data_root
        / "mbp-10"
        / f"{day.year:04d}"
        / f"{day.month:02d}"
        / f"{day.day:02d}"
        / f"glbx-mdp3-{day:%Y%m%d}.mbp-10.parquet"
    )


def _write_source(
    data_root: Path,
    day: date,
    rows: list[dict[str, object]],
    *,
    instrument_id: int | tuple[int, ...] = 101,
    row_group_size: int = 2,
) -> Path:
    schema = expected_mbp10_schema(metadata=_metadata(day, instrument_id))
    columns = {field.name: [row[field.name] for row in rows] for field in schema}
    table = pa.Table.from_pydict(columns, schema=schema)
    path = _source_path(data_root, day)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, row_group_size=row_group_size)
    return path


def _write_source_manifest(data_root: Path, paths: list[Path]) -> Path:
    directory = data_root / "derived" / "manifests"
    directory.mkdir(parents=True, exist_ok=True)
    records = []
    for path in sorted(paths):
        relative_uri = path.relative_to(data_root / "mbp-10").as_posix()
        source_date = date.fromisoformat("-".join(path.parts[-4:-1]))
        records.append(
            {
                "byte_size": path.stat().st_size,
                "relative_uri": relative_uri,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "source_date": source_date.isoformat(),
            }
        )
    manifest = directory / "mbp10_source_sha256_v1.jsonl"
    manifest.write_bytes(
        b"".join(
            (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode()
            for record in records
        )
    )
    return manifest


class StructuralQcUnitTest(unittest.TestCase):
    def test_config_is_frozen_and_canonically_fingerprinted(self) -> None:
        config = load_structural_qc_config(CONFIG_PATH)

        self.assertEqual(config.checker_version, quality.CHECKER_VERSION)
        self.assertEqual(config.expected_publisher_id, 1)
        self.assertEqual(config.known_actions, ("A", "C", "M", "N", "R", "T"))
        self.assertEqual(len(config.sha256), 64)
        self.assertEqual(config.sha256, load_structural_qc_config(CONFIG_PATH).sha256)

    def test_bad_timestamp_rows_do_not_hide_trusted_subsequence_regression(self) -> None:
        values = quality._instrument_bounds(
            np.array([1, 1, 1], dtype=np.uint64),
            np.array([101, 101, 101], dtype=np.uint64),
            np.array([100, 50, 60], dtype=np.int64),
            np.array([0, quality.F_BAD_TS_RECV, 0], dtype=np.uint8),
        )

        all_regressions, trusted_regressions, exempted, bounds = values
        self.assertEqual(all_regressions, 1)
        self.assertEqual(exempted, 1)
        self.assertEqual(trusted_regressions, 1)
        self.assertEqual(bounds, [[1, 101, 100, 0, 60, 0, 100, 60]])


class StructuralQcSyntheticParquetTest(unittest.TestCase):
    def test_complete_scan_is_canonical_diagnostic_only_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            day = date(2024, 1, 2)
            source = _write_source(data_root, day, _valid_rows(day, 101))
            source_manifest = _write_source_manifest(data_root, [source])
            progress: list[quality.StructuralQcProgress] = []

            report = scan_structural_quality(
                data_root,
                config_path=CONFIG_PATH,
                source_manifest_path=source_manifest,
                progress_callback=progress.append,
            )

            self.assertEqual(report.status, "COMPLETE")
            self.assertEqual(report.file_count, 1)
            self.assertEqual(report.row_group_count, 2)
            self.assertEqual(report.row_count, 4)
            self.assertEqual(report.passed_file_count, 1)
            self.assertEqual(report.failed_file_count, 0)
            self.assertEqual(report.hard_violation_count, 0)
            self.assertEqual(report.scanned_row_group_count, 2)
            self.assertTrue(report.manifest_path.is_relative_to(data_root.resolve() / "derived"))
            self.assertEqual(
                report.manifest_sha256,
                hashlib.sha256(report.manifest_path.read_bytes()).hexdigest(),
            )
            raw_line = report.manifest_path.read_bytes()
            record = json.loads(raw_line)
            self.assertEqual(
                raw_line,
                (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode(),
            )
            self.assertEqual(record["artifact_schema"], quality.FILE_ARTIFACT_SCHEMA)
            self.assertEqual(record["checker_version"], quality.CHECKER_VERSION)
            self.assertEqual(record["result"], "PASS")
            self.assertFalse(record["research_eligible"])
            self.assertTrue(record["coverage_complete"])
            self.assertEqual(record["hard_violation_count"], 0)
            self.assertEqual(record["diagnostic_counts"]["locked_bbo_rows"], 1)
            self.assertEqual(record["diagnostic_counts"]["crossed_bbo_rows"], 1)
            self.assertEqual(
                record["diagnostic_counts"]["bad_ts_recv_exempted_regression_per_instrument"],
                1,
            )
            self.assertEqual(
                [event.status for event in progress], ["SCANNED", "SCANNED", "COMPLETE"]
            )

            first_manifest = report.manifest_path.read_bytes()
            with mock.patch.object(
                quality,
                "_scan_row_group",
                side_effect=AssertionError("completed row group was rescanned"),
            ):
                rerun = scan_structural_quality(
                    data_root,
                    config_path=CONFIG_PATH,
                    source_manifest_path=source_manifest,
                )
            self.assertEqual(rerun.resumed_row_group_count, 2)
            self.assertEqual(rerun.scanned_row_group_count, 0)
            self.assertEqual(rerun.manifest_path.read_bytes(), first_manifest)

    def test_zero_tolerance_hard_checks_fail_the_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            day = date(2024, 1, 2)
            rows = _valid_rows(day, 101)
            bad = rows[-1]
            bad.update(
                {
                    "action": "R",
                    "depth": 10,
                    "instrument_id": 999,
                    "publisher_id": 2,
                    "rtype": 9,
                    "side": "B",
                    "ts_recv": _timestamp_ns(day) - 1,
                }
            )
            bad["bid_px_00"] = UNDEFINED_PRICE
            bad["ask_px_00"] = UNDEFINED_PRICE
            bad["bid_sz_00"] = 1
            bad["ask_sz_00"] = 1
            bad["bid_ct_00"] = 1
            bad["ask_ct_00"] = 1
            bad["bid_px_01"] = 100
            bad["ask_px_01"] = 100
            bad["bid_sz_01"] = 0
            bad["ask_sz_01"] = 0
            bad["bid_ct_01"] = 2
            bad["ask_ct_01"] = 2
            bad["bid_px_02"] = 100
            bad["ask_px_02"] = 100
            source = _write_source(data_root, day, rows)
            source_manifest = _write_source_manifest(data_root, [source])

            report = scan_structural_quality(
                data_root, config_path=CONFIG_PATH, source_manifest_path=source_manifest
            )

            record = json.loads(report.manifest_path.read_text())
            hard = record["hard_violation_counts"]
            self.assertEqual(record["result"], "FAIL")
            self.assertGreater(record["hard_violation_count"], 0)
            for name in (
                "unexpected_rtype",
                "unexpected_publisher_id",
                "reset_side_not_none",
                "reset_book_not_empty",
                "depth_out_of_range",
                "unmapped_instrument_id",
                "ts_recv_outside_request_range",
                "book_level_noncontiguous",
                "defined_book_price_zero_size",
                "defined_book_count_exceeds_size",
                "undefined_book_price_nonzero_size",
            ):
                self.assertGreater(hard[name], 0, name)

    def test_trusted_timestamp_state_crosses_row_groups_but_not_bad_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            day = date(2024, 1, 2)
            rows = _valid_rows(day, 101)
            # Row group 1 ends at trusted 200. Row group 2 starts with bad 150,
            # then trusted 160. Adjacent/all-row logic alone would miss 200->160.
            rows[-1]["ts_recv"] = _timestamp_ns(day) + 160
            source = _write_source(data_root, day, rows)
            source_manifest = _write_source_manifest(data_root, [source])

            report = scan_structural_quality(
                data_root, config_path=CONFIG_PATH, source_manifest_path=source_manifest
            )

            record = json.loads(report.manifest_path.read_text())
            self.assertEqual(record["result"], "FAIL")
            self.assertEqual(
                record["hard_violation_counts"]["trusted_ts_recv_regression_per_instrument"],
                1,
            )

    def test_clean_trade_book_mutation_is_hard_across_row_groups(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            day = date(2024, 1, 2)
            rows = [
                _structural_row(
                    day,
                    instrument_id=101,
                    offset_ns=100,
                    sequence=1,
                    action="A",
                    bid=100,
                    ask=101,
                ),
                _structural_row(
                    day,
                    instrument_id=101,
                    offset_ns=200,
                    sequence=2,
                    action="T",
                    bid=101,
                    ask=102,
                ),
            ]
            source = _write_source(data_root, day, rows, row_group_size=1)
            source_manifest = _write_source_manifest(data_root, [source])

            report = scan_structural_quality(
                data_root, config_path=CONFIG_PATH, source_manifest_path=source_manifest
            )

            record = json.loads(report.manifest_path.read_text())
            self.assertEqual(record["result"], "FAIL")
            self.assertEqual(record["hard_violation_counts"]["clean_trade_none_book_mutation"], 1)

    def test_maybe_bad_book_stays_invalid_until_snapshot_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            day = date(2024, 1, 2)
            rows = [
                _structural_row(
                    day,
                    instrument_id=101,
                    offset_ns=100,
                    sequence=1,
                    action="R",
                    empty=True,
                ),
                _structural_row(
                    day,
                    instrument_id=101,
                    offset_ns=200,
                    sequence=2,
                    action="A",
                    bid=100,
                    ask=101,
                ),
                _structural_row(
                    day,
                    instrument_id=101,
                    offset_ns=300,
                    sequence=3,
                    action="M",
                    bid=100,
                    ask=101,
                    flags=quality.F_MAYBE_BAD_BOOK,
                ),
                _structural_row(
                    day,
                    instrument_id=101,
                    offset_ns=400,
                    sequence=4,
                    action="A",
                    bid=101,
                    ask=102,
                ),
                # This mutation remains non-gating inside the invalid epoch.
                _structural_row(
                    day,
                    instrument_id=101,
                    offset_ns=500,
                    sequence=5,
                    action="T",
                    bid=102,
                    ask=103,
                ),
                _structural_row(
                    day,
                    instrument_id=101,
                    offset_ns=600,
                    sequence=6,
                    action="A",
                    bid=103,
                    ask=104,
                    flags=quality.F_SNAPSHOT,
                ),
                # A valid snapshot re-enables exact T/N comparison.
                _structural_row(
                    day,
                    instrument_id=101,
                    offset_ns=700,
                    sequence=7,
                    action="T",
                    bid=104,
                    ask=105,
                ),
            ]
            source = _write_source(data_root, day, rows, row_group_size=2)
            source_manifest = _write_source_manifest(data_root, [source])

            report = scan_structural_quality(
                data_root, config_path=CONFIG_PATH, source_manifest_path=source_manifest
            )

            record = json.loads(report.manifest_path.read_text())
            self.assertEqual(record["hard_violation_counts"]["clean_trade_none_book_mutation"], 1)

    def test_partial_resume_replays_all_prior_groups_for_book_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            day = date(2024, 1, 2)
            rows = [
                _structural_row(
                    day,
                    instrument_id=101,
                    offset_ns=100,
                    sequence=1,
                    action="R",
                    empty=True,
                ),
                _structural_row(
                    day,
                    instrument_id=101,
                    offset_ns=200,
                    sequence=2,
                    action="A",
                    bid=100,
                    ask=101,
                ),
                _structural_row(
                    day,
                    instrument_id=102,
                    offset_ns=300,
                    sequence=3,
                    action="R",
                    empty=True,
                ),
                _structural_row(
                    day,
                    instrument_id=102,
                    offset_ns=400,
                    sequence=4,
                    action="A",
                    bid=200,
                    ask=201,
                ),
                _structural_row(
                    day,
                    instrument_id=101,
                    offset_ns=500,
                    sequence=5,
                    action="T",
                    bid=101,
                    ask=102,
                ),
            ]
            source = _write_source(
                data_root,
                day,
                rows,
                instrument_id=(101, 102),
                row_group_size=2,
            )
            source_manifest = _write_source_manifest(data_root, [source])

            def interrupt(event: quality.StructuralQcProgress) -> None:
                if event.status == "SCANNED" and event.row_groups_complete == 2:
                    raise RuntimeError("interrupt after instrument 101 disappeared")

            with self.assertRaises(RuntimeError):
                scan_structural_quality(
                    data_root,
                    config_path=CONFIG_PATH,
                    source_manifest_path=source_manifest,
                    progress_callback=interrupt,
                )

            report = scan_structural_quality(
                data_root, config_path=CONFIG_PATH, source_manifest_path=source_manifest
            )
            record = json.loads(report.manifest_path.read_text())
            self.assertEqual(report.resumed_row_group_count, 2)
            self.assertEqual(report.scanned_row_group_count, 1)
            self.assertEqual(record["hard_violation_counts"]["clean_trade_none_book_mutation"], 1)

    def test_callback_resume_recovers_only_an_incomplete_tail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            day = date(2024, 1, 2)
            source = _write_source(data_root, day, _valid_rows(day, 101))
            source_manifest = _write_source_manifest(data_root, [source])

            def interrupt(event: quality.StructuralQcProgress) -> None:
                if event.status == "SCANNED":
                    raise RuntimeError("durable interruption")

            with self.assertRaisesRegex(RuntimeError, "durable interruption"):
                scan_structural_quality(
                    data_root,
                    config_path=CONFIG_PATH,
                    source_manifest_path=source_manifest,
                    progress_callback=interrupt,
                )
            checkpoint = data_root / "derived/manifests/mbp10_structural_qc_v1.checkpoint.jsonl"
            with checkpoint.open("ab") as handle:
                handle.write(b'{"artifact_schema":')

            with mock.patch.object(
                quality, "_scan_row_group", wraps=quality._scan_row_group
            ) as scan_group:
                report = scan_structural_quality(
                    data_root,
                    config_path=CONFIG_PATH,
                    source_manifest_path=source_manifest,
                )

            self.assertEqual(report.resumed_row_group_count, 1)
            self.assertEqual(report.scanned_row_group_count, 1)
            self.assertEqual(scan_group.call_count, 1)
            self.assertTrue(checkpoint.read_bytes().endswith(b"\n"))

    def test_canonical_checkpoint_metric_tampering_breaks_the_sha_chain(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            day = date(2024, 1, 2)
            source = _write_source(data_root, day, _valid_rows(day, 101))
            source_manifest = _write_source_manifest(data_root, [source])

            def interrupt(event: quality.StructuralQcProgress) -> None:
                if event.status == "SCANNED":
                    raise RuntimeError("interrupt")

            with self.assertRaises(RuntimeError):
                scan_structural_quality(
                    data_root,
                    config_path=CONFIG_PATH,
                    source_manifest_path=source_manifest,
                    progress_callback=interrupt,
                )
            checkpoint = data_root / "derived/manifests/mbp10_structural_qc_v1.checkpoint.jsonl"
            record = json.loads(checkpoint.read_text())
            record["diagnostic_counts"]["locked_bbo_rows"] += 1
            checkpoint.write_text(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")

            with self.assertRaisesRegex(StructuralQcError, "record SHA-256 drift"):
                scan_structural_quality(
                    data_root,
                    config_path=CONFIG_PATH,
                    source_manifest_path=source_manifest,
                )

    def test_authenticated_checkpoint_row_count_must_match_parquet_footer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            day = date(2024, 1, 2)
            source = _write_source(data_root, day, _valid_rows(day, 101))
            source_manifest = _write_source_manifest(data_root, [source])

            def interrupt(event: quality.StructuralQcProgress) -> None:
                if event.status == "SCANNED":
                    raise RuntimeError("interrupt")

            with self.assertRaises(RuntimeError):
                scan_structural_quality(
                    data_root,
                    config_path=CONFIG_PATH,
                    source_manifest_path=source_manifest,
                    progress_callback=interrupt,
                )
            checkpoint = data_root / "derived/manifests/mbp10_structural_qc_v1.checkpoint.jsonl"
            record = json.loads(checkpoint.read_text())
            record["row_count"] += 1
            record["checkpoint_record_sha256"] = quality._checkpoint_record_sha256(record)
            checkpoint.write_text(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")

            with self.assertRaisesRegex(
                StructuralQcError, "row-count disagrees with Parquet footer"
            ):
                scan_structural_quality(
                    data_root,
                    config_path=CONFIG_PATH,
                    source_manifest_path=source_manifest,
                )

    def test_existing_manifest_drift_is_not_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            day = date(2024, 1, 2)
            source = _write_source(data_root, day, _valid_rows(day, 101))
            source_manifest = _write_source_manifest(data_root, [source])
            report = scan_structural_quality(
                data_root, config_path=CONFIG_PATH, source_manifest_path=source_manifest
            )
            report.manifest_path.write_bytes(b"tampered\n")

            with self.assertRaisesRegex(StructuralQcError, "immutable QC manifest content drift"):
                scan_structural_quality(
                    data_root,
                    config_path=CONFIG_PATH,
                    source_manifest_path=source_manifest,
                )
            self.assertEqual(report.manifest_path.read_bytes(), b"tampered\n")

    def test_concurrent_manifest_winner_is_never_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            day = date(2024, 1, 2)
            source = _write_source(data_root, day, _valid_rows(day, 101))
            source_manifest = _write_source_manifest(data_root, [source])
            final_path = data_root / "derived/manifests/mbp10_structural_qc_v1.jsonl"

            def competing_publish(
                source_path: Path,
                destination_path: Path,
                *,
                follow_symlinks: bool,
            ) -> None:
                self.assertFalse(follow_symlinks)
                self.assertTrue(Path(source_path).is_file())
                Path(destination_path).write_bytes(b"concurrent-winner\n")
                raise FileExistsError

            with (
                mock.patch.object(quality.os, "link", side_effect=competing_publish),
                self.assertRaisesRegex(StructuralQcError, "immutable QC manifest content drift"),
            ):
                scan_structural_quality(
                    data_root,
                    config_path=CONFIG_PATH,
                    source_manifest_path=source_manifest,
                )
            self.assertEqual(final_path.read_bytes(), b"concurrent-winner\n")

    def test_stale_source_digest_is_rejected_before_row_scanning(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            day = date(2024, 1, 2)
            source = _write_source(data_root, day, _valid_rows(day, 101))
            source_manifest = _write_source_manifest(data_root, [source])
            record = json.loads(source_manifest.read_text())
            record["sha256"] = "0" * 64
            source_manifest.write_text(
                json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
            )

            with self.assertRaisesRegex(StructuralQcError, "source SHA-256 drift"):
                scan_structural_quality(
                    data_root,
                    config_path=CONFIG_PATH,
                    source_manifest_path=source_manifest,
                )

    def test_paths_cannot_escape_and_sources_cannot_be_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            data_root = base / "data"
            day = date(2024, 1, 2)
            source = _write_source(data_root, day, _valid_rows(day, 101))
            source_manifest = _write_source_manifest(data_root, [source])

            with self.assertRaisesRegex(StructuralQcError, "one filename"):
                scan_structural_quality(
                    data_root,
                    config_path=CONFIG_PATH,
                    source_manifest_path=source_manifest,
                    manifest_name="../escaped.jsonl",
                )
            self.assertFalse((data_root / "derived" / "escaped.jsonl").exists())

            outside = base / "outside.parquet"
            source.rename(outside)
            try:
                source.symlink_to(outside)
            except OSError:
                self.skipTest("symbolic links are unavailable")
            with self.assertRaisesRegex(StructuralQcError, "symbolic link"):
                scan_structural_quality(
                    data_root,
                    config_path=CONFIG_PATH,
                    source_manifest_path=source_manifest,
                )

    def test_newline_terminated_checkpoint_corruption_is_not_recovered(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            day = date(2024, 1, 2)
            source = _write_source(data_root, day, _valid_rows(day, 101))
            source_manifest = _write_source_manifest(data_root, [source])

            def interrupt(event: quality.StructuralQcProgress) -> None:
                if event.status == "SCANNED":
                    raise RuntimeError("interrupt")

            with self.assertRaises(RuntimeError):
                scan_structural_quality(
                    data_root,
                    config_path=CONFIG_PATH,
                    source_manifest_path=source_manifest,
                    progress_callback=interrupt,
                )
            checkpoint = data_root / "derived/manifests/mbp10_structural_qc_v1.checkpoint.jsonl"
            with checkpoint.open("ab") as handle:
                handle.write(b"{}\n")

            with self.assertRaisesRegex(StructuralQcError, "invalid checkpoint fields"):
                scan_structural_quality(
                    data_root,
                    config_path=CONFIG_PATH,
                    source_manifest_path=source_manifest,
                )


class QcMutationInspectionTest(unittest.TestCase):
    def _failed_scan(self, data_root: Path) -> tuple[Path, Path]:
        day = date(2024, 1, 2)
        rows = [
            _structural_row(
                day,
                instrument_id=101,
                offset_ns=100,
                sequence=1,
                action="A",
                bid=100,
                ask=101,
            ),
            _structural_row(
                day,
                instrument_id=101,
                offset_ns=200,
                sequence=2,
                action="T",
                bid=99,
                ask=101,
            ),
        ]
        source = _write_source(data_root, day, rows, row_group_size=1)
        source_manifest = _write_source_manifest(data_root, [source])
        scan = scan_structural_quality(
            data_root,
            config_path=CONFIG_PATH,
            source_manifest_path=source_manifest,
        )
        self.assertEqual(scan.failed_file_count, 1)
        return source, scan.manifest_path

    def test_replay_is_canonical_three_way_verified_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            source, qc_manifest = self._failed_scan(data_root)

            report = qc_mutations.inspect_qc_mutations(
                data_root,
                qc_manifest_path=qc_manifest,
            )

            self.assertTrue(report.created_output)
            self.assertEqual(report.failed_source_count, 1)
            self.assertEqual(report.replayed_row_group_count, 2)
            self.assertEqual(report.replayed_row_count, 2)
            self.assertEqual(report.mutation_count, 1)
            self.assertEqual(
                report.output_path.parent,
                data_root.resolve() / "derived" / "manifests",
            )
            self.assertEqual(
                report.output_sha256,
                hashlib.sha256(report.output_path.read_bytes()).hexdigest(),
            )
            self.assertEqual(
                report.qc_manifest_sha256,
                hashlib.sha256(qc_manifest.read_bytes()).hexdigest(),
            )
            self.assertEqual(
                report.reproduction[0].manifest_count,
                report.reproduction[0].scanner_count,
            )
            self.assertEqual(
                report.reproduction[0].scanner_count,
                report.reproduction[0].detail_count,
            )
            raw = report.output_path.read_bytes()
            record = json.loads(raw)
            self.assertEqual(raw, quality._canonical_line(record))
            self.assertEqual(record["mutation_id"], "TNM-001")
            self.assertEqual(record["artifact_schema"], qc_mutations.ARTIFACT_SCHEMA)
            self.assertEqual(
                record["source_sha256"], hashlib.sha256(source.read_bytes()).hexdigest()
            )
            self.assertEqual(record["qc_manifest_sha256"], report.qc_manifest_sha256)
            self.assertEqual(record["previous_row"]["file_row_ordinal_zero_based"], 0)
            self.assertEqual(record["current_row"]["file_row_ordinal_zero_based"], 1)
            self.assertEqual(record["current_row"]["row_group_index_zero_based"], 1)

            first_identity = report.output_path.stat()
            repeated = qc_mutations.inspect_qc_mutations(
                data_root,
                qc_manifest_path=qc_manifest,
            )
            second_identity = report.output_path.stat()
            self.assertFalse(repeated.created_output)
            self.assertEqual(repeated.output_sha256, report.output_sha256)
            self.assertEqual(first_identity.st_ino, second_identity.st_ino)
            self.assertEqual(first_identity.st_mtime_ns, second_identity.st_mtime_ns)

            drift_path = report.output_path.with_name("drift.jsonl")
            drift_path.write_bytes(b"existing-different-content\n")
            with self.assertRaisesRegex(
                qc_mutations.QcMutationInspectionError,
                "immutable mutation report content drift",
            ):
                qc_mutations.inspect_qc_mutations(
                    data_root,
                    qc_manifest_path=qc_manifest,
                    output_name=drift_path.name,
                )
            self.assertEqual(drift_path.read_bytes(), b"existing-different-content\n")

    def test_three_way_count_drift_is_rejected_before_publication(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            _, qc_manifest = self._failed_scan(data_root)
            record = json.loads(qc_manifest.read_text(encoding="utf-8"))
            record["hard_violation_counts"]["clean_trade_none_book_mutation"] = 2
            record["hard_violation_count"] = 2
            altered = qc_manifest.with_name("altered_qc.jsonl")
            altered.write_bytes(quality._canonical_line(record))

            with self.assertRaisesRegex(
                qc_mutations.QcMutationInspectionError,
                "three-way mutation count mismatch",
            ):
                qc_mutations.inspect_qc_mutations(
                    data_root,
                    qc_manifest_path=altered,
                    output_name="altered_details.jsonl",
                )
            self.assertFalse(altered.with_name("altered_details.jsonl").exists())

    def test_source_identity_drift_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            source, qc_manifest = self._failed_scan(data_root)
            source.write_bytes(source.read_bytes() + b"drift")

            with self.assertRaisesRegex(
                qc_mutations.QcMutationInspectionError,
                "source byte-size drift",
            ):
                qc_mutations.inspect_qc_mutations(
                    data_root,
                    qc_manifest_path=qc_manifest,
                )


if __name__ == "__main__":
    unittest.main()
