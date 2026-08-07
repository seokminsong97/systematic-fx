from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from concurrent.futures import Future
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Self
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

from systematic_fx.backtest.event_cache import (
    DailyCacheReport,
    DailyCacheSpec,
    EventCacheError,
    build_daily_cache_batch,
    build_daily_executable_cache,
    read_daily_executable_cache,
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


def _at(day: date, second: float) -> int:
    base = int(datetime.combine(day, time(12), tzinfo=UTC).timestamp()) * SECOND
    return base + round(second * SECOND)


def _metadata(day: date, mappings: list[tuple[str, int]]) -> dict[bytes, bytes]:
    document = {
        "dataset": DATASET_NAME,
        "schema": SCHEMA_NAME,
        "version": 3,
        "stype_out": "instrument_id",
        "start": int(datetime.combine(day, time(), tzinfo=UTC).timestamp()) * SECOND,
        "end": int(datetime.combine(day + timedelta(days=1), time(), tzinfo=UTC).timestamp())
        * SECOND,
        "mappings": [
            {
                "raw_symbol": symbol,
                "intervals": [
                    {
                        "start": "2022-01-01",
                        "end": "2022-04-01",
                        "symbol": str(instrument_id),
                    }
                ],
            }
            for symbol, instrument_id in mappings
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
    instrument_id: int,
    *,
    action: str = "A",
    sequence: int = 1,
    bid_ticks: int = 20_000,
    ask_ticks: int = 20_001,
) -> dict[str, object]:
    return {
        "ts_recv": timestamp_ns,
        "instrument_id": instrument_id,
        "action": action,
        "sequence": sequence,
        "bid_px_00": bid_ticks * TICK_RAW,
        "ask_px_00": ask_ticks * TICK_RAW,
        "bid_sz_00": 2,
        "ask_sz_00": 2,
    }


def _write_source(
    path: Path,
    *,
    day: date,
    events: list[dict[str, object]],
    mappings: list[tuple[str, int]] | None = None,
) -> None:
    schema = expected_mbp10_schema(metadata=_metadata(day, mappings or [("6EH2", 11)]))
    columns: dict[str, list[object]] = {field.name: [] for field in schema}
    for ordinal, event in enumerate(events):
        for field in schema:
            name = field.name
            if name in {"ts_recv", "ts_event"}:
                value: object = event["ts_recv"]
            elif name == "rtype":
                value = 10
            elif name == "publisher_id":
                value = 1
            elif name == "instrument_id":
                value = event["instrument_id"]
            elif name == "action":
                value = event.get("action", "A")
            elif name == "side":
                value = "N" if event.get("action") == "R" else "B"
            elif name == "depth":
                value = 0
            elif name == "price":
                value = UNDEFINED_PRICE
            elif name == "size":
                value = 1
            elif name in {"flags", "ts_in_delta"}:
                value = 0
            elif name == "sequence":
                value = event.get("sequence", ordinal + 1)
            elif name.startswith(("bid_px_", "ask_px_")):
                value = event.get(name, UNDEFINED_PRICE)
            elif name.startswith(("bid_sz_", "ask_sz_", "bid_ct_", "ask_ct_")):
                value = event.get(name, 0)
            else:  # pragma: no cover - the source contract is exhaustive
                raise AssertionError(name)
            columns[name].append(value)
    pq.write_table(pa.Table.from_pydict(columns, schema=schema), path, row_group_size=2)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class DailyExecutableCacheTests(unittest.TestCase):
    def test_build_reuse_and_read_preserve_lineage_and_reset_state(self) -> None:
        day = date(2022, 1, 4)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            (root / "derived").mkdir(parents=True)
            source = root / "source.parquet"
            _write_source(
                source,
                day=day,
                mappings=[("6EH2", 11), ("6EM2", 22)],
                events=[
                    _event(_at(day, 0), 11, sequence=1),
                    _event(_at(day, 0.1), 22, sequence=2),
                    _event(_at(day, 1), 11, action="R", sequence=3),
                    _event(_at(day, 1.2), 11, sequence=4),
                    _event(_at(day, 2.2), 11, sequence=5),
                ],
            )
            spec = DailyCacheSpec(day, source, _sha(source), "6EH2", 1_000)

            created = build_daily_executable_cache(spec, data_root=root)
            with mock.patch(
                "systematic_fx.backtest.event_cache._prepare_verified_raw_source",
                side_effect=AssertionError("an indexed cache must not decode raw MBP-10 again"),
            ):
                reused = build_daily_executable_cache(spec, data_root=root)
            quotes = tuple(read_daily_executable_cache(created))

            self.assertEqual(created.disposition, "CREATED")
            self.assertEqual(reused.disposition, "REUSED")
            self.assertEqual(created.path, reused.path)
            indexes = tuple(created.path.parent.joinpath("request_index").glob("*.json"))
            self.assertEqual(len(indexes), 1)
            self.assertEqual(indexes[0].stat().st_mode & 0o222, 0)
            self.assertTrue(created.path.is_relative_to((root / "derived").resolve()))
            index_document = json.loads(indexes[0].read_bytes())
            self.assertEqual(index_document["request"]["source_relative_uri"], "source.parquet")
            self.assertEqual(index_document["report"]["source_relative_uri"], "source.parquet")
            self.assertNotIn("source_path", index_document["request"])
            self.assertNotIn(str(root.resolve()), indexes[0].read_text())
            cache_metadata = json.loads(
                pq.ParquetFile(created.path).schema_arrow.metadata[b"systematic_fx.cache"]
            )
            self.assertEqual(cache_metadata["source_relative_uri"], "source.parquet")
            self.assertNotIn("source_path", cache_metadata)
            self.assertEqual(created.cached_quote_count, 4)
            self.assertEqual(created.valid_quote_count, 2)
            self.assertEqual(created.last_valid_event_index, 1004)
            self.assertEqual(created.last_valid_ts_recv_ns, _at(day, 2.2))
            self.assertEqual([item.source_row_index for item in quotes], [0, 2, 3, 4])
            self.assertEqual([item.quote.event_index for item in quotes], [1000, 1002, 1003, 1004])
            self.assertEqual(
                [item.invalid_reason for item in quotes],
                [None, "RESET", "RESET_NOT_REARMED", None],
            )
            self.assertEqual([item.contract_key for item in quotes], ["6EH2"] * 4)

    def test_batch_is_ordered_and_rejects_duplicate_semantic_keys(self) -> None:
        days = (date(2022, 1, 3), date(2022, 1, 4))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            (root / "derived").mkdir(parents=True)
            specs = []
            offset = 0
            for index, day in enumerate(days):
                source = root / f"source-{index}.parquet"
                _write_source(source, day=day, events=[_event(_at(day, 0), 11)])
                specs.append(DailyCacheSpec(day, source, _sha(source), "6EH2", offset))
                offset += pq.ParquetFile(source).metadata.num_rows

            progress: list[tuple[date, int, int]] = []
            reports = build_daily_cache_batch(
                tuple(specs),
                data_root=root,
                max_workers=1,
                progress_callback=lambda report, completed, total: progress.append(
                    (report.source_date, completed, total)
                ),
            )

            self.assertEqual([item.source_date for item in reports], list(days))
            self.assertEqual(progress, [(days[0], 1, 2), (days[1], 2, 2)])
            with self.assertRaisesRegex(EventCacheError, "duplicate"):
                build_daily_cache_batch((specs[0], specs[0]), data_root=root, max_workers=1)

    def test_reader_rejects_content_drift(self) -> None:
        day = date(2022, 1, 4)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            (root / "derived").mkdir(parents=True)
            source = root / "source.parquet"
            _write_source(source, day=day, events=[_event(_at(day, 0), 11)])
            report = build_daily_executable_cache(
                DailyCacheSpec(day, source, _sha(source), "6EH2", 0),
                data_root=root,
            )
            report.path.chmod(0o644)
            report.path.write_bytes(report.path.read_bytes() + b"drift")

            with self.assertRaisesRegex(EventCacheError, "read-only|drift"):
                tuple(read_daily_executable_cache(report))

    def test_reader_rejects_same_timestamp_sequence_regression(self) -> None:
        day = date(2022, 1, 4)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            (root / "derived").mkdir(parents=True)
            source = root / "source.parquet"
            timestamp = _at(day, 0)
            _write_source(
                source,
                day=day,
                events=[
                    _event(timestamp, 11, sequence=2),
                    _event(timestamp, 11, sequence=1),
                ],
            )
            report = build_daily_executable_cache(
                DailyCacheSpec(day, source, _sha(source), "6EH2", 0),
                data_root=root,
            )

            with self.assertRaisesRegex(EventCacheError, "sequence regressed"):
                tuple(read_daily_executable_cache(report))

    def test_request_index_report_must_be_bound_to_request(self) -> None:
        day = date(2022, 1, 4)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            (root / "derived").mkdir(parents=True)
            source = root / "source.parquet"
            _write_source(source, day=day, events=[_event(_at(day, 0), 11)])
            spec = DailyCacheSpec(day, source, _sha(source), "6EH2", 0)
            report = build_daily_executable_cache(spec, data_root=root)
            (index_path,) = tuple(report.path.parent.joinpath("request_index").glob("*.json"))
            document = json.loads(index_path.read_bytes())
            document["report"]["raw_symbol"] = "6EM2"
            index_path.chmod(0o644)
            index_path.write_bytes(
                json.dumps(
                    document,
                    ensure_ascii=False,
                    allow_nan=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
                + b"\n"
            )
            index_path.chmod(0o444)

            with self.assertRaisesRegex(EventCacheError, "not bound to its request"):
                build_daily_executable_cache(spec, data_root=root)

    def test_reader_hashes_and_reads_same_inode_then_checks_path_identity(self) -> None:
        day = date(2022, 1, 4)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            (root / "derived").mkdir(parents=True)
            source = root / "source.parquet"
            _write_source(source, day=day, events=[_event(_at(day, 0), 11)])
            report = build_daily_executable_cache(
                DailyCacheSpec(day, source, _sha(source), "6EH2", 0),
                data_root=root,
            )
            backup = report.path.with_name(f"backup-{report.path.name}")
            original = pq.ParquetFile
            swapped = False

            def swap_after_verified_open(candidate: object, *args: object, **kwargs: object):
                nonlocal swapped
                if not swapped:
                    swapped = True
                    report.path.replace(backup)
                    shutil.copy2(backup, report.path)
                    report.path.chmod(0o444)
                return original(candidate, *args, **kwargs)

            with (
                mock.patch(
                    "systematic_fx.backtest.event_cache.pq.ParquetFile",
                    side_effect=swap_after_verified_open,
                ),
                self.assertRaisesRegex(EventCacheError, "path identity changed"),
            ):
                tuple(read_daily_executable_cache(report))

    def test_raw_builder_hashes_and_reads_same_inode_then_checks_path_identity(self) -> None:
        day = date(2022, 1, 4)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            (root / "derived").mkdir(parents=True)
            source = root / "source.parquet"
            _write_source(source, day=day, events=[_event(_at(day, 0), 11)])
            backup = root / "source-original.parquet"
            original = pq.ParquetFile
            swapped = False

            def swap_for_same_fd_read(candidate: object, *args: object, **kwargs: object):
                nonlocal swapped
                if not swapped and not isinstance(candidate, (str, Path)):
                    swapped = True
                    source.replace(backup)
                    shutil.copy2(backup, source)
                return original(candidate, *args, **kwargs)

            with (
                mock.patch(
                    "systematic_fx.backtest.event_cache.pq.ParquetFile",
                    side_effect=swap_for_same_fd_read,
                ),
                self.assertRaisesRegex(EventCacheError, "path identity changed"),
            ):
                build_daily_executable_cache(
                    DailyCacheSpec(day, source, _sha(source), "6EH2", 0),
                    data_root=root,
                )

    def test_parallel_batch_never_has_more_futures_in_flight_than_workers(self) -> None:
        class InstrumentedPool:
            latest: InstrumentedPool | None = None

            def __init__(self, *, max_workers: int) -> None:
                self.max_workers = max_workers
                self.outstanding = 0
                self.maximum_outstanding = 0
                self.submission_count = 0
                InstrumentedPool.latest = self

            def __enter__(self) -> Self:
                return self

            def __exit__(self, *_: object) -> None:
                return None

            def submit(self, _callable: object, arguments: tuple[DailyCacheSpec, str]):
                spec, data_root = arguments
                self.outstanding += 1
                self.submission_count += 1
                self.maximum_outstanding = max(
                    self.maximum_outstanding,
                    self.outstanding,
                )
                future: Future[DailyCacheReport] = Future()
                future.pool = self  # type: ignore[attr-defined]
                future.set_result(
                    DailyCacheReport(
                        path=Path(data_root) / f"{spec.source_date}.parquet",
                        sha256="0" * 64,
                        byte_size=1,
                        disposition="CREATED",
                        source_date=spec.source_date,
                        source_path=str(spec.source_parquet_path),
                        source_sha256=spec.source_sha256,
                        raw_symbol=spec.raw_symbol,
                        instrument_id=1,
                        event_index_offset=spec.event_index_offset,
                        source_row_count=1,
                        cached_quote_count=1,
                        valid_quote_count=1,
                        first_event_index=spec.event_index_offset,
                        last_event_index=spec.event_index_offset,
                        first_ts_recv_ns=1,
                        last_ts_recv_ns=1,
                        last_valid_event_index=spec.event_index_offset,
                        last_valid_ts_recv_ns=1,
                    )
                )
                return future

        def instrumented_wait(futures: tuple[Future[DailyCacheReport], ...], **_: object):
            completed = min(futures, key=id)
            completed.pool.outstanding -= 1  # type: ignore[attr-defined]
            return {completed}, set(futures) - {completed}

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            (root / "derived").mkdir(parents=True)
            specs = tuple(
                DailyCacheSpec(
                    date(2022, 1, day),
                    root / f"source-{day}.parquet",
                    "0" * 64,
                    "6EH2",
                    day,
                )
                for day in range(1, 7)
            )
            with (
                mock.patch(
                    "systematic_fx.backtest.event_cache.ProcessPoolExecutor",
                    InstrumentedPool,
                ),
                mock.patch(
                    "systematic_fx.backtest.event_cache.wait",
                    side_effect=instrumented_wait,
                ),
            ):
                reports = build_daily_cache_batch(specs, data_root=root, max_workers=2)

        pool = InstrumentedPool.latest
        self.assertIsNotNone(pool)
        assert pool is not None
        self.assertEqual(pool.submission_count, len(specs))
        self.assertLessEqual(pool.maximum_outstanding, 2)
        self.assertEqual(
            tuple(report.source_date for report in reports), tuple(s.source_date for s in specs)
        )

    def test_cache_publication_rejects_symlinked_directory_component(self) -> None:
        day = date(2022, 1, 4)
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            root = base / "data"
            (root / "derived").mkdir(parents=True)
            outside = base / "outside"
            outside.mkdir()
            (root / "derived" / "backtest_event_cache").symlink_to(
                outside,
                target_is_directory=True,
            )
            source = root / "source.parquet"
            _write_source(source, day=day, events=[_event(_at(day, 0), 11)])

            with self.assertRaisesRegex(EventCacheError, "symbolic link"):
                build_daily_executable_cache(
                    DailyCacheSpec(day, source, _sha(source), "6EH2", 0),
                    data_root=root,
                )


if __name__ == "__main__":
    unittest.main()
