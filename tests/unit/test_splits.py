from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from collections.abc import Callable
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

from systematic_fx.validation.splits import (
    CALENDAR_ARTIFACT_FILENAME,
    CALENDAR_SCHEMA,
    MINIMUM_ELIGIBLE_SOURCE_DATES,
    PHASE1A_EXCLUDED_SOURCE_DATES,
    SPLIT_ARTIFACT_FILENAME,
    SPLIT_SCHEMA,
    SplitValidationError,
    build_phase1a_screening_calendar,
    build_phase1a_screening_split,
    publish_phase1a_screening_artifacts,
)

Record = dict[str, object]
RecordsMutation = Callable[[list[Record]], None]


def _canonical_lines(records: list[Record]) -> bytes:
    return b"".join(
        (
            json.dumps(
                record,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        for record in records
    )


def _fixture_dates(pass_count: int) -> tuple[date, ...]:
    excluded = frozenset(PHASE1A_EXCLUDED_SOURCE_DATES)
    passing: list[date] = []
    cursor = date(2022, 1, 1)
    while len(passing) < pass_count:
        if cursor not in excluded:
            passing.append(cursor)
        cursor += timedelta(days=1)
    return tuple(sorted((*passing, *PHASE1A_EXCLUDED_SOURCE_DATES)))


def _relative_uri(day: date) -> str:
    return f"{day:%Y/%m/%d}/glbx-mdp3-{day:%Y%m%d}.mbp-10.parquet"


def _write_manifests(
    directory: Path,
    *,
    pass_count: int = MINIMUM_ELIGIBLE_SOURCE_DATES,
    mutate_source: RecordsMutation | None = None,
    mutate_qc: RecordsMutation | None = None,
    noncanonical_source: bool = False,
) -> tuple[Path, Path]:
    source_records: list[Record] = []
    for day in _fixture_dates(pass_count):
        source_records.append(
            {
                "byte_size": 100,
                "relative_uri": _relative_uri(day),
                "sha256": hashlib.sha256(day.isoformat().encode("ascii")).hexdigest(),
                "source_date": day.isoformat(),
            }
        )
    if mutate_source is not None:
        mutate_source(source_records)

    source_path = directory / "source.jsonl"
    source_bytes = _canonical_lines(source_records)
    if noncanonical_source:
        source_bytes = (
            json.dumps(source_records[0]).encode("utf-8")
            + b"\n"
            + _canonical_lines(source_records[1:])
        )
    source_path.write_bytes(source_bytes)
    source_manifest_sha256 = hashlib.sha256(source_bytes).hexdigest()

    excluded = frozenset(PHASE1A_EXCLUDED_SOURCE_DATES)
    qc_records: list[Record] = []
    for source in source_records:
        source_day = date.fromisoformat(str(source["source_date"]))
        failed = source_day in excluded
        hard_count = 1 if failed else 0
        qc_records.append(
            {
                "artifact_schema": "systematic_fx.mbp10_structural_qc_file.v1",
                "checker_version": "mbp10_structural_qc_v1",
                "config_sha256": "a" * 64,
                "coverage_complete": True,
                "diagnostic_counts": {"crossed_bbo_rows": 0},
                "expected_row_count": 10,
                "expected_row_group_count": 1,
                "first_ts_recv_ns": 1,
                "hard_violation_count": hard_count,
                "hard_violation_counts": {"clean_trade_none_book_mutation": hard_count},
                "last_ts_recv_ns": 2,
                "relative_uri": source["relative_uri"],
                "research_eligible": False,
                "result": "FAIL" if failed else "PASS",
                "scanned_row_count": 10,
                "scanned_row_group_count": 1,
                "schema_fingerprint": "b" * 64,
                "source_byte_size": source["byte_size"],
                "source_date": source["source_date"],
                "source_manifest_sha256": source_manifest_sha256,
                "source_sha256": source["sha256"],
            }
        )
    if mutate_qc is not None:
        mutate_qc(qc_records)

    qc_path = directory / "qc.jsonl"
    qc_path.write_bytes(_canonical_lines(qc_records))
    return source_path, qc_path


def _built_artifacts(directory: Path):
    inputs = directory / "inputs"
    inputs.mkdir()
    source_path, qc_path = _write_manifests(inputs)
    calendar = build_phase1a_screening_calendar(source_path, qc_path)
    split = build_phase1a_screening_split(calendar)
    return calendar, split


def _publication_directory(directory: Path) -> Path:
    manifests = directory / "data" / "derived" / "manifests"
    manifests.mkdir(parents=True)
    return manifests


class Phase1AScreeningSplitTests(unittest.TestCase):
    def test_minimum_calendar_builds_canonical_performance_free_split(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source_path, qc_path = _write_manifests(Path(temporary))
            calendar = build_phase1a_screening_calendar(source_path, qc_path)
            split = build_phase1a_screening_split(calendar)

        self.assertEqual(len(calendar.source_dates), 740)
        self.assertEqual(calendar.qc_pass_record_count, 740)
        self.assertEqual(calendar.qc_fail_record_count, 6)
        self.assertFalse(set(calendar.source_dates) & set(PHASE1A_EXCLUDED_SOURCE_DATES))
        self.assertEqual(len(split.discovery), 220)
        self.assertEqual([len(fold) for fold in split.walk_forward_folds], [72] * 5)
        self.assertEqual(len(split.embargo), 20)
        self.assertEqual(len(split.sealed_holdout), 120)
        self.assertEqual(len(split.outcome_tail), 20)

        combined = (
            split.discovery
            + tuple(day for fold in split.walk_forward_folds for day in fold)
            + split.embargo
            + split.sealed_holdout
            + split.outcome_tail
        )
        self.assertEqual(combined, calendar.source_dates)

        calendar_payload = calendar.payload
        split_payload = split.payload
        self.assertEqual(calendar_payload["artifact_schema"], CALENDAR_SCHEMA)
        self.assertEqual(split_payload["artifact_schema"], SPLIT_SCHEMA)
        self.assertFalse(calendar_payload["authority"]["pass_backtest_allowed"])
        self.assertFalse(split_payload["authority"]["pass_backtest_allowed"])
        self.assertEqual(
            calendar_payload["qualification_semantics"]["calendar_kind"],
            "SOURCE_DATE_PROXY",
        )
        self.assertFalse(calendar_payload["qualification_semantics"]["definition_data_available"])
        self.assertFalse(split_payload["construction"]["performance_values_used"])
        self.assertEqual(calendar.sha256, hashlib.sha256(calendar.canonical_json()).hexdigest())
        self.assertEqual(split.sha256, hashlib.sha256(split.canonical_json()).hexdigest())
        self.assertEqual(
            calendar.canonical_json(),
            json.dumps(
                calendar_payload,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8"),
        )

    def test_extra_history_and_remainder_go_to_oldest_folds(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source_path, qc_path = _write_manifests(Path(temporary), pass_count=741)
            calendar = build_phase1a_screening_calendar(source_path, qc_path)
            first = build_phase1a_screening_split(calendar)
            second = build_phase1a_screening_split(calendar)

        self.assertEqual(len(first.discovery), 220 + (2 * (581 - 580)) // 5)
        self.assertEqual([len(fold) for fold in first.walk_forward_folds], [73, 72, 72, 72, 72])
        self.assertEqual(first.canonical_json(), second.canonical_json())
        self.assertEqual(first.sha256, second.sha256)

    def test_source_dates_must_be_unique_and_strictly_ordered(self) -> None:
        mutations: tuple[tuple[str, RecordsMutation, str], ...] = (
            (
                "duplicate",
                lambda rows: rows.insert(1, dict(rows[0])),
                "unique and strictly ordered|duplicate source date",
            ),
            (
                "reverse",
                lambda rows: rows.__setitem__(slice(0, 2), reversed(rows[:2])),
                "unique and strictly ordered|reverse-ordered",
            ),
        )
        for name, mutation, message in mutations:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                source_path, qc_path = _write_manifests(Path(temporary), mutate_source=mutation)
                with self.assertRaisesRegex(SplitValidationError, message):
                    build_phase1a_screening_calendar(source_path, qc_path)

    def test_manifest_identity_and_source_manifest_hash_mismatch_are_rejected(self) -> None:
        def identity_drift(rows: list[Record]) -> None:
            rows[0]["source_sha256"] = "f" * 64

        def manifest_hash_drift(rows: list[Record]) -> None:
            rows[0]["source_manifest_sha256"] = "f" * 64

        for name, mutation in (
            ("identity", identity_drift),
            ("source manifest hash", manifest_hash_drift),
        ):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                source_path, qc_path = _write_manifests(Path(temporary), mutate_qc=mutation)
                with self.assertRaisesRegex(SplitValidationError, "manifest mismatch"):
                    build_phase1a_screening_calendar(source_path, qc_path)

    def test_non_pass_on_non_excluded_date_is_rejected(self) -> None:
        def mutate(rows: list[Record]) -> None:
            rows[0]["result"] = "FAIL"
            rows[0]["hard_violation_count"] = 1
            rows[0]["hard_violation_counts"] = {"clean_trade_none_book_mutation": 1}

        with tempfile.TemporaryDirectory() as temporary:
            source_path, qc_path = _write_manifests(Path(temporary), mutate_qc=mutate)
            with self.assertRaisesRegex(SplitValidationError, "QC non-PASS"):
                build_phase1a_screening_calendar(source_path, qc_path)

    def test_exclusion_policy_and_qc_fail_set_cannot_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source_path, qc_path = _write_manifests(Path(temporary))
            with self.assertRaisesRegex(SplitValidationError, "exclusion drift"):
                build_phase1a_screening_calendar(
                    source_path,
                    qc_path,
                    excluded_source_dates=PHASE1A_EXCLUDED_SOURCE_DATES[:-1],
                )

        excluded_text = PHASE1A_EXCLUDED_SOURCE_DATES[0].isoformat()

        def erase_raw_fail(rows: list[Record]) -> None:
            record = next(row for row in rows if row["source_date"] == excluded_text)
            record["result"] = "PASS"
            record["hard_violation_count"] = 0
            record["hard_violation_counts"] = {"clean_trade_none_book_mutation": 0}

        with tempfile.TemporaryDirectory() as temporary:
            source_path, qc_path = _write_manifests(Path(temporary), mutate_qc=erase_raw_fail)
            with self.assertRaisesRegex(SplitValidationError, "exclusion/QC drift"):
                build_phase1a_screening_calendar(source_path, qc_path)

    def test_noncanonical_jsonl_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source_path, qc_path = _write_manifests(Path(temporary), noncanonical_source=True)
            with self.assertRaisesRegex(SplitValidationError, "not canonical JSONL"):
                build_phase1a_screening_calendar(source_path, qc_path)

    def test_split_rejects_fewer_than_740_eligible_dates(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source_path, qc_path = _write_manifests(Path(temporary), pass_count=739)
            calendar = build_phase1a_screening_calendar(source_path, qc_path)
            with self.assertRaisesRegex(SplitValidationError, "at least 740"):
                build_phase1a_screening_split(calendar)

    def test_publisher_creates_exact_bytes_then_reuses_without_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            calendar, split = _built_artifacts(root)
            manifests = _publication_directory(root)

            created = publish_phase1a_screening_artifacts(
                calendar,
                split,
                manifest_directory=manifests,
            )
            calendar_inode = created.calendar_path.stat().st_ino
            split_inode = created.split_path.stat().st_ino
            reused = publish_phase1a_screening_artifacts(
                calendar,
                split,
                manifest_directory=manifests,
            )

            self.assertEqual(created.calendar_disposition, "CREATED")
            self.assertEqual(created.split_disposition, "CREATED")
            self.assertEqual(reused.calendar_disposition, "REUSED")
            self.assertEqual(reused.split_disposition, "REUSED")
            self.assertEqual(created.calendar_path.read_bytes(), calendar.canonical_json())
            self.assertEqual(created.split_path.read_bytes(), split.canonical_json())
            self.assertEqual(created.calendar_path.stat().st_ino, calendar_inode)
            self.assertEqual(created.split_path.stat().st_ino, split_inode)
            self.assertEqual(created.calendar_sha256, calendar.sha256)
            self.assertEqual(created.split_sha256, split.sha256)
            self.assertEqual(
                sorted(path.name for path in manifests.iterdir()),
                sorted((CALENDAR_ARTIFACT_FILENAME, SPLIT_ARTIFACT_FILENAME)),
            )

    def test_publisher_preflights_both_files_and_rejects_content_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            calendar, split = _built_artifacts(root)
            manifests = _publication_directory(root)
            split_path = manifests / SPLIT_ARTIFACT_FILENAME
            split_path.write_bytes(b"drift")

            with self.assertRaisesRegex(SplitValidationError, "content drift"):
                publish_phase1a_screening_artifacts(
                    calendar,
                    split,
                    manifest_directory=manifests,
                )

            self.assertEqual(split_path.read_bytes(), b"drift")
            self.assertFalse((manifests / CALENDAR_ARTIFACT_FILENAME).exists())
            self.assertEqual([path.name for path in manifests.iterdir()], [SPLIT_ARTIFACT_FILENAME])

    def test_publisher_rejects_unbound_split_and_unsafe_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            calendar, split = _built_artifacts(root)
            manifests = _publication_directory(root)
            unbound = replace(split, calendar_sha256="0" * 64)

            with self.assertRaisesRegex(SplitValidationError, "not bound"):
                publish_phase1a_screening_artifacts(
                    calendar,
                    unbound,
                    manifest_directory=manifests,
                )

            unsafe = root / "manifests"
            unsafe.mkdir()
            with self.assertRaisesRegex(SplitValidationError, "data/derived/manifests"):
                publish_phase1a_screening_artifacts(
                    calendar,
                    split,
                    manifest_directory=unsafe,
                )

            non_directory = root / "other" / "data" / "derived" / "manifests"
            non_directory.parent.mkdir(parents=True)
            non_directory.write_text("not a directory")
            with self.assertRaisesRegex(SplitValidationError, "must be a directory"):
                publish_phase1a_screening_artifacts(
                    calendar,
                    split,
                    manifest_directory=non_directory,
                )

    def test_publisher_rejects_symlink_directory_and_target(self) -> None:
        if not hasattr(os, "symlink"):
            self.skipTest("symbolic links are unavailable")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            calendar, split = _built_artifacts(root)
            derived = root / "data" / "derived"
            derived.mkdir(parents=True)
            real_manifests = root / "real_manifests"
            real_manifests.mkdir()
            (derived / "manifests").symlink_to(real_manifests, target_is_directory=True)

            with self.assertRaisesRegex(SplitValidationError, "symbolic link"):
                publish_phase1a_screening_artifacts(
                    calendar,
                    split,
                    manifest_directory=derived / "manifests",
                )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            calendar, split = _built_artifacts(root)
            manifests = _publication_directory(root)
            outside = root / "outside.json"
            outside.write_bytes(calendar.canonical_json())
            (manifests / CALENDAR_ARTIFACT_FILENAME).symlink_to(outside)

            with self.assertRaisesRegex(SplitValidationError, "unsafe or not a regular file"):
                publish_phase1a_screening_artifacts(
                    calendar,
                    split,
                    manifest_directory=manifests,
                )
            self.assertEqual(outside.read_bytes(), calendar.canonical_json())


if __name__ == "__main__":
    unittest.main()
