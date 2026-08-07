from __future__ import annotations

import hashlib
import json
import tempfile
import tomllib
import unittest
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from systematic_fx.data.contract_selection import select_next_eligible_contract
from systematic_fx.data.contracts import (
    UNDEFINED_PRICE,
    compute_schema_fingerprint,
    expected_mbp10_schema,
)
from systematic_fx.data.quality import load_structural_qc_config
from systematic_fx.features.screening import (
    DEFAULT_CONFIG_PATH,
    DEFAULT_QC_CONFIG_PATH,
    DEPTH_LEVELS,
    FEATURE_VERSION,
    FIVE_MINUTE_SCHEMA,
    FORMULA_SHA256,
    NO_POSITIVE_PREVIOUS_SOURCE_TRADE_VOLUME,
    NO_PROVEN_COMPLETE_OBSERVED_1S_BUCKET,
    ONE_SECOND_NS,
    ONE_SECOND_SCHEMA,
    ScreeningFeatureBuildError,
    build_phase1a_screening_features,
    load_phase1a_screening_config,
    plan_phase1a_screening_no_entry_reason,
)
from systematic_fx.validation.splits import (
    PHASE1A_EXCLUDED_SOURCE_DATES,
    Phase1AScreeningCalendar,
)

PREVIOUS_DATE = date(2022, 1, 2)
SOURCE_DATE = date(2022, 1, 3)
TICK_RAW = 50_000
CODE_SNAPSHOT_SHA256 = "e" * 64

type MappingSpec = tuple[str, int]


def _midnight_ns(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp()) * ONE_SECOND_NS


def _metadata(day: date, mappings: list[MappingSpec]) -> dict[bytes, bytes]:
    document = {
        "dataset": "GLBX.MDP3",
        "schema": "mbp-10",
        "version": 3,
        "stype_out": "instrument_id",
        "start": _midnight_ns(day),
        "end": _midnight_ns(day + timedelta(days=1)),
        "mappings": [
            {
                "raw_symbol": symbol,
                "intervals": [
                    {
                        "start": "2022-01-01",
                        "end": "2023-01-01",
                        "symbol": str(instrument_id),
                    }
                ],
            }
            for symbol, instrument_id in mappings
        ],
    }
    return {
        b"dbn.dataset": b"GLBX.MDP3",
        b"dbn.schema": b"mbp-10",
        b"dbn.version": b"3",
        b"dbn.metadata": json.dumps(document, sort_keys=True).encode(),
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
    bid: int = 1_100_000_000,
    ask: int = 1_100_100_000,
    bid_size: int = 100,
    ask_size: int = 50,
    empty_book: bool = False,
) -> dict[str, object]:
    return {
        "offset_ns": offset_ns,
        "instrument_id": instrument_id,
        "action": action,
        "side": side,
        "size": size,
        "flags": flags,
        "bid": bid,
        "ask": ask,
        "bid_size": bid_size,
        "ask_size": ask_size,
        "empty_book": empty_book,
    }


def _write_source(
    path: Path,
    *,
    day: date,
    events: list[dict[str, object]],
    mappings: list[MappingSpec] | None = None,
    row_group_size: int = 64,
) -> None:
    mappings = mappings or [("6EH2", 101), ("6EM2", 202)]
    schema = expected_mbp10_schema(metadata=_metadata(day, mappings))
    columns: dict[str, list[object]] = {field.name: [] for field in schema}
    day_start = _midnight_ns(day)
    for sequence, event in enumerate(events, start=1):
        timestamp = day_start + int(event["offset_ns"])
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
            if bool(event["empty_book"]):
                columns[f"bid_px_{suffix}"].append(UNDEFINED_PRICE)
                columns[f"ask_px_{suffix}"].append(UNDEFINED_PRICE)
                columns[f"bid_sz_{suffix}"].append(0)
                columns[f"ask_sz_{suffix}"].append(0)
                columns[f"bid_ct_{suffix}"].append(0)
                columns[f"ask_ct_{suffix}"].append(0)
            else:
                columns[f"bid_px_{suffix}"].append(
                    UNDEFINED_PRICE if bid == UNDEFINED_PRICE else bid - level * TICK_RAW
                )
                columns[f"ask_px_{suffix}"].append(
                    UNDEFINED_PRICE if ask == UNDEFINED_PRICE else ask + level * TICK_RAW
                )
                columns[f"bid_sz_{suffix}"].append(int(event["bid_size"]) + level)
                columns[f"ask_sz_{suffix}"].append(int(event["ask_size"]) + level)
                columns[f"bid_ct_{suffix}"].append(1)
                columns[f"ask_ct_{suffix}"].append(1)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pydict(columns, schema=schema),
        path,
        row_group_size=row_group_size,
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_line(value: dict[str, object]) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        + b"\n"
    )


def _write_qualification_evidence(
    data_root: Path,
    *,
    previous: Path,
    current: Path,
    current_snapshot_start_boundary_only: bool = False,
) -> Phase1AScreeningCalendar:
    schema_fingerprint = compute_schema_fingerprint(pq.ParquetFile(current).schema_arrow)
    dated_sources: list[tuple[date, str, str, int, bool]] = [
        (
            PREVIOUS_DATE,
            previous.relative_to(data_root / "mbp-10").as_posix(),
            _sha256(previous),
            previous.stat().st_size,
            True,
        ),
        (
            SOURCE_DATE,
            current.relative_to(data_root / "mbp-10").as_posix(),
            _sha256(current),
            current.stat().st_size,
            True,
        ),
    ]
    dated_sources.extend(
        (
            excluded,
            f"{excluded:%Y/%m/%d}/glbx-mdp3-{excluded:%Y%m%d}.mbp-10.parquet",
            f"{index:x}" * 64,
            1,
            False,
        )
        for index, excluded in enumerate(PHASE1A_EXCLUDED_SOURCE_DATES, start=1)
    )
    dated_sources.sort(key=lambda item: item[0])
    source_records = [
        {
            "byte_size": byte_size,
            "relative_uri": relative_uri,
            "sha256": sha256,
            "source_date": source_day.isoformat(),
        }
        for source_day, relative_uri, sha256, byte_size, _passed in dated_sources
    ]
    manifests = data_root / "derived/manifests"
    manifests.mkdir(parents=True, exist_ok=True)
    source_manifest = manifests / "mbp10_source_sha256_v1.jsonl"
    source_manifest.write_bytes(b"".join(_canonical_line(record) for record in source_records))
    source_manifest_sha = _sha256(source_manifest)
    qc_config_sha = load_structural_qc_config(DEFAULT_QC_CONFIG_PATH).sha256
    qc_records: list[dict[str, object]] = []
    for source_day, relative_uri, source_sha, byte_size, passed in dated_sources:
        hard_count = 0 if passed else 1
        snapshot_boundary = current_snapshot_start_boundary_only and source_day == SOURCE_DATE
        qc_records.append(
            {
                "artifact_schema": "systematic_fx.mbp10_structural_qc_file.v1",
                "checker_version": "mbp10_structural_qc_v1",
                "config_sha256": qc_config_sha,
                "coverage_complete": True,
                "diagnostic_counts": {"snapshot_flag_rows": 1} if snapshot_boundary else {},
                "expected_row_count": 1,
                "expected_row_group_count": 1,
                "first_ts_recv_ns": _midnight_ns(SOURCE_DATE) if snapshot_boundary else 0,
                "hard_violation_count": hard_count,
                "hard_violation_counts": {"synthetic_exclusion": hard_count},
                "last_ts_recv_ns": _midnight_ns(SOURCE_DATE) if snapshot_boundary else 0,
                "relative_uri": relative_uri,
                "research_eligible": False,
                "result": "PASS" if passed else "FAIL",
                "scanned_row_count": 1,
                "scanned_row_group_count": 1,
                "schema_fingerprint": schema_fingerprint,
                "source_byte_size": byte_size,
                "source_date": source_day.isoformat(),
                "source_manifest_sha256": source_manifest_sha,
                "source_sha256": source_sha,
            }
        )
    qc_manifest = manifests / "mbp10_structural_qc_v1.jsonl"
    qc_manifest.write_bytes(b"".join(_canonical_line(record) for record in qc_records))
    calendar = Phase1AScreeningCalendar(
        source_dates=(PREVIOUS_DATE, SOURCE_DATE),
        excluded_source_dates=PHASE1A_EXCLUDED_SOURCE_DATES,
        source_manifest_sha256=source_manifest_sha,
        qc_manifest_sha256=_sha256(qc_manifest),
        source_record_count=len(dated_sources),
        qc_pass_record_count=2,
        qc_fail_record_count=len(PHASE1A_EXCLUDED_SOURCE_DATES),
        qc_config_sha256=qc_config_sha,
        schema_fingerprint=schema_fingerprint,
    )
    (manifests / "phase1a_screening_source_date_calendar_v1.json").write_bytes(
        calendar.canonical_json()
    )
    return calendar


def _build_inputs(
    root: Path,
    current_events: list[dict[str, object]],
    *,
    positive_prior_volume: bool = True,
    seed_snapshot: bool = True,
    current_snapshot_start_boundary_only: bool = False,
):
    data_root = root / "data"
    previous = data_root / "mbp-10/2022/01/02/previous.parquet"
    current = data_root / "mbp-10/2022/01/03/current.parquet"
    prior_action = "T" if positive_prior_volume else "A"
    _write_source(
        previous,
        day=PREVIOUS_DATE,
        events=[
            _event(ONE_SECOND_NS, action=prior_action, size=100),
            _event(
                2 * ONE_SECOND_NS,
                instrument_id=202,
                action="T" if positive_prior_volume else "A",
                size=10,
            ),
        ],
        row_group_size=1,
    )
    events = ([_event(0, flags=32)] if seed_snapshot else []) + current_events
    _write_source(current, day=SOURCE_DATE, events=events, row_group_size=2)
    selection = select_next_eligible_contract(
        previous,
        current,
        previous_source_date=PREVIOUS_DATE,
        eligible_source_date=SOURCE_DATE,
    )
    calendar = _write_qualification_evidence(
        data_root,
        previous=previous,
        current=current,
        current_snapshot_start_boundary_only=current_snapshot_start_boundary_only,
    )
    return data_root, current, selection, calendar


class ScreeningConfigTests(unittest.TestCase):
    def test_frozen_config_and_distinct_integer_only_schemas(self) -> None:
        config = load_phase1a_screening_config()
        with DEFAULT_CONFIG_PATH.open("rb") as handle:
            document = tomllib.load(handle)

        self.assertEqual(document["feature_set"]["id"], FEATURE_VERSION)
        self.assertEqual(config.formula_sha256, FORMULA_SHA256)
        self.assertTrue(document["feature_set"]["screening_only"])
        self.assertFalse(document["feature_set"]["definition_status_available"])
        self.assertNotEqual(FEATURE_VERSION, "mbp10_pilot_v1")
        for schema in (ONE_SECOND_SCHEMA, FIVE_MINUTE_SCHEMA):
            self.assertFalse(any(pa.types.is_floating(field.type) for field in schema))


class ScreeningFeatureBuildTests(unittest.TestCase):
    def test_plans_hash_bound_zero_previous_volume_as_recorded_no_entry(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            data_root, source, selection, calendar = _build_inputs(
                root,
                [],
                positive_prior_volume=False,
            )

            reason = plan_phase1a_screening_no_entry_reason(
                source,
                data_root=data_root,
                source_date=SOURCE_DATE,
                selection=selection,
                calendar=calendar,
            )

        self.assertEqual(reason, NO_POSITIVE_PREVIOUS_SOURCE_TRADE_VOLUME)
        self.assertEqual(selection.selected.previous_trade_rows, 0)
        self.assertEqual(selection.selected.previous_trade_volume, 0)

    def test_planner_rejects_prior_trade_rows_volume_positivity_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            data_root, source, selection, calendar = _build_inputs(
                root,
                [],
                positive_prior_volume=False,
            )

            for rows, volume in ((1, 0), (0, 1)):
                selected = replace(
                    selection.selected,
                    previous_trade_rows=rows,
                    previous_trade_volume=volume,
                )
                document = selection.as_dict()
                document["selected"] = selected.as_dict()
                document["candidates"] = [selected.as_dict()]
                canonical_bytes = _canonical_line(document).rstrip(b"\n")
                mismatched = replace(
                    selection,
                    selected=selected,
                    candidates=(selected,),
                    canonical_bytes=canonical_bytes,
                    sha256=hashlib.sha256(canonical_bytes).hexdigest(),
                )

                with (
                    self.subTest(rows=rows, volume=volume),
                    self.assertRaisesRegex(
                        ScreeningFeatureBuildError,
                        "rows and volume positivity disagree",
                    ),
                ):
                    plan_phase1a_screening_no_entry_reason(
                        source,
                        data_root=data_root,
                        source_date=SOURCE_DATE,
                        selection=mismatched,
                        calendar=calendar,
                    )

    def test_plans_hash_bound_snapshot_only_source_start_as_recorded_no_entry(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            data_root, source, selection, calendar = _build_inputs(
                root,
                [],
                current_snapshot_start_boundary_only=True,
            )

            reason = plan_phase1a_screening_no_entry_reason(
                source,
                data_root=data_root,
                source_date=SOURCE_DATE,
                selection=selection,
                calendar=calendar,
            )

        self.assertEqual(reason, NO_PROVEN_COMPLETE_OBSERVED_1S_BUCKET)

    def test_planner_does_not_catch_unproven_boundary_without_snapshot_qc_proof(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            data_root, source, selection, calendar = _build_inputs(root, [])

            reason = plan_phase1a_screening_no_entry_reason(
                source,
                data_root=data_root,
                source_date=SOURCE_DATE,
                selection=selection,
                calendar=calendar,
            )
            with self.assertRaisesRegex(
                ScreeningFeatureBuildError,
                "no proven complete observed 1s bucket",
            ):
                build_phase1a_screening_features(
                    source,
                    data_root=data_root,
                    source_date=SOURCE_DATE,
                    selection=selection,
                    calendar=calendar,
                    code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
                )

        self.assertIsNone(reason)

    def test_builds_integer_depth_imbalance_path_state_and_audit_metadata(self) -> None:
        events = [
            _event(1 * ONE_SECOND_NS, bid_size=100, ask_size=50),
            _event(2 * ONE_SECOND_NS, bid_size=110, ask_size=40),
            _event(3 * ONE_SECOND_NS, action="R", side="N", empty_book=True),
            _event(4 * ONE_SECOND_NS, bid_size=120, ask_size=30),
            _event(
                5 * ONE_SECOND_NS,
                bid=1_100_000_000,
                ask=1_100_000_000,
                bid_size=80,
                ask_size=80,
            ),
            _event(299_200_000_000, bid_size=40, ask_size=100),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            data_root, source, selection, calendar = _build_inputs(root, events)
            first_report = build_phase1a_screening_features(
                source,
                data_root=data_root,
                source_date=SOURCE_DATE,
                selection=selection,
                calendar=calendar,
                code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
            )
            second_report = build_phase1a_screening_features(
                source,
                data_root=data_root,
                source_date=SOURCE_DATE,
                selection=selection,
                calendar=calendar,
                code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
            )

            one_second_path = Path(first_report.one_second.path)
            five_minute_path = Path(first_report.five_minute.path)
            one_second_file = pq.ParquetFile(one_second_path)
            five_minute_file = pq.ParquetFile(five_minute_path)
            one_second_rows = one_second_file.read().to_pylist()
            five_minute_row = five_minute_file.read().to_pylist()[0]

            self.assertEqual(first_report.one_second.disposition, "CREATED")
            self.assertEqual(first_report.five_minute.disposition, "CREATED")
            self.assertEqual(second_report.one_second.disposition, "REUSED")
            self.assertEqual(second_report.five_minute.disposition, "REUSED")
            self.assertIn("derived/features_1s", one_second_path.as_posix())
            self.assertIn("derived/research_5m", five_minute_path.as_posix())
            self.assertIn(f"version={FEATURE_VERSION}", one_second_path.as_posix())
            self.assertEqual(first_report.contract_selection_sha256, selection.sha256)
            self.assertEqual(first_report.previous_volume_sha256, selection.previous_volume.sha256)
            self.assertEqual(first_report.source_sha256, _sha256(source))
            self.assertEqual(first_report.formula_sha256, FORMULA_SHA256)
            self.assertEqual(first_report.calendar_sha256, calendar.sha256)
            self.assertEqual(first_report.code_snapshot_sha256, CODE_SNAPSHOT_SHA256)
            self.assertEqual(first_report.source_start_partial_one_second_excluded, 1)

            metadata = one_second_file.schema_arrow.metadata or {}
            self.assertEqual(metadata[b"systematic_fx.source_sha256"].decode(), _sha256(source))
            self.assertEqual(
                metadata[b"systematic_fx.contract_selection_sha256"].decode(),
                selection.sha256,
            )
            self.assertEqual(
                metadata[b"systematic_fx.config_sha256"].decode(),
                first_report.config_sha256,
            )
            self.assertEqual(metadata[b"systematic_fx.definition_status_available"], b"false")
            self.assertEqual(
                metadata[b"systematic_fx.code_snapshot_sha256"].decode(),
                CODE_SNAPSHOT_SHA256,
            )
            self.assertEqual(
                metadata[b"systematic_fx.calendar_sha256"].decode(),
                calendar.sha256,
            )
            self.assertEqual(
                metadata[b"systematic_fx.source_manifest_sha256"].decode(),
                first_report.source_manifest_sha256,
            )
            self.assertEqual(
                metadata[b"systematic_fx.qc_manifest_sha256"].decode(),
                first_report.qc_manifest_sha256,
            )
            self.assertEqual(
                metadata[b"systematic_fx.qc_config_sha256"].decode(),
                first_report.qc_config_sha256,
            )

            first = one_second_rows[0]
            second = one_second_rows[1]
            reset = one_second_rows[2]
            rearmed = one_second_rows[3]
            self.assertEqual(first["imbalance_numerator_l1"], 50)
            self.assertEqual(first["imbalance_denominator_l1"], 150)
            self.assertEqual(first["imbalance_signed_ppm_l1"], 333_333)
            self.assertIsNone(first["bid_depth_change_l1"])
            self.assertEqual(second["bid_depth_change_l1"], 10)
            self.assertEqual(second["ask_depth_change_l1"], -10)
            self.assertEqual(second["imbalance_numerator_change_l1"], 20)
            self.assertTrue(reset["reset_seen"])
            self.assertTrue(reset["recovery_marker_seen"])
            self.assertTrue(reset["recovery_required_at_close"])
            self.assertFalse(reset["valid_second"])
            self.assertTrue(rearmed["recovery_rearmed"])
            self.assertFalse(rearmed["recovery_required_at_close"])
            self.assertTrue(rearmed["valid_second"])
            self.assertIsNone(rearmed["bid_depth_change_l1"])
            self.assertEqual(first["spread_ticks"], 2)
            self.assertTrue(first["price_on_tick_grid"])
            self.assertTrue(first["quote_fresh"])

            self.assertEqual(five_minute_row["observed_seconds"], 6)
            self.assertEqual(five_minute_row["missing_seconds"], 294)
            self.assertEqual(five_minute_row["reset_seen_seconds"], 1)
            self.assertEqual(five_minute_row["locked_seconds"], 1)
            self.assertEqual(five_minute_row["decision_quote_age_ms"], 800)
            self.assertTrue(five_minute_row["decision_quote_fresh"])
            self.assertEqual(five_minute_row["imbalance_sign_changes_l1"], 1)
            self.assertFalse(five_minute_row["source_local_signal_input_valid"])
            self.assertFalse(five_minute_row["signal_input_valid"])
            self.assertFalse(five_minute_row["definition_status_available"])
            for level in DEPTH_LEVELS:
                self.assertIn(f"bid_cum_size_l{level}_mean_trunc", five_minute_row)
                self.assertIn(f"imbalance_signed_ppm_l{level}_mean_trunc", five_minute_row)

    def test_complete_source_local_window_still_cannot_claim_signal_input_valid(self) -> None:
        events = [_event(second * ONE_SECOND_NS) for second in range(1, 301)]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            data_root, source, selection, calendar = _build_inputs(root, events)
            report = build_phase1a_screening_features(
                source,
                data_root=data_root,
                source_date=SOURCE_DATE,
                selection=selection,
                calendar=calendar,
                code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
            )
            row = pq.read_table(report.five_minute.path).to_pylist()[0]

        self.assertEqual(row["observed_seconds"], 300)
        self.assertEqual(row["valid_seconds"], 300)
        self.assertEqual(row["missing_seconds"], 0)
        self.assertTrue(row["source_window_complete"])
        self.assertTrue(row["source_local_signal_input_valid"])
        self.assertFalse(row["definition_status_available"])
        self.assertFalse(row["signal_input_valid"])

    def test_physical_order_late_row_is_ignored_without_rewriting_closed_second(self) -> None:
        events = [
            _event(1 * ONE_SECOND_NS, bid_size=90, ask_size=60),
            _event(2 * ONE_SECOND_NS, bid_size=100, ask_size=50),
            _event(1 * ONE_SECOND_NS, bid_size=999, ask_size=1),
            _event(3 * ONE_SECOND_NS, bid_size=110, ask_size=40),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            data_root, source, selection, calendar = _build_inputs(root, events)
            report = build_phase1a_screening_features(
                source,
                data_root=data_root,
                source_date=SOURCE_DATE,
                selection=selection,
                calendar=calendar,
                code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
            )
            rows = pq.read_table(report.one_second.path).to_pylist()

        self.assertEqual(report.selected_rows, 5)
        self.assertEqual(report.late_rows_ignored, 1)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[1]["bid_cum_size_l1"], 100)
        self.assertEqual(rows[2]["bid_depth_change_l1"], 10)

    def test_maybe_bad_book_persists_until_snapshot_then_adjacent_clean_second(self) -> None:
        events = [
            _event(1 * ONE_SECOND_NS),
            _event(2 * ONE_SECOND_NS, flags=4),
            _event(3 * ONE_SECOND_NS),
            _event(4 * ONE_SECOND_NS, flags=32),
            _event(5 * ONE_SECOND_NS),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            data_root, source, selection, calendar = _build_inputs(root, events)
            report = build_phase1a_screening_features(
                source,
                data_root=data_root,
                source_date=SOURCE_DATE,
                selection=selection,
                calendar=calendar,
                code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
            )
            rows = pq.read_table(report.one_second.path).to_pylist()

        self.assertTrue(rows[0]["valid_second"])
        self.assertTrue(rows[1]["maybe_bad_book"])
        self.assertFalse(rows[1]["valid_second"])
        self.assertTrue(rows[2]["base_book_valid"])
        self.assertFalse(rows[2]["valid_second"])
        self.assertTrue(rows[2]["recovery_required_at_close"])
        self.assertTrue(rows[3]["recovery_marker_seen"])
        self.assertFalse(rows[3]["valid_second"])
        self.assertTrue(rows[4]["recovery_rearmed"])
        self.assertTrue(rows[4]["valid_second"])

    def test_source_unknown_and_long_gap_cannot_rearm_without_new_marker(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            data_root, source, selection, calendar = _build_inputs(
                root,
                [_event(1 * ONE_SECOND_NS), _event(2 * ONE_SECOND_NS)],
                seed_snapshot=False,
            )
            report = build_phase1a_screening_features(
                source,
                data_root=data_root,
                source_date=SOURCE_DATE,
                selection=selection,
                calendar=calendar,
                code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
            )
            unknown_rows = pq.read_table(report.one_second.path).to_pylist()
        self.assertTrue(all(row["recovery_required_at_close"] for row in unknown_rows))
        self.assertTrue(all(not row["valid_second"] for row in unknown_rows))

        events = [
            _event(2 * ONE_SECOND_NS),
            _event(3 * ONE_SECOND_NS),
            _event(4 * ONE_SECOND_NS, flags=32),
            _event(6 * ONE_SECOND_NS),
            _event(7 * ONE_SECOND_NS),
            _event(8 * ONE_SECOND_NS, flags=32),
            _event(9 * ONE_SECOND_NS),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            data_root, source, selection, calendar = _build_inputs(root, events)
            report = build_phase1a_screening_features(
                source,
                data_root=data_root,
                source_date=SOURCE_DATE,
                selection=selection,
                calendar=calendar,
                code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
            )
            rows = pq.read_table(report.one_second.path).to_pylist()
        by_second = {
            int(row["bucket_end"].timestamp() - _midnight_ns(SOURCE_DATE) / ONE_SECOND_NS): row
            for row in rows
        }
        self.assertFalse(by_second[6]["valid_second"])
        self.assertFalse(by_second[7]["valid_second"])
        self.assertTrue(by_second[9]["recovery_rearmed"])
        self.assertTrue(by_second[9]["valid_second"])

    def test_unproven_source_boundaries_are_excluded_from_both_artifacts(self) -> None:
        day_ns = 86_400 * ONE_SECOND_NS
        events = [
            _event(1 * ONE_SECOND_NS),
            _event(day_ns - 200_000_000),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            data_root, source, selection, calendar = _build_inputs(root, events)
            report = build_phase1a_screening_features(
                source,
                data_root=data_root,
                source_date=SOURCE_DATE,
                selection=selection,
                calendar=calendar,
                code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
            )
            one_second = pq.read_table(report.one_second.path).to_pylist()
            five_minute = pq.read_table(report.five_minute.path).to_pylist()

        day_start = _midnight_ns(SOURCE_DATE)
        day_end = day_start + day_ns
        self.assertEqual(report.source_start_partial_one_second_excluded, 1)
        self.assertEqual(report.unproven_closed_boundary_one_second_excluded, 1)
        self.assertEqual(report.unproven_closed_boundary_five_minute_excluded, 1)
        self.assertTrue(
            all(
                int(row["bucket_end"].timestamp() * ONE_SECOND_NS) != day_start
                for row in one_second
            )
        )
        self.assertTrue(
            all(int(row["bucket_end"].timestamp() * ONE_SECOND_NS) != day_end for row in one_second)
        )
        self.assertTrue(
            all(
                int(row["bucket_end"].timestamp() * ONE_SECOND_NS) != day_end for row in five_minute
            )
        )

    def test_freshness_uses_exact_nanoseconds_not_floored_milliseconds(self) -> None:
        events = [
            _event(1 * ONE_SECOND_NS),
            _event(298_999_999_999),
            _event(
                299_200_000_000,
                bid=1_100_000_000,
                ask=1_100_000_000,
            ),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            data_root, source, selection, calendar = _build_inputs(root, events)
            report = build_phase1a_screening_features(
                source,
                data_root=data_root,
                source_date=SOURCE_DATE,
                selection=selection,
                calendar=calendar,
                code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
            )
            row = pq.read_table(report.five_minute.path).to_pylist()[0]
        self.assertEqual(row["decision_quote_age_ms"], 1_000)
        self.assertFalse(row["decision_quote_fresh"])

    def test_rejects_code_config_selection_source_evidence_and_positive_volume_drift(self) -> None:
        events = [_event(ONE_SECOND_NS)]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            data_root, source, selection, calendar = _build_inputs(root, events)
            with self.assertRaisesRegex(ScreeningFeatureBuildError, "code_snapshot_sha256"):
                build_phase1a_screening_features(
                    source,
                    data_root=data_root,
                    source_date=SOURCE_DATE,
                    selection=selection,
                    calendar=calendar,
                    code_snapshot_sha256="ABC",
                )

            drifted_selection = replace(
                selection,
                eligible_source_date=SOURCE_DATE + timedelta(days=1),
            )
            with self.assertRaisesRegex(ScreeningFeatureBuildError, "eligible_source_date"):
                build_phase1a_screening_features(
                    source,
                    data_root=data_root,
                    source_date=SOURCE_DATE,
                    selection=drifted_selection,
                    calendar=calendar,
                    code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
                )

            selection_document = json.loads(selection.canonical_bytes)
            selection_document["information_boundary"]["eligible_source_rows_read"] = True
            drifted_bytes = _canonical_line(selection_document)[:-1]
            drifted_boundary = replace(
                selection,
                canonical_bytes=drifted_bytes,
                sha256=hashlib.sha256(drifted_bytes).hexdigest(),
            )
            with self.assertRaisesRegex(ScreeningFeatureBuildError, "canonical document drift"):
                build_phase1a_screening_features(
                    source,
                    data_root=data_root,
                    source_date=SOURCE_DATE,
                    selection=drifted_boundary,
                    calendar=calendar,
                    code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
                )

            ineligible_calendar = replace(
                calendar,
                source_dates=(PREVIOUS_DATE,),
                source_record_count=7,
                qc_pass_record_count=1,
            )
            with self.assertRaisesRegex(ScreeningFeatureBuildError, "not eligible"):
                build_phase1a_screening_features(
                    source,
                    data_root=data_root,
                    source_date=SOURCE_DATE,
                    selection=selection,
                    calendar=ineligible_calendar,
                    code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
                )

            drifted_config = root / "drifted.toml"
            drifted_config.write_text(
                DEFAULT_CONFIG_PATH.read_text().replace(
                    "screening_only = true",
                    "screening_only = false",
                    1,
                )
            )
            with self.assertRaisesRegex(ScreeningFeatureBuildError, "config semantics drifted"):
                build_phase1a_screening_features(
                    source,
                    data_root=data_root,
                    source_date=SOURCE_DATE,
                    selection=selection,
                    calendar=calendar,
                    code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
                    config_path=drifted_config,
                )

            source.write_bytes(source.read_bytes() + b"drift")
            with self.assertRaisesRegex(ScreeningFeatureBuildError, "byte-size"):
                build_phase1a_screening_features(
                    source,
                    data_root=data_root,
                    source_date=SOURCE_DATE,
                    selection=selection,
                    calendar=calendar,
                    code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
                )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            data_root, source, selection, calendar = _build_inputs(
                root,
                events,
                positive_prior_volume=False,
            )
            with self.assertRaisesRegex(ScreeningFeatureBuildError, "positive prior trade volume"):
                build_phase1a_screening_features(
                    source,
                    data_root=data_root,
                    source_date=SOURCE_DATE,
                    selection=selection,
                    calendar=calendar,
                    code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
                )

    def test_rejects_footer_identity_and_immutable_output_drift(self) -> None:
        events = [_event(ONE_SECOND_NS)]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            data_root, source, selection, _calendar = _build_inputs(root, events)
            _write_source(
                source,
                day=SOURCE_DATE,
                events=events,
                mappings=[("6EH2", 999), ("6EM2", 202)],
            )
            previous = data_root / "mbp-10/2022/01/02/previous.parquet"
            calendar = _write_qualification_evidence(
                data_root,
                previous=previous,
                current=source,
            )
            with self.assertRaisesRegex(ScreeningFeatureBuildError, "footer outright"):
                build_phase1a_screening_features(
                    source,
                    data_root=data_root,
                    source_date=SOURCE_DATE,
                    selection=selection,
                    calendar=calendar,
                    code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
                )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            data_root, source, selection, calendar = _build_inputs(root, events)
            report = build_phase1a_screening_features(
                source,
                data_root=data_root,
                source_date=SOURCE_DATE,
                selection=selection,
                calendar=calendar,
                code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
            )
            with self.assertRaisesRegex(ScreeningFeatureBuildError, "content drift"):
                build_phase1a_screening_features(
                    source,
                    data_root=data_root,
                    source_date=SOURCE_DATE,
                    selection=selection,
                    calendar=calendar,
                    code_snapshot_sha256="f" * 64,
                )
            one_second = Path(report.one_second.path)
            five_minute_before = Path(report.five_minute.path).read_bytes()
            one_second.chmod(0o644)
            one_second.write_bytes(b"drift")

            with self.assertRaisesRegex(ScreeningFeatureBuildError, "content drift"):
                build_phase1a_screening_features(
                    source,
                    data_root=data_root,
                    source_date=SOURCE_DATE,
                    selection=selection,
                    calendar=calendar,
                    code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
                )
            self.assertEqual(one_second.read_bytes(), b"drift")
            self.assertEqual(Path(report.five_minute.path).read_bytes(), five_minute_before)


if __name__ == "__main__":
    unittest.main()
