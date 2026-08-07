from __future__ import annotations

import hashlib
import tempfile
import unittest
from collections.abc import Iterable
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Self
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq
from psycopg import IsolationLevel

from systematic_fx.data.contract_selection import (
    CONTRACT_SELECTION_POLICY_VERSION,
    CONTRACT_SELECTION_SCHEMA,
    ContractSelectionResult,
    ContractTradeVolume,
    EligibleContractCandidate,
    PreviousTradeVolumeSummary,
)
from systematic_fx.db import screening_feature_registry as registry
from systematic_fx.db.screening_feature_registry import (
    BatchEntryStatus,
    RawSourceReference,
    ScreeningFeatureArtifactError,
    ScreeningFeatureBatchEntry,
    ScreeningFeatureRegistrationReport,
    ScreeningFeatureRegistryDriftError,
    prepare_phase1a_screening_feature_batch,
    register_phase1a_screening_feature_batch,
)
from systematic_fx.features.screening import (
    FEATURE_VERSION,
    FIVE_MINUTE_SCHEMA,
    FORMULA_SHA256,
    NO_POSITIVE_PREVIOUS_SOURCE_TRADE_VOLUME,
    ONE_SECOND_SCHEMA,
    ScreeningArtifactReport,
    ScreeningFeatureBuildReport,
    load_phase1a_screening_config,
)
from systematic_fx.research.run_spec import RunSpec
from systematic_fx.validation.splits import (
    CALENDAR_VERSION,
    CAMPAIGN_ID,
    PHASE1A_EXCLUDED_SOURCE_DATES,
    SPLIT_VERSION,
    Phase1AScreeningCalendar,
)

SOURCE_DATES = tuple(date(2022, 1, 2) + timedelta(days=index) for index in range(5))
RAW_SYMBOL = "6EH2"
CONTRACT_MONTH = date(2022, 3, 1)
SOURCE_SCHEMA_SHA = "a" * 64
QC_MANIFEST_SHA = "c" * 64
QC_CONFIG_SHA = "d" * 64
CODE_SNAPSHOT_SHA = "e" * 64
SPLIT_SHA = "f" * 64
CODE_COMMIT = "1" * 40


def _canonical_line(value: dict[str, object]) -> bytes:
    import json

    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()


def _calendar(*, source_manifest_sha256: str) -> Phase1AScreeningCalendar:
    return Phase1AScreeningCalendar(
        source_dates=SOURCE_DATES,
        excluded_source_dates=PHASE1A_EXCLUDED_SOURCE_DATES,
        source_manifest_sha256=source_manifest_sha256,
        qc_manifest_sha256=QC_MANIFEST_SHA,
        source_record_count=len(SOURCE_DATES) + len(PHASE1A_EXCLUDED_SOURCE_DATES),
        qc_pass_record_count=len(SOURCE_DATES),
        qc_fail_record_count=len(PHASE1A_EXCLUDED_SOURCE_DATES),
        qc_config_sha256=QC_CONFIG_SHA,
        schema_fingerprint=SOURCE_SCHEMA_SHA,
    )


def _source_uri(day: date) -> str:
    stamp = day.strftime("%Y%m%d")
    return f"{day:%Y/%m/%d}/glbx-mdp3-{stamp}.mbp-10.parquet"


def _source(data_root: Path, day: date) -> RawSourceReference:
    relative_uri = _source_uri(day)
    path = data_root / "mbp-10" / relative_uri
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(f"raw-source:{day.isoformat()}".encode())
    return RawSourceReference(
        source_date=day,
        relative_uri=relative_uri,
        sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
    )


def _write_source_manifest(
    data_root: Path,
    sources: tuple[RawSourceReference, ...],
) -> str:
    source_by_date = {source.source_date: source for source in sources}
    records: list[dict[str, object]] = []
    for day in sorted((*SOURCE_DATES, *PHASE1A_EXCLUDED_SOURCE_DATES)):
        source = source_by_date.get(day)
        if source is None:
            source = RawSourceReference(
                source_date=day,
                relative_uri=_source_uri(day),
                sha256=hashlib.sha256(f"excluded:{day.isoformat()}".encode()).hexdigest(),
            )
            byte_size = 0
        else:
            byte_size = (data_root / "mbp-10" / source.relative_uri).stat().st_size
        records.append(
            {
                "byte_size": byte_size,
                "relative_uri": source.relative_uri,
                "sha256": source.sha256,
                "source_date": day.isoformat(),
            }
        )
    path = data_root / "derived/manifests/mbp10_source_sha256_v1.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = b"".join(_canonical_line(record) for record in records)
    path.write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()


def _write_footer_manifest(
    data_root: Path,
    sources: tuple[RawSourceReference, ...],
) -> str:
    source_by_date = {source.source_date: source for source in sources}
    records: list[dict[str, object]] = []
    for day in sorted((*SOURCE_DATES, *PHASE1A_EXCLUDED_SOURCE_DATES)):
        source = source_by_date.get(day)
        if source is None:
            relative_uri = _source_uri(day)
            byte_size = 0
            row_count = 0
        else:
            relative_uri = source.relative_uri
            byte_size = (data_root / "mbp-10" / relative_uri).stat().st_size
            row_count = 1
        records.append(
            {
                "contract": {
                    "dataset": "GLBX.MDP3",
                    "price_scale": "1e-9",
                    "schema": "mbp-10",
                },
                "file_size_bytes": byte_size,
                "path": relative_uri,
                "row_count": row_count,
                "schema_fingerprint": SOURCE_SCHEMA_SHA,
                "source_date": day.isoformat(),
            }
        )
    path = data_root / "derived/manifests/mbp10_footer_manifest_v1.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = b"".join(_canonical_line(record) for record in records)
    path.write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()


def _selection(
    previous: RawSourceReference,
    current: RawSourceReference,
    *,
    instrument_id: int,
    previous_trade_rows: int = 3,
    previous_trade_volume: int = 30,
) -> ContractSelectionResult:
    contract = ContractTradeVolume(
        contract_month=CONTRACT_MONTH,
        raw_symbols=(RAW_SYMBOL,),
        instrument_ids=(instrument_id,),
        trade_rows=previous_trade_rows,
        trade_volume=previous_trade_volume,
    )
    previous_document: dict[str, object] = {
        "artifact_schema": f"{CONTRACT_SELECTION_SCHEMA}.previous_volume",
        "contracts": [contract.as_dict()],
        "excluded_mappings": [],
        "policy_version": CONTRACT_SELECTION_POLICY_VERSION,
        "scan": {
            "excluded_trade_rows": 0,
            "excluded_trade_volume": 0,
            "row_groups_scanned": 1,
            "rows_scanned": 3,
            "trade_rows": previous_trade_rows,
            "trade_volume": previous_trade_volume,
        },
        "source_date": previous.source_date.isoformat(),
        "source_sha256": previous.sha256,
    }
    previous_bytes = _canonical_line(previous_document).rstrip(b"\n")
    previous_summary = PreviousTradeVolumeSummary(
        source_date=previous.source_date,
        row_groups_scanned=1,
        rows_scanned=3,
        trade_rows=previous_trade_rows,
        trade_volume=previous_trade_volume,
        excluded_trade_rows=0,
        excluded_trade_volume=0,
        source_sha256=previous.sha256,
        contracts=(contract,),
        excluded_mappings=(),
        canonical_bytes=previous_bytes,
        sha256=hashlib.sha256(previous_bytes).hexdigest(),
    )
    selected = EligibleContractCandidate(
        instrument_id=instrument_id,
        raw_symbol=RAW_SYMBOL,
        contract_month=CONTRACT_MONTH,
        previous_trade_rows=previous_trade_rows,
        previous_trade_volume=previous_trade_volume,
    )
    selection_document: dict[str, object] = {
        "artifact_schema": CONTRACT_SELECTION_SCHEMA,
        "candidates": [selected.as_dict()],
        "eligible_source_date": current.source_date.isoformat(),
        "eligible_source_sha256": current.sha256,
        "expiry_exclusions": [],
        "information_boundary": {
            "eligible_source_rows_read": False,
            "volume_source": "PREVIOUS_SOURCE_DATE_ONLY",
        },
        "policy_version": CONTRACT_SELECTION_POLICY_VERSION,
        "previous_source_date": previous.source_date.isoformat(),
        "previous_source_sha256": previous.sha256,
        "previous_volume_sha256": previous_summary.sha256,
        "selected": selected.as_dict(),
    }
    selection_bytes = _canonical_line(selection_document).rstrip(b"\n")
    return ContractSelectionResult(
        previous_source_date=previous.source_date,
        eligible_source_date=current.source_date,
        previous_source_sha256=previous.sha256,
        eligible_source_sha256=current.sha256,
        selected=selected,
        candidates=(selected,),
        expiry_exclusions=(),
        previous_volume=previous_summary,
        canonical_bytes=selection_bytes,
        sha256=hashlib.sha256(selection_bytes).hexdigest(),
    )


def _default_value(
    field: pa.Field,
    *,
    bucket_end: datetime,
    instrument_id: int,
) -> object:
    if field.name == "feature_version":
        return FEATURE_VERSION
    if field.name == "screening_only":
        return True
    if field.name == "definition_status_available":
        return False
    if field.name == "source_date":
        return bucket_end.date()
    if field.name == "contract":
        return RAW_SYMBOL
    if field.name == "instrument_id":
        return instrument_id
    if field.name == "bucket_end":
        return bucket_end
    if field.nullable:
        return None
    if pa.types.is_timestamp(field.type):
        return bucket_end
    if pa.types.is_date(field.type):
        return bucket_end.date()
    if pa.types.is_string(field.type):
        return "N"
    if pa.types.is_boolean(field.type):
        return False
    if pa.types.is_integer(field.type):
        return 0
    raise AssertionError(f"unhandled field: {field}")


def _placeholder_artifact() -> ScreeningArtifactReport:
    return ScreeningArtifactReport(
        path="placeholder",
        disposition="CREATED",
        sha256="0" * 64,
        rows=1,
        schema_sha256="0" * 64,
        min_bucket_end="2022-01-01T00:00:01+00:00",
        max_bucket_end="2022-01-01T00:00:01+00:00",
    )


def _base_report(
    data_root: Path,
    *,
    source: RawSourceReference,
    previous: RawSourceReference,
    selection: ContractSelectionResult,
    calendar: Phase1AScreeningCalendar,
) -> ScreeningFeatureBuildReport:
    config = load_phase1a_screening_config()
    return ScreeningFeatureBuildReport(
        feature_version=FEATURE_VERSION,
        screening_only=True,
        research_eligible=False,
        definition_status_available=False,
        source_path=str(data_root / "mbp-10" / source.relative_uri),
        source_date=source.source_date.isoformat(),
        source_sha256=source.sha256,
        source_schema_sha256=calendar.schema_fingerprint,
        source_manifest_sha256=calendar.source_manifest_sha256,
        qc_manifest_sha256=calendar.qc_manifest_sha256,
        qc_config_sha256=calendar.qc_config_sha256,
        calendar_sha256=calendar.sha256,
        code_snapshot_sha256=CODE_SNAPSHOT_SHA,
        source_rows=2,
        selected_rows=2,
        late_rows_ignored=0,
        source_start_partial_one_second_excluded=0,
        unproven_closed_boundary_one_second_excluded=0,
        unproven_closed_boundary_five_minute_excluded=0,
        config_path=str(config.path),
        config_sha256=config.sha256,
        formula_sha256=FORMULA_SHA256,
        contract_selection_sha256=selection.sha256,
        previous_volume_sha256=selection.previous_volume.sha256,
        previous_source_date=previous.source_date.isoformat(),
        instrument_id=selection.selected.instrument_id,
        contract=RAW_SYMBOL,
        contract_month=CONTRACT_MONTH.isoformat(),
        previous_trade_rows=3,
        previous_trade_volume=30,
        one_second=_placeholder_artifact(),
        five_minute=_placeholder_artifact(),
    )


def _write_feature_artifact(
    data_root: Path,
    *,
    report: ScreeningFeatureBuildReport,
    base_schema: pa.Schema,
    directory_name: str,
    granularity: str,
) -> ScreeningArtifactReport:
    path = (
        data_root
        / f"derived/{directory_name}/version={FEATURE_VERSION}/contract={RAW_SYMBOL}"
        / f"source_date={report.source_date}/part-000.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    source_date = date.fromisoformat(report.source_date)
    first = datetime.combine(source_date, datetime.min.time(), tzinfo=UTC) + timedelta(
        seconds=1 if granularity == "1s" else 300
    )
    second = first + timedelta(seconds=1 if granularity == "1s" else 300)
    schema = base_schema.with_metadata(
        registry._expected_artifact_metadata(report, granularity=granularity)
    )
    rows = [
        {
            field.name: _default_value(
                field,
                bucket_end=bucket_end,
                instrument_id=report.instrument_id,
            )
            for field in schema
        }
        for bucket_end in (first, second)
    ]
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return ScreeningArtifactReport(
        path=str(path),
        disposition="CREATED",
        sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        rows=2,
        schema_sha256=registry._schema_sha256(schema),
        min_bucket_end=first.isoformat(),
        max_bucket_end=second.isoformat(),
    )


def _built_entry(
    data_root: Path,
    *,
    source: RawSourceReference,
    previous: RawSourceReference,
    calendar: Phase1AScreeningCalendar,
    instrument_id: int,
) -> ScreeningFeatureBatchEntry:
    selection = _selection(previous, source, instrument_id=instrument_id)
    report = _base_report(
        data_root,
        source=source,
        previous=previous,
        selection=selection,
        calendar=calendar,
    )
    report = replace(
        report,
        one_second=_write_feature_artifact(
            data_root,
            report=report,
            base_schema=ONE_SECOND_SCHEMA,
            directory_name="features_1s",
            granularity="1s",
        ),
        five_minute=_write_feature_artifact(
            data_root,
            report=report,
            base_schema=FIVE_MINUTE_SCHEMA,
            directory_name="research_5m",
            granularity="5m",
        ),
    )
    return ScreeningFeatureBatchEntry(
        source=source,
        status=BatchEntryStatus.BUILT,
        report=report,
        selection=selection,
        previous_source=previous,
    )


def _parameters(entries: tuple[ScreeningFeatureBatchEntry, ...]) -> dict[str, object]:
    return {
        "batch_entries": [
            {
                "current_source": registry._source_document(entry.source),
                "no_entry_reason": entry.no_entry_reason,
                "previous_source": (
                    registry._source_document(entry.previous_source)
                    if entry.previous_source is not None
                    else None
                ),
                "previous_volume_sha256": (
                    entry.selection.previous_volume.sha256 if entry.selection is not None else None
                ),
                "previous_volume_document": (
                    entry.selection.previous_volume.as_dict()
                    if entry.selection is not None
                    else None
                ),
                "selection_sha256": entry.selection.sha256 if entry.selection is not None else None,
                "selection_document": (
                    entry.selection.as_dict() if entry.selection is not None else None
                ),
                "status": entry.status.value,
            }
            for entry in entries
        ],
        "batch_source_dates": [entry.source.source_date.isoformat() for entry in entries],
        "batch_status_by_date": {
            entry.source.source_date.isoformat(): entry.status.value for entry in entries
        },
        "config_sha256": load_phase1a_screening_config().sha256,
        "definition_status_available": False,
        "formula_sha256": FORMULA_SHA256,
        "no_entry_reason_by_date": {
            entry.source.source_date.isoformat(): entry.no_entry_reason
            for entry in entries
            if entry.status is BatchEntryStatus.RECORDED_NO_ENTRY
        },
        "previous_volume_sha256_by_date": {
            entry.source.source_date.isoformat(): entry.selection.previous_volume.sha256
            for entry in entries
            if entry.selection is not None
        },
        "research_eligible": False,
        "screening_only": True,
        "selection_sha256_by_date": {
            entry.source.source_date.isoformat(): entry.selection.sha256
            for entry in entries
            if entry.selection is not None
        },
    }


def _run_spec(
    calendar: Phase1AScreeningCalendar,
    entries: tuple[ScreeningFeatureBatchEntry, ...],
    *,
    footer_manifest_sha256: str,
) -> RunSpec:
    config = load_phase1a_screening_config()
    return RunSpec(
        campaign_id=CAMPAIGN_ID,
        experiment_id=None,
        run_kind="FEATURE_BUILD",
        engine_version="phase1a_screening_feature_builder_v1",
        source_manifest_hashes={
            "mbp10_footer_manifest_v1": footer_manifest_sha256,
            "mbp10_source_sha256_v1": calendar.source_manifest_sha256,
            "mbp10_structural_qc_v1": calendar.qc_manifest_sha256,
        },
        eligible_calendar_version=CALENDAR_VERSION,
        eligible_calendar_sha256=calendar.sha256,
        split_version=SPLIT_VERSION,
        split_sha256=SPLIT_SHA,
        feature_version=FEATURE_VERSION,
        feature_sha256=config.sha256,
        outcome_version="phase1a_outcomes_v1",
        outcome_sha256="2" * 64,
        cost_version="phase1a_conservative_cost_v1",
        cost_sha256="3" * 64,
        execution_version="phase1a_conservative_execution_v1",
        execution_sha256="4" * 64,
        code_commit=CODE_COMMIT,
        code_snapshot_sha256=CODE_SNAPSHOT_SHA,
        dependency_lock_sha256="5" * 64,
        runtime_environment={"python": "3.12", "platform": "unit-test"},
        random_seed=0,
        direction="BOTH",
        signal_policy={"applicable": False},
        entry_policy={"applicable": False},
        barrier_policy={"applicable": False},
        terminal_policy={"applicable": False},
        parameters=_parameters(entries),
    )


def _fixture(
    root: Path,
    *,
    middle_no_entry: bool = False,
) -> tuple[
    Path,
    Phase1AScreeningCalendar,
    tuple[ScreeningFeatureBatchEntry, ...],
    RunSpec,
]:
    data_root = root / "data"
    (data_root / "derived").mkdir(parents=True)
    (data_root / "mbp-10").mkdir()
    sources = tuple(_source(data_root, day) for day in SOURCE_DATES)
    footer_manifest_sha256 = _write_footer_manifest(data_root, sources)
    calendar = _calendar(source_manifest_sha256=_write_source_manifest(data_root, sources))
    entries: list[ScreeningFeatureBatchEntry] = [
        ScreeningFeatureBatchEntry(
            source=sources[0],
            status=BatchEntryStatus.RECORDED_NO_ENTRY,
            no_entry_reason="MISSING_PREVIOUS_COMPLETED_SESSION",
        )
    ]
    entries.append(
        _built_entry(
            data_root,
            source=sources[1],
            previous=sources[0],
            calendar=calendar,
            instrument_id=201,
        )
    )
    if middle_no_entry:
        entries.append(
            ScreeningFeatureBatchEntry(
                source=sources[2],
                status=BatchEntryStatus.RECORDED_NO_ENTRY,
                previous_source=sources[1],
                no_entry_reason="ACTIVE_MAPPING_AMBIGUITY",
            )
        )
        previous_for_fourth = sources[2]
    else:
        entries.append(
            _built_entry(
                data_root,
                source=sources[2],
                previous=sources[1],
                calendar=calendar,
                instrument_id=202,
            )
        )
        previous_for_fourth = sources[2]
    entries.append(
        _built_entry(
            data_root,
            source=sources[3],
            previous=previous_for_fourth,
            calendar=calendar,
            instrument_id=203,
        )
    )
    entries.append(
        _built_entry(
            data_root,
            source=sources[4],
            previous=sources[3],
            calendar=calendar,
            instrument_id=204,
        )
    )
    frozen_entries = tuple(entries)
    return (
        data_root,
        calendar,
        frozen_entries,
        _run_spec(
            calendar,
            frozen_entries,
            footer_manifest_sha256=footer_manifest_sha256,
        ),
    )


class ScreeningFeaturePreparationTests(unittest.TestCase):
    def test_verifies_five_date_batch_and_builds_screening_only_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root, calendar, entries, run_spec = _fixture(Path(directory))
            prepared = prepare_phase1a_screening_feature_batch(
                data_root=data_root,
                calendar=calendar,
                run_spec=run_spec,
                entries=entries,
                dataset_key="test_mbp10",
            )

            self.assertEqual(len(prepared.entries), 5)
            self.assertEqual(len(prepared.artifacts), 8)
            self.assertEqual(len(prepared.raw_sources), 5)
            self.assertEqual(
                prepared.footer_manifest_sha256,
                run_spec.source_manifest_hashes["mbp10_footer_manifest_v1"],
            )
            self.assertFalse(prepared.manifest_path.exists())
            self.assertEqual(
                prepared.manifest_path.parent.name,
                "phase1a_feature_build_v1",
            )
            self.assertEqual(
                prepared.manifest_document["authority"],
                {
                    "definition_status_available": False,
                    "research_eligible": False,
                    "screening_only": True,
                    "validation_scope": "BYTE_SCHEMA_METADATA_AND_LINEAGE_ONLY",
                },
            )
            first = prepared.manifest_document["batch"]["entries"][0]  # type: ignore[index]
            self.assertEqual(first["no_entry_reason"], "MISSING_PREVIOUS_COMPLETED_SESSION")
            built = prepared.manifest_document["batch"]["entries"][1]  # type: ignore[index]
            self.assertEqual(
                built["previous_source"]["sha256"],  # type: ignore[index]
                entries[0].source.sha256,
            )
            assert entries[1].selection is not None
            self.assertEqual(
                built["selection"]["selection_document"],  # type: ignore[index]
                entries[1].selection.as_dict(),
            )
            self.assertEqual(
                built["selection"]["previous_volume_document"],  # type: ignore[index]
                entries[1].selection.previous_volume.as_dict(),
            )
            self.assertEqual(
                prepared.manifest_document["run_spec"]["run_fingerprint"],  # type: ignore[index]
                run_spec.fingerprint,
            )

    def test_footer_manifest_and_runspec_identity_drift_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root, calendar, entries, run_spec = _fixture(Path(directory))
            wrong_run_spec = replace(
                run_spec,
                source_manifest_hashes={
                    **dict(run_spec.source_manifest_hashes),
                    "mbp10_footer_manifest_v1": "0" * 64,
                },
            )
            with self.assertRaisesRegex(
                ScreeningFeatureArtifactError,
                "footer/source/QC manifest identities drift",
            ):
                prepare_phase1a_screening_feature_batch(
                    data_root=data_root,
                    calendar=calendar,
                    run_spec=wrong_run_spec,
                    entries=entries,
                )

            prepared = prepare_phase1a_screening_feature_batch(
                data_root=data_root,
                calendar=calendar,
                run_spec=run_spec,
                entries=entries,
            )
            footer_path = prepared.footer_manifest.path
            payload = bytearray(footer_path.read_bytes())
            payload[len(payload) // 2] ^= 1
            footer_path.write_bytes(payload)
            with self.assertRaisesRegex(
                ScreeningFeatureRegistryDriftError,
                "footer manifest changed after preparation",
            ):
                registry._publish_prepared(prepared)

    def test_no_entry_is_allowed_in_any_position_with_reason_and_raw_decision_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root, calendar, entries, run_spec = _fixture(
                Path(directory),
                middle_no_entry=True,
            )
            prepared = prepare_phase1a_screening_feature_batch(
                data_root=data_root,
                calendar=calendar,
                run_spec=run_spec,
                entries=entries,
            )

            self.assertEqual(len(prepared.artifacts), 6)
            middle = prepared.entries[2]
            self.assertEqual(middle.status, BatchEntryStatus.RECORDED_NO_ENTRY)
            self.assertEqual(middle.no_entry_reason, "ACTIVE_MAPPING_AMBIGUITY")
            assert middle.previous_source is not None
            parameters = registry._plain_run_spec(run_spec)["parameters"]
            batch_entries = parameters["batch_entries"]  # type: ignore[index]
            self.assertEqual(batch_entries[2]["current_source"]["sha256"], middle.source.sha256)  # type: ignore[index]
            self.assertEqual(
                batch_entries[2]["previous_source"]["sha256"],  # type: ignore[index]
                middle.previous_source.sha256,
            )
            assert entries[1].selection is not None
            self.assertEqual(
                batch_entries[1]["selection_document"],  # type: ignore[index]
                entries[1].selection.as_dict(),
            )
            self.assertEqual(
                batch_entries[1]["previous_volume_document"],  # type: ignore[index]
                entries[1].selection.previous_volume.as_dict(),
            )

            stale_predecessor = list(entries)
            stale_predecessor[3] = replace(
                stale_predecessor[3],
                previous_source=entries[1].source,
            )
            with self.assertRaisesRegex(
                ScreeningFeatureArtifactError,
                "exact preceding canonical source manifest record",
            ):
                prepare_phase1a_screening_feature_batch(
                    data_root=data_root,
                    calendar=calendar,
                    run_spec=run_spec,
                    entries=stale_predecessor,
                )

            missing_reason = list(entries)
            missing_reason[2] = replace(missing_reason[2], no_entry_reason=None)
            with self.assertRaisesRegex(ScreeningFeatureArtifactError, "no_entry_reason"):
                prepare_phase1a_screening_feature_batch(
                    data_root=data_root,
                    calendar=calendar,
                    run_spec=run_spec,
                    entries=missing_reason,
                )
            built_reason = list(entries)
            built_reason[1] = replace(built_reason[1], no_entry_reason="SHOULD_NOT_EXIST")
            with self.assertRaisesRegex(ScreeningFeatureArtifactError, "must be None"):
                prepare_phase1a_screening_feature_batch(
                    data_root=data_root,
                    calendar=calendar,
                    run_spec=run_spec,
                    entries=built_reason,
                )

    def test_no_entry_manifest_preserves_selection_and_previous_volume_audit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root, calendar, original_entries, original_run_spec = _fixture(Path(directory))
            selected_no_entry = replace(
                original_entries[2],
                status=BatchEntryStatus.RECORDED_NO_ENTRY,
                report=None,
                no_entry_reason="NO_PROVEN_COMPLETE_OBSERVED_1S_BUCKET",
            )
            entries = (*original_entries[:2], selected_no_entry, *original_entries[3:])
            run_spec = _run_spec(
                calendar,
                entries,
                footer_manifest_sha256=original_run_spec.source_manifest_hashes[
                    "mbp10_footer_manifest_v1"
                ],
            )

            prepared = prepare_phase1a_screening_feature_batch(
                data_root=data_root,
                calendar=calendar,
                run_spec=run_spec,
                entries=entries,
            )

            assert selected_no_entry.selection is not None
            manifest_entry = prepared.manifest_document["batch"]["entries"][2]  # type: ignore[index]
            selection_audit = manifest_entry["selection_audit"]
            self.assertEqual(
                selection_audit["contract_selection_sha256"],
                selected_no_entry.selection.sha256,
            )
            self.assertEqual(
                selection_audit["previous_volume_sha256"],
                selected_no_entry.selection.previous_volume.sha256,
            )
            self.assertEqual(
                selection_audit["selection_document"],
                selected_no_entry.selection.as_dict(),
            )
            parameters = registry._plain_run_spec(run_spec)["parameters"]
            source_date = selected_no_entry.source.source_date.isoformat()
            self.assertEqual(
                parameters["selection_sha256_by_date"][source_date],
                selected_no_entry.selection.sha256,
            )
            self.assertEqual(
                parameters["previous_volume_sha256_by_date"][source_date],
                selected_no_entry.selection.previous_volume.sha256,
            )

    def test_zero_previous_volume_no_entry_preserves_canonical_selection_audit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root, calendar, original_entries, original_run_spec = _fixture(Path(directory))
            source = original_entries[2].source
            previous_source = original_entries[2].previous_source
            assert previous_source is not None
            zero_selection = _selection(
                previous_source,
                source,
                instrument_id=202,
                previous_trade_rows=0,
                previous_trade_volume=0,
            )
            selected_no_entry = replace(
                original_entries[2],
                status=BatchEntryStatus.RECORDED_NO_ENTRY,
                report=None,
                selection=zero_selection,
                no_entry_reason=NO_POSITIVE_PREVIOUS_SOURCE_TRADE_VOLUME,
            )
            entries = (*original_entries[:2], selected_no_entry, *original_entries[3:])
            run_spec = _run_spec(
                calendar,
                entries,
                footer_manifest_sha256=original_run_spec.source_manifest_hashes[
                    "mbp10_footer_manifest_v1"
                ],
            )

            prepared = prepare_phase1a_screening_feature_batch(
                data_root=data_root,
                calendar=calendar,
                run_spec=run_spec,
                entries=entries,
            )

            manifest_entry = prepared.manifest_document["batch"]["entries"][2]  # type: ignore[index]
            selection_audit = manifest_entry["selection_audit"]
            self.assertEqual(
                selection_audit["contract_selection_sha256"],
                zero_selection.sha256,
            )
            self.assertEqual(
                selection_audit["previous_volume_sha256"],
                zero_selection.previous_volume.sha256,
            )
            self.assertEqual(
                selection_audit["previous_volume_document"],
                zero_selection.previous_volume.as_dict(),
            )
            self.assertEqual(
                selection_audit["selection_document"],
                zero_selection.as_dict(),
            )
            parameters = registry._plain_run_spec(run_spec)["parameters"]
            source_date = selected_no_entry.source.source_date.isoformat()
            self.assertEqual(
                parameters["selection_sha256_by_date"][source_date],
                zero_selection.sha256,
            )
            self.assertEqual(
                parameters["previous_volume_sha256_by_date"][source_date],
                zero_selection.previous_volume.sha256,
            )

    def test_report_and_post_verification_byte_drift_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root, calendar, entries, run_spec = _fixture(Path(directory))
            drifted_entries = list(entries)
            assert drifted_entries[1].report is not None
            drifted_report = replace(
                drifted_entries[1].report,
                one_second=replace(drifted_entries[1].report.one_second, sha256="0" * 64),
            )
            drifted_entries[1] = replace(drifted_entries[1], report=drifted_report)
            with self.assertRaisesRegex(ScreeningFeatureArtifactError, "bytes differ"):
                prepare_phase1a_screening_feature_batch(
                    data_root=data_root,
                    calendar=calendar,
                    run_spec=run_spec,
                    entries=drifted_entries,
                )

            prepared = prepare_phase1a_screening_feature_batch(
                data_root=data_root,
                calendar=calendar,
                run_spec=run_spec,
                entries=entries,
            )
            original = prepared.artifacts[0].original_path
            original.write_bytes(original.read_bytes() + b"drift")
            with self.assertRaisesRegex(
                ScreeningFeatureRegistryDriftError,
                "changed after preparation",
            ):
                registry._publish_prepared(prepared)

    def test_same_size_current_and_previous_raw_source_mutations_are_rejected(self) -> None:
        for source_index, label in ((0, "previous"), (1, "current")):
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                data_root, calendar, entries, run_spec = _fixture(Path(directory))
                source = entries[source_index].source
                path = data_root / "mbp-10" / source.relative_uri
                original_size = path.stat().st_size
                payload = bytearray(path.read_bytes())
                payload[len(payload) // 2] ^= 1
                path.write_bytes(payload)
                self.assertEqual(path.stat().st_size, original_size)

                with self.assertRaisesRegex(
                    ScreeningFeatureArtifactError,
                    "canonical source manifest SHA-256",
                ):
                    prepare_phase1a_screening_feature_batch(
                        data_root=data_root,
                        calendar=calendar,
                        run_spec=run_spec,
                        entries=entries,
                    )


class _Result:
    def __init__(self, value: object) -> None:
        self.value = value

    def fetchone(self) -> dict[str, Any] | None:
        if isinstance(self.value, list):
            return self.value[0] if self.value else None
        return self.value if isinstance(self.value, dict) else None

    def fetchall(self) -> list[dict[str, Any]]:
        if isinstance(self.value, list):
            return self.value
        if isinstance(self.value, dict):
            return [self.value]
        return []


class _Transaction:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    def __enter__(self) -> Self:
        self.connection.transaction_entries += 1
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Connection:
    def __init__(self, responses: Iterable[object] = ()) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, object]] = []
        self.isolation_level: IsolationLevel | None = None
        self.transaction_entries = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def transaction(self) -> _Transaction:
        return _Transaction(self)

    def execute(self, sql: str, parameters: object = ()) -> _Result:
        self.calls.append((" ".join(sql.split()), parameters))
        if not self.responses:
            raise AssertionError(f"unexpected SQL: {sql}")
        return _Result(self.responses.pop(0))


class ScreeningFeatureTransactionTests(unittest.TestCase):
    def test_partition_sets_null_instrument_and_exact_current_previous_links(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root, calendar, entries, run_spec = _fixture(Path(directory))
            prepared = prepare_phase1a_screening_feature_batch(
                data_root=data_root,
                calendar=calendar,
                run_spec=run_spec,
                entries=entries,
            )
            artifact = prepared.artifacts[0]
            entry = registry._entry_for_artifact(prepared, artifact)
            assert entry.previous_source is not None
            source_ids = {
                raw.reference.relative_uri: index + 10
                for index, raw in enumerate(prepared.raw_sources)
            }
            control = registry._ControlPlane(
                dataset_id=7,
                campaign_id=8,
                research_run_spec_id=9,
                source_file_ids=source_ids,
            )
            metadata = registry._partition_metadata(
                prepared,
                artifact,
                research_run_spec_id=9,
            )
            partition_key = registry._partition_key(prepared, artifact)
            row = {
                "derived_partition_id": 51,
                "partition_key": partition_key,
                "dataset_id": 7,
                "instrument_id": None,
                "partition_type": artifact.partition_type,
                "definition_version": FEATURE_VERSION,
                "source_date": artifact.source_date,
                "uri": artifact.canonical_path.as_uri(),
                "sha256": artifact.sha256,
                "row_count": artifact.row_count,
                "min_event_time_ns": artifact.min_event_time_ns,
                "max_event_time_ns": artifact.max_event_time_ns,
                "source_manifest_sha256": calendar.source_manifest_sha256,
                "code_commit": run_spec.code_commit,
                "config_sha256": prepared.config_sha256,
                "manifest_artifact_id": 41,
                "build_job_id": 31,
                "status": "VALIDATED",
                "metadata": metadata,
                "validated_at": datetime.now(UTC),
            }
            links = sorted(
                (
                    (
                        source_ids[entry.source.relative_uri],
                        entry.source.sha256,
                    ),
                    (
                        source_ids[entry.previous_source.relative_uri],
                        entry.previous_source.sha256,
                    ),
                )
            )
            stored_links = [
                {"source_file_id": source_id, "source_sha256": source_sha}
                for source_id, source_sha in links
            ]
            connection = _Connection(
                [
                    {"derived_partition_id": 51},
                    [row],
                    None,
                    None,
                    stored_links,
                ]
            )

            partition_id, created = registry._ensure_partition(
                connection,
                prepared,
                artifact,
                control=control,
                manifest_artifact_id=41,
                build_job_id=31,
            )

            self.assertTrue(created)
            self.assertEqual(partition_id, 51)
            self.assertIn("instrument_id", connection.calls[0][0])
            self.assertIn("NULL", connection.calls[0][0])
            self.assertEqual(
                metadata["contract"]["provider_instrument_id"],  # type: ignore[index]
                entry.report.instrument_id if entry.report is not None else None,
            )
            source_inserts = [
                call for call in connection.calls if "derived_partition_sources" in call[0]
            ]
            self.assertEqual(len(source_inserts), 3)

            drifted_row = dict(row)
            drifted_row["instrument_id"] = 999
            drifted = _Connection([None, [drifted_row]])
            with self.assertRaisesRegex(
                ScreeningFeatureRegistryDriftError,
                "instrument_id",
            ):
                registry._ensure_partition(
                    drifted,
                    prepared,
                    artifact,
                    control=control,
                    manifest_artifact_id=41,
                    build_job_id=31,
                )

    def test_public_registration_is_serializable_and_returns_control_ids(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root, calendar, entries, run_spec = _fixture(Path(directory))
            prepared = prepare_phase1a_screening_feature_batch(
                data_root=data_root,
                calendar=calendar,
                run_spec=run_spec,
                entries=entries,
            )
            expected = ScreeningFeatureRegistrationReport(
                dataset_id=1,
                campaign_id=2,
                research_run_spec_id=3,
                build_job_id=4,
                manifest_artifact_id=5,
                partition_ids=tuple(range(10, 18)),
                source_file_ids=((entries[0].source.relative_uri, 6),),
                manifest_path=prepared.manifest_path,
                manifest_sha256=prepared.manifest_sha256,
                created_job=True,
                created_manifest_artifact=True,
                created_partitions=8,
            )
            connection = _Connection()
            with (
                mock.patch.object(
                    registry,
                    "prepare_phase1a_screening_feature_batch",
                    return_value=prepared,
                ),
                mock.patch.object(registry, "_publish_prepared") as publish,
                mock.patch.object(registry, "_register_prepared", return_value=expected),
                mock.patch.object(registry.psycopg, "connect", return_value=connection),
            ):
                result = register_phase1a_screening_feature_batch(
                    "postgresql:///test",
                    data_root=data_root,
                    calendar=calendar,
                    run_spec=run_spec,
                    entries=entries,
                )

            self.assertEqual(result, expected)
            self.assertEqual(connection.isolation_level, IsolationLevel.SERIALIZABLE)
            self.assertEqual(connection.transaction_entries, 1)
            publish.assert_called_once_with(prepared)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
