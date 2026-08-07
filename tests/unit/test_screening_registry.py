from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from contextlib import ExitStack
from copy import deepcopy
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from typing import Self
from unittest.mock import ANY, call, patch

import psycopg
from psycopg import IsolationLevel

from systematic_fx.db.screening_registry import (
    CONTROL_ARTIFACT_SUBDIRECTORY,
    DEFAULT_DATASET_KEY,
    EXPECTED_BARRIER_TICKS,
    REGISTRATION_SCHEMA,
    ScreeningRegistryDatabaseError,
    ScreeningRegistryDriftError,
    _assert_fields,
    _assert_registration_revision_invariants,
    _DatabaseRegistration,
    _ensure_artifact,
    _ensure_campaign,
    _ensure_days,
    _ensure_experiments,
    _ensure_splits,
    _load_registration_baseline,
    _open_verified_artifact_file,
    _publish_control_artifact,
    _verify_held_artifact_binding,
    prepare_phase1a_screening_registration,
    register_phase1a_screening_campaign,
)
from systematic_fx.research.hypotheses import canonical_json_bytes
from systematic_fx.research.provenance import build_code_snapshot, publish_code_snapshot
from systematic_fx.validation.splits import (
    PHASE1A_EXCLUDED_SOURCE_DATES,
    Phase1AScreeningCalendar,
    build_phase1a_screening_split,
)

ROOT = Path(__file__).resolve().parents[2]


class _Transaction:
    def __init__(self) -> None:
        self.entered = False
        self.exited = False

    def __enter__(self) -> Self:
        self.entered = True
        return self

    def __exit__(self, *_args: object) -> None:
        self.exited = True


class _Connection:
    def __init__(self) -> None:
        self.isolation_level: IsolationLevel | None = None
        self.transaction_context = _Transaction()
        self.executions: list[tuple[str, object]] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def transaction(self) -> _Transaction:
        return self.transaction_context

    def execute(self, statement: str, parameters: object = None) -> None:
        self.executions.append((" ".join(statement.split()), parameters))


class _QueryResult:
    def __init__(self, value: object) -> None:
        self.value = value

    def fetchone(self) -> dict[str, object] | None:
        if self.value is None or isinstance(self.value, dict):
            return self.value
        raise AssertionError("result does not contain one row")

    def fetchall(self) -> list[dict[str, object]]:
        if isinstance(self.value, list):
            return self.value
        raise AssertionError("result does not contain rows")


class _QueuedConnection:
    def __init__(self, responses: list[object]) -> None:
        self.responses = responses
        self.executions: list[str] = []

    def execute(self, statement: str, parameters: object = None) -> _QueryResult:
        del parameters
        self.executions.append(" ".join(statement.split()))
        if not self.responses:
            raise AssertionError(f"unexpected SQL: {statement}")
        return _QueryResult(self.responses.pop(0))


def _minimum_calendar() -> Phase1AScreeningCalendar:
    start = date(2022, 1, 2)
    end = date(2026, 7, 31)
    exclusions = frozenset(PHASE1A_EXCLUDED_SOURCE_DATES)
    source_dates: list[date] = []
    cursor = start
    while cursor < end and len(source_dates) < 739:
        if cursor not in exclusions:
            source_dates.append(cursor)
        cursor += timedelta(days=1)
    source_dates.append(end)
    return Phase1AScreeningCalendar(
        source_dates=tuple(source_dates),
        excluded_source_dates=PHASE1A_EXCLUDED_SOURCE_DATES,
        source_manifest_sha256="a" * 64,
        qc_manifest_sha256="b" * 64,
        source_record_count=746,
        qc_pass_record_count=740,
        qc_fail_record_count=6,
        qc_config_sha256="c" * 64,
        schema_fingerprint="d" * 64,
    )


def _prepared_fixture(directory: Path):
    data_root = directory / "data"
    (data_root / "mbp-10").mkdir(parents=True)
    manifests = data_root / "derived" / "manifests" / "phase1a_inputs"
    manifests.mkdir(parents=True)
    calendar = _minimum_calendar()
    split = build_phase1a_screening_split(calendar)
    calendar_path = manifests / "calendar.json"
    split_path = manifests / "split.json"
    calendar_path.write_bytes(calendar.canonical_json())
    split_path.write_bytes(split.canonical_json())
    code_snapshot = build_code_snapshot(ROOT, code_commit="1" * 40)
    published_snapshot = publish_code_snapshot(code_snapshot, data_root=data_root)
    prepared = prepare_phase1a_screening_registration(
        project_root=ROOT,
        data_root=data_root,
        calendar=calendar,
        split=split,
        calendar_artifact_path=calendar_path,
        split_artifact_path=split_path,
        code_snapshot_artifact_path=published_snapshot.path,
        code_commit="1" * 40,
        code_snapshot_sha256=published_snapshot.sha256,
        cost_input_manifest_sha256="f" * 64,
    )
    return prepared, calendar, split, calendar_path, split_path


def _prepared_code_revision(
    prepared: object,
    calendar: Phase1AScreeningCalendar,
    split: object,
    calendar_path: Path,
    split_path: Path,
    *,
    code_commit: str,
):
    snapshot = build_code_snapshot(ROOT, code_commit=code_commit)
    published = publish_code_snapshot(snapshot, data_root=prepared.data_root)
    return prepare_phase1a_screening_registration(
        project_root=ROOT,
        data_root=prepared.data_root,
        calendar=calendar,
        split=split,
        calendar_artifact_path=calendar_path,
        split_artifact_path=split_path,
        code_snapshot_artifact_path=published.path,
        code_commit=code_commit,
        code_snapshot_sha256=published.sha256,
        cost_input_manifest_sha256="f" * 64,
    )


class ScreeningRegistryPreparationTests(unittest.TestCase):
    def test_preparation_is_deterministic_complete_and_does_not_publish(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            prepared, calendar, split, calendar_path, split_path = _prepared_fixture(directory)
            second = prepare_phase1a_screening_registration(
                project_root=ROOT,
                data_root=prepared.data_root,
                calendar=calendar,
                split=split,
                calendar_artifact_path=calendar_path,
                split_artifact_path=split_path,
                code_snapshot_artifact_path=prepared.code_snapshot_artifact.path,
                code_commit="1" * 40,
                code_snapshot_sha256=prepared.code_snapshot_sha256,
                cost_input_manifest_sha256="f" * 64,
            )

            self.assertEqual(prepared.registration_bytes, second.registration_bytes)
            self.assertEqual(prepared.registration_sha256, second.registration_sha256)
            self.assertEqual(
                prepared.registration_sha256,
                hashlib.sha256(prepared.registration_bytes).hexdigest(),
            )
            self.assertFalse(prepared.control_artifact_directory.exists())
            self.assertEqual(prepared.dataset_key, DEFAULT_DATASET_KEY)
            self.assertEqual(prepared.campaign_document["status"], "DRAFT")
            self.assertEqual(len(prepared.split_specs), 9)
            self.assertEqual(prepared.split_specs[0].result_visibility, "VISIBLE")
            self.assertTrue(
                all(spec.result_visibility == "SEALED" for spec in prepared.split_specs[1:])
            )
            self.assertEqual(len(prepared.day_specs), 746)
            self.assertEqual(
                sum(day.eligibility_status == "INELIGIBLE" for day in prepared.day_specs),
                6,
            )
            self.assertTrue(
                all(
                    day.split_key is not None
                    for day in prepared.day_specs
                    if day.eligibility_status == "ELIGIBLE"
                )
            )
            self.assertEqual(len(prepared.experiment_specs), 60)
            first_experiment = prepared.experiment_specs[0]
            self.assertEqual(
                first_experiment.search_boundary["take_profit_ticks"],
                list(EXPECTED_BARRIER_TICKS),
            )
            self.assertEqual(
                first_experiment.search_boundary["stop_loss_ticks"],
                list(EXPECTED_BARRIER_TICKS),
            )
            self.assertEqual(first_experiment.search_boundary["barrier_cell_count"], 484)
            self.assertEqual(
                first_experiment.feature_definition_versions["features_1s"],
                "phase1a_mbp10_screening_v1",
            )
            self.assertEqual(
                first_experiment.feature_definition_versions["research_5m"],
                "phase1a_mbp10_screening_v1",
            )
            self.assertEqual(
                first_experiment.cost_assumptions["input_manifest_sha256"],
                "f" * 64,
            )

            document = json.loads(prepared.registration_bytes)
            self.assertEqual(document["artifact_schema"], REGISTRATION_SCHEMA)
            self.assertEqual(
                document["code"]["snapshot_sha256"],
                prepared.code_snapshot_sha256,
            )
            self.assertEqual(
                prepared.code_snapshot_artifact.sha256,
                prepared.code_snapshot_sha256,
            )
            self.assertEqual(document["barrier_surface"]["cell_count"], 484)
            self.assertEqual(document["split_identity"]["split_sha256"], split.sha256)
            self.assertEqual(
                document["provenance_inputs"]["source_manifest_sha256"],
                calendar.source_manifest_sha256,
            )
            self.assertEqual(
                set(document["config_inputs"]),
                {
                    "barrier_grid",
                    "bundle",
                    "campaign",
                    "cost",
                    "execution",
                    "parent_hypotheses",
                },
            )
            self.assertFalse(document["split_identity"]["sealed_boundaries_embedded"])
            self.assertNotIn(
                split.sealed_holdout[0].isoformat(),
                prepared.registration_bytes.decode("utf-8"),
            )

    def test_input_artifact_or_split_identity_drift_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            prepared, calendar, split, calendar_path, split_path = _prepared_fixture(directory)
            split_path.write_bytes(split.canonical_json() + b"\n")
            with self.assertRaisesRegex(
                ScreeningRegistryDriftError,
                "artifact bytes differ",
            ):
                prepare_phase1a_screening_registration(
                    project_root=ROOT,
                    data_root=prepared.data_root,
                    calendar=calendar,
                    split=split,
                    calendar_artifact_path=calendar_path,
                    split_artifact_path=split_path,
                    code_snapshot_artifact_path=prepared.code_snapshot_artifact.path,
                    code_commit="1" * 40,
                    code_snapshot_sha256=prepared.code_snapshot_sha256,
                    cost_input_manifest_sha256="f" * 64,
                )

            split_path.write_bytes(split.canonical_json())
            wrong_calendar_split = replace(split, calendar_sha256="0" * 64)
            with self.assertRaisesRegex(
                ScreeningRegistryDriftError,
                "different calendar",
            ):
                prepare_phase1a_screening_registration(
                    project_root=ROOT,
                    data_root=prepared.data_root,
                    calendar=calendar,
                    split=wrong_calendar_split,
                    calendar_artifact_path=calendar_path,
                    split_artifact_path=split_path,
                    code_snapshot_artifact_path=prepared.code_snapshot_artifact.path,
                    code_commit="1" * 40,
                    code_snapshot_sha256=prepared.code_snapshot_sha256,
                    cost_input_manifest_sha256="f" * 64,
                )

    def test_code_snapshot_embedded_bytes_are_verified(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            prepared, calendar, split, calendar_path, split_path = _prepared_fixture(
                Path(temporary)
            )
            document = json.loads(prepared.code_snapshot_artifact.path.read_bytes())
            document["files"][0]["content_base64"] = "AA=="
            corrupted_bytes = canonical_json_bytes(document)
            corrupted_sha256 = hashlib.sha256(corrupted_bytes).hexdigest()
            corrupted_path = (
                prepared.code_snapshot_artifact.path.parent / f"sha256={corrupted_sha256}.json"
            )
            corrupted_path.write_bytes(corrupted_bytes)

            with self.assertRaisesRegex(
                ScreeningRegistryDriftError,
                "bytes differ from its identity",
            ):
                prepare_phase1a_screening_registration(
                    project_root=ROOT,
                    data_root=prepared.data_root,
                    calendar=calendar,
                    split=split,
                    calendar_artifact_path=calendar_path,
                    split_artifact_path=split_path,
                    code_snapshot_artifact_path=corrupted_path,
                    code_commit="1" * 40,
                    code_snapshot_sha256=corrupted_sha256,
                    cost_input_manifest_sha256="f" * 64,
                )

    def test_control_artifact_is_exactly_idempotent_and_drift_rejecting(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            prepared, *_ = _prepared_fixture(Path(temporary))

            first_path, first_created = _publish_control_artifact(prepared)
            second_path, second_created = _publish_control_artifact(prepared)

            expected_parent = prepared.data_root.joinpath(*CONTROL_ARTIFACT_SUBDIRECTORY.parts)
            self.assertTrue(first_created)
            self.assertFalse(second_created)
            self.assertEqual(first_path, second_path)
            self.assertEqual(first_path.parent, expected_parent)
            self.assertEqual(first_path.read_bytes(), prepared.registration_bytes)
            self.assertEqual(first_path.stem, prepared.registration_sha256)

            first_path.write_bytes(b"immutable drift")
            with self.assertRaisesRegex(ScreeningRegistryDriftError, "content drift"):
                _publish_control_artifact(prepared)

    def test_held_artifact_rejects_path_inode_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            path = directory / "artifact.json"
            content = b'{"identity":"original"}'
            path.write_bytes(content)
            held = _open_verified_artifact_file(
                path,
                expected_sha256=hashlib.sha256(content).hexdigest(),
                expected_byte_size=len(content),
                label="test artifact",
            )
            try:
                replacement = directory / "replacement.json"
                replacement.write_bytes(content)
                os.replace(replacement, path)
                with self.assertRaisesRegex(
                    ScreeningRegistryDriftError,
                    "path or inode changed",
                ):
                    _verify_held_artifact_binding(held, label="test artifact")
            finally:
                os.close(held.descriptor)

    def test_code_only_revision_matches_baseline_invariant_projection(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            prepared, calendar, split, calendar_path, split_path = _prepared_fixture(
                Path(temporary)
            )
            revision = _prepared_code_revision(
                prepared,
                calendar,
                split,
                calendar_path,
                split_path,
                code_commit="2" * 40,
            )

            self.assertNotEqual(prepared.code_commit, revision.code_commit)
            self.assertNotEqual(
                prepared.code_snapshot_sha256,
                revision.code_snapshot_sha256,
            )
            self.assertNotEqual(
                prepared.registration_sha256,
                revision.registration_sha256,
            )
            _assert_registration_revision_invariants(
                prepared.registration_document,
                revision.registration_document,
            )

            drift_cases = {
                "config": ("config_inputs", "cost", "0" * 64),
                "cost": ("cost_assumptions", "variable_cost_ticks", 999),
                "calendar": ("calendar_identity", "calendar_sha256", "1" * 64),
                "split": ("split_identity", "split_sha256", "2" * 64),
                "policy": (
                    "registration_policy",
                    "serializable_transaction_required",
                    False,
                ),
            }
            for label, (section, field, value) in drift_cases.items():
                with self.subTest(label=label):
                    drifted = deepcopy(revision.registration_document)
                    drifted[section][field] = value
                    with self.assertRaisesRegex(
                        ScreeningRegistryDriftError,
                        "non-code research invariants",
                    ):
                        _assert_registration_revision_invariants(
                            prepared.registration_document,
                            drifted,
                        )

    def test_baseline_control_is_held_rehashed_and_campaign_identity_is_preserved(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            prepared, calendar, split, calendar_path, split_path = _prepared_fixture(
                Path(temporary)
            )
            baseline_path, _ = _publish_control_artifact(prepared)
            revision = _prepared_code_revision(
                prepared,
                calendar,
                split,
                calendar_path,
                split_path,
                code_commit="2" * 40,
            )
            artifact_id = 71
            metadata = {
                "artifact_schema": REGISTRATION_SCHEMA,
                "calendar_artifact_id": 3,
                "campaign_key": "phase1a_conservative_screening_v1",
                "code_snapshot_artifact_id": 5,
                "holdout_boundaries_embedded": False,
                "result_visibility": "SEALED",
                "split_artifact_id": 4,
            }
            artifact_row = {
                "artifact_id": artifact_id,
                "artifact_key": (f"phase1a-screening-registry:{prepared.registration_sha256}"),
                "artifact_type": "PHASE1A_SCREENING_REGISTRY",
                "uri": baseline_path.as_uri(),
                "sha256": prepared.registration_sha256,
                "byte_size": len(prepared.registration_bytes),
                "media_type": "application/json",
                "producer_job_id": None,
                "metadata": metadata,
            }
            baseline_connection = _QueuedConnection(
                [
                    {
                        "campaign_id": 2,
                        "code_commit": prepared.code_commit,
                        "config_sha256": prepared.registration_sha256,
                    },
                    [artifact_row],
                ]
            )

            held_artifacts = {}
            with ExitStack() as artifact_stack:
                baseline = _load_registration_baseline(
                    baseline_connection,
                    revision,
                    artifact_stack=artifact_stack,
                    held_artifacts=held_artifacts,
                )

                self.assertIsNotNone(baseline)
                assert baseline is not None
                self.assertEqual(baseline.control_artifact_id, artifact_id)
                self.assertEqual(baseline.code_commit, prepared.code_commit)
                self.assertEqual(
                    canonical_json_bytes(baseline.control_document),
                    canonical_json_bytes(prepared.registration_document),
                )
                self.assertEqual(baseline_connection.responses, [])
                baseline_held = held_artifacts[baseline_path]
                os.fstat(baseline_held.descriptor)
                _verify_held_artifact_binding(
                    baseline_held,
                    label="baseline control through transaction",
                )
            with self.assertRaises(OSError):
                os.fstat(baseline_held.descriptor)

            spec = revision.campaign_document
            campaign_row = {
                "campaign_id": 2,
                "campaign_key": "phase1a_conservative_screening_v1",
                "dataset_id": 1,
                "name": spec["name"],
                "status": "DRAFT",
                "selected_start_date": spec["selected_start_date"],
                "selected_end_date": spec["selected_end_date"],
                "roll_cutoff_date": None,
                "data_manifest_sha256": spec["data_manifest_sha256"],
                "feature_version": spec["feature_version"],
                "outcome_version": spec["outcome_version"],
                "cost_model_version": spec["cost_model_version"],
                "execution_model_version": spec["execution_model_version"],
                "code_commit": prepared.code_commit,
                "config_sha256": prepared.registration_sha256,
                "split_policy": spec["split_policy"],
                "trial_budget": spec["trial_budget"],
                "finalist_budget": spec["finalist_budget"],
            }
            campaign_connection = _QueuedConnection([None, campaign_row])

            campaign_id, created = _ensure_campaign(
                campaign_connection,
                revision,
                1,
                baseline=baseline,
            )

            self.assertEqual(campaign_id, 2)
            self.assertFalse(created)
            self.assertFalse(
                any(statement.startswith("UPDATE") for statement in campaign_connection.executions)
            )

            missing_experiment = _QueuedConnection([{"experiment_id": 999}])
            with self.assertRaisesRegex(
                ScreeningRegistryDriftError,
                "cannot create a missing baseline experiment",
            ):
                _ensure_experiments(
                    missing_experiment,
                    prepared=revision,
                    campaign_id=2,
                    registration_artifact_id=999,
                    baseline=baseline,
                )

    def test_code_revision_rejects_repairing_missing_baseline_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            prepared, *_ = _prepared_fixture(Path(temporary))

            held = _open_verified_artifact_file(
                prepared.calendar_artifact.path,
                expected_sha256=prepared.calendar_artifact.sha256,
                expected_byte_size=prepared.calendar_artifact.byte_size,
                label="calendar artifact",
            )
            try:
                missing_artifact = _QueuedConnection([{"artifact_id": 901}])
                with self.assertRaisesRegex(
                    ScreeningRegistryDriftError,
                    "cannot create missing baseline artifact",
                ):
                    _ensure_artifact(
                        missing_artifact,
                        artifact_key=f"phase1a-calendar:{prepared.calendar.sha256}",
                        artifact_type="PHASE1A_ELIGIBLE_CALENDAR",
                        path=prepared.calendar_artifact.path,
                        sha256=prepared.calendar_artifact.sha256,
                        byte_size=prepared.calendar_artifact.byte_size,
                        metadata={},
                        held_file=held,
                        allow_create=False,
                    )
                self.assertEqual(missing_artifact.responses, [])
            finally:
                os.close(held.descriptor)

            missing_split = _QueuedConnection([{"campaign_split_id": 902}])
            with self.assertRaisesRegex(
                ScreeningRegistryDriftError,
                "cannot create a missing baseline campaign split",
            ):
                _ensure_splits(
                    missing_split,
                    campaign_id=2,
                    specs=prepared.split_specs[:1],
                    allow_create=False,
                )
            self.assertEqual(missing_split.responses, [])

            split_spec = prepared.split_specs[0]
            extra_split = _QueuedConnection(
                [
                    None,
                    {
                        "campaign_split_id": 11,
                        "campaign_id": 2,
                        "split_key": split_spec.split_key,
                        "split_role": split_spec.split_role,
                        "fold_number": split_spec.fold_number,
                        "start_date": split_spec.start_date,
                        "end_date": split_spec.end_date,
                        "start_active_ordinal": split_spec.start_active_ordinal,
                        "end_active_ordinal": split_spec.end_active_ordinal,
                        "purge_before_days": split_spec.purge_before_days,
                        "purge_after_days": split_spec.purge_after_days,
                        "result_visibility": split_spec.result_visibility,
                        "revealed_at": None,
                    },
                    [
                        {"split_key": split_spec.split_key},
                        {"split_key": "unexpected-extra-split"},
                    ],
                ]
            )
            with self.assertRaisesRegex(
                ScreeningRegistryDriftError,
                "split key set differs",
            ):
                _ensure_splits(
                    extra_split,
                    campaign_id=2,
                    specs=prepared.split_specs[:1],
                    allow_create=False,
                )
            self.assertEqual(extra_split.responses, [])

            source_ids = {
                spec.calendar_date: index for index, spec in enumerate(prepared.day_specs, start=1)
            }
            split_ids = {
                spec.split_key: index for index, spec in enumerate(prepared.split_specs, start=1)
            }
            missing_days = _QueuedConnection([[{"campaign_day_id": 903}]])
            with self.assertRaisesRegex(
                ScreeningRegistryDriftError,
                "cannot create missing baseline campaign days",
            ):
                _ensure_days(
                    missing_days,
                    prepared=prepared,
                    dataset_id=1,
                    campaign_id=2,
                    source_ids=source_ids,
                    split_ids=split_ids,
                    allow_create=False,
                )
            self.assertEqual(missing_days.responses, [])

    def test_field_verification_reports_every_immutable_mismatch(self) -> None:
        with self.assertRaisesRegex(
            ScreeningRegistryDriftError,
            "alpha, beta",
        ):
            _assert_fields(
                label="test row",
                row={"alpha": 1, "beta": 2},
                expected={"alpha": 2, "beta": 3},
            )


class ScreeningRegistryTransactionTests(unittest.TestCase):
    def test_public_entrypoint_uses_serializable_atomic_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            prepared, calendar, split, calendar_path, split_path = _prepared_fixture(
                Path(temporary)
            )
            control_path, _ = _publish_control_artifact(prepared)
            registered = _DatabaseRegistration(
                dataset_id=1,
                campaign_id=2,
                calendar_artifact_id=3,
                split_artifact_id=4,
                code_snapshot_artifact_id=5,
                control_artifact_id=6,
                split_ids=tuple(range(10, 19)),
                experiment_ids=tuple(range(100, 160)),
                created_campaign=True,
                created_artifacts=4,
                created_splits=9,
                created_days=746,
                created_experiments=60,
            )
            connection = _Connection()

            with (
                patch(
                    "systematic_fx.db.screening_registry.prepare_phase1a_screening_registration",
                    return_value=prepared,
                ),
                patch(
                    "systematic_fx.db.screening_registry._publish_control_artifact",
                    return_value=(control_path, True),
                ),
                patch(
                    "systematic_fx.db.screening_registry._register_prepared",
                    return_value=registered,
                ) as register_prepared,
                patch(
                    "systematic_fx.db.screening_registry.psycopg.connect",
                    return_value=connection,
                ),
            ):
                report = register_phase1a_screening_campaign(
                    "postgresql:///mock-only",
                    project_root=ROOT,
                    data_root=prepared.data_root,
                    calendar=calendar,
                    split=split,
                    calendar_artifact_path=calendar_path,
                    split_artifact_path=split_path,
                    code_snapshot_artifact_path=prepared.code_snapshot_artifact.path,
                    code_commit="1" * 40,
                    code_snapshot_sha256=prepared.code_snapshot_sha256,
                    cost_input_manifest_sha256="f" * 64,
                )

            self.assertEqual(connection.isolation_level, IsolationLevel.SERIALIZABLE)
            self.assertTrue(connection.transaction_context.entered)
            self.assertTrue(connection.transaction_context.exited)
            self.assertEqual(len(connection.executions), 2)
            self.assertTrue(
                all("pg_advisory_xact_lock" in statement for statement, _ in connection.executions)
            )
            register_prepared.assert_called_once_with(
                connection,
                prepared=prepared,
                control_artifact_path=control_path,
                held_artifacts=ANY,
                artifact_stack=ANY,
            )
            self.assertEqual(report.campaign_key, "phase1a_conservative_screening_v1")
            self.assertEqual(report.code_snapshot_artifact_id, 5)
            self.assertEqual(report.created_artifacts, 4)
            self.assertEqual(len(report.experiment_ids), 60)
            self.assertTrue(report.created_control_artifact)

    def test_driver_error_is_wrapped_without_real_database_access(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            prepared, calendar, split, calendar_path, split_path = _prepared_fixture(
                Path(temporary)
            )
            control_path, _ = _publish_control_artifact(prepared)
            with (
                patch(
                    "systematic_fx.db.screening_registry.prepare_phase1a_screening_registration",
                    return_value=prepared,
                ),
                patch(
                    "systematic_fx.db.screening_registry._publish_control_artifact",
                    return_value=(control_path, False),
                ),
                patch(
                    "systematic_fx.db.screening_registry.psycopg.connect",
                    side_effect=psycopg.OperationalError("unavailable"),
                ),
                self.assertRaisesRegex(
                    ScreeningRegistryDatabaseError,
                    "PostgreSQL Phase 1A screening registration failed",
                ) as raised,
            ):
                register_phase1a_screening_campaign(
                    "postgresql:///mock-only",
                    project_root=ROOT,
                    data_root=prepared.data_root,
                    calendar=calendar,
                    split=split,
                    calendar_artifact_path=calendar_path,
                    split_artifact_path=split_path,
                    code_snapshot_artifact_path=prepared.code_snapshot_artifact.path,
                    code_commit="1" * 40,
                    code_snapshot_sha256=prepared.code_snapshot_sha256,
                    cost_input_manifest_sha256="f" * 64,
                )

            self.assertIsInstance(raised.exception.__cause__, psycopg.OperationalError)

    def test_serialization_conflicts_retry_the_entire_public_registration(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            prepared, calendar, split, calendar_path, split_path = _prepared_fixture(
                Path(temporary)
            )
            control_path, _ = _publish_control_artifact(prepared)
            registered = _DatabaseRegistration(
                dataset_id=1,
                campaign_id=2,
                calendar_artifact_id=3,
                split_artifact_id=4,
                code_snapshot_artifact_id=5,
                control_artifact_id=6,
                split_ids=tuple(range(10, 19)),
                experiment_ids=tuple(range(100, 160)),
                created_campaign=False,
                created_artifacts=0,
                created_splits=0,
                created_days=0,
                created_experiments=0,
            )
            connection = _Connection()
            conflicts = [
                psycopg.errors.SerializationFailure("concurrent outbox update") for _ in range(3)
            ]
            with (
                patch(
                    "systematic_fx.db.screening_registry.prepare_phase1a_screening_registration",
                    return_value=prepared,
                ) as prepare_registration,
                patch(
                    "systematic_fx.db.screening_registry._publish_control_artifact",
                    return_value=(control_path, False),
                ) as publish_control,
                patch(
                    "systematic_fx.db.screening_registry._open_verified_artifact_file",
                    wraps=_open_verified_artifact_file,
                ) as open_artifact,
                patch(
                    "systematic_fx.db.screening_registry._register_prepared",
                    return_value=registered,
                ) as register_prepared,
                patch(
                    "systematic_fx.db.screening_registry.psycopg.connect",
                    side_effect=[*conflicts, connection],
                ) as connect,
                patch("systematic_fx.db.postgres_retry.time.sleep") as sleep,
            ):
                report = register_phase1a_screening_campaign(
                    "postgresql:///mock-only",
                    project_root=ROOT,
                    data_root=prepared.data_root,
                    calendar=calendar,
                    split=split,
                    calendar_artifact_path=calendar_path,
                    split_artifact_path=split_path,
                    code_snapshot_artifact_path=prepared.code_snapshot_artifact.path,
                    code_commit="1" * 40,
                    code_snapshot_sha256=prepared.code_snapshot_sha256,
                    cost_input_manifest_sha256="f" * 64,
                )

            self.assertEqual(report.campaign_id, 2)
            self.assertEqual(prepare_registration.call_count, 4)
            self.assertEqual(publish_control.call_count, 4)
            self.assertEqual(open_artifact.call_count, 16)
            self.assertEqual(connect.call_count, 4)
            register_prepared.assert_called_once()
            sleep.assert_has_calls([call(0.01), call(0.05), call(0.2)])


if __name__ == "__main__":
    unittest.main()
