from __future__ import annotations

import json
import unittest
from collections.abc import Iterable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Self
from unittest import mock

import psycopg
from psycopg import IsolationLevel

from systematic_fx.db import run_registry
from systematic_fx.db.run_registry import (
    RunRegistryDatabaseError,
    RunRegistryDriftError,
    RunRegistryError,
    RunRegistryStateError,
    finish_run_attempt,
    register_run_spec,
    reserve_run_attempt,
    start_run_attempt,
)
from systematic_fx.research.run_spec import (
    RUN_SPEC_SCHEMA,
    RUN_SPEC_SCHEMA_VERSION,
    RunSpec,
)


def _run_spec(**overrides: object) -> RunSpec:
    values: dict[str, object] = {
        "campaign_id": "phase1_discovery_v1",
        "experiment_id": "phase1_discovery_v1:experiment:p1_01:v1",
        "run_kind": "BACKTEST",
        "engine_version": "event_engine_v1",
        "source_manifest_hashes": {"mbp10": "a" * 64},
        "eligible_calendar_version": "calendar_v1",
        "eligible_calendar_sha256": "b" * 64,
        "split_version": "split_v1",
        "split_sha256": "c" * 64,
        "feature_version": "features_v1",
        "feature_sha256": "d" * 64,
        "outcome_version": "outcomes_v1",
        "outcome_sha256": "e" * 64,
        "cost_version": "cost_v1",
        "cost_sha256": "f" * 64,
        "execution_version": "execution_v1",
        "execution_sha256": "0" * 64,
        "code_commit": "1" * 40,
        "code_snapshot_sha256": "3" * 64,
        "dependency_lock_sha256": "2" * 64,
        "runtime_environment": {"python": "3.12.13", "platform": "test"},
        "random_seed": 2**64 - 1,
        "direction": "LONG",
        "signal_policy": {"rule": "imbalance"},
        "entry_policy": {"order_type": "MARKET"},
        "barrier_policy": {"take_profit_ticks": 24, "stop_ticks": 16},
        "terminal_policy": {"rule": "roll_cutoff"},
        "parameters": {"lookback_bars": 12, "threshold": "0.75"},
    }
    values.update(overrides)
    return RunSpec(**values)  # type: ignore[arg-type]


class _Result:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self.row = row

    def fetchone(self) -> dict[str, Any] | None:
        return self.row


class _Transaction:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    def __enter__(self) -> Self:
        self.connection.transaction_entries += 1
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Connection:
    def __init__(self, responses: Iterable[dict[str, Any] | None | BaseException]) -> None:
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
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return _Result(response)


def _spec_row(
    spec: RunSpec,
    *,
    research_run_spec_id: int = 31,
    campaign_id: int = 11,
    experiment_id: int | None = 22,
    parent_run_spec_id: int | None = None,
) -> dict[str, object]:
    return {
        "research_run_spec_id": research_run_spec_id,
        "run_fingerprint": spec.fingerprint,
        "canonicalization_schema": RUN_SPEC_SCHEMA,
        "canonicalization_version": RUN_SPEC_SCHEMA_VERSION,
        "campaign_id": campaign_id,
        "experiment_id": experiment_id,
        "parent_run_spec_id": parent_run_spec_id,
        "run_kind": spec.run_kind,
        "engine_version": spec.engine_version,
        "canonical_spec": json.loads(spec.canonical_json()),
        "source_manifest_hashes": dict(spec.source_manifest_hashes),
        "eligible_calendar_version": spec.eligible_calendar_version,
        "eligible_calendar_sha256": spec.eligible_calendar_sha256,
        "split_version": spec.split_version,
        "split_sha256": spec.split_sha256,
        "feature_version": spec.feature_version,
        "feature_sha256": spec.feature_sha256,
        "outcome_version": spec.outcome_version,
        "outcome_sha256": spec.outcome_sha256,
        "cost_version": spec.cost_version,
        "cost_sha256": spec.cost_sha256,
        "execution_version": spec.execution_version,
        "execution_sha256": spec.execution_sha256,
        "code_commit": spec.code_commit,
        "code_snapshot_sha256": spec.code_snapshot_sha256,
        "dependency_lock_sha256": spec.dependency_lock_sha256,
        "deterministic_seed": Decimal(spec.random_seed),
        "direction": spec.direction,
    }


def _attempt_row(
    *,
    attempt_id: int = 41,
    spec_id: int = 31,
    number: int = 1,
    status: str,
    reused_attempt_id: int | None = None,
    result_artifact_id: int | None = None,
    trade_ledger_artifact_id: int | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> dict[str, object]:
    return {
        "research_run_attempt_id": attempt_id,
        "research_run_spec_id": spec_id,
        "attempt_number": number,
        "status": status,
        "reused_attempt_id": reused_attempt_id,
        "result_artifact_id": result_artifact_id,
        "trade_ledger_artifact_id": trade_ledger_artifact_id,
        "started_at": started_at,
        "finished_at": finished_at,
    }


class RunSpecRegistrationTests(unittest.TestCase):
    def _connection(
        self,
        spec: RunSpec,
        *,
        inserted: bool,
        stored: dict[str, object] | None = None,
    ) -> _Connection:
        return _Connection(
            [
                {"campaign_id": 11, "campaign_key": spec.campaign_id},
                {
                    "experiment_id": 22,
                    "experiment_key": spec.experiment_id,
                    "campaign_id": 11,
                },
                {"research_run_spec_id": 31} if inserted else None,
                stored or _spec_row(spec),
            ]
        )

    def test_registers_canonical_spec_and_uint64_seed_atomically(self) -> None:
        spec = _run_spec()
        connection = self._connection(spec, inserted=True)

        with mock.patch.object(run_registry.psycopg, "connect", return_value=connection):
            result = register_run_spec("postgresql:///test", spec)

        self.assertTrue(result.created)
        self.assertEqual(result.research_run_spec_id, 31)
        self.assertEqual(result.run_fingerprint, spec.fingerprint)
        self.assertEqual(connection.isolation_level, IsolationLevel.SERIALIZABLE)
        self.assertEqual(connection.transaction_entries, 1)
        self.assertIn("FOR SHARE", connection.calls[0][0])
        self.assertIn("ON CONFLICT (run_fingerprint) DO NOTHING", connection.calls[2][0])
        insert_parameters = connection.calls[2][1]
        self.assertIsInstance(insert_parameters, tuple)
        self.assertIn(Decimal(2**64 - 1), insert_parameters)
        self.assertFalse(connection.responses)

    def test_exact_fingerprint_is_reused_but_field_drift_is_rejected(self) -> None:
        spec = _run_spec()
        exact = self._connection(spec, inserted=False)
        with mock.patch.object(run_registry.psycopg, "connect", return_value=exact):
            result = register_run_spec("postgresql:///test", spec)
        self.assertFalse(result.created)
        self.assertEqual(result.research_run_spec_id, 31)

        drifted_row = _spec_row(spec)
        drifted_row["engine_version"] = "other-engine"
        drifted = self._connection(spec, inserted=False, stored=drifted_row)
        with (
            mock.patch.object(run_registry.psycopg, "connect", return_value=drifted),
            self.assertRaisesRegex(RunRegistryDriftError, "engine_version"),
        ):
            register_run_spec("postgresql:///test", spec)

    def test_parent_fingerprint_is_resolved_inside_the_same_campaign(self) -> None:
        parent_fingerprint = "3" * 64
        spec = _run_spec(
            parameters={
                "lookback_bars": 12,
                "parent_run_fingerprint": parent_fingerprint,
                "threshold": "0.75",
            }
        )
        stored = _spec_row(spec, parent_run_spec_id=77)
        connection = _Connection(
            [
                {"campaign_id": 11, "campaign_key": spec.campaign_id},
                {
                    "experiment_id": 22,
                    "experiment_key": spec.experiment_id,
                    "campaign_id": 11,
                },
                {"research_run_spec_id": 77, "campaign_id": 11},
                {"research_run_spec_id": 31},
                stored,
            ]
        )

        with mock.patch.object(run_registry.psycopg, "connect", return_value=connection):
            result = register_run_spec(
                "postgresql:///test",
                spec,
                parent_run_fingerprint=parent_fingerprint,
            )

        self.assertEqual(result.parent_run_spec_id, 77)
        self.assertIn("FOR SHARE", connection.calls[2][0])

    def test_parent_database_edge_must_be_inside_canonical_parameters(self) -> None:
        parent_fingerprint = "3" * 64
        spec = _run_spec()
        with self.assertRaisesRegex(RunRegistryError, "parent_run_fingerprint"):
            register_run_spec(
                "postgresql:///test",
                spec,
                parent_run_fingerprint=parent_fingerprint,
            )

        orphaned_parameter = _run_spec(parameters={"parent_run_fingerprint": parent_fingerprint})
        with self.assertRaisesRegex(RunRegistryError, "no database parent"):
            register_run_spec("postgresql:///test", orphaned_parameter)

    def test_campaign_level_spec_does_not_invent_an_experiment_owner(self) -> None:
        spec = _run_spec(run_kind="FEATURE_BUILD", experiment_id=None)
        connection = _Connection(
            [
                {"campaign_id": 11, "campaign_key": spec.campaign_id},
                {"research_run_spec_id": 31},
                _spec_row(spec, experiment_id=None),
            ]
        )

        with mock.patch.object(run_registry.psycopg, "connect", return_value=connection):
            result = register_run_spec("postgresql:///test", spec)

        self.assertIsNone(result.experiment_id)
        self.assertEqual(len(connection.calls), 3)
        self.assertIn("INSERT INTO systematic_fx.research_run_specs", connection.calls[1][0])


class RunAttemptReservationTests(unittest.TestCase):
    def test_reserves_the_next_queued_attempt_when_no_success_exists(self) -> None:
        fingerprint = "a" * 64
        queued = _attempt_row(attempt_id=43, number=3, status="QUEUED")
        connection = _Connection(
            [
                {"research_run_spec_id": 31, "run_fingerprint": fingerprint},
                None,
                None,
                {"last_attempt_number": 2},
                queued,
            ]
        )

        with mock.patch.object(run_registry.psycopg, "connect", return_value=connection):
            result = reserve_run_attempt(
                "postgresql:///test",
                run_fingerprint=fingerprint,
                job_id=9,
            )

        self.assertTrue(result.execute)
        self.assertEqual(result.status, "QUEUED")
        self.assertEqual(result.attempt_number, 3)
        self.assertIsNone(result.reused_attempt_id)
        self.assertEqual(connection.isolation_level, IsolationLevel.SERIALIZABLE)
        self.assertIn("FOR UPDATE", connection.calls[0][0])

    def test_successful_fingerprint_appends_a_skipped_duplicate(self) -> None:
        fingerprint = "b" * 64
        skipped = _attempt_row(
            attempt_id=44,
            number=2,
            status="SKIPPED_DUPLICATE",
            reused_attempt_id=41,
        )
        connection = _Connection(
            [
                {"research_run_spec_id": 31, "run_fingerprint": fingerprint},
                None,
                {
                    "research_run_attempt_id": 41,
                    "attempt_number": 1,
                    "result_artifact_id": 90,
                },
                {"last_attempt_number": 1},
                skipped,
            ]
        )

        with mock.patch.object(run_registry.psycopg, "connect", return_value=connection):
            result = reserve_run_attempt(
                "postgresql:///test",
                run_fingerprint=fingerprint,
            )

        self.assertFalse(result.execute)
        self.assertEqual(result.status, "SKIPPED_DUPLICATE")
        self.assertEqual(result.reused_attempt_id, 41)
        self.assertIn("status = 'SUCCEEDED'", connection.calls[2][0])
        self.assertIn("SKIPPED_DUPLICATE", connection.calls[4][0])

    def test_active_attempt_blocks_a_second_execution_reservation(self) -> None:
        fingerprint = "c" * 64
        for status in ("QUEUED", "RUNNING"):
            with self.subTest(status=status):
                connection = _Connection(
                    [
                        {"research_run_spec_id": 31, "run_fingerprint": fingerprint},
                        _attempt_row(attempt_id=51, number=2, status=status),
                    ]
                )
                with (
                    mock.patch.object(
                        run_registry.psycopg,
                        "connect",
                        return_value=connection,
                    ),
                    self.assertRaisesRegex(RunRegistryStateError, "active attempt 51"),
                ):
                    reserve_run_attempt(
                        "postgresql:///test",
                        run_fingerprint=fingerprint,
                    )
                self.assertEqual(len(connection.calls), 2)
                self.assertIn("status IN ('QUEUED', 'RUNNING')", connection.calls[1][0])


class RunAttemptTransitionTests(unittest.TestCase):
    def test_queued_attempt_transitions_to_running(self) -> None:
        now = datetime(2026, 8, 3, tzinfo=UTC)
        connection = _Connection(
            [
                _attempt_row(status="QUEUED"),
                _attempt_row(status="RUNNING", started_at=now),
            ]
        )

        with mock.patch.object(run_registry.psycopg, "connect", return_value=connection):
            result = start_run_attempt(
                "postgresql:///test",
                research_run_attempt_id=41,
            )

        self.assertEqual(result.status, "RUNNING")
        self.assertIn("FOR UPDATE", connection.calls[0][0])
        self.assertIn("status = 'QUEUED'", connection.calls[1][0])

    def test_start_rejects_every_nonqueued_state(self) -> None:
        for status in (
            "RUNNING",
            "SUCCEEDED",
            "REJECTED",
            "FAILED",
            "CANCELLED",
            "SKIPPED_DUPLICATE",
        ):
            with self.subTest(status=status):
                connection = _Connection([_attempt_row(status=status)])
                with (
                    mock.patch.object(
                        run_registry.psycopg,
                        "connect",
                        return_value=connection,
                    ),
                    self.assertRaises(RunRegistryStateError),
                ):
                    start_run_attempt(
                        "postgresql:///test",
                        research_run_attempt_id=41,
                    )
                self.assertEqual(len(connection.calls), 1)

    def test_running_attempt_transitions_to_success_with_artifacts(self) -> None:
        started = datetime(2026, 8, 3, 1, tzinfo=UTC)
        finished = datetime(2026, 8, 3, 2, tzinfo=UTC)
        connection = _Connection(
            [
                _attempt_row(status="RUNNING", started_at=started),
                _attempt_row(
                    status="SUCCEEDED",
                    started_at=started,
                    finished_at=finished,
                    result_artifact_id=90,
                    trade_ledger_artifact_id=91,
                ),
            ]
        )

        with mock.patch.object(run_registry.psycopg, "connect", return_value=connection):
            result = finish_run_attempt(
                "postgresql:///test",
                research_run_attempt_id=41,
                status="SUCCEEDED",
                result_summary={"net_pnl_ticks": 12},
                result_artifact_id=90,
                trade_ledger_artifact_id=91,
            )

        self.assertEqual(result.status, "SUCCEEDED")
        self.assertEqual(result.result_artifact_id, 90)
        self.assertEqual(result.trade_ledger_artifact_id, 91)

    def test_terminal_preconditions_and_source_state_are_strict(self) -> None:
        with self.assertRaisesRegex(RunRegistryStateError, "result_artifact_id"):
            finish_run_attempt(
                "postgresql:///test",
                research_run_attempt_id=41,
                status="SUCCEEDED",
            )
        with self.assertRaisesRegex(RunRegistryStateError, "error_message"):
            finish_run_attempt(
                "postgresql:///test",
                research_run_attempt_id=41,
                status="FAILED",
            )

        connection = _Connection([_attempt_row(status="QUEUED")])
        with (
            mock.patch.object(run_registry.psycopg, "connect", return_value=connection),
            self.assertRaisesRegex(RunRegistryStateError, "QUEUED -> CANCELLED"),
        ):
            finish_run_attempt(
                "postgresql:///test",
                research_run_attempt_id=41,
                status="CANCELLED",
            )

    def test_driver_errors_are_translated(self) -> None:
        with (
            mock.patch.object(
                run_registry.psycopg,
                "connect",
                side_effect=psycopg.OperationalError("unavailable"),
            ),
            self.assertRaises(RunRegistryDatabaseError) as raised,
        ):
            reserve_run_attempt(
                "postgresql:///test",
                run_fingerprint="c" * 64,
            )
        self.assertIsInstance(raised.exception.__cause__, psycopg.OperationalError)


if __name__ == "__main__":
    unittest.main()
