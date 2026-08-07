from __future__ import annotations

import hashlib
import json
import unittest
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime
from pathlib import Path

from systematic_fx.research.run_spec import RUN_SPEC_SCHEMA, RunSpec, RunSpecError


def _run_spec(**overrides: object) -> RunSpec:
    values: dict[str, object] = {
        "campaign_id": "campaign-2026-08",
        "experiment_id": "experiment-001",
        "run_kind": "BARRIER_SURFACE",
        "engine_version": "event-replay-v1",
        "source_manifest_hashes": {
            "full_content": "1" * 64,
            "footer": "2" * 64,
        },
        "eligible_calendar_version": "eligible-calendar-v1",
        "eligible_calendar_sha256": "3" * 64,
        "split_version": "purged-walk-forward-v2",
        "split_sha256": "4" * 64,
        "feature_version": "mbp10-features-v3",
        "feature_sha256": "5" * 64,
        "outcome_version": "first-touch-v2",
        "outcome_sha256": "6" * 64,
        "cost_version": "cost-model-v4",
        "cost_sha256": "7" * 64,
        "execution_version": "execution-model-v2",
        "execution_sha256": "8" * 64,
        "code_commit": "0123456789abcdef0123456789abcdef01234567",
        "code_snapshot_sha256": "a" * 64,
        "dependency_lock_sha256": "9" * 64,
        "runtime_environment": {
            "python": "3.12.13",
            "platform": "darwin-arm64",
            "postgresql": "18.4",
        },
        "random_seed": 42,
        "direction": "BOTH",
        "signal_policy": {
            "cadence_seconds": 300,
            "rule": {
                "features": ["imbalance", "spread"],
                "threshold": "0.75",
            },
        },
        "entry_policy": {
            "expiry_ms": 250,
            "order_type": "MARKETABLE_LIMIT",
        },
        "barrier_policy": {
            "loss_ticks": 16,
            "profit_ticks": 24,
            "tie_break": "STOP_FIRST",
        },
        "terminal_policy": {
            "max_holding_events": 1_000,
            "roll_exit": True,
        },
        "parameters": {
            "enabled": True,
            "model": {"alpha": "0.01", "layers": [1, 2, 3]},
        },
    }
    values.update(overrides)
    return RunSpec(**values)  # type: ignore[arg-type]


class RunSpecTests(unittest.TestCase):
    def test_canonical_json_and_hash_ignore_mapping_key_order(self) -> None:
        first = _run_spec()
        reordered = _run_spec(
            source_manifest_hashes={
                "footer": "2" * 64,
                "full_content": "1" * 64,
            },
            signal_policy={
                "rule": {
                    "threshold": "0.75",
                    "features": ["imbalance", "spread"],
                },
                "cadence_seconds": 300,
            },
            entry_policy={
                "order_type": "MARKETABLE_LIMIT",
                "expiry_ms": 250,
            },
            barrier_policy={
                "tie_break": "STOP_FIRST",
                "profit_ticks": 24,
                "loss_ticks": 16,
            },
            terminal_policy={"roll_exit": True, "max_holding_events": 1_000},
            parameters={
                "model": {"layers": [1, 2, 3], "alpha": "0.01"},
                "enabled": True,
            },
        )

        self.assertEqual(first.canonical_json(), reordered.canonical_json())
        self.assertEqual(first.fingerprint, reordered.fingerprint)
        self.assertEqual(
            first.fingerprint,
            hashlib.sha256(first.canonical_json()).hexdigest(),
        )

        payload = json.loads(first.canonical_json())
        self.assertEqual(payload["artifact_schema"], RUN_SPEC_SCHEMA)
        self.assertEqual(payload["campaign_id"], "campaign-2026-08")
        self.assertEqual(payload["barrier_policy"]["profit_ticks"], 24)
        self.assertEqual(
            first.canonical_json(),
            json.dumps(
                payload,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8"),
        )

    def test_any_value_change_changes_fingerprint(self) -> None:
        original = _run_spec()
        changed_policy = _run_spec(
            barrier_policy={
                "loss_ticks": 17,
                "profit_ticks": 24,
                "tie_break": "STOP_FIRST",
            }
        )
        changed_seed = replace(original, random_seed=43)

        self.assertNotEqual(original.fingerprint, changed_policy.fingerprint)
        self.assertNotEqual(original.fingerprint, changed_seed.fingerprint)

    def test_noncanonical_values_are_rejected_with_clear_errors(self) -> None:
        invalid_values = (
            (0.25, "binary floats"),
            (float("nan"), "binary floats"),
            (float("inf"), "binary floats"),
            (-float("inf"), "binary floats"),
            (Path("/tmp/input.parquet"), "Path values"),
            (datetime(2026, 8, 3, tzinfo=UTC), "date or datetime"),
            ({"LONG", "SHORT"}, "unordered set"),
        )

        for value, message in invalid_values:
            with self.subTest(value=value), self.assertRaisesRegex(RunSpecError, message):
                _run_spec(parameters={"invalid": value})

    def test_identity_seed_direction_and_mapping_validation(self) -> None:
        invalid_overrides = (
            ({"code_snapshot_sha256": "ABC"}, "lowercase 64-character"),
            ({"dependency_lock_sha256": "ABC"}, "lowercase 64-character"),
            ({"code_commit": "short"}, "full lowercase Git object ID"),
            ({"run_kind": "UNKNOWN"}, "run_kind must be one of"),
            ({"source_manifest_hashes": {}}, "non-empty mapping"),
            ({"random_seed": True}, "unsigned 64-bit integer"),
            ({"random_seed": -1}, "between 0 and 2\\^64 - 1"),
            ({"random_seed": 2**64}, "between 0 and 2\\^64 - 1"),
            ({"direction": "long"}, "direction must be one of"),
            ({"signal_policy": {}}, "signal_policy must not be empty"),
            ({"parameters": {1: "bad-key"}}, "non-empty string keys"),
        )

        for overrides, message in invalid_overrides:
            with self.subTest(overrides=overrides), self.assertRaisesRegex(RunSpecError, message):
                _run_spec(**overrides)

    def test_campaign_level_run_kinds_allow_null_experiment(self) -> None:
        for run_kind in ("FEATURE_BUILD", "OUTCOME_BUILD", "AI_SLICE", "QUERY"):
            with self.subTest(run_kind=run_kind):
                spec = _run_spec(run_kind=run_kind, experiment_id=None)
                self.assertIsNone(spec.experiment_id)
                self.assertIsNone(json.loads(spec.canonical_json())["experiment_id"])

        with self.assertRaisesRegex(RunSpecError, "experiment_id may be null"):
            _run_spec(run_kind="BARRIER_SURFACE", experiment_id=None)

    def test_inputs_are_detached_and_deeply_immutable(self) -> None:
        parameters = {
            "enabled": True,
            "model": {"alpha": "0.01", "layers": [1, 2, 3]},
        }
        spec = _run_spec(parameters=parameters)
        canonical_before_mutation = spec.canonical_json()

        parameters["enabled"] = False
        parameters["model"]["layers"].append(4)  # type: ignore[index,union-attr]

        self.assertEqual(spec.canonical_json(), canonical_before_mutation)
        self.assertEqual(spec.parameters["enabled"], True)
        self.assertEqual(
            spec.parameters["model"],
            {"alpha": "0.01", "layers": (1, 2, 3)},
        )

        with self.assertRaises(FrozenInstanceError):
            spec.direction = "LONG"
        with self.assertRaises(TypeError):
            spec.parameters["enabled"] = False  # type: ignore[index]


if __name__ == "__main__":
    unittest.main()
