import hashlib
import json
import os
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from systematic_fx.db.pattern_registry import (
    PatternRegistryDriftError,
    PatternRegistryError,
    PatternSliceObservation,
    _append_documents,
    _ExposureIdentity,
    _initial_documents,
    _open_validated_discovery_evidence,
    _validate_governed_query,
    _verify_open_artifact_binding,
)
from systematic_fx.features.screening import FEATURE_VERSION, FORMULA_SHA256
from systematic_fx.research.discovery_slice import (
    DISCOVERY_SLICE_SCHEMA,
    DISCOVERY_SLICE_VERSION,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256


class PatternRegistryTest(unittest.TestCase):
    def _observation(self, **overrides: object) -> PatternSliceObservation:
        values: dict[str, object] = {
            "campaign_key": "phase1a_conservative_screening_v1",
            "pattern_key": "phase1a:pattern:p2_01:v1",
            "query_id": "p2_01",
            "run_fingerprint": "a" * 64,
            "exposure_key": "phase1a:query:p2_01:slice-0001",
            "query_definition": {
                "id": "p2_01",
                "conditions": ["abs(l1)>=400000", "spread<=2"],
                "direction_rule": "SIGN_L1",
                "parent_hypothesis_ids": ["p2_01_l1_imbalance_persistence"],
            },
            "feature_identity": {
                "feature_version": "phase1a_mbp10_screening_v1",
                "formula_sha256": "b" * 64,
                "code_snapshot_sha256": "c" * 64,
            },
            "direction": "BOTH",
            "entry_condition": "abs(l1)>=400000 AND spread<=2",
            "economic_rationale": "Persistent displayed depth may predict continuation.",
            "applicable_regime": {"screening_only": True},
            "counterexamples": [{"horizon_bars": 1, "negative_count": 3}],
            "support_count": 7,
            "candidate_barrier_region": {},
            "forward_first_touch_summary": {"1": {"resolved_count": 6, "unresolved_count": 1}},
            "cost_assumptions": {
                "cost_version": "phase1a_conservative_cost_v1",
                "cost_sha256": "d" * 64,
            },
        }
        values.update(overrides)
        return PatternSliceObservation(**values)  # type: ignore[arg-type]

    @staticmethod
    def _exposure(identifier: int = 11, *, start_day: int = 2) -> _ExposureIdentity:
        start = datetime(2022, 1, start_day, tzinfo=UTC)
        return _ExposureIdentity(
            discovery_exposure_id=identifier,
            research_run_spec_id=identifier + 100,
            result_artifact_id=identifier + 200,
            source_interval_start=start,
            source_interval_end=start + timedelta(days=5),
        )

    def _governed_artifact_fixture(self, root: Path) -> dict[str, Any]:
        campaign_key = "phase1a_conservative_screening_v1"
        query_definition = {
            "id": "p2_01",
            "conditions": ["abs(l1)>=400000", "spread<=2"],
            "direction_rule": "SIGN_L1",
            "parent_hypothesis_ids": ["p2_01_l1_imbalance_persistence"],
        }
        requested_dates = [f"2022-01-0{day}" for day in range(2, 7)]
        feature_inputs = [
            {
                "path": "derived/research_5m/source_date=2022-01-03/part-000.parquet",
                "sha256": "1" * 64,
                "source_date": "2022-01-03",
            }
        ]
        negative = {
            "bucket_end_ns": 1,
            "direction": "LONG",
            "forward": {
                "1": None,
                "3": None,
                "6": None,
                "12": {
                    "aligned_close_x2_ticks": -2,
                    "maximum_adverse_excursion_x2_ticks": -4,
                    "maximum_favorable_excursion_x2_ticks": 1,
                },
            },
            "source_date": "2022-01-03",
            "variables": {"l1_last_ppm": 500_000},
        }
        positive = {
            "bucket_end_ns": 2,
            "direction": "LONG",
            "forward": {
                "1": None,
                "3": None,
                "6": None,
                "12": {
                    "aligned_close_x2_ticks": 4,
                    "maximum_adverse_excursion_x2_ticks": -1,
                    "maximum_favorable_excursion_x2_ticks": 6,
                },
            },
            "source_date": "2022-01-04",
            "variables": {"l1_last_ppm": 600_000},
        }
        forward = {
            "1": {"resolved_count": 0, "unresolved_count": 2},
            "3": {"resolved_count": 0, "unresolved_count": 2},
            "6": {"resolved_count": 0, "unresolved_count": 2},
            "12": {"resolved_count": 2, "unresolved_count": 0},
        }
        query_result = {
            "definition": query_definition,
            "direction_counts": {"LONG": 2, "SHORT": 0},
            "forward": forward,
            "occurrences": [negative, positive],
            "source_date_count": 2,
            "support_count": 2,
        }
        discovery_config_sha256 = "2" * 64
        barrier_sha256 = "3" * 64
        cost_sha256 = "4" * 64
        frozen_inputs = {
            "barrier_grid": {
                "document": {
                    "barrier_grid": {
                        "expected_cell_count": 4,
                        "stop_loss_pips": [12, 16],
                        "stop_loss_ticks": [24, 32],
                        "take_profit_pips": [12, 16],
                        "take_profit_ticks": [24, 32],
                    }
                },
                "sha256": barrier_sha256,
            },
            "cost": {
                "document": {
                    "economic_floor": {
                        "baseline_cost_ticks": 8,
                        "baseline_minimum_take_profit_ticks": 24,
                    },
                    "fully_loaded_fixed_allocation": {
                        "allocated_fixed_cost_ticks_per_round_trip": 4
                    },
                    "variable_cost": {"round_trip_debit_ticks": 4},
                },
                "sha256": cost_sha256,
            },
            "discovery_query": {"document": {}, "sha256": discovery_config_sha256},
            "parent_hypotheses": {
                "document": {
                    "hypotheses": [
                        {
                            "id": "p2_01_l1_imbalance_persistence",
                            "economic_rationale": "Persistent displayed depth may predict continuation.",
                        }
                    ]
                },
                "sha256": "5" * 64,
            },
        }
        shared_spec = {
            "artifact_schema": "systematic_fx.research_run_spec.v2",
            "schema_version": 2,
            "campaign_id": campaign_key,
            "source_manifest_hashes": {
                "mbp10_footer_manifest_v1": "6" * 64,
                "mbp10_source_sha256_v1": "7" * 64,
                "mbp10_structural_qc_v1": "8" * 64,
            },
            "eligible_calendar": {"version": "calendar_v1", "sha256": "9" * 64},
            "split": {"version": "split_v1", "sha256": "a" * 64},
            "feature": {"version": FEATURE_VERSION, "sha256": "b" * 64},
            "outcome": {"version": "barrier_v1", "sha256": barrier_sha256},
            "cost": {"version": "cost_v1", "sha256": cost_sha256},
            "execution": {"version": "execution_v1", "sha256": "c" * 64},
            "code_commit": "d" * 40,
            "code_snapshot_sha256": "e" * 64,
            "dependency_lock_sha256": "f" * 64,
            "runtime_environment": {"python": "synthetic"},
            "random_seed": 0,
            "direction": "BOTH",
            "signal_policy": {"signal_cadence_seconds": 300},
            "entry_policy": {"entry": "frozen"},
            "barrier_policy": {"barrier": "frozen"},
            "terminal_policy": {"terminal": "frozen"},
        }
        parent_parameters = {
            "candidate_queries": [query_definition],
            "candidate_query_definition_sha256": "0" * 64,
            "feature_inputs_by_date": {
                "2022-01-03": {
                    "relative_path": feature_inputs[0]["path"],
                    "sha256": feature_inputs[0]["sha256"],
                }
            },
            "feature_manifest_sha256": "1" * 64,
            "frozen_toml_inputs": frozen_inputs,
            "requested_source_dates": requested_dates,
        }
        parent_spec = {
            **shared_spec,
            "experiment_id": None,
            "run_kind": "AI_SLICE",
            "engine_version": "discovery_v1",
            "parameters": parent_parameters,
        }
        parent_fingerprint = canonical_sha256(parent_spec)
        artifact_document = {
            "artifact_schema": DISCOVERY_SLICE_SCHEMA,
            "artifact_version": DISCOVERY_SLICE_VERSION,
            "authority": {
                "maximum_authority": "OPEN_OBSERVATION",
                "pass_backtest_allowed": False,
                "screening_survivor_allowed": False,
                "screening_only": True,
            },
            "code_snapshot_sha256": shared_spec["code_snapshot_sha256"],
            "config": {
                "definition_sha256": parent_parameters["candidate_query_definition_sha256"],
                "relative_path": "configs/research/phase1a_discovery_slice_v1.toml",
                "sha256": discovery_config_sha256,
            },
            "coverage": [],
            "feature_distributions": {},
            "feature_inputs": feature_inputs,
            "no_entry_reasons": {},
            "query_results": [query_result],
            "requested_source_dates": requested_dates,
            "run_fingerprint": parent_fingerprint,
            "summary": {
                "candidate_query_count": 1,
                "eligible_rows": 2,
                "feature_rows": 2,
                "nonzero_support_query_count": 1,
                "zero_support_query_count": 0,
            },
        }
        payload = canonical_json_bytes(artifact_document) + b"\n"
        artifact_sha256 = hashlib.sha256(payload).hexdigest()
        data_root = root.resolve() / "data"
        artifact_path = (
            data_root
            / "derived/manifests"
            / DISCOVERY_SLICE_VERSION
            / f"sha256={artifact_sha256}.json"
        )
        artifact_path.parent.mkdir(parents=True)
        artifact_path.write_bytes(payload)
        query_parameters = {
            "candidate_query": query_definition,
            "discovery_artifact_relative_path": artifact_path.relative_to(data_root).as_posix(),
            "discovery_artifact_sha256": artifact_sha256,
            "frozen_toml_inputs": frozen_inputs,
            "parent_run_fingerprint": parent_fingerprint,
            "query_definition_sha256": canonical_sha256(query_definition),
            "query_result_sha256": canonical_sha256(query_result),
            "requested_source_dates": requested_dates,
            "research_eligible": False,
            "screening_only": True,
            "slice_index": 0,
        }
        query_spec = {
            **shared_spec,
            "experiment_id": None,
            "run_kind": "QUERY",
            "engine_version": "query_v1",
            "parameters": query_parameters,
        }
        query_fingerprint = canonical_sha256(query_spec)
        observation = self._observation(
            pattern_key=f"{campaign_key}:p2_01",
            run_fingerprint=query_fingerprint,
            exposure_key=f"{campaign_key}:query:00:p2_01",
            feature_identity={
                "calendar_sha256": "9" * 64,
                "code_snapshot_sha256": "e" * 64,
                "discovery_artifact_sha256": artifact_sha256,
                "discovery_config_sha256": discovery_config_sha256,
                "feature_config_sha256": "b" * 64,
                "feature_inputs": feature_inputs,
                "feature_manifest_sha256": "1" * 64,
                "feature_version": FEATURE_VERSION,
                "footer_manifest_sha256": "6" * 64,
                "formula_sha256": FORMULA_SHA256,
                "qc_manifest_sha256": "8" * 64,
                "source_manifest_sha256": "7" * 64,
            },
            entry_condition=("direction_rule=SIGN_L1; abs(l1)>=400000; spread<=2"),
            economic_rationale=(
                "p2_01_l1_imbalance_persistence: "
                "Persistent displayed depth may predict continuation."
            ),
            applicable_regime={
                "authority": "OPEN_OBSERVATION",
                "definition_status_available": False,
                "parent_hypothesis_ids": ["p2_01_l1_imbalance_persistence"],
                "research_eligible": False,
                "screening_only": True,
                "signal_cadence_seconds": 300,
            },
            counterexamples=[negative],
            support_count=2,
            candidate_barrier_region={
                "cell_count": 4,
                "stop_loss_pips": [12, 16],
                "stop_loss_ticks": [24, 32],
                "status": "NOT_EVALUATED_IN_DISCOVERY_SLICE",
                "take_profit_pips": [12, 16],
                "take_profit_ticks": [24, 32],
            },
            forward_first_touch_summary={
                "direction_counts": query_result["direction_counts"],
                "forward_close_and_excursion_proxy": forward,
                "first_touch_status": "NOT_COMPUTED",
                "reason": "DISCOVERY_SLICE_HAS_NO_EVENT_LEVEL_FIRST_TOUCH_OUTCOME",
                "source_date_count": 2,
            },
            cost_assumptions={
                "allocated_fixed_cost_ticks": 4,
                "baseline_cost_floor_ticks": 24,
                "cost_config_sha256": cost_sha256,
                "cost_model_version": "cost_v1",
                "status": "RECORDED_NOT_APPLIED_TO_FORWARD_PROXY",
                "variable_cost_ticks": 4,
            },
        )
        run_row = {
            "research_run_spec_id": 111,
            "campaign_id": 7,
            "parent_run_spec_id": 110,
            "run_fingerprint": query_fingerprint,
            "run_kind": "QUERY",
            "canonical_spec": query_spec,
            "parent_research_run_spec_id": 110,
            "parent_run_fingerprint": parent_fingerprint,
            "parent_run_kind": "AI_SLICE",
            "parent_canonical_spec": parent_spec,
        }
        exposure_row = {
            "discovery_exposure_id": 11,
            "campaign_id": 7,
            "exposure_key": observation.exposure_key,
            "exposure_type": "QUERY",
            "source_interval_start": datetime(2022, 1, 2, tzinfo=UTC),
            "source_interval_end": datetime(2022, 1, 7, tzinfo=UTC),
            "visible_to_ai": True,
            "research_eligible": False,
            "query_spec": {
                "candidate_query": query_definition,
                "query_definition_sha256": observation.query_definition_sha256,
                "run_fingerprint": query_fingerprint,
            },
            "result_summary": {
                "artifact_sha256": artifact_sha256,
                "direction_counts": query_result["direction_counts"],
                "source_date_count": 2,
                "support_count": 2,
            },
            "result_artifact_id": 211,
            "research_run_spec_id": 111,
            "config_sha256": discovery_config_sha256,
            "artifact_id": 211,
            "artifact_key": (
                f"{campaign_key}:discovery-exposure:{campaign_key}:ai-slice:00:{artifact_sha256}"
            ),
            "artifact_type": "DISCOVERY_EXPOSURE_RESULT",
            "artifact_uri": artifact_path.as_uri(),
            "artifact_sha256": artifact_sha256,
            "artifact_byte_size": len(payload),
            "artifact_media_type": "application/json",
            "artifact_metadata": {
                "campaign_key": campaign_key,
                "exposure_key": f"{campaign_key}:ai-slice:00",
                "exposure_type": "AI_SLICE",
                "run_fingerprint": parent_fingerprint,
            },
        }
        identity = _validate_governed_query(run_row, exposure_row, observation)
        return {
            "artifact_path": artifact_path,
            "exposure": identity,
            "exposure_row": exposure_row,
            "observation": observation,
            "payload": payload,
            "query_result": query_result,
            "run_row": run_row,
        }

    def test_observation_is_detached_and_definition_hash_is_stable(self) -> None:
        query = {
            "id": "p2_01",
            "conditions": ["abs(l1)>=400000", "spread<=2"],
            "direction_rule": "SIGN_L1",
            "parent_hypothesis_ids": ["p2_01_l1_imbalance_persistence"],
        }
        observation = self._observation(query_definition=query)
        digest = observation.query_definition_sha256

        query["conditions"].append("drift")

        self.assertEqual(
            observation.query_definition["conditions"],
            ["abs(l1)>=400000", "spread<=2"],
        )
        self.assertEqual(observation.query_definition_sha256, digest)
        with self.assertRaises(TypeError):
            observation.query_definition["id"] = "drift"  # type: ignore[index]

    def test_observation_rejects_incomplete_or_noncanonical_variables(self) -> None:
        with self.assertRaises(PatternRegistryError):
            self._observation(query_id="different")
        with self.assertRaises(PatternRegistryError):
            self._observation(support_count=-1)
        with self.assertRaises(PatternRegistryError):
            self._observation(feature_identity={"binary_float": 0.1})
        with self.assertRaises(PatternRegistryError):
            self._observation(run_fingerprint="not-a-sha")

    def test_query_run_and_exposure_must_bind_the_exact_definition(self) -> None:
        initial = self._observation()
        canonical_spec = {
            "parameters": {
                "candidate_query": dict(initial.query_definition),
                "query_definition_sha256": initial.query_definition_sha256,
            }
        }
        observation = self._observation(run_fingerprint=canonical_sha256(canonical_spec))
        run_row = {
            "research_run_spec_id": 111,
            "run_fingerprint": observation.run_fingerprint,
            "run_kind": "QUERY",
            "canonical_spec": canonical_spec,
        }
        exposure_row = {
            "discovery_exposure_id": 11,
            "research_run_spec_id": 111,
            "exposure_key": observation.exposure_key,
            "exposure_type": "QUERY",
            "visible_to_ai": True,
            "research_eligible": False,
            "result_artifact_id": 211,
            "query_spec": {
                "candidate_query": dict(observation.query_definition),
                "query_definition_sha256": observation.query_definition_sha256,
                "run_fingerprint": observation.run_fingerprint,
            },
            "source_interval_start": datetime(2022, 1, 2, tzinfo=UTC),
            "source_interval_end": datetime(2022, 1, 7, tzinfo=UTC),
        }

        identity = _validate_governed_query(run_row, exposure_row, observation)

        self.assertEqual(identity.discovery_exposure_id, 11)
        self.assertEqual(identity.result_artifact_id, 211)
        drifted = dict(exposure_row)
        drifted["query_spec"] = {**exposure_row["query_spec"], "unrecorded": True}
        with self.assertRaises(PatternRegistryDriftError):
            _validate_governed_query(run_row, drifted, observation)

    def test_discovery_artifact_bytes_bind_all_pattern_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._governed_artifact_fixture(Path(directory))
            evidence = _open_validated_discovery_evidence(
                fixture["run_row"],
                fixture["exposure_row"],
                fixture["observation"],
                fixture["exposure"],
            )
            try:
                self.assertEqual(evidence.query_result, fixture["query_result"])
                _verify_open_artifact_binding(evidence)
            finally:
                os.close(evidence.descriptor)

    def test_artifact_evidence_rejects_query_hash_and_observation_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._governed_artifact_fixture(Path(directory))
            original_observation = fixture["observation"]

            query_spec = json.loads(canonical_json_bytes(fixture["run_row"]["canonical_spec"]))
            query_spec["parameters"]["query_result_sha256"] = "0" * 64
            query_fingerprint = canonical_sha256(query_spec)
            query_hash_observation = replace(
                original_observation,
                run_fingerprint=query_fingerprint,
            )
            query_hash_run = {
                **fixture["run_row"],
                "canonical_spec": query_spec,
                "run_fingerprint": query_fingerprint,
            }
            query_hash_exposure = {
                **fixture["exposure_row"],
                "query_spec": {
                    **fixture["exposure_row"]["query_spec"],
                    "run_fingerprint": query_fingerprint,
                },
            }
            query_hash_identity = _validate_governed_query(
                query_hash_run,
                query_hash_exposure,
                query_hash_observation,
            )

            mutations = (
                (
                    "query_result_sha256",
                    query_hash_run,
                    query_hash_exposure,
                    query_hash_observation,
                    query_hash_identity,
                ),
                (
                    "support_count",
                    fixture["run_row"],
                    fixture["exposure_row"],
                    replace(original_observation, support_count=3),
                    fixture["exposure"],
                ),
                (
                    "feature_identity",
                    fixture["run_row"],
                    fixture["exposure_row"],
                    replace(
                        original_observation,
                        feature_identity={
                            **dict(original_observation.feature_identity),
                            "calendar_sha256": "0" * 64,
                        },
                    ),
                    fixture["exposure"],
                ),
                (
                    "forward_first_touch_summary",
                    fixture["run_row"],
                    fixture["exposure_row"],
                    replace(
                        original_observation,
                        forward_first_touch_summary={"drift": True},
                    ),
                    fixture["exposure"],
                ),
            )
            for label, run_row, exposure_row, observation, identity in mutations:
                with self.subTest(label=label), self.assertRaises(PatternRegistryDriftError):
                    evidence = _open_validated_discovery_evidence(
                        run_row,
                        exposure_row,
                        observation,
                        identity,
                    )
                    os.close(evidence.descriptor)

    def test_open_artifact_inode_remains_bound_until_pattern_commit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._governed_artifact_fixture(Path(directory))
            evidence = _open_validated_discovery_evidence(
                fixture["run_row"],
                fixture["exposure_row"],
                fixture["observation"],
                fixture["exposure"],
            )
            try:
                artifact_path = fixture["artifact_path"]
                artifact_path.rename(artifact_path.with_suffix(".replaced"))
                artifact_path.write_bytes(fixture["payload"])
                with self.assertRaisesRegex(PatternRegistryDriftError, "(inode|path) changed"):
                    _verify_open_artifact_binding(evidence)
            finally:
                os.close(evidence.descriptor)

    def test_rollup_is_idempotent_and_appends_nonoverlapping_slices(self) -> None:
        first = self._observation()
        first_exposure = self._exposure()
        features, counterexamples, summaries = _initial_documents(first, first_exposure)
        row = {
            "feature_definition_versions": features,
            "counterexamples": counterexamples,
            "forward_first_touch_summaries": summaries,
        }

        reused = _append_documents(row, first, first_exposure)

        self.assertFalse(reused[3])
        second = self._observation(
            run_fingerprint="e" * 64,
            exposure_key="phase1a:query:p2_01:slice-0002",
            support_count=2,
            counterexamples=[],
        )
        second_exposure = self._exposure(12, start_day=7)
        appended_features, _, appended_summaries, appended = _append_documents(
            row,
            second,
            second_exposure,
        )
        self.assertTrue(appended)
        self.assertEqual(len(appended_features["slice_identities"]), 2)
        self.assertEqual(len(appended_summaries["slice_observations"]), 2)

        overlap = self._exposure(13, start_day=6)
        with self.assertRaises(PatternRegistryDriftError):
            _append_documents(row, second, overlap)


if __name__ == "__main__":
    unittest.main()
