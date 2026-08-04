import copy
import unittest
from pathlib import Path

from systematic_fx.research.hypotheses import (
    EXPECTED_ABSOLUTE_BARRIER_TICKS,
    EXPECTED_LOOKBACK_BARS,
    EXPECTED_VOLATILITY_MULTIPLIERS,
    HypothesisConfigError,
    canonical_json_bytes,
    canonical_sha256,
    family_counts,
    load_hypothesis_bundle,
    load_toml_document,
    parse_hypothesis_bundle,
)

ROOT = Path(__file__).resolve().parents[2]
HYPOTHESES = ROOT / "configs" / "research" / "phase1_parent_hypotheses_v1.toml"


class Phase1HypothesisBundleTest(unittest.TestCase):
    def test_checked_in_bundle_has_sixty_balanced_a_priori_parents(self) -> None:
        bundle = load_hypothesis_bundle(HYPOTHESES)

        self.assertEqual(len(bundle.hypotheses), 60)
        self.assertEqual(family_counts(bundle.hypotheses), {f"P{i}": 10 for i in range(1, 7)})
        self.assertTrue(bundle.execution_blocked)
        self.assertEqual(bundle.lookback_bars, EXPECTED_LOOKBACK_BARS)
        self.assertEqual(bundle.absolute_barrier_ticks, EXPECTED_ABSOLUTE_BARRIER_TICKS)
        self.assertEqual(
            bundle.volatility_multipliers,
            EXPECTED_VOLATILITY_MULTIPLIERS,
        )
        self.assertEqual(bundle.campaign_strategy_variant_budget, 240)
        self.assertEqual(bundle.local_trial_budget, 272)
        self.assertEqual(sum(bundle.local_trial_budget_breakdown.values()), 272)
        self.assertTrue(
            all(item.interaction_family is not None for item in bundle.hypotheses[-10:])
        )
        self.assertTrue(all(item.interaction_family is None for item in bundle.hypotheses[:-10]))

    def test_canonical_hash_is_stable_and_binary_floats_are_rejected(self) -> None:
        first = load_hypothesis_bundle(HYPOTHESES)
        second = load_hypothesis_bundle(HYPOTHESES)

        self.assertEqual(first.config_sha256, second.config_sha256)
        self.assertEqual(
            canonical_sha256(first.registration_payload()),
            first.config_sha256,
        )
        with self.assertRaisesRegex(TypeError, "binary floats"):
            canonical_json_bytes({"unsafe": 0.5})

    def test_family_count_and_pending_execution_guards_reject_drift(self) -> None:
        document = load_toml_document(HYPOTHESES)

        missing = copy.deepcopy(document)
        missing["hypotheses"].pop()
        with self.assertRaisesRegex(HypothesisConfigError, "expected 60"):
            parse_hypothesis_bundle(missing)

        executable = copy.deepcopy(document)
        executable["bundle"]["execution_blocked"] = False
        with self.assertRaisesRegex(HypothesisConfigError, "must remain true"):
            parse_hypothesis_bundle(executable)

        unbounded = copy.deepcopy(document)
        unbounded["trial_budget"]["experiment_local_total"] = 200
        with self.assertRaises(HypothesisConfigError):
            parse_hypothesis_bundle(unbounded)


if __name__ == "__main__":
    unittest.main()
