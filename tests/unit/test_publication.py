import copy
import unittest
from pathlib import Path

from jsonschema import ValidationError

from systematic_fx.publication.config import load_publication_config
from systematic_fx.publication.contract import (
    canonical_payload_bytes,
    payload_sha256,
    validate_public_payload,
)
from systematic_fx.publication.provision import _application_url, _upsert_env_values
from systematic_fx.publication.snapshot import _model_is_ready

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "contracts/publication/research-snapshot.v2.schema.json"
PUBLICATION_CONFIG = ROOT / "configs/publication/research_site_v2.toml"


def _valid_payload() -> dict[str, object]:
    return {
        "metadata": {
            "schemaVersion": "2.0.0",
            "revision": 7,
            "dataAsOf": "2026-08-03",
            "publishedAt": "2026-08-03T12:00:00+00:00",
            "sourceRevision": "research-registry-sha256:abc+hypotheses-sha256:def",
            "disclosurePolicyVersion": "2.0.0",
        },
        "program": {
            "mode": "SCREENING_ONLY",
            "policyState": "FROZEN_SCREENING_POLICY",
            "maximumAuthority": "SCREENING_SURVIVOR",
            "backtestEligible": False,
            "paperEligible": False,
            "liveEligible": False,
            "disclosure": "Only allowlisted aggregate evidence is public.",
        },
        "campaign": {
            "key": "phase1a_conservative_screening_v1",
            "name": "Phase 1A",
            "status": "DRAFT",
            "stage": "OUTCOME_VALIDATION",
            "screeningAuthorized": True,
            "researchEligible": False,
            "strategyVariantBudget": 240,
            "sealedHoldoutFinalistBudget": 10,
            "summary": "Research is blocked until every prerequisite passes.",
        },
        "summary": {
            "families": 6,
            "hypotheses": 60,
            "observedPatterns": 1,
            "discoverySlices": 99,
            "queryExposures": 99,
            "runSpecs": 2,
            "runAttempts": 2,
            "succeededRuns": 1,
            "runningRuns": 1,
            "failedRuns": 0,
            "reusedRuns": 0,
            "outcomeCandidates": 1,
            "evaluatedCandidates": 1,
            "screeningSurvivors": 0,
            "screeningRejected": 1,
            "pendingCandidates": 0,
            "blockedCandidates": 0,
        },
        "dataQuality": {
            "datasetStatus": "VALIDATING",
            "sourceFiles": 1,
            "identifiedFiles": 1,
            "passedFiles": 0,
            "failedFiles": 1,
            "rowGroups": 2,
            "eventRows": 100,
            "hardViolations": 1,
            "warningSymbols": 0,
            "coverageStart": "2022-01-02",
            "coverageEnd": "2026-07-31",
            "eligibleDays": 1,
            "ineligibleDays": 0,
            "failedDates": [{"date": "2022-01-02", "violations": 1}],
        },
        "gates": [],
        "families": [],
        "hypotheses": [],
        "patterns": [
            {
                "id": "pattern-1",
                "family": "P1",
                "title": "Public pattern title",
                "status": "OPEN",
                "direction": "LONG",
                "description": "A bounded public description.",
                "parentHypothesisIds": ["p1_01"],
                "counterexampleCount": 2,
                "supportCount": 4,
                "observedSlices": 3,
                "evidenceState": "SCREENING_REJECTED",
                "screeningDecision": "SCREENING_REJECT",
                "updatedAt": "2026-08-03T12:00:00+00:00",
            }
        ],
        "runLedger": {
            "specs": 2,
            "attempts": 2,
            "reusedSuccesses": 0,
            "byKind": [],
        },
        "outcomeCandidates": [],
        "timeline": [],
    }


class PublicationContractTest(unittest.TestCase):
    def test_public_role_url_preserves_private_socket_coordinates(self) -> None:
        url = _application_url(
            "postgresql:///postgres?host=%2Fprivate%2Fsocket&port=55432",
            database_name="systematic_fx_public",
            role="systematic_fx_public_reader",
            password="safe-secret",
        )

        self.assertEqual(
            url,
            "postgresql://systematic_fx_public_reader:safe-secret@/systematic_fx_public?"
            "host=%2Fprivate%2Fsocket&port=55432",
        )

    def test_runtime_environment_update_replaces_duplicates_without_leaking_old_value(self) -> None:
        rendered = _upsert_env_values(
            "KEEP=value\nSITE_DATABASE_URL=old\nSITE_DATABASE_URL=stale\n",
            {"SITE_DATABASE_URL": "new"},
        )

        self.assertEqual(rendered, "KEEP=value\nSITE_DATABASE_URL=new\n")

    def test_pending_model_versions_cannot_open_research_gates(self) -> None:
        self.assertFalse(_model_is_ready("cost_pending_v1"))
        self.assertFalse(_model_is_ready("execution_unresolved_v2"))
        self.assertTrue(_model_is_ready("cost_numeric_v1"))

    def test_versioned_config_describes_exactly_six_families(self) -> None:
        config = load_publication_config(PUBLICATION_CONFIG)

        self.assertEqual(config.schema_version, "2.0.0")
        self.assertEqual(config.scope_key, "public-research")
        self.assertEqual(config.program_mode, "SCREENING_ONLY")
        self.assertEqual(config.policy_state, "FROZEN_SCREENING_POLICY")
        self.assertEqual(
            [candidate.candidate_id for candidate in config.outcome_candidates],
            [
                "p5_01_range_expansion_flow_continuation",
                "p1_05_unconfirmed_move_reversal",
            ],
        )
        self.assertEqual(
            [family.family_id for family in config.families],
            [
                "P1",
                "P2",
                "P3",
                "P4",
                "P5",
                "P6",
            ],
        )

    def test_canonical_hash_does_not_depend_on_mapping_order(self) -> None:
        first = {"b": 2, "a": 1}
        second = {"a": 1, "b": 2}

        self.assertEqual(canonical_payload_bytes(first), canonical_payload_bytes(second))
        self.assertEqual(payload_sha256(first), payload_sha256(second))

    def test_public_payload_accepts_only_the_allowlisted_shape(self) -> None:
        payload = _valid_payload()

        validate_public_payload(payload, CONTRACT)

        unsafe = copy.deepcopy(payload)
        unsafe["patterns"][0]["privatePath"] = "/raw/data"
        with self.assertRaises(ValidationError):
            validate_public_payload(unsafe, CONTRACT)

    def test_publication_migrations_encode_append_only_projection_and_outbox(self) -> None:
        private_migration = (ROOT / "migrations/0006_publication_outbox.sql").read_text()
        public_migration = (ROOT / "publication/migrations/0001_public_projection.sql").read_text()

        self.assertIn("FOR EACH STATEMENT", private_migration)
        self.assertIn("publication_outbox", private_migration)
        self.assertIn("current_research_publications", public_migration)
        self.assertIn("UNIQUE (campaign_key, revision)", public_migration)
        self.assertNotIn("ON DELETE CASCADE", public_migration)


if __name__ == "__main__":
    unittest.main()
