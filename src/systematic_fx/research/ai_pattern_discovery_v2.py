"""Direction-consistent catalog policy for the second pattern-proposal batch.

The first proposal batch is an immutable historical result.  This module does
not modify its request, catalog, parser, or replay path.  Instead it derives a
new, independently identified catalog from the frozen v1 universe and rejects
patterns whose family or declared direction is not expressed by the rule.

The v2 selector still has proposal-only authority: it consumes the same
outcome-blind :class:`DiscoveryContext` and delegates support, stability, and
diversity ranking to the frozen v1 implementation.  It cannot evaluate
performance, access a holdout, mutate a database, or promote a candidate.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from systematic_fx.research.ai_pattern_discovery import (
    AUTHORITY,
    RULE_SPACE_SHA256,
    DiscoveryContext,
    PatternDiscoveryError,
    PatternProposal,
    ProposalBatch,
    ProposalRequest,
    ProposedPattern,
    _deterministic_universe,
    _required_sha256,
    _select_proposals,
)
from systematic_fx.research.hypotheses import canonical_sha256

V1_CANDIDATE_CATALOG_COUNT: Final = 620
V1_CANDIDATE_CATALOG_SHA256: Final = (
    "b5ab777126eace96858c57cf619a954195d19187902bc1b6fbf56b8e1ad90ef3"
)

V2_CANDIDATE_CATALOG_COUNT: Final = 560
V2_CANDIDATE_CATALOG_SHA256: Final = (
    "01653f2faacd50b62552e649152ce3baf965aa45ca70fdaae6871d0ab75b0f71"
)
V2_FILTERED_DIRECTIONLESS_RANGE_COUNT: Final = 60
V2_REJECTED_V1_CANDIDATE_SHA256: Final = (
    "7b71032d369271071fd487fa3f4a31ede0c94981ce5aaa8c88fbae97ef690f1a"
)
V2_REJECTION_REASON: Final = "RANGE_DIRECTION_REQUIRES_SIGNED_BODY_PREDICATE"
V2_PROPOSER_MODE: Final = "DETERMINISTIC_OUTCOME_BLIND_DIRECTIONAL_V2"
DIRECTIONAL_PROPOSAL_ENVELOPE_SCHEMA: Final = (
    "systematic_fx.ai_pattern_directional_proposal_envelope.v2"
)
DIRECTIONAL_PROPOSAL_REQUEST_SCHEMA: Final = (
    "systematic_fx.ai_pattern_directional_proposal_request.v2"
)
DIRECTIONAL_PROPOSAL_BATCH_SCHEMA: Final = "systematic_fx.ai_pattern_directional_proposal_batch.v2"

V2_SEMANTIC_POLICY_DOCUMENT: Final = {
    "artifact_schema": "systematic_fx.ai_pattern_semantic_policy.v2",
    "families": {
        "BODY_CLOSE_CONFIRMATION": {
            "LONG": {
                "optional": [["range_ticks", "GE"]],
                "required": [
                    ["close_location_ppm", "GE"],
                    ["signed_body_ppm", "GE"],
                ],
            },
            "SHORT": {
                "optional": [["range_ticks", "GE"]],
                "required": [
                    ["close_location_ppm", "LE"],
                    ["signed_body_ppm", "LE"],
                ],
            },
        },
        "RANGE_EXPANSION_CONTINUATION": {
            "LONG": {
                "optional": [],
                "required": [
                    ["absolute_body_ppm", "GE"],
                    ["range_ticks", "GE"],
                    ["signed_body_ppm", "GE"],
                ],
            },
            "SHORT": {
                "optional": [],
                "required": [
                    ["absolute_body_ppm", "GE"],
                    ["range_ticks", "GE"],
                    ["signed_body_ppm", "LE"],
                ],
            },
        },
        "WICK_REJECTION_REVERSAL": {
            "LONG": {
                "optional": [["absolute_body_ppm", "GE"]],
                "required": [
                    ["close_location_ppm", "GE"],
                    ["lower_wick_ppm", "GE"],
                ],
            },
            "SHORT": {
                "optional": [["absolute_body_ppm", "GE"]],
                "required": [
                    ["close_location_ppm", "LE"],
                    ["upper_wick_ppm", "GE"],
                ],
            },
        },
    },
    "rule_space_sha256": RULE_SPACE_SHA256,
}
V2_SEMANTIC_POLICY_SHA256: Final = canonical_sha256(V2_SEMANTIC_POLICY_DOCUMENT)

V2_DETERMINISTIC_PROMPT_DOCUMENT: Final = {
    "candidate_catalog_count": V2_CANDIDATE_CATALOG_COUNT,
    "candidate_catalog_sha256": V2_CANDIDATE_CATALOG_SHA256,
    "contract": "outcome-blind direction-consistent support/stability/diversity proposer",
    "semantic_policy_sha256": V2_SEMANTIC_POLICY_SHA256,
    "version": 2,
}
V2_DETERMINISTIC_PROMPT_SHA256: Final = canonical_sha256(V2_DETERMINISTIC_PROMPT_DOCUMENT)


class V2PatternSemanticError(PatternDiscoveryError):
    """A finite DSL rule does not express its claimed family and direction."""


@dataclass(frozen=True, slots=True)
class DirectionalProposalEnvelope:
    """Canonical v2 policy identity bound around one base request SHA.

    The base :class:`ProposalRequest` remains unchanged so the historical v1
    implementation can still reconstruct batch 1.  This envelope supplies the
    missing v2 proposer, prompt, semantic-policy, derived-catalog, and explicit
    rejection identities for a new durable precommit.
    """

    base_request_sha256: str

    def __post_init__(self) -> None:
        _required_sha256(self.base_request_sha256, label="base_request_sha256")

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": DIRECTIONAL_PROPOSAL_ENVELOPE_SCHEMA,
            "authority": AUTHORITY,
            "base_request_sha256": self.base_request_sha256,
            "candidate_catalog": {
                "candidate_count": V2_CANDIDATE_CATALOG_COUNT,
                "candidate_sha256": V2_CANDIDATE_CATALOG_SHA256,
            },
            "derivation": {
                "rejected_candidate_count": V2_FILTERED_DIRECTIONLESS_RANGE_COUNT,
                "rejected_candidate_sha256": V2_REJECTED_V1_CANDIDATE_SHA256,
                "rejection_reason": V2_REJECTION_REASON,
                "source_candidate_count": V1_CANDIDATE_CATALOG_COUNT,
                "source_candidate_sha256": V1_CANDIDATE_CATALOG_SHA256,
            },
            "execution_prohibited": {
                "database_mutation": True,
                "m0b_epoch_registration": True,
                "paper_live_or_promotion": True,
                "performance_evaluation": True,
            },
            "proposer": {
                "mode": V2_PROPOSER_MODE,
                "prompt_sha256": V2_DETERMINISTIC_PROMPT_SHA256,
            },
            "semantic_policy": {
                "policy_schema": V2_SEMANTIC_POLICY_DOCUMENT["artifact_schema"],
                "policy_sha256": V2_SEMANTIC_POLICY_SHA256,
            },
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


def directional_proposal_precommit_document(
    base_request: ProposalRequest,
    envelope: DirectionalProposalEnvelope,
) -> dict[str, object]:
    """Bind the reusable base request and v2 policy into one precommit document."""

    if not isinstance(base_request, ProposalRequest):
        raise PatternDiscoveryError("v2 precommit requires a canonical base request")
    if not isinstance(envelope, DirectionalProposalEnvelope):
        raise PatternDiscoveryError("v2 precommit requires a directional envelope")
    if envelope.base_request_sha256 != base_request.sha256:
        raise PatternDiscoveryError("v2 envelope belongs to another base request")
    _validate_base_request_for_v2(base_request)
    return {
        "artifact_schema": DIRECTIONAL_PROPOSAL_REQUEST_SCHEMA,
        "authority": AUTHORITY,
        "base_request": base_request.as_dict(),
        "base_request_sha256": base_request.sha256,
        "directional_envelope": envelope.as_dict(),
        "directional_envelope_sha256": envelope.sha256,
    }


@dataclass(frozen=True, slots=True)
class DirectionalProposalBatch:
    """V2 policy identity plus the frozen selector's canonical batch payload."""

    envelope: DirectionalProposalEnvelope
    base_batch: ProposalBatch

    def __post_init__(self) -> None:
        if not isinstance(self.envelope, DirectionalProposalEnvelope):
            raise PatternDiscoveryError("v2 batch requires a canonical directional envelope")
        if not isinstance(self.base_batch, ProposalBatch):
            raise PatternDiscoveryError("v2 batch requires a canonical base batch")
        if self.base_batch.request_sha256 != self.envelope.base_request_sha256:
            raise PatternDiscoveryError("v2 batch belongs to another base request")
        if self.base_batch.candidate_universe_count != V2_CANDIDATE_CATALOG_COUNT:
            raise PatternDiscoveryError("v2 batch candidate count differs")
        for proposal in self.base_batch.proposals:
            validate_pattern_semantics_v2(proposal.pattern)

    @property
    def envelope_sha256(self) -> str:
        return self.envelope.sha256

    @property
    def proposals(self) -> tuple[PatternProposal, ...]:
        """Expose selected proposals without weakening the canonical wrapper."""

        return self.base_batch.proposals

    @property
    def candidate_universe_count(self) -> int:
        return self.base_batch.candidate_universe_count

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": DIRECTIONAL_PROPOSAL_BATCH_SCHEMA,
            "authority": AUTHORITY,
            "base_batch": self.base_batch.as_dict(),
            "base_batch_sha256": self.base_batch.sha256,
            "directional_envelope_sha256": self.envelope.sha256,
            "proposer_mode": V2_PROPOSER_MODE,
            "semantic_policy_sha256": V2_SEMANTIC_POLICY_SHA256,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


def _semantic_signatures(pattern: ProposedPattern) -> frozenset[tuple[str, str]]:
    return frozenset(
        (predicate.feature, predicate.operator) for predicate in pattern.rule.predicates
    )


def _required_and_optional_signatures(
    pattern: ProposedPattern,
) -> tuple[frozenset[tuple[str, str]], frozenset[tuple[str, str]]]:
    try:
        family_policy = V2_SEMANTIC_POLICY_DOCUMENT["families"][pattern.family]
        direction_policy = family_policy[pattern.direction]
    except (KeyError, TypeError) as error:
        raise V2PatternSemanticError("pattern family or direction is outside v2 policy") from error
    required = frozenset(tuple(item) for item in direction_policy["required"])
    optional = frozenset(tuple(item) for item in direction_policy["optional"])
    return required, optional


def validate_pattern_semantics_v2(pattern: ProposedPattern) -> ProposedPattern:
    """Return ``pattern`` only when its rule expresses its family and direction.

    Threshold validity and AND-only canonicalization remain owned by the v1
    finite DSL.  V2 adds the missing relationship between those predicates and
    the human-readable family/direction declaration.
    """

    if not isinstance(pattern, ProposedPattern):
        raise V2PatternSemanticError("v2 semantics require a canonical ProposedPattern")
    observed = _semantic_signatures(pattern)
    required, optional = _required_and_optional_signatures(pattern)
    if not required.issubset(observed) or not observed.issubset(required | optional):
        raise V2PatternSemanticError(
            f"{pattern.family}/{pattern.direction} rule violates v2 directional semantics"
        )
    return pattern


def deterministic_candidate_catalog_v2() -> tuple[ProposedPattern, ...]:
    """Return the exact 560-rule v2 catalog derived from the frozen v1 order."""

    v1_catalog = _deterministic_universe(3)
    if (
        len(v1_catalog) != V1_CANDIDATE_CATALOG_COUNT
        or canonical_sha256([pattern.as_dict() for pattern in v1_catalog])
        != V1_CANDIDATE_CATALOG_SHA256
    ):
        raise PatternDiscoveryError("frozen v1 candidate catalog identity drifted")

    accepted: list[ProposedPattern] = []
    rejected: list[ProposedPattern] = []
    for pattern in v1_catalog:
        try:
            accepted.append(validate_pattern_semantics_v2(pattern))
        except V2PatternSemanticError:
            rejected.append(pattern)
    catalog = tuple(accepted)
    if (
        len(rejected) != V2_FILTERED_DIRECTIONLESS_RANGE_COUNT
        or canonical_sha256([pattern.as_dict() for pattern in rejected])
        != V2_REJECTED_V1_CANDIDATE_SHA256
        or len(catalog) != V2_CANDIDATE_CATALOG_COUNT
        or canonical_sha256([pattern.as_dict() for pattern in catalog])
        != V2_CANDIDATE_CATALOG_SHA256
    ):
        raise PatternDiscoveryError("v2 candidate catalog identity drifted")
    return catalog


def _validate_base_request_for_v2(request: ProposalRequest) -> None:
    if request.proposer_mode != "DETERMINISTIC_OUTCOME_BLIND_V1":
        raise PatternDiscoveryError("v2 catalog requires the local outcome-blind selector")
    if (
        request.max_predicates_per_rule != 3
        or request.candidate_evaluation_budget != V2_CANDIDATE_CATALOG_COUNT
        or request.candidate_catalog_sha256 != V2_CANDIDATE_CATALOG_SHA256
    ):
        raise PatternDiscoveryError("request does not precommit the exact v2 candidate catalog")


def propose_deterministically_v2(
    request: ProposalRequest,
    context: DiscoveryContext,
    *,
    envelope: DirectionalProposalEnvelope,
) -> DirectionalProposalBatch:
    """Select proposals from the finite v2 catalog without outcome access.

    ``ProposalRequest`` remains the frozen transport envelope shared with v1;
    the precommitted candidate count and catalog SHA distinguish this policy.
    A future v2 run must therefore bind the constants exported by this module
    before any context is opened.
    """

    if not isinstance(request, ProposalRequest):
        raise PatternDiscoveryError("v2 proposer requires a canonical ProposalRequest")
    if not isinstance(envelope, DirectionalProposalEnvelope):
        raise PatternDiscoveryError("v2 proposer requires a directional envelope")
    _validate_base_request_for_v2(request)
    if envelope.base_request_sha256 != request.sha256:
        raise PatternDiscoveryError("v2 envelope belongs to another base request")
    catalog = deterministic_candidate_catalog_v2()
    return DirectionalProposalBatch(
        envelope=envelope,
        base_batch=_select_proposals(
            request,
            context,
            catalog,
            recorded_response_sha256=None,
        ),
    )


__all__ = [
    "DIRECTIONAL_PROPOSAL_BATCH_SCHEMA",
    "DIRECTIONAL_PROPOSAL_ENVELOPE_SCHEMA",
    "DIRECTIONAL_PROPOSAL_REQUEST_SCHEMA",
    "V1_CANDIDATE_CATALOG_COUNT",
    "V1_CANDIDATE_CATALOG_SHA256",
    "V2_CANDIDATE_CATALOG_COUNT",
    "V2_CANDIDATE_CATALOG_SHA256",
    "V2_DETERMINISTIC_PROMPT_DOCUMENT",
    "V2_DETERMINISTIC_PROMPT_SHA256",
    "V2_FILTERED_DIRECTIONLESS_RANGE_COUNT",
    "V2_PROPOSER_MODE",
    "V2_REJECTED_V1_CANDIDATE_SHA256",
    "V2_REJECTION_REASON",
    "V2_SEMANTIC_POLICY_DOCUMENT",
    "V2_SEMANTIC_POLICY_SHA256",
    "DirectionalProposalBatch",
    "DirectionalProposalEnvelope",
    "V2PatternSemanticError",
    "deterministic_candidate_catalog_v2",
    "directional_proposal_precommit_document",
    "propose_deterministically_v2",
    "validate_pattern_semantics_v2",
]
