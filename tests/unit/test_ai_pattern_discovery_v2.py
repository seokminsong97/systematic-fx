from __future__ import annotations

from dataclasses import replace

import pytest

from systematic_fx.research.ai_pattern_discovery import (
    ALLOWED_FEATURES,
    DETERMINISTIC_PROMPT_SHA256,
    AndRule,
    PatternDiscoveryError,
    ProposalRequest,
    ProposedPattern,
    RulePredicate,
    _deterministic_universe,
    build_discovery_context,
)
from systematic_fx.research.ai_pattern_discovery_v2 import (
    DIRECTIONAL_PROPOSAL_REQUEST_SCHEMA,
    V1_CANDIDATE_CATALOG_COUNT,
    V1_CANDIDATE_CATALOG_SHA256,
    V2_CANDIDATE_CATALOG_COUNT,
    V2_CANDIDATE_CATALOG_SHA256,
    V2_DETERMINISTIC_PROMPT_SHA256,
    V2_FILTERED_DIRECTIONLESS_RANGE_COUNT,
    V2_SEMANTIC_POLICY_SHA256,
    DirectionalProposalEnvelope,
    V2PatternSemanticError,
    deterministic_candidate_catalog_v2,
    directional_proposal_precommit_document,
    propose_deterministically_v2,
    validate_pattern_semantics_v2,
)
from systematic_fx.research.hypotheses import canonical_sha256


def _request() -> ProposalRequest:
    return ProposalRequest(
        request_key="synthetic.pattern.discovery.v2",
        proposer_mode="DETERMINISTIC_OUTCOME_BLIND_V1",
        provider_id="SYSTEMATIC_FX_LOCAL",
        model_id="OUTCOME_BLIND_SUPPORT_STABILITY_DIVERSITY",
        model_version="v1",
        prompt_sha256=DETERMINISTIC_PROMPT_SHA256,
        source_feature_sha256="a" * 64,
        source_feature_version="synthetic_features_v1",
        discovery_split_sha256="b" * 64,
        source_interval_start="2022-01-01",
        source_interval_end="2022-01-03",
        max_source_rows=100,
        max_context_bins=100,
        proposal_budget=3,
        max_predicates_per_rule=3,
        minimum_support_rows=2,
        minimum_session_count=3,
        minimum_stability_ppm=900_000,
        maximum_pairwise_overlap_ppm=1_000_000,
        max_model_calls=0,
        max_input_tokens=0,
        max_output_tokens=0,
        max_response_bytes=0,
        deterministic_seed=7,
        precommitted_at_utc="2026-08-13T00:00:00Z",
        candidate_evaluation_budget=V2_CANDIDATE_CATALOG_COUNT,
        candidate_catalog_sha256=V2_CANDIDATE_CATALOG_SHA256,
        code_commit="1" * 40,
        proposer_implementation_sha256="2" * 64,
        dependency_lock_sha256="3" * 64,
    )


def _feature_rows() -> list[dict[str, object]]:
    vectors = (
        (8, -600_000, 600_000, 100_000, 300_000, 100_000),
        (6, -300_000, 300_000, 250_000, 450_000, 50_000),
        (4, 300_000, 300_000, 750_000, 50_000, 450_000),
        (10, 600_000, 600_000, 900_000, 100_000, 300_000),
    )
    return [
        {"session_id": session, **dict(zip(ALLOWED_FEATURES, vector, strict=True))}
        for session in ("S1", "S2", "S3")
        for vector in vectors
    ]


def _pattern(
    family: str,
    direction: str,
    predicates: tuple[tuple[str, str, int], ...],
) -> ProposedPattern:
    rationale = {
        "BODY_CLOSE_CONFIRMATION": "DIRECTIONAL_BODY_CLOSE_CONFIRMATION",
        "RANGE_EXPANSION_CONTINUATION": "RANGE_BODY_CONTINUATION",
        "WICK_REJECTION_REVERSAL": "INTRABAR_WICK_REJECTION",
    }[family]
    return ProposedPattern(
        family=family,
        direction=direction,  # type: ignore[arg-type]
        rationale_code=rationale,
        rule=AndRule(
            tuple(
                RulePredicate(feature, operator, threshold)  # type: ignore[arg-type]
                for feature, operator, threshold in predicates
            )
        ),
    )


@pytest.mark.parametrize("direction", ("LONG", "SHORT"))
def test_v2_rejects_directionless_range_rules(direction: str) -> None:
    pattern = _pattern(
        "RANGE_EXPANSION_CONTINUATION",
        direction,
        (
            ("absolute_body_ppm", "GE", 750_000),
            ("range_ticks", "GE", 32),
        ),
    )

    with pytest.raises(V2PatternSemanticError, match="directional semantics"):
        validate_pattern_semantics_v2(pattern)


@pytest.mark.parametrize(
    ("pattern"),
    (
        _pattern(
            "BODY_CLOSE_CONFIRMATION",
            "LONG",
            (
                ("close_location_ppm", "LE", 400_000),
                ("signed_body_ppm", "LE", -250_000),
            ),
        ),
        _pattern(
            "WICK_REJECTION_REVERSAL",
            "SHORT",
            (
                ("close_location_ppm", "GE", 600_000),
                ("lower_wick_ppm", "GE", 500_000),
            ),
        ),
        _pattern(
            "WICK_REJECTION_REVERSAL",
            "LONG",
            (
                ("close_location_ppm", "GE", 600_000),
                ("upper_wick_ppm", "GE", 500_000),
            ),
        ),
    ),
)
def test_v2_rejects_family_or_direction_mismatch(pattern: ProposedPattern) -> None:
    with pytest.raises(V2PatternSemanticError, match="directional semantics"):
        validate_pattern_semantics_v2(pattern)


def test_v1_catalog_identity_remains_unchanged() -> None:
    catalog = _deterministic_universe(3)

    assert len(catalog) == V1_CANDIDATE_CATALOG_COUNT
    assert (
        canonical_sha256([pattern.as_dict() for pattern in catalog]) == V1_CANDIDATE_CATALOG_SHA256
    )


def test_v2_catalog_is_exact_and_all_rules_are_semantically_valid() -> None:
    catalog = deterministic_candidate_catalog_v2()

    assert len(catalog) == V2_CANDIDATE_CATALOG_COUNT
    assert (
        canonical_sha256([pattern.as_dict() for pattern in catalog]) == V2_CANDIDATE_CATALOG_SHA256
    )
    assert V1_CANDIDATE_CATALOG_COUNT - len(catalog) == V2_FILTERED_DIRECTIONLESS_RANGE_COUNT
    assert all(validate_pattern_semantics_v2(pattern) is pattern for pattern in catalog)
    assert all(
        len(pattern.rule.predicates) == 3
        for pattern in catalog
        if pattern.family == "RANGE_EXPANSION_CONTINUATION"
    )
    assert V2_SEMANTIC_POLICY_SHA256 == (
        "c2db18a152dda596d547a8bd9879f57c70f8b06212191164e5a7a2043b7ffa45"
    )
    assert V2_DETERMINISTIC_PROMPT_SHA256 == (
        "e39f633fd1b33ec113ed068e7abdecd31e4d5ad8a58da2b42ead2cc59c17ecec"
    )


def test_v2_proposer_is_deterministic_and_requires_exact_precommit() -> None:
    request = _request()
    envelope = DirectionalProposalEnvelope(request.sha256)
    first_context = build_discovery_context(request, _feature_rows())
    second_context = build_discovery_context(request, list(reversed(_feature_rows())))

    first = propose_deterministically_v2(request, first_context, envelope=envelope)
    second = propose_deterministically_v2(request, second_context, envelope=envelope)

    assert first_context.sha256 == second_context.sha256
    assert first.sha256 == second.sha256
    assert first.candidate_universe_count == V2_CANDIDATE_CATALOG_COUNT
    assert len(first.proposals) == request.proposal_budget
    assert all(
        validate_pattern_semantics_v2(proposal.pattern) is proposal.pattern
        for proposal in first.proposals
    )

    wrong_budget = replace(request, candidate_evaluation_budget=V1_CANDIDATE_CATALOG_COUNT)
    with pytest.raises(PatternDiscoveryError, match="exact v2 candidate catalog"):
        propose_deterministically_v2(wrong_budget, first_context, envelope=envelope)


def test_v2_envelope_binds_base_request_and_every_policy_identity() -> None:
    request = _request()
    first = DirectionalProposalEnvelope(request.sha256)
    second = DirectionalProposalEnvelope(request.sha256)

    assert first.sha256 == second.sha256
    document = directional_proposal_precommit_document(request, first)
    assert document["artifact_schema"] == DIRECTIONAL_PROPOSAL_REQUEST_SCHEMA
    assert document["base_request_sha256"] == request.sha256
    assert document["directional_envelope_sha256"] == first.sha256
    assert document["directional_envelope"] == first.as_dict()
    assert document["base_request"] == request.as_dict()

    other_request = replace(request, request_key="synthetic.pattern.discovery.v2.other")
    wrong_envelope = DirectionalProposalEnvelope(other_request.sha256)
    with pytest.raises(PatternDiscoveryError, match="another base request"):
        directional_proposal_precommit_document(request, wrong_envelope)
    context = build_discovery_context(request, _feature_rows())
    with pytest.raises(PatternDiscoveryError, match="another base request"):
        propose_deterministically_v2(request, context, envelope=wrong_envelope)
