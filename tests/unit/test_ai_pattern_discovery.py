from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path

import pytest

from systematic_fx.research.ai_pattern_discovery import (
    ALLOWED_FEATURES,
    DETERMINISTIC_PROMPT_SHA256,
    FINAL_STATUS,
    RECORDED_RESPONSE_SCHEMA,
    RULE_SCHEMA,
    AndRule,
    ImmutableArtifactError,
    PatternDiscoveryError,
    ProposalLedger,
    ProposalRequest,
    RulePredicate,
    UnsafeRecordedResponseError,
    begin_pattern_discovery,
    begin_pattern_discovery_with_context,
    build_discovery_context,
    complete_recorded_pattern_discovery,
    propose_deterministically,
    recorded_response_for_patterns,
    replay_recorded_response,
    run_deterministic_pattern_discovery,
    verify_immutable_artifact,
)
from systematic_fx.research.hypotheses import canonical_json_bytes


def _request(*, recorded: bool = False, budget: int = 3) -> ProposalRequest:
    shared = {
        "request_key": "synthetic.pattern.discovery.v1",
        "source_feature_sha256": "a" * 64,
        "source_feature_version": "synthetic_features_v1",
        "discovery_split_sha256": "b" * 64,
        "source_interval_start": "2022-01-01",
        "source_interval_end": "2022-01-03",
        "max_source_rows": 100,
        "max_context_bins": 100,
        "proposal_budget": budget,
        "max_predicates_per_rule": 3,
        "minimum_support_rows": 2,
        "minimum_session_count": 3,
        "minimum_stability_ppm": 900_000,
        "maximum_pairwise_overlap_ppm": 1_000_000,
        "deterministic_seed": 7,
        "precommitted_at_utc": "2026-08-13T00:00:00Z",
        "candidate_evaluation_budget": 620,
        "candidate_catalog_sha256": (
            "b5ab777126eace96858c57cf619a954195d19187902bc1b6fbf56b8e1ad90ef3"
        ),
        "code_commit": "1" * 40,
        "proposer_implementation_sha256": "2" * 64,
        "dependency_lock_sha256": "3" * 64,
    }
    if recorded:
        return ProposalRequest(
            proposer_mode="RECORDED_RESPONSE_V1",
            provider_id="RECORDED_CODEX",
            model_id="gpt-5.6",
            model_version="2026-08-13",
            prompt_sha256="c" * 64,
            max_model_calls=1,
            max_input_tokens=4_000,
            max_output_tokens=2_000,
            max_response_bytes=32_000,
            **shared,
        )
    return ProposalRequest(
        proposer_mode="DETERMINISTIC_OUTCOME_BLIND_V1",
        provider_id="SYSTEMATIC_FX_LOCAL",
        model_id="OUTCOME_BLIND_SUPPORT_STABILITY_DIVERSITY",
        model_version="v1",
        prompt_sha256=DETERMINISTIC_PROMPT_SHA256,
        max_model_calls=0,
        max_input_tokens=0,
        max_output_tokens=0,
        max_response_bytes=0,
        **shared,
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


def _recorded_response(request: ProposalRequest, context_sha256: str) -> bytes:
    document = {
        "artifact_schema": RECORDED_RESPONSE_SCHEMA,
        "context_sha256": context_sha256,
        "proposals": [
            {
                "direction": "LONG",
                "family": "BODY_CLOSE_CONFIRMATION",
                "rationale_code": "DIRECTIONAL_BODY_CLOSE_CONFIRMATION",
                "rule": {
                    "all": [
                        {
                            "feature": "signed_body_ppm",
                            "operator": "GE",
                            "threshold": 250_000,
                        },
                        {
                            "feature": "close_location_ppm",
                            "operator": "GE",
                            "threshold": 700_000,
                        },
                    ],
                    "artifact_schema": RULE_SCHEMA,
                },
            },
            {
                "direction": "SHORT",
                "family": "BODY_CLOSE_CONFIRMATION",
                "rationale_code": "DIRECTIONAL_BODY_CLOSE_CONFIRMATION",
                "rule": {
                    "all": [
                        {
                            "feature": "close_location_ppm",
                            "operator": "LE",
                            "threshold": 300_000,
                        },
                        {
                            "feature": "signed_body_ppm",
                            "operator": "LE",
                            "threshold": -250_000,
                        },
                    ],
                    "artifact_schema": RULE_SCHEMA,
                },
            },
        ],
        "request_sha256": request.sha256,
    }
    assert request.proposal_budget == 2
    return canonical_json_bytes(document)


def test_rule_canonicalization_and_exact_finite_lattice() -> None:
    first = RulePredicate("signed_body_ppm", "GE", 250_000)
    second = RulePredicate("close_location_ppm", "GE", 700_000)

    left = AndRule((first, second))
    right = AndRule((second, first))

    assert left == right
    assert left.sha256 == right.sha256
    with pytest.raises(PatternDiscoveryError, match="threshold"):
        RulePredicate("signed_body_ppm", "GE", 3)
    with pytest.raises(PatternDiscoveryError, match="contradictory"):
        AndRule(
            (
                RulePredicate("signed_body_ppm", "GE", 500_000),
                RulePredicate("signed_body_ppm", "LE", -250_000),
            )
        )


def test_deterministic_proposals_are_outcome_blind_and_spend_exact_budget() -> None:
    request = _request()
    features = _feature_rows()
    losing_labels = ["SL_FIRST"] * len(features)
    winning_labels = ["TP_FIRST"] * len(features)

    # Labels deliberately remain separate because the context API accepts only
    # an exact feature projection. Changing every outcome cannot enter its hash.
    assert losing_labels != winning_labels
    losing_context = build_discovery_context(request, list(features))
    winning_context = build_discovery_context(request, list(reversed(features)))
    losing_batch = propose_deterministically(request, losing_context)
    winning_batch = propose_deterministically(request, winning_context)

    assert losing_context.sha256 == winning_context.sha256
    assert losing_batch.sha256 == winning_batch.sha256
    assert len(losing_batch.proposals) == request.proposal_budget
    assert losing_batch.as_dict()["status"] == FINAL_STATUS
    assert all(proposal.as_dict()["outcome_metrics"] is None for proposal in losing_batch.proposals)

    tainted = dict(features[0])
    tainted["outcome"] = "TP_FIRST"
    with pytest.raises(PatternDiscoveryError, match="feature-only projection"):
        build_discovery_context(request, [tainted])


@pytest.mark.parametrize(
    "mutation",
    ("query", "code", "unknown_threshold", "duplicate_key"),
)
def test_recorded_response_rejects_unsafe_or_noncanonical_output(mutation: str) -> None:
    request = _request(recorded=True, budget=2)
    context = build_discovery_context(request, _feature_rows())
    raw = _recorded_response(request, context.sha256)
    if mutation == "duplicate_key":
        raw = raw.replace(b'"artifact_schema":', b'"query":"select *","query":"again","artifact_schema":', 1)
    else:
        document = json.loads(raw)
        if mutation in {"query", "code"}:
            document["proposals"][0][mutation] = "file:///sealed/holdout"  # type: ignore[index]
        else:
            document["proposals"][0]["rule"]["all"][0]["threshold"] = 3  # type: ignore[index]
        raw = canonical_json_bytes(document)

    with pytest.raises(UnsafeRecordedResponseError):
        replay_recorded_response(request, context, raw)


def test_recorded_response_replay_is_byte_deterministic() -> None:
    request = _request(recorded=True, budget=2)
    context = build_discovery_context(request, _feature_rows())
    raw = _recorded_response(request, context.sha256)

    first = replay_recorded_response(request, context, raw)
    second = replay_recorded_response(request, context, bytes(raw))

    assert first.as_dict() == second.as_dict()
    assert first.sha256 == second.sha256
    assert first.recorded_response_sha256 is not None
    assert len(first.proposals) == request.proposal_budget

    rebuilt = recorded_response_for_patterns(
        request,
        context,
        (proposal.pattern for proposal in first.proposals),
    )
    rebuilt_batch = replay_recorded_response(request, context, rebuilt)
    assert [item.pattern for item in rebuilt_batch.proposals] == [
        item.pattern for item in first.proposals
    ]


def test_precommit_precedes_context_iteration_and_artifacts_verify(tmp_path: Path) -> None:
    request = _request()
    ledger_root = tmp_path / "proposal-ledger"
    artifact_root = tmp_path / "proposal-artifacts"
    rows = _feature_rows()

    class GuardedRows(Iterable[Mapping[str, object]]):
        def __iter__(self) -> Iterator[Mapping[str, object]]:
            events = ProposalLedger(ledger_root).verify()
            assert [event.event_type for event in events] == ["PRECOMMITTED"]
            yield from rows

    result = run_deterministic_pattern_discovery(
        ledger_root=ledger_root,
        artifact_root=artifact_root,
        request=request,
        feature_rows=GuardedRows(),
    )

    assert [event.event_type for event in ProposalLedger(ledger_root).verify()] == [
        "PRECOMMITTED",
        "CONTEXT_PUBLISHED",
        "COMPLETED",
    ]
    assert result.report.as_dict()["status"] == FINAL_STATUS
    assert result.report.as_dict()["m0b_epoch_registered"] is False
    assert result.report.as_dict()["database_mutated"] is False
    assert result.report.as_dict()["performance_evaluated"] is False
    for identity, expected in (
        (result.start.request_artifact, canonical_json_bytes(request.as_dict())),
        (result.start.context_artifact, canonical_json_bytes(result.start.context.as_dict())),
        (result.batch_artifact, canonical_json_bytes(result.batch.as_dict())),
        (result.report_artifact, canonical_json_bytes(result.report.as_dict())),
    ):
        assert verify_immutable_artifact(artifact_root, identity, expected_bytes=expected) == expected

    report_path = artifact_root / result.report_artifact.relative_uri
    report_path.chmod(0o644)
    with pytest.raises(ImmutableArtifactError, match="verification"):
        verify_immutable_artifact(artifact_root, result.report_artifact)


def test_recorded_run_publishes_response_before_replay_completion(tmp_path: Path) -> None:
    request = _request(recorded=True, budget=2)
    ledger_root = tmp_path / "proposal-ledger"
    artifact_root = tmp_path / "proposal-artifacts"
    start = begin_pattern_discovery(
        ledger_root=ledger_root,
        artifact_root=artifact_root,
        request=request,
        feature_rows=_feature_rows(),
    )
    raw = _recorded_response(request, start.context.sha256)

    result = complete_recorded_pattern_discovery(
        ledger_root=ledger_root,
        artifact_root=artifact_root,
        start=start,
        raw_response=raw,
    )

    assert [event.event_type for event in ProposalLedger(ledger_root).verify()] == [
        "PRECOMMITTED",
        "CONTEXT_PUBLISHED",
        "RESPONSE_RECORDED",
        "COMPLETED",
    ]
    assert result.recorded_response_artifact is not None
    assert verify_immutable_artifact(artifact_root, result.recorded_response_artifact) == raw
    assert result.report.as_dict()["status"] == FINAL_STATUS


def test_verified_context_start_preserves_precommit_order(tmp_path: Path) -> None:
    request = _request()
    context = build_discovery_context(request, _feature_rows())
    ledger_root = tmp_path / "proposal-ledger"
    artifact_root = tmp_path / "proposal-artifacts"

    start = begin_pattern_discovery_with_context(
        ledger_root=ledger_root,
        artifact_root=artifact_root,
        request=request,
        context=context,
    )

    assert start.context == context
    assert [event.event_type for event in ProposalLedger(ledger_root).verify()] == [
        "PRECOMMITTED",
        "CONTEXT_PUBLISHED",
    ]


def test_real_verified_morphology_context_projects_with_frozen_identity() -> None:
    from systematic_fx.research.ai_pattern_discovery import (
        context_from_ai_discovery_document,
    )

    project = Path(__file__).resolve().parents[2]
    digest = "a7219ac7c2a27f16cdbdfae58a9fe17c4d69372d315444987cc3605c4ff633a4"
    path = (
        project
        / "data/derived/bar_patterns/ai_discovery_context"
        / "identity_sha256=12de34f325b5788330401e10275cad8e06471d589f561b003387d220c25806cd"
        / f"sha256={digest}.json"
    )
    if not path.is_file():
        pytest.skip("verified real AI Discovery context is not materialized")
    request = ProposalRequest(
        request_key="real.discovery.adapter.v1",
        proposer_mode="DETERMINISTIC_OUTCOME_BLIND_V1",
        provider_id="SYSTEMATIC_FX_LOCAL",
        model_id="OUTCOME_BLIND_SUPPORT_STABILITY_DIVERSITY",
        model_version="v1",
        prompt_sha256=DETERMINISTIC_PROMPT_SHA256,
        source_feature_sha256=digest,
        source_feature_version="completed_5m_bar_morphology_v1",
        discovery_split_sha256=(
            "5594725f6769a706018d414a5b27e3903f1d7d1cc22c98e93b6e973ead1af043"
        ),
        source_interval_start="2022-01-03",
        source_interval_end="2023-07-10",
        max_source_rows=120_000,
        max_context_bins=120_000,
        proposal_budget=1,
        max_predicates_per_rule=3,
        minimum_support_rows=500,
        minimum_session_count=80,
        minimum_stability_ppm=0,
        maximum_pairwise_overlap_ppm=950_000,
        max_model_calls=0,
        max_input_tokens=0,
        max_output_tokens=0,
        max_response_bytes=0,
        deterministic_seed=20260813,
        precommitted_at_utc="2026-08-13T00:00:00Z",
        candidate_evaluation_budget=620,
        candidate_catalog_sha256=(
            "b5ab777126eace96858c57cf619a954195d19187902bc1b6fbf56b8e1ad90ef3"
        ),
        code_commit="1" * 40,
        proposer_implementation_sha256="2" * 64,
        dependency_lock_sha256="3" * 64,
    )

    context = context_from_ai_discovery_document(
        request,
        json.loads(path.read_bytes()),
        expected_context_sha256=digest,
    )

    assert context.source_row_count == 106_605
    assert len(context.session_row_counts) == 469
    assert len(context.bins) == 84_207
