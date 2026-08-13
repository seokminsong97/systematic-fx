from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from systematic_fx import cli
from systematic_fx.research.ai_pattern_config import (
    AIPatternDiscoveryConfig,
    load_ai_pattern_discovery_config,
)
from systematic_fx.research.ai_pattern_discovery import (
    FINAL_STATUS,
    ProposalLedger,
    ProposalRequest,
    ProposalRunResult,
)
from systematic_fx.research.ai_pattern_run import (
    AIPatternOperationalRun,
    _start_after_precommit,
    render_ai_pattern_research_report,
)

ROOT = Path(__file__).resolve().parents[2]


def _config(request: ProposalRequest) -> AIPatternDiscoveryConfig:
    return AIPatternDiscoveryConfig(
        path=ROOT / "configs/research/ai_pattern_discovery_v1.toml",
        file_sha256="a" * 64,
        semantic_sha256="b" * 64,
        request=request,
        context_identity_sha256="c" * 64,
        visible_active_days=489,
        decision_active_days=469,
        source_bar_rows=111_297,
        decision_bar_rows=106_605,
        expected_context_bins=84_207,
    )


def test_cli_exposes_run_and_verify_without_database_arguments() -> None:
    parser = cli.build_parser()
    run = parser.parse_args(["research", "ai-pattern", "run", "--json"])
    verify = parser.parse_args(["research", "ai-pattern", "verify"])

    assert run.ai_pattern_action == "run"
    assert run.json is True
    assert verify.ai_pattern_action == "verify"
    assert not hasattr(run, "database_url")


def test_source_open_occurs_only_after_precommit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = replace(
        _synthetic_request(),
        source_feature_sha256="d" * 64,
        source_feature_version="completed_5m_bar_morphology_v1",
        discovery_split_sha256="e" * 64,
        source_interval_start="2022-01-03",
        source_interval_end="2023-07-10",
    )
    config = _config(base)
    observed: list[str] = []

    def load(_: Path) -> object:
        observed.extend(item.event_type for item in ProposalLedger(tmp_path / "ledger").verify())
        raise RuntimeError("deliberate source stop")

    monkeypatch.setattr("systematic_fx.research.ai_pattern_run.load_ai_discovery_context", load)
    with pytest.raises(RuntimeError, match="deliberate"):
        _start_after_precommit(project_root=ROOT, run_root=tmp_path, config=config)

    assert observed == ["PRECOMMITTED"]
    assert [item.event_type for item in ProposalLedger(tmp_path / "ledger").verify()] == [
        "PRECOMMITTED",
        "FAILED",
    ]


def _synthetic_request() -> ProposalRequest:
    from systematic_fx.research.ai_pattern_discovery import DETERMINISTIC_PROMPT_SHA256

    return ProposalRequest(
        request_key="synthetic.run.v1",
        proposer_mode="DETERMINISTIC_OUTCOME_BLIND_V1",
        provider_id="SYSTEMATIC_FX_LOCAL",
        model_id="OUTCOME_BLIND_SUPPORT_STABILITY_DIVERSITY",
        model_version="v1",
        prompt_sha256=DETERMINISTIC_PROMPT_SHA256,
        source_feature_sha256="1" * 64,
        source_feature_version="synthetic_v1",
        discovery_split_sha256="2" * 64,
        source_interval_start="2022-01-01",
        source_interval_end="2022-01-02",
        max_source_rows=100,
        max_context_bins=100,
        proposal_budget=1,
        max_predicates_per_rule=3,
        minimum_support_rows=1,
        minimum_session_count=1,
        minimum_stability_ppm=0,
        maximum_pairwise_overlap_ppm=1_000_000,
        max_model_calls=0,
        max_input_tokens=0,
        max_output_tokens=0,
        max_response_bytes=0,
        deterministic_seed=1,
        precommitted_at_utc="2026-08-13T00:00:00Z",
        candidate_evaluation_budget=620,
        candidate_catalog_sha256=(
            "b5ab777126eace96858c57cf619a954195d19187902bc1b6fbf56b8e1ad90ef3"
        ),
        code_commit="3" * 40,
        proposer_implementation_sha256="4" * 64,
        dependency_lock_sha256="5" * 64,
    )


def test_human_report_never_claims_performance() -> None:
    request = _synthetic_request()
    from systematic_fx.research.ai_pattern_discovery import (
        ArtifactIdentity,
        DiscoveryContext,
        DiscoveryVectorBin,
        ProposalRunReport,
        ProposalRunStart,
        propose_deterministically,
    )

    context = DiscoveryContext(
        request.sha256,
        request.source_feature_sha256,
        request.source_feature_version,
        request.discovery_split_sha256,
        request.source_interval_start,
        request.source_interval_end,
        1,
        (("S1", 1),),
        ((DiscoveryVectorBin("S1", (4, 250_000, 250_000, 700_000, 0, 0), 1)),),
    )
    batch = propose_deterministically(request, context)
    request_artifact = ArtifactIdentity("AI_PATTERN_PROPOSAL_REQUEST", request.sha256, 1, "r")
    context_artifact = ArtifactIdentity("AI_PATTERN_DISCOVERY_CONTEXT", context.sha256, 1, "c")
    batch_artifact = ArtifactIdentity("AI_PATTERN_PROPOSAL_BATCH", batch.sha256, 1, "b")
    report = ProposalRunReport(
        request.sha256,
        request_artifact,
        context.sha256,
        context_artifact,
        batch.sha256,
        batch_artifact,
        None,
    )
    result = ProposalRunResult(
        ProposalRunStart(request, request_artifact, context, context_artifact),
        batch,
        batch_artifact,
        report,
        ArtifactIdentity("AI_PATTERN_DISCOVERY_REPORT", report.sha256, 1, "p"),
        None,
    )
    run = AIPatternOperationalRun(_config(request), result, ROOT)

    content = render_ai_pattern_research_report(run)
    assert FINAL_STATUS in content
    assert "not alpha evidence" in content
    assert "Database / walk-forward / sealed holdout: untouched" in content


def test_checked_in_config_loads_after_provenance_is_pinned() -> None:
    config = load_ai_pattern_discovery_config(ROOT)
    assert config.request.candidate_evaluation_budget == 620
    assert config.request.proposal_budget == 12
