"""Commit-reconstructible operational composition for proposal Batch 3."""

from __future__ import annotations

import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from systematic_fx.research.ai_discovery_context import (
    EXPECTED_AI_DISCOVERY_CONTEXT_SHA256,
    load_ai_discovery_context,
    reopen_ai_discovery_context,
)
from systematic_fx.research.ai_pattern_config_v3 import (
    AI_PATTERN_V2_BATCH_SHA256,
    AI_PATTERN_V2_GOVERNED_REQUEST_SHA256,
    V3_CORRECTION_REASON,
    AIPatternDiscoveryConfigV3,
    load_ai_pattern_discovery_config_v3,
)
from systematic_fx.research.ai_pattern_discovery import (
    FINAL_STATUS,
    ArtifactIdentity,
    DiscoveryContext,
    ProposalLedger,
    context_from_ai_discovery_document,
    publish_canonical_artifact,
    verify_immutable_artifact,
)
from systematic_fx.research.ai_pattern_discovery_v2 import (
    V2_FILTERED_DIRECTIONLESS_RANGE_COUNT,
    DirectionalProposalBatch,
    directional_proposal_precommit_document,
    propose_deterministically_v2,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256

AI_PATTERN_RUN_V3_SCHEMA: Final = "systematic_fx.ai_pattern_operational_run.v3"
DEFAULT_AI_PATTERN_V3_ROOT: Final = Path("data/derived/bar_patterns/ai_pattern_discovery_v3")
_WRITE_BITS: Final = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH


class AIPatternRunV3Error(RuntimeError):
    """The reconstructible third proposal run or durable replay failed closed."""


@dataclass(frozen=True, slots=True)
class AIPatternOperationalRunV3:
    config: AIPatternDiscoveryConfigV3
    context: DiscoveryContext
    batch: DirectionalProposalBatch
    request_artifact: ArtifactIdentity
    context_artifact: ArtifactIdentity
    batch_artifact: ArtifactIdentity
    report_artifact: ArtifactIdentity
    root: Path

    @property
    def governed_request_sha256(self) -> str:
        return canonical_sha256(
            directional_proposal_precommit_document(
                self.config.request,
                self.config.envelope,
            )
        )

    def as_dict(self) -> dict[str, object]:
        base = self.batch.base_batch
        return {
            "base_request_sha256": self.config.request.sha256,
            "batch_artifact": self.batch_artifact.as_dict(),
            "candidate_universe_count": base.candidate_universe_count,
            "code_commit": self.config.request.code_commit,
            "config_file_sha256": self.config.file_sha256,
            "config_semantic_sha256": self.config.semantic_sha256,
            "context_sha256": self.context.sha256,
            "database_mutated": False,
            "directional_envelope_sha256": self.config.envelope.sha256,
            "diversity_rejected_count": base.diversity_rejected_count,
            "governed_request_sha256": self.governed_request_sha256,
            "ledger_event_count": 3,
            "m0b_epoch_registered": False,
            "performance_evaluated": False,
            "proposal_batch_sha256": self.batch.sha256,
            "proposal_count": len(base.proposals),
            "proposals": [
                {
                    "direction": item.pattern.direction,
                    "family": item.pattern.family,
                    "proposal_sha256": item.sha256,
                    "rule": item.pattern.rule.as_dict(),
                    "selection_rank": item.selection_rank,
                    "session_support_count": item.session_support_count,
                    "stability_ppm": item.stability_ppm,
                    "support_ppm": item.support_ppm,
                    "support_rows": item.support_rows,
                }
                for item in base.proposals
            ],
            "provenance_correction": V3_CORRECTION_REASON,
            "rejected_directionless_candidate_count": V2_FILTERED_DIRECTIONLESS_RANGE_COUNT,
            "report_artifact": self.report_artifact.as_dict(),
            "request_artifact": self.request_artifact.as_dict(),
            "schema": AI_PATTERN_RUN_V3_SCHEMA,
            "sealed_holdout_untouched": True,
            "status": FINAL_STATUS,
            "supersedes_batch_sha256": AI_PATTERN_V2_BATCH_SHA256,
            "supersedes_governed_request_sha256": AI_PATTERN_V2_GOVERNED_REQUEST_SHA256,
            "support_eligible_count": base.support_eligible_count,
            "walk_forward_untouched": True,
        }


def _project_root(value: Path | str) -> Path:
    root = Path(value).expanduser().resolve(strict=True)
    if not root.is_dir() or not (root / "pyproject.toml").is_file():
        raise AIPatternRunV3Error("project root is not a systematic-fx checkout")
    return root


def _fixed_run_root(project_root: Path, *, create: bool) -> Path:
    expected = project_root / DEFAULT_AI_PATTERN_V3_ROOT
    current = project_root
    for part in DEFAULT_AI_PATTERN_V3_ROOT.parts:
        current = current / part
        if current.is_symlink():
            raise AIPatternRunV3Error("AI proposal v3 root has a symbolic-link component")
    if create:
        expected.mkdir(parents=True, exist_ok=True)
    try:
        resolved = expected.resolve(strict=True)
    except FileNotFoundError as error:
        raise AIPatternRunV3Error("AI proposal v3 run does not exist") from error
    if resolved != expected.absolute() or not resolved.is_dir():
        raise AIPatternRunV3Error("AI proposal v3 run root is unsafe")
    return resolved


def _report_document(
    config: AIPatternDiscoveryConfigV3,
    context: DiscoveryContext,
    batch: DirectionalProposalBatch,
    request_artifact: ArtifactIdentity,
    context_artifact: ArtifactIdentity,
    batch_artifact: ArtifactIdentity,
) -> dict[str, object]:
    return {
        "artifact_schema": "systematic_fx.ai_pattern_directional_discovery_report.v3",
        "authority": "PROPOSE_CANDIDATES_ONLY",
        "base_request_sha256": config.request.sha256,
        "batch_artifact": batch_artifact.as_dict(),
        "batch_sha256": batch.sha256,
        "code_commit": config.request.code_commit,
        "context_artifact": context_artifact.as_dict(),
        "context_sha256": context.sha256,
        "database_mutated": False,
        "directional_envelope_sha256": config.envelope.sha256,
        "limitations": [
            "DISCOVERY_FEATURES_ONLY",
            "NO_LABEL_OR_OUTCOME_ACCESS",
            "NO_PERFORMANCE_EVALUATION",
            "NO_M0B_EPOCH_OR_DATABASE_REGISTRATION",
            "NO_WALK_FORWARD_OR_HOLDOUT_ACCESS",
            "NO_PAPER_LIVE_OR_PROMOTION_AUTHORITY",
        ],
        "m0b_epoch_registered": False,
        "performance_evaluated": False,
        "provenance_correction": V3_CORRECTION_REASON,
        "request_artifact": request_artifact.as_dict(),
        "sealed_holdout_untouched": True,
        "status": FINAL_STATUS,
        "supersedes": {
            "batch_sha256": AI_PATTERN_V2_BATCH_SHA256,
            "governed_request_sha256": AI_PATTERN_V2_GOVERNED_REQUEST_SHA256,
        },
        "walk_forward_untouched": True,
    }


def _start_after_precommit(
    *,
    project_root: Path,
    run_root: Path,
    config: AIPatternDiscoveryConfigV3,
) -> tuple[DiscoveryContext, ArtifactIdentity, ArtifactIdentity, str]:
    ledger = ProposalLedger(run_root / "ledger")
    artifacts = run_root / "artifacts"
    precommit = directional_proposal_precommit_document(config.request, config.envelope)
    governed_request_sha256 = canonical_sha256(precommit)
    request_artifact = publish_canonical_artifact(
        artifacts,
        artifact_type="AI_PATTERN_DIRECTIONAL_PROPOSAL_REQUEST",
        filename_prefix="directional-proposal-request",
        document=precommit,
    )
    if request_artifact.content_sha256 != governed_request_sha256:
        raise AIPatternRunV3Error("directional precommit artifact identity differs")
    ledger._append(
        "PRECOMMITTED",
        governed_request_sha256,
        {"request_artifact": request_artifact.as_dict()},
    )
    try:
        # The commit-reconstructible request is durable before market context is opened.
        source_artifact = load_ai_discovery_context(project_root)
        source_document = reopen_ai_discovery_context(project_root, source_artifact)
        context = context_from_ai_discovery_document(
            config.request,
            source_document,
            expected_context_sha256=source_artifact.sha256,
        )
        if (
            source_artifact.sha256 != EXPECTED_AI_DISCOVERY_CONTEXT_SHA256
            or context.source_row_count != config.decision_bar_rows
            or len(context.bins) != config.expected_context_bins
            or len(context.session_row_counts) != config.decision_active_days
        ):
            raise AIPatternRunV3Error("Discovery proposal v3 context differs")
        context_artifact = publish_canonical_artifact(
            artifacts,
            artifact_type="AI_PATTERN_DISCOVERY_CONTEXT",
            filename_prefix="discovery-context",
            document=context.as_dict(),
        )
        ledger._append(
            "CONTEXT_PUBLISHED",
            governed_request_sha256,
            {
                "base_request_sha256": config.request.sha256,
                "context_artifact": context_artifact.as_dict(),
                "directional_envelope_sha256": config.envelope.sha256,
            },
        )
    except Exception as error:
        ledger._append("FAILED", governed_request_sha256, {"failure_code": type(error).__name__})
        raise
    return context, request_artifact, context_artifact, governed_request_sha256


def run_ai_pattern_research_v3(project_root: Path | str) -> AIPatternOperationalRunV3:
    """Run one finite proposal batch whose complete runtime exists in its Git commit."""

    root = _project_root(project_root)
    run_root = _fixed_run_root(root, create=True)
    config = load_ai_pattern_discovery_config_v3(root)
    ledger = ProposalLedger(run_root / "ledger")
    if ledger.verify():
        raise AIPatternRunV3Error(
            "AI proposal v3 run already exists; verify it instead of expanding the budget"
        )
    context, request_artifact, context_artifact, governed_request_sha256 = _start_after_precommit(
        project_root=root, run_root=run_root, config=config
    )
    try:
        batch = propose_deterministically_v2(config.request, context, envelope=config.envelope)
        batch_artifact = publish_canonical_artifact(
            run_root / "artifacts",
            artifact_type="AI_PATTERN_DIRECTIONAL_PROPOSAL_BATCH",
            filename_prefix="directional-proposal-batch",
            document=batch.as_dict(),
        )
        report = _report_document(
            config,
            context,
            batch,
            request_artifact,
            context_artifact,
            batch_artifact,
        )
        report_artifact = publish_canonical_artifact(
            run_root / "artifacts",
            artifact_type="AI_PATTERN_DIRECTIONAL_DISCOVERY_REPORT",
            filename_prefix="directional-proposal-report",
            document=report,
        )
        ledger._append(
            "COMPLETED",
            governed_request_sha256,
            {
                "batch_artifact": batch_artifact.as_dict(),
                "report_artifact": report_artifact.as_dict(),
                "status": FINAL_STATUS,
            },
        )
    except Exception as error:
        ledger._append("FAILED", governed_request_sha256, {"failure_code": type(error).__name__})
        raise
    run = AIPatternOperationalRunV3(
        config,
        context,
        batch,
        request_artifact,
        context_artifact,
        batch_artifact,
        report_artifact,
        run_root,
    )
    verify_ai_pattern_research_v3(root, run=run)
    return run


def _identity(value: object) -> ArtifactIdentity:
    return ArtifactIdentity.from_dict(value)


def _exact_event_payload(payload: Mapping[str, object], keys: set[str]) -> None:
    if set(payload) != keys:
        raise AIPatternRunV3Error("proposal v3 ledger payload differs")


def verify_ai_pattern_research_v3(
    project_root: Path | str,
    *,
    run: AIPatternOperationalRunV3 | None = None,
) -> AIPatternOperationalRunV3:
    """Read-only reconstruct Batch 3 from its committed runtime and immutable inputs."""

    root = _project_root(project_root)
    run_root = _fixed_run_root(root, create=False)
    config = load_ai_pattern_discovery_config_v3(root)
    ledger_root = run_root / "ledger"
    artifacts_root = run_root / "artifacts"
    events_root = ledger_root / "events"
    if (
        ledger_root.is_symlink()
        or artifacts_root.is_symlink()
        or events_root.is_symlink()
        or not ledger_root.is_dir()
        or not artifacts_root.is_dir()
        or not events_root.is_dir()
    ):
        raise AIPatternRunV3Error("AI proposal v3 ledger does not exist or is unsafe")
    precommit = directional_proposal_precommit_document(config.request, config.envelope)
    governed_request_sha256 = canonical_sha256(precommit)
    events = ProposalLedger(ledger_root).verify()
    if [event.event_type for event in events] != [
        "PRECOMMITTED",
        "CONTEXT_PUBLISHED",
        "COMPLETED",
    ] or any(event.request_sha256 != governed_request_sha256 for event in events):
        raise AIPatternRunV3Error("proposal v3 ledger lifecycle differs")
    _exact_event_payload(events[0].payload, {"request_artifact"})
    request_artifact = _identity(events[0].payload["request_artifact"])
    _exact_event_payload(
        events[1].payload,
        {"base_request_sha256", "context_artifact", "directional_envelope_sha256"},
    )
    if (
        events[1].payload["base_request_sha256"] != config.request.sha256
        or events[1].payload["directional_envelope_sha256"] != config.envelope.sha256
    ):
        raise AIPatternRunV3Error("proposal v3 context lineage differs")
    context_artifact = _identity(events[1].payload["context_artifact"])
    _exact_event_payload(events[2].payload, {"batch_artifact", "report_artifact", "status"})
    if events[2].payload["status"] != FINAL_STATUS:
        raise AIPatternRunV3Error("proposal v3 status differs")
    batch_artifact = _identity(events[2].payload["batch_artifact"])
    report_artifact = _identity(events[2].payload["report_artifact"])
    if (
        request_artifact.artifact_type != "AI_PATTERN_DIRECTIONAL_PROPOSAL_REQUEST"
        or context_artifact.artifact_type != "AI_PATTERN_DISCOVERY_CONTEXT"
        or batch_artifact.artifact_type != "AI_PATTERN_DIRECTIONAL_PROPOSAL_BATCH"
        or report_artifact.artifact_type != "AI_PATTERN_DIRECTIONAL_DISCOVERY_REPORT"
    ):
        raise AIPatternRunV3Error("proposal v3 artifact roles differ")
    verify_immutable_artifact(
        artifacts_root,
        request_artifact,
        expected_bytes=canonical_json_bytes(precommit),
    )
    source_artifact = load_ai_discovery_context(root)
    source_document = reopen_ai_discovery_context(root, source_artifact)
    context = context_from_ai_discovery_document(
        config.request,
        source_document,
        expected_context_sha256=source_artifact.sha256,
    )
    verify_immutable_artifact(
        artifacts_root,
        context_artifact,
        expected_bytes=canonical_json_bytes(context.as_dict()),
    )
    batch = propose_deterministically_v2(config.request, context, envelope=config.envelope)
    verify_immutable_artifact(
        artifacts_root,
        batch_artifact,
        expected_bytes=canonical_json_bytes(batch.as_dict()),
    )
    report = _report_document(
        config,
        context,
        batch,
        request_artifact,
        context_artifact,
        batch_artifact,
    )
    verify_immutable_artifact(
        artifacts_root,
        report_artifact,
        expected_bytes=canonical_json_bytes(report),
    )
    rebuilt = AIPatternOperationalRunV3(
        config,
        context,
        batch,
        request_artifact,
        context_artifact,
        batch_artifact,
        report_artifact,
        run_root,
    )
    if run is not None and run.as_dict() != rebuilt.as_dict():
        raise AIPatternRunV3Error("in-memory proposal v3 run differs from durable replay")
    return rebuilt


def render_ai_pattern_research_report_v3(run: AIPatternOperationalRunV3) -> str:
    """Render the governed proposal-only result without performance language."""

    data = run.as_dict()
    lines = [
        "# AI Pattern Discovery Batch 3",
        "",
        "Commit-reconstructible, outcome-blind Discovery feature research.",
        "",
        f"- Status: `{data['status']}`",
        f"- Code commit: `{data['code_commit']}`",
        f"- Governed request SHA-256: `{data['governed_request_sha256']}`",
        f"- Directional batch SHA-256: `{data['proposal_batch_sha256']}`",
        f"- Candidate catalog: {data['candidate_universe_count']}",
        f"- Support-eligible candidates: {data['support_eligible_count']}",
        f"- Frozen hypotheses: {data['proposal_count']}",
        "- Performance evaluated: no",
        "- Walk-forward/holdout touched: no",
        "- M0b epoch or persistent DB mutated: no",
        "",
        "## Frozen hypotheses",
        "",
    ]
    for proposal in data["proposals"]:
        lines.append(
            f"{proposal['selection_rank']}. {proposal['family']} / {proposal['direction']} "
            f"— support {proposal['support_rows']} bars across "
            f"{proposal['session_support_count']} days"
        )
    lines.extend(
        [
            "",
            "These are occurrence hypotheses, not alpha, PnL, significance, or promotion evidence.",
            "",
        ]
    )
    return "\n".join(lines)


def publish_ai_pattern_markdown_report_v3(
    project_root: Path | str,
    run: AIPatternOperationalRunV3,
) -> Path:
    """Publish a convenience report outside the governed immutable evidence."""

    root = _project_root(project_root)
    output = root / "reports/generated/ai_pattern_discovery_batch_3.md"
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".md.tmp")
    temporary.write_text(render_ai_pattern_research_report_v3(run), encoding="utf-8")
    os.replace(temporary, output)
    return output


__all__ = [
    "AI_PATTERN_RUN_V3_SCHEMA",
    "DEFAULT_AI_PATTERN_V3_ROOT",
    "AIPatternOperationalRunV3",
    "AIPatternRunV3Error",
    "publish_ai_pattern_markdown_report_v3",
    "render_ai_pattern_research_report_v3",
    "run_ai_pattern_research_v3",
    "verify_ai_pattern_research_v3",
]
