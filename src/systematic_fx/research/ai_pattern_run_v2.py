"""Operational composition for the corrected direction-consistent proposal run."""

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
from systematic_fx.research.ai_pattern_config_v2 import (
    AI_PATTERN_V1_BATCH_SHA256,
    AI_PATTERN_V1_REQUEST_SHA256,
    AIPatternDiscoveryConfigV2,
    load_ai_pattern_discovery_config_v2,
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
    V2_REJECTION_REASON,
    DirectionalProposalBatch,
    directional_proposal_precommit_document,
    propose_deterministically_v2,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256

AI_PATTERN_RUN_V2_SCHEMA: Final = "systematic_fx.ai_pattern_operational_run.v2"
DEFAULT_AI_PATTERN_V2_ROOT: Final = Path("data/derived/bar_patterns/ai_pattern_discovery_v2")
_WRITE_BITS: Final = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH


class AIPatternRunV2Error(RuntimeError):
    """The corrected proposal run or durable replay failed closed."""


@dataclass(frozen=True, slots=True)
class AIPatternOperationalRunV2:
    config: AIPatternDiscoveryConfigV2
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
            "rejected_directionless_candidate_count": (V2_FILTERED_DIRECTIONLESS_RANGE_COUNT),
            "rejection_reason": V2_REJECTION_REASON,
            "report_artifact": self.report_artifact.as_dict(),
            "request_artifact": self.request_artifact.as_dict(),
            "schema": AI_PATTERN_RUN_V2_SCHEMA,
            "sealed_holdout_untouched": True,
            "status": FINAL_STATUS,
            "supersedes_batch_sha256": AI_PATTERN_V1_BATCH_SHA256,
            "supersedes_request_sha256": AI_PATTERN_V1_REQUEST_SHA256,
            "support_eligible_count": base.support_eligible_count,
            "walk_forward_untouched": True,
        }


def _project_root(value: Path | str) -> Path:
    root = Path(value).expanduser().resolve(strict=True)
    if not root.is_dir() or not (root / "pyproject.toml").is_file():
        raise AIPatternRunV2Error("project root is not a systematic-fx checkout")
    return root


def _fixed_run_root(project_root: Path, *, create: bool) -> Path:
    expected = project_root / DEFAULT_AI_PATTERN_V2_ROOT
    current = project_root
    for part in DEFAULT_AI_PATTERN_V2_ROOT.parts:
        current = current / part
        if current.is_symlink():
            raise AIPatternRunV2Error("AI proposal v2 root has a symbolic-link component")
    if create:
        expected.mkdir(parents=True, exist_ok=True)
    try:
        resolved = expected.resolve(strict=True)
    except FileNotFoundError as error:
        raise AIPatternRunV2Error("AI proposal v2 run does not exist") from error
    if resolved != expected.absolute() or not resolved.is_dir():
        raise AIPatternRunV2Error("AI proposal v2 run root is unsafe")
    return resolved


def _report_document(
    config: AIPatternDiscoveryConfigV2,
    context: DiscoveryContext,
    batch: DirectionalProposalBatch,
    request_artifact: ArtifactIdentity,
    context_artifact: ArtifactIdentity,
    batch_artifact: ArtifactIdentity,
) -> dict[str, object]:
    return {
        "artifact_schema": "systematic_fx.ai_pattern_directional_discovery_report.v2",
        "authority": "PROPOSE_CANDIDATES_ONLY",
        "base_request_sha256": config.request.sha256,
        "batch_artifact": batch_artifact.as_dict(),
        "batch_sha256": batch.sha256,
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
        "request_artifact": request_artifact.as_dict(),
        "sealed_holdout_untouched": True,
        "status": FINAL_STATUS,
        "superseded_v1": {
            "batch_sha256": AI_PATTERN_V1_BATCH_SHA256,
            "reason": V2_REJECTION_REASON,
            "rejected_candidate_count": V2_FILTERED_DIRECTIONLESS_RANGE_COUNT,
            "request_sha256": AI_PATTERN_V1_REQUEST_SHA256,
        },
    }


def _start_after_precommit(
    *,
    project_root: Path,
    run_root: Path,
    config: AIPatternDiscoveryConfigV2,
) -> tuple[
    DiscoveryContext,
    ArtifactIdentity,
    ArtifactIdentity,
    str,
]:
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
        raise AIPatternRunV2Error("directional precommit artifact identity differs")
    ledger._append(
        "PRECOMMITTED",
        governed_request_sha256,
        {"request_artifact": request_artifact.as_dict()},
    )
    try:
        # Market-derived context is not opened until the governed v2 request is durable.
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
            raise AIPatternRunV2Error("Discovery proposal v2 context differs")
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
        ledger._append(
            "FAILED",
            governed_request_sha256,
            {"failure_code": type(error).__name__},
        )
        raise
    return context, request_artifact, context_artifact, governed_request_sha256


def run_ai_pattern_research_v2(project_root: Path | str) -> AIPatternOperationalRunV2:
    """Run exactly one corrected, outcome-blind, direction-consistent proposal batch."""

    root = _project_root(project_root)
    run_root = _fixed_run_root(root, create=True)
    config = load_ai_pattern_discovery_config_v2(root)
    ledger = ProposalLedger(run_root / "ledger")
    if ledger.verify():
        raise AIPatternRunV2Error(
            "AI proposal v2 run already exists; verify it instead of expanding the budget"
        )
    context, request_artifact, context_artifact, governed_request_sha256 = _start_after_precommit(
        project_root=root, run_root=run_root, config=config
    )
    try:
        batch = propose_deterministically_v2(
            config.request,
            context,
            envelope=config.envelope,
        )
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
        ledger._append(
            "FAILED",
            governed_request_sha256,
            {"failure_code": type(error).__name__},
        )
        raise
    run = AIPatternOperationalRunV2(
        config,
        context,
        batch,
        request_artifact,
        context_artifact,
        batch_artifact,
        report_artifact,
        run_root,
    )
    verify_ai_pattern_research_v2(root, run=run)
    return run


def _identity(value: object) -> ArtifactIdentity:
    return ArtifactIdentity.from_dict(value)


def _exact_event_payload(payload: Mapping[str, object], keys: set[str]) -> None:
    if set(payload) != keys:
        raise AIPatternRunV2Error("proposal v2 ledger payload differs")


def verify_ai_pattern_research_v2(
    project_root: Path | str,
    *,
    run: AIPatternOperationalRunV2 | None = None,
) -> AIPatternOperationalRunV2:
    """Read-only reopen and byte-for-byte reconstruction of the corrected batch."""

    root = _project_root(project_root)
    run_root = _fixed_run_root(root, create=False)
    config = load_ai_pattern_discovery_config_v2(root)
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
        raise AIPatternRunV2Error("AI proposal v2 ledger does not exist or is unsafe")
    precommit = directional_proposal_precommit_document(config.request, config.envelope)
    governed_request_sha256 = canonical_sha256(precommit)
    events = ProposalLedger(ledger_root).verify()
    if [event.event_type for event in events] != [
        "PRECOMMITTED",
        "CONTEXT_PUBLISHED",
        "COMPLETED",
    ] or any(event.request_sha256 != governed_request_sha256 for event in events):
        raise AIPatternRunV2Error("proposal v2 ledger lifecycle differs")

    _exact_event_payload(events[0].payload, {"request_artifact"})
    request_artifact = _identity(events[0].payload["request_artifact"])
    _exact_event_payload(
        events[1].payload,
        {
            "base_request_sha256",
            "context_artifact",
            "directional_envelope_sha256",
        },
    )
    if (
        events[1].payload["base_request_sha256"] != config.request.sha256
        or events[1].payload["directional_envelope_sha256"] != config.envelope.sha256
    ):
        raise AIPatternRunV2Error("proposal v2 context lineage differs")
    context_artifact = _identity(events[1].payload["context_artifact"])
    _exact_event_payload(events[2].payload, {"batch_artifact", "report_artifact", "status"})
    if events[2].payload["status"] != FINAL_STATUS:
        raise AIPatternRunV2Error("proposal v2 status differs")
    batch_artifact = _identity(events[2].payload["batch_artifact"])
    report_artifact = _identity(events[2].payload["report_artifact"])
    if (
        request_artifact.artifact_type != "AI_PATTERN_DIRECTIONAL_PROPOSAL_REQUEST"
        or context_artifact.artifact_type != "AI_PATTERN_DISCOVERY_CONTEXT"
        or batch_artifact.artifact_type != "AI_PATTERN_DIRECTIONAL_PROPOSAL_BATCH"
        or report_artifact.artifact_type != "AI_PATTERN_DIRECTIONAL_DISCOVERY_REPORT"
    ):
        raise AIPatternRunV2Error("proposal v2 artifact roles differ")
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
    batch = propose_deterministically_v2(
        config.request,
        context,
        envelope=config.envelope,
    )
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
    rebuilt = AIPatternOperationalRunV2(
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
        raise AIPatternRunV2Error("in-memory proposal v2 run differs from durable replay")
    return rebuilt


def render_ai_pattern_research_report_v2(run: AIPatternOperationalRunV2) -> str:
    """Render the corrected proposal-only batch without performance language."""

    data = run.as_dict()
    lines = [
        "# Autonomous AI Pattern Discovery — Directional Proposal Batch 2",
        "",
        f"- Status: `{data['status']}`",
        "- Evidence ceiling: direction-consistent hypothesis proposals only.",
        (
            f"- Corrected finite search: {data['candidate_universe_count']} rules; "
            f"{data['proposal_count']} accepted proposals."
        ),
        (
            f"- Superseded Batch 1 after rejecting "
            f"{data['rejected_directionless_candidate_count']} directionless candidates."
        ),
        f"- Context SHA-256: `{data['context_sha256']}`",
        f"- Directional batch SHA-256: `{data['proposal_batch_sha256']}`",
        "- Database / walk-forward / sealed holdout: untouched.",
        "",
        "## Proposed patterns",
        "",
    ]
    for proposal in data["proposals"]:
        rule = " AND ".join(
            f"{item['feature']} {item['operator']} {item['threshold']}"
            for item in proposal["rule"]["all"]
        )
        lines.extend(
            [
                (
                    f"{proposal['selection_rank']}. **{proposal['family']} / "
                    f"{proposal['direction']}** — `{rule}`"
                ),
                (
                    f"   Support: {proposal['support_rows']:,} bars; "
                    f"active days: {proposal['session_support_count']}; "
                    f"stability: {proposal['stability_ppm'] / 10_000:.2f}%."
                ),
            ]
        )
    lines.extend(
        [
            "",
            (
                "These are occurrence hypotheses, not alpha or performance evidence. "
                "Evaluation remains blocked until research-eligible evidence exists."
            ),
            "",
        ]
    )
    return "\n".join(lines)


def publish_ai_pattern_markdown_report_v2(
    project_root: Path | str,
    run: AIPatternOperationalRunV2,
) -> Path:
    """Publish the human-readable Batch 2 view; governed JSON remains immutable."""

    root = _project_root(project_root)
    output = root / "reports/generated/ai_pattern_discovery_batch_2.md"
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp-{os.getpid()}")
    temporary.write_text(render_ai_pattern_research_report_v2(run), encoding="utf-8")
    os.replace(temporary, output)
    if output.stat().st_mode & _WRITE_BITS == 0:
        output.chmod(0o644)
    return output


__all__ = [
    "AI_PATTERN_RUN_V2_SCHEMA",
    "DEFAULT_AI_PATTERN_V2_ROOT",
    "AIPatternOperationalRunV2",
    "AIPatternRunV2Error",
    "publish_ai_pattern_markdown_report_v2",
    "render_ai_pattern_research_report_v2",
    "run_ai_pattern_research_v2",
    "verify_ai_pattern_research_v2",
]
