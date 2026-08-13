"""Operational composition for one bounded autonomous proposal-only research run."""

from __future__ import annotations

import json
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
from systematic_fx.research.ai_pattern_config import (
    AIPatternDiscoveryConfig,
    load_ai_pattern_discovery_config,
)
from systematic_fx.research.ai_pattern_discovery import (
    FINAL_STATUS,
    ArtifactIdentity,
    ProposalLedger,
    ProposalRunResult,
    ProposalRunStart,
    complete_deterministic_pattern_discovery,
    context_from_ai_discovery_document,
    propose_deterministically,
    publish_canonical_artifact,
    verify_immutable_artifact,
)
from systematic_fx.research.hypotheses import canonical_json_bytes

AI_PATTERN_RUN_SCHEMA: Final = "systematic_fx.ai_pattern_operational_run.v1"
DEFAULT_AI_PATTERN_ROOT: Final = Path("data/derived/bar_patterns/ai_pattern_discovery_v1")
_WRITE_BITS: Final = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH


class AIPatternRunError(RuntimeError):
    """The autonomous proposal run or its durable replay failed closed."""


@dataclass(frozen=True, slots=True)
class AIPatternOperationalRun:
    config: AIPatternDiscoveryConfig
    result: ProposalRunResult
    root: Path

    def as_dict(self) -> dict[str, object]:
        batch = self.result.batch
        return {
            "batch_artifact": self.result.batch_artifact.as_dict(),
            "candidate_universe_count": batch.candidate_universe_count,
            "config_file_sha256": self.config.file_sha256,
            "config_semantic_sha256": self.config.semantic_sha256,
            "context_sha256": batch.context_sha256,
            "database_mutated": False,
            "diversity_rejected_count": batch.diversity_rejected_count,
            "ledger_event_count": 3,
            "m0b_epoch_registered": False,
            "performance_evaluated": False,
            "proposal_batch_sha256": batch.sha256,
            "proposal_count": len(batch.proposals),
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
                for item in batch.proposals
            ],
            "report_artifact": self.result.report_artifact.as_dict(),
            "request_sha256": self.config.request.sha256,
            "schema": AI_PATTERN_RUN_SCHEMA,
            "sealed_holdout_untouched": True,
            "status": FINAL_STATUS,
            "support_eligible_count": batch.support_eligible_count,
            "walk_forward_untouched": True,
        }


def _project_root(value: Path | str) -> Path:
    root = Path(value).expanduser().resolve(strict=True)
    if not root.is_dir() or not (root / "pyproject.toml").is_file():
        raise AIPatternRunError("project root is not a systematic-fx checkout")
    return root


def _fixed_run_root(
    project_root: Path, value: Path | str | None, *, create: bool
) -> Path:
    expected = project_root / DEFAULT_AI_PATTERN_ROOT
    requested = expected if value is None else Path(value).expanduser()
    if not requested.is_absolute():
        requested = project_root / requested
    # The public workflow intentionally has one fixed repository-local root.
    if requested.absolute() != expected.absolute():
        raise AIPatternRunError("AI proposal run root must be the fixed checked-in location")
    current = project_root
    for part in DEFAULT_AI_PATTERN_ROOT.parts:
        current = current / part
        if current.is_symlink():
            raise AIPatternRunError("AI proposal run root has a symbolic-link component")
    if create:
        expected.mkdir(parents=True, exist_ok=True)
    try:
        resolved = expected.resolve(strict=True)
    except FileNotFoundError as error:
        raise AIPatternRunError("AI proposal run does not exist") from error
    if resolved != expected.absolute() or not resolved.is_dir():
        raise AIPatternRunError("AI proposal run root is unsafe")
    return resolved


def _start_after_precommit(
    *, project_root: Path, run_root: Path, config: AIPatternDiscoveryConfig
) -> ProposalRunStart:
    ledger = ProposalLedger(run_root / "ledger")
    artifacts = run_root / "artifacts"
    request = config.request
    request_artifact = publish_canonical_artifact(
        artifacts,
        artifact_type="AI_PATTERN_PROPOSAL_REQUEST",
        filename_prefix="proposal-request",
        document=request.as_dict(),
    )
    ledger.append_precommit(request, request_artifact)

    try:
        # No source artifact is opened before the PRECOMMITTED ledger event exists.
        source_artifact = load_ai_discovery_context(project_root)
        source_document = reopen_ai_discovery_context(project_root, source_artifact)
        context = context_from_ai_discovery_document(
            request,
            source_document,
            expected_context_sha256=source_artifact.sha256,
        )
        if (
            source_artifact.sha256 != EXPECTED_AI_DISCOVERY_CONTEXT_SHA256
            or context.source_row_count != config.decision_bar_rows
            or len(context.bins) != config.expected_context_bins
            or len(context.session_row_counts) != config.decision_active_days
        ):
            raise AIPatternRunError("Discovery proposal context differs from the frozen run")
        context_artifact = publish_canonical_artifact(
            artifacts,
            artifact_type="AI_PATTERN_DISCOVERY_CONTEXT",
            filename_prefix="discovery-context",
            document=context.as_dict(),
        )
        ledger.append_context(request, context, context_artifact)
    except Exception as error:
        ledger.append_failed(request, failure_code=type(error).__name__)
        raise
    return ProposalRunStart(request, request_artifact, context, context_artifact)


def run_ai_pattern_research(
    project_root: Path | str,
    *,
    run_root: Path | str | None = None,
) -> AIPatternOperationalRun:
    """Autonomously mine exactly twelve outcome-blind Discovery hypotheses once."""

    root = _project_root(project_root)
    selected_root = _fixed_run_root(root, run_root, create=True)
    config = load_ai_pattern_discovery_config(root)
    ledger = ProposalLedger(selected_root / "ledger")
    existing = ledger.verify()
    if existing:
        raise AIPatternRunError(
            "AI proposal run already exists; verify/replay it instead of expanding the budget"
        )
    start = _start_after_precommit(
        project_root=root,
        run_root=selected_root,
        config=config,
    )
    result = complete_deterministic_pattern_discovery(
        ledger_root=selected_root / "ledger",
        artifact_root=selected_root / "artifacts",
        start=start,
    )
    operational = AIPatternOperationalRun(config, result, selected_root)
    verify_ai_pattern_research(root, run=operational)
    return operational


def _identity(value: object) -> ArtifactIdentity:
    return ArtifactIdentity.from_dict(value)


def _event_artifact(event_payload: Mapping[str, object], key: str) -> ArtifactIdentity:
    if set(event_payload) != {key} or key not in event_payload:
        raise AIPatternRunError("proposal ledger payload differs from the exact run contract")
    return _identity(event_payload[key])


def verify_ai_pattern_research(
    project_root: Path | str,
    *,
    run: AIPatternOperationalRun | None = None,
    run_root: Path | str | None = None,
) -> AIPatternOperationalRun:
    """Reopen every source/result artifact and reproduce the autonomous batch bytes."""

    root = _project_root(project_root)
    selected_root = _fixed_run_root(
        root, run_root if run is None else run.root, create=False
    )
    config = load_ai_pattern_discovery_config(root)
    ledger_root = selected_root / "ledger"
    artifacts_root = selected_root / "artifacts"
    events_root = selected_root / "ledger/events"
    if (
        ledger_root.is_symlink()
        or artifacts_root.is_symlink()
        or events_root.is_symlink()
        or not ledger_root.is_dir()
        or not artifacts_root.is_dir()
        or not events_root.is_dir()
    ):
        raise AIPatternRunError("AI proposal ledger does not exist or is unsafe")
    events = ProposalLedger(ledger_root).verify()
    if [item.event_type for item in events] != [
        "PRECOMMITTED",
        "CONTEXT_PUBLISHED",
        "COMPLETED",
    ] or any(item.request_sha256 != config.request.sha256 for item in events):
        raise AIPatternRunError("proposal ledger lifecycle differs from the one finite run")
    request_identity = _event_artifact(events[0].payload, "request_artifact")
    context_identity = _event_artifact(events[1].payload, "context_artifact")
    completed = events[2].payload
    if set(completed) != {"batch_artifact", "report_artifact", "status"} or completed.get(
        "status"
    ) != FINAL_STATUS:
        raise AIPatternRunError("proposal completion payload differs")
    batch_identity = _identity(completed["batch_artifact"])
    report_identity = _identity(completed["report_artifact"])
    if (
        request_identity.artifact_type != "AI_PATTERN_PROPOSAL_REQUEST"
        or context_identity.artifact_type != "AI_PATTERN_DISCOVERY_CONTEXT"
        or batch_identity.artifact_type != "AI_PATTERN_PROPOSAL_BATCH"
        or report_identity.artifact_type != "AI_PATTERN_DISCOVERY_REPORT"
    ):
        raise AIPatternRunError("proposal ledger artifact roles differ")
    artifacts = selected_root / "artifacts"
    verify_immutable_artifact(
        artifacts,
        request_identity,
        expected_bytes=canonical_json_bytes(config.request.as_dict()),
    )

    source_artifact = load_ai_discovery_context(root)
    source_document = reopen_ai_discovery_context(root, source_artifact)
    context = context_from_ai_discovery_document(
        config.request,
        source_document,
        expected_context_sha256=source_artifact.sha256,
    )
    verify_immutable_artifact(
        artifacts,
        context_identity,
        expected_bytes=canonical_json_bytes(context.as_dict()),
    )
    batch = propose_deterministically(config.request, context)
    verify_immutable_artifact(
        artifacts,
        batch_identity,
        expected_bytes=canonical_json_bytes(batch.as_dict()),
    )
    if run is None:
        # Parse only the small report wrapper. All scientific payloads were rebuilt above.
        payload = verify_immutable_artifact(artifacts, report_identity)
        try:
            report_document = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise AIPatternRunError("proposal report is not JSON") from error
        expected_report = {
            "artifact_schema": "systematic_fx.ai_pattern_discovery_report.v1",
            "authority": "PROPOSE_CANDIDATES_ONLY",
            "batch_artifact": batch_identity.as_dict(),
            "batch_sha256": batch.sha256,
            "context_artifact": context_identity.as_dict(),
            "context_sha256": context.sha256,
            "database_mutated": False,
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
            "recorded_response_artifact": None,
            "request_artifact": request_identity.as_dict(),
            "request_sha256": config.request.sha256,
            "sealed_holdout_untouched": True,
            "status": FINAL_STATUS,
        }
        if report_document != expected_report or canonical_json_bytes(report_document) != payload:
            raise AIPatternRunError("proposal report differs from its deterministic reconstruction")
        # Rehydrate only after every durable identity has been verified.
        from systematic_fx.research.ai_pattern_discovery import ProposalRunReport

        report = ProposalRunReport(
            request_sha256=config.request.sha256,
            request_artifact=request_identity,
            context_sha256=context.sha256,
            context_artifact=context_identity,
            batch_sha256=batch.sha256,
            batch_artifact=batch_identity,
            recorded_response_artifact=None,
        )
        result = ProposalRunResult(
            start=ProposalRunStart(config.request, request_identity, context, context_identity),
            batch=batch,
            batch_artifact=batch_identity,
            report=report,
            report_artifact=report_identity,
            recorded_response_artifact=None,
        )
        run = AIPatternOperationalRun(config, result, selected_root)
    else:
        if (
            run.config != config
            or run.result.batch.as_dict() != batch.as_dict()
            or run.result.batch_artifact != batch_identity
            or run.result.report_artifact != report_identity
        ):
            raise AIPatternRunError("in-memory proposal run differs from durable replay")
        verify_immutable_artifact(
            artifacts,
            report_identity,
            expected_bytes=canonical_json_bytes(run.result.report.as_dict()),
        )
    return run


def render_ai_pattern_research_report(run: AIPatternOperationalRun) -> str:
    """Render a concise report that cannot be mistaken for performance evidence."""

    data = run.as_dict()
    lines = [
        "# Autonomous AI Pattern Discovery — Proposal Batch 1",
        "",
        f"- Status: `{data['status']}`",
        "- Evidence ceiling: hypothesis proposals only; no performance was evaluated.",
        (
            f"- Discovery input: {run.config.visible_active_days} visible active days, "
            f"{run.config.decision_bar_rows:,} completed 5-minute bars in the decision prefix."
        ),
        (
            f"- Finite search: {data['candidate_universe_count']} rules; "
            f"{data['proposal_count']} accepted proposals."
        ),
        f"- Context SHA-256: `{data['context_sha256']}`",
        f"- Proposal batch SHA-256: `{data['proposal_batch_sha256']}`",
        "- Database / walk-forward / sealed holdout: untouched.",
        "",
        "## Proposed patterns",
        "",
    ]
    for proposal in data["proposals"]:
        predicates = proposal["rule"]["all"]
        rule = " AND ".join(
            f"{item['feature']} {item['operator']} {item['threshold']}" for item in predicates
        )
        lines.extend(
            [
                (
                    f"{proposal['selection_rank']}. **{proposal['family']} / "
                    f"{proposal['direction']}** — `{rule}`"
                ),
                (
                    f"   Support: {proposal['support_rows']:,} bars "
                    f"({proposal['support_ppm'] / 10_000:.2f}%); "
                    f"active days: {proposal['session_support_count']}; "
                    f"stability: {proposal['stability_ppm'] / 10_000:.2f}%."
                ),
            ]
        )
    lines.extend(
        [
            "",
            "## Next governed transition",
            "",
            (
                "These rules are frozen and reproducible, but they are not alpha evidence. "
                "They remain awaiting research-eligible status/calendar/active-contract data "
                "before label evaluation, null controls, or an M0b epoch may begin."
            ),
            "",
        ]
    )
    return "\n".join(lines)


def publish_ai_pattern_markdown_report(
    project_root: Path | str, run: AIPatternOperationalRun
) -> Path:
    """Atomically publish the human report under reports/generated."""

    root = _project_root(project_root)
    output = root / "reports/generated/ai_pattern_discovery_batch_1.md"
    output.parent.mkdir(parents=True, exist_ok=True)
    content = render_ai_pattern_research_report(run)
    temporary = output.with_name(f".{output.name}.tmp-{os.getpid()}")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, output)
    if output.stat().st_mode & _WRITE_BITS == 0:
        output.chmod(0o644)
    return output


__all__ = [
    "AI_PATTERN_RUN_SCHEMA",
    "DEFAULT_AI_PATTERN_ROOT",
    "AIPatternOperationalRun",
    "AIPatternRunError",
    "publish_ai_pattern_markdown_report",
    "render_ai_pattern_research_report",
    "run_ai_pattern_research",
    "verify_ai_pattern_research",
]
