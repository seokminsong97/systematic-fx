"""Frozen configuration for the direction-consistent second proposal run."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from systematic_fx.research.ai_discovery_context import (
    AI_MORPHOLOGY_VERSION,
    EXPECTED_AI_DISCOVERY_CONTEXT_IDENTITY_SHA256,
    EXPECTED_AI_DISCOVERY_CONTEXT_SHA256,
    EXPECTED_DECISION_END_DATE,
    EXPECTED_DISCOVERY_ACTIVE_DAYS,
    EXPECTED_DISCOVERY_BAR_ROWS,
    EXPECTED_DISCOVERY_DECISION_DAYS,
    EXPECTED_DISCOVERY_START_DATE,
    EXPECTED_SPLIT_PLAN_SHA256,
)
from systematic_fx.research.ai_pattern_discovery import (
    AUTHORITY,
    DETERMINISTIC_PROMPT_SHA256,
    FINAL_STATUS,
    ProposalRequest,
)
from systematic_fx.research.ai_pattern_discovery_v2 import (
    V1_CANDIDATE_CATALOG_COUNT,
    V1_CANDIDATE_CATALOG_SHA256,
    V2_CANDIDATE_CATALOG_COUNT,
    V2_CANDIDATE_CATALOG_SHA256,
    V2_DETERMINISTIC_PROMPT_SHA256,
    V2_FILTERED_DIRECTIONLESS_RANGE_COUNT,
    V2_PROPOSER_MODE,
    V2_REJECTED_V1_CANDIDATE_SHA256,
    V2_REJECTION_REASON,
    V2_SEMANTIC_POLICY_SHA256,
    DirectionalProposalEnvelope,
)
from systematic_fx.research.hypotheses import canonical_sha256, load_toml_document
from systematic_fx.research.provenance import dependency_lock_sha256

AI_PATTERN_CONFIG_V2_SCHEMA: Final = "systematic_fx.ai_pattern_discovery_config.v2"
AI_PATTERN_CONFIG_V2_RELATIVE_PATH: Final = Path("configs/research/ai_pattern_discovery_v2.toml")
AI_PATTERN_V1_REQUEST_SHA256: Final = (
    "8539fbce5ea6335edacdac8e2f6fed3b7b504614efc5bedc0f82c261e56735b7"
)
AI_PATTERN_V1_BATCH_SHA256: Final = (
    "46a038bc7af2aa4947389674d015f730d60d0f7ae25dadee25e661cf02153df6"
)


class AIPatternConfigV2Error(ValueError):
    """The corrected proposal manifest differs from its frozen contract."""


def _proposer_implementation_sha256(project_root: Path) -> str:
    """Hash every local module participating in the v2 proposal run."""

    relative_paths = (
        "src/systematic_fx/backtest/barriers.py",
        "src/systematic_fx/data/contract_selection.py",
        "src/systematic_fx/data/contracts.py",
        "src/systematic_fx/data/instruments.py",
        "src/systematic_fx/features/bars.py",
        "src/systematic_fx/research/ai_discovery_context.py",
        "src/systematic_fx/research/ai_pattern_config_v2.py",
        "src/systematic_fx/research/ai_pattern_discovery.py",
        "src/systematic_fx/research/ai_pattern_discovery_v2.py",
        "src/systematic_fx/research/ai_pattern_run_v2.py",
        "src/systematic_fx/research/bar_artifacts.py",
        "src/systematic_fx/research/bar_config.py",
        "src/systematic_fx/research/bar_pipeline.py",
        "src/systematic_fx/research/hypotheses.py",
        "src/systematic_fx/research/provenance.py",
        "src/systematic_fx/validation/bar_splits.py",
    )
    modules: list[dict[str, object]] = []
    for relative in relative_paths:
        path = project_root / relative
        if path.is_symlink() or not path.is_file():
            raise AIPatternConfigV2Error("AI proposer implementation source is missing or symbolic")
        payload = path.read_bytes()
        modules.append(
            {
                "byte_size": len(payload),
                "relative_path": relative,
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
    return canonical_sha256(
        {
            "implementation_schema": "systematic_fx.ai_pattern_implementation.v2",
            "modules": modules,
        }
    )


@dataclass(frozen=True, slots=True)
class AIPatternDiscoveryConfigV2:
    path: Path
    file_sha256: str
    semantic_sha256: str
    request: ProposalRequest
    envelope: DirectionalProposalEnvelope
    context_identity_sha256: str
    visible_active_days: int
    decision_active_days: int
    source_bar_rows: int
    decision_bar_rows: int
    expected_context_bins: int

    def as_dict(self) -> dict[str, object]:
        return {
            "config_file_sha256": self.file_sha256,
            "config_semantic_sha256": self.semantic_sha256,
            "context_identity_sha256": self.context_identity_sha256,
            "decision_active_days": self.decision_active_days,
            "decision_bar_rows": self.decision_bar_rows,
            "directional_envelope": self.envelope.as_dict(),
            "directional_envelope_sha256": self.envelope.sha256,
            "expected_context_bins": self.expected_context_bins,
            "request": self.request.as_dict(),
            "schema": AI_PATTERN_CONFIG_V2_SCHEMA,
            "source_bar_rows": self.source_bar_rows,
            "visible_active_days": self.visible_active_days,
        }


def _object(value: object, *, label: str, keys: set[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise AIPatternConfigV2Error(f"{label} differs from the exact frozen schema")
    return value


def _integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AIPatternConfigV2Error(f"{label} must be a non-negative integer")
    return value


def load_ai_pattern_discovery_config_v2(
    project_root: Path | str,
    *,
    path: Path | str | None = None,
) -> AIPatternDiscoveryConfigV2:
    """Load the exact corrected catalog, policy, budget, and source identity."""

    root = Path(project_root).expanduser().resolve(strict=True)
    requested = AI_PATTERN_CONFIG_V2_RELATIVE_PATH if path is None else Path(path)
    selected = requested if requested.is_absolute() else root / requested
    if selected.is_symlink():
        raise AIPatternConfigV2Error("AI proposal config cannot be symbolic")
    resolved = selected.resolve(strict=True)
    if not resolved.is_relative_to(root) or resolved != root / AI_PATTERN_CONFIG_V2_RELATIVE_PATH:
        raise AIPatternConfigV2Error("AI proposal config must be the checked-in v2 manifest")
    document = load_toml_document(resolved)
    _object(
        document,
        label="AI proposal v2 config",
        keys={
            "authority",
            "base_model_id",
            "base_model_version",
            "base_proposer_mode",
            "base_provider_id",
            "budgets",
            "candidate_catalog_sha256",
            "candidate_evaluation_budget",
            "code_commit",
            "config_id",
            "correction",
            "dependency_lock_sha256",
            "deterministic_seed",
            "directional_prompt_sha256",
            "directional_proposer_mode",
            "precommitted_at_utc",
            "proposer_implementation_sha256",
            "request_key",
            "schema_version",
            "selection",
            "semantic_policy_sha256",
            "source",
            "status",
        },
    )
    source = _object(
        document["source"],
        label="source",
        keys={
            "context_identity_sha256",
            "context_sha256",
            "decision_active_days",
            "decision_bar_rows",
            "discovery_split_sha256",
            "expected_context_bins",
            "feature_version",
            "source_bar_rows",
            "source_interval_end",
            "source_interval_start",
            "visible_active_days",
        },
    )
    budgets = _object(
        document["budgets"],
        label="budgets",
        keys={
            "max_context_bins",
            "max_input_tokens",
            "max_model_calls",
            "max_output_tokens",
            "max_predicates_per_rule",
            "max_response_bytes",
            "max_source_rows",
            "proposal_budget",
        },
    )
    selection = _object(
        document["selection"],
        label="selection",
        keys={
            "maximum_pairwise_overlap_ppm",
            "minimum_session_count",
            "minimum_stability_ppm",
            "minimum_support_rows",
            "ranking_inputs",
        },
    )
    status = _object(
        document["status"],
        label="status",
        keys={
            "m0b_epoch_registered",
            "performance_evaluated",
            "persistent_database_mutated",
            "sealed_holdout_touched",
            "terminal_status",
            "walk_forward_touched",
        },
    )
    correction = _object(
        document["correction"],
        label="correction",
        keys={
            "rejected_candidate_count",
            "rejected_candidate_sha256",
            "rejection_reason",
            "source_candidate_count",
            "source_candidate_sha256",
            "supersedes_batch_sha256",
            "supersedes_request_sha256",
        },
    )
    expected_root = {
        "authority": AUTHORITY,
        "base_model_id": "OUTCOME_BLIND_SUPPORT_STABILITY_DIVERSITY",
        "base_model_version": "v1",
        "base_proposer_mode": "DETERMINISTIC_OUTCOME_BLIND_V1",
        "base_provider_id": "SYSTEMATIC_FX_LOCAL",
        "candidate_catalog_sha256": V2_CANDIDATE_CATALOG_SHA256,
        "candidate_evaluation_budget": V2_CANDIDATE_CATALOG_COUNT,
        "config_id": "ai_pattern_discovery_v2",
        "deterministic_seed": 20260813,
        "directional_prompt_sha256": V2_DETERMINISTIC_PROMPT_SHA256,
        "directional_proposer_mode": V2_PROPOSER_MODE,
        "precommitted_at_utc": "2026-08-13T15:20:00Z",
        "request_key": "ai_pattern_discovery_2026_08_13_v2",
        "schema_version": AI_PATTERN_CONFIG_V2_SCHEMA,
        "semantic_policy_sha256": V2_SEMANTIC_POLICY_SHA256,
    }
    if any(document[key] != value for key, value in expected_root.items()):
        raise AIPatternConfigV2Error("AI proposal v2 root identity drifted")
    if correction != {
        "rejected_candidate_count": V2_FILTERED_DIRECTIONLESS_RANGE_COUNT,
        "rejected_candidate_sha256": V2_REJECTED_V1_CANDIDATE_SHA256,
        "rejection_reason": V2_REJECTION_REASON,
        "source_candidate_count": V1_CANDIDATE_CATALOG_COUNT,
        "source_candidate_sha256": V1_CANDIDATE_CATALOG_SHA256,
        "supersedes_batch_sha256": AI_PATTERN_V1_BATCH_SHA256,
        "supersedes_request_sha256": AI_PATTERN_V1_REQUEST_SHA256,
    }:
        raise AIPatternConfigV2Error("AI proposal correction identity drifted")
    code_commit = str(document["code_commit"])
    implementation_sha256 = str(document["proposer_implementation_sha256"])
    dependency_sha256 = str(document["dependency_lock_sha256"])
    if (
        len(code_commit) != 40
        or len(implementation_sha256) != 64
        or len(dependency_sha256) != 64
        or _proposer_implementation_sha256(root) != implementation_sha256
        or dependency_lock_sha256(root) != dependency_sha256
    ):
        raise AIPatternConfigV2Error("AI proposal v2 executable provenance drifted")
    expected_source = {
        "context_identity_sha256": EXPECTED_AI_DISCOVERY_CONTEXT_IDENTITY_SHA256,
        "context_sha256": EXPECTED_AI_DISCOVERY_CONTEXT_SHA256,
        "decision_active_days": EXPECTED_DISCOVERY_DECISION_DAYS,
        "decision_bar_rows": 106_605,
        "discovery_split_sha256": EXPECTED_SPLIT_PLAN_SHA256,
        "expected_context_bins": 84_207,
        "feature_version": AI_MORPHOLOGY_VERSION,
        "source_bar_rows": EXPECTED_DISCOVERY_BAR_ROWS,
        "source_interval_end": EXPECTED_DECISION_END_DATE.isoformat(),
        "source_interval_start": EXPECTED_DISCOVERY_START_DATE.isoformat(),
        "visible_active_days": EXPECTED_DISCOVERY_ACTIVE_DAYS,
    }
    if source != expected_source:
        raise AIPatternConfigV2Error("AI proposal v2 source identity drifted")
    if budgets != {
        "max_context_bins": 120_000,
        "max_input_tokens": 0,
        "max_model_calls": 0,
        "max_output_tokens": 0,
        "max_predicates_per_rule": 3,
        "max_response_bytes": 0,
        "max_source_rows": 120_000,
        "proposal_budget": 12,
    }:
        raise AIPatternConfigV2Error("AI proposal v2 budgets drifted")
    if selection != {
        "maximum_pairwise_overlap_ppm": 950_000,
        "minimum_session_count": 80,
        "minimum_stability_ppm": 0,
        "minimum_support_rows": 500,
        "ranking_inputs": ["SUPPORT", "SESSION_STABILITY", "SIGNAL_DIVERSITY"],
    }:
        raise AIPatternConfigV2Error("AI proposal v2 selection policy drifted")
    if status != {
        "m0b_epoch_registered": False,
        "performance_evaluated": False,
        "persistent_database_mutated": False,
        "sealed_holdout_touched": False,
        "terminal_status": FINAL_STATUS,
        "walk_forward_touched": False,
    }:
        raise AIPatternConfigV2Error("AI proposal v2 status ceiling drifted")
    request = ProposalRequest(
        request_key=str(document["request_key"]),
        proposer_mode="DETERMINISTIC_OUTCOME_BLIND_V1",
        provider_id=str(document["base_provider_id"]),
        model_id=str(document["base_model_id"]),
        model_version=str(document["base_model_version"]),
        prompt_sha256=DETERMINISTIC_PROMPT_SHA256,
        source_feature_sha256=str(source["context_sha256"]),
        source_feature_version=str(source["feature_version"]),
        discovery_split_sha256=str(source["discovery_split_sha256"]),
        source_interval_start=str(source["source_interval_start"]),
        source_interval_end=str(source["source_interval_end"]),
        max_source_rows=_integer(budgets["max_source_rows"], label="max_source_rows"),
        max_context_bins=_integer(budgets["max_context_bins"], label="max_context_bins"),
        proposal_budget=_integer(budgets["proposal_budget"], label="proposal_budget"),
        max_predicates_per_rule=_integer(
            budgets["max_predicates_per_rule"], label="max_predicates_per_rule"
        ),
        minimum_support_rows=_integer(
            selection["minimum_support_rows"], label="minimum_support_rows"
        ),
        minimum_session_count=_integer(
            selection["minimum_session_count"], label="minimum_session_count"
        ),
        minimum_stability_ppm=_integer(
            selection["minimum_stability_ppm"], label="minimum_stability_ppm"
        ),
        maximum_pairwise_overlap_ppm=_integer(
            selection["maximum_pairwise_overlap_ppm"],
            label="maximum_pairwise_overlap_ppm",
        ),
        max_model_calls=_integer(budgets["max_model_calls"], label="max_model_calls"),
        max_input_tokens=_integer(budgets["max_input_tokens"], label="max_input_tokens"),
        max_output_tokens=_integer(budgets["max_output_tokens"], label="max_output_tokens"),
        max_response_bytes=_integer(budgets["max_response_bytes"], label="max_response_bytes"),
        deterministic_seed=_integer(document["deterministic_seed"], label="deterministic_seed"),
        precommitted_at_utc=str(document["precommitted_at_utc"]),
        candidate_evaluation_budget=_integer(
            document["candidate_evaluation_budget"], label="candidate_evaluation_budget"
        ),
        candidate_catalog_sha256=str(document["candidate_catalog_sha256"]),
        code_commit=code_commit,
        proposer_implementation_sha256=implementation_sha256,
        dependency_lock_sha256=dependency_sha256,
    )
    envelope = DirectionalProposalEnvelope(request.sha256)
    raw = resolved.read_bytes()
    return AIPatternDiscoveryConfigV2(
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        semantic_sha256=canonical_sha256(document),
        request=request,
        envelope=envelope,
        context_identity_sha256=str(source["context_identity_sha256"]),
        visible_active_days=_integer(source["visible_active_days"], label="visible_active_days"),
        decision_active_days=_integer(source["decision_active_days"], label="decision_active_days"),
        source_bar_rows=_integer(source["source_bar_rows"], label="source_bar_rows"),
        decision_bar_rows=_integer(source["decision_bar_rows"], label="decision_bar_rows"),
        expected_context_bins=_integer(
            source["expected_context_bins"], label="expected_context_bins"
        ),
    )


__all__ = [
    "AI_PATTERN_CONFIG_V2_RELATIVE_PATH",
    "AI_PATTERN_CONFIG_V2_SCHEMA",
    "AI_PATTERN_V1_BATCH_SHA256",
    "AI_PATTERN_V1_REQUEST_SHA256",
    "AIPatternConfigV2Error",
    "AIPatternDiscoveryConfigV2",
    "load_ai_pattern_discovery_config_v2",
]
