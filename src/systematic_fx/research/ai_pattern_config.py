"""Frozen configuration for the first autonomous AI pattern proposal run."""

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
from systematic_fx.research.hypotheses import canonical_sha256, load_toml_document
from systematic_fx.research.provenance import dependency_lock_sha256

AI_PATTERN_CONFIG_SCHEMA: Final = "systematic_fx.ai_pattern_discovery_config.v1"
AI_PATTERN_CONFIG_RELATIVE_PATH: Final = Path(
    "configs/research/ai_pattern_discovery_v1.toml"
)


class AIPatternConfigError(ValueError):
    """The autonomous proposal manifest differs from its frozen contract."""


def _proposer_implementation_sha256(project_root: Path) -> str:
    """Hash the exact proposer/runtime modules without self-referencing the TOML."""

    relative_paths = (
        "src/systematic_fx/features/bars.py",
        "src/systematic_fx/research/ai_discovery_context.py",
        "src/systematic_fx/research/ai_pattern_config.py",
        "src/systematic_fx/research/ai_pattern_discovery.py",
        "src/systematic_fx/research/ai_pattern_run.py",
        "src/systematic_fx/research/bar_artifacts.py",
        "src/systematic_fx/research/bar_config.py",
        "src/systematic_fx/research/bar_pipeline.py",
        "src/systematic_fx/research/hypotheses.py",
        "src/systematic_fx/validation/bar_splits.py",
    )
    files: list[dict[str, object]] = []
    for relative in relative_paths:
        path = project_root / relative
        if path.is_symlink() or not path.is_file():
            raise AIPatternConfigError("AI proposer implementation source is missing or symbolic")
        content = path.read_bytes()
        files.append(
            {
                "byte_size": len(content),
                "relative_path": relative,
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        )
    return canonical_sha256(
        {
            "implementation_schema": "systematic_fx.ai_pattern_implementation.v1",
            "modules": files,
        }
    )


@dataclass(frozen=True, slots=True)
class AIPatternDiscoveryConfig:
    path: Path
    file_sha256: str
    semantic_sha256: str
    request: ProposalRequest
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
            "expected_context_bins": self.expected_context_bins,
            "request": self.request.as_dict(),
            "schema": AI_PATTERN_CONFIG_SCHEMA,
            "source_bar_rows": self.source_bar_rows,
            "visible_active_days": self.visible_active_days,
        }


def _object(value: object, *, label: str, keys: set[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise AIPatternConfigError(f"{label} differs from the exact frozen schema")
    return value


def _integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AIPatternConfigError(f"{label} must be a non-negative integer")
    return value


def load_ai_pattern_discovery_config(
    project_root: Path | str,
    *,
    path: Path | str | None = None,
) -> AIPatternDiscoveryConfig:
    """Load the exact finite proposal budget and approved Discovery identity."""

    root = Path(project_root).expanduser().resolve(strict=True)
    requested = AI_PATTERN_CONFIG_RELATIVE_PATH if path is None else Path(path)
    selected = requested if requested.is_absolute() else root / requested
    if selected.is_symlink():
        raise AIPatternConfigError("AI proposal config cannot be symbolic")
    resolved = selected.resolve(strict=True)
    if not resolved.is_relative_to(root) or resolved != root / AI_PATTERN_CONFIG_RELATIVE_PATH:
        raise AIPatternConfigError("AI proposal config must be the checked-in frozen manifest")
    document = load_toml_document(resolved)
    root_keys = {
        "authority",
        "budgets",
        "candidate_catalog_sha256",
        "candidate_evaluation_budget",
        "code_commit",
        "proposer_implementation_sha256",
        "config_id",
        "deterministic_seed",
        "dependency_lock_sha256",
        "model_id",
        "model_version",
        "precommitted_at_utc",
        "proposer_mode",
        "provider_id",
        "request_key",
        "schema_version",
        "selection",
        "source",
        "status",
    }
    _object(document, label="AI proposal config", keys=root_keys)
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
    expected_root = {
        "authority": AUTHORITY,
        "candidate_catalog_sha256": (
            "b5ab777126eace96858c57cf619a954195d19187902bc1b6fbf56b8e1ad90ef3"
        ),
        "candidate_evaluation_budget": 620,
        "config_id": "ai_pattern_discovery_v1",
        "deterministic_seed": 20260813,
        "model_id": "OUTCOME_BLIND_SUPPORT_STABILITY_DIVERSITY",
        "model_version": "v1",
        "precommitted_at_utc": "2026-08-13T00:00:00Z",
        "proposer_mode": "DETERMINISTIC_OUTCOME_BLIND_V1",
        "provider_id": "SYSTEMATIC_FX_LOCAL",
        "request_key": "ai_pattern_discovery_2026_08_13_v1",
        "schema_version": AI_PATTERN_CONFIG_SCHEMA,
    }
    if any(document[key] != value for key, value in expected_root.items()):
        raise AIPatternConfigError("AI proposal root identity drifted")
    code_commit = str(document["code_commit"])
    implementation_sha256 = str(document["proposer_implementation_sha256"])
    dependency_sha256 = str(document["dependency_lock_sha256"])
    if (
        len(code_commit) not in {40, 64}
        or len(implementation_sha256) != 64
        or len(dependency_sha256) != 64
        or _proposer_implementation_sha256(root) != implementation_sha256
        or dependency_lock_sha256(root) != dependency_sha256
    ):
        raise AIPatternConfigError("AI proposal executable provenance drifted")
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
        raise AIPatternConfigError("AI proposal source identity drifted")
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
        raise AIPatternConfigError("AI proposal budgets drifted")
    if selection != {
        "maximum_pairwise_overlap_ppm": 950_000,
        "minimum_session_count": 80,
        "minimum_stability_ppm": 0,
        "minimum_support_rows": 500,
        "ranking_inputs": ["SUPPORT", "SESSION_STABILITY", "SIGNAL_DIVERSITY"],
    }:
        raise AIPatternConfigError("AI proposal selection policy drifted")
    if status != {
        "m0b_epoch_registered": False,
        "performance_evaluated": False,
        "persistent_database_mutated": False,
        "sealed_holdout_touched": False,
        "terminal_status": FINAL_STATUS,
        "walk_forward_touched": False,
    }:
        raise AIPatternConfigError("AI proposal status ceiling drifted")
    request = ProposalRequest(
        request_key=str(document["request_key"]),
        proposer_mode="DETERMINISTIC_OUTCOME_BLIND_V1",
        provider_id=str(document["provider_id"]),
        model_id=str(document["model_id"]),
        model_version=str(document["model_version"]),
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
        max_response_bytes=_integer(
            budgets["max_response_bytes"], label="max_response_bytes"
        ),
        deterministic_seed=_integer(
            document["deterministic_seed"], label="deterministic_seed"
        ),
        precommitted_at_utc=str(document["precommitted_at_utc"]),
        candidate_evaluation_budget=_integer(
            document["candidate_evaluation_budget"], label="candidate_evaluation_budget"
        ),
        candidate_catalog_sha256=str(document["candidate_catalog_sha256"]),
        code_commit=code_commit,
        proposer_implementation_sha256=implementation_sha256,
        dependency_lock_sha256=dependency_sha256,
    )
    raw = resolved.read_bytes()
    return AIPatternDiscoveryConfig(
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        semantic_sha256=canonical_sha256(document),
        request=request,
        context_identity_sha256=str(source["context_identity_sha256"]),
        visible_active_days=_integer(source["visible_active_days"], label="visible_active_days"),
        decision_active_days=_integer(
            source["decision_active_days"], label="decision_active_days"
        ),
        source_bar_rows=_integer(source["source_bar_rows"], label="source_bar_rows"),
        decision_bar_rows=_integer(source["decision_bar_rows"], label="decision_bar_rows"),
        expected_context_bins=_integer(
            source["expected_context_bins"], label="expected_context_bins"
        ),
    )


__all__ = [
    "AI_PATTERN_CONFIG_RELATIVE_PATH",
    "AI_PATTERN_CONFIG_SCHEMA",
    "AIPatternConfigError",
    "AIPatternDiscoveryConfig",
    "load_ai_pattern_discovery_config",
]
