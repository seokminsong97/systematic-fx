"""Frozen configuration for the commit-reconstructible third proposal run."""

from __future__ import annotations

import hashlib
import subprocess
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
    V2_CANDIDATE_CATALOG_COUNT,
    V2_CANDIDATE_CATALOG_SHA256,
    V2_DETERMINISTIC_PROMPT_SHA256,
    V2_PROPOSER_MODE,
    V2_SEMANTIC_POLICY_SHA256,
    DirectionalProposalEnvelope,
)
from systematic_fx.research.hypotheses import canonical_sha256, load_toml_document
from systematic_fx.research.provenance import dependency_lock_sha256

AI_PATTERN_CONFIG_V3_SCHEMA: Final = "systematic_fx.ai_pattern_discovery_config.v3"
AI_PATTERN_CONFIG_V3_RELATIVE_PATH: Final = Path("configs/research/ai_pattern_discovery_v3.toml")
AI_PATTERN_V2_GOVERNED_REQUEST_SHA256: Final = (
    "686989bba64c3af79acc206280bf5f543fc66bd482ec56523254c20f93049392"
)
AI_PATTERN_V2_BATCH_SHA256: Final = (
    "2a9a0642b841c57308f55061046dac9686ac76ace4257b7c01bca4c20537ef18"
)
V3_CORRECTION_REASON: Final = "EXECUTABLE_COMMIT_PROVENANCE_CORRECTION"

# Every Python module in the package is committed and byte-compared before
# market-derived context is opened.  Using the full package tree closes over
# lazy CLI imports and future transitive imports without a hand-maintained list.
V3_IMPLEMENTATION_SINGLE_FILES: Final = (
    "pyproject.toml",
    "uv.lock",
)


class AIPatternConfigV3Error(ValueError):
    """The reconstructible proposal manifest differs from its frozen contract."""


@dataclass(frozen=True, slots=True)
class AIPatternDiscoveryConfigV3:
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
            "schema": AI_PATTERN_CONFIG_V3_SCHEMA,
            "source_bar_rows": self.source_bar_rows,
            "visible_active_days": self.visible_active_days,
        }


def _object(value: object, *, label: str, keys: set[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise AIPatternConfigV3Error(f"{label} differs from the exact frozen schema")
    return value


def _current_implementation_paths(project_root: Path) -> tuple[str, ...]:
    package_root = project_root / "src/systematic_fx"
    if package_root.is_symlink() or not package_root.is_dir():
        raise AIPatternConfigV3Error("AI proposer package source is missing or symbolic")
    discovered: list[str] = list(V3_IMPLEMENTATION_SINGLE_FILES)
    for path in package_root.rglob("*.py"):
        if path.is_symlink() or not path.is_file() or "__pycache__" in path.parts:
            raise AIPatternConfigV3Error("AI proposer package contains unsafe source")
        discovered.append(path.relative_to(project_root).as_posix())
    ordered = tuple(sorted(discovered))
    if len(ordered) != len(set(ordered)):
        raise AIPatternConfigV3Error("AI proposer implementation contains duplicate paths")
    return ordered


def _implementation_document(project_root: Path) -> dict[str, object]:
    modules: list[dict[str, object]] = []
    for relative in _current_implementation_paths(project_root):
        path = project_root / relative
        if path.is_symlink() or not path.is_file():
            raise AIPatternConfigV3Error("AI proposer implementation source is missing or symbolic")
        payload = path.read_bytes()
        modules.append(
            {
                "byte_size": len(payload),
                "relative_path": relative,
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
    return {
        "implementation_schema": "systematic_fx.ai_pattern_implementation.v3",
        "modules": modules,
    }


def proposer_implementation_sha256_v3(project_root: Path | str) -> str:
    """Return the exact v3 runtime byte-catalog identity."""

    root = Path(project_root).expanduser().resolve(strict=True)
    return canonical_sha256(_implementation_document(root))


def _git(project_root: Path, *arguments: str) -> bytes:
    try:
        process = subprocess.run(
            ["git", "-C", str(project_root), *arguments],
            check=False,
            capture_output=True,
        )
    except OSError as error:
        raise AIPatternConfigV3Error("Git executable provenance cannot be verified") from error
    if process.returncode != 0:
        raise AIPatternConfigV3Error("Git executable provenance cannot be verified")
    return process.stdout


def verify_committed_implementation_v3(project_root: Path, code_commit: str) -> None:
    """Require every executed local module to equal a blob in ``code_commit``."""

    if (
        len(code_commit) != 40
        or any(character not in "0123456789abcdef" for character in code_commit)
        or not set(code_commit) - {"0"}
        or _git(project_root, "cat-file", "-t", code_commit).strip() != b"commit"
    ):
        raise AIPatternConfigV3Error("AI proposal code commit is not a full committed object")
    current_paths = _current_implementation_paths(project_root)
    committed_package_paths = tuple(
        sorted(
            line
            for line in _git(
                project_root,
                "ls-tree",
                "-r",
                "--name-only",
                code_commit,
                "--",
                "src/systematic_fx",
            )
            .decode("utf-8")
            .splitlines()
            if line.endswith(".py") and "__pycache__" not in Path(line).parts
        )
    )
    expected_paths = tuple(sorted((*V3_IMPLEMENTATION_SINGLE_FILES, *committed_package_paths)))
    if current_paths != expected_paths:
        raise AIPatternConfigV3Error("AI proposal runtime file set differs from its commit")
    for relative in current_paths:
        committed = _git(project_root, "show", f"{code_commit}:{relative}")
        current = (project_root / relative).read_bytes()
        if committed != current:
            raise AIPatternConfigV3Error("AI proposal runtime differs from its committed source")


def load_ai_pattern_discovery_config_v3(
    project_root: Path | str,
    *,
    path: Path | str | None = None,
) -> AIPatternDiscoveryConfigV3:
    """Load and verify the exact commit-reconstructible third proposal request."""

    root = Path(project_root).expanduser().resolve(strict=True)
    requested = AI_PATTERN_CONFIG_V3_RELATIVE_PATH if path is None else Path(path)
    selected = requested if requested.is_absolute() else root / requested
    if selected.is_symlink():
        raise AIPatternConfigV3Error("AI proposal config cannot be symbolic")
    resolved = selected.resolve(strict=True)
    if not resolved.is_relative_to(root) or resolved != root / AI_PATTERN_CONFIG_V3_RELATIVE_PATH:
        raise AIPatternConfigV3Error("AI proposal config must be the checked-in v3 manifest")
    document = load_toml_document(resolved)
    _object(
        document,
        label="AI proposal v3 config",
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
    expected_root = {
        "authority": AUTHORITY,
        "base_model_id": "OUTCOME_BLIND_SUPPORT_STABILITY_DIVERSITY",
        "base_model_version": "v1",
        "base_proposer_mode": "DETERMINISTIC_OUTCOME_BLIND_V1",
        "base_provider_id": "SYSTEMATIC_FX_LOCAL",
        "candidate_catalog_sha256": V2_CANDIDATE_CATALOG_SHA256,
        "candidate_evaluation_budget": V2_CANDIDATE_CATALOG_COUNT,
        "config_id": "ai_pattern_discovery_v3",
        "deterministic_seed": 20260813,
        "directional_prompt_sha256": V2_DETERMINISTIC_PROMPT_SHA256,
        "directional_proposer_mode": V2_PROPOSER_MODE,
        "precommitted_at_utc": "2026-08-13T17:00:00Z",
        "request_key": "ai_pattern_discovery_2026_08_13_v3",
        "schema_version": AI_PATTERN_CONFIG_V3_SCHEMA,
        "semantic_policy_sha256": V2_SEMANTIC_POLICY_SHA256,
    }
    if any(document[key] != value for key, value in expected_root.items()):
        raise AIPatternConfigV3Error("AI proposal v3 root identity drifted")
    correction = _object(
        document["correction"],
        label="correction",
        keys={"reason", "supersedes_batch_sha256", "supersedes_governed_request_sha256"},
    )
    if correction != {
        "reason": V3_CORRECTION_REASON,
        "supersedes_batch_sha256": AI_PATTERN_V2_BATCH_SHA256,
        "supersedes_governed_request_sha256": AI_PATTERN_V2_GOVERNED_REQUEST_SHA256,
    }:
        raise AIPatternConfigV3Error("AI proposal v3 correction identity drifted")
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
        raise AIPatternConfigV3Error("AI proposal v3 source identity drifted")
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
    expected_budgets = {
        "max_context_bins": 120_000,
        "max_input_tokens": 0,
        "max_model_calls": 0,
        "max_output_tokens": 0,
        "max_predicates_per_rule": 3,
        "max_response_bytes": 0,
        "max_source_rows": 120_000,
        "proposal_budget": 12,
    }
    if budgets != expected_budgets:
        raise AIPatternConfigV3Error("AI proposal v3 budgets drifted")
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
    expected_selection = {
        "maximum_pairwise_overlap_ppm": 950_000,
        "minimum_session_count": 80,
        "minimum_stability_ppm": 0,
        "minimum_support_rows": 500,
        "ranking_inputs": ["SUPPORT", "SESSION_STABILITY", "SIGNAL_DIVERSITY"],
    }
    if selection != expected_selection:
        raise AIPatternConfigV3Error("AI proposal v3 selection policy drifted")
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
    if status != {
        "m0b_epoch_registered": False,
        "performance_evaluated": False,
        "persistent_database_mutated": False,
        "sealed_holdout_touched": False,
        "terminal_status": FINAL_STATUS,
        "walk_forward_touched": False,
    }:
        raise AIPatternConfigV3Error("AI proposal v3 status ceiling drifted")
    code_commit = str(document["code_commit"])
    implementation_sha256 = str(document["proposer_implementation_sha256"])
    dependency_sha256 = str(document["dependency_lock_sha256"])
    if (
        proposer_implementation_sha256_v3(root) != implementation_sha256
        or dependency_lock_sha256(root) != dependency_sha256
    ):
        raise AIPatternConfigV3Error("AI proposal v3 executable provenance drifted")
    verify_committed_implementation_v3(root, code_commit)
    request = ProposalRequest(
        request_key=str(document["request_key"]),
        proposer_mode="DETERMINISTIC_OUTCOME_BLIND_V1",
        provider_id="SYSTEMATIC_FX_LOCAL",
        model_id="OUTCOME_BLIND_SUPPORT_STABILITY_DIVERSITY",
        model_version="v1",
        prompt_sha256=DETERMINISTIC_PROMPT_SHA256,
        source_feature_sha256=EXPECTED_AI_DISCOVERY_CONTEXT_SHA256,
        source_feature_version=AI_MORPHOLOGY_VERSION,
        discovery_split_sha256=EXPECTED_SPLIT_PLAN_SHA256,
        source_interval_start=EXPECTED_DISCOVERY_START_DATE.isoformat(),
        source_interval_end=EXPECTED_DECISION_END_DATE.isoformat(),
        max_source_rows=int(budgets["max_source_rows"]),
        max_context_bins=int(budgets["max_context_bins"]),
        proposal_budget=int(budgets["proposal_budget"]),
        max_predicates_per_rule=int(budgets["max_predicates_per_rule"]),
        minimum_support_rows=int(selection["minimum_support_rows"]),
        minimum_session_count=int(selection["minimum_session_count"]),
        minimum_stability_ppm=int(selection["minimum_stability_ppm"]),
        maximum_pairwise_overlap_ppm=int(selection["maximum_pairwise_overlap_ppm"]),
        max_model_calls=0,
        max_input_tokens=0,
        max_output_tokens=0,
        max_response_bytes=0,
        deterministic_seed=int(document["deterministic_seed"]),
        precommitted_at_utc=str(document["precommitted_at_utc"]),
        candidate_evaluation_budget=V2_CANDIDATE_CATALOG_COUNT,
        candidate_catalog_sha256=V2_CANDIDATE_CATALOG_SHA256,
        code_commit=code_commit,
        proposer_implementation_sha256=implementation_sha256,
        dependency_lock_sha256=dependency_sha256,
    )
    envelope = DirectionalProposalEnvelope(request.sha256)
    raw = resolved.read_bytes()
    return AIPatternDiscoveryConfigV3(
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        semantic_sha256=canonical_sha256(document),
        request=request,
        envelope=envelope,
        context_identity_sha256=EXPECTED_AI_DISCOVERY_CONTEXT_IDENTITY_SHA256,
        visible_active_days=EXPECTED_DISCOVERY_ACTIVE_DAYS,
        decision_active_days=EXPECTED_DISCOVERY_DECISION_DAYS,
        source_bar_rows=EXPECTED_DISCOVERY_BAR_ROWS,
        decision_bar_rows=106_605,
        expected_context_bins=84_207,
    )


__all__ = [
    "AI_PATTERN_CONFIG_V3_RELATIVE_PATH",
    "AI_PATTERN_CONFIG_V3_SCHEMA",
    "AI_PATTERN_V2_BATCH_SHA256",
    "AI_PATTERN_V2_GOVERNED_REQUEST_SHA256",
    "V3_CORRECTION_REASON",
    "V3_IMPLEMENTATION_SINGLE_FILES",
    "AIPatternConfigV3Error",
    "AIPatternDiscoveryConfigV3",
    "load_ai_pattern_discovery_config_v3",
    "proposer_implementation_sha256_v3",
    "verify_committed_implementation_v3",
]
