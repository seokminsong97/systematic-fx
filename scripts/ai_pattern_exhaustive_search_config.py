"""Frozen precommit for the exhaustive 518-member AI-pattern Search family.

The original Batch 3 evaluator and its 12-member result are immutable history.
This contract extends Search to every one of the 518 outcome-blind,
support-eligible rules while explicitly prohibiting walk-forward, embargo, and
holdout access.  Provenance values are filled only after this runtime has been
committed; the loader rejects every ``PENDING`` marker.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from scripts.ai_pattern_holdout_config import (
    AI_PATTERN_HOLDOUT_AUTHORITY,
    EXPECTED_BATCH3_GOVERNED_REQUEST_SHA256,
    EXPECTED_BATCH3_PROPOSAL_BATCH_SHA256,
    EXPECTED_DATASET_HANDOFF_SHA256,
    EXPECTED_DATASET_MANIFEST_SHA256,
    EXPECTED_SOURCE_MANIFEST_SHA256,
    EXPECTED_SPLIT_PLAN_SHA256,
    expected_ai_pattern_holdout_contract,
)
from systematic_fx.research.ai_pattern_discovery_v2 import (
    V2_CANDIDATE_CATALOG_COUNT,
    V2_CANDIDATE_CATALOG_SHA256,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.provenance import dependency_lock_sha256

AI_PATTERN_EXHAUSTIVE_CONFIG_SCHEMA: Final = "systematic_fx.ai_pattern_exhaustive_search_config.v1"
AI_PATTERN_EXHAUSTIVE_CONFIG_RELATIVE_PATH: Final = Path(
    "configs/research/ai_pattern_exhaustive_search_v1.toml"
)
AI_PATTERN_EXHAUSTIVE_AUTHORITY: Final = AI_PATTERN_HOLDOUT_AUTHORITY

EXPECTED_BATCH3_CONTEXT_SHA256: Final = (
    "842539b17aa6a17b29ea125cc324f98d324ae1f5931cee47fa238dc2f6310637"
)
EXPECTED_INITIAL_SEARCH_CONFIG_SEMANTIC_SHA256: Final = (
    "035497ab4879409ff2fa118138e3f304a07a9e96fb45f1778da7521e4ecd71ef"
)
EXPECTED_INITIAL_SEARCH_MASKS_SHA256: Final = (
    "eaef0c46adf1b3620bed80ad9720d3014375d0bc6da31c4dfeb47368cbbe5a99"
)
EXPECTED_INITIAL_SEARCH_RESULT_SHA256: Final = (
    "c950f07dbd690180f5119d57d841026b989f4ac3b0ffae0474d7444febdd7be5"
)
EXPECTED_REMAINING_ASSESSMENT_CATALOG_SHA256: Final = (
    "088c35d2b6781b74e058aa1eef4be8a87a7818e3a5b42d1bd000fb3883d36c3b"
)
EXPECTED_REMAINING_PATTERN_SHA_LIST_SHA256: Final = (
    "f34c5b2e6189136e758cc6f441622d6b2e417046f580b42a75bf367432aa77d3"
)
EXPECTED_BATCH_MANIFEST_SHA256: Final = (
    "022af03de649f829b5ae44f58c840bea05440bda36d7eeca9d5fc6d33fb0f322"
)
EXPECTED_ELIGIBLE_FAMILY_PATTERN_SHA256: Final = (
    "e269800244d62c346497dbbcdfdda540eb361f7273027f387fbc2efe27db4d59"
)
EXPECTED_SELECTED_PATTERN_ORDER_SHA256: Final = (
    "ad128cef2cb2ee5797cc85d987d3cf2145566ac397059fdef2e46f02551a95d0"
)
EXPECTED_SELECTED_PROPOSAL_ORDER_SHA256: Final = (
    "b33301df855fb4528044446cdf3e1f42b1f4007872bbc02fb68ed59387852956"
)
INITIAL_FAMILY_COUNT: Final = 12
SUPPORT_ELIGIBLE_FAMILY_COUNT: Final = 518
REMAINING_FAMILY_COUNT: Final = 506
BATCH_SIZE: Final = 12
BATCH_COUNT: Final = 43
FULL_BATCH_COUNT: Final = 42
FINAL_BATCH_SIZE: Final = 2

EXHAUSTIVE_IMPLEMENTATION_SCRIPTS: Final = (
    "scripts/ai_pattern_exhaustive_search_config.py",
    "scripts/ai_pattern_exhaustive_search_run.py",
    "scripts/run_ai_pattern_exhaustive_search.py",
)
_REUSED_EVALUATOR_SCRIPTS: Final = (
    "scripts/ai_pattern_holdout_config.py",
    "scripts/ai_pattern_holdout_engine.py",
    "scripts/ai_pattern_holdout_run.py",
    "scripts/run_ai_pattern_holdout.py",
)
_IMPLEMENTATION_SINGLE_FILES: Final = ("pyproject.toml", "uv.lock")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_COMMIT = re.compile(r"[0-9a-f]{40}")
_UTC_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")


class AIPatternExhaustiveConfigError(ValueError):
    """The exhaustive Search precommit differs from its exact contract."""


def _static_contract() -> dict[str, object]:
    inherited = expected_ai_pattern_holdout_contract()
    return {
        "authority": AI_PATTERN_EXHAUSTIVE_AUTHORITY,
        "batch3": {
            "context_sha256": EXPECTED_BATCH3_CONTEXT_SHA256,
            "governed_request_sha256": EXPECTED_BATCH3_GOVERNED_REQUEST_SHA256,
            "proposal_batch_sha256": EXPECTED_BATCH3_PROPOSAL_BATCH_SHA256,
        },
        "batching": {
            "batch_count": BATCH_COUNT,
            "batch_size": BATCH_SIZE,
            "final_batch_size": FINAL_BATCH_SIZE,
            "full_batch_count": FULL_BATCH_COUNT,
            "order": "PATTERN_SHA256_ASC_WITH_ELIGIBILITY_RANK_RETAINED_AS_EVIDENCE",
            "result_reporting": "ONE_CANONICAL_SUMMARY_AFTER_EACH_COMPLETED_BATCH",
        },
        "catalog": {
            "batch_manifest_sha256": EXPECTED_BATCH_MANIFEST_SHA256,
            "candidate_catalog_count": V2_CANDIDATE_CATALOG_COUNT,
            "candidate_catalog_sha256": V2_CANDIDATE_CATALOG_SHA256,
            "family_count": SUPPORT_ELIGIBLE_FAMILY_COUNT,
            "family_pattern_sha256": EXPECTED_ELIGIBLE_FAMILY_PATTERN_SHA256,
            "initial_evaluated_count": INITIAL_FAMILY_COUNT,
            "minimum_session_count": 80,
            "minimum_stability_ppm": 0,
            "minimum_support_rows": 500,
            "remaining_assessment_catalog_sha256": (EXPECTED_REMAINING_ASSESSMENT_CATALOG_SHA256),
            "remaining_count": REMAINING_FAMILY_COUNT,
            "remaining_pattern_sha_list_sha256": (EXPECTED_REMAINING_PATTERN_SHA_LIST_SHA256),
            "selected_pattern_order_sha256": EXPECTED_SELECTED_PATTERN_ORDER_SHA256,
            "selected_proposal_order_sha256": EXPECTED_SELECTED_PROPOSAL_ORDER_SHA256,
        },
        "config_id": "ai_pattern_exhaustive_search_v1",
        "dataset": inherited["dataset"],
        "execution": inherited["execution"],
        "initial_search": {
            "governed_request_sha256": EXPECTED_BATCH3_GOVERNED_REQUEST_SHA256,
            "config_semantic_sha256": EXPECTED_INITIAL_SEARCH_CONFIG_SEMANTIC_SHA256,
            "member_count": INITIAL_FAMILY_COUNT,
            "masks_sha256": EXPECTED_INITIAL_SEARCH_MASKS_SHA256,
            "reuse_policy": "REUSE_EXACT_SUMMARIES_NO_REEVALUATION",
            "result_sha256": EXPECTED_INITIAL_SEARCH_RESULT_SHA256,
        },
        "lifecycle": {
            "all_43_masks_frozen_before_first_new_one_second_loader": True,
            "batch_masks_before_one_second_outcomes": True,
            "completed_batch_resume_policy": "VERIFY_IMMUTABLE_ARTIFACT_AND_CONTINUE_NEXT",
            "crash_reconciliation": "ADOPT_EXACT_UNLEDGERED_CONTENT_ADDRESSED_ARTIFACT",
            "family_correction_timing": "ONLY_AFTER_ALL_43_REMAINING_BATCHES_COMPLETE",
            "precommit_before_outcomes": True,
            "stage_artifacts": "ATOMIC_CANONICAL_JSON_MODE_0444",
        },
        "multiplicity": {
            "family": "ALL_518_SUPPORT_ELIGIBLE_BATCH3_RULES",
            "maximum_finalists": 4,
            "method": "BENJAMINI_HOCHBERG",
            "missing_or_error_p_value": 1,
            "one_sided_q_denominator": 20,
            "one_sided_q_numerator": 1,
            "p_star": inherited["multiplicity"]["p_star"],
            "zero_daily_differences": inherited["multiplicity"]["zero_daily_differences"],
        },
        "nulls": inherited["nulls"],
        "schema_version": AI_PATTERN_EXHAUSTIVE_CONFIG_SCHEMA,
        "scope": {
            "design_timing": "RETROSPECTIVE_EXPANSION_AFTER_INITIAL_12_SEARCH_RESULTS_OBSERVED",
            "embargo_access": "PROHIBITED",
            "fresh_preregistered_or_oos_claim": False,
            "holdout_access": "PROHIBITED_UNTIL_EXHAUSTIVE_SEARCH_FINAL_ARTIFACT",
            "search_only": True,
            "walk_forward_access": "PROHIBITED_UNTIL_EXHAUSTIVE_SEARCH_FINAL_ARTIFACT",
        },
        "search_gates": inherited["search_gates"],
        "source_lineage": {
            "dataset_handoff_sha256": EXPECTED_DATASET_HANDOFF_SHA256,
            "dataset_manifest_sha256": EXPECTED_DATASET_MANIFEST_SHA256,
            "source_manifest_sha256": EXPECTED_SOURCE_MANIFEST_SHA256,
            "split_plan_sha256": EXPECTED_SPLIT_PLAN_SHA256,
        },
        "status": {
            "database_mutation": False,
            "network_access": False,
            "paper_live_or_promotion_authority": False,
            "physical_holdout_isolation": False,
            "strict_backtest_claim": False,
            "strict_sealed_holdout_claim": False,
        },
    }


def expected_ai_pattern_exhaustive_contract() -> dict[str, object]:
    """Return an isolated copy of the non-provenance contract."""

    return json.loads(canonical_json_bytes(_static_contract()))


def _toml_assignment(key: str, value: object) -> str:
    if isinstance(value, bool):
        encoded = "true" if value else "false"
    elif isinstance(value, int):
        encoded = str(value)
    elif isinstance(value, str):
        encoded = json.dumps(value, ensure_ascii=True)
    elif isinstance(value, list) and all(isinstance(item, str) for item in value):
        encoded = "[" + ", ".join(json.dumps(item) for item in value) + "]"
    else:  # pragma: no cover - static contract is intentionally TOML-flat
        raise AIPatternExhaustiveConfigError(f"cannot render TOML field {key}")
    return f"{key} = {encoded}"


def render_ai_pattern_exhaustive_toml_template() -> str:
    """Render a data-only precommit with deliberately invalid provenance."""

    document = {
        **_static_contract(),
        "code_commit": "PENDING_COMMITTED_EVALUATOR_GIT_SHA",
        "dependency_lock_sha256": "PENDING_DEPENDENCY_LOCK_SHA256",
        "evaluator_implementation_sha256": "PENDING_EVALUATOR_IMPLEMENTATION_SHA256",
        "precommitted_at_utc": "PENDING_UTC_TIMESTAMP",
    }
    scalar_keys = sorted(key for key, value in document.items() if not isinstance(value, dict))
    lines = [_toml_assignment(key, document[key]) for key in scalar_keys]
    for section in sorted(key for key, value in document.items() if isinstance(value, dict)):
        lines.extend(("", f"[{section}]"))
        table = document[section]
        if not isinstance(table, dict):  # pragma: no cover
            raise AIPatternExhaustiveConfigError("template table lost its type")
        lines.extend(_toml_assignment(key, table[key]) for key in sorted(table))
    return "\n".join(lines) + "\n"


def _implementation_paths(project_root: Path) -> tuple[str, ...]:
    package = project_root / "src/systematic_fx"
    if package.is_symlink() or not package.is_dir():
        raise AIPatternExhaustiveConfigError("systematic_fx source tree is unsafe")
    fixed = (
        *_IMPLEMENTATION_SINGLE_FILES,
        *_REUSED_EVALUATOR_SCRIPTS,
        *EXHAUSTIVE_IMPLEMENTATION_SCRIPTS,
    )
    paths = list(fixed)
    for relative in fixed:
        path = project_root / relative
        if path.is_symlink() or not path.is_file():
            raise AIPatternExhaustiveConfigError("exhaustive runtime file is unsafe")
    for path in package.rglob("*.py"):
        if path.is_symlink() or not path.is_file() or "__pycache__" in path.parts:
            raise AIPatternExhaustiveConfigError("systematic_fx source tree contains unsafe source")
        paths.append(path.relative_to(project_root).as_posix())
    ordered = tuple(sorted(paths))
    if len(set(ordered)) != len(ordered):
        raise AIPatternExhaustiveConfigError("exhaustive runtime paths are duplicated")
    return ordered


def exhaustive_implementation_sha256(project_root: Path | str) -> str:
    """Hash the full source closure authorized for exhaustive Search."""

    root = Path(project_root).expanduser().resolve(strict=True)
    modules = []
    for relative in _implementation_paths(root):
        payload = (root / relative).read_bytes()
        modules.append(
            {
                "byte_size": len(payload),
                "relative_path": relative,
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
    return canonical_sha256(
        {
            "implementation_schema": (
                "systematic_fx.ai_pattern_exhaustive_search_implementation.v1"
            ),
            "modules": modules,
        }
    )


def _git(project_root: Path, *arguments: str) -> bytes:
    try:
        process = subprocess.run(
            ["git", "-C", str(project_root), *arguments],
            check=False,
            capture_output=True,
        )
    except OSError as error:
        raise AIPatternExhaustiveConfigError("Git provenance cannot be verified") from error
    if process.returncode != 0:
        raise AIPatternExhaustiveConfigError("Git provenance cannot be verified")
    return process.stdout


def verify_committed_exhaustive_implementation(project_root: Path, code_commit: str) -> None:
    """Require every runtime byte to equal a regular blob in ``code_commit``."""

    if (
        _COMMIT.fullmatch(code_commit) is None
        or not set(code_commit) - {"0"}
        or _git(project_root, "cat-file", "-t", code_commit).strip() != b"commit"
    ):
        raise AIPatternExhaustiveConfigError("exhaustive code commit is invalid")
    current = _implementation_paths(project_root)
    committed_package = tuple(
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
            .decode()
            .splitlines()
            if line.endswith(".py") and "__pycache__" not in Path(line).parts
        )
    )
    expected = tuple(
        sorted(
            (
                *_IMPLEMENTATION_SINGLE_FILES,
                *_REUSED_EVALUATOR_SCRIPTS,
                *EXHAUSTIVE_IMPLEMENTATION_SCRIPTS,
                *committed_package,
            )
        )
    )
    if current != expected:
        raise AIPatternExhaustiveConfigError("exhaustive runtime file set differs")
    for relative in current:
        if (
            _git(project_root, "show", f"{code_commit}:{relative}")
            != (project_root / relative).read_bytes()
        ):
            raise AIPatternExhaustiveConfigError("exhaustive runtime differs from its commit")


@dataclass(frozen=True, slots=True)
class AIPatternExhaustiveConfig:
    path: Path
    file_sha256: str
    semantic_sha256: str
    code_commit: str
    evaluator_implementation_sha256: str
    dependency_lock_sha256: str
    precommitted_at_utc: str
    canonical_bytes: bytes

    def as_dict(self) -> dict[str, object]:
        value = json.loads(self.canonical_bytes)
        if not isinstance(value, dict):  # pragma: no cover
            raise AIPatternExhaustiveConfigError("config root is not an object")
        return value


def _validated_document(value: object) -> dict[str, object]:
    expected = _static_contract()
    dynamic = {
        "code_commit",
        "dependency_lock_sha256",
        "evaluator_implementation_sha256",
        "precommitted_at_utc",
    }
    if not isinstance(value, dict) or set(value) != set(expected) | dynamic:
        raise AIPatternExhaustiveConfigError("exhaustive config schema differs")
    for key, expected_value in expected.items():
        if value[key] != expected_value:
            raise AIPatternExhaustiveConfigError(f"exhaustive config {key} drifted")
    if _COMMIT.fullmatch(str(value["code_commit"])) is None:
        raise AIPatternExhaustiveConfigError("code_commit is not a full Git SHA")
    for key in ("dependency_lock_sha256", "evaluator_implementation_sha256"):
        if _SHA256.fullmatch(str(value[key])) is None:
            raise AIPatternExhaustiveConfigError(f"{key} is not a SHA-256")
    if _UTC_TIMESTAMP.fullmatch(str(value["precommitted_at_utc"])) is None:
        raise AIPatternExhaustiveConfigError("precommitted_at_utc is not canonical UTC")
    return value


def load_ai_pattern_exhaustive_config(
    project_root: Path | str,
) -> AIPatternExhaustiveConfig:
    """Load, validate, and bind the committed exhaustive Search runtime."""

    root = Path(project_root).expanduser().resolve(strict=True)
    path = root / AI_PATTERN_EXHAUSTIVE_CONFIG_RELATIVE_PATH
    if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root):
        raise AIPatternExhaustiveConfigError("exhaustive config is missing or unsafe")
    raw = path.read_bytes()
    try:
        document = _validated_document(tomllib.loads(raw.decode()))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise AIPatternExhaustiveConfigError("exhaustive config is invalid TOML") from error
    code_commit = str(document["code_commit"])
    implementation = str(document["evaluator_implementation_sha256"])
    dependency = str(document["dependency_lock_sha256"])
    if exhaustive_implementation_sha256(root) != implementation:
        raise AIPatternExhaustiveConfigError("exhaustive implementation identity drifted")
    if dependency_lock_sha256(root) != dependency:
        raise AIPatternExhaustiveConfigError("exhaustive dependency identity drifted")
    verify_committed_exhaustive_implementation(root, code_commit)
    return AIPatternExhaustiveConfig(
        path=path.resolve(strict=True),
        file_sha256=hashlib.sha256(raw).hexdigest(),
        semantic_sha256=canonical_sha256(document),
        code_commit=code_commit,
        evaluator_implementation_sha256=implementation,
        dependency_lock_sha256=dependency,
        precommitted_at_utc=str(document["precommitted_at_utc"]),
        canonical_bytes=canonical_json_bytes(document),
    )


__all__ = [
    "AI_PATTERN_EXHAUSTIVE_AUTHORITY",
    "AI_PATTERN_EXHAUSTIVE_CONFIG_RELATIVE_PATH",
    "AI_PATTERN_EXHAUSTIVE_CONFIG_SCHEMA",
    "BATCH_COUNT",
    "BATCH_SIZE",
    "EXHAUSTIVE_IMPLEMENTATION_SCRIPTS",
    "EXPECTED_INITIAL_SEARCH_MASKS_SHA256",
    "EXPECTED_INITIAL_SEARCH_RESULT_SHA256",
    "FINAL_BATCH_SIZE",
    "INITIAL_FAMILY_COUNT",
    "REMAINING_FAMILY_COUNT",
    "SUPPORT_ELIGIBLE_FAMILY_COUNT",
    "AIPatternExhaustiveConfig",
    "AIPatternExhaustiveConfigError",
    "exhaustive_implementation_sha256",
    "expected_ai_pattern_exhaustive_contract",
    "load_ai_pattern_exhaustive_config",
    "render_ai_pattern_exhaustive_toml_template",
    "verify_committed_exhaustive_implementation",
]
