"""Data-only precommit contract for delayed multi-timeframe research v1.

The source closure must be committed first.  Only then may
``render_ai_delayed_mtf_toml_template`` be rendered, have its four provenance
markers filled, and be committed as the separate data-only configuration.  The
loader compares every current ``src/**/*.py`` and ``scripts/**/*.py`` byte,
``pyproject.toml``, and ``uv.lock`` to the named source commit.
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
    DATASET_MANIFEST_RELATIVE_PATH,
    EXPECTED_DATASET_HANDOFF_SHA256,
    EXPECTED_DATASET_MANIFEST_SHA256,
    EXPECTED_SOURCE_MANIFEST_SHA256,
    EXPECTED_SPLIT_PLAN_SHA256,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.provenance import dependency_lock_sha256

AI_DELAYED_MTF_CONFIG_SCHEMA: Final = "systematic_fx.ai_delayed_mtf_config.v1"
AI_DELAYED_MTF_CONFIG_RELATIVE_PATH: Final = Path("configs/research/ai_delayed_mtf_v1.toml")
AI_DELAYED_MTF_AUTHORITY: Final = "UNSEALED_LOCAL_DELAYED_MTF_RESEARCH"
EXPECTED_ACTIVE_CALENDAR_SHA256: Final = (
    "b414eae72afdb1c149977ff0ea5b672069380997d91e74adf0407e35836e8ac1"
)
EXPECTED_CANDIDATE_COUNT: Final = 100
EXPECTED_SEARCH_MAXIMUM: Final = 8
EXPECTED_HOLDOUT_MAXIMUM: Final = 3
_IMPLEMENTATION_SINGLE_FILES: Final = ("pyproject.toml", "uv.lock")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_COMMIT = re.compile(r"[0-9a-f]{40}")
_UTC_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")


class AIDelayedMTFConfigError(ValueError):
    """The delayed-MTF precommit or committed implementation differs."""


def _engine_bindings() -> tuple[dict[str, object], dict[str, object]]:
    from scripts.ai_delayed_mtf_engine import (
        CANDIDATE_CATALOG_COUNT,
        CANDIDATE_CATALOG_SHA256,
        build_delayed_mtf_candidate_catalog,
        delayed_mtf_engine_contract,
    )

    contract = json.loads(canonical_json_bytes(delayed_mtf_engine_contract()))
    catalog_object = build_delayed_mtf_candidate_catalog()
    catalog = json.loads(canonical_json_bytes(catalog_object.as_dict()))
    if (
        not isinstance(contract, dict)
        or not isinstance(catalog, dict)
        or CANDIDATE_CATALOG_COUNT != EXPECTED_CANDIDATE_COUNT
        or catalog.get("candidate_count") != EXPECTED_CANDIDATE_COUNT
        or catalog.get("catalog_sha256") != CANDIDATE_CATALOG_SHA256
        or not isinstance(catalog.get("candidates"), list)
        or len(catalog["candidates"]) != EXPECTED_CANDIDATE_COUNT
        or canonical_sha256(catalog["candidates"]) != CANDIDATE_CATALOG_SHA256
    ):
        raise AIDelayedMTFConfigError("engine catalog/contract binding differs")
    return contract, catalog


def _static_contract() -> dict[str, object]:
    engine, catalog = _engine_bindings()
    catalog_sha256 = str(catalog["catalog_sha256"])
    return {
        "authority": AI_DELAYED_MTF_AUTHORITY,
        "catalog": {
            "candidate_catalog_sha256": catalog_sha256,
            "candidate_count": EXPECTED_CANDIDATE_COUNT,
            "compression_breakout_count": 24,
            "delayed_macd_count": 36,
            "family_count": EXPECTED_CANDIDATE_COUNT,
            "global_multiplicity_family_count": EXPECTED_CANDIDATE_COUNT,
            "meta_label_count": 0,
            "pullback_continuation_count": 24,
            "range_mean_reversion_count": 16,
            "stage_subset_order": "PRESERVE_CATALOG_SEMANTIC_SELECTION_RANK_ORDER",
        },
        "config_id": "ai_delayed_mtf_v1",
        "continuity": {
            "active_calendar_sha256": EXPECTED_ACTIVE_CALENDAR_SHA256,
            "cross_date_carry_policy": (
                "SAME_CONTRACT_OUTCOME_SPAN_ADJACENT_ALLOWLISTED_ACTIVE_DATE_GAP_LE_96H"
            ),
            "execution_continuity": "EXACT_SIGNAL_SEGMENT_ONLY",
            "indicator_policy": "INDICATOR_CONTINUITY_V1",
            "intra_date_gap_policy": "RESET",
            "maximum_cross_date_wall_gap_hours": 96,
            "official_schedule_evidence": False,
            "segment_contract_or_outcome_span_change_policy": "RESET",
            "stage_or_fold_start_policy": "RESET",
            "weekend_carry_allowed_when_predicates_hold": True,
        },
        "dataset": {
            "active_date_count": 1413,
            "active_date_end": "2026-07-31",
            "active_date_start": "2022-01-03",
            "active_calendar_payload": "BARE_ORDERED_ISO_DATE_STRING_LIST",
            "dataset_handoff_sha256": EXPECTED_DATASET_HANDOFF_SHA256,
            "dataset_manifest_relative_path": DATASET_MANIFEST_RELATIVE_PATH.as_posix(),
            "dataset_manifest_sha256": EXPECTED_DATASET_MANIFEST_SHA256,
            "embargo_access": "PROHIBITED",
            "source_manifest_sha256": EXPECTED_SOURCE_MANIFEST_SHA256,
            "split_plan_sha256": EXPECTED_SPLIT_PLAN_SHA256,
            "timeframes_seconds": [1, 300, 1800, 3600],
        },
        "engine": {
            "allowed_legacy_container_symbols": ["BarWithOutcomeSpan", "SignalMask"],
            "contract_canonical_json": canonical_json_bytes(engine).decode("ascii"),
            "contract_sha256": canonical_sha256(engine),
            "existing_holdout_engine_reuse": (
                "OLD_EXECUTION_AND_SIGNAL_LOGIC_PROHIBITED_NEUTRAL_DATA_CONTAINERS_ONLY"
            ),
            "fixed_horizon_primary_seconds": 10_800,
            "fixed_take_profit_or_stop_loss": False,
            "new_engine_module": "scripts.ai_delayed_mtf_engine",
        },
        "holdout_gates": {
            "active_entry_days_minimum": 18,
            "both_halves_net_ticks_strictly_positive": True,
            "contract_count_minimum": 2,
            "fills_minimum": 24,
            "holm_family_maximum": EXPECTED_HOLDOUT_MAXIMUM,
            "holm_alpha_denominator": 20,
            "holm_alpha_numerator": 1,
            "holm_method": "HOLM_STEP_DOWN",
            "net_over_maximum_drawdown_minimum_denominator": 4,
            "net_over_maximum_drawdown_minimum_numerator": 3,
            "net_ticks_strictly_positive": True,
            "null_deltas_strictly_positive": True,
            "profit_factor_minimum_denominator": 10,
            "profit_factor_minimum_numerator": 11,
            "terminal_fail_status": ("ONE_SHOT_UNSEALED_DELAYED_MTF_HOLDOUT_DIAGNOSTIC_FAIL"),
            "terminal_pass_rule": "AT_LEAST_ONE_HOLM_REJECT_AND_ALL_GATES_PASS",
            "terminal_pass_status": ("ONE_SHOT_UNSEALED_DELAYED_MTF_HOLDOUT_DIAGNOSTIC_PASS"),
        },
        "lifecycle": {
            "artifact_publication": "CONTENT_ADDRESSED_CANONICAL_JSON_MODE_0444",
            "candidate_change_after_precommit": "PROHIBITED",
            "crash_policy": "REPLAY_VERIFY_AND_RESUME_NEXT_EVENT",
            "failed_branch": "ANY_NONTERMINAL_INTEGRITY_FAILURE_TO_FAILED_TERMINAL",
            "holdout_feature_open_barrier": ("WF_MAXIMUM_3_FINALISTS_THEN_ONE_SHOT_AUTHORIZATION"),
            "holdout_masks_before_holdout_one_second_loader": True,
            "ledger": "ATOMIC_APPEND_ONLY_PREDECESSOR_HASHED_SINGLE_WRITER",
            "partial_or_intermediate_result_release": "PROHIBITED",
            "search_masks_before_search_one_second_loader": True,
            "stage_order_main": [
                "PRECOMMITTED",
                "SEARCH_MASKS_FROZEN_ALL_100",
                "SEARCH_RESULTS_RELEASED",
                "WALK_FORWARD_MASKS_FROZEN_SELECTED_MAXIMUM_8_ALL_5_FOLDS",
                "WALK_FORWARD_RESULTS_RELEASED_ALL_5_FOLDS",
                "HOLDOUT_AUTHORIZED_MAXIMUM_3",
                "HOLDOUT_MASKS_FROZEN",
                "HOLDOUT_RESULTS_RELEASED",
                "COMPLETED",
            ],
            "stage_order_no_search_finalists": [
                "SEARCH_RESULTS_RELEASED_EMPTY",
                "WALK_FORWARD_SKIPPED",
                "HOLDOUT_SKIPPED",
                "COMPLETED_NO_SEARCH_FINALISTS",
            ],
            "stage_order_no_walk_forward_finalists": [
                "WALK_FORWARD_RESULTS_RELEASED_EMPTY",
                "HOLDOUT_SKIPPED",
                "COMPLETED_NO_WALK_FORWARD_FINALISTS",
            ],
            "walk_forward_feature_open_barrier": "SEARCH_FROZEN_SELECTION_MAXIMUM_8",
            "walk_forward_masks_before_any_walk_forward_one_second_loader": True,
        },
        "multiplicity": {
            "daily_grouping": "SIGNAL_ANCHOR_DECISION_DATE_NOT_EXIT_DATE",
            "global_search_family_count": EXPECTED_CANDIDATE_COUNT,
            "holdout_method": "HOLM_STEP_DOWN",
            "missing_or_error_p_value": 1,
            "p_star": "MAX_ZERO_SHIFT_MATCHED_PAIRED_DAILY_EXACT_ONE_SIDED_P",
            "search_method": "BENJAMINI_HOCHBERG",
            "search_q_denominator": 20,
            "search_q_numerator": 1,
            "walk_forward_family": "FROZEN_SEARCH_SELECTION_MAXIMUM_8",
            "walk_forward_method": "BENJAMINI_HOCHBERG",
            "walk_forward_q_denominator": 20,
            "walk_forward_q_numerator": 1,
        },
        "nulls": {
            "master_seed": "ai-delayed-mtf-v1",
            "seed_type": "EXACT_UTF8_STRING",
        },
        "provenance": {
            "config_policy": "SOURCE_COMMIT_FIRST_THEN_DATA_ONLY_CONFIG_COMMIT",
            "implementation_closure": "ALL_SRC_PY_ALL_SCRIPTS_PY_PYPROJECT_UV_LOCK",
            "verify_policy": "FRESH_READ_ONLY_FULL_RESULT_RECOMPUTATION",
        },
        "schema_version": AI_DELAYED_MTF_CONFIG_SCHEMA,
        "scope": {
            "holdout_evidence": "ONE_SHOT_UNSEALED_LOCAL_DIAGNOSTIC",
            "physical_holdout_isolation": False,
            "search_claim": "RETROSPECTIVE_DEVELOPMENT_DIAGNOSTIC_NOT_OOS_EVIDENCE",
            "strict_backtest_claim": False,
            "walk_forward_claim": "FIRST_OOS_EVIDENCE_IF_PRECOMMITTED_BEFORE_OPEN",
        },
        "search_gates": {
            "active_entry_days_minimum": 24,
            "bh_q_denominator": 20,
            "bh_q_numerator": 1,
            "fills_each_reporting_block_minimum": 4,
            "fills_minimum": 36,
            "maximum_selection": EXPECTED_SEARCH_MAXIMUM,
            "net_ticks_strictly_positive": True,
            "null_deltas_strictly_positive": True,
            "positive_reporting_blocks_minimum": 3,
            "profit_factor_minimum_denominator": 20,
            "profit_factor_minimum_numerator": 21,
            "qualification": "BH_REJECT_AND_EVERY_SAMPLE_AND_ECONOMIC_GATE_PASS",
            "ranking": [
                "P_STAR_ASC",
                "WORST_GROUP_EV_DESC",
                "AGGREGATE_EV_DESC",
                "PROFIT_FACTOR_DESC",
                "CATALOG_SELECTION_RANK_ASC",
            ],
            "raw_signals_each_reporting_block_minimum": 6,
            "raw_signals_minimum": 48,
            "reporting_block_count": 4,
            "signal_days_minimum": 30,
            "worst_reporting_block_ev_ticks_minimum": -14,
        },
        "status": {
            "database_mutation": False,
            "network_access": False,
            "paper_live_or_promotion_authority": False,
            "physical_holdout_isolation": False,
            "strict_backtest_claim": False,
            "strict_sealed_holdout_claim": False,
        },
        "warmup": {
            "cross_stage_or_embargo_warmup_rows": "PROHIBITED",
            "engine_contract_sha256": canonical_sha256(engine),
            "first_post_closure_cross_semantics": "OBSERVATION_TIME_CHART_SEMANTICS",
            "stage_local_only": True,
            "stage_start_indicator_reset": True,
            "stage_start_warmup_ineligible": True,
        },
        "walk_forward_gates": {
            "active_entry_days_each_fold_minimum": 10,
            "active_entry_days_minimum": 75,
            "bh_q_denominator": 20,
            "bh_q_numerator": 1,
            "contract_count_minimum": 5,
            "fills_each_fold_minimum": 12,
            "fills_minimum": 100,
            "maximum_finalists": EXPECTED_HOLDOUT_MAXIMUM,
            "minimum_positive_folds": 4,
            "net_over_maximum_drawdown_minimum_denominator": 1,
            "net_over_maximum_drawdown_minimum_numerator": 1,
            "net_ticks_strictly_positive": True,
            "null_deltas_strictly_positive": True,
            "profit_factor_minimum_denominator": 10,
            "profit_factor_minimum_numerator": 11,
            "qualification": "BH_REJECT_AND_EVERY_SAMPLE_AND_ECONOMIC_GATE_PASS",
            "ranking": [
                "P_STAR_ASC",
                "WORST_GROUP_EV_DESC",
                "AGGREGATE_EV_DESC",
                "PROFIT_FACTOR_DESC",
                "CATALOG_SELECTION_RANK_ASC",
            ],
            "worst_fold_profit_factor_floor_denominator": 10,
            "worst_fold_profit_factor_floor_numerator": 7,
            "worst_loss_over_median_positive_maximum_denominator": 2,
            "worst_loss_over_median_positive_maximum_numerator": 3,
        },
    }


def expected_ai_delayed_mtf_contract() -> dict[str, object]:
    """Return an isolated copy of the exact non-provenance contract."""

    return json.loads(canonical_json_bytes(_static_contract()))


def _toml_assignment(key: str, value: object) -> str:
    if isinstance(value, bool):
        encoded = "true" if value else "false"
    elif isinstance(value, int):
        encoded = str(value)
    elif isinstance(value, str):
        encoded = json.dumps(value, ensure_ascii=True)
    elif isinstance(value, list) and all(isinstance(item, str) for item in value):
        encoded = "[" + ", ".join(json.dumps(item, ensure_ascii=True) for item in value) + "]"
    elif isinstance(value, list) and all(
        isinstance(item, int) and not isinstance(item, bool) for item in value
    ):
        encoded = "[" + ", ".join(str(item) for item in value) + "]"
    else:  # pragma: no cover - the frozen contract remains TOML-flat by section
        raise AIDelayedMTFConfigError(f"cannot render TOML field {key}")
    return f"{key} = {encoded}"


def render_ai_delayed_mtf_toml_template() -> str:
    """Render the config-only commit template with invalid provenance markers."""

    document = {
        **_static_contract(),
        "code_commit": "PENDING_COMMITTED_SOURCE_GIT_SHA",
        "dependency_lock_sha256": "PENDING_DEPENDENCY_LOCK_SHA256",
        "implementation_sha256": "PENDING_IMPLEMENTATION_SHA256",
        "precommitted_at_utc": "PENDING_UTC_TIMESTAMP",
    }
    scalar_keys = sorted(key for key, value in document.items() if not isinstance(value, dict))
    lines = [_toml_assignment(key, document[key]) for key in scalar_keys]
    for section in sorted(key for key, value in document.items() if isinstance(value, dict)):
        lines.extend(("", f"[{section}]"))
        table = document[section]
        if not isinstance(table, dict):  # pragma: no cover
            raise AIDelayedMTFConfigError("template section lost its table type")
        lines.extend(_toml_assignment(key, table[key]) for key in sorted(table))
    return "\n".join(lines) + "\n"


def _implementation_paths(project_root: Path) -> tuple[str, ...]:
    paths = list(_IMPLEMENTATION_SINGLE_FILES)
    for relative_root in (Path("src"), Path("scripts")):
        tree = project_root / relative_root
        if tree.is_symlink() or not tree.is_dir():
            raise AIDelayedMTFConfigError("implementation source tree is missing or symbolic")
        for path in tree.rglob("*.py"):
            if path.is_symlink() or not path.is_file() or "__pycache__" in path.parts:
                raise AIDelayedMTFConfigError("implementation source tree contains unsafe source")
            paths.append(path.relative_to(project_root).as_posix())
    for relative in _IMPLEMENTATION_SINGLE_FILES:
        path = project_root / relative
        if path.is_symlink() or not path.is_file():
            raise AIDelayedMTFConfigError("implementation project file is unsafe")
    ordered = tuple(sorted(paths))
    if len(set(ordered)) != len(ordered):
        raise AIDelayedMTFConfigError("implementation path set is duplicated")
    return ordered


def delayed_mtf_implementation_sha256(project_root: Path | str) -> str:
    """Hash the full source/scripts/project implementation closure."""

    root = Path(project_root).expanduser().resolve(strict=True)
    files = []
    for relative in _implementation_paths(root):
        payload = (root / relative).read_bytes()
        files.append(
            {
                "byte_size": len(payload),
                "relative_path": relative,
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
    return canonical_sha256(
        {
            "implementation_schema": "systematic_fx.ai_delayed_mtf_implementation.v1",
            "files": files,
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
        raise AIDelayedMTFConfigError("Git provenance cannot be verified") from error
    if process.returncode != 0:
        raise AIDelayedMTFConfigError("Git provenance cannot be verified")
    return process.stdout


def verify_committed_delayed_mtf_implementation(
    project_root: Path,
    code_commit: str,
) -> None:
    """Require the complete runtime file set and bytes to match one commit."""

    if (
        _COMMIT.fullmatch(code_commit) is None
        or not set(code_commit) - {"0"}
        or _git(project_root, "cat-file", "-t", code_commit).strip() != b"commit"
    ):
        raise AIDelayedMTFConfigError("delayed-MTF source commit is invalid")
    current = _implementation_paths(project_root)
    committed_sources = tuple(
        sorted(
            line
            for line in _git(
                project_root,
                "ls-tree",
                "-r",
                "--name-only",
                code_commit,
                "--",
                "src",
                "scripts",
            )
            .decode("utf-8")
            .splitlines()
            if line.endswith(".py") and "__pycache__" not in Path(line).parts
        )
    )
    expected = tuple(sorted((*_IMPLEMENTATION_SINGLE_FILES, *committed_sources)))
    if current != expected:
        raise AIDelayedMTFConfigError("implementation file set differs from source commit")
    for relative in current:
        if (
            _git(project_root, "cat-file", "-t", f"{code_commit}:{relative}").strip() != b"blob"
            or _git(project_root, "show", f"{code_commit}:{relative}")
            != (project_root / relative).read_bytes()
        ):
            raise AIDelayedMTFConfigError("implementation bytes differ from source commit")


@dataclass(frozen=True, slots=True)
class AIDelayedMTFConfig:
    path: Path
    file_sha256: str
    semantic_sha256: str
    code_commit: str
    implementation_sha256: str
    dependency_lock_sha256: str
    precommitted_at_utc: str
    canonical_bytes: bytes

    def as_dict(self) -> dict[str, object]:
        value = json.loads(self.canonical_bytes)
        if not isinstance(value, dict):  # pragma: no cover
            raise AIDelayedMTFConfigError("config root is not an object")
        return value


def _validated_document(value: object) -> dict[str, object]:
    expected = _static_contract()
    dynamic = {
        "code_commit",
        "dependency_lock_sha256",
        "implementation_sha256",
        "precommitted_at_utc",
    }
    if not isinstance(value, dict) or set(value) != set(expected) | dynamic:
        raise AIDelayedMTFConfigError("delayed-MTF config schema differs")
    for key, expected_value in expected.items():
        if value[key] != expected_value:
            raise AIDelayedMTFConfigError(f"delayed-MTF config {key} drifted")
    if _COMMIT.fullmatch(str(value["code_commit"])) is None:
        raise AIDelayedMTFConfigError("code_commit is not a full Git SHA")
    for key in ("dependency_lock_sha256", "implementation_sha256"):
        if _SHA256.fullmatch(str(value[key])) is None:
            raise AIDelayedMTFConfigError(f"{key} is not a SHA-256")
    if _UTC_TIMESTAMP.fullmatch(str(value["precommitted_at_utc"])) is None:
        raise AIDelayedMTFConfigError("precommitted_at_utc is not canonical UTC")
    return value


def load_ai_delayed_mtf_config(project_root: Path | str) -> AIDelayedMTFConfig:
    """Load and bind the data-only config to exact committed runtime bytes."""

    root = Path(project_root).expanduser().resolve(strict=True)
    path = root / AI_DELAYED_MTF_CONFIG_RELATIVE_PATH
    if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root):
        raise AIDelayedMTFConfigError("delayed-MTF config is missing or unsafe")
    raw = path.read_bytes()
    try:
        document = _validated_document(tomllib.loads(raw.decode("utf-8")))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise AIDelayedMTFConfigError("delayed-MTF config is invalid TOML") from error
    code_commit = str(document["code_commit"])
    implementation = str(document["implementation_sha256"])
    dependency = str(document["dependency_lock_sha256"])
    if delayed_mtf_implementation_sha256(root) != implementation:
        raise AIDelayedMTFConfigError("delayed-MTF implementation identity drifted")
    if dependency_lock_sha256(root) != dependency:
        raise AIDelayedMTFConfigError("delayed-MTF dependency identity drifted")
    verify_committed_delayed_mtf_implementation(root, code_commit)
    return AIDelayedMTFConfig(
        path=path.resolve(strict=True),
        file_sha256=hashlib.sha256(raw).hexdigest(),
        semantic_sha256=canonical_sha256(document),
        code_commit=code_commit,
        implementation_sha256=implementation,
        dependency_lock_sha256=dependency,
        precommitted_at_utc=str(document["precommitted_at_utc"]),
        canonical_bytes=canonical_json_bytes(document),
    )


__all__ = [
    "AI_DELAYED_MTF_AUTHORITY",
    "AI_DELAYED_MTF_CONFIG_RELATIVE_PATH",
    "AI_DELAYED_MTF_CONFIG_SCHEMA",
    "EXPECTED_ACTIVE_CALENDAR_SHA256",
    "EXPECTED_CANDIDATE_COUNT",
    "EXPECTED_HOLDOUT_MAXIMUM",
    "EXPECTED_SEARCH_MAXIMUM",
    "AIDelayedMTFConfig",
    "AIDelayedMTFConfigError",
    "delayed_mtf_implementation_sha256",
    "expected_ai_delayed_mtf_contract",
    "load_ai_delayed_mtf_config",
    "render_ai_delayed_mtf_toml_template",
    "verify_committed_delayed_mtf_implementation",
]
