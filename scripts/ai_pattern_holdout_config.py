"""Frozen contract for the governed Batch 3 performance evaluation."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from systematic_fx.research.hypotheses import (
    canonical_json_bytes,
    canonical_sha256,
)
from systematic_fx.research.provenance import dependency_lock_sha256

AI_PATTERN_HOLDOUT_CONFIG_SCHEMA: Final = "systematic_fx.ai_pattern_holdout_config.v1"
AI_PATTERN_HOLDOUT_CONFIG_RELATIVE_PATH: Final = Path("configs/research/ai_pattern_holdout_v1.toml")
AI_PATTERN_HOLDOUT_AUTHORITY: Final = "UNSEALED_LOCAL_BAR_SCREENING_HOLDOUT"

EXPECTED_BATCH3_GOVERNED_REQUEST_SHA256: Final = (
    "17df16a432cd544c1ffde7fd43add6e20272c90d1e8358487ddf5f804b59303c"
)
EXPECTED_BATCH3_PROPOSAL_BATCH_SHA256: Final = (
    "dfef5bad188f79af8fa63a6e74f8c9609df34778a9a050278f3740766d24ee4e"
)
EXPECTED_BATCH3_PROPOSAL_REPORT_SHA256: Final = (
    "c69c9273fcb53bceec03f15e96f952adc1f2e32c81ffacfb6a679ff99e6c4278"
)
EXPECTED_DATASET_MANIFEST_SHA256: Final = (
    "e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc"
)
EXPECTED_DATASET_HANDOFF_SHA256: Final = (
    "26b1bb96f7323cae13bbe5d670c12f3e85615bbb9aab56932ce6523e67af7b00"
)
EXPECTED_SPLIT_PLAN_SHA256: Final = (
    "5594725f6769a706018d414a5b27e3903f1d7d1cc22c98e93b6e973ead1af043"
)
EXPECTED_SOURCE_MANIFEST_SHA256: Final = (
    "14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de"
)
DATASET_MANIFEST_RELATIVE_PATH: Final = Path(
    "data/derived/bar_patterns/trade_bar_dataset_manifest/"
    "identity_sha256=b0ecab04cdd3626d3c488f9108c8e9184f5dd610f51950ab7e7f74a5b7524297/"
    f"sha256={EXPECTED_DATASET_MANIFEST_SHA256}.json"
)
AI_PATTERN_BATCH3_ROOT: Final = Path("data/derived/bar_patterns/ai_pattern_discovery_v3")
AI_PATTERN_BATCH3_BATCH_RELATIVE_PATH: Final = AI_PATTERN_BATCH3_ROOT / (
    f"artifacts/directional-proposal-batch-{EXPECTED_BATCH3_PROPOSAL_BATCH_SHA256}.json"
)
AI_PATTERN_BATCH3_REQUEST_RELATIVE_PATH: Final = AI_PATTERN_BATCH3_ROOT / (
    f"artifacts/directional-proposal-request-{EXPECTED_BATCH3_GOVERNED_REQUEST_SHA256}.json"
)
AI_PATTERN_BATCH3_REPORT_RELATIVE_PATH: Final = AI_PATTERN_BATCH3_ROOT / (
    f"artifacts/directional-proposal-report-{EXPECTED_BATCH3_PROPOSAL_REPORT_SHA256}.json"
)

FINAL_HOLDOUT_STATUSES: Final = (
    "NO_SEARCH_FINALISTS_HOLDOUT_NOT_OPENED",
    "NO_WALK_FORWARD_FINALISTS_HOLDOUT_NOT_OPENED",
    "ONE_SHOT_UNSEALED_BAR_HOLDOUT_DIAGNOSTIC_PASS",
    "ONE_SHOT_UNSEALED_BAR_HOLDOUT_DIAGNOSTIC_FAIL",
    "ONE_SHOT_UNSEALED_BAR_HOLDOUT_DIAGNOSTIC_INCONCLUSIVE",
)
HOLDOUT_IMPLEMENTATION_SCRIPTS: Final = (
    "scripts/ai_pattern_holdout_config.py",
    "scripts/ai_pattern_holdout_engine.py",
    "scripts/ai_pattern_holdout_run.py",
    "scripts/run_ai_pattern_holdout.py",
)
_IMPLEMENTATION_SINGLE_FILES: Final = ("pyproject.toml", "uv.lock")

_SHA256 = re.compile(r"[0-9a-f]{64}")
_COMMIT = re.compile(r"[0-9a-f]{40}")
_UTC_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")


class AIPatternHoldoutConfigError(ValueError):
    """The performance precommit differs from the exact frozen contract."""


def _static_contract() -> dict[str, object]:
    return {
        "authority": AI_PATTERN_HOLDOUT_AUTHORITY,
        "batch": {
            "governed_request_sha256": EXPECTED_BATCH3_GOVERNED_REQUEST_SHA256,
            "proposal_batch_sha256": EXPECTED_BATCH3_PROPOSAL_BATCH_SHA256,
            "proposal_count": 12,
            "proposal_report_sha256": EXPECTED_BATCH3_PROPOSAL_REPORT_SHA256,
        },
        "bootstrap": {
            "enabled": False,
            "reason": "DISABLED_IN_V1",
        },
        "config_id": "ai_pattern_holdout_v1",
        "dataset": {
            "dataset_handoff_sha256": EXPECTED_DATASET_HANDOFF_SHA256,
            "dataset_manifest_relative_path": DATASET_MANIFEST_RELATIVE_PATH.as_posix(),
            "dataset_manifest_sha256": EXPECTED_DATASET_MANIFEST_SHA256,
            "embargo_access": "PROHIBITED",
            "source_manifest_sha256": EXPECTED_SOURCE_MANIFEST_SHA256,
            "split_plan_sha256": EXPECTED_SPLIT_PLAN_SHA256,
        },
        "execution": {
            "entry_adverse_ticks": 2,
            "entry_policy": ("NEXT_CONTIGUOUS_SAME_CONTRACT_SIGNAL_SEGMENT_OUTCOME_SPAN_5M_OPEN"),
            "fully_loaded_round_trip_cost_ticks": 10,
            "holding_horizon_seconds": 3_600,
            "maximum_concurrent_positions_per_pattern": 1,
            "maximum_drawdown_basis": (
                "CHRONOLOGICAL_SEQUENTIAL_FULLY_LOADED_TRADE_NET_TICKS_FROM_ZERO"
            ),
            "path_timeframe_seconds": 1,
            "profit_target_ticks": 32,
            "profit_target_trade_through_ticks": 1,
            "scenario_id": "MODERATE_COMBINED",
            "signal_timeframe_seconds": 300,
            "stop_loss_minimum_adverse_ticks": 4,
            "stop_loss_ticks": 24,
            "terminal_adverse_ticks": 2,
            "terminal_policy": "LAST_OBSERVED_1S_CLOSE_BY_HORIZON_OR_SPAN_END",
            "tie_policy": "SAME_SECOND_STOP_FIRST",
            "variable_cost_ticks": 5,
            "allocated_fixed_cost_ticks": 5,
        },
        "holdout_gates": {
            "active_entry_days_minimum": 40,
            "contract_count_minimum": 2,
            "economic_net_ticks_strictly_positive": True,
            "fills_minimum": 80,
            "holm_family": "ALL_PREREGISTERED_WF_FINALISTS_MISSING_OR_ERROR_P_EQUALS_ONE",
            "holm_one_sided_alpha_denominator": 20,
            "holm_one_sided_alpha_numerator": 1,
            "maximum_finalists": 3,
            "net_over_maximum_drawdown_minimum_denominator": 1,
            "net_over_maximum_drawdown_minimum_numerator": 1,
            "null_deltas_strictly_positive": True,
            "profit_factor_minimum_denominator": 20,
            "profit_factor_minimum_numerator": 23,
            "sixty_day_calendar_halves_both_net_positive": True,
            "sixty_day_halves_assignment": "SIGNAL_DECISION_DATE_FIRST_60_AND_LAST_60",
            "terminal_fail_rule": (
                "NO_CANDIDATE_PASSES_AND_AT_LEAST_ONE_EVALUABLE_CANDIDATE_HAS_"
                "HARD_ECONOMIC_OR_STATISTICAL_FAILURE"
            ),
            "terminal_inconclusive_rule": (
                "NO_CANDIDATE_PASSES_AND_ALL_PREREGISTERED_CANDIDATES_ARE_NULL_OR_SAMPLE_INELIGIBLE"
            ),
            "terminal_pass_rule": "AT_LEAST_ONE_PREREGISTERED_WF_FINALIST_PASSES_ALL_GATES",
        },
        "lifecycle": {
            "all_post_precommit_failures_append_failed": True,
            "data_or_censor_integrity_exception": "FAILED_LEDGER_NO_TERMINAL_RESULT",
            "holdout_access": "ONLY_AFTER_HOLDOUT_AUTHORIZED",
            "holdout_masks_before_one_second_outcomes": True,
            "search_masks_before_one_second_outcomes": True,
            "stage_artifacts": "ATOMIC_CANONICAL_JSON_MODE_0444",
            "walk_forward_all_folds_before_results": True,
            "walk_forward_masks_before_one_second_outcomes": True,
            "walk_forward_one_second_memory_bound": "ONE_OUTCOME_SPAN_OR_ONE_FOLD",
        },
        "multiplicity": {
            "discovery_family": "ALL_12_BATCH3_PROPOSALS",
            "discovery_method": "BENJAMINI_HOCHBERG",
            "holdout_method": "HOLM_STEP_DOWN",
            "one_sided_alpha_denominator": 20,
            "one_sided_alpha_numerator": 1,
            "p_star": "MAX_OF_REAL_GT_ZERO_REAL_GT_SHIFT_REAL_GT_MATCHED_DAILY_SIGN_TESTS",
            "sign_test_daily_vector": (
                "EXIT_ACTIVE_DATE_ALL_STAGE_DATA_DATES_INCLUDING_OUTCOME_TAIL_EXPLICIT_ZERO"
            ),
            "stage_group_assignment": "SIGNAL_DECISION_DATE",
            "walk_forward_family": "ALL_DISCOVERY_FINALISTS",
            "walk_forward_method": "BENJAMINI_HOCHBERG",
            "zero_daily_differences": "OMITTED_FROM_EXACT_SIGN_TEST",
        },
        "nulls": {
            "hash_algorithm": "SHA256_CANONICAL_JSON_V1",
            "hash_fields": [
                "master_seed",
                "proposal_sha256",
                "stage",
                "fold",
                "source_date",
                "null_kind",
            ],
            "master_seed": 20_260_813,
            "matched_fallback_order": [
                ("SAME_DATE_CONTRACT_OUTCOME_SPAN_SIGNAL_SEGMENT_30M_UTC_BUCKET_CAUSAL_STRATUM"),
                (
                    "SAME_DATE_CONTRACT_OUTCOME_SPAN_SIGNAL_SEGMENT_"
                    "ADJACENT_PLUS_MINUS_1_30M_UTC_BUCKET_RETAIN_CAUSAL_STRATUM"
                ),
                ("SAME_DATE_CONTRACT_OUTCOME_SPAN_SIGNAL_SEGMENT_DROP_BUCKET_AND_CAUSAL_STRATUM"),
                "SAMPLE_INELIGIBLE",
            ],
            "matched_missing_history_policy": ("SEPARATE_CAUSAL_MISSING_PRIOR_20_HISTORY_STRATUM"),
            "matched_primary_strata": [
                "SOURCE_DATE",
                "CONTRACT",
                "OUTCOME_SPAN",
                "SIGNAL_SEGMENT_ID",
                "30M_UTC_BUCKET",
                "CAUSAL_PRIOR_20_BAR_RANGE_QUARTILE_OR_MISSING_PRIOR_20_HISTORY",
            ],
            "matched_sampling": "WITHOUT_REPLACEMENT_LOWEST_SHA256_SCORE",
            "null_kinds": ["DATE_SPAN_CIRCULAR_SHIFT", "CAUSAL_MATCHED_ENTRY"],
            "shift_offset": "1_PLUS_SHA256_MODULO_GROUP_SIZE_MINUS_ONE",
            "shift_scope": [
                "SOURCE_DATE",
                "CONTRACT",
                "OUTCOME_SPAN",
                "SIGNAL_SEGMENT_ID",
            ],
        },
        "schema_version": AI_PATTERN_HOLDOUT_CONFIG_SCHEMA,
        "search_gates": {
            "active_exit_days_minimum": 40,
            "bh_q_denominator": 20,
            "bh_q_numerator": 1,
            "economic_net_ticks_strictly_positive": True,
            "fills_minimum": 80,
            "maximum_finalists": 4,
            "median_signals_per_day_maximum": 10,
            "minimum_positive_reporting_blocks": 3,
            "null_deltas_strictly_positive": True,
            "profit_factor_minimum_denominator": 20,
            "profit_factor_minimum_numerator": 21,
            "ranking": [
                "P_STAR_ASC",
                "WORST_BLOCK_EV_DESC",
                "TOTAL_EV_DESC",
                "PROFIT_FACTOR_DESC",
                "PROPOSAL_SHA256_ASC",
            ],
            "raw_signals_each_reporting_block_minimum": 25,
            "raw_signal_support_basis": "ALL_COMPLETED_5M_RULE_MATCHES_BEFORE_ENTRY_ELIGIBILITY",
            "raw_signals_minimum": 160,
            "reporting_block_fills_minimum": 15,
            "signal_days_minimum": 40,
            "worst_reporting_block_ev_ticks_minimum": -2,
        },
        "status": {
            "database_mutation": False,
            "final_authority": AI_PATTERN_HOLDOUT_AUTHORITY,
            "network_access": False,
            "paper_live_or_promotion_authority": False,
            "physical_holdout_isolation": False,
            "strict_backtest_claim": False,
            "strict_sealed_holdout_claim": False,
        },
        "walk_forward_gates": {
            "active_entry_days_each_fold_minimum": 20,
            "active_entry_days_minimum": 150,
            "aggregate_economic_net_ticks_strictly_positive": True,
            "aggregate_profit_factor_minimum_denominator": 5,
            "aggregate_profit_factor_minimum_numerator": 6,
            "bh_q_denominator": 20,
            "bh_q_numerator": 1,
            "contract_count_minimum": 5,
            "fills_each_fold_minimum": 40,
            "fills_minimum": 300,
            "maximum_finalists": 3,
            "minimum_positive_folds": 4,
            "net_over_maximum_drawdown_minimum_denominator": 2,
            "net_over_maximum_drawdown_minimum_numerator": 3,
            "null_deltas_strictly_positive": True,
            "ranking": [
                "P_STAR_ASC",
                "WORST_FOLD_EV_DESC",
                "TOTAL_EV_DESC",
                "PROFIT_FACTOR_DESC",
                "PROPOSAL_SHA256_ASC",
            ],
            "worst_fold_profit_factor_floor_denominator": 4,
            "worst_fold_profit_factor_floor_numerator": 3,
            "worst_losing_fold_over_median_positive_profit_maximum_denominator": 2,
            "worst_losing_fold_over_median_positive_profit_maximum_numerator": 3,
        },
    }


def expected_ai_pattern_holdout_contract() -> dict[str, object]:
    """Return an isolated copy of the non-provenance precommit contract."""

    return json.loads(canonical_json_bytes(_static_contract()))


def render_ai_pattern_holdout_toml_template() -> str:
    """Render the exact data-only layout with deliberately invalid provenance markers."""

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
        if not isinstance(table, dict):  # pragma: no cover - selected above
            raise AIPatternHoldoutConfigError("template section lost its table type")
        lines.extend(_toml_assignment(key, table[key]) for key in sorted(table))
    return "\n".join(lines) + "\n"


def _toml_assignment(key: str, value: object) -> str:
    if isinstance(value, bool):
        encoded = "true" if value else "false"
    elif isinstance(value, int):
        encoded = str(value)
    elif isinstance(value, str):
        encoded = json.dumps(value, ensure_ascii=True)
    elif isinstance(value, list) and all(isinstance(item, str) for item in value):
        encoded = "[" + ", ".join(json.dumps(item, ensure_ascii=True) for item in value) + "]"
    else:  # pragma: no cover - contract values are intentionally TOML-flat
        raise AIPatternHoldoutConfigError(f"cannot render TOML contract field {key}")
    return f"{key} = {encoded}"


def _implementation_paths(project_root: Path) -> tuple[str, ...]:
    package_root = project_root / "src/systematic_fx"
    if package_root.is_symlink() or not package_root.is_dir():
        raise AIPatternHoldoutConfigError("systematic_fx source tree is missing or symbolic")
    paths = [*_IMPLEMENTATION_SINGLE_FILES, *HOLDOUT_IMPLEMENTATION_SCRIPTS]
    for relative in (*_IMPLEMENTATION_SINGLE_FILES, *HOLDOUT_IMPLEMENTATION_SCRIPTS):
        path = project_root / relative
        if path.is_symlink() or not path.is_file():
            raise AIPatternHoldoutConfigError("holdout implementation file is missing or symbolic")
    for path in package_root.rglob("*.py"):
        if path.is_symlink() or not path.is_file() or "__pycache__" in path.parts:
            raise AIPatternHoldoutConfigError("systematic_fx source tree contains unsafe source")
        paths.append(path.relative_to(project_root).as_posix())
    ordered = tuple(sorted(paths))
    if len(ordered) != len(set(ordered)):
        raise AIPatternHoldoutConfigError("holdout implementation contains duplicate paths")
    return ordered


def holdout_implementation_sha256(project_root: Path | str) -> str:
    """Hash the exact source closure authorized to evaluate Batch 3."""

    root = Path(project_root).expanduser().resolve(strict=True)
    modules: list[dict[str, object]] = []
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
            "implementation_schema": "systematic_fx.ai_pattern_holdout_implementation.v1",
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
        raise AIPatternHoldoutConfigError("Git provenance cannot be verified") from error
    if process.returncode != 0:
        raise AIPatternHoldoutConfigError("Git provenance cannot be verified")
    return process.stdout


def verify_committed_holdout_implementation(project_root: Path, code_commit: str) -> None:
    """Require the evaluator closure to equal regular blobs in ``code_commit``."""

    if (
        _COMMIT.fullmatch(code_commit) is None
        or not set(code_commit) - {"0"}
        or _git(project_root, "cat-file", "-t", code_commit).strip() != b"commit"
    ):
        raise AIPatternHoldoutConfigError("holdout code commit is not a committed object")
    current_paths = _implementation_paths(project_root)
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
    expected_paths = tuple(
        sorted(
            (
                *_IMPLEMENTATION_SINGLE_FILES,
                *HOLDOUT_IMPLEMENTATION_SCRIPTS,
                *committed_package_paths,
            )
        )
    )
    if current_paths != expected_paths:
        raise AIPatternHoldoutConfigError("holdout runtime file set differs from its commit")
    for relative in current_paths:
        current = (project_root / relative).read_bytes()
        if _git(project_root, "show", f"{code_commit}:{relative}") != current:
            raise AIPatternHoldoutConfigError("holdout runtime differs from its committed source")


@dataclass(frozen=True, slots=True)
class AIPatternHoldoutConfig:
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
        if not isinstance(value, dict):  # pragma: no cover - canonical root is fixed
            raise AIPatternHoldoutConfigError("holdout config root is not an object")
        return value

    @property
    def request_sha256(self) -> str:
        return self.semantic_sha256

    @property
    def authority(self) -> str:
        return AI_PATTERN_HOLDOUT_AUTHORITY


def _object(value: object, *, label: str, keys: set[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise AIPatternHoldoutConfigError(f"{label} differs from the exact frozen schema")
    return value


def _validated_document(document: object) -> dict[str, object]:
    expected = _static_contract()
    dynamic = {
        "code_commit",
        "dependency_lock_sha256",
        "evaluator_implementation_sha256",
        "precommitted_at_utc",
    }
    parsed = _object(
        document,
        label="AI pattern holdout config",
        keys=set(expected) | dynamic,
    )
    for key, value in expected.items():
        if parsed[key] != value:
            raise AIPatternHoldoutConfigError(f"AI pattern holdout {key} drifted")
    commit = parsed["code_commit"]
    implementation = parsed["evaluator_implementation_sha256"]
    dependency = parsed["dependency_lock_sha256"]
    timestamp = parsed["precommitted_at_utc"]
    if not isinstance(commit, str) or _COMMIT.fullmatch(commit) is None or not set(commit) - {"0"}:
        raise AIPatternHoldoutConfigError("holdout code_commit is not a full Git commit ID")
    for label, value in (
        ("evaluator_implementation_sha256", implementation),
        ("dependency_lock_sha256", dependency),
    ):
        if not isinstance(value, str) or _SHA256.fullmatch(value) is None or not set(value) - {"0"}:
            raise AIPatternHoldoutConfigError(f"holdout {label} is not a SHA-256")
    if not isinstance(timestamp, str) or _UTC_TIMESTAMP.fullmatch(timestamp) is None:
        raise AIPatternHoldoutConfigError("holdout precommit timestamp is not canonical UTC")
    return parsed


def load_ai_pattern_holdout_config(
    project_root: Path | str,
    *,
    path: Path | str | None = None,
) -> AIPatternHoldoutConfig:
    """Load the exact data-only precommit and prove its committed evaluator bytes."""

    requested_root = Path(project_root).expanduser()
    if requested_root.is_symlink():
        raise AIPatternHoldoutConfigError("project root cannot be symbolic")
    root = requested_root.resolve(strict=True)
    selected = root / AI_PATTERN_HOLDOUT_CONFIG_RELATIVE_PATH if path is None else Path(path)
    if not selected.is_absolute():
        selected = root / selected
    if selected.is_symlink():
        raise AIPatternHoldoutConfigError("holdout config cannot be symbolic")
    resolved = selected.resolve(strict=True)
    expected_path = (root / AI_PATTERN_HOLDOUT_CONFIG_RELATIVE_PATH).absolute()
    if resolved != expected_path or not resolved.is_relative_to(root):
        raise AIPatternHoldoutConfigError("holdout config must be the checked-in v1 precommit")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(resolved, flags)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise AIPatternHoldoutConfigError("holdout config is not a regular file")
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 1024 * 1024):
            chunks.append(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    lexical = resolved.stat(follow_symlinks=False)
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ) or (after.st_dev, after.st_ino, after.st_size) != (
        lexical.st_dev,
        lexical.st_ino,
        lexical.st_size,
    ):
        raise AIPatternHoldoutConfigError("holdout config changed while it was opened")
    raw = b"".join(chunks)
    try:
        parsed = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise AIPatternHoldoutConfigError("holdout config is invalid TOML") from error
    document = _validated_document(parsed)
    code_commit = str(document["code_commit"])
    implementation = str(document["evaluator_implementation_sha256"])
    dependency = str(document["dependency_lock_sha256"])
    if holdout_implementation_sha256(root) != implementation:
        raise AIPatternHoldoutConfigError("holdout evaluator implementation identity drifted")
    if dependency_lock_sha256(root) != dependency:
        raise AIPatternHoldoutConfigError("holdout dependency identity drifted")
    try:
        verify_committed_holdout_implementation(root, code_commit)
    except AIPatternHoldoutConfigError as error:
        raise AIPatternHoldoutConfigError(
            "holdout evaluator is not its committed source"
        ) from error
    return AIPatternHoldoutConfig(
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        semantic_sha256=canonical_sha256(document),
        code_commit=code_commit,
        evaluator_implementation_sha256=implementation,
        dependency_lock_sha256=dependency,
        precommitted_at_utc=str(document["precommitted_at_utc"]),
        canonical_bytes=canonical_json_bytes(document),
    )


__all__ = [
    "AI_PATTERN_BATCH3_BATCH_RELATIVE_PATH",
    "AI_PATTERN_BATCH3_REPORT_RELATIVE_PATH",
    "AI_PATTERN_BATCH3_REQUEST_RELATIVE_PATH",
    "AI_PATTERN_HOLDOUT_AUTHORITY",
    "AI_PATTERN_HOLDOUT_CONFIG_RELATIVE_PATH",
    "AI_PATTERN_HOLDOUT_CONFIG_SCHEMA",
    "DATASET_MANIFEST_RELATIVE_PATH",
    "EXPECTED_BATCH3_GOVERNED_REQUEST_SHA256",
    "EXPECTED_BATCH3_PROPOSAL_BATCH_SHA256",
    "EXPECTED_BATCH3_PROPOSAL_REPORT_SHA256",
    "EXPECTED_DATASET_HANDOFF_SHA256",
    "EXPECTED_DATASET_MANIFEST_SHA256",
    "EXPECTED_SOURCE_MANIFEST_SHA256",
    "EXPECTED_SPLIT_PLAN_SHA256",
    "FINAL_HOLDOUT_STATUSES",
    "HOLDOUT_IMPLEMENTATION_SCRIPTS",
    "AIPatternHoldoutConfig",
    "AIPatternHoldoutConfigError",
    "expected_ai_pattern_holdout_contract",
    "holdout_implementation_sha256",
    "load_ai_pattern_holdout_config",
    "render_ai_pattern_holdout_toml_template",
    "verify_committed_holdout_implementation",
]
