"""Data-only precommit and independent provenance for all-cases v1.

The source commit is made first.  The generated TOML is then filled with that
commit, the exact implementation/dependency identities, and a UTC timestamp in
a separate config-only commit. Runtime verification covers every Python blob in
this campaign package, the complete ``src/systematic_fx`` and ``scripts`` Python
trees, and both project dependency blobs.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import os
import platform
import re
import stat
import subprocess
import sys
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

AI_ALL_CASES_CAMPAIGN_DESIGN_ID: Final = "ai_all_cases_v1"
AI_ALL_CASES_CONFIG_ID: Final = "ai_all_cases_v1_attempt3"
AI_ALL_CASES_CONFIG_SCHEMA: Final = "systematic_fx.ai_all_cases_config.v3"
AI_ALL_CASES_CONFIG_RELATIVE_PATH: Final = Path("configs/research/ai_all_cases_v1_attempt3.toml")
AI_ALL_CASES_RUN_RELATIVE_ROOT: Final = Path("data/derived/bar_patterns/ai_all_cases_v1_attempt3")
AI_ALL_CASES_AUTHORITY: Final = "UNSEALED_LOCAL_AI_ALL_CASES_RESEARCH"
CAMPAIGN_PACKAGE_RELATIVE_PATH: Final = Path("campaigns/ai_all_cases_v1")
TRUSTED_BOOTSTRAP_RELATIVE_PATH: Final = CAMPAIGN_PACKAGE_RELATIVE_PATH / "bootstrap.py"
MAXIMUM_SEARCH_SELECTION: Final = 12
MAXIMUM_HOLDOUT_FINALISTS: Final = 3
WALK_FORWARD_FOLD_KEYS: Final = ("WF1", "WF2", "WF3", "WF4", "WF5")
EXPECTED_ACTIVE_CALENDAR_SHA256: Final = (
    "b414eae72afdb1c149977ff0ea5b672069380997d91e74adf0407e35836e8ac1"
)
EXPECTED_ACTIVE_DATE_COUNT: Final = 1_413
SEARCH_BLOCK_LENGTHS: Final = (59, 59, 59, 59, 59, 58, 58, 58)
_STAGE_RANGES: Final = (
    {
        "active_day_count": 489,
        "decision_day_count": 469,
        "decision_end_date": "2023-07-10",
        "end_date": "2023-08-02",
        "stage_key": "SEARCH",
        "start_date": "2022-01-03",
    },
    {
        "active_day_count": 153,
        "decision_day_count": 133,
        "decision_end_date": "2024-01-09",
        "end_date": "2024-02-01",
        "stage_key": "WF1",
        "start_date": "2023-08-03",
    },
    {
        "active_day_count": 153,
        "decision_day_count": 133,
        "decision_end_date": "2024-07-10",
        "end_date": "2024-08-04",
        "stage_key": "WF2",
        "start_date": "2024-02-02",
    },
    {
        "active_day_count": 153,
        "decision_day_count": 133,
        "decision_end_date": "2025-01-06",
        "end_date": "2025-01-29",
        "stage_key": "WF3",
        "start_date": "2024-08-05",
    },
    {
        "active_day_count": 153,
        "decision_day_count": 133,
        "decision_end_date": "2025-07-06",
        "end_date": "2025-07-29",
        "stage_key": "WF4",
        "start_date": "2025-01-30",
    },
    {
        "active_day_count": 152,
        "decision_day_count": 132,
        "decision_end_date": "2025-12-30",
        "end_date": "2026-01-22",
        "stage_key": "WF5",
        "start_date": "2025-07-30",
    },
    {
        "active_day_count": 20,
        "decision_day_count": 0,
        "decision_end_date": None,
        "end_date": "2026-02-15",
        "stage_key": "EMBARGO",
        "start_date": "2026-01-23",
    },
    {
        "active_day_count": 120,
        "decision_day_count": 120,
        "decision_end_date": "2026-07-08",
        "end_date": "2026-07-08",
        "stage_key": "HOLDOUT",
        "start_date": "2026-02-16",
    },
    {
        "active_day_count": 20,
        "decision_day_count": 0,
        "decision_end_date": None,
        "end_date": "2026-07-31",
        "stage_key": "HOLDOUT_OUTCOME_TAIL",
        "start_date": "2026-07-09",
    },
)
_SEARCH_BLOCKS: Final = (
    {"block_key": "B1", "day_count": 59, "end_date": "2022-03-11", "start_date": "2022-01-03"},
    {"block_key": "B2", "day_count": 59, "end_date": "2022-05-22", "start_date": "2022-03-13"},
    {"block_key": "B3", "day_count": 59, "end_date": "2022-07-29", "start_date": "2022-05-23"},
    {"block_key": "B4", "day_count": 59, "end_date": "2022-10-06", "start_date": "2022-07-31"},
    {"block_key": "B5", "day_count": 59, "end_date": "2022-12-14", "start_date": "2022-10-07"},
    {"block_key": "B6", "day_count": 58, "end_date": "2023-02-24", "start_date": "2022-12-15"},
    {"block_key": "B7", "day_count": 58, "end_date": "2023-05-03", "start_date": "2023-02-26"},
    {"block_key": "B8", "day_count": 58, "end_date": "2023-07-10", "start_date": "2023-05-04"},
)
_REPORTING_BLOCKS: Final = (
    {"block_key": "R1", "day_count": 118, "end_date": "2022-05-22", "start_date": "2022-01-03"},
    {"block_key": "R2", "day_count": 117, "end_date": "2022-10-05", "start_date": "2022-05-23"},
    {"block_key": "R3", "day_count": 117, "end_date": "2023-02-23", "start_date": "2022-10-06"},
    {"block_key": "R4", "day_count": 117, "end_date": "2023-07-10", "start_date": "2023-02-24"},
)
_PROJECT_BLOBS: Final = ("pyproject.toml", "uv.lock")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_COMMIT = re.compile(r"[0-9a-f]{40}")
_UTC_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")
_SCIENTIFIC_SECTION_KEYS: Final = (
    "authority",
    "bindings",
    "compute_caps",
    "dataset",
    "execution",
    "holdout_gates",
    "lifecycle",
    "ml",
    "multiplicity",
    "nulls",
    "scope",
    "search_design",
    "selection",
    "stage_a_gates",
    "status",
    "universe_counts",
    "walk_forward_gates",
)
_CATALOG_IDENTITY_BINDING_KEYS: Final = (
    "catalog_summaries_sha256",
    "complete_strategy_recipe_sha256",
    "direct_catalog_sha256",
    "entry_catalog_sha256",
    "exit_catalog_sha256",
    "meta_catalog_sha256",
    "stage_a_chunk_plan_sha256",
    "symbolic_contract_sha256",
)
_GATE_SECTION_KEYS: Final = (
    "holdout_gates",
    "search_gates",
    "stage_a_gates",
    "walk_forward_gates",
)
_COST_FIELD_KEYS: Final = (
    "allocated_fixed_cost_ticks",
    "entry_adverse_ticks",
    "standard_friction_ticks",
    "stress_friction_ticks",
    "terminal_adverse_ticks",
    "variable_cost_ticks",
)
_PREDECESSOR_SCIENTIFIC_SECTION_SHA256: Final = (
    "677248a6e59973445a08888ee2334e7a07095ee683803233542f349a9615bc04"
)
_ATTEMPT1_SCIENTIFIC_SECTION_SHA256: Final = (
    "11ed94cf78e796a9faec78142c9cfc1d797c50de97716e234531d44d124b5444"
)
_PREDECESSOR_CATALOG_IDENTITY_SHA256: Final = (
    "fcfd0dfcaacbcd630d340664a39e698f6528ef4f757f260bbbf77b5aea9dd155"
)
_PREDECESSOR_GATE_IDENTITY_SHA256: Final = (
    "17230ab79eb75154084ed79bbbc2b95e917b44d07b6da4cccccbcea02b405e89"
)
_PREDECESSOR_COST_IDENTITY_SHA256: Final = (
    "88849d988724ebc452d4ee9cc23fa85a917e64473f45c2f43fd6f0eda8f81070"
)
DETERMINISTIC_RUNTIME_ENV: Final = {
    "LC_ALL": "C",
    "MKL_NUM_THREADS": "1",
    "NUMEXPR_NUM_THREADS": "1",
    "OMP_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "PYTHONHASHSEED": "0",
    "TZ": "UTC",
    "VECLIB_MAXIMUM_THREADS": "1",
}
TRUSTED_BOOTSTRAP_POLICY: Final = {
    "bytecode_cache": ("UNIQUE_EMPTY_EXTERNAL_DIRECTORY_OWNER_MODE_0700_REMOVED_AFTER_PROCESS"),
    "dependency_provisioning": (
        "SEPARATE_UV_SYNC_LOCKED_OFFLINE_BEFORE_LAUNCH_UV_ABSENT_FROM_GOVERNED_EXEC_CHAIN"
    ),
    "entry_environment": (
        "ABSOLUTE_PINNED_USR_BIN_ENV_MINUS_SMALL_I_EXACT_ALLOWLIST_BEFORE_ANY_MUTATION"
    ),
    "invocation": (
        "PINNED_USR_BIN_ENV_MINUS_SMALL_I_TO_PINNED_ABSOLUTE_CPYTHON_"
        "MINUS_SMALL_S_MINUS_CAPITAL_P_MINUS_B_MINUS_CAPITAL_S"
    ),
    "local_bytecode_policy": ("REJECT_ANY_PYC_PYO_OR_PYCACHE_BEFORE_FIRST_WORKSPACE_IMPORT"),
    "python_distribution_trust": (
        "PINNED_USER_OWNED_CPYTHON_BINARY_BASE_PREFIX_AND_LOCKED_SITE_PACKAGES_OPERATOR_TRUST"
    ),
    "sys_path": "EXACT_CAPTURED_STDLIB_PATHS_THEN_LOCKED_SITE_PACKAGES",
    "trusted_git": (
        "ROOT_OWNED_ABSOLUTE_BINARY_EXACT_SHA_SIZE_VERSION_MINIMAL_ENV_EXPLICIT_GITDIR_"
        "NO_REPLACE_GRAFT_SHALLOW_ALTERNATE_LAZY_FETCH_OR_COMMIT_GRAPH"
    ),
}
_BOOTSTRAP_BASE_PATHS_ENV: Final = "AI_ALL_CASES_STDLIB_BASE_PATHS_JSON"
_BOOTSTRAP_BASE_PATHS_SHA_ENV: Final = "AI_ALL_CASES_STDLIB_BASE_PATHS_SHA256"
_CLEAN_ENTRY_ENV_SHA_ENV: Final = "AI_ALL_CASES_CLEAN_ENTRY_ENV_SHA256"
_TRUSTED_GIT_PATH_ENV: Final = "AI_ALL_CASES_TRUSTED_GIT_PATH"
_TRUSTED_GIT_SHA_ENV: Final = "AI_ALL_CASES_TRUSTED_GIT_SHA256"
_TRUSTED_GIT_VERSION_ENV: Final = "AI_ALL_CASES_TRUSTED_GIT_VERSION"
_TRUSTED_ENV_PATH: Final = Path("/usr/bin/env")
_TRUSTED_ENV_SHA256: Final = "6e506aec3c0cff703ac1e66cedc6f1945354ad41339a38db4425c7c88227128f"
_TRUSTED_ENV_SIZE: Final = 102_368
_TRUSTED_OPERATOR_UID: Final = 501
_TRUSTED_PYTHON_BASE_PREFIX: Final = Path(
    "/Users/seokminsong/.local/share/uv/python/cpython-3.12.13-macos-aarch64-none"
)
_TRUSTED_PYTHON_PATH: Final = _TRUSTED_PYTHON_BASE_PREFIX / "bin/python3.12"
_TRUSTED_PYTHON_SHA256: Final = "f64cf6322e4f20cd0458aab89c0d332895817bb8f243b943109b6a957582fd5d"
_TRUSTED_PYTHON_SIZE: Final = 18_041_104
_TRUSTED_PYTHON_VERSION: Final = "3.12.13"
_TRUSTED_CF_USER_TEXT_ENCODING: Final = "0x1F5:0x0:0x0"
_TRUSTED_GIT_PATH: Final = Path("/usr/bin/git")
_TRUSTED_GIT_SHA256: Final = "179301dcb41ea78accc3fa0048a7e6f6710d891945a751a34addd622020c1818"
_TRUSTED_GIT_SIZE: Final = 118_928
_TRUSTED_GIT_VERSION: Final = "git version 2.50.1 (Apple Git-155)"
_TRUSTED_PATH: Final = "/usr/bin:/bin"
_PRODUCTION_LAUNCHER_TEMPLATE: Final = (
    "/usr/bin/env",
    "-i",
    "VIRTUAL_ENV=<PROJECT_ROOT>/.venv",
    "LC_ALL=C",
    "MKL_NUM_THREADS=1",
    "NUMEXPR_NUM_THREADS=1",
    "OMP_NUM_THREADS=1",
    "OPENBLAS_NUM_THREADS=1",
    "PYTHONHASHSEED=0",
    "PYTHONDONTWRITEBYTECODE=1",
    "TZ=UTC",
    "VECLIB_MAXIMUM_THREADS=1",
    "__CF_USER_TEXT_ENCODING=0x1F5:0x0:0x0",
    str(_TRUSTED_PYTHON_PATH),
    "-s",
    "-P",
    "-B",
    "-S",
    "<PROJECT_ROOT>/campaigns/ai_all_cases_v1/bootstrap.py",
    "<ACTION>",
    "--project-root",
    "<PROJECT_ROOT>",
)


class AllCasesConfigError(ValueError):
    """The all-cases precommit or exact source closure differs."""


def _safe_project_descendant(
    project_root: Path,
    path: Path,
    *,
    directory: bool,
) -> Path:
    root = project_root.resolve(strict=True)
    try:
        relative = path.relative_to(project_root)
    except ValueError as error:
        raise AllCasesConfigError("runtime path leaves the project root") from error
    current = project_root
    for index, part in enumerate(relative.parts):
        current /= part
        try:
            metadata = current.lstat()
        except OSError as error:
            raise AllCasesConfigError("runtime path is missing") from error
        final = index == len(relative.parts) - 1
        if (
            current.is_symlink()
            or ((not final or directory) and not stat.S_ISDIR(metadata.st_mode))
            or (final and not directory and not stat.S_ISREG(metadata.st_mode))
        ):
            raise AllCasesConfigError("runtime path contains an unsafe component")
    if not path.resolve(strict=True).is_relative_to(root):
        raise AllCasesConfigError("runtime path resolves outside the project root")
    return path


def _require_deterministic_runtime_environment() -> None:
    if any(os.environ.get(key) != value for key, value in DETERMINISTIC_RUNTIME_ENV.items()):
        raise AllCasesConfigError(
            "deterministic runtime environment differs; launch with every frozen env value"
        )


def _workspace_bytecode_cache_paths(project_root: Path) -> tuple[Path, ...]:
    """Return non-source entries from every workspace import package root."""

    root = project_root.resolve(strict=True)
    unsafe: list[Path] = []
    for relative in (Path("campaigns"), Path("src/systematic_fx"), Path("scripts")):
        tree = _safe_project_descendant(root, root / relative, directory=True)
        if relative == Path("campaigns"):
            allowed = {tree / "__init__.py", tree / "ai_all_cases_v1"}
            unsafe.extend(path for path in tree.iterdir() if path not in allowed)
        for path in tree.rglob("*"):
            metadata = path.lstat()
            if (
                path.is_symlink()
                or (path.is_dir() and path.name == "__pycache__")
                or (
                    not path.is_dir()
                    and (not stat.S_ISREG(metadata.st_mode) or path.suffix != ".py")
                )
            ):
                unsafe.append(path)
    return tuple(sorted(set(unsafe)))


def _trusted_runtime_flag_document() -> dict[str, object]:
    """Expose immutable interpreter flags through a small testable boundary."""

    return {
        "dont_write_bytecode": sys.dont_write_bytecode,
        "hash_probe": hash("all-cases"),
        "hash_randomization": sys.flags.hash_randomization,
        "ignore_environment": sys.flags.ignore_environment,
        "isolated": sys.flags.isolated,
        "no_site": sys.flags.no_site,
        "no_user_site": sys.flags.no_user_site,
        "safe_path": sys.flags.safe_path,
        "utf8_mode": sys.flags.utf8_mode,
    }


def _clean_entry_environment(project_root: Path) -> dict[str, str]:
    if os.geteuid() != _TRUSTED_OPERATOR_UID:
        raise AllCasesConfigError("trusted operator identity differs")
    expected = {
        **DETERMINISTIC_RUNTIME_ENV,
        "PYTHONDONTWRITEBYTECODE": "1",
        "VIRTUAL_ENV": str(project_root / ".venv"),
        "__CF_USER_TEXT_ENCODING": _TRUSTED_CF_USER_TEXT_ENCODING,
    }
    if f"0x{os.geteuid():X}:0x0:0x0" != _TRUSTED_CF_USER_TEXT_ENCODING:
        raise AllCasesConfigError("trusted macOS text identity differs")
    return expected


def _clean_entry_environment_sha256(project_root: Path) -> str:
    return hashlib.sha256(_canonical_json_bytes(_clean_entry_environment(project_root))).hexdigest()


def _production_launcher_prefix(project_root: Path) -> tuple[str, ...]:
    environment = _clean_entry_environment(project_root)
    ordered_environment_keys = (
        "VIRTUAL_ENV",
        "LC_ALL",
        "MKL_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "PYTHONHASHSEED",
        "PYTHONDONTWRITEBYTECODE",
        "TZ",
        "VECLIB_MAXIMUM_THREADS",
        "__CF_USER_TEXT_ENCODING",
    )
    return (
        str(_TRUSTED_ENV_PATH),
        "-i",
        *(f"{key}={environment[key]}" for key in ordered_environment_keys),
        str(_TRUSTED_PYTHON_PATH),
        "-s",
        "-P",
        "-B",
        "-S",
        str(project_root / TRUSTED_BOOTSTRAP_RELATIVE_PATH),
    )


def _require_pinned_binary(
    path: Path,
    *,
    expected_uid: int,
    expected_size: int,
    expected_sha256: str,
) -> None:
    if not path.is_absolute():
        raise AllCasesConfigError("trusted executable path is not absolute")
    current = Path("/")
    components = path.parts[1:]
    for index, part in enumerate(components):
        current /= part
        try:
            metadata = current.lstat()
        except OSError as error:
            raise AllCasesConfigError("trusted executable path is missing") from error
        final = index == len(components) - 1
        if (
            current.is_symlink()
            or metadata.st_uid not in {0, expected_uid}
            or metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            or (not final and not stat.S_ISDIR(metadata.st_mode))
            or (final and not stat.S_ISREG(metadata.st_mode))
        ):
            raise AllCasesConfigError("trusted executable path contains an unsafe component")
    metadata = path.lstat()
    if (
        metadata.st_uid != expected_uid
        or metadata.st_size != expected_size
        or metadata.st_mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH) == 0
        or hashlib.sha256(path.read_bytes()).hexdigest() != expected_sha256
    ):
        raise AllCasesConfigError("trusted executable binary identity differs")


def _require_trusted_preexec_runtime(project_root: Path) -> None:
    """Revalidate the pinned env wrapper, CPython, and clean-entry attestation."""

    _require_pinned_binary(
        _TRUSTED_ENV_PATH,
        expected_uid=0,
        expected_size=_TRUSTED_ENV_SIZE,
        expected_sha256=_TRUSTED_ENV_SHA256,
    )
    _require_pinned_binary(
        _TRUSTED_PYTHON_PATH,
        expected_uid=_TRUSTED_OPERATOR_UID,
        expected_size=_TRUSTED_PYTHON_SIZE,
        expected_sha256=_TRUSTED_PYTHON_SHA256,
    )
    if (
        sys.executable != str(_TRUSTED_PYTHON_PATH)
        or Path(sys.base_prefix) != _TRUSTED_PYTHON_BASE_PREFIX
        or Path(sys.base_exec_prefix) != _TRUSTED_PYTHON_BASE_PREFIX
        or Path(sys.prefix) != _TRUSTED_PYTHON_BASE_PREFIX
        or Path(sys.exec_prefix) != _TRUSTED_PYTHON_BASE_PREFIX
        or platform.python_version() != _TRUSTED_PYTHON_VERSION
        or os.environ.get(_CLEAN_ENTRY_ENV_SHA_ENV) != _clean_entry_environment_sha256(project_root)
    ):
        raise AllCasesConfigError("trusted pre-exec launcher identity differs")


def _trusted_git_environment() -> dict[str, str]:
    return {
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_SYSTEM": os.devnull,
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "LANG": "C",
        "LC_ALL": "C",
        "PATH": _TRUSTED_PATH,
        "TZ": "UTC",
    }


def _require_trusted_git_runtime() -> None:
    """Revalidate the bootstrap-pinned provenance executable and clean process env."""

    if (
        os.environ.get("PATH") != _TRUSTED_PATH
        or any(key.startswith(("GIT_", "DYLD_", "LD_")) for key in os.environ)
        or os.environ.get(_TRUSTED_GIT_PATH_ENV) != str(_TRUSTED_GIT_PATH)
        or os.environ.get(_TRUSTED_GIT_SHA_ENV) != _TRUSTED_GIT_SHA256
        or os.environ.get(_TRUSTED_GIT_VERSION_ENV) != _TRUSTED_GIT_VERSION
    ):
        raise AllCasesConfigError("trusted Git bootstrap identity or environment differs")
    for path, directory in (
        (Path("/"), True),
        (Path("/usr"), True),
        (Path("/usr/bin"), True),
        (_TRUSTED_GIT_PATH, False),
    ):
        try:
            metadata = path.lstat()
        except OSError as error:
            raise AllCasesConfigError("trusted Git path is missing") from error
        if (
            path.is_symlink()
            or metadata.st_uid != 0
            or metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            or (directory and not stat.S_ISDIR(metadata.st_mode))
            or (not directory and not stat.S_ISREG(metadata.st_mode))
        ):
            raise AllCasesConfigError("trusted Git path contains an unsafe component")
    metadata = _TRUSTED_GIT_PATH.lstat()
    if (
        metadata.st_size != _TRUSTED_GIT_SIZE
        or metadata.st_mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH) == 0
        or hashlib.sha256(_TRUSTED_GIT_PATH.read_bytes()).hexdigest() != _TRUSTED_GIT_SHA256
    ):
        raise AllCasesConfigError("trusted Git binary identity differs")
    try:
        process = subprocess.run(
            [str(_TRUSTED_GIT_PATH), "--version"],
            check=False,
            capture_output=True,
            env=_trusted_git_environment(),
            stdin=subprocess.DEVNULL,
        )
    except OSError as error:
        raise AllCasesConfigError("trusted Git version cannot be verified") from error
    if (
        process.returncode != 0
        or process.stderr != b""
        or process.stdout.decode("ascii").strip() != _TRUSTED_GIT_VERSION
    ):
        raise AllCasesConfigError("trusted Git version differs")


def _require_trusted_bootstrap_runtime(project_root: Path) -> None:
    """Require the sole clean-environment bootstrap before any public action."""

    _require_deterministic_runtime_environment()
    root = project_root.resolve(strict=True)
    _require_trusted_preexec_runtime(root)
    flags = _trusted_runtime_flag_document()
    if (
        set(flags)
        != {
            "dont_write_bytecode",
            "hash_probe",
            "hash_randomization",
            "ignore_environment",
            "isolated",
            "no_site",
            "no_user_site",
            "safe_path",
            "utf8_mode",
        }
        or flags["dont_write_bytecode"] is not True
        or type(flags["isolated"]) is not int
        or flags["isolated"] != 0
        or type(flags["ignore_environment"]) is not int
        or flags["ignore_environment"] != 0
        or type(flags["no_site"]) is not int
        or flags["no_site"] != 1
        or type(flags["no_user_site"]) is not int
        or flags["no_user_site"] != 1
        or type(flags["hash_randomization"]) is not int
        or flags["hash_randomization"] != 0
        or type(flags["utf8_mode"]) is not int
        or flags["utf8_mode"] != 1
        or type(flags["hash_probe"]) is not int
        or flags["hash_probe"] != -4_299_525_529_514_689_000
        or flags["safe_path"] is not True
    ):
        raise AllCasesConfigError(
            "trusted bootstrap requires CPython deterministic hash, no-user-site, no-site, "
            "safe-path, and no-bytecode flags"
        )
    if _workspace_bytecode_cache_paths(root):
        raise AllCasesConfigError("workspace import surface contains a non-source entry")
    prefix_value = sys.pycache_prefix
    environment_prefix = os.environ.get("PYTHONPYCACHEPREFIX")
    if not isinstance(prefix_value, str) or environment_prefix != prefix_value:
        raise AllCasesConfigError("trusted external bytecode cache identity differs")
    prefix = Path(prefix_value)
    try:
        metadata = prefix.lstat()
        resolved_prefix = prefix.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise AllCasesConfigError("trusted external bytecode cache is missing") from error
    if (
        prefix.is_symlink()
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or resolved_prefix.is_relative_to(root)
        or any(prefix.iterdir())
    ):
        raise AllCasesConfigError("trusted external bytecode cache is unsafe or nonempty")
    expected_venv = root / ".venv"
    expected_site = expected_venv / (
        f"lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages"
    )
    if os.environ.get("VIRTUAL_ENV") != str(expected_venv):
        raise AllCasesConfigError("trusted virtual environment identity differs")
    try:
        _safe_project_descendant(root, expected_venv, directory=True)
        _safe_project_descendant(root, expected_site, directory=True)
        raw_base_paths = os.environ[_BOOTSTRAP_BASE_PATHS_ENV]
        base_paths_value = json.loads(raw_base_paths)
    except (KeyError, json.JSONDecodeError, OSError, TypeError) as error:
        raise AllCasesConfigError("trusted stdlib import path is incomplete") from error
    if (
        not isinstance(base_paths_value, list)
        or any(not isinstance(item, str) for item in base_paths_value)
        or len(set(base_paths_value)) != len(base_paths_value)
        or _canonical_json_bytes(base_paths_value).decode("ascii") != raw_base_paths
        or hashlib.sha256(raw_base_paths.encode("ascii")).hexdigest()
        != os.environ.get(_BOOTSTRAP_BASE_PATHS_SHA_ENV)
        or sys.path != [*base_paths_value, str(expected_site)]
        or len(set(sys.path)) != len(sys.path)
    ):
        raise AllCasesConfigError("trusted stdlib import path identity differs")
    base_prefix = Path(sys.base_prefix).resolve(strict=True)
    for item in base_paths_value:
        if not item or not Path(item).is_absolute():
            raise AllCasesConfigError("trusted stdlib base import path differs")
        if not Path(item).resolve(strict=False).is_relative_to(base_prefix):
            raise AllCasesConfigError("trusted stdlib base path leaves the interpreter")
    _require_trusted_git_runtime()
    expected_environment = {
        **_clean_entry_environment(root),
        _BOOTSTRAP_BASE_PATHS_ENV: raw_base_paths,
        _BOOTSTRAP_BASE_PATHS_SHA_ENV: hashlib.sha256(raw_base_paths.encode("ascii")).hexdigest(),
        _CLEAN_ENTRY_ENV_SHA_ENV: _clean_entry_environment_sha256(root),
        _TRUSTED_GIT_PATH_ENV: str(_TRUSTED_GIT_PATH),
        _TRUSTED_GIT_SHA_ENV: _TRUSTED_GIT_SHA256,
        _TRUSTED_GIT_VERSION_ENV: _TRUSTED_GIT_VERSION,
        "PATH": _TRUSTED_PATH,
        "PYTHONPYCACHEPREFIX": prefix_value,
    }
    if dict(os.environ) != expected_environment:
        raise AllCasesConfigError("trusted post-bootstrap environment contains an extra value")


def _runtime_module_origins(project_root: Path) -> dict[str, object]:
    """Bind import resolution to the caller's exact checkout, not another clone."""

    root = project_root.resolve(strict=True)
    campaign_relatives = {
        "campaigns": "campaigns/__init__.py",
        "campaigns.ai_all_cases_v1": "campaigns/ai_all_cases_v1/__init__.py",
        "campaigns.ai_all_cases_v1.__main__": "campaigns/ai_all_cases_v1/__main__.py",
        "campaigns.ai_all_cases_v1.bootstrap": "campaigns/ai_all_cases_v1/bootstrap.py",
        "campaigns.ai_all_cases_v1.config": "campaigns/ai_all_cases_v1/config.py",
        "campaigns.ai_all_cases_v1.ml": "campaigns/ai_all_cases_v1/ml.py",
        "campaigns.ai_all_cases_v1.pipeline": "campaigns/ai_all_cases_v1/pipeline.py",
        "campaigns.ai_all_cases_v1.run": "campaigns/ai_all_cases_v1/run.py",
        "campaigns.ai_all_cases_v1.symbolic": "campaigns/ai_all_cases_v1/symbolic.py",
    }
    legacy_relatives = {
        "scripts.ai_pattern_holdout_config": "scripts/ai_pattern_holdout_config.py",
        "scripts.ai_pattern_holdout_engine": "scripts/ai_pattern_holdout_engine.py",
        "systematic_fx": "src/systematic_fx/__init__.py",
        "systematic_fx.features.bars": "src/systematic_fx/features/bars.py",
        "systematic_fx.research.bar_pipeline": "src/systematic_fx/research/bar_pipeline.py",
        "systematic_fx.research.hypotheses": "src/systematic_fx/research/hypotheses.py",
        "systematic_fx.validation.bar_splits": "src/systematic_fx/validation/bar_splits.py",
    }

    def exact_origins(expected: dict[str, str], *, label: str) -> dict[str, str]:
        output: dict[str, str] = {}
        for name, relative in expected.items():
            specification = importlib.util.find_spec(name)
            origin = None if specification is None else specification.origin
            if not isinstance(origin, str):
                raise AllCasesConfigError(f"{label} module origin is incomplete")
            try:
                observed = Path(origin).resolve(strict=True)
                wanted = (root / relative).resolve(strict=True)
            except FileNotFoundError as error:
                raise AllCasesConfigError(f"{label} module origin is incomplete") from error
            if observed != wanted:
                raise AllCasesConfigError(f"{label} module resolves outside the project root")
            output[name] = str(observed)
        return output

    campaign = exact_origins(campaign_relatives, label="campaign")
    legacy = exact_origins(legacy_relatives, label="legacy runtime")
    scripts_namespace = importlib.util.find_spec("scripts")
    locations = () if scripts_namespace is None else scripts_namespace.submodule_search_locations
    try:
        script_locations = (
            ()
            if locations is None
            else tuple(Path(item).resolve(strict=True) for item in locations)
        )
        expected_scripts = (root / "scripts").resolve(strict=True)
    except FileNotFoundError as error:
        raise AllCasesConfigError("scripts namespace is incomplete") from error
    if script_locations != (expected_scripts,):
        raise AllCasesConfigError("scripts namespace resolves outside the project root")
    for name, module in tuple(sys.modules.items()):
        if not name.startswith(("systematic_fx.", "scripts.")):
            continue
        origin = getattr(getattr(module, "__spec__", None), "origin", None)
        if origin in {None, "built-in", "frozen"}:
            continue
        observed = Path(str(origin)).resolve(strict=True)
        if not (
            observed.is_relative_to((root / "src/systematic_fx").resolve(strict=True))
            or observed.is_relative_to((root / "scripts").resolve(strict=True))
        ):
            raise AllCasesConfigError("an imported legacy module belongs to another checkout")
    return {
        "campaign_module_origins": dict(sorted(campaign.items())),
        "legacy_module_origins": dict(sorted(legacy.items())),
    }


def _runtime_identity_document(project_root: Path | None = None) -> dict[str, object]:
    """Import numerical libraries only after the process-level environment guard."""

    _require_deterministic_runtime_environment()
    root = (
        Path(__file__).resolve(strict=True).parents[2]
        if project_root is None
        else project_root.resolve(strict=True)
    )
    origins = _runtime_module_origins(root)
    runtime_flags = _trusted_runtime_flag_document()
    libraries = {}
    for name in ("numpy", "scipy", "sklearn"):
        module = importlib.import_module(name)
        origin = getattr(getattr(module, "__spec__", None), "origin", None)
        version = getattr(module, "__version__", None)
        if not isinstance(origin, str) or not isinstance(version, str):
            raise AllCasesConfigError("numerical runtime identity is incomplete")
        libraries[name] = {"origin": origin, "version": version}
    return {
        **origins,
        "bootstrap_policy": dict(TRUSTED_BOOTSTRAP_POLICY),
        "bootstrap_runtime": {
            "dont_write_bytecode": runtime_flags["dont_write_bytecode"],
            "hash_probe": runtime_flags["hash_probe"],
            "hash_randomization": runtime_flags["hash_randomization"],
            "ignore_environment": runtime_flags["ignore_environment"],
            "isolated": runtime_flags["isolated"],
            "no_user_site": runtime_flags["no_user_site"],
            "stdlib_base_paths_sha256": os.environ.get(
                _BOOTSTRAP_BASE_PATHS_SHA_ENV,
                "NOT_RUNNING_UNDER_PUBLIC_BOOTSTRAP",
            ),
            "no_site": runtime_flags["no_site"],
            "pycache_prefix": "UNIQUE_EXTERNAL_OWNER_MODE_0700_EMPTY_DIRECTORY",
            "safe_path": runtime_flags["safe_path"],
            "utf8_mode": runtime_flags["utf8_mode"],
            "workspace_packages": [
                "EXPLICIT_SOURCE_LOADED:campaigns",
                "EXPLICIT_SOURCE_LOADED:scripts",
                "EXPLICIT_SOURCE_LOADED:systematic_fx",
            ],
        },
        "environment": dict(DETERMINISTIC_RUNTIME_ENV),
        "libraries": libraries,
        "python": {
            "base_prefix": str(_TRUSTED_PYTHON_BASE_PREFIX),
            "executable": str(_TRUSTED_PYTHON_PATH),
            "implementation": "CPython",
            "operator_trust_boundary": ("PINNED_USER_OWNED_DISTRIBUTION_AND_LOCKED_SITE_PACKAGES"),
            "sha256": _TRUSTED_PYTHON_SHA256,
            "size": _TRUSTED_PYTHON_SIZE,
            "version": _TRUSTED_PYTHON_VERSION,
        },
        "schema": "systematic_fx.ai_all_cases_runtime_identity.v1",
        "trusted_preexec": {
            "clean_entry_environment": _clean_entry_environment(root),
            "clean_entry_environment_sha256": _clean_entry_environment_sha256(root),
            "env_path": str(_TRUSTED_ENV_PATH),
            "env_sha256": _TRUSTED_ENV_SHA256,
            "env_size": _TRUSTED_ENV_SIZE,
            "invocation_prefix": list(_production_launcher_prefix(root)),
            "operator_uid": _TRUSTED_OPERATOR_UID,
        },
        "trusted_git": {
            "path": str(_TRUSTED_GIT_PATH),
            "sha256": _TRUSTED_GIT_SHA256,
            "size": _TRUSTED_GIT_SIZE,
            "subprocess_environment": _trusted_git_environment(),
            "version": _TRUSTED_GIT_VERSION,
        },
    }


def _canonical_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError) as error:
        raise AllCasesConfigError("contract is not canonical JSON") from error


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _as_document(value: object, *, label: str) -> dict[str, object]:
    candidate = value.as_dict() if hasattr(value, "as_dict") else value
    try:
        decoded = json.loads(_canonical_json_bytes(candidate))
    except (TypeError, ValueError) as error:  # pragma: no cover - normalized above
        raise AllCasesConfigError(f"{label} is not canonical") from error
    if not isinstance(decoded, dict):
        raise AllCasesConfigError(f"{label} is not an object")
    return decoded


def _catalog_summary(value: object, *, label: str) -> dict[str, object]:
    if hasattr(value, "as_dict"):
        document = _as_document(value, label=label)
        candidates = document.get("candidates")
        if not isinstance(candidates, list):
            raise AllCasesConfigError(f"{label} lacks candidates")
        candidate_count = document.get("candidate_count", len(candidates))
        catalog_sha256 = document.get("catalog_sha256", _canonical_sha256(candidates))
    elif isinstance(value, (list, tuple)):
        candidates = [_as_document(item, label=f"{label} candidate") for item in value]
        candidate_count = len(candidates)
        catalog_sha256 = _canonical_sha256(candidates)
    else:
        raise AllCasesConfigError(f"{label} has no supported catalog form")
    if (
        not isinstance(candidate_count, int)
        or isinstance(candidate_count, bool)
        or candidate_count != len(candidates)
        or not isinstance(catalog_sha256, str)
        or _SHA256.fullmatch(catalog_sha256) is None
        or _canonical_sha256(candidates) != catalog_sha256
    ):
        raise AllCasesConfigError(f"{label} identity differs")
    identifiers = []
    for item in candidates:
        identity_keys = tuple(
            key
            for key in item
            if key == "candidate_id" or (key.endswith("_id") and key != "schema_id")
        )
        preferred = "candidate_id" if "candidate_id" in identity_keys else None
        identity_key = preferred or (identity_keys[0] if len(identity_keys) == 1 else None)
        identifiers.append(item.get(identity_key) if identity_key is not None else None)
    if (
        any(not isinstance(item, str) or _SHA256.fullmatch(item) is None for item in identifiers)
        or len(set(identifiers)) != len(identifiers)
        or [item.get("selection_rank") for item in candidates]
        != list(range(1, len(candidates) + 1))
    ):
        raise AllCasesConfigError(f"{label} candidate identities differ")
    return {"candidate_count": candidate_count, "catalog_sha256": catalog_sha256}


def _research_bindings() -> dict[str, object]:
    """Rebuild every frozen symbolic and ML recipe identity from pure APIs."""

    _require_deterministic_runtime_environment()
    try:
        from .ml import (
            build_direct_candidate_catalog,
            build_meta_candidate_catalog,
            ml_engine_contract,
        )
        from .symbolic import (
            build_base_event_catalog,
            build_context_catalog,
            build_delay_catalog,
            build_entry_catalog,
            build_exit_catalog,
            build_time_filter_catalog,
            symbolic_engine_contract,
        )
    except (ImportError, AttributeError) as error:
        raise AllCasesConfigError("all-cases symbolic/ML bindings are incomplete") from error

    symbolic_contract = _as_document(symbolic_engine_contract(), label="symbolic contract")
    ml_contract = _as_document(ml_engine_contract(), label="ML contract")
    summaries = {
        "base_event": _catalog_summary(build_base_event_catalog(), label="base-event catalog"),
        "context": _catalog_summary(build_context_catalog(), label="context catalog"),
        "delay": _catalog_summary(build_delay_catalog(), label="delay catalog"),
        "entry": _catalog_summary(build_entry_catalog(), label="entry catalog"),
        "exit": _catalog_summary(build_exit_catalog(), label="exit catalog"),
        "direct_model": _catalog_summary(
            build_direct_candidate_catalog(), label="direct-model catalog"
        ),
        "meta_model": _catalog_summary(build_meta_candidate_catalog(), label="meta-model catalog"),
        "time_filter": _catalog_summary(build_time_filter_catalog(), label="time-filter catalog"),
    }
    axes = symbolic_contract.get("axes")
    if not isinstance(axes, dict):
        raise AllCasesConfigError("symbolic contract axes are missing")
    complete_recipe = axes.get("complete_strategy_recipe_sha256")
    if not isinstance(complete_recipe, str) or _SHA256.fullmatch(complete_recipe) is None:
        raise AllCasesConfigError("complete strategy recipe identity differs")
    anchor_recipe = axes.get("anchor_policy_recipe_sha256")
    stage_a_chunking = symbolic_contract.get("stage_a_chunking")
    stage_a_chunk_plan = (
        stage_a_chunking.get("chunk_plan_sha256") if isinstance(stage_a_chunking, dict) else None
    )
    if (
        not isinstance(anchor_recipe, str)
        or _SHA256.fullmatch(anchor_recipe) is None
        or not isinstance(stage_a_chunk_plan, str)
        or _SHA256.fullmatch(stage_a_chunk_plan) is None
    ):
        raise AllCasesConfigError("symbolic recipe/chunk-plan identity differs")
    return {
        "anchor_policy_recipe_sha256": anchor_recipe,
        "catalog_summaries_canonical_json": _canonical_json_bytes(summaries).decode("ascii"),
        "catalog_summaries_sha256": _canonical_sha256(summaries),
        "complete_strategy_recipe_sha256": complete_recipe,
        "direct_catalog_sha256": summaries["direct_model"]["catalog_sha256"],
        "entry_catalog_sha256": summaries["entry"]["catalog_sha256"],
        "exit_catalog_sha256": summaries["exit"]["catalog_sha256"],
        "ml_contract_canonical_json": _canonical_json_bytes(ml_contract).decode("ascii"),
        "ml_contract_sha256": _canonical_sha256(ml_contract),
        "meta_catalog_sha256": summaries["meta_model"]["catalog_sha256"],
        "stage_a_chunk_plan_sha256": stage_a_chunk_plan,
        "symbolic_contract_canonical_json": _canonical_json_bytes(symbolic_contract).decode(
            "ascii"
        ),
        "symbolic_contract_sha256": _canonical_sha256(symbolic_contract),
    }


def _selected_contract_sha256(document: dict[str, object], keys: tuple[str, ...]) -> str:
    if any(key not in document for key in keys):
        raise AllCasesConfigError("scientific contract section is incomplete")
    return _canonical_sha256({key: document[key] for key in keys})


def _scientific_section_sha256(document: dict[str, object]) -> str:
    """Hash the frozen attempt-1 scientific-section key projection exactly."""

    return _selected_contract_sha256(document, _SCIENTIFIC_SECTION_KEYS)


def _static_contract() -> dict[str, object]:
    bindings = _research_bindings()
    entry_lattice = {
        "limit": {
            "atr_retrace_fractions": ["1/4", "1/2"],
            "time_in_force_seconds": [1_800, 3_600],
        },
        "market": {"variant_count": 1},
        "stop": {
            "signal_extreme_buffer_ticks": [1, 4],
            "time_in_force_seconds": [1_800, 3_600],
        },
    }
    exit_lattice = {
        "bracket": {
            "cap_seconds": [3_600, 10_800, 21_600],
            "stop_loss_atr_fractions": ["1/2", "1", "3/2", "2"],
            "take_profit_atr_fractions": ["1/2", "1", "3/2", "2", "3"],
            "variant_count": 60,
        },
        "break_even": {
            "activation_atr_fractions": ["1/2", "1"],
            "cap_seconds": [10_800, 21_600],
            "initial_stop_atr_fractions": ["1/2", "1"],
            "variant_count": 8,
        },
        "rule": {
            "cap_seconds": [10_800, 21_600],
            "rules": ["OPPOSITE_TRIGGER", "CONTEXT_INVALID"],
            "variant_count": 4,
        },
        "terminal": {
            "horizons_seconds": [1_800, 3_600, 7_200, 10_800, 21_600],
            "variant_count": 5,
        },
        "trailing": {
            "activation_atr_fractions": ["1/2", "1"],
            "cap_seconds": [10_800, 21_600],
            "trail_atr_fractions": ["1/2", "1"],
            "variant_count": 8,
        },
    }
    contract: dict[str, object] = {
        "authority": AI_ALL_CASES_AUTHORITY,
        "bindings": bindings,
        "campaign_design_id": AI_ALL_CASES_CAMPAIGN_DESIGN_ID,
        "compute_caps": {
            "artifact_bytes_maximum": 20 * 1024**3,
            "artifact_cap_enforcement_scope": (
                "TOTAL_RUN_ROOT_UNIQUE_INODE_BYTES_PROJECTED_PREPUBLICATION_AND_"
                "POSTBOUNDARY_INCLUDING_PERMANENT_FAILED_EVENT_RESERVE"
            ),
            "actual_model_fit_concurrency": 1,
            "direct_candidate_chunk_count": 24,
            "direct_candidate_rows_per_chunk": 12,
            "feature_and_model_workers_maximum": 1,
            "mask_batch_default": 64,
            "mask_batch_maximum": 256,
            "meta_candidate_chunk_count": 24,
            "meta_candidate_rows_per_chunk": 8,
            "one_second_workers_maximum": 2,
            "resident_set_bytes_maximum": 32 * 1024**3,
            "resource_cap_enforcement_scope": (
                "PER_PROCESS_INVOCATION_CHECKED_AT_GOVERNED_CHUNK_SERVICE_BOUNDARIES_"
                "AND_PRE_RELEASE_NOT_ASYNC_INTERRUPT_NOT_CUMULATIVE_ACROSS_PRE_AUTH_RESUME"
            ),
            "search_fresh_campaign_projected_seconds": 162_962,
            "search_source_replay_and_resume_multiplier_maximum": 2,
            "search_source_replay_and_resume_projected_seconds_maximum": 325_924,
            "search_wall_seconds_maximum": 345_600,
            "stage_a_chunk_count": 64,
            "stage_a_policy_rows_per_chunk_maximum": 29_689,
            "stage_b_chunk_count": 64,
            "stage_b_evaluation_microbatch_size": 64,
            "stage_b_anchor_entry_world_pair_budget": 100_000,
            "stage_b_recipe_rows_per_chunk_maximum": 3_060,
            "verifier_wall_seconds_maximum": 172_800,
        },
        "config_id": AI_ALL_CASES_CONFIG_ID,
        "dataset": {
            "active_calendar_payload": "BARE_ORDERED_ISO_DATE_STRING_LIST",
            "active_calendar_sha256": EXPECTED_ACTIVE_CALENDAR_SHA256,
            "active_date_count": EXPECTED_ACTIVE_DATE_COUNT,
            "active_date_end": "2026-07-31",
            "active_date_start": "2022-01-03",
            "dataset_handoff_sha256": EXPECTED_DATASET_HANDOFF_SHA256,
            "dataset_manifest_relative_path": DATASET_MANIFEST_RELATIVE_PATH.as_posix(),
            "dataset_manifest_sha256": EXPECTED_DATASET_MANIFEST_SHA256,
            "embargo_access": "PROHIBITED",
            "available_timeframes_seconds": [1, 60, 300, 1_800, 3_600],
            "consumed_timeframes_seconds": [1, 300, 1_800, 3_600],
            "source_manifest_sha256": EXPECTED_SOURCE_MANIFEST_SHA256,
            "split_plan_sha256": EXPECTED_SPLIT_PLAN_SHA256,
            "timeframe_60_policy": "AVAILABLE_IN_MANIFEST_BUT_NOT_OPENED",
        },
        "execution": {
            "allocated_fixed_cost_ticks": 5,
            "entry_adverse_ticks": 2,
            "entry_lattice_canonical_json": _canonical_json_bytes(entry_lattice).decode("ascii"),
            "entry_exit_recipe_sha256": bindings["complete_strategy_recipe_sha256"],
            "entry_variant_count": 9,
            "direct_ml_decision_anchor": ("EXACT_FEATURE_CUTOFF_CLOCK_COMPLETED_NATIVE_BAR_END"),
            "direct_ml_entry_rule": (
                "FLOOR_NEXT_EXACT_SAME_LINEAGE_5M_FIRST_TRADE_NS_TO_1S;"
                "ENTRY_TICKS_NEXT_5M_OPEN_TICKS"
            ),
            "direct_ml_evaluation_response": "SIGNED_INT64_TERMINAL_MOVE_TICKS",
            "direct_ml_feature_cutoff": "BAR_END_NS_LTE_DECISION_NS_NOT_ENTRY_NS",
            "direct_ml_feature_and_target_price_unit": "INTEGER_TICKS",
            "direct_ml_missing_next_5m_policy": (
                "EXCLUDE_IN_FEATURE_ONLY_PRE_OUTCOME_OPPORTUNITY_LATTICE"
            ),
            "direct_ml_post_freeze_invalid_path_policy": "FATAL_NO_ROW_EXCLUSION",
            "direct_ml_training_target": (
                "SIGNED_TERMINAL_MOVE_TICKS_DIV_CAUSAL_NATIVE_ATR20_TICKS"
            ),
            "exit_lattice_canonical_json": _canonical_json_bytes(exit_lattice).decode("ascii"),
            "exit_variant_count": 85,
            "maximum_concurrent_positions_per_strategy": 1,
            "path_timeframe_seconds": 1,
            "standard_friction_ticks": 14,
            "stress_friction_ticks": 18,
            "structural_complete_case_anchor_lattice": (
                "CANDIDATE_INDEPENDENT_7H_SAME_LINEAGE_5M_ONLY_PRE_OUTCOME"
            ),
            "structural_complete_case_lookahead_seconds": 25_200,
            "structural_complete_case_pre_filter_support_preserved": True,
            "structural_complete_case_unexpected_post_freeze_censor": "FAIL_CLOSED",
            "structural_five_minute_gap_policy": (
                "SPLIT_NONOVERLAPPING_LOCAL_INTERVALS_AND_CENSOR_CROSS_GAP_PATHS"
            ),
            "terminal_adverse_ticks": 2,
            "variable_cost_ticks": 5,
        },
        "holdout_gates": {
            "active_entry_days_minimum": 18,
            "both_halves_net_ticks_strictly_positive": True,
            "contract_count_minimum": 2,
            "fills_minimum": 24,
            "holm_alpha_denominator": 20,
            "holm_alpha_numerator": 1,
            "holm_family_maximum": MAXIMUM_HOLDOUT_FINALISTS,
            "holm_method": "HOLM_STEP_DOWN",
            "holdout_halves_canonical_json": _canonical_json_bytes(
                (
                    {
                        "day_count": 60,
                        "end_date": "2026-04-27",
                        "half_key": "HOLDOUT_HALF_1",
                        "start_date": "2026-02-16",
                    },
                    {
                        "day_count": 60,
                        "end_date": "2026-07-08",
                        "half_key": "HOLDOUT_HALF_2",
                        "start_date": "2026-04-28",
                    },
                )
            ).decode("ascii"),
            "net_over_maximum_drawdown_minimum_denominator": 4,
            "net_over_maximum_drawdown_minimum_numerator": 3,
            "net_ticks_strictly_positive": True,
            "null_deltas_strictly_positive": True,
            "profit_factor_minimum_denominator": 10,
            "profit_factor_minimum_numerator": 11,
            "terminal_fail_status": ("ONE_SHOT_UNSEALED_ALL_CASES_HOLDOUT_DIAGNOSTIC_FAIL"),
            "terminal_inconclusive_rule": "EVERY_AUTHORIZED_CANDIDATE_SAMPLE_INELIGIBLE_P_EQ_1",
            "terminal_inconclusive_status": (
                "ONE_SHOT_UNSEALED_ALL_CASES_HOLDOUT_DIAGNOSTIC_INCONCLUSIVE"
            ),
            "terminal_pass_rule": "AT_LEAST_ONE_HOLM_REJECT_AND_ALL_GATES_PASS",
            "terminal_pass_status": ("ONE_SHOT_UNSEALED_ALL_CASES_HOLDOUT_DIAGNOSTIC_PASS"),
            "terminal_fail_rule": (
                "NO_PASS_AND_AT_LEAST_ONE_EVALUABLE_CANDIDATE_HARD_STATISTICAL_OR_ECONOMIC_FAIL"
            ),
        },
        "lifecycle": {
            "artifact_publication": "CONTENT_ADDRESSED_CANONICAL_JSON_MODE_0444",
            "candidate_or_recipe_change_after_precommit": "PROHIBITED",
            "crash_policy": ("OSERROR_RESUMABLE_VERIFIED_PREFIX_ONLY_BEFORE_HOLDOUT_AUTHORIZED"),
            "failed_branch": "DETERMINISTIC_INTEGRITY_ERROR_TO_FAILED_TERMINAL",
            "holdout_one_shot_error_policy": (
                "ANY_EXCEPTION_AT_OR_AFTER_HOLDOUT_AUTHORIZED_TO_FAILED_TERMINAL_NO_RETRY"
            ),
            "holdout_masks_before_holdout_outcome_loader": True,
            "ledger": "ATOMIC_APPEND_ONLY_PREDECESSOR_HASHED_SINGLE_WRITER",
            "partial_walk_forward_result_release": "PROHIBITED",
            "search_outcomes_before_universe_freeze": "PROHIBITED",
            "stage_order": [
                "PRECOMMITTED",
                "SEARCH_FEATURE_EVENT_UNIVERSE_AND_CATALOG_FROZEN",
                "SEARCH_TRAINING_SELECTION_AND_MODEL_ARTIFACTS_RELEASED_MAXIMUM_12",
                "WALK_FORWARD_MASKS_FROZEN_ALL_5_FOLDS",
                "WALK_FORWARD_RESULTS_RELEASED_ATOMIC_MAXIMUM_3",
                "HOLDOUT_AUTHORIZED",
                "HOLDOUT_MASKS_FROZEN",
                "HOLDOUT_RESULTS_RELEASED",
                "COMPLETED",
            ],
            "walk_forward_masks_before_any_walk_forward_outcome_loader": True,
            "zero_finalist_policy": "SKIP_ALL_LATER_FEATURE_AND_OUTCOME_PAYLOADS",
        },
        "ml": {
            "direct_candidate_count": 288,
            "final_search_fold_key": "SEARCH_FINAL",
            "hgb_maximum_leaves": 15,
            "hgb_maximum_nodes_per_tree": 29,
            "hgb_maximum_trees": 200,
            "linear_maximum_iterations": 50_000,
            "maximum_raw_feature_count": 221,
            "maximum_raw_rows_per_model": 110_000,
            "maximum_search_fit_count_after_rate_recipe_sharing": 5_040,
            "maximum_transformed_feature_count": 442,
            "meta_candidate_count": 192,
            "null_world_order": ["REAL", "CIRCULAR_TARGET", "MATCHED_TARGET"],
            "search_fit_fold_count": 7,
            "walk_forward_or_holdout_fit_count": 0,
        },
        "multiplicity": {
            "daily_grouping": "SIGNAL_ANCHOR_DECISION_DATE_NOT_EXIT_DATE",
            "daily_vector_domain": "EVERY_FROZEN_DECISION_DATE_WITH_EXPLICIT_ZERO_FILL",
            "holdout_method": "HOLM_STEP_DOWN",
            "holdout_economics_application": "AFTER_HOLM_REJECTION",
            "holdout_family": "EVERY_FROZEN_WALK_FORWARD_FINALIST",
            "holdout_one_sided_alpha_denominator": 20,
            "holdout_one_sided_alpha_numerator": 1,
            "missing_or_error_p_value": 1,
            "p_star": (
                "MAX_SIGN_REAL_GT_ZERO_SIGN_REAL_MINUS_CIRCULAR_GT_ZERO_"
                "SIGN_REAL_MINUS_MATCHED_GT_ZERO"
            ),
            "p_star_exact_test": "UPPER_TAIL_BINOMIAL_SIGN_TEST_WITH_RATIONAL_ARITHMETIC",
            "p_star_no_nonzero_differences": "ONE",
            "p_star_zero_differences": "EXCLUDED",
            "search_alpha_or_significance_claim": "NONE_TRAINING_DEVELOPMENT_STAGE",
            "search_descriptive_p_star_only": True,
            "search_method": "NO_INFERENTIAL_GATE",
            "walk_forward_economics_application": "AFTER_BH_REJECTION",
            "walk_forward_family": "EVERY_FROZEN_SEARCH_FINALIST_MAXIMUM_12",
            "walk_forward_method": "BENJAMINI_HOCHBERG",
            "walk_forward_one_sided_q_denominator": 20,
            "walk_forward_one_sided_q_numerator": 1,
            "multiplicity_comparison": "EXACT_FRACTION_LE_CRITICAL_VALUE",
            "multiplicity_tie_break": "CANONICAL_CANDIDATE_ID_ASC",
        },
        "nulls": {
            "entire_ml_pipeline_refit_per_world": True,
            "master_seed": "ai-all-cases-v1",
            "seed_derivation": (
                "UINT32_FIRST8_HEX_SHA256_CANONICAL_JSON_MASTER_SEED_CANDIDATE_ID_WORLD_FOLD_KEY_PURPOSE"
            ),
            "seed_type": "EXACT_UTF8_STRING",
            "world_order": ["REAL", "CIRCULAR_TARGET", "MATCHED_TARGET"],
        },
        "provenance": {
            "config_policy": "SOURCE_COMMIT_FIRST_THEN_DATA_ONLY_CONFIG_COMMIT",
            "dependency_provisioning": TRUSTED_BOOTSTRAP_POLICY["dependency_provisioning"],
            "implementation_closure": (
                "ENTIRE_CAMPAIGN_PACKAGE_ALL_SRC_SYSTEMATIC_FX_PY_ALL_SCRIPTS_PY_PROJECT_BLOBS"
            ),
            "production_launcher": (
                "PINNED_USR_BIN_ENV_MINUS_SMALL_I_TO_PINNED_ABSOLUTE_CPYTHON_"
                "MINUS_SMALL_S_MINUS_CAPITAL_P_MINUS_B_MINUS_CAPITAL_S_"
                "ABSOLUTE_BOOTSTRAP_ONLY"
            ),
            "production_launcher_argv_template": list(_PRODUCTION_LAUNCHER_TEMPLATE),
            "package_non_python_or_symlink_policy": "REJECT",
            "public_dependency_injection": "PROHIBITED",
            "trusted_git": TRUSTED_BOOTSTRAP_POLICY["trusted_git"],
            "verification": "FRESH_PROCESS_READ_ONLY_EXACT_RECOMPUTATION",
        },
        "runtime_environment": {
            **DETERMINISTIC_RUNTIME_ENV,
            "PATH": _TRUSTED_PATH,
            "PYTHONDONTWRITEBYTECODE": "1",
            "__CF_USER_TEXT_ENCODING": _TRUSTED_CF_USER_TEXT_ENCODING,
            "launch_policy": (
                "REQUIRE_PINNED_ENV_MINUS_SMALL_I_CLEAN_ENTRY_THEN_TRUSTED_SAFE_PATH_"
                "BOOTSTRAP_BEFORE_ANY_WORKSPACE_OR_NUMERICAL_IMPORT"
            ),
            "local_bytecode_policy": TRUSTED_BOOTSTRAP_POLICY["local_bytecode_policy"],
            "pycache_prefix_policy": TRUSTED_BOOTSTRAP_POLICY["bytecode_cache"],
            "sys_path_policy": TRUSTED_BOOTSTRAP_POLICY["sys_path"],
            "trusted_env_path": str(_TRUSTED_ENV_PATH),
            "trusted_env_sha256": _TRUSTED_ENV_SHA256,
            "trusted_env_size": _TRUSTED_ENV_SIZE,
            "trusted_git_path": str(_TRUSTED_GIT_PATH),
            "trusted_git_sha256": _TRUSTED_GIT_SHA256,
            "trusted_git_size": _TRUSTED_GIT_SIZE,
            "trusted_git_version": _TRUSTED_GIT_VERSION,
            "trusted_operator_uid": _TRUSTED_OPERATOR_UID,
            "trusted_python_base_prefix": str(_TRUSTED_PYTHON_BASE_PREFIX),
            "trusted_python_path": str(_TRUSTED_PYTHON_PATH),
            "trusted_python_sha256": _TRUSTED_PYTHON_SHA256,
            "trusted_python_size": _TRUSTED_PYTHON_SIZE,
            "trusted_python_version": _TRUSTED_PYTHON_VERSION,
        },
        "search_design": {
            "complete_symbolic_strategy_maximum": 195_840,
            "direct_and_meta_candidate_count": 480,
            "entry_variant_count": 9,
            "exit_variant_count": 85,
            "logical_anchor_policy_count": 1_900_080,
            "maximum_complete_and_ml_candidates": 196_320,
            "maximum_primary_hypothesis_units": 9_696_720,
            "maximum_stage_b_candidate_world_units": 587_520,
            "maximum_ml_candidate_world_units": 1_440,
            "maximum_real_and_control_scoring_units": 10_089_360,
            "outer_validation_block_keys": ["B3", "B4", "B5", "B6", "B7", "B8"],
            "outer_validation_count": 6,
            "purge_active_days": 20,
            "reference_horizon_count": 5,
            "reference_score_cell_count": 9_500_400,
            "reporting_block_count": 4,
            "search_block_count": 8,
            "search_block_lengths": list(SEARCH_BLOCK_LENGTHS),
            "search_blocks_canonical_json": _canonical_json_bytes(_SEARCH_BLOCKS).decode("ascii"),
            "search_final_refit": True,
            "search_internal_phase_order": [
                "STAGE_A_SCORE_CHUNKS",
                "STAGE_A_TOP256",
                "STAGE_B_PLAN_FROZEN",
                "STAGE_B_RAW_CHUNKS",
                "SYMBOLIC_TOP24",
                "DIRECT_ML_CHUNKS",
                "META_PLAN_FROZEN",
                "META_ML_CHUNKS",
                "FINAL_MAX12",
            ],
            "search_partial_chunk_policy": (
                "RAW_OR_OOF_AGGREGATES_ONLY_NO_SELECTION_NO_EARLY_STOP"
            ),
            "search_empty_symbolic_policy": (
                "STAGE_B_64_EMPTY_RAW_CHUNKS_DIRECT_288_FULL_META_192_EXPLICIT_INELIGIBLE_"
                "CONTINUE_TO_FINAL12"
            ),
            "search_phase_chunk_counts_canonical_json": _canonical_json_bytes(
                {
                    "DIRECT_ML_CHUNKS": 24,
                    "FINAL_MAX12": 1,
                    "META_ML_CHUNKS": 24,
                    "META_PLAN_FROZEN": 1,
                    "STAGE_A_SCORE_CHUNKS": 64,
                    "STAGE_A_TOP256": 1,
                    "STAGE_B_PLAN_FROZEN": 1,
                    "STAGE_B_RAW_CHUNKS": 64,
                    "SYMBOLIC_TOP24": 1,
                }
            ).decode("ascii"),
            "search_subledger_exact_event_count": 181,
            "search_resume_policy": "VERIFY_SUBLEDGER_AND_RUN_LOWEST_INCOMPLETE_CHUNK",
            "search_subledger": ("ATOMIC_PREDECESSOR_HASHED_MODE_0444_ALL_LEAVES_IN_FINAL_CLOSURE"),
            "stage_a_maximum_selection": 256,
            "stage_ranges_canonical_json": _canonical_json_bytes(_STAGE_RANGES).decode("ascii"),
            "standard_reporting_blocks_canonical_json": _canonical_json_bytes(
                _REPORTING_BLOCKS
            ).decode("ascii"),
        },
        "search_gates": {
            "active_entry_days_minimum": 30,
            "active_signal_days_minimum": 40,
            "complete_fills_each_outer_validation_minimum": 5,
            "complete_fills_minimum": 48,
            "direct_family_key": "DECISION_TIMEFRAME_SECONDS_PLUS_FEATURE_SET_ID",
            "diversity_anchor_action_identity": "ROW_ID_PLUS_DIRECTION",
            "diversity_anchor_jaccard_rule": "5_TIMES_INTERSECTION_LT_4_TIMES_UNION",
            "diversity_application": "GREEDY_IN_FROZEN_ECONOMIC_RANK_ORDER",
            "diversity_daily_pnl_correlation_rule": (
                "25_TIMES_COVARIANCE_NUMERATOR_SQUARED_LT_16_TIMES_VARIANCE_PRODUCT"
            ),
            "diversity_zero_variance_policy": "FAIL_CLOSED_PAIR_REJECTED",
            "family_selection_maximum": 2,
            "meta_family_key": "INHERIT_PREFIX_OR_FINAL_BASE_TRIGGER_FAMILY",
            "minimum_positive_outer_validations": 4,
            "minimum_positive_reporting_blocks": 3,
            "net_ticks_strictly_positive": True,
            "null_real_oof_net_strictly_gt_both_controls": True,
            "outer_validation_count": 6,
            "profit_factor_minimum_denominator": 20,
            "profit_factor_minimum_numerator": 21,
            "ranking": [
                "POSITIVE_OUTER_VALIDATION_COUNT_DESC",
                "WORST_OUTER_VALIDATION_EV_DESC",
                "STRESS_18_TICK_NET_DESC",
                "MEDIAN_OUTER_VALIDATION_EV_DESC",
                "MAXIMUM_DRAWDOWN_ASC",
                "CANONICAL_CANDIDATE_ID_ASC",
            ],
            "raw_signals_each_outer_validation_minimum": 6,
            "raw_signals_each_reporting_block_minimum": 6,
            "raw_signals_minimum": 60,
            "search_bh_or_alpha_gate": False,
            "search_selection_maximum": MAXIMUM_SEARCH_SELECTION,
            "selection_direct_maximum": 4,
            "selection_meta_maximum": 4,
            "selection_symbolic_maximum": 6,
            "symbolic_family_key": "BASE_EVENT_TRIGGER_FAMILY",
            "stress_18_tick_net_strictly_positive": True,
        },
        "stage_a_gates": {
            "active_signal_days_minimum": 40,
            "family_direction_maximum": 8,
            "family_maximum": 16,
            "horizon_active_entry_days_minimum": 30,
            "horizon_fills_minimum": 48,
            "horizon_net_ticks_strictly_positive": True,
            "horizon_positive_reporting_groups_minimum": 2,
            "minimum_robust_horizons": 3,
            "reference_entry": "FIRST_OBSERVED_1S_OPEN_IN_ANCHOR_TO_ANCHOR_PLUS_300S",
            "reference_missing_or_shortened_path": "ABSENT_FROM_SURFACE_NEVER_SHORTENED",
            "reference_path": "EXACT_FULL_SAME_LINEAGE_STRUCTURAL_5M_COVERAGE",
            "reference_structural_lattice_freeze": (
                "CANDIDATE_INDEPENDENT_7H_5M_ONLY_BEFORE_FIRST_SEARCH_1S_OPEN"
            ),
            "reference_terminal": (
                "LAST_1S_CLOSE_AT_EXACT_HORIZON_BOUNDARY_STALENESS_STRICTLY_LT_300S"
            ),
            "unexpected_censor_after_structural_lattice_freeze": "FAIL_CLOSED",
            "raw_signals_each_reporting_group_minimum": 6,
            "raw_signals_minimum": 60,
            "selection_maximum": 256,
            "selection_pair_budget_formula": (
                "SUM_EVALUABLE_SUPPORT_COUNT_TIMES_9_ENTRIES_TIMES_3_WORLDS_LE_100000"
            ),
            "selection_pair_budget_maximum": 100_000,
            "selection_pair_budget_policy": (
                "GLOBAL_DEDUPLICATED_FROZEN_RANK_GREEDY_SKIP_IF_REMAINING_BUDGET_EXCEEDED_"
                "NO_EARLY_STOP"
            ),
        },
        "schema_version": AI_ALL_CASES_CONFIG_SCHEMA,
        "scope": {
            "holdout_evidence": "ONE_SHOT_UNSEALED_LOCAL_DIAGNOSTIC",
            "physical_holdout_isolation": False,
            "search_claim": "RETROSPECTIVE_DEVELOPMENT_AND_TRAINING_NOT_OOS_EVIDENCE",
            "strict_backtest_claim": False,
            "walk_forward_claim": "FIRST_OOS_EVIDENCE_IF_PRECOMMITTED_BEFORE_OPEN",
        },
        "selection": {
            "holdout_family_maximum": MAXIMUM_HOLDOUT_FINALISTS,
            "search_selection_maximum": MAXIMUM_SEARCH_SELECTION,
            "semantic_subset_order": "PRESERVE_FROZEN_ECONOMIC_RANK_ORDER",
            "walk_forward_fold_keys": list(WALK_FORWARD_FOLD_KEYS),
            "walk_forward_selection_maximum": MAXIMUM_HOLDOUT_FINALISTS,
        },
        "status": {
            "database_mutation": False,
            "network_access": False,
            "paper_live_or_promotion_authority": False,
            "physical_holdout_isolation": False,
            "strict_backtest_claim": False,
            "strict_sealed_holdout_claim": False,
        },
        "universe_counts": {
            "base_event_count": 1_740,
            "context_count": 13,
            "delay_count": 6,
            "direct_ml_count": 288,
            "entry_variant_count": 9,
            "exit_variant_count": 85,
            "logical_anchor_policy_count": 1_900_080,
            "meta_ml_count": 192,
            "reference_horizon_count": 5,
            "reference_score_cell_count": 9_500_400,
            "time_filter_count": 14,
        },
        "walk_forward_gates": {
            "active_entry_days_each_fold_minimum": 10,
            "active_entry_days_minimum": 75,
            "bh_q_denominator": 20,
            "bh_q_numerator": 1,
            "contract_count_minimum": 5,
            "fills_each_fold_minimum": 12,
            "fills_minimum": 100,
            "maximum_finalists": MAXIMUM_HOLDOUT_FINALISTS,
            "minimum_positive_folds": 4,
            "model_refit_count": 0,
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
                "CANONICAL_CANDIDATE_ID_ASC",
            ],
            "worst_fold_profit_factor_floor_denominator": 10,
            "worst_fold_profit_factor_floor_numerator": 7,
            "worst_loss_over_median_positive_maximum_denominator": 2,
            "worst_loss_over_median_positive_maximum_numerator": 3,
        },
    }
    catalog_identity = _canonical_sha256(
        {key: bindings[key] for key in _CATALOG_IDENTITY_BINDING_KEYS}
    )
    gate_identity = _selected_contract_sha256(contract, _GATE_SECTION_KEYS)
    execution = contract.get("execution")
    if not isinstance(execution, dict):  # pragma: no cover - construction invariant
        raise AllCasesConfigError("execution contract is absent")
    cost_identity = _canonical_sha256({key: execution[key] for key in _COST_FIELD_KEYS})
    if (
        catalog_identity != _PREDECESSOR_CATALOG_IDENTITY_SHA256
        or gate_identity != _PREDECESSOR_GATE_IDENTITY_SHA256
        or cost_identity != _PREDECESSOR_COST_IDENTITY_SHA256
    ):
        raise AllCasesConfigError("attempt-3 catalogs, gates, or costs drifted")
    current_scientific_identity = _scientific_section_sha256(contract)
    if current_scientific_identity != _PREDECESSOR_SCIENTIFIC_SECTION_SHA256:
        raise AllCasesConfigError("attempt-3 scientific contract drifted")
    contract["recovery"] = {
        "attempt_number": 3,
        "catalog_identity_sha256": catalog_identity,
        "catalogs_unchanged": True,
        "cost_identity_sha256": cost_identity,
        "costs_unchanged": True,
        "current_scientific_section_sha256": current_scientific_identity,
        "embargo_opened": False,
        "failure_boundary": "AFTER_STAGE_A_TOP256_BEFORE_STAGE_B_PLAN_FROZEN",
        "failure_cause": (
            "CONTROL_OPPORTUNITY_CHRONOLOGICAL_ROWS_NOT_CANONICAL_UINT64_SEGMENT_ORDER"
        ),
        "failure_outcomes_opened": True,
        "gate_identity_sha256": gate_identity,
        "gates_unchanged": True,
        "holdout_opened": False,
        "implementation_delta": (
            "CONTROL_OPPORTUNITY_ROWS_SORTED_BY_EXISTING_TYPED_CANONICAL_KEY_"
            "BEFORE_HASH_AND_DATACLASS_VALIDATION"
        ),
        "predecessor_code_commit": "5de31e9c303a0b0bbcdb1151b01a49f1a41d4efc",
        "predecessor_config_commit": "f771e06da3987d5ebe1b1731f4a6fa278421a907",
        "predecessor_config_file_sha256": (
            "b2286a3135effd0dd3a244efdc4893e959b88e071e4ab84fb0bddfc960d6ed92"
        ),
        "predecessor_config_id": "ai_all_cases_v1_attempt2",
        "predecessor_config_relative_path": "configs/research/ai_all_cases_v1_attempt2.toml",
        "predecessor_config_semantic_sha256": (
            "c5632c024cc66902cb18b719194c81cde1090a9ee3716c7f1b8e2cd7ee845be5"
        ),
        "predecessor_failed_event_sha256": (
            "841f1235337a7be50599340acc4c98067bf423f98f3c4c62fa354a055d851cef"
        ),
        "predecessor_failure_code": "INTEGRITY_3AEE38A737287C9DFCEA7ED2",
        "predecessor_implementation_sha256": (
            "4de206f40842fec44591679f11b5c638db2ddb6e7e2ffaf8b12357495d917de3"
        ),
        "predecessor_internal_search_event_count": 65,
        "predecessor_internal_search_head_sha256": (
            "7692322614884f8d52f7be1279944ced04f53b63a7d15c5ea59d3631ebb68af6"
        ),
        "predecessor_outer_event_count": 3,
        "predecessor_precommitted_event_sha256": (
            "9e5ef8bcab21614bed30d11a40504398b2b530f3ba46ee4a66677bef7ea23822"
        ),
        "predecessor_request_sha256": (
            "6b8eef5a9746aaf8bba00e9513b9659a84ccdf039c6995e13315191d032aed0b"
        ),
        "predecessor_root_directory_count": 14,
        "predecessor_root_evidence_manifest_kind": "CANONICAL_NORMALIZED_TREE_ROWS",
        "predecessor_root_evidence_manifest_sha256": (
            "62fe4e87a2df0e23068685e6b0cc15b8816f459e7bebed6c404c0ec499d93c70"
        ),
        "predecessor_root_file_bytes": 6_259_097_194,
        "predecessor_root_file_count": 200,
        "predecessor_run_relative_root": "data/derived/bar_patterns/ai_all_cases_v1_attempt2",
        "predecessor_runtime_identity_sha256": (
            "bdc98c52f9e92550473b77785c9fa1e00845d5ea7fc51257e2a9f85f0b5de141"
        ),
        "predecessor_scientific_section_sha256": (_PREDECESSOR_SCIENTIFIC_SECTION_SHA256),
        "predecessor_search_universe_artifact_sha256": (
            "09941411919c5c17ccefac46541e7c98148fdd018c7f81744cce4e9a8e8a210f"
        ),
        "predecessor_search_universe_event_sha256": (
            "df770ca628496ebcd80908925530722c25c200d0fd47db0d0608ec52130bed01"
        ),
        "predecessor_search_universe_frozen": True,
        "predecessor_stage_a_score_chunk_count": 64,
        "predecessor_stage_a_top256_artifact_sha256": (
            "5f2830da226dc53aa3cfaeea0fa781a9ec07b7ed7e5579c590d4be5a99cfb446"
        ),
        "predecessor_stage_a_top256_complete": True,
        "predecessor_stage_b_plan_frozen": False,
        "predecessor_universe_leaf_count": 64,
        "predecessor_universe_root_sha256": (
            "3b303a00e55b8b294d773439dc47c802343224b463d49a692994b2933c3ea1b1"
        ),
        "scientific_contract_equality_claim": True,
        "scientific_delta": "NONE",
        "search_1s_opened": True,
        "walk_forward_opened": False,
    }
    return contract


def expected_ai_all_cases_contract() -> dict[str, object]:
    return json.loads(_canonical_json_bytes(_static_contract()))


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
    else:  # pragma: no cover - contract tables intentionally stay flat
        raise AllCasesConfigError(f"cannot render TOML field {key}")
    return f"{key} = {encoded}"


def render_ai_all_cases_toml_template() -> str:
    """Render invalid provenance markers for the later config-only commit."""

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
        table = document[section]
        if not isinstance(table, dict):  # pragma: no cover
            raise AllCasesConfigError("template section differs")
        lines.extend(("", f"[{section}]"))
        lines.extend(_toml_assignment(key, table[key]) for key in sorted(table))
    return "\n".join(lines) + "\n"


def _campaign_python_paths(project_root: Path) -> tuple[str, ...]:
    package = _safe_project_descendant(
        project_root,
        project_root / CAMPAIGN_PACKAGE_RELATIVE_PATH,
        directory=True,
    )
    campaigns_root = _safe_project_descendant(
        project_root,
        project_root / "campaigns",
        directory=True,
    )
    namespace_init = _safe_project_descendant(
        project_root,
        project_root / "campaigns/__init__.py",
        directory=False,
    )
    namespace_cache = project_root / "campaigns/__pycache__"
    if set(campaigns_root.iterdir()) != {namespace_init, package}:
        raise AllCasesConfigError("campaign namespace exposes an unbound import sibling")
    if namespace_init.is_symlink() or not namespace_init.is_file() or namespace_cache.exists():
        raise AllCasesConfigError("campaign namespace is unsafe or contains __pycache__")
    paths: list[str] = [namespace_init.relative_to(project_root).as_posix()]
    for path in package.rglob("*"):
        if path.is_symlink() or "__pycache__" in path.parts:
            raise AllCasesConfigError("campaign package contains a symlink or __pycache__")
        if path.is_dir():
            continue
        if not path.is_file() or path.suffix != ".py":
            raise AllCasesConfigError("campaign package contains a non-Python file")
        paths.append(path.relative_to(project_root).as_posix())
    ordered = tuple(sorted(paths))
    if not ordered or len(set(ordered)) != len(ordered):
        raise AllCasesConfigError("campaign source path set is empty or duplicated")
    return ordered


def _legacy_runtime_paths(project_root: Path) -> tuple[str, ...]:
    paths: list[str] = []
    for relative_root in (Path("src/systematic_fx"), Path("scripts")):
        tree = _safe_project_descendant(
            project_root,
            project_root / relative_root,
            directory=True,
        )
        for path in tree.rglob("*"):
            if path.is_symlink():
                raise AllCasesConfigError("legacy runtime contains a symbolic path")
            if path.name == "__pycache__":
                raise AllCasesConfigError("legacy runtime contains a non-Python import leaf")
            if path.is_dir():
                continue
            if not path.is_file() or path.suffix != ".py":
                raise AllCasesConfigError("legacy runtime contains a non-Python import leaf")
            paths.append(path.relative_to(project_root).as_posix())
    ordered = tuple(sorted(paths))
    if not ordered or len(set(ordered)) != len(ordered):
        raise AllCasesConfigError("legacy runtime path set is empty or duplicated")
    return ordered


def _blob_row(project_root: Path, relative: str, role: str) -> dict[str, object]:
    path = project_root / relative
    if path.is_symlink() or not path.is_file():
        raise AllCasesConfigError(f"{role} blob is missing or unsafe")
    payload = path.read_bytes()
    return {
        "byte_size": len(payload),
        "relative_path": relative,
        "role": role,
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def all_cases_implementation_document(project_root: Path | str) -> dict[str, object]:
    root = Path(project_root).expanduser().resolve(strict=True)
    campaign = _campaign_python_paths(root)
    legacy = _legacy_runtime_paths(root)
    return {
        "campaign_files": [_blob_row(root, relative, "CAMPAIGN") for relative in campaign],
        "implementation_schema": "systematic_fx.ai_all_cases_implementation.v1",
        "legacy_runtime_blobs": [
            _blob_row(root, relative, "LEGACY_RUNTIME") for relative in legacy
        ],
        "project_blobs": [_blob_row(root, relative, "PROJECT") for relative in _PROJECT_BLOBS],
    }


def all_cases_implementation_sha256(project_root: Path | str) -> str:
    return _canonical_sha256(all_cases_implementation_document(project_root))


def dependency_lock_sha256(project_root: Path | str) -> str:
    root = Path(project_root).expanduser().resolve(strict=True)
    lock = root / "uv.lock"
    if lock.is_symlink() or not lock.is_file():
        raise AllCasesConfigError("dependency lock is missing or unsafe")
    return hashlib.sha256(lock.read_bytes()).hexdigest()


def _load_validated_dataset_contract(
    project_root: Path,
) -> tuple[object, object]:
    """Rebuild the exact outcome-blind manifest, calendar, and split identity."""

    from systematic_fx.research.bar_pipeline import load_bar_dataset_manifest
    from systematic_fx.validation.bar_splits import plan_bar_splits

    manifest_path = (project_root / DATASET_MANIFEST_RELATIVE_PATH).resolve(strict=True)
    dataset = load_bar_dataset_manifest(
        manifest_path,
        expected_sha256=EXPECTED_DATASET_MANIFEST_SHA256,
    )
    eligible = dataset.eligible_active_dates
    if (
        dataset.dataset_manifest_sha256 != EXPECTED_DATASET_MANIFEST_SHA256
        or dataset.handoff_sha256 != EXPECTED_DATASET_HANDOFF_SHA256
        or dataset.source_manifest_sha256 != EXPECTED_SOURCE_MANIFEST_SHA256
        or len(eligible) != EXPECTED_ACTIVE_DATE_COUNT
        or eligible[0].isoformat() != "2022-01-03"
        or eligible[-1].isoformat() != "2026-07-31"
        or _canonical_sha256([item.isoformat() for item in eligible])
        != EXPECTED_ACTIVE_CALENDAR_SHA256
    ):
        raise AllCasesConfigError("dataset manifest or active-calendar identity differs")
    if any(
        {artifact.timeframe_seconds for artifact in partition.artifacts}
        != {1, 60, 300, 1_800, 3_600}
        for partition in dataset.partitions
    ):
        raise AllCasesConfigError("dataset timeframe lattice differs")
    split = plan_bar_splits(eligible)
    if split.sha256 != EXPECTED_SPLIT_PLAN_SHA256:
        raise AllCasesConfigError("dataset split identity differs")
    names = (
        "SEARCH",
        "WF1",
        "WF2",
        "WF3",
        "WF4",
        "WF5",
        "EMBARGO",
        "HOLDOUT",
        "HOLDOUT_OUTCOME_TAIL",
    )
    actual_ranges = []
    for name, value in zip(names, split.ranges, strict=True):
        decision_count = (
            0
            if value.decision_end_date is None
            else eligible.index(value.decision_end_date) - value.start_active_ordinal + 2
        )
        actual_ranges.append(
            {
                "active_day_count": value.active_day_count,
                "decision_day_count": decision_count,
                "decision_end_date": (
                    None if value.decision_end_date is None else value.decision_end_date.isoformat()
                ),
                "end_date": value.end_date.isoformat(),
                "stage_key": name,
                "start_date": value.start_date.isoformat(),
            }
        )
    if tuple(actual_ranges) != _STAGE_RANGES:
        raise AllCasesConfigError("stage boundaries differ")
    decisions = eligible[: split.discovery.active_day_count - 20]
    search_blocks = []
    cursor = 0
    for number, length in enumerate(SEARCH_BLOCK_LENGTHS, start=1):
        block = decisions[cursor : cursor + length]
        search_blocks.append(
            {
                "block_key": f"B{number}",
                "day_count": len(block),
                "end_date": block[-1].isoformat(),
                "start_date": block[0].isoformat(),
            }
        )
        cursor += length
    reporting_blocks = tuple(
        {
            "block_key": f"R{number}",
            "day_count": value.active_day_count,
            "end_date": value.end_date.isoformat(),
            "start_date": value.start_date.isoformat(),
        }
        for number, value in enumerate(split.discovery_reporting_blocks, start=1)
    )
    if (
        cursor != len(decisions)
        or tuple(search_blocks) != _SEARCH_BLOCKS
        or reporting_blocks != _REPORTING_BLOCKS
    ):
        raise AllCasesConfigError("Search block boundaries differ")
    return dataset, split


def _trusted_git_repository(project_root: Path) -> tuple[Path, Path]:
    """Reject Git database indirection that can rewrite committed provenance."""

    root = project_root.resolve(strict=True)
    git_dir = _safe_project_descendant(root, root / ".git", directory=True)
    for directory in (git_dir, git_dir / "info", git_dir / "objects", git_dir / "objects/info"):
        try:
            metadata = directory.lstat()
        except OSError as error:
            raise AllCasesConfigError("trusted Git database is structurally incomplete") from error
        if (
            directory.is_symlink()
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
        ):
            raise AllCasesConfigError("trusted Git database path is unsafe")
    forbidden = (
        git_dir / "commondir",
        git_dir / "info/grafts",
        git_dir / "objects/info/alternates",
        git_dir / "objects/info/http-alternates",
        git_dir / "shallow",
    )
    for path in forbidden:
        try:
            path.lstat()
        except FileNotFoundError:
            continue
        except OSError as error:
            raise AllCasesConfigError("trusted Git database cannot be inspected") from error
        raise AllCasesConfigError("trusted Git database contains provenance redirection")
    replace_root = git_dir / "refs/replace"
    try:
        replace_metadata = replace_root.lstat()
    except FileNotFoundError:
        replace_metadata = None
    except OSError as error:
        raise AllCasesConfigError("trusted Git replacement refs cannot be inspected") from error
    if replace_metadata is not None:
        if replace_root.is_symlink() or not stat.S_ISDIR(replace_metadata.st_mode):
            raise AllCasesConfigError("trusted Git replacement refs are unsafe")
        if any(replace_root.rglob("*")):
            raise AllCasesConfigError("trusted Git database contains replacement refs")
    return root, git_dir


def _git(project_root: Path, *arguments: str) -> bytes:
    _require_trusted_git_runtime()
    root, git_dir = _trusted_git_repository(project_root)
    try:
        process = subprocess.run(
            [
                str(_TRUSTED_GIT_PATH),
                "--no-replace-objects",
                "--no-lazy-fetch",
                "--no-pager",
                "--no-optional-locks",
                "-c",
                "core.fsmonitor=false",
                "-c",
                "core.commitGraph=false",
                "-c",
                f"core.hooksPath={os.devnull}",
                f"--git-dir={git_dir}",
                f"--work-tree={root}",
                *arguments,
            ],
            check=False,
            capture_output=True,
            env=_trusted_git_environment(),
            stdin=subprocess.DEVNULL,
        )
    except OSError as error:
        raise AllCasesConfigError("Git provenance cannot be verified") from error
    if process.returncode != 0 or process.stderr != b"":
        raise AllCasesConfigError("Git provenance cannot be verified")
    return process.stdout


def verify_committed_all_cases_implementation(project_root: Path, code_commit: str) -> None:
    """Require current closure paths and bytes to match the source commit."""

    if (
        _COMMIT.fullmatch(code_commit) is None
        or not set(code_commit) - {"0"}
        or _git(project_root, "cat-file", "-t", code_commit).strip() != b"commit"
    ):
        raise AllCasesConfigError("all-cases source commit is invalid")
    document = all_cases_implementation_document(project_root)
    rows = [
        *document["campaign_files"],
        *document["legacy_runtime_blobs"],
        *document["project_blobs"],
    ]
    committed_campaign = tuple(
        sorted(
            line
            for line in _git(
                project_root,
                "ls-tree",
                "-r",
                "--name-only",
                code_commit,
                "--",
                "campaigns/__init__.py",
                CAMPAIGN_PACKAGE_RELATIVE_PATH.as_posix(),
            )
            .decode("utf-8")
            .splitlines()
            if line
        )
    )
    current_campaign = tuple(row["relative_path"] for row in document["campaign_files"])
    if committed_campaign != current_campaign:
        raise AllCasesConfigError("campaign file set differs from source commit")
    committed_runtime = tuple(
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
                "scripts",
            )
            .decode("utf-8")
            .splitlines()
            if line.endswith(".py") and "__pycache__" not in Path(line).parts
        )
    )
    current_runtime = tuple(row["relative_path"] for row in document["legacy_runtime_blobs"])
    if committed_runtime != current_runtime:
        raise AllCasesConfigError("legacy runtime file set differs from source commit")
    for row in rows:
        relative = str(row["relative_path"])
        if (
            _git(project_root, "cat-file", "-t", f"{code_commit}:{relative}").strip() != b"blob"
            or _git(project_root, "show", f"{code_commit}:{relative}")
            != (project_root / relative).read_bytes()
        ):
            raise AllCasesConfigError("implementation bytes differ from source commit")


def _verify_data_only_config_commit(
    project_root: Path,
    code_commit: str,
    raw_config: bytes,
) -> None:
    """Prove the config is one committed add directly after the source commit."""

    relative = AI_ALL_CASES_CONFIG_RELATIVE_PATH.as_posix()
    if _git(project_root, "ls-files", "--error-unmatch", "--", relative).strip() != (
        relative.encode("utf-8")
    ):
        raise AllCasesConfigError("all-cases config is not tracked")
    if (
        _git(project_root, "cat-file", "-t", f"HEAD:{relative}").strip() != b"blob"
        or _git(project_root, "show", f"HEAD:{relative}") != raw_config
        or _git(project_root, "show", f":{relative}") != raw_config
    ):
        raise AllCasesConfigError("config worktree, index, and HEAD bytes differ")
    commits = tuple(
        line
        for line in _git(
            project_root,
            "log",
            "--first-parent",
            "--reverse",
            "--format=%H",
            "HEAD",
            "--",
            relative,
        )
        .decode("ascii")
        .splitlines()
        if line
    )
    if len(commits) != 1 or _COMMIT.fullmatch(commits[0]) is None:
        raise AllCasesConfigError("config has no unique data-only introduction commit")
    config_commit = commits[0]
    if _git(project_root, "rev-parse", f"{config_commit}^").decode("ascii").strip() != (
        code_commit
    ):
        raise AllCasesConfigError("config commit parent differs from the source commit")
    changed = tuple(
        line
        for line in _git(
            project_root,
            "diff-tree",
            "--no-commit-id",
            "--name-status",
            "-r",
            config_commit,
        )
        .decode("utf-8")
        .splitlines()
        if line
    )
    if changed != (f"A\t{relative}",):
        raise AllCasesConfigError("config introduction commit is not data-only")
    _git(project_root, "merge-base", "--is-ancestor", config_commit, "HEAD")
    dirty = _git(
        project_root,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        "--",
        "campaigns/__init__.py",
        CAMPAIGN_PACKAGE_RELATIVE_PATH.as_posix(),
        "src/systematic_fx",
        "scripts",
        *_PROJECT_BLOBS,
        relative,
    )
    if dirty:
        raise AllCasesConfigError("runtime/config closure is not clean at HEAD")


_PREDECESSOR_CONFIG_RELATIVE_PATH: Final = Path("configs/research/ai_all_cases_v1.toml")
_PREDECESSOR_RUN_RELATIVE_ROOT: Final = Path("data/derived/bar_patterns/ai_all_cases_v1")
_PREDECESSOR_CODE_COMMIT: Final = "35464327d67a8a4e1001c3ba258bcaef4be69715"
_PREDECESSOR_CONFIG_COMMIT: Final = "b51b320c8f2bdfdb0f5b42d65989aca092e0d4d4"
_PREDECESSOR_CONFIG_FILE_SHA256: Final = (
    "d63278a150345a086c73dc38daa4fff8a478fd43caaa1ea374e3584c793ccbd4"
)
_PREDECESSOR_CONFIG_SEMANTIC_SHA256: Final = (
    "bd2c5d86f76094dcaf82a209904b9ea23694a4aaf72421f3cbf042dce0faee96"
)
_PREDECESSOR_REQUEST_SHA256: Final = (
    "f2cba305bc6e0522a992a5562bf089ddcb92e8cdf12236000f0565d1028f351c"
)
_PREDECESSOR_PRECOMMITTED_EVENT_SHA256: Final = (
    "35a7d8d495869b27cb6cbf9feffa791fe3feef9438c4a74ef9dbfdad9f1de838"
)
_PREDECESSOR_FAILED_EVENT_SHA256: Final = (
    "1a93f3229f4f0d6be5ffe0722941c8d6d9a00a182ef6ee2f4be68453a7305a38"
)
_PREDECESSOR_RUNTIME_IDENTITY_SHA256: Final = (
    "bdc98c52f9e92550473b77785c9fa1e00845d5ea7fc51257e2a9f85f0b5de141"
)
_PREDECESSOR_FAILURE_CODE: Final = "INTEGRITY_D327C1949A6A6B78BAEA59A2"
_PREDECESSOR_REQUEST_RELATIVE_PATH: Final = (
    f"artifacts/all-cases-request-{_PREDECESSOR_REQUEST_SHA256}.json"
)
_PREDECESSOR_TREE_CONTRACT: Final = {
    ".": ("DIRECTORY", 0o755, 6, 192, None),
    ".mutation.lock": (
        "FILE",
        0o600,
        1,
        0,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    ),
    "artifacts": ("DIRECTORY", 0o755, 3, 96, None),
    _PREDECESSOR_REQUEST_RELATIVE_PATH: (
        "FILE",
        0o444,
        1,
        72_295,
        _PREDECESSOR_REQUEST_SHA256,
    ),
    "ledger": ("DIRECTORY", 0o755, 4, 128, None),
    "ledger/events": ("DIRECTORY", 0o755, 4, 128, None),
    "ledger/events/event-00000001.json": (
        "FILE",
        0o444,
        1,
        528,
        _PREDECESSOR_PRECOMMITTED_EVENT_SHA256,
    ),
    "ledger/events/event-00000002.json": (
        "FILE",
        0o444,
        1,
        376,
        _PREDECESSOR_FAILED_EVENT_SHA256,
    ),
    "ledger/staging": ("DIRECTORY", 0o755, 2, 64, None),
    "staging": ("DIRECTORY", 0o755, 3, 96, None),
    "staging/artifacts": ("DIRECTORY", 0o755, 2, 64, None),
}


def _predecessor_lstat_snapshot(root: Path) -> tuple[tuple[object, ...], ...]:
    paths = (root, *sorted(root.rglob("*")))
    rows: list[tuple[object, ...]] = []
    for path in paths:
        metadata = path.lstat()
        rows.append(
            (
                "." if path == root else path.relative_to(root).as_posix(),
                metadata.st_dev,
                metadata.st_ino,
                metadata.st_mode,
                metadata.st_nlink,
                metadata.st_uid,
                metadata.st_gid,
                metadata.st_size,
                metadata.st_mtime_ns,
                metadata.st_ctime_ns,
            )
        )
    return tuple(rows)


def _predecessor_config_snapshot(path: Path) -> tuple[object, ...]:
    metadata = path.lstat()
    digest = (
        hashlib.sha256(path.read_bytes()).hexdigest() if stat.S_ISREG(metadata.st_mode) else None
    )
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
        digest,
    )


def _verify_predecessor_tree_contract(root: Path) -> None:
    observed_paths = (root, *sorted(root.rglob("*")))
    observed_relative = tuple(
        "." if path == root else path.relative_to(root).as_posix() for path in observed_paths
    )
    if observed_relative != tuple(_PREDECESSOR_TREE_CONTRACT):
        raise AllCasesConfigError("predecessor run tree leaf set differs")
    for path, relative in zip(observed_paths, observed_relative, strict=True):
        expected_type, expected_mode, expected_nlink, expected_size, expected_sha = (
            _PREDECESSOR_TREE_CONTRACT[relative]
        )
        metadata = path.lstat()
        actual_type = (
            "DIRECTORY"
            if stat.S_ISDIR(metadata.st_mode)
            else "FILE"
            if stat.S_ISREG(metadata.st_mode)
            else "OTHER"
        )
        if (
            path.is_symlink()
            or actual_type != expected_type
            or stat.S_IMODE(metadata.st_mode) != expected_mode
            or metadata.st_nlink != expected_nlink
            or metadata.st_uid != os.geteuid()
            or metadata.st_size != expected_size
        ):
            raise AllCasesConfigError("predecessor run tree metadata differs")
        if (
            expected_sha is not None
            and hashlib.sha256(path.read_bytes()).hexdigest() != expected_sha
        ):
            raise AllCasesConfigError("predecessor run tree bytes differ")
    if (
        (root / "internal").exists()
        or (root / "internal").is_symlink()
        or any((root / "ledger/staging").iterdir())
        or any((root / "staging/artifacts").iterdir())
    ):
        raise AllCasesConfigError("predecessor crossed its recorded failure boundary")


def _verify_predecessor_config(
    project_root: Path,
) -> tuple[bytes, dict[str, object]]:
    relative = _PREDECESSOR_CONFIG_RELATIVE_PATH.as_posix()
    path = _safe_project_descendant(
        project_root,
        project_root / _PREDECESSOR_CONFIG_RELATIVE_PATH,
        directory=False,
    )
    raw = path.read_bytes()
    if (
        path.is_symlink()
        or stat.S_IMODE(path.stat().st_mode) != 0o644
        or path.stat().st_nlink != 1
        or path.stat().st_uid != os.geteuid()
        or hashlib.sha256(raw).hexdigest() != _PREDECESSOR_CONFIG_FILE_SHA256
        or _git(project_root, "cat-file", "-t", _PREDECESSOR_CONFIG_COMMIT).strip() != b"commit"
        or _git(project_root, "show", f"{_PREDECESSOR_CONFIG_COMMIT}:{relative}") != raw
        or _git(project_root, "show", f"HEAD:{relative}") != raw
        or _git(project_root, "show", f":{relative}") != raw
        or _git(project_root, "rev-parse", f"{_PREDECESSOR_CONFIG_COMMIT}^").decode("ascii").strip()
        != _PREDECESSOR_CODE_COMMIT
    ):
        raise AllCasesConfigError("predecessor config bytes or commit differs")
    changed = tuple(
        line
        for line in _git(
            project_root,
            "diff-tree",
            "--no-commit-id",
            "--name-status",
            "-r",
            _PREDECESSOR_CONFIG_COMMIT,
        )
        .decode("utf-8")
        .splitlines()
        if line
    )
    history = tuple(
        line
        for line in _git(
            project_root,
            "log",
            "--first-parent",
            "--reverse",
            "--format=%H",
            "HEAD",
            "--",
            relative,
        )
        .decode("ascii")
        .splitlines()
        if line
    )
    if (
        changed != (f"A\t{relative}",)
        or history != (_PREDECESSOR_CONFIG_COMMIT,)
        or _git(
            project_root,
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--",
            relative,
        )
        != b""
    ):
        raise AllCasesConfigError("predecessor config provenance differs")
    _git(project_root, "merge-base", "--is-ancestor", _PREDECESSOR_CONFIG_COMMIT, "HEAD")
    try:
        parsed = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:  # pragma: no cover
        raise AllCasesConfigError("predecessor config cannot be decoded") from error
    if (
        not isinstance(parsed, dict)
        or parsed.get("schema_version") != "systematic_fx.ai_all_cases_config.v1"
        or parsed.get("config_id") != "ai_all_cases_v1"
        or parsed.get("code_commit") != _PREDECESSOR_CODE_COMMIT
        or parsed.get("implementation_sha256")
        != "41b6a921b3cd38c8a5c8cb22f2137b5271809540df803f329059f34f51087962"
        or _canonical_sha256(parsed) != _PREDECESSOR_CONFIG_SEMANTIC_SHA256
        or _scientific_section_sha256(parsed) != _ATTEMPT1_SCIENTIFIC_SECTION_SHA256
    ):
        raise AllCasesConfigError("predecessor config semantic identity differs")
    bindings = parsed.get("bindings")
    execution = parsed.get("execution")
    if (
        not isinstance(bindings, dict)
        or not isinstance(execution, dict)
        or _canonical_sha256({key: bindings[key] for key in _CATALOG_IDENTITY_BINDING_KEYS})
        != _PREDECESSOR_CATALOG_IDENTITY_SHA256
        or _selected_contract_sha256(parsed, _GATE_SECTION_KEYS)
        != _PREDECESSOR_GATE_IDENTITY_SHA256
        or _canonical_sha256({key: execution[key] for key in _COST_FIELD_KEYS})
        != _PREDECESSOR_COST_IDENTITY_SHA256
    ):
        raise AllCasesConfigError("predecessor catalogs, gates, or costs differ")
    return raw, parsed


def _verify_predecessor_ledger_and_request(
    run_root: Path,
    predecessor_config: dict[str, object],
) -> None:
    request_path = run_root / _PREDECESSOR_REQUEST_RELATIVE_PATH
    event_paths = (
        run_root / "ledger/events/event-00000001.json",
        run_root / "ledger/events/event-00000002.json",
    )
    try:
        request = json.loads(request_path.read_bytes())
        events = tuple(json.loads(path.read_bytes()) for path in event_paths)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:  # pragma: no cover
        raise AllCasesConfigError("predecessor evidence is invalid JSON") from error
    if (
        _canonical_json_bytes(request) != request_path.read_bytes()
        or _canonical_json_bytes(request.get("config")) != _canonical_json_bytes(predecessor_config)
        or request.get("artifact_schema") != "systematic_fx.ai_all_cases_request.v1"
        or request.get("authority") != AI_ALL_CASES_AUTHORITY
        or request.get("config_file_sha256") != _PREDECESSOR_CONFIG_FILE_SHA256
        or request.get("config_semantic_sha256") != _PREDECESSOR_CONFIG_SEMANTIC_SHA256
        or request.get("runtime_identity_sha256") != _PREDECESSOR_RUNTIME_IDENTITY_SHA256
        or _canonical_sha256(request.get("runtime_identity"))
        != _PREDECESSOR_RUNTIME_IDENTITY_SHA256
    ):
        raise AllCasesConfigError("predecessor request binding differs")
    first, second = events
    if any(
        _canonical_json_bytes(event) != path.read_bytes()
        for event, path in zip(events, event_paths, strict=True)
    ):
        raise AllCasesConfigError("predecessor ledger bytes are not canonical")
    expected_artifact = {
        "artifact_type": "AI_ALL_CASES_REQUEST",
        "byte_size": 72_295,
        "relative_path": _PREDECESSOR_REQUEST_RELATIVE_PATH.removeprefix("artifacts/"),
        "sha256": _PREDECESSOR_REQUEST_SHA256,
    }
    if (
        set(first)
        != {
            "artifact_schema",
            "event_type",
            "payload",
            "predecessor_sha256",
            "recorded_at_utc",
            "request_sha256",
            "sequence",
        }
        or set(second) != set(first)
        or first.get("artifact_schema") != "systematic_fx.ai_all_cases_event.v1"
        or second.get("artifact_schema") != "systematic_fx.ai_all_cases_event.v1"
        or first.get("event_type") != "PRECOMMITTED"
        or second.get("event_type") != "FAILED"
        or first.get("sequence") != 1
        or second.get("sequence") != 2
        or first.get("predecessor_sha256") is not None
        or second.get("predecessor_sha256") != _PREDECESSOR_PRECOMMITTED_EVENT_SHA256
        or first.get("request_sha256") != _PREDECESSOR_REQUEST_SHA256
        or second.get("request_sha256") != _PREDECESSOR_REQUEST_SHA256
        or first.get("payload") != {"request_artifact": expected_artifact}
        or second.get("payload") != {"failure_code": _PREDECESSOR_FAILURE_CODE}
    ):
        raise AllCasesConfigError("predecessor ledger semantic boundary differs")


def verify_failed_predecessor_attempt(project_root: Path | str) -> None:
    """Read-only exact audit of the immutable attempt-1 PRECOMMITTED->FAILED evidence."""

    root = Path(project_root).expanduser().resolve(strict=True)
    run_root = _safe_project_descendant(
        root,
        root / _PREDECESSOR_RUN_RELATIVE_ROOT,
        directory=True,
    )
    config_path = root / _PREDECESSOR_CONFIG_RELATIVE_PATH
    before = _predecessor_lstat_snapshot(run_root)
    config_before = _predecessor_config_snapshot(config_path)
    try:
        _raw, predecessor_config = _verify_predecessor_config(root)
        _verify_predecessor_tree_contract(run_root)
        _verify_predecessor_ledger_and_request(run_root, predecessor_config)
    finally:
        try:
            after = _predecessor_lstat_snapshot(run_root)
        except OSError as error:  # pragma: no cover - adversarial concurrent mutation
            raise AllCasesConfigError("predecessor evidence changed during audit") from error
        if after != before:
            raise AllCasesConfigError("predecessor evidence mutated during read-only audit")
        try:
            config_after = _predecessor_config_snapshot(config_path)
        except OSError as error:  # pragma: no cover - adversarial concurrent mutation
            raise AllCasesConfigError("predecessor config changed during audit") from error
        if config_after != config_before:
            raise AllCasesConfigError("predecessor config mutated during read-only audit")


_ATTEMPT2_CONFIG_RELATIVE_PATH: Final = Path("configs/research/ai_all_cases_v1_attempt2.toml")
_ATTEMPT2_RUN_RELATIVE_ROOT: Final = Path("data/derived/bar_patterns/ai_all_cases_v1_attempt2")
_ATTEMPT2_CODE_COMMIT: Final = "5de31e9c303a0b0bbcdb1151b01a49f1a41d4efc"
_ATTEMPT2_CONFIG_COMMIT: Final = "f771e06da3987d5ebe1b1731f4a6fa278421a907"
_ATTEMPT2_CONFIG_FILE_SHA256: Final = (
    "b2286a3135effd0dd3a244efdc4893e959b88e071e4ab84fb0bddfc960d6ed92"
)
_ATTEMPT2_CONFIG_SEMANTIC_SHA256: Final = (
    "c5632c024cc66902cb18b719194c81cde1090a9ee3716c7f1b8e2cd7ee845be5"
)
_ATTEMPT2_IMPLEMENTATION_SHA256: Final = (
    "4de206f40842fec44591679f11b5c638db2ddb6e7e2ffaf8b12357495d917de3"
)
_ATTEMPT2_RUNTIME_IDENTITY_SHA256: Final = (
    "bdc98c52f9e92550473b77785c9fa1e00845d5ea7fc51257e2a9f85f0b5de141"
)
_ATTEMPT2_REQUEST_SHA256: Final = "6b8eef5a9746aaf8bba00e9513b9659a84ccdf039c6995e13315191d032aed0b"
_ATTEMPT2_UNIVERSE_ARTIFACT_SHA256: Final = (
    "09941411919c5c17ccefac46541e7c98148fdd018c7f81744cce4e9a8e8a210f"
)
_ATTEMPT2_UNIVERSE_ROOT_SHA256: Final = (
    "3b303a00e55b8b294d773439dc47c802343224b463d49a692994b2933c3ea1b1"
)
_ATTEMPT2_OUTER_EVENT_SHA256S: Final = (
    "9e5ef8bcab21614bed30d11a40504398b2b530f3ba46ee4a66677bef7ea23822",
    "df770ca628496ebcd80908925530722c25c200d0fd47db0d0608ec52130bed01",
    "841f1235337a7be50599340acc4c98067bf423f98f3c4c62fa354a055d851cef",
)
_ATTEMPT2_FAILURE_CODE: Final = "INTEGRITY_3AEE38A737287C9DFCEA7ED2"
_ATTEMPT2_INTERNAL_HEAD_SHA256: Final = (
    "7692322614884f8d52f7be1279944ced04f53b63a7d15c5ea59d3631ebb68af6"
)
_ATTEMPT2_TOP256_ARTIFACT_SHA256: Final = (
    "5f2830da226dc53aa3cfaeea0fa781a9ec07b7ed7e5579c590d4be5a99cfb446"
)
_ATTEMPT2_TREE_MANIFEST_SHA256: Final = (
    "62fe4e87a2df0e23068685e6b0cc15b8816f459e7bebed6c404c0ec499d93c70"
)
_ATTEMPT2_REQUEST_RELATIVE_PATH: Final = (
    f"artifacts/all-cases-request-{_ATTEMPT2_REQUEST_SHA256}.json"
)
_ATTEMPT2_UNIVERSE_RELATIVE_PATH: Final = (
    f"artifacts/search-universe-{_ATTEMPT2_UNIVERSE_ARTIFACT_SHA256}.json"
)


def _streaming_file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _attempt2_tree_manifest(
    run_root: Path,
) -> tuple[dict[str, object], dict[str, str]]:
    paths = (run_root, *sorted(run_root.rglob("*")))
    rows: list[dict[str, object]] = []
    file_sha_by_relative: dict[str, str] = {}
    file_count = 0
    directory_count = 0
    file_bytes = 0
    for path in paths:
        metadata = path.lstat()
        relative = "." if path == run_root else path.relative_to(run_root).as_posix()
        kind = (
            "DIRECTORY"
            if stat.S_ISDIR(metadata.st_mode)
            else "FILE"
            if stat.S_ISREG(metadata.st_mode)
            else "OTHER"
        )
        if (
            path.is_symlink()
            or kind == "OTHER"
            or metadata.st_uid != os.geteuid()
            or metadata.st_nlink < 1
            or (kind == "DIRECTORY" and stat.S_IMODE(metadata.st_mode) != 0o755)
            or (
                kind == "FILE"
                and stat.S_IMODE(metadata.st_mode)
                != (0o600 if relative == ".mutation.lock" else 0o444)
            )
            or (kind == "FILE" and metadata.st_nlink != 1)
        ):
            raise AllCasesConfigError("attempt-2 predecessor tree metadata differs")
        sha256 = None
        if kind == "FILE":
            file_count += 1
            file_bytes += metadata.st_size
            sha256 = _streaming_file_sha256(path)
            file_sha_by_relative[relative] = sha256
        else:
            directory_count += 1
        rows.append(
            {
                "kind": kind,
                "mode": stat.S_IMODE(metadata.st_mode),
                "nlink": metadata.st_nlink,
                "relative_path": relative,
                "sha256": sha256,
                "size": metadata.st_size,
            }
        )
    document: dict[str, object] = {
        "root_relative_path": _ATTEMPT2_RUN_RELATIVE_ROOT.as_posix(),
        "rows": rows,
        "schema": "systematic_fx.ai_all_cases_predecessor_tree_manifest.v1",
    }
    if (
        len(rows) != 214
        or file_count != 200
        or directory_count != 14
        or file_bytes != 6_259_097_194
        or _canonical_sha256(document) != _ATTEMPT2_TREE_MANIFEST_SHA256
    ):
        raise AllCasesConfigError("attempt-2 predecessor tree manifest differs")
    for relative in (
        "internal/search/staging",
        "internal/universe-staging",
        "ledger/staging",
        "staging/artifacts",
    ):
        if any((run_root / relative).iterdir()):
            raise AllCasesConfigError("attempt-2 predecessor staging is not empty")
    return document, file_sha_by_relative


def _verify_attempt2_config(project_root: Path) -> tuple[bytes, dict[str, object]]:
    relative = _ATTEMPT2_CONFIG_RELATIVE_PATH.as_posix()
    path = _safe_project_descendant(
        project_root,
        project_root / _ATTEMPT2_CONFIG_RELATIVE_PATH,
        directory=False,
    )
    raw = path.read_bytes()
    if (
        path.is_symlink()
        or stat.S_IMODE(path.stat().st_mode) != 0o644
        or path.stat().st_nlink != 1
        or path.stat().st_uid != os.geteuid()
        or hashlib.sha256(raw).hexdigest() != _ATTEMPT2_CONFIG_FILE_SHA256
        or _git(project_root, "cat-file", "-t", _ATTEMPT2_CONFIG_COMMIT).strip() != b"commit"
        or _git(project_root, "show", f"{_ATTEMPT2_CONFIG_COMMIT}:{relative}") != raw
        or _git(project_root, "show", f"HEAD:{relative}") != raw
        or _git(project_root, "show", f":{relative}") != raw
        or _git(project_root, "rev-parse", f"{_ATTEMPT2_CONFIG_COMMIT}^").decode("ascii").strip()
        != _ATTEMPT2_CODE_COMMIT
    ):
        raise AllCasesConfigError("attempt-2 predecessor config bytes or commit differs")
    changed = tuple(
        line
        for line in _git(
            project_root,
            "diff-tree",
            "--no-commit-id",
            "--name-status",
            "-r",
            _ATTEMPT2_CONFIG_COMMIT,
        )
        .decode("utf-8")
        .splitlines()
        if line
    )
    history = tuple(
        line
        for line in _git(
            project_root,
            "log",
            "--first-parent",
            "--reverse",
            "--format=%H",
            "HEAD",
            "--",
            relative,
        )
        .decode("ascii")
        .splitlines()
        if line
    )
    if (
        changed != (f"A\t{relative}",)
        or history != (_ATTEMPT2_CONFIG_COMMIT,)
        or _git(
            project_root,
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--",
            relative,
        )
        != b""
    ):
        raise AllCasesConfigError("attempt-2 predecessor config provenance differs")
    _git(project_root, "merge-base", "--is-ancestor", _ATTEMPT2_CONFIG_COMMIT, "HEAD")
    try:
        parsed = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:  # pragma: no cover
        raise AllCasesConfigError("attempt-2 predecessor config cannot be decoded") from error
    if (
        not isinstance(parsed, dict)
        or parsed.get("schema_version") != "systematic_fx.ai_all_cases_config.v2"
        or parsed.get("campaign_design_id") != AI_ALL_CASES_CAMPAIGN_DESIGN_ID
        or parsed.get("config_id") != "ai_all_cases_v1_attempt2"
        or parsed.get("code_commit") != _ATTEMPT2_CODE_COMMIT
        or parsed.get("implementation_sha256") != _ATTEMPT2_IMPLEMENTATION_SHA256
        or _canonical_sha256(parsed) != _ATTEMPT2_CONFIG_SEMANTIC_SHA256
        or _scientific_section_sha256(parsed) != _PREDECESSOR_SCIENTIFIC_SECTION_SHA256
    ):
        raise AllCasesConfigError("attempt-2 predecessor config semantic identity differs")
    bindings = parsed.get("bindings")
    execution = parsed.get("execution")
    if (
        not isinstance(bindings, dict)
        or not isinstance(execution, dict)
        or _canonical_sha256({key: bindings[key] for key in _CATALOG_IDENTITY_BINDING_KEYS})
        != _PREDECESSOR_CATALOG_IDENTITY_SHA256
        or _selected_contract_sha256(parsed, _GATE_SECTION_KEYS)
        != _PREDECESSOR_GATE_IDENTITY_SHA256
        or _canonical_sha256({key: execution[key] for key in _COST_FIELD_KEYS})
        != _PREDECESSOR_COST_IDENTITY_SHA256
    ):
        raise AllCasesConfigError("attempt-2 predecessor catalogs, gates, or costs differ")
    return raw, parsed


def _attempt2_json(path: Path, *, label: str) -> dict[str, object]:
    raw = path.read_bytes()
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:  # pragma: no cover
        raise AllCasesConfigError(f"attempt-2 {label} is invalid JSON") from error
    if not isinstance(value, dict) or _canonical_json_bytes(value) != raw:
        raise AllCasesConfigError(f"attempt-2 {label} bytes are not canonical")
    return value


def _verify_attempt2_outer_evidence(
    run_root: Path,
    predecessor_config: dict[str, object],
) -> dict[str, object]:
    request_path = run_root / _ATTEMPT2_REQUEST_RELATIVE_PATH
    universe_path = run_root / _ATTEMPT2_UNIVERSE_RELATIVE_PATH
    request = _attempt2_json(request_path, label="request")
    universe = _attempt2_json(universe_path, label="Search universe")
    event_paths = tuple(run_root / f"ledger/events/event-{index:08d}.json" for index in range(1, 4))
    events = tuple(
        _attempt2_json(path, label=f"outer event {index}")
        for index, path in enumerate(event_paths, start=1)
    )
    if (
        _canonical_json_bytes(request.get("config")) != _canonical_json_bytes(predecessor_config)
        or request.get("artifact_schema") != "systematic_fx.ai_all_cases_request.v1"
        or request.get("authority") != AI_ALL_CASES_AUTHORITY
        or request.get("config_file_sha256") != _ATTEMPT2_CONFIG_FILE_SHA256
        or request.get("config_semantic_sha256") != _ATTEMPT2_CONFIG_SEMANTIC_SHA256
        or request.get("runtime_identity_sha256") != _ATTEMPT2_RUNTIME_IDENTITY_SHA256
        or _canonical_sha256(request.get("runtime_identity")) != _ATTEMPT2_RUNTIME_IDENTITY_SHA256
    ):
        raise AllCasesConfigError("attempt-2 predecessor request binding differs")
    expected_keys = {
        "artifact_schema",
        "event_type",
        "payload",
        "predecessor_sha256",
        "recorded_at_utc",
        "request_sha256",
        "sequence",
    }
    if (
        any(set(event) != expected_keys for event in events)
        or any(
            event.get("artifact_schema") != "systematic_fx.ai_all_cases_event.v1"
            for event in events
        )
        or any(
            hashlib.sha256(path.read_bytes()).hexdigest() != expected
            for path, expected in zip(event_paths, _ATTEMPT2_OUTER_EVENT_SHA256S, strict=True)
        )
    ):
        raise AllCasesConfigError("attempt-2 predecessor outer event bytes differ")
    first, second, third = events
    request_artifact = {
        "artifact_type": "AI_ALL_CASES_REQUEST",
        "byte_size": 75_182,
        "relative_path": _ATTEMPT2_REQUEST_RELATIVE_PATH.removeprefix("artifacts/"),
        "sha256": _ATTEMPT2_REQUEST_SHA256,
    }
    universe_artifact = {
        "artifact_type": "AI_ALL_CASES_SEARCH_FEATURE_EVENT_UNIVERSE",
        "byte_size": 14_727,
        "relative_path": _ATTEMPT2_UNIVERSE_RELATIVE_PATH.removeprefix("artifacts/"),
        "sha256": _ATTEMPT2_UNIVERSE_ARTIFACT_SHA256,
    }
    if (
        tuple(event.get("event_type") for event in events)
        != ("PRECOMMITTED", "SEARCH_UNIVERSE_FROZEN", "FAILED")
        or tuple(event.get("sequence") for event in events) != (1, 2, 3)
        or tuple(event.get("request_sha256") for event in events) != (_ATTEMPT2_REQUEST_SHA256,) * 3
        or first.get("predecessor_sha256") is not None
        or second.get("predecessor_sha256") != _ATTEMPT2_OUTER_EVENT_SHA256S[0]
        or third.get("predecessor_sha256") != _ATTEMPT2_OUTER_EVENT_SHA256S[1]
        or first.get("payload") != {"request_artifact": request_artifact}
        or second.get("payload")
        != {
            "universe_artifact": universe_artifact,
            "universe_root_sha256": _ATTEMPT2_UNIVERSE_ROOT_SHA256,
        }
        or third.get("payload") != {"failure_code": _ATTEMPT2_FAILURE_CODE}
    ):
        raise AllCasesConfigError("attempt-2 predecessor outer boundary differs")
    if (
        universe.get("artifact_schema") != "systematic_fx.ai_all_cases_search_universe.v1"
        or universe.get("authority") != AI_ALL_CASES_AUTHORITY
        or universe.get("config_semantic_sha256") != _ATTEMPT2_CONFIG_SEMANTIC_SHA256
        or not isinstance(universe.get("payload"), dict)
        or universe["payload"].get("schema")
        != "systematic_fx.ai_all_cases_search_universe_payload.v1"
        or universe["payload"].get("universe_root_sha256") != _ATTEMPT2_UNIVERSE_ROOT_SHA256
    ):
        raise AllCasesConfigError("attempt-2 predecessor universe binding differs")
    return universe["payload"]


def _verify_attempt2_internal_evidence(
    run_root: Path,
    universe: dict[str, object],
    file_sha_by_relative: dict[str, str],
) -> None:
    universe_rows = universe.get("feature_mask_chunk_artifacts")
    if not isinstance(universe_rows, list) or len(universe_rows) != 64:
        raise AllCasesConfigError("attempt-2 predecessor universe leaf family differs")
    expected_universe_paths: set[str] = set()
    for index, row in enumerate(universe_rows):
        if not isinstance(row, dict) or set(row) != {
            "artifact_sha256",
            "chunk_index",
            "relative_path",
        }:
            raise AllCasesConfigError("attempt-2 predecessor universe leaf schema differs")
        digest = row["artifact_sha256"]
        filename = row["relative_path"]
        relative = f"internal/universe/{filename}"
        if (
            row["chunk_index"] != index
            or not isinstance(digest, str)
            or _SHA256.fullmatch(digest) is None
            or filename != f"universe-{index:03d}-{digest}.json"
            or file_sha_by_relative.get(relative) != digest
        ):
            raise AllCasesConfigError("attempt-2 predecessor universe leaf identity differs")
        expected_universe_paths.add(relative)
    observed_universe_paths = {
        relative for relative in file_sha_by_relative if relative.startswith("internal/universe/")
    }
    if observed_universe_paths != expected_universe_paths:
        raise AllCasesConfigError("attempt-2 predecessor universe closure differs")

    event_paths = tuple(
        run_root / f"internal/search/events/event-{index:08d}.json" for index in range(1, 66)
    )
    expected_artifact_paths: set[str] = set()
    predecessor_sha256: str | None = None
    for index, path in enumerate(event_paths, start=1):
        event = _attempt2_json(path, label=f"internal event {index}")
        raw_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
        phase = "STAGE_A_SCORE_CHUNKS" if index <= 64 else "STAGE_A_TOP256"
        chunk_index = index - 1 if index <= 64 else 0
        artifact_sha256 = event.get("artifact_sha256")
        artifact_filename = event.get("artifact_relative_path")
        artifact_relative = f"internal/search/artifacts/{artifact_filename}"
        if (
            set(event)
            != {
                "artifact_relative_path",
                "artifact_schema",
                "artifact_sha256",
                "chunk_index",
                "phase",
                "predecessor_sha256",
                "recorded_at_utc",
                "sequence",
            }
            or event.get("artifact_schema")
            != "systematic_fx.ai_all_cases_search_subledger_event.v1"
            or event.get("sequence") != index
            or event.get("phase") != phase
            or event.get("chunk_index") != chunk_index
            or event.get("predecessor_sha256") != predecessor_sha256
            or not isinstance(artifact_sha256, str)
            or _SHA256.fullmatch(artifact_sha256) is None
            or artifact_filename != f"{phase.lower()}-{chunk_index:06d}-{artifact_sha256}.json"
            or file_sha_by_relative.get(artifact_relative) != artifact_sha256
        ):
            raise AllCasesConfigError("attempt-2 predecessor internal chain differs")
        expected_artifact_paths.add(artifact_relative)
        predecessor_sha256 = raw_sha256
    observed_event_paths = {
        relative
        for relative in file_sha_by_relative
        if relative.startswith("internal/search/events/")
    }
    observed_artifact_paths = {
        relative
        for relative in file_sha_by_relative
        if relative.startswith("internal/search/artifacts/")
    }
    if (
        predecessor_sha256 != _ATTEMPT2_INTERNAL_HEAD_SHA256
        or observed_event_paths
        != {f"internal/search/events/event-{index:08d}.json" for index in range(1, 66)}
        or observed_artifact_paths != expected_artifact_paths
        or file_sha_by_relative.get(
            "internal/search/artifacts/"
            f"stage_a_top256-000000-{_ATTEMPT2_TOP256_ARTIFACT_SHA256}.json"
        )
        != _ATTEMPT2_TOP256_ARTIFACT_SHA256
        or any("stage_b" in relative for relative in file_sha_by_relative)
    ):
        raise AllCasesConfigError("attempt-2 predecessor internal boundary differs")


def verify_failed_attempt2_predecessor(project_root: Path | str) -> None:
    """Read-only exact audit of attempt 2 through Stage-A TOP256 and terminal failure."""

    root = Path(project_root).expanduser().resolve(strict=True)
    run_root = _safe_project_descendant(
        root,
        root / _ATTEMPT2_RUN_RELATIVE_ROOT,
        directory=True,
    )
    config_path = root / _ATTEMPT2_CONFIG_RELATIVE_PATH
    before = _predecessor_lstat_snapshot(run_root)
    config_before = _predecessor_config_snapshot(config_path)
    try:
        _raw, predecessor_config = _verify_attempt2_config(root)
        _manifest, file_sha_by_relative = _attempt2_tree_manifest(run_root)
        universe = _verify_attempt2_outer_evidence(run_root, predecessor_config)
        _verify_attempt2_internal_evidence(run_root, universe, file_sha_by_relative)
    finally:
        try:
            after = _predecessor_lstat_snapshot(run_root)
            config_after = _predecessor_config_snapshot(config_path)
        except OSError as error:  # pragma: no cover - adversarial concurrent mutation
            raise AllCasesConfigError("attempt-2 predecessor changed during audit") from error
        if after != before or config_after != config_before:
            raise AllCasesConfigError("attempt-2 predecessor mutated during read-only audit")


@dataclass(frozen=True, slots=True)
class AllCasesConfig:
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
            raise AllCasesConfigError("config root is not an object")
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
        raise AllCasesConfigError("all-cases config schema differs")
    for key, expected_value in expected.items():
        if _canonical_json_bytes(value[key]) != _canonical_json_bytes(expected_value):
            raise AllCasesConfigError(f"all-cases config {key} drifted")
    if not isinstance(value["code_commit"], str) or _COMMIT.fullmatch(value["code_commit"]) is None:
        raise AllCasesConfigError("code_commit is not a full Git SHA")
    for key in ("dependency_lock_sha256", "implementation_sha256"):
        if not isinstance(value[key], str) or _SHA256.fullmatch(value[key]) is None:
            raise AllCasesConfigError(f"{key} is not a SHA-256")
    if (
        not isinstance(value["precommitted_at_utc"], str)
        or _UTC_TIMESTAMP.fullmatch(value["precommitted_at_utc"]) is None
    ):
        raise AllCasesConfigError("precommitted_at_utc is not canonical UTC")
    return value


def load_ai_all_cases_config(project_root: Path | str) -> AllCasesConfig:
    """Load the data-only config and bind it to exact committed bytes."""

    _require_deterministic_runtime_environment()
    root = Path(project_root).expanduser().resolve(strict=True)
    path = root / AI_ALL_CASES_CONFIG_RELATIVE_PATH
    if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root):
        raise AllCasesConfigError("all-cases config is missing or unsafe")
    raw = path.read_bytes()
    try:
        document = _validated_document(tomllib.loads(raw.decode("utf-8")))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise AllCasesConfigError("all-cases config is invalid TOML") from error
    _runtime_module_origins(root)
    code_commit = str(document["code_commit"])
    implementation = str(document["implementation_sha256"])
    dependency = str(document["dependency_lock_sha256"])
    if all_cases_implementation_sha256(root) != implementation:
        raise AllCasesConfigError("all-cases implementation identity drifted")
    if dependency_lock_sha256(root) != dependency:
        raise AllCasesConfigError("all-cases dependency identity drifted")
    verify_committed_all_cases_implementation(root, code_commit)
    _verify_data_only_config_commit(root, code_commit, raw)
    return AllCasesConfig(
        path=path.resolve(strict=True),
        file_sha256=hashlib.sha256(raw).hexdigest(),
        semantic_sha256=_canonical_sha256(document),
        code_commit=code_commit,
        implementation_sha256=implementation,
        dependency_lock_sha256=dependency,
        precommitted_at_utc=str(document["precommitted_at_utc"]),
        canonical_bytes=_canonical_json_bytes(document),
    )


__all__ = [
    "AI_ALL_CASES_AUTHORITY",
    "AI_ALL_CASES_CAMPAIGN_DESIGN_ID",
    "AI_ALL_CASES_CONFIG_ID",
    "AI_ALL_CASES_CONFIG_RELATIVE_PATH",
    "AI_ALL_CASES_CONFIG_SCHEMA",
    "AI_ALL_CASES_RUN_RELATIVE_ROOT",
    "AllCasesConfig",
    "AllCasesConfigError",
    "all_cases_implementation_document",
    "all_cases_implementation_sha256",
    "dependency_lock_sha256",
    "expected_ai_all_cases_contract",
    "load_ai_all_cases_config",
    "render_ai_all_cases_toml_template",
    "verify_committed_all_cases_implementation",
    "verify_failed_attempt2_predecessor",
    "verify_failed_predecessor_attempt",
]
