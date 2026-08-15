"""Stdlib-only clean-environment launcher for the governed all-cases campaign.

The sole production invocation is the pinned ``/usr/bin/env -i`` wrapper,
followed by the pinned absolute CPython binary with ``-s -P -B -S`` and this
file's absolute path.  This module deliberately performs no workspace import
until it has verified that exact entry environment and executable chain,
rejected local bytecode caches, and installed a unique empty cache prefix
outside the checkout.
"""

from __future__ import annotations

import os
import sys


class BootstrapError(RuntimeError):
    """The process is not a safe clean-environment campaign launcher."""


def _early_clean_entry_environment() -> dict[str, str]:
    """Validate env-i bytes using only pinned-CPython frozen/builtin modules."""

    arguments = tuple(sys.argv[1:])
    positions = tuple(index for index, item in enumerate(arguments) if item == "--project-root")
    if len(positions) != 1 or positions[0] + 1 >= len(arguments):
        raise BootstrapError("exactly one explicit --project-root is required")
    raw_root = arguments[positions[0] + 1]
    if not raw_root.startswith("/") or raw_root == "/" or raw_root.endswith("/"):
        raise BootstrapError("--project-root value is not a canonical absolute path")
    expected = {
        "LC_ALL": "C",
        "MKL_NUM_THREADS": "1",
        "NUMEXPR_NUM_THREADS": "1",
        "OMP_NUM_THREADS": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONHASHSEED": "0",
        "TZ": "UTC",
        "VECLIB_MAXIMUM_THREADS": "1",
        "VIRTUAL_ENV": f"{raw_root}/.venv",
        "__CF_USER_TEXT_ENCODING": "0x1F5:0x0:0x0",
    }
    if os.geteuid() != 501 or dict(os.environ) != expected:
        raise BootstrapError(
            "bootstrap entry environment differs; use the pinned /usr/bin/env -i command"
        )
    return expected


_EARLY_ENTRY_ENVIRONMENT: dict[str, str] | None = None
if __name__ == "__main__":  # before any non-frozen/builtin import
    try:
        _EARLY_ENTRY_ENVIRONMENT = _early_clean_entry_environment()
    except BootstrapError as error:
        print(f"AI all-cases bootstrap failed closed: {error}", file=sys.stderr)
        raise SystemExit(1) from error


import hashlib
import importlib.machinery
import importlib.util
import json
import stat
import subprocess
import tempfile
import types
from pathlib import Path
from typing import Final

_RELATIVE_SELF: Final = Path("campaigns/ai_all_cases_v1/bootstrap.py")
_CACHE_ROOTS: Final = (Path("campaigns"), Path("src/systematic_fx"), Path("scripts"))
_BASE_PATHS_ENV: Final = "AI_ALL_CASES_STDLIB_BASE_PATHS_JSON"
_BASE_PATHS_SHA_ENV: Final = "AI_ALL_CASES_STDLIB_BASE_PATHS_SHA256"
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
_TRUSTED_PYTHON_VERSION: Final = (3, 12, 13)
_TRUSTED_CF_USER_TEXT_ENCODING: Final = "0x1F5:0x0:0x0"
_TRUSTED_GIT_PATH: Final = Path("/usr/bin/git")
_TRUSTED_GIT_SHA256: Final = "179301dcb41ea78accc3fa0048a7e6f6710d891945a751a34addd622020c1818"
_TRUSTED_GIT_SIZE: Final = 118_928
_TRUSTED_GIT_VERSION: Final = "git version 2.50.1 (Apple Git-155)"
_TRUSTED_PATH: Final = "/usr/bin:/bin"
_DETERMINISTIC_ENTRY_ENV: Final = {
    "LC_ALL": "C",
    "MKL_NUM_THREADS": "1",
    "NUMEXPR_NUM_THREADS": "1",
    "OMP_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "PYTHONDONTWRITEBYTECODE": "1",
    "PYTHONHASHSEED": "0",
    "TZ": "UTC",
    "VECLIB_MAXIMUM_THREADS": "1",
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
        raise BootstrapError("bootstrap identity is not canonical JSON") from error


def _clean_entry_environment(project_root: Path) -> dict[str, str]:
    if os.geteuid() != _TRUSTED_OPERATOR_UID:
        raise BootstrapError("trusted operator identity differs")
    expected = {
        **_DETERMINISTIC_ENTRY_ENV,
        "VIRTUAL_ENV": str(project_root / ".venv"),
        "__CF_USER_TEXT_ENCODING": _TRUSTED_CF_USER_TEXT_ENCODING,
    }
    if f"0x{os.geteuid():X}:0x0:0x0" != _TRUSTED_CF_USER_TEXT_ENCODING:
        raise BootstrapError("trusted macOS text identity differs")
    return expected


def _require_clean_entry_environment(project_root: Path) -> str:
    """Reject every inherited variable before this module mutates the environment."""

    expected = _clean_entry_environment(project_root)
    if dict(os.environ) != expected or (
        _EARLY_ENTRY_ENVIRONMENT is not None and _EARLY_ENTRY_ENVIRONMENT != expected
    ):
        raise BootstrapError(
            "bootstrap entry environment differs; use the pinned /usr/bin/env -i command"
        )
    return hashlib.sha256(_canonical_json_bytes(expected)).hexdigest()


def _require_pinned_binary(
    path: Path,
    *,
    expected_uid: int,
    expected_size: int,
    expected_sha256: str,
) -> None:
    """Require a nonsymbolic absolute path whose components are not shared-writable."""

    if not path.is_absolute():
        raise BootstrapError("trusted executable path is not absolute")
    current = Path("/")
    components = path.parts[1:]
    for index, part in enumerate(components):
        current /= part
        try:
            metadata = current.lstat()
        except OSError as error:
            raise BootstrapError("trusted executable path is missing") from error
        final = index == len(components) - 1
        if (
            current.is_symlink()
            or metadata.st_uid not in {0, expected_uid}
            or metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            or (not final and not stat.S_ISDIR(metadata.st_mode))
            or (final and not stat.S_ISREG(metadata.st_mode))
        ):
            raise BootstrapError("trusted executable path contains an unsafe component")
    metadata = path.lstat()
    if (
        metadata.st_uid != expected_uid
        or metadata.st_size != expected_size
        or metadata.st_mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH) == 0
        or hashlib.sha256(path.read_bytes()).hexdigest() != expected_sha256
    ):
        raise BootstrapError("trusted executable binary identity differs")


def _require_trusted_preexec_runtime() -> None:
    """Recheck the pinned env wrapper and already-running CPython executable."""

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
        or sys.version_info[:3] != _TRUSTED_PYTHON_VERSION
    ):
        raise BootstrapError("trusted CPython runtime identity differs")


def _safe_descendant(project_root: Path, path: Path, *, directory: bool) -> Path:
    try:
        relative = path.relative_to(project_root)
    except ValueError as error:
        raise BootstrapError("bootstrap path leaves the project root") from error
    current = project_root
    for index, part in enumerate(relative.parts):
        current /= part
        metadata = current.lstat()
        final = index == len(relative.parts) - 1
        if (
            current.is_symlink()
            or ((not final or directory) and not stat.S_ISDIR(metadata.st_mode))
            or (final and not directory and not stat.S_ISREG(metadata.st_mode))
        ):
            raise BootstrapError("bootstrap path contains an unsafe component")
    if not path.resolve(strict=True).is_relative_to(project_root):
        raise BootstrapError("bootstrap path resolves outside the project root")
    return path


def _project_root_from_argv(argv: tuple[str, ...]) -> Path:
    positions = tuple(index for index, item in enumerate(argv) if item == "--project-root")
    if len(positions) != 1 or positions[0] + 1 >= len(argv):
        raise BootstrapError("exactly one explicit --project-root is required")
    raw = argv[positions[0] + 1]
    requested_lexical = Path(raw)
    launcher_lexical = Path(sys.argv[0])
    if (
        not raw
        or raw.startswith("-")
        or not requested_lexical.is_absolute()
        or not launcher_lexical.is_absolute()
    ):
        raise BootstrapError("--project-root value is missing")
    try:
        requested = requested_lexical.resolve(strict=True)
        launcher = launcher_lexical.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise BootstrapError("bootstrap or project root is missing") from error
    expected_launcher = requested / _RELATIVE_SELF
    if (
        str(requested_lexical) != str(requested)
        or str(launcher_lexical) != str(expected_launcher)
        or launcher != expected_launcher
        or Path(__file__).resolve(strict=True) != expected_launcher
    ):
        raise BootstrapError("bootstrap path and requested project root differ")
    _safe_descendant(requested, requested / "campaigns", directory=True)
    _safe_descendant(requested, requested / "campaigns/ai_all_cases_v1", directory=True)
    _safe_descendant(requested, expected_launcher, directory=False)
    return requested


def _local_bytecode_entries(project_root: Path) -> tuple[Path, ...]:
    unsafe: list[Path] = []
    for relative in _CACHE_ROOTS:
        tree = _safe_descendant(project_root, project_root / relative, directory=True)
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


def _stdlib_base_paths(project_root: Path) -> tuple[str, ...]:
    if (
        sys.flags.isolated != 0
        or sys.flags.ignore_environment != 0
        or sys.flags.no_user_site != 1
        or sys.flags.safe_path is not True
        or sys.flags.no_site != 1
        or sys.flags.hash_randomization != 0
        or sys.flags.utf8_mode != 1
        or sys.dont_write_bytecode is not True
        or hash("all-cases") != -4_299_525_529_514_689_000
    ):
        raise BootstrapError(
            "CPython -s -P -B -S safe-path/no-site/no-bytecode deterministic mode is required"
        )
    output: list[str] = []
    for item in sys.path:
        if not isinstance(item, str) or not item or not Path(item).is_absolute():
            raise BootstrapError("stdlib base sys.path contains a relative entry")
        resolved = Path(item).resolve(strict=False)
        if resolved.is_relative_to(project_root):
            raise BootstrapError("stdlib base sys.path already enters the project")
        output.append(item)
    return tuple(output)


def _venv_site_packages(project_root: Path) -> Path:
    expected_venv = project_root / ".venv"
    if os.environ.get("VIRTUAL_ENV") != str(expected_venv):
        raise BootstrapError("VIRTUAL_ENV differs from the project-local locked environment")
    site_packages = expected_venv / (
        f"lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages"
    )
    _safe_descendant(project_root, expected_venv, directory=True)
    _safe_descendant(project_root, site_packages, directory=True)
    return site_packages


def _load_source_package(name: str, initializer: Path, package_root: Path) -> None:
    if name in sys.modules:
        raise BootstrapError(f"workspace package {name} predates bootstrap loading")
    specification = importlib.util.spec_from_file_location(
        name,
        initializer,
        submodule_search_locations=[str(package_root)],
    )
    if specification is None or specification.loader is None:
        raise BootstrapError(f"workspace package {name} has no source loader")
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    try:
        specification.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(name, None)
        raise


def _install_workspace_packages(project_root: Path) -> None:
    _load_source_package(
        "systematic_fx",
        project_root / "src/systematic_fx/__init__.py",
        project_root / "src/systematic_fx",
    )
    scripts = types.ModuleType("scripts")
    scripts.__package__ = "scripts"
    scripts.__path__ = [str(project_root / "scripts")]
    scripts_spec = importlib.machinery.ModuleSpec("scripts", loader=None, is_package=True)
    scripts_spec.submodule_search_locations = scripts.__path__
    scripts.__spec__ = scripts_spec
    if "scripts" in sys.modules:
        raise BootstrapError("workspace namespace scripts predates bootstrap loading")
    sys.modules["scripts"] = scripts
    _load_source_package(
        "campaigns",
        project_root / "campaigns/__init__.py",
        project_root / "campaigns",
    )
    _load_source_package(
        "campaigns.ai_all_cases_v1",
        project_root / "campaigns/ai_all_cases_v1/__init__.py",
        project_root / "campaigns/ai_all_cases_v1",
    )


def _external_cache_directory(project_root: Path) -> Path:
    path = Path(tempfile.mkdtemp(prefix="systematic-fx-ai-all-cases-pycache-"))
    os.chmod(path, 0o700)
    metadata = path.lstat()
    resolved = path.resolve(strict=True)
    if (
        path.is_symlink()
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or resolved.is_relative_to(project_root)
        or any(path.iterdir())
    ):
        raise BootstrapError("external bytecode cache directory is unsafe")
    return resolved


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


def _configure_trusted_git() -> None:
    """Pin the only external provenance executable before workspace import."""

    if any(key.startswith(("GIT_", "DYLD_", "LD_")) for key in os.environ):
        raise BootstrapError("prohibited loader or Git environment reached bootstrap")
    os.environ["PATH"] = _TRUSTED_PATH
    for path, directory in (
        (Path("/"), True),
        (Path("/usr"), True),
        (Path("/usr/bin"), True),
        (_TRUSTED_GIT_PATH, False),
    ):
        metadata = path.lstat()
        if (
            path.is_symlink()
            or metadata.st_uid != 0
            or metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            or (directory and not stat.S_ISDIR(metadata.st_mode))
            or (not directory and not stat.S_ISREG(metadata.st_mode))
        ):
            raise BootstrapError("trusted Git path contains an unsafe component")
    metadata = _TRUSTED_GIT_PATH.lstat()
    if (
        metadata.st_size != _TRUSTED_GIT_SIZE
        or metadata.st_mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH) == 0
        or hashlib.sha256(_TRUSTED_GIT_PATH.read_bytes()).hexdigest() != _TRUSTED_GIT_SHA256
    ):
        raise BootstrapError("trusted Git binary identity differs")
    try:
        process = subprocess.run(
            [str(_TRUSTED_GIT_PATH), "--version"],
            check=False,
            capture_output=True,
            env=_trusted_git_environment(),
            stdin=subprocess.DEVNULL,
        )
    except OSError as error:
        raise BootstrapError("trusted Git version cannot be verified") from error
    if (
        process.returncode != 0
        or process.stderr != b""
        or process.stdout.decode("ascii").strip() != _TRUSTED_GIT_VERSION
    ):
        raise BootstrapError("trusted Git version differs")
    os.environ[_TRUSTED_GIT_PATH_ENV] = str(_TRUSTED_GIT_PATH)
    os.environ[_TRUSTED_GIT_SHA_ENV] = _TRUSTED_GIT_SHA256
    os.environ[_TRUSTED_GIT_VERSION_ENV] = _TRUSTED_GIT_VERSION


def _remove_empty_cache(path: Path) -> None:
    if path.is_symlink() or not path.is_dir() or any(path.iterdir()):
        raise BootstrapError("external bytecode cache was populated or replaced")
    parent = path.parent
    path.rmdir()
    descriptor = os.open(parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def main(argv: tuple[str, ...] | None = None) -> int:
    arguments = tuple(sys.argv[1:] if argv is None else argv)
    root = _project_root_from_argv(arguments)
    clean_entry_sha256 = _require_clean_entry_environment(root)
    _require_trusted_preexec_runtime()
    os.environ[_CLEAN_ENTRY_ENV_SHA_ENV] = clean_entry_sha256
    _configure_trusted_git()
    if _local_bytecode_entries(root):
        raise BootstrapError("workspace import surface contains a non-source entry")
    base_paths = _stdlib_base_paths(root)
    site_packages = _venv_site_packages(root)
    cache = _external_cache_directory(root)
    sys.pycache_prefix = str(cache)
    os.environ["PYTHONPYCACHEPREFIX"] = str(cache)
    base_json = json.dumps(list(base_paths), ensure_ascii=True, separators=(",", ":"))
    os.environ[_BASE_PATHS_ENV] = base_json
    os.environ[_BASE_PATHS_SHA_ENV] = hashlib.sha256(base_json.encode("ascii")).hexdigest()
    sys.path[:] = [*base_paths, str(site_packages)]
    try:
        _install_workspace_packages(root)
        from campaigns.ai_all_cases_v1.config import _require_trusted_bootstrap_runtime

        _require_trusted_bootstrap_runtime(root)
        from campaigns.ai_all_cases_v1.__main__ import main as campaign_main

        return campaign_main(arguments)
    finally:
        _remove_empty_cache(cache)


if __name__ == "__main__":  # pragma: no cover - governed subprocess boundary
    try:
        raise SystemExit(main())
    except BootstrapError as error:
        print(f"AI all-cases bootstrap failed closed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
