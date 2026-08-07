"""Content and runtime provenance for governed research runs.

The Git commit records the repository base.  A content-addressed source snapshot
records the exact executable/configuration bytes as they existed for a run,
including an intentionally dirty working tree.  This prevents a base commit from
silently standing in for uncommitted research code.
"""

from __future__ import annotations

import base64
import hashlib
import importlib.metadata
import locale
import os
import platform
import re
import stat
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from systematic_fx.research.hypotheses import canonical_json_bytes

CODE_SNAPSHOT_SCHEMA: Final = "systematic_fx.code_snapshot.v2"
RUNTIME_ENVIRONMENT_SCHEMA: Final = "systematic_fx.runtime_environment.v1"
CODE_SNAPSHOT_DIRECTORY: Final = "code_snapshot_v2"

_GIT_OBJECT_ID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")
_DEFAULT_SINGLE_FILES: Final = (
    "Makefile",
    "pyproject.toml",
    "uv.lock",
)
_DEFAULT_TREES: Final = (
    ("configs", frozenset({".toml", ".md"})),
    ("docs", frozenset({".md"})),
    ("migrations", frozenset({".sql", ".md"})),
    ("src/systematic_fx", frozenset({".py"})),
)
_RUNTIME_PACKAGES: Final = (
    "numpy",
    "polars",
    "psycopg",
    "pyarrow",
    "scikit-learn",
    "scipy",
    "statsmodels",
    "systematic-fx",
)
_NUMERIC_ENVIRONMENT_KEYS: Final = (
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "POLARS_MAX_THREADS",
    "PYTHONHASHSEED",
    "TZ",
)


class ProvenanceError(ValueError):
    """Exact code/runtime provenance could not be constructed or published."""


@dataclass(frozen=True, slots=True)
class SnapshotFile:
    """Identity and reconstructible bytes of one executable snapshot file."""

    relative_path: str
    byte_size: int
    sha256: str
    executable: bool
    content_base64: str

    @property
    def payload(self) -> dict[str, object]:
        return {
            "byte_size": self.byte_size,
            "content_base64": self.content_base64,
            "content_encoding": "base64",
            "executable": self.executable,
            "relative_path": self.relative_path,
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class CodeSnapshot:
    """Canonical source/config bytes bound to one repository base commit."""

    code_commit: str
    files: tuple[SnapshotFile, ...]
    canonical_bytes: bytes
    sha256: str

    @property
    def payload(self) -> dict[str, object]:
        value = {
            "artifact_schema": CODE_SNAPSHOT_SCHEMA,
            "code_commit": self.code_commit,
            "file_count": len(self.files),
            "files": [item.payload for item in self.files],
        }
        return value


@dataclass(frozen=True, slots=True)
class PublishedCodeSnapshot:
    """Immutable derived artifact for a code snapshot."""

    path: Path
    sha256: str
    disposition: str


def _strict_root(value: Path | str) -> Path:
    requested = Path(value).expanduser()
    if requested.is_symlink():
        raise ProvenanceError("workspace_root cannot be a symbolic link")
    try:
        root = requested.resolve(strict=True)
    except FileNotFoundError as error:
        raise ProvenanceError(f"workspace_root does not exist: {requested}") from error
    if not root.is_dir():
        raise ProvenanceError(f"workspace_root is not a directory: {root}")
    return root


def _snapshot_paths(root: Path) -> tuple[Path, ...]:
    candidates: list[Path] = []
    for relative in _DEFAULT_SINGLE_FILES:
        path = root / relative
        if not path.exists():
            raise ProvenanceError(f"required snapshot input is missing: {relative}")
        candidates.append(path)
    for relative_root, suffixes in _DEFAULT_TREES:
        tree = root / relative_root
        if tree.is_symlink() or not tree.is_dir():
            raise ProvenanceError(f"required snapshot tree is unsafe or missing: {relative_root}")
        candidates.extend(
            path
            for path in tree.rglob("*")
            if path.suffix in suffixes and "__pycache__" not in path.parts
        )

    ordered = tuple(sorted(candidates, key=lambda path: path.relative_to(root).as_posix()))
    relative_paths = tuple(path.relative_to(root).as_posix() for path in ordered)
    if len(relative_paths) != len(set(relative_paths)):
        raise ProvenanceError("snapshot path enumeration contains duplicates")
    return ordered


def _hash_regular_file(root: Path, path: Path) -> SnapshotFile:
    relative = path.relative_to(root).as_posix()
    if path.is_symlink():
        raise ProvenanceError(f"snapshot input cannot be a symbolic link: {relative}")
    before = path.stat()
    if not stat.S_ISREG(before.st_mode):
        raise ProvenanceError(f"snapshot input must be a regular file: {relative}")
    digest = hashlib.sha256()
    byte_size = 0
    content = bytearray()
    with path.open("rb") as handle:
        opened = os.fstat(handle.fileno())
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise ProvenanceError(f"snapshot input changed while opening: {relative}")
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            byte_size += len(chunk)
            content.extend(chunk)
        after = os.fstat(handle.fileno())
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if identity_before != identity_after or byte_size != before.st_size:
        raise ProvenanceError(f"snapshot input changed while reading: {relative}")
    return SnapshotFile(
        relative_path=relative,
        byte_size=byte_size,
        sha256=digest.hexdigest(),
        executable=bool(before.st_mode & 0o111),
        content_base64=base64.b64encode(content).decode("ascii"),
    )


def build_code_snapshot(
    workspace_root: Path | str,
    *,
    code_commit: str,
) -> CodeSnapshot:
    """Archive every versioned runtime/config/policy byte used by the research tree."""

    if not isinstance(code_commit, str) or _GIT_OBJECT_ID.fullmatch(code_commit) is None:
        raise ProvenanceError("code_commit must be a full lowercase Git object ID")
    root = _strict_root(workspace_root)
    files = tuple(_hash_regular_file(root, path) for path in _snapshot_paths(root))
    payload = {
        "artifact_schema": CODE_SNAPSHOT_SCHEMA,
        "code_commit": code_commit,
        "file_count": len(files),
        "files": [item.payload for item in files],
    }
    canonical_bytes = canonical_json_bytes(payload)
    return CodeSnapshot(
        code_commit=code_commit,
        files=files,
        canonical_bytes=canonical_bytes,
        sha256=hashlib.sha256(canonical_bytes).hexdigest(),
    )


def dependency_lock_sha256(workspace_root: Path | str) -> str:
    """Return the exact ``uv.lock`` identity after a stable-file read."""

    root = _strict_root(workspace_root)
    return _hash_regular_file(root, root / "uv.lock").sha256


def runtime_environment() -> dict[str, object]:
    """Capture deterministic-computation environment fields without secrets."""

    package_versions: dict[str, str] = {}
    for package in _RUNTIME_PACKAGES:
        try:
            package_versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            package_versions[package] = "NOT_INSTALLED"
    locale_value = locale.getlocale()
    return {
        "artifact_schema": RUNTIME_ENVIRONMENT_SCHEMA,
        "python": {
            "implementation": platform.python_implementation(),
            "version": platform.python_version(),
            "byteorder": sys.byteorder,
        },
        "platform": {
            "machine": platform.machine(),
            "release": platform.release(),
            "system": platform.system(),
        },
        "packages": package_versions,
        "numeric_environment": {key: os.environ.get(key) for key in _NUMERIC_ENVIRONMENT_KEYS},
        "cpu_count": os.cpu_count(),
        "locale": {
            "language": locale_value[0],
            "encoding": locale_value[1],
        },
        "timezone": {
            "daylight": bool(time.daylight),
            "names": list(time.tzname),
            "utc_offset_seconds": -time.timezone,
        },
    }


def _strict_snapshot_directory(data_root: Path | str) -> Path:
    root = _strict_root(data_root)
    derived = root / "derived"
    manifests = derived / "manifests"
    for path, label in ((derived, "derived"), (manifests, "manifests")):
        if path.is_symlink() or not path.is_dir():
            raise ProvenanceError(f"data/{label} must be an existing non-symlink directory")
    directory = manifests / CODE_SNAPSHOT_DIRECTORY
    if directory.is_symlink():
        raise ProvenanceError("code snapshot directory cannot be a symbolic link")
    directory.mkdir(mode=0o755, exist_ok=True)
    if not directory.is_dir():
        raise ProvenanceError("code snapshot output is not a directory")
    return directory.resolve(strict=True)


def publish_code_snapshot(
    snapshot: CodeSnapshot,
    *,
    data_root: Path | str,
) -> PublishedCodeSnapshot:
    """Publish a snapshot below ``data/derived`` with no-overwrite idempotency."""

    if not isinstance(snapshot, CodeSnapshot):
        raise TypeError("snapshot must be a CodeSnapshot")
    if hashlib.sha256(snapshot.canonical_bytes).hexdigest() != snapshot.sha256:
        raise ProvenanceError("snapshot canonical bytes and SHA-256 disagree")
    directory = _strict_snapshot_directory(data_root)
    target = directory / f"sha256={snapshot.sha256}.json"
    if target.is_symlink():
        raise ProvenanceError("code snapshot target cannot be a symbolic link")
    if target.exists():
        if not target.is_file() or target.read_bytes() != snapshot.canonical_bytes:
            raise ProvenanceError("existing immutable code snapshot content drift")
        return PublishedCodeSnapshot(target, snapshot.sha256, "REUSED")

    descriptor, temporary_name = tempfile.mkstemp(
        dir=directory,
        prefix=f".sha256={snapshot.sha256}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(snapshot.canonical_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o444)
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError:
            if target.is_symlink() or not target.is_file():
                raise ProvenanceError("concurrent code snapshot target is unsafe")
            if target.read_bytes() != snapshot.canonical_bytes:
                raise ProvenanceError("concurrent immutable code snapshot content drift")
            disposition = "REUSED"
        else:
            disposition = "CREATED"
        directory_descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        temporary.unlink(missing_ok=True)
    return PublishedCodeSnapshot(target, snapshot.sha256, disposition)
