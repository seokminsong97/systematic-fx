"""Resumable full-content SHA-256 manifests for immutable MBP-10 sources.

The final manifest is deliberately small and database-shaped.  The separate
checkpoint retains filesystem identity fields so a resumed run never trusts a
digest for a source that changed since it was hashed.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from itertools import pairwise
from pathlib import Path, PurePosixPath
from typing import Literal

DEFAULT_CHUNK_SIZE_BYTES = 8 * 1024 * 1024
DEFAULT_MANIFEST_NAME = "mbp10_source_sha256_v1.jsonl"

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_SOURCE_URI_PATTERN = re.compile(
    r"(?P<year>[0-9]{4})/(?P<month>[0-9]{2})/(?P<day>[0-9]{2})/"
    r"glbx-mdp3-(?P<stamp>[0-9]{8})\.mbp-10\.parquet"
)
_CHECKPOINT_KEYS = frozenset(
    {
        "byte_size",
        "relative_uri",
        "sha256",
        "source_ctime_ns",
        "source_date",
        "source_device",
        "source_inode",
        "source_mtime_ns",
    }
)


class HashManifestError(ValueError):
    """Raised when an input, checkpoint, or source identity is unsafe."""


@dataclass(frozen=True)
class HashProgress:
    """One deterministic progress event emitted by the manifest builder."""

    status: Literal["RESUMED", "HASHED", "COMPLETE"]
    file_index: int
    file_count: int
    relative_uri: str | None
    file_bytes: int
    bytes_processed: int
    total_bytes: int

    def as_dict(self) -> dict[str, object]:
        return {
            "bytes_processed": self.bytes_processed,
            "file_bytes": self.file_bytes,
            "file_count": self.file_count,
            "file_index": self.file_index,
            "relative_uri": self.relative_uri,
            "status": self.status,
            "total_bytes": self.total_bytes,
        }


ProgressCallback = Callable[[HashProgress], None]


@dataclass(frozen=True)
class HashManifestReport:
    """Identity and coverage report for a completed source manifest."""

    data_root: Path
    dataset_root: Path
    manifest_path: Path
    checkpoint_path: Path
    manifest_sha256: str
    manifest_byte_size: int
    file_count: int
    total_source_bytes: int
    resumed_file_count: int
    hashed_file_count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "checkpoint_path": self.checkpoint_path.as_posix(),
            "data_root": self.data_root.as_posix(),
            "dataset_root": self.dataset_root.as_posix(),
            "file_count": self.file_count,
            "hashed_file_count": self.hashed_file_count,
            "manifest_byte_size": self.manifest_byte_size,
            "manifest_path": self.manifest_path.as_posix(),
            "manifest_sha256": self.manifest_sha256,
            "resumed_file_count": self.resumed_file_count,
            "total_source_bytes": self.total_source_bytes,
        }


@dataclass(frozen=True)
class _Source:
    path: Path
    relative_uri: str
    source_date: date
    byte_size: int


@dataclass(frozen=True)
class _Identity:
    byte_size: int
    mtime_ns: int
    ctime_ns: int
    device: int
    inode: int


def _canonical_line(record: Mapping[str, object]) -> bytes:
    return (
        json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _strict_root(path: Path | str, *, label: str) -> Path:
    expanded = Path(path).expanduser()
    if expanded.is_symlink():
        raise HashManifestError(f"{label} cannot be a symbolic link: {expanded}")
    try:
        resolved = expanded.resolve(strict=True)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"{label} does not exist: {expanded}") from exc
    if not resolved.is_dir():
        raise NotADirectoryError(f"{label} is not a directory: {resolved}")
    return resolved


def _relative_to(path: Path, root: Path, *, label: str) -> Path:
    try:
        return path.relative_to(root)
    except ValueError as exc:
        raise HashManifestError(f"{label} must remain inside data_root: {path}") from exc


def _assert_no_symlink_components(root: Path, relative_path: Path, *, label: str) -> None:
    current = root
    for part in relative_path.parts:
        current /= part
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"{label} does not exist: {current}") from exc
        if stat.S_ISLNK(mode):
            raise HashManifestError(f"{label} cannot traverse a symbolic link: {current}")


def _resolve_dataset_root(data_root: Path, dataset_root: Path | str | None) -> Path:
    requested = (
        Path(dataset_root).expanduser() if dataset_root is not None else data_root / "mbp-10"
    )
    if requested.is_symlink():
        raise HashManifestError(f"dataset_root cannot be a symbolic link: {requested}")
    resolved = requested.resolve(strict=True)
    relative = _relative_to(resolved, data_root, label="dataset_root")
    if not relative.parts or relative.parts[0] == "derived":
        raise HashManifestError("dataset_root must be a raw-data child outside data/derived")
    _assert_no_symlink_components(data_root, relative, label="dataset_root")
    if not resolved.is_dir():
        raise NotADirectoryError(f"dataset_root is not a directory: {resolved}")
    return resolved


def _ensure_manifest_directory(data_root: Path) -> Path:
    current = data_root
    for part in ("derived", "manifests"):
        current /= part
        if current.exists() or current.is_symlink():
            if current.is_symlink():
                raise HashManifestError(f"manifest directory cannot be a symbolic link: {current}")
            if not current.is_dir():
                raise HashManifestError(
                    f"manifest directory component is not a directory: {current}"
                )
        else:
            current.mkdir(mode=0o700)
    resolved = current.resolve(strict=True)
    _relative_to(resolved, data_root, label="manifest directory")
    return resolved


def _safe_output_path(directory: Path, name: str, *, label: str) -> Path:
    if not isinstance(name, str) or not name or Path(name).name != name or name in {".", ".."}:
        raise HashManifestError(f"{label} must be one filename without path components")
    if not name.endswith(".jsonl"):
        raise HashManifestError(f"{label} must end in .jsonl")
    path = directory / name
    if path.is_symlink():
        raise HashManifestError(f"{label} cannot be a symbolic link: {path}")
    return path


def _checkpoint_name(manifest_name: str) -> str:
    return f"{manifest_name.removesuffix('.jsonl')}.checkpoint.jsonl"


def _parse_source_date(relative_uri: str) -> date:
    match = _SOURCE_URI_PATTERN.fullmatch(relative_uri)
    if match is None:
        raise HashManifestError(f"invalid MBP-10 relative URI: {relative_uri!r}")
    try:
        parsed = date(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
        )
    except ValueError as exc:
        raise HashManifestError(f"invalid source date in relative URI: {relative_uri!r}") from exc
    if parsed.strftime("%Y%m%d") != match.group("stamp"):
        raise HashManifestError(f"partition and filename dates disagree: {relative_uri!r}")
    return parsed


def _validate_relative_uri(value: object) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise HashManifestError("relative URI must be a non-empty POSIX path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise HashManifestError(f"unsafe relative URI: {value!r}")
    if path.as_posix() != value:
        raise HashManifestError(f"relative URI is not canonical: {value!r}")
    _parse_source_date(value)
    return value


def _source_from_uri(
    dataset_root: Path,
    relative_uri: str,
    *,
    expected_size: int | None,
    expected_date: date | None,
) -> _Source:
    uri = _validate_relative_uri(relative_uri)
    uri_date = _parse_source_date(uri)
    if expected_date is not None and expected_date != uri_date:
        raise HashManifestError(
            f"source date disagrees with relative URI for {uri}: "
            f"{expected_date.isoformat()} != {uri_date.isoformat()}"
        )

    path = dataset_root.joinpath(*PurePosixPath(uri).parts)
    _assert_no_symlink_components(dataset_root, Path(*PurePosixPath(uri).parts), label="source")
    resolved = path.resolve(strict=True)
    _relative_to(resolved, dataset_root, label="source")
    mode = resolved.lstat().st_mode
    if not stat.S_ISREG(mode):
        raise HashManifestError(f"source is not a regular file: {resolved}")
    byte_size = resolved.stat().st_size
    if expected_size is not None and byte_size != expected_size:
        raise HashManifestError(
            f"source size drift for {uri}: footer={expected_size}, filesystem={byte_size}"
        )
    return _Source(
        path=resolved,
        relative_uri=uri,
        source_date=uri_date,
        byte_size=byte_size,
    )


def _iter_dataset_uris(dataset_root: Path) -> Iterator[str]:
    for directory, directory_names, file_names in os.walk(dataset_root, followlinks=False):
        current = Path(directory)
        for directory_name in directory_names:
            candidate = current / directory_name
            if candidate.is_symlink():
                raise HashManifestError(
                    f"dataset cannot contain a symbolic-link directory: {candidate}"
                )
        directory_names.sort()
        for file_name in sorted(file_names):
            if not file_name.endswith(".parquet"):
                continue
            candidate = current / file_name
            if candidate.is_symlink():
                raise HashManifestError(f"source cannot be a symbolic link: {candidate}")
            yield candidate.relative_to(dataset_root).as_posix()


def _sources_from_dataset(dataset_root: Path) -> list[_Source]:
    return [
        _source_from_uri(dataset_root, uri, expected_size=None, expected_date=None)
        for uri in _iter_dataset_uris(dataset_root)
    ]


def _required_nonnegative_int(value: object, *, field: str, line_number: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise HashManifestError(
            f"footer manifest line {line_number} field {field!r} must be a non-negative integer"
        )
    return value


def _sources_from_footer(dataset_root: Path, footer_manifest: Path | str) -> list[_Source]:
    manifest = Path(footer_manifest).expanduser()
    if manifest.is_symlink():
        raise HashManifestError(f"footer manifest cannot be a symbolic link: {manifest}")
    manifest = manifest.resolve(strict=True)
    if not manifest.is_file():
        raise HashManifestError(f"footer manifest is not a regular file: {manifest}")

    sources: list[_Source] = []
    with manifest.open("r", encoding="utf-8", newline="") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise HashManifestError(
                    f"invalid JSON on footer manifest line {line_number}"
                ) from exc
            if not isinstance(record, dict):
                raise HashManifestError(
                    f"footer manifest line {line_number} must contain a JSON object"
                )
            relative_uri = record.get("path", record.get("relative_uri"))
            byte_size_value = record.get("file_size_bytes", record.get("byte_size"))
            byte_size = _required_nonnegative_int(
                byte_size_value,
                field="file_size_bytes",
                line_number=line_number,
            )
            source_date_value = record.get("source_date")
            if not isinstance(source_date_value, str):
                raise HashManifestError(
                    f"footer manifest line {line_number} source_date must be an ISO date"
                )
            try:
                source_date = date.fromisoformat(source_date_value)
            except ValueError as exc:
                raise HashManifestError(
                    f"invalid source_date on footer manifest line {line_number}"
                ) from exc
            sources.append(
                _source_from_uri(
                    dataset_root,
                    _validate_relative_uri(relative_uri),
                    expected_size=byte_size,
                    expected_date=source_date,
                )
            )
    return sources


def _validate_sources(sources: list[_Source]) -> list[_Source]:
    sources.sort(key=lambda source: source.relative_uri)
    if not sources:
        raise HashManifestError("no MBP-10 Parquet sources were found")
    for previous, current in pairwise(sources):
        if previous.relative_uri == current.relative_uri:
            raise HashManifestError(f"duplicate source relative URI: {current.relative_uri}")
    return sources


def _identity(file_stat: os.stat_result) -> _Identity:
    return _Identity(
        byte_size=file_stat.st_size,
        mtime_ns=file_stat.st_mtime_ns,
        ctime_ns=file_stat.st_ctime_ns,
        device=file_stat.st_dev,
        inode=file_stat.st_ino,
    )


def _checkpoint_record(source: _Source, digest: str, identity: _Identity) -> dict[str, object]:
    return {
        "byte_size": source.byte_size,
        "relative_uri": source.relative_uri,
        "sha256": digest,
        "source_ctime_ns": identity.ctime_ns,
        "source_date": source.source_date.isoformat(),
        "source_device": identity.device,
        "source_inode": identity.inode,
        "source_mtime_ns": identity.mtime_ns,
    }


def _final_record(record: Mapping[str, object]) -> dict[str, object]:
    return {
        "byte_size": record["byte_size"],
        "relative_uri": record["relative_uri"],
        "sha256": record["sha256"],
        "source_date": record["source_date"],
    }


def _atomic_write(path: Path, records: Iterable[Mapping[str, object]]) -> tuple[str, int]:
    if path.is_symlink():
        raise HashManifestError(f"refusing to replace symbolic-link output: {path}")
    handle = tempfile.NamedTemporaryFile(  # noqa: SIM115
        mode="w+b",
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        delete=False,
    )
    temporary_path = Path(handle.name)
    digest = hashlib.sha256()
    byte_size = 0
    try:
        for record in records:
            line = _canonical_line(record)
            handle.write(line)
            digest.update(line)
            byte_size += len(line)
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        os.replace(temporary_path, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        handle.close()
        temporary_path.unlink(missing_ok=True)
        raise
    return digest.hexdigest(), byte_size


def _load_checkpoint(path: Path, sources: Sequence[_Source]) -> list[dict[str, object]]:
    if not path.exists():
        return []
    if path.is_symlink() or not path.is_file():
        raise HashManifestError(f"checkpoint must be a regular file: {path}")

    records: list[dict[str, object]] = []
    with path.open("rb") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            try:
                record = json.loads(raw_line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise HashManifestError(f"invalid checkpoint JSON on line {line_number}") from exc
            if not isinstance(record, dict) or set(record) != _CHECKPOINT_KEYS:
                raise HashManifestError(f"invalid checkpoint record fields on line {line_number}")
            if raw_line != _canonical_line(record):
                raise HashManifestError(f"checkpoint line {line_number} is not canonical JSONL")
            records.append(record)

    if len(records) > len(sources):
        raise HashManifestError("checkpoint contains more entries than the current source set")

    seen: set[str] = set()
    for index, record in enumerate(records):
        source = sources[index]
        uri = record["relative_uri"]
        if not isinstance(uri, str) or uri in seen:
            raise HashManifestError(f"duplicate or invalid checkpoint URI on line {index + 1}")
        seen.add(uri)
        if uri != source.relative_uri:
            raise HashManifestError(
                f"checkpoint path-order drift at line {index + 1}: {uri!r} != "
                f"{source.relative_uri!r}"
            )
        if record["byte_size"] != source.byte_size:
            raise HashManifestError(f"checkpoint size drift for {source.relative_uri}")
        if record["source_date"] != source.source_date.isoformat():
            raise HashManifestError(f"checkpoint date drift for {source.relative_uri}")
        digest = record["sha256"]
        if not isinstance(digest, str) or _SHA256_PATTERN.fullmatch(digest) is None:
            raise HashManifestError(f"invalid checkpoint SHA-256 for {source.relative_uri}")

        current = _identity(source.path.stat())
        expected_values = (
            record["byte_size"],
            record["source_mtime_ns"],
            record["source_ctime_ns"],
            record["source_device"],
            record["source_inode"],
        )
        if any(isinstance(value, bool) or not isinstance(value, int) for value in expected_values):
            raise HashManifestError(f"invalid checkpoint identity for {source.relative_uri}")
        expected = _Identity(
            byte_size=record["byte_size"],
            mtime_ns=record["source_mtime_ns"],
            ctime_ns=record["source_ctime_ns"],
            device=record["source_device"],
            inode=record["source_inode"],
        )
        if current != expected:
            raise HashManifestError(f"checkpoint identity drift for {source.relative_uri}")
    return records


def _hash_source(source: _Source, chunk_size_bytes: int) -> tuple[str, _Identity]:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(source.path, flags)
    digest = hashlib.sha256()
    try:
        before = _identity(os.fstat(descriptor))
        if before.byte_size != source.byte_size:
            raise HashManifestError(f"source size drift before hashing: {source.relative_uri}")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            while chunk := handle.read(chunk_size_bytes):
                digest.update(chunk)
        after = _identity(os.fstat(descriptor))
    finally:
        os.close(descriptor)

    if before != after:
        raise HashManifestError(f"source changed while hashing: {source.relative_uri}")
    if source.path.is_symlink() or _identity(source.path.stat()) != after:
        raise HashManifestError(f"source path changed while hashing: {source.relative_uri}")
    return digest.hexdigest(), after


def build_sha256_manifest(
    data_root: Path | str,
    *,
    dataset_root: Path | str | None = None,
    footer_manifest: Path | str | None = None,
    manifest_name: str = DEFAULT_MANIFEST_NAME,
    checkpoint_name: str | None = None,
    chunk_size_bytes: int = DEFAULT_CHUNK_SIZE_BYTES,
    progress_callback: ProgressCallback | None = None,
) -> HashManifestReport:
    """Build a deterministic, resumable full-content manifest under ``data/derived``.

    ``footer_manifest`` is an optional inventory input, not an output location.
    Its ``path``, ``file_size_bytes``, and ``source_date`` fields are verified
    against the raw filesystem.  Without it, daily Parquet files are discovered
    in sorted path order below ``dataset_root``.
    """

    if (
        isinstance(chunk_size_bytes, bool)
        or not isinstance(chunk_size_bytes, int)
        or chunk_size_bytes <= 0
    ):
        raise ValueError("chunk_size_bytes must be a positive integer")

    root = _strict_root(data_root, label="data_root")
    source_root = _resolve_dataset_root(root, dataset_root)
    manifest_directory = _ensure_manifest_directory(root)
    final_path = _safe_output_path(manifest_directory, manifest_name, label="manifest_name")
    checkpoint_path = _safe_output_path(
        manifest_directory,
        checkpoint_name or _checkpoint_name(manifest_name),
        label="checkpoint_name",
    )
    if checkpoint_path == final_path:
        raise HashManifestError("checkpoint and final manifest paths must differ")

    if footer_manifest is None:
        sources = _validate_sources(_sources_from_dataset(source_root))
    else:
        sources = _validate_sources(_sources_from_footer(source_root, footer_manifest))

    checkpoint_records = _load_checkpoint(checkpoint_path, sources)
    resumed_count = len(checkpoint_records)
    total_bytes = sum(source.byte_size for source in sources)
    bytes_processed = sum(source.byte_size for source in sources[:resumed_count])

    if progress_callback is not None:
        for index, source in enumerate(sources[:resumed_count], start=1):
            progress_callback(
                HashProgress(
                    status="RESUMED",
                    file_index=index,
                    file_count=len(sources),
                    relative_uri=source.relative_uri,
                    file_bytes=source.byte_size,
                    bytes_processed=sum(item.byte_size for item in sources[:index]),
                    total_bytes=total_bytes,
                )
            )

    for index in range(resumed_count, len(sources)):
        source = sources[index]
        digest, identity = _hash_source(source, chunk_size_bytes)
        checkpoint_records.append(_checkpoint_record(source, digest, identity))
        _atomic_write(checkpoint_path, checkpoint_records)
        bytes_processed += source.byte_size
        if progress_callback is not None:
            progress_callback(
                HashProgress(
                    status="HASHED",
                    file_index=index + 1,
                    file_count=len(sources),
                    relative_uri=source.relative_uri,
                    file_bytes=source.byte_size,
                    bytes_processed=bytes_processed,
                    total_bytes=total_bytes,
                )
            )

    manifest_sha256, manifest_byte_size = _atomic_write(
        final_path,
        (_final_record(record) for record in checkpoint_records),
    )
    if progress_callback is not None:
        progress_callback(
            HashProgress(
                status="COMPLETE",
                file_index=len(sources),
                file_count=len(sources),
                relative_uri=None,
                file_bytes=0,
                bytes_processed=total_bytes,
                total_bytes=total_bytes,
            )
        )

    return HashManifestReport(
        data_root=root,
        dataset_root=source_root,
        manifest_path=final_path,
        checkpoint_path=checkpoint_path,
        manifest_sha256=manifest_sha256,
        manifest_byte_size=manifest_byte_size,
        file_count=len(sources),
        total_source_bytes=total_bytes,
        resumed_file_count=resumed_count,
        hashed_file_count=len(sources) - resumed_count,
    )
