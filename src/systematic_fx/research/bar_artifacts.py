"""Safe, immutable artifacts for the bar-pattern research campaign.

The artifact path binds two independent identities:

* ``identity_sha256=...`` hashes the declared schema, record count, logical
  identity, and source manifest; and
* ``sha256=...`` hashes the bytes actually stored on disk.

This avoids treating identical bytes produced under different data contracts
as the same research evidence.  Publication never replaces an existing path,
uses an atomic hard-link publish, removes all write bits, and rejects symlinks
anywhere below the project root.
"""

from __future__ import annotations

import hashlib
import os
import re
import secrets
import stat
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import BinaryIO, Final, Literal

import pyarrow as pa
import pyarrow.parquet as pq

from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256

BAR_PATTERN_ARTIFACT_ROOT: Final = Path("data/derived/bar_patterns")
BAR_DATA_ARTIFACT_ROOT: Final = Path("data/derived/trade_bars")
BAR_ARTIFACT_IDENTITY_SCHEMA: Final = "systematic_fx.bar_artifact_identity.v1"

_ARTIFACT_SCHEMA = re.compile(r"systematic_fx\.[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*\.v[1-9][0-9]*")
_CANONICAL_ID = re.compile(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*")
_ARTIFACT_KEY = re.compile(r"[a-z0-9][a-z0-9:_./=-]*")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_SUFFIX = re.compile(r"\.[a-z0-9]+")
_WRITE_BITS: Final = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
_COPY_CHUNK_SIZE: Final = 1024 * 1024

BarArtifactRoot = Literal["bar_patterns", "bars"]


class BarArtifactError(ValueError):
    """An artifact identity, path, or byte stream is unsafe or inconsistent."""


class BarArtifactDriftError(BarArtifactError):
    """An immutable artifact path or its bytes differ from the frozen identity."""


def _canonical_mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise BarArtifactError(f"{label} must be a mapping")
    try:
        detached = canonical_json_bytes(value)
    except (TypeError, ValueError) as error:
        raise BarArtifactError(f"{label} must be strict canonical JSON") from error
    decoded = __import__("json").loads(detached)
    if not isinstance(decoded, dict):  # Mapping always canonicalizes to an object
        raise BarArtifactError(f"{label} must encode an object")
    return MappingProxyType(decoded)


def _canonical_nonempty(value: object, *, label: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise BarArtifactError(f"{label} is not canonical")
    return value


def _sha256(value: object, *, label: str) -> str:
    return _canonical_nonempty(value, label=label, pattern=_SHA256)


def _root_relative(root_kind: BarArtifactRoot) -> Path:
    if root_kind == "bar_patterns":
        return BAR_PATTERN_ARTIFACT_ROOT
    if root_kind == "bars":
        return BAR_DATA_ARTIFACT_ROOT
    raise BarArtifactError("root_kind must be 'bar_patterns' or 'bars'")


@dataclass(frozen=True, slots=True)
class BarArtifactDescriptor:
    """The content-independent contract of one immutable artifact."""

    artifact_key: str
    artifact_type: str
    artifact_schema: str
    artifact_version: int
    record_count: int
    schema_sha256: str
    source_manifest_sha256: str
    logical_identity: Mapping[str, object]
    media_type: str
    file_suffix: str
    root_kind: BarArtifactRoot = "bar_patterns"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "artifact_key",
            _canonical_nonempty(self.artifact_key, label="artifact_key", pattern=_ARTIFACT_KEY),
        )
        object.__setattr__(
            self,
            "artifact_type",
            _canonical_nonempty(
                self.artifact_type,
                label="artifact_type",
                pattern=_CANONICAL_ID,
            ),
        )
        object.__setattr__(
            self,
            "artifact_schema",
            _canonical_nonempty(
                self.artifact_schema,
                label="artifact_schema",
                pattern=_ARTIFACT_SCHEMA,
            ),
        )
        if (
            isinstance(self.artifact_version, bool)
            or not isinstance(self.artifact_version, int)
            or self.artifact_version <= 0
        ):
            raise BarArtifactError("artifact_version must be a positive integer")
        if (
            isinstance(self.record_count, bool)
            or not isinstance(self.record_count, int)
            or self.record_count < 0
        ):
            raise BarArtifactError("record_count must be a non-negative integer")
        object.__setattr__(
            self,
            "schema_sha256",
            _sha256(self.schema_sha256, label="schema_sha256"),
        )
        object.__setattr__(
            self,
            "source_manifest_sha256",
            _sha256(self.source_manifest_sha256, label="source_manifest_sha256"),
        )
        object.__setattr__(
            self,
            "logical_identity",
            _canonical_mapping(self.logical_identity, label="logical_identity"),
        )
        if (
            not isinstance(self.media_type, str)
            or not self.media_type.strip()
            or self.media_type != self.media_type.strip()
        ):
            raise BarArtifactError("media_type must be a canonical non-empty string")
        object.__setattr__(
            self,
            "file_suffix",
            _canonical_nonempty(self.file_suffix, label="file_suffix", pattern=_SUFFIX),
        )
        _root_relative(self.root_kind)

    def identity_document(self) -> dict[str, object]:
        return {
            "artifact_key": self.artifact_key,
            "artifact_schema": self.artifact_schema,
            "artifact_type": self.artifact_type,
            "artifact_version": self.artifact_version,
            "file_suffix": self.file_suffix,
            "identity_schema": BAR_ARTIFACT_IDENTITY_SCHEMA,
            "logical_identity": dict(self.logical_identity),
            "media_type": self.media_type,
            "record_count": self.record_count,
            "root_kind": self.root_kind,
            "schema_sha256": self.schema_sha256,
            "source_manifest_sha256": self.source_manifest_sha256,
        }

    @property
    def identity_sha256(self) -> str:
        return canonical_sha256(self.identity_document())

    @property
    def relative_directory(self) -> Path:
        return (
            _root_relative(self.root_kind)
            / self.artifact_type
            / f"identity_sha256={self.identity_sha256}"
        )


@dataclass(frozen=True, slots=True)
class PublishedBarArtifact:
    """A published inode plus every value needed by ``artifacts`` registration."""

    descriptor: BarArtifactDescriptor
    path: Path
    sha256: str
    byte_size: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "sha256", _sha256(self.sha256, label="artifact sha256"))
        if (
            isinstance(self.byte_size, bool)
            or not isinstance(self.byte_size, int)
            or self.byte_size < 0
        ):
            raise BarArtifactError("byte_size must be a non-negative integer")
        if not isinstance(self.path, Path) or not self.path.is_absolute():
            raise BarArtifactError("published artifact path must be absolute")
        expected_name = f"sha256={self.sha256}{self.descriptor.file_suffix}"
        if self.path.name != expected_name:
            raise BarArtifactError("published artifact filename differs from its SHA-256")

    @property
    def uri(self) -> str:
        return self.path.as_uri()

    def database_metadata(self) -> dict[str, object]:
        """Exact JSONB descriptor used for drift-rejecting DB registration."""

        return {
            **self.descriptor.identity_document(),
            "artifact_identity_sha256": self.descriptor.identity_sha256,
            "content_sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class OpenVerifiedBarArtifact:
    """A stable descriptor held open across a database transaction."""

    descriptor: int
    artifact: PublishedBarArtifact
    inode_identity: tuple[int, int, int, int, int, int]
    parent_descriptor: int


@dataclass(frozen=True, slots=True)
class _OpenDirectoryChain:
    root_path: Path
    relative: Path
    descriptors: tuple[int, ...]
    identities: tuple[tuple[int, int, int], ...]

    @property
    def parent_descriptor(self) -> int:
        return self.descriptors[-1]


def _inode_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _directory_identity(value: os.stat_result) -> tuple[int, int, int]:
    return value.st_dev, value.st_ino, stat.S_IFMT(value.st_mode)


def _project_root(project_root: Path) -> Path:
    if not isinstance(project_root, Path):
        raise BarArtifactError("project_root must be a Path")
    requested = project_root.expanduser()
    if requested.is_symlink():
        raise BarArtifactError("project_root cannot be a symbolic link")
    try:
        resolved = requested.resolve(strict=True)
    except OSError as error:
        raise BarArtifactError("project_root does not exist") from error
    if not resolved.is_dir():
        raise BarArtifactError("project_root must be a directory")
    return resolved


def _validate_relative_path(relative: Path) -> None:
    if relative.is_absolute() or not relative.parts:
        raise BarArtifactError("artifact directory must be a non-empty relative path")
    if any(part in {"", ".", ".."} for part in relative.parts):
        raise BarArtifactError("artifact directory contains an unsafe path component")


def _verify_directory_chain(chain: _OpenDirectoryChain) -> None:
    """Re-bind every held directory inode through its still-open parent."""

    try:
        root_details = chain.root_path.lstat()
    except OSError as error:
        raise BarArtifactDriftError("project root path disappeared") from error
    if _directory_identity(root_details) != chain.identities[0]:
        raise BarArtifactDriftError("project root path no longer names the held inode")
    for index, part in enumerate(chain.relative.parts, start=1):
        held = os.fstat(chain.descriptors[index])
        if _directory_identity(held) != chain.identities[index]:
            raise BarArtifactDriftError("held artifact directory inode changed")
        try:
            bound = os.stat(
                part,
                dir_fd=chain.descriptors[index - 1],
                follow_symlinks=False,
            )
        except OSError as error:
            raise BarArtifactDriftError("artifact ancestor path was replaced") from error
        if _directory_identity(bound) != chain.identities[index]:
            raise BarArtifactDriftError("artifact ancestor no longer names the held inode")


@contextmanager
def _open_directory_chain(
    root: Path,
    relative: Path,
    *,
    create: bool,
) -> Iterator[_OpenDirectoryChain]:
    """Open root-to-leaf dirfds with ``O_NOFOLLOW`` and retain every inode."""

    _validate_relative_path(relative)
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptors: list[int] = []
    identities: list[tuple[int, int, int]] = []
    try:
        try:
            root_descriptor = os.open(root, flags)
        except OSError as error:
            raise BarArtifactError("cannot open project root safely") from error
        descriptors.append(root_descriptor)
        root_details = os.fstat(root_descriptor)
        if not stat.S_ISDIR(root_details.st_mode):
            raise BarArtifactError("project_root is not a directory")
        identities.append(_directory_identity(root_details))
        for part in relative.parts:
            try:
                descriptor = os.open(part, flags, dir_fd=descriptors[-1])
            except FileNotFoundError:
                if not create:
                    raise BarArtifactDriftError(
                        f"artifact directory is missing: {relative}"
                    ) from None
                try:
                    os.mkdir(part, mode=0o755, dir_fd=descriptors[-1])
                except FileExistsError:
                    pass
                try:
                    descriptor = os.open(part, flags, dir_fd=descriptors[-1])
                except OSError as error:
                    raise BarArtifactError("cannot open created artifact directory") from error
            except OSError as error:
                raise BarArtifactError("artifact directory is unsafe or inaccessible") from error
            details = os.fstat(descriptor)
            if not stat.S_ISDIR(details.st_mode):
                os.close(descriptor)
                raise BarArtifactError("artifact path component is not a directory")
            descriptors.append(descriptor)
            identities.append(_directory_identity(details))
        chain = _OpenDirectoryChain(root, relative, tuple(descriptors), tuple(identities))
        _verify_directory_chain(chain)
        yield chain
        _verify_directory_chain(chain)
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _hash_descriptor(descriptor: int) -> tuple[str, int]:
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    byte_size = 0
    while chunk := os.read(descriptor, _COPY_CHUNK_SIZE):
        digest.update(chunk)
        byte_size += len(chunk)
    os.lseek(descriptor, 0, os.SEEK_SET)
    return digest.hexdigest(), byte_size


def arrow_schema_sha256(schema: pa.Schema) -> str:
    """Hash the lossless serialized Arrow schema, including field metadata."""

    if not isinstance(schema, pa.Schema):
        raise BarArtifactError("schema must be a pyarrow.Schema")
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def _verify_read_only_regular(details: os.stat_result, *, label: str) -> None:
    if not stat.S_ISREG(details.st_mode):
        raise BarArtifactDriftError(f"{label} is not a regular file")
    if details.st_mode & _WRITE_BITS:
        raise BarArtifactDriftError(f"{label} is not immutable (write bits are set)")


def _expected_path(root: Path, artifact: PublishedBarArtifact) -> Path:
    expected = (
        root
        / artifact.descriptor.relative_directory
        / f"sha256={artifact.sha256}{artifact.descriptor.file_suffix}"
    )
    if artifact.path != expected:
        raise BarArtifactDriftError("artifact path differs from its canonical identity path")
    return expected


def _verify_open_binding(
    opened: OpenVerifiedBarArtifact,
    chain: _OpenDirectoryChain,
) -> None:
    _verify_directory_chain(chain)
    current = os.fstat(opened.descriptor)
    if _inode_identity(current) != opened.inode_identity:
        raise BarArtifactDriftError("open artifact inode changed during verification")
    try:
        path_details = os.stat(
            opened.artifact.path.name,
            dir_fd=opened.parent_descriptor,
            follow_symlinks=False,
        )
    except OSError as error:
        raise BarArtifactDriftError("artifact path disappeared during verification") from error
    if _inode_identity(path_details) != opened.inode_identity:
        raise BarArtifactDriftError("artifact path no longer names the open immutable inode")


@contextmanager
def open_verified_bar_artifact(
    project_root: Path,
    artifact: PublishedBarArtifact,
) -> Iterator[OpenVerifiedBarArtifact]:
    """Hold a verified non-symlink inode open across a caller's transaction."""

    if not isinstance(artifact, PublishedBarArtifact):
        raise BarArtifactError("artifact must be a PublishedBarArtifact")
    root = _project_root(project_root)
    _expected_path(root, artifact)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    with _open_directory_chain(
        root,
        artifact.descriptor.relative_directory,
        create=False,
    ) as chain:
        try:
            descriptor = os.open(
                artifact.path.name,
                flags,
                dir_fd=chain.parent_descriptor,
            )
        except OSError as error:
            raise BarArtifactDriftError("cannot open immutable artifact safely") from error
        try:
            details = os.fstat(descriptor)
            _verify_read_only_regular(details, label="artifact")
            identity = _inode_identity(details)
            observed_sha256, observed_size = _hash_descriptor(descriptor)
            if observed_sha256 != artifact.sha256 or observed_size != artifact.byte_size:
                raise BarArtifactDriftError("artifact content differs from its published identity")
            opened = OpenVerifiedBarArtifact(
                descriptor,
                artifact,
                identity,
                chain.parent_descriptor,
            )
            _verify_open_binding(opened, chain)
            yield opened
            _verify_open_binding(opened, chain)
        finally:
            os.close(descriptor)


def verify_published_bar_artifact(
    project_root: Path,
    artifact: PublishedBarArtifact,
) -> None:
    """Re-hash and re-bind a published artifact without changing it."""

    with open_verified_bar_artifact(project_root, artifact):
        return


def _open_new_temporary(parent_descriptor: int) -> tuple[int, str]:
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    for _ in range(128):
        name = f".publish-{secrets.token_hex(16)}.tmp"
        try:
            return os.open(name, flags, 0o600, dir_fd=parent_descriptor), name
        except FileExistsError:
            continue
        except OSError as error:
            raise BarArtifactError("cannot create a safe staged artifact") from error
    raise BarArtifactError("cannot allocate a unique staged artifact name")


def _verify_named_artifact(
    chain: _OpenDirectoryChain,
    *,
    name: str,
    expected_sha256: str,
    expected_size: int,
) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(name, flags, dir_fd=chain.parent_descriptor)
    except OSError as error:
        raise BarArtifactDriftError("cannot open published artifact through held dirfd") from error
    try:
        details = os.fstat(descriptor)
        _verify_read_only_regular(details, label="published artifact")
        identity = _inode_identity(details)
        observed_sha256, observed_size = _hash_descriptor(descriptor)
        if observed_sha256 != expected_sha256 or observed_size != expected_size:
            raise BarArtifactDriftError("existing content address contains different bytes")
        bound = os.stat(name, dir_fd=chain.parent_descriptor, follow_symlinks=False)
        if _inode_identity(bound) != identity:
            raise BarArtifactDriftError("published artifact leaf was replaced")
        _verify_directory_chain(chain)
    finally:
        os.close(descriptor)


def _publish_from_descriptor(
    *,
    project_root: Path,
    descriptor: BarArtifactDescriptor,
    source_descriptor: int,
    source_identity: tuple[int, int, int, int, int, int] | None,
) -> PublishedBarArtifact:
    root = _project_root(project_root)
    source_sha256, source_size = _hash_descriptor(source_descriptor)
    destination_name = f"sha256={source_sha256}{descriptor.file_suffix}"
    with _open_directory_chain(
        root,
        descriptor.relative_directory,
        create=True,
    ) as chain:
        temporary_descriptor, temporary_name = _open_new_temporary(chain.parent_descriptor)
        try:
            copied_digest = hashlib.sha256()
            copied_size = 0
            os.lseek(source_descriptor, 0, os.SEEK_SET)
            while chunk := os.read(source_descriptor, _COPY_CHUNK_SIZE):
                view = memoryview(chunk)
                while view:
                    written = os.write(temporary_descriptor, view)
                    view = view[written:]
                copied_digest.update(chunk)
                copied_size += len(chunk)
            os.fsync(temporary_descriptor)
            if source_identity is not None and (
                _inode_identity(os.fstat(source_descriptor)) != source_identity
            ):
                raise BarArtifactDriftError("source artifact changed while it was copied")
            if copied_digest.hexdigest() != source_sha256 or copied_size != source_size:
                raise BarArtifactDriftError("published copy differs from its source bytes")
            os.fchmod(temporary_descriptor, 0o444)
            os.close(temporary_descriptor)
            temporary_descriptor = -1
            try:
                os.link(
                    temporary_name,
                    destination_name,
                    src_dir_fd=chain.parent_descriptor,
                    dst_dir_fd=chain.parent_descriptor,
                    follow_symlinks=False,
                )
            except FileExistsError:
                pass
            os.fsync(chain.parent_descriptor)
            _verify_named_artifact(
                chain,
                name=destination_name,
                expected_sha256=source_sha256,
                expected_size=source_size,
            )
        finally:
            if temporary_descriptor >= 0:
                os.close(temporary_descriptor)
            try:
                os.unlink(temporary_name, dir_fd=chain.parent_descriptor)
            except FileNotFoundError:
                pass
            os.fsync(chain.parent_descriptor)

    destination = root / descriptor.relative_directory / destination_name
    artifact = PublishedBarArtifact(
        descriptor=descriptor,
        path=destination,
        sha256=source_sha256,
        byte_size=source_size,
    )
    verify_published_bar_artifact(root, artifact)
    return artifact


def publish_bar_artifact_bytes(
    project_root: Path,
    descriptor: BarArtifactDescriptor,
    content: bytes,
) -> PublishedBarArtifact:
    """Atomically publish in-memory bytes under the descriptor's safe root."""

    if not isinstance(descriptor, BarArtifactDescriptor):
        raise BarArtifactError("descriptor must be a BarArtifactDescriptor")
    if not isinstance(content, bytes):
        raise BarArtifactError("content must be bytes")
    temporary_descriptor, temporary_name = tempfile.mkstemp(prefix="bar-artifact-source-")
    temporary_path = Path(temporary_name)
    try:
        view = memoryview(content)
        while view:
            written = os.write(temporary_descriptor, view)
            view = view[written:]
        os.fsync(temporary_descriptor)
        return _publish_from_descriptor(
            project_root=project_root,
            descriptor=descriptor,
            source_descriptor=temporary_descriptor,
            source_identity=None,
        )
    finally:
        os.close(temporary_descriptor)
        temporary_path.unlink(missing_ok=True)


def publish_bar_json_artifact(
    project_root: Path,
    descriptor: BarArtifactDescriptor,
    document: Mapping[str, object] | Sequence[object],
) -> PublishedBarArtifact:
    """Canonicalize strict JSON and publish it as immutable research evidence."""

    if descriptor.media_type != "application/json" or descriptor.file_suffix != ".json":
        raise BarArtifactError("JSON artifacts require application/json and .json")
    if isinstance(document, Mapping) and document.get("schema") != descriptor.artifact_schema:
        raise BarArtifactError("JSON document schema differs from its artifact descriptor")
    try:
        content = canonical_json_bytes(document)
    except (TypeError, ValueError) as error:
        raise BarArtifactError("JSON artifact document must be strict canonical JSON") from error
    return publish_bar_artifact_bytes(project_root, descriptor, content)


def publish_bar_parquet_table(
    project_root: Path,
    descriptor: BarArtifactDescriptor,
    table: pa.Table,
    *,
    compression: str = "zstd",
    use_dictionary: bool = False,
    write_statistics: bool = True,
    version: str = "2.6",
    row_group_size: int = 4_096,
) -> PublishedBarArtifact:
    """Write and publish Parquet without ever reopening a temporary pathname.

    One anonymous held temporary descriptor is used for the Arrow writer,
    fsync, footer/schema verification, content hashing, and immutable publish.
    This prevents a same-user pathname swap from substituting different rows
    between ``write_table`` and publication.
    """

    if not isinstance(descriptor, BarArtifactDescriptor):
        raise BarArtifactError("descriptor must be a BarArtifactDescriptor")
    if descriptor.media_type != "application/vnd.apache.parquet" or (
        descriptor.file_suffix != ".parquet"
    ):
        raise BarArtifactError("Parquet artifacts require the Parquet media type and .parquet")
    if not isinstance(table, pa.Table):
        raise BarArtifactError("table must be a pyarrow.Table")
    if table.num_rows != descriptor.record_count:
        raise BarArtifactError("table row count differs from the artifact identity")
    if arrow_schema_sha256(table.schema) != descriptor.schema_sha256:
        raise BarArtifactError("table schema differs from the artifact identity")
    try:
        with tempfile.TemporaryFile(prefix="bar-parquet-source-") as staged:
            pq.write_table(
                table,
                staged,
                compression=compression,
                use_dictionary=use_dictionary,
                write_statistics=write_statistics,
                version=version,
                row_group_size=row_group_size,
            )
            staged.flush()
            os.fsync(staged.fileno())
            staged.seek(0)
            parquet = pq.ParquetFile(staged)
            if (
                parquet.metadata.num_rows != descriptor.record_count
                or parquet.schema_arrow != table.schema
                or arrow_schema_sha256(parquet.schema_arrow) != descriptor.schema_sha256
            ):
                raise BarArtifactError("staged Parquet differs from the artifact identity")
            staged.seek(0)
            details = os.fstat(staged.fileno())
            return _publish_from_descriptor(
                project_root=project_root,
                descriptor=descriptor,
                source_descriptor=staged.fileno(),
                source_identity=_inode_identity(details),
            )
    except (OSError, pa.ArrowException) as error:
        raise BarArtifactError("cannot write or verify staged Parquet") from error


def publish_bar_artifact_open_file(
    project_root: Path,
    descriptor: BarArtifactDescriptor,
    source: BinaryIO,
) -> PublishedBarArtifact:
    """Publish from one caller-held regular file descriptor without path reopen.

    The caller retains ownership of ``source``.  Its exact inode is held across
    flush, fsync, hashing, copy, and immutable publication, closing the
    pathname-swap window that a large streamed artifact would otherwise have
    between staging and :func:`publish_bar_artifact_file`.
    """

    if not isinstance(descriptor, BarArtifactDescriptor):
        raise BarArtifactError("descriptor must be a BarArtifactDescriptor")
    try:
        source_descriptor = source.fileno()
        source.flush()
        os.fsync(source_descriptor)
        details = os.fstat(source_descriptor)
    except (AttributeError, OSError, ValueError) as error:
        raise BarArtifactError("source must expose one open, flushable file descriptor") from error
    if not stat.S_ISREG(details.st_mode):
        raise BarArtifactError("source descriptor must name a regular file")
    try:
        return _publish_from_descriptor(
            project_root=project_root,
            descriptor=descriptor,
            source_descriptor=source_descriptor,
            source_identity=_inode_identity(details),
        )
    except OSError as error:
        raise BarArtifactError("cannot publish the held source descriptor") from error


def publish_bar_artifact_file(
    project_root: Path,
    descriptor: BarArtifactDescriptor,
    source_path: Path,
) -> PublishedBarArtifact:
    """Stream a stable regular source file into an immutable content address."""

    if not isinstance(source_path, Path) or not source_path.is_absolute():
        raise BarArtifactError("source_path must be absolute")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        source_descriptor = os.open(source_path, flags)
    except OSError as error:
        raise BarArtifactError("cannot open source artifact safely") from error
    try:
        details = os.fstat(source_descriptor)
        if not stat.S_ISREG(details.st_mode):
            raise BarArtifactError("source artifact must be a regular file")
        if descriptor.file_suffix == ".parquet":
            if descriptor.media_type != "application/vnd.apache.parquet":
                raise BarArtifactError("Parquet artifacts require the Parquet media type")
            try:
                with os.fdopen(os.dup(source_descriptor), "rb") as source:
                    parquet = pq.ParquetFile(source)
                    observed_rows = parquet.metadata.num_rows
                    observed_schema_sha256 = arrow_schema_sha256(parquet.schema_arrow)
            except (OSError, pa.ArrowException) as error:
                raise BarArtifactError("source artifact is not readable Parquet") from error
            finally:
                os.lseek(source_descriptor, 0, os.SEEK_SET)
            if observed_rows != descriptor.record_count:
                raise BarArtifactError("Parquet row count differs from the artifact identity")
            if observed_schema_sha256 != descriptor.schema_sha256:
                raise BarArtifactError("Parquet schema differs from the artifact identity")
        return _publish_from_descriptor(
            project_root=project_root,
            descriptor=descriptor,
            source_descriptor=source_descriptor,
            source_identity=_inode_identity(details),
        )
    finally:
        os.close(source_descriptor)
