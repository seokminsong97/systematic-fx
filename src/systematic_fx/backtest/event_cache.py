"""Content-addressed daily executable-quote caches for shared replay.

The raw MBP-10 source is expensive to decode and contains many instruments.
This module performs that work once for each ``(source_date, raw_symbol)`` pair,
retains exact source-row ordering and invalid-book observations, and publishes
the normalized result below ``data/derived``.  Strategy signals, barrier cells,
and stress scenarios must all consume the same cache rather than reopening the
raw source independently.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import stat
import tempfile
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import BinaryIO, Final, Literal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from systematic_fx.backtest.barriers import ExecutableQuote
from systematic_fx.backtest.multisession import (
    _active_instrument_id,
    _decode_row,
    _lineaged_quote,
    _PreparedSource,
    _ResetAwareState,
    _source_date,
    _TerminalCandidate,
    _values,
)
from systematic_fx.backtest.multisession import (
    _FileIdentity as _MultisessionFileIdentity,
)
from systematic_fx.data.contract_selection import resolve_6e_contract_month
from systematic_fx.data.contracts import (
    Mbp10ContractError,
    compute_schema_fingerprint,
    decode_dbn_metadata,
    validate_mbp10_contract,
)

CACHE_SCHEMA: Final = "systematic_fx.phase1a_daily_executable_cache.v1"
CACHE_VERSION: Final = "phase1a_daily_executable_cache_v1"
CACHE_INDEX_SCHEMA: Final = "systematic_fx.phase1a_daily_executable_cache_index.v1"
MAX_CACHE_WORKERS: Final = 4
_SHA256_LENGTH: Final = 64
_READ_FLAGS: Final = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
_DIRECTORY_FLAGS: Final = _READ_FLAGS | getattr(os, "O_DIRECTORY", 0)


class EventCacheError(ValueError):
    """A cache request, source, or immutable artifact is invalid."""


@dataclass(frozen=True, slots=True)
class DailyCacheSpec:
    """One immutable raw-source/contract cache request."""

    source_date: date
    source_parquet_path: Path | str
    source_sha256: str
    raw_symbol: str
    event_index_offset: int

    def __post_init__(self) -> None:
        if isinstance(self.source_date, datetime) or not isinstance(self.source_date, date):
            raise EventCacheError("source_date must be a date")
        if not isinstance(self.source_parquet_path, (Path, str)):
            raise EventCacheError("source_parquet_path must be path-like")
        _sha256(self.source_sha256, label="source_sha256")
        if not isinstance(self.raw_symbol, str) or not self.raw_symbol.strip():
            raise EventCacheError("raw_symbol must be non-empty")
        if (
            isinstance(self.event_index_offset, bool)
            or not isinstance(self.event_index_offset, int)
            or self.event_index_offset < 0
        ):
            raise EventCacheError("event_index_offset must be a non-negative integer")

    @property
    def semantic_key(self) -> tuple[date, str]:
        return self.source_date, self.raw_symbol


@dataclass(frozen=True, slots=True)
class DailyCacheReport:
    """Identity and coverage of one published cache."""

    path: Path
    sha256: str
    byte_size: int
    disposition: Literal["CREATED", "REUSED"]
    source_date: date
    source_path: str
    source_sha256: str
    raw_symbol: str
    instrument_id: int
    event_index_offset: int
    source_row_count: int
    cached_quote_count: int
    valid_quote_count: int
    first_event_index: int
    last_event_index: int
    first_ts_recv_ns: int
    last_ts_recv_ns: int
    last_valid_event_index: int | None
    last_valid_ts_recv_ns: int | None

    def as_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["path"] = str(self.path)
        value["source_date"] = self.source_date.isoformat()
        return value


@dataclass(frozen=True, slots=True)
class CachedExecutableQuote:
    """One cache row with contract and immutable source lineage."""

    contract_key: str
    source_date: date
    source_sha256: str
    sequence: int
    source_row_index: int
    row_group_index: int
    row_index: int
    invalid_reason: str | None
    quote: ExecutableQuote


@dataclass(frozen=True, slots=True)
class _FileIdentity:
    size: int
    mtime_ns: int
    ctime_ns: int
    device: int
    inode: int


@dataclass(slots=True)
class _HeldFile:
    path: Path
    handle: BinaryIO
    identity: _FileIdentity
    sha256: str
    byte_size: int
    label: str

    def verify_identity(self) -> None:
        try:
            opened = os.fstat(self.handle.fileno())
            named = os.stat(self.path, follow_symlinks=False)
        except OSError as error:
            raise EventCacheError(
                f"{self.label} path identity disappeared while held: {self.path}"
            ) from error
        if not stat.S_ISREG(opened.st_mode) or not stat.S_ISREG(named.st_mode):
            raise EventCacheError(f"{self.label} is no longer a regular file: {self.path}")
        if _file_identity(opened) != self.identity or _file_identity(named) != self.identity:
            raise EventCacheError(
                f"{self.label} path identity changed while the verified file was held: {self.path}"
            )

    def close(self) -> None:
        self.handle.close()


@dataclass(slots=True)
class _HeldDirectory:
    path: Path
    descriptor: int
    identity: _FileIdentity

    def verify_identity(self) -> None:
        try:
            opened = os.fstat(self.descriptor)
            named = os.stat(self.path, follow_symlinks=False)
        except OSError as error:
            raise EventCacheError(
                f"artifact directory identity disappeared while held: {self.path}"
            ) from error
        if not stat.S_ISDIR(opened.st_mode) or not stat.S_ISDIR(named.st_mode):
            raise EventCacheError(f"artifact directory is no longer a directory: {self.path}")
        if not _same_inode(_file_identity(opened), self.identity) or not _same_inode(
            _file_identity(named), self.identity
        ):
            raise EventCacheError(f"artifact directory identity changed while held: {self.path}")

    def close(self) -> None:
        os.close(self.descriptor)


_CACHE_FIELDS: Final = (
    pa.field("event_index", pa.int64(), nullable=False),
    pa.field("ts_recv_ns", pa.int64(), nullable=False),
    pa.field("best_bid_ticks", pa.int64(), nullable=True),
    pa.field("best_ask_ticks", pa.int64(), nullable=True),
    pa.field("valid", pa.bool_(), nullable=False),
    pa.field("sequence", pa.uint32(), nullable=False),
    pa.field("source_row_index", pa.int64(), nullable=False),
    pa.field("row_group_index", pa.int32(), nullable=False),
    pa.field("row_index", pa.int32(), nullable=False),
    pa.field("invalid_reason", pa.string(), nullable=True),
)


def _sha256(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _SHA256_LENGTH
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise EventCacheError(f"{label} must be a lowercase SHA-256")
    return value


def _file_identity(value: os.stat_result) -> _FileIdentity:
    return _FileIdentity(
        size=value.st_size,
        mtime_ns=value.st_mtime_ns,
        ctime_ns=value.st_ctime_ns,
        device=value.st_dev,
        inode=value.st_ino,
    )


def _same_inode(left: _FileIdentity, right: _FileIdentity) -> bool:
    return (left.device, left.inode) == (right.device, right.inode)


def _open_hashed_regular_file(
    path: Path,
    *,
    label: str,
    expected_sha256: str | None = None,
    expected_byte_size: int | None = None,
    require_read_only: bool = False,
    directory_descriptor: int | None = None,
) -> _HeldFile:
    """Hash and rewind the exact descriptor later supplied to Arrow.

    Opening precedes all pathname checks.  The descriptor and pathname identities
    must agree both now and when the caller finishes consuming the file, closing
    the lstat/hash/reopen race that otherwise exists around ``ParquetFile(path)``.
    """

    requested: str | Path = path.name if directory_descriptor is not None else path
    try:
        descriptor = os.open(
            requested,
            _READ_FLAGS,
            dir_fd=directory_descriptor,
        )
    except OSError as error:
        raise EventCacheError(f"cannot safely open {label}: {path}") from error
    try:
        opened = os.fstat(descriptor)
        try:
            named = os.stat(path, follow_symlinks=False)
        except OSError as error:
            raise EventCacheError(f"cannot verify {label} pathname: {path}") from error
        identity = _file_identity(opened)
        if not stat.S_ISREG(opened.st_mode) or not stat.S_ISREG(named.st_mode):
            raise EventCacheError(f"{label} is not a regular file: {path}")
        if _file_identity(named) != identity:
            raise EventCacheError(f"{label} pathname does not identify the opened file: {path}")
        if require_read_only and opened.st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH):
            raise EventCacheError(f"{label} must be read-only")
        digest = hashlib.sha256()
        byte_size = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
            byte_size += len(chunk)
        after_hash = os.fstat(descriptor)
        if _file_identity(after_hash) != identity or byte_size != identity.size:
            raise EventCacheError(f"{label} changed while hashing: {path}")
        actual_sha256 = digest.hexdigest()
        if expected_sha256 is not None and actual_sha256 != expected_sha256:
            raise EventCacheError(f"{label} SHA-256 drift")
        if expected_byte_size is not None and byte_size != expected_byte_size:
            raise EventCacheError(f"{label} byte size drift")
        os.lseek(descriptor, 0, os.SEEK_SET)
        handle = os.fdopen(descriptor, "rb", closefd=True)
        descriptor = -1
        return _HeldFile(
            path=path,
            handle=handle,
            identity=identity,
            sha256=actual_sha256,
            byte_size=byte_size,
            label=label,
        )
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _open_artifact_directory(
    data_root: Path,
    relative_parts: Sequence[str],
    *,
    create: bool,
) -> _HeldDirectory:
    """Open a derived directory through held no-follow directory descriptors."""

    path = data_root / "derived"
    try:
        descriptor = os.open(path, _DIRECTORY_FLAGS)
    except OSError as error:
        raise EventCacheError(f"cannot safely open artifact directory: {path}") from error
    try:
        opened = os.fstat(descriptor)
        named = os.stat(path, follow_symlinks=False)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or not stat.S_ISDIR(named.st_mode)
            or _file_identity(opened) != _file_identity(named)
        ):
            raise EventCacheError(f"artifact directory identity is unsafe: {path}")
        for part in relative_parts:
            if not part or part in {".", ".."} or Path(part).name != part:
                raise EventCacheError("artifact directory component is invalid")
            if create:
                try:
                    os.mkdir(part, mode=0o755, dir_fd=descriptor)
                except FileExistsError:
                    pass
                except OSError as error:
                    raise EventCacheError(
                        f"cannot create artifact directory component: {path / part}"
                    ) from error
            try:
                child = os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptor)
            except OSError as error:
                raise EventCacheError(
                    "artifact directory component is not a safe non-symbolic link "
                    f"directory: {path / part}"
                ) from error
            try:
                child_stat = os.fstat(child)
                named_child = os.stat(part, dir_fd=descriptor, follow_symlinks=False)
                if (
                    not stat.S_ISDIR(child_stat.st_mode)
                    or not stat.S_ISDIR(named_child.st_mode)
                    or _file_identity(child_stat) != _file_identity(named_child)
                ):
                    raise EventCacheError(
                        f"artifact directory component identity is unsafe: {path / part}"
                    )
            except Exception:
                os.close(child)
                raise
            os.close(descriptor)
            descriptor = child
            path /= part
        identity = _file_identity(os.fstat(descriptor))
        held = _HeldDirectory(path=path, descriptor=descriptor, identity=identity)
        descriptor = -1
        held.verify_identity()
        return held
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _temporary_file_name(*, prefix: str, suffix: str) -> str:
    return f"{prefix}{secrets.token_hex(16)}{suffix}"


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


def _data_root(value: Path | str) -> Path:
    candidate = Path(value).expanduser()
    if candidate.is_symlink():
        raise EventCacheError("data_root cannot be a symbolic link")
    try:
        root = candidate.resolve(strict=True)
    except FileNotFoundError as error:
        raise EventCacheError("data_root does not exist") from error
    if root.name != "data" or not root.is_dir():
        raise EventCacheError("data_root must be an existing directory named data")
    derived = root / "derived"
    if not derived.is_dir() or derived.is_symlink():
        raise EventCacheError("data/derived must be an existing non-symlink directory")
    return root


def _resolved_source_path(spec: DailyCacheSpec, *, data_root: Path) -> Path:
    requested = Path(spec.source_parquet_path).expanduser()
    if requested.is_symlink():
        raise EventCacheError("source_parquet_path cannot be a symbolic link")
    try:
        absolute = Path(os.path.abspath(os.fspath(requested)))
        resolved = absolute.resolve(strict=True)
    except OSError as error:
        raise EventCacheError("source_parquet_path does not exist") from error
    if not resolved.is_file() or resolved.is_symlink():
        raise EventCacheError("source_parquet_path must be a regular non-symlink file")
    _source_relative_uri(resolved, data_root=data_root)
    return resolved


def _source_relative_uri(source_path: Path, *, data_root: Path) -> str:
    try:
        relative = source_path.relative_to(data_root)
    except ValueError as error:
        raise EventCacheError("source_parquet_path must be below data_root") from error
    if not relative.parts or ".." in relative.parts or relative.parts[0] == "derived":
        raise EventCacheError("source_parquet_path must identify raw data below data_root")
    return relative.as_posix()


def _cache_request(spec: DailyCacheSpec, *, source_relative_uri: str) -> dict[str, object]:
    return {
        "cache_schema": CACHE_SCHEMA,
        "cache_version": CACHE_VERSION,
        "event_index_offset": spec.event_index_offset,
        "raw_symbol": spec.raw_symbol,
        "source_date": spec.source_date.isoformat(),
        "source_relative_uri": source_relative_uri,
        "source_sha256": spec.source_sha256,
    }


def _cache_index_path(
    spec: DailyCacheSpec,
    *,
    source_relative_uri: str,
    data_root: Path,
) -> Path:
    request_sha256 = hashlib.sha256(
        _canonical_json_bytes(_cache_request(spec, source_relative_uri=source_relative_uri))
    ).hexdigest()
    return (
        data_root
        / "derived"
        / "backtest_event_cache"
        / CACHE_VERSION
        / "request_index"
        / f"request_sha256={request_sha256}.json"
    )


def _report_document(
    report: DailyCacheReport,
    *,
    data_root: Path,
) -> dict[str, object]:
    try:
        cache_relative_path = report.path.relative_to(data_root).as_posix()
    except ValueError as error:  # pragma: no cover - publisher always returns below data_root
        raise EventCacheError("cache report path is outside data_root") from error
    try:
        source_path = Path(report.source_path).resolve(strict=True)
    except OSError as error:
        raise EventCacheError("cache report raw source no longer exists") from error
    source_relative_uri = _source_relative_uri(source_path, data_root=data_root)
    return {
        "byte_size": report.byte_size,
        "cache_relative_path": cache_relative_path,
        "cached_quote_count": report.cached_quote_count,
        "event_index_offset": report.event_index_offset,
        "first_event_index": report.first_event_index,
        "first_ts_recv_ns": report.first_ts_recv_ns,
        "instrument_id": report.instrument_id,
        "last_event_index": report.last_event_index,
        "last_ts_recv_ns": report.last_ts_recv_ns,
        "last_valid_event_index": report.last_valid_event_index,
        "last_valid_ts_recv_ns": report.last_valid_ts_recv_ns,
        "raw_symbol": report.raw_symbol,
        "sha256": report.sha256,
        "source_date": report.source_date.isoformat(),
        "source_relative_uri": source_relative_uri,
        "source_row_count": report.source_row_count,
        "source_sha256": report.source_sha256,
        "valid_quote_count": report.valid_quote_count,
    }


def _verify_reused_cache_header(
    report: DailyCacheReport,
    *,
    source_relative_uri: str,
) -> None:
    held = _open_hashed_regular_file(
        report.path,
        label="indexed cache artifact",
        expected_sha256=report.sha256,
        expected_byte_size=report.byte_size,
        require_read_only=True,
    )
    try:
        parquet = pq.ParquetFile(held.handle)
        if parquet.metadata.num_rows != report.cached_quote_count:
            raise EventCacheError("indexed cache row count drift")
        raw_metadata = (parquet.schema_arrow.metadata or {}).get(b"systematic_fx.cache")
        try:
            metadata = None if raw_metadata is None else json.loads(raw_metadata)
        except (TypeError, json.JSONDecodeError) as error:
            raise EventCacheError("indexed cache metadata is invalid") from error
        expected = {
            "cache_schema": CACHE_SCHEMA,
            "cache_version": CACHE_VERSION,
            "event_index_offset": report.event_index_offset,
            "instrument_id": report.instrument_id,
            "raw_symbol": report.raw_symbol,
            "source_date": report.source_date.isoformat(),
            "source_relative_uri": source_relative_uri,
            "source_row_count": report.source_row_count,
            "source_sha256": report.source_sha256,
        }
        if metadata != expected:
            raise EventCacheError("indexed cache metadata drift")
        held.verify_identity()
    except (OSError, pa.ArrowException) as error:
        raise EventCacheError("cannot verify indexed cache Parquet") from error
    finally:
        held.close()


def _load_cache_index(
    spec: DailyCacheSpec,
    *,
    source_path: Path,
    data_root: Path,
) -> DailyCacheReport | None:
    source_relative_uri = _source_relative_uri(source_path, data_root=data_root)
    index_path = _cache_index_path(
        spec,
        source_relative_uri=source_relative_uri,
        data_root=data_root,
    )
    directory = _open_artifact_directory(
        data_root,
        ("backtest_event_cache", CACHE_VERSION, "request_index"),
        create=True,
    )
    try:
        try:
            os.stat(index_path.name, dir_fd=directory.descriptor, follow_symlinks=False)
        except FileNotFoundError:
            directory.verify_identity()
            return None
        held = _open_hashed_regular_file(
            index_path,
            label="cache request index",
            require_read_only=True,
            directory_descriptor=directory.descriptor,
        )
        try:
            payload = held.handle.read()
            held.verify_identity()
        finally:
            held.close()
        directory.verify_identity()
    finally:
        directory.close()
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise EventCacheError("cache request index is invalid JSON") from error
    if not isinstance(document, dict) or payload != _canonical_json_bytes(document):
        raise EventCacheError("cache request index is not canonical JSON")
    if set(document) != {"artifact_schema", "report", "request"}:
        raise EventCacheError("cache request index schema drift")
    if document.get("artifact_schema") != CACHE_INDEX_SCHEMA or document.get(
        "request"
    ) != _cache_request(spec, source_relative_uri=source_relative_uri):
        raise EventCacheError("cache request index identity drift")
    raw_report = document.get("report")
    if not isinstance(raw_report, dict):
        raise EventCacheError("cache request index report is invalid")
    expected_fields = {
        "byte_size",
        "cache_relative_path",
        "cached_quote_count",
        "event_index_offset",
        "first_event_index",
        "first_ts_recv_ns",
        "instrument_id",
        "last_event_index",
        "last_ts_recv_ns",
        "last_valid_event_index",
        "last_valid_ts_recv_ns",
        "raw_symbol",
        "sha256",
        "source_date",
        "source_relative_uri",
        "source_row_count",
        "source_sha256",
        "valid_quote_count",
    }
    if set(raw_report) != expected_fields:
        raise EventCacheError("cache request index report schema drift")
    request = document["request"]
    assert isinstance(request, dict)  # exact equality with the generated request above
    report_bindings = {
        "event_index_offset": raw_report.get("event_index_offset"),
        "raw_symbol": raw_report.get("raw_symbol"),
        "source_date": raw_report.get("source_date"),
        "source_relative_uri": raw_report.get("source_relative_uri"),
        "source_sha256": raw_report.get("source_sha256"),
    }
    request_bindings = {
        key: request.get(key)
        for key in (
            "event_index_offset",
            "raw_symbol",
            "source_date",
            "source_relative_uri",
            "source_sha256",
        )
    }
    if report_bindings != request_bindings or any(
        type(report_bindings[key]) is not type(request_bindings[key]) for key in report_bindings
    ):
        raise EventCacheError("cache request index report is not bound to its request")
    relative = Path(str(raw_report["cache_relative_path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise EventCacheError("cache request index contains an unsafe path")
    cache_path = (data_root / relative).resolve(strict=True)
    expected_directory = (data_root / "derived" / "backtest_event_cache" / CACHE_VERSION).resolve(
        strict=True
    )
    if (
        not cache_path.is_relative_to(expected_directory)
        or cache_path.is_symlink()
        or cache_path.name != f"sha256={raw_report['sha256']}.parquet"
    ):
        raise EventCacheError("cache request index target is unsafe")
    try:
        report = DailyCacheReport(
            path=cache_path,
            sha256=str(raw_report["sha256"]),
            byte_size=int(raw_report["byte_size"]),
            disposition="REUSED",
            source_date=date.fromisoformat(str(raw_report["source_date"])),
            source_path=str((data_root / source_relative_uri).resolve(strict=True)),
            source_sha256=str(raw_report["source_sha256"]),
            raw_symbol=str(raw_report["raw_symbol"]),
            instrument_id=int(raw_report["instrument_id"]),
            event_index_offset=int(raw_report["event_index_offset"]),
            source_row_count=int(raw_report["source_row_count"]),
            cached_quote_count=int(raw_report["cached_quote_count"]),
            valid_quote_count=int(raw_report["valid_quote_count"]),
            first_event_index=int(raw_report["first_event_index"]),
            last_event_index=int(raw_report["last_event_index"]),
            first_ts_recv_ns=int(raw_report["first_ts_recv_ns"]),
            last_ts_recv_ns=int(raw_report["last_ts_recv_ns"]),
            last_valid_event_index=(
                None
                if raw_report["last_valid_event_index"] is None
                else int(raw_report["last_valid_event_index"])
            ),
            last_valid_ts_recv_ns=(
                None
                if raw_report["last_valid_ts_recv_ns"] is None
                else int(raw_report["last_valid_ts_recv_ns"])
            ),
        )
    except (OSError, TypeError, ValueError) as error:
        raise EventCacheError("cache request index report values are invalid") from error
    if _report_document(report, data_root=data_root) != raw_report:
        raise EventCacheError("cache request index report values drift")
    _sha256(report.sha256, label="indexed cache sha256")
    if Path(report.source_path) != source_path:
        raise EventCacheError("cache request index source URI resolves to a different raw source")
    _verify_reused_cache_header(report, source_relative_uri=source_relative_uri)
    return report


def _publish_cache_index(
    spec: DailyCacheSpec,
    report: DailyCacheReport,
    *,
    source_path: Path,
    data_root: Path,
) -> None:
    source_relative_uri = _source_relative_uri(source_path, data_root=data_root)
    index_path = _cache_index_path(
        spec,
        source_relative_uri=source_relative_uri,
        data_root=data_root,
    )
    content = _canonical_json_bytes(
        {
            "artifact_schema": CACHE_INDEX_SCHEMA,
            "report": _report_document(report, data_root=data_root),
            "request": _cache_request(spec, source_relative_uri=source_relative_uri),
        }
    )
    directory = _open_artifact_directory(
        data_root,
        ("backtest_event_cache", CACHE_VERSION, "request_index"),
        create=True,
    )
    temporary_name = _temporary_file_name(
        prefix=f".{index_path.name}.",
        suffix=".tmp",
    )
    try:
        try:
            descriptor = os.open(
                temporary_name,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=directory.descriptor,
            )
        except OSError as error:  # pragma: no cover - cryptographically random local name
            raise EventCacheError("cannot create cache request index temporary") from error
        try:
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
                os.fchmod(handle.fileno(), 0o444)
        except Exception:
            try:
                os.close(descriptor)
            except OSError:
                pass
            raise
        try:
            os.link(
                temporary_name,
                index_path.name,
                src_dir_fd=directory.descriptor,
                dst_dir_fd=directory.descriptor,
                follow_symlinks=False,
            )
        except FileExistsError:
            pass
        held = _open_hashed_regular_file(
            index_path,
            label="published cache request index",
            require_read_only=True,
            directory_descriptor=directory.descriptor,
        )
        try:
            if held.handle.read() != content:
                raise EventCacheError("existing immutable cache request index drift")
            held.verify_identity()
        finally:
            held.close()
        os.fsync(directory.descriptor)
        directory.verify_identity()
    finally:
        try:
            os.unlink(temporary_name, dir_fd=directory.descriptor)
        except FileNotFoundError:
            pass
        finally:
            directory.close()


def _metadata(
    spec: DailyCacheSpec,
    *,
    instrument_id: int,
    source_row_count: int,
    source_relative_uri: str,
) -> dict[bytes, bytes]:
    document = {
        "cache_schema": CACHE_SCHEMA,
        "cache_version": CACHE_VERSION,
        "event_index_offset": spec.event_index_offset,
        "instrument_id": instrument_id,
        "raw_symbol": spec.raw_symbol,
        "source_date": spec.source_date.isoformat(),
        "source_relative_uri": source_relative_uri,
        "source_row_count": source_row_count,
        "source_sha256": spec.source_sha256,
    }
    return {
        b"systematic_fx.cache": json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    }


def _prepare_verified_raw_source(
    spec: DailyCacheSpec,
    *,
    source_path: Path,
    contract_month: date,
) -> tuple[_HeldFile, _PreparedSource, pq.ParquetFile]:
    """Prepare, hash, validate, and stream a raw source through one fd."""

    held = _open_hashed_regular_file(
        source_path,
        label="raw MBP-10 source",
        expected_sha256=spec.source_sha256,
    )
    try:
        parquet = pq.ParquetFile(held.handle)
        contract = validate_mbp10_contract(parquet.schema_arrow)
        raw_metadata = (parquet.schema_arrow.metadata or {}).get(b"dbn.metadata")
        if raw_metadata is None:
            raise EventCacheError("raw MBP-10 source dbn.metadata is missing")
        metadata = decode_dbn_metadata(raw_metadata)
        if _source_date(metadata, path=source_path) != spec.source_date:
            raise EventCacheError("raw MBP-10 footer source date differs from cache request")
        instrument_id = _active_instrument_id(
            raw_metadata,
            path=source_path,
            source_date=spec.source_date,
            raw_symbol=spec.raw_symbol,
            contract_month=contract_month,
        )
        identity = _MultisessionFileIdentity(
            size=held.identity.size,
            mtime_ns=held.identity.mtime_ns,
            ctime_ns=held.identity.ctime_ns,
            device=held.identity.device,
            inode=held.identity.inode,
        )
        prepared = _PreparedSource(
            path=source_path,
            source_date=spec.source_date,
            source_sha256=spec.source_sha256,
            schema_sha256=compute_schema_fingerprint(parquet.schema_arrow, contract),
            metadata_sha256=hashlib.sha256(raw_metadata).hexdigest(),
            instrument_id=instrument_id,
            raw_symbol=spec.raw_symbol,
            contract_month=contract_month,
            row_count=parquet.metadata.num_rows,
            row_group_count=parquet.metadata.num_row_groups,
            event_index_offset=spec.event_index_offset,
            identity=identity,
        )
        held.verify_identity()
        return held, prepared, parquet
    except (OSError, pa.ArrowException, Mbp10ContractError) as error:
        held.close()
        raise EventCacheError("cannot verify raw MBP-10 source Parquet") from error
    except Exception:
        held.close()
        raise


def _chunk_table(rows: list[dict[str, object]], schema: pa.Schema) -> pa.Table:
    arrays = [pa.array([row[field.name] for row in rows], type=field.type) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def _publish_cache(temporary: Path, *, data_root: Path) -> tuple[Path, str, int, str]:
    source_directory = _open_artifact_directory(data_root, (), create=False)
    target_directory = _open_artifact_directory(
        data_root,
        ("backtest_event_cache", CACHE_VERSION),
        create=True,
    )
    try:
        try:
            descriptor = os.open(
                temporary.name,
                _READ_FLAGS,
                dir_fd=source_directory.descriptor,
            )
        except OSError as error:
            raise EventCacheError("cannot safely reopen generated cache temporary") from error
        try:
            opened = os.fstat(descriptor)
            named = os.stat(
                temporary.name,
                dir_fd=source_directory.descriptor,
                follow_symlinks=False,
            )
            if not stat.S_ISREG(opened.st_mode) or _file_identity(opened) != _file_identity(named):
                raise EventCacheError("generated cache temporary identity is unsafe")
            os.fchmod(descriptor, 0o444)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        held_temporary = _open_hashed_regular_file(
            temporary,
            label="generated cache temporary",
            require_read_only=True,
            directory_descriptor=source_directory.descriptor,
        )
        digest = held_temporary.sha256
        byte_size = held_temporary.byte_size
        target = target_directory.path / f"sha256={digest}.parquet"
        disposition = "CREATED"
        try:
            try:
                os.link(
                    temporary.name,
                    target.name,
                    src_dir_fd=source_directory.descriptor,
                    dst_dir_fd=target_directory.descriptor,
                    follow_symlinks=False,
                )
            except FileExistsError:
                disposition = "REUSED"
            if disposition == "REUSED":
                held_target = _open_hashed_regular_file(
                    target,
                    label="existing content-addressed cache",
                    expected_sha256=digest,
                    expected_byte_size=byte_size,
                    require_read_only=True,
                    directory_descriptor=target_directory.descriptor,
                )
                try:
                    held_target.verify_identity()
                finally:
                    held_target.close()
                held_temporary.verify_identity()
            else:
                try:
                    opened_after_link = _file_identity(os.fstat(held_temporary.handle.fileno()))
                    source_after_link = _file_identity(
                        os.stat(
                            temporary.name,
                            dir_fd=source_directory.descriptor,
                            follow_symlinks=False,
                        )
                    )
                    target_after_link = _file_identity(
                        os.stat(
                            target.name,
                            dir_fd=target_directory.descriptor,
                            follow_symlinks=False,
                        )
                    )
                    if not (
                        _same_inode(opened_after_link, held_temporary.identity)
                        and opened_after_link.size == held_temporary.byte_size
                        and opened_after_link.mtime_ns == held_temporary.identity.mtime_ns
                        and source_after_link == opened_after_link
                        and target_after_link == opened_after_link
                    ):
                        raise EventCacheError(
                            "published cache hard link does not identify the verified temporary"
                        )
                    held_temporary.identity = opened_after_link
                except Exception:
                    try:
                        os.unlink(target.name, dir_fd=target_directory.descriptor)
                    except OSError:
                        pass
                    raise
            os.fsync(target_directory.descriptor)
            source_directory.verify_identity()
            target_directory.verify_identity()
        finally:
            held_temporary.close()
        return target, digest, byte_size, disposition
    finally:
        source_directory.close()
        target_directory.close()


def build_daily_executable_cache(
    spec: DailyCacheSpec,
    *,
    data_root: Path | str,
) -> DailyCacheReport:
    """Decode one raw file once and publish its fixed-contract BBO event path."""

    if not isinstance(spec, DailyCacheSpec):
        raise EventCacheError("spec must be a DailyCacheSpec")
    root = _data_root(data_root)
    source_path = _resolved_source_path(spec, data_root=root)
    source_relative_uri = _source_relative_uri(source_path, data_root=root)
    reused = _load_cache_index(spec, source_path=source_path, data_root=root)
    if reused is not None:
        return reused
    contract_month = resolve_6e_contract_month(spec.raw_symbol, source_date=spec.source_date)
    raw_held, prepared, parquet = _prepare_verified_raw_source(
        spec,
        source_path=source_path,
        contract_month=contract_month,
    )
    try:
        schema = pa.schema(
            _CACHE_FIELDS,
            metadata=_metadata(
                spec,
                instrument_id=prepared.instrument_id,
                source_row_count=prepared.row_count,
                source_relative_uri=source_relative_uri,
            ),
        )
        temporary_descriptor, temporary_name = tempfile.mkstemp(
            dir=root / "derived",
            prefix=f".{CACHE_VERSION}.",
            suffix=".parquet.tmp",
        )
        temporary_handle: BinaryIO | None = os.fdopen(
            temporary_descriptor,
            "w+b",
            closefd=True,
        )
    except Exception:
        try:
            os.close(temporary_descriptor)
        except (OSError, UnboundLocalError):
            pass
        try:
            raw_held.verify_identity()
        finally:
            raw_held.close()
        raise
    temporary = Path(temporary_name)
    cached_count = 0
    valid_count = 0
    first_event_index: int | None = None
    last_event_index: int | None = None
    first_ts_recv_ns: int | None = None
    last_ts_recv_ns: int | None = None
    last_valid_event_index: int | None = None
    last_valid_ts_recv_ns: int | None = None
    source_row_offset = 0
    prior_selected_ts_recv_ns: int | None = None
    state = _ResetAwareState()
    writer: pq.ParquetWriter | None = None
    try:
        writer = pq.ParquetWriter(
            temporary_handle,
            schema,
            compression="zstd",
            use_dictionary=("invalid_reason",),
            write_statistics=True,
            version="2.6",
        )
        for row_group_index in range(parquet.metadata.num_row_groups):
            table = parquet.read_row_group(
                row_group_index,
                columns=list(_values_columns()),
                use_threads=False,
            )
            selected_indexes = pc.indices_nonzero(
                pc.equal(
                    table["instrument_id"],
                    pa.scalar(prepared.instrument_id, type=table["instrument_id"].type),
                )
            )
            physical_row_indexes = selected_indexes.to_pylist()
            selected = table.take(selected_indexes)
            values = _values(selected)
            rows: list[dict[str, object]] = []
            for selected_index, physical_row_index in enumerate(physical_row_indexes):
                row_index = int(physical_row_index)
                source_row_index = source_row_offset + row_index
                decoded = _decode_row(
                    values,
                    selected_index,
                    lineage=f"{prepared.path}: row {source_row_index}",
                )
                if (
                    prior_selected_ts_recv_ns is not None
                    and decoded.ts_recv_ns < prior_selected_ts_recv_ns
                ):
                    raise EventCacheError(
                        "selected-contract ts_recv regressed in physical source order"
                    )
                prior_selected_ts_recv_ns = decoded.ts_recv_ns
                invalid_reason = state.observe(decoded)
                quote = _lineaged_quote(
                    _TerminalCandidate(
                        source=prepared,
                        row=decoded,
                        row_group_index=row_group_index,
                        row_index=row_index,
                        source_row_index=source_row_index,
                    ),
                    invalid_reason=invalid_reason,
                )
                rows.append(
                    {
                        "event_index": quote.event_index,
                        "ts_recv_ns": quote.ts_recv_ns,
                        "best_bid_ticks": quote.best_bid_ticks,
                        "best_ask_ticks": quote.best_ask_ticks,
                        "valid": quote.valid,
                        "sequence": quote.sequence,
                        "source_row_index": quote.source_row_index,
                        "row_group_index": quote.row_group_index,
                        "row_index": quote.row_index,
                        "invalid_reason": (
                            quote.invalid_reason.value if quote.invalid_reason is not None else None
                        ),
                    }
                )
                if first_event_index is None:
                    first_event_index = quote.event_index
                    first_ts_recv_ns = quote.ts_recv_ns
                last_event_index = quote.event_index
                last_ts_recv_ns = quote.ts_recv_ns
                cached_count += 1
                valid_count += int(quote.valid)
                if quote.valid:
                    last_valid_event_index = quote.event_index
                    last_valid_ts_recv_ns = quote.ts_recv_ns
            if rows:
                writer.write_table(_chunk_table(rows, schema), row_group_size=len(rows))
            source_row_offset += table.num_rows
        writer.close()
        writer = None
        if source_row_offset != prepared.row_count:
            raise EventCacheError("source row count changed during cache build")
        if cached_count == 0:
            raise EventCacheError("selected contract produced no cache rows")
        if None in (
            first_event_index,
            last_event_index,
            first_ts_recv_ns,
            last_ts_recv_ns,
        ):  # pragma: no cover - cached_count proves every bound was assigned
            raise EventCacheError("selected contract cache bounds are incomplete")
        assert temporary_handle is not None
        temporary_handle.flush()
        os.fsync(temporary_handle.fileno())
        opened_temporary = os.fstat(temporary_handle.fileno())
        named_temporary = os.stat(temporary, follow_symlinks=False)
        if not stat.S_ISREG(opened_temporary.st_mode) or _file_identity(
            opened_temporary
        ) != _file_identity(named_temporary):
            raise EventCacheError("generated cache pathname changed while writing")
        temporary_handle.close()
        temporary_handle = None
        raw_held.verify_identity()
        raw_held.close()
        raw_held = None
        target, digest, byte_size, disposition = _publish_cache(temporary, data_root=root)
    except Exception:
        if writer is not None:
            writer.close()
        raise
    finally:
        if temporary_handle is not None:
            temporary_handle.close()
        if raw_held is not None:
            try:
                raw_held.verify_identity()
            finally:
                raw_held.close()
        temporary.unlink(missing_ok=True)
    report = DailyCacheReport(
        path=target,
        sha256=digest,
        byte_size=byte_size,
        disposition=disposition,
        source_date=spec.source_date,
        source_path=str(source_path),
        source_sha256=spec.source_sha256,
        raw_symbol=spec.raw_symbol,
        instrument_id=prepared.instrument_id,
        event_index_offset=spec.event_index_offset,
        source_row_count=prepared.row_count,
        cached_quote_count=cached_count,
        valid_quote_count=valid_count,
        first_event_index=first_event_index,
        last_event_index=last_event_index,
        first_ts_recv_ns=first_ts_recv_ns,
        last_ts_recv_ns=last_ts_recv_ns,
        last_valid_event_index=last_valid_event_index,
        last_valid_ts_recv_ns=last_valid_ts_recv_ns,
    )
    _verify_reused_cache_header(
        report,
        source_relative_uri=source_relative_uri,
    )
    _publish_cache_index(
        spec,
        report,
        source_path=source_path,
        data_root=root,
    )
    return report


def _values_columns() -> tuple[str, ...]:
    # Keep the private source decoder and its exact frozen column set coupled.
    from systematic_fx.backtest.multisession import _COLUMNS

    return _COLUMNS


def _build_worker(arguments: tuple[DailyCacheSpec, str]) -> DailyCacheReport:
    spec, data_root = arguments
    return build_daily_executable_cache(spec, data_root=data_root)


def build_daily_cache_batch(
    specs: Sequence[DailyCacheSpec],
    *,
    data_root: Path | str,
    max_workers: int = MAX_CACHE_WORKERS,
    progress_callback: Callable[[DailyCacheReport, int, int], None] | None = None,
) -> tuple[DailyCacheReport, ...]:
    """Build independent date/contract caches with bounded process parallelism."""

    if isinstance(specs, (str, bytes)) or not isinstance(specs, Sequence) or not specs:
        raise EventCacheError("specs must be a non-empty sequence")
    ordered = tuple(specs)
    if any(not isinstance(spec, DailyCacheSpec) for spec in ordered):
        raise EventCacheError("specs must contain only DailyCacheSpec values")
    if len({spec.semantic_key for spec in ordered}) != len(ordered):
        raise EventCacheError("specs contain a duplicate source-date/contract key")
    if tuple(sorted(spec.semantic_key for spec in ordered)) != tuple(
        spec.semantic_key for spec in ordered
    ):
        raise EventCacheError("specs must be ordered by source date and raw symbol")
    if isinstance(max_workers, bool) or not isinstance(max_workers, int):
        raise EventCacheError("max_workers must be an integer")
    if not 1 <= max_workers <= MAX_CACHE_WORKERS:
        raise EventCacheError(f"max_workers must be between 1 and {MAX_CACHE_WORKERS}")
    if progress_callback is not None and not callable(progress_callback):
        raise EventCacheError("progress_callback must be callable")
    root = _data_root(data_root)
    if max_workers == 1:
        sequential: list[DailyCacheReport] = []
        for completed_count, spec in enumerate(ordered, start=1):
            report = build_daily_executable_cache(spec, data_root=root)
            sequential.append(report)
            if progress_callback is not None:
                progress_callback(report, completed_count, len(ordered))
        return tuple(sequential)
    reports_by_index: list[DailyCacheReport | None] = [None] * len(ordered)
    completed_count = 0
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        pending: dict[Future[DailyCacheReport], int] = {}
        next_index = 0
        while next_index < min(max_workers, len(ordered)):
            pending[executor.submit(_build_worker, (ordered[next_index], str(root)))] = next_index
            next_index += 1
        while pending:
            completed, _ = wait(tuple(pending), return_when=FIRST_COMPLETED)
            for future in sorted(completed, key=pending.__getitem__):
                result_index = pending.pop(future)
                report = future.result()
                reports_by_index[result_index] = report
                completed_count += 1
                if progress_callback is not None:
                    progress_callback(report, completed_count, len(ordered))
                if next_index < len(ordered):
                    pending[executor.submit(_build_worker, (ordered[next_index], str(root)))] = (
                        next_index
                    )
                    next_index += 1
    if any(report is None for report in reports_by_index):  # pragma: no cover - loop invariant
        raise EventCacheError("parallel cache builder omitted a result")
    reports = tuple(report for report in reports_by_index if report is not None)
    if tuple((item.source_date, item.raw_symbol) for item in reports) != tuple(
        spec.semantic_key for spec in ordered
    ):
        raise EventCacheError("parallel cache result order drift")
    return reports


def read_daily_executable_cache(
    report: DailyCacheReport,
) -> Iterator[CachedExecutableQuote]:
    """Verify and stream one immutable cache in canonical source-row order."""

    if not isinstance(report, DailyCacheReport):
        raise EventCacheError("report must be a DailyCacheReport")
    return _read_verified_daily_executable_cache(report)


def _cache_data_root(path: Path) -> Path:
    absolute = Path(os.path.abspath(os.fspath(path)))
    if (
        absolute.parent.name != CACHE_VERSION
        or absolute.parent.parent.name != "backtest_event_cache"
        or absolute.parent.parent.parent.name != "derived"
    ):
        raise EventCacheError("cache artifact path does not follow the frozen data layout")
    root = absolute.parent.parent.parent.parent
    if root.name != "data":
        raise EventCacheError("cache artifact path is not below a data root")
    return root


def _read_verified_daily_executable_cache(
    report: DailyCacheReport,
) -> Iterator[CachedExecutableQuote]:
    root = _cache_data_root(report.path)
    source_path = Path(os.path.abspath(report.source_path))
    source_relative_uri = _source_relative_uri(source_path, data_root=root)
    held = _open_hashed_regular_file(
        report.path,
        label="cache artifact",
        expected_sha256=report.sha256,
        expected_byte_size=report.byte_size,
        require_read_only=True,
    )
    try:
        try:
            parquet = pq.ParquetFile(held.handle)
        except (OSError, pa.ArrowException) as error:
            raise EventCacheError("cannot open verified cache Parquet") from error
        yield from _stream_daily_executable_cache(
            report,
            parquet=parquet,
            source_relative_uri=source_relative_uri,
        )
    finally:
        try:
            held.verify_identity()
        finally:
            held.close()


def _stream_daily_executable_cache(
    report: DailyCacheReport,
    *,
    parquet: pq.ParquetFile,
    source_relative_uri: str,
) -> Iterator[CachedExecutableQuote]:
    raw_metadata = (parquet.schema_arrow.metadata or {}).get(b"systematic_fx.cache")
    if raw_metadata is None:
        raise EventCacheError("cache metadata is missing")
    try:
        metadata = json.loads(raw_metadata)
    except (TypeError, json.JSONDecodeError) as error:
        raise EventCacheError("cache metadata is invalid JSON") from error
    expected_metadata = {
        "cache_schema": CACHE_SCHEMA,
        "cache_version": CACHE_VERSION,
        "event_index_offset": report.event_index_offset,
        "instrument_id": report.instrument_id,
        "raw_symbol": report.raw_symbol,
        "source_date": report.source_date.isoformat(),
        "source_relative_uri": source_relative_uri,
        "source_row_count": report.source_row_count,
        "source_sha256": report.source_sha256,
    }
    if metadata != expected_metadata:
        raise EventCacheError("cache metadata differs from its report")
    emitted = 0
    valid_count = 0
    last_valid_event_index: int | None = None
    last_valid_ts_recv_ns: int | None = None
    prior_event_index: int | None = None
    prior_source_row_index: int | None = None
    prior_ts_recv_ns: int | None = None
    prior_sequence: int | None = None
    first_event_index: int | None = None
    first_ts_recv_ns: int | None = None
    columns = [field.name for field in _CACHE_FIELDS]
    for row_group_index in range(parquet.metadata.num_row_groups):
        table = parquet.read_row_group(row_group_index, columns=columns, use_threads=False)
        values = {name: table[name].combine_chunks().to_pylist() for name in columns}
        for row_index in range(table.num_rows):
            event_index = int(values["event_index"][row_index])
            source_row_index = int(values["source_row_index"][row_index])
            if prior_event_index is not None and event_index <= prior_event_index:
                raise EventCacheError("cache event indexes are not strictly increasing")
            if prior_source_row_index is not None and source_row_index <= prior_source_row_index:
                raise EventCacheError("cache source row indexes are not strictly increasing")
            if event_index != report.event_index_offset + source_row_index:
                raise EventCacheError("cache event index differs from source-row lineage")
            prior_event_index = event_index
            prior_source_row_index = source_row_index
            valid = bool(values["valid"][row_index])
            ts_recv_ns = int(values["ts_recv_ns"][row_index])
            sequence = int(values["sequence"][row_index])
            if prior_ts_recv_ns is not None and ts_recv_ns < prior_ts_recv_ns:
                raise EventCacheError("cache ts_recv order regressed")
            if (
                prior_ts_recv_ns is not None
                and ts_recv_ns == prior_ts_recv_ns
                and prior_sequence is not None
                and sequence < prior_sequence
            ):
                raise EventCacheError("cache sequence regressed within one receive timestamp")
            prior_ts_recv_ns = ts_recv_ns
            prior_sequence = sequence
            if first_event_index is None:
                first_event_index = event_index
                first_ts_recv_ns = ts_recv_ns
            invalid_reason = values["invalid_reason"][row_index]
            if valid == (invalid_reason is not None):
                raise EventCacheError("cache valid flag and invalid reason disagree")
            quote = ExecutableQuote(
                event_index=event_index,
                ts_recv_ns=ts_recv_ns,
                best_bid_ticks=(
                    int(values["best_bid_ticks"][row_index])
                    if values["best_bid_ticks"][row_index] is not None
                    else None
                ),
                best_ask_ticks=(
                    int(values["best_ask_ticks"][row_index])
                    if values["best_ask_ticks"][row_index] is not None
                    else None
                ),
                valid=valid,
            )
            yield CachedExecutableQuote(
                contract_key=report.raw_symbol,
                source_date=report.source_date,
                source_sha256=report.source_sha256,
                sequence=sequence,
                source_row_index=source_row_index,
                row_group_index=int(values["row_group_index"][row_index]),
                row_index=int(values["row_index"][row_index]),
                invalid_reason=invalid_reason,
                quote=quote,
            )
            emitted += 1
            valid_count += int(valid)
            if valid:
                last_valid_event_index = event_index
                last_valid_ts_recv_ns = ts_recv_ns
    if emitted != report.cached_quote_count:
        raise EventCacheError("cache quote count differs from its report")
    if valid_count != report.valid_quote_count:
        raise EventCacheError("cache valid quote count differs from its report")
    if (
        first_event_index,
        prior_event_index,
        first_ts_recv_ns,
        prior_ts_recv_ns,
    ) != (
        report.first_event_index,
        report.last_event_index,
        report.first_ts_recv_ns,
        report.last_ts_recv_ns,
    ):
        raise EventCacheError("cache event bounds differ from its report")
    if (
        last_valid_event_index,
        last_valid_ts_recv_ns,
    ) != (
        report.last_valid_event_index,
        report.last_valid_ts_recv_ns,
    ):
        raise EventCacheError("cache terminal executable quote differs from its report")
