"""Restartable row-group structural qualification for immutable MBP-10 sources.

This scanner is intentionally narrower than research eligibility.  It proves
complete row-group coverage and applies only source-local structural rules.
Session calendars, trading status, contract definitions, snapshot recovery,
missing intervals, and economic suitability remain separate gates.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
import tomllib
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Final, Literal

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from systematic_fx.data.contracts import (
    UNDEFINED_PRICE,
    compute_schema_fingerprint,
    decode_dbn_metadata,
    validate_mbp10_contract,
)
from systematic_fx.data.instruments import parse_instrument_mappings

CHECKER_VERSION: Final = "mbp10_structural_qc_v1"
FILE_ARTIFACT_SCHEMA: Final = "systematic_fx.mbp10_structural_qc_file.v1"
CHECKPOINT_ARTIFACT_SCHEMA: Final = "systematic_fx.mbp10_structural_qc_row_group_checkpoint.v1"
DEFAULT_MANIFEST_NAME: Final = "mbp10_structural_qc_v1.jsonl"
DEFAULT_SOURCE_MANIFEST_NAME: Final = "mbp10_source_sha256_v1.jsonl"

F_MAYBE_BAD_BOOK: Final = 4
F_BAD_TS_RECV: Final = 8
F_SNAPSHOT: Final = 32

HARD_CHECKS: Final = (
    "null_required_values",
    "unexpected_rtype",
    "unexpected_publisher_id",
    "unknown_action",
    "unknown_side",
    "reset_side_not_none",
    "reset_book_not_empty",
    "depth_out_of_range",
    "unmapped_instrument_id",
    "ts_recv_outside_request_range",
    "trusted_ts_recv_regression_per_instrument",
    "clean_trade_none_book_mutation",
    "book_level_noncontiguous",
    "bid_ladder_not_strictly_descending",
    "ask_ladder_not_strictly_ascending",
    "defined_book_price_zero_size",
    "defined_book_price_zero_count",
    "defined_book_count_exceeds_size",
    "undefined_book_price_nonzero_size",
    "undefined_book_price_nonzero_count",
)

DIAGNOSTIC_CHECKS: Final = (
    "publisher_id_zero",
    "global_ts_recv_regression",
    "all_ts_recv_regression_per_instrument",
    "bad_ts_recv_exempted_regression_per_instrument",
    "ts_recv_before_ts_event",
    "global_ts_event_regression",
    "ts_event_outside_request_range",
    "negative_ts_in_delta",
    "sequence_repeat",
    "sequence_regression",
    "sequence_forward_gap",
    "adjacent_event_key_repeat",
    "reset_action_rows",
    "snapshot_flag_rows",
    "maybe_bad_book_flag_rows",
    "bad_ts_recv_flag_rows",
    "undefined_event_price_rows",
    "undefined_event_price_non_reset_rows",
    "incomplete_bbo_rows",
    "locked_bbo_rows",
    "crossed_bbo_rows",
)

_SOURCE_URI = re.compile(
    r"(?P<year>[0-9]{4})/(?P<month>[0-9]{2})/(?P<day>[0-9]{2})/"
    r"glbx-mdp3-(?P<stamp>[0-9]{8})\.mbp-10\.parquet"
)
_SHA256 = re.compile(r"[0-9a-f]{64}")
_SOURCE_KEYS = frozenset({"byte_size", "relative_uri", "sha256", "source_date"})
_IDENTITY_KEYS = frozenset({"ctime_ns", "device", "inode", "mtime_ns", "size"})
_CHECKPOINT_KEYS = frozenset(
    {
        "artifact_schema",
        "checker_version",
        "config_sha256",
        "source_manifest_sha256",
        "relative_uri",
        "source_date",
        "source_sha256",
        "source_byte_size",
        "source_identity",
        "schema_fingerprint",
        "expected_row_count",
        "expected_row_group_count",
        "row_group_index",
        "row_count",
        "first_ts_recv_ns",
        "last_ts_recv_ns",
        "first_ts_event_ns",
        "last_ts_event_ns",
        "instrument_ts_recv_bounds",
        "publisher_sequence_bounds",
        "first_event_key",
        "last_event_key",
        "hard_violation_counts",
        "diagnostic_counts",
        "previous_checkpoint_sha256",
        "checkpoint_record_sha256",
    }
)
_EMPTY_CHECKPOINT_SHA256: Final = "0" * 64

_BASE_COLUMNS: Final = (
    "ts_recv",
    "ts_event",
    "rtype",
    "publisher_id",
    "instrument_id",
    "action",
    "side",
    "depth",
    "price",
    "size",
    "flags",
    "ts_in_delta",
    "sequence",
)
_BOOK_COLUMNS: Final = tuple(
    name
    for level in range(10)
    for name in (
        f"bid_px_{level:02d}",
        f"ask_px_{level:02d}",
        f"bid_sz_{level:02d}",
        f"ask_sz_{level:02d}",
        f"bid_ct_{level:02d}",
        f"ask_ct_{level:02d}",
    )
)
QC_COLUMNS: Final = _BASE_COLUMNS + _BOOK_COLUMNS
BOOK_STATE_COLUMNS: Final = (
    "publisher_id",
    "instrument_id",
    "action",
    "flags",
    *_BOOK_COLUMNS,
)


class StructuralQcError(ValueError):
    """A provenance, configuration, checkpoint, or structural input is unsafe."""


@dataclass(frozen=True)
class StructuralQcConfig:
    """Frozen semantic configuration and its canonical fingerprint."""

    artifact_schema: str
    checkpoint_schema: str
    checker_version: str
    expected_rtype: int
    expected_publisher_id: int
    maximum_depth: int
    book_levels: int
    undefined_price: int
    known_actions: tuple[str, ...]
    known_sides: tuple[str, ...]
    hard_violation_maximum: int
    diagnostics_affect_result: bool
    hard_checks: tuple[str, ...]
    diagnostic_checks: tuple[str, ...]
    sha256: str

    def semantic_payload(self) -> dict[str, object]:
        return {
            "artifact_schema": self.artifact_schema,
            "book_levels": self.book_levels,
            "checker_version": self.checker_version,
            "checkpoint_schema": self.checkpoint_schema,
            "diagnostic_checks": list(self.diagnostic_checks),
            "diagnostics_affect_result": self.diagnostics_affect_result,
            "expected_rtype": self.expected_rtype,
            "expected_publisher_id": self.expected_publisher_id,
            "hard_checks": list(self.hard_checks),
            "hard_violation_maximum": self.hard_violation_maximum,
            "known_actions": list(self.known_actions),
            "known_sides": list(self.known_sides),
            "maximum_depth": self.maximum_depth,
            "undefined_price": self.undefined_price,
        }


@dataclass(frozen=True)
class StructuralQcProgress:
    """A deterministic durable progress event."""

    status: Literal["RESUMED", "SCANNED", "COMPLETE"]
    file_index: int
    file_count: int
    relative_uri: str | None
    row_group_index: int | None
    row_groups_in_file: int
    row_groups_complete: int
    total_row_groups: int
    rows_complete: int

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


ProgressCallback = Callable[[StructuralQcProgress], None]


@dataclass(frozen=True)
class StructuralQcReport:
    """Aggregate coverage and artifact identity for a completed scan."""

    status: Literal["COMPLETE"]
    checker_version: str
    config_path: Path
    config_sha256: str
    data_root: Path
    dataset_root: Path
    source_manifest_path: Path
    source_manifest_sha256: str
    manifest_path: Path
    checkpoint_path: Path
    manifest_sha256: str
    manifest_byte_size: int
    file_count: int
    passed_file_count: int
    failed_file_count: int
    row_group_count: int
    row_count: int
    hard_violation_count: int
    resumed_row_group_count: int
    scanned_row_group_count: int

    def as_dict(self) -> dict[str, object]:
        values = asdict(self)
        for key in (
            "config_path",
            "data_root",
            "dataset_root",
            "source_manifest_path",
            "manifest_path",
            "checkpoint_path",
        ):
            values[key] = values[key].as_posix()
        return values


@dataclass(frozen=True)
class _Identity:
    size: int
    mtime_ns: int
    ctime_ns: int
    device: int
    inode: int

    def as_dict(self) -> dict[str, int]:
        return {
            "ctime_ns": self.ctime_ns,
            "device": self.device,
            "inode": self.inode,
            "mtime_ns": self.mtime_ns,
            "size": self.size,
        }


@dataclass(frozen=True)
class _SourceSpec:
    relative_uri: str
    source_date: date
    sha256: str
    byte_size: int
    path: Path


@dataclass
class _ResumeFile:
    row_groups: int
    rows: int
    row_group_rows: list[int]
    expected_row_groups: int
    expected_rows: int
    schema_fingerprint: str
    identity: _Identity


@dataclass(frozen=True)
class _FileContext:
    schema_fingerprint: str
    expected_rows: int
    expected_row_groups: int
    request_start_ns: int
    request_end_ns: int
    mapped_ids: frozenset[int]


_BookState = dict[int, tuple[tuple[int, ...], bool]]


def _canonical_line(record: Mapping[str, object]) -> bytes:
    return (
        json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _canonical_sha(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def _checkpoint_record_sha256(record: Mapping[str, object]) -> str:
    payload = dict(record)
    payload.pop("checkpoint_record_sha256", None)
    return hashlib.sha256(_canonical_line(payload)).hexdigest()


def _required_int(value: object, *, label: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise StructuralQcError(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        raise StructuralQcError(f"{label} must be at least {minimum}")
    return value


def _strict_file(path: Path | str, *, label: str) -> Path:
    requested = Path(path).expanduser()
    if requested.is_symlink():
        raise StructuralQcError(f"{label} cannot be a symbolic link: {requested}")
    resolved = requested.resolve(strict=True)
    if not resolved.is_file() or not stat.S_ISREG(resolved.lstat().st_mode):
        raise StructuralQcError(f"{label} must be a regular file: {resolved}")
    return resolved


def _strict_root(path: Path | str, *, label: str) -> Path:
    requested = Path(path).expanduser()
    if requested.is_symlink():
        raise StructuralQcError(f"{label} cannot be a symbolic link: {requested}")
    resolved = requested.resolve(strict=True)
    if not resolved.is_dir():
        raise NotADirectoryError(f"{label} is not a directory: {resolved}")
    return resolved


def _contained(path: Path, root: Path, *, label: str) -> Path:
    try:
        return path.relative_to(root)
    except ValueError as exc:
        raise StructuralQcError(f"{label} must remain inside {root}") from exc


def _no_symlinks(root: Path, relative: Path, *, label: str) -> None:
    current = root
    for part in relative.parts:
        current /= part
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"{label} does not exist: {current}") from exc
        if stat.S_ISLNK(mode):
            raise StructuralQcError(f"{label} cannot traverse a symbolic link: {current}")


def _identity(value: os.stat_result) -> _Identity:
    return _Identity(
        size=value.st_size,
        mtime_ns=value.st_mtime_ns,
        ctime_ns=value.st_ctime_ns,
        device=value.st_dev,
        inode=value.st_ino,
    )


def _config_table(path: Path | str) -> tuple[Path, dict[str, Any]]:
    resolved = _strict_file(path, label="config_path")
    try:
        document = tomllib.loads(resolved.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise StructuralQcError(f"invalid structural QC TOML: {resolved}") from exc
    if set(document) != {"quality"} or not isinstance(document["quality"], dict):
        raise StructuralQcError("structural QC config must contain only a [quality] table")
    return resolved, document["quality"]


def load_structural_qc_config(path: Path | str) -> StructuralQcConfig:
    """Load a frozen v1 config and hash its canonical semantic payload."""

    _, table = _config_table(path)
    required_keys = {
        "artifact_schema",
        "checkpoint_schema",
        "checker_version",
        "expected_rtype",
        "expected_publisher_id",
        "maximum_depth",
        "book_levels",
        "undefined_price",
        "known_actions",
        "known_sides",
        "hard_violation_maximum",
        "diagnostics_affect_result",
        "hard_checks",
        "diagnostic_checks",
    }
    if set(table) != required_keys:
        missing = sorted(required_keys - set(table))
        extra = sorted(set(table) - required_keys)
        raise StructuralQcError(
            f"structural QC config keys differ; missing={missing}, extra={extra}"
        )

    actions = tuple(table["known_actions"])
    sides = tuple(table["known_sides"])
    hard_checks = tuple(table["hard_checks"])
    diagnostic_checks = tuple(table["diagnostic_checks"])
    expected = {
        "artifact_schema": FILE_ARTIFACT_SCHEMA,
        "checkpoint_schema": CHECKPOINT_ARTIFACT_SCHEMA,
        "checker_version": CHECKER_VERSION,
        "expected_rtype": 10,
        "expected_publisher_id": 1,
        "maximum_depth": 9,
        "book_levels": 10,
        "undefined_price": UNDEFINED_PRICE,
        "known_actions": ("A", "C", "M", "N", "R", "T"),
        "known_sides": ("A", "B", "N"),
        "hard_violation_maximum": 0,
        "diagnostics_affect_result": False,
        "hard_checks": HARD_CHECKS,
        "diagnostic_checks": DIAGNOSTIC_CHECKS,
    }
    actual = {
        "artifact_schema": table["artifact_schema"],
        "checkpoint_schema": table["checkpoint_schema"],
        "checker_version": table["checker_version"],
        "expected_rtype": table["expected_rtype"],
        "expected_publisher_id": table["expected_publisher_id"],
        "maximum_depth": table["maximum_depth"],
        "book_levels": table["book_levels"],
        "undefined_price": table["undefined_price"],
        "known_actions": actions,
        "known_sides": sides,
        "hard_violation_maximum": table["hard_violation_maximum"],
        "diagnostics_affect_result": table["diagnostics_affect_result"],
        "hard_checks": hard_checks,
        "diagnostic_checks": diagnostic_checks,
    }
    if actual != expected:
        differences = sorted(key for key in expected if actual[key] != expected[key])
        raise StructuralQcError(
            "v1 structural QC semantics cannot be weakened or changed: " + ", ".join(differences)
        )

    provisional = StructuralQcConfig(**actual, sha256="")
    return StructuralQcConfig(**actual, sha256=_canonical_sha(provisional.semantic_payload()))


def _parse_uri(value: object) -> tuple[str, date]:
    if not isinstance(value, str) or not value or "\\" in value:
        raise StructuralQcError("source relative_uri must be a canonical POSIX path")
    pure = PurePosixPath(value)
    if (
        pure.is_absolute()
        or pure.as_posix() != value
        or any(part in {"", ".", ".."} for part in pure.parts)
    ):
        raise StructuralQcError(f"unsafe source relative_uri: {value!r}")
    match = _SOURCE_URI.fullmatch(value)
    if match is None:
        raise StructuralQcError(f"invalid MBP-10 source relative_uri: {value!r}")
    try:
        source_date = date(
            int(match.group("year")), int(match.group("month")), int(match.group("day"))
        )
    except ValueError as exc:
        raise StructuralQcError(f"invalid date in relative_uri: {value!r}") from exc
    if source_date.strftime("%Y%m%d") != match.group("stamp"):
        raise StructuralQcError(f"partition and filename dates disagree: {value!r}")
    return value, source_date


def _load_source_manifest(
    data_root: Path,
    dataset_root: Path,
    manifest_path: Path | str,
) -> tuple[Path, str, list[_SourceSpec]]:
    path = _strict_file(manifest_path, label="source_manifest_path")
    relative_manifest = _contained(path, data_root, label="source_manifest_path")
    _no_symlinks(data_root, relative_manifest, label="source_manifest_path")

    digest = hashlib.sha256()
    sources: list[_SourceSpec] = []
    previous_uri: str | None = None
    with path.open("rb") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            digest.update(raw_line)
            try:
                record = json.loads(raw_line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise StructuralQcError(
                    f"invalid source manifest JSON on line {line_number}"
                ) from exc
            if not isinstance(record, dict) or set(record) != _SOURCE_KEYS:
                raise StructuralQcError(f"invalid source manifest fields on line {line_number}")
            if raw_line != _canonical_line(record):
                raise StructuralQcError(
                    f"source manifest line {line_number} is not canonical JSONL"
                )
            uri, uri_date = _parse_uri(record["relative_uri"])
            if previous_uri is not None and uri <= previous_uri:
                raise StructuralQcError("source manifest URIs must be unique and sorted")
            previous_uri = uri
            source_date_text = record["source_date"]
            if not isinstance(source_date_text, str) or source_date_text != uri_date.isoformat():
                raise StructuralQcError(f"source manifest date disagrees for {uri}")
            source_sha = record["sha256"]
            if not isinstance(source_sha, str) or _SHA256.fullmatch(source_sha) is None:
                raise StructuralQcError(f"invalid source SHA-256 for {uri}")
            byte_size = _required_int(record["byte_size"], label=f"{uri} byte_size", minimum=0)

            relative = Path(*PurePosixPath(uri).parts)
            _no_symlinks(dataset_root, relative, label="source")
            source_path = (dataset_root / relative).resolve(strict=True)
            _contained(source_path, dataset_root, label="source")
            mode = source_path.lstat().st_mode
            if not stat.S_ISREG(mode):
                raise StructuralQcError(f"source is not a regular file: {source_path}")
            if source_path.stat().st_size != byte_size:
                raise StructuralQcError(f"source byte-size drift for {uri}")
            sources.append(
                _SourceSpec(
                    relative_uri=uri,
                    source_date=uri_date,
                    sha256=source_sha,
                    byte_size=byte_size,
                    path=source_path,
                )
            )
    if not sources:
        raise StructuralQcError("source manifest is empty")
    return path, digest.hexdigest(), sources


def _output_directory(data_root: Path) -> Path:
    current = data_root
    for component in ("derived", "manifests"):
        current /= component
        if current.exists() or current.is_symlink():
            if current.is_symlink() or not current.is_dir():
                raise StructuralQcError(f"unsafe output directory component: {current}")
        else:
            current.mkdir(mode=0o700)
    return current.resolve(strict=True)


def _output_path(directory: Path, name: str, *, label: str) -> Path:
    if not isinstance(name, str) or not name or Path(name).name != name or name in {".", ".."}:
        raise StructuralQcError(f"{label} must be one filename")
    if not name.endswith(".jsonl"):
        raise StructuralQcError(f"{label} must end in .jsonl")
    path = directory / name
    if path.is_symlink():
        raise StructuralQcError(f"{label} cannot be a symbolic link: {path}")
    return path


def _checkpoint_name(manifest_name: str) -> str:
    return f"{manifest_name.removesuffix('.jsonl')}.checkpoint.jsonl"


def _descriptor(path: Path) -> tuple[int, Any, _Identity]:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    handle = os.fdopen(descriptor, "rb", closefd=False)
    return descriptor, handle, _identity(os.fstat(descriptor))


def _assert_identity(
    descriptor: int,
    path: Path,
    expected: _Identity,
    *,
    relative_uri: str,
) -> None:
    if _identity(os.fstat(descriptor)) != expected:
        raise StructuralQcError(f"source changed while scanning: {relative_uri}")
    if path.is_symlink() or _identity(path.stat()) != expected:
        raise StructuralQcError(f"source path changed while scanning: {relative_uri}")


def _hash_open_file(handle: Any) -> str:
    handle.seek(0)
    digest = hashlib.sha256()
    while chunk := handle.read(8 * 1024 * 1024):
        digest.update(chunk)
    handle.seek(0)
    return digest.hexdigest()


def _metadata_int(metadata: Mapping[str, Any], key: str) -> int:
    return _required_int(metadata.get(key), label=f"dbn.metadata {key!r}", minimum=0)


def _file_context(
    parquet_file: pq.ParquetFile,
    source: _SourceSpec,
    config: StructuralQcConfig,
) -> _FileContext:
    schema = parquet_file.schema_arrow
    contract = validate_mbp10_contract(schema)
    if contract.undefined_price != config.undefined_price:
        raise StructuralQcError(f"undefined-price config drift for {source.relative_uri}")
    raw_metadata = (schema.metadata or {}).get(b"dbn.metadata")
    if raw_metadata is None:
        raise StructuralQcError(f"dbn.metadata is missing for {source.relative_uri}")
    dbn_metadata = decode_dbn_metadata(raw_metadata)
    request_start_ns = _metadata_int(dbn_metadata, "start")
    request_end_ns = _metadata_int(dbn_metadata, "end")
    if request_end_ns <= request_start_ns:
        raise StructuralQcError(f"invalid request interval for {source.relative_uri}")
    request_date = datetime.fromtimestamp(request_start_ns // 1_000_000_000, tz=UTC).date()
    if request_date != source.source_date:
        raise StructuralQcError(f"request start date disagrees for {source.relative_uri}")

    mappings = parse_instrument_mappings(raw_metadata)
    mapped_ids = frozenset(
        mapping.instrument_id
        for mapping in mappings
        if mapping.interval_start <= source.source_date < mapping.interval_end
    )
    if not mapped_ids:
        raise StructuralQcError(f"no active instrument mappings for {source.relative_uri}")
    metadata = parquet_file.metadata
    return _FileContext(
        schema_fingerprint=compute_schema_fingerprint(schema, contract),
        expected_rows=metadata.num_rows,
        expected_row_groups=metadata.num_row_groups,
        request_start_ns=request_start_ns,
        request_end_ns=request_end_ns,
        mapped_ids=mapped_ids,
    )


def _count(mask: Any) -> int:
    value = pc.sum(pc.fill_null(mask, False)).as_py()
    return int(value or 0)


def _column_numpy(table: pa.Table, name: str, fill_value: object) -> np.ndarray[Any, Any]:
    values = pc.fill_null(table[name], fill_value)
    if pa.types.is_timestamp(values.type):
        values = pc.cast(values, pa.int64())
    return np.asarray(values.to_numpy(zero_copy_only=False))


def _instrument_bounds(
    publisher: np.ndarray[Any, Any],
    instrument: np.ndarray[Any, Any],
    ts_recv: np.ndarray[Any, Any],
    flags: np.ndarray[Any, Any],
) -> tuple[int, int, int, list[list[int | None]]]:
    if len(ts_recv) == 0:
        return 0, 0, 0, []
    keys = (publisher.astype(np.uint64) << np.uint64(32)) | instrument.astype(np.uint64)
    order = np.argsort(keys, kind="stable")
    sorted_keys = keys[order]
    sorted_ts = ts_recv[order].astype(np.int64, copy=False)
    sorted_flags = flags[order].astype(np.uint8, copy=False)
    same = sorted_keys[1:] == sorted_keys[:-1]
    regression = same & (sorted_ts[1:] < sorted_ts[:-1])
    adjacent_trusted = ((sorted_flags[1:] | sorted_flags[:-1]) & F_BAD_TS_RECV) == 0
    all_regressions = int(np.count_nonzero(regression))
    exempted_regressions = int(np.count_nonzero(regression & ~adjacent_trusted))

    starts = np.concatenate(([0], np.flatnonzero(~same) + 1))
    ends = np.concatenate((starts[1:] - 1, [len(order) - 1]))
    trusted_regressions = 0
    bounds: list[list[int | None]] = []
    for start, end in zip(starts, ends, strict=True):
        key = int(sorted_keys[start])
        group_flags = sorted_flags[start : end + 1]
        group_ts = sorted_ts[start : end + 1]
        trusted_ts = group_ts[(group_flags & F_BAD_TS_RECV) == 0]
        trusted_regressions += int(np.count_nonzero(trusted_ts[1:] < trusted_ts[:-1]))
        bounds.append(
            [
                key >> 32,
                key & (2**32 - 1),
                int(sorted_ts[start]),
                int(sorted_flags[start]),
                int(sorted_ts[end]),
                int(sorted_flags[end]),
                int(trusted_ts[0]) if len(trusted_ts) else None,
                int(trusted_ts[-1]) if len(trusted_ts) else None,
            ]
        )
    return all_regressions, trusted_regressions, exempted_regressions, bounds


def _sequence_bounds(
    publisher: np.ndarray[Any, Any], sequence: np.ndarray[Any, Any]
) -> tuple[int, int, int, list[list[int]]]:
    if len(sequence) == 0:
        return 0, 0, 0, []
    order = np.argsort(publisher, kind="stable")
    sorted_publishers = publisher[order].astype(np.uint64, copy=False)
    sorted_sequence = sequence[order].astype(np.int64, copy=False)
    same = sorted_publishers[1:] == sorted_publishers[:-1]
    repeats = int(np.count_nonzero(same & (sorted_sequence[1:] == sorted_sequence[:-1])))
    regressions = int(np.count_nonzero(same & (sorted_sequence[1:] < sorted_sequence[:-1])))
    gaps = int(np.count_nonzero(same & (sorted_sequence[1:] > sorted_sequence[:-1] + 1)))
    starts = np.concatenate(([0], np.flatnonzero(~same) + 1))
    ends = np.concatenate((starts[1:] - 1, [len(order) - 1]))
    bounds = [
        [
            int(sorted_publishers[start]),
            int(sorted_sequence[start]),
            int(sorted_sequence[end]),
        ]
        for start, end in zip(starts, ends, strict=True)
    ]
    return repeats, regressions, gaps, bounds


def _event_keys(
    table: pa.Table,
    ts_event: np.ndarray[Any, Any],
    publisher: np.ndarray[Any, Any],
    instrument: np.ndarray[Any, Any],
    sequence: np.ndarray[Any, Any],
) -> tuple[int, list[object] | None, list[object] | None]:
    if table.num_rows == 0:
        return 0, None, None
    action = _column_numpy(table, "action", "")
    side = _column_numpy(table, "side", "")
    price = _column_numpy(table, "price", UNDEFINED_PRICE).astype(np.int64, copy=False)
    size = _column_numpy(table, "size", 0).astype(np.uint64, copy=False)
    repeated = (
        (ts_event[1:] == ts_event[:-1])
        & (publisher[1:] == publisher[:-1])
        & (instrument[1:] == instrument[:-1])
        & (sequence[1:] == sequence[:-1])
        & (action[1:] == action[:-1])
        & (side[1:] == side[:-1])
        & (price[1:] == price[:-1])
        & (size[1:] == size[:-1])
    )

    def key(index: int) -> list[object]:
        return [
            int(ts_event[index]),
            int(publisher[index]),
            int(instrument[index]),
            int(sequence[index]),
            str(action[index]),
            str(side[index]),
            int(price[index]),
            int(size[index]),
        ]

    return int(np.count_nonzero(repeated)), key(0), key(-1)


def _clean_trade_none_book_mutations(
    table: pa.Table,
    prior_state: _BookState | None,
) -> tuple[int, _BookState]:
    """Compare clean T/N books with the prior physical row for each instrument.

    A structurally valid snapshot or empty reset establishes a valid baseline.
    MAYBE_BAD_BOOK invalidates the instrument until a later valid snapshot/reset;
    ordinary rows cannot recover it. State remains file-local. A partial resume
    replays every completed row group in the current file before new evidence.
    """

    state = dict(prior_state or {})
    if table.num_rows == 0:
        return 0, state

    publisher = _column_numpy(table, "publisher_id", 0).astype(np.uint64, copy=False)
    instrument = _column_numpy(table, "instrument_id", 0).astype(np.uint64, copy=False)
    action = _column_numpy(table, "action", "")
    flags = _column_numpy(table, "flags", 0).astype(np.uint8, copy=False)
    keys = (publisher << np.uint64(32)) | instrument
    order = np.argsort(keys, kind="stable")
    sorted_keys = keys[order]
    same = sorted_keys[1:] == sorted_keys[:-1]
    previous_indexes = order[:-1]
    current_indexes = order[1:]
    book_arrays: dict[str, np.ndarray[Any, Any]] = {}
    for name in _BOOK_COLUMNS:
        fill = UNDEFINED_PRICE if "_px_" in name else 0
        values = _column_numpy(table, name, fill).astype(np.int64, copy=False)
        book_arrays[name] = values

    local_valid = np.ones(table.num_rows, dtype=np.bool_)
    book_empty = np.ones(table.num_rows, dtype=np.bool_)
    prior_bid_defined: np.ndarray[Any, Any] | None = None
    prior_ask_defined: np.ndarray[Any, Any] | None = None
    prior_bid_price: np.ndarray[Any, Any] | None = None
    prior_ask_price: np.ndarray[Any, Any] | None = None
    for level in range(10):
        suffix = f"{level:02d}"
        bid_price = book_arrays[f"bid_px_{suffix}"]
        ask_price = book_arrays[f"ask_px_{suffix}"]
        bid_size = book_arrays[f"bid_sz_{suffix}"]
        ask_size = book_arrays[f"ask_sz_{suffix}"]
        bid_count = book_arrays[f"bid_ct_{suffix}"]
        ask_count = book_arrays[f"ask_ct_{suffix}"]
        bid_defined = bid_price != UNDEFINED_PRICE
        ask_defined = ask_price != UNDEFINED_PRICE
        local_valid &= ~(
            (bid_defined & ((bid_size == 0) | (bid_count == 0) | (bid_count > bid_size)))
            | (ask_defined & ((ask_size == 0) | (ask_count == 0) | (ask_count > ask_size)))
            | (~bid_defined & ((bid_size != 0) | (bid_count != 0)))
            | (~ask_defined & ((ask_size != 0) | (ask_count != 0)))
        )
        book_empty &= (
            ~bid_defined
            & ~ask_defined
            & (bid_size == 0)
            & (ask_size == 0)
            & (bid_count == 0)
            & (ask_count == 0)
        )
        if prior_bid_defined is not None:
            local_valid &= ~(
                (~prior_bid_defined & bid_defined)
                | (~prior_ask_defined & ask_defined)
                | (prior_bid_defined & bid_defined & (bid_price >= prior_bid_price))
                | (prior_ask_defined & ask_defined & (ask_price <= prior_ask_price))
            )
        prior_bid_defined = bid_defined
        prior_ask_defined = ask_defined
        prior_bid_price = bid_price
        prior_ask_price = ask_price

    changed = np.zeros(len(current_indexes), dtype=np.bool_)
    for values in book_arrays.values():
        changed |= values[current_indexes] != values[previous_indexes]

    starts = np.concatenate(([0], np.flatnonzero(~same) + 1))
    ends = np.concatenate((starts[1:] - 1, [len(order) - 1]))

    def book_tuple(row_index: int) -> tuple[int, ...]:
        return tuple(int(values[row_index]) for values in book_arrays.values())

    mutations = 0
    for start, end in zip(starts, ends, strict=True):
        group_indexes = order[start : end + 1]
        key = int(sorted_keys[start])
        previous = state.get(key)
        starting_valid = previous[1] if previous is not None else False
        group_actions = action[group_indexes]
        group_flags = flags[group_indexes]
        group_local_valid = local_valid[group_indexes]
        group_empty = book_empty[group_indexes]
        maybe_bad = (group_flags & F_MAYBE_BAD_BOOK) != 0
        snapshot_recovery = ((group_flags & F_SNAPSHOT) != 0) & group_local_valid
        reset_recovery = (group_actions == "R") & group_local_valid & group_empty
        recovery = snapshot_recovery | reset_recovery
        invalid_row = (
            maybe_bad
            | ~group_local_valid
            | ((((group_flags & F_SNAPSHOT) != 0) | (group_actions == "R")) & ~recovery)
        )
        markers = np.where(invalid_row, -1, np.where(recovery, 1, 0))
        if previous is None and markers[0] == 0:
            # A source-local instrument can begin on an ordinary, structurally
            # valid full MBP row. It seeds state but cannot itself be compared.
            markers[0] = 1
        marker_locations = np.where(markers != 0, np.arange(len(markers)) + 1, 0)
        last_marker_locations = np.maximum.accumulate(marker_locations)
        valid = np.full(len(group_indexes), starting_valid, dtype=np.bool_)
        has_marker = last_marker_locations > 0
        valid[has_marker] = markers[last_marker_locations[has_marker] - 1] == 1

        if previous is not None:
            first_index = int(group_indexes[0])
            first_is_clean_trade_none = (
                action[first_index] in {"T", "N"}
                and (flags[first_index] & (F_SNAPSHOT | F_MAYBE_BAD_BOOK)) == 0
            )
            if (
                starting_valid
                and valid[0]
                and first_is_clean_trade_none
                and book_tuple(first_index) != previous[0]
            ):
                mutations += 1

        if len(group_indexes) > 1:
            group_current = group_indexes[1:]
            current_is_clean_trade_none = np.isin(action[group_current], ("T", "N")) & (
                (flags[group_current] & (F_SNAPSHOT | F_MAYBE_BAD_BOOK)) == 0
            )
            comparable = valid[:-1] & valid[1:] & current_is_clean_trade_none
            # ``changed`` follows the same stable key order, so this group slice
            # maps directly to its adjacent comparisons.
            mutations += int(np.count_nonzero(comparable & changed[start:end]))

        last_index = int(group_indexes[-1])
        state[key] = (book_tuple(last_index), bool(valid[-1]))
    return mutations, state


def _scan_row_group(
    table: pa.Table,
    *,
    config: StructuralQcConfig,
    context: _FileContext,
    prior_book_state: _BookState | None = None,
) -> dict[str, object]:
    hard = dict.fromkeys(config.hard_checks, 0)
    diagnostics = dict.fromkeys(config.diagnostic_checks, 0)

    hard["null_required_values"] = sum(table[name].null_count for name in QC_COLUMNS)
    valid_rtype = pc.is_valid(table["rtype"])
    hard["unexpected_rtype"] = _count(
        pc.and_(valid_rtype, pc.not_equal(table["rtype"], config.expected_rtype))
    )
    hard["unexpected_publisher_id"] = _count(
        pc.and_(
            pc.is_valid(table["publisher_id"]),
            pc.not_equal(table["publisher_id"], config.expected_publisher_id),
        )
    )
    action_values = pa.array(config.known_actions, type=pa.string())
    side_values = pa.array(config.known_sides, type=pa.string())
    hard["unknown_action"] = _count(
        pc.and_(pc.is_valid(table["action"]), pc.invert(pc.is_in(table["action"], action_values)))
    )
    hard["unknown_side"] = _count(
        pc.and_(pc.is_valid(table["side"]), pc.invert(pc.is_in(table["side"], side_values)))
    )
    hard["depth_out_of_range"] = _count(
        pc.and_(pc.is_valid(table["depth"]), pc.greater(table["depth"], config.maximum_depth))
    )
    mapped_values = pa.array(sorted(context.mapped_ids), type=pa.uint32())
    hard["unmapped_instrument_id"] = _count(
        pc.and_(
            pc.is_valid(table["instrument_id"]),
            pc.invert(pc.is_in(table["instrument_id"], mapped_values)),
        )
    )

    ts_recv = _column_numpy(table, "ts_recv", 0).astype(np.int64, copy=False)
    ts_event = _column_numpy(table, "ts_event", 0).astype(np.int64, copy=False)
    publisher = _column_numpy(table, "publisher_id", 0).astype(np.uint64, copy=False)
    instrument = _column_numpy(table, "instrument_id", 0).astype(np.uint64, copy=False)
    flags = _column_numpy(table, "flags", 0).astype(np.uint8, copy=False)
    sequence = _column_numpy(table, "sequence", 0).astype(np.uint64, copy=False)
    ts_in_delta = _column_numpy(table, "ts_in_delta", 0).astype(np.int64, copy=False)

    hard["ts_recv_outside_request_range"] = int(
        np.count_nonzero((ts_recv < context.request_start_ns) | (ts_recv >= context.request_end_ns))
    )
    diagnostics["publisher_id_zero"] = int(np.count_nonzero(publisher == 0))
    diagnostics["global_ts_recv_regression"] = int(np.count_nonzero(ts_recv[1:] < ts_recv[:-1]))
    all_regressions, trusted_regressions, exempted, instrument_bounds = _instrument_bounds(
        publisher, instrument, ts_recv, flags
    )
    diagnostics["all_ts_recv_regression_per_instrument"] = all_regressions
    diagnostics["bad_ts_recv_exempted_regression_per_instrument"] = exempted
    hard["trusted_ts_recv_regression_per_instrument"] = trusted_regressions
    diagnostics["ts_recv_before_ts_event"] = int(np.count_nonzero(ts_recv < ts_event))
    diagnostics["global_ts_event_regression"] = int(np.count_nonzero(ts_event[1:] < ts_event[:-1]))
    diagnostics["ts_event_outside_request_range"] = int(
        np.count_nonzero(
            (ts_event < context.request_start_ns) | (ts_event >= context.request_end_ns)
        )
    )
    diagnostics["negative_ts_in_delta"] = int(np.count_nonzero(ts_in_delta < 0))
    repeats, regressions, gaps, sequence_bounds = _sequence_bounds(publisher, sequence)
    diagnostics["sequence_repeat"] = repeats
    diagnostics["sequence_regression"] = regressions
    diagnostics["sequence_forward_gap"] = gaps
    adjacent_repeats, first_event_key, last_event_key = _event_keys(
        table, ts_event, publisher, instrument, sequence
    )
    diagnostics["adjacent_event_key_repeat"] = adjacent_repeats
    book_mutations, book_state = _clean_trade_none_book_mutations(table, prior_book_state)
    hard["clean_trade_none_book_mutation"] = book_mutations

    action = table["action"]
    diagnostics["reset_action_rows"] = _count(pc.equal(action, "R"))
    reset_rows = pc.equal(action, "R")
    hard["reset_side_not_none"] = _count(pc.and_(reset_rows, pc.not_equal(table["side"], "N")))
    diagnostics["snapshot_flag_rows"] = int(np.count_nonzero((flags & F_SNAPSHOT) != 0))
    diagnostics["maybe_bad_book_flag_rows"] = int(np.count_nonzero((flags & F_MAYBE_BAD_BOOK) != 0))
    diagnostics["bad_ts_recv_flag_rows"] = int(np.count_nonzero((flags & F_BAD_TS_RECV) != 0))
    event_price_undefined = pc.equal(table["price"], config.undefined_price)
    diagnostics["undefined_event_price_rows"] = _count(event_price_undefined)
    diagnostics["undefined_event_price_non_reset_rows"] = _count(
        pc.and_(event_price_undefined, pc.not_equal(action, "R"))
    )

    prior_bid_defined: Any | None = None
    prior_ask_defined: Any | None = None
    prior_bid_price: Any | None = None
    prior_ask_price: Any | None = None
    reset_book_nonempty: Any = pa.array(np.zeros(table.num_rows, dtype=np.bool_()))
    bbo_bid_defined: Any | None = None
    bbo_ask_defined: Any | None = None
    bbo_bid_price: Any | None = None
    bbo_ask_price: Any | None = None
    for level in range(config.book_levels):
        suffix = f"{level:02d}"
        bid_price = table[f"bid_px_{suffix}"]
        ask_price = table[f"ask_px_{suffix}"]
        bid_size = table[f"bid_sz_{suffix}"]
        ask_size = table[f"ask_sz_{suffix}"]
        bid_count = table[f"bid_ct_{suffix}"]
        ask_count = table[f"ask_ct_{suffix}"]
        bid_defined = pc.and_(
            pc.is_valid(bid_price), pc.not_equal(bid_price, config.undefined_price)
        )
        ask_defined = pc.and_(
            pc.is_valid(ask_price), pc.not_equal(ask_price, config.undefined_price)
        )
        reset_book_nonempty = pc.or_(
            reset_book_nonempty,
            pc.or_(
                pc.or_(bid_defined, ask_defined),
                pc.or_(
                    pc.or_(pc.not_equal(bid_size, 0), pc.not_equal(ask_size, 0)),
                    pc.or_(pc.not_equal(bid_count, 0), pc.not_equal(ask_count, 0)),
                ),
            ),
        )

        hard["defined_book_price_zero_size"] += _count(
            pc.or_(
                pc.and_(bid_defined, pc.equal(bid_size, 0)),
                pc.and_(ask_defined, pc.equal(ask_size, 0)),
            )
        )
        hard["defined_book_price_zero_count"] += _count(
            pc.or_(
                pc.and_(bid_defined, pc.equal(bid_count, 0)),
                pc.and_(ask_defined, pc.equal(ask_count, 0)),
            )
        )
        hard["defined_book_count_exceeds_size"] += _count(
            pc.or_(
                pc.and_(bid_defined, pc.greater(bid_count, bid_size)),
                pc.and_(ask_defined, pc.greater(ask_count, ask_size)),
            )
        )
        hard["undefined_book_price_nonzero_size"] += _count(
            pc.or_(
                pc.and_(pc.invert(bid_defined), pc.not_equal(bid_size, 0)),
                pc.and_(pc.invert(ask_defined), pc.not_equal(ask_size, 0)),
            )
        )
        hard["undefined_book_price_nonzero_count"] += _count(
            pc.or_(
                pc.and_(pc.invert(bid_defined), pc.not_equal(bid_count, 0)),
                pc.and_(pc.invert(ask_defined), pc.not_equal(ask_count, 0)),
            )
        )
        if prior_bid_defined is not None:
            hard["book_level_noncontiguous"] += _count(
                pc.or_(
                    pc.and_(pc.invert(prior_bid_defined), bid_defined),
                    pc.and_(pc.invert(prior_ask_defined), ask_defined),
                )
            )
            hard["bid_ladder_not_strictly_descending"] += _count(
                pc.and_(
                    pc.and_(prior_bid_defined, bid_defined),
                    pc.greater_equal(bid_price, prior_bid_price),
                )
            )
            hard["ask_ladder_not_strictly_ascending"] += _count(
                pc.and_(
                    pc.and_(prior_ask_defined, ask_defined),
                    pc.less_equal(ask_price, prior_ask_price),
                )
            )
        else:
            bbo_bid_defined = bid_defined
            bbo_ask_defined = ask_defined
            bbo_bid_price = bid_price
            bbo_ask_price = ask_price
        prior_bid_defined = bid_defined
        prior_ask_defined = ask_defined
        prior_bid_price = bid_price
        prior_ask_price = ask_price

    assert bbo_bid_defined is not None
    assert bbo_ask_defined is not None
    assert bbo_bid_price is not None
    assert bbo_ask_price is not None
    valid_bbo = pc.and_(bbo_bid_defined, bbo_ask_defined)
    diagnostics["incomplete_bbo_rows"] = _count(pc.invert(valid_bbo))
    diagnostics["locked_bbo_rows"] = _count(
        pc.and_(valid_bbo, pc.equal(bbo_bid_price, bbo_ask_price))
    )
    diagnostics["crossed_bbo_rows"] = _count(
        pc.and_(valid_bbo, pc.greater(bbo_bid_price, bbo_ask_price))
    )
    hard["reset_book_not_empty"] = _count(pc.and_(reset_rows, reset_book_nonempty))

    return {
        "diagnostic_counts": diagnostics,
        "first_event_key": first_event_key,
        "first_ts_event_ns": int(ts_event[0]) if len(ts_event) else None,
        "first_ts_recv_ns": int(ts_recv[0]) if len(ts_recv) else None,
        "hard_violation_counts": hard,
        "instrument_ts_recv_bounds": instrument_bounds,
        "last_event_key": last_event_key,
        "last_ts_event_ns": int(ts_event[-1]) if len(ts_event) else None,
        "last_ts_recv_ns": int(ts_recv[-1]) if len(ts_recv) else None,
        "publisher_sequence_bounds": sequence_bounds,
        "_book_state": book_state,
    }


def _validate_counter_map(
    value: object, expected_keys: Sequence[str], *, label: str
) -> dict[str, int]:
    if not isinstance(value, dict) or tuple(value) != tuple(sorted(expected_keys)):
        # Canonical JSON sorts object keys, so parsed insertion order is sorted.
        raise StructuralQcError(f"{label} has invalid counter fields")
    result: dict[str, int] = {}
    for key in expected_keys:
        result[key] = _required_int(value.get(key), label=f"{label}.{key}", minimum=0)
    return result


def _identity_from_record(value: object, *, label: str) -> _Identity:
    if not isinstance(value, dict) or set(value) != _IDENTITY_KEYS:
        raise StructuralQcError(f"{label} has invalid source identity")
    return _Identity(
        size=_required_int(value["size"], label=f"{label}.size", minimum=0),
        mtime_ns=_required_int(value["mtime_ns"], label=f"{label}.mtime_ns", minimum=0),
        ctime_ns=_required_int(value["ctime_ns"], label=f"{label}.ctime_ns", minimum=0),
        device=_required_int(value["device"], label=f"{label}.device", minimum=0),
        inode=_required_int(value["inode"], label=f"{label}.inode", minimum=0),
    )


def _checkpoint_records(path: Path) -> Iterator[tuple[int, dict[str, object]]]:
    previous_sha256 = _EMPTY_CHECKPOINT_SHA256
    with path.open("rb") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            try:
                record = json.loads(raw_line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise StructuralQcError(f"invalid checkpoint JSON on line {line_number}") from exc
            if not isinstance(record, dict) or set(record) != _CHECKPOINT_KEYS:
                raise StructuralQcError(f"invalid checkpoint fields on line {line_number}")
            if raw_line != _canonical_line(record):
                raise StructuralQcError(f"checkpoint line {line_number} is not canonical JSONL")
            claimed_previous = record["previous_checkpoint_sha256"]
            claimed_record = record["checkpoint_record_sha256"]
            if (
                not isinstance(claimed_previous, str)
                or _SHA256.fullmatch(claimed_previous) is None
                or not isinstance(claimed_record, str)
                or _SHA256.fullmatch(claimed_record) is None
            ):
                raise StructuralQcError(
                    f"checkpoint line {line_number} has an invalid SHA-256 chain"
                )
            if claimed_previous != previous_sha256:
                raise StructuralQcError(
                    f"checkpoint line {line_number} previous SHA-256 chain drift"
                )
            if claimed_record != _checkpoint_record_sha256(record):
                raise StructuralQcError(f"checkpoint line {line_number} record SHA-256 drift")
            previous_sha256 = claimed_record
            yield line_number, record


def _recover_partial_checkpoint_tail(path: Path) -> None:
    """Atomically discard only a non-newline-terminated crash tail.

    A malformed newline-terminated line or any corruption before the final
    incomplete line remains a hard error in ``_checkpoint_records``.
    """

    if not path.exists():
        return
    if path.is_symlink() or not path.is_file():
        raise StructuralQcError(f"checkpoint must be a regular file: {path}")
    file_size = path.stat().st_size
    if file_size == 0:
        return
    with path.open("rb") as handle:
        handle.seek(-1, os.SEEK_END)
        if handle.read(1) == b"\n":
            return
        complete_size = 0
        cursor = file_size
        while cursor > 0:
            read_size = min(64 * 1024, cursor)
            cursor -= read_size
            handle.seek(cursor)
            chunk = handle.read(read_size)
            newline = chunk.rfind(b"\n")
            if newline >= 0:
                complete_size = cursor + newline + 1
                break

    temporary_handle = tempfile.NamedTemporaryFile(  # noqa: SIM115
        mode="w+b",
        prefix=f".{path.name}.",
        suffix=".repair.tmp",
        dir=path.parent,
        delete=False,
    )
    temporary = Path(temporary_handle.name)
    try:
        with path.open("rb") as source_handle:
            remaining = complete_size
            while remaining:
                chunk = source_handle.read(min(8 * 1024 * 1024, remaining))
                if not chunk:
                    raise StructuralQcError("checkpoint ended before its valid prefix")
                temporary_handle.write(chunk)
                remaining -= len(chunk)
        temporary_handle.flush()
        os.fsync(temporary_handle.fileno())
        temporary_handle.close()
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        temporary_handle.close()
        temporary.unlink(missing_ok=True)
        raise


def _validate_checkpoint_line(
    record: dict[str, object],
    *,
    source: _SourceSpec,
    config: StructuralQcConfig,
    source_manifest_sha256: str,
    line_number: int,
) -> tuple[int, int, int, int, str, _Identity]:
    label = f"checkpoint line {line_number}"
    expected_values = {
        "artifact_schema": config.checkpoint_schema,
        "checker_version": config.checker_version,
        "config_sha256": config.sha256,
        "source_manifest_sha256": source_manifest_sha256,
        "relative_uri": source.relative_uri,
        "source_date": source.source_date.isoformat(),
        "source_sha256": source.sha256,
        "source_byte_size": source.byte_size,
    }
    for key, expected in expected_values.items():
        if record[key] != expected:
            raise StructuralQcError(f"{label} lineage drift in {key}")
    row_group_index = _required_int(
        record["row_group_index"], label=f"{label}.row_group_index", minimum=0
    )
    row_count = _required_int(record["row_count"], label=f"{label}.row_count", minimum=0)
    expected_groups = _required_int(
        record["expected_row_group_count"],
        label=f"{label}.expected_row_group_count",
        minimum=1,
    )
    expected_rows = _required_int(
        record["expected_row_count"], label=f"{label}.expected_row_count", minimum=0
    )
    schema_fingerprint = record["schema_fingerprint"]
    if not isinstance(schema_fingerprint, str) or _SHA256.fullmatch(schema_fingerprint) is None:
        raise StructuralQcError(f"{label} has invalid schema_fingerprint")
    _validate_counter_map(record["hard_violation_counts"], config.hard_checks, label=label)
    _validate_counter_map(record["diagnostic_counts"], config.diagnostic_checks, label=label)
    identity = _identity_from_record(record["source_identity"], label=label)
    return row_group_index, row_count, expected_groups, expected_rows, schema_fingerprint, identity


def _load_checkpoint(
    path: Path,
    *,
    sources: Sequence[_SourceSpec],
    config: StructuralQcConfig,
    source_manifest_sha256: str,
    progress_callback: ProgressCallback | None,
) -> tuple[dict[str, _ResumeFile], int, int, str]:
    if not path.exists():
        return {}, 0, 0, _EMPTY_CHECKPOINT_SHA256
    _recover_partial_checkpoint_tail(path)

    source_indexes = {source.relative_uri: index for index, source in enumerate(sources)}
    summaries: dict[str, _ResumeFile] = {}
    current_source_index = -1
    resumed_groups = 0
    resumed_rows = 0
    last_checkpoint_sha256 = _EMPTY_CHECKPOINT_SHA256
    for line_number, record in _checkpoint_records(path):
        uri = record["relative_uri"]
        if not isinstance(uri, str) or uri not in source_indexes:
            raise StructuralQcError(f"checkpoint line {line_number} has an unknown source URI")
        source_index = source_indexes[uri]
        if source_index not in {current_source_index, current_source_index + 1}:
            raise StructuralQcError("checkpoint is not a contiguous source/row-group prefix")
        if source_index == current_source_index + 1:
            if current_source_index >= 0:
                prior = summaries[sources[current_source_index].relative_uri]
                if prior.row_groups != prior.expected_row_groups:
                    raise StructuralQcError("checkpoint skips an incomplete source file")
            current_source_index = source_index

        source = sources[source_index]
        values = _validate_checkpoint_line(
            record,
            source=source,
            config=config,
            source_manifest_sha256=source_manifest_sha256,
            line_number=line_number,
        )
        row_group_index, row_count, expected_groups, expected_rows, fingerprint, identity = values
        summary = summaries.get(uri)
        if summary is None:
            if row_group_index != 0:
                raise StructuralQcError(f"checkpoint for {uri} does not start at row group 0")
            current_identity = _identity(source.path.stat())
            if current_identity != identity:
                raise StructuralQcError(f"checkpoint source identity drift for {uri}")
            summary = _ResumeFile(
                0,
                0,
                [],
                expected_groups,
                expected_rows,
                fingerprint,
                identity,
            )
            summaries[uri] = summary
        if row_group_index != summary.row_groups:
            raise StructuralQcError(f"checkpoint row-group order drift for {uri}")
        if (
            expected_groups != summary.expected_row_groups
            or expected_rows != summary.expected_rows
            or fingerprint != summary.schema_fingerprint
            or identity != summary.identity
        ):
            raise StructuralQcError(f"checkpoint file identity drift for {uri}")
        if row_group_index >= expected_groups:
            raise StructuralQcError(f"checkpoint has excess row groups for {uri}")
        summary.row_groups += 1
        summary.rows += row_count
        summary.row_group_rows.append(row_count)
        last_checkpoint_sha256 = str(record["checkpoint_record_sha256"])
        resumed_groups += 1
        resumed_rows += row_count
        if progress_callback is not None:
            progress_callback(
                StructuralQcProgress(
                    status="RESUMED",
                    file_index=source_index + 1,
                    file_count=len(sources),
                    relative_uri=uri,
                    row_group_index=row_group_index,
                    row_groups_in_file=expected_groups,
                    row_groups_complete=resumed_groups,
                    total_row_groups=0,
                    rows_complete=resumed_rows,
                )
            )
    return summaries, resumed_groups, resumed_rows, last_checkpoint_sha256


def _append_checkpoint(path: Path, record: Mapping[str, object]) -> None:
    if path.is_symlink():
        raise StructuralQcError(f"refusing symbolic-link checkpoint: {path}")
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    payload = _canonical_line(record)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short checkpoint write")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _checkpoint_record(
    *,
    source: _SourceSpec,
    identity: _Identity,
    context: _FileContext,
    config: StructuralQcConfig,
    source_manifest_sha256: str,
    previous_checkpoint_sha256: str,
    row_group_index: int,
    row_count: int,
    metrics: Mapping[str, object],
) -> dict[str, object]:
    record: dict[str, object] = {
        "artifact_schema": config.checkpoint_schema,
        "checker_version": config.checker_version,
        "config_sha256": config.sha256,
        "diagnostic_counts": metrics["diagnostic_counts"],
        "expected_row_count": context.expected_rows,
        "expected_row_group_count": context.expected_row_groups,
        "first_event_key": metrics["first_event_key"],
        "first_ts_event_ns": metrics["first_ts_event_ns"],
        "first_ts_recv_ns": metrics["first_ts_recv_ns"],
        "hard_violation_counts": metrics["hard_violation_counts"],
        "instrument_ts_recv_bounds": metrics["instrument_ts_recv_bounds"],
        "last_event_key": metrics["last_event_key"],
        "last_ts_event_ns": metrics["last_ts_event_ns"],
        "last_ts_recv_ns": metrics["last_ts_recv_ns"],
        "publisher_sequence_bounds": metrics["publisher_sequence_bounds"],
        "previous_checkpoint_sha256": previous_checkpoint_sha256,
        "relative_uri": source.relative_uri,
        "row_count": row_count,
        "row_group_index": row_group_index,
        "schema_fingerprint": context.schema_fingerprint,
        "source_byte_size": source.byte_size,
        "source_date": source.source_date.isoformat(),
        "source_identity": identity.as_dict(),
        "source_manifest_sha256": source_manifest_sha256,
        "source_sha256": source.sha256,
    }
    record["checkpoint_record_sha256"] = _checkpoint_record_sha256(record)
    return record


def _add_counts(target: dict[str, int], source: Mapping[str, object]) -> None:
    for key in target:
        target[key] += int(source[key])


def _final_record(
    records: Sequence[dict[str, object]],
    *,
    source: _SourceSpec,
    config: StructuralQcConfig,
    source_manifest_sha256: str,
) -> dict[str, object]:
    if not records:
        raise StructuralQcError(f"no checkpoint rows for source: {source.relative_uri}")
    first = records[0]
    expected_groups = int(first["expected_row_group_count"])
    expected_rows = int(first["expected_row_count"])
    hard = dict.fromkeys(config.hard_checks, 0)
    diagnostics = dict.fromkeys(config.diagnostic_checks, 0)
    prior_instrument: dict[tuple[int, int], tuple[int, int]] = {}
    prior_trusted_ts_recv: dict[tuple[int, int], int] = {}
    prior_sequence: dict[int, int] = {}
    prior_ts_recv: int | None = None
    prior_ts_event: int | None = None
    prior_event_key: list[object] | None = None
    scanned_rows = 0

    for expected_index, record in enumerate(records):
        if int(record["row_group_index"]) != expected_index:
            raise StructuralQcError(f"row-group gap while publishing {source.relative_uri}")
        scanned_rows += int(record["row_count"])
        _add_counts(hard, record["hard_violation_counts"])
        _add_counts(diagnostics, record["diagnostic_counts"])

        first_recv = record["first_ts_recv_ns"]
        first_event = record["first_ts_event_ns"]
        if prior_ts_recv is not None and first_recv is not None and int(first_recv) < prior_ts_recv:
            diagnostics["global_ts_recv_regression"] += 1
        if (
            prior_ts_event is not None
            and first_event is not None
            and int(first_event) < prior_ts_event
        ):
            diagnostics["global_ts_event_regression"] += 1
        if prior_event_key is not None and record["first_event_key"] == prior_event_key:
            diagnostics["adjacent_event_key_repeat"] += 1

        for bound in record["instrument_ts_recv_bounds"]:
            if not isinstance(bound, list) or len(bound) != 8:
                raise StructuralQcError(
                    f"invalid instrument timestamp bound for {source.relative_uri}"
                )
            publisher_id, instrument_id, start_ts, start_flags, end_ts, end_flags = map(
                int, bound[:6]
            )
            first_trusted = int(bound[6]) if bound[6] is not None else None
            last_trusted = int(bound[7]) if bound[7] is not None else None
            key = (publisher_id, instrument_id)
            previous = prior_instrument.get(key)
            if previous is not None and start_ts < previous[0]:
                diagnostics["all_ts_recv_regression_per_instrument"] += 1
                if ((start_flags | previous[1]) & F_BAD_TS_RECV) != 0:
                    diagnostics["bad_ts_recv_exempted_regression_per_instrument"] += 1
            prior_instrument[key] = (end_ts, end_flags)
            previous_trusted = prior_trusted_ts_recv.get(key)
            if (
                previous_trusted is not None
                and first_trusted is not None
                and first_trusted < previous_trusted
            ):
                hard["trusted_ts_recv_regression_per_instrument"] += 1
            if last_trusted is not None:
                prior_trusted_ts_recv[key] = last_trusted

        for bound in record["publisher_sequence_bounds"]:
            publisher_id, start_sequence, end_sequence = map(int, bound)
            previous_sequence = prior_sequence.get(publisher_id)
            if previous_sequence is not None:
                if start_sequence == previous_sequence:
                    diagnostics["sequence_repeat"] += 1
                elif start_sequence < previous_sequence:
                    diagnostics["sequence_regression"] += 1
                elif start_sequence > previous_sequence + 1:
                    diagnostics["sequence_forward_gap"] += 1
            prior_sequence[publisher_id] = end_sequence

        if record["last_ts_recv_ns"] is not None:
            prior_ts_recv = int(record["last_ts_recv_ns"])
        if record["last_ts_event_ns"] is not None:
            prior_ts_event = int(record["last_ts_event_ns"])
        prior_event_key = record["last_event_key"]

    coverage_complete = len(records) == expected_groups and scanned_rows == expected_rows
    hard_total = sum(hard.values())
    return {
        "artifact_schema": config.artifact_schema,
        "checker_version": config.checker_version,
        "config_sha256": config.sha256,
        "coverage_complete": coverage_complete,
        "diagnostic_counts": diagnostics,
        "expected_row_count": expected_rows,
        "expected_row_group_count": expected_groups,
        "first_ts_recv_ns": records[0]["first_ts_recv_ns"],
        "hard_violation_count": hard_total,
        "hard_violation_counts": hard,
        "last_ts_recv_ns": records[-1]["last_ts_recv_ns"],
        "relative_uri": source.relative_uri,
        "research_eligible": False,
        "result": "PASS" if coverage_complete and hard_total == 0 else "FAIL",
        "scanned_row_count": scanned_rows,
        "scanned_row_group_count": len(records),
        "schema_fingerprint": first["schema_fingerprint"],
        "source_byte_size": source.byte_size,
        "source_date": source.source_date.isoformat(),
        "source_manifest_sha256": source_manifest_sha256,
        "source_sha256": source.sha256,
    }


def _atomic_publish(
    manifest_path: Path,
    checkpoint_path: Path,
    *,
    sources: Sequence[_SourceSpec],
    config: StructuralQcConfig,
    source_manifest_sha256: str,
) -> tuple[str, int, int, int, int, int, int]:
    if manifest_path.is_symlink():
        raise StructuralQcError(f"refusing symbolic-link manifest: {manifest_path}")
    if manifest_path.exists() and not manifest_path.is_file():
        raise StructuralQcError(f"manifest must be a regular file: {manifest_path}")
    handle = tempfile.NamedTemporaryFile(  # noqa: SIM115
        mode="w+b",
        prefix=f".{manifest_path.name}.",
        suffix=".tmp",
        dir=manifest_path.parent,
        delete=False,
    )
    temporary = Path(handle.name)
    digest = hashlib.sha256()
    byte_size = 0
    passed = 0
    failed = 0
    row_groups = 0
    rows = 0
    hard_total = 0
    source_index = 0
    current_records: list[dict[str, object]] = []

    def publish_current() -> None:
        nonlocal byte_size, passed, failed, row_groups, rows, hard_total, source_index
        if not current_records:
            return
        source = sources[source_index]
        final = _final_record(
            current_records,
            source=source,
            config=config,
            source_manifest_sha256=source_manifest_sha256,
        )
        if not final["coverage_complete"]:
            raise StructuralQcError(f"incomplete checkpoint coverage for {source.relative_uri}")
        line = _canonical_line(final)
        handle.write(line)
        digest.update(line)
        byte_size += len(line)
        passed += final["result"] == "PASS"
        failed += final["result"] == "FAIL"
        row_groups += int(final["scanned_row_group_count"])
        rows += int(final["scanned_row_count"])
        hard_total += int(final["hard_violation_count"])
        source_index += 1
        current_records.clear()

    try:
        for _, record in _checkpoint_records(checkpoint_path):
            if source_index >= len(sources):
                raise StructuralQcError("checkpoint has records beyond the source manifest")
            expected_uri = sources[source_index].relative_uri
            if record["relative_uri"] != expected_uri:
                publish_current()
                if (
                    source_index >= len(sources)
                    or record["relative_uri"] != sources[source_index].relative_uri
                ):
                    raise StructuralQcError("checkpoint source order drift during publication")
            current_records.append(record)
        publish_current()
        if source_index != len(sources):
            raise StructuralQcError("checkpoint does not cover every source file")
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()

        def winner_is_identical() -> bool:
            flags = os.O_RDONLY
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            try:
                descriptor = os.open(manifest_path, flags)
            except OSError as exc:
                raise StructuralQcError(
                    f"existing immutable QC manifest is unsafe: {manifest_path}"
                ) from exc
            try:
                winner_stat = os.fstat(descriptor)
                if not stat.S_ISREG(winner_stat.st_mode):
                    raise StructuralQcError(
                        f"existing immutable QC manifest is not regular: {manifest_path}"
                    )
                if winner_stat.st_size != temporary.stat().st_size:
                    return False
                with (
                    os.fdopen(descriptor, "rb", closefd=False) as existing,
                    temporary.open("rb") as candidate,
                ):
                    while True:
                        existing_chunk = existing.read(8 * 1024 * 1024)
                        candidate_chunk = candidate.read(8 * 1024 * 1024)
                        if existing_chunk != candidate_chunk:
                            return False
                        if not existing_chunk:
                            return True
            finally:
                os.close(descriptor)

        published = False
        try:
            os.link(temporary, manifest_path, follow_symlinks=False)
            published = True
        except FileExistsError:
            if not winner_is_identical():
                raise StructuralQcError(
                    f"existing immutable QC manifest content drift: {manifest_path}"
                )
        temporary.unlink()
        if published:
            directory_fd = os.open(manifest_path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    except BaseException:
        handle.close()
        temporary.unlink(missing_ok=True)
        raise
    return digest.hexdigest(), byte_size, passed, failed, row_groups, rows, hard_total


def scan_structural_quality(
    data_root: Path | str,
    *,
    config_path: Path | str,
    source_manifest_path: Path | str | None = None,
    dataset_root: Path | str | None = None,
    manifest_name: str = DEFAULT_MANIFEST_NAME,
    checkpoint_name: str | None = None,
    progress_callback: ProgressCallback | None = None,
) -> StructuralQcReport:
    """Scan every source row group and publish deterministic file-level JSONL.

    The source content SHA is checked before a file's first row group. A resume
    trusts only a checkpoint whose recorded SHA still equals the immutable input
    manifest and whose filesystem identity is unchanged. Identity is rechecked
    before and after every subsequently scanned row group.
    """

    root = _strict_root(data_root, label="data_root")
    requested_dataset = (
        Path(dataset_root).expanduser() if dataset_root is not None else root / "mbp-10"
    )
    if requested_dataset.is_symlink():
        raise StructuralQcError("dataset_root cannot be a symbolic link")
    source_root = requested_dataset.resolve(strict=True)
    relative_dataset = _contained(source_root, root, label="dataset_root")
    if not relative_dataset.parts or relative_dataset.parts[0] == "derived":
        raise StructuralQcError("dataset_root must be a raw-data child outside data/derived")
    _no_symlinks(root, relative_dataset, label="dataset_root")
    if not source_root.is_dir():
        raise NotADirectoryError(f"dataset_root is not a directory: {source_root}")

    resolved_config_path, _ = _config_table(config_path)
    config = load_structural_qc_config(resolved_config_path)
    outputs = _output_directory(root)
    source_manifest = source_manifest_path or outputs / DEFAULT_SOURCE_MANIFEST_NAME
    resolved_source_manifest, source_manifest_sha256, sources = _load_source_manifest(
        root, source_root, source_manifest
    )
    manifest_path = _output_path(outputs, manifest_name, label="manifest_name")
    checkpoint_path = _output_path(
        outputs,
        checkpoint_name or _checkpoint_name(manifest_name),
        label="checkpoint_name",
    )
    if manifest_path == checkpoint_path or manifest_path == resolved_source_manifest:
        raise StructuralQcError("input, checkpoint, and final manifest paths must differ")

    resume, resumed_groups, resumed_rows, previous_checkpoint_sha256 = _load_checkpoint(
        checkpoint_path,
        sources=sources,
        config=config,
        source_manifest_sha256=source_manifest_sha256,
        progress_callback=progress_callback,
    )
    scanned_groups = 0
    rows_complete = resumed_rows
    total_groups = sum(item.expected_row_groups for item in resume.values())

    for file_index, source in enumerate(sources, start=1):
        descriptor, handle, opened_identity = _descriptor(source.path)
        try:
            if opened_identity.size != source.byte_size:
                raise StructuralQcError(f"source size drift for {source.relative_uri}")
            summary = resume.get(source.relative_uri)
            if summary is None:
                if _hash_open_file(handle) != source.sha256:
                    raise StructuralQcError(f"source SHA-256 drift for {source.relative_uri}")
            elif summary.identity != opened_identity:
                raise StructuralQcError(
                    f"checkpoint source identity drift for {source.relative_uri}"
                )
            handle.seek(0)
            parquet_file = pq.ParquetFile(handle)
            context = _file_context(parquet_file, source, config)
            total_groups += context.expected_row_groups - (
                summary.expected_row_groups if summary is not None else 0
            )
            start_group = summary.row_groups if summary is not None else 0
            if summary is not None and (
                summary.expected_rows != context.expected_rows
                or summary.expected_row_groups != context.expected_row_groups
                or summary.schema_fingerprint != context.schema_fingerprint
            ):
                raise StructuralQcError(f"checkpoint footer drift for {source.relative_uri}")
            if summary is not None:
                for saved_index, saved_rows in enumerate(summary.row_group_rows):
                    footer_rows = parquet_file.metadata.row_group(saved_index).num_rows
                    if saved_rows != footer_rows:
                        raise StructuralQcError(
                            "checkpoint row-count disagrees with Parquet footer for "
                            f"{source.relative_uri} row group {saved_index}"
                        )
            if context.expected_row_groups < 1:
                raise StructuralQcError(f"source has no row groups: {source.relative_uri}")

            book_state: _BookState = {}
            if 0 < start_group < context.expected_row_groups:
                # A partial resume must reconstruct the last physical book row
                # for every instrument, including instruments absent from the
                # immediately preceding row group. Replay is state-only; saved
                # evidence is neither recounted nor rewritten.
                for replay_index in range(start_group):
                    _assert_identity(
                        descriptor,
                        source.path,
                        opened_identity,
                        relative_uri=source.relative_uri,
                    )
                    replay_table = parquet_file.read_row_group(
                        replay_index,
                        columns=list(BOOK_STATE_COLUMNS),
                        use_threads=False,
                    )
                    if replay_table.num_rows != summary.row_group_rows[replay_index]:
                        raise StructuralQcError(
                            "state replay row-count drift for "
                            f"{source.relative_uri} row group {replay_index}"
                        )
                    _, book_state = _clean_trade_none_book_mutations(replay_table, book_state)
                _assert_identity(
                    descriptor, source.path, opened_identity, relative_uri=source.relative_uri
                )

            for row_group_index in range(start_group, context.expected_row_groups):
                _assert_identity(
                    descriptor, source.path, opened_identity, relative_uri=source.relative_uri
                )
                table = parquet_file.read_row_group(
                    row_group_index, columns=list(QC_COLUMNS), use_threads=False
                )
                expected_group_rows = parquet_file.metadata.row_group(row_group_index).num_rows
                if table.num_rows != expected_group_rows:
                    raise StructuralQcError(
                        f"row-group row-count drift for {source.relative_uri} group {row_group_index}"
                    )
                metrics = _scan_row_group(
                    table,
                    config=config,
                    context=context,
                    prior_book_state=book_state,
                )
                book_state = metrics.pop("_book_state")
                _assert_identity(
                    descriptor, source.path, opened_identity, relative_uri=source.relative_uri
                )
                record = _checkpoint_record(
                    source=source,
                    identity=opened_identity,
                    context=context,
                    config=config,
                    source_manifest_sha256=source_manifest_sha256,
                    previous_checkpoint_sha256=previous_checkpoint_sha256,
                    row_group_index=row_group_index,
                    row_count=table.num_rows,
                    metrics=metrics,
                )
                _append_checkpoint(checkpoint_path, record)
                previous_checkpoint_sha256 = str(record["checkpoint_record_sha256"])
                _assert_identity(
                    descriptor, source.path, opened_identity, relative_uri=source.relative_uri
                )
                scanned_groups += 1
                rows_complete += table.num_rows
                if progress_callback is not None:
                    progress_callback(
                        StructuralQcProgress(
                            status="SCANNED",
                            file_index=file_index,
                            file_count=len(sources),
                            relative_uri=source.relative_uri,
                            row_group_index=row_group_index,
                            row_groups_in_file=context.expected_row_groups,
                            row_groups_complete=resumed_groups + scanned_groups,
                            total_row_groups=total_groups,
                            rows_complete=rows_complete,
                        )
                    )
        finally:
            handle.close()
            os.close(descriptor)

    values = _atomic_publish(
        manifest_path,
        checkpoint_path,
        sources=sources,
        config=config,
        source_manifest_sha256=source_manifest_sha256,
    )
    manifest_sha, manifest_bytes, passed, failed, row_groups, rows, hard_total = values
    if progress_callback is not None:
        progress_callback(
            StructuralQcProgress(
                status="COMPLETE",
                file_index=len(sources),
                file_count=len(sources),
                relative_uri=None,
                row_group_index=None,
                row_groups_in_file=0,
                row_groups_complete=row_groups,
                total_row_groups=row_groups,
                rows_complete=rows,
            )
        )
    return StructuralQcReport(
        status="COMPLETE",
        checker_version=config.checker_version,
        config_path=resolved_config_path,
        config_sha256=config.sha256,
        data_root=root,
        dataset_root=source_root,
        source_manifest_path=resolved_source_manifest,
        source_manifest_sha256=source_manifest_sha256,
        manifest_path=manifest_path,
        checkpoint_path=checkpoint_path,
        manifest_sha256=manifest_sha,
        manifest_byte_size=manifest_bytes,
        file_count=len(sources),
        passed_file_count=passed,
        failed_file_count=failed,
        row_group_count=row_groups,
        row_count=rows,
        hard_violation_count=hard_total,
        resumed_row_group_count=resumed_groups,
        scanned_row_group_count=scanned_groups,
    )
