"""Deterministic row-level evidence for structural-QC book mutations.

The structural scanner deliberately emits only aggregate counts.  This module
replays failed immutable sources and publishes the exact adjacent rows behind
``clean_trade_none_book_mutation`` without changing the scanner's v1 rules.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Final

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from systematic_fx.data.contracts import UNDEFINED_PRICE
from systematic_fx.data.instruments import parse_instrument_mappings
from systematic_fx.data.quality import (
    _BOOK_COLUMNS,
    CHECKER_VERSION,
    F_MAYBE_BAD_BOOK,
    F_SNAPSHOT,
    FILE_ARTIFACT_SCHEMA,
    HARD_CHECKS,
    QC_COLUMNS,
    _clean_trade_none_book_mutations,
    _column_numpy,
)

ARTIFACT_SCHEMA: Final = "systematic_fx.mbp10_clean_trade_none_book_mutation_detail.v1"
ANALYSIS_VERSION: Final = "mbp10_clean_trade_none_book_mutation_analysis_v1"
DEFAULT_QC_MANIFEST_NAME: Final = "mbp10_structural_qc_v1.jsonl"
DEFAULT_OUTPUT_NAME: Final = "mbp10_clean_trade_none_book_mutations_v1.jsonl"
PRICE_SCALE: Final = "1e-9"

_SOURCE_URI = re.compile(
    r"(?P<year>[0-9]{4})/(?P<month>[0-9]{2})/(?P<day>[0-9]{2})/"
    r"glbx-mdp3-(?P<stamp>[0-9]{8})\.mbp-10\.parquet"
)
_SHA256 = re.compile(r"[0-9a-f]{64}")
_QC_RECORD_KEYS = frozenset(
    {
        "artifact_schema",
        "checker_version",
        "config_sha256",
        "coverage_complete",
        "diagnostic_counts",
        "expected_row_count",
        "expected_row_group_count",
        "first_ts_recv_ns",
        "hard_violation_count",
        "hard_violation_counts",
        "last_ts_recv_ns",
        "relative_uri",
        "research_eligible",
        "result",
        "scanned_row_count",
        "scanned_row_group_count",
        "schema_fingerprint",
        "source_byte_size",
        "source_date",
        "source_manifest_sha256",
        "source_sha256",
    }
)


class QcMutationInspectionError(ValueError):
    """The QC evidence or an immutable source cannot be reproduced safely."""


@dataclass(frozen=True)
class MutationReproduction:
    """Three independently obtained counts for one failed source."""

    source_uri: str
    manifest_count: int
    scanner_count: int
    detail_count: int

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class QcMutationInspectionReport:
    """Identity and coverage of a completed mutation replay."""

    status: str
    data_root: Path
    dataset_root: Path
    qc_manifest_path: Path
    qc_manifest_sha256: str
    output_path: Path
    output_sha256: str
    output_byte_size: int
    created_output: bool
    failed_source_count: int
    replayed_row_group_count: int
    replayed_row_count: int
    mutation_count: int
    reproduction: tuple[MutationReproduction, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "created_output": self.created_output,
            "data_root": self.data_root.as_posix(),
            "dataset_root": self.dataset_root.as_posix(),
            "failed_source_count": self.failed_source_count,
            "mutation_count": self.mutation_count,
            "output_byte_size": self.output_byte_size,
            "output_path": self.output_path.as_posix(),
            "output_sha256": self.output_sha256,
            "qc_manifest_path": self.qc_manifest_path.as_posix(),
            "qc_manifest_sha256": self.qc_manifest_sha256,
            "replayed_row_count": self.replayed_row_count,
            "replayed_row_group_count": self.replayed_row_group_count,
            "reproduction": [item.as_dict() for item in self.reproduction],
            "status": self.status,
        }


@dataclass(frozen=True)
class _FileIdentity:
    size: int
    mtime_ns: int
    ctime_ns: int
    device: int
    inode: int


@dataclass(frozen=True)
class _FailedSource:
    relative_uri: str
    source_date: date
    source_sha256: str
    source_byte_size: int
    expected_rows: int
    expected_row_groups: int
    expected_mutations: int
    checker_version: str
    config_sha256: str


@dataclass(frozen=True)
class _DetailState:
    book: tuple[int, ...]
    row: Mapping[str, object]
    valid: bool


def _canonical_line(record: Mapping[str, object]) -> bytes:
    return (
        json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _required_int(value: object, *, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise QcMutationInspectionError(f"{label} must be an integer at least {minimum}")
    return value


def _required_sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise QcMutationInspectionError(f"{label} must be a lowercase SHA-256")
    return value


def _strict_root(path: Path | str, *, label: str) -> Path:
    requested = Path(path).expanduser()
    if requested.is_symlink():
        raise QcMutationInspectionError(f"{label} cannot be a symbolic link: {requested}")
    resolved = requested.resolve(strict=True)
    if not resolved.is_dir():
        raise NotADirectoryError(f"{label} is not a directory: {resolved}")
    return resolved


def _strict_file(path: Path | str, *, label: str) -> Path:
    requested = Path(path).expanduser()
    if requested.is_symlink():
        raise QcMutationInspectionError(f"{label} cannot be a symbolic link: {requested}")
    resolved = requested.resolve(strict=True)
    if not stat.S_ISREG(resolved.lstat().st_mode):
        raise QcMutationInspectionError(f"{label} must be a regular file: {resolved}")
    return resolved


def _contained(path: Path, root: Path, *, label: str) -> Path:
    try:
        return path.relative_to(root)
    except ValueError as error:
        raise QcMutationInspectionError(f"{label} must remain inside {root}") from error


def _no_symlinks(root: Path, relative: Path, *, label: str) -> None:
    current = root
    for part in relative.parts:
        current /= part
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError as error:
            raise FileNotFoundError(f"{label} does not exist: {current}") from error
        if stat.S_ISLNK(mode):
            raise QcMutationInspectionError(f"{label} cannot traverse a symbolic link: {current}")


def _parse_source_uri(value: object) -> tuple[str, date]:
    if not isinstance(value, str) or not value or "\\" in value:
        raise QcMutationInspectionError("QC relative_uri must be a canonical POSIX path")
    pure = PurePosixPath(value)
    if (
        pure.is_absolute()
        or pure.as_posix() != value
        or any(part in {"", ".", ".."} for part in pure.parts)
    ):
        raise QcMutationInspectionError(f"unsafe QC relative_uri: {value!r}")
    match = _SOURCE_URI.fullmatch(value)
    if match is None:
        raise QcMutationInspectionError(f"invalid MBP-10 relative_uri: {value!r}")
    try:
        source_date = date(
            int(match.group("year")), int(match.group("month")), int(match.group("day"))
        )
    except ValueError as error:
        raise QcMutationInspectionError(f"invalid date in relative_uri: {value!r}") from error
    if source_date.strftime("%Y%m%d") != match.group("stamp"):
        raise QcMutationInspectionError(f"partition and filename dates disagree: {value!r}")
    return value, source_date


def _validate_qc_record(record: object, *, line_number: int) -> _FailedSource | None:
    if not isinstance(record, dict) or set(record) != _QC_RECORD_KEYS:
        raise QcMutationInspectionError(f"invalid final QC fields on line {line_number}")
    if record["artifact_schema"] != FILE_ARTIFACT_SCHEMA:
        raise QcMutationInspectionError(f"unsupported QC artifact schema on line {line_number}")
    if record["checker_version"] != CHECKER_VERSION:
        raise QcMutationInspectionError(f"unsupported QC checker version on line {line_number}")
    if record["coverage_complete"] is not True:
        raise QcMutationInspectionError(f"incomplete final QC record on line {line_number}")
    if record["research_eligible"] is not False:
        raise QcMutationInspectionError(f"unexpected research eligibility on line {line_number}")

    relative_uri, uri_date = _parse_source_uri(record["relative_uri"])
    if record["source_date"] != uri_date.isoformat():
        raise QcMutationInspectionError(f"QC source date disagrees for {relative_uri}")
    source_sha256 = _required_sha256(record["source_sha256"], label=f"{relative_uri} source_sha256")
    _required_sha256(record["source_manifest_sha256"], label="source_manifest_sha256")
    config_sha256 = _required_sha256(record["config_sha256"], label="config_sha256")
    _required_sha256(record["schema_fingerprint"], label=f"{relative_uri} schema_fingerprint")

    expected_rows = _required_int(
        record["expected_row_count"], label=f"{relative_uri} expected_row_count"
    )
    expected_groups = _required_int(
        record["expected_row_group_count"],
        label=f"{relative_uri} expected_row_group_count",
        minimum=1,
    )
    if record["scanned_row_count"] != expected_rows:
        raise QcMutationInspectionError(f"QC row coverage differs for {relative_uri}")
    if record["scanned_row_group_count"] != expected_groups:
        raise QcMutationInspectionError(f"QC row-group coverage differs for {relative_uri}")

    hard = record["hard_violation_counts"]
    if not isinstance(hard, dict) or set(hard) != set(HARD_CHECKS):
        raise QcMutationInspectionError(f"invalid hard violation fields for {relative_uri}")
    hard_counts = {
        key: _required_int(value, label=f"{relative_uri} hard count {key}")
        for key, value in hard.items()
    }
    hard_total = _required_int(
        record["hard_violation_count"], label=f"{relative_uri} hard_violation_count"
    )
    if hard_total != sum(hard_counts.values()):
        raise QcMutationInspectionError(f"hard violation total disagrees for {relative_uri}")
    expected_result = "PASS" if hard_total == 0 else "FAIL"
    if record["result"] != expected_result:
        raise QcMutationInspectionError(f"QC result disagrees with hard counts for {relative_uri}")
    if expected_result != "FAIL":
        return None

    return _FailedSource(
        relative_uri=relative_uri,
        source_date=uri_date,
        source_sha256=source_sha256,
        source_byte_size=_required_int(
            record["source_byte_size"], label=f"{relative_uri} source_byte_size"
        ),
        expected_rows=expected_rows,
        expected_row_groups=expected_groups,
        expected_mutations=hard_counts["clean_trade_none_book_mutation"],
        checker_version=CHECKER_VERSION,
        config_sha256=config_sha256,
    )


def _file_identity(value: os.stat_result) -> _FileIdentity:
    return _FileIdentity(
        size=value.st_size,
        mtime_ns=value.st_mtime_ns,
        ctime_ns=value.st_ctime_ns,
        device=value.st_dev,
        inode=value.st_ino,
    )


def _open_regular(path: Path, *, label: str) -> tuple[int, Any, _FileIdentity]:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise QcMutationInspectionError(f"cannot open immutable {label}: {path}") from error
    identity = _file_identity(os.fstat(descriptor))
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise QcMutationInspectionError(f"{label} must be a regular file: {path}")
    return descriptor, os.fdopen(descriptor, "rb", closefd=False), identity


def _assert_identity(
    descriptor: int,
    path: Path,
    identity: _FileIdentity,
    *,
    label: str,
) -> None:
    if _file_identity(os.fstat(descriptor)) != identity:
        raise QcMutationInspectionError(f"{label} changed while being read: {path}")
    if path.is_symlink() or _file_identity(path.stat()) != identity:
        raise QcMutationInspectionError(f"{label} path changed while being read: {path}")


def _hash_handle(handle: Any) -> str:
    handle.seek(0)
    digest = hashlib.sha256()
    while chunk := handle.read(8 * 1024 * 1024):
        digest.update(chunk)
    handle.seek(0)
    return digest.hexdigest()


def _load_failed_sources(path: Path) -> tuple[str, list[_FailedSource]]:
    descriptor, handle, identity = _open_regular(path, label="QC manifest")
    digest = hashlib.sha256()
    failures: list[_FailedSource] = []
    previous_uri: str | None = None
    record_count = 0
    config_sha256: str | None = None
    source_manifest_sha256: str | None = None
    try:
        for line_number, raw_line in enumerate(handle, start=1):
            record_count += 1
            digest.update(raw_line)
            try:
                record = json.loads(raw_line)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise QcMutationInspectionError(
                    f"invalid final QC JSON on line {line_number}"
                ) from error
            if raw_line != _canonical_line(record):
                raise QcMutationInspectionError(
                    f"final QC line {line_number} is not canonical JSONL"
                )
            failure = _validate_qc_record(record, line_number=line_number)
            relative_uri = str(record["relative_uri"])
            if previous_uri is not None and relative_uri <= previous_uri:
                raise QcMutationInspectionError("final QC URIs must be unique and sorted")
            previous_uri = relative_uri
            current_config = str(record["config_sha256"])
            current_sources = str(record["source_manifest_sha256"])
            if config_sha256 is None:
                config_sha256 = current_config
                source_manifest_sha256 = current_sources
            elif config_sha256 != current_config or source_manifest_sha256 != current_sources:
                raise QcMutationInspectionError("final QC manifest lineage is not uniform")
            if failure is not None:
                failures.append(failure)
        _assert_identity(descriptor, path, identity, label="QC manifest")
    finally:
        handle.close()
        os.close(descriptor)
    if record_count == 0:
        raise QcMutationInspectionError("final QC manifest is empty")
    if not failures:
        raise QcMutationInspectionError("final QC manifest contains no failed sources")
    if sum(item.expected_mutations for item in failures) == 0:
        raise QcMutationInspectionError(
            "failed sources contain no clean_trade_none_book_mutation evidence"
        )
    return digest.hexdigest(), failures


def _timestamp_text(value: int) -> str:
    seconds, nanoseconds = divmod(value, 1_000_000_000)
    base = datetime.fromtimestamp(seconds, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{base}.{nanoseconds:09d}Z"


def _row_payload(
    *,
    row_index: int,
    row_group_index: int,
    file_row_offset: int,
    arrays: Mapping[str, np.ndarray[Any, Any]],
) -> dict[str, object]:
    ts_event_ns = int(arrays["ts_event"][row_index])
    ts_recv_ns = int(arrays["ts_recv"][row_index])
    flags = int(arrays["flags"][row_index])
    return {
        "action": str(arrays["action"][row_index]),
        "depth": int(arrays["depth"][row_index]),
        "file_row_ordinal_zero_based": file_row_offset + row_index,
        "flags": flags,
        "flags_hex": f"0x{flags:02x}",
        "price_raw": int(arrays["price"][row_index]),
        "publisher_id": int(arrays["publisher_id"][row_index]),
        "row_group_index_zero_based": row_group_index,
        "row_group_row_ordinal_zero_based": row_index,
        "sequence": int(arrays["sequence"][row_index]),
        "side": str(arrays["side"][row_index]),
        "size": int(arrays["size"][row_index]),
        "ts_event": _timestamp_text(ts_event_ns),
        "ts_event_ns": ts_event_ns,
        "ts_recv": _timestamp_text(ts_recv_ns),
        "ts_recv_ns": ts_recv_ns,
    }


def _row_arrays(table: pa.Table) -> dict[str, np.ndarray[Any, Any]]:
    arrays: dict[str, np.ndarray[Any, Any]] = {}
    for name in (
        "publisher_id",
        "instrument_id",
        "action",
        "flags",
        "ts_event",
        "ts_recv",
        "sequence",
        "side",
        "price",
        "size",
        "depth",
    ):
        fill: object = "" if name in {"action", "side"} else 0
        if name == "price":
            fill = UNDEFINED_PRICE
        arrays[name] = _column_numpy(table, name, fill)
    arrays["publisher_id"] = arrays["publisher_id"].astype(np.uint64, copy=False)
    arrays["instrument_id"] = arrays["instrument_id"].astype(np.uint64, copy=False)
    arrays["flags"] = arrays["flags"].astype(np.uint8, copy=False)
    arrays["ts_event"] = arrays["ts_event"].astype(np.int64, copy=False)
    arrays["ts_recv"] = arrays["ts_recv"].astype(np.int64, copy=False)
    arrays["sequence"] = arrays["sequence"].astype(np.uint64, copy=False)
    return arrays


def _book_arrays(
    table: pa.Table,
) -> tuple[dict[str, np.ndarray[Any, Any]], np.ndarray[Any, Any], np.ndarray[Any, Any]]:
    books: dict[str, np.ndarray[Any, Any]] = {}
    for name in _BOOK_COLUMNS:
        fill = UNDEFINED_PRICE if "_px_" in name else 0
        books[name] = _column_numpy(table, name, fill).astype(np.int64, copy=False)

    local_valid = np.ones(table.num_rows, dtype=np.bool_)
    book_empty = np.ones(table.num_rows, dtype=np.bool_)
    prior_bid_defined: np.ndarray[Any, Any] | None = None
    prior_ask_defined: np.ndarray[Any, Any] | None = None
    prior_bid_price: np.ndarray[Any, Any] | None = None
    prior_ask_price: np.ndarray[Any, Any] | None = None
    for level in range(10):
        suffix = f"{level:02d}"
        bid_price = books[f"bid_px_{suffix}"]
        ask_price = books[f"ask_px_{suffix}"]
        bid_size = books[f"bid_sz_{suffix}"]
        ask_size = books[f"ask_sz_{suffix}"]
        bid_count = books[f"bid_ct_{suffix}"]
        ask_count = books[f"ask_ct_{suffix}"]
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
    return books, local_valid, book_empty


def _changed_fields(
    previous_book: Sequence[int], current_book: Sequence[int]
) -> list[dict[str, object]]:
    return [
        {"current_raw": current, "field": name, "previous_raw": previous}
        for name, previous, current in zip(_BOOK_COLUMNS, previous_book, current_book, strict=True)
        if previous != current
    ]


def _mapped_symbols(parquet_file: pq.ParquetFile, source_date: date) -> dict[int, tuple[str, ...]]:
    raw = (parquet_file.schema_arrow.metadata or {}).get(b"dbn.metadata")
    if raw is None:
        raise QcMutationInspectionError("dbn.metadata is missing while resolving raw symbols")
    output: dict[int, set[str]] = {}
    for mapping in parse_instrument_mappings(raw):
        if mapping.interval_start <= source_date < mapping.interval_end:
            output.setdefault(mapping.instrument_id, set()).add(mapping.raw_symbol)
    return {instrument_id: tuple(sorted(symbols)) for instrument_id, symbols in output.items()}


def _analyze_row_group(
    table: pa.Table,
    *,
    row_group_index: int,
    file_row_offset: int,
    state: dict[int, _DetailState],
    source: _FailedSource,
    symbols: Mapping[int, tuple[str, ...]],
    qc_manifest_sha256: str,
) -> list[dict[str, object]]:
    if table.num_rows == 0:
        return []
    arrays = _row_arrays(table)
    books, local_valid, book_empty = _book_arrays(table)
    keys = (arrays["publisher_id"] << np.uint64(32)) | arrays["instrument_id"]
    order = np.argsort(keys, kind="stable")
    sorted_keys = keys[order]
    same = sorted_keys[1:] == sorted_keys[:-1]
    starts = np.concatenate(([0], np.flatnonzero(~same) + 1))
    ends = np.concatenate((starts[1:] - 1, [len(order) - 1]))

    previous_indexes = order[:-1]
    current_indexes = order[1:]
    changed = np.zeros(len(current_indexes), dtype=np.bool_)
    for values in books.values():
        changed |= values[current_indexes] != values[previous_indexes]
    details: list[dict[str, object]] = []

    def book_tuple(index: int) -> tuple[int, ...]:
        return tuple(int(values[index]) for values in books.values())

    for start, end in zip(starts, ends, strict=True):
        group_indexes = order[start : end + 1]
        key = int(sorted_keys[start])
        previous = state.get(key)
        starting_valid = previous.valid if previous is not None else False
        group_actions = arrays["action"][group_indexes]
        group_flags = arrays["flags"][group_indexes]
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
            markers[0] = 1
        marker_locations = np.where(markers != 0, np.arange(len(markers)) + 1, 0)
        last_marker_locations = np.maximum.accumulate(marker_locations)
        valid = np.full(len(group_indexes), starting_valid, dtype=np.bool_)
        has_marker = last_marker_locations > 0
        valid[has_marker] = markers[last_marker_locations[has_marker] - 1] == 1

        candidate_pairs: list[tuple[_DetailState, int, tuple[int, ...]]] = []
        first_index = int(group_indexes[0])
        first_book = book_tuple(first_index)
        first_is_clean_trade_none = (
            arrays["action"][first_index] in {"T", "N"}
            and (int(arrays["flags"][first_index]) & (F_SNAPSHOT | F_MAYBE_BAD_BOOK)) == 0
        )
        if (
            previous is not None
            and previous.valid
            and bool(valid[0])
            and first_is_clean_trade_none
            and first_book != previous.book
        ):
            candidate_pairs.append((previous, first_index, first_book))

        if len(group_indexes) > 1:
            group_current = group_indexes[1:]
            current_is_clean_trade_none = np.isin(arrays["action"][group_current], ("T", "N")) & (
                (arrays["flags"][group_current] & (F_SNAPSHOT | F_MAYBE_BAD_BOOK)) == 0
            )
            comparable = valid[:-1] & valid[1:] & current_is_clean_trade_none
            mutation_positions = np.flatnonzero(comparable & changed[start:end]) + 1
            for position in mutation_positions:
                current_index = int(group_indexes[position])
                previous_index = int(group_indexes[position - 1])
                candidate_pairs.append(
                    (
                        _DetailState(
                            book=book_tuple(previous_index),
                            row=_row_payload(
                                row_index=previous_index,
                                row_group_index=row_group_index,
                                file_row_offset=file_row_offset,
                                arrays=arrays,
                            ),
                            valid=bool(valid[position - 1]),
                        ),
                        current_index,
                        book_tuple(current_index),
                    )
                )

        instrument_id = int(arrays["instrument_id"][first_index])
        raw_symbols = symbols.get(instrument_id, ())
        for previous_item, current_index, current_book in candidate_pairs:
            current_row = _row_payload(
                row_index=current_index,
                row_group_index=row_group_index,
                file_row_offset=file_row_offset,
                arrays=arrays,
            )
            changes = _changed_fields(previous_item.book, current_book)
            details.append(
                {
                    "analysis_version": ANALYSIS_VERSION,
                    "artifact_schema": ARTIFACT_SCHEMA,
                    "changed_book_field_count": len(changes),
                    "changed_book_fields": changes,
                    "checker_version": source.checker_version,
                    "config_sha256": source.config_sha256,
                    "current_row": current_row,
                    "instrument_id": instrument_id,
                    "price_scale": PRICE_SCALE,
                    "previous_row": dict(previous_item.row),
                    "qc_manifest_sha256": qc_manifest_sha256,
                    "raw_symbol": raw_symbols[0] if len(raw_symbols) == 1 else None,
                    "raw_symbols": list(raw_symbols),
                    "source_date": source.source_date.isoformat(),
                    "source_sha256": source.source_sha256,
                    "source_uri": source.relative_uri,
                }
            )
        last_index = int(group_indexes[-1])
        state[key] = _DetailState(
            book=book_tuple(last_index),
            row=_row_payload(
                row_index=last_index,
                row_group_index=row_group_index,
                file_row_offset=file_row_offset,
                arrays=arrays,
            ),
            valid=bool(valid[-1]),
        )
    return details


def _output_directory(data_root: Path) -> Path:
    current = data_root
    for component in ("derived", "manifests"):
        current /= component
        if current.exists() or current.is_symlink():
            if current.is_symlink() or not current.is_dir():
                raise QcMutationInspectionError(f"unsafe output directory component: {current}")
        else:
            current.mkdir(mode=0o700)
    return current.resolve(strict=True)


def _output_path(directory: Path, output_name: str) -> Path:
    if (
        not isinstance(output_name, str)
        or not output_name
        or Path(output_name).name != output_name
        or output_name in {".", ".."}
        or not output_name.endswith(".jsonl")
    ):
        raise QcMutationInspectionError("output_name must be one .jsonl filename")
    path = directory / output_name
    if path.is_symlink():
        raise QcMutationInspectionError(f"output cannot be a symbolic link: {path}")
    return path


def _files_identical(first: Path, second: Path) -> bool:
    if first.stat().st_size != second.stat().st_size:
        return False
    with first.open("rb") as first_handle, second.open("rb") as second_handle:
        while True:
            first_chunk = first_handle.read(8 * 1024 * 1024)
            second_chunk = second_handle.read(8 * 1024 * 1024)
            if first_chunk != second_chunk:
                return False
            if not first_chunk:
                return True


def _publish_immutable(output_path: Path, payload: bytes) -> tuple[str, bool]:
    digest = hashlib.sha256(payload).hexdigest()
    handle = tempfile.NamedTemporaryFile(  # noqa: SIM115
        mode="w+b",
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        dir=output_path.parent,
        delete=False,
    )
    temporary = Path(handle.name)
    created = False
    try:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        try:
            os.link(temporary, output_path, follow_symlinks=False)
            created = True
        except FileExistsError:
            if output_path.is_symlink() or not stat.S_ISREG(output_path.lstat().st_mode):
                raise QcMutationInspectionError(f"existing output is unsafe: {output_path}")
            if not _files_identical(output_path, temporary):
                raise QcMutationInspectionError(
                    f"existing immutable mutation report content drift: {output_path}"
                )
        temporary.unlink()
        if created:
            directory_fd = os.open(output_path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    except BaseException:
        handle.close()
        temporary.unlink(missing_ok=True)
        raise
    return digest, created


def inspect_qc_mutations(
    data_root: Path | str,
    *,
    qc_manifest_path: Path | str | None = None,
    dataset_root: Path | str | None = None,
    output_name: str = DEFAULT_OUTPUT_NAME,
) -> QcMutationInspectionReport:
    """Replay every failed QC source and publish canonical row-level evidence.

    For every failed file, the count recorded by the final QC manifest must
    equal both the unmodified v1 scanner helper's replay count and this module's
    independently located detail rows.  Source content SHA-256 is checked before
    replay.  An existing output is accepted only when its bytes are identical.
    """

    root = _strict_root(data_root, label="data_root")
    requested_dataset = (
        Path(dataset_root).expanduser() if dataset_root is not None else root / "mbp-10"
    )
    if requested_dataset.is_symlink():
        raise QcMutationInspectionError("dataset_root cannot be a symbolic link")
    source_root = requested_dataset.resolve(strict=True)
    relative_dataset = _contained(source_root, root, label="dataset_root")
    if not relative_dataset.parts or relative_dataset.parts[0] == "derived":
        raise QcMutationInspectionError("dataset_root must be a raw-data child outside derived")
    _no_symlinks(root, relative_dataset, label="dataset_root")
    if not source_root.is_dir():
        raise NotADirectoryError(f"dataset_root is not a directory: {source_root}")

    outputs = _output_directory(root)
    output_path = _output_path(outputs, output_name)
    requested_qc = qc_manifest_path or outputs / DEFAULT_QC_MANIFEST_NAME
    qc_manifest = _strict_file(requested_qc, label="qc_manifest_path")
    if qc_manifest == output_path:
        raise QcMutationInspectionError("QC input and mutation output paths must differ")
    qc_manifest_sha256, failures = _load_failed_sources(qc_manifest)

    details: list[dict[str, object]] = []
    reproductions: list[MutationReproduction] = []
    replayed_groups = 0
    replayed_rows = 0
    for source in failures:
        relative = Path(*PurePosixPath(source.relative_uri).parts)
        _no_symlinks(source_root, relative, label="source")
        source_path = (source_root / relative).resolve(strict=True)
        _contained(source_path, source_root, label="source")
        descriptor, handle, identity = _open_regular(source_path, label="source")
        try:
            if identity.size != source.source_byte_size:
                raise QcMutationInspectionError(f"source byte-size drift: {source.relative_uri}")
            if _hash_handle(handle) != source.source_sha256:
                raise QcMutationInspectionError(f"source SHA-256 drift: {source.relative_uri}")
            _assert_identity(descriptor, source_path, identity, label="source")
            parquet_file = pq.ParquetFile(handle)
            if parquet_file.metadata.num_rows != source.expected_rows:
                raise QcMutationInspectionError(f"source row-count drift: {source.relative_uri}")
            if parquet_file.metadata.num_row_groups != source.expected_row_groups:
                raise QcMutationInspectionError(
                    f"source row-group-count drift: {source.relative_uri}"
                )
            symbols = _mapped_symbols(parquet_file, source.source_date)
            scanner_state: dict[int, tuple[tuple[int, ...], bool]] = {}
            detail_state: dict[int, _DetailState] = {}
            scanner_count = 0
            source_details: list[dict[str, object]] = []
            file_row_offset = 0
            for row_group_index in range(source.expected_row_groups):
                _assert_identity(descriptor, source_path, identity, label="source")
                table = parquet_file.read_row_group(
                    row_group_index, columns=list(QC_COLUMNS), use_threads=False
                )
                expected_group_rows = parquet_file.metadata.row_group(row_group_index).num_rows
                if table.num_rows != expected_group_rows:
                    raise QcMutationInspectionError(
                        f"row-group row-count drift: {source.relative_uri} group {row_group_index}"
                    )
                count, scanner_state = _clean_trade_none_book_mutations(table, scanner_state)
                scanner_count += count
                source_details.extend(
                    _analyze_row_group(
                        table,
                        row_group_index=row_group_index,
                        file_row_offset=file_row_offset,
                        state=detail_state,
                        source=source,
                        symbols=symbols,
                        qc_manifest_sha256=qc_manifest_sha256,
                    )
                )
                file_row_offset += table.num_rows
                replayed_groups += 1
                replayed_rows += table.num_rows
                _assert_identity(descriptor, source_path, identity, label="source")
        finally:
            handle.close()
            os.close(descriptor)

        reproduction = MutationReproduction(
            source_uri=source.relative_uri,
            manifest_count=source.expected_mutations,
            scanner_count=scanner_count,
            detail_count=len(source_details),
        )
        reproductions.append(reproduction)
        if not (
            reproduction.manifest_count == reproduction.scanner_count == reproduction.detail_count
        ):
            raise QcMutationInspectionError(
                f"three-way mutation count mismatch for {source.relative_uri}: "
                f"manifest={reproduction.manifest_count}, "
                f"scanner={reproduction.scanner_count}, detail={reproduction.detail_count}"
            )
        details.extend(source_details)

    details.sort(
        key=lambda item: (
            str(item["source_uri"]),
            int(item["current_row"]["file_row_ordinal_zero_based"]),  # type: ignore[index]
            int(item["instrument_id"]),
        )
    )
    for mutation_index, record in enumerate(details, start=1):
        record["mutation_id"] = f"TNM-{mutation_index:03d}"
    payload = b"".join(_canonical_line(record) for record in details)
    output_sha256, created = _publish_immutable(output_path, payload)
    return QcMutationInspectionReport(
        status="COMPLETE",
        data_root=root,
        dataset_root=source_root,
        qc_manifest_path=qc_manifest,
        qc_manifest_sha256=qc_manifest_sha256,
        output_path=output_path,
        output_sha256=output_sha256,
        output_byte_size=len(payload),
        created_output=created,
        failed_source_count=len(failures),
        replayed_row_group_count=replayed_groups,
        replayed_row_count=replayed_rows,
        mutation_count=len(details),
        reproduction=tuple(reproductions),
    )
