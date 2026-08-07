"""Point-in-time 6E contract selection from prior-day trade volume.

Only the previous source file's ``instrument_id``, ``action``, and ``size``
columns are streamed.  The next eligible source file is opened for footer
metadata only; its event rows are never consulted.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, BinaryIO, Final

import pyarrow as pa
import pyarrow.parquet as pq

from systematic_fx.data.contracts import Mbp10ContractError, decode_dbn_metadata
from systematic_fx.data.instruments import (
    InstrumentKind,
    InstrumentMapping,
    parse_instrument_mappings,
)

CONTRACT_SELECTION_SCHEMA: Final = "systematic_fx.contract_selection.v1"
CONTRACT_SELECTION_POLICY_VERSION: Final = "previous_source_trade_volume_v1"

_FUTURES_CONTRACT = re.compile(
    r"^(?P<root>[A-Z0-9]+)(?P<month>[FGHJKMNQUVXZ])(?P<year>[0-9]{1,2})$"
)
_MONTH_NUMBER = {
    "F": 1,
    "G": 2,
    "H": 3,
    "J": 4,
    "K": 5,
    "M": 6,
    "N": 7,
    "Q": 8,
    "U": 9,
    "V": 10,
    "X": 11,
    "Z": 12,
}
_REQUIRED_COLUMNS = {
    "instrument_id": pa.uint32(),
    "action": pa.string(),
    "size": pa.uint32(),
}
_UINT32_MAX = 2**32 - 1
_SHA256 = re.compile(r"[0-9a-f]{64}")
_HASH_CHUNK_SIZE = 8 * 1024 * 1024


class ContractSelectionError(ValueError):
    """A source file or mapping cannot support a non-leaking contract decision."""


@dataclass(frozen=True, slots=True)
class ActiveOutrightContract:
    """One source-date-active 6E outright mapping."""

    instrument_id: int
    raw_symbol: str
    contract_month: date

    def as_dict(self) -> dict[str, object]:
        return {
            "contract_month": self.contract_month.isoformat(),
            "instrument_id": self.instrument_id,
            "raw_symbol": self.raw_symbol,
        }


@dataclass(frozen=True, slots=True)
class ContractTradeVolume:
    """Previous-source-date trade volume for one active economic contract month."""

    contract_month: date
    raw_symbols: tuple[str, ...]
    instrument_ids: tuple[int, ...]
    trade_rows: int
    trade_volume: int

    def as_dict(self) -> dict[str, object]:
        return {
            "contract_month": self.contract_month.isoformat(),
            "instrument_ids": list(self.instrument_ids),
            "raw_symbols": list(self.raw_symbols),
            "trade_rows": self.trade_rows,
            "trade_volume": self.trade_volume,
        }


@dataclass(frozen=True, slots=True)
class ExcludedMapping:
    """An active footer mapping excluded from 6E execution candidacy."""

    instrument_id: int
    raw_symbol: str
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            "instrument_id": self.instrument_id,
            "raw_symbol": self.raw_symbol,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class PreviousTradeVolumeSummary:
    """Canonical previous-day volume evidence used by contract selection."""

    source_date: date
    row_groups_scanned: int
    rows_scanned: int
    trade_rows: int
    trade_volume: int
    excluded_trade_rows: int
    excluded_trade_volume: int
    source_sha256: str
    contracts: tuple[ContractTradeVolume, ...]
    excluded_mappings: tuple[ExcludedMapping, ...]
    canonical_bytes: bytes
    sha256: str

    def as_dict(self) -> dict[str, object]:
        value = json.loads(self.canonical_bytes)
        if not isinstance(value, dict):  # defensive: canonical summaries are objects
            raise ContractSelectionError("canonical volume summary is not an object")
        return value


@dataclass(frozen=True, slots=True)
class EligibleContractCandidate:
    """One next-date candidate paired with prior-date trade volume."""

    instrument_id: int
    raw_symbol: str
    contract_month: date
    previous_trade_rows: int
    previous_trade_volume: int

    def as_dict(self) -> dict[str, object]:
        return {
            "contract_month": self.contract_month.isoformat(),
            "instrument_id": self.instrument_id,
            "previous_trade_rows": self.previous_trade_rows,
            "previous_trade_volume": self.previous_trade_volume,
            "raw_symbol": self.raw_symbol,
        }


@dataclass(frozen=True, slots=True)
class ContractSelectionResult:
    """Selected next-date execution mapping and its canonical audit summary."""

    previous_source_date: date
    eligible_source_date: date
    previous_source_sha256: str
    eligible_source_sha256: str
    selected: EligibleContractCandidate
    candidates: tuple[EligibleContractCandidate, ...]
    expiry_exclusions: tuple[ActiveOutrightContract, ...]
    previous_volume: PreviousTradeVolumeSummary
    canonical_bytes: bytes
    sha256: str

    def as_dict(self) -> dict[str, object]:
        value = json.loads(self.canonical_bytes)
        if not isinstance(value, dict):
            raise ContractSelectionError("canonical selection summary is not an object")
        return value


@dataclass(frozen=True, slots=True)
class _ResolvedFooter:
    source_date: date
    mappings_by_id: Mapping[int, InstrumentMapping]
    outright_contracts: tuple[ActiveOutrightContract, ...]
    exclusions: tuple[ExcludedMapping, ...]


@dataclass(frozen=True, slots=True)
class _FileIdentity:
    device: int
    inode: int
    byte_size: int
    mtime_ns: int
    ctime_ns: int


@dataclass(slots=True)
class _VerifiedParquet:
    path: Path
    descriptor: int
    handle: BinaryIO
    parquet: pq.ParquetFile
    identity: _FileIdentity
    source_sha256: str

    def assert_stable(self, *, rehash: bool = False) -> None:
        try:
            current = _file_identity(os.fstat(self.descriptor))
        except OSError as error:
            raise ContractSelectionError(
                f"source descriptor became unreadable: {self.path}"
            ) from error
        if current != self.identity:
            raise ContractSelectionError(
                f"source file changed while contract selection was reading it: {self.path}"
            )
        if rehash and _descriptor_sha256(self.descriptor) != self.source_sha256:
            raise ContractSelectionError(
                f"source bytes changed while contract selection was reading them: {self.path}"
            )


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _required_date(value: object, *, label: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise ContractSelectionError(f"{label} must be a date")
    return value


def _optional_sha256(value: object, *, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ContractSelectionError(f"{label} must be a lowercase SHA-256")
    return value


def _file_identity(value: os.stat_result) -> _FileIdentity:
    return _FileIdentity(
        device=value.st_dev,
        inode=value.st_ino,
        byte_size=value.st_size,
        mtime_ns=value.st_mtime_ns,
        ctime_ns=value.st_ctime_ns,
    )


def _descriptor_sha256(descriptor: int) -> str:
    digest = hashlib.sha256()
    offset = 0
    try:
        while chunk := os.pread(descriptor, _HASH_CHUNK_SIZE, offset):
            digest.update(chunk)
            offset += len(chunk)
    except OSError as error:
        raise ContractSelectionError("cannot hash the opened source descriptor") from error
    return digest.hexdigest()


def resolve_6e_contract_month(raw_symbol: str, *, source_date: date) -> date:
    """Resolve a one/two-digit futures year to the nearest source-date era.

    The month distance, rather than the year alone, breaks decade/century
    ambiguity.  An exact-distance tie prefers the later contract month.
    """

    source_date = _required_date(source_date, label="source_date")
    if not isinstance(raw_symbol, str):
        raise ContractSelectionError("raw_symbol must be a string")
    match = _FUTURES_CONTRACT.fullmatch(raw_symbol)
    if match is None or match.group("root") != "6E":
        raise ContractSelectionError(f"not a parseable 6E outright symbol: {raw_symbol!r}")

    year_digits = match.group("year")
    modulus = 10 ** len(year_digits)
    residue = int(year_digits)
    base = (source_date.year // modulus) * modulus + residue
    source_month = date(source_date.year, source_date.month, 1)
    candidates = tuple(
        date(year, _MONTH_NUMBER[match.group("month")], 1)
        for year in (base - modulus, base, base + modulus)
        if 1 <= year <= 9999
    )
    if not candidates:
        raise ContractSelectionError(f"cannot resolve contract year for {raw_symbol!r}")
    return min(
        candidates,
        key=lambda candidate: (
            abs((candidate - source_month).days),
            0 if candidate >= source_month else 1,
            candidate,
        ),
    )


def _source_date_from_metadata(metadata: Mapping[str, Any], *, path: Path) -> date:
    start = metadata.get("start")
    if isinstance(start, bool) or not isinstance(start, int) or start < 0:
        raise ContractSelectionError(f"{path}: dbn.metadata start must be non-negative ns")
    try:
        return datetime.fromtimestamp(start // 1_000_000_000, tz=UTC).date()
    except (OSError, OverflowError, ValueError) as error:
        raise ContractSelectionError(f"{path}: dbn.metadata start is outside UTC range") from error


def _validate_minimum_schema(parquet: pq.ParquetFile, *, path: Path) -> None:
    schema = parquet.schema_arrow
    for name, expected_type in _REQUIRED_COLUMNS.items():
        index = schema.get_field_index(name)
        if index < 0:
            raise ContractSelectionError(f"{path}: required column {name!r} is missing")
        field = schema.field(index)
        if field.type != expected_type or field.nullable:
            raise ContractSelectionError(
                f"{path}: {name!r} must be non-null {expected_type}; found {field}"
            )


@contextmanager
def _open_footer(
    path: Path | str,
    *,
    expected_source_date: date,
    expected_source_sha256: str | None = None,
) -> Iterator[_VerifiedParquet]:
    expected_source_date = _required_date(expected_source_date, label="expected_source_date")
    expected_sha256 = _optional_sha256(
        expected_source_sha256,
        label="expected_source_sha256",
    )
    requested = Path(path).expanduser()
    if requested.is_symlink():
        raise ContractSelectionError(f"source Parquet cannot be a symbolic link: {requested}")
    try:
        resolved = requested.resolve(strict=True)
    except FileNotFoundError as error:
        raise ContractSelectionError(f"source Parquet does not exist: {requested}") from error
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(resolved, flags)
    except OSError as error:
        raise ContractSelectionError(f"cannot open source Parquet: {resolved}") from error
    handle: BinaryIO | None = None
    verified: _VerifiedParquet | None = None
    try:
        identity = _file_identity(os.fstat(descriptor))
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ContractSelectionError(f"source Parquet is not a regular file: {resolved}")
        observed_sha256 = _descriptor_sha256(descriptor)
        if expected_sha256 is not None and observed_sha256 != expected_sha256:
            raise ContractSelectionError(
                f"source Parquet SHA-256 differs from expected manifest identity: {resolved}"
            )
        if _file_identity(os.fstat(descriptor)) != identity:
            raise ContractSelectionError(
                f"source Parquet changed while its SHA-256 was verified: {resolved}"
            )
        handle = os.fdopen(os.dup(descriptor), "rb")
        try:
            parquet = pq.ParquetFile(handle)
        except (OSError, pa.ArrowException) as error:
            raise ContractSelectionError(f"cannot open Parquet footer: {resolved}") from error
        verified = _VerifiedParquet(
            path=resolved,
            descriptor=descriptor,
            handle=handle,
            parquet=parquet,
            identity=identity,
            source_sha256=observed_sha256,
        )
        verified.assert_stable()
        _validate_minimum_schema(parquet, path=resolved)
        raw_metadata = (parquet.schema_arrow.metadata or {}).get(b"dbn.metadata")
        if raw_metadata is None:
            raise ContractSelectionError(f"{resolved}: schema metadata 'dbn.metadata' is missing")
        try:
            metadata = decode_dbn_metadata(raw_metadata)
        except Mbp10ContractError as error:
            raise ContractSelectionError(f"{resolved}: {error}") from error
        actual_source_date = _source_date_from_metadata(metadata, path=resolved)
        if actual_source_date != expected_source_date:
            raise ContractSelectionError(
                f"{resolved}: footer source date {actual_source_date} != {expected_source_date}"
            )
        verified.assert_stable()
        yield verified
    finally:
        try:
            if verified is not None:
                verified.assert_stable(rehash=True)
        finally:
            if handle is not None:
                handle.close()
            os.close(descriptor)


def _mapping_exclusion(mapping: InstrumentMapping) -> str | None:
    if mapping.kind is InstrumentKind.CALENDAR_SPREAD:
        return "CALENDAR_SPREAD"
    if mapping.kind is not InstrumentKind.OUTRIGHT:
        return "UNKNOWN_OR_UNPARSEABLE"
    match = _FUTURES_CONTRACT.fullmatch(mapping.raw_symbol)
    if match is None:
        return "UNKNOWN_OR_UNPARSEABLE"
    if match.group("root") != "6E":
        return "OTHER_ROOT"
    return None


def _resolve_footer(
    parquet: pq.ParquetFile,
    *,
    path: Path,
    source_date: date,
) -> _ResolvedFooter:
    raw_metadata = (parquet.schema_arrow.metadata or {}).get(b"dbn.metadata")
    assert raw_metadata is not None
    try:
        mappings = parse_instrument_mappings(raw_metadata)
    except Mbp10ContractError as error:
        raise ContractSelectionError(f"{path}: {error}") from error

    active = tuple(
        mapping
        for mapping in mappings
        if mapping.interval_start <= source_date < mapping.interval_end
    )
    by_id: dict[int, InstrumentMapping] = {}
    by_symbol: dict[str, InstrumentMapping] = {}
    for mapping in active:
        existing_id = by_id.get(mapping.instrument_id)
        if existing_id is not None and existing_id.raw_symbol != mapping.raw_symbol:
            raise ContractSelectionError(
                f"{path}: active instrument_id {mapping.instrument_id} maps to both "
                f"{existing_id.raw_symbol!r} and {mapping.raw_symbol!r}"
            )
        existing_symbol = by_symbol.get(mapping.raw_symbol)
        if existing_symbol is not None and existing_symbol.instrument_id != mapping.instrument_id:
            raise ContractSelectionError(
                f"{path}: active raw symbol {mapping.raw_symbol!r} maps to both "
                f"{existing_symbol.instrument_id} and {mapping.instrument_id}"
            )
        by_id[mapping.instrument_id] = mapping
        by_symbol[mapping.raw_symbol] = mapping

    contracts: list[ActiveOutrightContract] = []
    exclusions: list[ExcludedMapping] = []
    for mapping in sorted(by_id.values(), key=lambda item: (item.raw_symbol, item.instrument_id)):
        reason = _mapping_exclusion(mapping)
        if reason is not None:
            exclusions.append(
                ExcludedMapping(
                    instrument_id=mapping.instrument_id,
                    raw_symbol=mapping.raw_symbol,
                    reason=reason,
                )
            )
            continue
        try:
            contract_month = resolve_6e_contract_month(
                mapping.raw_symbol,
                source_date=source_date,
            )
        except ContractSelectionError:
            exclusions.append(
                ExcludedMapping(
                    instrument_id=mapping.instrument_id,
                    raw_symbol=mapping.raw_symbol,
                    reason="UNKNOWN_OR_UNPARSEABLE",
                )
            )
            continue
        contracts.append(
            ActiveOutrightContract(
                instrument_id=mapping.instrument_id,
                raw_symbol=mapping.raw_symbol,
                contract_month=contract_month,
            )
        )

    return _ResolvedFooter(
        source_date=source_date,
        mappings_by_id=dict(sorted(by_id.items())),
        outright_contracts=tuple(
            sorted(
                contracts,
                key=lambda item: (item.contract_month, item.instrument_id, item.raw_symbol),
            )
        ),
        exclusions=tuple(
            sorted(exclusions, key=lambda item: (item.reason, item.raw_symbol, item.instrument_id))
        ),
    )


def resolve_active_6e_outrights(
    path: Path | str,
    *,
    source_date: date,
) -> tuple[ActiveOutrightContract, ...]:
    """Resolve active 6E outright IDs from one daily footer without reading rows."""

    source_date = _required_date(source_date, label="source_date")
    with _open_footer(path, expected_source_date=source_date) as opened:
        opened.assert_stable()
        footer = _resolve_footer(
            opened.parquet,
            path=opened.path,
            source_date=source_date,
        )
        opened.assert_stable()
        return footer.outright_contracts


def _stream_trade_totals(
    parquet: pq.ParquetFile,
    *,
    path: Path,
    assert_stable: Callable[[], None],
) -> tuple[dict[int, tuple[int, int]], int, int]:
    totals: dict[int, tuple[int, int]] = {}
    rows_scanned = 0
    row_groups_scanned = 0
    try:
        for row_group_index in range(parquet.metadata.num_row_groups):
            assert_stable()
            table = parquet.read_row_group(
                row_group_index,
                columns=("instrument_id", "action", "size"),
                use_threads=False,
            )
            assert_stable()
            row_groups_scanned += 1
            rows_scanned += table.num_rows
            instrument_ids = table["instrument_id"].to_pylist()
            actions = table["action"].to_pylist()
            sizes = table["size"].to_pylist()
            for row_offset, (instrument_id, action, size) in enumerate(
                zip(instrument_ids, actions, sizes, strict=True)
            ):
                if instrument_id is None or action is None or size is None:
                    raise ContractSelectionError(
                        f"{path}: null in required trade columns at row group "
                        f"{row_group_index}, row {row_offset}"
                    )
                if not isinstance(instrument_id, int) or not 0 <= instrument_id <= _UINT32_MAX:
                    raise ContractSelectionError(f"{path}: invalid instrument_id {instrument_id!r}")
                if not isinstance(action, str):
                    raise ContractSelectionError(f"{path}: invalid action {action!r}")
                if not isinstance(size, int) or not 0 <= size <= _UINT32_MAX:
                    raise ContractSelectionError(f"{path}: invalid size {size!r}")
                if action != "T":
                    continue
                trade_rows, trade_volume = totals.get(instrument_id, (0, 0))
                totals[instrument_id] = (trade_rows + 1, trade_volume + size)
    except (OSError, pa.ArrowException) as error:
        raise ContractSelectionError(f"cannot stream trade columns from {path}") from error

    if rows_scanned != parquet.metadata.num_rows:
        raise ContractSelectionError(
            f"{path}: streamed {rows_scanned} rows but footer reports {parquet.metadata.num_rows}"
        )
    return totals, row_groups_scanned, rows_scanned


def summarize_previous_trade_volume(
    path: Path | str,
    *,
    source_date: date,
    source_sha256: str | None = None,
) -> PreviousTradeVolumeSummary:
    """Stream only prior-day trade columns and create canonical volume evidence."""

    source_date = _required_date(source_date, label="source_date")
    with _open_footer(
        path,
        expected_source_date=source_date,
        expected_source_sha256=source_sha256,
    ) as opened:
        opened.assert_stable()
        footer = _resolve_footer(
            opened.parquet,
            path=opened.path,
            source_date=source_date,
        )
        opened.assert_stable()
        totals, row_groups_scanned, rows_scanned = _stream_trade_totals(
            opened.parquet,
            path=opened.path,
            assert_stable=opened.assert_stable,
        )
        observed_source_sha256 = opened.source_sha256
        resolved = opened.path

    for instrument_id in totals:
        if instrument_id not in footer.mappings_by_id:
            raise ContractSelectionError(
                f"{resolved}: trade instrument_id {instrument_id} has no source-date-active mapping"
            )

    accepted_by_id = {contract.instrument_id: contract for contract in footer.outright_contracts}
    aggregate: dict[date, dict[str, object]] = {}
    excluded_trade_rows = 0
    excluded_trade_volume = 0
    for instrument_id, mapping in footer.mappings_by_id.items():
        trade_rows, trade_volume = totals.get(instrument_id, (0, 0))
        contract = accepted_by_id.get(instrument_id)
        if contract is None:
            excluded_trade_rows += trade_rows
            excluded_trade_volume += trade_volume
            continue
        state = aggregate.setdefault(
            contract.contract_month,
            {
                "instrument_ids": [],
                "raw_symbols": [],
                "trade_rows": 0,
                "trade_volume": 0,
            },
        )
        state["instrument_ids"].append(instrument_id)  # type: ignore[union-attr]
        state["raw_symbols"].append(mapping.raw_symbol)  # type: ignore[union-attr]
        state["trade_rows"] = int(state["trade_rows"]) + trade_rows
        state["trade_volume"] = int(state["trade_volume"]) + trade_volume

    contracts = tuple(
        ContractTradeVolume(
            contract_month=contract_month,
            instrument_ids=tuple(sorted(state["instrument_ids"])),  # type: ignore[arg-type]
            raw_symbols=tuple(sorted(state["raw_symbols"])),  # type: ignore[arg-type]
            trade_rows=int(state["trade_rows"]),
            trade_volume=int(state["trade_volume"]),
        )
        for contract_month, state in sorted(aggregate.items())
    )
    trade_rows = sum(rows for rows, _ in totals.values())
    trade_volume = sum(volume for _, volume in totals.values())
    document: dict[str, object] = {
        "artifact_schema": f"{CONTRACT_SELECTION_SCHEMA}.previous_volume",
        "policy_version": CONTRACT_SELECTION_POLICY_VERSION,
        "source_date": source_date.isoformat(),
        "source_sha256": observed_source_sha256,
        "scan": {
            "columns": ["instrument_id", "action", "size"],
            "row_groups_scanned": row_groups_scanned,
            "rows_scanned": rows_scanned,
            "trade_rows": trade_rows,
            "trade_volume": trade_volume,
            "excluded_trade_rows": excluded_trade_rows,
            "excluded_trade_volume": excluded_trade_volume,
        },
        "contracts": [contract.as_dict() for contract in contracts],
        "excluded_mappings": [item.as_dict() for item in footer.exclusions],
    }
    canonical_bytes = _canonical_bytes(document)
    return PreviousTradeVolumeSummary(
        source_date=source_date,
        row_groups_scanned=row_groups_scanned,
        rows_scanned=rows_scanned,
        trade_rows=trade_rows,
        trade_volume=trade_volume,
        excluded_trade_rows=excluded_trade_rows,
        excluded_trade_volume=excluded_trade_volume,
        source_sha256=observed_source_sha256,
        contracts=contracts,
        excluded_mappings=footer.exclusions,
        canonical_bytes=canonical_bytes,
        sha256=hashlib.sha256(canonical_bytes).hexdigest(),
    )


def select_next_eligible_contract(
    previous_source_path: Path | str,
    eligible_source_path: Path | str,
    *,
    previous_source_date: date,
    eligible_source_date: date,
    previous_source_sha256: str | None = None,
    eligible_source_sha256: str | None = None,
) -> ContractSelectionResult:
    """Select the next-date active 6E outright using only prior-date trade rows."""

    previous_source_date = _required_date(previous_source_date, label="previous_source_date")
    eligible_source_date = _required_date(eligible_source_date, label="eligible_source_date")
    if previous_source_date >= eligible_source_date:
        raise ContractSelectionError("previous_source_date must precede eligible_source_date")
    previous_resolved = Path(previous_source_path).expanduser().resolve()
    eligible_resolved = Path(eligible_source_path).expanduser().resolve()
    if previous_resolved == eligible_resolved:
        raise ContractSelectionError("previous and eligible source paths must be different files")

    previous_volume = summarize_previous_trade_volume(
        previous_resolved,
        source_date=previous_source_date,
        source_sha256=previous_source_sha256,
    )
    with _open_footer(
        eligible_resolved,
        expected_source_date=eligible_source_date,
        expected_source_sha256=eligible_source_sha256,
    ) as eligible_opened:
        eligible_opened.assert_stable()
        eligible_footer = _resolve_footer(
            eligible_opened.parquet,
            path=eligible_opened.path,
            source_date=eligible_source_date,
        )
        eligible_opened.assert_stable()
        eligible_observed_sha256 = eligible_opened.source_sha256
        eligible_path = eligible_opened.path

    volume_by_month = {contract.contract_month: contract for contract in previous_volume.contracts}
    eligible_month = date(eligible_source_date.year, eligible_source_date.month, 1)
    expiry_exclusions = tuple(
        contract
        for contract in eligible_footer.outright_contracts
        if contract.contract_month <= eligible_month
    )
    allowed = tuple(
        contract
        for contract in eligible_footer.outright_contracts
        if contract.contract_month > eligible_month
    )
    if not allowed:
        raise ContractSelectionError(
            f"{eligible_path}: no active 6E outright remains after expiry-month exclusion"
        )

    candidates = tuple(
        sorted(
            (
                EligibleContractCandidate(
                    instrument_id=contract.instrument_id,
                    raw_symbol=contract.raw_symbol,
                    contract_month=contract.contract_month,
                    previous_trade_rows=(
                        volume_by_month[contract.contract_month].trade_rows
                        if contract.contract_month in volume_by_month
                        else 0
                    ),
                    previous_trade_volume=(
                        volume_by_month[contract.contract_month].trade_volume
                        if contract.contract_month in volume_by_month
                        else 0
                    ),
                )
                for contract in allowed
            ),
            key=lambda item: (
                -item.previous_trade_volume,
                item.contract_month,
                item.instrument_id,
                item.raw_symbol,
            ),
        )
    )
    selected = candidates[0]
    document: dict[str, object] = {
        "artifact_schema": CONTRACT_SELECTION_SCHEMA,
        "policy_version": CONTRACT_SELECTION_POLICY_VERSION,
        "information_boundary": {
            "eligible_source_rows_read": False,
            "volume_source": "PREVIOUS_SOURCE_DATE_ONLY",
        },
        "previous_source_date": previous_source_date.isoformat(),
        "previous_source_sha256": previous_volume.source_sha256,
        "eligible_source_date": eligible_source_date.isoformat(),
        "eligible_source_sha256": eligible_observed_sha256,
        "previous_volume_sha256": previous_volume.sha256,
        "ranking": [
            "previous_trade_volume_desc",
            "nearest_later_contract_month",
            "instrument_id",
            "raw_symbol",
        ],
        "expiry_rule": "contract_month_must_be_after_eligible_source_month",
        "candidates": [candidate.as_dict() for candidate in candidates],
        "expiry_exclusions": [contract.as_dict() for contract in expiry_exclusions],
        "mapping_exclusions": [item.as_dict() for item in eligible_footer.exclusions],
        "selected": selected.as_dict(),
    }
    canonical_bytes = _canonical_bytes(document)
    return ContractSelectionResult(
        previous_source_date=previous_source_date,
        eligible_source_date=eligible_source_date,
        previous_source_sha256=previous_volume.source_sha256,
        eligible_source_sha256=eligible_observed_sha256,
        selected=selected,
        candidates=candidates,
        expiry_exclusions=expiry_exclusions,
        previous_volume=previous_volume,
        canonical_bytes=canonical_bytes,
        sha256=hashlib.sha256(canonical_bytes).hexdigest(),
    )
