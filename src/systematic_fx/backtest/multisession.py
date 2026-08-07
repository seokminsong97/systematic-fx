"""Lazy, fixed-contract MBP-10 continuation across later source sessions.

The entry-day path is owned by :mod:`systematic_fx.backtest.entry`.  This
module chains that path to an explicitly ordered, integrity-bound set of later
daily Parquet sources.  Contract identity is frozen to the entry's raw symbol;
only its source-date-active instrument id may change.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import Final

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from systematic_fx.backtest.barriers import ExecutableQuote
from systematic_fx.backtest.economics import EntryStatus
from systematic_fx.backtest.entry import (
    BboInvalidReason,
    EntryReplayResult,
    raw_6e_price_to_ticks,
)
from systematic_fx.data.contract_selection import (
    ContractSelectionError,
    resolve_6e_contract_month,
)
from systematic_fx.data.contracts import (
    UNDEFINED_PRICE,
    Mbp10ContractError,
    compute_schema_fingerprint,
    decode_dbn_metadata,
    validate_mbp10_contract,
)
from systematic_fx.data.instruments import InstrumentKind, parse_instrument_mappings

MULTISESSION_PATH_SCHEMA: Final = "systematic_fx.multisession_path.v1"
MAX_OBSERVATION_SOURCE_SESSIONS: Final = 20
RESET_REARM_NS: Final = 1_000_000_000

F_MAYBE_BAD_BOOK: Final = 4
F_BAD_TS_RECV: Final = 8
_KNOWN_ACTIONS: Final = frozenset({"A", "C", "F", "M", "N", "R", "T"})
_SHA256 = re.compile(r"[0-9a-f]{64}")
_UINT32_MAX: Final = 2**32 - 1
_COLUMNS: Final = (
    "ts_recv",
    "instrument_id",
    "action",
    "flags",
    "sequence",
    "bid_px_00",
    "ask_px_00",
    "bid_sz_00",
    "ask_sz_00",
)


class MultisessionReplayError(ValueError):
    """A continuation source or immutable replay contract has drifted."""


class CutoffReason(StrEnum):
    """Why the supplied continuation window ended."""

    OBSERVATION_WINDOW_END = "OBSERVATION_WINDOW_END"
    EXPIRY_MONTH = "EXPIRY_MONTH"


@dataclass(frozen=True, slots=True)
class SourceSession:
    """One explicitly ordered later source file."""

    source_date: date
    parquet_path: Path | str

    def __post_init__(self) -> None:
        if isinstance(self.source_date, datetime) or not isinstance(self.source_date, date):
            raise MultisessionReplayError("source_date must be a date")
        if not isinstance(self.parquet_path, (Path, str)):
            raise MultisessionReplayError("parquet_path must be a path")


@dataclass(frozen=True, slots=True, kw_only=True)
class LineagedExecutableQuote(ExecutableQuote):
    """Barrier-compatible quote retaining exact later-source row lineage."""

    source_date: date
    source_path: str
    source_sha256: str
    instrument_id: int
    row_group_index: int
    row_index: int
    source_row_index: int
    sequence: int
    invalid_reason: BboInvalidReason | None

    def __post_init__(self) -> None:
        ExecutableQuote.__post_init__(self)
        if isinstance(self.source_date, datetime) or not isinstance(self.source_date, date):
            raise MultisessionReplayError("quote source_date must be a date")
        if _SHA256.fullmatch(self.source_sha256) is None:
            raise MultisessionReplayError("quote source_sha256 is invalid")
        for label in (
            "instrument_id",
            "row_group_index",
            "row_index",
            "source_row_index",
            "sequence",
        ):
            value = getattr(self, label)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise MultisessionReplayError(f"quote {label} must be a non-negative integer")


@dataclass(frozen=True, slots=True)
class SourceAudit:
    """Immutable footer, mapping, and content identity for one included source."""

    source_date: date
    source_path: str
    source_sha256: str
    schema_sha256: str
    dbn_metadata_sha256: str
    instrument_id: int
    raw_symbol: str
    row_count: int
    row_group_count: int
    event_index_offset: int

    def as_dict(self) -> dict[str, object]:
        return {
            "dbn_metadata_sha256": self.dbn_metadata_sha256,
            "event_index_offset": self.event_index_offset,
            "instrument_id": self.instrument_id,
            "raw_symbol": self.raw_symbol,
            "row_count": self.row_count,
            "row_group_count": self.row_group_count,
            "schema_sha256": self.schema_sha256,
            "source_date": self.source_date.isoformat(),
            "source_path": self.source_path,
            "source_sha256": self.source_sha256,
        }


@dataclass(frozen=True, slots=True)
class TerminalLookupAudit:
    """Bounded reverse-row-group work used to separate the terminal quote."""

    method: str
    source_dates_opened: tuple[date, ...]
    row_groups_read: int
    rows_examined: int
    terminal_event_index: int | None
    terminal_source_date: date | None
    terminal_row_group_index: int | None
    terminal_row_index: int | None

    def as_dict(self) -> dict[str, object]:
        return {
            "method": self.method,
            "row_groups_read": self.row_groups_read,
            "rows_examined": self.rows_examined,
            "source_dates_opened": [value.isoformat() for value in self.source_dates_opened],
            "terminal_event_index": self.terminal_event_index,
            "terminal_row_group_index": self.terminal_row_group_index,
            "terminal_row_index": self.terminal_row_index,
            "terminal_source_date": (
                self.terminal_source_date.isoformat()
                if self.terminal_source_date is not None
                else None
            ),
        }


@dataclass(frozen=True, slots=True)
class MultisessionAudit:
    """Canonical identity and cutoff record for a prepared continuation."""

    entry_sha256: str
    source_manifest_sha256: str
    raw_symbol: str
    contract_month: date
    maximum_source_sessions: int
    cutoff_reason: CutoffReason
    expiry_cutoff_source_date: date | None
    supplied_source_dates: tuple[date, ...]
    excluded_source_dates: tuple[date, ...]
    sources: tuple[SourceAudit, ...]
    terminal_lookup: TerminalLookupAudit
    canonical_bytes: bytes
    sha256: str

    def as_dict(self) -> dict[str, object]:
        value = json.loads(self.canonical_bytes)
        if not isinstance(value, dict):  # pragma: no cover - constructed internally
            raise MultisessionReplayError("canonical multisession audit is not an object")
        return value


@dataclass(frozen=True, slots=True)
class _FileIdentity:
    size: int
    mtime_ns: int
    ctime_ns: int
    device: int
    inode: int


@dataclass(frozen=True, slots=True)
class _PreparedSource:
    path: Path
    source_date: date
    source_sha256: str
    schema_sha256: str
    metadata_sha256: str
    instrument_id: int
    raw_symbol: str
    contract_month: date
    row_count: int
    row_group_count: int
    event_index_offset: int
    identity: _FileIdentity


@dataclass(frozen=True, slots=True)
class _DecodedRow:
    ts_recv_ns: int
    action: str
    sequence: int
    bid_ticks: int | None
    ask_ticks: int | None
    bid_size: int
    ask_size: int
    row_invalid_reason: BboInvalidReason | None

    @property
    def complete(self) -> bool:
        return self.row_invalid_reason is None and self.bid_size >= 1 and self.ask_size >= 1


@dataclass(frozen=True, slots=True)
class _TerminalCandidate:
    source: _PreparedSource
    row: _DecodedRow
    row_group_index: int
    row_index: int
    source_row_index: int


class _ResetAwareState:
    """The entry module's frozen one-valid-second reset rearm state."""

    __slots__ = ("candidate_last_ns", "candidate_start_ns", "reset_pending")

    def __init__(self) -> None:
        self.reset_pending = False
        self.candidate_start_ns: int | None = None
        self.candidate_last_ns: int | None = None

    def observe(self, row: _DecodedRow) -> BboInvalidReason | None:
        if row.row_invalid_reason is BboInvalidReason.RESET:
            self.reset_pending = True
            self.candidate_start_ns = None
            self.candidate_last_ns = None
            return BboInvalidReason.RESET

        ordinary_reason = row.row_invalid_reason
        if ordinary_reason is None and not row.complete:
            ordinary_reason = BboInvalidReason.MISSING_DEPTH
        if not self.reset_pending:
            return ordinary_reason
        if not row.complete:
            self.candidate_start_ns = None
            self.candidate_last_ns = None
            return ordinary_reason
        if (
            self.candidate_start_ns is None
            or self.candidate_last_ns is None
            or row.ts_recv_ns - self.candidate_last_ns > RESET_REARM_NS
        ):
            self.candidate_start_ns = row.ts_recv_ns
            self.candidate_last_ns = row.ts_recv_ns
            return BboInvalidReason.RESET_NOT_REARMED
        self.candidate_last_ns = row.ts_recv_ns
        if row.ts_recv_ns - self.candidate_start_ns >= RESET_REARM_NS:
            self.reset_pending = False
            return None
        return BboInvalidReason.RESET_NOT_REARMED


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise MultisessionReplayError(f"{label} must be a lowercase SHA-256")
    return value


def _identity(path: Path) -> _FileIdentity:
    try:
        stat = path.stat()
    except OSError as error:
        raise MultisessionReplayError(f"cannot stat continuation source: {path}") from error
    return _FileIdentity(
        size=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
        ctime_ns=stat.st_ctime_ns,
        device=stat.st_dev,
        inode=stat.st_ino,
    )


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(8 * 1024 * 1024):
                digest.update(chunk)
    except OSError as error:
        raise MultisessionReplayError(f"cannot hash continuation source: {path}") from error
    return digest.hexdigest()


def _source_date(metadata: Mapping[str, object], *, path: Path) -> date:
    start = metadata.get("start")
    if isinstance(start, bool) or not isinstance(start, int) or start < 0:
        raise MultisessionReplayError(f"{path}: dbn.metadata start must be non-negative ns")
    try:
        return datetime.fromtimestamp(start // 1_000_000_000, tz=UTC).date()
    except (OSError, OverflowError, ValueError) as error:
        raise MultisessionReplayError(f"{path}: dbn.metadata start is outside UTC") from error


def _active_instrument_id(
    raw_metadata: bytes,
    *,
    path: Path,
    source_date: date,
    raw_symbol: str,
    contract_month: date,
) -> int:
    try:
        mappings = parse_instrument_mappings(raw_metadata)
    except Mbp10ContractError as error:
        raise MultisessionReplayError(f"{path}: invalid footer mappings") from error
    active = tuple(
        mapping
        for mapping in mappings
        if mapping.interval_start <= source_date < mapping.interval_end
    )
    ids: dict[int, str] = {}
    symbols: dict[str, int] = {}
    for mapping in active:
        prior_symbol = ids.get(mapping.instrument_id)
        if prior_symbol is not None and prior_symbol != mapping.raw_symbol:
            raise MultisessionReplayError(
                f"{path}: active instrument_id {mapping.instrument_id} is ambiguous"
            )
        prior_id = symbols.get(mapping.raw_symbol)
        if prior_id is not None and prior_id != mapping.instrument_id:
            raise MultisessionReplayError(
                f"{path}: active raw symbol {mapping.raw_symbol!r} is ambiguous"
            )
        ids[mapping.instrument_id] = mapping.raw_symbol
        symbols[mapping.raw_symbol] = mapping.instrument_id

    exact = [
        mapping
        for mapping in active
        if mapping.raw_symbol == raw_symbol and mapping.kind is InstrumentKind.OUTRIGHT
    ]
    if len(exact) != 1:
        raise MultisessionReplayError(
            f"{path}: fixed raw symbol {raw_symbol!r} is not one active outright mapping"
        )
    try:
        actual_month = resolve_6e_contract_month(raw_symbol, source_date=source_date)
    except ContractSelectionError as error:
        raise MultisessionReplayError(
            f"{path}: fixed raw symbol is no longer resolvable"
        ) from error
    if actual_month != contract_month:
        raise MultisessionReplayError(
            f"{path}: fixed raw symbol resolved to {actual_month}, expected {contract_month}"
        )
    return exact[0].instrument_id


def _prepare_source(
    session: SourceSession,
    *,
    expected_sha256: str,
    raw_symbol: str,
    contract_month: date,
    event_index_offset: int,
) -> _PreparedSource:
    expanded = Path(session.parquet_path).expanduser()
    if expanded.is_symlink():
        raise MultisessionReplayError(f"continuation source cannot be a symlink: {expanded}")
    try:
        path = expanded.resolve(strict=True)
    except FileNotFoundError as error:
        raise MultisessionReplayError(f"continuation source does not exist: {expanded}") from error
    if not path.is_file() or path.suffix.lower() != ".parquet":
        raise MultisessionReplayError(f"continuation source must be a regular Parquet file: {path}")
    before = _identity(path)
    actual_sha256 = _hash_file(path)
    if actual_sha256 != expected_sha256:
        raise MultisessionReplayError(f"{path}: source SHA-256 differs from supplied manifest")
    try:
        parquet = pq.ParquetFile(path)
        contract = validate_mbp10_contract(parquet.schema_arrow)
        raw_metadata = (parquet.schema_arrow.metadata or {}).get(b"dbn.metadata")
        if raw_metadata is None:
            raise MultisessionReplayError(f"{path}: dbn.metadata is missing")
        metadata = decode_dbn_metadata(raw_metadata)
    except (OSError, pa.ArrowException, Mbp10ContractError) as error:
        raise MultisessionReplayError(f"invalid MBP-10 continuation source: {path}") from error
    if _source_date(metadata, path=path) != session.source_date:
        raise MultisessionReplayError(
            f"{path}: footer source date differs from ordered source date"
        )
    instrument_id = _active_instrument_id(
        raw_metadata,
        path=path,
        source_date=session.source_date,
        raw_symbol=raw_symbol,
        contract_month=contract_month,
    )
    after = _identity(path)
    if after != before:
        raise MultisessionReplayError(f"{path}: source identity changed during preparation")
    return _PreparedSource(
        path=path,
        source_date=session.source_date,
        source_sha256=expected_sha256,
        schema_sha256=compute_schema_fingerprint(parquet.schema_arrow, contract),
        metadata_sha256=hashlib.sha256(raw_metadata).hexdigest(),
        instrument_id=instrument_id,
        raw_symbol=raw_symbol,
        contract_month=contract_month,
        row_count=parquet.metadata.num_rows,
        row_group_count=parquet.metadata.num_row_groups,
        event_index_offset=event_index_offset,
        identity=after,
    )


def _values(table: pa.Table) -> dict[str, list[object]]:
    timestamps = pc.cast(table["ts_recv"].combine_chunks(), pa.int64()).to_pylist()
    return {
        name: timestamps if name == "ts_recv" else table[name].combine_chunks().to_pylist()
        for name in _COLUMNS
    }


def _decode_row(values: Mapping[str, list[object]], row_index: int, *, lineage: str) -> _DecodedRow:
    ts_recv_ns = int(values["ts_recv"][row_index])
    action = str(values["action"][row_index])
    flags = int(values["flags"][row_index])
    sequence = int(values["sequence"][row_index])
    bid_raw = int(values["bid_px_00"][row_index])
    ask_raw = int(values["ask_px_00"][row_index])
    bid_size = int(values["bid_sz_00"][row_index])
    ask_size = int(values["ask_sz_00"][row_index])
    if action not in _KNOWN_ACTIONS:
        raise MultisessionReplayError(f"{lineage}: unknown MBP-10 action {action!r}")
    if not 0 <= sequence <= _UINT32_MAX:
        raise MultisessionReplayError(f"{lineage}: sequence is outside uint32")

    try:
        bid_ticks = None if bid_raw == UNDEFINED_PRICE else raw_6e_price_to_ticks(bid_raw)
        ask_ticks = None if ask_raw == UNDEFINED_PRICE else raw_6e_price_to_ticks(ask_raw)
    except ValueError as error:
        raise MultisessionReplayError(f"{lineage}: off-grid 6E BBO") from error
    reason: BboInvalidReason | None = None
    if action == "R":
        reason = BboInvalidReason.RESET
    elif flags & F_MAYBE_BAD_BOOK:
        reason = BboInvalidReason.MAYBE_BAD_BOOK
    elif flags & F_BAD_TS_RECV:
        reason = BboInvalidReason.BAD_TS_RECV
    elif bid_ticks is None or ask_ticks is None:
        reason = BboInvalidReason.UNDEFINED_BBO
    elif bid_ticks == ask_ticks:
        reason = BboInvalidReason.LOCKED_BOOK
    elif bid_ticks > ask_ticks:
        reason = BboInvalidReason.CROSSED_BOOK
    return _DecodedRow(
        ts_recv_ns=ts_recv_ns,
        action=action,
        sequence=sequence,
        bid_ticks=bid_ticks,
        ask_ticks=ask_ticks,
        bid_size=bid_size,
        ask_size=ask_size,
        row_invalid_reason=reason,
    )


def _lineaged_quote(
    candidate: _TerminalCandidate,
    *,
    invalid_reason: BboInvalidReason | None,
) -> LineagedExecutableQuote:
    row = candidate.row
    return LineagedExecutableQuote(
        event_index=candidate.source.event_index_offset + candidate.source_row_index,
        ts_recv_ns=row.ts_recv_ns,
        best_bid_ticks=row.bid_ticks,
        best_ask_ticks=row.ask_ticks,
        valid=invalid_reason is None,
        source_date=candidate.source.source_date,
        source_path=str(candidate.source.path),
        source_sha256=candidate.source.source_sha256,
        instrument_id=candidate.source.instrument_id,
        row_group_index=candidate.row_group_index,
        row_index=candidate.row_index,
        source_row_index=candidate.source_row_index,
        sequence=row.sequence,
        invalid_reason=invalid_reason,
    )


def _reverse_terminal(
    sources: Sequence[_PreparedSource],
) -> tuple[LineagedExecutableQuote | None, TerminalLookupAudit]:
    opened: list[date] = []
    row_groups_read = 0
    rows_examined = 0
    terminal: _TerminalCandidate | None = None

    for source in reversed(sources):
        if _identity(source.path) != source.identity:
            raise MultisessionReplayError(f"{source.path}: source identity drifted before lookup")
        try:
            parquet = pq.ParquetFile(source.path)
        except (OSError, pa.ArrowException) as error:
            raise MultisessionReplayError(f"cannot open terminal source: {source.path}") from error
        opened.append(source.source_date)
        candidate: _TerminalCandidate | None = None
        run_newest_ns: int | None = None
        run_oldest_ns: int | None = None
        rearmed = False
        newer_selected_ts: int | None = None
        row_group_offsets: list[int] = []
        row_group_offset = 0
        for row_group_index in range(parquet.metadata.num_row_groups):
            row_group_offsets.append(row_group_offset)
            row_group_offset += parquet.metadata.row_group(row_group_index).num_rows
        try:
            for row_group_index in reversed(range(parquet.metadata.num_row_groups)):
                table = parquet.read_row_group(
                    row_group_index,
                    columns=list(_COLUMNS),
                    use_threads=False,
                )
                row_groups_read += 1
                rows_examined += table.num_rows
                values = _values(table)
                for row_index in reversed(range(table.num_rows)):
                    if int(values["instrument_id"][row_index]) != source.instrument_id:
                        continue
                    source_row_index = row_group_offsets[row_group_index] + row_index
                    row = _decode_row(
                        values,
                        row_index,
                        lineage=f"{source.path}: row {source_row_index}",
                    )
                    if newer_selected_ts is not None and row.ts_recv_ns > newer_selected_ts:
                        raise MultisessionReplayError(
                            f"{source.path}: fixed-symbol ts_recv regressed in physical order"
                        )
                    newer_selected_ts = row.ts_recv_ns
                    item = _TerminalCandidate(
                        source=source,
                        row=row,
                        row_group_index=row_group_index,
                        row_index=row_index,
                        source_row_index=source_row_index,
                    )
                    if candidate is None and row.complete:
                        candidate = item
                    if row.complete:
                        if run_newest_ns is None or run_oldest_ns is None:
                            run_newest_ns = row.ts_recv_ns
                            run_oldest_ns = row.ts_recv_ns
                        elif run_oldest_ns - row.ts_recv_ns <= RESET_REARM_NS:
                            run_oldest_ns = row.ts_recv_ns
                        else:
                            run_newest_ns = row.ts_recv_ns
                            run_oldest_ns = row.ts_recv_ns
                        if run_newest_ns - run_oldest_ns >= RESET_REARM_NS:
                            rearmed = True
                            terminal = candidate
                            break
                    else:
                        run_newest_ns = None
                        run_oldest_ns = None
                    if row.row_invalid_reason is BboInvalidReason.RESET:
                        if candidate is not None and rearmed:
                            terminal = candidate
                            break
                        candidate = None
                        rearmed = False
                if terminal is not None:
                    break
            if terminal is None and candidate is not None:
                # Like entry replay, each independently requested daily source starts armed.
                terminal = candidate
        except (OSError, pa.ArrowException) as error:
            raise MultisessionReplayError(
                f"cannot reverse-read terminal columns from {source.path}"
            ) from error
        if terminal is not None:
            break

    quote = _lineaged_quote(terminal, invalid_reason=None) if terminal is not None else None
    audit = TerminalLookupAudit(
        method="BOUNDED_REVERSE_DAILY_ROW_GROUP_SCAN",
        source_dates_opened=tuple(opened),
        row_groups_read=row_groups_read,
        rows_examined=rows_examined,
        terminal_event_index=quote.event_index if quote is not None else None,
        terminal_source_date=quote.source_date if quote is not None else None,
        terminal_row_group_index=quote.row_group_index if quote is not None else None,
        terminal_row_index=quote.row_index if quote is not None else None,
    )
    return quote, audit


def _reopen_for_stream(source: _PreparedSource) -> pq.ParquetFile:
    if _identity(source.path) != source.identity:
        raise MultisessionReplayError(f"{source.path}: source identity drifted before streaming")
    try:
        parquet = pq.ParquetFile(source.path)
        contract = validate_mbp10_contract(parquet.schema_arrow)
    except (OSError, pa.ArrowException, Mbp10ContractError) as error:
        raise MultisessionReplayError(f"cannot reopen MBP-10 source: {source.path}") from error
    raw_metadata = (parquet.schema_arrow.metadata or {}).get(b"dbn.metadata")
    if raw_metadata is None:
        raise MultisessionReplayError(f"{source.path}: dbn.metadata disappeared")
    if (
        parquet.metadata.num_rows != source.row_count
        or parquet.metadata.num_row_groups != source.row_group_count
        or compute_schema_fingerprint(parquet.schema_arrow, contract) != source.schema_sha256
        or hashlib.sha256(raw_metadata).hexdigest() != source.metadata_sha256
    ):
        raise MultisessionReplayError(f"{source.path}: footer identity drifted before streaming")
    instrument_id = _active_instrument_id(
        raw_metadata,
        path=source.path,
        source_date=source.source_date,
        raw_symbol=source.raw_symbol,
        contract_month=source.contract_month,
    )
    if instrument_id != source.instrument_id:
        raise MultisessionReplayError(f"{source.path}: fixed-symbol instrument mapping drifted")
    return parquet


class MultisessionExecutablePath:
    """Single-consumption entry-plus-later-sessions quote iterator.

    ``terminal_event`` is known before iteration and is excluded from ordinary
    events, allowing direct use as ``events=path, terminal_event=path.terminal_event``.
    Later daily files are opened once by the forward iterator and read one row
    group at a time.  Integrity hashing, footer preparation, and the recorded
    bounded reverse terminal lookup are separate prerequisite passes.
    """

    __slots__ = (
        "_consumed",
        "_entry",
        "_source_files_streamed",
        "_source_row_groups_streamed",
        "_source_rows_streamed",
        "_sources",
        "audit",
        "terminal_event",
    )

    def __init__(
        self,
        *,
        entry: EntryReplayResult,
        sources: tuple[_PreparedSource, ...],
        terminal_event: LineagedExecutableQuote | None,
        audit: MultisessionAudit,
    ) -> None:
        self._entry = entry
        self._sources = sources
        self.terminal_event = terminal_event
        self.audit = audit
        self._consumed = False
        self._source_files_streamed = 0
        self._source_row_groups_streamed = 0
        self._source_rows_streamed = 0

    @property
    def consumed(self) -> bool:
        return self._consumed

    @property
    def source_files_streamed(self) -> int:
        return self._source_files_streamed

    @property
    def source_row_groups_streamed(self) -> int:
        return self._source_row_groups_streamed

    @property
    def source_rows_streamed(self) -> int:
        return self._source_rows_streamed

    def __iter__(self) -> Iterator[ExecutableQuote]:
        if self._consumed:
            raise MultisessionReplayError("multisession path may be consumed only once")
        self._consumed = True
        return self._iterate()

    def _iterate(self) -> Iterator[ExecutableQuote]:
        previous: ExecutableQuote | None = None
        entry_path = self._entry.executable_path
        if entry_path is None:  # guarded at preparation
            raise MultisessionReplayError("filled entry lost its executable path")
        try:
            for event in entry_path:
                if not isinstance(event, ExecutableQuote):
                    raise MultisessionReplayError("entry path contains a non-executable quote")
                if previous is not None and (
                    event.event_index <= previous.event_index
                    or event.ts_recv_ns < previous.ts_recv_ns
                ):
                    raise MultisessionReplayError("entry path canonical order drifted")
                previous = event
                yield event
        except ValueError as error:
            raise MultisessionReplayError("entry executable path cannot be extended") from error

        terminal_index = (
            self.terminal_event.event_index if self.terminal_event is not None else None
        )
        for source in self._sources:
            if terminal_index is not None and source.event_index_offset > terminal_index:
                break
            parquet = _reopen_for_stream(source)
            self._source_files_streamed += 1
            source_row_offset = 0
            state = _ResetAwareState()
            try:
                for row_group_index in range(parquet.metadata.num_row_groups):
                    table = parquet.read_row_group(
                        row_group_index,
                        columns=list(_COLUMNS),
                        use_threads=False,
                    )
                    self._source_row_groups_streamed += 1
                    self._source_rows_streamed += table.num_rows
                    values = _values(table)
                    for row_index in range(table.num_rows):
                        source_row_index = source_row_offset + row_index
                        event_index = source.event_index_offset + source_row_index
                        if terminal_index is not None and event_index >= terminal_index:
                            return
                        if int(values["instrument_id"][row_index]) != source.instrument_id:
                            continue
                        row = _decode_row(
                            values,
                            row_index,
                            lineage=f"{source.path}: row {source_row_index}",
                        )
                        invalid_reason = state.observe(row)
                        event = _lineaged_quote(
                            _TerminalCandidate(
                                source=source,
                                row=row,
                                row_group_index=row_group_index,
                                row_index=row_index,
                                source_row_index=source_row_index,
                            ),
                            invalid_reason=invalid_reason,
                        )
                        if previous is not None and event.event_index <= previous.event_index:
                            raise MultisessionReplayError(
                                "canonical event indexes are not strictly increasing"
                            )
                        if previous is not None and event.ts_recv_ns < previous.ts_recv_ns:
                            raise MultisessionReplayError(
                                "fixed-symbol ts_recv regressed across source sessions"
                            )
                        previous = event
                        yield event
                    source_row_offset += table.num_rows
            except (OSError, pa.ArrowException) as error:
                raise MultisessionReplayError(
                    f"cannot stream continuation columns from {source.path}"
                ) from error
            if source_row_offset != source.row_count:
                raise MultisessionReplayError(
                    f"{source.path}: streamed row count differs from footer"
                )
            if _identity(source.path) != source.identity:
                raise MultisessionReplayError(
                    f"{source.path}: source identity drifted while streaming"
                )


def _normalize_sha_map(
    values: Mapping[date | str, str],
    source_dates: tuple[date, ...],
) -> dict[date, str]:
    if not isinstance(values, Mapping):
        raise MultisessionReplayError("source_sha256_by_date must be a mapping")
    normalized: dict[date, str] = {}
    for raw_key, raw_sha in values.items():
        if isinstance(raw_key, datetime):
            raise MultisessionReplayError("source SHA keys must be dates, not datetimes")
        if isinstance(raw_key, date):
            key = raw_key
        elif isinstance(raw_key, str):
            try:
                key = date.fromisoformat(raw_key)
            except ValueError as error:
                raise MultisessionReplayError(
                    f"invalid source SHA date key: {raw_key!r}"
                ) from error
        else:
            raise MultisessionReplayError("source SHA keys must be dates or ISO date strings")
        if key in normalized:
            raise MultisessionReplayError(f"duplicate normalized source SHA key: {key}")
        normalized[key] = _sha(raw_sha, label=f"source SHA for {key}")
    if set(normalized) != set(source_dates):
        raise MultisessionReplayError("source SHA map must exactly match ordered source dates")
    return normalized


def build_multisession_path(
    *,
    entry: EntryReplayResult,
    sources: Sequence[SourceSession],
    source_sha256_by_date: Mapping[date | str, str],
    source_manifest_sha256: str,
) -> MultisessionExecutablePath:
    """Prepare an immutable fixed-contract continuation and lazy event stream.

    Sources must be strictly increasing and later than the entry source.  At
    most 20 pre-expiry sources are accepted.  The first source in or after the
    frozen contract's expiry month and everything after it are recorded but
    never opened, hashed, mapped, or streamed.
    """

    if not isinstance(entry, EntryReplayResult):
        raise MultisessionReplayError("entry must be an EntryReplayResult")
    if (
        entry.status is not EntryStatus.ENTRY_FILLED
        or entry.fill_quantity_contracts != 1
        or entry.fill_price_ticks is None
        or entry.fill_price_raw is None
        or entry.executable_path is None
    ):
        raise MultisessionReplayError("multisession continuation requires one filled entry")
    if entry.executable_path.terminal_quote is not None:
        raise MultisessionReplayError(
            "entry path already has a terminal cutoff and cannot be losslessly extended"
        )
    if not isinstance(sources, Sequence) or isinstance(sources, (str, bytes)) or not sources:
        raise MultisessionReplayError("sources must be a non-empty ordered sequence")
    ordered = tuple(sources)
    if any(not isinstance(item, SourceSession) for item in ordered):
        raise MultisessionReplayError("sources must contain only SourceSession values")
    source_dates = tuple(item.source_date for item in ordered)
    if any(left >= right for left, right in pairwise(source_dates)):
        raise MultisessionReplayError("source dates must be strictly increasing")
    if source_dates[0] <= entry.audit.eligible_source_date:
        raise MultisessionReplayError(
            "all continuation sources must be later than the entry source"
        )

    manifest_sha = _sha(source_manifest_sha256, label="source_manifest_sha256")
    sha_by_date = _normalize_sha_map(source_sha256_by_date, source_dates)
    raw_symbol = entry.audit.selected_raw_symbol
    contract_month = entry.audit.selected_contract_month
    if contract_month.day != 1:
        raise MultisessionReplayError("entry contract month must be normalized to its first day")
    try:
        entry_month = resolve_6e_contract_month(
            raw_symbol,
            source_date=entry.audit.eligible_source_date,
        )
    except ContractSelectionError as error:
        raise MultisessionReplayError(
            "entry raw symbol is not a parseable fixed 6E contract"
        ) from error
    if entry_month != contract_month:
        raise MultisessionReplayError("entry raw symbol and frozen contract month differ")

    included_specs: list[SourceSession] = []
    expiry_cutoff: date | None = None
    for session in ordered:
        source_month = date(session.source_date.year, session.source_date.month, 1)
        if source_month >= contract_month:
            expiry_cutoff = session.source_date
            break
        included_specs.append(session)
    if len(included_specs) > MAX_OBSERVATION_SOURCE_SESSIONS:
        raise MultisessionReplayError(
            f"observation window exceeds {MAX_OBSERVATION_SOURCE_SESSIONS} source sessions"
        )
    if not included_specs:
        raise MultisessionReplayError("expiry cutoff leaves no later eligible source session")
    excluded_dates = source_dates[len(included_specs) :]
    cutoff_reason = (
        CutoffReason.EXPIRY_MONTH
        if expiry_cutoff is not None
        else CutoffReason.OBSERVATION_WINDOW_END
    )

    prepared: list[_PreparedSource] = []
    event_index_offset = entry.audit.source_footer_rows
    for session in included_specs:
        source = _prepare_source(
            session,
            expected_sha256=sha_by_date[session.source_date],
            raw_symbol=raw_symbol,
            contract_month=contract_month,
            event_index_offset=event_index_offset,
        )
        prepared.append(source)
        event_index_offset += source.row_count
    terminal_event, terminal_lookup = _reverse_terminal(prepared)
    source_audits = tuple(
        SourceAudit(
            source_date=item.source_date,
            source_path=str(item.path),
            source_sha256=item.source_sha256,
            schema_sha256=item.schema_sha256,
            dbn_metadata_sha256=item.metadata_sha256,
            instrument_id=item.instrument_id,
            raw_symbol=item.raw_symbol,
            row_count=item.row_count,
            row_group_count=item.row_group_count,
            event_index_offset=item.event_index_offset,
        )
        for item in prepared
    )
    document: dict[str, object] = {
        "artifact_schema": MULTISESSION_PATH_SCHEMA,
        "contract": {
            "contract_month": contract_month.isoformat(),
            "raw_symbol": raw_symbol,
            "selection": "FIXED_ENTRY_RAW_SYMBOL_FOOTER_ID_ONLY",
        },
        "cutoff": {
            "excluded_source_dates": [value.isoformat() for value in excluded_dates],
            "expiry_cutoff_source_date": (
                expiry_cutoff.isoformat() if expiry_cutoff is not None else None
            ),
            "maximum_source_sessions": MAX_OBSERVATION_SOURCE_SESSIONS,
            "reason": cutoff_reason.value,
        },
        "entry_sha256": entry.sha256,
        "source_manifest_sha256": manifest_sha,
        "source_sha256_by_date": {value.isoformat(): sha_by_date[value] for value in source_dates},
        "sources": [item.as_dict() for item in source_audits],
        "supplied_source_dates": [value.isoformat() for value in source_dates],
        "terminal_lookup": terminal_lookup.as_dict(),
    }
    canonical = _canonical_bytes(document)
    audit = MultisessionAudit(
        entry_sha256=entry.sha256,
        source_manifest_sha256=manifest_sha,
        raw_symbol=raw_symbol,
        contract_month=contract_month,
        maximum_source_sessions=MAX_OBSERVATION_SOURCE_SESSIONS,
        cutoff_reason=cutoff_reason,
        expiry_cutoff_source_date=expiry_cutoff,
        supplied_source_dates=source_dates,
        excluded_source_dates=excluded_dates,
        sources=source_audits,
        terminal_lookup=terminal_lookup,
        canonical_bytes=canonical,
        sha256=hashlib.sha256(canonical).hexdigest(),
    )
    return MultisessionExecutablePath(
        entry=entry,
        sources=tuple(prepared),
        terminal_event=terminal_event,
        audit=audit,
    )


__all__ = [
    "MAX_OBSERVATION_SOURCE_SESSIONS",
    "MULTISESSION_PATH_SCHEMA",
    "CutoffReason",
    "LineagedExecutableQuote",
    "MultisessionAudit",
    "MultisessionExecutablePath",
    "MultisessionReplayError",
    "SourceAudit",
    "SourceSession",
    "TerminalLookupAudit",
    "build_multisession_path",
]
