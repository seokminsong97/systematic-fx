"""Point-in-time Phase 1A entry gate and executable quote-path reader.

Contract choice is an input to this module, never an inference from the execution
source.  The selected instrument's rows are consumed in physical Parquet order.
The returned quote path is materialized by that single pass and may itself be
consumed only once by :mod:`systematic_fx.backtest.barriers`.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from systematic_fx.backtest.barriers import (
    BARRIER_TICKS,
    STOP_LATENCY_NS,
    STOP_MINIMUM_ADVERSE_TICKS,
    TAKE_PROFIT_TRADE_THROUGH_TICKS,
    Direction,
    ExecutableQuote,
)
from systematic_fx.backtest.economics import EntryStatus
from systematic_fx.data.contract_selection import (
    CONTRACT_SELECTION_POLICY_VERSION,
    ContractSelectionResult,
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
from systematic_fx.research.screening_config import ConservativeScreeningBundle

ENTRY_ARTIFACT_SCHEMA: Final = "systematic_fx.phase1a_entry.v1"
FROZEN_EXECUTION_POLICY_ID: Final = "phase1a_conservative_execution_v1"
ROUTING_DELAY_NS: Final = 1_000_000_000
MAX_QUOTE_AGE_NS: Final = 1_000_000_000
FIVE_MINUTE_NS: Final = 300_000_000_000
SIX_E_TICK_RAW: Final = 50_000

F_MAYBE_BAD_BOOK: Final = 4
F_BAD_TS_RECV: Final = 8
F_SNAPSHOT: Final = 32
_KNOWN_ACTIONS: Final = frozenset({"A", "C", "F", "M", "N", "R", "T"})
_SHA256_LENGTH: Final = 64
_UINT32_MAX: Final = 2**32 - 1
_INT64_MIN: Final = -(2**63)
_INT64_MAX: Final = 2**63 - 1
_BOOK_COLUMNS: Final = tuple(
    f"{side}_{kind}_{level:02d}"
    for level in range(10)
    for side in ("bid", "ask")
    for kind in ("px", "sz", "ct")
)
_ENTRY_COLUMNS: Final = (
    "ts_recv",
    "instrument_id",
    "action",
    "side",
    "flags",
    "sequence",
    *_BOOK_COLUMNS,
)


class EntryReplayError(ValueError):
    """The supplied selection, policy, source, or price path is not replayable."""


class EntryReason(StrEnum):
    """Auditable terminal reason for an entry attempt."""

    FILLED_AT_DELAYED_OPPOSITE_BBO = "FILLED_AT_DELAYED_OPPOSITE_BBO"
    NO_DECISION_QUOTE = "NO_DECISION_QUOTE"
    STALE_DECISION_QUOTE = "STALE_DECISION_QUOTE"
    INVALID_DECISION_BBO = "INVALID_DECISION_BBO"
    STALE_BBO_DURING_ROUTE = "STALE_BBO_DURING_ROUTE"
    INVALID_BBO_DURING_ROUTE = "INVALID_BBO_DURING_ROUTE"
    RESET_DURING_ROUTE = "RESET_DURING_ROUTE"
    NO_ENTRY_ELIGIBILITY_EVENT = "NO_ENTRY_ELIGIBILITY_EVENT"
    INVALID_ENTRY_ELIGIBILITY_BBO = "INVALID_ENTRY_ELIGIBILITY_BBO"
    INVALID_ENTRY_ATTEMPT_BBO = "INVALID_ENTRY_ATTEMPT_BBO"
    INSUFFICIENT_EXECUTABLE_SIZE = "INSUFFICIENT_EXECUTABLE_SIZE"
    PRICE_OUTSIDE_LIMIT = "PRICE_OUTSIDE_LIMIT"


class BboInvalidReason(StrEnum):
    """Row/state reason an observed BBO is not executable."""

    UNDEFINED_BBO = "UNDEFINED_BBO"
    LOCKED_BOOK = "LOCKED_BOOK"
    CROSSED_BOOK = "CROSSED_BOOK"
    MAYBE_BAD_BOOK = "MAYBE_BAD_BOOK"
    BAD_TS_RECV = "BAD_TS_RECV"
    RESET = "RESET"
    RESET_NOT_REARMED = "RESET_NOT_REARMED"
    MISSING_DEPTH = "MISSING_DEPTH"
    INVALID_BOOK_STRUCTURE = "INVALID_BOOK_STRUCTURE"
    INVALID_RECOVERY_MARKER = "INVALID_RECOVERY_MARKER"
    SOURCE_STATE_UNKNOWN = "SOURCE_STATE_UNKNOWN"
    RECOVERY_MARKER = "RECOVERY_MARKER"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"
    RECOVERY_GAP = "RECOVERY_GAP"


@dataclass(frozen=True, slots=True)
class EntryEventReference:
    """Exact physical source row and BBO state used by the entry audit."""

    event_index: int
    row_group_index: int
    row_index: int
    ts_recv_ns: int
    sequence: int
    action: str
    side: str
    flags: int
    snapshot: bool
    book_structurally_valid: bool
    book_empty: bool
    valid_recovery_marker: bool
    bid_price_raw: int
    ask_price_raw: int
    bid_price_ticks: int | None
    ask_price_ticks: int | None
    bid_size: int
    ask_size: int
    row_invalid_reasons: tuple[BboInvalidReason, ...]
    row_invalid_reason: BboInvalidReason | None
    invalid_reasons: tuple[BboInvalidReason, ...]
    invalid_reason: BboInvalidReason | None

    @property
    def valid(self) -> bool:
        return self.invalid_reason is None

    def as_dict(self) -> dict[str, object]:
        return {
            "action": self.action,
            "ask_price_raw": self.ask_price_raw,
            "ask_price_ticks": self.ask_price_ticks,
            "ask_size": self.ask_size,
            "bid_price_raw": self.bid_price_raw,
            "bid_price_ticks": self.bid_price_ticks,
            "bid_size": self.bid_size,
            "event_index": self.event_index,
            "flags": self.flags,
            "book_empty": self.book_empty,
            "book_structurally_valid": self.book_structurally_valid,
            "invalid_reason": (
                self.invalid_reason.value if self.invalid_reason is not None else None
            ),
            "invalid_reasons": [reason.value for reason in self.invalid_reasons],
            "row_group_index": self.row_group_index,
            "row_index": self.row_index,
            "row_invalid_reason": (
                self.row_invalid_reason.value if self.row_invalid_reason is not None else None
            ),
            "row_invalid_reasons": [reason.value for reason in self.row_invalid_reasons],
            "sequence": self.sequence,
            "side": self.side,
            "snapshot": self.snapshot,
            "ts_recv_ns": self.ts_recv_ns,
            "valid": self.valid,
            "valid_recovery_marker": self.valid_recovery_marker,
        }


class ExecutableQuotePath:
    """Single-consumption barrier path built by one physical source pass.

    ``terminal_quote`` is excluded from the ordinary events.  It is the last
    valid executable quote strictly before the requested terminal cutoff, so it
    can be passed separately to ``replay_barrier_surface``.
    """

    __slots__ = (
        "_consumed",
        "_events",
        "source_path_passes",
        "terminal_quote",
        "terminal_reference",
    )

    def __init__(
        self,
        events: tuple[ExecutableQuote, ...],
        *,
        terminal_quote: ExecutableQuote | None,
        terminal_reference: EntryEventReference | None,
    ) -> None:
        self._events = events
        self.terminal_quote = terminal_quote
        self.terminal_reference = terminal_reference
        self.source_path_passes = 1
        self._consumed = False

    def __iter__(self) -> Iterator[ExecutableQuote]:
        if self._consumed:
            raise EntryReplayError("executable quote path may be consumed only once")
        self._consumed = True
        return iter(self._events)

    @property
    def event_count(self) -> int:
        return len(self._events)

    @property
    def valid_event_count(self) -> int:
        return sum(event.valid for event in self._events)


@dataclass(frozen=True, slots=True)
class EntryAudit:
    """Immutable selection, policy, source, gate, and event references."""

    source_path: str
    source_schema_sha256: str
    source_dbn_metadata_sha256: str
    source_footer_rows: int
    source_footer_row_groups: int
    source_rows_examined: int
    source_row_groups_read: int
    selected_instrument_id: int
    selected_raw_symbol: str
    selected_contract_month: date
    previous_source_date: date
    eligible_source_date: date
    selection_sha256: str
    previous_volume_sha256: str
    contract_selection_policy_version: str
    execution_policy_id: str
    execution_policy_sha256: str
    screening_bundle_sha256: str
    decision_ts_recv_ns: int
    entry_eligibility_ts_recv_ns: int
    decision_event: EntryEventReference | None
    eligibility_snapshot: EntryEventReference | None
    attempt_event: EntryEventReference | None
    entry_limit_side: str | None
    entry_limit_price_raw: int | None
    entry_limit_price_ticks: int | None
    failure_event: EntryEventReference | None
    route_event_count: int
    maximum_route_quote_gap_ns: int
    observed_invalid_reasons: tuple[BboInvalidReason, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {
            "contract": {
                "contract_month": self.selected_contract_month.isoformat(),
                "instrument_id": self.selected_instrument_id,
                "raw_symbol": self.selected_raw_symbol,
            },
            "entry_gate": {
                "decision_event": (
                    self.decision_event.as_dict() if self.decision_event is not None else None
                ),
                "attempt_event": (
                    self.attempt_event.as_dict() if self.attempt_event is not None else None
                ),
                "eligibility_snapshot": (
                    self.eligibility_snapshot.as_dict()
                    if self.eligibility_snapshot is not None
                    else None
                ),
                "entry_eligibility_ts_recv_ns": self.entry_eligibility_ts_recv_ns,
                "entry_limit": (
                    {
                        "price_raw": self.entry_limit_price_raw,
                        "price_ticks": self.entry_limit_price_ticks,
                        "side": self.entry_limit_side,
                    }
                    if self.entry_limit_price_ticks is not None
                    else None
                ),
                "failure_event": (
                    self.failure_event.as_dict() if self.failure_event is not None else None
                ),
                "maximum_route_quote_gap_ns": self.maximum_route_quote_gap_ns,
                "missing_status_fallback": "OBSERVED_ACTIVITY_AND_VALID_FRESH_BOOK",
                "observed_invalid_reasons": [
                    reason.value for reason in self.observed_invalid_reasons
                ],
                "reference_trading_status_available": False,
                "route_event_count": self.route_event_count,
            },
            "information_boundary": {
                "contract_selection_policy_version": self.contract_selection_policy_version,
                "eligible_source_date": self.eligible_source_date.isoformat(),
                "previous_volume_sha256": self.previous_volume_sha256,
                "previous_source_date": self.previous_source_date.isoformat(),
                "selection_sha256": self.selection_sha256,
            },
            "policy": {
                "execution_policy_id": self.execution_policy_id,
                "execution_policy_sha256": self.execution_policy_sha256,
                "screening_bundle_sha256": self.screening_bundle_sha256,
            },
            "source": {
                "dbn_metadata_sha256": self.source_dbn_metadata_sha256,
                "footer_row_groups": self.source_footer_row_groups,
                "footer_rows": self.source_footer_rows,
                "path": self.source_path,
                "row_groups_read": self.source_row_groups_read,
                "rows_examined": self.source_rows_examined,
                "schema_sha256": self.source_schema_sha256,
            },
            "decision_ts_recv_ns": self.decision_ts_recv_ns,
        }


@dataclass(frozen=True, slots=True)
class EntryReplayResult:
    """Entry outcome plus a barrier-compatible post-fill path when filled."""

    status: EntryStatus
    reason: EntryReason
    direction: Direction
    fill_price_ticks: int | None
    fill_price_raw: int | None
    fill_quantity_contracts: int
    executable_path: ExecutableQuotePath | None
    audit: EntryAudit
    canonical_bytes: bytes
    sha256: str

    def as_dict(self) -> dict[str, object]:
        value = json.loads(self.canonical_bytes)
        if not isinstance(value, dict):
            raise EntryReplayError("canonical entry result is not an object")
        return value


@dataclass(frozen=True, slots=True)
class _SourceContext:
    path: Path
    parquet: pq.ParquetFile
    request_start_ns: int
    request_end_ns: int
    schema_sha256: str
    metadata_sha256: str


@dataclass(frozen=True, slots=True)
class _PathItem:
    quote: ExecutableQuote
    reference: EntryEventReference


@dataclass(slots=True)
class _ScanState:
    rows_examined: int = 0
    row_groups_read: int = 0
    route_event_count: int = 0
    maximum_route_quote_gap_ns: int = 0
    decision_event: EntryEventReference | None = None
    eligibility_snapshot: EntryEventReference | None = None
    attempt_event: EntryEventReference | None = None
    entry_limit_side: str | None = None
    entry_limit_price_raw: int | None = None
    entry_limit_price_ticks: int | None = None
    failure_event: EntryEventReference | None = None
    observed_invalid_reasons: set[BboInvalidReason] = field(default_factory=set)


class _ResetAwareBook:
    """Conservative right-closed recovery state shared with feature construction.

    A source begins unknown.  A structurally valid snapshot or empty ``R/N``
    reset is only a recovery marker; the marker's observed second stays invalid.
    Recovery completes when the exactly adjacent observed second is proven closed
    and its final BBO and whole-second flags are clean.  Ordinary rows never invent
    trusted state, and a skipped/dirty candidate requires a new marker.
    """

    __slots__ = (
        "_bucket_end_ns",
        "_bucket_finalized",
        "_bucket_flags_or",
        "_bucket_marker_seen",
        "_bucket_reset_seen",
        "_current",
        "_expected_rearm_bucket_ns",
        "_last_recovery_outcome",
        "_recovery_origin",
        "_recovery_required",
    )

    def __init__(self) -> None:
        self._bucket_end_ns: int | None = None
        self._bucket_finalized = False
        self._bucket_flags_or = 0
        self._bucket_marker_seen = False
        self._bucket_reset_seen = False
        self._current: EntryEventReference | None = None
        self._expected_rearm_bucket_ns: int | None = None
        self._last_recovery_outcome: tuple[str, BboInvalidReason] | None = None
        self._recovery_origin = BboInvalidReason.SOURCE_STATE_UNKNOWN
        self._recovery_required = True

    @property
    def reset_pending(self) -> bool:
        return self._recovery_required and self._recovery_origin in {
            BboInvalidReason.RESET,
            BboInvalidReason.RESET_NOT_REARMED,
        }

    @staticmethod
    def _bucket_end(timestamp_ns: int) -> int:
        return -(-timestamp_ns // ROUTING_DELAY_NS) * ROUTING_DELAY_NS

    @staticmethod
    def _unique_reasons(
        *groups: tuple[BboInvalidReason, ...],
    ) -> tuple[BboInvalidReason, ...]:
        return tuple(dict.fromkeys(reason for group in groups for reason in group))

    def _decorated(self, reference: EntryEventReference) -> EntryEventReference:
        state_reasons: tuple[BboInvalidReason, ...] = ()
        if self._bucket_marker_seen:
            state_reasons += (BboInvalidReason.RECOVERY_MARKER,)
        if self._recovery_required:
            origin = self._recovery_origin
            if origin is BboInvalidReason.RESET:
                origin = BboInvalidReason.RESET_NOT_REARMED
            state_reasons += (origin, BboInvalidReason.RECOVERY_REQUIRED)
        reasons = self._unique_reasons(reference.row_invalid_reasons, state_reasons)
        return replace(
            reference,
            invalid_reasons=reasons,
            invalid_reason=reasons[0] if reasons else None,
        )

    def _base_book_valid(self) -> bool:
        reference = self._current
        arithmetic_valid = bool(
            reference is not None
            and _INT64_MIN <= reference.bid_price_raw + reference.ask_price_raw <= _INT64_MAX
            and _INT64_MIN <= reference.ask_price_raw - reference.bid_price_raw <= _INT64_MAX
        )
        return bool(
            reference is not None
            and reference.book_structurally_valid
            and reference.bid_price_ticks is not None
            and reference.ask_price_ticks is not None
            and reference.bid_price_ticks < reference.ask_price_ticks
            and reference.bid_size >= 1
            and reference.ask_size >= 1
            and arithmetic_valid
            and not self._bucket_marker_seen
            and not self._bucket_reset_seen
            and not self._bucket_flags_or & (F_MAYBE_BAD_BOOK | F_BAD_TS_RECV)
        )

    def _candidate_failure_origin(self) -> BboInvalidReason:
        reference = self._current
        if self._bucket_flags_or & F_MAYBE_BAD_BOOK:
            return BboInvalidReason.MAYBE_BAD_BOOK
        if self._bucket_flags_or & F_BAD_TS_RECV:
            return BboInvalidReason.BAD_TS_RECV
        if reference is not None and reference.row_invalid_reason is not None:
            return reference.row_invalid_reason
        return BboInvalidReason.RECOVERY_REQUIRED

    def _finalize_bucket(self) -> None:
        if self._bucket_end_ns is None or self._bucket_finalized:
            return
        outcome = self._last_recovery_outcome
        if outcome is not None and outcome[0] == "RECOVERED":
            self._recovery_required = True
            self._recovery_origin = outcome[1]
            self._expected_rearm_bucket_ns = self._bucket_end_ns + ROUTING_DELAY_NS
        elif outcome is not None:
            self._recovery_required = True
            self._recovery_origin = outcome[1]
            self._expected_rearm_bucket_ns = None
        elif self._recovery_required and self._expected_rearm_bucket_ns is not None:
            if self._expected_rearm_bucket_ns == self._bucket_end_ns and self._base_book_valid():
                self._recovery_required = False
                self._recovery_origin = BboInvalidReason.RECOVERY_REQUIRED
            else:
                self._recovery_origin = self._candidate_failure_origin()
            self._expected_rearm_bucket_ns = None
        self._bucket_finalized = True

    def _start_bucket(self, bucket_end_ns: int) -> None:
        self._finalize_bucket()
        if (
            self._recovery_required
            and self._expected_rearm_bucket_ns is not None
            and self._expected_rearm_bucket_ns != bucket_end_ns
        ):
            self._expected_rearm_bucket_ns = None
            self._recovery_origin = BboInvalidReason.RECOVERY_GAP
        self._bucket_end_ns = bucket_end_ns
        self._bucket_finalized = False
        self._bucket_flags_or = 0
        self._bucket_marker_seen = False
        self._bucket_reset_seen = False
        self._last_recovery_outcome = None

    def observe(self, reference: EntryEventReference) -> EntryEventReference:
        bucket_end_ns = self._bucket_end(reference.ts_recv_ns)
        if self._bucket_end_ns != bucket_end_ns:
            self._start_bucket(bucket_end_ns)

        self._current = reference
        self._bucket_flags_or |= reference.flags
        marker_seen = reference.snapshot or reference.action == "R"
        self._bucket_marker_seen |= marker_seen
        self._bucket_reset_seen |= reference.action == "R"

        persistent_reason: BboInvalidReason | None = None
        if reference.flags & F_MAYBE_BAD_BOOK:
            persistent_reason = BboInvalidReason.MAYBE_BAD_BOOK
        elif not reference.book_structurally_valid:
            persistent_reason = BboInvalidReason.INVALID_BOOK_STRUCTURE
        elif marker_seen and not reference.valid_recovery_marker:
            persistent_reason = BboInvalidReason.INVALID_RECOVERY_MARKER

        if persistent_reason is not None:
            self._last_recovery_outcome = ("INVALIDATED", persistent_reason)
            self._recovery_required = True
            self._recovery_origin = persistent_reason
            self._expected_rearm_bucket_ns = None
        elif reference.valid_recovery_marker:
            origin = (
                BboInvalidReason.RESET
                if reference.action == "R"
                else BboInvalidReason.RECOVERY_MARKER
            )
            self._last_recovery_outcome = ("RECOVERED", origin)
            self._recovery_required = True
            self._recovery_origin = origin
            self._expected_rearm_bucket_ns = None

        return self._decorated(reference)

    def reference_at(
        self,
        timestamp_ns: int,
        *,
        boundary_proven: bool = False,
    ) -> EntryEventReference | None:
        reference = self._current
        if reference is None:
            return None
        if (
            boundary_proven
            and self._bucket_end_ns is not None
            and self._bucket_end_ns <= timestamp_ns
        ):
            self._finalize_bucket()
        return self._decorated(reference)


def raw_6e_price_to_ticks(raw_price: int) -> int:
    """Convert Databento's 1e-9 integer price to exact 6E ticks."""

    if isinstance(raw_price, bool) or not isinstance(raw_price, int):
        raise EntryReplayError("raw 6E price must be an integer")
    if raw_price == UNDEFINED_PRICE:
        raise EntryReplayError("undefined raw price has no 6E tick value")
    ticks, remainder = divmod(raw_price, SIX_E_TICK_RAW)
    if remainder:
        raise EntryReplayError(
            f"raw price {raw_price} is off the exact 6E tick grid ({SIX_E_TICK_RAW})"
        )
    return ticks


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _require_int(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise EntryReplayError(f"{label} must be an integer")
    return value


def _direction(value: Direction | str) -> Direction:
    try:
        return Direction(value)
    except (TypeError, ValueError) as error:
        raise EntryReplayError("direction must be LONG or SHORT") from error


def _validate_policy(policy: ConservativeScreeningBundle) -> None:
    if not isinstance(policy, ConservativeScreeningBundle):
        raise EntryReplayError("policy must be a validated ConservativeScreeningBundle")
    if policy.execution.config_id != FROZEN_EXECUTION_POLICY_ID:
        raise EntryReplayError(f"execution policy must be {FROZEN_EXECUTION_POLICY_ID!r}")
    if policy.execution_version != FROZEN_EXECUTION_POLICY_ID:
        raise EntryReplayError("campaign and entry execution policy IDs differ")
    if policy.routing_delay_ms != 1000:
        raise EntryReplayError("frozen baseline routing delay must be 1000 ms")
    if policy.routing_delay_ms * 1_000_000 != ROUTING_DELAY_NS:
        raise EntryReplayError("entry routing delay and frozen policy differ")
    if STOP_LATENCY_NS != ROUTING_DELAY_NS:
        raise EntryReplayError("entry and barrier baseline latency constants differ")
    if policy.stop_adverse_ticks != STOP_MINIMUM_ADVERSE_TICKS:
        raise EntryReplayError("policy and barrier stop-adversity constants differ")
    if policy.take_profit_trade_through_ticks != TAKE_PROFIT_TRADE_THROUGH_TICKS:
        raise EntryReplayError("policy and barrier trade-through constants differ")
    if policy.barrier_ticks != BARRIER_TICKS:
        raise EntryReplayError("policy and barrier tick grids differ")
    for label, value in (
        ("execution policy SHA-256", policy.execution.sha256),
        ("screening bundle SHA-256", policy.bundle_sha256),
    ):
        if len(value) != _SHA256_LENGTH or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise EntryReplayError(f"{label} is invalid")


def _validate_selection(selection: ContractSelectionResult) -> None:
    if not isinstance(selection, ContractSelectionResult):
        raise EntryReplayError("selection must be a ContractSelectionResult")
    if selection.previous_source_date >= selection.eligible_source_date:
        raise EntryReplayError("selection must use a strictly earlier volume source date")
    if selection.previous_volume.source_date != selection.previous_source_date:
        raise EntryReplayError("selection previous-volume source date is inconsistent")
    if selection.selected not in selection.candidates:
        raise EntryReplayError("selected contract is absent from selection candidates")
    if hashlib.sha256(selection.previous_volume.canonical_bytes).hexdigest() != (
        selection.previous_volume.sha256
    ):
        raise EntryReplayError("previous-volume canonical SHA-256 mismatch")
    if hashlib.sha256(selection.canonical_bytes).hexdigest() != selection.sha256:
        raise EntryReplayError("contract-selection canonical SHA-256 mismatch")

    document = selection.as_dict()
    boundary = document.get("information_boundary")
    if not isinstance(boundary, dict) or boundary.get("eligible_source_rows_read") is not False:
        raise EntryReplayError("selection does not prove the eligible-row information boundary")
    if boundary.get("volume_source") != "PREVIOUS_SOURCE_DATE_ONLY":
        raise EntryReplayError("selection volume source is not previous-date-only")
    if document.get("policy_version") != CONTRACT_SELECTION_POLICY_VERSION:
        raise EntryReplayError("contract-selection policy version is not frozen")
    if document.get("previous_volume_sha256") != selection.previous_volume.sha256:
        raise EntryReplayError("selection previous-volume audit reference is inconsistent")
    if document.get("selected") != selection.selected.as_dict():
        raise EntryReplayError("selected contract and canonical selection document differ")


def _metadata_ns(metadata: Mapping[str, Any], key: str) -> int:
    value = metadata.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise EntryReplayError(f"dbn.metadata {key!r} must be non-negative integer ns")
    return value


def _open_source(
    source_parquet_path: Path | str,
    *,
    selection: ContractSelectionResult,
    decision_ts_recv_ns: int,
    eligibility_ts_recv_ns: int,
) -> _SourceContext:
    path = Path(source_parquet_path).expanduser().resolve()
    if not path.is_file() or path.suffix.lower() != ".parquet":
        raise EntryReplayError(f"execution source must be a regular Parquet file: {path}")
    try:
        parquet = pq.ParquetFile(path)
        contract = validate_mbp10_contract(parquet.schema_arrow)
    except (OSError, pa.ArrowException, Mbp10ContractError) as error:
        raise EntryReplayError(f"invalid MBP-10 execution source: {path}") from error

    raw_metadata = (parquet.schema_arrow.metadata or {}).get(b"dbn.metadata")
    if raw_metadata is None:
        raise EntryReplayError("execution source is missing dbn.metadata")
    try:
        metadata = decode_dbn_metadata(raw_metadata)
        mappings = parse_instrument_mappings(raw_metadata)
    except Mbp10ContractError as error:
        raise EntryReplayError(f"invalid execution-source mappings: {path}") from error

    request_start_ns = _metadata_ns(metadata, "start")
    request_end_ns = _metadata_ns(metadata, "end")
    if request_start_ns >= request_end_ns:
        raise EntryReplayError("execution-source request range is empty")
    try:
        source_date = datetime.fromtimestamp(request_start_ns // 1_000_000_000, tz=UTC).date()
    except (OSError, OverflowError, ValueError) as error:
        raise EntryReplayError("execution-source start is outside the UTC range") from error
    if source_date != selection.eligible_source_date:
        raise EntryReplayError("execution source date differs from selection eligible date")
    if not request_start_ns <= decision_ts_recv_ns < request_end_ns:
        raise EntryReplayError("decision timestamp is outside the execution-source request range")
    if not request_start_ns <= eligibility_ts_recv_ns < request_end_ns:
        raise EntryReplayError("entry eligibility crosses the execution-source request range")

    selected = selection.selected
    exact = [
        mapping
        for mapping in mappings
        if mapping.interval_start <= selection.eligible_source_date < mapping.interval_end
        and mapping.instrument_id == selected.instrument_id
        and mapping.raw_symbol == selected.raw_symbol
        and mapping.kind is InstrumentKind.OUTRIGHT
    ]
    active_by_id = [
        mapping
        for mapping in mappings
        if mapping.interval_start <= selection.eligible_source_date < mapping.interval_end
        and mapping.instrument_id == selected.instrument_id
    ]
    if len(exact) != 1 or len(active_by_id) != 1:
        raise EntryReplayError(
            "selected instrument_id/raw_symbol is not one exact source-date-active outright"
        )
    if (
        resolve_6e_contract_month(
            selected.raw_symbol,
            source_date=selection.eligible_source_date,
        )
        != selected.contract_month
    ):
        raise EntryReplayError("selected raw symbol and contract month differ")

    return _SourceContext(
        path=path,
        parquet=parquet,
        request_start_ns=request_start_ns,
        request_end_ns=request_end_ns,
        schema_sha256=compute_schema_fingerprint(parquet.schema_arrow, contract),
        metadata_sha256=hashlib.sha256(raw_metadata).hexdigest(),
    )


def _price_ticks(raw_price: int) -> int | None:
    if raw_price == UNDEFINED_PRICE:
        return None
    return raw_6e_price_to_ticks(raw_price)


def _row_book_structure(
    values: Mapping[str, list[object]],
    row_index: int,
) -> tuple[bool, bool]:
    structurally_valid = True
    empty = True
    prior_price: dict[str, int | None] = {"bid": None, "ask": None}
    undefined_seen = {"bid": False, "ask": False}
    try:
        for level in range(10):
            suffix = f"{level:02d}"
            for side in ("bid", "ask"):
                price = int(values[f"{side}_px_{suffix}"][row_index])
                size = int(values[f"{side}_sz_{suffix}"][row_index])
                count = int(values[f"{side}_ct_{suffix}"][row_index])
                defined = price != UNDEFINED_PRICE
                if defined:
                    empty = False
                    if undefined_seen[side] or size <= 0 or count <= 0 or count > size:
                        structurally_valid = False
                    previous = prior_price[side]
                    if previous is not None and (
                        (side == "bid" and price >= previous)
                        or (side == "ask" and price <= previous)
                    ):
                        structurally_valid = False
                    prior_price[side] = price
                else:
                    undefined_seen[side] = True
                    if size != 0 or count != 0:
                        structurally_valid = False
    except (TypeError, ValueError, OverflowError) as error:
        raise EntryReplayError("entry recovery audit contains invalid book values") from error
    return structurally_valid, empty


def _decode_reference(
    *,
    event_index: int,
    row_group_index: int,
    row_index: int,
    values: Mapping[str, list[object]],
) -> EntryEventReference:
    ts_recv_ns = int(values["ts_recv"][row_index])
    action = str(values["action"][row_index])
    side = str(values["side"][row_index])
    flags = int(values["flags"][row_index])
    sequence = int(values["sequence"][row_index])
    bid_raw = int(values["bid_px_00"][row_index])
    ask_raw = int(values["ask_px_00"][row_index])
    bid_size = int(values["bid_sz_00"][row_index])
    ask_size = int(values["ask_sz_00"][row_index])
    if action not in _KNOWN_ACTIONS:
        raise EntryReplayError(f"unknown MBP-10 action {action!r} at source row {event_index}")
    if not 0 <= sequence <= _UINT32_MAX:
        raise EntryReplayError(f"invalid sequence at source row {event_index}")

    structurally_valid, book_empty = _row_book_structure(values, row_index)
    bid_ticks = _price_ticks(bid_raw)
    ask_ticks = _price_ticks(ask_raw)
    snapshot = bool(flags & F_SNAPSHOT)
    maybe_bad = bool(flags & F_MAYBE_BAD_BOOK)
    valid_reset = (
        action == "R" and side == "N" and structurally_valid and book_empty and not maybe_bad
    )
    valid_snapshot = snapshot and action != "R" and structurally_valid and not maybe_bad
    valid_recovery_marker = valid_reset or valid_snapshot
    reasons: list[BboInvalidReason] = []
    if action == "R":
        reasons.append(BboInvalidReason.RESET)
    if maybe_bad:
        reasons.append(BboInvalidReason.MAYBE_BAD_BOOK)
    if flags & F_BAD_TS_RECV:
        reasons.append(BboInvalidReason.BAD_TS_RECV)
    if bid_ticks is None or ask_ticks is None:
        reasons.append(BboInvalidReason.UNDEFINED_BBO)
    elif bid_ticks == ask_ticks:
        reasons.append(BboInvalidReason.LOCKED_BOOK)
    elif bid_ticks > ask_ticks:
        reasons.append(BboInvalidReason.CROSSED_BOOK)
    if (bid_ticks is not None and bid_size < 1) or (ask_ticks is not None and ask_size < 1):
        reasons.append(BboInvalidReason.MISSING_DEPTH)
    if not structurally_valid:
        reasons.append(BboInvalidReason.INVALID_BOOK_STRUCTURE)
    if (snapshot or action == "R") and not valid_recovery_marker:
        reasons.append(BboInvalidReason.INVALID_RECOVERY_MARKER)
    row_reasons = tuple(dict.fromkeys(reasons))
    reason = row_reasons[0] if row_reasons else None

    return EntryEventReference(
        event_index=event_index,
        row_group_index=row_group_index,
        row_index=row_index,
        ts_recv_ns=ts_recv_ns,
        sequence=sequence,
        action=action,
        side=side,
        flags=flags,
        snapshot=snapshot,
        book_structurally_valid=structurally_valid,
        book_empty=book_empty,
        valid_recovery_marker=valid_recovery_marker,
        bid_price_raw=bid_raw,
        ask_price_raw=ask_raw,
        bid_price_ticks=bid_ticks,
        ask_price_ticks=ask_ticks,
        bid_size=bid_size,
        ask_size=ask_size,
        row_invalid_reasons=row_reasons,
        row_invalid_reason=reason,
        invalid_reasons=row_reasons,
        invalid_reason=reason,
    )


def _quote(reference: EntryEventReference) -> ExecutableQuote:
    return ExecutableQuote(
        event_index=reference.event_index,
        ts_recv_ns=reference.ts_recv_ns,
        best_bid_ticks=reference.bid_price_ticks,
        best_ask_ticks=reference.ask_price_ticks,
        valid=reference.valid,
    )


def _opposite_bbo(
    reference: EntryEventReference,
    direction: Direction,
) -> tuple[int, int | None, int, str]:
    if direction is Direction.LONG:
        return (
            reference.ask_price_raw,
            reference.ask_price_ticks,
            reference.ask_size,
            "BEST_ASK",
        )
    return (
        reference.bid_price_raw,
        reference.bid_price_ticks,
        reference.bid_size,
        "BEST_BID",
    )


def _freeze_entry_limit(
    scan: _ScanState,
    *,
    snapshot: EntryEventReference,
    direction: Direction,
) -> None:
    raw_price, tick_price, _, side = _opposite_bbo(snapshot, direction)
    if tick_price is None:
        raise EntryReplayError("valid eligibility snapshot is missing its opposite BBO")
    scan.entry_limit_side = side
    scan.entry_limit_price_raw = raw_price
    scan.entry_limit_price_ticks = tick_price


def _record_reference(
    scan: _ScanState,
    reference: EntryEventReference,
) -> EntryEventReference:
    scan.observed_invalid_reasons.update(reference.invalid_reasons)
    return reference


def _make_audit(
    *,
    source: _SourceContext,
    selection: ContractSelectionResult,
    policy: ConservativeScreeningBundle,
    decision_ts_recv_ns: int,
    eligibility_ts_recv_ns: int,
    scan: _ScanState,
) -> EntryAudit:
    selected = selection.selected
    return EntryAudit(
        source_path=str(source.path),
        source_schema_sha256=source.schema_sha256,
        source_dbn_metadata_sha256=source.metadata_sha256,
        source_footer_rows=source.parquet.metadata.num_rows,
        source_footer_row_groups=source.parquet.metadata.num_row_groups,
        source_rows_examined=scan.rows_examined,
        source_row_groups_read=scan.row_groups_read,
        selected_instrument_id=selected.instrument_id,
        selected_raw_symbol=selected.raw_symbol,
        selected_contract_month=selected.contract_month,
        previous_source_date=selection.previous_source_date,
        eligible_source_date=selection.eligible_source_date,
        selection_sha256=selection.sha256,
        previous_volume_sha256=selection.previous_volume.sha256,
        contract_selection_policy_version=CONTRACT_SELECTION_POLICY_VERSION,
        execution_policy_id=policy.execution.config_id,
        execution_policy_sha256=policy.execution.sha256,
        screening_bundle_sha256=policy.bundle_sha256,
        decision_ts_recv_ns=decision_ts_recv_ns,
        entry_eligibility_ts_recv_ns=eligibility_ts_recv_ns,
        decision_event=scan.decision_event,
        eligibility_snapshot=scan.eligibility_snapshot,
        attempt_event=scan.attempt_event,
        entry_limit_side=scan.entry_limit_side,
        entry_limit_price_raw=scan.entry_limit_price_raw,
        entry_limit_price_ticks=scan.entry_limit_price_ticks,
        failure_event=scan.failure_event,
        route_event_count=scan.route_event_count,
        maximum_route_quote_gap_ns=scan.maximum_route_quote_gap_ns,
        observed_invalid_reasons=tuple(
            sorted(scan.observed_invalid_reasons, key=lambda reason: reason.value)
        ),
    )


def _result(
    *,
    status: EntryStatus,
    reason: EntryReason,
    direction: Direction,
    fill_price_ticks: int | None,
    fill_price_raw: int | None,
    path: ExecutableQuotePath | None,
    audit: EntryAudit,
) -> EntryReplayResult:
    fill_quantity = 1 if status is EntryStatus.ENTRY_FILLED else 0
    document: dict[str, object] = {
        "artifact_schema": ENTRY_ARTIFACT_SCHEMA,
        "audit": audit.as_dict(),
        "direction": direction.value,
        "entry_order": {
            "entry_eligibility_delay_ns": ROUTING_DELAY_NS,
            "fill_reference": "OPPOSITE_BBO_AT_FIRST_EVENT_AT_OR_AFTER_ELIGIBILITY",
            "limit_offset_ticks": 0,
            "limit_reference": "OPPOSITE_BBO_AT_ENTRY_ELIGIBILITY",
            "maximum_quote_age_ns": MAX_QUOTE_AGE_NS,
            "partial_fill_allowed": False,
            "quantity_contracts": 1,
            "retry_allowed": False,
            "time_in_force": "IOC",
            "walk_beyond_limit_allowed": False,
        },
        "fill": (
            {
                "actual_fill_price_raw": fill_price_raw,
                "actual_fill_price_ticks": fill_price_ticks,
                "event": (
                    audit.attempt_event.as_dict() if audit.attempt_event is not None else None
                ),
                "quantity_contracts": fill_quantity,
            }
            if status is EntryStatus.ENTRY_FILLED
            else None
        ),
        "path": (
            {
                "event_count": path.event_count,
                "source_path_passes": path.source_path_passes,
                "terminal_event": (
                    path.terminal_reference.as_dict()
                    if path.terminal_reference is not None
                    else None
                ),
                "valid_event_count": path.valid_event_count,
            }
            if path is not None
            else None
        ),
        "reason": reason.value,
        "status": status.value,
    }
    canonical_bytes = _canonical_bytes(document)
    return EntryReplayResult(
        status=status,
        reason=reason,
        direction=direction,
        fill_price_ticks=fill_price_ticks,
        fill_price_raw=fill_price_raw,
        fill_quantity_contracts=fill_quantity,
        executable_path=path,
        audit=audit,
        canonical_bytes=canonical_bytes,
        sha256=hashlib.sha256(canonical_bytes).hexdigest(),
    )


def _not_filled(
    *,
    reason: EntryReason,
    direction: Direction,
    source: _SourceContext,
    selection: ContractSelectionResult,
    policy: ConservativeScreeningBundle,
    decision_ts_recv_ns: int,
    eligibility_ts_recv_ns: int,
    scan: _ScanState,
) -> EntryReplayResult:
    return _result(
        status=EntryStatus.ENTRY_NOT_FILLED,
        reason=reason,
        direction=direction,
        fill_price_ticks=None,
        fill_price_raw=None,
        path=None,
        audit=_make_audit(
            source=source,
            selection=selection,
            policy=policy,
            decision_ts_recv_ns=decision_ts_recv_ns,
            eligibility_ts_recv_ns=eligibility_ts_recv_ns,
            scan=scan,
        ),
    )


def _terminal_path(
    items: list[_PathItem],
    *,
    fill_item: _PathItem,
    terminal_cutoff_ts_recv_ns: int | None,
) -> ExecutableQuotePath:
    if terminal_cutoff_ts_recv_ns is None:
        return ExecutableQuotePath(
            tuple(item.quote for item in items),
            terminal_quote=None,
            terminal_reference=None,
        )

    candidates = [fill_item, *items]
    terminal_index = max(index for index, item in enumerate(candidates) if item.quote.valid)
    terminal = candidates[terminal_index]
    ordinary = tuple(item.quote for item in candidates[1:terminal_index])
    return ExecutableQuotePath(
        ordinary,
        terminal_quote=terminal.quote,
        terminal_reference=terminal.reference,
    )


def read_entry_path(
    *,
    selection: ContractSelectionResult,
    source_parquet_path: Path | str,
    decision_ts_recv_ns: int,
    direction: Direction | str,
    policy: ConservativeScreeningBundle,
    terminal_cutoff_ts_recv_ns: int | None = None,
) -> EntryReplayResult:
    """Replay the frozen delayed IOC gate and build one post-entry quote path.

    The decision timestamp must be a UTC-nanosecond boundary divisible by five
    minutes.  Only the contract already selected from previous-date volume is
    read.  The opposite BBO observable exactly at eligibility freezes the zero-
    expansion limit.  The first physical selected-instrument event at or after
    decision + one second is the sole IOC attempt and cannot chase a worse BBO.
    """

    _validate_selection(selection)
    _validate_policy(policy)
    decision_ts_recv_ns = _require_int(decision_ts_recv_ns, label="decision_ts_recv_ns")
    if decision_ts_recv_ns < 0 or decision_ts_recv_ns % FIVE_MINUTE_NS:
        raise EntryReplayError(
            "decision_ts_recv_ns must be a non-negative right-closed five-minute boundary"
        )
    resolved_direction = _direction(direction)
    eligibility_ts_recv_ns = decision_ts_recv_ns + ROUTING_DELAY_NS
    source = _open_source(
        source_parquet_path,
        selection=selection,
        decision_ts_recv_ns=decision_ts_recv_ns,
        eligibility_ts_recv_ns=eligibility_ts_recv_ns,
    )
    if terminal_cutoff_ts_recv_ns is not None:
        terminal_cutoff_ts_recv_ns = _require_int(
            terminal_cutoff_ts_recv_ns,
            label="terminal_cutoff_ts_recv_ns",
        )
        if not eligibility_ts_recv_ns < terminal_cutoff_ts_recv_ns <= source.request_end_ns:
            raise EntryReplayError(
                "terminal cutoff must follow eligibility and remain within the source range"
            )

    selected_id = selection.selected.instrument_id
    book = _ResetAwareBook()
    scan = _ScanState()
    decision_checked = False
    gate_last_quote_ts: int | None = None
    previous_selected_ts: int | None = None
    fill_item: _PathItem | None = None
    path_items: list[_PathItem] = []
    source_row_offset = 0
    stop_scan = False

    try:
        for row_group_index in range(source.parquet.metadata.num_row_groups):
            table = source.parquet.read_row_group(
                row_group_index,
                columns=list(_ENTRY_COLUMNS),
                use_threads=False,
            )
            scan.row_groups_read += 1
            timestamps = pc.cast(table["ts_recv"].combine_chunks(), pa.int64()).to_pylist()
            values = {
                name: (
                    timestamps if name == "ts_recv" else table[name].combine_chunks().to_pylist()
                )
                for name in _ENTRY_COLUMNS
            }

            for row_index in range(table.num_rows):
                scan.rows_examined += 1
                event_index = source_row_offset + row_index
                if int(values["instrument_id"][row_index]) != selected_id:
                    continue
                reference = _decode_reference(
                    event_index=event_index,
                    row_group_index=row_group_index,
                    row_index=row_index,
                    values=values,
                )
                if previous_selected_ts is not None and reference.ts_recv_ns < previous_selected_ts:
                    raise EntryReplayError(
                        "selected-instrument ts_recv regressed in physical source order"
                    )
                previous_selected_ts = reference.ts_recv_ns

                if not decision_checked and reference.ts_recv_ns <= decision_ts_recv_ns:
                    _record_reference(scan, book.observe(reference))
                    continue

                if not decision_checked:
                    decision_reference = book.reference_at(
                        decision_ts_recv_ns,
                        boundary_proven=True,
                    )
                    scan.decision_event = (
                        _record_reference(scan, decision_reference)
                        if decision_reference is not None
                        else None
                    )
                    decision_checked = True
                    if scan.decision_event is None:
                        return _not_filled(
                            reason=EntryReason.NO_DECISION_QUOTE,
                            direction=resolved_direction,
                            source=source,
                            selection=selection,
                            policy=policy,
                            decision_ts_recv_ns=decision_ts_recv_ns,
                            eligibility_ts_recv_ns=eligibility_ts_recv_ns,
                            scan=scan,
                        )
                    if decision_ts_recv_ns - scan.decision_event.ts_recv_ns > MAX_QUOTE_AGE_NS:
                        scan.failure_event = scan.decision_event
                        return _not_filled(
                            reason=EntryReason.STALE_DECISION_QUOTE,
                            direction=resolved_direction,
                            source=source,
                            selection=selection,
                            policy=policy,
                            decision_ts_recv_ns=decision_ts_recv_ns,
                            eligibility_ts_recv_ns=eligibility_ts_recv_ns,
                            scan=scan,
                        )
                    if not scan.decision_event.valid:
                        scan.failure_event = scan.decision_event
                        return _not_filled(
                            reason=EntryReason.INVALID_DECISION_BBO,
                            direction=resolved_direction,
                            source=source,
                            selection=selection,
                            policy=policy,
                            decision_ts_recv_ns=decision_ts_recv_ns,
                            eligibility_ts_recv_ns=eligibility_ts_recv_ns,
                            scan=scan,
                        )
                    gate_last_quote_ts = scan.decision_event.ts_recv_ns

                if fill_item is None and reference.ts_recv_ns < eligibility_ts_recv_ns:
                    if gate_last_quote_ts is None:
                        raise EntryReplayError("entry gate lost its decision quote state")
                    gap = reference.ts_recv_ns - gate_last_quote_ts
                    scan.maximum_route_quote_gap_ns = max(
                        scan.maximum_route_quote_gap_ns,
                        gap,
                    )
                    if gap > MAX_QUOTE_AGE_NS:
                        scan.failure_event = reference
                        return _not_filled(
                            reason=EntryReason.STALE_BBO_DURING_ROUTE,
                            direction=resolved_direction,
                            source=source,
                            selection=selection,
                            policy=policy,
                            decision_ts_recv_ns=decision_ts_recv_ns,
                            eligibility_ts_recv_ns=eligibility_ts_recv_ns,
                            scan=scan,
                        )
                    reset_before = book.reset_pending
                    routed = _record_reference(scan, book.observe(reference))
                    scan.route_event_count += 1
                    gate_last_quote_ts = reference.ts_recv_ns
                    if not routed.valid:
                        scan.failure_event = routed
                        route_reason = (
                            EntryReason.RESET_DURING_ROUTE
                            if routed.invalid_reason
                            in {BboInvalidReason.RESET, BboInvalidReason.RESET_NOT_REARMED}
                            or reset_before
                            or book.reset_pending
                            else EntryReason.INVALID_BBO_DURING_ROUTE
                        )
                        return _not_filled(
                            reason=route_reason,
                            direction=resolved_direction,
                            source=source,
                            selection=selection,
                            policy=policy,
                            decision_ts_recv_ns=decision_ts_recv_ns,
                            eligibility_ts_recv_ns=eligibility_ts_recv_ns,
                            scan=scan,
                        )
                    continue

                if fill_item is None:
                    if terminal_cutoff_ts_recv_ns is not None and (
                        reference.ts_recv_ns >= terminal_cutoff_ts_recv_ns
                    ):
                        stop_scan = True
                        break
                    if gate_last_quote_ts is None:
                        raise EntryReplayError("entry gate lost its routing quote state")
                    final_gap = eligibility_ts_recv_ns - gate_last_quote_ts
                    scan.maximum_route_quote_gap_ns = max(
                        scan.maximum_route_quote_gap_ns,
                        final_gap,
                    )
                    if final_gap > MAX_QUOTE_AGE_NS:
                        scan.failure_event = reference
                        return _not_filled(
                            reason=EntryReason.STALE_BBO_DURING_ROUTE,
                            direction=resolved_direction,
                            source=source,
                            selection=selection,
                            policy=policy,
                            decision_ts_recv_ns=decision_ts_recv_ns,
                            eligibility_ts_recv_ns=eligibility_ts_recv_ns,
                            scan=scan,
                        )
                    exact_eligibility_event = reference.ts_recv_ns == eligibility_ts_recv_ns
                    if exact_eligibility_event:
                        snapshot = _record_reference(scan, book.observe(reference))
                        attempt = snapshot
                    else:
                        eligibility_reference = book.reference_at(
                            eligibility_ts_recv_ns,
                            boundary_proven=True,
                        )
                        snapshot = (
                            _record_reference(scan, eligibility_reference)
                            if eligibility_reference is not None
                            else None
                        )
                        attempt = None
                    if snapshot is None:
                        raise EntryReplayError("entry gate lost its eligibility snapshot")
                    scan.eligibility_snapshot = snapshot
                    _, snapshot_ticks, snapshot_size, _ = _opposite_bbo(
                        snapshot,
                        resolved_direction,
                    )
                    if snapshot_ticks is not None:
                        _freeze_entry_limit(
                            scan,
                            snapshot=snapshot,
                            direction=resolved_direction,
                        )
                    if not snapshot.valid:
                        if exact_eligibility_event:
                            scan.attempt_event = snapshot
                        scan.failure_event = snapshot
                        reason = (
                            EntryReason.INSUFFICIENT_EXECUTABLE_SIZE
                            if BboInvalidReason.MISSING_DEPTH in snapshot.invalid_reasons
                            and snapshot_size < 1
                            else EntryReason.INVALID_ENTRY_ELIGIBILITY_BBO
                        )
                        return _not_filled(
                            reason=reason,
                            direction=resolved_direction,
                            source=source,
                            selection=selection,
                            policy=policy,
                            decision_ts_recv_ns=decision_ts_recv_ns,
                            eligibility_ts_recv_ns=eligibility_ts_recv_ns,
                            scan=scan,
                        )
                    if snapshot_ticks is None:
                        raise EntryReplayError(
                            "valid eligibility snapshot is missing its opposite BBO"
                        )

                    if attempt is None:
                        attempt = _record_reference(scan, book.observe(reference))
                    scan.attempt_event = attempt
                    _, attempt_ticks, attempt_size, _ = _opposite_bbo(
                        attempt,
                        resolved_direction,
                    )
                    if not attempt.valid:
                        scan.failure_event = attempt
                        reason = (
                            EntryReason.INSUFFICIENT_EXECUTABLE_SIZE
                            if BboInvalidReason.MISSING_DEPTH in attempt.invalid_reasons
                            and attempt_size < 1
                            else EntryReason.INVALID_ENTRY_ATTEMPT_BBO
                        )
                        return _not_filled(
                            reason=reason,
                            direction=resolved_direction,
                            source=source,
                            selection=selection,
                            policy=policy,
                            decision_ts_recv_ns=decision_ts_recv_ns,
                            eligibility_ts_recv_ns=eligibility_ts_recv_ns,
                            scan=scan,
                        )
                    if attempt_size < 1:
                        scan.failure_event = attempt
                        return _not_filled(
                            reason=EntryReason.INSUFFICIENT_EXECUTABLE_SIZE,
                            direction=resolved_direction,
                            source=source,
                            selection=selection,
                            policy=policy,
                            decision_ts_recv_ns=decision_ts_recv_ns,
                            eligibility_ts_recv_ns=eligibility_ts_recv_ns,
                            scan=scan,
                        )
                    if attempt_ticks is None:
                        raise EntryReplayError("valid IOC attempt is missing its opposite BBO")
                    outside_limit = (
                        attempt_ticks > snapshot_ticks
                        if resolved_direction is Direction.LONG
                        else attempt_ticks < snapshot_ticks
                    )
                    if outside_limit:
                        scan.failure_event = attempt
                        return _not_filled(
                            reason=EntryReason.PRICE_OUTSIDE_LIMIT,
                            direction=resolved_direction,
                            source=source,
                            selection=selection,
                            policy=policy,
                            decision_ts_recv_ns=decision_ts_recv_ns,
                            eligibility_ts_recv_ns=eligibility_ts_recv_ns,
                            scan=scan,
                        )
                    fill_item = _PathItem(quote=_quote(attempt), reference=attempt)
                    continue

                if terminal_cutoff_ts_recv_ns is not None and (
                    reference.ts_recv_ns >= terminal_cutoff_ts_recv_ns
                ):
                    stop_scan = True
                    break
                executable = _record_reference(scan, book.observe(reference))
                path_items.append(_PathItem(quote=_quote(executable), reference=executable))

            source_row_offset += table.num_rows
            if stop_scan:
                break
    except (OSError, pa.ArrowException) as error:
        raise EntryReplayError(f"cannot stream entry columns from {source.path}") from error

    if not decision_checked:
        decision_reference = book.reference_at(
            decision_ts_recv_ns,
            boundary_proven=True,
        )
        scan.decision_event = (
            _record_reference(scan, decision_reference) if decision_reference is not None else None
        )
        if scan.decision_event is None:
            return _not_filled(
                reason=EntryReason.NO_DECISION_QUOTE,
                direction=resolved_direction,
                source=source,
                selection=selection,
                policy=policy,
                decision_ts_recv_ns=decision_ts_recv_ns,
                eligibility_ts_recv_ns=eligibility_ts_recv_ns,
                scan=scan,
            )
        if decision_ts_recv_ns - scan.decision_event.ts_recv_ns > MAX_QUOTE_AGE_NS:
            scan.failure_event = scan.decision_event
            return _not_filled(
                reason=EntryReason.STALE_DECISION_QUOTE,
                direction=resolved_direction,
                source=source,
                selection=selection,
                policy=policy,
                decision_ts_recv_ns=decision_ts_recv_ns,
                eligibility_ts_recv_ns=eligibility_ts_recv_ns,
                scan=scan,
            )
        if not scan.decision_event.valid:
            scan.failure_event = scan.decision_event
            return _not_filled(
                reason=EntryReason.INVALID_DECISION_BBO,
                direction=resolved_direction,
                source=source,
                selection=selection,
                policy=policy,
                decision_ts_recv_ns=decision_ts_recv_ns,
                eligibility_ts_recv_ns=eligibility_ts_recv_ns,
                scan=scan,
            )
        gate_last_quote_ts = scan.decision_event.ts_recv_ns

    if fill_item is None:
        if gate_last_quote_ts is None:
            raise EntryReplayError("entry gate lost its decision quote state")
        final_gap = eligibility_ts_recv_ns - gate_last_quote_ts
        scan.maximum_route_quote_gap_ns = max(
            scan.maximum_route_quote_gap_ns,
            final_gap,
        )
        if final_gap > MAX_QUOTE_AGE_NS:
            scan.failure_event = scan.decision_event
            return _not_filled(
                reason=EntryReason.STALE_BBO_DURING_ROUTE,
                direction=resolved_direction,
                source=source,
                selection=selection,
                policy=policy,
                decision_ts_recv_ns=decision_ts_recv_ns,
                eligibility_ts_recv_ns=eligibility_ts_recv_ns,
                scan=scan,
            )
        eligibility_reference = book.reference_at(
            eligibility_ts_recv_ns,
            boundary_proven=True,
        )
        snapshot = (
            _record_reference(scan, eligibility_reference)
            if eligibility_reference is not None
            else None
        )
        if snapshot is None:
            raise EntryReplayError("entry gate lost its eligibility snapshot")
        scan.eligibility_snapshot = snapshot
        if not snapshot.valid:
            scan.failure_event = snapshot
            return _not_filled(
                reason=EntryReason.INVALID_ENTRY_ELIGIBILITY_BBO,
                direction=resolved_direction,
                source=source,
                selection=selection,
                policy=policy,
                decision_ts_recv_ns=decision_ts_recv_ns,
                eligibility_ts_recv_ns=eligibility_ts_recv_ns,
                scan=scan,
            )
        _freeze_entry_limit(
            scan,
            snapshot=snapshot,
            direction=resolved_direction,
        )
        return _not_filled(
            reason=EntryReason.NO_ENTRY_ELIGIBILITY_EVENT,
            direction=resolved_direction,
            source=source,
            selection=selection,
            policy=policy,
            decision_ts_recv_ns=decision_ts_recv_ns,
            eligibility_ts_recv_ns=eligibility_ts_recv_ns,
            scan=scan,
        )

    path = _terminal_path(
        path_items,
        fill_item=fill_item,
        terminal_cutoff_ts_recv_ns=terminal_cutoff_ts_recv_ns,
    )
    fill_reference = fill_item.reference
    fill_price_ticks = (
        fill_reference.ask_price_ticks
        if resolved_direction is Direction.LONG
        else fill_reference.bid_price_ticks
    )
    fill_price_raw = (
        fill_reference.ask_price_raw
        if resolved_direction is Direction.LONG
        else fill_reference.bid_price_raw
    )
    if fill_price_ticks is None:
        raise EntryReplayError("valid entry event is missing the opposite BBO tick price")
    return _result(
        status=EntryStatus.ENTRY_FILLED,
        reason=EntryReason.FILLED_AT_DELAYED_OPPOSITE_BBO,
        direction=resolved_direction,
        fill_price_ticks=fill_price_ticks,
        fill_price_raw=fill_price_raw,
        path=path,
        audit=_make_audit(
            source=source,
            selection=selection,
            policy=policy,
            decision_ts_recv_ns=decision_ts_recv_ns,
            eligibility_ts_recv_ns=eligibility_ts_recv_ns,
            scan=scan,
        ),
    )
