"""Pure chronological replay shared by every Phase 1A barrier cell.

The module consumes a contract-tagged stream of :class:`ExecutableQuote` values
exactly once.  It owns entry routing, per-cell occupancy, barrier state, the
independent 20-active-session first-touch clock, and mandatory terminal exits.
All state needed to resume at an event boundary is JSON serializable.
"""

from __future__ import annotations

from bisect import bisect_right
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Final

from systematic_fx.backtest.barriers import (
    BARRIER_TICKS,
    BarrierOutcome,
    Direction,
    ExecutableQuote,
)
from systematic_fx.backtest.economics import EntryStatus

FIRST_TOUCH_ACTIVE_SESSIONS: Final = 20
CHECKPOINT_SCHEMA_VERSION: Final = 4
_NS_PER_MS: Final = 1_000_000
MAX_EXECUTABLE_QUOTE_AGE_NS: Final = 1_000_000_000


class SharedReplayError(ValueError):
    """The shared stream or replay state violates a deterministic invariant."""


def _require_int(value: object, *, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SharedReplayError(f"{label} must be an integer")
    if value < minimum:
        raise SharedReplayError(f"{label} must be at least {minimum}")
    return value


def _require_text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise SharedReplayError(f"{label} must be a non-empty string")
    return value


def _validate_utc_month(value: object) -> str:
    month = _require_text(value, label="utc_month")
    if (
        len(month) != 7
        or month[4] != "-"
        or not month[:4].isdigit()
        or not month[5:].isdigit()
        or not 1 <= int(month[5:]) <= 12
    ):
        raise SharedReplayError("utc_month must be canonical YYYY-MM")
    return month


@dataclass(frozen=True, slots=True)
class ExecutionScenario:
    """One frozen execution path; prices are integer minimum-tick units."""

    scenario_id: str
    routing_delay_ns: int
    entry_adverse_ticks: int
    take_profit_trade_through_ticks: int
    stop_latency_ns: int
    stop_minimum_adverse_ticks: int
    terminal_exit_adverse_ticks: int

    def __post_init__(self) -> None:
        _require_text(self.scenario_id, label="scenario_id")
        _require_int(self.routing_delay_ns, label="routing_delay_ns")
        _require_int(self.entry_adverse_ticks, label="entry_adverse_ticks")
        _require_int(
            self.take_profit_trade_through_ticks,
            label="take_profit_trade_through_ticks",
            minimum=1,
        )
        _require_int(self.stop_latency_ns, label="stop_latency_ns")
        _require_int(
            self.stop_minimum_adverse_ticks,
            label="stop_minimum_adverse_ticks",
            minimum=1,
        )
        _require_int(
            self.terminal_exit_adverse_ticks,
            label="terminal_exit_adverse_ticks",
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "scenario_id": self.scenario_id,
            "routing_delay_ns": self.routing_delay_ns,
            "entry_adverse_ticks": self.entry_adverse_ticks,
            "take_profit_trade_through_ticks": self.take_profit_trade_through_ticks,
            "stop_latency_ns": self.stop_latency_ns,
            "stop_minimum_adverse_ticks": self.stop_minimum_adverse_ticks,
            "terminal_exit_adverse_ticks": self.terminal_exit_adverse_ticks,
        }


EXECUTION_SCENARIOS: Final = (
    ExecutionScenario(
        "BASELINE",
        1_000 * _NS_PER_MS,
        0,
        1,
        1_000 * _NS_PER_MS,
        2,
        0,
    ),
    ExecutionScenario(
        "MODERATE_COMBINED",
        1_500 * _NS_PER_MS,
        1,
        1,
        1_500 * _NS_PER_MS,
        4,
        1,
    ),
    ExecutionScenario(
        "SEVERE_DIAGNOSTIC",
        2_000 * _NS_PER_MS,
        2,
        2,
        2_000 * _NS_PER_MS,
        6,
        2,
    ),
)
EXECUTION_SCENARIO_BY_ID: Final = {
    scenario.scenario_id: scenario for scenario in EXECUTION_SCENARIOS
}
_SCENARIO_RANK: Final = {
    scenario.scenario_id: rank for rank, scenario in enumerate(EXECUTION_SCENARIOS)
}


@dataclass(frozen=True, slots=True)
class SignalSeed:
    """The complete immutable P5 signal identity admitted to replay."""

    signal_id: str
    decision_ts_recv_ns: int
    utc_month: str
    direction: Direction
    contract_key: str

    def __post_init__(self) -> None:
        _require_text(self.signal_id, label="signal_id")
        _require_int(self.decision_ts_recv_ns, label="decision_ts_recv_ns")
        _validate_utc_month(self.utc_month)
        if not isinstance(self.direction, Direction):
            raise SharedReplayError("direction must be a Direction")
        _require_text(self.contract_key, label="contract_key")

    def as_dict(self) -> dict[str, object]:
        return {
            "signal_id": self.signal_id,
            "decision_ts_recv_ns": self.decision_ts_recv_ns,
            "utc_month": self.utc_month,
            "direction": self.direction.value,
            "contract_key": self.contract_key,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> SignalSeed:
        try:
            direction = Direction(value["direction"])
        except (KeyError, TypeError, ValueError) as error:
            raise SharedReplayError("checkpoint signal has an invalid direction") from error
        return cls(
            signal_id=value.get("signal_id"),  # type: ignore[arg-type]
            decision_ts_recv_ns=value.get("decision_ts_recv_ns"),  # type: ignore[arg-type]
            utc_month=value.get("utc_month"),  # type: ignore[arg-type]
            direction=direction,
            contract_key=value.get("contract_key"),  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class SharedExecutableQuote:
    """One cache-reader record in the shared cross-contract event stream.

    ``session_ordinal`` must increase by one for each active session of a
    contract.  The first event whose ordinal is 20 greater than the entry
    session closes the label clock, while leaving the portfolio position open.
    A terminal record is the final valid executable quote for that contract.
    """

    contract_key: str
    quote: ExecutableQuote
    source_date: date
    session_ordinal: int
    sequence: int
    terminal: bool = False

    def __post_init__(self) -> None:
        _require_text(self.contract_key, label="contract_key")
        if not isinstance(self.quote, ExecutableQuote):
            raise SharedReplayError("quote must be an ExecutableQuote")
        if type(self.source_date) is not date:
            raise SharedReplayError("source_date must be a date")
        _require_int(self.session_ordinal, label="session_ordinal")
        _require_int(self.sequence, label="sequence")
        if not isinstance(self.terminal, bool):
            raise SharedReplayError("terminal must be a boolean")
        if self.terminal and not self.quote.valid:
            raise SharedReplayError("a terminal quote must be valid and executable")

    @property
    def ordering_key(self) -> tuple[int, int, int, str]:
        """Canonical key required from the merged cache reader."""

        return (
            self.quote.ts_recv_ns,
            self.sequence,
            self.quote.event_index,
            self.contract_key,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "contract_key": self.contract_key,
            "source_date": self.source_date.isoformat(),
            "session_ordinal": self.session_ordinal,
            "sequence": self.sequence,
            "terminal": self.terminal,
            "quote": {
                "event_index": self.quote.event_index,
                "ts_recv_ns": self.quote.ts_recv_ns,
                "best_bid_ticks": self.quote.best_bid_ticks,
                "best_ask_ticks": self.quote.best_ask_ticks,
                "valid": self.quote.valid,
            },
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> SharedExecutableQuote:
        quote_value = value.get("quote")
        if not isinstance(quote_value, Mapping):
            raise SharedReplayError("checkpoint event quote must be an object")
        try:
            source_date = date.fromisoformat(str(value["source_date"]))
            quote = ExecutableQuote(
                event_index=quote_value.get("event_index"),  # type: ignore[arg-type]
                ts_recv_ns=quote_value.get("ts_recv_ns"),  # type: ignore[arg-type]
                best_bid_ticks=quote_value.get("best_bid_ticks"),  # type: ignore[arg-type]
                best_ask_ticks=quote_value.get("best_ask_ticks"),  # type: ignore[arg-type]
                valid=quote_value.get("valid"),  # type: ignore[arg-type]
            )
        except (KeyError, TypeError, ValueError) as error:
            raise SharedReplayError("checkpoint event is malformed") from error
        return cls(
            contract_key=value.get("contract_key"),  # type: ignore[arg-type]
            quote=quote,
            source_date=source_date,
            session_ordinal=value.get("session_ordinal"),  # type: ignore[arg-type]
            sequence=value.get("sequence"),  # type: ignore[arg-type]
            terminal=value.get("terminal"),  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class ReplayEventReference:
    """Exact cache event supporting an entry, trigger, fill, censor, or exit."""

    contract_key: str
    source_date: date
    session_ordinal: int
    event_index: int
    ts_recv_ns: int
    best_bid_ticks: int | None
    best_ask_ticks: int | None
    valid: bool

    def __post_init__(self) -> None:
        _require_text(self.contract_key, label="contract_key")
        if type(self.source_date) is not date:
            raise SharedReplayError("reference source_date must be a date")
        _require_int(self.session_ordinal, label="session_ordinal")
        _require_int(self.event_index, label="event_index")
        _require_int(self.ts_recv_ns, label="ts_recv_ns")
        for label, price in (
            ("best_bid_ticks", self.best_bid_ticks),
            ("best_ask_ticks", self.best_ask_ticks),
        ):
            if price is not None:
                _require_int(price, label=label, minimum=-(2**63))
        if not isinstance(self.valid, bool):
            raise SharedReplayError("reference valid must be a boolean")
        if self.valid and (
            self.best_bid_ticks is None
            or self.best_ask_ticks is None
            or self.best_bid_ticks >= self.best_ask_ticks
        ):
            raise SharedReplayError("valid reference requires an executable bid/ask")

    def as_dict(self) -> dict[str, object]:
        return {
            "contract_key": self.contract_key,
            "source_date": self.source_date.isoformat(),
            "session_ordinal": self.session_ordinal,
            "event_index": self.event_index,
            "ts_recv_ns": self.ts_recv_ns,
            "best_bid_ticks": self.best_bid_ticks,
            "best_ask_ticks": self.best_ask_ticks,
            "valid": self.valid,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ReplayEventReference:
        try:
            valid = value["valid"]
            if not isinstance(valid, bool):
                raise SharedReplayError("checkpoint event reference valid must be boolean")
            return cls(
                contract_key=_require_text(value["contract_key"], label="contract_key"),
                source_date=date.fromisoformat(str(value["source_date"])),
                session_ordinal=_require_int(value["session_ordinal"], label="session_ordinal"),
                event_index=_require_int(value["event_index"], label="event_index"),
                ts_recv_ns=_require_int(value["ts_recv_ns"], label="ts_recv_ns"),
                best_bid_ticks=(
                    None
                    if value.get("best_bid_ticks") is None
                    else _require_int(
                        value["best_bid_ticks"],
                        label="best_bid_ticks",
                        minimum=-(2**63),
                    )
                ),
                best_ask_ticks=(
                    None
                    if value.get("best_ask_ticks") is None
                    else _require_int(
                        value["best_ask_ticks"],
                        label="best_ask_ticks",
                        minimum=-(2**63),
                    )
                ),
                valid=valid,
            )
        except (KeyError, TypeError, ValueError) as error:
            raise SharedReplayError("checkpoint event reference is malformed") from error


@dataclass(frozen=True, slots=True)
class ReplayResultRecord:
    """Deterministic result for one signal/scenario/direction/contract/cell."""

    signal_id: str
    decision_ts_recv_ns: int
    utc_month: str
    scenario_id: str
    direction: Direction
    contract_key: str
    cell_id: str
    take_profit_ticks: int
    stop_loss_ticks: int
    entry_status: EntryStatus
    entry_eligibility_ts_recv_ns: int
    entry_fill_price_ticks: int | None
    buying_price_ticks: int | None
    selling_price_ticks: int | None
    loss_price_ticks: int | None
    take_profit_target_price_ticks: int | None
    stop_trigger_price_ticks: int | None
    first_touch_outcome: BarrierOutcome | None
    portfolio_outcome: BarrierOutcome | None
    exit_fill_price_ticks: int | None
    decision_ref: ReplayEventReference | None
    eligibility_ref: ReplayEventReference | None
    attempt_ref: ReplayEventReference | None
    entry_ref: ReplayEventReference | None
    trigger_ref: ReplayEventReference | None
    fill_ref: ReplayEventReference | None
    first_touch_censor_ref: ReplayEventReference | None
    terminal_ref: ReplayEventReference | None
    entry_limit_price_ticks: int | None
    route_event_count: int
    maximum_route_quote_gap_ns: int
    failure_ref: ReplayEventReference | None
    occupying_signal_id: str | None
    no_fill_reason: str | None
    completion_ts_recv_ns: int | None

    def as_dict(self) -> dict[str, object]:
        return {
            "signal_id": self.signal_id,
            "decision_ts_recv_ns": self.decision_ts_recv_ns,
            "utc_month": self.utc_month,
            "scenario_id": self.scenario_id,
            "direction": self.direction.value,
            "contract_key": self.contract_key,
            "cell_id": self.cell_id,
            "take_profit_ticks": self.take_profit_ticks,
            "stop_loss_ticks": self.stop_loss_ticks,
            "entry_status": self.entry_status.value,
            "entry_eligibility_ts_recv_ns": self.entry_eligibility_ts_recv_ns,
            "entry_fill_price_ticks": self.entry_fill_price_ticks,
            "buying_price_ticks": self.buying_price_ticks,
            "selling_price_ticks": self.selling_price_ticks,
            "loss_price_ticks": self.loss_price_ticks,
            "take_profit_target_price_ticks": self.take_profit_target_price_ticks,
            "stop_trigger_price_ticks": self.stop_trigger_price_ticks,
            "first_touch_outcome": (
                None if self.first_touch_outcome is None else self.first_touch_outcome.value
            ),
            "portfolio_outcome": (
                None if self.portfolio_outcome is None else self.portfolio_outcome.value
            ),
            "exit_fill_price_ticks": self.exit_fill_price_ticks,
            "decision_ref": (None if self.decision_ref is None else self.decision_ref.as_dict()),
            "eligibility_ref": (
                None if self.eligibility_ref is None else self.eligibility_ref.as_dict()
            ),
            "attempt_ref": None if self.attempt_ref is None else self.attempt_ref.as_dict(),
            "entry_ref": None if self.entry_ref is None else self.entry_ref.as_dict(),
            "trigger_ref": None if self.trigger_ref is None else self.trigger_ref.as_dict(),
            "fill_ref": None if self.fill_ref is None else self.fill_ref.as_dict(),
            "first_touch_censor_ref": (
                None
                if self.first_touch_censor_ref is None
                else self.first_touch_censor_ref.as_dict()
            ),
            "terminal_ref": None if self.terminal_ref is None else self.terminal_ref.as_dict(),
            "entry_limit_price_ticks": self.entry_limit_price_ticks,
            "route_event_count": self.route_event_count,
            "maximum_route_quote_gap_ns": self.maximum_route_quote_gap_ns,
            "failure_ref": None if self.failure_ref is None else self.failure_ref.as_dict(),
            "occupying_signal_id": self.occupying_signal_id,
            "no_fill_reason": self.no_fill_reason,
            "completion_ts_recv_ns": self.completion_ts_recv_ns,
        }


@dataclass(frozen=True, slots=True)
class _CellKey:
    scenario_id: str
    direction: Direction
    contract_key: str
    take_profit_ticks: int
    stop_loss_ticks: int


@dataclass(slots=True)
class _PendingEntry:
    signal_id: str
    scenario_id: str
    cells: list[tuple[int, int]]
    decision_ref: ReplayEventReference
    last_gate_ref: ReplayEventReference
    route_event_count: int = 0
    maximum_route_quote_gap_ns: int = 0


@dataclass(frozen=True, slots=True)
class _OwnerKey:
    signal_id: str
    scenario_id: str


@dataclass(slots=True)
class _PositionGroup:
    signal_id: str
    scenario_id: str
    active_cells: set[tuple[int, int]]
    entry_fill_price_ticks: int
    entry_ref: ReplayEventReference
    entry_session_ordinal: int
    decision_ref: ReplayEventReference
    eligibility_ref: ReplayEventReference
    attempt_ref: ReplayEventReference
    entry_limit_price_ticks: int
    route_event_count: int
    maximum_route_quote_gap_ns: int
    stop_trigger_refs: dict[int, ReplayEventReference]
    censored_cells: set[tuple[int, int]]
    first_touch_censor_ref: ReplayEventReference | None = None


def _event_reference(event: SharedExecutableQuote) -> ReplayEventReference:
    quote = event.quote
    return ReplayEventReference(
        contract_key=event.contract_key,
        source_date=event.source_date,
        session_ordinal=event.session_ordinal,
        event_index=quote.event_index,
        ts_recv_ns=quote.ts_recv_ns,
        best_bid_ticks=quote.best_bid_ticks,
        best_ask_ticks=quote.best_ask_ticks,
        valid=quote.valid,
    )


def _reference_from_optional(value: object) -> ReplayEventReference | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise SharedReplayError("checkpoint reference must be an object or null")
    return ReplayEventReference.from_dict(value)


def _cell_id(take_profit_ticks: int, stop_loss_ticks: int) -> str:
    return f"tp{take_profit_ticks}_sl{stop_loss_ticks}"


class SharedReplay:
    """Single-pass state machine for the full frozen Phase 1A replay space."""

    def __init__(self, signals: Sequence[SignalSeed]) -> None:
        seeds = tuple(signals)
        if any(not isinstance(seed, SignalSeed) for seed in seeds):
            raise SharedReplayError("signals must contain only SignalSeed values")
        identities = [seed.signal_id for seed in seeds]
        if len(set(identities)) != len(identities):
            raise SharedReplayError("signal_id values must be unique")

        self._signals = tuple(
            sorted(
                seeds,
                key=lambda seed: (
                    seed.decision_ts_recv_ns,
                    seed.signal_id,
                    seed.direction.value,
                    seed.contract_key,
                ),
            )
        )
        self._signal_by_id = {seed.signal_id: seed for seed in self._signals}
        self._signal_rank = {seed.signal_id: rank for rank, seed in enumerate(self._signals)}
        self._signal_cursor = 0
        self._pending: list[_PendingEntry] = []
        self._position_groups: dict[_OwnerKey, _PositionGroup] = {}
        self._groups_by_contract: dict[str, set[_OwnerKey]] = {}
        self._occupied: dict[_CellKey, str] = {}
        self._records: dict[tuple[str, str, int, int], ReplayResultRecord] = {}
        self._result_record_count = 0
        self._drained_record_count = 0
        self._buffer: list[SharedExecutableQuote] = []
        self._last_input_event: SharedExecutableQuote | None = None
        self._latest_event_by_contract: dict[str, SharedExecutableQuote] = {}
        self._last_session_by_contract: dict[str, int] = {}
        self._last_source_date_by_contract: dict[str, date] = {}
        self._input_terminal_contracts: set[str] = set()
        self._terminated_contracts: set[str] = set()
        self._completed_source_date: date | None = None
        self._completed_boundary_ts_recv_ns: int | None = None
        self._source_event_count = 0
        self._owner_group_advance_count = 0
        self._threshold_capacity_work_count = 0
        self._finished = False

    @property
    def source_event_count(self) -> int:
        return self._source_event_count

    @property
    def source_stream_passes(self) -> int:
        """The core always consumes its supplied iterable in one forward pass."""

        return 1

    @property
    def active_position_count(self) -> int:
        return sum(len(group.active_cells) for group in self._position_groups.values())

    @property
    def active_owner_group_count(self) -> int:
        return len(self._position_groups)

    @property
    def owner_group_advance_count(self) -> int:
        return self._owner_group_advance_count

    @property
    def threshold_capacity_work_count(self) -> int:
        return self._threshold_capacity_work_count

    @property
    def pending_entry_count(self) -> int:
        return len(self._pending)

    @property
    def result_record_count(self) -> int:
        return self._result_record_count

    @property
    def drained_record_count(self) -> int:
        return self._drained_record_count

    @property
    def completed_source_date(self) -> date | None:
        return self._completed_source_date

    @property
    def finished(self) -> bool:
        return self._finished

    def process(self, events: Iterable[SharedExecutableQuote]) -> None:
        """Consume one forward-only chunk; chunks form one logical event pass."""

        if self._finished:
            raise SharedReplayError("a finished replay cannot consume more events")
        for event in events:
            self._accept_event(event)

    def _accept_event(self, event: object) -> None:
        if not isinstance(event, SharedExecutableQuote):
            raise SharedReplayError("event stream must contain SharedExecutableQuote values")
        previous = self._last_input_event
        if previous is not None and event.ordering_key <= previous.ordering_key:
            raise SharedReplayError(
                "events must be strictly ordered by "
                "(ts_recv_ns, sequence, event_index, contract_key)"
            )
        if (
            self._completed_boundary_ts_recv_ns is not None
            and event.quote.ts_recv_ns <= self._completed_boundary_ts_recv_ns
        ):
            raise SharedReplayError(
                "an event cannot cross a completed source-date timestamp boundary"
            )
        if (
            self._completed_source_date is not None
            and event.source_date <= self._completed_source_date
        ):
            raise SharedReplayError("an event cannot belong to a completed source date")
        if event.contract_key in self._input_terminal_contracts:
            raise SharedReplayError("a contract cannot emit events after its terminal quote")

        prior_session = self._last_session_by_contract.get(event.contract_key)
        if prior_session is not None and event.session_ordinal < prior_session:
            raise SharedReplayError("session_ordinal must be non-decreasing per contract")
        prior_date = self._last_source_date_by_contract.get(event.contract_key)
        if prior_date is not None and event.source_date < prior_date:
            raise SharedReplayError("source_date must be non-decreasing per contract")

        if event.terminal:
            self._input_terminal_contracts.add(event.contract_key)
        self._last_session_by_contract[event.contract_key] = event.session_ordinal
        self._last_source_date_by_contract[event.contract_key] = event.source_date
        self._last_input_event = event
        self._source_event_count += 1

        if self._buffer and event.quote.ts_recv_ns != self._buffer[0].quote.ts_recv_ns:
            self._flush_buffer()
        self._buffer.append(event)

    def _flush_buffer(self) -> None:
        if not self._buffer:
            return
        group = tuple(self._buffer)
        self._buffer.clear()
        group_ts = group[0].quote.ts_recv_ns
        self._activate_signals_before(group_ts)

        by_contract: dict[str, list[SharedExecutableQuote]] = {}
        terminal_by_contract: dict[str, SharedExecutableQuote] = {}
        for event in group:
            by_contract.setdefault(event.contract_key, []).append(event)
            if event.terminal:
                if event.contract_key in terminal_by_contract:
                    raise SharedReplayError("a contract has duplicate terminal events")
                terminal_by_contract[event.contract_key] = event

        self._apply_first_touch_censors(by_contract)
        for contract_key, contract_events in by_contract.items():
            owner_keys = sorted(
                self._groups_by_contract.get(contract_key, ()),
                key=lambda owner: (
                    self._signal_rank[owner.signal_id],
                    _SCENARIO_RANK[owner.scenario_id],
                ),
            )
            for owner_key in owner_keys:
                position_group = self._position_groups.get(owner_key)
                if position_group is not None:
                    self._advance_position_group(
                        owner_key,
                        position_group,
                        contract_events,
                        terminal_by_contract.get(contract_key),
                    )

        self._advance_pending_entries(by_contract)
        for contract_key, contract_events in by_contract.items():
            self._latest_event_by_contract[contract_key] = contract_events[-1]
        self._terminated_contracts.update(terminal_by_contract)
        self._activate_signals_at(group_ts)

    def _activate_signals_before(self, ts_recv_ns: int) -> None:
        while self._signal_cursor < len(self._signals):
            seed = self._signals[self._signal_cursor]
            if seed.decision_ts_recv_ns >= ts_recv_ns:
                break
            self._signal_cursor += 1
            self._activate_signal(seed)

    def _activate_signals_at(self, ts_recv_ns: int) -> None:
        while self._signal_cursor < len(self._signals):
            seed = self._signals[self._signal_cursor]
            if seed.decision_ts_recv_ns != ts_recv_ns:
                break
            self._signal_cursor += 1
            self._activate_signal(seed)

    def _activate_signal(self, seed: SignalSeed) -> None:
        decision_event = self._latest_event_by_contract.get(seed.contract_key)
        decision_ref = None if decision_event is None else _event_reference(decision_event)
        gate_failure: str | None = None
        if decision_ref is None:
            gate_failure = "NO_DECISION_QUOTE"
        elif seed.decision_ts_recv_ns - decision_ref.ts_recv_ns > MAX_EXECUTABLE_QUOTE_AGE_NS:
            gate_failure = "STALE_DECISION_QUOTE"
        elif not decision_ref.valid:
            gate_failure = "INVALID_DECISION_BBO"

        for scenario in EXECUTION_SCENARIOS:
            eligibility = seed.decision_ts_recv_ns + scenario.routing_delay_ns
            free_cells: list[tuple[int, int]] = []
            for take_profit in BARRIER_TICKS:
                for stop_loss in BARRIER_TICKS:
                    key = _CellKey(
                        scenario.scenario_id,
                        seed.direction,
                        seed.contract_key,
                        take_profit,
                        stop_loss,
                    )
                    owner = self._occupied.get(key)
                    if owner is not None:
                        self._store_non_entry_record(
                            seed,
                            scenario,
                            take_profit,
                            stop_loss,
                            EntryStatus.SKIPPED_OCCUPIED,
                            eligibility,
                            reason="LOG_AND_SKIP_OCCUPIED",
                            occupying_signal_id=owner,
                            decision_ref=decision_ref,
                        )
                        continue
                    failure_reason = (
                        "CONTRACT_ALREADY_TERMINATED"
                        if seed.contract_key in self._terminated_contracts
                        else gate_failure
                    )
                    if failure_reason is not None:
                        self._store_non_entry_record(
                            seed,
                            scenario,
                            take_profit,
                            stop_loss,
                            EntryStatus.ENTRY_NOT_FILLED,
                            eligibility,
                            reason=failure_reason,
                            decision_ref=decision_ref,
                            failure_ref=decision_ref,
                        )
                        continue
                    self._occupied[key] = seed.signal_id
                    free_cells.append((take_profit, stop_loss))
            if free_cells:
                if decision_ref is None:  # guarded by gate_failure above
                    raise SharedReplayError("valid entry gate lost its decision reference")
                self._pending.append(
                    _PendingEntry(
                        seed.signal_id,
                        scenario.scenario_id,
                        free_cells,
                        decision_ref,
                        decision_ref,
                    )
                )

    def _apply_first_touch_censors(
        self,
        events_by_contract: Mapping[str, Sequence[SharedExecutableQuote]],
    ) -> None:
        for contract_key, events in events_by_contract.items():
            owner_keys = sorted(
                self._groups_by_contract.get(contract_key, ()),
                key=lambda owner: (
                    self._signal_rank[owner.signal_id],
                    _SCENARIO_RANK[owner.scenario_id],
                ),
            )
            for owner_key in owner_keys:
                position_group = self._position_groups.get(owner_key)
                if position_group is None or position_group.first_touch_censor_ref is not None:
                    continue
                censor_ordinal = position_group.entry_session_ordinal + FIRST_TOUCH_ACTIVE_SESSIONS
                censor_event = next(
                    (event for event in events if event.session_ordinal >= censor_ordinal),
                    None,
                )
                if censor_event is None:
                    continue
                unresolved = {
                    cell
                    for cell in position_group.active_cells
                    if cell[1] not in position_group.stop_trigger_refs
                }
                position_group.censored_cells.update(unresolved)
                position_group.first_touch_censor_ref = _event_reference(censor_event)

    def _advance_position_group(
        self,
        owner_key: _OwnerKey,
        position_group: _PositionGroup,
        events: Sequence[SharedExecutableQuote],
        terminal_event: SharedExecutableQuote | None,
    ) -> None:
        """Advance one owner surface without scanning its 484 cells per event."""

        self._owner_group_advance_count += 1
        scenario = EXECUTION_SCENARIO_BY_ID[position_group.scenario_id]
        seed = self._signal_by_id[position_group.signal_id]
        direction = seed.direction
        terminal_index = None if terminal_event is None else terminal_event.quote.event_index
        normal_events = tuple(
            event
            for event in events
            if terminal_index is None or event.quote.event_index < terminal_index
        )
        fill_events = tuple(
            event
            for event in events
            if terminal_index is None or event.quote.event_index <= terminal_index
        )

        # A routed stop owns its whole SL column.  At most 22 columns are checked,
        # and a fill materializes that column only once.
        for stop_loss, trigger_ref in tuple(position_group.stop_trigger_refs.items()):
            column_cells = [
                (take_profit, stop_loss)
                for take_profit in BARRIER_TICKS
                if (take_profit, stop_loss) in position_group.active_cells
            ]
            if not column_cells:
                del position_group.stop_trigger_refs[stop_loss]
                continue
            eligibility = trigger_ref.ts_recv_ns + scenario.stop_latency_ns
            fill_event = next(
                (
                    event
                    for event in fill_events
                    if event.quote.ts_recv_ns >= eligibility and event.quote.valid
                ),
                None,
            )
            if fill_event is None:
                continue
            observed = fill_event.quote.executable_price(direction)
            stop_price = self._stop_price(position_group, direction, stop_loss)
            if direction is Direction.LONG:
                fill_price = min(
                    observed,
                    stop_price - scenario.stop_minimum_adverse_ticks,
                )
            else:
                fill_price = max(
                    observed,
                    stop_price + scenario.stop_minimum_adverse_ticks,
                )
            for take_profit, _ in column_cells:
                self._close_group_cell(
                    position_group,
                    take_profit,
                    stop_loss,
                    BarrierOutcome.STOP_FIRST,
                    fill_event,
                    fill_price,
                )
            del position_group.stop_trigger_refs[stop_loss]

        if not position_group.active_cells:
            self._drop_empty_position_group(owner_key, position_group)
            return

        active_stop_losses = {
            stop_loss
            for _, stop_loss in position_group.active_cells
            if stop_loss not in position_group.stop_trigger_refs
        }
        active_take_profits = {
            take_profit
            for take_profit, stop_loss in position_group.active_cells
            if stop_loss not in position_group.stop_trigger_refs
        }
        stop_hits: dict[int, SharedExecutableQuote] = {}
        take_profit_hits: dict[int, SharedExecutableQuote] = {}

        for event in normal_events:
            if not event.quote.valid:
                continue
            executable = event.quote.executable_price(direction)
            if direction is Direction.LONG:
                stop_capacity = position_group.entry_fill_price_ticks - executable
                take_profit_capacity = (
                    executable
                    - position_group.entry_fill_price_ticks
                    - scenario.take_profit_trade_through_ticks
                )
            else:
                stop_capacity = executable - position_group.entry_fill_price_ticks
                take_profit_capacity = (
                    position_group.entry_fill_price_ticks
                    - executable
                    - scenario.take_profit_trade_through_ticks
                )
            self._threshold_capacity_work_count += 2
            for stop_loss in BARRIER_TICKS[: bisect_right(BARRIER_TICKS, stop_capacity)]:
                if stop_loss in active_stop_losses and stop_loss not in stop_hits:
                    stop_hits[stop_loss] = event
            for take_profit in BARRIER_TICKS[: bisect_right(BARRIER_TICKS, take_profit_capacity)]:
                if take_profit in active_take_profits and take_profit not in take_profit_hits:
                    take_profit_hits[take_profit] = event

        # Mark every stop column before materializing any TP row.  This makes
        # STOP_FIRST independent of within-timestamp event order.
        for stop_loss, event in stop_hits.items():
            position_group.stop_trigger_refs[stop_loss] = _event_reference(event)

        for take_profit, event in take_profit_hits.items():
            target_price = self._take_profit_price(
                position_group,
                direction,
                take_profit,
            )
            for stop_loss in BARRIER_TICKS:
                cell = (take_profit, stop_loss)
                if (
                    cell in position_group.active_cells
                    and stop_loss not in position_group.stop_trigger_refs
                ):
                    self._close_group_cell(
                        position_group,
                        take_profit,
                        stop_loss,
                        BarrierOutcome.TP_FIRST,
                        event,
                        target_price,
                    )

        if terminal_event is not None and position_group.active_cells:
            observed = terminal_event.quote.executable_price(direction)
            terminal_price = (
                observed - scenario.terminal_exit_adverse_ticks
                if direction is Direction.LONG
                else observed + scenario.terminal_exit_adverse_ticks
            )
            for take_profit, stop_loss in sorted(position_group.active_cells):
                self._close_group_cell(
                    position_group,
                    take_profit,
                    stop_loss,
                    BarrierOutcome.TERMINAL_EXIT,
                    terminal_event,
                    terminal_price,
                    terminal_event=terminal_event,
                )

        self._drop_empty_position_group(owner_key, position_group)

    @staticmethod
    def _take_profit_price(
        position_group: _PositionGroup,
        direction: Direction,
        take_profit: int,
    ) -> int:
        if direction is Direction.LONG:
            return position_group.entry_fill_price_ticks + take_profit
        return position_group.entry_fill_price_ticks - take_profit

    @staticmethod
    def _stop_price(
        position_group: _PositionGroup,
        direction: Direction,
        stop_loss: int,
    ) -> int:
        if direction is Direction.LONG:
            return position_group.entry_fill_price_ticks - stop_loss
        return position_group.entry_fill_price_ticks + stop_loss

    def _drop_empty_position_group(
        self,
        owner_key: _OwnerKey,
        position_group: _PositionGroup,
    ) -> None:
        if position_group.active_cells:
            return
        self._position_groups.pop(owner_key, None)
        seed = self._signal_by_id[position_group.signal_id]
        owners = self._groups_by_contract.get(seed.contract_key)
        if owners is not None:
            owners.discard(owner_key)
            if not owners:
                del self._groups_by_contract[seed.contract_key]

    def _advance_pending_entries(
        self,
        events_by_contract: Mapping[str, Sequence[SharedExecutableQuote]],
    ) -> None:
        still_pending: list[_PendingEntry] = []
        for pending in self._pending:
            seed = self._signal_by_id[pending.signal_id]
            scenario = EXECUTION_SCENARIO_BY_ID[pending.scenario_id]
            eligibility = seed.decision_ts_recv_ns + scenario.routing_delay_ns
            contract_events = events_by_contract.get(seed.contract_key, ())
            if not contract_events:
                still_pending.append(pending)
                continue
            finalized = False
            for event_offset, event in enumerate(contract_events):
                reference = _event_reference(event)
                if event.terminal:
                    self._finalize_pending_no_fill(
                        pending,
                        reason="TERMINAL_BEFORE_ENTRY",
                        failure_ref=reference,
                    )
                    finalized = True
                    break
                if event.quote.ts_recv_ns < eligibility:
                    gap = event.quote.ts_recv_ns - pending.last_gate_ref.ts_recv_ns
                    pending.maximum_route_quote_gap_ns = max(
                        pending.maximum_route_quote_gap_ns,
                        gap,
                    )
                    if gap > MAX_EXECUTABLE_QUOTE_AGE_NS:
                        self._finalize_pending_no_fill(
                            pending,
                            reason="STALE_BBO_DURING_ROUTE",
                            failure_ref=reference,
                        )
                        finalized = True
                        break
                    pending.route_event_count += 1
                    pending.last_gate_ref = reference
                    if not reference.valid:
                        self._finalize_pending_no_fill(
                            pending,
                            reason="INVALID_BBO_DURING_ROUTE",
                            failure_ref=reference,
                        )
                        finalized = True
                        break
                    continue

                final_gap = eligibility - pending.last_gate_ref.ts_recv_ns
                pending.maximum_route_quote_gap_ns = max(
                    pending.maximum_route_quote_gap_ns,
                    final_gap,
                )
                if final_gap > MAX_EXECUTABLE_QUOTE_AGE_NS:
                    self._finalize_pending_no_fill(
                        pending,
                        reason="STALE_BBO_DURING_ROUTE",
                        failure_ref=reference,
                    )
                    finalized = True
                    break

                eligibility_ref = (
                    reference if event.quote.ts_recv_ns == eligibility else pending.last_gate_ref
                )
                if not eligibility_ref.valid:
                    self._finalize_pending_no_fill(
                        pending,
                        reason="INVALID_ENTRY_ELIGIBILITY_BBO",
                        eligibility_ref=eligibility_ref,
                        attempt_ref=(reference if event.quote.ts_recv_ns == eligibility else None),
                        failure_ref=eligibility_ref,
                    )
                    finalized = True
                    break
                entry_limit = self._opposite_entry_price(seed.direction, eligibility_ref)
                if not reference.valid:
                    self._finalize_pending_no_fill(
                        pending,
                        reason="INVALID_ENTRY_ATTEMPT_BBO",
                        eligibility_ref=eligibility_ref,
                        attempt_ref=reference,
                        entry_limit_price_ticks=entry_limit,
                        failure_ref=reference,
                    )
                    finalized = True
                    break
                observed_attempt_price = self._opposite_entry_price(
                    seed.direction,
                    reference,
                )
                attempt_price = (
                    observed_attempt_price + scenario.entry_adverse_ticks
                    if seed.direction is Direction.LONG
                    else observed_attempt_price - scenario.entry_adverse_ticks
                )
                outside_limit = (
                    attempt_price > entry_limit
                    if seed.direction is Direction.LONG
                    else attempt_price < entry_limit
                )
                if outside_limit:
                    self._finalize_pending_no_fill(
                        pending,
                        reason="PRICE_OUTSIDE_LIMIT",
                        eligibility_ref=eligibility_ref,
                        attempt_ref=reference,
                        entry_limit_price_ticks=entry_limit,
                        failure_ref=reference,
                    )
                    finalized = True
                    break
                owner_key = self._fill_pending_entry(
                    pending,
                    event,
                    eligibility_ref=eligibility_ref,
                    entry_limit_price_ticks=entry_limit,
                )
                post_entry_events = contract_events[event_offset + 1 :]
                if post_entry_events:
                    post_entry_terminal = next(
                        (item for item in post_entry_events if item.terminal),
                        None,
                    )
                    position_group = self._position_groups.get(owner_key)
                    if position_group is not None:
                        self._advance_position_group(
                            owner_key,
                            position_group,
                            post_entry_events,
                            terminal_event=post_entry_terminal,
                        )
                finalized = True
                break
            if not finalized:
                still_pending.append(pending)
        self._pending = still_pending

    @staticmethod
    def _opposite_entry_price(
        direction: Direction,
        reference: ReplayEventReference,
    ) -> int:
        price = (
            reference.best_ask_ticks if direction is Direction.LONG else reference.best_bid_ticks
        )
        if price is None:
            raise SharedReplayError("valid entry-gate reference has no opposite BBO")
        return price

    def _fill_pending_entry(
        self,
        pending: _PendingEntry,
        event: SharedExecutableQuote,
        *,
        eligibility_ref: ReplayEventReference,
        entry_limit_price_ticks: int,
    ) -> _OwnerKey:
        seed = self._signal_by_id[pending.signal_id]
        scenario = EXECUTION_SCENARIO_BY_ID[pending.scenario_id]
        if seed.direction is Direction.LONG:
            best_ask = event.quote.best_ask_ticks
            if best_ask is None:
                raise SharedReplayError("valid entry quote has no ask")
            entry_price = best_ask + scenario.entry_adverse_ticks
        else:
            best_bid = event.quote.best_bid_ticks
            if best_bid is None:
                raise SharedReplayError("valid entry quote has no bid")
            entry_price = best_bid - scenario.entry_adverse_ticks
        outside_limit = (
            entry_price > entry_limit_price_ticks
            if seed.direction is Direction.LONG
            else entry_price < entry_limit_price_ticks
        )
        if outside_limit:  # guarded before calling; keep the fill boundary fail-closed
            raise SharedReplayError("stressed entry fill cannot walk beyond the IOC limit")
        reference = _event_reference(event)

        owner_key = _OwnerKey(seed.signal_id, scenario.scenario_id)
        if owner_key in self._position_groups:
            raise SharedReplayError("signal/scenario already owns a live position group")
        for take_profit, stop_loss in pending.cells:
            key = _CellKey(
                scenario.scenario_id,
                seed.direction,
                seed.contract_key,
                take_profit,
                stop_loss,
            )
            if self._occupied.get(key) != seed.signal_id:
                raise SharedReplayError("entry reservation disagrees with occupancy state")
        self._position_groups[owner_key] = _PositionGroup(
            signal_id=seed.signal_id,
            scenario_id=scenario.scenario_id,
            active_cells=set(pending.cells),
            entry_fill_price_ticks=entry_price,
            entry_ref=reference,
            entry_session_ordinal=event.session_ordinal,
            decision_ref=pending.decision_ref,
            eligibility_ref=eligibility_ref,
            attempt_ref=reference,
            entry_limit_price_ticks=entry_limit_price_ticks,
            route_event_count=pending.route_event_count,
            maximum_route_quote_gap_ns=pending.maximum_route_quote_gap_ns,
            stop_trigger_refs={},
            censored_cells=set(),
        )
        self._groups_by_contract.setdefault(seed.contract_key, set()).add(owner_key)
        return owner_key

    def _finalize_pending_no_fill(
        self,
        pending: _PendingEntry,
        *,
        reason: str,
        eligibility_ref: ReplayEventReference | None = None,
        attempt_ref: ReplayEventReference | None = None,
        entry_limit_price_ticks: int | None = None,
        failure_ref: ReplayEventReference | None = None,
    ) -> None:
        seed = self._signal_by_id[pending.signal_id]
        scenario = EXECUTION_SCENARIO_BY_ID[pending.scenario_id]
        eligibility = seed.decision_ts_recv_ns + scenario.routing_delay_ns
        for take_profit, stop_loss in pending.cells:
            key = _CellKey(
                scenario.scenario_id,
                seed.direction,
                seed.contract_key,
                take_profit,
                stop_loss,
            )
            if self._occupied.get(key) != seed.signal_id:
                raise SharedReplayError("no-fill reservation disagrees with occupancy state")
            del self._occupied[key]
            self._store_non_entry_record(
                seed,
                scenario,
                take_profit,
                stop_loss,
                EntryStatus.ENTRY_NOT_FILLED,
                eligibility,
                reason=reason,
                decision_ref=pending.decision_ref,
                eligibility_ref=eligibility_ref,
                attempt_ref=attempt_ref,
                entry_limit_price_ticks=entry_limit_price_ticks,
                route_event_count=pending.route_event_count,
                maximum_route_quote_gap_ns=pending.maximum_route_quote_gap_ns,
                failure_ref=failure_ref,
            )

    def _close_group_cell(
        self,
        position_group: _PositionGroup,
        take_profit_ticks: int,
        stop_loss_ticks: int,
        portfolio_outcome: BarrierOutcome,
        fill_event: SharedExecutableQuote,
        exit_fill_price_ticks: int,
        *,
        terminal_event: SharedExecutableQuote | None = None,
    ) -> None:
        cell = (take_profit_ticks, stop_loss_ticks)
        if cell not in position_group.active_cells:
            raise SharedReplayError("position group cannot close an inactive cell")
        seed = self._signal_by_id[position_group.signal_id]
        scenario = EXECUTION_SCENARIO_BY_ID[position_group.scenario_id]
        take_profit_price = self._take_profit_price(
            position_group,
            seed.direction,
            take_profit_ticks,
        )
        stop_price = self._stop_price(position_group, seed.direction, stop_loss_ticks)
        trigger_ref = position_group.stop_trigger_refs.get(stop_loss_ticks)
        if cell in position_group.censored_cells:
            first_touch_outcome = BarrierOutcome.CENSORED
            censor_ref = position_group.first_touch_censor_ref
        elif trigger_ref is not None:
            first_touch_outcome = BarrierOutcome.STOP_FIRST
            censor_ref = None
        else:
            first_touch_outcome = portfolio_outcome
            censor_ref = None
        if seed.direction is Direction.LONG:
            buying_price = position_group.entry_fill_price_ticks
            selling_price = take_profit_price
        else:
            buying_price = take_profit_price
            selling_price = position_group.entry_fill_price_ticks

        record = ReplayResultRecord(
            signal_id=seed.signal_id,
            decision_ts_recv_ns=seed.decision_ts_recv_ns,
            utc_month=seed.utc_month,
            scenario_id=scenario.scenario_id,
            direction=seed.direction,
            contract_key=seed.contract_key,
            cell_id=_cell_id(take_profit_ticks, stop_loss_ticks),
            take_profit_ticks=take_profit_ticks,
            stop_loss_ticks=stop_loss_ticks,
            entry_status=EntryStatus.ENTRY_FILLED,
            entry_eligibility_ts_recv_ns=(seed.decision_ts_recv_ns + scenario.routing_delay_ns),
            entry_fill_price_ticks=position_group.entry_fill_price_ticks,
            buying_price_ticks=buying_price,
            selling_price_ticks=selling_price,
            loss_price_ticks=stop_price,
            take_profit_target_price_ticks=take_profit_price,
            stop_trigger_price_ticks=stop_price,
            first_touch_outcome=first_touch_outcome,
            portfolio_outcome=portfolio_outcome,
            exit_fill_price_ticks=exit_fill_price_ticks,
            decision_ref=position_group.decision_ref,
            eligibility_ref=position_group.eligibility_ref,
            attempt_ref=position_group.attempt_ref,
            entry_ref=position_group.entry_ref,
            trigger_ref=trigger_ref,
            fill_ref=_event_reference(fill_event),
            first_touch_censor_ref=censor_ref,
            terminal_ref=(None if terminal_event is None else _event_reference(terminal_event)),
            entry_limit_price_ticks=position_group.entry_limit_price_ticks,
            route_event_count=position_group.route_event_count,
            maximum_route_quote_gap_ns=position_group.maximum_route_quote_gap_ns,
            failure_ref=None,
            occupying_signal_id=None,
            no_fill_reason=None,
            completion_ts_recv_ns=fill_event.quote.ts_recv_ns,
        )
        self._store_record(record)
        key = _CellKey(
            scenario.scenario_id,
            seed.direction,
            seed.contract_key,
            take_profit_ticks,
            stop_loss_ticks,
        )
        if self._occupied.get(key) != seed.signal_id:
            raise SharedReplayError("position close disagrees with occupancy state")
        del self._occupied[key]
        position_group.active_cells.remove(cell)
        position_group.censored_cells.discard(cell)

    def _store_non_entry_record(
        self,
        seed: SignalSeed,
        scenario: ExecutionScenario,
        take_profit_ticks: int,
        stop_loss_ticks: int,
        status: EntryStatus,
        eligibility: int,
        *,
        reason: str,
        occupying_signal_id: str | None = None,
        decision_ref: ReplayEventReference | None = None,
        eligibility_ref: ReplayEventReference | None = None,
        attempt_ref: ReplayEventReference | None = None,
        entry_limit_price_ticks: int | None = None,
        route_event_count: int = 0,
        maximum_route_quote_gap_ns: int = 0,
        failure_ref: ReplayEventReference | None = None,
    ) -> None:
        self._store_record(
            ReplayResultRecord(
                signal_id=seed.signal_id,
                decision_ts_recv_ns=seed.decision_ts_recv_ns,
                utc_month=seed.utc_month,
                scenario_id=scenario.scenario_id,
                direction=seed.direction,
                contract_key=seed.contract_key,
                cell_id=_cell_id(take_profit_ticks, stop_loss_ticks),
                take_profit_ticks=take_profit_ticks,
                stop_loss_ticks=stop_loss_ticks,
                entry_status=status,
                entry_eligibility_ts_recv_ns=eligibility,
                entry_fill_price_ticks=None,
                buying_price_ticks=None,
                selling_price_ticks=None,
                loss_price_ticks=None,
                take_profit_target_price_ticks=None,
                stop_trigger_price_ticks=None,
                first_touch_outcome=None,
                portfolio_outcome=None,
                exit_fill_price_ticks=None,
                decision_ref=decision_ref,
                eligibility_ref=eligibility_ref,
                attempt_ref=attempt_ref,
                entry_ref=None,
                trigger_ref=None,
                fill_ref=None,
                first_touch_censor_ref=None,
                terminal_ref=None,
                entry_limit_price_ticks=entry_limit_price_ticks,
                route_event_count=route_event_count,
                maximum_route_quote_gap_ns=maximum_route_quote_gap_ns,
                failure_ref=failure_ref,
                occupying_signal_id=occupying_signal_id,
                no_fill_reason=reason,
                completion_ts_recv_ns=None,
            )
        )

    def _store_record(self, record: ReplayResultRecord) -> None:
        key = (
            record.signal_id,
            record.scenario_id,
            record.take_profit_ticks,
            record.stop_loss_ticks,
        )
        if key in self._records:
            raise SharedReplayError("duplicate deterministic result identity")
        self._records[key] = record
        self._result_record_count += 1

    def result_records(self) -> tuple[ReplayResultRecord, ...]:
        """Return the undrained canonical chronological result buffer."""

        return tuple(self._records.values())

    def drain_result_records(self) -> tuple[ReplayResultRecord, ...]:
        """Return and clear only completed records, preserving every live state."""

        records = self.result_records()
        self._records.clear()
        self._drained_record_count += len(records)
        return records

    def complete_source_date(self, source_date: date) -> None:
        """Flush the final timestamp after a reader merged one complete source date.

        The declaration makes a later event at the same receive timestamp invalid;
        otherwise such an event could retroactively change a STOP_FIRST tie.
        """

        if self._finished:
            raise SharedReplayError("a finished replay has no open source-date boundary")
        if type(source_date) is not date:
            raise SharedReplayError("completed source_date must be a date")
        if self._completed_source_date is not None and source_date <= self._completed_source_date:
            raise SharedReplayError("completed source dates must be strictly increasing")
        if any(value > source_date for value in self._last_source_date_by_contract.values()):
            raise SharedReplayError("cannot complete a source date after consuming a later date")
        if any(event.source_date > source_date for event in self._buffer):
            raise SharedReplayError("tie buffer contains an event after the completed source date")
        self._flush_buffer()
        self._completed_source_date = source_date
        self._completed_boundary_ts_recv_ns = (
            None if self._last_input_event is None else self._last_input_event.quote.ts_recv_ns
        )

    def finish(self) -> tuple[ReplayResultRecord, ...]:
        """Flush the stream and require every filled position to have an exit."""

        if self._finished:
            return self.result_records()
        self._flush_buffer()

        for pending in tuple(self._pending):
            seed = self._signal_by_id[pending.signal_id]
            scenario = EXECUTION_SCENARIO_BY_ID[pending.scenario_id]
            eligibility = seed.decision_ts_recv_ns + scenario.routing_delay_ns
            final_gap = eligibility - pending.last_gate_ref.ts_recv_ns
            pending.maximum_route_quote_gap_ns = max(
                pending.maximum_route_quote_gap_ns,
                final_gap,
            )
            if final_gap > MAX_EXECUTABLE_QUOTE_AGE_NS:
                self._finalize_pending_no_fill(
                    pending,
                    reason="STALE_BBO_DURING_ROUTE",
                    failure_ref=pending.last_gate_ref,
                )
            else:
                self._finalize_pending_no_fill(
                    pending,
                    reason="NO_ENTRY_ELIGIBILITY_EVENT",
                    eligibility_ref=pending.last_gate_ref,
                    entry_limit_price_ticks=self._opposite_entry_price(
                        seed.direction,
                        pending.last_gate_ref,
                    ),
                )
        self._pending.clear()

        # Signals beyond the final source event never reached an entry attempt.
        # Finalize each independently so an earlier no-attempt reservation cannot
        # manufacture an occupied skip for a later unseen signal.
        while self._signal_cursor < len(self._signals):
            seed = self._signals[self._signal_cursor]
            self._signal_cursor += 1
            pending_start = len(self._pending)
            self._activate_signal(seed)
            newly_pending = tuple(self._pending[pending_start:])
            del self._pending[pending_start:]
            for pending in newly_pending:
                scenario = EXECUTION_SCENARIO_BY_ID[pending.scenario_id]
                eligibility = seed.decision_ts_recv_ns + scenario.routing_delay_ns
                final_gap = eligibility - pending.last_gate_ref.ts_recv_ns
                pending.maximum_route_quote_gap_ns = max(
                    pending.maximum_route_quote_gap_ns,
                    final_gap,
                )
                if final_gap > MAX_EXECUTABLE_QUOTE_AGE_NS:
                    self._finalize_pending_no_fill(
                        pending,
                        reason="STALE_BBO_DURING_ROUTE",
                        failure_ref=pending.last_gate_ref,
                    )
                else:
                    self._finalize_pending_no_fill(
                        pending,
                        reason="NO_ENTRY_ELIGIBILITY_EVENT",
                        eligibility_ref=pending.last_gate_ref,
                        entry_limit_price_ticks=self._opposite_entry_price(
                            seed.direction,
                            pending.last_gate_ref,
                        ),
                    )

        if self._position_groups:
            contracts = sorted(
                {
                    self._signal_by_id[group.signal_id].contract_key
                    for group in self._position_groups.values()
                }
            )
            raise SharedReplayError(
                "filled positions require mandatory terminal exits; unresolved contracts: "
                + ", ".join(contracts)
            )
        if self._occupied:
            raise SharedReplayError("occupancy remains without a pending entry or position")
        self._finished = True
        return self.result_records()

    def checkpoint(self) -> dict[str, object]:
        """Return complete JSON-serializable replay state without flushing a tie group."""

        return {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "first_touch_active_sessions": FIRST_TOUCH_ACTIVE_SESSIONS,
            "barrier_ticks": list(BARRIER_TICKS),
            "execution_scenarios": [scenario.as_dict() for scenario in EXECUTION_SCENARIOS],
            "signals": [seed.as_dict() for seed in self._signals],
            "signal_cursor": self._signal_cursor,
            "pending_entries": [
                {
                    "signal_id": pending.signal_id,
                    "scenario_id": pending.scenario_id,
                    "cells": [list(cell) for cell in pending.cells],
                    "decision_ref": pending.decision_ref.as_dict(),
                    "last_gate_ref": pending.last_gate_ref.as_dict(),
                    "route_event_count": pending.route_event_count,
                    "maximum_route_quote_gap_ns": pending.maximum_route_quote_gap_ns,
                }
                for pending in self._pending
            ],
            "position_groups": [
                self._position_group_as_dict(owner_key, position_group)
                for owner_key, position_group in sorted(
                    self._position_groups.items(),
                    key=lambda item: (
                        self._signal_rank[item[0].signal_id],
                        _SCENARIO_RANK[item[0].scenario_id],
                    ),
                )
            ],
            "occupancy": [
                {
                    "scenario_id": key.scenario_id,
                    "direction": key.direction.value,
                    "contract_key": key.contract_key,
                    "take_profit_ticks": key.take_profit_ticks,
                    "stop_loss_ticks": key.stop_loss_ticks,
                    "signal_id": owner,
                }
                for key, owner in sorted(self._occupied.items(), key=self._internal_cell_sort_key)
            ],
            "records": [record.as_dict() for record in self.result_records()],
            "result_record_count": self._result_record_count,
            "drained_record_count": self._drained_record_count,
            "buffer": [event.as_dict() for event in self._buffer],
            "last_input_event": (
                None if self._last_input_event is None else self._last_input_event.as_dict()
            ),
            "latest_event_by_contract": {
                contract: event.as_dict()
                for contract, event in sorted(self._latest_event_by_contract.items())
            },
            "last_session_by_contract": dict(sorted(self._last_session_by_contract.items())),
            "last_source_date_by_contract": {
                contract: source_date.isoformat()
                for contract, source_date in sorted(self._last_source_date_by_contract.items())
            },
            "input_terminal_contracts": sorted(self._input_terminal_contracts),
            "terminated_contracts": sorted(self._terminated_contracts),
            "completed_source_date": (
                None
                if self._completed_source_date is None
                else self._completed_source_date.isoformat()
            ),
            "completed_boundary_ts_recv_ns": self._completed_boundary_ts_recv_ns,
            "source_event_count": self._source_event_count,
            "owner_group_advance_count": self._owner_group_advance_count,
            "threshold_capacity_work_count": self._threshold_capacity_work_count,
            "finished": self._finished,
        }

    @classmethod
    def from_checkpoint(cls, payload: Mapping[str, object]) -> SharedReplay:
        """Restore state produced by :meth:`checkpoint` after frozen-policy checks."""

        if not isinstance(payload, Mapping):
            raise SharedReplayError("checkpoint must be an object")
        if payload.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
            raise SharedReplayError("unsupported shared replay checkpoint version")
        if payload.get("first_touch_active_sessions") != FIRST_TOUCH_ACTIVE_SESSIONS:
            raise SharedReplayError("checkpoint first-touch policy differs from frozen policy")
        if payload.get("barrier_ticks") != list(BARRIER_TICKS):
            raise SharedReplayError("checkpoint barrier grid differs from frozen grid")
        if payload.get("execution_scenarios") != [
            scenario.as_dict() for scenario in EXECUTION_SCENARIOS
        ]:
            raise SharedReplayError("checkpoint execution scenarios differ from frozen policy")

        signal_values = payload.get("signals")
        if not isinstance(signal_values, list) or any(
            not isinstance(value, Mapping) for value in signal_values
        ):
            raise SharedReplayError("checkpoint signals must be a list of objects")
        replay = cls([SignalSeed.from_dict(value) for value in signal_values])
        replay._signal_cursor = _require_int(payload.get("signal_cursor"), label="signal_cursor")
        if replay._signal_cursor > len(replay._signals):
            raise SharedReplayError("checkpoint signal_cursor exceeds signal count")

        pending_values = payload.get("pending_entries")
        if not isinstance(pending_values, list):
            raise SharedReplayError("checkpoint pending_entries must be a list")
        replay._pending = [replay._pending_from_dict(value) for value in pending_values]

        position_group_values = payload.get("position_groups")
        if not isinstance(position_group_values, list):
            raise SharedReplayError("checkpoint position_groups must be a list")
        replay._position_groups = {}
        replay._groups_by_contract = {}
        for value in position_group_values:
            owner_key, position_group = replay._position_group_from_dict(value)
            if owner_key in replay._position_groups:
                raise SharedReplayError("checkpoint has duplicate position owner groups")
            replay._position_groups[owner_key] = position_group
            seed = replay._signal_by_id[position_group.signal_id]
            replay._groups_by_contract.setdefault(seed.contract_key, set()).add(owner_key)

        occupancy_values = payload.get("occupancy")
        if not isinstance(occupancy_values, list):
            raise SharedReplayError("checkpoint occupancy must be a list")
        replay._occupied = {}
        for value in occupancy_values:
            key, owner = replay._occupancy_from_dict(value)
            if key in replay._occupied:
                raise SharedReplayError("checkpoint has duplicate occupancy cells")
            replay._occupied[key] = owner

        record_values = payload.get("records")
        if not isinstance(record_values, list):
            raise SharedReplayError("checkpoint records must be a list")
        replay._records = {}
        replay._result_record_count = 0
        for value in record_values:
            record = replay.record_from_dict(value)
            replay._store_record(record)
        buffered_record_count = len(replay._records)
        replay._result_record_count = _require_int(
            payload.get("result_record_count"), label="result_record_count"
        )
        replay._drained_record_count = _require_int(
            payload.get("drained_record_count"), label="drained_record_count"
        )
        if replay._result_record_count != replay._drained_record_count + buffered_record_count:
            raise SharedReplayError("checkpoint result counters disagree with record buffer")

        buffer_values = payload.get("buffer")
        if not isinstance(buffer_values, list) or any(
            not isinstance(value, Mapping) for value in buffer_values
        ):
            raise SharedReplayError("checkpoint buffer must be a list of objects")
        replay._buffer = [SharedExecutableQuote.from_dict(value) for value in buffer_values]

        last_input_value = payload.get("last_input_event")
        if last_input_value is None:
            replay._last_input_event = None
        elif isinstance(last_input_value, Mapping):
            replay._last_input_event = SharedExecutableQuote.from_dict(last_input_value)
        else:
            raise SharedReplayError("checkpoint last_input_event is malformed")

        latest_values = payload.get("latest_event_by_contract")
        if not isinstance(latest_values, Mapping) or any(
            not isinstance(value, Mapping) for value in latest_values.values()
        ):
            raise SharedReplayError("checkpoint latest_event_by_contract must be an object")
        replay._latest_event_by_contract = {
            str(contract): SharedExecutableQuote.from_dict(value)
            for contract, value in latest_values.items()
        }

        sessions = payload.get("last_session_by_contract")
        source_dates = payload.get("last_source_date_by_contract")
        if not isinstance(sessions, Mapping) or not isinstance(source_dates, Mapping):
            raise SharedReplayError("checkpoint contract cursors must be objects")
        replay._last_session_by_contract = {
            str(contract): _require_int(value, label="session_ordinal")
            for contract, value in sessions.items()
        }
        try:
            replay._last_source_date_by_contract = {
                str(contract): date.fromisoformat(str(value))
                for contract, value in source_dates.items()
            }
        except ValueError as error:
            raise SharedReplayError("checkpoint contains an invalid source_date") from error

        replay._input_terminal_contracts = replay._string_set(
            payload.get("input_terminal_contracts"), "input_terminal_contracts"
        )
        replay._terminated_contracts = replay._string_set(
            payload.get("terminated_contracts"), "terminated_contracts"
        )
        completed_source_date = payload.get("completed_source_date")
        try:
            replay._completed_source_date = (
                None
                if completed_source_date is None
                else date.fromisoformat(str(completed_source_date))
            )
        except ValueError as error:
            raise SharedReplayError("checkpoint completed_source_date is invalid") from error
        completed_boundary = payload.get("completed_boundary_ts_recv_ns")
        replay._completed_boundary_ts_recv_ns = (
            None
            if completed_boundary is None
            else _require_int(completed_boundary, label="completed_boundary_ts_recv_ns")
        )
        replay._source_event_count = _require_int(
            payload.get("source_event_count"), label="source_event_count"
        )
        replay._owner_group_advance_count = _require_int(
            payload.get("owner_group_advance_count"),
            label="owner_group_advance_count",
        )
        replay._threshold_capacity_work_count = _require_int(
            payload.get("threshold_capacity_work_count"),
            label="threshold_capacity_work_count",
        )
        finished = payload.get("finished")
        if not isinstance(finished, bool):
            raise SharedReplayError("checkpoint finished must be a boolean")
        replay._finished = finished
        replay._validate_restored_state()
        return replay

    @staticmethod
    def _string_set(value: object, label: str) -> set[str]:
        if not isinstance(value, list) or any(
            not isinstance(item, str) or not item for item in value
        ):
            raise SharedReplayError(f"checkpoint {label} must be a string list")
        if len(set(value)) != len(value):
            raise SharedReplayError(f"checkpoint {label} contains duplicates")
        return set(value)

    @staticmethod
    def _internal_cell_sort_key(
        item: tuple[_CellKey, object],
    ) -> tuple[int, str, str, int, int]:
        key = item[0]
        return (
            _SCENARIO_RANK[key.scenario_id],
            key.direction.value,
            key.contract_key,
            key.take_profit_ticks,
            key.stop_loss_ticks,
        )

    def _position_group_as_dict(
        self,
        owner_key: _OwnerKey,
        position_group: _PositionGroup,
    ) -> dict[str, object]:
        seed = self._signal_by_id[position_group.signal_id]
        return {
            "signal_id": owner_key.signal_id,
            "scenario_id": owner_key.scenario_id,
            "direction": seed.direction.value,
            "contract_key": seed.contract_key,
            "active_cells": [
                [take_profit, stop_loss]
                for take_profit, stop_loss in sorted(position_group.active_cells)
            ],
            "entry_fill_price_ticks": position_group.entry_fill_price_ticks,
            "entry_ref": position_group.entry_ref.as_dict(),
            "entry_session_ordinal": position_group.entry_session_ordinal,
            "decision_ref": position_group.decision_ref.as_dict(),
            "eligibility_ref": position_group.eligibility_ref.as_dict(),
            "attempt_ref": position_group.attempt_ref.as_dict(),
            "entry_limit_price_ticks": position_group.entry_limit_price_ticks,
            "route_event_count": position_group.route_event_count,
            "maximum_route_quote_gap_ns": position_group.maximum_route_quote_gap_ns,
            "stop_trigger_refs": [
                {
                    "stop_loss_ticks": stop_loss,
                    "trigger_ref": trigger_ref.as_dict(),
                }
                for stop_loss, trigger_ref in sorted(position_group.stop_trigger_refs.items())
            ],
            "censored_cells": [
                [take_profit, stop_loss]
                for take_profit, stop_loss in sorted(position_group.censored_cells)
            ],
            "first_touch_censor_ref": (
                None
                if position_group.first_touch_censor_ref is None
                else position_group.first_touch_censor_ref.as_dict()
            ),
        }

    def _pending_from_dict(self, value: object) -> _PendingEntry:
        if not isinstance(value, Mapping):
            raise SharedReplayError("checkpoint pending entry must be an object")
        signal_id = _require_text(value.get("signal_id"), label="signal_id")
        scenario_id = _require_text(value.get("scenario_id"), label="scenario_id")
        if signal_id not in self._signal_by_id or scenario_id not in EXECUTION_SCENARIO_BY_ID:
            raise SharedReplayError("checkpoint pending entry has an unknown identity")
        cell_values = value.get("cells")
        if not isinstance(cell_values, list):
            raise SharedReplayError("checkpoint pending cells must be a list")
        cells: list[tuple[int, int]] = []
        for cell in cell_values:
            if not isinstance(cell, list) or len(cell) != 2:
                raise SharedReplayError("checkpoint pending cell is malformed")
            take_profit = _require_int(cell[0], label="take_profit_ticks")
            stop_loss = _require_int(cell[1], label="stop_loss_ticks")
            if take_profit not in BARRIER_TICKS or stop_loss not in BARRIER_TICKS:
                raise SharedReplayError("checkpoint pending cell is outside the frozen grid")
            cells.append((take_profit, stop_loss))
        if len(set(cells)) != len(cells):
            raise SharedReplayError("checkpoint pending entry has duplicate cells")
        decision_ref = _reference_from_optional(value.get("decision_ref"))
        last_gate_ref = _reference_from_optional(value.get("last_gate_ref"))
        if decision_ref is None or last_gate_ref is None:
            raise SharedReplayError("checkpoint pending entry requires gate references")
        return _PendingEntry(
            signal_id,
            scenario_id,
            cells,
            decision_ref,
            last_gate_ref,
            _require_int(value.get("route_event_count"), label="route_event_count"),
            _require_int(
                value.get("maximum_route_quote_gap_ns"),
                label="maximum_route_quote_gap_ns",
            ),
        )

    def _position_group_from_dict(
        self,
        value: object,
    ) -> tuple[_OwnerKey, _PositionGroup]:
        if not isinstance(value, Mapping):
            raise SharedReplayError("checkpoint position group must be an object")
        signal_id = _require_text(value.get("signal_id"), label="signal_id")
        scenario_id = _require_text(value.get("scenario_id"), label="scenario_id")
        if scenario_id not in EXECUTION_SCENARIO_BY_ID:
            raise SharedReplayError("checkpoint position group has an unknown scenario")
        seed = self._signal_by_id.get(signal_id)
        if seed is None:
            raise SharedReplayError("checkpoint position group has an unknown signal")
        try:
            direction = Direction(value.get("direction"))
        except (TypeError, ValueError) as error:
            raise SharedReplayError("checkpoint position group has an invalid direction") from error
        contract_key = _require_text(value.get("contract_key"), label="contract_key")
        if seed.direction is not direction or seed.contract_key != contract_key:
            raise SharedReplayError("checkpoint position group disagrees with its signal seed")

        def parse_cells(raw: object, *, label: str) -> set[tuple[int, int]]:
            if not isinstance(raw, list):
                raise SharedReplayError(f"checkpoint {label} must be a list")
            cells: set[tuple[int, int]] = set()
            for item in raw:
                if not isinstance(item, list) or len(item) != 2:
                    raise SharedReplayError(f"checkpoint {label} cell is malformed")
                take_profit = _require_int(item[0], label="take_profit_ticks")
                stop_loss = _require_int(item[1], label="stop_loss_ticks")
                if take_profit not in BARRIER_TICKS or stop_loss not in BARRIER_TICKS:
                    raise SharedReplayError(f"checkpoint {label} is outside the frozen grid")
                cell = (take_profit, stop_loss)
                if cell in cells:
                    raise SharedReplayError(f"checkpoint {label} contains duplicate cells")
                cells.add(cell)
            return cells

        active_cells = parse_cells(value.get("active_cells"), label="active_cells")
        if not active_cells:
            raise SharedReplayError("checkpoint position group cannot be empty")
        censored_cells = parse_cells(value.get("censored_cells"), label="censored_cells")
        if not censored_cells.issubset(active_cells):
            raise SharedReplayError("checkpoint censored cells must remain active")

        entry_ref = _reference_from_optional(value.get("entry_ref"))
        if entry_ref is None:
            raise SharedReplayError("checkpoint position group requires an entry_ref")
        decision_ref = _reference_from_optional(value.get("decision_ref"))
        eligibility_ref = _reference_from_optional(value.get("eligibility_ref"))
        attempt_ref = _reference_from_optional(value.get("attempt_ref"))
        if decision_ref is None or eligibility_ref is None or attempt_ref is None:
            raise SharedReplayError(
                "checkpoint position group requires complete entry-gate references"
            )
        stop_trigger_values = value.get("stop_trigger_refs")
        if not isinstance(stop_trigger_values, list):
            raise SharedReplayError("checkpoint stop_trigger_refs must be a list")
        stop_trigger_refs: dict[int, ReplayEventReference] = {}
        for item in stop_trigger_values:
            if not isinstance(item, Mapping):
                raise SharedReplayError("checkpoint stop trigger must be an object")
            stop_loss = _require_int(item.get("stop_loss_ticks"), label="stop_loss_ticks")
            trigger_ref = _reference_from_optional(item.get("trigger_ref"))
            if stop_loss not in BARRIER_TICKS or trigger_ref is None:
                raise SharedReplayError("checkpoint stop trigger is malformed")
            if stop_loss in stop_trigger_refs:
                raise SharedReplayError("checkpoint has duplicate stop triggers")
            if not any(cell[1] == stop_loss for cell in active_cells):
                raise SharedReplayError("checkpoint stop trigger has no active column")
            stop_trigger_refs[stop_loss] = trigger_ref

        position_group = _PositionGroup(
            signal_id=signal_id,
            scenario_id=scenario_id,
            active_cells=active_cells,
            entry_fill_price_ticks=_require_int(
                value.get("entry_fill_price_ticks"),
                label="entry_fill_price_ticks",
                minimum=-(2**63),
            ),
            entry_ref=entry_ref,
            entry_session_ordinal=_require_int(
                value.get("entry_session_ordinal"), label="entry_session_ordinal"
            ),
            decision_ref=decision_ref,
            eligibility_ref=eligibility_ref,
            attempt_ref=attempt_ref,
            entry_limit_price_ticks=_require_int(
                value.get("entry_limit_price_ticks"),
                label="entry_limit_price_ticks",
                minimum=-(2**63),
            ),
            route_event_count=_require_int(
                value.get("route_event_count"), label="route_event_count"
            ),
            maximum_route_quote_gap_ns=_require_int(
                value.get("maximum_route_quote_gap_ns"),
                label="maximum_route_quote_gap_ns",
            ),
            stop_trigger_refs=stop_trigger_refs,
            censored_cells=censored_cells,
            first_touch_censor_ref=_reference_from_optional(value.get("first_touch_censor_ref")),
        )
        return _OwnerKey(signal_id, scenario_id), position_group

    def _occupancy_from_dict(self, value: object) -> tuple[_CellKey, str]:
        if not isinstance(value, Mapping):
            raise SharedReplayError("checkpoint occupancy must be an object")
        key = self._cell_key_from_dict(value)
        owner = _require_text(value.get("signal_id"), label="signal_id")
        if owner not in self._signal_by_id:
            raise SharedReplayError("checkpoint occupancy has an unknown signal")
        return key, owner

    @staticmethod
    def _cell_key_from_dict(value: Mapping[str, object]) -> _CellKey:
        scenario_id = _require_text(value.get("scenario_id"), label="scenario_id")
        if scenario_id not in EXECUTION_SCENARIO_BY_ID:
            raise SharedReplayError("checkpoint cell has an unknown scenario")
        try:
            direction = Direction(value.get("direction"))
        except (TypeError, ValueError) as error:
            raise SharedReplayError("checkpoint cell has an invalid direction") from error
        take_profit = _require_int(value.get("take_profit_ticks"), label="take_profit_ticks")
        stop_loss = _require_int(value.get("stop_loss_ticks"), label="stop_loss_ticks")
        if take_profit not in BARRIER_TICKS or stop_loss not in BARRIER_TICKS:
            raise SharedReplayError("checkpoint cell is outside the frozen grid")
        return _CellKey(
            scenario_id,
            direction,
            _require_text(value.get("contract_key"), label="contract_key"),
            take_profit,
            stop_loss,
        )

    @staticmethod
    def record_from_dict(value: object) -> ReplayResultRecord:
        """Strictly rebuild the lossless public detail-record mapping.

        Checkpoint resume and immutable result-shard readers intentionally use
        this one parser so persisted Buying/Selling/Loss prices and event
        references cannot acquire divergent deserialization rules.
        """

        if not isinstance(value, Mapping):
            raise SharedReplayError("checkpoint result record must be an object")
        try:
            direction = Direction(value.get("direction"))
            entry_status = EntryStatus(value.get("entry_status"))
            first_touch = (
                None
                if value.get("first_touch_outcome") is None
                else BarrierOutcome(value.get("first_touch_outcome"))
            )
            portfolio = (
                None
                if value.get("portfolio_outcome") is None
                else BarrierOutcome(value.get("portfolio_outcome"))
            )
        except (TypeError, ValueError) as error:
            raise SharedReplayError("checkpoint result record contains an invalid enum") from error

        def optional_int(label: str) -> int | None:
            raw = value.get(label)
            return None if raw is None else _require_int(raw, label=label, minimum=-(2**63))

        return ReplayResultRecord(
            signal_id=_require_text(value.get("signal_id"), label="signal_id"),
            decision_ts_recv_ns=_require_int(
                value.get("decision_ts_recv_ns"), label="decision_ts_recv_ns"
            ),
            utc_month=_validate_utc_month(value.get("utc_month")),
            scenario_id=_require_text(value.get("scenario_id"), label="scenario_id"),
            direction=direction,
            contract_key=_require_text(value.get("contract_key"), label="contract_key"),
            cell_id=_require_text(value.get("cell_id"), label="cell_id"),
            take_profit_ticks=_require_int(
                value.get("take_profit_ticks"), label="take_profit_ticks"
            ),
            stop_loss_ticks=_require_int(value.get("stop_loss_ticks"), label="stop_loss_ticks"),
            entry_status=entry_status,
            entry_eligibility_ts_recv_ns=_require_int(
                value.get("entry_eligibility_ts_recv_ns"),
                label="entry_eligibility_ts_recv_ns",
            ),
            entry_fill_price_ticks=optional_int("entry_fill_price_ticks"),
            buying_price_ticks=optional_int("buying_price_ticks"),
            selling_price_ticks=optional_int("selling_price_ticks"),
            loss_price_ticks=optional_int("loss_price_ticks"),
            take_profit_target_price_ticks=optional_int("take_profit_target_price_ticks"),
            stop_trigger_price_ticks=optional_int("stop_trigger_price_ticks"),
            first_touch_outcome=first_touch,
            portfolio_outcome=portfolio,
            exit_fill_price_ticks=optional_int("exit_fill_price_ticks"),
            decision_ref=_reference_from_optional(value.get("decision_ref")),
            eligibility_ref=_reference_from_optional(value.get("eligibility_ref")),
            attempt_ref=_reference_from_optional(value.get("attempt_ref")),
            entry_ref=_reference_from_optional(value.get("entry_ref")),
            trigger_ref=_reference_from_optional(value.get("trigger_ref")),
            fill_ref=_reference_from_optional(value.get("fill_ref")),
            first_touch_censor_ref=_reference_from_optional(value.get("first_touch_censor_ref")),
            terminal_ref=_reference_from_optional(value.get("terminal_ref")),
            entry_limit_price_ticks=optional_int("entry_limit_price_ticks"),
            route_event_count=_require_int(
                value.get("route_event_count"), label="route_event_count"
            ),
            maximum_route_quote_gap_ns=_require_int(
                value.get("maximum_route_quote_gap_ns"),
                label="maximum_route_quote_gap_ns",
            ),
            failure_ref=_reference_from_optional(value.get("failure_ref")),
            occupying_signal_id=(
                None
                if value.get("occupying_signal_id") is None
                else _require_text(value.get("occupying_signal_id"), label="occupying_signal_id")
            ),
            no_fill_reason=(
                None
                if value.get("no_fill_reason") is None
                else _require_text(value.get("no_fill_reason"), label="no_fill_reason")
            ),
            completion_ts_recv_ns=optional_int("completion_ts_recv_ns"),
        )

    def _validate_restored_state(self) -> None:
        expected_occupied: dict[_CellKey, str] = {}
        for pending in self._pending:
            seed = self._signal_by_id[pending.signal_id]
            for take_profit, stop_loss in pending.cells:
                key = _CellKey(
                    pending.scenario_id,
                    seed.direction,
                    seed.contract_key,
                    take_profit,
                    stop_loss,
                )
                if key in expected_occupied:
                    raise SharedReplayError("checkpoint pending reservations overlap")
                expected_occupied[key] = pending.signal_id
        expected_groups_by_contract: dict[str, set[_OwnerKey]] = {}
        for owner_key, position_group in self._position_groups.items():
            if (
                owner_key.signal_id != position_group.signal_id
                or owner_key.scenario_id != position_group.scenario_id
            ):
                raise SharedReplayError("checkpoint position owner identity disagrees")
            seed = self._signal_by_id[position_group.signal_id]
            expected_groups_by_contract.setdefault(seed.contract_key, set()).add(owner_key)
            for take_profit, stop_loss in position_group.active_cells:
                key = _CellKey(
                    position_group.scenario_id,
                    seed.direction,
                    seed.contract_key,
                    take_profit,
                    stop_loss,
                )
                if key in expected_occupied:
                    raise SharedReplayError(
                        "checkpoint position group overlaps a pending reservation"
                    )
                expected_occupied[key] = position_group.signal_id
        if expected_groups_by_contract != self._groups_by_contract:
            raise SharedReplayError("checkpoint contract owner-group index disagrees")
        if expected_occupied != self._occupied:
            raise SharedReplayError("checkpoint occupancy does not match pending/position state")
        live_result_keys = {
            (position_group.signal_id, position_group.scenario_id, take_profit, stop_loss)
            for position_group in self._position_groups.values()
            for take_profit, stop_loss in position_group.active_cells
        }
        live_result_keys.update(
            (
                pending.signal_id,
                pending.scenario_id,
                take_profit,
                stop_loss,
            )
            for pending in self._pending
            for take_profit, stop_loss in pending.cells
        )
        if live_result_keys.intersection(self._records):
            raise SharedReplayError("checkpoint result buffer overlaps live replay state")
        if self._buffer:
            timestamps = {event.quote.ts_recv_ns for event in self._buffer}
            if len(timestamps) != 1:
                raise SharedReplayError("checkpoint tie buffer contains multiple timestamps")
        if self._last_input_event is None:
            if self._source_event_count or self._buffer:
                raise SharedReplayError("checkpoint source cursor is incomplete")
        elif self._source_event_count == 0:
            raise SharedReplayError("checkpoint last input exists with zero source events")
        if self._completed_source_date is not None and self._buffer:
            raise SharedReplayError("completed source-date checkpoint retains a tie buffer")
        if (
            self._completed_boundary_ts_recv_ns is not None
            and self._last_input_event is not None
            and self._completed_boundary_ts_recv_ns > self._last_input_event.quote.ts_recv_ns
        ):
            raise SharedReplayError("checkpoint completed boundary exceeds its input cursor")
        for contract, event in self._latest_event_by_contract.items():
            if event.contract_key != contract:
                raise SharedReplayError("checkpoint latest-event contract key disagrees")
        if self._finished and (
            self._pending or self._position_groups or self._occupied or self._buffer
        ):
            raise SharedReplayError("finished checkpoint retains live replay state")


__all__ = [
    "CHECKPOINT_SCHEMA_VERSION",
    "EXECUTION_SCENARIOS",
    "EXECUTION_SCENARIO_BY_ID",
    "FIRST_TOUCH_ACTIVE_SESSIONS",
    "MAX_EXECUTABLE_QUOTE_AGE_NS",
    "ExecutionScenario",
    "ReplayEventReference",
    "ReplayResultRecord",
    "SharedExecutableQuote",
    "SharedReplay",
    "SharedReplayError",
    "SignalSeed",
]
