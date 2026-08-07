"""Conservative event-ordered barrier replay for one already-filled position.

The engine intentionally has no signal, position-allocation, or portfolio-occupancy
logic.  A caller must enforce the one-position policy and skipped-signal accounting
before invoking this module.  One invocation consumes one direction's executable
quote path once, records first threshold events, and then reuses those 44 threshold
results to materialize the complete 22 by 22 surface.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from itertools import pairwise
from typing import Final

BARRIER_TICKS: Final = tuple(range(24, 193, 8))
EXPECTED_AXIS_COUNT: Final = 22
EXPECTED_CELL_COUNT: Final = 484
TAKE_PROFIT_TRADE_THROUGH_TICKS: Final = 1
STOP_LATENCY_NS: Final = 1_000_000_000
STOP_MINIMUM_ADVERSE_TICKS: Final = 2
PORTFOLIO_OCCUPANCY_SUPPORTED: Final = False


class BarrierReplayError(ValueError):
    """The replay inputs cannot produce a deterministic governed surface."""


class Direction(StrEnum):
    """The side of the already-filled position being replayed."""

    LONG = "LONG"
    SHORT = "SHORT"


class BarrierOutcome(StrEnum):
    """Mutually exclusive terminal state for one barrier cell."""

    TP_FIRST = "TP_FIRST"
    STOP_FIRST = "STOP_FIRST"
    TERMINAL_EXIT = "TERMINAL_EXIT"
    CENSORED = "CENSORED"


class CensorReason(StrEnum):
    """Why a cell has no executable exit fill in the supplied path."""

    OBSERVATION_WINDOW_ENDED = "OBSERVATION_WINDOW_ENDED"
    STOP_TRIGGERED_BUT_UNFILLED = "STOP_TRIGGERED_BUT_UNFILLED"
    INVALID_TERMINAL_QUOTE = "INVALID_TERMINAL_QUOTE"


def _require_int(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise BarrierReplayError(f"{label} must be an integer")
    return value


@dataclass(frozen=True, slots=True)
class ExecutableQuote:
    """One canonically ordered executable BBO observation.

    ``event_index`` is the upstream canonical source-path ordinal.  Equal receive
    timestamps are permitted, but indexes must remain strictly increasing.  An
    event marked invalid is retained for ordering/audit purposes and is never
    allowed to trigger or fill an order, even if it carries numeric prices.
    """

    event_index: int
    ts_recv_ns: int
    best_bid_ticks: int | None
    best_ask_ticks: int | None
    valid: bool = True

    def __post_init__(self) -> None:
        _require_int(self.event_index, label="event_index")
        _require_int(self.ts_recv_ns, label="ts_recv_ns")
        if self.event_index < 0:
            raise BarrierReplayError("event_index must be non-negative")
        if not isinstance(self.valid, bool):
            raise BarrierReplayError("valid must be a boolean")

        for field_name in ("best_bid_ticks", "best_ask_ticks"):
            value = getattr(self, field_name)
            if value is not None:
                _require_int(value, label=field_name)

        if self.valid:
            if self.best_bid_ticks is None or self.best_ask_ticks is None:
                raise BarrierReplayError("a valid quote requires executable bid and ask prices")
            if self.best_bid_ticks >= self.best_ask_ticks:
                raise BarrierReplayError("a valid executable quote must have best_bid_ticks < ask")

    def executable_price(self, direction: Direction) -> int:
        """Return the executable liquidation side; invalid quotes are forbidden."""

        if not self.valid:
            raise BarrierReplayError("an invalid quote has no executable price")
        price = self.best_bid_ticks if direction is Direction.LONG else self.best_ask_ticks
        if price is None:  # guarded by validation, retained as a defensive invariant
            raise BarrierReplayError("valid quote is missing its executable side")
        return price


@dataclass(frozen=True, slots=True)
class EventReference:
    """Auditable reference to the exact executable event used by a threshold."""

    event_index: int
    ts_recv_ns: int
    executable_price_ticks: int


@dataclass(frozen=True, slots=True)
class TakeProfitThresholdResult:
    """First one-tick trade-through, independent of every stop distance."""

    distance_ticks: int
    target_price_ticks: int
    trade_through_price_ticks: int
    fill_event: EventReference | None


@dataclass(frozen=True, slots=True)
class StopThresholdResult:
    """First stop trigger and its delayed executable fill, if one exists."""

    distance_ticks: int
    trigger_price_ticks: int
    trigger_event: EventReference | None
    fill_event: EventReference | None
    fill_price_ticks: int | None


@dataclass(frozen=True, slots=True)
class ThresholdReplay:
    """The 22 TP and 22 stop threshold results shared by all 484 cells."""

    direction: Direction
    source_event_count: int
    valid_event_count: int
    source_path_passes: int
    take_profits: tuple[TakeProfitThresholdResult, ...]
    stops: tuple[StopThresholdResult, ...]


@dataclass(frozen=True, slots=True)
class BarrierCellResult:
    """Complete price and event record for one TP/SL cell."""

    cell_id: str
    direction: Direction
    take_profit_ticks: int
    stop_loss_ticks: int
    entry_fill_price_ticks: int
    buying_price_ticks: int
    selling_price_ticks: int
    take_profit_target_price_ticks: int
    loss_trigger_price_ticks: int
    outcome: BarrierOutcome
    exit_fill_price_ticks: int | None
    take_profit_fill_price_ticks: int | None
    loss_fill_price_ticks: int | None
    terminal_fill_price_ticks: int | None
    trigger_event_index: int | None
    trigger_ts_recv_ns: int | None
    fill_event_index: int | None
    fill_ts_recv_ns: int | None
    take_profit_fill_event_index: int | None
    take_profit_fill_ts_recv_ns: int | None
    loss_trigger_event_index: int | None
    loss_trigger_ts_recv_ns: int | None
    loss_fill_event_index: int | None
    loss_fill_ts_recv_ns: int | None
    terminal_event_index: int | None
    terminal_ts_recv_ns: int | None
    terminal_event_valid: bool | None
    censor_reason: CensorReason | None


@dataclass(frozen=True, slots=True)
class BarrierSurface:
    """One direction's fixed 484-cell result, derived from one path replay.

    Portfolio occupancy is deliberately outside this object.  ``cells`` are in
    take-profit-major, then stop-loss-minor order, matching cell identity policy.
    """

    direction: Direction
    entry_fill_price_ticks: int
    take_profit_grid: tuple[int, ...]
    stop_loss_grid: tuple[int, ...]
    cells: tuple[BarrierCellResult, ...]
    thresholds: ThresholdReplay

    def cell(self, take_profit_ticks: int, stop_loss_ticks: int) -> BarrierCellResult:
        """Return one cell by its exact registered distances."""

        try:
            take_profit_index = self.take_profit_grid.index(take_profit_ticks)
            stop_loss_index = self.stop_loss_grid.index(stop_loss_ticks)
        except ValueError as error:
            raise KeyError(
                f"unknown barrier cell tp{take_profit_ticks}_sl{stop_loss_ticks}"
            ) from error
        index = take_profit_index * len(self.stop_loss_grid) + stop_loss_index
        return self.cells[index]


def _validated_direction(value: Direction | str) -> Direction:
    try:
        return Direction(value)
    except (TypeError, ValueError) as error:
        raise BarrierReplayError("direction must be LONG or SHORT") from error


def _validated_grid(values: Iterable[int], *, label: str) -> tuple[int, ...]:
    axis = tuple(values)
    if any(isinstance(value, bool) or not isinstance(value, int) for value in axis):
        raise BarrierReplayError(f"{label} must contain only integer tick distances")
    if len(set(axis)) != len(axis):
        raise BarrierReplayError(f"{label} contains duplicate distances")
    if any(left >= right for left, right in pairwise(axis)):
        raise BarrierReplayError(f"{label} must be in strictly increasing input order")
    if axis != BARRIER_TICKS:
        raise BarrierReplayError(
            f"{label} must equal the frozen 22-value grid 24..192 in steps of 8"
        )
    return axis


def _event_reference(event: ExecutableQuote, direction: Direction) -> EventReference:
    return EventReference(
        event_index=event.event_index,
        ts_recv_ns=event.ts_recv_ns,
        executable_price_ticks=event.executable_price(direction),
    )


def _validate_next_event(
    event: object,
    previous: ExecutableQuote | None,
    *,
    label: str,
) -> ExecutableQuote:
    if not isinstance(event, ExecutableQuote):
        raise BarrierReplayError(f"{label} must contain only ExecutableQuote values")
    if previous is not None:
        if event.event_index <= previous.event_index:
            raise BarrierReplayError("event_index input order must be strictly increasing")
        if event.ts_recv_ns < previous.ts_recv_ns:
            raise BarrierReplayError("ts_recv_ns input order must be non-decreasing")
    return event


def _planned_prices(
    direction: Direction,
    entry_fill_price_ticks: int,
    take_profit_ticks: int,
    stop_loss_ticks: int,
) -> tuple[int, int, int, int]:
    if direction is Direction.LONG:
        target = entry_fill_price_ticks + take_profit_ticks
        stop = entry_fill_price_ticks - stop_loss_ticks
        return entry_fill_price_ticks, target, target, stop
    target = entry_fill_price_ticks - take_profit_ticks
    stop = entry_fill_price_ticks + stop_loss_ticks
    return target, entry_fill_price_ticks, target, stop


def _take_profit_capacity(
    direction: Direction,
    entry_fill_price_ticks: int,
    executable_price_ticks: int,
) -> int:
    if direction is Direction.LONG:
        return executable_price_ticks - entry_fill_price_ticks - TAKE_PROFIT_TRADE_THROUGH_TICKS
    return entry_fill_price_ticks - executable_price_ticks - TAKE_PROFIT_TRADE_THROUGH_TICKS


def _stop_capacity(
    direction: Direction,
    entry_fill_price_ticks: int,
    executable_price_ticks: int,
) -> int:
    if direction is Direction.LONG:
        return entry_fill_price_ticks - executable_price_ticks
    return executable_price_ticks - entry_fill_price_ticks


def _stop_fill_price(
    direction: Direction,
    trigger_price_ticks: int,
    first_executable_price_ticks: int,
) -> int:
    if direction is Direction.LONG:
        minimum_adverse = trigger_price_ticks - STOP_MINIMUM_ADVERSE_TICKS
        return min(first_executable_price_ticks, minimum_adverse)
    minimum_adverse = trigger_price_ticks + STOP_MINIMUM_ADVERSE_TICKS
    return max(first_executable_price_ticks, minimum_adverse)


def _same_or_earlier_timestamp(
    stop_event: EventReference,
    take_profit_event: EventReference | None,
) -> bool:
    """STOP_FIRST deliberately ignores event index when timestamps tie."""

    return take_profit_event is None or stop_event.ts_recv_ns <= take_profit_event.ts_recv_ns


def replay_barrier_surface(
    *,
    direction: Direction | str,
    entry_fill_price_ticks: int,
    events: Iterable[ExecutableQuote],
    terminal_event: ExecutableQuote | None = None,
    take_profit_grid: Iterable[int] = BARRIER_TICKS,
    stop_loss_grid: Iterable[int] = BARRIER_TICKS,
) -> BarrierSurface:
    """Replay one executable path once and emit the complete conservative surface.

    ``terminal_event`` is a forced-liquidation observation after the ordinary
    barrier path.  It may fill an already-routed stop when latency has elapsed;
    otherwise a valid terminal quote liquidates unresolved cells on the executable
    side.  An invalid terminal quote never fills and leaves those cells censored.
    The terminal observation is not a new take-profit or stop-trigger opportunity.
    """

    resolved_direction = _validated_direction(direction)
    entry_price = _require_int(entry_fill_price_ticks, label="entry_fill_price_ticks")
    take_profits = _validated_grid(take_profit_grid, label="take_profit_grid")
    stops = _validated_grid(stop_loss_grid, label="stop_loss_grid")

    take_profit_hits: list[EventReference | None] = [None] * len(take_profits)
    stop_triggers: list[EventReference | None] = [None] * len(stops)
    stop_fill_events: list[EventReference | None] = [None] * len(stops)
    stop_fill_prices: list[int | None] = [None] * len(stops)
    pending_stop_indexes: set[int] = set()
    next_take_profit_index = 0
    next_stop_index = 0
    source_event_count = 0
    valid_event_count = 0

    def fill_pending_stops(event: ExecutableQuote) -> None:
        if not event.valid:
            return
        executable_price = event.executable_price(resolved_direction)
        for stop_index in sorted(pending_stop_indexes):
            trigger = stop_triggers[stop_index]
            if trigger is None:
                raise BarrierReplayError("pending stop is missing its trigger")
            if event.ts_recv_ns < trigger.ts_recv_ns + STOP_LATENCY_NS:
                continue
            stop_fill_events[stop_index] = _event_reference(event, resolved_direction)
            trigger_price = (
                entry_price - stops[stop_index]
                if resolved_direction is Direction.LONG
                else entry_price + stops[stop_index]
            )
            stop_fill_prices[stop_index] = _stop_fill_price(
                resolved_direction,
                trigger_price,
                executable_price,
            )
            pending_stop_indexes.remove(stop_index)

    def process_timestamp_group(group: Sequence[ExecutableQuote]) -> None:
        nonlocal next_take_profit_index, next_stop_index

        # Stops routed on an earlier timestamp receive the first valid eligible event.
        for event in group:
            fill_pending_stops(event)

        # Nested thresholds make one excursion update reusable across all distances.
        for event in group:
            if not event.valid:
                continue
            executable_price = event.executable_price(resolved_direction)
            take_profit_capacity = _take_profit_capacity(
                resolved_direction,
                entry_price,
                executable_price,
            )
            while (
                next_take_profit_index < len(take_profits)
                and take_profits[next_take_profit_index] <= take_profit_capacity
            ):
                take_profit_hits[next_take_profit_index] = _event_reference(
                    event, resolved_direction
                )
                next_take_profit_index += 1

            stop_capacity = _stop_capacity(
                resolved_direction,
                entry_price,
                executable_price,
            )
            while next_stop_index < len(stops) and stops[next_stop_index] <= stop_capacity:
                stop_triggers[next_stop_index] = _event_reference(event, resolved_direction)
                pending_stop_indexes.add(next_stop_index)
                next_stop_index += 1

    previous_event: ExecutableQuote | None = None
    timestamp_group: list[ExecutableQuote] = []
    for raw_event in events:
        event = _validate_next_event(raw_event, previous_event, label="events")
        if timestamp_group and event.ts_recv_ns != timestamp_group[0].ts_recv_ns:
            process_timestamp_group(timestamp_group)
            timestamp_group = []
        timestamp_group.append(event)
        source_event_count += 1
        valid_event_count += int(event.valid)
        previous_event = event
    if timestamp_group:
        process_timestamp_group(timestamp_group)

    if terminal_event is not None:
        terminal_event = _validate_next_event(
            terminal_event,
            previous_event,
            label="terminal_event",
        )
        fill_pending_stops(terminal_event)

    take_profit_thresholds = tuple(
        TakeProfitThresholdResult(
            distance_ticks=distance,
            target_price_ticks=(
                entry_price + distance
                if resolved_direction is Direction.LONG
                else entry_price - distance
            ),
            trade_through_price_ticks=(
                entry_price + distance + TAKE_PROFIT_TRADE_THROUGH_TICKS
                if resolved_direction is Direction.LONG
                else entry_price - distance - TAKE_PROFIT_TRADE_THROUGH_TICKS
            ),
            fill_event=take_profit_hits[index],
        )
        for index, distance in enumerate(take_profits)
    )
    stop_thresholds = tuple(
        StopThresholdResult(
            distance_ticks=distance,
            trigger_price_ticks=(
                entry_price - distance
                if resolved_direction is Direction.LONG
                else entry_price + distance
            ),
            trigger_event=stop_triggers[index],
            fill_event=stop_fill_events[index],
            fill_price_ticks=stop_fill_prices[index],
        )
        for index, distance in enumerate(stops)
    )
    threshold_replay = ThresholdReplay(
        direction=resolved_direction,
        source_event_count=source_event_count,
        valid_event_count=valid_event_count,
        source_path_passes=1,
        take_profits=take_profit_thresholds,
        stops=stop_thresholds,
    )

    terminal_reference = (
        _event_reference(terminal_event, resolved_direction)
        if terminal_event is not None and terminal_event.valid
        else None
    )
    cells: list[BarrierCellResult] = []
    for take_profit in take_profit_thresholds:
        for stop in stop_thresholds:
            buying_price, selling_price, target_price, loss_trigger_price = _planned_prices(
                resolved_direction,
                entry_price,
                take_profit.distance_ticks,
                stop.distance_ticks,
            )
            take_profit_event = take_profit.fill_event
            loss_trigger_event = stop.trigger_event
            stop_wins = loss_trigger_event is not None and _same_or_earlier_timestamp(
                loss_trigger_event,
                take_profit_event,
            )

            outcome: BarrierOutcome
            exit_fill_price: int | None
            take_profit_fill_price: int | None = None
            loss_fill_price: int | None = None
            terminal_fill_price: int | None = None
            trigger_event: EventReference | None = None
            fill_event: EventReference | None = None
            recorded_take_profit_event: EventReference | None = None
            recorded_loss_trigger: EventReference | None = None
            recorded_loss_fill: EventReference | None = None
            censor_reason: CensorReason | None = None

            if stop_wins:
                trigger_event = loss_trigger_event
                recorded_loss_trigger = loss_trigger_event
                if stop.fill_event is not None:
                    outcome = BarrierOutcome.STOP_FIRST
                    fill_event = stop.fill_event
                    recorded_loss_fill = stop.fill_event
                    exit_fill_price = stop.fill_price_ticks
                    loss_fill_price = stop.fill_price_ticks
                elif terminal_reference is not None:
                    outcome = BarrierOutcome.TERMINAL_EXIT
                    fill_event = terminal_reference
                    exit_fill_price = terminal_reference.executable_price_ticks
                    terminal_fill_price = exit_fill_price
                else:
                    outcome = BarrierOutcome.CENSORED
                    exit_fill_price = None
                    censor_reason = CensorReason.STOP_TRIGGERED_BUT_UNFILLED
            elif take_profit_event is not None:
                outcome = BarrierOutcome.TP_FIRST
                trigger_event = take_profit_event
                fill_event = take_profit_event
                recorded_take_profit_event = take_profit_event
                exit_fill_price = target_price
                take_profit_fill_price = target_price
            elif terminal_reference is not None:
                outcome = BarrierOutcome.TERMINAL_EXIT
                fill_event = terminal_reference
                exit_fill_price = terminal_reference.executable_price_ticks
                terminal_fill_price = exit_fill_price
            else:
                outcome = BarrierOutcome.CENSORED
                exit_fill_price = None
                censor_reason = (
                    CensorReason.INVALID_TERMINAL_QUOTE
                    if terminal_event is not None
                    else CensorReason.OBSERVATION_WINDOW_ENDED
                )

            cells.append(
                BarrierCellResult(
                    cell_id=(f"tp{take_profit.distance_ticks}_sl{stop.distance_ticks}"),
                    direction=resolved_direction,
                    take_profit_ticks=take_profit.distance_ticks,
                    stop_loss_ticks=stop.distance_ticks,
                    entry_fill_price_ticks=entry_price,
                    buying_price_ticks=buying_price,
                    selling_price_ticks=selling_price,
                    take_profit_target_price_ticks=target_price,
                    loss_trigger_price_ticks=loss_trigger_price,
                    outcome=outcome,
                    exit_fill_price_ticks=exit_fill_price,
                    take_profit_fill_price_ticks=take_profit_fill_price,
                    loss_fill_price_ticks=loss_fill_price,
                    terminal_fill_price_ticks=terminal_fill_price,
                    trigger_event_index=(
                        trigger_event.event_index if trigger_event is not None else None
                    ),
                    trigger_ts_recv_ns=(
                        trigger_event.ts_recv_ns if trigger_event is not None else None
                    ),
                    fill_event_index=(fill_event.event_index if fill_event is not None else None),
                    fill_ts_recv_ns=(fill_event.ts_recv_ns if fill_event is not None else None),
                    take_profit_fill_event_index=(
                        recorded_take_profit_event.event_index
                        if recorded_take_profit_event is not None
                        else None
                    ),
                    take_profit_fill_ts_recv_ns=(
                        recorded_take_profit_event.ts_recv_ns
                        if recorded_take_profit_event is not None
                        else None
                    ),
                    loss_trigger_event_index=(
                        recorded_loss_trigger.event_index
                        if recorded_loss_trigger is not None
                        else None
                    ),
                    loss_trigger_ts_recv_ns=(
                        recorded_loss_trigger.ts_recv_ns
                        if recorded_loss_trigger is not None
                        else None
                    ),
                    loss_fill_event_index=(
                        recorded_loss_fill.event_index if recorded_loss_fill is not None else None
                    ),
                    loss_fill_ts_recv_ns=(
                        recorded_loss_fill.ts_recv_ns if recorded_loss_fill is not None else None
                    ),
                    terminal_event_index=(
                        terminal_event.event_index if terminal_event is not None else None
                    ),
                    terminal_ts_recv_ns=(
                        terminal_event.ts_recv_ns if terminal_event is not None else None
                    ),
                    terminal_event_valid=(
                        terminal_event.valid if terminal_event is not None else None
                    ),
                    censor_reason=censor_reason,
                )
            )

    cell_ids = {cell.cell_id for cell in cells}
    if len(cells) != EXPECTED_CELL_COUNT or len(cell_ids) != EXPECTED_CELL_COUNT:
        raise BarrierReplayError("complete surface must contain 484 unique cell identities")

    return BarrierSurface(
        direction=resolved_direction,
        entry_fill_price_ticks=entry_price,
        take_profit_grid=take_profits,
        stop_loss_grid=stops,
        cells=tuple(cells),
        thresholds=threshold_replay,
    )
