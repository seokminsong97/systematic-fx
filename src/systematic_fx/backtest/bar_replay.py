"""Conservative first-hit replay over canonical one-second trade bars.

This module deliberately knows nothing about MBP events or order-book state.  A
caller supplies one immutable, single-contract segment of trade OHLCV bars and
the index of the first one-second bar in the signal timeframe's next bucket.
The complete 22 by 22 barrier surface is then resolved from one-second highs
and lows.  If take-profit and stop are both observable in the same second the
stop wins, because their intrasecond order is unknowable from bar data.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Final, Protocol

import numpy as np

from systematic_fx.backtest.barriers import BARRIER_TICKS, Direction
from systematic_fx.backtest.economics import CostScenario

ONE_SECOND_NS: Final = 1_000_000_000


class BarReplayError(ValueError):
    """A bar path cannot support deterministic conservative replay."""


class TradeBarLike(Protocol):
    """Neutral fields required from the canonical trade-bar layer."""

    timeframe_seconds: int
    segment_id: int
    contract: str
    start_ns: int
    end_ns: int
    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int


@dataclass(frozen=True, slots=True)
class BarExecutionScenario:
    """Frozen bar-proxy execution and cost assumptions for one stress level."""

    scenario_id: str
    entry_adverse_ticks: int
    take_profit_trade_through_ticks: int
    stop_total_minimum_adverse_ticks: int
    terminal_exit_adverse_ticks: int
    variable_debit_ticks: int
    fixed_pool_multiplier: Decimal

    def __post_init__(self) -> None:
        if self.scenario_id not in {"BASELINE", "MODERATE_COMBINED", "SEVERE_DIAGNOSTIC"}:
            raise BarReplayError("unknown bar execution scenario")
        integer_fields = (
            "entry_adverse_ticks",
            "take_profit_trade_through_ticks",
            "stop_total_minimum_adverse_ticks",
            "terminal_exit_adverse_ticks",
            "variable_debit_ticks",
        )
        for field_name in integer_fields:
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise BarReplayError(f"{field_name} must be a non-negative integer")
        if self.take_profit_trade_through_ticks <= 0:
            raise BarReplayError("take_profit_trade_through_ticks must be positive")
        if self.stop_total_minimum_adverse_ticks <= 0:
            raise BarReplayError("stop_total_minimum_adverse_ticks must be positive")
        if self.variable_debit_ticks <= 0:
            raise BarReplayError("variable_debit_ticks must be positive")
        if (
            not isinstance(self.fixed_pool_multiplier, Decimal)
            or not self.fixed_pool_multiplier.is_finite()
            or self.fixed_pool_multiplier <= 0
        ):
            raise BarReplayError("fixed_pool_multiplier must be a positive finite Decimal")

    @property
    def allocated_fixed_cost_ticks(self) -> int:
        """Conservative per-round-trip share of the monthly fixed-cost pool."""

        return CostScenario(
            self.scenario_id,
            self.variable_debit_ticks,
            self.fixed_pool_multiplier,
        ).allocated_fixed_ticks


BAR_EXECUTION_SCENARIOS: Final = {
    "BASELINE": BarExecutionScenario(
        scenario_id="BASELINE",
        entry_adverse_ticks=1,
        take_profit_trade_through_ticks=1,
        stop_total_minimum_adverse_ticks=2,
        terminal_exit_adverse_ticks=1,
        variable_debit_ticks=4,
        fixed_pool_multiplier=Decimal("1.00"),
    ),
    "MODERATE_COMBINED": BarExecutionScenario(
        scenario_id="MODERATE_COMBINED",
        entry_adverse_ticks=2,
        take_profit_trade_through_ticks=1,
        stop_total_minimum_adverse_ticks=4,
        terminal_exit_adverse_ticks=2,
        variable_debit_ticks=5,
        fixed_pool_multiplier=Decimal("1.25"),
    ),
    "SEVERE_DIAGNOSTIC": BarExecutionScenario(
        scenario_id="SEVERE_DIAGNOSTIC",
        entry_adverse_ticks=3,
        take_profit_trade_through_ticks=2,
        stop_total_minimum_adverse_ticks=6,
        terminal_exit_adverse_ticks=3,
        variable_debit_ticks=6,
        fixed_pool_multiplier=Decimal("1.50"),
    ),
}


@dataclass(frozen=True, slots=True)
class BarThresholdHit:
    """The first one-second bar satisfying one threshold."""

    distance_ticks: int
    trigger_price_ticks: int
    path_index: int | None
    bar_start_ns: int | None


@dataclass(frozen=True, slots=True)
class BarCellOutcome:
    """One next-open entry's result for one TP/SL pair."""

    direction: Direction
    take_profit_ticks: int
    stop_loss_ticks: int
    entry_path_index: int
    entry_fill_price_ticks: int
    buying_price_ticks: int
    selling_price_ticks: int
    take_profit_target_price_ticks: int
    loss_trigger_price_ticks: int
    outcome: str
    exit_path_index: int
    exit_fill_price_ticks: int
    gross_pnl_ticks: int
    variable_debit_ticks: int
    allocated_fixed_cost_ticks: int
    fully_loaded_net_pnl_ticks: int
    take_profit_hit_index: int | None
    stop_hit_index: int | None
    same_second_stop_first: bool


@dataclass(frozen=True, slots=True)
class BarSignalSurface:
    """The complete frozen barrier grid for one filled next-open signal."""

    scenario_id: str
    direction: Direction
    segment_id: int
    contract: str
    entry_path_index: int
    entry_fill_price_ticks: int
    terminal_path_index: int
    fixed_pool_multiplier: Decimal
    take_profit_hits: tuple[BarThresholdHit, ...]
    stop_hits: tuple[BarThresholdHit, ...]
    cells: tuple[BarCellOutcome, ...]

    def cell(self, take_profit_ticks: int, stop_loss_ticks: int) -> BarCellOutcome:
        try:
            tp_index = BARRIER_TICKS.index(take_profit_ticks)
            sl_index = BARRIER_TICKS.index(stop_loss_ticks)
        except ValueError as error:
            raise KeyError(
                f"unknown barrier cell tp{take_profit_ticks}_sl{stop_loss_ticks}"
            ) from error
        return self.cells[tp_index * len(BARRIER_TICKS) + sl_index]


def _integer(value: object, *, label: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise BarReplayError(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        raise BarReplayError(f"{label} must be at least {minimum}")
    return value


def _direction(value: Direction | str) -> Direction:
    try:
        return Direction(value)
    except (TypeError, ValueError) as error:
        raise BarReplayError("direction must be LONG or SHORT") from error


class BarPathIndex:
    """Range-max/min index for one immutable selected-contract segment."""

    def __init__(self, bars: Sequence[TradeBarLike]) -> None:
        if not isinstance(bars, Sequence) or not bars:
            raise BarReplayError("bar path must be a non-empty sequence")
        first = bars[0]
        segment_id = _integer(first.segment_id, label="segment_id", minimum=1)
        contract = first.contract
        if not isinstance(contract, str) or not contract:
            raise BarReplayError("contract must be a non-empty string")

        highs: list[int] = []
        lows: list[int] = []
        previous_start: int | None = None
        for index, bar in enumerate(bars):
            if bar.timeframe_seconds != 1:
                raise BarReplayError("first-hit path must contain only one-second bars")
            if bar.segment_id != segment_id or bar.contract != contract:
                raise BarReplayError("bar path crosses a contract or quality segment")
            start_ns = _integer(bar.start_ns, label=f"bars[{index}].start_ns", minimum=0)
            end_ns = _integer(bar.end_ns, label=f"bars[{index}].end_ns", minimum=1)
            if end_ns - start_ns != ONE_SECOND_NS:
                raise BarReplayError("one-second bar boundary width drift")
            if previous_start is not None and start_ns <= previous_start:
                raise BarReplayError("one-second bars must be strictly time ordered")
            previous_start = start_ns
            open_ticks = _integer(bar.open_ticks, label="open_ticks", minimum=1)
            high_ticks = _integer(bar.high_ticks, label="high_ticks", minimum=1)
            low_ticks = _integer(bar.low_ticks, label="low_ticks", minimum=1)
            close_ticks = _integer(bar.close_ticks, label="close_ticks", minimum=1)
            if high_ticks < max(open_ticks, close_ticks) or low_ticks > min(
                open_ticks, close_ticks
            ):
                raise BarReplayError("bar OHLC ordering is invalid")
            if low_ticks > high_ticks:
                raise BarReplayError("bar low exceeds high")
            highs.append(high_ticks)
            lows.append(low_ticks)

        self.bars = tuple(bars)
        self.segment_id = segment_id
        self.contract = contract
        size = 1
        while size < len(bars):
            size <<= 1
        self._size = size
        self._max_tree = np.full(size * 2, np.iinfo(np.int64).min, dtype=np.int64)
        self._min_tree = np.full(size * 2, np.iinfo(np.int64).max, dtype=np.int64)
        self._max_tree[size : size + len(highs)] = np.asarray(highs, dtype=np.int64)
        self._min_tree[size : size + len(lows)] = np.asarray(lows, dtype=np.int64)
        for node in range(size - 1, 0, -1):
            self._max_tree[node] = max(self._max_tree[node * 2], self._max_tree[node * 2 + 1])
            self._min_tree[node] = min(self._min_tree[node * 2], self._min_tree[node * 2 + 1])

    def _first(
        self,
        *,
        start: int,
        end_exclusive: int,
        threshold: int,
        high: bool,
    ) -> int | None:
        _integer(start, label="start", minimum=0)
        _integer(end_exclusive, label="end_exclusive", minimum=1)
        _integer(threshold, label="threshold")
        if start >= end_exclusive or end_exclusive > len(self.bars):
            raise BarReplayError("first-hit query range is invalid")
        tree = self._max_tree if high else self._min_tree

        def qualifies(node: int) -> bool:
            value = int(tree[node])
            return value >= threshold if high else value <= threshold

        def search(node: int, left: int, right: int) -> int | None:
            if right <= start or end_exclusive <= left or not qualifies(node):
                return None
            if right - left == 1:
                return left if left < len(self.bars) else None
            midpoint = (left + right) // 2
            found = search(node * 2, left, midpoint)
            return found if found is not None else search(node * 2 + 1, midpoint, right)

        return search(1, 0, self._size)

    def first_high_at_or_above(self, start: int, end_exclusive: int, threshold: int) -> int | None:
        return self._first(
            start=start,
            end_exclusive=end_exclusive,
            threshold=threshold,
            high=True,
        )

    def first_low_at_or_below(self, start: int, end_exclusive: int, threshold: int) -> int | None:
        return self._first(
            start=start,
            end_exclusive=end_exclusive,
            threshold=threshold,
            high=False,
        )


def replay_bar_signal(
    path: BarPathIndex,
    *,
    entry_path_index: int,
    direction: Direction | str,
    scenario: BarExecutionScenario,
) -> BarSignalSurface:
    """Resolve the frozen 484-cell surface for one next-bucket entry."""

    if not isinstance(path, BarPathIndex):
        raise BarReplayError("path must be a BarPathIndex")
    entry_index = _integer(entry_path_index, label="entry_path_index", minimum=0)
    if entry_index >= len(path.bars):
        raise BarReplayError("entry_path_index is outside the segment")
    if not isinstance(scenario, BarExecutionScenario):
        raise BarReplayError("scenario must be a BarExecutionScenario")
    side = _direction(direction)
    entry_bar = path.bars[entry_index]
    sign = 1 if side is Direction.LONG else -1
    entry_fill = entry_bar.open_ticks + sign * scenario.entry_adverse_ticks
    terminal_index = len(path.bars) - 1
    terminal_bar = path.bars[terminal_index]

    tp_hits: list[BarThresholdHit] = []
    stop_hits: list[BarThresholdHit] = []
    for distance in BARRIER_TICKS:
        target = entry_fill + sign * distance
        stop = entry_fill - sign * distance
        if side is Direction.LONG:
            tp_index = path.first_high_at_or_above(
                entry_index,
                len(path.bars),
                target + scenario.take_profit_trade_through_ticks,
            )
            stop_index = path.first_low_at_or_below(entry_index, len(path.bars), stop)
        else:
            tp_index = path.first_low_at_or_below(
                entry_index,
                len(path.bars),
                target - scenario.take_profit_trade_through_ticks,
            )
            stop_index = path.first_high_at_or_above(entry_index, len(path.bars), stop)
        tp_hits.append(
            BarThresholdHit(
                distance_ticks=distance,
                trigger_price_ticks=target,
                path_index=tp_index,
                bar_start_ns=None if tp_index is None else path.bars[tp_index].start_ns,
            )
        )
        stop_hits.append(
            BarThresholdHit(
                distance_ticks=distance,
                trigger_price_ticks=stop,
                path_index=stop_index,
                bar_start_ns=None if stop_index is None else path.bars[stop_index].start_ns,
            )
        )

    cells: list[BarCellOutcome] = []
    for tp_hit in tp_hits:
        for stop_hit in stop_hits:
            tp_index = tp_hit.path_index
            stop_index = stop_hit.path_index
            same_second = tp_index is not None and tp_index == stop_index
            if stop_index is not None and (tp_index is None or stop_index <= tp_index):
                outcome = "STOP_FIRST"
                exit_index = stop_index
                observed_open = path.bars[exit_index].open_ticks
                if side is Direction.LONG:
                    exit_fill = min(
                        observed_open,
                        stop_hit.trigger_price_ticks - scenario.stop_total_minimum_adverse_ticks,
                    )
                else:
                    exit_fill = max(
                        observed_open,
                        stop_hit.trigger_price_ticks + scenario.stop_total_minimum_adverse_ticks,
                    )
            elif tp_index is not None:
                outcome = "TP_FIRST"
                exit_index = tp_index
                exit_fill = tp_hit.trigger_price_ticks
            else:
                outcome = "TERMINAL_EXIT"
                exit_index = terminal_index
                exit_fill = (
                    terminal_bar.close_ticks - scenario.terminal_exit_adverse_ticks
                    if side is Direction.LONG
                    else terminal_bar.close_ticks + scenario.terminal_exit_adverse_ticks
                )
            gross = exit_fill - entry_fill if side is Direction.LONG else entry_fill - exit_fill
            buying_price = entry_fill if side is Direction.LONG else exit_fill
            selling_price = exit_fill if side is Direction.LONG else entry_fill
            cells.append(
                BarCellOutcome(
                    direction=side,
                    take_profit_ticks=tp_hit.distance_ticks,
                    stop_loss_ticks=stop_hit.distance_ticks,
                    entry_path_index=entry_index,
                    entry_fill_price_ticks=entry_fill,
                    buying_price_ticks=buying_price,
                    selling_price_ticks=selling_price,
                    take_profit_target_price_ticks=tp_hit.trigger_price_ticks,
                    loss_trigger_price_ticks=stop_hit.trigger_price_ticks,
                    outcome=outcome,
                    exit_path_index=exit_index,
                    exit_fill_price_ticks=exit_fill,
                    gross_pnl_ticks=gross,
                    variable_debit_ticks=scenario.variable_debit_ticks,
                    allocated_fixed_cost_ticks=scenario.allocated_fixed_cost_ticks,
                    fully_loaded_net_pnl_ticks=(
                        gross - scenario.variable_debit_ticks - scenario.allocated_fixed_cost_ticks
                    ),
                    take_profit_hit_index=tp_index,
                    stop_hit_index=stop_index,
                    same_second_stop_first=same_second,
                )
            )

    return BarSignalSurface(
        scenario_id=scenario.scenario_id,
        direction=side,
        segment_id=path.segment_id,
        contract=path.contract,
        entry_path_index=entry_index,
        entry_fill_price_ticks=entry_fill,
        terminal_path_index=terminal_index,
        fixed_pool_multiplier=scenario.fixed_pool_multiplier,
        take_profit_hits=tuple(tp_hits),
        stop_hits=tuple(stop_hits),
        cells=tuple(cells),
    )
