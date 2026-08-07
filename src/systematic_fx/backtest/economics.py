"""Fully loaded Phase 1A economics and adjacent-surface selection.

Executable fill prices already contain spread, latency, depth, and stop-gap
effects.  This module therefore debits only the separately frozen variable and
fixed costs; embedded fill effects are never charged twice.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal
from enum import StrEnum
from statistics import median
from typing import Final

from systematic_fx.backtest.barriers import (
    BARRIER_TICKS,
    EXPECTED_CELL_COUNT,
    BarrierCellResult,
    BarrierOutcome,
    BarrierSurface,
    Direction,
)

TICK_VALUE_USD: Final = Decimal("6.25")
EXPECTED_MONTHLY_ROUND_TRIPS: Final = 20
BASE_MONTHLY_FIXED_POOL_USD: Final = Decimal("500.00")


class EconomicsError(ValueError):
    """A surface is incomplete or its accounting inputs are inconsistent."""


class EntryStatus(StrEnum):
    """Entry/occupancy state retained for every candidate signal."""

    ENTRY_FILLED = "ENTRY_FILLED"
    ENTRY_NOT_FILLED = "ENTRY_NOT_FILLED"
    SKIPPED_OCCUPIED = "SKIPPED_OCCUPIED"


@dataclass(frozen=True, slots=True)
class CostScenario:
    """One exact cost scenario paired with an independently replayed execution path."""

    scenario_id: str
    variable_debit_ticks: int
    fixed_pool_multiplier: Decimal

    def __post_init__(self) -> None:
        if self.scenario_id not in {"BASELINE", "MODERATE_COMBINED", "SEVERE_DIAGNOSTIC"}:
            raise EconomicsError("unknown Phase 1A cost scenario")
        if isinstance(self.variable_debit_ticks, bool) or self.variable_debit_ticks <= 0:
            raise EconomicsError("variable_debit_ticks must be a positive integer")
        if not isinstance(self.fixed_pool_multiplier, Decimal):
            raise EconomicsError("fixed_pool_multiplier must be Decimal")
        if not self.fixed_pool_multiplier.is_finite() or self.fixed_pool_multiplier <= 0:
            raise EconomicsError("fixed_pool_multiplier must be positive and finite")

    @property
    def monthly_fixed_pool_usd(self) -> Decimal:
        return BASE_MONTHLY_FIXED_POOL_USD * self.fixed_pool_multiplier

    @property
    def allocated_fixed_ticks(self) -> int:
        raw = self.monthly_fixed_pool_usd / Decimal(EXPECTED_MONTHLY_ROUND_TRIPS) / TICK_VALUE_USD
        return int(raw.to_integral_value(rounding=ROUND_CEILING))


COST_SCENARIOS: Final = {
    "BASELINE": CostScenario("BASELINE", 4, Decimal("1.00")),
    "MODERATE_COMBINED": CostScenario("MODERATE_COMBINED", 5, Decimal("1.25")),
    "SEVERE_DIAGNOSTIC": CostScenario("SEVERE_DIAGNOSTIC", 6, Decimal("1.50")),
}


@dataclass(frozen=True, slots=True)
class SignalSurface:
    """One registered signal and its entry result for a fixed execution scenario."""

    signal_id: str
    signal_ts_recv_ns: int
    utc_month: str
    entry_status: EntryStatus
    surface: BarrierSurface | None = None
    no_fill_reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.signal_id, str) or not self.signal_id:
            raise EconomicsError("signal_id must be a non-empty string")
        if isinstance(self.signal_ts_recv_ns, bool) or not isinstance(self.signal_ts_recv_ns, int):
            raise EconomicsError("signal_ts_recv_ns must be an integer")
        if (
            len(self.utc_month) != 7
            or self.utc_month[4] != "-"
            or not self.utc_month[:4].isdigit()
            or not self.utc_month[5:].isdigit()
            or not 1 <= int(self.utc_month[5:]) <= 12
        ):
            raise EconomicsError("utc_month must be canonical YYYY-MM")
        if self.entry_status is EntryStatus.ENTRY_FILLED:
            if not isinstance(self.surface, BarrierSurface):
                raise EconomicsError("ENTRY_FILLED requires a BarrierSurface")
            if self.no_fill_reason is not None:
                raise EconomicsError("ENTRY_FILLED cannot have no_fill_reason")
        else:
            if self.surface is not None:
                raise EconomicsError("unfilled/skipped signals cannot have a surface")
            if not isinstance(self.no_fill_reason, str) or not self.no_fill_reason:
                raise EconomicsError("unfilled/skipped signals require a reason")


@dataclass(frozen=True, slots=True)
class CellEconomics:
    """Complete signal, outcome, cost, and PnL accounting for one grid cell."""

    scenario_id: str
    cell_id: str
    direction: Direction
    take_profit_ticks: int
    stop_loss_ticks: int
    signal_count: int
    entry_fill_count: int
    entry_not_filled_count: int
    skipped_occupied_count: int
    take_profit_first_count: int
    stop_first_count: int
    terminal_exit_count: int
    censored_count: int
    gross_pnl_ticks: int
    variable_cost_ticks: int
    allocated_fixed_cost_ticks: int
    fully_loaded_net_pnl_ticks: int
    fully_loaded_net_ev_ticks: Decimal | None
    fully_loaded_net_pnl_usd: Decimal
    calendar_month_net_pnl_usd: Decimal
    profit_factor: Decimal | None
    maximum_drawdown_usd: Decimal
    complete: bool

    @property
    def payload(self) -> dict[str, object]:
        return {
            "scenario_id": self.scenario_id,
            "cell_id": self.cell_id,
            "direction": self.direction,
            "take_profit_ticks": self.take_profit_ticks,
            "stop_loss_ticks": self.stop_loss_ticks,
            "signal_count": self.signal_count,
            "entry_fill_count": self.entry_fill_count,
            "entry_not_filled_count": self.entry_not_filled_count,
            "skipped_occupied_count": self.skipped_occupied_count,
            "take_profit_first_count": self.take_profit_first_count,
            "stop_first_count": self.stop_first_count,
            "terminal_exit_count": self.terminal_exit_count,
            "censored_count": self.censored_count,
            "gross_pnl_ticks": self.gross_pnl_ticks,
            "variable_cost_ticks": self.variable_cost_ticks,
            "allocated_fixed_cost_ticks": self.allocated_fixed_cost_ticks,
            "fully_loaded_net_pnl_ticks": self.fully_loaded_net_pnl_ticks,
            "fully_loaded_net_ev_ticks": _decimal_text(self.fully_loaded_net_ev_ticks),
            "fully_loaded_net_pnl_usd": _decimal_text(self.fully_loaded_net_pnl_usd),
            "calendar_month_net_pnl_usd": _decimal_text(self.calendar_month_net_pnl_usd),
            "profit_factor": _decimal_text(self.profit_factor),
            "maximum_drawdown_usd": _decimal_text(self.maximum_drawdown_usd),
            "complete": self.complete,
        }


@dataclass(frozen=True, slots=True)
class EconomicSurface:
    """All 484 costed cells for one direction and scenario."""

    scenario_id: str
    direction: Direction
    cells: tuple[CellEconomics, ...]

    def __post_init__(self) -> None:
        if len(self.cells) != EXPECTED_CELL_COUNT:
            raise EconomicsError("economic surface must contain exactly 484 cells")
        identities = {(cell.take_profit_ticks, cell.stop_loss_ticks) for cell in self.cells}
        expected = {(tp, sl) for tp in BARRIER_TICKS for sl in BARRIER_TICKS}
        if identities != expected:
            raise EconomicsError("economic surface has a missing or duplicate grid cell")
        if any(
            cell.scenario_id != self.scenario_id or cell.direction is not self.direction
            for cell in self.cells
        ):
            raise EconomicsError("economic surface cells disagree on scenario or direction")

    def cell(self, take_profit_ticks: int, stop_loss_ticks: int) -> CellEconomics:
        index = BARRIER_TICKS.index(take_profit_ticks) * len(BARRIER_TICKS)
        index += BARRIER_TICKS.index(stop_loss_ticks)
        return self.cells[index]


@dataclass(frozen=True, slots=True)
class ScreeningSelection:
    """Conservative adjacent-stability decision; never a backtest pass."""

    label: str
    selected_cell_id: str | None
    selected_take_profit_ticks: int | None
    selected_stop_loss_ticks: int | None
    positive_region_size: int
    rejection_reasons: tuple[str, ...]


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def _gross_ticks(cell: BarrierCellResult) -> int | None:
    if cell.exit_fill_price_ticks is None:
        return None
    if cell.direction is Direction.LONG:
        return cell.exit_fill_price_ticks - cell.entry_fill_price_ticks
    return cell.entry_fill_price_ticks - cell.exit_fill_price_ticks


def _maximum_drawdown_ticks(values: Iterable[int]) -> int:
    equity = 0
    peak = 0
    maximum = 0
    for value in values:
        equity += value
        peak = max(peak, equity)
        maximum = max(maximum, peak - equity)
    return maximum


def _profit_factor(values: Sequence[int]) -> Decimal | None:
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    if losses == 0:
        return None
    return Decimal(gains) / Decimal(losses)


def _validate_signals(signals: Sequence[SignalSurface]) -> tuple[Direction, tuple[str, ...]]:
    if not signals:
        raise EconomicsError("at least one registered signal is required")
    identifiers: set[str] = set()
    previous_ts: int | None = None
    direction: Direction | None = None
    months: set[str] = set()
    for signal in signals:
        if not isinstance(signal, SignalSurface):
            raise EconomicsError("signals must contain SignalSurface values")
        if signal.signal_id in identifiers:
            raise EconomicsError(f"duplicate signal_id: {signal.signal_id}")
        identifiers.add(signal.signal_id)
        if previous_ts is not None and signal.signal_ts_recv_ns < previous_ts:
            raise EconomicsError("signals must be ordered by non-decreasing receive time")
        previous_ts = signal.signal_ts_recv_ns
        months.add(signal.utc_month)
        if signal.surface is not None:
            if direction is None:
                direction = signal.surface.direction
            elif signal.surface.direction is not direction:
                raise EconomicsError("filled signals disagree on direction")
    if direction is None:
        raise EconomicsError("a surface direction cannot be inferred without a filled entry")
    return direction, tuple(sorted(months))


def evaluate_economic_surface(
    signals: Sequence[SignalSurface],
    *,
    scenario: CostScenario,
    observed_utc_months: Sequence[str],
) -> EconomicSurface:
    """Cost every grid cell while retaining no-fill, occupied, and censored signals."""

    if not isinstance(scenario, CostScenario):
        raise TypeError("scenario must be a CostScenario")
    direction, signal_months = _validate_signals(signals)
    months = tuple(observed_utc_months)
    if not months or len(months) != len(set(months)) or tuple(sorted(months)) != months:
        raise EconomicsError("observed_utc_months must be unique and strictly sorted")
    if not set(signal_months) <= set(months):
        raise EconomicsError("every signal month must be an observed month")

    first_surface = next(signal.surface for signal in signals if signal.surface is not None)
    assert first_surface is not None
    expected_grid = (first_surface.take_profit_grid, first_surface.stop_loss_grid)
    for signal in signals:
        if (
            signal.surface is not None
            and (
                signal.surface.take_profit_grid,
                signal.surface.stop_loss_grid,
            )
            != expected_grid
        ):
            raise EconomicsError("filled signal surfaces use different grid axes")

    cells: list[CellEconomics] = []
    for cell_index in range(EXPECTED_CELL_COUNT):
        closed_net_ticks: list[int] = []
        gross_pnl_ticks = 0
        tp_count = 0
        stop_count = 0
        terminal_count = 0
        censored_count = 0
        for signal in signals:
            if signal.surface is None:
                continue
            cell = signal.surface.cells[cell_index]
            if cell.outcome is BarrierOutcome.TP_FIRST:
                tp_count += 1
            elif cell.outcome is BarrierOutcome.STOP_FIRST:
                stop_count += 1
            elif cell.outcome is BarrierOutcome.TERMINAL_EXIT:
                terminal_count += 1
            else:
                censored_count += 1
            gross = _gross_ticks(cell)
            if gross is None:
                continue
            gross_pnl_ticks += gross
            closed_net_ticks.append(
                gross - scenario.variable_debit_ticks - scenario.allocated_fixed_ticks
            )

        filled_count = sum(signal.surface is not None for signal in signals)
        entry_not_filled = sum(
            signal.entry_status is EntryStatus.ENTRY_NOT_FILLED for signal in signals
        )
        skipped = sum(signal.entry_status is EntryStatus.SKIPPED_OCCUPIED for signal in signals)
        closed_count = len(closed_net_ticks)
        variable_cost_ticks = closed_count * scenario.variable_debit_ticks
        fixed_cost_ticks = closed_count * scenario.allocated_fixed_ticks
        net_pnl_ticks = sum(closed_net_ticks)
        net_ev = Decimal(net_pnl_ticks) / Decimal(closed_count) if closed_count else None
        calendar_gross_usd = Decimal(gross_pnl_ticks) * TICK_VALUE_USD
        calendar_variable_usd = Decimal(variable_cost_ticks) * TICK_VALUE_USD
        calendar_net_usd = (
            calendar_gross_usd
            - calendar_variable_usd
            - Decimal(len(months)) * scenario.monthly_fixed_pool_usd
        )
        template = first_surface.cells[cell_index]
        cells.append(
            CellEconomics(
                scenario_id=scenario.scenario_id,
                cell_id=template.cell_id,
                direction=direction,
                take_profit_ticks=template.take_profit_ticks,
                stop_loss_ticks=template.stop_loss_ticks,
                signal_count=len(signals),
                entry_fill_count=filled_count,
                entry_not_filled_count=entry_not_filled,
                skipped_occupied_count=skipped,
                take_profit_first_count=tp_count,
                stop_first_count=stop_count,
                terminal_exit_count=terminal_count,
                censored_count=censored_count,
                gross_pnl_ticks=gross_pnl_ticks,
                variable_cost_ticks=variable_cost_ticks,
                allocated_fixed_cost_ticks=fixed_cost_ticks,
                fully_loaded_net_pnl_ticks=net_pnl_ticks,
                fully_loaded_net_ev_ticks=net_ev,
                fully_loaded_net_pnl_usd=Decimal(net_pnl_ticks) * TICK_VALUE_USD,
                calendar_month_net_pnl_usd=calendar_net_usd,
                profit_factor=_profit_factor(closed_net_ticks),
                maximum_drawdown_usd=(
                    Decimal(_maximum_drawdown_ticks(closed_net_ticks)) * TICK_VALUE_USD
                ),
                complete=(filled_count == closed_count and censored_count == 0),
            )
        )
    return EconomicSurface(scenario.scenario_id, direction, tuple(cells))


def _neighbors(index: tuple[int, int], *, include_self: bool) -> tuple[tuple[int, int], ...]:
    row, column = index
    result: list[tuple[int, int]] = []
    for row_offset in (-1, 0, 1):
        for column_offset in (-1, 0, 1):
            if not include_self and row_offset == column_offset == 0:
                continue
            candidate = row + row_offset, column + column_offset
            if 0 <= candidate[0] < len(BARRIER_TICKS) and 0 <= candidate[1] < len(BARRIER_TICKS):
                result.append(candidate)
    return tuple(result)


def _cell_mapping(surface: EconomicSurface) -> dict[tuple[int, int], CellEconomics]:
    return {
        (
            BARRIER_TICKS.index(cell.take_profit_ticks),
            BARRIER_TICKS.index(cell.stop_loss_ticks),
        ): cell
        for cell in surface.cells
    }


def _positive(cell: CellEconomics) -> bool:
    return (
        cell.complete
        and cell.fully_loaded_net_ev_ticks is not None
        and cell.fully_loaded_net_ev_ticks > 0
    )


def _connected_components(values: set[tuple[int, int]]) -> list[set[tuple[int, int]]]:
    remaining = set(values)
    components: list[set[tuple[int, int]]] = []
    while remaining:
        seed = min(remaining)
        component = {seed}
        queue = deque([seed])
        remaining.remove(seed)
        while queue:
            current = queue.popleft()
            for neighbor in _neighbors(current, include_self=False):
                if neighbor in remaining:
                    remaining.remove(neighbor)
                    component.add(neighbor)
                    queue.append(neighbor)
        components.append(component)
    return components


def _stable_at(
    index: tuple[int, int],
    mapping: Mapping[tuple[int, int], CellEconomics],
) -> bool:
    row, column = index
    if row in {0, len(BARRIER_TICKS) - 1} or column in {0, len(BARRIER_TICKS) - 1}:
        return False
    window = [mapping[item] for item in _neighbors(index, include_self=True)]
    selected = mapping[index]
    selected_ev = selected.fully_loaded_net_ev_ticks
    if selected_ev is None or selected_ev <= 0 or sum(_positive(cell) for cell in window) < 7:
        return False
    all_ev = [cell.fully_loaded_net_ev_ticks or Decimal(0) for cell in window]
    positive_ev = [cell.fully_loaded_net_ev_ticks for cell in window if _positive(cell)]
    if not positive_ev:
        return False
    window_median = median(all_ev)
    positive_median = median(positive_ev)
    return window_median / selected_ev >= Decimal(
        "0.50"
    ) and selected_ev / positive_median <= Decimal("2.00")


def _region_medoid(
    region: set[tuple[int, int]],
    eligible: set[tuple[int, int]],
) -> tuple[int, int] | None:
    candidates = region & eligible
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda candidate: (
            sum(
                max(abs(candidate[0] - other[0]), abs(candidate[1] - other[1])) for other in region
            ),
            candidate[1],
            candidate[0],
        ),
    )


def select_stable_screening_cell(
    baseline: EconomicSurface,
    moderate: EconomicSurface,
) -> ScreeningSelection:
    """Apply the frozen 3x3 stability gate and return at most SCREENING_SURVIVOR."""

    if baseline.scenario_id != "BASELINE" or moderate.scenario_id != "MODERATE_COMBINED":
        raise EconomicsError("stability requires BASELINE and MODERATE_COMBINED surfaces")
    if baseline.direction is not moderate.direction:
        raise EconomicsError("stability surfaces must use the same direction")
    baseline_cells = _cell_mapping(baseline)
    moderate_cells = _cell_mapping(moderate)
    reasons: list[str] = []
    if any(not cell.complete for cell in baseline.cells + moderate.cells):
        reasons.append("INCOMPLETE_OR_CENSORED_SURFACE")

    jointly_positive = {
        index
        for index in baseline_cells
        if _positive(baseline_cells[index]) and _positive(moderate_cells[index])
    }
    components = _connected_components(jointly_positive)
    if len(components) != 1:
        reasons.append("JOINT_POSITIVE_REGION_NOT_SINGLE_CONTIGUOUS_COMPONENT")
    stable = {
        index
        for index in jointly_positive
        if _stable_at(index, baseline_cells) and _stable_at(index, moderate_cells)
    }
    if not stable:
        reasons.append("NO_INTERIOR_7_OF_9_STABLE_CELL")

    region = components[0] if len(components) == 1 else set()
    selected_index = _region_medoid(region, stable) if region else None
    if selected_index is None:
        reasons.append("NO_STABLE_REGION_MEDOID")
    elif moderate_cells[selected_index].calendar_month_net_pnl_usd <= 0:
        reasons.append("MODERATE_FULLY_LOADED_CALENDAR_PNL_NOT_POSITIVE")
    else:
        profit_factor = moderate_cells[selected_index].profit_factor
        if profit_factor is not None and profit_factor < Decimal("1.05"):
            reasons.append("MODERATE_PROFIT_FACTOR_BELOW_1_05")

    if reasons:
        return ScreeningSelection(
            label="SCREENING_REJECT",
            selected_cell_id=None,
            selected_take_profit_ticks=None,
            selected_stop_loss_ticks=None,
            positive_region_size=len(region),
            rejection_reasons=tuple(dict.fromkeys(reasons)),
        )
    assert selected_index is not None
    selected = baseline_cells[selected_index]
    return ScreeningSelection(
        label="SCREENING_SURVIVOR",
        selected_cell_id=selected.cell_id,
        selected_take_profit_ticks=selected.take_profit_ticks,
        selected_stop_loss_ticks=selected.stop_loss_ticks,
        positive_region_size=len(region),
        rejection_reasons=(),
    )
