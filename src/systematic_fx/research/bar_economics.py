"""Streaming conservative economics for one bar-pattern candidate."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import Decimal

from systematic_fx.backtest.bar_replay import BAR_EXECUTION_SCENARIOS, BarSignalSurface
from systematic_fx.backtest.barriers import BARRIER_TICKS, Direction
from systematic_fx.backtest.economics import BASE_MONTHLY_FIXED_POOL_USD, TICK_VALUE_USD


class BarEconomicsError(ValueError):
    """Candidate signal results cannot form a complete economic surface."""


@dataclass(frozen=True, slots=True)
class CandidateSignalReplay:
    """One registered signal with either a filled next-open surface or no fill."""

    signal_id: str
    signal_ts_ns: int
    block_key: str
    utc_month: str
    surface: BarSignalSurface | None
    no_fill_reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.signal_id, str) or not self.signal_id:
            raise BarEconomicsError("signal_id must be non-empty")
        if isinstance(self.signal_ts_ns, bool) or not isinstance(self.signal_ts_ns, int):
            raise BarEconomicsError("signal_ts_ns must be an integer")
        if not isinstance(self.block_key, str) or not self.block_key:
            raise BarEconomicsError("block_key must be non-empty")
        if (
            len(self.utc_month) != 7
            or self.utc_month[4] != "-"
            or not self.utc_month[:4].isdigit()
            or not self.utc_month[5:].isdigit()
            or not 1 <= int(self.utc_month[5:]) <= 12
        ):
            raise BarEconomicsError("utc_month must use YYYY-MM")
        if self.surface is None:
            if not isinstance(self.no_fill_reason, str) or not self.no_fill_reason:
                raise BarEconomicsError("an unfilled signal requires a no-fill reason")
        elif self.no_fill_reason is not None:
            raise BarEconomicsError("a filled signal cannot have a no-fill reason")


@dataclass(frozen=True, slots=True)
class BarBlockEconomics:
    """Chronological economics for one frozen discovery reporting block."""

    block_key: str
    entry_fill_count: int
    fully_loaded_net_pnl_ticks: int
    fully_loaded_net_ev_ticks: Decimal | None
    gross_profit_ticks: int
    maximum_drawdown_ticks: int

    def as_dict(self) -> dict[str, object]:
        return {
            "block_key": self.block_key,
            "entry_fill_count": self.entry_fill_count,
            "fully_loaded_net_ev_ticks": _decimal_text(self.fully_loaded_net_ev_ticks),
            "fully_loaded_net_pnl_ticks": self.fully_loaded_net_pnl_ticks,
            "gross_profit_ticks": self.gross_profit_ticks,
            "maximum_drawdown_ticks": self.maximum_drawdown_ticks,
        }


@dataclass(frozen=True, slots=True)
class BarCellEconomics:
    """One direction/scenario/TP/SL cell including occupancy and all costs."""

    scenario_id: str
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
    gross_pnl_ticks: int
    variable_cost_ticks: int
    allocated_fixed_cost_ticks: int
    fully_loaded_net_pnl_ticks: int
    fully_loaded_net_ev_ticks: Decimal | None
    calendar_month_net_pnl_usd: Decimal
    profit_factor: Decimal | None
    maximum_drawdown_ticks: int
    blocks: tuple[BarBlockEconomics, ...]

    @property
    def cell_id(self) -> str:
        return f"tp{self.take_profit_ticks}_sl{self.stop_loss_ticks}"

    def as_dict(self) -> dict[str, object]:
        return {
            "allocated_fixed_cost_ticks": self.allocated_fixed_cost_ticks,
            "blocks": [item.as_dict() for item in self.blocks],
            "calendar_month_net_pnl_usd": _decimal_text(self.calendar_month_net_pnl_usd),
            "cell_id": self.cell_id,
            "direction": self.direction.value,
            "entry_fill_count": self.entry_fill_count,
            "entry_not_filled_count": self.entry_not_filled_count,
            "fully_loaded_net_ev_ticks": _decimal_text(self.fully_loaded_net_ev_ticks),
            "fully_loaded_net_pnl_ticks": self.fully_loaded_net_pnl_ticks,
            "gross_pnl_ticks": self.gross_pnl_ticks,
            "maximum_drawdown_ticks": self.maximum_drawdown_ticks,
            "profit_factor": _decimal_text(self.profit_factor),
            "scenario_id": self.scenario_id,
            "signal_count": self.signal_count,
            "skipped_occupied_count": self.skipped_occupied_count,
            "stop_first_count": self.stop_first_count,
            "stop_loss_ticks": self.stop_loss_ticks,
            "take_profit_first_count": self.take_profit_first_count,
            "take_profit_ticks": self.take_profit_ticks,
            "terminal_exit_count": self.terminal_exit_count,
            "variable_cost_ticks": self.variable_cost_ticks,
        }


@dataclass(slots=True)
class _BlockAccumulator:
    entry_fill_count: int = 0
    net_pnl_ticks: int = 0
    gross_profit_ticks: int = 0
    equity_ticks: int = 0
    peak_equity_ticks: int = 0
    maximum_drawdown_ticks: int = 0

    def add(
        self,
        *,
        gross_pnl_ticks: int,
        fully_loaded_net_pnl_ticks: int,
    ) -> None:
        self.entry_fill_count += 1
        self.net_pnl_ticks += fully_loaded_net_pnl_ticks
        self.gross_profit_ticks += max(gross_pnl_ticks, 0)
        self.equity_ticks += fully_loaded_net_pnl_ticks
        self.peak_equity_ticks = max(self.peak_equity_ticks, self.equity_ticks)
        self.maximum_drawdown_ticks = max(
            self.maximum_drawdown_ticks,
            self.peak_equity_ticks - self.equity_ticks,
        )


@dataclass(slots=True)
class _CellAccumulator:
    signal_count: int = 0
    entry_fill_count: int = 0
    entry_not_filled_count: int = 0
    skipped_occupied_count: int = 0
    take_profit_first_count: int = 0
    stop_first_count: int = 0
    terminal_exit_count: int = 0
    gross_pnl_ticks: int = 0
    variable_cost_ticks: int = 0
    allocated_fixed_cost_ticks: int = 0
    net_value_count: int = 0
    net_pnl_ticks: int = 0
    net_gain_ticks: int = 0
    net_loss_ticks: int = 0
    equity_ticks: int = 0
    peak_equity_ticks: int = 0
    maximum_drawdown_ticks: int = 0
    blocks: dict[str, _BlockAccumulator] = field(default_factory=dict)
    occupied_segment_id: int | None = None
    occupied_through_index: int | None = None

    def add_net(self, value: int) -> None:
        self.net_value_count += 1
        self.net_pnl_ticks += value
        self.net_gain_ticks += max(value, 0)
        self.net_loss_ticks += max(-value, 0)
        self.equity_ticks += value
        self.peak_equity_ticks = max(self.peak_equity_ticks, self.equity_ticks)
        self.maximum_drawdown_ticks = max(
            self.maximum_drawdown_ticks,
            self.peak_equity_ticks - self.equity_ticks,
        )


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def _maximum_drawdown(values: Sequence[int]) -> int:
    equity = 0
    peak = 0
    maximum = 0
    for value in values:
        equity += value
        peak = max(peak, equity)
        maximum = max(maximum, peak - equity)
    return maximum


def _profit_factor(gains: int, losses: int) -> Decimal | None:
    if losses:
        return Decimal(gains) / Decimal(losses)
    if gains:
        return Decimal("Infinity")
    return None


class BarCandidateEconomicsAccumulator:
    """Stream all signals through 484 independent one-position portfolios."""

    def __init__(
        self,
        *,
        scenario_id: str,
        direction: Direction | str,
        block_keys: Sequence[str],
        observed_utc_months: Sequence[str],
    ) -> None:
        try:
            self._direction = Direction(direction)
        except (TypeError, ValueError) as error:
            raise BarEconomicsError("direction must be LONG or SHORT") from error
        scenario = BAR_EXECUTION_SCENARIOS.get(scenario_id)
        if scenario is None:
            raise BarEconomicsError("scenario_id is outside the frozen scenarios")
        blocks = tuple(block_keys)
        if not blocks or blocks != tuple(dict.fromkeys(blocks)):
            raise BarEconomicsError("block_keys must be non-empty and unique")
        months = tuple(observed_utc_months)
        if not months or months != tuple(sorted(set(months))):
            raise BarEconomicsError("observed_utc_months must be sorted and unique")
        self._scenario_id = scenario_id
        self._fixed_pool_multiplier = scenario.fixed_pool_multiplier
        self._block_keys = blocks
        self._months = months
        self._cells = {
            (take_profit, stop_loss): _CellAccumulator(
                blocks={key: _BlockAccumulator() for key in blocks}
            )
            for take_profit in BARRIER_TICKS
            for stop_loss in BARRIER_TICKS
        }
        self._previous_signal_ts: int | None = None
        self._previous_signal_id: str | None = None

    def add(self, signal: CandidateSignalReplay) -> None:
        """Add one chronologically ordered signal to every frozen cell."""

        if not isinstance(signal, CandidateSignalReplay):
            raise BarEconomicsError("signal must be a CandidateSignalReplay")
        if (
            self._previous_signal_ts == signal.signal_ts_ns
            and self._previous_signal_id == signal.signal_id
        ):
            raise BarEconomicsError(f"duplicate signal_id: {signal.signal_id}")
        if self._previous_signal_ts is not None and signal.signal_ts_ns <= self._previous_signal_ts:
            raise BarEconomicsError("signals must be strictly chronologically ordered")
        if signal.block_key not in self._block_keys:
            raise BarEconomicsError("signal block is outside the frozen reporting blocks")
        if signal.utc_month not in self._months:
            raise BarEconomicsError("signal month is outside the observed calendar")
        self._previous_signal_ts = signal.signal_ts_ns
        self._previous_signal_id = signal.signal_id

        for cell in self._cells.values():
            cell.signal_count += 1
        if signal.surface is None:
            for cell in self._cells.values():
                cell.entry_not_filled_count += 1
            return

        surface = signal.surface
        if (
            surface.scenario_id != self._scenario_id
            or surface.direction is not self._direction
            or surface.fixed_pool_multiplier != self._fixed_pool_multiplier
        ):
            raise BarEconomicsError("signal surface disagrees with scenario or direction")
        if len(surface.cells) != len(self._cells):
            raise BarEconomicsError("signal surface does not contain 484 cells")

        for outcome in surface.cells:
            cell = self._cells[(outcome.take_profit_ticks, outcome.stop_loss_ticks)]
            occupied = (
                cell.occupied_segment_id == surface.segment_id
                and cell.occupied_through_index is not None
                and surface.entry_path_index <= cell.occupied_through_index
            )
            if occupied:
                cell.skipped_occupied_count += 1
                continue
            cell.occupied_segment_id = surface.segment_id
            cell.occupied_through_index = outcome.exit_path_index
            cell.entry_fill_count += 1
            if outcome.outcome == "TP_FIRST":
                cell.take_profit_first_count += 1
            elif outcome.outcome == "STOP_FIRST":
                cell.stop_first_count += 1
            elif outcome.outcome == "TERMINAL_EXIT":
                cell.terminal_exit_count += 1
            else:  # pragma: no cover - BarCellOutcome is a closed producer
                raise BarEconomicsError("unknown bar replay outcome")
            cell.gross_pnl_ticks += outcome.gross_pnl_ticks
            cell.variable_cost_ticks += outcome.variable_debit_ticks
            cell.allocated_fixed_cost_ticks += outcome.allocated_fixed_cost_ticks
            net = outcome.fully_loaded_net_pnl_ticks
            cell.add_net(net)
            block = cell.blocks[signal.block_key]
            block.add(
                gross_pnl_ticks=outcome.gross_pnl_ticks,
                fully_loaded_net_pnl_ticks=net,
            )

    def finalize(self) -> tuple[BarCellEconomics, ...]:
        """Return all 484 cells in canonical TP-major order."""

        summaries: list[BarCellEconomics] = []
        for take_profit in BARRIER_TICKS:
            for stop_loss in BARRIER_TICKS:
                cell = self._cells[(take_profit, stop_loss)]
                if cell.signal_count != (
                    cell.entry_fill_count
                    + cell.entry_not_filled_count
                    + cell.skipped_occupied_count
                ):
                    raise BarEconomicsError("entry accounting does not balance")
                if cell.entry_fill_count != (
                    cell.take_profit_first_count + cell.stop_first_count + cell.terminal_exit_count
                ):
                    raise BarEconomicsError("outcome accounting does not balance")
                net_pnl = cell.net_pnl_ticks
                calendar_net = (
                    Decimal(cell.gross_pnl_ticks - cell.variable_cost_ticks) * TICK_VALUE_USD
                    - Decimal(len(self._months))
                    * BASE_MONTHLY_FIXED_POOL_USD
                    * self._fixed_pool_multiplier
                )
                blocks = tuple(
                    BarBlockEconomics(
                        block_key=key,
                        entry_fill_count=cell.blocks[key].entry_fill_count,
                        fully_loaded_net_pnl_ticks=cell.blocks[key].net_pnl_ticks,
                        fully_loaded_net_ev_ticks=(
                            None
                            if not cell.blocks[key].entry_fill_count
                            else Decimal(cell.blocks[key].net_pnl_ticks)
                            / Decimal(cell.blocks[key].entry_fill_count)
                        ),
                        gross_profit_ticks=cell.blocks[key].gross_profit_ticks,
                        maximum_drawdown_ticks=cell.blocks[key].maximum_drawdown_ticks,
                    )
                    for key in self._block_keys
                )
                summaries.append(
                    BarCellEconomics(
                        scenario_id=self._scenario_id,
                        direction=self._direction,
                        take_profit_ticks=take_profit,
                        stop_loss_ticks=stop_loss,
                        signal_count=cell.signal_count,
                        entry_fill_count=cell.entry_fill_count,
                        entry_not_filled_count=cell.entry_not_filled_count,
                        skipped_occupied_count=cell.skipped_occupied_count,
                        take_profit_first_count=cell.take_profit_first_count,
                        stop_first_count=cell.stop_first_count,
                        terminal_exit_count=cell.terminal_exit_count,
                        gross_pnl_ticks=cell.gross_pnl_ticks,
                        variable_cost_ticks=cell.variable_cost_ticks,
                        allocated_fixed_cost_ticks=cell.allocated_fixed_cost_ticks,
                        fully_loaded_net_pnl_ticks=net_pnl,
                        fully_loaded_net_ev_ticks=(
                            None
                            if not cell.net_value_count
                            else Decimal(net_pnl) / Decimal(cell.net_value_count)
                        ),
                        calendar_month_net_pnl_usd=calendar_net,
                        profit_factor=_profit_factor(
                            cell.net_gain_ticks,
                            cell.net_loss_ticks,
                        ),
                        maximum_drawdown_ticks=cell.maximum_drawdown_ticks,
                        blocks=blocks,
                    )
                )
        return tuple(summaries)
