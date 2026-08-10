"""Compact one-position economic replay for state-model decisions.

The engine retains only 12 x 3 x 49 bounded accumulators.  A caller may stream
executed trade records directly to an artifact sink; they are never retained
in the returned result.  First-hit queries are served by one verified
outcome-span index, and a fold terminal is mandatory for every filled signal.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager, ExitStack
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from fractions import Fraction
from typing import Final, Protocol

from systematic_fx.backtest.economics import (
    BASE_MONTHLY_FIXED_POOL_USD,
    EXPECTED_MONTHLY_ROUND_TRIPS,
    TICK_VALUE_USD,
)
from systematic_fx.research.bar_state_features import (
    MAX_VOLATILITY_TICKS,
    MIN_VOLATILITY_TICKS,
    VOLATILITY_ROUND_TICKS,
    round_half_up_fraction,
)
from systematic_fx.research.bar_state_labels import StateOneSecondPathIndex
from systematic_fx.research.bar_state_model import StateTradeDecision

STATE_PORTFOLIO_SCHEMA: Final = "systematic_fx.bar_state_portfolio.v1"
STATE_VOLATILITY_MULTIPLIERS: Final = (
    Fraction(1, 2),
    Fraction(3, 4),
    Fraction(1, 1),
    Fraction(3, 2),
    Fraction(2, 1),
    Fraction(3, 1),
    Fraction(4, 1),
)
STATE_SCENARIO_IDS: Final = ("BASELINE", "MODERATE_COMBINED", "SEVERE_DIAGNOSTIC")
MAX_STATE_CANDIDATES: Final = 12
MAX_STATE_ACCUMULATORS: Final = MAX_STATE_CANDIDATES * 3 * 7 * 7
MAX_STATE_PORTFOLIO_SIGNALS: Final = 1_000_000


class BarStatePortfolioError(ValueError):
    """A signal stream, execution grid, or accounting result is invalid."""


class StateTradeOutcome(StrEnum):
    TP_FIRST = "TP_FIRST"
    STOP_FIRST = "STOP_FIRST"
    TERMINAL_EXIT = "TERMINAL_EXIT"


class RatioLike(Protocol):
    numerator: int
    denominator: int


class ExecutionScenarioLike(Protocol):
    scenario_id: str
    entry_adverse_ticks: int
    take_profit_trade_through_ticks: int
    stop_total_minimum_adverse_ticks: int
    terminal_exit_adverse_ticks: int
    variable_debit_ticks: int
    fixed_pool_multiplier: RatioLike


class StatePortfolioPathSource(Protocol):
    """Open one verified outcome span and release it before the next span."""

    def open_path(self, path_id: int) -> AbstractContextManager[StateOneSecondPathIndex]: ...


@dataclass(frozen=True, slots=True)
class StateExecutionScenario:
    scenario_id: str
    entry_adverse_ticks: int
    take_profit_trade_through_ticks: int
    stop_total_minimum_adverse_ticks: int
    terminal_exit_adverse_ticks: int
    variable_debit_ticks: int
    fixed_pool_multiplier: Fraction

    def __post_init__(self) -> None:
        if self.scenario_id not in STATE_SCENARIO_IDS:
            raise BarStatePortfolioError("scenario_id is outside the frozen three scenarios")
        for name in (
            "entry_adverse_ticks",
            "take_profit_trade_through_ticks",
            "stop_total_minimum_adverse_ticks",
            "terminal_exit_adverse_ticks",
            "variable_debit_ticks",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise BarStatePortfolioError(f"{name} must be a non-negative integer")
        if (
            self.take_profit_trade_through_ticks <= 0
            or self.stop_total_minimum_adverse_ticks <= 0
            or self.variable_debit_ticks <= 0
        ):
            raise BarStatePortfolioError("execution and cost debits must be positive")
        if not isinstance(self.fixed_pool_multiplier, Fraction) or self.fixed_pool_multiplier <= 0:
            raise BarStatePortfolioError("fixed_pool_multiplier must be a positive Fraction")

    @classmethod
    def from_spec(cls, value: ExecutionScenarioLike) -> StateExecutionScenario:
        return cls(
            scenario_id=value.scenario_id,
            entry_adverse_ticks=value.entry_adverse_ticks,
            take_profit_trade_through_ticks=value.take_profit_trade_through_ticks,
            stop_total_minimum_adverse_ticks=value.stop_total_minimum_adverse_ticks,
            terminal_exit_adverse_ticks=value.terminal_exit_adverse_ticks,
            variable_debit_ticks=value.variable_debit_ticks,
            fixed_pool_multiplier=Fraction(
                value.fixed_pool_multiplier.numerator,
                value.fixed_pool_multiplier.denominator,
            ),
        )

    @property
    def allocated_fixed_cost_ticks(self) -> int:
        raw = (
            Fraction(int(BASE_MONTHLY_FIXED_POOL_USD * 100), 100)
            * self.fixed_pool_multiplier
            / EXPECTED_MONTHLY_ROUND_TRIPS
            / Fraction(int(TICK_VALUE_USD * 100), 100)
        )
        return (raw.numerator + raw.denominator - 1) // raw.denominator

    def as_dict(self) -> dict[str, object]:
        return {
            "allocated_fixed_cost_ticks": self.allocated_fixed_cost_ticks,
            "entry_adverse_ticks": self.entry_adverse_ticks,
            "fixed_pool_multiplier": {
                "denominator": self.fixed_pool_multiplier.denominator,
                "numerator": self.fixed_pool_multiplier.numerator,
            },
            "scenario_id": self.scenario_id,
            "stop_total_minimum_adverse_ticks": self.stop_total_minimum_adverse_ticks,
            "take_profit_trade_through_ticks": self.take_profit_trade_through_ticks,
            "terminal_exit_adverse_ticks": self.terminal_exit_adverse_ticks,
            "variable_debit_ticks": self.variable_debit_ticks,
        }


DEFAULT_STATE_EXECUTION_SCENARIOS: Final = (
    StateExecutionScenario("BASELINE", 1, 1, 2, 1, 4, Fraction(1, 1)),
    StateExecutionScenario("MODERATE_COMBINED", 2, 1, 4, 2, 5, Fraction(5, 4)),
    StateExecutionScenario("SEVERE_DIAGNOSTIC", 3, 2, 6, 3, 6, Fraction(3, 2)),
)


@dataclass(frozen=True, slots=True)
class StatePortfolioGrid:
    take_profit_multipliers: tuple[Fraction, ...] = STATE_VOLATILITY_MULTIPLIERS
    stop_loss_multipliers: tuple[Fraction, ...] = STATE_VOLATILITY_MULTIPLIERS

    def __post_init__(self) -> None:
        if (
            self.take_profit_multipliers != STATE_VOLATILITY_MULTIPLIERS
            or self.stop_loss_multipliers != STATE_VOLATILITY_MULTIPLIERS
        ):
            raise BarStatePortfolioError("portfolio grid differs from the frozen 7x7 axes")

    @property
    def cell_count(self) -> int:
        return len(self.take_profit_multipliers) * len(self.stop_loss_multipliers)

    def as_dict(self) -> dict[str, object]:
        def ratios(values: Sequence[Fraction]) -> list[dict[str, int]]:
            return [
                {"denominator": value.denominator, "numerator": value.numerator} for value in values
            ]

        return {
            "distance_clamp_ticks": [MIN_VOLATILITY_TICKS, MAX_VOLATILITY_TICKS],
            "distance_rounding": "NEAREST_8_TICKS_HALF_UP",
            "stop_loss_multipliers": ratios(self.stop_loss_multipliers),
            "take_profit_multipliers": ratios(self.take_profit_multipliers),
        }


DEFAULT_STATE_PORTFOLIO_GRID: Final = StatePortfolioGrid()


@dataclass(frozen=True, slots=True)
class StatePortfolioSignal:
    """One OOS prediction, filled or otherwise, in chronological order."""

    signal_id: str
    candidate_key: str
    fold_key: str
    block_key: str
    decision_ns: int
    signal_active_date: date
    entry_active_date: date | None
    entry_utc_month: str | None
    contract: str
    decision: StateTradeDecision
    atr_true_range_sum_ticks: int
    path_id: int | None = None
    entry_path_index: int | None = None
    fold_terminal_path_index: int | None = None
    no_fill_reason: str | None = None

    def __post_init__(self) -> None:
        if not self.signal_id or not self.candidate_key or not self.fold_key or not self.block_key:
            raise BarStatePortfolioError("signal identity fields must be non-empty")
        if (
            isinstance(self.signal_active_date, datetime)
            or not isinstance(self.signal_active_date, date)
            or not self.contract
        ):
            raise BarStatePortfolioError("signal active date or contract is invalid")
        if isinstance(self.decision_ns, bool) or not isinstance(self.decision_ns, int):
            raise BarStatePortfolioError("decision_ns must be an integer")
        if (
            isinstance(self.atr_true_range_sum_ticks, bool)
            or not isinstance(self.atr_true_range_sum_ticks, int)
            or self.atr_true_range_sum_ticks <= 0
        ):
            raise BarStatePortfolioError("signal ATR sum must be a positive integer")
        path_fields = (self.path_id, self.entry_path_index, self.fold_terminal_path_index)
        if self.decision is StateTradeDecision.NO_TRADE:
            if any(value is not None for value in path_fields) or self.no_fill_reason is not None:
                raise BarStatePortfolioError("NO_TRADE cannot carry entry execution state")
        elif all(value is None for value in path_fields):
            if not self.no_fill_reason:
                raise BarStatePortfolioError("unfilled directional signal requires a reason")
        elif any(value is None for value in path_fields):
            raise BarStatePortfolioError("filled signal requires complete path coordinates")
        else:
            if self.path_id is None or self.path_id <= 0:
                raise BarStatePortfolioError("filled signal path_id is invalid")
            if self.entry_path_index is None or self.entry_path_index < 0:
                raise BarStatePortfolioError("filled signal entry index is invalid")
            if (
                self.fold_terminal_path_index is None
                or self.fold_terminal_path_index < self.entry_path_index
            ):
                raise BarStatePortfolioError("fold terminal precedes signal entry")
            if self.no_fill_reason is not None:
                raise BarStatePortfolioError("filled signal cannot carry no_fill_reason")
        if self.entry_filled:
            if isinstance(self.entry_active_date, datetime) or not isinstance(
                self.entry_active_date, date
            ):
                raise BarStatePortfolioError("filled signal requires entry_active_date")
            month = self.entry_utc_month
            if (
                not isinstance(month, str)
                or len(month) != 7
                or month[4] != "-"
                or not month[:4].isdigit()
                or not month[5:].isdigit()
                or not 1 <= int(month[5:]) <= 12
            ):
                raise BarStatePortfolioError("filled signal entry_utc_month must use YYYY-MM")
        elif self.entry_active_date is not None or self.entry_utc_month is not None:
            raise BarStatePortfolioError("unfilled/NO_TRADE signal cannot claim an entry date")

    @property
    def entry_filled(self) -> bool:
        return self.entry_path_index is not None


@dataclass(frozen=True, slots=True)
class StateTradeRecord:
    signal_id: str
    candidate_key: str
    fold_key: str
    block_key: str
    signal_active_date: date
    entry_active_date: date
    exit_active_date: date
    entry_utc_month: str
    exit_utc_month: str
    contract: str
    scenario_id: str
    direction: StateTradeDecision
    take_profit_multiplier: Fraction
    stop_loss_multiplier: Fraction
    take_profit_ticks: int
    stop_loss_ticks: int
    entry_path_index: int
    exit_path_index: int
    entry_fill_price_ticks: int
    exit_fill_price_ticks: int
    buying_price_ticks: int
    selling_price_ticks: int
    take_profit_target_price_ticks: int
    loss_trigger_price_ticks: int
    outcome: StateTradeOutcome
    same_second_stop_first: bool
    gross_pnl_ticks: int
    variable_cost_ticks: int
    allocated_fixed_cost_ticks: int
    fully_loaded_net_pnl_ticks: int

    def as_dict(self) -> dict[str, object]:
        return {
            "allocated_fixed_cost_ticks": self.allocated_fixed_cost_ticks,
            "block_key": self.block_key,
            "signal_active_date": self.signal_active_date.isoformat(),
            "entry_active_date": self.entry_active_date.isoformat(),
            "exit_active_date": self.exit_active_date.isoformat(),
            "candidate_key": self.candidate_key,
            "contract": self.contract,
            "direction": self.direction.value,
            "entry_fill_price_ticks": self.entry_fill_price_ticks,
            "entry_path_index": self.entry_path_index,
            "exit_fill_price_ticks": self.exit_fill_price_ticks,
            "exit_path_index": self.exit_path_index,
            "fold_key": self.fold_key,
            "fully_loaded_net_pnl_ticks": self.fully_loaded_net_pnl_ticks,
            "gross_pnl_ticks": self.gross_pnl_ticks,
            "buying_price_ticks": self.buying_price_ticks,
            "loss_trigger_price_ticks": self.loss_trigger_price_ticks,
            "outcome": self.outcome.value,
            "same_second_stop_first": self.same_second_stop_first,
            "scenario_id": self.scenario_id,
            "signal_id": self.signal_id,
            "selling_price_ticks": self.selling_price_ticks,
            "stop_loss_multiplier": {
                "denominator": self.stop_loss_multiplier.denominator,
                "numerator": self.stop_loss_multiplier.numerator,
            },
            "stop_loss_ticks": self.stop_loss_ticks,
            "take_profit_multiplier": {
                "denominator": self.take_profit_multiplier.denominator,
                "numerator": self.take_profit_multiplier.numerator,
            },
            "take_profit_ticks": self.take_profit_ticks,
            "take_profit_target_price_ticks": self.take_profit_target_price_ticks,
            "entry_utc_month": self.entry_utc_month,
            "exit_utc_month": self.exit_utc_month,
            "variable_cost_ticks": self.variable_cost_ticks,
        }


@dataclass(frozen=True, slots=True)
class StateBlockPortfolioSummary:
    block_key: str
    entry_fill_count: int
    fully_loaded_net_pnl_ticks: int
    fully_loaded_net_ev_ticks: Decimal | None
    maximum_drawdown_ticks: int
    positive_gross_ticks: int

    def as_dict(self) -> dict[str, object]:
        return {
            "block_key": self.block_key,
            "entry_fill_count": self.entry_fill_count,
            "fully_loaded_net_ev_ticks": (
                None
                if self.fully_loaded_net_ev_ticks is None
                else format(self.fully_loaded_net_ev_ticks, "f")
            ),
            "fully_loaded_net_pnl_ticks": self.fully_loaded_net_pnl_ticks,
            "maximum_drawdown_ticks": self.maximum_drawdown_ticks,
            "positive_gross_ticks": self.positive_gross_ticks,
        }


@dataclass(frozen=True, slots=True)
class StatePortfolioCellSummary:
    candidate_key: str
    scenario_id: str
    take_profit_multiplier: Fraction
    stop_loss_multiplier: Fraction
    signal_count: int
    no_trade_count: int
    entry_fill_count: int
    entry_not_filled_count: int
    skipped_occupied_count: int
    take_profit_first_count: int
    stop_first_count: int
    terminal_exit_count: int
    same_second_stop_first_count: int
    gross_pnl_ticks: int
    variable_cost_ticks: int
    allocated_fixed_cost_ticks: int
    fully_loaded_net_pnl_ticks: int
    fully_loaded_net_ev_ticks: Decimal | None
    calendar_month_net_pnl_usd: Decimal
    profit_factor: Decimal | None
    maximum_drawdown_ticks: int
    distinct_take_profit_distance_count: int
    distinct_stop_loss_distance_count: int
    daily_net_pnl_ticks: tuple[tuple[date, int], ...]
    daily_fill_count: tuple[tuple[date, int], ...]
    positive_gross_by_contract: tuple[tuple[str, int], ...]
    blocks: tuple[StateBlockPortfolioSummary, ...]

    @property
    def cell_id(self) -> str:
        return (
            f"tpm{self.take_profit_multiplier.numerator}_{self.take_profit_multiplier.denominator}"
            f"_slm{self.stop_loss_multiplier.numerator}_{self.stop_loss_multiplier.denominator}"
        )

    def as_dict(self) -> dict[str, object]:
        decimal_text = lambda value: None if value is None else format(value, "f")
        return {
            "allocated_fixed_cost_ticks": self.allocated_fixed_cost_ticks,
            "blocks": [item.as_dict() for item in self.blocks],
            "calendar_month_net_pnl_usd": format(self.calendar_month_net_pnl_usd, "f"),
            "candidate_key": self.candidate_key,
            "cell_id": self.cell_id,
            "distinct_stop_loss_distance_count": self.distinct_stop_loss_distance_count,
            "distinct_take_profit_distance_count": self.distinct_take_profit_distance_count,
            "daily_net_pnl_ticks": [
                {"active_date": active_date.isoformat(), "net_pnl_ticks": value}
                for active_date, value in self.daily_net_pnl_ticks
            ],
            "daily_fill_count": [
                {"active_date": active_date.isoformat(), "fill_count": value}
                for active_date, value in self.daily_fill_count
            ],
            "entry_fill_count": self.entry_fill_count,
            "entry_not_filled_count": self.entry_not_filled_count,
            "fully_loaded_net_ev_ticks": decimal_text(self.fully_loaded_net_ev_ticks),
            "fully_loaded_net_pnl_ticks": self.fully_loaded_net_pnl_ticks,
            "gross_pnl_ticks": self.gross_pnl_ticks,
            "maximum_drawdown_ticks": self.maximum_drawdown_ticks,
            "no_trade_count": self.no_trade_count,
            "profit_factor": decimal_text(self.profit_factor),
            "positive_gross_by_contract": [
                {"contract": contract, "positive_gross_ticks": value}
                for contract, value in self.positive_gross_by_contract
            ],
            "same_second_stop_first_count": self.same_second_stop_first_count,
            "scenario_id": self.scenario_id,
            "signal_count": self.signal_count,
            "skipped_occupied_count": self.skipped_occupied_count,
            "stop_first_count": self.stop_first_count,
            "stop_loss_multiplier": {
                "denominator": self.stop_loss_multiplier.denominator,
                "numerator": self.stop_loss_multiplier.numerator,
            },
            "take_profit_first_count": self.take_profit_first_count,
            "take_profit_multiplier": {
                "denominator": self.take_profit_multiplier.denominator,
                "numerator": self.take_profit_multiplier.numerator,
            },
            "terminal_exit_count": self.terminal_exit_count,
            "variable_cost_ticks": self.variable_cost_ticks,
        }


@dataclass(frozen=True, slots=True)
class StatePortfolioMemoryPlan:
    input_signal_count: int
    maximum_input_signal_count: int
    candidate_count: int
    scenario_count: int
    grid_cell_count: int
    accumulator_count: int
    retained_trade_record_count: int


@dataclass(frozen=True, slots=True)
class StatePortfolioProgress:
    completed_signal_count: int
    total_signal_count: int
    executed_trade_record_count: int


@dataclass(frozen=True, slots=True)
class StateAxisResolutionSummary:
    candidate_key: str
    filled_directional_signal_count: int
    unique_axis_vector_count: int
    axis_vector_sha256: tuple[str, ...]
    per_signal_distinct_count_histogram: tuple[tuple[int, int], ...]

    def __post_init__(self) -> None:
        if len(self.axis_vector_sha256) != 7:
            raise BarStatePortfolioError("axis resolution must bind seven vectors")
        if self.unique_axis_vector_count != len(set(self.axis_vector_sha256)):
            raise BarStatePortfolioError("axis vector uniqueness count drift")


@dataclass(frozen=True, slots=True)
class StatePortfolioReplaySummary:
    signal_count: int
    executed_trade_record_count: int
    candidate_keys: tuple[str, ...]
    observed_utc_months: tuple[str, ...]
    cells: tuple[StatePortfolioCellSummary, ...]
    axis_resolutions: tuple[StateAxisResolutionSummary, ...]
    memory_plan: StatePortfolioMemoryPlan

    def __post_init__(self) -> None:
        expected = len(self.candidate_keys) * len(STATE_SCENARIO_IDS) * 49
        if len(self.cells) != expected:
            raise BarStatePortfolioError("portfolio summary does not contain every cell")
        if tuple(item.candidate_key for item in self.axis_resolutions) != self.candidate_keys:
            raise BarStatePortfolioError("axis resolution summaries differ from candidates")


@dataclass(slots=True)
class _BlockAccumulator:
    fills: int = 0
    net: int = 0
    equity: int = 0
    peak: int = 0
    maximum_drawdown: int = 0
    positive_gross: int = 0

    def add(self, net: int, gross: int) -> None:
        self.fills += 1
        self.net += net
        self.equity += net
        self.peak = max(self.peak, self.equity)
        self.maximum_drawdown = max(self.maximum_drawdown, self.peak - self.equity)
        self.positive_gross += max(gross, 0)


@dataclass(slots=True)
class _CellAccumulator:
    signal_count: int = 0
    no_trade_count: int = 0
    entry_fill_count: int = 0
    entry_not_filled_count: int = 0
    skipped_occupied_count: int = 0
    take_profit_first_count: int = 0
    stop_first_count: int = 0
    terminal_exit_count: int = 0
    same_second_stop_first_count: int = 0
    gross_pnl_ticks: int = 0
    variable_cost_ticks: int = 0
    allocated_fixed_cost_ticks: int = 0
    net_pnl_ticks: int = 0
    net_gains: int = 0
    net_losses: int = 0
    equity: int = 0
    peak: int = 0
    maximum_drawdown: int = 0
    occupied_through: dict[tuple[str, int], int] = field(default_factory=dict)
    tp_distances: set[int] = field(default_factory=set)
    sl_distances: set[int] = field(default_factory=set)
    blocks: dict[str, _BlockAccumulator] = field(default_factory=dict)
    daily_net: dict[date, int] = field(default_factory=dict)
    daily_fills: dict[date, int] = field(default_factory=dict)
    positive_gross_by_contract: dict[str, int] = field(default_factory=dict)

    def add_net(self, value: int) -> None:
        self.net_pnl_ticks += value
        self.net_gains += max(value, 0)
        self.net_losses += max(-value, 0)
        self.equity += value
        self.peak = max(self.peak, self.equity)
        self.maximum_drawdown = max(self.maximum_drawdown, self.peak - self.equity)


def realized_distance_ticks(atr_true_range_sum_ticks: int, multiplier: Fraction) -> int:
    """Apply the frozen per-signal half-up 8-tick rounding and clamp."""

    if (
        isinstance(atr_true_range_sum_ticks, bool)
        or not isinstance(atr_true_range_sum_ticks, int)
        or atr_true_range_sum_ticks <= 0
    ):
        raise BarStatePortfolioError("ATR sum must be a positive integer")
    if not isinstance(multiplier, Fraction) or multiplier not in STATE_VOLATILITY_MULTIPLIERS:
        raise BarStatePortfolioError("multiplier is outside the frozen axis")
    rounded = round_half_up_fraction(
        Fraction(atr_true_range_sum_ticks, 20) * multiplier,
        VOLATILITY_ROUND_TICKS,
    )
    return min(MAX_VOLATILITY_TICKS, max(MIN_VOLATILITY_TICKS, rounded))


def plan_state_portfolio_memory(
    signals: Sequence[StatePortfolioSignal],
) -> StatePortfolioMemoryPlan:
    if len(signals) > MAX_STATE_PORTFOLIO_SIGNALS:
        raise BarStatePortfolioError("portfolio signal-count preflight cap exceeded")
    candidates = tuple(sorted({item.candidate_key for item in signals}))
    if not candidates or len(candidates) > MAX_STATE_CANDIDATES:
        raise BarStatePortfolioError("portfolio candidate count is outside 1..12")
    accumulator_count = len(candidates) * len(STATE_SCENARIO_IDS) * 49
    if accumulator_count > MAX_STATE_ACCUMULATORS:
        raise BarStatePortfolioError("portfolio accumulator cap exceeded")
    return StatePortfolioMemoryPlan(
        input_signal_count=len(signals),
        maximum_input_signal_count=MAX_STATE_PORTFOLIO_SIGNALS,
        candidate_count=len(candidates),
        scenario_count=len(STATE_SCENARIO_IDS),
        grid_cell_count=49,
        accumulator_count=accumulator_count,
        retained_trade_record_count=0,
    )


def _first_hits(
    path: StateOneSecondPathIndex,
    *,
    entry_index: int,
    terminal_index: int,
    entry_fill: int,
    direction: StateTradeDecision,
    distances: Sequence[int],
    scenario: StateExecutionScenario,
) -> tuple[tuple[int | None, ...], tuple[int | None, ...]]:
    end = terminal_index + 1
    tp_hits: list[int | None] = []
    sl_hits: list[int | None] = []
    for distance in distances:
        if direction is StateTradeDecision.LONG:
            tp_hits.append(
                path.first_high_at_or_above(
                    entry_index,
                    end,
                    entry_fill + distance + scenario.take_profit_trade_through_ticks,
                )
            )
            sl_hits.append(path.first_low_at_or_below(entry_index, end, entry_fill - distance))
        else:
            tp_hits.append(
                path.first_low_at_or_below(
                    entry_index,
                    end,
                    entry_fill - distance - scenario.take_profit_trade_through_ticks,
                )
            )
            sl_hits.append(path.first_high_at_or_above(entry_index, end, entry_fill + distance))
    return tuple(tp_hits), tuple(sl_hits)


def stream_state_portfolio(
    signals: Sequence[StatePortfolioSignal],
    *,
    path_source: StatePortfolioPathSource,
    observed_utc_months: Sequence[str],
    grid: StatePortfolioGrid = DEFAULT_STATE_PORTFOLIO_GRID,
    scenarios: Sequence[StateExecutionScenario] = DEFAULT_STATE_EXECUTION_SCENARIOS,
    trade_sink: Callable[[StateTradeRecord], None] | None = None,
    progress: Callable[[StatePortfolioProgress], None] | None = None,
    progress_every: int = 1_000,
) -> StatePortfolioReplaySummary:
    """Replay all OOS signals without retaining per-trade outcomes."""

    if not signals:
        raise BarStatePortfolioError("portfolio requires at least one signal")
    if not isinstance(grid, StatePortfolioGrid):
        raise BarStatePortfolioError("grid must be StatePortfolioGrid")
    scenario_values = tuple(scenarios)
    if scenario_values != DEFAULT_STATE_EXECUTION_SCENARIOS:
        raise BarStatePortfolioError("scenario execution/cost values differ from the frozen order")
    months = tuple(observed_utc_months)
    if not months or months != tuple(sorted(set(months))):
        raise BarStatePortfolioError("observed_utc_months must be sorted and unique")
    if (
        isinstance(progress_every, bool)
        or not isinstance(progress_every, int)
        or progress_every <= 0
    ):
        raise BarStatePortfolioError("progress_every must be a positive integer")
    memory_plan = plan_state_portfolio_memory(signals)
    candidate_keys = tuple(sorted({item.candidate_key for item in signals}))
    blocks_by_candidate: dict[str, set[str]] = defaultdict(set)
    previous_key: tuple[int, str, str] | None = None
    seen_signal_ids: set[str] = set()
    seen_candidate_decisions: set[tuple[str, int]] = set()
    for signal in signals:
        ordering_key = signal.decision_ns, signal.candidate_key, signal.signal_id
        if previous_key is not None and ordering_key <= previous_key:
            raise BarStatePortfolioError(
                "signals must be strictly ordered by decision/candidate/id"
            )
        previous_key = ordering_key
        if signal.signal_id in seen_signal_ids:
            raise BarStatePortfolioError("duplicate signal_id")
        seen_signal_ids.add(signal.signal_id)
        candidate_decision = signal.candidate_key, signal.decision_ns
        if candidate_decision in seen_candidate_decisions:
            raise BarStatePortfolioError("duplicate candidate decision identity")
        seen_candidate_decisions.add(candidate_decision)
        if signal.entry_utc_month is not None and signal.entry_utc_month not in months:
            raise BarStatePortfolioError("signal month is outside observed_utc_months")
        blocks_by_candidate[signal.candidate_key].add(signal.block_key)

    accumulators: dict[tuple[str, str, int, int], _CellAccumulator] = {}
    cells_by_candidate: dict[str, list[_CellAccumulator]] = {
        candidate_key: [] for candidate_key in candidate_keys
    }
    for candidate_key in candidate_keys:
        blocks = sorted(blocks_by_candidate[candidate_key])
        for scenario in scenario_values:
            for tp_index in range(7):
                for sl_index in range(7):
                    accumulator = _CellAccumulator(
                        blocks={key: _BlockAccumulator() for key in blocks}
                    )
                    accumulators[candidate_key, scenario.scenario_id, tp_index, sl_index] = (
                        accumulator
                    )
                    cells_by_candidate[candidate_key].append(accumulator)

    axis_digests = {
        candidate_key: tuple(hashlib.sha256() for _ in range(7)) for candidate_key in candidate_keys
    }
    axis_distinct_histograms: dict[str, dict[int, int]] = {
        candidate_key: {} for candidate_key in candidate_keys
    }
    directional_filled_counts = {candidate_key: 0 for candidate_key in candidate_keys}

    executed = 0
    active_path_id: int | None = None
    active_path: StateOneSecondPathIndex | None = None
    released_path_ids: set[int] = set()
    path_stack = ExitStack()
    try:
        for signal_index, signal in enumerate(signals, start=1):
            for cell in cells_by_candidate[signal.candidate_key]:
                cell.signal_count += 1
                if signal.decision is StateTradeDecision.NO_TRADE:
                    cell.no_trade_count += 1
                elif not signal.entry_filled:
                    cell.entry_not_filled_count += 1
            if signal.decision is not StateTradeDecision.NO_TRADE and signal.entry_filled:
                assert signal.path_id is not None
                assert signal.entry_path_index is not None
                assert signal.fold_terminal_path_index is not None
                assert signal.entry_active_date is not None
                assert signal.entry_utc_month is not None
                if signal.path_id != active_path_id:
                    if signal.path_id in released_path_ids:
                        raise BarStatePortfolioError(
                            "signal stream re-enters a released outcome span"
                        )
                    if active_path_id is not None:
                        released_path_ids.add(active_path_id)
                    path_stack.close()
                    path_stack = ExitStack()
                    active_path = path_stack.enter_context(path_source.open_path(signal.path_id))
                    active_path_id = signal.path_id
                if active_path is None:
                    raise BarStatePortfolioError("filled signal path is unavailable")
                path = active_path
                if path.path_id != signal.path_id:
                    raise BarStatePortfolioError("path mapping identity drift")
                if path.contract != signal.contract:
                    raise BarStatePortfolioError("signal contract differs from its outcome span")
                if signal.entry_path_index >= len(path.bars):
                    raise BarStatePortfolioError("signal entry is outside the verified path")
                if signal.fold_terminal_path_index >= len(path.bars):
                    raise BarStatePortfolioError("fold terminal is outside the verified path")
                entry_bar = path.bars[signal.entry_path_index]
                if entry_bar.source_date != signal.entry_active_date:
                    raise BarStatePortfolioError(
                        "signal entry date differs from its verified path bar"
                    )
                sign = 1 if signal.decision is StateTradeDecision.LONG else -1
                distances = tuple(
                    realized_distance_ticks(signal.atr_true_range_sum_ticks, value)
                    for value in STATE_VOLATILITY_MULTIPLIERS
                )
                directional_filled_counts[signal.candidate_key] += 1
                distinct_count = len(set(distances))
                histogram = axis_distinct_histograms[signal.candidate_key]
                histogram[distinct_count] = histogram.get(distinct_count, 0) + 1
                for digest, distance in zip(
                    axis_digests[signal.candidate_key], distances, strict=True
                ):
                    digest.update(signal.signal_id.encode("utf-8"))
                    digest.update(b"|")
                    digest.update(str(distance).encode("ascii"))
                    digest.update(b"\n")
                for scenario in scenario_values:
                    entry_fill = entry_bar.open_ticks + sign * scenario.entry_adverse_ticks
                    tp_hits, sl_hits = _first_hits(
                        path,
                        entry_index=signal.entry_path_index,
                        terminal_index=signal.fold_terminal_path_index,
                        entry_fill=entry_fill,
                        direction=signal.decision,
                        distances=distances,
                        scenario=scenario,
                    )
                    terminal_bar = path.bars[signal.fold_terminal_path_index]
                    for tp_index, tp_multiplier in enumerate(grid.take_profit_multipliers):
                        tp_distance = distances[tp_index]
                        for sl_index, sl_multiplier in enumerate(grid.stop_loss_multipliers):
                            sl_distance = distances[sl_index]
                            cell = accumulators[
                                signal.candidate_key,
                                scenario.scenario_id,
                                tp_index,
                                sl_index,
                            ]
                            cell.tp_distances.add(tp_distance)
                            cell.sl_distances.add(sl_distance)
                            occupancy_key = signal.fold_key, signal.path_id
                            occupied_through = cell.occupied_through.get(occupancy_key)
                            if (
                                occupied_through is not None
                                and signal.entry_path_index <= occupied_through
                            ):
                                cell.skipped_occupied_count += 1
                                continue
                            tp_hit = tp_hits[tp_index]
                            stop_hit = sl_hits[sl_index]
                            same_second = tp_hit is not None and tp_hit == stop_hit
                            if stop_hit is not None and (tp_hit is None or stop_hit <= tp_hit):
                                outcome = StateTradeOutcome.STOP_FIRST
                                exit_index = stop_hit
                                stop_trigger = entry_fill - sign * sl_distance
                                observed_open = path.bars[exit_index].open_ticks
                                exit_fill = (
                                    min(
                                        observed_open,
                                        stop_trigger - scenario.stop_total_minimum_adverse_ticks,
                                    )
                                    if sign == 1
                                    else max(
                                        observed_open,
                                        stop_trigger + scenario.stop_total_minimum_adverse_ticks,
                                    )
                                )
                            elif tp_hit is not None:
                                outcome = StateTradeOutcome.TP_FIRST
                                exit_index = tp_hit
                                exit_fill = entry_fill + sign * tp_distance
                            else:
                                outcome = StateTradeOutcome.TERMINAL_EXIT
                                exit_index = signal.fold_terminal_path_index
                                exit_fill = (
                                    terminal_bar.close_ticks - scenario.terminal_exit_adverse_ticks
                                    if sign == 1
                                    else terminal_bar.close_ticks
                                    + scenario.terminal_exit_adverse_ticks
                                )
                            cell.occupied_through[occupancy_key] = exit_index
                            gross = sign * (exit_fill - entry_fill)
                            variable = scenario.variable_debit_ticks
                            fixed = scenario.allocated_fixed_cost_ticks
                            net = gross - variable - fixed
                            cell.entry_fill_count += 1
                            if outcome is StateTradeOutcome.TP_FIRST:
                                cell.take_profit_first_count += 1
                            elif outcome is StateTradeOutcome.STOP_FIRST:
                                cell.stop_first_count += 1
                                cell.same_second_stop_first_count += int(same_second)
                            else:
                                cell.terminal_exit_count += 1
                            cell.gross_pnl_ticks += gross
                            cell.variable_cost_ticks += variable
                            cell.allocated_fixed_cost_ticks += fixed
                            cell.add_net(net)
                            cell.blocks[signal.block_key].add(net, gross)
                            exit_active_date = path.bars[exit_index].source_date
                            cell.daily_net[exit_active_date] = (
                                cell.daily_net.get(exit_active_date, 0) + net
                            )
                            cell.daily_fills[exit_active_date] = (
                                cell.daily_fills.get(exit_active_date, 0) + 1
                            )
                            cell.positive_gross_by_contract[signal.contract] = (
                                cell.positive_gross_by_contract.get(signal.contract, 0)
                                + max(gross, 0)
                            )
                            executed += 1
                            if trade_sink is not None:
                                target = entry_fill + sign * tp_distance
                                loss_trigger = entry_fill - sign * sl_distance
                                trade_sink(
                                    StateTradeRecord(
                                        signal_id=signal.signal_id,
                                        candidate_key=signal.candidate_key,
                                        fold_key=signal.fold_key,
                                        block_key=signal.block_key,
                                        signal_active_date=signal.signal_active_date,
                                        entry_active_date=signal.entry_active_date,
                                        exit_active_date=exit_active_date,
                                        entry_utc_month=signal.entry_utc_month,
                                        exit_utc_month=(
                                            f"{exit_active_date.year:04d}-"
                                            f"{exit_active_date.month:02d}"
                                        ),
                                        contract=signal.contract,
                                        scenario_id=scenario.scenario_id,
                                        direction=signal.decision,
                                        take_profit_multiplier=tp_multiplier,
                                        stop_loss_multiplier=sl_multiplier,
                                        take_profit_ticks=tp_distance,
                                        stop_loss_ticks=sl_distance,
                                        entry_path_index=signal.entry_path_index,
                                        exit_path_index=exit_index,
                                        entry_fill_price_ticks=entry_fill,
                                        exit_fill_price_ticks=exit_fill,
                                        buying_price_ticks=(entry_fill if sign == 1 else exit_fill),
                                        selling_price_ticks=(
                                            exit_fill if sign == 1 else entry_fill
                                        ),
                                        take_profit_target_price_ticks=target,
                                        loss_trigger_price_ticks=loss_trigger,
                                        outcome=outcome,
                                        same_second_stop_first=same_second,
                                        gross_pnl_ticks=gross,
                                        variable_cost_ticks=variable,
                                        allocated_fixed_cost_ticks=fixed,
                                        fully_loaded_net_pnl_ticks=net,
                                    )
                                )
            if progress is not None and (
                signal_index % progress_every == 0 or signal_index == len(signals)
            ):
                progress(StatePortfolioProgress(signal_index, len(signals), executed))
    finally:
        path_stack.close()

    summaries: list[StatePortfolioCellSummary] = []
    for candidate_key in candidate_keys:
        for scenario in scenario_values:
            monthly_fixed = (
                Decimal(len(months))
                * BASE_MONTHLY_FIXED_POOL_USD
                * Decimal(scenario.fixed_pool_multiplier.numerator)
                / Decimal(scenario.fixed_pool_multiplier.denominator)
            )
            for tp_index, tp_multiplier in enumerate(grid.take_profit_multipliers):
                for sl_index, sl_multiplier in enumerate(grid.stop_loss_multipliers):
                    cell = accumulators[
                        candidate_key,
                        scenario.scenario_id,
                        tp_index,
                        sl_index,
                    ]
                    if cell.signal_count != (
                        cell.no_trade_count
                        + cell.entry_not_filled_count
                        + cell.skipped_occupied_count
                        + cell.entry_fill_count
                    ):
                        raise BarStatePortfolioError("portfolio entry accounting does not balance")
                    if cell.entry_fill_count != (
                        cell.take_profit_first_count
                        + cell.stop_first_count
                        + cell.terminal_exit_count
                    ):
                        raise BarStatePortfolioError(
                            "portfolio outcome accounting does not balance"
                        )
                    profit_factor = (
                        Decimal("Infinity")
                        if cell.net_gains and not cell.net_losses
                        else None
                        if not cell.net_losses
                        else Decimal(cell.net_gains) / Decimal(cell.net_losses)
                    )
                    summaries.append(
                        StatePortfolioCellSummary(
                            candidate_key=candidate_key,
                            scenario_id=scenario.scenario_id,
                            take_profit_multiplier=tp_multiplier,
                            stop_loss_multiplier=sl_multiplier,
                            signal_count=cell.signal_count,
                            no_trade_count=cell.no_trade_count,
                            entry_fill_count=cell.entry_fill_count,
                            entry_not_filled_count=cell.entry_not_filled_count,
                            skipped_occupied_count=cell.skipped_occupied_count,
                            take_profit_first_count=cell.take_profit_first_count,
                            stop_first_count=cell.stop_first_count,
                            terminal_exit_count=cell.terminal_exit_count,
                            same_second_stop_first_count=cell.same_second_stop_first_count,
                            gross_pnl_ticks=cell.gross_pnl_ticks,
                            variable_cost_ticks=cell.variable_cost_ticks,
                            allocated_fixed_cost_ticks=cell.allocated_fixed_cost_ticks,
                            fully_loaded_net_pnl_ticks=cell.net_pnl_ticks,
                            fully_loaded_net_ev_ticks=(
                                None
                                if not cell.entry_fill_count
                                else Decimal(cell.net_pnl_ticks) / Decimal(cell.entry_fill_count)
                            ),
                            calendar_month_net_pnl_usd=(
                                Decimal(cell.gross_pnl_ticks - cell.variable_cost_ticks)
                                * TICK_VALUE_USD
                                - monthly_fixed
                            ),
                            profit_factor=profit_factor,
                            maximum_drawdown_ticks=cell.maximum_drawdown,
                            distinct_take_profit_distance_count=len(cell.tp_distances),
                            distinct_stop_loss_distance_count=len(cell.sl_distances),
                            daily_net_pnl_ticks=tuple(sorted(cell.daily_net.items())),
                            daily_fill_count=tuple(sorted(cell.daily_fills.items())),
                            positive_gross_by_contract=tuple(
                                sorted(cell.positive_gross_by_contract.items())
                            ),
                            blocks=tuple(
                                StateBlockPortfolioSummary(
                                    block_key=key,
                                    entry_fill_count=block.fills,
                                    fully_loaded_net_pnl_ticks=block.net,
                                    fully_loaded_net_ev_ticks=(
                                        None
                                        if not block.fills
                                        else Decimal(block.net) / Decimal(block.fills)
                                    ),
                                    maximum_drawdown_ticks=block.maximum_drawdown,
                                    positive_gross_ticks=block.positive_gross,
                                )
                                for key, block in sorted(cell.blocks.items())
                            ),
                        )
                    )
    if len(summaries) != memory_plan.accumulator_count:
        raise BarStatePortfolioError("portfolio summary count differs from memory plan")
    return StatePortfolioReplaySummary(
        signal_count=len(signals),
        executed_trade_record_count=executed,
        candidate_keys=candidate_keys,
        observed_utc_months=months,
        cells=tuple(summaries),
        axis_resolutions=tuple(
            StateAxisResolutionSummary(
                candidate_key=candidate_key,
                filled_directional_signal_count=directional_filled_counts[candidate_key],
                unique_axis_vector_count=len(
                    {digest.hexdigest() for digest in axis_digests[candidate_key]}
                ),
                axis_vector_sha256=tuple(
                    digest.hexdigest() for digest in axis_digests[candidate_key]
                ),
                per_signal_distinct_count_histogram=tuple(
                    sorted(axis_distinct_histograms[candidate_key].items())
                ),
            )
            for candidate_key in candidate_keys
        ),
        memory_plan=memory_plan,
    )


__all__ = [
    "DEFAULT_STATE_EXECUTION_SCENARIOS",
    "DEFAULT_STATE_PORTFOLIO_GRID",
    "MAX_STATE_ACCUMULATORS",
    "MAX_STATE_CANDIDATES",
    "MAX_STATE_PORTFOLIO_SIGNALS",
    "STATE_PORTFOLIO_SCHEMA",
    "STATE_SCENARIO_IDS",
    "STATE_VOLATILITY_MULTIPLIERS",
    "BarStatePortfolioError",
    "StateAxisResolutionSummary",
    "StateBlockPortfolioSummary",
    "StateExecutionScenario",
    "StatePortfolioCellSummary",
    "StatePortfolioGrid",
    "StatePortfolioMemoryPlan",
    "StatePortfolioPathSource",
    "StatePortfolioProgress",
    "StatePortfolioReplaySummary",
    "StatePortfolioSignal",
    "StateTradeOutcome",
    "StateTradeRecord",
    "plan_state_portfolio_memory",
    "realized_distance_ticks",
    "stream_state_portfolio",
]
