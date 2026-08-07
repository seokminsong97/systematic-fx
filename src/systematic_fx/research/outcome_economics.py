"""Streaming economics for the Phase 1A shared outcome replay.

The shared replay emits one record for every signal, execution scenario, and
barrier cell.  This module turns those records into the 2,904 normalized rows
owned by the PostgreSQL outcome registry without rebuilding per-signal barrier
surfaces in memory.  First-touch labels and portfolio exits deliberately remain
separate: censoring controls completeness, while actual exits control PnL.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Final

from systematic_fx.backtest.barriers import BARRIER_TICKS, BarrierOutcome, Direction
from systematic_fx.backtest.economics import (
    TICK_VALUE_USD,
    CostScenario,
    EntryStatus,
)
from systematic_fx.backtest.shared_replay import ReplayResultRecord
from systematic_fx.db.outcome_registry import OutcomeCellSummary
from systematic_fx.research.outcome_config import OutcomeScenario

_SCENARIO_IDS: Final = (
    "BASELINE",
    "MODERATE_COMBINED",
    "SEVERE_DIAGNOSTIC",
)


class OutcomeEconomicsError(ValueError):
    """Replay records cannot form one complete governed economic surface."""


def _maximum_drawdown_ticks(values: Sequence[int]) -> int:
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
    return None if losses == 0 else Decimal(gains) / Decimal(losses)


@dataclass(slots=True)
class _CellAccumulator:
    seen_signal_bits: int = 0
    signal_count: int = 0
    entry_fill_count: int = 0
    entry_not_filled_count: int = 0
    skipped_occupied_count: int = 0
    take_profit_first_count: int = 0
    stop_first_count: int = 0
    terminal_exit_count: int = 0
    censored_count: int = 0
    gross_pnl_ticks: int = 0
    chronological_net_ticks: list[tuple[int, int, int]] = field(default_factory=list)


class OutcomeEconomicsAccumulator:
    """Incrementally validate and cost all shared-replay detail records.

    Signal membership is represented as a compact bit set per result cell.  It
    therefore detects duplicates and omissions without retaining 1.6 million
    long signal identifiers in Python sets.  Closed-trade PnL retains only a
    completion timestamp, signal ordinal, and integer ticks so realized-equity
    drawdown follows actual exit order with a deterministic same-time tie-break.
    """

    def __init__(
        self,
        *,
        signal_directions: Mapping[str, Direction],
        observed_utc_months: Sequence[str],
        scenarios: Sequence[OutcomeScenario],
    ) -> None:
        if not isinstance(signal_directions, Mapping) or not signal_directions:
            raise OutcomeEconomicsError("signal_directions must be a non-empty mapping")
        ordered_signals = tuple(signal_directions.items())
        if len({signal_id for signal_id, _ in ordered_signals}) != len(ordered_signals):
            raise OutcomeEconomicsError("signal identifiers must be unique")
        if any(
            not isinstance(signal_id, str) or not signal_id or not isinstance(direction, Direction)
            for signal_id, direction in ordered_signals
        ):
            raise OutcomeEconomicsError("signal_directions contains an invalid identity")

        months = tuple(observed_utc_months)
        if not months or months != tuple(sorted(set(months))):
            raise OutcomeEconomicsError("observed_utc_months must be non-empty, unique, and sorted")
        if any(
            len(month) != 7
            or month[4] != "-"
            or not month[:4].isdigit()
            or not month[5:].isdigit()
            or not 1 <= int(month[5:]) <= 12
            for month in months
        ):
            raise OutcomeEconomicsError("observed_utc_months must use canonical YYYY-MM")

        scenario_values = tuple(scenarios)
        if tuple(item.scenario_id for item in scenario_values) != _SCENARIO_IDS:
            raise OutcomeEconomicsError("outcome scenarios must use the frozen canonical order")
        self._cost_scenarios = {
            item.scenario_id: CostScenario(
                item.scenario_id,
                item.variable_debit_ticks,
                item.fixed_cost_pool_multiplier,
            )
            for item in scenario_values
        }
        self._signal_direction = dict(ordered_signals)
        self._signal_ordinal = {
            signal_id: ordinal for ordinal, (signal_id, _) in enumerate(ordered_signals)
        }
        self._expected_bits = {
            direction: sum(
                1 << ordinal
                for ordinal, (_, signal_direction) in enumerate(ordered_signals)
                if signal_direction is direction
            )
            for direction in Direction
        }
        self._months = months
        self._cells = {
            (scenario_id, direction, take_profit, stop_loss): _CellAccumulator()
            for scenario_id in _SCENARIO_IDS
            for direction in Direction
            for take_profit in BARRIER_TICKS
            for stop_loss in BARRIER_TICKS
        }
        self._record_count = 0

    @property
    def record_count(self) -> int:
        return self._record_count

    def add(self, record: ReplayResultRecord) -> None:
        """Validate and incorporate one unique replay detail record."""

        if not isinstance(record, ReplayResultRecord):
            raise OutcomeEconomicsError("record must be a ReplayResultRecord")
        expected_direction = self._signal_direction.get(record.signal_id)
        if expected_direction is None:
            raise OutcomeEconomicsError(f"unknown replay signal: {record.signal_id}")
        if record.direction is not expected_direction:
            raise OutcomeEconomicsError("replay record direction differs from its signal")
        identity = (
            record.scenario_id,
            record.direction,
            record.take_profit_ticks,
            record.stop_loss_ticks,
        )
        cell = self._cells.get(identity)
        if cell is None:
            raise OutcomeEconomicsError("replay record is outside the frozen result grid")

        ordinal = self._signal_ordinal[record.signal_id]
        signal_bit = 1 << ordinal
        if cell.seen_signal_bits & signal_bit:
            raise OutcomeEconomicsError(
                "duplicate replay result identity for signal/scenario/direction/cell"
            )
        cell.seen_signal_bits |= signal_bit
        cell.signal_count += 1
        self._record_count += 1

        if record.entry_status is EntryStatus.ENTRY_NOT_FILLED:
            if record.no_fill_reason is None or any(
                value is not None
                for value in (
                    record.entry_fill_price_ticks,
                    record.first_touch_outcome,
                    record.portfolio_outcome,
                    record.exit_fill_price_ticks,
                )
            ):
                raise OutcomeEconomicsError("ENTRY_NOT_FILLED record has fill or outcome state")
            cell.entry_not_filled_count += 1
            return
        if record.entry_status is EntryStatus.SKIPPED_OCCUPIED:
            if record.occupying_signal_id is None or any(
                value is not None
                for value in (
                    record.entry_fill_price_ticks,
                    record.first_touch_outcome,
                    record.portfolio_outcome,
                    record.exit_fill_price_ticks,
                )
            ):
                raise OutcomeEconomicsError("SKIPPED_OCCUPIED record has no owner or has a fill")
            cell.skipped_occupied_count += 1
            return
        if record.entry_status is not EntryStatus.ENTRY_FILLED:
            raise OutcomeEconomicsError("unknown replay entry status")

        if (
            record.entry_fill_price_ticks is None
            or record.first_touch_outcome is None
            or record.portfolio_outcome
            not in {
                BarrierOutcome.TP_FIRST,
                BarrierOutcome.STOP_FIRST,
                BarrierOutcome.TERMINAL_EXIT,
            }
            or record.exit_fill_price_ticks is None
        ):
            raise OutcomeEconomicsError("filled replay record lacks a real portfolio exit")

        entry_price = record.entry_fill_price_ticks
        if record.direction is Direction.LONG:
            expected_prices = (
                entry_price,
                entry_price + record.take_profit_ticks,
                entry_price - record.stop_loss_ticks,
            )
        else:
            expected_prices = (
                entry_price - record.take_profit_ticks,
                entry_price,
                entry_price + record.stop_loss_ticks,
            )
        if (
            record.buying_price_ticks,
            record.selling_price_ticks,
            record.loss_price_ticks,
        ) != expected_prices or (
            record.take_profit_target_price_ticks,
            record.stop_trigger_price_ticks,
        ) != (expected_prices[1 if record.direction is Direction.LONG else 0], expected_prices[2]):
            raise OutcomeEconomicsError(
                "filled replay Buying/Selling/Loss prices disagree with its bracket"
            )
        if (
            record.no_fill_reason is not None
            or record.occupying_signal_id is not None
            or record.completion_ts_recv_ns is None
            or record.entry_ref is None
            or record.fill_ref is None
        ):
            raise OutcomeEconomicsError("filled replay record lacks complete execution audit state")
        if (record.first_touch_outcome is BarrierOutcome.CENSORED) != (
            record.first_touch_censor_ref is not None
        ):
            raise OutcomeEconomicsError("first-touch censor label/reference drift")
        if (record.portfolio_outcome is BarrierOutcome.TERMINAL_EXIT) != (
            record.terminal_ref is not None
        ):
            raise OutcomeEconomicsError("terminal outcome/reference drift")

        cell.entry_fill_count += 1
        if record.first_touch_outcome is BarrierOutcome.TP_FIRST:
            cell.take_profit_first_count += 1
        elif record.first_touch_outcome is BarrierOutcome.STOP_FIRST:
            cell.stop_first_count += 1
        elif record.first_touch_outcome is BarrierOutcome.TERMINAL_EXIT:
            cell.terminal_exit_count += 1
        elif record.first_touch_outcome is BarrierOutcome.CENSORED:
            cell.censored_count += 1
        else:  # pragma: no cover - enum is closed and None was rejected above
            raise OutcomeEconomicsError("filled replay record has an invalid first-touch label")

        gross_ticks = (
            record.exit_fill_price_ticks - entry_price
            if record.direction is Direction.LONG
            else entry_price - record.exit_fill_price_ticks
        )
        cost = self._cost_scenarios[record.scenario_id]
        net_ticks = gross_ticks - cost.variable_debit_ticks - cost.allocated_fixed_ticks
        cell.gross_pnl_ticks += gross_ticks
        cell.chronological_net_ticks.append((record.completion_ts_recv_ns, ordinal, net_ticks))

    def extend(self, records: Sequence[ReplayResultRecord]) -> None:
        if isinstance(records, (str, bytes)) or not isinstance(records, Sequence):
            raise OutcomeEconomicsError("records must be a sequence")
        for record in records:
            self.add(record)

    def finalize(self) -> tuple[OutcomeCellSummary, ...]:
        """Require the complete 2,904-cell surface and return canonical rows."""

        summaries: list[OutcomeCellSummary] = []
        for scenario_id in _SCENARIO_IDS:
            cost = self._cost_scenarios[scenario_id]
            for direction in Direction:
                expected_bits = self._expected_bits[direction]
                expected_signal_count = expected_bits.bit_count()
                for take_profit in BARRIER_TICKS:
                    for stop_loss in BARRIER_TICKS:
                        cell = self._cells[(scenario_id, direction, take_profit, stop_loss)]
                        if cell.seen_signal_bits != expected_bits:
                            raise OutcomeEconomicsError(
                                "economic surface has a missing or foreign signal result"
                            )
                        if cell.signal_count != expected_signal_count:
                            raise OutcomeEconomicsError("economic surface signal count drift")
                        if cell.signal_count != (
                            cell.entry_fill_count
                            + cell.entry_not_filled_count
                            + cell.skipped_occupied_count
                        ):
                            raise OutcomeEconomicsError("entry accounting does not balance")
                        if cell.entry_fill_count != (
                            cell.take_profit_first_count
                            + cell.stop_first_count
                            + cell.terminal_exit_count
                            + cell.censored_count
                        ):
                            raise OutcomeEconomicsError("first-touch accounting does not balance")

                        ordered_net = [
                            net
                            for _, _, net in sorted(
                                cell.chronological_net_ticks,
                                key=lambda item: (item[0], item[1]),
                            )
                        ]
                        closed_count = len(ordered_net)
                        if closed_count != cell.entry_fill_count:
                            raise OutcomeEconomicsError(
                                "portfolio exit accounting does not balance"
                            )
                        variable_cost = closed_count * cost.variable_debit_ticks
                        fixed_cost = closed_count * cost.allocated_fixed_ticks
                        net_pnl = sum(ordered_net)
                        calendar_net = (
                            Decimal(cell.gross_pnl_ticks - variable_cost) * TICK_VALUE_USD
                            - Decimal(len(self._months)) * cost.monthly_fixed_pool_usd
                        )
                        summaries.append(
                            OutcomeCellSummary(
                                scenario_id=scenario_id,
                                direction=direction.value,
                                take_profit_ticks=take_profit,
                                stop_loss_ticks=stop_loss,
                                signal_count=cell.signal_count,
                                entry_fill_count=cell.entry_fill_count,
                                entry_not_filled_count=cell.entry_not_filled_count,
                                skipped_occupied_count=cell.skipped_occupied_count,
                                take_profit_first_count=cell.take_profit_first_count,
                                stop_first_count=cell.stop_first_count,
                                terminal_exit_count=cell.terminal_exit_count,
                                censored_count=cell.censored_count,
                                gross_pnl_ticks=cell.gross_pnl_ticks,
                                variable_cost_ticks=variable_cost,
                                allocated_fixed_cost_ticks=fixed_cost,
                                fully_loaded_net_pnl_ticks=net_pnl,
                                fully_loaded_net_ev_ticks=(
                                    None
                                    if closed_count == 0
                                    else Decimal(net_pnl) / Decimal(closed_count)
                                ),
                                fully_loaded_net_pnl_usd=(Decimal(net_pnl) * TICK_VALUE_USD),
                                calendar_month_net_pnl_usd=calendar_net,
                                profit_factor=_profit_factor(ordered_net),
                                maximum_drawdown_usd=(
                                    Decimal(_maximum_drawdown_ticks(ordered_net)) * TICK_VALUE_USD
                                ),
                                complete=cell.censored_count == 0,
                            )
                        )
        return tuple(summaries)


def observed_signal_months(records: Sequence[tuple[str, str]]) -> tuple[str, ...]:
    """Return canonical observed months from ``(signal_id, YYYY-MM)`` pairs."""

    if isinstance(records, (str, bytes)) or not isinstance(records, Sequence) or not records:
        raise OutcomeEconomicsError("signal month records must be a non-empty sequence")
    months = tuple(sorted({month for _, month in records}))
    if any(not signal_id or not month for signal_id, month in records):
        raise OutcomeEconomicsError("signal month records contain an empty value")
    return months
