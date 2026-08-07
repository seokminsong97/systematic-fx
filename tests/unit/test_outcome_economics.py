from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest

from systematic_fx.backtest.barriers import BarrierOutcome, Direction
from systematic_fx.backtest.economics import EntryStatus
from systematic_fx.backtest.shared_replay import ReplayEventReference, ReplayResultRecord
from systematic_fx.research.outcome_config import OutcomeScenario
from systematic_fx.research.outcome_economics import (
    OutcomeEconomicsAccumulator,
    OutcomeEconomicsError,
)


def _scenarios() -> tuple[OutcomeScenario, ...]:
    return (
        OutcomeScenario("BASELINE", 1, 0, 1, 0, 2, 4, Decimal("1.00")),
        OutcomeScenario("MODERATE_COMBINED", 1, 1, 1, 1, 4, 5, Decimal("1.25")),
        OutcomeScenario("SEVERE_DIAGNOSTIC", 1, 2, 2, 2, 6, 6, Decimal("1.50")),
    )


def _record(
    signal_id: str,
    direction: Direction,
    scenario_id: str,
    tp: int,
    sl: int,
    *,
    status: EntryStatus = EntryStatus.ENTRY_FILLED,
    first_touch: BarrierOutcome | None = BarrierOutcome.TP_FIRST,
    portfolio: BarrierOutcome | None = BarrierOutcome.TP_FIRST,
    entry: int | None = 1_000,
    exit_fill: int | None = 1_024,
    completion_ts_recv_ns: int = 3,
) -> ReplayResultRecord:
    skipped = status is EntryStatus.SKIPPED_OCCUPIED
    unfilled = status is not EntryStatus.ENTRY_FILLED
    buying_price = (
        None if unfilled else (entry if direction is Direction.LONG else (entry or 0) - tp)
    )
    selling_price = (
        None if unfilled else ((entry or 0) + tp if direction is Direction.LONG else entry)
    )
    loss_price = (
        None
        if unfilled
        else ((entry or 0) - sl if direction is Direction.LONG else (entry or 0) + sl)
    )
    reference = ReplayEventReference(
        contract_key="6EH2",
        source_date=date(2022, 1, 3),
        session_ordinal=0,
        event_index=1,
        ts_recv_ns=3,
        best_bid_ticks=999,
        best_ask_ticks=1_000,
        valid=True,
    )
    return ReplayResultRecord(
        signal_id=signal_id,
        decision_ts_recv_ns=1,
        utc_month="2022-01",
        scenario_id=scenario_id,
        direction=direction,
        contract_key="6EH2",
        cell_id=f"tp{tp}_sl{sl}",
        take_profit_ticks=tp,
        stop_loss_ticks=sl,
        entry_status=status,
        entry_eligibility_ts_recv_ns=2,
        entry_fill_price_ticks=None if unfilled else entry,
        buying_price_ticks=buying_price,
        selling_price_ticks=selling_price,
        loss_price_ticks=loss_price,
        take_profit_target_price_ticks=(
            None if unfilled else (selling_price if direction is Direction.LONG else buying_price)
        ),
        stop_trigger_price_ticks=loss_price,
        first_touch_outcome=None if unfilled else first_touch,
        portfolio_outcome=None if unfilled else portfolio,
        exit_fill_price_ticks=None if unfilled else exit_fill,
        decision_ref=None,
        eligibility_ref=None,
        attempt_ref=None,
        entry_ref=None if unfilled else reference,
        trigger_ref=None,
        fill_ref=None if unfilled else reference,
        first_touch_censor_ref=(
            reference if not unfilled and first_touch is BarrierOutcome.CENSORED else None
        ),
        terminal_ref=(
            reference if not unfilled and portfolio is BarrierOutcome.TERMINAL_EXIT else None
        ),
        entry_limit_price_ticks=None,
        route_event_count=0,
        maximum_route_quote_gap_ns=0,
        failure_ref=None,
        occupying_signal_id="prior" if skipped else None,
        no_fill_reason="POSITION_OPEN" if skipped else ("NO_FILL" if unfilled else None),
        completion_ts_recv_ns=None if unfilled else completion_ts_recv_ns,
    )


def _complete_records() -> tuple[ReplayResultRecord, ...]:
    rows = []
    for scenario in ("BASELINE", "MODERATE_COMBINED", "SEVERE_DIAGNOSTIC"):
        for signal_id, direction in (("long", Direction.LONG), ("short", Direction.SHORT)):
            for tp in range(24, 193, 8):
                for sl in range(24, 193, 8):
                    if signal_id == "short":
                        rows.append(
                            _record(
                                signal_id,
                                direction,
                                scenario,
                                tp,
                                sl,
                                status=EntryStatus.ENTRY_NOT_FILLED,
                                first_touch=None,
                                portfolio=None,
                                entry=None,
                                exit_fill=None,
                            )
                        )
                    else:
                        rows.append(_record(signal_id, direction, scenario, tp, sl))
    return tuple(rows)


def test_complete_surface_costs_actual_exits_and_preserves_direction_counts() -> None:
    accumulator = OutcomeEconomicsAccumulator(
        signal_directions={"long": Direction.LONG, "short": Direction.SHORT},
        observed_utc_months=("2022-01", "2022-02"),
        scenarios=_scenarios(),
    )
    accumulator.extend(_complete_records())

    summaries = accumulator.finalize()
    baseline_long = summaries[0]
    baseline_short = summaries[484]

    assert len(summaries) == 2_904
    assert accumulator.record_count == 2_904
    assert baseline_long.signal_count == baseline_long.entry_fill_count == 1
    assert baseline_long.gross_pnl_ticks == 24
    assert baseline_long.variable_cost_ticks == 4
    assert baseline_long.allocated_fixed_cost_ticks == 4
    assert baseline_long.fully_loaded_net_pnl_ticks == 16
    assert baseline_long.fully_loaded_net_ev_ticks == Decimal(16)
    assert baseline_long.calendar_month_net_pnl_usd == Decimal("-875.00")
    assert baseline_short.signal_count == baseline_short.entry_not_filled_count == 1
    assert baseline_short.fully_loaded_net_ev_ticks is None
    assert baseline_short.calendar_month_net_pnl_usd == Decimal("-1000.00")


def test_censored_label_uses_actual_portfolio_exit_for_pnl_but_is_incomplete() -> None:
    accumulator = OutcomeEconomicsAccumulator(
        signal_directions={"long": Direction.LONG},
        observed_utc_months=("2022-01",),
        scenarios=_scenarios(),
    )
    rows = []
    for scenario in ("BASELINE", "MODERATE_COMBINED", "SEVERE_DIAGNOSTIC"):
        for tp in range(24, 193, 8):
            for sl in range(24, 193, 8):
                rows.append(
                    _record(
                        "long",
                        Direction.LONG,
                        scenario,
                        tp,
                        sl,
                        first_touch=BarrierOutcome.CENSORED,
                        portfolio=BarrierOutcome.TERMINAL_EXIT,
                    )
                )
    accumulator.extend(tuple(rows))

    summaries = accumulator.finalize()
    baseline = summaries[0]
    assert baseline.censored_count == 1
    assert baseline.gross_pnl_ticks == 24
    assert baseline.fully_loaded_net_pnl_ticks == 16
    assert baseline.complete is False
    record = rows[0]
    assert record.first_touch_outcome is BarrierOutcome.CENSORED
    assert record.exit_fill_price_ticks == 1_024


def test_duplicate_and_direction_drift_are_rejected() -> None:
    accumulator = OutcomeEconomicsAccumulator(
        signal_directions={"long": Direction.LONG},
        observed_utc_months=("2022-01",),
        scenarios=_scenarios(),
    )
    record = _record("long", Direction.LONG, "BASELINE", 24, 24)
    accumulator.add(record)
    with pytest.raises(OutcomeEconomicsError, match="duplicate"):
        accumulator.add(record)
    with pytest.raises(OutcomeEconomicsError, match="direction differs"):
        accumulator.add(replace(record, direction=Direction.SHORT, take_profit_ticks=32))


def test_maximum_drawdown_uses_realized_exit_order_not_signal_order() -> None:
    signals = {
        "loss-first": Direction.LONG,
        "gain-last": Direction.LONG,
        "loss-second": Direction.LONG,
    }
    accumulator = OutcomeEconomicsAccumulator(
        signal_directions=signals,
        observed_utc_months=("2022-01",),
        scenarios=_scenarios(),
    )
    exit_by_signal = {
        "loss-first": (968, 1),  # baseline net -40 ticks
        "gain-last": (1_038, 3),  # baseline net +30 ticks
        "loss-second": (968, 2),  # baseline net -40 ticks
    }
    rows = []
    for scenario in ("BASELINE", "MODERATE_COMBINED", "SEVERE_DIAGNOSTIC"):
        for signal_id in signals:
            exit_fill, completion = exit_by_signal[signal_id]
            for tp in range(24, 193, 8):
                for sl in range(24, 193, 8):
                    rows.append(
                        _record(
                            signal_id,
                            Direction.LONG,
                            scenario,
                            tp,
                            sl,
                            first_touch=BarrierOutcome.TERMINAL_EXIT,
                            portfolio=BarrierOutcome.TERMINAL_EXIT,
                            exit_fill=exit_fill,
                            completion_ts_recv_ns=completion,
                        )
                    )
    accumulator.extend(tuple(rows))

    baseline_long = accumulator.finalize()[0]
    assert baseline_long.maximum_drawdown_usd == Decimal("500.00")
