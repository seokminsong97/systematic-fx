from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date
from fractions import Fraction

import pytest

from systematic_fx.research.bar_state_labels import StateOneSecondPathIndex
from systematic_fx.research.bar_state_model import StateTradeDecision
from systematic_fx.research.bar_state_portfolio import (
    DEFAULT_STATE_EXECUTION_SCENARIOS,
    BarStatePortfolioError,
    StatePortfolioSignal,
    StateTradeOutcome,
    realized_distance_ticks,
    stream_state_portfolio,
)


@dataclass(frozen=True)
class _Second:
    timeframe_seconds: int
    segment_id: int
    outcome_span_id: int
    contract: str
    source_date: date
    start_ns: int
    end_ns: int
    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int


class _PathSource:
    def __init__(self, path: StateOneSecondPathIndex) -> None:
        self.path = path
        self.open_count = 0
        self.close_count = 0

    @contextmanager
    def open_path(self, path_id: int):
        assert path_id == self.path.path_id
        self.open_count += 1
        try:
            yield self.path
        finally:
            self.close_count += 1


def _path() -> StateOneSecondPathIndex:
    values = []
    for index, (high, low) in enumerate(((1_040, 960), (1_010, 990), (1_010, 990))):
        start = 1_000_000_000 + index * 1_000_000_000
        values.append(
            _Second(
                timeframe_seconds=1,
                segment_id=1,
                outcome_span_id=3,
                contract="6EH2",
                source_date=date(2022, 1, 3),
                start_ns=start,
                end_ns=start + 1_000_000_000,
                open_ticks=1_000,
                high_ticks=high,
                low_ticks=low,
                close_ticks=1_000,
            )
        )
    return StateOneSecondPathIndex(tuple(values), path_id=3)


def _signal(signal_id: str, decision_ns: int) -> StatePortfolioSignal:
    return StatePortfolioSignal(
        signal_id=signal_id,
        candidate_key="candidate_a",
        fold_key="discovery_inner_1",
        block_key="discovery_inner_1",
        decision_ns=decision_ns,
        signal_active_date=date(2022, 1, 3),
        entry_active_date=date(2022, 1, 3),
        entry_utc_month="2022-01",
        contract="6EH2",
        decision=StateTradeDecision.LONG,
        atr_true_range_sum_ticks=560,
        path_id=3,
        entry_path_index=0,
        fold_terminal_path_index=2,
    )


def test_distance_uses_exact_atr_then_rounds_once() -> None:
    # Exact ATR is 560/20=28; 28*1.5=42 rounds to 40.  Rounding ATR to
    # 32 first would incorrectly produce 48.
    assert realized_distance_ticks(560, Fraction(3, 2)) == 40


def test_streaming_replay_is_stop_first_one_position_and_reconstructable() -> None:
    source = _PathSource(_path())
    records = []
    summary = stream_state_portfolio(
        (_signal("a", 1), _signal("b", 2)),
        path_source=source,
        observed_utc_months=("2022-01",),
        trade_sink=records.append,
        progress_every=1,
    )

    assert source.open_count == source.close_count == 1
    assert summary.memory_plan.retained_trade_record_count == 0
    assert summary.executed_trade_record_count == 3 * 49
    baseline_min = next(
        item
        for item in summary.cells
        if item.scenario_id == "BASELINE"
        and item.take_profit_multiplier == Fraction(1, 2)
        and item.stop_loss_multiplier == Fraction(1, 2)
    )
    assert baseline_min.entry_fill_count == 1
    assert baseline_min.skipped_occupied_count == 1
    assert baseline_min.stop_first_count == 1
    assert baseline_min.same_second_stop_first_count == 1
    record = next(
        item
        for item in records
        if item.scenario_id == "BASELINE"
        and item.take_profit_multiplier == Fraction(1, 2)
        and item.stop_loss_multiplier == Fraction(1, 2)
    )
    assert record.outcome is StateTradeOutcome.STOP_FIRST
    assert record.gross_pnl_ticks == record.selling_price_ticks - record.buying_price_ticks
    assert record.take_profit_target_price_ticks == record.entry_fill_price_ticks + 24
    assert record.loss_trigger_price_ticks == record.entry_fill_price_ticks - 24
    assert record.exit_active_date == date(2022, 1, 3)
    assert summary.axis_resolutions[0].unique_axis_vector_count >= 4


def test_scenario_numeric_drift_fails_closed() -> None:
    drifted = (
        replace(DEFAULT_STATE_EXECUTION_SCENARIOS[0], variable_debit_ticks=5),
        *DEFAULT_STATE_EXECUTION_SCENARIOS[1:],
    )
    with pytest.raises(BarStatePortfolioError, match="execution/cost"):
        stream_state_portfolio(
            (_signal("a", 1),),
            path_source=_PathSource(_path()),
            observed_utc_months=("2022-01",),
            scenarios=drifted,
        )


def test_path_contract_mismatch_fails_closed() -> None:
    with pytest.raises(BarStatePortfolioError, match="contract"):
        stream_state_portfolio(
            (replace(_signal("a", 1), contract="6EM2"),),
            path_source=_PathSource(_path()),
            observed_utc_months=("2022-01",),
        )


def test_duplicate_candidate_decision_identity_fails_closed() -> None:
    with pytest.raises(BarStatePortfolioError, match="candidate decision"):
        stream_state_portfolio(
            (_signal("a", 1), _signal("b", 1)),
            path_source=_PathSource(_path()),
            observed_utc_months=("2022-01",),
        )


def test_entry_coordinate_outside_verified_path_fails_closed() -> None:
    with pytest.raises(BarStatePortfolioError, match="entry is outside"):
        stream_state_portfolio(
            (
                replace(
                    _signal("a", 1),
                    entry_path_index=3,
                    fold_terminal_path_index=3,
                ),
            ),
            path_source=_PathSource(_path()),
            observed_utc_months=("2022-01",),
        )
