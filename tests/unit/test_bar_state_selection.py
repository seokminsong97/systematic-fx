from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from fractions import Fraction

from systematic_fx.research.bar_state_model import StateTradeDecision
from systematic_fx.research.bar_state_portfolio import (
    MAX_STATE_PORTFOLIO_SIGNALS,
    STATE_VOLATILITY_MULTIPLIERS,
    StateAxisResolutionSummary,
    StateBlockPortfolioSummary,
    StatePortfolioCellSummary,
    StatePortfolioMemoryPlan,
    StatePortfolioReplaySummary,
    StatePortfolioSignal,
)
from systematic_fx.research.bar_state_selection import (
    BH_FAMILY_SIZE,
    DISCOVERY_INNER_FOLD_KEYS,
    StateCandidateSupport,
    StateFinalistRank,
    StateFoldEvaluationCalendar,
    _bh_adjust,
    _eligible_promotion_components,
    finalist_rank_key,
    select_state_finalists,
    summarize_candidate_support,
)


def _candidate_keys() -> tuple[str, ...]:
    return tuple(f"candidate_{index:02d}" for index in range(12))


def _empty_cell(candidate_key: str, scenario_id: str, tp: int, sl: int):
    blocks = tuple(
        StateBlockPortfolioSummary(key, 0, 0, None, 0, 0) for key in DISCOVERY_INNER_FOLD_KEYS
    )
    return StatePortfolioCellSummary(
        candidate_key=candidate_key,
        scenario_id=scenario_id,
        take_profit_multiplier=STATE_VOLATILITY_MULTIPLIERS[tp],
        stop_loss_multiplier=STATE_VOLATILITY_MULTIPLIERS[sl],
        signal_count=0,
        no_trade_count=0,
        entry_fill_count=0,
        entry_not_filled_count=0,
        skipped_occupied_count=0,
        take_profit_first_count=0,
        stop_first_count=0,
        terminal_exit_count=0,
        same_second_stop_first_count=0,
        gross_pnl_ticks=0,
        variable_cost_ticks=0,
        allocated_fixed_cost_ticks=0,
        fully_loaded_net_pnl_ticks=0,
        fully_loaded_net_ev_ticks=None,
        calendar_month_net_pnl_usd=Decimal(-1),
        profit_factor=None,
        maximum_drawdown_ticks=0,
        distinct_take_profit_distance_count=0,
        distinct_stop_loss_distance_count=0,
        daily_net_pnl_ticks=(),
        daily_fill_count=(),
        positive_gross_by_contract=(),
        blocks=blocks,
    )


def _empty_portfolio() -> StatePortfolioReplaySummary:
    keys = _candidate_keys()
    cells = tuple(
        _empty_cell(candidate, scenario, tp, sl)
        for candidate in keys
        for scenario in ("BASELINE", "MODERATE_COMBINED", "SEVERE_DIAGNOSTIC")
        for tp in range(7)
        for sl in range(7)
    )
    axes = tuple(
        StateAxisResolutionSummary(
            candidate_key=key,
            filled_directional_signal_count=0,
            unique_axis_vector_count=1,
            axis_vector_sha256=("0" * 64,) * 7,
            per_signal_distinct_count_histogram=(),
        )
        for key in keys
    )
    return StatePortfolioReplaySummary(
        signal_count=0,
        executed_trade_record_count=0,
        candidate_keys=keys,
        observed_utc_months=("2022-01",),
        cells=cells,
        axis_resolutions=axes,
        memory_plan=StatePortfolioMemoryPlan(
            input_signal_count=0,
            maximum_input_signal_count=MAX_STATE_PORTFOLIO_SIGNALS,
            candidate_count=12,
            scenario_count=3,
            grid_cell_count=49,
            accumulator_count=12 * 3 * 49,
            retained_trade_record_count=0,
        ),
    )


def _calendars() -> tuple[StateFoldEvaluationCalendar, ...]:
    first = date(2022, 1, 1)
    result = []
    offset = 0
    for key, count in zip(DISCOVERY_INNER_FOLD_KEYS, (117, 117, 137), strict=True):
        values = tuple(first + timedelta(days=offset + index) for index in range(count))
        result.append(StateFoldEvaluationCalendar(key, values))
        offset += count
    return tuple(result)


def test_failed_candidates_retain_full_804_hypothesis_family(monkeypatch) -> None:
    monkeypatch.setattr(
        "systematic_fx.research.bar_state_selection._frozen_fold_evaluation_calendars",
        lambda _plan: _calendars(),
    )
    supports = tuple(
        StateCandidateSupport(key, 300, 0, 0, tuple(zip(DISCOVERY_INNER_FOLD_KEYS, (0, 0, 0))))
        for key in _candidate_keys()
    )
    result = select_state_finalists(
        _empty_portfolio(),
        candidate_order=_candidate_keys(),
        supports=supports,
        split_plan=object(),  # replaced by exact-calendar adapter above
    )

    assert result.bh_family_size == BH_FAMILY_SIZE == 804
    assert len(result.multiplicity_results) == 12 * 49
    assert all(item.raw_p_value == 1 for item in result.multiplicity_results)
    assert result.finalist_keys == ()


def test_post_bh_component_of_eight_cannot_promote() -> None:
    connected_eight = {(0, index) for index in range(7)} | {(1, 0)}
    assert _eligible_promotion_components(connected_eight) == ()
    connected_nine = connected_eight | {(1, 1)}
    assert len(_eligible_promotion_components(connected_nine)[0]) == 9


def test_finalist_metric_tie_uses_candidate_key_ascending() -> None:
    common = {
        "positive_fold_count": 3,
        "worst_fold_ev_ticks": Decimal(1),
        "bootstrap_lower_bound_ev_ticks": Fraction(1, 2),
        "overall_moderate_ev_ticks": Decimal(2),
        "maximum_drawdown_ticks": 5,
        "stop_loss_index": 3,
        "take_profit_index": 2,
    }
    values = [
        StateFinalistRank(candidate_key="candidate_b", **common),
        StateFinalistRank(candidate_key="candidate_a", **common),
    ]
    assert [item.candidate_key for item in sorted(values, key=finalist_rank_key)] == [
        "candidate_a",
        "candidate_b",
    ]


def test_bh_order_and_failed_cells_do_not_shrink_denominator() -> None:
    keys = {
        (candidate, tp, sl): Fraction(1)
        for candidate in _candidate_keys()
        for tp in range(7)
        for sl in range(7)
    }
    first = (_candidate_keys()[0], 0, 0)
    keys[first] = Fraction(1, 20_000)
    forward = _bh_adjust(keys)
    reverse = _bh_adjust(dict(reversed(tuple(keys.items()))))
    assert forward == reverse
    assert forward[first][1] is True
    assert forward[first][0] == Fraction(804, 20_000)


def test_support_uses_signal_date_not_cross_midnight_entry_date() -> None:
    signal = StatePortfolioSignal(
        signal_id="cross_midnight",
        candidate_key="candidate_a",
        fold_key="discovery_inner_1",
        block_key="discovery_inner_1",
        decision_ns=1,
        signal_active_date=date(2022, 1, 3),
        entry_active_date=date(2022, 1, 4),
        entry_utc_month="2022-01",
        contract="6EH2",
        decision=StateTradeDecision.LONG,
        atr_true_range_sum_ticks=480,
        path_id=1,
        entry_path_index=0,
        fold_terminal_path_index=0,
    )
    support = summarize_candidate_support((signal,), timeframe_by_candidate={"candidate_a": 300})[0]
    assert support.distinct_signal_day_count == 1
    assert support.raw_signal_count_by_fold == (
        ("discovery_inner_1", 1),
        ("discovery_inner_2", 0),
        ("discovery_inner_3", 0),
    )


def test_no_trade_has_no_forged_entry_date() -> None:
    signal = StatePortfolioSignal(
        signal_id="no_trade",
        candidate_key="candidate_a",
        fold_key="discovery_inner_1",
        block_key="discovery_inner_1",
        decision_ns=1,
        signal_active_date=date(2022, 1, 3),
        entry_active_date=None,
        entry_utc_month=None,
        contract="6EH2",
        decision=StateTradeDecision.NO_TRADE,
        atr_true_range_sum_ticks=480,
    )
    assert replace(signal, signal_id="still_no_trade").entry_active_date is None
