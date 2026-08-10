from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from systematic_fx.backtest.barriers import BARRIER_TICKS, Direction
from systematic_fx.research.bar_economics import BarBlockEconomics, BarCellEconomics
from systematic_fx.research.bar_selection import (
    BarSupportEvidence,
    rank_bar_finalists,
    screen_bar_candidate,
)


def _cell(
    scenario: str,
    tp: int,
    sl: int,
    *,
    positive: bool,
    positive_profit_factor: Decimal = Decimal("1.2"),
    positive_block_net_ticks: tuple[int, int, int, int] = (25, 25, 25, 25),
    positive_block_gross_ticks: tuple[int, int, int, int] = (100, 100, 100, 100),
) -> BarCellEconomics:
    net = 100 if positive else -100
    ev = Decimal(2) if positive else Decimal(-2)
    blocks = tuple(
        BarBlockEconomics(
            block_key=f"b{index}",
            entry_fill_count=10,
            fully_loaded_net_pnl_ticks=block_net if positive else -25,
            fully_loaded_net_ev_ticks=(
                Decimal(block_net) / Decimal(10) if positive else Decimal("-2.5")
            ),
            gross_profit_ticks=block_gross if positive else 0,
            maximum_drawdown_ticks=10,
        )
        for index, (block_net, block_gross) in enumerate(
            zip(positive_block_net_ticks, positive_block_gross_ticks, strict=True),
            start=1,
        )
    )
    return BarCellEconomics(
        scenario_id=scenario,
        direction=Direction.LONG,
        take_profit_ticks=tp,
        stop_loss_ticks=sl,
        signal_count=50,
        entry_fill_count=40,
        entry_not_filled_count=0,
        skipped_occupied_count=10,
        take_profit_first_count=20,
        stop_first_count=20,
        terminal_exit_count=0,
        gross_pnl_ticks=200,
        variable_cost_ticks=160,
        allocated_fixed_cost_ticks=160,
        fully_loaded_net_pnl_ticks=net,
        fully_loaded_net_ev_ticks=ev,
        calendar_month_net_pnl_usd=Decimal("100" if positive else "-100"),
        profit_factor=positive_profit_factor if positive else Decimal("0.8"),
        maximum_drawdown_ticks=50,
        blocks=blocks,
    )


def _surfaces(
    positive_cells: set[tuple[int, int]],
    *,
    positive_profit_factor: Decimal = Decimal("1.2"),
    positive_block_net_ticks: tuple[int, int, int, int] = (25, 25, 25, 25),
    positive_block_gross_ticks: tuple[int, int, int, int] = (100, 100, 100, 100),
):
    return {
        scenario: tuple(
            _cell(
                scenario,
                tp,
                sl,
                positive=(tp, sl) in positive_cells,
                positive_profit_factor=positive_profit_factor,
                positive_block_net_ticks=positive_block_net_ticks,
                positive_block_gross_ticks=positive_block_gross_ticks,
            )
            for tp in BARRIER_TICKS
            for sl in BARRIER_TICKS
        )
        for scenario in ("BASELINE", "MODERATE_COMBINED", "SEVERE_DIAGNOSTIC")
    }


def _support(**changes: object) -> BarSupportEvidence:
    value = BarSupportEvidence(
        candidate_key="bar_v1_5m_l01_f1_long",
        timeframe_seconds=300,
        direction=Direction.LONG,
        raw_signal_count=200,
        distinct_signal_day_count=80,
        block_signal_counts=(50, 50, 50, 50),
        median_signals_per_active_day_numerator=4,
        median_signals_per_active_day_denominator=1,
    )
    return replace(value, **changes)


def test_support_gate_rejects_sparse_candidate_before_economics() -> None:
    decision = screen_bar_candidate(
        _support(raw_signal_count=20, block_signal_counts=(20, 0, 0, 0)),
        _surfaces(set()),
    )

    assert decision.label == "SUPPORT_REJECT"
    assert "INSUFFICIENT_RAW_SIGNALS" in decision.rejection_reasons
    assert decision.selected_buy_sell_loss_formula is None


def test_isolated_positive_cell_is_not_a_finalist() -> None:
    decision = screen_bar_candidate(_support(), _surfaces({(104, 104)}))

    assert decision.label == "ECONOMIC_REJECT"
    assert "NO_CONTIGUOUS_POSITIVE_COMPONENT_SIZE_9" in decision.rejection_reasons


def test_stable_three_by_three_region_selects_medoid_and_prices() -> None:
    axis = (96, 104, 112)
    region = {(tp, sl) for tp in axis for sl in axis}
    decision = screen_bar_candidate(_support(), _surfaces(region))

    assert decision.label == "DISCOVERY_FINALIST"
    assert decision.selected_take_profit_ticks == 104
    assert decision.selected_stop_loss_ticks == 104
    assert decision.positive_component_size == 9
    assert decision.selected_buy_sell_loss_formula == {
        "buying_price": "next_bar_open_ticks + scenario.entry_adverse_ticks",
        "selling_price": "buying_price_ticks + 104 ticks",
        "loss_price": "buying_price_ticks - 104 ticks",
    }


def test_zero_loss_positive_profit_factor_passes_economic_gate() -> None:
    axis = (96, 104, 112)
    region = {(tp, sl) for tp in axis for sl in axis}

    decision = screen_bar_candidate(
        _support(),
        _surfaces(region, positive_profit_factor=Decimal("Infinity")),
    )

    assert decision.label == "DISCOVERY_FINALIST"


def test_block_concentration_uses_pre_cost_gross_not_positive_net() -> None:
    axis = (96, 104, 112)
    region = {(tp, sl) for tp in axis for sl in axis}
    skewed_positive_net = (97, 1, 1, 1)

    gross_based = screen_bar_candidate(
        _support(),
        _surfaces(
            region,
            positive_block_net_ticks=skewed_positive_net,
            positive_block_gross_ticks=(100, 100, 100, 100),
        ),
    )
    positive_net_proxy = screen_bar_candidate(
        _support(),
        _surfaces(
            region,
            positive_block_net_ticks=skewed_positive_net,
            positive_block_gross_ticks=skewed_positive_net,
        ),
    )

    assert gross_based.label == "DISCOVERY_FINALIST"
    assert positive_net_proxy.label == "ECONOMIC_REJECT"


def test_rank_is_deterministic_and_budgeted() -> None:
    base = screen_bar_candidate(
        _support(),
        _surfaces({(tp, sl) for tp in (96, 104, 112) for sl in (96, 104, 112)}),
    )
    decisions = tuple(replace(base, candidate_key=f"candidate_{index:02d}") for index in range(12))

    ranked = rank_bar_finalists(tuple(reversed(decisions)))

    assert len(ranked) == 10
    assert [item.candidate_key for item in ranked] == [
        f"candidate_{index:02d}" for index in range(10)
    ]
