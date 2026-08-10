from __future__ import annotations

from dataclasses import dataclass

import pytest

from systematic_fx.backtest.bar_replay import (
    BAR_EXECUTION_SCENARIOS,
    BarPathIndex,
    BarReplayError,
    replay_bar_signal,
)
from systematic_fx.backtest.barriers import BARRIER_TICKS, Direction


@dataclass(frozen=True)
class TinyBar:
    timeframe_seconds: int
    segment_id: int
    contract: str
    start_ns: int
    end_ns: int
    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int


def _bar(
    second: int,
    open_ticks: int,
    high_ticks: int,
    low_ticks: int,
    close_ticks: int,
    *,
    timeframe_seconds: int = 1,
    segment_id: int = 1,
    contract: str = "6EZ6",
) -> TinyBar:
    start_ns = second * 1_000_000_000
    return TinyBar(
        timeframe_seconds=timeframe_seconds,
        segment_id=segment_id,
        contract=contract,
        start_ns=start_ns,
        end_ns=start_ns + timeframe_seconds * 1_000_000_000,
        open_ticks=open_ticks,
        high_ticks=high_ticks,
        low_ticks=low_ticks,
        close_ticks=close_ticks,
    )


def test_long_next_open_take_profit_and_cost_fields() -> None:
    path = BarPathIndex(
        (
            _bar(0, 20000, 20003, 19998, 20001),
            _bar(1, 20001, 20030, 20000, 20028),
        )
    )
    scenario = BAR_EXECUTION_SCENARIOS["BASELINE"]

    surface = replay_bar_signal(
        path,
        entry_path_index=0,
        direction=Direction.LONG,
        scenario=scenario,
    )
    cell = surface.cell(24, 24)

    assert surface.entry_fill_price_ticks == 20001
    assert cell.outcome == "TP_FIRST"
    assert cell.take_profit_target_price_ticks == 20025
    assert cell.exit_fill_price_ticks == 20025
    assert cell.buying_price_ticks == 20001
    assert cell.selling_price_ticks == 20025
    assert cell.gross_pnl_ticks == 24
    assert cell.variable_debit_ticks == 4
    assert cell.allocated_fixed_cost_ticks == 4
    assert cell.fully_loaded_net_pnl_ticks == 16


def test_short_next_open_take_profit_and_price_triplet() -> None:
    path = BarPathIndex(
        (
            _bar(0, 20000, 20002, 19970, 19972),
            _bar(1, 19972, 19974, 19968, 19970),
        )
    )

    cell = replay_bar_signal(
        path,
        entry_path_index=0,
        direction=Direction.SHORT,
        scenario=BAR_EXECUTION_SCENARIOS["BASELINE"],
    ).cell(24, 24)

    assert cell.entry_fill_price_ticks == 19999
    assert cell.take_profit_target_price_ticks == 19975
    assert cell.loss_trigger_price_ticks == 20023
    assert cell.outcome == "TP_FIRST"
    assert cell.buying_price_ticks == 19975
    assert cell.selling_price_ticks == 19999
    assert cell.fully_loaded_net_pnl_ticks == 16


def test_same_second_take_profit_and_stop_is_stop_first() -> None:
    path = BarPathIndex((_bar(0, 20000, 20030, 19970, 20000),))

    cell = replay_bar_signal(
        path,
        entry_path_index=0,
        direction="LONG",
        scenario=BAR_EXECUTION_SCENARIOS["BASELINE"],
    ).cell(24, 24)

    assert cell.outcome == "STOP_FIRST"
    assert cell.same_second_stop_first is True
    assert cell.take_profit_hit_index == 0
    assert cell.stop_hit_index == 0
    assert cell.loss_trigger_price_ticks == 19977
    assert cell.exit_fill_price_ticks == 19975
    assert cell.fully_loaded_net_pnl_ticks == -34


def test_gap_through_stop_uses_worse_observed_open() -> None:
    path = BarPathIndex(
        (
            _bar(0, 20000, 20002, 19999, 20001),
            _bar(1, 19960, 19970, 19955, 19965),
        )
    )

    cell = replay_bar_signal(
        path,
        entry_path_index=0,
        direction="LONG",
        scenario=BAR_EXECUTION_SCENARIOS["BASELINE"],
    ).cell(192, 24)

    assert cell.outcome == "STOP_FIRST"
    assert cell.loss_trigger_price_ticks == 19977
    assert cell.exit_fill_price_ticks == 19960
    assert cell.fully_loaded_net_pnl_ticks == -49


def test_unresolved_cell_exits_at_segment_terminal() -> None:
    path = BarPathIndex(
        (
            _bar(0, 20000, 20002, 19999, 20001),
            _bar(2, 20001, 20003, 20000, 20002),
        )
    )

    cell = replay_bar_signal(
        path,
        entry_path_index=0,
        direction="LONG",
        scenario=BAR_EXECUTION_SCENARIOS["MODERATE_COMBINED"],
    ).cell(192, 192)

    assert cell.outcome == "TERMINAL_EXIT"
    assert cell.exit_path_index == 1
    assert cell.exit_fill_price_ticks == 20000
    assert cell.fully_loaded_net_pnl_ticks == -12


def test_threshold_range_queries_find_first_qualifying_bar() -> None:
    path = BarPathIndex(
        (
            _bar(0, 100, 104, 98, 101),
            _bar(2, 101, 107, 100, 106),
            _bar(3, 106, 110, 95, 100),
        )
    )

    assert path.first_high_at_or_above(0, 3, 107) == 1
    assert path.first_high_at_or_above(2, 3, 107) == 2
    assert path.first_low_at_or_below(0, 2, 99) == 0
    assert path.first_low_at_or_below(1, 3, 99) == 2
    assert path.first_high_at_or_above(0, 3, 111) is None


@pytest.mark.parametrize(
    ("bars", "message"),
    [
        ((_bar(0, 100, 101, 99, 100, timeframe_seconds=5),), "one-second"),
        (
            (
                _bar(0, 100, 101, 99, 100),
                _bar(1, 100, 101, 99, 100, segment_id=2),
            ),
            "segment",
        ),
        (
            (_bar(0, 100, 99, 98, 100),),
            "OHLC",
        ),
        (
            (_bar(1, 100, 101, 99, 100), _bar(1, 100, 101, 99, 100)),
            "time ordered",
        ),
    ],
)
def test_path_rejects_invalid_inputs(bars: tuple[TinyBar, ...], message: str) -> None:
    with pytest.raises(BarReplayError, match=message):
        BarPathIndex(bars)


def test_surface_contains_the_frozen_grid() -> None:
    surface = replay_bar_signal(
        BarPathIndex((_bar(0, 20000, 20001, 19999, 20000),)),
        entry_path_index=0,
        direction="SHORT",
        scenario=BAR_EXECUTION_SCENARIOS["SEVERE_DIAGNOSTIC"],
    )

    assert len(surface.take_profit_hits) == len(BARRIER_TICKS) == 22
    assert len(surface.stop_hits) == 22
    assert len(surface.cells) == 484
    assert surface.cells[0].take_profit_ticks == 24
    assert surface.cells[-1].stop_loss_ticks == 192
