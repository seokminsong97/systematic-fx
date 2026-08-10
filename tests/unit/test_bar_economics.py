from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from systematic_fx.backtest.bar_replay import (
    BAR_EXECUTION_SCENARIOS,
    BarPathIndex,
    replay_bar_signal,
)
from systematic_fx.backtest.barriers import Direction
from systematic_fx.research.bar_economics import (
    BarCandidateEconomicsAccumulator,
    CandidateSignalReplay,
)


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
    segment_id: int = 1,
) -> TinyBar:
    start_ns = second * 1_000_000_000
    return TinyBar(
        timeframe_seconds=1,
        segment_id=segment_id,
        contract="6EZ6",
        start_ns=start_ns,
        end_ns=start_ns + 1_000_000_000,
        open_ticks=open_ticks,
        high_ticks=high_ticks,
        low_ticks=low_ticks,
        close_ticks=close_ticks,
    )


def _surface(*, second: int, high: int, low: int, close: int = 20000):
    path = BarPathIndex(
        (
            _bar(second, 20000, high, low, close),
            _bar(second + 1, close, close + 1, close - 1, close),
        )
    )
    return replay_bar_signal(
        path,
        entry_path_index=0,
        direction=Direction.LONG,
        scenario=BAR_EXECUTION_SCENARIOS["BASELINE"],
    )


def test_accumulator_applies_cell_specific_occupancy_and_costs() -> None:
    accumulator = BarCandidateEconomicsAccumulator(
        scenario_id="BASELINE",
        direction="LONG",
        block_keys=("b1", "b2"),
        observed_utc_months=("2022-01",),
    )
    accumulator.add(
        CandidateSignalReplay(
            signal_id="a",
            signal_ts_ns=1,
            block_key="b1",
            utc_month="2022-01",
            surface=_surface(second=0, high=20030, low=19999, close=20028),
        )
    )
    accumulator.add(
        CandidateSignalReplay(
            signal_id="b",
            signal_ts_ns=2,
            block_key="b1",
            utc_month="2022-01",
            surface=_surface(second=0, high=20030, low=19999, close=20028),
        )
    )
    accumulator.add(
        CandidateSignalReplay(
            signal_id="c",
            signal_ts_ns=3,
            block_key="b2",
            utc_month="2022-01",
            surface=None,
            no_fill_reason="NEXT_BUCKET_EMPTY",
        )
    )

    cell = accumulator.finalize()[0]

    assert cell.signal_count == 3
    assert cell.entry_fill_count == 1
    assert cell.skipped_occupied_count == 1
    assert cell.entry_not_filled_count == 1
    assert cell.take_profit_first_count == 1
    assert cell.fully_loaded_net_pnl_ticks == 16
    assert cell.profit_factor == Decimal("Infinity")
    assert cell.as_dict()["profit_factor"] == "Infinity"
    assert cell.blocks[0].entry_fill_count == 1
    assert cell.blocks[0].fully_loaded_net_pnl_ticks == 16
    assert cell.blocks[0].gross_profit_ticks == 24
    assert cell.blocks[1].entry_fill_count == 0
    assert cell.calendar_month_net_pnl_usd == -375


def test_zero_gain_and_zero_loss_profit_factor_remains_undefined() -> None:
    accumulator = BarCandidateEconomicsAccumulator(
        scenario_id="BASELINE",
        direction="LONG",
        block_keys=("b1",),
        observed_utc_months=("2022-01",),
    )

    cell = accumulator.finalize()[0]

    assert cell.profit_factor is None
    assert cell.as_dict()["profit_factor"] is None


def test_new_segment_releases_occupancy() -> None:
    accumulator = BarCandidateEconomicsAccumulator(
        scenario_id="BASELINE",
        direction="LONG",
        block_keys=("b1",),
        observed_utc_months=("2022-01",),
    )
    first = _surface(second=0, high=20002, low=19999)
    second_path = BarPathIndex(
        (
            _bar(0, 20000, 20030, 19999, 20028, segment_id=2),
            _bar(1, 20028, 20029, 20027, 20028, segment_id=2),
        )
    )
    second = replay_bar_signal(
        second_path,
        entry_path_index=0,
        direction="LONG",
        scenario=BAR_EXECUTION_SCENARIOS["BASELINE"],
    )
    for index, surface in enumerate((first, second)):
        accumulator.add(
            CandidateSignalReplay(
                signal_id=str(index),
                signal_ts_ns=index,
                block_key="b1",
                utc_month="2022-01",
                surface=surface,
            )
        )

    cell = accumulator.finalize()[0]
    assert cell.entry_fill_count == 2
    assert cell.skipped_occupied_count == 0


def test_negative_and_positive_trades_produce_profit_factor_and_drawdown() -> None:
    accumulator = BarCandidateEconomicsAccumulator(
        scenario_id="BASELINE",
        direction="LONG",
        block_keys=("b1",),
        observed_utc_months=("2022-01",),
    )
    losing = _surface(second=0, high=20002, low=19970)
    winning_path = BarPathIndex(
        (
            _bar(0, 20000, 20030, 19999, 20028, segment_id=2),
            _bar(1, 20028, 20029, 20027, 20028, segment_id=2),
        )
    )
    winning = replay_bar_signal(
        winning_path,
        entry_path_index=0,
        direction="LONG",
        scenario=BAR_EXECUTION_SCENARIOS["BASELINE"],
    )
    for index, surface in enumerate((losing, winning)):
        accumulator.add(
            CandidateSignalReplay(
                signal_id=str(index),
                signal_ts_ns=index,
                block_key="b1",
                utc_month="2022-01",
                surface=surface,
            )
        )

    cell = accumulator.finalize()[0]
    assert cell.fully_loaded_net_pnl_ticks == -18
    assert cell.profit_factor is not None
    assert str(cell.profit_factor) == "0.4705882352941176470588235294"
    assert cell.maximum_drawdown_ticks == 34
