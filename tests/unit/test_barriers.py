from __future__ import annotations

import unittest
from collections.abc import Iterator, Sequence

from systematic_fx.backtest.barriers import (
    BARRIER_TICKS,
    EXPECTED_CELL_COUNT,
    PORTFOLIO_OCCUPANCY_SUPPORTED,
    BarrierOutcome,
    BarrierReplayError,
    CensorReason,
    Direction,
    ExecutableQuote,
    replay_barrier_surface,
)

SECOND = 1_000_000_000


def _quote(
    event_index: int,
    ts_recv_ns: int,
    bid: int | None,
    ask: int | None,
    *,
    valid: bool = True,
) -> ExecutableQuote:
    return ExecutableQuote(
        event_index=event_index,
        ts_recv_ns=ts_recv_ns,
        best_bid_ticks=bid,
        best_ask_ticks=ask,
        valid=valid,
    )


class _SinglePassEvents:
    def __init__(self, events: Sequence[ExecutableQuote]) -> None:
        self._events = events
        self.iteration_count = 0

    def __iter__(self) -> Iterator[ExecutableQuote]:
        self.iteration_count += 1
        if self.iteration_count != 1:
            raise AssertionError("source event path was rescanned")
        yield from self._events


class BarrierSurfaceTests(unittest.TestCase):
    def test_long_surface_is_complete_and_take_profit_requires_trade_through(self) -> None:
        events = _SinglePassEvents(
            [
                _quote(10, 0, 1_024, 1_025),  # target touch is not a fill
                _quote(11, SECOND, 1_025, 1_026),  # one tick through target
            ]
        )
        terminal = _quote(12, 2 * SECOND, 1_005, 1_006)

        surface = replay_barrier_surface(
            direction=Direction.LONG,
            entry_fill_price_ticks=1_000,
            events=events,
            terminal_event=terminal,
        )

        self.assertFalse(PORTFOLIO_OCCUPANCY_SUPPORTED)
        self.assertEqual(events.iteration_count, 1)
        self.assertEqual(len(surface.cells), EXPECTED_CELL_COUNT)
        self.assertEqual(
            {cell.cell_id for cell in surface.cells},
            {
                f"tp{take_profit}_sl{stop}"
                for take_profit in BARRIER_TICKS
                for stop in BARRIER_TICKS
            },
        )
        self.assertEqual(surface.cells[0].cell_id, "tp24_sl24")
        self.assertEqual(surface.cells[-1].cell_id, "tp192_sl192")
        self.assertEqual(surface.thresholds.source_path_passes, 1)
        self.assertEqual(surface.thresholds.source_event_count, 2)
        self.assertEqual(len(surface.thresholds.take_profits), 22)
        self.assertEqual(len(surface.thresholds.stops), 22)

        take_profit = surface.cell(24, 24)
        self.assertEqual(take_profit.outcome, BarrierOutcome.TP_FIRST)
        self.assertEqual(take_profit.buying_price_ticks, 1_000)
        self.assertEqual(take_profit.selling_price_ticks, 1_024)
        self.assertEqual(take_profit.loss_trigger_price_ticks, 976)
        self.assertEqual(take_profit.exit_fill_price_ticks, 1_024)
        self.assertEqual(take_profit.take_profit_fill_price_ticks, 1_024)
        self.assertEqual(take_profit.trigger_event_index, 11)
        self.assertEqual(take_profit.fill_event_index, 11)
        self.assertEqual(take_profit.fill_ts_recv_ns, SECOND)
        self.assertIsNone(take_profit.loss_trigger_event_index)

        threshold = surface.thresholds.take_profits[0]
        self.assertEqual(threshold.target_price_ticks, 1_024)
        self.assertEqual(threshold.trade_through_price_ticks, 1_025)
        self.assertEqual(threshold.fill_event.event_index, 11)  # type: ignore[union-attr]

        unresolved_larger_target = surface.cell(32, 24)
        self.assertEqual(unresolved_larger_target.outcome, BarrierOutcome.TERMINAL_EXIT)
        self.assertEqual(unresolved_larger_target.exit_fill_price_ticks, 1_005)
        self.assertEqual(unresolved_larger_target.fill_event_index, 12)

    def test_same_timestamp_uses_stop_first_even_when_tp_event_index_is_earlier(self) -> None:
        surface = replay_barrier_surface(
            direction="LONG",
            entry_fill_price_ticks=1_000,
            events=[
                _quote(20, 5 * SECOND, 1_025, 1_026),
                _quote(21, 5 * SECOND, 976, 977),
                _quote(22, 6 * SECOND, 980, 981),
            ],
        )

        cell = surface.cell(24, 24)
        self.assertEqual(cell.outcome, BarrierOutcome.STOP_FIRST)
        self.assertEqual(cell.loss_trigger_price_ticks, 976)
        self.assertEqual(cell.loss_fill_price_ticks, 974)
        self.assertEqual(cell.trigger_event_index, 21)
        self.assertEqual(cell.trigger_ts_recv_ns, 5 * SECOND)
        self.assertEqual(cell.fill_event_index, 22)
        self.assertEqual(cell.fill_ts_recv_ns, 6 * SECOND)
        self.assertEqual(cell.loss_trigger_event_index, 21)
        self.assertEqual(cell.loss_fill_event_index, 22)
        self.assertIsNone(cell.take_profit_fill_event_index)

        self.assertEqual(
            surface.thresholds.take_profits[0].fill_event.event_index,  # type: ignore[union-attr]
            20,
        )
        self.assertEqual(
            surface.thresholds.stops[0].trigger_event.event_index,  # type: ignore[union-attr]
            21,
        )

    def test_invalid_quote_cannot_fill_and_long_stop_preserves_worse_gap(self) -> None:
        surface = replay_barrier_surface(
            direction=Direction.LONG,
            entry_fill_price_ticks=1_000,
            events=[
                _quote(30, 0, 976, 977),
                _quote(31, SECOND, 900, 901, valid=False),
                _quote(32, SECOND + 200_000_000, 960, 961),
            ],
        )

        cell = surface.cell(24, 24)
        self.assertEqual(cell.outcome, BarrierOutcome.STOP_FIRST)
        self.assertEqual(cell.loss_trigger_event_index, 30)
        self.assertEqual(cell.loss_fill_event_index, 32)
        self.assertEqual(cell.loss_fill_price_ticks, 960)
        self.assertEqual(surface.thresholds.valid_event_count, 2)

    def test_short_prices_and_minimum_two_tick_stop_adversity(self) -> None:
        surface = replay_barrier_surface(
            direction=Direction.SHORT,
            entry_fill_price_ticks=1_000,
            events=[
                _quote(40, 0, 1_023, 1_024),
                _quote(41, SECOND, 1_024, 1_025),
            ],
        )

        cell = surface.cell(24, 24)
        self.assertEqual(cell.outcome, BarrierOutcome.STOP_FIRST)
        self.assertEqual(cell.buying_price_ticks, 976)
        self.assertEqual(cell.selling_price_ticks, 1_000)
        self.assertEqual(cell.take_profit_target_price_ticks, 976)
        self.assertEqual(cell.loss_trigger_price_ticks, 1_024)
        self.assertEqual(cell.loss_fill_price_ticks, 1_026)
        self.assertEqual(cell.loss_trigger_event_index, 40)
        self.assertEqual(cell.loss_fill_event_index, 41)

        take_profit_surface = replay_barrier_surface(
            direction=Direction.SHORT,
            entry_fill_price_ticks=1_000,
            events=[
                _quote(42, 2 * SECOND, 975, 976),  # target touch is not enough
                _quote(43, 3 * SECOND, 974, 975),
            ],
        )
        take_profit = take_profit_surface.cell(24, 24)
        self.assertEqual(take_profit.outcome, BarrierOutcome.TP_FIRST)
        self.assertEqual(take_profit.exit_fill_price_ticks, 976)
        self.assertEqual(take_profit.take_profit_fill_event_index, 43)

    def test_terminal_and_censored_states_are_explicit(self) -> None:
        long_terminal = replay_barrier_surface(
            direction="LONG",
            entry_fill_price_ticks=1_000,
            events=[],
            terminal_event=_quote(50, SECOND, 998, 1_002),
        ).cell(24, 24)
        short_terminal = replay_barrier_surface(
            direction="SHORT",
            entry_fill_price_ticks=1_000,
            events=[],
            terminal_event=_quote(50, SECOND, 998, 1_002),
        ).cell(24, 24)
        censored = replay_barrier_surface(
            direction="LONG",
            entry_fill_price_ticks=1_000,
            events=[],
        ).cell(24, 24)
        invalid_terminal = replay_barrier_surface(
            direction="LONG",
            entry_fill_price_ticks=1_000,
            events=[],
            terminal_event=_quote(50, SECOND, 900, 901, valid=False),
        ).cell(24, 24)

        self.assertEqual(long_terminal.outcome, BarrierOutcome.TERMINAL_EXIT)
        self.assertEqual(long_terminal.terminal_fill_price_ticks, 998)
        self.assertEqual(short_terminal.terminal_fill_price_ticks, 1_002)
        self.assertEqual(censored.outcome, BarrierOutcome.CENSORED)
        self.assertEqual(censored.censor_reason, CensorReason.OBSERVATION_WINDOW_ENDED)
        self.assertEqual(invalid_terminal.outcome, BarrierOutcome.CENSORED)
        self.assertEqual(invalid_terminal.censor_reason, CensorReason.INVALID_TERMINAL_QUOTE)
        self.assertEqual(invalid_terminal.terminal_event_index, 50)
        self.assertFalse(invalid_terminal.terminal_event_valid)

    def test_trigger_without_a_valid_delayed_fill_is_censored(self) -> None:
        cell = replay_barrier_surface(
            direction="LONG",
            entry_fill_price_ticks=1_000,
            events=[
                _quote(60, 0, 976, 977),
                _quote(61, 2 * SECOND, None, None, valid=False),
            ],
        ).cell(24, 24)

        self.assertEqual(cell.outcome, BarrierOutcome.CENSORED)
        self.assertEqual(cell.censor_reason, CensorReason.STOP_TRIGGERED_BUT_UNFILLED)
        self.assertEqual(cell.trigger_event_index, 60)
        self.assertIsNone(cell.fill_event_index)
        self.assertIsNone(cell.loss_fill_price_ticks)

    def test_event_and_grid_input_order_and_duplicates_are_rejected(self) -> None:
        duplicate_grid = (24, 24, *BARRIER_TICKS[2:])
        reversed_prefix_grid = (32, 24, *BARRIER_TICKS[2:])

        invalid_cases = (
            (
                {"events": [], "take_profit_grid": duplicate_grid},
                "duplicate distances",
            ),
            (
                {"events": [], "stop_loss_grid": reversed_prefix_grid},
                "strictly increasing input order",
            ),
            (
                {"events": [], "take_profit_grid": BARRIER_TICKS[:-1]},
                "frozen 22-value grid",
            ),
            (
                {
                    "events": [
                        _quote(70, SECOND, 999, 1_000),
                        _quote(71, 0, 999, 1_000),
                    ]
                },
                "ts_recv_ns input order",
            ),
            (
                {
                    "events": [
                        _quote(70, 0, 999, 1_000),
                        _quote(70, SECOND, 999, 1_000),
                    ]
                },
                "event_index input order",
            ),
            ({"events": [], "direction": "BOTH"}, "LONG or SHORT"),
        )

        for overrides, message in invalid_cases:
            arguments = {
                "direction": "LONG",
                "entry_fill_price_ticks": 1_000,
                **overrides,
            }
            with self.subTest(message=message), self.assertRaisesRegex(BarrierReplayError, message):
                replay_barrier_surface(**arguments)  # type: ignore[arg-type]

        with self.assertRaisesRegex(BarrierReplayError, "best_bid_ticks < ask"):
            _quote(80, 0, 1_000, 1_000)
        with self.assertRaisesRegex(BarrierReplayError, "integer"):
            replay_barrier_surface(
                direction="LONG",
                entry_fill_price_ticks=True,
                events=[],
            )


if __name__ == "__main__":
    unittest.main()
