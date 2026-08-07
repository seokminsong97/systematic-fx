from __future__ import annotations

import unittest
from dataclasses import replace
from decimal import Decimal

from systematic_fx.backtest.barriers import (
    BARRIER_TICKS,
    Direction,
    ExecutableQuote,
    replay_barrier_surface,
)
from systematic_fx.backtest.economics import (
    COST_SCENARIOS,
    CellEconomics,
    EconomicsError,
    EconomicSurface,
    EntryStatus,
    SignalSurface,
    evaluate_economic_surface,
    select_stable_screening_cell,
)


class EconomicSurfaceTests(unittest.TestCase):
    def test_cost_scenarios_and_complete_signal_accounting(self) -> None:
        self.assertEqual(COST_SCENARIOS["BASELINE"].allocated_fixed_ticks, 4)
        self.assertEqual(COST_SCENARIOS["MODERATE_COMBINED"].allocated_fixed_ticks, 5)
        self.assertEqual(COST_SCENARIOS["SEVERE_DIAGNOSTIC"].allocated_fixed_ticks, 6)

        win = replay_barrier_surface(
            direction="LONG",
            entry_fill_price_ticks=1_000,
            events=[ExecutableQuote(1, 1, 1_193, 1_194)],
        )
        loss = replay_barrier_surface(
            direction="LONG",
            entry_fill_price_ticks=1_000,
            events=[
                ExecutableQuote(2, 2, 976, 977),
                ExecutableQuote(3, 1_000_000_002, 974, 975),
            ],
        )
        signals = (
            SignalSurface("one", 1, "2022-01", EntryStatus.ENTRY_FILLED, win),
            SignalSurface("two", 2, "2022-01", EntryStatus.ENTRY_FILLED, loss),
            SignalSurface(
                "three",
                3,
                "2022-01",
                EntryStatus.ENTRY_NOT_FILLED,
                no_fill_reason="STALE_BOOK",
            ),
            SignalSurface(
                "four",
                4,
                "2022-02",
                EntryStatus.SKIPPED_OCCUPIED,
                no_fill_reason="POSITION_OPEN",
            ),
        )

        result = evaluate_economic_surface(
            signals,
            scenario=COST_SCENARIOS["BASELINE"],
            observed_utc_months=("2022-01", "2022-02"),
        )
        cell = result.cell(24, 24)

        self.assertEqual(len(result.cells), 484)
        self.assertEqual(cell.signal_count, 4)
        self.assertEqual(cell.entry_fill_count, 2)
        self.assertEqual(cell.entry_not_filled_count, 1)
        self.assertEqual(cell.skipped_occupied_count, 1)
        self.assertEqual(cell.take_profit_first_count, 1)
        self.assertEqual(cell.stop_first_count, 1)
        self.assertEqual(cell.censored_count, 0)
        self.assertEqual(cell.gross_pnl_ticks, 24 - 26)
        self.assertEqual(cell.variable_cost_ticks, 8)
        self.assertEqual(cell.allocated_fixed_cost_ticks, 8)
        self.assertEqual(cell.fully_loaded_net_pnl_ticks, -18)
        self.assertEqual(cell.fully_loaded_net_ev_ticks, Decimal(-9))
        self.assertEqual(cell.calendar_month_net_pnl_usd, Decimal("-1062.50"))
        self.assertTrue(cell.complete)

    def test_censored_fill_is_explicitly_incomplete(self) -> None:
        censored = replay_barrier_surface(
            direction="SHORT",
            entry_fill_price_ticks=1_000,
            events=[],
        )
        result = evaluate_economic_surface(
            (SignalSurface("one", 1, "2022-01", EntryStatus.ENTRY_FILLED, censored),),
            scenario=COST_SCENARIOS["BASELINE"],
            observed_utc_months=("2022-01",),
        )
        cell = result.cell(24, 24)
        self.assertEqual(cell.censored_count, 1)
        self.assertEqual(cell.fully_loaded_net_ev_ticks, None)
        self.assertFalse(cell.complete)

    def test_signal_and_month_drift_are_rejected(self) -> None:
        surface = replay_barrier_surface(
            direction=Direction.LONG,
            entry_fill_price_ticks=1_000,
            events=[ExecutableQuote(1, 1, 1_193, 1_194)],
        )
        signal = SignalSurface("one", 1, "2022-01", EntryStatus.ENTRY_FILLED, surface)
        with self.assertRaisesRegex(EconomicsError, "every signal month"):
            evaluate_economic_surface(
                (signal,),
                scenario=COST_SCENARIOS["BASELINE"],
                observed_utc_months=("2022-02",),
            )


def _synthetic_economic_surface(scenario_id: str) -> EconomicSurface:
    cells = []
    for tp in BARRIER_TICKS:
        for sl in BARRIER_TICKS:
            positive = 32 <= tp <= 56 and 32 <= sl <= 56
            ev = Decimal(10) if positive else Decimal(-1)
            cells.append(
                CellEconomics(
                    scenario_id=scenario_id,
                    cell_id=f"tp{tp}_sl{sl}",
                    direction=Direction.LONG,
                    take_profit_ticks=tp,
                    stop_loss_ticks=sl,
                    signal_count=20,
                    entry_fill_count=20,
                    entry_not_filled_count=0,
                    skipped_occupied_count=0,
                    take_profit_first_count=12 if positive else 0,
                    stop_first_count=8 if positive else 20,
                    terminal_exit_count=0,
                    censored_count=0,
                    gross_pnl_ticks=200 if positive else -20,
                    variable_cost_ticks=80,
                    allocated_fixed_cost_ticks=80,
                    fully_loaded_net_pnl_ticks=200 if positive else -20,
                    fully_loaded_net_ev_ticks=ev,
                    fully_loaded_net_pnl_usd=Decimal(1250) if positive else Decimal(-125),
                    calendar_month_net_pnl_usd=(Decimal(750) if positive else Decimal(-625)),
                    profit_factor=Decimal("1.50") if positive else Decimal(0),
                    maximum_drawdown_usd=Decimal(100),
                    complete=True,
                )
            )
    return EconomicSurface(scenario_id, Direction.LONG, tuple(cells))


class StabilityTests(unittest.TestCase):
    def test_single_contiguous_interior_region_selects_medoid_not_best_cell(self) -> None:
        baseline = _synthetic_economic_surface("BASELINE")
        moderate = _synthetic_economic_surface("MODERATE_COMBINED")

        selection = select_stable_screening_cell(baseline, moderate)

        self.assertEqual(selection.label, "SCREENING_SURVIVOR")
        self.assertEqual(selection.positive_region_size, 16)
        self.assertEqual(selection.selected_take_profit_ticks, 40)
        self.assertEqual(selection.selected_stop_loss_ticks, 40)
        self.assertEqual(selection.rejection_reasons, ())

    def test_incomplete_or_fragmented_surface_rejects_without_selection(self) -> None:
        baseline_cells = list(_synthetic_economic_surface("BASELINE").cells)
        moderate_cells = list(_synthetic_economic_surface("MODERATE_COMBINED").cells)
        isolated_index = BARRIER_TICKS.index(160) * 22 + BARRIER_TICKS.index(160)
        for cells in (baseline_cells, moderate_cells):
            cells[isolated_index] = replace(
                cells[isolated_index],
                fully_loaded_net_ev_ticks=Decimal(5),
                calendar_month_net_pnl_usd=Decimal(5),
            )
        baseline = EconomicSurface("BASELINE", Direction.LONG, tuple(baseline_cells))
        fragmented = EconomicSurface("MODERATE_COMBINED", Direction.LONG, tuple(moderate_cells))

        selection = select_stable_screening_cell(baseline, fragmented)

        self.assertEqual(selection.label, "SCREENING_REJECT")
        self.assertIsNone(selection.selected_cell_id)
        self.assertIn(
            "JOINT_POSITIVE_REGION_NOT_SINGLE_CONTIGUOUS_COMPONENT",
            selection.rejection_reasons,
        )


if __name__ == "__main__":
    unittest.main()
