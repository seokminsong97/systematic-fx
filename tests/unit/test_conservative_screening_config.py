from __future__ import annotations

import tomllib
import unittest
from decimal import Decimal
from pathlib import Path

from systematic_fx.research.screening_config import load_conservative_screening_bundle
from systematic_fx.validation.splits import CALENDAR_VERSION, SPLIT_VERSION

ROOT = Path(__file__).resolve().parents[2]


def _load(relative: str) -> dict[str, object]:
    with (ROOT / relative).open("rb") as handle:
        return tomllib.load(handle)


class ConservativeScreeningConfigTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.campaign = _load("configs/campaigns/phase1a_conservative_screening_v1.toml")
        cls.cost = _load("configs/costs/phase1a_conservative_cost_v1.toml")
        cls.execution = _load("configs/execution/phase1a_conservative_execution_v1.toml")
        cls.grid = _load("configs/research/phase1a_barrier_grid_v1.toml")

    def test_campaign_has_screening_only_authority_and_exact_exclusions(self) -> None:
        authority = self.campaign["authority"]
        policy = self.campaign["data_policy"]
        boundary = policy["reference_boundary"]

        self.assertEqual(authority["maximum_authority"], "SCREENING_SURVIVOR")
        self.assertFalse(authority["pass_backtest_authority"])
        self.assertFalse(authority["paper_authority"])
        self.assertFalse(boundary["definition_status_required_for_screening"])
        self.assertTrue(boundary["definition_status_required_for_pass_backtest"])
        self.assertFalse(boundary["invent_trading_status_allowed"])
        self.assertEqual(
            [str(value) for value in policy["failed_source_dates"]],
            [
                "2024-06-30",
                "2024-07-01",
                "2024-07-14",
                "2026-04-19",
                "2026-06-07",
                "2026-06-21",
            ],
        )

    def test_grid_is_complete_uniform_four_pip_surface(self) -> None:
        grid = self.grid["barrier_grid"]
        pips = list(range(12, 97, 4))
        ticks = [value * 2 for value in pips]

        self.assertEqual(grid["take_profit_pips"], pips)
        self.assertEqual(grid["stop_loss_pips"], pips)
        self.assertEqual(grid["take_profit_ticks"], ticks)
        self.assertEqual(grid["stop_loss_ticks"], ticks)
        self.assertEqual(len(pips), 22)
        self.assertEqual(grid["expected_cell_count"], len(pips) ** 2)
        self.assertEqual(grid["expected_cell_count"], 484)
        self.assertFalse(grid["preselection_pruning_allowed"])

    def test_frozen_costs_sum_and_set_twelve_pip_baseline_floor(self) -> None:
        model = self.cost["cost_model"]
        variable = self.cost["variable_cost"]
        fixed = self.cost["fully_loaded_fixed_allocation"]
        categories = fixed["screening_monthly_assumptions_usd"]
        floor = self.cost["economic_floor"]

        category_sum = sum(Decimal(value) for key, value in categories.items() if key != "total")
        self.assertEqual(category_sum, Decimal(categories["total"]))
        self.assertEqual(category_sum, Decimal(fixed["monthly_pool_usd"]))
        self.assertEqual(model["tick_value_usd"], "6.25")
        self.assertEqual(variable["round_trip_debit_ticks"], 4)
        self.assertEqual(fixed["allocated_fixed_cost_ticks_per_round_trip"], 4)
        self.assertEqual(floor["baseline_cost_ticks"], 8)
        self.assertEqual(floor["baseline_minimum_take_profit_ticks"], 24)
        self.assertEqual(floor["baseline_minimum_take_profit_pips"], 12)

    def test_execution_is_delayed_executable_and_stop_adverse(self) -> None:
        latency = self.execution["latency"]
        entry_gate = self.execution["entry_gate"]
        entry = self.execution["entry_order"]
        target = self.execution["take_profit"]
        stop = self.execution["stop"]
        ordering = self.execution["event_ordering"]

        self.assertEqual(latency["baseline_routing_delay_ms"], 1000)
        self.assertFalse(entry_gate["require_trading_status_active"])
        self.assertTrue(entry_gate["require_screening_contract_selected_from_previous_session"])
        self.assertEqual(entry["type"], "MARKETABLE_LIMIT")
        self.assertEqual(entry["time_in_force"], "IOC")
        self.assertFalse(entry["midpoint_fill_allowed"])
        self.assertFalse(target["touch_is_fill"])
        self.assertEqual(target["trade_through_ticks"], 1)
        self.assertFalse(stop["trigger_is_fill"])
        self.assertEqual(stop["baseline_minimum_adverse_ticks"], 2)
        self.assertEqual(ordering["same_timestamp_tie_break"], "STOP_FIRST")

    def test_cross_config_versions_are_exact(self) -> None:
        campaign = self.campaign["campaign"]
        self.assertEqual(campaign["cost_model_version"], self.cost["cost_model"]["id"])
        self.assertEqual(
            campaign["execution_model_version"],
            self.execution["execution_model"]["id"],
        )
        self.assertEqual(campaign["barrier_grid_version"], self.grid["barrier_grid"]["id"])

    def test_typed_bundle_covers_every_config_and_operational_constant(self) -> None:
        bundle = load_conservative_screening_bundle(ROOT)

        self.assertEqual(
            set(bundle.config_hashes), {"campaign", "cost", "execution", "barrier_grid"}
        )
        self.assertTrue(all(len(value) == 64 for value in bundle.config_hashes.values()))
        self.assertEqual(len(bundle.bundle_sha256), 64)
        self.assertEqual(bundle.barrier_ticks, tuple(range(24, 193, 8)))
        self.assertEqual(bundle.calendar_version, CALENDAR_VERSION)
        self.assertEqual(bundle.split_version, SPLIT_VERSION)
        self.assertEqual(
            bundle.missing_previous_session_behavior,
            "NO_ENTRY_ENTIRE_SESSION",
        )
        self.assertEqual(bundle.baseline_cost_floor_ticks, 24)
        self.assertEqual(bundle.routing_delay_ms, 1000)
        self.assertEqual(bundle.stop_adverse_ticks, 2)
        self.assertEqual(bundle.take_profit_trade_through_ticks, 1)


if __name__ == "__main__":
    unittest.main()
