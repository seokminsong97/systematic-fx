from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path

from systematic_fx.backtest.event_cache import CACHE_SCHEMA, CACHE_VERSION
from systematic_fx.research.outcome_config import (
    EXPECTED_SCENARIO_IDS,
    P5_QUERY_ID,
    load_outcome_replay_config,
)

ROOT = Path(__file__).resolve().parents[2]


class OutcomeReplayConfigTests(unittest.TestCase):
    def test_p5_config_freezes_all_replay_dimensions_and_paths(self) -> None:
        config = load_outcome_replay_config(ROOT)

        self.assertEqual(config.query_id, P5_QUERY_ID)
        self.assertEqual(config.slice_indices, tuple(range(99)))
        self.assertEqual(config.expected_signal_count, 1_111)
        self.assertEqual(dict(config.expected_direction_counts), {"LONG": 529, "SHORT": 582})
        self.assertEqual(config.barrier_ticks, tuple(range(24, 193, 8)))
        self.assertEqual(config.expected_cell_count, 484)
        self.assertEqual(config.first_touch_observation_sessions, 20)
        self.assertEqual(config.maximum_cache_workers, 4)
        self.assertEqual(config.expected_cache_partition_count, 485)
        self.assertEqual(config.expected_completed_source_date_count, 485)
        self.assertEqual(config.expected_last_completed_source_date, date(2023, 8, 31))
        self.assertEqual(config.expected_signal_source_date_count, 238)
        self.assertEqual(config.expected_contract_count, 7)
        self.assertEqual(len(config.expected_artifact_manifest_sha256), 64)
        self.assertEqual(len(config.expected_signal_manifest_sha256), 64)
        self.assertEqual(len(config.expected_input_plan_sha256), 64)
        self.assertEqual(
            tuple(scenario.scenario_id for scenario in config.scenarios),
            EXPECTED_SCENARIO_IDS,
        )
        self.assertTrue(config.cache_output_relative.is_relative_to(Path("data/derived")))
        self.assertTrue(config.checkpoint_output_relative.is_relative_to(Path("data/derived")))
        self.assertTrue(config.result_output_relative.is_relative_to(Path("data/derived")))
        self.assertEqual(len(config.sha256), 64)
        self.assertEqual(
            set(config.config_hashes),
            {"campaign", "cost", "execution", "barrier_grid", "discovery", "outcome_replay"},
        )

    def test_canonical_parameters_record_every_execution_variable(self) -> None:
        parameters = load_outcome_replay_config(ROOT).canonical_parameters()

        self.assertEqual(parameters["cache"]["schema"], CACHE_SCHEMA)
        self.assertEqual(parameters["cache"]["version"], CACHE_VERSION)
        self.assertEqual(
            parameters["global_event_order"],
            [
                "ts_recv_ns",
                "sequence",
                "event_index",
                "contract_key",
            ],
        )
        self.assertEqual(len(parameters["scenarios"]), 3)
        self.assertEqual(
            set(parameters["scenarios"][0]),
            {
                "entry_additional_adverse_ticks",
                "fixed_cost_pool_multiplier",
                "other_market_exit_additional_adverse_ticks",
                "routing_delay_ns",
                "scenario_id",
                "stop_total_minimum_adverse_ticks",
                "take_profit_trade_through_ticks",
                "variable_debit_ticks",
            },
        )
        self.assertTrue(parameters["portfolio_position_continues_after_censor"])
        self.assertEqual(parameters["occupied_signal_behavior"], "LOG_AND_SKIP")
        self.assertEqual(parameters["expected_completed_source_date_count"], 485)
        self.assertEqual(parameters["expected_last_completed_source_date"], "2023-08-31")
        self.assertEqual(
            parameters["terminal_partition_resolution"],
            "REVERSE_SCAN_LAST_VALID_EXECUTABLE_QUOTE_PARTITION_V1",
        )


if __name__ == "__main__":
    unittest.main()
