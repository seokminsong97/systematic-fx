from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path

from systematic_fx.backtest.event_cache import CACHE_SCHEMA, CACHE_VERSION
from systematic_fx.research.hypotheses import canonical_sha256
from systematic_fx.research.outcome_config import (
    EXPECTED_SCENARIO_IDS,
    P1_OUTCOME_ARTIFACT_SCHEMA,
    P1_OUTCOME_CONFIG_RELATIVE_PATH,
    P1_QUERY_ID,
    P4_01_OUTCOME_CONFIG_RELATIVE_PATH,
    P4_01_QUERY_ID,
    P4_02_OUTCOME_CONFIG_RELATIVE_PATH,
    P4_02_QUERY_ID,
    P4_PAIR_CONFIG_SHA256,
    P4_PAIR_ID,
    P5_QUERY_ID,
    load_outcome_replay_config,
    load_p4_pair_outcome_config,
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

    def test_p1_config_freezes_distinct_query_counts_hashes_and_paths(self) -> None:
        config = load_outcome_replay_config(
            ROOT,
            config_path=P1_OUTCOME_CONFIG_RELATIVE_PATH,
        )

        self.assertEqual(config.outcome_config_id, "phase1a_p1_05_outcome_replay_v1")
        self.assertEqual(config.query_id, P1_QUERY_ID)
        self.assertEqual(config.outcome_artifact_schema, P1_OUTCOME_ARTIFACT_SCHEMA)
        self.assertEqual(config.expected_signal_count, 943)
        self.assertEqual(dict(config.expected_direction_counts), {"LONG": 446, "SHORT": 497})
        self.assertEqual(config.expected_signal_source_date_count, 216)
        self.assertEqual(config.expected_contract_count, 7)
        self.assertEqual(config.expected_cache_partition_count, 478)
        self.assertEqual(config.expected_completed_source_date_count, 478)
        self.assertEqual(config.expected_detail_record_count, 1_369_236)
        self.assertEqual(config.expected_summary_row_count, 2_904)
        self.assertEqual(config.expected_first_completed_source_date, date(2022, 1, 7))
        self.assertEqual(config.expected_last_completed_source_date, date(2023, 8, 31))
        self.assertEqual(
            config.expected_artifact_manifest_sha256,
            "23037db1dd12784e379b76effa4f3056cec18d9ae2db7fe7e54e11f2f5424d33",
        )
        self.assertEqual(
            config.expected_signal_manifest_sha256,
            "733728670870dd438e79dfadd9df80043a0f2baf9553733cf89382132fefba25",
        )
        self.assertEqual(
            config.expected_input_plan_sha256,
            "3ad39a9bff36e0eae1c87687bf38108b663394624582167bdbf5d848fe5b0252",
        )
        self.assertEqual(
            config.checkpoint_output_relative,
            Path("data/derived/outcomes/checkpoints/phase1a_p1_05_outcome_replay_v1"),
        )
        self.assertEqual(
            config.result_output_relative,
            Path("data/derived/outcomes/phase1a_p1_05_outcome_replay_v1"),
        )

    def test_p5_canonical_parameters_remain_byte_compatible(self) -> None:
        config = load_outcome_replay_config(ROOT)

        self.assertEqual(
            canonical_sha256(config.canonical_parameters()),
            "00b3258353e99da08b40c201cccd2002b0a7d4ddf2538756ae6be28f0a047bfc",
        )
        self.assertNotIn("expected_first_completed_source_date", config.canonical_parameters())
        self.assertNotIn("outcome_artifact_schema", config.canonical_parameters())

    def test_p4_configs_freeze_distinct_pair_members_and_input_identities(self) -> None:
        first = load_outcome_replay_config(
            ROOT,
            config_path=P4_01_OUTCOME_CONFIG_RELATIVE_PATH,
        )
        second = load_outcome_replay_config(
            ROOT,
            config_path=P4_02_OUTCOME_CONFIG_RELATIVE_PATH,
        )

        self.assertEqual((first.query_id, second.query_id), (P4_01_QUERY_ID, P4_02_QUERY_ID))
        self.assertEqual((first.expected_signal_count, second.expected_signal_count), (334, 340))
        self.assertEqual(
            (dict(first.expected_direction_counts), dict(second.expected_direction_counts)),
            ({"LONG": 175, "SHORT": 159}, {"LONG": 159, "SHORT": 181}),
        )
        self.assertEqual(
            (first.expected_signal_source_date_count, second.expected_signal_source_date_count),
            (143, 155),
        )
        self.assertEqual(
            (first.expected_cache_partition_count, second.expected_cache_partition_count),
            (472, 455),
        )
        self.assertEqual(
            (
                first.expected_first_completed_source_date,
                second.expected_first_completed_source_date,
            ),
            (date(2022, 1, 3), date(2022, 1, 18)),
        )
        self.assertEqual(
            (first.sha256, second.sha256),
            (
                "a98f0c7bcaaca70bbcfe4da7f80414a96bd664c36e025176f0163a9c2a455d25",
                "e9b49a0f45f4988403163085d3e4cc2e960c91cf630ea6d2cc24b7ce95a64220",
            ),
        )
        self.assertEqual(
            first.canonical_parameters()["campaign_sequence"],
            second.canonical_parameters()["campaign_sequence"],
        )

    def test_p4_pair_config_freezes_atomic_release_and_cumulative_ledger(self) -> None:
        pair = load_p4_pair_outcome_config(ROOT)

        self.assertEqual(pair.pair_id, P4_PAIR_ID)
        self.assertEqual(pair.sha256, P4_PAIR_CONFIG_SHA256)
        self.assertEqual(pair.ordered_query_ids, (P4_01_QUERY_ID, P4_02_QUERY_ID))
        self.assertEqual(pair.expected_candidate_count, 2)
        self.assertEqual(pair.expected_summary_count, 5_808)
        self.assertEqual(pair.expected_decision_count, 4)
        self.assertEqual(pair.expected_signal_count, 674)
        self.assertEqual(pair.expected_detail_record_count, 978_648)
        self.assertEqual(pair.new_pair_cell_count, 1_936)
        self.assertEqual(pair.observed_prior_cell_count, 1_936)
        self.assertEqual(pair.cumulative_observed_cell_count, 3_872)
        self.assertEqual(pair.fixed_query_potential_ledger_count, 10_648)


if __name__ == "__main__":
    unittest.main()
