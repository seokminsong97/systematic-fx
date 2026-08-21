from __future__ import annotations

import sys
import tomllib
from datetime import UTC, date, datetime
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from campaigns.e2a_month_end_v1.config import frozen_config
from campaigns.e2a_month_end_v1.engine import (
    LegacyQuoteTape,
    TradeTape,
    calendar_event_date,
    legacy_adjacency_event_dates,
    london_decision_epoch,
    provisional_forward_dates,
)


def test_calendar_rule_recovers_sunday_edge_months() -> None:
    active = (
        date(2022, 7, 29),
        date(2022, 7, 31),
        date(2022, 8, 1),
        date(2023, 4, 28),
        date(2023, 4, 30),
        date(2023, 5, 1),
        date(2025, 8, 29),
        date(2025, 8, 31),
        date(2025, 9, 1),
        date(2025, 11, 28),
        date(2025, 11, 30),
        date(2025, 12, 1),
    )

    assert calendar_event_date(active, 2022, 7) == date(2022, 7, 29)
    assert calendar_event_date(active, 2023, 4) == date(2023, 4, 28)
    assert calendar_event_date(active, 2025, 8) == date(2025, 8, 29)
    assert calendar_event_date(active, 2025, 11) == date(2025, 11, 28)
    assert not {
        date(2022, 7, 29),
        date(2023, 4, 28),
        date(2025, 8, 29),
        date(2025, 11, 28),
    }.intersection(legacy_adjacency_event_dates(active))


def test_p15_physical_lookup_does_not_use_later_trade_in_same_second() -> None:
    second = 1_650_000_000
    tape = TradeTape(
        physical_ts_ns=np.array(
            [second * 1_000_000_000 - 1, second * 1_000_000_000 + 900_000_000],
            dtype=np.int64,
        ),
        physical_price_ticks=np.array([22_243, 22_244], dtype=np.int64),
        bar_start_seconds=np.array([second - 1, second], dtype=np.int64),
        bar_close_ticks=np.array([22_243, 22_244], dtype=np.int64),
    )

    assert (
        tape.physical_trade_at_or_before(
            second * 1_000_000_000,
            maximum_staleness_seconds=1_800,
        )
        == 22_243
    )
    assert tape.legacy_bar_close(second, maximum_staleness=1_800) == 22_244


def test_legacy_quote_tape_exposes_unbounded_stale_compatibility_state() -> None:
    midnight = 1_700_000_000
    tape = LegacyQuoteTape(
        day_midnight_epoch=midnight,
        state_seconds=np.array([1_000], dtype=np.int64),
        state_ts_ns=np.array([(midnight + 1_000) * 1_000_000_000], dtype=np.int64),
        bid_ticks=np.array([20_000], dtype=np.int64),
        ask_ticks=np.array([20_001], dtype=np.int64),
        valid=np.array([True], dtype=np.bool_),
    )

    quote = tape.quote_at_or_after(midnight + 20_000, maximum_wait=0)
    assert quote is not None
    assert quote.epoch_second == midnight + 20_000
    assert quote.source_state_ts_ns == (midnight + 1_000) * 1_000_000_000
    terminal = tape.last_valid_second()
    assert terminal is not None
    assert terminal.epoch_second == midnight + 86_399


def test_frozen_next_event_and_london_dst_schedule() -> None:
    config = frozen_config()
    opportunities = provisional_forward_dates(config)

    assert opportunities == (
        date(2026, 8, 31),
        date(2026, 9, 30),
        date(2026, 10, 30),
        date(2026, 11, 30),
        date(2026, 12, 31),
        date(2027, 1, 29),
        date(2027, 2, 26),
        date(2027, 3, 31),
        date(2027, 4, 30),
        date(2027, 5, 31),
        date(2027, 6, 30),
        date(2027, 7, 30),
    )
    assert datetime.fromtimestamp(london_decision_epoch(opportunities[0], config), UTC) == (
        datetime(2026, 8, 31, 14, 0, tzinfo=UTC)
    )
    assert datetime.fromtimestamp(london_decision_epoch(opportunities[2], config), UTC) == (
        datetime(2026, 10, 30, 15, 0, tzinfo=UTC)
    )
    assert datetime.fromtimestamp(london_decision_epoch(opportunities[7], config), UTC) == (
        datetime(2027, 3, 31, 14, 0, tzinfo=UTC)
    )


def test_config_is_single_candidate_fail_closed() -> None:
    config = frozen_config()

    assert config.candidate_count == 1
    assert config.campaign_status == "HISTORICAL_EVIDENCE_CONFLICT"
    assert config.forward_status == "PLANNED_NOT_ARMABLE"
    assert config.take_profit is None
    assert config.stop_loss is None
    assert config.minimum_forward_events == 12
    assert config.multiplicity_policy == "SINGLE_CANDIDATE_NO_BH_NO_NEW_SIGNIFICANCE_CLAIM"


def test_toml_registration_binds_the_python_contract() -> None:
    registration_path = (
        Path(__file__).resolve().parents[2] / "configs/campaigns/e2a_month_end_v1.toml"
    )
    registration = tomllib.loads(registration_path.read_text(encoding="utf-8"))
    config = frozen_config()

    assert registration["campaign"]["id"] == config.campaign_id
    assert registration["campaign"]["candidate_count"] == 1
    assert registration["campaign"]["status"] == config.campaign_status
    assert registration["campaign"]["python_semantic_sha256"] == config.semantic_sha256
    assert registration["authority"]["allowed_execution_mode"] == "OFFLINE_SHADOW_ONLY"
    assert registration["authority"]["paper_order_authority"] is False
    assert registration["historical_evidence"]["independent_audit_status"] == "CONFLICT"
