from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from campaigns.e2a_month_end_v1.config import frozen_config
from campaigns.e2a_month_end_v1.engine import DayPlan, TradeTape, london_decision_epoch
from campaigns.e2a_month_end_v1.strict import (
    RESET_POLICY,
    derive_strict_signals,
    strict_valid_quote_mask,
)


def test_lab_minimal_policy_rejects_locked_dirty_snapshot_and_reset_rows() -> None:
    selected = 11232
    undefined = 9_223_372_036_854_775_807
    mask = strict_valid_quote_mask(
        instrument_id=np.array([selected] * 8, dtype=np.uint32),
        action=np.array(["A", "A", "A", "R", "A", "A", "A", "A"], dtype=object),
        flags=np.array([0, 0, 4, 0, 32, 0, 0, 0], dtype=np.uint8),
        bid_raw=np.array(
            [
                22_243 * 50_000,
                22_243 * 50_000,
                22_243 * 50_000,
                undefined,
                22_243 * 50_000,
                22_243 * 50_000,
                22_243 * 50_000 + 1,
                22_243 * 50_000,
            ],
            dtype=np.int64,
        ),
        ask_raw=np.array(
            [
                22_244 * 50_000,
                22_243 * 50_000,
                22_244 * 50_000,
                undefined,
                22_244 * 50_000,
                22_244 * 50_000,
                22_244 * 50_000,
                22_244 * 50_000,
            ],
            dtype=np.int64,
        ),
        bid_size=np.array([1, 1, 1, 0, 1, 0, 1, 1], dtype=np.int64),
        ask_size=np.ones(8, dtype=np.int64),
        selected_instrument_id=selected,
    )

    assert RESET_POLICY == "LAB_MINIMAL_NEXT_CLEAN_ROW_REARM_NOT_REPO_GOVERNED"
    assert mask.tolist() == [True, False, False, False, False, False, False, True]


class _SyntheticTradeDataset:
    def __init__(self) -> None:
        dates = (date(2022, 7, 1), date(2022, 7, 29), date(2022, 7, 31))
        self.plans = tuple(
            DayPlan(item, "6EU2", 44629, "0" * 64, Path(f"/{item}.parquet")) for item in dates
        )
        self._tapes = {
            dates[0]: TradeTape(
                physical_ts_ns=np.array([1, 2], dtype=np.int64),
                physical_price_ticks=np.array([21_072, 21_065], dtype=np.int64),
                bar_start_seconds=np.array([1, 2], dtype=np.int64),
                bar_close_ticks=np.array([21_072, 21_065], dtype=np.int64),
            )
        }
        decision = london_decision_epoch(dates[1], frozen_config())
        self._tapes[dates[1]] = TradeTape(
            physical_ts_ns=np.array([decision * 1_000_000_000 - 1], dtype=np.int64),
            physical_price_ticks=np.array([20_400], dtype=np.int64),
            bar_start_seconds=np.array([decision - 1], dtype=np.int64),
            bar_close_ticks=np.array([20_400], dtype=np.int64),
        )

    def trade_tape(self, source_date: date) -> TradeTape:
        return self._tapes[source_date]


def test_strict_signal_uses_frozen_plus60_lookup_and_calendar_weekday() -> None:
    signals = derive_strict_signals(_SyntheticTradeDataset(), frozen_config())  # type: ignore[arg-type]

    assert len(signals) == 1
    assert signals[0].event_date == date(2022, 7, 29)
    assert signals[0].month_open_ticks == 21_065
    assert signals[0].p15_ticks == 20_400
    assert signals[0].direction == 1
