from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from fractions import Fraction

from systematic_fx.research.bar_state_config import frozen_bar_state_candidates
from systematic_fx.research.bar_state_features import (
    BarStateFeatureSpec,
    build_bar_state_features,
)


@dataclass(frozen=True)
class _Bar:
    timeframe_seconds: int
    segment_id: int
    contract: str
    source_date: date
    start_ns: int
    end_ns: int
    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int
    trade_count: int = 10
    volume: int = 20
    buy_volume: int | None = 11
    sell_volume: int | None = 9


def _bars(timeframe: int, count: int, *, start_seconds: int = 0) -> tuple[_Bar, ...]:
    width = timeframe * 1_000_000_000
    start = start_seconds * 1_000_000_000
    values = []
    for index in range(count):
        open_ticks = 20_000 + index * 2
        close_ticks = open_ticks + (1 if index % 2 == 0 else -1)
        values.append(
            _Bar(
                timeframe_seconds=timeframe,
                segment_id=1,
                contract="6EH2",
                source_date=date(2022, 1, 3),
                start_ns=start + index * width,
                end_ns=start + (index + 1) * width,
                open_ticks=open_ticks,
                high_ticks=max(open_ticks, close_ticks) + 3 + index % 3,
                low_ticks=min(open_ticks, close_ticks) - 2,
                close_ticks=close_ticks,
                trade_count=10 + index,
                volume=20 + index * 2,
            )
        )
    return tuple(values)


def test_config_candidate_feature_contract_is_exact_and_uppercase() -> None:
    for candidate in frozen_bar_state_candidates():
        spec = BarStateFeatureSpec(
            timeframe_seconds=candidate.timeframe_seconds,
            feature_set_id=candidate.feature_set.feature_set_id,
            feature_names=candidate.feature_set.feature_ids,
        )
        assert (
            BarStateFeatureSpec.frozen(
                candidate.timeframe_seconds,
                candidate.feature_set.feature_set_id,
            )
            == spec
        )
        assert len(spec.feature_names) <= 40


def test_feature_rows_are_causal_and_use_single_exact_fraction_conversion() -> None:
    bars = _bars(300, 23)
    spec = BarStateFeatureSpec.frozen(300, "MORPHOLOGY")
    original = build_bar_state_features(bars, spec=spec)
    mutated = build_bar_state_features(
        (*bars[:-1], replace(bars[-1], high_ticks=bars[-1].high_ticks + 100)),
        spec=spec,
    )

    assert original[:-1] == mutated[:-1]
    row = original[0]
    expected = float(
        Fraction(
            (bars[20].close_ticks - bars[19].close_ticks) * 20,
            row.atr_true_range_sum_ticks,
        )
    )
    assert row.feature_map["ret_1"].hex() == expected.hex()
    assert row.decision_ns == bars[20].end_ns


def test_5m_state_suppresses_rows_until_causal_30m_atr_return_exists() -> None:
    spec = BarStateFeatureSpec.frozen(300, "STATE")
    early = _bars(300, 25)
    exclusions = []
    assert (
        build_bar_state_features(
            early,
            spec=spec,
            completed_30m_bars=(),
            exclusion_sink=exclusions.append,
        )
        == ()
    )
    assert {item.reason for item in exclusions} == {"MISSING_CAUSALLY_COMPLETED_30M_ATR_RETURN"}

    raw_30m = _bars(1_800, 22)
    completed_30m = (
        *raw_30m[:-1],
        replace(
            raw_30m[-1],
            close_ticks=raw_30m[-1].close_ticks + 4,
            high_ticks=raw_30m[-1].high_ticks + 4,
        ),
    )
    start_seconds = 22 * 1_800
    later = _bars(300, 22, start_seconds=start_seconds)
    rows = build_bar_state_features(
        later,
        spec=spec,
        completed_30m_bars=completed_30m,
    )
    assert len(rows) == 2
    assert rows[0].feature_map["higher_tf_ret_1"] != 0.0
