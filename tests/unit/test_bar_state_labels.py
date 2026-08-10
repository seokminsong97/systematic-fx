from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, timedelta

from systematic_fx.research.bar_state_features import (
    MORPHOLOGY_FEATURE_NAMES,
    BarStateFeatureRow,
)
from systematic_fx.research.bar_state_labels import (
    StateCensorReason,
    StateOneSecondPathIndex,
    StatePathLabel,
    StateVerifiedEntryBar,
    label_bar_state_feature,
)


@dataclass(frozen=True)
class _Second:
    timeframe_seconds: int
    segment_id: int
    outcome_span_id: int
    contract: str
    source_date: date
    start_ns: int
    end_ns: int
    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int


@dataclass(frozen=True)
class _Entry:
    timeframe_seconds: int
    segment_id: int
    contract: str
    source_date: date
    start_ns: int
    end_ns: int
    first_trade_ns: int
    open_ticks: int


def _feature() -> BarStateFeatureRow:
    return BarStateFeatureRow(
        feature_set_id="MORPHOLOGY",
        feature_names=MORPHOLOGY_FEATURE_NAMES,
        timeframe_seconds=300,
        segment_id=7,
        contract="6EH2",
        source_date=date(2022, 1, 3),
        signal_start_ns=0,
        decision_ns=300_000_000_000,
        atr_true_range_sum_ticks=480,
        volatility_ticks=24,
        values=(0.0,) * 6,
    )


def _entry(*, gap_seconds: int = 0, segment_id: int = 7) -> StateVerifiedEntryBar:
    raw = _Entry(
        timeframe_seconds=300,
        segment_id=segment_id,
        contract="6EH2",
        source_date=date(2022, 1, 3),
        start_ns=300_000_000_000 + gap_seconds * 1_000_000_000,
        end_ns=600_000_000_000 + gap_seconds * 1_000_000_000,
        first_trade_ns=300_000_000_123 + gap_seconds * 1_000_000_000,
        open_ticks=1_000,
    )
    predecessor = _Entry(
        timeframe_seconds=300,
        segment_id=7,
        contract="6EH2",
        source_date=date(2022, 1, 3),
        start_ns=0,
        end_ns=300_000_000_000,
        first_trade_ns=123,
        open_ticks=1_000,
    )
    return StateVerifiedEntryBar.from_adjacent(
        predecessor,
        raw,
        predecessor_outcome_span_id=9,
        entry_outcome_span_id=9,
    )


def _calendar() -> tuple[date, ...]:
    first = date(2022, 1, 3)
    return tuple(first + timedelta(days=index) for index in range(20))


def _second(
    index: int,
    *,
    start_offset_seconds: int = 0,
    day: int = 0,
    segment_id: int = 7,
    high: int = 1_010,
    low: int = 990,
) -> _Second:
    start = 300_000_000_000 + start_offset_seconds * 1_000_000_000 + index * 1_000_000_000
    return _Second(
        timeframe_seconds=1,
        segment_id=segment_id,
        outcome_span_id=9,
        contract="6EH2",
        source_date=_calendar()[day],
        start_ns=start,
        end_ns=start + 1_000_000_000,
        open_ticks=1_000,
        high_ticks=high,
        low_ticks=low,
        close_ticks=1_000,
    )


def test_simultaneous_symmetric_touch_is_censored_not_directional() -> None:
    path = StateOneSecondPathIndex((_second(0, high=1_024, low=976),), path_id=9)
    label = label_bar_state_feature(
        _feature(),
        entry_bar=_entry(),
        path=path,
        active_dates=_calendar(),
    )

    assert label.label is StatePathLabel.CENSORED
    assert label.censor_reason is StateCensorReason.SIMULTANEOUS_AMBIGUOUS
    assert label.simultaneous_ambiguous is True


def test_normal_maintenance_segment_change_is_bridged_inside_verified_span() -> None:
    gap_seconds = 3_600
    bars = (
        _second(0, start_offset_seconds=gap_seconds, segment_id=8),
        _second(1, start_offset_seconds=gap_seconds, segment_id=9, day=1, high=1_025),
    )
    label = label_bar_state_feature(
        _feature(),
        entry_bar=_entry(gap_seconds=gap_seconds, segment_id=8),
        path=StateOneSecondPathIndex(bars, path_id=9),
        active_dates=_calendar(),
    )

    assert label.label is StatePathLabel.UP_FIRST
    assert label.upper_hit_path_index == 1
    assert label.entry_signal_bar_start_ns > label.decision_ns


def test_entry_successor_proof_cannot_cross_an_outcome_span() -> None:
    entry = replace(_entry(), outcome_span_id=10)
    try:
        label_bar_state_feature(
            _feature(),
            entry_bar=entry,
            path=StateOneSecondPathIndex((_second(0),), path_id=9),
            active_dates=_calendar(),
        )
    except ValueError as error:
        assert "outcome span" in str(error)
    else:  # pragma: no cover - fail-closed assertion
        raise AssertionError("cross-outcome-span entry was accepted")


def test_early_outcome_span_end_has_explicit_boundary_censor_reason() -> None:
    path = StateOneSecondPathIndex((_second(0),), path_id=9)
    label = label_bar_state_feature(
        _feature(),
        entry_bar=_entry(),
        path=path,
        active_dates=_calendar(),
    )

    assert label.path_truncated_before_horizon is True
    assert label.censor_reason is StateCensorReason.CONTRACT_OR_QUALITY_BOUNDARY


def test_first_touch_observed_before_later_span_end_remains_directional() -> None:
    path = StateOneSecondPathIndex((_second(0, high=1_025),), path_id=9)
    label = label_bar_state_feature(
        _feature(),
        entry_bar=_entry(),
        path=path,
        active_dates=_calendar(),
    )

    assert label.path_truncated_before_horizon is True
    assert label.label is StatePathLabel.UP_FIRST
    assert label.censor_reason is None


def test_path_rejects_forged_mixed_outcome_span_rows() -> None:
    first = _second(0)
    forged = replace(_second(1), outcome_span_id=10)
    try:
        StateOneSecondPathIndex((first, forged), path_id=9)
    except ValueError as error:
        assert "verified outcome span" in str(error)
    else:  # pragma: no cover - fail-closed assertion
        raise AssertionError("mixed outcome spans were accepted")
