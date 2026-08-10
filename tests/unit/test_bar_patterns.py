from __future__ import annotations

from dataclasses import dataclass, replace
from fractions import Fraction
from pathlib import Path

import pytest

from systematic_fx.backtest.barriers import Direction
from systematic_fx.research.bar_config import frozen_bar_pattern_candidates
from systematic_fx.research.bar_patterns import (
    BarPatternError,
    build_bar_pattern_context,
    evaluate_bar_pattern,
)

ROOT = Path(__file__).resolve().parents[2]
TIMEFRAME_SECONDS = 300
TIMEFRAME_NS = TIMEFRAME_SECONDS * 1_000_000_000
TRIGGER_INDEX = 22
PRICE_ORIGIN = 10_000


@dataclass(frozen=True, slots=True)
class _Bar:
    timeframe_seconds: int
    segment_id: int
    contract: str
    start_ns: int
    end_ns: int
    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int


_FAMILY_BARS = {
    "F1": ((100, 105, 99, 104), (104, 108, 103, 108)),
    "F2": ((100, 103, 99, 102), (101, 104, 100, 103)),
    "F3": ((104, 105, 99, 100), (98, 104, 94, 103)),
    "F4": ((104, 105, 99, 100), (100, 105, 99, 104)),
    "F5": ((100, 102, 99, 101), (101, 105, 100, 104)),
    "F6": ((100, 102, 98, 100), (98, 100, 97, 100)),
}


def _bar(index: int, directed: tuple[int, int, int, int], direction: Direction) -> _Bar:
    open_value, high_value, low_value, close_value = directed
    if direction is Direction.LONG:
        raw_open = PRICE_ORIGIN + open_value
        raw_high = PRICE_ORIGIN + high_value
        raw_low = PRICE_ORIGIN + low_value
        raw_close = PRICE_ORIGIN + close_value
    else:
        raw_open = PRICE_ORIGIN - open_value
        raw_high = PRICE_ORIGIN - low_value
        raw_low = PRICE_ORIGIN - high_value
        raw_close = PRICE_ORIGIN - close_value
    start_ns = index * TIMEFRAME_NS
    return _Bar(
        timeframe_seconds=TIMEFRAME_SECONDS,
        segment_id=1,
        contract="6EH4",
        start_ns=start_ns,
        end_ns=start_ns + TIMEFRAME_NS,
        open_ticks=raw_open,
        high_ticks=raw_high,
        low_ticks=raw_low,
        close_ticks=raw_close,
    )


def _bars(family_id: str, direction: Direction) -> tuple[_Bar, ...]:
    warmup = (100, 102, 98, 100)
    setup, trigger = _FAMILY_BARS[family_id]
    directed = [warmup] * 21 + [setup, trigger, (100, 102, 98, 100)]
    return tuple(_bar(index, item, direction) for index, item in enumerate(directed))


def _candidate(family_id: str, direction: Direction):
    return next(
        candidate
        for candidate in frozen_bar_pattern_candidates()
        if candidate.timeframe_seconds == TIMEFRAME_SECONDS
        and candidate.setup_lookback_bars == 1
        and candidate.family.family_id == family_id
        and candidate.direction is direction
    )


@pytest.mark.parametrize("family_id", ["F1", "F2", "F3", "F4", "F5", "F6"])
@pytest.mark.parametrize("direction", [Direction.LONG, Direction.SHORT])
def test_each_frozen_family_matches_its_mirrored_integer_fixture(
    family_id: str,
    direction: Direction,
) -> None:
    evaluation = evaluate_bar_pattern(
        _bars(family_id, direction),
        trigger_index=TRIGGER_INDEX,
        candidate=_candidate(family_id, direction),
    )

    assert evaluation.matched
    assert evaluation.failed_gate_ids == ()
    assert evaluation.context.setup_start_index == 21
    assert evaluation.context.setup_end_index == 21
    assert evaluation.context.trigger_index == 22
    assert evaluation.context.entry_index == 23
    assert evaluation.context.decision_ns == evaluation.context.next_open.start_ns
    assert [gate.gate_id for gate in evaluation.gates] == [
        gate_id for gate_id, _ in evaluation.candidate.family.gates
    ]


def test_metrics_are_exact_rationals_at_registered_boundaries() -> None:
    bars = list(_bars("F1", Direction.LONG))
    bars[21] = _bar(21, (100, 103, 99, 103), Direction.LONG)
    evaluation = evaluate_bar_pattern(
        bars,
        trigger_index=TRIGGER_INDEX,
        candidate=_candidate("F1", Direction.LONG),
    )

    assert evaluation.context.metrics.r_setup_move_over_atr == Fraction(3, 4)
    assert evaluation.matched
    assert evaluation.context.metrics.as_dict()["r_setup_move_over_atr"] == {
        "denominator": 4,
        "numerator": 3,
    }

    bars[21] = _bar(21, (100, 102, 99, 102), Direction.LONG)
    failed = evaluate_bar_pattern(
        bars,
        trigger_index=TRIGGER_INDEX,
        candidate=_candidate("F1", Direction.LONG),
    )
    assert not failed.matched
    assert "R_GE_3_4" in failed.failed_gate_ids


def test_context_records_every_source_and_derived_variable_without_entry_bar_future() -> None:
    bars = list(_bars("F2", Direction.LONG))
    bars[23] = replace(bars[23], high_ticks=-1, low_ticks=-2, close_ticks=-3)

    context = build_bar_pattern_context(
        bars,
        trigger_index=TRIGGER_INDEX,
        setup_lookback_bars=1,
        direction=Direction.LONG,
    )
    payload = context.as_dict()

    assert len(payload["atr_bars"]) == 20
    assert len(payload["setup_bars"]) == 1
    assert set(payload["next_open"]) == {"open_ticks", "source_index", "start_ns"}
    assert payload["next_open"]["source_index"] == 23
    assert payload["history_previous_bar"]["source_index"] == 0
    assert context.metrics.r_setup_move_over_atr == Fraction(1, 2)
    assert context.metrics.e_setup_efficiency == Fraction(1, 2)


def test_matching_trigger_without_next_bar_is_retained_as_unfilled() -> None:
    bars = _bars("F1", Direction.LONG)[:-1]

    evaluation = evaluate_bar_pattern(
        bars,
        trigger_index=len(bars) - 1,
        candidate=_candidate("F1", Direction.LONG),
    )

    assert evaluation.matched is True
    assert evaluation.context.next_open is None
    assert evaluation.context.entry_index is None


def test_context_rejects_missing_bars_and_contract_or_quality_resets() -> None:
    bars = list(_bars("F1", Direction.LONG))
    bars[10] = replace(
        bars[10],
        start_ns=bars[10].start_ns + 1,
        end_ns=bars[10].end_ns + 1,
    )
    with pytest.raises(BarPatternError, match="missing or overlapping"):
        evaluate_bar_pattern(
            bars,
            trigger_index=TRIGGER_INDEX,
            candidate=_candidate("F1", Direction.LONG),
        )

    bars = list(_bars("F1", Direction.LONG))
    bars[23] = replace(bars[23], segment_id=2)
    evaluation = evaluate_bar_pattern(
        bars,
        trigger_index=TRIGGER_INDEX,
        candidate=_candidate("F1", Direction.LONG),
    )
    assert evaluation.context.next_open is None

    bars = list(_bars("F1", Direction.LONG))
    bars[20] = replace(bars[20], segment_id=2)
    with pytest.raises(BarPatternError, match="contract or quality segment"):
        evaluate_bar_pattern(
            bars,
            trigger_index=TRIGGER_INDEX,
            candidate=_candidate("F1", Direction.LONG),
        )


def test_context_rejects_zero_denominators_and_insufficient_history() -> None:
    flat = tuple(_bar(index, (100, 100, 100, 100), Direction.LONG) for index in range(24))
    with pytest.raises(BarPatternError, match="ATR denominator is zero"):
        evaluate_bar_pattern(
            flat,
            trigger_index=TRIGGER_INDEX,
            candidate=_candidate("F1", Direction.LONG),
        )

    with pytest.raises(BarPatternError, match="insufficient completed history"):
        evaluate_bar_pattern(
            _bars("F1", Direction.LONG),
            trigger_index=21,
            candidate=_candidate("F1", Direction.LONG),
        )


@pytest.mark.parametrize("lookback", [1, 2, 3, 4, 6, 12])
def test_atr_setup_trigger_and_entry_windows_are_exact_for_every_setup_length(
    lookback: int,
) -> None:
    trigger_index = 21 + lookback
    bars = tuple(
        _bar(index, (100, 102, 98, 100), Direction.LONG) for index in range(trigger_index + 2)
    )

    context = build_bar_pattern_context(
        bars,
        trigger_index=trigger_index,
        setup_lookback_bars=lookback,
        direction=Direction.LONG,
    )

    assert context.history_previous_bar.source_index == 0
    assert [bar.source_index for bar in context.atr_bars] == list(range(1, 21))
    assert [bar.source_index for bar in context.setup_bars] == list(range(21, 21 + lookback))
    assert context.trigger_index == 21 + lookback
    assert context.entry_index == 22 + lookback
