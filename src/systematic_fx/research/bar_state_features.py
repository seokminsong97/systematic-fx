"""Causal, bounded feature extraction for bar state-model research.

Every row is available only after its signal bar has closed.  Rolling windows
are reset at the canonical trade-bar segment boundary, and the 5-minute
higher-timeframe feature uses only 30-minute bars whose ``end_ns`` is no later
than the decision timestamp.  These rules are deliberately independent of a
research split so the same immutable feature row can be reused by every fold.
"""

from __future__ import annotations

import math
from collections import deque
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import date
from fractions import Fraction
from typing import Final, Protocol

FEATURE_SCHEMA: Final = "systematic_fx.bar_state_feature.v1"
SUPPORTED_STATE_TIMEFRAMES_SECONDS: Final = (300, 1_800)
ATR_LOOKBACK_BARS: Final = 20
MAX_STATE_FEATURE_COUNT: Final = 40
VOLATILITY_ROUND_TICKS: Final = 8
MIN_VOLATILITY_TICKS: Final = 24
MAX_VOLATILITY_TICKS: Final = 192

MORPHOLOGY_FEATURE_NAMES: Final = (
    "ret_1",
    "body_atr",
    "range_atr",
    "upper_wick_atr",
    "lower_wick_atr",
    "close_location",
)
STATE_FEATURE_NAMES: Final = MORPHOLOGY_FEATURE_NAMES + (
    "ret_3",
    "ret_6",
    "trend_6_atr",
    "realized_range_6",
    "atr_ratio_5_20",
    "volume_z20",
    "trade_count_z20",
    "buy_imbalance",
    "tod_sin",
    "tod_cos",
    "gap_from_prev_atr",
    "higher_tf_ret_1",
)
FEATURE_NAMES_BY_SET: Final = {
    "MORPHOLOGY": MORPHOLOGY_FEATURE_NAMES,
    "STATE": STATE_FEATURE_NAMES,
}


class BarStateFeatureError(ValueError):
    """Feature inputs or a derived point-in-time value are invalid."""


class StateBarLike(Protocol):
    """Fields required from the verified canonical trade-bar layer."""

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
    trade_count: int
    volume: int
    buy_volume: int | None
    sell_volume: int | None


@dataclass(frozen=True, slots=True)
class BarStateFeatureSpec:
    """Frozen feature-set choice for one timeframe."""

    timeframe_seconds: int
    feature_set_id: str
    feature_names: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.timeframe_seconds not in SUPPORTED_STATE_TIMEFRAMES_SECONDS:
            raise BarStateFeatureError("state features support only 5m and 30m bars")
        expected = FEATURE_NAMES_BY_SET.get(self.feature_set_id)
        if expected is None:
            raise BarStateFeatureError("feature_set_id must be MORPHOLOGY or STATE")
        if self.feature_names != expected:
            raise BarStateFeatureError("feature_names differ from the frozen feature set")
        if not self.feature_names or len(self.feature_names) > MAX_STATE_FEATURE_COUNT:
            raise BarStateFeatureError("feature count is outside the frozen bound")
        if len(set(self.feature_names)) != len(self.feature_names):
            raise BarStateFeatureError("feature_names must be unique")

    @classmethod
    def frozen(cls, timeframe_seconds: int, feature_set_id: str) -> BarStateFeatureSpec:
        try:
            names = FEATURE_NAMES_BY_SET[feature_set_id]
        except KeyError as error:
            raise BarStateFeatureError("feature_set_id must be MORPHOLOGY or STATE") from error
        return cls(timeframe_seconds, feature_set_id, names)

    def as_dict(self) -> dict[str, object]:
        return {
            "atr_lookback_bars": ATR_LOOKBACK_BARS,
            "feature_names": list(self.feature_names),
            "feature_schema": FEATURE_SCHEMA,
            "feature_set_id": self.feature_set_id,
            "higher_timeframe_policy": (
                "LAST_CAUSALLY_COMPLETED_30M"
                if self.timeframe_seconds == 300
                else "ZERO_SENTINEL_FOR_30M"
            ),
            "max_feature_count": MAX_STATE_FEATURE_COUNT,
            "timeframe_seconds": self.timeframe_seconds,
        }


@dataclass(frozen=True, slots=True)
class BarStateFeatureRow:
    """One immutable, split-independent feature vector at a bar close."""

    feature_set_id: str
    feature_names: tuple[str, ...]
    timeframe_seconds: int
    segment_id: int
    contract: str
    source_date: date
    signal_start_ns: int
    decision_ns: int
    atr_true_range_sum_ticks: int
    volatility_ticks: int
    values: tuple[float, ...]

    def __post_init__(self) -> None:
        expected = FEATURE_NAMES_BY_SET.get(self.feature_set_id)
        if self.feature_names != expected:
            raise BarStateFeatureError("row feature_names differ from its frozen feature set")
        if self.timeframe_seconds not in SUPPORTED_STATE_TIMEFRAMES_SECONDS:
            raise BarStateFeatureError("row has an unsupported timeframe")
        if self.segment_id <= 0 or not self.contract:
            raise BarStateFeatureError("row segment identity is invalid")
        if self.signal_start_ns < 0 or self.decision_ns <= self.signal_start_ns:
            raise BarStateFeatureError("row decision interval is invalid")
        if self.atr_true_range_sum_ticks <= 0:
            raise BarStateFeatureError("row ATR sum must be positive")
        if (
            self.volatility_ticks < MIN_VOLATILITY_TICKS
            or self.volatility_ticks > MAX_VOLATILITY_TICKS
            or self.volatility_ticks % VOLATILITY_ROUND_TICKS
        ):
            raise BarStateFeatureError("row volatility is outside the frozen tick grid")
        if len(self.values) != len(self.feature_names):
            raise BarStateFeatureError("row value count differs from feature_names")
        if any(not math.isfinite(value) for value in self.values):
            raise BarStateFeatureError("row values must be finite")

    @property
    def feature_map(self) -> dict[str, float]:
        return dict(zip(self.feature_names, self.values, strict=True))

    @property
    def exact_atr_ticks(self) -> Fraction:
        return Fraction(self.atr_true_range_sum_ticks, ATR_LOOKBACK_BARS)

    def as_dict(self) -> dict[str, object]:
        return {
            "atr_lookback_bars": ATR_LOOKBACK_BARS,
            "atr_true_range_sum_ticks": self.atr_true_range_sum_ticks,
            "contract": self.contract,
            "decision_ns": self.decision_ns,
            "feature_names": list(self.feature_names),
            "feature_schema": FEATURE_SCHEMA,
            "feature_set_id": self.feature_set_id,
            "segment_id": self.segment_id,
            "signal_start_ns": self.signal_start_ns,
            "source_date": self.source_date.isoformat(),
            "timeframe_seconds": self.timeframe_seconds,
            "values_hex": [value.hex() for value in self.values],
            "volatility_ticks": self.volatility_ticks,
        }


@dataclass(frozen=True, slots=True)
class BarStateFeatureExclusion:
    """One auditable otherwise-eligible row suppressed by a frozen rule."""

    reason: str
    timeframe_seconds: int
    segment_id: int
    contract: str
    signal_start_ns: int
    decision_ns: int

    def __post_init__(self) -> None:
        if self.reason not in {"MISSING_CAUSALLY_COMPLETED_30M_ATR_RETURN", "ZERO_ATR"}:
            raise BarStateFeatureError("unknown feature exclusion reason")


def round_half_up_fraction(value: Fraction, quantum: int) -> int:
    """Round a non-negative rational to the nearest quantum, ties upward."""

    if not isinstance(value, Fraction) or value < 0:
        raise BarStateFeatureError("rounded value must be a non-negative Fraction")
    if isinstance(quantum, bool) or not isinstance(quantum, int) or quantum <= 0:
        raise BarStateFeatureError("rounding quantum must be a positive integer")
    units = value / quantum
    rounded_units = (2 * units.numerator + units.denominator) // (2 * units.denominator)
    return rounded_units * quantum


def volatility_ticks_from_atr_sum(atr_true_range_sum_ticks: int) -> int:
    """Return the frozen 8-tick, 24..192 point-in-time ATR proxy."""

    if (
        isinstance(atr_true_range_sum_ticks, bool)
        or not isinstance(atr_true_range_sum_ticks, int)
        or atr_true_range_sum_ticks <= 0
    ):
        raise BarStateFeatureError("ATR sum must be a positive integer")
    rounded = round_half_up_fraction(
        Fraction(atr_true_range_sum_ticks, ATR_LOOKBACK_BARS),
        VOLATILITY_ROUND_TICKS,
    )
    return min(MAX_VOLATILITY_TICKS, max(MIN_VOLATILITY_TICKS, rounded))


def _validate_bar(
    bar: StateBarLike, *, expected_timeframe: int, previous_start: int | None
) -> None:
    if bar.timeframe_seconds != expected_timeframe:
        raise BarStateFeatureError("bar timeframe differs from the feature specification")
    if bar.segment_id <= 0 or not bar.contract:
        raise BarStateFeatureError("bar segment identity is invalid")
    if bar.start_ns < 0 or bar.end_ns <= bar.start_ns:
        raise BarStateFeatureError("bar interval is invalid")
    if previous_start is not None and bar.start_ns <= previous_start:
        raise BarStateFeatureError("bars must be strictly ordered by start_ns")
    if bar.low_ticks > min(bar.open_ticks, bar.close_ticks):
        raise BarStateFeatureError("bar low exceeds open or close")
    if bar.high_ticks < max(bar.open_ticks, bar.close_ticks):
        raise BarStateFeatureError("bar high is below open or close")
    if min(bar.low_ticks, bar.open_ticks, bar.close_ticks, bar.high_ticks) <= 0:
        raise BarStateFeatureError("bar prices must be positive")
    if bar.trade_count <= 0 or bar.volume < 0:
        raise BarStateFeatureError("bar activity fields are invalid")


def _true_range(current: StateBarLike, previous: StateBarLike) -> int:
    return max(
        current.high_ticks - current.low_ticks,
        abs(current.high_ticks - previous.close_ticks),
        abs(current.low_ticks - previous.close_ticks),
    )


def _population_z(value: int, window: Sequence[int]) -> float:
    mean = sum(window) / len(window)
    variance = sum((item - mean) ** 2 for item in window) / len(window)
    if variance == 0:
        return 0.0
    return (value - mean) / math.sqrt(variance)


def _atr_normalized(delta_ticks: int, atr_sum_ticks: int) -> float:
    return float(Fraction(delta_ticks * ATR_LOOKBACK_BARS, atr_sum_ticks))


def _ols_slope(values: Sequence[int]) -> Fraction:
    count = len(values)
    x_mean = Fraction(count - 1, 2)
    y_mean = Fraction(sum(values), count)
    denominator = sum((Fraction(index) - x_mean) ** 2 for index in range(count))
    return (
        sum((Fraction(index) - x_mean) * (value - y_mean) for index, value in enumerate(values))
        / denominator
    )


def _higher_timeframe_returns(
    bars_30m: Sequence[StateBarLike],
) -> tuple[tuple[int, int, str, float], ...]:
    """Build (end, segment, contract, prior-ATR-normalized return) records."""

    records: list[tuple[int, int, str, float]] = []
    window: deque[StateBarLike] = deque(maxlen=ATR_LOOKBACK_BARS + 1)
    active_key: tuple[int, str] | None = None
    previous_start: int | None = None
    for bar in bars_30m:
        _validate_bar(bar, expected_timeframe=1_800, previous_start=previous_start)
        previous_start = bar.start_ns
        key = bar.segment_id, bar.contract
        if key != active_key:
            active_key = key
            window.clear()
        window.append(bar)
        if len(window) != ATR_LOOKBACK_BARS + 1:
            continue
        values = tuple(window)
        atr_sum = sum(_true_range(values[index], values[index - 1]) for index in range(1, 21))
        if atr_sum <= 0:
            normalized_return = 0.0
        else:
            normalized_return = float(
                Fraction(
                    (values[-1].close_ticks - values[-2].close_ticks) * ATR_LOOKBACK_BARS,
                    atr_sum,
                )
            )
        records.append((bar.end_ns, bar.segment_id, bar.contract, normalized_return))
    return tuple(records)


def _higher_return_at(
    records: Sequence[tuple[int, int, str, float]],
    *,
    decision_ns: int,
    segment_id: int,
    contract: str,
    cursor: int,
) -> tuple[float | None, int]:
    while cursor < len(records) and records[cursor][0] <= decision_ns:
        cursor += 1
    for index in range(cursor - 1, -1, -1):
        end_ns, record_segment, record_contract, value = records[index]
        if end_ns > decision_ns:
            continue
        if record_segment == segment_id and record_contract == contract:
            return value, cursor
        # Segment identifiers are monotone in canonical data, so an older
        # different segment cannot become a match after this point.
        if record_segment < segment_id:
            break
    return None, cursor


def iter_bar_state_features(
    bars: Sequence[StateBarLike],
    *,
    spec: BarStateFeatureSpec,
    completed_30m_bars: Sequence[StateBarLike] = (),
    exclusion_sink: Callable[[BarStateFeatureExclusion], None] | None = None,
) -> Iterator[BarStateFeatureRow]:
    """Yield bounded causal feature rows in signal-bar time order.

    The ATR denominator is the exact rational mean of the 20 true ranges
    ending at the decision bar.  A 21-bar same-segment window is therefore
    required.  ``completed_30m_bars`` is ignored for a 30-minute specification
    and is only consulted at ``end_ns <= decision_ns`` for 5-minute rows.
    """

    if not isinstance(spec, BarStateFeatureSpec):
        raise BarStateFeatureError("spec must be BarStateFeatureSpec")
    higher_records = (
        _higher_timeframe_returns(completed_30m_bars)
        if spec.timeframe_seconds == 300 and spec.feature_set_id == "STATE"
        else ()
    )
    higher_cursor = 0
    active_key: tuple[int, str] | None = None
    window: deque[StateBarLike] = deque(maxlen=ATR_LOOKBACK_BARS + 1)
    previous_start: int | None = None
    for bar in bars:
        _validate_bar(
            bar,
            expected_timeframe=spec.timeframe_seconds,
            previous_start=previous_start,
        )
        previous_start = bar.start_ns
        key = bar.segment_id, bar.contract
        if key != active_key:
            active_key = key
            window.clear()
        window.append(bar)
        if len(window) != ATR_LOOKBACK_BARS + 1:
            continue
        values = tuple(window)
        current = values[-1]
        previous = values[-2]
        true_ranges = tuple(_true_range(values[index], values[index - 1]) for index in range(1, 21))
        atr_sum = sum(true_ranges)
        if atr_sum <= 0:
            if exclusion_sink is not None:
                exclusion_sink(
                    BarStateFeatureExclusion(
                        reason="ZERO_ATR",
                        timeframe_seconds=current.timeframe_seconds,
                        segment_id=current.segment_id,
                        contract=current.contract,
                        signal_start_ns=current.start_ns,
                        decision_ns=current.end_ns,
                    )
                )
            continue
        current_range = current.high_ticks - current.low_ticks
        feature_values: dict[str, float] = {
            "ret_1": _atr_normalized(current.close_ticks - previous.close_ticks, atr_sum),
            "body_atr": _atr_normalized(current.close_ticks - current.open_ticks, atr_sum),
            "range_atr": _atr_normalized(current_range, atr_sum),
            "upper_wick_atr": _atr_normalized(
                current.high_ticks - max(current.open_ticks, current.close_ticks), atr_sum
            ),
            "lower_wick_atr": _atr_normalized(
                min(current.open_ticks, current.close_ticks) - current.low_ticks, atr_sum
            ),
            "close_location": (
                0.5
                if current_range == 0
                else float(Fraction(current.close_ticks - current.low_ticks, current_range))
            ),
        }
        if spec.feature_set_id == "STATE":
            closes_6 = tuple(item.close_ticks for item in values[-6:])
            volume_window = tuple(item.volume for item in values[-20:])
            trade_window = tuple(item.trade_count for item in values[-20:])
            buy = current.buy_volume
            sell = current.sell_volume
            if buy is None or sell is None or buy + sell == 0:
                imbalance = 0.0
            else:
                imbalance = (buy - sell) / (buy + sell)
            seconds = (current.start_ns // 1_000_000_000) % 86_400
            angle = 2 * math.pi * seconds / 86_400
            higher_return: float | None = 0.0
            if spec.timeframe_seconds == 300:
                higher_return, higher_cursor = _higher_return_at(
                    higher_records,
                    decision_ns=current.end_ns,
                    segment_id=current.segment_id,
                    contract=current.contract,
                    cursor=higher_cursor,
                )
                if higher_return is None:
                    if exclusion_sink is not None:
                        exclusion_sink(
                            BarStateFeatureExclusion(
                                reason="MISSING_CAUSALLY_COMPLETED_30M_ATR_RETURN",
                                timeframe_seconds=current.timeframe_seconds,
                                segment_id=current.segment_id,
                                contract=current.contract,
                                signal_start_ns=current.start_ns,
                                decision_ns=current.end_ns,
                            )
                        )
                    continue
            feature_values.update(
                {
                    "ret_3": _atr_normalized(current.close_ticks - values[-4].close_ticks, atr_sum),
                    "ret_6": _atr_normalized(current.close_ticks - values[-7].close_ticks, atr_sum),
                    "trend_6_atr": float(_ols_slope(closes_6) * ATR_LOOKBACK_BARS / atr_sum),
                    "realized_range_6": _atr_normalized(
                        max(item.high_ticks for item in values[-6:])
                        - min(item.low_ticks for item in values[-6:]),
                        atr_sum,
                    ),
                    "atr_ratio_5_20": float(Fraction(sum(true_ranges[-5:]) * 4, atr_sum)),
                    "volume_z20": _population_z(current.volume, volume_window),
                    "trade_count_z20": _population_z(current.trade_count, trade_window),
                    "buy_imbalance": imbalance,
                    "tod_sin": math.sin(angle),
                    "tod_cos": math.cos(angle),
                    "gap_from_prev_atr": _atr_normalized(
                        current.open_ticks - previous.close_ticks, atr_sum
                    ),
                    "higher_tf_ret_1": higher_return,
                }
            )
        yield BarStateFeatureRow(
            feature_set_id=spec.feature_set_id,
            feature_names=spec.feature_names,
            timeframe_seconds=spec.timeframe_seconds,
            segment_id=current.segment_id,
            contract=current.contract,
            source_date=current.source_date,
            signal_start_ns=current.start_ns,
            decision_ns=current.end_ns,
            atr_true_range_sum_ticks=atr_sum,
            volatility_ticks=volatility_ticks_from_atr_sum(atr_sum),
            values=tuple(float(feature_values[name]) for name in spec.feature_names),
        )


def build_bar_state_features(
    bars: Sequence[StateBarLike],
    *,
    spec: BarStateFeatureSpec,
    completed_30m_bars: Sequence[StateBarLike] = (),
    exclusion_sink: Callable[[BarStateFeatureExclusion], None] | None = None,
) -> tuple[BarStateFeatureRow, ...]:
    """Materialize the feature iterator for small tests or artifact writers."""

    return tuple(
        iter_bar_state_features(
            bars,
            spec=spec,
            completed_30m_bars=completed_30m_bars,
            exclusion_sink=exclusion_sink,
        )
    )


__all__ = [
    "ATR_LOOKBACK_BARS",
    "FEATURE_NAMES_BY_SET",
    "FEATURE_SCHEMA",
    "MAX_STATE_FEATURE_COUNT",
    "MAX_VOLATILITY_TICKS",
    "MIN_VOLATILITY_TICKS",
    "MORPHOLOGY_FEATURE_NAMES",
    "STATE_FEATURE_NAMES",
    "SUPPORTED_STATE_TIMEFRAMES_SECONDS",
    "VOLATILITY_ROUND_TICKS",
    "BarStateFeatureError",
    "BarStateFeatureExclusion",
    "BarStateFeatureRow",
    "BarStateFeatureSpec",
    "build_bar_state_features",
    "iter_bar_state_features",
    "round_half_up_fraction",
    "volatility_ticks_from_atr_sum",
]
