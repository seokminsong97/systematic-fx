"""Exact, point-in-time evaluation of the six frozen v1 OHLC pattern families.

For trigger index ``t`` and setup length ``L``, formulas may read only the
20-bar ATR window ending at ``t-L-1``, setup bars ``t-L..t-1``, and trigger bar
``t``.  The returned entry reference exposes only the open of ``t+1``.  Every
ratio is a :class:`fractions.Fraction`; binary floating point and fitted
thresholds are absent from this module.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from fractions import Fraction
from typing import Final, Protocol

from systematic_fx.backtest.barriers import Direction
from systematic_fx.research.bar_config import (
    ATR_LOOKBACK_BARS,
    PATTERN_FAMILY_BY_ID,
    SETUP_LOOKBACK_BARS,
    SIGNAL_TIMEFRAMES_SECONDS,
    BarPatternCandidate,
    PatternFamilySpec,
)

NANOSECONDS_PER_SECOND: Final = 1_000_000_000


class BarPatternError(ValueError):
    """A candidate or bar context violates the frozen point-in-time contract."""


class PatternBarLike(Protocol):
    """Neutral OHLC fields consumed from the canonical selected-contract bars."""

    timeframe_seconds: int
    segment_id: int
    contract: str
    start_ns: int
    end_ns: int
    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int


def _integer(value: object, *, label: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise BarPatternError(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        raise BarPatternError(f"{label} must be at least {minimum}")
    return value


def _ratio_payload(value: Fraction) -> dict[str, int]:
    return {"denominator": value.denominator, "numerator": value.numerator}


@dataclass(frozen=True, slots=True)
class PatternBarSnapshot:
    """All raw and direction-transformed inputs read from one completed bar."""

    source_index: int
    start_ns: int
    end_ns: int
    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int
    directional_open_ticks: int
    directional_high_ticks: int
    directional_low_ticks: int
    directional_close_ticks: int
    true_range_ticks: int | None

    def as_dict(self) -> dict[str, int | None]:
        return {
            "close_ticks": self.close_ticks,
            "directional_close_ticks": self.directional_close_ticks,
            "directional_high_ticks": self.directional_high_ticks,
            "directional_low_ticks": self.directional_low_ticks,
            "directional_open_ticks": self.directional_open_ticks,
            "end_ns": self.end_ns,
            "high_ticks": self.high_ticks,
            "low_ticks": self.low_ticks,
            "open_ticks": self.open_ticks,
            "source_index": self.source_index,
            "start_ns": self.start_ns,
            "true_range_ticks": self.true_range_ticks,
        }


@dataclass(frozen=True, slots=True)
class NextOpenReference:
    """The only field exposed from the future entry bar: its opening price."""

    source_index: int
    start_ns: int
    open_ticks: int

    def as_dict(self) -> dict[str, int]:
        return {
            "open_ticks": self.open_ticks,
            "source_index": self.source_index,
            "start_ns": self.start_ns,
        }


@dataclass(frozen=True, slots=True)
class BarPatternMetrics:
    """Every derived integer or exact rational available to a family gate."""

    atr_period_bars: int
    atr_true_range_sum_ticks: int
    setup_true_range_sum_ticks: int
    setup_median_true_range_ticks: Fraction
    setup_directional_move_ticks: int
    setup_directional_high_ticks: int
    setup_directional_low_ticks: int
    trigger_true_range_ticks: int
    trigger_directional_range_ticks: int
    trigger_directional_body_ticks: int
    prior_directional_body_absolute_ticks: int
    r_setup_move_over_atr: Fraction
    e_setup_efficiency: Fraction
    w_setup_width_over_atr: Fraction
    v_setup_median_range_over_atr: Fraction
    x_trigger_range_over_atr: Fraction
    b_trigger_body_fraction: Fraction
    q_trigger_close_location: Fraction
    k_trigger_lower_wick_fraction: Fraction
    p_pullback_depth_over_atr: Fraction
    j_breakout_distance_over_atr: Fraction
    n_failed_break_distance_over_atr: Fraction
    z_failed_break_recovery_over_atr: Fraction

    def as_dict(self) -> dict[str, object]:
        return {
            "atr_period_bars": self.atr_period_bars,
            "atr_true_range_sum_ticks": self.atr_true_range_sum_ticks,
            "b_trigger_body_fraction": _ratio_payload(self.b_trigger_body_fraction),
            "e_setup_efficiency": _ratio_payload(self.e_setup_efficiency),
            "j_breakout_distance_over_atr": _ratio_payload(self.j_breakout_distance_over_atr),
            "k_trigger_lower_wick_fraction": _ratio_payload(self.k_trigger_lower_wick_fraction),
            "n_failed_break_distance_over_atr": _ratio_payload(
                self.n_failed_break_distance_over_atr
            ),
            "p_pullback_depth_over_atr": _ratio_payload(self.p_pullback_depth_over_atr),
            "prior_directional_body_absolute_ticks": (self.prior_directional_body_absolute_ticks),
            "q_trigger_close_location": _ratio_payload(self.q_trigger_close_location),
            "r_setup_move_over_atr": _ratio_payload(self.r_setup_move_over_atr),
            "setup_directional_high_ticks": self.setup_directional_high_ticks,
            "setup_directional_low_ticks": self.setup_directional_low_ticks,
            "setup_directional_move_ticks": self.setup_directional_move_ticks,
            "setup_median_true_range_ticks": _ratio_payload(self.setup_median_true_range_ticks),
            "setup_true_range_sum_ticks": self.setup_true_range_sum_ticks,
            "trigger_directional_body_ticks": self.trigger_directional_body_ticks,
            "trigger_directional_range_ticks": self.trigger_directional_range_ticks,
            "trigger_true_range_ticks": self.trigger_true_range_ticks,
            "v_setup_median_range_over_atr": _ratio_payload(self.v_setup_median_range_over_atr),
            "w_setup_width_over_atr": _ratio_payload(self.w_setup_width_over_atr),
            "x_trigger_range_over_atr": _ratio_payload(self.x_trigger_range_over_atr),
            "z_failed_break_recovery_over_atr": _ratio_payload(
                self.z_failed_break_recovery_over_atr
            ),
        }


@dataclass(frozen=True, slots=True)
class BarPatternContext:
    """Leakage-safe variables for one timeframe/lookback/direction at trigger ``t``."""

    timeframe_seconds: int
    segment_id: int
    contract: str
    direction: Direction
    setup_lookback_bars: int
    history_previous_bar: PatternBarSnapshot
    atr_bars: tuple[PatternBarSnapshot, ...]
    setup_bars: tuple[PatternBarSnapshot, ...]
    trigger_bar: PatternBarSnapshot
    next_open: NextOpenReference | None
    metrics: BarPatternMetrics

    @property
    def setup_start_index(self) -> int:
        return self.setup_bars[0].source_index

    @property
    def setup_end_index(self) -> int:
        return self.setup_bars[-1].source_index

    @property
    def trigger_index(self) -> int:
        return self.trigger_bar.source_index

    @property
    def entry_index(self) -> int | None:
        return None if self.next_open is None else self.next_open.source_index

    @property
    def decision_ns(self) -> int:
        return self.trigger_bar.end_ns

    def as_dict(self) -> dict[str, object]:
        return {
            "atr_bars": [bar.as_dict() for bar in self.atr_bars],
            "contract": self.contract,
            "decision_ns": self.decision_ns,
            "direction": self.direction.value,
            "history_previous_bar": self.history_previous_bar.as_dict(),
            "metrics": self.metrics.as_dict(),
            "next_open": None if self.next_open is None else self.next_open.as_dict(),
            "segment_id": self.segment_id,
            "setup_bars": [bar.as_dict() for bar in self.setup_bars],
            "setup_lookback_bars": self.setup_lookback_bars,
            "timeframe_seconds": self.timeframe_seconds,
            "trigger_bar": self.trigger_bar.as_dict(),
        }


@dataclass(frozen=True, slots=True)
class PatternGateEvaluation:
    gate_id: str
    expression: str
    passed: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "expression": self.expression,
            "gate_id": self.gate_id,
            "passed": self.passed,
        }


@dataclass(frozen=True, slots=True)
class BarPatternEvaluation:
    """Complete auditable gate result for one candidate at one trigger."""

    candidate: BarPatternCandidate
    context: BarPatternContext
    gates: tuple[PatternGateEvaluation, ...]

    @property
    def matched(self) -> bool:
        return all(gate.passed for gate in self.gates)

    @property
    def failed_gate_ids(self) -> tuple[str, ...]:
        return tuple(gate.gate_id for gate in self.gates if not gate.passed)

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_definition_sha256": self.candidate.definition_sha256,
            "candidate_key": self.candidate.candidate_key,
            "context": self.context.as_dict(),
            "failed_gate_ids": list(self.failed_gate_ids),
            "gates": [gate.as_dict() for gate in self.gates],
            "matched": self.matched,
        }


def _direction(value: Direction | str) -> Direction:
    try:
        return Direction(value)
    except (TypeError, ValueError) as error:
        raise BarPatternError("direction must be LONG or SHORT") from error


def _validate_bar_identity(
    bar: PatternBarLike,
    *,
    index: int,
    timeframe_seconds: int,
    segment_id: int,
    contract: str,
) -> tuple[int, int]:
    if bar.timeframe_seconds != timeframe_seconds:
        raise BarPatternError(f"bars[{index}] crosses the signal timeframe")
    if bar.segment_id != segment_id or bar.contract != contract:
        raise BarPatternError(f"bars[{index}] crosses a contract or quality segment")
    start_ns = _integer(bar.start_ns, label=f"bars[{index}].start_ns", minimum=0)
    end_ns = _integer(bar.end_ns, label=f"bars[{index}].end_ns", minimum=1)
    if end_ns - start_ns != timeframe_seconds * NANOSECONDS_PER_SECOND:
        raise BarPatternError(f"bars[{index}] has a non-canonical boundary width")
    return start_ns, end_ns


def _completed_bar_snapshot(
    bar: PatternBarLike,
    *,
    index: int,
    start_ns: int,
    end_ns: int,
    sign: int,
    previous_close_ticks: int | None,
) -> PatternBarSnapshot:
    open_ticks = _integer(bar.open_ticks, label=f"bars[{index}].open_ticks", minimum=1)
    high_ticks = _integer(bar.high_ticks, label=f"bars[{index}].high_ticks", minimum=1)
    low_ticks = _integer(bar.low_ticks, label=f"bars[{index}].low_ticks", minimum=1)
    close_ticks = _integer(bar.close_ticks, label=f"bars[{index}].close_ticks", minimum=1)
    if low_ticks > min(open_ticks, close_ticks) or high_ticks < max(open_ticks, close_ticks):
        raise BarPatternError(f"bars[{index}] has invalid OHLC ordering")
    if low_ticks > high_ticks:
        raise BarPatternError(f"bars[{index}] low exceeds high")
    directional_open = sign * open_ticks
    directional_close = sign * close_ticks
    directional_high = max(sign * high_ticks, sign * low_ticks)
    directional_low = min(sign * high_ticks, sign * low_ticks)
    true_range = None
    if previous_close_ticks is not None:
        true_range = max(
            high_ticks - low_ticks,
            abs(high_ticks - previous_close_ticks),
            abs(low_ticks - previous_close_ticks),
        )
    return PatternBarSnapshot(
        source_index=index,
        start_ns=start_ns,
        end_ns=end_ns,
        open_ticks=open_ticks,
        high_ticks=high_ticks,
        low_ticks=low_ticks,
        close_ticks=close_ticks,
        directional_open_ticks=directional_open,
        directional_high_ticks=directional_high,
        directional_low_ticks=directional_low,
        directional_close_ticks=directional_close,
        true_range_ticks=true_range,
    )


def _median_integer(values: Sequence[int]) -> Fraction:
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return Fraction(ordered[midpoint], 1)
    return Fraction(ordered[midpoint - 1] + ordered[midpoint], 2)


def build_bar_pattern_context(
    bars: Sequence[PatternBarLike],
    *,
    trigger_index: int,
    setup_lookback_bars: int,
    direction: Direction | str,
) -> BarPatternContext:
    """Construct exact variables without reading beyond the next bar's open."""

    if not bars:
        raise BarPatternError("bars must be a non-empty sequence")
    trigger = _integer(trigger_index, label="trigger_index", minimum=0)
    if setup_lookback_bars not in SETUP_LOOKBACK_BARS:
        raise BarPatternError("setup_lookback_bars is outside the frozen v1 catalog")
    setup_start = trigger - setup_lookback_bars
    history_previous_index = setup_start - ATR_LOOKBACK_BARS - 1
    entry_index = trigger + 1
    if history_previous_index < 0:
        raise BarPatternError("insufficient completed history for the frozen ATR window")

    side = _direction(direction)
    sign = 1 if side is Direction.LONG else -1
    trigger_source = bars[trigger]
    timeframe_seconds = _integer(
        trigger_source.timeframe_seconds,
        label="timeframe_seconds",
        minimum=1,
    )
    if timeframe_seconds not in SIGNAL_TIMEFRAMES_SECONDS:
        raise BarPatternError("bar timeframe is outside the frozen v1 catalog")
    segment_id = _integer(trigger_source.segment_id, label="segment_id", minimum=1)
    contract = trigger_source.contract
    if not isinstance(contract, str) or not contract:
        raise BarPatternError("contract must be a non-empty string")

    snapshots: dict[int, PatternBarSnapshot] = {}
    previous_end_ns: int | None = None
    previous_close_ticks: int | None = None
    for index in range(history_previous_index, trigger + 1):
        bar = bars[index]
        start_ns, end_ns = _validate_bar_identity(
            bar,
            index=index,
            timeframe_seconds=timeframe_seconds,
            segment_id=segment_id,
            contract=contract,
        )
        if previous_end_ns is not None and start_ns != previous_end_ns:
            raise BarPatternError("pattern context contains a missing or overlapping signal bar")
        snapshot = _completed_bar_snapshot(
            bar,
            index=index,
            start_ns=start_ns,
            end_ns=end_ns,
            sign=sign,
            previous_close_ticks=previous_close_ticks,
        )
        snapshots[index] = snapshot
        previous_end_ns = end_ns
        previous_close_ticks = snapshot.close_ticks

    next_open: NextOpenReference | None = None
    if entry_index < len(bars):
        entry_source = bars[entry_index]
        entry_start_ns: int | None = None
        try:
            candidate_start_ns, _ = _validate_bar_identity(
                entry_source,
                index=entry_index,
                timeframe_seconds=timeframe_seconds,
                segment_id=segment_id,
                contract=contract,
            )
            if previous_end_ns is not None and candidate_start_ns == previous_end_ns:
                entry_start_ns = candidate_start_ns
        except BarPatternError:
            pass
        if entry_start_ns is not None:
            next_open = NextOpenReference(
                source_index=entry_index,
                start_ns=entry_start_ns,
                open_ticks=_integer(
                    entry_source.open_ticks,
                    label=f"bars[{entry_index}].open_ticks",
                    minimum=1,
                ),
            )

    atr_start = setup_start - ATR_LOOKBACK_BARS
    atr_bars = tuple(snapshots[index] for index in range(atr_start, setup_start))
    setup_bars = tuple(snapshots[index] for index in range(setup_start, trigger))
    trigger_bar = snapshots[trigger]
    atr_true_ranges = tuple(bar.true_range_ticks for bar in atr_bars)
    setup_true_ranges = tuple(bar.true_range_ticks for bar in setup_bars)
    if any(value is None for value in atr_true_ranges + setup_true_ranges):
        raise AssertionError("true-range predecessor construction drift")
    atr_values = tuple(int(value) for value in atr_true_ranges)
    setup_values = tuple(int(value) for value in setup_true_ranges)
    trigger_true_range = trigger_bar.true_range_ticks
    if trigger_true_range is None:
        raise AssertionError("trigger true range is unavailable")
    atr_sum = sum(atr_values)
    if atr_sum <= 0:
        raise BarPatternError("ATR denominator is zero")
    setup_tr_sum = sum(setup_values)
    if setup_tr_sum <= 0:
        raise BarPatternError("setup efficiency denominator is zero")
    trigger_directional_range = (
        trigger_bar.directional_high_ticks - trigger_bar.directional_low_ticks
    )
    if trigger_directional_range <= 0:
        raise BarPatternError("trigger body/location denominator is zero")

    setup_first = setup_bars[0]
    setup_last = setup_bars[-1]
    setup_high = max(bar.directional_high_ticks for bar in setup_bars)
    setup_low = min(bar.directional_low_ticks for bar in setup_bars)
    setup_move = setup_last.directional_close_ticks - setup_first.directional_open_ticks
    median_setup_tr = _median_integer(setup_values)
    trigger_body = trigger_bar.directional_close_ticks - trigger_bar.directional_open_ticks
    atr_ratio_scale = Fraction(ATR_LOOKBACK_BARS, atr_sum)
    metrics = BarPatternMetrics(
        atr_period_bars=ATR_LOOKBACK_BARS,
        atr_true_range_sum_ticks=atr_sum,
        setup_true_range_sum_ticks=setup_tr_sum,
        setup_median_true_range_ticks=median_setup_tr,
        setup_directional_move_ticks=setup_move,
        setup_directional_high_ticks=setup_high,
        setup_directional_low_ticks=setup_low,
        trigger_true_range_ticks=trigger_true_range,
        trigger_directional_range_ticks=trigger_directional_range,
        trigger_directional_body_ticks=trigger_body,
        prior_directional_body_absolute_ticks=abs(
            setup_last.directional_close_ticks - setup_last.directional_open_ticks
        ),
        r_setup_move_over_atr=setup_move * atr_ratio_scale,
        e_setup_efficiency=Fraction(setup_move, setup_tr_sum),
        w_setup_width_over_atr=(setup_high - setup_low) * atr_ratio_scale,
        v_setup_median_range_over_atr=median_setup_tr * atr_ratio_scale,
        x_trigger_range_over_atr=trigger_true_range * atr_ratio_scale,
        b_trigger_body_fraction=Fraction(trigger_body, trigger_directional_range),
        q_trigger_close_location=Fraction(
            trigger_bar.directional_close_ticks - trigger_bar.directional_low_ticks,
            trigger_directional_range,
        ),
        k_trigger_lower_wick_fraction=Fraction(
            min(
                trigger_bar.directional_open_ticks,
                trigger_bar.directional_close_ticks,
            )
            - trigger_bar.directional_low_ticks,
            trigger_directional_range,
        ),
        p_pullback_depth_over_atr=(
            setup_last.directional_close_ticks - trigger_bar.directional_low_ticks
        )
        * atr_ratio_scale,
        j_breakout_distance_over_atr=(trigger_bar.directional_close_ticks - setup_high)
        * atr_ratio_scale,
        n_failed_break_distance_over_atr=(setup_low - trigger_bar.directional_low_ticks)
        * atr_ratio_scale,
        z_failed_break_recovery_over_atr=(trigger_bar.directional_close_ticks - setup_low)
        * atr_ratio_scale,
    )
    return BarPatternContext(
        timeframe_seconds=timeframe_seconds,
        segment_id=segment_id,
        contract=contract,
        direction=side,
        setup_lookback_bars=setup_lookback_bars,
        history_previous_bar=snapshots[history_previous_index],
        atr_bars=atr_bars,
        setup_bars=setup_bars,
        trigger_bar=trigger_bar,
        next_open=next_open,
        metrics=metrics,
    )


def _gate_values(
    context: BarPatternContext,
    family: PatternFamilySpec,
) -> tuple[tuple[str, bool], ...]:
    metrics = context.metrics
    trigger = context.trigger_bar
    previous = context.setup_bars[-1]

    if family.family_id == "F1":
        return (
            ("R_GE_3_4", metrics.r_setup_move_over_atr >= Fraction(3, 4)),
            ("E_GE_7_20", metrics.e_setup_efficiency >= Fraction(7, 20)),
            ("X_GE_1_2", metrics.x_trigger_range_over_atr >= Fraction(1, 2)),
            ("B_GE_1_2", metrics.b_trigger_body_fraction >= Fraction(1, 2)),
            ("Q_GE_3_4", metrics.q_trigger_close_location >= Fraction(3, 4)),
        )
    if family.family_id == "F2":
        return (
            ("R_GE_1_2", metrics.r_setup_move_over_atr >= Fraction(1, 2)),
            ("E_GE_1_4", metrics.e_setup_efficiency >= Fraction(1, 4)),
            ("P_GE_1_4", metrics.p_pullback_depth_over_atr >= Fraction(1, 4)),
            (
                "CLOSE_RECLAIMS_SETUP",
                trigger.directional_close_ticks >= previous.directional_close_ticks,
            ),
            ("X_GE_1_2", metrics.x_trigger_range_over_atr >= Fraction(1, 2)),
            ("B_GE_0", metrics.b_trigger_body_fraction >= 0),
            ("Q_GE_2_3", metrics.q_trigger_close_location >= Fraction(2, 3)),
            ("K_GE_1_4", metrics.k_trigger_lower_wick_fraction >= Fraction(1, 4)),
        )
    if family.family_id == "F3":
        return (
            ("R_LE_NEG_1", metrics.r_setup_move_over_atr <= -1),
            ("E_LE_NEG_7_20", metrics.e_setup_efficiency <= Fraction(-7, 20)),
            ("X_GE_3_4", metrics.x_trigger_range_over_atr >= Fraction(3, 4)),
            ("B_GE_0", metrics.b_trigger_body_fraction >= 0),
            ("Q_GE_2_3", metrics.q_trigger_close_location >= Fraction(2, 3)),
            ("K_GE_2_5", metrics.k_trigger_lower_wick_fraction >= Fraction(2, 5)),
        )
    if family.family_id == "F4":
        return (
            ("R_LE_NEG_1_2", metrics.r_setup_move_over_atr <= Fraction(-1, 2)),
            (
                "PRIOR_BAR_OPPOSITE",
                previous.directional_close_ticks < previous.directional_open_ticks,
            ),
            (
                "TRIGGER_OPENS_BELOW_PRIOR_CLOSE",
                trigger.directional_open_ticks <= previous.directional_close_ticks,
            ),
            (
                "TRIGGER_CLOSES_ABOVE_PRIOR_OPEN",
                trigger.directional_close_ticks >= previous.directional_open_ticks,
            ),
            (
                "BODY_GE_3_4_PRIOR_BODY",
                metrics.trigger_directional_body_ticks * 4
                >= 3 * metrics.prior_directional_body_absolute_ticks,
            ),
            ("X_GE_1_2", metrics.x_trigger_range_over_atr >= Fraction(1, 2)),
            ("Q_GE_2_3", metrics.q_trigger_close_location >= Fraction(2, 3)),
        )
    if family.family_id == "F5":
        return (
            ("W_LE_3_2", metrics.w_setup_width_over_atr <= Fraction(3, 2)),
            ("V_LE_3_4", metrics.v_setup_median_range_over_atr <= Fraction(3, 4)),
            ("J_GE_1_10", metrics.j_breakout_distance_over_atr >= Fraction(1, 10)),
            ("X_GE_3_4", metrics.x_trigger_range_over_atr >= Fraction(3, 4)),
            ("B_GE_1_2", metrics.b_trigger_body_fraction >= Fraction(1, 2)),
            ("Q_GE_3_4", metrics.q_trigger_close_location >= Fraction(3, 4)),
        )
    if family.family_id == "F6":
        return (
            ("W_GE_1_2", metrics.w_setup_width_over_atr >= Fraction(1, 2)),
            ("N_GE_1_10", metrics.n_failed_break_distance_over_atr >= Fraction(1, 10)),
            ("Z_GE_1_10", metrics.z_failed_break_recovery_over_atr >= Fraction(1, 10)),
            (
                "CLOSE_NOT_ABOVE_SETUP_HIGH",
                trigger.directional_close_ticks <= metrics.setup_directional_high_ticks,
            ),
            ("X_GE_3_4", metrics.x_trigger_range_over_atr >= Fraction(3, 4)),
            ("B_GE_0", metrics.b_trigger_body_fraction >= 0),
            ("Q_GE_2_3", metrics.q_trigger_close_location >= Fraction(2, 3)),
            ("K_GE_1_4", metrics.k_trigger_lower_wick_fraction >= Fraction(1, 4)),
        )
    raise BarPatternError(f"unknown frozen family: {family.family_id}")


def evaluate_bar_pattern_context(
    context: BarPatternContext,
    *,
    candidate: BarPatternCandidate,
) -> BarPatternEvaluation:
    """Evaluate one already-built context against its candidate's fixed gates."""

    if candidate.timeframe_seconds != context.timeframe_seconds:
        raise BarPatternError("candidate timeframe does not match the context")
    if candidate.setup_lookback_bars != context.setup_lookback_bars:
        raise BarPatternError("candidate setup length does not match the context")
    if candidate.direction is not context.direction:
        raise BarPatternError("candidate direction does not match the context")
    frozen_family = PATTERN_FAMILY_BY_ID.get(candidate.family.family_id)
    if frozen_family != candidate.family:
        raise BarPatternError("candidate family differs from the frozen v1 definition")
    values = _gate_values(context, candidate.family)
    expected_gate_ids = tuple(gate_id for gate_id, _ in candidate.family.gates)
    actual_gate_ids = tuple(gate_id for gate_id, _ in values)
    if actual_gate_ids != expected_gate_ids:
        raise AssertionError("pattern family gate implementation drift")
    expressions = dict(candidate.family.gates)
    gates = tuple(
        PatternGateEvaluation(gate_id, expressions[gate_id], passed) for gate_id, passed in values
    )
    return BarPatternEvaluation(candidate=candidate, context=context, gates=gates)


def evaluate_bar_pattern(
    bars: Sequence[PatternBarLike],
    *,
    trigger_index: int,
    candidate: BarPatternCandidate,
) -> BarPatternEvaluation:
    """Build the point-in-time context and evaluate one frozen candidate."""

    context = build_bar_pattern_context(
        bars,
        trigger_index=trigger_index,
        setup_lookback_bars=candidate.setup_lookback_bars,
        direction=candidate.direction,
    )
    return evaluate_bar_pattern_context(context, candidate=candidate)
