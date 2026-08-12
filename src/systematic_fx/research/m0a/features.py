"""Point-in-time 5m event features with completed 30m/1h context."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from itertools import pairwise
from typing import TYPE_CHECKING

from systematic_fx.research.m0a.model import EventFeature, M0aDataError, MarketFixture, QuoteEvent

if TYPE_CHECKING:
    from systematic_fx.research.m0a.config import EpochConfig
    from systematic_fx.research.m0a.model import SessionWindow


_NS_PER_SECOND = 1_000_000_000
_PPM = 1_000_000


@dataclass(frozen=True, slots=True)
class _FiveMinuteBar:
    start_ts_ns: int
    end_ts_ns: int
    instrument_id: int
    session_id: str
    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int
    spread_ticks: int
    depth_imbalance_ppm: int
    all_quotes_valid: bool


def _round_ratio(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise M0aDataError("positive denominator required")
    if numerator >= 0:
        return (numerator + denominator // 2) // denominator
    return -((-numerator + denominator // 2) // denominator)


def _aggregate_bar(
    session: SessionWindow,
    bucket: int,
    events: Iterable[QuoteEvent],
    decision_clock_seconds: int,
) -> _FiveMinuteBar:
    ordered = tuple(events)
    if not ordered:
        raise M0aDataError("cannot aggregate an empty quote bucket")
    valid = tuple(event for event in ordered if event.valid)
    if not valid:
        # Preserve a deterministic invalid row without manufacturing prices.
        valid = (ordered[-1],)
    midpoints = tuple((event.bid_ticks + event.ask_ticks) // 2 for event in valid)
    last = valid[-1]
    depth_total = last.bid_depth_l10 + last.ask_depth_l10
    imbalance = (
        0 if depth_total == 0 else ((last.bid_depth_l10 - last.ask_depth_l10) * _PPM) // depth_total
    )
    start_ts_ns = session.open_ts_ns + bucket * decision_clock_seconds * _NS_PER_SECOND
    return _FiveMinuteBar(
        start_ts_ns=start_ts_ns,
        end_ts_ns=start_ts_ns + decision_clock_seconds * _NS_PER_SECOND,
        instrument_id=session.active_instrument_id,
        session_id=session.session_id,
        open_ticks=midpoints[0],
        high_ticks=max(midpoints),
        low_ticks=min(midpoints),
        close_ticks=midpoints[-1],
        spread_ticks=last.ask_ticks - last.bid_ticks,
        depth_imbalance_ppm=imbalance,
        all_quotes_valid=all(event.valid for event in ordered),
    )


def _bars_for_session(
    epoch: EpochConfig,
    fixture: MarketFixture,
    session: SessionWindow,
) -> tuple[_FiveMinuteBar, ...]:
    width_ns = epoch.decision_clock_seconds * _NS_PER_SECOND
    bucket_count, remainder = divmod(session.close_ts_ns - session.open_ts_ns, width_ns)
    if remainder or bucket_count <= 0:
        raise M0aDataError("session span must be an exact positive number of decision bars")
    buckets: list[list[QuoteEvent]] = [[] for _ in range(bucket_count)]
    for event in fixture.quote_events:
        if event.session_id != session.session_id:
            continue
        if event.instrument_id != session.active_instrument_id:
            raise M0aDataError("a session quote silently switched instrument_id")
        if not session.open_ts_ns <= event.ts_ns < session.close_ts_ns:
            raise M0aDataError("quote event is outside its declared session")
        bucket = (event.ts_ns - session.open_ts_ns) // width_ns
        buckets[bucket].append(event)
    if any(not bucket for bucket in buckets):
        raise M0aDataError("fixture has an incomplete 5m decision bucket")
    return tuple(
        _aggregate_bar(session, index, events, epoch.decision_clock_seconds)
        for index, events in enumerate(buckets)
    )


def _true_ranges(bars: tuple[_FiveMinuteBar, ...]) -> tuple[int, ...]:
    ranges: list[int] = []
    previous_close: int | None = None
    for bar in bars:
        true_range = bar.high_ticks - bar.low_ticks
        if previous_close is not None:
            true_range = max(
                true_range,
                abs(bar.high_ticks - previous_close),
                abs(bar.low_ticks - previous_close),
            )
        ranges.append(true_range)
        previous_close = bar.close_ticks
    return tuple(ranges)


def _volatilities(true_ranges: tuple[int, ...], lookback: int) -> tuple[int, ...]:
    values: list[int] = []
    for index in range(len(true_ranges)):
        start = max(0, index - lookback + 1)
        sample = true_ranges[start : index + 1]
        values.append(max(1, _round_ratio(sum(sample), len(sample))))
    return tuple(values)


def _latest_completed_context(
    bars: tuple[_FiveMinuteBar, ...],
    index: int,
    window_bars: int,
) -> tuple[int | None, int | None]:
    # Only aligned windows whose closing bar is no later than the decision time
    # are eligible.  An in-progress 30m/1h candle is never read.
    completed_end = ((index + 1) // window_bars) * window_bars - 1
    if completed_end < window_bars - 1:
        return None, None
    start = completed_end - window_bars + 1
    context = bars[start : completed_end + 1]
    if len(context) != window_bars:
        return None, None
    return context[-1].close_ticks - context[0].open_ticks, context[-1].end_ts_ns


def _pullback_length(bars: tuple[_FiveMinuteBar, ...], index: int, trend_ticks: int | None) -> int:
    if trend_ticks in (None, 0) or index == 0:
        return 0
    trend_sign = 1 if trend_ticks > 0 else -1
    length = 0
    # The current completed 5m bar is the possible continuation trigger.  The
    # pullback is therefore the immediately preceding run of counter-trend bars,
    # all of which were already known when the trigger bar closed.
    cursor = index - 1
    while cursor > 0 and length < 12:
        move = bars[cursor].close_ticks - bars[cursor - 1].close_ticks
        if move == 0 or move * trend_sign >= 0:
            break
        length += 1
        cursor -= 1
    return length


def build_features(
    epoch: EpochConfig,
    fixture: MarketFixture,
) -> tuple[EventFeature, ...]:
    """Build causal feature rows at every completed 5m decision timestamp."""

    epoch.verify_unchanged()
    if fixture.dataset_hash != epoch.dataset_hash or fixture.content_sha256 != epoch.dataset_hash:
        raise M0aDataError("feature source does not match the epoch dataset hash")
    evidence = {item.trading_date: item for item in fixture.previous_day_volumes}
    sessions = tuple(sorted(fixture.sessions, key=lambda item: item.open_ts_ns))
    result: list[EventFeature] = []
    previous_active_instrument: int | None = None

    for session in sessions:
        selection = evidence.get(session.trading_date)
        if selection is None or selection.selected_instrument_id != session.active_instrument_id:
            raise M0aDataError("session contract lacks valid previous-day-volume evidence")
        bars = _bars_for_session(epoch, fixture, session)
        true_ranges = _true_ranges(bars)
        volatilities = _volatilities(true_ranges, epoch.atr_lookback_bars)
        switched_contract = (
            previous_active_instrument is not None
            and previous_active_instrument != session.active_instrument_id
        )
        roll_warmup = max(
            12,
            epoch.atr_lookback_bars + epoch.quantile_lookback_bars,
            epoch.short_trend_lookback_bars,
        )

        for index, bar in enumerate(bars):
            flags: list[str] = []
            if not bar.all_quotes_valid:
                flags.append("INVALID_SOURCE_QUOTE")
            if index + 1 < epoch.atr_lookback_bars:
                flags.append("INSUFFICIENT_ATR_HISTORY")

            prior_start = index - epoch.quantile_lookback_bars
            prior_volatilities = (
                volatilities[prior_start:index]
                if prior_start >= epoch.atr_lookback_bars - 1
                else ()
            )
            if len(prior_volatilities) != epoch.quantile_lookback_bars:
                volatility_quantile: int | None = None
                flags.append("INSUFFICIENT_PRIOR_QUANTILE_HISTORY")
            else:
                less_or_equal = sum(value <= volatilities[index] for value in prior_volatilities)
                volatility_quantile = (less_or_equal * _PPM) // len(prior_volatilities)

            trend_30m, context_30m_end = _latest_completed_context(bars, index, 6)
            trend_1h, context_1h_end = _latest_completed_context(bars, index, 12)
            if trend_30m is None:
                flags.append("MISSING_COMPLETED_30M_CONTEXT")
            if trend_1h is None:
                flags.append("MISSING_COMPLETED_1H_CONTEXT")

            if index < epoch.short_trend_lookback_bars:
                short_trend = 0
                flags.append("INSUFFICIENT_SHORT_TREND_HISTORY")
            else:
                short_trend = (
                    bar.close_ticks - bars[index - epoch.short_trend_lookback_bars].close_ticks
                )

            roll_cross = switched_contract and index < roll_warmup
            if roll_cross:
                flags.append("ROLL_CROSS_LOOKBACK")
            inside_roll_guard = any(
                guard.contains(bar.end_ts_ns, bar.instrument_id) for guard in fixture.roll_guards
            )

            bar_range = bar.high_ticks - bar.low_ticks
            body_ratio = (
                0 if bar_range == 0 else (abs(bar.close_ticks - bar.open_ticks) * _PPM) // bar_range
            )
            close_location = (
                _PPM // 2
                if bar_range == 0
                else ((bar.close_ticks - bar.low_ticks) * _PPM) // bar_range
            )
            trailing_return = 0 if index == 0 else bar.close_ticks - bars[index - 1].close_ticks
            result.append(
                EventFeature(
                    event_ts_ns=bar.end_ts_ns,
                    instrument_id=bar.instrument_id,
                    session_id=bar.session_id,
                    trading_date=session.trading_date,
                    feature_version=epoch.feature_version,
                    source_data_version=fixture.source_data_version,
                    bar_open_ticks=bar.open_ticks,
                    bar_high_ticks=bar.high_ticks,
                    bar_low_ticks=bar.low_ticks,
                    bar_close_ticks=bar.close_ticks,
                    trailing_return_ticks=trailing_return,
                    range_ticks=bar_range,
                    volatility_ticks=volatilities[index],
                    body_ratio_ppm=body_ratio,
                    close_location_ppm=close_location,
                    short_trend_ticks=short_trend,
                    pullback_length=_pullback_length(bars, index, trend_1h),
                    spread_ticks=bar.spread_ticks,
                    depth_imbalance_ppm=bar.depth_imbalance_ppm,
                    volatility_quantile_ppm=volatility_quantile,
                    trend_30m_ticks=trend_30m,
                    context_30m_end_ns=context_30m_end,
                    trend_1h_ticks=trend_1h,
                    context_1h_end_ns=context_1h_end,
                    roll_cross=roll_cross,
                    inside_roll_guard=inside_roll_guard,
                    feature_valid=not flags,
                    validity_flags=tuple(flags),
                )
            )
        previous_active_instrument = session.active_instrument_id

    if any(left.event_ts_ns >= right.event_ts_ns for left, right in pairwise(result)):
        raise M0aDataError("feature rows are not in strict chronological order")
    return tuple(result)
