"""Quote-aware first-passage labels for the deterministic M0a barrier grid."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from systematic_fx.research.m0a.model import (
    BarrierSpec,
    Direction,
    EventFeature,
    FirstTouchType,
    M0aDataError,
    MarketFixture,
    QuoteAwareLabel,
    QuoteEvent,
)

if TYPE_CHECKING:
    from systematic_fx.research.m0a.config import EpochConfig
    from systematic_fx.research.m0a.model import SessionWindow


_NS_PER_SECOND = 1_000_000_000


@dataclass(frozen=True, slots=True)
class _Touch:
    touch_type: FirstTouchType
    event: QuoteEvent
    exit_price_ticks: int
    ambiguous: bool
    raw_fallback_used: bool


def _barrier_distance(volatility_ticks: int, numerator: int, denominator: int) -> int:
    if volatility_ticks <= 0 or numerator <= 0 or denominator <= 0:
        raise M0aDataError("barrier distance inputs must be positive")
    return max(1, (volatility_ticks * numerator + denominator // 2) // denominator)


def _is_tp(
    event: QuoteEvent,
    direction: Direction,
    tp_price_ticks: int,
    trade_through_ticks: int,
) -> bool:
    if event.trade_action != "TRADE" or event.trade_price_ticks is None:
        return False
    if direction is Direction.LONG:
        return event.trade_price_ticks >= tp_price_ticks + trade_through_ticks
    return event.trade_price_ticks <= tp_price_ticks - trade_through_ticks


def _is_sl(
    event: QuoteEvent,
    direction: Direction,
    sl_price_ticks: int,
) -> bool:
    if direction is Direction.LONG:
        return event.bid_ticks <= sl_price_ticks
    return event.ask_ticks >= sl_price_ticks


def _stop_price(event: QuoteEvent, direction: Direction) -> int:
    return event.bid_ticks if direction is Direction.LONG else event.ask_ticks


def _raw_fallback(
    events: tuple[QuoteEvent, ...],
    direction: Direction,
    tp_price_ticks: int,
    sl_price_ticks: int,
    trade_through_ticks: int,
) -> _Touch:
    for event in events:
        tp_hit = _is_tp(event, direction, tp_price_ticks, trade_through_ticks)
        sl_hit = _is_sl(event, direction, sl_price_ticks)
        # A single quote claiming both sides is conservatively a stop.  Normal
        # ordered MBP events resolve to whichever condition first becomes true.
        if sl_hit:
            return _Touch(FirstTouchType.SL_FIRST, event, _stop_price(event, direction), True, True)
        if tp_hit:
            return _Touch(FirstTouchType.TP_FIRST, event, tp_price_ticks, True, True)
    raise M0aDataError("ambiguous aggregate second did not resolve in raw events")


def _first_passage(
    events: tuple[QuoteEvent, ...],
    direction: Direction,
    tp_price_ticks: int,
    sl_price_ticks: int,
    trade_through_ticks: int,
) -> _Touch | None:
    by_second: list[tuple[QuoteEvent, ...]] = []
    current_second: int | None = None
    current: list[QuoteEvent] = []
    for event in events:
        second = event.ts_ns // _NS_PER_SECOND
        if current_second is not None and second != current_second:
            by_second.append(tuple(current))
            current = []
        current_second = second
        current.append(event)
    if current:
        by_second.append(tuple(current))

    for second_events in by_second:
        executable = (
            tuple(event.bid_ticks for event in second_events)
            if direction is Direction.LONG
            else tuple(event.ask_ticks for event in second_events)
        )
        trade_prices = tuple(
            event.trade_price_ticks
            for event in second_events
            if event.trade_action == "TRADE" and event.trade_price_ticks is not None
        )
        if direction is Direction.LONG:
            tp_possible = bool(trade_prices) and (
                max(trade_prices) >= tp_price_ticks + trade_through_ticks
            )
            sl_possible = min(executable) <= sl_price_ticks
        else:
            tp_possible = bool(trade_prices) and (
                min(trade_prices) <= tp_price_ticks - trade_through_ticks
            )
            sl_possible = max(executable) >= sl_price_ticks
        if tp_possible and sl_possible:
            return _raw_fallback(
                second_events,
                direction,
                tp_price_ticks,
                sl_price_ticks,
                trade_through_ticks,
            )
        if sl_possible:
            event = next(
                event for event in second_events if _is_sl(event, direction, sl_price_ticks)
            )
            return _Touch(
                FirstTouchType.SL_FIRST,
                event,
                _stop_price(event, direction),
                False,
                False,
            )
        if tp_possible:
            event = next(
                event
                for event in second_events
                if _is_tp(event, direction, tp_price_ticks, trade_through_ticks)
            )
            return _Touch(FirstTouchType.TP_FIRST, event, tp_price_ticks, False, False)
    return None


def _invalid_label(
    epoch: EpochConfig,
    feature: EventFeature,
    direction: Direction,
    barrier: BarrierSpec,
    reason: str,
) -> QuoteAwareLabel:
    return QuoteAwareLabel(
        event_ts_ns=feature.event_ts_ns,
        instrument_id=feature.instrument_id,
        direction=direction,
        barrier_id=barrier.barrier_id,
        k_tp_num=barrier.k_tp_num,
        k_tp_den=barrier.k_tp_den,
        k_sl_num=barrier.k_sl_num,
        k_sl_den=barrier.k_sl_den,
        max_hold_seconds=barrier.max_hold_seconds,
        entry_ts_ns=None,
        entry_price_ticks=None,
        tp_price_ticks=None,
        sl_price_ticks=None,
        first_touch_type=FirstTouchType.INVALID,
        first_touch_ts_ns=None,
        exit_ts_ns=None,
        exit_price_ticks=None,
        timeout=False,
        ambiguous=False,
        raw_fallback_used=False,
        cost_ticks=epoch.round_trip_cost_ticks,
        gross_pnl_ticks=None,
        net_pnl_ticks=None,
        label_version=epoch.label_version,
        eligible=False,
        invalid_reason=reason,
    )


def _eligible_events(
    all_events: tuple[QuoteEvent, ...],
    all_timestamps: tuple[int, ...],
    first_ts_ns: int,
    last_ts_ns: int,
) -> tuple[QuoteEvent, ...]:
    left = bisect_left(all_timestamps, first_ts_ns)
    right = bisect_right(all_timestamps, last_ts_ns)
    return tuple(event for event in all_events[left:right] if event.valid)


def _label_one(
    epoch: EpochConfig,
    fixture: MarketFixture,
    session: SessionWindow,
    feature: EventFeature,
    direction: Direction,
    barrier: BarrierSpec,
    events: tuple[QuoteEvent, ...],
    event_timestamps: tuple[int, ...],
) -> QuoteAwareLabel:
    if feature.feature_version != epoch.feature_version:
        return _invalid_label(epoch, feature, direction, barrier, "FEATURE_VERSION_MISMATCH")
    if not feature.feature_valid:
        return _invalid_label(epoch, feature, direction, barrier, "INVALID_FEATURE")
    if feature.instrument_id != session.active_instrument_id:
        return _invalid_label(epoch, feature, direction, barrier, "ACTIVE_CONTRACT_MISMATCH")
    if epoch.no_entry_inside_roll_guard and any(
        guard.contains(feature.event_ts_ns, feature.instrument_id) for guard in fixture.roll_guards
    ):
        return _invalid_label(epoch, feature, direction, barrier, "ROLL_GUARD")

    horizon_ts_ns = feature.event_ts_ns + barrier.max_hold_seconds * _NS_PER_SECOND
    # This decision is made before reading a single future outcome event.
    if horizon_ts_ns > session.close_ts_ns:
        return _invalid_label(epoch, feature, direction, barrier, "WOULD_CROSS_SESSION_CLOSE")
    instrument = next(
        metadata
        for metadata in fixture.instruments
        if metadata.instrument_id == feature.instrument_id
    )
    if horizon_ts_ns >= instrument.expiry_ts_ns:
        return _invalid_label(epoch, feature, direction, barrier, "DELIVERY_OR_EXPIRY_GUARD")

    route_ts_ns = feature.event_ts_ns + epoch.route_delay_seconds * _NS_PER_SECOND
    path = _eligible_events(events, event_timestamps, route_ts_ns, horizon_ts_ns)
    if not path:
        return _invalid_label(epoch, feature, direction, barrier, "NO_ELIGIBLE_ENTRY_QUOTE")
    entry_event = path[0]
    if entry_event.instrument_id != feature.instrument_id:
        raise M0aDataError("entry event changed instrument_id")

    if direction is Direction.LONG:
        entry_price = entry_event.ask_ticks + epoch.entry_adverse_ticks
    else:
        entry_price = entry_event.bid_ticks - epoch.entry_adverse_ticks
    tp_distance = _barrier_distance(feature.volatility_ticks, barrier.k_tp_num, barrier.k_tp_den)
    sl_distance = _barrier_distance(feature.volatility_ticks, barrier.k_sl_num, barrier.k_sl_den)
    if direction is Direction.LONG:
        tp_price = entry_price + tp_distance
        sl_price = entry_price - sl_distance
    else:
        tp_price = entry_price - tp_distance
        sl_price = entry_price + sl_distance

    touch = _first_passage(
        path,
        direction,
        tp_price,
        sl_price,
        epoch.tp_trade_through_ticks,
    )
    if touch is None:
        last = path[-1]
        exit_price = last.bid_ticks if direction is Direction.LONG else last.ask_ticks
        touch_type = FirstTouchType.TIMEOUT
        first_touch_ts: int | None = None
        exit_ts = last.ts_ns
        ambiguous = False
        raw_fallback_used = False
    else:
        exit_price = touch.exit_price_ticks
        touch_type = touch.touch_type
        first_touch_ts = touch.event.ts_ns
        exit_ts = touch.event.ts_ns
        ambiguous = touch.ambiguous
        raw_fallback_used = touch.raw_fallback_used
    gross_pnl = (
        exit_price - entry_price if direction is Direction.LONG else entry_price - exit_price
    )
    return QuoteAwareLabel(
        event_ts_ns=feature.event_ts_ns,
        instrument_id=feature.instrument_id,
        direction=direction,
        barrier_id=barrier.barrier_id,
        k_tp_num=barrier.k_tp_num,
        k_tp_den=barrier.k_tp_den,
        k_sl_num=barrier.k_sl_num,
        k_sl_den=barrier.k_sl_den,
        max_hold_seconds=barrier.max_hold_seconds,
        entry_ts_ns=entry_event.ts_ns,
        entry_price_ticks=entry_price,
        tp_price_ticks=tp_price,
        sl_price_ticks=sl_price,
        first_touch_type=touch_type,
        first_touch_ts_ns=first_touch_ts,
        exit_ts_ns=exit_ts,
        exit_price_ticks=exit_price,
        timeout=touch_type is FirstTouchType.TIMEOUT,
        ambiguous=ambiguous,
        raw_fallback_used=raw_fallback_used,
        cost_ticks=epoch.round_trip_cost_ticks,
        gross_pnl_ticks=gross_pnl,
        net_pnl_ticks=gross_pnl - epoch.round_trip_cost_ticks,
        label_version=epoch.label_version,
        eligible=True,
        invalid_reason=None,
    )


def build_labels(
    epoch: EpochConfig,
    fixture: MarketFixture,
    features: Iterable[EventFeature],
) -> tuple[QuoteAwareLabel, ...]:
    """Build the complete quote-aware 2-direction x 27-barrier label store."""

    epoch.verify_unchanged()
    if fixture.dataset_hash != epoch.dataset_hash or fixture.content_sha256 != epoch.dataset_hash:
        raise M0aDataError("label source does not match the epoch dataset hash")
    sessions = {session.session_id: session for session in fixture.sessions}
    indexed: dict[tuple[str, int], list[QuoteEvent]] = defaultdict(list)
    for event in fixture.quote_events:
        indexed[(event.session_id, event.instrument_id)].append(event)
    event_indexes = {
        key: (
            tuple(events),
            tuple(event.ts_ns for event in events),
        )
        for key, events in indexed.items()
    }

    result: list[QuoteAwareLabel] = []
    seen_features: set[tuple[int, int]] = set()
    for feature in features:
        feature_key = (feature.event_ts_ns, feature.instrument_id)
        if feature_key in seen_features:
            raise M0aDataError("duplicate event feature key")
        seen_features.add(feature_key)
        try:
            session = sessions[feature.session_id]
            events, timestamps = event_indexes[(feature.session_id, feature.instrument_id)]
        except KeyError as exc:
            raise M0aDataError("feature references unknown session/instrument quotes") from exc
        for direction in (Direction.LONG, Direction.SHORT):
            for barrier in epoch.barrier_specs:
                result.append(
                    _label_one(
                        epoch,
                        fixture,
                        session,
                        feature,
                        direction,
                        barrier,
                        events,
                        timestamps,
                    )
                )

    expected = len(seen_features) * 2 * len(epoch.barrier_specs)
    if len(result) != expected:
        raise M0aDataError("label grid cardinality invariant failed")
    return tuple(result)
