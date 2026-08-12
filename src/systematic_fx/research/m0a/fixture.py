"""Small deterministic 6E MBP-10-style fixture for the M0a walking skeleton."""

from __future__ import annotations

import calendar
from dataclasses import replace
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from systematic_fx.research.m0a.model import (
    InstrumentMetadata,
    M0aDataError,
    MarketFixture,
    PreviousDayVolume,
    QuoteEvent,
    RollGuard,
    SessionWindow,
)

if TYPE_CHECKING:
    from systematic_fx.research.m0a.config import EpochConfig


_NS_PER_SECOND = 1_000_000_000
_FIXTURE_VERSION = "m0a-6e-mbp10-fixture-v1"
_OLD_CONTRACT = 6_000_922
_NEW_CONTRACT = 6_001_222

# An intentionally planted engineering pattern makes the walking skeleton prove
# occupancy de-duplication, positive first passage, walk-forward, and survivor
# registration mechanics.  It is synthetic test structure, never evidence of
# market alpha.  Minute offsets are measured from the 13:00 UTC session open.
_ENGINEERING_PATTERN_STARTS: dict[date, tuple[int, ...]] = {
    date(2022, 8, 29): (296, 297, 298, 299, 300),
    date(2022, 8, 30): (296, 297, 298, 299, 300),
    date(2022, 8, 31): (296, 297, 298, 299, 300),
    date(2022, 9, 1): (296, 297, 298, 299, 300),
    date(2022, 9, 2): (296, 297, 298, 299, 300),
}


def _timestamp_ns(day: date, hour: int, minute: int = 0, second: int = 0) -> int:
    value = datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=UTC)
    return calendar.timegm(value.utctimetuple()) * _NS_PER_SECOND


def _session(
    day: date,
    instrument_id: int,
) -> SessionWindow:
    return SessionWindow(
        session_id=f"CME_6E_{day.isoformat()}",
        trading_date=day,
        open_ts_ns=_timestamp_ns(day, 13),
        close_ts_ns=_timestamp_ns(day, 21),
        active_instrument_id=instrument_id,
    )


def _midpoint_path(session_index: int, minute_index: int, *, contract_base: int) -> int:
    """Deterministic persistent random-walk-like path with a bounded quiet regime."""

    normal_steps = (2, 0, -2, 4, 2, -4, 0, 2, -2, -2, 4, 0, 2, -4, 2, 0)
    quiet_steps = (0, 0, 0, 1, 0, 0, 0, -1)
    value = contract_base + session_index * 7
    for index in range(minute_index + 1):
        if 255 <= index < 345:
            step = quiet_steps[(index + session_index) % len(quiet_steps)]
        else:
            step = normal_steps[(index + session_index * 3) % len(normal_steps)]
            if session_index == 2:
                step = -step
        value += step
    return value


def _engineering_midpoint(day: date, minute_index: int, base_midpoint: int) -> int:
    starts = _ENGINEERING_PATTERN_STARTS.get(day, ())
    if not starts:
        return base_midpoint
    first = starts[0]
    last = starts[-1]
    # A three-bar pullback (13 ticks down), a short cluster of adjacent 5m
    # continuation triggers, then a delayed +20 tick passage.  The delay keeps
    # adjacent raw events overlapping while producing candidate-specific exits.
    if first - 15 <= minute_index < first:
        pullback_minute = minute_index - (first - 15)
        return base_midpoint - (pullback_minute * 13 // 14)
    if first <= minute_index <= last + 4:
        return base_midpoint + 5
    if last + 5 <= minute_index < last + 15:
        return base_midpoint + 5
    if last + 15 <= minute_index < last + 25:
        return base_midpoint + 25
    return base_midpoint


def _build_unchecked(epoch: EpochConfig) -> MarketFixture:
    if epoch.fixture_version != _FIXTURE_VERSION:
        raise M0aDataError(f"unsupported deterministic fixture version: {epoch.fixture_version}")

    instruments = (
        InstrumentMetadata(
            instrument_id=_OLD_CONTRACT,
            symbol="6EU22",
            tick_size_numerator=1,
            tick_size_denominator=200_000,
            expiry_ts_ns=_timestamp_ns(date(2022, 9, 16), 21),
        ),
        InstrumentMetadata(
            instrument_id=_NEW_CONTRACT,
            symbol="6EZ22",
            tick_size_numerator=1,
            tick_size_denominator=200_000,
            expiry_ts_ns=_timestamp_ns(date(2022, 12, 16), 21),
        ),
    )
    sessions = (
        _session(date(2022, 8, 29), _OLD_CONTRACT),
        _session(date(2022, 8, 30), _OLD_CONTRACT),
        _session(date(2022, 8, 31), _OLD_CONTRACT),
        _session(date(2022, 9, 1), _NEW_CONTRACT),
        _session(date(2022, 9, 2), _NEW_CONTRACT),  # Friday/session-close case.
    )
    previous_day_volumes = (
        PreviousDayVolume(
            trading_date=date(2022, 8, 29),
            observed_date=date(2022, 8, 26),
            volumes=((_OLD_CONTRACT, 15_000), (_NEW_CONTRACT, 4_000)),
            selected_instrument_id=_OLD_CONTRACT,
        ),
        PreviousDayVolume(
            trading_date=date(2022, 8, 30),
            observed_date=date(2022, 8, 29),
            volumes=((_OLD_CONTRACT, 16_000), (_NEW_CONTRACT, 5_000)),
            selected_instrument_id=_OLD_CONTRACT,
        ),
        PreviousDayVolume(
            trading_date=date(2022, 8, 31),
            observed_date=date(2022, 8, 30),
            volumes=((_OLD_CONTRACT, 15_000), (_NEW_CONTRACT, 9_000)),
            selected_instrument_id=_OLD_CONTRACT,
        ),
        PreviousDayVolume(
            trading_date=date(2022, 9, 1),
            observed_date=date(2022, 8, 31),
            volumes=((_OLD_CONTRACT, 9_000), (_NEW_CONTRACT, 17_000)),
            selected_instrument_id=_NEW_CONTRACT,
        ),
        PreviousDayVolume(
            trading_date=date(2022, 9, 2),
            observed_date=date(2022, 9, 1),
            volumes=((_OLD_CONTRACT, 6_000), (_NEW_CONTRACT, 18_000)),
            selected_instrument_id=_NEW_CONTRACT,
        ),
    )
    roll_guards = (
        RollGuard(
            instrument_id=_OLD_CONTRACT,
            start_ts_ns=_timestamp_ns(date(2022, 8, 31), 19),
            end_ts_ns=_timestamp_ns(date(2022, 8, 31), 21),
            reason="precommitted_roll_guard_before_contract_switch",
        ),
        RollGuard(
            instrument_id=_NEW_CONTRACT,
            start_ts_ns=_timestamp_ns(date(2022, 9, 1), 13),
            end_ts_ns=_timestamp_ns(date(2022, 9, 1), 14),
            reason="precommitted_roll_guard_after_contract_switch",
        ),
    )

    raw_events: list[QuoteEvent] = []
    provisional_index = 0
    for session_index, session in enumerate(sessions):
        contract_base = 20_000 if session.active_instrument_id == _OLD_CONTRACT else 20_300
        for minute_index in range(8 * 60):
            ts_ns = session.open_ts_ns + (minute_index * 60 + 1) * _NS_PER_SECOND
            base_midpoint = _midpoint_path(
                session_index,
                minute_index,
                contract_base=contract_base,
            )
            mid_ticks = _engineering_midpoint(
                session.trading_date,
                minute_index,
                base_midpoint,
            )
            bid_size = 10 + ((minute_index * 3 + session_index) % 17)
            ask_size = 9 + ((minute_index * 5 + session_index * 2) % 19)
            bid_depth = bid_size * 8 + (minute_index % 13)
            ask_depth = ask_size * 8 + ((minute_index * 2) % 11)
            if any(
                start <= minute_index <= start + 4
                for start in _ENGINEERING_PATTERN_STARTS.get(session.trading_date, ())
            ):
                bid_depth = 400
                ask_depth = 100
            buy_aggressor = (minute_index + session_index) % 2 == 0
            raw_events.append(
                QuoteEvent(
                    event_index=provisional_index,
                    ts_ns=ts_ns,
                    instrument_id=session.active_instrument_id,
                    session_id=session.session_id,
                    bid_ticks=mid_ticks - 1,
                    ask_ticks=mid_ticks + 1,
                    bid_size_l1=bid_size,
                    ask_size_l1=ask_size,
                    bid_depth_l10=bid_depth,
                    ask_depth_l10=ask_depth,
                    trade_price_ticks=mid_ticks + (1 if buy_aggressor else -1),
                    trade_size=1 + minute_index % 5,
                    trade_action="TRADE",
                    trade_aggressor_side="BUY" if buy_aggressor else "SELL",
                )
            )
            provisional_index += 1

    # Both barriers are possible in this aggregate second.  Ordered events make
    # the long side hit a passive-TP trade-through first and the short side hit
    # its stop first, exercising the raw-event fallback deterministically.
    ambiguity_session = sessions[1]
    ambiguity_second = _timestamp_ns(ambiguity_session.trading_date, 16, 10, 30)
    anchor = _midpoint_path(1, 190, contract_base=20_000)
    for offset_ns, mid_ticks in ((100_000_000, anchor + 30), (200_000_000, anchor - 30)):
        raw_events.append(
            QuoteEvent(
                event_index=provisional_index,
                ts_ns=ambiguity_second + offset_ns,
                instrument_id=ambiguity_session.active_instrument_id,
                session_id=ambiguity_session.session_id,
                bid_ticks=mid_ticks - 1,
                ask_ticks=mid_ticks + 1,
                bid_size_l1=20,
                ask_size_l1=20,
                bid_depth_l10=180,
                ask_depth_l10=180,
                trade_price_ticks=mid_ticks,
                trade_size=5,
                trade_action="TRADE",
                trade_aggressor_side="BUY" if mid_ticks > anchor else "SELL",
            )
        )
        provisional_index += 1

    ordered = sorted(raw_events, key=lambda event: (event.ts_ns, event.event_index))
    events = tuple(replace(event, event_index=index) for index, event in enumerate(ordered))
    return MarketFixture(
        fixture_version=epoch.fixture_version,
        source_data_version=epoch.dataset_version,
        dataset_hash=epoch.dataset_hash,
        instruments=instruments,
        sessions=sessions,
        previous_day_volumes=previous_day_volumes,
        roll_guards=roll_guards,
        quote_events=events,
    )


def build_fixture(epoch: EpochConfig) -> MarketFixture:
    """Build the exact normal/roll/Friday fixture committed by ``epoch``."""

    epoch.verify_unchanged()
    fixture = _build_unchecked(epoch)
    if fixture.content_sha256 != epoch.dataset_hash:
        raise M0aDataError(
            "fixture content does not match the precommitted dataset_hash: "
            f"expected={epoch.dataset_hash}, actual={fixture.content_sha256}"
        )
    return fixture
