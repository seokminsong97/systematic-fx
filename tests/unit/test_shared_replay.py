from __future__ import annotations

import json
from datetime import date

import pytest

from systematic_fx.backtest.barriers import (
    BARRIER_TICKS,
    EXPECTED_CELL_COUNT,
    BarrierOutcome,
    Direction,
    ExecutableQuote,
)
from systematic_fx.backtest.economics import EntryStatus
from systematic_fx.backtest.shared_replay import (
    EXECUTION_SCENARIOS,
    FIRST_TOUCH_ACTIVE_SESSIONS,
    SharedExecutableQuote,
    SharedReplay,
    SharedReplayError,
    SignalSeed,
)

SECOND = 1_000_000_000
SOURCE_DATE = date(2022, 1, 3)


def _seed(
    signal_id: str,
    *,
    decision_seconds: float = 0,
    direction: Direction = Direction.LONG,
    contract: str = "6EH2",
) -> SignalSeed:
    return SignalSeed(
        signal_id=signal_id,
        decision_ts_recv_ns=int(decision_seconds * SECOND),
        utc_month="2022-01",
        direction=direction,
        contract_key=contract,
    )


def _event(
    event_index: int,
    seconds: float,
    bid: int | None,
    ask: int | None,
    *,
    contract: str = "6EH2",
    session: int = 0,
    valid: bool = True,
    terminal: bool = False,
    source_date: date = SOURCE_DATE,
) -> SharedExecutableQuote:
    return SharedExecutableQuote(
        contract_key=contract,
        quote=ExecutableQuote(
            event_index=event_index,
            ts_recv_ns=int(seconds * SECOND),
            best_bid_ticks=bid,
            best_ask_ticks=ask,
            valid=valid,
        ),
        source_date=source_date,
        session_ordinal=session,
        sequence=event_index,
        terminal=terminal,
    )


def _record(
    replay: SharedReplay,
    signal_id: str,
    scenario_id: str,
    *,
    take_profit: int = 24,
    stop_loss: int = 24,
):
    return next(
        record
        for record in replay.result_records()
        if record.signal_id == signal_id
        and record.scenario_id == scenario_id
        and record.take_profit_ticks == take_profit
        and record.stop_loss_ticks == stop_loss
    )


def _scenario_entry_events(
    *,
    contract: str = "6EH2",
    start_index: int = 0,
    direction: Direction = Direction.LONG,
):
    return [
        _event(start_index, 0.0, 100, 101, contract=contract),
        *_routed_scenario_entry_events(
            start_index=start_index + 1,
            decision_seconds=0.0,
            contract=contract,
            direction=direction,
        ),
    ]


def _routed_scenario_entry_events(
    *,
    start_index: int,
    decision_seconds: float,
    contract: str = "6EH2",
    direction: Direction = Direction.LONG,
):
    # A fresh pre-eligibility quote freezes each limit, then the first routed
    # attempt improves far enough to absorb the full adverse stress inside it.
    prices = (
        ((104, 105), (100, 101), (102, 103), (100, 101), (102, 103), (99, 100))
        if direction is Direction.LONG
        else ((99, 100), (100, 101), (98, 99), (100, 101), (98, 99), (101, 102))
    )
    offsets = (0.9, 1.000000001, 1.4, 1.500000001, 1.9, 2.000000001)
    return [
        _event(
            start_index + offset,
            decision_seconds + seconds,
            *price,
            contract=contract,
        )
        for offset, (seconds, price) in enumerate(zip(offsets, prices, strict=True))
    ]


def test_long_short_full_grid_and_auditable_prices() -> None:
    replay = SharedReplay(
        [
            _seed("long", contract="LONG_CONTRACT"),
            _seed("short", direction=Direction.SHORT, contract="SHORT_CONTRACT"),
        ]
    )
    replay.process(
        [
            _event(0, 0.0, 100, 101, contract="LONG_CONTRACT"),
            _event(1, 0.0, 200, 201, contract="SHORT_CONTRACT"),
            _event(2, 1.0, 100, 101, contract="LONG_CONTRACT"),
            _event(3, 1.0, 200, 201, contract="SHORT_CONTRACT"),
            _event(4, 1.5, 100, 101, contract="LONG_CONTRACT"),
            _event(5, 1.5, 200, 201, contract="SHORT_CONTRACT"),
            _event(6, 2.0, 100, 101, contract="LONG_CONTRACT"),
            _event(7, 2.0, 200, 201, contract="SHORT_CONTRACT"),
            _event(8, 3.0, 130, 131, contract="LONG_CONTRACT"),
            _event(9, 3.0, 229, 230, contract="SHORT_CONTRACT"),
            _event(10, 4.0, 235, 236, contract="SHORT_CONTRACT"),
            _event(11, 4.5, 235, 236, contract="SHORT_CONTRACT"),
            _event(12, 5.0, 235, 236, contract="SHORT_CONTRACT"),
            _event(13, 6.0, 110, 111, contract="LONG_CONTRACT", terminal=True),
            _event(14, 6.0, 235, 236, contract="SHORT_CONTRACT", terminal=True),
        ]
    )
    results = replay.finish()

    assert len(results) == 2 * len(EXECUTION_SCENARIOS) * EXPECTED_CELL_COUNT
    assert {
        (record.take_profit_ticks, record.stop_loss_ticks)
        for record in results
        if record.signal_id == "long" and record.scenario_id == "BASELINE"
    } == {(take_profit, stop_loss) for take_profit in BARRIER_TICKS for stop_loss in BARRIER_TICKS}
    assert replay.source_stream_passes == 1

    long_record = _record(replay, "long", "BASELINE")
    assert long_record.entry_fill_price_ticks == 101
    assert long_record.buying_price_ticks == 101
    assert long_record.selling_price_ticks == 125
    assert long_record.loss_price_ticks == 77
    assert long_record.portfolio_outcome is BarrierOutcome.TP_FIRST
    assert long_record.exit_fill_price_ticks == 125
    assert long_record.entry_ref is not None
    assert long_record.entry_ref.event_index == 2
    assert long_record.fill_ref is not None
    assert long_record.fill_ref.event_index == 8

    short_record = _record(replay, "short", "BASELINE")
    assert short_record.entry_fill_price_ticks == 200
    assert short_record.buying_price_ticks == 176
    assert short_record.selling_price_ticks == 200
    assert short_record.loss_price_ticks == 224
    assert short_record.first_touch_outcome is BarrierOutcome.STOP_FIRST
    assert short_record.portfolio_outcome is BarrierOutcome.STOP_FIRST
    assert short_record.trigger_ref is not None
    assert short_record.trigger_ref.event_index == 9
    assert short_record.fill_ref is not None
    assert short_record.fill_ref.event_index == 10
    assert short_record.exit_fill_price_ticks == 236


def test_scenario_entry_adversity_stop_latency_and_minimum_adverse_fill() -> None:
    replay = SharedReplay([_seed("stress")])
    replay.process(
        [
            _event(0, 0.0, 100, 105),
            _event(1, 0.9, 100, 105),
            _event(2, 1.1, 99, 100),
            _event(3, 1.4, 103, 104),
            _event(4, 1.6, 101, 102),
            _event(5, 1.9, 102, 103),
            _event(6, 2.1, 99, 100),
            _event(7, 3.0, 70, 71),
            _event(8, 4.0, 75, 76),
            _event(9, 4.5, 80, 81),
            _event(10, 5.0, 60, 61),
            _event(11, 6.0, 60, 61, terminal=True),
        ]
    )
    replay.finish()

    baseline = _record(replay, "stress", "BASELINE")
    moderate = _record(replay, "stress", "MODERATE_COMBINED")
    severe = _record(replay, "stress", "SEVERE_DIAGNOSTIC")

    assert [
        baseline.entry_fill_price_ticks,
        moderate.entry_fill_price_ticks,
        severe.entry_fill_price_ticks,
    ] == [
        100,
        103,
        102,
    ]
    assert [
        baseline.entry_ref.event_index,
        moderate.entry_ref.event_index,
        severe.entry_ref.event_index,
    ] == [  # type: ignore[union-attr]
        2,
        4,
        6,
    ]
    assert [
        baseline.fill_ref.event_index,
        moderate.fill_ref.event_index,
        severe.fill_ref.event_index,
    ] == [  # type: ignore[union-attr]
        8,
        9,
        10,
    ]
    assert baseline.exit_fill_price_ticks == 74
    assert moderate.exit_fill_price_ticks == 75
    assert severe.exit_fill_price_ticks == 60


def test_stressed_entry_adversity_cannot_walk_beyond_frozen_ioc_limit() -> None:
    replay = SharedReplay([_seed("capped-stress")])
    replay.process(
        [
            _event(0, 0.0, 100, 101),
            _event(1, 1.0, 100, 101),
            _event(2, 1.5, 100, 101),
            _event(3, 2.0, 100, 101),
            _event(4, 3.0, 100, 101, terminal=True),
        ]
    )
    replay.finish()

    baseline = _record(replay, "capped-stress", "BASELINE")
    moderate = _record(replay, "capped-stress", "MODERATE_COMBINED")
    severe = _record(replay, "capped-stress", "SEVERE_DIAGNOSTIC")

    assert baseline.entry_status is EntryStatus.ENTRY_FILLED
    for stressed in (moderate, severe):
        assert stressed.entry_status is EntryStatus.ENTRY_NOT_FILLED
        assert stressed.no_fill_reason == "PRICE_OUTSIDE_LIMIT"
        assert stressed.entry_fill_price_ticks is None
        assert stressed.entry_limit_price_ticks == 101


def test_same_timestamp_take_profit_and_stop_is_always_stop_first() -> None:
    replay = SharedReplay([_seed("tie")])
    replay.process(
        [
            *_scenario_entry_events(),
            # The favorable event has the lower event index.  The adverse event
            # at the same receive timestamp must still win every cell tie.
            _event(7, 3.0, 400, 401),
            _event(8, 3.0, -100, -99),
            _event(9, 4.0, -100, -99),
            _event(10, 4.5, -100, -99),
            _event(11, 5.0, -100, -99),
        ]
    )
    results = replay.finish()

    assert len(results) == len(EXECUTION_SCENARIOS) * EXPECTED_CELL_COUNT
    assert {record.portfolio_outcome for record in results} == {BarrierOutcome.STOP_FIRST}
    assert {record.trigger_ref.event_index for record in results if record.trigger_ref} == {8}


def test_events_after_entry_with_same_timestamp_are_replayed_in_order() -> None:
    replay = SharedReplay([_seed("entry-tie")])
    replay.process(
        [
            _event(0, 0.0, 100, 101),
            _event(1, 1.0, 100, 101),
            _event(2, 1.0, 130, 131),
            _event(3, 1.0, 70, 71),
            _event(4, 1.5, 100, 101),
            _event(5, 2.0, 100, 101),
            _event(6, 2.0, 70, 71),
            _event(7, 2.5, 70, 71),
            _event(8, 3.0, 70, 71),
            _event(9, 4.0, 70, 71, terminal=True),
        ]
    )
    replay.finish()

    baseline = _record(replay, "entry-tie", "BASELINE")
    assert baseline.entry_ref is not None
    assert baseline.entry_ref.event_index == 1
    assert baseline.first_touch_outcome is BarrierOutcome.STOP_FIRST
    assert baseline.trigger_ref is not None
    assert baseline.trigger_ref.event_index == 3


def test_occupied_signal_is_logged_per_cell_and_later_signal_can_enter() -> None:
    replay = SharedReplay(
        [
            _seed("owner"),
            _seed("blocked", decision_seconds=0.5),
            _seed("later", decision_seconds=3.0),
        ]
    )
    replay.process(
        [
            *_scenario_entry_events(),
            _event(7, 3.0, 400, 401),
            *_routed_scenario_entry_events(start_index=8, decision_seconds=3.0),
            _event(14, 6.0, 100, 101, terminal=True),
        ]
    )
    results = replay.finish()

    blocked = [record for record in results if record.signal_id == "blocked"]
    later = [record for record in results if record.signal_id == "later"]
    assert len(blocked) == len(EXECUTION_SCENARIOS) * EXPECTED_CELL_COUNT
    assert {record.entry_status for record in blocked} == {EntryStatus.SKIPPED_OCCUPIED}
    assert {record.occupying_signal_id for record in blocked} == {"owner"}
    assert {record.no_fill_reason for record in blocked} == {"LOG_AND_SKIP_OCCUPIED"}
    assert len(later) == len(EXECUTION_SCENARIOS) * EXPECTED_CELL_COUNT
    assert {record.entry_status for record in later} == {EntryStatus.ENTRY_FILLED}


def test_partial_row_exit_creates_partial_owner_mask_for_next_signal() -> None:
    replay = SharedReplay([_seed("first"), _seed("partial", decision_seconds=3.0)])
    replay.process(
        [
            *_scenario_entry_events(),
            # Only TP=24 crosses in every scenario; the other 21 TP rows stay occupied.
            _event(7, 3.0, 130, 131),
            *_routed_scenario_entry_events(start_index=8, decision_seconds=3.0),
            _event(14, 6.0, 100, 101, terminal=True),
        ]
    )
    results = replay.finish()

    for scenario in EXECUTION_SCENARIOS:
        partial = [
            record
            for record in results
            if record.signal_id == "partial" and record.scenario_id == scenario.scenario_id
        ]
        filled = [record for record in partial if record.entry_status is EntryStatus.ENTRY_FILLED]
        skipped = [
            record for record in partial if record.entry_status is EntryStatus.SKIPPED_OCCUPIED
        ]
        assert len(filled) == len(BARRIER_TICKS)
        assert {record.take_profit_ticks for record in filled} == {24}
        assert len(skipped) == EXPECTED_CELL_COUNT - len(BARRIER_TICKS)


def test_invalid_ioc_attempt_is_no_fill_without_retry() -> None:
    replay = SharedReplay([_seed("no-fill")])
    replay.process(
        [
            _event(0, 0.0, 100, 101),
            _event(1, 1.1, None, None, valid=False),
            # The next physical quote is executable, but retry is forbidden.
            _event(2, 1.2, 100, 101),
        ]
    )
    results = replay.finish()

    assert len(results) == len(EXECUTION_SCENARIOS) * EXPECTED_CELL_COUNT
    assert {record.entry_status for record in results} == {EntryStatus.ENTRY_NOT_FILLED}
    baseline = _record(replay, "no-fill", "BASELINE")
    assert baseline.no_fill_reason == "INVALID_ENTRY_ATTEMPT_BBO"
    assert baseline.attempt_ref is not None
    assert baseline.attempt_ref.event_index == 1
    assert baseline.failure_ref == baseline.attempt_ref
    assert all(record.entry_ref is None for record in results)


def test_stale_decision_quote_is_explicit_no_fill() -> None:
    replay = SharedReplay([_seed("stale-decision", decision_seconds=2.0)])
    replay.process(
        [
            _event(0, 0.5, 100, 101),
            _event(1, 3.0, 100, 101),
        ]
    )
    results = replay.finish()

    assert {record.no_fill_reason for record in results} == {"STALE_DECISION_QUOTE"}
    assert all(record.decision_ref is not None for record in results)
    assert {record.decision_ref.event_index for record in results if record.decision_ref} == {0}


def test_invalid_quote_during_route_blocks_every_scenario() -> None:
    replay = SharedReplay([_seed("route-invalid")])
    replay.process(
        [
            _event(0, 0.0, 100, 101),
            _event(1, 0.5, None, None, valid=False),
            _event(2, 1.0, 100, 101),
        ]
    )
    results = replay.finish()

    assert {record.no_fill_reason for record in results} == {"INVALID_BBO_DURING_ROUTE"}
    assert {record.route_event_count for record in results} == {1}
    assert {record.failure_ref.event_index for record in results if record.failure_ref} == {1}


def test_route_gap_over_one_second_is_stale_even_with_fresh_decision() -> None:
    replay = SharedReplay([_seed("route-stale", decision_seconds=2.0)])
    replay.process(
        [
            _event(0, 1.5, 100, 101),
            _event(1, 2.7, 100, 101),
        ]
    )
    results = replay.finish()

    assert {record.no_fill_reason for record in results} == {"STALE_BBO_DURING_ROUTE"}
    assert {record.maximum_route_quote_gap_ns for record in results} == {1_200_000_000}


@pytest.mark.parametrize(
    ("direction", "attempt_bid", "attempt_ask", "expected_limit"),
    [
        (Direction.LONG, 101, 102, 101),
        (Direction.SHORT, 99, 100, 100),
    ],
)
def test_first_ioc_price_worse_than_frozen_eligibility_limit_does_not_fill(
    direction: Direction,
    attempt_bid: int,
    attempt_ask: int,
    expected_limit: int,
) -> None:
    replay = SharedReplay([_seed("outside", direction=direction)])
    replay.process(
        [
            _event(0, 0.0, 100, 101),
            _event(1, 1.1, attempt_bid, attempt_ask),
        ]
    )
    replay.finish()

    baseline = _record(replay, "outside", "BASELINE")
    assert baseline.entry_status is EntryStatus.ENTRY_NOT_FILLED
    assert baseline.no_fill_reason == "PRICE_OUTSIDE_LIMIT"
    assert baseline.entry_limit_price_ticks == expected_limit
    assert baseline.eligibility_ref is not None
    assert baseline.eligibility_ref.event_index == 0
    assert baseline.attempt_ref is not None
    assert baseline.attempt_ref.event_index == 1


def test_exact_decision_event_is_seen_after_prior_positions_exit() -> None:
    replay = SharedReplay([_seed("old"), _seed("exact-next", decision_seconds=3.0)])
    replay.process(
        [
            *_scenario_entry_events(),
            # This quote exits every old cell and is also the new signal's
            # right-closed decision snapshot.
            _event(7, 3.0, 400, 401),
            *_routed_scenario_entry_events(start_index=8, decision_seconds=3.0),
            _event(14, 6.0, 100, 101, terminal=True),
        ]
    )
    results = replay.finish()

    next_records = [record for record in results if record.signal_id == "exact-next"]
    assert {record.entry_status for record in next_records} == {EntryStatus.ENTRY_FILLED}
    assert {record.decision_ref.event_index for record in next_records if record.decision_ref} == {
        7
    }
    assert not any(record.entry_status is EntryStatus.SKIPPED_OCCUPIED for record in next_records)


def test_scenario_take_profit_trade_through_is_independently_enforced() -> None:
    replay = SharedReplay([_seed("trade-through")])
    replay.process(
        [
            *_scenario_entry_events(),
            _event(7, 3.0, 127, 128),
            _event(8, 4.0, 127, 128, terminal=True),
        ]
    )
    replay.finish()

    assert _record(replay, "trade-through", "BASELINE").portfolio_outcome is BarrierOutcome.TP_FIRST
    assert (
        _record(replay, "trade-through", "MODERATE_COMBINED").portfolio_outcome
        is BarrierOutcome.TP_FIRST
    )
    assert (
        _record(replay, "trade-through", "SEVERE_DIAGNOSTIC").portfolio_outcome
        is BarrierOutcome.TERMINAL_EXIT
    )


def test_first_touch_censor_does_not_close_or_rewrite_portfolio_position() -> None:
    replay = SharedReplay([_seed("censored")])
    replay.process(
        [
            *_scenario_entry_events(),
            _event(
                7,
                5.0,
                100,
                101,
                session=FIRST_TOUCH_ACTIVE_SESSIONS,
                source_date=date(2022, 2, 1),
            ),
            _event(
                8,
                6.0,
                400,
                401,
                session=FIRST_TOUCH_ACTIVE_SESSIONS,
                source_date=date(2022, 2, 1),
            ),
        ]
    )
    results = replay.finish()

    assert len(results) == len(EXECUTION_SCENARIOS) * EXPECTED_CELL_COUNT
    assert {record.first_touch_outcome for record in results} == {BarrierOutcome.CENSORED}
    assert {record.portfolio_outcome for record in results} == {BarrierOutcome.TP_FIRST}
    assert {
        record.first_touch_censor_ref.session_ordinal
        for record in results
        if record.first_touch_censor_ref is not None
    } == {FIRST_TOUCH_ACTIVE_SESSIONS}
    assert all(record.completion_ts_recv_ns == 6 * SECOND for record in results)


def test_terminal_exit_uses_executable_side_and_scenario_adversity() -> None:
    replay = SharedReplay([_seed("terminal", direction=Direction.SHORT)])
    replay.process(
        [
            *_scenario_entry_events(direction=Direction.SHORT),
            _event(7, 3.0, 104, 105, terminal=True),
        ]
    )
    results = replay.finish()

    assert {record.portfolio_outcome for record in results} == {BarrierOutcome.TERMINAL_EXIT}
    assert {
        _record(replay, "terminal", scenario.scenario_id).exit_fill_price_ticks
        for scenario in EXECUTION_SCENARIOS
    } == {105, 106, 107}
    assert all(record.terminal_ref == record.fill_ref for record in results)
    assert {record.terminal_ref.event_index for record in results if record.terminal_ref} == {7}


def test_same_timestamp_ordinary_entry_before_terminal_fills_then_exits() -> None:
    replay = SharedReplay([_seed("terminal-order")])
    replay.process(
        [
            _event(0, 0.0, 100, 101),
            _event(1, 1.0, 100, 101),
            _event(2, 1.0, 99, 100, terminal=True),
        ]
    )
    replay.finish()

    baseline = _record(replay, "terminal-order", "BASELINE")
    assert baseline.entry_status is EntryStatus.ENTRY_FILLED
    assert baseline.entry_ref is not None
    assert baseline.entry_ref.event_index == 1
    assert baseline.portfolio_outcome is BarrierOutcome.TERMINAL_EXIT
    assert baseline.terminal_ref is not None
    assert baseline.terminal_ref.event_index == 2

    moderate = _record(replay, "terminal-order", "MODERATE_COMBINED")
    assert moderate.entry_status is EntryStatus.ENTRY_NOT_FILLED
    assert moderate.no_fill_reason == "TERMINAL_BEFORE_ENTRY"
    assert moderate.failure_ref is not None
    assert moderate.failure_ref.event_index == 2


def test_ordering_session_and_post_terminal_invariants() -> None:
    replay = SharedReplay([])
    replay.process([_event(2, 1.0, 100, 101)])
    with pytest.raises(SharedReplayError, match="strictly ordered"):
        replay.process([_event(1, 1.0, 100, 101)])

    replay = SharedReplay([])
    replay.process([_event(1, 1.0, 100, 101, session=2)])
    with pytest.raises(SharedReplayError, match="session_ordinal"):
        replay.process([_event(2, 2.0, 100, 101, session=1)])

    replay = SharedReplay([])
    replay.process([_event(1, 1.0, 100, 101, terminal=True)])
    with pytest.raises(SharedReplayError, match="after its terminal"):
        replay.process([_event(2, 2.0, 100, 101)])

    replay = SharedReplay([])
    replay.process([_event(1, 1.0, 100, 101)])
    replay.complete_source_date(SOURCE_DATE)
    with pytest.raises(SharedReplayError, match="completed source-date"):
        replay.process([_event(2, 1.0, 100, 101, source_date=date(2022, 1, 4))])


def test_checkpoint_resume_equals_uninterrupted_even_inside_timestamp_tie() -> None:
    events = [
        *_scenario_entry_events(),
        _event(7, 3.0, 400, 401),
        _event(8, 3.0, -100, -99),
        _event(9, 4.0, -100, -99),
        _event(10, 4.5, -100, -99),
        _event(11, 5.0, -100, -99),
    ]

    uninterrupted = SharedReplay([_seed("resume")])
    uninterrupted.process(events)
    uninterrupted_results = [record.as_dict() for record in uninterrupted.finish()]

    resumed = SharedReplay([_seed("resume")])
    resumed.process(events[:5])
    # The checkpoint contains only the first half of a same-timestamp tie group.
    payload = json.loads(json.dumps(resumed.checkpoint(), sort_keys=True))
    resumed = SharedReplay.from_checkpoint(payload)
    resumed.process(events[5:8])
    # Exercise restoration of accumulated records as well as live positions.
    payload = json.loads(json.dumps(resumed.checkpoint(), sort_keys=True))
    resumed = SharedReplay.from_checkpoint(payload)
    resumed.process(events[8:])
    resumed_results = [record.as_dict() for record in resumed.finish()]

    assert resumed_results == uninterrupted_results
    assert json.dumps(resumed.checkpoint(), sort_keys=True) == json.dumps(
        uninterrupted.checkpoint(), sort_keys=True
    )


def test_result_record_public_mapping_round_trip_is_lossless() -> None:
    replay = SharedReplay([_seed("record-round-trip")])
    replay.process(
        [
            *_scenario_entry_events(),
            _event(7, 2.5, 100, 101, terminal=True),
        ]
    )
    replay.finish()
    record = replay.result_records()[0]

    assert SharedReplay.record_from_dict(record.as_dict()) == record


def test_complete_source_date_flushes_buffer_and_resumes_identically() -> None:
    next_date = date(2022, 1, 4)
    first_date_events = _scenario_entry_events()
    next_date_events = [
        _event(7, 3.0, 400, 401, source_date=next_date, session=1),
    ]

    uninterrupted = SharedReplay([_seed("date-boundary")])
    uninterrupted.process(first_date_events)
    uninterrupted.complete_source_date(SOURCE_DATE)
    uninterrupted.process(next_date_events)
    uninterrupted.complete_source_date(next_date)
    uninterrupted_results = [record.as_dict() for record in uninterrupted.finish()]

    resumed = SharedReplay([_seed("date-boundary")])
    resumed.process(first_date_events)
    resumed.complete_source_date(SOURCE_DATE)
    boundary_payload = resumed.checkpoint()
    assert boundary_payload["buffer"] == []
    resumed = SharedReplay.from_checkpoint(json.loads(json.dumps(boundary_payload, sort_keys=True)))
    resumed.process(next_date_events)
    resumed.complete_source_date(next_date)
    resumed_results = [record.as_dict() for record in resumed.finish()]

    assert resumed_results == uninterrupted_results


def test_drained_daily_records_plus_resume_equal_no_drain_replay() -> None:
    next_date = date(2022, 1, 4)
    signals = [_seed("a", contract="A"), _seed("b", contract="B")]
    first_date_events = [
        _event(0, 0.0, 100, 101, contract="A"),
        _event(1, 0.0, 100, 101, contract="B"),
        _event(2, 1.0, 100, 101, contract="A"),
        _event(3, 1.0, 100, 101, contract="B"),
        _event(4, 1.5, 100, 101, contract="A"),
        _event(5, 1.5, 100, 101, contract="B"),
        _event(6, 2.0, 100, 101, contract="A"),
        _event(7, 2.0, 100, 101, contract="B"),
        _event(8, 2.5, 400, 401, contract="A"),
    ]
    next_date_events = [
        _event(
            9,
            3.0,
            400,
            401,
            contract="B",
            source_date=next_date,
            session=1,
        )
    ]

    no_drain = SharedReplay(signals)
    no_drain.process(first_date_events)
    no_drain.complete_source_date(SOURCE_DATE)
    no_drain.process(next_date_events)
    no_drain.complete_source_date(next_date)
    expected = [record.as_dict() for record in no_drain.finish()]

    drained = SharedReplay(signals)
    drained.process(first_date_events)
    drained.complete_source_date(SOURCE_DATE)
    first_shard = [record.as_dict() for record in drained.drain_result_records()]
    assert drained.result_records() == ()
    payload = json.loads(json.dumps(drained.checkpoint(), sort_keys=True))
    assert payload["records"] == []
    drained = SharedReplay.from_checkpoint(payload)
    drained.process(next_date_events)
    drained.complete_source_date(next_date)
    drained.finish()
    second_shard = [record.as_dict() for record in drained.drain_result_records()]

    assert first_shard + second_shard == expected
    assert drained.active_position_count == 0
    assert drained.pending_entry_count == 0


def test_stationary_10k_events_advance_owner_groups_not_484_cells() -> None:
    stationary_event_count = 10_002
    replay = SharedReplay([_seed("work-bound")])
    events = [*_scenario_entry_events()]
    events.extend(
        SharedExecutableQuote(
            contract_key="6EH2",
            quote=ExecutableQuote(
                event_index=7 + offset,
                ts_recv_ns=2 * SECOND + offset + 1,
                best_bid_ticks=100,
                best_ask_ticks=101,
            ),
            source_date=SOURCE_DATE,
            session_ordinal=0,
            sequence=7 + offset,
        )
        for offset in range(stationary_event_count)
    )
    events.append(
        _event(
            7 + stationary_event_count,
            3.0,
            100,
            101,
            terminal=True,
        )
    )

    replay.process(events)
    results = replay.finish()

    assert len(results) == len(EXECUTION_SCENARIOS) * EXPECTED_CELL_COUNT
    assert replay.active_owner_group_count == 0
    # Three owner surfaces, not 1,452 cell positions, receive each stationary
    # timestamp.  Capacity work is two O(1) bisects per group/event.
    assert replay.owner_group_advance_count == 3 * stationary_event_count + 7
    assert replay.threshold_capacity_work_count == 6 * stationary_event_count + 12
