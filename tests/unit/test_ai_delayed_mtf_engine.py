from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime

import pytest

import scripts.ai_delayed_mtf_engine as engine
from scripts.ai_delayed_mtf_engine import (
    CANDIDATE_CATALOG_COUNT,
    CANDIDATE_CATALOG_SHA256,
    DEFAULT_MASTER_NULL_SEED,
    EMA_SCALE,
    CandidateCatalog,
    DelayedMtfEngineError,
    FrozenCandidateMasks,
    FrozenStageMasks,
    build_delayed_mtf_candidate_catalog,
    delayed_mtf_engine_contract,
    evaluate_delayed_mtf_stage_parts,
    freeze_delayed_mtf_stage_masks,
)
from scripts.ai_pattern_holdout_engine import BarWithOutcomeSpan, SignalMask
from systematic_fx.features.bars import ONE_SECOND_NS, TradeBar
from systematic_fx.research.hypotheses import canonical_sha256

BASE_NS = int(datetime(2024, 1, 2, tzinfo=UTC).timestamp()) * ONE_SECOND_NS


def _bar(
    timeframe: int,
    start_ns: int,
    *,
    open_ticks: int = 1_000,
    high_ticks: int | None = None,
    low_ticks: int | None = None,
    close_ticks: int | None = None,
    contract: str = "6EH4",
    segment_id: int = 1,
    first_trade_ns: int | None = None,
    last_trade_ns: int | None = None,
) -> TradeBar:
    high = open_ticks if high_ticks is None else high_ticks
    low = open_ticks if low_ticks is None else low_ticks
    close = open_ticks if close_ticks is None else close_ticks
    first = start_ns if first_trade_ns is None else first_trade_ns
    last = first if last_trade_ns is None else last_trade_ns
    return TradeBar(
        timeframe_seconds=timeframe,
        segment_id=segment_id,
        contract=contract,
        source_date=datetime.fromtimestamp(start_ns // ONE_SECOND_NS, tz=UTC).date(),
        start_ns=start_ns,
        end_ns=start_ns + timeframe * ONE_SECOND_NS,
        first_trade_ns=first,
        last_trade_ns=last,
        open_ticks=open_ticks,
        high_ticks=high,
        low_ticks=low,
        close_ticks=close,
        trade_count=1,
        volume=1,
        observed_subbars=1 if timeframe == 1 else timeframe,
    )


def _wrapped(bar: TradeBar, span: int = 1) -> BarWithOutcomeSpan:
    return BarWithOutcomeSpan(bar, span)


def _constant_bars(
    timeframe: int,
    start_ns: int,
    count: int,
    *,
    price: int = 1_000,
    high_offset: int = 1,
    low_offset: int = 1,
) -> tuple[BarWithOutcomeSpan, ...]:
    return tuple(
        _wrapped(
            _bar(
                timeframe,
                start_ns + index * timeframe * ONE_SECOND_NS,
                open_ticks=price,
                high_ticks=price + high_offset,
                low_ticks=price - low_offset,
                close_ticks=price,
            )
        )
        for index in range(count)
    )


def _candidate(family: str, **parameters: int) -> engine.SymbolicCandidate:
    return next(
        item
        for item in build_delayed_mtf_candidate_catalog().candidates
        if item.family == family
        and item.direction == "LONG"
        and all(item.parameter(key) == value for key, value in parameters.items())
    )


def test_catalog_is_exact_canonical_100_member_family() -> None:
    catalog = build_delayed_mtf_candidate_catalog()

    assert isinstance(catalog, CandidateCatalog)
    assert len(catalog.candidates) == CANDIDATE_CATALOG_COUNT == 100
    assert catalog.catalog_sha256 == CANDIDATE_CATALOG_SHA256
    assert Counter(item.family for item in catalog.candidates) == {
        "DELAYED_MACD": 36,
        "COMPRESSION_BREAKOUT": 24,
        "TREND_PULLBACK_CONTINUATION": 24,
        "RANGE_REGIME_MEAN_REVERSION": 16,
    }
    assert tuple(item.selection_rank for item in catalog.candidates) == tuple(range(1, 101))
    assert catalog.as_dict() == build_delayed_mtf_candidate_catalog().as_dict()
    contract = delayed_mtf_engine_contract()
    assert contract["candidate_catalog_sha256"] == CANDIDATE_CATALOG_SHA256
    assert contract["execution"]["total_friction_ticks"] == 14  # type: ignore[index]
    assert contract["nulls"]["master_seed"] == DEFAULT_MASTER_NULL_SEED  # type: ignore[index]


@pytest.mark.parametrize(
    ("numerator", "denominator", "expected"),
    ((5, 2, 2), (7, 2, 4), (-5, 2, -2), (-7, 2, -4)),
)
def test_fixed_point_round_half_even_is_exact_for_signed_ties(
    numerator: int, denominator: int, expected: int
) -> None:
    assert engine._round_half_even(numerator, denominator) == expected


def test_one_hour_macd_warms_across_adjacent_active_dates_and_gap_resets() -> None:
    bars = _constant_bars(engine.ONE_HOUR, BASE_NS, 35)
    dates = tuple(sorted({item.bar.source_date for item in bars}))
    series = engine._stage_series(bars, dates)
    cache = engine._FeatureCache({engine.ONE_HOUR: series})

    histogram = cache.macd_histogram(engine.ONE_HOUR, 12, 26, 9)

    assert all(value is None for value in histogram[:33])
    assert histogram[33] == 0
    assert series.continues[24]

    fives = list(_constant_bars(engine.FIVE_MINUTES, BASE_NS, 7))
    del fives[3]
    five_series = engine._stage_series(tuple(fives), (fives[0].bar.source_date,))
    five_cache = engine._FeatureCache({engine.FIVE_MINUTES: five_series})
    ema = five_cache.ema(engine.FIVE_MINUTES, 3)
    assert ema[2] == 1_000 * EMA_SCALE
    assert ema[3] is None
    assert ema[5] == 1_000 * EMA_SCALE


def test_cross_date_bridge_rejects_nonadjacent_stage_date_and_over_96_hours() -> None:
    first = _wrapped(_bar(engine.ONE_HOUR, BASE_NS + 23 * engine.ONE_HOUR * ONE_SECOND_NS))
    six_days = 6 * 24 * engine.ONE_HOUR * ONE_SECOND_NS
    second = _wrapped(_bar(engine.ONE_HOUR, BASE_NS + six_days))
    dates = (first.bar.source_date, second.bar.source_date)

    series = engine._stage_series((first, second), dates)

    assert not series.continues[1]


class _FakeMacdCache:
    def __init__(self, values: tuple[int | None, ...]) -> None:
        self.values = values

    def macd_histogram(
        self, timeframe: int, fast: int, slow: int, signal: int
    ) -> tuple[int | None, ...]:
        del timeframe, fast, slow, signal
        return self.values


def test_macd_cross_waits_for_hour_and_cancels_when_persistence_dies() -> None:
    start = BASE_NS + 10 * engine.ONE_HOUR * ONE_SECOND_NS + 45 * 60 * ONE_SECOND_NS
    bars = _constant_bars(engine.FIVE_MINUTES, start, 3)
    series = engine._stage_series(bars, (bars[0].bar.source_date,))
    anchor_map = engine._anchor_map(bars)
    candidate = _candidate(
        "DELAYED_MACD",
        trigger_timeframe_seconds=engine.FIVE_MINUTES,
        fast_period=8,
        slow_period=21,
        signal_period=5,
        extra_delay_hours=0,
    )

    alive = engine._raw_macd_mask(
        candidate, series, _FakeMacdCache((-1, 1, 1)), anchor_map, len(bars)
    )
    dead = engine._raw_macd_mask(
        candidate, series, _FakeMacdCache((-1, 1, -1)), anchor_map, len(bars)
    )

    assert alive == (False, False, True)
    assert dead == (False, False, False)


def test_compression_uses_disjoint_context_windows_and_strict_breakout() -> None:
    contexts = tuple(
        _wrapped(
            _bar(
                engine.HALF_HOUR,
                BASE_NS + index * engine.HALF_HOUR * ONE_SECOND_NS,
                open_ticks=100,
                high_ticks=110 if index < 6 else 105,
                low_ticks=100,
                close_ticks=105 if index < 6 else 102,
            )
        )
        for index in range(12)
    )
    trigger_start = BASE_NS + 6 * engine.ONE_HOUR * ONE_SECOND_NS
    triggers = tuple(
        _wrapped(
            _bar(
                engine.FIVE_MINUTES,
                trigger_start + index * engine.FIVE_MINUTES * ONE_SECOND_NS,
                open_ticks=100,
                high_ticks=104 if index < 3 else 106,
                low_ticks=99,
                close_ticks=100 if index < 3 else 105,
            )
        )
        for index in range(4)
    )
    dates = (contexts[0].bar.source_date,)
    context_series = engine._stage_series(contexts, dates)
    trigger_series = engine._stage_series(triggers, dates)
    candidate = _candidate(
        "COMPRESSION_BREAKOUT",
        context_timeframe_seconds=engine.HALF_HOUR,
        trigger_timeframe_seconds=engine.FIVE_MINUTES,
        compression_window=6,
        breakout_window=3,
    )

    event = engine._compression_event(
        candidate,
        "LONG",
        trigger_series,
        context_series,
        3,
        engine._continuous_start(trigger_series),
        engine._continuous_start(context_series),
    )

    assert event == (104, 99)


def test_range_false_break_is_strict_and_zero_efficiency_qualifies() -> None:
    bars = list(_constant_bars(engine.FIVE_MINUTES, BASE_NS, 14, price=100))
    bars[-1] = _wrapped(
        _bar(
            engine.FIVE_MINUTES,
            bars[-1].bar.start_ns,
            open_ticks=100,
            high_ticks=101,
            low_ticks=98,
            close_ticks=100,
        )
    )
    series = engine._stage_series(tuple(bars), (bars[0].bar.source_date,))
    candidate = _candidate(
        "RANGE_REGIME_MEAN_REVERSION",
        trigger_timeframe_seconds=engine.FIVE_MINUTES,
        lookback=12,
        efficiency_numerator=1,
        efficiency_denominator=3,
    )

    event = engine._range_event(candidate, "LONG", series, 13, engine._continuous_start(series))

    assert event == (99, 101)


def test_pullback_requires_strict_reclaim_and_persists_only_with_live_htf_trend() -> None:
    contexts = tuple(
        _wrapped(
            _bar(
                engine.HALF_HOUR,
                BASE_NS + index * engine.HALF_HOUR * ONE_SECOND_NS,
                open_ticks=100 + index,
                high_ticks=101 + index,
                low_ticks=99 + index,
                close_ticks=100 + index,
            )
        )
        for index in range(23)
    )
    day = contexts[0].bar.source_date
    context_series = engine._stage_series(contexts, (day,))
    cache = engine._FeatureCache({engine.HALF_HOUR: context_series})
    candidate = _candidate(
        "TREND_PULLBACK_CONTINUATION",
        context_timeframe_seconds=engine.HALF_HOUR,
        trigger_timeframe_seconds=engine.FIVE_MINUTES,
        fast_period=8,
        slow_period=21,
        pullback_bars=2,
    )
    fast = cache.ema(engine.HALF_HOUR, 8)[-1]
    assert fast is not None
    pullback_close = fast // EMA_SCALE
    reclaim_close = pullback_close + 2
    trigger_start = contexts[-1].bar.end_ns
    triggers = tuple(
        _wrapped(
            _bar(
                engine.FIVE_MINUTES,
                trigger_start + index * engine.FIVE_MINUTES * ONE_SECOND_NS,
                open_ticks=close,
                high_ticks=close + 1,
                low_ticks=close - 1,
                close_ticks=close,
            )
        )
        for index, close in enumerate((pullback_close, pullback_close, reclaim_close))
    )
    trigger_series = engine._stage_series(triggers, (day,))

    event = engine._pullback_event(
        candidate,
        "LONG",
        trigger_series,
        context_series,
        2,
        engine._continuous_start(trigger_series),
        engine._continuous_start(context_series),
        cache,
    )

    assert event == ()
    assert engine._generic_persists(
        candidate,
        (),
        trigger_series,
        context_series,
        2,
        engine._continuous_start(context_series),
        cache,
    )

    equal_reclaim = list(triggers)
    equal_tick = fast // EMA_SCALE
    equal_reclaim[-1] = _wrapped(
        _bar(
            engine.FIVE_MINUTES,
            equal_reclaim[-1].bar.start_ns,
            open_ticks=equal_tick,
            high_ticks=equal_tick + 1,
            low_ticks=equal_tick - 1,
            close_ticks=equal_tick,
        )
    )
    equal_series = engine._stage_series(tuple(equal_reclaim), (day,))
    assert (
        engine._pullback_event(
            candidate,
            "LONG",
            equal_series,
            context_series,
            2,
            engine._continuous_start(equal_series),
            engine._continuous_start(context_series),
            cache,
        )
        is None
    )


def _hourly_opportunities(
    starts: tuple[int, ...], segments: tuple[int, ...]
) -> tuple[BarWithOutcomeSpan, ...]:
    return tuple(
        _wrapped(
            _bar(
                engine.FIVE_MINUTES,
                start,
                open_ticks=100,
                high_ticks=101,
                low_ticks=99,
                close_ticks=100,
                segment_id=segment,
            )
        )
        for start, segment in zip(starts, segments, strict=True)
    )


def test_circular_control_retains_invariant_group_but_is_globally_nonidentity() -> None:
    starts = tuple(BASE_NS + (55 + 60 * index) * 60 * ONE_SECOND_NS for index in range(6))
    fives = _hourly_opportunities(starts, (1, 1, 2, 2, 2, 2))
    candidate_id = build_delayed_mtf_candidate_catalog().candidate_ids[0]
    real = SignalMask(
        f"{candidate_id}:real",
        candidate_id,
        "REAL",
        (True, True, True, False, False, False),
    )

    control, reason = engine._circular_control(
        real,
        fives,
        (True,) * len(fives),
        seed=DEFAULT_MASTER_NULL_SEED,
        stage_key="SEARCH",
    )

    assert reason is None
    assert control is not None
    assert control.signal_count == real.signal_count
    assert control.values[:2] == (True, True)
    assert control.values != real.values


def test_matched_control_uses_l0_then_l8_with_exact_cardinality() -> None:
    candidate_id = build_delayed_mtf_candidate_catalog().candidate_ids[0]
    same_day = _hourly_opportunities(
        (
            BASE_NS + 55 * 60 * ONE_SECOND_NS,
            BASE_NS + 115 * 60 * ONE_SECOND_NS,
        ),
        (1, 1),
    )
    real = SignalMask(f"{candidate_id}:real", candidate_id, "REAL", (True, False))
    strata = {0: ("VOL_Q1", "RANGE"), 1: ("VOL_Q1", "RANGE")}

    l0, l0_pairs, reason = engine._matched_control(
        real,
        same_day,
        (True, True),
        strata,
        seed=DEFAULT_MASTER_NULL_SEED,
        stage_key="SEARCH",
    )

    assert reason is None
    assert l0 is not None and l0.signal_count == 1 and l0.values != real.values
    assert l0_pairs == ((0, 1, 0),)

    cross_day = _hourly_opportunities(
        (
            BASE_NS + 55 * 60 * ONE_SECOND_NS,
            BASE_NS + (24 * 60 + 12 * 60 + 55) * 60 * ONE_SECOND_NS,
        ),
        (1, 9),
    )
    l8, l8_pairs, reason = engine._matched_control(
        real,
        cross_day,
        (True, True),
        {0: ("VOL_Q1", "TREND_UP"), 1: ("VOL_Q4", "TREND_DOWN")},
        seed=DEFAULT_MASTER_NULL_SEED,
        stage_key="SEARCH",
    )

    assert reason is None
    assert l8 is not None and l8.signal_count == real.signal_count
    assert l8_pairs == ((0, 1, 8),)


def test_freeze_commits_raw_signal_that_lacks_three_hour_execution_tail() -> None:
    fives = list(_constant_bars(engine.FIVE_MINUTES, BASE_NS, 48, price=100))
    fives[-1] = _wrapped(
        _bar(
            engine.FIVE_MINUTES,
            fives[-1].bar.start_ns,
            open_ticks=100,
            high_ticks=101,
            low_ticks=98,
            close_ticks=100,
        )
    )
    halves = _constant_bars(engine.HALF_HOUR, BASE_NS, 8, price=100)
    hours = _constant_bars(engine.ONE_HOUR, BASE_NS, 4, price=100)
    candidate = _candidate(
        "RANGE_REGIME_MEAN_REVERSION",
        trigger_timeframe_seconds=engine.FIVE_MINUTES,
        lookback=12,
        efficiency_numerator=1,
        efficiency_denominator=3,
    )
    day = fives[0].bar.source_date

    frozen = freeze_delayed_mtf_stage_masks(
        "SEARCH",
        tuple(fives),
        halves,
        hours,
        decision_dates=(day,),
        allowed_stage_tail_end_ns=fives[-1].bar.end_ns,
        seed=DEFAULT_MASTER_NULL_SEED,
        candidate_ids=(candidate.candidate_id,),
        group_by_date={day: "B1"},
    )

    masks = frozen.candidate_masks[0]
    assert masks.raw_signal_count == 1
    assert masks.raw_signal_daily_counts == ((day, 1),)
    assert masks.raw_signal_group_counts == (("B1", 1),)
    assert masks.real.signal_count == 0
    assert frozen.as_dict()["commitment_sha256"] == frozen.commitment_sha256


def _manual_fixed_horizon_fixture() -> tuple[
    tuple[BarWithOutcomeSpan, ...], FrozenStageMasks, tuple[BarWithOutcomeSpan, ...]
]:
    start = BASE_NS + 55 * 60 * ONE_SECOND_NS
    fives_list = list(_constant_bars(engine.FIVE_MINUTES, start, 37, price=1_000))
    terminal = fives_list[36].bar
    fives_list[36] = _wrapped(
        _bar(
            engine.FIVE_MINUTES,
            terminal.start_ns,
            open_ticks=1_010,
            high_ticks=1_010,
            low_ticks=1_010,
            close_ticks=1_010,
        )
    )
    fives = tuple(fives_list)
    candidate = build_delayed_mtf_candidate_catalog().candidates[0]
    values = (True,) + (False,) * 36
    real = SignalMask(f"{candidate.candidate_id}:real", candidate.candidate_id, "REAL", values)
    day = fives[0].bar.source_date
    candidate_masks = FrozenCandidateMasks(
        candidate,
        real,
        None,
        None,
        (),
        values,
        1,
        ((day, 1),),
        (("F1", 1),),
        False,
        "CONTROLS_NOT_INCLUDED_IN_UNIT_FIXTURE",
    )
    five_sha = canonical_sha256(engine._bar_view_payload(fives))
    frozen = FrozenStageMasks(
        "SEARCH",
        CANDIDATE_CATALOG_SHA256,
        (candidate_masks,),
        values,
        five_sha,
        canonical_sha256({"fixture": "multi-timeframe"}),
    )
    entry_start = fives[1].bar.start_ns
    terminal_start = fives[36].bar.start_ns
    ones = (
        _wrapped(_bar(1, entry_start, open_ticks=1_000)),
        _wrapped(_bar(1, terminal_start, open_ticks=1_010, close_ticks=1_010)),
    )
    return fives, frozen, ones


def _manual_stage_for_positions(
    count: int, positions: tuple[int, ...]
) -> tuple[tuple[BarWithOutcomeSpan, ...], FrozenStageMasks]:
    start = BASE_NS + 55 * 60 * ONE_SECOND_NS
    fives = _constant_bars(engine.FIVE_MINUTES, start, count, price=1_000)
    candidate = build_delayed_mtf_candidate_catalog().candidates[0]
    values = tuple(index in positions for index in range(count))
    real = SignalMask(f"{candidate.candidate_id}:real", candidate.candidate_id, "REAL", values)
    day = fives[0].bar.source_date
    candidate_masks = FrozenCandidateMasks(
        candidate,
        real,
        None,
        None,
        (),
        values,
        len(positions),
        ((day, len(positions)),),
        (("F1", len(positions)),),
        False,
        "CONTROLS_NOT_INCLUDED_IN_UNIT_FIXTURE",
    )
    frozen = FrozenStageMasks(
        "SEARCH",
        CANDIDATE_CATALOG_SHA256,
        (candidate_masks,),
        values,
        canonical_sha256(engine._bar_view_payload(fives)),
        canonical_sha256({"fixture_positions": list(positions)}),
    )
    return fives, frozen


def test_fixed_horizon_uses_verified_terminal_and_exact_fourteen_tick_friction() -> None:
    fives, frozen, ones = _manual_fixed_horizon_fixture()
    day = fives[0].bar.source_date

    result = evaluate_delayed_mtf_stage_parts(
        "SEARCH",
        fives,
        (ones,),
        frozen,
        allowed_stage_tail_end_ns=fives[-1].bar.end_ns,
        reporting_dates=(day,),
        group_by_date={day: "F1"},
    )

    evaluation = result.candidates[0].real
    trade = evaluation.trades[0]
    assert trade.reference_pnl_ticks == 10
    assert trade.entry_fill_ticks == 1_002
    assert trade.exit_fill_ticks == 1_008
    assert trade.gross_pnl_ticks == 6
    assert trade.fully_loaded_net_pnl_ticks == -4
    assert evaluation.summary.daily_net_ticks == ((day, -4),)
    assert result.as_dict()["result_sha256"] == result.result_sha256


def test_fixed_horizon_censors_missing_terminal_instead_of_truncating() -> None:
    fives, frozen, ones = _manual_fixed_horizon_fixture()

    result = evaluate_delayed_mtf_stage_parts(
        "SEARCH",
        fives,
        (ones[:1],),
        frozen,
        allowed_stage_tail_end_ns=fives[-1].bar.end_ns,
    )

    evaluation = result.candidates[0].real
    assert evaluation.summary.trade_count == 0
    assert evaluation.summary.censored_signal_count == 1
    assert evaluation.censored_signals[0].reason == "TERMINAL_BAR_VERIFICATION_FAILED"


def test_occupancy_skips_strict_overlap_but_allows_entry_equal_to_prior_exit() -> None:
    fives, frozen = _manual_stage_for_positions(73, (0, 12, 36))
    ones = tuple(
        _wrapped(_bar(1, fives[index].bar.start_ns, open_ticks=1_000)) for index in (1, 36, 37, 72)
    )

    result = evaluate_delayed_mtf_stage_parts(
        "SEARCH",
        fives,
        (ones,),
        frozen,
        allowed_stage_tail_end_ns=fives[-1].bar.end_ns,
    )

    evaluation = result.candidates[0].real
    assert tuple(trade.signal_index for trade in evaluation.trades) == (0, 36)
    assert evaluation.summary.skipped_occupied_count == 1


def test_mask_occupancy_is_global_across_lineage_changes() -> None:
    # Contract/span/segment are deliberately absent from the occupancy helper:
    # changing lineage cannot open a second overlapping position for one mask.
    prior_exit_ns = BASE_NS + 4 * engine.ONE_HOUR * ONE_SECOND_NS
    different_lineage_entry = BASE_NS + 2 * engine.ONE_HOUR * ONE_SECOND_NS

    assert engine._entry_overlaps_mask_occupancy(prior_exit_ns, different_lineage_entry)
    assert not engine._entry_overlaps_mask_occupancy(prior_exit_ns, prior_exit_ns)


def test_censored_signal_does_not_occupy_later_entry() -> None:
    fives, frozen = _manual_stage_for_positions(49, (0, 12))
    ones = tuple(
        _wrapped(_bar(1, fives[index].bar.start_ns, open_ticks=1_000)) for index in (1, 13, 48)
    )

    result = evaluate_delayed_mtf_stage_parts(
        "SEARCH",
        fives,
        (ones,),
        frozen,
        allowed_stage_tail_end_ns=fives[-1].bar.end_ns,
    )

    evaluation = result.candidates[0].real
    assert tuple(item.signal_index for item in evaluation.censored_signals) == (0,)
    assert tuple(item.signal_index for item in evaluation.trades) == (12,)
    assert evaluation.summary.skipped_occupied_count == 0


def test_wrong_master_seed_is_rejected_before_mask_generation() -> None:
    fives = _constant_bars(engine.FIVE_MINUTES, BASE_NS, 48)
    halves = _constant_bars(engine.HALF_HOUR, BASE_NS, 8)
    hours = _constant_bars(engine.ONE_HOUR, BASE_NS, 4)
    day = fives[0].bar.source_date

    with pytest.raises(DelayedMtfEngineError, match="frozen master seed"):
        freeze_delayed_mtf_stage_masks(
            "SEARCH",
            fives,
            halves,
            hours,
            decision_dates=(day,),
            allowed_stage_tail_end_ns=fives[-1].bar.end_ns,
            seed="wrong",
            candidate_ids=(build_delayed_mtf_candidate_catalog().candidate_ids[0],),
        )
