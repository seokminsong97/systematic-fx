from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from fractions import Fraction

import pytest

import scripts.ai_pattern_holdout_engine as engine_module
from scripts.ai_pattern_holdout_engine import (
    MATCHED_RELAXATION_LEVELS,
    MISSING_PRIOR_20_HISTORY,
    BarWithOutcomeSpan,
    CircularNullResult,
    EvaluationSummary,
    ExecutionSpec,
    FrozenProposal,
    GroupSummary,
    HoldoutEvaluationError,
    PatternEvaluation,
    ProposalMaskSet,
    SignalMask,
    StageCandidateSummary,
    StageEvaluationResult,
    StageMaskBundle,
    benjamini_hochberg,
    build_stage_masks,
    causal_range_quartiles,
    circular_shift_null_mask,
    evaluate_signal_masks,
    exact_one_sided_sign_test,
    holm_step_down,
    matched_random_null_mask,
    morphology_features,
    paired_daily_sign_test,
    select_stage_result,
)
from systematic_fx.features.bars import ONE_SECOND_NS, TradeBar
from systematic_fx.research.ai_pattern_discovery import AndRule, RulePredicate
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
    first_offset_seconds: int = 0,
) -> TradeBar:
    high = open_ticks if high_ticks is None else high_ticks
    low = open_ticks if low_ticks is None else low_ticks
    close = open_ticks if close_ticks is None else close_ticks
    width = timeframe * ONE_SECOND_NS
    first = start_ns + first_offset_seconds * ONE_SECOND_NS
    return TradeBar(
        timeframe_seconds=timeframe,
        segment_id=segment_id,
        contract=contract,
        source_date=datetime.fromtimestamp(start_ns // ONE_SECOND_NS, tz=UTC).date(),
        start_ns=start_ns,
        end_ns=start_ns + width,
        first_trade_ns=first,
        last_trade_ns=first,
        open_ticks=open_ticks,
        high_ticks=high,
        low_ticks=low,
        close_ticks=close,
        trade_count=1,
        volume=1,
        observed_subbars=1 if timeframe == 1 else timeframe,
    )


def _wrapped(bar: TradeBar, *, span: int = 1) -> BarWithOutcomeSpan:
    return BarWithOutcomeSpan(bar, span)


def _three_fives(*, entry_first_offset: int = 5, span: int = 1) -> tuple[BarWithOutcomeSpan, ...]:
    return (
        _wrapped(_bar(300, BASE_NS, open_ticks=990, high_ticks=1_010, low_ticks=980), span=span),
        _wrapped(
            _bar(300, BASE_NS + 300 * ONE_SECOND_NS, first_offset_seconds=entry_first_offset),
            span=span,
        ),
        _wrapped(
            _bar(300, BASE_NS + 3_900 * ONE_SECOND_NS, open_ticks=1_010),
            span=span,
        ),
    )


def _mask(values: tuple[bool, ...], kind: str = "REAL") -> SignalMask:
    return SignalMask(
        f"{'a' * 64}:{kind.lower()}",
        "a" * 64,
        kind,  # type: ignore[arg-type]
        values,
    )


def _evaluate(
    ones: tuple[BarWithOutcomeSpan, ...],
    *,
    direction: str = "LONG",
    fives: tuple[BarWithOutcomeSpan, ...] | None = None,
) -> PatternEvaluation:
    signal_bars = _three_fives() if fives is None else fives
    mask = _mask(tuple(index == 0 for index in range(len(signal_bars))))
    return evaluate_signal_masks(
        signal_bars,
        ones,
        {mask.key: mask},
        {mask.key: direction},  # type: ignore[dict-item]
        allowed_stage_tail_end_ns=max(item.bar.end_ns for item in signal_bars),
        reporting_dates={item.bar.source_date for item in signal_bars},
    )[mask.key]


def test_morphology_exactly_matches_discovery_integer_contract() -> None:
    bar = _bar(300, BASE_NS, open_ticks=100, high_ticks=110, low_ticks=90, close_ticks=105)

    features = morphology_features(bar)

    assert features.as_dict() == {
        "absolute_body_ppm": 250_000,
        "close_location_ppm": 750_000,
        "lower_wick_ppm": 500_000,
        "range_ticks": 20,
        "signed_body_ppm": 250_000,
        "upper_wick_ppm": 250_000,
    }
    assert morphology_features(_bar(300, BASE_NS)).as_dict() == {
        "absolute_body_ppm": 0,
        "close_location_ppm": 500_000,
        "lower_wick_ppm": 0,
        "range_ticks": 0,
        "signed_body_ppm": 0,
        "upper_wick_ppm": 0,
    }


def test_long_tp_uses_first_trade_second_trade_through_and_full_cost() -> None:
    entry_start = BASE_NS + 305 * ONE_SECOND_NS
    ones = (
        _wrapped(_bar(1, entry_start, open_ticks=1_000)),
        _wrapped(_bar(1, entry_start + ONE_SECOND_NS, high_ticks=1_035, low_ticks=1_000)),
    )

    result = _evaluate(ones)

    trade = result.trades[0]
    assert trade.entry_start_ns == entry_start
    assert trade.entry_fill_ticks == 1_002
    assert trade.disposition == "TP_FIRST"
    assert trade.exit_fill_ticks == 1_034
    assert trade.gross_pnl_ticks == 32
    assert trade.fully_loaded_net_pnl_ticks == 22


def test_same_second_stop_first_and_gap_minimum_adverse_are_conservative() -> None:
    entry_start = BASE_NS + 305 * ONE_SECOND_NS
    ones = (
        _wrapped(_bar(1, entry_start, open_ticks=1_000)),
        _wrapped(
            _bar(
                1,
                entry_start + ONE_SECOND_NS,
                open_ticks=970,
                high_ticks=1_035,
                low_ticks=960,
                close_ticks=970,
            )
        ),
    )

    trade = _evaluate(ones).trades[0]

    assert trade.disposition == "STOP_FIRST"
    assert trade.same_second_stop_first
    assert trade.exit_fill_ticks == 970
    assert trade.gross_pnl_ticks == -32
    assert trade.fully_loaded_net_pnl_ticks == -42


def test_short_tp_is_symmetric() -> None:
    entry_start = BASE_NS + 305 * ONE_SECOND_NS
    ones = (
        _wrapped(_bar(1, entry_start, open_ticks=1_000)),
        _wrapped(_bar(1, entry_start + ONE_SECOND_NS, high_ticks=1_000, low_ticks=965)),
    )

    trade = _evaluate(ones, direction="SHORT").trades[0]

    assert trade.entry_fill_ticks == 998
    assert trade.exit_fill_ticks == 966
    assert trade.gross_pnl_ticks == 32
    assert trade.fully_loaded_net_pnl_ticks == 22


def test_five_minute_path_boundary_index_is_built_once_per_multi_signal_evaluation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fives = tuple(_wrapped(_bar(300, BASE_NS + index * 300 * ONE_SECOND_NS)) for index in range(5))
    entry_starts = (fives[1].bar.start_ns, fives[3].bar.start_ns)
    ones = tuple(
        _wrapped(_bar(1, start, high_ticks=1_035, low_ticks=1_000)) for start in entry_starts
    )
    mask = _mask((True, False, True, False, False))
    original = engine_module._five_minute_path_end_ns
    calls = 0

    def spy(values: object) -> object:
        nonlocal calls
        calls += 1
        return original(values)  # type: ignore[arg-type]

    monkeypatch.setattr(engine_module, "_five_minute_path_end_ns", spy)

    result = evaluate_signal_masks(
        fives,
        ones,
        {mask.key: mask},
        {mask.key: "LONG"},
        allowed_stage_tail_end_ns=fives[-1].bar.end_ns,
    )[mask.key]

    assert calls == 1
    assert result.summary.trade_count == 2


def test_timeout_uses_exact_horizon_timestamp_and_last_known_close() -> None:
    entry_start = BASE_NS + 305 * ONE_SECOND_NS
    horizon = entry_start + 3_600 * ONE_SECOND_NS
    ones = (
        _wrapped(_bar(1, entry_start, open_ticks=1_000)),
        _wrapped(_bar(1, horizon - ONE_SECOND_NS, open_ticks=1_010, close_ticks=1_010)),
    )

    trade = _evaluate(ones).trades[0]

    assert trade.disposition == "TIMEOUT"
    assert trade.exit_ns == horizon
    assert trade.exit_fill_ticks == 1_008
    assert trade.fully_loaded_net_pnl_ticks == -4


def test_timeout_daily_accounting_uses_exit_date_not_signal_date() -> None:
    base = int(datetime(2024, 1, 2, 23, 25, tzinfo=UTC).timestamp()) * ONE_SECOND_NS
    entry_bucket = base + 300 * ONE_SECOND_NS
    fives = (
        _wrapped(_bar(300, base)),
        _wrapped(_bar(300, entry_bucket, first_offset_seconds=5)),
        _wrapped(_bar(300, entry_bucket + 3_600 * ONE_SECOND_NS)),
    )
    entry_start = entry_bucket + 5 * ONE_SECOND_NS
    horizon = entry_start + 3_600 * ONE_SECOND_NS
    ones = (
        _wrapped(_bar(1, entry_start)),
        _wrapped(_bar(1, horizon - ONE_SECOND_NS, close_ticks=1_001, high_ticks=1_001)),
    )

    result = _evaluate(ones, fives=fives)

    assert result.trades[0].signal_date == date(2024, 1, 2)
    assert result.trades[0].exit_date == date(2024, 1, 3)
    assert dict(result.summary.daily_trade_counts)[date(2024, 1, 3)] == 1
    assert dict(result.summary.daily_trade_counts)[date(2024, 1, 2)] == 0


@pytest.mark.parametrize("changed", ("contract", "span", "segment"))
def test_entry_never_links_across_contract_span_or_segment(changed: str) -> None:
    fives = list(_three_fives())
    entry = fives[1]
    if changed == "contract":
        entry = _wrapped(replace(entry.bar, contract="6EM4"), span=entry.outcome_span_id)
    elif changed == "span":
        entry = _wrapped(entry.bar, span=2)
    else:
        entry = _wrapped(replace(entry.bar, segment_id=2), span=entry.outcome_span_id)
    fives[1] = entry
    entry_start = entry.bar.first_trade_ns // ONE_SECOND_NS * ONE_SECOND_NS
    ones = (
        _wrapped(
            _bar(1, entry_start, contract=entry.bar.contract, segment_id=entry.bar.segment_id),
            span=entry.outcome_span_id,
        ),
    )

    with pytest.raises(HoldoutEvaluationError, match="same-path"):
        _evaluate(ones, fives=tuple(fives))


def test_circular_shift_is_scoped_nonidentical_and_cardinality_preserving() -> None:
    bars = tuple(_wrapped(_bar(300, BASE_NS + index * 300 * ONE_SECOND_NS)) for index in range(8))
    eligible = (True,) * len(bars)
    real = _mask((True, False, False, True, False, False, False, False))

    result = circular_shift_null_mask(
        real,
        bars,
        eligible,
        master_seed=20260813,
        stage_key="SEARCH",
    )

    assert isinstance(result, CircularNullResult)
    assert result.sample_eligible and result.mask is not None
    assert result.mask.signal_count == real.signal_count
    assert result.mask.values != real.values
    assert result.mask.null_seed_sha256s


@pytest.mark.parametrize("scope_field", ("contract", "span", "segment"))
def test_circular_shift_never_moves_a_signal_across_path_scope(scope_field: str) -> None:
    bars: list[BarWithOutcomeSpan] = []
    for index in range(8):
        second_scope = index >= 4
        contract = "6EM4" if second_scope and scope_field == "contract" else "6EH4"
        segment = 2 if second_scope and scope_field == "segment" else 1
        span = 2 if second_scope and scope_field == "span" else 1
        bars.append(
            _wrapped(
                _bar(
                    300,
                    BASE_NS + index * 300 * ONE_SECOND_NS,
                    contract=contract,
                    segment_id=segment,
                ),
                span=span,
            )
        )
    real = _mask((True, False, False, False, True, False, False, False))

    result = circular_shift_null_mask(
        real,
        tuple(bars),
        (True,) * 8,
        master_seed=20260813,
        stage_key="SEARCH",
    )

    assert result.mask is not None
    assert sum(result.mask.values[:4]) == 1
    assert sum(result.mask.values[4:]) == 1


def test_circular_shift_fails_closed_for_selected_singleton_scope() -> None:
    bars = (_wrapped(_bar(300, BASE_NS)),)
    result = circular_shift_null_mask(
        _mask((True,)), bars, (True,), master_seed=7, stage_key="SEARCH"
    )

    assert not result.sample_eligible
    assert result.mask is None
    assert result.reason == "CIRCULAR_GROUP_HAS_NO_NONIDENTICAL_ROTATION"


def test_causal_quartile_uses_only_prior_twenty_contiguous_bars() -> None:
    bars = tuple(
        _wrapped(
            _bar(
                300,
                BASE_NS + index * 300 * ONE_SECOND_NS,
                open_ticks=1_000,
                high_ticks=1_001 + index,
                low_ticks=1_000,
                close_ticks=1_000,
            )
        )
        for index in range(22)
    )

    quartiles = causal_range_quartiles(bars)

    assert quartiles[:20] == (None,) * 20
    assert quartiles[20] == 3


@pytest.mark.parametrize("scope_field", ("contract", "span", "segment"))
def test_matched_control_never_crosses_contract_span_or_segment(scope_field: str) -> None:
    first = [
        _wrapped(_bar(300, BASE_NS + index * 300 * ONE_SECOND_NS, high_ticks=1_004))
        for index in range(21)
    ]
    second_start = BASE_NS + 30 * 300 * ONE_SECOND_NS
    second = [
        _wrapped(
            _bar(
                300,
                second_start + index * 300 * ONE_SECOND_NS,
                high_ticks=1_004,
                contract="6EM4" if scope_field == "contract" else "6EH4",
                segment_id=2 if scope_field == "segment" else 1,
            ),
            span=2 if scope_field == "span" else 1,
        )
        for index in range(21)
    ]
    bars = tuple(first + second)
    values = [False] * len(bars)
    values[20] = True
    eligible = [False] * len(bars)
    eligible[20] = True
    eligible[-1] = True

    result = matched_random_null_mask(
        _mask(tuple(values)),
        bars,
        tuple(eligible),
        master_seed=20260813,
        stage_key="SEARCH",
    )

    assert not result.sample_eligible
    assert result.reason == "INSUFFICIENT_CAUSAL_MATCHED_POOL"


def test_missing_history_is_a_preserved_matched_stratum_not_a_dropped_signal() -> None:
    identity = "a" * 64
    proposal = FrozenProposal(
        1,
        identity,
        "LONG",
        AndRule((RulePredicate("range_ticks", "GE", 8),)),
    )
    bars = tuple(
        _wrapped(
            _bar(
                300,
                BASE_NS + index * 300 * ONE_SECOND_NS,
                high_ticks=1_008 if index in (0, 22) else 1_004,
            )
        )
        for index in range(30)
    )
    group_by_date = {bars[0].bar.source_date: "G1"}

    bundle = build_stage_masks(
        "SEARCH",
        bars,
        (proposal,),
        (identity,),
        ExecutionSpec(),
        20260813,
        group_by_date,
    )

    item = bundle.proposal_masks[0]
    assert item.rule_support_count == 2
    assert item.real.signal_count == 2
    assert item.sample_eligible
    assert item.circular_shift is not None
    assert item.matched_random is not None
    assert item.circular_shift.signal_count == item.real.signal_count
    assert item.matched_random.signal_count == item.real.signal_count
    pair_by_real = {pair.real_start_ns: pair for pair in item.matched_pairs}
    early_pair = pair_by_real[bars[0].bar.start_ns]
    assert early_pair.real_range_stratum == MISSING_PRIOR_20_HISTORY
    assert early_pair.matched_range_stratum == MISSING_PRIOR_20_HISTORY
    assert early_pair.relaxation_policy == MATCHED_RELAXATION_LEVELS[0]


def test_matched_relaxation_level_one_retains_missing_history_stratum() -> None:
    bars = tuple(
        _wrapped(_bar(300, BASE_NS + index * 300 * ONE_SECOND_NS, high_ticks=1_004))
        for index in range(8)
    )
    real_values = tuple(index == 5 for index in range(len(bars)))
    eligible = tuple(index in (5, 6) for index in range(len(bars)))

    result = matched_random_null_mask(
        _mask(real_values),
        bars,
        eligible,
        master_seed=20260813,
        stage_key="SEARCH",
    )

    assert result.sample_eligible
    assert result.pairs[0].relaxation_level == 1
    assert result.pairs[0].relaxation_policy == MATCHED_RELAXATION_LEVELS[1]
    assert result.pairs[0].real_range_stratum == MISSING_PRIOR_20_HISTORY
    assert result.pairs[0].matched_range_stratum == MISSING_PRIOR_20_HISTORY


def test_matched_relaxation_level_two_can_drop_bucket_and_causal_stratum() -> None:
    bars = tuple(
        _wrapped(_bar(300, BASE_NS + index * 300 * ONE_SECOND_NS, high_ticks=1_004))
        for index in range(22)
    )
    real_values = tuple(index == 20 for index in range(len(bars)))
    eligible = tuple(index in (0, 20) for index in range(len(bars)))

    result = matched_random_null_mask(
        _mask(real_values),
        bars,
        eligible,
        master_seed=20260813,
        stage_key="SEARCH",
    )

    assert result.sample_eligible
    assert result.pairs[0].relaxation_level == 2
    assert result.pairs[0].relaxation_policy == MATCHED_RELAXATION_LEVELS[2]
    assert result.pairs[0].real_range_stratum == "Q3"
    assert result.pairs[0].matched_range_stratum == MISSING_PRIOR_20_HISTORY


def test_signal_mask_artifact_commits_exact_vector_and_selected_coordinates() -> None:
    mask = _mask((False, True, False, True))

    document = mask.as_dict()

    assert document["value_count"] == 4
    assert document["selected_indexes"] == [1, 3]
    assert document["mask_values_sha256"] == canonical_sha256([False, True, False, True])
    assert document["selected_index_sha256"] == canonical_sha256([1, 3])


def test_exact_sign_tests_and_family_corrections_remain_rational() -> None:
    assert exact_one_sided_sign_test([1, 2, 3, 4, 5]) == Fraction(1, 32)
    assert exact_one_sided_sign_test([0, 0]) == 1
    assert paired_daily_sign_test(
        ((date(2024, 1, 2), 2), (date(2024, 1, 3), 0)),
        ((date(2024, 1, 2), 1), (date(2024, 1, 3), 0)),
    ) == Fraction(1, 2)

    bh = benjamini_hochberg({"a": Fraction(1, 100), "b": Fraction(1, 50), "c": Fraction(1, 2)})
    holm = holm_step_down({"a": Fraction(1, 100), "b": Fraction(1, 50), "c": Fraction(1, 2)})

    assert [item.rejected for item in bh] == [True, True, False]
    assert [item.rejected for item in holm] == [True, True, False]
    assert all(isinstance(item.adjusted_p_value, Fraction) for item in (*bh, *holm))


def _summary(
    *, net: int, groups: int = 4, profit_factor: Fraction = Fraction(2)
) -> EvaluationSummary:
    group_values = tuple(
        GroupSummary(
            f"G{index}",
            50,
            20,
            30,
            20,
            20,
            600,
            net // groups,
            600,
            100,
            Fraction(net, groups * 30),
            profit_factor,
            False,
        )
        for index in range(groups)
    )
    days = tuple((date(2024, 1, 1 + index), 1) for index in range(20))
    return EvaluationSummary(
        raw_signal_count=200,
        signal_day_count=50,
        median_signals_per_signal_day=Fraction(4),
        trade_count=120,
        skipped_occupied_count=0,
        active_entry_day_count=50,
        active_exit_day_count=50,
        contract_count=5,
        take_profit_first_count=80,
        stop_first_count=40,
        timeout_count=0,
        same_second_stop_first_count=0,
        gross_pnl_ticks=net + 1_200,
        variable_cost_ticks=600,
        allocated_fixed_cost_ticks=600,
        fully_loaded_net_pnl_ticks=net,
        maximum_drawdown_ticks=max(1, net // 2),
        net_gains_ticks=2_000,
        net_losses_ticks=1_000,
        expected_value_ticks=Fraction(net, 120),
        profit_factor=profit_factor,
        profit_factor_unbounded=False,
        daily_net_ticks=days,
        daily_trade_counts=days,
        daily_signal_counts=days,
        group_summaries=group_values,
    )


def test_select_search_result_applies_bh_economic_gates_and_rank() -> None:
    identity = "a" * 64
    real = _summary(net=1_200)
    null = _summary(net=0)
    candidate = StageCandidateSummary(
        identity,
        real,
        null,
        null,
        Fraction(1, 1_000),
        Fraction(1, 1_000),
        Fraction(1, 1_000),
        Fraction(1, 1_000),
        200,
        50,
        tuple((date(2024, 1, 1 + index), 4) for index in range(20)),
        tuple((f"G{index}", 50) for index in range(4)),
    )
    proposal = FrozenProposal(
        1,
        identity,
        "LONG",
        AndRule((RulePredicate("range_ticks", "GE", 1),)),
    )
    mask = SignalMask(f"{identity}:real", identity, "REAL", (True, False))
    shifted = SignalMask(
        f"{identity}:shift", identity, "CIRCULAR_SHIFT", (False, True), ("b" * 64,)
    )
    matched = SignalMask(
        f"{identity}:match", identity, "MATCHED_RANDOM", (False, True), ("c" * 64,)
    )
    mask_set = ProposalMaskSet(
        proposal,
        mask,
        shifted,
        matched,
        (),
        200,
        50,
        candidate.rule_support_daily_counts,
        candidate.rule_support_group_counts,
        True,
        None,
    )
    masks = StageMaskBundle("SEARCH", "d" * 64, (True, True), (mask_set,))
    raw = StageEvaluationResult("SEARCH", (candidate,), (), "RAW")

    selected = select_stage_result("SEARCH", raw, masks, (identity,))

    assert selected.finalist_proposal_sha256s == (identity,)
    assert selected.classification == "SEARCH_FINALISTS_SELECTED"
    assert selected.multiplicity_decisions[0].rejected


def test_build_stage_masks_keeps_rule_support_separate_from_evaluable_mask() -> None:
    identity = "a" * 64
    proposal = FrozenProposal(
        1,
        identity,
        "LONG",
        AndRule((RulePredicate("range_ticks", "GE", 1),)),
    )
    bars = tuple(
        _wrapped(
            _bar(
                300,
                BASE_NS + index * 300 * ONE_SECOND_NS,
                high_ticks=1_004,
            )
        )
        for index in range(24)
    )
    group = {bars[0].bar.source_date: "G1"}

    bundle = build_stage_masks("SEARCH", bars, (proposal,), (identity,), ExecutionSpec(), 7, group)

    item = bundle.proposal_masks[0]
    assert item.rule_support_count == 24
    assert item.real.signal_count == 23  # final row has no contiguous next entry bucket
    assert bundle.as_dict()["five_minute_view_sha256"] == bundle.five_minute_view_sha256


def _gate_mask_set(identity: str, *, eligible: bool = True) -> ProposalMaskSet:
    proposal = FrozenProposal(
        1,
        identity,
        "LONG",
        AndRule((RulePredicate("range_ticks", "GE", 1),)),
    )
    real = SignalMask(f"{identity}:real", identity, "REAL", (True, False))
    shifted = (
        SignalMask(
            f"{identity}:shift",
            identity,
            "CIRCULAR_SHIFT",
            (False, True),
            ("b" * 64,),
        )
        if eligible
        else None
    )
    matched = (
        SignalMask(
            f"{identity}:match",
            identity,
            "MATCHED_RANDOM",
            (False, True),
            ("c" * 64,),
        )
        if eligible
        else None
    )
    return ProposalMaskSet(
        proposal,
        real,
        shifted,
        matched,
        (),
        200,
        50,
        (),
        (),
        eligible,
        None if eligible else "SAMPLE_INELIGIBLE",
    )


def test_walk_forward_losing_fold_boundary_is_three_halves() -> None:
    identity = "d" * 64
    positive = GroupSummary(
        "P",
        50,
        30,
        60,
        30,
        30,
        500,
        100,
        300,
        200,
        Fraction(5, 3),
        Fraction(3, 2),
        False,
    )
    losing = replace(
        positive,
        group_key="L",
        gross_pnl_ticks=450,
        fully_loaded_net_pnl_ticks=-150,
        net_gains_ticks=450,
        net_losses_ticks=600,
        expected_value_ticks=Fraction(-5, 2),
        profit_factor=Fraction(3, 4),
    )
    folds = tuple(replace(positive, group_key=f"F{index}") for index in range(4)) + (losing,)
    real = replace(
        _summary(net=250, groups=5),
        trade_count=300,
        active_entry_day_count=150,
        active_exit_day_count=150,
        contract_count=5,
        maximum_drawdown_ticks=100,
        profit_factor=Fraction(2),
        group_summaries=folds,
    )
    null = replace(real, fully_loaded_net_pnl_ticks=0)
    candidate = StageCandidateSummary(
        identity,
        real,
        null,
        null,
        Fraction(1, 1_000),
        Fraction(1, 1_000),
        Fraction(1, 1_000),
        Fraction(1, 1_000),
    )
    masks = StageMaskBundle("WALK_FORWARD", "e" * 64, (True, True), (_gate_mask_set(identity),))
    raw = StageEvaluationResult("WALK_FORWARD", (candidate,), (), "RAW")

    selected = select_stage_result("WALK_FORWARD", raw, masks, (identity,))

    assert selected.finalist_proposal_sha256s == (identity,)
    assert "WORST_LOSING_FOLD_TOO_LARGE" not in selected.gate_decisions[0].failure_reasons


def test_holdout_mixed_ineligible_and_hard_failure_is_fail_not_inconclusive() -> None:
    failed_identity = "d" * 64
    ineligible_identity = "e" * 64
    failed_real = replace(
        _summary(net=-100, groups=2),
        trade_count=100,
        active_entry_day_count=50,
        contract_count=2,
        group_summaries=tuple(
            replace(item, fully_loaded_net_pnl_ticks=-50)
            for item in _summary(net=-100, groups=2).group_summaries
        ),
    )
    null = replace(failed_real, fully_loaded_net_pnl_ticks=-200)
    failed = StageCandidateSummary(
        failed_identity,
        failed_real,
        null,
        null,
        Fraction(1, 1_000),
        Fraction(1, 1_000),
        Fraction(1, 1_000),
        Fraction(1, 1_000),
    )
    masks = StageMaskBundle(
        "HOLDOUT",
        "f" * 64,
        (True, True),
        (
            _gate_mask_set(failed_identity),
            _gate_mask_set(ineligible_identity, eligible=False),
        ),
    )

    selected = select_stage_result(
        "HOLDOUT",
        StageEvaluationResult("HOLDOUT", (failed,), (), "RAW"),
        masks,
        (failed_identity, ineligible_identity),
    )

    assert selected.classification == "ONE_SHOT_UNSEALED_BAR_HOLDOUT_DIAGNOSTIC_FAIL"
