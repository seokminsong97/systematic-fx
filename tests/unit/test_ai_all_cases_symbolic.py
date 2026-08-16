from __future__ import annotations

from collections import Counter
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from fractions import Fraction
from itertools import islice
from random import Random

import pytest

from campaigns.ai_all_cases_v1.symbolic import (
    ANCHOR_POLICY_RECIPE_SHA256,
    BASE_EVENT_CATALOG_SHA256,
    COMPLETE_STRATEGY_MAXIMUM,
    COMPLETE_STRATEGY_RECIPE_SHA256,
    CONTEXT_CATALOG_SHA256,
    DELAY_CATALOG_SHA256,
    DIRECT_TERMINAL_HORIZONS_SECONDS,
    ENTRY_CATALOG_SHA256,
    ENTRY_POLICY_COUNT,
    EXIT_CATALOG_SHA256,
    EXIT_POLICY_COUNT,
    EXPERT_FEATURE_FORMULA_SHA256,
    EXPERT_FEATURE_NAMES,
    LOGICAL_ANCHOR_POLICY_COUNT,
    REFERENCE_HORIZONS_SECONDS,
    REFERENCE_SCORE_CELL_COUNT,
    STAGE_A_OUTER_CHUNK_COUNT,
    STAGE_A_POLICY_ROWS_PER_CHUNK_MAXIMUM,
    TIME_FILTER_CATALOG_SHA256,
    AnchorRecord,
    CompleteEvaluationChunk,
    EventOccurrence,
    OneSecondPath,
    PolicyMask,
    ReferenceOutcomeSurface,
    RuleExitSchedule,
    RuleExitTimes,
    SelectedStrategyDetailRequest,
    SharedPathEvaluator,
    StructuralEligibilityLattice,
    build_base_event_catalog,
    build_candidate_policy_cube,
    build_causal_expert_feature_artifact,
    build_context_catalog,
    build_control_opportunity_lattice,
    build_delay_catalog,
    build_direct_opportunity_lattice,
    build_entry_catalog,
    build_exit_catalog,
    build_reference_outcome_surfaces,
    build_stage_a_chunk_plan,
    build_stage_b_chunk_plan,
    build_structural_eligibility_lattice,
    build_symbolic_stage,
    build_time_filter_catalog,
    causal_expert_feature_artifact_from_dict,
    complete_evaluation_chunk_from_dict,
    complete_evaluation_coverage_from_dict,
    complete_strategy_evaluation_from_dict,
    deduplicate_feature_masks,
    direct_opportunity_lattice_from_dict,
    evaluate_selected_strategy_details,
    freeze_entry_orders,
    freeze_feature_control_masks,
    freeze_structurally_eligible_policy_mask,
    frozen_control_masks_from_dict,
    iter_anchor_mask_batches,
    iter_anchor_policies,
    iter_complete_strategy_recipes,
    policy_mask_from_dict,
    score_stage_a_reference_horizons,
    select_stage_a_top256,
    select_symbolic_top24_for_meta,
    stage_a_selection_from_dict,
    stage_b_kernel_budget_projection,
    structural_eligibility_lattice_from_dict,
    structurally_eligible_policy_mask_from_dict,
    symbolic_engine_contract,
    verify_complete_evaluation_coverage,
)
from scripts.ai_pattern_holdout_engine import BarWithOutcomeSpan
from systematic_fx.features.bars import ONE_SECOND_NS, TradeBar
from systematic_fx.research.hypotheses import canonical_sha256


def _bars(
    timeframe: int,
    count: int,
    *,
    close_override: dict[int, int] | None = None,
    classified_volume: bool = True,
) -> tuple[BarWithOutcomeSpan, ...]:
    start = int(datetime(2022, 1, 3, tzinfo=UTC).timestamp()) * ONE_SECOND_NS
    width = timeframe * ONE_SECOND_NS
    output = []
    prior_close = 1_000
    for index in range(count):
        start_ns = start + index * width
        end_ns = start_ns + width
        close = 1_000 + ((index * 7) % 31) - 15
        if close_override and index in close_override:
            close = close_override[index]
        open_ticks = prior_close
        high = max(open_ticks, close) + 2
        low = min(open_ticks, close) - 2
        volume = 100 + index % 17
        buy_volume = 55 + index % 7 if classified_volume else None
        sell_volume = volume - buy_volume if classified_volume else None
        output.append(
            BarWithOutcomeSpan(
                TradeBar(
                    timeframe,
                    1,
                    "6EH2",
                    datetime.fromtimestamp(start_ns // ONE_SECOND_NS, tz=UTC).date(),
                    start_ns,
                    end_ns,
                    start_ns,
                    end_ns - 1,
                    open_ticks,
                    high,
                    low,
                    close,
                    10,
                    volume,
                    1,
                    buy_volume,
                    sell_volume,
                ),
                1,
            )
        )
        prior_close = close
    return tuple(output)


def _stage(hours: int = 120):
    bars = {
        300: _bars(300, hours * 12),
        1_800: _bars(1_800, hours * 2),
        3_600: _bars(3_600, hours),
    }
    dates = tuple(sorted({item.bar.source_date for values in bars.values() for item in values}))
    return build_symbolic_stage(bars, dates), bars, dates


def _synthetic_records(count: int = 60) -> tuple[AnchorRecord, ...]:
    start_date = date(2022, 1, 3)
    return tuple(
        AnchorRecord(
            start_date + timedelta(days=index),
            "6EH2",
            1,
            1,
            index * 86_400 * ONE_SECOND_NS,
            "LONG",
            index * 86_400 * ONE_SECOND_NS - 300 * ONE_SECOND_NS,
            index * 86_400 * ONE_SECOND_NS,
            1_000,
            1_004,
            996,
            1_002,
            80,
            20,
            (),
        )
        for index in range(count)
    )


PATH_START_NS = int(datetime(2022, 1, 3, 12, tzinfo=UTC).timestamp()) * ONE_SECOND_NS


def _wrapped_bar(
    timeframe: int,
    start_ns: int,
    open_ticks: int,
    high_ticks: int,
    low_ticks: int,
    close_ticks: int,
    *,
    contract: str = "6EH2",
    outcome_span_id: int = 1,
    segment_id: int = 1,
) -> BarWithOutcomeSpan:
    width = timeframe * ONE_SECOND_NS
    return BarWithOutcomeSpan(
        TradeBar(
            timeframe,
            segment_id,
            contract,
            datetime.fromtimestamp(start_ns // ONE_SECOND_NS, tz=UTC).date(),
            start_ns,
            start_ns + width,
            start_ns,
            start_ns + width - 1,
            open_ticks,
            high_ticks,
            low_ticks,
            close_ticks,
            1,
            1,
            1,
            1,
            0,
        ),
        outcome_span_id,
    )


def _outcome_path(
    rows: tuple[tuple[int, int, int, int, int], ...],
    *,
    duration_seconds: int = 25_200,
    contract: str = "6EH2",
    outcome_span_id: int = 1,
    segment_id: int = 1,
    structural_rows: tuple[BarWithOutcomeSpan, ...] | None = None,
    path_start_ns: int = PATH_START_NS,
) -> OneSecondPath:
    one_seconds = tuple(
        _wrapped_bar(
            1,
            path_start_ns + offset * ONE_SECOND_NS,
            open_ticks,
            high_ticks,
            low_ticks,
            close_ticks,
            contract=contract,
            outcome_span_id=outcome_span_id,
            segment_id=segment_id,
        )
        for offset, open_ticks, high_ticks, low_ticks, close_ticks in rows
    )
    structural = structural_rows or tuple(
        _wrapped_bar(
            300,
            path_start_ns + offset * 300 * ONE_SECOND_NS,
            100,
            101,
            99,
            100,
            contract=contract,
            outcome_span_id=outcome_span_id,
            segment_id=segment_id,
        )
        for offset in range(duration_seconds // 300)
    )
    return OneSecondPath.from_rows(
        one_seconds,
        coverage_start_ns=path_start_ns,
        coverage_end_ns=path_start_ns + duration_seconds * ONE_SECOND_NS,
        structural_five_minute_bars=structural,
    )


def _path_anchor(
    *,
    offset_seconds: int = 0,
    contract: str = "6EH2",
    outcome_span_id: int = 1,
    segment_id: int = 1,
    direction: str = "LONG",
    path_start_ns: int = PATH_START_NS,
) -> AnchorRecord:
    anchor_ns = path_start_ns + offset_seconds * ONE_SECOND_NS
    return AnchorRecord(
        datetime.fromtimestamp(anchor_ns // ONE_SECOND_NS, tz=UTC).date(),
        contract,
        outcome_span_id,
        segment_id,
        anchor_ns,
        direction,
        anchor_ns - 300 * ONE_SECOND_NS,
        anchor_ns,
        100,
        103,
        97,
        100,
        200,
        20,
        (),
    )


def _recipe(policy_id: str, *, entry_kind: str, exit_kind: str):
    entries = {item.entry_id: item for item in build_entry_catalog().candidates}
    exits = {item.exit_id: item for item in build_exit_catalog().candidates}
    for recipe in iter_complete_strategy_recipes((policy_id,)):
        if (
            entries[recipe.entry_policy_id].kind == entry_kind
            and exits[recipe.exit_policy_id].kind == exit_kind
        ):
            return recipe
    raise AssertionError("requested recipe kind is absent")


def _evaluate_one(
    path: OneSecondPath,
    *,
    entry_kind: str = "MARKET",
    exit_kind: str = "TERMINAL",
    anchor: AnchorRecord | None = None,
    rule_times: RuleExitTimes | None = None,
):
    policy = next(iter_anchor_policies())
    selected_anchor = anchor or _path_anchor()
    mask = PolicyMask.from_records(policy, "MOMENTUM_RETURN", "LONG", (selected_anchor,))
    recipe = _recipe(policy.policy_id, entry_kind=entry_kind, exit_kind=exit_kind)
    schedule = (
        None
        if exit_kind != "RULE"
        else RuleExitSchedule.from_rows(
            policy.policy_id,
            (
                rule_times
                or RuleExitTimes(
                    selected_anchor.outcome_key,
                    selected_anchor.anchor_ns + 10 * ONE_SECOND_NS,
                    None,
                ),
            ),
        )
    )
    day = selected_anchor.source_date
    evaluation = SharedPathEvaluator((path,)).evaluate(
        recipe,
        mask,
        rule_schedule=schedule,
        reporting_group_by_date={day: "R1"},
        outer_validation_by_date={
            day + timedelta(days=index): f"B{index + 3}" for index in range(6)
        },
    )
    return evaluation, mask, recipe


def test_exact_base_catalog_counts_order_and_identity() -> None:
    catalog = build_base_event_catalog()

    assert len(catalog.candidates) == 1_740
    assert catalog.catalog_sha256 == BASE_EVENT_CATALOG_SHA256
    assert BASE_EVENT_CATALOG_SHA256 == (
        "d9d09d524f9e2be2427a1243fbb391dcb6c74b3072942e0e4b555563734c1909"
    )
    assert [item.selection_rank for item in catalog.candidates] == list(range(1, 1_741))
    assert len(set(catalog.candidate_ids)) == 1_740
    assert Counter(item.family for item in catalog.candidates) == {
        "MOMENTUM_RETURN": 144,
        "DONCHIAN_BREAKOUT": 120,
        "EMA_TREND": 48,
        "MACD_STATE": 72,
        "RSI_STATE": 72,
        "STOCHASTIC_STATE": 72,
        "BOLLINGER_STATE": 108,
        "RANGE_REVERSION": 48,
        "COMPRESSION_BREAKOUT": 216,
        "PULLBACK_CONTINUATION": 144,
        "BODY_CONTINUATION": 54,
        "WICK_REJECTION": 54,
        "STRUCTURAL_PRICE_ACTION": 48,
        "NBAR_PRICE_ACTION": 54,
        "VOLATILITY_EXPANSION": 54,
        "EFFICIENCY_RATIO": 108,
        "VOLUME_FLOW": 108,
        "ROLLING_VWAP": 108,
        "SWING_FAILURE": 72,
        "GAP_EVENT": 36,
    }


def test_exact_common_axes_and_streamed_policy_recipe() -> None:
    contexts = build_context_catalog()
    times = build_time_filter_catalog()
    delays = build_delay_catalog()
    first_two = tuple(islice(iter_anchor_policies(), 2))

    assert len(contexts) == 13
    assert len(times) == 14
    assert len(delays) == 6
    assert len({item.context_id for item in contexts}) == 13
    assert len({item.time_filter_id for item in times}) == 14
    assert len({item.delay_id for item in delays}) == 6
    assert [item.policy_rank for item in first_two] == [1, 2]
    assert first_two[0].base_candidate_id == first_two[1].base_candidate_id
    assert first_two[0].delay_id != first_two[1].delay_id
    assert LOGICAL_ANCHOR_POLICY_COUNT == 1_900_080
    assert REFERENCE_SCORE_CELL_COUNT == 9_500_400
    assert all(
        len(value) == 64
        for value in (
            CONTEXT_CATALOG_SHA256,
            TIME_FILTER_CATALOG_SHA256,
            DELAY_CATALOG_SHA256,
            ANCHOR_POLICY_RECIPE_SHA256,
        )
    )


def test_every_formula_family_builds_causal_state_with_shared_cache() -> None:
    stage, _bars_by_timeframe, _dates = _stage()
    representatives = {}
    for candidate in build_base_event_catalog().candidates:
        representatives.setdefault(candidate.family, candidate)

    assert len(representatives) == 20
    for candidate in representatives.values():
        frames = stage.frames(candidate)
        assert len(frames) == len(
            stage.series_by_timeframe[candidate.trigger_timeframe_seconds].bars
        )
        assert all(
            isinstance(item.active, bool) and isinstance(item.event, bool) for item in frames
        )


def test_future_bars_do_not_change_an_earlier_event_mask() -> None:
    stage, bars, dates = _stage(hours=10)
    cutoff = bars[300][80].bar.end_ns
    prefix_bars = {
        timeframe: tuple(item for item in values if item.bar.end_ns <= cutoff)
        for timeframe, values in bars.items()
    }
    prefix_dates = tuple(
        sorted({item.bar.source_date for values in prefix_bars.values() for item in values})
    )
    prefix = build_symbolic_stage(prefix_bars, prefix_dates)
    first_policy = next(iter_anchor_policies())

    full_records = tuple(
        item for item in stage.policy_mask(first_policy).records if item.anchor_ns <= cutoff
    )
    assert full_records == prefix.policy_mask(first_policy).records
    assert dates[0] == prefix_dates[0]


def test_all_context_time_and_delay_axes_generate_feature_only_masks() -> None:
    stage, _bars_by_timeframe, _dates = _stage(hours=72)
    candidate = build_base_event_catalog().candidates[0]
    policies = (
        policy
        for policy in iter_anchor_policies()
        if policy.base_candidate_id == candidate.candidate_id
    )
    selected = tuple(islice(policies, 13 * 14 * 6))

    assert len(selected) == 1_092
    masks = tuple(stage.policy_mask(policy) for policy in selected)
    assert all(mask.policy.base_candidate_id == candidate.candidate_id for mask in masks)
    assert all(mask.mask_sha256 for mask in masks)


def test_feature_mask_dedup_keeps_earliest_semantic_rank() -> None:
    first, second = tuple(islice(iter_anchor_policies(), 2))
    records = _synthetic_records(2)
    first_mask = PolicyMask.from_records(first, "MOMENTUM_RETURN", "LONG", records)
    second_mask = PolicyMask.from_records(second, "MOMENTUM_RETURN", "LONG", records)

    result = deduplicate_feature_masks((second_mask, first_mask))

    assert result.representatives == (first_mask,)
    assert len(result.aliases) == 1
    assert result.aliases[0].alias_policy_id == second.policy_id
    assert result.aliases[0].representative_policy_id == first.policy_id


def test_sparse_batch_uses_one_shared_anchor_universe() -> None:
    stage, _bars_by_timeframe, _dates = _stage(hours=48)
    policies = tuple(islice(iter_anchor_policies(), 2))

    batches = tuple(iter_anchor_mask_batches(stage, batch_size=2, policies=policies))

    assert len(batches) == 1
    assert len(batches[0].masks) == 2
    assert batches[0].first_policy_rank == 1
    assert batches[0].last_policy_rank == 2
    assert all(item.bits_hex for item in batches[0].masks)


def test_stage_a_scores_all_horizons_and_selects_deterministically() -> None:
    policy = next(iter_anchor_policies())
    records = _synthetic_records()
    mask = PolicyMask.from_records(policy, "MOMENTUM_RETURN", "LONG", records)
    group_by_date = {
        record.source_date: f"fold_{index // 20 + 1}" for index, record in enumerate(records)
    }
    surfaces = tuple(
        ReferenceOutcomeSurface(
            horizon,
            {record.outcome_key: 30 for record in records},
        )
        for horizon in REFERENCE_HORIZONS_SECONDS
    )

    scores = score_stage_a_reference_horizons((mask,), surfaces, group_by_date)
    selection = select_stage_a_top256(scores)

    assert len(scores) == 1
    assert scores[0].eligible is True
    assert scores[0].robust_horizon_count == 5
    assert all(item.net_ticks == 60 * 16 for item in scores[0].horizons)
    assert selection.classification == "STAGE_A_ANCHORS_SELECTED"
    assert selection.selected_policy_ids == (policy.policy_id,)
    assert stage_a_selection_from_dict(selection.as_dict()) == selection
    assert policy_mask_from_dict(mask.as_dict()) == mask


def test_stage_a_rejects_an_incomplete_horizon_surface_family() -> None:
    policy = next(iter_anchor_policies())
    records = _synthetic_records()
    mask = PolicyMask.from_records(policy, "MOMENTUM_RETURN", "LONG", records)
    groups = {record.source_date: f"fold_{index % 3}" for index, record in enumerate(records)}
    surfaces = (
        ReferenceOutcomeSurface(
            REFERENCE_HORIZONS_SECONDS[0],
            {record.outcome_key: 30 for record in records},
        ),
    )

    with pytest.raises(ValueError, match="all five exact horizons"):
        score_stage_a_reference_horizons((mask,), surfaces, groups)


def test_public_contract_binds_counts_hashes_and_outcome_boundary() -> None:
    contract = symbolic_engine_contract()

    assert contract["schema"] == "systematic_fx.ai_all_cases_symbolic_engine.v1"
    assert contract["axes"]["base_event_count"] == 1_740
    assert contract["axes"]["logical_anchor_policy_count"] == 1_900_080
    assert contract["axes"]["reference_score_cell_count"] == 9_500_400
    assert contract["execution_boundary"]["outcomes_in_signal_or_mask_api"] is False
    assert contract["execution_boundary"]["direct_missing_next_5m"] == (
        "EXCLUDED_AND_COMMITTED_BEFORE_OUTCOMES"
    )
    assert contract["deduplication"]["outcome_access"] is False
    assert contract["stage_b_execution"]["maximum_concurrent_positions_per_strategy"] == 1
    assert "global_maximum_concurrent_positions" not in contract["stage_b_execution"]
    assert contract["feature_only_controls"]["native_trigger_geometry"] == (
        "EXACT_CANDIDATE_TF_OHLC_AND_ATR20"
    )
    assert contract["stage_b_vector_kernel"]["public_batch_api"] == (
        "SharedPathEvaluator.fixed_exit_outcome_batch"
    )
    assert "NONOVERLAPPING_PATH_INTERVALS" in contract["stage_b_chunking"]["streaming_reset_safety"]


def test_entry_exit_and_complete_recipe_catalogs_are_exact() -> None:
    entries = build_entry_catalog()
    exits = build_exit_catalog()
    policy_id = next(iter_anchor_policies()).policy_id
    recipes = tuple(iter_complete_strategy_recipes((policy_id,)))
    stage_b_plan = build_stage_b_chunk_plan(256)

    assert len(entries.candidates) == ENTRY_POLICY_COUNT == 9
    assert len(exits.candidates) == EXIT_POLICY_COUNT == 85
    assert Counter(item.kind for item in entries.candidates) == {
        "MARKET": 1,
        "STOP_SIGNAL_EXTREME": 4,
        "LIMIT_ATR_RETRACE": 4,
    }
    assert Counter(item.kind for item in exits.candidates) == {
        "TERMINAL": 5,
        "BRACKET": 60,
        "TRAILING": 8,
        "BREAK_EVEN": 8,
        "RULE": 4,
    }
    assert len(recipes) == 765
    assert recipes[0].strategy_rank == 1
    assert recipes[-1].strategy_rank == 765
    assert len(stage_b_plan) == 64
    assert {item.strategy_count for item in stage_b_plan} == {3_060}
    empty_plan = build_stage_b_chunk_plan(0)
    assert len(empty_plan) == 64
    assert all(
        (item.first_strategy_rank, item.last_strategy_rank, item.strategy_count) == (1, 0, 0)
        for item in empty_plan
    )
    assert tuple(iter_complete_strategy_recipes(())) == ()
    assert COMPLETE_STRATEGY_MAXIMUM == 195_840
    assert all(
        len(value) == 64
        for value in (
            ENTRY_CATALOG_SHA256,
            EXIT_CATALOG_SHA256,
            COMPLETE_STRATEGY_RECIPE_SHA256,
        )
    )


def test_candidate_cube_matches_naive_masks_and_fixed_64_way_plan() -> None:
    stage, _bars_by_timeframe, _dates = _stage(hours=48)
    candidate = build_base_event_catalog().candidates[0]
    cube = build_candidate_policy_cube(stage, candidate)
    encoded = tuple(islice(cube.iter_masks(), 12))
    plan = build_stage_a_chunk_plan()

    for item in encoded:
        naive = stage.policy_mask(item.policy)
        assert item.mask_sha256 == naive.mask_sha256
        assert item.support_count == naive.support_count
        assert cube.records_for_bits(item.bits) == naive.records
    assert len(plan) == STAGE_A_OUTER_CHUNK_COUNT == 64
    assert plan[0].first_policy_rank == 1
    assert plan[-1].last_policy_rank == LOGICAL_ANCHOR_POLICY_COUNT
    assert plan[0].policy_count == STAGE_A_POLICY_ROWS_PER_CHUNK_MAXIMUM == 29_689
    assert plan[-1].policy_count == 29_673


def test_stage_a_global_dedup_keeps_earliest_cross_candidate_policy() -> None:
    first = next(iter_anchor_policies())
    second = next(islice(iter_anchor_policies(), 1_092, None))
    records = _synthetic_records()
    masks = (
        PolicyMask.from_records(first, "MOMENTUM_RETURN", "LONG", records),
        PolicyMask.from_records(second, "MOMENTUM_RETURN", "LONG", records),
    )
    groups = {record.source_date: f"G{index // 20 + 1}" for index, record in enumerate(records)}
    surfaces = tuple(
        ReferenceOutcomeSurface(horizon, {record.outcome_key: 30 for record in records})
        for horizon in REFERENCE_HORIZONS_SECONDS
    )

    selection = select_stage_a_top256(score_stage_a_reference_horizons(masks, surfaces, groups))

    assert selection.selected_policy_ids == (first.policy_id,)
    assert selection.deduplicated_policy_count == 1
    assert selection.alias_count == 1
    assert selection.alias_chain_sha256 != "0" * 64


def test_stage_a_dedup_prefers_raw_gate_eligible_alias_and_binds_raw_hash() -> None:
    policies = tuple(islice(iter_anchor_policies(), 2))
    records = _synthetic_records()
    groups = {record.source_date: f"G{index // 20 + 1}" for index, record in enumerate(records)}
    surfaces = tuple(
        ReferenceOutcomeSurface(horizon, {record.outcome_key: 30 for record in records})
        for horizon in REFERENCE_HORIZONS_SECONDS
    )
    template = score_stage_a_reference_horizons(
        (PolicyMask.from_records(policies[0], "MOMENTUM_RETURN", "LONG", records),),
        surfaces,
        groups,
    )[0]
    raw_ineligible = replace(
        template,
        policy_id=policies[0].policy_id,
        policy_rank=policies[0].policy_rank,
        raw_mask_sha256=canonical_sha256("raw-ineligible"),
        support_count=10,
        support_day_count=10,
        eligible=False,
        rejection_reasons=("RAW_SUPPORT_LT_60", "SUPPORT_DAYS_LT_40"),
    )
    raw_eligible = replace(
        template,
        policy_id=policies[1].policy_id,
        policy_rank=policies[1].policy_rank,
        raw_mask_sha256=canonical_sha256("raw-eligible"),
    )

    selection = select_stage_a_top256((raw_ineligible, raw_eligible))

    assert raw_ineligible.raw_gate_eligible is False
    assert raw_eligible.raw_gate_eligible is True
    assert selection.selected_policy_ids == (raw_eligible.policy_id,)
    assert selection.alias_count == 1
    assert selection.alias_chain_sha256 != "0" * 64
    assert raw_eligible.as_dict()["raw_gate_eligible"] is True


def test_stage_a_cumulative_three_world_pair_budget_skips_overflow_deterministically() -> None:
    policies = tuple(islice(iter_anchor_policies(), 3))
    records = _synthetic_records()
    groups = {record.source_date: f"G{index // 20 + 1}" for index, record in enumerate(records)}
    surfaces = tuple(
        ReferenceOutcomeSurface(horizon, {record.outcome_key: 30 for record in records})
        for horizon in REFERENCE_HORIZONS_SECONDS
    )
    template = score_stage_a_reference_horizons(
        (PolicyMask.from_records(policies[0], "MOMENTUM_RETURN", "LONG", records),),
        surfaces,
        groups,
    )[0]
    support_values = (2_000, 2_000, 1_000)
    scores = tuple(
        replace(
            template,
            policy_id=policy.policy_id,
            policy_rank=policy.policy_rank,
            mask_sha256=canonical_sha256(f"mask-{index}"),
            raw_mask_sha256=canonical_sha256(f"raw-{index}"),
            support_count=support,
            evaluable_support_count=support,
        )
        for index, (policy, support) in enumerate(
            zip(policies, support_values, strict=True), start=1
        )
    )

    selection = select_stage_a_top256(scores)

    assert selection.selected_policy_ids == (policies[0].policy_id, policies[2].policy_id)
    assert selection.stage_b_pair_budget_maximum == 100_000
    assert selection.stage_b_pair_budget_used == (2_000 + 1_000) * 9 * 3 == 81_000
    assert selection.budget_rejected_policy_count == 1
    assert len(selection.budget_decision_sha256) == 64
    assert stage_a_selection_from_dict(selection.as_dict()) == selection


def test_efficiency_fade_requires_current_to_remain_on_fade_side() -> None:
    overrides = {index: 100 for index in range(11)}
    overrides[9] = 90
    overrides[10] = 101
    bars = {
        300: _bars(300, 30, close_override=overrides),
        1_800: _bars(1_800, 30),
        3_600: _bars(3_600, 30),
    }
    dates = tuple(sorted({item.bar.source_date for values in bars.values() for item in values}))
    stage = build_symbolic_stage(bars, dates)
    candidate = next(
        item
        for item in build_base_event_catalog().candidates
        if item.family == "EFFICIENCY_RATIO"
        and item.direction == "LONG"
        and item.trigger_timeframe_seconds == 300
        and item.parameter("lookback_bars") == 10
        and item.parameter("mode") == 2
    )

    assert stage.frames(candidate)[10].active is False


def test_feature_only_entry_orders_freeze_exact_market_stop_and_limit_prices() -> None:
    policy = next(iter_anchor_policies())
    anchor = _path_anchor()
    mask = PolicyMask.from_records(policy, "MOMENTUM_RETURN", "LONG", (anchor,))
    batch = freeze_entry_orders(mask)
    by_kind: dict[str, list] = {}
    entries = {item.entry_id: item for item in build_entry_catalog().candidates}
    for order in batch.orders:
        by_kind.setdefault(entries[order.entry_policy_id].kind, []).append(order)

    assert by_kind["MARKET"][0].order_ticks is None
    assert {item.order_ticks for item in by_kind["STOP_SIGNAL_EXTREME"]} == {104, 107}
    assert {item.order_ticks for item in by_kind["LIMIT_ATR_RETRACE"]} == {95, 98}
    assert canonical_sha256(batch.definition_dict()) == batch.artifact_sha256


def test_causal_expert_eight_are_exact_recipe_bound_and_outcome_blind() -> None:
    policy = next(iter_anchor_policies())
    candidate = next(
        item
        for item in build_base_event_catalog().candidates
        if item.candidate_id == policy.base_candidate_id
    )
    context = next(item for item in build_context_catalog() if item.context_id == policy.context_id)
    anchor = _path_anchor()
    mask = PolicyMask.from_records(policy, candidate.family, candidate.direction, (anchor,))
    stop_entry = next(
        item
        for item in build_entry_catalog().candidates
        if item.kind == "STOP_SIGNAL_EXTREME"
        and item.parameter("buffer_ticks") == 1
        and item.parameter("time_in_force_seconds") == 1_800
    )
    order = freeze_entry_orders(mask, (stop_entry,)).orders[0]
    bracket = next(
        item
        for item in build_exit_catalog().candidates
        if item.kind == "BRACKET"
        and item.fraction_parameter("take_profit_atr") == Fraction(1, 2)
        and item.fraction_parameter("stop_loss_atr") == Fraction(1, 2)
    )

    artifact = build_causal_expert_feature_artifact(
        candidate,
        context,
        policy,
        anchor,
        order,
        bracket,
    )
    values = {item.feature_name: item.fraction for item in artifact.values}

    assert tuple(values) == EXPERT_FEATURE_NAMES
    assert artifact.formula_sha256 == EXPERT_FEATURE_FORMULA_SHA256
    assert artifact.anchor_policy_id == policy.policy_id
    assert values == {
        "expert_signal_strength": Fraction(0),
        "expert_event_age_native_bars": Fraction(0),
        "expert_context_relation": Fraction(0),
        "expert_atr_ticks": Fraction(10),
        "expert_signal_range_atr": Fraction(3, 5),
        "expert_time_to_entry_seconds": Fraction(1_800),
        "expert_planned_entry_distance_atr": Fraction(2, 5),
        "expert_reward_risk_ratio": Fraction(1),
    }
    assert causal_expert_feature_artifact_from_dict(artifact.as_dict()) == artifact

    delayed_anchor = replace(anchor, anchor_ns=anchor.anchor_ns + 600 * ONE_SECOND_NS)
    delayed_mask = PolicyMask.from_records(
        policy,
        candidate.family,
        candidate.direction,
        (delayed_anchor,),
    )
    delayed_order = freeze_entry_orders(delayed_mask, (stop_entry,)).orders[0]
    delayed = build_causal_expert_feature_artifact(
        candidate,
        context,
        policy,
        delayed_anchor,
        delayed_order,
        bracket,
    )
    assert delayed.values[1].fraction == 2

    wrong_context = build_context_catalog()[1]
    with pytest.raises(ValueError, match="decision cutoff"):
        build_causal_expert_feature_artifact(
            candidate,
            wrong_context,
            policy,
            anchor,
            order,
            bracket,
        )

    module = __import__("campaigns.ai_all_cases_v1.symbolic", fromlist=["_resolve_exit"])
    path = _outcome_path(((5, 100, 110, 100, 105), (3_604, 105, 105, 105, 105)))
    entry_attempt = module._resolve_entry(order, path)
    realized_exit = module._resolve_exit(order, entry_attempt, bracket, path, None)
    with pytest.raises(ValueError, match="non-causal input type"):
        build_causal_expert_feature_artifact(
            candidate,
            context,
            policy,
            anchor,
            order,
            realized_exit,
        )


def test_market_first_observed_trade_and_all_exit_policy_families() -> None:
    terminal_path = _outcome_path(((5, 100, 100, 100, 100), (1_804, 108, 108, 108, 108)))
    terminal, _mask, _recipe_value = _evaluate_one(terminal_path)

    bracket_path = _outcome_path(((5, 100, 110, 95, 100), (3_604, 100, 100, 100, 100)))
    bracket, _mask, _recipe_value = _evaluate_one(bracket_path, exit_kind="BRACKET")

    trailing_path = _outcome_path(
        ((5, 100, 100, 100, 100), (10, 106, 112, 106, 108), (10_804, 108, 108, 108, 108))
    )
    trailing, _mask, _recipe_value = _evaluate_one(trailing_path, exit_kind="TRAILING")

    break_even_path = _outcome_path(
        ((5, 100, 100, 100, 100), (10, 104, 110, 100, 104), (10_804, 104, 104, 104, 104))
    )
    break_even, _mask, _recipe_value = _evaluate_one(break_even_path, exit_kind="BREAK_EVEN")

    rule_path = _outcome_path(
        ((5, 100, 100, 100, 100), (10, 105, 105, 105, 105), (10_804, 105, 105, 105, 105))
    )
    rule, _mask, _recipe_value = _evaluate_one(rule_path, exit_kind="RULE")

    assert terminal.fill_count == 1
    assert terminal.total_reference_pnl_ticks == 8
    assert bracket.fill_count == 1
    assert bracket.total_net_ticks < 0  # same-second TP+SL resolves stop first
    assert trailing.fill_count == 1
    assert break_even.fill_count == 1
    assert rule.fill_count == 1
    assert all(
        canonical_sha256(item.definition_dict()) == item.artifact_sha256
        for item in (terminal, bracket, trailing, break_even, rule)
    )


def test_reference_surface_fail_closes_missing_entry_and_stale_terminal() -> None:
    anchor = _path_anchor()
    missing_entry = _outcome_path(((301, 100, 100, 100, 100),))
    missing_surfaces = build_reference_outcome_surfaces((anchor,), (missing_entry,))

    assert all(anchor.outcome_key in item.censored_anchor_keys for item in missing_surfaces)
    assert all(anchor.outcome_key not in item.gross_ticks_by_anchor for item in missing_surfaces)

    stale = _outcome_path(((5, 100, 100, 100, 100), (3_299, 108, 108, 108, 108)))
    fresh = _outcome_path(((5, 100, 100, 100, 100), (3_300, 108, 108, 108, 108)))
    stale_hour = next(
        item
        for item in build_reference_outcome_surfaces((anchor,), (stale,))
        if item.horizon_seconds == 3_600
    )
    fresh_hour = next(
        item
        for item in build_reference_outcome_surfaces((anchor,), (fresh,))
        if item.horizon_seconds == 3_600
    )

    assert anchor.outcome_key in stale_hour.censored_anchor_keys
    assert fresh_hour.gross_ticks_by_anchor[anchor.outcome_key] == 8


def test_vectorized_fixed_exit_cache_matches_scalar_all_81_policies() -> None:
    module = __import__("campaigns.ai_all_cases_v1.symbolic", fromlist=["_resolve_exit"])
    path = _outcome_path(
        (
            (5, 100, 103, 96, 101),
            (20, 101, 108, 99, 106),
            (100, 106, 112, 104, 110),
            (1_804, 110, 114, 105, 108),
            (3_604, 108, 116, 102, 104),
            (10_804, 104, 120, 95, 118),
            (21_604, 118, 121, 110, 115),
        )
    )
    policy = next(iter_anchor_policies())
    mask = PolicyMask.from_records(
        policy,
        "MOMENTUM_RETURN",
        "LONG",
        (_path_anchor(),),
    )
    market = build_entry_catalog().candidates[0]
    order = freeze_entry_orders(mask, (market,)).orders[0]
    entry = module._resolve_entry(order, path)
    evaluator = SharedPathEvaluator((path,))
    fixed_batch = evaluator.fixed_exit_outcome_batch(order, entry, path)

    for exit_policy in build_exit_catalog().candidates:
        if exit_policy.kind == "RULE":
            continue
        vectorized = evaluator._exit(order, entry, exit_policy, path, None)
        scalar = module._resolve_exit(order, entry, exit_policy, path, None)
        assert vectorized.as_dict() == scalar.as_dict(), exit_policy.as_dict()

    assert len(fixed_batch) == EXIT_POLICY_COUNT - 4 == 81
    assert len({exit_id for exit_id, _outcome in fixed_batch}) == 81
    budget = stage_b_kernel_budget_projection()
    assert budget["filled_anchor_entry_pairs"] == 100_000
    assert budget["maximum_support_per_mask"] == 14_842
    assert budget["python_full_path_iterations"] == 0
    assert budget["projected_seconds"] < 86_400
    assert budget["peak_shared_kernel_cache_bytes_upper_bound"] < 2 * 1_024**3
    assert budget["within_24h"] is True


@pytest.mark.parametrize(
    ("direction", "rows", "expected_reference"),
    (
        (
            "LONG",
            (
                (5, 100, 102, 100, 101),
                (10, 104, 108, 103, 106),
                (20, 95, 100, 94, 96),
                (10_804, 96, 96, 96, 96),
            ),
            95,
        ),
        (
            "SHORT",
            (
                (5, 100, 100, 98, 99),
                (10, 95, 97, 92, 94),
                (20, 110, 112, 108, 111),
                (10_804, 111, 111, 111, 111),
            ),
            110,
        ),
    ),
)
def test_vector_break_even_uses_exact_activation_row_then_gap_aware_later_stop(
    direction: str,
    rows: tuple[tuple[int, int, int, int, int], ...],
    expected_reference: int,
) -> None:
    module = __import__("campaigns.ai_all_cases_v1.symbolic", fromlist=["_resolve_exit"])
    path = _outcome_path(rows)
    policy = next(iter_anchor_policies())
    anchor = _path_anchor(direction=direction)
    mask = PolicyMask.from_records(policy, "MOMENTUM_RETURN", direction, (anchor,))
    market = build_entry_catalog().candidates[0]
    order = freeze_entry_orders(mask, (market,)).orders[0]
    entry = module._resolve_entry(order, path)
    break_even = next(
        item
        for item in build_exit_catalog().candidates
        if item.kind == "BREAK_EVEN"
        and item.fraction_parameter("activation_atr") == Fraction(1, 2)
        and item.fraction_parameter("initial_stop_atr") == Fraction(1, 2)
        and item.cap_seconds == 10_800
    )
    scalar = module._resolve_exit(order, entry, break_even, path, None)
    vector = SharedPathEvaluator((path,))._exit(order, entry, break_even, path, None)

    assert scalar.reason == vector.reason == "BREAK_EVEN_STOP"
    assert scalar.exit_reference_ticks == vector.exit_reference_ticks == expected_reference
    assert scalar.as_dict() == vector.as_dict()


def test_randomized_long_short_all_entries_all_fixed_exits_match_scalar() -> None:
    module = __import__("campaigns.ai_all_cases_v1.symbolic", fromlist=["_resolve_exit"])
    fixed_exits = tuple(item for item in build_exit_catalog().candidates if item.kind != "RULE")
    policy = next(iter_anchor_policies())

    for direction_index, direction in enumerate(("LONG", "SHORT")):
        anchor = _path_anchor(direction=direction)
        mask = PolicyMask.from_records(policy, "MOMENTUM_RETURN", direction, (anchor,))
        for entry_policy in build_entry_catalog().candidates:
            order = freeze_entry_orders(mask, (entry_policy,)).orders[0]
            center = 100 if order.order_ticks is None else order.order_ticks
            if direction == "LONG":
                first = (5, center, center + 2, center, center + 1)
            else:
                first = (5, center, center, center - 2, center - 1)
            rng = Random(10_000 * direction_index + entry_policy.selection_rank)
            offsets = set(rng.sample(range(10, 21_500), 128))
            offsets.update(5 + horizon - 10 for horizon in REFERENCE_HORIZONS_SECONDS)
            random_rows = []
            prior_close = first[4]
            for offset in sorted(offsets):
                open_ticks = max(20, prior_close + rng.randint(-12, 12))
                close_ticks = max(20, open_ticks + rng.randint(-8, 8))
                high_ticks = max(open_ticks, close_ticks) + rng.randint(0, 8)
                low_ticks = min(open_ticks, close_ticks) - rng.randint(0, 8)
                random_rows.append((offset, open_ticks, high_ticks, max(1, low_ticks), close_ticks))
                prior_close = close_ticks
            path = _outcome_path((first, *random_rows))
            entry = module._resolve_entry(order, path)
            assert entry.filled, (direction, entry_policy.as_dict(), entry.as_dict())
            evaluator = SharedPathEvaluator((path,))
            vector = dict(evaluator.fixed_exit_outcome_batch(order, entry, path))
            assert len(vector) == len(fixed_exits) == 81
            for exit_policy in fixed_exits:
                scalar = module._resolve_exit(order, entry, exit_policy, path, None)
                assert vector[exit_policy.exit_id].as_dict() == scalar.as_dict(), (
                    direction,
                    entry_policy.as_dict(),
                    exit_policy.as_dict(),
                )


def test_stop_and_limit_entries_touch_with_frozen_tif() -> None:
    stop_path = _outcome_path(((10, 100, 104, 100, 104), (1_809, 110, 110, 110, 110)))
    stop, _mask, _recipe_value = _evaluate_one(stop_path, entry_kind="STOP_SIGNAL_EXTREME")
    limit_path = _outcome_path(((10, 100, 100, 98, 99), (1_809, 105, 105, 105, 105)))
    limit, _mask, _recipe_value = _evaluate_one(limit_path, entry_kind="LIMIT_ATR_RETRACE")

    assert stop.fill_count == 1
    assert limit.fill_count == 1
    assert stop.unfilled_entry_count == limit.unfilled_entry_count == 0


def test_compact_stage_b_chunk_factors_shared_coverage_and_strictly_replays() -> None:
    path = _outcome_path(
        ((5, 100, 100, 100, 100), (1_804, 108, 108, 108, 108), (3_604, 110, 110, 110, 110))
    )
    terminal, _mask, _recipe_value = _evaluate_one(path, exit_kind="TERMINAL")
    bracket, _mask, _recipe_value = _evaluate_one(path, exit_kind="BRACKET")
    chunk = CompleteEvaluationChunk.from_evaluations((terminal, bracket))
    payload = chunk.as_dict()

    assert len(payload["evaluations"]) == 2
    assert len(payload["coverage_shapes"]) == 1
    assert len(payload["behavior_leaf_rows"]) == 2
    assert all("evaluated_lineages" not in item for item in payload["evaluations"])
    assert complete_evaluation_chunk_from_dict(payload) == chunk

    tampered = {
        **payload,
        "coverage_shapes": [{**payload["coverage_shapes"][0], "evaluated_anchor_counts": [999]}],
    }
    with pytest.raises(ValueError, match="complete evaluation chunk payload is invalid"):
        complete_evaluation_chunk_from_dict(tampered)


def test_bounded_detailed_rows_exactly_reproduce_compact_evaluation() -> None:
    path = _outcome_path(((5, 100, 100, 100, 100), (1_804, 108, 108, 108, 108)))
    policy = next(iter_anchor_policies())
    anchor = _path_anchor()
    mask = PolicyMask.from_records(policy, "MOMENTUM_RETURN", "LONG", (anchor,))
    recipe = _recipe(policy.policy_id, entry_kind="MARKET", exit_kind="TERMINAL")
    evaluator = SharedPathEvaluator((path,))
    day = anchor.source_date
    details = evaluate_selected_strategy_details(
        evaluator,
        (SelectedStrategyDetailRequest(1, "B3", "REAL", recipe, mask, None),),
        reporting_group_by_date={day: "R1"},
        outer_validation_by_date={day: "B3"},
    )

    assert len(details) == 1 and len(details[0].rows) == 1
    row = details[0].rows[0]
    assert row.status == "FILLED"
    assert row.entry_ns is not None and row.exit_ns is not None
    assert row.net_pnl_ticks == 8 - 14
    assert canonical_sha256(details[0].definition_dict()) == details[0].artifact_sha256


def test_structural_5m_proof_localizes_gap_and_rejects_lineage_cross() -> None:
    full_structural = tuple(
        _wrapped_bar(
            300,
            PATH_START_NS + offset * 300 * ONE_SECOND_NS,
            100,
            101,
            99,
            100,
        )
        for offset in range(84)
    )
    gapped_path = _outcome_path(
        (
            (5, 100, 100, 100, 100),
            (1_805, 100, 100, 100, 100),
            (3_604, 105, 105, 105, 105),
        ),
        structural_rows=full_structural[:4] + full_structural[5:],
    )
    crossing, _mask, _recipe_value = _evaluate_one(gapped_path)
    unaffected, _mask, _recipe_value = _evaluate_one(
        gapped_path,
        anchor=_path_anchor(offset_seconds=1_800),
    )

    assert gapped_path.coverage_intervals == (
        (PATH_START_NS, PATH_START_NS + 1_200 * ONE_SECOND_NS),
        (PATH_START_NS + 1_500 * ONE_SECOND_NS, PATH_START_NS + 25_200 * ONE_SECOND_NS),
    )
    assert crossing.censored_count == 1 and crossing.fill_count == 0
    assert unaffected.censored_count == 0 and unaffected.fill_count == 1
    crossed = list(full_structural)
    crossed[10] = _wrapped_bar(
        300,
        crossed[10].bar.start_ns,
        100,
        101,
        99,
        100,
        contract="6EM2",
    )
    with pytest.raises(ValueError, match="crosses lineage"):
        _outcome_path(
            ((5, 100, 100, 100, 100),),
            structural_rows=tuple(crossed),
        )

    short_path = _outcome_path(
        ((5, 100, 100, 100, 100), (1_799, 105, 105, 105, 105)),
        duration_seconds=1_800,
    )
    censored, mask, _recipe_value = _evaluate_one(short_path)
    coverage = verify_complete_evaluation_coverage(censored, mask)
    gate = __import__(
        "campaigns.ai_all_cases_v1.symbolic", fromlist=["apply_complete_search_gates"]
    ).apply_complete_search_gates(
        censored,
        censored,
        censored,
        coverage_commitments=(coverage, coverage, coverage),
    )

    assert censored.censored_count == 1
    assert censored.fill_count == 0
    assert "CENSORED_COUNT_NONZERO" in gate.rejection_reasons
    assert complete_strategy_evaluation_from_dict(censored.as_dict()) == censored
    chunk = CompleteEvaluationChunk.from_evaluations((censored,))
    compact = chunk.as_dict()
    assert compact["serialization"] == "FACTORED_COVERAGE_AND_BEHAVIOR_LEAVES"
    assert len(compact["coverage_shapes"]) == 1
    assert "evaluated_lineages" not in compact["evaluations"][0]
    assert "behavior_leaf_sha256s" not in compact["evaluations"][0]
    assert complete_evaluation_chunk_from_dict(chunk.as_dict()) == chunk
    assert complete_evaluation_coverage_from_dict(coverage.as_dict()) == coverage
    tampered = {**coverage.as_dict(), "unexpected": 1}
    with pytest.raises(ValueError, match="canonical payload differs"):
        complete_evaluation_coverage_from_dict(tampered)


def test_preoutcome_structural_lattice_keeps_raw_support_and_drops_only_crossing_anchor() -> None:
    full = _bars(300, 100)
    gapped = full[:5] + full[6:]
    dates = tuple(sorted({item.bar.source_date for item in gapped}))
    lattice = build_structural_eligibility_lattice(
        gapped,
        decision_dates=dates,
        allowed_tail_end_ns=gapped[-1].bar.end_ns,
    )
    policy = next(iter_anchor_policies())

    def anchor(wrapped: BarWithOutcomeSpan) -> AnchorRecord:
        bar = wrapped.bar
        return AnchorRecord(
            bar.source_date,
            bar.contract,
            wrapped.outcome_span_id,
            bar.segment_id,
            bar.end_ns,
            "LONG",
            bar.start_ns,
            bar.end_ns,
            bar.open_ticks,
            bar.high_ticks,
            bar.low_ticks,
            bar.close_ticks,
            100,
            20,
            (),
        )

    crossing = anchor(full[0])
    unaffected = anchor(full[10])
    raw = PolicyMask.from_records(
        policy,
        "MOMENTUM_RETURN",
        "LONG",
        (crossing, unaffected),
    )
    frozen = freeze_structurally_eligible_policy_mask(raw, lattice)

    assert frozen.raw_support_count == 2
    assert frozen.evaluable_support_count == 1
    assert frozen.evaluable_mask.records == (unaffected,)
    assert frozen.excluded_anchor_keys == (crossing.outcome_key,)
    assert canonical_sha256(frozen.definition_dict()) == frozen.commitment_sha256
    assert structural_eligibility_lattice_from_dict(lattice.as_dict()) == lattice
    assert structurally_eligible_policy_mask_from_dict(frozen.as_dict()) == frozen


def test_direct_opportunity_lattice_freezes_next_5m_first_trade_and_open() -> None:
    bars = list(_bars(300, 100))
    next_bar = bars[1]
    bars[1] = BarWithOutcomeSpan(
        replace(
            next_bar.bar,
            first_trade_ns=next_bar.bar.start_ns + 7 * ONE_SECOND_NS + 123,
            open_ticks=1_111,
            high_ticks=max(next_bar.bar.high_ticks, 1_111),
            low_ticks=min(next_bar.bar.low_ticks, 1_111),
        ),
        next_bar.outcome_span_id,
    )
    dates = tuple(sorted({item.bar.source_date for item in bars}))
    structural = build_structural_eligibility_lattice(
        bars,
        decision_dates=dates,
        allowed_tail_end_ns=bars[-1].bar.end_ns,
    )
    direct = build_direct_opportunity_lattice(bars, structural)

    first = direct.opportunities[0]
    assert first.decision_ns == bars[0].bar.end_ns
    assert first.scheduled_entry_ns == bars[1].bar.start_ns + 7 * ONE_SECOND_NS
    assert first.scheduled_entry_ticks == bars[1].bar.open_ticks == 1_111
    assert direct.opportunity_count == direct.structural_anchor_count
    assert direct.excluded_anchor_keys == ()
    assert direct_opportunity_lattice_from_dict(direct.as_dict()) == direct

    missing_next = build_direct_opportunity_lattice(
        tuple(bars[:1] + bars[2:]),
        structural,
    )
    assert structural.eligible_anchor_keys[0] in missing_next.excluded_anchor_keys
    assert (
        missing_next.opportunity_count + len(missing_next.excluded_anchor_keys)
        == missing_next.structural_anchor_count
    )


def _direct_terminal_liveness_case(
    horizon_seconds: int,
    *,
    terminal_age_seconds: int,
    containing_first_before_exit: bool = False,
) -> tuple[
    list[BarWithOutcomeSpan],
    StructuralEligibilityLattice,
    tuple[str, int, int, int],
]:
    bars = list(_bars(300, 100))
    entry_offset_ns = 18 * ONE_SECOND_NS
    following = bars[1]
    bars[1] = BarWithOutcomeSpan(
        replace(
            following.bar,
            first_trade_ns=following.bar.start_ns + entry_offset_ns,
        ),
        following.outcome_span_id,
    )
    exit_ns = following.bar.start_ns + entry_offset_ns + horizon_seconds * ONE_SECOND_NS
    containing_index = 1 + horizon_seconds // 300
    containing = bars[containing_index]
    containing_first_ns = exit_ns - 1 if containing_first_before_exit else exit_ns
    bars[containing_index] = BarWithOutcomeSpan(
        replace(containing.bar, first_trade_ns=containing_first_ns),
        containing.outcome_span_id,
    )
    previous = bars[containing_index - 1]
    bars[containing_index - 1] = BarWithOutcomeSpan(
        replace(
            previous.bar,
            last_trade_ns=exit_ns - (terminal_age_seconds + 1) * ONE_SECOND_NS,
        ),
        previous.outcome_span_id,
    )
    dates = tuple(sorted({item.bar.source_date for item in bars}))
    structural = build_structural_eligibility_lattice(
        bars,
        decision_dates=dates,
        allowed_tail_end_ns=bars[-1].bar.end_ns,
    )
    return bars, structural, structural.eligible_anchor_keys[0]


@pytest.mark.parametrize(
    ("terminal_age_seconds", "expected_eligible"),
    ((299, True), (300, False), (317, False)),
)
def test_direct_terminal_liveness_uses_strict_one_second_bar_end_threshold(
    terminal_age_seconds: int,
    expected_eligible: bool,
) -> None:
    bars, structural, target = _direct_terminal_liveness_case(
        3_600,
        terminal_age_seconds=terminal_age_seconds,
    )
    direct = build_direct_opportunity_lattice(bars, structural)
    opportunity_keys = tuple(item.structural_anchor_key for item in direct.opportunities)

    assert (target in opportunity_keys) is expected_eligible
    assert (target in direct.excluded_anchor_keys) is not expected_eligible
    assert direct.opportunity_count + len(direct.excluded_anchor_keys) == (
        direct.structural_anchor_count
    )
    assert canonical_sha256(direct.definition_dict()) == direct.artifact_sha256
    assert direct_opportunity_lattice_from_dict(direct.as_dict()) == direct


def test_direct_terminal_liveness_prefers_trade_in_containing_bar() -> None:
    bars, structural, target = _direct_terminal_liveness_case(
        3_600,
        terminal_age_seconds=317,
        containing_first_before_exit=True,
    )
    direct = build_direct_opportunity_lattice(bars, structural)

    assert target in {item.structural_anchor_key for item in direct.opportunities}
    assert target not in direct.excluded_anchor_keys


def test_direct_terminal_liveness_assigns_exact_cap_boundary_to_preceding_bar() -> None:
    bars = list(_bars(300, 100))
    dates = tuple(sorted({item.bar.source_date for item in bars}))
    structural = build_structural_eligibility_lattice(
        bars,
        decision_dates=dates,
        allowed_tail_end_ns=bars[-1].bar.end_ns,
    )
    target = structural.eligible_anchor_keys[0]
    # The target's aligned 6h cap is exactly bars[73].start_ns. Removing that
    # next bar must not move the half-open endpoint out of bars[72].
    direct = build_direct_opportunity_lattice(bars[:73] + bars[74:], structural)

    assert target in {item.structural_anchor_key for item in direct.opportunities}
    assert target not in direct.excluded_anchor_keys


@pytest.mark.parametrize("horizon_seconds", DIRECT_TERMINAL_HORIZONS_SECONDS)
def test_any_failed_direct_terminal_horizon_excludes_the_whole_anchor(
    horizon_seconds: int,
) -> None:
    bars, structural, target = _direct_terminal_liveness_case(
        horizon_seconds,
        terminal_age_seconds=300,
    )
    direct = build_direct_opportunity_lattice(bars, structural)

    assert target not in {item.structural_anchor_key for item in direct.opportunities}
    assert target in direct.excluded_anchor_keys


@pytest.mark.parametrize("break_kind", ("GAP", "LINEAGE"))
def test_direct_terminal_liveness_requires_contiguous_same_lineage_5m_coverage(
    break_kind: str,
) -> None:
    bars = list(_bars(300, 100))
    dates = tuple(sorted({item.bar.source_date for item in bars}))
    structural = build_structural_eligibility_lattice(
        bars,
        decision_dates=dates,
        allowed_tail_end_ns=bars[-1].bar.end_ns,
    )
    target = structural.eligible_anchor_keys[0]
    if break_kind == "GAP":
        direct_bars = bars[:12] + bars[13:]
    else:
        crossed = bars[12]
        bars[12] = BarWithOutcomeSpan(
            replace(crossed.bar, contract="6EM2"),
            crossed.outcome_span_id,
        )
        direct_bars = bars

    direct = build_direct_opportunity_lattice(direct_bars, structural)

    assert target in direct.excluded_anchor_keys
    assert target not in {item.structural_anchor_key for item in direct.opportunities}
    assert direct.opportunity_count + len(direct.excluded_anchor_keys) == (
        direct.structural_anchor_count
    )


def test_global_occupancy_skips_cross_lineage_overlap_in_timestamp_order() -> None:
    first_anchor = _path_anchor(contract="ZZZ")
    second_anchor = _path_anchor(offset_seconds=300, contract="AAA", outcome_span_id=2)
    policy = next(iter_anchor_policies())
    mask = PolicyMask.from_records(
        policy,
        "MOMENTUM_RETURN",
        "LONG",
        (first_anchor, second_anchor),
    )
    recipe = _recipe(policy.policy_id, entry_kind="MARKET", exit_kind="TERMINAL")
    first_path = _outcome_path(
        ((5, 100, 100, 100, 100), (1_804, 105, 105, 105, 105)),
        contract="ZZZ",
    )
    second_path = _outcome_path(
        ((300, 100, 100, 100, 100), (2_099, 105, 105, 105, 105)),
        contract="AAA",
        outcome_span_id=2,
    )
    day = first_anchor.source_date
    evaluation = SharedPathEvaluator((first_path, second_path)).evaluate(
        recipe,
        mask,
        rule_schedule=None,
        reporting_group_by_date={day: "R1"},
        outer_validation_by_date={day: "B3"},
    )

    assert evaluation.fill_count == 1
    assert evaluation.skipped_occupied_count == 1


def test_complete_coverage_verifier_rejects_silently_missing_path_lineage() -> None:
    policy = next(iter_anchor_policies())
    first = _path_anchor()
    missing = _path_anchor(contract="6EM2", outcome_span_id=2)
    mask = PolicyMask.from_records(
        policy,
        "MOMENTUM_RETURN",
        "LONG",
        (first, missing),
    )
    recipe = _recipe(policy.policy_id, entry_kind="MARKET", exit_kind="TERMINAL")
    path = _outcome_path(((5, 100, 100, 100, 100), (1_804, 105, 105, 105, 105)))
    day = first.source_date
    evaluation = SharedPathEvaluator((path,)).evaluate(
        recipe,
        mask,
        rule_schedule=None,
        reporting_group_by_date={day: "R1"},
        outer_validation_by_date={day: "B3"},
    )

    assert evaluation.raw_signal_count == 1
    with pytest.raises(ValueError, match="silently omitted"):
        verify_complete_evaluation_coverage(evaluation, mask)


def test_feature_only_controls_are_distinct_group_cardinality_preserving_and_committed() -> None:
    fives = _bars(300, 150)
    decision_dates = tuple(sorted({item.bar.source_date for item in fives}))
    lattice = build_control_opportunity_lattice(
        fives,
        decision_dates=decision_dates,
        allowed_tail_end_ns=fives[-1].bar.end_ns,
    )
    policy = next(iter_anchor_policies())
    records = tuple(item.anchor("LONG") for item in lattice.opportunities[:2])
    real = PolicyMask.from_records(policy, "MOMENTUM_RETURN", "LONG", records)
    controls = freeze_feature_control_masks(
        "SEARCH",
        real,
        lattice,
        reporting_group_by_date={day: "R1" for day in decision_dates},
    )

    assert controls.sample_eligible is True
    assert controls.circular is not None and controls.matched is not None
    assert controls.circular.mask_sha256 != real.mask_sha256
    assert controls.matched.mask_sha256 != real.mask_sha256
    assert controls.circular.support_count == controls.matched.support_count == 2
    assert canonical_sha256(controls.definition_dict()) == controls.commitment_sha256
    assert all(0 <= item.fallback_level <= 6 for item in controls.matched_pairs)
    assert "evaluable_daily_counts" in controls.as_dict()
    assert "raw_daily_counts" not in controls.as_dict()
    assert frozen_control_masks_from_dict(controls.as_dict()) == controls
    duplicated_control = replace(
        controls.matched_pairs[1],
        control_anchor_key=controls.matched_pairs[0].control_anchor_key,
    )
    with pytest.raises(ValueError, match="bijection"):
        replace(
            controls,
            matched_pairs=(controls.matched_pairs[0], duplicated_control),
        )


def test_control_opportunity_lattice_canonicalizes_nonmonotonic_uint64_segments() -> None:
    source = _bars(300, 220)
    high_segment_id = 2**63 + 123
    bars = tuple(
        BarWithOutcomeSpan(
            replace(
                wrapped.bar,
                segment_id=high_segment_id if index < 110 else 7,
            ),
            wrapped.outcome_span_id,
        )
        for index, wrapped in enumerate(source)
    )
    decision_dates = tuple(sorted({item.bar.source_date for item in bars}))

    first = build_control_opportunity_lattice(
        bars,
        decision_dates=decision_dates,
        allowed_tail_end_ns=bars[-1].bar.end_ns,
    )
    second = build_control_opportunity_lattice(
        bars,
        decision_dates=decision_dates,
        allowed_tail_end_ns=bars[-1].bar.end_ns,
    )

    assert len(first.opportunities) == 12
    assert first.opportunities == tuple(sorted(first.opportunities))
    assert Counter(item.segment_id for item in first.opportunities) == {
        7: 6,
        high_segment_id: 6,
    }
    assert first == second
    with pytest.raises(ValueError, match="must be non-empty"):
        replace(first, opportunities=())
    with pytest.raises(ValueError, match="non-canonical"):
        replace(first, opportunities=tuple(reversed(first.opportunities)))


def test_deterministic_complete_maximum_matching_repairs_greedy_dead_end() -> None:
    module = __import__(
        "campaigns.ai_all_cases_v1.symbolic",
        fromlist=["_deterministic_complete_maximum_matching"],
    )
    adjacency = {0: (10, 11), 1: (10,)}

    first = module._deterministic_complete_maximum_matching(adjacency, (0, 1))
    second = module._deterministic_complete_maximum_matching(adjacency, (0, 1))

    assert first == second == {0: 11, 1: 10}
    assert module._deterministic_complete_maximum_matching({0: (10,), 1: (10,)}, (0, 1)) is None

    rng = Random(91)
    for node_count in range(1, 6):
        targets = tuple(range(10, 10 + node_count + 1))
        for _case in range(40):
            graph = {
                source: tuple(target for target in targets if rng.randrange(3) != 0)
                for source in range(node_count)
            }

            def brute(
                source: int,
                used: frozenset[int],
                *,
                total: int = node_count,
                edges: dict[int, tuple[int, ...]] = graph,
            ) -> bool:
                if source == total:
                    return True
                return any(
                    target not in used and brute(source + 1, used | {target})
                    for target in edges[source]
                )

            expected = brute(0, frozenset())
            observed = module._deterministic_complete_maximum_matching(
                graph,
                tuple(range(node_count)),
            )
            assert (observed is not None) is expected
            if observed is not None:
                assert len(set(observed.values())) == node_count


def test_circular_control_keeps_saturated_orbit_invariant_before_matched_failure() -> None:
    fives = _bars(300, 500)
    decision_dates = tuple(sorted({item.bar.source_date for item in fives}))
    lattice = build_control_opportunity_lattice(
        fives,
        decision_dates=decision_dates,
        allowed_tail_end_ns=fives[-1].bar.end_ns,
    )
    by_group: dict[tuple[date, str, int, int], list] = {}
    for opportunity in lattice.opportunities:
        by_group.setdefault(opportunity.group_key, []).append(opportunity)
    groups = [rows for rows in by_group.values() if len(rows) >= 2]
    assert len(groups) >= 2
    policy = next(iter_anchor_policies())
    records = tuple(item.anchor("LONG") for item in (*groups[0], groups[1][0]))
    real = PolicyMask.from_records(policy, "MOMENTUM_RETURN", "LONG", records)

    controls = freeze_feature_control_masks(
        "SEARCH",
        real,
        lattice,
        reporting_group_by_date={day: "R1" for day in decision_dates},
    )

    assert controls.sample_eligible is False
    assert controls.ineligibility_reason == "MATCHED_COMPLETE_MAXIMUM_MATCHING_IMPOSSIBLE"


def test_one_hour_control_preserves_real_lag_and_uses_native_stop_geometry() -> None:
    fives = _bars(300, 500)
    start_ns = fives[0].bar.start_ns
    hours = tuple(
        _wrapped_bar(
            3_600,
            start_ns + index * 3_600 * ONE_SECOND_NS,
            1_000,
            1_500 + index,
            900,
            1_000,
        )
        for index in range(42)
    )
    decision_dates = tuple(sorted({item.bar.source_date for item in fives}))
    lattice = build_control_opportunity_lattice(
        fives,
        decision_dates=decision_dates,
        allowed_tail_end_ns=fives[-1].bar.end_ns,
        signal_bars_by_timeframe={300: fives, 3_600: hours},
    )
    candidate = next(
        item
        for item in build_base_event_catalog().candidates
        if item.trigger_timeframe_seconds == 3_600 and item.direction == "LONG"
    )
    immediate = build_delay_catalog()[0]
    policy = next(
        item
        for item in iter_anchor_policies()
        if item.base_candidate_id == candidate.candidate_id and item.delay_id == immediate.delay_id
    )
    trigger_by_end = {
        item.end_ns: item for item in lattice.trigger_states if item.timeframe_seconds == 3_600
    }
    eligible = [
        item
        for item in lattice.opportunities
        if item.anchor_ns in trigger_by_end
        and item.source_date == lattice.opportunities[0].source_date
    ]
    if len(eligible) < 2:
        eligible = [item for item in lattice.opportunities if item.anchor_ns in trigger_by_end]
    real_records = tuple(
        item.anchor("LONG", trigger_by_end[item.anchor_ns]) for item in eligible[:2]
    )
    real = PolicyMask.from_records(policy, candidate.family, "LONG", real_records)
    controls = freeze_feature_control_masks(
        "SEARCH",
        real,
        lattice,
        reporting_group_by_date={day: "R1" for day in decision_dates},
    )

    assert controls.sample_eligible is True
    assert controls.trigger_timeframe_seconds == 3_600
    assert controls.circular is not None and controls.matched is not None
    for control in (*controls.circular.records, *controls.matched.records):
        assert control.anchor_ns - control.trigger_end_ns == 0
        assert control.trigger_end_ns - control.trigger_start_ns == 3_600 * ONE_SECOND_NS
        assert control.trigger_high_ticks >= 1_500
    stop = next(
        item
        for item in build_entry_catalog().candidates
        if item.kind == "STOP_SIGNAL_EXTREME" and item.parameter("buffer_ticks") == 1
    )
    orders = freeze_entry_orders(controls.circular, (stop,)).orders
    assert all(item.order_ticks == item.anchor.trigger_high_ticks + 1 for item in orders)
    assert all(item.order_ticks > 1_400 for item in orders)


def test_delayed_persistence_uses_later_native_bar_and_rejects_internal_gap() -> None:
    stage, bars, dates = _stage()
    candidate = next(
        item
        for item in build_base_event_catalog().candidates
        if item.trigger_timeframe_seconds == 300 and item.direction == "LONG"
    )
    delay = next(item for item in build_delay_catalog() if item.kind == "NATIVE_PLUS_1_ALIVE")
    series = stage.series_by_timeframe[300]
    event_index = 100
    event_bar = series.bars[event_index]
    event = EventOccurrence(
        candidate.candidate_id,
        "LONG",
        300,
        event_index,
        event_bar.bar.source_date,
        event_bar.bar.contract,
        event_bar.outcome_span_id,
        event_bar.bar.segment_id,
        event_bar.bar.start_ns,
        event_bar.bar.end_ns,
        event_bar.bar.open_ticks,
        event_bar.bar.high_ticks,
        event_bar.bar.low_ticks,
        event_bar.bar.close_ticks,
        100,
        (("level_ticks", 0), ("persistence_kind", 1)),
    )
    target_ns = series.bars[event_index + 1].bar.end_ns

    assert stage._alive_at(candidate, event, target_ns, delay) is True

    gapped_fives = bars[300][: event_index + 1] + bars[300][event_index + 2 :]
    gapped = build_symbolic_stage(
        {300: gapped_fives, 1_800: bars[1_800], 3_600: bars[3_600]},
        dates,
    )
    assert gapped._alive_at(candidate, event, target_ns + 300 * ONE_SECOND_NS, delay) is False


def test_delayed_context_qualifies_on_latest_completed_bar_at_execution_anchor() -> None:
    stage, _bars_by_timeframe, _dates = _stage()
    candidate = next(
        item
        for item in build_base_event_catalog().candidates
        if item.trigger_timeframe_seconds == 300 and item.direction == "LONG"
    )
    fives = stage.series_by_timeframe[300].bars
    trigger = fives[500].bar
    execution = fives[600]
    anchor = AnchorRecord(
        execution.bar.source_date,
        execution.bar.contract,
        execution.outcome_span_id,
        execution.bar.segment_id,
        execution.bar.end_ns,
        "LONG",
        trigger.start_ns,
        trigger.end_ns,
        trigger.open_ticks,
        trigger.high_ticks,
        trigger.low_ticks,
        trigger.close_ticks,
        100,
        20,
        (),
    )
    eligible_context = None
    eligible_index = None
    for context in build_context_catalog()[1:]:
        index = stage._context_index(context.timeframe_seconds, execution, anchor.anchor_ns)
        if index is not None and stage._context_state(candidate, context, index):
            eligible_context = context
            eligible_index = index
            break

    assert eligible_context is not None and eligible_index is not None
    context_bar = stage.series_by_timeframe[eligible_context.timeframe_seconds].bars[eligible_index]
    assert context_bar.bar.end_ns > anchor.trigger_end_ns
    assert stage.context_matches(candidate, eligible_context, anchor) is True


def test_symbolic_top24_is_distinct_from_final_six_quota() -> None:
    second_start = PATH_START_NS + 7 * 86_400 * ONE_SECOND_NS
    offsets = tuple(index * 22_000 for index in range(6))
    path_rows = tuple(
        sorted(
            (
                *(item for offset in offsets for item in ((offset + 5, 100, 100, 100, 100),)),
                *(
                    item
                    for offset in offsets
                    for horizon in (1_800, 3_600, 7_200, 10_800, 21_600)
                    for item in ((offset + horizon + 4, 105, 105, 105, 105),)
                ),
            )
        )
    )
    first_path = _outcome_path(path_rows, duration_seconds=140_400)
    second_path = _outcome_path(
        path_rows,
        duration_seconds=140_400,
        contract="6EM2",
        outcome_span_id=2,
        path_start_ns=second_start,
    )
    policy = next(iter_anchor_policies())
    anchors = tuple(_path_anchor(offset_seconds=offset) for offset in offsets) + tuple(
        _path_anchor(
            offset_seconds=offset,
            contract="6EM2",
            outcome_span_id=2,
            path_start_ns=second_start,
        )
        for offset in offsets
    )
    mask = PolicyMask.from_records(policy, "MOMENTUM_RETURN", "LONG", anchors)
    recipes = tuple(islice(iter_complete_strategy_recipes((policy.policy_id,)), 25))
    evaluator = SharedPathEvaluator((first_path, second_path))
    first_days = {item.source_date for item in anchors[:6]}
    second_days = {item.source_date for item in anchors[6:]}
    reporting_groups = {day: "R1" for day in first_days | second_days}
    outer_groups = {
        **{day: "B1" for day in first_days},
        **{day: "B2" for day in second_days},
    }
    evaluations = tuple(
        evaluator.evaluate(
            recipe,
            mask,
            rule_schedule=None,
            reporting_group_by_date=reporting_groups,
            outer_validation_by_date=outer_groups,
        )
        for recipe in recipes
    )

    selection = select_symbolic_top24_for_meta("B3", evaluations)

    assert len(selection.selected_strategy_ids) == 24
    assert all(
        item in {recipe.strategy_id for recipe in recipes}
        for item in selection.selected_strategy_ids
    )
