from __future__ import annotations

import copy
import json
from dataclasses import replace
from datetime import date, timedelta
from fractions import Fraction
from itertools import islice
from types import SimpleNamespace

import numpy as np
import pytest

from campaigns.ai_all_cases_v1 import ml, symbolic


def _direct_candidate(
    learner: str = "ENET_A",
    *,
    feature_set: str = "PRICE_GEOMETRY_36",
    timeframe: int = 300,
    horizon: int = 3_600,
    rate: Fraction = Fraction(1, 20),
) -> ml.DirectCandidate:
    return next(
        candidate
        for candidate in ml.DIRECT_CANDIDATE_CATALOG
        if candidate.learner_id == learner
        and candidate.feature_set_id == feature_set
        and candidate.decision_timeframe_seconds == timeframe
        and candidate.horizon_seconds == horizon
        and candidate.action_rate == rate
    )


def _meta_candidate(
    classifier: str = "META_ENET",
    *,
    feature_set: str = "FULL_MTF_213",
    rank: int = 1,
    rate: Fraction = Fraction(3, 10),
) -> ml.MetaCandidate:
    return next(
        candidate
        for candidate in ml.META_CANDIDATE_CATALOG
        if candidate.classifier_id == classifier
        and candidate.feature_set_id == feature_set
        and candidate.symbolic_rank_slot == rank
        and candidate.retain_rate == rate
    )


def _ranking_certificate(
    *,
    world: str = "REAL",
    fold_key: str = "SEARCH_FINAL",
    training_dates: tuple[date, ...],
    base_trigger_family: str = "MACD_STATE",
    salt: str = "default",
    count: int = 24,
    first_recipe: symbolic.CompleteStrategyRecipe | None = None,
    first_policy: symbolic.AnchorPolicy | None = None,
) -> ml.SymbolicRankingCertificate:
    entry = symbolic.build_entry_catalog().candidates[0]
    exit_policy = symbolic.build_exit_catalog().candidates[0]
    if (first_recipe is None) != (first_policy is None):
        raise AssertionError("first recipe and policy must be supplied together")
    offset = int(ml.canonical_sha256({"salt": salt})[:4], 16) % 128
    policies = list(islice(symbolic.iter_anchor_policies(), offset, offset + count + 1))
    if first_policy is not None:
        policies = [
            first_policy,
            *(item for item in policies if item.policy_id != first_policy.policy_id),
        ]
    policies = policies[:count]
    base_by_id = {
        item.candidate_id: item for item in symbolic.build_base_event_catalog().candidates
    }

    def recipe_at(index: int, policy: symbolic.AnchorPolicy) -> symbolic.CompleteStrategyRecipe:
        if index == 1 and first_recipe is not None:
            assert first_recipe.anchor_policy_id == policy.policy_id
            return first_recipe
        definition = {
            "anchor_policy_id": policy.policy_id,
            "entry_policy_id": entry.entry_id,
            "exit_policy_id": exit_policy.exit_id,
            "schema": symbolic.COMPLETE_STRATEGY_SCHEMA,
        }
        return symbolic.CompleteStrategyRecipe(
            (entry.selection_rank - 1) * symbolic.EXIT_POLICY_COUNT + exit_policy.selection_rank,
            symbolic.canonical_sha256(definition),
            1,
            policy.policy_id,
            entry.entry_id,
            exit_policy.exit_id,
        )

    recipes = tuple(recipe_at(index, policy) for index, policy in enumerate(policies, start=1))
    ranked = tuple(
        ml.RankedSymbolicStrategy(
            index,
            recipe.strategy_id,
            base_by_id[policy.base_candidate_id].family,
            recipe.anchor_policy_id,
            policy.base_candidate_id,
            policy.context_id,
            policy.time_filter_id,
            policy.delay_id,
            recipe.entry_policy_id,
            recipe.exit_policy_id,
        )
        for index, (recipe, policy) in enumerate(zip(recipes, policies, strict=True), start=1)
    )
    return ml.build_symbolic_ranking_certificate(
        null_world=world,
        fold_key=fold_key,
        training_dates=training_dates,
        ranked_strategies=ranked,
    )


def _matrix(
    *,
    row_count: int = 240,
    feature_set: str = "PRICE_GEOMETRY_36",
    seed: int = 17,
    meta: bool = False,
    one_date_per_row: bool = False,
    targets: np.ndarray | None = None,
    base_trigger_family: str = "MACD_STATE",
    ranking_world: str = "REAL",
    ranking_fold_key: str = "SEARCH_FINAL",
    ranking_training_dates: tuple[date, ...] | None = None,
    ranking_salt: str = "default",
    ranking_first_recipe: symbolic.CompleteStrategyRecipe | None = None,
    ranking_first_policy: symbolic.AnchorPolicy | None = None,
) -> ml.TrainingMatrix:
    rng = np.random.default_rng(seed)
    names = ml.FEATURE_NAMES_BY_SET[feature_set]
    values = rng.normal(size=(row_count, len(names)))
    values[3, 0] = np.nan
    if targets is None:
        signal = np.nan_to_num(values[:, 1]) + 0.4 * np.nan_to_num(values[:, 2])
        targets = (
            (signal + rng.normal(scale=0.5, size=row_count) > 0).astype(np.float64)
            if meta
            else signal * 0.2 + rng.normal(scale=0.05, size=row_count)
        )
    terminal_move_ticks = None
    if not meta:
        terminal_move_ticks = np.rint(np.asarray(targets) * 50.0).astype(np.int64)
        targets = terminal_move_ticks.astype(np.float64) / 50.0
    epoch_day = date(2020, 1, 1)
    decision_dates = tuple(
        epoch_day + timedelta(days=index if one_date_per_row else index // 10)
        for index in range(row_count)
    )
    certificate = (
        _ranking_certificate(
            world=ranking_world,
            fold_key=ranking_fold_key,
            training_dates=(
                tuple(sorted(set(decision_dates)))
                if ranking_training_dates is None
                else ranking_training_dates
            ),
            base_trigger_family=base_trigger_family,
            salt=ranking_salt,
            first_recipe=ranking_first_recipe,
            first_policy=ranking_first_policy,
        )
        if meta
        else None
    )
    entry_ns = (np.arange(row_count, dtype=np.int64) + 1) * 1_000_000_000
    return ml.TrainingMatrix(
        feature_set_id=feature_set,
        feature_names=names,
        row_ids=tuple(f"row-{index:06d}" for index in range(row_count)),
        decision_dates=decision_dates,
        decision_ns=entry_ns,
        entry_ns=entry_ns,
        label_exit_ns=entry_ns + 10_000,
        values=values,
        targets=np.asarray(targets, dtype=np.float64),
        atr_ticks=np.full(row_count, 50.0),
        contracts=tuple("6E" if index % 2 == 0 else "M6E" for index in range(row_count)),
        outcome_span_ids=(np.arange(row_count, dtype=np.int64) // 4) % 5 + 1,
        segment_ids=np.ones(row_count, dtype=np.int64),
        outcome_lineage_sha256="f" * 64,
        opportunity_lattice_sha256="e" * 64,
        entry_schedule_sha256="d" * 64,
        match_strata=tuple(f"stratum-{index % 3}" for index in range(row_count)),
        base_directions=(
            np.where(np.arange(row_count) % 2 == 0, 1, -1).astype(np.int8) if meta else None
        ),
        terminal_move_ticks=terminal_move_ticks,
        realized_net_ticks=(
            np.where(np.asarray(targets) > 0, 12, -10).astype(np.int64) if meta else None
        ),
        task_timeframe_seconds=None if meta else 300,
        task_horizon_seconds=None if meta else 3_600,
        base_strategy_id=(
            certificate.ranked_strategies[0].strategy_id if certificate is not None else None
        ),
        base_trigger_family=(
            certificate.ranked_strategies[0].trigger_family
            if meta and certificate is not None and certificate.ranked_strategies
            else None
        ),
        symbolic_ranking_certificate=certificate,
        expert_artifact_sha256s=(("c" * 64,) * row_count if meta else None),
        expert_formula_sha256=(symbolic.EXPERT_FEATURE_FORMULA_SHA256 if meta else None),
    )


def _ranking_dates(matrix: ml.TrainingMatrix) -> tuple[date, ...]:
    certificate = matrix.symbolic_ranking_certificate
    assert certificate is not None
    return certificate.training_dates


def _bars(timeframe: int, count: int) -> ml.CausalBarSeries:
    index = np.arange(count, dtype=np.float64)
    close = 100.0 + 0.002 * index + 0.1 * np.sin(index / 11.0)
    open_price = close - 0.01 * np.cos(index / 7.0)
    high = np.maximum(open_price, close) + 0.05
    low = np.minimum(open_price, close) - 0.05
    volume = 100.0 + index % 17
    imbalance = 0.1 * np.sin(index / 5.0)
    buy = volume * (1.0 + imbalance) / 2.0
    sell = volume - buy
    return ml.CausalBarSeries(
        timeframe_seconds=timeframe,
        bar_end_ns=(np.arange(count, dtype=np.int64) + 1) * timeframe * 1_000_000_000,
        open=open_price,
        high=high,
        low=low,
        close=close,
        volume=volume,
        trade_count=20.0 + index % 9,
        source_dates=tuple(date(2024, 1, 1) for _ in range(count)),
        contracts=tuple("6E" for _ in range(count)),
        outcome_span_ids=np.ones(count, dtype=np.int64),
        segment_ids=np.ones(count, dtype=np.int64),
        stage_date_ranks=np.zeros(count, dtype=np.int64),
        stage_key="SEARCH",
        buy_volume=buy,
        sell_volume=sell,
    )


def _direct_feature_rows(
    matrix: ml.TrainingMatrix,
    count: int,
    *,
    stage_key: str = "WF1",
) -> ml.CausalFeatureRows:
    assert matrix.task_timeframe_seconds is not None
    values = np.nan_to_num(matrix.values[:count], nan=0.0)
    return ml.CausalFeatureRows(
        feature_set_id=matrix.feature_set_id,
        feature_names=matrix.feature_names,
        row_ids=matrix.row_ids[:count],
        decision_ns=matrix.decision_ns[:count],
        entry_ns=matrix.entry_ns[:count],
        source_dates=matrix.decision_dates[:count],
        contracts=matrix.contracts[:count],
        outcome_span_ids=matrix.outcome_span_ids[:count],
        segment_ids=matrix.segment_ids[:count],
        stage_date_ranks=np.arange(count, dtype=np.int64),
        stage_key=stage_key,
        decision_timeframe_seconds=matrix.task_timeframe_seconds,
        entry_schedule_sha256=matrix.entry_schedule_sha256,
        source_commitment_sha256="b" * 64,
        retained_input_indexes=np.arange(count, dtype=np.int64),
        aligned_bar_indexes=np.zeros((count, len(ml.TF_ORDER)), dtype=np.int64),
        values=values,
        atr_ticks_by_timeframe=np.repeat(matrix.atr_ticks[:count, None], len(ml.TF_ORDER), axis=1),
    )


def _schedule(
    matrix: ml.TrainingMatrix,
    count: int,
    *,
    stage_key: str = "WF1",
    candidate: ml.DirectCandidate | None = None,
) -> ml.OutcomeFreeExecutionSchedule:
    assert matrix.task_timeframe_seconds is not None
    assert matrix.task_horizon_seconds is not None
    candidate = candidate or _direct_candidate(
        feature_set=matrix.feature_set_id,
        timeframe=matrix.task_timeframe_seconds,
        horizon=matrix.task_horizon_seconds,
    )
    dates = tuple(sorted(set(matrix.decision_dates)))
    certificate = ml.build_stage_partition_date_certificate(
        stage_key,
        dates,
        upstream_plan_sha256=ml.stage_partition_date_plan_sha256(stage_key, dates),
    )
    feature_rows = _direct_feature_rows(matrix, count, stage_key=stage_key)
    return ml.OutcomeFreeExecutionSchedule(
        candidate_id=candidate.candidate_id,
        feature_set_id=candidate.feature_set_id,
        task_timeframe_seconds=candidate.decision_timeframe_seconds,
        task_horizon_seconds=candidate.horizon_seconds,
        row_ids=matrix.row_ids[:count],
        decision_dates=matrix.decision_dates[:count],
        partition_date_certificate=certificate,
        decision_ns=tuple(int(value) for value in matrix.decision_ns[:count]),
        entry_ns=tuple(int(value) for value in matrix.entry_ns[:count]),
        planned_exit_ns=tuple(
            int(value) + candidate.horizon_seconds * 1_000_000_000
            for value in matrix.entry_ns[:count]
        ),
        contracts=matrix.contracts[:count],
        outcome_span_ids=tuple(int(value) for value in matrix.outcome_span_ids[:count]),
        segment_ids=tuple(int(value) for value in matrix.segment_ids[:count]),
        stage_key=stage_key,
        lineage_sha256=matrix.outcome_lineage_sha256,
        opportunity_lattice_sha256=matrix.opportunity_lattice_sha256,
        entry_schedule_sha256=matrix.entry_schedule_sha256,
        feature_rows_sha256=feature_rows.artifact_sha256,
    )


def _causal_rows(
    *, plus_expert: bool = False
) -> tuple[ml.CausalFeatureRows, dict[int, ml.CausalBarSeries]]:
    bars = {
        300: _bars(300, 2_200),
        1_800: _bars(1_800, 400),
        3_600: _bars(3_600, 200),
    }
    decisions = np.arange(130, 180, dtype=np.int64) * 3_600 * 1_000_000_000
    entries = decisions + 1_000_000_000
    identifiers = tuple(f"anchor-{index:03d}" for index in range(len(entries)))
    expert_artifacts: tuple[symbolic.CausalExpertFeatureArtifact, ...] | None = None
    if plus_expert:
        candidate = next(
            item
            for item in symbolic.build_base_event_catalog().candidates
            if item.trigger_timeframe_seconds == 300 and item.direction == "LONG"
        )
        context = symbolic.build_context_catalog()[0]
        policy = next(
            item
            for item in symbolic.iter_anchor_policies()
            if item.base_candidate_id == candidate.candidate_id
            and item.context_id == context.context_id
        )
        market = symbolic.build_entry_catalog().candidates[0]
        exit_policy = symbolic.build_exit_catalog().candidates[0]
        artifacts: list[symbolic.CausalExpertFeatureArtifact] = []
        for decision in decisions:
            anchor = symbolic.AnchorRecord(
                date(2024, 1, 1),
                "6E",
                1,
                1,
                int(decision),
                candidate.direction,
                int(decision) - 300 * 1_000_000_000,
                int(decision),
                2_000_000,
                2_000_010,
                1_999_990,
                2_000_005,
                2_000,
                20,
                (),
            )
            order_definition = {
                "anchor": anchor.as_dict(),
                "entry_policy_id": market.entry_id,
                "expires_ns": int(decision) + 300 * 1_000_000_000,
                "kind": market.kind,
                "order_ticks": None,
                "schema": symbolic.PATH_OUTCOME_SCHEMA,
                "valid_from_ns": int(decision),
            }
            order = symbolic.FrozenEntryOrder(
                symbolic.canonical_sha256(order_definition),
                anchor,
                market.entry_id,
                market.kind,
                None,
                int(decision),
                int(decision) + 300 * 1_000_000_000,
            )
            artifacts.append(
                symbolic.build_causal_expert_feature_artifact(
                    candidate, context, policy, anchor, order, exit_policy
                )
            )
        expert_artifacts = tuple(artifacts)
        identifiers = tuple(item.order_id for item in expert_artifacts)
    anchors = ml.CausalAnchorRows(
        row_ids=identifiers,
        decision_ns=decisions,
        entry_ns=entries,
        source_dates=tuple(date(2024, 1, 1) for _ in entries),
        contracts=tuple("6E" for _ in entries),
        outcome_span_ids=np.ones(len(entries), dtype=np.int64),
        segment_ids=np.ones(len(entries), dtype=np.int64),
        stage_date_ranks=np.zeros(len(entries), dtype=np.int64),
        stage_key="SEARCH",
        decision_timeframe_seconds=300,
        entry_schedule_sha256="a" * 64,
    )
    rows = ml.build_causal_feature_rows(
        anchors=anchors,
        bars_by_timeframe=bars,
        feature_set_id=("FULL_MTF_PLUS_EXPERT_221" if plus_expert else "FULL_MTF_213"),
        expert_artifacts=expert_artifacts,
    )
    return rows, bars


def _symbolic_orders_for_feature_rows(
    rows: ml.CausalFeatureRows,
    indexes: tuple[int, ...],
) -> tuple[
    ml.CausalFeatureRows,
    tuple[symbolic.FrozenEntryOrder, ...],
    symbolic.EntryOrderBatch,
    symbolic.CompleteStrategyRecipe,
    tuple[symbolic.CausalExpertFeatureArtifact, ...],
    symbolic.AnchorPolicy,
]:
    market = next(
        item for item in symbolic.build_entry_catalog().candidates if item.kind == "MARKET"
    )
    base_candidate = next(
        item
        for item in symbolic.build_base_event_catalog().candidates
        if item.trigger_timeframe_seconds == 300 and item.direction == "LONG"
    )
    context = symbolic.build_context_catalog()[0]
    policy = next(
        item
        for item in symbolic.iter_anchor_policies()
        if item.base_candidate_id == base_candidate.candidate_id
        and item.context_id == context.context_id
    )
    exit_policy = symbolic.build_exit_catalog().candidates[0]
    orders: list[symbolic.FrozenEntryOrder] = []
    for index in indexes:
        decision = int(rows.decision_ns[index])
        direction = base_candidate.direction
        anchor = symbolic.AnchorRecord(
            rows.source_dates[index],
            rows.contracts[index],
            int(rows.outcome_span_ids[index]),
            int(rows.segment_ids[index]),
            decision,
            direction,
            decision - 300 * 1_000_000_000,
            decision,
            2_000_000,
            2_000_010,
            1_999_990,
            2_000_005,
            2_000,
            20,
            (),
        )
        definition = {
            "anchor": anchor.as_dict(),
            "entry_policy_id": market.entry_id,
            "expires_ns": decision + 300 * 1_000_000_000,
            "kind": market.kind,
            "order_ticks": None,
            "schema": symbolic.PATH_OUTCOME_SCHEMA,
            "valid_from_ns": decision,
        }
        orders.append(
            symbolic.FrozenEntryOrder(
                symbolic.canonical_sha256(definition),
                anchor,
                market.entry_id,
                market.kind,
                None,
                decision,
                decision + 300 * 1_000_000_000,
            )
        )
    selected = rows.take(indexes)
    canonical_orders = tuple(orders)
    keyed = replace(selected, row_ids=tuple(item.order_id for item in canonical_orders))
    anchor_policy_id = policy.policy_id
    batch_definition = {
        "anchor_policy_id": anchor_policy_id,
        "orders": [item.as_dict() for item in canonical_orders],
        "schema": symbolic.PATH_OUTCOME_SCHEMA,
    }
    batch = symbolic.EntryOrderBatch(
        anchor_policy_id,
        canonical_orders,
        symbolic.canonical_sha256(batch_definition),
    )
    recipe_definition = {
        "anchor_policy_id": anchor_policy_id,
        "entry_policy_id": market.entry_id,
        "exit_policy_id": exit_policy.exit_id,
        "schema": symbolic.COMPLETE_STRATEGY_SCHEMA,
    }
    recipe = symbolic.CompleteStrategyRecipe(
        (market.selection_rank - 1) * symbolic.EXIT_POLICY_COUNT + exit_policy.selection_rank,
        symbolic.canonical_sha256(recipe_definition),
        1,
        anchor_policy_id,
        market.entry_id,
        exit_policy.exit_id,
    )
    experts = tuple(
        symbolic.build_causal_expert_feature_artifact(
            base_candidate,
            context,
            policy,
            order.anchor,
            order,
            exit_policy,
        )
        for order in canonical_orders
    )
    return keyed, canonical_orders, batch, recipe, experts, policy


def _expert_base_family(
    experts: tuple[symbolic.CausalExpertFeatureArtifact, ...],
) -> str:
    base_candidate_id = experts[0].base_candidate_id
    return next(
        item.family
        for item in symbolic.build_base_event_catalog().candidates
        if item.candidate_id == base_candidate_id
    )


def _self_consistent_expert_transplant(
    artifact: symbolic.CausalExpertFeatureArtifact,
    *,
    base_candidate_id: str | None = None,
    context_id: str | None = None,
    values: tuple[symbolic.CausalExpertValue, ...] | None = None,
) -> symbolic.CausalExpertFeatureArtifact:
    exact_values = artifact.values if values is None else values
    values_sha256 = symbolic.canonical_sha256([item.as_dict() for item in exact_values])
    inputs_sha256 = symbolic.canonical_sha256(
        {
            "original_inputs_sha256": artifact.inputs_sha256,
            "transplant_base_candidate_id": base_candidate_id,
            "transplant_context_id": context_id,
            "values_sha256": values_sha256,
        }
    )
    definition = {
        "anchor_key": list(artifact.anchor_key),
        "anchor_policy_id": artifact.anchor_policy_id,
        "base_candidate_id": base_candidate_id or artifact.base_candidate_id,
        "context_id": context_id or artifact.context_id,
        "exit_policy_id": artifact.exit_policy_id,
        "formula_sha256": artifact.formula_sha256,
        "inputs_sha256": inputs_sha256,
        "order_id": artifact.order_id,
        "schema": symbolic.EXPERT_FEATURE_SCHEMA,
        "values": [item.as_dict() for item in exact_values],
        "values_sha256": values_sha256,
    }
    return symbolic.CausalExpertFeatureArtifact(
        artifact.anchor_key,
        artifact.anchor_policy_id,
        base_candidate_id or artifact.base_candidate_id,
        context_id or artifact.context_id,
        artifact.order_id,
        artifact.exit_policy_id,
        exact_values,
        inputs_sha256,
        values_sha256,
        artifact.formula_sha256,
        symbolic.canonical_sha256(definition),
    )


def _crossfit_fixture(
    world: str,
    *,
    admitted_indexes: tuple[int, ...],
    long_indexes: tuple[int, ...],
    score_offset: float,
) -> ml.CrossFittedScores:
    count = 12
    candidate = _direct_candidate()
    directions = tuple(
        ml.TradeDirection.LONG
        if index in admitted_indexes and index in long_indexes
        else ml.TradeDirection.SHORT
        if index in admitted_indexes
        else ml.TradeDirection.FLAT
        for index in range(count)
    )
    dates = tuple(date(2024, 1, 1) + timedelta(days=index // 4) for index in range(count))
    entries = tuple(1_000_000_000 + index * 2_000_000_000 for index in range(count))
    provisional = ml.CrossFittedScores(
        candidate.candidate_id,
        world,
        tuple(range(count)),
        tuple(f"row-{index:02d}" for index in range(count)),
        tuple(ml.SEARCH_OUTER_FOLD_KEYS[index // 2] for index in range(count)),
        tuple(score_offset + index / 100.0 for index in range(count)),
        tuple(index in admitted_indexes for index in range(count)),
        tuple(index in admitted_indexes for index in range(count)),
        directions,
        tuple(20.0 + index for index in range(count)),
        dates,
        entries,
        tuple(value + 1_000_000_000 for value in entries),
        tuple("6E" for _ in range(count)),
        tuple(f"{index + 1:x}" * 64 for index in range(6)),
        tuple(0.1 for _ in range(6)),
        300,
        3_600,
        ("f" * 64,) * 6,
        ("e" * 64,) * 6,
        ("d" * 64,) * 6,
        ("b" * 64,) * 6,
        ("a" * 64,) * 6,
        tuple(index in admitted_indexes for index in range(count)),
        directions,
    )
    bound = _matrix_bound_to_crossfit(provisional)
    return replace(
        provisional,
        fold_source_matrix_sha256s=(ml.training_rows_sha256(bound),) * 6,
        fold_outcome_values_sha256s=(ml.outcome_values_sha256(bound),) * 6,
    )


def _matrix_bound_to_crossfit(
    result: ml.CrossFittedScores,
    matrix: ml.TrainingMatrix | None = None,
) -> ml.TrainingMatrix:
    source = _matrix(row_count=len(result.row_ids)) if matrix is None else matrix
    return replace(
        source,
        row_ids=result.row_ids,
        decision_dates=result.decision_dates,
        decision_ns=np.asarray(result.entry_ns, dtype=np.int64),
        entry_ns=np.asarray(result.entry_ns, dtype=np.int64),
        label_exit_ns=np.asarray(result.planned_exit_ns, dtype=np.int64),
        contracts=result.contracts,
    )


def test_exact_feature_closure_and_finite_catalog_documents() -> None:
    assert tuple(
        map(
            len,
            (
                ml.PRICE_GEOMETRY_36,
                ml.TECHNICAL_STATE_159,
                ml.FLOW_TIME_REGIME_90,
                ml.FULL_MTF_213,
                ml.FULL_MTF_PLUS_EXPERT_221,
            ),
        )
    ) == (36, 159, 90, 213, 221)
    assert ml.PRICE_GEOMETRY_36[:3] == (
        "tf0300_ret_atr_1",
        "tf0300_ret_atr_2",
        "tf0300_ret_atr_3",
    )
    assert ml.FULL_MTF_213[-1] == ("cross_volatility_regime_agreement_tf1800_tf3600")
    assert ml.FULL_MTF_PLUS_EXPERT_221[-8:] == ml.EXPERT_FEATURE_NAMES

    direct = ml.direct_catalog_document()
    meta = ml.meta_catalog_document()
    assert direct["candidate_count"] == 288
    assert meta["candidate_count"] == 192
    assert direct["catalog_sha256"] == ml.DIRECT_CATALOG_SHA256
    assert meta["catalog_sha256"] == ml.META_CATALOG_SHA256
    assert ml.canonical_sha256(direct["candidates"]) == direct["catalog_sha256"]
    assert ml.canonical_sha256(meta["candidates"]) == meta["catalog_sha256"]
    assert len({item.candidate_id for item in ml.DIRECT_CANDIDATE_CATALOG}) == 288
    assert len({item.candidate_id for item in ml.META_CANDIDATE_CATALOG}) == 192

    contract = ml.ml_engine_contract()
    assert contract == ml.ml_contract()
    assert contract["compute_caps"]["maximum_search_model_fits"] == 5_040
    assert contract["fit_recipe_sharing"]["direct_recipe_count"] == 144
    assert contract["fit_recipe_sharing"]["meta_recipe_count"] == 96
    assert contract["compute_caps"]["wf_holdout_fits"] == 0
    assert contract["cross_validation"]["block_sizes"] == [59, 59, 59, 59, 59, 58, 58, 58]
    assert contract["libraries"]["sklearn_required"] == "1.9.0"
    assert (
        contract["feature_engine"]["expert_8_formula_sha256"]
        == symbolic.EXPERT_FEATURE_FORMULA_SHA256
    )
    assert contract["nulls"]["order"] == [
        "REAL",
        "CIRCULAR_TARGET",
        "MATCHED_TARGET",
    ]
    benchmark = ml.ml_compute_feasibility_document()
    assert benchmark["row_count"] == 110_000
    assert benchmark["transformed_feature_count"] == 442
    assert benchmark["null_permutation_benchmark"]["row_count"] == 110_000
    assert benchmark["null_permutation_benchmark"]["matched_wall_milliseconds"] == 439
    assert benchmark["extrapolation"]["within_wall_cap"]
    assert benchmark["execution_path"] == "SEQUENTIAL_ONE_FIT_AT_A_TIME_NO_WORKER_DIVISOR"
    assert benchmark["extrapolation"]["sequential_ml_seconds"] == 77_681
    assert benchmark["extrapolation"]["total_campaign_projected_seconds"] == 162_962
    assert benchmark["extrapolation"]["within_memory_cap"]
    assert benchmark["extrapolation"]["worst_live_meta_training_matrix_count"] == 42
    assert benchmark["extrapolation"]["safety_adjusted_peak_resident_bytes"] == 30_563_016_864
    assert benchmark["cache_retention_benchmark"]["cache_peak_state_count"] == 21
    assert [item["search_fit_count"] for item in benchmark["workload_projection"]] == [
        756,
        756,
        756,
        756,
        1_008,
        1_008,
    ]
    assert benchmark["benchmark_sha256"] == ml.canonical_sha256(
        {key: value for key, value in benchmark.items() if key != "benchmark_sha256"}
    )
    assert ml.MLComputeFeasibilityEvidence.from_dict(benchmark).as_dict() == benchmark
    tampered_benchmark = copy.deepcopy(benchmark)
    tampered_benchmark["workload_projection"][0]["search_fit_count"] = 755
    with pytest.raises(ml.AllCasesMLError):
        ml.MLComputeFeasibilityEvidence.from_dict(tampered_benchmark)
    assert contract["compute_feasibility"] == benchmark
    assert 3 * 7 * (144 + 96) == contract["compute_caps"]["maximum_search_model_fits"]
    cache_schedule = ml.shared_fit_cache_schedule_document()
    assert cache_schedule["total_fit_count"] == 5_040
    assert cache_schedule["total_cache_hits"] == 5_040
    assert cache_schedule["direct"]["peak_state_count"] == 21
    assert cache_schedule["meta"]["final_state_count"] == 0
    assert contract["fit_recipe_sharing"]["schedule_evidence"] == cache_schedule


def test_feature_builder_uses_only_completed_bars_and_exact_subsets() -> None:
    rows, bars = _causal_rows()
    assert rows.values.shape == (50, 213)
    assert rows.feature_names == ml.FULL_MTF_213
    assert np.isfinite(rows.values).all()
    assert np.all(rows.atr_ticks_by_timeframe > 0)
    for timeframe_column, timeframe in enumerate(ml.TF_ORDER):
        aligned_ends = bars[timeframe].bar_end_ns[rows.aligned_bar_indexes[:, timeframe_column]]
        assert np.all(aligned_ends <= rows.decision_ns)
    assert rows.for_feature_set("PRICE_GEOMETRY_36").values.shape == (50, 36)
    assert rows.for_feature_set("FLOW_TIME_REGIME_90").values.shape == (50, 90)

    cutoff = int(rows.decision_ns[20])
    changed_bars = dict(bars)
    source = bars[3_600]
    close = np.array(source.close, copy=True)
    future = source.bar_end_ns > cutoff
    close[future] += 10_000.0
    high = np.maximum(np.array(source.high, copy=True), close + 0.05)
    changed_bars[3_600] = ml.CausalBarSeries(
        timeframe_seconds=3_600,
        bar_end_ns=source.bar_end_ns,
        open=source.open,
        high=high,
        low=source.low,
        close=close,
        volume=source.volume,
        trade_count=source.trade_count,
        source_dates=source.source_dates,
        contracts=source.contracts,
        outcome_span_ids=source.outcome_span_ids,
        segment_ids=source.segment_ids,
        stage_date_ranks=source.stage_date_ranks,
        stage_key=source.stage_key,
        buy_volume=source.buy_volume,
        sell_volume=source.sell_volume,
    )
    rebuilt = ml.build_causal_feature_rows(
        anchors=ml.CausalAnchorRows(
            row_ids=rows.row_ids,
            decision_ns=rows.decision_ns,
            entry_ns=rows.entry_ns,
            source_dates=rows.source_dates,
            contracts=rows.contracts,
            outcome_span_ids=rows.outcome_span_ids,
            segment_ids=rows.segment_ids,
            stage_date_ranks=rows.stage_date_ranks,
            stage_key=rows.stage_key,
            decision_timeframe_seconds=rows.decision_timeframe_seconds,
            entry_schedule_sha256=rows.entry_schedule_sha256,
        ),
        bars_by_timeframe=changed_bars,
    )
    assert np.array_equal(rows.values[:21], rebuilt.values[:21])
    assert not np.array_equal(rows.values[21:], rebuilt.values[21:])

    plus, _ = _causal_rows(plus_expert=True)
    assert plus.values.shape == (50, 221)
    assert plus.feature_names[-8:] == ml.EXPERT_FEATURE_NAMES
    assert np.array_equal(plus.for_feature_set("FULL_MTF_213").values, rows.values)
    assert plus.expert_formula_sha256 == symbolic.EXPERT_FEATURE_FORMULA_SHA256
    assert plus.expert_artifact_commitment_sha256 is not None
    assert plus.for_feature_set("FULL_MTF_213").expert_artifact_sha256s is None

    wrong_stage = {**bars, 300: replace(bars[300], stage_key="WF1")}
    anchors = ml.CausalAnchorRows(
        row_ids=rows.row_ids,
        decision_ns=rows.decision_ns,
        entry_ns=rows.entry_ns,
        source_dates=rows.source_dates,
        contracts=rows.contracts,
        outcome_span_ids=rows.outcome_span_ids,
        segment_ids=rows.segment_ids,
        stage_date_ranks=rows.stage_date_ranks,
        stage_key=rows.stage_key,
        decision_timeframe_seconds=rows.decision_timeframe_seconds,
        entry_schedule_sha256=rows.entry_schedule_sha256,
    )
    with pytest.raises(ml.AllCasesMLError, match="timeframe binding"):
        ml.build_causal_feature_rows(anchors=anchors, bars_by_timeframe=wrong_stage)
    with pytest.raises(ml.AllCasesMLError, match="typed causal symbolic"):
        ml.build_causal_feature_rows(
            anchors=anchors,
            bars_by_timeframe=bars,
            feature_set_id="FULL_MTF_PLUS_EXPERT_221",
            expert_artifacts=tuple(SimpleNamespace() for _ in anchors.row_ids),
        )


def test_outcome_free_sources_reject_float_times_and_boolean_lineage_before_commitment() -> None:
    bars = _bars(300, 60)
    for field_name, forged in (
        ("bar_end_ns", bars.bar_end_ns.astype(np.float64)),
        ("outcome_span_ids", np.ones(60, dtype=np.bool_)),
        ("segment_ids", np.ones(60, dtype=np.bool_)),
        ("stage_date_ranks", np.zeros(60, dtype=np.bool_)),
    ):
        with pytest.raises(ml.AllCasesMLError, match="exact integral"):
            replace(bars, **{field_name: forged})
    for field_name, forged in (
        ("open", np.ones(60, dtype=np.bool_)),
        ("volume", np.full(60, "1.0")),
    ):
        with pytest.raises(ml.AllCasesMLError, match="raw numeric"):
            replace(bars, **{field_name: forged})

    rows, _ = _causal_rows()
    anchor_kwargs = {
        "row_ids": rows.row_ids,
        "decision_ns": rows.decision_ns,
        "entry_ns": rows.entry_ns,
        "source_dates": rows.source_dates,
        "contracts": rows.contracts,
        "outcome_span_ids": rows.outcome_span_ids,
        "segment_ids": rows.segment_ids,
        "stage_date_ranks": rows.stage_date_ranks,
        "stage_key": rows.stage_key,
        "decision_timeframe_seconds": rows.decision_timeframe_seconds,
        "entry_schedule_sha256": rows.entry_schedule_sha256,
    }
    for field_name, forged in (
        ("decision_ns", rows.decision_ns.astype(np.float64)),
        ("entry_ns", rows.entry_ns.astype(np.float64)),
        ("outcome_span_ids", np.ones(rows.row_count, dtype=np.bool_)),
        ("segment_ids", np.ones(rows.row_count, dtype=np.bool_)),
        ("stage_date_ranks", np.zeros(rows.row_count, dtype=np.bool_)),
    ):
        with pytest.raises(ml.AllCasesMLError, match="exact integral"):
            ml.CausalAnchorRows(**{**anchor_kwargs, field_name: forged})
    for field_name, forged in (
        ("values", np.ones(rows.values.shape, dtype=np.bool_)),
        (
            "atr_ticks_by_timeframe",
            np.full(rows.atr_ticks_by_timeframe.shape, "100.0"),
        ),
    ):
        with pytest.raises(ml.AllCasesMLError, match="raw numeric"):
            replace(rows, **{field_name: forged})


def test_decision_cutoff_and_utc_features_ignore_later_scheduled_entry() -> None:
    bars = {
        timeframe: replace(
            _bars(timeframe, count),
            bar_end_ns=_bars(timeframe, count).bar_end_ns - 1_000_000_000,
        )
        for timeframe, count in ((300, 2_200), (1_800, 400), (3_600, 200))
    }
    decisions = np.arange(130, 180, dtype=np.int64) * 3_600 * 1_000_000_000 - 1_000_000_000
    entries = decisions + 1_000_000_000
    common = {
        "row_ids": tuple(f"cutoff-{index:03d}" for index in range(len(decisions))),
        "decision_ns": decisions,
        "source_dates": tuple(date(2024, 1, 1) for _ in decisions),
        "contracts": tuple("6E" for _ in decisions),
        "outcome_span_ids": np.ones(len(decisions), dtype=np.int64),
        "segment_ids": np.ones(len(decisions), dtype=np.int64),
        "stage_date_ranks": np.zeros(len(decisions), dtype=np.int64),
        "stage_key": "SEARCH",
        "decision_timeframe_seconds": 300,
    }
    first = ml.build_causal_feature_rows(
        anchors=ml.CausalAnchorRows(
            **common,
            entry_ns=entries,
            entry_schedule_sha256="a" * 64,
        ),
        bars_by_timeframe=bars,
    )
    later = ml.build_causal_feature_rows(
        anchors=ml.CausalAnchorRows(
            **common,
            entry_ns=entries + 120 * 1_000_000_000,
            entry_schedule_sha256="b" * 64,
        ),
        bars_by_timeframe=bars,
    )
    assert np.array_equal(first.aligned_bar_indexes, later.aligned_bar_indexes)
    assert np.array_equal(first.values, later.values)

    midnight_index = 144 - 130
    positions = {name: index for index, name in enumerate(first.feature_names)}
    assert first.entry_ns[midnight_index] // 86_400_000_000_000 == (
        first.decision_ns[midnight_index] // 86_400_000_000_000 + 1
    )
    assert first.values[midnight_index, positions["utc_4h_20"]] == 1.0
    assert first.values[midnight_index, positions["utc_4h_00"]] == 0.0
    decision_weekday = (int(first.decision_ns[midnight_index]) // 86_400_000_000_000 + 3) % 7
    assert (
        first.values[
            midnight_index,
            positions[f"utc_weekday_{decision_weekday}"],
        ]
        == 1.0
    )


def test_structural_lattice_preserves_raw_anchors_and_freezes_feature_only_subset() -> None:
    rows, bars = _causal_rows()
    anchors = ml.CausalAnchorRows(
        row_ids=rows.row_ids,
        decision_ns=rows.decision_ns,
        entry_ns=rows.entry_ns,
        source_dates=rows.source_dates,
        contracts=rows.contracts,
        outcome_span_ids=rows.outcome_span_ids,
        segment_ids=rows.segment_ids,
        stage_date_ranks=rows.stage_date_ranks,
        stage_key=rows.stage_key,
        decision_timeframe_seconds=rows.decision_timeframe_seconds,
        entry_schedule_sha256=rows.entry_schedule_sha256,
    )
    lattice = ml.build_structural_opportunity_lattice(anchors, bars[300])
    assert len(lattice.row_ids) == rows.row_count == 50
    assert 2 <= len(lattice.eligible_row_ids) < len(lattice.row_ids)
    assert lattice == ml.build_structural_opportunity_lattice(anchors, bars[300])
    eligible = ml.apply_structural_opportunity_lattice(anchors, lattice)
    assert eligible.row_ids == lattice.eligible_row_ids
    assert lattice.artifact_sha256 == ml.canonical_sha256(lattice.definition_dict())


def test_direct_oos_schedule_preserves_decision_entry_and_lattice_proofs() -> None:
    rows, bars = _causal_rows()
    wf_rows = replace(rows, stage_key="WF1")
    anchors = ml.CausalAnchorRows(
        row_ids=wf_rows.row_ids,
        decision_ns=wf_rows.decision_ns,
        entry_ns=wf_rows.entry_ns,
        source_dates=wf_rows.source_dates,
        contracts=wf_rows.contracts,
        outcome_span_ids=wf_rows.outcome_span_ids,
        segment_ids=wf_rows.segment_ids,
        stage_date_ranks=wf_rows.stage_date_ranks,
        stage_key="WF1",
        decision_timeframe_seconds=wf_rows.decision_timeframe_seconds,
        entry_schedule_sha256=wf_rows.entry_schedule_sha256,
    )
    lattice = ml.build_structural_opportunity_lattice(
        anchors,
        replace(bars[300], stage_key="WF1"),
    )
    eligible_indexes = tuple(index for index, eligible in enumerate(lattice.eligible) if eligible)
    direct = _direct_candidate()
    eligible_rows = wf_rows.take(eligible_indexes).for_feature_set(direct.feature_set_id)
    signal_dates = tuple(sorted(set(wf_rows.source_dates)))
    partition_dates = (*signal_dates, signal_dates[-1] + timedelta(days=1))
    date_certificate = ml.build_stage_partition_date_certificate(
        "WF1",
        partition_dates,
        upstream_plan_sha256=ml.stage_partition_date_plan_sha256("WF1", partition_dates),
    )
    direct_schedule = ml.build_direct_outcome_free_execution_schedule(
        direct,
        eligible_rows,
        partition_key="WF1",
        partition_date_certificate=date_certificate,
        outcome_lineage_sha256="c" * 64,
        opportunity_lattice=lattice,
    )
    assert direct_schedule.decision_ns != direct_schedule.entry_ns
    assert direct_schedule.entry_schedule_sha256 == eligible_rows.entry_schedule_sha256
    assert direct_schedule.opportunity_lattice_sha256 == lattice.artifact_sha256
    assert all(
        exit_ns - entry_ns == direct.horizon_seconds * 1_000_000_000
        for entry_ns, exit_ns in zip(
            direct_schedule.entry_ns,
            direct_schedule.planned_exit_ns,
            strict=True,
        )
    )
    assert direct_schedule.feature_rows_sha256 == eligible_rows.artifact_sha256
    mismatched_contracts = replace(
        eligible_rows,
        contracts=("M6E", *eligible_rows.contracts[1:]),
    )
    with pytest.raises(ml.AllCasesMLError, match="feature/lattice row identity"):
        ml.build_direct_outcome_free_execution_schedule(
            direct,
            mismatched_contracts,
            partition_key="WF1",
            partition_date_certificate=date_certificate,
            outcome_lineage_sha256="c" * 64,
            opportunity_lattice=lattice,
        )
    with pytest.raises(ml.AllCasesMLError, match="date certificate"):
        ml.build_stage_partition_date_certificate(
            "WF1",
            partition_dates[:1],
            upstream_plan_sha256=date_certificate.upstream_plan_sha256,
        )


def test_causal_feature_artifact_binds_exact_values_atr_and_lineage_to_direct_freeze() -> None:
    matrix = _matrix(row_count=180)
    candidate = _direct_candidate()
    model = ml.fit_direct_model(candidate, matrix)
    rows = _direct_feature_rows(matrix, 20)
    schedule = _schedule(matrix, 20)
    assert schedule.feature_rows_sha256 == rows.artifact_sha256
    with pytest.raises(ValueError):
        rows.values.setflags(write=True)
    mask = ml.freeze_prediction_mask(
        model,
        rows,
        partition_key="WF1",
        execution_schedule=schedule,
    )
    assert mask.execution_schedule == schedule

    variants = (
        replace(rows, values=rows.values[::-1]),
        replace(rows, values=np.zeros_like(rows.values)),
        replace(rows, atr_ticks_by_timeframe=rows.atr_ticks_by_timeframe * 10.0),
        replace(rows, contracts=("M6E", *rows.contracts[1:])),
        replace(
            rows,
            outcome_span_ids=np.asarray(rows.outcome_span_ids, dtype=np.int64) + 1,
        ),
        replace(rows, segment_ids=np.asarray(rows.segment_ids, dtype=np.int64) + 1),
        replace(
            rows,
            decision_ns=np.asarray(rows.decision_ns, dtype=np.int64) - 1_000_000_000,
        ),
        replace(rows, entry_ns=np.asarray(rows.entry_ns, dtype=np.int64) + 1_000_000_000),
        replace(
            rows,
            source_dates=(rows.source_dates[0] + timedelta(days=1), *rows.source_dates[1:]),
        ),
        replace(
            rows,
            retained_input_indexes=np.asarray(rows.retained_input_indexes, dtype=np.int64) + 1,
        ),
        replace(
            rows,
            aligned_bar_indexes=np.asarray(rows.aligned_bar_indexes, dtype=np.int64) + 1,
        ),
        replace(rows, entry_schedule_sha256="c" * 64),
        replace(rows, source_commitment_sha256="c" * 64),
    )
    assert all(item.artifact_sha256 != rows.artifact_sha256 for item in variants)
    for altered in variants:
        with pytest.raises(ml.AllCasesMLError, match="feature rows"):
            ml.freeze_prediction_mask(
                model,
                altered,
                partition_key="WF1",
                execution_schedule=schedule,
            )
    document = mask.as_dict()
    document["prediction_input_sha256"] = "z" * 64
    with pytest.raises(ml.AllCasesMLError, match="binding"):
        ml.FrozenPredictionMask.from_dict(document)


def test_match_strata_are_fixed_outcome_free_and_used_by_matrix_builder() -> None:
    rows, _ = _causal_rows()
    strata = ml.build_match_strata(rows)
    assert strata == ml.build_match_strata(rows)
    assert len(strata) == rows.row_count
    assert all(value.startswith("tf0300_utc4h_") and value.count("|") == 2 for value in strata)


def test_lineage_breaks_reset_features_and_adjacent_weekend_bridge_is_bounded() -> None:
    base = _bars(3_600, 120)
    segments = np.ones(120, dtype=np.int64)
    segments[60:] = 2
    segment_break = replace(base, segment_ids=segments)
    assert segment_break._run_length_by_index[59] == 60
    assert segment_break._run_length_by_index[60] == 1
    offsets = np.zeros(120)
    offsets[:60] = 1_000.0
    changed_before_break = replace(
        segment_break,
        open=segment_break.open + offsets,
        high=segment_break.high + offsets,
        low=segment_break.low + offsets,
        close=segment_break.close + offsets,
    )
    original_columns, original_atr = ml._timeframe_feature_columns(segment_break)
    changed_columns, changed_atr = ml._timeframe_feature_columns(changed_before_break)
    assert np.array_equal(original_atr[109:], changed_atr[109:])
    assert all(
        np.array_equal(original_columns[name][109:], changed_columns[name][109:])
        for name in original_columns
    )

    contracts = tuple("6E" if index < 60 else "6M" for index in range(120))
    contract_roll = replace(base, contracts=contracts)
    assert contract_roll._run_length_by_index[59] == 60
    assert contract_roll._run_length_by_index[60] == 1

    friday = date(2024, 1, 5)
    monday = date(2024, 1, 8)
    source_dates = tuple(friday if index < 60 else monday for index in range(120))
    ranks = np.where(np.arange(120) < 60, 0, 1)
    weekend_ends = np.array(base.bar_end_ns, copy=True)
    weekend_ends[60:] += 72 * 3_600 * 1_000_000_000
    weekend = replace(
        base,
        bar_end_ns=weekend_ends,
        source_dates=source_dates,
        stage_date_ranks=ranks,
    )
    assert weekend._continuity_by_index[60]
    assert weekend._run_length_by_index[60] == 61

    long_gap_ends = np.array(base.bar_end_ns, copy=True)
    long_gap_ends[60:] += 97 * 3_600 * 1_000_000_000
    long_gap = replace(
        base,
        bar_end_ns=long_gap_ends,
        source_dates=source_dates,
        stage_date_ranks=ranks,
    )
    assert not long_gap._continuity_by_index[60]
    assert long_gap._run_length_by_index[60] == 1


def _duplicate_timestamp_lineages(source: ml.CausalBarSeries) -> ml.CausalBarSeries:
    count = len(source.close)
    repeated = np.repeat(np.arange(count), 2)
    offsets = np.tile((0.0, 10.0), count)
    return ml.CausalBarSeries(
        timeframe_seconds=source.timeframe_seconds,
        bar_end_ns=source.bar_end_ns[repeated],
        open=source.open[repeated] + offsets,
        high=source.high[repeated] + offsets,
        low=source.low[repeated] + offsets,
        close=source.close[repeated] + offsets,
        volume=source.volume[repeated],
        trade_count=source.trade_count[repeated],
        source_dates=tuple(source.source_dates[index] for index in repeated),
        contracts=tuple(value for _ in range(count) for value in ("6E", "6J")),
        outcome_span_ids=np.tile((1, 2), count),
        segment_ids=np.ones(count * 2, dtype=np.int64),
        stage_date_ranks=source.stage_date_ranks[repeated],
        stage_key=source.stage_key,
        buy_volume=source.buy_volume[repeated],
        sell_volume=source.sell_volume[repeated],
    )


def test_same_timestamp_lineages_never_cross_join() -> None:
    bars = {
        300: _duplicate_timestamp_lineages(_bars(300, 800)),
        1_800: _duplicate_timestamp_lineages(_bars(1_800, 130)),
        3_600: _duplicate_timestamp_lineages(_bars(3_600, 65)),
    }
    entries = np.arange(55, 65, dtype=np.int64) * 3_600 * 1_000_000_000
    anchors = ml.CausalAnchorRows(
        row_ids=tuple(f"same-time-{index}" for index in range(len(entries))),
        decision_ns=entries,
        entry_ns=entries,
        source_dates=tuple(date(2024, 1, 1) for _ in entries),
        contracts=tuple("6E" for _ in entries),
        outcome_span_ids=np.ones(len(entries), dtype=np.int64),
        segment_ids=np.ones(len(entries), dtype=np.int64),
        stage_date_ranks=np.zeros(len(entries), dtype=np.int64),
        stage_key="SEARCH",
        decision_timeframe_seconds=300,
        entry_schedule_sha256="a" * 64,
    )
    rows = ml.build_causal_feature_rows(anchors=anchors, bars_by_timeframe=bars)
    assert rows.row_count == len(entries)
    for timeframe_column, timeframe in enumerate(ml.TF_ORDER):
        assert all(
            bars[timeframe].contracts[index] == "6E"
            for index in rows.aligned_bar_indexes[:, timeframe_column]
        )
        assert all(
            bars[timeframe].outcome_span_ids[index] == 1
            for index in rows.aligned_bar_indexes[:, timeframe_column]
        )


def test_outcome_builders_bind_direct_and_meta_training_rows() -> None:
    rows, _ = _causal_rows(plus_expert=True)
    count = rows.row_count
    direct_candidate = _direct_candidate(
        "ENET_A",
        feature_set="FLOW_TIME_REGIME_90",
        timeframe=300,
        horizon=3_600,
    )
    entry_ticks = np.arange(count, dtype=np.int64) + 2_000_000
    terminal_move_ticks = np.arange(count, dtype=np.int64) - count // 2
    terminal_ticks = entry_ticks + terminal_move_ticks
    exits = rows.entry_ns + direct_candidate.horizon_seconds * 1_000_000_000
    valid = np.ones(count, dtype=np.bool_)
    direct_inputs = {
        "fill_ns": rows.entry_ns,
        "label_exit_ns": exits,
        "entry_ticks": entry_ticks,
        "terminal_ticks": terminal_ticks,
        "outcome_contracts": rows.contracts,
        "outcome_span_ids": rows.outcome_span_ids,
        "segment_ids": rows.segment_ids,
        "valid_label_paths": valid,
        "outcome_lineage_sha256": "c" * 64,
        "opportunity_lattice_sha256": "e" * 64,
    }
    direct = ml.build_direct_training_matrix(
        direct_candidate,
        rows,
        **direct_inputs,
    )
    eligible = np.flatnonzero(valid)
    assert np.allclose(direct.targets, terminal_move_ticks[eligible] / direct.atr_ticks)
    assert np.array_equal(direct.terminal_move_ticks, terminal_move_ticks)
    assert np.array_equal(
        direct.label_exit_ns,
        exits[eligible],
    )
    assert direct.values.shape == (count, 90)
    assert np.array_equal(direct.outcome_span_ids, rows.outcome_span_ids[eligible])
    assert direct.outcome_lineage_sha256 == "c" * 64
    assert direct.match_strata == ml.build_match_strata(rows.for_feature_set("FLOW_TIME_REGIME_90"))
    assert not hasattr(ml, "build_direct_terminal_targets")
    for field_name, forged in (
        ("fill_ns", rows.entry_ns.astype(np.float64)),
        ("label_exit_ns", exits.astype(np.float64)),
        ("outcome_span_ids", np.ones(count, dtype=np.bool_)),
        ("segment_ids", np.ones(count, dtype=np.bool_)),
    ):
        with pytest.raises(ml.AllCasesMLError, match="exact integral"):
            ml.build_direct_training_matrix(
                direct_candidate,
                rows,
                **{**direct_inputs, field_name: forged},
            )

    missing = np.array(valid, copy=True)
    missing[1] = False
    with pytest.raises(ml.AllCasesMLError, match="opportunity lattice"):
        ml.build_direct_training_matrix(
            direct_candidate,
            rows,
            fill_ns=rows.entry_ns,
            label_exit_ns=exits,
            entry_ticks=entry_ticks,
            terminal_ticks=terminal_ticks,
            outcome_contracts=rows.contracts,
            outcome_span_ids=rows.outcome_span_ids,
            segment_ids=rows.segment_ids,
            valid_label_paths=missing,
            outcome_lineage_sha256="c" * 64,
            opportunity_lattice_sha256="e" * 64,
        )

    shortened = np.array(exits, copy=True)
    shortened[2] -= 1
    with pytest.raises(ml.AllCasesMLError, match="shortened"):
        ml.build_direct_training_matrix(
            direct_candidate,
            rows,
            fill_ns=rows.entry_ns,
            label_exit_ns=shortened,
            entry_ticks=entry_ticks,
            terminal_ticks=terminal_ticks,
            outcome_contracts=rows.contracts,
            outcome_span_ids=rows.outcome_span_ids,
            segment_ids=rows.segment_ids,
            valid_label_paths=valid,
            outcome_lineage_sha256="c" * 64,
            opportunity_lattice_sha256="e" * 64,
        )
    wrong_spans = np.array(rows.outcome_span_ids, copy=True)
    wrong_spans[2] += 1
    with pytest.raises(ml.AllCasesMLError, match="cross-lineage"):
        ml.build_direct_training_matrix(
            direct_candidate,
            rows,
            fill_ns=rows.entry_ns,
            label_exit_ns=exits,
            entry_ticks=entry_ticks,
            terminal_ticks=terminal_ticks,
            outcome_contracts=rows.contracts,
            outcome_span_ids=wrong_spans,
            segment_ids=rows.segment_ids,
            valid_label_paths=valid,
            outcome_lineage_sha256="c" * 64,
            opportunity_lattice_sha256="e" * 64,
        )

    meta_candidate = _meta_candidate(feature_set="FULL_MTF_PLUS_EXPERT_221")
    meta_rows, _orders, order_batch, recipe, experts, policy = _symbolic_orders_for_feature_rows(
        rows, tuple(range(count))
    )
    base_indexes = tuple(range(0, count, 2))
    net_ticks = np.arange(count, dtype=np.int64) - count // 2
    exits = meta_rows.entry_ns + 7_200 * 1_000_000_000
    certificate = _ranking_certificate(
        training_dates=tuple(sorted(set(meta_rows.source_dates))),
        base_trigger_family=_expert_base_family(experts),
        first_recipe=recipe,
        first_policy=policy,
    )
    directions = np.where(np.arange(count) % 3, 1, -1).astype(np.int8)
    meta_inputs = {
        "base_row_indexes": base_indexes,
        "base_entry_ns": meta_rows.entry_ns,
        "fully_loaded_net_ticks": net_ticks,
        "base_directions": directions,
        "atr_ticks": np.full(count, 100.0),
        "label_exit_ns": exits,
        "outcome_contracts": meta_rows.contracts,
        "outcome_span_ids": meta_rows.outcome_span_ids,
        "segment_ids": meta_rows.segment_ids,
        "valid_label_paths": np.ones(count, dtype=np.bool_),
        "outcome_lineage_sha256": "d" * 64,
        "opportunity_lattice_sha256": "e" * 64,
        "symbolic_ranking_certificate": certificate,
        "strategy_recipe": recipe,
        "base_order_batch": order_batch,
        "expert_artifacts": experts,
    }
    meta = ml.build_meta_training_matrix(
        meta_candidate,
        meta_rows,
        **meta_inputs,
    )
    assert meta.values.shape == (len(base_indexes), 221)
    assert np.array_equal(meta.targets, (net_ticks[list(base_indexes)] > 0).astype(float))
    assert meta.base_strategy_id == recipe.strategy_id
    for field_name, forged in (
        ("base_entry_ns", meta_rows.entry_ns.astype(np.float64)),
        ("label_exit_ns", exits.astype(np.float64)),
        ("outcome_span_ids", np.ones(count, dtype=np.bool_)),
        ("segment_ids", np.ones(count, dtype=np.bool_)),
        ("base_directions", directions.astype(np.float64)),
        ("base_directions", directions.astype(np.bool_)),
    ):
        with pytest.raises(ml.AllCasesMLError, match="exact integral"):
            ml.build_meta_training_matrix(
                meta_candidate,
                meta_rows,
                **{**meta_inputs, field_name: forged},
            )
    for forged_atr in (
        np.ones(count, dtype=np.bool_),
        np.full(count, "100.0"),
    ):
        with pytest.raises(ml.AllCasesMLError, match="raw numeric"):
            ml.build_meta_training_matrix(
                meta_candidate,
                meta_rows,
                **{**meta_inputs, "atr_ticks": forged_atr},
            )
    with pytest.raises(ml.AllCasesMLError, match="policy or catalog recipe"):
        replace(certificate.ranked_strategies[0], trigger_family="NOT_THE_EXPERT_BASE_FAMILY")
    wrong_batch_definition = {
        "anchor_policy_id": "b" * 64,
        "orders": [item.as_dict() for item in order_batch.orders],
        "schema": symbolic.PATH_OUTCOME_SCHEMA,
    }
    wrong_batch = symbolic.EntryOrderBatch(
        "b" * 64,
        order_batch.orders,
        symbolic.canonical_sha256(wrong_batch_definition),
    )
    with pytest.raises(ml.AllCasesMLError, match="recipe, orders, or Expert-8"):
        ml.build_meta_training_matrix(
            meta_candidate,
            meta_rows,
            base_row_indexes=base_indexes,
            base_entry_ns=meta_rows.entry_ns,
            fully_loaded_net_ticks=net_ticks,
            base_directions=np.where(np.arange(count) % 3, 1, -1),
            atr_ticks=np.full(count, 100.0),
            label_exit_ns=exits,
            outcome_contracts=meta_rows.contracts,
            outcome_span_ids=meta_rows.outcome_span_ids,
            segment_ids=meta_rows.segment_ids,
            valid_label_paths=np.ones(count, dtype=np.bool_),
            outcome_lineage_sha256="d" * 64,
            opportunity_lattice_sha256="e" * 64,
            symbolic_ranking_certificate=certificate,
            strategy_recipe=recipe,
            base_order_batch=wrong_batch,
            expert_artifacts=experts,
        )


def test_exact_tick_economics_never_reconstruct_from_normalized_float_targets() -> None:
    matrix = _matrix(row_count=12)
    result = _crossfit_fixture(
        "REAL",
        admitted_indexes=(0, 4),
        long_indexes=(0,),
        score_offset=1.0,
    )
    matrix = _matrix_bound_to_crossfit(result, matrix)
    resolved = ml.direct_crossfit_realized_net_ticks(result, matrix)
    assert resolved[0] == int(matrix.terminal_move_ticks[0]) - 14
    assert resolved[4] == -int(matrix.terminal_move_ticks[4]) - 14
    assert np.count_nonzero(resolved) == 2
    perturbed = replace(matrix, targets=np.linspace(-1_000_000.25, 1_000_000.75, 12))
    with pytest.raises(ml.AllCasesMLError, match="outcome matrix differs"):
        ml.direct_crossfit_realized_net_ticks(result, perturbed)
    wrong_ticks = np.array(matrix.terminal_move_ticks, copy=True)
    wrong_ticks[0] += 1_000
    with pytest.raises(ml.AllCasesMLError, match="outcome matrix differs"):
        ml.direct_crossfit_realized_net_ticks(
            result,
            replace(
                matrix,
                terminal_move_ticks=wrong_ticks,
                targets=wrong_ticks.astype(np.float64) / matrix.atr_ticks,
            ),
        )
    with pytest.raises(ml.AllCasesMLError, match="outcome matrix differs"):
        ml.direct_crossfit_realized_net_ticks(
            result,
            replace(
                perturbed,
                task_horizon_seconds=10_800,
                terminal_move_ticks=perturbed.terminal_move_ticks + 1_000,
            ),
        )
    with pytest.raises(ml.AllCasesMLError, match="outcome matrix differs"):
        ml.direct_crossfit_realized_net_ticks(
            result,
            replace(
                perturbed,
                row_ids=("wrong-row", *perturbed.row_ids[1:]),
            ),
        )

    with pytest.raises(ml.AllCasesMLError, match="exact integral"):
        replace(matrix, terminal_move_ticks=matrix.terminal_move_ticks.astype(np.float64))
    for field_name, forged in (
        ("decision_ns", matrix.decision_ns.astype(np.float64)),
        ("entry_ns", matrix.entry_ns.astype(np.float64)),
        ("label_exit_ns", matrix.label_exit_ns.astype(np.float64)),
        ("outcome_span_ids", matrix.outcome_span_ids.astype(np.bool_)),
        ("segment_ids", matrix.segment_ids.astype(np.bool_)),
    ):
        with pytest.raises(ml.AllCasesMLError, match="exact integral"):
            replace(matrix, **{field_name: forged})
    for field_name, forged in (
        ("values", np.ones(matrix.values.shape, dtype=np.bool_)),
        ("targets", np.full(matrix.targets.shape, "0.5")),
        ("atr_ticks", np.ones(matrix.atr_ticks.shape, dtype=np.bool_)),
    ):
        with pytest.raises(ml.AllCasesMLError, match="raw numeric"):
            replace(matrix, **{field_name: forged})
    meta = _matrix(row_count=12, feature_set="FULL_MTF_213", meta=True)
    with pytest.raises(ml.AllCasesMLError, match="exact integral"):
        replace(meta, realized_net_ticks=meta.realized_net_ticks.astype(np.float64))
    with pytest.raises(ml.AllCasesMLError, match="exact integral"):
        replace(meta, base_directions=meta.base_directions.astype(np.float64))


def test_search_blocks_and_label_overlap_purge_are_exact() -> None:
    dates = tuple(date(2020, 1, 1) + timedelta(days=index) for index in range(469))
    plan = ml.build_search_block_plan(dates)
    assert tuple(len(block) for block in plan.blocks) == ml.SEARCH_BLOCK_SIZES
    assert tuple(fold.fold_key for fold in plan.outer_folds) == ml.SEARCH_OUTER_FOLD_KEYS
    assert plan == ml.build_search_block_plan(dates)
    assert plan.sha256 == ml.build_search_block_plan(dates).sha256

    matrix = _matrix(row_count=469, one_date_per_row=True)
    exits = np.array(matrix.label_exit_ns, copy=True)
    first_b3_entry = int(matrix.entry_ns[118])
    exits[117] = first_b3_entry
    matrix = replace(matrix, label_exit_ns=exits)
    rows = ml.purged_fold_rows(matrix, plan.outer_folds[0])
    assert rows.validation_indexes == tuple(range(118, 177))
    assert rows.training_indexes == tuple(range(117))
    assert rows.purged_training_count == 1
    assert all(matrix.label_exit_ns[index] < first_b3_entry for index in rows.training_indexes)

    with pytest.raises(ml.AllCasesMLError, match="469 unique"):
        ml.build_search_block_plan(dates[:-1])


def test_post_purge_zero_or_one_training_row_is_candidate_local() -> None:
    dates = tuple(date(2020, 1, 1) + timedelta(days=index) for index in range(469))
    plan = ml.build_search_block_plan(dates)
    matrix = _matrix(row_count=469, one_date_per_row=True)
    fold = plan.outer_folds[0]
    validation_indexes = tuple(
        index for index, value in enumerate(matrix.decision_dates) if value in fold.validation_dates
    )
    first_validation_entry = min(int(matrix.entry_ns[index]) for index in validation_indexes)
    exits = np.array(matrix.label_exit_ns, copy=True)
    training_indexes = tuple(
        index for index, value in enumerate(matrix.decision_dates) if value in fold.training_dates
    )
    for index in training_indexes[1:]:
        exits[index] = first_validation_entry
    sparse = replace(matrix, label_exit_ns=exits)
    with pytest.raises(ml.MLCandidateIneligible) as raised:
        ml.purged_fold_rows(sparse, fold)
    assert raised.value.reason is ml.MLIneligibilityReason.INSUFFICIENT_FOLD_ROWS
    assert raised.value.scope_key == "B3"


def test_null_target_maps_are_training_only_bijections_and_deterministic() -> None:
    matrix = _matrix(row_count=42)
    assert len(set(matrix.outcome_span_ids.tolist())) < matrix.row_count
    training = tuple(range(36))
    candidate = _direct_candidate()
    real = ml.target_permutation_indexes(
        matrix,
        training,
        world=ml.NullWorld.REAL,
        candidate_id=candidate.candidate_id,
        fold_key="B3",
    )
    circular = ml.target_permutation_indexes(
        matrix,
        training,
        world=ml.NullWorld.CIRCULAR_TARGET,
        candidate_id=candidate.candidate_id,
        fold_key="B3",
    )
    matched = ml.target_permutation_indexes(
        matrix,
        training,
        world=ml.NullWorld.MATCHED_TARGET,
        candidate_id=candidate.candidate_id,
        fold_key="B3",
    )
    assert real == training
    matched_plan = ml.target_permutation_plan(
        matrix,
        training,
        world="MATCHED_TARGET",
        candidate_id=candidate.candidate_id,
        fold_key="B3",
    )
    assert matched_plan.source_indexes == matched
    assert (
        matched_plan.exact_stratum_count
        + matched_plan.coarse_stratum_count
        + matched_plan.same_contract_fallback_count
        == len(training)
    )
    assert circular == ml.target_permutation_indexes(
        matrix,
        training,
        world="CIRCULAR_TARGET",
        candidate_id=candidate.candidate_id,
        fold_key="B3",
    )
    assert set(circular) == set(training) == set(matched)
    assert circular != matched
    for sources in (circular, matched):
        assert all(
            destination != source for destination, source in zip(training, sources, strict=True)
        )
        assert all(
            matrix.outcome_span_ids[destination] != matrix.outcome_span_ids[source]
            for destination, source in zip(training, sources, strict=True)
        )
        assert not set(sources).intersection(range(36, 42))
    for destination, source in zip(training, matched, strict=True):
        assert matrix.contracts[destination] == matrix.contracts[source]

    singleton_exact_strata = replace(
        matrix,
        contracts=tuple("6E" for _ in range(matrix.row_count)),
        match_strata=tuple(f"regime|unique-{index}" for index in range(matrix.row_count)),
    )
    relaxed = ml.target_permutation_plan(
        singleton_exact_strata,
        training,
        world="MATCHED_TARGET",
        candidate_id=candidate.candidate_id,
        fold_key="B3",
    )
    assert relaxed.exact_stratum_count == 0
    assert relaxed.coarse_stratum_count == len(training)
    assert set(relaxed.source_indexes) == set(training)

    changed_validation = np.array(matrix.targets, copy=True)
    changed_validation[36:] += 1_000_000
    mutated = replace(matrix, targets=changed_validation)
    assert np.array_equal(
        ml.permuted_training_targets(
            matrix,
            training,
            world="MATCHED_TARGET",
            candidate_id=candidate.candidate_id,
            fold_key="B3",
        ),
        ml.permuted_training_targets(
            mutated,
            training,
            world="MATCHED_TARGET",
            candidate_id=candidate.candidate_id,
            fold_key="B3",
        ),
    )


@pytest.mark.parametrize("indexes", ([False, True], [0.9, 1.9], ["0", "1"]))
def test_target_permutation_indexes_reject_coercible_non_integer_indexes(
    indexes: list[object],
) -> None:
    with pytest.raises(ml.AllCasesMLError, match="exact integers"):
        ml.target_permutation_indexes(
            _matrix(row_count=20),
            indexes,
            world="REAL",
            candidate_id=_direct_candidate().candidate_id,
            fold_key="SEARCH_FINAL",
        )


def test_span_derangements_handle_large_uneven_feasible_blocks_without_recursion() -> None:
    matrix = _matrix(row_count=1_100)
    spans = np.asarray([1] * 550 + [2] * 330 + [3] * 220, dtype=np.int64)
    matrix = replace(
        matrix,
        contracts=("6E",) * matrix.row_count,
        outcome_span_ids=spans,
        match_strata=tuple(f"utc|regime-{index % 7}" for index in range(matrix.row_count)),
    )
    indexes = tuple(range(matrix.row_count))
    circular = ml.target_permutation_indexes(
        matrix,
        indexes,
        world="CIRCULAR_TARGET",
        candidate_id=_direct_candidate().candidate_id,
        fold_key="SEARCH_FINAL",
    )
    matched = ml.target_permutation_indexes(
        matrix,
        indexes,
        world="MATCHED_TARGET",
        candidate_id=_direct_candidate().candidate_id,
        fold_key="SEARCH_FINAL",
    )
    assert set(circular) == set(matched) == set(indexes)
    assert circular != matched
    assert all(spans[destination] != spans[source] for destination, source in enumerate(circular))
    assert all(spans[destination] != spans[source] for destination, source in enumerate(matched))


def test_matched_null_finds_distinct_feasible_bijection_or_fails_candidate_local() -> None:
    matrix = replace(
        _matrix(row_count=4),
        contracts=("6E",) * 4,
        outcome_span_ids=np.asarray((1, 1, 2, 2), dtype=np.int64),
        match_strata=("same",) * 4,
    )
    indexes = tuple(range(4))
    candidate_id = _direct_candidate().candidate_id
    circular = ml.target_permutation_indexes(
        matrix,
        indexes,
        world="CIRCULAR_TARGET",
        candidate_id=candidate_id,
        fold_key="SEARCH_FINAL",
    )
    matched = ml.target_permutation_indexes(
        matrix,
        indexes,
        world="MATCHED_TARGET",
        candidate_id=candidate_id,
        fold_key="SEARCH_FINAL",
    )
    assert matched != circular
    assert set(matched) == set(indexes)
    assert all(
        matrix.outcome_span_ids[destination] != matrix.outcome_span_ids[source]
        for destination, source in zip(indexes, matched, strict=True)
    )

    impossible = replace(
        matrix,
        outcome_span_ids=np.asarray((1, 2, 3, 4), dtype=np.int64),
    )
    with pytest.raises(ml.MLCandidateIneligible) as raised:
        ml.target_permutation_indexes(
            impossible,
            (0, 1),
            world="MATCHED_TARGET",
            candidate_id=candidate_id,
            fold_key="SEARCH_FINAL",
        )
    assert raised.value.reason is ml.MLIneligibilityReason.NULL_DERANGEMENT_INFEASIBLE


def test_direct_linear_model_is_deterministic_portable_and_train_only() -> None:
    matrix = _matrix(row_count=180)
    candidate = _direct_candidate("ENET_A")
    first = ml.fit_direct_model(candidate, matrix)
    second = ml.fit_direct_model(candidate, matrix)
    reopened = ml.CanonicalMLModel.from_canonical_bytes(first.canonical_bytes)
    assert first.canonical_bytes == second.canonical_bytes
    assert first.sha256 == reopened.sha256
    assert first.preprocessor.medians[0] == pytest.approx(
        np.nanmedian(matrix.values[:, 0]), abs=0.0
    )
    assert first.preprocessor.missing_indicator_indexes == (0,)
    assert np.array_equal(
        first.predict_scores(matrix.values), reopened.predict_scores(matrix.values)
    )
    assert json.loads(first.canonical_bytes)["artifact_encoding"] == (
        "CANONICAL_JSON_FLOAT_HEX_NO_PICKLE"
    )

    feature_rows = _direct_feature_rows(matrix, 20)
    predictions = ml.predict_actions(
        reopened,
        feature_rows.values,
        row_ids=matrix.row_ids[:20],
        atr_ticks=feature_rows.atr_ticks_by_timeframe[:, 0],
    )
    assert len(predictions.scores) == 20
    assert all(
        not admitted or edge is not None and edge > 0
        for admitted, edge in zip(
            predictions.admitted,
            predictions.estimated_net_edge_ticks,
            strict=True,
        )
    )
    assert predictions.as_dict()["schema"] == ml.PREDICTION_SCHEMA
    frozen = ml.freeze_prediction_mask(
        reopened,
        feature_rows,
        partition_key="WF1",
        execution_schedule=_schedule(matrix, 20),
    )
    assert frozen.predictions == ml.apply_nonoverlap_occupancy(
        predictions,
        entry_ns=matrix.entry_ns[:20],
        planned_exit_ns=np.asarray(_schedule(matrix, 20).planned_exit_ns, dtype=np.int64),
        contracts=matrix.contracts[:20],
    )
    assert frozen.as_dict()["schema"] == ml.FROZEN_MASK_SCHEMA
    assert ml.OutcomeFreeExecutionSchedule.from_dict(_schedule(matrix, 20).as_dict()) == _schedule(
        matrix, 20
    )
    assert ml.FrozenPredictionMask.from_dict(frozen.as_dict()) == frozen
    assert ml.PredictionBatch.from_dict(predictions.as_dict()) == predictions
    assert frozen == ml.freeze_prediction_mask(
        reopened,
        feature_rows,
        partition_key="WF1",
        execution_schedule=_schedule(matrix, 20),
    )
    outer_model = ml.fit_direct_model(candidate, matrix, fold_key="B3")
    with pytest.raises(ml.AllCasesMLError, match="SEARCH_FINAL"):
        ml.freeze_prediction_mask(
            outer_model,
            feature_rows,
            partition_key="WF1",
            execution_schedule=_schedule(matrix, 20),
        )

    document = json.loads(first.canonical_bytes)
    document["candidate"]["learner_id"] = "HGB_7"
    tampered = ml.canonical_json_bytes(document)
    with pytest.raises(ml.AllCasesMLError, match="candidate binding"):
        ml.CanonicalMLModel.from_canonical_bytes(tampered)
    with pytest.raises(ml.AllCasesMLError, match="canonical schema"):
        ml.CanonicalMLModel.from_canonical_bytes(first.canonical_bytes + b" ")

    for key, replacement, message in (
        ("model_id", "0" * 64, "derived identity"),
        ("random_state", first.random_state + 1, "derived random state"),
        ("numpy_version", "0.0.0", "runtime version"),
        ("fold_key", "HACK", "world/fold"),
    ):
        changed = json.loads(first.canonical_bytes)
        changed[key] = replacement
        with pytest.raises(ml.AllCasesMLError, match=message):
            ml.CanonicalMLModel.from_canonical_bytes(ml.canonical_json_bytes(changed))

    hgb = ml.fit_direct_model(_direct_candidate("HGB_7"), matrix)
    transplanted = json.loads(first.canonical_bytes)
    transplanted["predictor"] = json.loads(hgb.canonical_bytes)["predictor"]
    with pytest.raises(ml.AllCasesMLError, match="learner-recipe"):
        ml.CanonicalMLModel.from_canonical_bytes(ml.canonical_json_bytes(transplanted))
    with pytest.raises(ml.AllCasesMLError, match="OOS feature rows"):
        ml.freeze_prediction_mask(
            reopened,
            feature_rows,
            partition_key="WF1",
            execution_schedule=_schedule(matrix, 20, stage_key="WF2"),
        )


def test_same_kind_predictors_bind_exact_recipe_caps_and_model_docs_are_deep_frozen() -> None:
    matrix = _matrix(row_count=1_000)
    seven = ml.fit_direct_model(_direct_candidate("HGB_7"), matrix)
    fifteen = ml.fit_direct_model(_direct_candidate("HGB_15"), matrix)
    seven_document = json.loads(seven.canonical_bytes)
    fifteen_predictor = json.loads(fifteen.canonical_bytes)["predictor"]
    assert max(sum(node["is_leaf"] for node in tree) for tree in fifteen_predictor["trees"]) == 15
    fifteen_predictor["learner_recipe_sha256"] = ml.canonical_sha256(
        seven_document["learner_recipe"]
    )
    seven_document["predictor"] = fifteen_predictor
    with pytest.raises(ml.AllCasesMLError, match="leaf/node cap"):
        ml.CanonicalMLModel.from_canonical_bytes(ml.canonical_json_bytes(seven_document))

    shortened = json.loads(seven.canonical_bytes)
    shortened["predictor"]["trees"].pop()
    with pytest.raises(ml.AllCasesMLError, match="tree count"):
        ml.CanonicalMLModel.from_canonical_bytes(ml.canonical_json_bytes(shortened))

    enet_a = ml.fit_direct_model(_direct_candidate("ENET_A"), matrix)
    enet_b = ml.fit_direct_model(_direct_candidate("ENET_B"), matrix)
    transplanted = json.loads(enet_a.canonical_bytes)
    transplanted["predictor"] = json.loads(enet_b.canonical_bytes)["predictor"]
    with pytest.raises(ml.AllCasesMLError, match="learner-recipe"):
        ml.CanonicalMLModel.from_canonical_bytes(ml.canonical_json_bytes(transplanted))

    before = enet_a.sha256
    with pytest.raises(TypeError):
        enet_a.predictor["coefficient_hex"][0] = (123.0).hex()
    with pytest.raises(TypeError):
        enet_a.candidate["learner_id"] = "ENET_B"
    assert enet_a.sha256 == before


def test_frozen_preprocessor_parser_rejects_bool_and_coerced_widths() -> None:
    fitted = ml.FrozenPreprocessor.fit(
        np.asarray(((1.0, np.nan), (2.0, 3.0)), dtype=np.float64),
        scale=True,
    )
    assert ml.FrozenPreprocessor.from_dict(fitted.as_dict()) == fitted
    for key, bad_value in (
        ("raw_feature_count", True),
        ("raw_feature_count", 2.0),
        ("transformed_feature_count", True),
        ("transformed_feature_count", 3.0),
    ):
        document = fitted.as_dict()
        document[key] = bad_value
        with pytest.raises(ml.AllCasesMLError, match="exact integers"):
            ml.FrozenPreprocessor.from_dict(document)
    document = fitted.as_dict()
    document["missing_indicator_indexes"] = [True]
    with pytest.raises(ml.AllCasesMLError, match="exact integers"):
        ml.FrozenPreprocessor.from_dict(document)
    with pytest.raises(ml.AllCasesMLError, match="raw width"):
        ml.FrozenPreprocessor(True, (0.0,), (), None, None)


def test_oos_schedule_binds_full_partition_calendar_and_emits_explicit_zero_days() -> None:
    matrix = _matrix(row_count=180)
    candidate = _direct_candidate()
    model = ml.fit_direct_model(candidate, matrix)
    schedule = _schedule(matrix, 2)
    feature_rows = _direct_feature_rows(matrix, 2)
    assert (
        schedule.candidate_id,
        schedule.feature_set_id,
        schedule.task_timeframe_seconds,
        schedule.task_horizon_seconds,
    ) == (
        candidate.candidate_id,
        candidate.feature_set_id,
        candidate.decision_timeframe_seconds,
        candidate.horizon_seconds,
    )
    assert all(
        exit_ns == entry_ns + candidate.horizon_seconds * 1_000_000_000
        for entry_ns, exit_ns in zip(
            schedule.entry_ns,
            schedule.planned_exit_ns,
            strict=True,
        )
    )
    assert set(schedule.decision_dates) < set(schedule.partition_decision_dates)
    mask = ml.freeze_prediction_mask(
        model,
        feature_rows,
        partition_key="WF1",
        execution_schedule=schedule,
    )
    outcomes = ml.build_frozen_resolved_outcome_rows(
        schedule,
        response_kind="DIRECT_TERMINAL_MOVE_TICKS",
        row_ids=schedule.row_ids,
        actual_exit_ns=schedule.planned_exit_ns,
        realized_values=(25, -10),
        valid_label_paths=(True, True),
        outcome_contracts=schedule.contracts,
        outcome_span_ids=schedule.outcome_span_ids,
        segment_ids=schedule.segment_ids,
        outcome_lineage_sha256=schedule.lineage_sha256,
        opportunity_lattice_sha256=schedule.opportunity_lattice_sha256,
    )
    evaluation = ml.evaluate_frozen_mask_economics(
        mask,
        outcomes,
        alignment_proof_sha256="9" * 64,
    )
    daily = dict(evaluation.daily_net_ticks)
    assert tuple(daily) == tuple(value.isoformat() for value in schedule.partition_decision_dates)
    assert all(
        daily[value.isoformat()] == 0
        for value in schedule.partition_decision_dates
        if value not in set(schedule.decision_dates)
    )

    wrong_exit = (schedule.planned_exit_ns[0] + 1, *schedule.planned_exit_ns[1:])
    with pytest.raises(ml.AllCasesMLError, match="execution schedule differs"):
        replace(schedule, planned_exit_ns=wrong_exit)
    alternate = _direct_candidate("ENET_B")
    wrong_candidate_schedule = replace(schedule, candidate_id=alternate.candidate_id)
    with pytest.raises(ml.AllCasesMLError, match="feature rows"):
        ml.freeze_prediction_mask(
            model,
            feature_rows,
            partition_key="WF1",
            execution_schedule=wrong_candidate_schedule,
        )
    with pytest.raises(ml.AllCasesMLError, match="feature rows"):
        ml.freeze_prediction_mask(
            model,
            feature_rows.values[::-1],
            partition_key="WF1",
            execution_schedule=schedule,
        )
    mismatched_rows = replace(
        feature_rows,
        contracts=("M6E", *feature_rows.contracts[1:]),
    )
    with pytest.raises(ml.AllCasesMLError, match="feature rows"):
        ml.freeze_prediction_mask(
            model,
            mismatched_rows,
            partition_key="WF1",
            execution_schedule=schedule,
        )
    malformed = schedule.as_dict()
    malformed["task_horizon_seconds"] = True
    with pytest.raises(ml.AllCasesMLError, match="document values"):
        ml.OutcomeFreeExecutionSchedule.from_dict(malformed)


def test_meta_oos_is_an_anchor_gate_then_symbolic_order_filter() -> None:
    candidate = _meta_candidate()
    source_rows, _ = _causal_rows()
    indexes = tuple(range(0, 20, 2))
    feature_rows, orders, order_batch, recipe, experts, policy = _symbolic_orders_for_feature_rows(
        replace(source_rows, stage_key="WF1"), indexes
    )
    matrix = _matrix(
        row_count=200,
        feature_set="FULL_MTF_213",
        meta=True,
        seed=53,
        base_trigger_family=_expert_base_family(experts),
        ranking_first_recipe=recipe,
        ranking_first_policy=policy,
    )
    model = ml.fit_meta_model(candidate, matrix, ranking_training_dates=_ranking_dates(matrix))
    certificate = model.symbolic_ranking_certificate
    assert certificate is not None
    schedule = ml.build_meta_anchor_gate_schedule(
        candidate,
        feature_rows,
        base_order_batch=order_batch,
        strategy_recipe=recipe,
        expert_artifacts=experts,
        partition_key="WF1",
        symbolic_ranking_certificate=certificate,
    )
    gate = ml.freeze_meta_anchor_gate(model, feature_rows, schedule)
    admitted_orders = ml.apply_meta_gate_to_symbolic_orders(gate, order_batch)
    assert schedule.decision_ns == tuple(item.anchor.outcome_key[3] for item in orders)
    assert tuple(item.order_id for item in admitted_orders) == tuple(
        order_id
        for order_id, admitted in zip(schedule.order_ids, gate.predictions.admitted, strict=True)
        if admitted
    )
    schedule_document = schedule.as_dict()
    assert all(
        "entry_ns" not in item and "exit_ns" not in item for item in schedule_document["rows"]
    )
    assert ml.MetaAnchorGateSchedule.from_dict(schedule_document) == schedule
    assert ml.FrozenMetaAnchorGate.from_dict(gate.as_dict()) == gate

    unrelated_certificate = _ranking_certificate(
        training_dates=certificate.training_dates,
        salt="unrelated-recipe",
    )
    with pytest.raises(ml.AllCasesMLError, match="recipe, orders, or Expert-8"):
        ml.build_meta_anchor_gate_schedule(
            candidate,
            feature_rows,
            base_order_batch=order_batch,
            strategy_recipe=recipe,
            expert_artifacts=experts,
            partition_key="WF1",
            symbolic_ranking_certificate=unrelated_certificate,
        )
    with pytest.raises(ml.AllCasesMLError, match="recipe, orders, or Expert-8"):
        ml.build_meta_anchor_gate_schedule(
            candidate,
            feature_rows,
            base_order_batch=order_batch,
            strategy_recipe=recipe,
            expert_artifacts=tuple(reversed(experts)),
            partition_key="WF1",
            symbolic_ranking_certificate=certificate,
        )

    other_batch_definition = {
        "anchor_policy_id": "b" * 64,
        "orders": [item.as_dict() for item in orders],
        "schema": symbolic.PATH_OUTCOME_SCHEMA,
    }
    other_batch = symbolic.EntryOrderBatch(
        "b" * 64,
        orders,
        symbolic.canonical_sha256(other_batch_definition),
    )
    with pytest.raises(ml.AllCasesMLError, match="order binding"):
        ml.apply_meta_gate_to_symbolic_orders(gate, other_batch)

    tampered = gate.as_dict()
    tampered["partition_key"] = "WF2"
    with pytest.raises(ml.AllCasesMLError, match="binding"):
        ml.FrozenMetaAnchorGate.from_dict(tampered)


def test_meta_freeze_and_recipe_rebuild_bind_feature_artifact_and_exact_expert_eight() -> None:
    candidate = _meta_candidate(feature_set="FULL_MTF_PLUS_EXPERT_221")
    source_rows, _ = _causal_rows(plus_expert=True)
    feature_rows, _orders, order_batch, recipe, experts, policy = _symbolic_orders_for_feature_rows(
        replace(source_rows, stage_key="WF1"), tuple(range(10))
    )
    matrix = _matrix(
        row_count=200,
        feature_set="FULL_MTF_PLUS_EXPERT_221",
        meta=True,
        seed=97,
        ranking_first_recipe=recipe,
        ranking_first_policy=policy,
    )
    model = ml.fit_meta_model(candidate, matrix, ranking_training_dates=_ranking_dates(matrix))
    certificate = model.symbolic_ranking_certificate
    assert certificate is not None
    schedule = ml.build_meta_anchor_gate_schedule(
        candidate,
        feature_rows,
        base_order_batch=order_batch,
        strategy_recipe=recipe,
        expert_artifacts=experts,
        partition_key="WF1",
        symbolic_ranking_certificate=certificate,
    )
    gate = ml.freeze_meta_anchor_gate(model, feature_rows, schedule)
    assert schedule.feature_rows_sha256 == feature_rows.artifact_sha256

    variants = (
        replace(feature_rows, values=feature_rows.values[::-1]),
        replace(feature_rows, values=np.zeros_like(feature_rows.values)),
        replace(feature_rows, atr_ticks_by_timeframe=feature_rows.atr_ticks_by_timeframe * 10),
        replace(feature_rows, contracts=("M6E", *feature_rows.contracts[1:])),
        replace(
            feature_rows,
            outcome_span_ids=np.asarray(feature_rows.outcome_span_ids, dtype=np.int64) + 1,
        ),
        replace(
            feature_rows,
            entry_ns=np.asarray(feature_rows.entry_ns, dtype=np.int64) + 1_000_000_000,
        ),
        replace(feature_rows, entry_schedule_sha256="c" * 64),
        replace(feature_rows, source_commitment_sha256="c" * 64),
    )
    for altered in variants:
        with pytest.raises(ml.AllCasesMLError, match="model/schedule binding"):
            ml.freeze_meta_anchor_gate(model, altered, schedule)

    original = experts[0]
    base = next(
        item
        for item in symbolic.build_base_event_catalog().candidates
        if item.candidate_id == original.base_candidate_id
    )
    alternate_base = next(
        item
        for item in symbolic.build_base_event_catalog().candidates
        if item.family == base.family
        and item.trigger_timeframe_seconds == base.trigger_timeframe_seconds
        and item.direction != base.direction
    )
    changed_values = (
        symbolic.CausalExpertValue.from_fraction(
            original.values[0].feature_name,
            original.values[0].fraction + 1,
        ),
        *original.values[1:],
    )
    transplants = (
        _self_consistent_expert_transplant(
            original,
            base_candidate_id=alternate_base.candidate_id,
        ),
        _self_consistent_expert_transplant(original, context_id="f" * 64),
        _self_consistent_expert_transplant(original, values=changed_values),
    )
    for transplanted in transplants:
        altered_experts = (transplanted, *experts[1:])
        altered_rows = replace(
            feature_rows,
            expert_artifact_sha256s=tuple(item.artifact_sha256 for item in altered_experts),
        )
        with pytest.raises(ml.AllCasesMLError, match="recipe|Expert-8"):
            ml.build_meta_anchor_gate_schedule(
                candidate,
                altered_rows,
                base_order_batch=order_batch,
                strategy_recipe=recipe,
                expert_artifacts=altered_experts,
                partition_key="WF1",
                symbolic_ranking_certificate=certificate,
            )

    document = gate.as_dict()
    document["prediction_input_sha256"] = "z" * 64
    with pytest.raises(ml.AllCasesMLError, match="binding"):
        ml.FrozenMetaAnchorGate.from_dict(document)


def test_meta_control_alignment_is_at_anchor_before_symbolic_execution() -> None:
    candidate = _meta_candidate()
    source_rows, _ = _causal_rows()
    feature_rows, _orders, order_batch, recipe, experts, policy = _symbolic_orders_for_feature_rows(
        replace(source_rows, stage_key="WF1"), tuple(range(10))
    )
    requested_by_world = {
        "REAL": {0, 1},
        "CIRCULAR_TARGET": {0, 1, 2, 3},
        "MATCHED_TARGET": {0, 1, 4, 5},
    }
    favored_by_world = {
        "REAL": {0, 1},
        "CIRCULAR_TARGET": {2, 3},
        "MATCHED_TARGET": {4, 5},
    }
    gates: list[ml.FrozenMetaAnchorGate] = []
    for world_index, world in enumerate(ml.NULL_WORLD_ORDER):
        matrix = _matrix(
            row_count=200,
            feature_set="FULL_MTF_213",
            meta=True,
            seed=61 + world_index,
            ranking_world=world,
            base_trigger_family=_expert_base_family(experts),
            ranking_first_recipe=recipe,
            ranking_first_policy=policy,
        )
        replacement_targets = None if world == "REAL" else np.roll(matrix.targets, world_index + 1)
        model = ml.fit_meta_model(
            candidate,
            matrix,
            ranking_training_dates=_ranking_dates(matrix),
            world=world,
            training_targets=replacement_targets,
        )
        certificate = model.symbolic_ranking_certificate
        assert certificate is not None
        schedule = ml.build_meta_anchor_gate_schedule(
            candidate,
            feature_rows,
            base_order_batch=order_batch,
            strategy_recipe=recipe,
            expert_artifacts=experts,
            partition_key="WF1",
            symbolic_ranking_certificate=certificate,
        )
        gate = ml.freeze_meta_anchor_gate(model, feature_rows, schedule)
        requested_indexes = requested_by_world[world]
        scores = tuple(
            10.0 if index in favored_by_world[world] else float(index) for index in range(10)
        )
        requested = tuple(index in requested_indexes for index in range(10))
        directions = tuple(
            schedule.base_directions[index] if requested[index] else ml.TradeDirection.FLAT
            for index in range(10)
        )
        predictions = ml.PredictionBatch(
            gate.model_sha256,
            candidate.candidate_id,
            schedule.order_ids,
            scores,
            requested,
            requested,
            directions,
            (None,) * 10,
        )
        gates.append(replace(gate, predictions=predictions))

    aligned = ml.align_frozen_meta_anchor_gates(*gates)
    aligned_gates = (aligned.real, aligned.circular_target, aligned.matched_target)
    assert all(sum(item.predictions.admitted) == 2 for item in aligned_gates)
    selected = {world: row_ids for world, row_ids in aligned.proof.selected_row_ids_by_world}
    for item in aligned_gates:
        assert (
            tuple(
                order.order_id for order in ml.apply_meta_gate_to_symbolic_orders(item, order_batch)
            )
            == selected[item.null_world]
        )


def test_rate_variants_share_one_exact_fit_but_freeze_separate_thresholds() -> None:
    direct_matrix = _matrix(row_count=200, seed=19)
    direct_five = _direct_candidate(rate=Fraction(1, 20))
    direct_ten = _direct_candidate(rate=Fraction(1, 10))
    assert ml.direct_fit_recipe_id(direct_five) == ml.direct_fit_recipe_id(direct_ten)
    cache = ml.SharedFitCache()
    five_model = ml.fit_direct_model(direct_five, direct_matrix, cache=cache)
    ten_model = ml.fit_direct_model(direct_ten, direct_matrix, cache=cache)
    assert (cache.fit_count, cache.cache_hits, cache.state_count) == (1, 1, 0)
    direct_cache_evidence = cache.terminal_evidence(maximum_fit_count=1)
    assert (
        ml.SharedFitCacheEvidence.from_dict(direct_cache_evidence.as_dict())
        == direct_cache_evidence
    )
    assert five_model.predictor == ten_model.predictor
    assert five_model.preprocessor == ten_model.preprocessor
    assert five_model.random_state == ten_model.random_state
    assert np.array_equal(
        five_model.predict_scores(direct_matrix.values),
        ten_model.predict_scores(direct_matrix.values),
    )
    assert five_model.admission_threshold >= ten_model.admission_threshold
    assert five_model.model_id != ten_model.model_id
    training_indexes = tuple(range(direct_matrix.row_count))
    assert ml.target_permutation_indexes(
        direct_matrix,
        training_indexes,
        world="CIRCULAR_TARGET",
        candidate_id=direct_five.candidate_id,
        fold_key="SEARCH_FINAL",
    ) == ml.target_permutation_indexes(
        direct_matrix,
        training_indexes,
        world="CIRCULAR_TARGET",
        candidate_id=direct_ten.candidate_id,
        fold_key="SEARCH_FINAL",
    )

    hgb_five = _direct_candidate("HGB_7", rate=Fraction(1, 20))
    hgb_ten = _direct_candidate("HGB_7", rate=Fraction(1, 10))
    hgb_cache = ml.SharedFitCache()
    hgb_five_model = ml.fit_direct_model(hgb_five, direct_matrix, cache=hgb_cache)
    hgb_ten_model = ml.fit_direct_model(hgb_ten, direct_matrix, cache=hgb_cache)
    assert (hgb_cache.fit_count, hgb_cache.cache_hits, hgb_cache.state_count) == (1, 1, 0)
    assert hgb_cache.terminal_evidence(maximum_fit_count=1).eviction_count == 1
    assert hgb_five_model.predictor == hgb_ten_model.predictor
    assert hgb_five_model.admission_threshold >= hgb_ten_model.admission_threshold

    meta_matrix = _matrix(
        row_count=200,
        feature_set="FULL_MTF_213",
        meta=True,
        seed=29,
    )
    meta_thirty = _meta_candidate(rate=Fraction(3, 10))
    meta_fifty = _meta_candidate(rate=Fraction(1, 2))
    assert ml.meta_fit_recipe_id(meta_thirty) == ml.meta_fit_recipe_id(meta_fifty)
    meta_cache = ml.SharedFitCache()
    thirty_model = ml.fit_meta_model(
        meta_thirty,
        meta_matrix,
        ranking_training_dates=_ranking_dates(meta_matrix),
        cache=meta_cache,
    )
    fifty_model = ml.fit_meta_model(
        meta_fifty,
        meta_matrix,
        ranking_training_dates=_ranking_dates(meta_matrix),
        cache=meta_cache,
    )
    assert (meta_cache.fit_count, meta_cache.cache_hits, meta_cache.state_count) == (1, 1, 0)
    assert meta_cache.terminal_evidence(maximum_fit_count=1).final_retained_bytes == 0
    assert thirty_model.predictor == fifty_model.predictor
    assert np.array_equal(
        thirty_model.predict_scores(meta_matrix.values),
        fifty_model.predict_scores(meta_matrix.values),
    )
    assert thirty_model.admission_threshold >= fifty_model.admission_threshold


def test_shared_fit_cache_is_peak_bounded_evicted_and_resume_aggregated() -> None:
    cache = ml.SharedFitCache()
    preprocessor = ml.FrozenPreprocessor(1, (0.0,), (), None, None)
    predictor = {
        "coefficient_hex": [(0.0).hex()],
        "intercept_hex": (0.0).hex(),
        "kind": "ELASTIC_NET_REGRESSOR",
        "learner_recipe_sha256": "1" * 64,
    }
    for index in range(21):
        cache._store(
            ml._CachedFitState(
                "1" * 64,
                "SIGNED_NORMALIZED_RETURN",
                "PRICE_GEOMETRY_36",
                ("x",),
                "REAL",
                "B3",
                preprocessor,
                predictor,
                (0.25, -0.5),
                2,
                f"{index + 101:064x}",
                7,
                None,
                None,
                None,
            )
        )
    assert cache.state_count == cache.peak_state_count == 21
    assert cache.current_retained_bytes == cache.peak_retained_bytes > 0
    for index in range(21):
        assert (
            cache._get(
                "1" * 64,
                "REAL",
                "B3",
                f"{index + 101:064x}",
            )
            is not None
        )
    proof = cache.terminal_evidence(maximum_fit_count=21)
    assert (proof.final_state_count, proof.final_retained_bytes) == (0, 0)
    with pytest.raises(ml.AllCasesMLError, match="beyond its two variants"):
        cache._get("1" * 64, "REAL", "B3", f"{101:064x}")

    direct_chunk = ml.SharedFitCacheEvidence(126, 126, 126, 0, 126, 0, 21, 0, 123_456)
    direct_aggregate = ml.aggregate_shared_fit_cache_evidence(
        "DIRECT",
        (direct_chunk,) * 24,
    )
    assert direct_aggregate.as_dict()["fit_count"] == 3_024
    assert (
        ml.SharedFitCacheAggregateEvidence.from_dict(direct_aggregate.as_dict()) == direct_aggregate
    )
    with pytest.raises(ml.AllCasesMLError):
        ml.aggregate_shared_fit_cache_evidence("DIRECT", (direct_chunk,) * 23)

    empty_meta_chunk = ml.SharedFitCacheEvidence(84, 0, 0, 0, 0, 0, 0, 0, 0)
    partial_meta_chunk = ml.SharedFitCacheEvidence(84, 1, 1, 0, 1, 0, 1, 0, 1_024)
    partial_meta = ml.aggregate_shared_fit_cache_evidence(
        "META",
        (partial_meta_chunk, *(empty_meta_chunk,) * 23),
    )
    assert partial_meta.as_dict()["fit_count"] == 1
    assert partial_meta.as_dict()["maximum_fit_count"] == 2_016


@pytest.mark.parametrize("first_rate", [Fraction(1, 20), Fraction(1, 10)])
def test_shared_fit_cache_asymmetric_rate_exit_is_explicitly_discarded(
    first_rate: Fraction,
) -> None:
    matrix = _matrix(row_count=180)
    cache = ml.SharedFitCache()
    first = _direct_candidate("ENET_A", rate=first_rate)
    ml.fit_direct_model(first, matrix, cache=cache)
    assert (cache.fit_count, cache.cache_hits, cache.state_count) == (1, 0, 1)
    ml.fit_direct_model(
        _direct_candidate("ENET_B", rate=first_rate),
        matrix,
        cache=cache,
    )
    assert (cache.fit_count, cache.cache_hits, cache.discarded_state_count) == (2, 0, 1)
    assert cache.discard_unconsumed_states() == 1
    evidence = cache.terminal_evidence(maximum_fit_count=2)
    assert (
        evidence.cache_hits,
        evidence.discarded_state_count,
        evidence.eviction_count,
        evidence.final_state_count,
        evidence.final_retained_bytes,
    ) == (0, 2, 2, 0, 0)
    assert ml.SharedFitCacheEvidence.from_dict(evidence.as_dict()) == evidence

    second_rate = Fraction(1, 10) if first_rate == Fraction(1, 20) else Fraction(1, 20)
    with pytest.raises(ml.AllCasesMLError, match="beyond its two variants"):
        ml.fit_direct_model(
            _direct_candidate("ENET_A", rate=second_rate),
            matrix,
            cache=cache,
        )


@pytest.mark.parametrize("learner", ["HGB_7", "HGB_15"])
def test_direct_nonlinear_models_round_trip_without_estimator_pickle(learner: str) -> None:
    matrix = _matrix(row_count=180, seed=23)
    candidate = _direct_candidate(learner)
    first = ml.fit_direct_model(candidate, matrix)
    second = ml.fit_direct_model(candidate, matrix)
    reopened = ml.CanonicalMLModel.from_canonical_bytes(first.canonical_bytes)
    assert first.canonical_bytes == second.canonical_bytes
    assert len(first.predictor["trees"]) == 200
    assert np.array_equal(
        first.predict_scores(matrix.values), reopened.predict_scores(matrix.values)
    )


@pytest.mark.parametrize("classifier", ["META_ENET", "META_HGB_7"])
def test_meta_gate_binds_ranked_strategy_and_preserves_base_direction(classifier: str) -> None:
    matrix = _matrix(row_count=200, feature_set="FULL_MTF_213", meta=True, seed=31)
    candidate = _meta_candidate(classifier)
    model = ml.fit_meta_model(candidate, matrix, ranking_training_dates=_ranking_dates(matrix))
    reopened = ml.CanonicalMLModel.from_canonical_bytes(model.canonical_bytes)
    assert reopened.base_strategy_id == matrix.base_strategy_id
    assert reopened.symbolic_ranking_sha256 == matrix.symbolic_ranking_sha256
    assert np.array_equal(
        model.predict_scores(matrix.values), reopened.predict_scores(matrix.values)
    )
    predictions = ml.predict_actions(
        reopened,
        matrix.values[:30],
        row_ids=matrix.row_ids[:30],
        base_directions=matrix.base_directions[:30],
    )
    for keep, base, direction in zip(
        predictions.admitted,
        matrix.base_directions[:30],
        predictions.directions,
        strict=True,
    ):
        expected = (
            ml.TradeDirection.LONG
            if keep and base == 1
            else ml.TradeDirection.SHORT
            if keep
            else ml.TradeDirection.FLAT
        )
        assert direction is expected


def test_cross_fit_does_not_read_last_validation_targets() -> None:
    dates = tuple(date(2020, 1, 1) + timedelta(days=index) for index in range(469))
    plan = ml.build_search_block_plan(dates)
    matrix = _matrix(row_count=469, one_date_per_row=True, seed=41)
    candidate = _direct_candidate("ENET_B")
    feasibility = ml.probe_null_world_feasibility(
        matrix,
        plan,
        candidate_id=candidate.candidate_id,
    )
    assert len(feasibility["records"]) == 14
    unsigned = {key: value for key, value in feasibility.items() if key != "report_sha256"}
    assert feasibility["report_sha256"] == ml.canonical_sha256(unsigned)
    original = ml.cross_fit_direct_candidate(candidate, matrix, plan)

    targets = np.array(matrix.targets, copy=True)
    last_validation = plan.outer_folds[-1].validation_dates
    last_indexes = [
        index for index, value in enumerate(matrix.decision_dates) if value in set(last_validation)
    ]
    targets[last_indexes] += 1_000_000
    mutated = replace(matrix, targets=targets)
    repeated = ml.cross_fit_direct_candidate(candidate, mutated, plan)
    assert original.fold_model_sha256 == repeated.fold_model_sha256
    assert original.scores == repeated.scores
    assert original.fold_source_matrix_sha256s != repeated.fold_source_matrix_sha256s
    assert original.fold_outcome_values_sha256s == repeated.fold_outcome_values_sha256s
    assert set(original.fold_keys) == set(ml.SEARCH_OUTER_FOLD_KEYS)
    assert len(original.row_indexes) == sum(ml.SEARCH_BLOCK_SIZES[2:])


def test_meta_cross_fit_binds_a_distinct_prior_prefix_ranking_per_fold() -> None:
    dates = tuple(date(2020, 1, 1) + timedelta(days=index) for index in range(469))
    plan = ml.build_search_block_plan(dates)
    base = _matrix(
        row_count=469,
        feature_set="FULL_MTF_213",
        meta=True,
        one_date_per_row=True,
        seed=47,
    )
    matrices = {}
    for world_index, world in zip((0, 3, 6), ml.NULL_WORLD_ORDER, strict=True):
        by_fold = {}
        for index, fold_key in enumerate(ml.SEARCH_OUTER_FOLD_KEYS, start=1):
            certificate = _ranking_certificate(
                world=world,
                fold_key=fold_key,
                training_dates=next(
                    fold.training_dates for fold in plan.outer_folds if fold.fold_key == fold_key
                ),
                base_trigger_family="MACD_STATE",
                salt=f"{world_index}-{index}",
            )
            by_fold[fold_key] = replace(
                base,
                base_strategy_id=certificate.ranked_strategies[0].strategy_id,
                symbolic_ranking_certificate=certificate,
            )
        matrices[world] = by_fold
    candidate = _meta_candidate("META_ENET")
    original = ml.cross_fit_meta_candidate(candidate, matrices, plan)
    assert original.fold_base_strategy_ids == tuple(
        matrices["REAL"][key].base_strategy_id for key in ml.SEARCH_OUTER_FOLD_KEYS
    )
    resolved = ml.meta_crossfit_realized_net_ticks(original, matrices)
    assert resolved.shape == (len(original.row_ids),)
    wrong_matrices = {world: dict(by_fold) for world, by_fold in matrices.items()}
    wrong_matrices["REAL"]["B3"] = replace(
        wrong_matrices["REAL"]["B3"],
        outcome_lineage_sha256="a" * 64,
    )
    with pytest.raises(ml.AllCasesMLError, match="recipe or lineage"):
        ml.meta_crossfit_realized_net_ticks(original, wrong_matrices)
    wrong_values = {world: dict(by_fold) for world, by_fold in matrices.items()}
    b3 = wrong_values["REAL"]["B3"]
    realized = np.array(b3.realized_net_ticks, copy=True)
    positive_index = int(np.flatnonzero(realized > 0)[0])
    realized[positive_index] += 1
    wrong_values["REAL"]["B3"] = replace(b3, realized_net_ticks=realized)
    with pytest.raises(ml.AllCasesMLError, match="recipe or lineage"):
        ml.meta_crossfit_realized_net_ticks(original, wrong_values)
    assert original.fold_symbolic_ranking_sha256 == tuple(
        matrices["REAL"][key].symbolic_ranking_sha256 for key in ml.SEARCH_OUTER_FOLD_KEYS
    )

    last = matrices["REAL"]["B8"]
    targets = np.array(last.targets, copy=True)
    targets[-58:] = 1.0 - targets[-58:]
    mutated = {
        **matrices,
        "REAL": {**matrices["REAL"], "B8": replace(last, targets=targets)},
    }
    repeated = ml.cross_fit_meta_candidate(candidate, mutated, plan)
    assert repeated.fold_base_strategy_ids == original.fold_base_strategy_ids
    assert repeated.fold_symbolic_ranking_sha256 == original.fold_symbolic_ranking_sha256
    assert repeated.fold_model_sha256 == original.fold_model_sha256
    assert repeated.scores == original.scores
    assert repeated.fold_source_matrix_sha256s != original.fold_source_matrix_sha256s
    assert repeated.fold_outcome_values_sha256s == original.fold_outcome_values_sha256s


def test_symbolic_ranking_certificate_is_typed_and_sparse_rank_is_candidate_local() -> None:
    training_dates = (date(2020, 1, 1), date(2020, 1, 2))
    certificate = _ranking_certificate(training_dates=training_dates, count=1)
    assert certificate.strategy_at_rank(1) is not None
    assert certificate.strategy_at_rank(2) is None
    assert ml.SymbolicRankingCertificate.from_dict(certificate.as_dict()) == certificate

    tampered = certificate.as_dict()
    tampered["ranked_strategies"][0]["rank_slot"] = 2
    with pytest.raises(ml.AllCasesMLError, match="certificate"):
        ml.SymbolicRankingCertificate.from_dict(tampered)

    matrix = _matrix(row_count=180, feature_set="FULL_MTF_213", meta=True)
    empty = _ranking_certificate(training_dates=tuple(sorted(set(matrix.decision_dates))), count=0)
    sparse = replace(matrix, symbolic_ranking_certificate=empty)
    with pytest.raises(ml.MLCandidateIneligible) as raised:
        ml.fit_meta_model(
            _meta_candidate(rank=1),
            sparse,
            ranking_training_dates=empty.training_dates,
        )
    assert raised.value.reason is ml.MLIneligibilityReason.INSUFFICIENT_BASE_STRATEGY_RANK


def test_search_final_meta_rows_may_be_sparse_but_certificate_domain_is_exact() -> None:
    candidate = _meta_candidate()
    matrix = _matrix(row_count=180, feature_set="FULL_MTF_213", meta=True)
    matrix_dates = tuple(sorted(set(matrix.decision_dates)))
    full_domain = matrix_dates + tuple(
        matrix_dates[-1] + timedelta(days=index) for index in range(1, 5)
    )
    assert matrix.symbolic_ranking_certificate is not None
    ranked = matrix.symbolic_ranking_certificate.ranked_strategies[0]
    policy = symbolic.AnchorPolicy(
        1,
        ranked.anchor_policy_id,
        ranked.base_candidate_id,
        ranked.context_id,
        ranked.time_filter_id,
        ranked.delay_id,
    )
    certificate = _ranking_certificate(
        training_dates=full_domain,
        base_trigger_family=matrix.base_trigger_family or "MACD_STATE",
        first_recipe=symbolic.CompleteStrategyRecipe(
            1,
            matrix.base_strategy_id or "1" * 64,
            1,
            ranked.anchor_policy_id,
            ranked.entry_policy_id,
            ranked.exit_policy_id,
        ),
        first_policy=policy,
    )
    sparse = replace(matrix, symbolic_ranking_certificate=certificate)
    model = ml.fit_meta_model(
        candidate,
        sparse,
        ranking_training_dates=full_domain,
    )
    assert model.symbolic_ranking_certificate == certificate
    with pytest.raises(ml.AllCasesMLError, match="ranking was not rebuilt"):
        ml.fit_meta_model(
            candidate,
            sparse,
            ranking_training_dates=matrix_dates,
        )


def test_null_world_is_refitted_and_meta_requires_binary_rank_bound_data() -> None:
    matrix = _matrix(row_count=180)
    candidate = _direct_candidate()
    indexes = tuple(range(matrix.row_count))
    targets = ml.permuted_training_targets(
        matrix,
        indexes,
        world="CIRCULAR_TARGET",
        candidate_id=candidate.candidate_id,
        fold_key="SEARCH_FINAL",
    )
    real = ml.fit_direct_model(candidate, matrix)
    null = ml.fit_direct_model(
        candidate,
        matrix,
        world="CIRCULAR_TARGET",
        training_targets=targets,
    )
    assert real.model_id != null.model_id
    assert real.training_rows_sha256 != null.training_rows_sha256
    with pytest.raises(ml.AllCasesMLError, match="requires explicit"):
        ml.fit_direct_model(candidate, matrix, world="MATCHED_TARGET")

    nonbinary = _matrix(
        row_count=180,
        feature_set="FULL_MTF_213",
        meta=True,
        targets=np.linspace(0.0, 1.0, 180),
    )
    with pytest.raises(ml.AllCasesMLError, match="both binary classes"):
        ml.fit_meta_model(
            _meta_candidate(),
            nonbinary,
            ranking_training_dates=_ranking_dates(nonbinary),
        )


def test_global_occupancy_does_not_reset_at_contract_roll() -> None:
    candidate = _direct_candidate()
    batch = ml.PredictionBatch(
        "a" * 64,
        candidate.candidate_id,
        ("first", "rolled-overlap", "at-exit"),
        (1.0, 1.1, 1.2),
        (True, True, True),
        (True, True, True),
        (ml.TradeDirection.LONG,) * 3,
        (10.0, 11.0, 12.0),
    )
    occupied = ml.apply_nonoverlap_occupancy(
        batch,
        entry_ns=np.array([10, 15, 20]),
        planned_exit_ns=np.array([20, 25, 30]),
        contracts=("6E", "M6E", "M6E"),
    )
    assert occupied.admitted == (True, False, True)
    assert occupied.directions == (
        ml.TradeDirection.LONG,
        ml.TradeDirection.FLAT,
        ml.TradeDirection.LONG,
    )


def test_crossfit_global_occupancy_does_not_reset_at_outer_fold_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dates = tuple(date(2020, 1, 1) + timedelta(days=index) for index in range(469))
    plan = ml.build_search_block_plan(dates)
    matrix = _matrix(row_count=469, one_date_per_row=True)
    exits = np.array(matrix.label_exit_ns, copy=True)
    exits[176] = int(matrix.entry_ns[177]) + 1
    matrix = replace(matrix, label_exit_ns=exits)
    candidate = _direct_candidate()

    monkeypatch.setattr(ml, "probe_null_world_feasibility", lambda *args, **kwargs: {})

    def fake_fit(*args: object, **kwargs: object) -> SimpleNamespace:
        fold_key = str(kwargs["fold_key"])
        return SimpleNamespace(
            sha256=ml.canonical_sha256({"fold_key": fold_key}),
            admission_threshold=0.1,
        )

    def fake_predict(
        model: SimpleNamespace,
        values: np.ndarray,
        *,
        row_ids: tuple[str, ...],
        atr_ticks: np.ndarray,
    ) -> ml.PredictionBatch:
        del atr_ticks
        count = len(values)
        return ml.PredictionBatch(
            model.sha256,
            candidate.candidate_id,
            row_ids,
            tuple(1.0 for _ in range(count)),
            tuple(True for _ in range(count)),
            tuple(True for _ in range(count)),
            tuple(ml.TradeDirection.LONG for _ in range(count)),
            tuple(20.0 for _ in range(count)),
        )

    monkeypatch.setattr(ml, "fit_direct_model", fake_fit)
    monkeypatch.setattr(ml, "predict_actions", fake_predict)
    result = ml.cross_fit_direct_candidate(candidate, matrix, plan)
    assert result.fold_keys[58:60] == ("B3", "B4")
    assert result.requested[58:60] == (True, True)
    assert result.admitted[58:60] == (True, False)


def test_control_alignment_matches_each_date_direction_and_fails_closed() -> None:
    real = _crossfit_fixture(
        "REAL",
        admitted_indexes=(0, 4),
        long_indexes=(0,),
        score_offset=1.0,
    )
    assert ml.CrossFittedScores.from_dict(real.as_dict()) == real
    tampered = real.as_dict()
    tampered["rows"][0]["score_hex"] = (999.0).hex()
    with pytest.raises(ml.AllCasesMLError, match="round trip"):
        ml.CrossFittedScores.from_dict(tampered)
    wrong_boolean = real.as_dict()
    wrong_boolean["rows"][0]["requested"] = 1
    wrong_boolean["rows"][0]["admitted"] = 1
    wrong_boolean["artifact_sha256"] = ml.canonical_sha256(
        {key: value for key, value in wrong_boolean.items() if key != "artifact_sha256"}
    )
    with pytest.raises(ml.AllCasesMLError, match="document rows"):
        ml.CrossFittedScores.from_dict(wrong_boolean)
    circular = _crossfit_fixture(
        "CIRCULAR_TARGET",
        admitted_indexes=(1, 5, 8),
        long_indexes=(1, 8),
        score_offset=2.0,
    )
    matched = _crossfit_fixture(
        "MATCHED_TARGET",
        admitted_indexes=(2, 6, 9),
        long_indexes=(2, 9),
        score_offset=3.0,
    )
    aligned = ml.align_cross_fitted_null_controls(real, circular, matched)
    assert aligned.circular_target.admitted == tuple(index in {1, 5} for index in range(12))
    assert aligned.matched_target.admitted == tuple(index in {2, 6} for index in range(12))
    assert aligned.proof.target_count_records == (
        ("2024-01-01", "LONG", 1),
        ("2024-01-02", "SHORT", 1),
    )
    assert aligned.proof.artifact_sha256 == ml.canonical_sha256(aligned.proof.definition_dict())

    alternate_circular = _crossfit_fixture(
        "CIRCULAR_TARGET",
        admitted_indexes=(0, 1, 4, 5, 8),
        long_indexes=(0, 1, 8),
        score_offset=2.0,
    )
    alternate_matched = _crossfit_fixture(
        "MATCHED_TARGET",
        admitted_indexes=(0, 2, 4, 6, 9),
        long_indexes=(0, 2, 9),
        score_offset=3.0,
    )
    alternate = ml.align_cross_fitted_null_controls(
        real,
        alternate_circular,
        alternate_matched,
    )
    assert alternate.proof.source_mask_sha256_by_world != (
        aligned.proof.source_mask_sha256_by_world
    )
    with pytest.raises(ml.AllCasesMLError, match="not paired"):
        ml.AlignedCrossFitControls(
            aligned.real,
            aligned.circular_target,
            aligned.matched_target,
            alternate.proof,
        )

    insufficient = replace(
        circular,
        admitted=tuple(index in {1, 8} for index in range(12)),
        directions=tuple(
            ml.TradeDirection.LONG if index in {1, 8} else ml.TradeDirection.FLAT
            for index in range(12)
        ),
    )
    with pytest.raises(ml.MLCandidateIneligible) as raised:
        ml.align_cross_fitted_null_controls(real, insufficient, matched)
    assert raised.value.reason is ml.MLIneligibilityReason.CONTROL_ALIGNMENT_INSUFFICIENT


def test_search_economics_are_from_aligned_actions_and_shared_gate_contract() -> None:
    real = _crossfit_fixture("REAL", admitted_indexes=(0, 4), long_indexes=(0,), score_offset=1.0)
    circular = _crossfit_fixture(
        "CIRCULAR_TARGET", admitted_indexes=(1, 5, 8), long_indexes=(1, 8), score_offset=2.0
    )
    matched = _crossfit_fixture(
        "MATCHED_TARGET", admitted_indexes=(2, 6, 9), long_indexes=(2, 9), score_offset=3.0
    )
    aligned = ml.align_cross_fitted_null_controls(real, circular, matched)
    matrix = _matrix_bound_to_crossfit(real)
    reporting = {
        value: f"R{index + 1}" for index, value in enumerate(sorted(set(real.decision_dates)))
    }
    evaluated = ml.evaluate_aligned_ml_search_controls(
        aligned,
        reporting_group_by_date=reporting,
        direct_matrix=matrix,
    )
    assert evaluated.real.fill_count == 2
    assert evaluated.real.raw_signal_count == 2
    assert evaluated.real.alignment_proof_sha256 == aligned.proof.artifact_sha256
    gate = ml.apply_ml_search_gates(evaluated)
    assert not gate.eligible
    assert "RAW_SIGNALS_LT_60" in gate.rejection_reasons
    selection = ml.select_ml_search_candidates((evaluated.real,), (gate,))
    assert selection.classification == "NO_ML_CANDIDATES"


def test_search_diversity_uses_exact_action_jaccard_and_integer_daily_correlation() -> None:
    reporting = {date(2024, 1, 1) + timedelta(days=index): f"R{index + 1}" for index in range(3)}

    def evaluated(
        admitted: tuple[int, ...],
        daily_values: tuple[int, int, int],
    ) -> ml.MLSearchEconomicEvaluation:
        result = _crossfit_fixture(
            "REAL",
            admitted_indexes=admitted,
            long_indexes=admitted,
            score_offset=1.0,
        )
        net = np.zeros(12, dtype=np.int64)
        for index, value in zip(admitted, daily_values, strict=True):
            net[index] = value
        return ml.evaluate_ml_crossfit_economics(
            result,
            net,
            reporting_group_by_date=reporting,
            alignment_proof_sha256="9" * 64,
        )

    base = evaluated((0, 4, 8), (1, 2, 3))
    orthogonal = evaluated((1, 5, 9), (1, -2, 1))
    correlated = evaluated((1, 5, 9), (2, 4, 6))
    same_actions = evaluated((0, 4, 8), (1, -2, 1))
    zero_variance = evaluated((1, 5, 9), (1, 1, 1))
    assert ml._ml_search_pair_is_diverse(base, orthogonal)
    assert not ml._ml_search_pair_is_diverse(base, correlated)
    assert not ml._ml_search_pair_is_diverse(base, same_actions)
    assert not ml._ml_search_pair_is_diverse(base, zero_variance)
    assert tuple(day for day, _ in base.daily_net_ticks) == tuple(
        value.isoformat() for value in reporting
    )


def test_frozen_oos_outcomes_preserve_lattice_and_never_drop_missing_rows() -> None:
    matrix = _matrix(row_count=180)
    model = ml.fit_direct_model(_direct_candidate(), matrix)
    schedule = _schedule(matrix, 20)
    feature_rows = _direct_feature_rows(matrix, 20)
    mask = ml.freeze_prediction_mask(
        model,
        feature_rows,
        partition_key="WF1",
        execution_schedule=schedule,
    )
    move_ticks = tuple(index - 5 for index in range(20))
    outcomes = ml.build_frozen_resolved_outcome_rows(
        schedule,
        response_kind="DIRECT_TERMINAL_MOVE_TICKS",
        row_ids=schedule.row_ids,
        actual_exit_ns=schedule.planned_exit_ns,
        realized_values=move_ticks,
        valid_label_paths=(True,) * 20,
        outcome_contracts=schedule.contracts,
        outcome_span_ids=schedule.outcome_span_ids,
        segment_ids=schedule.segment_ids,
        outcome_lineage_sha256=schedule.lineage_sha256,
        opportunity_lattice_sha256=schedule.opportunity_lattice_sha256,
    )
    evaluated = ml.evaluate_frozen_mask_economics(
        mask,
        outcomes,
        alignment_proof_sha256="9" * 64,
    )
    assert len(mask.execution_schedule.row_ids) == 20
    assert evaluated.raw_signal_count == sum(mask.predictions.requested)
    assert evaluated.raw_signal_count == 2
    assert evaluated.fill_count == sum(mask.predictions.admitted)
    with pytest.raises(ml.AllCasesMLError, match="resolved outcomes differ"):
        replace(outcomes, actual_exit_ns=(True, *outcomes.actual_exit_ns[1:]))

    with pytest.raises(ml.AllCasesMLError, match="execution schedule differs"):
        replace(
            schedule,
            outcome_span_ids=(True, *schedule.outcome_span_ids[1:]),
        )
    with pytest.raises(ml.AllCasesMLError, match="execution schedule differs"):
        replace(
            schedule,
            planned_exit_ns=(float(schedule.planned_exit_ns[0]), *schedule.planned_exit_ns[1:]),
        )

    response = {
        "response_kind": "DIRECT_TERMINAL_MOVE_TICKS",
        "row_ids": schedule.row_ids,
        "actual_exit_ns": schedule.planned_exit_ns,
        "realized_values": move_ticks,
        "valid_label_paths": (True,) * 20,
        "outcome_contracts": schedule.contracts,
        "outcome_span_ids": schedule.outcome_span_ids,
        "segment_ids": schedule.segment_ids,
        "outcome_lineage_sha256": schedule.lineage_sha256,
        "opportunity_lattice_sha256": schedule.opportunity_lattice_sha256,
    }
    for key, bad_values in (
        ("actual_exit_ns", (True, *schedule.planned_exit_ns[1:])),
        ("outcome_span_ids", (True,) * 20),
        ("segment_ids", (True,) * 20),
    ):
        with pytest.raises(ml.AllCasesMLError, match="exact int64"):
            ml.build_frozen_resolved_outcome_rows(
                schedule,
                **{**response, key: bad_values},
            )

    missing = [True] * 20
    missing[3] = False
    with pytest.raises(ml.AllCasesMLError, match="missing, shortened"):
        ml.build_frozen_resolved_outcome_rows(
            schedule,
            response_kind="DIRECT_TERMINAL_MOVE_TICKS",
            row_ids=schedule.row_ids,
            actual_exit_ns=schedule.planned_exit_ns,
            realized_values=move_ticks,
            valid_label_paths=missing,
            outcome_contracts=schedule.contracts,
            outcome_span_ids=schedule.outcome_span_ids,
            segment_ids=schedule.segment_ids,
            outcome_lineage_sha256=schedule.lineage_sha256,
            opportunity_lattice_sha256=schedule.opportunity_lattice_sha256,
        )


def test_null_derangement_infeasibility_is_candidate_local_not_integrity_error() -> None:
    matrix = _matrix(row_count=20)
    spans = np.array([1] * 12 + [2] * 8, dtype=np.int64)
    one_contract = replace(matrix, contracts=("6E",) * 20, outcome_span_ids=spans)
    with pytest.raises(ml.MLCandidateIneligible) as raised:
        ml.target_permutation_indexes(
            one_contract,
            tuple(range(20)),
            world="MATCHED_TARGET",
            candidate_id=_direct_candidate().candidate_id,
            fold_key="SEARCH_FINAL",
        )
    assert raised.value.reason is ml.MLIneligibilityReason.NULL_DERANGEMENT_INFEASIBLE
    assert raised.value.as_dict()["schema"].endswith("ineligibility.v1")
