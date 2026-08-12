from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from systematic_fx.research.m0a import build_features, build_fixture, build_labels, load_epoch
from systematic_fx.research.m0a.controls import generate_null_candidates
from systematic_fx.research.m0a.evaluate import (
    SEARCH_DATA_RESULT,
    AdmissionRules,
    EvaluationError,
    assert_walking_skeleton,
    evaluate_candidate,
    evaluate_epoch,
    evaluate_null_candidate,
)
from systematic_fx.research.m0a.family import (
    PULLBACK_CONTINUATION_FAMILY_ID,
    PullbackContinuationParameters,
    StrategyCandidate,
    generate_candidates,
)
from systematic_fx.research.m0a.model import (
    BarrierSpec,
    Direction,
    EventFeature,
    FirstTouchType,
    QuoteAwareLabel,
)
from systematic_fx.research.m0a.report import (
    EpochReportMetadata,
    render_durable_markdown_report,
    render_markdown_report,
)

ONE_SECOND_NS = 1_000_000_000
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _barrier() -> BarrierSpec:
    return BarrierSpec(
        barrier_id="tp1_sl1_h15m",
        k_tp_num=1,
        k_tp_den=1,
        k_sl_num=1,
        k_sl_den=1,
        max_hold_seconds=900,
    )


def _candidate(*, generation_seed: int = 19, generation_index: int = 0) -> StrategyCandidate:
    return StrategyCandidate(
        family_id=PULLBACK_CONTINUATION_FAMILY_ID,
        direction=Direction.LONG,
        barrier=_barrier(),
        parameters=PullbackContinuationParameters(
            trend_1h_min_ticks=2,
            pullback_length_min=1,
            pullback_length_max=4,
            close_location_threshold_ppm=650_000,
            volatility_quantile_min_ppm=100_000,
            volatility_quantile_max_ppm=900_000,
            imbalance_threshold_ppm=None,
        ),
        generation_seed=generation_seed,
        generation_index=generation_index,
    )


def _fixture_rows() -> tuple[tuple[EventFeature, ...], tuple[QuoteAwareLabel, ...]]:
    base = 1_800_000_000 * ONE_SECOND_NS
    signal_indexes = {0, 1, 4, 5, 8, 9, 12, 13}
    features: list[EventFeature] = []
    labels: list[QuoteAwareLabel] = []
    barrier = _barrier()
    for index in range(16):
        event_ts = base + index * 300 * ONE_SECOND_NS
        trading_date = date(2026, 1, 5) + timedelta(days=index // 4)
        is_signal = index in signal_indexes
        feature = EventFeature(
            event_ts_ns=event_ts,
            instrument_id=101,
            session_id=f"rth-{index // 4}",
            trading_date=trading_date,
            feature_version="m0a_feature_v1",
            source_data_version="fixture_v1",
            bar_open_ticks=20_000,
            bar_high_ticks=20_006,
            bar_low_ticks=19_996,
            bar_close_ticks=20_004 if is_signal else 19_998,
            trailing_return_ticks=3 if is_signal else -2,
            range_ticks=10,
            volatility_ticks=8,
            body_ratio_ppm=400_000,
            close_location_ppm=800_000 if is_signal else 200_000,
            short_trend_ticks=-1,
            pullback_length=2,
            spread_ticks=1,
            depth_imbalance_ppm=100_000 if is_signal else -100_000,
            volatility_quantile_ppm=500_000,
            trend_30m_ticks=2,
            context_30m_end_ns=event_ts,
            trend_1h_ticks=5,
            context_1h_end_ns=event_ts,
            roll_cross=False,
            inside_roll_guard=False,
            feature_valid=True,
            validity_flags=(),
        )
        features.append(feature)

        # Candidate events are deliberately favorable; matched non-signal
        # entries are unfavorable.  Consecutive signal pairs overlap because
        # every label remains open for twelve minutes.
        outcome = FirstTouchType.TP_FIRST if is_signal else FirstTouchType.SL_FIRST
        gross = 8 if outcome is FirstTouchType.TP_FIRST else -5
        entry_price = 20_001
        exit_price = entry_price + gross
        labels.append(
            QuoteAwareLabel(
                event_ts_ns=event_ts,
                instrument_id=101,
                direction=Direction.LONG,
                barrier_id=barrier.barrier_id,
                k_tp_num=barrier.k_tp_num,
                k_tp_den=barrier.k_tp_den,
                k_sl_num=barrier.k_sl_num,
                k_sl_den=barrier.k_sl_den,
                max_hold_seconds=barrier.max_hold_seconds,
                entry_ts_ns=event_ts + ONE_SECOND_NS,
                entry_price_ticks=entry_price,
                tp_price_ticks=entry_price + 8,
                sl_price_ticks=entry_price - 5,
                first_touch_type=outcome,
                first_touch_ts_ns=event_ts + 720 * ONE_SECOND_NS,
                exit_ts_ns=event_ts + 720 * ONE_SECOND_NS,
                exit_price_ticks=exit_price,
                timeout=False,
                ambiguous=False,
                raw_fallback_used=False,
                cost_ticks=2,
                gross_pnl_ticks=gross,
                net_pnl_ticks=gross - 2,
                label_version="m0a_label_v1",
                eligible=True,
                invalid_reason=None,
            )
        )
    return tuple(features), tuple(labels)


def test_family_generation_is_finite_deterministic_and_hashes_configuration() -> None:
    barriers = (
        _barrier(),
        BarrierSpec("tp3_4_sl1_h30m", 3, 4, 1, 1, 1_800),
    )
    first = generate_candidates(budget=20, seed=42, barriers=barriers)
    repeated = generate_candidates(budget=20, seed=42, barriers=barriers)

    assert first == repeated
    assert len(first) == 20
    assert len({item.candidate_hash for item in first}) == 20
    assert (
        _candidate().candidate_hash
        == _candidate(
            generation_seed=999,
            generation_index=91,
        ).candidate_hash
    )

    assert (
        AdmissionRules.from_config(
            {
                "min_raw_events": 3,
                "min_sequential_trades": 2,
                "min_active_days": 1,
                "min_tp_probability_ppm": 500_000,
                "min_positive_folds": 1,
                "require_positive_net_ev": True,
            }
        )
        == AdmissionRules()
    )


def test_raw_and_flat_metrics_differ_and_sequential_intervals_do_not_overlap() -> None:
    features, labels = _fixture_rows()
    result = evaluate_candidate(
        _candidate(),
        features,
        labels,
        seed=77,
        feature_lookback_seconds=300,
        fold_count=3,
        control_block_size=3,
    )

    assert result.raw_event_metrics.trade_count == 8
    assert result.flat_only_metrics.trade_count == 4
    assert result.sequential_metrics.trade_count == 4
    assert result.raw_event_metrics.trade_count != result.flat_only_metrics.trade_count
    assert result.flat_only_metrics.skipped_occupied_count == 4
    assert all(
        left.exit_ts_ns <= right.entry_ts_ns
        for left, right in zip(result.trades, result.trades[1:])
    )
    assert {trade.instrument_id for trade in result.trades} == {101}
    assert result.sequential_metrics.net_pnl_ticks > 0
    assert result.stressed_cost_metrics.net_pnl_ticks < result.sequential_metrics.net_pnl_ticks


def test_walk_forward_and_both_controls_are_deterministic_search_data_results() -> None:
    features, labels = _fixture_rows()
    options = {
        "seed": 123,
        "feature_lookback_seconds": 300,
        "fold_count": 3,
        "control_block_size": 3,
        "admission_rules": AdmissionRules(min_positive_folds=0),
    }
    first = evaluate_candidate(_candidate(), features, labels, **options)
    repeated = evaluate_candidate(_candidate(), features, labels, **options)

    assert first.as_dict() == repeated.as_dict()
    assert first.folds
    assert all(fold.result_role == SEARCH_DATA_RESULT for fold in first.folds)
    assert all(fold.purge_seconds >= 1_200 for fold in first.folds)
    assert first.circular_shift_control.method == "CIRCULAR_BLOCK_TIME_SHIFT"
    assert first.matched_random_control.method == "MATCHED_RANDOM_ENTRY"
    assert first.circular_shift_control.selection["original_signal_count"] == 8
    assert first.matched_random_control.selection["holding_horizon_seconds"] == 900
    assert first.paper_eligible is False
    assert first.live_eligible is False

    from_manifest_mapping = evaluate_candidate(
        _candidate(),
        features,
        labels,
        seed=123,
        feature_lookback_seconds=300,
        admission_rules=AdmissionRules().as_dict() | {"authority": "SEARCH_DATA_ONLY"},
    )
    assert from_manifest_mapping.status == first.status

    with pytest.raises(EvaluationError, match="purge"):
        evaluate_candidate(
            _candidate(),
            features,
            labels,
            seed=123,
            feature_lookback_seconds=300,
            purge_seconds=1_199,
        )


def test_explicit_null_candidates_are_two_per_real_and_standalone_ledger_payloads() -> None:
    features, labels = _fixture_rows()
    parent = _candidate()
    nulls = generate_null_candidates((parent,), seed=456, circular_block_size=3)
    repeated = generate_null_candidates((parent,), seed=456, circular_block_size=3)

    assert nulls == repeated
    assert [item.control_id for item in nulls] == [
        "circular_block_shift_v1",
        "matched_random_entry_v1",
    ]
    assert len({item.candidate_hash for item in nulls}) == 2
    for null in nulls:
        result = evaluate_null_candidate(null, parent, features, labels)
        payload = result.as_dict()
        assert payload["candidate_hash"] == null.candidate_hash
        assert payload["parent_candidate_hash"] == parent.candidate_hash
        assert payload["status"] == "SCREENED_OUT"
        assert payload["admitted"] is False


def test_walking_skeleton_boundary_requires_survivor_dedup_folds_and_exact_null_budget() -> None:
    features, labels = _fixture_rows()
    epoch = evaluate_epoch(
        (_candidate(),),
        features,
        labels,
        seed=321,
        feature_lookback_seconds=300,
        fold_count=3,
        control_block_size=3,
        admission_rules=AdmissionRules(min_positive_folds=0),
    )
    assert_walking_skeleton(epoch, expected_real_budget=1, expected_null_budget=2)

    with pytest.raises(EvaluationError, match="null budget"):
        assert_walking_skeleton(epoch, expected_real_budget=1, expected_null_budget=3)


def test_report_states_holdout_and_promotion_boundary_without_overclaiming() -> None:
    features, labels = _fixture_rows()
    epoch = evaluate_epoch(
        (_candidate(),),
        features,
        labels,
        seed=321,
        feature_lookback_seconds=300,
        fold_count=3,
        control_block_size=3,
        admission_rules=AdmissionRules(min_positive_folds=0),
    )
    candidate_hash = _candidate().candidate_hash
    metadata = EpochReportMetadata(
        epoch_id="m0a-fixture-epoch",
        epoch_hash="a" * 64,
        dataset_version="m0a_fixture_v1",
        dataset_hash="b" * 64,
        feature_version="m0a_feature_v1",
        label_version="m0a_label_v1",
        code_commit="c" * 40,
        execution_model_version="m0a_quote_execution_v1",
        real_candidate_budget=1,
        null_candidate_budget=2,
        roll_exclusion_count=3,
        session_exclusion_count=4,
        ambiguous_label_count=2,
        candidate_registered_at={candidate_hash: "2026-08-11T18:00:00Z"},
    )
    report = render_markdown_report(epoch, metadata)

    for text in (
        "Search-data result",
        "Exploratory only",
        "Sealed holdout untouched",
        "Not paper eligible",
        "Not live eligible",
        "awaiting sealed holdout",
        "CIRCULAR_BLOCK_TIME_SHIFT",
        "MATCHED_RANDOM_ENTRY",
        "Passive take-profit requires trade-through",
        "candidate_registered_at",
        "UNTOUCHED_ACCESS_DENIED",
    ):
        assert text in report
    assert "validated alpha" not in report.lower()
    assert "proven strategy" not in report.lower()


def test_canonical_fixture_epoch_completes_the_evaluation_walking_skeleton() -> None:
    config = load_epoch(PROJECT_ROOT / "epochs/m0a_fixture_v1.toml")
    fixture = build_fixture(config)
    features = build_features(config, fixture)
    labels = build_labels(config, fixture, features)
    candidates = generate_candidates(
        budget=config.real_candidate_budget,
        seed=config.random_seeds[0],
        barriers=config.barrier_specs,
    )
    nulls = generate_null_candidates(candidates, seed=config.random_seeds[1])
    evaluation = evaluate_epoch(
        candidates,
        features,
        labels,
        seed=config.random_seeds[1],
        feature_lookback_seconds=max(
            config.atr_lookback_bars,
            config.quantile_lookback_bars,
            config.short_trend_lookback_bars,
        )
        * config.decision_clock_seconds,
        admission_rules=AdmissionRules.from_config(config),
    )

    assert len(nulls) == config.null_candidate_budget == 24
    assert_walking_skeleton(
        evaluation,
        expected_real_budget=config.real_candidate_budget,
        expected_null_budget=config.null_candidate_budget,
    )
    survivor = next(item for item in evaluation.evaluations if item.search_data_survivor)
    assert survivor.raw_event_metrics.trade_count == 8
    assert survivor.flat_only_metrics.trade_count == 5
    assert survivor.sequential_metrics.net_pnl_ticks == 11


def test_durable_report_renders_verified_mapping_records_without_dataclass_reconstruction() -> None:
    features, labels = _fixture_rows()
    result = evaluate_candidate(
        _candidate(),
        features,
        labels,
        seed=321,
        feature_lookback_seconds=300,
        admission_rules=AdmissionRules(min_positive_folds=0),
    )
    candidate = _candidate()
    report = render_durable_markdown_report(
        {
            "status": "COMPLETED",
            "candidate_status_counts": {"REGISTERED": 1, "SCREENED_OUT": 2},
            "attempt_status_counts": {"COMPLETED": 3},
        },
        (
            {
                "candidate_sha256": candidate.candidate_hash,
                "candidate_kind": "REAL",
                "status": "REGISTERED",
                "registered_at": "2026-08-11T18:00:00Z",
                "candidate_payload": candidate.as_dict(),
                "evaluation": result.as_dict(),
                "attempt_count": 2,
            },
        ),
        EpochReportMetadata(
            epoch_id="durable-m0a",
            epoch_hash="d" * 64,
            dataset_version="fixture_v1",
            dataset_hash="e" * 64,
            feature_version="feature_v1",
            label_version="label_v1",
            code_commit="f" * 40,
            execution_model_version="execution_v1",
            real_candidate_budget=3,
            null_candidate_budget=6,
        ),
    )

    assert "Durable budget and execution status" in report
    assert candidate.candidate_hash in report
    assert "CIRCULAR_BLOCK_TIME_SHIFT" in report
    assert "MATCHED_RANDOM_ENTRY" in report
    assert "Sealed holdout untouched" in report
    assert "Retries: 1" in report
    assert "Real budget used: 1" in report
