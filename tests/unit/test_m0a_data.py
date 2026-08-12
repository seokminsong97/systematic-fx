from __future__ import annotations

from collections import Counter
from dataclasses import replace
from itertools import pairwise
from pathlib import Path

import pytest

from systematic_fx.research.m0a import (
    Direction,
    EventFeature,
    FirstTouchType,
    M0aConfigError,
    MarketFixture,
    QuoteAwareLabel,
    QuoteEvent,
    build_features,
    build_fixture,
    build_labels,
    compute_code_snapshot_sha256,
    load_epoch,
)
from systematic_fx.research.m0a.evaluate import AdmissionRules, generate_and_evaluate_epoch
from systematic_fx.research.m0a.family import generate_candidates
from systematic_fx.research.m0a.labels import _first_passage

EPOCH_PATH = Path(__file__).resolve().parents[2] / "epochs" / "m0a_fixture_v1.toml"
NS_PER_SECOND = 1_000_000_000


@pytest.fixture(scope="module")
def epoch():
    return load_epoch(EPOCH_PATH)


@pytest.fixture(scope="module")
def fixture(epoch):
    return build_fixture(epoch)


@pytest.fixture(scope="module")
def features(epoch, fixture):
    return build_features(epoch, fixture)


@pytest.fixture(scope="module")
def labels(epoch, fixture, features):
    return build_labels(epoch, fixture, features)


def test_epoch_manifest_has_stable_semantic_and_file_hashes(epoch, tmp_path: Path) -> None:
    exact = tmp_path / "exact.toml"
    whitespace = tmp_path / "whitespace.toml"
    original = EPOCH_PATH.read_text(encoding="utf-8")
    exact.write_text(original, encoding="utf-8")
    whitespace.write_text(original + "\n", encoding="utf-8")

    exact_epoch = load_epoch(exact)
    whitespace_epoch = load_epoch(whitespace)
    assert exact_epoch.epoch_hash == epoch.epoch_hash == whitespace_epoch.epoch_hash
    assert exact_epoch.file_sha256 == epoch.file_sha256
    assert whitespace_epoch.file_sha256 != epoch.file_sha256
    assert len(epoch.epoch_hash) == len(epoch.file_sha256) == 64


def test_epoch_manifest_is_rechecked_before_every_build(tmp_path: Path) -> None:
    copied = tmp_path / "epoch.toml"
    copied.write_bytes(EPOCH_PATH.read_bytes())
    epoch = load_epoch(copied)
    copied.write_text(copied.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(M0aConfigError, match="changed after"):
        epoch.verify_unchanged()


def test_epoch_rejects_zero_or_drifted_runtime_snapshot(tmp_path: Path) -> None:
    zero = tmp_path / "zero-snapshot.toml"
    drifted = tmp_path / "drifted-snapshot.toml"
    text = EPOCH_PATH.read_text(encoding="utf-8")
    zero.write_text(
        text.replace(compute_code_snapshot_sha256(), "0" * 64),
        encoding="utf-8",
    )
    drifted.write_text(
        text.replace(compute_code_snapshot_sha256(), "f" * 64),
        encoding="utf-8",
    )
    with pytest.raises(M0aConfigError, match="zero sentinel"):
        load_epoch(zero)
    with pytest.raises(M0aConfigError, match="current M0a runtime source"):
        load_epoch(drifted)


@pytest.mark.parametrize("unsafe_key", ["holdout_path", "sealed_uri", "db_credential", "path"])
def test_epoch_rejects_any_unsafe_access_key(tmp_path: Path, unsafe_key: str) -> None:
    manifest = tmp_path / f"unsafe-{unsafe_key}.toml"
    text = EPOCH_PATH.read_text(encoding="utf-8").replace(
        'family_id = "pullback_continuation_v1"',
        f'family_id = "pullback_continuation_v1"\n{unsafe_key} = "forbidden"',
    )
    manifest.write_text(text, encoding="utf-8")
    with pytest.raises(M0aConfigError, match="forbidden research manifest key"):
        load_epoch(manifest)


def test_epoch_rejects_holdout_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEMATIC_FX_HOLDOUT_TOKEN", "must-never-enter-research")
    with pytest.raises(M0aConfigError, match="must not receive"):
        load_epoch(EPOCH_PATH)


def test_epoch_rejects_unconsumed_manifest_keys(tmp_path: Path) -> None:
    manifest = tmp_path / "unknown-key.toml"
    text = EPOCH_PATH.read_text(encoding="utf-8").replace(
        "fold_count = 3",
        "fold_count = 3\nfuture_knob = 99",
    )
    manifest.write_text(text, encoding="utf-8")
    with pytest.raises(M0aConfigError, match=r"\[evaluation\] keys differ"):
        load_epoch(manifest)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("real_candidate_budget = 12", "real_candidate_budget = 0"),
        ("null_candidate_budget = 24", "null_candidate_budget = 10001"),
    ],
)
def test_epoch_requires_finite_positive_budgets(
    tmp_path: Path,
    old: str,
    new: str,
) -> None:
    manifest = tmp_path / "bad-budget.toml"
    manifest.write_text(EPOCH_PATH.read_text(encoding="utf-8").replace(old, new), encoding="utf-8")
    with pytest.raises(M0aConfigError, match="budgets must be finite"):
        load_epoch(manifest)


def test_epoch_commits_exact_barrier_grid_and_execution(epoch) -> None:
    assert epoch.search_budget == 12
    assert epoch.null_budget == 24
    assert len(epoch.barrier_specs) == 27
    assert {(spec.k_tp_num, spec.k_tp_den) for spec in epoch.barrier_specs} == {
        (3, 4),
        (1, 1),
        (5, 4),
    }
    assert {(spec.k_sl_num, spec.k_sl_den) for spec in epoch.barrier_specs} == {
        (1, 2),
        (3, 4),
        (1, 1),
    }
    assert {spec.max_hold_seconds for spec in epoch.barrier_specs} == {1800, 3600, 7200}
    assert epoch.entry_adverse_ticks == epoch.tp_trade_through_ticks == 1
    assert epoch.code_commit == "600d77e745d08db0add6c3646c23856ef5b01c40"
    assert len(epoch.code_snapshot_sha256) == 64
    assert epoch.code_snapshot_sha256 == compute_code_snapshot_sha256()
    assert epoch.admission_rules == {
        "min_raw_events": 3,
        "min_sequential_trades": 2,
        "min_active_days": 1,
        "min_tp_probability_ppm": 500_000,
        "min_positive_folds": 1,
        "require_positive_net_ev": True,
    }
    assert epoch.family_search_space.as_dict() == {
        "trend_1h_min_ticks": [1, 2, 3, 4, 6],
        "pullback_length_min": [1, 2, 3],
        "pullback_length_max": [3, 4, 6, 8],
        "close_location_threshold_ppm": [550_000, 650_000, 750_000, 850_000],
        "volatility_quantile_min_ppm": [0, 100_000, 200_000, 300_000],
        "volatility_quantile_max_ppm": [700_000, 800_000, 900_000, 1_000_000],
        "imbalance_threshold_ppm": [None, 0, 50_000, 150_000, 250_000],
        "directions": ["long", "short"],
        "feature_tier": "M0A_MINIMAL",
        "max_generation_attempts_per_candidate": 200,
        "min_generation_attempts": 1_000,
    }
    assert epoch.evaluation_options == {
        "cooldown_seconds": 0,
        "feature_lookback_seconds": 3_600,
        "purge_policy": "MAX_HOLD_PLUS_FEATURE_LOOKBACK",
        "fold_count": 3,
        "control_block_size": 4,
        "stressed_cost_numerator": 3,
        "stressed_cost_denominator": 2,
    }
    assert epoch.daemon_options == {
        "lease_seconds": 60,
        "system_error_threshold": 3,
        "worker_restart_after_experiments": 12,
        "run_epoch_max_cycles": 1_000,
        "run_epoch_stop_when_idle": True,
        "poll_interval_milliseconds": 0,
    }


def test_epoch_family_axes_drive_the_canonical_candidate_plan(epoch) -> None:
    candidates = generate_candidates(
        budget=epoch.real_candidate_budget,
        seed=epoch.random_seeds[0],
        barriers=epoch.barrier_specs,
        family_id=epoch.family_id,
        search_space=epoch.family_search_space,
    )
    assert len(candidates) == epoch.real_candidate_budget
    for candidate in candidates:
        rule = candidate.parameters
        axes = epoch.family_search_space
        assert candidate.direction in axes.directions
        assert candidate.feature_tier == axes.feature_tier
        assert rule.trend_1h_min_ticks in axes.trend_1h_min_ticks
        assert rule.pullback_length_min in axes.pullback_length_min
        assert rule.pullback_length_max in axes.pullback_length_max
        assert rule.close_location_threshold_ppm in axes.close_location_threshold_ppm
        assert rule.volatility_quantile_min_ppm in axes.volatility_quantile_min_ppm
        assert rule.volatility_quantile_max_ppm in axes.volatility_quantile_max_ppm
        assert rule.imbalance_threshold_ppm in axes.imbalance_threshold_ppm


def test_family_axis_change_changes_epoch_and_candidate_identity(
    epoch,
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "changed-family-axis.toml"
    manifest.write_text(
        EPOCH_PATH.read_text(encoding="utf-8").replace(
            "trend_1h_min_ticks = [1, 2, 3, 4, 6]",
            "trend_1h_min_ticks = [6, 4, 3, 2, 1]",
        ),
        encoding="utf-8",
    )
    changed = load_epoch(manifest)
    original_candidates = generate_candidates(
        budget=epoch.real_candidate_budget,
        seed=epoch.random_seeds[0],
        barriers=epoch.barrier_specs,
        family_id=epoch.family_id,
        search_space=epoch.family_search_space,
    )
    changed_candidates = generate_candidates(
        budget=changed.real_candidate_budget,
        seed=changed.random_seeds[0],
        barriers=changed.barrier_specs,
        family_id=changed.family_id,
        search_space=changed.family_search_space,
    )
    assert changed.epoch_hash != epoch.epoch_hash
    assert tuple(item.candidate_hash for item in changed_candidates) != tuple(
        item.candidate_hash for item in original_candidates
    )


def test_fixture_is_content_addressed_and_round_trips(epoch, fixture) -> None:
    assert fixture.dataset_hash == fixture.content_sha256 == epoch.dataset_hash
    assert build_fixture(epoch).as_dict() == fixture.as_dict()
    assert MarketFixture.from_dict(fixture.as_dict()) == fixture
    assert len(fixture.quote_events) == 2402


def test_fixture_has_explicit_normal_roll_and_friday_metadata(fixture) -> None:
    assert len(fixture.instruments) == 2
    assert {item.symbol for item in fixture.instruments} == {"6EU22", "6EZ22"}
    assert {
        (item.tick_size_numerator, item.tick_size_denominator) for item in fixture.instruments
    } == {(1, 200_000)}
    assert all(item.expiry_ts_ns > fixture.sessions[-1].close_ts_ns for item in fixture.instruments)
    assert len(fixture.sessions) == len(fixture.previous_day_volumes) == 5
    assert fixture.sessions[-1].trading_date.weekday() == 4
    active = [session.active_instrument_id for session in fixture.sessions]
    assert active[:3] == [active[0]] * 3
    assert active[3:] == [active[-1]] * 2
    assert active[0] != active[-1]
    assert len(fixture.roll_guards) == 2


def test_active_contract_uses_strictly_previous_day_volume(fixture) -> None:
    sessions = {session.trading_date: session for session in fixture.sessions}
    for evidence in fixture.previous_day_volumes:
        assert evidence.observed_date < evidence.trading_date
        expected = min(evidence.volumes, key=lambda item: (-item[1], item[0]))[0]
        assert evidence.selected_instrument_id == expected
        assert sessions[evidence.trading_date].active_instrument_id == expected


def test_every_quote_stays_on_its_session_active_contract(fixture) -> None:
    sessions = {session.session_id: session for session in fixture.sessions}
    for event in fixture.quote_events:
        session = sessions[event.session_id]
        assert event.instrument_id == session.active_instrument_id
        assert session.open_ts_ns <= event.ts_ns < session.close_ts_ns
        assert event.ask_ticks - event.bid_ticks == 2
        assert event.bid_depth_l10 > 0 and event.ask_depth_l10 > 0
        assert event.trade_action == "TRADE"
        assert event.trade_price_ticks is not None
        assert event.trade_size > 0
        assert event.trade_aggressor_side in {"BUY", "SELL"}


def test_features_are_complete_5m_rows_with_causal_context(features, fixture) -> None:
    assert len(features) == len(fixture.sessions) * 96
    assert sum(row.feature_valid for row in features) == 394
    assert all(left.event_ts_ns < right.event_ts_ns for left, right in pairwise(features))
    assert {row.spread_ticks for row in features} == {2}
    assert all(-1_000_000 <= row.depth_imbalance_ppm <= 1_000_000 for row in features)
    for row in features:
        if row.context_30m_end_ns is not None:
            assert row.context_30m_end_ns <= row.event_ts_ns
            assert row.context_30m_end_ns % (30 * 60 * NS_PER_SECOND) == 0
        if row.context_1h_end_ns is not None:
            assert row.context_1h_end_ns <= row.event_ts_ns
            assert row.context_1h_end_ns % (60 * 60 * NS_PER_SECOND) == 0


def test_trailing_volatility_quantile_uses_prior_rows_only(epoch, features) -> None:
    by_session: dict[str, list[EventFeature]] = {}
    for row in features:
        by_session.setdefault(row.session_id, []).append(row)
    checked = 0
    for rows in by_session.values():
        for index, row in enumerate(rows):
            if row.volatility_quantile_ppm is None:
                continue
            prior = rows[index - epoch.quantile_lookback_bars : index]
            assert len(prior) == epoch.quantile_lookback_bars
            expected = (
                sum(item.volatility_ticks <= row.volatility_ticks for item in prior)
                * 1_000_000
                // len(prior)
            )
            assert row.volatility_quantile_ppm == expected
            assert row not in prior
            checked += 1
    assert checked > 0


def test_roll_cross_is_explicit_and_never_silently_valid(features, fixture) -> None:
    new_contract = fixture.sessions[3].active_instrument_id
    first_new_session = fixture.sessions[3].session_id
    roll_rows = [row for row in features if row.session_id == first_new_session and row.roll_cross]
    assert len(roll_rows) == 18
    assert all(row.instrument_id == new_contract for row in roll_rows)
    assert all(not row.feature_valid for row in roll_rows)
    assert all("ROLL_CROSS_LOOKBACK" in row.validity_flags for row in roll_rows)
    assert any(row.instrument_id == new_contract and row.feature_valid for row in features)


def test_feature_and_label_rows_round_trip(features, labels) -> None:
    assert EventFeature.from_dict(features[100].as_dict()) == features[100]
    assert QuoteAwareLabel.from_dict(labels[10_000].as_dict()) == labels[10_000]


def test_label_store_covers_full_grid_and_all_outcomes(epoch, features, labels) -> None:
    assert len(labels) == len(features) * 2 * len(epoch.barrier_specs)
    outcomes = Counter(label.first_touch_type for label in labels)
    assert set(outcomes) == {
        FirstTouchType.TP_FIRST,
        FirstTouchType.SL_FIRST,
        FirstTouchType.TIMEOUT,
        FirstTouchType.INVALID,
    }
    assert all(count > 0 for count in outcomes.values())
    assert sum(label.ambiguous for label in labels) > 0
    assert all(label.raw_fallback_used for label in labels if label.ambiguous)


def test_entry_and_exit_use_the_correct_executable_quote_side(epoch, fixture, labels) -> None:
    events = {(event.ts_ns, event.instrument_id): event for event in fixture.quote_events}
    eligible = [label for label in labels if label.eligible]
    for label in eligible[::113]:
        entry = events[(label.entry_ts_ns, label.instrument_id)]
        if label.direction is Direction.LONG:
            assert label.entry_price_ticks == entry.ask_ticks + epoch.entry_adverse_ticks
        else:
            assert label.entry_price_ticks == entry.bid_ticks - epoch.entry_adverse_ticks

        exit_event = events[(label.exit_ts_ns, label.instrument_id)]
        if label.first_touch_type is FirstTouchType.TP_FIRST:
            assert label.exit_price_ticks == label.tp_price_ticks
            if label.direction is Direction.LONG:
                assert exit_event.trade_price_ticks >= label.tp_price_ticks + 1
            else:
                assert exit_event.trade_price_ticks <= label.tp_price_ticks - 1
        elif label.first_touch_type is FirstTouchType.SL_FIRST:
            expected = (
                exit_event.bid_ticks if label.direction is Direction.LONG else exit_event.ask_ticks
            )
            assert label.exit_price_ticks == expected
        else:
            expected = (
                exit_event.bid_ticks if label.direction is Direction.LONG else exit_event.ask_ticks
            )
            assert label.exit_price_ticks == expected


def test_passive_tp_requires_trade_through_not_touch() -> None:
    touch_only = QuoteEvent(0, 1_000_000_100, 1, "s", 105, 107, 1, 1, 10, 10)
    quote_cross_without_trade = QuoteEvent(
        1,
        2_000_000_100,
        1,
        "s",
        106,
        108,
        1,
        1,
        10,
        10,
    )
    traded_through = QuoteEvent(
        2,
        3_000_000_100,
        1,
        "s",
        106,
        108,
        1,
        1,
        10,
        10,
        trade_price_ticks=106,
        trade_size=1,
        trade_action="TRADE",
        trade_aggressor_side="BUY",
    )
    assert _first_passage((touch_only,), Direction.LONG, 105, 95, 1) is None
    assert (
        _first_passage(
            (touch_only, quote_cross_without_trade),
            Direction.LONG,
            105,
            95,
            1,
        )
        is None
    )
    touch = _first_passage(
        (touch_only, quote_cross_without_trade, traded_through),
        Direction.LONG,
        105,
        95,
        1,
    )
    assert touch is not None
    assert touch.touch_type is FirstTouchType.TP_FIRST
    assert touch.exit_price_ticks == 105

    short_quote_cross = QuoteEvent(
        0,
        4_000_000_100,
        1,
        "s",
        92,
        94,
        1,
        1,
        10,
        10,
    )
    short_trade_through = QuoteEvent(
        1,
        5_000_000_100,
        1,
        "s",
        92,
        94,
        1,
        1,
        10,
        10,
        trade_price_ticks=94,
        trade_size=1,
        trade_action="TRADE",
        trade_aggressor_side="SELL",
    )
    assert _first_passage((short_quote_cross,), Direction.SHORT, 95, 105, 1) is None
    short_touch = _first_passage(
        (short_quote_cross, short_trade_through),
        Direction.SHORT,
        95,
        105,
        1,
    )
    assert short_touch is not None
    assert short_touch.touch_type is FirstTouchType.TP_FIRST
    assert short_touch.exit_price_ticks == 95


def test_same_second_ambiguity_falls_back_to_raw_sequence() -> None:
    up = QuoteEvent(
        0,
        1_000_000_100,
        1,
        "s",
        106,
        108,
        1,
        1,
        10,
        10,
        trade_price_ticks=106,
        trade_size=1,
        trade_action="TRADE",
        trade_aggressor_side="BUY",
    )
    down = QuoteEvent(
        1,
        1_000_000_200,
        1,
        "s",
        94,
        96,
        1,
        1,
        10,
        10,
        trade_price_ticks=94,
        trade_size=1,
        trade_action="TRADE",
        trade_aggressor_side="SELL",
    )
    tp_first = _first_passage((up, down), Direction.LONG, 105, 95, 1)
    assert tp_first is not None
    assert (tp_first.touch_type, tp_first.ambiguous, tp_first.raw_fallback_used) == (
        FirstTouchType.TP_FIRST,
        True,
        True,
    )

    down_first = replace(down, event_index=0, ts_ns=2_000_000_100)
    up_second = replace(up, event_index=1, ts_ns=2_000_000_200)
    sl_first = _first_passage((down_first, up_second), Direction.LONG, 105, 95, 1)
    assert sl_first is not None
    assert (sl_first.touch_type, sl_first.ambiguous, sl_first.raw_fallback_used) == (
        FirstTouchType.SL_FIRST,
        True,
        True,
    )


def test_pipeline_fixture_exercises_raw_ambiguity_order(labels) -> None:
    ambiguous = [label for label in labels if label.ambiguous]
    assert {label.first_touch_type for label in ambiguous} == {
        FirstTouchType.TP_FIRST,
        FirstTouchType.SL_FIRST,
    }
    assert all(label.first_touch_ts_ns is not None for label in ambiguous)


def test_roll_guard_and_session_close_are_preentry_invalidations(features, labels) -> None:
    inside_guard = {
        (row.event_ts_ns, row.instrument_id)
        for row in features
        if row.inside_roll_guard and row.feature_valid
    }
    assert inside_guard
    guard_labels = [
        label for label in labels if (label.event_ts_ns, label.instrument_id) in inside_guard
    ]
    assert guard_labels
    assert all(label.first_touch_type is FirstTouchType.INVALID for label in guard_labels)
    assert all(label.invalid_reason == "ROLL_GUARD" for label in guard_labels)
    assert all(label.entry_ts_ns is None and label.exit_ts_ns is None for label in guard_labels)

    close_invalid = [
        label for label in labels if label.invalid_reason == "WOULD_CROSS_SESSION_CLOSE"
    ]
    assert close_invalid
    assert all(
        label.entry_ts_ns is None and label.first_touch_ts_ns is None for label in close_invalid
    )


def test_every_eligible_trade_stays_on_entry_instrument_and_inside_session(
    fixture,
    features,
    labels,
) -> None:
    session_by_feature = {(row.event_ts_ns, row.instrument_id): row.session_id for row in features}
    sessions = {session.session_id: session for session in fixture.sessions}
    events = {(event.ts_ns, event.instrument_id): event for event in fixture.quote_events}
    for label in labels:
        if not label.eligible:
            continue
        session = sessions[session_by_feature[(label.event_ts_ns, label.instrument_id)]]
        assert label.entry_ts_ns <= label.exit_ts_ns <= session.close_ts_ns
        assert label.exit_ts_ns <= label.event_ts_ns + label.max_hold_seconds * NS_PER_SECOND
        assert events[(label.entry_ts_ns, label.instrument_id)].instrument_id == label.instrument_id
        assert events[(label.exit_ts_ns, label.instrument_id)].instrument_id == label.instrument_id


def test_precommitted_real_budget_completes_the_data_evaluation_loop(
    epoch,
    features,
    labels,
) -> None:
    result = generate_and_evaluate_epoch(
        candidate_budget=epoch.real_candidate_budget,
        candidate_seed=epoch.random_seeds[0],
        barriers=epoch.barrier_specs,
        features=features,
        labels=labels,
        admission_rules=AdmissionRules(**epoch.admission_rules),
    )
    repeated = generate_and_evaluate_epoch(
        candidate_budget=epoch.real_candidate_budget,
        candidate_seed=epoch.random_seeds[0],
        barriers=epoch.barrier_specs,
        features=features,
        labels=labels,
        admission_rules=AdmissionRules(**epoch.admission_rules),
    )

    assert result.as_dict() == repeated.as_dict()
    assert result.real_experiments_attempted == epoch.real_candidate_budget
    assert result.null_experiments_attempted == epoch.null_candidate_budget
    assert not result.failures
    assert any(item.raw_event_metrics.trade_count > 0 for item in result.evaluations)
    assert any(item.sequential_metrics.trade_count > 0 for item in result.evaluations)
    survivor = result.evaluations[8]
    assert survivor.candidate.candidate_hash == (
        "0705b02f19c67303e8379f515d29cafe03007efc748e88f84d010cf7d9d4dc7d"
    )
    assert survivor.status == "SEARCH_DATA_SURVIVOR"
    assert survivor.raw_event_metrics.trade_count == 8
    assert survivor.flat_only_metrics.trade_count == 5
    assert survivor.raw_event_metrics.trade_count > survivor.flat_only_metrics.trade_count
    assert survivor.sequential_metrics.tp_first_count == 4
    assert survivor.sequential_metrics.net_pnl_ticks == 11
    assert sum(fold.validation_metrics.net_pnl_ticks > 0 for fold in survivor.folds) >= 1
