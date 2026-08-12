from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from systematic_fx.research.m0a import (
    FirstTouchType,
    MarketFixture,
    build_features,
    build_fixture,
    build_labels,
    load_epoch,
)

EPOCH_PATH = Path(__file__).resolve().parents[2] / "epochs" / "m0a_fixture_v1.toml"


def _content_addressed(fixture: MarketFixture, **changes: object) -> MarketFixture:
    draft = replace(fixture, dataset_hash="0" * 64, **changes)
    return replace(draft, dataset_hash=draft.content_sha256)


def _epoch_for_dataset(tmp_path: Path, dataset_hash: str):
    manifest = tmp_path / f"epoch-{dataset_hash[:12]}.toml"
    text = EPOCH_PATH.read_text(encoding="utf-8")
    original = load_epoch(EPOCH_PATH).dataset_hash
    manifest.write_text(text.replace(original, dataset_hash), encoding="utf-8")
    return load_epoch(manifest)


def test_future_quote_changes_cannot_change_past_feature_rows(tmp_path: Path) -> None:
    epoch = load_epoch(EPOCH_PATH)
    fixture = build_fixture(epoch)
    baseline = build_features(epoch, fixture)
    cutoff = fixture.sessions[0].open_ts_ns + 4 * 60 * 60 * 1_000_000_000

    mutated_events = tuple(
        replace(
            event,
            bid_ticks=event.bid_ticks + 200,
            ask_ticks=event.ask_ticks + 200,
            bid_depth_l10=event.bid_depth_l10 + 500,
        )
        if event.ts_ns > cutoff
        else event
        for event in fixture.quote_events
    )
    mutated_fixture = _content_addressed(fixture, quote_events=mutated_events)
    mutated_epoch = _epoch_for_dataset(tmp_path, mutated_fixture.dataset_hash)
    mutated = build_features(mutated_epoch, mutated_fixture)

    baseline_past = [row.as_dict() for row in baseline if row.event_ts_ns <= cutoff]
    mutated_past = [row.as_dict() for row in mutated if row.event_ts_ns <= cutoff]
    assert baseline_past
    assert mutated_past == baseline_past
    assert any(
        left.as_dict() != right.as_dict()
        for left, right in zip(baseline, mutated)
        if left.event_ts_ns > cutoff
    )


def test_future_contract_volume_evidence_cannot_change_prior_features(tmp_path: Path) -> None:
    epoch = load_epoch(EPOCH_PATH)
    fixture = build_fixture(epoch)
    baseline = build_features(epoch, fixture)
    future = fixture.previous_day_volumes[-1]
    winner = future.selected_instrument_id
    loser = next(instrument for instrument, _ in future.volumes if instrument != winner)
    changed_future = replace(future, volumes=((loser, 1), (winner, 99_999)))
    evidence = (*fixture.previous_day_volumes[:-1], changed_future)
    mutated_fixture = _content_addressed(fixture, previous_day_volumes=evidence)
    mutated_epoch = _epoch_for_dataset(tmp_path, mutated_fixture.dataset_hash)
    mutated = build_features(mutated_epoch, mutated_fixture)
    future_session_open = fixture.sessions[-1].open_ts_ns

    assert [row.as_dict() for row in baseline if row.event_ts_ns < future_session_open] == [
        row.as_dict() for row in mutated if row.event_ts_ns < future_session_open
    ]
    assert all(item.observed_date < item.trading_date for item in evidence)


def test_completed_context_never_reads_an_open_30m_or_1h_candle() -> None:
    epoch = load_epoch(EPOCH_PATH)
    fixture = build_fixture(epoch)
    features = build_features(epoch, fixture)
    first = fixture.sessions[0]
    rows = [row for row in features if row.session_id == first.session_id]

    # 5m decisions 13:05..13:25 cannot see a completed 30m context.
    assert all(row.context_30m_end_ns is None for row in rows[:5])
    assert rows[5].context_30m_end_ns == rows[5].event_ts_ns
    # 13:05..13:55 cannot see a completed 1h context; 14:00 can.
    assert all(row.context_1h_end_ns is None for row in rows[:11])
    assert rows[11].context_1h_end_ns == rows[11].event_ts_ns
    assert all(
        row.context_30m_end_ns is None or row.context_30m_end_ns <= row.event_ts_ns
        for row in features
    )
    assert all(
        row.context_1h_end_ns is None or row.context_1h_end_ns <= row.event_ts_ns
        for row in features
    )


def test_trailing_quantile_window_excludes_current_and_future_rows() -> None:
    epoch = load_epoch(EPOCH_PATH)
    fixture = build_fixture(epoch)
    features = build_features(epoch, fixture)
    first_session = fixture.sessions[0].session_id
    rows = [row for row in features if row.session_id == first_session]
    index = next(index for index, row in enumerate(rows) if row.volatility_quantile_ppm is not None)
    row = rows[index]
    prior = rows[index - epoch.quantile_lookback_bars : index]

    assert len(prior) == epoch.quantile_lookback_bars
    assert all(item.event_ts_ns < row.event_ts_ns for item in prior)
    expected = (
        sum(item.volatility_ticks <= row.volatility_ticks for item in prior)
        * 1_000_000
        // epoch.quantile_lookback_bars
    )
    assert row.volatility_quantile_ppm == expected


def test_session_close_exclusion_is_decided_without_outcome_data(tmp_path: Path) -> None:
    epoch = load_epoch(EPOCH_PATH)
    fixture = build_fixture(epoch)
    features = build_features(epoch, fixture)
    friday = fixture.sessions[-1]
    target = next(
        row
        for row in features
        if row.session_id == friday.session_id
        and row.feature_valid
        and row.event_ts_ns > friday.close_ts_ns - 60 * 60 * 1_000_000_000
    )
    original = build_labels(epoch, fixture, (target,))
    original_crossing = tuple(
        label
        for label in original
        if label.max_hold_seconds == 7200 and label.invalid_reason == "WOULD_CROSS_SESSION_CLOSE"
    )
    assert len(original_crossing) == 18

    mutated_events = tuple(
        replace(event, bid_ticks=event.bid_ticks - 500, ask_ticks=event.ask_ticks + 500)
        if target.event_ts_ns < event.ts_ns < friday.close_ts_ns
        else event
        for event in fixture.quote_events
    )
    mutated_fixture = _content_addressed(fixture, quote_events=mutated_events)
    mutated_epoch = _epoch_for_dataset(tmp_path, mutated_fixture.dataset_hash)
    mutated = build_labels(mutated_epoch, mutated_fixture, (target,))
    mutated_crossing = tuple(
        label
        for label in mutated
        if label.max_hold_seconds == 7200 and label.invalid_reason == "WOULD_CROSS_SESSION_CLOSE"
    )

    assert tuple(label.as_dict() for label in mutated_crossing) == tuple(
        label.as_dict() for label in original_crossing
    )
    assert all(label.first_touch_type is FirstTouchType.INVALID for label in mutated_crossing)
    assert all(label.entry_ts_ns is None for label in mutated_crossing)


def test_roll_transition_is_flagged_and_instrument_never_blends() -> None:
    epoch = load_epoch(EPOCH_PATH)
    fixture = build_fixture(epoch)
    features = build_features(epoch, fixture)
    labels = build_labels(epoch, fixture, features)
    old_instrument = fixture.sessions[2].active_instrument_id
    new_instrument = fixture.sessions[3].active_instrument_id
    switch_ts = fixture.sessions[3].open_ts_ns

    before = [row for row in features if row.event_ts_ns <= switch_ts]
    after = [row for row in features if row.event_ts_ns > switch_ts]
    assert before[-1].instrument_id == old_instrument
    assert after[0].instrument_id == new_instrument
    assert after[0].roll_cross
    assert not after[0].feature_valid

    events = {(event.ts_ns, event.instrument_id): event for event in fixture.quote_events}
    for label in labels:
        if not label.eligible:
            continue
        assert events[(label.entry_ts_ns, label.instrument_id)].instrument_id == label.instrument_id
        assert events[(label.exit_ts_ns, label.instrument_id)].instrument_id == label.instrument_id
