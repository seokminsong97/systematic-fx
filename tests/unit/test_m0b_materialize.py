from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from systematic_fx.research.m0b import RealSliceError, load_real_slice_config
from systematic_fx.research.m0b.adapter import (
    _verify_feature_policy_row,
    _verify_label_policy_row,
    _verify_quote_policy_row,
    _window_by_session,
)
from systematic_fx.research.m0b.materialize import (
    _feature_rows,
    _first_passage,
    _label_rows,
    _RawSession,
    _second_rows,
    _Window,
)

CONFIG = Path(__file__).resolve().parents[2] / "configs/research/m0b_real_slice_v1.toml"

_NS = 1_000_000_000
_TICK_RAW = 50_000


def _raw(
    rows: list[tuple[int, int, int, int | None, int, bool]],
) -> _RawSession:
    """Rows are offset-ns, bid ticks, ask ticks, trade ticks, side, valid quote."""

    count = len(rows)
    trade = np.array([item[3] is not None for item in rows], dtype=np.bool_)
    return _RawSession(
        ts_ns=np.array([item[0] for item in rows], dtype=np.int64),
        sequence=np.arange(count, dtype=np.uint32),
        ordinal=np.arange(count, dtype=np.uint64),
        bid_raw=np.array([item[1] * _TICK_RAW for item in rows], dtype=np.int64),
        ask_raw=np.array([item[2] * _TICK_RAW for item in rows], dtype=np.int64),
        bid_size=np.full(count, 10, dtype=np.uint32),
        ask_size=np.full(count, 12, dtype=np.uint32),
        trade_price_raw=np.array([(item[3] or 0) * _TICK_RAW for item in rows], dtype=np.int64),
        trade_size=np.where(trade, 1, 0).astype(np.uint32),
        flags=np.zeros(count, dtype=np.uint8),
        is_trade=trade,
        is_reset=np.zeros(count, dtype=np.bool_),
        side_code=np.array([item[4] for item in rows], dtype=np.int8),
        valid_quote=np.array([item[5] for item in rows], dtype=np.bool_),
    )


def _session(base: int, *, role: str = "NORMAL") -> SimpleNamespace:
    return SimpleNamespace(
        session_id="CME_GLOBEX_6E:2022-09-01",
        trading_date=date(2022, 9, 1),
        raw_symbol="6EZ2",
        instrument_id=191026,
        role=role,
        open_ts_ns=base,
        close_ts_ns=base + 4 * 3600 * _NS,
    )


def _seconds(config: object, raw: _RawSession, session: object):
    return _second_rows(
        config,
        raw,
        session=session,
        source_manifest_sha256="a" * 64,
    )


def test_passive_tp_needs_aggressor_trade_through_not_quote_touch() -> None:
    config = load_real_slice_config(CONFIG)
    base = 1_700_000_000 * _NS
    session = _session(base)
    quote_touch = _raw(
        [
            (base + 100, 99, 100, None, 0, True),
            (base + 200, 101, 102, 102, -1, True),  # sell print cannot fill long TP
        ]
    )
    quote_rows, starts, ends = _seconds(config, quote_touch, session)
    touch = _first_passage(
        config,
        quote_touch,
        quote_rows,
        starts,
        ends,
        entry_index=0,
        horizon_ts_ns=base + _NS,
        direction="LONG",
        tp=101,
        sl=95,
    )
    assert touch[0] == "TIMEOUT"
    assert touch[1] == base + _NS

    buy_trade_through = _raw(
        [
            (base + 100, 99, 100, None, 0, True),
            (base + 200, 99, 100, 102, 1, True),
        ]
    )
    quote_rows, starts, ends = _seconds(config, buy_trade_through, session)
    touch = _first_passage(
        config,
        buy_trade_through,
        quote_rows,
        starts,
        ends,
        entry_index=0,
        horizon_ts_ns=base + _NS,
        direction="LONG",
        tp=101,
        sl=95,
    )
    assert touch[0] == "TP_FIRST"

    entry_row_trade_is_not_future_fill = _raw(
        [
            (base + 100, 99, 100, 102, 1, True),
            (base + 200, 95, 96, None, 0, True),
        ]
    )
    quote_rows, starts, ends = _seconds(config, entry_row_trade_is_not_future_fill, session)
    touch = _first_passage(
        config,
        entry_row_trade_is_not_future_fill,
        quote_rows,
        starts,
        ends,
        entry_index=0,
        horizon_ts_ns=base + _NS,
        direction="LONG",
        tp=101,
        sl=95,
    )
    assert touch[0] == "SL_FIRST"
    assert touch[1] == base + 200

    entry_book_stop_beats_entry_row_historical_trade = _raw(
        [
            (base + 100, 95, 100, 102, 1, True),
            (base + 200, 99, 100, 102, 1, True),
        ]
    )
    quote_rows, starts, ends = _seconds(
        config, entry_book_stop_beats_entry_row_historical_trade, session
    )
    touch = _first_passage(
        config,
        entry_book_stop_beats_entry_row_historical_trade,
        quote_rows,
        starts,
        ends,
        entry_index=0,
        horizon_ts_ns=base + _NS,
        direction="LONG",
        tp=101,
        sl=95,
    )
    assert touch == ("SL_FIRST", base + 100, 95, False, False)


def test_same_second_ambiguity_replays_raw_order_conservatively() -> None:
    config = load_real_slice_config(CONFIG)
    base = 1_700_000_000 * _NS
    session = _session(base)
    stop_then_tp = _raw(
        [
            (base + 100, 99, 100, None, 0, True),
            (base + 200, 98, 99, None, 0, True),
            (base + 300, 99, 100, 102, 1, True),
        ]
    )
    quote_rows, starts, ends = _seconds(config, stop_then_tp, session)
    touch = _first_passage(
        config,
        stop_then_tp,
        quote_rows,
        starts,
        ends,
        entry_index=0,
        horizon_ts_ns=base + _NS,
        direction="LONG",
        tp=101,
        sl=98,
    )
    assert touch == ("SL_FIRST", base + 200, 98, True, True)

    tp_then_stop = _raw(
        [
            (base + 100, 99, 100, None, 0, True),
            (base + 200, 99, 100, 102, 1, True),
            (base + 300, 98, 99, None, 0, True),
        ]
    )
    quote_rows, starts, ends = _seconds(config, tp_then_stop, session)
    touch = _first_passage(
        config,
        tp_then_stop,
        quote_rows,
        starts,
        ends,
        entry_index=0,
        horizon_ts_ns=base + _NS,
        direction="LONG",
        tp=101,
        sl=98,
    )
    assert touch == ("TP_FIRST", base + 200, 101, True, True)


def test_entry_sides_and_no_cross_session_are_pre_outcome_rules() -> None:
    config = load_real_slice_config(CONFIG)
    base = 1_700_000_000 * _NS
    session = _session(base)
    raw = _raw(
        [
            (base + 2 * _NS, 100, 101, None, 0, True),
            (base + 3 * _NS, 100, 101, None, 0, True),
        ]
    )
    quote_rows, starts, ends = _seconds(config, raw, session)
    feature = {
        "event_ts_ns": base,
        "instrument_id": session.instrument_id,
        "raw_symbol": session.raw_symbol,
        "session_id": session.session_id,
        "feature_valid": True,
        "volatility_ticks": 4,
    }
    contract = SimpleNamespace(
        roll_guard_start_ts_ns=base + 20 * 86400 * _NS,
        last_trade_ts_ns=base + 30 * 86400 * _NS,
    )
    labels = _label_rows(
        config,
        raw,
        quote_rows,
        starts,
        ends,
        [feature],
        session=session,
        window=_Window(base, session.close_ts_ns),
        contract=contract,
    )
    assert {item["entry_price_ticks"] for item in labels if item["direction"] == "LONG"} == {102}
    assert {item["entry_price_ticks"] for item in labels if item["direction"] == "SHORT"} == {99}
    assert all(not item["entry_eligible"] for item in labels)
    assert all(item["invalid_reason"] == "SCHEDULE_ONLY_STATUS_UNVERIFIED" for item in labels)

    near_close = {**feature, "event_ts_ns": session.close_ts_ns - 60 * _NS}
    closed = _label_rows(
        config,
        raw,
        quote_rows,
        starts,
        ends,
        [near_close],
        session=session,
        window=_Window(base, session.close_ts_ns),
        contract=contract,
    )
    assert all(item["first_touch_type"] == "INVALID" for item in closed)
    assert all(item["invalid_reason"] == "WOULD_CROSS_SESSION_CLOSE" for item in closed)


def test_feature_context_and_quantiles_are_point_in_time() -> None:
    config = load_real_slice_config(CONFIG)
    base = 1_700_000_000 * _NS
    session = _session(base, role="CONTRACT_TRANSITION_CONTEXT_NOT_ACTIVE_SELECTION")
    rows = [
        (base + index * 300 * _NS + _NS, 100 + index, 101 + index, None, 0, True)
        for index in range(48)
    ]
    raw = _raw(rows)
    features = _feature_rows(
        config,
        raw,
        [],
        session=session,
        window=_Window(base, base + 4 * 3600 * _NS),
    )
    changed_rows = [*rows]
    changed_rows[-1] = (changed_rows[-1][0], 10_000, 10_001, None, 0, True)
    changed = _feature_rows(
        config,
        _raw(changed_rows),
        [],
        session=session,
        window=_Window(base, base + 4 * 3600 * _NS),
    )
    assert features[:-1] == changed[:-1]
    for feature in features:
        assert feature["context_30m_end_ns"] is None or (
            feature["context_30m_end_ns"] <= feature["event_ts_ns"]
        )
        assert feature["context_1h_end_ns"] is None or (
            feature["context_1h_end_ns"] <= feature["event_ts_ns"]
        )
    assert features[0]["contract_transition_context"]
    assert not features[0]["active_selection_proven"]
    assert "CONTRACT_TRANSITION_CONTEXT_NOT_ACTIVE_SELECTION" in features[0]["validity_flags"]
    assert features[23]["volatility_quantile_ppm"] is not None


def _semantic_rows():
    config = load_real_slice_config(CONFIG)
    base = 1_700_000_000 * _NS
    session_open = base - config.window_start_seconds[0] * _NS
    session = SimpleNamespace(
        session_id="CME_GLOBEX_6E:2022-09-01",
        trading_date=date(2022, 9, 1),
        raw_symbol="6EZ2",
        instrument_id=191026,
        role="NORMAL",
        open_ts_ns=session_open,
        close_ts_ns=session_open + 23 * 3600 * _NS,
    )
    raw = _raw(
        [
            (base + index * 300 * _NS + _NS, 100 + index, 101 + index, None, 0, True)
            for index in range(48)
        ]
    )
    source_sha = "a" * 64
    quote_sha = "b" * 64
    feature_sha = "c" * 64
    build = SimpleNamespace(
        source_manifest=SimpleNamespace(content_sha256=source_sha),
        quote_manifest=SimpleNamespace(content_sha256=quote_sha),
        feature_manifest=SimpleNamespace(content_sha256=feature_sha),
        sessions=(session,),
    )
    window = _Window(base, base + config.window_duration_seconds * _NS)
    quotes, starts, ends = _second_rows(
        config,
        raw,
        session=session,
        source_manifest_sha256=source_sha,
    )
    features = _feature_rows(config, raw, quotes, session=session, window=window)
    for feature in features:
        feature["parent_quote_manifest_sha256"] = quote_sha
    contract = SimpleNamespace(
        roll_guard_start_ts_ns=base + 40 * 86400 * _NS,
        last_trade_ts_ns=base + 50 * 86400 * _NS,
    )
    labels = _label_rows(
        config,
        raw,
        quotes,
        starts,
        ends,
        features,
        session=session,
        window=window,
        contract=contract,
    )
    for label in labels:
        label["parent_feature_manifest_sha256"] = feature_sha
    feature = next(item for item in features if item["feature_valid"])
    label = next(
        item
        for item in labels
        if item["event_ts_ns"] == feature["event_ts_ns"] and item["mechanical_outcome_valid"]
    )
    return config, build, quotes[0], feature, label


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ({"source_manifest_sha256": "f" * 64}, "source lineage"),
        ({"second_start_ts_ns": 1}, "materialization window"),
    ),
)
def test_quote_semantics_reject_rehashed_lineage_or_window_forgery(
    mutation: dict[str, object], message: str
) -> None:
    config, build, quote, _, _ = _semantic_rows()
    windows = _window_by_session(config, build)
    with pytest.raises(RealSliceError, match=message):
        _verify_quote_policy_row(config, build, windows, {**quote, **mutation})


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ({"feature_version": "forged"}, "version"),
        ({"event_ts_ns": 1}, "decision-clock"),
    ),
)
def test_feature_semantics_reject_rehashed_version_or_timestamp_forgery(
    mutation: dict[str, object], message: str
) -> None:
    config, build, _, feature, _ = _semantic_rows()
    windows = _window_by_session(config, build)
    with pytest.raises(RealSliceError, match=message):
        _verify_feature_policy_row(config, build, windows, {**feature, **mutation})


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ({"label_version": "forged"}, "version"),
        ({"direction": "BUY"}, "direction"),
        ({"tp_price_ticks": 1}, "price direction"),
        ({"first_touch_type": "TIMEOUT", "timeout": False}, "timeout shape"),
    ),
)
def test_label_semantics_reject_rehashed_version_direction_price_or_touch_forgery(
    mutation: dict[str, object], message: str
) -> None:
    config, build, _, feature, label = _semantic_rows()
    windows = _window_by_session(config, build)
    validity = {(feature["session_id"], feature["event_ts_ns"]): True}
    with pytest.raises(RealSliceError, match=message):
        _verify_label_policy_row(
            config,
            build,
            windows,
            validity,
            {**label, **mutation},
        )


def test_timeout_label_requires_exact_horizon_exit() -> None:
    config, build, _, feature, label = _semantic_rows()
    windows = _window_by_session(config, build)
    validity = {(feature["session_id"], feature["event_ts_ns"]): True}
    timeout = {
        **label,
        "first_touch_type": "TIMEOUT",
        "first_touch_ts_ns": None,
        "timeout": True,
        "exit_ts_ns": label["event_ts_ns"] + label["max_hold_seconds"] * _NS - 1,
    }
    with pytest.raises(RealSliceError, match="timeout shape"):
        _verify_label_policy_row(config, build, windows, validity, timeout)
