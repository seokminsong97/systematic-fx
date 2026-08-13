"""Bounded adapter plan and invariant verification for real CME 6E MBP-10."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.m0b.config import (
    RealSliceConfig,
    _resolve_existing_search_path,
    canonical_real_slice_config,
)
from systematic_fx.research.m0b.model import (
    ArtifactIdentity,
    RealSliceBuild,
    RealSliceError,
    SessionSlice,
)

_NS = 1_000_000_000
_QUOTE_KEYS = {
    "aggressor_buy_trade_max_ticks",
    "aggressor_sell_trade_min_ticks",
    "artifact_schema",
    "ask_size_l1",
    "ask_ticks",
    "bid_size_l1",
    "bid_ticks",
    "event_count",
    "instrument_id",
    "max_ask_ticks",
    "min_bid_ticks",
    "raw_first_ordinal",
    "raw_last_ordinal",
    "raw_order_available",
    "raw_symbol",
    "research_eligible",
    "second_start_ts_ns",
    "session_id",
    "source_manifest_sha256",
    "status_coverage",
    "trading_date",
    "valid_quote_count",
}
_FEATURE_KEYS = {
    "active_selection_proven",
    "artifact_schema",
    "bar_close_ticks",
    "bar_high_ticks",
    "bar_low_ticks",
    "bar_open_ticks",
    "context_1h_end_ns",
    "context_30m_end_ns",
    "contract_transition_context",
    "depth_imbalance_ppm",
    "event_ts_ns",
    "feature_valid",
    "feature_version",
    "instrument_id",
    "parent_quote_manifest_sha256",
    "range_ticks",
    "raw_symbol",
    "research_eligible",
    "role",
    "roll_cross",
    "session_id",
    "short_trend_ticks",
    "spread_ticks",
    "status_coverage",
    "trading_date",
    "trend_1h_ticks",
    "trend_30m_ticks",
    "validity_flags",
    "volatility_quantile_ppm",
    "volatility_ticks",
}
_LABEL_KEYS = {
    "ambiguous",
    "artifact_schema",
    "barrier_id",
    "cost_ticks",
    "direction",
    "entry_eligible",
    "entry_price_ticks",
    "entry_ts_ns",
    "event_ts_ns",
    "exit_price_ticks",
    "exit_ts_ns",
    "first_touch_ts_ns",
    "first_touch_type",
    "gross_pnl_ticks",
    "instrument_id",
    "invalid_reason",
    "k_sl_den",
    "k_sl_num",
    "k_tp_den",
    "k_tp_num",
    "label_version",
    "max_hold_seconds",
    "mechanical_outcome_valid",
    "net_pnl_ticks",
    "parent_feature_manifest_sha256",
    "raw_fallback_used",
    "raw_symbol",
    "research_context_only",
    "session_id",
    "sl_price_ticks",
    "status_coverage",
    "timeout",
    "tp_price_ticks",
}
_FEATURE_FLAGS = {
    "CONTRACT_TRANSITION_CONTEXT_NOT_ACTIVE_SELECTION",
    "INSUFFICIENT_ATR_HISTORY",
    "INSUFFICIENT_PRIOR_QUANTILE_HISTORY",
    "INSUFFICIENT_SHORT_TREND_HISTORY",
    "MISSING_COMPLETED_1H_CONTEXT",
    "MISSING_COMPLETED_30M_CONTEXT",
    "NO_VALID_QUOTES_IN_BAR",
}
_INVALID_REASONS = {
    "DELIVERY_OR_EXPIRY_GUARD",
    "INVALID_FEATURE",
    "MATERIALIZATION_WINDOW_INCOMPLETE",
    "NO_ELIGIBLE_ENTRY_QUOTE",
    "ROLL_GUARD",
    "WOULD_CROSS_ROLL_GUARD",
    "WOULD_CROSS_SESSION_CLOSE",
}


def _source_payload(
    cfg: RealSliceConfig,
    reference_sha256: str,
    sessions: tuple[SessionSlice, ...],
) -> dict[str, Any]:
    return {
        "artifact_schema": "systematic_fx.m0b_real_source_manifest.v1",
        "adapter_version": cfg.source_adapter_version,
        "config_hash": cfg.config_hash,
        "reference_sha256": reference_sha256,
        "source_registry_sha256": cfg.source_manifest_sha256,
        "sources": [item.as_dict() for item in cfg.sources],
        "sessions": [item.as_dict() for item in sessions],
        "previous_source_volume_context": [
            item.as_dict() for item in cfg.previous_source_volume_context
        ],
        "authority": cfg.research_authority,
    }


def _ts_ns(value: Any, *, label: str) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, datetime) and value.tzinfo is not None:
        return int(value.astimezone(UTC).timestamp() * 1_000_000_000)
    raise RealSliceError(f"reference {label} must be an aware datetime or integer ns")


def _intersecting_source_dates(open_ns: int, close_ns: int) -> tuple[date, ...]:
    if open_ns >= close_ns:
        raise RealSliceError("reference session must have a positive span")
    first = datetime.fromtimestamp(open_ns // 1_000_000_000, tz=UTC).date()
    last = datetime.fromtimestamp((close_ns - 1) // 1_000_000_000, tz=UTC).date()
    values: list[date] = []
    cursor = first
    while cursor <= last:
        values.append(cursor)
        cursor += timedelta(days=1)
    return tuple(values)


def _reference_session(reference: Any, trading_date: date) -> Any:
    try:
        return reference.session_for(trading_date)
    except Exception as error:
        raise RealSliceError(f"CME reference has no verified session for {trading_date}") from error


def _reference_contract(reference: Any, raw_symbol: str, trading_date: date) -> Any:
    try:
        return reference.contract(raw_symbol, as_of_date=trading_date)
    except Exception as error:
        raise RealSliceError(
            f"CME reference has no point-in-time contract {raw_symbol} on {trading_date}"
        ) from error


def build_real_slice(
    config: RealSliceConfig | str | Path,
    *,
    reference: Any | None = None,
) -> RealSliceBuild:
    """Build immutable source/feature/label plans for the exact allowlisted slice.

    This API deliberately does not scan a directory or open a database.  Raw
    MBP-10 materialization is a subsequent bounded streaming step; its absence
    is represented by zero row counts rather than silently treating a cache gap
    as an empty market session.
    """

    cfg = canonical_real_slice_config(config)
    cfg.verify_unchanged()
    if any(cfg.active_selection_proven):
        raise RealSliceError("schedule-only M0b cannot assert an active execution contract")
    if reference is None:
        try:
            from systematic_fx.data.cme_reference import load_cme_6e_reference
        except ImportError as error:  # pragma: no cover - integration lands independently
            raise RealSliceError("CME reference adapter is not installed") from error
        project_root = cfg.manifest_path.parents[2]
        reference_path = _resolve_existing_search_path(
            project_root / cfg.reference_config,
            label="CME reference config",
            kind="file",
        )
        if not reference_path.is_relative_to(project_root):
            raise RealSliceError("CME reference config escaped the project root")
        reference = load_cme_6e_reference(reference_path)

    sessions: list[SessionSlice] = []
    allowlisted = set(cfg.source_dates)
    for trading_date, role, symbol, instrument_id, cache in zip(
        cfg.trading_dates,
        cfg.roles,
        cfg.expected_contracts,
        cfg.expected_instrument_ids,
        cfg.cache_expectations,
        strict=True,
    ):
        session = _reference_session(reference, trading_date)
        contract = _reference_contract(reference, symbol, trading_date)
        session_date = getattr(session, "trading_date", None)
        session_id = str(getattr(session, "session_id", ""))
        if session_date != trading_date or not session_id:
            raise RealSliceError("CME reference session identity differs from the allowlist")
        open_ns = _ts_ns(getattr(session, "open_ts_ns", None), label="open_ts_ns")
        close_ns = _ts_ns(getattr(session, "close_ts_ns", None), label="close_ts_ns")
        sources = _intersecting_source_dates(open_ns, close_ns)
        if not set(sources) <= allowlisted:
            raise RealSliceError("session intersects a raw source outside the exact allowlist")
        if getattr(contract, "raw_symbol", None) != symbol:
            raise RealSliceError("reference contract differs from the selected execution contract")
        tick_num = int(getattr(contract, "tick_size_numerator", 0))
        tick_den = int(getattr(contract, "tick_size_denominator", 0))
        if (tick_num, tick_den) != (1, 20_000):
            raise RealSliceError("reference 6E tick size differs from the raw 50000 grid")
        if role == "FRIDAY" and trading_date.weekday() != 4:
            raise RealSliceError("FRIDAY role is not a Friday")
        sessions.append(
            SessionSlice(
                trading_date=trading_date,
                role=role,
                session_id=session_id,
                open_ts_ns=open_ns,
                close_ts_ns=close_ns,
                raw_symbol=symbol,
                instrument_id=instrument_id,
                source_dates=sources,
                cache_status=cache.status,
                active_selection_proven=cfg.active_selection_proven[len(sessions)],
            )
        )

    reference_hash = str(getattr(reference, "sha256", ""))
    if len(reference_hash) != 64:
        raise RealSliceError("CME reference must expose a content SHA-256")
    if reference_hash != cfg.reference_config_sha256:
        raise RealSliceError("CME reference bytes differ from the immutable slice manifest")
    session_tuple = tuple(sessions)
    source_payload = _source_payload(cfg, reference_hash, session_tuple)
    source_hash = canonical_sha256(source_payload)
    quote_payload = {
        "artifact_schema": "systematic_fx.m0b_real_quote_1s_manifest.v1",
        "source_adapter_version": cfg.source_adapter_version,
        "parent_source_manifest_sha256": source_hash,
        "actual_trade_extrema_preserved": True,
        "raw_event_order_fallback_required": True,
        "row_state": "STAGED_NOT_MATERIALIZED",
    }
    quote_hash = canonical_sha256(quote_payload)
    feature_payload = {
        "artifact_schema": "systematic_fx.m0b_real_feature_manifest.v1",
        "feature_version": cfg.feature_version,
        "parent_quote_manifest_sha256": quote_hash,
        "row_state": "STAGED_NOT_MATERIALIZED",
    }
    feature_hash = canonical_sha256(feature_payload)
    label_payload = {
        "artifact_schema": "systematic_fx.m0b_real_label_manifest.v1",
        "label_version": cfg.label_version,
        "execution_model_version": cfg.execution_model_version,
        "parent_feature_manifest_sha256": feature_hash,
        "actual_trade_through_required": True,
        "raw_event_ambiguity_fallback_required": True,
        "row_state": "STAGED_NOT_MATERIALIZED",
    }
    return RealSliceBuild(
        slice_id=cfg.slice_id,
        config_hash=cfg.config_hash,
        source_manifest=ArtifactIdentity("SOURCE", len(cfg.sources), source_hash, None),
        quote_manifest=ArtifactIdentity("QUOTE_1S", 0, quote_hash, source_hash),
        feature_manifest=ArtifactIdentity("FEATURE", 0, feature_hash, quote_hash),
        label_manifest=ArtifactIdentity("LABEL", 0, canonical_sha256(label_payload), feature_hash),
        sessions=session_tuple,
    )


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def verify_real_slice(
    build: RealSliceBuild,
    config: RealSliceConfig | str | Path,
    *,
    data_root: str | Path,
    verify_source_bytes: bool = True,
    staged_root: str | Path | None = None,
) -> None:
    """Verify chain/session/contract invariants and optionally exact raw bytes."""

    cfg = canonical_real_slice_config(config)
    cfg.verify_unchanged()
    try:
        from systematic_fx.data.cme_reference import load_cme_6e_reference
    except ImportError as error:  # pragma: no cover
        raise RealSliceError("CME reference adapter is not installed") from error
    project_root = cfg.manifest_path.parents[2]
    reference_path = _resolve_existing_search_path(
        project_root / cfg.reference_config,
        label="CME reference config",
        kind="file",
    )
    if not reference_path.is_relative_to(project_root):
        raise RealSliceError("CME reference config escaped the project root")
    canonical_reference = load_cme_6e_reference(reference_path)
    canonical_plan = build_real_slice(cfg, reference=canonical_reference)
    if not build.search_only or not build.sealed_holdout_untouched:
        raise RealSliceError("real slice exceeded search-only authority")
    if build.config_hash != cfg.config_hash:
        raise RealSliceError("real-slice build belongs to a different config")
    if build.quote_manifest.parent_sha256 != build.source_manifest.content_sha256:
        raise RealSliceError("quote manifest lineage is broken")
    if build.feature_manifest.parent_sha256 != build.quote_manifest.content_sha256:
        raise RealSliceError("feature manifest lineage is broken")
    if build.label_manifest.parent_sha256 != build.feature_manifest.content_sha256:
        raise RealSliceError("label manifest lineage is broken")
    if build.slice_id != canonical_plan.slice_id or build.sessions != canonical_plan.sessions:
        raise RealSliceError("staged sessions differ from the canonical CME reference plan")
    if build.source_manifest.content_sha256 != canonical_plan.source_manifest.content_sha256:
        raise RealSliceError("source manifest differs from the canonical reference plan")
    for session in build.sessions:
        if session.open_ts_ns >= session.close_ts_ns:
            raise RealSliceError("session has non-positive duration")
        if not set(session.source_dates) <= set(cfg.source_dates):
            raise RealSliceError("session escaped the source allowlist")
    root = _resolve_existing_search_path(data_root, label="data_root", kind="directory")
    if verify_source_bytes:
        mbp_root = _resolve_existing_search_path(
            root / "mbp-10", label="MBP-10 root", kind="directory"
        )
        for source in cfg.sources:
            resolved = _resolve_existing_search_path(
                mbp_root / source.relative_uri,
                label="raw source",
                kind="file",
            )
            if not resolved.is_relative_to(mbp_root):
                raise RealSliceError("raw source path escaped the MBP-10 root")
            if _file_sha256(resolved) != source.sha256:
                raise RealSliceError(f"raw source SHA-256 drift: {source.source_date}")
    identities = (
        build.source_manifest,
        build.quote_manifest,
        build.feature_manifest,
        build.label_manifest,
    )
    if tuple(item.artifact_type for item in identities) != (
        "SOURCE",
        "QUOTE_1S",
        "FEATURE",
        "LABEL",
    ):
        raise RealSliceError("real-slice artifact types or order drifted")
    if build.source_manifest.row_count != len(cfg.sources):
        raise RealSliceError("source artifact cardinality differs from the frozen allowlist")
    expected_feature_rows = len(build.sessions) * (
        cfg.window_duration_seconds // cfg.decision_clock_seconds
    )
    expected_label_rows = (
        expected_feature_rows
        * 2
        * len(cfg.barrier_k_tp_numerators)
        * len(cfg.barrier_k_sl_numerators)
        * len(cfg.max_hold_seconds)
    )
    materialized = build.quote_manifest.row_count > 0
    if materialized:
        if (
            build.feature_manifest.row_count != expected_feature_rows
            or build.label_manifest.row_count != expected_label_rows
        ):
            raise RealSliceError("materialized feature or label cardinality drifted")
    elif any(item.row_count for item in identities[1:]) or any(
        item.relative_uri is not None for item in identities
    ):
        raise RealSliceError("partially materialized real-slice build is forbidden")
    if materialized:
        if staged_root is None:
            raise RealSliceError("materialized verification requires the explicit staged_root")
        staged = _resolve_existing_search_path(staged_root, label="staged_root", kind="directory")
        artifact_paths: dict[str, Path] = {}
        for identity, prefix, suffix in zip(
            identities,
            ("source", "quote", "feature", "label"),
            ("json", "jsonl", "jsonl", "jsonl"),
            strict=True,
        ):
            expected_uri = f"{prefix}-{identity.content_sha256}.{suffix}"
            if identity.relative_uri != expected_uri:
                raise RealSliceError("materialized artifact URI is not its canonical leaf")
            requested_artifact = staged / identity.relative_uri
            artifact = _resolve_existing_search_path(
                requested_artifact,
                label=f"{identity.artifact_type} artifact",
                kind="file",
            )
            if not artifact.is_relative_to(staged):
                raise RealSliceError("materialized artifact escaped staged_root")
            if _file_sha256(artifact) != identity.content_sha256:
                raise RealSliceError(f"{identity.artifact_type} artifact SHA-256 drift")
            artifact_paths[identity.artifact_type] = artifact
            if identity.artifact_type != "SOURCE":
                with artifact.open("rb") as handle:
                    rows = sum(1 for _ in handle)
                if rows != identity.row_count:
                    raise RealSliceError(f"{identity.artifact_type} artifact row count drift")
        _verify_materialized_policy_rows(cfg, build, artifact_paths)


def _integer(value: object, *, label: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RealSliceError(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        raise RealSliceError(f"{label} is below its semantic bound")
    return value


def _optional_integer(value: object, *, label: str) -> int | None:
    return None if value is None else _integer(value, label=label)


def _exact_row(row: object, keys: set[str], *, artifact_type: str) -> dict[str, Any]:
    if not isinstance(row, dict) or set(row) != keys:
        raise RealSliceError(f"{artifact_type} row keys differ from the frozen schema")
    return row


def _window_by_session(
    cfg: RealSliceConfig, build: RealSliceBuild
) -> dict[str, tuple[SessionSlice, int, int]]:
    result: dict[str, tuple[SessionSlice, int, int]] = {}
    for index, session in enumerate(build.sessions):
        opened = session.open_ts_ns + cfg.window_start_seconds[index] * _NS
        closed = opened + cfg.window_duration_seconds * _NS
        if closed > session.close_ts_ns:
            raise RealSliceError("materialized window crosses its canonical session")
        result[session.session_id] = (session, opened, closed)
    if len(result) != len(build.sessions):
        raise RealSliceError("canonical sessions repeat a session_id")
    return result


def _session_row(
    row: dict[str, Any],
    windows: dict[str, tuple[SessionSlice, int, int]],
    *,
    include_role: bool,
    include_trading_date: bool,
) -> tuple[SessionSlice, int, int]:
    session_id = row.get("session_id")
    try:
        session, opened, closed = windows[str(session_id)]
    except KeyError as error:
        raise RealSliceError("artifact row names a non-canonical session") from error
    if (
        _integer(row.get("instrument_id"), label="artifact instrument_id") != session.instrument_id
        or row.get("session_id") != session.session_id
        or row.get("raw_symbol") != session.raw_symbol
        or (include_role and row.get("role") != session.role)
        or (include_trading_date and row.get("trading_date") != session.trading_date.isoformat())
    ):
        raise RealSliceError("artifact row session/contract identity drifted")
    return session, opened, closed


def _verify_quote_policy_row(
    cfg: RealSliceConfig,
    build: RealSliceBuild,
    windows: dict[str, tuple[SessionSlice, int, int]],
    row: object,
) -> tuple[int, int]:
    value = _exact_row(row, _QUOTE_KEYS, artifact_type="QUOTE_1S")
    if value["artifact_schema"] != "systematic_fx.m0b_quote_second.v1":
        raise RealSliceError("QUOTE_1S semantic schema drift")
    session, opened, closed = _session_row(
        value, windows, include_role=False, include_trading_date=True
    )
    timestamp = _integer(value["second_start_ts_ns"], label="quote timestamp")
    if timestamp % _NS or not opened <= timestamp < closed:
        raise RealSliceError("QUOTE_1S timestamp escaped its exact materialization window")
    if (
        value["source_manifest_sha256"] != build.source_manifest.content_sha256
        or value["status_coverage"] is not False
        or value["research_eligible"] is not False
        or value["raw_order_available"] is not True
    ):
        raise RealSliceError("QUOTE_1S authority or source lineage drift")
    event_count = _integer(value["event_count"], label="quote event_count", minimum=1)
    valid_count = _integer(value["valid_quote_count"], label="quote valid_quote_count", minimum=0)
    if valid_count > event_count:
        raise RealSliceError("QUOTE_1S valid quote count exceeds its raw event count")
    first_ordinal = _integer(value["raw_first_ordinal"], label="quote raw_first_ordinal", minimum=0)
    last_ordinal = _integer(
        value["raw_last_ordinal"], label="quote raw_last_ordinal", minimum=first_ordinal
    )
    bid = _optional_integer(value["bid_ticks"], label="quote bid")
    ask = _optional_integer(value["ask_ticks"], label="quote ask")
    bid_size = _optional_integer(value["bid_size_l1"], label="quote bid size")
    ask_size = _optional_integer(value["ask_size_l1"], label="quote ask size")
    minimum_bid = _optional_integer(value["min_bid_ticks"], label="quote minimum bid")
    maximum_ask = _optional_integer(value["max_ask_ticks"], label="quote maximum ask")
    _optional_integer(value["aggressor_buy_trade_max_ticks"], label="quote aggressor-buy maximum")
    _optional_integer(value["aggressor_sell_trade_min_ticks"], label="quote aggressor-sell minimum")
    quote_shape = (bid, ask, bid_size, ask_size, minimum_bid, maximum_ask)
    if valid_count == 0:
        if any(item is not None for item in quote_shape):
            raise RealSliceError("QUOTE_1S empty quote aggregate carries quote state")
    elif (
        any(item is None for item in quote_shape)
        or int(bid) >= int(ask)
        or int(bid_size) < 0
        or int(ask_size) < 0
        or int(minimum_bid) > int(bid)
        or int(maximum_ask) < int(ask)
    ):
        raise RealSliceError("QUOTE_1S bid/ask aggregate shape is invalid")
    del cfg, last_ordinal
    return build.sessions.index(session), timestamp


def _verify_feature_policy_row(
    cfg: RealSliceConfig,
    build: RealSliceBuild,
    windows: dict[str, tuple[SessionSlice, int, int]],
    row: object,
) -> tuple[tuple[str, int], bool]:
    value = _exact_row(row, _FEATURE_KEYS, artifact_type="FEATURE")
    if value["artifact_schema"] != "systematic_fx.m0b_event_feature.v1":
        raise RealSliceError("FEATURE semantic schema drift")
    session, opened, closed = _session_row(
        value, windows, include_role=True, include_trading_date=True
    )
    timestamp = _integer(value["event_ts_ns"], label="feature event timestamp")
    width = cfg.decision_clock_seconds * _NS
    if not opened < timestamp <= closed or (timestamp - opened) % width:
        raise RealSliceError("FEATURE timestamp is not an exact decision-clock event")
    transition = session.role == "CONTRACT_TRANSITION_CONTEXT_NOT_ACTIVE_SELECTION"
    if (
        value["feature_version"] != cfg.feature_version
        or value["parent_quote_manifest_sha256"] != build.quote_manifest.content_sha256
        or value["status_coverage"] is not False
        or value["research_eligible"] is not False
        or value["active_selection_proven"] is not False
        or value["roll_cross"] is not False
        or value["contract_transition_context"] is not transition
    ):
        raise RealSliceError("FEATURE authority, version, lineage, or roll state drift")
    flags = value["validity_flags"]
    if (
        not isinstance(flags, list)
        or any(not isinstance(item, str) or item not in _FEATURE_FLAGS for item in flags)
        or len(flags) != len(set(flags))
        or not isinstance(value["feature_valid"], bool)
        or value["feature_valid"] is not (not flags)
    ):
        raise RealSliceError("FEATURE validity flags are inconsistent")
    prices = tuple(
        _optional_integer(value[key], label=f"feature {key}")
        for key in ("bar_open_ticks", "bar_high_ticks", "bar_low_ticks", "bar_close_ticks")
    )
    bar_range = _optional_integer(value["range_ticks"], label="feature range")
    spread = _optional_integer(value["spread_ticks"], label="feature spread")
    imbalance = _optional_integer(value["depth_imbalance_ppm"], label="feature depth imbalance")
    if all(item is None for item in prices):
        if bar_range is not None or spread is not None or imbalance is not None:
            raise RealSliceError("FEATURE empty bar carries price-derived state")
    elif any(item is None for item in prices):
        raise RealSliceError("FEATURE OHLC state is partial")
    else:
        opened_price, high, low, closed_price = (int(item) for item in prices)
        if (
            low > min(opened_price, closed_price)
            or high < max(opened_price, closed_price)
            or bar_range != high - low
            or spread is None
            or spread <= 0
            or imbalance is None
            or not -1_000_000 <= imbalance <= 1_000_000
        ):
            raise RealSliceError("FEATURE OHLC/range/spread shape is invalid")
    _integer(value["volatility_ticks"], label="feature volatility", minimum=1)
    quantile = _optional_integer(
        value["volatility_quantile_ppm"], label="feature volatility quantile"
    )
    if quantile is not None and not 0 <= quantile <= 1_000_000:
        raise RealSliceError("FEATURE volatility quantile is outside [0, 1]")
    for key in ("short_trend_ticks", "trend_30m_ticks", "trend_1h_ticks"):
        _optional_integer(value[key], label=f"feature {key}")
    for key in ("context_30m_end_ns", "context_1h_end_ns"):
        context_end = _optional_integer(value[key], label=f"feature {key}")
        if context_end is not None and not opened < context_end <= timestamp:
            raise RealSliceError("FEATURE context timestamp is not point-in-time bounded")
    return (session.session_id, timestamp), bool(value["feature_valid"])


def _verify_label_policy_row(
    cfg: RealSliceConfig,
    build: RealSliceBuild,
    windows: dict[str, tuple[SessionSlice, int, int]],
    feature_validity: dict[tuple[str, int], bool],
    row: object,
) -> tuple[str, int, str, int, int, int]:
    value = _exact_row(row, _LABEL_KEYS, artifact_type="LABEL")
    if value["artifact_schema"] != "systematic_fx.m0b_quote_label.v1":
        raise RealSliceError("LABEL semantic schema drift")
    session, opened, closed = _session_row(
        value, windows, include_role=False, include_trading_date=False
    )
    event_ts = _integer(value["event_ts_ns"], label="label event timestamp")
    feature_key = (session.session_id, event_ts)
    if feature_key not in feature_validity or not opened < event_ts <= closed:
        raise RealSliceError("LABEL event has no exact feature parent")
    direction = value["direction"]
    if direction not in {"LONG", "SHORT"}:
        raise RealSliceError("LABEL direction is not LONG or SHORT")
    k_tp = _integer(value["k_tp_num"], label="label k_tp_num")
    k_sl = _integer(value["k_sl_num"], label="label k_sl_num")
    hold = _integer(value["max_hold_seconds"], label="label max_hold_seconds")
    if (
        k_tp not in cfg.barrier_k_tp_numerators
        or k_sl not in cfg.barrier_k_sl_numerators
        or hold not in cfg.max_hold_seconds
        or value["k_tp_den"] != cfg.barrier_k_tp_denominator
        or value["k_sl_den"] != cfg.barrier_k_sl_denominator
        or value["barrier_id"]
        != (
            f"tp{k_tp}of{cfg.barrier_k_tp_denominator}_"
            f"sl{k_sl}of{cfg.barrier_k_sl_denominator}_h{hold}"
        )
    ):
        raise RealSliceError("LABEL barrier specification escaped the frozen grid")
    _integer(value["k_tp_den"], label="label k_tp_den")
    _integer(value["k_sl_den"], label="label k_sl_den")
    _integer(value["cost_ticks"], label="label cost_ticks")
    if (
        value["label_version"] != cfg.label_version
        or value["parent_feature_manifest_sha256"] != build.feature_manifest.content_sha256
        or value["cost_ticks"] != cfg.round_trip_cost_ticks
        or value["status_coverage"] is not False
        or value["entry_eligible"] is not False
        or value["research_context_only"] is not True
    ):
        raise RealSliceError("LABEL authority, version, cost, or lineage drift")
    for key in ("timeout", "ambiguous", "raw_fallback_used", "mechanical_outcome_valid"):
        if not isinstance(value[key], bool):
            raise RealSliceError(f"LABEL {key} must be boolean")
    mechanical = value["mechanical_outcome_valid"]
    outcome_fields = (
        "entry_ts_ns",
        "entry_price_ticks",
        "tp_price_ticks",
        "sl_price_ticks",
        "first_touch_ts_ns",
        "exit_ts_ns",
        "exit_price_ticks",
        "gross_pnl_ticks",
        "net_pnl_ticks",
    )
    if not mechanical:
        if (
            value["first_touch_type"] != "INVALID"
            or any(value[key] is not None for key in outcome_fields)
            or value["timeout"]
            or value["ambiguous"]
            or value["raw_fallback_used"]
            or value["invalid_reason"] not in _INVALID_REASONS
            or (not feature_validity[feature_key]) != (value["invalid_reason"] == "INVALID_FEATURE")
        ):
            raise RealSliceError("LABEL invalid-outcome shape is inconsistent")
    else:
        if not feature_validity[feature_key]:
            raise RealSliceError("LABEL has a mechanical outcome for an invalid feature")
        entry_ts = _integer(value["entry_ts_ns"], label="label entry timestamp")
        exit_ts = _integer(value["exit_ts_ns"], label="label exit timestamp")
        entry = _integer(value["entry_price_ticks"], label="label entry price", minimum=1)
        tp = _integer(value["tp_price_ticks"], label="label TP price", minimum=1)
        sl = _integer(value["sl_price_ticks"], label="label SL price", minimum=1)
        exit_price = _integer(value["exit_price_ticks"], label="label exit price", minimum=1)
        gross = _integer(value["gross_pnl_ticks"], label="label gross PnL")
        net = _integer(value["net_pnl_ticks"], label="label net PnL")
        horizon = event_ts + hold * _NS
        if (
            entry_ts < event_ts + cfg.route_delay_seconds * _NS
            or entry_ts > horizon
            or exit_ts < entry_ts
            or exit_ts > horizon
            or exit_ts > closed
            or (direction == "LONG" and not tp > entry > sl)
            or (direction == "SHORT" and not tp < entry < sl)
        ):
            raise RealSliceError("LABEL entry/exit/barrier timing or price direction is invalid")
        expected_gross = exit_price - entry if direction == "LONG" else entry - exit_price
        if gross != expected_gross or net != gross - cfg.round_trip_cost_ticks:
            raise RealSliceError("LABEL PnL arithmetic is inconsistent")
        touch = value["first_touch_type"]
        touch_ts = value["first_touch_ts_ns"]
        if touch == "TIMEOUT":
            if touch_ts is not None or not value["timeout"] or exit_ts != horizon:
                raise RealSliceError("LABEL timeout shape is inconsistent")
        elif touch in {"TP_FIRST", "SL_FIRST"}:
            if (
                _integer(touch_ts, label="label first-touch timestamp") != exit_ts
                or value["timeout"]
                or (touch == "TP_FIRST" and exit_price != tp)
                or (
                    touch == "SL_FIRST"
                    and (
                        (direction == "LONG" and exit_price > sl)
                        or (direction == "SHORT" and exit_price < sl)
                    )
                )
            ):
                raise RealSliceError("LABEL first-touch price/timestamp shape is inconsistent")
        else:
            raise RealSliceError("LABEL first_touch_type is invalid")
        if (
            value["ambiguous"] is not value["raw_fallback_used"]
            or value["invalid_reason"] != "SCHEDULE_ONLY_STATUS_UNVERIFIED"
        ):
            raise RealSliceError("LABEL raw fallback or status-only reason is inconsistent")
    return session.session_id, event_ts, str(direction), k_tp, k_sl, hold


def _verified_jsonl_rows(path: Path, *, artifact_type: str):
    with path.open("rb") as handle:
        for line_number, payload in enumerate(handle, start=1):
            try:
                row = json.loads(payload)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise RealSliceError(f"{artifact_type} row {line_number} is not JSON") from error
            if not isinstance(row, dict) or canonical_json_bytes(row) + b"\n" != payload:
                raise RealSliceError(f"{artifact_type} row {line_number} is not canonical JSONL")
            yield row


def _verify_materialized_policy_rows(
    cfg: RealSliceConfig,
    build: RealSliceBuild,
    artifact_paths: dict[str, Path],
) -> None:
    windows = _window_by_session(cfg, build)
    quote_keys: set[tuple[int, int]] = set()
    for row in _verified_jsonl_rows(artifact_paths["QUOTE_1S"], artifact_type="QUOTE_1S"):
        key = _verify_quote_policy_row(cfg, build, windows, row)
        if key in quote_keys:
            raise RealSliceError("QUOTE_1S repeats a session-second")
        quote_keys.add(key)
    if len(quote_keys) != build.quote_manifest.row_count:
        raise RealSliceError("QUOTE_1S semantic cardinality drifted")

    feature_validity: dict[tuple[str, int], bool] = {}
    for row in _verified_jsonl_rows(artifact_paths["FEATURE"], artifact_type="FEATURE"):
        key, valid = _verify_feature_policy_row(cfg, build, windows, row)
        if key in feature_validity:
            raise RealSliceError("FEATURE repeats a decision-clock event")
        feature_validity[key] = valid
    if len(feature_validity) != build.feature_manifest.row_count:
        raise RealSliceError("FEATURE semantic cardinality drifted")

    label_keys: set[tuple[str, int, str, int, int, int]] = set()
    for row in _verified_jsonl_rows(artifact_paths["LABEL"], artifact_type="LABEL"):
        key = _verify_label_policy_row(cfg, build, windows, feature_validity, row)
        if key in label_keys:
            raise RealSliceError("LABEL repeats an event/direction/barrier outcome")
        label_keys.add(key)
    if len(label_keys) != build.label_manifest.row_count:
        raise RealSliceError("LABEL semantic cardinality drifted")
