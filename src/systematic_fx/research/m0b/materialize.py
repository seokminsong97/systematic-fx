"""Bounded raw MBP-10 projection and content-addressed M0b artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from systematic_fx.data.contracts import UNDEFINED_PRICE, validate_mbp10_contract
from systematic_fx.data.instruments import parse_instrument_mappings
from systematic_fx.research.hypotheses import canonical_json_bytes
from systematic_fx.research.m0b.adapter import (
    _file_sha256,
    _source_payload,
    build_real_slice,
)
from systematic_fx.research.m0b.config import (
    RealSliceConfig,
    _reject_path_tokens,
    _reject_symlink_components,
    _resolve_existing_search_path,
    canonical_real_slice_config,
)
from systematic_fx.research.m0b.model import ArtifactIdentity, RealSliceBuild, RealSliceError

_NS: Final = 1_000_000_000
_F_MAYBE_BAD_BOOK: Final = 4
_F_BAD_TS_RECV: Final = 8
_RAW_COLUMNS: Final = (
    "ts_recv",
    "instrument_id",
    "action",
    "side",
    "flags",
    "sequence",
    "price",
    "size",
    "bid_px_00",
    "ask_px_00",
    "bid_sz_00",
    "ask_sz_00",
)


@dataclass(frozen=True, slots=True)
class _RawSession:
    ts_ns: np.ndarray
    sequence: np.ndarray
    ordinal: np.ndarray
    bid_raw: np.ndarray
    ask_raw: np.ndarray
    bid_size: np.ndarray
    ask_size: np.ndarray
    trade_price_raw: np.ndarray
    trade_size: np.ndarray
    flags: np.ndarray
    is_trade: np.ndarray
    is_reset: np.ndarray
    side_code: np.ndarray  # 1=B/aggressor buy, -1=A/aggressor sell, 0=unknown
    valid_quote: np.ndarray

    @property
    def row_count(self) -> int:
        return len(self.ts_ns)


@dataclass(frozen=True, slots=True)
class _Window:
    open_ts_ns: int
    close_ts_ns: int


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _jsonl_bytes(rows: list[dict[str, Any]]) -> bytes:
    return b"".join(canonical_json_bytes(row) + b"\n" for row in rows)


def _safe_root(path: str | Path, *, label: str, create: bool = False) -> Path:
    requested = Path(path).expanduser()
    if ".." in requested.parts:
        raise RealSliceError(f"{label} cannot contain traversal")
    _reject_path_tokens(requested, label=label)
    _reject_symlink_components(requested, label=label)
    if create:
        try:
            requested.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise RealSliceError(f"{label} could not be created") from error
    return _resolve_existing_search_path(requested, label=label, kind="directory")


def _source_path(root: Path, relative_uri: str) -> Path:
    base = _resolve_existing_search_path(root / "mbp-10", label="MBP-10 root", kind="directory")
    requested = base / relative_uri
    resolved = _resolve_existing_search_path(requested, label="raw source", kind="file")
    if not resolved.is_relative_to(base):
        raise RealSliceError("raw source escaped the exact MBP-10 root")
    return resolved


def _stat_identity(path: Path) -> tuple[int, int, int, int, int]:
    details = path.stat(follow_symlinks=False)
    return (
        details.st_dev,
        details.st_ino,
        details.st_size,
        details.st_mtime_ns,
        details.st_ctime_ns,
    )


def _verify_source_registry(cfg: RealSliceConfig, project_root: Path) -> None:
    requested = project_root / cfg.source_manifest
    path = _resolve_existing_search_path(requested, label="source registry", kind="file")
    if not path.is_relative_to(project_root):
        raise RealSliceError("source registry escaped the project root")
    if _file_sha256(path) != cfg.source_manifest_sha256:
        raise RealSliceError("source registry SHA-256 differs from the immutable slice manifest")
    expected = {item.relative_uri: item for item in cfg.sources}
    observed: dict[str, dict[str, Any]] = {}
    with path.open("rb") as handle:
        for number, payload in enumerate(handle, start=1):
            try:
                row = json.loads(payload)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise RealSliceError(f"source registry line {number} is invalid JSON") from error
            if not isinstance(row, dict):
                raise RealSliceError("source registry rows must be objects")
            relative_uri = row.get("relative_uri")
            if relative_uri in expected:
                if relative_uri in observed:
                    raise RealSliceError("source registry repeats an allowlisted raw source")
                observed[str(relative_uri)] = row
    if set(observed) != set(expected):
        raise RealSliceError("source registry does not cover the exact raw allowlist")
    for relative_uri, source in expected.items():
        row = observed[relative_uri]
        if (
            row.get("source_date") != source.source_date.isoformat()
            or row.get("sha256") != source.sha256
        ):
            raise RealSliceError("source registry lineage differs from the slice manifest")


def _verify_previous_source_volumes(cfg: RealSliceConfig, data_root: Path) -> None:
    sources = {item.source_date: item for item in cfg.sources}
    ids_by_symbol: dict[str, int] = {}
    for symbol, instrument_id in zip(
        cfg.expected_contracts, cfg.expected_instrument_ids, strict=True
    ):
        if symbol in ids_by_symbol and ids_by_symbol[symbol] != instrument_id:
            raise RealSliceError("one staged symbol maps to multiple instrument IDs")
        ids_by_symbol[symbol] = instrument_id
    for context in cfg.previous_source_volume_context:
        try:
            source = sources[context.evidence_source_date]
            selected_id = ids_by_symbol[context.selected_raw_symbol]
            other_id = ids_by_symbol[context.other_raw_symbol]
        except KeyError as error:
            raise RealSliceError(
                "volume evidence is outside the source/symbol allowlist"
            ) from error
        path = _source_path(data_root, source.relative_uri)
        parquet = pq.ParquetFile(path)
        for instrument_id, symbol in (
            (selected_id, context.selected_raw_symbol),
            (other_id, context.other_raw_symbol),
        ):
            _validate_mapping(
                parquet,
                source_date=source.source_date,
                instrument_id=instrument_id,
                raw_symbol=symbol,
            )
        totals = {selected_id: [0, 0], other_id: [0, 0]}
        for row_group_index in range(parquet.num_row_groups):
            table = parquet.read_row_group(
                row_group_index,
                columns=["instrument_id", "action", "size"],
                use_threads=False,
            )
            trades = table.filter(pc.equal(table["action"], "T"))
            for instrument_id, total in totals.items():
                selected = trades.filter(pc.equal(trades["instrument_id"], instrument_id))
                total[0] += selected.num_rows
                volume = pc.sum(pc.cast(selected["size"], pa.int64())).as_py()
                total[1] += 0 if volume is None else int(volume)
        expected = (
            context.selected_trade_rows,
            context.selected_trade_volume,
            context.other_trade_rows,
            context.other_trade_volume,
        )
        observed = (
            *totals[selected_id],
            *totals[other_id],
        )
        if observed != expected:
            raise RealSliceError("observed previous-source volume context drifted")


def _ns_from_stat(value: object) -> int:
    raw = getattr(value, "value", None)
    if isinstance(raw, int):
        return raw
    if isinstance(value, datetime) and value.tzinfo is not None:
        return int(value.astimezone(UTC).timestamp() * _NS)
    return int(pc.cast(pa.array([value]), pa.int64())[0].as_py())


def _row_group_overlaps(parquet: pq.ParquetFile, index: int, window: _Window) -> bool:
    column_index = parquet.schema_arrow.get_field_index("ts_recv")
    stats = parquet.metadata.row_group(index).column(column_index).statistics
    if stats is None or not stats.has_min_max:
        return True
    minimum = _ns_from_stat(stats.min)
    maximum = _ns_from_stat(stats.max)
    return maximum >= window.open_ts_ns and minimum < window.close_ts_ns


def _validate_mapping(
    parquet: pq.ParquetFile,
    *,
    source_date: date,
    instrument_id: int,
    raw_symbol: str,
) -> None:
    validate_mbp10_contract(parquet.schema_arrow)
    payload = (parquet.schema_arrow.metadata or {}).get(b"dbn.metadata")
    if payload is None:
        raise RealSliceError("raw source has no DBN mapping metadata")
    matches = tuple(
        item
        for item in parse_instrument_mappings(payload)
        if item.interval_start <= source_date < item.interval_end
        and item.instrument_id == instrument_id
        and item.raw_symbol == raw_symbol
    )
    if len(matches) != 1:
        raise RealSliceError("raw footer does not prove the selected contract mapping")


def _empty_array(dtype: np.dtype[Any]) -> np.ndarray:
    return np.empty(0, dtype=dtype)


def _project_window(
    cfg: RealSliceConfig,
    *,
    data_root: Path,
    session_index: int,
    window: _Window,
) -> _RawSession:
    session_date = cfg.trading_dates[session_index]
    instrument_id = cfg.expected_instrument_ids[session_index]
    raw_symbol = cfg.expected_contracts[session_index]
    chunks: dict[str, list[np.ndarray]] = {
        key: []
        for key in (
            "ts_ns",
            "sequence",
            "ordinal",
            "bid_raw",
            "ask_raw",
            "bid_size",
            "ask_size",
            "trade_price_raw",
            "trade_size",
            "flags",
            "is_trade",
            "is_reset",
            "side_code",
        )
    }
    source_rank = {item.source_date: rank for rank, item in enumerate(cfg.sources)}
    selected_count = 0
    for source in cfg.sources:
        source_start = int(
            datetime(
                source.source_date.year,
                source.source_date.month,
                source.source_date.day,
                tzinfo=UTC,
            ).timestamp()
            * _NS
        )
        source_end = source_start + 86400 * _NS
        if source_end <= window.open_ts_ns or source_start >= window.close_ts_ns:
            continue
        path = _source_path(data_root, source.relative_uri)
        parquet = pq.ParquetFile(path)
        _validate_mapping(
            parquet,
            source_date=source.source_date,
            instrument_id=instrument_id,
            raw_symbol=raw_symbol,
        )
        row_offset = 0
        for row_group_index in range(parquet.num_row_groups):
            row_count = parquet.metadata.row_group(row_group_index).num_rows
            if row_count > cfg.batch_rows:
                raise RealSliceError("raw Parquet row group exceeds the immutable batch bound")
            if not _row_group_overlaps(parquet, row_group_index, window):
                row_offset += row_count
                continue
            table = parquet.read_row_group(
                row_group_index,
                columns=list(_RAW_COLUMNS),
                use_threads=False,
            )
            timestamps = pc.cast(table["ts_recv"], pa.int64())
            mask = pc.and_(
                pc.equal(table["instrument_id"], instrument_id),
                pc.and_(
                    pc.greater_equal(timestamps, window.open_ts_ns),
                    pc.less(timestamps, window.close_ts_ns),
                ),
            )
            indices = pc.indices_nonzero(mask)
            if len(indices) == 0:
                row_offset += row_count
                continue
            selected = table.take(indices)
            local = indices.to_numpy(zero_copy_only=False).astype(np.uint64, copy=False)
            ts = pc.cast(selected["ts_recv"], pa.int64()).to_numpy(zero_copy_only=False)
            action = selected["action"]
            side = selected["side"]
            chunks["ts_ns"].append(ts.astype(np.int64, copy=False))
            chunks["sequence"].append(
                selected["sequence"].to_numpy(zero_copy_only=False).astype(np.uint32, copy=False)
            )
            chunks["ordinal"].append(
                (np.uint64(source_rank[source.source_date]) << np.uint64(56))
                | (local + np.uint64(row_offset))
            )
            for raw_name, source_name, dtype in (
                ("bid_raw", "bid_px_00", np.int64),
                ("ask_raw", "ask_px_00", np.int64),
                ("bid_size", "bid_sz_00", np.uint32),
                ("ask_size", "ask_sz_00", np.uint32),
                ("trade_price_raw", "price", np.int64),
                ("trade_size", "size", np.uint32),
                ("flags", "flags", np.uint8),
            ):
                chunks[raw_name].append(
                    selected[source_name].to_numpy(zero_copy_only=False).astype(dtype, copy=False)
                )
            chunks["is_trade"].append(
                pc.equal(action, "T").to_numpy(zero_copy_only=False).astype(np.bool_, copy=False)
            )
            chunks["is_reset"].append(
                pc.equal(action, "R").to_numpy(zero_copy_only=False).astype(np.bool_, copy=False)
            )
            buy = pc.equal(side, "B").to_numpy(zero_copy_only=False)
            sell = pc.equal(side, "A").to_numpy(zero_copy_only=False)
            chunks["side_code"].append(
                np.where(buy, 1, np.where(sell, -1, 0)).astype(np.int8, copy=False)
            )
            selected_count += len(indices)
            if selected_count > cfg.max_selected_raw_events:
                raise RealSliceError("selected raw-event count exceeds the immutable bound")
            row_offset += row_count

    if selected_count == 0:
        raise RealSliceError(f"bounded real window has no {raw_symbol} events: {session_date}")

    values = {key: np.concatenate(parts) for key, parts in chunks.items()}
    if np.any(values["ts_ns"][1:] < values["ts_ns"][:-1]):
        raise RealSliceError("selected raw events regress in receive timestamp")
    same_ts = values["ts_ns"][1:] == values["ts_ns"][:-1]
    if np.any(values["sequence"][1:][same_ts] < values["sequence"][:-1][same_ts]):
        raise RealSliceError("selected raw sequence regresses within a receive timestamp")
    bad_flags = (values["flags"] & (_F_MAYBE_BAD_BOOK | _F_BAD_TS_RECV)) != 0
    defined = (values["bid_raw"] != UNDEFINED_PRICE) & (values["ask_raw"] != UNDEFINED_PRICE)
    on_grid = (values["bid_raw"] % cfg.tick_size_raw == 0) & (
        values["ask_raw"] % cfg.tick_size_raw == 0
    )
    valid_quote = (
        defined
        & on_grid
        & (values["bid_raw"] < values["ask_raw"])
        & (values["bid_size"] > 0)
        & (values["ask_size"] > 0)
        & ~bad_flags
        & ~values["is_reset"]
    )
    return _RawSession(valid_quote=valid_quote, **values)


def _second_rows(
    cfg: RealSliceConfig,
    raw: _RawSession,
    *,
    session: Any,
    source_manifest_sha256: str,
) -> tuple[list[dict[str, Any]], np.ndarray, np.ndarray]:
    seconds = raw.ts_ns // _NS
    starts = np.r_[0, np.flatnonzero(seconds[1:] != seconds[:-1]) + 1]
    ends = np.r_[starts[1:], raw.row_count]
    if len(starts) > cfg.max_quote_seconds:
        raise RealSliceError("quote-second count exceeds the immutable bound")
    rows: list[dict[str, Any]] = []
    for start, end in zip(starts.tolist(), ends.tolist(), strict=True):
        valid_indices = np.flatnonzero(raw.valid_quote[start:end]) + start
        trade_valid = (
            raw.is_trade[start:end]
            & ((raw.flags[start:end] & _F_BAD_TS_RECV) == 0)
            & (raw.trade_price_raw[start:end] != UNDEFINED_PRICE)
            & (raw.trade_price_raw[start:end] % cfg.tick_size_raw == 0)
        )
        buys = np.flatnonzero(trade_valid & (raw.side_code[start:end] == 1)) + start
        sells = np.flatnonzero(trade_valid & (raw.side_code[start:end] == -1)) + start
        last = int(valid_indices[-1]) if len(valid_indices) else None
        rows.append(
            {
                "artifact_schema": "systematic_fx.m0b_quote_second.v1",
                "source_manifest_sha256": source_manifest_sha256,
                "session_id": session.session_id,
                "trading_date": session.trading_date.isoformat(),
                "raw_symbol": session.raw_symbol,
                "instrument_id": session.instrument_id,
                "second_start_ts_ns": int(seconds[start] * _NS),
                "event_count": end - start,
                "valid_quote_count": len(valid_indices),
                "bid_ticks": None if last is None else int(raw.bid_raw[last] // cfg.tick_size_raw),
                "ask_ticks": None if last is None else int(raw.ask_raw[last] // cfg.tick_size_raw),
                "bid_size_l1": None if last is None else int(raw.bid_size[last]),
                "ask_size_l1": None if last is None else int(raw.ask_size[last]),
                "min_bid_ticks": (
                    None
                    if not len(valid_indices)
                    else int(raw.bid_raw[valid_indices].min() // cfg.tick_size_raw)
                ),
                "max_ask_ticks": (
                    None
                    if not len(valid_indices)
                    else int(raw.ask_raw[valid_indices].max() // cfg.tick_size_raw)
                ),
                "aggressor_buy_trade_max_ticks": (
                    None
                    if not len(buys)
                    else int(raw.trade_price_raw[buys].max() // cfg.tick_size_raw)
                ),
                "aggressor_sell_trade_min_ticks": (
                    None
                    if not len(sells)
                    else int(raw.trade_price_raw[sells].min() // cfg.tick_size_raw)
                ),
                "raw_first_ordinal": int(raw.ordinal[start]),
                "raw_last_ordinal": int(raw.ordinal[end - 1]),
                "raw_order_available": True,
                "status_coverage": False,
                "research_eligible": False,
            }
        )
    return rows, starts.astype(np.int64), ends.astype(np.int64)


def _round_ratio(numerator: int, denominator: int) -> int:
    if numerator >= 0:
        return (numerator + denominator // 2) // denominator
    return -((-numerator + denominator // 2) // denominator)


def _feature_rows(
    cfg: RealSliceConfig,
    raw: _RawSession,
    quote_rows: list[dict[str, Any]],
    *,
    session: Any,
    window: _Window,
) -> list[dict[str, Any]]:
    width_ns = cfg.decision_clock_seconds * _NS
    count = cfg.window_duration_seconds // cfg.decision_clock_seconds
    bars: list[dict[str, Any]] = []
    for index in range(count):
        start = window.open_ts_ns + index * width_ns
        end = start + width_ns
        quote_start = int(np.searchsorted(raw.ts_ns, start, side="left"))
        quote_end = int(np.searchsorted(raw.ts_ns, end, side="left"))
        valid = np.flatnonzero(raw.valid_quote[quote_start:quote_end]) + quote_start
        if not len(valid):
            bars.append({"start": start, "end": end, "valid": False})
            continue
        mids = (
            raw.bid_raw[valid] // cfg.tick_size_raw + raw.ask_raw[valid] // cfg.tick_size_raw
        ) // 2
        last = int(valid[-1])
        depth_total = int(raw.bid_size[last]) + int(raw.ask_size[last])
        bars.append(
            {
                "start": start,
                "end": end,
                "valid": True,
                "open": int(mids[0]),
                "high": int(mids.max()),
                "low": int(mids.min()),
                "close": int(mids[-1]),
                "spread": int((raw.ask_raw[last] - raw.bid_raw[last]) // cfg.tick_size_raw),
                "imbalance": (
                    0
                    if depth_total == 0
                    else ((int(raw.bid_size[last]) - int(raw.ask_size[last])) * 1_000_000)
                    // depth_total
                ),
            }
        )

    true_ranges: list[int] = []
    prior_close: int | None = None
    for bar in bars:
        if not bar["valid"]:
            true_ranges.append(0)
            prior_close = None
            continue
        value = int(bar["high"]) - int(bar["low"])
        if prior_close is not None:
            value = max(
                value,
                abs(int(bar["high"]) - prior_close),
                abs(int(bar["low"]) - prior_close),
            )
        true_ranges.append(value)
        prior_close = int(bar["close"])
    volatilities: list[int] = []
    for index in range(count):
        start = max(0, index - cfg.atr_lookback_bars + 1)
        sample = true_ranges[start : index + 1]
        volatilities.append(max(1, _round_ratio(sum(sample), len(sample))))

    def context(index: int, window_bars: int) -> tuple[int | None, int | None]:
        completed_end = ((index + 1) // window_bars) * window_bars - 1
        if completed_end < window_bars - 1:
            return None, None
        sample = bars[completed_end - window_bars + 1 : completed_end + 1]
        if len(sample) != window_bars or any(not item["valid"] for item in sample):
            return None, None
        return int(sample[-1]["close"]) - int(sample[0]["open"]), int(sample[-1]["end"])

    rows: list[dict[str, Any]] = []
    for index, bar in enumerate(bars):
        flags: list[str] = []
        if not bar["valid"]:
            flags.append("NO_VALID_QUOTES_IN_BAR")
        if index + 1 < cfg.atr_lookback_bars:
            flags.append("INSUFFICIENT_ATR_HISTORY")
        prior_start = index - cfg.quantile_lookback_bars
        prior_volatility = (
            volatilities[prior_start:index] if prior_start >= cfg.atr_lookback_bars - 1 else []
        )
        if len(prior_volatility) != cfg.quantile_lookback_bars:
            quantile: int | None = None
            flags.append("INSUFFICIENT_PRIOR_QUANTILE_HISTORY")
        else:
            quantile = (
                sum(value <= volatilities[index] for value in prior_volatility) * 1_000_000
            ) // len(prior_volatility)
        trend_30m, context_30m_end = context(index, 6)
        trend_1h, context_1h_end = context(index, 12)
        if trend_30m is None:
            flags.append("MISSING_COMPLETED_30M_CONTEXT")
        if trend_1h is None:
            flags.append("MISSING_COMPLETED_1H_CONTEXT")
        if index < cfg.short_trend_lookback_bars or not bar["valid"]:
            short_trend: int | None = None
            flags.append("INSUFFICIENT_SHORT_TREND_HISTORY")
        else:
            previous = bars[index - cfg.short_trend_lookback_bars]
            short_trend = (
                None if not previous["valid"] else int(bar["close"]) - int(previous["close"])
            )
        transition_context = session.role == "CONTRACT_TRANSITION_CONTEXT_NOT_ACTIVE_SELECTION"
        if transition_context and index == 0:
            flags.append("CONTRACT_TRANSITION_CONTEXT_NOT_ACTIVE_SELECTION")
        rows.append(
            {
                "artifact_schema": "systematic_fx.m0b_event_feature.v1",
                "event_ts_ns": int(bar["end"]),
                "instrument_id": session.instrument_id,
                "raw_symbol": session.raw_symbol,
                "session_id": session.session_id,
                "trading_date": session.trading_date.isoformat(),
                "role": session.role,
                "feature_version": cfg.feature_version,
                "bar_open_ticks": bar.get("open"),
                "bar_high_ticks": bar.get("high"),
                "bar_low_ticks": bar.get("low"),
                "bar_close_ticks": bar.get("close"),
                "range_ticks": (None if not bar["valid"] else int(bar["high"]) - int(bar["low"])),
                "volatility_ticks": volatilities[index],
                "volatility_quantile_ppm": quantile,
                "spread_ticks": bar.get("spread"),
                "depth_imbalance_ppm": bar.get("imbalance"),
                "short_trend_ticks": short_trend,
                "trend_30m_ticks": trend_30m,
                "context_30m_end_ns": context_30m_end,
                "trend_1h_ticks": trend_1h,
                "context_1h_end_ns": context_1h_end,
                "roll_cross": False,
                "contract_transition_context": transition_context,
                "active_selection_proven": False,
                "feature_valid": not flags,
                "validity_flags": flags,
                "status_coverage": False,
                "research_eligible": False,
            }
        )
    del quote_rows  # quote lineage is bound by the parent artifact hash.
    return rows


def _raw_touch(
    cfg: RealSliceConfig,
    raw: _RawSession,
    start: int,
    end: int,
    *,
    direction: str,
    tp: int,
    sl: int,
) -> tuple[str, int, int]:
    for index in range(start, end):
        stop = raw.valid_quote[index] and (
            (direction == "LONG" and raw.bid_raw[index] // cfg.tick_size_raw <= sl)
            or (direction == "SHORT" and raw.ask_raw[index] // cfg.tick_size_raw >= sl)
        )
        trade_ok = (
            raw.is_trade[index]
            and not raw.flags[index] & _F_BAD_TS_RECV
            and raw.trade_price_raw[index] != UNDEFINED_PRICE
            and raw.trade_price_raw[index] % cfg.tick_size_raw == 0
        )
        take_profit = trade_ok and (
            (
                direction == "LONG"
                and raw.side_code[index] == 1
                and raw.trade_price_raw[index] // cfg.tick_size_raw
                >= tp + cfg.tp_trade_through_ticks
            )
            or (
                direction == "SHORT"
                and raw.side_code[index] == -1
                and raw.trade_price_raw[index] // cfg.tick_size_raw
                <= tp - cfg.tp_trade_through_ticks
            )
        )
        if stop:
            price = (
                raw.bid_raw[index] // cfg.tick_size_raw
                if direction == "LONG"
                else raw.ask_raw[index] // cfg.tick_size_raw
            )
            return "SL_FIRST", int(raw.ts_ns[index]), int(price)
        if take_profit:
            return "TP_FIRST", int(raw.ts_ns[index]), tp
    raise RealSliceError("aggregate ambiguity did not resolve in ordered raw events")


def _first_passage(
    cfg: RealSliceConfig,
    raw: _RawSession,
    quote_rows: list[dict[str, Any]],
    second_starts: np.ndarray,
    second_ends: np.ndarray,
    *,
    entry_index: int,
    horizon_ts_ns: int,
    direction: str,
    tp: int,
    sl: int,
) -> tuple[str, int | None, int, bool, bool]:
    # The entry book is observed at entry_index.  A trade carried by that same
    # historical event happened before the route could place the bracket, so it
    # may not prove a passive TP fill.  The entry book can, however, make the
    # conservative marketable stop immediate once the entry completes.
    entry_stop = raw.valid_quote[entry_index] and (
        (direction == "LONG" and raw.bid_raw[entry_index] // cfg.tick_size_raw <= sl)
        or (direction == "SHORT" and raw.ask_raw[entry_index] // cfg.tick_size_raw >= sl)
    )
    if entry_stop:
        price = (
            raw.bid_raw[entry_index] // cfg.tick_size_raw
            if direction == "LONG"
            else raw.ask_raw[entry_index] // cfg.tick_size_raw
        )
        return "SL_FIRST", int(raw.ts_ns[entry_index]), int(price), False, False
    # All remaining TP/SL observations must be strictly later ordered events.
    scan_start = entry_index + 1
    first_second = int(np.searchsorted(second_ends, scan_start, side="right"))
    for second_index in range(first_second, len(quote_rows)):
        row = quote_rows[second_index]
        if int(row["second_start_ts_ns"]) > horizon_ts_ns:
            break
        start = max(int(second_starts[second_index]), scan_start)
        end = int(second_ends[second_index])
        end = (
            int(np.searchsorted(raw.ts_ns, horizon_ts_ns, side="right", sorter=None))
            if (end == raw.row_count or raw.ts_ns[end - 1] > horizon_ts_ns)
            else end
        )
        if start >= end:
            continue
        whole_second = start == int(second_starts[second_index]) and end == int(
            second_ends[second_index]
        )
        if whole_second and direction == "LONG":
            tp_possible = (
                row["aggressor_buy_trade_max_ticks"] is not None
                and int(row["aggressor_buy_trade_max_ticks"]) >= tp + cfg.tp_trade_through_ticks
            )
            sl_possible = row["min_bid_ticks"] is not None and int(row["min_bid_ticks"]) <= sl
        elif whole_second:
            tp_possible = (
                row["aggressor_sell_trade_min_ticks"] is not None
                and int(row["aggressor_sell_trade_min_ticks"]) <= tp - cfg.tp_trade_through_ticks
            )
            sl_possible = row["max_ask_ticks"] is not None and int(row["max_ask_ticks"]) >= sl
        else:
            valid_trade = (
                raw.is_trade[start:end]
                & ((raw.flags[start:end] & _F_BAD_TS_RECV) == 0)
                & (raw.trade_price_raw[start:end] != UNDEFINED_PRICE)
                & (raw.trade_price_raw[start:end] % cfg.tick_size_raw == 0)
            )
            if direction == "LONG":
                tp_possible = bool(
                    np.any(
                        valid_trade
                        & (raw.side_code[start:end] == 1)
                        & (
                            raw.trade_price_raw[start:end] // cfg.tick_size_raw
                            >= tp + cfg.tp_trade_through_ticks
                        )
                    )
                )
                sl_possible = bool(
                    np.any(
                        raw.valid_quote[start:end]
                        & (raw.bid_raw[start:end] // cfg.tick_size_raw <= sl)
                    )
                )
            else:
                tp_possible = bool(
                    np.any(
                        valid_trade
                        & (raw.side_code[start:end] == -1)
                        & (
                            raw.trade_price_raw[start:end] // cfg.tick_size_raw
                            <= tp - cfg.tp_trade_through_ticks
                        )
                    )
                )
                sl_possible = bool(
                    np.any(
                        raw.valid_quote[start:end]
                        & (raw.ask_raw[start:end] // cfg.tick_size_raw >= sl)
                    )
                )
        if tp_possible and sl_possible:
            touch, timestamp, price = _raw_touch(
                cfg, raw, start, end, direction=direction, tp=tp, sl=sl
            )
            return touch, timestamp, price, True, True
        if sl_possible:
            for index in range(start, end):
                if raw.valid_quote[index] and (
                    (direction == "LONG" and raw.bid_raw[index] // cfg.tick_size_raw <= sl)
                    or (direction == "SHORT" and raw.ask_raw[index] // cfg.tick_size_raw >= sl)
                ):
                    price = (
                        raw.bid_raw[index] // cfg.tick_size_raw
                        if direction == "LONG"
                        else raw.ask_raw[index] // cfg.tick_size_raw
                    )
                    return "SL_FIRST", int(raw.ts_ns[index]), int(price), False, False
        if tp_possible:
            for index in range(start, end):
                if (
                    raw.is_trade[index]
                    and not raw.flags[index] & _F_BAD_TS_RECV
                    and raw.trade_price_raw[index] != UNDEFINED_PRICE
                    and raw.trade_price_raw[index] % cfg.tick_size_raw == 0
                    and (
                        (
                            direction == "LONG"
                            and raw.side_code[index] == 1
                            and raw.trade_price_raw[index] // cfg.tick_size_raw
                            >= tp + cfg.tp_trade_through_ticks
                        )
                        or (
                            direction == "SHORT"
                            and raw.side_code[index] == -1
                            and raw.trade_price_raw[index] // cfg.tick_size_raw
                            <= tp - cfg.tp_trade_through_ticks
                        )
                    )
                ):
                    return "TP_FIRST", int(raw.ts_ns[index]), tp, False, False
    valid = np.flatnonzero(
        raw.valid_quote[entry_index:] & (raw.ts_ns[entry_index:] <= horizon_ts_ns)
    )
    if not len(valid):
        raise RealSliceError("timeout path lost its eligible entry quote")
    last = entry_index + int(valid[-1])
    price = (
        raw.bid_raw[last] // cfg.tick_size_raw
        if direction == "LONG"
        else raw.ask_raw[last] // cfg.tick_size_raw
    )
    return "TIMEOUT", horizon_ts_ns, int(price), False, False


def _invalid_label(
    cfg: RealSliceConfig,
    feature: dict[str, Any],
    *,
    direction: str,
    barrier_id: str,
    k_tp: int,
    k_sl: int,
    hold: int,
    reason: str,
) -> dict[str, Any]:
    return {
        "artifact_schema": "systematic_fx.m0b_quote_label.v1",
        "event_ts_ns": feature["event_ts_ns"],
        "instrument_id": feature["instrument_id"],
        "raw_symbol": feature["raw_symbol"],
        "session_id": feature["session_id"],
        "direction": direction,
        "barrier_id": barrier_id,
        "k_tp_num": k_tp,
        "k_tp_den": cfg.barrier_k_tp_denominator,
        "k_sl_num": k_sl,
        "k_sl_den": cfg.barrier_k_sl_denominator,
        "max_hold_seconds": hold,
        "entry_ts_ns": None,
        "entry_price_ticks": None,
        "tp_price_ticks": None,
        "sl_price_ticks": None,
        "first_touch_type": "INVALID",
        "first_touch_ts_ns": None,
        "exit_ts_ns": None,
        "exit_price_ticks": None,
        "timeout": False,
        "ambiguous": False,
        "raw_fallback_used": False,
        "gross_pnl_ticks": None,
        "net_pnl_ticks": None,
        "cost_ticks": cfg.round_trip_cost_ticks,
        "label_version": cfg.label_version,
        "mechanical_outcome_valid": False,
        "entry_eligible": False,
        "research_context_only": True,
        "invalid_reason": reason,
        "status_coverage": False,
    }


def _label_rows(
    cfg: RealSliceConfig,
    raw: _RawSession,
    quote_rows: list[dict[str, Any]],
    second_starts: np.ndarray,
    second_ends: np.ndarray,
    features: list[dict[str, Any]],
    *,
    session: Any,
    window: _Window,
    contract: Any,
) -> list[dict[str, Any]]:
    valid_quote_indices = np.flatnonzero(raw.valid_quote)
    rows: list[dict[str, Any]] = []
    for feature in features:
        for direction in ("LONG", "SHORT"):
            for k_tp in cfg.barrier_k_tp_numerators:
                for k_sl in cfg.barrier_k_sl_numerators:
                    for hold in cfg.max_hold_seconds:
                        barrier_id = (
                            f"tp{k_tp}of{cfg.barrier_k_tp_denominator}_"
                            f"sl{k_sl}of{cfg.barrier_k_sl_denominator}_h{hold}"
                        )
                        if not feature["feature_valid"]:
                            rows.append(
                                _invalid_label(
                                    cfg,
                                    feature,
                                    direction=direction,
                                    barrier_id=barrier_id,
                                    k_tp=k_tp,
                                    k_sl=k_sl,
                                    hold=hold,
                                    reason="INVALID_FEATURE",
                                )
                            )
                            continue
                        horizon = int(feature["event_ts_ns"]) + hold * _NS
                        reason: str | None = None
                        if int(feature["event_ts_ns"]) >= int(contract.roll_guard_start_ts_ns):
                            reason = "ROLL_GUARD"
                        elif horizon > session.close_ts_ns:
                            reason = "WOULD_CROSS_SESSION_CLOSE"
                        elif horizon > int(contract.roll_guard_start_ts_ns):
                            reason = "WOULD_CROSS_ROLL_GUARD"
                        elif horizon >= int(contract.last_trade_ts_ns):
                            reason = "DELIVERY_OR_EXPIRY_GUARD"
                        elif horizon > window.close_ts_ns:
                            reason = "MATERIALIZATION_WINDOW_INCOMPLETE"
                        if reason is not None:
                            rows.append(
                                _invalid_label(
                                    cfg,
                                    feature,
                                    direction=direction,
                                    barrier_id=barrier_id,
                                    k_tp=k_tp,
                                    k_sl=k_sl,
                                    hold=hold,
                                    reason=reason,
                                )
                            )
                            continue
                        route = int(feature["event_ts_ns"]) + cfg.route_delay_seconds * _NS
                        position = int(np.searchsorted(raw.ts_ns[valid_quote_indices], route))
                        if position >= len(valid_quote_indices):
                            rows.append(
                                _invalid_label(
                                    cfg,
                                    feature,
                                    direction=direction,
                                    barrier_id=barrier_id,
                                    k_tp=k_tp,
                                    k_sl=k_sl,
                                    hold=hold,
                                    reason="NO_ELIGIBLE_ENTRY_QUOTE",
                                )
                            )
                            continue
                        entry = int(valid_quote_indices[position])
                        if raw.ts_ns[entry] > horizon:
                            rows.append(
                                _invalid_label(
                                    cfg,
                                    feature,
                                    direction=direction,
                                    barrier_id=barrier_id,
                                    k_tp=k_tp,
                                    k_sl=k_sl,
                                    hold=hold,
                                    reason="NO_ELIGIBLE_ENTRY_QUOTE",
                                )
                            )
                            continue
                        if direction == "LONG":
                            entry_price = (
                                int(raw.ask_raw[entry] // cfg.tick_size_raw)
                                + cfg.entry_adverse_ticks
                            )
                        else:
                            entry_price = (
                                int(raw.bid_raw[entry] // cfg.tick_size_raw)
                                - cfg.entry_adverse_ticks
                            )
                        volatility = int(feature["volatility_ticks"])
                        tp_distance = max(
                            1,
                            _round_ratio(volatility * k_tp, cfg.barrier_k_tp_denominator),
                        )
                        sl_distance = max(
                            1,
                            _round_ratio(volatility * k_sl, cfg.barrier_k_sl_denominator),
                        )
                        tp = (
                            entry_price + tp_distance
                            if direction == "LONG"
                            else entry_price - tp_distance
                        )
                        sl = (
                            entry_price - sl_distance
                            if direction == "LONG"
                            else entry_price + sl_distance
                        )
                        touch, touch_ts, exit_price, ambiguous, fallback = _first_passage(
                            cfg,
                            raw,
                            quote_rows,
                            second_starts,
                            second_ends,
                            entry_index=entry,
                            horizon_ts_ns=horizon,
                            direction=direction,
                            tp=tp,
                            sl=sl,
                        )
                        first_touch_ts = None if touch == "TIMEOUT" else touch_ts
                        exit_ts = touch_ts
                        gross = (
                            exit_price - entry_price
                            if direction == "LONG"
                            else entry_price - exit_price
                        )
                        rows.append(
                            {
                                **_invalid_label(
                                    cfg,
                                    feature,
                                    direction=direction,
                                    barrier_id=barrier_id,
                                    k_tp=k_tp,
                                    k_sl=k_sl,
                                    hold=hold,
                                    reason="SCHEDULE_ONLY_STATUS_UNVERIFIED",
                                ),
                                "entry_ts_ns": int(raw.ts_ns[entry]),
                                "entry_price_ticks": entry_price,
                                "tp_price_ticks": tp,
                                "sl_price_ticks": sl,
                                "first_touch_type": touch,
                                "first_touch_ts_ns": first_touch_ts,
                                "exit_ts_ns": exit_ts,
                                "exit_price_ticks": exit_price,
                                "timeout": touch == "TIMEOUT",
                                "ambiguous": ambiguous,
                                "raw_fallback_used": fallback,
                                "gross_pnl_ticks": gross,
                                "net_pnl_ticks": gross - cfg.round_trip_cost_ticks,
                                "mechanical_outcome_valid": True,
                            }
                        )
    return rows


def _write_artifact(root: Path, name: str, payload: bytes) -> tuple[str, str]:
    sha256 = _sha256_bytes(payload)
    relative = f"{name}-{sha256}.{'json' if name in {'source', 'build'} else 'jsonl'}"
    target = root / relative
    if target.exists():
        if target.is_symlink() or _file_sha256(target) != sha256:
            raise RealSliceError("existing content-addressed artifact is corrupt")
        return sha256, relative
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{name}-", dir=root)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        directory_descriptor = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if temporary.exists():
            temporary.unlink()
    return sha256, relative


def materialize_real_slice(
    config: RealSliceConfig | str | Path,
    *,
    data_root: str | Path,
    output_root: str | Path | None = None,
    reference: Any | None = None,
) -> RealSliceBuild:
    """Stream only immutable windows and emit quote/features/labels atomically."""

    cfg = canonical_real_slice_config(config)
    raw_root = _safe_root(data_root, label="data_root")
    project_root = cfg.manifest_path.parents[2]
    from systematic_fx.data.cme_reference import Cme6EReference, load_cme_6e_reference

    reference_path = _resolve_existing_search_path(
        project_root / cfg.reference_config,
        label="CME reference config",
        kind="file",
    )
    if not reference_path.is_relative_to(project_root):
        raise RealSliceError("CME reference config escaped the project root")
    canonical_reference = load_cme_6e_reference(reference_path)
    if reference is not None and (
        type(reference) is not Cme6EReference or reference != canonical_reference
    ):
        raise RealSliceError("materialization reference differs from the immutable CME config")
    reference = canonical_reference
    _verify_source_registry(cfg, project_root)
    if getattr(reference, "status_coverage", None) is not False:
        raise RealSliceError("M0b v1 is frozen to schedule-only, status-unverified reference data")
    plan = build_real_slice(cfg, reference=reference)
    source_identities: dict[date, tuple[Path, tuple[int, int, int, int, int]]] = {}
    for source in cfg.sources:
        source_path = _source_path(raw_root, source.relative_uri)
        before = _stat_identity(source_path)
        if _file_sha256(source_path) != source.sha256:
            raise RealSliceError(f"raw source SHA-256 drift: {source.source_date}")
        if _stat_identity(source_path) != before:
            raise RealSliceError("raw source changed while its content hash was verified")
        source_identities[source.source_date] = (source_path, before)
    _verify_previous_source_volumes(cfg, raw_root)

    destination = _safe_root(
        output_root if output_root is not None else project_root / cfg.staged_root,
        label="staged_root",
        create=True,
    )
    source_payload = _source_payload(cfg, str(reference.sha256), plan.sessions)
    source_bytes = canonical_json_bytes(source_payload)
    source_sha, source_uri = _write_artifact(destination, "source", source_bytes)
    if source_sha != plan.source_manifest.content_sha256:
        raise RealSliceError("source manifest identity differs from the immutable plan")

    all_quotes: list[dict[str, Any]] = []
    all_features: list[dict[str, Any]] = []
    session_material: list[
        tuple[
            _RawSession,
            list[dict[str, Any]],
            np.ndarray,
            np.ndarray,
            list[dict[str, Any]],
            Any,
            _Window,
        ]
    ] = []
    total_selected_raw_events = 0
    for index, session in enumerate(plan.sessions):
        window = _Window(
            session.open_ts_ns + cfg.window_start_seconds[index] * _NS,
            session.open_ts_ns
            + (cfg.window_start_seconds[index] + cfg.window_duration_seconds) * _NS,
        )
        if window.close_ts_ns > session.close_ts_ns:
            raise RealSliceError("materialization window crosses scheduled session close")
        raw = _project_window(cfg, data_root=raw_root, session_index=index, window=window)
        total_selected_raw_events += raw.row_count
        if total_selected_raw_events > cfg.max_selected_raw_events:
            raise RealSliceError("global selected raw-event count exceeds the immutable bound")
        quote_rows, starts, ends = _second_rows(
            cfg,
            raw,
            session=session,
            source_manifest_sha256=source_sha,
        )
        features = _feature_rows(cfg, raw, quote_rows, session=session, window=window)
        all_quotes.extend(quote_rows)
        all_features.extend(features)
        session_material.append((raw, quote_rows, starts, ends, features, session, window))
    if len(all_quotes) > cfg.max_quote_seconds or len(all_features) > cfg.max_feature_rows:
        raise RealSliceError("materialized artifact exceeded a global immutable bound")

    quote_bytes = _jsonl_bytes(all_quotes)
    quote_sha, quote_uri = _write_artifact(destination, "quote", quote_bytes)
    for feature in all_features:
        feature["parent_quote_manifest_sha256"] = quote_sha
    feature_bytes = _jsonl_bytes(all_features)
    feature_sha, feature_uri = _write_artifact(destination, "feature", feature_bytes)

    all_labels: list[dict[str, Any]] = []
    for raw, quote_rows, starts, ends, features, session, window in session_material:
        contract = reference.contract(session.raw_symbol, as_of_date=session.trading_date)
        labels = _label_rows(
            cfg,
            raw,
            quote_rows,
            starts,
            ends,
            features,
            session=session,
            window=window,
            contract=contract,
        )
        all_labels.extend(labels)
    if len(all_labels) != len(all_features) * 54 or len(all_labels) > cfg.max_label_rows:
        raise RealSliceError("quote-aware label grid cardinality differs from the immutable bound")
    for label in all_labels:
        label["parent_feature_manifest_sha256"] = feature_sha
    label_bytes = _jsonl_bytes(all_labels)
    label_sha, label_uri = _write_artifact(destination, "label", label_bytes)

    result = RealSliceBuild(
        slice_id=cfg.slice_id,
        config_hash=cfg.config_hash,
        source_manifest=ArtifactIdentity("SOURCE", len(cfg.sources), source_sha, None, source_uri),
        quote_manifest=ArtifactIdentity(
            "QUOTE_1S", len(all_quotes), quote_sha, source_sha, quote_uri
        ),
        feature_manifest=ArtifactIdentity(
            "FEATURE", len(all_features), feature_sha, quote_sha, feature_uri
        ),
        label_manifest=ArtifactIdentity(
            "LABEL", len(all_labels), label_sha, feature_sha, label_uri
        ),
        sessions=plan.sessions,
    )
    build_bytes = canonical_json_bytes(result.as_dict())
    build_sha, _ = _write_artifact(destination, "build", build_bytes)
    if build_sha != result.sha256:
        raise RealSliceError("durable build identity is not canonical")
    for path, identity in source_identities.values():
        if _stat_identity(path) != identity:
            raise RealSliceError("raw source changed during materialization")
    return result


def load_materialized_real_slice(path: str | Path) -> RealSliceBuild:
    """Reopen one canonical content-addressed build without scanning a directory."""

    resolved = _resolve_existing_search_path(
        path,
        label="materialized build",
        kind="file",
    )
    payload = resolved.read_bytes()
    sha256 = _sha256_bytes(payload)
    if resolved.name != f"build-{sha256}.json":
        raise RealSliceError("materialized build filename is not content-addressed")
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RealSliceError("materialized build is not valid canonical JSON") from error
    if not isinstance(document, dict) or canonical_json_bytes(document) != payload:
        raise RealSliceError("materialized build bytes are not canonical")
    build = RealSliceBuild.from_dict(document)
    if build.sha256 != sha256:
        raise RealSliceError("materialized build semantic identity drifted")
    return build
