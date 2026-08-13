"""Strict loader for the bounded, search-only M0b real-data slice."""

from __future__ import annotations

import hashlib
import os
import tomllib
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path, PurePosixPath

from systematic_fx.research.hypotheses import canonical_sha256
from systematic_fx.research.m0b.model import RealSliceError, SourceSpec

_UNSAFE_STORAGE_TOKENS = ("holdout", "sealed", "credential", "forward")


@dataclass(frozen=True, slots=True)
class CacheExpectation:
    source_date: date
    raw_symbol: str
    status: str


@dataclass(frozen=True, slots=True)
class PreviousSourceVolumeContext:
    trading_date: date
    evidence_source_date: date
    selected_raw_symbol: str
    selected_trade_rows: int
    selected_trade_volume: int
    other_raw_symbol: str
    other_trade_rows: int
    other_trade_volume: int

    def as_dict(self) -> dict[str, object]:
        return {
            "trading_date": self.trading_date.isoformat(),
            "evidence_source_date": self.evidence_source_date.isoformat(),
            "selected_raw_symbol": self.selected_raw_symbol,
            "selected_trade_rows": self.selected_trade_rows,
            "selected_trade_volume": self.selected_trade_volume,
            "other_raw_symbol": self.other_raw_symbol,
            "other_trade_rows": self.other_trade_rows,
            "other_trade_volume": self.other_trade_volume,
        }


@dataclass(frozen=True, slots=True)
class RealSliceConfig:
    slice_id: str
    source_schema: str
    reference_config: str
    reference_config_sha256: str
    source_manifest: str
    source_manifest_sha256: str
    staged_root: str
    source_dates: tuple[date, ...]
    trading_dates: tuple[date, ...]
    roles: tuple[str, ...]
    expected_contracts: tuple[str, ...]
    expected_instrument_ids: tuple[int, ...]
    active_selection_proven: tuple[bool, ...]
    research_authority: str
    session_policy: str
    source_adapter_version: str
    feature_version: str
    label_version: str
    execution_model_version: str
    tick_size_raw: int
    route_delay_seconds: int
    entry_adverse_ticks: int
    tp_trade_through_ticks: int
    round_trip_cost_ticks: int
    window_start_seconds: tuple[int, ...]
    window_duration_seconds: int
    decision_clock_seconds: int
    atr_lookback_bars: int
    quantile_lookback_bars: int
    short_trend_lookback_bars: int
    batch_rows: int
    max_selected_raw_events: int
    max_quote_seconds: int
    max_feature_rows: int
    max_label_rows: int
    barrier_k_tp_numerators: tuple[int, ...]
    barrier_k_tp_denominator: int
    barrier_k_sl_numerators: tuple[int, ...]
    barrier_k_sl_denominator: int
    max_hold_seconds: tuple[int, ...]
    sources: tuple[SourceSpec, ...]
    previous_source_volume_context: tuple[PreviousSourceVolumeContext, ...]
    cache_expectations: tuple[CacheExpectation, ...]
    config_hash: str
    file_sha256: str
    manifest_path: Path

    def verify_unchanged(self) -> None:
        _reject_holdout_environment()
        _reject_symlink_components(self.manifest_path, label="real-slice config")
        if hashlib.sha256(self.manifest_path.read_bytes()).hexdigest() != self.file_sha256:
            raise RealSliceError("M0b real-slice manifest changed after load")


def canonical_real_slice_config(value: RealSliceConfig | str | Path) -> RealSliceConfig:
    """Return file-derived configuration and reject mutated dataclass injection."""

    if not isinstance(value, RealSliceConfig):
        return load_real_slice_config(value)
    canonical = load_real_slice_config(value.manifest_path)
    if value != canonical:
        raise RealSliceError("in-memory M0b config differs from its immutable manifest")
    return canonical


def _date_axis(value: object, label: str) -> tuple[date, ...]:
    if not isinstance(value, list) or not value:
        raise RealSliceError(f"{label} must be a non-empty TOML date array")
    result = tuple(value)
    if any(isinstance(item, datetime) or not isinstance(item, date) for item in result):
        raise RealSliceError(f"{label} must contain dates only")
    if result != tuple(sorted(set(result))):
        raise RealSliceError(f"{label} must be unique and increasing")
    return result


def _reject_holdout_environment() -> None:
    leaked = sorted(
        key
        for key in os.environ
        if key.upper().startswith("SYSTEMATIC_FX_HOLDOUT_")
        or key.upper().startswith("SYSTEMATIC_FX_FORWARD_")
    )
    if leaked:
        raise RealSliceError("M0b search adapter cannot receive holdout/forward environment")


def _require_exact_keys(value: object, expected: set[str], *, label: str) -> None:
    if not isinstance(value, dict) or set(value) != expected:
        raise RealSliceError(f"{label} keys differ from the frozen schema")


def _sha256_text(value: object, *, label: str) -> str:
    text = str(value)
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise RealSliceError(f"{label} must be a lowercase SHA-256")
    return text


def _reject_path_tokens(path: Path, *, label: str) -> None:
    if any(
        any(token in part.casefold() for token in _UNSAFE_STORAGE_TOKENS) for part in path.parts
    ):
        raise RealSliceError(f"{label} cannot name holdout, sealed, credential, or forward storage")


def _reject_symlink_components(path: Path, *, label: str) -> None:
    """Reject every existing symlink component, not only the final leaf."""

    absolute = path if path.is_absolute() else Path.cwd() / path
    cursor = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        cursor /= part
        if cursor.is_symlink():
            raise RealSliceError(f"{label} cannot traverse a symbolic link")


def _resolve_existing_search_path(
    value: str | Path,
    *,
    label: str,
    kind: str | None = None,
) -> Path:
    """Resolve an existing search path without accepting lexical or symlink escapes."""

    text = os.fspath(value)
    requested = Path(text).expanduser()
    if not text or "\x00" in text or ".." in requested.parts:
        raise RealSliceError(f"{label} cannot contain traversal or an empty path")
    _reject_path_tokens(requested, label=label)
    _reject_symlink_components(requested, label=label)
    try:
        resolved = requested.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise RealSliceError(f"{label} does not resolve to an existing path") from error
    _reject_path_tokens(resolved, label=label)
    _reject_symlink_components(resolved, label=label)
    if kind == "file" and not resolved.is_file():
        raise RealSliceError(f"{label} must be a regular file")
    if kind == "directory" and not resolved.is_dir():
        raise RealSliceError(f"{label} must be a real directory")
    return resolved


def _relative_search_path(value: object, *, label: str) -> str:
    text = str(value)
    path = PurePosixPath(text)
    if (
        not text
        or text != text.strip()
        or "\\" in text
        or "\x00" in text
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
        or path.as_posix() != text
        or any(
            any(token in part.casefold() for token in _UNSAFE_STORAGE_TOKENS) for part in path.parts
        )
    ):
        raise RealSliceError(f"{label} must be a bounded search-only relative path")
    return text


def load_real_slice_config(path: str | Path) -> RealSliceConfig:
    """Load an exact allowlist; no directory discovery is part of this API."""

    _reject_holdout_environment()
    resolved = _resolve_existing_search_path(path, label="real-slice config", kind="file")
    raw = resolved.read_bytes()
    try:
        document = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise RealSliceError("real-slice config must be valid UTF-8 TOML") from error
    if set(document) != {
        "slice",
        "versions",
        "execution",
        "materialization",
        "sources",
        "cache_expectations",
        "previous_source_volume_context",
    }:
        raise RealSliceError("real-slice config tables differ from the frozen schema")
    section = document["slice"]
    versions = document["versions"]
    execution = document["execution"]
    materialization = document["materialization"]
    _require_exact_keys(
        section,
        {
            "schema_version",
            "slice_id",
            "source_schema",
            "reference_config",
            "reference_config_sha256",
            "source_manifest",
            "source_manifest_sha256",
            "staged_root",
            "source_dates",
            "trading_dates",
            "roles",
            "expected_contracts",
            "expected_instrument_ids",
            "active_selection_proven",
            "max_source_dates",
            "max_trading_dates",
            "research_authority",
            "session_policy",
        },
        label="slice",
    )
    _require_exact_keys(
        versions,
        {
            "source_adapter_version",
            "feature_version",
            "label_version",
            "execution_model_version",
        },
        label="versions",
    )
    _require_exact_keys(
        execution,
        {
            "tick_size_raw",
            "route_delay_seconds",
            "entry_adverse_ticks",
            "tp_trade_through_ticks",
            "round_trip_cost_ticks",
        },
        label="execution",
    )
    _require_exact_keys(
        materialization,
        {
            "window_start_seconds",
            "window_duration_seconds",
            "decision_clock_seconds",
            "atr_lookback_bars",
            "quantile_lookback_bars",
            "short_trend_lookback_bars",
            "batch_rows",
            "max_selected_raw_events",
            "max_quote_seconds",
            "max_feature_rows",
            "max_label_rows",
            "barrier_k_tp_numerators",
            "barrier_k_tp_denominator",
            "barrier_k_sl_numerators",
            "barrier_k_sl_denominator",
            "max_hold_seconds",
        },
        label="materialization",
    )
    for item in document["sources"]:
        _require_exact_keys(item, {"source_date", "relative_uri", "sha256"}, label="source")
    for item in document["cache_expectations"]:
        _require_exact_keys(
            item,
            {"source_date", "raw_symbol", "status"},
            label="cache expectation",
        )
    for item in document["previous_source_volume_context"]:
        _require_exact_keys(
            item,
            {
                "trading_date",
                "evidence_source_date",
                "selected_raw_symbol",
                "selected_trade_rows",
                "selected_trade_volume",
                "other_raw_symbol",
                "other_trade_rows",
                "other_trade_volume",
            },
            label="previous-source volume context",
        )
    source_dates = _date_axis(section.get("source_dates"), "source_dates")
    trading_dates = _date_axis(section.get("trading_dates"), "trading_dates")
    if section.get("schema_version") != 1:
        raise RealSliceError("only real-slice schema_version=1 is supported")
    if len(source_dates) > int(section.get("max_source_dates", 0)) or len(trading_dates) > int(
        section.get("max_trading_dates", 0)
    ):
        raise RealSliceError("real-slice allowlist exceeds its precommitted bound")
    roles = tuple(section.get("roles", ()))
    contracts = tuple(section.get("expected_contracts", ()))
    instrument_ids = tuple(section.get("expected_instrument_ids", ()))
    if not (len(trading_dates) == len(roles) == len(contracts) == len(instrument_ids) == 3):
        raise RealSliceError("M0b requires exactly normal, transition-context, and Friday sessions")
    if (
        roles
        != (
            "NORMAL",
            "CONTRACT_TRANSITION_CONTEXT_NOT_ACTIVE_SELECTION",
            "FRIDAY",
        )
        or trading_dates[-1].weekday() != 4
    ):
        raise RealSliceError("real-slice roles or Friday boundary are invalid")
    if contracts[0] == contracts[1] or contracts[1] != contracts[2]:
        raise RealSliceError("real-slice must contain one explicit contract transition context")
    active_selection_proven = tuple(section.get("active_selection_proven", ()))
    if active_selection_proven != (False, False, False):
        raise RealSliceError("schedule-only M0b cannot claim an active execution selection")
    authority = str(section.get("research_authority", ""))
    if authority != "SEARCH_ONLY_NOT_HOLDOUT_NOT_FORWARD":
        raise RealSliceError("M0b authority must remain search-only")
    if section.get("session_policy") != "NO_CROSS_CLOSED_MARKET":
        raise RealSliceError("M0b requires NO_CROSS_CLOSED_MARKET")
    sources = tuple(
        SourceSpec(
            item["source_date"],
            _relative_search_path(item["relative_uri"], label="source relative_uri"),
            str(item["sha256"]),
        )
        for item in document["sources"]
    )
    if tuple(item.source_date for item in sources) != source_dates:
        raise RealSliceError("source records must exactly match the source-date allowlist")
    cache = tuple(
        CacheExpectation(item["source_date"], str(item["raw_symbol"]), str(item["status"]))
        for item in document["cache_expectations"]
    )
    if tuple(item.source_date for item in cache) != trading_dates:
        raise RealSliceError("cache expectations must cover every trading date")
    if cache[1].status != "MISSING_BUILD_FROM_RAW":
        raise RealSliceError("the transition-date cache gap must be explicit")
    volume_context = tuple(
        PreviousSourceVolumeContext(
            trading_date=item["trading_date"],
            evidence_source_date=item["evidence_source_date"],
            selected_raw_symbol=str(item["selected_raw_symbol"]),
            selected_trade_rows=int(item["selected_trade_rows"]),
            selected_trade_volume=int(item["selected_trade_volume"]),
            other_raw_symbol=str(item["other_raw_symbol"]),
            other_trade_rows=int(item["other_trade_rows"]),
            other_trade_volume=int(item["other_trade_volume"]),
        )
        for item in document["previous_source_volume_context"]
    )
    if tuple(item.trading_date for item in volume_context) != trading_dates:
        raise RealSliceError("previous-source volume context must cover every staged session")
    for index, item in enumerate(volume_context):
        if (
            item.evidence_source_date >= item.trading_date
            or item.selected_raw_symbol != contracts[index]
            or min(
                item.selected_trade_rows,
                item.selected_trade_volume,
                item.other_trade_rows,
                item.other_trade_volume,
            )
            < 0
        ):
            raise RealSliceError("previous-source volume context is inconsistent")
    transition = volume_context[1]
    if transition.selected_trade_volume >= transition.other_trade_volume:
        raise RealSliceError("transition context must prove it was not the prior-volume winner")
    tick = int(execution.get("tick_size_raw", 0))
    if tick != 50_000:
        raise RealSliceError("M0b 6E tick_size_raw must be 50000")
    if (
        int(execution["route_delay_seconds"]),
        int(execution["entry_adverse_ticks"]),
        int(execution["tp_trade_through_ticks"]),
        int(execution["round_trip_cost_ticks"]),
    ) != (1, 1, 1, 2):
        raise RealSliceError("M0b execution assumptions differ from the frozen conservative v1")
    window_starts = tuple(int(item) for item in materialization["window_start_seconds"])
    if len(window_starts) != 3 or any(item < 0 for item in window_starts):
        raise RealSliceError("one non-negative materialization window offset is required per role")
    window_duration = int(materialization["window_duration_seconds"])
    decision_clock = int(materialization["decision_clock_seconds"])
    if window_duration <= 0 or decision_clock <= 0 or window_duration % decision_clock:
        raise RealSliceError("materialization window must contain whole decision bars")
    if any(item + window_duration > 23 * 3600 for item in window_starts):
        raise RealSliceError("materialization window escapes the regular 23-hour session")
    positive_scalars = {
        key: int(materialization[key])
        for key in (
            "atr_lookback_bars",
            "quantile_lookback_bars",
            "short_trend_lookback_bars",
            "batch_rows",
            "max_selected_raw_events",
            "max_quote_seconds",
            "max_feature_rows",
            "max_label_rows",
            "barrier_k_tp_denominator",
            "barrier_k_sl_denominator",
        )
    }
    if any(value <= 0 for value in positive_scalars.values()):
        raise RealSliceError("materialization bounds and lookbacks must be positive")
    k_tp = tuple(int(item) for item in materialization["barrier_k_tp_numerators"])
    k_sl = tuple(int(item) for item in materialization["barrier_k_sl_numerators"])
    holds = tuple(int(item) for item in materialization["max_hold_seconds"])
    if k_tp != (3, 4, 5) or k_sl != (2, 3, 4) or holds != (1800, 3600, 7200):
        raise RealSliceError("M0b barrier grid differs from the precommitted small grid")
    expected_features = len(trading_dates) * (window_duration // decision_clock)
    expected_labels = expected_features * 2 * len(k_tp) * len(k_sl) * len(holds)
    if expected_features > positive_scalars["max_feature_rows"]:
        raise RealSliceError("feature cardinality exceeds the precommitted materialization bound")
    if expected_labels > positive_scalars["max_label_rows"]:
        raise RealSliceError("label cardinality exceeds the precommitted materialization bound")
    return RealSliceConfig(
        slice_id=str(section["slice_id"]),
        source_schema=str(section["source_schema"]),
        reference_config=_relative_search_path(
            section["reference_config"], label="reference_config"
        ),
        reference_config_sha256=_sha256_text(
            section["reference_config_sha256"], label="reference_config_sha256"
        ),
        source_manifest=_relative_search_path(section["source_manifest"], label="source_manifest"),
        source_manifest_sha256=_sha256_text(
            section["source_manifest_sha256"], label="source_manifest_sha256"
        ),
        staged_root=_relative_search_path(section["staged_root"], label="staged_root"),
        source_dates=source_dates,
        trading_dates=trading_dates,
        roles=roles,
        expected_contracts=contracts,
        expected_instrument_ids=tuple(int(item) for item in instrument_ids),
        active_selection_proven=active_selection_proven,
        research_authority=authority,
        session_policy=str(section["session_policy"]),
        source_adapter_version=str(versions["source_adapter_version"]),
        feature_version=str(versions["feature_version"]),
        label_version=str(versions["label_version"]),
        execution_model_version=str(versions["execution_model_version"]),
        tick_size_raw=tick,
        route_delay_seconds=int(execution["route_delay_seconds"]),
        entry_adverse_ticks=int(execution["entry_adverse_ticks"]),
        tp_trade_through_ticks=int(execution["tp_trade_through_ticks"]),
        round_trip_cost_ticks=int(execution["round_trip_cost_ticks"]),
        window_start_seconds=window_starts,
        window_duration_seconds=window_duration,
        decision_clock_seconds=decision_clock,
        atr_lookback_bars=positive_scalars["atr_lookback_bars"],
        quantile_lookback_bars=positive_scalars["quantile_lookback_bars"],
        short_trend_lookback_bars=positive_scalars["short_trend_lookback_bars"],
        batch_rows=positive_scalars["batch_rows"],
        max_selected_raw_events=positive_scalars["max_selected_raw_events"],
        max_quote_seconds=positive_scalars["max_quote_seconds"],
        max_feature_rows=positive_scalars["max_feature_rows"],
        max_label_rows=positive_scalars["max_label_rows"],
        barrier_k_tp_numerators=k_tp,
        barrier_k_tp_denominator=positive_scalars["barrier_k_tp_denominator"],
        barrier_k_sl_numerators=k_sl,
        barrier_k_sl_denominator=positive_scalars["barrier_k_sl_denominator"],
        max_hold_seconds=holds,
        sources=sources,
        previous_source_volume_context=volume_context,
        cache_expectations=cache,
        config_hash=canonical_sha256(document),
        file_sha256=hashlib.sha256(raw).hexdigest(),
        manifest_path=resolved,
    )
