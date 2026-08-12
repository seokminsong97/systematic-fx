"""Immutable data models for the deterministic M0a research skeleton.

Prices are represented as integer instrument ticks and ratios as integer parts
per million.  Keeping binary floating point out of durable research artifacts
makes canonical hashes and replay results independent of platform details.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from typing import Any

from systematic_fx.research.hypotheses import canonical_sha256


class M0aError(RuntimeError):
    """Base class for fail-closed M0a errors."""


class M0aConfigError(M0aError):
    """An epoch manifest is unsafe or internally inconsistent."""


class M0aDataError(M0aError):
    """A fixture, feature, or label invariant was violated."""


class Direction(StrEnum):
    LONG = "long"
    SHORT = "short"


class FirstTouchType(StrEnum):
    TP_FIRST = "TP_FIRST"
    SL_FIRST = "SL_FIRST"
    TIMEOUT = "TIMEOUT"
    INVALID = "INVALID"


@dataclass(frozen=True, slots=True)
class BarrierSpec:
    barrier_id: str
    k_tp_num: int
    k_tp_den: int
    k_sl_num: int
    k_sl_den: int
    max_hold_seconds: int

    def __post_init__(self) -> None:
        if not self.barrier_id:
            raise M0aDataError("barrier_id must not be empty")
        if min(self.k_tp_num, self.k_tp_den, self.k_sl_num, self.k_sl_den) <= 0:
            raise M0aDataError("barrier multipliers must be positive fractions")
        if self.max_hold_seconds <= 0:
            raise M0aDataError("max_hold_seconds must be positive")

    def as_dict(self) -> dict[str, Any]:
        return {
            "barrier_id": self.barrier_id,
            "k_tp_num": self.k_tp_num,
            "k_tp_den": self.k_tp_den,
            "k_sl_num": self.k_sl_num,
            "k_sl_den": self.k_sl_den,
            "max_hold_seconds": self.max_hold_seconds,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> BarrierSpec:
        return cls(
            barrier_id=str(value["barrier_id"]),
            k_tp_num=int(value["k_tp_num"]),
            k_tp_den=int(value["k_tp_den"]),
            k_sl_num=int(value["k_sl_num"]),
            k_sl_den=int(value["k_sl_den"]),
            max_hold_seconds=int(value["max_hold_seconds"]),
        )


@dataclass(frozen=True, slots=True)
class InstrumentMetadata:
    instrument_id: int
    symbol: str
    tick_size_numerator: int
    tick_size_denominator: int
    expiry_ts_ns: int

    def __post_init__(self) -> None:
        if self.instrument_id <= 0 or not self.symbol:
            raise M0aDataError("instrument metadata requires a positive id and symbol")
        if self.tick_size_numerator <= 0 or self.tick_size_denominator <= 0:
            raise M0aDataError("tick size must be a positive rational")

    def as_dict(self) -> dict[str, Any]:
        return {
            "instrument_id": self.instrument_id,
            "symbol": self.symbol,
            "tick_size_numerator": self.tick_size_numerator,
            "tick_size_denominator": self.tick_size_denominator,
            "expiry_ts_ns": self.expiry_ts_ns,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> InstrumentMetadata:
        return cls(**{key: value[key] for key in cls.__dataclass_fields__})


@dataclass(frozen=True, slots=True)
class SessionWindow:
    session_id: str
    trading_date: date
    open_ts_ns: int
    close_ts_ns: int
    active_instrument_id: int

    def __post_init__(self) -> None:
        if not self.session_id or self.open_ts_ns >= self.close_ts_ns:
            raise M0aDataError("session window must have a non-empty id and positive span")
        if self.active_instrument_id <= 0:
            raise M0aDataError("session active instrument id must be positive")

    def as_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "trading_date": self.trading_date.isoformat(),
            "open_ts_ns": self.open_ts_ns,
            "close_ts_ns": self.close_ts_ns,
            "active_instrument_id": self.active_instrument_id,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> SessionWindow:
        return cls(
            session_id=str(value["session_id"]),
            trading_date=date.fromisoformat(str(value["trading_date"])),
            open_ts_ns=int(value["open_ts_ns"]),
            close_ts_ns=int(value["close_ts_ns"]),
            active_instrument_id=int(value["active_instrument_id"]),
        )


@dataclass(frozen=True, slots=True)
class PreviousDayVolume:
    trading_date: date
    observed_date: date
    volumes: tuple[tuple[int, int], ...]
    selected_instrument_id: int

    def __post_init__(self) -> None:
        if self.observed_date >= self.trading_date:
            raise M0aDataError("active-contract evidence must be known before trading_date")
        if not self.volumes or any(
            instrument <= 0 or volume < 0 for instrument, volume in self.volumes
        ):
            raise M0aDataError("previous-day volumes must be non-negative")
        ranked = sorted(self.volumes, key=lambda item: (-item[1], item[0]))
        if self.selected_instrument_id != ranked[0][0]:
            raise M0aDataError(
                "selected contract is not the deterministic previous-day-volume winner"
            )

    def as_dict(self) -> dict[str, Any]:
        return {
            "trading_date": self.trading_date.isoformat(),
            "observed_date": self.observed_date.isoformat(),
            "volumes": [[instrument, volume] for instrument, volume in self.volumes],
            "selected_instrument_id": self.selected_instrument_id,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> PreviousDayVolume:
        return cls(
            trading_date=date.fromisoformat(str(value["trading_date"])),
            observed_date=date.fromisoformat(str(value["observed_date"])),
            volumes=tuple((int(row[0]), int(row[1])) for row in value["volumes"]),
            selected_instrument_id=int(value["selected_instrument_id"]),
        )


@dataclass(frozen=True, slots=True)
class RollGuard:
    instrument_id: int
    start_ts_ns: int
    end_ts_ns: int
    reason: str

    def __post_init__(self) -> None:
        if self.instrument_id <= 0 or self.start_ts_ns >= self.end_ts_ns or not self.reason:
            raise M0aDataError("invalid roll guard")

    def contains(self, ts_ns: int, instrument_id: int) -> bool:
        return instrument_id == self.instrument_id and self.start_ts_ns <= ts_ns < self.end_ts_ns

    def as_dict(self) -> dict[str, Any]:
        return {
            "instrument_id": self.instrument_id,
            "start_ts_ns": self.start_ts_ns,
            "end_ts_ns": self.end_ts_ns,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> RollGuard:
        return cls(**{key: value[key] for key in cls.__dataclass_fields__})


@dataclass(frozen=True, slots=True)
class QuoteEvent:
    event_index: int
    ts_ns: int
    instrument_id: int
    session_id: str
    bid_ticks: int
    ask_ticks: int
    bid_size_l1: int
    ask_size_l1: int
    bid_depth_l10: int
    ask_depth_l10: int
    valid: bool = True
    trade_price_ticks: int | None = None
    trade_size: int = 0
    trade_action: str | None = None
    trade_aggressor_side: str | None = None

    def __post_init__(self) -> None:
        if self.event_index < 0 or self.ts_ns <= 0 or self.instrument_id <= 0:
            raise M0aDataError("quote event identifiers must be positive")
        if self.valid and self.bid_ticks >= self.ask_ticks:
            raise M0aDataError("valid quotes require bid below ask")
        if min(self.bid_size_l1, self.ask_size_l1, self.bid_depth_l10, self.ask_depth_l10) < 0:
            raise M0aDataError("quote sizes cannot be negative")
        if self.trade_price_ticks is None:
            if (
                self.trade_size != 0
                or self.trade_action is not None
                or self.trade_aggressor_side is not None
            ):
                raise M0aDataError("non-trade events cannot carry trade metadata")
        elif (
            self.trade_size <= 0
            or self.trade_action != "TRADE"
            or self.trade_aggressor_side not in {"BUY", "SELL", "UNKNOWN"}
        ):
            raise M0aDataError(
                "trade prints require price, positive size, action, and aggressor side"
            )

    def as_dict(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in self.__dataclass_fields__}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> QuoteEvent:
        fields = {key: value[key] for key in cls.__dataclass_fields__ if key in value}
        return cls(**fields)


@dataclass(frozen=True, slots=True)
class MarketFixture:
    fixture_version: str
    source_data_version: str
    dataset_hash: str
    instruments: tuple[InstrumentMetadata, ...]
    sessions: tuple[SessionWindow, ...]
    previous_day_volumes: tuple[PreviousDayVolume, ...]
    roll_guards: tuple[RollGuard, ...]
    quote_events: tuple[QuoteEvent, ...]

    def __post_init__(self) -> None:
        if len(self.dataset_hash) != 64:
            raise M0aDataError("dataset_hash must be a SHA-256 hex digest")
        instrument_ids = {item.instrument_id for item in self.instruments}
        if len(instrument_ids) != len(self.instruments):
            raise M0aDataError("instrument ids must be unique")
        session_ids = {item.session_id for item in self.sessions}
        if len(session_ids) != len(self.sessions):
            raise M0aDataError("session ids must be unique")
        last_key: tuple[int, int] | None = None
        for event in self.quote_events:
            if event.instrument_id not in instrument_ids or event.session_id not in session_ids:
                raise M0aDataError("quote event references unknown metadata")
            key = (event.ts_ns, event.event_index)
            if last_key is not None and key <= last_key:
                raise M0aDataError("quote events must have strict chronological sequence ordering")
            last_key = key

    def identity_payload(self) -> dict[str, Any]:
        return {
            "fixture_version": self.fixture_version,
            "source_data_version": self.source_data_version,
            "instruments": [item.as_dict() for item in self.instruments],
            "sessions": [item.as_dict() for item in self.sessions],
            "previous_day_volumes": [item.as_dict() for item in self.previous_day_volumes],
            "roll_guards": [item.as_dict() for item in self.roll_guards],
            "quote_events": [item.as_dict() for item in self.quote_events],
        }

    @property
    def content_sha256(self) -> str:
        return canonical_sha256(self.identity_payload())

    @property
    def events(self) -> tuple[QuoteEvent, ...]:
        return self.quote_events

    def as_dict(self) -> dict[str, Any]:
        return {"dataset_hash": self.dataset_hash, **self.identity_payload()}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> MarketFixture:
        return cls(
            fixture_version=str(value["fixture_version"]),
            source_data_version=str(value["source_data_version"]),
            dataset_hash=str(value["dataset_hash"]),
            instruments=tuple(InstrumentMetadata.from_dict(row) for row in value["instruments"]),
            sessions=tuple(SessionWindow.from_dict(row) for row in value["sessions"]),
            previous_day_volumes=tuple(
                PreviousDayVolume.from_dict(row) for row in value["previous_day_volumes"]
            ),
            roll_guards=tuple(RollGuard.from_dict(row) for row in value["roll_guards"]),
            quote_events=tuple(QuoteEvent.from_dict(row) for row in value["quote_events"]),
        )


@dataclass(frozen=True, slots=True)
class EventFeature:
    event_ts_ns: int
    instrument_id: int
    session_id: str
    trading_date: date
    feature_version: str
    source_data_version: str
    bar_open_ticks: int
    bar_high_ticks: int
    bar_low_ticks: int
    bar_close_ticks: int
    trailing_return_ticks: int
    range_ticks: int
    volatility_ticks: int
    body_ratio_ppm: int
    close_location_ppm: int
    short_trend_ticks: int
    pullback_length: int
    spread_ticks: int
    depth_imbalance_ppm: int
    volatility_quantile_ppm: int | None
    trend_30m_ticks: int | None
    context_30m_end_ns: int | None
    trend_1h_ticks: int | None
    context_1h_end_ns: int | None
    roll_cross: bool
    inside_roll_guard: bool
    feature_valid: bool
    validity_flags: tuple[str, ...]

    @property
    def event_ts(self) -> int:
        return self.event_ts_ns

    def as_dict(self) -> dict[str, Any]:
        value = {key: getattr(self, key) for key in self.__dataclass_fields__}
        value["trading_date"] = self.trading_date.isoformat()
        value["validity_flags"] = list(self.validity_flags)
        return value

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> EventFeature:
        values = {key: value[key] for key in cls.__dataclass_fields__}
        values["trading_date"] = date.fromisoformat(str(values["trading_date"]))
        values["validity_flags"] = tuple(values["validity_flags"])
        return cls(**values)


@dataclass(frozen=True, slots=True)
class QuoteAwareLabel:
    event_ts_ns: int
    instrument_id: int
    direction: Direction
    barrier_id: str
    k_tp_num: int
    k_tp_den: int
    k_sl_num: int
    k_sl_den: int
    max_hold_seconds: int
    entry_ts_ns: int | None
    entry_price_ticks: int | None
    tp_price_ticks: int | None
    sl_price_ticks: int | None
    first_touch_type: FirstTouchType
    first_touch_ts_ns: int | None
    exit_ts_ns: int | None
    exit_price_ticks: int | None
    timeout: bool
    ambiguous: bool
    raw_fallback_used: bool
    cost_ticks: int
    gross_pnl_ticks: int | None
    net_pnl_ticks: int | None
    label_version: str
    eligible: bool
    invalid_reason: str | None

    def __post_init__(self) -> None:
        if self.eligible == (self.first_touch_type is FirstTouchType.INVALID):
            raise M0aDataError("eligible and first_touch_type disagree")
        if not self.eligible and not self.invalid_reason:
            raise M0aDataError("invalid labels require a reason")
        if self.timeout != (self.first_touch_type is FirstTouchType.TIMEOUT):
            raise M0aDataError("timeout flag and first_touch_type disagree")

    @property
    def event_ts(self) -> int:
        return self.event_ts_ns

    @property
    def entry_ts(self) -> int | None:
        return self.entry_ts_ns

    @property
    def first_touch_ts(self) -> int | None:
        return self.first_touch_ts_ns

    @property
    def exit_ts(self) -> int | None:
        return self.exit_ts_ns

    @property
    def entry_price(self) -> int | None:
        return self.entry_price_ticks

    @property
    def tp_price(self) -> int | None:
        return self.tp_price_ticks

    @property
    def sl_price(self) -> int | None:
        return self.sl_price_ticks

    @property
    def exit_price(self) -> int | None:
        return self.exit_price_ticks

    def as_dict(self) -> dict[str, Any]:
        value = {key: getattr(self, key) for key in self.__dataclass_fields__}
        value["direction"] = self.direction.value
        value["first_touch_type"] = self.first_touch_type.value
        return value

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> QuoteAwareLabel:
        values = {key: value[key] for key in cls.__dataclass_fields__}
        values["direction"] = Direction(str(values["direction"]))
        values["first_touch_type"] = FirstTouchType(str(values["first_touch_type"]))
        return cls(**values)


# Short aliases are convenient for future stores without making the durable names vague.
FeatureRow = EventFeature
LabelRow = QuoteAwareLabel
