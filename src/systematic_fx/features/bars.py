"""Point-in-time, trade-only bars for bar-first screening research.

Raw MBP-10 is decoded only by :func:`build_daily_trade_bar_artifacts`.  Its
published Parquet artifacts contain ordinary trade OHLCV fields and exact
next-bar linkage metadata; book depth, event actions, sequence numbers, raw
prices, and provider instrument identifiers are deliberately absent.

The canonical base layer is one-second trade OHLCV.  Wider bars are derived
associatively from that layer, so direct and staged resampling produce the same
OHLCV and ``observed_subbars`` values.  All availability and bucket decisions
use ``ts_recv`` with left-closed, right-open UTC intervals.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import stat
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import Final

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from systematic_fx.data.contract_selection import resolve_6e_contract_month
from systematic_fx.data.contracts import UNDEFINED_PRICE, decode_dbn_metadata
from systematic_fx.data.instruments import (
    InstrumentKind,
    InstrumentMapping,
    parse_instrument_mappings,
)

BAR_VERSION: Final = "trade_bar_v1"
BAR_SCHEMA: Final = "systematic_fx.trade_bar.v1"
PLAN_SCHEMA: Final = "systematic_fx.trade_bar_daily_plan.v1"
REPORT_SCHEMA: Final = "systematic_fx.trade_bar_daily_build_report.v1"
VOLUME_SCHEMA: Final = "systematic_fx.trade_bar_daily_volume.v1"
SEGMENT_POLICY_VERSION: Final = "trade_gap_segment_v1"
SELECTION_POLICY_VERSION: Final = "previous_eligible_source_volume_v1"

PRICE_SCALE: Final = "1e-9"
TICK_SIZE_RAW: Final = 50_000
ONE_SECOND_NS: Final = 1_000_000_000
DEFAULT_GAP_BREAK_SECONDS: Final = 3_600
SUPPORTED_TIMEFRAMES_SECONDS: Final = (1, 60, 300, 1_800, 3_600)
TIMEFRAME_LABELS: Final = {
    1: "1s",
    60: "1m",
    300: "5m",
    1_800: "30m",
    3_600: "1h",
}
F_BAD_TS_RECV: Final = 8

# The structural-QC result preserved these source dates as hard failures.  They
# are excluded before their rows can affect bars or the next eligible volume
# summary.
QC_EXCLUDED_SOURCE_DATES: Final = frozenset(
    {
        date(2024, 6, 30),
        date(2024, 7, 1),
        date(2024, 7, 14),
        date(2026, 4, 19),
        date(2026, 6, 7),
        date(2026, 6, 21),
    }
)

_UINT32_MAX: Final = 2**32 - 1
_UINT64_MAX: Final = 2**64 - 1
_INT64_MAX: Final = 2**63 - 1
_SHA256_LENGTH: Final = 64


class TradeBarError(ValueError):
    """A bar plan, trade input, resample, or publication is invalid."""


class DailyPlanStatus(StrEnum):
    """Whether one source date is allowed to produce selected-contract bars."""

    SELECTED = "SELECTED"
    QC_EXCLUDED = "QC_EXCLUDED"
    NO_PREVIOUS_ELIGIBLE_SOURCE = "NO_PREVIOUS_ELIGIBLE_SOURCE"
    NO_ELIGIBLE_CONTRACT = "NO_ELIGIBLE_CONTRACT"
    NO_POSITIVE_PREVIOUS_VOLUME = "NO_POSITIVE_PREVIOUS_VOLUME"


class NextBarLinkStatus(StrEnum):
    """Relationship between a closed bar and the next row in a supplied span."""

    EXACT_NEXT_BAR = "EXACT_NEXT_BAR"
    GAP = "GAP"
    SEGMENT_BOUNDARY = "SEGMENT_BOUNDARY"
    PARTITION_END = "PARTITION_END"


def _require_int(
    value: object,
    *,
    label: str,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TradeBarError(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        raise TradeBarError(f"{label} must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise TradeBarError(f"{label} must be <= {maximum}")
    return value


def _require_date(value: object, *, label: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise TradeBarError(f"{label} must be a date")
    return value


def _require_sha256(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _SHA256_LENGTH
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise TradeBarError(f"{label} must be a lowercase SHA-256")
    return value


def _nonempty(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TradeBarError(f"{label} must be a non-empty string")
    return value


def _canonical_bytes(value: object) -> bytes:
    try:
        return (
            json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("ascii")
            + b"\n"
        )
    except (TypeError, ValueError) as error:
        raise TradeBarError("canonical payload is not JSON-serializable") from error


def canonical_sha256(value: object) -> str:
    """Return the canonical SHA-256 used by plans, summaries, and reports."""

    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _source_date_for_ns(value: int) -> date:
    try:
        return datetime.fromtimestamp(value // ONE_SECOND_NS, tz=UTC).date()
    except (OSError, OverflowError, ValueError) as error:
        raise TradeBarError("timestamp is outside the supported UTC range") from error


@dataclass(frozen=True, slots=True)
class TradePrint:
    """One selected-contract trade before integer-tick aggregation.

    ``physical_ordinal`` is the zero-based row ordinal in the daily source.  It
    is retained only long enough to make equal-timestamp ordering deterministic
    and is never written to a bar artifact.
    """

    ts_recv_ns: int
    sequence: int
    physical_ordinal: int
    price_raw: int
    size: int
    side: str

    def __post_init__(self) -> None:
        _require_int(self.ts_recv_ns, label="ts_recv_ns", minimum=0)
        _require_int(self.sequence, label="sequence", minimum=0, maximum=_UINT32_MAX)
        _require_int(self.physical_ordinal, label="physical_ordinal", minimum=0)
        price = _require_int(self.price_raw, label="price_raw")
        if price == UNDEFINED_PRICE:
            raise TradeBarError("trade price cannot use the undefined-price sentinel")
        if price <= 0:
            raise TradeBarError("trade price must be positive")
        if price % TICK_SIZE_RAW:
            raise TradeBarError("trade price is off the 6E tick grid")
        _require_int(self.size, label="size", minimum=0, maximum=_UINT32_MAX)
        _nonempty(self.side, label="side")

    @property
    def price_ticks(self) -> int:
        return self.price_raw // TICK_SIZE_RAW

    @property
    def ordering_key(self) -> tuple[int, int, int]:
        return self.ts_recv_ns, self.sequence, self.physical_ordinal


@dataclass(frozen=True, slots=True)
class TradeBar:
    """Neutral, replay-consumable selected-contract trade OHLCV bar.

    ``observed_subbars`` always counts observed one-second base bars.  That
    convention is what makes repeated resampling associative.
    """

    timeframe_seconds: int
    segment_id: int
    contract: str
    source_date: date
    start_ns: int
    end_ns: int
    first_trade_ns: int
    last_trade_ns: int
    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int
    trade_count: int
    volume: int
    observed_subbars: int
    buy_volume: int | None = None
    sell_volume: int | None = None

    def __post_init__(self) -> None:
        timeframe = _require_int(
            self.timeframe_seconds,
            label="timeframe_seconds",
            minimum=1,
        )
        if timeframe not in SUPPORTED_TIMEFRAMES_SECONDS:
            raise TradeBarError("unsupported trade-bar timeframe")
        _require_int(
            self.segment_id,
            label="segment_id",
            minimum=1,
            maximum=_UINT64_MAX,
        )
        _nonempty(self.contract, label="contract")
        source_date = _require_date(self.source_date, label="source_date")
        start = _require_int(self.start_ns, label="start_ns", minimum=0)
        end = _require_int(self.end_ns, label="end_ns", minimum=1)
        width = timeframe * ONE_SECOND_NS
        if start % width or end != start + width:
            raise TradeBarError("bar must use an aligned [start,end) UTC interval")
        first = _require_int(self.first_trade_ns, label="first_trade_ns", minimum=0)
        last = _require_int(self.last_trade_ns, label="last_trade_ns", minimum=0)
        if not start <= first <= last < end:
            raise TradeBarError("trade timestamps must lie inside [start,end)")
        if _source_date_for_ns(start) != source_date:
            raise TradeBarError("bar start UTC date differs from source_date")
        prices = {
            name: _require_int(getattr(self, name), label=name, minimum=1)
            for name in ("open_ticks", "high_ticks", "low_ticks", "close_ticks")
        }
        if not (
            prices["low_ticks"]
            <= min(prices["open_ticks"], prices["close_ticks"])
            <= max(prices["open_ticks"], prices["close_ticks"])
            <= prices["high_ticks"]
        ):
            raise TradeBarError("OHLC tick ordering is invalid")
        _require_int(self.trade_count, label="trade_count", minimum=1, maximum=_UINT64_MAX)
        volume = _require_int(self.volume, label="volume", minimum=0, maximum=_UINT64_MAX)
        observed = _require_int(
            self.observed_subbars,
            label="observed_subbars",
            minimum=1,
            maximum=timeframe,
        )
        if timeframe == 1 and observed != 1:
            raise TradeBarError("one-second bars must have one observed subbar")
        classified = 0
        for field_name in ("buy_volume", "sell_volume"):
            value = getattr(self, field_name)
            if value is not None:
                classified += _require_int(
                    value,
                    label=field_name,
                    minimum=0,
                    maximum=_UINT64_MAX,
                )
        if (self.buy_volume is None) != (self.sell_volume is None):
            raise TradeBarError("buy_volume and sell_volume must both be present or both null")
        if classified > volume:
            raise TradeBarError("classified side volume exceeds total volume")

    def as_dict(self) -> dict[str, object]:
        return {
            "buy_volume": self.buy_volume,
            "close_ticks": self.close_ticks,
            "contract": self.contract,
            "end_ns": self.end_ns,
            "first_trade_ns": self.first_trade_ns,
            "high_ticks": self.high_ticks,
            "last_trade_ns": self.last_trade_ns,
            "low_ticks": self.low_ticks,
            "observed_subbars": self.observed_subbars,
            "open_ticks": self.open_ticks,
            "segment_id": self.segment_id,
            "sell_volume": self.sell_volume,
            "source_date": self.source_date.isoformat(),
            "start_ns": self.start_ns,
            "timeframe_seconds": self.timeframe_seconds,
            "trade_count": self.trade_count,
            "volume": self.volume,
        }


@dataclass(frozen=True, slots=True)
class SegmentTail:
    """The only cross-partition state needed to continue a gap segment."""

    contract: str
    segment_id: int
    source_date: date
    last_bar_end_ns: int
    last_trade_ns: int

    def __post_init__(self) -> None:
        _nonempty(self.contract, label="contract")
        _require_int(
            self.segment_id,
            label="segment_id",
            minimum=1,
            maximum=_UINT64_MAX,
        )
        _require_date(self.source_date, label="source_date")
        end = _require_int(self.last_bar_end_ns, label="last_bar_end_ns", minimum=1)
        last = _require_int(self.last_trade_ns, label="last_trade_ns", minimum=0)
        if last >= end:
            raise TradeBarError("segment tail trade must precede its bar end")

    def as_dict(self) -> dict[str, object]:
        return {
            "contract": self.contract,
            "last_bar_end_ns": self.last_bar_end_ns,
            "last_trade_ns": self.last_trade_ns,
            "segment_id": self.segment_id,
            "source_date": self.source_date.isoformat(),
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


@dataclass(frozen=True, slots=True)
class NextBarLink:
    """Exact next-bar metadata kept separate from the neutral ``TradeBar``."""

    timeframe_seconds: int
    segment_id: int
    contract: str
    source_date: date
    current_start_ns: int
    current_end_ns: int
    status: NextBarLinkStatus
    next_bar_start_ns: int | None
    next_first_trade_ns: int | None

    def as_dict(self) -> dict[str, object]:
        return {
            "contract": self.contract,
            "current_end_ns": self.current_end_ns,
            "current_start_ns": self.current_start_ns,
            "next_bar_start_ns": self.next_bar_start_ns,
            "next_first_trade_ns": self.next_first_trade_ns,
            "segment_id": self.segment_id,
            "source_date": self.source_date.isoformat(),
            "status": self.status.value,
            "timeframe_seconds": self.timeframe_seconds,
        }


@dataclass(frozen=True, slots=True)
class ContractVolume:
    """Previous eligible source-date trade evidence for one contract month."""

    contract_month: date
    instrument_ids: tuple[int, ...]
    raw_symbols: tuple[str, ...]
    trade_count: int
    volume: int

    def __post_init__(self) -> None:
        _require_date(self.contract_month, label="contract_month")
        if not self.instrument_ids or not self.raw_symbols:
            raise TradeBarError("contract volume must retain mapping identity")
        if tuple(sorted(set(self.instrument_ids))) != self.instrument_ids:
            raise TradeBarError("contract-volume instrument_ids must be sorted and unique")
        if tuple(sorted(set(self.raw_symbols))) != self.raw_symbols:
            raise TradeBarError("contract-volume symbols must be sorted and unique")
        for instrument_id in self.instrument_ids:
            _require_int(
                instrument_id,
                label="instrument_id",
                minimum=0,
                maximum=_UINT32_MAX,
            )
        for symbol in self.raw_symbols:
            _nonempty(symbol, label="raw_symbol")
        _require_int(self.trade_count, label="trade_count", minimum=0, maximum=_UINT64_MAX)
        _require_int(self.volume, label="volume", minimum=0, maximum=_UINT64_MAX)

    def as_dict(self) -> dict[str, object]:
        return {
            "contract_month": self.contract_month.isoformat(),
            "instrument_ids": list(self.instrument_ids),
            "raw_symbols": list(self.raw_symbols),
            "trade_count": self.trade_count,
            "volume": self.volume,
        }


@dataclass(frozen=True, slots=True)
class DailyVolumeSummary:
    """Immutable trade-volume evidence from one QC-eligible source date."""

    source_date: date
    source_sha256: str
    qc_eligible: bool
    contracts: tuple[ContractVolume, ...]

    def __post_init__(self) -> None:
        _require_date(self.source_date, label="source_date")
        _require_sha256(self.source_sha256, label="source_sha256")
        if not isinstance(self.qc_eligible, bool):
            raise TradeBarError("qc_eligible must be a boolean")
        months = tuple(item.contract_month for item in self.contracts)
        if tuple(sorted(months)) != months or len(set(months)) != len(months):
            raise TradeBarError("daily contract volumes must be unique and month-sorted")

    def payload(self) -> dict[str, object]:
        return {
            "artifact_schema": VOLUME_SCHEMA,
            "contracts": [item.as_dict() for item in self.contracts],
            "qc_eligible": self.qc_eligible,
            "source_date": self.source_date.isoformat(),
            "source_sha256": self.source_sha256,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.payload())

    def as_dict(self) -> dict[str, object]:
        result = self.payload()
        result["sha256"] = self.sha256
        return result


@dataclass(frozen=True, slots=True)
class DailyBarPlan:
    """Canonical point-in-time selection decision for one source date."""

    source_date: date
    source_sha256: str
    status: DailyPlanStatus
    mapping_sha256: str
    previous_volume_sha256: str | None
    previous_source_date: date | None
    selected_instrument_id: int | None
    selected_contract: str | None
    selected_contract_month: date | None
    selected_previous_trade_count: int | None
    selected_previous_volume: int | None
    previous_segment_tail_sha256: str | None
    gap_break_seconds: int

    def __post_init__(self) -> None:
        _require_date(self.source_date, label="source_date")
        _require_sha256(self.source_sha256, label="source_sha256")
        if not isinstance(self.status, DailyPlanStatus):
            raise TradeBarError("status must be a DailyPlanStatus")
        _require_sha256(self.mapping_sha256, label="mapping_sha256")
        _require_int(self.gap_break_seconds, label="gap_break_seconds", minimum=1)
        if self.previous_volume_sha256 is not None:
            _require_sha256(self.previous_volume_sha256, label="previous_volume_sha256")
        if self.previous_segment_tail_sha256 is not None:
            _require_sha256(
                self.previous_segment_tail_sha256,
                label="previous_segment_tail_sha256",
            )
        selected_values = (
            self.selected_instrument_id,
            self.selected_contract,
            self.selected_contract_month,
            self.selected_previous_trade_count,
            self.selected_previous_volume,
        )
        if self.status is DailyPlanStatus.SELECTED:
            if any(value is None for value in selected_values):
                raise TradeBarError("SELECTED plan is missing selected-contract identity")
            assert self.selected_instrument_id is not None
            assert self.selected_contract is not None
            assert self.selected_contract_month is not None
            assert self.selected_previous_trade_count is not None
            assert self.selected_previous_volume is not None
            _require_int(
                self.selected_instrument_id,
                label="selected_instrument_id",
                minimum=0,
                maximum=_UINT32_MAX,
            )
            _nonempty(self.selected_contract, label="selected_contract")
            _require_date(self.selected_contract_month, label="selected_contract_month")
            _require_int(
                self.selected_previous_trade_count,
                label="selected_previous_trade_count",
                minimum=1,
            )
            _require_int(
                self.selected_previous_volume,
                label="selected_previous_volume",
                minimum=1,
            )
        elif any(value is not None for value in selected_values):
            raise TradeBarError("non-selected plan cannot retain selected-contract fields")
        if (self.previous_source_date is None) != (self.previous_volume_sha256 is None):
            raise TradeBarError("previous source date/hash identity is incomplete")

    def payload(self) -> dict[str, object]:
        return {
            "artifact_schema": PLAN_SCHEMA,
            "bar_version": BAR_VERSION,
            "gap_break_seconds": self.gap_break_seconds,
            "mapping_sha256": self.mapping_sha256,
            "previous_segment_tail_sha256": self.previous_segment_tail_sha256,
            "previous_source_date": (
                self.previous_source_date.isoformat()
                if self.previous_source_date is not None
                else None
            ),
            "previous_volume_sha256": self.previous_volume_sha256,
            "qc_exclusion_policy_sha256": QC_EXCLUSION_POLICY_SHA256,
            "selected_contract": self.selected_contract,
            "selected_contract_month": (
                self.selected_contract_month.isoformat()
                if self.selected_contract_month is not None
                else None
            ),
            "selected_instrument_id": self.selected_instrument_id,
            "selected_previous_trade_count": self.selected_previous_trade_count,
            "selected_previous_volume": self.selected_previous_volume,
            "selection_policy_sha256": SELECTION_POLICY_SHA256,
            "selection_policy_version": SELECTION_POLICY_VERSION,
            "segment_policy_version": SEGMENT_POLICY_VERSION,
            "source_date": self.source_date.isoformat(),
            "source_sha256": self.source_sha256,
            "status": self.status.value,
            "timeframes_seconds": list(SUPPORTED_TIMEFRAMES_SECONDS),
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.payload())

    def as_dict(self) -> dict[str, object]:
        result = self.payload()
        result["sha256"] = self.sha256
        return result


@dataclass(frozen=True, slots=True)
class TradeBarArtifactDescriptor:
    """Semantic artifact identity persisted in a canonical manifest."""

    timeframe_seconds: int
    relative_uri: str
    sha256: str
    byte_size: int
    row_count: int

    def __post_init__(self) -> None:
        if self.timeframe_seconds not in SUPPORTED_TIMEFRAMES_SECONDS:
            raise TradeBarError("artifact timeframe is unsupported")
        _nonempty(self.relative_uri, label="relative_uri")
        _require_sha256(self.sha256, label="artifact_sha256")
        _require_int(self.byte_size, label="byte_size", minimum=1)
        _require_int(self.row_count, label="row_count", minimum=1)

    def semantic_dict(self) -> dict[str, object]:
        return {
            "byte_size": self.byte_size,
            "relative_uri": self.relative_uri,
            "row_count": self.row_count,
            "sha256": self.sha256,
            "timeframe_seconds": self.timeframe_seconds,
        }

    def as_dict(self) -> dict[str, object]:
        return self.semantic_dict()

    @staticmethod
    def from_mapping(
        value: Mapping[str, object],
    ) -> TradeBarArtifactDescriptor:
        """Restore the exact disposition-free descriptor stored in a manifest."""

        if not isinstance(value, Mapping):
            raise TradeBarError("artifact descriptor payload must be a mapping")
        fields = {
            "byte_size",
            "relative_uri",
            "row_count",
            "sha256",
            "timeframe_seconds",
        }
        if set(value) != fields:
            raise TradeBarError("artifact descriptor payload fields are not canonical")
        return TradeBarArtifactDescriptor(
            timeframe_seconds=_require_int(
                value["timeframe_seconds"],
                label="timeframe_seconds",
                minimum=1,
            ),
            relative_uri=_nonempty(value["relative_uri"], label="relative_uri"),
            sha256=_require_sha256(value["sha256"], label="artifact_sha256"),
            byte_size=_require_int(value["byte_size"], label="byte_size", minimum=1),
            row_count=_require_int(value["row_count"], label="row_count", minimum=1),
        )


@dataclass(frozen=True, slots=True)
class BarArtifact(TradeBarArtifactDescriptor):
    """Build-time publication descriptor with non-semantic disposition."""

    disposition: str

    def __post_init__(self) -> None:
        TradeBarArtifactDescriptor.__post_init__(self)
        if self.disposition not in {"CREATED", "REUSED"}:
            raise TradeBarError("artifact disposition must be CREATED or REUSED")

    def as_dict(self) -> dict[str, object]:
        result = self.semantic_dict()
        result["disposition"] = self.disposition
        return result


@dataclass(frozen=True, slots=True)
class DailyBarBuildReport:
    """Deterministic result of one efficient daily projection and publication."""

    plan: DailyBarPlan
    source_scanned: bool
    source_row_count: int
    source_trade_count: int
    selected_trade_count: int
    bad_ts_recv_trades_excluded: int
    current_volume_summary: DailyVolumeSummary | None
    segment_tail: SegmentTail | None
    artifacts: tuple[BarArtifact, ...]
    link_sha256_by_timeframe: tuple[tuple[int, str], ...]

    def semantic_payload(self) -> dict[str, object]:
        return {
            "artifact_schema": REPORT_SCHEMA,
            "artifacts": [artifact.semantic_dict() for artifact in self.artifacts],
            "bad_ts_recv_trades_excluded": self.bad_ts_recv_trades_excluded,
            "bar_version": BAR_VERSION,
            "current_volume_sha256": (
                self.current_volume_summary.sha256
                if self.current_volume_summary is not None
                else None
            ),
            "link_sha256_by_timeframe": [
                {"sha256": digest, "timeframe_seconds": timeframe}
                for timeframe, digest in self.link_sha256_by_timeframe
            ],
            "plan_sha256": self.plan.sha256,
            "segment_tail": self.segment_tail.as_dict() if self.segment_tail else None,
            "selected_trade_count": self.selected_trade_count,
            "source_row_count": self.source_row_count,
            "source_scanned": self.source_scanned,
            "source_trade_count": self.source_trade_count,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.semantic_payload())

    def as_dict(self) -> dict[str, object]:
        result = self.semantic_payload()
        result.update(
            {
                "artifacts": [artifact.as_dict() for artifact in self.artifacts],
                "current_volume_summary": (
                    self.current_volume_summary.as_dict()
                    if self.current_volume_summary is not None
                    else None
                ),
                "plan": self.plan.as_dict(),
                "sha256": self.sha256,
            }
        )
        return result


QC_EXCLUSION_POLICY_SHA256: Final = canonical_sha256(
    {
        "dates": sorted(item.isoformat() for item in QC_EXCLUDED_SOURCE_DATES),
        "effect": "EXCLUDE_SOURCE_FROM_BARS_AND_PREVIOUS_ELIGIBLE_VOLUME",
        "version": "mbp10_six_source_date_exclusion_v1",
    }
)

SELECTION_POLICY_SHA256: Final = canonical_sha256(
    {
        "availability": "PREVIOUS_ELIGIBLE_SOURCE_DATE_ONLY",
        "contract_month": "STRICTLY_AFTER_SOURCE_CALENDAR_MONTH",
        "positive_previous_trade_count_required": True,
        "positive_previous_volume_required": True,
        "ranking": [
            "previous_volume_desc",
            "contract_month",
            "instrument_id",
            "raw_symbol",
        ],
        "version": SELECTION_POLICY_VERSION,
    }
)


def _active_6e_outrights(
    mappings: Iterable[InstrumentMapping],
    *,
    source_date: date,
) -> tuple[tuple[int, str, date], ...]:
    resolved_date = _require_date(source_date, label="source_date")
    by_id: dict[int, tuple[str, date]] = {}
    by_symbol: dict[str, int] = {}
    for mapping in mappings:
        if not isinstance(mapping, InstrumentMapping):
            raise TradeBarError("mappings must contain InstrumentMapping values")
        if not mapping.interval_start <= resolved_date < mapping.interval_end:
            continue
        if mapping.kind is not InstrumentKind.OUTRIGHT:
            continue
        try:
            contract_month = resolve_6e_contract_month(
                mapping.raw_symbol,
                source_date=resolved_date,
            )
        except ValueError:
            continue
        prior_id = by_id.get(mapping.instrument_id)
        if prior_id is not None and prior_id != (mapping.raw_symbol, contract_month):
            raise TradeBarError("active provider instrument maps to multiple 6E contracts")
        prior_symbol = by_symbol.get(mapping.raw_symbol)
        if prior_symbol is not None and prior_symbol != mapping.instrument_id:
            raise TradeBarError("active 6E symbol maps to multiple provider instruments")
        by_id[mapping.instrument_id] = (mapping.raw_symbol, contract_month)
        by_symbol[mapping.raw_symbol] = mapping.instrument_id
    return tuple(
        sorted(
            (
                (instrument_id, symbol, contract_month)
                for instrument_id, (symbol, contract_month) in by_id.items()
            ),
            key=lambda item: (item[2], item[0], item[1]),
        )
    )


def make_daily_volume_summary(
    *,
    source_date: date,
    source_sha256: str,
    mappings: Iterable[InstrumentMapping],
    totals_by_instrument: Mapping[int, tuple[int, int]],
    qc_eligible: bool = True,
) -> DailyVolumeSummary:
    """Aggregate current trade totals by mapped 6E contract month."""

    resolved_date = _require_date(source_date, label="source_date")
    source_sha = _require_sha256(source_sha256, label="source_sha256")
    if not isinstance(qc_eligible, bool):
        raise TradeBarError("qc_eligible must be a boolean")
    aggregate: dict[date, dict[str, object]] = {}
    for instrument_id, symbol, contract_month in _active_6e_outrights(
        mappings,
        source_date=resolved_date,
    ):
        totals = totals_by_instrument.get(instrument_id, (0, 0))
        if not isinstance(totals, tuple) or len(totals) != 2:
            raise TradeBarError("instrument totals must be (trade_count, volume) tuples")
        trade_count = _require_int(
            totals[0],
            label="instrument trade_count",
            minimum=0,
            maximum=_UINT64_MAX,
        )
        volume = _require_int(
            totals[1],
            label="instrument volume",
            minimum=0,
            maximum=_UINT64_MAX,
        )
        state = aggregate.setdefault(
            contract_month,
            {"instrument_ids": [], "raw_symbols": [], "trade_count": 0, "volume": 0},
        )
        state["instrument_ids"].append(instrument_id)  # type: ignore[union-attr]
        state["raw_symbols"].append(symbol)  # type: ignore[union-attr]
        state["trade_count"] = int(state["trade_count"]) + trade_count
        state["volume"] = int(state["volume"]) + volume
    contracts = tuple(
        ContractVolume(
            contract_month=contract_month,
            instrument_ids=tuple(sorted(set(state["instrument_ids"]))),  # type: ignore[arg-type]
            raw_symbols=tuple(sorted(set(state["raw_symbols"]))),  # type: ignore[arg-type]
            trade_count=int(state["trade_count"]),
            volume=int(state["volume"]),
        )
        for contract_month, state in sorted(aggregate.items())
    )
    return DailyVolumeSummary(
        source_date=resolved_date,
        source_sha256=source_sha,
        qc_eligible=qc_eligible,
        contracts=contracts,
    )


def _mapping_sha256(mappings: Iterable[InstrumentMapping], *, source_date: date) -> str:
    return canonical_sha256(
        [
            {
                "contract_month": contract_month.isoformat(),
                "instrument_id": instrument_id,
                "raw_symbol": symbol,
            }
            for instrument_id, symbol, contract_month in _active_6e_outrights(
                mappings,
                source_date=source_date,
            )
        ]
    )


def plan_daily_trade_bars(
    *,
    source_date: date,
    source_sha256: str,
    mappings: Iterable[InstrumentMapping],
    previous_volume_summary: DailyVolumeSummary | None,
    previous_segment_tail: SegmentTail | None = None,
    gap_break_seconds: int = DEFAULT_GAP_BREAK_SECONDS,
) -> DailyBarPlan:
    """Select one current contract without reading any current-date trade row."""

    resolved_date = _require_date(source_date, label="source_date")
    source_sha = _require_sha256(source_sha256, label="source_sha256")
    gap = _require_int(gap_break_seconds, label="gap_break_seconds", minimum=1)
    mapping_values = tuple(mappings)
    mapping_sha = _mapping_sha256(mapping_values, source_date=resolved_date)
    prior_tail_sha = previous_segment_tail.sha256 if previous_segment_tail else None

    common: dict[str, object] = {
        "source_date": resolved_date,
        "source_sha256": source_sha,
        "mapping_sha256": mapping_sha,
        "previous_segment_tail_sha256": prior_tail_sha,
        "gap_break_seconds": gap,
    }
    previous_fields: dict[str, object] = {
        "previous_volume_sha256": None,
        "previous_source_date": None,
    }
    empty_selection: dict[str, object] = {
        "selected_instrument_id": None,
        "selected_contract": None,
        "selected_contract_month": None,
        "selected_previous_trade_count": None,
        "selected_previous_volume": None,
    }
    if resolved_date in QC_EXCLUDED_SOURCE_DATES:
        return DailyBarPlan(
            status=DailyPlanStatus.QC_EXCLUDED,
            **common,  # type: ignore[arg-type]
            **previous_fields,  # type: ignore[arg-type]
            **empty_selection,  # type: ignore[arg-type]
        )
    if previous_volume_summary is None:
        return DailyBarPlan(
            status=DailyPlanStatus.NO_PREVIOUS_ELIGIBLE_SOURCE,
            **common,  # type: ignore[arg-type]
            **previous_fields,  # type: ignore[arg-type]
            **empty_selection,  # type: ignore[arg-type]
        )
    if not isinstance(previous_volume_summary, DailyVolumeSummary):
        raise TradeBarError("previous_volume_summary has the wrong type")
    if not previous_volume_summary.qc_eligible:
        raise TradeBarError("previous volume summary must be QC eligible")
    if previous_volume_summary.source_date >= resolved_date:
        raise TradeBarError("previous volume summary must precede source_date")
    previous_fields = {
        "previous_volume_sha256": previous_volume_summary.sha256,
        "previous_source_date": previous_volume_summary.source_date,
    }
    previous_by_month = {item.contract_month: item for item in previous_volume_summary.contracts}
    source_month = date(resolved_date.year, resolved_date.month, 1)
    candidates = [
        (
            instrument_id,
            symbol,
            contract_month,
            previous_by_month.get(contract_month),
        )
        for instrument_id, symbol, contract_month in _active_6e_outrights(
            mapping_values,
            source_date=resolved_date,
        )
        if contract_month > source_month
    ]
    if not candidates:
        return DailyBarPlan(
            status=DailyPlanStatus.NO_ELIGIBLE_CONTRACT,
            **common,  # type: ignore[arg-type]
            **previous_fields,  # type: ignore[arg-type]
            **empty_selection,  # type: ignore[arg-type]
        )
    candidates.sort(
        key=lambda item: (
            -(item[3].volume if item[3] is not None else 0),
            item[2],
            item[0],
            item[1],
        )
    )
    instrument_id, symbol, contract_month, evidence = candidates[0]
    if evidence is None or evidence.trade_count <= 0 or evidence.volume <= 0:
        return DailyBarPlan(
            status=DailyPlanStatus.NO_POSITIVE_PREVIOUS_VOLUME,
            **common,  # type: ignore[arg-type]
            **previous_fields,  # type: ignore[arg-type]
            **empty_selection,  # type: ignore[arg-type]
        )
    return DailyBarPlan(
        status=DailyPlanStatus.SELECTED,
        selected_instrument_id=instrument_id,
        selected_contract=symbol,
        selected_contract_month=contract_month,
        selected_previous_trade_count=evidence.trade_count,
        selected_previous_volume=evidence.volume,
        **common,  # type: ignore[arg-type]
        **previous_fields,  # type: ignore[arg-type]
    )


def _segment_identifier(*, contract: str, first_start_ns: int) -> int:
    """Return a stable positive uint64 compatible with research/replay APIs."""

    digest = hashlib.sha256(
        _canonical_bytes(
            {
                "contract": contract,
                "first_start_ns": first_start_ns,
                "policy": SEGMENT_POLICY_VERSION,
            }
        )
    ).digest()
    # Zero is reserved as an invalid/unset identifier.  A 64-bit digest prefix
    # keeps the Parquet representation compact while remaining deterministic.
    return int.from_bytes(digest[:8], "big") or 1


def _assign_segments(
    bars: Sequence[TradeBar],
    *,
    previous_segment_tail: SegmentTail | None,
    gap_break_seconds: int,
) -> tuple[tuple[TradeBar, ...], SegmentTail | None]:
    if not bars:
        return (), previous_segment_tail
    gap_ns = (
        _require_int(
            gap_break_seconds,
            label="gap_break_seconds",
            minimum=1,
        )
        * ONE_SECOND_NS
    )
    ordered = tuple(sorted(bars, key=lambda item: (item.start_ns, item.first_trade_ns)))
    first = ordered[0]
    if previous_segment_tail is not None and first.start_ns < previous_segment_tail.last_bar_end_ns:
        raise TradeBarError("current bars overlap previous segment tail")
    continue_previous = (
        previous_segment_tail is not None
        and previous_segment_tail.contract == first.contract
        and first.start_ns - previous_segment_tail.last_bar_end_ns < gap_ns
    )
    segment_id = (
        previous_segment_tail.segment_id
        if continue_previous and previous_segment_tail is not None
        else _segment_identifier(contract=first.contract, first_start_ns=first.start_ns)
    )
    assigned: list[TradeBar] = []
    previous_end = (
        previous_segment_tail.last_bar_end_ns
        if continue_previous and previous_segment_tail is not None
        else None
    )
    previous_contract = first.contract
    for bar in ordered:
        if previous_end is not None:
            if bar.start_ns < previous_end:
                raise TradeBarError("trade bars overlap or regress")
            if bar.contract != previous_contract or bar.start_ns - previous_end >= gap_ns:
                segment_id = _segment_identifier(
                    contract=bar.contract,
                    first_start_ns=bar.start_ns,
                )
        assigned.append(replace(bar, segment_id=segment_id))
        previous_end = bar.end_ns
        previous_contract = bar.contract
    last = assigned[-1]
    return tuple(assigned), SegmentTail(
        contract=last.contract,
        segment_id=last.segment_id,
        source_date=last.source_date,
        last_bar_end_ns=last.end_ns,
        last_trade_ns=last.last_trade_ns,
    )


def build_one_second_trade_bars(
    prints: Iterable[TradePrint],
    *,
    contract: str,
    source_date: date,
    previous_segment_tail: SegmentTail | None = None,
    gap_break_seconds: int = DEFAULT_GAP_BREAK_SECONDS,
) -> tuple[TradeBar, ...]:
    """Build deterministic observed one-second OHLCV bars from selected trades."""

    symbol = _nonempty(contract, label="contract")
    resolved_date = _require_date(source_date, label="source_date")
    values = tuple(prints)
    if any(not isinstance(item, TradePrint) for item in values):
        raise TradeBarError("prints must contain only TradePrint values")
    ordered = tuple(sorted(values, key=lambda item: item.ordering_key))
    for left, right in pairwise(ordered):
        if left.ordering_key == right.ordering_key:
            raise TradeBarError("duplicate trade ordering key")
    grouped: dict[int, list[TradePrint]] = defaultdict(list)
    for item in ordered:
        if _source_date_for_ns(item.ts_recv_ns) != resolved_date:
            raise TradeBarError("trade timestamp UTC date differs from source_date")
        start = item.ts_recv_ns // ONE_SECOND_NS * ONE_SECOND_NS
        grouped[start].append(item)
    unsegmented: list[TradeBar] = []
    for start, group in sorted(grouped.items()):
        prices = [item.price_ticks for item in group]
        buy_volume = sum(item.size for item in group if item.side == "B")
        sell_volume = sum(item.size for item in group if item.side == "A")
        unsegmented.append(
            TradeBar(
                timeframe_seconds=1,
                segment_id=1,
                contract=symbol,
                source_date=resolved_date,
                start_ns=start,
                end_ns=start + ONE_SECOND_NS,
                first_trade_ns=group[0].ts_recv_ns,
                last_trade_ns=group[-1].ts_recv_ns,
                open_ticks=prices[0],
                high_ticks=max(prices),
                low_ticks=min(prices),
                close_ticks=prices[-1],
                trade_count=len(group),
                volume=sum(item.size for item in group),
                observed_subbars=1,
                buy_volume=buy_volume,
                sell_volume=sell_volume,
            )
        )
    assigned, _ = _assign_segments(
        unsegmented,
        previous_segment_tail=previous_segment_tail,
        gap_break_seconds=gap_break_seconds,
    )
    return assigned


def segment_tail(bars: Sequence[TradeBar]) -> SegmentTail | None:
    """Return the continuation tail for a chronologically ordered bar span."""

    if not bars:
        return None
    ordered = tuple(sorted(bars, key=lambda item: (item.start_ns, item.segment_id)))
    last = ordered[-1]
    return SegmentTail(
        contract=last.contract,
        segment_id=last.segment_id,
        source_date=last.source_date,
        last_bar_end_ns=last.end_ns,
        last_trade_ns=last.last_trade_ns,
    )


def resample_trade_bars(
    bars: Iterable[TradeBar],
    *,
    timeframe_seconds: int,
) -> tuple[TradeBar, ...]:
    """Associatively resample observed trade bars to a supported wider interval."""

    target = _require_int(timeframe_seconds, label="timeframe_seconds", minimum=1)
    if target not in SUPPORTED_TIMEFRAMES_SECONDS:
        raise TradeBarError("unsupported resample timeframe")
    values = tuple(bars)
    if not values:
        return ()
    if any(not isinstance(item, TradeBar) for item in values):
        raise TradeBarError("bars must contain only TradeBar values")
    input_widths = {item.timeframe_seconds for item in values}
    if len(input_widths) != 1:
        raise TradeBarError("resample input must have one timeframe")
    source_width = next(iter(input_widths))
    if target < source_width or target % source_width:
        raise TradeBarError("target timeframe must be an integer multiple of input")
    if target == source_width:
        return tuple(sorted(values, key=lambda item: (item.start_ns, item.segment_id)))
    target_ns = target * ONE_SECOND_NS
    grouped: dict[tuple[date, str, int, int], list[TradeBar]] = defaultdict(list)
    identities: set[tuple[int, str, int]] = set()
    for bar in values:
        identity = (bar.segment_id, bar.contract, bar.start_ns)
        if identity in identities:
            raise TradeBarError("duplicate input bar identity")
        identities.add(identity)
        start = bar.start_ns // target_ns * target_ns
        if _source_date_for_ns(start) != bar.source_date:
            raise TradeBarError("resample bucket crosses source-date identity")
        grouped[(bar.source_date, bar.contract, bar.segment_id, start)].append(bar)
    result: list[TradeBar] = []
    for (bar_date, contract, segment_id, start), group in sorted(
        grouped.items(),
        key=lambda item: (item[0][3], item[0][2], item[0][1]),
    ):
        ordered = sorted(group, key=lambda item: (item.start_ns, item.first_trade_ns))
        for left, right in pairwise(ordered):
            if right.start_ns < left.end_ns:
                raise TradeBarError("resample input bars overlap")
        buy_volume = (
            sum(item.buy_volume or 0 for item in ordered)
            if all(item.buy_volume is not None for item in ordered)
            else None
        )
        sell_volume = (
            sum(item.sell_volume or 0 for item in ordered)
            if all(item.sell_volume is not None for item in ordered)
            else None
        )
        result.append(
            TradeBar(
                timeframe_seconds=target,
                segment_id=segment_id,
                contract=contract,
                source_date=bar_date,
                start_ns=start,
                end_ns=start + target_ns,
                first_trade_ns=ordered[0].first_trade_ns,
                last_trade_ns=ordered[-1].last_trade_ns,
                open_ticks=ordered[0].open_ticks,
                high_ticks=max(item.high_ticks for item in ordered),
                low_ticks=min(item.low_ticks for item in ordered),
                close_ticks=ordered[-1].close_ticks,
                trade_count=sum(item.trade_count for item in ordered),
                volume=sum(item.volume for item in ordered),
                observed_subbars=sum(item.observed_subbars for item in ordered),
                buy_volume=buy_volume,
                sell_volume=sell_volume,
            )
        )
    return tuple(sorted(result, key=lambda item: (item.start_ns, item.segment_id)))


def link_next_bars(bars: Iterable[TradeBar]) -> tuple[NextBarLink, ...]:
    """Describe exact next-bar availability for the supplied immutable span."""

    values = tuple(bars)
    if not values:
        return ()
    if any(not isinstance(item, TradeBar) for item in values):
        raise TradeBarError("bars must contain only TradeBar values")
    timeframes = {item.timeframe_seconds for item in values}
    if len(timeframes) != 1:
        raise TradeBarError("next-bar linking requires one timeframe")
    ordered = tuple(sorted(values, key=lambda item: (item.start_ns, item.segment_id)))
    links: list[NextBarLink] = []
    for index, current in enumerate(ordered):
        following = ordered[index + 1] if index + 1 < len(ordered) else None
        if following is None:
            status = NextBarLinkStatus.PARTITION_END
            next_start = None
            next_trade = None
        elif following.segment_id != current.segment_id or following.contract != current.contract:
            status = NextBarLinkStatus.SEGMENT_BOUNDARY
            next_start = following.start_ns
            next_trade = following.first_trade_ns
        elif following.start_ns == current.end_ns:
            status = NextBarLinkStatus.EXACT_NEXT_BAR
            next_start = following.start_ns
            next_trade = following.first_trade_ns
        elif following.start_ns > current.end_ns:
            status = NextBarLinkStatus.GAP
            next_start = following.start_ns
            next_trade = following.first_trade_ns
        else:
            raise TradeBarError("same-segment bars overlap during next-bar linking")
        links.append(
            NextBarLink(
                timeframe_seconds=current.timeframe_seconds,
                segment_id=current.segment_id,
                contract=current.contract,
                source_date=current.source_date,
                current_start_ns=current.start_ns,
                current_end_ns=current.end_ns,
                status=status,
                next_bar_start_ns=next_start,
                next_first_trade_ns=next_trade,
            )
        )
    return tuple(links)


_BAR_FIELDS: Final = (
    pa.field("bar_version", pa.string(), nullable=False),
    pa.field("timeframe_seconds", pa.uint32(), nullable=False),
    pa.field("segment_id", pa.uint64(), nullable=False),
    pa.field("contract", pa.string(), nullable=False),
    pa.field("source_date", pa.date32(), nullable=False),
    pa.field("start_ns", pa.int64(), nullable=False),
    pa.field("end_ns", pa.int64(), nullable=False),
    pa.field("first_trade_ns", pa.int64(), nullable=False),
    pa.field("last_trade_ns", pa.int64(), nullable=False),
    pa.field("open_ticks", pa.int64(), nullable=False),
    pa.field("high_ticks", pa.int64(), nullable=False),
    pa.field("low_ticks", pa.int64(), nullable=False),
    pa.field("close_ticks", pa.int64(), nullable=False),
    pa.field("trade_count", pa.uint64(), nullable=False),
    pa.field("volume", pa.uint64(), nullable=False),
    pa.field("buy_volume", pa.uint64(), nullable=True),
    pa.field("sell_volume", pa.uint64(), nullable=True),
    pa.field("observed_subbars", pa.uint32(), nullable=False),
    pa.field("next_link_status", pa.string(), nullable=False),
    pa.field("next_bar_start_ns", pa.int64(), nullable=True),
    pa.field("next_first_trade_ns", pa.int64(), nullable=True),
)

_BAR_STATIC_METADATA: Final = {
    b"systematic_fx.bar_schema": BAR_SCHEMA.encode("ascii"),
    b"systematic_fx.bar_version": BAR_VERSION.encode("ascii"),
    b"systematic_fx.bucket_rule": b"LEFT_CLOSED_RIGHT_OPEN_UTC",
    b"systematic_fx.link_scope": b"PARTITION",
    b"systematic_fx.price_scale": PRICE_SCALE.encode("ascii"),
    b"systematic_fx.tick_size_raw": str(TICK_SIZE_RAW).encode("ascii"),
}
_BAR_DYNAMIC_METADATA_KEYS: Final = frozenset(
    {
        b"systematic_fx.plan_sha256",
        b"systematic_fx.source_date",
        b"systematic_fx.source_sha256",
    }
)


def trade_bars_to_table(
    bars: Iterable[TradeBar],
    *,
    plan_sha256: str,
    source_sha256: str,
) -> pa.Table:
    """Encode bars and partition-scoped next-link metadata without MBP fields."""

    plan_sha = _require_sha256(plan_sha256, label="plan_sha256")
    source_sha = _require_sha256(source_sha256, label="source_sha256")
    values = tuple(sorted(bars, key=lambda item: (item.start_ns, item.segment_id)))
    if not values:
        raise TradeBarError("cannot encode an empty trade-bar table")
    source_dates = {bar.source_date for bar in values}
    if len(source_dates) != 1:
        raise TradeBarError("one trade-bar artifact must contain one source date")
    artifact_source_date = next(iter(source_dates))
    links = link_next_bars(values)
    records: list[dict[str, object]] = []
    for bar, link in zip(values, links, strict=True):
        record = bar.as_dict()
        record["source_date"] = bar.source_date
        record.update(
            {
                "bar_version": BAR_VERSION,
                "next_link_status": link.status.value,
                "next_bar_start_ns": link.next_bar_start_ns,
                "next_first_trade_ns": link.next_first_trade_ns,
            }
        )
        records.append(record)
    metadata = {
        **_BAR_STATIC_METADATA,
        b"systematic_fx.plan_sha256": plan_sha.encode("ascii"),
        b"systematic_fx.source_date": artifact_source_date.isoformat().encode("ascii"),
        b"systematic_fx.source_sha256": source_sha.encode("ascii"),
    }
    schema = pa.schema(_BAR_FIELDS, metadata=metadata)
    try:
        return pa.Table.from_pylist(records, schema=schema)
    except (pa.ArrowException, OverflowError, TypeError, ValueError) as error:
        raise TradeBarError("trade bars violate the frozen Arrow schema") from error


def _resolve_data_root(value: Path | str) -> Path:
    requested = Path(value).expanduser()
    if requested.is_symlink():
        raise TradeBarError("data_root cannot be a symbolic link")
    try:
        resolved = requested.resolve(strict=True)
    except FileNotFoundError as error:
        raise TradeBarError("data_root must already exist") from error
    if not resolved.is_dir():
        raise TradeBarError("data_root must be a directory")
    return resolved


def _trade_bar_artifact_relative_parts(
    artifact: TradeBarArtifactDescriptor,
) -> tuple[Path, str]:
    label = TIMEFRAME_LABELS[artifact.timeframe_seconds]
    parent = Path("derived") / "trade_bars" / f"version={BAR_VERSION}" / f"timeframe={label}"
    leaf = f"sha256={artifact.sha256}.parquet"
    expected = parent / leaf
    if artifact.relative_uri != expected.as_posix():
        raise TradeBarError("trade-bar artifact URI is not canonical for its descriptor")
    return parent, leaf


def _directory_inode_identity(value: os.stat_result) -> tuple[int, int, int]:
    return value.st_dev, value.st_ino, stat.S_IFMT(value.st_mode)


def _file_inode_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


@dataclass(frozen=True, slots=True)
class _HeldTradeBarDirectoryChain:
    root: Path
    relative_parent: Path
    descriptors: tuple[int, ...]
    identities: tuple[tuple[int, int, int], ...]

    @property
    def parent_descriptor(self) -> int:
        return self.descriptors[-1]


@dataclass(frozen=True, slots=True)
class _OpenedTradeBarArtifact:
    descriptor: int
    path: Path
    leaf_name: str
    identity: tuple[int, int, int, int, int, int]
    chain: _HeldTradeBarDirectoryChain


def _verify_trade_bar_directory_chain(chain: _HeldTradeBarDirectoryChain) -> None:
    try:
        root_details = chain.root.lstat()
    except OSError as error:
        raise TradeBarError("data_root path disappeared during verified loading") from error
    if _directory_inode_identity(root_details) != chain.identities[0]:
        raise TradeBarError("data_root path changed during verified loading")
    for index, part in enumerate(chain.relative_parent.parts, start=1):
        held_details = os.fstat(chain.descriptors[index])
        if _directory_inode_identity(held_details) != chain.identities[index]:
            raise TradeBarError("held trade-bar directory changed during verified loading")
        try:
            bound_details = os.stat(
                part,
                dir_fd=chain.descriptors[index - 1],
                follow_symlinks=False,
            )
        except OSError as error:
            raise TradeBarError(
                "trade-bar artifact ancestor changed during verified loading"
            ) from error
        if _directory_inode_identity(bound_details) != chain.identities[index]:
            raise TradeBarError("trade-bar artifact ancestor changed during verified loading")


@contextmanager
def _hold_trade_bar_directory_chain(
    root: Path,
    relative_parent: Path,
    *,
    create: bool = False,
) -> Iterator[_HeldTradeBarDirectoryChain]:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptors: list[int] = []
    identities: list[tuple[int, int, int]] = []
    try:
        try:
            root_descriptor = os.open(root, flags)
        except OSError as error:
            raise TradeBarError("cannot open data_root safely") from error
        descriptors.append(root_descriptor)
        root_details = os.fstat(root_descriptor)
        if not stat.S_ISDIR(root_details.st_mode):
            raise TradeBarError("data_root must be a directory")
        identities.append(_directory_inode_identity(root_details))
        for part in relative_parent.parts:
            try:
                descriptor = os.open(part, flags, dir_fd=descriptors[-1])
            except FileNotFoundError:
                if not create:
                    raise TradeBarError("trade-bar artifact path does not exist") from None
                try:
                    os.mkdir(part, mode=0o755, dir_fd=descriptors[-1])
                except FileExistsError:
                    pass
                try:
                    descriptor = os.open(part, flags, dir_fd=descriptors[-1])
                except OSError as error:
                    raise TradeBarError(
                        "cannot open created trade-bar artifact directory safely"
                    ) from error
            except OSError as error:
                raise TradeBarError(
                    "trade-bar artifact path cannot contain a symbolic link or unsafe directory"
                ) from error
            details = os.fstat(descriptor)
            if not stat.S_ISDIR(details.st_mode):
                os.close(descriptor)
                raise TradeBarError("trade-bar artifact parent must be a directory")
            descriptors.append(descriptor)
            identities.append(_directory_inode_identity(details))
        chain = _HeldTradeBarDirectoryChain(
            root=root,
            relative_parent=relative_parent,
            descriptors=tuple(descriptors),
            identities=tuple(identities),
        )
        _verify_trade_bar_directory_chain(chain)
        yield chain
        _verify_trade_bar_directory_chain(chain)
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _verify_opened_trade_bar_artifact(opened: _OpenedTradeBarArtifact) -> None:
    _verify_trade_bar_directory_chain(opened.chain)
    current = os.fstat(opened.descriptor)
    if _file_inode_identity(current) != opened.identity:
        raise TradeBarError("trade-bar artifact inode changed during verified loading")
    try:
        parent_entry = os.stat(
            opened.leaf_name,
            dir_fd=opened.chain.parent_descriptor,
            follow_symlinks=False,
        )
        lexical_entry = opened.path.lstat()
    except OSError as error:
        raise TradeBarError("trade-bar artifact leaf changed during verified loading") from error
    if (
        _file_inode_identity(parent_entry) != opened.identity
        or _file_inode_identity(lexical_entry) != opened.identity
    ):
        raise TradeBarError("trade-bar artifact leaf changed during verified loading")


@contextmanager
def _open_verified_trade_bar_artifact(
    root: Path,
    artifact: TradeBarArtifactDescriptor,
) -> Iterator[_OpenedTradeBarArtifact]:
    relative_parent, leaf_name = _trade_bar_artifact_relative_parts(artifact)
    path = root / relative_parent / leaf_name
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    with _hold_trade_bar_directory_chain(root, relative_parent) as chain:
        try:
            descriptor = os.open(leaf_name, flags, dir_fd=chain.parent_descriptor)
        except OSError as error:
            raise TradeBarError(
                "trade-bar artifact leaf cannot be a symbolic link or unsafe file"
            ) from error
        try:
            details = os.fstat(descriptor)
            if not stat.S_ISREG(details.st_mode):
                raise TradeBarError("trade-bar artifact must be a regular file")
            opened = _OpenedTradeBarArtifact(
                descriptor=descriptor,
                path=path,
                leaf_name=leaf_name,
                identity=_file_inode_identity(details),
                chain=chain,
            )
            _verify_opened_trade_bar_artifact(opened)
            try:
                yield opened
            finally:
                _verify_opened_trade_bar_artifact(opened)
        finally:
            os.close(descriptor)


def _sha256_descriptor(descriptor: int) -> str:
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    while chunk := os.read(descriptor, 1024 * 1024):
        digest.update(chunk)
    os.lseek(descriptor, 0, os.SEEK_SET)
    return digest.hexdigest()


def load_trade_bar_artifact(
    data_root: Path | str,
    artifact: TradeBarArtifactDescriptor,
    *,
    expected_plan_sha256: str | None = None,
    expected_source_sha256: str | None = None,
    expected_source_date: date | None = None,
) -> tuple[TradeBar, ...]:
    """Load one descriptor-verified immutable trade-bar artifact.

    The optional expected identities let a discovery manifest bind the file
    back to its daily plan and source.  Even when omitted, both SHA values must
    exist in the exact frozen Arrow metadata and be canonical lowercase hashes.
    Every directory inode is held from ``data_root`` through the timeframe
    parent; the content hash and complete Parquet read use one opened leaf file
    descriptor, which is rebound to both parent entry and lexical path before
    returning.
    """

    if not isinstance(artifact, TradeBarArtifactDescriptor):
        raise TradeBarError("artifact must be a TradeBarArtifactDescriptor")
    root = _resolve_data_root(data_root)
    with _open_verified_trade_bar_artifact(root, artifact) as opened:
        return _load_opened_trade_bar_artifact(
            opened,
            artifact,
            expected_plan_sha256=expected_plan_sha256,
            expected_source_sha256=expected_source_sha256,
            expected_source_date=expected_source_date,
        )


def _load_opened_trade_bar_artifact(
    opened: _OpenedTradeBarArtifact,
    artifact: TradeBarArtifactDescriptor,
    *,
    expected_plan_sha256: str | None,
    expected_source_sha256: str | None,
    expected_source_date: date | None,
) -> tuple[TradeBar, ...]:
    if opened.identity[3] != artifact.byte_size:
        raise TradeBarError("trade-bar artifact byte size differs from descriptor")
    if _sha256_descriptor(opened.descriptor) != artifact.sha256:
        raise TradeBarError("trade-bar artifact SHA-256 differs from descriptor")
    source = os.fdopen(opened.descriptor, "rb", closefd=False)
    try:
        try:
            source.seek(0)
            parquet = pq.ParquetFile(source)
        except (OSError, pa.ArrowException) as error:
            raise TradeBarError("cannot open trade-bar artifact") from error
        schema = parquet.schema_arrow
        observed_row_count = parquet.metadata.num_rows
        try:
            table = parquet.read(use_threads=True)
        except (OSError, pa.ArrowException) as error:
            raise TradeBarError("cannot read trade-bar artifact") from error
    finally:
        source.close()
    if observed_row_count != artifact.row_count:
        raise TradeBarError("trade-bar artifact row count differs from descriptor")
    if schema.remove_metadata() != pa.schema(_BAR_FIELDS):
        raise TradeBarError("trade-bar artifact has an incompatible Arrow schema")
    metadata = schema.metadata or {}
    expected_metadata_keys = set(_BAR_STATIC_METADATA) | set(_BAR_DYNAMIC_METADATA_KEYS)
    if set(metadata) != expected_metadata_keys:
        raise TradeBarError("trade-bar artifact metadata keys differ from the frozen schema")
    if any(metadata.get(key) != value for key, value in _BAR_STATIC_METADATA.items()):
        raise TradeBarError("trade-bar artifact static metadata differs from the frozen schema")
    try:
        plan_sha = _require_sha256(
            metadata[b"systematic_fx.plan_sha256"].decode("ascii"),
            label="artifact plan_sha256",
        )
        source_sha = _require_sha256(
            metadata[b"systematic_fx.source_sha256"].decode("ascii"),
            label="artifact source_sha256",
        )
        source_date_text = metadata[b"systematic_fx.source_date"].decode("ascii")
        metadata_source_date = date.fromisoformat(source_date_text)
        if metadata_source_date.isoformat() != source_date_text:
            raise ValueError
    except (UnicodeDecodeError, KeyError, ValueError) as error:
        raise TradeBarError("trade-bar artifact dynamic metadata is invalid") from error
    if expected_plan_sha256 is not None and plan_sha != _require_sha256(
        expected_plan_sha256,
        label="expected_plan_sha256",
    ):
        raise TradeBarError("trade-bar artifact plan identity differs from expectation")
    if expected_source_sha256 is not None and source_sha != _require_sha256(
        expected_source_sha256,
        label="expected_source_sha256",
    ):
        raise TradeBarError("trade-bar artifact source identity differs from expectation")
    resolved_source_date = (
        _require_date(expected_source_date, label="expected_source_date")
        if expected_source_date is not None
        else None
    )
    if resolved_source_date is not None and metadata_source_date != resolved_source_date:
        raise TradeBarError("trade-bar artifact source date differs from expectation")
    if table.schema != schema or table.num_rows != artifact.row_count:
        raise TradeBarError("trade-bar artifact changed while being read")
    records = table.to_pylist()
    bars: list[TradeBar] = []
    link_records: list[tuple[object, object, object]] = []
    for index, record in enumerate(records):
        if record["bar_version"] != BAR_VERSION:
            raise TradeBarError(f"trade-bar artifact row {index} has a wrong version")
        if record["timeframe_seconds"] != artifact.timeframe_seconds:
            raise TradeBarError(f"trade-bar artifact row {index} has a wrong timeframe")
        bar = TradeBar(
            timeframe_seconds=record["timeframe_seconds"],
            segment_id=record["segment_id"],
            contract=record["contract"],
            source_date=record["source_date"],
            start_ns=record["start_ns"],
            end_ns=record["end_ns"],
            first_trade_ns=record["first_trade_ns"],
            last_trade_ns=record["last_trade_ns"],
            open_ticks=record["open_ticks"],
            high_ticks=record["high_ticks"],
            low_ticks=record["low_ticks"],
            close_ticks=record["close_ticks"],
            trade_count=record["trade_count"],
            volume=record["volume"],
            observed_subbars=record["observed_subbars"],
            buy_volume=record["buy_volume"],
            sell_volume=record["sell_volume"],
        )
        if bar.source_date != metadata_source_date:
            raise TradeBarError("trade-bar row source date differs from artifact metadata")
        bars.append(bar)
        link_records.append(
            (
                record["next_link_status"],
                record["next_bar_start_ns"],
                record["next_first_trade_ns"],
            )
        )
    if len({bar.source_date for bar in bars}) != 1:
        raise TradeBarError("daily trade-bar artifact must contain one source date")
    if len({bar.contract for bar in bars}) != 1:
        raise TradeBarError("daily trade-bar artifact must contain one contract")
    identities = [(bar.start_ns, bar.segment_id) for bar in bars]
    if identities != sorted(identities) or len(identities) != len(set(identities)):
        raise TradeBarError("trade-bar artifact rows are not canonically ordered and unique")
    calculated_links = link_next_bars(bars)
    for index, (stored, calculated) in enumerate(zip(link_records, calculated_links, strict=True)):
        expected_link = (
            calculated.status.value,
            calculated.next_bar_start_ns,
            calculated.next_first_trade_ns,
        )
        if stored != expected_link:
            raise TradeBarError(f"trade-bar artifact row {index} has invalid next-link metadata")
    return tuple(bars)


def _open_trade_bar_temporary(parent_descriptor: int) -> tuple[int, str]:
    flags = (
        os.O_RDWR
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    for _ in range(128):
        name = f".trade-bars-{secrets.token_hex(16)}.parquet.tmp"
        try:
            return os.open(name, flags, 0o600, dir_fd=parent_descriptor), name
        except FileExistsError:
            continue
        except OSError as error:
            raise TradeBarError("cannot create a safe staged trade-bar artifact") from error
    raise TradeBarError("cannot allocate a unique staged trade-bar artifact")


def _verify_staged_trade_bar(
    chain: _HeldTradeBarDirectoryChain,
    *,
    descriptor: int,
    name: str,
    identity: tuple[int, int, int, int, int, int],
) -> None:
    _verify_trade_bar_directory_chain(chain)
    if _file_inode_identity(os.fstat(descriptor)) != identity:
        raise TradeBarError("staged trade-bar inode changed during publication")
    try:
        bound = os.stat(name, dir_fd=chain.parent_descriptor, follow_symlinks=False)
    except OSError as error:
        raise TradeBarError("staged trade-bar path changed during publication") from error
    if _file_inode_identity(bound) != identity:
        raise TradeBarError("staged trade-bar path changed during publication")


def _verify_named_trade_bar_target(
    chain: _HeldTradeBarDirectoryChain,
    *,
    name: str,
    expected_sha256: str,
    expected_size: int,
) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(name, flags, dir_fd=chain.parent_descriptor)
    except OSError as error:
        raise TradeBarError("cannot open published trade-bar target safely") from error
    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode) or details.st_mode & 0o222:
            raise TradeBarError("published trade-bar target must be a read-only regular file")
        identity = _file_inode_identity(details)
        observed_sha256 = _sha256_descriptor(descriptor)
        if observed_sha256 != expected_sha256 or details.st_size != expected_size:
            raise TradeBarError("content-addressed target differs from staged bytes")
        if _file_inode_identity(os.fstat(descriptor)) != identity:
            raise TradeBarError("published trade-bar inode changed during verification")
        bound = os.stat(name, dir_fd=chain.parent_descriptor, follow_symlinks=False)
        lexical = (chain.root / chain.relative_parent / name).lstat()
        if _file_inode_identity(bound) != identity or _file_inode_identity(lexical) != identity:
            raise TradeBarError("published trade-bar target changed during verification")
        _verify_trade_bar_directory_chain(chain)
    except OSError as error:
        raise TradeBarError("published trade-bar target changed during verification") from error
    finally:
        os.close(descriptor)


def _publish_table(
    table: pa.Table,
    *,
    data_root: Path,
    timeframe_seconds: int,
) -> BarArtifact:
    label = TIMEFRAME_LABELS[timeframe_seconds]
    relative_parent = (
        Path("derived") / "trade_bars" / f"version={BAR_VERSION}" / f"timeframe={label}"
    )
    with _hold_trade_bar_directory_chain(
        data_root,
        relative_parent,
        create=True,
    ) as chain:
        descriptor, temporary_name = _open_trade_bar_temporary(chain.parent_descriptor)
        staged = None
        try:
            staged = os.fdopen(descriptor, "w+b", closefd=False)
            pq.write_table(
                table,
                staged,
                compression="zstd",
                use_dictionary=True,
                write_statistics=True,
                version="2.6",
                row_group_size=65_536,
            )
            staged.flush()
            os.fsync(descriptor)
            staged.seek(0)
            check = pq.ParquetFile(staged)
            if check.metadata.num_rows != table.num_rows or check.schema_arrow != table.schema:
                raise TradeBarError("staged trade-bar Parquet failed schema/row validation")
            digest = _sha256_descriptor(descriptor)
            byte_size = os.fstat(descriptor).st_size
            os.fchmod(descriptor, 0o444)
            staged_identity = _file_inode_identity(os.fstat(descriptor))
            _verify_staged_trade_bar(
                chain,
                descriptor=descriptor,
                name=temporary_name,
                identity=staged_identity,
            )
            target_name = f"sha256={digest}.parquet"
            disposition = "CREATED"
            try:
                os.link(
                    temporary_name,
                    target_name,
                    src_dir_fd=chain.parent_descriptor,
                    dst_dir_fd=chain.parent_descriptor,
                    follow_symlinks=False,
                )
            except FileExistsError:
                disposition = "REUSED"
            _verify_named_trade_bar_target(
                chain,
                name=target_name,
                expected_sha256=digest,
                expected_size=byte_size,
            )
            os.fsync(chain.parent_descriptor)
            target = data_root / relative_parent / target_name
            return BarArtifact(
                timeframe_seconds=timeframe_seconds,
                relative_uri=target.relative_to(data_root).as_posix(),
                sha256=digest,
                byte_size=byte_size,
                row_count=table.num_rows,
                disposition=disposition,
            )
        finally:
            if staged is not None:
                staged.close()
            os.close(descriptor)
            try:
                os.unlink(temporary_name, dir_fd=chain.parent_descriptor)
            except FileNotFoundError:
                pass
            os.fsync(chain.parent_descriptor)


_RAW_COLUMNS: Final = {
    "ts_recv": pa.timestamp("ns", tz="UTC"),
    "instrument_id": pa.uint32(),
    "action": pa.string(),
    "side": pa.string(),
    "price": pa.int64(),
    "size": pa.uint32(),
    "flags": pa.uint8(),
    "sequence": pa.uint32(),
}


@dataclass(frozen=True, slots=True)
class _HeldDailySourceDirectoryChain:
    root: Path
    relative_parent: Path
    descriptors: tuple[int, ...]
    identities: tuple[tuple[int, int, int], ...]

    @property
    def parent_descriptor(self) -> int:
        return self.descriptors[-1]


@dataclass(frozen=True, slots=True)
class _OpenedDailySource:
    descriptor: int
    path: Path
    leaf_name: str
    identity: tuple[int, int, int, int, int, int]
    chain: _HeldDailySourceDirectoryChain
    parquet: pq.ParquetFile
    mappings: tuple[InstrumentMapping, ...]


def _daily_source_relative_parts(source_date: date) -> tuple[Path, str]:
    parent = (
        Path("mbp-10")
        / f"{source_date.year:04d}"
        / f"{source_date.month:02d}"
        / f"{source_date.day:02d}"
    )
    leaf = f"glbx-mdp3-{source_date:%Y%m%d}.mbp-10.parquet"
    return parent, leaf


def _verify_daily_source_directory_chain(chain: _HeldDailySourceDirectoryChain) -> None:
    try:
        root_details = chain.root.lstat()
    except OSError as error:
        raise TradeBarError("data_root path disappeared during source projection") from error
    if _directory_inode_identity(root_details) != chain.identities[0]:
        raise TradeBarError("data_root path changed during source projection")
    for index, part in enumerate(chain.relative_parent.parts, start=1):
        held_details = os.fstat(chain.descriptors[index])
        if _directory_inode_identity(held_details) != chain.identities[index]:
            raise TradeBarError("held source directory changed during source projection")
        try:
            bound_details = os.stat(
                part,
                dir_fd=chain.descriptors[index - 1],
                follow_symlinks=False,
            )
        except OSError as error:
            raise TradeBarError("source ancestor changed during source projection") from error
        if _directory_inode_identity(bound_details) != chain.identities[index]:
            raise TradeBarError("source ancestor changed during source projection")


@contextmanager
def _hold_daily_source_directory_chain(
    root: Path,
    relative_parent: Path,
) -> Iterator[_HeldDailySourceDirectoryChain]:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptors: list[int] = []
    identities: list[tuple[int, int, int]] = []
    try:
        try:
            root_descriptor = os.open(root, flags)
        except OSError as error:
            raise TradeBarError("cannot open data_root safely for source projection") from error
        descriptors.append(root_descriptor)
        root_details = os.fstat(root_descriptor)
        if not stat.S_ISDIR(root_details.st_mode):
            raise TradeBarError("data_root must be a directory")
        identities.append(_directory_inode_identity(root_details))
        for part in relative_parent.parts:
            try:
                descriptor = os.open(part, flags, dir_fd=descriptors[-1])
            except FileNotFoundError as error:
                raise TradeBarError("canonical source directory does not exist") from error
            except OSError as error:
                raise TradeBarError(
                    "source path cannot contain a symbolic link or unsafe directory"
                ) from error
            details = os.fstat(descriptor)
            if not stat.S_ISDIR(details.st_mode):
                os.close(descriptor)
                raise TradeBarError("source parent must be a directory")
            descriptors.append(descriptor)
            identities.append(_directory_inode_identity(details))
        chain = _HeldDailySourceDirectoryChain(
            root=root,
            relative_parent=relative_parent,
            descriptors=tuple(descriptors),
            identities=tuple(identities),
        )
        _verify_daily_source_directory_chain(chain)
        yield chain
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _verify_opened_daily_source(opened: _OpenedDailySource) -> None:
    _verify_daily_source_directory_chain(opened.chain)
    current = os.fstat(opened.descriptor)
    if _file_inode_identity(current) != opened.identity:
        raise TradeBarError("source Parquet inode changed during source projection")
    try:
        parent_entry = os.stat(
            opened.leaf_name,
            dir_fd=opened.chain.parent_descriptor,
            follow_symlinks=False,
        )
        lexical_entry = opened.path.lstat()
    except OSError as error:
        raise TradeBarError("source Parquet leaf changed during source projection") from error
    if (
        _file_inode_identity(parent_entry) != opened.identity
        or _file_inode_identity(lexical_entry) != opened.identity
    ):
        raise TradeBarError("source Parquet leaf changed during source projection")


@contextmanager
def _open_verified_daily_source(
    root: Path,
    path: Path | str,
    *,
    source_date: date,
    expected_sha256: str,
) -> Iterator[_OpenedDailySource]:
    relative_parent, leaf_name = _daily_source_relative_parts(source_date)
    expected_path = root / relative_parent / leaf_name
    requested = Path(os.path.abspath(Path(path).expanduser()))
    if requested != expected_path:
        raise TradeBarError("source Parquet path is not canonical for source_date and data_root")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    with _hold_daily_source_directory_chain(root, relative_parent) as chain:
        try:
            descriptor = os.open(leaf_name, flags, dir_fd=chain.parent_descriptor)
        except OSError as error:
            raise TradeBarError(
                "source Parquet leaf cannot be a symbolic link or unsafe file"
            ) from error
        source = None
        try:
            details = os.fstat(descriptor)
            if not stat.S_ISREG(details.st_mode):
                raise TradeBarError("source Parquet must be a regular file")
            if _sha256_descriptor(descriptor) != expected_sha256:
                raise TradeBarError("source Parquet SHA-256 differs from the frozen manifest")
            source = os.fdopen(descriptor, "rb", closefd=False)
            try:
                source.seek(0)
                parquet = pq.ParquetFile(source)
            except (OSError, pa.ArrowException) as error:
                raise TradeBarError("cannot open source Parquet") from error
            schema = parquet.schema_arrow
            for name, expected_type in _RAW_COLUMNS.items():
                index = schema.get_field_index(name)
                if index < 0:
                    raise TradeBarError(f"source Parquet is missing required column {name!r}")
                field = schema.field(index)
                if field.type != expected_type or field.nullable:
                    raise TradeBarError(f"source column {name!r} has incompatible type/nullability")
            raw_metadata = (schema.metadata or {}).get(b"dbn.metadata")
            if raw_metadata is None:
                raise TradeBarError("source Parquet lacks dbn.metadata")
            metadata = decode_dbn_metadata(raw_metadata)
            start = metadata.get("start")
            if isinstance(start, bool) or not isinstance(start, int) or start < 0:
                raise TradeBarError("dbn.metadata start is invalid")
            if _source_date_for_ns(start) != source_date:
                raise TradeBarError("source footer date differs from requested source_date")
            opened = _OpenedDailySource(
                descriptor=descriptor,
                path=expected_path,
                leaf_name=leaf_name,
                identity=_file_inode_identity(details),
                chain=chain,
                parquet=parquet,
                mappings=parse_instrument_mappings(raw_metadata),
            )
            _verify_opened_daily_source(opened)
            try:
                yield opened
            finally:
                _verify_opened_daily_source(opened)
        finally:
            if source is not None:
                source.close()
            os.close(descriptor)


@dataclass(frozen=True, slots=True)
class _TradeScan:
    source_rows: int
    source_trades: int
    selected_trades: tuple[TradePrint, ...]
    bad_ts_recv_selected_trades: int
    totals_by_instrument: Mapping[int, tuple[int, int]]


def _scan_daily_trades(
    parquet: pq.ParquetFile,
    *,
    selected_instrument_id: int | None,
) -> _TradeScan:
    totals: dict[int, tuple[int, int]] = {}
    selected: list[TradePrint] = []
    source_rows = 0
    source_trades = 0
    bad_selected = 0
    columns = tuple(_RAW_COLUMNS)
    try:
        for row_group_index in range(parquet.metadata.num_row_groups):
            table = parquet.read_row_group(
                row_group_index,
                columns=columns,
                use_threads=True,
            )
            actions = table["action"]
            indices = pc.indices_nonzero(pc.equal(actions, "T"))
            trades = table.take(indices)
            local_offsets = indices.to_pylist()
            source_trades += trades.num_rows
            instrument_ids = trades["instrument_id"].to_pylist()
            sizes = trades["size"].to_pylist()
            for instrument_id, size in zip(instrument_ids, sizes, strict=True):
                if instrument_id is None or size is None:
                    raise TradeBarError("trade summary columns cannot contain nulls")
                prior_count, prior_volume = totals.get(int(instrument_id), (0, 0))
                totals[int(instrument_id)] = (prior_count + 1, prior_volume + int(size))
            if selected_instrument_id is not None and trades.num_rows:
                timestamps = pc.cast(trades["ts_recv"], pa.int64()).to_pylist()
                sides = trades["side"].to_pylist()
                prices = trades["price"].to_pylist()
                flags = trades["flags"].to_pylist()
                sequences = trades["sequence"].to_pylist()
                for index, instrument_id in enumerate(instrument_ids):
                    if int(instrument_id) != selected_instrument_id:
                        continue
                    values = (
                        timestamps[index],
                        sequences[index],
                        local_offsets[index],
                        prices[index],
                        sizes[index],
                        sides[index],
                        flags[index],
                    )
                    if any(value is None for value in values):
                        raise TradeBarError("selected trade input cannot contain nulls")
                    if int(flags[index]) & F_BAD_TS_RECV:
                        bad_selected += 1
                        continue
                    selected.append(
                        TradePrint(
                            ts_recv_ns=int(timestamps[index]),
                            sequence=int(sequences[index]),
                            physical_ordinal=source_rows + int(local_offsets[index]),
                            price_raw=int(prices[index]),
                            size=int(sizes[index]),
                            side=str(sides[index]),
                        )
                    )
            source_rows += table.num_rows
    except (OSError, pa.ArrowException) as error:
        raise TradeBarError("cannot scan daily trade projection") from error
    if source_rows != parquet.metadata.num_rows:
        raise TradeBarError("trade scan row count differs from Parquet footer")
    return _TradeScan(
        source_rows=source_rows,
        source_trades=source_trades,
        selected_trades=tuple(selected),
        bad_ts_recv_selected_trades=bad_selected,
        totals_by_instrument=dict(sorted(totals.items())),
    )


def build_daily_trade_bar_artifacts(
    source_parquet_path: Path | str,
    *,
    data_root: Path | str,
    source_date: date,
    verified_source_sha256: str,
    previous_volume_summary: DailyVolumeSummary | None,
    previous_segment_tail: SegmentTail | None = None,
    gap_break_seconds: int = DEFAULT_GAP_BREAK_SECONDS,
) -> DailyBarBuildReport:
    """Project one daily source once and atomically publish all bar timeframes.

    ``verified_source_sha256`` must come from the already-qualified source
    manifest.  The raw leaf is opened beneath a held canonical
    ``data_root/mbp-10/YYYY/MM/DD`` directory chain without following symlinks.
    Its complete bytes are hashed and the same open descriptor is then used for
    the Parquet footer and every row-group scan.  All path components and the
    leaf inode are rebound before returning.
    """

    root = _resolve_data_root(data_root)
    resolved_date = _require_date(source_date, label="source_date")
    source_sha = _require_sha256(verified_source_sha256, label="verified_source_sha256")
    with _open_verified_daily_source(
        root,
        source_parquet_path,
        source_date=resolved_date,
        expected_sha256=source_sha,
    ) as source:
        mappings = source.mappings
        plan = plan_daily_trade_bars(
            source_date=resolved_date,
            source_sha256=source_sha,
            mappings=mappings,
            previous_volume_summary=previous_volume_summary,
            previous_segment_tail=previous_segment_tail,
            gap_break_seconds=gap_break_seconds,
        )
        if plan.status is DailyPlanStatus.QC_EXCLUDED:
            return DailyBarBuildReport(
                plan=plan,
                source_scanned=False,
                source_row_count=source.parquet.metadata.num_rows,
                source_trade_count=0,
                selected_trade_count=0,
                bad_ts_recv_trades_excluded=0,
                current_volume_summary=None,
                segment_tail=previous_segment_tail,
                artifacts=(),
                link_sha256_by_timeframe=(),
            )
        scan = _scan_daily_trades(
            source.parquet,
            selected_instrument_id=plan.selected_instrument_id,
        )
    volume_summary = make_daily_volume_summary(
        source_date=resolved_date,
        source_sha256=source_sha,
        mappings=mappings,
        totals_by_instrument=scan.totals_by_instrument,
        qc_eligible=True,
    )
    if plan.status is not DailyPlanStatus.SELECTED or not scan.selected_trades:
        return DailyBarBuildReport(
            plan=plan,
            source_scanned=True,
            source_row_count=scan.source_rows,
            source_trade_count=scan.source_trades,
            selected_trade_count=len(scan.selected_trades),
            bad_ts_recv_trades_excluded=scan.bad_ts_recv_selected_trades,
            current_volume_summary=volume_summary,
            segment_tail=previous_segment_tail,
            artifacts=(),
            link_sha256_by_timeframe=(),
        )
    assert plan.selected_contract is not None
    one_second = build_one_second_trade_bars(
        scan.selected_trades,
        contract=plan.selected_contract,
        source_date=resolved_date,
        previous_segment_tail=previous_segment_tail,
        gap_break_seconds=gap_break_seconds,
    )
    bars_by_timeframe: list[tuple[int, tuple[TradeBar, ...]]] = [(1, one_second)]
    for timeframe in SUPPORTED_TIMEFRAMES_SECONDS[1:]:
        bars_by_timeframe.append(
            (timeframe, resample_trade_bars(one_second, timeframe_seconds=timeframe))
        )
    artifacts: list[BarArtifact] = []
    link_hashes: list[tuple[int, str]] = []
    for timeframe, bars in bars_by_timeframe:
        if not bars:
            continue
        links = link_next_bars(bars)
        link_hashes.append((timeframe, canonical_sha256([link.as_dict() for link in links])))
        table = trade_bars_to_table(
            bars,
            plan_sha256=plan.sha256,
            source_sha256=source_sha,
        )
        artifacts.append(
            _publish_table(
                table,
                data_root=root,
                timeframe_seconds=timeframe,
            )
        )
    tail = segment_tail(one_second)
    return DailyBarBuildReport(
        plan=plan,
        source_scanned=True,
        source_row_count=scan.source_rows,
        source_trade_count=scan.source_trades,
        selected_trade_count=len(scan.selected_trades),
        bad_ts_recv_trades_excluded=scan.bad_ts_recv_selected_trades,
        current_volume_summary=volume_summary,
        segment_tail=tail,
        artifacts=tuple(artifacts),
        link_sha256_by_timeframe=tuple(link_hashes),
    )


__all__ = [
    "BAR_SCHEMA",
    "BAR_VERSION",
    "DEFAULT_GAP_BREAK_SECONDS",
    "QC_EXCLUDED_SOURCE_DATES",
    "SUPPORTED_TIMEFRAMES_SECONDS",
    "TICK_SIZE_RAW",
    "BarArtifact",
    "ContractVolume",
    "DailyBarBuildReport",
    "DailyBarPlan",
    "DailyPlanStatus",
    "DailyVolumeSummary",
    "NextBarLink",
    "NextBarLinkStatus",
    "SegmentTail",
    "TradeBar",
    "TradeBarArtifactDescriptor",
    "TradeBarError",
    "TradePrint",
    "build_daily_trade_bar_artifacts",
    "build_one_second_trade_bars",
    "canonical_sha256",
    "link_next_bars",
    "load_trade_bar_artifact",
    "make_daily_volume_summary",
    "plan_daily_trade_bars",
    "resample_trade_bars",
    "segment_tail",
    "trade_bars_to_table",
]
