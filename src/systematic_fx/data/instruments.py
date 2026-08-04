"""Instrument mappings embedded in DBN Parquet schema metadata."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any

from systematic_fx.data.contracts import Mbp10ContractError, decode_dbn_metadata

_FUTURES_CONTRACT = re.compile(
    r"^(?P<root>[A-Z0-9]+)(?P<month>[FGHJKMNQUVXZ])(?P<year>[0-9]{1,2})$"
)
_UINT32_MAX = 2**32 - 1


class InstrumentKind(str, Enum):
    OUTRIGHT = "outright"
    CALENDAR_SPREAD = "calendar_spread"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class InstrumentMapping:
    """One DBN instrument-id mapping over an end-exclusive date interval."""

    instrument_id: int
    raw_symbol: str
    kind: InstrumentKind
    interval_start: date
    interval_end: date

    def as_dict(self) -> dict[str, object]:
        return {
            "instrument_id": self.instrument_id,
            "interval_end": self.interval_end.isoformat(),
            "interval_start": self.interval_start.isoformat(),
            "kind": self.kind.value,
            "raw_symbol": self.raw_symbol,
        }


def classify_raw_symbol(raw_symbol: str) -> InstrumentKind:
    """Classify an outright future, same-root calendar spread, or unknown symbol."""

    if not isinstance(raw_symbol, str) or not raw_symbol:
        return InstrumentKind.UNKNOWN
    if _FUTURES_CONTRACT.fullmatch(raw_symbol):
        return InstrumentKind.OUTRIGHT

    legs = raw_symbol.split("-")
    if len(legs) != 2:
        return InstrumentKind.UNKNOWN
    first = _FUTURES_CONTRACT.fullmatch(legs[0])
    second = _FUTURES_CONTRACT.fullmatch(legs[1])
    if first is not None and second is not None and first.group("root") == second.group("root"):
        return InstrumentKind.CALENDAR_SPREAD
    return InstrumentKind.UNKNOWN


def _parse_date(value: object, *, location: str) -> date:
    if not isinstance(value, str):
        raise Mbp10ContractError(f"{location} must be an ISO date string")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise Mbp10ContractError(f"{location} is not a valid ISO date: {value!r}") from exc


def _parse_instrument_id(value: object, *, location: str) -> int:
    if isinstance(value, bool):
        raise Mbp10ContractError(f"{location} must be a uint32 instrument id")
    if isinstance(value, int):
        instrument_id = value
    elif isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
        instrument_id = int(value)
    else:
        raise Mbp10ContractError(f"{location} must be a uint32 instrument id")
    if not 0 <= instrument_id <= _UINT32_MAX:
        raise Mbp10ContractError(f"{location} is outside the uint32 range")
    return instrument_id


def parse_instrument_mappings(
    payload: bytes | str | Mapping[str, Any],
) -> tuple[InstrumentMapping, ...]:
    """Flatten and validate DBN ``mappings`` while retaining interval dates."""

    metadata = decode_dbn_metadata(payload)
    raw_mappings = metadata.get("mappings")
    if not isinstance(raw_mappings, list):
        raise Mbp10ContractError("dbn.metadata mappings must be a list")

    parsed: list[InstrumentMapping] = []
    for mapping_index, raw_mapping in enumerate(raw_mappings):
        mapping_location = f"dbn.metadata mappings[{mapping_index}]"
        if not isinstance(raw_mapping, dict):
            raise Mbp10ContractError(f"{mapping_location} must be an object")
        raw_symbol = raw_mapping.get("raw_symbol")
        if not isinstance(raw_symbol, str) or not raw_symbol:
            raise Mbp10ContractError(f"{mapping_location}.raw_symbol must be a non-empty string")
        intervals = raw_mapping.get("intervals")
        if not isinstance(intervals, list):
            raise Mbp10ContractError(f"{mapping_location}.intervals must be a list")

        kind = classify_raw_symbol(raw_symbol)
        for interval_index, raw_interval in enumerate(intervals):
            interval_location = f"{mapping_location}.intervals[{interval_index}]"
            if not isinstance(raw_interval, dict):
                raise Mbp10ContractError(f"{interval_location} must be an object")
            interval_start = _parse_date(
                raw_interval.get("start"), location=f"{interval_location}.start"
            )
            interval_end = _parse_date(raw_interval.get("end"), location=f"{interval_location}.end")
            if interval_end <= interval_start:
                raise Mbp10ContractError(f"{interval_location}.end must be after its start date")
            instrument_id = _parse_instrument_id(
                raw_interval.get("symbol"), location=f"{interval_location}.symbol"
            )
            parsed.append(
                InstrumentMapping(
                    instrument_id=instrument_id,
                    raw_symbol=raw_symbol,
                    kind=kind,
                    interval_start=interval_start,
                    interval_end=interval_end,
                )
            )

    return tuple(
        sorted(
            parsed,
            key=lambda item: (
                item.interval_start,
                item.interval_end,
                item.raw_symbol,
                item.instrument_id,
                item.kind.value,
            ),
        )
    )
