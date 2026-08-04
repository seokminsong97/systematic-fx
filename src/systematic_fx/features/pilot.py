"""Non-research MBP-10 pilot feature builder.

This module deliberately implements only the small, frozen subset documented
by ``configs/features/mbp10_pilot_v1.toml``.  It does not select contracts,
infer trading status, forward-fill unobserved seconds, create labels, or claim
to implement the broader ``mbp10_v1`` design.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Final

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from systematic_fx.data.contracts import UNDEFINED_PRICE, validate_mbp10_contract
from systematic_fx.data.instruments import (
    InstrumentKind,
    classify_raw_symbol,
    parse_instrument_mappings,
)

FEATURE_VERSION: Final = "mbp10_pilot_v1"
RESEARCH_ELIGIBLE: Final = False
PRICE_SCALE: Final = "1e-9"
ONE_SECOND_NS: Final = 1_000_000_000
FIVE_MINUTE_NS: Final = 300 * ONE_SECOND_NS

# DBN flags used by the row-local validity formula.  The pilot intentionally
# does not invent an invalidate-until-rebuilt state machine: a later complete,
# unflagged full MBP-10 row can be valid again.
F_MAYBE_BAD_BOOK: Final = 4
F_BAD_TS_RECV: Final = 8
F_SNAPSHOT: Final = 32

KNOWN_ACTIONS: Final = ("A", "C", "F", "M", "N", "R", "T")
DEPTH_LEVELS: Final = (1, 3, 5, 10)
_UINT32_MAX: Final = 2**32 - 1
_INT64_MIN: Final = -(2**63)
_INT64_MAX: Final = 2**63 - 1
_OUTRIGHT_PATH_COMPONENT = re.compile(r"^[A-Z0-9]+[FGHJKMNQUVXZ][0-9]{1,2}$")

# These strings are part of the versioned contract.  Changing any formula or
# schema requires a new feature version and rebuilt output partitions.
PILOT_FORMULAS: Final[tuple[tuple[str, str], ...]] = (
    ("selection", "explicit instrument_id and explicit active outright raw_symbol only"),
    ("event_order", "physical Parquet row order across sequential row groups"),
    ("one_second_bucket", "ceil(ts_recv_ns / 1e9) * 1e9; interval (end-1s,end]"),
    ("late_event", "ignore rows whose 1s bucket is older than the current open bucket"),
    ("observed_seconds", "emit one row only for a second containing a selected event"),
    ("book_snapshot", "all book fields come from the last selected event row in the second"),
    ("undefined_price", "encoded int64 max is null before comparison or arithmetic"),
    (
        "valid_second",
        "BBO present and bid<ask and no last-row MAYBE_BAD_BOOK/BAD_TS_RECV/reset flag",
    ),
    ("locked_book", "BBO present and bid==ask"),
    ("crossed_book", "BBO present and bid>ask"),
    ("trade_flow", "action T only: side B buy, side A sell, side N/other unknown"),
    ("signed_trade_volume", "aggressor_buy_volume-aggressor_sell_volume"),
    ("five_minute_bucket", "ceil(one_second_bucket_end_ns / 300e9) * 300e9"),
    ("missing_seconds", "300-observed_seconds; no implicit rows and no forward fill"),
    ("five_minute_prices", "OHLC mid_x2 and spread statistics over valid seconds only"),
    ("five_minute_counts", "sums of closed observed-second counts within (end-5m,end]"),
)


def _formula_fingerprint() -> str:
    payload = json.dumps(PILOT_FORMULAS, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(payload).hexdigest()


FORMULA_SHA256: Final = _formula_fingerprint()


def _schema_metadata(granularity: str) -> dict[bytes, bytes]:
    return {
        b"systematic_fx.feature_version": FEATURE_VERSION.encode(),
        b"systematic_fx.formula_sha256": FORMULA_SHA256.encode(),
        b"systematic_fx.granularity": granularity.encode(),
        b"systematic_fx.price_scale": PRICE_SCALE.encode(),
        b"systematic_fx.research_eligible": b"false",
    }


_IDENTITY_FIELDS = [
    pa.field("feature_version", pa.string(), nullable=False),
    pa.field("research_eligible", pa.bool_(), nullable=False),
    pa.field("source_date", pa.date32(), nullable=False),
    pa.field("contract", pa.string(), nullable=False),
    pa.field("instrument_id", pa.uint32(), nullable=False),
    pa.field("bucket_end", pa.timestamp("ns", tz="UTC"), nullable=False),
]

_ACTION_COUNT_FIELDS = [
    pa.field(f"action_{action.lower()}_count", pa.uint64(), nullable=False)
    for action in KNOWN_ACTIONS
] + [pa.field("action_other_count", pa.uint64(), nullable=False)]

_FLOW_FIELDS = [
    pa.field("event_count", pa.uint64(), nullable=False),
    *_ACTION_COUNT_FIELDS,
    pa.field("trade_count", pa.uint64(), nullable=False),
    pa.field("trade_volume", pa.uint64(), nullable=False),
    pa.field("aggressor_buy_volume", pa.uint64(), nullable=False),
    pa.field("aggressor_sell_volume", pa.uint64(), nullable=False),
    pa.field("unknown_side_trade_volume", pa.uint64(), nullable=False),
    pa.field("signed_trade_volume", pa.int64(), nullable=False),
]

_CUMULATIVE_SIZE_FIELDS = [
    field
    for level in DEPTH_LEVELS
    for field in (
        pa.field(f"bid_cum_size_l{level}", pa.uint64(), nullable=False),
        pa.field(f"ask_cum_size_l{level}", pa.uint64(), nullable=False),
    )
]

ONE_SECOND_SCHEMA: Final = pa.schema(
    [
        *_IDENTITY_FIELDS,
        pa.field("source_last_row", pa.uint64(), nullable=False),
        pa.field("last_action", pa.string(), nullable=False),
        pa.field("last_side", pa.string(), nullable=False),
        pa.field("last_flags", pa.uint8(), nullable=False),
        pa.field("bid_px_00_raw", pa.int64(), nullable=True),
        pa.field("ask_px_00_raw", pa.int64(), nullable=True),
        pa.field("mid_px_x2_raw", pa.int64(), nullable=True),
        pa.field("spread_raw", pa.int64(), nullable=True),
        pa.field("bid_size_00", pa.uint32(), nullable=True),
        pa.field("ask_size_00", pa.uint32(), nullable=True),
        pa.field("bid_count_00", pa.uint32(), nullable=True),
        pa.field("ask_count_00", pa.uint32(), nullable=True),
        *_CUMULATIVE_SIZE_FIELDS,
        pa.field("bid_valid_levels", pa.uint8(), nullable=False),
        pa.field("ask_valid_levels", pa.uint8(), nullable=False),
        *_FLOW_FIELDS,
        pa.field("observed_second", pa.bool_(), nullable=False),
        pa.field("missing_second", pa.bool_(), nullable=False),
        pa.field("book_missing", pa.bool_(), nullable=False),
        pa.field("valid_second", pa.bool_(), nullable=False),
        pa.field("locked_book", pa.bool_(), nullable=False),
        pa.field("crossed_book", pa.bool_(), nullable=False),
        pa.field("maybe_bad_book", pa.bool_(), nullable=False),
        pa.field("bad_ts_recv", pa.bool_(), nullable=False),
        pa.field("snapshot_row", pa.bool_(), nullable=False),
        pa.field("reset_row", pa.bool_(), nullable=False),
        pa.field("price_arithmetic_overflow", pa.bool_(), nullable=False),
    ],
    metadata=_schema_metadata("1s"),
)

FIVE_MINUTE_SCHEMA: Final = pa.schema(
    [
        *_IDENTITY_FIELDS,
        pa.field("first_1s_bucket_end", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("last_1s_bucket_end", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("last_source_row", pa.uint64(), nullable=False),
        pa.field("last_bid_px_00_raw", pa.int64(), nullable=True),
        pa.field("last_ask_px_00_raw", pa.int64(), nullable=True),
        pa.field("last_spread_raw", pa.int64(), nullable=True),
        pa.field("last_bid_cum_size_l10", pa.uint64(), nullable=False),
        pa.field("last_ask_cum_size_l10", pa.uint64(), nullable=False),
        pa.field("mid_px_x2_raw_open", pa.int64(), nullable=True),
        pa.field("mid_px_x2_raw_high", pa.int64(), nullable=True),
        pa.field("mid_px_x2_raw_low", pa.int64(), nullable=True),
        pa.field("mid_px_x2_raw_close", pa.int64(), nullable=True),
        pa.field("spread_raw_min", pa.int64(), nullable=True),
        pa.field("spread_raw_max", pa.int64(), nullable=True),
        pa.field("spread_raw_mean", pa.float64(), nullable=True),
        *_FLOW_FIELDS,
        pa.field("observed_seconds", pa.uint16(), nullable=False),
        pa.field("missing_seconds", pa.uint16(), nullable=False),
        pa.field("valid_seconds", pa.uint16(), nullable=False),
        pa.field("invalid_seconds", pa.uint16(), nullable=False),
        pa.field("book_missing_seconds", pa.uint16(), nullable=False),
        pa.field("locked_seconds", pa.uint16(), nullable=False),
        pa.field("crossed_seconds", pa.uint16(), nullable=False),
        pa.field("maybe_bad_book_seconds", pa.uint16(), nullable=False),
        pa.field("bad_ts_recv_seconds", pa.uint16(), nullable=False),
        pa.field("reset_seconds", pa.uint16(), nullable=False),
        pa.field("source_window_complete", pa.bool_(), nullable=False),
        pa.field("closed_bucket", pa.bool_(), nullable=False),
        pa.field("valid_window", pa.bool_(), nullable=False),
    ],
    metadata=_schema_metadata("5m"),
)

RAW_COLUMNS: Final = (
    "ts_recv",
    "instrument_id",
    "action",
    "side",
    "size",
    "flags",
    *(
        name
        for level in range(10)
        for name in (
            f"bid_px_{level:02d}",
            f"ask_px_{level:02d}",
            f"bid_sz_{level:02d}",
            f"ask_sz_{level:02d}",
            f"bid_ct_{level:02d}",
            f"ask_ct_{level:02d}",
        )
    ),
)


class PilotBuildError(ValueError):
    """The explicit source or requested pilot build is invalid."""


class PilotPathError(PilotBuildError):
    """A source or output path violates the pilot containment contract."""


@dataclass(frozen=True)
class ArtifactReport:
    """Integrity report for one atomically published Parquet artifact."""

    path: str
    sha256: str
    rows: int
    schema_sha256: str
    min_bucket_end: str
    max_bucket_end: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class PilotBuildReport:
    """In-memory build report; no third report artifact is written."""

    feature_version: str
    research_eligible: bool
    source_path: str
    source_sha256: str
    source_rows: int
    selected_rows: int
    late_rows_ignored: int
    instrument_id: int
    contract: str
    source_date: str
    formula_sha256: str
    one_second: ArtifactReport
    five_minute: ArtifactReport

    def as_dict(self) -> dict[str, object]:
        result = asdict(self)
        return result

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), sort_keys=True)


def _schema_fingerprint(schema: pa.Schema) -> str:
    metadata = {
        key.decode("utf-8"): value.decode("utf-8")
        for key, value in sorted((schema.metadata or {}).items())
    }
    payload = {
        "fields": [
            {"name": item.name, "nullable": item.nullable, "type": str(item.type)}
            for item in schema
        ],
        "metadata": metadata,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


ONE_SECOND_SCHEMA_SHA256: Final = _schema_fingerprint(ONE_SECOND_SCHEMA)
FIVE_MINUTE_SCHEMA_SHA256: Final = _schema_fingerprint(FIVE_MINUTE_SCHEMA)


def _right_closed_bucket_end_ns(timestamp_ns: int, width_ns: int) -> int:
    if width_ns <= 0:
        raise ValueError("bucket width must be positive")
    return -(-timestamp_ns // width_ns) * width_ns


def _parse_source_date(value: date | str) -> date:
    if isinstance(value, datetime):
        raise TypeError("source_date must be a date or ISO date string, not datetime")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise TypeError("source_date must be a date or ISO date string")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise PilotBuildError(f"source_date is not an ISO date: {value!r}") from exc


def _utc_ns(value: date) -> int:
    instant = datetime.combine(value, time.min, tzinfo=UTC)
    return int(instant.timestamp()) * ONE_SECOND_NS


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _check_no_symlink_below(root: Path, path: Path) -> None:
    relative = path.relative_to(root)
    cursor = root
    for part in relative.parts:
        cursor = cursor / part
        if cursor.is_symlink():
            raise PilotPathError(f"symlink output component is forbidden: {cursor}")


def _validate_paths(
    raw_parquet_path: Path | str,
    data_root: Path | str,
    *,
    symbol: str,
    source_date: date,
) -> tuple[Path, Path, Path]:
    root_input = Path(data_root).expanduser()
    if root_input.is_symlink():
        raise PilotPathError("data_root itself must not be a symlink")
    try:
        root = root_input.resolve(strict=True)
    except FileNotFoundError as exc:
        raise PilotPathError(f"data_root does not exist: {root_input}") from exc
    if not root.is_dir():
        raise PilotPathError(f"data_root is not a directory: {root}")

    raw_input = Path(raw_parquet_path).expanduser()
    if raw_input.is_symlink():
        raise PilotPathError("raw Parquet path itself must not be a symlink")
    try:
        raw = raw_input.resolve(strict=True)
    except FileNotFoundError as exc:
        raise PilotPathError(f"raw Parquet path does not exist: {raw_input}") from exc
    if not raw.is_file() or raw.suffix.lower() != ".parquet":
        raise PilotPathError(f"raw source must be a regular .parquet file: {raw}")

    derived = root / "derived"
    if derived.is_symlink():
        raise PilotPathError("data_root/derived must not be a symlink")
    if derived.exists() and not derived.is_dir():
        raise PilotPathError("data_root/derived must be a directory")
    derived_resolved = derived.resolve(strict=False)
    if not _is_relative_to(derived_resolved, root):
        raise PilotPathError("derived output escapes data_root")
    if _is_relative_to(raw, derived_resolved):
        raise PilotPathError("raw source must not be inside data_root/derived")

    partition = (
        f"version={FEATURE_VERSION}",
        f"contract={symbol}",
        f"source_date={source_date.isoformat()}",
    )
    one_second = derived.joinpath("features_1s", *partition, "part-000.parquet")
    five_minute = derived.joinpath("research_5m", *partition, "part-000.parquet")

    for target in (one_second, five_minute):
        resolved = target.resolve(strict=False)
        if not _is_relative_to(resolved, derived_resolved):
            raise PilotPathError(f"output path escapes data_root/derived: {target}")
        _check_no_symlink_below(root, target)
        if target.is_symlink():
            raise PilotPathError(f"output target must not be a symlink: {target}")
        if target.exists():
            raise PilotPathError(f"refusing to overwrite existing output: {target}")
        if raw == resolved:
            raise PilotPathError("output target resolves to the raw source")

    return raw, one_second, five_minute


def _validate_explicit_selection(
    parquet_file: pq.ParquetFile,
    *,
    instrument_id: int,
    symbol: str,
    source_date: date,
) -> None:
    validate_mbp10_contract(parquet_file.schema_arrow)
    if isinstance(instrument_id, bool) or not isinstance(instrument_id, int):
        raise TypeError("instrument_id must be an integer")
    if not 0 <= instrument_id <= _UINT32_MAX:
        raise PilotBuildError("instrument_id is outside the uint32 range")
    if not isinstance(symbol, str) or not _OUTRIGHT_PATH_COMPONENT.fullmatch(symbol):
        raise PilotBuildError("symbol must be one safe outright raw-symbol path component")
    if classify_raw_symbol(symbol) is not InstrumentKind.OUTRIGHT:
        raise PilotBuildError(f"symbol is not an outright futures contract: {symbol!r}")

    metadata = parquet_file.schema_arrow.metadata or {}
    payload = metadata.get(b"dbn.metadata")
    if payload is None:
        raise PilotBuildError("raw schema metadata is missing dbn.metadata")
    active = [
        mapping
        for mapping in parse_instrument_mappings(payload)
        if mapping.interval_start <= source_date < mapping.interval_end
        and mapping.instrument_id == instrument_id
    ]
    exact = [
        mapping
        for mapping in active
        if mapping.raw_symbol == symbol and mapping.kind is InstrumentKind.OUTRIGHT
    ]
    if len(exact) != 1 or len(active) != 1:
        raise PilotBuildError(
            "explicit instrument_id/symbol must resolve to exactly one active outright mapping "
            f"on {source_date.isoformat()}"
        )


def _safe_int64(value: int) -> int | None:
    if _INT64_MIN <= value <= _INT64_MAX:
        return value
    return None


def _masked_price(value: object) -> int | None:
    encoded = int(value)
    return None if encoded == UNDEFINED_PRICE else encoded


@dataclass
class _SecondAccumulator:
    bucket_end_ns: int
    event_count: int = 0
    action_counts: dict[str, int] = field(
        default_factory=lambda: {action: 0 for action in KNOWN_ACTIONS}
    )
    action_other_count: int = 0
    trade_count: int = 0
    trade_volume: int = 0
    aggressor_buy_volume: int = 0
    aggressor_sell_volume: int = 0
    unknown_side_trade_volume: int = 0
    source_last_row: int = 0
    last_action: str = ""
    last_side: str = ""
    last_flags: int = 0
    last_snapshot: dict[str, object] = field(default_factory=dict)

    def add(self, columns: dict[str, list[object]], index: int, source_row: int) -> None:
        action = str(columns["action"][index])
        side = str(columns["side"][index])
        size = int(columns["size"][index])
        self.event_count += 1
        if action in self.action_counts:
            self.action_counts[action] += 1
        else:
            self.action_other_count += 1
        if action == "T":
            self.trade_count += 1
            self.trade_volume += size
            if side == "B":
                self.aggressor_buy_volume += size
            elif side == "A":
                self.aggressor_sell_volume += size
            else:
                self.unknown_side_trade_volume += size

        self.source_last_row = source_row
        self.last_action = action
        self.last_side = side
        self.last_flags = int(columns["flags"][index])
        snapshot: dict[str, object] = {}
        for level in range(10):
            suffix = f"{level:02d}"
            for prefix in ("bid_px", "ask_px", "bid_sz", "ask_sz", "bid_ct", "ask_ct"):
                key = f"{prefix}_{suffix}"
                snapshot[key] = columns[key][index]
        self.last_snapshot = snapshot

    def record(self, *, source_date: date, instrument_id: int, symbol: str) -> dict[str, object]:
        bid_prices = [
            _masked_price(self.last_snapshot[f"bid_px_{level:02d}"]) for level in range(10)
        ]
        ask_prices = [
            _masked_price(self.last_snapshot[f"ask_px_{level:02d}"]) for level in range(10)
        ]
        bid = bid_prices[0]
        ask = ask_prices[0]
        book_missing = bid is None or ask is None
        locked = bid is not None and ask is not None and bid == ask
        crossed = bid is not None and ask is not None and bid > ask

        mid_x2 = _safe_int64(bid + ask) if bid is not None and ask is not None else None
        spread = _safe_int64(ask - bid) if bid is not None and ask is not None else None
        arithmetic_overflow = not book_missing and (mid_x2 is None or spread is None)

        bid_cumulative: list[int] = []
        ask_cumulative: list[int] = []
        bid_running = 0
        ask_running = 0
        for level in range(10):
            suffix = f"{level:02d}"
            if bid_prices[level] is not None:
                bid_running += int(self.last_snapshot[f"bid_sz_{suffix}"])
            if ask_prices[level] is not None:
                ask_running += int(self.last_snapshot[f"ask_sz_{suffix}"])
            bid_cumulative.append(bid_running)
            ask_cumulative.append(ask_running)

        maybe_bad_book = bool(self.last_flags & F_MAYBE_BAD_BOOK)
        bad_ts_recv = bool(self.last_flags & F_BAD_TS_RECV)
        snapshot_row = bool(self.last_flags & F_SNAPSHOT)
        reset_row = self.last_action == "R"
        valid_second = (
            not book_missing
            and not locked
            and not crossed
            and not maybe_bad_book
            and not bad_ts_recv
            and not reset_row
            and not arithmetic_overflow
        )

        result: dict[str, object] = {
            "feature_version": FEATURE_VERSION,
            "research_eligible": RESEARCH_ELIGIBLE,
            "source_date": source_date,
            "contract": symbol,
            "instrument_id": instrument_id,
            "bucket_end": self.bucket_end_ns,
            "source_last_row": self.source_last_row,
            "last_action": self.last_action,
            "last_side": self.last_side,
            "last_flags": self.last_flags,
            "bid_px_00_raw": bid,
            "ask_px_00_raw": ask,
            "mid_px_x2_raw": mid_x2,
            "spread_raw": spread,
            "bid_size_00": (None if bid is None else int(self.last_snapshot["bid_sz_00"])),
            "ask_size_00": (None if ask is None else int(self.last_snapshot["ask_sz_00"])),
            "bid_count_00": (None if bid is None else int(self.last_snapshot["bid_ct_00"])),
            "ask_count_00": (None if ask is None else int(self.last_snapshot["ask_ct_00"])),
            "bid_valid_levels": sum(price is not None for price in bid_prices),
            "ask_valid_levels": sum(price is not None for price in ask_prices),
            "event_count": self.event_count,
            "action_other_count": self.action_other_count,
            "trade_count": self.trade_count,
            "trade_volume": self.trade_volume,
            "aggressor_buy_volume": self.aggressor_buy_volume,
            "aggressor_sell_volume": self.aggressor_sell_volume,
            "unknown_side_trade_volume": self.unknown_side_trade_volume,
            "signed_trade_volume": self.aggressor_buy_volume - self.aggressor_sell_volume,
            "observed_second": True,
            "missing_second": False,
            "book_missing": book_missing,
            "valid_second": valid_second,
            "locked_book": locked,
            "crossed_book": crossed,
            "maybe_bad_book": maybe_bad_book,
            "bad_ts_recv": bad_ts_recv,
            "snapshot_row": snapshot_row,
            "reset_row": reset_row,
            "price_arithmetic_overflow": arithmetic_overflow,
        }
        for action in KNOWN_ACTIONS:
            result[f"action_{action.lower()}_count"] = self.action_counts[action]
        for level in DEPTH_LEVELS:
            result[f"bid_cum_size_l{level}"] = bid_cumulative[level - 1]
            result[f"ask_cum_size_l{level}"] = ask_cumulative[level - 1]
        return result


def _read_one_second_records(
    parquet_file: pq.ParquetFile,
    *,
    instrument_id: int,
    symbol: str,
    source_date: date,
) -> tuple[list[dict[str, object]], int, int]:
    start_ns = _utc_ns(source_date)
    end_ns = _utc_ns(source_date + timedelta(days=1))
    records: list[dict[str, object]] = []
    current: _SecondAccumulator | None = None
    source_row_offset = 0
    selected_rows = 0
    late_rows_ignored = 0

    for row_group_index in range(parquet_file.num_row_groups):
        table = parquet_file.read_row_group(
            row_group_index,
            columns=list(RAW_COLUMNS),
            use_threads=False,
        )
        ids = table["instrument_id"].combine_chunks().to_numpy(zero_copy_only=False)
        selected_indices = np.flatnonzero(ids == instrument_id)
        if selected_indices.size == 0:
            source_row_offset += table.num_rows
            continue

        selected = table.take(pa.array(selected_indices, type=pa.int64()))
        columns = {name: selected[name].combine_chunks().to_pylist() for name in RAW_COLUMNS}
        timestamps = pc.cast(selected["ts_recv"].combine_chunks(), pa.int64()).to_pylist()

        for selected_index, local_row_index in enumerate(selected_indices.tolist()):
            timestamp_ns = int(timestamps[selected_index])
            if not start_ns <= timestamp_ns < end_ns:
                raise PilotBuildError(
                    "selected ts_recv lies outside explicit source_date: "
                    f"row={source_row_offset + local_row_index}, ts_recv_ns={timestamp_ns}"
                )
            selected_rows += 1
            bucket_end_ns = _right_closed_bucket_end_ns(timestamp_ns, ONE_SECOND_NS)
            if current is None:
                current = _SecondAccumulator(bucket_end_ns)
            elif bucket_end_ns < current.bucket_end_ns:
                late_rows_ignored += 1
                continue
            elif bucket_end_ns > current.bucket_end_ns:
                records.append(
                    current.record(
                        source_date=source_date,
                        instrument_id=instrument_id,
                        symbol=symbol,
                    )
                )
                current = _SecondAccumulator(bucket_end_ns)
            current.add(
                columns,
                selected_index,
                source_row_offset + int(local_row_index),
            )
        source_row_offset += table.num_rows

    if current is not None:
        records.append(
            current.record(
                source_date=source_date,
                instrument_id=instrument_id,
                symbol=symbol,
            )
        )
    if selected_rows == 0:
        raise PilotBuildError("explicit instrument_id has no rows in the source file")
    if not records:
        raise PilotBuildError("no selected events remained after the late-event policy")
    return records, selected_rows, late_rows_ignored


def _sum_fields(rows: list[dict[str, object]], fields: list[str]) -> dict[str, int]:
    return {name: sum(int(row[name]) for row in rows) for name in fields}


def _five_minute_record(
    rows: list[dict[str, object]],
    *,
    bucket_end_ns: int,
    source_date: date,
) -> dict[str, object]:
    first = rows[0]
    last = rows[-1]
    observed = len(rows)
    if observed > 300:
        raise PilotBuildError("a five-minute bucket contains more than 300 observed seconds")
    valid_rows = [row for row in rows if bool(row["valid_second"])]
    mids = [int(row["mid_px_x2_raw"]) for row in valid_rows]
    spreads = [int(row["spread_raw"]) for row in valid_rows]

    flow_names = [field.name for field in _FLOW_FIELDS]
    totals = _sum_fields(rows, flow_names)
    day_start_ns = _utc_ns(source_date)
    day_end_ns = _utc_ns(source_date + timedelta(days=1))
    source_window_complete = (
        bucket_end_ns - FIVE_MINUTE_NS >= day_start_ns and bucket_end_ns <= day_end_ns
    )
    valid_seconds = len(valid_rows)
    result: dict[str, object] = {
        "feature_version": FEATURE_VERSION,
        "research_eligible": RESEARCH_ELIGIBLE,
        "source_date": source_date,
        "contract": first["contract"],
        "instrument_id": first["instrument_id"],
        "bucket_end": bucket_end_ns,
        "first_1s_bucket_end": first["bucket_end"],
        "last_1s_bucket_end": last["bucket_end"],
        "last_source_row": last["source_last_row"],
        "last_bid_px_00_raw": last["bid_px_00_raw"],
        "last_ask_px_00_raw": last["ask_px_00_raw"],
        "last_spread_raw": last["spread_raw"],
        "last_bid_cum_size_l10": last["bid_cum_size_l10"],
        "last_ask_cum_size_l10": last["ask_cum_size_l10"],
        "mid_px_x2_raw_open": mids[0] if mids else None,
        "mid_px_x2_raw_high": max(mids) if mids else None,
        "mid_px_x2_raw_low": min(mids) if mids else None,
        "mid_px_x2_raw_close": mids[-1] if mids else None,
        "spread_raw_min": min(spreads) if spreads else None,
        "spread_raw_max": max(spreads) if spreads else None,
        "spread_raw_mean": (sum(spreads) / len(spreads)) if spreads else None,
        **totals,
        "observed_seconds": observed,
        "missing_seconds": 300 - observed,
        "valid_seconds": valid_seconds,
        "invalid_seconds": observed - valid_seconds,
        "book_missing_seconds": sum(bool(row["book_missing"]) for row in rows),
        "locked_seconds": sum(bool(row["locked_book"]) for row in rows),
        "crossed_seconds": sum(bool(row["crossed_book"]) for row in rows),
        "maybe_bad_book_seconds": sum(bool(row["maybe_bad_book"]) for row in rows),
        "bad_ts_recv_seconds": sum(bool(row["bad_ts_recv"]) for row in rows),
        "reset_seconds": sum(bool(row["reset_row"]) for row in rows),
        "source_window_complete": source_window_complete,
        "closed_bucket": True,
        "valid_window": source_window_complete and observed == 300 and valid_seconds == observed,
    }
    return result


def _build_five_minute_records(
    one_second_records: list[dict[str, object]], *, source_date: date
) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    current_end: int | None = None
    current_rows: list[dict[str, object]] = []
    for row in one_second_records:
        bucket_end_ns = _right_closed_bucket_end_ns(int(row["bucket_end"]), FIVE_MINUTE_NS)
        if current_end is None:
            current_end = bucket_end_ns
        elif bucket_end_ns != current_end:
            result.append(
                _five_minute_record(
                    current_rows,
                    bucket_end_ns=current_end,
                    source_date=source_date,
                )
            )
            current_end = bucket_end_ns
            current_rows = []
        current_rows.append(row)
    if current_end is not None:
        result.append(
            _five_minute_record(
                current_rows,
                bucket_end_ns=current_end,
                source_date=source_date,
            )
        )
    return result


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _timestamp_iso(value: object) -> str:
    if not isinstance(value, datetime):
        raise PilotBuildError("artifact bucket timestamp did not decode as datetime")
    return value.isoformat()


@dataclass(frozen=True)
class _StagedArtifact:
    temporary_path: Path
    target_path: Path
    sha256: str
    rows: int
    schema_sha256: str
    min_bucket_end: str
    max_bucket_end: str

    def report(self) -> ArtifactReport:
        return ArtifactReport(
            path=str(self.target_path),
            sha256=self.sha256,
            rows=self.rows,
            schema_sha256=self.schema_sha256,
            min_bucket_end=self.min_bucket_end,
            max_bucket_end=self.max_bucket_end,
        )


def _stage_table(table: pa.Table, target: Path, schema_sha256: str) -> _StagedArtifact:
    target.parent.mkdir(parents=True, exist_ok=True)
    data_root = next(parent for parent in target.parents if parent.name == "derived").parent
    _check_no_symlink_below(data_root, target)
    if target.exists() or target.is_symlink():
        raise PilotPathError(f"refusing to overwrite existing output: {target}")

    descriptor, temporary_name = tempfile.mkstemp(
        dir=target.parent,
        prefix=".part-000.",
        suffix=".parquet.tmp",
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        pq.write_table(
            table,
            temporary,
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
        )
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        check = pq.ParquetFile(temporary)
        if check.metadata.num_rows != table.num_rows or check.schema_arrow != table.schema:
            raise PilotBuildError(f"staged Parquet validation failed: {target}")
        bucket = check.read(columns=["bucket_end"])["bucket_end"]
        minimum = pc.min(bucket).as_py()
        maximum = pc.max(bucket).as_py()
        return _StagedArtifact(
            temporary_path=temporary,
            target_path=target,
            sha256=_sha256_file(temporary),
            rows=table.num_rows,
            schema_sha256=schema_sha256,
            min_bucket_end=_timestamp_iso(minimum),
            max_bucket_end=_timestamp_iso(maximum),
        )
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _publish_staged(staged: tuple[_StagedArtifact, ...]) -> None:
    published: list[Path] = []
    try:
        for artifact in staged:
            # A same-filesystem hard link publishes the fully closed file in one
            # atomic namespace operation and, unlike replace(), cannot overwrite
            # a concurrently-created target.
            os.link(
                artifact.temporary_path,
                artifact.target_path,
                follow_symlinks=False,
            )
            published.append(artifact.target_path)
    except FileExistsError as exc:
        raise PilotPathError("refusing to overwrite a concurrently-created output") from exc
    finally:
        if len(published) != len(staged):
            for path in published:
                path.unlink(missing_ok=True)
        for artifact in staged:
            artifact.temporary_path.unlink(missing_ok=True)


def build_pilot_features(
    raw_parquet_path: Path | str,
    *,
    data_root: Path | str,
    instrument_id: int,
    symbol: str,
    source_date: date | str,
) -> PilotBuildReport:
    """Build the explicit non-research 1-second and 5-minute pilot artifacts.

    The caller must provide the daily raw file, the provider's daily instrument
    ID, its outright raw symbol, and the UTC source date.  There is intentionally
    no active-contract, expiry, or roll-selection behavior in this function.
    Existing outputs are never overwritten.
    """

    parsed_date = _parse_source_date(source_date)
    if isinstance(instrument_id, bool) or not isinstance(instrument_id, int):
        raise TypeError("instrument_id must be an integer")
    if not isinstance(symbol, str):
        raise TypeError("symbol must be a string")
    if not _OUTRIGHT_PATH_COMPONENT.fullmatch(symbol):
        raise PilotBuildError("symbol must be one safe outright raw-symbol path component")

    raw, one_second_path, five_minute_path = _validate_paths(
        raw_parquet_path,
        data_root,
        symbol=symbol,
        source_date=parsed_date,
    )
    source_stat_before = raw.stat()
    parquet_file = pq.ParquetFile(raw)
    _validate_explicit_selection(
        parquet_file,
        instrument_id=instrument_id,
        symbol=symbol,
        source_date=parsed_date,
    )

    one_second_records, selected_rows, late_rows_ignored = _read_one_second_records(
        parquet_file,
        instrument_id=instrument_id,
        symbol=symbol,
        source_date=parsed_date,
    )
    five_minute_records = _build_five_minute_records(
        one_second_records,
        source_date=parsed_date,
    )
    one_second_table = pa.Table.from_pylist(one_second_records, schema=ONE_SECOND_SCHEMA)
    five_minute_table = pa.Table.from_pylist(five_minute_records, schema=FIVE_MINUTE_SCHEMA)

    source_sha256 = _sha256_file(raw)
    source_stat_after = raw.stat()
    if (
        source_stat_before.st_dev,
        source_stat_before.st_ino,
        source_stat_before.st_size,
        source_stat_before.st_mtime_ns,
    ) != (
        source_stat_after.st_dev,
        source_stat_after.st_ino,
        source_stat_after.st_size,
        source_stat_after.st_mtime_ns,
    ):
        raise PilotBuildError("raw source changed while the pilot build was running")

    staged: list[_StagedArtifact] = []
    try:
        staged.append(_stage_table(one_second_table, one_second_path, ONE_SECOND_SCHEMA_SHA256))
        staged.append(_stage_table(five_minute_table, five_minute_path, FIVE_MINUTE_SCHEMA_SHA256))
        _publish_staged(tuple(staged))
    except Exception:
        for artifact in staged:
            artifact.temporary_path.unlink(missing_ok=True)
        raise

    return PilotBuildReport(
        feature_version=FEATURE_VERSION,
        research_eligible=RESEARCH_ELIGIBLE,
        source_path=str(raw),
        source_sha256=source_sha256,
        source_rows=parquet_file.metadata.num_rows,
        selected_rows=selected_rows,
        late_rows_ignored=late_rows_ignored,
        instrument_id=instrument_id,
        contract=symbol,
        source_date=parsed_date.isoformat(),
        formula_sha256=FORMULA_SHA256,
        one_second=staged[0].report(),
        five_minute=staged[1].report(),
    )
