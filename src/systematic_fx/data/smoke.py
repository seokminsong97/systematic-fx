"""Bounded structural smoke checks for MBP-10 Parquet event rows.

The checker intentionally reads only the first ``max_row_groups`` row groups.
It is an early ingestion gate, not a replacement for a complete daily quality
scan.  BBO state counts are diagnostics because an incomplete, locked, or
crossed book can legitimately occur while a book is being reconstructed.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

STRUCTURAL_COLUMNS = (
    "ts_recv",
    "ts_event",
    "instrument_id",
    "action",
    "side",
    "depth",
    "bid_px_00",
    "ask_px_00",
)

# Databento DBN enum values. Keeping these literals local avoids requiring the
# separate ``databento-dbn`` runtime package merely to validate Parquet rows.
# MBP-n carries aggregate book changes and trades, not MBO Fill records. ``N``
# is retained for flag-only/normalization records rolled out by Databento.
KNOWN_ACTIONS = frozenset({"A", "C", "M", "N", "R", "T"})
KNOWN_SIDES = frozenset({"A", "B", "N"})
DEFAULT_UNDEFINED_PRICE = 2**63 - 1


class SmokeCheckError(ValueError):
    """The source cannot be structurally checked as an MBP-10 Parquet file."""


@dataclass(frozen=True)
class EventSmokeResult:
    """JSON-safe aggregate from a bounded event-row scan."""

    source_path: str
    row_groups_available: int
    row_groups_scanned: int
    rows_scanned: int
    mapped_instrument_count: int
    ts_recv_regressions: int
    ts_recv_before_ts_event: int
    depth_out_of_range: int
    unknown_actions: int
    unknown_sides: int
    unknown_instrument_ids: int
    invalid_bbo: int
    locked_bbo: int
    crossed_bbo: int
    structural_violations: int
    passed: bool

    def as_dict(self) -> dict[str, object]:
        """Return only standard JSON-compatible values."""

        return asdict(self)

    def to_json(self) -> str:
        """Serialize the result deterministically for CLI and artifact output."""

        return json.dumps(self.as_dict(), sort_keys=True)


def _metadata_value(metadata: Mapping[bytes, bytes], key: bytes) -> bytes | None:
    value = metadata.get(key)
    if value is None:
        return None
    if not isinstance(value, bytes):
        raise SmokeCheckError(f"Parquet metadata {key.decode()} is not bytes")
    return value


def _mapped_instrument_ids(metadata: Mapping[bytes, bytes]) -> frozenset[int]:
    raw_metadata = _metadata_value(metadata, b"dbn.metadata")
    if raw_metadata is None:
        raise SmokeCheckError("Parquet metadata is missing dbn.metadata")

    try:
        dbn_metadata = json.loads(raw_metadata)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SmokeCheckError("Parquet dbn.metadata is not valid JSON") from exc

    mappings = dbn_metadata.get("mappings")
    if not isinstance(mappings, list):
        raise SmokeCheckError("Parquet dbn.metadata has no mappings list")

    instrument_ids: set[int] = set()
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        intervals = mapping.get("intervals", ())
        if not isinstance(intervals, list):
            continue
        for interval in intervals:
            if not isinstance(interval, dict):
                continue
            symbol = interval.get("symbol")
            try:
                instrument_ids.add(int(symbol))
            except (TypeError, ValueError):
                continue

    if not instrument_ids:
        raise SmokeCheckError("Parquet dbn.metadata contains no mapped instrument IDs")
    return frozenset(instrument_ids)


def _undefined_price(metadata: Mapping[bytes, bytes]) -> int:
    raw_price = _metadata_value(metadata, b"mbo_mbp10.undefined_price")
    if raw_price is None:
        return DEFAULT_UNDEFINED_PRICE
    try:
        return int(raw_price)
    except (UnicodeDecodeError, ValueError) as exc:
        raise SmokeCheckError(
            "Parquet mbo_mbp10.undefined_price metadata is not an integer"
        ) from exc


def _count_true(mask: Any) -> int:
    """Count true values while treating null comparison results as false."""

    non_null_mask = pc.fill_null(mask, False)
    total = pc.sum(non_null_mask).as_py()
    return int(total or 0)


def _unknown_count(values: Any, known_values: pa.Array) -> int:
    known_mask = pc.fill_null(pc.is_in(values, value_set=known_values), False)
    return _count_true(pc.invert(known_mask))


def _valid_price(values: Any, undefined_price: int) -> Any:
    # Calendar-spread prices can legitimately be zero or negative. Instrument
    # classification happens later, so this all-instrument smoke scan treats a
    # price as invalid only when it is null or the encoded undefined sentinel.
    return pc.fill_null(pc.not_equal(values, undefined_price), False)


def smoke_check_parquet(
    source_path: Path,
    *,
    max_row_groups: int = 1,
) -> EventSmokeResult:
    """Check a bounded prefix of one MBP-10 Parquet file.

    Structural counters determine ``passed``. BBO counters are validity masks
    only and never fail this smoke check. A full quality job must still scan all
    row groups before a source day becomes research-eligible.
    """

    if max_row_groups < 1:
        raise ValueError("max_row_groups must be at least 1")

    path = source_path.expanduser().resolve()
    parquet_file = pq.ParquetFile(path)
    file_metadata = parquet_file.metadata.metadata or {}
    mapped_ids = _mapped_instrument_ids(file_metadata)
    undefined_price = _undefined_price(file_metadata)

    available_columns = frozenset(parquet_file.schema_arrow.names)
    missing_columns = sorted(set(STRUCTURAL_COLUMNS) - available_columns)
    if missing_columns:
        raise SmokeCheckError(
            "Parquet file is missing structural columns: " + ", ".join(missing_columns)
        )

    row_groups_scanned = min(max_row_groups, parquet_file.num_row_groups)
    rows_scanned = 0
    ts_recv_regressions = 0
    ts_recv_before_ts_event = 0
    depth_out_of_range = 0
    unknown_actions = 0
    unknown_sides = 0
    unknown_instrument_ids = 0
    invalid_bbo = 0
    locked_bbo = 0
    crossed_bbo = 0
    previous_ts_recv: object | None = None

    known_action_values = pa.array(sorted(KNOWN_ACTIONS), type=pa.string())
    known_side_values = pa.array(sorted(KNOWN_SIDES), type=pa.string())
    mapped_id_values = pa.array(sorted(mapped_ids), type=pa.uint64())

    for row_group_index in range(row_groups_scanned):
        table = parquet_file.read_row_group(
            row_group_index,
            columns=list(STRUCTURAL_COLUMNS),
            use_threads=False,
        )
        row_count = table.num_rows
        rows_scanned += row_count
        if row_count == 0:
            continue

        ts_recv = table["ts_recv"]
        ts_event = table["ts_event"]
        if previous_ts_recv is not None:
            first_ts_recv = ts_recv[0].as_py()
            if first_ts_recv is not None and first_ts_recv < previous_ts_recv:
                ts_recv_regressions += 1
        if row_count > 1:
            ts_recv_regressions += _count_true(
                pc.less(ts_recv.slice(1), ts_recv.slice(0, row_count - 1))
            )
        previous_ts_recv = ts_recv[row_count - 1].as_py()

        ts_recv_before_ts_event += _count_true(pc.less(ts_recv, ts_event))

        depth = table["depth"]
        invalid_depth_mask = pc.or_kleene(pc.less(depth, 0), pc.greater(depth, 9))
        depth_out_of_range += _count_true(invalid_depth_mask)

        unknown_actions += _unknown_count(table["action"], known_action_values)
        unknown_sides += _unknown_count(table["side"], known_side_values)
        unknown_instrument_ids += _unknown_count(
            pc.cast(table["instrument_id"], pa.uint64()), mapped_id_values
        )

        bid_price = table["bid_px_00"]
        ask_price = table["ask_px_00"]
        valid_bbo = pc.and_kleene(
            _valid_price(bid_price, undefined_price),
            _valid_price(ask_price, undefined_price),
        )
        valid_bbo = pc.fill_null(valid_bbo, False)
        invalid_bbo += _count_true(pc.invert(valid_bbo))
        locked_bbo += _count_true(pc.and_kleene(valid_bbo, pc.equal(bid_price, ask_price)))
        crossed_bbo += _count_true(pc.and_kleene(valid_bbo, pc.greater(bid_price, ask_price)))

    structural_violations = sum(
        (
            ts_recv_regressions,
            depth_out_of_range,
            unknown_actions,
            unknown_sides,
            unknown_instrument_ids,
        )
    )
    return EventSmokeResult(
        source_path=str(path),
        row_groups_available=parquet_file.num_row_groups,
        row_groups_scanned=row_groups_scanned,
        rows_scanned=rows_scanned,
        mapped_instrument_count=len(mapped_ids),
        ts_recv_regressions=ts_recv_regressions,
        ts_recv_before_ts_event=ts_recv_before_ts_event,
        depth_out_of_range=depth_out_of_range,
        unknown_actions=unknown_actions,
        unknown_sides=unknown_sides,
        unknown_instrument_ids=unknown_instrument_ids,
        invalid_bbo=invalid_bbo,
        locked_bbo=locked_bbo,
        crossed_bbo=crossed_bbo,
        structural_violations=structural_violations,
        passed=structural_violations == 0,
    )
