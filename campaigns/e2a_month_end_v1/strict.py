"""Strict physical-row audit semantics for the frozen e2a candidate.

This module deliberately does not claim to be the repo-governed entry policy.
The handover never froze reset/recovery handling.  The replay below names the
minimal policy used for the independent audit: a reset or snapshot row is not
executable, and the next clean ordinary physical row immediately rearms the
book.  The command-line audit remains fail-closed because that policy differs
from :class:`systematic_fx.backtest.entry._ResetAwareBook`.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Final

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .config import E2AConfig
from .engine import (
    F_BAD_TS_RECV,
    F_MAYBE_BAD_BOOK,
    F_SNAPSHOT,
    SECOND_NS,
    TICK_PRICE_RAW,
    UNDEFINED_PRICE,
    DayPlan,
    E2AReproductionError,
    RawDataset,
    SignalEvent,
    derive_signals,
)

RESET_POLICY: Final = "LAB_MINIMAL_NEXT_CLEAN_ROW_REARM_NOT_REPO_GOVERNED"
SEMANTIC_AMBIGUITY: Final = (
    "SECTION2_DOES_NOT_FREEZE_RESET_RECOVERY;_THIS_REPLAY_IS_NOT_REPO_GOVERNED_ENTRY"
)
STRICT_BAD_FLAG_MASK: Final = F_MAYBE_BAD_BOOK | F_BAD_TS_RECV | F_SNAPSHOT
_QUOTE_COLUMNS: Final = (
    "ts_recv",
    "instrument_id",
    "action",
    "flags",
    "bid_px_00",
    "ask_px_00",
    "bid_sz_00",
    "ask_sz_00",
)


@dataclass(frozen=True, slots=True)
class PhysicalQuote:
    """One executable selected-contract physical MBP-10 row."""

    source_date: date
    ts_recv_ns: int
    bid_ticks: int
    ask_ticks: int
    bid_size: int
    ask_size: int
    flags: int

    @property
    def mid_x2(self) -> int:
        return self.bid_ticks + self.ask_ticks

    def as_dict(self) -> dict[str, object]:
        return {
            "ask_size": self.ask_size,
            "ask_ticks": self.ask_ticks,
            "bid_size": self.bid_size,
            "bid_ticks": self.bid_ticks,
            "flags": self.flags,
            "source_date": self.source_date.isoformat(),
            "ts_recv_ns": self.ts_recv_ns,
        }


@dataclass(frozen=True, slots=True)
class StrictPhysicalTrade:
    signal: SignalEvent
    entry: PhysicalQuote
    exit: PhysicalQuote
    entry_px: int
    exit_px: int
    exit_kind: str

    @property
    def gross_ticks(self) -> int:
        return self.signal.direction * (self.exit_px - self.entry_px)

    @property
    def directed_mid_x2(self) -> int:
        return self.signal.direction * (self.exit.mid_x2 - self.entry.mid_x2)

    def as_dict(self) -> dict[str, object]:
        return {
            **self.signal.as_dict(),
            "directed_mid_ticks": f"{self.directed_mid_x2 / 2:.1f}",
            "entry": self.entry.as_dict(),
            "entry_px": self.entry_px,
            "exit": self.exit.as_dict(),
            "exit_kind": self.exit_kind,
            "exit_px": self.exit_px,
            "gross_ticks": self.gross_ticks,
        }


@dataclass(frozen=True, slots=True)
class StrictPhysicalSkip:
    signal: SignalEvent
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {**self.signal.as_dict(), "reason": self.reason}


@dataclass(frozen=True, slots=True)
class StrictPhysicalReplay:
    trades: tuple[StrictPhysicalTrade, ...]
    skips: tuple[StrictPhysicalSkip, ...]


def _timestamp_ns(column: pa.ChunkedArray | pa.Array) -> np.ndarray:
    return pc.cast(column, pa.int64()).to_numpy(zero_copy_only=False).astype(np.int64, copy=False)


def _statistic_ns(value: object) -> int:
    if hasattr(value, "value"):
        return int(value.value)  # type: ignore[union-attr]
    return int(np.datetime64(value, "ns").astype(np.int64))


def strict_valid_quote_mask(
    *,
    instrument_id: np.ndarray,
    action: np.ndarray,
    flags: np.ndarray,
    bid_raw: np.ndarray,
    ask_raw: np.ndarray,
    bid_size: np.ndarray,
    ask_size: np.ndarray,
    selected_instrument_id: int,
) -> np.ndarray:
    """Return the frozen lab-minimal physical-row executability mask."""

    defined = (
        (bid_raw != UNDEFINED_PRICE) & (ask_raw != UNDEFINED_PRICE) & (bid_raw > 0) & (ask_raw > 0)
    )
    aligned = (bid_raw % TICK_PRICE_RAW == 0) & (ask_raw % TICK_PRICE_RAW == 0)
    return (
        (instrument_id == selected_instrument_id)
        & (action != "R")
        & ((flags & STRICT_BAD_FLAG_MASK) == 0)
        & defined
        & aligned
        & (bid_raw < ask_raw)
        & (bid_size >= 1)
        & (ask_size >= 1)
    )


class StrictPhysicalQuoteReader:
    """Lazy row-group-pruned reader with source verification inherited from RawDataset."""

    def __init__(self, dataset: RawDataset) -> None:
        self.dataset = dataset
        self._bounds: dict[date, tuple[PhysicalQuote | None, PhysicalQuote | None]] = {}

    @staticmethod
    def _row_group_range(parquet: pq.ParquetFile, row_group: int) -> tuple[int, int]:
        metadata = parquet.metadata.row_group(row_group)
        for index in range(metadata.num_columns):
            column = metadata.column(index)
            if column.path_in_schema == "ts_recv":
                statistics = column.statistics
                if statistics is None or not statistics.has_min_max:
                    return -(1 << 63), (1 << 63) - 1
                return _statistic_ns(statistics.min), _statistic_ns(statistics.max)
        raise E2AReproductionError("raw quote source has no ts_recv column statistics")

    @staticmethod
    def _quotes(
        table: pa.Table,
        plan: DayPlan,
        *,
        start_ns: int | None,
        end_ns: int | None,
    ) -> tuple[np.ndarray, ...]:
        timestamps = _timestamp_ns(table["ts_recv"])
        instrument = table["instrument_id"].to_numpy(zero_copy_only=False)
        action = table["action"].to_numpy(zero_copy_only=False)
        flags = table["flags"].to_numpy(zero_copy_only=False)
        bid_raw = table["bid_px_00"].to_numpy(zero_copy_only=False).astype(np.int64)
        ask_raw = table["ask_px_00"].to_numpy(zero_copy_only=False).astype(np.int64)
        bid_size = table["bid_sz_00"].to_numpy(zero_copy_only=False)
        ask_size = table["ask_sz_00"].to_numpy(zero_copy_only=False)
        valid = strict_valid_quote_mask(
            instrument_id=instrument,
            action=action,
            flags=flags,
            bid_raw=bid_raw,
            ask_raw=ask_raw,
            bid_size=bid_size,
            ask_size=ask_size,
            selected_instrument_id=plan.instrument_id,
        )
        if start_ns is not None:
            valid &= timestamps >= start_ns
        if end_ns is not None:
            valid &= timestamps <= end_ns
        return timestamps, flags, bid_raw, ask_raw, bid_size, ask_size, valid

    @staticmethod
    def _quote_at(
        values: tuple[np.ndarray, ...],
        position: int,
        source_date: date,
    ) -> PhysicalQuote:
        timestamps, flags, bid_raw, ask_raw, bid_size, ask_size, _valid = values
        return PhysicalQuote(
            source_date=source_date,
            ts_recv_ns=int(timestamps[position]),
            bid_ticks=int(bid_raw[position] // TICK_PRICE_RAW),
            ask_ticks=int(ask_raw[position] // TICK_PRICE_RAW),
            bid_size=int(bid_size[position]),
            ask_size=int(ask_size[position]),
            flags=int(flags[position]),
        )

    def first_valid(
        self,
        plan: DayPlan,
        *,
        start_ns: int | None = None,
        end_ns: int | None = None,
    ) -> PhysicalQuote | None:
        with self.dataset.verified_parquet(plan) as parquet:
            for row_group in range(parquet.metadata.num_row_groups):
                minimum, maximum = self._row_group_range(parquet, row_group)
                if start_ns is not None and maximum < start_ns:
                    continue
                if end_ns is not None and minimum > end_ns:
                    break
                values = self._quotes(
                    parquet.read_row_group(
                        row_group,
                        columns=list(_QUOTE_COLUMNS),
                        use_threads=False,
                    ),
                    plan,
                    start_ns=start_ns,
                    end_ns=end_ns,
                )
                positions = np.flatnonzero(values[-1])
                if len(positions):
                    return self._quote_at(values, int(positions[0]), plan.source_date)
        return None

    def last_valid(self, plan: DayPlan) -> PhysicalQuote | None:
        cached = self._bounds.get(plan.source_date)
        if cached is not None:
            return cached[1]
        last: PhysicalQuote | None = None
        with self.dataset.verified_parquet(plan) as parquet:
            for row_group in range(parquet.metadata.num_row_groups - 1, -1, -1):
                values = self._quotes(
                    parquet.read_row_group(
                        row_group,
                        columns=list(_QUOTE_COLUMNS),
                        use_threads=False,
                    ),
                    plan,
                    start_ns=None,
                    end_ns=None,
                )
                positions = np.flatnonzero(values[-1])
                if len(positions):
                    last = self._quote_at(values, int(positions[-1]), plan.source_date)
                    break
        first = self.first_valid(plan) if last is not None else None
        self._bounds[plan.source_date] = (first, last)
        return last

    def first_and_last(
        self,
        plan: DayPlan,
    ) -> tuple[PhysicalQuote | None, PhysicalQuote | None]:
        cached = self._bounds.get(plan.source_date)
        if cached is not None:
            return cached
        last = self.last_valid(plan)
        return self._bounds.get(plan.source_date, (None, last))


def derive_strict_signals(dataset: RawDataset, config: E2AConfig) -> tuple[SignalEvent, ...]:
    """Reuse the frozen +60-second month-open and physical p15 signal contract."""

    return derive_signals(dataset, config)


def replay_strict_physical(
    dataset: RawDataset,
    config: E2AConfig,
    signals: tuple[SignalEvent, ...],
    *,
    reader: StrictPhysicalQuoteReader | None = None,
) -> StrictPhysicalReplay:
    """Replay exact physical rows under :data:`RESET_POLICY`."""

    quote_reader = reader or StrictPhysicalQuoteReader(dataset)
    plan_index = {plan.source_date: index for index, plan in enumerate(dataset.plans)}
    trades: list[StrictPhysicalTrade] = []
    skips: list[StrictPhysicalSkip] = []
    busy_until_ns = -1
    for signal in sorted(signals, key=lambda item: item.decision_epoch):
        decision_ns = signal.decision_epoch * SECOND_NS
        if decision_ns <= busy_until_ns:
            skips.append(StrictPhysicalSkip(signal, "POSITION_ALREADY_OPEN"))
            continue
        plan = dataset.by_date[signal.event_date]
        eligibility_ns = decision_ns + config.entry_delay_seconds * SECOND_NS
        entry = quote_reader.first_valid(
            plan,
            start_ns=eligibility_ns,
            end_ns=eligibility_ns + config.entry_wait_seconds * SECOND_NS,
        )
        if entry is None:
            skips.append(StrictPhysicalSkip(signal, "ENTRY_NO_FILL"))
            continue
        target_ns = entry.ts_recv_ns + config.holding_seconds * SECOND_NS
        previous = quote_reader.last_valid(plan)
        if previous is None or previous.ts_recv_ns <= entry.ts_recv_ns:
            skips.append(StrictPhysicalSkip(signal, "NO_POST_FILL_STREAM"))
            continue

        exit_quote: PhysicalQuote | None = None
        exit_kind: str | None = None
        if target_ns <= previous.ts_recv_ns:
            exit_quote = quote_reader.first_valid(plan, start_ns=target_ns)
            exit_kind = "HORIZON" if exit_quote is not None else None

        position = plan_index[plan.source_date] + 1
        while exit_quote is None and position < len(dataset.plans):
            following = dataset.plans[position]
            if (following.contract, following.instrument_id) != (
                plan.contract,
                plan.instrument_id,
            ):
                exit_quote = previous
                exit_kind = "BOUNDARY_CONTRACT"
                break
            first, last = quote_reader.first_and_last(following)
            if first is None or last is None:
                position += 1
                continue
            if first.ts_recv_ns - previous.ts_recv_ns > (
                config.maximum_stream_gap_seconds * SECOND_NS
            ):
                exit_quote = previous
                exit_kind = "BOUNDARY_96H"
                break
            if target_ns <= last.ts_recv_ns:
                exit_quote = quote_reader.first_valid(following, start_ns=target_ns)
                if exit_quote is not None:
                    exit_kind = "HORIZON"
                    break
            previous = last
            position += 1

        if exit_quote is None or exit_kind is None:
            skips.append(StrictPhysicalSkip(signal, "NO_EXIT"))
            continue
        direction = signal.direction
        entry_px = entry.ask_ticks if direction == 1 else entry.bid_ticks
        exit_px = exit_quote.bid_ticks if direction == 1 else exit_quote.ask_ticks
        trade = StrictPhysicalTrade(
            signal=signal,
            entry=entry,
            exit=exit_quote,
            entry_px=entry_px,
            exit_px=exit_px,
            exit_kind=exit_kind,
        )
        trades.append(trade)
        busy_until_ns = exit_quote.ts_recv_ns
    return StrictPhysicalReplay(tuple(trades), tuple(skips))


def strict_summary(
    trades: Iterable[StrictPhysicalTrade],
    *,
    debit_ticks_x2: int = 3,
) -> dict[str, object]:
    rows = tuple(trades)
    gross = sum(item.gross_ticks for item in rows)
    net_x2 = 2 * gross - debit_ticks_x2 * len(rows)
    mid_x2 = sum(item.directed_mid_x2 for item in rows)
    return {
        "completed_count": len(rows),
        "directed_mid_ticks": f"{mid_x2 / 2:.1f}",
        "gross_ticks": gross,
        "long_count": sum(item.signal.direction == 1 for item in rows),
        "net_at_1_5_ticks": f"{net_x2 / 2:.1f}",
        "short_count": sum(item.signal.direction == -1 for item in rows),
        "win_count": sum(2 * item.gross_ticks - debit_ticks_x2 > 0 for item in rows),
    }


def implementation_path() -> Path:
    return Path(__file__).resolve()
