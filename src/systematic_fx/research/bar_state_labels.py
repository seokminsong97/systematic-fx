"""Split-independent symmetric path labels for bar state-model research.

The target is observed from the next verified signal-bar open over twenty
active dates.  Its upper and lower barriers are symmetric around that open and
use the feature row's point-in-time ATR proxy.  One-second OHLC cannot reveal
the ordering when both barriers occur in the same second, so such observations
are censored instead of introducing a directional bias.  Direction-specific
``STOP_FIRST`` treatment belongs exclusively to economic replay.
"""

from __future__ import annotations

import bisect
import hashlib
import json
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from typing import Protocol

import numpy as np

from systematic_fx.research.bar_state_features import BarStateFeatureRow

ONE_SECOND_NS = 1_000_000_000
STATE_LABEL_SCHEMA = "systematic_fx.bar_state_label.v1"
LABEL_HORIZON_ACTIVE_DAYS = 20
MAX_ONE_SECOND_PATH_ROWS = 4_000_000


class BarStateLabelError(ValueError):
    """A label request or verified path is invalid."""


class IncompleteStateLabelHorizon(BarStateLabelError):
    """The immutable calendar has fewer than twenty future active dates."""


class StatePathLabel(StrEnum):
    """Three split-independent classes used by the multinomial model."""

    UP_FIRST = "UP_FIRST"
    DOWN_FIRST = "DOWN_FIRST"
    CENSORED = "CENSORED"


class StateCensorReason(StrEnum):
    NO_TOUCH = "NO_TOUCH"
    SIMULTANEOUS_AMBIGUOUS = "SIMULTANEOUS_AMBIGUOUS"
    CONTRACT_OR_QUALITY_BOUNDARY = "CONTRACT_OR_QUALITY_BOUNDARY"


class StateOneSecondBarLike(Protocol):
    timeframe_seconds: int
    segment_id: int
    outcome_span_id: int
    contract: str
    source_date: date
    start_ns: int
    end_ns: int
    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int


class StateEntryBarLike(Protocol):
    timeframe_seconds: int
    segment_id: int
    contract: str
    source_date: date
    start_ns: int
    end_ns: int
    first_trade_ns: int
    open_ticks: int


class StateSignalBarLike(Protocol):
    timeframe_seconds: int
    contract: str
    source_date: date
    start_ns: int
    end_ns: int


@dataclass(frozen=True, slots=True)
class StateVerifiedEntryBar:
    """Immediate observed successor bound to one verified outcome span.

    A maintenance gap may make ``start_ns`` later than the predecessor's end.
    The adapter proves "next" by constructing this value only from adjacent
    observed signal bars inside the same manifest outcome span.  The bound
    predecessor identity lets the label primitive reject a misjoined row.
    """

    timeframe_seconds: int
    segment_id: int
    contract: str
    source_date: date
    start_ns: int
    end_ns: int
    first_trade_ns: int
    open_ticks: int
    outcome_span_id: int
    predecessor_signal_start_ns: int
    predecessor_decision_ns: int
    predecessor_source_date: date

    def __post_init__(self) -> None:
        if self.timeframe_seconds not in (300, 1_800):
            raise BarStateLabelError("entry timeframe is outside the frozen signal bars")
        if not self.contract:
            raise BarStateLabelError("entry contract must be non-empty")
        if (
            isinstance(self.outcome_span_id, bool)
            or not isinstance(self.outcome_span_id, int)
            or self.outcome_span_id <= 0
        ):
            raise BarStateLabelError("entry outcome_span_id must be a positive integer")
        width = self.timeframe_seconds * ONE_SECOND_NS
        if (
            self.start_ns < 0
            or self.start_ns % ONE_SECOND_NS
            or self.end_ns != self.start_ns + width
            or self.end_ns % ONE_SECOND_NS
        ):
            raise BarStateLabelError("entry signal-bar interval is invalid")
        if not self.start_ns <= self.first_trade_ns < self.end_ns:
            raise BarStateLabelError("entry first trade is outside its signal bar")
        if (
            self.predecessor_signal_start_ns < 0
            or self.predecessor_decision_ns != self.predecessor_signal_start_ns + width
        ):
            raise BarStateLabelError("entry predecessor interval is invalid")
        if self.start_ns < self.predecessor_decision_ns:
            raise BarStateLabelError("entry overlaps its observed predecessor")
        if self.source_date < self.predecessor_source_date:
            raise BarStateLabelError("entry date precedes its observed predecessor")
        if self.open_ticks <= 0:
            raise BarStateLabelError("entry price must be positive")

    @classmethod
    def from_adjacent(
        cls,
        predecessor: StateSignalBarLike,
        entry: StateEntryBarLike,
        *,
        predecessor_outcome_span_id: int,
        entry_outcome_span_id: int,
    ) -> StateVerifiedEntryBar:
        """Bind two adapter-verified adjacent observed signal bars."""

        if predecessor.timeframe_seconds != entry.timeframe_seconds:
            raise BarStateLabelError("adjacent signal bars have different timeframes")
        if predecessor.contract != entry.contract:
            raise BarStateLabelError("adjacent signal bars cross a contract")
        if predecessor_outcome_span_id != entry_outcome_span_id:
            raise BarStateLabelError("adjacent signal bars cross an outcome span")
        return cls(
            timeframe_seconds=entry.timeframe_seconds,
            segment_id=entry.segment_id,
            contract=entry.contract,
            source_date=entry.source_date,
            start_ns=entry.start_ns,
            end_ns=entry.end_ns,
            first_trade_ns=entry.first_trade_ns,
            open_ticks=entry.open_ticks,
            outcome_span_id=entry_outcome_span_id,
            predecessor_signal_start_ns=predecessor.start_ns,
            predecessor_decision_ns=predecessor.end_ns,
            predecessor_source_date=predecessor.source_date,
        )


class StateLabelSource(Protocol):
    """Dependency-injection boundary around verified bar artifact loading."""

    def next_bar(self, feature: BarStateFeatureRow) -> StateVerifiedEntryBar | None: ...

    def one_second_path(
        self,
        feature: BarStateFeatureRow,
        entry_bar: StateVerifiedEntryBar,
    ) -> StateOneSecondPathIndex: ...


@dataclass(frozen=True, slots=True)
class BarStateLabel:
    """One exact target and enough lineage to audit its first-hit decision."""

    label: StatePathLabel
    timeframe_seconds: int
    segment_id: int
    contract: str
    signal_start_ns: int
    decision_ns: int
    entry_path_id: int
    entry_path_index: int
    entry_signal_bar_start_ns: int
    entry_signal_bar_end_ns: int
    entry_start_ns: int
    entry_price_ticks: int
    volatility_ticks: int
    upper_barrier_ticks: int
    lower_barrier_ticks: int
    upper_hit_path_index: int | None
    lower_hit_path_index: int | None
    terminal_path_index: int
    terminal_start_ns: int
    horizon_start_date: date
    horizon_terminal_date: date
    path_truncated_before_horizon: bool
    censor_reason: StateCensorReason | None

    def __post_init__(self) -> None:
        if self.label is StatePathLabel.CENSORED:
            if self.censor_reason is None:
                raise BarStateLabelError("censored label requires a censor reason")
        elif self.censor_reason is not None:
            raise BarStateLabelError("directional label cannot have a censor reason")
        if self.entry_path_id <= 0 or self.entry_path_index < 0:
            raise BarStateLabelError("label path identity is invalid")
        if self.terminal_path_index < self.entry_path_index:
            raise BarStateLabelError("label terminal precedes entry")
        if not (
            self.decision_ns
            <= self.entry_signal_bar_start_ns
            <= self.entry_start_ns
            < self.entry_signal_bar_end_ns
        ):
            raise BarStateLabelError("label entry signal-bar lineage is invalid")
        if self.upper_barrier_ticks != self.entry_price_ticks + self.volatility_ticks:
            raise BarStateLabelError("upper label barrier is not symmetric")
        if self.lower_barrier_ticks != self.entry_price_ticks - self.volatility_ticks:
            raise BarStateLabelError("lower label barrier is not symmetric")
        if self.horizon_terminal_date < self.horizon_start_date:
            raise BarStateLabelError("label active-date horizon is inverted")

    @property
    def simultaneous_ambiguous(self) -> bool:
        return self.censor_reason is StateCensorReason.SIMULTANEOUS_AMBIGUOUS

    def as_dict(self) -> dict[str, object]:
        return {
            "censor_reason": None if self.censor_reason is None else self.censor_reason.value,
            "contract": self.contract,
            "decision_ns": self.decision_ns,
            "entry_path_id": self.entry_path_id,
            "entry_path_index": self.entry_path_index,
            "entry_price_ticks": self.entry_price_ticks,
            "entry_signal_bar_end_ns": self.entry_signal_bar_end_ns,
            "entry_signal_bar_start_ns": self.entry_signal_bar_start_ns,
            "entry_start_ns": self.entry_start_ns,
            "horizon_start_date": self.horizon_start_date.isoformat(),
            "horizon_terminal_date": self.horizon_terminal_date.isoformat(),
            "label": self.label.value,
            "label_horizon_active_days": LABEL_HORIZON_ACTIVE_DAYS,
            "label_schema": STATE_LABEL_SCHEMA,
            "lower_barrier_ticks": self.lower_barrier_ticks,
            "lower_hit_path_index": self.lower_hit_path_index,
            "path_truncated_before_horizon": self.path_truncated_before_horizon,
            "segment_id": self.segment_id,
            "signal_start_ns": self.signal_start_ns,
            "terminal_path_index": self.terminal_path_index,
            "terminal_start_ns": self.terminal_start_ns,
            "timeframe_seconds": self.timeframe_seconds,
            "upper_barrier_ticks": self.upper_barrier_ticks,
            "upper_hit_path_index": self.upper_hit_path_index,
            "volatility_ticks": self.volatility_ticks,
        }

    @property
    def sha256(self) -> str:
        raw = (
            json.dumps(
                self.as_dict(),
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("ascii")
            + b"\n"
        )
        return hashlib.sha256(raw).hexdigest()


class StateOneSecondPathIndex:
    """First-hit range index over one immutable outcome span.

    Normal maintenance gaps may cross canonical context segment identifiers;
    ``path_id`` is therefore the externally verified outcome-span identity.
    Contract changes must be represented by a new instance.
    """

    def __init__(
        self,
        bars: Sequence[StateOneSecondBarLike],
        *,
        path_id: int,
    ) -> None:
        if isinstance(path_id, bool) or not isinstance(path_id, int) or path_id <= 0:
            raise BarStateLabelError("path_id must be a positive integer")
        if not isinstance(bars, Sequence) or not bars:
            raise BarStateLabelError("one-second path must be a non-empty sequence")
        if len(bars) > MAX_ONE_SECOND_PATH_ROWS:
            raise BarStateLabelError("one-second outcome span exceeds the memory preflight cap")
        contract = bars[0].contract
        if not contract:
            raise BarStateLabelError("path contract must be non-empty")
        highs: list[int] = []
        lows: list[int] = []
        starts: list[int] = []
        source_dates: list[date] = []
        previous_start: int | None = None
        previous_date: date | None = None
        for bar in bars:
            if bar.timeframe_seconds != 1:
                raise BarStateLabelError("label path must contain one-second bars")
            if bar.contract != contract:
                raise BarStateLabelError("label path crosses a contract")
            if bar.outcome_span_id != path_id:
                raise BarStateLabelError("one-second row differs from the verified outcome span")
            if bar.end_ns - bar.start_ns != ONE_SECOND_NS or bar.start_ns < 0:
                raise BarStateLabelError("one-second path interval is invalid")
            if previous_start is not None and bar.start_ns <= previous_start:
                raise BarStateLabelError("one-second path must be strictly ordered")
            if previous_date is not None and bar.source_date < previous_date:
                raise BarStateLabelError("one-second path source dates are unordered")
            if bar.low_ticks > min(bar.open_ticks, bar.close_ticks):
                raise BarStateLabelError("one-second low exceeds open or close")
            if bar.high_ticks < max(bar.open_ticks, bar.close_ticks):
                raise BarStateLabelError("one-second high is below open or close")
            if min(bar.low_ticks, bar.open_ticks, bar.close_ticks, bar.high_ticks) <= 0:
                raise BarStateLabelError("one-second prices must be positive")
            starts.append(bar.start_ns)
            source_dates.append(bar.source_date)
            highs.append(bar.high_ticks)
            lows.append(bar.low_ticks)
            previous_start = bar.start_ns
            previous_date = bar.source_date

        self.bars = tuple(bars)
        self.path_id = path_id
        self.contract = contract
        self._starts = tuple(starts)
        self._source_dates = tuple(source_dates)
        size = 1
        while size < len(bars):
            size <<= 1
        self._size = size
        self._max_tree = np.full(size * 2, np.iinfo(np.int64).min, dtype=np.int64)
        self._min_tree = np.full(size * 2, np.iinfo(np.int64).max, dtype=np.int64)
        self._max_tree[size : size + len(bars)] = np.asarray(highs, dtype=np.int64)
        self._min_tree[size : size + len(bars)] = np.asarray(lows, dtype=np.int64)
        for node in range(size - 1, 0, -1):
            self._max_tree[node] = max(self._max_tree[node * 2], self._max_tree[node * 2 + 1])
            self._min_tree[node] = min(self._min_tree[node * 2], self._min_tree[node * 2 + 1])

    def index_at_start(self, start_ns: int) -> int:
        index = bisect.bisect_left(self._starts, start_ns)
        if index >= len(self._starts) or self._starts[index] != start_ns:
            raise BarStateLabelError("entry one-second bar is absent from its verified path")
        return index

    def last_index_on_or_before_date(self, terminal_date: date) -> int | None:
        index = bisect.bisect_right(self._source_dates, terminal_date) - 1
        return index if index >= 0 else None

    def _first(
        self,
        *,
        start: int,
        end_exclusive: int,
        threshold: int,
        high: bool,
    ) -> int | None:
        if start < 0 or start >= end_exclusive or end_exclusive > len(self.bars):
            raise BarStateLabelError("first-hit range is invalid")
        tree = self._max_tree if high else self._min_tree

        def qualifies(node: int) -> bool:
            value = int(tree[node])
            return value >= threshold if high else value <= threshold

        def search(node: int, left: int, right: int) -> int | None:
            if right <= start or end_exclusive <= left or not qualifies(node):
                return None
            if right - left == 1:
                return left if left < len(self.bars) else None
            midpoint = (left + right) // 2
            result = search(node * 2, left, midpoint)
            return result if result is not None else search(node * 2 + 1, midpoint, right)

        return search(1, 0, self._size)

    def first_high_at_or_above(
        self,
        start: int,
        end_exclusive: int,
        threshold: int,
    ) -> int | None:
        return self._first(
            start=start,
            end_exclusive=end_exclusive,
            threshold=threshold,
            high=True,
        )

    def first_low_at_or_below(
        self,
        start: int,
        end_exclusive: int,
        threshold: int,
    ) -> int | None:
        return self._first(
            start=start,
            end_exclusive=end_exclusive,
            threshold=threshold,
            high=False,
        )


def _calendar_terminal_date(
    entry_date: date,
    active_dates: Sequence[date],
) -> date:
    if not active_dates or tuple(sorted(set(active_dates))) != tuple(active_dates):
        raise BarStateLabelError("active_dates must be sorted and unique")
    index = bisect.bisect_left(active_dates, entry_date)
    if index >= len(active_dates) or active_dates[index] != entry_date:
        raise BarStateLabelError("entry date is absent from the active-day calendar")
    terminal_index = index + LABEL_HORIZON_ACTIVE_DAYS - 1
    if terminal_index >= len(active_dates):
        raise IncompleteStateLabelHorizon("twenty active label dates are unavailable")
    return active_dates[terminal_index]


def label_bar_state_feature(
    feature: BarStateFeatureRow,
    *,
    entry_bar: StateVerifiedEntryBar,
    path: StateOneSecondPathIndex,
    active_dates: Sequence[date],
) -> BarStateLabel:
    """Resolve the unbiased 1x-volatility label for one feature row."""

    if not isinstance(feature, BarStateFeatureRow):
        raise BarStateLabelError("feature must be BarStateFeatureRow")
    if not isinstance(path, StateOneSecondPathIndex):
        raise BarStateLabelError("path must be StateOneSecondPathIndex")
    if not isinstance(entry_bar, StateVerifiedEntryBar):
        raise BarStateLabelError("entry bar lacks immediate-successor proof")
    if entry_bar.timeframe_seconds != feature.timeframe_seconds:
        raise BarStateLabelError("entry bar timeframe differs from feature row")
    if entry_bar.contract != feature.contract or path.contract != feature.contract:
        raise BarStateLabelError("label request crosses a contract")
    if entry_bar.outcome_span_id != path.path_id:
        raise BarStateLabelError("entry bar differs from its verified outcome span")
    if (
        entry_bar.predecessor_signal_start_ns != feature.signal_start_ns
        or entry_bar.predecessor_decision_ns != feature.decision_ns
        or entry_bar.predecessor_source_date != feature.source_date
    ):
        raise BarStateLabelError("entry successor proof differs from feature identity")
    entry_start_ns = entry_bar.first_trade_ns // ONE_SECOND_NS * ONE_SECOND_NS
    entry_index = path.index_at_start(entry_start_ns)
    entry_second = path.bars[entry_index]
    if (
        entry_second.open_ticks != entry_bar.open_ticks
        or not entry_bar.start_ns <= entry_second.start_ns < entry_bar.end_ns
    ):
        raise BarStateLabelError("entry signal bar and one-second path disagree")

    terminal_date = _calendar_terminal_date(entry_bar.source_date, active_dates)
    date_terminal_index = path.last_index_on_or_before_date(terminal_date)
    if date_terminal_index is None or date_terminal_index < entry_index:
        raise BarStateLabelError("verified path ends before label entry")
    terminal_index = min(date_terminal_index, len(path.bars) - 1)
    path_truncated = path.bars[terminal_index].source_date < terminal_date
    end_exclusive = terminal_index + 1
    upper = entry_bar.open_ticks + feature.volatility_ticks
    lower = entry_bar.open_ticks - feature.volatility_ticks
    upper_index = path.first_high_at_or_above(entry_index, end_exclusive, upper)
    lower_index = path.first_low_at_or_below(entry_index, end_exclusive, lower)
    if upper_index is not None and upper_index == lower_index:
        label = StatePathLabel.CENSORED
        reason = StateCensorReason.SIMULTANEOUS_AMBIGUOUS
    elif upper_index is not None and (lower_index is None or upper_index < lower_index):
        label = StatePathLabel.UP_FIRST
        reason = None
    elif lower_index is not None:
        label = StatePathLabel.DOWN_FIRST
        reason = None
    elif path_truncated:
        label = StatePathLabel.CENSORED
        reason = StateCensorReason.CONTRACT_OR_QUALITY_BOUNDARY
    else:
        label = StatePathLabel.CENSORED
        reason = StateCensorReason.NO_TOUCH
    terminal = path.bars[terminal_index]
    return BarStateLabel(
        label=label,
        timeframe_seconds=feature.timeframe_seconds,
        segment_id=feature.segment_id,
        contract=feature.contract,
        signal_start_ns=feature.signal_start_ns,
        decision_ns=feature.decision_ns,
        entry_path_id=path.path_id,
        entry_path_index=entry_index,
        entry_signal_bar_start_ns=entry_bar.start_ns,
        entry_signal_bar_end_ns=entry_bar.end_ns,
        entry_start_ns=entry_start_ns,
        entry_price_ticks=entry_bar.open_ticks,
        volatility_ticks=feature.volatility_ticks,
        upper_barrier_ticks=upper,
        lower_barrier_ticks=lower,
        upper_hit_path_index=upper_index,
        lower_hit_path_index=lower_index,
        terminal_path_index=terminal_index,
        terminal_start_ns=terminal.start_ns,
        horizon_start_date=entry_bar.source_date,
        horizon_terminal_date=terminal_date,
        path_truncated_before_horizon=path_truncated,
        censor_reason=reason,
    )


def iter_bar_state_labels(
    features: Sequence[BarStateFeatureRow],
    *,
    source: StateLabelSource,
    active_dates: Sequence[date],
) -> Iterator[BarStateLabel]:
    """Stream complete-horizon labels through a verified artifact adapter.

    Missing immediate observed successors within the same outcome span and
    incomplete right-edge horizons are ineligible, not silently censored.  The
    caller records their counts in dataset QC.
    """

    active_path: StateOneSecondPathIndex | None = None
    released_path_ids: set[int] = set()
    for feature in features:
        entry_bar = source.next_bar(feature)
        if entry_bar is None:
            continue
        try:
            requested_path = source.one_second_path(feature, entry_bar)
            if active_path is None or active_path.path_id != requested_path.path_id:
                if requested_path.path_id in released_path_ids:
                    raise BarStateLabelError("label stream re-enters a released outcome span")
                if active_path is not None:
                    released_path_ids.add(active_path.path_id)
                active_path = requested_path
            path = active_path
            yield label_bar_state_feature(
                feature,
                entry_bar=entry_bar,
                path=path,
                active_dates=active_dates,
            )
        except IncompleteStateLabelHorizon:
            continue


def labels_by_decision(
    labels: Sequence[BarStateLabel],
) -> Mapping[tuple[int, str, int], BarStateLabel]:
    """Return an exact unique lookup for model joins."""

    result: dict[tuple[int, str, int], BarStateLabel] = {}
    for label in labels:
        key = label.timeframe_seconds, label.contract, label.decision_ns
        if key in result:
            raise BarStateLabelError("duplicate label decision identity")
        result[key] = label
    return result


__all__ = [
    "LABEL_HORIZON_ACTIVE_DAYS",
    "MAX_ONE_SECOND_PATH_ROWS",
    "STATE_LABEL_SCHEMA",
    "BarStateLabel",
    "BarStateLabelError",
    "IncompleteStateLabelHorizon",
    "StateCensorReason",
    "StateLabelSource",
    "StateOneSecondPathIndex",
    "StatePathLabel",
    "StateVerifiedEntryBar",
    "iter_bar_state_labels",
    "label_bar_state_feature",
    "labels_by_decision",
]
