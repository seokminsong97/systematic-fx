"""Pure, outcome-blind multi-timeframe signal and fixed-horizon primitives.

The module deliberately has no filesystem, database, network, CLI, or clock
access.  Signal masks are frozen from completed 5m/30m/1h ``TradeBar`` views
before any one-second outcome payload is supplied.  The primary execution
contract is a three-hour fixed horizon; it has no TP/SL path dependency.
"""

from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict, deque
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from fractions import Fraction
from functools import lru_cache
from itertools import pairwise
from math import comb
from typing import Final, Literal

from scripts.ai_pattern_holdout_engine import BarWithOutcomeSpan, SignalMask
from systematic_fx.features.bars import ONE_SECOND_NS
from systematic_fx.research.hypotheses import canonical_sha256

ENGINE_SCHEMA: Final = "systematic_fx.ai_delayed_mtf_engine.v1"
CATALOG_SCHEMA: Final = "systematic_fx.ai_delayed_mtf_catalog.v1"
MASK_SCHEMA: Final = "systematic_fx.ai_delayed_mtf_masks.v1"
RESULT_SCHEMA: Final = "systematic_fx.ai_delayed_mtf_result.v1"

FIVE_MINUTES: Final = 300
HALF_HOUR: Final = 1_800
ONE_HOUR: Final = 3_600
PRIMARY_HORIZON_SECONDS: Final = 10_800
EMA_SCALE: Final = 1_000_000
MAX_DAILY_BRIDGE_SECONDS: Final = 96 * 3_600
ENTRY_ADVERSE_TICKS: Final = 2
TERMINAL_ADVERSE_TICKS: Final = 2
VARIABLE_DEBIT_TICKS: Final = 5
ALLOCATED_FIXED_COST_TICKS: Final = 5
TOTAL_FRICTION_TICKS: Final = 14
DEFAULT_MASTER_NULL_SEED: Final = "ai-delayed-mtf-v1"
MATCH_TIME_BUCKET_HOURS: Final = 4
VOLATILITY_HISTORY: Final = 20
VOLATILITY_WINDOW_5M: Final = 12
REGIME_WINDOW_5M: Final = 12

CANDIDATE_CATALOG_COUNT: Final = 100
MACD_PARAMETER_SETS: Final = ((8, 21, 5), (12, 26, 9))
EMA_PARAMETER_SETS: Final = ((8, 21), (12, 26))

Direction = Literal["LONG", "SHORT"]
Family = Literal[
    "DELAYED_MACD",
    "COMPRESSION_BREAKOUT",
    "TREND_PULLBACK_CONTINUATION",
    "RANGE_REGIME_MEAN_REVERSION",
]
MaskRole = Literal["REAL", "CIRCULAR_SHIFT", "MATCHED_RANDOM"]


class DelayedMtfEngineError(ValueError):
    """A candidate, bar view, frozen mask, or outcome violates the contract."""


def _require_int(value: object, *, label: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DelayedMtfEngineError(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        raise DelayedMtfEngineError(f"{label} must be >= {minimum}")
    return value


def _require_nonempty(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise DelayedMtfEngineError(f"{label} must be a non-empty string")
    return value


def _require_sha(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise DelayedMtfEngineError(f"{label} must be a lowercase SHA-256")
    return value


def _round_half_even(numerator: int, denominator: int) -> int:
    """Return exact signed integer division with bankers' rounding."""

    if denominator <= 0:
        raise DelayedMtfEngineError("rounding denominator must be positive")
    sign = -1 if numerator < 0 else 1
    quotient, remainder = divmod(abs(numerator), denominator)
    twice = remainder * 2
    if twice > denominator or (twice == denominator and quotient % 2):
        quotient += 1
    return sign * quotient


def _utc_hour(start_ns: int) -> int:
    return datetime.fromtimestamp(start_ns // ONE_SECOND_NS, tz=UTC).hour


@dataclass(frozen=True, slots=True)
class SymbolicCandidate:
    """One canonical member of the globally multiplicity-controlled family."""

    selection_rank: int
    candidate_id: str
    family: Family
    direction: Direction
    parameters: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        _require_int(self.selection_rank, label="selection_rank", minimum=1)
        _require_sha(self.candidate_id, label="candidate_id")
        if self.family not in (
            "DELAYED_MACD",
            "COMPRESSION_BREAKOUT",
            "TREND_PULLBACK_CONTINUATION",
            "RANGE_REGIME_MEAN_REVERSION",
        ):
            raise DelayedMtfEngineError("unknown candidate family")
        if self.direction not in ("LONG", "SHORT"):
            raise DelayedMtfEngineError("candidate direction must be LONG or SHORT")
        if not self.parameters or tuple(sorted(self.parameters)) != self.parameters:
            raise DelayedMtfEngineError("candidate parameters must be sorted and non-empty")
        if len({key for key, _ in self.parameters}) != len(self.parameters):
            raise DelayedMtfEngineError("candidate parameter names must be unique")
        for key, value in self.parameters:
            _require_nonempty(key, label="candidate parameter name")
            _require_int(value, label=f"candidate parameter {key}")
        if canonical_sha256(self.definition_dict()) != self.candidate_id:
            raise DelayedMtfEngineError("candidate id differs from its canonical definition")

    def parameter(self, name: str) -> int:
        try:
            return dict(self.parameters)[name]
        except KeyError as error:
            raise DelayedMtfEngineError(f"candidate lacks parameter {name!r}") from error

    def definition_dict(self) -> dict[str, object]:
        return {
            "direction": self.direction,
            "family": self.family,
            "parameters": {key: value for key, value in self.parameters},
            "schema": CATALOG_SCHEMA,
        }

    def as_dict(self) -> dict[str, object]:
        return {
            **self.definition_dict(),
            "candidate_id": self.candidate_id,
            "selection_rank": self.selection_rank,
        }


@dataclass(frozen=True, slots=True)
class CandidateCatalog:
    candidates: tuple[SymbolicCandidate, ...]
    catalog_sha256: str

    def __post_init__(self) -> None:
        if len(self.candidates) != CANDIDATE_CATALOG_COUNT:
            raise DelayedMtfEngineError("candidate catalog must contain exactly 100 rows")
        if tuple(item.selection_rank for item in self.candidates) != tuple(
            range(1, len(self.candidates) + 1)
        ):
            raise DelayedMtfEngineError("candidate ranks must be canonical and contiguous")
        if len(set(self.candidate_ids)) != len(self.candidates):
            raise DelayedMtfEngineError("candidate ids must be unique")
        _require_sha(self.catalog_sha256, label="catalog_sha256")
        if canonical_sha256([item.as_dict() for item in self.candidates]) != self.catalog_sha256:
            raise DelayedMtfEngineError("catalog hash differs from its candidates")

    @property
    def candidate_ids(self) -> tuple[str, ...]:
        return tuple(item.candidate_id for item in self.candidates)

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_count": len(self.candidates),
            "candidate_ids": list(self.candidate_ids),
            "candidates": [item.as_dict() for item in self.candidates],
            "catalog_sha256": self.catalog_sha256,
            "schema": CATALOG_SCHEMA,
        }


def _candidate(
    rank: int, family: Family, direction: Direction, **parameters: int
) -> SymbolicCandidate:
    pairs = tuple(sorted(parameters.items()))
    definition = {
        "direction": direction,
        "family": family,
        "parameters": dict(pairs),
        "schema": CATALOG_SCHEMA,
    }
    return SymbolicCandidate(rank, canonical_sha256(definition), family, direction, pairs)


@lru_cache(maxsize=1)
def build_delayed_mtf_candidate_catalog() -> CandidateCatalog:
    """Return the frozen 100-member symbolic candidate catalog."""

    rows: list[SymbolicCandidate] = []

    def add(family: Family, direction: Direction, **parameters: int) -> None:
        rows.append(_candidate(len(rows) + 1, family, direction, **parameters))

    for timeframe in (FIVE_MINUTES, HALF_HOUR, ONE_HOUR):
        for fast, slow, signal in MACD_PARAMETER_SETS:
            for extra_delay_hours in (0, 1, 2):
                for direction in ("LONG", "SHORT"):
                    add(
                        "DELAYED_MACD",
                        direction,
                        extra_delay_hours=extra_delay_hours,
                        fast_period=fast,
                        signal_period=signal,
                        slow_period=slow,
                        trigger_timeframe_seconds=timeframe,
                    )
    for context, trigger in ((HALF_HOUR, FIVE_MINUTES), (ONE_HOUR, HALF_HOUR)):
        for compression_window in (6, 12):
            for breakout_window in (3, 6, 12):
                for direction in ("LONG", "SHORT"):
                    add(
                        "COMPRESSION_BREAKOUT",
                        direction,
                        breakout_window=breakout_window,
                        compression_window=compression_window,
                        context_timeframe_seconds=context,
                        trigger_timeframe_seconds=trigger,
                    )
    for context, trigger in ((HALF_HOUR, FIVE_MINUTES), (ONE_HOUR, HALF_HOUR)):
        for fast, slow in EMA_PARAMETER_SETS:
            for pullback_bars in (2, 3, 4):
                for direction in ("LONG", "SHORT"):
                    add(
                        "TREND_PULLBACK_CONTINUATION",
                        direction,
                        context_timeframe_seconds=context,
                        fast_period=fast,
                        pullback_bars=pullback_bars,
                        slow_period=slow,
                        trigger_timeframe_seconds=trigger,
                    )
    for trigger in (FIVE_MINUTES, HALF_HOUR):
        for lookback in (12, 24):
            for numerator, denominator in ((1, 3), (1, 2)):
                for direction in ("LONG", "SHORT"):
                    add(
                        "RANGE_REGIME_MEAN_REVERSION",
                        direction,
                        efficiency_denominator=denominator,
                        efficiency_numerator=numerator,
                        lookback=lookback,
                        trigger_timeframe_seconds=trigger,
                    )
    catalog = CandidateCatalog(tuple(rows), canonical_sha256([item.as_dict() for item in rows]))
    family_counts: dict[str, int] = defaultdict(int)
    for item in rows:
        family_counts[item.family] += 1
    if dict(family_counts) != {
        "DELAYED_MACD": 36,
        "COMPRESSION_BREAKOUT": 24,
        "TREND_PULLBACK_CONTINUATION": 24,
        "RANGE_REGIME_MEAN_REVERSION": 16,
    }:
        raise AssertionError("internal delayed-MTF family dimensions changed")
    return catalog


CANDIDATE_CATALOG_SHA256: Final = build_delayed_mtf_candidate_catalog().catalog_sha256


def delayed_mtf_engine_contract() -> dict[str, object]:
    """Return the complete calculation identity bound by campaign config."""

    return {
        "candidate_catalog_count": CANDIDATE_CATALOG_COUNT,
        "candidate_catalog_sha256": CANDIDATE_CATALOG_SHA256,
        "catalog_family_counts": {
            "COMPRESSION_BREAKOUT": 24,
            "DELAYED_MACD": 36,
            "RANGE_REGIME_MEAN_REVERSION": 16,
            "TREND_PULLBACK_CONTINUATION": 24,
        },
        "continuity": {
            "cross_date_bridge": (
                "adjacent allowlisted active source dates, same contract/outcome span, "
                "wall gap <=96h"
            ),
            "cross_date_max_seconds": MAX_DAILY_BRIDGE_SECONDS,
            "intra_date": "exact timeframe adjacency and same contract/span/segment",
            "stage_start": "reset; no pre-stage warmup",
        },
        "ema": {
            "rounding": "ROUND_HALF_EVEN",
            "scale_microticks": EMA_SCALE,
            "seed": "period-close SMA",
            "update": "round_half_even(((p-1)*ema+2*x)/(p+1))",
        },
        "execution": {
            "allocated_fixed_cost_ticks": ALLOCATED_FIXED_COST_TICKS,
            "entry_adverse_ticks": ENTRY_ADVERSE_TICKS,
            "horizon_seconds": PRIMARY_HORIZON_SECONDS,
            "scenario_id": "DELAYED_FIXED_HORIZON_3H",
            "occupancy": (
                "one position per mask; skip entry strictly before prior exit; equality "
                "allowed; censored signals do not occupy"
            ),
            "terminal_adverse_ticks": TERMINAL_ADVERSE_TICKS,
            "total_friction_ticks": TOTAL_FRICTION_TICKS,
            "variable_debit_ticks": VARIABLE_DEBIT_TICKS,
        },
        "formulas": {
            "compression": (
                "disjoint completed context ranges: 2*sum(recent_n)<=sum(previous_n); "
                "trigger close beyond prior-b trigger high/low; persist beyond frozen level"
            ),
            "macd": (
                "closed-bar histogram cross; target ceil(end,1h)+extra delay; strict-sign "
                "persistence"
            ),
            "pullback": (
                "HTF fast/slow EMA direction+slope; m preceding LTF closes pullback-side "
                "then current trend-side cross; trend-side persistence"
            ),
            "range": (
                "pre-trigger efficiency ratio <=1/3 or1/2; false channel break and close "
                "back inside; inside persistence"
            ),
        },
        "nulls": {
            "circular": (
                "date/contract/span/segment aligned-hour orbits; invariant group orbits "
                "retained; aggregate mask must preserve cardinality and be nonidentity"
            ),
            "master_seed": DEFAULT_MASTER_NULL_SEED,
            "matched_common": (
                "exclude all real positions; deterministic without replacement; preserve "
                "cardinality/nonidentity; persist pair and relaxation-level histogram"
            ),
            "matched_relaxation_levels": [
                "L0 same date/contract/span/segment + exact vol/regime + same 4h bucket",
                "L1 L0 with adjacent 4h bucket",
                "L2 same date/contract/span/segment + exact vol/regime, drop bucket",
                "L3 same date/contract/span/segment + same regime",
                "L4 same date/contract/span/segment, any complete-case opportunity",
                "L5 same date/contract/span, drop segment + exact vol/regime",
                "L6 same date/contract/span, any strata ranked by cyclic 4h distance",
                "L7 same contract/span across dates + exact strata + same/adjacent 4h",
                (
                    "L8 same contract/span any complete-case ranked by cyclic 4h, regime, "
                    "vol ordinal, date distance, seed hash"
                ),
            ],
            "missing_strata": [
                "MISSING_BASE_VOLATILITY_HISTORY",
                "MISSING_PRIOR_20_HISTORY",
                "MISSING_REGIME_HISTORY",
            ],
            "opportunity_lattice": (
                "candidate-independent exact 3h structural coverage and "
                "date/contract/span/segment group size>=2; every opportunity has an "
                "explicit causal stratum"
            ),
        },
        "schema": ENGINE_SCHEMA,
        "neutral_imports": {
            "allowed": ["BarWithOutcomeSpan", "SignalMask"],
            "old_execution_or_signal_functions_called": False,
            "source_module": "scripts.ai_pattern_holdout_engine",
        },
    }


def _validated_bars(
    values: Sequence[BarWithOutcomeSpan], *, timeframe_seconds: int, label: str
) -> tuple[BarWithOutcomeSpan, ...]:
    if not isinstance(values, Sequence) or not values:
        raise DelayedMtfEngineError(f"{label} must be a non-empty sequence")
    bars = tuple(values)
    prior: tuple[int, str, int, int] | None = None
    seen: set[tuple[int, str, int, int]] = set()
    for wrapped in bars:
        if not isinstance(wrapped, BarWithOutcomeSpan):
            raise DelayedMtfEngineError(f"{label} must contain BarWithOutcomeSpan values")
        bar = wrapped.bar
        if bar.timeframe_seconds != timeframe_seconds:
            raise DelayedMtfEngineError(f"{label} contains the wrong timeframe")
        identity = bar.start_ns, bar.contract, wrapped.outcome_span_id, bar.segment_id
        if identity in seen:
            raise DelayedMtfEngineError(f"{label} contains a duplicate bar identity")
        seen.add(identity)
        if prior is not None and identity <= prior:
            raise DelayedMtfEngineError(f"{label} must use canonical chronological order")
        prior = identity
    return bars


def _validated_dates(values: Iterable[date]) -> tuple[date, ...]:
    dates = tuple(sorted(set(values)))
    if not dates or any(isinstance(item, datetime) or not isinstance(item, date) for item in dates):
        raise DelayedMtfEngineError("decision_dates must contain non-empty date values")
    return dates


def _lineage(item: BarWithOutcomeSpan) -> tuple[str, int, int]:
    return item.bar.contract, item.outcome_span_id, item.bar.segment_id


def _indicator_continues(
    previous: BarWithOutcomeSpan,
    current: BarWithOutcomeSpan,
    *,
    date_rank: Mapping[date, int],
) -> bool:
    left = previous.bar
    right = current.bar
    if left.timeframe_seconds != right.timeframe_seconds:
        return False
    if left.source_date == right.source_date:
        return left.end_ns == right.start_ns and _lineage(previous) == _lineage(current)
    gap_ns = right.start_ns - left.end_ns
    return (
        date_rank.get(right.source_date, -2) == date_rank.get(left.source_date, -1) + 1
        and left.contract == right.contract
        and previous.outcome_span_id == current.outcome_span_id
        and 0 <= gap_ns <= MAX_DAILY_BRIDGE_SECONDS * ONE_SECOND_NS
    )


@dataclass(frozen=True, slots=True)
class _Series:
    bars: tuple[BarWithOutcomeSpan, ...]
    continues: tuple[bool, ...]
    end_ns: tuple[int, ...]
    decision_dates: tuple[date, ...]

    def __post_init__(self) -> None:
        if (
            len(self.bars) != len(self.continues)
            or len(self.bars) != len(self.end_ns)
            or not self.bars
        ):
            raise DelayedMtfEngineError("internal series shape is invalid")
        if self.continues[0]:
            raise DelayedMtfEngineError("the first series row cannot continue a predecessor")


def _stage_series(bars: Sequence[BarWithOutcomeSpan], decision_dates: tuple[date, ...]) -> _Series:
    allowed = frozenset(decision_dates)
    selected = tuple(item for item in bars if item.bar.source_date in allowed)
    if not selected:
        raise DelayedMtfEngineError("a timeframe has no bars on the decision dates")
    rank = {item: index for index, item in enumerate(decision_dates)}
    continues = [False]
    for previous, current in pairwise(selected):
        continues.append(_indicator_continues(previous, current, date_rank=rank))
    return _Series(
        selected,
        tuple(continues),
        tuple(item.bar.end_ns for item in selected),
        decision_dates,
    )


class _FeatureCache:
    """One stage-local shared fixed-point EMA/MACD cache."""

    def __init__(self, series_by_timeframe: Mapping[int, _Series]) -> None:
        self.series_by_timeframe = dict(series_by_timeframe)
        self._ema: dict[tuple[int, int], tuple[int | None, ...]] = {}
        self._macd: dict[tuple[int, int, int, int], tuple[int | None, ...]] = {}

    def ema(self, timeframe: int, period: int) -> tuple[int | None, ...]:
        key = timeframe, period
        cached = self._ema.get(key)
        if cached is not None:
            return cached
        series = self.series_by_timeframe[timeframe]
        seed: deque[int] = deque(maxlen=period)
        value: int | None = None
        output: list[int | None] = []
        for index, wrapped in enumerate(series.bars):
            if not series.continues[index]:
                seed.clear()
                value = None
            close = wrapped.bar.close_ticks * EMA_SCALE
            if value is None:
                seed.append(close)
                if len(seed) == period:
                    value = _round_half_even(sum(seed), period)
            else:
                value = _round_half_even((period - 1) * value + 2 * close, period + 1)
            output.append(value)
        result = tuple(output)
        self._ema[key] = result
        return result

    def macd_histogram(
        self, timeframe: int, fast: int, slow: int, signal: int
    ) -> tuple[int | None, ...]:
        key = timeframe, fast, slow, signal
        cached = self._macd.get(key)
        if cached is not None:
            return cached
        series = self.series_by_timeframe[timeframe]
        fast_values = self.ema(timeframe, fast)
        slow_values = self.ema(timeframe, slow)
        signal_seed: deque[int] = deque(maxlen=signal)
        signal_value: int | None = None
        output: list[int | None] = []
        for index, (fast_value, slow_value) in enumerate(
            zip(fast_values, slow_values, strict=True)
        ):
            if not series.continues[index]:
                signal_seed.clear()
                signal_value = None
            if fast_value is None or slow_value is None:
                output.append(None)
                continue
            macd = fast_value - slow_value
            if signal_value is None:
                signal_seed.append(macd)
                if len(signal_seed) == signal:
                    signal_value = _round_half_even(sum(signal_seed), signal)
            else:
                signal_value = _round_half_even((signal - 1) * signal_value + 2 * macd, signal + 1)
            output.append(None if signal_value is None else macd - signal_value)
        result = tuple(output)
        self._macd[key] = result
        return result


def _ceil_hour_ns(value: int) -> int:
    width = ONE_HOUR * ONE_SECOND_NS
    return ((value + width - 1) // width) * width


def _execution_eligible_positions(
    bars: tuple[BarWithOutcomeSpan, ...],
    *,
    decision_dates: tuple[date, ...],
    allowed_stage_tail_end_ns: int,
) -> tuple[bool, ...]:
    dates = frozenset(decision_dates)
    tail = _require_int(allowed_stage_tail_end_ns, label="allowed_stage_tail_end_ns", minimum=1)
    required_bars = PRIMARY_HORIZON_SECONDS // FIVE_MINUTES
    eligible = [False] * len(bars)
    for index, anchor in enumerate(bars):
        target = anchor.bar.end_ns
        if (
            anchor.bar.source_date not in dates
            or target % (ONE_HOUR * ONE_SECOND_NS)
            or target + PRIMARY_HORIZON_SECONDS * ONE_SECOND_NS > tail
            or index + required_bars >= len(bars)
        ):
            continue
        lineage = _lineage(anchor)
        expected_start = target
        valid = True
        for offset in range(1, required_bars + 1):
            future = bars[index + offset]
            if future.bar.start_ns != expected_start or _lineage(future) != lineage:
                valid = False
                break
            expected_start = future.bar.end_ns
        eligible[index] = valid and expected_start == (
            target + PRIMARY_HORIZON_SECONDS * ONE_SECOND_NS
        )
    # Controls preserve date/contract/span/segment.  A singleton group cannot
    # support a nonidentity circular control, so it is excluded from the
    # candidate-independent opportunity lattice before any signal is applied.
    group_counts: dict[tuple[date, str, int, int], int] = defaultdict(int)
    for wrapped, allowed in zip(bars, eligible, strict=True):
        if allowed:
            group_counts[_opportunity_group(wrapped)] += 1
    return tuple(
        allowed and group_counts[_opportunity_group(wrapped)] >= 2
        for wrapped, allowed in zip(bars, eligible, strict=True)
    )


def _compatible_join(
    context: BarWithOutcomeSpan,
    trigger: BarWithOutcomeSpan,
    decision_dates: tuple[date, ...],
) -> bool:
    if (
        context.bar.contract != trigger.bar.contract
        or context.outcome_span_id != trigger.outcome_span_id
    ):
        return False
    if context.bar.source_date == trigger.bar.source_date:
        return context.bar.segment_id == trigger.bar.segment_id
    date_rank = {item: index for index, item in enumerate(decision_dates)}
    gap_ns = trigger.bar.start_ns - context.bar.end_ns
    return (
        date_rank.get(trigger.bar.source_date, -2) == date_rank.get(context.bar.source_date, -1) + 1
        and 0 <= gap_ns <= MAX_DAILY_BRIDGE_SECONDS * ONE_SECOND_NS
    )


def _continuous_start(series: _Series) -> tuple[int, ...]:
    output: list[int] = []
    start = 0
    for index, continuation in enumerate(series.continues):
        if not continuation:
            start = index
        output.append(start)
    return tuple(output)


def _context_index(
    context: _Series,
    trigger: BarWithOutcomeSpan,
    *,
    no_later_than_ns: int,
) -> int | None:
    index = bisect_right(context.end_ns, no_later_than_ns) - 1
    if index < 0:
        return None
    # Use the latest completed context observation only.  Scanning backward
    # across an intervening segment/contract would silently resurrect stale
    # state after a boundary.
    return index if _compatible_join(context.bars[index], trigger, context.decision_dates) else None


def _anchor_map(
    fives: tuple[BarWithOutcomeSpan, ...],
) -> dict[tuple[int, str, int, int], int]:
    output: dict[tuple[int, str, int, int], int] = {}
    for index, item in enumerate(fives):
        key = item.bar.end_ns, item.bar.contract, item.outcome_span_id, item.bar.segment_id
        if key in output:
            raise DelayedMtfEngineError("five-minute anchors are not unique")
        output[key] = index
    return output


def _raw_macd_mask(
    candidate: SymbolicCandidate,
    series: _Series,
    cache: _FeatureCache,
    anchor_by_key: Mapping[tuple[int, str, int, int], int],
    value_count: int,
) -> tuple[bool, ...]:
    timeframe = candidate.parameter("trigger_timeframe_seconds")
    histogram = cache.macd_histogram(
        timeframe,
        candidate.parameter("fast_period"),
        candidate.parameter("slow_period"),
        candidate.parameter("signal_period"),
    )
    delay = candidate.parameter("extra_delay_hours") * ONE_HOUR * ONE_SECOND_NS
    selected = [False] * value_count
    previous_histogram: int | None = None
    pending_target: int | None = None
    for index, (wrapped, value) in enumerate(zip(series.bars, histogram, strict=True)):
        if not series.continues[index]:
            previous_histogram = None
            pending_target = None
        direction_alive = value is not None and (
            value > 0 if candidate.direction == "LONG" else value < 0
        )
        if pending_target is not None and (
            not direction_alive or wrapped.bar.end_ns > pending_target
        ):
            pending_target = None
        desired_cross = (
            value is not None
            and previous_histogram is not None
            and (
                (candidate.direction == "LONG" and previous_histogram <= 0 < value)
                or (candidate.direction == "SHORT" and previous_histogram >= 0 > value)
            )
        )
        if desired_cross:
            pending_target = _ceil_hour_ns(wrapped.bar.end_ns) + delay
        if pending_target == wrapped.bar.end_ns and direction_alive:
            key = (
                wrapped.bar.end_ns,
                wrapped.bar.contract,
                wrapped.outcome_span_id,
                wrapped.bar.segment_id,
            )
            anchor = anchor_by_key.get(key)
            if anchor is not None:
                selected[anchor] = True
            pending_target = None
        previous_histogram = value
    return tuple(selected)


def _compression_event(
    candidate: SymbolicCandidate,
    direction: Direction,
    trigger: _Series,
    context: _Series,
    trigger_index: int,
    trigger_start: tuple[int, ...],
    context_start: tuple[int, ...],
) -> tuple[int, int] | None:
    window = candidate.parameter("compression_window")
    breakout = candidate.parameter("breakout_window")
    current = trigger.bars[trigger_index]
    if trigger_index - breakout < trigger_start[trigger_index]:
        return None
    context_index = _context_index(context, current, no_later_than_ns=current.bar.start_ns)
    if context_index is None or context_index - 2 * window + 1 < context_start[context_index]:
        return None
    previous_ranges = [
        item.bar.high_ticks - item.bar.low_ticks
        for item in context.bars[context_index - 2 * window + 1 : context_index - window + 1]
    ]
    recent_ranges = [
        item.bar.high_ticks - item.bar.low_ticks
        for item in context.bars[context_index - window + 1 : context_index + 1]
    ]
    if 2 * sum(recent_ranges) > sum(previous_ranges):
        return None
    history = trigger.bars[trigger_index - breakout : trigger_index]
    low = min(item.bar.low_ticks for item in history)
    high = max(item.bar.high_ticks for item in history)
    if direction == "LONG" and current.bar.close_ticks > high:
        return high, low
    if direction == "SHORT" and current.bar.close_ticks < low:
        return low, high
    return None


def _pullback_context(
    candidate: SymbolicCandidate,
    direction: Direction,
    current: BarWithOutcomeSpan,
    context: _Series,
    context_start: tuple[int, ...],
    cache: _FeatureCache,
) -> tuple[bool, int] | None:
    timeframe = candidate.parameter("context_timeframe_seconds")
    fast_values = cache.ema(timeframe, candidate.parameter("fast_period"))
    slow_values = cache.ema(timeframe, candidate.parameter("slow_period"))
    index = _context_index(context, current, no_later_than_ns=current.bar.end_ns)
    if index is None or index - 1 < context_start[index]:
        return None
    fast = fast_values[index]
    slow = slow_values[index]
    prior_fast = fast_values[index - 1]
    if fast is None or slow is None or prior_fast is None:
        return None
    trend = (
        fast > slow and fast > prior_fast
        if direction == "LONG"
        else fast < slow and fast < prior_fast
    )
    return trend, fast


def _pullback_event(
    candidate: SymbolicCandidate,
    direction: Direction,
    trigger: _Series,
    context: _Series,
    trigger_index: int,
    trigger_start: tuple[int, ...],
    context_start: tuple[int, ...],
    cache: _FeatureCache,
) -> tuple[int, ...] | None:
    count = candidate.parameter("pullback_bars")
    if trigger_index - count < trigger_start[trigger_index]:
        return None
    current = trigger.bars[trigger_index]
    state = _pullback_context(candidate, direction, current, context, context_start, cache)
    if state is None:
        return None
    trend, fast = state
    if not trend:
        return None
    previous = trigger.bars[trigger_index - count : trigger_index]
    if direction == "LONG":
        event = all(item.bar.close_ticks * EMA_SCALE <= fast for item in previous) and (
            current.bar.close_ticks * EMA_SCALE > fast
        )
    else:
        event = all(item.bar.close_ticks * EMA_SCALE >= fast for item in previous) and (
            current.bar.close_ticks * EMA_SCALE < fast
        )
    return () if event else None


def _range_event(
    candidate: SymbolicCandidate,
    direction: Direction,
    trigger: _Series,
    trigger_index: int,
    trigger_start: tuple[int, ...],
) -> tuple[int, int] | None:
    lookback = candidate.parameter("lookback")
    if trigger_index - lookback - 1 < trigger_start[trigger_index]:
        return None
    history = trigger.bars[trigger_index - lookback - 1 : trigger_index]
    closes = [item.bar.close_ticks for item in history]
    travelled = sum(abs(right - left) for left, right in pairwise(closes))
    displacement = abs(closes[-1] - closes[0])
    if displacement * candidate.parameter(
        "efficiency_denominator"
    ) > travelled * candidate.parameter("efficiency_numerator"):
        return None
    channel = history[1:]
    low = min(item.bar.low_ticks for item in channel)
    high = max(item.bar.high_ticks for item in channel)
    current = trigger.bars[trigger_index].bar
    if direction == "LONG" and current.low_ticks < low < current.close_ticks:
        return low, high
    if direction == "SHORT" and current.high_ticks > high > current.close_ticks:
        return high, low
    return None


def _generic_persists(
    candidate: SymbolicCandidate,
    payload: tuple[int, ...],
    trigger: _Series,
    context: _Series | None,
    trigger_index: int,
    context_start: tuple[int, ...] | None,
    cache: _FeatureCache,
) -> bool:
    close = trigger.bars[trigger_index].bar.close_ticks
    if candidate.family == "COMPRESSION_BREAKOUT":
        return close > payload[0] if candidate.direction == "LONG" else close < payload[0]
    if candidate.family == "RANGE_REGIME_MEAN_REVERSION":
        low, high = sorted(payload)
        return low < close < high
    if candidate.family == "TREND_PULLBACK_CONTINUATION":
        if context is None or context_start is None:
            return False
        state = _pullback_context(
            candidate,
            candidate.direction,
            trigger.bars[trigger_index],
            context,
            context_start,
            cache,
        )
        if state is None:
            return False
        trend, fast = state
        return trend and (
            close * EMA_SCALE > fast if candidate.direction == "LONG" else close * EMA_SCALE < fast
        )
    raise AssertionError("unexpected generic candidate family")


def _generic_event(
    candidate: SymbolicCandidate,
    direction: Direction,
    trigger: _Series,
    context: _Series | None,
    trigger_index: int,
    trigger_start: tuple[int, ...],
    context_start: tuple[int, ...] | None,
    cache: _FeatureCache,
) -> tuple[int, ...] | None:
    if candidate.family == "COMPRESSION_BREAKOUT":
        if context is None or context_start is None:
            return None
        return _compression_event(
            candidate,
            direction,
            trigger,
            context,
            trigger_index,
            trigger_start,
            context_start,
        )
    if candidate.family == "TREND_PULLBACK_CONTINUATION":
        if context is None or context_start is None:
            return None
        return _pullback_event(
            candidate,
            direction,
            trigger,
            context,
            trigger_index,
            trigger_start,
            context_start,
            cache,
        )
    if candidate.family == "RANGE_REGIME_MEAN_REVERSION":
        return _range_event(candidate, direction, trigger, trigger_index, trigger_start)
    raise AssertionError("unexpected generic candidate family")


def _raw_generic_mask(
    candidate: SymbolicCandidate,
    trigger: _Series,
    context: _Series | None,
    cache: _FeatureCache,
    anchor_by_key: Mapping[tuple[int, str, int, int], int],
    value_count: int,
) -> tuple[bool, ...]:
    selected = [False] * value_count
    trigger_start = _continuous_start(trigger)
    context_start = None if context is None else _continuous_start(context)
    pending_target: int | None = None
    pending_payload: tuple[int, ...] | None = None
    for index, wrapped in enumerate(trigger.bars):
        if not trigger.continues[index]:
            pending_target = None
            pending_payload = None
        if pending_target is not None and (
            wrapped.bar.end_ns > pending_target
            or not _generic_persists(
                candidate,
                pending_payload or (),
                trigger,
                context,
                index,
                context_start,
                cache,
            )
        ):
            pending_target = None
            pending_payload = None
        same = _generic_event(
            candidate,
            candidate.direction,
            trigger,
            context,
            index,
            trigger_start,
            context_start,
            cache,
        )
        opposite_direction: Direction = "SHORT" if candidate.direction == "LONG" else "LONG"
        opposite = _generic_event(
            candidate,
            opposite_direction,
            trigger,
            context,
            index,
            trigger_start,
            context_start,
            cache,
        )
        if opposite is not None:
            pending_target = None
            pending_payload = None
        if same is not None:
            # A newer same-direction event replaces the frozen level and event identity.
            pending_target = _ceil_hour_ns(wrapped.bar.end_ns)
            pending_payload = same
        if pending_target == wrapped.bar.end_ns and pending_payload is not None:
            key = (
                wrapped.bar.end_ns,
                wrapped.bar.contract,
                wrapped.outcome_span_id,
                wrapped.bar.segment_id,
            )
            anchor = anchor_by_key.get(key)
            if anchor is not None:
                selected[anchor] = True
            pending_target = None
            pending_payload = None
    return tuple(selected)


def _causal_match_strata(
    five_series: _Series,
    original_index: Mapping[tuple[int, str, int, int], int],
    eligible: tuple[bool, ...],
) -> dict[int, tuple[str, str]]:
    """Build candidate-independent, pre-entry volatility/regime strata."""

    starts = _continuous_start(five_series)
    prior_volatility: list[int] = []
    output: dict[int, tuple[str, str]] = {}
    for index, wrapped in enumerate(five_series.bars):
        if not five_series.continues[index]:
            prior_volatility = []
        key = (
            wrapped.bar.end_ns,
            wrapped.bar.contract,
            wrapped.outcome_span_id,
            wrapped.bar.segment_id,
        )
        position = original_index[key]
        if not eligible[position]:
            continue
        if index - VOLATILITY_WINDOW_5M + 1 < starts[index]:
            output[position] = (
                "MISSING_BASE_VOLATILITY_HISTORY",
                "MISSING_REGIME_HISTORY",
            )
            continue
        recent = five_series.bars[index - VOLATILITY_WINDOW_5M + 1 : index + 1]
        volatility = sum(item.bar.high_ticks - item.bar.low_ticks for item in recent)
        if len(prior_volatility) < VOLATILITY_HISTORY:
            volatility_stratum = "MISSING_PRIOR_20_HISTORY"
        else:
            reference = prior_volatility[-VOLATILITY_HISTORY:]
            rank = sum(item <= volatility for item in reference)
            quartile = min(3, 4 * rank // (len(reference) + 1))
            volatility_stratum = f"VOL_Q{quartile + 1}"
        if index - REGIME_WINDOW_5M < starts[index]:
            regime = "MISSING_REGIME_HISTORY"
        else:
            regime_rows = five_series.bars[index - REGIME_WINDOW_5M : index + 1]
            closes = [item.bar.close_ticks for item in regime_rows]
            travelled = sum(abs(right - left) for left, right in pairwise(closes))
            delta = closes[-1] - closes[0]
            if travelled == 0 or 3 * abs(delta) <= travelled:
                regime = "RANGE"
            elif delta > 0:
                regime = "TREND_UP"
            else:
                regime = "TREND_DOWN"
        output[position] = volatility_stratum, regime
        prior_volatility.append(volatility)
    return output


def _null_seed(seed: int | str, stage_key: str, candidate_id: str, role: str) -> str:
    if isinstance(seed, bool) or not isinstance(seed, (int, str)):
        raise DelayedMtfEngineError("null seed must be an integer or string")
    return canonical_sha256(
        {
            "candidate_id": candidate_id,
            "master_seed": seed,
            "role": role,
            "stage_key": stage_key,
        }
    )


def _opportunity_group(item: BarWithOutcomeSpan) -> tuple[date, str, int, int]:
    return item.bar.source_date, item.bar.contract, item.outcome_span_id, item.bar.segment_id


def _circular_control(
    real: SignalMask,
    fives: tuple[BarWithOutcomeSpan, ...],
    eligible: tuple[bool, ...],
    *,
    seed: int | str,
    stage_key: str,
) -> tuple[SignalMask | None, str | None]:
    by_group: dict[tuple[date, str, int, int], list[int]] = defaultdict(list)
    for index, (wrapped, allowed) in enumerate(zip(fives, eligible, strict=True)):
        if allowed:
            by_group[_opportunity_group(wrapped)].append(index)
    output = [False] * len(fives)
    seed_sha = _null_seed(seed, stage_key, real.proposal_sha256, "CIRCULAR_SHIFT")
    for group, positions in sorted(by_group.items()):
        source = [real.values[index] for index in positions]
        count = sum(source)
        if count == 0:
            continue
        if count == len(source) or len(source) < 2:
            for position, selected in zip(positions, source, strict=True):
                output[position] = selected
            continue
        start = (
            int(canonical_sha256({"group": list(map(str, group)), "seed": seed_sha}), 16)
            % (len(source) - 1)
            + 1
        )
        chosen: list[bool] | None = None
        for step in range(len(source) - 1):
            offset = (start + step - 1) % (len(source) - 1) + 1
            shifted = source[-offset:] + source[:-offset]
            if shifted != source:
                chosen = shifted
                break
        if chosen is None:
            return None, "NONIDENTITY_CIRCULAR_SHIFT_IMPOSSIBLE"
        for position, selected in zip(positions, chosen, strict=True):
            output[position] = selected
    values = tuple(output)
    if sum(values) != real.signal_count or values == real.values:
        return None, "NONIDENTITY_CIRCULAR_SHIFT_IMPOSSIBLE"
    return (
        SignalMask(
            f"{real.proposal_sha256}:circular_shift",
            real.proposal_sha256,
            "CIRCULAR_SHIFT",
            values,
            (seed_sha,),
        ),
        None,
    )


def _matched_control(
    real: SignalMask,
    fives: tuple[BarWithOutcomeSpan, ...],
    eligible: tuple[bool, ...],
    strata: Mapping[int, tuple[str, str]],
    *,
    seed: int | str,
    stage_key: str,
) -> tuple[SignalMask | None, tuple[tuple[int, int, int], ...], str | None]:
    seed_sha = _null_seed(seed, stage_key, real.proposal_sha256, "MATCHED_RANDOM")
    real_positions = [index for index, selected in enumerate(real.values) if selected]
    if any(index not in strata for index in real_positions):
        return None, (), "MISSING_CAUSAL_MATCH_STRATUM"
    real_set = set(real_positions)
    available = [
        index
        for index, allowed in enumerate(eligible)
        if allowed and index not in real_set and index in strata
    ]
    used: set[int] = set()
    pairs: list[tuple[int, int, int]] = []

    def broad_pool_size(real_index: int) -> int:
        wrapped = fives[real_index]
        return sum(
            index not in real_set
            and fives[index].bar.contract == wrapped.bar.contract
            and fives[index].outcome_span_id == wrapped.outcome_span_id
            for index in available
        )

    ordered_real = sorted(
        real_positions,
        key=lambda index: (
            broad_pool_size(index),
            canonical_sha256({"real_index": index, "seed": seed_sha}),
        ),
    )
    for real_index in ordered_real:
        real_bar = fives[real_index]
        real_group = _opportunity_group(real_bar)
        real_bucket = _utc_hour(real_bar.bar.end_ns) // MATCH_TIME_BUCKET_HOURS
        real_date, real_contract, real_span, _real_segment = real_group
        exact_stratum_pool = [
            index
            for index in available
            if index not in used
            and _opportunity_group(fives[index]) == real_group
            and strata[index] == strata[real_index]
        ]
        regime_pool = [
            index
            for index in available
            if index not in used
            and _opportunity_group(fives[index]) == real_group
            and strata[index][1] == strata[real_index][1]
        ]
        group_pool = [
            index
            for index in available
            if index not in used and _opportunity_group(fives[index]) == real_group
        ]
        same_date_span_pool = [
            index
            for index in available
            if index not in used
            and fives[index].bar.source_date == real_date
            and fives[index].bar.contract == real_contract
            and fives[index].outcome_span_id == real_span
        ]
        cross_date_span_pool = [
            index
            for index in available
            if index not in used
            and fives[index].bar.contract == real_contract
            and fives[index].outcome_span_id == real_span
        ]

        def bucket_distance(index: int, reference_bucket: int = real_bucket) -> int:
            bucket = _utc_hour(fives[index].bar.end_ns) // MATCH_TIME_BUCKET_HOURS
            direct = abs(bucket - reference_bucket)
            return min(direct, 6 - direct)

        same_date_exact_strata = [
            index for index in same_date_span_pool if strata[index] == strata[real_index]
        ]
        cross_date_exact_near_bucket = [
            index
            for index in cross_date_span_pool
            if strata[index] == strata[real_index] and bucket_distance(index) <= 1
        ]
        candidates_by_level = (
            [
                index
                for index in exact_stratum_pool
                if _utc_hour(fives[index].bar.end_ns) // MATCH_TIME_BUCKET_HOURS == real_bucket
            ],
            [
                index
                for index in exact_stratum_pool
                if abs(_utc_hour(fives[index].bar.end_ns) // MATCH_TIME_BUCKET_HOURS - real_bucket)
                == 1
            ],
            exact_stratum_pool,
            regime_pool,
            group_pool,
            same_date_exact_strata,
            same_date_span_pool,
            cross_date_exact_near_bucket,
            cross_date_span_pool,
        )
        choice: int | None = None
        chosen_level = -1
        for level, candidates in enumerate(candidates_by_level):
            if candidates:

                def choice_key(
                    index: int,
                    selected_level: int = level,
                    selected_real_index: int = real_index,
                    selected_real_date: date = real_date,
                ) -> tuple[object, ...]:
                    hash_key = canonical_sha256(
                        {
                            "control_index": index,
                            "level": selected_level,
                            "real_index": selected_real_index,
                            "seed": seed_sha,
                        }
                    )
                    if selected_level == 6:
                        return bucket_distance(index), hash_key
                    if selected_level == 7:
                        return (
                            bucket_distance(index),
                            abs(
                                fives[index].bar.source_date.toordinal()
                                - selected_real_date.toordinal()
                            ),
                            hash_key,
                        )
                    if selected_level == 8:
                        real_vol, real_regime = strata[selected_real_index]
                        control_vol, control_regime = strata[index]

                        def vol_ordinal(value: str) -> int:
                            return int(value[-1]) if value.startswith("VOL_Q") else -1

                        return (
                            bucket_distance(index),
                            control_regime != real_regime,
                            abs(vol_ordinal(control_vol) - vol_ordinal(real_vol)),
                            abs(
                                fives[index].bar.source_date.toordinal()
                                - selected_real_date.toordinal()
                            ),
                            hash_key,
                        )
                    return (hash_key,)

                choice = min(candidates, key=choice_key)
                chosen_level = level
                break
        if choice is None:
            return None, (), "MATCHED_RANDOM_WITHOUT_REPLACEMENT_IMPOSSIBLE"
        used.add(choice)
        pairs.append((real_index, choice, chosen_level))
    output = [False] * len(fives)
    for _, control_index, _ in pairs:
        output[control_index] = True
    values = tuple(output)
    if sum(values) != real.signal_count or values == real.values:
        return None, (), "DISTINCT_MATCHED_RANDOM_IMPOSSIBLE"
    return (
        SignalMask(
            f"{real.proposal_sha256}:matched_random",
            real.proposal_sha256,
            "MATCHED_RANDOM",
            values,
            (seed_sha,),
        ),
        tuple(sorted(pairs)),
        None,
    )


@dataclass(frozen=True, slots=True)
class FrozenCandidateMasks:
    candidate: SymbolicCandidate
    real: SignalMask
    circular_shift: SignalMask | None
    matched_random: SignalMask | None
    matched_pairs: tuple[tuple[int, int, int], ...]
    raw_values: tuple[bool, ...]
    raw_signal_count: int
    raw_signal_daily_counts: tuple[tuple[date, int], ...]
    raw_signal_group_counts: tuple[tuple[str, int], ...]
    sample_eligible: bool
    ineligibility_reason: str | None

    def __post_init__(self) -> None:
        if self.real.proposal_sha256 != self.candidate.candidate_id:
            raise DelayedMtfEngineError("real mask candidate identity differs")
        if self.sample_eligible != (
            self.circular_shift is not None and self.matched_random is not None
        ):
            raise DelayedMtfEngineError("candidate null eligibility is inconsistent")
        if self.sample_eligible == (self.ineligibility_reason is not None):
            raise DelayedMtfEngineError("candidate ineligibility reason is inconsistent")
        _require_int(self.raw_signal_count, label="raw_signal_count", minimum=0)
        if (
            len(self.raw_values) != len(self.real.values)
            or any(not isinstance(value, bool) for value in self.raw_values)
            or sum(self.raw_values) != self.raw_signal_count
        ):
            raise DelayedMtfEngineError("raw mask is inconsistent with its count")

    def masks(self) -> tuple[SignalMask, ...]:
        controls = tuple(
            item for item in (self.circular_shift, self.matched_random) if item is not None
        )
        return (self.real, *controls)

    def as_dict(self) -> dict[str, object]:
        relaxation_counts: dict[int, int] = defaultdict(int)
        for _real, _control, level in self.matched_pairs:
            relaxation_counts[level] += 1
        return {
            "candidate": self.candidate.as_dict(),
            "circular_shift": (
                None if self.circular_shift is None else self.circular_shift.as_dict()
            ),
            "ineligibility_reason": self.ineligibility_reason,
            "matched_pairs": [
                {
                    "control_index": control,
                    "real_index": real,
                    "relaxation_level": level,
                }
                for real, control, level in self.matched_pairs
            ],
            "matched_relaxation_counts": [
                {"match_count": count, "relaxation_level": level}
                for level, count in sorted(relaxation_counts.items())
            ],
            "matched_random": (
                None if self.matched_random is None else self.matched_random.as_dict()
            ),
            "raw_selected_indexes": [
                index for index, selected in enumerate(self.raw_values) if selected
            ],
            "raw_signal_daily_counts": [
                {"decision_date": day.isoformat(), "signal_count": count}
                for day, count in self.raw_signal_daily_counts
            ],
            "raw_signal_group_counts": [
                {"group_key": key, "signal_count": count}
                for key, count in self.raw_signal_group_counts
            ],
            "raw_signal_count": self.raw_signal_count,
            "raw_values_sha256": canonical_sha256(list(self.raw_values)),
            "real": self.real.as_dict(),
            "sample_eligible": self.sample_eligible,
        }


@dataclass(frozen=True, slots=True)
class FrozenStageMasks:
    stage_key: str
    catalog_sha256: str
    candidate_masks: tuple[FrozenCandidateMasks, ...]
    eligible_positions: tuple[bool, ...]
    five_minute_view_sha256: str
    multi_timeframe_view_sha256: str

    def __post_init__(self) -> None:
        _require_nonempty(self.stage_key, label="stage_key")
        _require_sha(self.catalog_sha256, label="catalog_sha256")
        _require_sha(self.five_minute_view_sha256, label="five_minute_view_sha256")
        _require_sha(self.multi_timeframe_view_sha256, label="multi_timeframe_view_sha256")
        if not self.candidate_masks or not self.eligible_positions:
            raise DelayedMtfEngineError("frozen stage masks cannot be empty")
        ids = tuple(item.candidate.candidate_id for item in self.candidate_masks)
        if len(set(ids)) != len(ids):
            raise DelayedMtfEngineError("frozen stage candidate ids must be unique")
        if tuple(sorted(item.candidate.selection_rank for item in self.candidate_masks)) != tuple(
            item.candidate.selection_rank for item in self.candidate_masks
        ):
            raise DelayedMtfEngineError("frozen stage candidates must retain catalog order")
        for item in self.candidate_masks:
            if len(item.real.values) != len(self.eligible_positions):
                raise DelayedMtfEngineError("mask length differs from five-minute view")

    @property
    def candidate_ids(self) -> tuple[str, ...]:
        return tuple(item.candidate.candidate_id for item in self.candidate_masks)

    @property
    def commitment_sha256(self) -> str:
        return canonical_sha256(self.as_dict(include_commitment=False))

    def as_dict(self, *, include_commitment: bool = True) -> dict[str, object]:
        payload: dict[str, object] = {
            "candidate_ids": list(self.candidate_ids),
            "candidate_masks": [item.as_dict() for item in self.candidate_masks],
            "catalog_sha256": self.catalog_sha256,
            "eligible_positions_sha256": canonical_sha256(list(self.eligible_positions)),
            "eligible_position_count": sum(self.eligible_positions),
            "five_minute_view_sha256": self.five_minute_view_sha256,
            "multi_timeframe_view_sha256": self.multi_timeframe_view_sha256,
            "schema": MASK_SCHEMA,
            "stage_key": self.stage_key,
        }
        if include_commitment:
            payload["commitment_sha256"] = canonical_sha256(payload)
        return payload


def _bar_view_payload(values: Sequence[BarWithOutcomeSpan]) -> list[dict[str, object]]:
    return [{"bar": item.bar.as_dict(), "outcome_span_id": item.outcome_span_id} for item in values]


def freeze_delayed_mtf_stage_masks(
    stage_key: str,
    five_minute_bars: Sequence[BarWithOutcomeSpan],
    half_hour_bars: Sequence[BarWithOutcomeSpan],
    one_hour_bars: Sequence[BarWithOutcomeSpan],
    *,
    decision_dates: Iterable[date],
    allowed_stage_tail_end_ns: int,
    seed: int | str,
    candidate_ids: Sequence[str] | None = None,
    group_by_date: Mapping[date, str] | None = None,
) -> FrozenStageMasks:
    """Freeze real and outcome-blind control masks from completed bars only."""

    key = _require_nonempty(stage_key, label="stage_key")
    if seed != DEFAULT_MASTER_NULL_SEED:
        raise DelayedMtfEngineError(
            f"seed must equal the frozen master seed {DEFAULT_MASTER_NULL_SEED!r}"
        )
    dates = _validated_dates(decision_dates)
    raw_groups = {} if group_by_date is None else dict(group_by_date)
    if any(
        isinstance(day, datetime)
        or not isinstance(day, date)
        or not isinstance(group, str)
        or not group
        for day, group in raw_groups.items()
    ):
        raise DelayedMtfEngineError("group_by_date must map dates to non-empty strings")
    fives = _validated_bars(
        five_minute_bars, timeframe_seconds=FIVE_MINUTES, label="five_minute_bars"
    )
    halves = _validated_bars(half_hour_bars, timeframe_seconds=HALF_HOUR, label="half_hour_bars")
    hours = _validated_bars(one_hour_bars, timeframe_seconds=ONE_HOUR, label="one_hour_bars")
    catalog = build_delayed_mtf_candidate_catalog()
    if candidate_ids is None:
        candidates = catalog.candidates
    else:
        requested = tuple(candidate_ids)
        if not requested or len(set(requested)) != len(requested):
            raise DelayedMtfEngineError("candidate_ids must be non-empty and unique")
        by_id = {item.candidate_id: item for item in catalog.candidates}
        if any(item not in by_id for item in requested):
            raise DelayedMtfEngineError("candidate_ids contains an unknown catalog identity")
        requested_set = set(requested)
        candidates = tuple(
            item for item in catalog.candidates if item.candidate_id in requested_set
        )
    series_by_timeframe = {
        FIVE_MINUTES: _stage_series(fives, dates),
        HALF_HOUR: _stage_series(halves, dates),
        ONE_HOUR: _stage_series(hours, dates),
    }
    cache = _FeatureCache(series_by_timeframe)
    anchor_by_key = _anchor_map(fives)
    eligible = _execution_eligible_positions(
        fives,
        decision_dates=dates,
        allowed_stage_tail_end_ns=allowed_stage_tail_end_ns,
    )
    strata = _causal_match_strata(series_by_timeframe[FIVE_MINUTES], anchor_by_key, eligible)
    if any(allowed and index not in strata for index, allowed in enumerate(eligible)):
        raise DelayedMtfEngineError(
            "complete-case opportunity lattice lacks a causal match stratum"
        )
    frozen: list[FrozenCandidateMasks] = []
    for candidate in candidates:
        trigger_tf = candidate.parameter("trigger_timeframe_seconds")
        trigger = series_by_timeframe[trigger_tf]
        if candidate.family == "DELAYED_MACD":
            raw = _raw_macd_mask(candidate, trigger, cache, anchor_by_key, len(fives))
        else:
            context = (
                None
                if candidate.family == "RANGE_REGIME_MEAN_REVERSION"
                else series_by_timeframe[candidate.parameter("context_timeframe_seconds")]
            )
            raw = _raw_generic_mask(candidate, trigger, context, cache, anchor_by_key, len(fives))
        real_values = tuple(
            selected and allowed for selected, allowed in zip(raw, eligible, strict=True)
        )
        real = SignalMask(
            f"{candidate.candidate_id}:real",
            candidate.candidate_id,
            "REAL",
            real_values,
        )
        circular, circular_reason = _circular_control(
            real, fives, eligible, seed=seed, stage_key=key
        )
        matched, pairs, matched_reason = _matched_control(
            real, fives, eligible, strata, seed=seed, stage_key=key
        )
        # Zero-signal candidates cannot form distinct equal-cardinality controls.
        reason = circular_reason or matched_reason
        daily_counts: dict[date, int] = defaultdict(int)
        group_counts: dict[str, int] = defaultdict(int)
        for wrapped, selected in zip(fives, raw, strict=True):
            if selected:
                daily_counts[wrapped.bar.source_date] += 1
                if wrapped.bar.source_date in raw_groups:
                    group_counts[raw_groups[wrapped.bar.source_date]] += 1
        frozen.append(
            FrozenCandidateMasks(
                candidate,
                real,
                circular,
                matched,
                pairs,
                raw,
                sum(raw),
                tuple(sorted(daily_counts.items())),
                tuple(sorted(group_counts.items())),
                reason is None,
                reason,
            )
        )
    view_sha = canonical_sha256(
        {
            "30m": _bar_view_payload(halves),
            "1h": _bar_view_payload(hours),
            "5m": _bar_view_payload(fives),
            "decision_dates": [item.isoformat() for item in dates],
        }
    )
    return FrozenStageMasks(
        key,
        catalog.catalog_sha256,
        tuple(frozen),
        eligible,
        canonical_sha256(_bar_view_payload(fives)),
        view_sha,
    )


def holdout_evaluation_inputs(
    frozen: FrozenStageMasks,
    *,
    include_controls: bool = True,
) -> tuple[dict[str, SignalMask], dict[str, Direction]]:
    """Adapt frozen masks to the generic holdout evaluator's mapping interface."""

    if not isinstance(frozen, FrozenStageMasks):
        raise DelayedMtfEngineError("frozen must be FrozenStageMasks")
    masks: dict[str, SignalMask] = {}
    directions: dict[str, Direction] = {}
    for item in frozen.candidate_masks:
        selected = item.masks() if include_controls else (item.real,)
        for mask in selected:
            masks[mask.key] = mask
            directions[mask.key] = item.candidate.direction
    return masks, directions


# Stable concise aliases used by orchestration code.
freeze_stage_masks = freeze_delayed_mtf_stage_masks


def _fraction_payload(value: Fraction | None) -> dict[str, int] | None:
    if value is None:
        return None
    return {"denominator": value.denominator, "numerator": value.numerator}


def _median(values: Sequence[int]) -> Fraction | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return Fraction(ordered[middle], 1)
    return Fraction(ordered[middle - 1] + ordered[middle], 2)


def _maximum_drawdown(values: Sequence[int]) -> int:
    equity = 0
    peak = 0
    maximum = 0
    for value in values:
        equity += value
        peak = max(peak, equity)
        maximum = max(maximum, peak - equity)
    return maximum


def exact_one_sided_sign_test(values: Iterable[int]) -> Fraction:
    nonzero = tuple(value for value in values if value != 0)
    if not nonzero:
        return Fraction(1, 1)
    positives = sum(value > 0 for value in nonzero)
    return Fraction(
        sum(comb(len(nonzero), k) for k in range(positives, len(nonzero) + 1)), 2 ** len(nonzero)
    )


def _paired_sign_test(
    left: Sequence[tuple[date, int]], right: Sequence[tuple[date, int]]
) -> Fraction:
    left_map = dict(left)
    right_map = dict(right)
    dates = tuple(sorted(set(left_map) | set(right_map)))
    return exact_one_sided_sign_test(left_map.get(day, 0) - right_map.get(day, 0) for day in dates)


@dataclass(frozen=True, slots=True)
class FixedHorizonTrade:
    mask_key: str
    candidate_id: str
    mask_role: MaskRole
    direction: Direction
    signal_index: int
    decision_date: date
    contract: str
    outcome_span_id: int
    segment_id: int
    signal_end_ns: int
    entry_ns: int
    exit_ns: int
    entry_reference_ticks: int
    exit_reference_ticks: int
    entry_fill_ticks: int
    exit_fill_ticks: int
    reference_pnl_ticks: int
    gross_pnl_ticks: int
    fully_loaded_net_pnl_ticks: int

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "contract": self.contract,
            "decision_date": self.decision_date.isoformat(),
            "direction": self.direction,
            "entry_fill_ticks": self.entry_fill_ticks,
            "entry_ns": self.entry_ns,
            "entry_reference_ticks": self.entry_reference_ticks,
            "exit_fill_ticks": self.exit_fill_ticks,
            "exit_ns": self.exit_ns,
            "exit_reference_ticks": self.exit_reference_ticks,
            "fully_loaded_net_pnl_ticks": self.fully_loaded_net_pnl_ticks,
            "gross_pnl_ticks": self.gross_pnl_ticks,
            "mask_key": self.mask_key,
            "mask_role": self.mask_role,
            "outcome_span_id": self.outcome_span_id,
            "reference_pnl_ticks": self.reference_pnl_ticks,
            "segment_id": self.segment_id,
            "signal_end_ns": self.signal_end_ns,
            "signal_index": self.signal_index,
        }


@dataclass(frozen=True, slots=True)
class CensoredSignal:
    mask_key: str
    signal_index: int
    decision_date: date
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            "decision_date": self.decision_date.isoformat(),
            "mask_key": self.mask_key,
            "reason": self.reason,
            "signal_index": self.signal_index,
        }


@dataclass(frozen=True, slots=True)
class FixedHorizonGroupSummary:
    group_key: str
    trade_count: int
    total_net_ticks: int
    mean_net_ticks: Fraction | None
    positive_trade_count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "group_key": self.group_key,
            "mean_net_ticks": _fraction_payload(self.mean_net_ticks),
            "positive_trade_count": self.positive_trade_count,
            "total_net_ticks": self.total_net_ticks,
            "trade_count": self.trade_count,
        }


@dataclass(frozen=True, slots=True)
class FixedHorizonEvaluationSummary:
    raw_signal_count: int
    trade_count: int
    censored_signal_count: int
    skipped_occupied_count: int
    total_reference_pnl_ticks: int
    total_gross_pnl_ticks: int
    total_net_pnl_ticks: int
    mean_net_pnl_ticks: Fraction | None
    median_net_pnl_ticks: Fraction | None
    profit_factor: Fraction | None
    maximum_drawdown_ticks: int
    positive_trade_count: int
    daily_net_ticks: tuple[tuple[date, int], ...]
    daily_trade_counts: tuple[tuple[date, int], ...]
    group_summaries: tuple[FixedHorizonGroupSummary, ...]
    p_vs_zero: Fraction

    def as_dict(self) -> dict[str, object]:
        return {
            "censored_signal_count": self.censored_signal_count,
            "daily_net_ticks": [
                {"decision_date": day.isoformat(), "net_ticks": value}
                for day, value in self.daily_net_ticks
            ],
            "daily_trade_counts": [
                {"decision_date": day.isoformat(), "trade_count": value}
                for day, value in self.daily_trade_counts
            ],
            "group_summaries": [item.as_dict() for item in self.group_summaries],
            "maximum_drawdown_ticks": self.maximum_drawdown_ticks,
            "mean_net_pnl_ticks": _fraction_payload(self.mean_net_pnl_ticks),
            "median_net_pnl_ticks": _fraction_payload(self.median_net_pnl_ticks),
            "p_vs_zero": _fraction_payload(self.p_vs_zero),
            "positive_trade_count": self.positive_trade_count,
            "profit_factor": _fraction_payload(self.profit_factor),
            "raw_signal_count": self.raw_signal_count,
            "skipped_occupied_count": self.skipped_occupied_count,
            "total_gross_pnl_ticks": self.total_gross_pnl_ticks,
            "total_net_pnl_ticks": self.total_net_pnl_ticks,
            "total_reference_pnl_ticks": self.total_reference_pnl_ticks,
            "trade_count": self.trade_count,
        }


@dataclass(frozen=True, slots=True)
class FixedHorizonMaskEvaluation:
    mask_key: str
    candidate_id: str
    mask_role: MaskRole
    direction: Direction
    trades: tuple[FixedHorizonTrade, ...]
    censored_signals: tuple[CensoredSignal, ...]
    summary: FixedHorizonEvaluationSummary

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "censored_signals": [item.as_dict() for item in self.censored_signals],
            "direction": self.direction,
            "mask_key": self.mask_key,
            "mask_role": self.mask_role,
            "summary": self.summary.as_dict(),
            "trades": [item.as_dict() for item in self.trades],
        }


@dataclass(frozen=True, slots=True)
class FixedHorizonCandidateResult:
    candidate: SymbolicCandidate
    raw_signal_count: int
    raw_signal_daily_counts: tuple[tuple[date, int], ...]
    raw_signal_group_counts: tuple[tuple[str, int], ...]
    sample_eligible: bool
    ineligibility_reason: str | None
    real: FixedHorizonMaskEvaluation
    circular_shift: FixedHorizonMaskEvaluation | None
    matched_random: FixedHorizonMaskEvaluation | None
    p_vs_zero: Fraction
    p_vs_circular_shift: Fraction | None
    p_vs_matched_random: Fraction | None
    conservative_p_value: Fraction | None

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate": self.candidate.as_dict(),
            "circular_shift": (
                None if self.circular_shift is None else self.circular_shift.as_dict()
            ),
            "conservative_p_value": _fraction_payload(self.conservative_p_value),
            "ineligibility_reason": self.ineligibility_reason,
            "matched_random": (
                None if self.matched_random is None else self.matched_random.as_dict()
            ),
            "p_vs_circular_shift": _fraction_payload(self.p_vs_circular_shift),
            "p_vs_matched_random": _fraction_payload(self.p_vs_matched_random),
            "p_vs_zero": _fraction_payload(self.p_vs_zero),
            "raw_signal_count": self.raw_signal_count,
            "raw_signal_daily_counts": [
                {"decision_date": day.isoformat(), "signal_count": count}
                for day, count in self.raw_signal_daily_counts
            ],
            "raw_signal_group_counts": [
                {"group_key": key, "signal_count": count}
                for key, count in self.raw_signal_group_counts
            ],
            "real": self.real.as_dict(),
            "sample_eligible": self.sample_eligible,
        }


@dataclass(frozen=True, slots=True)
class FixedHorizonStageResult:
    stage_key: str
    mask_commitment_sha256: str
    candidates: tuple[FixedHorizonCandidateResult, ...]

    @property
    def result_sha256(self) -> str:
        return canonical_sha256(self.as_dict(include_result_sha=False))

    def as_dict(self, *, include_result_sha: bool = True) -> dict[str, object]:
        payload: dict[str, object] = {
            "candidates": [item.as_dict() for item in self.candidates],
            "mask_commitment_sha256": self.mask_commitment_sha256,
            "schema": RESULT_SCHEMA,
            "stage_key": self.stage_key,
        }
        if include_result_sha:
            payload["result_sha256"] = canonical_sha256(payload)
        return payload


@dataclass(frozen=True, slots=True)
class _OneSecondPath:
    rows: tuple[BarWithOutcomeSpan, ...]
    starts: tuple[int, ...]
    ends: tuple[int, ...]


def _one_second_paths(
    values: Sequence[BarWithOutcomeSpan],
) -> dict[tuple[str, int, int], _OneSecondPath]:
    rows = _validated_bars(values, timeframe_seconds=1, label="one_second_part")
    grouped: dict[tuple[str, int, int], list[BarWithOutcomeSpan]] = defaultdict(list)
    for item in rows:
        grouped[_lineage(item)].append(item)
    output: dict[tuple[str, int, int], _OneSecondPath] = {}
    for key, items in grouped.items():
        ordered = tuple(items)
        if any(right.bar.start_ns <= left.bar.start_ns for left, right in pairwise(ordered)):
            raise DelayedMtfEngineError("one-second path is not strictly chronological")
        output[key] = _OneSecondPath(
            ordered,
            tuple(item.bar.start_ns for item in ordered),
            tuple(item.bar.end_ns for item in ordered),
        )
    return output


def _trade_for_signal(
    mask: SignalMask,
    role: MaskRole,
    direction: Direction,
    signal_index: int,
    fives: tuple[BarWithOutcomeSpan, ...],
    paths: Mapping[tuple[str, int, int], _OneSecondPath],
) -> FixedHorizonTrade | str:
    anchor = fives[signal_index]
    required = PRIMARY_HORIZON_SECONDS // FIVE_MINUTES
    if signal_index + required >= len(fives):
        return "MISSING_STRUCTURAL_HORIZON"
    entry_bar = fives[signal_index + 1]
    terminal_bar = fives[signal_index + required]
    target = anchor.bar.end_ns
    horizon = target + PRIMARY_HORIZON_SECONDS * ONE_SECOND_NS
    lineage = _lineage(anchor)
    if (
        entry_bar.bar.start_ns != target
        or terminal_bar.bar.end_ns != horizon
        or _lineage(entry_bar) != lineage
        or _lineage(terminal_bar) != lineage
    ):
        return "STRUCTURAL_LINEAGE_MISMATCH"
    path = paths.get(lineage)
    if path is None:
        return "MISSING_ONE_SECOND_LINEAGE"
    entry_second = entry_bar.bar.first_trade_ns // ONE_SECOND_NS * ONE_SECOND_NS
    entry_index = bisect_right(path.starts, entry_second) - 1
    if entry_index < 0 or path.starts[entry_index] != entry_second:
        return "MISSING_ENTRY_SECOND"
    entry_row = path.rows[entry_index].bar
    if (
        not (target <= entry_row.start_ns < entry_bar.bar.end_ns)
        or entry_row.open_ticks != entry_bar.bar.open_ticks
    ):
        return "ENTRY_BAR_VERIFICATION_FAILED"
    terminal_second = terminal_bar.bar.last_trade_ns // ONE_SECOND_NS * ONE_SECOND_NS
    exit_index = bisect_right(path.ends, horizon) - 1
    if exit_index < entry_index:
        return "MISSING_TERMINAL_SECOND"
    exit_row = path.rows[exit_index].bar
    if (
        exit_row.start_ns != terminal_second
        or exit_row.close_ticks != terminal_bar.bar.close_ticks
        or exit_row.end_ns > horizon
        or exit_row.start_ns < terminal_bar.bar.start_ns
    ):
        return "TERMINAL_BAR_VERIFICATION_FAILED"
    entry_reference = entry_row.open_ticks
    exit_reference = exit_row.close_ticks
    if direction == "LONG":
        entry_fill = entry_reference + ENTRY_ADVERSE_TICKS
        exit_fill = exit_reference - TERMINAL_ADVERSE_TICKS
        reference_pnl = exit_reference - entry_reference
        gross = exit_fill - entry_fill
    else:
        entry_fill = entry_reference - ENTRY_ADVERSE_TICKS
        exit_fill = exit_reference + TERMINAL_ADVERSE_TICKS
        reference_pnl = entry_reference - exit_reference
        gross = entry_fill - exit_fill
    net = gross - VARIABLE_DEBIT_TICKS - ALLOCATED_FIXED_COST_TICKS
    return FixedHorizonTrade(
        mask.key,
        mask.proposal_sha256,
        role,
        direction,
        signal_index,
        anchor.bar.source_date,
        anchor.bar.contract,
        anchor.outcome_span_id,
        anchor.bar.segment_id,
        anchor.bar.end_ns,
        entry_row.start_ns,
        horizon,
        entry_reference,
        exit_reference,
        entry_fill,
        exit_fill,
        reference_pnl,
        gross,
        net,
    )


def _summarize_fixed_horizon(
    trades: Sequence[FixedHorizonTrade],
    *,
    raw_signal_count: int,
    censored_signal_count: int,
    skipped_occupied_count: int,
    reporting_dates: tuple[date, ...],
    group_by_date: Mapping[date, str],
) -> FixedHorizonEvaluationSummary:
    ordered = tuple(sorted(trades, key=lambda item: (item.entry_ns, item.signal_index)))
    net_values = [item.fully_loaded_net_pnl_ticks for item in ordered]
    gains = sum(value for value in net_values if value > 0)
    losses = -sum(value for value in net_values if value < 0)
    profit_factor = None if losses == 0 else Fraction(gains, losses)
    dates = tuple(sorted(set(reporting_dates) | {item.decision_date for item in ordered}))
    daily_net: dict[date, int] = {day: 0 for day in dates}
    daily_counts: dict[date, int] = {day: 0 for day in dates}
    grouped: dict[str, list[int]] = defaultdict(list)
    for trade in ordered:
        daily_net[trade.decision_date] = (
            daily_net.get(trade.decision_date, 0) + trade.fully_loaded_net_pnl_ticks
        )
        daily_counts[trade.decision_date] = daily_counts.get(trade.decision_date, 0) + 1
        if trade.decision_date in group_by_date:
            grouped[group_by_date[trade.decision_date]].append(trade.fully_loaded_net_pnl_ticks)
    daily_net_tuple = tuple(sorted(daily_net.items()))
    group_summaries = tuple(
        FixedHorizonGroupSummary(
            key,
            len(values),
            sum(values),
            Fraction(sum(values), len(values)) if values else None,
            sum(value > 0 for value in values),
        )
        for key, values in sorted(grouped.items())
    )
    return FixedHorizonEvaluationSummary(
        raw_signal_count,
        len(ordered),
        censored_signal_count,
        skipped_occupied_count,
        sum(item.reference_pnl_ticks for item in ordered),
        sum(item.gross_pnl_ticks for item in ordered),
        sum(net_values),
        Fraction(sum(net_values), len(net_values)) if net_values else None,
        _median(net_values),
        profit_factor,
        _maximum_drawdown(net_values),
        sum(value > 0 for value in net_values),
        daily_net_tuple,
        tuple(sorted(daily_counts.items())),
        group_summaries,
        exact_one_sided_sign_test(value for _, value in daily_net_tuple),
    )


def _entry_overlaps_mask_occupancy(occupied_through_ns: int | None, entry_target_ns: int) -> bool:
    """Return mask-global overlap; lineage intentionally is not an input."""

    return occupied_through_ns is not None and entry_target_ns < occupied_through_ns


def evaluate_delayed_mtf_stage_parts(
    stage_key: str,
    five_minute_bars: Sequence[BarWithOutcomeSpan],
    one_second_parts: Iterable[Sequence[BarWithOutcomeSpan]],
    frozen_masks: FrozenStageMasks,
    *,
    allowed_stage_tail_end_ns: int,
    reporting_dates: Iterable[date] = (),
    group_by_date: Mapping[date, str] | None = None,
) -> FixedHorizonStageResult:
    """Stream verified outcome-span parts through all masks using one index per part."""

    key = _require_nonempty(stage_key, label="stage_key")
    if not isinstance(frozen_masks, FrozenStageMasks) or frozen_masks.stage_key != key:
        raise DelayedMtfEngineError("stage key differs from frozen masks")
    fives = _validated_bars(
        five_minute_bars, timeframe_seconds=FIVE_MINUTES, label="five_minute_bars"
    )
    if canonical_sha256(_bar_view_payload(fives)) != frozen_masks.five_minute_view_sha256:
        raise DelayedMtfEngineError("five-minute view differs from the frozen commitment")
    if len(fives) != len(frozen_masks.eligible_positions):
        raise DelayedMtfEngineError("five-minute view length differs from frozen masks")
    tail = _require_int(allowed_stage_tail_end_ns, label="allowed_stage_tail_end_ns", minimum=1)
    if max(item.bar.end_ns for item in fives) > tail:
        raise DelayedMtfEngineError("five-minute view extends beyond allowed stage tail")
    report_dates = tuple(sorted(set(reporting_dates)))
    if any(isinstance(item, datetime) or not isinstance(item, date) for item in report_dates):
        raise DelayedMtfEngineError("reporting_dates must contain date values")
    groups = {} if group_by_date is None else dict(group_by_date)
    if any(
        not isinstance(day, date) or not isinstance(value, str) or not value
        for day, value in groups.items()
    ):
        raise DelayedMtfEngineError("group_by_date must map dates to non-empty strings")
    masks, directions = holdout_evaluation_inputs(frozen_masks)
    roles: dict[str, MaskRole] = {
        mask.key: mask.kind
        for mask in masks.values()  # type: ignore[dict-item]
    }
    selected_positions = {
        mask.key: tuple(index for index, selected in enumerate(mask.values) if selected)
        for mask in masks.values()
    }
    trades: dict[str, list[FixedHorizonTrade]] = {mask_key: [] for mask_key in masks}
    censored: dict[str, list[CensoredSignal]] = {mask_key: [] for mask_key in masks}
    skipped: dict[str, int] = {mask_key: 0 for mask_key in masks}
    processed: set[tuple[str, int]] = set()
    occupied_through: dict[str, int | None] = {mask_key: None for mask_key in masks}
    processed_spans: set[int] = set()
    prior_part_start: int | None = None
    for raw_part in one_second_parts:
        part = tuple(raw_part)
        paths = _one_second_paths(part)
        spans = {item.outcome_span_id for item in part}
        if len(spans) != 1:
            raise DelayedMtfEngineError("one-second outcome part must contain one outcome span")
        span = next(iter(spans))
        if span in processed_spans:
            raise DelayedMtfEngineError("one-second outcome span was supplied more than once")
        processed_spans.add(span)
        part_start = min(item.bar.start_ns for item in part)
        if prior_part_start is not None and part_start <= prior_part_start:
            raise DelayedMtfEngineError("one-second outcome parts must be chronological")
        prior_part_start = part_start
        for mask_key in sorted(masks):
            mask = masks[mask_key]
            direction = directions[mask_key]
            role = roles[mask_key]
            for signal_index in selected_positions[mask_key]:
                if fives[signal_index].outcome_span_id != span:
                    continue
                identity = mask_key, signal_index
                if identity in processed:
                    raise DelayedMtfEngineError("a frozen signal was evaluated more than once")
                processed.add(identity)
                anchor = fives[signal_index]
                entry_target = anchor.bar.end_ns
                current_occupancy = occupied_through[mask_key]
                if _entry_overlaps_mask_occupancy(current_occupancy, entry_target):
                    skipped[mask_key] += 1
                    continue
                outcome = _trade_for_signal(mask, role, direction, signal_index, fives, paths)
                if isinstance(outcome, str):
                    censored[mask_key].append(
                        CensoredSignal(
                            mask_key,
                            signal_index,
                            anchor.bar.source_date,
                            outcome,
                        )
                    )
                    continue
                trades[mask_key].append(outcome)
                occupied_through[mask_key] = outcome.exit_ns
    for mask_key, positions in selected_positions.items():
        for signal_index in positions:
            if (mask_key, signal_index) not in processed:
                anchor = fives[signal_index]
                censored[mask_key].append(
                    CensoredSignal(
                        mask_key,
                        signal_index,
                        anchor.bar.source_date,
                        "MISSING_ONE_SECOND_OUTCOME_PART",
                    )
                )
    evaluations: dict[str, FixedHorizonMaskEvaluation] = {}
    for mask_key in sorted(masks):
        mask = masks[mask_key]
        summary = _summarize_fixed_horizon(
            trades[mask_key],
            raw_signal_count=mask.signal_count,
            censored_signal_count=len(censored[mask_key]),
            skipped_occupied_count=skipped[mask_key],
            reporting_dates=report_dates,
            group_by_date=groups,
        )
        evaluations[mask_key] = FixedHorizonMaskEvaluation(
            mask_key,
            mask.proposal_sha256,
            roles[mask_key],
            directions[mask_key],
            tuple(trades[mask_key]),
            tuple(sorted(censored[mask_key], key=lambda item: item.signal_index)),
            summary,
        )
    candidate_results: list[FixedHorizonCandidateResult] = []
    for item in frozen_masks.candidate_masks:
        real = evaluations[item.real.key]
        circular = None if item.circular_shift is None else evaluations[item.circular_shift.key]
        matched = None if item.matched_random is None else evaluations[item.matched_random.key]
        p_zero = real.summary.p_vs_zero
        p_circular = (
            None
            if circular is None
            else _paired_sign_test(real.summary.daily_net_ticks, circular.summary.daily_net_ticks)
        )
        p_matched = (
            None
            if matched is None
            else _paired_sign_test(real.summary.daily_net_ticks, matched.summary.daily_net_ticks)
        )
        conservative = (
            None if p_circular is None or p_matched is None else max(p_zero, p_circular, p_matched)
        )
        candidate_results.append(
            FixedHorizonCandidateResult(
                item.candidate,
                item.raw_signal_count,
                item.raw_signal_daily_counts,
                item.raw_signal_group_counts,
                item.sample_eligible,
                item.ineligibility_reason,
                real,
                circular,
                matched,
                p_zero,
                p_circular,
                p_matched,
                conservative,
            )
        )
    return FixedHorizonStageResult(
        key,
        frozen_masks.commitment_sha256,
        tuple(candidate_results),
    )


def evaluate_delayed_mtf_stage(
    stage_key: str,
    frozen_masks: FrozenStageMasks,
    five_minute_bars: Sequence[BarWithOutcomeSpan],
    one_second_bars: Sequence[BarWithOutcomeSpan],
    *,
    allowed_stage_tail_end_ns: int,
    reporting_dates: Iterable[date] = (),
    group_by_date: Mapping[date, str] | None = None,
) -> FixedHorizonStageResult:
    """Convenience wrapper for a stage supplied as one outcome-span part."""

    return evaluate_delayed_mtf_stage_parts(
        stage_key,
        five_minute_bars,
        (one_second_bars,),
        frozen_masks,
        allowed_stage_tail_end_ns=allowed_stage_tail_end_ns,
        reporting_dates=reporting_dates,
        group_by_date=group_by_date,
    )


evaluate_stage = evaluate_delayed_mtf_stage


__all__ = [
    "ALLOCATED_FIXED_COST_TICKS",
    "CANDIDATE_CATALOG_COUNT",
    "CANDIDATE_CATALOG_SHA256",
    "EMA_SCALE",
    "ENTRY_ADVERSE_TICKS",
    "PRIMARY_HORIZON_SECONDS",
    "TERMINAL_ADVERSE_TICKS",
    "TOTAL_FRICTION_TICKS",
    "VARIABLE_DEBIT_TICKS",
    "CandidateCatalog",
    "CensoredSignal",
    "DelayedMtfEngineError",
    "FixedHorizonCandidateResult",
    "FixedHorizonEvaluationSummary",
    "FixedHorizonGroupSummary",
    "FixedHorizonMaskEvaluation",
    "FixedHorizonStageResult",
    "FixedHorizonTrade",
    "FrozenCandidateMasks",
    "FrozenStageMasks",
    "SymbolicCandidate",
    "build_delayed_mtf_candidate_catalog",
    "delayed_mtf_engine_contract",
    "evaluate_delayed_mtf_stage",
    "evaluate_delayed_mtf_stage_parts",
    "evaluate_stage",
    "exact_one_sided_sign_test",
    "freeze_delayed_mtf_stage_masks",
    "freeze_stage_masks",
    "holdout_evaluation_inputs",
]
