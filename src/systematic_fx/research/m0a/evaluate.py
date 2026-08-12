"""Deterministic event screening and sequential evaluation for M0a.

All public functions are pure with respect to market data and contain no wall
clock, database, network, or LLM calls.  Search-data walk-forward results and
null controls are descriptive engineering evidence only; none of the result
types can express Paper or Live eligibility.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, localcontext
from typing import Final

from systematic_fx.research.m0a.controls import (
    CircularShiftSelection,
    MatchedRandomSelection,
    NullControlCandidate,
    circular_block_shift,
    matched_random_entries,
)
from systematic_fx.research.m0a.family import (
    StrategyCandidate,
    candidate_signal,
    generate_candidates,
)
from systematic_fx.research.m0a.model import (
    BarrierSpec,
    Direction,
    EventFeature,
    FirstTouchType,
    M0aDataError,
    QuoteAwareLabel,
)

SEARCH_DATA_RESULT: Final = "SEARCH_DATA_EXPLORATORY"
SURVIVOR_STATUS: Final = "SEARCH_DATA_SURVIVOR"
SCREENED_OUT_STATUS: Final = "SCREENED_OUT"
AUTHORITY_STATUS: Final = "AWAITING_SEALED_HOLDOUT"
ONE_SECOND_NS: Final = 1_000_000_000


class EvaluationError(M0aDataError):
    """Feature, label, split, or candidate inputs violate an M0a invariant."""


def _decimal_ratio(numerator: int, denominator: int) -> Decimal | None:
    if denominator == 0:
        return None
    with localcontext() as context:
        context.prec = 28
        return Decimal(numerator) / Decimal(denominator)


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


@dataclass(frozen=True, slots=True)
class PerformanceMetrics:
    """One exact event or strategy metric bundle."""

    signal_count: int
    trade_count: int
    invalid_count: int
    skipped_occupied_count: int
    tp_first_count: int
    sl_first_count: int
    timeout_count: int
    ambiguous_count: int
    raw_fallback_count: int
    gross_pnl_ticks: int
    cost_ticks: int
    net_pnl_ticks: int
    maximum_drawdown_ticks: int
    active_days: int
    session_distribution: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        counts = (
            self.signal_count,
            self.trade_count,
            self.invalid_count,
            self.skipped_occupied_count,
            self.tp_first_count,
            self.sl_first_count,
            self.timeout_count,
            self.ambiguous_count,
            self.raw_fallback_count,
            self.cost_ticks,
            self.maximum_drawdown_ticks,
            self.active_days,
        )
        if any(isinstance(value, bool) or value < 0 for value in counts):
            raise EvaluationError("metric counts and debits must be non-negative integers")
        if self.tp_first_count + self.sl_first_count + self.timeout_count != self.trade_count:
            raise EvaluationError("trade outcome counts do not sum to trade_count")
        if self.trade_count + self.invalid_count + self.skipped_occupied_count > self.signal_count:
            raise EvaluationError("metric disposition counts exceed signal_count")
        if tuple(sorted(self.session_distribution)) != self.session_distribution:
            raise EvaluationError("session_distribution must be sorted")
        if sum(count for _, count in self.session_distribution) != self.trade_count:
            raise EvaluationError("session_distribution does not sum to trade_count")

    @property
    def tp_probability_ppm(self) -> int | None:
        return (
            None if self.trade_count == 0 else self.tp_first_count * 1_000_000 // self.trade_count
        )

    @property
    def gross_ev_ticks(self) -> Decimal | None:
        return _decimal_ratio(self.gross_pnl_ticks, self.trade_count)

    @property
    def net_ev_ticks(self) -> Decimal | None:
        return _decimal_ratio(self.net_pnl_ticks, self.trade_count)

    def as_dict(self) -> dict[str, object]:
        return {
            "active_days": self.active_days,
            "ambiguous_count": self.ambiguous_count,
            "cost_ticks": self.cost_ticks,
            "gross_ev_ticks": _decimal_text(self.gross_ev_ticks),
            "gross_pnl_ticks": self.gross_pnl_ticks,
            "invalid_count": self.invalid_count,
            "maximum_drawdown_ticks": self.maximum_drawdown_ticks,
            "net_ev_ticks": _decimal_text(self.net_ev_ticks),
            "net_pnl_ticks": self.net_pnl_ticks,
            "raw_fallback_count": self.raw_fallback_count,
            "session_distribution": [
                {"session_id": session, "trade_count": count}
                for session, count in self.session_distribution
            ],
            "signal_count": self.signal_count,
            "skipped_occupied_count": self.skipped_occupied_count,
            "sl_first_count": self.sl_first_count,
            "timeout_count": self.timeout_count,
            "tp_first_count": self.tp_first_count,
            "tp_probability_ppm": self.tp_probability_ppm,
            "trade_count": self.trade_count,
        }


@dataclass(frozen=True, slots=True)
class TradeRecord:
    candidate_hash: str
    event_ts_ns: int
    entry_ts_ns: int
    exit_ts_ns: int
    instrument_id: int
    session_id: str
    trading_date: date
    direction: Direction
    barrier_id: str
    outcome: FirstTouchType
    entry_price_ticks: int
    exit_price_ticks: int
    gross_pnl_ticks: int
    cost_ticks: int
    net_pnl_ticks: int
    ambiguous: bool
    raw_fallback_used: bool

    def __post_init__(self) -> None:
        if self.entry_ts_ns < self.event_ts_ns or self.exit_ts_ns < self.entry_ts_ns:
            raise EvaluationError("trade timestamps are not chronological")
        if self.instrument_id <= 0 or not self.session_id:
            raise EvaluationError("trade identity is invalid")
        if self.outcome is FirstTouchType.INVALID:
            raise EvaluationError("an INVALID label cannot become a trade")
        if self.cost_ticks < 0 or self.net_pnl_ticks != self.gross_pnl_ticks - self.cost_ticks:
            raise EvaluationError("trade cost accounting is inconsistent")

    def as_dict(self) -> dict[str, object]:
        return {
            "ambiguous": self.ambiguous,
            "barrier_id": self.barrier_id,
            "candidate_hash": self.candidate_hash,
            "cost_ticks": self.cost_ticks,
            "direction": self.direction.value,
            "entry_price_ticks": self.entry_price_ticks,
            "entry_ts_ns": self.entry_ts_ns,
            "event_ts_ns": self.event_ts_ns,
            "exit_price_ticks": self.exit_price_ticks,
            "exit_ts_ns": self.exit_ts_ns,
            "gross_pnl_ticks": self.gross_pnl_ticks,
            "instrument_id": self.instrument_id,
            "net_pnl_ticks": self.net_pnl_ticks,
            "outcome": self.outcome.value,
            "raw_fallback_used": self.raw_fallback_used,
            "session_id": self.session_id,
            "trading_date": self.trading_date.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class FoldResult:
    fold_number: int
    result_role: str
    purge_seconds: int
    calibration_start_ns: int | None
    calibration_end_ns: int | None
    validation_start_ns: int
    validation_end_ns: int
    calibration_metrics: PerformanceMetrics
    validation_metrics: PerformanceMetrics

    def as_dict(self) -> dict[str, object]:
        return {
            "calibration_end_ns": self.calibration_end_ns,
            "calibration_metrics": self.calibration_metrics.as_dict(),
            "calibration_start_ns": self.calibration_start_ns,
            "fold_number": self.fold_number,
            "purge_seconds": self.purge_seconds,
            "result_role": self.result_role,
            "validation_end_ns": self.validation_end_ns,
            "validation_metrics": self.validation_metrics.as_dict(),
            "validation_start_ns": self.validation_start_ns,
        }


@dataclass(frozen=True, slots=True)
class ControlResult:
    control_id: str
    method: str
    metrics: PerformanceMetrics
    selection: Mapping[str, object]
    net_ev_uplift_ticks: Decimal | None

    def as_dict(self) -> dict[str, object]:
        return {
            "control_id": self.control_id,
            "method": self.method,
            "metrics": self.metrics.as_dict(),
            "net_ev_uplift_ticks": _decimal_text(self.net_ev_uplift_ticks),
            "selection": dict(self.selection),
        }


@dataclass(frozen=True, slots=True)
class AdmissionRules:
    """A modest engineering screen, never a Paper/Live promotion rule."""

    min_raw_events: int = 3
    min_sequential_trades: int = 2
    min_active_days: int = 1
    min_tp_probability_ppm: int = 500_000
    min_positive_folds: int = 1
    require_positive_net_ev: bool = True

    def __post_init__(self) -> None:
        if (
            min(
                self.min_raw_events,
                self.min_sequential_trades,
                self.min_active_days,
                self.min_positive_folds,
            )
            < 0
        ):
            raise EvaluationError("admission count thresholds must be non-negative")
        if not 0 <= self.min_tp_probability_ppm <= 1_000_000:
            raise EvaluationError("admission probability threshold is invalid")

    def as_dict(self) -> dict[str, object]:
        return {
            "authority": "SEARCH_DATA_ONLY",
            "min_active_days": self.min_active_days,
            "min_positive_folds": self.min_positive_folds,
            "min_raw_events": self.min_raw_events,
            "min_sequential_trades": self.min_sequential_trades,
            "min_tp_probability_ppm": self.min_tp_probability_ppm,
            "require_positive_net_ev": self.require_positive_net_ev,
        }

    @classmethod
    def from_config(cls, config: object) -> AdmissionRules:
        """Read exact preregistered thresholds from an epoch config or mapping."""

        source: object
        if hasattr(config, "admission_rules"):
            source = config.admission_rules
        elif isinstance(config, Mapping) and "admission_rules" in config:
            source = config["admission_rules"]
        else:
            source = config
        if isinstance(source, AdmissionRules):
            return source
        if isinstance(source, Mapping):
            values = source
            return cls(
                min_raw_events=int(values["min_raw_events"]),
                min_sequential_trades=int(values["min_sequential_trades"]),
                min_active_days=int(values["min_active_days"]),
                min_tp_probability_ppm=int(values["min_tp_probability_ppm"]),
                min_positive_folds=int(values["min_positive_folds"]),
                require_positive_net_ev=bool(values["require_positive_net_ev"]),
            )
        field_names = (
            "min_raw_events",
            "min_sequential_trades",
            "min_active_days",
            "min_tp_probability_ppm",
            "min_positive_folds",
            "require_positive_net_ev",
        )
        try:
            values = {field: getattr(source, f"admission_{field}") for field in field_names}
        except AttributeError as error:
            raise EvaluationError(
                "epoch config is missing preregistered admission rules"
            ) from error
        return cls(
            min_raw_events=int(values["min_raw_events"]),
            min_sequential_trades=int(values["min_sequential_trades"]),
            min_active_days=int(values["min_active_days"]),
            min_tp_probability_ppm=int(values["min_tp_probability_ppm"]),
            min_positive_folds=int(values["min_positive_folds"]),
            require_positive_net_ev=bool(values["require_positive_net_ev"]),
        )


DEFAULT_ADMISSION_RULES: Final = AdmissionRules()


@dataclass(frozen=True, slots=True)
class CandidateEvaluation:
    candidate: StrategyCandidate
    raw_event_metrics: PerformanceMetrics
    flat_only_metrics: PerformanceMetrics
    sequential_metrics: PerformanceMetrics
    stressed_cost_metrics: PerformanceMetrics
    trades: tuple[TradeRecord, ...]
    folds: tuple[FoldResult, ...]
    circular_shift_control: ControlResult
    matched_random_control: ControlResult
    status: str
    admission_reasons: tuple[str, ...]
    authority_status: str = AUTHORITY_STATUS
    paper_eligible: bool = False
    live_eligible: bool = False

    @property
    def search_data_survivor(self) -> bool:
        return self.status == SURVIVOR_STATUS

    @property
    def admitted(self) -> bool:
        """Admission means search-data survivor only, never Paper/Live authority."""

        return self.search_data_survivor

    def as_dict(self) -> dict[str, object]:
        return {
            "admission_reasons": list(self.admission_reasons),
            "admitted": self.admitted,
            "authority_status": self.authority_status,
            "candidate": self.candidate.as_dict(),
            "circular_shift_control": self.circular_shift_control.as_dict(),
            "flat_only_metrics": self.flat_only_metrics.as_dict(),
            "fold_metrics": [item.as_dict() for item in self.folds],
            "live_eligible": self.live_eligible,
            "matched_random_control": self.matched_random_control.as_dict(),
            "paper_eligible": self.paper_eligible,
            "raw_event_metrics": self.raw_event_metrics.as_dict(),
            "result_scope": SEARCH_DATA_RESULT,
            "search_data_survivor": self.search_data_survivor,
            "sequential_metrics": self.sequential_metrics.as_dict(),
            "status": self.status,
            "stressed_cost_metrics": self.stressed_cost_metrics.as_dict(),
            "trade_count": len(self.trades),
        }


@dataclass(frozen=True, slots=True)
class CandidateFailure:
    candidate_hash: str
    error_type: str
    error: str

    def as_dict(self) -> dict[str, str]:
        return {
            "candidate_hash": self.candidate_hash,
            "error": self.error,
            "error_type": self.error_type,
        }


@dataclass(frozen=True, slots=True)
class EpochEvaluation:
    seed: int
    evaluations: tuple[CandidateEvaluation, ...]
    failures: tuple[CandidateFailure, ...]
    real_experiments_attempted: int
    null_experiments_attempted: int
    result_scope: str = SEARCH_DATA_RESULT
    sealed_holdout_untouched: bool = True
    paper_eligible: bool = False
    live_eligible: bool = False

    @property
    def ranked(self) -> tuple[CandidateEvaluation, ...]:
        return tuple(
            sorted(
                self.evaluations,
                key=lambda item: (
                    not item.search_data_survivor,
                    -item.sequential_metrics.net_pnl_ticks,
                    -item.sequential_metrics.trade_count,
                    item.candidate.candidate_hash,
                ),
            )
        )

    @property
    def top_candidate(self) -> CandidateEvaluation | None:
        return self.ranked[0] if self.evaluations else None

    def as_dict(self) -> dict[str, object]:
        return {
            "evaluations": [item.as_dict() for item in self.ranked],
            "failures": [item.as_dict() for item in self.failures],
            "live_eligible": self.live_eligible,
            "null_experiments_attempted": self.null_experiments_attempted,
            "paper_eligible": self.paper_eligible,
            "real_experiments_attempted": self.real_experiments_attempted,
            "result_scope": self.result_scope,
            "sealed_holdout_untouched": self.sealed_holdout_untouched,
            "seed": self.seed,
        }


@dataclass(frozen=True, slots=True)
class NullCandidateEvaluation:
    """Standalone ledger payload for one explicit null candidate."""

    candidate_hash: str
    parent_candidate_hash: str
    control_id: str
    method: str
    raw_event_metrics: PerformanceMetrics
    flat_only_metrics: PerformanceMetrics
    sequential_metrics: PerformanceMetrics
    stressed_cost_metrics: PerformanceMetrics
    selection: Mapping[str, object]
    status: str = SCREENED_OUT_STATUS
    admitted: bool = False
    authority_status: str = AUTHORITY_STATUS
    paper_eligible: bool = False
    live_eligible: bool = False

    def as_dict(self) -> dict[str, object]:
        control = {
            "control_id": self.control_id,
            "method": self.method,
            "metrics": self.sequential_metrics.as_dict(),
            "selection": dict(self.selection),
        }
        return {
            "admitted": self.admitted,
            "authority_status": self.authority_status,
            "candidate_hash": self.candidate_hash,
            "control_id": self.control_id,
            "controls": {self.control_id: control},
            "flat_only_metrics": self.flat_only_metrics.as_dict(),
            "fold_metrics": [],
            "live_eligible": self.live_eligible,
            "method": self.method,
            "paper_eligible": self.paper_eligible,
            "parent_candidate_hash": self.parent_candidate_hash,
            "raw_event_metrics": self.raw_event_metrics.as_dict(),
            "result_scope": SEARCH_DATA_RESULT,
            "selection": dict(self.selection),
            "sequential_metrics": self.sequential_metrics.as_dict(),
            "status": self.status,
            "stressed_cost_metrics": self.stressed_cost_metrics.as_dict(),
        }


@dataclass(frozen=True, slots=True)
class _Observation:
    feature: EventFeature
    label: QuoteAwareLabel | None


def _empty_metrics(*, signal_count: int = 0, invalid_count: int = 0) -> PerformanceMetrics:
    return PerformanceMetrics(
        signal_count=signal_count,
        trade_count=0,
        invalid_count=invalid_count,
        skipped_occupied_count=0,
        tp_first_count=0,
        sl_first_count=0,
        timeout_count=0,
        ambiguous_count=0,
        raw_fallback_count=0,
        gross_pnl_ticks=0,
        cost_ticks=0,
        net_pnl_ticks=0,
        maximum_drawdown_ticks=0,
        active_days=0,
        session_distribution=(),
    )


def _validated_features(features: Sequence[EventFeature]) -> tuple[EventFeature, ...]:
    rows = tuple(features)
    if any(not isinstance(item, EventFeature) for item in rows):
        raise EvaluationError("features must contain only EventFeature values")
    ordered = tuple(sorted(rows, key=lambda item: (item.event_ts_ns, item.instrument_id)))
    keys = tuple((item.event_ts_ns, item.instrument_id) for item in ordered)
    if len(keys) != len(set(keys)):
        raise EvaluationError("feature event/instrument identity must be unique")
    return ordered


def _label_key(label: QuoteAwareLabel) -> tuple[int, int, str, str]:
    direction = (
        label.direction.value if isinstance(label.direction, Direction) else str(label.direction)
    )
    return label.event_ts_ns, label.instrument_id, direction, label.barrier_id


def _validated_label_index(
    labels: Sequence[QuoteAwareLabel],
) -> dict[tuple[int, int, str, str], QuoteAwareLabel]:
    index: dict[tuple[int, int, str, str], QuoteAwareLabel] = {}
    for label in labels:
        if not isinstance(label, QuoteAwareLabel):
            raise EvaluationError("labels must contain only QuoteAwareLabel values")
        key = _label_key(label)
        if key in index:
            raise EvaluationError(
                "label event/instrument/direction/barrier identity must be unique"
            )
        index[key] = label
    return index


def _barrier_matches(label: QuoteAwareLabel, barrier: BarrierSpec) -> bool:
    return (
        label.barrier_id == barrier.barrier_id
        and label.k_tp_num == barrier.k_tp_num
        and label.k_tp_den == barrier.k_tp_den
        and label.k_sl_num == barrier.k_sl_num
        and label.k_sl_den == barrier.k_sl_den
        and label.max_hold_seconds == barrier.max_hold_seconds
    )


def _lookup_label(
    candidate: StrategyCandidate,
    feature: EventFeature,
    label_index: Mapping[tuple[int, int, str, str], QuoteAwareLabel],
) -> QuoteAwareLabel | None:
    key = (
        feature.event_ts_ns,
        feature.instrument_id,
        candidate.direction.value,
        candidate.barrier.barrier_id,
    )
    label = label_index.get(key)
    if label is not None and not _barrier_matches(label, candidate.barrier):
        raise EvaluationError("label barrier parameters drift from candidate barrier identity")
    return label


def _candidate_observations(
    candidate: StrategyCandidate,
    features: tuple[EventFeature, ...],
    label_index: Mapping[tuple[int, int, str, str], QuoteAwareLabel],
) -> tuple[_Observation, ...]:
    return tuple(
        _Observation(feature=feature, label=_lookup_label(candidate, feature, label_index))
        for feature in features
        if candidate_signal(candidate, feature)
    )


def _trade_from_observation(
    candidate: StrategyCandidate,
    observation: _Observation,
    *,
    cost_numerator: int = 1,
    cost_denominator: int = 1,
) -> TradeRecord | None:
    label = observation.label
    feature = observation.feature
    if (
        label is None
        or not label.eligible
        or label.first_touch_type is FirstTouchType.INVALID
        or label.entry_ts_ns is None
        or label.entry_price_ticks is None
        or label.exit_ts_ns is None
        or label.exit_price_ticks is None
        or label.gross_pnl_ticks is None
    ):
        return None
    if label.event_ts_ns != feature.event_ts_ns or label.instrument_id != feature.instrument_id:
        raise EvaluationError("label identity differs from its feature row")
    if label.entry_ts_ns < feature.event_ts_ns or label.exit_ts_ns < label.entry_ts_ns:
        raise EvaluationError("eligible label timestamps are not chronological")
    if cost_numerator <= 0 or cost_denominator <= 0:
        raise EvaluationError("cost stress must be a positive rational")
    stressed_cost = (label.cost_ticks * cost_numerator + cost_denominator - 1) // cost_denominator
    net = label.gross_pnl_ticks - stressed_cost
    if cost_numerator == cost_denominator and (
        label.net_pnl_ticks is None or label.net_pnl_ticks != net
    ):
        raise EvaluationError("label net PnL differs from gross PnL less cost")
    return TradeRecord(
        candidate_hash=candidate.candidate_hash,
        event_ts_ns=feature.event_ts_ns,
        entry_ts_ns=label.entry_ts_ns,
        exit_ts_ns=label.exit_ts_ns,
        instrument_id=feature.instrument_id,
        session_id=feature.session_id,
        trading_date=feature.trading_date,
        direction=candidate.direction,
        barrier_id=candidate.barrier.barrier_id,
        outcome=label.first_touch_type,
        entry_price_ticks=label.entry_price_ticks,
        exit_price_ticks=label.exit_price_ticks,
        gross_pnl_ticks=label.gross_pnl_ticks,
        cost_ticks=stressed_cost,
        net_pnl_ticks=net,
        ambiguous=label.ambiguous,
        raw_fallback_used=label.raw_fallback_used,
    )


def _metrics(
    trades: Sequence[TradeRecord],
    *,
    signal_count: int,
    invalid_count: int,
    skipped_occupied_count: int,
) -> PerformanceMetrics:
    values = tuple(trades)
    if not values:
        return PerformanceMetrics(
            signal_count=signal_count,
            trade_count=0,
            invalid_count=invalid_count,
            skipped_occupied_count=skipped_occupied_count,
            tp_first_count=0,
            sl_first_count=0,
            timeout_count=0,
            ambiguous_count=0,
            raw_fallback_count=0,
            gross_pnl_ticks=0,
            cost_ticks=0,
            net_pnl_ticks=0,
            maximum_drawdown_ticks=0,
            active_days=0,
            session_distribution=(),
        )
    equity = 0
    peak = 0
    drawdown = 0
    for trade in values:
        equity += trade.net_pnl_ticks
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    sessions = Counter(item.session_id for item in values)
    return PerformanceMetrics(
        signal_count=signal_count,
        trade_count=len(values),
        invalid_count=invalid_count,
        skipped_occupied_count=skipped_occupied_count,
        tp_first_count=sum(item.outcome is FirstTouchType.TP_FIRST for item in values),
        sl_first_count=sum(item.outcome is FirstTouchType.SL_FIRST for item in values),
        timeout_count=sum(item.outcome is FirstTouchType.TIMEOUT for item in values),
        ambiguous_count=sum(item.ambiguous for item in values),
        raw_fallback_count=sum(item.raw_fallback_used for item in values),
        gross_pnl_ticks=sum(item.gross_pnl_ticks for item in values),
        cost_ticks=sum(item.cost_ticks for item in values),
        net_pnl_ticks=sum(item.net_pnl_ticks for item in values),
        maximum_drawdown_ticks=drawdown,
        active_days=len({item.trading_date for item in values}),
        session_distribution=tuple(sorted(sessions.items())),
    )


def _raw_metrics(
    candidate: StrategyCandidate,
    observations: Sequence[_Observation],
) -> PerformanceMetrics:
    trades = tuple(
        trade
        for observation in observations
        if (trade := _trade_from_observation(candidate, observation)) is not None
    )
    return _metrics(
        trades,
        signal_count=len(observations),
        invalid_count=len(observations) - len(trades),
        skipped_occupied_count=0,
    )


def _occupancy_select(
    candidate: StrategyCandidate,
    observations: Sequence[_Observation],
    *,
    cooldown_ns: int,
    cost_numerator: int = 1,
    cost_denominator: int = 1,
) -> tuple[tuple[TradeRecord, ...], PerformanceMetrics]:
    if cooldown_ns < 0:
        raise EvaluationError("cooldown_ns must be non-negative")
    accepted: list[TradeRecord] = []
    invalid = 0
    skipped = 0
    next_available_ts: int | None = None
    for observation in sorted(observations, key=lambda item: item.feature.event_ts_ns):
        trade = _trade_from_observation(
            candidate,
            observation,
            cost_numerator=cost_numerator,
            cost_denominator=cost_denominator,
        )
        if trade is None:
            invalid += 1
            continue
        if next_available_ts is not None and observation.feature.event_ts_ns < next_available_ts:
            skipped += 1
            continue
        if accepted and trade.entry_ts_ns < accepted[-1].exit_ts_ns + cooldown_ns:
            raise EvaluationError("accepted sequential trade intervals overlap")
        accepted.append(trade)
        next_available_ts = trade.exit_ts_ns + cooldown_ns
    metrics = _metrics(
        accepted,
        signal_count=len(observations),
        invalid_count=invalid,
        skipped_occupied_count=skipped,
    )
    return tuple(accepted), metrics


def simulate_sequential(
    candidate: StrategyCandidate,
    features: Sequence[EventFeature],
    labels: Sequence[QuoteAwareLabel],
    *,
    cooldown_seconds: int = 0,
) -> tuple[tuple[TradeRecord, ...], PerformanceMetrics]:
    """Public one-position simulator using candidate-specific label exit times."""

    if cooldown_seconds < 0:
        raise EvaluationError("cooldown_seconds must be non-negative")
    feature_rows = _validated_features(features)
    label_index = _validated_label_index(labels)
    observations = _candidate_observations(candidate, feature_rows, label_index)
    return _occupancy_select(
        candidate,
        observations,
        cooldown_ns=cooldown_seconds * ONE_SECOND_NS,
    )


def _partition_lengths(total: int, groups: int) -> tuple[int, ...]:
    quotient, remainder = divmod(total, groups)
    return tuple(quotient + int(index < remainder) for index in range(groups))


def _walk_forward(
    candidate: StrategyCandidate,
    observations: Sequence[_Observation],
    *,
    fold_count: int,
    purge_seconds: int,
    cooldown_ns: int,
) -> tuple[FoldResult, ...]:
    rows = tuple(sorted(observations, key=lambda item: item.feature.event_ts_ns))
    if len(rows) < 2:
        return ()
    requested = min(fold_count, len(rows) - 1)
    if requested <= 0:
        return ()
    initial = max(1, len(rows) // (requested + 1))
    remaining = len(rows) - initial
    requested = min(requested, remaining)
    lengths = _partition_lengths(remaining, requested)
    cursor = initial
    purge_ns = purge_seconds * ONE_SECOND_NS
    folds: list[FoldResult] = []
    for fold_number, length in enumerate(lengths, start=1):
        validation = rows[cursor : cursor + length]
        cursor += length
        if not validation:
            continue
        validation_start = validation[0].feature.event_ts_ns
        calibration = tuple(
            item
            for item in rows[: cursor - length]
            if item.feature.event_ts_ns < validation_start - purge_ns
        )
        _, calibration_metrics = _occupancy_select(
            candidate,
            calibration,
            cooldown_ns=cooldown_ns,
        )
        _, validation_metrics = _occupancy_select(
            candidate,
            validation,
            cooldown_ns=cooldown_ns,
        )
        folds.append(
            FoldResult(
                fold_number=fold_number,
                result_role=SEARCH_DATA_RESULT,
                purge_seconds=purge_seconds,
                calibration_start_ns=(
                    None if not calibration else calibration[0].feature.event_ts_ns
                ),
                calibration_end_ns=(
                    None if not calibration else calibration[-1].feature.event_ts_ns
                ),
                validation_start_ns=validation_start,
                validation_end_ns=validation[-1].feature.event_ts_ns,
                calibration_metrics=calibration_metrics,
                validation_metrics=validation_metrics,
            )
        )
    return tuple(folds)


def _selected_observations(
    candidate: StrategyCandidate,
    features: tuple[EventFeature, ...],
    label_index: Mapping[tuple[int, int, str, str], QuoteAwareLabel],
    indices: Sequence[int],
) -> tuple[_Observation, ...]:
    return tuple(
        _Observation(
            feature=features[index],
            label=_lookup_label(candidate, features[index], label_index),
        )
        for index in indices
    )


def _uplift(real: PerformanceMetrics, control: PerformanceMetrics) -> Decimal | None:
    if real.net_ev_ticks is None or control.net_ev_ticks is None:
        return None
    return real.net_ev_ticks - control.net_ev_ticks


def _controls(
    candidate: StrategyCandidate,
    features: tuple[EventFeature, ...],
    label_index: Mapping[tuple[int, int, str, str], QuoteAwareLabel],
    *,
    seed: int,
    block_size: int,
    cooldown_ns: int,
    real_metrics: PerformanceMetrics,
) -> tuple[ControlResult, ControlResult]:
    signal_mask = tuple(candidate_signal(candidate, feature) for feature in features)
    derived_seed = seed ^ int(candidate.candidate_hash[:16], 16)
    if len(features) >= 2:
        shifted: CircularShiftSelection = circular_block_shift(
            signal_mask,
            block_size=block_size,
            seed=derived_seed,
        )
        shifted_indices = tuple(
            index for index, selected in enumerate(shifted.shifted_mask) if selected
        )
        shifted_observations = _selected_observations(
            candidate,
            features,
            label_index,
            shifted_indices,
        )
        _, shifted_metrics = _occupancy_select(
            candidate,
            shifted_observations,
            cooldown_ns=cooldown_ns,
        )
        shifted_selection = shifted.as_dict()
    else:
        shifted_metrics = _empty_metrics()
        shifted_selection = {
            "block_size": block_size,
            "reason": "INSUFFICIENT_EVENT_ROWS",
            "shift_blocks": 0,
        }
    circular_result = ControlResult(
        control_id="circular_block_shift_v1",
        method="CIRCULAR_BLOCK_TIME_SHIFT",
        metrics=shifted_metrics,
        selection=shifted_selection,
        net_ev_uplift_ticks=_uplift(real_metrics, shifted_metrics),
    )

    signal_indices = tuple(index for index, selected in enumerate(signal_mask) if selected)
    eligible_indices = tuple(
        index
        for index, feature in enumerate(features)
        if (label := _lookup_label(candidate, feature, label_index)) is not None
        and label.eligible
        and feature.feature_valid
        and not feature.roll_cross
        and not feature.inside_roll_guard
    )
    matched: MatchedRandomSelection = matched_random_entries(
        features,
        signal_indices=signal_indices,
        eligible_indices=eligible_indices,
        seed=derived_seed ^ 0x9E3779B97F4A7C15,
    )
    matched_observations = _selected_observations(
        candidate,
        features,
        label_index,
        matched.selected_indices,
    )
    _, matched_metrics = _occupancy_select(
        candidate,
        matched_observations,
        cooldown_ns=cooldown_ns,
    )
    matched_result = ControlResult(
        control_id="matched_random_entry_v1",
        method="MATCHED_RANDOM_ENTRY",
        metrics=matched_metrics,
        selection={
            **matched.as_dict(),
            "direction": candidate.direction.value,
            "holding_horizon_seconds": candidate.barrier.max_hold_seconds,
            "matching_axes": ["month", "session", "volatility_regime"],
        },
        net_ev_uplift_ticks=_uplift(real_metrics, matched_metrics),
    )
    return circular_result, matched_result


def _admission(
    raw: PerformanceMetrics,
    sequential: PerformanceMetrics,
    folds: Sequence[FoldResult],
    rules: AdmissionRules,
) -> tuple[str, tuple[str, ...]]:
    reasons: list[str] = []
    if raw.trade_count < rules.min_raw_events:
        reasons.append("RAW_EVENT_SUPPORT_BELOW_MINIMUM")
    if sequential.trade_count < rules.min_sequential_trades:
        reasons.append("SEQUENTIAL_TRADE_SUPPORT_BELOW_MINIMUM")
    if sequential.active_days < rules.min_active_days:
        reasons.append("ACTIVE_DAY_SUPPORT_BELOW_MINIMUM")
    probability = sequential.tp_probability_ppm
    if probability is None or probability < rules.min_tp_probability_ppm:
        reasons.append("TP_FIRST_PROBABILITY_BELOW_SCREEN")
    if rules.require_positive_net_ev and (
        sequential.net_ev_ticks is None or sequential.net_ev_ticks <= 0
    ):
        reasons.append("NON_POSITIVE_NET_EV")
    positive_folds = sum(item.validation_metrics.net_pnl_ticks > 0 for item in folds)
    if positive_folds < rules.min_positive_folds:
        reasons.append("POSITIVE_SEARCH_FOLD_SUPPORT_BELOW_MINIMUM")
    return (SURVIVOR_STATUS if not reasons else SCREENED_OUT_STATUS), tuple(reasons)


def evaluate_candidate(
    candidate: StrategyCandidate,
    features: Sequence[EventFeature],
    labels: Sequence[QuoteAwareLabel],
    *,
    seed: int,
    cooldown_seconds: int = 0,
    feature_lookback_seconds: int = 3_600,
    purge_seconds: int | None = None,
    fold_count: int = 3,
    control_block_size: int = 4,
    stressed_cost_numerator: int = 3,
    stressed_cost_denominator: int = 2,
    admission_rules: AdmissionRules | Mapping[str, object] = DEFAULT_ADMISSION_RULES,
) -> CandidateEvaluation:
    """Run every M0a search-data stage for one immutable candidate."""

    if not isinstance(candidate, StrategyCandidate):
        raise EvaluationError("candidate must be a StrategyCandidate")
    if min(cooldown_seconds, feature_lookback_seconds) < 0:
        raise EvaluationError("cooldown and feature lookback must be non-negative")
    if isinstance(fold_count, bool) or fold_count <= 0:
        raise EvaluationError("fold_count must be positive")
    rules = (
        admission_rules
        if isinstance(admission_rules, AdmissionRules)
        else AdmissionRules.from_config(admission_rules)
    )
    required_purge = candidate.barrier.max_hold_seconds + feature_lookback_seconds
    effective_purge = required_purge if purge_seconds is None else purge_seconds
    if effective_purge < required_purge:
        raise EvaluationError("purge must cover max hold plus feature lookback")

    feature_rows = _validated_features(features)
    label_index = _validated_label_index(labels)
    observations = _candidate_observations(candidate, feature_rows, label_index)
    raw = _raw_metrics(candidate, observations)
    cooldown_ns = cooldown_seconds * ONE_SECOND_NS
    flat_trades, flat = _occupancy_select(
        candidate,
        observations,
        cooldown_ns=cooldown_ns,
    )
    # Stage A and the stateful simulator intentionally share label economics but
    # are executed independently.  The second pass asserts the actual trade
    # intervals and emits the durable chronological trade list.
    trades, sequential = _occupancy_select(
        candidate,
        observations,
        cooldown_ns=cooldown_ns,
    )
    if flat_trades != trades or flat != sequential:
        raise AssertionError("flat-only and deterministic one-position replay diverged")
    _, stressed = _occupancy_select(
        candidate,
        observations,
        cooldown_ns=cooldown_ns,
        cost_numerator=stressed_cost_numerator,
        cost_denominator=stressed_cost_denominator,
    )
    folds = _walk_forward(
        candidate,
        observations,
        fold_count=fold_count,
        purge_seconds=effective_purge,
        cooldown_ns=cooldown_ns,
    )
    circular, matched = _controls(
        candidate,
        feature_rows,
        label_index,
        seed=seed,
        block_size=control_block_size,
        cooldown_ns=cooldown_ns,
        real_metrics=sequential,
    )
    status, reasons = _admission(raw, sequential, folds, rules)
    return CandidateEvaluation(
        candidate=candidate,
        raw_event_metrics=raw,
        flat_only_metrics=flat,
        sequential_metrics=sequential,
        stressed_cost_metrics=stressed,
        trades=trades,
        folds=folds,
        circular_shift_control=circular,
        matched_random_control=matched,
        status=status,
        admission_reasons=reasons,
    )


def evaluate_null_candidate(
    null_candidate: NullControlCandidate,
    parent_candidate: StrategyCandidate,
    features: Sequence[EventFeature],
    labels: Sequence[QuoteAwareLabel],
    *,
    cooldown_seconds: int = 0,
    stressed_cost_numerator: int = 3,
    stressed_cost_denominator: int = 2,
) -> NullCandidateEvaluation:
    """Evaluate one preregistered null through the same label/simulator path.

    Signature is intentionally ledger-friendly: the null row carries its parent
    hash and seed; the caller reconstructs the immutable parent candidate and
    search data on restart.
    """

    if not isinstance(null_candidate, NullControlCandidate):
        raise EvaluationError("null_candidate must be a NullControlCandidate")
    if null_candidate.parent_candidate_hash != parent_candidate.candidate_hash:
        raise EvaluationError("null candidate parent identity differs from parent_candidate")
    feature_rows = _validated_features(features)
    label_index = _validated_label_index(labels)
    signal_mask = tuple(candidate_signal(parent_candidate, feature) for feature in feature_rows)
    signal_indices = tuple(index for index, selected in enumerate(signal_mask) if selected)
    cooldown_ns = cooldown_seconds * ONE_SECOND_NS

    if null_candidate.control_id == "circular_block_shift_v1":
        block_size = int(null_candidate.parameters["block_size"])
        shifted = circular_block_shift(
            signal_mask,
            block_size=block_size,
            seed=null_candidate.random_seed,
        )
        selected_indices = tuple(
            index for index, selected in enumerate(shifted.shifted_mask) if selected
        )
        selection: Mapping[str, object] = shifted.as_dict()
    elif null_candidate.control_id == "matched_random_entry_v1":
        eligible_indices = tuple(
            index
            for index, feature in enumerate(feature_rows)
            if (label := _lookup_label(parent_candidate, feature, label_index)) is not None
            and label.eligible
            and feature.feature_valid
            and not feature.roll_cross
            and not feature.inside_roll_guard
        )
        matched = matched_random_entries(
            feature_rows,
            signal_indices=signal_indices,
            eligible_indices=eligible_indices,
            seed=null_candidate.random_seed,
        )
        selected_indices = matched.selected_indices
        selection = {
            **matched.as_dict(),
            "direction": parent_candidate.direction.value,
            "holding_horizon_seconds": parent_candidate.barrier.max_hold_seconds,
            "matching_axes": ["month", "session", "volatility_regime"],
        }
    else:  # guarded by NullControlCandidate
        raise EvaluationError("unsupported null control")

    observations = _selected_observations(
        parent_candidate,
        feature_rows,
        label_index,
        selected_indices,
    )
    raw = _raw_metrics(parent_candidate, observations)
    _, flat = _occupancy_select(
        parent_candidate,
        observations,
        cooldown_ns=cooldown_ns,
    )
    _, sequential = _occupancy_select(
        parent_candidate,
        observations,
        cooldown_ns=cooldown_ns,
    )
    _, stressed = _occupancy_select(
        parent_candidate,
        observations,
        cooldown_ns=cooldown_ns,
        cost_numerator=stressed_cost_numerator,
        cost_denominator=stressed_cost_denominator,
    )
    return NullCandidateEvaluation(
        candidate_hash=null_candidate.candidate_hash,
        parent_candidate_hash=parent_candidate.candidate_hash,
        control_id=null_candidate.control_id,
        method=null_candidate.method,
        raw_event_metrics=raw,
        flat_only_metrics=flat,
        sequential_metrics=sequential,
        stressed_cost_metrics=stressed,
        selection=selection,
    )


def evaluate_epoch(
    candidates: Sequence[StrategyCandidate],
    features: Sequence[EventFeature],
    labels: Sequence[QuoteAwareLabel],
    *,
    seed: int,
    cooldown_seconds: int = 0,
    feature_lookback_seconds: int = 3_600,
    purge_seconds: int | None = None,
    fold_count: int = 3,
    control_block_size: int = 4,
    stressed_cost_numerator: int = 3,
    stressed_cost_denominator: int = 2,
    admission_rules: AdmissionRules | Mapping[str, object] = DEFAULT_ADMISSION_RULES,
) -> EpochEvaluation:
    """Evaluate a fixed candidate budget, isolating individual candidate failures."""

    values = tuple(candidates)
    hashes = tuple(item.candidate_hash for item in values)
    if len(hashes) != len(set(hashes)):
        raise EvaluationError("epoch candidate budget contains duplicate canonical hashes")
    evaluations: list[CandidateEvaluation] = []
    failures: list[CandidateFailure] = []
    for candidate in values:
        try:
            evaluations.append(
                evaluate_candidate(
                    candidate,
                    features,
                    labels,
                    seed=seed,
                    cooldown_seconds=cooldown_seconds,
                    feature_lookback_seconds=feature_lookback_seconds,
                    purge_seconds=purge_seconds,
                    fold_count=fold_count,
                    control_block_size=control_block_size,
                    stressed_cost_numerator=stressed_cost_numerator,
                    stressed_cost_denominator=stressed_cost_denominator,
                    admission_rules=admission_rules,
                )
            )
        except (M0aDataError, ValueError, ArithmeticError, AssertionError) as error:
            failures.append(
                CandidateFailure(
                    candidate_hash=candidate.candidate_hash,
                    error_type=type(error).__name__,
                    error=str(error),
                )
            )
    return EpochEvaluation(
        seed=seed,
        evaluations=tuple(evaluations),
        failures=tuple(failures),
        real_experiments_attempted=len(values),
        # The epoch precommits both null candidates for every real candidate.
        # A parent failure cannot adaptively reclaim or shrink that budget.
        null_experiments_attempted=2 * len(values),
    )


def assert_walking_skeleton(
    evaluation: EpochEvaluation,
    *,
    expected_real_budget: int,
    expected_null_budget: int,
) -> None:
    """Assert the M0a completion conditions after a fixed fixture epoch.

    This is an engineering invariant, not admission logic.  It cannot change a
    candidate result or allocate more search budget.
    """

    if evaluation.real_experiments_attempted != expected_real_budget:
        raise EvaluationError("walking skeleton did not attempt its exact real budget")
    if evaluation.null_experiments_attempted != expected_null_budget:
        raise EvaluationError("walking skeleton did not account for its exact null budget")
    if expected_null_budget != 2 * expected_real_budget:
        raise EvaluationError("M0a requires exactly two null controls per real candidate")
    if not any(item.search_data_survivor for item in evaluation.evaluations):
        raise EvaluationError("walking skeleton produced no search-data survivor")
    if not any(
        item.raw_event_metrics.trade_count > item.flat_only_metrics.trade_count
        for item in evaluation.evaluations
    ):
        raise EvaluationError("walking skeleton did not exercise exit_ts occupancy dedup")
    for result in evaluation.evaluations:
        if any(
            left.exit_ts_ns > right.entry_ts_ns
            for left, right in zip(result.trades, result.trades[1:])
        ):
            raise EvaluationError("walking skeleton produced overlapping sequential trades")
    survivor = next(item for item in evaluation.evaluations if item.search_data_survivor)
    if not survivor.folds:
        raise EvaluationError("walking-skeleton survivor lacks search-data folds")
    if not survivor.circular_shift_control or not survivor.matched_random_control:
        raise EvaluationError("walking-skeleton survivor lacks both null controls")


def generate_and_evaluate_epoch(
    *,
    candidate_budget: int,
    candidate_seed: int,
    barriers: Sequence[BarrierSpec],
    features: Sequence[EventFeature],
    labels: Sequence[QuoteAwareLabel],
    evaluation_seed: int | None = None,
    **evaluation_options: object,
) -> EpochEvaluation:
    """Pipeline-facing fixed-budget generation plus evaluation convenience API."""

    candidates = generate_candidates(
        budget=candidate_budget,
        seed=candidate_seed,
        barriers=barriers,
    )
    allowed = {
        "admission_rules",
        "control_block_size",
        "cooldown_seconds",
        "feature_lookback_seconds",
        "fold_count",
        "purge_seconds",
        "stressed_cost_denominator",
        "stressed_cost_numerator",
    }
    unknown = sorted(set(evaluation_options) - allowed)
    if unknown:
        raise EvaluationError(f"unknown evaluation options: {unknown}")
    return evaluate_epoch(
        candidates,
        features,
        labels,
        seed=candidate_seed if evaluation_seed is None else evaluation_seed,
        **evaluation_options,  # type: ignore[arg-type]
    )
