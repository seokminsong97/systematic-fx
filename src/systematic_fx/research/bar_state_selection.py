"""Frozen OOS gates, full-cell multiplicity, and finalist selection for State V2."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from fractions import Fraction
from statistics import median
from typing import Final

import numpy as np

from systematic_fx.research.bar_state_model import StateTradeDecision
from systematic_fx.research.bar_state_portfolio import (
    STATE_VOLATILITY_MULTIPLIERS,
    StateAxisResolutionSummary,
    StatePortfolioCellSummary,
    StatePortfolioReplaySummary,
    StatePortfolioSignal,
)
from systematic_fx.validation.bar_state_splits import (
    BarStateSplitPlan,
    require_frozen_bar_state_split,
)

STATE_SELECTION_SCHEMA: Final = "systematic_fx.bar_state_selection.v1"
EXPECTED_STATE_CANDIDATES: Final = 12
STATE_CELL_COUNT: Final = 49
PREDECESSOR_VARIANT_COUNT: Final = 216
BH_FAMILY_SIZE: Final = PREDECESSOR_VARIANT_COUNT + EXPECTED_STATE_CANDIDATES * STATE_CELL_COUNT
BH_Q: Final = Fraction(1, 20)
BOOTSTRAP_REPLICATES: Final = 10_000
BOOTSTRAP_MEAN_BLOCK_ACTIVE_DAYS: Final = 10
BOOTSTRAP_RANDOM_SEED: Final = 20_260_809
BOOTSTRAP_LCB_ORDER_INDEX: Final = 499
MAXIMUM_FINALISTS: Final = 4
MINIMUM_UNIQUE_AXIS_VECTORS: Final = 4
DISCOVERY_INNER_FOLD_KEYS: Final = (
    "discovery_inner_1",
    "discovery_inner_2",
    "discovery_inner_3",
)


class BarStateSelectionError(ValueError):
    """OOS evidence is incomplete or differs from the frozen gate contract."""


@dataclass(frozen=True, slots=True)
class StateCandidateSupport:
    candidate_key: str
    timeframe_seconds: int
    raw_directional_signal_count: int
    distinct_signal_day_count: int
    raw_signal_count_by_fold: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        if not self.candidate_key or self.timeframe_seconds not in {300, 1_800}:
            raise BarStateSelectionError("candidate support identity is invalid")
        if self.raw_directional_signal_count < 0 or self.distinct_signal_day_count < 0:
            raise BarStateSelectionError("candidate support counts cannot be negative")
        if tuple(key for key, _ in self.raw_signal_count_by_fold) != (DISCOVERY_INNER_FOLD_KEYS):
            raise BarStateSelectionError("candidate fold counts differ from exact inner folds")
        if sum(value for _, value in self.raw_signal_count_by_fold) != (
            self.raw_directional_signal_count
        ):
            raise BarStateSelectionError("candidate fold counts do not sum to raw signals")


def summarize_candidate_support(
    signals: Sequence[StatePortfolioSignal],
    *,
    timeframe_by_candidate: Mapping[str, int],
) -> tuple[StateCandidateSupport, ...]:
    """Count pre-occupancy LONG/SHORT OOS decisions for support gates."""

    by_candidate: dict[str, list[StatePortfolioSignal]] = defaultdict(list)
    for signal in signals:
        if signal.candidate_key not in timeframe_by_candidate:
            raise BarStateSelectionError("signal candidate is absent from timeframe mapping")
        if signal.decision is not StateTradeDecision.NO_TRADE:
            by_candidate[signal.candidate_key].append(signal)
    result: list[StateCandidateSupport] = []
    for candidate_key in sorted(timeframe_by_candidate):
        values = by_candidate[candidate_key]
        fold_counts: dict[str, int] = dict.fromkeys(DISCOVERY_INNER_FOLD_KEYS, 0)
        for signal in values:
            if signal.fold_key not in fold_counts:
                raise BarStateSelectionError("signal fold key is outside Discovery inner folds")
            fold_counts[signal.fold_key] += 1
        result.append(
            StateCandidateSupport(
                candidate_key=candidate_key,
                timeframe_seconds=timeframe_by_candidate[candidate_key],
                raw_directional_signal_count=len(values),
                distinct_signal_day_count=len({item.signal_active_date for item in values}),
                raw_signal_count_by_fold=tuple(fold_counts.items()),
            )
        )
    return tuple(result)


@dataclass(frozen=True, slots=True)
class StateFoldEvaluationCalendar:
    fold_key: str
    active_dates: tuple[date, ...]

    def __post_init__(self) -> None:
        if not self.fold_key:
            raise BarStateSelectionError("fold calendar key must be non-empty")
        if not self.active_dates or tuple(sorted(set(self.active_dates))) != self.active_dates:
            raise BarStateSelectionError("fold active dates must be sorted and unique")
        if any(
            isinstance(value, datetime) or not isinstance(value, date)
            for value in self.active_dates
        ):
            raise BarStateSelectionError("fold calendar must contain dates")


def _frozen_fold_evaluation_calendars(
    split_plan: BarStateSplitPlan,
) -> tuple[StateFoldEvaluationCalendar, ...]:
    if not isinstance(split_plan, BarStateSplitPlan):
        raise BarStateSelectionError("split_plan must be BarStateSplitPlan")
    require_frozen_bar_state_split(split_plan)
    calendars: list[StateFoldEvaluationCalendar] = []
    for key, fold in zip(DISCOVERY_INNER_FOLD_KEYS, split_plan.inner_folds, strict=True):
        start = fold.oos_decisions.start_active_ordinal - 1
        end = fold.outcome_tail.end_active_ordinal
        values = split_plan.eligible_dates[start:end]
        if (
            not values
            or values[0] != fold.oos_decisions.start_date
            or values[-1] != fold.outcome_tail.end_date
        ):
            raise BarStateSelectionError("inner fold evaluation calendar differs from split")
        calendars.append(StateFoldEvaluationCalendar(key, values))
    result = tuple(calendars)
    if tuple(len(item.active_dates) for item in result) != (117, 117, 137):
        raise BarStateSelectionError("frozen fold evaluation lengths differ from 117/117/137")
    if tuple(value for item in result for value in item.active_dates) != tuple(
        sorted({value for item in result for value in item.active_dates})
    ):
        raise BarStateSelectionError("frozen fold evaluation calendars overlap or reorder")
    return result


@dataclass(frozen=True, slots=True)
class StateCellMultiplicityResult:
    candidate_key: str
    take_profit_index: int
    stop_loss_index: int
    raw_p_value: Fraction
    adjusted_p_value: Fraction
    bh_rejected: bool
    deterministic_gate_passed: bool
    bootstrap_lower_bound_ev_ticks: Fraction | None
    rejection_reasons: tuple[str, ...]

    @property
    def canonical_key(self) -> tuple[str, int, int]:
        return self.candidate_key, self.take_profit_index, self.stop_loss_index


@dataclass(frozen=True, slots=True)
class StateCandidateSelection:
    candidate_key: str
    final_label: str
    selected_take_profit_index: int | None
    selected_stop_loss_index: int | None
    selected_take_profit_multiplier: Fraction | None
    selected_stop_loss_multiplier: Fraction | None
    positive_component_size: int
    positive_inner_fold_count: int
    worst_fold_moderate_ev_ticks: Decimal | None
    moderate_ev_ticks: Decimal | None
    bootstrap_lower_bound_ev_ticks: Fraction | None
    maximum_drawdown_ticks: int | None
    rejection_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class StateSelectionProgress:
    completed_bootstrap_cell_count: int
    total_state_cell_count: int = EXPECTED_STATE_CANDIDATES * STATE_CELL_COUNT


@dataclass(frozen=True, slots=True)
class StateFinalistRank:
    candidate_key: str
    positive_fold_count: int
    worst_fold_ev_ticks: Decimal
    bootstrap_lower_bound_ev_ticks: Fraction
    overall_moderate_ev_ticks: Decimal
    maximum_drawdown_ticks: int
    stop_loss_index: int
    take_profit_index: int


def finalist_rank_key(value: StateFinalistRank) -> tuple[object, ...]:
    """Ascending sort key implementing the exact four-finalist tie policy."""

    lower = Decimal(value.bootstrap_lower_bound_ev_ticks.numerator) / Decimal(
        value.bootstrap_lower_bound_ev_ticks.denominator
    )
    return (
        -value.positive_fold_count,
        -value.worst_fold_ev_ticks,
        -lower,
        -value.overall_moderate_ev_ticks,
        value.maximum_drawdown_ticks,
        value.stop_loss_index,
        value.take_profit_index,
        value.candidate_key,
    )


@dataclass(frozen=True, slots=True)
class StateSelectionResult:
    candidate_results: tuple[StateCandidateSelection, ...]
    multiplicity_results: tuple[StateCellMultiplicityResult, ...]
    finalist_keys: tuple[str, ...]
    bh_family_size: int = BH_FAMILY_SIZE
    bootstrap_convention: str = "FOLD_LOCAL_STATIONARY_PCG64_ALIGNED_EXIT_DAY_NET_AND_FILL_COUNTS"

    def __post_init__(self) -> None:
        if len(self.candidate_results) != EXPECTED_STATE_CANDIDATES:
            raise BarStateSelectionError("selection must retain all twelve candidates")
        if len(self.multiplicity_results) != EXPECTED_STATE_CANDIDATES * STATE_CELL_COUNT:
            raise BarStateSelectionError("selection must retain all 588 State cells")
        candidate_keys = tuple(item.candidate_key for item in self.candidate_results)
        if candidate_keys != tuple(sorted(set(candidate_keys))):
            raise BarStateSelectionError("candidate results must use unique canonical order")
        expected_cells = {
            (candidate_key, tp_index, sl_index)
            for candidate_key in candidate_keys
            for tp_index in range(7)
            for sl_index in range(7)
        }
        if {item.canonical_key for item in self.multiplicity_results} != expected_cells:
            raise BarStateSelectionError("multiplicity results differ from the full cell ledger")
        if len(self.finalist_keys) > MAXIMUM_FINALISTS or len(set(self.finalist_keys)) != len(
            self.finalist_keys
        ):
            raise BarStateSelectionError("selection exceeds the four-finalist cap")
        labeled_finalists = {
            item.candidate_key for item in self.candidate_results if item.final_label == "FINALIST"
        }
        if set(self.finalist_keys) != labeled_finalists:
            raise BarStateSelectionError("finalist keys differ from candidate terminal labels")
        if any(item.final_label not in {"FINALIST", "REJECTED"} for item in self.candidate_results):
            raise BarStateSelectionError("candidate final label is invalid")
        if self.bh_family_size != BH_FAMILY_SIZE:
            raise BarStateSelectionError("selection BH family size drift")


@dataclass(frozen=True, slots=True)
class _BootstrapResult:
    lower_bound_ev_ticks: Fraction | None
    p_value: Fraction


@dataclass(frozen=True, slots=True)
class _CellState:
    candidate_key: str
    tp_index: int
    sl_index: int
    baseline: StatePortfolioCellSummary
    moderate: StatePortfolioCellSummary
    severe: StatePortfolioCellSummary
    component: frozenset[tuple[int, int]]
    deterministic_reasons: tuple[str, ...]
    bootstrap: _BootstrapResult | None

    @property
    def deterministic_passed(self) -> bool:
        return not self.deterministic_reasons


def _support_reasons(value: StateCandidateSupport) -> tuple[str, ...]:
    if value.timeframe_seconds == 300:
        minimum_raw, minimum_fold, minimum_days = 160, 25, 40
    else:
        minimum_raw, minimum_fold, minimum_days = 100, 15, 35
    reasons: list[str] = []
    if value.raw_directional_signal_count < minimum_raw:
        reasons.append("SUPPORT_RAW_SIGNALS")
    if len(value.raw_signal_count_by_fold) != 3 or any(
        count < minimum_fold for _, count in value.raw_signal_count_by_fold
    ):
        reasons.append("SUPPORT_SIGNALS_PER_FOLD")
    if value.distinct_signal_day_count < minimum_days:
        reasons.append("SUPPORT_DISTINCT_SIGNAL_DAYS")
    return tuple(reasons)


def _cell_maps(
    summary: StatePortfolioReplaySummary,
) -> dict[str, dict[str, dict[tuple[int, int], StatePortfolioCellSummary]]]:
    result: dict[str, dict[str, dict[tuple[int, int], StatePortfolioCellSummary]]] = {}
    tp_index = {value: index for index, value in enumerate(STATE_VOLATILITY_MULTIPLIERS)}
    for cell in summary.cells:
        try:
            coordinate = tp_index[cell.take_profit_multiplier], tp_index[cell.stop_loss_multiplier]
        except KeyError as error:
            raise BarStateSelectionError("portfolio cell multiplier is outside the grid") from error
        scenario_map = result.setdefault(cell.candidate_key, {}).setdefault(cell.scenario_id, {})
        if coordinate in scenario_map:
            raise BarStateSelectionError("duplicate portfolio cell coordinate")
        scenario_map[coordinate] = cell
    expected = {(tp, sl) for tp in range(7) for sl in range(7)}
    for candidate_key in summary.candidate_keys:
        if set(result.get(candidate_key, {})) != {
            "BASELINE",
            "MODERATE_COMBINED",
            "SEVERE_DIAGNOSTIC",
        }:
            raise BarStateSelectionError("candidate is missing a scenario surface")
        if any(set(values) != expected for values in result[candidate_key].values()):
            raise BarStateSelectionError("candidate scenario surface is incomplete")
    return result


def _components(values: set[tuple[int, int]]) -> tuple[frozenset[tuple[int, int]], ...]:
    remaining = set(values)
    result: list[frozenset[tuple[int, int]]] = []
    while remaining:
        start = min(remaining)
        stack = [start]
        component: set[tuple[int, int]] = set()
        while stack:
            current = stack.pop()
            if current not in remaining:
                continue
            remaining.remove(current)
            component.add(current)
            tp, sl = current
            stack.extend(
                item
                for item in ((tp - 1, sl), (tp + 1, sl), (tp, sl - 1), (tp, sl + 1))
                if item in remaining
            )
        result.append(frozenset(component))
    return tuple(sorted(result, key=lambda item: (-len(item), tuple(sorted(item)))))


def _eligible_promotion_components(
    values: set[tuple[int, int]],
) -> tuple[frozenset[tuple[int, int]], ...]:
    """Retain only post-BH four-neighbor components large enough to promote."""

    return tuple(component for component in _components(values) if len(component) >= 9)


def _ratio_share(values: Sequence[int]) -> Fraction | None:
    total = sum(values)
    return None if total <= 0 else Fraction(max(values, default=0), total)


def _cell_gate_reasons(
    coordinate: tuple[int, int],
    *,
    baseline: StatePortfolioCellSummary,
    moderate: StatePortfolioCellSummary,
    severe: StatePortfolioCellSummary,
    positive_component: frozenset[tuple[int, int]],
    baseline_by_coordinate: Mapping[tuple[int, int], StatePortfolioCellSummary],
    moderate_by_coordinate: Mapping[tuple[int, int], StatePortfolioCellSummary],
    support_reasons: Sequence[str],
    axis_resolution: StateAxisResolutionSummary,
) -> tuple[str, ...]:
    reasons = list(support_reasons)
    if axis_resolution.unique_axis_vector_count < MINIMUM_UNIQUE_AXIS_VECTORS:
        reasons.append("GRID_AXIS_VECTOR_COLLAPSE")
    if baseline.fully_loaded_net_ev_ticks is None or baseline.fully_loaded_net_ev_ticks <= 0:
        reasons.append("BASELINE_NET_EV")
    if len(positive_component) < 9:
        reasons.append("POSITIVE_COMPONENT_SIZE")
    tp, sl = coordinate
    neighborhood = {
        (other_tp, other_sl)
        for other_tp in range(max(0, tp - 1), min(7, tp + 2))
        for other_sl in range(max(0, sl - 1), min(7, sl + 2))
    }
    positive_neighbors = {
        item
        for item in neighborhood
        if baseline_by_coordinate[item].fully_loaded_net_ev_ticks is not None
        and baseline_by_coordinate[item].fully_loaded_net_ev_ticks > 0
        and moderate_by_coordinate[item].fully_loaded_net_ev_ticks is not None
        and moderate_by_coordinate[item].fully_loaded_net_ev_ticks > 0
        and moderate_by_coordinate[item].fully_loaded_net_pnl_ticks > 0
    }
    if len(positive_neighbors) < 7:
        reasons.append("POSITIVE_3X3_STABILITY")
    neighbor_evs = [
        moderate_by_coordinate[item].fully_loaded_net_ev_ticks
        for item in sorted(neighborhood - {coordinate})
        if moderate_by_coordinate[item].fully_loaded_net_ev_ticks is not None
    ]
    if (
        moderate.fully_loaded_net_ev_ticks is None
        or not neighbor_evs
        or median(neighbor_evs) < moderate.fully_loaded_net_ev_ticks * Decimal("0.5")
    ):
        reasons.append("NEIGHBOR_MEDIAN_EV")
    if moderate.entry_fill_count < 40:
        reasons.append("MINIMUM_FILLED_ROUND_TRIPS")
    if len(moderate.blocks) != 3 or any(item.entry_fill_count < 8 for item in moderate.blocks):
        reasons.append("MINIMUM_FILLS_PER_FOLD")
    positive_folds = sum(item.fully_loaded_net_pnl_ticks > 0 for item in moderate.blocks)
    if positive_folds < 2:
        reasons.append("MINIMUM_POSITIVE_FOLDS")
    if moderate.fully_loaded_net_pnl_ticks <= 0:
        reasons.append("MODERATE_NET_PNL")
    if moderate.calendar_month_net_pnl_usd <= 0:
        reasons.append("MODERATE_CALENDAR_NET_PNL")
    if moderate.profit_factor is None or moderate.profit_factor < Decimal("1.1"):
        reasons.append("MODERATE_PROFIT_FACTOR")
    block_evs = [item.fully_loaded_net_ev_ticks for item in moderate.blocks]
    if any(value is None for value in block_evs) or min(
        value for value in block_evs if value is not None
    ) < Decimal(-2):
        reasons.append("MODERATE_WORST_FOLD_EV")
    if severe.fully_loaded_net_ev_ticks is None or severe.fully_loaded_net_ev_ticks < 0:
        reasons.append("SEVERE_NET_EV")
    fold_share = _ratio_share([item.positive_gross_ticks for item in moderate.blocks])
    contract_share = _ratio_share([value for _, value in moderate.positive_gross_by_contract])
    if fold_share is None or fold_share > Fraction(1, 2):
        reasons.append("FOLD_POSITIVE_GROSS_CONCENTRATION")
    if contract_share is None or contract_share > Fraction(1, 2):
        reasons.append("CONTRACT_POSITIVE_GROSS_CONCENTRATION")
    return tuple(dict.fromkeys(reasons))


def _stationary_weights(
    fold_lengths: Sequence[int],
) -> tuple[np.ndarray, ...]:
    if tuple(fold_lengths) != (117, 117, 137):
        raise BarStateSelectionError("Discovery bootstrap fold lengths must be 117/117/137")
    generator = np.random.Generator(np.random.PCG64(BOOTSTRAP_RANDOM_SEED))
    result: list[np.ndarray] = []
    replicate_ids = np.arange(BOOTSTRAP_REPLICATES)
    for length in fold_lengths:
        indices = generator.integers(0, length, size=BOOTSTRAP_REPLICATES)
        weights = np.zeros((BOOTSTRAP_REPLICATES, length), dtype=np.int16)
        for _ in range(length):
            np.add.at(weights, (replicate_ids, indices), 1)
            restart = generator.random(BOOTSTRAP_REPLICATES) < Fraction(
                1, BOOTSTRAP_MEAN_BLOCK_ACTIVE_DAYS
            )
            fresh = generator.integers(0, length, size=BOOTSTRAP_REPLICATES)
            indices = np.where(restart, fresh, (indices + 1) % length)
        result.append(weights)
    return tuple(result)


def _aligned_daily_vectors(
    cell: StatePortfolioCellSummary,
    calendars: Sequence[StateFoldEvaluationCalendar],
) -> tuple[tuple[np.ndarray, np.ndarray], ...]:
    net = dict(cell.daily_net_pnl_ticks)
    fills = dict(cell.daily_fill_count)
    if set(net) != set(fills) or any(value <= 0 for value in fills.values()):
        raise BarStateSelectionError("cell daily net/fill keys are not exactly aligned")
    visible = {value for calendar in calendars for value in calendar.active_dates}
    if not set(net).issubset(visible) or not set(fills).issubset(visible):
        raise BarStateSelectionError("cell daily evidence falls outside evaluation calendars")
    return tuple(
        (
            np.asarray([net.get(value, 0) for value in calendar.active_dates], dtype=np.int64),
            np.asarray([fills.get(value, 0) for value in calendar.active_dates], dtype=np.int64),
        )
        for calendar in calendars
    )


def _bootstrap_cell(
    cell: StatePortfolioCellSummary,
    *,
    calendars: Sequence[StateFoldEvaluationCalendar],
    weights: Sequence[np.ndarray],
) -> _BootstrapResult:
    aligned = _aligned_daily_vectors(cell, calendars)
    observed_net = sum(int(values.sum()) for values, _ in aligned)
    observed_fills = sum(int(values.sum()) for _, values in aligned)
    if observed_fills != cell.entry_fill_count or observed_fills <= 0:
        raise BarStateSelectionError("cell daily fill evidence differs from aggregate fills")
    if observed_net != cell.fully_loaded_net_pnl_ticks:
        raise BarStateSelectionError("cell daily net evidence differs from aggregate net PnL")
    boot_net = sum(matrix @ net for matrix, (net, _fills) in zip(weights, aligned, strict=True))
    boot_fills = sum(matrix @ fills for matrix, (_net, fills) in zip(weights, aligned, strict=True))
    ratios: list[Fraction | None] = [
        None if fill_count == 0 else Fraction(int(net_value), int(fill_count))
        for net_value, fill_count in zip(boot_net, boot_fills, strict=True)
    ]
    ordered = sorted(ratios, key=lambda value: (value is not None, value or Fraction()))
    lower = ordered[BOOTSTRAP_LCB_ORDER_INDEX]
    zero_fill = boot_fills == 0
    # Centered-null statistic compared with the observed EV, using exact
    # cross-multiplication: (boot_net*F-N*boot_fills)/(F*boot_fills) >= N/F.
    exceed = zero_fill | (boot_net * observed_fills >= 2 * observed_net * boot_fills)
    p_value = Fraction(1 + int(np.count_nonzero(exceed)), BOOTSTRAP_REPLICATES + 1)
    return _BootstrapResult(lower_bound_ev_ticks=lower, p_value=p_value)


def _bh_adjust(
    raw_state_p: Mapping[tuple[str, int, int], Fraction],
) -> dict[tuple[str, int, int], tuple[Fraction, bool]]:
    if len(raw_state_p) != EXPECTED_STATE_CANDIDATES * STATE_CELL_COUNT:
        raise BarStateSelectionError("BH state-cell ledger must contain exactly 588 cells")
    family: list[tuple[Fraction, tuple[str, int, int] | None, str]] = [
        (Fraction(1), None, f"predecessor_{index:03d}")
        for index in range(PREDECESSOR_VARIANT_COUNT)
    ]
    family.extend(
        (value, key, f"state:{key[0]}:{key[1]}:{key[2]}")
        for key, value in sorted(raw_state_p.items())
    )
    ordered = sorted(family, key=lambda item: (item[0], item[2]))
    cutoff: Fraction | None = None
    for rank, (value, _key, _stable) in enumerate(ordered, start=1):
        if value <= BH_Q * rank / BH_FAMILY_SIZE:
            cutoff = value
    adjusted_by_stable: dict[str, Fraction] = {}
    running = Fraction(1)
    for rank in range(BH_FAMILY_SIZE, 0, -1):
        value, _key, stable = ordered[rank - 1]
        running = min(running, value * BH_FAMILY_SIZE / rank)
        adjusted_by_stable[stable] = min(Fraction(1), running)
    return {
        key: (
            adjusted_by_stable[f"state:{key[0]}:{key[1]}:{key[2]}"],
            cutoff is not None and value <= cutoff,
        )
        for key, value in raw_state_p.items()
    }


def _medoid(component: frozenset[tuple[int, int]]) -> tuple[tuple[int, int], ...]:
    scores = {
        value: sum(abs(value[0] - other[0]) + abs(value[1] - other[1]) for other in component)
        for value in component
    }
    minimum = min(scores.values())
    return tuple(sorted(value for value, score in scores.items() if score == minimum))


def _cell_rank(state: _CellState) -> tuple[object, ...]:
    block_evs = tuple(item.fully_loaded_net_ev_ticks for item in state.moderate.blocks)
    worst = min(value for value in block_evs if value is not None)
    lower = state.bootstrap.lower_bound_ev_ticks if state.bootstrap is not None else None
    return (
        worst,
        state.moderate.fully_loaded_net_ev_ticks or Decimal("-Infinity"),
        Decimal(lower.numerator) / Decimal(lower.denominator)
        if lower is not None
        else Decimal("-Infinity"),
        -state.moderate.maximum_drawdown_ticks,
        -state.sl_index,
        -state.tp_index,
        state.candidate_key,
    )


def select_state_finalists(
    portfolio: StatePortfolioReplaySummary,
    *,
    candidate_order: Sequence[str],
    supports: Sequence[StateCandidateSupport],
    split_plan: BarStateSplitPlan,
    progress: Callable[[StateSelectionProgress], None] | None = None,
) -> StateSelectionResult:
    """Apply all frozen gates and return at most four Discovery finalists."""

    candidates = tuple(candidate_order)
    if len(candidates) != EXPECTED_STATE_CANDIDATES or len(set(candidates)) != len(candidates):
        raise BarStateSelectionError("candidate_order must contain the exact twelve candidates")
    if candidates != tuple(sorted(candidates)):
        raise BarStateSelectionError("candidate_order must use canonical key order")
    if portfolio.candidate_keys != candidates:
        raise BarStateSelectionError("portfolio candidates differ from candidate_order")
    support_by_key = {item.candidate_key: item for item in supports}
    if set(support_by_key) != set(candidates) or len(supports) != len(candidates):
        raise BarStateSelectionError("support evidence differs from the candidate catalog")
    calendars = _frozen_fold_evaluation_calendars(split_plan)
    maps = _cell_maps(portfolio)
    axis_by_key = {item.candidate_key: item for item in portfolio.axis_resolutions}
    weights: tuple[np.ndarray, ...] | None = None
    states: dict[tuple[str, int, int], _CellState] = {}
    bootstrapped = 0
    for candidate_key in candidates:
        baseline_map = maps[candidate_key]["BASELINE"]
        moderate_map = maps[candidate_key]["MODERATE_COMBINED"]
        positive = {
            coordinate
            for coordinate, cell in baseline_map.items()
            if cell.fully_loaded_net_ev_ticks is not None
            and cell.fully_loaded_net_ev_ticks > 0
            and moderate_map[coordinate].fully_loaded_net_ev_ticks is not None
            and moderate_map[coordinate].fully_loaded_net_ev_ticks > 0
            and moderate_map[coordinate].fully_loaded_net_pnl_ticks > 0
        }
        components = _components(positive)
        component_by_coordinate = {
            coordinate: component for component in components for coordinate in component
        }
        for tp_index in range(7):
            for sl_index in range(7):
                coordinate = tp_index, sl_index
                baseline = baseline_map[coordinate]
                moderate = maps[candidate_key]["MODERATE_COMBINED"][coordinate]
                severe = maps[candidate_key]["SEVERE_DIAGNOSTIC"][coordinate]
                component = component_by_coordinate.get(coordinate, frozenset())
                reasons = _cell_gate_reasons(
                    coordinate,
                    baseline=baseline,
                    moderate=moderate,
                    severe=severe,
                    positive_component=component,
                    baseline_by_coordinate=baseline_map,
                    moderate_by_coordinate=moderate_map,
                    support_reasons=_support_reasons(support_by_key[candidate_key]),
                    axis_resolution=axis_by_key[candidate_key],
                )
                bootstrap = None
                if not reasons:
                    if weights is None:
                        weights = _stationary_weights(
                            [len(item.active_dates) for item in calendars]
                        )
                    bootstrap = _bootstrap_cell(moderate, calendars=calendars, weights=weights)
                    bootstrapped += 1
                    if bootstrap.lower_bound_ev_ticks is None or (
                        bootstrap.lower_bound_ev_ticks <= 0
                    ):
                        reasons = (*reasons, "BOOTSTRAP_LOWER_BOUND")
                    if progress is not None:
                        progress(StateSelectionProgress(bootstrapped))
                states[candidate_key, tp_index, sl_index] = _CellState(
                    candidate_key=candidate_key,
                    tp_index=tp_index,
                    sl_index=sl_index,
                    baseline=baseline,
                    moderate=moderate,
                    severe=severe,
                    component=component,
                    deterministic_reasons=reasons,
                    bootstrap=bootstrap,
                )

    raw_p = {
        key: (
            state.bootstrap.p_value
            if state.bootstrap is not None and not state.deterministic_reasons
            else Fraction(1)
        )
        for key, state in states.items()
    }
    adjusted = _bh_adjust(raw_p)
    multiplicity = tuple(
        StateCellMultiplicityResult(
            candidate_key=key[0],
            take_profit_index=key[1],
            stop_loss_index=key[2],
            raw_p_value=raw_p[key],
            adjusted_p_value=adjusted[key][0],
            bh_rejected=adjusted[key][1],
            deterministic_gate_passed=not state.deterministic_reasons,
            bootstrap_lower_bound_ev_ticks=(
                None if state.bootstrap is None else state.bootstrap.lower_bound_ev_ticks
            ),
            rejection_reasons=state.deterministic_reasons,
        )
        for key, state in sorted(states.items())
    )
    bh_pass = {item.canonical_key for item in multiplicity if item.bh_rejected}

    chosen: dict[str, tuple[_CellState, int]] = {}
    candidate_reasons: dict[str, tuple[str, ...]] = {}
    for candidate_key in candidates:
        eligible = {
            (tp, sl)
            for tp in range(7)
            for sl in range(7)
            if (candidate_key, tp, sl) in bh_pass
            and not states[candidate_key, tp, sl].deterministic_reasons
        }
        eligible_components = _eligible_promotion_components(eligible)
        if not eligible_components:
            reasons = sorted(
                {
                    reason
                    for tp in range(7)
                    for sl in range(7)
                    for reason in states[candidate_key, tp, sl].deterministic_reasons
                }
            )
            if not any((candidate_key, tp, sl) in bh_pass for tp in range(7) for sl in range(7)):
                reasons.append("BH_MULTIPLICITY")
            else:
                reasons.append("POST_BH_COMPONENT_SIZE")
            candidate_reasons[candidate_key] = tuple(dict.fromkeys(reasons))
            continue
        largest_size = max(len(item) for item in eligible_components)
        medoid_states = [
            states[candidate_key, *coordinate]
            for component in eligible_components
            if len(component) == largest_size
            for coordinate in _medoid(component)
        ]
        chosen[candidate_key] = max(medoid_states, key=_cell_rank), largest_size

    def candidate_rank(item: tuple[str, tuple[_CellState, int]]) -> tuple[object, ...]:
        candidate_key, (state, _component_size) = item
        positive_folds = sum(
            block.fully_loaded_net_pnl_ticks > 0 for block in state.moderate.blocks
        )
        worst = min(
            block.fully_loaded_net_ev_ticks
            for block in state.moderate.blocks
            if block.fully_loaded_net_ev_ticks is not None
        )
        lower = state.bootstrap.lower_bound_ev_ticks if state.bootstrap else None
        if lower is None or state.moderate.fully_loaded_net_ev_ticks is None:
            raise BarStateSelectionError("eligible finalist is missing rank economics")
        return finalist_rank_key(
            StateFinalistRank(
                candidate_key=candidate_key,
                positive_fold_count=positive_folds,
                worst_fold_ev_ticks=worst,
                bootstrap_lower_bound_ev_ticks=lower,
                overall_moderate_ev_ticks=state.moderate.fully_loaded_net_ev_ticks,
                maximum_drawdown_ticks=state.moderate.maximum_drawdown_ticks,
                stop_loss_index=state.sl_index,
                take_profit_index=state.tp_index,
            )
        )

    ranked = sorted(chosen.items(), key=candidate_rank)
    finalist_keys = tuple(candidate_key for candidate_key, _ in ranked[:MAXIMUM_FINALISTS])
    results: list[StateCandidateSelection] = []
    for candidate_key in candidates:
        selected_state = chosen.get(candidate_key)
        if selected_state is None:
            results.append(
                StateCandidateSelection(
                    candidate_key=candidate_key,
                    final_label="REJECTED",
                    selected_take_profit_index=None,
                    selected_stop_loss_index=None,
                    selected_take_profit_multiplier=None,
                    selected_stop_loss_multiplier=None,
                    positive_component_size=0,
                    positive_inner_fold_count=0,
                    worst_fold_moderate_ev_ticks=None,
                    moderate_ev_ticks=None,
                    bootstrap_lower_bound_ev_ticks=None,
                    maximum_drawdown_ticks=None,
                    rejection_reasons=candidate_reasons[candidate_key],
                )
            )
            continue
        state, post_bh_component_size = selected_state
        selected = candidate_key in finalist_keys
        block_evs = tuple(item.fully_loaded_net_ev_ticks for item in state.moderate.blocks)
        results.append(
            StateCandidateSelection(
                candidate_key=candidate_key,
                final_label="FINALIST" if selected else "REJECTED",
                selected_take_profit_index=state.tp_index,
                selected_stop_loss_index=state.sl_index,
                selected_take_profit_multiplier=STATE_VOLATILITY_MULTIPLIERS[state.tp_index],
                selected_stop_loss_multiplier=STATE_VOLATILITY_MULTIPLIERS[state.sl_index],
                positive_component_size=post_bh_component_size,
                positive_inner_fold_count=sum(
                    item.fully_loaded_net_pnl_ticks > 0 for item in state.moderate.blocks
                ),
                worst_fold_moderate_ev_ticks=min(value for value in block_evs if value is not None),
                moderate_ev_ticks=state.moderate.fully_loaded_net_ev_ticks,
                bootstrap_lower_bound_ev_ticks=(
                    None if state.bootstrap is None else state.bootstrap.lower_bound_ev_ticks
                ),
                maximum_drawdown_ticks=state.moderate.maximum_drawdown_ticks,
                rejection_reasons=() if selected else ("MAXIMUM_FINALIST_LIMIT",),
            )
        )
    return StateSelectionResult(
        candidate_results=tuple(results),
        multiplicity_results=multiplicity,
        finalist_keys=finalist_keys,
    )


__all__ = [
    "BH_FAMILY_SIZE",
    "BOOTSTRAP_LCB_ORDER_INDEX",
    "BOOTSTRAP_MEAN_BLOCK_ACTIVE_DAYS",
    "BOOTSTRAP_RANDOM_SEED",
    "BOOTSTRAP_REPLICATES",
    "DISCOVERY_INNER_FOLD_KEYS",
    "MAXIMUM_FINALISTS",
    "PREDECESSOR_VARIANT_COUNT",
    "STATE_SELECTION_SCHEMA",
    "BarStateSelectionError",
    "StateCandidateSelection",
    "StateCandidateSupport",
    "StateCellMultiplicityResult",
    "StateFinalistRank",
    "StateFoldEvaluationCalendar",
    "StateSelectionProgress",
    "StateSelectionResult",
    "finalist_rank_key",
    "select_state_finalists",
    "summarize_candidate_support",
]
