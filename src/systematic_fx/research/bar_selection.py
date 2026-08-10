"""Frozen support, period-stability, and adjacent-grid screening for bar patterns."""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal

from systematic_fx.backtest.barriers import BARRIER_TICKS, Direction
from systematic_fx.research.bar_economics import BarCellEconomics


class BarSelectionError(ValueError):
    """A discovery surface is incomplete or contradicts its frozen identity."""


@dataclass(frozen=True, slots=True)
class BarSupportEvidence:
    """Outcome-free signal support for one exact candidate."""

    candidate_key: str
    timeframe_seconds: int
    direction: Direction
    raw_signal_count: int
    distinct_signal_day_count: int
    block_signal_counts: tuple[int, int, int, int]
    median_signals_per_active_day_numerator: int
    median_signals_per_active_day_denominator: int


@dataclass(frozen=True, slots=True)
class BarCandidateDecision:
    """One deterministic discovery decision and optional executable bracket."""

    candidate_key: str
    direction: Direction
    label: str
    selected_take_profit_ticks: int | None
    selected_stop_loss_ticks: int | None
    positive_component_size: int
    rejection_reasons: tuple[str, ...]
    positive_block_count: int
    worst_block_moderate_ev_ticks: Decimal | None
    overall_moderate_ev_ticks: Decimal | None
    moderate_maximum_drawdown_ticks: int | None

    @property
    def selected_buy_sell_loss_formula(self) -> Mapping[str, str] | None:
        if self.selected_take_profit_ticks is None or self.selected_stop_loss_ticks is None:
            return None
        if self.direction is Direction.LONG:
            return {
                "buying_price": "next_bar_open_ticks + scenario.entry_adverse_ticks",
                "selling_price": (f"buying_price_ticks + {self.selected_take_profit_ticks} ticks"),
                "loss_price": f"buying_price_ticks - {self.selected_stop_loss_ticks} ticks",
            }
        return {
            "selling_price": "next_bar_open_ticks - scenario.entry_adverse_ticks",
            "buying_price": f"selling_price_ticks - {self.selected_take_profit_ticks} ticks",
            "loss_price": f"selling_price_ticks + {self.selected_stop_loss_ticks} ticks",
        }


_SUPPORT_GATES = {
    300: (160, 25, 40, 10),
    1800: (100, 15, 35, 6),
    3600: (80, 12, 30, 4),
}
_SCENARIOS = ("BASELINE", "MODERATE_COMBINED", "SEVERE_DIAGNOSTIC")


def _support_reasons(value: BarSupportEvidence) -> tuple[str, ...]:
    gate = _SUPPORT_GATES.get(value.timeframe_seconds)
    if gate is None:
        raise BarSelectionError("support timeframe is outside 5m/30m/1h")
    minimum_signals, minimum_per_block, minimum_days, maximum_median = gate
    reasons: list[str] = []
    if value.raw_signal_count < minimum_signals:
        reasons.append("INSUFFICIENT_RAW_SIGNALS")
    if min(value.block_signal_counts) < minimum_per_block:
        reasons.append("INSUFFICIENT_EACH_DISCOVERY_BLOCK")
    if value.distinct_signal_day_count < minimum_days:
        reasons.append("INSUFFICIENT_DISTINCT_SIGNAL_DAYS")
    if value.median_signals_per_active_day_denominator <= 0:
        raise BarSelectionError("median denominator must be positive")
    if (
        value.median_signals_per_active_day_numerator
        > maximum_median * value.median_signals_per_active_day_denominator
    ):
        reasons.append("NONSELECTIVE_SIGNAL_FREQUENCY")
    return tuple(reasons)


def _surface_map(
    values: Sequence[BarCellEconomics],
    *,
    scenario_id: str,
    direction: Direction,
) -> dict[tuple[int, int], BarCellEconomics]:
    cells: dict[tuple[int, int], BarCellEconomics] = {}
    for value in values:
        if not isinstance(value, BarCellEconomics):
            raise BarSelectionError("surfaces must contain BarCellEconomics")
        if value.scenario_id != scenario_id or value.direction is not direction:
            raise BarSelectionError("surface scenario/direction identity drift")
        identity = (value.take_profit_ticks, value.stop_loss_ticks)
        if identity in cells:
            raise BarSelectionError("duplicate barrier cell")
        cells[identity] = value
    expected = {(tp, sl) for tp in BARRIER_TICKS for sl in BARRIER_TICKS}
    if set(cells) != expected:
        raise BarSelectionError("surface must contain the complete 484-cell grid")
    return cells


def _moderate_core_eligible(
    baseline: BarCellEconomics,
    moderate: BarCellEconomics,
) -> bool:
    if baseline.entry_fill_count < 40 or moderate.entry_fill_count < 40:
        return False
    if len(baseline.blocks) != 4 or len(moderate.blocks) != 4:
        raise BarSelectionError("discovery economics require exactly four blocks")
    if min(item.entry_fill_count for item in baseline.blocks) < 8:
        return False
    if min(item.entry_fill_count for item in moderate.blocks) < 8:
        return False
    if baseline.fully_loaded_net_ev_ticks is None or baseline.fully_loaded_net_ev_ticks <= 0:
        return False
    if moderate.fully_loaded_net_pnl_ticks <= 0:
        return False
    if moderate.calendar_month_net_pnl_usd <= 0:
        return False
    if moderate.profit_factor is None or moderate.profit_factor < Decimal("1.05"):
        return False
    positive_blocks = [block for block in moderate.blocks if block.fully_loaded_net_pnl_ticks > 0]
    if len(positive_blocks) < 3:
        return False
    block_evs = [item.fully_loaded_net_ev_ticks for item in moderate.blocks]
    if any(value is None for value in block_evs):
        return False
    if min(value for value in block_evs if value is not None) < Decimal(-2):
        return False
    positive_gross = [item.gross_profit_ticks for item in positive_blocks]
    return bool(positive_gross) and max(positive_gross) * 2 <= sum(positive_gross)


def _neighbors(identity: tuple[int, int], *, diagonal: bool) -> tuple[tuple[int, int], ...]:
    tp_index = BARRIER_TICKS.index(identity[0])
    sl_index = BARRIER_TICKS.index(identity[1])
    values: list[tuple[int, int]] = []
    for tp_delta in (-1, 0, 1):
        for sl_delta in (-1, 0, 1):
            if tp_delta == 0 and sl_delta == 0:
                continue
            if not diagonal and abs(tp_delta) + abs(sl_delta) != 1:
                continue
            next_tp = tp_index + tp_delta
            next_sl = sl_index + sl_delta
            if 0 <= next_tp < len(BARRIER_TICKS) and 0 <= next_sl < len(BARRIER_TICKS):
                values.append((BARRIER_TICKS[next_tp], BARRIER_TICKS[next_sl]))
    return tuple(values)


def _components(values: set[tuple[int, int]]) -> tuple[frozenset[tuple[int, int]], ...]:
    remaining = set(values)
    result: list[frozenset[tuple[int, int]]] = []
    while remaining:
        seed = min(remaining)
        queue = deque((seed,))
        component: set[tuple[int, int]] = set()
        remaining.remove(seed)
        while queue:
            current = queue.popleft()
            component.add(current)
            for neighbor in _neighbors(current, diagonal=False):
                if neighbor in remaining:
                    remaining.remove(neighbor)
                    queue.append(neighbor)
        result.append(frozenset(component))
    return tuple(result)


def _median(values: Sequence[Decimal]) -> Decimal:
    ordered = sorted(values)
    if not ordered:
        raise BarSelectionError("cannot compute an empty median")
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal(2)


def _is_locally_stable(
    identity: tuple[int, int],
    baseline: Mapping[tuple[int, int], BarCellEconomics],
    moderate: Mapping[tuple[int, int], BarCellEconomics],
) -> bool:
    tp_index = BARRIER_TICKS.index(identity[0])
    sl_index = BARRIER_TICKS.index(identity[1])
    if tp_index in {0, len(BARRIER_TICKS) - 1} or sl_index in {
        0,
        len(BARRIER_TICKS) - 1,
    }:
        return False
    neighborhood = (identity, *_neighbors(identity, diagonal=True))
    if (
        sum(
            baseline[item].fully_loaded_net_ev_ticks is not None
            and baseline[item].fully_loaded_net_ev_ticks > 0
            for item in neighborhood
        )
        < 7
    ):
        return False
    if sum(moderate[item].fully_loaded_net_pnl_ticks > 0 for item in neighborhood) < 7:
        return False
    selected_ev = moderate[identity].fully_loaded_net_ev_ticks
    if selected_ev is None or selected_ev <= 0:
        return False
    positive_neighbors = [
        moderate[item].fully_loaded_net_ev_ticks
        for item in _neighbors(identity, diagonal=True)
        if moderate[item].fully_loaded_net_ev_ticks is not None
        and moderate[item].fully_loaded_net_ev_ticks > 0
    ]
    if not positive_neighbors:
        return False
    neighbor_median = _median([value for value in positive_neighbors if value is not None])
    return neighbor_median * 2 >= selected_ev


def _component_medoid(component: frozenset[tuple[int, int]]) -> tuple[int, int]:
    def distance(candidate: tuple[int, int]) -> int:
        candidate_tp = BARRIER_TICKS.index(candidate[0])
        candidate_sl = BARRIER_TICKS.index(candidate[1])
        return sum(
            abs(candidate_tp - BARRIER_TICKS.index(item[0]))
            + abs(candidate_sl - BARRIER_TICKS.index(item[1]))
            for item in component
        )

    return min(component, key=lambda item: (distance(item), item[1], item[0]))


def screen_bar_candidate(
    support: BarSupportEvidence,
    surfaces: Mapping[str, Sequence[BarCellEconomics]],
) -> BarCandidateDecision:
    """Apply the frozen discovery gates without looking at validation/holdout data."""

    if not isinstance(support, BarSupportEvidence):
        raise BarSelectionError("support must be BarSupportEvidence")
    if set(surfaces) != set(_SCENARIOS):
        raise BarSelectionError("all three frozen scenario surfaces are required")
    mapped = {
        scenario_id: _surface_map(
            surfaces[scenario_id],
            scenario_id=scenario_id,
            direction=support.direction,
        )
        for scenario_id in _SCENARIOS
    }
    support_reasons = _support_reasons(support)
    if support_reasons:
        return BarCandidateDecision(
            candidate_key=support.candidate_key,
            direction=support.direction,
            label="SUPPORT_REJECT",
            selected_take_profit_ticks=None,
            selected_stop_loss_ticks=None,
            positive_component_size=0,
            rejection_reasons=support_reasons,
            positive_block_count=0,
            worst_block_moderate_ev_ticks=None,
            overall_moderate_ev_ticks=None,
            moderate_maximum_drawdown_ticks=None,
        )

    baseline = mapped["BASELINE"]
    moderate = mapped["MODERATE_COMBINED"]
    eligible = {
        identity
        for identity in baseline
        if _moderate_core_eligible(baseline[identity], moderate[identity])
    }
    components = tuple(component for component in _components(eligible) if len(component) >= 9)
    stable_by_component = {
        component: tuple(
            identity
            for identity in sorted(component)
            if _is_locally_stable(identity, baseline, moderate)
        )
        for component in components
    }
    usable = tuple(component for component in components if stable_by_component[component])
    if not usable:
        reasons: list[str] = []
        if not eligible:
            reasons.append("NO_FULLY_LOADED_PERIOD_STABLE_CELL")
        if not components:
            reasons.append("NO_CONTIGUOUS_POSITIVE_COMPONENT_SIZE_9")
        reasons.append("NO_INTERIOR_7_OF_9_STABLE_CELL")
        return BarCandidateDecision(
            candidate_key=support.candidate_key,
            direction=support.direction,
            label="ECONOMIC_REJECT",
            selected_take_profit_ticks=None,
            selected_stop_loss_ticks=None,
            positive_component_size=max((len(item) for item in components), default=0),
            rejection_reasons=tuple(dict.fromkeys(reasons)),
            positive_block_count=0,
            worst_block_moderate_ev_ticks=None,
            overall_moderate_ev_ticks=None,
            moderate_maximum_drawdown_ticks=None,
        )

    component = max(
        usable,
        key=lambda item: (
            len(item),
            _median(
                [
                    moderate[identity].fully_loaded_net_ev_ticks
                    for identity in item
                    if moderate[identity].fully_loaded_net_ev_ticks is not None
                ]
            ),
            tuple(sorted(item)),
        ),
    )
    stable = stable_by_component[component]
    medoid = _component_medoid(component)
    if medoid in stable:
        selected = medoid
    else:
        component_median = _median(
            [
                moderate[identity].fully_loaded_net_ev_ticks
                for identity in component
                if moderate[identity].fully_loaded_net_ev_ticks is not None
            ]
        )
        within_ten_percent = [
            identity
            for identity in stable
            if moderate[identity].fully_loaded_net_ev_ticks is not None
            and moderate[identity].fully_loaded_net_ev_ticks >= component_median * Decimal("0.9")
        ]
        pool = within_ten_percent or list(stable)
        selected = min(pool, key=lambda item: (item[1], item[0]))
    selected_cell = moderate[selected]
    block_evs = [item.fully_loaded_net_ev_ticks for item in selected_cell.blocks]
    if any(value is None for value in block_evs):  # guarded by core eligibility
        raise BarSelectionError("selected cell has an empty discovery block")
    return BarCandidateDecision(
        candidate_key=support.candidate_key,
        direction=support.direction,
        label="DISCOVERY_FINALIST",
        selected_take_profit_ticks=selected[0],
        selected_stop_loss_ticks=selected[1],
        positive_component_size=len(component),
        rejection_reasons=(),
        positive_block_count=sum(
            item.fully_loaded_net_pnl_ticks > 0 for item in selected_cell.blocks
        ),
        worst_block_moderate_ev_ticks=min(value for value in block_evs if value is not None),
        overall_moderate_ev_ticks=selected_cell.fully_loaded_net_ev_ticks,
        moderate_maximum_drawdown_ticks=selected_cell.maximum_drawdown_ticks,
    )


def rank_bar_finalists(
    decisions: Sequence[BarCandidateDecision],
    *,
    limit: int = 10,
) -> tuple[BarCandidateDecision, ...]:
    """Apply the preregistered lexicographic ranking and finalist budget."""

    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 10:
        raise BarSelectionError("finalist limit must be between 1 and 10")
    finalists = [item for item in decisions if item.label == "DISCOVERY_FINALIST"]
    if any(
        item.selected_take_profit_ticks is None
        or item.selected_stop_loss_ticks is None
        or item.worst_block_moderate_ev_ticks is None
        or item.overall_moderate_ev_ticks is None
        or item.moderate_maximum_drawdown_ticks is None
        for item in finalists
    ):
        raise BarSelectionError("finalist decision lacks ranking fields")
    finalists.sort(
        key=lambda item: (
            -item.positive_block_count,
            -item.worst_block_moderate_ev_ticks,  # type: ignore[operator]
            -item.overall_moderate_ev_ticks,  # type: ignore[operator]
            item.moderate_maximum_drawdown_ticks,
            item.selected_stop_loss_ticks,
            item.selected_take_profit_ticks,
            item.candidate_key,
        )
    )
    return tuple(finalists[:limit])
