"""Strict loader for the frozen Phase 1A conservative screening bundle."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from systematic_fx.research.hypotheses import canonical_sha256, load_toml_document
from systematic_fx.validation.splits import CALENDAR_VERSION, SPLIT_VERSION


class ScreeningConfigError(ValueError):
    """The four-file screening policy bundle is incomplete or inconsistent."""


@dataclass(frozen=True, slots=True)
class ConfigIdentity:
    """Version and canonical content identity of one TOML input."""

    path: Path
    config_id: str
    sha256: str


@dataclass(frozen=True, slots=True)
class ConservativeScreeningBundle:
    """Validated operational values and hashes for Phase 1A."""

    campaign: ConfigIdentity
    cost: ConfigIdentity
    execution: ConfigIdentity
    barrier_grid: ConfigIdentity
    bundle_sha256: str
    source_start: str
    source_end: str
    excluded_dates: tuple[str, ...]
    calendar_version: str
    split_version: str
    feature_version: str
    outcome_version: str
    cost_version: str
    execution_version: str
    barrier_grid_version: str
    barrier_ticks: tuple[int, ...]
    missing_previous_session_behavior: str
    variable_cost_ticks: int
    allocated_fixed_cost_ticks: int
    baseline_cost_floor_ticks: int
    routing_delay_ms: int
    stop_adverse_ticks: int
    take_profit_trade_through_ticks: int

    @property
    def config_hashes(self) -> dict[str, str]:
        return {
            "campaign": self.campaign.sha256,
            "cost": self.cost.sha256,
            "execution": self.execution.sha256,
            "barrier_grid": self.barrier_grid.sha256,
        }


def _table(document: dict[str, Any], key: str) -> dict[str, Any]:
    value = document.get(key)
    if not isinstance(value, dict):
        raise ScreeningConfigError(f"{key} must be a TOML table")
    return value


def _string(table: dict[str, Any], key: str, *, label: str | None = None) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ScreeningConfigError(f"{label or key} must be a non-empty string")
    return value


def _integer(table: dict[str, Any], key: str, *, positive: bool = True) -> int:
    value = table.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ScreeningConfigError(f"{key} must be an integer")
    if positive and value <= 0:
        raise ScreeningConfigError(f"{key} must be positive")
    return value


def _identity(path: Path, document: dict[str, Any], table: str) -> ConfigIdentity:
    root = _table(document, table)
    return ConfigIdentity(
        path=path.resolve(),
        config_id=_string(root, "id", label=f"{table}.id"),
        sha256=canonical_sha256(document),
    )


def load_conservative_screening_bundle(root: Path) -> ConservativeScreeningBundle:
    """Load and cross-check the complete frozen screening configuration set."""

    resolved = root.expanduser().resolve()
    paths = {
        "campaign": resolved / "configs/campaigns/phase1a_conservative_screening_v1.toml",
        "cost": resolved / "configs/costs/phase1a_conservative_cost_v1.toml",
        "execution": resolved / "configs/execution/phase1a_conservative_execution_v1.toml",
        "grid": resolved / "configs/research/phase1a_barrier_grid_v1.toml",
    }
    campaign_document = load_toml_document(paths["campaign"])
    cost_document = load_toml_document(paths["cost"])
    execution_document = load_toml_document(paths["execution"])
    grid_document = load_toml_document(paths["grid"])

    campaign = _table(campaign_document, "campaign")
    authority = _table(campaign_document, "authority")
    data_policy = _table(campaign_document, "data_policy")
    boundary = _table(data_policy, "reference_boundary")
    contract_selection = _table(campaign_document, "contract_selection")
    cost = _table(cost_document, "cost_model")
    variable = _table(cost_document, "variable_cost")
    fixed = _table(cost_document, "fully_loaded_fixed_allocation")
    fixed_categories = _table(fixed, "screening_monthly_assumptions_usd")
    floor = _table(cost_document, "economic_floor")
    execution = _table(execution_document, "execution_model")
    latency = _table(execution_document, "latency")
    target = _table(execution_document, "take_profit")
    stop = _table(execution_document, "stop")
    ordering = _table(execution_document, "event_ordering")
    grid = _table(grid_document, "barrier_grid")

    if authority.get("maximum_authority") != "SCREENING_SURVIVOR":
        raise ScreeningConfigError("campaign authority must stop at SCREENING_SURVIVOR")
    if authority.get("pass_backtest_authority") is not False:
        raise ScreeningConfigError("Phase 1A cannot have PASS_BACKTEST authority")
    if boundary.get("definition_status_required_for_screening") is not False:
        raise ScreeningConfigError("definition/status must remain optional for Phase 1A screening")
    if boundary.get("definition_status_required_for_pass_backtest") is not True:
        raise ScreeningConfigError("definition/status must remain mandatory after Phase 1A")
    if boundary.get("invent_trading_status_allowed") is not False:
        raise ScreeningConfigError("trading status may not be fabricated")
    missing_previous_session_behavior = _string(
        contract_selection,
        "missing_previous_session_behavior",
        label="contract_selection.missing_previous_session_behavior",
    )
    if missing_previous_session_behavior != "NO_ENTRY_ENTIRE_SESSION":
        raise ScreeningConfigError(
            "missing previous-session selection evidence must block the entire session"
        )

    campaign_cost = _string(campaign, "cost_model_version")
    campaign_execution = _string(campaign, "execution_model_version")
    campaign_grid = _string(campaign, "barrier_grid_version")
    campaign_calendar = _string(campaign, "eligible_calendar_version")
    campaign_split = _string(campaign, "split_version")
    if campaign_calendar != CALENDAR_VERSION:
        raise ScreeningConfigError("campaign and calendar implementation IDs differ")
    if campaign_split != SPLIT_VERSION:
        raise ScreeningConfigError("campaign and split implementation IDs differ")
    if campaign_cost != _string(cost, "id"):
        raise ScreeningConfigError("campaign and cost IDs differ")
    if campaign_execution != _string(execution, "id"):
        raise ScreeningConfigError("campaign and execution IDs differ")
    if campaign_grid != _string(grid, "id"):
        raise ScreeningConfigError("campaign and barrier-grid IDs differ")

    pips = list(range(12, 97, 4))
    ticks = tuple(value * 2 for value in pips)
    if grid.get("take_profit_pips") != pips or grid.get("stop_loss_pips") != pips:
        raise ScreeningConfigError("barrier pip axes must be 12..96 in four-pip steps")
    if (
        tuple(grid.get("take_profit_ticks", ())) != ticks
        or tuple(grid.get("stop_loss_ticks", ())) != ticks
    ):
        raise ScreeningConfigError("barrier tick axes must be 24..192 in eight-tick steps")
    if _integer(grid, "expected_cell_count") != len(ticks) ** 2:
        raise ScreeningConfigError("barrier surface must contain exactly 484 cells")
    if grid.get("preselection_pruning_allowed") is not False:
        raise ScreeningConfigError("barrier preselection is forbidden")

    category_total = sum(
        Decimal(value) for key, value in fixed_categories.items() if key != "total"
    )
    if category_total != Decimal(str(fixed_categories.get("total"))):
        raise ScreeningConfigError("fixed-cost categories do not sum to total")
    if category_total != Decimal(_string(fixed, "monthly_pool_usd")):
        raise ScreeningConfigError("fixed-cost total and monthly pool differ")
    variable_ticks = _integer(variable, "round_trip_debit_ticks")
    fixed_ticks = _integer(fixed, "allocated_fixed_cost_ticks_per_round_trip")
    cost_ticks = _integer(floor, "baseline_cost_ticks")
    floor_ticks = _integer(floor, "baseline_minimum_take_profit_ticks")
    if cost_ticks != variable_ticks + fixed_ticks or floor_ticks != max(10, 3 * cost_ticks):
        raise ScreeningConfigError("baseline cost floor is inconsistent")
    if ticks[0] != floor_ticks:
        raise ScreeningConfigError("the first barrier must equal the conservative cost floor")

    if ordering.get("same_timestamp_tie_break") != "STOP_FIRST":
        raise ScreeningConfigError("same-time barrier ties must be STOP_FIRST")
    if target.get("touch_is_fill") is not False:
        raise ScreeningConfigError("take-profit touch cannot be a fill")
    if stop.get("trigger_is_fill") is not False:
        raise ScreeningConfigError("stop trigger cannot equal stop fill")

    excluded = data_policy.get("failed_source_dates")
    if not isinstance(excluded, list) or len(excluded) != 6:
        raise ScreeningConfigError("exactly six structurally failed dates must be excluded")
    excluded_dates = tuple(value.isoformat() for value in excluded)

    identities = {
        "campaign": _identity(paths["campaign"], campaign_document, "campaign"),
        "cost": _identity(paths["cost"], cost_document, "cost_model"),
        "execution": _identity(paths["execution"], execution_document, "execution_model"),
        "barrier_grid": _identity(paths["grid"], grid_document, "barrier_grid"),
    }
    bundle_sha256 = canonical_sha256(
        {
            "schema": "systematic_fx.phase1a_screening_config_bundle.v1",
            "configs": {key: value.sha256 for key, value in identities.items()},
        }
    )
    return ConservativeScreeningBundle(
        campaign=identities["campaign"],
        cost=identities["cost"],
        execution=identities["execution"],
        barrier_grid=identities["barrier_grid"],
        bundle_sha256=bundle_sha256,
        source_start=campaign["source_start"].isoformat(),
        source_end=campaign["source_end"].isoformat(),
        excluded_dates=excluded_dates,
        calendar_version=campaign_calendar,
        split_version=campaign_split,
        feature_version=_string(campaign, "feature_version"),
        outcome_version=_string(campaign, "outcome_version"),
        cost_version=campaign_cost,
        execution_version=campaign_execution,
        barrier_grid_version=campaign_grid,
        barrier_ticks=ticks,
        missing_previous_session_behavior=missing_previous_session_behavior,
        variable_cost_ticks=variable_ticks,
        allocated_fixed_cost_ticks=fixed_ticks,
        baseline_cost_floor_ticks=floor_ticks,
        routing_delay_ms=_integer(latency, "baseline_routing_delay_ms"),
        stop_adverse_ticks=_integer(stop, "baseline_minimum_adverse_ticks"),
        take_profit_trade_through_ticks=_integer(target, "trade_through_ticks"),
    )
