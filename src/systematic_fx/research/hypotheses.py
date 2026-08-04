"""Validated, deterministic Phase 1 parent-hypothesis specifications."""

from __future__ import annotations

import hashlib
import json
import tomllib
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

PRIMARY_FAMILIES = frozenset({"P1", "P2", "P3", "P4", "P5", "P6"})
MODEL_FAMILIES = frozenset(
    {
        "DETERMINISTIC_THRESHOLD",
        "DETERMINISTIC_STATE_MACHINE",
        "REGULARIZED_LINEAR",
        "GENERALIZED_ADDITIVE",
        "SHALLOW_TREE_ENSEMBLE",
    }
)
DIRECTIONS = frozenset({"LONG", "SHORT", "BOTH"})
EXPECTED_LOOKBACK_BARS = (3, 6, 12, 24, 48, 96)
EXPECTED_ABSOLUTE_BARRIER_TICKS = (10, 16, 24, 32, 48, 64, 96, 128, 192)
EXPECTED_VOLATILITY_MULTIPLIERS = (
    "0.50",
    "0.75",
    "1.00",
    "1.50",
    "2.00",
    "3.00",
    "4.00",
)
EXPECTED_PARENT_COUNT = 60
EXPECTED_PARENTS_PER_FAMILY = 10
EXPECTED_CAMPAIGN_VARIANT_BUDGET = 240
MINIMUM_LOCAL_TRIAL_BUDGET = 264


class HypothesisConfigError(ValueError):
    """A hypothesis bundle is incomplete, unbounded, or internally inconsistent."""


def _nonempty_string(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise HypothesisConfigError(f"{label} must be a non-empty string")
    return value.strip()


def _string_tuple(value: object, *, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise HypothesisConfigError(f"{label} must be a non-empty array")
    strings = tuple(_nonempty_string(item, label=f"{label} item") for item in value)
    if len(strings) != len(set(strings)):
        raise HypothesisConfigError(f"{label} must not contain duplicates")
    return strings


def _integer_tuple(value: object, *, label: str) -> tuple[int, ...]:
    if not isinstance(value, list) or not value:
        raise HypothesisConfigError(f"{label} must be a non-empty integer array")
    if any(isinstance(item, bool) or not isinstance(item, int) for item in value):
        raise HypothesisConfigError(f"{label} must contain only integers")
    integers = tuple(value)
    if len(integers) != len(set(integers)):
        raise HypothesisConfigError(f"{label} must not contain duplicates")
    return integers


def _mapping(value: object, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise HypothesisConfigError(f"{label} must be a TOML table")
    return value


def _positive_decimal_string(value: object, *, label: str) -> str:
    text = _nonempty_string(value, label=label)
    try:
        parsed = Decimal(text)
    except InvalidOperation as error:
        raise HypothesisConfigError(f"{label} must be a decimal string") from error
    if not parsed.is_finite() or parsed <= 0:
        raise HypothesisConfigError(f"{label} must be positive and finite")
    return text


def _normalize_for_json(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return _normalize_for_json(asdict(value))
    if isinstance(value, Mapping):
        normalized: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("canonical JSON mappings require string keys")
            normalized[key] = _normalize_for_json(item)
        return normalized
    if isinstance(value, (list, tuple)):
        return [_normalize_for_json(item) for item in value]
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise TypeError("canonical JSON does not permit non-finite decimals")
        return format(value, "f")
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, float):
        raise TypeError("canonical research JSON does not permit binary floats")
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"unsupported canonical JSON value: {type(value).__name__}")


def canonical_json_bytes(value: object) -> bytes:
    """Serialize research state deterministically without binary floating point."""

    normalized = _normalize_for_json(value)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_sha256(value: object) -> str:
    """Return the SHA-256 of :func:`canonical_json_bytes`."""

    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def load_toml_document(path: Path) -> dict[str, Any]:
    """Load one TOML document and reject a missing or non-file path."""

    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise HypothesisConfigError(f"TOML config does not exist: {resolved}")
    try:
        with resolved.open("rb") as handle:
            document = tomllib.load(handle)
    except tomllib.TOMLDecodeError as error:
        raise HypothesisConfigError(f"invalid TOML config {resolved}: {error}") from error
    return document


@dataclass(frozen=True)
class HypothesisSpec:
    """One a-priori parent hypothesis, before any Discovery observation."""

    hypothesis_id: str
    family: str
    title: str
    direction: str
    model_family: str
    hypothesis: str
    entry_condition: str
    economic_rationale: str
    features: tuple[str, ...]
    interaction_family: str | None = None

    def registration_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "hypothesis_id": self.hypothesis_id,
            "family": self.family,
            "title": self.title,
            "direction": self.direction,
            "model_family": self.model_family,
            "hypothesis": self.hypothesis,
            "entry_condition": self.entry_condition,
            "economic_rationale": self.economic_rationale,
            "features": list(self.features),
        }
        if self.interaction_family is not None:
            payload["interaction_family"] = self.interaction_family
        return payload


@dataclass(frozen=True)
class HypothesisBundle:
    """The complete bounded Phase 1 parent-hypothesis catalog."""

    bundle_id: str
    schema_version: int
    execution_blocked: bool
    block_reasons: tuple[str, ...]
    instrument: str
    parent_symbol: str
    tick_size: str
    tick_value: str
    market_units_status: str
    feature_definition_versions: Mapping[str, str]
    lookback_bars: tuple[int, ...]
    absolute_barrier_ticks: tuple[int, ...]
    volatility_multipliers: tuple[str, ...]
    observation_active_sessions: int
    cost_floor_minimum_ticks: int
    cost_floor_multiplier: int
    signal_cadence_seconds: int
    selection_rule: str
    campaign_strategy_variant_budget: int
    strategy_variants_per_parent: int
    descendants_per_parent: int
    local_trial_budget: int
    local_trial_budget_breakdown: Mapping[str, int]
    hypotheses: tuple[HypothesisSpec, ...]

    def registration_payload(self) -> dict[str, object]:
        return {
            "bundle": {
                "id": self.bundle_id,
                "schema_version": self.schema_version,
                "execution_blocked": self.execution_blocked,
                "block_reasons": list(self.block_reasons),
            },
            "market_units": {
                "instrument": self.instrument,
                "parent_symbol": self.parent_symbol,
                "tick_size": self.tick_size,
                "tick_value": self.tick_value,
                "status": self.market_units_status,
            },
            "feature_definition_versions": dict(self.feature_definition_versions),
            "search_boundary": {
                "signal_cadence_seconds": self.signal_cadence_seconds,
                "lookback_bars": list(self.lookback_bars),
                "absolute_barrier_ticks": list(self.absolute_barrier_ticks),
                "volatility_multipliers": list(self.volatility_multipliers),
                "observation_active_sessions": self.observation_active_sessions,
                "cost_floor": {
                    "minimum_ticks": self.cost_floor_minimum_ticks,
                    "conservative_cost_multiplier": self.cost_floor_multiplier,
                    "effective_floor_ticks": None,
                },
                "selection_rule": self.selection_rule,
            },
            "trial_budget": {
                "campaign_strategy_variants": self.campaign_strategy_variant_budget,
                "strategy_variants_per_parent": self.strategy_variants_per_parent,
                "descendants_per_parent": self.descendants_per_parent,
                "experiment_local_total": self.local_trial_budget,
                "experiment_local_breakdown": dict(self.local_trial_budget_breakdown),
                "campaign_budget_counts_only": "STRATEGY_VARIANT",
                "experiment_budget_counts": "ALL_EXPERIMENT_TRIAL_ROWS",
            },
            "hypotheses": [item.registration_payload() for item in self.hypotheses],
        }

    @property
    def config_sha256(self) -> str:
        return canonical_sha256(self.registration_payload())


def _parse_hypothesis(raw: Mapping[str, Any], *, index: int) -> HypothesisSpec:
    prefix = f"hypotheses[{index}]"
    family = _nonempty_string(raw.get("family"), label=f"{prefix}.family").upper()
    if family not in PRIMARY_FAMILIES:
        raise HypothesisConfigError(f"{prefix}.family must be one of {sorted(PRIMARY_FAMILIES)}")

    hypothesis_id = _nonempty_string(raw.get("id"), label=f"{prefix}.id").lower()
    if not hypothesis_id.startswith(f"{family.lower()}_"):
        raise HypothesisConfigError(f"{prefix}.id must start with {family.lower()}_")
    model_family = _nonempty_string(raw.get("model_family"), label=f"{prefix}.model_family").upper()
    if model_family not in MODEL_FAMILIES:
        raise HypothesisConfigError(
            f"{prefix}.model_family must be one of {sorted(MODEL_FAMILIES)}"
        )
    direction = _nonempty_string(raw.get("direction"), label=f"{prefix}.direction").upper()
    if direction not in DIRECTIONS:
        raise HypothesisConfigError(f"{prefix}.direction must be one of {sorted(DIRECTIONS)}")

    interaction_raw = raw.get("interaction_family")
    interaction_family = None
    if interaction_raw is not None:
        interaction_family = _nonempty_string(
            interaction_raw, label=f"{prefix}.interaction_family"
        ).upper()
    if family == "P6":
        if interaction_family not in PRIMARY_FAMILIES - {"P6"}:
            raise HypothesisConfigError(f"{prefix}.interaction_family must identify one of P1-P5")
    elif interaction_family is not None:
        raise HypothesisConfigError(
            f"{prefix}.interaction_family is permitted only for P6 hypotheses"
        )

    return HypothesisSpec(
        hypothesis_id=hypothesis_id,
        family=family,
        title=_nonempty_string(raw.get("title"), label=f"{prefix}.title"),
        direction=direction,
        model_family=model_family,
        hypothesis=_nonempty_string(raw.get("hypothesis"), label=f"{prefix}.hypothesis"),
        entry_condition=_nonempty_string(
            raw.get("entry_condition"), label=f"{prefix}.entry_condition"
        ),
        economic_rationale=_nonempty_string(
            raw.get("economic_rationale"), label=f"{prefix}.economic_rationale"
        ),
        features=_string_tuple(raw.get("features"), label=f"{prefix}.features"),
        interaction_family=interaction_family,
    )


def parse_hypothesis_bundle(document: Mapping[str, Any]) -> HypothesisBundle:
    """Validate a decoded Phase 1 hypothesis TOML document."""

    bundle_raw = _mapping(document.get("bundle"), label="bundle")
    market_raw = _mapping(document.get("market_units"), label="market_units")
    versions_raw = _mapping(
        document.get("feature_definition_versions"), label="feature_definition_versions"
    )
    search_raw = _mapping(document.get("search_boundary"), label="search_boundary")
    budget_raw = _mapping(document.get("trial_budget"), label="trial_budget")
    breakdown_raw = _mapping(
        budget_raw.get("experiment_local_breakdown"),
        label="trial_budget.experiment_local_breakdown",
    )

    execution_blocked = bundle_raw.get("execution_blocked")
    if execution_blocked is not True:
        raise HypothesisConfigError(
            "bundle.execution_blocked must remain true while costs and execution are pending"
        )
    block_reasons = _string_tuple(bundle_raw.get("block_reasons"), label="bundle.block_reasons")

    schema_version = bundle_raw.get("schema_version")
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version < 1
    ):
        raise HypothesisConfigError("bundle.schema_version must be a positive integer")

    market_status = _nonempty_string(market_raw.get("status"), label="market_units.status")
    if market_status != "REVERIFY_REQUIRED":
        raise HypothesisConfigError("market_units.status must be REVERIFY_REQUIRED")

    versions: dict[str, str] = {}
    for key in ("features_1s", "research_5m", "outcomes"):
        versions[key] = _nonempty_string(
            versions_raw.get(key), label=f"feature_definition_versions.{key}"
        )

    lookbacks = _integer_tuple(search_raw.get("lookback_bars"), label="lookback_bars")
    if lookbacks != EXPECTED_LOOKBACK_BARS:
        raise HypothesisConfigError(f"lookback_bars must equal {EXPECTED_LOOKBACK_BARS}")
    absolute_ticks = _integer_tuple(
        search_raw.get("absolute_barrier_ticks"), label="absolute_barrier_ticks"
    )
    if absolute_ticks != EXPECTED_ABSOLUTE_BARRIER_TICKS:
        raise HypothesisConfigError(
            f"absolute_barrier_ticks must equal {EXPECTED_ABSOLUTE_BARRIER_TICKS}"
        )
    multipliers = _string_tuple(
        search_raw.get("volatility_multipliers"), label="volatility_multipliers"
    )
    if multipliers != EXPECTED_VOLATILITY_MULTIPLIERS:
        raise HypothesisConfigError(
            f"volatility_multipliers must equal {EXPECTED_VOLATILITY_MULTIPLIERS}"
        )

    positive_integer_fields = (
        (search_raw, "observation_active_sessions"),
        (search_raw, "cost_floor_minimum_ticks"),
        (search_raw, "cost_floor_multiplier"),
        (search_raw, "signal_cadence_seconds"),
        (budget_raw, "campaign_strategy_variants"),
        (budget_raw, "strategy_variants_per_parent"),
        (budget_raw, "descendants_per_parent"),
        (budget_raw, "experiment_local_total"),
    )
    parsed_integers: dict[str, int] = {}
    for table, key in positive_integer_fields:
        value = table.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise HypothesisConfigError(f"{key} must be a positive integer")
        parsed_integers[key] = value

    if parsed_integers["campaign_strategy_variants"] != EXPECTED_CAMPAIGN_VARIANT_BUDGET:
        raise HypothesisConfigError(
            f"campaign_strategy_variants must equal {EXPECTED_CAMPAIGN_VARIANT_BUDGET}"
        )
    if parsed_integers["strategy_variants_per_parent"] != 4:
        raise HypothesisConfigError("strategy_variants_per_parent must equal 4")
    if parsed_integers["descendants_per_parent"] != 3:
        raise HypothesisConfigError("descendants_per_parent must equal 3")
    if parsed_integers["experiment_local_total"] < MINIMUM_LOCAL_TRIAL_BUDGET:
        raise HypothesisConfigError(
            "experiment_local_total cannot hold four variants and both barrier surfaces"
        )

    breakdown: dict[str, int] = {}
    for key, value in breakdown_raw.items():
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise HypothesisConfigError(
                f"trial_budget.experiment_local_breakdown.{key} must be nonnegative"
            )
        breakdown[key] = value
    if sum(breakdown.values()) != parsed_integers["experiment_local_total"]:
        raise HypothesisConfigError("experiment_local_breakdown must sum to experiment_local_total")
    if breakdown.get("strategy_variant") != 4:
        raise HypothesisConfigError("local strategy_variant budget must equal 4")
    if breakdown.get("absolute_barrier_cell") != 162:
        raise HypothesisConfigError("absolute barrier budget must cover two 9x9 surfaces")
    if breakdown.get("volatility_barrier_cell") != 98:
        raise HypothesisConfigError("volatility barrier budget must cover two 7x7 surfaces")

    raw_hypotheses = document.get("hypotheses")
    if not isinstance(raw_hypotheses, list):
        raise HypothesisConfigError("hypotheses must be an array of tables")
    hypotheses = tuple(
        _parse_hypothesis(_mapping(raw, label=f"hypotheses[{index}]"), index=index)
        for index, raw in enumerate(raw_hypotheses)
    )
    if len(hypotheses) != EXPECTED_PARENT_COUNT:
        raise HypothesisConfigError(
            f"expected {EXPECTED_PARENT_COUNT} parent hypotheses, found {len(hypotheses)}"
        )
    ids = [item.hypothesis_id for item in hypotheses]
    if len(ids) != len(set(ids)):
        raise HypothesisConfigError("hypothesis ids must be unique")
    family_counts = Counter(item.family for item in hypotheses)
    expected_counts = {family: EXPECTED_PARENTS_PER_FAMILY for family in PRIMARY_FAMILIES}
    if dict(family_counts) != expected_counts:
        raise HypothesisConfigError(
            f"each primary family must contain exactly {EXPECTED_PARENTS_PER_FAMILY} parents"
        )

    return HypothesisBundle(
        bundle_id=_nonempty_string(bundle_raw.get("id"), label="bundle.id"),
        schema_version=schema_version,
        execution_blocked=True,
        block_reasons=block_reasons,
        instrument=_nonempty_string(market_raw.get("instrument"), label="market_units.instrument"),
        parent_symbol=_nonempty_string(
            market_raw.get("parent_symbol"), label="market_units.parent_symbol"
        ),
        tick_size=_positive_decimal_string(
            market_raw.get("tick_size"), label="market_units.tick_size"
        ),
        tick_value=_positive_decimal_string(
            market_raw.get("tick_value"), label="market_units.tick_value"
        ),
        market_units_status=market_status,
        feature_definition_versions=versions,
        lookback_bars=lookbacks,
        absolute_barrier_ticks=absolute_ticks,
        volatility_multipliers=multipliers,
        observation_active_sessions=parsed_integers["observation_active_sessions"],
        cost_floor_minimum_ticks=parsed_integers["cost_floor_minimum_ticks"],
        cost_floor_multiplier=parsed_integers["cost_floor_multiplier"],
        signal_cadence_seconds=parsed_integers["signal_cadence_seconds"],
        selection_rule=_nonempty_string(
            search_raw.get("selection_rule"), label="search_boundary.selection_rule"
        ),
        campaign_strategy_variant_budget=parsed_integers["campaign_strategy_variants"],
        strategy_variants_per_parent=parsed_integers["strategy_variants_per_parent"],
        descendants_per_parent=parsed_integers["descendants_per_parent"],
        local_trial_budget=parsed_integers["experiment_local_total"],
        local_trial_budget_breakdown=breakdown,
        hypotheses=hypotheses,
    )


def load_hypothesis_bundle(path: Path) -> HypothesisBundle:
    """Load and validate the checked-in Phase 1 parent-hypothesis bundle."""

    return parse_hypothesis_bundle(load_toml_document(path))


def family_counts(hypotheses: Sequence[HypothesisSpec]) -> dict[str, int]:
    """Return stable P1-P6 counts for reports and registration artifacts."""

    counts = Counter(item.family for item in hypotheses)
    return {family: counts[family] for family in sorted(PRIMARY_FAMILIES)}
