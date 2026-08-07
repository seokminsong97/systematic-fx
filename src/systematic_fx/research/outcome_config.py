"""Strict orchestration policy for the Phase 1A shared outcome replay."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final

from systematic_fx.backtest.event_cache import (
    CACHE_INDEX_SCHEMA,
    CACHE_SCHEMA,
    CACHE_VERSION,
    MAX_CACHE_WORKERS,
)
from systematic_fx.research.discovery_slice import load_discovery_slice_config
from systematic_fx.research.hypotheses import canonical_sha256, load_toml_document
from systematic_fx.research.screening_config import (
    ConservativeScreeningBundle,
    load_conservative_screening_bundle,
)

OUTCOME_CONFIG_RELATIVE_PATH: Final = Path("configs/research/phase1a_p5_outcome_replay_v1.toml")
OUTCOME_ARTIFACT_SCHEMA: Final = "systematic_fx.phase1a_p5_outcome_replay.v1"
OUTCOME_REPLAY_ENGINE_VERSION: Final = "phase1a_shared_outcome_replay_v1"
P5_QUERY_ID: Final = "p5_01_range_expansion_flow_continuation"
TERMINAL_EXIT_POLICY: Final = "LAST_VALID_EXECUTABLE_QUOTE_BEFORE_EXPIRY_MONTH_START"
TERMINAL_PARTITION_RESOLUTION_POLICY: Final = (
    "REVERSE_SCAN_LAST_VALID_EXECUTABLE_QUOTE_PARTITION_V1"
)
EXPECTED_SLICE_INDICES: Final = tuple(range(99))
EXPECTED_SIGNAL_COUNT: Final = 1_111
EXPECTED_LONG_SIGNAL_COUNT: Final = 529
EXPECTED_SHORT_SIGNAL_COUNT: Final = 582
EXPECTED_SIGNAL_SOURCE_DATE_COUNT: Final = 238
EXPECTED_CONTRACT_COUNT: Final = 7
EXPECTED_CACHE_PARTITION_COUNT: Final = 485
EXPECTED_COMPLETED_SOURCE_DATE_COUNT: Final = 485
EXPECTED_FIRST_COMPLETED_SOURCE_DATE: Final = date(2022, 1, 3)
EXPECTED_LAST_COMPLETED_SOURCE_DATE: Final = date(2023, 8, 31)
EXPECTED_ARTIFACT_MANIFEST_SHA256: Final = (
    "23037db1dd12784e379b76effa4f3056cec18d9ae2db7fe7e54e11f2f5424d33"
)
EXPECTED_SIGNAL_MANIFEST_SHA256: Final = (
    "96b693b9210631b5bde9283de5bbf0a25afd537da8fb947f02d47427eff67d74"
)
EXPECTED_INPUT_PLAN_SHA256: Final = (
    "d492e6c17391083b7fc64a8ee05575c84bf4a53c050aae986914c996546e6e84"
)
EXPECTED_SCENARIO_IDS: Final = (
    "BASELINE",
    "MODERATE_COMBINED",
    "SEVERE_DIAGNOSTIC",
)


class OutcomeConfigError(ValueError):
    """The shared replay configuration is incomplete or internally inconsistent."""


@dataclass(frozen=True, slots=True)
class OutcomeScenario:
    """Execution and cost variables for one independently occupied scenario."""

    scenario_id: str
    routing_delay_ns: int
    entry_additional_adverse_ticks: int
    take_profit_trade_through_ticks: int
    other_market_exit_additional_adverse_ticks: int
    stop_total_minimum_adverse_ticks: int
    variable_debit_ticks: int
    fixed_cost_pool_multiplier: Decimal

    def as_dict(self) -> dict[str, object]:
        return {
            "entry_additional_adverse_ticks": self.entry_additional_adverse_ticks,
            "fixed_cost_pool_multiplier": str(self.fixed_cost_pool_multiplier),
            "other_market_exit_additional_adverse_ticks": (
                self.other_market_exit_additional_adverse_ticks
            ),
            "routing_delay_ns": self.routing_delay_ns,
            "scenario_id": self.scenario_id,
            "stop_total_minimum_adverse_ticks": self.stop_total_minimum_adverse_ticks,
            "take_profit_trade_through_ticks": self.take_profit_trade_through_ticks,
            "variable_debit_ticks": self.variable_debit_ticks,
        }


@dataclass(frozen=True, slots=True)
class OutcomeReplayConfig:
    """All governed variables needed to plan and identify one p5 outcome replay."""

    path: Path
    sha256: str
    config_id: str
    campaign_key: str
    query_id: str
    slice_indices: tuple[int, ...]
    source_dates_per_slice: int
    expected_signal_count: int
    expected_direction_counts: tuple[tuple[str, int], ...]
    expected_signal_source_date_count: int
    expected_contract_count: int
    expected_cache_partition_count: int
    expected_completed_source_date_count: int
    expected_last_completed_source_date: date
    expected_artifact_manifest_sha256: str
    expected_signal_manifest_sha256: str
    expected_input_plan_sha256: str
    screening_bundle: ConservativeScreeningBundle
    discovery_config_sha256: str
    discovery_definition_sha256: str
    scenarios: tuple[OutcomeScenario, ...]
    barrier_ticks: tuple[int, ...]
    first_touch_observation_sessions: int
    maximum_cache_workers: int
    cache_output_relative: Path
    checkpoint_output_relative: Path
    result_output_relative: Path
    source_footer_manifest_relative: Path
    source_sha256_manifest_relative: Path
    eligible_calendar_relative: Path
    split_relative: Path

    @property
    def expected_cell_count(self) -> int:
        return len(self.barrier_ticks) ** 2

    @property
    def config_hashes(self) -> dict[str, str]:
        return {
            **self.screening_bundle.config_hashes,
            "discovery": self.discovery_config_sha256,
            "outcome_replay": self.sha256,
        }

    def canonical_parameters(self) -> dict[str, object]:
        """Return the complete non-secret replay variables for RunSpec/DB lineage."""

        return {
            "barrier_ticks": list(self.barrier_ticks),
            "cache": {
                "key": ["source_date", "raw_symbol"],
                "maximum_in_flight_partitions": self.maximum_cache_workers,
                "maximum_parallel_workers": self.maximum_cache_workers,
                "semantic_request_index_required": True,
                "request_index_schema": CACHE_INDEX_SCHEMA,
                "schema": CACHE_SCHEMA,
                "version": CACHE_VERSION,
                "worker_unit": "ONE_CACHE_KEY",
            },
            "checkpoint": {
                "boundary": "SOURCE_DATE_COMPLETE",
                "source_dates_per_checkpoint": 1,
            },
            "discovery_definition_sha256": self.discovery_definition_sha256,
            "expected_direction_counts": dict(self.expected_direction_counts),
            "expected_signal_count": self.expected_signal_count,
            "expected_signal_source_date_count": self.expected_signal_source_date_count,
            "expected_contract_count": self.expected_contract_count,
            "expected_cache_partition_count": self.expected_cache_partition_count,
            "expected_completed_source_date_count": (self.expected_completed_source_date_count),
            "expected_last_completed_source_date": (
                self.expected_last_completed_source_date.isoformat()
            ),
            "expected_artifact_manifest_sha256": self.expected_artifact_manifest_sha256,
            "expected_signal_manifest_sha256": self.expected_signal_manifest_sha256,
            "expected_input_plan_sha256": self.expected_input_plan_sha256,
            "first_touch_observation_active_sessions": (self.first_touch_observation_sessions),
            "global_event_order": [
                "ts_recv_ns",
                "sequence",
                "event_index",
                "contract_key",
            ],
            "occupied_signal_behavior": "LOG_AND_SKIP",
            "occupancy_key": [
                "scenario_id",
                "direction",
                "contract_key",
                "grid_cell_id",
            ],
            "portfolio_position_continues_after_censor": True,
            "query_id": self.query_id,
            "scenarios": [scenario.as_dict() for scenario in self.scenarios],
            "slice_indices": list(self.slice_indices),
            "terminal_exit": TERMINAL_EXIT_POLICY,
            "terminal_partition_resolution": TERMINAL_PARTITION_RESOLUTION_POLICY,
        }


def _table(document: dict[str, Any], name: str) -> dict[str, Any]:
    value = document.get(name)
    if not isinstance(value, dict):
        raise OutcomeConfigError(f"{name} must be a TOML table")
    return value


def _string(table: dict[str, Any], name: str) -> str:
    value = table.get(name)
    if not isinstance(value, str) or not value or value != value.strip():
        raise OutcomeConfigError(f"{name} must be a canonical non-empty string")
    return value


def _integer(table: dict[str, Any], name: str, *, minimum: int = 0) -> int:
    value = table.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise OutcomeConfigError(f"{name} must be an integer >= {minimum}")
    return value


def _boolean(table: dict[str, Any], name: str, *, expected: bool) -> None:
    if table.get(name) is not expected:
        raise OutcomeConfigError(f"{name} must be {expected}")


def _date(table: dict[str, Any], name: str) -> date:
    value = _string(table, name)
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise OutcomeConfigError(f"{name} must be a canonical ISO source date") from error
    if parsed.isoformat() != value:
        raise OutcomeConfigError(f"{name} must be a canonical ISO source date")
    return parsed


def _relative_path(value: object, *, label: str, derived: bool = False) -> Path:
    if not isinstance(value, str) or not value:
        raise OutcomeConfigError(f"{label} must be a non-empty relative path")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise OutcomeConfigError(f"{label} must be a canonical project-relative path")
    if derived and path.parts[:2] != ("data", "derived"):
        raise OutcomeConfigError(f"{label} must remain below data/derived")
    return path


def _scenario_rows(document: dict[str, Any], *, label: str) -> tuple[dict[str, Any], ...]:
    raw = document.get("stress_scenarios")
    if not isinstance(raw, list) or any(not isinstance(item, dict) for item in raw):
        raise OutcomeConfigError(f"{label}.stress_scenarios must be an array of tables")
    rows = tuple(raw)
    if tuple(row.get("id") for row in rows) != EXPECTED_SCENARIO_IDS:
        raise OutcomeConfigError(f"{label} scenario identity/order drift")
    return rows


def _decimal(value: object, *, label: str) -> Decimal:
    if not isinstance(value, str):
        raise OutcomeConfigError(f"{label} must be an exact decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise OutcomeConfigError(f"{label} is not a decimal") from error
    if not parsed.is_finite() or parsed <= 0:
        raise OutcomeConfigError(f"{label} must be positive and finite")
    return parsed


def _scenarios(project_root: Path) -> tuple[OutcomeScenario, ...]:
    execution = load_toml_document(
        project_root / "configs/execution/phase1a_conservative_execution_v1.toml"
    )
    costs = load_toml_document(project_root / "configs/costs/phase1a_conservative_cost_v1.toml")
    execution_rows = _scenario_rows(execution, label="execution")
    cost_rows = _scenario_rows(costs, label="cost")
    scenarios: list[OutcomeScenario] = []
    for execution_row, cost_row in zip(execution_rows, cost_rows, strict=True):
        scenario_id = _string(execution_row, "id")
        if _string(cost_row, "id") != scenario_id:
            raise OutcomeConfigError("cost and execution scenario IDs differ")
        scenarios.append(
            OutcomeScenario(
                scenario_id=scenario_id,
                routing_delay_ns=_integer(execution_row, "routing_delay_ms", minimum=1) * 1_000_000,
                entry_additional_adverse_ticks=_integer(
                    execution_row, "entry_additional_adverse_ticks"
                ),
                take_profit_trade_through_ticks=_integer(
                    execution_row, "take_profit_trade_through_ticks", minimum=1
                ),
                other_market_exit_additional_adverse_ticks=_integer(
                    execution_row, "other_market_exit_additional_adverse_ticks"
                ),
                stop_total_minimum_adverse_ticks=_integer(
                    execution_row, "stop_total_minimum_adverse_ticks", minimum=1
                ),
                variable_debit_ticks=_integer(cost_row, "round_trip_debit_ticks", minimum=1),
                fixed_cost_pool_multiplier=_decimal(
                    cost_row["fixed_cost_pool_multiplier"],
                    label=f"{scenario_id}.fixed_cost_pool_multiplier",
                ),
            )
        )
    return tuple(scenarios)


def load_outcome_replay_config(
    project_root: Path,
    *,
    config_path: Path | None = None,
) -> OutcomeReplayConfig:
    """Load the p5 replay policy and reject any drift from frozen Phase 1A inputs."""

    root = project_root.expanduser().resolve(strict=True)
    requested = config_path or root / OUTCOME_CONFIG_RELATIVE_PATH
    path = requested.expanduser().resolve(strict=True)
    document = load_toml_document(path)
    expected_tables = {
        "cache",
        "campaign_sequence",
        "checkpoint",
        "discovery_inputs",
        "frozen_policy_inputs",
        "outcome_replay",
        "replay",
        "results",
    }
    if set(document) != expected_tables:
        raise OutcomeConfigError("outcome replay top-level table schema drift")

    replay_id = _table(document, "outcome_replay")
    discovery = _table(document, "discovery_inputs")
    policy = _table(document, "frozen_policy_inputs")
    cache = _table(document, "cache")
    replay = _table(document, "replay")
    checkpoint = _table(document, "checkpoint")
    results = _table(document, "results")
    sequence = _table(document, "campaign_sequence")

    if _integer(replay_id, "schema_version", minimum=1) != 1:
        raise OutcomeConfigError("unsupported outcome replay schema_version")
    if _string(replay_id, "id") != "phase1a_p5_outcome_replay_v1":
        raise OutcomeConfigError("outcome replay ID drift")
    if _string(replay_id, "engine_version") != OUTCOME_REPLAY_ENGINE_VERSION:
        raise OutcomeConfigError("outcome replay engine version drift")
    if _string(replay_id, "query_id") != P5_QUERY_ID:
        raise OutcomeConfigError("the first outcome replay must remain p5_01")
    _boolean(replay_id, "screening_only", expected=True)
    if _string(replay_id, "maximum_authority") != "SCREENING_SURVIVOR":
        raise OutcomeConfigError("outcome replay cannot confer backtest or trading authority")

    first = _integer(discovery, "first_slice_index")
    last = _integer(discovery, "last_slice_index")
    slices = tuple(range(first, last + 1))
    if slices != EXPECTED_SLICE_INDICES or _integer(
        discovery, "expected_slice_count", minimum=1
    ) != len(slices):
        raise OutcomeConfigError("p5 replay must consume the canonical 99-slice prefix")
    expected_signal_count = _integer(discovery, "expected_signal_count", minimum=1)
    long_count = _integer(discovery, "expected_long_signal_count", minimum=1)
    short_count = _integer(discovery, "expected_short_signal_count", minimum=1)
    if (expected_signal_count, long_count, short_count) != (
        EXPECTED_SIGNAL_COUNT,
        EXPECTED_LONG_SIGNAL_COUNT,
        EXPECTED_SHORT_SIGNAL_COUNT,
    ) or long_count + short_count != expected_signal_count:
        raise OutcomeConfigError("p5 frozen support/direction counts drift")
    if (
        _integer(discovery, "expected_signal_source_date_count", minimum=1)
        != (EXPECTED_SIGNAL_SOURCE_DATE_COUNT)
        or _integer(discovery, "expected_contract_count", minimum=1) != EXPECTED_CONTRACT_COUNT
    ):
        raise OutcomeConfigError("p5 frozen signal-date/contract counts drift")
    if (
        _string(discovery, "expected_artifact_manifest_sha256") != EXPECTED_ARTIFACT_MANIFEST_SHA256
        or _string(discovery, "expected_signal_manifest_sha256") != EXPECTED_SIGNAL_MANIFEST_SHA256
    ):
        raise OutcomeConfigError("p5 frozen Discovery artifact/signal manifests drift")
    for name, expected in (
        ("require_canonical_success_attempt", True),
        ("require_visible_to_ai", True),
        ("require_research_eligible_false", True),
        ("require_all_occurrence_variables", True),
        ("require_artifact_sha256_verification", True),
    ):
        _boolean(discovery, name, expected=expected)

    screening = load_conservative_screening_bundle(root)
    # The configured path is separately checked so an equivalent-looking alternate file is forbidden.
    configured_discovery = _relative_path(
        discovery["discovery_config_path"], label="discovery_config_path"
    )
    if configured_discovery != Path("configs/research/phase1a_discovery_slice_v1.toml"):
        raise OutcomeConfigError("Discovery config path drift")
    discovery_config = load_discovery_slice_config(root / configured_discovery)
    if P5_QUERY_ID not in {query.query_id for query in discovery_config.candidate_queries}:
        raise OutcomeConfigError("p5 query is absent from the frozen Discovery config")

    expected_policy_paths = {
        "campaign_path": screening.campaign.path.relative_to(root),
        "cost_path": screening.cost.path.relative_to(root),
        "execution_path": screening.execution.path.relative_to(root),
        "barrier_grid_path": screening.barrier_grid.path.relative_to(root),
    }
    for name, expected in expected_policy_paths.items():
        if _relative_path(policy[name], label=name) != expected:
            raise OutcomeConfigError(f"{name} differs from the frozen screening bundle")

    if _string(cache, "schema") != CACHE_SCHEMA or _string(cache, "version") != CACHE_VERSION:
        raise OutcomeConfigError("event cache schema/version drift")
    if _string(cache, "request_index_schema") != CACHE_INDEX_SCHEMA:
        raise OutcomeConfigError("event cache request-index schema drift")
    if cache.get("partition_key") != ["source_date", "raw_symbol"]:
        raise OutcomeConfigError("event cache partition key drift")
    maximum_workers = _integer(cache, "maximum_parallel_workers", minimum=1)
    if maximum_workers > MAX_CACHE_WORKERS:
        raise OutcomeConfigError("cache worker count exceeds the governed bound")
    if _integer(cache, "maximum_in_flight_partitions", minimum=1) != maximum_workers:
        raise OutcomeConfigError("cache in-flight partition bound must equal worker count")
    if _string(cache, "worker_unit") != "ONE_CACHE_KEY":
        raise OutcomeConfigError("each cache worker must own exactly one cache key")
    if _integer(cache, "expected_partition_count", minimum=1) != (EXPECTED_CACHE_PARTITION_COUNT):
        raise OutcomeConfigError("p5 expected cache partition count drift")
    if _string(cache, "expected_plan_sha256") != EXPECTED_INPUT_PLAN_SHA256:
        raise OutcomeConfigError("p5 expected input plan identity drift")
    _boolean(cache, "retain_invalid_observations", expected=True)
    _boolean(cache, "retain_source_row_lineage", expected=True)
    _boolean(cache, "semantic_request_index_required", expected=True)

    if _integer(replay, "logical_passes", minimum=1) != 1:
        raise OutcomeConfigError("economic replay must use one logical chronological pass")
    if _integer(replay, "expected_completed_source_date_count", minimum=1) != (
        EXPECTED_COMPLETED_SOURCE_DATE_COUNT
    ):
        raise OutcomeConfigError("completed replay source-date count drift")
    if _date(replay, "expected_last_completed_source_date") != (
        EXPECTED_LAST_COMPLETED_SOURCE_DATE
    ):
        raise OutcomeConfigError("completed replay final source-date drift")
    if replay.get("global_event_order") != [
        "ts_recv_ns",
        "sequence",
        "event_index",
        "contract_key",
    ]:
        raise OutcomeConfigError("global replay ordering drift")
    if replay.get("occupancy_key") != [
        "scenario_id",
        "direction",
        "contract_key",
        "grid_cell_id",
    ]:
        raise OutcomeConfigError("portfolio occupancy key drift")
    if _integer(replay, "first_touch_observation_active_sessions", minimum=1) != 20:
        raise OutcomeConfigError("first-touch observation must remain 20 active sessions")
    _boolean(replay, "portfolio_position_continues_after_censor", expected=True)
    if _string(replay, "occupied_signal_behavior") != "LOG_AND_SKIP":
        raise OutcomeConfigError("occupied signals must be logged and skipped")
    if (
        _string(replay, "terminal_exit") != TERMINAL_EXIT_POLICY
        or _string(replay, "terminal_partition_resolution") != TERMINAL_PARTITION_RESOLUTION_POLICY
        or _string(replay, "terminal_exit_failure") != "HARD_FAILURE"
    ):
        raise OutcomeConfigError("terminal exit/resolution policy drift")

    if (
        _string(checkpoint, "boundary") != "SOURCE_DATE_COMPLETE"
        or _integer(checkpoint, "source_dates_per_checkpoint", minimum=1) != 1
    ):
        raise OutcomeConfigError("checkpoint boundary/cadence drift")
    for name in (
        "resume_requires_exact_run_fingerprint",
        "resume_requires_exact_cache_manifest",
        "resume_requires_preceding_checkpoint_sha256",
    ):
        _boolean(checkpoint, name, expected=True)

    if _string(results, "artifact_schema") != OUTCOME_ARTIFACT_SCHEMA:
        raise OutcomeConfigError("outcome artifact schema drift")
    if tuple(results.get("required_scenarios", ())) != EXPECTED_SCENARIO_IDS:
        raise OutcomeConfigError("required result scenarios drift")
    if results.get("required_directions") != ["LONG", "SHORT"]:
        raise OutcomeConfigError("required result directions drift")
    expected_cells = len(screening.barrier_ticks) ** 2
    if _integer(results, "expected_grid_cell_count", minimum=1) != expected_cells:
        raise OutcomeConfigError("result grid cell count drift")
    if _integer(results, "expected_scenario_direction_cell_count", minimum=1) != (
        len(EXPECTED_SCENARIO_IDS) * 2 * expected_cells
    ):
        raise OutcomeConfigError("scenario/direction result cardinality drift")
    for name in (
        "append_only_postgresql_registry",
        "record_every_signal",
        "record_every_grid_cell",
        "record_entry_not_filled",
        "record_skipped_occupied",
        "record_censored",
        "record_terminal_exit",
        "record_all_execution_variables",
    ):
        _boolean(results, name, expected=True)

    if (
        _string(sequence, "first_query_id") != P5_QUERY_ID
        or _string(sequence, "second_query_id") != "p1_05_unconfirmed_move_reversal"
        or _string(sequence, "second_query_may_start_after")
        != "P5_COMPLETE_AND_LINEAGE_RESUME_AUDIT_PASSED"
    ):
        raise OutcomeConfigError("campaign query sequence drift")

    return OutcomeReplayConfig(
        path=path,
        sha256=canonical_sha256(document),
        config_id=_string(replay_id, "id"),
        campaign_key=_string(replay_id, "campaign_key"),
        query_id=P5_QUERY_ID,
        slice_indices=slices,
        source_dates_per_slice=_integer(discovery, "source_dates_per_slice", minimum=1),
        expected_signal_count=expected_signal_count,
        expected_direction_counts=(("LONG", long_count), ("SHORT", short_count)),
        expected_signal_source_date_count=EXPECTED_SIGNAL_SOURCE_DATE_COUNT,
        expected_contract_count=EXPECTED_CONTRACT_COUNT,
        expected_cache_partition_count=EXPECTED_CACHE_PARTITION_COUNT,
        expected_completed_source_date_count=EXPECTED_COMPLETED_SOURCE_DATE_COUNT,
        expected_last_completed_source_date=EXPECTED_LAST_COMPLETED_SOURCE_DATE,
        expected_artifact_manifest_sha256=EXPECTED_ARTIFACT_MANIFEST_SHA256,
        expected_signal_manifest_sha256=EXPECTED_SIGNAL_MANIFEST_SHA256,
        expected_input_plan_sha256=EXPECTED_INPUT_PLAN_SHA256,
        screening_bundle=screening,
        discovery_config_sha256=discovery_config.sha256,
        discovery_definition_sha256=discovery_config.definition_sha256,
        scenarios=_scenarios(root),
        barrier_ticks=screening.barrier_ticks,
        first_touch_observation_sessions=20,
        maximum_cache_workers=maximum_workers,
        cache_output_relative=_relative_path(
            cache["output_directory"], label="cache.output_directory", derived=True
        ),
        checkpoint_output_relative=_relative_path(
            checkpoint["output_directory"],
            label="checkpoint.output_directory",
            derived=True,
        ),
        result_output_relative=_relative_path(
            results["output_directory"], label="results.output_directory", derived=True
        ),
        source_footer_manifest_relative=_relative_path(
            policy["source_footer_manifest"],
            label="source_footer_manifest",
            derived=True,
        ),
        source_sha256_manifest_relative=_relative_path(
            policy["source_sha256_manifest"],
            label="source_sha256_manifest",
            derived=True,
        ),
        eligible_calendar_relative=_relative_path(
            policy["eligible_calendar_artifact"],
            label="eligible_calendar_artifact",
            derived=True,
        ),
        split_relative=_relative_path(
            policy["split_artifact"], label="split_artifact", derived=True
        ),
    )
