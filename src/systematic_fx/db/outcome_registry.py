"""Governed persistence for the ordered Phase 1A MBP-10 outcome replays.

The immutable :class:`~systematic_fx.research.run_spec.RunSpec` and its append-
preserved run attempts remain the execution authority.  This module adds only
the replay-specific state that does not fit that generic ledger: a manifest per
real attempt, an append-only source-date checkpoint chain, and the normalized economic
surface.  A successful publication is one SERIALIZABLE transaction containing
all 2,904 cells, the content-addressed result artifact, the successful generic
attempt, and the replay manifest transition.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Mapping, Sequence
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from functools import wraps
from pathlib import Path, PurePosixPath
from typing import Any, Final, ParamSpec, TypeVar
from urllib.parse import unquote, urlparse

import psycopg
from psycopg import IsolationLevel
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.backtest.barriers import Direction
from systematic_fx.backtest.economics import (
    CellEconomics,
    EconomicSurface,
    select_stable_screening_cell,
)
from systematic_fx.db.postgres_retry import retry_serialization_failures
from systematic_fx.research.run_spec import RUN_SPEC_SCHEMA, RUN_SPEC_SCHEMA_VERSION

CAMPAIGN_KEY: Final = "phase1a_conservative_screening_v1"
P5_QUERY_ID: Final = "p5_01_range_expansion_flow_continuation"
P1_05_QUERY_ID: Final = "p1_05_unconfirmed_move_reversal"
PATTERN_KEY: Final = P5_QUERY_ID
OUTCOME_ENGINE_VERSION: Final = "phase1a_shared_outcome_replay_v1"
OUTCOME_CONFIG_ID: Final = "phase1a_p5_outcome_replay_v1"
OUTCOME_ARTIFACT_SCHEMA: Final = "systematic_fx.phase1a_p5_outcome_replay.v1"
P1_05_OUTCOME_CONFIG_ID: Final = "phase1a_p1_05_outcome_replay_v1"
P1_05_OUTCOME_ARTIFACT_SCHEMA: Final = "systematic_fx.phase1a_p1_05_outcome_replay.v1"
CHECKPOINT_ARTIFACT_SCHEMA: Final = "systematic_fx.phase1a_outcome_checkpoint.v1"
DISCOVERY_ARTIFACT_SCHEMA: Final = "systematic_fx.phase1a_discovery_slice.v1"
OUTCOME_ARTIFACT_TYPE: Final = "PHASE1A_OUTCOME_REPLAY_RESULT"
CHECKPOINT_ARTIFACT_TYPE: Final = "PHASE1A_OUTCOME_REPLAY_CHECKPOINT"

SCENARIO_IDS: Final = (
    "BASELINE",
    "MODERATE_COMBINED",
    "SEVERE_DIAGNOSTIC",
)
DIRECTION_IDS: Final = ("LONG", "SHORT")
BARRIER_TICKS: Final = tuple(range(24, 193, 8))
EXPECTED_SOURCE_SLICE_COUNT: Final = 99
EXPECTED_SOURCE_OCCURRENCE_COUNT: Final = 1111
EXPECTED_DIRECTION_SIGNAL_COUNTS: Final = {"LONG": 529, "SHORT": 582}
EXPECTED_CACHE_PARTITION_COUNT: Final = 485
EXPECTED_PLANNED_SOURCE_DATE_COUNT: Final = 485
EXPECTED_FINAL_SOURCE_DATE: Final = date(2023, 8, 31)
EXPECTED_CELL_COUNT: Final = 484
EXPECTED_SUMMARY_COUNT: Final = len(SCENARIO_IDS) * len(DIRECTION_IDS) * EXPECTED_CELL_COUNT
EXPECTED_DETAIL_RECORD_COUNT: Final = (
    EXPECTED_SOURCE_OCCURRENCE_COUNT * len(SCENARIO_IDS) * EXPECTED_CELL_COUNT
)
P1_05_EXPECTED_SOURCE_OCCURRENCE_COUNT: Final = 943
P1_05_EXPECTED_DIRECTION_SIGNAL_COUNTS: Final = {"LONG": 446, "SHORT": 497}
P1_05_EXPECTED_CACHE_PARTITION_COUNT: Final = 478
P1_05_EXPECTED_PLANNED_SOURCE_DATE_COUNT: Final = 478
P1_05_EXPECTED_DETAIL_RECORD_COUNT: Final = (
    P1_05_EXPECTED_SOURCE_OCCURRENCE_COUNT * len(SCENARIO_IDS) * EXPECTED_CELL_COUNT
)
SCENARIO_COST_TICKS_PER_FILL: Final = {
    "BASELINE": (4, 4),
    "MODERATE_COMBINED": (5, 5),
    "SEVERE_DIAGNOSTIC": (6, 6),
}

_CACHE_MANIFEST_SCHEMA: Final = "systematic_fx.phase1a_outcome_cache_manifest.v1"
_CACHE_MANIFEST_VERSION: Final = "phase1a_outcome_cache_manifest_v1"
_CACHE_SCHEMA: Final = "systematic_fx.phase1a_daily_executable_cache.v1"
_CACHE_VERSION: Final = "phase1a_daily_executable_cache_v1"
_CHECKPOINT_PROGRESS_SCHEMA: Final = "systematic_fx.phase1a_outcome_progress.v1"
_TERMINAL_EXIT_POLICY: Final = "LAST_VALID_EXECUTABLE_QUOTE_BEFORE_EXPIRY_MONTH_START"
_TERMINAL_PARTITION_RESOLUTION_POLICY: Final = (
    "REVERSE_SCAN_LAST_VALID_EXECUTABLE_QUOTE_PARTITION_V1"
)
_DETAIL_SHARD_DIRECTORY: Final = PurePosixPath(
    "outcomes/phase1a_p5_outcome_replay_v1/detail_shards"
)
_CHECKPOINT_DIRECTORY: Final = PurePosixPath("outcomes/checkpoints/phase1a_p5_outcome_replay_v1")
_EQUIVALENCE_AUDIT_DIRECTORY: Final = PurePosixPath(
    "outcomes/audits/phase1a_p5_outcome_equivalence_v1"
)
_CACHE_MANIFEST_DIRECTORY: Final = PurePosixPath(
    "backtest_event_cache/phase1a_daily_executable_cache_v1/manifests"
)


@dataclass(frozen=True, slots=True)
class OutcomeQueryProfile:
    """One frozen candidate identity accepted by the shared replay registry."""

    query_id: str
    outcome_config_id: str
    outcome_artifact_schema: str
    source_slice_count: int
    source_occurrence_count: int
    direction_signal_counts: Mapping[str, int]
    cache_partition_count: int
    planned_source_date_count: int
    final_source_date: date

    @property
    def detail_record_count(self) -> int:
        return self.source_occurrence_count * len(SCENARIO_IDS) * EXPECTED_CELL_COUNT

    @property
    def detail_shard_directory(self) -> PurePosixPath:
        return PurePosixPath("outcomes") / self.outcome_config_id / "detail_shards"

    @property
    def checkpoint_directory(self) -> PurePosixPath:
        return PurePosixPath("outcomes/checkpoints") / self.outcome_config_id


P5_OUTCOME_QUERY_PROFILE: Final = OutcomeQueryProfile(
    query_id=P5_QUERY_ID,
    outcome_config_id=OUTCOME_CONFIG_ID,
    outcome_artifact_schema=OUTCOME_ARTIFACT_SCHEMA,
    source_slice_count=EXPECTED_SOURCE_SLICE_COUNT,
    source_occurrence_count=EXPECTED_SOURCE_OCCURRENCE_COUNT,
    direction_signal_counts=EXPECTED_DIRECTION_SIGNAL_COUNTS,
    cache_partition_count=EXPECTED_CACHE_PARTITION_COUNT,
    planned_source_date_count=EXPECTED_PLANNED_SOURCE_DATE_COUNT,
    final_source_date=EXPECTED_FINAL_SOURCE_DATE,
)
P1_05_OUTCOME_QUERY_PROFILE: Final = OutcomeQueryProfile(
    query_id=P1_05_QUERY_ID,
    outcome_config_id=P1_05_OUTCOME_CONFIG_ID,
    outcome_artifact_schema=P1_05_OUTCOME_ARTIFACT_SCHEMA,
    source_slice_count=EXPECTED_SOURCE_SLICE_COUNT,
    source_occurrence_count=P1_05_EXPECTED_SOURCE_OCCURRENCE_COUNT,
    direction_signal_counts=P1_05_EXPECTED_DIRECTION_SIGNAL_COUNTS,
    cache_partition_count=P1_05_EXPECTED_CACHE_PARTITION_COUNT,
    planned_source_date_count=P1_05_EXPECTED_PLANNED_SOURCE_DATE_COUNT,
    final_source_date=EXPECTED_FINAL_SOURCE_DATE,
)
_OUTCOME_QUERY_PROFILES: Final = {
    profile.query_id: profile for profile in (P5_OUTCOME_QUERY_PROFILE, P1_05_OUTCOME_QUERY_PROFILE)
}

_CACHE_MANIFEST_REFERENCE_FIELDS: Final = {
    "artifact_relative_uri",
    "artifact_sha256",
    "byte_size",
    "cache_count",
    "cache_entries_sha256",
    "cache_plan_sha256",
    "input_manifest_sha256",
}
_CACHE_MANIFEST_FIELDS: Final = {
    "artifact_schema",
    "artifact_version",
    "cache_count",
    "cache_entries",
    "cache_entries_sha256",
    "cache_plan_sha256",
    "cache_schema",
    "cache_version",
    "input_manifest_sha256",
    "partition_key",
}
_CACHE_ENTRY_FIELDS: Final = {
    "artifact_relative_uri",
    "artifact_sha256",
    "byte_size",
    "cached_quote_count",
    "event_index_offset",
    "first_event_index",
    "first_ts_recv_ns",
    "instrument_id",
    "last_event_index",
    "last_ts_recv_ns",
    "last_valid_event_index",
    "last_valid_ts_recv_ns",
    "raw_symbol",
    "source_date",
    "source_relative_uri",
    "source_row_count",
    "source_sha256",
    "valid_quote_count",
}
_DETAIL_SHARD_FIELDS: Final = {
    "artifact_relative_uri",
    "artifact_sha256",
    "byte_size",
    "record_manifest_sha256",
    "row_count",
    "run_fingerprint",
    "shard_sequence",
    "source_date",
}
_INPUT_LINEAGE_FIELDS: Final = {
    "cache_plan_sha256",
    "calendar_sha256",
    "discovery_input_manifest_sha256",
    "expected_completed_source_date_count",
    "expected_last_completed_source_date",
    "footer_manifest_sha256",
    "input_plan_sha256",
    "portable_artifact_manifest_sha256",
    "rich_source_artifact_manifest_sha256",
    "signal_manifest_sha256",
    "source_hash_manifest_sha256",
    "source_record_manifest_sha256",
    "split_sha256",
    "terminal_resolution_sha256",
}
_INPUT_LINEAGE_SHA256_FIELDS: Final = _INPUT_LINEAGE_FIELDS - {
    "expected_completed_source_date_count",
    "expected_last_completed_source_date",
}
_PREDECESSOR_INPUT_LINEAGE_FIELDS: Final = {
    "predecessor_cell_summaries_sha256",
    "predecessor_detail_shard_manifest_sha256",
    "predecessor_equivalence_audit_artifact_sha256",
    "predecessor_equivalence_audit_id",
    "predecessor_final_checkpoint_sha256",
    "predecessor_input_lineage_sha256",
    "predecessor_outcome_replay_manifest_id",
    "predecessor_result_artifact_sha256",
    "predecessor_run_fingerprint",
}
_PREDECESSOR_INPUT_LINEAGE_SHA256_FIELDS: Final = {
    field_name
    for field_name in _PREDECESSOR_INPUT_LINEAGE_FIELDS
    if field_name.endswith(("_sha256", "_fingerprint"))
}
_FINAL_CHECKPOINT_REFERENCE_FIELDS: Final = {
    "artifact_relative_uri",
    "artifact_sha256",
    "byte_size",
    "checkpoint_sequence",
    "last_completed_source_date",
    "progress_metadata",
    "progress_metadata_sha256",
}
_CHECKPOINT_FIELDS: Final = {
    "artifact_schema",
    "cache_manifest",
    "checkpoint_sequence",
    "completed_source_date_count",
    "detail_record_count",
    "detail_shard_manifest_sha256",
    "detail_shards",
    "input_lineage",
    "input_lineage_sha256",
    "last_completed_source_date",
    "outcome_config_id",
    "outcome_replay_manifest_id",
    "predecessor_checkpoint_sha256",
    "progress_metadata",
    "progress_metadata_sha256",
    "query_id",
    "replay_state",
    "replay_state_sha256",
    "run_fingerprint",
    "source_event_count",
}
_FINAL_RESULT_FIELDS: Final = {
    "artifact_schema",
    "cache_manifest",
    "cell_summaries",
    "cell_summaries_sha256",
    "detail_record_count",
    "detail_shard_count",
    "detail_shard_manifest_sha256",
    "detail_shards",
    "direction_ids",
    "final_checkpoint",
    "input_lineage",
    "input_lineage_sha256",
    "outcome_config_id",
    "query_id",
    "run_fingerprint",
    "scenario_ids",
    "source_artifact_manifest_sha256",
    "source_occurrence_count",
    "source_slice_count",
    "summary_row_count",
}

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MAX_ATTEMPT_NUMBER: Final = 2**31 - 1
_WRITE_BITS: Final = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
_P = ParamSpec("_P")
_R = TypeVar("_R")


class OutcomeRegistryError(RuntimeError):
    """The governed outcome replay could not be validated or persisted."""


class OutcomeRegistryDriftError(OutcomeRegistryError):
    """An immutable outcome identity already exists with different content."""


class OutcomeRegistryStateError(OutcomeRegistryError):
    """An outcome replay was asked to make an invalid state transition."""


class OutcomeRegistryDatabaseError(OutcomeRegistryError):
    """PostgreSQL rejected or could not complete an outcome registry operation."""


def outcome_query_profile(query_id: str = P5_QUERY_ID) -> OutcomeQueryProfile:
    """Resolve one frozen ordered-candidate profile and reject unknown queries."""

    canonical_query_id = _nonempty(query_id, label="query_id")
    try:
        return _OUTCOME_QUERY_PROFILES[canonical_query_id]
    except KeyError as error:
        raise OutcomeRegistryError(
            f"query_id is not an approved Phase 1A outcome candidate: {canonical_query_id}"
        ) from error


def _translate_psycopg_errors(
    operation: str,
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    """Retry whole serialization-aborted calls before exposing one stable API."""

    def decorate(function: Callable[_P, _R]) -> Callable[_P, _R]:
        @wraps(function)
        def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            try:
                return retry_serialization_failures(function, *args, **kwargs)
            except OutcomeRegistryError:
                raise
            except psycopg.Error as error:
                raise OutcomeRegistryDatabaseError(f"PostgreSQL {operation} failed") from error

        return wrapped

    return decorate


def _nonempty(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise OutcomeRegistryError(f"{label} must be a non-empty string")
    if value != value.strip():
        raise OutcomeRegistryError(f"{label} must not have leading or trailing whitespace")
    return value


def _sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise OutcomeRegistryError(f"{label} must be a lowercase SHA-256")
    return value


def _positive_identifier(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise OutcomeRegistryError(f"{label} must be a positive integer")
    return value


def _nonnegative_integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise OutcomeRegistryError(f"{label} must be a non-negative integer")
    return value


def _database_url(value: object) -> str:
    return _nonempty(value, label="database_url")


def _canonical_value(value: object, *, label: str) -> object:
    """Detach strict JSON values and reject floats, unordered values, and odd keys."""

    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        raise OutcomeRegistryError(f"{label} cannot contain binary floats")
    if isinstance(value, Mapping):
        result: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise OutcomeRegistryError(f"{label} mappings require non-empty string keys")
            result[key] = _canonical_value(item, label=f"{label}.{key}")
        return result
    if isinstance(value, (list, tuple)):
        return [
            _canonical_value(item, label=f"{label}[{index}]") for index, item in enumerate(value)
        ]
    raise OutcomeRegistryError(
        f"{label} contains unsupported canonical JSON type {type(value).__name__}"
    )


def _canonical_mapping(value: object, *, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise OutcomeRegistryError(f"{label} must be a mapping")
    result = _canonical_value(value, label=label)
    if not isinstance(result, dict):  # pragma: no cover - guarded above
        raise TypeError("canonical mapping did not remain a mapping")
    return result


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _decimal(value: object, *, label: str, optional: bool = False) -> Decimal | None:
    if optional and value is None:
        return None
    if not isinstance(value, Decimal) or not value.is_finite():
        suffix = " or None" if optional else ""
        raise OutcomeRegistryError(f"{label} must be a finite Decimal{suffix}")
    return value


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


@dataclass(frozen=True, slots=True)
class OutcomeCellSummary:
    """One fully loaded cell matching ``backtest.economics.CellEconomics``."""

    scenario_id: str
    direction: str
    take_profit_ticks: int
    stop_loss_ticks: int
    signal_count: int
    entry_fill_count: int
    entry_not_filled_count: int
    skipped_occupied_count: int
    take_profit_first_count: int
    stop_first_count: int
    terminal_exit_count: int
    censored_count: int
    gross_pnl_ticks: int
    variable_cost_ticks: int
    allocated_fixed_cost_ticks: int
    fully_loaded_net_pnl_ticks: int
    fully_loaded_net_ev_ticks: Decimal | None
    fully_loaded_net_pnl_usd: Decimal
    calendar_month_net_pnl_usd: Decimal
    profit_factor: Decimal | None
    maximum_drawdown_usd: Decimal
    complete: bool

    def __post_init__(self) -> None:
        if self.scenario_id not in SCENARIO_IDS:
            raise OutcomeRegistryError("unknown Phase 1A outcome scenario")
        if self.direction not in DIRECTION_IDS:
            raise OutcomeRegistryError("direction must be LONG or SHORT")
        if self.take_profit_ticks not in BARRIER_TICKS:
            raise OutcomeRegistryError("take_profit_ticks is outside the frozen grid")
        if self.stop_loss_ticks not in BARRIER_TICKS:
            raise OutcomeRegistryError("stop_loss_ticks is outside the frozen grid")
        count_fields = (
            "signal_count",
            "entry_fill_count",
            "entry_not_filled_count",
            "skipped_occupied_count",
            "take_profit_first_count",
            "stop_first_count",
            "terminal_exit_count",
            "censored_count",
            "variable_cost_ticks",
            "allocated_fixed_cost_ticks",
        )
        for field_name in count_fields:
            _nonnegative_integer(getattr(self, field_name), label=field_name)
        for field_name in ("gross_pnl_ticks", "fully_loaded_net_pnl_ticks"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise OutcomeRegistryError(f"{field_name} must be an integer")

        if self.signal_count != (
            self.entry_fill_count + self.entry_not_filled_count + self.skipped_occupied_count
        ):
            raise OutcomeRegistryError("signal accounting does not balance")
        if self.entry_fill_count != (
            self.take_profit_first_count
            + self.stop_first_count
            + self.terminal_exit_count
            + self.censored_count
        ):
            raise OutcomeRegistryError("entry outcome accounting does not balance")
        if self.fully_loaded_net_pnl_ticks != (
            self.gross_pnl_ticks - self.variable_cost_ticks - self.allocated_fixed_cost_ticks
        ):
            raise OutcomeRegistryError("fully loaded tick accounting does not balance")
        _decimal(
            self.fully_loaded_net_ev_ticks,
            label="fully_loaded_net_ev_ticks",
            optional=True,
        )
        _decimal(
            self.fully_loaded_net_pnl_usd,
            label="fully_loaded_net_pnl_usd",
        )
        _decimal(
            self.calendar_month_net_pnl_usd,
            label="calendar_month_net_pnl_usd",
        )
        profit_factor = _decimal(
            self.profit_factor,
            label="profit_factor",
            optional=True,
        )
        maximum_drawdown = _decimal(
            self.maximum_drawdown_usd,
            label="maximum_drawdown_usd",
        )
        if profit_factor is not None and profit_factor < 0:
            raise OutcomeRegistryError("profit_factor must be non-negative")
        if maximum_drawdown is None or maximum_drawdown < 0:  # defensive for type narrowing
            raise OutcomeRegistryError("maximum_drawdown_usd must be non-negative")
        if not isinstance(self.complete, bool):
            raise OutcomeRegistryError("complete must be a boolean")
        if self.complete != (self.censored_count == 0):
            raise OutcomeRegistryError("complete must agree with censored_count")

    @property
    def identity(self) -> tuple[str, str, int, int]:
        return (
            self.scenario_id,
            self.direction,
            self.take_profit_ticks,
            self.stop_loss_ticks,
        )

    @property
    def payload(self) -> dict[str, object]:
        return {
            "allocated_fixed_cost_ticks": self.allocated_fixed_cost_ticks,
            "calendar_month_net_pnl_usd": _decimal_text(self.calendar_month_net_pnl_usd),
            "censored_count": self.censored_count,
            "complete": self.complete,
            "direction": self.direction,
            "entry_fill_count": self.entry_fill_count,
            "entry_not_filled_count": self.entry_not_filled_count,
            "fully_loaded_net_ev_ticks": _decimal_text(self.fully_loaded_net_ev_ticks),
            "fully_loaded_net_pnl_ticks": self.fully_loaded_net_pnl_ticks,
            "fully_loaded_net_pnl_usd": _decimal_text(self.fully_loaded_net_pnl_usd),
            "gross_pnl_ticks": self.gross_pnl_ticks,
            "maximum_drawdown_usd": _decimal_text(self.maximum_drawdown_usd),
            "profit_factor": _decimal_text(self.profit_factor),
            "scenario_id": self.scenario_id,
            "signal_count": self.signal_count,
            "skipped_occupied_count": self.skipped_occupied_count,
            "stop_first_count": self.stop_first_count,
            "stop_loss_ticks": self.stop_loss_ticks,
            "take_profit_first_count": self.take_profit_first_count,
            "take_profit_ticks": self.take_profit_ticks,
            "terminal_exit_count": self.terminal_exit_count,
            "variable_cost_ticks": self.variable_cost_ticks,
        }

    @property
    def summary_sha256(self) -> str:
        return _canonical_sha256(self.payload)

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> OutcomeCellSummary:
        """Build from the public ``CellEconomics.payload`` representation."""

        document = dict(value)
        document.pop("cell_id", None)
        decimal_fields = (
            "fully_loaded_net_ev_ticks",
            "fully_loaded_net_pnl_usd",
            "calendar_month_net_pnl_usd",
            "profit_factor",
            "maximum_drawdown_usd",
        )
        for field_name in decimal_fields:
            raw = document.get(field_name)
            if raw is not None:
                if not isinstance(raw, str):
                    raise OutcomeRegistryError(
                        f"{field_name} payload must be a canonical decimal string or null"
                    )
                try:
                    document[field_name] = Decimal(raw)
                except Exception as error:
                    raise OutcomeRegistryError(
                        f"{field_name} payload is not a valid Decimal"
                    ) from error
        try:
            return cls(**document)  # type: ignore[arg-type]
        except TypeError as error:
            raise OutcomeRegistryError("cell summary field schema drift") from error


def _expected_cell_identities() -> tuple[tuple[str, str, int, int], ...]:
    return tuple(
        (scenario, direction, take_profit, stop_loss)
        for scenario in SCENARIO_IDS
        for direction in DIRECTION_IDS
        for take_profit in BARRIER_TICKS
        for stop_loss in BARRIER_TICKS
    )


def validate_complete_cell_summaries(
    summaries: Sequence[OutcomeCellSummary],
    *,
    query_id: str = P5_QUERY_ID,
) -> tuple[tuple[OutcomeCellSummary, ...], str]:
    """Return canonical cell order and its aggregate content digest."""

    profile = outcome_query_profile(query_id)
    if isinstance(summaries, (str, bytes)) or not isinstance(summaries, Sequence):
        raise OutcomeRegistryError("cell_summaries must be a sequence")
    if len(summaries) != EXPECTED_SUMMARY_COUNT:
        raise OutcomeRegistryError(
            f"cell_summaries must contain exactly {EXPECTED_SUMMARY_COUNT} rows"
        )
    by_identity: dict[tuple[str, str, int, int], OutcomeCellSummary] = {}
    for value in summaries:
        if not isinstance(value, OutcomeCellSummary):
            raise OutcomeRegistryError("cell_summaries must contain OutcomeCellSummary values")
        if value.identity in by_identity:
            raise OutcomeRegistryError(f"duplicate cell summary identity: {value.identity}")
        expected_signal_count = profile.direction_signal_counts[value.direction]
        if value.signal_count != expected_signal_count:
            raise OutcomeRegistryError(
                f"{value.direction} cell signal_count must equal "
                f"{expected_signal_count} at Phase 1A completion"
            )
        variable_per_fill, fixed_per_fill = SCENARIO_COST_TICKS_PER_FILL[value.scenario_id]
        if value.variable_cost_ticks != value.entry_fill_count * variable_per_fill:
            raise OutcomeRegistryError(
                f"{value.scenario_id} variable cost must equal "
                f"{variable_per_fill} ticks per filled entry at Phase 1A completion"
            )
        if value.allocated_fixed_cost_ticks != value.entry_fill_count * fixed_per_fill:
            raise OutcomeRegistryError(
                f"{value.scenario_id} allocated fixed cost must equal "
                f"{fixed_per_fill} ticks per filled entry at Phase 1A completion"
            )
        by_identity[value.identity] = value
    expected = _expected_cell_identities()
    if set(by_identity) != set(expected):
        raise OutcomeRegistryError("cell_summaries have a missing or unknown grid cell")
    ordered = tuple(by_identity[identity] for identity in expected)
    digest = _canonical_sha256([cell.payload for cell in ordered])
    return ordered, digest


@dataclass(frozen=True, slots=True)
class OutcomePredecessorGate:
    """Immutable p5 completion/equivalence proof required before p1_05."""

    equivalence_audit_id: int
    equivalence_audit_artifact_sha256: str
    predecessor_outcome_replay_manifest_id: int
    predecessor_run_fingerprint: str
    predecessor_result_artifact_sha256: str
    predecessor_input_lineage_sha256: str
    predecessor_cell_summaries_sha256: str
    predecessor_detail_shard_manifest_sha256: str
    predecessor_final_checkpoint_sha256: str

    @property
    def parameters(self) -> dict[str, object]:
        return {
            "predecessor_cell_summaries_sha256": self.predecessor_cell_summaries_sha256,
            "predecessor_detail_shard_manifest_sha256": (
                self.predecessor_detail_shard_manifest_sha256
            ),
            "predecessor_equivalence_audit_artifact_sha256": (
                self.equivalence_audit_artifact_sha256
            ),
            "predecessor_equivalence_audit_id": self.equivalence_audit_id,
            "predecessor_final_checkpoint_sha256": (self.predecessor_final_checkpoint_sha256),
            "predecessor_input_lineage_sha256": self.predecessor_input_lineage_sha256,
            "predecessor_outcome_replay_manifest_id": (self.predecessor_outcome_replay_manifest_id),
            "predecessor_result_artifact_sha256": (self.predecessor_result_artifact_sha256),
            "predecessor_run_fingerprint": self.predecessor_run_fingerprint,
        }


def phase1a_outcome_parameters(
    source_artifact_manifest_sha256: str,
    *,
    query_id: str = P5_QUERY_ID,
    predecessor_gate: OutcomePredecessorGate | None = None,
) -> dict[str, object]:
    """Return the exact query-aware RunSpec subset enforced by PostgreSQL."""

    profile = outcome_query_profile(query_id)
    source_sha256 = _sha256(
        source_artifact_manifest_sha256,
        label="source_artifact_manifest_sha256",
    )
    parameters: dict[str, object] = {
        "cell_count_per_surface": EXPECTED_CELL_COUNT,
        "direction_ids": list(DIRECTION_IDS),
        "expected_detail_record_count": profile.detail_record_count,
        "expected_direction_signal_counts": dict(profile.direction_signal_counts),
        "expected_summary_count": EXPECTED_SUMMARY_COUNT,
        "final_source_date": profile.final_source_date.isoformat(),
        "outcome_config_id": profile.outcome_config_id,
        "planned_source_date_count": profile.planned_source_date_count,
        "query_id": profile.query_id,
        "scenario_ids": list(SCENARIO_IDS),
        "scenario_cost_ticks_per_fill": {
            scenario_id: {
                "allocated_fixed": costs[1],
                "variable": costs[0],
            }
            for scenario_id, costs in SCENARIO_COST_TICKS_PER_FILL.items()
        },
        "source_artifact_manifest_sha256": source_sha256,
        "source_occurrence_count": profile.source_occurrence_count,
        "source_slice_count": profile.source_slice_count,
        "stop_loss_ticks": list(BARRIER_TICKS),
        "take_profit_ticks": list(BARRIER_TICKS),
    }
    if profile.query_id == P1_05_QUERY_ID:
        if not isinstance(predecessor_gate, OutcomePredecessorGate):
            raise OutcomeRegistryStateError(
                "p1_05 outcome parameters require a verified p5 predecessor gate"
            )
        parameters.update(predecessor_gate.parameters)
    elif predecessor_gate is not None:
        raise OutcomeRegistryError("p5 outcome parameters cannot bind a predecessor gate")
    return parameters


def phase1a_p5_outcome_parameters(source_artifact_manifest_sha256: str) -> dict[str, object]:
    """Backward-compatible p5 RunSpec parameter builder."""

    return phase1a_outcome_parameters(source_artifact_manifest_sha256)


@dataclass(frozen=True, slots=True)
class P5SourceArtifactDescriptor:
    """One byte-verified canonical AI_SLICE artifact containing p5 occurrences."""

    slice_index: int
    discovery_exposure_id: int
    research_run_spec_id: int
    result_artifact_id: int
    run_fingerprint: str
    path: Path
    sha256: str
    byte_size: int
    requested_source_dates: tuple[str, ...]
    occurrence_count: int

    @property
    def manifest_payload(self) -> dict[str, object]:
        return {
            "byte_size": self.byte_size,
            "occurrence_count": self.occurrence_count,
            "requested_source_dates": list(self.requested_source_dates),
            "run_fingerprint": self.run_fingerprint,
            "sha256": self.sha256,
            "slice_index": self.slice_index,
        }


@dataclass(frozen=True, slots=True)
class P5SourceArtifactSet:
    """The exact ordered set of all 99 immutable p5 AI_SLICE inputs."""

    artifacts: tuple[P5SourceArtifactDescriptor, ...]
    source_artifact_manifest_sha256: str
    occurrence_count: int


OutcomeSourceArtifactDescriptor = P5SourceArtifactDescriptor
OutcomeSourceArtifactSet = P5SourceArtifactSet


@dataclass(frozen=True, slots=True)
class OutcomeReplayReservation:
    """A fresh/resumed replay attempt or an append-preserved duplicate skip."""

    outcome_replay_manifest_id: int
    research_run_spec_id: int
    research_run_attempt_id: int
    attempt_number: int
    attempt_status: str
    replay_status: str
    execute: bool
    reused_attempt_id: int | None
    created_manifest: bool


@dataclass(frozen=True, slots=True)
class OutcomeReplayState:
    """Validated current state for one real outcome replay attempt."""

    outcome_replay_manifest_id: int
    research_run_spec_id: int
    research_run_attempt_id: int
    attempt_number: int
    run_fingerprint: str
    status: str
    source_artifact_manifest_sha256: str
    result_artifact_id: int | None
    result_artifact_sha256: str | None
    cell_summaries_sha256: str | None


@dataclass(frozen=True, slots=True)
class OutcomeCheckpointReport:
    """One append-only SOURCE_DATE_COMPLETE checkpoint artifact."""

    outcome_replay_manifest_id: int
    run_fingerprint: str
    checkpoint_sequence: int
    completed_source_date_count: int
    last_completed_source_date: date
    source_event_count: int
    checkpoint_artifact_id: int
    checkpoint_artifact_sha256: str
    checkpoint_artifact_uri: str
    checkpoint_artifact_byte_size: int
    predecessor_checkpoint_sha256: str | None
    created: bool


@dataclass(frozen=True, slots=True)
class LoadedOutcomeCheckpoint:
    """Byte-verified latest checkpoint state for one resumable replay."""

    outcome_replay_manifest_id: int
    research_run_spec_id: int
    research_run_attempt_id: int
    run_fingerprint: str
    manifest_status: str
    checkpoint_sequence: int
    completed_source_date_count: int
    last_completed_source_date: date
    source_event_count: int
    checkpoint_artifact_id: int
    checkpoint_artifact_sha256: str
    checkpoint_artifact_uri: str
    checkpoint_artifact_byte_size: int
    checkpoint_artifact_path: Path
    predecessor_checkpoint_sha256: str | None
    progress_metadata: dict[str, object]
    progress_metadata_sha256: str
    checkpoint_document: dict[str, Any]


@dataclass(frozen=True, slots=True)
class OutcomeCompletionReport:
    """Immutable successful replay and result artifact identities."""

    outcome_replay_manifest_id: int
    research_run_spec_id: int
    research_run_attempt_id: int
    result_artifact_id: int
    run_fingerprint: str
    result_artifact_sha256: str
    result_artifact_uri: str
    result_artifact_byte_size: int
    cell_summaries_sha256: str
    summary_row_count: int
    created_artifact: bool
    completed: bool


@dataclass(frozen=True, slots=True)
class OutcomeReplayAuditCheckpoint:
    """One byte-verified checkpoint identity exposed to the audit runner."""

    checkpoint_sequence: int
    checkpoint_artifact_sha256: str
    checkpoint_artifact_byte_size: int
    predecessor_checkpoint_sha256: str | None
    last_completed_source_date: date
    source_event_count: int
    progress_metadata: dict[str, object]

    @property
    def progress_metadata_sha256(self) -> str:
        return _canonical_sha256(self.progress_metadata)

    @property
    def identity_payload(self) -> dict[str, object]:
        return {
            "checkpoint_artifact_byte_size": self.checkpoint_artifact_byte_size,
            "checkpoint_artifact_sha256": self.checkpoint_artifact_sha256,
            "checkpoint_sequence": self.checkpoint_sequence,
            "last_completed_source_date": self.last_completed_source_date.isoformat(),
            "predecessor_checkpoint_sha256": self.predecessor_checkpoint_sha256,
            "progress_metadata_sha256": self.progress_metadata_sha256,
            "source_event_count": self.source_event_count,
        }


@dataclass(frozen=True, slots=True)
class OutcomeReplayAuditSubject:
    """Complete immutable p5 success identity consumed by an equivalence audit."""

    outcome_replay_manifest_id: int
    research_run_spec_id: int
    research_run_attempt_id: int
    run_fingerprint: str
    status: str
    query_id: str
    source_artifact_manifest_sha256: str
    result_artifact_id: int
    result_artifact_path: Path
    result_artifact_sha256: str
    result_artifact_byte_size: int
    cell_summaries_sha256: str
    cache_manifest_sha256: str
    detail_shard_manifest_sha256: str
    input_lineage_sha256: str
    final_checkpoint_sha256: str
    final_checkpoint_sequence: int
    source_event_count: int
    detail_record_count: int
    summary_row_count: int
    checkpoints: tuple[OutcomeReplayAuditCheckpoint, ...]

    @property
    def checkpoint_chain_sha256(self) -> str:
        return _canonical_sha256([checkpoint.identity_payload for checkpoint in self.checkpoints])

    @property
    def subject_payload(self) -> dict[str, object]:
        return {
            "cache_manifest_sha256": self.cache_manifest_sha256,
            "cell_summaries_sha256": self.cell_summaries_sha256,
            "checkpoint_chain_sha256": self.checkpoint_chain_sha256,
            "checkpoint_count": len(self.checkpoints),
            "detail_record_count": self.detail_record_count,
            "detail_shard_manifest_sha256": self.detail_shard_manifest_sha256,
            "final_checkpoint_sequence": self.final_checkpoint_sequence,
            "final_checkpoint_sha256": self.final_checkpoint_sha256,
            "input_lineage_sha256": self.input_lineage_sha256,
            "outcome_config_id": OUTCOME_CONFIG_ID,
            "outcome_replay_manifest_id": self.outcome_replay_manifest_id,
            "query_id": self.query_id,
            "research_run_attempt_id": self.research_run_attempt_id,
            "research_run_spec_id": self.research_run_spec_id,
            "result_artifact_byte_size": self.result_artifact_byte_size,
            "result_artifact_sha256": self.result_artifact_sha256,
            "run_fingerprint": self.run_fingerprint,
            "source_artifact_manifest_sha256": self.source_artifact_manifest_sha256,
            "source_event_count": self.source_event_count,
            "status": self.status,
            "summary_row_count": self.summary_row_count,
        }


@dataclass(frozen=True, slots=True)
class OutcomeEquivalenceAuditReport:
    """One append-preserved successful p5 uninterrupted/resumed comparison."""

    outcome_equivalence_audit_id: int
    predecessor_outcome_replay_manifest_id: int
    validation_research_run_spec_id: int
    validation_research_run_attempt_id: int
    validation_run_fingerprint: str
    audit_artifact_id: int
    audit_artifact_sha256: str
    audit_artifact_uri: str
    audit_artifact_byte_size: int
    checkpoint_chain_sha256: str
    passed: bool
    created: bool


@dataclass(frozen=True, slots=True)
class LoadedOutcomeEquivalenceAudit:
    """Byte-verified PASSED audit selected by its successful attempt."""

    audit: OutcomeEquivalenceAuditReport
    predecessor_gate: OutcomePredecessorGate
    audit_artifact_path: Path


@dataclass(frozen=True, slots=True)
class OutcomeScreeningDecision:
    """One direction's terminal conservative-screening status."""

    direction: str
    decision_label: str
    selected_take_profit_ticks: int | None
    selected_stop_loss_ticks: int | None
    positive_region_size: int
    rejection_reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.direction not in DIRECTION_IDS:
            raise OutcomeRegistryError("screening decision direction must be LONG or SHORT")
        if self.decision_label not in {"SCREENING_REJECT", "SCREENING_SURVIVOR"}:
            raise OutcomeRegistryError("unknown outcome screening decision label")
        if (self.selected_take_profit_ticks is None) != (self.selected_stop_loss_ticks is None):
            raise OutcomeRegistryError("selected TP and SL must be present or absent together")
        if self.decision_label == "SCREENING_SURVIVOR" and (
            self.selected_take_profit_ticks is None
        ):
            raise OutcomeRegistryError("a screening survivor requires one selected cell")
        for label, value in (
            ("selected_take_profit_ticks", self.selected_take_profit_ticks),
            ("selected_stop_loss_ticks", self.selected_stop_loss_ticks),
        ):
            if value is not None and value not in BARRIER_TICKS:
                raise OutcomeRegistryError(f"{label} is outside the frozen grid")
        _nonnegative_integer(self.positive_region_size, label="positive_region_size")
        if (
            not isinstance(self.rejection_reasons, tuple)
            or any(
                not isinstance(reason, str) or not reason.strip()
                for reason in self.rejection_reasons
            )
            or len(set(self.rejection_reasons)) != len(self.rejection_reasons)
        ):
            raise OutcomeRegistryError("rejection_reasons must be unique non-empty strings")
        if self.decision_label == "SCREENING_REJECT" and not self.rejection_reasons:
            raise OutcomeRegistryError("a screening rejection requires at least one reason")

    def payload(self, *, outcome_replay_manifest_id: int) -> dict[str, object]:
        return {
            "decision_label": self.decision_label,
            "direction": self.direction,
            "outcome_replay_manifest_id": outcome_replay_manifest_id,
            "positive_region_size": self.positive_region_size,
            "rejection_reasons": list(self.rejection_reasons),
            "selected_stop_loss_ticks": self.selected_stop_loss_ticks,
            "selected_take_profit_ticks": self.selected_take_profit_ticks,
        }


@dataclass(frozen=True, slots=True)
class _FileIdentity:
    device: int
    inode: int
    mode: int
    size: int
    modified_ns: int
    changed_ns: int


@dataclass(slots=True)
class _HeldFile:
    descriptor: int
    path: Path
    parent_path: Path
    parent_descriptor: int
    parent_identity: _FileIdentity
    filename: str
    identity: _FileIdentity
    sha256: str
    byte_size: int
    content: bytes

    def close(self) -> None:
        if self.descriptor >= 0:
            os.close(self.descriptor)
            self.descriptor = -1
        if self.parent_descriptor >= 0:
            os.close(self.parent_descriptor)
            self.parent_descriptor = -1


def _file_identity(value: os.stat_result) -> _FileIdentity:
    return _FileIdentity(
        device=value.st_dev,
        inode=value.st_ino,
        mode=value.st_mode,
        size=value.st_size,
        modified_ns=value.st_mtime_ns,
        changed_ns=value.st_ctime_ns,
    )


def _resolved_data_root(value: Path | str) -> tuple[Path, Path]:
    root = Path(value)
    if not root.is_absolute():
        root = Path.cwd() / root
    lexical = Path(os.path.abspath(os.fspath(root)))
    try:
        resolved = lexical.resolve(strict=True)
    except OSError as error:
        raise OutcomeRegistryError("data_root does not exist") from error
    if lexical != resolved or lexical.is_symlink() or lexical.name != "data":
        raise OutcomeRegistryError("data_root must be the real non-symlink data directory")
    derived = lexical / "derived"
    try:
        derived_mode = derived.lstat().st_mode
    except OSError as error:
        raise OutcomeRegistryError("data/derived does not exist") from error
    if not stat.S_ISDIR(derived_mode) or stat.S_ISLNK(derived_mode):
        raise OutcomeRegistryError("data/derived must be a real directory")
    return lexical, derived


def _assert_no_symlink_components(root: Path, path: Path) -> None:
    try:
        relative = path.relative_to(root)
    except ValueError as error:
        raise OutcomeRegistryError("artifact is outside data/derived") from error
    cursor = root
    for part in relative.parts:
        cursor /= part
        try:
            mode = cursor.lstat().st_mode
        except OSError as error:
            raise OutcomeRegistryError(f"artifact path is not reachable: {path}") from error
        if stat.S_ISLNK(mode):
            raise OutcomeRegistryError(f"artifact path contains a symlink: {path}")


def _same_file_node(left: _FileIdentity, right: _FileIdentity) -> bool:
    return (left.device, left.inode, stat.S_IFMT(left.mode)) == (
        right.device,
        right.inode,
        stat.S_IFMT(right.mode),
    )


def _directory_open_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _open_root_directory(path: Path, expected: _FileIdentity) -> int:
    try:
        before = _file_identity(path.lstat())
        descriptor = os.open(path, _directory_open_flags())
    except OSError as error:
        raise OutcomeRegistryError(f"cannot securely open artifact directory: {path}") from error
    try:
        opened = _file_identity(os.fstat(descriptor))
        after = _file_identity(path.lstat())
        if (
            not stat.S_ISDIR(opened.mode)
            or stat.S_ISLNK(before.mode)
            or not _same_file_node(before, expected)
            or not _same_file_node(opened, expected)
            or not _same_file_node(after, expected)
        ):
            raise OutcomeRegistryDriftError("artifact directory identity changed while opening")
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _openat_directory(
    parent_descriptor: int,
    name: str,
    expected: _FileIdentity,
) -> int:
    try:
        before = _file_identity(os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False))
        descriptor = os.open(
            name,
            _directory_open_flags(),
            dir_fd=parent_descriptor,
        )
    except OSError as error:
        raise OutcomeRegistryError(
            f"cannot securely open artifact directory component: {name}"
        ) from error
    try:
        opened = _file_identity(os.fstat(descriptor))
        after = _file_identity(os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False))
        if (
            not stat.S_ISDIR(opened.mode)
            or stat.S_ISLNK(before.mode)
            or not _same_file_node(before, expected)
            or not _same_file_node(opened, expected)
            or not _same_file_node(after, expected)
        ):
            raise OutcomeRegistryDriftError("artifact directory component changed while opening")
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _open_held_immutable_file(path: Path, *, data_root: Path | str) -> _HeldFile:
    root, derived = _resolved_data_root(data_root)
    candidate = path
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    candidate = Path(os.path.abspath(os.fspath(candidate)))
    _assert_no_symlink_components(derived, candidate)
    try:
        if candidate.resolve(strict=True) != candidate:
            raise OutcomeRegistryError("artifact path is not canonical")
    except OSError as error:
        raise OutcomeRegistryError(f"artifact path is not reachable: {candidate}") from error
    relative = candidate.relative_to(derived)
    root_identity = _file_identity(root.lstat())
    derived_identity = _file_identity(derived.lstat())
    component_identities: list[_FileIdentity] = []
    cursor = derived
    for part in relative.parts:
        cursor /= part
        component_identities.append(_file_identity(cursor.lstat()))

    current_descriptor = _open_root_directory(root, root_identity)
    current_path = root
    try:
        child_descriptor = _openat_directory(
            current_descriptor,
            "derived",
            derived_identity,
        )
        os.close(current_descriptor)
        current_descriptor = child_descriptor
        current_path = derived
        for part, identity in zip(
            relative.parts[:-1],
            component_identities[:-1],
            strict=True,
        ):
            child_descriptor = _openat_directory(
                current_descriptor,
                part,
                identity,
            )
            os.close(current_descriptor)
            current_descriptor = child_descriptor
            current_path /= part
        filename = relative.parts[-1]
        expected_file = component_identities[-1]
        try:
            entry_before = _file_identity(
                os.stat(
                    filename,
                    dir_fd=current_descriptor,
                    follow_symlinks=False,
                )
            )
            descriptor = os.open(
                filename,
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=current_descriptor,
            )
        except OSError as error:
            raise OutcomeRegistryError(
                f"cannot securely open immutable artifact: {candidate}"
            ) from error
        entry_after = _file_identity(
            os.stat(
                filename,
                dir_fd=current_descriptor,
                follow_symlinks=False,
            )
        )
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise OutcomeRegistryError("artifact must be a regular file")
        if before.st_mode & _WRITE_BITS:
            raise OutcomeRegistryError("artifact must be read-only before registration")
        identity = _file_identity(before)
        if (
            entry_before != expected_file
            or identity != expected_file
            or entry_after != expected_file
        ):
            raise OutcomeRegistryDriftError("artifact file identity changed while opening")
        digest = hashlib.sha256()
        chunks: list[bytes] = []
        offset = 0
        while chunk := os.pread(descriptor, 1024 * 1024, offset):
            digest.update(chunk)
            chunks.append(chunk)
            offset += len(chunk)
        content = b"".join(chunks)
        if _file_identity(os.fstat(descriptor)) != identity or len(content) != before.st_size:
            raise OutcomeRegistryDriftError("artifact changed while it was read")
        held = _HeldFile(
            descriptor=descriptor,
            path=candidate,
            parent_path=current_path,
            parent_descriptor=current_descriptor,
            parent_identity=_file_identity(os.fstat(current_descriptor)),
            filename=filename,
            identity=identity,
            sha256=digest.hexdigest(),
            byte_size=len(content),
            content=content,
        )
        _verify_held_file(held)
        return held
    except Exception:
        if "descriptor" in locals():
            os.close(descriptor)
        os.close(current_descriptor)
        raise


def _verify_held_file(held: _HeldFile) -> None:
    if held.descriptor < 0 or _file_identity(os.fstat(held.descriptor)) != held.identity:
        raise OutcomeRegistryDriftError("open artifact inode changed before commit")
    if held.parent_descriptor < 0 or not _same_file_node(
        _file_identity(os.fstat(held.parent_descriptor)),
        held.parent_identity,
    ):
        raise OutcomeRegistryDriftError("open artifact parent directory changed before commit")
    try:
        entry_identity = _file_identity(
            os.stat(
                held.filename,
                dir_fd=held.parent_descriptor,
                follow_symlinks=False,
            )
        )
        parent_path_identity = _file_identity(held.parent_path.lstat())
        path_identity = _file_identity(held.path.lstat())
    except OSError as error:
        raise OutcomeRegistryDriftError("artifact path disappeared before commit") from error
    if (
        entry_identity != held.identity
        or path_identity != held.identity
        or not stat.S_ISREG(path_identity.mode)
        or not _same_file_node(parent_path_identity, held.parent_identity)
    ):
        raise OutcomeRegistryDriftError("artifact path changed before commit")


def _require_held_parent(
    held: _HeldFile,
    *,
    data_root: Path | str,
    expected_parent: PurePosixPath,
    label: str,
) -> None:
    _, derived = _resolved_data_root(data_root)
    expected = derived.joinpath(*expected_parent.parts)
    if held.parent_path != expected or held.path.parent != expected:
        raise OutcomeRegistryDriftError(f"{label} is outside its frozen artifact directory")


def _path_from_file_uri(value: object, *, label: str) -> Path:
    if not isinstance(value, str):
        raise OutcomeRegistryDriftError(f"{label} URI is missing")
    parsed = urlparse(value)
    if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
        raise OutcomeRegistryDriftError(f"{label} must use a local file URI")
    return Path(unquote(parsed.path))


def _canonical_json_document(content: bytes, *, label: str) -> dict[str, Any]:
    try:
        decoded = content.decode("utf-8")
        value = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise OutcomeRegistryDriftError(f"{label} is not valid UTF-8 JSON") from error
    if not isinstance(value, dict):
        raise OutcomeRegistryDriftError(f"{label} must contain one JSON object")
    if _canonical_json_bytes(value) + b"\n" != content:
        raise OutcomeRegistryDriftError(f"{label} is not canonical JSON plus newline")
    return value


def _validate_source_artifact_document(
    document: Mapping[str, Any],
    *,
    run_fingerprint: str,
    slice_index: int,
    profile: OutcomeQueryProfile = P5_OUTCOME_QUERY_PROFILE,
) -> tuple[tuple[str, ...], int]:
    if document.get("artifact_schema") != DISCOVERY_ARTIFACT_SCHEMA:
        raise OutcomeRegistryDriftError("AI_SLICE artifact schema drift")
    if document.get("run_fingerprint") != run_fingerprint:
        raise OutcomeRegistryDriftError("AI_SLICE artifact run fingerprint drift")
    dates = document.get("requested_source_dates")
    if (
        not isinstance(dates, list)
        or len(dates) != 5
        or any(not isinstance(value, str) for value in dates)
    ):
        raise OutcomeRegistryDriftError("AI_SLICE requested source dates are invalid")
    query_results = document.get("query_results")
    if not isinstance(query_results, list):
        raise OutcomeRegistryDriftError("AI_SLICE query_results are invalid")
    matches: list[Mapping[str, Any]] = []
    for result in query_results:
        if not isinstance(result, Mapping):
            raise OutcomeRegistryDriftError("AI_SLICE query result is not an object")
        definition = result.get("definition")
        if isinstance(definition, Mapping) and definition.get("id") == profile.query_id:
            matches.append(result)
    if len(matches) != 1:
        query_label = "p5" if profile.query_id == P5_QUERY_ID else profile.query_id
        raise OutcomeRegistryDriftError(
            f"AI_SLICE {slice_index} must contain exactly one canonical {query_label} query"
        )
    query_result = matches[0]
    occurrences = query_result.get("occurrences")
    support_count = query_result.get("support_count")
    direction_counts = query_result.get("direction_counts")
    if (
        not isinstance(occurrences, list)
        or isinstance(support_count, bool)
        or not isinstance(support_count, int)
        or support_count != len(occurrences)
        or not isinstance(direction_counts, Mapping)
        or set(direction_counts) != set(DIRECTION_IDS)
        or any(
            isinstance(direction_counts[key], bool)
            or not isinstance(direction_counts[key], int)
            or direction_counts[key] < 0
            for key in DIRECTION_IDS
        )
        or sum(int(direction_counts[key]) for key in DIRECTION_IDS) != support_count
    ):
        raise OutcomeRegistryDriftError("AI_SLICE outcome occurrence accounting drift")
    for occurrence in occurrences:
        if (
            not isinstance(occurrence, Mapping)
            or occurrence.get("direction") not in DIRECTION_IDS
            or occurrence.get("source_date") not in dates
        ):
            raise OutcomeRegistryDriftError("AI_SLICE outcome occurrence identity drift")
    return tuple(dates), support_count


def _set_serializable(connection: psycopg.Connection[dict[str, Any]]) -> None:
    connection.isolation_level = IsolationLevel.SERIALIZABLE


def _set_serializable_read_only(
    connection: psycopg.Connection[dict[str, Any]],
) -> None:
    connection.isolation_level = IsolationLevel.SERIALIZABLE
    connection.read_only = True


@_translate_psycopg_errors("Phase 1A outcome source artifact loading")
def load_phase1a_p5_source_artifacts(
    database_url: str,
    *,
    data_root: Path | str,
    query_id: str = P5_QUERY_ID,
) -> P5SourceArtifactSet:
    """Load the byte-verified AI_SLICE inputs for one frozen candidate."""

    target = _database_url(database_url)
    profile = outcome_query_profile(query_id)
    _resolved_data_root(data_root)
    with psycopg.connect(target, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with connection.transaction():
            rows = connection.execute(
                """
                SELECT exposure.discovery_exposure_id, exposure.exposure_key,
                       run_spec.research_run_spec_id, run_spec.run_fingerprint,
                       run_spec.canonical_spec, attempt.research_run_attempt_id,
                       artifact.artifact_id AS result_artifact_id,
                       artifact.artifact_type, artifact.uri AS artifact_uri,
                       artifact.sha256 AS artifact_sha256,
                       artifact.byte_size AS artifact_byte_size
                FROM systematic_fx.discovery_exposures AS exposure
                JOIN systematic_fx.campaigns AS campaign
                  ON campaign.campaign_id = exposure.campaign_id
                JOIN systematic_fx.research_run_specs AS run_spec
                  ON run_spec.research_run_spec_id = exposure.research_run_spec_id
                 AND run_spec.campaign_id = exposure.campaign_id
                JOIN systematic_fx.research_run_attempts AS attempt
                  ON attempt.research_run_spec_id = run_spec.research_run_spec_id
                 AND attempt.status = 'SUCCEEDED'
                 AND attempt.result_artifact_id = exposure.result_artifact_id
                JOIN systematic_fx.artifacts AS artifact
                  ON artifact.artifact_id = exposure.result_artifact_id
                WHERE campaign.campaign_key = %s
                  AND exposure.exposure_type = 'AI_SLICE'
                  AND exposure.visible_to_ai = true
                  AND exposure.research_eligible = false
                  AND run_spec.run_kind = 'AI_SLICE'
                ORDER BY exposure.exposure_key
                FOR SHARE OF exposure, run_spec, attempt, artifact
                """,
                (CAMPAIGN_KEY,),
            ).fetchall()
            if len(rows) != profile.source_slice_count:
                raise OutcomeRegistryDriftError(
                    f"expected {profile.source_slice_count} successful AI_SLICE artifacts; "
                    f"found {len(rows)}"
                )

            descriptors: list[P5SourceArtifactDescriptor] = []
            for row in rows:
                canonical_spec = row.get("canonical_spec")
                if not isinstance(canonical_spec, dict):
                    raise OutcomeRegistryDriftError("AI_SLICE canonical RunSpec is invalid")
                parameters = canonical_spec.get("parameters")
                if not isinstance(parameters, dict):
                    raise OutcomeRegistryDriftError("AI_SLICE RunSpec parameters are invalid")
                raw_slice_index = parameters.get("slice_index")
                if (
                    isinstance(raw_slice_index, bool)
                    or not isinstance(raw_slice_index, int)
                    or not 0 <= raw_slice_index < profile.source_slice_count
                ):
                    raise OutcomeRegistryDriftError("AI_SLICE slice_index is invalid")
                expected_key = f"{CAMPAIGN_KEY}:ai-slice:{raw_slice_index:02d}"
                if row.get("exposure_key") != expected_key:
                    raise OutcomeRegistryDriftError("AI_SLICE exposure key drift")
                fingerprint = _sha256(
                    row.get("run_fingerprint"),
                    label="AI_SLICE run_fingerprint",
                )
                artifact_sha256 = _sha256(
                    row.get("artifact_sha256"),
                    label="AI_SLICE artifact SHA-256",
                )
                byte_size = row.get("artifact_byte_size")
                if isinstance(byte_size, bool) or not isinstance(byte_size, int) or byte_size <= 0:
                    raise OutcomeRegistryDriftError("AI_SLICE artifact byte size is invalid")
                if row.get("artifact_type") != "DISCOVERY_EXPOSURE_RESULT":
                    raise OutcomeRegistryDriftError("AI_SLICE artifact type drift")
                path = _path_from_file_uri(
                    row.get("artifact_uri"),
                    label="AI_SLICE artifact",
                )
                held = _open_held_immutable_file(path, data_root=data_root)
                try:
                    if (
                        held.sha256 != artifact_sha256
                        or held.byte_size != byte_size
                        or held.path.name != f"sha256={artifact_sha256}.json"
                    ):
                        raise OutcomeRegistryDriftError("AI_SLICE artifact content drift")
                    document = _canonical_json_document(
                        held.content,
                        label="AI_SLICE artifact",
                    )
                    source_dates, occurrence_count = _validate_source_artifact_document(
                        document,
                        run_fingerprint=fingerprint,
                        slice_index=raw_slice_index,
                        profile=profile,
                    )
                    _verify_held_file(held)
                finally:
                    held.close()
                descriptors.append(
                    P5SourceArtifactDescriptor(
                        slice_index=raw_slice_index,
                        discovery_exposure_id=_positive_identifier(
                            row.get("discovery_exposure_id"),
                            label="discovery_exposure_id",
                        ),
                        research_run_spec_id=_positive_identifier(
                            row.get("research_run_spec_id"),
                            label="research_run_spec_id",
                        ),
                        result_artifact_id=_positive_identifier(
                            row.get("result_artifact_id"),
                            label="result_artifact_id",
                        ),
                        run_fingerprint=fingerprint,
                        path=held.path,
                        sha256=artifact_sha256,
                        byte_size=byte_size,
                        requested_source_dates=source_dates,
                        occurrence_count=occurrence_count,
                    )
                )

            descriptors.sort(key=lambda item: item.slice_index)
            if tuple(item.slice_index for item in descriptors) != tuple(
                range(profile.source_slice_count)
            ):
                raise OutcomeRegistryDriftError("AI_SLICE indexes are incomplete or duplicated")
            occurrence_count = sum(item.occurrence_count for item in descriptors)
            if occurrence_count != profile.source_occurrence_count:
                raise OutcomeRegistryDriftError(
                    f"canonical {profile.query_id} occurrence count differs from the frozen "
                    f"{profile.source_occurrence_count:,}"
                )
            manifest_sha256 = _canonical_sha256([item.manifest_payload for item in descriptors])
    return P5SourceArtifactSet(
        artifacts=tuple(descriptors),
        source_artifact_manifest_sha256=manifest_sha256,
        occurrence_count=occurrence_count,
    )


load_phase1a_outcome_source_artifacts = load_phase1a_p5_source_artifacts


def _predecessor_gate_from_row(row: Mapping[str, Any]) -> OutcomePredecessorGate:
    if row.get("passed") is not True:
        raise OutcomeRegistryStateError("p5 predecessor equivalence audit has not passed")
    return OutcomePredecessorGate(
        equivalence_audit_id=_positive_identifier(
            row.get("outcome_equivalence_audit_id"),
            label="outcome_equivalence_audit_id",
        ),
        equivalence_audit_artifact_sha256=_sha256(
            row.get("audit_artifact_sha256"),
            label="audit_artifact_sha256",
        ),
        predecessor_outcome_replay_manifest_id=_positive_identifier(
            row.get("predecessor_outcome_replay_manifest_id"),
            label="predecessor_outcome_replay_manifest_id",
        ),
        predecessor_run_fingerprint=_sha256(
            row.get("predecessor_run_fingerprint"),
            label="predecessor_run_fingerprint",
        ),
        predecessor_result_artifact_sha256=_sha256(
            row.get("predecessor_result_artifact_sha256"),
            label="predecessor_result_artifact_sha256",
        ),
        predecessor_input_lineage_sha256=_sha256(
            row.get("input_lineage_sha256"),
            label="input_lineage_sha256",
        ),
        predecessor_cell_summaries_sha256=_sha256(
            row.get("cell_summaries_sha256"),
            label="cell_summaries_sha256",
        ),
        predecessor_detail_shard_manifest_sha256=_sha256(
            row.get("detail_shard_manifest_sha256"),
            label="detail_shard_manifest_sha256",
        ),
        predecessor_final_checkpoint_sha256=_sha256(
            row.get("final_checkpoint_sha256"),
            label="final_checkpoint_sha256",
        ),
    )


def _load_p1_predecessor_gate(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    equivalence_audit_id: int | None = None,
) -> OutcomePredecessorGate:
    if equivalence_audit_id is None:
        selected_audit_id = None
    else:
        selected_audit_id = _positive_identifier(
            equivalence_audit_id,
            label="equivalence_audit_id",
        )
    rows = connection.execute(
        """
        SELECT audit.*, artifact.sha256 AS audit_artifact_sha256
        FROM systematic_fx.phase1a_outcome_replay_equivalence_audits AS audit
        JOIN systematic_fx.artifacts AS artifact
          ON artifact.artifact_id = audit.audit_artifact_id
        WHERE audit.passed = true
          AND (%s::bigint IS NULL OR audit.outcome_equivalence_audit_id = %s)
        ORDER BY audit.outcome_equivalence_audit_id
        """,
        (selected_audit_id, selected_audit_id),
    ).fetchall()
    if not rows:
        raise OutcomeRegistryStateError(
            "p1_05 reservation requires a PASSED p5 checkpoint/resume equivalence audit"
        )
    if len(rows) != 1:
        raise OutcomeRegistryStateError(
            "p1_05 predecessor audit is ambiguous; select one equivalence_audit_id"
        )
    return _predecessor_gate_from_row(rows[0])


@_translate_psycopg_errors("Phase 1A p1 predecessor gate loading")
def load_phase1a_p1_predecessor_gate(
    database_url: str,
    *,
    data_root: Path | str,
    equivalence_audit_id: int | None = None,
) -> OutcomePredecessorGate:
    """Read one passed gate and re-verify its registered immutable bytes."""

    target = _database_url(database_url)
    with psycopg.connect(target, row_factory=dict_row) as connection:
        _set_serializable_read_only(connection)
        with connection.transaction():
            if equivalence_audit_id is None:
                selected_audit_id = None
            else:
                selected_audit_id = _positive_identifier(
                    equivalence_audit_id,
                    label="equivalence_audit_id",
                )
            rows = connection.execute(
                """
                SELECT outcome_equivalence_audit_id,
                       validation_research_run_attempt_id
                FROM systematic_fx.phase1a_outcome_replay_equivalence_audits
                WHERE passed = true
                  AND (%s::bigint IS NULL OR outcome_equivalence_audit_id = %s)
                ORDER BY outcome_equivalence_audit_id
                """,
                (selected_audit_id, selected_audit_id),
            ).fetchall()
    if not rows:
        raise OutcomeRegistryStateError(
            "p1_05 reservation requires a PASSED p5 checkpoint/resume equivalence audit"
        )
    if len(rows) != 1:
        raise OutcomeRegistryStateError(
            "p1_05 predecessor audit is ambiguous; select one equivalence_audit_id"
        )
    loaded = load_phase1a_p5_equivalence_audit_for_attempt(
        target,
        validation_research_run_attempt_id=_positive_identifier(
            rows[0].get("validation_research_run_attempt_id"),
            label="validation_research_run_attempt_id",
        ),
        data_root=data_root,
    )
    if (
        selected_audit_id is not None
        and loaded.audit.outcome_equivalence_audit_id != selected_audit_id
    ):
        raise OutcomeRegistryDriftError("selected predecessor audit identity drift")
    return loaded.predecessor_gate


verify_phase1a_p1_predecessor_gate = load_phase1a_p1_predecessor_gate


def _validate_governed_run_spec(
    row: Mapping[str, Any],
    *,
    run_fingerprint: str,
    source_artifact_manifest_sha256: str,
    profile: OutcomeQueryProfile = P5_OUTCOME_QUERY_PROFILE,
    predecessor_gate: OutcomePredecessorGate | None = None,
) -> tuple[int, int]:
    expected_parameters = phase1a_outcome_parameters(
        source_artifact_manifest_sha256,
        query_id=profile.query_id,
        predecessor_gate=predecessor_gate,
    )
    canonical_spec = row.get("canonical_spec")
    if not isinstance(canonical_spec, dict):
        raise OutcomeRegistryDriftError("outcome RunSpec canonical_spec is invalid")
    if _canonical_sha256(canonical_spec) != run_fingerprint:
        raise OutcomeRegistryDriftError("outcome RunSpec canonical fingerprint drift")
    parameters = canonical_spec.get("parameters")
    if not isinstance(parameters, dict) or any(
        parameters.get(key) != value for key, value in expected_parameters.items()
    ):
        raise OutcomeRegistryDriftError("outcome RunSpec parameter drift")
    expected_fields = {
        "campaign_key": CAMPAIGN_KEY,
        "canonicalization_schema": RUN_SPEC_SCHEMA,
        "canonicalization_version": RUN_SPEC_SCHEMA_VERSION,
        "direction": "BOTH",
        "engine_version": OUTCOME_ENGINE_VERSION,
        "experiment_id": None,
        "run_fingerprint": run_fingerprint,
        "run_kind": "OUTCOME_BUILD",
    }
    mismatches = [
        field for field, expected in expected_fields.items() if row.get(field) != expected
    ]
    if mismatches:
        raise OutcomeRegistryDriftError(
            "outcome RunSpec ownership drift in fields: " + ", ".join(sorted(mismatches))
        )
    return (
        _positive_identifier(row.get("research_run_spec_id"), label="research_run_spec_id"),
        _positive_identifier(row.get("campaign_id"), label="campaign_id"),
    )


def _load_run_spec_for_update(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    run_fingerprint: str,
) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT run_spec.research_run_spec_id, run_spec.run_fingerprint,
               run_spec.canonicalization_schema, run_spec.canonicalization_version,
               run_spec.campaign_id, campaign.campaign_key,
               run_spec.experiment_id, run_spec.run_kind, run_spec.engine_version,
               run_spec.direction, run_spec.canonical_spec
        FROM systematic_fx.research_run_specs AS run_spec
        JOIN systematic_fx.campaigns AS campaign
          ON campaign.campaign_id = run_spec.campaign_id
        WHERE run_spec.run_fingerprint = %s
        FOR UPDATE OF run_spec
        """,
        (run_fingerprint,),
    ).fetchone()
    if row is None:
        raise OutcomeRegistryError(f"outcome RunSpec {run_fingerprint} does not exist")
    return row


def _manifest_state(row: Mapping[str, Any]) -> OutcomeReplayState:
    return OutcomeReplayState(
        outcome_replay_manifest_id=int(row["outcome_replay_manifest_id"]),
        research_run_spec_id=int(row["research_run_spec_id"]),
        research_run_attempt_id=int(row["research_run_attempt_id"]),
        attempt_number=int(row["attempt_number"]),
        run_fingerprint=str(row["run_fingerprint"]),
        status=str(row["status"]),
        source_artifact_manifest_sha256=str(row["source_artifact_manifest_sha256"]),
        result_artifact_id=(
            None if row.get("result_artifact_id") is None else int(row["result_artifact_id"])
        ),
        result_artifact_sha256=row.get("result_artifact_sha256"),
        cell_summaries_sha256=row.get("cell_summaries_sha256"),
    )


def _assert_manifest_identity(
    row: Mapping[str, Any],
    *,
    run_fingerprint: str,
    source_artifact_manifest_sha256: str,
    profile: OutcomeQueryProfile = P5_OUTCOME_QUERY_PROFILE,
) -> None:
    expected = {
        "barrier_axis_size": len(BARRIER_TICKS),
        "cell_count_per_surface": EXPECTED_CELL_COUNT,
        "direction_count": len(DIRECTION_IDS),
        "expected_detail_record_count": profile.detail_record_count,
        "expected_summary_count": EXPECTED_SUMMARY_COUNT,
        "final_source_date": profile.final_source_date,
        "pattern_key": profile.query_id,
        "planned_source_date_count": profile.planned_source_date_count,
        "run_fingerprint": run_fingerprint,
        "scenario_count": len(SCENARIO_IDS),
        "source_artifact_manifest_sha256": source_artifact_manifest_sha256,
        "source_occurrence_count": profile.source_occurrence_count,
        "source_slice_count": profile.source_slice_count,
    }
    mismatches = [key for key, value in expected.items() if row.get(key) != value]
    if mismatches:
        raise OutcomeRegistryDriftError(
            "outcome replay manifest drift in fields: " + ", ".join(sorted(mismatches))
        )


@_translate_psycopg_errors("Phase 1A outcome replay reservation")
def reserve_phase1a_outcome_replay(
    database_url: str,
    *,
    run_fingerprint: str,
    source_artifact_manifest_sha256: str,
    query_id: str = P5_QUERY_ID,
    predecessor_equivalence_audit_id: int | None = None,
    data_root: Path | str | None = None,
) -> OutcomeReplayReservation:
    """Reserve, exactly resume, or duplicate-skip one ordered outcome run."""

    target = _database_url(database_url)
    profile = outcome_query_profile(query_id)
    fingerprint = _sha256(run_fingerprint, label="run_fingerprint")
    source_sha256 = _sha256(
        source_artifact_manifest_sha256,
        label="source_artifact_manifest_sha256",
    )
    with (
        psycopg.connect(target, row_factory=dict_row) as connection,
        ExitStack() as held_files,
    ):
        _set_serializable(connection)
        with connection.transaction():
            audit_evidence: _HeldFile | None = None
            spec = _load_run_spec_for_update(connection, run_fingerprint=fingerprint)
            predecessor_gate = None
            if profile.query_id == P1_05_QUERY_ID:
                canonical_spec = spec.get("canonical_spec")
                parameters = (
                    canonical_spec.get("parameters")
                    if isinstance(canonical_spec, Mapping)
                    else None
                )
                if not isinstance(parameters, Mapping):
                    raise OutcomeRegistryDriftError("outcome RunSpec parameters are invalid")
                recorded_audit_id = parameters.get("predecessor_equivalence_audit_id")
                if predecessor_equivalence_audit_id is not None:
                    requested_audit_id = _positive_identifier(
                        predecessor_equivalence_audit_id,
                        label="predecessor_equivalence_audit_id",
                    )
                    if recorded_audit_id != requested_audit_id:
                        raise OutcomeRegistryDriftError(
                            "p1_05 RunSpec predecessor audit identity drift"
                        )
                if isinstance(recorded_audit_id, bool) or not isinstance(recorded_audit_id, int):
                    raise OutcomeRegistryDriftError(
                        "p1_05 RunSpec must bind predecessor_equivalence_audit_id"
                    )
                if data_root is None:
                    raise OutcomeRegistryError(
                        "p1_05 reservation requires data_root for audit byte verification"
                    )
                predecessor_gate, audit_evidence = _hold_phase1a_p1_predecessor_evidence(
                    target,
                    connection,
                    equivalence_audit_id=recorded_audit_id,
                    data_root=data_root,
                )
                held_files.callback(audit_evidence.close)
            elif predecessor_equivalence_audit_id is not None:
                raise OutcomeRegistryError("p5 reservation cannot select a predecessor audit")
            research_run_spec_id, campaign_id = _validate_governed_run_spec(
                spec,
                run_fingerprint=fingerprint,
                source_artifact_manifest_sha256=source_sha256,
                profile=profile,
                predecessor_gate=predecessor_gate,
            )
            attempts = connection.execute(
                """
                SELECT attempt.research_run_attempt_id, attempt.attempt_number,
                       attempt.status, attempt.reused_attempt_id,
                       manifest.outcome_replay_manifest_id, manifest.run_fingerprint,
                       manifest.source_artifact_manifest_sha256,
                       manifest.source_slice_count, manifest.source_occurrence_count,
                       manifest.scenario_count, manifest.direction_count,
                       manifest.barrier_axis_size, manifest.cell_count_per_surface,
                       manifest.expected_summary_count,
                       manifest.expected_detail_record_count,
                       manifest.planned_source_date_count,
                       manifest.final_source_date, manifest.pattern_key,
                       manifest.status AS manifest_status,
                       manifest.result_artifact_id,
                       manifest.result_artifact_sha256,
                       manifest.cell_summaries_sha256
                FROM systematic_fx.research_run_attempts AS attempt
                LEFT JOIN systematic_fx.phase1a_outcome_replay_manifests AS manifest
                  ON manifest.research_run_attempt_id = attempt.research_run_attempt_id
                WHERE attempt.research_run_spec_id = %s
                ORDER BY attempt.attempt_number
                FOR UPDATE OF attempt
                """,
                (research_run_spec_id,),
            ).fetchall()
            successful = [row for row in attempts if row["status"] == "SUCCEEDED"]
            active = [row for row in attempts if row["status"] in {"QUEUED", "RUNNING"}]
            if len(successful) > 1 or len(active) > 1:
                raise OutcomeRegistryDriftError("outcome run attempt cardinality drift")
            if successful:
                success = successful[0]
                _assert_manifest_identity(
                    success,
                    run_fingerprint=fingerprint,
                    source_artifact_manifest_sha256=source_sha256,
                    profile=profile,
                )
                if success.get("manifest_status") != "SUCCEEDED":
                    raise OutcomeRegistryDriftError("successful outcome manifest state drift")
                next_attempt = max(int(row["attempt_number"]) for row in attempts) + 1
                if next_attempt > _MAX_ATTEMPT_NUMBER:
                    raise OutcomeRegistryStateError("outcome attempt number exhausted")
                skipped = connection.execute(
                    """
                    INSERT INTO systematic_fx.research_run_attempts
                        (research_run_spec_id, attempt_number, status,
                         reused_attempt_id, result_summary, finished_at)
                    VALUES (%s, %s, 'SKIPPED_DUPLICATE', %s, %s,
                            statement_timestamp())
                    RETURNING research_run_attempt_id
                    """,
                    (
                        research_run_spec_id,
                        next_attempt,
                        int(success["research_run_attempt_id"]),
                        Jsonb(
                            {
                                "reason": "EXACT_SUCCESS_EXISTS",
                                "run_fingerprint": fingerprint,
                            }
                        ),
                    ),
                ).fetchone()
                if skipped is None:  # pragma: no cover - RETURNING is mandatory
                    raise OutcomeRegistryDatabaseError("duplicate skip returned no identity")
                if audit_evidence is not None:
                    _verify_held_file(audit_evidence)
                return OutcomeReplayReservation(
                    outcome_replay_manifest_id=int(success["outcome_replay_manifest_id"]),
                    research_run_spec_id=research_run_spec_id,
                    research_run_attempt_id=int(skipped["research_run_attempt_id"]),
                    attempt_number=next_attempt,
                    attempt_status="SKIPPED_DUPLICATE",
                    replay_status="SUCCEEDED",
                    execute=False,
                    reused_attempt_id=int(success["research_run_attempt_id"]),
                    created_manifest=False,
                )
            if active:
                current = active[0]
                if current.get("outcome_replay_manifest_id") is None:
                    raise OutcomeRegistryDriftError(
                        "active governed outcome attempt has no replay manifest"
                    )
                _assert_manifest_identity(
                    current,
                    run_fingerprint=fingerprint,
                    source_artifact_manifest_sha256=source_sha256,
                    profile=profile,
                )
                if current.get("manifest_status") != current.get("status"):
                    raise OutcomeRegistryDriftError("active outcome state drift")
                if audit_evidence is not None:
                    _verify_held_file(audit_evidence)
                return OutcomeReplayReservation(
                    outcome_replay_manifest_id=int(current["outcome_replay_manifest_id"]),
                    research_run_spec_id=research_run_spec_id,
                    research_run_attempt_id=int(current["research_run_attempt_id"]),
                    attempt_number=int(current["attempt_number"]),
                    attempt_status=str(current["status"]),
                    replay_status=str(current["manifest_status"]),
                    execute=True,
                    reused_attempt_id=None,
                    created_manifest=False,
                )

            next_attempt = max((int(row["attempt_number"]) for row in attempts), default=0) + 1
            if next_attempt > _MAX_ATTEMPT_NUMBER:
                raise OutcomeRegistryStateError("outcome attempt number exhausted")
            attempt = connection.execute(
                """
                INSERT INTO systematic_fx.research_run_attempts
                    (research_run_spec_id, attempt_number, status, result_summary)
                VALUES (%s, %s, 'QUEUED', '{}'::jsonb)
                RETURNING research_run_attempt_id
                """,
                (research_run_spec_id, next_attempt),
            ).fetchone()
            if attempt is None:  # pragma: no cover - RETURNING is mandatory
                raise OutcomeRegistryDatabaseError("outcome attempt returned no identity")
            research_run_attempt_id = int(attempt["research_run_attempt_id"])
            manifest = connection.execute(
                """
                INSERT INTO systematic_fx.phase1a_outcome_replay_manifests
                    (research_run_spec_id, research_run_attempt_id, campaign_id,
                     run_fingerprint, pattern_key, source_artifact_manifest_sha256,
                     source_slice_count, source_occurrence_count, scenario_count,
                     direction_count, barrier_axis_size, cell_count_per_surface,
                     expected_summary_count, expected_detail_record_count,
                     planned_source_date_count, final_source_date, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        'QUEUED')
                RETURNING outcome_replay_manifest_id
                """,
                (
                    research_run_spec_id,
                    research_run_attempt_id,
                    campaign_id,
                    fingerprint,
                    profile.query_id,
                    source_sha256,
                    profile.source_slice_count,
                    profile.source_occurrence_count,
                    len(SCENARIO_IDS),
                    len(DIRECTION_IDS),
                    len(BARRIER_TICKS),
                    EXPECTED_CELL_COUNT,
                    EXPECTED_SUMMARY_COUNT,
                    profile.detail_record_count,
                    profile.planned_source_date_count,
                    profile.final_source_date,
                ),
            ).fetchone()
            if manifest is None:  # pragma: no cover - RETURNING is mandatory
                raise OutcomeRegistryDatabaseError("outcome manifest returned no identity")
            if audit_evidence is not None:
                _verify_held_file(audit_evidence)
            return OutcomeReplayReservation(
                outcome_replay_manifest_id=int(manifest["outcome_replay_manifest_id"]),
                research_run_spec_id=research_run_spec_id,
                research_run_attempt_id=research_run_attempt_id,
                attempt_number=next_attempt,
                attempt_status="QUEUED",
                replay_status="QUEUED",
                execute=True,
                reused_attempt_id=None,
                created_manifest=True,
            )


def _load_manifest_for_update(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    outcome_replay_manifest_id: int,
) -> dict[str, Any]:
    row = connection.execute(
        """
          SELECT manifest.*, attempt.attempt_number,
                 attempt.status AS attempt_status,
                 attempt.result_artifact_id AS attempt_result_artifact_id,
                 attempt.started_at AS attempt_started_at,
                 attempt.finished_at AS attempt_finished_at,
                 attempt.error_message AS attempt_error_message,
                 run_spec.canonical_spec AS run_spec_canonical_spec
          FROM systematic_fx.phase1a_outcome_replay_manifests AS manifest
          JOIN systematic_fx.research_run_attempts AS attempt
            ON attempt.research_run_attempt_id = manifest.research_run_attempt_id
           AND attempt.research_run_spec_id = manifest.research_run_spec_id
          JOIN systematic_fx.research_run_specs AS run_spec
            ON run_spec.research_run_spec_id = manifest.research_run_spec_id
           AND run_spec.campaign_id = manifest.campaign_id
           AND run_spec.run_fingerprint = manifest.run_fingerprint
        WHERE manifest.outcome_replay_manifest_id = %s
        FOR UPDATE OF manifest, attempt
        """,
        (outcome_replay_manifest_id,),
    ).fetchone()
    if row is None:
        raise OutcomeRegistryError(
            f"outcome replay manifest {outcome_replay_manifest_id} does not exist"
        )
    return row


def _assert_live_manifest(
    row: Mapping[str, Any],
    *,
    run_fingerprint: str,
) -> None:
    profile = outcome_query_profile(str(row.get("pattern_key", "")))
    source_sha = _sha256(
        row.get("source_artifact_manifest_sha256"),
        label="source_artifact_manifest_sha256",
    )
    _assert_manifest_identity(
        row,
        run_fingerprint=run_fingerprint,
        source_artifact_manifest_sha256=source_sha,
        profile=profile,
    )
    if row.get("attempt_status") != row.get("status"):
        raise OutcomeRegistryDriftError("outcome replay and attempt status drift")


@_translate_psycopg_errors("Phase 1A outcome replay start")
def start_phase1a_outcome_replay(
    database_url: str,
    *,
    outcome_replay_manifest_id: int,
    run_fingerprint: str,
    data_root: Path | str | None = None,
) -> OutcomeReplayState:
    """Atomically transition the generic attempt and replay manifest to RUNNING."""

    target = _database_url(database_url)
    manifest_id = _positive_identifier(
        outcome_replay_manifest_id,
        label="outcome_replay_manifest_id",
    )
    fingerprint = _sha256(run_fingerprint, label="run_fingerprint")
    with (
        psycopg.connect(target, row_factory=dict_row) as connection,
        ExitStack() as held_files,
    ):
        _set_serializable(connection)
        with connection.transaction():
            audit_evidence: _HeldFile | None = None
            row = _load_manifest_for_update(
                connection,
                outcome_replay_manifest_id=manifest_id,
            )
            _assert_live_manifest(row, run_fingerprint=fingerprint)
            if row["pattern_key"] == P1_05_QUERY_ID:
                if data_root is None:
                    raise OutcomeRegistryError(
                        "p1_05 start requires data_root for audit byte verification"
                    )
                run_spec = row.get("run_spec_canonical_spec")
                parameters = run_spec.get("parameters") if isinstance(run_spec, Mapping) else None
                if not isinstance(parameters, Mapping):
                    raise OutcomeRegistryDriftError(
                        "p1_05 RunSpec predecessor parameters are missing"
                    )
                _, audit_evidence = _hold_phase1a_p1_predecessor_evidence(
                    target,
                    connection,
                    equivalence_audit_id=_positive_identifier(
                        parameters.get("predecessor_equivalence_audit_id"),
                        label="predecessor_equivalence_audit_id",
                    ),
                    data_root=data_root,
                )
                held_files.callback(audit_evidence.close)
            if row["status"] == "RUNNING":
                if audit_evidence is not None:
                    _verify_held_file(audit_evidence)
                return _manifest_state(row)
            if row["status"] != "QUEUED":
                raise OutcomeRegistryStateError(f"cannot start outcome replay from {row['status']}")
            started_at = datetime.now(UTC)
            connection.execute(
                """
                UPDATE systematic_fx.research_run_attempts
                SET status = 'RUNNING', started_at = %s
                WHERE research_run_attempt_id = %s AND status = 'QUEUED'
                """,
                (started_at, int(row["research_run_attempt_id"])),
            )
            updated = connection.execute(
                """
                UPDATE systematic_fx.phase1a_outcome_replay_manifests
                SET status = 'RUNNING', started_at = %s
                WHERE outcome_replay_manifest_id = %s AND status = 'QUEUED'
                RETURNING *, %s::integer AS attempt_number
                """,
                (started_at, manifest_id, int(row["attempt_number"])),
            ).fetchone()
            if updated is None:
                raise OutcomeRegistryStateError("queued outcome replay changed concurrently")
            if audit_evidence is not None:
                _verify_held_file(audit_evidence)
            return _manifest_state(updated)


def _validate_checkpoint_artifact(
    held: _HeldFile,
    *,
    outcome_replay_manifest_id: int,
    run_fingerprint: str,
    checkpoint_sequence: int,
    completed_source_date_count: int,
    last_completed_source_date: date,
    source_event_count: int,
    predecessor_checkpoint_sha256: str | None,
    progress_metadata_sha256: str,
    profile: OutcomeQueryProfile = P5_OUTCOME_QUERY_PROFILE,
) -> dict[str, Any]:
    if held.path.name != f"sha256={held.sha256}.json":
        raise OutcomeRegistryError("checkpoint artifact filename must be sha256=<content>.json")
    document = _canonical_json_document(held.content, label="outcome checkpoint artifact")
    expected = {
        "artifact_schema": CHECKPOINT_ARTIFACT_SCHEMA,
        "checkpoint_sequence": checkpoint_sequence,
        "completed_source_date_count": completed_source_date_count,
        "last_completed_source_date": last_completed_source_date.isoformat(),
        "outcome_config_id": profile.outcome_config_id,
        "outcome_replay_manifest_id": outcome_replay_manifest_id,
        "predecessor_checkpoint_sha256": predecessor_checkpoint_sha256,
        "progress_metadata_sha256": progress_metadata_sha256,
        "query_id": profile.query_id,
        "run_fingerprint": run_fingerprint,
        "source_event_count": source_event_count,
    }
    mismatches = [key for key, value in expected.items() if document.get(key) != value]
    if mismatches:
        raise OutcomeRegistryDriftError(
            "outcome checkpoint artifact drift in fields: " + ", ".join(sorted(mismatches))
        )
    return document


def _checkpoint_artifact_key(
    *,
    outcome_replay_manifest_id: int,
    run_fingerprint: str,
    checkpoint_sequence: int,
) -> str:
    return (
        f"{CAMPAIGN_KEY}:outcome-checkpoint:{run_fingerprint}:"
        f"manifest-{outcome_replay_manifest_id}:"
        f"{checkpoint_sequence:06d}"
    )


def _checkpoint_artifact_metadata(
    *,
    outcome_replay_manifest_id: int,
    run_fingerprint: str,
    checkpoint_sequence: int,
    last_completed_source_date: date,
    profile: OutcomeQueryProfile = P5_OUTCOME_QUERY_PROFILE,
) -> dict[str, object]:
    return {
        "campaign_key": CAMPAIGN_KEY,
        "checkpoint_sequence": checkpoint_sequence,
        "last_completed_source_date": last_completed_source_date.isoformat(),
        "outcome_config_id": profile.outcome_config_id,
        "outcome_replay_manifest_id": outcome_replay_manifest_id,
        "query_id": profile.query_id,
        "run_fingerprint": run_fingerprint,
    }


def _ensure_checkpoint_artifact(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    outcome_replay_manifest_id: int,
    run_fingerprint: str,
    checkpoint_sequence: int,
    last_completed_source_date: date,
    held: _HeldFile,
    profile: OutcomeQueryProfile = P5_OUTCOME_QUERY_PROFILE,
) -> tuple[int, bool]:
    artifact_key = _checkpoint_artifact_key(
        outcome_replay_manifest_id=outcome_replay_manifest_id,
        run_fingerprint=run_fingerprint,
        checkpoint_sequence=checkpoint_sequence,
    )
    artifact_uri = held.path.as_uri()
    metadata = _checkpoint_artifact_metadata(
        outcome_replay_manifest_id=outcome_replay_manifest_id,
        run_fingerprint=run_fingerprint,
        checkpoint_sequence=checkpoint_sequence,
        last_completed_source_date=last_completed_source_date,
        profile=profile,
    )
    rows = connection.execute(
        """
        SELECT artifact_id, artifact_key, artifact_type, uri, sha256, byte_size,
               media_type, producer_job_id, metadata
        FROM systematic_fx.artifacts
        WHERE artifact_key = %s OR uri = %s
        FOR UPDATE
        """,
        (artifact_key, artifact_uri),
    ).fetchall()
    if not rows:
        row = connection.execute(
            """
            INSERT INTO systematic_fx.artifacts
                (artifact_key, artifact_type, uri, sha256, byte_size,
                 media_type, metadata)
            VALUES (%s, %s, %s, %s, %s, 'application/json', %s)
            RETURNING artifact_id
            """,
            (
                artifact_key,
                CHECKPOINT_ARTIFACT_TYPE,
                artifact_uri,
                held.sha256,
                held.byte_size,
                Jsonb(metadata),
            ),
        ).fetchone()
        if row is None:  # pragma: no cover - RETURNING is mandatory
            raise OutcomeRegistryDatabaseError("checkpoint artifact returned no identity")
        return int(row["artifact_id"]), True
    if len(rows) != 1:
        raise OutcomeRegistryDriftError("checkpoint artifact key and URI differ")
    row = rows[0]
    expected = {
        "artifact_key": artifact_key,
        "artifact_type": CHECKPOINT_ARTIFACT_TYPE,
        "byte_size": held.byte_size,
        "media_type": "application/json",
        "metadata": metadata,
        "producer_job_id": None,
        "sha256": held.sha256,
        "uri": artifact_uri,
    }
    mismatches = [key for key, value in expected.items() if row.get(key) != value]
    if mismatches:
        raise OutcomeRegistryDriftError(
            "checkpoint artifact immutable drift in fields: " + ", ".join(sorted(mismatches))
        )
    return int(row["artifact_id"]), False


@_translate_psycopg_errors("Phase 1A outcome checkpoint registration")
def register_phase1a_outcome_checkpoint(
    database_url: str,
    *,
    outcome_replay_manifest_id: int,
    run_fingerprint: str,
    checkpoint_sequence: int,
    completed_source_date_count: int,
    last_completed_source_date: date,
    source_event_count: int,
    predecessor_checkpoint_sha256: str | None,
    progress_metadata: Mapping[str, object],
    checkpoint_artifact_path: Path,
    data_root: Path | str,
    query_id: str = P5_QUERY_ID,
) -> OutcomeCheckpointReport:
    """Append one immutable SOURCE_DATE_COMPLETE artifact to the hash chain."""

    target = _database_url(database_url)
    profile = outcome_query_profile(query_id)
    manifest_id = _positive_identifier(
        outcome_replay_manifest_id,
        label="outcome_replay_manifest_id",
    )
    fingerprint = _sha256(run_fingerprint, label="run_fingerprint")
    sequence = _positive_identifier(checkpoint_sequence, label="checkpoint_sequence")
    source_date_count = _positive_identifier(
        completed_source_date_count,
        label="completed_source_date_count",
    )
    if source_date_count != sequence:
        raise OutcomeRegistryError("SOURCE_DATE_COMPLETE count must equal checkpoint_sequence")
    if not isinstance(last_completed_source_date, date) or isinstance(
        last_completed_source_date, datetime
    ):
        raise OutcomeRegistryError("last_completed_source_date must be a date")
    event_count = _nonnegative_integer(source_event_count, label="source_event_count")
    if sequence == 1:
        if predecessor_checkpoint_sha256 is not None:
            raise OutcomeRegistryError("checkpoint 1 cannot have a predecessor")
        predecessor_sha256 = None
    else:
        predecessor_sha256 = _sha256(
            predecessor_checkpoint_sha256,
            label="predecessor_checkpoint_sha256",
        )
    metadata = _canonical_mapping(progress_metadata, label="progress_metadata")
    metadata_sha256 = _canonical_sha256(metadata)
    held = _open_held_immutable_file(Path(checkpoint_artifact_path), data_root=data_root)
    try:
        _require_held_parent(
            held,
            data_root=data_root,
            expected_parent=profile.checkpoint_directory,
            label="outcome checkpoint artifact",
        )
        _validate_checkpoint_artifact(
            held,
            outcome_replay_manifest_id=manifest_id,
            run_fingerprint=fingerprint,
            checkpoint_sequence=sequence,
            completed_source_date_count=source_date_count,
            last_completed_source_date=last_completed_source_date,
            source_event_count=event_count,
            predecessor_checkpoint_sha256=predecessor_sha256,
            progress_metadata_sha256=metadata_sha256,
            profile=profile,
        )
        with (
            psycopg.connect(target, row_factory=dict_row) as connection,
            ExitStack() as held_files,
        ):
            _set_serializable(connection)
            with connection.transaction():
                audit_evidence: _HeldFile | None = None
                manifest = _load_manifest_for_update(
                    connection,
                    outcome_replay_manifest_id=manifest_id,
                )
                _assert_live_manifest(manifest, run_fingerprint=fingerprint)
                if manifest["pattern_key"] != profile.query_id:
                    raise OutcomeRegistryDriftError("checkpoint query identity drift")
                if profile.query_id == P1_05_QUERY_ID:
                    run_spec = manifest.get("run_spec_canonical_spec")
                    parameters = (
                        run_spec.get("parameters") if isinstance(run_spec, Mapping) else None
                    )
                    if not isinstance(parameters, Mapping):
                        raise OutcomeRegistryDriftError(
                            "p1_05 RunSpec predecessor parameters are missing"
                        )
                    _, audit_evidence = _hold_phase1a_p1_predecessor_evidence(
                        target,
                        connection,
                        equivalence_audit_id=_positive_identifier(
                            parameters.get("predecessor_equivalence_audit_id"),
                            label="predecessor_equivalence_audit_id",
                        ),
                        data_root=data_root,
                    )
                    held_files.callback(audit_evidence.close)
                if manifest["status"] != "RUNNING":
                    raise OutcomeRegistryStateError("checkpoint requires a RUNNING replay")
                current = connection.execute(
                    """
                    SELECT checkpoint.*, artifact.uri AS checkpoint_artifact_uri
                    FROM systematic_fx.phase1a_outcome_replay_checkpoints AS checkpoint
                    JOIN systematic_fx.artifacts AS artifact
                      ON artifact.artifact_id = checkpoint.checkpoint_artifact_id
                    WHERE checkpoint.outcome_replay_manifest_id = %s
                      AND checkpoint.checkpoint_sequence = %s
                    FOR UPDATE OF checkpoint, artifact
                    """,
                    (manifest_id, sequence),
                ).fetchone()
                expected = {
                    "run_fingerprint": fingerprint,
                    "completed_source_date_count": source_date_count,
                    "last_completed_source_date": last_completed_source_date,
                    "source_event_count": event_count,
                    "checkpoint_artifact_sha256": held.sha256,
                    "checkpoint_artifact_byte_size": held.byte_size,
                    "checkpoint_artifact_uri": held.path.as_uri(),
                    "predecessor_checkpoint_sha256": predecessor_sha256,
                    "progress_metadata": metadata,
                }
                if current is not None:
                    mismatches = [
                        key for key, value in expected.items() if current.get(key) != value
                    ]
                    if mismatches:
                        raise OutcomeRegistryDriftError(
                            "checkpoint immutable drift in fields: " + ", ".join(sorted(mismatches))
                        )
                    _verify_held_file(held)
                    if audit_evidence is not None:
                        _verify_held_file(audit_evidence)
                    return OutcomeCheckpointReport(
                        outcome_replay_manifest_id=manifest_id,
                        run_fingerprint=fingerprint,
                        checkpoint_sequence=sequence,
                        completed_source_date_count=source_date_count,
                        last_completed_source_date=last_completed_source_date,
                        source_event_count=event_count,
                        checkpoint_artifact_id=int(current["checkpoint_artifact_id"]),
                        checkpoint_artifact_sha256=held.sha256,
                        checkpoint_artifact_uri=held.path.as_uri(),
                        checkpoint_artifact_byte_size=held.byte_size,
                        predecessor_checkpoint_sha256=predecessor_sha256,
                        created=False,
                    )

                previous = connection.execute(
                    """
                    SELECT checkpoint_sequence, completed_source_date_count,
                           last_completed_source_date, source_event_count,
                           checkpoint_artifact_sha256
                    FROM systematic_fx.phase1a_outcome_replay_checkpoints
                    WHERE outcome_replay_manifest_id = %s
                    ORDER BY checkpoint_sequence DESC
                    LIMIT 1
                    FOR UPDATE
                    """,
                    (manifest_id,),
                ).fetchone()
                if previous is None:
                    if sequence != 1 or predecessor_sha256 is not None:
                        raise OutcomeRegistryStateError(
                            "first checkpoint must be sequence 1 without predecessor"
                        )
                elif (
                    sequence != int(previous["checkpoint_sequence"]) + 1
                    or source_date_count != int(previous["completed_source_date_count"]) + 1
                    or last_completed_source_date <= previous["last_completed_source_date"]
                    or event_count < int(previous["source_event_count"])
                    or predecessor_sha256 != previous["checkpoint_artifact_sha256"]
                ):
                    raise OutcomeRegistryDriftError(
                        "checkpoint does not extend the latest source-date hash chain"
                    )
                artifact_id, _ = _ensure_checkpoint_artifact(
                    connection,
                    outcome_replay_manifest_id=manifest_id,
                    run_fingerprint=fingerprint,
                    checkpoint_sequence=sequence,
                    last_completed_source_date=last_completed_source_date,
                    held=held,
                    profile=profile,
                )
                connection.execute(
                    """
                    INSERT INTO systematic_fx.phase1a_outcome_replay_checkpoints
                        (outcome_replay_manifest_id, checkpoint_sequence,
                         run_fingerprint, completed_source_date_count,
                         last_completed_source_date, source_event_count,
                         checkpoint_artifact_id, checkpoint_artifact_sha256,
                         checkpoint_artifact_byte_size,
                         predecessor_checkpoint_sha256, progress_metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        manifest_id,
                        sequence,
                        fingerprint,
                        source_date_count,
                        last_completed_source_date,
                        event_count,
                        artifact_id,
                        held.sha256,
                        held.byte_size,
                        predecessor_sha256,
                        Jsonb(metadata),
                    ),
                )
                _verify_held_file(held)
                if audit_evidence is not None:
                    _verify_held_file(audit_evidence)
        return OutcomeCheckpointReport(
            outcome_replay_manifest_id=manifest_id,
            run_fingerprint=fingerprint,
            checkpoint_sequence=sequence,
            completed_source_date_count=source_date_count,
            last_completed_source_date=last_completed_source_date,
            source_event_count=event_count,
            checkpoint_artifact_id=artifact_id,
            checkpoint_artifact_sha256=held.sha256,
            checkpoint_artifact_uri=held.path.as_uri(),
            checkpoint_artifact_byte_size=held.byte_size,
            predecessor_checkpoint_sha256=predecessor_sha256,
            created=True,
        )
    finally:
        held.close()


def _validate_checkpoint_chain_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    outcome_replay_manifest_id: int,
    run_fingerprint: str,
    data_root: Path | str,
    profile: OutcomeQueryProfile = P5_OUTCOME_QUERY_PROFILE,
) -> tuple[Mapping[str, Any], dict[str, object], Path] | None:
    """Validate the complete DB hash chain and return its normalized tail."""

    if not rows:
        return None
    _, derived = _resolved_data_root(data_root)
    previous_date: date | None = None
    previous_event_count: int | None = None
    previous_artifact_sha256: str | None = None
    latest_progress_metadata: dict[str, object] | None = None
    latest_artifact_path: Path | None = None
    for expected_sequence, row in enumerate(rows, start=1):
        sequence = _positive_identifier(
            row.get("checkpoint_sequence"),
            label="checkpoint_sequence",
        )
        if sequence != expected_sequence:
            raise OutcomeRegistryDriftError(
                "outcome checkpoint DB chain has a missing or unordered sequence"
            )
        if row.get("run_fingerprint") != run_fingerprint:
            raise OutcomeRegistryDriftError("outcome checkpoint fingerprint drift")
        source_date_count = _positive_identifier(
            row.get("completed_source_date_count"),
            label="completed_source_date_count",
        )
        if source_date_count != sequence:
            raise OutcomeRegistryDriftError(
                "outcome checkpoint source-date count differs from sequence"
            )
        source_date = row.get("last_completed_source_date")
        if not isinstance(source_date, date) or isinstance(source_date, datetime):
            raise OutcomeRegistryDriftError("outcome checkpoint last source date is invalid")
        event_count = _nonnegative_integer(
            row.get("source_event_count"),
            label="source_event_count",
        )
        checkpoint_sha256 = _sha256(
            row.get("checkpoint_artifact_sha256"),
            label="checkpoint_artifact_sha256",
        )
        checkpoint_byte_size = _positive_identifier(
            row.get("checkpoint_artifact_byte_size"),
            label="checkpoint_artifact_byte_size",
        )
        predecessor = row.get("predecessor_checkpoint_sha256")
        if sequence == 1:
            if predecessor is not None:
                raise OutcomeRegistryDriftError("first outcome checkpoint has a predecessor")
        elif (
            _sha256(predecessor, label="predecessor_checkpoint_sha256") != previous_artifact_sha256
        ):
            raise OutcomeRegistryDriftError("outcome checkpoint predecessor hash chain drift")
        if previous_date is not None and source_date <= previous_date:
            raise OutcomeRegistryDriftError(
                "outcome checkpoint source dates are not strictly increasing"
            )
        if previous_event_count is not None and event_count < previous_event_count:
            raise OutcomeRegistryDriftError("outcome checkpoint event count is not monotonic")

        checkpoint_artifact_id = _positive_identifier(
            row.get("checkpoint_artifact_id"),
            label="checkpoint_artifact_id",
        )
        if row.get("stored_artifact_id") != checkpoint_artifact_id:
            raise OutcomeRegistryDriftError("outcome checkpoint artifact identity drift")
        expected_artifact_metadata = _checkpoint_artifact_metadata(
            outcome_replay_manifest_id=outcome_replay_manifest_id,
            run_fingerprint=run_fingerprint,
            checkpoint_sequence=sequence,
            last_completed_source_date=source_date,
            profile=profile,
        )
        expected_artifact = {
            "artifact_key": _checkpoint_artifact_key(
                outcome_replay_manifest_id=outcome_replay_manifest_id,
                run_fingerprint=run_fingerprint,
                checkpoint_sequence=sequence,
            ),
            "artifact_type": CHECKPOINT_ARTIFACT_TYPE,
            "artifact_sha256": checkpoint_sha256,
            "artifact_byte_size": checkpoint_byte_size,
            "media_type": "application/json",
            "producer_job_id": None,
            "artifact_metadata": expected_artifact_metadata,
        }
        mismatches = [key for key, value in expected_artifact.items() if row.get(key) != value]
        if mismatches:
            raise OutcomeRegistryDriftError(
                "outcome checkpoint artifact DB drift in fields: " + ", ".join(sorted(mismatches))
            )
        artifact_uri = row.get("artifact_uri")
        artifact_path = _path_from_file_uri(
            artifact_uri,
            label="outcome checkpoint artifact",
        )
        if (
            not artifact_path.is_absolute()
            or artifact_path.as_uri() != artifact_uri
            or artifact_path.name != f"sha256={checkpoint_sha256}.json"
        ):
            raise OutcomeRegistryDriftError("outcome checkpoint artifact URI is not canonical")
        try:
            artifact_relative = artifact_path.relative_to(derived)
        except ValueError as error:
            raise OutcomeRegistryDriftError(
                "outcome checkpoint artifact URI is outside data/derived"
            ) from error
        if PurePosixPath(artifact_relative.as_posix()).parent != profile.checkpoint_directory:
            raise OutcomeRegistryDriftError(
                "outcome checkpoint artifact URI is outside its query namespace"
            )
        progress_metadata = _canonical_mapping(
            row.get("progress_metadata"),
            label="progress_metadata",
        )

        previous_date = source_date
        previous_event_count = event_count
        previous_artifact_sha256 = checkpoint_sha256
        latest_progress_metadata = progress_metadata
        latest_artifact_path = artifact_path

    if latest_progress_metadata is None or latest_artifact_path is None:  # pragma: no cover
        raise OutcomeRegistryDatabaseError("checkpoint chain tail was not resolved")
    return rows[-1], latest_progress_metadata, latest_artifact_path


@_translate_psycopg_errors("Phase 1A latest outcome checkpoint loading")
def load_latest_phase1a_outcome_checkpoint(
    database_url: str,
    *,
    outcome_replay_manifest_id: int,
    run_fingerprint: str,
    data_root: Path | str,
) -> LoadedOutcomeCheckpoint | None:
    """Load the latest exact replay state from a verified append-only chain.

    The transaction is strictly read-only.  A QUEUED or RUNNING replay without a
    checkpoint returns ``None``; terminal attempts are immutable records rather
    than resumable executions and are rejected.
    """

    target = _database_url(database_url)
    manifest_id = _positive_identifier(
        outcome_replay_manifest_id,
        label="outcome_replay_manifest_id",
    )
    fingerprint = _sha256(run_fingerprint, label="run_fingerprint")
    _resolved_data_root(data_root)
    with psycopg.connect(target, row_factory=dict_row) as connection:
        _set_serializable_read_only(connection)
        with connection.transaction():
            manifest = connection.execute(
                """
                SELECT manifest.*, attempt.attempt_number,
                       attempt.status AS attempt_status,
                       campaign.campaign_key,
                       run_spec.canonicalization_schema,
                       run_spec.canonicalization_version,
                       run_spec.experiment_id, run_spec.run_kind,
                       run_spec.engine_version, run_spec.direction,
                       run_spec.canonical_spec
                FROM systematic_fx.phase1a_outcome_replay_manifests AS manifest
                JOIN systematic_fx.research_run_attempts AS attempt
                  ON attempt.research_run_attempt_id =
                        manifest.research_run_attempt_id
                 AND attempt.research_run_spec_id = manifest.research_run_spec_id
                JOIN systematic_fx.research_run_specs AS run_spec
                  ON run_spec.research_run_spec_id = manifest.research_run_spec_id
                 AND run_spec.campaign_id = manifest.campaign_id
                 AND run_spec.run_fingerprint = manifest.run_fingerprint
                JOIN systematic_fx.campaigns AS campaign
                  ON campaign.campaign_id = manifest.campaign_id
                WHERE manifest.outcome_replay_manifest_id = %s
                """,
                (manifest_id,),
            ).fetchone()
            if manifest is None:
                raise OutcomeRegistryError(f"outcome replay manifest {manifest_id} does not exist")
            _assert_live_manifest(manifest, run_fingerprint=fingerprint)
            source_sha256 = _sha256(
                manifest.get("source_artifact_manifest_sha256"),
                label="source_artifact_manifest_sha256",
            )
            research_run_spec_id, _ = _validate_governed_run_spec(
                manifest,
                run_fingerprint=fingerprint,
                source_artifact_manifest_sha256=source_sha256,
                profile=outcome_query_profile(str(manifest["pattern_key"])),
                predecessor_gate=(
                    _load_p1_predecessor_gate(
                        connection,
                        equivalence_audit_id=int(
                            manifest["canonical_spec"]["parameters"][
                                "predecessor_equivalence_audit_id"
                            ]
                        ),
                    )
                    if manifest["pattern_key"] == P1_05_QUERY_ID
                    else None
                ),
            )
            manifest_status = str(manifest["status"])
            if manifest_status not in {"QUEUED", "RUNNING"}:
                raise OutcomeRegistryStateError(
                    f"cannot resume outcome replay from {manifest_status}"
                )

            rows = connection.execute(
                """
                SELECT checkpoint.*,
                       artifact.artifact_id AS stored_artifact_id,
                       artifact.artifact_key,
                       artifact.artifact_type,
                       artifact.uri AS artifact_uri,
                       artifact.sha256 AS artifact_sha256,
                       artifact.byte_size AS artifact_byte_size,
                       artifact.media_type,
                       artifact.producer_job_id,
                       artifact.metadata AS artifact_metadata
                FROM systematic_fx.phase1a_outcome_replay_checkpoints AS checkpoint
                LEFT JOIN systematic_fx.artifacts AS artifact
                  ON artifact.artifact_id = checkpoint.checkpoint_artifact_id
                WHERE checkpoint.outcome_replay_manifest_id = %s
                ORDER BY checkpoint.checkpoint_sequence
                """,
                (manifest_id,),
            ).fetchall()
            validated = _validate_checkpoint_chain_rows(
                rows,
                outcome_replay_manifest_id=manifest_id,
                run_fingerprint=fingerprint,
                data_root=data_root,
                profile=outcome_query_profile(str(manifest["pattern_key"])),
            )
            if validated is None:
                return None
            if manifest_status == "QUEUED":
                raise OutcomeRegistryDriftError(
                    "QUEUED outcome replay unexpectedly has checkpoints"
                )
            latest, progress_metadata, artifact_path = validated
            held = _open_held_immutable_file(artifact_path, data_root=data_root)
            try:
                _require_held_parent(
                    held,
                    data_root=data_root,
                    expected_parent=outcome_query_profile(
                        str(manifest["pattern_key"])
                    ).checkpoint_directory,
                    label="outcome checkpoint artifact",
                )
                checkpoint_sha256 = _sha256(
                    latest.get("checkpoint_artifact_sha256"),
                    label="checkpoint_artifact_sha256",
                )
                checkpoint_byte_size = _positive_identifier(
                    latest.get("checkpoint_artifact_byte_size"),
                    label="checkpoint_artifact_byte_size",
                )
                artifact_uri = str(latest["artifact_uri"])
                if (
                    held.sha256 != checkpoint_sha256
                    or held.byte_size != checkpoint_byte_size
                    or held.path.as_uri() != artifact_uri
                ):
                    raise OutcomeRegistryDriftError(
                        "latest outcome checkpoint file differs from DB identity"
                    )
                progress_metadata_sha256 = _canonical_sha256(progress_metadata)
                source_date = latest["last_completed_source_date"]
                sequence = int(latest["checkpoint_sequence"])
                source_date_count = int(latest["completed_source_date_count"])
                event_count = int(latest["source_event_count"])
                predecessor = latest.get("predecessor_checkpoint_sha256")
                checkpoint_document = _validate_checkpoint_artifact(
                    held,
                    outcome_replay_manifest_id=manifest_id,
                    run_fingerprint=fingerprint,
                    checkpoint_sequence=sequence,
                    completed_source_date_count=source_date_count,
                    last_completed_source_date=source_date,
                    source_event_count=event_count,
                    predecessor_checkpoint_sha256=predecessor,
                    progress_metadata_sha256=progress_metadata_sha256,
                    profile=outcome_query_profile(str(manifest["pattern_key"])),
                )
                _verify_held_file(held)
                return LoadedOutcomeCheckpoint(
                    outcome_replay_manifest_id=manifest_id,
                    research_run_spec_id=research_run_spec_id,
                    research_run_attempt_id=int(manifest["research_run_attempt_id"]),
                    run_fingerprint=fingerprint,
                    manifest_status=manifest_status,
                    checkpoint_sequence=sequence,
                    completed_source_date_count=source_date_count,
                    last_completed_source_date=source_date,
                    source_event_count=event_count,
                    checkpoint_artifact_id=int(latest["checkpoint_artifact_id"]),
                    checkpoint_artifact_sha256=checkpoint_sha256,
                    checkpoint_artifact_uri=artifact_uri,
                    checkpoint_artifact_byte_size=checkpoint_byte_size,
                    checkpoint_artifact_path=held.path,
                    predecessor_checkpoint_sha256=predecessor,
                    progress_metadata=progress_metadata,
                    progress_metadata_sha256=progress_metadata_sha256,
                    checkpoint_document=checkpoint_document,
                )
            finally:
                held.close()


@_translate_psycopg_errors("Phase 1A p5 equivalence-audit subject loading")
def load_phase1a_p5_audit_subject(
    database_url: str,
    *,
    data_root: Path | str,
    outcome_replay_manifest_id: int | None = None,
) -> OutcomeReplayAuditSubject:
    """Load and stream-verify the complete successful p5 subject read-only."""

    target = _database_url(database_url)
    selected_manifest_id = (
        None
        if outcome_replay_manifest_id is None
        else _positive_identifier(
            outcome_replay_manifest_id,
            label="outcome_replay_manifest_id",
        )
    )
    _, derived = _resolved_data_root(data_root)
    with psycopg.connect(target, row_factory=dict_row) as connection:
        _set_serializable_read_only(connection)
        with connection.transaction():
            subjects = connection.execute(
                """
                SELECT manifest.*, attempt.attempt_number,
                       attempt.status AS attempt_status,
                       run_spec.canonicalization_schema,
                       run_spec.canonicalization_version,
                       run_spec.experiment_id, run_spec.run_kind,
                       run_spec.engine_version, run_spec.direction,
                       run_spec.canonical_spec,
                       campaign.campaign_key,
                       artifact.artifact_id AS stored_result_artifact_id,
                       artifact.uri AS result_artifact_uri,
                       artifact.sha256 AS stored_result_artifact_sha256,
                       artifact.byte_size AS stored_result_artifact_byte_size,
                       artifact.artifact_type AS result_artifact_type,
                       artifact.media_type AS result_media_type,
                       artifact.metadata AS result_artifact_metadata
                FROM systematic_fx.phase1a_outcome_replay_manifests AS manifest
                JOIN systematic_fx.research_run_attempts AS attempt
                  ON attempt.research_run_attempt_id = manifest.research_run_attempt_id
                 AND attempt.research_run_spec_id = manifest.research_run_spec_id
                JOIN systematic_fx.research_run_specs AS run_spec
                  ON run_spec.research_run_spec_id = manifest.research_run_spec_id
                 AND run_spec.campaign_id = manifest.campaign_id
                 AND run_spec.run_fingerprint = manifest.run_fingerprint
                JOIN systematic_fx.campaigns AS campaign
                  ON campaign.campaign_id = manifest.campaign_id
                JOIN systematic_fx.artifacts AS artifact
                  ON artifact.artifact_id = manifest.result_artifact_id
                WHERE manifest.pattern_key = %s
                  AND manifest.status = 'SUCCEEDED'
                  AND (%s::bigint IS NULL OR manifest.outcome_replay_manifest_id = %s)
                ORDER BY manifest.outcome_replay_manifest_id
                """,
                (P5_QUERY_ID, selected_manifest_id, selected_manifest_id),
            ).fetchall()
            if not subjects:
                raise OutcomeRegistryStateError(
                    "p5 equivalence audit requires one successful outcome replay"
                )
            if len(subjects) != 1:
                raise OutcomeRegistryStateError(
                    "successful p5 audit subject is ambiguous; select one manifest id"
                )
            subject = subjects[0]
            fingerprint = _sha256(subject.get("run_fingerprint"), label="run_fingerprint")
            source_sha256 = _sha256(
                subject.get("source_artifact_manifest_sha256"),
                label="source_artifact_manifest_sha256",
            )
            _assert_manifest_identity(
                subject,
                run_fingerprint=fingerprint,
                source_artifact_manifest_sha256=source_sha256,
                profile=P5_OUTCOME_QUERY_PROFILE,
            )
            _validate_governed_run_spec(
                subject,
                run_fingerprint=fingerprint,
                source_artifact_manifest_sha256=source_sha256,
            )
            if subject.get("attempt_status") != "SUCCEEDED":
                raise OutcomeRegistryDriftError("p5 audit subject attempt state drift")
            result_sha256 = _sha256(
                subject.get("result_artifact_sha256"),
                label="result_artifact_sha256",
            )
            result_byte_size = _positive_identifier(
                subject.get("result_artifact_byte_size"),
                label="result_artifact_byte_size",
            )
            result_artifact_id = _positive_identifier(
                subject.get("result_artifact_id"),
                label="result_artifact_id",
            )
            if (
                subject.get("stored_result_artifact_id") != result_artifact_id
                or subject.get("stored_result_artifact_sha256") != result_sha256
                or subject.get("stored_result_artifact_byte_size") != result_byte_size
                or subject.get("result_artifact_type") != OUTCOME_ARTIFACT_TYPE
                or subject.get("result_media_type") != "application/json"
            ):
                raise OutcomeRegistryDriftError("p5 audit subject result registry drift")
            result_path = _path_from_file_uri(
                subject.get("result_artifact_uri"),
                label="p5 audit subject result",
            )
            try:
                result_relative = result_path.relative_to(derived)
            except ValueError as error:
                raise OutcomeRegistryDriftError(
                    "p5 audit subject result is outside data/derived"
                ) from error
            _verify_relative_artifact(
                result_relative.as_posix(),
                data_root=data_root,
                expected_parent=PurePosixPath("outcomes") / OUTCOME_CONFIG_ID,
                expected_sha256=result_sha256,
                expected_byte_size=result_byte_size,
                suffix=".json",
                label="p5 audit subject result",
            )
            metadata = _canonical_mapping(
                subject.get("result_artifact_metadata"),
                label="p5 audit subject result metadata",
            )
            cell_sha256 = _sha256(
                subject.get("cell_summaries_sha256"),
                label="cell_summaries_sha256",
            )
            expected_metadata = {
                "campaign_key": CAMPAIGN_KEY,
                "cell_summaries_sha256": cell_sha256,
                "outcome_config_id": OUTCOME_CONFIG_ID,
                "query_id": P5_QUERY_ID,
                "run_fingerprint": fingerprint,
                "source_artifact_manifest_sha256": source_sha256,
                "summary_row_count": EXPECTED_SUMMARY_COUNT,
            }
            mismatches = [
                key for key, expected in expected_metadata.items() if metadata.get(key) != expected
            ]
            if mismatches:
                raise OutcomeRegistryDriftError(
                    "p5 audit subject result metadata drift in fields: "
                    + ", ".join(sorted(mismatches))
                )
            checkpoint_rows = connection.execute(
                """
                SELECT checkpoint.*,
                       artifact.artifact_id AS stored_artifact_id,
                       artifact.artifact_key,
                       artifact.artifact_type,
                       artifact.uri AS artifact_uri,
                       artifact.sha256 AS artifact_sha256,
                       artifact.byte_size AS artifact_byte_size,
                       artifact.media_type,
                       artifact.producer_job_id,
                       artifact.metadata AS artifact_metadata
                FROM systematic_fx.phase1a_outcome_replay_checkpoints AS checkpoint
                JOIN systematic_fx.artifacts AS artifact
                  ON artifact.artifact_id = checkpoint.checkpoint_artifact_id
                WHERE checkpoint.outcome_replay_manifest_id = %s
                ORDER BY checkpoint.checkpoint_sequence
                """,
                (int(subject["outcome_replay_manifest_id"]),),
            ).fetchall()
            validated = _validate_checkpoint_chain_rows(
                checkpoint_rows,
                outcome_replay_manifest_id=int(subject["outcome_replay_manifest_id"]),
                run_fingerprint=fingerprint,
                data_root=data_root,
            )
            if validated is None or len(checkpoint_rows) != EXPECTED_PLANNED_SOURCE_DATE_COUNT:
                raise OutcomeRegistryDriftError("p5 audit subject checkpoint chain is incomplete")
            checkpoints: list[OutcomeReplayAuditCheckpoint] = []
            for row in checkpoint_rows:
                checkpoint_path = _path_from_file_uri(
                    row.get("artifact_uri"),
                    label="p5 audit checkpoint",
                )
                try:
                    checkpoint_relative = checkpoint_path.relative_to(derived)
                except ValueError as error:
                    raise OutcomeRegistryDriftError(
                        "p5 audit checkpoint is outside data/derived"
                    ) from error
                _verify_relative_artifact(
                    checkpoint_relative.as_posix(),
                    data_root=data_root,
                    expected_parent=P5_OUTCOME_QUERY_PROFILE.checkpoint_directory,
                    expected_sha256=row.get("checkpoint_artifact_sha256"),
                    expected_byte_size=row.get("checkpoint_artifact_byte_size"),
                    suffix=".json",
                    label=f"p5 audit checkpoint {row['checkpoint_sequence']}",
                )
                checkpoints.append(
                    OutcomeReplayAuditCheckpoint(
                        checkpoint_sequence=int(row["checkpoint_sequence"]),
                        checkpoint_artifact_sha256=str(row["checkpoint_artifact_sha256"]),
                        checkpoint_artifact_byte_size=int(row["checkpoint_artifact_byte_size"]),
                        predecessor_checkpoint_sha256=row.get("predecessor_checkpoint_sha256"),
                        last_completed_source_date=row["last_completed_source_date"],
                        source_event_count=int(row["source_event_count"]),
                        progress_metadata=_canonical_mapping(
                            row.get("progress_metadata"),
                            label="p5 audit checkpoint progress metadata",
                        ),
                    )
                )
            final_checkpoint = checkpoints[-1]
            final_checkpoint_sha256 = _sha256(
                metadata.get("final_checkpoint_sha256"),
                label="final_checkpoint_sha256",
            )
            if final_checkpoint.checkpoint_artifact_sha256 != final_checkpoint_sha256:
                raise OutcomeRegistryDriftError("p5 audit subject final checkpoint drift")
            detail_record_count = _positive_identifier(
                metadata.get("detail_record_count"),
                label="detail_record_count",
            )
            if detail_record_count != EXPECTED_DETAIL_RECORD_COUNT:
                raise OutcomeRegistryDriftError("p5 audit subject detail count drift")
            return OutcomeReplayAuditSubject(
                outcome_replay_manifest_id=int(subject["outcome_replay_manifest_id"]),
                research_run_spec_id=int(subject["research_run_spec_id"]),
                research_run_attempt_id=int(subject["research_run_attempt_id"]),
                run_fingerprint=fingerprint,
                status="SUCCEEDED",
                query_id=P5_QUERY_ID,
                source_artifact_manifest_sha256=source_sha256,
                result_artifact_id=result_artifact_id,
                result_artifact_path=result_path,
                result_artifact_sha256=result_sha256,
                result_artifact_byte_size=result_byte_size,
                cell_summaries_sha256=cell_sha256,
                cache_manifest_sha256=_sha256(
                    metadata.get("cache_manifest_sha256"),
                    label="cache_manifest_sha256",
                ),
                detail_shard_manifest_sha256=_sha256(
                    metadata.get("detail_shard_manifest_sha256"),
                    label="detail_shard_manifest_sha256",
                ),
                input_lineage_sha256=_sha256(
                    metadata.get("input_lineage_sha256"),
                    label="input_lineage_sha256",
                ),
                final_checkpoint_sha256=final_checkpoint_sha256,
                final_checkpoint_sequence=final_checkpoint.checkpoint_sequence,
                source_event_count=final_checkpoint.source_event_count,
                detail_record_count=detail_record_count,
                summary_row_count=EXPECTED_SUMMARY_COUNT,
                checkpoints=tuple(checkpoints),
            )


_EQUIVALENCE_AUDIT_SCHEMA: Final = "systematic_fx.phase1a_p5_outcome_equivalence_audit.v1"
_EQUIVALENCE_AUDIT_VERSION: Final = "phase1a_p5_outcome_equivalence_v1"
_EQUIVALENCE_AUDIT_KIND: Final = "UNINTERRUPTED_VS_RESUMED_BYTE_EQUIVALENCE"
_EQUIVALENCE_AUDIT_ENGINE_VERSION: Final = "phase1a_outcome_equivalence_audit_v1"
_EQUIVALENCE_AUDIT_ARTIFACT_TYPE: Final = "PHASE1A_OUTCOME_REPLAY_EQUIVALENCE_AUDIT"
_EQUIVALENCE_AUDIT_FIELDS: Final = {
    "artifact_schema",
    "audit_kind",
    "audit_version",
    "comparisons",
    "execution_policy",
    "mismatches",
    "observed",
    "passed",
    "query_id",
    "subject",
}
_EQUIVALENCE_SUBJECT_FIELDS: Final = {
    "cache_manifest_sha256",
    "cell_summaries_sha256",
    "checkpoint_chain_sha256",
    "checkpoint_count",
    "detail_record_count",
    "detail_shard_manifest_sha256",
    "final_checkpoint_sequence",
    "final_checkpoint_sha256",
    "input_lineage_sha256",
    "outcome_config_id",
    "outcome_replay_manifest_id",
    "query_id",
    "research_run_attempt_id",
    "research_run_spec_id",
    "result_artifact_byte_size",
    "result_artifact_sha256",
    "run_fingerprint",
    "source_artifact_manifest_sha256",
    "source_event_count",
    "status",
    "summary_row_count",
}
_EQUIVALENCE_COMPARISON_FIELDS: Final = {
    "cache_manifest_sha256",
    "cell_summaries_sha256",
    "checkpoint_chain_sha256",
    "checkpoints_all_reused",
    "control_plane_noops",
    "detail_record_count",
    "detail_shard_manifest_sha256",
    "detail_shards_all_reused",
    "final_checkpoint_sequence",
    "final_checkpoint_sha256",
    "final_result_reused",
    "forced_uninterrupted_start",
    "input_lineage_sha256",
    "manifest_identity",
    "result_artifact_byte_size",
    "result_artifact_sha256",
    "run_fingerprint",
    "source_event_count",
    "summary_row_count",
}
_EQUIVALENCE_EXECUTION_POLICY: Final = {
    "checkpoint_load": "FORCED_NONE",
    "control_plane_mutations": "NO_OP",
    "daily_artifact_policy": "MUST_REUSE_IDENTICAL_CONTENT",
    "replay_start": "FIRST_PLANNED_SOURCE_DATE",
}
_EQUIVALENCE_OBSERVED_FIELDS: Final = {
    "cache_manifest_sha256",
    "cell_summaries_sha256",
    "checkpoint_chain_sha256",
    "checkpoint_count",
    "checkpoint_load_count",
    "checkpoint_publication_count",
    "checkpoint_reused_count",
    "complete_noop_count",
    "detail_record_count",
    "detail_shard_manifest_sha256",
    "detail_shard_publication_count",
    "detail_shard_reused_count",
    "final_checkpoint_sequence",
    "final_checkpoint_sha256",
    "final_result_disposition",
    "input_lineage_sha256",
    "outcome_replay_manifest_id",
    "result_artifact_byte_size",
    "result_artifact_sha256",
    "run_fingerprint",
    "source_event_count",
    "start_noop_count",
    "summary_row_count",
}


def _validate_equivalence_audit_subject_payload(value: object) -> dict[str, object]:
    subject = _canonical_mapping(value, label="p5 equivalence audit subject")
    if set(subject) != _EQUIVALENCE_SUBJECT_FIELDS:
        raise OutcomeRegistryDriftError("p5 equivalence audit subject field schema drift")
    expected = {
        "checkpoint_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "detail_record_count": EXPECTED_DETAIL_RECORD_COUNT,
        "final_checkpoint_sequence": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "outcome_config_id": OUTCOME_CONFIG_ID,
        "query_id": P5_QUERY_ID,
        "status": "SUCCEEDED",
        "summary_row_count": EXPECTED_SUMMARY_COUNT,
    }
    mismatches = [key for key, item in expected.items() if subject.get(key) != item]
    if mismatches:
        raise OutcomeRegistryDriftError(
            "p5 equivalence audit subject frozen identity drift in fields: "
            + ", ".join(sorted(mismatches))
        )
    for key in (
        "outcome_replay_manifest_id",
        "research_run_attempt_id",
        "research_run_spec_id",
        "result_artifact_byte_size",
        "source_event_count",
    ):
        _positive_identifier(subject.get(key), label=f"audit subject {key}")
    for key in (
        "cache_manifest_sha256",
        "cell_summaries_sha256",
        "checkpoint_chain_sha256",
        "detail_shard_manifest_sha256",
        "final_checkpoint_sha256",
        "input_lineage_sha256",
        "result_artifact_sha256",
        "run_fingerprint",
        "source_artifact_manifest_sha256",
    ):
        _sha256(subject.get(key), label=f"audit subject {key}")
    return subject


def _validate_equivalence_audit_payload(
    held: _HeldFile,
    *,
    expected_subject: Mapping[str, object],
) -> dict[str, Any]:
    if held.path.name != f"sha256={held.sha256}.json":
        raise OutcomeRegistryError("audit artifact filename must be sha256=<content>.json")
    document = _canonical_json_document(held.content, label="p5 equivalence audit artifact")
    if set(document) != _EQUIVALENCE_AUDIT_FIELDS:
        raise OutcomeRegistryDriftError("p5 equivalence audit field schema drift")
    subject = _validate_equivalence_audit_subject_payload(expected_subject)
    expected_static = {
        "artifact_schema": _EQUIVALENCE_AUDIT_SCHEMA,
        "audit_kind": _EQUIVALENCE_AUDIT_KIND,
        "audit_version": _EQUIVALENCE_AUDIT_VERSION,
        "passed": True,
        "query_id": P5_QUERY_ID,
        "subject": subject,
    }
    mismatches = [key for key, expected in expected_static.items() if document.get(key) != expected]
    if mismatches:
        raise OutcomeRegistryDriftError(
            "p5 equivalence audit identity drift in fields: " + ", ".join(sorted(mismatches))
        )
    if document.get("mismatches") != []:
        raise OutcomeRegistryStateError("a mismatched p5 replay cannot register a PASSED audit")
    execution_policy = _canonical_mapping(
        document.get("execution_policy"),
        label="p5 equivalence execution policy",
    )
    if execution_policy != _EQUIVALENCE_EXECUTION_POLICY:
        raise OutcomeRegistryDriftError("p5 equivalence execution policy drift")
    observed = _canonical_mapping(
        document.get("observed"),
        label="p5 equivalence observed identity",
    )
    if set(observed) != _EQUIVALENCE_OBSERVED_FIELDS:
        raise OutcomeRegistryDriftError("p5 equivalence observed field schema drift")
    expected_observed = {
        "cache_manifest_sha256": subject["cache_manifest_sha256"],
        "cell_summaries_sha256": subject["cell_summaries_sha256"],
        "checkpoint_chain_sha256": subject["checkpoint_chain_sha256"],
        "checkpoint_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "checkpoint_load_count": 1,
        "checkpoint_publication_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "checkpoint_reused_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "complete_noop_count": 1,
        "detail_record_count": subject["detail_record_count"],
        "detail_shard_manifest_sha256": subject["detail_shard_manifest_sha256"],
        "detail_shard_publication_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "detail_shard_reused_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "final_checkpoint_sequence": subject["final_checkpoint_sequence"],
        "final_checkpoint_sha256": subject["final_checkpoint_sha256"],
        "final_result_disposition": "REUSED",
        "input_lineage_sha256": subject["input_lineage_sha256"],
        "outcome_replay_manifest_id": subject["outcome_replay_manifest_id"],
        "result_artifact_byte_size": subject["result_artifact_byte_size"],
        "result_artifact_sha256": subject["result_artifact_sha256"],
        "run_fingerprint": subject["run_fingerprint"],
        "source_event_count": subject["source_event_count"],
        "start_noop_count": 1,
        "summary_row_count": subject["summary_row_count"],
    }
    observed_mismatches = [
        key for key, expected in expected_observed.items() if observed.get(key) != expected
    ]
    if observed_mismatches:
        raise OutcomeRegistryDriftError(
            "uninterrupted p5 observed identity drift in fields: "
            + ", ".join(sorted(observed_mismatches))
        )
    computed_comparisons = {
        "cache_manifest_sha256": (
            observed["cache_manifest_sha256"] == subject["cache_manifest_sha256"]
        ),
        "cell_summaries_sha256": (
            observed["cell_summaries_sha256"] == subject["cell_summaries_sha256"]
        ),
        "checkpoint_chain_sha256": (
            observed["checkpoint_chain_sha256"] == subject["checkpoint_chain_sha256"]
        ),
        "checkpoints_all_reused": (
            observed["checkpoint_publication_count"] == EXPECTED_PLANNED_SOURCE_DATE_COUNT
            and observed["checkpoint_reused_count"] == EXPECTED_PLANNED_SOURCE_DATE_COUNT
        ),
        "control_plane_noops": (
            observed["start_noop_count"] == 1 and observed["complete_noop_count"] == 1
        ),
        "detail_record_count": (observed["detail_record_count"] == subject["detail_record_count"]),
        "detail_shard_manifest_sha256": (
            observed["detail_shard_manifest_sha256"] == subject["detail_shard_manifest_sha256"]
        ),
        "detail_shards_all_reused": (
            observed["detail_shard_publication_count"] == EXPECTED_PLANNED_SOURCE_DATE_COUNT
            and observed["detail_shard_reused_count"] == EXPECTED_PLANNED_SOURCE_DATE_COUNT
        ),
        "final_checkpoint_sequence": (
            observed["final_checkpoint_sequence"] == subject["final_checkpoint_sequence"]
        ),
        "final_checkpoint_sha256": (
            observed["final_checkpoint_sha256"] == subject["final_checkpoint_sha256"]
        ),
        "final_result_reused": observed["final_result_disposition"] == "REUSED",
        "forced_uninterrupted_start": observed["checkpoint_load_count"] == 1,
        "input_lineage_sha256": (
            observed["input_lineage_sha256"] == subject["input_lineage_sha256"]
        ),
        "manifest_identity": (
            observed["outcome_replay_manifest_id"] == subject["outcome_replay_manifest_id"]
            and observed["run_fingerprint"] == subject["run_fingerprint"]
        ),
        "result_artifact_byte_size": (
            observed["result_artifact_byte_size"] == subject["result_artifact_byte_size"]
        ),
        "result_artifact_sha256": (
            observed["result_artifact_sha256"] == subject["result_artifact_sha256"]
        ),
        "run_fingerprint": observed["run_fingerprint"] == subject["run_fingerprint"],
        "source_event_count": (observed["source_event_count"] == subject["source_event_count"]),
        "summary_row_count": observed["summary_row_count"] == subject["summary_row_count"],
    }
    comparisons = _canonical_mapping(
        document.get("comparisons"),
        label="p5 equivalence comparisons",
    )
    if set(comparisons) != _EQUIVALENCE_COMPARISON_FIELDS:
        raise OutcomeRegistryDriftError("p5 equivalence comparison field schema drift")
    if comparisons != computed_comparisons or any(
        value is not True for value in computed_comparisons.values()
    ):
        raise OutcomeRegistryStateError(
            "p5 equivalence comparisons differ from recomputed observed semantics"
        )
    return document


def _validate_equivalence_audit_artifact(
    held: _HeldFile,
    *,
    subject: OutcomeReplayAuditSubject,
) -> dict[str, Any]:
    return _validate_equivalence_audit_payload(
        held,
        expected_subject=subject.subject_payload,
    )


def _validate_equivalence_audit_run_spec(
    row: Mapping[str, Any],
    *,
    subject: OutcomeReplayAuditSubject,
) -> str:
    """Validate the frozen VALIDATION RunSpec bound to one p5 audit."""

    validation_fingerprint = _sha256(
        row.get("run_fingerprint"),
        label="validation_run_fingerprint",
    )
    canonical_spec = row.get("canonical_spec")
    if not isinstance(canonical_spec, Mapping):
        raise OutcomeRegistryDriftError("validation RunSpec payload is invalid")
    if _canonical_sha256(canonical_spec) != validation_fingerprint:
        raise OutcomeRegistryDriftError("validation RunSpec fingerprint drift")
    parameters = canonical_spec.get("parameters")
    if not isinstance(parameters, Mapping):
        raise OutcomeRegistryDriftError("validation RunSpec parameters are invalid")
    expected_parameters = {
        "audit_kind": _EQUIVALENCE_AUDIT_KIND,
        "cache_manifest_sha256": subject.cache_manifest_sha256,
        "cell_summaries_sha256": subject.cell_summaries_sha256,
        "checkpoint_chain_sha256": subject.checkpoint_chain_sha256,
        "checkpoint_count": len(subject.checkpoints),
        "detail_shard_manifest_sha256": subject.detail_shard_manifest_sha256,
        "final_checkpoint_sha256": subject.final_checkpoint_sha256,
        "input_lineage_sha256": subject.input_lineage_sha256,
        "predecessor_outcome_replay_manifest_id": (subject.outcome_replay_manifest_id),
        "predecessor_result_artifact_sha256": subject.result_artifact_sha256,
        "predecessor_run_fingerprint": subject.run_fingerprint,
        "query_id": P5_QUERY_ID,
    }
    ownership_expected = {
        "campaign_key": CAMPAIGN_KEY,
        "direction": "BOTH",
        "engine_version": _EQUIVALENCE_AUDIT_ENGINE_VERSION,
        "experiment_id": None,
        "run_kind": "VALIDATION",
    }
    drift = [key for key, expected in ownership_expected.items() if row.get(key) != expected]
    drift.extend(
        key for key, expected in expected_parameters.items() if parameters.get(key) != expected
    )
    if drift:
        raise OutcomeRegistryDriftError(
            "validation audit RunSpec drift in fields: " + ", ".join(sorted(set(drift)))
        )
    return validation_fingerprint


@_translate_psycopg_errors("Phase 1A p5 equivalence-audit registration")
def register_phase1a_p5_equivalence_audit(
    database_url: str,
    *,
    validation_research_run_attempt_id: int,
    subject: OutcomeReplayAuditSubject,
    audit_artifact_path: Path,
    data_root: Path | str,
) -> OutcomeEquivalenceAuditReport:
    """Atomically publish one PASSED validation attempt, artifact, and gate."""

    target = _database_url(database_url)
    attempt_id = _positive_identifier(
        validation_research_run_attempt_id,
        label="validation_research_run_attempt_id",
    )
    if not isinstance(subject, OutcomeReplayAuditSubject):
        raise OutcomeRegistryError("subject must be an OutcomeReplayAuditSubject")
    if subject.status != "SUCCEEDED" or subject.query_id != P5_QUERY_ID:
        raise OutcomeRegistryStateError("equivalence audit subject must be successful p5")
    held = _open_held_immutable_file(Path(audit_artifact_path), data_root=data_root)
    try:
        _require_held_parent(
            held,
            data_root=data_root,
            expected_parent=_EQUIVALENCE_AUDIT_DIRECTORY,
            label="p5 equivalence audit artifact",
        )
        _validate_equivalence_audit_artifact(held, subject=subject)
        with psycopg.connect(target, row_factory=dict_row) as connection:
            _set_serializable(connection)
            with connection.transaction():
                attempt = connection.execute(
                    """
                    SELECT attempt.research_run_attempt_id,
                           attempt.research_run_spec_id,
                           attempt.status, attempt.result_artifact_id,
                           attempt.started_at, attempt.finished_at,
                           attempt.result_summary,
                           run_spec.run_fingerprint,
                           run_spec.run_kind, run_spec.engine_version,
                           run_spec.direction, run_spec.experiment_id,
                           run_spec.canonical_spec,
                           run_spec.campaign_id, campaign.campaign_key
                    FROM systematic_fx.research_run_attempts AS attempt
                    JOIN systematic_fx.research_run_specs AS run_spec
                      ON run_spec.research_run_spec_id = attempt.research_run_spec_id
                    JOIN systematic_fx.campaigns AS campaign
                      ON campaign.campaign_id = run_spec.campaign_id
                    WHERE attempt.research_run_attempt_id = %s
                    FOR UPDATE OF attempt, run_spec, campaign
                    """,
                    (attempt_id,),
                ).fetchone()
                if attempt is None:
                    raise OutcomeRegistryError("validation audit attempt does not exist")
                validation_fingerprint = _validate_equivalence_audit_run_spec(
                    attempt,
                    subject=subject,
                )
                existing = connection.execute(
                    """
                    SELECT audit.*, artifact.uri AS audit_artifact_uri,
                           artifact.sha256 AS stored_audit_artifact_sha256,
                           artifact.byte_size AS stored_audit_artifact_byte_size
                    FROM systematic_fx.phase1a_outcome_replay_equivalence_audits AS audit
                    JOIN systematic_fx.artifacts AS artifact
                      ON artifact.artifact_id = audit.audit_artifact_id
                    WHERE audit.validation_research_run_attempt_id = %s
                    FOR SHARE OF audit, artifact
                    """,
                    (attempt_id,),
                ).fetchone()
                if existing is not None:
                    expected_existing = {
                        "audit_artifact_sha256": held.sha256,
                        "audit_artifact_uri": held.path.as_uri(),
                        "audit_artifact_byte_size": held.byte_size,
                        "checkpoint_chain_sha256": subject.checkpoint_chain_sha256,
                        "passed": True,
                        "predecessor_outcome_replay_manifest_id": (
                            subject.outcome_replay_manifest_id
                        ),
                        "stored_audit_artifact_sha256": held.sha256,
                        "stored_audit_artifact_byte_size": held.byte_size,
                    }
                    mismatches = [
                        key
                        for key, expected in expected_existing.items()
                        if existing.get(key) != expected
                    ]
                    if mismatches:
                        raise OutcomeRegistryDriftError(
                            "stored equivalence audit drift in fields: "
                            + ", ".join(sorted(mismatches))
                        )
                    _verify_held_file(held)
                    return OutcomeEquivalenceAuditReport(
                        outcome_equivalence_audit_id=int(existing["outcome_equivalence_audit_id"]),
                        predecessor_outcome_replay_manifest_id=(subject.outcome_replay_manifest_id),
                        validation_research_run_spec_id=int(attempt["research_run_spec_id"]),
                        validation_research_run_attempt_id=attempt_id,
                        validation_run_fingerprint=validation_fingerprint,
                        audit_artifact_id=int(existing["audit_artifact_id"]),
                        audit_artifact_sha256=held.sha256,
                        audit_artifact_uri=held.path.as_uri(),
                        audit_artifact_byte_size=held.byte_size,
                        checkpoint_chain_sha256=subject.checkpoint_chain_sha256,
                        passed=True,
                        created=False,
                    )
                if attempt.get("status") != "RUNNING":
                    raise OutcomeRegistryStateError(
                        "new equivalence audit requires a RUNNING validation attempt"
                    )
                artifact_key = f"{CAMPAIGN_KEY}:outcome-equivalence-audit:{validation_fingerprint}"
                artifact_uri = held.path.as_uri()
                metadata = {
                    "audit_kind": _EQUIVALENCE_AUDIT_KIND,
                    "campaign_key": CAMPAIGN_KEY,
                    "checkpoint_chain_sha256": subject.checkpoint_chain_sha256,
                    "passed": True,
                    "predecessor_outcome_replay_manifest_id": (subject.outcome_replay_manifest_id),
                    "predecessor_result_artifact_sha256": (subject.result_artifact_sha256),
                    "validation_run_fingerprint": validation_fingerprint,
                }
                artifact_rows = connection.execute(
                    """
                    SELECT artifact_id, artifact_key, artifact_type, uri, sha256,
                           byte_size, media_type, producer_job_id, metadata
                    FROM systematic_fx.artifacts
                    WHERE artifact_key = %s OR uri = %s
                    FOR UPDATE
                    """,
                    (artifact_key, artifact_uri),
                ).fetchall()
                if artifact_rows:
                    raise OutcomeRegistryDriftError(
                        "unregistered equivalence artifact key or URI already exists"
                    )
                artifact = connection.execute(
                    """
                    INSERT INTO systematic_fx.artifacts
                        (artifact_key, artifact_type, uri, sha256, byte_size,
                         media_type, metadata)
                    VALUES (%s, %s, %s, %s, %s, 'application/json', %s)
                    RETURNING artifact_id
                    """,
                    (
                        artifact_key,
                        _EQUIVALENCE_AUDIT_ARTIFACT_TYPE,
                        artifact_uri,
                        held.sha256,
                        held.byte_size,
                        Jsonb(metadata),
                    ),
                ).fetchone()
                if artifact is None:  # pragma: no cover
                    raise OutcomeRegistryDatabaseError("audit artifact returned no identity")
                artifact_id = int(artifact["artifact_id"])
                finished_at = datetime.now(UTC)
                result_summary = {
                    "audit_artifact_sha256": held.sha256,
                    "checkpoint_chain_sha256": subject.checkpoint_chain_sha256,
                    "passed": True,
                    "predecessor_outcome_replay_manifest_id": (subject.outcome_replay_manifest_id),
                    "predecessor_result_artifact_sha256": (subject.result_artifact_sha256),
                }
                updated = connection.execute(
                    """
                    UPDATE systematic_fx.research_run_attempts
                    SET status = 'SUCCEEDED', result_artifact_id = %s,
                        result_summary = %s, finished_at = %s
                    WHERE research_run_attempt_id = %s AND status = 'RUNNING'
                    RETURNING research_run_attempt_id
                    """,
                    (artifact_id, Jsonb(result_summary), finished_at, attempt_id),
                ).fetchone()
                if updated is None:
                    raise OutcomeRegistryStateError(
                        "validation attempt changed before audit publication"
                    )
                inserted = connection.execute(
                    """
                    INSERT INTO systematic_fx.phase1a_outcome_replay_equivalence_audits
                        (predecessor_outcome_replay_manifest_id, campaign_id,
                         validation_research_run_spec_id,
                         validation_research_run_attempt_id,
                         validation_run_fingerprint, audit_artifact_id,
                         audit_artifact_sha256, audit_artifact_byte_size,
                         predecessor_run_fingerprint,
                         predecessor_result_artifact_sha256,
                         uninterrupted_result_artifact_sha256,
                         resumed_result_artifact_sha256, cache_manifest_sha256,
                         cell_summaries_sha256, detail_shard_manifest_sha256,
                         input_lineage_sha256, final_checkpoint_sha256,
                         checkpoint_chain_sha256, checkpoint_count, passed)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, true)
                    RETURNING outcome_equivalence_audit_id
                    """,
                    (
                        subject.outcome_replay_manifest_id,
                        int(attempt["campaign_id"]),
                        int(attempt["research_run_spec_id"]),
                        attempt_id,
                        validation_fingerprint,
                        artifact_id,
                        held.sha256,
                        held.byte_size,
                        subject.run_fingerprint,
                        subject.result_artifact_sha256,
                        subject.result_artifact_sha256,
                        subject.result_artifact_sha256,
                        subject.cache_manifest_sha256,
                        subject.cell_summaries_sha256,
                        subject.detail_shard_manifest_sha256,
                        subject.input_lineage_sha256,
                        subject.final_checkpoint_sha256,
                        subject.checkpoint_chain_sha256,
                        len(subject.checkpoints),
                    ),
                ).fetchone()
                if inserted is None:  # pragma: no cover
                    raise OutcomeRegistryDatabaseError("audit registration returned no identity")
                audit_id = int(inserted["outcome_equivalence_audit_id"])
                _verify_held_file(held)
        return OutcomeEquivalenceAuditReport(
            outcome_equivalence_audit_id=audit_id,
            predecessor_outcome_replay_manifest_id=subject.outcome_replay_manifest_id,
            validation_research_run_spec_id=int(attempt["research_run_spec_id"]),
            validation_research_run_attempt_id=attempt_id,
            validation_run_fingerprint=validation_fingerprint,
            audit_artifact_id=artifact_id,
            audit_artifact_sha256=held.sha256,
            audit_artifact_uri=held.path.as_uri(),
            audit_artifact_byte_size=held.byte_size,
            checkpoint_chain_sha256=subject.checkpoint_chain_sha256,
            passed=True,
            created=True,
        )
    finally:
        held.close()


@_translate_psycopg_errors("Phase 1A p5 equivalence-audit attempt loading")
def load_phase1a_p5_equivalence_audit_for_attempt(
    database_url: str,
    *,
    validation_research_run_attempt_id: int,
    data_root: Path | str,
) -> LoadedOutcomeEquivalenceAudit:
    """Load the exact PASSED audit owned by one successful validation attempt."""

    target = _database_url(database_url)
    attempt_id = _positive_identifier(
        validation_research_run_attempt_id,
        label="validation_research_run_attempt_id",
    )
    with psycopg.connect(target, row_factory=dict_row) as connection:
        _set_serializable_read_only(connection)
        with connection.transaction():
            row = connection.execute(
                """
                SELECT audit.*,
                       attempt.status AS validation_attempt_status,
                       attempt.result_artifact_id AS validation_attempt_artifact_id,
                       attempt.result_summary AS validation_attempt_result_summary,
                       attempt.started_at AS validation_attempt_started_at,
                       attempt.finished_at AS validation_attempt_finished_at,
                       run_spec.run_fingerprint,
                       run_spec.run_kind, run_spec.engine_version,
                       run_spec.direction, run_spec.experiment_id,
                       run_spec.canonical_spec,
                       campaign.campaign_key,
                       artifact.artifact_id AS stored_audit_artifact_id,
                       artifact.artifact_key AS stored_audit_artifact_key,
                       artifact.artifact_type AS stored_audit_artifact_type,
                       artifact.uri AS audit_artifact_uri,
                       artifact.sha256 AS stored_audit_artifact_sha256,
                       artifact.byte_size AS stored_audit_artifact_byte_size,
                       artifact.media_type AS stored_audit_media_type,
                       artifact.producer_job_id AS stored_audit_producer_job_id,
                       artifact.metadata AS stored_audit_metadata
                FROM systematic_fx.phase1a_outcome_replay_equivalence_audits AS audit
                JOIN systematic_fx.research_run_attempts AS attempt
                  ON attempt.research_run_attempt_id =
                     audit.validation_research_run_attempt_id
                 AND attempt.research_run_spec_id =
                     audit.validation_research_run_spec_id
                JOIN systematic_fx.research_run_specs AS run_spec
                  ON run_spec.research_run_spec_id =
                     audit.validation_research_run_spec_id
                 AND run_spec.campaign_id = audit.campaign_id
                 AND run_spec.run_fingerprint = audit.validation_run_fingerprint
                JOIN systematic_fx.campaigns AS campaign
                  ON campaign.campaign_id = audit.campaign_id
                JOIN systematic_fx.artifacts AS artifact
                  ON artifact.artifact_id = audit.audit_artifact_id
                WHERE audit.validation_research_run_attempt_id = %s
                """,
                (attempt_id,),
            ).fetchone()
    if row is None:
        raise OutcomeRegistryStateError(
            "validation reused_attempt_id is not bound to a PASSED p5 equivalence audit"
        )

    predecessor_manifest_id = _positive_identifier(
        row.get("predecessor_outcome_replay_manifest_id"),
        label="predecessor_outcome_replay_manifest_id",
    )
    subject = load_phase1a_p5_audit_subject(
        target,
        data_root=data_root,
        outcome_replay_manifest_id=predecessor_manifest_id,
    )
    validation_fingerprint = _validate_equivalence_audit_run_spec(
        row,
        subject=subject,
    )
    if row.get("validation_attempt_status") != "SUCCEEDED":
        raise OutcomeRegistryStateError("equivalence audit validation attempt is not SUCCEEDED")
    if not isinstance(row.get("validation_attempt_started_at"), datetime) or not isinstance(
        row.get("validation_attempt_finished_at"), datetime
    ):
        raise OutcomeRegistryDriftError(
            "successful equivalence audit attempt has incomplete timestamps"
        )

    audit_artifact_id = _positive_identifier(
        row.get("audit_artifact_id"),
        label="audit_artifact_id",
    )
    audit_artifact_sha256 = _sha256(
        row.get("audit_artifact_sha256"),
        label="audit_artifact_sha256",
    )
    audit_artifact_byte_size = _positive_identifier(
        row.get("audit_artifact_byte_size"),
        label="audit_artifact_byte_size",
    )
    expected_row = {
        "cache_manifest_sha256": subject.cache_manifest_sha256,
        "cell_summaries_sha256": subject.cell_summaries_sha256,
        "checkpoint_chain_sha256": subject.checkpoint_chain_sha256,
        "checkpoint_count": len(subject.checkpoints),
        "detail_shard_manifest_sha256": subject.detail_shard_manifest_sha256,
        "final_checkpoint_sha256": subject.final_checkpoint_sha256,
        "input_lineage_sha256": subject.input_lineage_sha256,
        "passed": True,
        "predecessor_result_artifact_sha256": subject.result_artifact_sha256,
        "predecessor_run_fingerprint": subject.run_fingerprint,
        "resumed_result_artifact_sha256": subject.result_artifact_sha256,
        "stored_audit_artifact_byte_size": audit_artifact_byte_size,
        "stored_audit_artifact_id": audit_artifact_id,
        "stored_audit_artifact_sha256": audit_artifact_sha256,
        "uninterrupted_result_artifact_sha256": subject.result_artifact_sha256,
        "validation_attempt_artifact_id": audit_artifact_id,
        "validation_research_run_attempt_id": attempt_id,
        "validation_run_fingerprint": validation_fingerprint,
    }
    row_drift = [key for key, expected in expected_row.items() if row.get(key) != expected]
    if row_drift:
        raise OutcomeRegistryDriftError(
            "stored equivalence audit attempt drift in fields: " + ", ".join(sorted(row_drift))
        )

    expected_result_summary = {
        "audit_artifact_sha256": audit_artifact_sha256,
        "checkpoint_chain_sha256": subject.checkpoint_chain_sha256,
        "passed": True,
        "predecessor_outcome_replay_manifest_id": subject.outcome_replay_manifest_id,
        "predecessor_result_artifact_sha256": subject.result_artifact_sha256,
    }
    if (
        _canonical_mapping(
            row.get("validation_attempt_result_summary"),
            label="validation attempt result_summary",
        )
        != expected_result_summary
    ):
        raise OutcomeRegistryDriftError("validation attempt result_summary drift")
    expected_artifact_metadata = {
        "audit_kind": _EQUIVALENCE_AUDIT_KIND,
        "campaign_key": CAMPAIGN_KEY,
        "checkpoint_chain_sha256": subject.checkpoint_chain_sha256,
        "passed": True,
        "predecessor_outcome_replay_manifest_id": subject.outcome_replay_manifest_id,
        "predecessor_result_artifact_sha256": subject.result_artifact_sha256,
        "validation_run_fingerprint": validation_fingerprint,
    }
    expected_artifact = {
        "stored_audit_artifact_key": (
            f"{CAMPAIGN_KEY}:outcome-equivalence-audit:{validation_fingerprint}"
        ),
        "stored_audit_artifact_type": _EQUIVALENCE_AUDIT_ARTIFACT_TYPE,
        "stored_audit_media_type": "application/json",
        "stored_audit_producer_job_id": None,
    }
    artifact_drift = [
        key for key, expected in expected_artifact.items() if row.get(key) != expected
    ]
    if (
        artifact_drift
        or _canonical_mapping(
            row.get("stored_audit_metadata"),
            label="equivalence audit artifact metadata",
        )
        != expected_artifact_metadata
    ):
        raise OutcomeRegistryDriftError(
            "equivalence audit artifact registry drift"
            + (" in fields: " + ", ".join(sorted(artifact_drift)) if artifact_drift else "")
        )

    audit_path = _path_from_file_uri(
        row.get("audit_artifact_uri"),
        label="p5 equivalence audit artifact",
    )
    held = _open_held_immutable_file(audit_path, data_root=data_root)
    try:
        _require_held_parent(
            held,
            data_root=data_root,
            expected_parent=_EQUIVALENCE_AUDIT_DIRECTORY,
            label="p5 equivalence audit artifact",
        )
        if (
            held.sha256 != audit_artifact_sha256
            or held.byte_size != audit_artifact_byte_size
            or held.path.as_uri() != row.get("audit_artifact_uri")
        ):
            raise OutcomeRegistryDriftError(
                "p5 equivalence audit file differs from its registry identity"
            )
        _validate_equivalence_audit_artifact(held, subject=subject)
        _verify_held_file(held)
        report = OutcomeEquivalenceAuditReport(
            outcome_equivalence_audit_id=_positive_identifier(
                row.get("outcome_equivalence_audit_id"),
                label="outcome_equivalence_audit_id",
            ),
            predecessor_outcome_replay_manifest_id=predecessor_manifest_id,
            validation_research_run_spec_id=_positive_identifier(
                row.get("validation_research_run_spec_id"),
                label="validation_research_run_spec_id",
            ),
            validation_research_run_attempt_id=attempt_id,
            validation_run_fingerprint=validation_fingerprint,
            audit_artifact_id=audit_artifact_id,
            audit_artifact_sha256=audit_artifact_sha256,
            audit_artifact_uri=held.path.as_uri(),
            audit_artifact_byte_size=audit_artifact_byte_size,
            checkpoint_chain_sha256=subject.checkpoint_chain_sha256,
            passed=True,
            created=False,
        )
        return LoadedOutcomeEquivalenceAudit(
            audit=report,
            predecessor_gate=_predecessor_gate_from_row(row),
            audit_artifact_path=held.path,
        )
    finally:
        held.close()


def _hold_phase1a_p1_predecessor_evidence(
    database_url: str,
    connection: psycopg.Connection[dict[str, Any]],
    *,
    equivalence_audit_id: int,
    data_root: Path | str,
) -> tuple[OutcomePredecessorGate, _HeldFile]:
    """Lightly revalidate live audit evidence and retain secure descriptors."""

    _database_url(database_url)
    audit_id = _positive_identifier(
        equivalence_audit_id,
        label="equivalence_audit_id",
    )
    row = connection.execute(
        """
        SELECT audit.*,
               audit_artifact.uri AS audit_artifact_uri,
               audit_artifact.sha256 AS stored_audit_artifact_sha256,
               audit_artifact.byte_size AS stored_audit_artifact_byte_size,
               audit_artifact.artifact_type AS stored_audit_artifact_type,
               audit_artifact.media_type AS stored_audit_media_type,
               subject_manifest.pattern_key AS subject_query_id,
               subject_manifest.status AS subject_status,
               subject_manifest.research_run_spec_id AS subject_run_spec_id,
               subject_manifest.research_run_attempt_id AS subject_run_attempt_id,
               subject_manifest.run_fingerprint AS subject_run_fingerprint,
               subject_manifest.source_artifact_manifest_sha256,
               subject_manifest.result_artifact_id AS subject_result_artifact_id,
               subject_manifest.result_artifact_sha256 AS subject_result_sha256,
               subject_manifest.result_artifact_byte_size AS subject_result_byte_size,
               subject_manifest.cell_summaries_sha256 AS subject_cell_sha256,
               subject_manifest.expected_detail_record_count,
               subject_manifest.expected_summary_count,
               result_artifact.artifact_id AS stored_result_artifact_id,
               result_artifact.sha256 AS stored_result_sha256,
               result_artifact.byte_size AS stored_result_byte_size,
               result_artifact.metadata AS subject_result_metadata,
               final_checkpoint.checkpoint_sequence AS final_checkpoint_sequence,
               final_checkpoint.checkpoint_artifact_sha256 AS stored_final_checkpoint_sha256,
               final_checkpoint.source_event_count AS subject_source_event_count
        FROM systematic_fx.phase1a_outcome_replay_equivalence_audits AS audit
        JOIN systematic_fx.artifacts AS audit_artifact
          ON audit_artifact.artifact_id = audit.audit_artifact_id
        JOIN systematic_fx.phase1a_outcome_replay_manifests AS subject_manifest
          ON subject_manifest.outcome_replay_manifest_id =
             audit.predecessor_outcome_replay_manifest_id
        JOIN systematic_fx.artifacts AS result_artifact
          ON result_artifact.artifact_id = subject_manifest.result_artifact_id
        JOIN systematic_fx.phase1a_outcome_replay_checkpoints AS final_checkpoint
          ON final_checkpoint.outcome_replay_manifest_id =
             subject_manifest.outcome_replay_manifest_id
         AND final_checkpoint.checkpoint_sequence = %s
        WHERE audit.outcome_equivalence_audit_id = %s AND audit.passed = true
        """,
        (EXPECTED_PLANNED_SOURCE_DATE_COUNT, audit_id),
    ).fetchone()
    if row is None:
        raise OutcomeRegistryStateError(
            "p1_05 state transition requires its PASSED predecessor audit"
        )
    stored_gate = _predecessor_gate_from_row(row)
    result_metadata = _canonical_mapping(
        row.get("subject_result_metadata"),
        label="p5 predecessor result metadata",
    )
    expected_row = {
        "audit_artifact_byte_size": row.get("stored_audit_artifact_byte_size"),
        "audit_artifact_sha256": row.get("stored_audit_artifact_sha256"),
        "cell_summaries_sha256": row.get("subject_cell_sha256"),
        "expected_detail_record_count": EXPECTED_DETAIL_RECORD_COUNT,
        "expected_summary_count": EXPECTED_SUMMARY_COUNT,
        "final_checkpoint_sequence": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "input_lineage_sha256": result_metadata.get("input_lineage_sha256"),
        "predecessor_result_artifact_sha256": row.get("subject_result_sha256"),
        "predecessor_run_fingerprint": row.get("subject_run_fingerprint"),
        "stored_audit_artifact_type": _EQUIVALENCE_AUDIT_ARTIFACT_TYPE,
        "stored_audit_media_type": "application/json",
        "stored_final_checkpoint_sha256": row.get("final_checkpoint_sha256"),
        "stored_result_artifact_id": row.get("subject_result_artifact_id"),
        "stored_result_byte_size": row.get("subject_result_byte_size"),
        "stored_result_sha256": row.get("subject_result_sha256"),
        "subject_query_id": P5_QUERY_ID,
        "subject_status": "SUCCEEDED",
    }
    row_drift = [key for key, expected in expected_row.items() if row.get(key) != expected]
    expected_metadata = {
        "cache_manifest_sha256": row.get("cache_manifest_sha256"),
        "cell_summaries_sha256": row.get("cell_summaries_sha256"),
        "detail_record_count": EXPECTED_DETAIL_RECORD_COUNT,
        "detail_shard_manifest_sha256": row.get("detail_shard_manifest_sha256"),
        "final_checkpoint_sha256": row.get("final_checkpoint_sha256"),
        "input_lineage_sha256": row.get("input_lineage_sha256"),
        "outcome_config_id": OUTCOME_CONFIG_ID,
        "query_id": P5_QUERY_ID,
        "summary_row_count": EXPECTED_SUMMARY_COUNT,
    }
    row_drift.extend(
        f"subject_result_metadata.{key}"
        for key, expected in expected_metadata.items()
        if result_metadata.get(key) != expected
    )
    if row_drift:
        raise OutcomeRegistryDriftError(
            "p1_05 predecessor registry evidence drift in fields: " + ", ".join(sorted(row_drift))
        )
    subject_payload = {
        "cache_manifest_sha256": row["cache_manifest_sha256"],
        "cell_summaries_sha256": row["cell_summaries_sha256"],
        "checkpoint_chain_sha256": row["checkpoint_chain_sha256"],
        "checkpoint_count": row["checkpoint_count"],
        "detail_record_count": row["expected_detail_record_count"],
        "detail_shard_manifest_sha256": row["detail_shard_manifest_sha256"],
        "final_checkpoint_sequence": row["final_checkpoint_sequence"],
        "final_checkpoint_sha256": row["final_checkpoint_sha256"],
        "input_lineage_sha256": row["input_lineage_sha256"],
        "outcome_config_id": OUTCOME_CONFIG_ID,
        "outcome_replay_manifest_id": row["predecessor_outcome_replay_manifest_id"],
        "query_id": P5_QUERY_ID,
        "research_run_attempt_id": row["subject_run_attempt_id"],
        "research_run_spec_id": row["subject_run_spec_id"],
        "result_artifact_byte_size": row["subject_result_byte_size"],
        "result_artifact_sha256": row["subject_result_sha256"],
        "run_fingerprint": row["subject_run_fingerprint"],
        "source_artifact_manifest_sha256": row["source_artifact_manifest_sha256"],
        "source_event_count": row["subject_source_event_count"],
        "status": "SUCCEEDED",
        "summary_row_count": row["expected_summary_count"],
    }
    audit_path = _path_from_file_uri(
        row.get("audit_artifact_uri"),
        label="p5 equivalence audit artifact",
    )
    held = _open_held_immutable_file(audit_path, data_root=data_root)
    try:
        _require_held_parent(
            held,
            data_root=data_root,
            expected_parent=_EQUIVALENCE_AUDIT_DIRECTORY,
            label="p5 equivalence audit artifact",
        )
        if (
            held.sha256 != row.get("audit_artifact_sha256")
            or held.byte_size != row.get("audit_artifact_byte_size")
            or held.path.as_uri() != row.get("audit_artifact_uri")
        ):
            raise OutcomeRegistryDriftError(
                "p1_05 predecessor audit bytes changed while being held"
            )
        _validate_equivalence_audit_payload(
            held,
            expected_subject=subject_payload,
        )
        _verify_held_file(held)
        return stored_gate, held
    except Exception:
        held.close()
        raise


@_translate_psycopg_errors("Phase 1A p5 equivalence-audit subject lookup")
def find_phase1a_p5_equivalence_audit_for_subject(
    database_url: str,
    *,
    predecessor_outcome_replay_manifest_id: int,
    data_root: Path | str,
) -> LoadedOutcomeEquivalenceAudit | None:
    """Find and byte-verify the singleton audit for one p5 replay subject."""

    target = _database_url(database_url)
    manifest_id = _positive_identifier(
        predecessor_outcome_replay_manifest_id,
        label="predecessor_outcome_replay_manifest_id",
    )
    with psycopg.connect(target, row_factory=dict_row) as connection:
        _set_serializable_read_only(connection)
        with connection.transaction():
            rows = connection.execute(
                """
                SELECT validation_research_run_attempt_id
                FROM systematic_fx.phase1a_outcome_replay_equivalence_audits
                WHERE predecessor_outcome_replay_manifest_id = %s
                  AND passed = true
                ORDER BY outcome_equivalence_audit_id
                """,
                (manifest_id,),
            ).fetchall()
    if not rows:
        return None
    if len(rows) != 1:
        raise OutcomeRegistryDriftError("p5 replay subject has multiple PASSED equivalence audits")
    loaded = load_phase1a_p5_equivalence_audit_for_attempt(
        target,
        validation_research_run_attempt_id=_positive_identifier(
            rows[0].get("validation_research_run_attempt_id"),
            label="validation_research_run_attempt_id",
        ),
        data_root=data_root,
    )
    if loaded.audit.predecessor_outcome_replay_manifest_id != manifest_id:
        raise OutcomeRegistryDriftError("equivalence audit subject lookup drift")
    return loaded


@dataclass(frozen=True, slots=True)
class _ValidatedCompletionLineage:
    document: dict[str, Any]
    detail_shards: tuple[dict[str, object], ...]
    detail_shard_manifest_sha256: str
    detail_record_count: int
    planned_source_dates: tuple[date, ...]
    cache_manifest_sha256: str
    cache_manifest_byte_size: int
    cache_plan_sha256: str
    input_manifest_sha256: str
    input_lineage: dict[str, object]
    input_lineage_sha256: str
    final_checkpoint_reference: dict[str, object]


def _iso_day(value: object, *, label: str) -> date:
    if not isinstance(value, str):
        raise OutcomeRegistryDriftError(f"{label} must be a canonical ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise OutcomeRegistryDriftError(f"{label} must be a canonical ISO date") from error
    if parsed.isoformat() != value:
        raise OutcomeRegistryDriftError(f"{label} must be a canonical ISO date")
    return parsed


def _relative_uri(
    value: object,
    *,
    label: str,
    expected_parent: PurePosixPath | None = None,
    expected_name: str | None = None,
) -> PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value:
        raise OutcomeRegistryDriftError(f"{label} must be a canonical relative URI")
    relative = PurePosixPath(value)
    if (
        relative.is_absolute()
        or relative.as_posix() != value
        or any(part in {"", ".", ".."} for part in relative.parts)
    ):
        raise OutcomeRegistryDriftError(f"{label} must be a canonical relative URI")
    if expected_parent is not None and relative.parent != expected_parent:
        raise OutcomeRegistryDriftError(f"{label} is outside its frozen artifact directory")
    if expected_name is not None and relative.name != expected_name:
        raise OutcomeRegistryDriftError(f"{label} filename differs from its content identity")
    return relative


def _verify_relative_artifact(
    relative_uri: object,
    *,
    data_root: Path | str,
    expected_parent: PurePosixPath,
    expected_sha256: object,
    expected_byte_size: object,
    suffix: str,
    label: str,
) -> Path:
    digest = _sha256(expected_sha256, label=f"{label} sha256")
    byte_size = _positive_identifier(expected_byte_size, label=f"{label} byte_size")
    relative = _relative_uri(
        relative_uri,
        label=f"{label} relative URI",
        expected_parent=expected_parent,
        expected_name=f"sha256={digest}{suffix}",
    )
    _, derived = _resolved_data_root(data_root)
    candidate = derived.joinpath(*relative.parts)
    held = _open_held_immutable_file(candidate, data_root=data_root)
    try:
        _require_held_parent(
            held,
            data_root=data_root,
            expected_parent=expected_parent,
            label=label,
        )
        if held.sha256 != digest or held.byte_size != byte_size:
            raise OutcomeRegistryDriftError(f"{label} content identity drift")
        _verify_held_file(held)
    finally:
        held.close()
    return candidate


def _validated_input_lineage(
    value: object,
    *,
    source_artifact_manifest_sha256: str,
    profile: OutcomeQueryProfile = P5_OUTCOME_QUERY_PROFILE,
    predecessor_gate: OutcomePredecessorGate | None = None,
) -> dict[str, object]:
    expected_fields = set(_INPUT_LINEAGE_FIELDS)
    if profile.query_id == P1_05_QUERY_ID:
        expected_fields.update(_PREDECESSOR_INPUT_LINEAGE_FIELDS)
    if not isinstance(value, Mapping) or set(value) != expected_fields:
        raise OutcomeRegistryDriftError("outcome input lineage schema drift")
    lineage = _canonical_mapping(value, label="outcome input lineage")
    for field_name in _INPUT_LINEAGE_SHA256_FIELDS:
        _sha256(lineage.get(field_name), label=f"input lineage {field_name}")
    if (
        lineage.get("expected_completed_source_date_count") != profile.planned_source_date_count
        or _iso_day(
            lineage.get("expected_last_completed_source_date"),
            label="input lineage expected_last_completed_source_date",
        )
        != profile.final_source_date
    ):
        raise OutcomeRegistryDriftError(
            "outcome input lineage must retain its frozen completion boundary"
        )
    if lineage["rich_source_artifact_manifest_sha256"] != source_artifact_manifest_sha256:
        raise OutcomeRegistryDriftError("outcome input lineage source-artifact drift")
    if profile.query_id == P1_05_QUERY_ID:
        if predecessor_gate is None:
            raise OutcomeRegistryStateError("p1_05 input lineage requires a predecessor gate")
        expected_predecessor = predecessor_gate.parameters
        mismatches = [
            field_name
            for field_name, expected in expected_predecessor.items()
            if lineage.get(field_name) != expected
        ]
        if mismatches:
            raise OutcomeRegistryDriftError(
                "p1_05 input predecessor lineage drift in fields: " + ", ".join(sorted(mismatches))
            )
    return lineage


def _validated_final_checkpoint_reference(
    value: object,
    *,
    profile: OutcomeQueryProfile = P5_OUTCOME_QUERY_PROFILE,
) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != _FINAL_CHECKPOINT_REFERENCE_FIELDS:
        raise OutcomeRegistryDriftError("final checkpoint reference schema drift")
    reference = _canonical_mapping(value, label="final checkpoint reference")
    digest = _sha256(reference.get("artifact_sha256"), label="final checkpoint reference sha256")
    _positive_identifier(reference.get("byte_size"), label="final checkpoint reference byte_size")
    _relative_uri(
        reference.get("artifact_relative_uri"),
        label="final checkpoint reference URI",
        expected_parent=profile.checkpoint_directory,
        expected_name=f"sha256={digest}.json",
    )
    if (
        reference.get("checkpoint_sequence") != profile.planned_source_date_count
        or _iso_day(
            reference.get("last_completed_source_date"),
            label="final checkpoint reference source date",
        )
        != profile.final_source_date
    ):
        raise OutcomeRegistryDriftError(
            "final checkpoint reference must identify the frozen terminal source date"
        )
    progress = _canonical_mapping(
        reference.get("progress_metadata"),
        label="final checkpoint reference progress metadata",
    )
    progress_sha256 = _sha256(
        reference.get("progress_metadata_sha256"),
        label="final checkpoint reference progress sha256",
    )
    if _canonical_sha256(progress) != progress_sha256:
        raise OutcomeRegistryDriftError("final checkpoint reference progress metadata hash drift")
    return reference


def _validated_cache_manifest(
    value: object,
    *,
    data_root: Path | str,
    profile: OutcomeQueryProfile = P5_OUTCOME_QUERY_PROFILE,
) -> tuple[str, int, str, str, tuple[date, ...]]:
    if not isinstance(value, Mapping) or set(value) != _CACHE_MANIFEST_REFERENCE_FIELDS:
        raise OutcomeRegistryDriftError("result cache-manifest reference schema drift")
    reference = _canonical_mapping(value, label="result cache-manifest reference")
    digest = _sha256(reference.get("artifact_sha256"), label="cache manifest sha256")
    byte_size = _positive_identifier(reference.get("byte_size"), label="cache manifest byte_size")
    path = _verify_relative_artifact(
        reference.get("artifact_relative_uri"),
        data_root=data_root,
        expected_parent=_CACHE_MANIFEST_DIRECTORY,
        expected_sha256=digest,
        expected_byte_size=byte_size,
        suffix=".json",
        label="cache manifest",
    )
    held = _open_held_immutable_file(path, data_root=data_root)
    try:
        if (held.sha256, held.byte_size) != (digest, byte_size):
            raise OutcomeRegistryDriftError("cache manifest file identity drift")
        document = _canonical_json_document(held.content, label="outcome cache manifest")
    finally:
        held.close()
    if set(document) != _CACHE_MANIFEST_FIELDS:
        raise OutcomeRegistryDriftError("outcome cache manifest field schema drift")
    expected_static = {
        "artifact_schema": _CACHE_MANIFEST_SCHEMA,
        "artifact_version": _CACHE_MANIFEST_VERSION,
        "cache_count": profile.cache_partition_count,
        "cache_schema": _CACHE_SCHEMA,
        "cache_version": _CACHE_VERSION,
        "partition_key": ["source_date", "raw_symbol"],
    }
    if any(document.get(key) != expected for key, expected in expected_static.items()):
        raise OutcomeRegistryDriftError("outcome cache manifest frozen identity drift")
    entries = document.get("cache_entries")
    if not isinstance(entries, list) or len(entries) != profile.cache_partition_count:
        raise OutcomeRegistryDriftError(
            "outcome cache manifest partition count differs from the frozen query plan"
        )
    entries_sha256 = _sha256(document.get("cache_entries_sha256"), label="cache entries sha256")
    if _canonical_sha256(entries) != entries_sha256:
        raise OutcomeRegistryDriftError("outcome cache entry manifest hash drift")
    if (
        reference.get("cache_count") != profile.cache_partition_count
        or reference.get("cache_entries_sha256") != entries_sha256
    ):
        raise OutcomeRegistryDriftError("result cache-manifest reference content drift")
    cache_plan_sha256 = _sha256(document.get("cache_plan_sha256"), label="cache plan sha256")
    input_manifest_sha256 = _sha256(
        document.get("input_manifest_sha256"), label="cache input manifest sha256"
    )
    if (
        reference.get("cache_plan_sha256") != cache_plan_sha256
        or reference.get("input_manifest_sha256") != input_manifest_sha256
    ):
        raise OutcomeRegistryDriftError("result cache-manifest plan/input binding drift")

    keys: list[tuple[date, str]] = []
    for index, raw_entry in enumerate(entries):
        if not isinstance(raw_entry, Mapping) or set(raw_entry) != _CACHE_ENTRY_FIELDS:
            raise OutcomeRegistryDriftError(f"cache entry {index} field schema drift")
        entry = _canonical_mapping(raw_entry, label=f"cache entry {index}")
        source_date = _iso_day(entry.get("source_date"), label=f"cache entry {index} date")
        raw_symbol = _nonempty(entry.get("raw_symbol"), label=f"cache entry {index} symbol")
        artifact_sha256 = _sha256(
            entry.get("artifact_sha256"), label=f"cache entry {index} artifact sha256"
        )
        _relative_uri(
            entry.get("artifact_relative_uri"),
            label=f"cache entry {index} artifact URI",
            expected_parent=PurePosixPath("backtest_event_cache") / _CACHE_VERSION,
            expected_name=f"sha256={artifact_sha256}.parquet",
        )
        _relative_uri(entry.get("source_relative_uri"), label=f"cache entry {index} source URI")
        _sha256(entry.get("source_sha256"), label=f"cache entry {index} source sha256")
        for name in (
            "byte_size",
            "cached_quote_count",
            "instrument_id",
            "source_row_count",
        ):
            _positive_identifier(entry.get(name), label=f"cache entry {index} {name}")
        for name in (
            "event_index_offset",
            "first_event_index",
            "first_ts_recv_ns",
            "last_event_index",
            "last_ts_recv_ns",
            "valid_quote_count",
        ):
            _nonnegative_integer(entry.get(name), label=f"cache entry {index} {name}")
        keys.append((source_date, raw_symbol))
    if tuple(keys) != tuple(sorted(set(keys))):
        raise OutcomeRegistryDriftError("cache date/contract keys are duplicated or unordered")
    planned_source_dates = tuple(sorted({source_date for source_date, _ in keys}))
    if (
        len(planned_source_dates) != profile.planned_source_date_count
        or planned_source_dates[-1] != profile.final_source_date
    ):
        raise OutcomeRegistryDriftError(
            "cache manifest must cover the frozen query source-date boundary"
        )
    return (
        digest,
        byte_size,
        cache_plan_sha256,
        input_manifest_sha256,
        planned_source_dates,
    )


def _validated_detail_shards(
    value: object,
    *,
    data_root: Path | str,
    run_fingerprint: str,
    profile: OutcomeQueryProfile = P5_OUTCOME_QUERY_PROFILE,
) -> tuple[tuple[dict[str, object], ...], str, int]:
    if not isinstance(value, list) or not value:
        raise OutcomeRegistryDriftError("result detail_shards must be a non-empty list")
    shards: list[dict[str, object]] = []
    prior_date: date | None = None
    record_count = 0
    seen_sha256: set[str] = set()
    for sequence, raw_shard in enumerate(value, start=1):
        if not isinstance(raw_shard, Mapping) or set(raw_shard) != _DETAIL_SHARD_FIELDS:
            raise OutcomeRegistryDriftError(f"detail shard {sequence} field schema drift")
        shard = _canonical_mapping(raw_shard, label=f"detail shard {sequence}")
        if shard.get("shard_sequence") != sequence:
            raise OutcomeRegistryDriftError("detail shard sequence must be contiguous from one")
        if shard.get("run_fingerprint") != run_fingerprint:
            raise OutcomeRegistryDriftError("detail shard run fingerprint drift")
        source_date = _iso_day(
            shard.get("source_date"), label=f"detail shard {sequence} source_date"
        )
        if prior_date is not None and source_date <= prior_date:
            raise OutcomeRegistryDriftError("detail shard dates must be strictly increasing")
        prior_date = source_date
        digest = _sha256(shard.get("artifact_sha256"), label=f"detail shard {sequence} sha256")
        if digest in seen_sha256:
            raise OutcomeRegistryDriftError("detail shard content identity is repeated")
        seen_sha256.add(digest)
        _sha256(
            shard.get("record_manifest_sha256"),
            label=f"detail shard {sequence} record manifest sha256",
        )
        rows = _nonnegative_integer(
            shard.get("row_count"), label=f"detail shard {sequence} row_count"
        )
        record_count += rows
        _verify_relative_artifact(
            shard.get("artifact_relative_uri"),
            data_root=data_root,
            expected_parent=profile.detail_shard_directory,
            expected_sha256=digest,
            expected_byte_size=shard.get("byte_size"),
            suffix=".parquet",
            label=f"detail shard {sequence}",
        )
        shards.append(shard)
    if record_count != profile.detail_record_count:
        raise OutcomeRegistryDriftError(
            f"outcome replay must retain exactly {profile.detail_record_count} detail rows"
        )
    manifest_sha256 = _canonical_sha256(shards)
    return tuple(shards), manifest_sha256, record_count


def _validate_result_artifact(
    held: _HeldFile,
    *,
    run_fingerprint: str,
    source_artifact_manifest_sha256: str,
    cell_summaries_sha256: str,
    cell_summaries: Sequence[OutcomeCellSummary],
    data_root: Path | str,
    profile: OutcomeQueryProfile = P5_OUTCOME_QUERY_PROFILE,
    predecessor_gate: OutcomePredecessorGate | None = None,
) -> _ValidatedCompletionLineage:
    if held.path.name != f"sha256={held.sha256}.json":
        raise OutcomeRegistryError("result artifact filename must be sha256=<content>.json")
    document = _canonical_json_document(held.content, label="outcome result artifact")
    if set(document) != _FINAL_RESULT_FIELDS:
        raise OutcomeRegistryDriftError("outcome result artifact field schema drift")
    expected = {
        "artifact_schema": profile.outcome_artifact_schema,
        "cell_summaries_sha256": cell_summaries_sha256,
        "direction_ids": list(DIRECTION_IDS),
        "outcome_config_id": profile.outcome_config_id,
        "query_id": profile.query_id,
        "run_fingerprint": run_fingerprint,
        "scenario_ids": list(SCENARIO_IDS),
        "source_artifact_manifest_sha256": source_artifact_manifest_sha256,
        "source_occurrence_count": profile.source_occurrence_count,
        "source_slice_count": profile.source_slice_count,
        "summary_row_count": EXPECTED_SUMMARY_COUNT,
        "detail_record_count": profile.detail_record_count,
    }
    mismatches = [key for key, value in expected.items() if document.get(key) != value]
    if mismatches:
        raise OutcomeRegistryDriftError(
            "outcome result artifact drift in fields: " + ", ".join(sorted(mismatches))
        )
    expected_cells = [cell.payload for cell in cell_summaries]
    if document.get("cell_summaries") != expected_cells:
        raise OutcomeRegistryDriftError("outcome result cell summaries differ from DB input")
    if _canonical_sha256(expected_cells) != cell_summaries_sha256:
        raise OutcomeRegistryDriftError("outcome result cell summary semantic hash drift")

    detail_shards, detail_manifest_sha256, detail_record_count = _validated_detail_shards(
        document.get("detail_shards"),
        data_root=data_root,
        run_fingerprint=run_fingerprint,
        profile=profile,
    )
    if (
        document.get("detail_shard_count") != len(detail_shards)
        or document.get("detail_shard_manifest_sha256") != detail_manifest_sha256
    ):
        raise OutcomeRegistryDriftError("outcome result detail-shard lineage drift")
    (
        cache_manifest_sha256,
        cache_manifest_byte_size,
        cache_plan_sha256,
        input_manifest_sha256,
        planned_source_dates,
    ) = _validated_cache_manifest(
        document.get("cache_manifest"),
        data_root=data_root,
        profile=profile,
    )
    shard_dates = tuple(
        _iso_day(shard["source_date"], label="detail shard source_date") for shard in detail_shards
    )
    if shard_dates != planned_source_dates:
        raise OutcomeRegistryDriftError(
            "detail shard dates differ from the cache-manifest source-date plan"
        )
    input_lineage = _validated_input_lineage(
        document.get("input_lineage"),
        source_artifact_manifest_sha256=source_artifact_manifest_sha256,
        profile=profile,
        predecessor_gate=predecessor_gate,
    )
    input_lineage_sha256 = _sha256(
        document.get("input_lineage_sha256"), label="result input lineage sha256"
    )
    if _canonical_sha256(input_lineage) != input_lineage_sha256:
        raise OutcomeRegistryDriftError("result input-lineage semantic hash drift")
    if (
        input_lineage.get("cache_plan_sha256") != cache_plan_sha256
        or input_lineage.get("discovery_input_manifest_sha256") != input_manifest_sha256
    ):
        raise OutcomeRegistryDriftError("result cache/input lineage binding drift")
    final_checkpoint_reference = _validated_final_checkpoint_reference(
        document.get("final_checkpoint"),
        profile=profile,
    )
    return _ValidatedCompletionLineage(
        document=document,
        detail_shards=detail_shards,
        detail_shard_manifest_sha256=detail_manifest_sha256,
        detail_record_count=detail_record_count,
        planned_source_dates=planned_source_dates,
        cache_manifest_sha256=cache_manifest_sha256,
        cache_manifest_byte_size=cache_manifest_byte_size,
        cache_plan_sha256=cache_plan_sha256,
        input_manifest_sha256=input_manifest_sha256,
        input_lineage=input_lineage,
        input_lineage_sha256=input_lineage_sha256,
        final_checkpoint_reference=final_checkpoint_reference,
    )


def _validate_run_spec_completion_lineage(
    manifest: Mapping[str, Any],
    lineage: _ValidatedCompletionLineage,
    *,
    profile: OutcomeQueryProfile = P5_OUTCOME_QUERY_PROFILE,
    predecessor_gate: OutcomePredecessorGate | None = None,
) -> None:
    canonical_spec = manifest.get("run_spec_canonical_spec")
    if not isinstance(canonical_spec, Mapping):
        raise OutcomeRegistryDriftError("outcome RunSpec canonical payload is missing")
    parameters = canonical_spec.get("parameters")
    if not isinstance(parameters, Mapping):
        raise OutcomeRegistryDriftError("outcome RunSpec parameters are missing")
    expected = {
        "cache_manifest_sha256": lineage.cache_manifest_sha256,
        "cache_partition_count": profile.cache_partition_count,
        "expected_detail_record_count": profile.detail_record_count,
        "expected_completed_source_date_count": profile.planned_source_date_count,
        "expected_last_completed_source_date": profile.final_source_date.isoformat(),
        "final_source_date": profile.final_source_date.isoformat(),
        "input_plan_sha256": lineage.input_lineage["input_plan_sha256"],
        "planned_source_date_count": profile.planned_source_date_count,
        "portable_discovery_artifact_manifest_sha256": lineage.input_lineage[
            "portable_artifact_manifest_sha256"
        ],
        "portable_discovery_input_manifest_sha256": lineage.input_manifest_sha256,
        "portable_signal_manifest_sha256": lineage.input_lineage["signal_manifest_sha256"],
        "source_record_manifest_sha256": lineage.input_lineage["source_record_manifest_sha256"],
        "terminal_resolution_sha256": lineage.input_lineage["terminal_resolution_sha256"],
    }
    if profile.query_id == P1_05_QUERY_ID:
        if predecessor_gate is None:
            raise OutcomeRegistryStateError("p1_05 completion requires predecessor proof")
        expected.update(predecessor_gate.parameters)
    mismatches = [key for key, value in expected.items() if parameters.get(key) != value]
    if mismatches:
        raise OutcomeRegistryDriftError(
            "outcome RunSpec completion lineage drift in fields: " + ", ".join(sorted(mismatches))
        )
    terminal_resolution = _canonical_mapping(
        parameters.get("terminal_resolution"),
        label="outcome RunSpec terminal resolution",
    )
    if (
        _canonical_sha256(terminal_resolution)
        != lineage.input_lineage["terminal_resolution_sha256"]
        or terminal_resolution.get("terminal_exit_policy") != _TERMINAL_EXIT_POLICY
        or terminal_resolution.get("partition_resolution_policy")
        != _TERMINAL_PARTITION_RESOLUTION_POLICY
    ):
        raise OutcomeRegistryDriftError("outcome RunSpec terminal-resolution lineage drift")
    terminal_policy = _canonical_mapping(
        canonical_spec.get("terminal_policy"),
        label="outcome RunSpec terminal policy",
    )
    if (
        terminal_policy.get("terminal_exit") != _TERMINAL_EXIT_POLICY
        or terminal_policy.get("terminal_partition_resolution")
        != _TERMINAL_PARTITION_RESOLUTION_POLICY
        or terminal_policy.get("terminal_resolution_sha256")
        != lineage.input_lineage["terminal_resolution_sha256"]
    ):
        raise OutcomeRegistryDriftError("outcome RunSpec terminal policy drift")


def _validate_final_checkpoint(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    manifest_id: int,
    run_fingerprint: str,
    lineage: _ValidatedCompletionLineage,
    data_root: Path | str,
    profile: OutcomeQueryProfile = P5_OUTCOME_QUERY_PROFILE,
) -> tuple[str, int]:
    rows = connection.execute(
        """
        SELECT checkpoint.*,
               artifact.artifact_id AS stored_artifact_id,
               artifact.uri AS artifact_uri,
               artifact.sha256 AS artifact_sha256,
               artifact.byte_size AS artifact_byte_size,
               artifact.artifact_type,
               artifact.media_type,
               artifact.producer_job_id,
               artifact.metadata AS artifact_metadata
        FROM systematic_fx.phase1a_outcome_replay_checkpoints AS checkpoint
        JOIN systematic_fx.artifacts AS artifact
          ON artifact.artifact_id = checkpoint.checkpoint_artifact_id
        WHERE checkpoint.outcome_replay_manifest_id = %s
        ORDER BY checkpoint.checkpoint_sequence
        FOR SHARE OF checkpoint, artifact
        """,
        (manifest_id,),
    ).fetchall()
    planned_count = len(lineage.planned_source_dates)
    if not rows or len(rows) != planned_count:
        raise OutcomeRegistryStateError(
            "outcome completion requires one checkpoint per planned source date"
        )
    for expected_sequence, (expected_date, row) in enumerate(
        zip(lineage.planned_source_dates, rows, strict=True),
        start=1,
    ):
        if (
            int(row["checkpoint_sequence"]) != expected_sequence
            or int(row["completed_source_date_count"]) != expected_sequence
            or row["last_completed_source_date"] != expected_date
        ):
            raise OutcomeRegistryDriftError(
                "checkpoint sequence/date chain differs from the planned cache dates"
            )

    latest = rows[-1]
    latest_sha256 = _sha256(
        latest.get("checkpoint_artifact_sha256"), label="final checkpoint sha256"
    )
    latest_byte_size = _positive_identifier(
        latest.get("checkpoint_artifact_byte_size"),
        label="final checkpoint byte_size",
    )
    if (
        latest.get("stored_artifact_id") != latest.get("checkpoint_artifact_id")
        or latest.get("artifact_sha256") != latest_sha256
        or latest.get("artifact_byte_size") != latest_byte_size
        or latest.get("artifact_type") != CHECKPOINT_ARTIFACT_TYPE
        or latest.get("media_type") != "application/json"
        or latest.get("producer_job_id") is not None
    ):
        raise OutcomeRegistryDriftError("final checkpoint registry artifact drift")
    progress = _canonical_mapping(
        latest.get("progress_metadata"), label="final checkpoint progress metadata"
    )
    progress_sha256 = _canonical_sha256(progress)
    checkpoint_path = _path_from_file_uri(
        latest.get("artifact_uri"), label="final checkpoint artifact"
    )
    reference = lineage.final_checkpoint_reference
    _, derived = _resolved_data_root(data_root)
    reference_relative = _relative_uri(
        reference.get("artifact_relative_uri"),
        label="final checkpoint reference URI",
        expected_parent=profile.checkpoint_directory,
        expected_name=f"sha256={latest_sha256}.json",
    )
    expected_reference = {
        "artifact_relative_uri": reference_relative.as_posix(),
        "artifact_sha256": latest_sha256,
        "byte_size": latest_byte_size,
        "checkpoint_sequence": planned_count,
        "last_completed_source_date": lineage.planned_source_dates[-1].isoformat(),
        "progress_metadata": progress,
        "progress_metadata_sha256": progress_sha256,
    }
    expected_artifact_metadata = _checkpoint_artifact_metadata(
        outcome_replay_manifest_id=manifest_id,
        run_fingerprint=run_fingerprint,
        checkpoint_sequence=planned_count,
        last_completed_source_date=lineage.planned_source_dates[-1],
        profile=profile,
    )
    if (
        reference != expected_reference
        or checkpoint_path != derived.joinpath(*reference_relative.parts)
        or latest.get("artifact_metadata") != expected_artifact_metadata
    ):
        raise OutcomeRegistryDriftError(
            "final result checkpoint reference differs from the latest DB checkpoint"
        )
    held = _open_held_immutable_file(checkpoint_path, data_root=data_root)
    try:
        _require_held_parent(
            held,
            data_root=data_root,
            expected_parent=profile.checkpoint_directory,
            label="final checkpoint artifact",
        )
        if (held.sha256, held.byte_size) != (latest_sha256, latest_byte_size):
            raise OutcomeRegistryDriftError("final checkpoint file identity drift")
        document = _validate_checkpoint_artifact(
            held,
            outcome_replay_manifest_id=manifest_id,
            run_fingerprint=run_fingerprint,
            checkpoint_sequence=planned_count,
            completed_source_date_count=planned_count,
            last_completed_source_date=lineage.planned_source_dates[-1],
            source_event_count=int(latest["source_event_count"]),
            predecessor_checkpoint_sha256=latest.get("predecessor_checkpoint_sha256"),
            progress_metadata_sha256=progress_sha256,
            profile=profile,
        )
        if set(document) != _CHECKPOINT_FIELDS:
            raise OutcomeRegistryDriftError("final checkpoint field schema drift")
        if document.get("progress_metadata") != progress:
            raise OutcomeRegistryDriftError("final checkpoint progress metadata drift")
        if document.get("detail_shards") != list(lineage.detail_shards):
            raise OutcomeRegistryDriftError("final checkpoint detail-shard lineage drift")
        if (
            document.get("detail_record_count") != lineage.detail_record_count
            or document.get("detail_shard_manifest_sha256") != lineage.detail_shard_manifest_sha256
            or document.get("cache_manifest") != lineage.document.get("cache_manifest")
            or document.get("input_lineage") != lineage.input_lineage
            or document.get("input_lineage_sha256") != lineage.input_lineage_sha256
        ):
            raise OutcomeRegistryDriftError("final checkpoint/result lineage binding drift")
        replay_state = document.get("replay_state")
        if not isinstance(replay_state, Mapping):
            raise OutcomeRegistryDriftError("final checkpoint replay_state is missing")
        replay_state_sha256 = _sha256(
            document.get("replay_state_sha256"), label="final replay state sha256"
        )
        if _canonical_sha256(replay_state) != replay_state_sha256:
            raise OutcomeRegistryDriftError("final replay-state semantic hash drift")
        expected_empty_fields = (
            "buffer",
            "occupancy",
            "pending_entries",
            "position_groups",
            "records",
        )
        if (
            replay_state.get("finished") is not True
            or replay_state.get("completed_source_date")
            != lineage.planned_source_dates[-1].isoformat()
            or replay_state.get("source_event_count") != int(latest["source_event_count"])
            or replay_state.get("result_record_count") != profile.detail_record_count
            or replay_state.get("drained_record_count") != profile.detail_record_count
            or replay_state.get("signal_cursor") != profile.source_occurrence_count
            or not isinstance(replay_state.get("signals"), list)
            or len(replay_state["signals"]) != profile.source_occurrence_count
            or any(replay_state.get(name) != [] for name in expected_empty_fields)
        ):
            raise OutcomeRegistryDriftError(
                "final checkpoint is not a fully drained finished replay"
            )
        expected_progress = {
            "artifact_schema": _CHECKPOINT_PROGRESS_SCHEMA,
            "cache_manifest_sha256": lineage.cache_manifest_sha256,
            "detail_record_count": profile.detail_record_count,
            "detail_shard_count": planned_count,
            "detail_shard_manifest_sha256": lineage.detail_shard_manifest_sha256,
            "input_lineage_sha256": lineage.input_lineage_sha256,
            "replay_finished": True,
            "replay_state_sha256": replay_state_sha256,
            "source_event_count": int(latest["source_event_count"]),
        }
        if progress != expected_progress:
            raise OutcomeRegistryDriftError("final checkpoint progress summary drift")
        _verify_held_file(held)
    finally:
        held.close()
    return latest_sha256, planned_count


def _cell_insert_parameters(
    manifest_id: int,
    run_fingerprint: str,
    cell: OutcomeCellSummary,
) -> tuple[object, ...]:
    return (
        manifest_id,
        run_fingerprint,
        cell.scenario_id,
        cell.direction,
        cell.take_profit_ticks,
        cell.stop_loss_ticks,
        cell.signal_count,
        cell.entry_fill_count,
        cell.entry_not_filled_count,
        cell.skipped_occupied_count,
        cell.take_profit_first_count,
        cell.stop_first_count,
        cell.terminal_exit_count,
        cell.censored_count,
        cell.gross_pnl_ticks,
        cell.variable_cost_ticks,
        cell.allocated_fixed_cost_ticks,
        cell.fully_loaded_net_pnl_ticks,
        cell.fully_loaded_net_ev_ticks,
        cell.fully_loaded_net_pnl_usd,
        cell.calendar_month_net_pnl_usd,
        cell.profit_factor,
        cell.maximum_drawdown_usd,
        cell.complete,
        cell.summary_sha256,
    )


def _cell_matches_row(cell: OutcomeCellSummary, row: Mapping[str, Any]) -> bool:
    expected = {
        "allocated_fixed_cost_ticks": cell.allocated_fixed_cost_ticks,
        "calendar_month_net_pnl_usd": cell.calendar_month_net_pnl_usd,
        "censored_count": cell.censored_count,
        "complete": cell.complete,
        "direction": cell.direction,
        "entry_fill_count": cell.entry_fill_count,
        "entry_not_filled_count": cell.entry_not_filled_count,
        "fully_loaded_net_ev_ticks": cell.fully_loaded_net_ev_ticks,
        "fully_loaded_net_pnl_ticks": cell.fully_loaded_net_pnl_ticks,
        "fully_loaded_net_pnl_usd": cell.fully_loaded_net_pnl_usd,
        "gross_pnl_ticks": cell.gross_pnl_ticks,
        "maximum_drawdown_usd": cell.maximum_drawdown_usd,
        "profit_factor": cell.profit_factor,
        "scenario_id": cell.scenario_id,
        "signal_count": cell.signal_count,
        "skipped_occupied_count": cell.skipped_occupied_count,
        "stop_first_count": cell.stop_first_count,
        "stop_loss_ticks": cell.stop_loss_ticks,
        "summary_sha256": cell.summary_sha256,
        "take_profit_first_count": cell.take_profit_first_count,
        "take_profit_ticks": cell.take_profit_ticks,
        "terminal_exit_count": cell.terminal_exit_count,
        "variable_cost_ticks": cell.variable_cost_ticks,
    }
    return all(row.get(key) == value for key, value in expected.items())


def _register_cells(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    manifest_id: int,
    run_fingerprint: str,
    cells: tuple[OutcomeCellSummary, ...],
) -> None:
    existing_rows = connection.execute(
        """
        SELECT *
        FROM systematic_fx.phase1a_outcome_cell_summaries
        WHERE outcome_replay_manifest_id = %s
        ORDER BY scenario_id, direction, take_profit_ticks, stop_loss_ticks
        FOR SHARE
        """,
        (manifest_id,),
    ).fetchall()
    existing = {
        (
            str(row["scenario_id"]),
            str(row["direction"]),
            int(row["take_profit_ticks"]),
            int(row["stop_loss_ticks"]),
        ): row
        for row in existing_rows
    }
    expected_identities = {cell.identity for cell in cells}
    if not set(existing) <= expected_identities:
        raise OutcomeRegistryDriftError("outcome replay has unknown stored cell summaries")
    missing: list[OutcomeCellSummary] = []
    for cell in cells:
        row = existing.get(cell.identity)
        if row is None:
            missing.append(cell)
        elif not _cell_matches_row(cell, row):
            raise OutcomeRegistryDriftError(f"stored outcome cell summary drift: {cell.identity}")
    if not missing:
        return
    sql = """
        INSERT INTO systematic_fx.phase1a_outcome_cell_summaries
            (outcome_replay_manifest_id, run_fingerprint, scenario_id, direction,
             take_profit_ticks, stop_loss_ticks, signal_count, entry_fill_count,
             entry_not_filled_count, skipped_occupied_count,
             take_profit_first_count, stop_first_count, terminal_exit_count,
             censored_count, gross_pnl_ticks, variable_cost_ticks,
             allocated_fixed_cost_ticks, fully_loaded_net_pnl_ticks,
             fully_loaded_net_ev_ticks, fully_loaded_net_pnl_usd,
             calendar_month_net_pnl_usd, profit_factor, maximum_drawdown_usd,
             complete, summary_sha256)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with connection.cursor() as cursor:
        cursor.executemany(
            sql,
            [_cell_insert_parameters(manifest_id, run_fingerprint, cell) for cell in missing],
        )


def derive_phase1a_outcome_screening_decisions(
    cells: Sequence[OutcomeCellSummary],
    *,
    query_id: str = P5_QUERY_ID,
) -> tuple[OutcomeScreeningDecision, ...]:
    """Apply the one frozen adjacent-stability selector to both directions."""

    ordered, _ = validate_complete_cell_summaries(cells, query_id=query_id)

    def economic_cell(cell: OutcomeCellSummary) -> CellEconomics:
        values = {
            field_name: getattr(cell, field_name)
            for field_name in OutcomeCellSummary.__dataclass_fields__
            if field_name != "direction"
        }
        return CellEconomics(
            cell_id=(
                f"{cell.scenario_id}:{cell.direction}:"
                f"tp{cell.take_profit_ticks}:sl{cell.stop_loss_ticks}"
            ),
            direction=Direction(cell.direction),
            **values,
        )

    decisions: list[OutcomeScreeningDecision] = []
    for direction_id in DIRECTION_IDS:
        direction = Direction(direction_id)
        surfaces: dict[str, EconomicSurface] = {}
        for scenario_id in ("BASELINE", "MODERATE_COMBINED"):
            surface_cells = tuple(
                economic_cell(cell)
                for cell in ordered
                if cell.scenario_id == scenario_id and cell.direction == direction_id
            )
            surfaces[scenario_id] = EconomicSurface(
                scenario_id=scenario_id,
                direction=direction,
                cells=surface_cells,
            )
        selection = select_stable_screening_cell(
            surfaces["BASELINE"],
            surfaces["MODERATE_COMBINED"],
        )
        decisions.append(
            OutcomeScreeningDecision(
                direction=direction_id,
                decision_label=selection.label,
                selected_take_profit_ticks=selection.selected_take_profit_ticks,
                selected_stop_loss_ticks=selection.selected_stop_loss_ticks,
                positive_region_size=selection.positive_region_size,
                rejection_reasons=selection.rejection_reasons,
            )
        )
    return tuple(decisions)


def _register_screening_decisions(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    manifest_id: int,
    decisions: Sequence[OutcomeScreeningDecision],
) -> None:
    existing_rows = connection.execute(
        """
        SELECT *
        FROM systematic_fx.phase1a_outcome_screening_decisions
        WHERE outcome_replay_manifest_id = %s
        ORDER BY direction
        FOR SHARE
        """,
        (manifest_id,),
    ).fetchall()
    existing = {str(row["direction"]): row for row in existing_rows}
    for decision in decisions:
        payload = decision.payload(outcome_replay_manifest_id=manifest_id)
        decision_sha256 = _canonical_sha256(payload)
        row = existing.get(decision.direction)
        expected = {
            **payload,
            "decision_sha256": decision_sha256,
            "rejection_reasons": list(decision.rejection_reasons),
        }
        if row is not None:
            mismatches = [key for key, value in expected.items() if row.get(key) != value]
            if mismatches:
                raise OutcomeRegistryDriftError(
                    "stored screening decision drift in fields: " + ", ".join(sorted(mismatches))
                )
            continue
        connection.execute(
            """
            INSERT INTO systematic_fx.phase1a_outcome_screening_decisions
                (outcome_replay_manifest_id, direction, decision_label,
                 selected_take_profit_ticks, selected_stop_loss_ticks,
                 positive_region_size, rejection_reasons, decision_sha256)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                manifest_id,
                decision.direction,
                decision.decision_label,
                decision.selected_take_profit_ticks,
                decision.selected_stop_loss_ticks,
                decision.positive_region_size,
                Jsonb(list(decision.rejection_reasons)),
                decision_sha256,
            ),
        )


def _ensure_result_artifact(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    run_fingerprint: str,
    source_artifact_manifest_sha256: str,
    cells_sha256: str,
    lineage: _ValidatedCompletionLineage,
    final_checkpoint_sha256: str,
    held: _HeldFile,
    profile: OutcomeQueryProfile = P5_OUTCOME_QUERY_PROFILE,
    predecessor_gate: OutcomePredecessorGate | None = None,
) -> tuple[int, bool, dict[str, object]]:
    artifact_key = f"{CAMPAIGN_KEY}:outcome-replay:{run_fingerprint}"
    artifact_uri = held.path.as_uri()
    metadata: dict[str, object] = {
        "campaign_key": CAMPAIGN_KEY,
        "cache_manifest_sha256": lineage.cache_manifest_sha256,
        "cell_summaries_sha256": cells_sha256,
        "detail_record_count": lineage.detail_record_count,
        "detail_shard_count": len(lineage.detail_shards),
        "detail_shard_manifest_sha256": lineage.detail_shard_manifest_sha256,
        "direction_count": len(DIRECTION_IDS),
        "final_checkpoint_sha256": final_checkpoint_sha256,
        "input_lineage_sha256": lineage.input_lineage_sha256,
        "outcome_config_id": profile.outcome_config_id,
        "planned_source_date_count": len(lineage.planned_source_dates),
        "query_id": profile.query_id,
        "run_fingerprint": run_fingerprint,
        "scenario_count": len(SCENARIO_IDS),
        "source_artifact_manifest_sha256": source_artifact_manifest_sha256,
        "summary_row_count": EXPECTED_SUMMARY_COUNT,
    }
    if profile.query_id == P1_05_QUERY_ID:
        if predecessor_gate is None:
            raise OutcomeRegistryStateError("p1_05 result requires predecessor proof")
        metadata.update(predecessor_gate.parameters)
    rows = connection.execute(
        """
        SELECT artifact_id, artifact_key, artifact_type, uri, sha256, byte_size,
               media_type, producer_job_id, metadata
        FROM systematic_fx.artifacts
        WHERE artifact_key = %s OR uri = %s
        FOR UPDATE
        """,
        (artifact_key, artifact_uri),
    ).fetchall()
    if not rows:
        row = connection.execute(
            """
            INSERT INTO systematic_fx.artifacts
                (artifact_key, artifact_type, uri, sha256, byte_size,
                 media_type, metadata)
            VALUES (%s, %s, %s, %s, %s, 'application/json', %s)
            RETURNING artifact_id
            """,
            (
                artifact_key,
                OUTCOME_ARTIFACT_TYPE,
                artifact_uri,
                held.sha256,
                held.byte_size,
                Jsonb(metadata),
            ),
        ).fetchone()
        if row is None:  # pragma: no cover - RETURNING is mandatory
            raise OutcomeRegistryDatabaseError("result artifact returned no identity")
        return int(row["artifact_id"]), True, metadata
    if len(rows) != 1:
        raise OutcomeRegistryDriftError("result artifact key and URI identify different rows")
    row = rows[0]
    expected = {
        "artifact_key": artifact_key,
        "artifact_type": OUTCOME_ARTIFACT_TYPE,
        "byte_size": held.byte_size,
        "media_type": "application/json",
        "metadata": metadata,
        "producer_job_id": None,
        "sha256": held.sha256,
        "uri": artifact_uri,
    }
    mismatches = [key for key, value in expected.items() if row.get(key) != value]
    if mismatches:
        raise OutcomeRegistryDriftError(
            "result artifact immutable content drift in fields: " + ", ".join(sorted(mismatches))
        )
    return int(row["artifact_id"]), False, metadata


def _verify_completed_cells(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    manifest_id: int,
    cells: tuple[OutcomeCellSummary, ...],
) -> None:
    rows = connection.execute(
        """
        SELECT *
        FROM systematic_fx.phase1a_outcome_cell_summaries
        WHERE outcome_replay_manifest_id = %s
        """,
        (manifest_id,),
    ).fetchall()
    if len(rows) != EXPECTED_SUMMARY_COUNT:
        raise OutcomeRegistryDriftError("successful outcome replay cell count drift")
    by_identity = {
        (
            str(row["scenario_id"]),
            str(row["direction"]),
            int(row["take_profit_ticks"]),
            int(row["stop_loss_ticks"]),
        ): row
        for row in rows
    }
    if any(
        cell.identity not in by_identity or not _cell_matches_row(cell, by_identity[cell.identity])
        for cell in cells
    ):
        raise OutcomeRegistryDriftError("successful outcome replay cell content drift")


@_translate_psycopg_errors("atomic Phase 1A outcome replay completion")
def complete_phase1a_outcome_replay(
    database_url: str,
    *,
    outcome_replay_manifest_id: int,
    run_fingerprint: str,
    cell_summaries: Sequence[OutcomeCellSummary],
    result_artifact_path: Path,
    data_root: Path | str,
    query_id: str = P5_QUERY_ID,
) -> OutcomeCompletionReport:
    """Atomically publish all cells, one immutable artifact, and one success."""

    target = _database_url(database_url)
    profile = outcome_query_profile(query_id)
    manifest_id = _positive_identifier(
        outcome_replay_manifest_id,
        label="outcome_replay_manifest_id",
    )
    fingerprint = _sha256(run_fingerprint, label="run_fingerprint")
    cells, cells_sha256 = validate_complete_cell_summaries(
        cell_summaries,
        query_id=profile.query_id,
    )
    screening_decisions = derive_phase1a_outcome_screening_decisions(
        cells,
        query_id=profile.query_id,
    )
    held = _open_held_immutable_file(Path(result_artifact_path), data_root=data_root)
    try:
        _require_held_parent(
            held,
            data_root=data_root,
            expected_parent=PurePosixPath("outcomes") / profile.outcome_config_id,
            label="outcome result artifact",
        )
        with (
            psycopg.connect(target, row_factory=dict_row) as connection,
            ExitStack() as held_files,
        ):
            _set_serializable(connection)
            with connection.transaction():
                audit_evidence: _HeldFile | None = None
                manifest = _load_manifest_for_update(
                    connection,
                    outcome_replay_manifest_id=manifest_id,
                )
                _assert_live_manifest(manifest, run_fingerprint=fingerprint)
                if manifest["pattern_key"] != profile.query_id:
                    raise OutcomeRegistryDriftError("result query identity drift")
                predecessor_gate = None
                if profile.query_id == P1_05_QUERY_ID:
                    run_spec = manifest.get("run_spec_canonical_spec")
                    parameters = (
                        run_spec.get("parameters") if isinstance(run_spec, Mapping) else None
                    )
                    if not isinstance(parameters, Mapping):
                        raise OutcomeRegistryDriftError(
                            "p1_05 RunSpec predecessor parameters are missing"
                        )
                    audit_id = _positive_identifier(
                        parameters.get("predecessor_equivalence_audit_id"),
                        label="predecessor_equivalence_audit_id",
                    )
                    fully_verified_gate = load_phase1a_p1_predecessor_gate(
                        target,
                        data_root=data_root,
                        equivalence_audit_id=audit_id,
                    )
                    predecessor_gate, audit_evidence = _hold_phase1a_p1_predecessor_evidence(
                        target,
                        connection,
                        equivalence_audit_id=audit_id,
                        data_root=data_root,
                    )
                    held_files.callback(audit_evidence.close)
                    if predecessor_gate != fully_verified_gate:
                        raise OutcomeRegistryDriftError(
                            "p1_05 predecessor audit changed during completion"
                        )
                source_sha256 = _sha256(
                    manifest.get("source_artifact_manifest_sha256"),
                    label="source_artifact_manifest_sha256",
                )
                lineage = _validate_result_artifact(
                    held,
                    run_fingerprint=fingerprint,
                    source_artifact_manifest_sha256=source_sha256,
                    cell_summaries_sha256=cells_sha256,
                    cell_summaries=cells,
                    data_root=data_root,
                    profile=profile,
                    predecessor_gate=predecessor_gate,
                )
                _validate_run_spec_completion_lineage(
                    manifest,
                    lineage,
                    profile=profile,
                    predecessor_gate=predecessor_gate,
                )
                final_checkpoint_sha256, planned_source_date_count = _validate_final_checkpoint(
                    connection,
                    manifest_id=manifest_id,
                    run_fingerprint=fingerprint,
                    lineage=lineage,
                    data_root=data_root,
                    profile=profile,
                )
                if manifest["status"] == "SUCCEEDED":
                    if (
                        manifest.get("result_artifact_sha256") != held.sha256
                        or manifest.get("result_artifact_byte_size") != held.byte_size
                        or manifest.get("cell_summaries_sha256") != cells_sha256
                    ):
                        raise OutcomeRegistryDriftError(
                            "successful outcome replay artifact identity drift"
                        )
                    _verify_completed_cells(
                        connection,
                        manifest_id=manifest_id,
                        cells=cells,
                    )
                    artifact_rows = connection.execute(
                        """
                        SELECT artifact_id, uri, sha256, byte_size
                        FROM systematic_fx.artifacts
                        WHERE artifact_id = %s
                        FOR SHARE
                        """,
                        (int(manifest["result_artifact_id"]),),
                    ).fetchall()
                    if len(artifact_rows) != 1 or any(
                        artifact_rows[0].get(key) != value
                        for key, value in {
                            "uri": held.path.as_uri(),
                            "sha256": held.sha256,
                            "byte_size": held.byte_size,
                        }.items()
                    ):
                        raise OutcomeRegistryDriftError("successful result artifact row drift")
                    _register_screening_decisions(
                        connection,
                        manifest_id=manifest_id,
                        decisions=screening_decisions,
                    )
                    _verify_held_file(held)
                    if audit_evidence is not None:
                        _verify_held_file(audit_evidence)
                    return OutcomeCompletionReport(
                        outcome_replay_manifest_id=manifest_id,
                        research_run_spec_id=int(manifest["research_run_spec_id"]),
                        research_run_attempt_id=int(manifest["research_run_attempt_id"]),
                        result_artifact_id=int(manifest["result_artifact_id"]),
                        run_fingerprint=fingerprint,
                        result_artifact_sha256=held.sha256,
                        result_artifact_uri=held.path.as_uri(),
                        result_artifact_byte_size=held.byte_size,
                        cell_summaries_sha256=cells_sha256,
                        summary_row_count=EXPECTED_SUMMARY_COUNT,
                        created_artifact=False,
                        completed=False,
                    )
                if manifest["status"] != "RUNNING":
                    raise OutcomeRegistryStateError(
                        f"cannot complete outcome replay from {manifest['status']}"
                    )

                _register_cells(
                    connection,
                    manifest_id=manifest_id,
                    run_fingerprint=fingerprint,
                    cells=cells,
                )
                result_artifact_id, created_artifact, _ = _ensure_result_artifact(
                    connection,
                    run_fingerprint=fingerprint,
                    source_artifact_manifest_sha256=source_sha256,
                    cells_sha256=cells_sha256,
                    lineage=lineage,
                    final_checkpoint_sha256=final_checkpoint_sha256,
                    held=held,
                    profile=profile,
                    predecessor_gate=predecessor_gate,
                )
                finished_at = datetime.now(UTC)
                result_summary = {
                    "artifact_sha256": held.sha256,
                    "cache_manifest_sha256": lineage.cache_manifest_sha256,
                    "cell_summaries_sha256": cells_sha256,
                    "detail_record_count": lineage.detail_record_count,
                    "detail_shard_count": len(lineage.detail_shards),
                    "detail_shard_manifest_sha256": (lineage.detail_shard_manifest_sha256),
                    "final_checkpoint_sha256": final_checkpoint_sha256,
                    "input_lineage_sha256": lineage.input_lineage_sha256,
                    "outcome_config_id": profile.outcome_config_id,
                    "planned_source_date_count": planned_source_date_count,
                    "query_id": profile.query_id,
                    "source_artifact_manifest_sha256": source_sha256,
                    "summary_row_count": EXPECTED_SUMMARY_COUNT,
                }
                if predecessor_gate is not None:
                    result_summary.update(predecessor_gate.parameters)
                attempt = connection.execute(
                    """
                    UPDATE systematic_fx.research_run_attempts
                    SET status = 'SUCCEEDED', result_artifact_id = %s,
                        result_summary = %s, finished_at = %s
                    WHERE research_run_attempt_id = %s AND status = 'RUNNING'
                    RETURNING research_run_attempt_id
                    """,
                    (
                        result_artifact_id,
                        Jsonb(result_summary),
                        finished_at,
                        int(manifest["research_run_attempt_id"]),
                    ),
                ).fetchone()
                if attempt is None:
                    raise OutcomeRegistryStateError(
                        "running outcome attempt changed before completion"
                    )
                updated = connection.execute(
                    """
                    UPDATE systematic_fx.phase1a_outcome_replay_manifests
                    SET status = 'SUCCEEDED', result_artifact_id = %s,
                        result_artifact_sha256 = %s,
                        result_artifact_byte_size = %s,
                        cell_summaries_sha256 = %s,
                        finished_at = %s
                    WHERE outcome_replay_manifest_id = %s AND status = 'RUNNING'
                    RETURNING outcome_replay_manifest_id
                    """,
                    (
                        result_artifact_id,
                        held.sha256,
                        held.byte_size,
                        cells_sha256,
                        finished_at,
                        manifest_id,
                    ),
                ).fetchone()
                if updated is None:
                    raise OutcomeRegistryStateError(
                        "running outcome manifest changed before completion"
                    )
                _register_screening_decisions(
                    connection,
                    manifest_id=manifest_id,
                    decisions=screening_decisions,
                )
                _verify_held_file(held)
                if audit_evidence is not None:
                    _verify_held_file(audit_evidence)
        return OutcomeCompletionReport(
            outcome_replay_manifest_id=manifest_id,
            research_run_spec_id=int(manifest["research_run_spec_id"]),
            research_run_attempt_id=int(manifest["research_run_attempt_id"]),
            result_artifact_id=result_artifact_id,
            run_fingerprint=fingerprint,
            result_artifact_sha256=held.sha256,
            result_artifact_uri=held.path.as_uri(),
            result_artifact_byte_size=held.byte_size,
            cell_summaries_sha256=cells_sha256,
            summary_row_count=EXPECTED_SUMMARY_COUNT,
            created_artifact=created_artifact,
            completed=True,
        )
    finally:
        held.close()


@_translate_psycopg_errors("Phase 1A outcome replay failure")
def fail_phase1a_outcome_replay(
    database_url: str,
    *,
    outcome_replay_manifest_id: int,
    run_fingerprint: str,
    error_message: str,
) -> OutcomeReplayState:
    """Atomically terminalize a queued/running replay and its generic attempt."""

    target = _database_url(database_url)
    manifest_id = _positive_identifier(
        outcome_replay_manifest_id,
        label="outcome_replay_manifest_id",
    )
    fingerprint = _sha256(run_fingerprint, label="run_fingerprint")
    message = _nonempty(error_message, label="error_message")
    with psycopg.connect(target, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with connection.transaction():
            row = _load_manifest_for_update(
                connection,
                outcome_replay_manifest_id=manifest_id,
            )
            _assert_live_manifest(row, run_fingerprint=fingerprint)
            if row["status"] == "FAILED":
                if row.get("error_message") != message:
                    raise OutcomeRegistryDriftError("failed outcome error message drift")
                return _manifest_state(row)
            if row["status"] not in {"QUEUED", "RUNNING"}:
                raise OutcomeRegistryStateError(f"cannot fail outcome replay from {row['status']}")
            finished_at = datetime.now(UTC)
            connection.execute(
                """
                UPDATE systematic_fx.research_run_attempts
                SET status = 'FAILED', finished_at = %s, error_message = %s
                WHERE research_run_attempt_id = %s
                  AND status IN ('QUEUED', 'RUNNING')
                """,
                (finished_at, message, int(row["research_run_attempt_id"])),
            )
            updated = connection.execute(
                """
                UPDATE systematic_fx.phase1a_outcome_replay_manifests
                SET status = 'FAILED', finished_at = %s, error_message = %s
                WHERE outcome_replay_manifest_id = %s
                  AND status IN ('QUEUED', 'RUNNING')
                RETURNING *, %s::integer AS attempt_number
                """,
                (finished_at, message, manifest_id, int(row["attempt_number"])),
            ).fetchone()
            if updated is None:
                raise OutcomeRegistryStateError("outcome replay changed before failure")
            return _manifest_state(updated)


@_translate_psycopg_errors("Phase 1A outcome screening decision registration")
def register_phase1a_outcome_screening_decisions(
    database_url: str,
    *,
    outcome_replay_manifest_id: int,
    decisions: Sequence[OutcomeScreeningDecision],
) -> tuple[OutcomeScreeningDecision, ...]:
    """Atomically append the LONG and SHORT terminal decisions for one replay."""

    target = _database_url(database_url)
    manifest_id = _positive_identifier(
        outcome_replay_manifest_id,
        label="outcome_replay_manifest_id",
    )
    if isinstance(decisions, (str, bytes)) or not isinstance(decisions, Sequence):
        raise OutcomeRegistryError("decisions must be a sequence")
    by_direction: dict[str, OutcomeScreeningDecision] = {}
    for decision in decisions:
        if not isinstance(decision, OutcomeScreeningDecision):
            raise OutcomeRegistryError("decisions must contain OutcomeScreeningDecision values")
        if decision.direction in by_direction:
            raise OutcomeRegistryError("duplicate screening decision direction")
        by_direction[decision.direction] = decision
    if set(by_direction) != set(DIRECTION_IDS):
        raise OutcomeRegistryError("screening decisions must contain LONG and SHORT")
    ordered = tuple(by_direction[direction] for direction in DIRECTION_IDS)
    with psycopg.connect(target, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with connection.transaction():
            manifest = connection.execute(
                """
                SELECT status, pattern_key
                FROM systematic_fx.phase1a_outcome_replay_manifests
                WHERE outcome_replay_manifest_id = %s
                FOR SHARE
                """,
                (manifest_id,),
            ).fetchone()
            if manifest is None:
                raise OutcomeRegistryError("outcome replay manifest does not exist")
            if manifest["status"] != "SUCCEEDED":
                raise OutcomeRegistryStateError(
                    "screening decisions require a successful outcome replay"
                )
            profile = outcome_query_profile(str(manifest["pattern_key"]))
            rows = connection.execute(
                """
                SELECT scenario_id, direction, take_profit_ticks, stop_loss_ticks,
                       signal_count, entry_fill_count, entry_not_filled_count,
                       skipped_occupied_count, take_profit_first_count,
                       stop_first_count, terminal_exit_count, censored_count,
                       gross_pnl_ticks, variable_cost_ticks,
                       allocated_fixed_cost_ticks, fully_loaded_net_pnl_ticks,
                       fully_loaded_net_ev_ticks, fully_loaded_net_pnl_usd,
                       calendar_month_net_pnl_usd, profit_factor,
                       maximum_drawdown_usd, complete
                FROM systematic_fx.phase1a_outcome_cell_summaries
                WHERE outcome_replay_manifest_id = %s
                ORDER BY scenario_id, direction,
                         take_profit_ticks, stop_loss_ticks
                FOR SHARE
                """,
                (manifest_id,),
            ).fetchall()
            stored_cells = tuple(OutcomeCellSummary(**dict(row)) for row in rows)
            derived = derive_phase1a_outcome_screening_decisions(
                stored_cells,
                query_id=profile.query_id,
            )
            if ordered != derived:
                raise OutcomeRegistryDriftError(
                    "supplied screening decisions differ from the frozen selector "
                    "applied to stored cells"
                )
            _register_screening_decisions(
                connection,
                manifest_id=manifest_id,
                decisions=derived,
            )
    return derived


@_translate_psycopg_errors("Phase 1A outcome screening decision derivation")
def derive_and_register_phase1a_outcome_screening_decisions(
    database_url: str,
    *,
    outcome_replay_manifest_id: int,
) -> tuple[OutcomeScreeningDecision, ...]:
    """Backfill or verify selector-owned decisions from the immutable DB surface."""

    target = _database_url(database_url)
    manifest_id = _positive_identifier(
        outcome_replay_manifest_id,
        label="outcome_replay_manifest_id",
    )
    with psycopg.connect(target, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with connection.transaction():
            manifest = connection.execute(
                """
                SELECT status, pattern_key
                FROM systematic_fx.phase1a_outcome_replay_manifests
                WHERE outcome_replay_manifest_id = %s
                FOR SHARE
                """,
                (manifest_id,),
            ).fetchone()
            if manifest is None:
                raise OutcomeRegistryError("outcome replay manifest does not exist")
            if manifest["status"] != "SUCCEEDED":
                raise OutcomeRegistryStateError(
                    "screening decision derivation requires a successful replay"
                )
            profile = outcome_query_profile(str(manifest["pattern_key"]))
            rows = connection.execute(
                """
                SELECT scenario_id, direction, take_profit_ticks, stop_loss_ticks,
                       signal_count, entry_fill_count, entry_not_filled_count,
                       skipped_occupied_count, take_profit_first_count,
                       stop_first_count, terminal_exit_count, censored_count,
                       gross_pnl_ticks, variable_cost_ticks,
                       allocated_fixed_cost_ticks, fully_loaded_net_pnl_ticks,
                       fully_loaded_net_ev_ticks, fully_loaded_net_pnl_usd,
                       calendar_month_net_pnl_usd, profit_factor,
                       maximum_drawdown_usd, complete
                FROM systematic_fx.phase1a_outcome_cell_summaries
                WHERE outcome_replay_manifest_id = %s
                ORDER BY scenario_id, direction,
                         take_profit_ticks, stop_loss_ticks
                FOR SHARE
                """,
                (manifest_id,),
            ).fetchall()
            cells = tuple(OutcomeCellSummary(**dict(row)) for row in rows)
            decisions = derive_phase1a_outcome_screening_decisions(
                cells,
                query_id=profile.query_id,
            )
            _register_screening_decisions(
                connection,
                manifest_id=manifest_id,
                decisions=decisions,
            )
            return decisions
