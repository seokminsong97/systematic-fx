"""Governed persistence for the Phase 1A p5 MBP-10 outcome replay.

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

from systematic_fx.db.postgres_retry import retry_serialization_failures
from systematic_fx.research.run_spec import RUN_SPEC_SCHEMA, RUN_SPEC_SCHEMA_VERSION

CAMPAIGN_KEY: Final = "phase1a_conservative_screening_v1"
P5_QUERY_ID: Final = "p5_01_range_expansion_flow_continuation"
PATTERN_KEY: Final = P5_QUERY_ID
OUTCOME_ENGINE_VERSION: Final = "phase1a_shared_outcome_replay_v1"
OUTCOME_CONFIG_ID: Final = "phase1a_p5_outcome_replay_v1"
OUTCOME_ARTIFACT_SCHEMA: Final = "systematic_fx.phase1a_p5_outcome_replay.v1"
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
_CACHE_MANIFEST_DIRECTORY: Final = PurePosixPath(
    "backtest_event_cache/phase1a_daily_executable_cache_v1/manifests"
)

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
) -> tuple[tuple[OutcomeCellSummary, ...], str]:
    """Return canonical cell order and its aggregate content digest."""

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
        expected_signal_count = EXPECTED_DIRECTION_SIGNAL_COUNTS[value.direction]
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


def phase1a_p5_outcome_parameters(source_artifact_manifest_sha256: str) -> dict[str, object]:
    """Return the exact RunSpec parameter subset enforced by migration 0013."""

    source_sha256 = _sha256(
        source_artifact_manifest_sha256,
        label="source_artifact_manifest_sha256",
    )
    return {
        "cell_count_per_surface": EXPECTED_CELL_COUNT,
        "direction_ids": list(DIRECTION_IDS),
        "expected_detail_record_count": EXPECTED_DETAIL_RECORD_COUNT,
        "expected_direction_signal_counts": dict(EXPECTED_DIRECTION_SIGNAL_COUNTS),
        "expected_summary_count": EXPECTED_SUMMARY_COUNT,
        "final_source_date": EXPECTED_FINAL_SOURCE_DATE.isoformat(),
        "outcome_config_id": OUTCOME_CONFIG_ID,
        "planned_source_date_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "query_id": P5_QUERY_ID,
        "scenario_ids": list(SCENARIO_IDS),
        "scenario_cost_ticks_per_fill": {
            scenario_id: {
                "allocated_fixed": costs[1],
                "variable": costs[0],
            }
            for scenario_id, costs in SCENARIO_COST_TICKS_PER_FILL.items()
        },
        "source_artifact_manifest_sha256": source_sha256,
        "source_occurrence_count": EXPECTED_SOURCE_OCCURRENCE_COUNT,
        "source_slice_count": EXPECTED_SOURCE_SLICE_COUNT,
        "stop_loss_ticks": list(BARRIER_TICKS),
        "take_profit_ticks": list(BARRIER_TICKS),
    }


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
    identity: _FileIdentity
    sha256: str
    byte_size: int
    content: bytes

    def close(self) -> None:
        if self.descriptor >= 0:
            os.close(self.descriptor)
            self.descriptor = -1


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


def _open_held_immutable_file(path: Path, *, data_root: Path | str) -> _HeldFile:
    _, derived = _resolved_data_root(data_root)
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

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as error:
        raise OutcomeRegistryError(f"cannot open immutable artifact: {candidate}") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise OutcomeRegistryError("artifact must be a regular file")
        if before.st_mode & _WRITE_BITS:
            raise OutcomeRegistryError("artifact must be read-only before registration")
        identity = _file_identity(before)
        digest = hashlib.sha256()
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
            chunks.append(chunk)
        content = b"".join(chunks)
        if _file_identity(os.fstat(descriptor)) != identity or len(content) != before.st_size:
            raise OutcomeRegistryDriftError("artifact changed while it was read")
        return _HeldFile(
            descriptor=descriptor,
            path=candidate,
            identity=identity,
            sha256=digest.hexdigest(),
            byte_size=len(content),
            content=content,
        )
    except Exception:
        os.close(descriptor)
        raise


def _verify_held_file(held: _HeldFile) -> None:
    if held.descriptor < 0 or _file_identity(os.fstat(held.descriptor)) != held.identity:
        raise OutcomeRegistryDriftError("open artifact inode changed before commit")
    try:
        path_identity = _file_identity(held.path.lstat())
    except OSError as error:
        raise OutcomeRegistryDriftError("artifact path disappeared before commit") from error
    if path_identity != held.identity or not stat.S_ISREG(path_identity.mode):
        raise OutcomeRegistryDriftError("artifact path changed before commit")


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
        if isinstance(definition, Mapping) and definition.get("id") == P5_QUERY_ID:
            matches.append(result)
    if len(matches) != 1:
        raise OutcomeRegistryDriftError(
            f"AI_SLICE {slice_index} must contain exactly one canonical p5 query"
        )
    p5 = matches[0]
    occurrences = p5.get("occurrences")
    support_count = p5.get("support_count")
    direction_counts = p5.get("direction_counts")
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
        raise OutcomeRegistryDriftError("AI_SLICE p5 occurrence accounting drift")
    for occurrence in occurrences:
        if (
            not isinstance(occurrence, Mapping)
            or occurrence.get("direction") not in DIRECTION_IDS
            or occurrence.get("source_date") not in dates
        ):
            raise OutcomeRegistryDriftError("AI_SLICE p5 occurrence identity drift")
    return tuple(dates), support_count


def _set_serializable(connection: psycopg.Connection[dict[str, Any]]) -> None:
    connection.isolation_level = IsolationLevel.SERIALIZABLE


def _set_serializable_read_only(
    connection: psycopg.Connection[dict[str, Any]],
) -> None:
    connection.isolation_level = IsolationLevel.SERIALIZABLE
    connection.read_only = True


@_translate_psycopg_errors("Phase 1A p5 source artifact loading")
def load_phase1a_p5_source_artifacts(
    database_url: str,
    *,
    data_root: Path | str,
) -> P5SourceArtifactSet:
    """Load and byte-validate the complete ordered set of 99 AI_SLICE inputs."""

    target = _database_url(database_url)
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
            if len(rows) != EXPECTED_SOURCE_SLICE_COUNT:
                raise OutcomeRegistryDriftError(
                    f"expected {EXPECTED_SOURCE_SLICE_COUNT} successful AI_SLICE artifacts; "
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
                    or not 0 <= raw_slice_index < EXPECTED_SOURCE_SLICE_COUNT
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
                range(EXPECTED_SOURCE_SLICE_COUNT)
            ):
                raise OutcomeRegistryDriftError("AI_SLICE indexes are incomplete or duplicated")
            occurrence_count = sum(item.occurrence_count for item in descriptors)
            if occurrence_count != EXPECTED_SOURCE_OCCURRENCE_COUNT:
                raise OutcomeRegistryDriftError(
                    "canonical p5 occurrence count differs from the frozen 1,111"
                )
            manifest_sha256 = _canonical_sha256([item.manifest_payload for item in descriptors])
    return P5SourceArtifactSet(
        artifacts=tuple(descriptors),
        source_artifact_manifest_sha256=manifest_sha256,
        occurrence_count=occurrence_count,
    )


def _validate_governed_run_spec(
    row: Mapping[str, Any],
    *,
    run_fingerprint: str,
    source_artifact_manifest_sha256: str,
) -> tuple[int, int]:
    expected_parameters = phase1a_p5_outcome_parameters(source_artifact_manifest_sha256)
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
) -> None:
    expected = {
        "barrier_axis_size": len(BARRIER_TICKS),
        "cell_count_per_surface": EXPECTED_CELL_COUNT,
        "direction_count": len(DIRECTION_IDS),
        "expected_detail_record_count": EXPECTED_DETAIL_RECORD_COUNT,
        "expected_summary_count": EXPECTED_SUMMARY_COUNT,
        "final_source_date": EXPECTED_FINAL_SOURCE_DATE,
        "pattern_key": PATTERN_KEY,
        "planned_source_date_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "run_fingerprint": run_fingerprint,
        "scenario_count": len(SCENARIO_IDS),
        "source_artifact_manifest_sha256": source_artifact_manifest_sha256,
        "source_occurrence_count": EXPECTED_SOURCE_OCCURRENCE_COUNT,
        "source_slice_count": EXPECTED_SOURCE_SLICE_COUNT,
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
) -> OutcomeReplayReservation:
    """Reserve, exactly resume, or duplicate-skip one canonical p5 outcome run."""

    target = _database_url(database_url)
    fingerprint = _sha256(run_fingerprint, label="run_fingerprint")
    source_sha256 = _sha256(
        source_artifact_manifest_sha256,
        label="source_artifact_manifest_sha256",
    )
    with psycopg.connect(target, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with connection.transaction():
            spec = _load_run_spec_for_update(connection, run_fingerprint=fingerprint)
            research_run_spec_id, campaign_id = _validate_governed_run_spec(
                spec,
                run_fingerprint=fingerprint,
                source_artifact_manifest_sha256=source_sha256,
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
                )
                if current.get("manifest_status") != current.get("status"):
                    raise OutcomeRegistryDriftError("active outcome state drift")
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
                    PATTERN_KEY,
                    source_sha256,
                    EXPECTED_SOURCE_SLICE_COUNT,
                    EXPECTED_SOURCE_OCCURRENCE_COUNT,
                    len(SCENARIO_IDS),
                    len(DIRECTION_IDS),
                    len(BARRIER_TICKS),
                    EXPECTED_CELL_COUNT,
                    EXPECTED_SUMMARY_COUNT,
                    EXPECTED_DETAIL_RECORD_COUNT,
                    EXPECTED_PLANNED_SOURCE_DATE_COUNT,
                    EXPECTED_FINAL_SOURCE_DATE,
                ),
            ).fetchone()
            if manifest is None:  # pragma: no cover - RETURNING is mandatory
                raise OutcomeRegistryDatabaseError("outcome manifest returned no identity")
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
    source_sha = _sha256(
        row.get("source_artifact_manifest_sha256"),
        label="source_artifact_manifest_sha256",
    )
    _assert_manifest_identity(
        row,
        run_fingerprint=run_fingerprint,
        source_artifact_manifest_sha256=source_sha,
    )
    if row.get("attempt_status") != row.get("status"):
        raise OutcomeRegistryDriftError("outcome replay and attempt status drift")


@_translate_psycopg_errors("Phase 1A outcome replay start")
def start_phase1a_outcome_replay(
    database_url: str,
    *,
    outcome_replay_manifest_id: int,
    run_fingerprint: str,
) -> OutcomeReplayState:
    """Atomically transition the generic attempt and replay manifest to RUNNING."""

    target = _database_url(database_url)
    manifest_id = _positive_identifier(
        outcome_replay_manifest_id,
        label="outcome_replay_manifest_id",
    )
    fingerprint = _sha256(run_fingerprint, label="run_fingerprint")
    with psycopg.connect(target, row_factory=dict_row) as connection:
        _set_serializable(connection)
        with connection.transaction():
            row = _load_manifest_for_update(
                connection,
                outcome_replay_manifest_id=manifest_id,
            )
            _assert_live_manifest(row, run_fingerprint=fingerprint)
            if row["status"] == "RUNNING":
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
) -> dict[str, Any]:
    if held.path.name != f"sha256={held.sha256}.json":
        raise OutcomeRegistryError("checkpoint artifact filename must be sha256=<content>.json")
    document = _canonical_json_document(held.content, label="outcome checkpoint artifact")
    expected = {
        "artifact_schema": CHECKPOINT_ARTIFACT_SCHEMA,
        "checkpoint_sequence": checkpoint_sequence,
        "completed_source_date_count": completed_source_date_count,
        "last_completed_source_date": last_completed_source_date.isoformat(),
        "outcome_config_id": OUTCOME_CONFIG_ID,
        "outcome_replay_manifest_id": outcome_replay_manifest_id,
        "predecessor_checkpoint_sha256": predecessor_checkpoint_sha256,
        "progress_metadata_sha256": progress_metadata_sha256,
        "query_id": P5_QUERY_ID,
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
) -> dict[str, object]:
    return {
        "campaign_key": CAMPAIGN_KEY,
        "checkpoint_sequence": checkpoint_sequence,
        "last_completed_source_date": last_completed_source_date.isoformat(),
        "outcome_config_id": OUTCOME_CONFIG_ID,
        "outcome_replay_manifest_id": outcome_replay_manifest_id,
        "query_id": P5_QUERY_ID,
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
) -> OutcomeCheckpointReport:
    """Append one immutable SOURCE_DATE_COMPLETE artifact to the hash chain."""

    target = _database_url(database_url)
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
        )
        with psycopg.connect(target, row_factory=dict_row) as connection:
            _set_serializable(connection)
            with connection.transaction():
                manifest = _load_manifest_for_update(
                    connection,
                    outcome_replay_manifest_id=manifest_id,
                )
                _assert_live_manifest(manifest, run_fingerprint=fingerprint)
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
            artifact_path.relative_to(derived)
        except ValueError as error:
            raise OutcomeRegistryDriftError(
                "outcome checkpoint artifact URI is outside data/derived"
            ) from error
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
    _assert_no_symlink_components(derived, candidate)
    try:
        if candidate.resolve(strict=True) != candidate:
            raise OutcomeRegistryDriftError(f"{label} path is not canonical")
    except OSError as error:
        raise OutcomeRegistryDriftError(f"{label} path is not reachable") from error
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(candidate, flags)
    except OSError as error:
        raise OutcomeRegistryDriftError(f"cannot open immutable {label}") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_mode & _WRITE_BITS:
            raise OutcomeRegistryDriftError(f"{label} must be a read-only regular file")
        identity = _file_identity(before)
        observed = hashlib.sha256()
        observed_size = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            observed.update(chunk)
            observed_size += len(chunk)
        if (
            _file_identity(os.fstat(descriptor)) != identity
            or observed_size != before.st_size
            or observed.hexdigest() != digest
            or observed_size != byte_size
        ):
            raise OutcomeRegistryDriftError(f"{label} content identity drift")
        if _file_identity(candidate.lstat()) != identity:
            raise OutcomeRegistryDriftError(f"{label} path identity drift")
    finally:
        os.close(descriptor)
    return candidate


def _validated_input_lineage(
    value: object,
    *,
    source_artifact_manifest_sha256: str,
) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != _INPUT_LINEAGE_FIELDS:
        raise OutcomeRegistryDriftError("outcome input lineage schema drift")
    lineage = _canonical_mapping(value, label="outcome input lineage")
    for field_name in _INPUT_LINEAGE_SHA256_FIELDS:
        _sha256(lineage.get(field_name), label=f"input lineage {field_name}")
    if (
        lineage.get("expected_completed_source_date_count") != EXPECTED_PLANNED_SOURCE_DATE_COUNT
        or _iso_day(
            lineage.get("expected_last_completed_source_date"),
            label="input lineage expected_last_completed_source_date",
        )
        != EXPECTED_FINAL_SOURCE_DATE
    ):
        raise OutcomeRegistryDriftError(
            "outcome input lineage must retain the frozen 485-date completion boundary"
        )
    if lineage["rich_source_artifact_manifest_sha256"] != source_artifact_manifest_sha256:
        raise OutcomeRegistryDriftError("outcome input lineage source-artifact drift")
    return lineage


def _validated_final_checkpoint_reference(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != _FINAL_CHECKPOINT_REFERENCE_FIELDS:
        raise OutcomeRegistryDriftError("final checkpoint reference schema drift")
    reference = _canonical_mapping(value, label="final checkpoint reference")
    digest = _sha256(reference.get("artifact_sha256"), label="final checkpoint reference sha256")
    _positive_identifier(reference.get("byte_size"), label="final checkpoint reference byte_size")
    _relative_uri(
        reference.get("artifact_relative_uri"),
        label="final checkpoint reference URI",
        expected_parent=_CHECKPOINT_DIRECTORY,
        expected_name=f"sha256={digest}.json",
    )
    if (
        reference.get("checkpoint_sequence") != EXPECTED_PLANNED_SOURCE_DATE_COUNT
        or _iso_day(
            reference.get("last_completed_source_date"),
            label="final checkpoint reference source date",
        )
        != EXPECTED_FINAL_SOURCE_DATE
    ):
        raise OutcomeRegistryDriftError(
            "final checkpoint reference must identify checkpoint 485 on 2023-08-31"
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
        "cache_count": EXPECTED_CACHE_PARTITION_COUNT,
        "cache_schema": _CACHE_SCHEMA,
        "cache_version": _CACHE_VERSION,
        "partition_key": ["source_date", "raw_symbol"],
    }
    if any(document.get(key) != expected for key, expected in expected_static.items()):
        raise OutcomeRegistryDriftError("outcome cache manifest frozen identity drift")
    entries = document.get("cache_entries")
    if not isinstance(entries, list) or len(entries) != EXPECTED_CACHE_PARTITION_COUNT:
        raise OutcomeRegistryDriftError("outcome cache manifest must contain 485 partitions")
    entries_sha256 = _sha256(document.get("cache_entries_sha256"), label="cache entries sha256")
    if _canonical_sha256(entries) != entries_sha256:
        raise OutcomeRegistryDriftError("outcome cache entry manifest hash drift")
    if (
        reference.get("cache_count") != EXPECTED_CACHE_PARTITION_COUNT
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
        len(planned_source_dates) != EXPECTED_PLANNED_SOURCE_DATE_COUNT
        or planned_source_dates[-1] != EXPECTED_FINAL_SOURCE_DATE
    ):
        raise OutcomeRegistryDriftError(
            "cache manifest must cover the frozen 485 source dates through 2023-08-31"
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
            expected_parent=_DETAIL_SHARD_DIRECTORY,
            expected_sha256=digest,
            expected_byte_size=shard.get("byte_size"),
            suffix=".parquet",
            label=f"detail shard {sequence}",
        )
        shards.append(shard)
    if record_count != EXPECTED_DETAIL_RECORD_COUNT:
        raise OutcomeRegistryDriftError(
            f"outcome replay must retain exactly {EXPECTED_DETAIL_RECORD_COUNT} detail rows"
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
) -> _ValidatedCompletionLineage:
    if held.path.name != f"sha256={held.sha256}.json":
        raise OutcomeRegistryError("result artifact filename must be sha256=<content>.json")
    document = _canonical_json_document(held.content, label="outcome result artifact")
    if set(document) != _FINAL_RESULT_FIELDS:
        raise OutcomeRegistryDriftError("outcome result artifact field schema drift")
    expected = {
        "artifact_schema": OUTCOME_ARTIFACT_SCHEMA,
        "cell_summaries_sha256": cell_summaries_sha256,
        "direction_ids": list(DIRECTION_IDS),
        "outcome_config_id": OUTCOME_CONFIG_ID,
        "query_id": P5_QUERY_ID,
        "run_fingerprint": run_fingerprint,
        "scenario_ids": list(SCENARIO_IDS),
        "source_artifact_manifest_sha256": source_artifact_manifest_sha256,
        "source_occurrence_count": EXPECTED_SOURCE_OCCURRENCE_COUNT,
        "source_slice_count": EXPECTED_SOURCE_SLICE_COUNT,
        "summary_row_count": EXPECTED_SUMMARY_COUNT,
        "detail_record_count": EXPECTED_DETAIL_RECORD_COUNT,
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
    ) = _validated_cache_manifest(document.get("cache_manifest"), data_root=data_root)
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
        document.get("final_checkpoint")
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
) -> None:
    canonical_spec = manifest.get("run_spec_canonical_spec")
    if not isinstance(canonical_spec, Mapping):
        raise OutcomeRegistryDriftError("outcome RunSpec canonical payload is missing")
    parameters = canonical_spec.get("parameters")
    if not isinstance(parameters, Mapping):
        raise OutcomeRegistryDriftError("outcome RunSpec parameters are missing")
    expected = {
        "cache_manifest_sha256": lineage.cache_manifest_sha256,
        "cache_partition_count": EXPECTED_CACHE_PARTITION_COUNT,
        "expected_detail_record_count": EXPECTED_DETAIL_RECORD_COUNT,
        "expected_completed_source_date_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "expected_last_completed_source_date": EXPECTED_FINAL_SOURCE_DATE.isoformat(),
        "final_source_date": EXPECTED_FINAL_SOURCE_DATE.isoformat(),
        "input_plan_sha256": lineage.input_lineage["input_plan_sha256"],
        "planned_source_date_count": EXPECTED_PLANNED_SOURCE_DATE_COUNT,
        "portable_discovery_artifact_manifest_sha256": lineage.input_lineage[
            "portable_artifact_manifest_sha256"
        ],
        "portable_discovery_input_manifest_sha256": lineage.input_manifest_sha256,
        "portable_signal_manifest_sha256": lineage.input_lineage["signal_manifest_sha256"],
        "source_record_manifest_sha256": lineage.input_lineage["source_record_manifest_sha256"],
        "terminal_resolution_sha256": lineage.input_lineage["terminal_resolution_sha256"],
    }
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
        expected_parent=_CHECKPOINT_DIRECTORY,
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
            or replay_state.get("result_record_count") != EXPECTED_DETAIL_RECORD_COUNT
            or replay_state.get("drained_record_count") != EXPECTED_DETAIL_RECORD_COUNT
            or replay_state.get("signal_cursor") != EXPECTED_SOURCE_OCCURRENCE_COUNT
            or not isinstance(replay_state.get("signals"), list)
            or len(replay_state["signals"]) != EXPECTED_SOURCE_OCCURRENCE_COUNT
            or any(replay_state.get(name) != [] for name in expected_empty_fields)
        ):
            raise OutcomeRegistryDriftError(
                "final checkpoint is not a fully drained finished replay"
            )
        expected_progress = {
            "artifact_schema": _CHECKPOINT_PROGRESS_SCHEMA,
            "cache_manifest_sha256": lineage.cache_manifest_sha256,
            "detail_record_count": EXPECTED_DETAIL_RECORD_COUNT,
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


def _ensure_result_artifact(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    run_fingerprint: str,
    source_artifact_manifest_sha256: str,
    cells_sha256: str,
    lineage: _ValidatedCompletionLineage,
    final_checkpoint_sha256: str,
    held: _HeldFile,
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
        "outcome_config_id": OUTCOME_CONFIG_ID,
        "planned_source_date_count": len(lineage.planned_source_dates),
        "query_id": P5_QUERY_ID,
        "run_fingerprint": run_fingerprint,
        "scenario_count": len(SCENARIO_IDS),
        "source_artifact_manifest_sha256": source_artifact_manifest_sha256,
        "summary_row_count": EXPECTED_SUMMARY_COUNT,
    }
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
) -> OutcomeCompletionReport:
    """Atomically publish all cells, one immutable artifact, and one success."""

    target = _database_url(database_url)
    manifest_id = _positive_identifier(
        outcome_replay_manifest_id,
        label="outcome_replay_manifest_id",
    )
    fingerprint = _sha256(run_fingerprint, label="run_fingerprint")
    cells, cells_sha256 = validate_complete_cell_summaries(cell_summaries)
    held = _open_held_immutable_file(Path(result_artifact_path), data_root=data_root)
    try:
        with psycopg.connect(target, row_factory=dict_row) as connection:
            _set_serializable(connection)
            with connection.transaction():
                manifest = _load_manifest_for_update(
                    connection,
                    outcome_replay_manifest_id=manifest_id,
                )
                _assert_live_manifest(manifest, run_fingerprint=fingerprint)
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
                )
                _validate_run_spec_completion_lineage(manifest, lineage)
                final_checkpoint_sha256, planned_source_date_count = _validate_final_checkpoint(
                    connection,
                    manifest_id=manifest_id,
                    run_fingerprint=fingerprint,
                    lineage=lineage,
                    data_root=data_root,
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
                    _verify_held_file(held)
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
                    "outcome_config_id": OUTCOME_CONFIG_ID,
                    "planned_source_date_count": planned_source_date_count,
                    "query_id": P5_QUERY_ID,
                    "source_artifact_manifest_sha256": source_sha256,
                    "summary_row_count": EXPECTED_SUMMARY_COUNT,
                }
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
                _verify_held_file(held)
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
