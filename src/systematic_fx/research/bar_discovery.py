"""Deterministic Discovery execution for the frozen bar-pattern catalog.

Only the visible Discovery range is loaded.  Signal decisions stop at the
predeclared Discovery decision boundary, while the final 20 active days remain
available solely as an outcome tail.  Paths are truncated at the Discovery
boundary, which implements the frozen mandatory terminal-exit policy without
opening any walk-forward or holdout artifact.

The fixture-oriented in-memory scan retains every matched point-in-time
context.  The production entry point instead holds one verified outcome span,
fixed-size running economics for all 216 candidates, and bounded evidence
buffers while immediately publishing compact contexts/replays to Parquet.
"""

from __future__ import annotations

import hashlib
import json
import os
from array import array
from bisect import bisect_left
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from fractions import Fraction
from pathlib import Path
from typing import Final, Protocol

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from systematic_fx.backtest.bar_replay import (
    BAR_EXECUTION_SCENARIOS,
    BarCellOutcome,
    BarPathIndex,
    BarSignalSurface,
    BarThresholdHit,
    replay_bar_signal,
)
from systematic_fx.backtest.barriers import BARRIER_TICKS, Direction
from systematic_fx.features.bars import (
    DailyBarBuildReport,
    TradeBar,
    TradeBarArtifactDescriptor,
    load_trade_bar_artifact,
)
from systematic_fx.research.bar_artifacts import (
    BarArtifactDescriptor,
    PublishedBarArtifact,
    arrow_schema_sha256,
    open_verified_bar_artifact,
    publish_bar_json_artifact,
    publish_bar_parquet_table,
)
from systematic_fx.research.bar_config import (
    ALLOCATED_VARIANT_COUNT,
    BAR_PATTERN_CONFIG_SEMANTIC_SHA256,
    SIGNAL_TIMEFRAMES_SECONDS,
    BarPatternCandidate,
    frozen_bar_pattern_candidates,
)
from systematic_fx.research.bar_economics import (
    BarCandidateEconomicsAccumulator,
    BarCellEconomics,
    CandidateSignalReplay,
)
from systematic_fx.research.bar_patterns import (
    BarPatternContext,
    BarPatternError,
    BarPatternEvaluation,
    build_bar_pattern_context,
    evaluate_bar_pattern_context,
)
from systematic_fx.research.bar_pipeline import (
    BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
    BarDatasetBuildResult,
    BarDatasetPartition,
    LoadedBarDatasetManifest,
)
from systematic_fx.research.bar_selection import (
    BarCandidateDecision,
    BarSupportEvidence,
    rank_bar_finalists,
    screen_bar_candidate,
)
from systematic_fx.research.hypotheses import canonical_sha256
from systematic_fx.validation.bar_splits import BarDateRange, BarSplitPlan

DISCOVERY_RESULT_SCHEMA: Final = "systematic_fx.bar_pattern_discovery_result.v1"
DISCOVERY_EVIDENCE_SCHEMA: Final = "systematic_fx.bar_pattern_discovery_evidence.v1"
DISCOVERY_SPOOL_VERSION: Final = "bar_pattern_discovery_spool_v1"
ENTRY_FILLED: Final = "ENTRY_FILLED"
ENTRY_NOT_FILLED: Final = "ENTRY_NOT_FILLED"
NEXT_SIGNAL_BAR_UNAVAILABLE: Final = "NEXT_SIGNAL_BAR_UNAVAILABLE"
_REQUIRED_TIMEFRAMES: Final = (1, *SIGNAL_TIMEFRAMES_SECONDS)
_SCENARIO_IDS: Final = (
    "BASELINE",
    "MODERATE_COMBINED",
    "SEVERE_DIAGNOSTIC",
)
_ONE_SECOND_NS: Final = 1_000_000_000
_MATCH_SHARD_MAX_RECORDS: Final = 4_096
_REPLAY_SHARD_MAX_RECORDS: Final = 256


class BarDiscoveryError(RuntimeError):
    """Discovery inputs or deterministic accounting are inconsistent."""


class _DiscoveryBarLike(Protocol):
    timeframe_seconds: int
    segment_id: int
    contract: str
    source_date: date
    start_ns: int
    end_ns: int
    first_trade_ns: int
    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int


@dataclass(frozen=True, slots=True)
class _OutcomePathBar:
    """A compact 1s view whose segment identity is the frozen outcome span.

    Signal bars keep their original gap-derived ``segment_id`` so pattern
    context never crosses a missing/overlapping signal interval.  Outcome
    replay deliberately uses the coarser manifest-derived span instead, which
    lets an open position survive ordinary maintenance and market-closed gaps.
    """

    timeframe_seconds: int
    segment_id: int
    contract: str
    source_date: date
    start_ns: int
    end_ns: int
    first_trade_ns: int
    open_ticks: int
    high_ticks: int
    low_ticks: int
    close_ticks: int

    @classmethod
    def from_bar(cls, bar: _DiscoveryBarLike, *, outcome_span_id: int) -> _OutcomePathBar:
        return cls(
            timeframe_seconds=bar.timeframe_seconds,
            segment_id=outcome_span_id,
            contract=bar.contract,
            source_date=bar.source_date,
            start_ns=bar.start_ns,
            end_ns=bar.end_ns,
            first_trade_ns=bar.first_trade_ns,
            open_ticks=bar.open_ticks,
            high_ticks=bar.high_ticks,
            low_ticks=bar.low_ticks,
            close_ticks=bar.close_ticks,
        )


class _ArtifactLoader(Protocol):
    def __call__(
        self,
        data_root: Path | str,
        artifact: TradeBarArtifactDescriptor,
        *,
        expected_plan_sha256: str | None = None,
        expected_source_sha256: str | None = None,
        expected_source_date: date | None = None,
    ) -> tuple[TradeBar, ...]: ...


class _BarStartLookup:
    """Compact O(log n) start timestamp index for one ordered outcome path."""

    __slots__ = ("_starts",)

    def __init__(self, bars: Sequence[_DiscoveryBarLike]) -> None:
        self._starts = array("q", (item.start_ns for item in bars))

    def __getitem__(self, start_ns: int) -> int:
        index = bisect_left(self._starts, start_ns)
        if index >= len(self._starts) or self._starts[index] != start_ns:
            raise KeyError(start_ns)
        return index


def _sha256(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise BarDiscoveryError(f"{label} must be a lowercase SHA-256")
    return value


def _canonical_bytes(value: object) -> bytes:
    try:
        return (
            json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("ascii")
            + b"\n"
        )
    except (TypeError, ValueError) as error:
        raise BarDiscoveryError("Discovery result is not strict canonical JSON") from error


BarDiscoveryPartition = BarDatasetPartition


def _partition_identity(value: BarDatasetPartition) -> dict[str, object]:
    return value.identity_dict()


@dataclass(frozen=True, slots=True)
class BarDiscoveryEvidenceShard:
    """One immutable content-addressed Parquet evidence shard."""

    record_kind: str
    segment_id: int
    shard_ordinal: int
    contract: str
    first_decision_ns: int | None
    last_decision_ns: int | None
    row_count: int
    relative_uri: str
    sha256: str
    byte_size: int
    artifact: PublishedBarArtifact

    def as_dict(self) -> dict[str, object]:
        return {
            "byte_size": self.byte_size,
            "artifact_descriptor": self.artifact.descriptor.identity_document(),
            "artifact_identity_sha256": self.artifact.descriptor.identity_sha256,
            "contract": self.contract,
            "first_decision_ns": self.first_decision_ns,
            "last_decision_ns": self.last_decision_ns,
            "record_kind": self.record_kind,
            "relative_uri": self.relative_uri,
            "row_count": self.row_count,
            "shard_ordinal": self.shard_ordinal,
            "segment_id": self.segment_id,
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class BarDiscoveryEvidenceManifest:
    """Bounded-memory evidence retained below ``data/derived``."""

    evidence_identity_sha256: str
    source_identity_sha256: str
    source_manifest_sha256: str | None
    dataset_build_sha256: str | None
    outcome_span_policy_sha256: str
    split_plan_sha256: str
    config_semantic_sha256: str
    candidate_catalog_sha256: str
    shards: tuple[BarDiscoveryEvidenceShard, ...]
    relative_uri: str
    sha256: str
    byte_size: int
    artifact: PublishedBarArtifact

    @property
    def matched_record_count(self) -> int:
        return sum(item.row_count for item in self.shards if item.record_kind == "matches")

    @property
    def replay_record_count(self) -> int:
        return sum(item.row_count for item in self.shards if item.record_kind == "replays")

    def as_dict(self) -> dict[str, object]:
        return {
            "byte_size": self.byte_size,
            "artifact_descriptor": self.artifact.descriptor.identity_document(),
            "artifact_identity_sha256": self.artifact.descriptor.identity_sha256,
            "candidate_catalog_sha256": self.candidate_catalog_sha256,
            "config_semantic_sha256": self.config_semantic_sha256,
            "dataset_build_sha256": self.dataset_build_sha256,
            "evidence_identity_sha256": self.evidence_identity_sha256,
            "matched_record_count": self.matched_record_count,
            "outcome_span_policy_sha256": self.outcome_span_policy_sha256,
            "relative_uri": self.relative_uri,
            "replay_record_count": self.replay_record_count,
            "sha256": self.sha256,
            "shards": [item.as_dict() for item in self.shards],
            "source_identity_sha256": self.source_identity_sha256,
            "source_manifest_sha256": self.source_manifest_sha256,
            "split_plan_sha256": self.split_plan_sha256,
        }


@dataclass(frozen=True, slots=True)
class BarDiscoveryProgress:
    """Non-economic operational progress safe to expose during a sealed run."""

    stage: str
    completed_active_dates: int
    total_active_dates: int
    completed_segments: int
    loaded_bar_counts: tuple[tuple[int, int], ...]
    matched_signal_count: int
    evidence_shard_count: int


@dataclass(frozen=True, slots=True)
class BarDiscoveryMemoryPlan:
    """Deterministic row/object caps for the production streaming topology."""

    visible_active_date_count: int
    outcome_span_count: int
    maximum_outcome_span_id: int
    maximum_outcome_span_contract: str
    maximum_outcome_span_bar_counts: tuple[tuple[int, int], ...]
    maximum_outcome_span_total_bar_count: int
    concurrent_cell_accumulator_count: int
    final_summary_cell_count: int
    maximum_materialized_replay_cell_count: int
    maximum_buffered_match_record_count: int
    maximum_buffered_replay_record_count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "concurrent_cell_accumulator_count": self.concurrent_cell_accumulator_count,
            "final_summary_cell_count": self.final_summary_cell_count,
            "maximum_buffered_match_record_count": (self.maximum_buffered_match_record_count),
            "maximum_buffered_replay_record_count": (self.maximum_buffered_replay_record_count),
            "maximum_materialized_replay_cell_count": (self.maximum_materialized_replay_cell_count),
            "maximum_outcome_span_bar_counts": [
                {"row_count": count, "timeframe_seconds": timeframe}
                for timeframe, count in self.maximum_outcome_span_bar_counts
            ],
            "maximum_outcome_span_contract": self.maximum_outcome_span_contract,
            "maximum_outcome_span_id": self.maximum_outcome_span_id,
            "maximum_outcome_span_total_bar_count": (self.maximum_outcome_span_total_bar_count),
            "outcome_span_count": self.outcome_span_count,
            "visible_active_date_count": self.visible_active_date_count,
            "whole_discovery_rows_retained": False,
        }


type BarDiscoverySource = (
    BarDatasetBuildResult
    | LoadedBarDatasetManifest
    | Sequence[DailyBarBuildReport]
    | Sequence[BarDatasetPartition]
)


@dataclass(frozen=True, slots=True)
class BarCompactThresholdHit:
    """One of the 44 sufficient first-hit records for a scenario replay."""

    distance_ticks: int
    trigger_price_ticks: int
    path_index: int | None
    bar_start_ns: int | None
    observed_bar_open_ticks: int | None

    def as_dict(self) -> dict[str, int | None]:
        return {
            "bar_start_ns": self.bar_start_ns,
            "distance_ticks": self.distance_ticks,
            "observed_bar_open_ticks": self.observed_bar_open_ticks,
            "path_index": self.path_index,
            "trigger_price_ticks": self.trigger_price_ticks,
        }


@dataclass(frozen=True, slots=True)
class BarCompactScenarioReplay:
    """Sufficient statistics for all 484 outcomes without cell duplication."""

    scenario_id: str
    direction: Direction
    segment_id: int
    contract: str
    entry_path_index: int
    entry_1s_start_ns: int
    entry_fill_price_ticks: int
    terminal_path_index: int
    terminal_1s_start_ns: int
    terminal_close_ticks: int
    take_profit_hits: tuple[BarCompactThresholdHit, ...]
    stop_hits: tuple[BarCompactThresholdHit, ...]

    def __post_init__(self) -> None:
        if self.scenario_id not in _SCENARIO_IDS:
            raise BarDiscoveryError("compact replay has an unknown scenario")
        if len(self.take_profit_hits) != len(BARRIER_TICKS) or len(self.stop_hits) != len(
            BARRIER_TICKS
        ):
            raise BarDiscoveryError("compact replay must retain exactly 44 threshold hits")
        if tuple(item.distance_ticks for item in self.take_profit_hits) != BARRIER_TICKS:
            raise BarDiscoveryError("compact TP hits differ from the frozen barrier axis")
        if tuple(item.distance_ticks for item in self.stop_hits) != BARRIER_TICKS:
            raise BarDiscoveryError("compact SL hits differ from the frozen barrier axis")

    def cell(self, take_profit_ticks: int, stop_loss_ticks: int) -> BarCellOutcome:
        """Reconstruct one exact cell outcome from the compact sufficient statistics."""

        try:
            tp_hit = self.take_profit_hits[BARRIER_TICKS.index(take_profit_ticks)]
            stop_hit = self.stop_hits[BARRIER_TICKS.index(stop_loss_ticks)]
        except ValueError as error:
            raise KeyError(
                f"unknown barrier cell tp{take_profit_ticks}_sl{stop_loss_ticks}"
            ) from error
        scenario = BAR_EXECUTION_SCENARIOS[self.scenario_id]
        tp_index = tp_hit.path_index
        stop_index = stop_hit.path_index
        same_second = tp_index is not None and tp_index == stop_index
        if stop_index is not None and (tp_index is None or stop_index <= tp_index):
            outcome = "STOP_FIRST"
            exit_index = stop_index
            observed_open = stop_hit.observed_bar_open_ticks
            if observed_open is None:  # guarded by compact-surface construction
                raise BarDiscoveryError("stop hit lacks its observed 1s open")
            if self.direction is Direction.LONG:
                exit_fill = min(
                    observed_open,
                    stop_hit.trigger_price_ticks - scenario.stop_total_minimum_adverse_ticks,
                )
            else:
                exit_fill = max(
                    observed_open,
                    stop_hit.trigger_price_ticks + scenario.stop_total_minimum_adverse_ticks,
                )
        elif tp_index is not None:
            outcome = "TP_FIRST"
            exit_index = tp_index
            exit_fill = tp_hit.trigger_price_ticks
        else:
            outcome = "TERMINAL_EXIT"
            exit_index = self.terminal_path_index
            exit_fill = (
                self.terminal_close_ticks - scenario.terminal_exit_adverse_ticks
                if self.direction is Direction.LONG
                else self.terminal_close_ticks + scenario.terminal_exit_adverse_ticks
            )
        gross = (
            exit_fill - self.entry_fill_price_ticks
            if self.direction is Direction.LONG
            else self.entry_fill_price_ticks - exit_fill
        )
        buying_price = (
            self.entry_fill_price_ticks if self.direction is Direction.LONG else exit_fill
        )
        selling_price = (
            exit_fill if self.direction is Direction.LONG else self.entry_fill_price_ticks
        )
        return BarCellOutcome(
            direction=self.direction,
            take_profit_ticks=take_profit_ticks,
            stop_loss_ticks=stop_loss_ticks,
            entry_path_index=self.entry_path_index,
            entry_fill_price_ticks=self.entry_fill_price_ticks,
            buying_price_ticks=buying_price,
            selling_price_ticks=selling_price,
            take_profit_target_price_ticks=tp_hit.trigger_price_ticks,
            loss_trigger_price_ticks=stop_hit.trigger_price_ticks,
            outcome=outcome,
            exit_path_index=exit_index,
            exit_fill_price_ticks=exit_fill,
            gross_pnl_ticks=gross,
            variable_debit_ticks=scenario.variable_debit_ticks,
            allocated_fixed_cost_ticks=scenario.allocated_fixed_cost_ticks,
            fully_loaded_net_pnl_ticks=(
                gross - scenario.variable_debit_ticks - scenario.allocated_fixed_cost_ticks
            ),
            take_profit_hit_index=tp_index,
            stop_hit_index=stop_index,
            same_second_stop_first=same_second,
        )

    def to_surface(self) -> BarSignalSurface:
        """Materialize 484 cells only while feeding an occupancy accumulator."""

        scenario = BAR_EXECUTION_SCENARIOS[self.scenario_id]
        return BarSignalSurface(
            scenario_id=self.scenario_id,
            direction=self.direction,
            segment_id=self.segment_id,
            contract=self.contract,
            entry_path_index=self.entry_path_index,
            entry_fill_price_ticks=self.entry_fill_price_ticks,
            terminal_path_index=self.terminal_path_index,
            fixed_pool_multiplier=scenario.fixed_pool_multiplier,
            take_profit_hits=tuple(
                BarThresholdHit(
                    distance_ticks=item.distance_ticks,
                    trigger_price_ticks=item.trigger_price_ticks,
                    path_index=item.path_index,
                    bar_start_ns=item.bar_start_ns,
                )
                for item in self.take_profit_hits
            ),
            stop_hits=tuple(
                BarThresholdHit(
                    distance_ticks=item.distance_ticks,
                    trigger_price_ticks=item.trigger_price_ticks,
                    path_index=item.path_index,
                    bar_start_ns=item.bar_start_ns,
                )
                for item in self.stop_hits
            ),
            cells=tuple(
                self.cell(take_profit, stop_loss)
                for take_profit in BARRIER_TICKS
                for stop_loss in BARRIER_TICKS
            ),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "contract": self.contract,
            "direction": self.direction.value,
            "entry_1s_start_ns": self.entry_1s_start_ns,
            "entry_fill_price_ticks": self.entry_fill_price_ticks,
            "entry_path_index": self.entry_path_index,
            "scenario_id": self.scenario_id,
            "segment_id": self.segment_id,
            "stop_hits": [item.as_dict() for item in self.stop_hits],
            "take_profit_hits": [item.as_dict() for item in self.take_profit_hits],
            "terminal_1s_start_ns": self.terminal_1s_start_ns,
            "terminal_close_ticks": self.terminal_close_ticks,
            "terminal_path_index": self.terminal_path_index,
        }


@dataclass(frozen=True, slots=True)
class BarCompactReplayBundle:
    """One direction/entry replay shared by every matching pattern candidate."""

    replay_key: str
    scenarios: tuple[BarCompactScenarioReplay, ...]

    def __post_init__(self) -> None:
        _sha256(self.replay_key, label="replay_key")
        if tuple(item.scenario_id for item in self.scenarios) != _SCENARIO_IDS:
            raise BarDiscoveryError("replay bundle scenarios are incomplete or unordered")

    def scenario(self, scenario_id: str) -> BarCompactScenarioReplay:
        for item in self.scenarios:
            if item.scenario_id == scenario_id:
                return item
        raise KeyError(scenario_id)

    def as_dict(self) -> dict[str, object]:
        return {
            "replay_key": self.replay_key,
            "scenarios": [item.as_dict() for item in self.scenarios],
        }


@dataclass(frozen=True, slots=True)
class BarMatchedSignal:
    """One matched definition with its complete leakage-safe context."""

    signal_id: str
    signal_date: date
    block_key: str
    outcome_span_id: int
    evaluation: BarPatternEvaluation
    entry_status: str
    no_fill_reason: str | None
    entry_path_index: int | None
    entry_1s_start_ns: int | None
    replay_key: str | None

    def __post_init__(self) -> None:
        if not self.signal_id:
            raise BarDiscoveryError("signal_id must be non-empty")
        if (
            isinstance(self.outcome_span_id, bool)
            or not isinstance(self.outcome_span_id, int)
            or self.outcome_span_id <= 0
        ):
            raise BarDiscoveryError("outcome_span_id must be a positive integer")
        if self.entry_status not in {ENTRY_FILLED, ENTRY_NOT_FILLED}:
            raise BarDiscoveryError("entry_status is outside the frozen states")
        if self.entry_status == ENTRY_FILLED:
            if (
                self.no_fill_reason is not None
                or self.entry_path_index is None
                or self.entry_1s_start_ns is None
                or self.replay_key is None
            ):
                raise BarDiscoveryError("filled signal entry linkage is incomplete")
        elif (
            self.no_fill_reason is None
            or self.entry_path_index is not None
            or self.entry_1s_start_ns is not None
            or self.replay_key is not None
        ):
            raise BarDiscoveryError("unfilled signal entry linkage is inconsistent")
        if not self.evaluation.matched:
            raise BarDiscoveryError("only matched evaluations may become signals")

    @property
    def candidate_key(self) -> str:
        return self.evaluation.candidate.candidate_key

    @property
    def decision_ns(self) -> int:
        return self.evaluation.context.decision_ns

    @property
    def segment_key(self) -> tuple[int, str]:
        return self.outcome_span_id, self.evaluation.context.contract

    def as_dict(self) -> dict[str, object]:
        return {
            "block_key": self.block_key,
            "entry_1s_start_ns": self.entry_1s_start_ns,
            "entry_path_index": self.entry_path_index,
            "entry_status": self.entry_status,
            "evaluation": self.evaluation.as_dict(),
            "no_fill_reason": self.no_fill_reason,
            "outcome_span_id": self.outcome_span_id,
            "replay_key": self.replay_key,
            "signal_date": self.signal_date.isoformat(),
            "signal_id": self.signal_id,
        }


@dataclass(frozen=True, slots=True)
class BarScenarioDiscoveryEconomics:
    """The complete 484-cell result for one frozen execution scenario."""

    scenario_id: str
    cells: tuple[BarCellEconomics, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "cells": [item.as_dict() for item in self.cells],
            "scenario_id": self.scenario_id,
        }


def _decision_payload(value: BarCandidateDecision) -> dict[str, object]:
    return {
        "candidate_key": value.candidate_key,
        "direction": value.direction.value,
        "label": value.label,
        "moderate_maximum_drawdown_ticks": value.moderate_maximum_drawdown_ticks,
        "overall_moderate_ev_ticks": (
            None
            if value.overall_moderate_ev_ticks is None
            else format(value.overall_moderate_ev_ticks, "f")
        ),
        "positive_block_count": value.positive_block_count,
        "positive_component_size": value.positive_component_size,
        "rejection_reasons": list(value.rejection_reasons),
        "selected_buy_sell_loss_formula": value.selected_buy_sell_loss_formula,
        "selected_stop_loss_ticks": value.selected_stop_loss_ticks,
        "selected_take_profit_ticks": value.selected_take_profit_ticks,
        "worst_block_moderate_ev_ticks": (
            None
            if value.worst_block_moderate_ev_ticks is None
            else format(value.worst_block_moderate_ev_ticks, "f")
        ),
    }


def _support_payload(value: BarSupportEvidence) -> dict[str, object]:
    return {
        "block_signal_counts": list(value.block_signal_counts),
        "candidate_key": value.candidate_key,
        "direction": value.direction.value,
        "distinct_signal_day_count": value.distinct_signal_day_count,
        "median_signals_per_active_day_denominator": (
            value.median_signals_per_active_day_denominator
        ),
        "median_signals_per_active_day_numerator": (value.median_signals_per_active_day_numerator),
        "raw_signal_count": value.raw_signal_count,
        "timeframe_seconds": value.timeframe_seconds,
    }


@dataclass(frozen=True, slots=True)
class BarCandidateDiscoveryResult:
    """All support, gates, matches, economics, and decision for one variant."""

    candidate: BarPatternCandidate
    decision_trigger_count: int
    evaluated_count: int
    context_not_evaluable_count: int
    context_rejection_counts: tuple[tuple[str, int], ...]
    failed_gate_counts: tuple[tuple[str, int], ...]
    matched_signal_count: int
    matched_signals: tuple[BarMatchedSignal, ...]
    support: BarSupportEvidence
    economics: tuple[BarScenarioDiscoveryEconomics, ...]
    decision: BarCandidateDecision
    final_label: str

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_definition": self.candidate.definition_payload(),
            "candidate_definition_sha256": self.candidate.definition_sha256,
            "candidate_key": self.candidate.candidate_key,
            "context_not_evaluable_count": self.context_not_evaluable_count,
            "context_rejection_counts": [
                {"count": count, "reason": reason}
                for reason, count in self.context_rejection_counts
            ],
            "decision": _decision_payload(self.decision),
            "decision_trigger_count": self.decision_trigger_count,
            "economics": [item.as_dict() for item in self.economics],
            "evaluated_count": self.evaluated_count,
            "failed_gate_counts": [
                {"count": count, "gate_id": gate_id} for gate_id, count in self.failed_gate_counts
            ],
            "final_label": self.final_label,
            "matched_signal_count": self.matched_signal_count,
            "matched_signals": [item.as_dict() for item in self.matched_signals],
            "support": _support_payload(self.support),
        }


@dataclass(frozen=True, slots=True)
class BarDiscoveryResult:
    """The deterministic, publication-ready Discovery result payload."""

    source_identity_sha256: str
    dataset_build_sha256: str | None
    outcome_span_policy_sha256: str
    config_semantic_sha256: str
    candidate_catalog_sha256: str
    split_plan_sha256: str
    loaded_source_dates: tuple[date, ...]
    decision_dates: tuple[date, ...]
    loaded_bar_counts: tuple[tuple[int, int], ...]
    replay_catalog: tuple[BarCompactReplayBundle, ...]
    evidence_manifest: BarDiscoveryEvidenceManifest | None
    candidate_results: tuple[BarCandidateDiscoveryResult, ...]
    ranked_finalist_keys: tuple[str, ...]
    budget_rejected_keys: tuple[str, ...]

    @property
    def evaluated_count(self) -> int:
        return sum(item.evaluated_count for item in self.candidate_results)

    @property
    def matched_signal_count(self) -> int:
        return sum(item.matched_signal_count for item in self.candidate_results)

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": DISCOVERY_RESULT_SCHEMA,
            "budget_rejected_keys": list(self.budget_rejected_keys),
            "candidate_catalog_sha256": self.candidate_catalog_sha256,
            "candidate_count": len(self.candidate_results),
            "candidate_results": [item.as_dict() for item in self.candidate_results],
            "dataset_build_sha256": self.dataset_build_sha256,
            "decision_dates": [item.isoformat() for item in self.decision_dates],
            "evaluated_count": self.evaluated_count,
            "evidence_manifest": (
                None if self.evidence_manifest is None else self.evidence_manifest.as_dict()
            ),
            "config_semantic_sha256": self.config_semantic_sha256,
            "loaded_bar_counts": [
                {"row_count": count, "timeframe_seconds": timeframe}
                for timeframe, count in self.loaded_bar_counts
            ],
            "loaded_source_dates": [item.isoformat() for item in self.loaded_source_dates],
            "matched_signal_count": self.matched_signal_count,
            "outcome_span_policy_sha256": self.outcome_span_policy_sha256,
            "replay_catalog": [item.as_dict() for item in self.replay_catalog],
            "replay_catalog_count": len(self.replay_catalog),
            "ranked_finalist_keys": list(self.ranked_finalist_keys),
            "source_identity_sha256": self.source_identity_sha256,
            "split_plan_sha256": self.split_plan_sha256,
        }

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.as_dict())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


def _cell_outcome_payload(value: BarCellOutcome | None) -> dict[str, object] | None:
    if value is None:
        return None
    return {
        "allocated_fixed_cost_ticks": value.allocated_fixed_cost_ticks,
        "buying_price_ticks": value.buying_price_ticks,
        "direction": value.direction.value,
        "entry_fill_price_ticks": value.entry_fill_price_ticks,
        "entry_path_index": value.entry_path_index,
        "exit_fill_price_ticks": value.exit_fill_price_ticks,
        "exit_path_index": value.exit_path_index,
        "fully_loaded_net_pnl_ticks": value.fully_loaded_net_pnl_ticks,
        "gross_pnl_ticks": value.gross_pnl_ticks,
        "loss_trigger_price_ticks": value.loss_trigger_price_ticks,
        "outcome": value.outcome,
        "same_second_stop_first": value.same_second_stop_first,
        "selling_price_ticks": value.selling_price_ticks,
        "stop_hit_index": value.stop_hit_index,
        "stop_loss_ticks": value.stop_loss_ticks,
        "take_profit_hit_index": value.take_profit_hit_index,
        "take_profit_target_price_ticks": value.take_profit_target_price_ticks,
        "take_profit_ticks": value.take_profit_ticks,
        "variable_debit_ticks": value.variable_debit_ticks,
    }


@dataclass(frozen=True, slots=True)
class BarDiscoveryCellLedgerRecord:
    """One streaming Parquet-ready cell disposition for one matched signal."""

    candidate_key: str
    signal_id: str
    signal_date: date
    block_key: str
    scenario_id: str
    take_profit_ticks: int
    stop_loss_ticks: int
    disposition: str
    outcome: BarCellOutcome | None

    def as_dict(self) -> dict[str, object]:
        return {
            "block_key": self.block_key,
            "candidate_key": self.candidate_key,
            "disposition": self.disposition,
            "outcome": _cell_outcome_payload(self.outcome),
            "scenario_id": self.scenario_id,
            "signal_date": self.signal_date.isoformat(),
            "signal_id": self.signal_id,
            "stop_loss_ticks": self.stop_loss_ticks,
            "take_profit_ticks": self.take_profit_ticks,
        }


def iter_bar_discovery_cell_ledger(
    result: BarDiscoveryResult,
    *,
    candidate_key: str,
):
    """Yield ordered cell dispositions without embedding them in summary JSON.

    The iterator reproduces the same safe occupancy comparison used by
    :class:`BarCandidateEconomicsAccumulator`: an entry is allowed only when
    its path index is strictly greater than the prior cell's exit index.
    """

    if not isinstance(result, BarDiscoveryResult):
        raise BarDiscoveryError("result must be a BarDiscoveryResult")
    try:
        candidate_result = next(
            item
            for item in result.candidate_results
            if item.candidate.candidate_key == candidate_key
        )
    except StopIteration as error:
        raise KeyError(candidate_key) from error
    bundles = {item.replay_key: item for item in result.replay_catalog}
    occupied: dict[tuple[str, int, int], tuple[int, int]] = {}
    for signal in candidate_result.matched_signals:
        for scenario_id in _SCENARIO_IDS:
            compact = None
            if signal.replay_key is not None:
                try:
                    compact = bundles[signal.replay_key].scenario(scenario_id)
                except KeyError as error:
                    raise BarDiscoveryError("matched signal references a missing replay") from error
            for take_profit in BARRIER_TICKS:
                for stop_loss in BARRIER_TICKS:
                    identity = (scenario_id, take_profit, stop_loss)
                    outcome: BarCellOutcome | None = None
                    if compact is None:
                        disposition = ENTRY_NOT_FILLED
                    else:
                        prior = occupied.get(identity)
                        if (
                            prior is not None
                            and prior[0] == compact.segment_id
                            and compact.entry_path_index <= prior[1]
                        ):
                            disposition = "SKIPPED_OCCUPIED"
                        else:
                            disposition = ENTRY_FILLED
                            outcome = compact.cell(take_profit, stop_loss)
                            occupied[identity] = (
                                compact.segment_id,
                                outcome.exit_path_index,
                            )
                    yield BarDiscoveryCellLedgerRecord(
                        candidate_key=candidate_key,
                        signal_id=signal.signal_id,
                        signal_date=signal.signal_date,
                        block_key=signal.block_key,
                        scenario_id=scenario_id,
                        take_profit_ticks=take_profit,
                        stop_loss_ticks=stop_loss,
                        disposition=disposition,
                        outcome=outcome,
                    )


@dataclass(slots=True)
class _CandidateScan:
    decision_trigger_count: int = 0
    evaluated_count: int = 0
    context_not_evaluable_count: int = 0
    context_rejections: Counter[str] | None = None
    failed_gates: Counter[str] | None = None
    matches: list[BarMatchedSignal] | None = None
    matched_signal_count: int = 0
    matched_daily_counts: Counter[date] | None = None
    matched_block_counts: Counter[str] | None = None

    def __post_init__(self) -> None:
        self.context_rejections = Counter()
        self.failed_gates = Counter()
        self.matches = []
        self.matched_daily_counts = Counter()
        self.matched_block_counts = Counter()


def _partitions_from_reports(
    reports: Sequence[DailyBarBuildReport],
) -> tuple[BarDiscoveryPartition, ...]:
    """Reproduce the manifest's quality-span state machine for build results.

    The complete ordered report stream is required because a non-selected
    source report is itself evidence of a quality break before the next active
    partition.  Missing raw calendar dates are absent from the stream and do
    not create a break.
    """

    active_contract: str | None = None
    outcome_span_id = 0
    break_before_next_active = False
    partitions: list[BarDiscoveryPartition] = []
    for report in reports:
        if not isinstance(report, DailyBarBuildReport):
            raise BarDiscoveryError("report sources must contain DailyBarBuildReport values")
        selected = report.plan.status.value == "SELECTED"
        contract = report.plan.selected_contract
        if not selected:
            if report.artifacts or contract is not None:
                raise BarDiscoveryError("unqualified report published an active partition")
            break_before_next_active = True
            continue
        if contract is None:
            raise BarDiscoveryError("selected report is missing its contract")
        if not report.artifacts:
            if active_contract is not None and contract != active_contract:
                break_before_next_active = True
            continue
        if outcome_span_id == 0 or break_before_next_active or contract != active_contract:
            outcome_span_id += 1
        active_contract = contract
        break_before_next_active = False
        partitions.append(
            BarDiscoveryPartition(
                source_date=report.plan.source_date,
                contract=contract,
                outcome_span_id=outcome_span_id,
                plan_sha256=report.plan.sha256,
                source_sha256=report.plan.source_sha256,
                artifacts=tuple(report.artifacts),
            )
        )
    return tuple(partitions)


def _source_partitions(
    source: BarDiscoverySource,
    split_plan: BarSplitPlan,
) -> tuple[tuple[BarDiscoveryPartition, ...], str | None, str | None]:
    dataset_sha256: str | None = None
    source_manifest_sha256: str | None = None
    if isinstance(source, BarDatasetBuildResult):
        if source.eligible_active_dates != split_plan.eligible_dates:
            raise BarDiscoveryError("dataset active dates differ from the frozen split plan")
        dataset_sha256 = _sha256(source.sha256, label="dataset build sha256")
        source_manifest_sha256 = _sha256(
            source.plan.source_manifest_sha256,
            label="dataset source manifest sha256",
        )
        partitions = list(_partitions_from_reports(source.reports))
    elif isinstance(source, LoadedBarDatasetManifest):
        if source.eligible_active_dates != split_plan.eligible_dates:
            raise BarDiscoveryError("loaded dataset active dates differ from the frozen split plan")
        dataset_sha256 = _sha256(
            source.dataset_manifest_sha256,
            label="loaded dataset manifest sha256",
        )
        source_manifest_sha256 = _sha256(
            source.source_manifest_sha256,
            label="loaded source manifest sha256",
        )
        partitions = list(source.partitions)
    elif isinstance(source, Sequence) and not isinstance(source, (str, bytes)):
        if all(isinstance(item, BarDatasetPartition) for item in source):
            partitions = list(source)
        elif all(isinstance(item, DailyBarBuildReport) for item in source):
            partitions = list(_partitions_from_reports(source))
        else:
            raise BarDiscoveryError("source sequence mixes unsupported partition values")
    else:
        raise BarDiscoveryError(
            "source must be a build result, loaded manifest, or report partitions"
        )
    dates = tuple(item.source_date for item in partitions)
    if dates != tuple(sorted(set(dates))):
        raise BarDiscoveryError("active source partitions must be unique and date ordered")
    return tuple(partitions), dataset_sha256, source_manifest_sha256


def _result_source_identity(
    source: BarDiscoverySource,
    visible_partitions: Sequence[BarDiscoveryPartition],
) -> str:
    """Preserve the authoritative dataset identity when one is available."""

    if isinstance(source, LoadedBarDatasetManifest):
        return _sha256(
            source.dataset_manifest_sha256,
            label="loaded dataset result source sha256",
        )
    if isinstance(source, BarDatasetBuildResult):
        return _sha256(source.sha256, label="built dataset result source sha256")
    return hashlib.sha256(
        _canonical_bytes([_partition_identity(item) for item in visible_partitions])
    ).hexdigest()


_MATCH_SCHEMA = pa.schema(
    (
        pa.field("candidate_key", pa.string(), nullable=False),
        pa.field("signal_id", pa.string(), nullable=False),
        pa.field("signal_date", pa.date32(), nullable=False),
        pa.field("decision_ns", pa.int64(), nullable=False),
        pa.field("block_key", pa.string(), nullable=False),
        pa.field("outcome_span_id", pa.int64(), nullable=False),
        pa.field("entry_status", pa.string(), nullable=False),
        pa.field("no_fill_reason", pa.string(), nullable=True),
        pa.field("entry_path_index", pa.int64(), nullable=True),
        pa.field("entry_1s_start_ns", pa.int64(), nullable=True),
        pa.field("replay_key", pa.string(), nullable=True),
        pa.field("evaluation_json", pa.large_string(), nullable=False),
    )
)
_REPLAY_SCHEMA = pa.schema(
    (
        pa.field("replay_key", pa.string(), nullable=False),
        pa.field("decision_ns", pa.int64(), nullable=False),
        pa.field("bundle_json", pa.large_string(), nullable=False),
    )
)


class _EvidenceWriter:
    """Publish evidence through the repository's held-dirfd artifact primitive."""

    def __init__(
        self,
        *,
        data_root: Path | str,
        source_identity_sha256: str,
        dataset_build_sha256: str | None,
        source_manifest_sha256: str | None,
        split_plan_sha256: str,
        candidate_catalog_sha256: str,
    ) -> None:
        identity_document = {
            "candidate_catalog_sha256": candidate_catalog_sha256,
            "config_semantic_sha256": BAR_PATTERN_CONFIG_SEMANTIC_SHA256,
            "dataset_build_sha256": dataset_build_sha256,
            "evidence_schema": DISCOVERY_EVIDENCE_SCHEMA,
            "outcome_span_policy_sha256": BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
            "source_identity_sha256": source_identity_sha256,
            "source_manifest_sha256": source_manifest_sha256,
            "split_plan_sha256": split_plan_sha256,
            "spool_version": DISCOVERY_SPOOL_VERSION,
        }
        self.identity_sha256 = hashlib.sha256(_canonical_bytes(identity_document)).hexdigest()
        requested = Path(data_root).expanduser()
        if requested.is_symlink():
            raise BarDiscoveryError("data_root cannot be a symlink")
        self.data_root = requested.resolve(strict=True)
        if not self.data_root.is_dir() or self.data_root.name != "data":
            raise BarDiscoveryError(
                "production evidence data_root must be the project data directory"
            )
        self.project_root = self.data_root.parent
        if (self.project_root / "data").resolve(strict=True) != self.data_root:
            raise BarDiscoveryError("data_root is not bound below its project root")
        self.source_identity_sha256 = source_identity_sha256
        self.dataset_build_sha256 = dataset_build_sha256
        self.source_manifest_sha256 = source_manifest_sha256
        self.bound_dataset_sha256 = dataset_build_sha256 or source_identity_sha256
        self.bound_source_manifest_sha256 = source_manifest_sha256 or source_identity_sha256
        self.split_plan_sha256 = split_plan_sha256
        self.candidate_catalog_sha256 = candidate_catalog_sha256
        self.metadata = {
            b"systematic_fx.candidate_catalog_sha256": candidate_catalog_sha256.encode("ascii"),
            b"systematic_fx.config_semantic_sha256": (
                BAR_PATTERN_CONFIG_SEMANTIC_SHA256.encode("ascii")
            ),
            b"systematic_fx.dataset_build_sha256": (self.bound_dataset_sha256.encode("ascii")),
            b"systematic_fx.evidence_identity_sha256": self.identity_sha256.encode("ascii"),
            b"systematic_fx.evidence_schema": DISCOVERY_EVIDENCE_SCHEMA.encode("ascii"),
            b"systematic_fx.outcome_span_policy_sha256": (
                BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256.encode("ascii")
            ),
            b"systematic_fx.source_identity_sha256": source_identity_sha256.encode("ascii"),
            b"systematic_fx.source_manifest_sha256": (
                self.bound_source_manifest_sha256.encode("ascii")
            ),
            b"systematic_fx.split_plan_sha256": split_plan_sha256.encode("ascii"),
        }
        self.shards: list[BarDiscoveryEvidenceShard] = []
        self._shard_counts: Counter[tuple[str, int, str]] = Counter()

    def _publish_table(
        self,
        *,
        record_kind: str,
        segment_id: int,
        contract: str,
        records: Sequence[Mapping[str, object]],
    ) -> None:
        if not records:
            return
        schema = (_MATCH_SCHEMA if record_kind == "matches" else _REPLAY_SCHEMA).with_metadata(
            self.metadata
        )
        table = pa.Table.from_pylist(list(records), schema=schema)
        shard_counter_key = record_kind, segment_id, contract
        shard_ordinal = self._shard_counts[shard_counter_key]
        self._shard_counts[shard_counter_key] += 1
        decisions = [int(item["decision_ns"]) for item in records]
        artifact_descriptor = BarArtifactDescriptor(
            artifact_key=(
                f"bar_pattern_discovery_v1:evidence:{record_kind}:"
                f"span{segment_id}:shard{shard_ordinal:06d}:"
                f"{hashlib.sha256(contract.encode('ascii')).hexdigest()}"
            ),
            artifact_type=f"bar_discovery_{record_kind}_shard",
            artifact_schema=DISCOVERY_EVIDENCE_SCHEMA,
            artifact_version=1,
            record_count=table.num_rows,
            schema_sha256=arrow_schema_sha256(schema),
            source_manifest_sha256=self.bound_source_manifest_sha256,
            logical_identity={
                "candidate_catalog_sha256": self.candidate_catalog_sha256,
                "config_semantic_sha256": BAR_PATTERN_CONFIG_SEMANTIC_SHA256,
                "contract": contract,
                "dataset_manifest_sha256": self.bound_dataset_sha256,
                "evidence_identity_sha256": self.identity_sha256,
                "first_decision_ns": min(decisions),
                "last_decision_ns": max(decisions),
                "record_kind": record_kind,
                "row_count": table.num_rows,
                "outcome_span_policy_sha256": BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
                "schema_sha256": arrow_schema_sha256(schema),
                "shard_ordinal": shard_ordinal,
                "segment_id": segment_id,
                "source_identity_sha256": self.source_identity_sha256,
                "split_plan_sha256": self.split_plan_sha256,
            },
            media_type="application/vnd.apache.parquet",
            file_suffix=".parquet",
        )
        artifact = publish_bar_parquet_table(
            self.project_root,
            artifact_descriptor,
            table,
        )
        self.shards.append(
            BarDiscoveryEvidenceShard(
                record_kind=record_kind,
                segment_id=segment_id,
                shard_ordinal=shard_ordinal,
                contract=contract,
                first_decision_ns=min(decisions),
                last_decision_ns=max(decisions),
                row_count=table.num_rows,
                relative_uri=artifact.path.relative_to(self.project_root).as_posix(),
                sha256=artifact.sha256,
                byte_size=artifact.byte_size,
                artifact=artifact,
            )
        )

    def write_segment(
        self,
        *,
        segment_id: int,
        contract: str,
        matches: Sequence[Mapping[str, object]],
        replays: Sequence[Mapping[str, object]],
    ) -> None:
        ordered_matches = tuple(
            sorted(
                matches,
                key=lambda item: (
                    int(item["decision_ns"]),
                    str(item["candidate_key"]),
                    str(item["signal_id"]),
                ),
            )
        )
        ordered_replays = tuple(
            sorted(replays, key=lambda item: (int(item["decision_ns"]), str(item["replay_key"])))
        )
        self._publish_table(
            record_kind="matches",
            segment_id=segment_id,
            contract=contract,
            records=ordered_matches,
        )
        self._publish_table(
            record_kind="replays",
            segment_id=segment_id,
            contract=contract,
            records=ordered_replays,
        )

    def finalize(self) -> BarDiscoveryEvidenceManifest:
        shards = tuple(
            sorted(
                self.shards,
                key=lambda item: (
                    item.first_decision_ns if item.first_decision_ns is not None else -1,
                    item.segment_id,
                    item.record_kind,
                    item.shard_ordinal,
                ),
            )
        )
        document = {
            "candidate_catalog_sha256": self.candidate_catalog_sha256,
            "config_semantic_sha256": BAR_PATTERN_CONFIG_SEMANTIC_SHA256,
            "dataset_build_sha256": self.dataset_build_sha256,
            "evidence_identity_sha256": self.identity_sha256,
            "evidence_schema": DISCOVERY_EVIDENCE_SCHEMA,
            "outcome_span_policy_sha256": BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
            "schema": DISCOVERY_EVIDENCE_SCHEMA,
            "shards": [item.as_dict() for item in shards],
            "source_identity_sha256": self.source_identity_sha256,
            "source_manifest_sha256": self.source_manifest_sha256,
            "split_plan_sha256": self.split_plan_sha256,
            "spool_version": DISCOVERY_SPOOL_VERSION,
        }
        manifest_schema_sha256 = hashlib.sha256(
            _canonical_bytes(
                {
                    "evidence_schema": DISCOVERY_EVIDENCE_SCHEMA,
                    "record": "manifest_with_verified_parquet_shards",
                    "version": 1,
                }
            )
        ).hexdigest()
        manifest_descriptor = BarArtifactDescriptor(
            artifact_key=f"bar_pattern_discovery_v1:evidence:manifest:{self.identity_sha256}",
            artifact_type="bar_discovery_evidence_manifest",
            artifact_schema=DISCOVERY_EVIDENCE_SCHEMA,
            artifact_version=1,
            record_count=len(shards),
            schema_sha256=manifest_schema_sha256,
            source_manifest_sha256=self.bound_source_manifest_sha256,
            logical_identity={
                "candidate_catalog_sha256": self.candidate_catalog_sha256,
                "config_semantic_sha256": BAR_PATTERN_CONFIG_SEMANTIC_SHA256,
                "dataset_manifest_sha256": self.bound_dataset_sha256,
                "evidence_identity_sha256": self.identity_sha256,
                "outcome_span_policy_sha256": BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
                "record_count": len(shards),
                "schema_sha256": manifest_schema_sha256,
                "source_identity_sha256": self.source_identity_sha256,
                "split_plan_sha256": self.split_plan_sha256,
            },
            media_type="application/json",
            file_suffix=".json",
        )
        artifact = publish_bar_json_artifact(
            self.project_root,
            manifest_descriptor,
            document,
        )
        return BarDiscoveryEvidenceManifest(
            evidence_identity_sha256=self.identity_sha256,
            source_identity_sha256=self.source_identity_sha256,
            source_manifest_sha256=self.source_manifest_sha256,
            dataset_build_sha256=self.dataset_build_sha256,
            outcome_span_policy_sha256=BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
            split_plan_sha256=self.split_plan_sha256,
            config_semantic_sha256=BAR_PATTERN_CONFIG_SEMANTIC_SHA256,
            candidate_catalog_sha256=self.candidate_catalog_sha256,
            shards=shards,
            relative_uri=artifact.path.relative_to(self.project_root).as_posix(),
            sha256=artifact.sha256,
            byte_size=artifact.byte_size,
            artifact=artifact,
        )


def iter_bar_discovery_evidence_records(
    data_root: Path | str,
    manifest: BarDiscoveryEvidenceManifest,
    *,
    record_kind: str,
    candidate_key: str | None = None,
):
    """Verify and stream retained match/replay records from Parquet shards."""

    if record_kind not in {"matches", "replays"}:
        raise BarDiscoveryError("record_kind must be matches or replays")
    if candidate_key is not None and record_kind != "matches":
        raise BarDiscoveryError("candidate filtering is available only for match records")
    root = Path(data_root).expanduser().resolve(strict=True)
    if root.name != "data" or not root.is_dir():
        raise BarDiscoveryError("evidence reader requires the project data directory")
    project_root = root.parent
    with open_verified_bar_artifact(project_root, manifest.artifact) as opened_manifest:
        os.lseek(opened_manifest.descriptor, 0, os.SEEK_SET)
        raw = bytearray()
        while chunk := os.read(opened_manifest.descriptor, 1024 * 1024):
            raw.extend(chunk)
        document = json.loads(raw)
        if (
            not isinstance(document, dict)
            or document.get("schema") != DISCOVERY_EVIDENCE_SCHEMA
            or document.get("evidence_identity_sha256") != manifest.evidence_identity_sha256
            or document.get("shards") != [item.as_dict() for item in manifest.shards]
        ):
            raise BarDiscoveryError("verified evidence manifest payload drift")
    for shard in manifest.shards:
        if shard.record_kind != record_kind:
            continue
        with (
            open_verified_bar_artifact(project_root, shard.artifact) as opened,
            os.fdopen(os.dup(opened.descriptor), "rb") as source,
        ):
            parquet = pq.ParquetFile(source)
            if (
                parquet.metadata.num_rows != shard.row_count
                or arrow_schema_sha256(parquet.schema_arrow)
                != shard.artifact.descriptor.schema_sha256
            ):
                raise BarDiscoveryError("verified evidence shard schema/row drift")
            table = parquet.read()
        if candidate_key is not None:
            table = table.filter(pc.equal(table["candidate_key"], candidate_key))
        for record in table.to_pylist():
            if record_kind == "matches":
                record["evaluation"] = json.loads(record.pop("evaluation_json"))
            else:
                record["bundle"] = json.loads(record.pop("bundle_json"))
            yield record


def _range_dates(split_plan: BarSplitPlan, value: BarDateRange) -> tuple[date, ...]:
    start = value.start_active_ordinal - 1
    end = value.end_active_ordinal
    dates = split_plan.eligible_dates[start:end]
    if (
        not dates
        or dates[0] != value.start_date
        or dates[-1] != value.end_date
        or len(dates) != value.active_day_count
    ):
        raise BarDiscoveryError(f"split range {value.split_key} differs from active ordinals")
    return dates


def _split_maps(
    split_plan: BarSplitPlan,
) -> tuple[tuple[date, ...], tuple[date, ...], dict[date, str]]:
    if not isinstance(split_plan, BarSplitPlan):
        raise BarDiscoveryError("split_plan must be a BarSplitPlan")
    discovery_dates = _range_dates(split_plan, split_plan.discovery)
    decision_end = split_plan.discovery.decision_end_date
    if decision_end is None or decision_end not in discovery_dates:
        raise BarDiscoveryError("Discovery must have an in-range decision boundary")
    decision_dates = discovery_dates[: discovery_dates.index(decision_end) + 1]
    block_by_date: dict[date, str] = {}
    for block in split_plan.discovery_reporting_blocks:
        if block.role != "DISCOVERY_REPORTING_BLOCK":
            raise BarDiscoveryError("Discovery block role drift")
        for source_date in _range_dates(split_plan, block):
            if source_date in block_by_date:
                raise BarDiscoveryError("Discovery reporting blocks overlap")
            block_by_date[source_date] = block.split_key
    if set(block_by_date) != set(decision_dates):
        raise BarDiscoveryError("Discovery reporting blocks do not partition decision dates")
    if len(split_plan.discovery_reporting_blocks) != 4:
        raise BarDiscoveryError("Discovery requires exactly four reporting blocks")
    return discovery_dates, decision_dates, block_by_date


def _memory_plan_from_partitions(
    partitions: Sequence[BarDiscoveryPartition],
    *,
    discovery_dates: Sequence[date],
) -> BarDiscoveryMemoryPlan:
    visible = set(discovery_dates)
    counts_by_span: dict[tuple[int, str], Counter[int]] = defaultdict(Counter)
    for partition in partitions:
        if partition.source_date not in visible:
            continue
        span_counts = counts_by_span[partition.outcome_span_id, partition.contract]
        for artifact in partition.artifacts:
            if artifact.timeframe_seconds in _REQUIRED_TIMEFRAMES:
                span_counts[artifact.timeframe_seconds] += artifact.row_count
    if not counts_by_span:
        raise BarDiscoveryError("Discovery memory plan has no visible outcome span")
    maximum_key, maximum_counts = max(
        counts_by_span.items(),
        key=lambda item: (sum(item[1].values()), item[0]),
    )
    cell_count = ALLOCATED_VARIANT_COUNT * len(_SCENARIO_IDS) * len(BARRIER_TICKS) ** 2
    return BarDiscoveryMemoryPlan(
        visible_active_date_count=len(discovery_dates),
        outcome_span_count=len(counts_by_span),
        maximum_outcome_span_id=maximum_key[0],
        maximum_outcome_span_contract=maximum_key[1],
        maximum_outcome_span_bar_counts=tuple(
            (timeframe, maximum_counts[timeframe]) for timeframe in _REQUIRED_TIMEFRAMES
        ),
        maximum_outcome_span_total_bar_count=sum(maximum_counts.values()),
        concurrent_cell_accumulator_count=cell_count,
        final_summary_cell_count=cell_count,
        maximum_materialized_replay_cell_count=(2 * len(_SCENARIO_IDS) * len(BARRIER_TICKS) ** 2),
        maximum_buffered_match_record_count=_MATCH_SHARD_MAX_RECORDS,
        maximum_buffered_replay_record_count=_REPLAY_SHARD_MAX_RECORDS,
    )


def plan_streaming_bar_discovery_memory(
    source: LoadedBarDatasetManifest,
    *,
    split_plan: BarSplitPlan,
) -> BarDiscoveryMemoryPlan:
    """Return production memory caps without opening any bar artifact."""

    if not isinstance(source, LoadedBarDatasetManifest):
        raise BarDiscoveryError("memory planning requires LoadedBarDatasetManifest")
    discovery_dates, _decision_dates, _block_by_date = _split_maps(split_plan)
    partitions, _dataset_sha256, _source_manifest_sha256 = _source_partitions(
        source,
        split_plan,
    )
    return _memory_plan_from_partitions(
        partitions,
        discovery_dates=discovery_dates,
    )


def _load_discovery_bars(
    *,
    data_root: Path | str,
    partitions: Sequence[BarDiscoveryPartition],
    discovery_dates: Sequence[date],
    artifact_loader: _ArtifactLoader,
) -> tuple[
    dict[tuple[int, int, str], tuple[_DiscoveryBarLike, ...]],
    tuple[date, ...],
    tuple[tuple[int, int], ...],
]:
    by_date = {item.source_date: item for item in partitions}
    missing = [item for item in discovery_dates if item not in by_date]
    if missing:
        raise BarDiscoveryError(f"Discovery is missing {len(missing)} active daily bar partitions")
    grouped: dict[tuple[int, int, str], list[_DiscoveryBarLike]] = defaultdict(list)
    counts: Counter[int] = Counter()
    for source_date in discovery_dates:
        partition = by_date[source_date]
        artifacts = {item.timeframe_seconds: item for item in partition.artifacts}
        if not set(_REQUIRED_TIMEFRAMES).issubset(artifacts):
            raise BarDiscoveryError("daily partition does not bind every required timeframe")
        for timeframe in _REQUIRED_TIMEFRAMES:
            descriptor = artifacts[timeframe]
            bars = artifact_loader(
                data_root,
                descriptor,
                expected_plan_sha256=partition.plan_sha256,
                expected_source_sha256=partition.source_sha256,
                expected_source_date=source_date,
            )
            for bar in bars:
                if (
                    bar.timeframe_seconds != timeframe
                    or bar.source_date != source_date
                    or bar.contract != partition.contract
                    or bar.segment_id <= 0
                ):
                    raise BarDiscoveryError("loaded bar differs from its bound daily artifact")
                grouped[(timeframe, bar.segment_id, bar.contract)].append(bar)
                counts[timeframe] += 1
    canonical: dict[tuple[int, int, str], tuple[_DiscoveryBarLike, ...]] = {}
    for key, values in grouped.items():
        ordered = tuple(sorted(values, key=lambda item: item.start_ns))
        starts = tuple(item.start_ns for item in ordered)
        if starts != tuple(sorted(set(starts))):
            raise BarDiscoveryError("a grouped segment has duplicate or unordered bars")
        canonical[key] = ordered
    return (
        canonical,
        tuple(discovery_dates),
        tuple((timeframe, counts[timeframe]) for timeframe in _REQUIRED_TIMEFRAMES),
    )


def _context_rejection(error: BarPatternError) -> str:
    message = str(error)
    expected = {
        "insufficient completed history for the frozen ATR window": "INSUFFICIENT_HISTORY",
        "pattern context contains a missing or overlapping signal bar": (
            "NONCONTIGUOUS_SIGNAL_CONTEXT"
        ),
        "ATR denominator is zero": "ZERO_ATR",
        "setup efficiency denominator is zero": "ZERO_SETUP_TRUE_RANGE",
        "trigger body/location denominator is zero": "ZERO_TRIGGER_RANGE",
    }
    reason = expected.get(message)
    if reason is None:
        raise BarDiscoveryError(f"unexpected pattern-context failure: {message}") from error
    return reason


def _entry_path_index(
    *,
    context: BarPatternContext,
    signal_bars: Sequence[_DiscoveryBarLike],
    path: BarPathIndex,
    path_index_by_start_ns: _BarStartLookup,
) -> tuple[int, int] | None:
    if context.next_open is None:
        return None
    entry_signal_bar = signal_bars[context.next_open.source_index]
    if entry_signal_bar.open_ticks != context.next_open.open_ticks:
        raise BarDiscoveryError("next-open context differs from its signal bar")
    expected_start = entry_signal_bar.first_trade_ns // _ONE_SECOND_NS * _ONE_SECOND_NS
    try:
        path_index = path_index_by_start_ns[expected_start]
    except KeyError as error:
        raise BarDiscoveryError("next signal-bar open has no corresponding 1s bar") from error
    entry_bar = path.bars[path_index]
    if (
        entry_bar.open_ticks != entry_signal_bar.open_ticks
        or not entry_signal_bar.start_ns <= entry_bar.start_ns < entry_signal_bar.end_ns
    ):
        raise BarDiscoveryError("next signal-bar open and corresponding 1s bar disagree")
    return path_index, entry_bar.start_ns


def _signal_id(evaluation: BarPatternEvaluation) -> str:
    context = evaluation.context
    payload = (
        f"{evaluation.candidate.candidate_key}|{context.segment_id}|"
        f"{context.contract}|{context.trigger_bar.start_ns}"
    ).encode("ascii")
    return hashlib.sha256(payload).hexdigest()


def _replay_key(
    *,
    context: BarPatternContext,
    entry_path_index: int,
    outcome_span_id: int | None = None,
) -> str:
    path_identity = context.segment_id if outcome_span_id is None else outcome_span_id
    payload = (
        f"{context.direction.value}|{path_identity}|{context.contract}|{entry_path_index}"
    ).encode("ascii")
    return hashlib.sha256(payload).hexdigest()


def _compact_hit(value: BarThresholdHit, path: BarPathIndex) -> BarCompactThresholdHit:
    return BarCompactThresholdHit(
        distance_ticks=value.distance_ticks,
        trigger_price_ticks=value.trigger_price_ticks,
        path_index=value.path_index,
        bar_start_ns=value.bar_start_ns,
        observed_bar_open_ticks=(
            None if value.path_index is None else path.bars[value.path_index].open_ticks
        ),
    )


def _compact_surface(
    surface: BarSignalSurface,
    path: BarPathIndex,
) -> BarCompactScenarioReplay:
    terminal = path.bars[surface.terminal_path_index]
    entry = path.bars[surface.entry_path_index]
    compact = BarCompactScenarioReplay(
        scenario_id=surface.scenario_id,
        direction=surface.direction,
        segment_id=surface.segment_id,
        contract=surface.contract,
        entry_path_index=surface.entry_path_index,
        entry_1s_start_ns=entry.start_ns,
        entry_fill_price_ticks=surface.entry_fill_price_ticks,
        terminal_path_index=surface.terminal_path_index,
        terminal_1s_start_ns=terminal.start_ns,
        terminal_close_ticks=terminal.close_ticks,
        take_profit_hits=tuple(_compact_hit(item, path) for item in surface.take_profit_hits),
        stop_hits=tuple(_compact_hit(item, path) for item in surface.stop_hits),
    )
    # A compact replay is evidence only if it regenerates every original cell.
    if compact.to_surface().cells != surface.cells:
        raise BarDiscoveryError("compact replay does not reconstruct its 484-cell surface")
    return compact


def _median_counts(values: Sequence[int]) -> Fraction:
    if not values:
        raise BarDiscoveryError("cannot compute support on an empty decision calendar")
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return Fraction(ordered[middle], 1)
    return Fraction(ordered[middle - 1] + ordered[middle], 2)


def _support(
    candidate: BarPatternCandidate,
    matches: Sequence[BarMatchedSignal],
    *,
    decision_dates: Sequence[date],
    block_keys: Sequence[str],
) -> BarSupportEvidence:
    daily = Counter(item.signal_date for item in matches)
    blocks = Counter(item.block_key for item in matches)
    median = _median_counts([daily[item] for item in decision_dates])
    return BarSupportEvidence(
        candidate_key=candidate.candidate_key,
        timeframe_seconds=candidate.timeframe_seconds,
        direction=candidate.direction,
        raw_signal_count=len(matches),
        distinct_signal_day_count=len(daily),
        block_signal_counts=tuple(blocks[item] for item in block_keys),  # type: ignore[arg-type]
        median_signals_per_active_day_numerator=median.numerator,
        median_signals_per_active_day_denominator=median.denominator,
    )


def _support_from_scan(
    candidate: BarPatternCandidate,
    scan: _CandidateScan,
    *,
    decision_dates: Sequence[date],
    block_keys: Sequence[str],
) -> BarSupportEvidence:
    if scan.matched_daily_counts is None or scan.matched_block_counts is None:
        raise AssertionError("candidate support counters were not initialized")
    median = _median_counts([scan.matched_daily_counts[item] for item in decision_dates])
    return BarSupportEvidence(
        candidate_key=candidate.candidate_key,
        timeframe_seconds=candidate.timeframe_seconds,
        direction=candidate.direction,
        raw_signal_count=scan.matched_signal_count,
        distinct_signal_day_count=len(scan.matched_daily_counts),
        block_signal_counts=tuple(scan.matched_block_counts[item] for item in block_keys),  # type: ignore[arg-type]
        median_signals_per_active_day_numerator=median.numerator,
        median_signals_per_active_day_denominator=median.denominator,
    )


def _economic_attribution(
    signal: BarMatchedSignal,
    *,
    block_by_date: Mapping[date, str],
) -> tuple[str, str]:
    """Attribute filled economics to actual entry time, not decision time.

    Discovery reporting blocks partition decision dates.  An entry occurring
    on a market-closed/non-decision date inherits the most recent reporting
    block (and post-decision outcome-tail entries remain in block four).  UTC
    month always comes directly from the actual 1s entry timestamp.  A no-fill
    has no entry timestamp, so its accounting disposition uses the signal day.
    """

    attribution_date = signal.signal_date
    if signal.entry_1s_start_ns is not None:
        attribution_date = datetime.fromtimestamp(
            signal.entry_1s_start_ns // _ONE_SECOND_NS,
            tz=UTC,
        ).date()
    ordered_dates = tuple(sorted(block_by_date))
    eligible = [item for item in ordered_dates if item <= attribution_date]
    block_date = eligible[-1] if eligible else ordered_dates[0]
    month = (
        signal.signal_date.strftime("%Y-%m")
        if signal.entry_1s_start_ns is None
        else datetime.fromtimestamp(
            signal.entry_1s_start_ns // _ONE_SECOND_NS,
            tz=UTC,
        ).strftime("%Y-%m")
    )
    return block_by_date[block_date], month


def _candidate_economics(
    candidate: BarPatternCandidate,
    matches: Sequence[BarMatchedSignal],
    *,
    paths: Mapping[tuple[int, str], BarPathIndex],
    block_keys: Sequence[str],
    block_by_date: Mapping[date, str],
    observed_months: Sequence[str],
    replay_cache: dict[str, BarCompactReplayBundle],
) -> tuple[BarScenarioDiscoveryEconomics, ...]:
    results: list[BarScenarioDiscoveryEconomics] = []
    for scenario_id in _SCENARIO_IDS:
        accumulator = BarCandidateEconomicsAccumulator(
            scenario_id=scenario_id,
            direction=candidate.direction,
            block_keys=block_keys,
            observed_utc_months=observed_months,
        )
        for signal in matches:
            surface = None
            if signal.entry_status == ENTRY_FILLED:
                if signal.entry_path_index is None or signal.replay_key is None:
                    raise AssertionError("filled signal lost its path index")
                path = paths.get(signal.segment_key)
                if path is None:
                    raise BarDiscoveryError("matched signal lost its 1s segment path")
                bundle = replay_cache.get(signal.replay_key)
                if bundle is None:
                    compact_scenarios: list[BarCompactScenarioReplay] = []
                    for cached_scenario_id in _SCENARIO_IDS:
                        cached_surface = replay_bar_signal(
                            path,
                            entry_path_index=signal.entry_path_index,
                            direction=candidate.direction,
                            scenario=BAR_EXECUTION_SCENARIOS[cached_scenario_id],
                        )
                        compact_scenarios.append(_compact_surface(cached_surface, path))
                    bundle = BarCompactReplayBundle(
                        replay_key=signal.replay_key,
                        scenarios=tuple(compact_scenarios),
                    )
                    replay_cache[signal.replay_key] = bundle
                compact = bundle.scenario(scenario_id)
                if (
                    compact.direction is not candidate.direction
                    or compact.entry_path_index != signal.entry_path_index
                    or (compact.segment_id, compact.contract) != signal.segment_key
                ):
                    raise BarDiscoveryError("shared compact replay identity drift")
                surface = compact.to_surface()
            economic_block, signal_month = _economic_attribution(
                signal,
                block_by_date=block_by_date,
            )
            accumulator.add(
                CandidateSignalReplay(
                    signal_id=signal.signal_id,
                    signal_ts_ns=signal.decision_ns,
                    block_key=economic_block,
                    utc_month=signal_month,
                    surface=surface,
                    no_fill_reason=(None if surface is not None else signal.no_fill_reason),
                )
            )
        results.append(
            BarScenarioDiscoveryEconomics(
                scenario_id=scenario_id,
                cells=accumulator.finalize(),
            )
        )
    return tuple(results)


def _streaming_accumulators(
    candidate: BarPatternCandidate,
    *,
    accumulators: dict[tuple[str, str], BarCandidateEconomicsAccumulator],
    block_keys: Sequence[str],
    observed_months: Sequence[str],
) -> tuple[BarCandidateEconomicsAccumulator, ...]:
    values: list[BarCandidateEconomicsAccumulator] = []
    for scenario_id in _SCENARIO_IDS:
        key = candidate.candidate_key, scenario_id
        accumulator = accumulators.get(key)
        if accumulator is None:
            accumulator = BarCandidateEconomicsAccumulator(
                scenario_id=scenario_id,
                direction=candidate.direction,
                block_keys=block_keys,
                observed_utc_months=observed_months,
            )
            accumulators[key] = accumulator
        values.append(accumulator)
    return tuple(values)


def _scan_streaming_outcome_span(
    *,
    outcome_span_key: tuple[int, str],
    bars_by_timeframe: Mapping[int, Sequence[_DiscoveryBarLike]],
    by_dimensions: Mapping[tuple[int, int, object], Sequence[BarPatternCandidate]],
    scans: Mapping[str, _CandidateScan],
    decision_set: set[date],
    block_by_date: Mapping[date, str],
    block_keys: Sequence[str],
    observed_months: Sequence[str],
    accumulators: dict[tuple[str, str], BarCandidateEconomicsAccumulator],
    evidence_writer: _EvidenceWriter | None = None,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Scan one manifest quality span while preserving narrower signal segments."""

    outcome_span_id, contract = outcome_span_key
    source_one_second = tuple(sorted(bars_by_timeframe.get(1, ()), key=lambda item: item.start_ns))
    if not source_one_second:
        raise BarDiscoveryError("streamed outcome span has no 1s path")
    one_second = tuple(
        item
        if item.segment_id == outcome_span_id
        else _OutcomePathBar.from_bar(item, outcome_span_id=outcome_span_id)
        for item in source_one_second
    )
    path = BarPathIndex(one_second)
    if (path.segment_id, path.contract) != outcome_span_key:
        raise BarDiscoveryError("streamed outcome path identity drift")
    path_index_by_start = _BarStartLookup(path.bars)
    match_records: list[dict[str, object]] = []
    replay_records: list[dict[str, object]] = []
    emitted_replay_keys: set[str] = set()

    def flush_records(*, force: bool = False) -> None:
        if evidence_writer is None:
            return
        matches = (
            tuple(match_records) if force or len(match_records) >= _MATCH_SHARD_MAX_RECORDS else ()
        )
        replays = (
            tuple(replay_records)
            if force or len(replay_records) >= _REPLAY_SHARD_MAX_RECORDS
            else ()
        )
        if matches or replays:
            evidence_writer.write_segment(
                segment_id=outcome_span_id,
                contract=contract,
                matches=matches,
                replays=replays,
            )
        if matches:
            match_records.clear()
        if replays:
            replay_records.clear()

    for timeframe in SIGNAL_TIMEFRAMES_SECONDS:
        signal_groups: dict[tuple[int, str], list[_DiscoveryBarLike]] = defaultdict(list)
        for bar in bars_by_timeframe.get(timeframe, ()):
            signal_groups[(bar.segment_id, bar.contract)].append(bar)
        ordered_signal_groups = sorted(
            (
                (
                    key,
                    tuple(sorted(values, key=lambda item: item.start_ns)),
                )
                for key, values in signal_groups.items()
            ),
            key=lambda item: (item[1][0].start_ns, item[0]),
        )
        dimensions = tuple(
            (lookback, direction, candidates)
            for (width, lookback, direction), candidates in by_dimensions.items()
            if width == timeframe
        )
        for signal_segment_key, signal_bars in ordered_signal_groups:
            if signal_segment_key[1] != contract:
                raise BarDiscoveryError("signal segment crosses the outcome-span contract")
            for trigger_index, trigger_bar in enumerate(signal_bars):
                if trigger_bar.source_date not in decision_set:
                    continue
                block_key = block_by_date[trigger_bar.source_date]
                surfaces_for_trigger: dict[str, dict[str, BarSignalSurface]] = {}
                for lookback, direction, dimension_candidates in dimensions:
                    for candidate in dimension_candidates:
                        scans[candidate.candidate_key].decision_trigger_count += 1
                    try:
                        context = build_bar_pattern_context(
                            signal_bars,
                            trigger_index=trigger_index,
                            setup_lookback_bars=lookback,
                            direction=direction,
                        )
                    except BarPatternError as error:
                        reason = _context_rejection(error)
                        for candidate in dimension_candidates:
                            scan = scans[candidate.candidate_key]
                            scan.context_not_evaluable_count += 1
                            if scan.context_rejections is None:
                                raise AssertionError
                            scan.context_rejections[reason] += 1
                        continue
                    entry = _entry_path_index(
                        context=context,
                        signal_bars=signal_bars,
                        path=path,
                        path_index_by_start_ns=path_index_by_start,
                    )
                    for candidate in dimension_candidates:
                        scan = scans[candidate.candidate_key]
                        evaluation = evaluate_bar_pattern_context(context, candidate=candidate)
                        scan.evaluated_count += 1
                        if scan.failed_gates is None:
                            raise AssertionError
                        scan.failed_gates.update(evaluation.failed_gate_ids)
                        if not evaluation.matched:
                            continue
                        scan.matched_signal_count += 1
                        if scan.matched_daily_counts is None or scan.matched_block_counts is None:
                            raise AssertionError
                        scan.matched_daily_counts[trigger_bar.source_date] += 1
                        scan.matched_block_counts[block_key] += 1
                        if entry is None:
                            entry_status = ENTRY_NOT_FILLED
                            no_fill_reason = NEXT_SIGNAL_BAR_UNAVAILABLE
                            entry_path_index = None
                            entry_start_ns = None
                            replay_key = None
                            surfaces = None
                        else:
                            entry_status = ENTRY_FILLED
                            no_fill_reason = None
                            entry_path_index, entry_start_ns = entry
                            replay_key = _replay_key(
                                context=context,
                                entry_path_index=entry_path_index,
                                outcome_span_id=outcome_span_id,
                            )
                            surfaces = surfaces_for_trigger.get(replay_key)
                            if surfaces is None:
                                surfaces = {}
                                for scenario_id in _SCENARIO_IDS:
                                    surface = replay_bar_signal(
                                        path,
                                        entry_path_index=entry_path_index,
                                        direction=candidate.direction,
                                        scenario=BAR_EXECUTION_SCENARIOS[scenario_id],
                                    )
                                    surfaces[scenario_id] = surface
                                surfaces_for_trigger[replay_key] = surfaces
                                if replay_key not in emitted_replay_keys:
                                    bundle = BarCompactReplayBundle(
                                        replay_key=replay_key,
                                        scenarios=tuple(
                                            _compact_surface(surfaces[item], path)
                                            for item in _SCENARIO_IDS
                                        ),
                                    )
                                    replay_records.append(
                                        {
                                            "bundle_json": _canonical_bytes(bundle.as_dict())
                                            .decode("ascii")
                                            .rstrip("\n"),
                                            "decision_ns": evaluation.context.decision_ns,
                                            "replay_key": replay_key,
                                        }
                                    )
                                    emitted_replay_keys.add(replay_key)
                        signal = BarMatchedSignal(
                            signal_id=_signal_id(evaluation),
                            signal_date=trigger_bar.source_date,
                            block_key=block_key,
                            outcome_span_id=outcome_span_id,
                            evaluation=evaluation,
                            entry_status=entry_status,
                            no_fill_reason=no_fill_reason,
                            entry_path_index=entry_path_index,
                            entry_1s_start_ns=entry_start_ns,
                            replay_key=replay_key,
                        )
                        match_records.append(
                            {
                                "block_key": signal.block_key,
                                "candidate_key": candidate.candidate_key,
                                "decision_ns": signal.decision_ns,
                                "entry_1s_start_ns": signal.entry_1s_start_ns,
                                "entry_path_index": signal.entry_path_index,
                                "entry_status": signal.entry_status,
                                "evaluation_json": _canonical_bytes(evaluation.as_dict())
                                .decode("ascii")
                                .rstrip("\n"),
                                "no_fill_reason": signal.no_fill_reason,
                                "outcome_span_id": signal.outcome_span_id,
                                "replay_key": signal.replay_key,
                                "signal_date": signal.signal_date,
                                "signal_id": signal.signal_id,
                            }
                        )
                        candidate_accumulators = _streaming_accumulators(
                            candidate,
                            accumulators=accumulators,
                            block_keys=block_keys,
                            observed_months=observed_months,
                        )
                        economic_block, signal_month = _economic_attribution(
                            signal,
                            block_by_date=block_by_date,
                        )
                        for scenario_id, accumulator in zip(
                            _SCENARIO_IDS,
                            candidate_accumulators,
                            strict=True,
                        ):
                            surface = None if surfaces is None else surfaces[scenario_id]
                            accumulator.add(
                                CandidateSignalReplay(
                                    signal_id=signal.signal_id,
                                    signal_ts_ns=signal.decision_ns,
                                    block_key=economic_block,
                                    utc_month=signal_month,
                                    surface=surface,
                                    no_fill_reason=(
                                        signal.no_fill_reason if surface is None else None
                                    ),
                                )
                            )
                        flush_records()
    flush_records(force=True)
    return match_records, replay_records


def _run_streaming_bar_pattern_discovery(
    source: BarDiscoverySource,
    *,
    split_plan: BarSplitPlan,
    data_root: Path | str,
    artifact_loader: _ArtifactLoader = load_trade_bar_artifact,
    candidates: Sequence[BarPatternCandidate] | None = None,
    progress: Callable[[BarDiscoveryProgress], None] | None = None,
) -> BarDiscoveryResult:
    """Shared streaming implementation; public production use is manifest-only.

    The manifest's ``outcome_span_id`` carries an open position through normal
    maintenance and market-closed gaps.  Original TradeBar segments remain the
    stricter signal-context boundary.  A span is flushed only when the verified
    manifest proves a contract/quality break or at the Discovery split end,
    which is the sole mandatory terminal exit.  Context and compact first-hit
    evidence are emitted in bounded, content-addressed Parquet shards.
    """

    discovery_dates, decision_dates, block_by_date = _split_maps(split_plan)
    partitions, dataset_sha256, source_manifest_sha256 = _source_partitions(
        source,
        split_plan,
    )
    by_date = {item.source_date: item for item in partitions}
    missing = [item for item in discovery_dates if item not in by_date]
    if missing:
        raise BarDiscoveryError(f"Discovery is missing {len(missing)} active daily bar partitions")
    visible_partitions = tuple(by_date[item] for item in discovery_dates)
    source_identity_sha256 = _result_source_identity(source, visible_partitions)
    catalog = tuple(candidates) if candidates is not None else frozen_bar_pattern_candidates()
    frozen = frozen_bar_pattern_candidates()
    if catalog != frozen or len(catalog) != ALLOCATED_VARIANT_COUNT:
        raise BarDiscoveryError("Discovery must evaluate the exact frozen 216-candidate catalog")
    candidate_catalog_sha256 = canonical_sha256([item.definition_payload() for item in catalog])
    writer = _EvidenceWriter(
        data_root=data_root,
        source_identity_sha256=source_identity_sha256,
        dataset_build_sha256=dataset_sha256,
        source_manifest_sha256=source_manifest_sha256,
        split_plan_sha256=split_plan.sha256,
        candidate_catalog_sha256=candidate_catalog_sha256,
    )
    by_dimensions: dict[tuple[int, int, object], list[BarPatternCandidate]] = defaultdict(list)
    for candidate in catalog:
        by_dimensions[
            (
                candidate.timeframe_seconds,
                candidate.setup_lookback_bars,
                candidate.direction,
            )
        ].append(candidate)
    scans = {item.candidate_key: _CandidateScan() for item in catalog}
    accumulators: dict[tuple[str, str], BarCandidateEconomicsAccumulator] = {}
    block_keys = tuple(item.split_key for item in split_plan.discovery_reporting_blocks)
    observed_months = tuple(sorted({item.strftime("%Y-%m") for item in discovery_dates}))
    decision_set = set(decision_dates)
    open_outcome_span_key: tuple[int, str] | None = None
    open_outcome_bars: dict[int, list[_DiscoveryBarLike]] = {
        item: [] for item in _REQUIRED_TIMEFRAMES
    }
    counts: Counter[int] = Counter()
    completed_active_dates = 0
    completed_segments = 0

    def report(stage: str) -> None:
        if progress is None:
            return
        progress(
            BarDiscoveryProgress(
                stage=stage,
                completed_active_dates=completed_active_dates,
                total_active_dates=len(discovery_dates),
                completed_segments=completed_segments,
                loaded_bar_counts=tuple(
                    (timeframe, counts[timeframe]) for timeframe in _REQUIRED_TIMEFRAMES
                ),
                matched_signal_count=sum(item.matched_signal_count for item in scans.values()),
                evidence_shard_count=len(writer.shards),
            )
        )

    def flush() -> None:
        nonlocal completed_segments, open_outcome_span_key, open_outcome_bars
        if open_outcome_span_key is None:
            return
        matches, replays = _scan_streaming_outcome_span(
            outcome_span_key=open_outcome_span_key,
            bars_by_timeframe=open_outcome_bars,
            by_dimensions=by_dimensions,
            scans=scans,
            decision_set=decision_set,
            block_by_date=block_by_date,
            block_keys=block_keys,
            observed_months=observed_months,
            accumulators=accumulators,
            evidence_writer=writer,
        )
        # A test double may return records directly.  The production scanner
        # flushes bounded chunks through ``evidence_writer`` and returns empty.
        writer.write_segment(
            segment_id=open_outcome_span_key[0],
            contract=open_outcome_span_key[1],
            matches=matches,
            replays=replays,
        )
        completed_segments += 1
        open_outcome_span_key = None
        open_outcome_bars = {item: [] for item in _REQUIRED_TIMEFRAMES}
        report("FLUSH_OUTCOME_SPAN")

    for source_date in discovery_dates:
        partition = by_date[source_date]
        partition_span_key = partition.outcome_span_id, partition.contract
        if open_outcome_span_key is not None and partition_span_key != open_outcome_span_key:
            flush()
        if open_outcome_span_key is None:
            open_outcome_span_key = partition_span_key
        artifacts = {item.timeframe_seconds: item for item in partition.artifacts}
        if not set(_REQUIRED_TIMEFRAMES).issubset(artifacts):
            raise BarDiscoveryError("daily partition does not bind every required timeframe")
        daily_one_second_count = 0
        for timeframe in _REQUIRED_TIMEFRAMES:
            bars = artifact_loader(
                data_root,
                artifacts[timeframe],
                expected_plan_sha256=partition.plan_sha256,
                expected_source_sha256=partition.source_sha256,
                expected_source_date=source_date,
            )
            for bar in bars:
                if (
                    bar.timeframe_seconds != timeframe
                    or bar.source_date != source_date
                    or bar.contract != partition.contract
                    or bar.segment_id <= 0
                ):
                    raise BarDiscoveryError("loaded bar differs from its bound daily artifact")
                open_outcome_bars[timeframe].append(
                    _OutcomePathBar.from_bar(
                        bar,
                        outcome_span_id=partition.outcome_span_id,
                    )
                    if timeframe == 1
                    else bar
                )
                counts[timeframe] += 1
                if timeframe == 1:
                    daily_one_second_count += 1
        if not daily_one_second_count:
            raise BarDiscoveryError("an active Discovery partition has no 1s bars")
        completed_active_dates += 1
        report("LOAD_ACTIVE_DATE")
    flush()
    evidence_manifest = writer.finalize()
    report("EVIDENCE_COMPLETE")

    candidate_results: list[BarCandidateDiscoveryResult] = []
    for candidate in catalog:
        scan = scans[candidate.candidate_key]
        if scan.decision_trigger_count != scan.evaluated_count + scan.context_not_evaluable_count:
            raise BarDiscoveryError("candidate context accounting does not balance")
        if scan.context_rejections is None or scan.failed_gates is None:
            raise AssertionError
        support = _support_from_scan(
            candidate,
            scan,
            decision_dates=decision_dates,
            block_keys=block_keys,
        )
        candidate_accumulators = _streaming_accumulators(
            candidate,
            accumulators=accumulators,
            block_keys=block_keys,
            observed_months=observed_months,
        )
        economics_values: list[BarScenarioDiscoveryEconomics] = []
        for scenario_id, accumulator in zip(
            _SCENARIO_IDS,
            candidate_accumulators,
            strict=True,
        ):
            economics_values.append(
                BarScenarioDiscoveryEconomics(
                    scenario_id=scenario_id,
                    cells=accumulator.finalize(),
                )
            )
            del accumulators[candidate.candidate_key, scenario_id]
        economics = tuple(economics_values)
        decision = screen_bar_candidate(
            support,
            {item.scenario_id: item.cells for item in economics},
        )
        candidate_results.append(
            BarCandidateDiscoveryResult(
                candidate=candidate,
                decision_trigger_count=scan.decision_trigger_count,
                evaluated_count=scan.evaluated_count,
                context_not_evaluable_count=scan.context_not_evaluable_count,
                context_rejection_counts=tuple(sorted(scan.context_rejections.items())),
                failed_gate_counts=tuple(sorted(scan.failed_gates.items())),
                matched_signal_count=scan.matched_signal_count,
                matched_signals=(),
                support=support,
                economics=economics,
                decision=decision,
                final_label=decision.label,
            )
        )
    ranked = rank_bar_finalists(tuple(item.decision for item in candidate_results))
    ranked_keys = tuple(item.candidate_key for item in ranked)
    ranked_set = set(ranked_keys)
    budget_rejected_keys = tuple(
        item.candidate.candidate_key
        for item in candidate_results
        if item.decision.label == "DISCOVERY_FINALIST"
        and item.candidate.candidate_key not in ranked_set
    )
    budget_rejected_set = set(budget_rejected_keys)
    final_results = tuple(
        replace(
            item,
            final_label=(
                "DISCOVERY_FINALIST_SELECTED"
                if item.candidate.candidate_key in ranked_set
                else (
                    "DISCOVERY_FINALIST_BUDGET_REJECTED"
                    if item.candidate.candidate_key in budget_rejected_set
                    else item.decision.label
                )
            ),
        )
        for item in candidate_results
    )
    result = BarDiscoveryResult(
        source_identity_sha256=source_identity_sha256,
        dataset_build_sha256=dataset_sha256,
        outcome_span_policy_sha256=BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
        config_semantic_sha256=BAR_PATTERN_CONFIG_SEMANTIC_SHA256,
        candidate_catalog_sha256=candidate_catalog_sha256,
        split_plan_sha256=split_plan.sha256,
        loaded_source_dates=tuple(discovery_dates),
        decision_dates=tuple(decision_dates),
        loaded_bar_counts=tuple(
            (timeframe, counts[timeframe]) for timeframe in _REQUIRED_TIMEFRAMES
        ),
        replay_catalog=(),
        evidence_manifest=evidence_manifest,
        candidate_results=final_results,
        ranked_finalist_keys=ranked_keys,
        budget_rejected_keys=budget_rejected_keys,
    )
    report("COMPLETE")
    return result


def run_streaming_bar_pattern_discovery(
    source: LoadedBarDatasetManifest,
    *,
    split_plan: BarSplitPlan,
    data_root: Path | str,
    artifact_loader: _ArtifactLoader = load_trade_bar_artifact,
    candidates: Sequence[BarPatternCandidate] | None = None,
    progress: Callable[[BarDiscoveryProgress], None] | None = None,
) -> BarDiscoveryResult:
    """Run production Discovery from an authenticated dataset handoff only.

    Requiring :class:`LoadedBarDatasetManifest` makes the dataset manifest,
    source manifest, contract/quality outcome spans, and their frozen policy
    non-null prerequisites of every published evidence descriptor.
    """

    if not isinstance(source, LoadedBarDatasetManifest):
        raise BarDiscoveryError("streaming production Discovery requires LoadedBarDatasetManifest")
    return _run_streaming_bar_pattern_discovery(
        source,
        split_plan=split_plan,
        data_root=data_root,
        artifact_loader=artifact_loader,
        candidates=candidates,
        progress=progress,
    )


def run_bar_pattern_discovery(
    source: BarDiscoverySource,
    *,
    split_plan: BarSplitPlan,
    data_root: Path | str,
    artifact_loader: _ArtifactLoader = load_trade_bar_artifact,
    candidates: Sequence[BarPatternCandidate] | None = None,
) -> BarDiscoveryResult:
    """Run the fixture-oriented in-memory engine with no early pruning.

    ``artifact_loader`` is injectable only to permit tiny verified unit
    fixtures.  Real research must use
    :func:`run_streaming_bar_pattern_discovery`; materializing all Discovery
    bars and match contexts through this function is intentionally unsupported
    as an operating topology.
    """

    discovery_dates, decision_dates, block_by_date = _split_maps(split_plan)
    partitions, dataset_sha256, _source_manifest_sha256 = _source_partitions(
        source,
        split_plan,
    )
    discovery_partition_set = set(discovery_dates)
    visible_partitions = tuple(
        item for item in partitions if item.source_date in discovery_partition_set
    )
    grouped, loaded_dates, loaded_counts = _load_discovery_bars(
        data_root=data_root,
        partitions=visible_partitions,
        discovery_dates=discovery_dates,
        artifact_loader=artifact_loader,
    )
    source_identity_sha256 = _result_source_identity(source, visible_partitions)

    catalog = tuple(candidates) if candidates is not None else frozen_bar_pattern_candidates()
    frozen = frozen_bar_pattern_candidates()
    if catalog != frozen or len(catalog) != ALLOCATED_VARIANT_COUNT:
        raise BarDiscoveryError("Discovery must evaluate the exact frozen 216-candidate catalog")
    candidate_catalog_sha256 = canonical_sha256([item.definition_payload() for item in catalog])
    by_dimensions: dict[
        tuple[int, int, object],
        list[BarPatternCandidate],
    ] = defaultdict(list)
    for candidate in catalog:
        by_dimensions[
            (
                candidate.timeframe_seconds,
                candidate.setup_lookback_bars,
                candidate.direction,
            )
        ].append(candidate)
    scans = {item.candidate_key: _CandidateScan() for item in catalog}

    partition_by_date = {item.source_date: item for item in visible_partitions}
    outcome_one_second_groups: dict[tuple[int, str], list[_OutcomePathBar]] = defaultdict(list)
    for (timeframe, _signal_segment_id, contract), values in grouped.items():
        if timeframe != 1:
            continue
        for bar in values:
            partition = partition_by_date[bar.source_date]
            if contract != partition.contract:
                raise BarDiscoveryError("1s bar contract differs from its outcome partition")
            outcome_one_second_groups[(partition.outcome_span_id, partition.contract)].append(
                _OutcomePathBar.from_bar(
                    bar,
                    outcome_span_id=partition.outcome_span_id,
                )
            )
    paths = {
        key: BarPathIndex(tuple(sorted(values, key=lambda item: item.start_ns)))
        for key, values in outcome_one_second_groups.items()
    }
    path_indexes_by_start_ns = {key: _BarStartLookup(path.bars) for key, path in paths.items()}
    decision_set = set(decision_dates)
    for timeframe in SIGNAL_TIMEFRAMES_SECONDS:
        signal_groups = sorted(
            (
                ((segment_id, contract), values)
                for (width, segment_id, contract), values in grouped.items()
                if width == timeframe
            ),
            key=lambda item: (item[1][0].start_ns, item[0]),
        )
        for _signal_segment_key, signal_bars in signal_groups:
            outcome_span_keys = {
                (
                    partition_by_date[item.source_date].outcome_span_id,
                    partition_by_date[item.source_date].contract,
                )
                for item in signal_bars
            }
            if len(outcome_span_keys) != 1:
                raise BarDiscoveryError("one signal segment crosses an outcome-span boundary")
            outcome_span_key = next(iter(outcome_span_keys))
            path = paths.get(outcome_span_key)
            if path is None:
                raise BarDiscoveryError("signal segment has no canonical outcome path")
            for trigger_index, trigger_bar in enumerate(signal_bars):
                if trigger_bar.source_date not in decision_set:
                    continue
                block_key = block_by_date[trigger_bar.source_date]
                for (width, lookback, direction), dimension_candidates in by_dimensions.items():
                    if width != timeframe:
                        continue
                    for candidate in dimension_candidates:
                        scans[candidate.candidate_key].decision_trigger_count += 1
                    try:
                        context = build_bar_pattern_context(
                            signal_bars,
                            trigger_index=trigger_index,
                            setup_lookback_bars=lookback,
                            direction=direction,
                        )
                    except BarPatternError as error:
                        reason = _context_rejection(error)
                        for candidate in dimension_candidates:
                            scan = scans[candidate.candidate_key]
                            scan.context_not_evaluable_count += 1
                            if scan.context_rejections is None:  # pragma: no cover
                                raise AssertionError
                            scan.context_rejections[reason] += 1
                        continue
                    entry = _entry_path_index(
                        context=context,
                        signal_bars=signal_bars,
                        path=path,
                        path_index_by_start_ns=path_indexes_by_start_ns[outcome_span_key],
                    )
                    for candidate in dimension_candidates:
                        evaluation = evaluate_bar_pattern_context(
                            context,
                            candidate=candidate,
                        )
                        scan = scans[candidate.candidate_key]
                        scan.evaluated_count += 1
                        if scan.failed_gates is None or scan.matches is None:  # pragma: no cover
                            raise AssertionError
                        scan.failed_gates.update(evaluation.failed_gate_ids)
                        if not evaluation.matched:
                            continue
                        scan.matched_signal_count += 1
                        if scan.matched_daily_counts is None or scan.matched_block_counts is None:
                            raise AssertionError
                        scan.matched_daily_counts[trigger_bar.source_date] += 1
                        scan.matched_block_counts[block_key] += 1
                        if entry is None:
                            entry_status = ENTRY_NOT_FILLED
                            no_fill_reason = NEXT_SIGNAL_BAR_UNAVAILABLE
                            entry_path_index = None
                            entry_start_ns = None
                            replay_key = None
                        else:
                            entry_status = ENTRY_FILLED
                            no_fill_reason = None
                            entry_path_index, entry_start_ns = entry
                            replay_key = _replay_key(
                                context=context,
                                entry_path_index=entry_path_index,
                                outcome_span_id=outcome_span_key[0],
                            )
                        scan.matches.append(
                            BarMatchedSignal(
                                signal_id=_signal_id(evaluation),
                                signal_date=trigger_bar.source_date,
                                block_key=block_key,
                                outcome_span_id=outcome_span_key[0],
                                evaluation=evaluation,
                                entry_status=entry_status,
                                no_fill_reason=no_fill_reason,
                                entry_path_index=entry_path_index,
                                entry_1s_start_ns=entry_start_ns,
                                replay_key=replay_key,
                            )
                        )

    block_keys = tuple(item.split_key for item in split_plan.discovery_reporting_blocks)
    # Fixed operating cost covers every month in which the Discovery portfolio
    # is active, including the no-entry outcome tail.
    observed_months = tuple(sorted({item.strftime("%Y-%m") for item in discovery_dates}))
    replay_cache: dict[str, BarCompactReplayBundle] = {}
    candidate_results: list[BarCandidateDiscoveryResult] = []
    for candidate in catalog:
        scan = scans[candidate.candidate_key]
        if scan.decision_trigger_count != scan.evaluated_count + scan.context_not_evaluable_count:
            raise BarDiscoveryError("candidate context accounting does not balance")
        if scan.matches is None or scan.context_rejections is None or scan.failed_gates is None:
            raise AssertionError("candidate scan containers were not initialized")
        matches = tuple(
            sorted(
                scan.matches,
                key=lambda item: (
                    item.decision_ns,
                    item.evaluation.context.segment_id,
                    item.signal_id,
                ),
            )
        )
        support = _support(
            candidate,
            matches,
            decision_dates=decision_dates,
            block_keys=block_keys,
        )
        economics = _candidate_economics(
            candidate,
            matches,
            paths=paths,
            block_keys=block_keys,
            block_by_date=block_by_date,
            observed_months=observed_months,
            replay_cache=replay_cache,
        )
        surfaces = {item.scenario_id: item.cells for item in economics}
        decision = screen_bar_candidate(support, surfaces)
        candidate_results.append(
            BarCandidateDiscoveryResult(
                candidate=candidate,
                decision_trigger_count=scan.decision_trigger_count,
                evaluated_count=scan.evaluated_count,
                context_not_evaluable_count=scan.context_not_evaluable_count,
                context_rejection_counts=tuple(sorted(scan.context_rejections.items())),
                failed_gate_counts=tuple(sorted(scan.failed_gates.items())),
                matched_signal_count=len(matches),
                matched_signals=matches,
                support=support,
                economics=economics,
                decision=decision,
                final_label=decision.label,
            )
        )
    ranked = rank_bar_finalists(tuple(item.decision for item in candidate_results))
    ranked_keys = tuple(item.candidate_key for item in ranked)
    ranked_set = set(ranked_keys)
    budget_rejected_keys = tuple(
        item.candidate.candidate_key
        for item in candidate_results
        if item.decision.label == "DISCOVERY_FINALIST"
        and item.candidate.candidate_key not in ranked_set
    )
    budget_rejected_set = set(budget_rejected_keys)
    candidate_results = [
        replace(
            item,
            final_label=(
                "DISCOVERY_FINALIST_SELECTED"
                if item.candidate.candidate_key in ranked_set
                else (
                    "DISCOVERY_FINALIST_BUDGET_REJECTED"
                    if item.candidate.candidate_key in budget_rejected_set
                    else item.decision.label
                )
            ),
        )
        for item in candidate_results
    ]
    return BarDiscoveryResult(
        source_identity_sha256=source_identity_sha256,
        dataset_build_sha256=dataset_sha256,
        outcome_span_policy_sha256=BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
        config_semantic_sha256=BAR_PATTERN_CONFIG_SEMANTIC_SHA256,
        candidate_catalog_sha256=candidate_catalog_sha256,
        split_plan_sha256=split_plan.sha256,
        loaded_source_dates=loaded_dates,
        decision_dates=tuple(decision_dates),
        loaded_bar_counts=loaded_counts,
        replay_catalog=tuple(replay_cache[key] for key in sorted(replay_cache)),
        evidence_manifest=None,
        candidate_results=tuple(candidate_results),
        ranked_finalist_keys=ranked_keys,
        budget_rejected_keys=budget_rejected_keys,
    )
