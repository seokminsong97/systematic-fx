"""Fixed production adapter and private Search chunk persistence.

The public lifecycle in :mod:`campaigns.ai_all_cases_v1.run` calls this module
through one fixed private service bundle.  Search is intentionally implemented
as a predecessor-hashed internal subledger: long-running raw/OOF chunks are
resumable, but no partial metric is exposed by the outer campaign ledger.
"""

from __future__ import annotations

import hashlib
import json
import os
import resource
import stat
import sys
import tempfile
import time
from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from fractions import Fraction
from itertools import groupby
from math import comb
from numbers import Integral
from pathlib import Path
from typing import TYPE_CHECKING, Final

from .config import (
    AI_ALL_CASES_CONFIG_ID,
    AI_ALL_CASES_RUN_RELATIVE_ROOT,
    AllCasesConfig,
    _load_validated_dataset_contract,
    _require_deterministic_runtime_environment,
)

if TYPE_CHECKING:
    from systematic_fx.features.bars import TradeBar
    from systematic_fx.research.bar_pipeline import BarDatasetPartition

_RUN_RELATIVE_ROOT: Final = AI_ALL_CASES_RUN_RELATIVE_ROOT
_SEARCH_INTERNAL_RELATIVE_ROOT: Final = _RUN_RELATIVE_ROOT / "internal/search"
_SEARCH_PHASE_ORDER: Final = (
    "STAGE_A_SCORE_CHUNKS",
    "STAGE_A_TOP256",
    "STAGE_B_PLAN_FROZEN",
    "STAGE_B_RAW_CHUNKS",
    "SYMBOLIC_TOP24",
    "DIRECT_ML_CHUNKS",
    "META_PLAN_FROZEN",
    "META_ML_CHUNKS",
    "FINAL_MAX12",
)
_WRITE_BITS: Final = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
_OUTER_FAILURE_EVENT_RESERVE_BYTES: Final = 16_384
_SUBLEDGER_EVENT_SCHEMA: Final = "systematic_fx.ai_all_cases_search_subledger_event.v1"
_SUBLEDGER_ARTIFACT_SCHEMA: Final = "systematic_fx.ai_all_cases_search_chunk.v1"
_RAW_SEARCH_PHASES: Final = frozenset(
    {
        "STAGE_A_SCORE_CHUNKS",
        "STAGE_B_RAW_CHUNKS",
        "DIRECT_ML_CHUNKS",
        "META_ML_CHUNKS",
    }
)
_FORBIDDEN_RAW_KEY_FRAGMENTS: Final = (
    "bh_",
    "early_stop",
    "finalist",
    "holm",
    "selected_candidate",
    "significant",
)


class AllCasesPipelineError(RuntimeError):
    """The fixed production adapter or internal Search evidence differs."""


class _VerifiedPrefixIncomplete(AllCasesPipelineError):
    """A read-only source replay reached the exact lowest missing coordinate."""

    def __init__(self, phase: str, chunk_index: int) -> None:
        super().__init__(f"verified prefix ends before {phase}[{chunk_index}]")
        self.phase = phase
        self.chunk_index = chunk_index


class _PipelineResourceGuard:
    """Absolute Search/verifier deadline plus RSS and immutable-byte caps."""

    def __init__(
        self,
        project_root: Path,
        config: AllCasesConfig,
        *,
        verifier: bool,
    ) -> None:
        caps = config.as_dict().get("compute_caps")
        if not isinstance(caps, Mapping):
            raise AllCasesPipelineError("pipeline compute-cap contract is absent")
        wall_key = "verifier_wall_seconds_maximum" if verifier else "search_wall_seconds_maximum"
        try:
            self.artifact_cap = int(caps["artifact_bytes_maximum"])
            self.rss_cap = int(caps["resident_set_bytes_maximum"])
            self.wall_cap = int(caps[wall_key])
        except (KeyError, TypeError, ValueError) as error:
            raise AllCasesPipelineError("pipeline compute-cap values differ") from error
        if min(self.artifact_cap, self.rss_cap, self.wall_cap) <= 0:
            raise AllCasesPipelineError("pipeline compute caps must be positive")
        self.run_root = project_root / _RUN_RELATIVE_ROOT
        self.started = time.monotonic()

    def _regular_bytes(self) -> int:
        if not self.run_root.exists():
            return 0
        seen: set[tuple[int, int]] = set()
        total = 0
        for path in self.run_root.rglob("*"):
            metadata = path.lstat()
            if stat.S_ISLNK(metadata.st_mode):
                raise AllCasesPipelineError("pipeline resource scan found a symlink")
            if stat.S_ISDIR(metadata.st_mode):
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise AllCasesPipelineError("pipeline resource scan found a special file")
            identity = metadata.st_dev, metadata.st_ino
            if identity not in seen:
                total += metadata.st_size
                seen.add(identity)
        return total

    def check(self, boundary: str) -> None:
        if time.monotonic() - self.started > self.wall_cap:
            raise AllCasesPipelineError(f"pipeline wall-time cap exceeded at {boundary}")
        observed_rss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        if sys.platform != "darwin":
            observed_rss *= 1024
        if observed_rss > self.rss_cap:
            raise AllCasesPipelineError(f"pipeline resident-set cap exceeded at {boundary}")
        if self._regular_bytes() + _OUTER_FAILURE_EVENT_RESERVE_BYTES > self.artifact_cap:
            raise AllCasesPipelineError(f"pipeline artifact-byte cap exceeded at {boundary}")

    def ensure_additional_bytes(self, byte_count: int, boundary: str) -> None:
        if isinstance(byte_count, bool) or not isinstance(byte_count, int) or byte_count < 0:
            raise AllCasesPipelineError("pipeline projected byte count differs")
        self.check(boundary)
        if (
            self._regular_bytes() + byte_count + _OUTER_FAILURE_EVENT_RESERVE_BYTES
            > self.artifact_cap
        ):
            raise AllCasesPipelineError(
                f"pipeline projected artifact-byte cap exceeded at {boundary}"
            )


def _canonical_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError) as error:
        raise AllCasesPipelineError("pipeline value is not canonical JSON") from error


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _require_sha(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise AllCasesPipelineError(f"{label} is not a lowercase SHA-256")
    return value


def _validate_raw_chunk_payload(value: object, *, phase: str) -> None:
    if phase not in _RAW_SEARCH_PHASES:
        return
    if not isinstance(value, Mapping):
        raise AllCasesPipelineError("raw Search chunk must be a mapping")
    exact_top_level = {
        "STAGE_A_SCORE_CHUNKS": {
            "schema",
            "score_chunk",
            "structural_lattice_sha256",
        },
        "STAGE_B_RAW_CHUNKS": {
            "chunk",
            "coverage_by_world",
            "evaluation_chunks_by_world",
            "schema",
        },
        "DIRECT_ML_CHUNKS": {
            "candidate_count",
            "candidates",
            "fit_cache_evidence",
            "first_catalog_rank",
            "last_catalog_rank",
            "schema",
        },
        "META_ML_CHUNKS": {
            "candidate_count",
            "candidates",
            "fit_cache_evidence",
            "first_catalog_rank",
            "last_catalog_rank",
            "schema",
        },
    }
    if set(value) != exact_top_level[phase]:
        raise AllCasesPipelineError("raw Search chunk schema contains adaptive fields")
    if phase == "STAGE_B_RAW_CHUNKS":
        evaluation_worlds = value["evaluation_chunks_by_world"]
        coverage_worlds = value["coverage_by_world"]
        expected_worlds = {
            "REAL",
            "CIRCULAR",
            "MATCHED",
        }
        if (
            not isinstance(evaluation_worlds, Mapping)
            or set(evaluation_worlds) != expected_worlds
            or not isinstance(coverage_worlds, Mapping)
            or set(coverage_worlds) != expected_worlds
            or any(
                item is not None and not isinstance(item, Mapping)
                for item in evaluation_worlds.values()
            )
        ):
            raise AllCasesPipelineError("Stage-B raw worlds differ")
        row_keys = {
            "coverage",
            "ineligibility_reason",
            "strategy_id",
        }
        if any(
            not isinstance(rows, list)
            or any(not isinstance(row, Mapping) or set(row) != row_keys for row in rows)
            for rows in coverage_worlds.values()
        ):
            raise AllCasesPipelineError("Stage-B raw coverage row schema differs")
    if phase in {"DIRECT_ML_CHUNKS", "META_ML_CHUNKS"}:
        rows = value["candidates"]
        count = value["candidate_count"]
        first = value["first_catalog_rank"]
        last = value["last_catalog_rank"]
        expected_candidate_keys = (
            {
                "candidate",
                "control_alignment",
                "crossfit_summaries",
                "final_model_sha256_by_world",
                "frozen_model_artifact",
                "gate",
                "ineligibility",
                "null_feasibility",
                "schema",
                "search_controls",
                "training_rows_sha256",
            }
            if phase == "DIRECT_ML_CHUNKS"
            else {
                "candidate",
                "control_alignment",
                "crossfit_summaries",
                "final_model_sha256_by_world",
                "frozen_model_artifact",
                "gate",
                "ineligibility",
                "null_feasibility",
                "schema",
                "search_controls",
                "training_rows_sha256_by_world_and_fold",
            }
        )
        if (
            isinstance(count, bool)
            or not isinstance(count, int)
            or count < 0
            or isinstance(first, bool)
            or not isinstance(first, int)
            or isinstance(last, bool)
            or not isinstance(last, int)
            or (count == 0 and (first, last) != (1, 0))
            or (count > 0 and (first < 1 or last - first + 1 != count))
            or not isinstance(rows, list)
            or len(rows) != count
            or any(
                not isinstance(row, Mapping) or set(row) != expected_candidate_keys for row in rows
            )
        ):
            raise AllCasesPipelineError("ML raw candidate row schema differs")
    pending = [value]
    while pending:
        current = pending.pop()
        if isinstance(current, Mapping):
            for key, child in current.items():
                normalized = str(key).lower()
                if any(fragment in normalized for fragment in _FORBIDDEN_RAW_KEY_FRAGMENTS):
                    raise AllCasesPipelineError(
                        "raw Search chunk contains a selection or inferential release"
                    )
                pending.append(child)
        elif isinstance(current, (list, tuple)):
            pending.extend(current)


def _exact_ml_integer(value: object, *, label: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise AllCasesPipelineError(f"{label} is not an exact integer")
    if minimum is not None and value < minimum:
        raise AllCasesPipelineError(f"{label} is below its frozen minimum")
    return value


def _decode_ml_group(ml: object, value: object) -> object:
    keys = {
        "active_entry_dates",
        "active_signal_dates",
        "fill_count",
        "gross_loss_ticks",
        "gross_profit_ticks",
        "group_key",
        "maximum_drawdown_ticks",
        "net_ticks",
        "raw_signal_count",
        "stress_net_ticks",
    }
    if not isinstance(value, dict) or set(value) != keys:
        raise AllCasesPipelineError("Search-prefix ML economic group schema differs")

    def dates(raw: object) -> tuple[date, ...]:
        if not isinstance(raw, list) or any(not isinstance(item, str) for item in raw):
            raise AllCasesPipelineError("Search-prefix ML economic group dates differ")
        try:
            output = tuple(date.fromisoformat(item) for item in raw)
        except ValueError as error:
            raise AllCasesPipelineError("Search-prefix ML economic group date differs") from error
        if [item.isoformat() for item in output] != raw:
            raise AllCasesPipelineError("Search-prefix ML economic group date is noncanonical")
        return output

    numeric = {
        key: _exact_ml_integer(value[key], label=f"Search-prefix ML group {key}")
        for key in (
            "fill_count",
            "gross_loss_ticks",
            "gross_profit_ticks",
            "maximum_drawdown_ticks",
            "net_ticks",
            "raw_signal_count",
            "stress_net_ticks",
        )
    }
    if not isinstance(value["group_key"], str) or not value["group_key"]:
        raise AllCasesPipelineError("Search-prefix ML economic group key differs")
    try:
        typed = ml.MLGroupEconomicAggregate(
            value["group_key"],
            numeric["raw_signal_count"],
            dates(value["active_signal_dates"]),
            numeric["fill_count"],
            dates(value["active_entry_dates"]),
            numeric["net_ticks"],
            numeric["stress_net_ticks"],
            numeric["gross_profit_ticks"],
            numeric["gross_loss_ticks"],
            numeric["maximum_drawdown_ticks"],
        )
    except ml.AllCasesMLError as error:
        raise AllCasesPipelineError("Search-prefix ML economic group differs") from error
    if _canonical_json_bytes(typed.as_dict()) != _canonical_json_bytes(value):
        raise AllCasesPipelineError("Search-prefix ML economic group round trip differs")
    return typed


def _decode_ml_evaluation(ml: object, value: object, candidate: object, world: str) -> object:
    keys = {
        "active_entry_days",
        "active_signal_days",
        "action_identities",
        "alignment_proof_sha256",
        "artifact_sha256",
        "candidate_id",
        "candidate_kind",
        "daily_net_ticks",
        "family_key",
        "fill_count",
        "gross_loss_ticks",
        "gross_profit_ticks",
        "maximum_drawdown_ticks",
        "null_world",
        "outer_validations",
        "raw_signal_count",
        "reporting_groups",
        "schema",
        "total_net_ticks",
        "total_stress_net_ticks",
    }
    if (
        not isinstance(value, dict)
        or set(value) != keys
        or value.get("schema") != ml.ML_SEARCH_EVALUATION_SCHEMA
        or value.get("candidate_id") != candidate.candidate_id
        or value.get("null_world") != world
    ):
        raise AllCasesPipelineError("Search-prefix ML evaluation schema differs")
    candidate_kind = "DIRECT" if candidate.candidate_id in ml.DIRECT_CANDIDATE_BY_ID else "META"
    if value.get("candidate_kind") != candidate_kind:
        raise AllCasesPipelineError("Search-prefix ML evaluation kind differs")
    for key in ("alignment_proof_sha256", "artifact_sha256", "family_key"):
        _require_sha(value.get(key), label=f"Search-prefix ML evaluation {key}")
    actions = value["action_identities"]
    daily = value["daily_net_ticks"]
    if (
        not isinstance(actions, list)
        or any(
            not isinstance(item, list)
            or len(item) != 2
            or not isinstance(item[0], str)
            or not item[0]
            or item[1] not in {"LONG", "SHORT"}
            for item in actions
        )
        or not isinstance(daily, list)
        or any(
            not isinstance(item, list)
            or len(item) != 2
            or not isinstance(item[0], str)
            or isinstance(item[1], bool)
            or not isinstance(item[1], int)
            for item in daily
        )
    ):
        raise AllCasesPipelineError("Search-prefix ML evaluation rows differ")
    for item in daily:
        try:
            if date.fromisoformat(item[0]).isoformat() != item[0]:
                raise ValueError
        except ValueError as error:
            raise AllCasesPipelineError("Search-prefix ML evaluation date differs") from error
    reporting = value["reporting_groups"]
    outer = value["outer_validations"]
    if not isinstance(reporting, list) or not isinstance(outer, list):
        raise AllCasesPipelineError("Search-prefix ML evaluation groups differ")
    integers = {
        key: _exact_ml_integer(value[key], label=f"Search-prefix ML evaluation {key}")
        for key in (
            "active_entry_days",
            "active_signal_days",
            "fill_count",
            "gross_loss_ticks",
            "gross_profit_ticks",
            "maximum_drawdown_ticks",
            "raw_signal_count",
            "total_net_ticks",
            "total_stress_net_ticks",
        )
    }
    try:
        typed = ml.MLSearchEconomicEvaluation(
            candidate.candidate_id,
            candidate_kind,
            value["family_key"],
            world,
            value["alignment_proof_sha256"],
            integers["raw_signal_count"],
            integers["active_signal_days"],
            integers["fill_count"],
            integers["active_entry_days"],
            integers["total_net_ticks"],
            integers["total_stress_net_ticks"],
            integers["gross_profit_ticks"],
            integers["gross_loss_ticks"],
            integers["maximum_drawdown_ticks"],
            tuple((item[0], item[1]) for item in actions),
            tuple((item[0], item[1]) for item in daily),
            tuple(_decode_ml_group(ml, item) for item in reporting),
            tuple(_decode_ml_group(ml, item) for item in outer),
            value["artifact_sha256"],
        )
    except ml.AllCasesMLError as error:
        raise AllCasesPipelineError("Search-prefix ML evaluation differs") from error
    if _canonical_json_bytes(typed.as_dict()) != _canonical_json_bytes(value):
        raise AllCasesPipelineError("Search-prefix ML evaluation round trip differs")
    return typed


def _decode_ml_gate(ml: object, value: object, candidate: object) -> object:
    keys = {
        "artifact_sha256",
        "candidate_id",
        "candidate_kind",
        "eligible",
        "family_key",
        "median_outer_ev_denominator",
        "median_outer_ev_numerator",
        "positive_outer_validation_count",
        "positive_reporting_group_count",
        "rejection_reasons",
        "schema",
        "worst_outer_ev_denominator",
        "worst_outer_ev_numerator",
    }
    if (
        not isinstance(value, dict)
        or set(value) != keys
        or value.get("schema") != ml.ML_SEARCH_GATE_SCHEMA
    ):
        raise AllCasesPipelineError("Search-prefix ML gate schema differs")
    candidate_kind = "DIRECT" if candidate.candidate_id in ml.DIRECT_CANDIDATE_BY_ID else "META"
    reasons = value["rejection_reasons"]
    if (
        value.get("candidate_id") != candidate.candidate_id
        or value.get("candidate_kind") != candidate_kind
        or type(value.get("eligible")) is not bool
        or not isinstance(reasons, list)
        or any(not isinstance(item, str) or not item for item in reasons)
    ):
        raise AllCasesPipelineError("Search-prefix ML gate identity differs")
    _require_sha(value.get("family_key"), label="Search-prefix ML gate family")
    _require_sha(value.get("artifact_sha256"), label="Search-prefix ML gate SHA")
    numeric = {
        key: _exact_ml_integer(value[key], label=f"Search-prefix ML gate {key}")
        for key in (
            "median_outer_ev_denominator",
            "median_outer_ev_numerator",
            "positive_outer_validation_count",
            "positive_reporting_group_count",
            "worst_outer_ev_denominator",
            "worst_outer_ev_numerator",
        )
    }
    try:
        typed = ml.MLSearchGateResult(
            candidate.candidate_id,
            candidate_kind,
            value["family_key"],
            value["eligible"],
            tuple(reasons),
            numeric["positive_reporting_group_count"],
            numeric["positive_outer_validation_count"],
            numeric["worst_outer_ev_numerator"],
            numeric["worst_outer_ev_denominator"],
            numeric["median_outer_ev_numerator"],
            numeric["median_outer_ev_denominator"],
            value["artifact_sha256"],
        )
    except ml.AllCasesMLError as error:
        raise AllCasesPipelineError("Search-prefix ML gate differs") from error
    if _canonical_json_bytes(typed.as_dict()) != _canonical_json_bytes(value):
        raise AllCasesPipelineError("Search-prefix ML gate round trip differs")
    return typed


def _decode_control_alignment(ml: object, value: object, candidate_id: str) -> object:
    keys = {
        "aligned_mask_sha256_by_world",
        "artifact_sha256",
        "candidate_id",
        "schema",
        "scope_key",
        "selected_row_ids_by_world",
        "source_mask_sha256_by_world",
        "target_count_records",
    }
    if (
        not isinstance(value, dict)
        or set(value) != keys
        or value.get("schema") != ml.CONTROL_ALIGNMENT_SCHEMA
        or value.get("candidate_id") != candidate_id
        or value.get("scope_key") != "SEARCH_OOF_B3_B8"
    ):
        raise AllCasesPipelineError("Search-prefix control alignment schema differs")

    def world_shas(raw: object) -> tuple[tuple[str, str], ...]:
        if (
            not isinstance(raw, list)
            or len(raw) != len(ml.NULL_WORLD_ORDER)
            or any(
                not isinstance(item, list)
                or len(item) != 2
                or item[0] != ml.NULL_WORLD_ORDER[index]
                or not isinstance(item[1], str)
                for index, item in enumerate(raw)
            )
        ):
            raise AllCasesPipelineError("Search-prefix control alignment SHA rows differ")
        for _world, digest in raw:
            _require_sha(digest, label="Search-prefix control alignment mask SHA")
        return tuple((item[0], item[1]) for item in raw)

    selected = value["selected_row_ids_by_world"]
    if (
        not isinstance(selected, list)
        or len(selected) != len(ml.NULL_WORLD_ORDER)
        or any(
            not isinstance(item, list)
            or len(item) != 2
            or item[0] != ml.NULL_WORLD_ORDER[index]
            or not isinstance(item[1], list)
            or any(not isinstance(row_id, str) or not row_id for row_id in item[1])
            for index, item in enumerate(selected)
        )
    ):
        raise AllCasesPipelineError("Search-prefix control alignment selected rows differ")
    counts = value["target_count_records"]
    if not isinstance(counts, list) or any(
        not isinstance(item, list)
        or len(item) != 3
        or not isinstance(item[0], str)
        or item[1] not in {"LONG", "SHORT"}
        or isinstance(item[2], bool)
        or not isinstance(item[2], int)
        for item in counts
    ):
        raise AllCasesPipelineError("Search-prefix control alignment count rows differ")
    for item in counts:
        try:
            if date.fromisoformat(item[0]).isoformat() != item[0]:
                raise ValueError
        except ValueError as error:
            raise AllCasesPipelineError("Search-prefix control alignment date differs") from error
    _require_sha(value.get("artifact_sha256"), label="Search-prefix control alignment SHA")
    try:
        typed = ml.ControlAlignmentProof(
            candidate_id,
            "SEARCH_OOF_B3_B8",
            tuple((item[0], item[1], item[2]) for item in counts),
            world_shas(value["source_mask_sha256_by_world"]),
            world_shas(value["aligned_mask_sha256_by_world"]),
            tuple((item[0], tuple(item[1])) for item in selected),
            value["artifact_sha256"],
        )
    except ml.AllCasesMLError as error:
        raise AllCasesPipelineError("Search-prefix control alignment differs") from error
    if _canonical_json_bytes(typed.as_dict()) != _canonical_json_bytes(value):
        raise AllCasesPipelineError("Search-prefix control alignment round trip differs")
    return typed


def _decode_ml_search_controls(ml: object, value: object, candidate: object) -> tuple[object, ...]:
    if (
        not isinstance(value, dict)
        or set(value) != {"alignment_proof_sha256", "evaluations", "schema"}
        or value.get("schema") != "systematic_fx.ai_all_cases_ml_search_controls.v1"
    ):
        raise AllCasesPipelineError("Search-prefix ML controls schema differs")
    alignment = _require_sha(
        value.get("alignment_proof_sha256"), label="Search-prefix ML controls alignment"
    )
    evaluations = value["evaluations"]
    if not isinstance(evaluations, list) or len(evaluations) != len(ml.NULL_WORLD_ORDER):
        raise AllCasesPipelineError("Search-prefix ML control worlds differ")
    typed = tuple(
        _decode_ml_evaluation(ml, item, candidate, world)
        for item, world in zip(evaluations, ml.NULL_WORLD_ORDER, strict=True)
    )
    if any(item.alignment_proof_sha256 != alignment for item in typed):
        raise AllCasesPipelineError("Search-prefix ML controls alignment differs")
    return typed


def _validate_ml_ineligibility(ml: object, value: object, candidate_id: str) -> None:
    keys = {"candidate_id", "message", "reason", "schema", "scope_key"}
    if (
        not isinstance(value, dict)
        or set(value) != keys
        or value.get("schema") != "systematic_fx.ai_all_cases_ml_ineligibility.v1"
        or value.get("candidate_id") not in {None, candidate_id}
        or not isinstance(value.get("message"), str)
        or not value["message"]
        or value.get("reason") not in {item.value for item in ml.MLIneligibilityReason}
        or (
            value.get("scope_key") is not None
            and (not isinstance(value["scope_key"], str) or not value["scope_key"])
        )
    ):
        raise AllCasesPipelineError("Search-prefix ML ineligibility differs")


def _validate_null_feasibility(ml: object, value: object, candidate: object, *, meta: bool) -> None:
    top_keys = (
        {"candidate_id", "records", "report_sha256", "schema", "search_block_plan_sha256"}
        if meta
        else {
            "candidate_id",
            "records",
            "report_sha256",
            "schema",
            "search_block_plan_sha256",
            "training_rows_sha256",
        }
    )
    schema = (
        "systematic_fx.ai_all_cases_meta_null_feasibility.v1"
        if meta
        else "systematic_fx.ai_all_cases_null_feasibility.v1"
    )
    if (
        not isinstance(value, dict)
        or set(value) != top_keys
        or value.get("schema") != schema
        or value.get("candidate_id") != candidate.candidate_id
        or not isinstance(value.get("records"), list)
    ):
        raise AllCasesPipelineError("Search-prefix null feasibility schema differs")
    _require_sha(value.get("report_sha256"), label="Search-prefix null feasibility report")
    _require_sha(value.get("search_block_plan_sha256"), label="Search-prefix null feasibility plan")
    if not meta:
        _require_sha(value.get("training_rows_sha256"), label="Search-prefix null training rows")
    definition = {key: item for key, item in value.items() if key != "report_sha256"}
    if _sha256(definition) != value["report_sha256"]:
        raise AllCasesPipelineError("Search-prefix null feasibility hash differs")
    expected_scopes = (
        ml.SEARCH_OUTER_FOLD_KEYS if meta else (*ml.SEARCH_OUTER_FOLD_KEYS, "SEARCH_FINAL")
    )
    expected_coordinates = tuple(
        (scope, world)
        for scope in expected_scopes
        for world in ("CIRCULAR_TARGET", "MATCHED_TARGET")
    )
    record_keys = (
        {
            "base_strategy_id",
            "coarse_stratum_count",
            "exact_stratum_count",
            "fold_key",
            "permutation_plan_sha256",
            "row_count",
            "same_contract_fallback_count",
            "symbolic_ranking_sha256",
            "world",
        }
        if meta
        else {
            "coarse_stratum_count",
            "exact_stratum_count",
            "fold_key",
            "permutation_plan_sha256",
            "row_count",
            "same_contract_fallback_count",
            "world",
        }
    )
    records = value["records"]
    if len(records) != len(expected_coordinates):
        raise AllCasesPipelineError("Search-prefix null feasibility count differs")
    for raw, coordinate in zip(records, expected_coordinates, strict=True):
        if (
            not isinstance(raw, dict)
            or set(raw) != record_keys
            or (raw.get("fold_key"), raw.get("world")) != coordinate
        ):
            raise AllCasesPipelineError("Search-prefix null feasibility coordinate differs")
        for key in (
            "coarse_stratum_count",
            "exact_stratum_count",
            "row_count",
            "same_contract_fallback_count",
        ):
            _exact_ml_integer(
                raw.get(key), label=f"Search-prefix null feasibility {key}", minimum=0
            )
        _require_sha(raw.get("permutation_plan_sha256"), label="Search-prefix permutation plan")
        if meta:
            _require_sha(raw.get("base_strategy_id"), label="Search-prefix meta base strategy")
            _require_sha(raw.get("symbolic_ranking_sha256"), label="Search-prefix meta ranking")


def _decode_target_permutation(ml: object, value: object, candidate_id: str, world: str) -> object:
    keys = {
        "candidate_id",
        "coarse_stratum_count",
        "destination_indexes",
        "exact_stratum_count",
        "fold_key",
        "same_contract_fallback_count",
        "schema",
        "source_indexes",
        "world",
    }
    if (
        not isinstance(value, dict)
        or set(value) != keys
        or value.get("schema") != "systematic_fx.ai_all_cases_target_permutation_plan.v1"
        or value.get("candidate_id") != candidate_id
        or value.get("world") != world
        or value.get("fold_key") != "SEARCH_FINAL"
        or not isinstance(value.get("destination_indexes"), list)
        or not isinstance(value.get("source_indexes"), list)
        or any(
            isinstance(item, bool) or not isinstance(item, int)
            for item in (*value["destination_indexes"], *value["source_indexes"])
        )
    ):
        raise AllCasesPipelineError("Search-prefix target permutation schema differs")
    numeric = tuple(
        _exact_ml_integer(value[key], label=f"Search-prefix permutation {key}", minimum=0)
        for key in (
            "exact_stratum_count",
            "coarse_stratum_count",
            "same_contract_fallback_count",
        )
    )
    try:
        typed = ml.TargetPermutationPlan(
            world,
            candidate_id,
            "SEARCH_FINAL",
            tuple(value["destination_indexes"]),
            tuple(value["source_indexes"]),
            *numeric,
        )
    except ml.AllCasesMLError as error:
        raise AllCasesPipelineError("Search-prefix target permutation differs") from error
    if _canonical_json_bytes(typed.as_dict()) != _canonical_json_bytes(value):
        raise AllCasesPipelineError("Search-prefix target permutation round trip differs")
    return typed


def _validate_crossfit_summary(
    ml: object,
    value: object,
    candidate: object,
    world: str,
    *,
    meta: bool,
) -> None:
    keys = {
        "artifact_sha256",
        "candidate_id",
        "fold_admission_threshold_hex",
        "fold_base_strategy_ids",
        "fold_base_trigger_families",
        "fold_entry_schedule_sha256s",
        "fold_model_sha256",
        "fold_opportunity_lattice_sha256s",
        "fold_outcome_lineage_sha256s",
        "fold_outcome_values_sha256s",
        "fold_source_matrix_sha256s",
        "fold_symbolic_ranking_sha256",
        "null_world",
        "row_count",
        "schema",
        "task_horizon_seconds",
        "task_timeframe_seconds",
    }
    if (
        not isinstance(value, dict)
        or set(value) != keys
        or value.get("schema") != "systematic_fx.ai_all_cases_ml_crossfit_summary.v1"
        or value.get("candidate_id") != candidate.candidate_id
        or value.get("null_world") != world
        or _exact_ml_integer(value.get("row_count"), label="Search-prefix crossfit rows", minimum=1)
        < 1
    ):
        raise AllCasesPipelineError("Search-prefix ML crossfit summary differs")
    _require_sha(value.get("artifact_sha256"), label="Search-prefix ML crossfit SHA")
    six_sha_fields = (
        "fold_entry_schedule_sha256s",
        "fold_model_sha256",
        "fold_opportunity_lattice_sha256s",
        "fold_outcome_lineage_sha256s",
        "fold_outcome_values_sha256s",
        "fold_source_matrix_sha256s",
    )
    for key in six_sha_fields:
        values = value[key]
        if not isinstance(values, list) or len(values) != len(ml.SEARCH_OUTER_FOLD_KEYS):
            raise AllCasesPipelineError("Search-prefix ML crossfit SHA family differs")
        for digest in values:
            _require_sha(digest, label=f"Search-prefix ML crossfit {key}")
    thresholds = value["fold_admission_threshold_hex"]
    if not isinstance(thresholds, list) or len(thresholds) != len(ml.SEARCH_OUTER_FOLD_KEYS):
        raise AllCasesPipelineError("Search-prefix ML crossfit thresholds differ")
    for raw in thresholds:
        if not isinstance(raw, str):
            raise AllCasesPipelineError("Search-prefix ML threshold is not text")
        try:
            parsed = float.fromhex(raw)
        except ValueError as error:
            raise AllCasesPipelineError("Search-prefix ML threshold differs") from error
        if parsed.hex() != raw or parsed < 0:
            raise AllCasesPipelineError("Search-prefix ML threshold is noncanonical")
    base_ids = value["fold_base_strategy_ids"]
    families = value["fold_base_trigger_families"]
    rankings = value["fold_symbolic_ranking_sha256"]
    expected_count = len(ml.SEARCH_OUTER_FOLD_KEYS) if meta else 0
    if (
        not isinstance(base_ids, list)
        or not isinstance(families, list)
        or not isinstance(rankings, list)
        or len(base_ids) != expected_count
        or len(families) != expected_count
        or len(rankings) != expected_count
        or any(not isinstance(item, str) or not item for item in families)
    ):
        raise AllCasesPipelineError("Search-prefix ML crossfit base family differs")
    for digest in (*base_ids, *rankings):
        _require_sha(digest, label="Search-prefix ML crossfit base SHA")
    expected_tf = None if meta else candidate.decision_timeframe_seconds
    expected_horizon = None if meta else candidate.horizon_seconds
    if (
        value.get("task_timeframe_seconds") != expected_tf
        or value.get("task_horizon_seconds") != expected_horizon
        or (
            not meta
            and (
                type(value["task_timeframe_seconds"]) is not int
                or type(value["task_horizon_seconds"]) is not int
            )
        )
    ):
        raise AllCasesPipelineError("Search-prefix ML crossfit task differs")


def _validate_ml_training_commitment(
    ml: object,
    value: object,
    *,
    meta: bool,
) -> None:
    if not meta:
        _require_sha(value, label="Search-prefix direct training rows")
        return
    scopes = (*ml.SEARCH_OUTER_FOLD_KEYS, "SEARCH_FINAL")
    if not isinstance(value, dict) or set(value) != set(ml.NULL_WORLD_ORDER):
        raise AllCasesPipelineError("Search-prefix meta training worlds differ")
    for world in ml.NULL_WORLD_ORDER:
        rows = value[world]
        if not isinstance(rows, dict) or tuple(rows) != scopes:
            raise AllCasesPipelineError("Search-prefix meta training scopes differ")
        for digest in rows.values():
            _require_sha(digest, label="Search-prefix meta training rows")


def _validate_ml_candidate_row(
    ml: object,
    value: object,
    candidate: object,
    *,
    candidate_kind: str,
    candidate_schema: str,
) -> tuple[Mapping[str, object] | None, Mapping[str, object] | None]:
    """Strictly decode one immutable raw ML row and its nested typed artifacts."""

    meta = candidate_kind == "META_ML"
    keys = {
        "candidate",
        "control_alignment",
        "crossfit_summaries",
        "final_model_sha256_by_world",
        "frozen_model_artifact",
        "gate",
        "ineligibility",
        "null_feasibility",
        "schema",
        "search_controls",
        ("training_rows_sha256_by_world_and_fold" if meta else "training_rows_sha256"),
    }
    if (
        not isinstance(value, dict)
        or set(value) != keys
        or value.get("schema") != candidate_schema
        or _canonical_json_bytes(value.get("candidate"))
        != _canonical_json_bytes(candidate.as_dict())
    ):
        raise AllCasesPipelineError("Search-prefix ML candidate row schema differs")
    training_key = "training_rows_sha256_by_world_and_fold" if meta else "training_rows_sha256"
    ineligibility = value["ineligibility"]
    if ineligibility is not None:
        _validate_ml_ineligibility(ml, ineligibility, candidate.candidate_id)
        expected_empty_training: object = {} if meta else None
        if (
            value["control_alignment"] is not None
            or value["crossfit_summaries"] != []
            or value["final_model_sha256_by_world"] != []
            or value["frozen_model_artifact"] is not None
            or value["gate"] is not None
            or value["null_feasibility"] is not None
            or value["search_controls"] is not None
            or value[training_key] != expected_empty_training
        ):
            raise AllCasesPipelineError("Search-prefix ineligible ML row leaked fit evidence")
        return None, None

    summaries = value["crossfit_summaries"]
    model_hashes = value["final_model_sha256_by_world"]
    if (
        not isinstance(summaries, list)
        or len(summaries) != len(ml.NULL_WORLD_ORDER)
        or not isinstance(model_hashes, list)
        or len(model_hashes) != len(ml.NULL_WORLD_ORDER)
    ):
        raise AllCasesPipelineError("Search-prefix fitted ML world family differs")
    for summary, world in zip(summaries, ml.NULL_WORLD_ORDER, strict=True):
        _validate_crossfit_summary(ml, summary, candidate, world, meta=meta)
    for digest in model_hashes:
        _require_sha(digest, label="Search-prefix final model SHA")
    alignment = _decode_control_alignment(ml, value["control_alignment"], candidate.candidate_id)
    evaluations = _decode_ml_search_controls(ml, value["search_controls"], candidate)
    if any(item.alignment_proof_sha256 != alignment.artifact_sha256 for item in evaluations):
        raise AllCasesPipelineError("Search-prefix ML control proof differs")
    gate = _decode_ml_gate(ml, value["gate"], candidate)
    if gate.family_key != evaluations[0].family_key:
        raise AllCasesPipelineError("Search-prefix ML gate family differs")
    _validate_null_feasibility(ml, value["null_feasibility"], candidate, meta=meta)
    _validate_ml_training_commitment(ml, value[training_key], meta=meta)

    frozen = value["frozen_model_artifact"]
    if gate.eligible != (frozen is not None):
        raise AllCasesPipelineError("Search-prefix ML frozen-model eligibility differs")
    if frozen is None:
        return _validate_search_selection_row(
            _direct_selection_row_from_documents(
                value["search_controls"]["evaluations"][0], value["gate"]
            )
            if not meta
            else _meta_selection_row_from_documents(
                value["search_controls"]["evaluations"][0], value["gate"]
            )
        ) if gate.eligible else None, None
    if (
        not isinstance(frozen, dict)
        or set(frozen)
        != {
            "candidate_id",
            "candidate_kind",
            "family_key",
            "model_document",
            "model_sha256",
        }
        or frozen.get("candidate_id") != candidate.candidate_id
        or frozen.get("candidate_kind") != candidate_kind
        or frozen.get("family_key") != gate.family_key
        or not isinstance(frozen.get("model_document"), dict)
        or _sha256(frozen["model_document"]) != frozen.get("model_sha256")
    ):
        raise AllCasesPipelineError("Search-prefix frozen ML artifact differs")
    model_document = frozen["model_document"]
    model_keys = (
        {
            "candidate",
            "candidate_id",
            "candidate_kind",
            "control_alignment",
            "expert_artifact_commitment_sha256s",
            "family_key",
            "final_models",
            "final_permutation_plans",
            "gate",
            "meta_plan_sha256",
            "null_feasibility",
            "schema",
            "search_controls",
            "symbolic_order_batch_sha256s",
            "training_rows_sha256_by_world_and_fold",
        }
        if meta
        else {
            "candidate",
            "candidate_id",
            "candidate_kind",
            "control_alignment",
            "family_key",
            "final_models",
            "final_permutation_plans",
            "gate",
            "null_feasibility",
            "opportunity_lattice_sha256",
            "schema",
            "search_controls",
            "training_rows_sha256",
        }
    )
    expected_model_schema = (
        "systematic_fx.ai_all_cases_frozen_meta_model.v1"
        if meta
        else "systematic_fx.ai_all_cases_frozen_direct_model.v1"
    )
    if (
        set(model_document) != model_keys
        or model_document.get("schema") != expected_model_schema
        or _canonical_json_bytes(model_document.get("candidate"))
        != _canonical_json_bytes(candidate.as_dict())
        or model_document.get("candidate_id") != candidate.candidate_id
        or model_document.get("candidate_kind") != candidate_kind
        or model_document.get("family_key") != gate.family_key
        or _canonical_json_bytes(model_document.get("control_alignment"))
        != _canonical_json_bytes(value["control_alignment"])
        or _canonical_json_bytes(model_document.get("gate")) != _canonical_json_bytes(value["gate"])
        or _canonical_json_bytes(model_document.get("null_feasibility"))
        != _canonical_json_bytes(value["null_feasibility"])
        or _canonical_json_bytes(model_document.get("search_controls"))
        != _canonical_json_bytes(value["search_controls"])
        or _canonical_json_bytes(model_document.get(training_key))
        != _canonical_json_bytes(value[training_key])
    ):
        raise AllCasesPipelineError("Search-prefix frozen model document differs")
    if meta:
        _require_sha(model_document.get("meta_plan_sha256"), label="Search-prefix meta plan SHA")
        for key in (
            "expert_artifact_commitment_sha256s",
            "symbolic_order_batch_sha256s",
        ):
            values = model_document.get(key)
            if not isinstance(values, list):
                raise AllCasesPipelineError("Search-prefix meta model SHA family differs")
            for digest in values:
                _require_sha(digest, label=f"Search-prefix meta model {key}")
    else:
        _require_sha(
            model_document.get("opportunity_lattice_sha256"),
            label="Search-prefix direct opportunity lattice",
        )
    models = model_document.get("final_models")
    permutations = model_document.get("final_permutation_plans")
    if (
        not isinstance(models, list)
        or len(models) != len(ml.NULL_WORLD_ORDER)
        or not isinstance(permutations, list)
        or len(permutations) != len(ml.NULL_WORLD_ORDER)
    ):
        raise AllCasesPipelineError("Search-prefix final model family differs")
    try:
        reopened = tuple(
            ml.CanonicalMLModel.from_canonical_bytes(_canonical_json_bytes(item)) for item in models
        )
    except ml.AllCasesMLError as error:
        raise AllCasesPipelineError("Search-prefix final model differs") from error
    if (
        tuple(item.null_world for item in reopened) != ml.NULL_WORLD_ORDER
        or any(
            item.candidate_id != candidate.candidate_id or item.fold_key != "SEARCH_FINAL"
            for item in reopened
        )
        or [item.sha256 for item in reopened] != model_hashes
    ):
        raise AllCasesPipelineError("Search-prefix final model identity differs")
    for raw, world in zip(permutations, ml.NULL_WORLD_ORDER, strict=True):
        _decode_target_permutation(ml, raw, candidate.candidate_id, world)
    selection = _validate_search_selection_row(
        _direct_selection_row_from_documents(
            value["search_controls"]["evaluations"][0], value["gate"]
        )
        if not meta
        else _meta_selection_row_from_documents(
            value["search_controls"]["evaluations"][0], value["gate"]
        )
    )
    return selection, frozen


def _preflight_ml_search_prefix(
    ledger: _SearchSubledger,
    events: Sequence[_SearchEvent],
    *,
    feature_plan: object,
    stage_b_evidence: object,
    state: object,
) -> None:
    """Strictly decode persisted ML phases without touching outcome payloads."""

    from . import ml

    selection_rows_by_kind: dict[str, list[Mapping[str, object]]] = {
        "DIRECT_ML": [],
        "META_ML": [],
    }
    model_artifacts_by_kind: dict[str, list[Mapping[str, object]]] = {
        "DIRECT_ML": [],
        "META_ML": [],
    }
    cache_rows_by_kind: dict[str, list[object]] = {"DIRECT": [], "META": []}

    def validate_phase(
        phase: str,
        catalog: Sequence[object],
        *,
        candidate_kind: str,
        aggregate_kind: str,
        chunk_fit_cap: int,
        chunk_schema: str,
        candidate_schema: str,
    ) -> None:
        ranges = _balanced_chunk_ranges(len(catalog), 24)
        phase_events = tuple(event for event in events if event.phase == phase)
        for index, event in enumerate(phase_events):
            if index >= len(ranges):
                raise AllCasesPipelineError("Search-prefix ML chunk count differs")
            start, end = ranges[index]
            expected = tuple(catalog[start:end])
            payload = ledger._artifact_payload(event)
            rows = payload.get("candidates")
            if (
                payload.get("schema") != chunk_schema
                or type(payload.get("candidate_count")) is not int
                or payload.get("candidate_count") != len(expected)
                or type(payload.get("first_catalog_rank")) is not int
                or payload.get("first_catalog_rank") != expected[0].selection_rank
                or type(payload.get("last_catalog_rank")) is not int
                or payload.get("last_catalog_rank") != expected[-1].selection_rank
                or not isinstance(rows, list)
                or len(rows) != len(expected)
            ):
                raise AllCasesPipelineError("Search-prefix ML chunk coordinate differs")
            try:
                cache = ml.SharedFitCacheEvidence.from_dict(payload.get("fit_cache_evidence"))
            except ml.AllCasesMLError as error:
                raise AllCasesPipelineError("Search-prefix ML cache evidence differs") from error
            if cache.maximum_fit_count != chunk_fit_cap:
                raise AllCasesPipelineError("Search-prefix ML fit cap differs")
            cache_rows_by_kind[aggregate_kind].append(cache)
            for candidate, row in zip(expected, rows, strict=True):
                selection, frozen = _validate_ml_candidate_row(
                    ml,
                    row,
                    candidate,
                    candidate_kind=candidate_kind,
                    candidate_schema=candidate_schema,
                )
                if selection is not None:
                    selection_rows_by_kind[candidate_kind].append(selection)
                if frozen is not None:
                    model_artifacts_by_kind[candidate_kind].append(frozen)

    validate_phase(
        "DIRECT_ML_CHUNKS",
        ml.build_direct_candidate_catalog(),
        candidate_kind="DIRECT_ML",
        aggregate_kind="DIRECT",
        chunk_fit_cap=126,
        chunk_schema="systematic_fx.ai_all_cases_direct_search_chunk.v1",
        candidate_schema="systematic_fx.ai_all_cases_direct_search_candidate.v1",
    )
    meta_plan_events = tuple(event for event in events if event.phase == "META_PLAN_FROZEN")
    symbolic_selection_rows: list[Mapping[str, object]] = []
    symbolic_frozen_artifacts: list[Mapping[str, object]] = []
    if meta_plan_events:
        if len(meta_plan_events) != 1:
            raise AllCasesPipelineError("Search-prefix meta plan count differs")
        plan = ledger._artifact_payload(meta_plan_events[0])
        if (
            set(plan)
            != {
                "certificates_by_world_and_scope",
                "schema",
                "source_symbolic_top24_sha256",
                "symbolic_frozen_artifacts",
                "symbolic_selection_rows",
            }
            or plan.get("schema") != "systematic_fx.ai_all_cases_meta_rank_slot_plan.v1"
            or not isinstance(plan.get("symbolic_frozen_artifacts"), list)
            or not isinstance(plan.get("symbolic_selection_rows"), list)
        ):
            raise AllCasesPipelineError("Search-prefix meta plan schema differs")
        worlds = plan.get("certificates_by_world_and_scope")
        scopes = (*ml.SEARCH_OUTER_FOLD_KEYS, "SEARCH_FINAL")
        expected_source = _sha256(stage_b_evidence.top24_document)
        selected_symbolic_ids = tuple(stage_b_evidence.symbolic_selection.selected_strategy_ids)
        raw_symbolic_rows = plan["symbolic_selection_rows"]
        raw_symbolic_artifacts = plan["symbolic_frozen_artifacts"]
        if (
            plan.get("source_symbolic_top24_sha256") != expected_source
            or not isinstance(worlds, Mapping)
            or set(worlds) != set(ml.NULL_WORLD_ORDER)
            or not isinstance(raw_symbolic_rows, list)
            or not isinstance(raw_symbolic_artifacts, list)
            or len(raw_symbolic_rows) != len(selected_symbolic_ids)
            or len(raw_symbolic_artifacts) != len(selected_symbolic_ids)
        ):
            raise AllCasesPipelineError("Search-prefix meta plan worlds differ")
        recipe_by_id = {item.strategy_id: item for item in feature_plan.recipes}
        gates_by_id = {item.strategy_id: item for item in stage_b_evidence.gate_results}
        row_ids = []
        for raw in raw_symbolic_rows:
            row = _validate_search_selection_row(raw)
            if row["candidate_kind"] != "SYMBOLIC":
                raise AllCasesPipelineError("Search-prefix symbolic selection kind differs")
            row_ids.append(row["candidate_id"])
            symbolic_selection_rows.append(row)
        artifact_ids = []
        for raw in raw_symbolic_artifacts:
            if not isinstance(raw, dict) or set(raw) != {
                "candidate_id",
                "candidate_kind",
                "family_key",
                "strategy_document",
                "strategy_sha256",
            }:
                raise AllCasesPipelineError("Search-prefix symbolic artifact schema differs")
            candidate_id = _require_sha(
                raw.get("candidate_id"), label="Search-prefix symbolic candidate ID"
            )
            recipe = recipe_by_id.get(candidate_id)
            policy = (
                None if recipe is None else feature_plan.policies_by_id.get(recipe.anchor_policy_id)
            )
            family = (
                None
                if recipe is None
                else feature_plan.family_by_policy_id.get(recipe.anchor_policy_id)
            )
            gate = gates_by_id.get(candidate_id)
            document = raw.get("strategy_document")
            if (
                raw.get("candidate_kind") != "SYMBOLIC"
                or raw.get("family_key") != family
                or recipe is None
                or policy is None
                or gate is None
                or not isinstance(document, dict)
                or set(document)
                != {
                    "anchor_policy",
                    "candidate_id",
                    "candidate_kind",
                    "catalog_selection_rank",
                    "detail",
                    "family_key",
                    "recipe",
                    "schema",
                    "search_evaluation_artifact_sha256",
                    "search_gate",
                }
                or document.get("schema")
                != "systematic_fx.ai_all_cases_frozen_symbolic_strategy.v1"
                or document.get("candidate_id") != candidate_id
                or document.get("candidate_kind") != "SYMBOLIC"
                or type(document.get("catalog_selection_rank")) is not int
                or document.get("catalog_selection_rank") != recipe.strategy_rank
                or document.get("family_key") != family
                or _canonical_json_bytes(document.get("recipe"))
                != _canonical_json_bytes(recipe.as_dict())
                or _canonical_json_bytes(document.get("anchor_policy"))
                != _canonical_json_bytes(policy.as_dict())
                or _canonical_json_bytes(document.get("search_gate"))
                != _canonical_json_bytes(gate.as_dict())
                or document.get("search_evaluation_artifact_sha256")
                != next(
                    (
                        item.artifact_sha256
                        for item in stage_b_evidence.representative_real_evaluations
                        if item.recipe.strategy_id == candidate_id
                    ),
                    None,
                )
                or not isinstance(document.get("detail"), dict)
                or _sha256(document) != raw.get("strategy_sha256")
            ):
                raise AllCasesPipelineError("Search-prefix symbolic artifact binding differs")
            artifact_ids.append(candidate_id)
            symbolic_frozen_artifacts.append(raw)
        if tuple(row_ids) != selected_symbolic_ids or tuple(artifact_ids) != selected_symbolic_ids:
            raise AllCasesPipelineError("Search-prefix symbolic plan order differs")
        search_plan = ml.build_search_block_plan(state.plan.decision_dates)
        training_dates = {
            fold.fold_key: tuple(fold.training_dates) for fold in search_plan.outer_folds
        }
        training_dates["SEARCH_FINAL"] = tuple(search_plan.decision_dates)
        symbolic_world_by_ml = {
            "REAL": "REAL",
            "CIRCULAR_TARGET": "CIRCULAR",
            "MATCHED_TARGET": "MATCHED",
        }
        for world in ml.NULL_WORLD_ORDER:
            raw_scopes = worlds[world]
            if not isinstance(raw_scopes, Mapping) or tuple(raw_scopes) != scopes:
                raise AllCasesPipelineError("Search-prefix meta plan scopes differ")
            for scope in scopes:
                certificate = ml.SymbolicRankingCertificate.from_dict(raw_scopes[scope])
                ranking = stage_b_evidence.top24_by_world_and_scope[symbolic_world_by_ml[world]][
                    scope
                ]
                ranked = []
                for rank, strategy_id in enumerate(ranking.selected_strategy_ids, start=1):
                    recipe = recipe_by_id.get(strategy_id)
                    if recipe is None:
                        raise AllCasesPipelineError(
                            "Search-prefix meta ranking escapes recipe family"
                        )
                    policy = feature_plan.policies_by_id[recipe.anchor_policy_id]
                    ranked.append(
                        ml.RankedSymbolicStrategy(
                            rank,
                            strategy_id,
                            feature_plan.family_by_policy_id[recipe.anchor_policy_id],
                            recipe.anchor_policy_id,
                            policy.base_candidate_id,
                            policy.context_id,
                            policy.time_filter_id,
                            policy.delay_id,
                            recipe.entry_policy_id,
                            recipe.exit_policy_id,
                        )
                    )
                expected_certificate = ml.build_symbolic_ranking_certificate(
                    null_world=world,
                    fold_key=scope,
                    training_dates=training_dates[scope],
                    ranked_strategies=ranked,
                )
                if certificate != expected_certificate:
                    raise AllCasesPipelineError("Search-prefix meta certificate differs")
    validate_phase(
        "META_ML_CHUNKS",
        ml.build_meta_candidate_catalog(),
        candidate_kind="META_ML",
        aggregate_kind="META",
        chunk_fit_cap=84,
        chunk_schema="systematic_fx.ai_all_cases_meta_search_chunk.v1",
        candidate_schema="systematic_fx.ai_all_cases_meta_search_candidate.v1",
    )
    final_events = tuple(event for event in events if event.phase == "FINAL_MAX12")
    if final_events:
        if len(final_events) != 1:
            raise AllCasesPipelineError("Search-prefix final barrier count differs")
        final = ledger._artifact_payload(final_events[0])
        raw_selected = final.get("selected_candidate_ids")
        if (
            not isinstance(raw_selected, list)
            or len(raw_selected) > 12
            or any(not isinstance(item, str) for item in raw_selected)
            or len(set(raw_selected)) != len(raw_selected)
        ):
            raise AllCasesPipelineError("Search-prefix final selected IDs differ")
        for candidate_id in raw_selected:
            _require_sha(candidate_id, label="Search-prefix final selected candidate ID")
        all_selection_rows = [
            *symbolic_selection_rows,
            *selection_rows_by_kind["DIRECT_ML"],
            *selection_rows_by_kind["META_ML"],
        ]
        expected_selected = _select_diverse_search_candidates(all_selection_rows)
        direct_by_id = {item["candidate_id"]: item for item in model_artifacts_by_kind["DIRECT_ML"]}
        meta_by_id = {item["candidate_id"]: item for item in model_artifacts_by_kind["META_ML"]}
        symbolic_by_id = {item["candidate_id"]: item for item in symbolic_frozen_artifacts}
        if len(cache_rows_by_kind["DIRECT"]) != 24 or len(cache_rows_by_kind["META"]) != 24:
            raise AllCasesPipelineError("Search-prefix final cache family is incomplete")
        try:
            direct_cache = ml.aggregate_shared_fit_cache_evidence(
                "DIRECT", tuple(cache_rows_by_kind["DIRECT"])
            ).as_dict()
            meta_cache = ml.aggregate_shared_fit_cache_evidence(
                "META", tuple(cache_rows_by_kind["META"])
            ).as_dict()
        except ml.AllCasesMLError as error:
            raise AllCasesPipelineError("Search-prefix final cache aggregate differs") from error
        expected_model_ids = tuple(
            item for item in expected_selected if item in direct_by_id or item in meta_by_id
        )
        expected_strategy_ids = tuple(item for item in expected_selected if item in symbolic_by_id)
        expected_final = {
            "eligible_candidate_evidence_sha256": _sha256(all_selection_rows),
            "fit_cache_aggregate_sha256s": [
                direct_cache["artifact_sha256"],
                meta_cache["artifact_sha256"],
            ],
            "model_artifact_sha256s": [
                (direct_by_id.get(candidate_id) or meta_by_id[candidate_id])["model_sha256"]
                for candidate_id in expected_model_ids
            ],
            "schema": "systematic_fx.ai_all_cases_search_final_selection.v1",
            "selected_candidate_ids": list(expected_selected),
            "strategy_artifact_sha256s": [
                symbolic_by_id[candidate_id]["strategy_sha256"]
                for candidate_id in expected_strategy_ids
            ],
        }
        if _canonical_json_bytes(final) != _canonical_json_bytes(expected_final):
            raise AllCasesPipelineError("Search-prefix final barrier differs")


def _safe_directory(path: Path, *, create: bool) -> Path:
    absolute = path.absolute()
    for candidate in (absolute, *absolute.parents):
        if not candidate.exists() and not candidate.is_symlink():
            continue
        metadata = candidate.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise AllCasesPipelineError("internal directory has an unsafe ancestor")
    if create:
        absolute.mkdir(parents=True, exist_ok=True)
    try:
        resolved = path.resolve(strict=True)
    except FileNotFoundError as error:
        raise AllCasesPipelineError("internal directory is missing") from error
    if resolved != absolute or not resolved.is_dir():
        raise AllCasesPipelineError("internal directory is unsafe")
    return resolved


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError("internal atomic write made no progress")
        view = view[written:]


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _recover_staging(staging: Path, published_roots: Sequence[Path]) -> None:
    paths = tuple(staging.iterdir())
    if len(paths) > 1:
        raise AllCasesPipelineError("internal staging contains multiple crash orphans")
    for path in paths:
        metadata = path.lstat()
        if (
            path.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or not path.name.startswith(".chunk-")
            or not path.name.endswith(".tmp")
            or metadata.st_uid != os.geteuid()
            or metadata.st_nlink not in {1, 2}
        ):
            raise AllCasesPipelineError("internal staging contains an unsafe orphan")
        if metadata.st_nlink == 2:
            companions = []
            for published_root in published_roots:
                for candidate in published_root.iterdir():
                    if candidate.is_symlink() or not candidate.is_file():
                        continue
                    candidate_metadata = candidate.stat()
                    if (candidate_metadata.st_dev, candidate_metadata.st_ino) == (
                        metadata.st_dev,
                        metadata.st_ino,
                    ):
                        companions.append(candidate)
            if (
                len(companions) != 1
                or companions[0].suffix != ".json"
                or companions[0].stat().st_mode & _WRITE_BITS
            ):
                raise AllCasesPipelineError("linked internal staging orphan is unsafe")
        path.unlink()
        _fsync_directory(staging)


def _universe_prefix_leaves(
    project_root: Path,
    config: AllCasesConfig,
    *,
    recover: bool,
) -> tuple[dict[str, object], ...]:
    """Recover and structurally verify the outcome-free universe chunk prefix."""

    from . import symbolic

    run_root = project_root / _RUN_RELATIVE_ROOT
    root_path = run_root / "internal/universe"
    staging_path = run_root / "internal/universe-staging"
    if not root_path.exists() and not staging_path.exists():
        return ()
    if not root_path.is_dir():
        raise AllCasesPipelineError("feature-universe store is structurally incomplete")
    root = _safe_directory(root_path, create=False)
    if not staging_path.exists():
        if not recover:
            raise AllCasesPipelineError("feature-universe staging directory is missing")
        staging = _safe_directory(staging_path, create=True)
    else:
        staging = _safe_directory(staging_path, create=False)
    if recover:
        _recover_staging(staging, (root,))
    elif any(staging.iterdir()):
        raise AllCasesPipelineError("feature-universe verification found staging bytes")

    plan = symbolic.build_stage_a_chunk_plan()
    paths = sorted(root.iterdir())
    if len(paths) > len(plan):
        raise AllCasesPipelineError("feature-universe prefix exceeds its frozen plan")
    leaves = []
    common_payload_keys = {
        "artifact_sha256",
        "candidate_cubes",
        "chunk",
        "policies",
        "schema",
        "structural_opportunity_lattice_sha256",
    }
    first_payload_keys = common_payload_keys | {
        "direct_feature_summaries",
        "direct_feature_universe_sha256",
        "direct_opportunity_lattice",
        "structural_opportunity_lattice",
    }
    for expected_index, path in enumerate(paths):
        metadata = path.lstat()
        if (
            path.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or metadata.st_nlink != 1
            or metadata.st_mode & _WRITE_BITS
        ):
            raise AllCasesPipelineError("feature-universe prefix contains unsafe bytes")
        raw = path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        try:
            document = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise AllCasesPipelineError("feature-universe prefix is invalid JSON") from error
        if (
            not isinstance(document, dict)
            or _canonical_json_bytes(document) != raw
            or set(document)
            != {
                "artifact_schema",
                "chunk_index",
                "config_semantic_sha256",
                "payload",
            }
            or document.get("artifact_schema")
            != "systematic_fx.ai_all_cases_feature_universe_chunk.v1"
            or type(document.get("chunk_index")) is not int
            or document.get("chunk_index") != expected_index
            or document.get("config_semantic_sha256") != config.semantic_sha256
            or not isinstance(document.get("payload"), dict)
            or path.name != f"universe-{expected_index:03d}-{digest}.json"
        ):
            raise AllCasesPipelineError("feature-universe prefix envelope differs")
        payload = document["payload"]
        expected_keys = first_payload_keys if expected_index == 0 else common_payload_keys
        if (
            set(payload) != expected_keys
            or payload.get("schema") != symbolic.MASK_SCHEMA
            or _canonical_json_bytes(payload.get("chunk"))
            != _canonical_json_bytes(plan[expected_index].as_dict())
            or not isinstance(payload.get("candidate_cubes"), list)
            or not isinstance(payload.get("policies"), list)
            or len(payload["policies"]) != plan[expected_index].policy_count
        ):
            raise AllCasesPipelineError("feature-universe prefix payload differs")
        definition = {
            "candidate_cubes": payload["candidate_cubes"],
            "chunk": payload["chunk"],
            "policies": payload["policies"],
            "schema": payload["schema"],
        }
        if payload.get("artifact_sha256") != _sha256(definition):
            raise AllCasesPipelineError("feature-universe commitment hash differs")
        _require_sha(
            payload.get("structural_opportunity_lattice_sha256"),
            label="feature-universe structural lattice SHA",
        )
        if expected_index == 0:
            _require_sha(
                payload.get("direct_feature_universe_sha256"),
                label="direct feature-universe SHA",
            )
            if not isinstance(payload.get("direct_feature_summaries"), list):
                raise AllCasesPipelineError("direct feature-universe summaries differ")
            structural = symbolic.structural_eligibility_lattice_from_dict(
                payload.get("structural_opportunity_lattice")
            )
            direct = symbolic.direct_opportunity_lattice_from_dict(
                payload.get("direct_opportunity_lattice")
            )
            if (
                structural.artifact_sha256 != payload["structural_opportunity_lattice_sha256"]
                or direct.structural_lattice_sha256 != structural.artifact_sha256
            ):
                raise AllCasesPipelineError("feature-universe lattice binding differs")
        leaves.append(
            {
                "artifact_sha256": digest,
                "chunk_index": expected_index,
                "relative_path": path.name,
            }
        )
    return tuple(leaves)


def recover_and_verify_internal_prefix(
    project_root: Path,
    config: AllCasesConfig,
) -> None:
    """Recover bounded publisher temps and verify stores before any outcome service."""

    _universe_prefix_leaves(project_root, config, recover=True)
    search_root = project_root / _SEARCH_INTERNAL_RELATIVE_ROOT
    if search_root.exists():
        if not search_root.is_dir():
            raise AllCasesPipelineError("Search internal store is not a directory")
        ledger = _SearchSubledger(project_root, config, create=True)
        # An artifact published before its event is not adopted across an
        # invocation boundary.  verify() proves the sole orphan is the exact
        # next coordinate; discard it and let source replay rebuild the bytes.
        ledger.discard_validated_artifact_orphan()
        ledger.verify_exact_artifact_closure()


def verify_no_internal_evidence_before_precommit(project_root: Path) -> None:
    """Reject any internal leaf before the outer PRECOMMITTED event exists."""

    run_root = project_root / _RUN_RELATIVE_ROOT
    for relative in (
        Path("internal/universe"),
        Path("internal/universe-staging"),
        Path("internal/search"),
    ):
        root = run_root / relative
        if not root.exists() and not root.is_symlink():
            continue
        if root.is_symlink() or not root.is_dir():
            raise AllCasesPipelineError("precommit internal namespace is unsafe")
        for path in root.rglob("*"):
            metadata = path.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
                raise AllCasesPipelineError("internal evidence exists before PRECOMMITTED")


def verify_search_store_empty_before_universe_release(project_root: Path) -> None:
    """Search persistence is illegal until the outer universe barrier is durable."""

    root = project_root / _SEARCH_INTERNAL_RELATIVE_ROOT
    if not root.exists() and not root.is_symlink():
        return
    if root.is_symlink() or not root.is_dir():
        raise AllCasesPipelineError("pre-universe Search namespace is unsafe")
    for path in root.rglob("*"):
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise AllCasesPipelineError("Search evidence predates the universe release")


def verify_search_prefix_semantics(
    project_root: Path,
    config: AllCasesConfig,
    universe: Mapping[str, object],
) -> None:
    """Outcome-free typed replay of every observed Search-prefix barrier.

    Source recomputation follows in the production Search service.  This early
    pass ensures malformed canonical bytes cannot trigger a 1-second outcome
    read before their typed coordinates and feature-only derivations fail.
    """

    from . import symbolic

    search_root = project_root / _SEARCH_INTERNAL_RELATIVE_ROOT
    if not search_root.exists():
        return
    ledger = _SearchSubledger(project_root, config, create=False)
    events = ledger.verify()
    if not events:
        return
    state = _search_feature_state(project_root)
    if state.structural_lattice.artifact_sha256 != universe.get(
        "structural_opportunity_lattice_sha256"
    ):
        raise AllCasesPipelineError("Search prefix structural lattice differs")
    plan = tuple(symbolic.build_stage_a_chunk_plan())
    score_events = tuple(event for event in events if event.phase == "STAGE_A_SCORE_CHUNKS")
    score_chunks = []
    for index, event in enumerate(score_events):
        payload = ledger._artifact_payload(event)
        if (
            set(payload) != {"schema", "score_chunk", "structural_lattice_sha256"}
            or payload.get("schema") != "systematic_fx.ai_all_cases_stage_a_raw_chunk.v1"
            or payload.get("structural_lattice_sha256") != state.structural_lattice.artifact_sha256
        ):
            raise AllCasesPipelineError("Search-prefix Stage-A raw schema differs")
        chunk = symbolic.stage_a_score_chunk_from_dict(payload.get("score_chunk"))
        if index >= len(plan) or chunk.chunk != plan[index]:
            raise AllCasesPipelineError("Search-prefix Stage-A coordinate differs")
        score_chunks.append(chunk)

    selection_events = tuple(event for event in events if event.phase == "STAGE_A_TOP256")
    if not selection_events:
        return
    if len(score_chunks) != len(plan) or len(selection_events) != 1:
        raise AllCasesPipelineError("Search-prefix Stage-A barrier is premature")
    selection_payload = ledger._artifact_payload(selection_events[0])
    replayed_selection = _select_stage_a_from_score_chunks(config, score_chunks)
    expected_selection_payload = {
        "schema": "systematic_fx.ai_all_cases_stage_a_selection.v1",
        "selection": replayed_selection.as_dict(),
        "source_chunk_artifact_sha256s": [item.artifact_sha256 for item in score_chunks],
    }
    if _canonical_json_bytes(selection_payload) != _canonical_json_bytes(
        expected_selection_payload
    ):
        raise AllCasesPipelineError("Search-prefix Stage-A selector replay differs")
    if (
        symbolic.stage_a_selection_from_dict(selection_payload.get("selection"))
        != replayed_selection
    ):
        raise AllCasesPipelineError("Search-prefix Stage-A typed selection differs")

    plan_events = tuple(event for event in events if event.phase == "STAGE_B_PLAN_FROZEN")
    if not plan_events:
        return
    if len(plan_events) != 1:
        raise AllCasesPipelineError("Search-prefix Stage-B plan count differs")
    feature_plan = _ensure_stage_b_feature_plan(
        config,
        ledger,
        state,
        replayed_selection,
        verify_only=True,
    )
    raw_events = tuple(event for event in events if event.phase == "STAGE_B_RAW_CHUNKS")
    evaluations: dict[str, dict[str, object]] = {
        world: {} for world in ("REAL", "CIRCULAR", "MATCHED")
    }
    coverage: dict[str, dict[str, object]] = {
        world: {} for world in ("REAL", "CIRCULAR", "MATCHED")
    }
    ineligibility: dict[str, dict[str, str]] = {
        world: {} for world in ("REAL", "CIRCULAR", "MATCHED")
    }
    for index, event in enumerate(raw_events):
        _consume_stage_b_raw_chunk(
            symbolic,
            index=index,
            payload=ledger._artifact_payload(event),
            chunks=feature_plan.chunks,
            recipes=feature_plan.recipes,
            masks_by_world=feature_plan.masks_by_world,
            evaluations=evaluations,
            coverage=coverage,
            ineligibility=ineligibility,
        )
    top24_events = tuple(event for event in events if event.phase == "SYMBOLIC_TOP24")
    if not top24_events:
        return
    if len(raw_events) != len(feature_plan.chunks) or len(top24_events) != 1:
        raise AllCasesPipelineError("Search-prefix symbolic barrier is premature")
    replayed = _aggregate_stage_b_search_evidence(
        symbolic,
        feature_plan.recipes,
        feature_plan.family_by_policy_id,
        evaluations,
        coverage,
        ineligibility,
    )
    if _canonical_json_bytes(ledger._artifact_payload(top24_events[0])) != _canonical_json_bytes(
        replayed.top24_document
    ):
        raise AllCasesPipelineError("Search-prefix symbolic aggregate replay differs")
    _preflight_ml_search_prefix(
        ledger,
        events,
        feature_plan=feature_plan,
        stage_b_evidence=replayed,
        state=state,
    )


def verify_observed_internal_prefixes(
    project_root: Path,
    config: AllCasesConfig,
) -> None:
    """Read-only validate every internal store that is present, released or not."""

    _universe_prefix_leaves(project_root, config, recover=False)
    search_root = project_root / _SEARCH_INTERNAL_RELATIVE_ROOT
    if search_root.exists():
        if not search_root.is_dir():
            raise AllCasesPipelineError("Search internal store is not a directory")
        _SearchSubledger(project_root, config, create=False).verify()


def close_internal_prefix_for_terminal_failure(
    project_root: Path,
    config: AllCasesConfig,
) -> None:
    """Recover publisher temps and remove only a validated next Search artifact orphan."""

    _universe_prefix_leaves(project_root, config, recover=True)
    search_root = project_root / _SEARCH_INTERNAL_RELATIVE_ROOT
    if search_root.exists():
        if not search_root.is_dir():
            raise AllCasesPipelineError("Search internal store is not a directory")
        ledger = _SearchSubledger(project_root, config, create=True)
        ledger.discard_validated_artifact_orphan()
        ledger.verify_exact_artifact_closure()


def verify_terminal_internal_prefixes(
    project_root: Path,
    config: AllCasesConfig,
) -> None:
    """Read-only verify terminal internal prefixes with no resumable artifact orphan."""

    _universe_prefix_leaves(project_root, config, recover=False)
    search_root = project_root / _SEARCH_INTERNAL_RELATIVE_ROOT
    if search_root.exists():
        if not search_root.is_dir():
            raise AllCasesPipelineError("Search internal store is not a directory")
        _SearchSubledger(project_root, config, create=False).verify_exact_artifact_closure()


def verify_internal_universe_release(
    project_root: Path,
    config: AllCasesConfig,
    universe: Mapping[str, object],
) -> None:
    """Cross-bind the exact 64 internal universe leaves to the public barrier."""

    root = project_root / _RUN_RELATIVE_ROOT / "internal/universe"
    production_release = config.as_dict().get("config_id") == AI_ALL_CASES_CONFIG_ID
    if (
        production_release
        and universe.get("schema") != "systematic_fx.ai_all_cases_search_universe_payload.v1"
    ):
        raise AllCasesPipelineError("released feature-universe payload schema differs")
    if not root.exists():
        if production_release:
            raise AllCasesPipelineError("released feature-universe store is missing")
        return
    observed = _universe_prefix_leaves(project_root, config, recover=False)
    expected = universe.get("feature_mask_chunk_artifacts")
    if not isinstance(expected, list) or [dict(item) for item in observed] != expected:
        raise AllCasesPipelineError("feature-universe public leaf closure differs")


def _publish_mode_0444(
    root: Path,
    relative_path: str,
    raw: bytes,
    *,
    staging: Path,
) -> None:
    destination = root / relative_path
    if destination.exists():
        if (
            destination.is_symlink()
            or not destination.is_file()
            or destination.stat().st_nlink != 1
            or destination.stat().st_mode & _WRITE_BITS
            or destination.read_bytes() != raw
        ):
            raise AllCasesPipelineError("existing internal artifact differs")
        return
    descriptor, temporary_name = tempfile.mkstemp(prefix=".chunk-", suffix=".tmp", dir=staging)
    temporary = Path(temporary_name)
    try:
        _write_all(descriptor, raw)
        os.fchmod(descriptor, 0o444)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, destination, follow_symlinks=False)
        except FileExistsError:
            pass
        directory = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        else:
            _fsync_directory(staging)
    if destination.read_bytes() != raw or destination.stat().st_mode & _WRITE_BITS:
        raise AllCasesPipelineError("internal artifact publication differs")


@dataclass(frozen=True, slots=True)
class _SearchLeaf:
    phase: str
    chunk_index: int
    artifact_sha256: str
    relative_path: str

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_sha256": self.artifact_sha256,
            "chunk_index": self.chunk_index,
            "phase": self.phase,
            "relative_path": self.relative_path,
        }


@dataclass(frozen=True, slots=True)
class _SearchEvent:
    sequence: int
    predecessor_sha256: str | None
    phase: str
    chunk_index: int
    artifact_sha256: str
    artifact_relative_path: str
    recorded_at_utc: str

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_relative_path": self.artifact_relative_path,
            "artifact_schema": _SUBLEDGER_EVENT_SCHEMA,
            "artifact_sha256": self.artifact_sha256,
            "chunk_index": self.chunk_index,
            "phase": self.phase,
            "predecessor_sha256": self.predecessor_sha256,
            "recorded_at_utc": self.recorded_at_utc,
            "sequence": self.sequence,
        }

    @property
    def sha256(self) -> str:
        return _sha256(self.as_dict())


class _SearchSubledger:
    """Internal immutable chunk store with deterministic lowest-missing resume."""

    def __init__(
        self,
        project_root: Path,
        config: AllCasesConfig,
        *,
        create: bool,
        resources: _PipelineResourceGuard | None = None,
        allow_incomplete_verify: bool = False,
    ) -> None:
        self.config = config
        phase_counts_raw = (
            config.as_dict()
            .get("search_design", {})
            .get("search_phase_chunk_counts_canonical_json")
        )
        try:
            phase_counts = json.loads(phase_counts_raw)
        except (TypeError, json.JSONDecodeError) as error:
            raise AllCasesPipelineError("internal phase-count contract is invalid") from error
        if (
            not isinstance(phase_counts, dict)
            or set(phase_counts) != set(_SEARCH_PHASE_ORDER)
            or any(
                isinstance(value, bool) or not isinstance(value, int) or value < 1
                for value in phase_counts.values()
            )
        ):
            raise AllCasesPipelineError("internal phase-count contract differs")
        self.expected_phase_counts = {phase: phase_counts[phase] for phase in _SEARCH_PHASE_ORDER}
        self.resources = (
            resources
            if resources is not None
            else _PipelineResourceGuard(project_root, config, verifier=not create)
        )
        self.allow_incomplete_verify = allow_incomplete_verify
        self.root = _safe_directory(project_root / _SEARCH_INTERNAL_RELATIVE_ROOT, create=create)
        self.artifacts = _safe_directory(self.root / "artifacts", create=create)
        self.events = _safe_directory(self.root / "events", create=create)
        self.staging = _safe_directory(self.root / "staging", create=create)
        if create:
            _recover_staging(self.staging, (self.artifacts, self.events))
        elif any(self.staging.iterdir()):
            raise AllCasesPipelineError("read-only verification found a staging orphan")
        self.resources.check("SEARCH_SUBLEDGER_OPEN")

    def _artifact_payload(self, event: _SearchEvent) -> dict[str, object]:
        path = self.artifacts / event.artifact_relative_path
        if (
            path.is_symlink()
            or not path.is_file()
            or path.resolve(strict=True).parent != self.artifacts
            or path.stat().st_nlink != 1
            or path.stat().st_mode & _WRITE_BITS
        ):
            raise AllCasesPipelineError("internal chunk artifact is unsafe")
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != event.artifact_sha256:
            raise AllCasesPipelineError("internal chunk artifact hash differs")
        try:
            document = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise AllCasesPipelineError("internal chunk artifact is invalid JSON") from error
        if (
            not isinstance(document, dict)
            or _canonical_json_bytes(document) != raw
            or set(document)
            != {
                "artifact_schema",
                "chunk_index",
                "config_semantic_sha256",
                "payload",
                "phase",
            }
            or type(document.get("chunk_index")) is not int
            or not isinstance(document.get("config_semantic_sha256"), str)
            or not isinstance(document.get("phase"), str)
            or document.get("artifact_schema") != _SUBLEDGER_ARTIFACT_SCHEMA
            or document.get("config_semantic_sha256") != self.config.semantic_sha256
            or document.get("phase") != event.phase
            or document.get("chunk_index") != event.chunk_index
            or not isinstance(document.get("payload"), dict)
        ):
            raise AllCasesPipelineError("internal chunk envelope differs")
        _validate_raw_chunk_payload(document["payload"], phase=event.phase)
        return document["payload"]

    def verify(self) -> tuple[_SearchEvent, ...]:
        self.resources.check("SEARCH_SUBLEDGER_VERIFY_START")
        paths = sorted(self.events.iterdir())
        if any(
            path.is_symlink()
            or not path.is_file()
            or path.stat().st_nlink != 1
            or path.stat().st_mode & _WRITE_BITS
            for path in paths
        ):
            raise AllCasesPipelineError("internal subledger contains an unsafe event")
        output: list[_SearchEvent] = []
        predecessor: str | None = None
        phase_position = {phase: index for index, phase in enumerate(_SEARCH_PHASE_ORDER)}
        next_index: dict[str, int] = defaultdict(int)
        prior_coordinate: tuple[int, int] | None = None
        prior_phase: str | None = None
        for expected, path in enumerate(paths, start=1):
            if path.name != f"event-{expected:08d}.json" or path.stat().st_mode & _WRITE_BITS:
                raise AllCasesPipelineError("internal subledger sequence or mode differs")
            raw = path.read_bytes()
            try:
                document = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise AllCasesPipelineError("internal event is invalid JSON") from error
            if (
                not isinstance(document, dict)
                or _canonical_json_bytes(document) != raw
                or document.get("artifact_schema") != _SUBLEDGER_EVENT_SCHEMA
                or set(document)
                != {
                    "artifact_relative_path",
                    "artifact_schema",
                    "artifact_sha256",
                    "chunk_index",
                    "phase",
                    "predecessor_sha256",
                    "recorded_at_utc",
                    "sequence",
                }
            ):
                raise AllCasesPipelineError("internal event schema differs")
            if (
                type(document["sequence"]) is not int
                or type(document["chunk_index"]) is not int
                or not isinstance(document["phase"], str)
                or not isinstance(document["artifact_sha256"], str)
                or not isinstance(document["artifact_relative_path"], str)
                or not isinstance(document["recorded_at_utc"], str)
                or (
                    document["predecessor_sha256"] is not None
                    and not isinstance(document["predecessor_sha256"], str)
                )
            ):
                raise AllCasesPipelineError("internal event value types differ")
            event = _SearchEvent(
                document["sequence"],
                document["predecessor_sha256"],
                document["phase"],
                document["chunk_index"],
                document["artifact_sha256"],
                document["artifact_relative_path"],
                document["recorded_at_utc"],
            )
            try:
                parsed_timestamp = datetime.strptime(
                    event.recorded_at_utc, "%Y-%m-%dT%H:%M:%S.%fZ"
                ).replace(tzinfo=UTC)
            except ValueError as error:
                raise AllCasesPipelineError("internal event timestamp differs") from error
            if parsed_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ") != event.recorded_at_utc:
                raise AllCasesPipelineError("internal event timestamp is not canonical")
            if _canonical_json_bytes(event.as_dict()) != raw:
                raise AllCasesPipelineError("internal event canonical values differ")
            _require_sha(event.artifact_sha256, label="internal artifact SHA")
            if event.predecessor_sha256 is not None:
                _require_sha(event.predecessor_sha256, label="internal predecessor SHA")
            expected_artifact_name = (
                f"{event.phase.lower()}-{event.chunk_index:06d}-{event.artifact_sha256}.json"
            )
            if event.phase not in phase_position or event.predecessor_sha256 != predecessor:
                raise AllCasesPipelineError("internal event phase or predecessor differs")
            coordinate = phase_position[event.phase], event.chunk_index
            if (
                event.sequence != expected
                or event.chunk_index != next_index[event.phase]
                or event.chunk_index >= self.expected_phase_counts[event.phase]
                or (prior_coordinate is not None and coordinate <= prior_coordinate)
                or event.artifact_relative_path != expected_artifact_name
            ):
                raise AllCasesPipelineError("internal event order differs")
            if prior_phase is None:
                if event.phase != _SEARCH_PHASE_ORDER[0]:
                    raise AllCasesPipelineError("internal subledger skipped its first phase")
            elif event.phase != prior_phase and (
                phase_position[event.phase] != phase_position[prior_phase] + 1
                or next_index[prior_phase] != self.expected_phase_counts[prior_phase]
            ):
                raise AllCasesPipelineError("internal phase advanced before its frozen chunk count")
            self._artifact_payload(event)
            output.append(event)
            predecessor = event.sha256
            prior_coordinate = coordinate
            prior_phase = event.phase
            next_index[event.phase] += 1
        referenced = {event.artifact_relative_path for event in output}
        observed = set()
        for path in self.artifacts.iterdir():
            metadata = path.stat(follow_symlinks=False)
            if (
                path.is_symlink()
                or not path.is_file()
                or metadata.st_nlink != 1
                or metadata.st_mode & _WRITE_BITS
            ):
                raise AllCasesPipelineError("internal artifact directory contains unsafe bytes")
            raw = path.read_bytes()
            digest = hashlib.sha256(raw).hexdigest()
            if not path.name.endswith(f"-{digest}.json"):
                raise AllCasesPipelineError("internal artifact filename/content hash differs")
            observed.add(path.name)
        orphans = observed - referenced
        if not referenced.issubset(observed) or len(orphans) > 1:
            raise AllCasesPipelineError("internal artifact leaf set differs from subledger")
        if orphans:
            next_phase = next(
                (
                    phase
                    for phase in _SEARCH_PHASE_ORDER
                    if next_index[phase] < self.expected_phase_counts[phase]
                ),
                None,
            )
            orphan_name = next(iter(orphans))
            if next_phase is None:
                raise AllCasesPipelineError("complete Search subledger has an orphan")
            orphan_path = self.artifacts / orphan_name
            orphan_raw = orphan_path.read_bytes()
            orphan_digest = hashlib.sha256(orphan_raw).hexdigest()
            expected_prefix = f"{next_phase.lower()}-{next_index[next_phase]:06d}-"
            try:
                orphan_document = json.loads(orphan_raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise AllCasesPipelineError("next internal orphan is invalid JSON") from error
            if (
                orphan_name != f"{expected_prefix}{orphan_digest}.json"
                or not isinstance(orphan_document, dict)
                or _canonical_json_bytes(orphan_document) != orphan_raw
                or set(orphan_document)
                != {
                    "artifact_schema",
                    "chunk_index",
                    "config_semantic_sha256",
                    "payload",
                    "phase",
                }
                or orphan_document.get("artifact_schema") != _SUBLEDGER_ARTIFACT_SCHEMA
                or orphan_document.get("config_semantic_sha256") != self.config.semantic_sha256
                or orphan_document.get("phase") != next_phase
                or type(orphan_document.get("chunk_index")) is not int
                or orphan_document.get("chunk_index") != next_index[next_phase]
                or not isinstance(orphan_document.get("payload"), dict)
            ):
                raise AllCasesPipelineError(
                    "internal orphan is not the exact next phase coordinate"
                )
            _validate_raw_chunk_payload(orphan_document["payload"], phase=next_phase)
        self.resources.check("SEARCH_SUBLEDGER_VERIFY_END")
        return tuple(output)

    def discard_validated_artifact_orphan(self) -> None:
        """Discard only the one exact event-before-publication artifact at terminal failure."""

        events = self.verify()
        referenced = {event.artifact_relative_path for event in events}
        observed = {path.name for path in self.artifacts.iterdir()}
        orphans = observed - referenced
        if not orphans:
            return
        if len(orphans) != 1:
            raise AllCasesPipelineError("terminal Search artifact orphan set differs")
        # verify() above proved this is the canonical artifact for the exact next coordinate.
        path = self.artifacts / next(iter(orphans))
        metadata = path.stat(follow_symlinks=False)
        raw = path.read_bytes()
        if (
            path.is_symlink()
            or not path.is_file()
            or metadata.st_nlink != 1
            or metadata.st_mode & _WRITE_BITS
            or not path.name.endswith(f"-{hashlib.sha256(raw).hexdigest()}.json")
        ):
            raise AllCasesPipelineError("terminal Search artifact orphan changed")
        path.unlink()
        descriptor = os.open(self.artifacts, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        self.verify_exact_artifact_closure()

    def verify_exact_artifact_closure(self) -> tuple[_SearchEvent, ...]:
        """Verify a terminal/internal release with no resumable artifact orphan."""

        events = self.verify()
        referenced = {event.artifact_relative_path for event in events}
        observed = {path.name for path in self.artifacts.iterdir()}
        if referenced != observed:
            raise AllCasesPipelineError("terminal Search artifact leaf closure differs")
        return events

    def _append(self, phase: str, chunk_index: int, payload: Mapping[str, object]) -> _SearchEvent:
        _validate_raw_chunk_payload(payload, phase=phase)
        events = self.verify()
        phase_position = _SEARCH_PHASE_ORDER.index(phase)
        if events:
            prior_position = _SEARCH_PHASE_ORDER.index(events[-1].phase)
            if phase_position < prior_position:
                raise AllCasesPipelineError("internal phase cannot move backward")
            if phase_position > prior_position and (
                phase_position != prior_position + 1
                or sum(event.phase == events[-1].phase for event in events)
                != self.expected_phase_counts[events[-1].phase]
            ):
                raise AllCasesPipelineError(
                    "internal phase cannot advance before its frozen chunk count"
                )
        elif phase != _SEARCH_PHASE_ORDER[0]:
            raise AllCasesPipelineError("internal append skipped its first phase")
        expected_index = sum(event.phase == phase for event in events)
        if chunk_index != expected_index or chunk_index >= self.expected_phase_counts[phase]:
            raise AllCasesPipelineError("internal append is not the lowest incomplete chunk")
        envelope = {
            "artifact_schema": _SUBLEDGER_ARTIFACT_SCHEMA,
            "chunk_index": chunk_index,
            "config_semantic_sha256": self.config.semantic_sha256,
            "payload": dict(payload),
            "phase": phase,
        }
        raw = _canonical_json_bytes(envelope)
        digest = hashlib.sha256(raw).hexdigest()
        relative = f"{phase.lower()}-{chunk_index:06d}-{digest}.json"
        referenced = {event.artifact_relative_path for event in events}
        orphans = {path.name for path in self.artifacts.iterdir()} - referenced
        if orphans and orphans != {relative}:
            raise AllCasesPipelineError("orphan artifact differs from lowest incomplete chunk")
        if not (self.artifacts / relative).exists():
            self.resources.ensure_additional_bytes(
                len(raw), f"SEARCH_SUBLEDGER_{phase}_{chunk_index}_ARTIFACT"
            )
        _publish_mode_0444(self.artifacts, relative, raw, staging=self.staging)
        event = _SearchEvent(
            len(events) + 1,
            events[-1].sha256 if events else None,
            phase,
            chunk_index,
            digest,
            relative,
            datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z"),
        )
        event_raw = _canonical_json_bytes(event.as_dict())
        self.resources.ensure_additional_bytes(
            len(event_raw), f"SEARCH_SUBLEDGER_{phase}_{chunk_index}_EVENT"
        )
        _publish_mode_0444(
            self.events,
            f"event-{event.sequence:08d}.json",
            event_raw,
            staging=self.staging,
        )
        verified = self.verify()
        if not verified or verified[-1].sha256 != event.sha256:
            raise AllCasesPipelineError("internal append did not replay")
        return event

    def ensure_phase(
        self,
        phase: str,
        count: int,
        builder: Callable[[int], Mapping[str, object]],
        *,
        verify_only: bool,
        resume_consumer: Callable[[int, Mapping[str, object]], None] | None = None,
    ) -> tuple[dict[str, object], ...]:
        if phase not in _SEARCH_PHASE_ORDER or count < 1:
            raise AllCasesPipelineError("internal phase plan differs")
        events = self.verify()
        existing = tuple(event for event in events if event.phase == phase)
        if len(existing) > count:
            raise AllCasesPipelineError("internal phase exceeds frozen chunk count")
        output: list[dict[str, object]] = []
        for index in range(count):
            self.resources.check(f"{phase}_{index:06d}_START")
            if index < len(existing):
                stored = self._artifact_payload(existing[index])
                # A prior process could have published canonical but forged raw
                # evidence.  Re-run the source builder for every pre-existing
                # coordinate before any missing continuation is allowed.  The
                # builder reconstructs the phase sinks; consuming the stored row
                # again would duplicate Stage-B/ML state.
                rebuilt = dict(builder(index))
                if _canonical_json_bytes(stored) != _canonical_json_bytes(rebuilt):
                    raise AllCasesPipelineError("internal fresh chunk recomputation differs")
                output.append(stored)
            else:
                if verify_only:
                    if self.allow_incomplete_verify:
                        raise _VerifiedPrefixIncomplete(phase, index)
                    raise AllCasesPipelineError("fresh verification found an incomplete phase")
                rebuilt = dict(builder(index))
                event = self._append(phase, index, rebuilt)
                output.append(self._artifact_payload(event))
            self.resources.check(f"{phase}_{index:06d}_END")
        return tuple(output)

    def leaf_closure(self) -> tuple[dict[str, object], ...]:
        return tuple(
            {
                "artifact_sha256": event.artifact_sha256,
                "chunk_index": event.chunk_index,
                "phase": event.phase,
            }
            for event in self.verify()
        )

    def assert_complete(self, expected_counts: Mapping[str, int]) -> None:
        if tuple(expected_counts) != _SEARCH_PHASE_ORDER:
            raise AllCasesPipelineError("final Search phase plan order differs")
        events = self.verify()
        actual = {phase: 0 for phase in _SEARCH_PHASE_ORDER}
        for event in events:
            actual[event.phase] += 1
        if actual != dict(expected_counts):
            raise AllCasesPipelineError("final Search phase counts differ")
        referenced = {event.artifact_relative_path for event in events}
        observed = {path.name for path in self.artifacts.iterdir()}
        if referenced != observed:
            raise AllCasesPipelineError("final Search artifact leaf closure is not exact")

    @property
    def head_sha256(self) -> str:
        events = self.verify()
        if not events:
            raise AllCasesPipelineError("Search subledger is empty")
        return events[-1].sha256


@dataclass(frozen=True, slots=True)
class _StagePlan:
    stage_key: str
    decision_dates: tuple[date, ...]
    partitions: tuple[BarDatasetPartition, ...]
    reporting_group_by_date: Mapping[date, str]
    outer_validation_by_date: Mapping[date, str]
    search_block_by_date: Mapping[date, str]


@dataclass(frozen=True, slots=True)
class _Plans:
    search: _StagePlan
    walk_forward: tuple[_StagePlan, ...]
    holdout: _StagePlan


@dataclass(frozen=True, slots=True)
class _SearchFeatureState:
    plan: _StagePlan
    bars_by_timeframe: Mapping[int, tuple[object, ...]]
    symbolic_stage: object
    structural_lattice: object
    direct_opportunity_lattice: object
    allowed_tail_end_ns: int


@dataclass(frozen=True, slots=True)
class _StageBFeaturePlan:
    recipes: tuple[object, ...]
    chunks: tuple[object, ...]
    policies_by_id: Mapping[str, object]
    family_by_policy_id: Mapping[str, str]
    controls_by_policy_id: Mapping[str, object]
    masks_by_world: Mapping[str, Mapping[str, object]]
    schedules_by_world: Mapping[str, Mapping[str, object]]
    plan_document: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class _StageBSearchEvidence:
    evaluations_by_world: Mapping[str, Mapping[str, object]]
    coverage_by_world: Mapping[str, Mapping[str, object]]
    ineligibility_by_world: Mapping[str, Mapping[str, str]]
    representative_real_evaluations: tuple[object, ...]
    gate_results: tuple[object, ...]
    top24_by_world_and_scope: Mapping[str, Mapping[str, object]]
    symbolic_selection: object
    top24_document: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class _DirectFeatureBundle:
    """Candidate-independent causal rows and exact scheduled entry prices."""

    feature_rows: object
    scheduled_entry_ticks: tuple[int, ...]
    opportunity_lattice: object

    @property
    def opportunity_lattice_sha256(self) -> str:
        return str(self.opportunity_lattice.artifact_sha256)


@dataclass(frozen=True, slots=True)
class _DirectOutcomeBundle:
    """Full-horizon exact 1s terminal responses aligned to causal rows."""

    fill_ns: object
    label_exit_ns: object
    entry_ticks: object
    terminal_ticks: object
    outcome_contracts: tuple[str, ...]
    outcome_span_ids: object
    segment_ids: object
    valid_label_paths: object
    outcome_lineage_sha256: str
    row_ids: tuple[str, ...]
    source_dates: tuple[date, ...]
    decision_ns: tuple[int, ...]
    decision_timeframe_seconds: int
    horizon_seconds: int
    entry_schedule_sha256: str
    opportunity_lattice_sha256: str


@dataclass(frozen=True, slots=True)
class _DirectSearchEvidence:
    candidate_rows: tuple[Mapping[str, object], ...]
    selection_rows: tuple[Mapping[str, object], ...]
    model_artifacts_by_candidate: Mapping[str, Mapping[str, object]]
    fit_cache_aggregate: Mapping[str, object]
    fit_count: int


@dataclass(frozen=True, slots=True)
class _MetaSearchEvidence:
    candidate_rows: tuple[Mapping[str, object], ...]
    selection_rows: tuple[Mapping[str, object], ...]
    model_artifacts_by_candidate: Mapping[str, Mapping[str, object]]
    plan_document: Mapping[str, object]
    fit_cache_aggregate: Mapping[str, object]
    fit_count: int


@dataclass(frozen=True, slots=True)
class _MetaFeaturePlan:
    certificates_by_world_and_scope: Mapping[str, Mapping[str, object]]
    plan_document: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class _MetaBaseBundle:
    feature_rows: object
    base_entry_ns: object
    fully_loaded_net_ticks: object
    base_directions: object
    atr_ticks: object
    label_exit_ns: object
    outcome_contracts: tuple[str, ...]
    outcome_span_ids: object
    segment_ids: object
    valid_label_paths: object
    outcome_lineage_sha256: str
    opportunity_lattice_sha256: str
    expert_artifact_commitment_sha256: str
    entry_order_batch_sha256: str
    expert_artifacts: tuple[object, ...]
    entry_order_batch: object
    strategy_recipe: object


@dataclass(frozen=True, slots=True)
class _FrozenSearchCandidate:
    candidate_id: str
    candidate_kind: str
    family_key: str
    catalog_selection_rank: int
    document: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class _StageWorldExecution:
    """One outcome-free symbolic execution request for a frozen null world."""

    recipe: object
    mask: object
    rule_schedule: object | None


@dataclass(frozen=True, slots=True)
class _StageCandidateMask:
    """Private typed mask state; only its canonical commitment is published."""

    candidate: _FrozenSearchCandidate
    stage_key: str
    mask_document: Mapping[str, object]
    direct_controls: object | None
    direct_response_coordinate: tuple[int, int] | None
    symbolic_worlds: Mapping[str, _StageWorldExecution]
    sample_eligible: bool

    @property
    def mask_sha256(self) -> str:
        return _sha256(self.mask_document)


@dataclass(frozen=True, slots=True)
class _FilledTradeEvidence:
    """Canonical candidate/world/lineage-bound filled trade used for durable OOS replay."""

    candidate_id: str
    stage_key: str
    world: str
    action_id: str
    decision_date: str
    decision_ns: int
    entry_ns: int
    exit_ns: int
    contract: str
    outcome_span_id: int
    segment_id: int
    direction: str
    net_ticks: int
    trade_sha256: str

    def __post_init__(self) -> None:
        _require_sha(self.candidate_id, label="filled-trade candidate ID")
        _require_sha(self.action_id, label="filled-trade action ID")
        _require_sha(self.trade_sha256, label="filled-trade SHA")
        try:
            canonical_day = date.fromisoformat(self.decision_date).isoformat()
        except ValueError as error:
            raise AllCasesPipelineError("filled-trade decision date differs") from error
        if (
            not self.stage_key
            or self.world not in {"REAL", "CIRCULAR_TARGET", "MATCHED_TARGET"}
            or canonical_day != self.decision_date
            or not self.contract
            or self.direction not in {"LONG", "SHORT"}
            or any(
                isinstance(value, bool) or not isinstance(value, int)
                for value in (
                    self.decision_ns,
                    self.entry_ns,
                    self.exit_ns,
                    self.outcome_span_id,
                    self.segment_id,
                    self.net_ticks,
                )
            )
            or self.decision_ns < 0
            or self.entry_ns < self.decision_ns
            or self.exit_ns < self.entry_ns
            or self.outcome_span_id < 1
            or self.segment_id < 1
            or self.segment_id > (1 << 64) - 1
            or _sha256(self.definition_dict()) != self.trade_sha256
        ):
            raise AllCasesPipelineError("filled-trade evidence differs")

    def definition_dict(self) -> dict[str, object]:
        return {
            "action_id": self.action_id,
            "candidate_id": self.candidate_id,
            "contract": self.contract,
            "decision_date": self.decision_date,
            "decision_ns": self.decision_ns,
            "direction": self.direction,
            "entry_ns": self.entry_ns,
            "exit_ns": self.exit_ns,
            "net_ticks": self.net_ticks,
            "outcome_span_id": self.outcome_span_id,
            "segment_id": self.segment_id,
            "stage_key": self.stage_key,
            "world": self.world,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.definition_dict(), "trade_sha256": self.trade_sha256}

    @property
    def sort_key(self) -> tuple[object, ...]:
        return self.entry_ns, self.exit_ns, self.action_id, self.trade_sha256


def _filled_trade_evidence(
    *,
    candidate_id: str,
    stage_key: str,
    world: str,
    source_identity: object,
    decision_date: str,
    decision_ns: int,
    entry_ns: int,
    exit_ns: int,
    contract: str,
    outcome_span_id: int,
    segment_id: int,
    direction: str,
    net_ticks: int,
) -> _FilledTradeEvidence:
    action_id = _sha256(
        {
            "candidate_id": candidate_id,
            "source_identity": source_identity,
            "stage_key": stage_key,
            "world": world,
        }
    )
    definition = {
        "action_id": action_id,
        "candidate_id": candidate_id,
        "contract": contract,
        "decision_date": decision_date,
        "decision_ns": decision_ns,
        "direction": direction,
        "entry_ns": entry_ns,
        "exit_ns": exit_ns,
        "net_ticks": net_ticks,
        "outcome_span_id": outcome_span_id,
        "segment_id": segment_id,
        "stage_key": stage_key,
        "world": world,
    }
    return _FilledTradeEvidence(
        candidate_id,
        stage_key,
        world,
        action_id,
        decision_date,
        decision_ns,
        entry_ns,
        exit_ns,
        contract,
        outcome_span_id,
        segment_id,
        direction,
        net_ticks,
        _sha256(definition),
    )


@dataclass(frozen=True, slots=True)
class _WorldPartitionEvidence:
    """Exact, chronologically ordered post-freeze trade evidence for one world."""

    daily_net_ticks: tuple[tuple[str, int], ...]
    filled_trades: tuple[_FilledTradeEvidence, ...]


@dataclass(frozen=True, slots=True)
class _CandidatePartitionEvidence:
    candidate: _FrozenSearchCandidate
    stage_key: str
    sample_eligible: bool
    worlds: Mapping[str, _WorldPartitionEvidence | None]


def _plans(project_root: Path) -> _Plans:
    dataset, split = _load_validated_dataset_contract(project_root)
    eligible = dataset.eligible_active_dates

    def partitions(start: int, end: int) -> tuple[BarDatasetPartition, ...]:
        return dataset.partitions[start - 1 : end]

    search_decisions = eligible[
        split.discovery.start_active_ordinal - 1 : split.discovery.end_active_ordinal - 20
    ]
    reporting: dict[date, str] = {}
    for number, block in enumerate(split.discovery_reporting_blocks, start=1):
        for source_date in eligible[block.start_active_ordinal - 1 : block.end_active_ordinal]:
            reporting[source_date] = f"R{number}"
    outer: dict[date, str] = {}
    all_search_blocks: dict[date, str] = {}
    cursor = 0
    block_lengths = (59, 59, 59, 59, 59, 58, 58, 58)
    for number, length in enumerate(block_lengths, start=1):
        for source_date in search_decisions[cursor : cursor + length]:
            all_search_blocks[source_date] = f"B{number}"
            if number >= 3:
                outer[source_date] = f"B{number}"
        cursor += length
    search = _StagePlan(
        "SEARCH",
        tuple(search_decisions),
        partitions(split.discovery.start_active_ordinal, split.discovery.end_active_ordinal),
        reporting,
        outer,
        all_search_blocks,
    )
    walk = []
    for fold in split.walk_forward_folds:
        decisions = eligible[fold.start_active_ordinal - 1 : fold.end_active_ordinal - 20]
        mapping = {source_date: f"WF{fold.fold_number}" for source_date in decisions}
        walk.append(
            _StagePlan(
                f"WF{fold.fold_number}",
                tuple(decisions),
                partitions(fold.start_active_ordinal, fold.end_active_ordinal),
                mapping,
                mapping,
                mapping,
            )
        )
    holdout_decisions = eligible[
        split.holdout.start_active_ordinal - 1 : split.holdout.end_active_ordinal
    ]
    halves = {
        source_date: "HOLDOUT_HALF_1" if index < 60 else "HOLDOUT_HALF_2"
        for index, source_date in enumerate(holdout_decisions)
    }
    holdout = _StagePlan(
        "HOLDOUT",
        tuple(holdout_decisions),
        partitions(split.holdout.start_active_ordinal, split.outcome_tail.end_active_ordinal),
        halves,
        halves,
        halves,
    )
    return _Plans(search, tuple(walk), holdout)


def _load_stage_bars(
    project_root: Path,
    partitions: Sequence[BarDatasetPartition],
    timeframe_seconds: int,
) -> tuple[object, ...]:
    from scripts.ai_pattern_holdout_engine import BarWithOutcomeSpan
    from systematic_fx.features.bars import load_trade_bar_artifact

    if timeframe_seconds not in {1, 300, 1_800, 3_600}:
        raise AllCasesPipelineError("adapter requested an uncommitted timeframe")
    data_root = project_root / "data"
    output = []
    for partition in partitions:
        matches = tuple(
            artifact
            for artifact in partition.artifacts
            if artifact.timeframe_seconds == timeframe_seconds
        )
        if len(matches) != 1:
            raise AllCasesPipelineError("partition lacks one exact timeframe artifact")
        bars = load_trade_bar_artifact(
            data_root,
            matches[0],
            expected_plan_sha256=partition.plan_sha256,
            expected_source_sha256=partition.source_sha256,
            expected_source_date=partition.source_date,
        )
        if any(
            bar.source_date != partition.source_date
            or bar.contract != partition.contract
            or bar.timeframe_seconds != timeframe_seconds
            for bar in bars
        ):
            raise AllCasesPipelineError("loaded bars differ from manifest lineage")
        output.extend(BarWithOutcomeSpan(bar, partition.outcome_span_id) for bar in bars)
    return tuple(output)


def _feature_bars(project_root: Path, plan: _StagePlan) -> dict[int, tuple[object, ...]]:
    return {
        timeframe: _load_stage_bars(project_root, plan.partitions, timeframe)
        for timeframe in (300, 1_800, 3_600)
    }


def _search_feature_state(project_root: Path) -> _SearchFeatureState:
    """Rebuild the exact feature-only Search state and 7h anchor lattice."""

    return _stage_feature_state(project_root, _plans(project_root).search)


def _stage_feature_state(project_root: Path, plan: _StagePlan) -> _SearchFeatureState:
    """Rebuild one stage's exact feature-only symbolic/direct opportunity state."""

    from . import symbolic

    bars_by_timeframe = _feature_bars(project_root, plan)
    allowed_tail_end_ns = max(item.bar.end_ns for item in bars_by_timeframe[300])
    stage = symbolic.build_symbolic_stage(bars_by_timeframe, plan.decision_dates)
    lattice = symbolic.build_structural_eligibility_lattice(
        bars_by_timeframe[300],
        decision_dates=plan.decision_dates,
        allowed_tail_end_ns=allowed_tail_end_ns,
    )
    direct_lattice = symbolic.build_direct_opportunity_lattice(bars_by_timeframe[300], lattice)
    return _SearchFeatureState(
        plan,
        bars_by_timeframe,
        stage,
        lattice,
        direct_lattice,
        allowed_tail_end_ns,
    )


def _ml_bar_series(state: _SearchFeatureState) -> Mapping[int, object]:
    """Translate verified integer-tick bars into ML's causal lineage container."""

    import numpy as np

    from . import ml

    stage_dates = tuple(sorted({item.bar.source_date for item in state.bars_by_timeframe[300]}))
    rank_by_date = {value: index for index, value in enumerate(stage_dates)}
    output: dict[int, object] = {}
    for timeframe in ml.TF_ORDER:
        wrapped = tuple(
            sorted(
                state.bars_by_timeframe[timeframe],
                key=lambda item: (
                    item.bar.end_ns,
                    item.bar.contract,
                    item.outcome_span_id,
                    item.bar.segment_id,
                ),
            )
        )
        if len(
            {
                (
                    item.bar.end_ns,
                    item.bar.contract,
                    item.outcome_span_id,
                    item.bar.segment_id,
                )
                for item in wrapped
            }
        ) != len(wrapped):
            raise AllCasesPipelineError("ML causal bar lineage is duplicated")
        have_flow = all(
            item.bar.buy_volume is not None and item.bar.sell_volume is not None for item in wrapped
        )
        no_flow = all(
            item.bar.buy_volume is None and item.bar.sell_volume is None for item in wrapped
        )
        if not (have_flow or no_flow):
            raise AllCasesPipelineError("ML causal flow columns are only partially available")
        output[timeframe] = ml.CausalBarSeries(
            timeframe_seconds=timeframe,
            bar_end_ns=np.asarray([item.bar.end_ns for item in wrapped], dtype=np.int64),
            open=np.asarray([item.bar.open_ticks for item in wrapped], dtype=np.float64),
            high=np.asarray([item.bar.high_ticks for item in wrapped], dtype=np.float64),
            low=np.asarray([item.bar.low_ticks for item in wrapped], dtype=np.float64),
            close=np.asarray([item.bar.close_ticks for item in wrapped], dtype=np.float64),
            volume=np.asarray([item.bar.volume for item in wrapped], dtype=np.float64),
            trade_count=np.asarray([item.bar.trade_count for item in wrapped], dtype=np.float64),
            source_dates=tuple(item.bar.source_date for item in wrapped),
            contracts=tuple(item.bar.contract for item in wrapped),
            outcome_span_ids=np.asarray([item.outcome_span_id for item in wrapped], dtype=np.int64),
            segment_ids=np.asarray([item.bar.segment_id for item in wrapped], dtype=np.uint64),
            stage_date_ranks=np.asarray(
                [rank_by_date[item.bar.source_date] for item in wrapped], dtype=np.int64
            ),
            stage_key=state.plan.stage_key,
            buy_volume=(
                np.asarray([item.bar.buy_volume for item in wrapped], dtype=np.float64)
                if have_flow
                else None
            ),
            sell_volume=(
                np.asarray([item.bar.sell_volume for item in wrapped], dtype=np.float64)
                if have_flow
                else None
            ),
        )
    return output


def _direct_required_coordinates(
    required_coordinates: set[tuple[int, int]] | frozenset[tuple[int, int]] | None,
) -> frozenset[tuple[int, int]]:
    """Normalize the fixed Direct 3x3 response lattice without numeric coercion."""

    from . import ml

    available = frozenset(
        (timeframe, horizon) for timeframe in ml.TF_ORDER for horizon in (3_600, 10_800, 21_600)
    )
    if required_coordinates is None:
        return available
    if not isinstance(required_coordinates, (set, frozenset)) or any(
        not isinstance(coordinate, tuple)
        or len(coordinate) != 2
        or any(type(value) is not int for value in coordinate)
        for coordinate in required_coordinates
    ):
        raise AllCasesPipelineError("direct response coordinates differ")
    normalized = frozenset(required_coordinates)
    if not normalized <= available:
        raise AllCasesPipelineError("direct response coordinates escape the frozen lattice")
    return normalized


def _direct_feature_bundles(
    state: _SearchFeatureState,
    *,
    required_coordinates: set[tuple[int, int]] | frozenset[tuple[int, int]] | None = None,
) -> Mapping[int, _DirectFeatureBundle]:
    """Freeze outcome-free feature tables only for requested native timeframes."""

    import numpy as np

    from . import ml

    coordinates = _direct_required_coordinates(required_coordinates)
    required_timeframes = frozenset(timeframe for timeframe, _horizon in coordinates)
    if not required_timeframes:
        return {}
    series = _ml_bar_series(state)
    stage_rank = {value: index for index, value in enumerate(state.plan.decision_dates)}
    output: dict[int, _DirectFeatureBundle] = {}
    for timeframe in ml.TF_ORDER:
        if timeframe not in required_timeframes:
            continue
        native_keys = {
            (
                item.bar.contract,
                item.outcome_span_id,
                item.bar.segment_id,
                item.bar.end_ns,
            )
            for item in state.bars_by_timeframe[timeframe]
            if item.bar.source_date in stage_rank
        }
        opportunities = tuple(
            sorted(
                (
                    item
                    for item in state.direct_opportunity_lattice.opportunities
                    if item.structural_anchor_key in native_keys
                    and item.decision_source_date in stage_rank
                ),
                key=lambda item: (
                    item.decision_ns,
                    item.contract,
                    item.outcome_span_id,
                    item.segment_id,
                ),
            )
        )
        if len(opportunities) < 2:
            raise AllCasesPipelineError("direct ML native opportunity family is empty")
        schedule_rows = [
            {
                "contract": item.contract,
                "decision_ns": item.decision_ns,
                "decision_source_date": item.decision_source_date.isoformat(),
                "outcome_span_id": item.outcome_span_id,
                "scheduled_entry_ns": item.scheduled_entry_ns,
                "scheduled_entry_ticks": item.scheduled_entry_ticks,
                "segment_id": item.segment_id,
            }
            for item in opportunities
        ]
        entry_schedule_sha256 = _sha256(
            {
                "rows": schedule_rows,
                "schema": "systematic_fx.ai_all_cases_direct_entry_schedule.v1",
                "stage_key": state.plan.stage_key,
                "timeframe_seconds": timeframe,
            }
        )
        row_ids = tuple(
            _sha256(
                {
                    **row,
                    "schema": "systematic_fx.ai_all_cases_direct_opportunity_row.v1",
                    "timeframe_seconds": timeframe,
                }
            )
            for row in schedule_rows
        )
        anchors = ml.CausalAnchorRows(
            row_ids=row_ids,
            decision_ns=np.asarray([item.decision_ns for item in opportunities], dtype=np.int64),
            entry_ns=np.asarray(
                [item.scheduled_entry_ns for item in opportunities], dtype=np.int64
            ),
            source_dates=tuple(item.decision_source_date for item in opportunities),
            contracts=tuple(item.contract for item in opportunities),
            outcome_span_ids=np.asarray(
                [item.outcome_span_id for item in opportunities], dtype=np.int64
            ),
            segment_ids=np.asarray([item.segment_id for item in opportunities], dtype=np.uint64),
            stage_date_ranks=np.asarray(
                [stage_rank[item.decision_source_date] for item in opportunities],
                dtype=np.int64,
            ),
            stage_key=state.plan.stage_key,
            decision_timeframe_seconds=timeframe,
            entry_schedule_sha256=entry_schedule_sha256,
        )
        opportunity_lattice = ml.build_structural_opportunity_lattice(
            anchors,
            series[300],
        )
        if not all(opportunity_lattice.eligible):
            raise AllCasesPipelineError("symbolic and ML structural opportunity lattices disagree")
        feature_rows = ml.build_causal_feature_rows(
            anchors=anchors,
            bars_by_timeframe=series,
            feature_set_id="FULL_MTF_213",
        )
        retained = tuple(int(value) for value in feature_rows.retained_input_indexes)
        scheduled_ticks = tuple(opportunities[index].scheduled_entry_ticks for index in retained)
        if len(scheduled_ticks) != feature_rows.row_count:
            raise AllCasesPipelineError("direct ML retained entry schedule differs")
        output[timeframe] = _DirectFeatureBundle(
            feature_rows,
            scheduled_ticks,
            opportunity_lattice,
        )
    return output


def _direct_feature_universe_commitment(
    bundles: Mapping[int, _DirectFeatureBundle],
) -> tuple[str, tuple[dict[str, object], ...]]:
    """Hash exact feature-only Direct rows without JSON-expanding dense matrices."""

    import numpy as np

    digest = hashlib.sha256()
    summaries = []
    for timeframe in sorted(bundles):
        bundle = bundles[timeframe]
        rows = bundle.feature_rows
        digest.update(str(timeframe).encode("ascii") + b"\0")
        digest.update(rows.entry_schedule_sha256.encode("ascii") + b"\0")
        digest.update(bundle.opportunity_lattice_sha256.encode("ascii") + b"\0")
        digest.update(_canonical_json_bytes(list(rows.feature_names)))
        for row_id in rows.row_ids:
            digest.update(row_id.encode("ascii") + b"\0")
        for values in (
            rows.decision_ns,
            rows.entry_ns,
            rows.outcome_span_ids,
            rows.segment_ids,
            rows.stage_date_ranks,
            rows.retained_input_indexes,
            rows.aligned_bar_indexes,
            rows.values,
            rows.atr_ticks_by_timeframe,
            np.asarray(bundle.scheduled_entry_ticks, dtype=np.int64),
        ):
            array = np.ascontiguousarray(values)
            digest.update(array.dtype.str.encode("ascii") + b"\0")
            digest.update(_canonical_json_bytes(list(array.shape)))
            digest.update(array.tobytes(order="C"))
        summaries.append(
            {
                "entry_schedule_sha256": rows.entry_schedule_sha256,
                "feature_name_count": len(rows.feature_names),
                "opportunity_lattice_sha256": bundle.opportunity_lattice_sha256,
                "row_count": rows.row_count,
                "timeframe_seconds": timeframe,
            }
        )
    summary_rows = tuple(summaries)
    digest.update(_canonical_json_bytes(summary_rows))
    return digest.hexdigest(), summary_rows


def _direct_outcome_bundles(
    project_root: Path,
    state: _SearchFeatureState,
    feature_bundles: Mapping[int, _DirectFeatureBundle],
    *,
    required_coordinates: set[tuple[int, int]] | frozenset[tuple[int, int]] | None = None,
) -> Mapping[tuple[int, int], _DirectOutcomeBundle]:
    """Resolve requested Direct coordinates in one outcome-span streaming pass."""

    import numpy as np

    coordinates = _direct_required_coordinates(required_coordinates)
    if not coordinates:
        return {}
    required_timeframes = frozenset(timeframe for timeframe, _horizon in coordinates)
    if not required_timeframes <= set(feature_bundles):
        raise AllCasesPipelineError("direct response features omit a requested timeframe")
    horizons_by_timeframe = {
        timeframe: tuple(
            horizon for horizon in (3_600, 10_800, 21_600) if (timeframe, horizon) in coordinates
        )
        for timeframe in sorted(required_timeframes)
    }
    indexes_by_timeframe_lineage: dict[int, dict[tuple[str, int, int], list[int]]] = {}
    entry_ticks_by_timeframe: dict[int, object] = {}
    valid_by_coordinate: dict[tuple[int, int], object] = {}
    terminal_by_coordinate: dict[tuple[int, int], object] = {}
    seen_by_timeframe: dict[int, set[tuple[str, int, int]]] = {}
    for timeframe in sorted(required_timeframes):
        bundle = feature_bundles[timeframe]
        rows = bundle.feature_rows
        by_lineage: dict[tuple[str, int, int], list[int]] = defaultdict(list)
        for index, lineage in enumerate(
            zip(rows.contracts, rows.outcome_span_ids, rows.segment_ids, strict=True)
        ):
            by_lineage[str(lineage[0]), int(lineage[1]), int(lineage[2])].append(index)
        indexes_by_timeframe_lineage[timeframe] = by_lineage
        entry_ticks_by_timeframe[timeframe] = np.asarray(
            bundle.scheduled_entry_ticks, dtype=np.int64
        )
        seen_by_timeframe[timeframe] = set()
        for horizon in horizons_by_timeframe[timeframe]:
            valid_by_coordinate[timeframe, horizon] = np.zeros(rows.row_count, dtype=np.bool_)
            terminal_by_coordinate[timeframe, horizon] = np.zeros(rows.row_count, dtype=np.int64)

    for paths in _one_second_path_parts(project_root, state.plan):
        for path in paths:
            for timeframe in sorted(required_timeframes):
                bundle = feature_bundles[timeframe]
                indexes = indexes_by_timeframe_lineage[timeframe].get(path.lineage, ())
                if not indexes:
                    continue
                if path.lineage in seen_by_timeframe[timeframe]:
                    raise AllCasesPipelineError("direct outcome lineage was opened twice")
                seen_by_timeframe[timeframe].add(path.lineage)
                rows = bundle.feature_rows
                entry_ticks = entry_ticks_by_timeframe[timeframe]
                for index in indexes:
                    entry_ns = int(rows.entry_ns[index])
                    entry_index = bisect_left(path.starts, entry_ns)
                    if (
                        entry_index >= len(path.rows)
                        or path.rows[entry_index].bar.start_ns != entry_ns
                        or path.rows[entry_index].bar.open_ticks != int(entry_ticks[index])
                    ):
                        raise AllCasesPipelineError(
                            "scheduled direct first-second/open proof differs from 1s"
                        )
                    for horizon in horizons_by_timeframe[timeframe]:
                        exit_ns = entry_ns + horizon * 1_000_000_000
                        if not path.structurally_covers(entry_ns, exit_ns):
                            raise AllCasesPipelineError(
                                "frozen direct opportunity lacks full structural path coverage"
                            )
                        terminal_index = bisect_right(path.ends, exit_ns) - 1
                        if (
                            terminal_index < entry_index
                            or not 0 <= exit_ns - path.ends[terminal_index] < 300 * 1_000_000_000
                        ):
                            raise AllCasesPipelineError(
                                "frozen direct terminal is missing or stale"
                            )
                        terminal_by_coordinate[timeframe, horizon][index] = path.rows[
                            terminal_index
                        ].bar.close_ticks
                        valid_by_coordinate[timeframe, horizon][index] = True

    output: dict[tuple[int, int], _DirectOutcomeBundle] = {}
    for timeframe in sorted(required_timeframes):
        bundle = feature_bundles[timeframe]
        rows = bundle.feature_rows
        if set(indexes_by_timeframe_lineage[timeframe]) != seen_by_timeframe[timeframe]:
            raise AllCasesPipelineError("direct outcome stream omitted a frozen lineage")
        fill_ns = np.asarray(rows.entry_ns, dtype=np.int64)
        for horizon in horizons_by_timeframe[timeframe]:
            valid = valid_by_coordinate[timeframe, horizon]
            if not bool(np.all(valid)):
                raise AllCasesPipelineError("direct outcome stream omitted a frozen row")
            label_exit_ns = fill_ns + horizon * 1_000_000_000
            lineage_document = {
                "entry_schedule_sha256": rows.entry_schedule_sha256,
                "horizon_seconds": horizon,
                "opportunity_lattice_sha256": bundle.opportunity_lattice_sha256,
                "rows": [
                    {
                        "contract": rows.contracts[index],
                        "entry_ns": int(fill_ns[index]),
                        "label_exit_ns": int(label_exit_ns[index]),
                        "outcome_span_id": int(rows.outcome_span_ids[index]),
                        "row_id": rows.row_ids[index],
                        "segment_id": int(rows.segment_ids[index]),
                    }
                    for index in range(rows.row_count)
                ],
                "schema": "systematic_fx.ai_all_cases_direct_outcome_lineage.v1",
            }
            output[timeframe, horizon] = _DirectOutcomeBundle(
                fill_ns,
                label_exit_ns,
                entry_ticks_by_timeframe[timeframe],
                terminal_by_coordinate[timeframe, horizon],
                rows.contracts,
                np.asarray(rows.outcome_span_ids, dtype=np.int64),
                np.asarray(rows.segment_ids, dtype=np.uint64),
                tuple(bool(value) for value in valid),
                _sha256(lineage_document),
                rows.row_ids,
                rows.source_dates,
                tuple(int(value) for value in rows.decision_ns),
                timeframe,
                horizon,
                rows.entry_schedule_sha256,
                bundle.opportunity_lattice_sha256,
            )
    return output


def _structural_reference_anchors(state: _SearchFeatureState) -> tuple[object, ...]:
    """Build both-direction surface keys for every frozen structural anchor."""

    from .symbolic import AnchorRecord

    bars = {
        (
            item.bar.contract,
            item.outcome_span_id,
            item.bar.segment_id,
            item.bar.end_ns,
        ): item
        for item in state.bars_by_timeframe[300]
    }
    keys = state.structural_lattice.eligible_anchor_keys
    if len(bars) != len(state.bars_by_timeframe[300]) or any(key not in bars for key in keys):
        raise AllCasesPipelineError("structural lattice cannot be reconstructed from 5m bars")
    anchors = []
    for key in keys:
        wrapped = bars[key]
        bar = wrapped.bar
        for direction in ("LONG", "SHORT"):
            anchors.append(
                AnchorRecord(
                    bar.source_date,
                    bar.contract,
                    wrapped.outcome_span_id,
                    bar.segment_id,
                    bar.end_ns,
                    direction,
                    bar.start_ns,
                    bar.end_ns,
                    bar.open_ticks,
                    bar.high_ticks,
                    bar.low_ticks,
                    bar.close_ticks,
                    1,
                    1,
                    (("structural_reference_surface", 1),),
                )
            )
    return tuple(anchors)


def _search_reference_surfaces(
    project_root: Path,
    state: _SearchFeatureState,
) -> tuple[object, ...]:
    """Stream 1s spans into complete five-horizon surfaces, failing on any censor."""

    from . import symbolic

    anchors = _structural_reference_anchors(state)
    anchors_by_lineage: dict[tuple[str, int, int], list[object]] = defaultdict(list)
    for anchor in anchors:
        anchors_by_lineage[
            anchor.contract,
            anchor.outcome_span_id,
            anchor.segment_id,
        ].append(anchor)
    parts = []
    seen_lineages: set[tuple[str, int, int]] = set()
    for paths in _one_second_path_parts(project_root, state.plan):
        lineages = {path.lineage for path in paths}
        if seen_lineages & lineages:
            raise AllCasesPipelineError("one Search path lineage was opened twice")
        seen_lineages.update(lineages)
        scoped = tuple(
            anchor for lineage in sorted(lineages) for anchor in anchors_by_lineage.get(lineage, ())
        )
        if not scoped:
            continue
        part = symbolic.build_reference_outcome_surfaces(scoped, paths)
        if any(surface.censored_anchor_keys for surface in part):
            raise AllCasesPipelineError("frozen structural anchor has a censored 1s path")
        parts.append(part)
    if set(anchors_by_lineage) - seen_lineages or not parts:
        raise AllCasesPipelineError("Search reference surfaces omit a structural lineage")
    surfaces = symbolic.merge_reference_outcome_surfaces(parts)
    expected_keys = {anchor.outcome_key for anchor in anchors}
    if any(
        set(surface.gross_ticks_by_anchor) != expected_keys or surface.censored_anchor_keys
        for surface in surfaces
    ):
        raise AllCasesPipelineError("Search reference surface is not structurally complete")
    return surfaces


def _one_second_path_parts(project_root: Path, plan: _StagePlan) -> Iterator[tuple[object, ...]]:
    """Open at most one outcome span and yield exact segment paths."""

    from .symbolic import OneSecondPath

    for _span_id, grouped_partitions in groupby(
        plan.partitions, key=lambda item: item.outcome_span_id
    ):
        span_partitions = tuple(grouped_partitions)
        rows = _load_stage_bars(project_root, span_partitions, 1)
        structural_rows = _load_stage_bars(project_root, span_partitions, 300)
        structural_by_lineage = _lineage_groups(structural_rows)
        paths = []
        observed_lineages = set()
        for lineage, values in _lineage_groups(rows).items():
            structural = structural_by_lineage.get(lineage)
            if not structural:
                raise AllCasesPipelineError("1s lineage lacks structural 5m coverage proof")
            observed_lineages.add(lineage)
            paths.append(
                OneSecondPath.from_rows(
                    values,
                    coverage_start_ns=structural[0].bar.start_ns,
                    coverage_end_ns=structural[-1].bar.end_ns,
                    structural_five_minute_bars=structural,
                )
            )
        if set(structural_by_lineage) != observed_lineages:
            raise AllCasesPipelineError("structural 5m lineage has no 1s path rows")
        yield tuple(paths)


def _lineage_groups(values: Sequence[object]) -> dict[tuple[str, int, int], tuple[object, ...]]:
    output: dict[tuple[str, int, int], tuple[object, ...]] = {}
    for lineage, grouped in groupby(
        values,
        key=lambda item: (
            item.bar.contract,
            item.outcome_span_id,
            item.bar.segment_id,
        ),
    ):
        if lineage in output:
            raise AllCasesPipelineError("one lineage appears in multiple noncontiguous runs")
        output[lineage] = tuple(grouped)
    return output


def _trade_bars(values: Sequence[object]) -> tuple[TradeBar, ...]:
    return tuple(value.bar for value in values)


def _balanced_chunk_ranges(total: int, count: int) -> tuple[tuple[int, int], ...]:
    if total < count or count < 1:
        raise AllCasesPipelineError("balanced chunk dimensions differ")
    quotient, remainder = divmod(total, count)
    cursor = 0
    output = []
    for index in range(count):
        length = quotient + (1 if index < remainder else 0)
        output.append((cursor, cursor + length))
        cursor += length
    if cursor != total:
        raise AllCasesPipelineError("balanced chunk plan does not cover its family")
    return tuple(output)


def _exact_one_sided_sign_test(values: Sequence[int]) -> Fraction:
    """Exact upper-tail sign test; omit zeros and return one for an empty test."""

    if any(isinstance(value, bool) or not isinstance(value, int) for value in values):
        raise AllCasesPipelineError("sign-test values must be exact integers")
    nonzero = tuple(value for value in values if value != 0)
    if not nonzero:
        return Fraction(1, 1)
    positives = sum(value > 0 for value in nonzero)
    width = len(nonzero)
    return Fraction(sum(comb(width, index) for index in range(positives, width + 1)), 2**width)


def _frozen_daily_vector(
    decision_dates: Sequence[str], daily_net_ticks: Mapping[str, int]
) -> tuple[int, ...]:
    dates = tuple(decision_dates)
    if (
        not dates
        or tuple(sorted(dates)) != dates
        or len(set(dates)) != len(dates)
        or any(not isinstance(item, str) or not item for item in dates)
        or any(key not in set(dates) for key in daily_net_ticks)
        or any(
            isinstance(value, bool) or not isinstance(value, int)
            for value in daily_net_ticks.values()
        )
    ):
        raise AllCasesPipelineError("daily evidence differs from the frozen decision-date vector")
    return tuple(daily_net_ticks.get(day, 0) for day in dates)


def _daily_p_star(
    decision_dates: Sequence[str],
    real: Mapping[str, int],
    circular: Mapping[str, int] | None,
    matched: Mapping[str, int] | None,
    *,
    eligible: bool,
) -> Fraction:
    """Maximum of REAL>0 and both paired REAL-control exact daily sign tests."""

    if not eligible or circular is None or matched is None:
        return Fraction(1, 1)
    real_values = _frozen_daily_vector(decision_dates, real)
    circular_values = _frozen_daily_vector(decision_dates, circular)
    matched_values = _frozen_daily_vector(decision_dates, matched)
    return max(
        _exact_one_sided_sign_test(real_values),
        _exact_one_sided_sign_test(
            tuple(left - right for left, right in zip(real_values, circular_values, strict=True))
        ),
        _exact_one_sided_sign_test(
            tuple(left - right for left, right in zip(real_values, matched_values, strict=True))
        ),
    )


def _benjamini_hochberg_rejections(
    candidate_ids: Sequence[str], p_values: Mapping[str, Fraction], *, q: Fraction
) -> tuple[str, ...]:
    """Exact BH over the entire frozen family, including p=1 failures."""

    family = tuple(candidate_ids)
    if (
        not family
        or len(set(family)) != len(family)
        or set(p_values) != set(family)
        or not isinstance(q, Fraction)
        or not 0 < q <= 1
        or any(
            not isinstance(value, Fraction) or not 0 <= value <= 1 for value in p_values.values()
        )
    ):
        raise AllCasesPipelineError("BH family or exact p-values differ")
    ordered = tuple(sorted(family, key=lambda candidate_id: (p_values[candidate_id], candidate_id)))
    largest = 0
    for rank, candidate_id in enumerate(ordered, start=1):
        if p_values[candidate_id] <= q * rank / len(ordered):
            largest = rank
    rejected = set(ordered[:largest])
    return tuple(candidate_id for candidate_id in family if candidate_id in rejected)


def _holm_rejections(
    candidate_ids: Sequence[str], p_values: Mapping[str, Fraction], *, alpha: Fraction
) -> tuple[str, ...]:
    """Exact Holm step-down over every frozen WF finalist."""

    family = tuple(candidate_ids)
    if (
        not family
        or len(set(family)) != len(family)
        or set(p_values) != set(family)
        or not isinstance(alpha, Fraction)
        or not 0 < alpha <= 1
        or any(
            not isinstance(value, Fraction) or not 0 <= value <= 1 for value in p_values.values()
        )
    ):
        raise AllCasesPipelineError("Holm family or exact p-values differ")
    ordered = tuple(sorted(family, key=lambda candidate_id: (p_values[candidate_id], candidate_id)))
    rejected: set[str] = set()
    for index, candidate_id in enumerate(ordered):
        if p_values[candidate_id] > alpha / (len(ordered) - index):
            break
        rejected.add(candidate_id)
    return tuple(candidate_id for candidate_id in family if candidate_id in rejected)


def _strict_pairwise_diversity(left: Mapping[str, object], right: Mapping[str, object]) -> bool:
    left_actions = {
        (row[0], row[1])
        for row in left["oof_actions"]  # type: ignore[union-attr]
    }
    right_actions = {
        (row[0], row[1])
        for row in right["oof_actions"]  # type: ignore[union-attr]
    }
    union_count = len(left_actions | right_actions)
    if union_count == 0 or 5 * len(left_actions & right_actions) >= 4 * union_count:
        return False
    left_daily = left["daily_net_ticks"]
    right_daily = right["daily_net_ticks"]
    if not isinstance(left_daily, Mapping) or not isinstance(right_daily, Mapping):
        raise AllCasesPipelineError("Search diversity daily PnL mapping differs")
    days = tuple(sorted(set(left_daily) | set(right_daily)))
    if len(days) < 2:
        return False
    x = [left_daily.get(day, 0) for day in days]
    y = [right_daily.get(day, 0) for day in days]
    count = len(days)
    covariance = count * sum(a * b for a, b in zip(x, y, strict=True)) - sum(x) * sum(y)
    variance_x = count * sum(value * value for value in x) - sum(x) ** 2
    variance_y = count * sum(value * value for value in y) - sum(y) ** 2
    if variance_x <= 0 or variance_y <= 0:
        return False
    return 25 * covariance * covariance < 16 * variance_x * variance_y


def _search_rank_key(candidate: Mapping[str, object]) -> tuple[object, ...]:
    return (
        -candidate["positive_outer_validation_count"],  # type: ignore[operator]
        -Fraction(
            candidate["worst_outer_ev_numerator"],  # type: ignore[arg-type]
            candidate["worst_outer_ev_denominator"],  # type: ignore[arg-type]
        ),
        -candidate["stress_net_ticks"],  # type: ignore[operator]
        -Fraction(
            candidate["median_outer_ev_numerator"],  # type: ignore[arg-type]
            candidate["median_outer_ev_denominator"],  # type: ignore[arg-type]
        ),
        candidate["maximum_drawdown_ticks"],
        candidate["candidate_id"],
    )


def _validate_search_selection_row(value: object) -> dict[str, object]:
    """Require the exact typed row consumed by rank and diversity selection."""

    expected_keys = {
        "candidate_id",
        "candidate_kind",
        "daily_net_ticks",
        "family_key",
        "maximum_drawdown_ticks",
        "median_outer_ev_denominator",
        "median_outer_ev_numerator",
        "oof_actions",
        "positive_outer_validation_count",
        "stress_net_ticks",
        "worst_outer_ev_denominator",
        "worst_outer_ev_numerator",
    }
    if not isinstance(value, Mapping) or set(value) != expected_keys:
        raise AllCasesPipelineError("Search selection-row schema differs")
    _require_sha(value["candidate_id"], label="Search selection candidate ID")
    if (
        value["candidate_kind"] not in {"SYMBOLIC", "DIRECT_ML", "META_ML"}
        or not isinstance(value["family_key"], str)
        or not value["family_key"]
    ):
        raise AllCasesPipelineError("Search selection candidate family differs")
    integer_fields = (
        "maximum_drawdown_ticks",
        "median_outer_ev_denominator",
        "median_outer_ev_numerator",
        "positive_outer_validation_count",
        "stress_net_ticks",
        "worst_outer_ev_denominator",
        "worst_outer_ev_numerator",
    )
    if any(
        isinstance(value[field], bool) or not isinstance(value[field], int)
        for field in integer_fields
    ) or any(
        value[field] <= 0 for field in ("median_outer_ev_denominator", "worst_outer_ev_denominator")
    ):
        raise AllCasesPipelineError("Search selection integer metric differs")
    if (
        value["maximum_drawdown_ticks"] < 0
        or not 0 <= value["positive_outer_validation_count"] <= 6
    ):
        raise AllCasesPipelineError("Search selection bounded metric differs")
    daily = value["daily_net_ticks"]
    if not isinstance(daily, Mapping) or not daily:
        raise AllCasesPipelineError("Search selection daily vector differs")
    daily_items = tuple(daily.items())
    for day, net_ticks in daily_items:
        try:
            canonical_day = date.fromisoformat(day).isoformat()
        except (TypeError, ValueError) as error:
            raise AllCasesPipelineError("Search selection daily date differs") from error
        if day != canonical_day or isinstance(net_ticks, bool) or not isinstance(net_ticks, int):
            raise AllCasesPipelineError("Search selection daily value differs")
    if daily_items != tuple(sorted(daily_items)):
        raise AllCasesPipelineError("Search selection daily vector is not canonical")
    actions = value["oof_actions"]
    if not isinstance(actions, list) or not actions:
        raise AllCasesPipelineError("Search selection action vector differs")
    normalized_actions = []
    for row in actions:
        if (
            not isinstance(row, list)
            or len(row) != 2
            or not isinstance(row[0], str)
            or not isinstance(row[1], str)
            or row[1] not in {"LONG", "SHORT"}
        ):
            raise AllCasesPipelineError("Search selection action identity differs")
        _require_sha(row[0], label="Search selection action ID")
        normalized_actions.append((row[0], row[1]))
    if tuple(normalized_actions) != tuple(sorted(set(normalized_actions))):
        raise AllCasesPipelineError("Search selection actions are not canonical")
    return dict(value)


def _select_diverse_search_candidates(
    candidates: Sequence[Mapping[str, object]],
) -> tuple[str, ...]:
    """Apply the frozen rank, family/quota, and exact rational diversity rules."""

    validated = tuple(_validate_search_selection_row(item) for item in candidates)
    ordered = sorted(validated, key=_search_rank_key)
    if len({str(item["candidate_id"]) for item in ordered}) != len(ordered):
        raise AllCasesPipelineError("Search final-selection candidates are duplicated")
    selected: list[Mapping[str, object]] = []
    kind_counts: dict[str, int] = defaultdict(int)
    family_counts: dict[str, int] = defaultdict(int)
    quotas = {"SYMBOLIC": 6, "DIRECT_ML": 4, "META_ML": 4}
    for candidate in ordered:
        kind = str(candidate["candidate_kind"])
        family = str(candidate["family_key"])
        if kind not in quotas or not family:
            raise AllCasesPipelineError("Search final-selection family/kind differs")
        if kind_counts[kind] >= quotas[kind] or family_counts[family] >= 2:
            continue
        if any(not _strict_pairwise_diversity(candidate, prior) for prior in selected):
            continue
        selected.append(candidate)
        kind_counts[kind] += 1
        family_counts[family] += 1
        if len(selected) == 12:
            break
    return tuple(str(item["candidate_id"]) for item in selected)


def _universe_artifact(
    project_root: Path,
    config: AllCasesConfig,
    index: int,
    payload: Mapping[str, object],
    *,
    verify_only: bool,
    allow_incomplete_verify: bool = False,
    resources: _PipelineResourceGuard | None = None,
) -> dict[str, object]:
    root = _safe_directory(
        project_root / _RUN_RELATIVE_ROOT / "internal/universe",
        create=not verify_only,
    )
    staging = _safe_directory(
        project_root / _RUN_RELATIVE_ROOT / "internal/universe-staging",
        create=not verify_only,
    )
    if verify_only:
        if any(staging.iterdir()):
            raise AllCasesPipelineError("read-only universe verification found staging bytes")
    else:
        _recover_staging(staging, (root,))
    envelope = {
        "artifact_schema": "systematic_fx.ai_all_cases_feature_universe_chunk.v1",
        "chunk_index": index,
        "config_semantic_sha256": config.semantic_sha256,
        "payload": dict(payload),
    }
    raw = _canonical_json_bytes(envelope)
    digest = hashlib.sha256(raw).hexdigest()
    relative = f"universe-{index:03d}-{digest}.json"
    matches = tuple(root.glob(f"universe-{index:03d}-*.json"))
    if matches and (len(matches) != 1 or matches[0].name != relative):
        raise AllCasesPipelineError("persisted universe chunk differs on recomputation")
    if verify_only and not matches:
        if allow_incomplete_verify:
            raise _VerifiedPrefixIncomplete("SEARCH_UNIVERSE", index)
        raise AllCasesPipelineError("fresh universe verification found a missing chunk")
    if resources is not None and not matches:
        resources.ensure_additional_bytes(len(raw), f"SEARCH_UNIVERSE_CHUNK_{index:03d}_PUBLISH")
    _publish_mode_0444(root, relative, raw, staging=staging)
    return {
        "artifact_sha256": digest,
        "chunk_index": index,
        "relative_path": relative,
    }


def production_services(*, verify_only: bool = False) -> object:
    """Build the single fixed private service bundle used by public execution."""

    _require_deterministic_runtime_environment()
    from .run import _AllCasesServices

    shared_resources: _PipelineResourceGuard | None = None
    shared_identity: tuple[Path, str] | None = None
    replay_resources_by_identity: dict[tuple[Path, str], _PipelineResourceGuard] = {}

    def resources(root: Path, config: AllCasesConfig) -> _PipelineResourceGuard:
        nonlocal shared_identity, shared_resources
        identity = root, config.semantic_sha256
        if shared_resources is None:
            shared_identity = identity
            shared_resources = _PipelineResourceGuard(root, config, verifier=verify_only)
        elif identity != shared_identity:
            raise AllCasesPipelineError("production services crossed run identities")
        return shared_resources

    def freeze_universe(root: Path, config: AllCasesConfig) -> object:
        guard = resources(root, config)
        guard.check("SERVICE_FREEZE_UNIVERSE_START")
        result = _freeze_search_universe(
            root,
            config,
            verify_only=verify_only,
            resources=guard,
        )
        guard.check("SERVICE_FREEZE_UNIVERSE_END")
        return result

    def train_search(root: Path, config: AllCasesConfig, universe: Mapping[str, object]) -> object:
        guard = resources(root, config)
        guard.check("SERVICE_TRAIN_SEARCH_START")
        result = _train_select_search(
            root,
            config,
            universe,
            verify_only=verify_only,
            resources=guard,
        )
        guard.check("SERVICE_TRAIN_SEARCH_END")
        return result

    def replay_resources(root: Path, config: AllCasesConfig) -> _PipelineResourceGuard:
        identity = root, config.semantic_sha256
        guard = replay_resources_by_identity.get(identity)
        if guard is None:
            guard = _PipelineResourceGuard(root, config, verifier=True)
            replay_resources_by_identity[identity] = guard
        return guard

    def replay_universe_prefix(root: Path, config: AllCasesConfig) -> object:
        guard = replay_resources(root, config)
        guard.check("REPLAY_SEARCH_UNIVERSE_PREFIX_START")
        try:
            payload = _freeze_search_universe(
                root,
                config,
                verify_only=True,
                allow_incomplete_verify=True,
                resources=guard,
            )
        except _VerifiedPrefixIncomplete as incomplete:
            return {
                "complete": False,
                "next_chunk_index": incomplete.chunk_index,
                "next_phase": incomplete.phase,
                "payload": None,
            }
        guard.check("REPLAY_SEARCH_UNIVERSE_PREFIX_END")
        return {
            "complete": True,
            "next_chunk_index": None,
            "next_phase": None,
            "payload": payload,
        }

    def replay_search_prefix(
        root: Path,
        config: AllCasesConfig,
        universe: Mapping[str, object],
    ) -> object:
        guard = replay_resources(root, config)
        guard.check("REPLAY_SEARCH_PREFIX_START")
        try:
            payload = _train_select_search(
                root,
                config,
                universe,
                verify_only=True,
                allow_incomplete_verify=True,
                resources=guard,
            )
        except _VerifiedPrefixIncomplete as incomplete:
            return {
                "complete": False,
                "next_chunk_index": incomplete.chunk_index,
                "next_phase": incomplete.phase,
                "payload": None,
            }
        guard.check("REPLAY_SEARCH_PREFIX_END")
        return {
            "complete": True,
            "next_chunk_index": None,
            "next_phase": None,
            "payload": payload,
        }

    return _AllCasesServices(
        freeze_search_universe=freeze_universe,
        train_select_search=train_search,
        freeze_walk_forward_masks=_freeze_walk_forward_masks,
        evaluate_walk_forward=_evaluate_walk_forward,
        freeze_holdout_masks=_freeze_holdout_masks,
        evaluate_holdout=_evaluate_holdout,
        replay_search_universe_prefix=replay_universe_prefix,
        replay_search_prefix=replay_search_prefix,
    )


# Domain adapters below are completed against the pure symbolic/ML APIs in this
# same provenance closure.  They deliberately remain private.
def _freeze_search_universe(
    project_root: Path,
    config: AllCasesConfig,
    *,
    verify_only: bool = False,
    allow_incomplete_verify: bool = False,
    resources: _PipelineResourceGuard | None = None,
) -> object:
    from . import ml, symbolic

    resources = (
        resources
        if resources is not None
        else _PipelineResourceGuard(project_root, config, verifier=verify_only)
    )
    resources.check("SEARCH_UNIVERSE_START")
    state = _search_feature_state(project_root)
    stage = state.symbolic_stage
    structural_lattice = state.structural_lattice
    direct_features = _direct_feature_bundles(state)
    direct_feature_sha256, direct_feature_summaries = _direct_feature_universe_commitment(
        direct_features
    )
    contract = symbolic.symbolic_engine_contract()
    axes = contract["axes"]
    policy_count = int(axes["logical_anchor_policy_count"])
    chunk_plan = symbolic.build_stage_a_chunk_plan()
    if len(chunk_plan) != 64:
        raise AllCasesPipelineError("symbolic Stage-A plan differs from 64 chunks")
    leaves = []
    commitments = symbolic.iter_feature_universe_commitment_chunks(
        stage,
        structural_lattice=structural_lattice,
    )
    for expected_chunk, commitment in zip(chunk_plan, commitments, strict=True):
        resources.check(f"SEARCH_UNIVERSE_CHUNK_{expected_chunk.chunk_index:03d}_START")
        if commitment.chunk != expected_chunk:
            raise AllCasesPipelineError("feature commitment chunk differs from frozen plan")
        payload = {
            **commitment.as_dict(),
            "structural_opportunity_lattice_sha256": structural_lattice.artifact_sha256,
        }
        if expected_chunk.chunk_index == 0:
            payload["structural_opportunity_lattice"] = structural_lattice.as_dict()
            payload["direct_opportunity_lattice"] = state.direct_opportunity_lattice.as_dict()
            payload["direct_feature_summaries"] = list(direct_feature_summaries)
            payload["direct_feature_universe_sha256"] = direct_feature_sha256
        leaves.append(
            _universe_artifact(
                project_root,
                config,
                expected_chunk.chunk_index,
                payload,
                verify_only=verify_only,
                allow_incomplete_verify=allow_incomplete_verify,
                resources=resources,
            )
        )
        resources.check(f"SEARCH_UNIVERSE_CHUNK_{expected_chunk.chunk_index:03d}_END")
    universe_root = _safe_directory(
        project_root / _RUN_RELATIVE_ROOT / "internal/universe", create=False
    )
    observed = set()
    for path in universe_root.iterdir():
        metadata = path.stat(follow_symlinks=False)
        if (
            path.is_symlink()
            or not path.is_file()
            or metadata.st_nlink != 1
            or metadata.st_mode & _WRITE_BITS
        ):
            raise AllCasesPipelineError("feature universe artifact directory contains unsafe bytes")
        raw = path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        if not path.name.endswith(f"-{digest}.json"):
            raise AllCasesPipelineError("feature universe artifact filename/content hash differs")
        observed.add(path.name)
    expected = {str(leaf["relative_path"]) for leaf in leaves}
    if observed != expected:
        raise AllCasesPipelineError("feature universe artifact leaf closure differs")
    universe_staging = _safe_directory(
        project_root / _RUN_RELATIVE_ROOT / "internal/universe-staging", create=False
    )
    if any(universe_staging.iterdir()):
        raise AllCasesPipelineError("feature universe staging is not empty at release")
    resources.check("SEARCH_UNIVERSE_END")
    direct = ml.direct_catalog_document()
    meta = ml.meta_catalog_document()
    entry_exit_recipe_sha256 = config.as_dict()["execution"]["entry_exit_recipe_sha256"]
    identity = {
        "anchor_policy_count": policy_count,
        "anchor_policy_identity_root_sha256": _sha256(
            {
                "anchor_policy_recipe_sha256": axes["anchor_policy_recipe_sha256"],
                "nesting_order": ["BASE_EVENT", "CONTEXT", "TIME_FILTER", "DELAY"],
                "policy_count": policy_count,
            }
        ),
        "anchor_policy_recipe_sha256": axes["anchor_policy_recipe_sha256"],
        "direct_catalog_sha256": direct["catalog_sha256"],
        "direct_feature_universe_sha256": direct_feature_sha256,
        "direct_opportunity_count": len(state.direct_opportunity_lattice.opportunities),
        "direct_opportunity_lattice_sha256": (state.direct_opportunity_lattice.artifact_sha256),
        "entry_exit_recipe_sha256": entry_exit_recipe_sha256,
        "feature_event_universe_sha256": _sha256(leaves),
        "meta_catalog_sha256": meta["catalog_sha256"],
        "stage_a_chunk_plan_sha256": contract["stage_a_chunking"]["chunk_plan_sha256"],
        "stage_a_chunk_count": 64,
        "stage_a_policy_rows_per_chunk_maximum": max(chunk.policy_count for chunk in chunk_plan),
        "structural_opportunity_count": len(structural_lattice.eligible_anchor_keys),
        "structural_opportunity_lattice_lookahead_seconds": (
            structural_lattice.maximum_path_seconds
        ),
        "structural_opportunity_lattice_sha256": structural_lattice.artifact_sha256,
    }
    return {
        **identity,
        "feature_mask_chunk_artifacts": leaves,
        "schema": "systematic_fx.ai_all_cases_search_universe_payload.v1",
        "universe_root_sha256": _sha256(identity),
    }


def _search_phase_counts(config: AllCasesConfig) -> dict[str, int]:
    value = (
        config.as_dict().get("search_design", {}).get("search_phase_chunk_counts_canonical_json")
    )
    if not isinstance(value, str):
        raise AllCasesPipelineError("Search phase-count contract is absent")
    try:
        document = json.loads(value)
    except json.JSONDecodeError as error:
        raise AllCasesPipelineError("Search phase-count contract is invalid") from error
    if (
        not isinstance(document, dict)
        or set(document) != set(_SEARCH_PHASE_ORDER)
        or any(
            isinstance(count, bool) or not isinstance(count, int) or count < 1
            for count in document.values()
        )
        or sum(document.values()) != 181
    ):
        raise AllCasesPipelineError("Search phase-count contract differs")
    return {phase: int(document[phase]) for phase in _SEARCH_PHASE_ORDER}


def _select_stage_a_from_score_chunks(
    config: AllCasesConfig,
    score_chunks: Sequence[object],
) -> object:
    """Replay the frozen Stage-A selector from its complete ordered raw evidence."""

    from . import symbolic

    chunks = tuple(score_chunks)
    if len(chunks) != _search_phase_counts(config)["STAGE_A_SCORE_CHUNKS"]:
        raise AllCasesPipelineError("Stage-A replay does not contain every raw chunk")
    if tuple(chunk.chunk.chunk_index for chunk in chunks) != tuple(range(len(chunks))):
        raise AllCasesPipelineError("Stage-A replay chunk order differs")
    scores = tuple(score for chunk in chunks for score in chunk.scores)
    selection = symbolic.select_stage_a_top256(scores)
    pair_budget = (
        config.as_dict().get("compute_caps", {}).get("stage_b_anchor_entry_world_pair_budget")
    )
    if (
        pair_budget != 100_000
        or selection.stage_b_pair_budget_maximum != pair_budget
        or selection.stage_b_pair_budget_used
        != sum(item.evaluable_support_count for item in selection.selected_scores) * 9 * 3
        or selection.stage_b_pair_budget_used > pair_budget
        or selection.budget_rejected_policy_count < 0
    ):
        raise AllCasesPipelineError("Stage-A selection violates its frozen Stage-B budget")
    return selection


def _ensure_stage_a_search(
    project_root: Path,
    config: AllCasesConfig,
    universe: Mapping[str, object],
    ledger: _SearchSubledger,
    state: _SearchFeatureState,
    *,
    verify_only: bool,
) -> tuple[tuple[object, ...], object, tuple[object, ...]]:
    """Evaluate all 64 frozen Stage-A chunks and publish one atomic top-256 barrier."""

    from . import symbolic

    if state.structural_lattice.artifact_sha256 != universe.get(
        "structural_opportunity_lattice_sha256"
    ):
        raise AllCasesPipelineError("Search structural lattice differs from the universe barrier")
    surfaces = _search_reference_surfaces(project_root, state)
    plan = symbolic.build_stage_a_chunk_plan()
    counts = _search_phase_counts(config)
    if len(plan) != counts["STAGE_A_SCORE_CHUNKS"]:
        raise AllCasesPipelineError("Stage-A internal plan differs")
    score_chunks: dict[int, object] = {}

    def score_builder(index: int) -> Mapping[str, object]:
        chunk = symbolic.score_stage_a_cube_chunk(
            state.symbolic_stage,
            plan[index],
            surfaces,
            state.plan.reporting_group_by_date,
            structural_lattice=state.structural_lattice,
        )
        score_chunks[index] = chunk
        return {
            "schema": "systematic_fx.ai_all_cases_stage_a_raw_chunk.v1",
            "score_chunk": chunk.as_dict(),
            "structural_lattice_sha256": state.structural_lattice.artifact_sha256,
        }

    def resume_score(index: int, payload: Mapping[str, object]) -> None:
        if payload.get("structural_lattice_sha256") != state.structural_lattice.artifact_sha256:
            raise AllCasesPipelineError("persisted Stage-A structural lattice differs")
        chunk = symbolic.stage_a_score_chunk_from_dict(payload.get("score_chunk"))
        if chunk.chunk != plan[index]:
            raise AllCasesPipelineError("persisted Stage-A chunk coordinate differs")
        score_chunks[index] = chunk

    ledger.ensure_phase(
        "STAGE_A_SCORE_CHUNKS",
        counts["STAGE_A_SCORE_CHUNKS"],
        score_builder,
        verify_only=verify_only,
        resume_consumer=resume_score,
    )
    ordered_chunks = tuple(score_chunks[index] for index in range(len(plan)))
    selection = _select_stage_a_from_score_chunks(config, ordered_chunks)

    def selection_builder(index: int) -> Mapping[str, object]:
        if index != 0:
            raise AllCasesPipelineError("Stage-A selection is a single barrier")
        return {
            "schema": "systematic_fx.ai_all_cases_stage_a_selection.v1",
            "selection": selection.as_dict(),
            "source_chunk_artifact_sha256s": [chunk.artifact_sha256 for chunk in ordered_chunks],
        }

    expected_selection_payload = selection_builder(0)

    def resume_selection(index: int, payload: Mapping[str, object]) -> None:
        if index != 0 or dict(payload) != expected_selection_payload:
            raise AllCasesPipelineError(
                "persisted Stage-A selection differs from its raw-score replay"
            )
        reopened = symbolic.stage_a_selection_from_dict(payload.get("selection"))
        if reopened != selection:
            raise AllCasesPipelineError("persisted Stage-A selection did not round trip")

    ledger.ensure_phase(
        "STAGE_A_TOP256",
        counts["STAGE_A_TOP256"],
        selection_builder,
        verify_only=verify_only,
        resume_consumer=resume_selection,
    )
    return surfaces, selection, ordered_chunks


def _policy_from_stage_a_score(score: object) -> object:
    from . import symbolic

    contexts = symbolic.build_context_catalog()
    times = symbolic.build_time_filter_catalog()
    delays = symbolic.build_delay_catalog()
    per_candidate = len(contexts) * len(times) * len(delays)
    candidate_rank, remainder = divmod(score.policy_rank - 1, per_candidate)
    context_rank, remainder = divmod(remainder, len(times) * len(delays))
    time_rank, delay_rank = divmod(remainder, len(delays))
    candidate = symbolic.build_base_event_catalog().candidates[candidate_rank]
    policy = symbolic.AnchorPolicy(
        score.policy_rank,
        score.policy_id,
        candidate.candidate_id,
        contexts[context_rank].context_id,
        times[time_rank].time_filter_id,
        delays[delay_rank].delay_id,
    )
    if candidate.candidate_id != score.base_candidate_id:
        raise AllCasesPipelineError("Stage-A policy rank/base identity differs")
    return policy


def _empty_stage_b_chunk_documents() -> tuple[dict[str, object], ...]:
    rows = []
    for index in range(64):
        definition = {
            "chunk_index": index,
            "first_strategy_rank": 1,
            "last_strategy_rank": 0,
            "schema": "systematic_fx.ai_all_cases_complete_path_outcome.v1",
            "strategy_count": 0,
        }
        rows.append({**definition, "chunk_id": _sha256(definition)})
    return tuple(rows)


def _stage_b_chunk_bounds(chunk: object) -> tuple[int, int, int]:
    document = chunk.as_dict() if hasattr(chunk, "as_dict") else dict(chunk)
    try:
        first = int(document["first_strategy_rank"])
        last = int(document["last_strategy_rank"])
        count = int(document["strategy_count"])
    except (KeyError, TypeError, ValueError) as error:
        raise AllCasesPipelineError("Stage-B chunk bounds are invalid") from error
    if count == 0:
        if (first, last) != (1, 0):
            raise AllCasesPipelineError("empty Stage-B chunk bounds differ")
    elif first < 1 or last < first or last - first + 1 != count:
        raise AllCasesPipelineError("Stage-B chunk bounds differ")
    return first, last, count


def _ensure_stage_b_feature_plan(
    config: AllCasesConfig,
    ledger: _SearchSubledger,
    state: _SearchFeatureState,
    stage_a_selection: object,
    *,
    verify_only: bool,
) -> _StageBFeaturePlan:
    """Freeze every Stage-B recipe/control/order/rule commitment before its 1s evaluation."""

    from . import symbolic

    policies_by_id: dict[str, object] = {}
    family_by_policy_id: dict[str, str] = {}
    controls_by_policy_id: dict[str, object] = {}
    masks_by_world: dict[str, dict[str, object]] = {
        "REAL": {},
        "CIRCULAR": {},
        "MATCHED": {},
    }
    schedules_by_world: dict[str, dict[str, object]] = {
        "REAL": {},
        "CIRCULAR": {},
        "MATCHED": {},
    }
    control_lattice = symbolic.build_control_opportunity_lattice(
        state.bars_by_timeframe[300],
        decision_dates=state.plan.decision_dates,
        allowed_tail_end_ns=state.allowed_tail_end_ns,
        signal_bars_by_timeframe=state.bars_by_timeframe,
    )
    structural_keys = set(state.structural_lattice.eligible_anchor_keys)
    control_keys = {
        (
            item.contract,
            item.outcome_span_id,
            item.segment_id,
            item.anchor_ns,
        )
        for item in control_lattice.opportunities
    }
    if not control_keys <= structural_keys:
        raise AllCasesPipelineError("control opportunities escape the frozen structural lattice")

    policy_commitments = []
    for score in stage_a_selection.selected_scores:
        policy = _policy_from_stage_a_score(score)
        raw_mask = state.symbolic_stage.policy_mask(policy)
        structural = symbolic.freeze_structurally_eligible_policy_mask(
            raw_mask,
            state.structural_lattice,
        )
        if (
            raw_mask.mask_sha256 != score.raw_mask_sha256
            or structural.evaluable_mask.mask_sha256 != score.mask_sha256
        ):
            raise AllCasesPipelineError("selected Stage-A mask differs on feature-only replay")
        controls = symbolic.freeze_feature_control_masks(
            "SEARCH",
            structural.evaluable_mask,
            control_lattice,
            reporting_group_by_date=state.plan.reporting_group_by_date,
        )
        rules = symbolic.build_control_rule_exit_schedules(
            state.symbolic_stage,
            policy,
            controls,
        )
        policies_by_id[policy.policy_id] = policy
        family_by_policy_id[policy.policy_id] = str(score.family)
        controls_by_policy_id[policy.policy_id] = controls
        world_masks = {
            "REAL": controls.real,
            "CIRCULAR": controls.circular,
            "MATCHED": controls.matched,
        }
        world_schedules = {
            "REAL": rules.real,
            "CIRCULAR": rules.circular,
            "MATCHED": rules.matched,
        }
        entry_order_sha256s = {}
        for world in ("REAL", "CIRCULAR", "MATCHED"):
            mask = world_masks[world]
            schedule = world_schedules[world]
            if mask is not None:
                masks_by_world[world][policy.policy_id] = mask
                entry_order_sha256s[world] = symbolic.freeze_entry_orders(mask).artifact_sha256
            else:
                entry_order_sha256s[world] = None
            if schedule is not None:
                schedules_by_world[world][policy.policy_id] = schedule
        policy_commitments.append(
            {
                "control_masks": controls.as_dict(),
                "entry_order_sha256s": entry_order_sha256s,
                "family": str(score.family),
                "policy_id": policy.policy_id,
                "rule_schedules": rules.as_dict(),
                "structural_mask": structural.as_dict(),
            }
        )

    selected_policy_ids = tuple(stage_a_selection.selected_policy_ids)
    recipes = (
        tuple(symbolic.iter_complete_strategy_recipes(selected_policy_ids))
        if selected_policy_ids
        else ()
    )
    chunks: tuple[object, ...] = tuple(symbolic.build_stage_b_chunk_plan(len(selected_policy_ids)))
    if len(chunks) != 64 or len(recipes) != len(selected_policy_ids) * 9 * 85:
        raise AllCasesPipelineError("Stage-B recipe/chunk dimensions differ")
    chunk_documents = [
        chunk.as_dict() if hasattr(chunk, "as_dict") else dict(chunk) for chunk in chunks
    ]
    plan_document = {
        "complete_recipe_count": len(recipes),
        "complete_recipe_root_sha256": _sha256([recipe.as_dict() for recipe in recipes]),
        "control_opportunity_lattice_sha256": control_lattice.artifact_sha256,
        "policy_feature_commitments": policy_commitments,
        "schema": "systematic_fx.ai_all_cases_stage_b_feature_plan.v1",
        "selected_anchor_policy_ids": list(selected_policy_ids),
        "stage_b_chunks": chunk_documents,
        "structural_lattice_sha256": state.structural_lattice.artifact_sha256,
    }
    counts = _search_phase_counts(config)

    def resume_plan(index: int, payload: Mapping[str, object]) -> None:
        if index != 0 or dict(payload) != plan_document:
            raise AllCasesPipelineError(
                "persisted Stage-B feature plan differs on feature-only replay"
            )

    ledger.ensure_phase(
        "STAGE_B_PLAN_FROZEN",
        counts["STAGE_B_PLAN_FROZEN"],
        lambda index: (
            plan_document
            if index == 0
            else (_ for _ in ()).throw(AllCasesPipelineError("Stage-B plan is a single barrier"))
        ),
        verify_only=verify_only,
        resume_consumer=resume_plan,
    )
    return _StageBFeaturePlan(
        recipes,
        chunks,
        policies_by_id,
        family_by_policy_id,
        controls_by_policy_id,
        masks_by_world,
        schedules_by_world,
        plan_document,
    )


def _evaluate_stage_b_recipe_chunk(
    project_root: Path,
    state: _SearchFeatureState,
    feature_plan: _StageBFeaturePlan,
    chunk_index: int,
    evaluation_sink: dict[str, dict[str, object]],
    coverage_sink: dict[str, dict[str, object]],
    ineligibility_sink: dict[str, dict[str, str]],
) -> Mapping[str, object]:
    """Stream one frozen recipe chunk across disjoint outcome spans and three worlds."""

    from . import symbolic

    chunk = feature_plan.chunks[chunk_index]
    first, last, count = _stage_b_chunk_bounds(chunk)
    chunk_document = chunk.as_dict() if hasattr(chunk, "as_dict") else dict(chunk)
    if count == 0:
        return {
            "chunk": chunk_document,
            "coverage_by_world": {world: [] for world in ("REAL", "CIRCULAR", "MATCHED")},
            "evaluation_chunks_by_world": {
                world: None for world in ("REAL", "CIRCULAR", "MATCHED")
            },
            "schema": "systematic_fx.ai_all_cases_stage_b_raw_chunk.v1",
        }
    recipes = feature_plan.recipes[first - 1 : last]
    if len(recipes) != count:
        raise AllCasesPipelineError("Stage-B recipe slice differs from its frozen chunk")

    partials: dict[str, dict[str, list[object]]] = {
        world: {recipe.strategy_id: [] for recipe in recipes}
        for world in ("REAL", "CIRCULAR", "MATCHED")
    }
    for paths in _one_second_path_parts(project_root, state.plan):
        for world in ("REAL", "CIRCULAR", "MATCHED"):
            masks = feature_plan.masks_by_world[world]
            schedules = feature_plan.schedules_by_world[world]
            evaluable_recipes = tuple(
                recipe for recipe in recipes if recipe.anchor_policy_id in masks
            )
            if not evaluable_recipes:
                continue
            for microbatch in symbolic.iter_complete_strategy_evaluation_chunks(
                evaluable_recipes,
                masks_by_policy_id=masks,
                paths=paths,
                rule_schedules_by_policy_id=schedules,
                reporting_group_by_date=state.plan.reporting_group_by_date,
                outer_validation_by_date=state.plan.search_block_by_date,
                batch_size=64,
            ):
                for evaluation in microbatch.evaluations:
                    partials[world][evaluation.recipe.strategy_id].append(evaluation)

    coverage_rows_by_world: dict[str, list[dict[str, object]]] = {
        world: [] for world in ("REAL", "CIRCULAR", "MATCHED")
    }
    chunk_evaluations_by_world: dict[str, list[object]] = {
        world: [] for world in ("REAL", "CIRCULAR", "MATCHED")
    }
    for world in ("REAL", "CIRCULAR", "MATCHED"):
        masks = feature_plan.masks_by_world[world]
        for recipe in recipes:
            strategy_id = recipe.strategy_id
            mask = masks.get(recipe.anchor_policy_id)
            if mask is None:
                reason = "FEATURE_CONTROL_MASK_INELIGIBLE"
                ineligibility_sink[world][strategy_id] = reason
                coverage_rows_by_world[world].append(
                    {
                        "coverage": None,
                        "ineligibility_reason": reason,
                        "strategy_id": strategy_id,
                    }
                )
                continue
            parts = partials[world][strategy_id]
            if not parts:
                raise AllCasesPipelineError("Stage-B evaluation omitted a frozen recipe")
            evaluation = symbolic.merge_complete_strategy_evaluations(parts)
            coverage = symbolic.verify_complete_evaluation_coverage(evaluation, mask)
            if evaluation.censored_count != 0:
                raise AllCasesPipelineError(
                    "post-freeze Stage-B structural path unexpectedly censored"
                )
            evaluation_sink[world][strategy_id] = evaluation
            coverage_sink[world][strategy_id] = coverage
            chunk_evaluations_by_world[world].append(evaluation)
            coverage_rows_by_world[world].append(
                {
                    "coverage": coverage.as_dict(),
                    "ineligibility_reason": None,
                    "strategy_id": strategy_id,
                }
            )
    return {
        "chunk": chunk_document,
        "coverage_by_world": coverage_rows_by_world,
        "evaluation_chunks_by_world": {
            world: (
                None
                if not chunk_evaluations_by_world[world]
                else symbolic.CompleteEvaluationChunk.from_evaluations(
                    chunk_evaluations_by_world[world]
                ).as_dict()
            )
            for world in ("REAL", "CIRCULAR", "MATCHED")
        },
        "schema": "systematic_fx.ai_all_cases_stage_b_raw_chunk.v1",
    }


def _consume_stage_b_raw_chunk(
    symbolic: object,
    *,
    index: int,
    payload: Mapping[str, object],
    chunks: Sequence[object],
    recipes: Sequence[object],
    masks_by_world: Mapping[str, Mapping[str, object]],
    evaluations: dict[str, dict[str, object]],
    coverage: dict[str, dict[str, object]],
    ineligibility: dict[str, dict[str, str]],
) -> None:
    """Strictly restore one Stage-B raw chunk into deterministic replay state."""

    worlds = ("REAL", "CIRCULAR", "MATCHED")
    _validate_raw_chunk_payload(payload, phase="STAGE_B_RAW_CHUNKS")
    if payload.get("schema") != "systematic_fx.ai_all_cases_stage_b_raw_chunk.v1":
        raise AllCasesPipelineError("persisted Stage-B raw schema differs")
    if isinstance(index, bool) or not isinstance(index, int) or not 0 <= index < len(chunks):
        raise AllCasesPipelineError("persisted Stage-B raw coordinate differs")
    expected_chunk = chunks[index]
    expected_document = (
        expected_chunk.as_dict() if hasattr(expected_chunk, "as_dict") else dict(expected_chunk)
    )
    observed_chunk = payload.get("chunk")
    if _canonical_json_bytes(observed_chunk) != _canonical_json_bytes(expected_document):
        raise AllCasesPipelineError("persisted Stage-B chunk plan differs")
    first, last, count = _stage_b_chunk_bounds(expected_chunk)
    chunk_recipes = tuple(recipes[first - 1 : last])
    expected_strategy_ids = tuple(recipe.strategy_id for recipe in chunk_recipes)
    if len(chunk_recipes) != count or len(set(expected_strategy_ids)) != count:
        raise AllCasesPipelineError("persisted Stage-B recipe slice differs")
    recipe_by_id = {recipe.strategy_id: recipe for recipe in chunk_recipes}
    evaluation_worlds = payload.get("evaluation_chunks_by_world")
    coverage_worlds = payload.get("coverage_by_world")
    if (
        not isinstance(evaluation_worlds, Mapping)
        or set(evaluation_worlds) != set(worlds)
        or not isinstance(coverage_worlds, Mapping)
        or set(coverage_worlds) != set(worlds)
        or set(masks_by_world) != set(worlds)
    ):
        raise AllCasesPipelineError("persisted Stage-B worlds differ")
    for world in worlds:
        compact = evaluation_worlds[world]
        try:
            decoded_evaluations = (
                ()
                if compact is None
                else symbolic.complete_evaluation_chunk_from_dict(compact).evaluations
            )
        except symbolic.SymbolicEngineError as error:
            raise AllCasesPipelineError("persisted compact Stage-B evaluations differ") from error
        decoded_by_id: dict[str, object] = {}
        for evaluation in decoded_evaluations:
            strategy_id = evaluation.recipe.strategy_id
            if (
                strategy_id in decoded_by_id
                or strategy_id not in recipe_by_id
                or evaluation.recipe != recipe_by_id[strategy_id]
            ):
                raise AllCasesPipelineError("persisted compact Stage-B evaluations differ")
            decoded_by_id[strategy_id] = evaluation
        raw_rows = coverage_worlds[world]
        if (
            not isinstance(raw_rows, list)
            or tuple(row.get("strategy_id") for row in raw_rows) != expected_strategy_ids
        ):
            raise AllCasesPipelineError("persisted Stage-B world rows differ")
        for raw in raw_rows:
            strategy_id = _require_sha(
                raw.get("strategy_id"), label="persisted Stage-B strategy ID"
            )
            recipe = recipe_by_id[strategy_id]
            mask = masks_by_world[world].get(recipe.anchor_policy_id)
            reason = raw.get("ineligibility_reason")
            if reason is not None:
                if (
                    reason != "FEATURE_CONTROL_MASK_INELIGIBLE"
                    or raw.get("coverage") is not None
                    or strategy_id in decoded_by_id
                    or mask is not None
                    or strategy_id in ineligibility[world]
                    or strategy_id in evaluations[world]
                ):
                    raise AllCasesPipelineError("persisted Stage-B ineligibility differs")
                ineligibility[world][strategy_id] = reason
                continue
            evaluation = decoded_by_id.pop(strategy_id, None)
            if (
                evaluation is None
                or mask is None
                or strategy_id in evaluations[world]
                or strategy_id in ineligibility[world]
                or evaluation.censored_count != 0
            ):
                raise AllCasesPipelineError("persisted Stage-B coverage lacks its evaluation")
            try:
                commitment = symbolic.complete_evaluation_coverage_from_dict(raw.get("coverage"))
                replayed = symbolic.verify_complete_evaluation_coverage(evaluation, mask)
            except symbolic.SymbolicEngineError as error:
                raise AllCasesPipelineError("persisted Stage-B coverage differs") from error
            if replayed != commitment:
                raise AllCasesPipelineError("persisted Stage-B evaluation identity differs")
            evaluations[world][strategy_id] = evaluation
            coverage[world][strategy_id] = commitment
        if decoded_by_id:
            raise AllCasesPipelineError("persisted compact Stage-B evaluation lacks coverage")


def _aggregate_stage_b_search_evidence(
    symbolic: object,
    recipes: Sequence[object],
    family_by_policy_id: Mapping[str, str],
    evaluations: Mapping[str, Mapping[str, object]],
    coverage: Mapping[str, Mapping[str, object]],
    ineligibility: Mapping[str, Mapping[str, str]],
) -> _StageBSearchEvidence:
    """Replay every Stage-B gate, ranking, and selection from immutable raw aggregates."""

    worlds = ("REAL", "CIRCULAR", "MATCHED")
    canonical_recipes = tuple(recipes)
    strategy_ids = tuple(recipe.strategy_id for recipe in canonical_recipes)
    if (
        set(evaluations) != set(worlds)
        or set(coverage) != set(worlds)
        or set(ineligibility) != set(worlds)
        or len(set(strategy_ids)) != len(strategy_ids)
    ):
        raise AllCasesPipelineError("Stage-B aggregate replay family differs")
    expected_ids = set(strategy_ids)
    for world in worlds:
        evaluated_ids = set(evaluations[world])
        ineligible_ids = set(ineligibility[world])
        if (
            evaluated_ids & ineligible_ids
            or evaluated_ids | ineligible_ids != expected_ids
            or set(coverage[world]) != evaluated_ids
            or any(
                reason != "FEATURE_CONTROL_MASK_INELIGIBLE"
                for reason in ineligibility[world].values()
            )
        ):
            raise AllCasesPipelineError("Stage-B raw aggregate closure differs")
    if set(evaluations["REAL"]) != expected_ids or ineligibility["REAL"]:
        raise AllCasesPipelineError("Stage-B REAL aggregate closure differs")

    real_values = tuple(evaluations["REAL"][strategy_id] for strategy_id in strategy_ids)
    representatives = (
        symbolic.deduplicate_complete_evaluations(real_values).representatives
        if real_values
        else ()
    )
    gates = []
    for real in representatives:
        strategy_id = real.recipe.strategy_id
        if any(strategy_id not in evaluations[world] for world in ("CIRCULAR", "MATCHED")):
            continue
        gates.append(
            symbolic.apply_complete_search_gates(
                real,
                evaluations["CIRCULAR"][strategy_id],
                evaluations["MATCHED"][strategy_id],
                coverage_commitments=(
                    coverage["REAL"][strategy_id],
                    coverage["CIRCULAR"][strategy_id],
                    coverage["MATCHED"][strategy_id],
                ),
            )
        )
    symbolic_selection = symbolic.select_complete_search_symbolic(
        representatives,
        gates,
        family_by_policy_id,
    )
    scopes = ("B3", "B4", "B5", "B6", "B7", "B8", "SEARCH_FINAL")
    top24: dict[str, dict[str, object]] = {}
    for world in worlds:
        world_values = tuple(
            evaluations[world][item.recipe.strategy_id]
            for item in representatives
            if item.recipe.strategy_id in evaluations[world]
        )
        top24[world] = {
            scope: symbolic.select_symbolic_top24_for_meta(scope, world_values) for scope in scopes
        }
    top24_document = {
        "complete_search_gate_results": [item.as_dict() for item in gates],
        "ineligibility_by_world": {world: dict(ineligibility[world]) for world in worlds},
        "schema": "systematic_fx.ai_all_cases_symbolic_top24.v1",
        "symbolic_search_selection": symbolic_selection.as_dict(),
        "top24_by_world_and_scope": {
            world: {scope: value.as_dict() for scope, value in by_scope.items()}
            for world, by_scope in top24.items()
        },
    }
    return _StageBSearchEvidence(
        evaluations,
        coverage,
        ineligibility,
        tuple(representatives),
        tuple(gates),
        top24,
        symbolic_selection,
        top24_document,
    )


def _ensure_stage_b_search_evidence(
    project_root: Path,
    config: AllCasesConfig,
    ledger: _SearchSubledger,
    state: _SearchFeatureState,
    feature_plan: _StageBFeaturePlan,
    *,
    verify_only: bool,
) -> _StageBSearchEvidence:
    """Finish all 64 raw chunks, then release symbolic rankings at one barrier."""

    from . import symbolic

    counts = _search_phase_counts(config)
    if counts["STAGE_B_RAW_CHUNKS"] != len(feature_plan.chunks):
        raise AllCasesPipelineError("Stage-B raw phase count differs")
    evaluations: dict[str, dict[str, object]] = {
        world: {} for world in ("REAL", "CIRCULAR", "MATCHED")
    }
    coverage: dict[str, dict[str, object]] = {
        world: {} for world in ("REAL", "CIRCULAR", "MATCHED")
    }
    ineligibility: dict[str, dict[str, str]] = {
        world: {} for world in ("REAL", "CIRCULAR", "MATCHED")
    }

    def resume_chunk(index: int, payload: Mapping[str, object]) -> None:
        _consume_stage_b_raw_chunk(
            symbolic,
            index=index,
            payload=payload,
            chunks=feature_plan.chunks,
            recipes=feature_plan.recipes,
            masks_by_world=feature_plan.masks_by_world,
            evaluations=evaluations,
            coverage=coverage,
            ineligibility=ineligibility,
        )

    ledger.ensure_phase(
        "STAGE_B_RAW_CHUNKS",
        counts["STAGE_B_RAW_CHUNKS"],
        lambda index: _evaluate_stage_b_recipe_chunk(
            project_root,
            state,
            feature_plan,
            index,
            evaluations,
            coverage,
            ineligibility,
        ),
        verify_only=verify_only,
        resume_consumer=resume_chunk,
    )

    evidence = _aggregate_stage_b_search_evidence(
        symbolic,
        feature_plan.recipes,
        feature_plan.family_by_policy_id,
        evaluations,
        coverage,
        ineligibility,
    )

    def resume_top24(index: int, payload: Mapping[str, object]) -> None:
        if index != 0 or _canonical_json_bytes(payload) != _canonical_json_bytes(
            evidence.top24_document
        ):
            raise AllCasesPipelineError("persisted symbolic top24 differs from aggregate replay")
        reopened_selection = symbolic.complete_search_selection_from_dict(
            payload.get("symbolic_search_selection")
        )
        if reopened_selection != evidence.symbolic_selection:
            raise AllCasesPipelineError("persisted symbolic selection did not round trip")
        persisted_rankings = payload.get("top24_by_world_and_scope")
        if not isinstance(persisted_rankings, Mapping):
            raise AllCasesPipelineError("persisted symbolic top24 rankings differ")
        for world, by_scope in evidence.top24_by_world_and_scope.items():
            raw_world = persisted_rankings.get(world)
            if not isinstance(raw_world, Mapping):
                raise AllCasesPipelineError("persisted symbolic top24 world differs")
            for scope, expected in by_scope.items():
                reopened = symbolic.symbolic_top24_selection_from_dict(raw_world.get(scope))
                if reopened != expected:
                    raise AllCasesPipelineError(
                        "persisted symbolic top24 ranking did not round trip"
                    )

    ledger.ensure_phase(
        "SYMBOLIC_TOP24",
        counts["SYMBOLIC_TOP24"],
        lambda index: (
            evidence.top24_document
            if index == 0
            else (_ for _ in ()).throw(AllCasesPipelineError("symbolic top24 is a single barrier"))
        ),
        verify_only=verify_only,
        resume_consumer=resume_top24,
    )
    return evidence


def _merge_selected_strategy_detail_parts(
    symbolic: object,
    parts: Sequence[object],
    expected_evaluation: object,
) -> object:
    values = tuple(parts)
    if not values:
        raise AllCasesPipelineError("selected symbolic detail has no streamed parts")
    first = values[0]
    if any(
        item.selection_rank != first.selection_rank
        or item.scope_key != first.scope_key
        or item.world != first.world
        or item.recipe != first.recipe
        for item in values[1:]
    ):
        raise AllCasesPipelineError("selected symbolic detail parts differ")
    rows = tuple(row for item in values for row in item.rows)
    keys = tuple(row.anchor_key for row in rows)
    if len(set(keys)) != len(keys):
        raise AllCasesPipelineError("selected symbolic detail repeats an anchor")
    if (
        len(rows) != expected_evaluation.raw_signal_count
        or sum(row.status == "FILLED" for row in rows) != expected_evaluation.fill_count
        or sum(row.censored for row in rows) != expected_evaluation.censored_count
        or sum(row.net_pnl_ticks or 0 for row in rows) != expected_evaluation.total_net_ticks
    ):
        raise AllCasesPipelineError("selected symbolic detail does not reproduce its aggregate")
    definition = {
        "evaluation_artifact_sha256": expected_evaluation.artifact_sha256,
        "recipe": first.recipe.as_dict(),
        "rows": [row.as_dict() for row in rows],
        "schema": first.as_dict()["schema"],
        "scope_key": first.scope_key,
        "selection_rank": first.selection_rank,
        "world": first.world,
    }
    return symbolic.SelectedStrategyDetailedOutcome(
        first.selection_rank,
        first.scope_key,
        first.world,
        first.recipe,
        expected_evaluation.artifact_sha256,
        rows,
        _sha256(definition),
    )


def _stream_selected_symbolic_details(
    project_root: Path,
    state: _SearchFeatureState,
    requests: Sequence[object],
    expected_evaluations: Mapping[str, object],
) -> tuple[object, ...]:
    """Replay a bounded ranked family span-by-span and bind it to merged aggregates."""

    from . import symbolic

    values = tuple(requests)
    if not values:
        return ()
    if len(values) > 24:
        raise AllCasesPipelineError("one symbolic detail replay exceeds 24 requests")
    by_coordinate: dict[tuple[str, str, int], list[object]] = {
        (item.scope_key, item.world, item.selection_rank): [] for item in values
    }
    for paths in _one_second_path_parts(project_root, state.plan):
        evaluator = symbolic.SharedPathEvaluator(paths)
        parts = symbolic.evaluate_selected_strategy_details(
            evaluator,
            values,
            reporting_group_by_date=state.plan.reporting_group_by_date,
            outer_validation_by_date=state.plan.search_block_by_date,
        )
        for item in parts:
            by_coordinate[item.scope_key, item.world, item.selection_rank].append(item)
    output = []
    for request in values:
        expected = expected_evaluations.get(request.recipe.strategy_id)
        if expected is None:
            raise AllCasesPipelineError("symbolic detail lacks its compact evaluation")
        output.append(
            _merge_selected_strategy_detail_parts(
                symbolic,
                by_coordinate[request.scope_key, request.world, request.selection_rank],
                expected,
            )
        )
    return tuple(output)


def _symbolic_search_candidate_rows(
    project_root: Path,
    state: _SearchFeatureState,
    feature_plan: _StageBFeaturePlan,
    evidence: _StageBSearchEvidence,
) -> tuple[tuple[dict[str, object], ...], tuple[dict[str, object], ...]]:
    """Build bounded real detailed rows for symbolic finalists and diversity evidence."""

    from . import symbolic

    recipe_by_id = {recipe.strategy_id: recipe for recipe in feature_plan.recipes}
    evaluation_by_id = {
        item.recipe.strategy_id: item for item in evidence.representative_real_evaluations
    }
    gate_by_id = {item.strategy_id: item for item in evidence.gate_results}
    selected_ids = tuple(evidence.symbolic_selection.selected_strategy_ids)
    requests = []
    for rank, strategy_id in enumerate(selected_ids, start=1):
        recipe = recipe_by_id[strategy_id]
        requests.append(
            symbolic.SelectedStrategyDetailRequest(
                rank,
                "SEARCH_FINAL_SYMBOLIC",
                "REAL",
                recipe,
                feature_plan.masks_by_world["REAL"][recipe.anchor_policy_id],
                feature_plan.schedules_by_world["REAL"].get(recipe.anchor_policy_id),
            )
        )
    details = _stream_selected_symbolic_details(
        project_root,
        state,
        requests,
        evaluation_by_id,
    )
    decision_days = tuple(day.isoformat() for day in state.plan.decision_dates)
    candidates = []
    artifacts = []
    for detail in details:
        strategy_id = detail.recipe.strategy_id
        evaluation = evaluation_by_id[strategy_id]
        gate = gate_by_id[strategy_id]
        family = feature_plan.family_by_policy_id[detail.recipe.anchor_policy_id]
        daily = {day: 0 for day in decision_days}
        actions = []
        for row in detail.rows:
            if row.status != "FILLED" or row.outer_validation not in {
                "B3",
                "B4",
                "B5",
                "B6",
                "B7",
                "B8",
            }:
                continue
            row_id = _sha256(
                {
                    "anchor_key": list(row.anchor_key),
                    "schema": "systematic_fx.ai_all_cases_symbolic_action.v1",
                }
            )
            actions.append([row_id, row.direction])
            daily[row.source_date.isoformat()] += int(row.net_pnl_ticks or 0)
        candidate = {
            "candidate_id": strategy_id,
            "candidate_kind": "SYMBOLIC",
            "daily_net_ticks": daily,
            "family_key": family,
            "maximum_drawdown_ticks": evaluation.maximum_drawdown_ticks,
            "median_outer_ev_denominator": gate.median_outer_ev_denominator,
            "median_outer_ev_numerator": gate.median_outer_ev_numerator,
            "oof_actions": sorted(actions),
            "positive_outer_validation_count": gate.positive_outer_validation_count,
            "stress_net_ticks": evaluation.total_stress_net_ticks,
            "worst_outer_ev_denominator": gate.worst_outer_ev_denominator,
            "worst_outer_ev_numerator": gate.worst_outer_ev_numerator,
        }
        strategy_document = {
            "anchor_policy": feature_plan.policies_by_id[detail.recipe.anchor_policy_id].as_dict(),
            "candidate_id": strategy_id,
            "candidate_kind": "SYMBOLIC",
            "catalog_selection_rank": detail.recipe.strategy_rank,
            "detail": detail.as_dict(),
            "family_key": family,
            "recipe": detail.recipe.as_dict(),
            "search_evaluation_artifact_sha256": evaluation.artifact_sha256,
            "search_gate": gate.as_dict(),
            "schema": "systematic_fx.ai_all_cases_frozen_symbolic_strategy.v1",
        }
        candidates.append(candidate)
        artifacts.append(
            {
                "candidate_id": strategy_id,
                "candidate_kind": "SYMBOLIC",
                "family_key": family,
                "strategy_document": strategy_document,
                "strategy_sha256": _sha256(strategy_document),
            }
        )
    return tuple(candidates), tuple(artifacts)


def _crossfit_summary(value: object) -> dict[str, object]:
    """Keep bounded OOF fit/mask identity without copying every score row."""

    return {
        "artifact_sha256": value.artifact_sha256,
        "candidate_id": value.candidate_id,
        "fold_admission_threshold_hex": [
            float(item).hex() for item in value.fold_admission_thresholds
        ],
        "fold_base_strategy_ids": list(value.fold_base_strategy_ids),
        "fold_base_trigger_families": list(value.fold_base_trigger_families),
        "fold_entry_schedule_sha256s": list(value.fold_entry_schedule_sha256s),
        "fold_model_sha256": list(value.fold_model_sha256),
        "fold_opportunity_lattice_sha256s": list(value.fold_opportunity_lattice_sha256s),
        "fold_outcome_lineage_sha256s": list(value.fold_outcome_lineage_sha256s),
        "fold_outcome_values_sha256s": list(value.fold_outcome_values_sha256s),
        "fold_source_matrix_sha256s": list(value.fold_source_matrix_sha256s),
        "fold_symbolic_ranking_sha256": list(value.fold_symbolic_ranking_sha256),
        "null_world": value.null_world,
        "row_count": len(value.row_ids),
        "schema": "systematic_fx.ai_all_cases_ml_crossfit_summary.v1",
        "task_horizon_seconds": value.task_horizon_seconds,
        "task_timeframe_seconds": value.task_timeframe_seconds,
    }


def _validate_embedded_artifact(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or "artifact_sha256" not in value:
        raise AllCasesPipelineError(f"{label} is not a hashed artifact")
    identity = _require_sha(value["artifact_sha256"], label=f"{label} SHA")
    definition = {key: item for key, item in value.items() if key != "artifact_sha256"}
    if _sha256(definition) != identity:
        raise AllCasesPipelineError(f"{label} hash differs")
    return value


def _daily_ticks_mapping_from_serialized(
    value: object,
    *,
    label: str,
) -> dict[str, int]:
    if not isinstance(value, list):
        raise AllCasesPipelineError(f"{label} daily vector differs")
    output: dict[str, int] = {}
    prior: str | None = None
    for row in value:
        if (
            not isinstance(row, list)
            or len(row) != 2
            or not isinstance(row[0], str)
            or isinstance(row[1], bool)
            or not isinstance(row[1], int)
            or row[0] in output
            or (prior is not None and row[0] <= prior)
        ):
            raise AllCasesPipelineError(f"{label} daily row differs")
        try:
            if date.fromisoformat(row[0]).isoformat() != row[0]:
                raise ValueError
        except ValueError as error:
            raise AllCasesPipelineError(f"{label} daily date differs") from error
        output[row[0]] = row[1]
        prior = row[0]
    if not output:
        raise AllCasesPipelineError(f"{label} daily vector is empty")
    return output


def _direct_selection_row_from_documents(
    evaluation: Mapping[str, object], gate: Mapping[str, object]
) -> dict[str, object]:
    _validate_embedded_artifact(evaluation, label="direct Search evaluation")
    _validate_embedded_artifact(gate, label="direct Search gate")
    if (
        gate.get("eligible") is not True
        or gate.get("candidate_id") != evaluation.get("candidate_id")
        or gate.get("family_key") != evaluation.get("family_key")
        or evaluation.get("candidate_kind") != "DIRECT"
        or not isinstance(evaluation.get("action_identities"), list)
    ):
        raise AllCasesPipelineError("eligible direct Search evidence differs")
    return {
        "candidate_id": gate["candidate_id"],
        "candidate_kind": "DIRECT_ML",
        "daily_net_ticks": _daily_ticks_mapping_from_serialized(
            evaluation.get("daily_net_ticks"), label="direct Search evaluation"
        ),
        "family_key": gate["family_key"],
        "maximum_drawdown_ticks": evaluation["maximum_drawdown_ticks"],
        "median_outer_ev_denominator": gate["median_outer_ev_denominator"],
        "median_outer_ev_numerator": gate["median_outer_ev_numerator"],
        "oof_actions": evaluation["action_identities"],
        "positive_outer_validation_count": gate["positive_outer_validation_count"],
        "stress_net_ticks": evaluation["total_stress_net_ticks"],
        "worst_outer_ev_denominator": gate["worst_outer_ev_denominator"],
        "worst_outer_ev_numerator": gate["worst_outer_ev_numerator"],
    }


def _ml_search_controls_document(value: object) -> dict[str, object]:
    return {
        "alignment_proof_sha256": value.alignment_proof_sha256,
        "evaluations": [
            value.real.as_dict(),
            value.circular_target.as_dict(),
            value.matched_target.as_dict(),
        ],
        "schema": "systematic_fx.ai_all_cases_ml_search_controls.v1",
    }


def _direct_selection_row(evaluation: object, gate: object) -> dict[str, object]:
    return {
        "candidate_id": gate.candidate_id,
        "candidate_kind": "DIRECT_ML",
        "daily_net_ticks": dict(evaluation.daily_net_ticks),
        "family_key": gate.family_key,
        "maximum_drawdown_ticks": evaluation.maximum_drawdown_ticks,
        "median_outer_ev_denominator": gate.median_outer_ev_denominator,
        "median_outer_ev_numerator": gate.median_outer_ev_numerator,
        "oof_actions": [list(item) for item in evaluation.action_identities],
        "positive_outer_validation_count": gate.positive_outer_validation_count,
        "stress_net_ticks": evaluation.total_stress_net_ticks,
        "worst_outer_ev_denominator": gate.worst_outer_ev_denominator,
        "worst_outer_ev_numerator": gate.worst_outer_ev_numerator,
    }


def _ensure_direct_ml_search(
    project_root: Path,
    config: AllCasesConfig,
    ledger: _SearchSubledger,
    state: _SearchFeatureState,
    universe: Mapping[str, object],
    *,
    verify_only: bool,
) -> _DirectSearchEvidence:
    """Run all 288 fixed direct candidates in 24 immutable chunks."""

    from . import ml

    catalog = ml.build_direct_candidate_catalog()
    ranges = _balanced_chunk_ranges(len(catalog), 24)
    counts = _search_phase_counts(config)
    if counts["DIRECT_ML_CHUNKS"] != len(ranges):
        raise AllCasesPipelineError("direct ML chunk plan differs")
    features = _direct_feature_bundles(state)
    feature_sha256, _summaries = _direct_feature_universe_commitment(features)
    if feature_sha256 != universe.get("direct_feature_universe_sha256"):
        raise AllCasesPipelineError("direct ML features differ from the universe barrier")
    outcomes: Mapping[tuple[int, int], _DirectOutcomeBundle] | None = None
    search_plan = ml.build_search_block_plan(state.plan.decision_dates)
    matrix_cache: dict[tuple[int, str, int], object] = {}
    rows_by_rank: dict[int, Mapping[str, object]] = {}
    selection_by_rank: dict[int, Mapping[str, object]] = {}
    artifacts_by_id: dict[str, Mapping[str, object]] = {}
    fit_cache_evidence_by_chunk: dict[int, object] = {}

    def matrix_for(candidate: object) -> object:
        nonlocal outcomes
        key = (
            candidate.decision_timeframe_seconds,
            candidate.feature_set_id,
            candidate.horizon_seconds,
        )
        cached = matrix_cache.get(key)
        if cached is not None:
            return cached
        if outcomes is None:
            outcomes = _direct_outcome_bundles(project_root, state, features)
        feature_bundle = features[candidate.decision_timeframe_seconds]
        response = outcomes[candidate.decision_timeframe_seconds, candidate.horizon_seconds]
        matrix = ml.build_direct_training_matrix(
            candidate,
            feature_bundle.feature_rows,
            fill_ns=response.fill_ns,
            label_exit_ns=response.label_exit_ns,
            entry_ticks=response.entry_ticks,
            terminal_ticks=response.terminal_ticks,
            outcome_contracts=response.outcome_contracts,
            outcome_span_ids=response.outcome_span_ids,
            segment_ids=response.segment_ids,
            valid_label_paths=response.valid_label_paths,
            outcome_lineage_sha256=response.outcome_lineage_sha256,
            opportunity_lattice_sha256=feature_bundle.opportunity_lattice_sha256,
        )
        matrix_cache[key] = matrix
        return matrix

    def candidate_document(
        candidate: object,
        fit_cache: object,
    ) -> Mapping[str, object]:
        try:
            matrix = matrix_for(candidate)
            feasibility = ml.probe_null_world_feasibility(
                matrix,
                search_plan,
                candidate_id=candidate.candidate_id,
            )
            crossfits = tuple(
                ml.cross_fit_direct_candidate(
                    candidate,
                    matrix,
                    search_plan,
                    world=world,
                    cache=fit_cache,
                )
                for world in ml.NULL_WORLD_ORDER
            )
            aligned = ml.align_cross_fitted_null_controls(*crossfits)
            evaluated = ml.evaluate_aligned_ml_search_controls(
                aligned,
                reporting_group_by_date=state.plan.reporting_group_by_date,
                direct_matrix=matrix,
            )
            gate = ml.apply_ml_search_gates(evaluated)
            final_models = []
            final_permutations = []
            indexes = tuple(range(matrix.row_count))
            for world in ml.NULL_WORLD_ORDER:
                permutation = ml.target_permutation_plan(
                    matrix,
                    indexes,
                    world=world,
                    candidate_id=candidate.candidate_id,
                    fold_key="SEARCH_FINAL",
                )
                targets = (
                    None
                    if world == "REAL"
                    else ml.permuted_training_targets(
                        matrix,
                        indexes,
                        world=world,
                        candidate_id=candidate.candidate_id,
                        fold_key="SEARCH_FINAL",
                    )
                )
                model = ml.fit_direct_model(
                    candidate,
                    matrix,
                    world=world,
                    fold_key="SEARCH_FINAL",
                    training_targets=targets,
                    cache=fit_cache,
                )
                final_permutations.append(permutation.as_dict())
                final_models.append(model)
            family_key = ml.ml_candidate_family_key(candidate)
            model_document = {
                "candidate": candidate.as_dict(),
                "candidate_id": candidate.candidate_id,
                "candidate_kind": "DIRECT_ML",
                "control_alignment": aligned.proof.as_dict(),
                "family_key": family_key,
                "final_models": [item.as_dict() for item in final_models],
                "final_permutation_plans": final_permutations,
                "gate": gate.as_dict(),
                "null_feasibility": feasibility,
                "opportunity_lattice_sha256": matrix.opportunity_lattice_sha256,
                "schema": "systematic_fx.ai_all_cases_frozen_direct_model.v1",
                "search_controls": _ml_search_controls_document(evaluated),
                "training_rows_sha256": ml.training_rows_sha256(matrix),
            }
            if gate.eligible:
                selection_by_rank[candidate.selection_rank] = _direct_selection_row(
                    evaluated.real, gate
                )
                frozen_model_artifact = {
                    "candidate_id": candidate.candidate_id,
                    "candidate_kind": "DIRECT_ML",
                    "family_key": family_key,
                    "model_document": model_document,
                    "model_sha256": _sha256(model_document),
                }
                artifacts_by_id[candidate.candidate_id] = frozen_model_artifact
            else:
                frozen_model_artifact = None
            return {
                "candidate": candidate.as_dict(),
                "control_alignment": aligned.proof.as_dict(),
                "crossfit_summaries": [_crossfit_summary(item) for item in crossfits],
                "final_model_sha256_by_world": [item.sha256 for item in final_models],
                "frozen_model_artifact": frozen_model_artifact,
                "gate": gate.as_dict(),
                "ineligibility": None,
                "null_feasibility": feasibility,
                "schema": "systematic_fx.ai_all_cases_direct_search_candidate.v1",
                "search_controls": _ml_search_controls_document(evaluated),
                "training_rows_sha256": ml.training_rows_sha256(matrix),
            }
        except ml.MLCandidateIneligible as error:
            return {
                "candidate": candidate.as_dict(),
                "control_alignment": None,
                "crossfit_summaries": [],
                "final_model_sha256_by_world": [],
                "frozen_model_artifact": None,
                "gate": None,
                "ineligibility": error.as_dict(),
                "null_feasibility": None,
                "schema": "systematic_fx.ai_all_cases_direct_search_candidate.v1",
                "search_controls": None,
                "training_rows_sha256": None,
            }

    def chunk_builder(index: int) -> Mapping[str, object]:
        fit_cache = ml.SharedFitCache()
        start, end = ranges[index]
        chunk_rows = []
        for candidate in catalog[start:end]:
            row = candidate_document(candidate, fit_cache)
            rows_by_rank[candidate.selection_rank] = row
            chunk_rows.append(row)
        fit_cache.discard_unconsumed_states()
        cache_evidence = fit_cache.terminal_evidence(maximum_fit_count=126)
        fit_cache_evidence_by_chunk[index] = cache_evidence
        return {
            "candidate_count": len(chunk_rows),
            "candidates": chunk_rows,
            "fit_cache_evidence": cache_evidence.as_dict(),
            "first_catalog_rank": catalog[start].selection_rank,
            "last_catalog_rank": catalog[end - 1].selection_rank,
            "schema": "systematic_fx.ai_all_cases_direct_search_chunk.v1",
        }

    def resume_chunk(index: int, payload: Mapping[str, object]) -> None:
        start, end = ranges[index]
        expected = catalog[start:end]
        rows = payload.get("candidates")
        if (
            type(payload.get("candidate_count")) is not int
            or type(payload.get("first_catalog_rank")) is not int
            or type(payload.get("last_catalog_rank")) is not int
            or payload.get("candidate_count") != len(expected)
            or payload.get("first_catalog_rank") != expected[0].selection_rank
            or payload.get("last_catalog_rank") != expected[-1].selection_rank
            or not isinstance(rows, list)
            or len(rows) != len(expected)
        ):
            raise AllCasesPipelineError("persisted direct ML chunk bounds differ")
        cache_document = payload.get("fit_cache_evidence")
        if not isinstance(cache_document, Mapping):
            raise AllCasesPipelineError("persisted direct fit-cache evidence differs")
        try:
            cache_evidence = ml.SharedFitCacheEvidence.from_dict(cache_document)
        except ml.AllCasesMLError as error:
            raise AllCasesPipelineError("persisted direct fit-cache evidence differs") from error
        if cache_evidence.maximum_fit_count != 126:
            raise AllCasesPipelineError("persisted direct fit-cache cap differs")
        fit_cache_evidence_by_chunk[index] = cache_evidence
        for candidate, raw in zip(expected, rows, strict=True):
            if not isinstance(raw, Mapping) or _canonical_json_bytes(
                raw.get("candidate")
            ) != _canonical_json_bytes(candidate.as_dict()):
                raise AllCasesPipelineError("persisted direct candidate identity differs")
            rows_by_rank[candidate.selection_rank] = raw
            frozen = raw.get("frozen_model_artifact")
            if frozen is None:
                continue
            if not isinstance(frozen, Mapping) or set(frozen) != {
                "candidate_id",
                "candidate_kind",
                "family_key",
                "model_document",
                "model_sha256",
            }:
                raise AllCasesPipelineError("persisted direct model artifact differs")
            model_document = frozen["model_document"]
            if (
                frozen["candidate_id"] != candidate.candidate_id
                or frozen["candidate_kind"] != "DIRECT_ML"
                or not isinstance(model_document, Mapping)
                or _sha256(model_document) != frozen["model_sha256"]
            ):
                raise AllCasesPipelineError("persisted direct model hash differs")
            model_rows = model_document.get("final_models")
            if not isinstance(model_rows, list) or len(model_rows) != 3:
                raise AllCasesPipelineError("persisted direct final-model family differs")
            reopened = tuple(
                ml.CanonicalMLModel.from_canonical_bytes(_canonical_json_bytes(model_row))
                for model_row in model_rows
            )
            if tuple(item.null_world for item in reopened) != ml.NULL_WORLD_ORDER or any(
                item.candidate_id != candidate.candidate_id for item in reopened
            ):
                raise AllCasesPipelineError("persisted direct final-model binding differs")
            gate = raw.get("gate")
            controls = raw.get("search_controls")
            if (
                not isinstance(gate, Mapping)
                or gate.get("eligible") is not True
                or not isinstance(controls, Mapping)
                or not isinstance(controls.get("evaluations"), list)
                or len(controls["evaluations"]) != 3
                or not isinstance(controls["evaluations"][0], Mapping)
            ):
                raise AllCasesPipelineError("persisted eligible direct evidence differs")
            selection_by_rank[candidate.selection_rank] = _direct_selection_row_from_documents(
                controls["evaluations"][0], gate
            )
            artifacts_by_id[candidate.candidate_id] = frozen

    ledger.ensure_phase(
        "DIRECT_ML_CHUNKS",
        counts["DIRECT_ML_CHUNKS"],
        chunk_builder,
        verify_only=verify_only,
        resume_consumer=resume_chunk,
    )
    if tuple(rows_by_rank) != tuple(range(1, len(catalog) + 1)):
        raise AllCasesPipelineError("direct ML chunks omit a catalog candidate")
    if tuple(fit_cache_evidence_by_chunk) != tuple(range(len(ranges))):
        raise AllCasesPipelineError("direct ML fit-cache chunks are incomplete")
    try:
        cache_aggregate = ml.aggregate_shared_fit_cache_evidence(
            "DIRECT",
            tuple(fit_cache_evidence_by_chunk[index] for index in range(len(ranges))),
        )
    except ml.AllCasesMLError as error:
        raise AllCasesPipelineError("direct ML fit-cache aggregate differs") from error
    cache_aggregate_document = cache_aggregate.as_dict()
    return _DirectSearchEvidence(
        tuple(rows_by_rank[index] for index in range(1, len(catalog) + 1)),
        tuple(selection_by_rank[index] for index in sorted(selection_by_rank)),
        artifacts_by_id,
        cache_aggregate_document,
        int(cache_aggregate_document["fit_count"]),
    )


def _candidate_ids_from_search_rows(
    rows: Sequence[Mapping[str, object]],
    expected_catalog: Sequence[object],
    *,
    label: str,
) -> tuple[str, ...]:
    if len(rows) != len(expected_catalog):
        raise AllCasesPipelineError(f"{label} candidate family size differs")
    output = []
    for row, expected in zip(rows, expected_catalog, strict=True):
        candidate = row.get("candidate")
        expected_document = expected.as_dict()
        if not isinstance(candidate, Mapping) or dict(candidate) != expected_document:
            raise AllCasesPipelineError(f"{label} candidate catalog order differs")
        output.append(_require_sha(candidate.get("candidate_id"), label=f"{label} ID"))
    if len(set(output)) != len(output):
        raise AllCasesPipelineError(f"{label} candidate family is duplicated")
    return tuple(output)


def _ensure_meta_feature_plan(
    config: AllCasesConfig,
    ledger: _SearchSubledger,
    state: _SearchFeatureState,
    feature_plan: _StageBFeaturePlan,
    stage_b_evidence: _StageBSearchEvidence,
    symbolic_selection_rows: Sequence[Mapping[str, object]],
    symbolic_artifacts: Sequence[Mapping[str, object]],
    *,
    verify_only: bool,
) -> _MetaFeaturePlan:
    """Freeze typed world/fold rank-slot certificates before any meta fitting."""

    from . import ml

    world_names = {
        "REAL": "REAL",
        "CIRCULAR": "CIRCULAR_TARGET",
        "MATCHED": "MATCHED_TARGET",
    }
    recipe_by_id = {recipe.strategy_id: recipe for recipe in feature_plan.recipes}
    search_plan = ml.build_search_block_plan(state.plan.decision_dates)
    training_dates_by_scope = {
        fold.fold_key: tuple(fold.training_dates) for fold in search_plan.outer_folds
    }
    training_dates_by_scope["SEARCH_FINAL"] = tuple(state.plan.decision_dates)
    certificates: dict[str, dict[str, object]] = {world: {} for world in ml.NULL_WORLD_ORDER}
    for symbolic_world, ml_world in world_names.items():
        rankings = stage_b_evidence.top24_by_world_and_scope.get(symbolic_world)
        if not isinstance(rankings, Mapping) or set(rankings) != set(training_dates_by_scope):
            raise AllCasesPipelineError("symbolic meta ranking scopes differ")
        for scope, training_dates in training_dates_by_scope.items():
            ranking = rankings[scope]
            if ranking.scope_key != scope:
                raise AllCasesPipelineError("symbolic meta ranking scope identity differs")
            ranked = []
            for rank, strategy_id in enumerate(ranking.selected_strategy_ids, start=1):
                recipe = recipe_by_id.get(strategy_id)
                if recipe is None:
                    raise AllCasesPipelineError(
                        "symbolic meta ranking escapes the frozen Stage-B family"
                    )
                family = feature_plan.family_by_policy_id.get(recipe.anchor_policy_id)
                policy = feature_plan.policies_by_id.get(recipe.anchor_policy_id)
                if not isinstance(family, str) or not family:
                    raise AllCasesPipelineError("symbolic meta ranking lacks its trigger family")
                if policy is None:
                    raise AllCasesPipelineError("symbolic meta ranking lacks its anchor policy")
                ranked.append(
                    ml.RankedSymbolicStrategy(
                        rank,
                        strategy_id,
                        family,
                        recipe.anchor_policy_id,
                        policy.base_candidate_id,
                        policy.context_id,
                        policy.time_filter_id,
                        policy.delay_id,
                        recipe.entry_policy_id,
                        recipe.exit_policy_id,
                    )
                )
            certificate = ml.build_symbolic_ranking_certificate(
                null_world=ml_world,
                fold_key=scope,
                training_dates=training_dates,
                ranked_strategies=ranked,
            )
            certificates[ml_world][scope] = certificate
    plan_document = {
        "certificates_by_world_and_scope": {
            world: {
                scope: certificates[world][scope].as_dict()
                for scope in (*ml.SEARCH_OUTER_FOLD_KEYS, "SEARCH_FINAL")
            }
            for world in ml.NULL_WORLD_ORDER
        },
        "schema": "systematic_fx.ai_all_cases_meta_rank_slot_plan.v1",
        "source_symbolic_top24_sha256": _sha256(stage_b_evidence.top24_document),
        "symbolic_frozen_artifacts": [dict(item) for item in symbolic_artifacts],
        "symbolic_selection_rows": [dict(item) for item in symbolic_selection_rows],
    }
    counts = _search_phase_counts(config)

    def resume_plan(index: int, payload: Mapping[str, object]) -> None:
        if index != 0 or dict(payload) != plan_document:
            raise AllCasesPipelineError(
                "persisted meta rank-slot plan differs on prior-prefix replay"
            )
        raw_worlds = payload.get("certificates_by_world_and_scope")
        if not isinstance(raw_worlds, Mapping):
            raise AllCasesPipelineError("persisted meta rank-slot worlds differ")
        for world, by_scope in certificates.items():
            raw_scopes = raw_worlds.get(world)
            if not isinstance(raw_scopes, Mapping):
                raise AllCasesPipelineError("persisted meta rank-slot world differs")
            for scope, expected in by_scope.items():
                reopened = ml.SymbolicRankingCertificate.from_dict(raw_scopes.get(scope))
                if reopened != expected:
                    raise AllCasesPipelineError(
                        "persisted meta rank-slot certificate did not round trip"
                    )

    ledger.ensure_phase(
        "META_PLAN_FROZEN",
        counts["META_PLAN_FROZEN"],
        lambda index: (
            plan_document
            if index == 0
            else (_ for _ in ()).throw(
                AllCasesPipelineError("meta rank-slot plan is a single barrier")
            )
        ),
        verify_only=verify_only,
        resume_consumer=resume_plan,
    )
    return _MetaFeaturePlan(certificates, plan_document)


def _stage_b_recipe_components(
    feature_plan: _StageBFeaturePlan,
    recipe: object,
) -> tuple[object, object, object, object, object]:
    """Resolve a frozen recipe to its exact catalog objects for Expert-8."""

    from . import symbolic

    policy = feature_plan.policies_by_id.get(recipe.anchor_policy_id)
    if policy is None:
        raise AllCasesPipelineError("complete strategy lacks its frozen anchor policy")
    candidates = {
        item.candidate_id: item for item in symbolic.build_base_event_catalog().candidates
    }
    contexts = {item.context_id: item for item in symbolic.build_context_catalog()}
    entries = {item.entry_id: item for item in symbolic.build_entry_catalog().candidates}
    exits = {item.exit_id: item for item in symbolic.build_exit_catalog().candidates}
    candidate = candidates.get(policy.base_candidate_id)
    context = contexts.get(policy.context_id)
    entry = entries.get(recipe.entry_policy_id)
    exit_policy = exits.get(recipe.exit_policy_id)
    if any(item is None for item in (candidate, context, entry, exit_policy)):
        raise AllCasesPipelineError("complete strategy catalog binding differs")
    return candidate, context, policy, entry, exit_policy


def _stream_meta_base_details(
    project_root: Path,
    state: _SearchFeatureState,
    feature_plan: _StageBFeaturePlan,
    stage_b_evidence: _StageBSearchEvidence,
    meta_plan: _MetaFeaturePlan,
) -> Mapping[str, Mapping[str, object]]:
    """Replay every unique rank-slot base in one shared 1s stream, batched by 24."""

    from . import symbolic

    symbolic_world_by_ml = {
        "REAL": "REAL",
        "CIRCULAR_TARGET": "CIRCULAR",
        "MATCHED_TARGET": "MATCHED",
    }
    recipe_by_id = {recipe.strategy_id: recipe for recipe in feature_plan.recipes}
    requests: list[tuple[str, str, object]] = []
    for ml_world, symbolic_world in symbolic_world_by_ml.items():
        strategy_ids = {
            item.strategy_id
            for certificate in meta_plan.certificates_by_world_and_scope[ml_world].values()
            for item in certificate.ranked_strategies
        }
        ordered_ids = tuple(
            sorted(strategy_ids, key=lambda strategy_id: recipe_by_id[strategy_id].strategy_rank)
        )
        for batch_index, start in enumerate(range(0, len(ordered_ids), 24)):
            for local_rank, strategy_id in enumerate(ordered_ids[start : start + 24], start=1):
                recipe = recipe_by_id[strategy_id]
                mask = feature_plan.masks_by_world[symbolic_world].get(recipe.anchor_policy_id)
                if (
                    mask is None
                    or strategy_id not in stage_b_evidence.evaluations_by_world[symbolic_world]
                ):
                    raise AllCasesPipelineError(
                        "meta base ranking lacks its frozen mask/evaluation"
                    )
                request = symbolic.SelectedStrategyDetailRequest(
                    local_rank,
                    f"META_BASE_DETAIL_{ml_world}_{batch_index:03d}",
                    symbolic_world,
                    recipe,
                    mask,
                    feature_plan.schedules_by_world[symbolic_world].get(recipe.anchor_policy_id),
                )
                requests.append((ml_world, strategy_id, request))
    output: dict[str, dict[str, object]] = {world: {} for world in symbolic_world_by_ml}
    if not requests:
        return output
    batches = tuple(
        tuple(item[2] for item in requests[start : start + 24])
        for start in range(0, len(requests), 24)
    )
    parts_by_coordinate: dict[tuple[str, str, int], list[object]] = {
        (request.scope_key, request.world, request.selection_rank): [] for _, _, request in requests
    }
    for paths in _one_second_path_parts(project_root, state.plan):
        evaluator = symbolic.SharedPathEvaluator(paths)
        for batch in batches:
            details = symbolic.evaluate_selected_strategy_details(
                evaluator,
                batch,
                reporting_group_by_date=state.plan.reporting_group_by_date,
                outer_validation_by_date=state.plan.search_block_by_date,
            )
            for detail in details:
                parts_by_coordinate[detail.scope_key, detail.world, detail.selection_rank].append(
                    detail
                )
    for ml_world, strategy_id, request in requests:
        symbolic_world = symbolic_world_by_ml[ml_world]
        expected = stage_b_evidence.evaluations_by_world[symbolic_world][strategy_id]
        detail = _merge_selected_strategy_detail_parts(
            symbolic,
            parts_by_coordinate[request.scope_key, request.world, request.selection_rank],
            expected,
        )
        output[ml_world][strategy_id] = detail
    return output


def _meta_base_bundle(
    state: _SearchFeatureState,
    feature_plan: _StageBFeaturePlan,
    recipe: object,
    mask: object,
    detail: object,
    bars_by_timeframe: Mapping[int, object],
) -> _MetaBaseBundle:
    """Adapt one fully evaluated base strategy to formula-bound causal meta rows."""

    import numpy as np

    from . import ml, symbolic

    candidate, context, policy, entry, exit_policy = _stage_b_recipe_components(
        feature_plan, recipe
    )
    if detail.recipe != recipe or mask.policy.policy_id != recipe.anchor_policy_id:
        raise AllCasesPipelineError("meta base detail differs from its frozen recipe")
    order_batch = symbolic.freeze_entry_orders(mask, (entry,))
    order_by_anchor = {item.anchor.outcome_key: item for item in order_batch.orders}
    anchor_by_key = {item.outcome_key: item for item in mask.records}
    filled_rows = tuple(item for item in detail.rows if item.status == "FILLED")
    if len(filled_rows) < 2:
        raise ml.MLCandidateIneligible(
            ml.MLIneligibilityReason.INSUFFICIENT_FOLD_ROWS,
            "meta base strategy has fewer than two filled causal rows",
            scope_key=detail.scope_key,
        )
    anchors = []
    orders = []
    experts = []
    for row in filled_rows:
        anchor = anchor_by_key.get(row.anchor_key)
        order = order_by_anchor.get(row.anchor_key)
        if (
            anchor is None
            or order is None
            or row.censored
            or row.entry_ns is None
            or row.exit_ns is None
            or row.net_pnl_ticks is None
            or row.entry_fill_ticks is None
            or row.exit_fill_ticks is None
            or row.entry_ns % 1_000_000_000 != 0
            or row.exit_ns <= row.entry_ns
        ):
            raise AllCasesPipelineError("meta filled base row is incomplete")
        anchors.append(anchor)
        orders.append(order)
        experts.append(
            symbolic.build_causal_expert_feature_artifact(
                candidate,
                context,
                policy,
                anchor,
                order,
                exit_policy,
            )
        )
    entry_schedule_sha256 = _sha256(
        {
            "detail_artifact_sha256": detail.artifact_sha256,
            "entry_order_batch_sha256": order_batch.artifact_sha256,
            "rows": [
                {
                    "actual_entry_ns": row.entry_ns,
                    "order_id": order.order_id,
                }
                for row, order in zip(filled_rows, orders, strict=True)
            ],
            "schema": "systematic_fx.ai_all_cases_meta_search_entry_schedule.v1",
        }
    )
    stage_rank = {source_date: index for index, source_date in enumerate(state.plan.decision_dates)}
    anchor_rows = ml.CausalAnchorRows(
        row_ids=tuple(order.order_id for order in orders),
        decision_ns=np.asarray([anchor.anchor_ns for anchor in anchors], dtype=np.int64),
        entry_ns=np.asarray([row.entry_ns for row in filled_rows], dtype=np.int64),
        source_dates=tuple(anchor.source_date for anchor in anchors),
        contracts=tuple(anchor.contract for anchor in anchors),
        outcome_span_ids=np.asarray([anchor.outcome_span_id for anchor in anchors], dtype=np.int64),
        segment_ids=np.asarray([anchor.segment_id for anchor in anchors], dtype=np.uint64),
        stage_date_ranks=np.asarray(
            [stage_rank[anchor.source_date] for anchor in anchors], dtype=np.int64
        ),
        stage_key=state.plan.stage_key,
        decision_timeframe_seconds=candidate.trigger_timeframe_seconds,
        entry_schedule_sha256=entry_schedule_sha256,
    )
    try:
        feature_rows = ml.build_causal_feature_rows(
            anchors=anchor_rows,
            bars_by_timeframe=bars_by_timeframe,
            feature_set_id="FULL_MTF_PLUS_EXPERT_221",
            expert_artifacts=experts,
        )
    except ml.AllCasesMLError as error:
        if "fewer than two anchors have complete causal history" not in str(error):
            raise
        raise ml.MLCandidateIneligible(
            ml.MLIneligibilityReason.INSUFFICIENT_FOLD_ROWS,
            "meta base strategy has fewer than two complete causal histories",
            scope_key=detail.scope_key,
        ) from error
    retained = tuple(int(index) for index in feature_rows.retained_input_indexes)
    selected_rows = tuple(filled_rows[index] for index in retained)
    selected_anchors = tuple(anchors[index] for index in retained)
    selected_orders = tuple(orders[index] for index in retained)
    selected_experts = tuple(experts[index] for index in retained)
    retained_mask = symbolic.PolicyMask.from_records(
        mask.policy,
        mask.family,
        mask.direction,
        selected_anchors,
    )
    retained_order_batch = symbolic.freeze_entry_orders(retained_mask, (entry,))
    if tuple(feature_rows.row_ids) != tuple(item.order_id for item in selected_orders) or tuple(
        item.order_id for item in retained_order_batch.orders
    ) != tuple(item.order_id for item in selected_orders):
        raise AllCasesPipelineError("meta feature rows lost their symbolic order identity")
    outcome_lineage_sha256 = _sha256(
        {
            "detail_artifact_sha256": detail.artifact_sha256,
            "expert_artifact_sha256s": [item.artifact_sha256 for item in selected_experts],
            "row_ids": list(feature_rows.row_ids),
            "schema": "systematic_fx.ai_all_cases_meta_search_outcome_lineage.v1",
        }
    )
    expert_commitment = feature_rows.expert_artifact_commitment_sha256
    if not isinstance(expert_commitment, str):
        raise AllCasesPipelineError("meta causal features lack Expert-8 commitment")
    count = feature_rows.row_count
    return _MetaBaseBundle(
        feature_rows,
        np.asarray([row.entry_ns for row in selected_rows], dtype=np.int64),
        np.asarray([row.net_pnl_ticks for row in selected_rows], dtype=np.int64),
        np.asarray(
            [1 if anchor.direction == "LONG" else -1 for anchor in selected_anchors],
            dtype=np.int8,
        ),
        np.asarray(
            [anchor.atr_sum_ticks / anchor.atr_denominator for anchor in selected_anchors],
            dtype=np.float64,
        ),
        np.asarray([row.exit_ns for row in selected_rows], dtype=np.int64),
        tuple(anchor.contract for anchor in selected_anchors),
        np.asarray([anchor.outcome_span_id for anchor in selected_anchors], dtype=np.int64),
        np.asarray([anchor.segment_id for anchor in selected_anchors], dtype=np.uint64),
        np.ones(count, dtype=np.bool_),
        outcome_lineage_sha256,
        state.structural_lattice.artifact_sha256,
        expert_commitment,
        retained_order_batch.artifact_sha256,
        selected_experts,
        retained_order_batch,
        recipe,
    )


def _meta_training_matrix(
    candidate: object,
    certificate: object,
    bundle: _MetaBaseBundle,
) -> object:
    from . import ml

    return ml.build_meta_training_matrix(
        candidate,
        bundle.feature_rows,
        base_row_indexes=tuple(range(bundle.feature_rows.row_count)),
        base_entry_ns=bundle.base_entry_ns,
        fully_loaded_net_ticks=bundle.fully_loaded_net_ticks,
        base_directions=bundle.base_directions,
        atr_ticks=bundle.atr_ticks,
        label_exit_ns=bundle.label_exit_ns,
        outcome_contracts=bundle.outcome_contracts,
        outcome_span_ids=bundle.outcome_span_ids,
        segment_ids=bundle.segment_ids,
        valid_label_paths=bundle.valid_label_paths,
        outcome_lineage_sha256=bundle.outcome_lineage_sha256,
        opportunity_lattice_sha256=bundle.opportunity_lattice_sha256,
        symbolic_ranking_certificate=certificate,
        strategy_recipe=bundle.strategy_recipe,
        base_order_batch=bundle.entry_order_batch,
        expert_artifacts=bundle.expert_artifacts,
    )


def _meta_selection_row(evaluation: object, gate: object) -> dict[str, object]:
    return {
        "candidate_id": gate.candidate_id,
        "candidate_kind": "META_ML",
        "daily_net_ticks": dict(evaluation.daily_net_ticks),
        "family_key": gate.family_key,
        "maximum_drawdown_ticks": evaluation.maximum_drawdown_ticks,
        "median_outer_ev_denominator": gate.median_outer_ev_denominator,
        "median_outer_ev_numerator": gate.median_outer_ev_numerator,
        "oof_actions": [list(item) for item in evaluation.action_identities],
        "positive_outer_validation_count": gate.positive_outer_validation_count,
        "stress_net_ticks": evaluation.total_stress_net_ticks,
        "worst_outer_ev_denominator": gate.worst_outer_ev_denominator,
        "worst_outer_ev_numerator": gate.worst_outer_ev_numerator,
    }


def _meta_selection_row_from_documents(
    evaluation: Mapping[str, object], gate: Mapping[str, object]
) -> dict[str, object]:
    _validate_embedded_artifact(evaluation, label="meta Search evaluation")
    _validate_embedded_artifact(gate, label="meta Search gate")
    if (
        gate.get("eligible") is not True
        or gate.get("candidate_id") != evaluation.get("candidate_id")
        or gate.get("family_key") != evaluation.get("family_key")
        or evaluation.get("candidate_kind") != "META"
        or not isinstance(evaluation.get("action_identities"), list)
    ):
        raise AllCasesPipelineError("eligible meta Search evidence differs")
    return {
        "candidate_id": gate["candidate_id"],
        "candidate_kind": "META_ML",
        "daily_net_ticks": _daily_ticks_mapping_from_serialized(
            evaluation.get("daily_net_ticks"), label="meta Search evaluation"
        ),
        "family_key": gate["family_key"],
        "maximum_drawdown_ticks": evaluation["maximum_drawdown_ticks"],
        "median_outer_ev_denominator": gate["median_outer_ev_denominator"],
        "median_outer_ev_numerator": gate["median_outer_ev_numerator"],
        "oof_actions": evaluation["action_identities"],
        "positive_outer_validation_count": gate["positive_outer_validation_count"],
        "stress_net_ticks": evaluation["total_stress_net_ticks"],
        "worst_outer_ev_denominator": gate["worst_outer_ev_denominator"],
        "worst_outer_ev_numerator": gate["worst_outer_ev_numerator"],
    }


def _ensure_meta_ml_search(
    project_root: Path,
    config: AllCasesConfig,
    ledger: _SearchSubledger,
    state: _SearchFeatureState,
    feature_plan: _StageBFeaturePlan,
    stage_b_evidence: _StageBSearchEvidence,
    meta_plan: _MetaFeaturePlan,
    *,
    verify_only: bool,
) -> _MetaSearchEvidence:
    """Run all 192 rank-slot gates in 24 fixed chunks with independent worlds."""

    from . import ml

    catalog = ml.build_meta_candidate_catalog()
    ranges = _balanced_chunk_ranges(len(catalog), 24)
    counts = _search_phase_counts(config)
    if counts["META_ML_CHUNKS"] != len(ranges):
        raise AllCasesPipelineError("meta ML chunk plan differs")
    search_plan = ml.build_search_block_plan(state.plan.decision_dates)
    bars_by_timeframe = _ml_bar_series(state)
    detail_cache: Mapping[str, Mapping[str, object]] | None = None
    rows_by_rank: dict[int, Mapping[str, object]] = {}
    selection_by_rank: dict[int, Mapping[str, object]] = {}
    artifacts_by_id: dict[str, Mapping[str, object]] = {}
    fit_cache_evidence_by_chunk: dict[int, object] = {}
    symbolic_world_by_ml = {
        "REAL": "REAL",
        "CIRCULAR_TARGET": "CIRCULAR",
        "MATCHED_TARGET": "MATCHED",
    }
    recipe_by_id = {recipe.strategy_id: recipe for recipe in feature_plan.recipes}
    matrix_family_cache: dict[
        tuple[int, str],
        tuple[
            Mapping[str, Mapping[str, object]],
            Mapping[str, object],
            tuple[str, ...],
            tuple[str, ...],
        ],
    ] = {}

    def details() -> Mapping[str, Mapping[str, object]]:
        nonlocal detail_cache
        if detail_cache is None:
            detail_cache = _stream_meta_base_details(
                project_root,
                state,
                feature_plan,
                stage_b_evidence,
                meta_plan,
            )
        return detail_cache

    def matrix_family(
        candidate: object,
    ) -> tuple[
        Mapping[str, Mapping[str, object]],
        Mapping[str, object],
        tuple[str, ...],
        tuple[str, ...],
    ]:
        cache_key = candidate.symbolic_rank_slot, candidate.feature_set_id
        cached = matrix_family_cache.get(cache_key)
        if cached is not None:
            return cached
        bundles: dict[tuple[str, str], _MetaBaseBundle] = {}
        fold_matrices: dict[str, dict[str, object]] = {world: {} for world in ml.NULL_WORLD_ORDER}
        final_matrices: dict[str, object] = {}
        expert_commitments: set[str] = set()
        order_batch_sha256s: set[str] = set()

        def matrix_for(world: str, scope: str) -> object:
            certificate = meta_plan.certificates_by_world_and_scope[world][scope]
            ranked = certificate.strategy_at_rank(candidate.symbolic_rank_slot)
            if ranked is None:
                raise ml.MLCandidateIneligible(
                    ml.MLIneligibilityReason.INSUFFICIENT_BASE_STRATEGY_RANK,
                    "the prior-prefix symbolic ranking lacks this meta rank slot",
                    candidate_id=candidate.candidate_id,
                    scope_key=scope,
                )
            recipe = recipe_by_id.get(ranked.strategy_id)
            symbolic_world = symbolic_world_by_ml[world]
            mask = (
                None
                if recipe is None
                else feature_plan.masks_by_world[symbolic_world].get(recipe.anchor_policy_id)
            )
            detail = details()[world].get(ranked.strategy_id)
            if recipe is None or mask is None or detail is None:
                raise AllCasesPipelineError("meta rank-slot certificate lacks frozen base evidence")
            key = world, ranked.strategy_id
            bundle = bundles.get(key)
            if bundle is None:
                bundle = _meta_base_bundle(
                    state,
                    feature_plan,
                    recipe,
                    mask,
                    detail,
                    bars_by_timeframe,
                )
                bundles[key] = bundle
            expert_commitments.add(bundle.expert_artifact_commitment_sha256)
            order_batch_sha256s.add(bundle.entry_order_batch_sha256)
            return _meta_training_matrix(candidate, certificate, bundle)

        for world in ml.NULL_WORLD_ORDER:
            for fold_key in ml.SEARCH_OUTER_FOLD_KEYS:
                fold_matrices[world][fold_key] = matrix_for(world, fold_key)
            final_matrices[world] = matrix_for(world, "SEARCH_FINAL")
        result = (
            fold_matrices,
            final_matrices,
            tuple(sorted(expert_commitments)),
            tuple(sorted(order_batch_sha256s)),
        )
        matrix_family_cache[cache_key] = result
        return result

    def candidate_document(
        candidate: object,
        fit_cache: object,
    ) -> Mapping[str, object]:
        try:
            matrices, final_matrices, expert_shas, order_batch_shas = matrix_family(candidate)
            feasibility = ml.probe_meta_crossfit_null_feasibility(candidate, matrices, search_plan)
            crossfits = tuple(
                ml.cross_fit_meta_candidate(
                    candidate,
                    matrices,
                    search_plan,
                    world=world,
                    cache=fit_cache,
                )
                for world in ml.NULL_WORLD_ORDER
            )
            aligned = ml.align_cross_fitted_null_controls(*crossfits)
            final_models = []
            final_permutations = []
            for world in ml.NULL_WORLD_ORDER:
                matrix = final_matrices[world]
                indexes = tuple(range(matrix.row_count))
                permutation = ml.target_permutation_plan(
                    matrix,
                    indexes,
                    world=world,
                    candidate_id=candidate.candidate_id,
                    fold_key="SEARCH_FINAL",
                )
                targets = (
                    None
                    if world == "REAL"
                    else ml.permuted_training_targets(
                        matrix,
                        indexes,
                        world=world,
                        candidate_id=candidate.candidate_id,
                        fold_key="SEARCH_FINAL",
                    )
                )
                final_models.append(
                    ml.fit_meta_model(
                        candidate,
                        matrix,
                        ranking_training_dates=state.plan.decision_dates,
                        world=world,
                        fold_key="SEARCH_FINAL",
                        training_targets=targets,
                        cache=fit_cache,
                    )
                )
                final_permutations.append(permutation.as_dict())
            evaluated = ml.evaluate_aligned_ml_search_controls(
                aligned,
                reporting_group_by_date=state.plan.reporting_group_by_date,
                meta_matrices_by_world_and_fold=matrices,
                meta_search_final_real_model=final_models[0],
            )
            gate = ml.apply_ml_search_gates(evaluated)
            training_rows = {
                world: {
                    **{
                        fold_key: ml.training_rows_sha256(matrices[world][fold_key])
                        for fold_key in ml.SEARCH_OUTER_FOLD_KEYS
                    },
                    "SEARCH_FINAL": ml.training_rows_sha256(final_matrices[world]),
                }
                for world in ml.NULL_WORLD_ORDER
            }
            model_document = {
                "candidate": candidate.as_dict(),
                "candidate_id": candidate.candidate_id,
                "candidate_kind": "META_ML",
                "control_alignment": aligned.proof.as_dict(),
                "expert_artifact_commitment_sha256s": list(expert_shas),
                "family_key": gate.family_key,
                "final_models": [item.as_dict() for item in final_models],
                "final_permutation_plans": final_permutations,
                "gate": gate.as_dict(),
                "meta_plan_sha256": _sha256(meta_plan.plan_document),
                "null_feasibility": feasibility,
                "symbolic_order_batch_sha256s": list(order_batch_shas),
                "schema": "systematic_fx.ai_all_cases_frozen_meta_model.v1",
                "search_controls": _ml_search_controls_document(evaluated),
                "training_rows_sha256_by_world_and_fold": training_rows,
            }
            if gate.eligible:
                selection_by_rank[candidate.selection_rank] = _meta_selection_row(
                    evaluated.real, gate
                )
                frozen_model_artifact = {
                    "candidate_id": candidate.candidate_id,
                    "candidate_kind": "META_ML",
                    "family_key": gate.family_key,
                    "model_document": model_document,
                    "model_sha256": _sha256(model_document),
                }
                artifacts_by_id[candidate.candidate_id] = frozen_model_artifact
            else:
                frozen_model_artifact = None
            return {
                "candidate": candidate.as_dict(),
                "control_alignment": aligned.proof.as_dict(),
                "crossfit_summaries": [_crossfit_summary(item) for item in crossfits],
                "final_model_sha256_by_world": [item.sha256 for item in final_models],
                "frozen_model_artifact": frozen_model_artifact,
                "gate": gate.as_dict(),
                "ineligibility": None,
                "null_feasibility": feasibility,
                "schema": "systematic_fx.ai_all_cases_meta_search_candidate.v1",
                "search_controls": _ml_search_controls_document(evaluated),
                "training_rows_sha256_by_world_and_fold": training_rows,
            }
        except ml.MLCandidateIneligible as error:
            if error.candidate_id is None:
                error.candidate_id = candidate.candidate_id
            return {
                "candidate": candidate.as_dict(),
                "control_alignment": None,
                "crossfit_summaries": [],
                "final_model_sha256_by_world": [],
                "frozen_model_artifact": None,
                "gate": None,
                "ineligibility": error.as_dict(),
                "null_feasibility": None,
                "schema": "systematic_fx.ai_all_cases_meta_search_candidate.v1",
                "search_controls": None,
                "training_rows_sha256_by_world_and_fold": {},
            }

    def chunk_builder(index: int) -> Mapping[str, object]:
        matrix_family_cache.clear()
        fit_cache = ml.SharedFitCache()
        start, end = ranges[index]
        chunk_rows = []
        for candidate in catalog[start:end]:
            row = candidate_document(candidate, fit_cache)
            rows_by_rank[candidate.selection_rank] = row
            chunk_rows.append(row)
        fit_cache.discard_unconsumed_states()
        cache_evidence = fit_cache.terminal_evidence(maximum_fit_count=84)
        fit_cache_evidence_by_chunk[index] = cache_evidence
        return {
            "candidate_count": len(chunk_rows),
            "candidates": chunk_rows,
            "fit_cache_evidence": cache_evidence.as_dict(),
            "first_catalog_rank": catalog[start].selection_rank,
            "last_catalog_rank": catalog[end - 1].selection_rank,
            "schema": "systematic_fx.ai_all_cases_meta_search_chunk.v1",
        }

    def resume_chunk(index: int, payload: Mapping[str, object]) -> None:
        start, end = ranges[index]
        expected = catalog[start:end]
        rows = payload.get("candidates")
        if (
            type(payload.get("candidate_count")) is not int
            or type(payload.get("first_catalog_rank")) is not int
            or type(payload.get("last_catalog_rank")) is not int
            or payload.get("candidate_count") != len(expected)
            or payload.get("first_catalog_rank") != expected[0].selection_rank
            or payload.get("last_catalog_rank") != expected[-1].selection_rank
            or not isinstance(rows, list)
            or len(rows) != len(expected)
        ):
            raise AllCasesPipelineError("persisted meta ML chunk bounds differ")
        cache_document = payload.get("fit_cache_evidence")
        if not isinstance(cache_document, Mapping):
            raise AllCasesPipelineError("persisted meta fit-cache evidence differs")
        try:
            cache_evidence = ml.SharedFitCacheEvidence.from_dict(cache_document)
        except ml.AllCasesMLError as error:
            raise AllCasesPipelineError("persisted meta fit-cache evidence differs") from error
        if cache_evidence.maximum_fit_count != 84:
            raise AllCasesPipelineError("persisted meta fit-cache cap differs")
        fit_cache_evidence_by_chunk[index] = cache_evidence
        for candidate, raw in zip(expected, rows, strict=True):
            if not isinstance(raw, Mapping) or _canonical_json_bytes(
                raw.get("candidate")
            ) != _canonical_json_bytes(candidate.as_dict()):
                raise AllCasesPipelineError("persisted meta candidate identity differs")
            rows_by_rank[candidate.selection_rank] = raw
            frozen = raw.get("frozen_model_artifact")
            if frozen is None:
                continue
            if not isinstance(frozen, Mapping) or set(frozen) != {
                "candidate_id",
                "candidate_kind",
                "family_key",
                "model_document",
                "model_sha256",
            }:
                raise AllCasesPipelineError("persisted meta model artifact differs")
            model_document = frozen["model_document"]
            if (
                frozen["candidate_id"] != candidate.candidate_id
                or frozen["candidate_kind"] != "META_ML"
                or not isinstance(model_document, Mapping)
                or _sha256(model_document) != frozen["model_sha256"]
            ):
                raise AllCasesPipelineError("persisted meta model hash differs")
            model_rows = model_document.get("final_models")
            if not isinstance(model_rows, list) or len(model_rows) != 3:
                raise AllCasesPipelineError("persisted meta final-model family differs")
            reopened = tuple(
                ml.CanonicalMLModel.from_canonical_bytes(_canonical_json_bytes(model_row))
                for model_row in model_rows
            )
            if tuple(item.null_world for item in reopened) != ml.NULL_WORLD_ORDER or any(
                item.candidate_id != candidate.candidate_id
                or item.symbolic_ranking_certificate is None
                for item in reopened
            ):
                raise AllCasesPipelineError("persisted meta final-model binding differs")
            gate = raw.get("gate")
            controls = raw.get("search_controls")
            if (
                not isinstance(gate, Mapping)
                or gate.get("eligible") is not True
                or not isinstance(controls, Mapping)
                or not isinstance(controls.get("evaluations"), list)
                or len(controls["evaluations"]) != 3
                or not isinstance(controls["evaluations"][0], Mapping)
            ):
                raise AllCasesPipelineError("persisted eligible meta evidence differs")
            selection_by_rank[candidate.selection_rank] = _meta_selection_row_from_documents(
                controls["evaluations"][0], gate
            )
            artifacts_by_id[candidate.candidate_id] = frozen

    ledger.ensure_phase(
        "META_ML_CHUNKS",
        counts["META_ML_CHUNKS"],
        chunk_builder,
        verify_only=verify_only,
        resume_consumer=resume_chunk,
    )
    if tuple(rows_by_rank) != tuple(range(1, len(catalog) + 1)):
        raise AllCasesPipelineError("meta ML chunks omit a catalog candidate")
    if tuple(fit_cache_evidence_by_chunk) != tuple(range(len(ranges))):
        raise AllCasesPipelineError("meta ML fit-cache chunks are incomplete")
    try:
        cache_aggregate = ml.aggregate_shared_fit_cache_evidence(
            "META",
            tuple(fit_cache_evidence_by_chunk[index] for index in range(len(ranges))),
        )
    except ml.AllCasesMLError as error:
        raise AllCasesPipelineError("meta ML fit-cache aggregate differs") from error
    cache_aggregate_document = cache_aggregate.as_dict()
    return _MetaSearchEvidence(
        tuple(rows_by_rank[index] for index in range(1, len(catalog) + 1)),
        tuple(selection_by_rank[index] for index in sorted(selection_by_rank)),
        artifacts_by_id,
        meta_plan.plan_document,
        cache_aggregate_document,
        int(cache_aggregate_document["fit_count"]),
    )


def _finalize_search_result(
    config: AllCasesConfig,
    universe: Mapping[str, object],
    ledger: _SearchSubledger,
    stage_a_selection: object,
    feature_plan: _StageBFeaturePlan,
    stage_b_evidence: _StageBSearchEvidence,
    symbolic_selection_rows: Sequence[Mapping[str, object]],
    symbolic_artifacts: Sequence[Mapping[str, object]],
    direct_evidence: _DirectSearchEvidence,
    meta_evidence: _MetaSearchEvidence,
    *,
    verify_only: bool,
) -> dict[str, object]:
    """Release the one Search selection barrier and its exact public evidence root."""

    from . import ml

    direct_ids = _candidate_ids_from_search_rows(
        direct_evidence.candidate_rows,
        ml.build_direct_candidate_catalog(),
        label="direct Search",
    )
    meta_ids = _candidate_ids_from_search_rows(
        meta_evidence.candidate_rows,
        ml.build_meta_candidate_catalog(),
        label="meta Search",
    )
    symbolic_ids = tuple(recipe.strategy_id for recipe in feature_plan.recipes)
    evaluated_ids = (*symbolic_ids, *direct_ids, *meta_ids)
    if len(set(evaluated_ids)) != len(evaluated_ids):
        raise AllCasesPipelineError("Search evaluated candidate identities collide")
    if tuple(stage_a_selection.selected_policy_ids) != tuple(
        feature_plan.plan_document.get("selected_anchor_policy_ids", ())
    ):
        raise AllCasesPipelineError("Stage-B plan differs from the Stage-A barrier")

    all_selection_rows = (
        *symbolic_selection_rows,
        *direct_evidence.selection_rows,
        *meta_evidence.selection_rows,
    )
    eligible_ids = tuple(str(row.get("candidate_id")) for row in all_selection_rows)
    if len(set(eligible_ids)) != len(eligible_ids) or not set(eligible_ids).issubset(evaluated_ids):
        raise AllCasesPipelineError("Search eligible selection family differs")
    selected_ids = _select_diverse_search_candidates(all_selection_rows)

    artifacts_by_id: dict[str, Mapping[str, object]] = {}
    strategy_artifacts_by_id: dict[str, Mapping[str, object]] = {}
    for raw in symbolic_artifacts:
        candidate_id = _require_sha(
            raw.get("candidate_id"), label="symbolic frozen artifact candidate ID"
        )
        if candidate_id in strategy_artifacts_by_id:
            raise AllCasesPipelineError("symbolic frozen artifacts are duplicated")
        strategy_artifacts_by_id[candidate_id] = raw
    for source in (
        direct_evidence.model_artifacts_by_candidate,
        meta_evidence.model_artifacts_by_candidate,
    ):
        for candidate_id, raw in source.items():
            if candidate_id in artifacts_by_id:
                raise AllCasesPipelineError("ML frozen artifacts are duplicated")
            artifacts_by_id[candidate_id] = raw
    if any(
        candidate_id not in strategy_artifacts_by_id and candidate_id not in artifacts_by_id
        for candidate_id in selected_ids
    ):
        raise AllCasesPipelineError("selected Search candidate lacks a frozen artifact")
    strategy_artifacts = tuple(
        strategy_artifacts_by_id[candidate_id]
        for candidate_id in selected_ids
        if candidate_id in strategy_artifacts_by_id
    )
    model_artifacts = tuple(
        artifacts_by_id[candidate_id]
        for candidate_id in selected_ids
        if candidate_id in artifacts_by_id
    )
    selection_evidence = {
        "eligible_candidate_evidence_sha256": _sha256(list(all_selection_rows)),
        "fit_cache_aggregate_sha256s": [
            direct_evidence.fit_cache_aggregate["artifact_sha256"],
            meta_evidence.fit_cache_aggregate["artifact_sha256"],
        ],
        "model_artifact_sha256s": [item["model_sha256"] for item in model_artifacts],
        "schema": "systematic_fx.ai_all_cases_search_final_selection.v1",
        "selected_candidate_ids": list(selected_ids),
        "strategy_artifact_sha256s": [item["strategy_sha256"] for item in strategy_artifacts],
    }
    counts = _search_phase_counts(config)

    def resume_final(index: int, payload: Mapping[str, object]) -> None:
        if index != 0 or dict(payload) != selection_evidence:
            raise AllCasesPipelineError(
                "persisted Search-final selection differs on aggregate replay"
            )

    ledger.ensure_phase(
        "FINAL_MAX12",
        counts["FINAL_MAX12"],
        lambda index: (
            selection_evidence
            if index == 0
            else (_ for _ in ()).throw(
                AllCasesPipelineError("Search-final selection is a single barrier")
            )
        ),
        verify_only=verify_only,
        resume_consumer=resume_final,
    )
    ledger.assert_complete(counts)
    leaves = ledger.leaf_closure()
    recipe_root = _require_sha(
        feature_plan.plan_document.get("complete_recipe_root_sha256"),
        label="complete symbolic recipe root",
    )
    stage_a_policy_ids = tuple(stage_a_selection.selected_policy_ids)
    complete_derivation = {
        "complete_symbolic_candidate_count": len(symbolic_ids),
        "complete_symbolic_candidate_root_sha256": recipe_root,
        "entry_exit_recipe_sha256": universe.get("entry_exit_recipe_sha256"),
        "stage_a_selected_policy_ids": list(stage_a_policy_ids),
    }
    return {
        "complete_symbolic_candidate_count": len(symbolic_ids),
        "complete_symbolic_candidate_root_sha256": recipe_root,
        "complete_symbolic_derivation_sha256": _sha256(complete_derivation),
        "direct_candidate_count": len(direct_ids),
        "direct_fit_cache_aggregate": dict(direct_evidence.fit_cache_aggregate),
        "evaluated_candidate_ids": list(evaluated_ids),
        "evaluated_family_sha256": _sha256(list(evaluated_ids)),
        "meta_candidate_count": len(meta_ids),
        "meta_fit_cache_aggregate": dict(meta_evidence.fit_cache_aggregate),
        "meta_plan_sha256": _sha256(meta_evidence.plan_document),
        "model_artifacts": [dict(item) for item in model_artifacts],
        "schema": "systematic_fx.ai_all_cases_search_result_payload.v1",
        "search_chunk_artifacts": list(leaves),
        "search_chunk_leaf_closure_sha256": _sha256(list(leaves)),
        "search_subledger_head_sha256": ledger.head_sha256,
        "selected_candidate_ids": list(selected_ids),
        "stage_a_selected_policy_ids": list(stage_a_policy_ids),
        "stage_a_selection_artifact_sha256": stage_a_selection.artifact_sha256,
        "stage_a_selection_proof_sha256": _sha256(
            {
                "selected_policy_ids": list(stage_a_policy_ids),
                "universe_root_sha256": universe.get("universe_root_sha256"),
            }
        ),
        "stage_b_plan_sha256": _sha256(feature_plan.plan_document),
        "strategy_artifacts": [dict(item) for item in strategy_artifacts],
        "symbolic_top24_artifact_sha256": _sha256(stage_b_evidence.top24_document),
        "universe_root_sha256": universe.get("universe_root_sha256"),
    }


def _frozen_search_candidates(
    search: Mapping[str, object], candidate_ids: Sequence[str]
) -> tuple[_FrozenSearchCandidate, ...]:
    """Strictly bind selected IDs to their one Search-final strategy/model document."""

    selected = tuple(candidate_ids)
    if len(set(selected)) != len(selected):
        raise AllCasesPipelineError("selected Search candidates are duplicated")
    found: dict[str, _FrozenSearchCandidate] = {}
    collections = (
        ("strategy_artifacts", "strategy_document", "strategy_sha256", "SYMBOLIC"),
        ("model_artifacts", "model_document", "model_sha256", None),
    )
    for collection_key, document_key, digest_key, required_kind in collections:
        collection = search.get(collection_key)
        if not isinstance(collection, list):
            raise AllCasesPipelineError("Search frozen artifact collection differs")
        for raw in collection:
            if not isinstance(raw, Mapping) or not isinstance(raw.get(document_key), Mapping):
                raise AllCasesPipelineError("Search frozen artifact row differs")
            candidate_id = _require_sha(raw.get("candidate_id"), label="Search frozen candidate ID")
            document = raw[document_key]
            if (
                candidate_id in found
                or document.get("candidate_id") != candidate_id
                or _sha256(document) != raw.get(digest_key)
                or raw.get("candidate_kind") != document.get("candidate_kind")
                or raw.get("family_key") != document.get("family_key")
                or (required_kind is not None and raw.get("candidate_kind") != required_kind)
            ):
                raise AllCasesPipelineError("Search frozen artifact binding differs")
            candidate = document.get("candidate")
            recipe = document.get("recipe")
            rank = (
                candidate.get("selection_rank")
                if isinstance(candidate, Mapping)
                else recipe.get("strategy_rank")
                if isinstance(recipe, Mapping)
                else document.get("catalog_selection_rank")
            )
            if isinstance(rank, bool) or not isinstance(rank, int) or rank < 1:
                raise AllCasesPipelineError("Search frozen catalog rank differs")
            found[candidate_id] = _FrozenSearchCandidate(
                candidate_id,
                str(raw["candidate_kind"]),
                str(raw["family_key"]),
                rank,
                document,
            )
    if set(found) != set(selected):
        raise AllCasesPipelineError("selected Search family lacks exact frozen artifacts")
    return tuple(found[candidate_id] for candidate_id in selected)


def _symbolic_policy_and_recipe(candidate: _FrozenSearchCandidate) -> tuple[object, object]:
    from . import symbolic

    if candidate.candidate_kind != "SYMBOLIC":
        raise AllCasesPipelineError("requested symbolic parts from a model candidate")
    policy_row = candidate.document.get("anchor_policy")
    recipe_row = candidate.document.get("recipe")
    if not isinstance(policy_row, Mapping) or not isinstance(recipe_row, Mapping):
        raise AllCasesPipelineError("frozen symbolic strategy parts are absent")
    try:
        policy = symbolic.AnchorPolicy(
            int(policy_row["policy_rank"]),
            str(policy_row["policy_id"]),
            str(policy_row["base_candidate_id"]),
            str(policy_row["context_id"]),
            str(policy_row["time_filter_id"]),
            str(policy_row["delay_id"]),
        )
        recipe = symbolic.CompleteStrategyRecipe(
            int(recipe_row["strategy_rank"]),
            str(recipe_row["strategy_id"]),
            int(recipe_row["anchor_selection_rank"]),
            str(recipe_row["anchor_policy_id"]),
            str(recipe_row["entry_policy_id"]),
            str(recipe_row["exit_policy_id"]),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise AllCasesPipelineError("frozen symbolic strategy cannot be decoded") from error
    if (
        _canonical_json_bytes(policy.as_dict()) != _canonical_json_bytes(dict(policy_row))
        or _canonical_json_bytes(recipe.as_dict()) != _canonical_json_bytes(dict(recipe_row))
        or recipe.anchor_policy_id != policy.policy_id
        or recipe.strategy_id != candidate.candidate_id
    ):
        raise AllCasesPipelineError("frozen symbolic strategy round trip differs")
    return policy, recipe


def _persisted_search_payload(project_root: Path, config: AllCasesConfig) -> Mapping[str, object]:
    """Read the governed outer Search release needed by holdout mask replay."""

    from . import run

    run_root = run._fixed_run_root(project_root, create=False)
    artifacts = run._safe_directory(run_root / "artifacts", create=False)
    ledger = run._Ledger(run_root / "ledger", create=False)
    events = ledger.verify()
    return run._event_payload(
        artifacts,
        events,
        "SEARCH_RESULTS_RELEASED",
        schema="systematic_fx.ai_all_cases_search_results.v1",
        config=config,
    )


def _search_ml_artifact_registry(
    ledger: _SearchSubledger,
    events: Sequence[_SearchEvent],
    search: Mapping[str, object],
    recipe_by_id: Mapping[str, object],
    policy_by_id: Mapping[str, object],
    family_by_policy_id: Mapping[str, str],
    symbolic_top24: Mapping[str, Mapping[str, object]],
    symbolic_search_selection: object,
    symbolic_gate_by_id: Mapping[str, object],
    training_dates_by_scope: Mapping[str, tuple[date, ...]],
) -> Mapping[str, Mapping[str, object]]:
    """Cross-bind selected public models to their immutable raw-chunk bytes."""

    from . import ml

    registry: dict[str, Mapping[str, object]] = {}
    cache_documents: dict[str, dict[str, object]] = {}
    selection_rows_by_kind: dict[str, list[Mapping[str, object]]] = {
        "DIRECT_ML": [],
        "META_ML": [],
    }
    catalogs = {
        "DIRECT_ML_CHUNKS": (
            "DIRECT_ML",
            "DIRECT",
            tuple(ml.build_direct_candidate_catalog()),
            126,
            "direct_fit_cache_aggregate",
        ),
        "META_ML_CHUNKS": (
            "META_ML",
            "META",
            tuple(ml.build_meta_candidate_catalog()),
            84,
            "meta_fit_cache_aggregate",
        ),
    }
    for phase, (
        candidate_kind,
        aggregate_kind,
        catalog,
        per_chunk_fit_cap,
        public_aggregate_key,
    ) in catalogs.items():
        ranges = _balanced_chunk_ranges(len(catalog), 24)
        phase_events = tuple(event for event in events if event.phase == phase)
        if len(phase_events) != len(ranges):
            raise AllCasesPipelineError("Search ML raw chunk family is incomplete")
        cache_evidence = []
        for index, (event, (start, end)) in enumerate(zip(phase_events, ranges, strict=True)):
            if event.chunk_index != index:
                raise AllCasesPipelineError("Search ML raw chunk order differs")
            payload = ledger._artifact_payload(event)
            expected = catalog[start:end]
            rows = payload.get("candidates")
            if (
                type(payload.get("candidate_count")) is not int
                or type(payload.get("first_catalog_rank")) is not int
                or type(payload.get("last_catalog_rank")) is not int
                or payload.get("candidate_count") != len(expected)
                or payload.get("first_catalog_rank") != expected[0].selection_rank
                or payload.get("last_catalog_rank") != expected[-1].selection_rank
                or not isinstance(rows, list)
                or len(rows) != len(expected)
            ):
                raise AllCasesPipelineError("Search ML raw chunk bounds differ")
            raw_cache = payload.get("fit_cache_evidence")
            if not isinstance(raw_cache, Mapping):
                raise AllCasesPipelineError("Search ML cache evidence is absent")
            try:
                cache_row = ml.SharedFitCacheEvidence.from_dict(raw_cache)
            except ml.AllCasesMLError as error:
                raise AllCasesPipelineError("Search ML cache evidence differs") from error
            if cache_row.maximum_fit_count != per_chunk_fit_cap:
                raise AllCasesPipelineError("Search ML per-chunk fit cap differs")
            cache_evidence.append(cache_row)
            for candidate, row in zip(expected, rows, strict=True):
                if not isinstance(row, Mapping) or _canonical_json_bytes(
                    row.get("candidate")
                ) != _canonical_json_bytes(candidate.as_dict()):
                    raise AllCasesPipelineError("Search ML raw catalog identity differs")
                frozen = row.get("frozen_model_artifact")
                if frozen is None:
                    if isinstance(row.get("gate"), Mapping) and row["gate"].get("eligible") is True:
                        raise AllCasesPipelineError("eligible Search ML row lacks a frozen model")
                    continue
                if not isinstance(frozen, Mapping) or set(frozen) != {
                    "candidate_id",
                    "candidate_kind",
                    "family_key",
                    "model_document",
                    "model_sha256",
                }:
                    raise AllCasesPipelineError("Search raw frozen-model schema differs")
                model_document = frozen["model_document"]
                candidate_id = candidate.candidate_id
                if (
                    candidate_id in registry
                    or frozen.get("candidate_id") != candidate_id
                    or frozen.get("candidate_kind") != candidate_kind
                    or not isinstance(frozen.get("family_key"), str)
                    or not frozen["family_key"]
                    or not isinstance(model_document, Mapping)
                    or _canonical_json_bytes(model_document.get("candidate"))
                    != _canonical_json_bytes(candidate.as_dict())
                    or model_document.get("candidate_id") != candidate_id
                    or model_document.get("candidate_kind") != candidate_kind
                    or model_document.get("family_key") != frozen["family_key"]
                    or _sha256(model_document) != frozen.get("model_sha256")
                ):
                    raise AllCasesPipelineError("Search raw frozen-model binding differs")
                gate = row.get("gate")
                controls = row.get("search_controls")
                if (
                    not isinstance(gate, Mapping)
                    or gate.get("eligible") is not True
                    or not isinstance(controls, Mapping)
                    or not isinstance(controls.get("evaluations"), list)
                    or len(controls["evaluations"]) != 3
                    or not isinstance(controls["evaluations"][0], Mapping)
                ):
                    raise AllCasesPipelineError("eligible Search ML selection evidence differs")
                selection_rows_by_kind[candidate_kind].append(
                    _direct_selection_row_from_documents(controls["evaluations"][0], gate)
                    if candidate_kind == "DIRECT_ML"
                    else _meta_selection_row_from_documents(controls["evaluations"][0], gate)
                )
                registry[candidate_id] = frozen
        try:
            aggregate = ml.aggregate_shared_fit_cache_evidence(
                aggregate_kind, tuple(cache_evidence)
            ).as_dict()
        except ml.AllCasesMLError as error:
            raise AllCasesPipelineError("Search ML cache aggregate differs") from error
        public_aggregate = search.get(public_aggregate_key)
        if not isinstance(public_aggregate, Mapping) or _canonical_json_bytes(
            public_aggregate
        ) != _canonical_json_bytes(aggregate):
            raise AllCasesPipelineError("outer Search fit-cache aggregate differs")
        cache_documents[aggregate_kind] = aggregate

    meta_events = tuple(
        event for event in events if event.phase == "META_PLAN_FROZEN" and event.chunk_index == 0
    )
    if len(meta_events) != 1:
        raise AllCasesPipelineError("Search meta rank-slot plan is missing")
    meta_plan = ledger._artifact_payload(meta_events[0])
    raw_certificates = meta_plan.get("certificates_by_world_and_scope")
    if (
        set(meta_plan)
        != {
            "certificates_by_world_and_scope",
            "schema",
            "source_symbolic_top24_sha256",
            "symbolic_frozen_artifacts",
            "symbolic_selection_rows",
        }
        or meta_plan.get("schema") != "systematic_fx.ai_all_cases_meta_rank_slot_plan.v1"
        or not isinstance(raw_certificates, Mapping)
        or set(raw_certificates) != set(ml.NULL_WORLD_ORDER)
        or search.get("meta_plan_sha256") != _sha256(meta_plan)
        or search.get("symbolic_top24_artifact_sha256")
        != meta_plan.get("source_symbolic_top24_sha256")
    ):
        raise AllCasesPipelineError("outer Search meta rank-slot plan differs")
    certificates: dict[str, dict[str, object]] = {}
    expected_scopes = (*ml.SEARCH_OUTER_FOLD_KEYS, "SEARCH_FINAL")
    if (
        set(symbolic_top24) != set(ml.NULL_WORLD_ORDER)
        or tuple(training_dates_by_scope) != expected_scopes
        or meta_plan.get("source_symbolic_top24_sha256")
        != search.get("symbolic_top24_artifact_sha256")
    ):
        raise AllCasesPipelineError("Search symbolic ranking source differs")
    for world in ml.NULL_WORLD_ORDER:
        raw_scopes = raw_certificates[world]
        typed_scopes = symbolic_top24[world]
        if (
            not isinstance(raw_scopes, Mapping)
            or tuple(raw_scopes) != expected_scopes
            or tuple(typed_scopes) != expected_scopes
        ):
            raise AllCasesPipelineError("Search meta rank-slot scopes differ")
        certificates[world] = {}
        for scope in expected_scopes:
            try:
                certificate = ml.SymbolicRankingCertificate.from_dict(raw_scopes[scope])
            except ml.AllCasesMLError as error:
                raise AllCasesPipelineError("Search meta rank-slot certificate differs") from error
            ranking = typed_scopes[scope]
            ranked = []
            for rank, strategy_id in enumerate(ranking.selected_strategy_ids, start=1):
                recipe = recipe_by_id.get(strategy_id)
                if recipe is None:
                    raise AllCasesPipelineError(
                        "Search meta ranking escapes the Stage-B recipe family"
                    )
                family = family_by_policy_id.get(recipe.anchor_policy_id)
                policy = policy_by_id.get(recipe.anchor_policy_id)
                if not isinstance(family, str) or not family:
                    raise AllCasesPipelineError("Search meta ranking lacks its trigger family")
                if policy is None:
                    raise AllCasesPipelineError("Search meta ranking lacks its anchor policy")
                ranked.append(
                    ml.RankedSymbolicStrategy(
                        rank,
                        strategy_id,
                        family,
                        recipe.anchor_policy_id,
                        policy.base_candidate_id,
                        policy.context_id,
                        policy.time_filter_id,
                        policy.delay_id,
                        recipe.entry_policy_id,
                        recipe.exit_policy_id,
                    )
                )
            expected_certificate = ml.build_symbolic_ranking_certificate(
                null_world=world,
                fold_key=scope,
                training_dates=training_dates_by_scope[scope],
                ranked_strategies=ranked,
            )
            if (
                certificate.null_world != world
                or certificate.fold_key != scope
                or certificate != expected_certificate
            ):
                raise AllCasesPipelineError("Search meta certificate coordinate differs")
            certificates[world][scope] = certificate

    symbolic_rows = meta_plan.get("symbolic_selection_rows")
    symbolic_artifacts = meta_plan.get("symbolic_frozen_artifacts")
    if not isinstance(symbolic_rows, list) or not isinstance(symbolic_artifacts, list):
        raise AllCasesPipelineError("Search symbolic selection evidence is absent")
    selected_symbolic_ids = tuple(symbolic_search_selection.selected_strategy_ids)
    symbolic_artifacts_by_id: dict[str, Mapping[str, object]] = {}
    symbolic_row_ids = []
    for row in symbolic_rows:
        row = _validate_search_selection_row(row)
        if row["candidate_kind"] != "SYMBOLIC":
            raise AllCasesPipelineError("Search symbolic selection row differs")
        symbolic_row_ids.append(
            _require_sha(row.get("candidate_id"), label="symbolic selection candidate ID")
        )
    for artifact in symbolic_artifacts:
        if not isinstance(artifact, Mapping) or set(artifact) != {
            "candidate_id",
            "candidate_kind",
            "family_key",
            "strategy_document",
            "strategy_sha256",
        }:
            raise AllCasesPipelineError("Search symbolic frozen artifact differs")
        candidate_id = _require_sha(
            artifact.get("candidate_id"), label="symbolic frozen candidate ID"
        )
        document = artifact.get("strategy_document")
        recipe = recipe_by_id.get(candidate_id)
        family = None if recipe is None else family_by_policy_id.get(recipe.anchor_policy_id)
        gate = symbolic_gate_by_id.get(candidate_id)
        if (
            candidate_id in symbolic_artifacts_by_id
            or artifact.get("candidate_kind") != "SYMBOLIC"
            or artifact.get("family_key") != family
            or not isinstance(document, Mapping)
            or document.get("candidate_id") != candidate_id
            or document.get("candidate_kind") != "SYMBOLIC"
            or document.get("family_key") != family
            or recipe is None
            or _canonical_json_bytes(document.get("recipe"))
            != _canonical_json_bytes(recipe.as_dict())
            or recipe.anchor_policy_id not in policy_by_id
            or _canonical_json_bytes(document.get("anchor_policy"))
            != _canonical_json_bytes(policy_by_id[recipe.anchor_policy_id].as_dict())
            or _canonical_json_bytes(document.get("search_gate"))
            != _canonical_json_bytes(None if gate is None else gate.as_dict())
            or _sha256(document) != artifact.get("strategy_sha256")
        ):
            raise AllCasesPipelineError("Search symbolic frozen binding differs")
        symbolic_artifacts_by_id[candidate_id] = artifact
    if (
        tuple(symbolic_artifacts_by_id) != tuple(symbolic_row_ids)
        or tuple(symbolic_row_ids) != selected_symbolic_ids
    ):
        raise AllCasesPipelineError("Search symbolic row/artifact order differs")

    all_selection_rows = (
        *symbolic_rows,
        *selection_rows_by_kind["DIRECT_ML"],
        *selection_rows_by_kind["META_ML"],
    )
    expected_selected = _select_diverse_search_candidates(all_selection_rows)

    outer_models = search.get("model_artifacts")
    outer_strategies = search.get("strategy_artifacts")
    selected = search.get("selected_candidate_ids")
    if (
        not isinstance(outer_models, list)
        or not isinstance(outer_strategies, list)
        or not isinstance(selected, list)
        or selected != list(expected_selected)
    ):
        raise AllCasesPipelineError("outer Search model selection differs")
    outer_by_id: dict[str, Mapping[str, object]] = {}
    for raw in outer_models:
        if not isinstance(raw, Mapping):
            raise AllCasesPipelineError("outer Search model artifact differs")
        candidate_id = _require_sha(
            raw.get("candidate_id"), label="outer Search model candidate ID"
        )
        frozen = registry.get(candidate_id)
        if (
            candidate_id in outer_by_id
            or frozen is None
            or _canonical_json_bytes(raw) != _canonical_json_bytes(frozen)
        ):
            raise AllCasesPipelineError("outer Search model differs from immutable raw chunk")
        outer_by_id[candidate_id] = raw
    selected_model_ids = tuple(
        candidate_id for candidate_id in selected if candidate_id in registry
    )
    if tuple(outer_by_id) != selected_model_ids:
        raise AllCasesPipelineError("outer Search selected model order differs")

    outer_strategies_by_id: dict[str, Mapping[str, object]] = {}
    for raw in outer_strategies:
        if not isinstance(raw, Mapping):
            raise AllCasesPipelineError("outer Search strategy artifact differs")
        candidate_id = _require_sha(
            raw.get("candidate_id"), label="outer Search strategy candidate ID"
        )
        frozen = symbolic_artifacts_by_id.get(candidate_id)
        if (
            candidate_id in outer_strategies_by_id
            or frozen is None
            or _canonical_json_bytes(raw) != _canonical_json_bytes(frozen)
        ):
            raise AllCasesPipelineError(
                "outer Search strategy differs from immutable symbolic barrier"
            )
        outer_strategies_by_id[candidate_id] = raw
    selected_strategy_ids = tuple(
        candidate_id for candidate_id in selected if candidate_id in symbolic_artifacts_by_id
    )
    if tuple(outer_strategies_by_id) != selected_strategy_ids:
        raise AllCasesPipelineError("outer Search selected strategy order differs")

    for candidate_id, artifact in outer_by_id.items():
        if artifact.get("candidate_kind") != "META_ML":
            continue
        model_document = artifact["model_document"]
        model_rows = model_document.get("final_models")
        if (
            model_document.get("meta_plan_sha256") != search.get("meta_plan_sha256")
            or not isinstance(model_rows, list)
            or len(model_rows) != 3
        ):
            raise AllCasesPipelineError("outer Search meta model plan binding differs")
        try:
            models = tuple(
                ml.CanonicalMLModel.from_canonical_bytes(_canonical_json_bytes(item))
                for item in model_rows
            )
        except ml.AllCasesMLError as error:
            raise AllCasesPipelineError("outer Search meta model differs") from error
        if any(
            model.null_world != world
            or model.fold_key != "SEARCH_FINAL"
            or model.candidate_id != candidate_id
            or model.symbolic_ranking_certificate != certificates[world]["SEARCH_FINAL"]
            for world, model in zip(ml.NULL_WORLD_ORDER, models, strict=True)
        ):
            raise AllCasesPipelineError("outer Search meta certificate binding differs")

    final_events = tuple(
        event for event in events if event.phase == "FINAL_MAX12" and event.chunk_index == 0
    )
    if len(final_events) != 1:
        raise AllCasesPipelineError("Search final-selection barrier is missing")
    final = ledger._artifact_payload(final_events[0])
    if (
        set(final)
        != {
            "eligible_candidate_evidence_sha256",
            "fit_cache_aggregate_sha256s",
            "model_artifact_sha256s",
            "schema",
            "selected_candidate_ids",
            "strategy_artifact_sha256s",
        }
        or final.get("schema") != "systematic_fx.ai_all_cases_search_final_selection.v1"
        or final.get("selected_candidate_ids") != selected
        or final.get("eligible_candidate_evidence_sha256") != _sha256(list(all_selection_rows))
        or final.get("model_artifact_sha256s")
        != [outer_by_id[candidate_id]["model_sha256"] for candidate_id in selected_model_ids]
        or final.get("strategy_artifact_sha256s")
        != [
            outer_strategies_by_id[candidate_id]["strategy_sha256"]
            for candidate_id in selected_strategy_ids
        ]
        or final.get("fit_cache_aggregate_sha256s")
        != [
            cache_documents["DIRECT"]["artifact_sha256"],
            cache_documents["META"]["artifact_sha256"],
        ]
    ):
        raise AllCasesPipelineError("outer Search final-model barrier differs")
    return registry


def _search_recipe_registry(
    project_root: Path,
    config: AllCasesConfig,
    search: Mapping[str, object],
) -> tuple[Mapping[str, object], Mapping[str, object], Mapping[str, str]]:
    """Restore the exact Stage-B derivation barrier without reopening outcomes."""

    from . import ml, symbolic

    ledger = _SearchSubledger(project_root, config, create=False)
    counts = _search_phase_counts(config)
    ledger.assert_complete(counts)
    events = ledger.verify()
    leaves = ledger.leaf_closure()
    if (
        search.get("search_chunk_artifacts") != list(leaves)
        or search.get("search_chunk_leaf_closure_sha256") != _sha256(list(leaves))
        or search.get("search_subledger_head_sha256") != ledger.head_sha256
    ):
        raise AllCasesPipelineError("outer Search release differs from its internal closure")
    stage_a_events = tuple(
        event for event in events if event.phase == "STAGE_A_TOP256" and event.chunk_index == 0
    )
    score_events = tuple(event for event in events if event.phase == "STAGE_A_SCORE_CHUNKS")
    if len(stage_a_events) != 1 or len(score_events) != counts["STAGE_A_SCORE_CHUNKS"]:
        raise AllCasesPipelineError("Search Stage-A selection barrier is missing")
    stage_a_payload = ledger._artifact_payload(stage_a_events[0])
    if (
        set(stage_a_payload)
        != {
            "schema",
            "selection",
            "source_chunk_artifact_sha256s",
        }
        or stage_a_payload.get("schema") != "systematic_fx.ai_all_cases_stage_a_selection.v1"
    ):
        raise AllCasesPipelineError("Search Stage-A selection schema differs")
    try:
        stage_a_selection = symbolic.stage_a_selection_from_dict(stage_a_payload.get("selection"))
    except symbolic.SymbolicEngineError as error:
        raise AllCasesPipelineError("Search Stage-A selection differs") from error
    score_chunks = []
    source_score_sha256s = []
    structural_lattice_sha256s = []
    expected_stage_a_plan = tuple(symbolic.build_stage_a_chunk_plan())
    if len(expected_stage_a_plan) != len(score_events):
        raise AllCasesPipelineError("Search Stage-A frozen chunk plan differs")
    for index, event in enumerate(score_events):
        if event.chunk_index != index:
            raise AllCasesPipelineError("Search Stage-A score order differs")
        score_payload = ledger._artifact_payload(event)
        if (
            set(score_payload) != {"schema", "score_chunk", "structural_lattice_sha256"}
            or score_payload.get("schema") != "systematic_fx.ai_all_cases_stage_a_raw_chunk.v1"
        ):
            raise AllCasesPipelineError("Search Stage-A raw score schema differs")
        try:
            score_chunk = symbolic.stage_a_score_chunk_from_dict(score_payload.get("score_chunk"))
        except symbolic.SymbolicEngineError as error:
            raise AllCasesPipelineError("Search Stage-A raw score differs") from error
        if score_chunk.chunk != expected_stage_a_plan[index]:
            raise AllCasesPipelineError("Search Stage-A score coordinate differs")
        score_chunks.append(score_chunk)
        source_score_sha256s.append(score_chunk.artifact_sha256)
        structural_lattice_sha256s.append(
            _require_sha(
                score_payload.get("structural_lattice_sha256"),
                label="Search Stage-A structural lattice SHA",
            )
        )
    replayed_stage_a_selection = _select_stage_a_from_score_chunks(config, score_chunks)
    if (
        stage_a_selection != replayed_stage_a_selection
        or _canonical_json_bytes(stage_a_payload.get("selection"))
        != _canonical_json_bytes(replayed_stage_a_selection.as_dict())
        or stage_a_payload.get("source_chunk_artifact_sha256s") != source_score_sha256s
        or search.get("stage_a_selection_artifact_sha256") != stage_a_selection.artifact_sha256
        or search.get("stage_a_selected_policy_ids") != list(stage_a_selection.selected_policy_ids)
    ):
        raise AllCasesPipelineError("outer Search Stage-A selection differs")
    stage_a_selection = replayed_stage_a_selection
    matches = tuple(
        event for event in events if event.phase == "STAGE_B_PLAN_FROZEN" and event.chunk_index == 0
    )
    if len(matches) != 1:
        raise AllCasesPipelineError("Search Stage-B derivation barrier is missing")
    plan = ledger._artifact_payload(matches[0])
    if (
        set(plan)
        != {
            "complete_recipe_count",
            "complete_recipe_root_sha256",
            "control_opportunity_lattice_sha256",
            "policy_feature_commitments",
            "schema",
            "selected_anchor_policy_ids",
            "stage_b_chunks",
            "structural_lattice_sha256",
        }
        or plan.get("schema") != "systematic_fx.ai_all_cases_stage_b_feature_plan.v1"
    ):
        raise AllCasesPipelineError("Search Stage-B derivation schema differs")
    if search.get("stage_b_plan_sha256") != _sha256(plan):
        raise AllCasesPipelineError("outer Search Stage-B plan commitment differs")
    if set(structural_lattice_sha256s) != {plan["structural_lattice_sha256"]}:
        raise AllCasesPipelineError("Search Stage-A/B structural lattice binding differs")
    selected = plan["selected_anchor_policy_ids"]
    commitments = plan["policy_feature_commitments"]
    if (
        not isinstance(selected, list)
        or not isinstance(commitments, list)
        or isinstance(plan["complete_recipe_count"], bool)
        or not isinstance(plan["complete_recipe_count"], int)
    ):
        raise AllCasesPipelineError("Search Stage-B derivation arrays differ")
    if tuple(selected) != tuple(stage_a_selection.selected_policy_ids):
        raise AllCasesPipelineError("Search Stage-B plan escapes the Stage-A selection")
    selected_scores = {item.policy_id: item for item in stage_a_selection.selected_scores}
    if len(selected_scores) != len(stage_a_selection.selected_scores):
        raise AllCasesPipelineError("Search Stage-A selected scores are duplicated")
    policies: dict[str, object] = {}
    families: dict[str, str] = {}
    masks_by_world: dict[str, dict[str, object]] = {
        world: {} for world in ("REAL", "CIRCULAR", "MATCHED")
    }
    for item in commitments:
        if not isinstance(item, Mapping) or set(item) != {
            "control_masks",
            "entry_order_sha256s",
            "family",
            "policy_id",
            "rule_schedules",
            "structural_mask",
        }:
            raise AllCasesPipelineError("Search Stage-B policy commitment differs")
        try:
            structural = symbolic.structurally_eligible_policy_mask_from_dict(
                item["structural_mask"]
            )
            controls = symbolic.frozen_control_masks_from_dict(item["control_masks"])
        except symbolic.SymbolicEngineError as error:
            raise AllCasesPipelineError("Search Stage-B policy commitment differs") from error
        policy = structural.raw_mask.policy
        family = item["family"]
        score = selected_scores.get(policy.policy_id)
        world_masks = {
            "REAL": controls.real,
            "CIRCULAR": controls.circular,
            "MATCHED": controls.matched,
        }
        entry_order_sha256s = item["entry_order_sha256s"]
        if (
            policy.policy_id != item["policy_id"]
            or policy.policy_id in policies
            or not isinstance(family, str)
            or not family
            or score is None
            or family != score.family
            or policy.base_candidate_id != score.base_candidate_id
            or structural.raw_mask.mask_sha256 != score.raw_mask_sha256
            or structural.evaluable_mask.mask_sha256 != score.mask_sha256
            or structural.raw_mask.support_count != score.support_count
            or structural.evaluable_mask.support_count != score.evaluable_support_count
            or structural.lattice_sha256 != plan["structural_lattice_sha256"]
            or controls.stage_key != "SEARCH"
            or controls.real != structural.evaluable_mask
            or controls.opportunity_lattice_sha256 != plan["control_opportunity_lattice_sha256"]
            or not isinstance(entry_order_sha256s, Mapping)
            or set(entry_order_sha256s) != set(world_masks)
        ):
            raise AllCasesPipelineError("Search Stage-B policy identity differs")
        for world, mask in world_masks.items():
            expected_order_sha256 = (
                None if mask is None else symbolic.freeze_entry_orders(mask).artifact_sha256
            )
            if entry_order_sha256s[world] != expected_order_sha256:
                raise AllCasesPipelineError("Search Stage-B entry-order binding differs")
            if mask is not None:
                masks_by_world[world][policy.policy_id] = mask
        policies[policy.policy_id] = policy
        families[policy.policy_id] = family
    if tuple(policies) != tuple(selected):
        raise AllCasesPipelineError("Search Stage-B selected-policy order differs")
    recipes = tuple(symbolic.iter_complete_strategy_recipes(tuple(selected))) if selected else ()
    chunks = tuple(symbolic.build_stage_b_chunk_plan(len(selected)))
    if (
        len(recipes) != plan["complete_recipe_count"]
        or _sha256([item.as_dict() for item in recipes]) != plan["complete_recipe_root_sha256"]
        or _canonical_json_bytes(plan["stage_b_chunks"])
        != _canonical_json_bytes([item.as_dict() for item in chunks])
    ):
        raise AllCasesPipelineError("Search Stage-B recipe derivation differs")
    recipe_by_id = {item.strategy_id: item for item in recipes}
    if len(recipe_by_id) != len(recipes):
        raise AllCasesPipelineError("Search Stage-B recipes are duplicated")

    raw_events = tuple(event for event in events if event.phase == "STAGE_B_RAW_CHUNKS")
    if len(raw_events) != counts["STAGE_B_RAW_CHUNKS"] or tuple(
        event.chunk_index for event in raw_events
    ) != tuple(range(len(chunks))):
        raise AllCasesPipelineError("Search Stage-B raw evidence closure differs")
    evaluations: dict[str, dict[str, object]] = {
        world: {} for world in ("REAL", "CIRCULAR", "MATCHED")
    }
    coverage: dict[str, dict[str, object]] = {
        world: {} for world in ("REAL", "CIRCULAR", "MATCHED")
    }
    ineligibility: dict[str, dict[str, str]] = {
        world: {} for world in ("REAL", "CIRCULAR", "MATCHED")
    }
    for index, event in enumerate(raw_events):
        _consume_stage_b_raw_chunk(
            symbolic,
            index=index,
            payload=ledger._artifact_payload(event),
            chunks=chunks,
            recipes=recipes,
            masks_by_world=masks_by_world,
            evaluations=evaluations,
            coverage=coverage,
            ineligibility=ineligibility,
        )
    replayed_stage_b = _aggregate_stage_b_search_evidence(
        symbolic,
        recipes,
        families,
        evaluations,
        coverage,
        ineligibility,
    )

    top24_events = tuple(
        event for event in events if event.phase == "SYMBOLIC_TOP24" and event.chunk_index == 0
    )
    if len(top24_events) != 1:
        raise AllCasesPipelineError("Search symbolic ranking barrier is missing")
    top24_payload = ledger._artifact_payload(top24_events[0])
    if (
        set(top24_payload)
        != {
            "complete_search_gate_results",
            "ineligibility_by_world",
            "schema",
            "symbolic_search_selection",
            "top24_by_world_and_scope",
        }
        or top24_payload.get("schema") != "systematic_fx.ai_all_cases_symbolic_top24.v1"
        or search.get("symbolic_top24_artifact_sha256") != _sha256(top24_payload)
        or _canonical_json_bytes(top24_payload)
        != _canonical_json_bytes(replayed_stage_b.top24_document)
    ):
        raise AllCasesPipelineError("outer Search symbolic ranking barrier differs")
    gates = replayed_stage_b.gate_results
    symbolic_selection = replayed_stage_b.symbolic_selection
    gate_by_id = {item.strategy_id: item for item in gates}
    if (
        len(gate_by_id) != len(gates)
        or any(strategy_id not in recipe_by_id for strategy_id in gate_by_id)
        or symbolic_selection.eligible_count != sum(item.eligible for item in gates)
        or any(
            strategy_id not in gate_by_id or not gate_by_id[strategy_id].eligible
            for strategy_id in symbolic_selection.selected_strategy_ids
        )
        or symbolic_selection.classification
        != (
            "SYMBOLIC_SEARCH_SELECTED"
            if symbolic_selection.selected_strategy_ids
            else "NO_SYMBOLIC_SEARCH_FINALISTS"
        )
    ):
        raise AllCasesPipelineError("Search symbolic selection/gate binding differs")
    symbolic_world_to_ml = {
        "REAL": "REAL",
        "CIRCULAR": "CIRCULAR_TARGET",
        "MATCHED": "MATCHED_TARGET",
    }
    scopes = (*ml.SEARCH_OUTER_FOLD_KEYS, "SEARCH_FINAL")
    typed_top24: dict[str, dict[str, object]] = {world: {} for world in ml.NULL_WORLD_ORDER}
    for symbolic_world, ml_world in symbolic_world_to_ml.items():
        replayed_scopes = replayed_stage_b.top24_by_world_and_scope[symbolic_world]
        if tuple(replayed_scopes) != scopes:
            raise AllCasesPipelineError("Search symbolic top24 scopes differ")
        for scope in scopes:
            ranking = replayed_scopes[scope]
            if ranking.scope_key != scope or any(
                strategy_id not in recipe_by_id for strategy_id in ranking.selected_strategy_ids
            ):
                raise AllCasesPipelineError(
                    "Search symbolic top24 ranking escapes its recipe family"
                )
            typed_top24[ml_world][scope] = ranking

    search_plan = ml.build_search_block_plan(_plans(project_root).search.decision_dates)
    training_dates_by_scope = {
        fold.fold_key: tuple(fold.training_dates) for fold in search_plan.outer_folds
    }
    training_dates_by_scope["SEARCH_FINAL"] = tuple(search_plan.decision_dates)
    _search_ml_artifact_registry(
        ledger,
        events,
        search,
        recipe_by_id,
        policies,
        families,
        typed_top24,
        symbolic_selection,
        gate_by_id,
        training_dates_by_scope,
    )
    return recipe_by_id, policies, families


def verify_internal_search_release(
    project_root: Path,
    config: AllCasesConfig,
    search: Mapping[str, object],
) -> None:
    """Require the complete internal Search closure behind a production release."""

    if config.as_dict().get("config_id") != AI_ALL_CASES_CONFIG_ID:
        return
    if search.get("schema") != "systematic_fx.ai_all_cases_search_result_payload.v1":
        raise AllCasesPipelineError("released Search payload schema differs")
    search_root = project_root / _SEARCH_INTERNAL_RELATIVE_ROOT
    if not search_root.is_dir():
        raise AllCasesPipelineError("released Search internal store is missing")
    _search_recipe_registry(project_root, config, search)


def _model_candidate_and_models(
    candidate: _FrozenSearchCandidate,
) -> tuple[object, tuple[object, ...]]:
    """Strictly reopen the three immutable Search-final models."""

    from . import ml

    catalog = (
        ml.build_direct_candidate_catalog()
        if candidate.candidate_kind == "DIRECT_ML"
        else ml.build_meta_candidate_catalog()
        if candidate.candidate_kind == "META_ML"
        else ()
    )
    lookup = {item.candidate_id: item for item in catalog}
    typed = lookup.get(candidate.candidate_id)
    rows = candidate.document.get("final_models")
    if (
        typed is None
        or _canonical_json_bytes(candidate.document.get("candidate"))
        != _canonical_json_bytes(typed.as_dict())
        or not isinstance(rows, list)
        or len(rows) != 3
    ):
        raise AllCasesPipelineError("frozen Search-final model family differs")
    models = tuple(
        ml.CanonicalMLModel.from_canonical_bytes(_canonical_json_bytes(item)) for item in rows
    )
    if tuple(item.null_world for item in models) != ml.NULL_WORLD_ORDER or any(
        item.candidate_id != typed.candidate_id or item.fold_key != "SEARCH_FINAL"
        for item in models
    ):
        raise AllCasesPipelineError("frozen Search-final model binding differs")
    return typed, models


def _stage_policy_controls(
    state: _SearchFeatureState,
    policy: object,
    *,
    control_lattice: object,
    cache: dict[str, tuple[object, object]],
) -> tuple[object, object]:
    """Freeze one stage's real/control masks and rule schedules before any 1s row."""

    from . import symbolic

    cached = cache.get(policy.policy_id)
    if cached is not None:
        return cached
    raw = state.symbolic_stage.policy_mask(policy)
    structural = symbolic.freeze_structurally_eligible_policy_mask(raw, state.structural_lattice)
    controls = symbolic.freeze_feature_control_masks(
        state.plan.stage_key,
        structural.evaluable_mask,
        control_lattice,
        reporting_group_by_date=state.plan.reporting_group_by_date,
    )
    schedules = symbolic.build_control_rule_exit_schedules(state.symbolic_stage, policy, controls)
    cache[policy.policy_id] = controls, schedules
    return controls, schedules


def _world_member(value: object, world: str) -> object | None:
    attribute = {
        "REAL": "real",
        "CIRCULAR_TARGET": "circular",
        "MATCHED_TARGET": "matched",
    }[world]
    return getattr(value, attribute)


def _recipe_parts(recipe: object) -> tuple[object, object]:
    from . import symbolic

    entries = {item.entry_id: item for item in symbolic.build_entry_catalog().candidates}
    exits = {item.exit_id: item for item in symbolic.build_exit_catalog().candidates}
    try:
        return entries[recipe.entry_policy_id], exits[recipe.exit_policy_id]
    except KeyError as error:
        raise AllCasesPipelineError("complete recipe refers to an unknown entry/exit") from error


def _direct_oos_lineage_sha256(bundle: _DirectFeatureBundle, horizon_seconds: int) -> str:
    rows = bundle.feature_rows
    return _sha256(
        {
            "entry_schedule_sha256": rows.entry_schedule_sha256,
            "horizon_seconds": horizon_seconds,
            "opportunity_lattice_sha256": bundle.opportunity_lattice_sha256,
            "rows": [
                {
                    "contract": rows.contracts[index],
                    "entry_ns": int(rows.entry_ns[index]),
                    "label_exit_ns": int(rows.entry_ns[index]) + horizon_seconds * 1_000_000_000,
                    "outcome_span_id": int(rows.outcome_span_ids[index]),
                    "row_id": rows.row_ids[index],
                    "segment_id": int(rows.segment_ids[index]),
                }
                for index in range(rows.row_count)
            ],
            "schema": "systematic_fx.ai_all_cases_direct_outcome_lineage.v1",
        }
    )


def _meta_feature_gate(
    state: _SearchFeatureState,
    typed_candidate: object,
    model: object,
    recipe: object,
    policy: object,
    base_mask: object,
    bars_by_timeframe: Mapping[int, object],
) -> tuple[object, object, object, object] | None:
    """Build one world's outcome-blind Expert-8 rows, base orders, and gate."""

    import numpy as np

    from . import ml, symbolic

    entry, exit_policy = _recipe_parts(recipe)
    base_orders = symbolic.freeze_entry_orders(base_mask, (entry,))
    if len(base_orders.orders) < 2:
        return None
    base_candidates = {
        item.candidate_id: item for item in symbolic.build_base_event_catalog().candidates
    }
    contexts = {item.context_id: item for item in symbolic.build_context_catalog()}
    try:
        base_candidate = base_candidates[policy.base_candidate_id]
        context = contexts[policy.context_id]
    except KeyError as error:
        raise AllCasesPipelineError("meta base policy catalog identity differs") from error
    experts = tuple(
        symbolic.build_causal_expert_feature_artifact(
            base_candidate,
            context,
            policy,
            order.anchor,
            order,
            exit_policy,
        )
        for order in base_orders.orders
    )
    stage_rank = {source_date: index for index, source_date in enumerate(state.plan.decision_dates)}
    anchors = ml.CausalAnchorRows(
        row_ids=tuple(order.order_id for order in base_orders.orders),
        decision_ns=np.asarray(
            [order.anchor.anchor_ns for order in base_orders.orders], dtype=np.int64
        ),
        entry_ns=np.asarray(
            [order.anchor.anchor_ns for order in base_orders.orders], dtype=np.int64
        ),
        source_dates=tuple(order.anchor.source_date for order in base_orders.orders),
        contracts=tuple(order.anchor.contract for order in base_orders.orders),
        outcome_span_ids=np.asarray(
            [order.anchor.outcome_span_id for order in base_orders.orders], dtype=np.int64
        ),
        segment_ids=np.asarray(
            [order.anchor.segment_id for order in base_orders.orders], dtype=np.uint64
        ),
        stage_date_ranks=np.asarray(
            [stage_rank[order.anchor.source_date] for order in base_orders.orders],
            dtype=np.int64,
        ),
        stage_key=state.plan.stage_key,
        decision_timeframe_seconds=base_candidate.trigger_timeframe_seconds,
        entry_schedule_sha256=base_orders.artifact_sha256,
    )
    try:
        feature_rows = ml.build_causal_feature_rows(
            anchors=anchors,
            bars_by_timeframe=bars_by_timeframe,
            feature_set_id="FULL_MTF_PLUS_EXPERT_221",
            expert_artifacts=experts,
        ).for_feature_set(typed_candidate.feature_set_id)
    except ml.AllCasesMLError as error:
        if "fewer than two anchors have complete causal history" in str(error):
            return None
        raise
    retained = tuple(int(value) for value in feature_rows.retained_input_indexes)
    retained_anchors = tuple(base_orders.orders[index].anchor for index in retained)
    retained_experts = tuple(experts[index] for index in retained)
    retained_mask = symbolic.PolicyMask.from_records(
        base_mask.policy,
        base_mask.family,
        base_mask.direction,
        retained_anchors,
    )
    retained_orders = symbolic.freeze_entry_orders(retained_mask, (entry,))
    if tuple(item.order_id for item in retained_orders.orders) != feature_rows.row_ids:
        raise AllCasesPipelineError("meta retained feature/order identities differ")
    certificate = model.symbolic_ranking_certificate
    if certificate is None:
        raise AllCasesPipelineError("meta Search-final model lacks its ranking certificate")
    schedule = ml.build_meta_anchor_gate_schedule(
        typed_candidate,
        feature_rows,
        base_order_batch=retained_orders,
        strategy_recipe=recipe,
        expert_artifacts=retained_experts,
        partition_key=state.plan.stage_key,
        symbolic_ranking_certificate=certificate,
    )
    gate = ml.freeze_meta_anchor_gate(model, feature_rows, schedule)
    return gate, retained_orders, feature_rows, retained_mask


def _meta_admitted_execution(
    state: _SearchFeatureState,
    gate: object,
    base_orders: object,
    base_mask: object,
    recipe: object,
) -> _StageWorldExecution:
    """Turn a frozen meta gate into a typed symbolic mask and prove order identity."""

    from . import ml, symbolic

    admitted = ml.apply_meta_gate_to_symbolic_orders(gate, base_orders)
    admitted_mask = symbolic.PolicyMask.from_records(
        base_mask.policy,
        base_mask.family,
        base_mask.direction,
        tuple(item.anchor for item in admitted),
    )
    entry, _exit = _recipe_parts(recipe)
    replayed = symbolic.freeze_entry_orders(admitted_mask, (entry,))
    if tuple(item.order_id for item in replayed.orders) != tuple(
        item.order_id for item in admitted
    ):
        raise AllCasesPipelineError("meta admitted symbolic orders do not replay")
    rule_schedule = symbolic.build_rule_exit_schedule(
        state.symbolic_stage, base_mask.policy, admitted_mask.records
    )
    return _StageWorldExecution(recipe, admitted_mask, rule_schedule)


def _freeze_stage_candidate_masks(
    project_root: Path,
    config: AllCasesConfig,
    state: _SearchFeatureState,
    candidates: Sequence[_FrozenSearchCandidate],
    search: Mapping[str, object],
) -> tuple[_StageCandidateMask, ...]:
    """Freeze every candidate/world action mask for one stage without opening 1s."""

    from . import ml, symbolic

    recipe_by_id, search_policies, _search_families = _search_recipe_registry(
        project_root, config, search
    )
    needs_symbolic = any(item.candidate_kind in {"SYMBOLIC", "META_ML"} for item in candidates)
    control_lattice = (
        symbolic.build_control_opportunity_lattice(
            state.bars_by_timeframe[300],
            decision_dates=state.plan.decision_dates,
            allowed_tail_end_ns=state.allowed_tail_end_ns,
            signal_bars_by_timeframe=state.bars_by_timeframe,
        )
        if needs_symbolic
        else None
    )
    policy_cache: dict[str, tuple[object, object]] = {}
    partition_date_certificate = ml.build_stage_partition_date_certificate(
        state.plan.stage_key,
        state.plan.decision_dates,
        upstream_plan_sha256=ml.stage_partition_date_plan_sha256(
            state.plan.stage_key, state.plan.decision_dates
        ),
    )
    if (
        partition_date_certificate.stage_key != state.plan.stage_key
        or partition_date_certificate.decision_dates != state.plan.decision_dates
    ):
        raise AllCasesPipelineError("stage partition date certificate differs from its plan")
    direct_coordinates = {
        (typed.decision_timeframe_seconds, typed.horizon_seconds)
        for frozen in candidates
        if frozen.candidate_kind == "DIRECT_ML"
        for typed, _models in (_model_candidate_and_models(frozen),)
    }
    direct_bundles: Mapping[int, _DirectFeatureBundle] | None = None
    meta_bars = (
        _ml_bar_series(state)
        if any(item.candidate_kind == "META_ML" for item in candidates)
        else None
    )
    output: list[_StageCandidateMask] = []

    for frozen in candidates:
        if frozen.candidate_kind == "SYMBOLIC":
            policy, recipe = _symbolic_policy_and_recipe(frozen)
            if control_lattice is None:  # pragma: no cover - guarded by needs_symbolic
                raise AllCasesPipelineError("symbolic control lattice is absent")
            controls, schedules = _stage_policy_controls(
                state,
                policy,
                control_lattice=control_lattice,
                cache=policy_cache,
            )
            worlds: dict[str, _StageWorldExecution] = {}
            for world in ml.NULL_WORLD_ORDER:
                mask = _world_member(controls, world)
                schedule = _world_member(schedules, world)
                if mask is not None:
                    worlds[world] = _StageWorldExecution(recipe, mask, schedule)
            sample_eligible = bool(controls.sample_eligible) and len(worlds) == 3
            mask_document = {
                "candidate_id": frozen.candidate_id,
                "candidate_kind": frozen.candidate_kind,
                "control_lattice_sha256": control_lattice.artifact_sha256,
                "control_masks": controls.as_dict(),
                "decision_dates": [item.isoformat() for item in state.plan.decision_dates],
                "recipe": recipe.as_dict(),
                "rule_schedules": schedules.as_dict(),
                "sample_eligible": sample_eligible,
                "schema": "systematic_fx.ai_all_cases_symbolic_oos_mask.v1",
                "stage_key": state.plan.stage_key,
                "structural_lattice_sha256": state.structural_lattice.artifact_sha256,
            }
            output.append(
                _StageCandidateMask(
                    frozen,
                    state.plan.stage_key,
                    mask_document,
                    None,
                    None,
                    worlds,
                    sample_eligible,
                )
            )
            continue

        typed, models = _model_candidate_and_models(frozen)
        if frozen.candidate_kind == "DIRECT_ML":
            if direct_bundles is None:
                direct_bundles = _direct_feature_bundles(
                    state,
                    required_coordinates=direct_coordinates,
                )
            bundle = direct_bundles[typed.decision_timeframe_seconds]
            feature_rows = bundle.feature_rows.for_feature_set(typed.feature_set_id)
            lineage_sha = _direct_oos_lineage_sha256(bundle, typed.horizon_seconds)
            schedule = ml.build_direct_outcome_free_execution_schedule(
                typed,
                feature_rows,
                partition_key=state.plan.stage_key,
                partition_date_certificate=partition_date_certificate,
                outcome_lineage_sha256=lineage_sha,
                opportunity_lattice=bundle.opportunity_lattice,
            )
            raw_masks = tuple(
                ml.freeze_prediction_mask(
                    model,
                    feature_rows,
                    partition_key=state.plan.stage_key,
                    execution_schedule=schedule,
                )
                for model in models
            )
            ineligibility = None
            try:
                aligned = ml.align_frozen_prediction_masks(*raw_masks)
            except ml.MLCandidateIneligible as error:
                aligned = None
                ineligibility = error.as_dict()
            sample_eligible = aligned is not None
            direct_controls: object = aligned if aligned is not None else raw_masks
            mask_document = {
                "aligned_control_proof": (None if aligned is None else aligned.proof.as_dict()),
                "candidate": typed.as_dict(),
                "candidate_id": frozen.candidate_id,
                "candidate_kind": frozen.candidate_kind,
                "decision_dates": [item.isoformat() for item in state.plan.decision_dates],
                "frozen_masks": [
                    item.as_dict()
                    for item in (
                        raw_masks
                        if aligned is None
                        else (aligned.real, aligned.circular_target, aligned.matched_target)
                    )
                ],
                "ineligibility": ineligibility,
                "sample_eligible": sample_eligible,
                "schema": "systematic_fx.ai_all_cases_direct_oos_mask.v1",
                "stage_key": state.plan.stage_key,
            }
            output.append(
                _StageCandidateMask(
                    frozen,
                    state.plan.stage_key,
                    mask_document,
                    direct_controls,
                    (typed.decision_timeframe_seconds, typed.horizon_seconds),
                    {},
                    sample_eligible,
                )
            )
            continue

        if frozen.candidate_kind != "META_ML" or control_lattice is None:
            raise AllCasesPipelineError("frozen candidate kind is unsupported")
        raw_worlds: dict[str, tuple[object, object, object, object, object]] = {}
        ineligibility_reason: str | None = None
        for world, model in zip(ml.NULL_WORLD_ORDER, models, strict=True):
            certificate = model.symbolic_ranking_certificate
            ranked = (
                None
                if certificate is None
                else certificate.strategy_at_rank(typed.symbolic_rank_slot)
            )
            recipe = None if ranked is None else recipe_by_id.get(ranked.strategy_id)
            policy = None if recipe is None else search_policies.get(recipe.anchor_policy_id)
            if recipe is None or policy is None or model.base_strategy_id != recipe.strategy_id:
                raise AllCasesPipelineError("meta Search-final rank-slot recipe binding differs")
            controls, _schedules = _stage_policy_controls(
                state,
                policy,
                control_lattice=control_lattice,
                cache=policy_cache,
            )
            base_mask = _world_member(controls, world)
            if base_mask is None:
                ineligibility_reason = "SYMBOLIC_NULL_CONTROL_INELIGIBLE"
                continue
            built = _meta_feature_gate(
                state,
                typed,
                model,
                recipe,
                policy,
                base_mask,
                meta_bars,
            )
            if built is None:
                ineligibility_reason = "INSUFFICIENT_CAUSAL_META_FEATURE_ROWS"
                continue
            gate, base_orders, feature_rows, retained_mask = built
            raw_worlds[world] = (
                gate,
                base_orders,
                retained_mask,
                recipe,
                feature_rows,
            )
        aligned_meta = None
        if tuple(raw_worlds) == ml.NULL_WORLD_ORDER:
            try:
                aligned_meta = ml.align_frozen_meta_anchor_gates(
                    *(raw_worlds[world][0] for world in ml.NULL_WORLD_ORDER)
                )
            except ml.MLCandidateIneligible as error:
                ineligibility_reason = str(error)
        sample_eligible = aligned_meta is not None
        gates_by_world = (
            {
                "REAL": aligned_meta.real,
                "CIRCULAR_TARGET": aligned_meta.circular_target,
                "MATCHED_TARGET": aligned_meta.matched_target,
            }
            if aligned_meta is not None
            else {world: values[0] for world, values in raw_worlds.items() if world == "REAL"}
        )
        executions: dict[str, _StageWorldExecution] = {}
        for world, gate in gates_by_world.items():
            _raw_gate, base_orders, retained_mask, recipe, _features = raw_worlds[world]
            executions[world] = _meta_admitted_execution(
                state, gate, base_orders, retained_mask, recipe
            )
        mask_document = {
            "aligned_control_proof": (
                None if aligned_meta is None else aligned_meta.proof.as_dict()
            ),
            "base_worlds": {
                world: {
                    "base_entry_order_batch_sha256": values[1].artifact_sha256,
                    "base_mask_sha256": values[2].mask_sha256,
                    "base_recipe": values[3].as_dict(),
                    "expert_artifact_commitment_sha256": (
                        values[4].expert_artifact_commitment_sha256
                    ),
                    "gate": gates_by_world[world].as_dict() if world in gates_by_world else None,
                }
                for world, values in raw_worlds.items()
            },
            "candidate": typed.as_dict(),
            "candidate_id": frozen.candidate_id,
            "candidate_kind": frozen.candidate_kind,
            "control_lattice_sha256": control_lattice.artifact_sha256,
            "decision_dates": [item.isoformat() for item in state.plan.decision_dates],
            "ineligibility_reason": ineligibility_reason,
            "sample_eligible": sample_eligible,
            "schema": "systematic_fx.ai_all_cases_meta_oos_mask.v1",
            "stage_key": state.plan.stage_key,
            "structural_lattice_sha256": state.structural_lattice.artifact_sha256,
        }
        output.append(
            _StageCandidateMask(
                frozen,
                state.plan.stage_key,
                mask_document,
                None,
                None,
                executions,
                sample_eligible,
            )
        )
    return tuple(output)


def _empty_world_evidence(decision_dates: Sequence[date]) -> _WorldPartitionEvidence:
    return _WorldPartitionEvidence(tuple((item.isoformat(), 0) for item in decision_dates), ())


def _direct_partition_evidence(
    state: _SearchFeatureState,
    runtime: _StageCandidateMask,
    response: _DirectOutcomeBundle,
) -> Mapping[str, _WorldPartitionEvidence | None]:
    """Apply already-frozen direct masks to one exact terminal response surface."""

    from . import ml

    def exact_integers(values: object, *, label: str) -> tuple[int, ...]:
        try:
            raw = tuple(values)  # type: ignore[arg-type]
        except TypeError as error:
            raise AllCasesPipelineError(f"{label} is not an integer sequence") from error
        if any(
            isinstance(value, bool)
            or not isinstance(value, Integral)
            or not -(2**63) <= int(value) < 2**63
            for value in raw
        ):
            raise AllCasesPipelineError(f"{label} contains a non-exact integer")
        return tuple(int(value) for value in raw)

    def exact_positive_uint64s(values: object, *, label: str) -> tuple[int, ...]:
        try:
            raw = tuple(values)  # type: ignore[arg-type]
        except TypeError as error:
            raise AllCasesPipelineError(f"{label} is not an integer sequence") from error
        if any(
            isinstance(value, bool) or not isinstance(value, Integral) or not 0 < int(value) < 2**64
            for value in raw
        ):
            raise AllCasesPipelineError(f"{label} contains a non-exact integer")
        return tuple(int(value) for value in raw)

    controls = runtime.direct_controls
    if controls is None:
        raise AllCasesPipelineError("direct mask runtime is absent")
    masks = (
        (controls.real, controls.circular_target, controls.matched_target)
        if hasattr(controls, "proof")
        else tuple(controls)
    )
    if len(masks) != 3 or tuple(item.null_world for item in masks) != ml.NULL_WORLD_ORDER:
        raise AllCasesPipelineError("direct frozen world order differs")
    first_schedule = masks[0].execution_schedule
    fill_ns = exact_integers(response.fill_ns, label="direct fill_ns")
    decision_ns = exact_integers(response.decision_ns, label="direct decision_ns")
    label_exit_ns = exact_integers(response.label_exit_ns, label="direct label_exit_ns")
    entry_ticks = exact_integers(response.entry_ticks, label="direct entry_ticks")
    terminal_ticks = exact_integers(response.terminal_ticks, label="direct terminal_ticks")
    outcome_span_ids = exact_integers(response.outcome_span_ids, label="direct outcome_span_ids")
    segment_ids = exact_positive_uint64s(response.segment_ids, label="direct segment_ids")
    valid_label_paths = tuple(response.valid_label_paths)
    if any(type(value) is not bool for value in valid_label_paths):
        raise AllCasesPipelineError("direct valid_label_paths contains a non-boolean")
    if (
        response.row_ids != first_schedule.row_ids
        or response.source_dates != first_schedule.decision_dates
        or decision_ns != first_schedule.decision_ns
        or fill_ns != first_schedule.entry_ns
        or label_exit_ns != first_schedule.planned_exit_ns
        or response.outcome_contracts != first_schedule.contracts
        or outcome_span_ids != first_schedule.outcome_span_ids
        or segment_ids != first_schedule.segment_ids
        or response.outcome_lineage_sha256 != first_schedule.lineage_sha256
        or response.entry_schedule_sha256 != first_schedule.entry_schedule_sha256
        or response.opportunity_lattice_sha256 != first_schedule.opportunity_lattice_sha256
        or runtime.direct_response_coordinate
        != (response.decision_timeframe_seconds, response.horizon_seconds)
    ):
        raise AllCasesPipelineError("direct OOS response/mask lineage differs")
    if any(item.execution_schedule != first_schedule for item in masks[1:]):
        raise AllCasesPipelineError("direct mask schedules differ across worlds")
    moves = tuple(
        terminal - entry for terminal, entry in zip(terminal_ticks, entry_ticks, strict=True)
    )
    typed_outcomes = {
        world: ml.build_frozen_resolved_outcome_rows(
            mask.execution_schedule,
            response_kind="DIRECT_TERMINAL_MOVE_TICKS",
            row_ids=response.row_ids,
            actual_exit_ns=response.label_exit_ns,
            realized_values=moves,
            valid_label_paths=response.valid_label_paths,
            outcome_contracts=response.outcome_contracts,
            outcome_span_ids=response.outcome_span_ids,
            segment_ids=response.segment_ids,
            outcome_lineage_sha256=response.outcome_lineage_sha256,
            opportunity_lattice_sha256=response.opportunity_lattice_sha256,
        )
        for world, mask in zip(ml.NULL_WORLD_ORDER, masks, strict=True)
    }
    if runtime.sample_eligible:
        ml.evaluate_aligned_frozen_controls(controls, typed_outcomes)
    output: dict[str, _WorldPartitionEvidence | None] = {}
    for world, mask in zip(ml.NULL_WORLD_ORDER, masks, strict=True):
        if world != "REAL" and not runtime.sample_eligible:
            output[world] = None
            continue
        schedule = mask.execution_schedule
        daily = {item.isoformat(): 0 for item in state.plan.decision_dates}
        trades = []
        for index, admitted in enumerate(mask.predictions.admitted):
            if not admitted:
                continue
            direction = mask.predictions.directions[index]
            if direction is ml.TradeDirection.FLAT:
                raise AllCasesPipelineError("admitted direct action is flat")
            signed = 1 if direction is ml.TradeDirection.LONG else -1
            net = signed * moves[index] - ml.TOTAL_FRICTION_TICKS
            source_date = schedule.decision_dates[index].isoformat()
            if source_date not in daily:
                raise AllCasesPipelineError("direct action escapes the frozen decision dates")
            daily[source_date] += net
            trades.append(
                _filled_trade_evidence(
                    candidate_id=runtime.candidate.candidate_id,
                    stage_key=state.plan.stage_key,
                    world=world,
                    source_identity={"direct_row_id": schedule.row_ids[index]},
                    decision_date=source_date,
                    decision_ns=int(schedule.decision_ns[index]),
                    entry_ns=int(schedule.entry_ns[index]),
                    exit_ns=int(schedule.planned_exit_ns[index]),
                    contract=schedule.contracts[index],
                    outcome_span_id=int(schedule.outcome_span_ids[index]),
                    segment_id=int(schedule.segment_ids[index]),
                    direction=direction.value,
                    net_ticks=net,
                )
            )
        output[world] = _WorldPartitionEvidence(
            tuple(sorted(daily.items())),
            tuple(sorted(trades, key=lambda item: item.sort_key)),
        )
    return output


def _symbolic_partition_evidence(
    project_root: Path,
    state: _SearchFeatureState,
    runtimes: Sequence[_StageCandidateMask],
) -> Mapping[str, Mapping[str, _WorldPartitionEvidence | None]]:
    """Evaluate all frozen symbolic/meta masks in one shared span stream."""

    if not runtimes:
        return {}

    from . import ml, symbolic

    request_rows: list[tuple[str, str, object]] = []
    for runtime in runtimes:
        for world, execution in runtime.symbolic_worlds.items():
            symbolic_world = {
                "REAL": "REAL",
                "CIRCULAR_TARGET": "CIRCULAR",
                "MATCHED_TARGET": "MATCHED",
            }[world]
            request = symbolic.SelectedStrategyDetailRequest(
                1,
                f"{state.plan.stage_key}:{runtime.candidate.candidate_id}",
                symbolic_world,
                execution.recipe,
                execution.mask,
                execution.rule_schedule,
            )
            request_rows.append((runtime.candidate.candidate_id, world, request))
    parts: dict[tuple[str, str], list[object]] = {
        (candidate_id, world): [] for candidate_id, world, _request in request_rows
    }
    for paths in _one_second_path_parts(project_root, state.plan):
        evaluator = symbolic.SharedPathEvaluator(paths)
        for start in range(0, len(request_rows), 24):
            batch = request_rows[start : start + 24]
            if not batch:
                continue
            detailed = symbolic.evaluate_selected_strategy_details(
                evaluator,
                tuple(item[2] for item in batch),
                reporting_group_by_date=state.plan.reporting_group_by_date,
                outer_validation_by_date=state.plan.reporting_group_by_date,
            )
            if len(detailed) != len(batch):
                raise AllCasesPipelineError("symbolic OOS detail batch cardinality differs")
            for coordinate, value in zip(batch, detailed, strict=True):
                parts[coordinate[:2]].append(value)
    output: dict[str, dict[str, _WorldPartitionEvidence | None]] = {}
    request_by_coordinate = {
        (candidate_id, world): request for candidate_id, world, request in request_rows
    }
    for runtime in runtimes:
        worlds: dict[str, _WorldPartitionEvidence | None] = {}
        for world in ml.NULL_WORLD_ORDER:
            coordinate = runtime.candidate.candidate_id, world
            request = request_by_coordinate.get(coordinate)
            if request is None:
                worlds[world] = (
                    None if world != "REAL" else _empty_world_evidence(state.plan.decision_dates)
                )
                continue
            rows = tuple(row for detail in parts[coordinate] for row in detail.rows)
            expected_keys = {item.outcome_key for item in request.mask.records}
            observed_keys = {item.anchor_key for item in rows}
            if (
                observed_keys != expected_keys
                or len(rows) != len(expected_keys)
                or any(item.censored for item in rows)
            ):
                raise AllCasesPipelineError(
                    "frozen symbolic OOS mask is missing, duplicated, or censored"
                )
            daily = {item.isoformat(): 0 for item in state.plan.decision_dates}
            trades = []
            for row in rows:
                if row.status != "FILLED":
                    continue
                if row.entry_ns is None or row.net_pnl_ticks is None:
                    raise AllCasesPipelineError("filled symbolic OOS row is incomplete")
                day = row.source_date.isoformat()
                if day not in daily:
                    raise AllCasesPipelineError("symbolic action escapes the frozen decision dates")
                net = int(row.net_pnl_ticks)
                daily[day] += net
                if row.exit_ns is None:
                    raise AllCasesPipelineError("filled symbolic OOS row lacks an exit")
                trades.append(
                    _filled_trade_evidence(
                        candidate_id=runtime.candidate.candidate_id,
                        stage_key=state.plan.stage_key,
                        world=world,
                        source_identity={
                            "anchor_key": list(row.anchor_key),
                            "recipe_id": request.recipe.strategy_id,
                        },
                        decision_date=day,
                        decision_ns=int(row.anchor_key[3]),
                        entry_ns=int(row.entry_ns),
                        exit_ns=int(row.exit_ns),
                        contract=str(row.anchor_key[0]),
                        outcome_span_id=int(row.anchor_key[1]),
                        segment_id=int(row.anchor_key[2]),
                        direction=str(row.direction),
                        net_ticks=net,
                    )
                )
            worlds[world] = _WorldPartitionEvidence(
                tuple(sorted(daily.items())),
                tuple(sorted(trades, key=lambda item: item.sort_key)),
            )
        if not runtime.sample_eligible:
            worlds["CIRCULAR_TARGET"] = None
            worlds["MATCHED_TARGET"] = None
        output[runtime.candidate.candidate_id] = worlds
    return output


def _evaluate_stage_candidate_masks(
    project_root: Path,
    state: _SearchFeatureState,
    runtimes: Sequence[_StageCandidateMask],
) -> tuple[_CandidatePartitionEvidence, ...]:
    """Open one stage's outcomes only after all of its mask commitments exist."""

    direct_runtimes = tuple(
        item for item in runtimes if item.candidate.candidate_kind == "DIRECT_ML"
    )
    symbolic_runtimes = tuple(
        item for item in runtimes if item.candidate.candidate_kind != "DIRECT_ML"
    )
    symbolic_values = _symbolic_partition_evidence(project_root, state, symbolic_runtimes)
    direct_values: dict[str, Mapping[str, _WorldPartitionEvidence | None]] = {}
    if direct_runtimes:
        direct_coordinates = {
            coordinate
            for runtime in direct_runtimes
            for coordinate in (runtime.direct_response_coordinate,)
            if coordinate is not None
        }
        if len(direct_coordinates) == 0 or any(
            runtime.direct_response_coordinate is None for runtime in direct_runtimes
        ):
            raise AllCasesPipelineError("direct response coordinate is absent")
        features = _direct_feature_bundles(
            state,
            required_coordinates=direct_coordinates,
        )
        outcomes = _direct_outcome_bundles(
            project_root,
            state,
            features,
            required_coordinates=direct_coordinates,
        )
        for runtime in direct_runtimes:
            coordinate = runtime.direct_response_coordinate
            if coordinate is None:  # pragma: no cover - checked above
                raise AllCasesPipelineError("direct response coordinate is absent")
            direct_values[runtime.candidate.candidate_id] = _direct_partition_evidence(
                state, runtime, outcomes[coordinate]
            )
    output = []
    for runtime in runtimes:
        worlds = (
            direct_values[runtime.candidate.candidate_id]
            if runtime.candidate.candidate_kind == "DIRECT_ML"
            else symbolic_values[runtime.candidate.candidate_id]
        )
        output.append(
            _CandidatePartitionEvidence(
                runtime.candidate,
                state.plan.stage_key,
                runtime.sample_eligible,
                worlds,
            )
        )
    return tuple(output)


def _fraction_document(value: Fraction | None) -> dict[str, int] | None:
    return (
        None if value is None else {"denominator": value.denominator, "numerator": value.numerator}
    )


def _maximum_drawdown(values: Sequence[int]) -> int:
    equity = 0
    peak = 0
    maximum = 0
    for value in values:
        equity += value
        peak = max(peak, equity)
        maximum = max(maximum, peak - equity)
    return maximum


def _profit_factor(values: Sequence[int], *, finite_infinity: bool = False) -> Fraction | None:
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    if losses == 0:
        if finite_infinity:
            return Fraction(10**18, 1) if gains > 0 else Fraction(0, 1)
        return None
    return Fraction(gains, losses)


def _daily_world_document(
    value: _WorldPartitionEvidence | None,
) -> list[dict[str, object]] | None:
    return (
        None
        if value is None
        else [{"decision_date": day, "net_ticks": net} for day, net in value.daily_net_ticks]
    )


def _filled_trade_summary_world_document(
    value: _WorldPartitionEvidence | None,
) -> list[dict[str, object]] | None:
    """Return one exact sufficient-statistic row per frozen decision date.

    The full trade rows remain private transient state.  The durable result keeps
    the algebra needed to replay fills, active days, contracts, PF, and
    chronological maximum drawdown, plus a commitment to the ordered trade
    identities.  This bounds evidence size by the frozen date domain rather than
    by the number of fills.
    """

    if value is None:
        return None
    by_date: dict[str, list[_FilledTradeEvidence]] = {
        day: [] for day, _net in value.daily_net_ticks
    }
    for trade in value.filled_trades:
        try:
            by_date[trade.decision_date].append(trade)
        except KeyError as error:
            raise AllCasesPipelineError("filled trade escapes its frozen date domain") from error
    output: list[dict[str, object]] = []
    ordered_trades: list[_FilledTradeEvidence] = []
    for decision_date, expected_net in value.daily_net_ticks:
        trades = tuple(sorted(by_date[decision_date], key=lambda item: item.sort_key))
        ordered_trades.extend(trades)
        net_values = tuple(item.net_ticks for item in trades)
        total = sum(net_values)
        if total != expected_net:
            raise AllCasesPipelineError("filled-trade daily total differs")
        equity = 0
        maximum_prefix = 0
        minimum_prefix = 0
        peak = 0
        maximum_drawdown = 0
        for net in net_values:
            equity += net
            maximum_prefix = max(maximum_prefix, equity)
            minimum_prefix = min(minimum_prefix, equity)
            peak = max(peak, equity)
            maximum_drawdown = max(maximum_drawdown, peak - equity)
        output.append(
            {
                "contract_ids": sorted({item.contract for item in trades}),
                "decision_date": decision_date,
                "equity_maximum_prefix_ticks": maximum_prefix,
                "equity_minimum_prefix_ticks": minimum_prefix,
                "equity_total_ticks": total,
                "fill_count": len(trades),
                "gross_gain_ticks": sum(item for item in net_values if item > 0),
                "gross_loss_ticks": -sum(item for item in net_values if item < 0),
                "maximum_drawdown_ticks": maximum_drawdown,
                "trade_identity_root_sha256": _sha256([item.trade_sha256 for item in trades]),
            }
        )
    if tuple(ordered_trades) != value.filled_trades:
        raise AllCasesPipelineError(
            "filled trades are not in frozen-date chronological identity order"
        )
    return output


def _candidate_descriptor_document(candidate: _FrozenSearchCandidate) -> dict[str, object]:
    return {
        "candidate_id": candidate.candidate_id,
        "candidate_kind": candidate.candidate_kind,
        "catalog_selection_rank": candidate.catalog_selection_rank,
        "family_key": candidate.family_key,
        "frozen_artifact_sha256": _sha256(candidate.document),
    }


def _aggregate_candidate_partitions(
    parts: Sequence[_CandidatePartitionEvidence],
    *,
    label: str,
) -> dict[str, object]:
    """Build the exact evidence document consumed by the governed validators."""

    from . import run

    values = tuple(parts)
    if not values or label not in {"walk-forward", "holdout"}:
        raise AllCasesPipelineError("candidate partition aggregate scope differs")
    candidate = values[0].candidate
    if any(item.candidate != candidate for item in values):
        raise AllCasesPipelineError("candidate partition identities differ")
    sample_eligible = all(item.sample_eligible for item in values)
    combined_worlds: dict[str, _WorldPartitionEvidence | None] = {}
    for world in ("REAL", "CIRCULAR_TARGET", "MATCHED_TARGET"):
        world_parts = tuple(item.worlds[world] for item in values)
        if world != "REAL" and not sample_eligible:
            combined_worlds[world] = None
            continue
        if any(item is None for item in world_parts):
            raise AllCasesPipelineError("eligible OOS control world is absent")
        present = tuple(item for item in world_parts if item is not None)
        daily = tuple(row for item in present for row in item.daily_net_ticks)
        if daily != tuple(sorted(daily)) or len({day for day, _net in daily}) != len(daily):
            raise AllCasesPipelineError("OOS partition daily domains overlap or reorder")
        trades = tuple(
            sorted(
                (row for item in present for row in item.filled_trades),
                key=lambda item: item.sort_key,
            )
        )
        if any(
            row.candidate_id != candidate.candidate_id
            or row.world != world
            or row.stage_key != part.stage_key
            for part in values
            for row in (part.worlds[world].filled_trades if part.worlds[world] is not None else ())
        ):
            raise AllCasesPipelineError("OOS filled-trade candidate/world binding differs")
        if len({row.trade_sha256 for row in trades}) != len(trades):
            raise AllCasesPipelineError("OOS filled-trade identity is duplicated")
        combined_worlds[world] = _WorldPartitionEvidence(daily, trades)
    real = combined_worlds["REAL"]
    if real is None:  # pragma: no cover - REAL is always materialized
        raise AllCasesPipelineError("REAL OOS evidence is absent")
    net_values = tuple(item.net_ticks for item in real.filled_trades)
    active_days = len({item.decision_date for item in real.filled_trades})
    contracts = len({item.contract for item in real.filled_trades})
    common_evidence: dict[str, object] = {
        "active_entry_days": active_days,
        "contract_count": contracts,
        "fill_count": len(net_values),
        "maximum_drawdown_ticks": _maximum_drawdown(net_values),
        "net_ticks": sum(net_values),
        "profit_factor": _fraction_document(_profit_factor(net_values)),
    }
    if label == "walk-forward":
        fold_trades = tuple(
            tuple(item.net_ticks for item in part.worlds["REAL"].filled_trades)  # type: ignore[union-attr]
            for part in values
        )
        fold_nets = tuple(sum(item) for item in fold_trades)
        fold_fills = tuple(len(item) for item in fold_trades)
        positive = tuple(value for value in fold_nets if value > 0)
        losses = tuple(-value for value in fold_nets if value < 0)
        median_positive = (
            None
            if not positive
            else Fraction(sorted(positive)[len(positive) // 2], 1)
            if len(positive) % 2
            else Fraction(
                sorted(positive)[len(positive) // 2 - 1] + sorted(positive)[len(positive) // 2],
                2,
            )
        )
        loss_ratio = (
            Fraction(0, 1)
            if not losses
            else None
            if median_positive is None
            else Fraction(max(losses), 1) / median_positive
        )
        common_evidence.update(
            {
                "fold_active_entry_days": [
                    len({row.decision_date for row in part.worlds["REAL"].filled_trades})  # type: ignore[union-attr]
                    for part in values
                ],
                "fold_fill_counts": list(fold_fills),
                "fold_net_ticks": list(fold_nets),
                "worst_fold_ev_ticks": _fraction_document(
                    min(
                        (
                            Fraction(net, fills) if fills else Fraction(-(10**18), 1)
                            for net, fills in zip(fold_nets, fold_fills, strict=True)
                        ),
                        default=Fraction(-(10**18), 1),
                    )
                ),
                "worst_fold_profit_factor": _fraction_document(
                    min(
                        (_profit_factor(item, finite_infinity=True) for item in fold_trades),
                        default=Fraction(0, 1),
                    )
                ),
                "worst_loss_over_median_positive": _fraction_document(loss_ratio),
            }
        )
    else:
        daily = dict(real.daily_net_ticks)
        half_nets = [0, 0]
        for index, (day, net) in enumerate(sorted(daily.items())):
            half_nets[0 if index < 60 else 1] += net
        drawdown = int(common_evidence["maximum_drawdown_ticks"])
        common_evidence.update(
            {
                "half_net_ticks": half_nets,
                "net_over_maximum_drawdown": _fraction_document(
                    None if drawdown == 0 else Fraction(int(common_evidence["net_ticks"]), drawdown)
                ),
            }
        )
    daily_document = {
        world: _daily_world_document(combined_worlds[world])
        for world in ("REAL", "CIRCULAR_TARGET", "MATCHED_TARGET")
    }
    filled_trade_summary_document = {
        world: _filled_trade_summary_world_document(combined_worlds[world])
        for world in ("REAL", "CIRCULAR_TARGET", "MATCHED_TARGET")
    }
    real_daily = dict(real.daily_net_ticks)
    circular = combined_worlds["CIRCULAR_TARGET"]
    matched = combined_worlds["MATCHED_TARGET"]
    p_star = _daily_p_star(
        tuple(real_daily),
        real_daily,
        None if circular is None else dict(circular.daily_net_ticks),
        None if matched is None else dict(matched.daily_net_ticks),
        eligible=sample_eligible,
    )
    document: dict[str, object] = {
        "candidate_descriptor": _candidate_descriptor_document(candidate),
        "candidate_kind": candidate.candidate_kind,
        "catalog_selection_rank": candidate.catalog_selection_rank,
        "daily_net_ticks_by_world": daily_document,
        "economic_gate_pass": False,
        "evidence": common_evidence,
        "failure_reasons": [],
        "filled_trade_summaries_by_world": filled_trade_summary_document,
        "p_star": _fraction_document(p_star),
        "sample_eligible": sample_eligible,
    }
    expected_gate, reasons = (
        run._expected_walk_economics(document)
        if label == "walk-forward"
        else run._expected_holdout_economics(document)
    )
    document["economic_gate_pass"] = expected_gate
    document["failure_reasons"] = list(reasons)
    return document


def _candidate_result_row(candidate_id: str, document: Mapping[str, object]) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "result_document": dict(document),
        "result_sha256": _sha256(document),
    }


def _walk_result_payload(
    candidate_ids: tuple[str, ...],
    by_candidate: Mapping[str, Sequence[_CandidatePartitionEvidence]],
) -> dict[str, object]:
    base = {
        candidate_id: _aggregate_candidate_partitions(
            by_candidate[candidate_id], label="walk-forward"
        )
        for candidate_id in candidate_ids
    }
    p_values = {
        candidate_id: Fraction(
            base[candidate_id]["p_star"]["numerator"],  # type: ignore[index]
            base[candidate_id]["p_star"]["denominator"],  # type: ignore[index]
        )
        for candidate_id in candidate_ids
    }
    rejected = set(_benjamini_hochberg_rejections(candidate_ids, p_values, q=Fraction(1, 20)))
    eligible = []
    for candidate_id in candidate_ids:
        document = base[candidate_id]
        document["bh_rejected"] = candidate_id in rejected
        document["selected_before_budget"] = bool(
            candidate_id in rejected
            and document["sample_eligible"]
            and document["economic_gate_pass"]
        )
        document["selected_finalist"] = False
        document["finalist_rank"] = None
        if document["selected_before_budget"]:
            eligible.append(candidate_id)

    def rank_key(candidate_id: str) -> tuple[object, ...]:
        document = base[candidate_id]
        evidence = document["evidence"]
        net = int(evidence["net_ticks"])  # type: ignore[index]
        fills = int(evidence["fill_count"])  # type: ignore[index]
        aggregate_ev = Fraction(net, fills) if fills else Fraction(-(10**18), 1)
        profit_raw = evidence["profit_factor"]  # type: ignore[index]
        profit = (
            None
            if profit_raw is None
            else Fraction(profit_raw["numerator"], profit_raw["denominator"])
        )
        worst = evidence["worst_fold_ev_ticks"]  # type: ignore[index]
        return (
            p_values[candidate_id],
            -Fraction(worst["numerator"], worst["denominator"]),
            -aggregate_ev,
            0 if profit is None and net > 0 else 1,
            -(profit or Fraction(0, 1)),
            int(document["catalog_selection_rank"]),
            candidate_id,
        )

    finalists = tuple(sorted(eligible, key=rank_key)[:3])
    for rank, candidate_id in enumerate(finalists, start=1):
        base[candidate_id]["selected_finalist"] = True
        base[candidate_id]["finalist_rank"] = rank
    rows = [
        _candidate_result_row(candidate_id, base[candidate_id]) for candidate_id in candidate_ids
    ]
    multiplicity = {
        "candidate_results": rows,
        "finalist_candidate_ids": list(finalists),
        "method": "BENJAMINI_HOCHBERG",
    }
    return {
        "all_folds_complete": True,
        "candidate_ids": list(candidate_ids),
        "candidate_results": rows,
        "finalist_candidate_ids": list(finalists),
        "fold_keys": ["WF1", "WF2", "WF3", "WF4", "WF5"],
        "multiplicity_sha256": _sha256(multiplicity),
        "schema": "systematic_fx.ai_all_cases_walk_forward_result_payload.v1",
    }


def _holdout_result_payload(
    candidate_ids: tuple[str, ...],
    values: Sequence[_CandidatePartitionEvidence],
) -> dict[str, object]:
    by_id = {item.candidate.candidate_id: item for item in values}
    if tuple(by_id) != candidate_ids:
        raise AllCasesPipelineError("holdout candidate order differs")
    base = {
        candidate_id: _aggregate_candidate_partitions((by_id[candidate_id],), label="holdout")
        for candidate_id in candidate_ids
    }
    p_values = {
        candidate_id: Fraction(
            base[candidate_id]["p_star"]["numerator"],  # type: ignore[index]
            base[candidate_id]["p_star"]["denominator"],  # type: ignore[index]
        )
        for candidate_id in candidate_ids
    }
    rejected = set(_holm_rejections(candidate_ids, p_values, alpha=Fraction(1, 20)))
    for candidate_id in candidate_ids:
        document = base[candidate_id]
        document["holm_rejected"] = candidate_id in rejected
        document["verdict_pass"] = bool(
            candidate_id in rejected
            and document["sample_eligible"]
            and document["economic_gate_pass"]
        )
    any_pass = any(bool(item["verdict_pass"]) for item in base.values())
    every_ineligible = all(not bool(item["sample_eligible"]) for item in base.values())
    classification = (
        "ONE_SHOT_UNSEALED_ALL_CASES_HOLDOUT_DIAGNOSTIC_PASS"
        if any_pass
        else "ONE_SHOT_UNSEALED_ALL_CASES_HOLDOUT_DIAGNOSTIC_INCONCLUSIVE"
        if every_ineligible
        else "ONE_SHOT_UNSEALED_ALL_CASES_HOLDOUT_DIAGNOSTIC_FAIL"
    )
    rows = [
        _candidate_result_row(candidate_id, base[candidate_id]) for candidate_id in candidate_ids
    ]
    return {
        "candidate_ids": list(candidate_ids),
        "candidate_results": rows,
        "classification": classification,
        "holm_sha256": _sha256(
            {
                "candidate_results": rows,
                "classification": classification,
                "method": "HOLM_STEP_DOWN",
            }
        ),
        "schema": "systematic_fx.ai_all_cases_holdout_result_payload.v1",
    }


def _train_select_search(
    project_root: Path,
    config: AllCasesConfig,
    universe: Mapping[str, object],
    *,
    verify_only: bool = False,
    allow_incomplete_verify: bool = False,
    resources: _PipelineResourceGuard | None = None,
) -> object:
    if universe.get("schema") != "systematic_fx.ai_all_cases_search_universe_payload.v1":
        raise AllCasesPipelineError("Search universe payload schema differs")
    ledger = (
        _SearchSubledger(
            project_root,
            config,
            create=not verify_only,
            resources=resources,
            allow_incomplete_verify=allow_incomplete_verify,
        )
        if resources is not None
        else _SearchSubledger(
            project_root,
            config,
            create=not verify_only,
            allow_incomplete_verify=allow_incomplete_verify,
        )
    )
    ledger.verify()
    state = _search_feature_state(project_root)
    _surfaces, stage_a_selection, _score_chunks = _ensure_stage_a_search(
        project_root,
        config,
        universe,
        ledger,
        state,
        verify_only=verify_only,
    )
    stage_b_plan = _ensure_stage_b_feature_plan(
        config,
        ledger,
        state,
        stage_a_selection,
        verify_only=verify_only,
    )
    stage_b_evidence = _ensure_stage_b_search_evidence(
        project_root,
        config,
        ledger,
        state,
        stage_b_plan,
        verify_only=verify_only,
    )
    symbolic_rows, symbolic_artifacts = _symbolic_search_candidate_rows(
        project_root,
        state,
        stage_b_plan,
        stage_b_evidence,
    )
    direct_evidence = _ensure_direct_ml_search(
        project_root,
        config,
        ledger,
        state,
        universe,
        verify_only=verify_only,
    )
    meta_plan = _ensure_meta_feature_plan(
        config,
        ledger,
        state,
        stage_b_plan,
        stage_b_evidence,
        symbolic_rows,
        symbolic_artifacts,
        verify_only=verify_only,
    )
    meta_evidence = _ensure_meta_ml_search(
        project_root,
        config,
        ledger,
        state,
        stage_b_plan,
        stage_b_evidence,
        meta_plan,
        verify_only=verify_only,
    )
    return _finalize_search_result(
        config,
        universe,
        ledger,
        stage_a_selection,
        stage_b_plan,
        stage_b_evidence,
        symbolic_rows,
        symbolic_artifacts,
        direct_evidence,
        meta_evidence,
        verify_only=verify_only,
    )


def _freeze_walk_forward_masks(
    project_root: Path,
    config: AllCasesConfig,
    candidate_ids: tuple[str, ...],
    search: Mapping[str, object],
) -> object:
    candidates = _frozen_search_candidates(search, candidate_ids)
    plans = _plans(project_root).walk_forward
    by_stage: dict[str, tuple[_StageCandidateMask, ...]] = {}
    for plan in plans:
        state = _stage_feature_state(project_root, plan)
        by_stage[plan.stage_key] = _freeze_stage_candidate_masks(
            project_root, config, state, candidates, search
        )
    documents = [
        {
            "candidate_id": candidate.candidate_id,
            "fold_key": plan.stage_key,
            "mask_kind": candidate.candidate_kind,
            "mask_sha256": next(
                item.mask_sha256
                for item in by_stage[plan.stage_key]
                if item.candidate.candidate_id == candidate.candidate_id
            ),
        }
        for candidate in candidates
        for plan in plans
    ]
    return {
        "candidate_ids": list(candidate_ids),
        "fold_keys": [item.stage_key for item in plans],
        "mask_commitment_sha256": _sha256(documents),
        "mask_documents": documents,
        "schema": "systematic_fx.ai_all_cases_walk_forward_masks_payload.v1",
    }


def _evaluate_walk_forward(
    project_root: Path,
    config: AllCasesConfig,
    candidate_ids: tuple[str, ...],
    masks: Mapping[str, object],
    search: Mapping[str, object],
) -> object:
    candidates = _frozen_search_candidates(search, candidate_ids)
    plans = _plans(project_root).walk_forward
    states_and_masks = []
    for plan in plans:
        state = _stage_feature_state(project_root, plan)
        frozen = _freeze_stage_candidate_masks(project_root, config, state, candidates, search)
        states_and_masks.append((state, frozen))
    expected = [
        {
            "candidate_id": candidate.candidate_id,
            "fold_key": state.plan.stage_key,
            "mask_kind": candidate.candidate_kind,
            "mask_sha256": next(
                item.mask_sha256
                for item in frozen
                if item.candidate.candidate_id == candidate.candidate_id
            ),
        }
        for candidate in candidates
        for state, frozen in states_and_masks
    ]
    if (
        masks.get("candidate_ids") != list(candidate_ids)
        or masks.get("fold_keys") != [item.stage_key for item in plans]
        or masks.get("mask_documents") != expected
        or masks.get("mask_commitment_sha256") != _sha256(expected)
    ):
        raise AllCasesPipelineError("walk-forward masks differ on feature-only replay")
    by_candidate: dict[str, list[_CandidatePartitionEvidence]] = {
        item.candidate_id: [] for item in candidates
    }
    # No call below occurs until all five folds/all candidates above have replayed.
    for state, frozen in states_and_masks:
        evaluated = _evaluate_stage_candidate_masks(project_root, state, frozen)
        for item in evaluated:
            by_candidate[item.candidate.candidate_id].append(item)
    return _walk_result_payload(candidate_ids, by_candidate)


def _freeze_holdout_masks(
    project_root: Path,
    config: AllCasesConfig,
    candidate_ids: tuple[str, ...],
    walk: Mapping[str, object],
) -> object:
    if walk.get("finalist_candidate_ids") != list(candidate_ids):
        raise AllCasesPipelineError("holdout family differs from the WF release")
    search = _persisted_search_payload(project_root, config)
    candidates = _frozen_search_candidates(search, candidate_ids)
    plan = _plans(project_root).holdout
    state = _stage_feature_state(project_root, plan)
    frozen = _freeze_stage_candidate_masks(project_root, config, state, candidates, search)
    documents = [
        {
            "candidate_id": item.candidate.candidate_id,
            "mask_kind": item.candidate.candidate_kind,
            "mask_sha256": item.mask_sha256,
        }
        for item in frozen
    ]
    return {
        "candidate_ids": list(candidate_ids),
        "mask_commitment_sha256": _sha256(documents),
        "mask_documents": documents,
        "schema": "systematic_fx.ai_all_cases_holdout_masks_payload.v1",
    }


def _evaluate_holdout(
    project_root: Path,
    config: AllCasesConfig,
    candidate_ids: tuple[str, ...],
    masks: Mapping[str, object],
) -> object:
    search = _persisted_search_payload(project_root, config)
    candidates = _frozen_search_candidates(search, candidate_ids)
    plan = _plans(project_root).holdout
    state = _stage_feature_state(project_root, plan)
    frozen = _freeze_stage_candidate_masks(project_root, config, state, candidates, search)
    expected = [
        {
            "candidate_id": item.candidate.candidate_id,
            "mask_kind": item.candidate.candidate_kind,
            "mask_sha256": item.mask_sha256,
        }
        for item in frozen
    ]
    if (
        masks.get("candidate_ids") != list(candidate_ids)
        or masks.get("mask_documents") != expected
        or masks.get("mask_commitment_sha256") != _sha256(expected)
    ):
        raise AllCasesPipelineError("holdout masks differ on feature-only replay")
    evaluated = _evaluate_stage_candidate_masks(project_root, state, frozen)
    return _holdout_result_payload(candidate_ids, evaluated)
