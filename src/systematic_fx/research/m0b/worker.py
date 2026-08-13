"""Deterministic, crash-resumable M0b candidate worker.

Only precomputed search-data labels and a precommitted signal artifact are
read.  Each checkpoint is immutable and content addressed; a small atomic
pointer makes a completed shard range visible.  A crash before pointer publish
therefore replays the same range and produces the same bytes.

This module intentionally has no LLM, holdout, paper/live, or order-routing
surface.  Its strongest classification is a search-data survivor awaiting an
independent gate.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Protocol

from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.m0b.first_passage_store import (
    FirstPassageStore,
    FirstPassageStoreError,
    _file_sha256,
    _fsync_directory,
    _leaf,
    _publish_immutable,
    _sha256,
    load_first_passage_store,
)
from systematic_fx.research.m0b.materialize import _safe_root

SIGNAL_SCHEMA: Final = "systematic_fx.m0b_candidate_signal.v1"
CHECKPOINT_SCHEMA: Final = "systematic_fx.m0b_checkpoint.v1"
WORKER_STATE_SCHEMA: Final = "systematic_fx.m0b_worker_state.v1"
TRADE_SHARD_SCHEMA: Final = "systematic_fx.m0b_sequential_trade_shard.v1"
RESULT_SCHEMA: Final = "systematic_fx.m0b_candidate_result.v1"
POINTER_SCHEMA: Final = "systematic_fx.m0b_worker_pointer.v1"
_NS: Final = 1_000_000_000
_OUTCOMES: Final = {"TP_FIRST", "SL_FIRST", "TIMEOUT"}
_WRITE_BITS: Final = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
_MAX_CHECKPOINT_SHARD_INTERVAL: Final = 100_000
_MAX_BARRIER_COMPONENT: Final = 1_000_000
_MAX_COOLDOWN_NS: Final = 31_536_000_000_000_000
_MAX_HOLD_SECONDS: Final = 31_536_000
_MAX_SEARCH_FOLDS: Final = 10_000
_MAX_SIGNALS: Final = 1_000_000
_MAX_STRESS_EXTRA_COST_TICKS: Final = 1_000_000
_EXECUTION_ASSUMPTIONS: Final = {
    "entry": "NEXT_ELIGIBLE_QUOTE_WITH_PRECOMMITTED_ADVERSE_TICKS",
    "latency": "SINGLE_CONSERVATIVE_SCREENING_ASSUMPTION",
    "long_entry_exit": "ASK_THEN_BID",
    "one_position": True,
    "passive_tp_fill": "POST_ENTRY_AGGRESSOR_TRADE_THROUGH_ONLY",
    "short_entry_exit": "BID_THEN_ASK",
    "stop": "MARKETABLE_EXECUTION",
    "timeout": "EXACT_EVENT_TS_PLUS_MAX_HOLD",
}


class M0bWorkerError(FirstPassageStoreError):
    """A precommitment, checkpoint, or deterministic evaluation failed closed."""


def _integer(value: object, *, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise M0bWorkerError(f"{label} must be an integer >= {minimum}")
    return value


def _bounded_integer(
    value: object,
    *,
    label: str,
    minimum: int,
    maximum: int,
) -> int:
    observed = _integer(value, label=label, minimum=minimum)
    if observed > maximum:
        raise M0bWorkerError(f"{label} exceeds the governed maximum {maximum}")
    return observed


def _nonempty(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise M0bWorkerError(f"{label} must be a canonical non-empty string")
    return value


@dataclass(frozen=True, slots=True)
class SignalArtifact:
    candidate_sha256: str
    feature_sha256: str
    row_count: int
    byte_size: int
    content_sha256: str
    relative_uri: str

    def __post_init__(self) -> None:
        _sha256(self.candidate_sha256, label="signal candidate_sha256")
        _sha256(self.feature_sha256, label="signal feature_sha256")
        _sha256(self.content_sha256, label="signal content_sha256")
        _integer(self.row_count, label="signal row_count")
        _integer(self.byte_size, label="signal byte_size")
        _leaf(self.relative_uri, label="signal relative_uri")
        if self.relative_uri != f"candidate-signals-{self.content_sha256}.jsonl":
            raise M0bWorkerError("signal URI is not content addressed")

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": "systematic_fx.m0b_signal_artifact.v1",
            "byte_size": self.byte_size,
            "candidate_sha256": self.candidate_sha256,
            "content_sha256": self.content_sha256,
            "feature_sha256": self.feature_sha256,
            "relative_uri": self.relative_uri,
            "row_count": self.row_count,
        }


@dataclass(frozen=True, slots=True)
class NumericAdmissionRules:
    """Frozen integer thresholds; classification cannot be caller supplied."""

    min_raw_events: int
    min_flat_trades: int
    min_sequential_trades: int
    min_active_days: int
    min_tp_probability_ppm: int
    require_positive_net_ev: bool
    min_positive_search_folds: int
    max_stressed_cost_ev_floor_ticks: int

    def __post_init__(self) -> None:
        for name in (
            "min_raw_events",
            "min_flat_trades",
            "min_sequential_trades",
            "min_active_days",
            "min_tp_probability_ppm",
            "min_positive_search_folds",
        ):
            _integer(getattr(self, name), label=name)
        if self.min_tp_probability_ppm > 1_000_000:
            raise M0bWorkerError("min_tp_probability_ppm exceeds one million")
        if not isinstance(self.require_positive_net_ev, bool):
            raise M0bWorkerError("require_positive_net_ev must be boolean")
        if isinstance(self.max_stressed_cost_ev_floor_ticks, bool) or not isinstance(
            self.max_stressed_cost_ev_floor_ticks, int
        ):
            raise M0bWorkerError("max_stressed_cost_ev_floor_ticks must be an integer")

    def as_dict(self) -> dict[str, object]:
        return {
            "contract_version": "m0b_numeric_admission_v1",
            "max_stressed_cost_ev_floor_ticks": self.max_stressed_cost_ev_floor_ticks,
            "maximum_authority": "REGISTER",
            "min_active_days": self.min_active_days,
            "min_flat_trades": self.min_flat_trades,
            "min_positive_search_folds": self.min_positive_search_folds,
            "min_raw_events": self.min_raw_events,
            "min_sequential_trades": self.min_sequential_trades,
            "min_tp_probability_ppm": self.min_tp_probability_ppm,
            "require_positive_net_ev": self.require_positive_net_ev,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


@dataclass(frozen=True, slots=True)
class VolatilityBarrierSpec:
    """Exact rational volatility barrier used by labels and one candidate."""

    barrier_id: str
    k_tp_num: int
    k_tp_den: int
    k_sl_num: int
    k_sl_den: int
    max_hold_seconds: int

    def __post_init__(self) -> None:
        _nonempty(self.barrier_id, label="barrier_id")
        for name in ("k_tp_num", "k_tp_den", "k_sl_num", "k_sl_den"):
            _bounded_integer(
                getattr(self, name),
                label=name,
                minimum=1,
                maximum=_MAX_BARRIER_COMPONENT,
            )
        _bounded_integer(
            self.max_hold_seconds,
            label="max_hold_seconds",
            minimum=1,
            maximum=_MAX_HOLD_SECONDS,
        )
        expected = (
            f"tp{self.k_tp_num}of{self.k_tp_den}_"
            f"sl{self.k_sl_num}of{self.k_sl_den}_h{self.max_hold_seconds}"
        )
        if self.barrier_id != expected:
            raise M0bWorkerError("barrier_id differs from its exact rational specification")

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": "systematic_fx.m0b_volatility_barrier.v1",
            "barrier_id": self.barrier_id,
            "k_sl_den": self.k_sl_den,
            "k_sl_num": self.k_sl_num,
            "k_tp_den": self.k_tp_den,
            "k_tp_num": self.k_tp_num,
            "max_hold_seconds": self.max_hold_seconds,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


@dataclass(frozen=True, slots=True)
class CandidateWorkSpec:
    epoch_sha256: str
    candidate_sha256: str
    first_passage_store_sha256: str
    signals: SignalArtifact
    candidate_kind: str
    direction: str
    barrier: VolatilityBarrierSpec
    cooldown_ns: int
    stress_extra_cost_ticks: int
    search_fold_count: int
    max_signals: int
    max_trades: int
    checkpoint_shard_interval: int
    deterministic_seed: int
    code_snapshot_sha256: str
    cost_sha256: str
    execution_sha256: str
    split_sha256: str
    admission_rules: NumericAdmissionRules
    search_only: bool = True
    sealed_holdout_untouched: bool = True

    def __post_init__(self) -> None:
        for name in (
            "epoch_sha256",
            "candidate_sha256",
            "first_passage_store_sha256",
            "code_snapshot_sha256",
            "cost_sha256",
            "execution_sha256",
            "split_sha256",
        ):
            _sha256(getattr(self, name), label=name)
        if not isinstance(self.signals, SignalArtifact):
            raise M0bWorkerError("signals must be a canonical SignalArtifact")
        if self.signals.candidate_sha256 != self.candidate_sha256:
            raise M0bWorkerError("signal artifact belongs to another candidate")
        if self.candidate_kind not in {"REAL", "NULL"}:
            raise M0bWorkerError("candidate_kind must be REAL or NULL")
        if self.direction not in {"LONG", "SHORT"}:
            raise M0bWorkerError("worker direction must be LONG or SHORT")
        if not isinstance(self.barrier, VolatilityBarrierSpec):
            raise M0bWorkerError("barrier must be a canonical VolatilityBarrierSpec")
        _bounded_integer(
            self.cooldown_ns,
            label="cooldown_ns",
            minimum=0,
            maximum=_MAX_COOLDOWN_NS,
        )
        _bounded_integer(
            self.stress_extra_cost_ticks,
            label="stress_extra_cost_ticks",
            minimum=0,
            maximum=_MAX_STRESS_EXTRA_COST_TICKS,
        )
        _bounded_integer(
            self.search_fold_count,
            label="search_fold_count",
            minimum=1,
            maximum=_MAX_SEARCH_FOLDS,
        )
        _bounded_integer(
            self.max_signals,
            label="max_signals",
            minimum=1,
            maximum=_MAX_SIGNALS,
        )
        _bounded_integer(
            self.max_trades,
            label="max_trades",
            minimum=1,
            maximum=_MAX_SIGNALS,
        )
        _bounded_integer(
            self.checkpoint_shard_interval,
            label="checkpoint_shard_interval",
            minimum=1,
            maximum=_MAX_CHECKPOINT_SHARD_INTERVAL,
        )
        _integer(self.deterministic_seed, label="deterministic_seed")
        if self.signals.row_count > self.max_signals or self.max_trades > self.max_signals:
            raise M0bWorkerError("signal/trade bounds differ from the precommitted budget")
        if not isinstance(self.admission_rules, NumericAdmissionRules):
            raise M0bWorkerError("admission_rules must be frozen numeric rules")
        if self.admission_rules.min_positive_search_folds > self.search_fold_count:
            raise M0bWorkerError("positive-fold threshold exceeds the fold count")
        if not self.search_only or not self.sealed_holdout_untouched:
            raise M0bWorkerError("worker authority must remain search-only")

    def as_dict(self) -> dict[str, object]:
        return {
            "admission_rules": self.admission_rules.as_dict(),
            "admission_rules_sha256": self.admission_rules.sha256,
            "artifact_schema": "systematic_fx.m0b_candidate_work_spec.v2",
            "barrier": self.barrier.as_dict(),
            "barrier_sha256": self.barrier.sha256,
            "candidate_sha256": self.candidate_sha256,
            "candidate_kind": self.candidate_kind,
            "code_snapshot_sha256": self.code_snapshot_sha256,
            "cost_sha256": self.cost_sha256,
            "deterministic_seed": self.deterministic_seed,
            "direction": self.direction,
            "epoch_sha256": self.epoch_sha256,
            "evaluation_policy": self.evaluation_policy,
            "evaluation_policy_sha256": self.evaluation_policy_sha256,
            "execution_assumptions": _EXECUTION_ASSUMPTIONS,
            "execution_contract_version": "m0b_worker_execution_v1",
            "execution_sha256": self.execution_sha256,
            "first_passage_store_sha256": self.first_passage_store_sha256,
            "sealed_holdout_untouched": self.sealed_holdout_untouched,
            "search_only": self.search_only,
            "signals": self.signals.as_dict(),
            "split_sha256": self.split_sha256,
        }

    @property
    def barrier_id(self) -> str:
        return self.barrier.barrier_id

    @property
    def evaluation_policy(self) -> dict[str, object]:
        return {
            "artifact_schema": "systematic_fx.m0b_worker_evaluation_policy.v1",
            "checkpoint_shard_interval": self.checkpoint_shard_interval,
            "cooldown_ns": self.cooldown_ns,
            "max_signals": self.max_signals,
            "max_trades": self.max_trades,
            "search_fold_count": self.search_fold_count,
            "stress_extra_cost_ticks": self.stress_extra_cost_ticks,
        }

    @property
    def evaluation_policy_sha256(self) -> str:
        return canonical_sha256(self.evaluation_policy)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


@dataclass(frozen=True, slots=True)
class WorkerAttempt:
    m0b_candidate_id: int
    research_run_attempt_id: int

    def __post_init__(self) -> None:
        _integer(self.m0b_candidate_id, label="m0b_candidate_id", minimum=1)
        _integer(self.research_run_attempt_id, label="research_run_attempt_id", minimum=1)


@dataclass(frozen=True, slots=True)
class WorkerRunResult:
    work_spec_sha256: str
    checkpoint_sha256: str
    checkpoint_relative_uri: str
    complete: bool
    result_sha256: str | None
    result_relative_uri: str | None
    classification: str | None
    metrics: dict[str, object] | None


@dataclass(frozen=True, slots=True)
class CandidateWorkArtifact:
    """Loaded canonical bytes used by atomic candidate registration."""

    work: CandidateWorkSpec
    path: Path
    canonical_bytes: bytes
    content_sha256: str
    byte_size: int
    relative_uri: str
    source_build_sha256: str
    source_label_sha256: str
    source_feature_sha256: str
    media_type: str = "application/json"

    def __post_init__(self) -> None:
        if not isinstance(self.work, CandidateWorkSpec):
            raise M0bWorkerError("work artifact requires a CandidateWorkSpec")
        if not isinstance(self.path, Path) or not self.path.is_absolute():
            raise M0bWorkerError("work artifact path must be absolute")
        if not isinstance(self.canonical_bytes, bytes):
            raise M0bWorkerError("work artifact canonical_bytes must be bytes")
        _sha256(self.content_sha256, label="work artifact content_sha256")
        _integer(self.byte_size, label="work artifact byte_size", minimum=1)
        _leaf(self.relative_uri, label="work artifact relative_uri")
        _sha256(self.source_build_sha256, label="work source_build_sha256")
        _sha256(self.source_label_sha256, label="work source_label_sha256")
        _sha256(self.source_feature_sha256, label="work source_feature_sha256")
        expected_bytes = canonical_json_bytes(self.work.as_dict())
        if (
            self.canonical_bytes != expected_bytes
            or len(self.canonical_bytes) != self.byte_size
            or hashlib.sha256(self.canonical_bytes).hexdigest() != self.content_sha256
            or self.content_sha256 != self.work.sha256
            or self.relative_uri != f"candidate-work-{self.content_sha256}.json"
            or self.path.name != self.relative_uri
            or self.media_type != "application/json"
        ):
            raise M0bWorkerError("work artifact bytes, path, and semantic identity differ")

    def metadata(self) -> dict[str, object]:
        """Exact database metadata; local path authority is intentionally absent."""

        return {
            "admission_rules_sha256": self.work.admission_rules.sha256,
            "barrier": self.work.barrier.as_dict(),
            "barrier_sha256": self.work.barrier.sha256,
            "candidate_kind": self.work.candidate_kind,
            "candidate_sha256": self.work.candidate_sha256,
            "code_snapshot_sha256": self.work.code_snapshot_sha256,
            "cost_sha256": self.work.cost_sha256,
            "data_role": "SEARCH",
            "deterministic_seed": self.work.deterministic_seed,
            "direction": self.work.direction,
            "epoch_sha256": self.work.epoch_sha256,
            "evaluation_policy": self.work.evaluation_policy,
            "evaluation_policy_sha256": self.work.evaluation_policy_sha256,
            "execution_sha256": self.work.execution_sha256,
            "first_passage_store_sha256": self.work.first_passage_store_sha256,
            "identity_schema": "systematic_fx.m0b.candidate_work.v2",
            "signal_artifact_sha256": self.work.signals.content_sha256,
            "source_build_sha256": self.source_build_sha256,
            "source_feature_sha256": self.source_feature_sha256,
            "source_label_sha256": self.source_label_sha256,
            "split_sha256": self.work.split_sha256,
            "work_spec_sha256": self.work.sha256,
        }

    @property
    def artifact_key(self) -> str:
        return (
            f"m0b-candidate-work:{self.work.epoch_sha256}:"
            f"{self.work.candidate_sha256}:{self.content_sha256}"
        )

    @property
    def artifact_uri(self) -> str:
        return (
            f"m0b-work://search/{self.work.epoch_sha256}/"
            f"{self.work.candidate_sha256}/sha256={self.content_sha256}.json"
        )


@dataclass(frozen=True, slots=True)
class CandidateJob:
    """One already-registered, finite worker job; never generated at runtime."""

    work: CandidateWorkSpec
    attempt: WorkerAttempt
    first_passage_manifest: Path
    worker_root: Path

    def __post_init__(self) -> None:
        if not isinstance(self.work, CandidateWorkSpec) or not isinstance(
            self.attempt, WorkerAttempt
        ):
            raise M0bWorkerError("candidate job identities are invalid")


@dataclass(frozen=True, slots=True)
class DaemonJobResult:
    candidate_sha256: str
    status: str
    result: WorkerRunResult | None
    error: str | None


class WorkerObserver(Protocol):
    """Idempotent integration hook for the PostgreSQL control plane."""

    def checkpoint_published(
        self,
        *,
        checkpoint_sha256: str,
        checkpoint: dict[str, object],
        relative_uri: str,
    ) -> None: ...

    def result_published(
        self,
        *,
        result_sha256: str,
        result: dict[str, object],
        relative_uri: str,
    ) -> None: ...

    def failure_published(self, *, error_message: str, retryable: bool) -> None: ...


def _signal_key(row: dict[str, Any]) -> tuple[int, int, str]:
    event_ts = row.get("event_ts_ns")
    instrument = row.get("instrument_id")
    session = row.get("session_id")
    if (
        isinstance(event_ts, bool)
        or not isinstance(event_ts, int)
        or event_ts < 0
        or isinstance(instrument, bool)
        or not isinstance(instrument, int)
        or instrument <= 0
        or not isinstance(session, str)
        or not session
    ):
        raise M0bWorkerError("signal event identity is invalid")
    return event_ts, instrument, session


def publish_signal_artifact(
    root: str | Path,
    *,
    candidate_sha256: str,
    feature_sha256: str,
    rows: list[dict[str, object]],
    max_signals: int,
    search_fold_count: int,
) -> SignalArtifact:
    """Publish one sorted signal mask as an immutable candidate input."""

    _sha256(candidate_sha256, label="candidate_sha256")
    _sha256(feature_sha256, label="feature_sha256")
    _integer(max_signals, label="max_signals", minimum=1)
    _integer(search_fold_count, label="search_fold_count", minimum=1)
    if len(rows) > max_signals:
        raise M0bWorkerError("signals exceed the precommitted bound")
    destination = _safe_root(root, label="m0b_worker_root", create=True)
    payloads: list[bytes] = []
    previous: tuple[int, int, str] | None = None
    expected_keys = {
        "artifact_schema",
        "candidate_sha256",
        "event_ts_ns",
        "feature_sha256",
        "instrument_id",
        "search_fold",
        "session_id",
    }
    for row in rows:
        if set(row) != expected_keys or row.get("artifact_schema") != SIGNAL_SCHEMA:
            raise M0bWorkerError("signal row schema differs")
        if (
            row.get("candidate_sha256") != candidate_sha256
            or row.get("feature_sha256") != feature_sha256
        ):
            raise M0bWorkerError("signal row lineage differs")
        key = _signal_key(row)
        if previous is not None and key <= previous:
            raise M0bWorkerError("signal rows must be strictly ordered and unique")
        previous = key
        fold = row.get("search_fold")
        if isinstance(fold, bool) or not isinstance(fold, int) or not 0 <= fold < search_fold_count:
            raise M0bWorkerError("signal search_fold is outside the precommitment")
        payloads.append(canonical_json_bytes(row) + b"\n")
    payload = b"".join(payloads)
    content_sha256 = hashlib.sha256(payload).hexdigest()
    relative_uri = f"candidate-signals-{content_sha256}.jsonl"
    _publish_immutable(destination, relative_uri, payload)
    return SignalArtifact(
        candidate_sha256=candidate_sha256,
        feature_sha256=feature_sha256,
        row_count=len(rows),
        byte_size=len(payload),
        content_sha256=content_sha256,
        relative_uri=relative_uri,
    )


def publish_candidate_work_manifest(
    root: str | Path,
    work: CandidateWorkSpec,
) -> str:
    """Durably freeze every input and execution assumption before evaluation."""

    if not isinstance(work, CandidateWorkSpec):
        raise M0bWorkerError("work manifest requires a canonical CandidateWorkSpec")
    destination = _safe_root(root, label="m0b_worker_root", create=True)
    payload = canonical_json_bytes(work.as_dict())
    relative_uri = f"candidate-work-{work.sha256}.json"
    _publish_immutable(destination, relative_uri, payload)
    return relative_uri


def load_candidate_work_manifest(path: str | Path) -> CandidateWorkSpec:
    """Load exactly one content-addressed work manifest without defaults."""

    requested = Path(path).expanduser()
    root = _safe_root(requested.parent, label="m0b_worker_root")
    leaf = _leaf(requested.name, label="candidate work manifest")
    document, observed = _read_canonical_document(root / leaf, label="candidate work manifest")
    expected_keys = {
        "admission_rules",
        "admission_rules_sha256",
        "artifact_schema",
        "barrier",
        "barrier_sha256",
        "candidate_sha256",
        "candidate_kind",
        "code_snapshot_sha256",
        "cost_sha256",
        "deterministic_seed",
        "direction",
        "epoch_sha256",
        "evaluation_policy",
        "evaluation_policy_sha256",
        "execution_assumptions",
        "execution_contract_version",
        "execution_sha256",
        "first_passage_store_sha256",
        "sealed_holdout_untouched",
        "search_only",
        "signals",
        "split_sha256",
    }
    if (
        set(document) != expected_keys
        or document.get("artifact_schema") != "systematic_fx.m0b_candidate_work_spec.v2"
    ):
        raise M0bWorkerError("candidate work manifest schema differs")
    if (
        document.get("execution_contract_version") != "m0b_worker_execution_v1"
        or document.get("execution_assumptions") != _EXECUTION_ASSUMPTIONS
    ):
        raise M0bWorkerError("candidate work execution assumptions differ")
    signal = document.get("signals")
    if not isinstance(signal, dict) or set(signal) != {
        "artifact_schema",
        "byte_size",
        "candidate_sha256",
        "content_sha256",
        "feature_sha256",
        "relative_uri",
        "row_count",
    }:
        raise M0bWorkerError("candidate signal identity differs")
    barrier = document.get("barrier")
    if not isinstance(barrier, dict) or set(barrier) != {
        "artifact_schema",
        "barrier_id",
        "k_sl_den",
        "k_sl_num",
        "k_tp_den",
        "k_tp_num",
        "max_hold_seconds",
    }:
        raise M0bWorkerError("candidate barrier identity differs")
    evaluation = document.get("evaluation_policy")
    if not isinstance(evaluation, dict) or set(evaluation) != {
        "artifact_schema",
        "checkpoint_shard_interval",
        "cooldown_ns",
        "max_signals",
        "max_trades",
        "search_fold_count",
        "stress_extra_cost_ticks",
    }:
        raise M0bWorkerError("candidate evaluation policy differs")
    if (
        barrier.get("artifact_schema") != "systematic_fx.m0b_volatility_barrier.v1"
        or evaluation.get("artifact_schema") != "systematic_fx.m0b_worker_evaluation_policy.v1"
    ):
        raise M0bWorkerError("candidate barrier/evaluation schema differs")
    rules = document.get("admission_rules")
    if not isinstance(rules, dict) or set(rules) != {
        "contract_version",
        "max_stressed_cost_ev_floor_ticks",
        "maximum_authority",
        "min_active_days",
        "min_flat_trades",
        "min_positive_search_folds",
        "min_raw_events",
        "min_sequential_trades",
        "min_tp_probability_ppm",
        "require_positive_net_ev",
    }:
        raise M0bWorkerError("candidate numeric admission rules differ")
    if (
        rules.get("contract_version") != "m0b_numeric_admission_v1"
        or rules.get("maximum_authority") != "REGISTER"
    ):
        raise M0bWorkerError("candidate admission authority differs")
    try:
        admission = NumericAdmissionRules(
            min_raw_events=rules["min_raw_events"],
            min_flat_trades=rules["min_flat_trades"],
            min_sequential_trades=rules["min_sequential_trades"],
            min_active_days=rules["min_active_days"],
            min_tp_probability_ppm=rules["min_tp_probability_ppm"],
            require_positive_net_ev=rules["require_positive_net_ev"],
            min_positive_search_folds=rules["min_positive_search_folds"],
            max_stressed_cost_ev_floor_ticks=rules["max_stressed_cost_ev_floor_ticks"],
        )
        signals = SignalArtifact(
            candidate_sha256=signal["candidate_sha256"],
            feature_sha256=signal["feature_sha256"],
            row_count=signal["row_count"],
            byte_size=signal["byte_size"],
            content_sha256=signal["content_sha256"],
            relative_uri=signal["relative_uri"],
        )
        barrier_spec = VolatilityBarrierSpec(
            barrier_id=barrier["barrier_id"],
            k_tp_num=barrier["k_tp_num"],
            k_tp_den=barrier["k_tp_den"],
            k_sl_num=barrier["k_sl_num"],
            k_sl_den=barrier["k_sl_den"],
            max_hold_seconds=barrier["max_hold_seconds"],
        )
        work = CandidateWorkSpec(
            epoch_sha256=document["epoch_sha256"],
            candidate_sha256=document["candidate_sha256"],
            first_passage_store_sha256=document["first_passage_store_sha256"],
            signals=signals,
            candidate_kind=document["candidate_kind"],
            direction=document["direction"],
            barrier=barrier_spec,
            cooldown_ns=evaluation["cooldown_ns"],
            stress_extra_cost_ticks=evaluation["stress_extra_cost_ticks"],
            search_fold_count=evaluation["search_fold_count"],
            max_signals=evaluation["max_signals"],
            max_trades=evaluation["max_trades"],
            checkpoint_shard_interval=evaluation["checkpoint_shard_interval"],
            deterministic_seed=document["deterministic_seed"],
            code_snapshot_sha256=document["code_snapshot_sha256"],
            cost_sha256=document["cost_sha256"],
            execution_sha256=document["execution_sha256"],
            split_sha256=document["split_sha256"],
            admission_rules=admission,
            search_only=document["search_only"],
            sealed_holdout_untouched=document["sealed_holdout_untouched"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise M0bWorkerError("candidate work manifest values are malformed") from error
    if document.get("admission_rules_sha256") != admission.sha256:
        raise M0bWorkerError("candidate admission rule hash differs")
    if (
        document.get("barrier_sha256") != barrier_spec.sha256
        or document.get("evaluation_policy_sha256") != work.evaluation_policy_sha256
    ):
        raise M0bWorkerError("candidate barrier/evaluation policy hash differs")
    if work.sha256 != observed or leaf != f"candidate-work-{observed}.json":
        raise M0bWorkerError("candidate work filename/hash differ")
    return work


def load_candidate_work_artifact(
    path: str | Path,
    *,
    reconcile_inputs: bool = True,
) -> CandidateWorkArtifact:
    """Load registration-ready work bytes and reconcile referenced inputs.

    The returned bytes are detached from the path, preventing a later pathname
    swap from changing the identity inserted by the registry transaction.
    """

    requested = Path(path).expanduser()
    root = _safe_root(requested.parent, label="m0b_worker_root")
    leaf = _leaf(requested.name, label="candidate work manifest")
    bounded = root / leaf
    if bounded.is_symlink():
        raise M0bWorkerError("candidate work manifest cannot be symbolic")
    resolved = bounded.resolve(strict=True)
    if not resolved.is_relative_to(root):
        raise M0bWorkerError("candidate work manifest escaped its bounded root")
    work = load_candidate_work_manifest(resolved)
    payload = resolved.read_bytes()
    store_path = root / f"first-passage-store-{work.first_passage_store_sha256}.json"
    store = load_first_passage_store(store_path, verify_shards=reconcile_inputs)
    if store.sha256 != work.first_passage_store_sha256:
        raise M0bWorkerError("candidate work first-passage input differs")
    if reconcile_inputs:
        _reconcile_work_barrier_universe(root, store, work)
        _load_signals(root, work)
    return CandidateWorkArtifact(
        work=work,
        path=resolved,
        canonical_bytes=payload,
        content_sha256=hashlib.sha256(payload).hexdigest(),
        byte_size=len(payload),
        relative_uri=leaf,
        source_build_sha256=store.source_build_sha256,
        source_label_sha256=store.source_label_sha256,
        source_feature_sha256=store.source_feature_sha256,
    )


def _reconcile_work_barrier_universe(
    root: Path,
    store: FirstPassageStore,
    work: CandidateWorkSpec,
) -> None:
    """Require the exact representation used by Work to exist in its store."""

    expected = (
        work.direction,
        work.barrier_id,
        work.barrier.k_tp_num,
        work.barrier.k_tp_den,
        work.barrier.k_sl_num,
        work.barrier.k_sl_den,
        work.barrier.max_hold_seconds,
    )
    for shard in store.shards:
        with (root / shard.relative_uri).open("rb") as handle:
            for payload in handle:
                row = json.loads(payload)
                observed = (
                    row.get("direction"),
                    row.get("barrier_id"),
                    row.get("k_tp_num"),
                    row.get("k_tp_den"),
                    row.get("k_sl_num"),
                    row.get("k_sl_den"),
                    row.get("max_hold_seconds"),
                )
                if observed == expected:
                    return
    raise M0bWorkerError(
        "CandidateWork exact direction/barrier representation is absent from its store"
    )


def _load_signals(root: Path, work: CandidateWorkSpec) -> list[dict[str, Any]]:
    path = root / work.signals.relative_uri
    if (
        path.is_symlink()
        or not path.is_file()
        or path.stat().st_mode & _WRITE_BITS
        or path.stat().st_size != work.signals.byte_size
        or _file_sha256(path) != work.signals.content_sha256
    ):
        raise M0bWorkerError("signal artifact bytes differ from the work spec")
    rows: list[dict[str, Any]] = []
    previous: tuple[int, int, str] | None = None
    with path.open("rb") as handle:
        for payload in handle:
            try:
                row = json.loads(payload)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise M0bWorkerError("signal artifact is invalid JSONL") from error
            if not isinstance(row, dict) or canonical_json_bytes(row) + b"\n" != payload:
                raise M0bWorkerError("signal artifact is not canonical")
            if (
                row.get("artifact_schema") != SIGNAL_SCHEMA
                or row.get("candidate_sha256") != work.candidate_sha256
                or row.get("feature_sha256") != work.signals.feature_sha256
            ):
                raise M0bWorkerError("signal artifact lineage differs")
            key = _signal_key(row)
            if previous is not None and key <= previous:
                raise M0bWorkerError("signal artifact order differs")
            previous = key
            fold = row.get("search_fold")
            if (
                isinstance(fold, bool)
                or not isinstance(fold, int)
                or not 0 <= fold < work.search_fold_count
            ):
                raise M0bWorkerError("signal fold differs from the work spec")
            rows.append(row)
            if len(rows) > work.max_signals:
                raise M0bWorkerError("signal artifact exceeds the work budget")
    if len(rows) != work.signals.row_count:
        raise M0bWorkerError("signal artifact cardinality differs")
    return rows


def _initial_state(work: CandidateWorkSpec) -> dict[str, object]:
    return {
        "accepted_tp_count": 0,
        "active_session_ids": [],
        "complete": False,
        "fold_net_pnl_ticks": [0] * work.search_fold_count,
        "fold_trade_counts": [0] * work.search_fold_count,
        "ineligible_signal_count": 0,
        "matching_label_count": 0,
        "missing_label_count": 0,
        "next_available_ts_ns": None,
        "next_shard_ordinal": 1,
        "next_signal_index": 0,
        "overlap_signal_count": 0,
        "raw_event_count": 0,
        "raw_net_pnl_ticks": 0,
        "raw_tp_count": 0,
        "result_artifact": None,
        "sequential_net_pnl_ticks": 0,
        "sequential_stressed_net_pnl_ticks": 0,
        "sequential_trade_count": 0,
        "state_schema": WORKER_STATE_SCHEMA,
        "trade_shards": [],
        "work_spec_sha256": work.sha256,
    }


def _label_event_key(row: dict[str, Any]) -> tuple[int, int, str]:
    return _signal_key(row)


def _validate_executable_label(row: dict[str, Any], work: CandidateWorkSpec) -> None:
    outcome = row.get("first_touch_type")
    if outcome not in _OUTCOMES:
        raise M0bWorkerError("eligible label has no deterministic first-passage outcome")
    event_ts = _integer(row.get("event_ts_ns"), label="label event_ts_ns")
    entry_ts = _integer(row.get("entry_ts_ns"), label="label entry_ts_ns")
    exit_ts = _integer(row.get("exit_ts_ns"), label="label exit_ts_ns")
    hold = _integer(row.get("max_hold_seconds"), label="label max_hold_seconds", minimum=1)
    if entry_ts < event_ts or exit_ts < entry_ts:
        raise M0bWorkerError("eligible label time order differs")
    if exit_ts > event_ts + hold * _NS:
        raise M0bWorkerError("eligible label exits after its precommitted horizon")
    if outcome == "TIMEOUT":
        if (
            row.get("timeout") is not True
            or row.get("first_touch_ts_ns") is not None
            or exit_ts != event_ts + hold * _NS
        ):
            raise M0bWorkerError("TIMEOUT does not exit at the exact precommitted horizon")
    else:
        first_touch = _integer(row.get("first_touch_ts_ns"), label="first_touch_ts_ns")
        if row.get("timeout") is not False or first_touch != exit_ts:
            raise M0bWorkerError("first-touch timestamp differs from the exit")
        if outcome == "TP_FIRST" and first_touch <= entry_ts:
            raise M0bWorkerError("passive TP is not a post-entry trade-through outcome")
    if (
        row.get("direction") != work.direction
        or row.get("barrier_id") != work.barrier_id
        or any(
            row.get(name) != getattr(work.barrier, name)
            for name in (
                "k_tp_num",
                "k_tp_den",
                "k_sl_num",
                "k_sl_den",
                "max_hold_seconds",
            )
        )
    ):
        raise M0bWorkerError("selected label differs from candidate execution parameters")
    _integer(row.get("entry_price_ticks"), label="entry_price_ticks", minimum=1)
    _integer(row.get("exit_price_ticks"), label="exit_price_ticks", minimum=1)
    net = row.get("net_pnl_ticks")
    if isinstance(net, bool) or not isinstance(net, int):
        raise M0bWorkerError("net_pnl_ticks must be an integer")


def _trade_row(
    row: dict[str, Any],
    signal: dict[str, Any],
    work: CandidateWorkSpec,
) -> dict[str, object]:
    return {
        "artifact_schema": "systematic_fx.m0b_sequential_trade.v1",
        "barrier_id": work.barrier_id,
        "direction": work.direction,
        "entry_price_ticks": row["entry_price_ticks"],
        "entry_ts_ns": row["entry_ts_ns"],
        "event_ts_ns": row["event_ts_ns"],
        "exit_price_ticks": row["exit_price_ticks"],
        "exit_ts_ns": row["exit_ts_ns"],
        "first_touch_type": row["first_touch_type"],
        "instrument_id": row["instrument_id"],
        "net_pnl_ticks": row["net_pnl_ticks"],
        "search_fold": signal["search_fold"],
        "session_id": row["session_id"],
        "stressed_net_pnl_ticks": int(row["net_pnl_ticks"]) - work.stress_extra_cost_ticks,
    }


def _process_range(
    root: Path,
    store: FirstPassageStore,
    signals: list[dict[str, Any]],
    work: CandidateWorkSpec,
    state: dict[str, object],
    *,
    first_ordinal: int,
    last_ordinal: int,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    mutable = json.loads(canonical_json_bytes(state))
    signal_index = int(mutable["next_signal_index"])
    next_available = mutable["next_available_ts_ns"]
    trades: list[dict[str, object]] = []
    active_sessions = set(mutable["active_session_ids"])
    fold_pnl = list(mutable["fold_net_pnl_ticks"])
    fold_counts = list(mutable["fold_trade_counts"])

    for shard in store.shards[first_ordinal - 1 : last_ordinal]:
        path = root / shard.relative_uri
        if (
            path.is_symlink()
            or not path.is_file()
            or path.stat().st_mode & _WRITE_BITS
            or path.stat().st_size != shard.byte_size
            or _file_sha256(path) != shard.content_sha256
        ):
            raise M0bWorkerError("first-passage shard drifted during worker execution")
        with path.open("rb") as handle:
            for payload in handle:
                try:
                    row = json.loads(payload)
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise M0bWorkerError("first-passage shard is invalid JSONL") from error
                if not isinstance(row, dict) or canonical_json_bytes(row) + b"\n" != payload:
                    raise M0bWorkerError("first-passage shard row is not canonical")
                if (
                    row.get("direction") != work.direction
                    or row.get("barrier_id") != work.barrier_id
                    or any(
                        row.get(name) != getattr(work.barrier, name)
                        for name in (
                            "k_tp_num",
                            "k_tp_den",
                            "k_sl_num",
                            "k_sl_den",
                            "max_hold_seconds",
                        )
                    )
                ):
                    continue
                label_key = _label_event_key(row)
                while (
                    signal_index < len(signals) and _signal_key(signals[signal_index]) < label_key
                ):
                    mutable["missing_label_count"] += 1
                    signal_index += 1
                if signal_index >= len(signals) or _signal_key(signals[signal_index]) != label_key:
                    continue
                signal = signals[signal_index]
                signal_index += 1
                mutable["matching_label_count"] += 1
                if (
                    row.get("entry_eligible") is not True
                    or row.get("mechanical_outcome_valid") is not True
                ):
                    mutable["ineligible_signal_count"] += 1
                    continue
                _validate_executable_label(row, work)
                mutable["raw_event_count"] += 1
                mutable["raw_net_pnl_ticks"] += int(row["net_pnl_ticks"])
                if row["first_touch_type"] == "TP_FIRST":
                    mutable["raw_tp_count"] += 1
                event_ts = int(row["event_ts_ns"])
                if next_available is not None and event_ts < int(next_available):
                    mutable["overlap_signal_count"] += 1
                    continue
                if int(mutable["sequential_trade_count"]) >= work.max_trades:
                    raise M0bWorkerError("accepted trades exceed the precommitted worker bound")
                trade = _trade_row(row, signal, work)
                trades.append(trade)
                mutable["sequential_trade_count"] += 1
                mutable["sequential_net_pnl_ticks"] += int(trade["net_pnl_ticks"])
                mutable["sequential_stressed_net_pnl_ticks"] += int(trade["stressed_net_pnl_ticks"])
                if trade["first_touch_type"] == "TP_FIRST":
                    mutable["accepted_tp_count"] += 1
                fold = int(trade["search_fold"])
                fold_pnl[fold] += int(trade["net_pnl_ticks"])
                fold_counts[fold] += 1
                active_sessions.add(str(trade["session_id"]))
                next_available = int(trade["exit_ts_ns"]) + work.cooldown_ns
        while (
            signal_index < len(signals)
            and _signal_key(signals[signal_index]) <= shard.last_event_key
        ):
            mutable["missing_label_count"] += 1
            signal_index += 1

    mutable["active_session_ids"] = sorted(active_sessions)
    mutable["fold_net_pnl_ticks"] = fold_pnl
    mutable["fold_trade_counts"] = fold_counts
    mutable["next_available_ts_ns"] = next_available
    mutable["next_signal_index"] = signal_index
    mutable["next_shard_ordinal"] = last_ordinal + 1
    return mutable, trades


def _publish_trade_shard(
    root: Path,
    work: CandidateWorkSpec,
    *,
    ordinal: int,
    first_store_shard: int,
    last_store_shard: int,
    trades: list[dict[str, object]],
) -> dict[str, object]:
    document = {
        "artifact_schema": TRADE_SHARD_SCHEMA,
        "first_store_shard": first_store_shard,
        "last_store_shard": last_store_shard,
        "ordinal": ordinal,
        "row_count": len(trades),
        "trades": trades,
        "work_spec_sha256": work.sha256,
    }
    payload = canonical_json_bytes(document)
    sha256 = hashlib.sha256(payload).hexdigest()
    relative_uri = f"candidate-trades-{ordinal:06d}-{sha256}.json"
    _publish_immutable(root, relative_uri, payload)
    return {
        "byte_size": len(payload),
        "content_sha256": sha256,
        "first_store_shard": first_store_shard,
        "last_store_shard": last_store_shard,
        "ordinal": ordinal,
        "relative_uri": relative_uri,
        "row_count": len(trades),
    }


def _metrics(state: dict[str, object]) -> dict[str, object]:
    sequential = int(state["sequential_trade_count"])
    net = int(state["sequential_net_pnl_ticks"])
    stressed = int(state["sequential_stressed_net_pnl_ticks"])
    tp_count = int(state["accepted_tp_count"])
    probability = 0 if sequential == 0 else (tp_count * 1_000_000 + sequential // 2) // sequential
    return {
        "active_days": len(state["active_session_ids"]),
        "flat_trades": sequential,
        "net_pnl_ticks": net,
        "positive_search_folds": sum(1 for value in state["fold_net_pnl_ticks"] if int(value) > 0),
        "raw_events": int(state["raw_event_count"]),
        "sequential_trades": sequential,
        "stressed_net_pnl_ticks": stressed,
        "tp_probability_ppm": probability,
    }


def _classification(
    metrics: dict[str, object], rules: NumericAdmissionRules, *, candidate_kind: str
) -> str:
    count = int(metrics["sequential_trades"])
    net = int(metrics["net_pnl_ticks"])
    stressed = int(metrics["stressed_net_pnl_ticks"])
    passed = (
        int(metrics["raw_events"]) >= rules.min_raw_events
        and int(metrics["flat_trades"]) >= rules.min_flat_trades
        and count >= rules.min_sequential_trades
        and int(metrics["active_days"]) >= rules.min_active_days
        and int(metrics["tp_probability_ppm"]) >= rules.min_tp_probability_ppm
        and int(metrics["positive_search_folds"]) >= rules.min_positive_search_folds
        and count > 0
        and (not rules.require_positive_net_ev or net > 0)
        and stressed >= rules.max_stressed_cost_ev_floor_ticks * count
    )
    return "REGISTERED" if candidate_kind == "REAL" and passed else "SCREENED_OUT"


def _publish_result(
    root: Path,
    work: CandidateWorkSpec,
    state: dict[str, object],
) -> tuple[dict[str, object], str, str, int]:
    metrics = _metrics(state)
    raw_event_count = int(state["raw_event_count"])
    raw_tp_count = int(state["raw_tp_count"])
    raw_tp_probability_ppm = (
        0
        if raw_event_count == 0
        else (raw_tp_count * 1_000_000 + raw_event_count // 2) // raw_event_count
    )
    classification = _classification(
        metrics, work.admission_rules, candidate_kind=work.candidate_kind
    )
    document = {
        "admission_rules": work.admission_rules.as_dict(),
        "admission_rules_sha256": work.admission_rules.sha256,
        "artifact_schema": RESULT_SCHEMA,
        "authority": {
            "paper_eligible": False,
            "live_eligible": False,
            "sealed_holdout_untouched": True,
            "strongest_status": "REGISTER",
        },
        "candidate_sha256": work.candidate_sha256,
        "candidate_kind": work.candidate_kind,
        "classification": classification,
        "epoch_sha256": work.epoch_sha256,
        "execution_assumptions": _EXECUTION_ASSUMPTIONS,
        "first_passage_store_sha256": work.first_passage_store_sha256,
        "metrics": metrics,
        "report_metrics": {
            "folds": [
                {
                    "fold": fold,
                    "net_ev_ticks": {
                        "denominator_trades": int(state["fold_trade_counts"][fold]),
                        "numerator_ticks": int(state["fold_net_pnl_ticks"][fold]),
                    },
                    "net_pnl_ticks": int(state["fold_net_pnl_ticks"][fold]),
                    "trade_count": int(state["fold_trade_counts"][fold]),
                }
                for fold in range(work.search_fold_count)
            ],
            "flat_only_trade_count": int(state["sequential_trade_count"]),
            "net_ev_ticks": {
                "denominator_trades": int(state["sequential_trade_count"]),
                "numerator_ticks": int(state["sequential_net_pnl_ticks"]),
            },
            "raw_event_count": int(state["raw_event_count"]),
            "raw_net_pnl_ticks": int(state["raw_net_pnl_ticks"]),
            "raw_tp_count": int(state["raw_tp_count"]),
            "raw_tp_probability_ppm": raw_tp_probability_ppm,
            "sequential_trade_count": int(state["sequential_trade_count"]),
            "stressed_cost_ev_ticks": {
                "denominator_trades": int(state["sequential_trade_count"]),
                "numerator_ticks": int(state["sequential_stressed_net_pnl_ticks"]),
            },
        },
        "signal_artifact_sha256": work.signals.content_sha256,
        "trade_shards": state["trade_shards"],
        "work_spec_sha256": work.sha256,
    }
    payload = canonical_json_bytes(document)
    sha256 = hashlib.sha256(payload).hexdigest()
    relative_uri = f"candidate-result-{sha256}.json"
    _publish_immutable(root, relative_uri, payload)
    return document, sha256, relative_uri, len(payload)


def _pointer_uri(work: CandidateWorkSpec, attempt: WorkerAttempt) -> str:
    return (
        f"worker-{work.sha256}-candidate-{attempt.m0b_candidate_id}-"
        f"attempt-{attempt.research_run_attempt_id}.pointer.json"
    )


def _write_pointer(
    root: Path,
    work: CandidateWorkSpec,
    attempt: WorkerAttempt,
    *,
    checkpoint_sha256: str,
    checkpoint_relative_uri: str,
) -> None:
    document = {
        "artifact_schema": POINTER_SCHEMA,
        "checkpoint_relative_uri": checkpoint_relative_uri,
        "checkpoint_sha256": checkpoint_sha256,
        "m0b_candidate_id": attempt.m0b_candidate_id,
        "research_run_attempt_id": attempt.research_run_attempt_id,
        "work_spec_sha256": work.sha256,
    }
    payload = canonical_json_bytes(document)
    target = root / _pointer_uri(work, attempt)
    descriptor, temporary_name = tempfile.mkstemp(prefix=".m0b-pointer-", dir=root)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        _fsync_directory(root)
    finally:
        temporary.unlink(missing_ok=True)


def _read_canonical_document(
    path: Path,
    *,
    label: str,
    require_read_only: bool = True,
) -> tuple[dict[str, Any], str]:
    if path.is_symlink() or not path.is_file():
        raise M0bWorkerError(f"{label} is absent or symbolic")
    if require_read_only and path.stat().st_mode & _WRITE_BITS:
        raise M0bWorkerError(f"{label} is not immutable")
    payload = path.read_bytes()
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise M0bWorkerError(f"{label} is invalid JSON") from error
    if not isinstance(value, dict) or canonical_json_bytes(value) != payload:
        raise M0bWorkerError(f"{label} is not canonical")
    return value, hashlib.sha256(payload).hexdigest()


def _verify_trade_shards(
    root: Path,
    work: CandidateWorkSpec,
    state: dict[str, Any],
) -> None:
    shards = state.get("trade_shards")
    if not isinstance(shards, list):
        raise M0bWorkerError("checkpoint trade_shards is not an array")
    expected_ordinal = 1
    previous_store_shard = 0
    trades: list[dict[str, Any]] = []
    for identity in shards:
        if not isinstance(identity, dict) or identity.get("ordinal") != expected_ordinal:
            raise M0bWorkerError("trade shard ordinals are not contiguous")
        uri = _leaf(identity.get("relative_uri"), label="trade shard URI")
        content_sha256 = _sha256(identity.get("content_sha256"), label="trade shard SHA-256")
        path = root / uri
        document, observed = _read_canonical_document(path, label="trade shard")
        if (
            observed != content_sha256
            or uri != f"candidate-trades-{expected_ordinal:06d}-{observed}.json"
        ):
            raise M0bWorkerError("trade shard filename/hash differ")
        if (
            document.get("artifact_schema") != TRADE_SHARD_SCHEMA
            or document.get("work_spec_sha256") != work.sha256
            or document.get("ordinal") != expected_ordinal
            or document.get("first_store_shard") != previous_store_shard + 1
            or document.get("last_store_shard") != identity.get("last_store_shard")
            or document.get("row_count") != identity.get("row_count")
            or not isinstance(document.get("trades"), list)
            or len(document["trades"]) != identity.get("row_count")
            or path.stat().st_size != identity.get("byte_size")
        ):
            raise M0bWorkerError("trade shard semantic identity differs")
        previous_store_shard = int(document["last_store_shard"])
        trades.extend(document["trades"])
        expected_ordinal += 1
    if previous_store_shard != int(state["next_shard_ordinal"]) - 1:
        raise M0bWorkerError("trade shards do not cover the checkpoint cursor")
    if len(trades) != int(state["sequential_trade_count"]):
        raise M0bWorkerError("trade shard cardinality differs from checkpoint metrics")
    previous_exit: int | None = None
    net = stressed = tp_count = 0
    active_sessions: set[str] = set()
    folds = [0] * work.search_fold_count
    fold_counts = [0] * work.search_fold_count
    for trade in trades:
        event_ts = _integer(trade.get("event_ts_ns"), label="trade event_ts_ns")
        exit_ts = _integer(trade.get("exit_ts_ns"), label="trade exit_ts_ns")
        if previous_exit is not None and event_ts < previous_exit + work.cooldown_ns:
            raise M0bWorkerError("sequential trade intervals overlap")
        previous_exit = exit_ts
        net += int(trade["net_pnl_ticks"])
        stressed += int(trade["stressed_net_pnl_ticks"])
        tp_count += trade.get("first_touch_type") == "TP_FIRST"
        active_sessions.add(str(trade["session_id"]))
        folds[int(trade["search_fold"])] += int(trade["net_pnl_ticks"])
        fold_counts[int(trade["search_fold"])] += 1
    if (
        net != int(state["sequential_net_pnl_ticks"])
        or stressed != int(state["sequential_stressed_net_pnl_ticks"])
        or tp_count != int(state["accepted_tp_count"])
        or sorted(active_sessions) != state["active_session_ids"]
        or folds != state["fold_net_pnl_ticks"]
        or fold_counts != state["fold_trade_counts"]
        or (previous_exit is None and state["next_available_ts_ns"] is not None)
        or (
            previous_exit is not None
            and state["next_available_ts_ns"] != previous_exit + work.cooldown_ns
        )
    ):
        raise M0bWorkerError("checkpoint sequential state differs from durable trades")


def _verify_result_artifact(
    root: Path,
    work: CandidateWorkSpec,
    state: dict[str, Any],
) -> None:
    identity = state.get("result_artifact")
    if state.get("complete") is not True:
        if identity is not None:
            raise M0bWorkerError("incomplete checkpoint cannot bind a result artifact")
        return
    if not isinstance(identity, dict) or set(identity) != {
        "byte_size",
        "classification",
        "content_sha256",
        "metrics",
        "relative_uri",
    }:
        raise M0bWorkerError("completed checkpoint result identity differs")
    content_sha256 = _sha256(identity.get("content_sha256"), label="checkpoint result SHA-256")
    byte_size = _integer(identity.get("byte_size"), label="checkpoint result byte_size", minimum=1)
    relative_uri = _leaf(identity.get("relative_uri"), label="checkpoint result URI")
    if relative_uri != f"candidate-result-{content_sha256}.json":
        raise M0bWorkerError("checkpoint result filename/hash differ")
    result, observed = _read_canonical_document(root / relative_uri, label="candidate result")
    if (
        observed != content_sha256
        or (root / relative_uri).stat().st_size != byte_size
        or result.get("artifact_schema") != RESULT_SCHEMA
        or result.get("work_spec_sha256") != work.sha256
        or result.get("classification") != identity.get("classification")
        or result.get("metrics") != identity.get("metrics")
    ):
        raise M0bWorkerError("checkpoint result bytes or semantics differ")


def _load_cursor(
    root: Path,
    work: CandidateWorkSpec,
    attempt: WorkerAttempt,
) -> tuple[dict[str, Any], str, str] | None:
    pointer_path = root / _pointer_uri(work, attempt)
    if not pointer_path.exists() and not pointer_path.is_symlink():
        return None
    pointer, _ = _read_canonical_document(
        pointer_path,
        label="worker pointer",
        require_read_only=False,
    )
    if (
        pointer.get("artifact_schema") != POINTER_SCHEMA
        or pointer.get("work_spec_sha256") != work.sha256
        or pointer.get("m0b_candidate_id") != attempt.m0b_candidate_id
        or pointer.get("research_run_attempt_id") != attempt.research_run_attempt_id
    ):
        raise M0bWorkerError("worker pointer identity differs")
    checkpoint_sha256 = _sha256(pointer.get("checkpoint_sha256"), label="checkpoint SHA-256")
    checkpoint_uri = _leaf(pointer.get("checkpoint_relative_uri"), label="checkpoint URI")
    if checkpoint_uri != f"checkpoint-{checkpoint_sha256}.json":
        raise M0bWorkerError("checkpoint URI is not content addressed")
    checkpoint, observed = _read_canonical_document(root / checkpoint_uri, label="checkpoint")
    if observed != checkpoint_sha256 or canonical_sha256(checkpoint) != checkpoint_sha256:
        raise M0bWorkerError("checkpoint content hash differs")
    if (
        checkpoint.get("artifact_schema") != CHECKPOINT_SCHEMA
        or checkpoint.get("m0b_candidate_id") != attempt.m0b_candidate_id
        or checkpoint.get("research_run_attempt_id") != attempt.research_run_attempt_id
    ):
        raise M0bWorkerError("checkpoint control-plane identity differs")
    sequence = _integer(
        checkpoint.get("checkpoint_sequence"), label="checkpoint sequence", minimum=1
    )
    predecessor = checkpoint.get("predecessor_sha256")
    if (sequence == 1 and predecessor is not None) or (
        sequence > 1 and _sha256(predecessor, label="checkpoint predecessor") == ""
    ):
        raise M0bWorkerError("checkpoint predecessor shape differs")
    chain_sequence = sequence
    chain_predecessor = predecessor
    while chain_sequence > 1:
        predecessor_sha256 = _sha256(chain_predecessor, label="checkpoint predecessor")
        predecessor_document, predecessor_observed = _read_canonical_document(
            root / f"checkpoint-{predecessor_sha256}.json",
            label="checkpoint predecessor",
        )
        if (
            predecessor_observed != predecessor_sha256
            or predecessor_document.get("artifact_schema") != CHECKPOINT_SCHEMA
            or predecessor_document.get("m0b_candidate_id") != attempt.m0b_candidate_id
            or predecessor_document.get("research_run_attempt_id")
            != attempt.research_run_attempt_id
            or predecessor_document.get("checkpoint_sequence") != chain_sequence - 1
        ):
            raise M0bWorkerError("checkpoint predecessor chain differs")
        chain_sequence -= 1
        chain_predecessor = predecessor_document.get("predecessor_sha256")
    if chain_predecessor is not None:
        raise M0bWorkerError("checkpoint predecessor chain does not terminate")
    state = checkpoint.get("state")
    if not isinstance(state, dict) or state.get("state_schema") != WORKER_STATE_SCHEMA:
        raise M0bWorkerError("checkpoint state schema differs")
    if state.get("work_spec_sha256") != work.sha256:
        raise M0bWorkerError("checkpoint belongs to another work spec")
    _verify_trade_shards(root, work, state)
    _verify_result_artifact(root, work, state)
    return checkpoint, checkpoint_sha256, checkpoint_uri


def _checkpoint_result(
    checkpoint: dict[str, Any],
    checkpoint_sha256: str,
    checkpoint_uri: str,
) -> WorkerRunResult:
    state = checkpoint["state"]
    result_identity = state.get("result_artifact")
    result_sha256 = None
    result_uri = None
    classification = None
    metrics = None
    if result_identity is not None:
        result_sha256 = str(result_identity["content_sha256"])
        result_uri = str(result_identity["relative_uri"])
        classification = str(result_identity["classification"])
        metrics = dict(result_identity["metrics"])
    return WorkerRunResult(
        work_spec_sha256=str(state["work_spec_sha256"]),
        checkpoint_sha256=checkpoint_sha256,
        checkpoint_relative_uri=checkpoint_uri,
        complete=bool(state["complete"]),
        result_sha256=result_sha256,
        result_relative_uri=result_uri,
        classification=classification,
        metrics=metrics,
    )


def run_candidate_work(
    work: CandidateWorkSpec,
    attempt: WorkerAttempt,
    *,
    first_passage_manifest: str | Path,
    worker_root: str | Path,
    max_checkpoints: int | None = None,
    observer: WorkerObserver | None = None,
) -> WorkerRunResult:
    """Run or resume finite candidate work at deterministic shard boundaries.

    ``max_checkpoints`` only limits this process invocation; it cannot change a
    checkpoint boundary or any result byte.  Calling again resumes the same
    attempt.  This is the worker-recycle hook for a long-lived daemon.
    """

    if not isinstance(work, CandidateWorkSpec) or not isinstance(attempt, WorkerAttempt):
        raise M0bWorkerError("worker requires canonical work and attempt identities")
    if max_checkpoints is not None:
        _integer(max_checkpoints, label="max_checkpoints", minimum=1)
    root = _safe_root(worker_root, label="m0b_worker_root", create=True)
    manifest_path = Path(first_passage_manifest).expanduser()
    store = load_first_passage_store(manifest_path, verify_shards=True)
    if store.sha256 != work.first_passage_store_sha256:
        raise M0bWorkerError("first-passage manifest differs from the work spec")
    if manifest_path.parent.resolve() != root.resolve():
        raise M0bWorkerError("worker inputs and durable state must share the bounded root")
    signals = _load_signals(root, work)
    loaded = _load_cursor(root, work, attempt)
    if loaded is None:
        state = _initial_state(work)
        sequence = 0
        predecessor = None
    else:
        checkpoint, checkpoint_sha256, checkpoint_uri = loaded
        if observer is not None:
            observer.checkpoint_published(
                checkpoint_sha256=checkpoint_sha256,
                checkpoint=checkpoint,
                relative_uri=checkpoint_uri,
            )
        current = _checkpoint_result(checkpoint, checkpoint_sha256, checkpoint_uri)
        if current.complete:
            assert current.result_sha256 is not None and current.result_relative_uri is not None
            result, observed = _read_canonical_document(
                root / current.result_relative_uri,
                label="candidate result",
            )
            if observed != current.result_sha256 or result.get("work_spec_sha256") != work.sha256:
                raise M0bWorkerError("candidate result differs from completed checkpoint")
            if observer is not None:
                observer.result_published(
                    result_sha256=observed,
                    result=result,
                    relative_uri=current.result_relative_uri,
                )
            return current
        state = checkpoint["state"]
        sequence = int(checkpoint["checkpoint_sequence"])
        predecessor = checkpoint_sha256

    published = 0
    latest: WorkerRunResult | None = None
    while int(state["next_shard_ordinal"]) <= len(store.shards):
        first = int(state["next_shard_ordinal"])
        last = min(first + work.checkpoint_shard_interval - 1, len(store.shards))
        next_state, trades = _process_range(
            root,
            store,
            signals,
            work,
            state,
            first_ordinal=first,
            last_ordinal=last,
        )
        trade_identity = _publish_trade_shard(
            root,
            work,
            ordinal=sequence + 1,
            first_store_shard=first,
            last_store_shard=last,
            trades=trades,
        )
        next_state["trade_shards"] = [*state["trade_shards"], trade_identity]
        is_complete = last == len(store.shards)
        if is_complete:
            while int(next_state["next_signal_index"]) < len(signals):
                next_state["missing_label_count"] += 1
                next_state["next_signal_index"] += 1
            next_state["complete"] = True
            result, result_sha256, result_uri, result_byte_size = _publish_result(
                root, work, next_state
            )
            next_state["result_artifact"] = {
                "byte_size": result_byte_size,
                "classification": result["classification"],
                "content_sha256": result_sha256,
                "metrics": result["metrics"],
                "relative_uri": result_uri,
            }
        sequence += 1
        checkpoint = {
            "artifact_schema": CHECKPOINT_SCHEMA,
            "checkpoint_sequence": sequence,
            "m0b_candidate_id": attempt.m0b_candidate_id,
            "predecessor_sha256": predecessor,
            "research_run_attempt_id": attempt.research_run_attempt_id,
            "state": next_state,
        }
        checkpoint_payload = canonical_json_bytes(checkpoint)
        checkpoint_sha256 = hashlib.sha256(checkpoint_payload).hexdigest()
        checkpoint_uri = f"checkpoint-{checkpoint_sha256}.json"
        _publish_immutable(root, checkpoint_uri, checkpoint_payload)
        _write_pointer(
            root,
            work,
            attempt,
            checkpoint_sha256=checkpoint_sha256,
            checkpoint_relative_uri=checkpoint_uri,
        )
        if observer is not None:
            observer.checkpoint_published(
                checkpoint_sha256=checkpoint_sha256,
                checkpoint=checkpoint,
                relative_uri=checkpoint_uri,
            )
        latest = _checkpoint_result(checkpoint, checkpoint_sha256, checkpoint_uri)
        state = next_state
        predecessor = checkpoint_sha256
        published += 1
        if latest.complete:
            assert latest.result_sha256 is not None and latest.result_relative_uri is not None
            result, _ = _read_canonical_document(root / latest.result_relative_uri, label="result")
            if observer is not None:
                observer.result_published(
                    result_sha256=latest.result_sha256,
                    result=result,
                    relative_uri=latest.result_relative_uri,
                )
            return latest
        if max_checkpoints is not None and published >= max_checkpoints:
            return latest
    if latest is None:
        raise M0bWorkerError("empty first-passage store cannot execute candidate work")
    return latest


def run_bounded_daemon_cycle(
    jobs: tuple[CandidateJob, ...],
    *,
    max_jobs: int,
    max_checkpoints_per_job: int,
    observer_factory: Callable[[CandidateJob], WorkerObserver | None] | None = None,
) -> tuple[DaemonJobResult, ...]:
    """Advance a precommitted queue once, isolating individual job failure.

    The caller may keep invoking this cycle indefinitely, but each call has an
    explicit job/checkpoint budget and this function has no candidate creation
    API.  Worker recycling is therefore a process-level concern, not an excuse
    to expand an epoch's immutable search space.
    """

    _integer(max_jobs, label="max_jobs", minimum=1)
    _integer(max_checkpoints_per_job, label="max_checkpoints_per_job", minimum=1)
    if not isinstance(jobs, tuple) or any(not isinstance(job, CandidateJob) for job in jobs):
        raise M0bWorkerError("daemon cycle requires a frozen tuple of candidate jobs")
    if len({job.work.sha256 for job in jobs}) != len(jobs) or len(
        {job.work.candidate_sha256 for job in jobs}
    ) != len(jobs):
        raise M0bWorkerError("daemon cycle cannot contain duplicate work or candidates")
    results: list[DaemonJobResult] = []
    for job in jobs[:max_jobs]:
        job_observer = None if observer_factory is None else observer_factory(job)
        try:
            result = run_candidate_work(
                job.work,
                job.attempt,
                first_passage_manifest=job.first_passage_manifest,
                worker_root=job.worker_root,
                max_checkpoints=max_checkpoints_per_job,
                observer=job_observer,
            )
        except (FirstPassageStoreError, OSError, RuntimeError) as error:
            error_message = f"{type(error).__name__}: {error}"[:4000]
            if job_observer is not None:
                try:
                    job_observer.failure_published(
                        error_message=error_message,
                        retryable=True,
                    )
                except (FirstPassageStoreError, OSError, RuntimeError) as failure_error:
                    error_message = (
                        f"{error_message}; durable failure registration failed: "
                        f"{type(failure_error).__name__}: {failure_error}"
                    )[:4000]
            results.append(
                DaemonJobResult(
                    candidate_sha256=job.work.candidate_sha256,
                    status="FAILED",
                    result=None,
                    error=error_message,
                )
            )
            continue
        results.append(
            DaemonJobResult(
                candidate_sha256=job.work.candidate_sha256,
                status="COMPLETED" if result.complete else "RUNNING",
                result=result,
                error=None,
            )
        )
    return tuple(results)
