"""Deterministic p5 uninterrupted-versus-resumed replay equivalence audit.

The original p5 replay was interrupted and resumed from a source-date
checkpoint.  Before p1_05 may consume that result as a governed predecessor,
this module can execute the same replay from source date one while deliberately
disabling checkpoint loading and all replay-ledger mutations.  The normal
artifact publishers are still used: every daily detail shard, checkpoint, and
the final result must therefore resolve to the already-published immutable
bytes.

The orchestration is intentionally expressed through injected services.  The
database registry owns loading a byte-verified ``SUCCEEDED`` subject and
recording the resulting audit, while this module owns only the deterministic
replay, comparison, and content-addressed audit evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import stat
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path
from typing import Any, Final, Literal

from systematic_fx.db.outcome_registry import EXPECTED_SUMMARY_COUNT, P5_QUERY_ID
from systematic_fx.features.screening import FEATURE_VERSION, load_phase1a_screening_config
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.outcome_config import OUTCOME_CONFIG_RELATIVE_PATH
from systematic_fx.research.phase1a_outcome_pipeline import (
    OutcomePipelineServices,
    OutcomeProgress,
    PreparedOutcomeInputs,
    _prepare_inputs,
    _report_sha,
    _resolve_terminals,
    _run_replay,
    _strict_root,
    _validate_cache_reports,
)
from systematic_fx.research.phase1a_outcome_pipeline import (
    _data_layout as _pipeline_data_layout,
)
from systematic_fx.research.phase1a_outcome_pipeline import (
    _default_services as _default_pipeline_services,
)
from systematic_fx.research.run_spec import RunSpec
from systematic_fx.validation.splits import (
    CALENDAR_VERSION,
    CAMPAIGN_ID,
    SPLIT_VERSION,
)

AUDIT_SCHEMA: Final = "systematic_fx.phase1a_p5_outcome_equivalence_audit.v1"
AUDIT_VERSION: Final = "phase1a_p5_outcome_equivalence_v1"
AUDIT_KIND: Final = "UNINTERRUPTED_VS_RESUMED_BYTE_EQUIVALENCE"
AUDIT_DIRECTORY: Final = Path("outcomes/audits") / AUDIT_VERSION
AUDIT_ENGINE_VERSION: Final = "phase1a_outcome_equivalence_audit_v1"
AUDIT_RANDOM_SEED: Final = 0

_SHA256: Final = re.compile(r"[0-9a-f]{64}")
_WRITE_BITS: Final = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH


class OutcomeEquivalenceAuditError(RuntimeError):
    """The governed p5 equivalence audit is invalid or could not complete."""


def _sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise OutcomeEquivalenceAuditError(f"{label} must be a lowercase SHA-256")
    return value


def _positive_integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise OutcomeEquivalenceAuditError(f"{label} must be a positive integer")
    return value


def _nonnegative_integer(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise OutcomeEquivalenceAuditError(f"{label} must be a nonnegative integer")
    return value


def _source_date(value: object, *, label: str) -> date:
    if isinstance(value, datetime):
        raise OutcomeEquivalenceAuditError(f"{label} must not be a datetime")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise OutcomeEquivalenceAuditError(f"{label} must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise OutcomeEquivalenceAuditError(f"{label} must be an ISO date") from error
    if parsed.isoformat() != value:
        raise OutcomeEquivalenceAuditError(f"{label} must be a canonical ISO date")
    return parsed


def _member(value: object, *names: str, default: object = None) -> object:
    """Read one field from a mapping or a registry dataclass."""

    for name in names:
        if isinstance(value, Mapping) and name in value:
            return value[name]
        if hasattr(value, name):
            return getattr(value, name)
    return default


@dataclass(frozen=True, slots=True)
class OutcomeEquivalenceCheckpoint:
    """One DB-verified checkpoint identity in the expected or observed chain."""

    checkpoint_sequence: int
    checkpoint_artifact_sha256: str
    checkpoint_artifact_byte_size: int
    predecessor_checkpoint_sha256: str | None
    last_completed_source_date: date
    source_event_count: int
    progress_metadata_sha256: str

    def __post_init__(self) -> None:
        _positive_integer(self.checkpoint_sequence, label="checkpoint_sequence")
        _sha256(self.checkpoint_artifact_sha256, label="checkpoint_artifact_sha256")
        _positive_integer(
            self.checkpoint_artifact_byte_size,
            label="checkpoint_artifact_byte_size",
        )
        if self.predecessor_checkpoint_sha256 is not None:
            _sha256(
                self.predecessor_checkpoint_sha256,
                label="predecessor_checkpoint_sha256",
            )
        _source_date(self.last_completed_source_date, label="last_completed_source_date")
        _positive_integer(self.source_event_count, label="source_event_count")
        _sha256(self.progress_metadata_sha256, label="progress_metadata_sha256")

    def as_dict(self) -> dict[str, object]:
        return {
            "checkpoint_artifact_byte_size": self.checkpoint_artifact_byte_size,
            "checkpoint_artifact_sha256": self.checkpoint_artifact_sha256,
            "checkpoint_sequence": self.checkpoint_sequence,
            "last_completed_source_date": self.last_completed_source_date.isoformat(),
            "predecessor_checkpoint_sha256": self.predecessor_checkpoint_sha256,
            "progress_metadata_sha256": self.progress_metadata_sha256,
            "source_event_count": self.source_event_count,
        }


def checkpoint_chain_sha256(
    checkpoints: Sequence[OutcomeEquivalenceCheckpoint],
) -> str:
    """Return the canonical digest of a complete ordered checkpoint chain."""

    values = tuple(checkpoints)
    if not values:
        raise OutcomeEquivalenceAuditError("checkpoint chain cannot be empty")
    predecessor: str | None = None
    previous_date: date | None = None
    previous_events = 0
    for expected_sequence, checkpoint in enumerate(values, start=1):
        if not isinstance(checkpoint, OutcomeEquivalenceCheckpoint):
            raise OutcomeEquivalenceAuditError(
                "checkpoint chain must contain OutcomeEquivalenceCheckpoint values"
            )
        if checkpoint.checkpoint_sequence != expected_sequence:
            raise OutcomeEquivalenceAuditError("checkpoint chain is not contiguous")
        if checkpoint.predecessor_checkpoint_sha256 != predecessor:
            raise OutcomeEquivalenceAuditError("checkpoint predecessor chain is broken")
        if previous_date is not None and checkpoint.last_completed_source_date <= previous_date:
            raise OutcomeEquivalenceAuditError("checkpoint source dates are not strictly ordered")
        if checkpoint.source_event_count <= previous_events:
            raise OutcomeEquivalenceAuditError(
                "checkpoint event counts are not strictly increasing"
            )
        predecessor = checkpoint.checkpoint_artifact_sha256
        previous_date = checkpoint.last_completed_source_date
        previous_events = checkpoint.source_event_count
    return canonical_sha256([checkpoint.as_dict() for checkpoint in values])


@dataclass(frozen=True, slots=True)
class OutcomeEquivalenceSubject:
    """Byte-verified, successful p5 replay identity loaded by the registry."""

    outcome_replay_manifest_id: int
    research_run_spec_id: int
    research_run_attempt_id: int
    status: str
    query_id: str
    outcome_config_id: str
    run_fingerprint: str
    source_artifact_manifest_sha256: str
    cache_manifest_sha256: str
    result_artifact_sha256: str
    result_artifact_byte_size: int
    cell_summaries_sha256: str
    detail_shard_manifest_sha256: str
    input_lineage_sha256: str
    final_checkpoint_sha256: str
    final_checkpoint_sequence: int
    source_event_count: int
    detail_record_count: int
    summary_row_count: int
    checkpoints: tuple[OutcomeEquivalenceCheckpoint, ...]

    def __post_init__(self) -> None:
        for value, label in (
            (self.outcome_replay_manifest_id, "outcome_replay_manifest_id"),
            (self.research_run_spec_id, "research_run_spec_id"),
            (self.research_run_attempt_id, "research_run_attempt_id"),
            (self.result_artifact_byte_size, "result_artifact_byte_size"),
            (self.final_checkpoint_sequence, "final_checkpoint_sequence"),
            (self.source_event_count, "source_event_count"),
            (self.detail_record_count, "detail_record_count"),
            (self.summary_row_count, "summary_row_count"),
        ):
            _positive_integer(value, label=label)
        if self.status != "SUCCEEDED":
            raise OutcomeEquivalenceAuditError("equivalence subject must be SUCCEEDED")
        if self.query_id != P5_QUERY_ID:
            raise OutcomeEquivalenceAuditError("equivalence subject must be the frozen p5 query")
        if self.summary_row_count != EXPECTED_SUMMARY_COUNT:
            raise OutcomeEquivalenceAuditError("equivalence subject summary grid is incomplete")
        if not isinstance(self.outcome_config_id, str) or not self.outcome_config_id:
            raise OutcomeEquivalenceAuditError("outcome_config_id must be non-empty")
        for value, label in (
            (self.run_fingerprint, "run_fingerprint"),
            (self.source_artifact_manifest_sha256, "source_artifact_manifest_sha256"),
            (self.cache_manifest_sha256, "cache_manifest_sha256"),
            (self.result_artifact_sha256, "result_artifact_sha256"),
            (self.cell_summaries_sha256, "cell_summaries_sha256"),
            (self.detail_shard_manifest_sha256, "detail_shard_manifest_sha256"),
            (self.input_lineage_sha256, "input_lineage_sha256"),
            (self.final_checkpoint_sha256, "final_checkpoint_sha256"),
        ):
            _sha256(value, label=label)
        _ = checkpoint_chain_sha256(self.checkpoints)
        final = self.checkpoints[-1]
        if (
            len(self.checkpoints) != self.final_checkpoint_sequence
            or final.checkpoint_sequence != self.final_checkpoint_sequence
            or final.checkpoint_artifact_sha256 != self.final_checkpoint_sha256
            or final.source_event_count != self.source_event_count
        ):
            raise OutcomeEquivalenceAuditError(
                "equivalence subject final checkpoint differs from its chain"
            )

    @property
    def checkpoint_chain_sha256(self) -> str:
        return checkpoint_chain_sha256(self.checkpoints)

    def as_dict(self) -> dict[str, object]:
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
            "outcome_config_id": self.outcome_config_id,
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


def _checkpoint_from_registry(value: object) -> OutcomeEquivalenceCheckpoint:
    progress = _member(value, "progress_metadata", default={})
    progress_sha256 = _member(value, "progress_metadata_sha256")
    if progress_sha256 is None:
        if not isinstance(progress, Mapping):
            raise OutcomeEquivalenceAuditError("checkpoint progress metadata is invalid")
        progress_sha256 = canonical_sha256(progress)
    return OutcomeEquivalenceCheckpoint(
        checkpoint_sequence=_positive_integer(
            _member(value, "checkpoint_sequence"),
            label="checkpoint_sequence",
        ),
        checkpoint_artifact_sha256=_sha256(
            _member(value, "checkpoint_artifact_sha256", "artifact_sha256", "sha256"),
            label="checkpoint_artifact_sha256",
        ),
        checkpoint_artifact_byte_size=_positive_integer(
            _member(value, "checkpoint_artifact_byte_size", "artifact_byte_size", "byte_size"),
            label="checkpoint_artifact_byte_size",
        ),
        predecessor_checkpoint_sha256=(
            None
            if _member(value, "predecessor_checkpoint_sha256") is None
            else _sha256(
                _member(value, "predecessor_checkpoint_sha256"),
                label="predecessor_checkpoint_sha256",
            )
        ),
        last_completed_source_date=_source_date(
            _member(value, "last_completed_source_date"),
            label="last_completed_source_date",
        ),
        source_event_count=_positive_integer(
            _member(value, "source_event_count"),
            label="source_event_count",
        ),
        progress_metadata_sha256=_sha256(
            progress_sha256,
            label="progress_metadata_sha256",
        ),
    )


def outcome_equivalence_subject_from_registry(value: object) -> OutcomeEquivalenceSubject:
    """Normalize a registry mapping/dataclass without coupling to its concrete type."""

    raw_checkpoints = _member(value, "checkpoints", "checkpoint_chain")
    if isinstance(raw_checkpoints, (str, bytes)) or not isinstance(raw_checkpoints, Sequence):
        raise OutcomeEquivalenceAuditError("registry subject checkpoints must be a sequence")
    checkpoints = tuple(_checkpoint_from_registry(item) for item in raw_checkpoints)
    if not checkpoints:
        raise OutcomeEquivalenceAuditError("registry subject checkpoint chain is empty")
    final_progress = _member(raw_checkpoints[-1], "progress_metadata", default={})
    if not isinstance(final_progress, Mapping):
        final_progress = {}
    detail_count = _member(value, "detail_record_count")
    if detail_count is None:
        detail_count = final_progress.get("detail_record_count")
    summary_count = _member(value, "summary_row_count", default=EXPECTED_SUMMARY_COUNT)
    source_event_count = _member(value, "source_event_count")
    if source_event_count is None:
        source_event_count = checkpoints[-1].source_event_count
    outcome_config_id = _member(
        value,
        "outcome_config_id",
        default="phase1a_p5_outcome_replay_v1",
    )
    return OutcomeEquivalenceSubject(
        outcome_replay_manifest_id=_positive_integer(
            _member(value, "outcome_replay_manifest_id"),
            label="outcome_replay_manifest_id",
        ),
        research_run_spec_id=_positive_integer(
            _member(value, "research_run_spec_id"),
            label="research_run_spec_id",
        ),
        research_run_attempt_id=_positive_integer(
            _member(value, "research_run_attempt_id"),
            label="research_run_attempt_id",
        ),
        status=str(_member(value, "status")),
        query_id=str(_member(value, "query_id", default=P5_QUERY_ID)),
        outcome_config_id=str(outcome_config_id),
        run_fingerprint=_sha256(
            _member(value, "run_fingerprint"),
            label="run_fingerprint",
        ),
        source_artifact_manifest_sha256=_sha256(
            _member(value, "source_artifact_manifest_sha256"),
            label="source_artifact_manifest_sha256",
        ),
        cache_manifest_sha256=_sha256(
            _member(value, "cache_manifest_sha256"),
            label="cache_manifest_sha256",
        ),
        result_artifact_sha256=_sha256(
            _member(value, "result_artifact_sha256"),
            label="result_artifact_sha256",
        ),
        result_artifact_byte_size=_positive_integer(
            _member(value, "result_artifact_byte_size"),
            label="result_artifact_byte_size",
        ),
        cell_summaries_sha256=_sha256(
            _member(value, "cell_summaries_sha256"),
            label="cell_summaries_sha256",
        ),
        detail_shard_manifest_sha256=_sha256(
            _member(value, "detail_shard_manifest_sha256"),
            label="detail_shard_manifest_sha256",
        ),
        input_lineage_sha256=_sha256(
            _member(value, "input_lineage_sha256"),
            label="input_lineage_sha256",
        ),
        final_checkpoint_sha256=_sha256(
            _member(value, "final_checkpoint_sha256"),
            label="final_checkpoint_sha256",
        ),
        final_checkpoint_sequence=_positive_integer(
            _member(value, "final_checkpoint_sequence"),
            label="final_checkpoint_sequence",
        ),
        source_event_count=_positive_integer(
            source_event_count,
            label="source_event_count",
        ),
        detail_record_count=_positive_integer(
            detail_count,
            label="detail_record_count",
        ),
        summary_row_count=_positive_integer(
            summary_count,
            label="summary_row_count",
        ),
        checkpoints=checkpoints,
    )


@dataclass(frozen=True, slots=True)
class OutcomeEquivalenceObservation:
    """All identities independently produced by the uninterrupted replay."""

    outcome_replay_manifest_id: int
    run_fingerprint: str
    cache_manifest_sha256: str
    result_artifact_sha256: str
    result_artifact_byte_size: int
    cell_summaries_sha256: str
    detail_shard_manifest_sha256: str
    input_lineage_sha256: str
    final_checkpoint_sha256: str
    final_checkpoint_sequence: int
    source_event_count: int
    detail_record_count: int
    summary_row_count: int
    checkpoints: tuple[OutcomeEquivalenceCheckpoint, ...]
    detail_shard_publication_count: int
    detail_shard_reused_count: int
    checkpoint_publication_count: int
    checkpoint_reused_count: int
    final_result_disposition: str
    checkpoint_load_count: int
    start_noop_count: int
    complete_noop_count: int

    def __post_init__(self) -> None:
        _positive_integer(
            self.outcome_replay_manifest_id,
            label="outcome_replay_manifest_id",
        )
        for value, label in (
            (self.run_fingerprint, "run_fingerprint"),
            (self.cache_manifest_sha256, "cache_manifest_sha256"),
            (self.result_artifact_sha256, "result_artifact_sha256"),
            (self.cell_summaries_sha256, "cell_summaries_sha256"),
            (self.detail_shard_manifest_sha256, "detail_shard_manifest_sha256"),
            (self.input_lineage_sha256, "input_lineage_sha256"),
            (self.final_checkpoint_sha256, "final_checkpoint_sha256"),
        ):
            _sha256(value, label=label)
        for value, label in (
            (self.result_artifact_byte_size, "result_artifact_byte_size"),
            (self.final_checkpoint_sequence, "final_checkpoint_sequence"),
            (self.source_event_count, "source_event_count"),
            (self.detail_record_count, "detail_record_count"),
            (self.summary_row_count, "summary_row_count"),
        ):
            _positive_integer(value, label=label)
        for value, label in (
            (self.detail_shard_publication_count, "detail_shard_publication_count"),
            (self.detail_shard_reused_count, "detail_shard_reused_count"),
            (self.checkpoint_publication_count, "checkpoint_publication_count"),
            (self.checkpoint_reused_count, "checkpoint_reused_count"),
            (self.checkpoint_load_count, "checkpoint_load_count"),
            (self.start_noop_count, "start_noop_count"),
            (self.complete_noop_count, "complete_noop_count"),
        ):
            _nonnegative_integer(value, label=label)
        if self.summary_row_count != EXPECTED_SUMMARY_COUNT:
            raise OutcomeEquivalenceAuditError("observed summary grid is incomplete")
        if self.final_result_disposition not in {"CREATED", "REUSED"}:
            raise OutcomeEquivalenceAuditError("final result disposition is invalid")
        _ = checkpoint_chain_sha256(self.checkpoints)
        final = self.checkpoints[-1]
        if (
            final.checkpoint_artifact_sha256 != self.final_checkpoint_sha256
            or final.checkpoint_sequence != self.final_checkpoint_sequence
            or final.source_event_count != self.source_event_count
        ):
            raise OutcomeEquivalenceAuditError(
                "observed final checkpoint differs from its checkpoint chain"
            )

    @property
    def checkpoint_chain_sha256(self) -> str:
        return checkpoint_chain_sha256(self.checkpoints)

    def as_dict(self) -> dict[str, object]:
        return {
            "cache_manifest_sha256": self.cache_manifest_sha256,
            "cell_summaries_sha256": self.cell_summaries_sha256,
            "checkpoint_chain_sha256": self.checkpoint_chain_sha256,
            "checkpoint_count": len(self.checkpoints),
            "checkpoint_load_count": self.checkpoint_load_count,
            "checkpoint_publication_count": self.checkpoint_publication_count,
            "checkpoint_reused_count": self.checkpoint_reused_count,
            "complete_noop_count": self.complete_noop_count,
            "detail_record_count": self.detail_record_count,
            "detail_shard_manifest_sha256": self.detail_shard_manifest_sha256,
            "detail_shard_publication_count": self.detail_shard_publication_count,
            "detail_shard_reused_count": self.detail_shard_reused_count,
            "final_checkpoint_sequence": self.final_checkpoint_sequence,
            "final_checkpoint_sha256": self.final_checkpoint_sha256,
            "final_result_disposition": self.final_result_disposition,
            "input_lineage_sha256": self.input_lineage_sha256,
            "outcome_replay_manifest_id": self.outcome_replay_manifest_id,
            "result_artifact_byte_size": self.result_artifact_byte_size,
            "result_artifact_sha256": self.result_artifact_sha256,
            "run_fingerprint": self.run_fingerprint,
            "source_event_count": self.source_event_count,
            "start_noop_count": self.start_noop_count,
            "summary_row_count": self.summary_row_count,
        }


@dataclass(frozen=True, slots=True)
class OutcomeEquivalenceAuditReport:
    """Portable deterministic comparison persisted as the audit artifact."""

    subject: OutcomeEquivalenceSubject
    observed: OutcomeEquivalenceObservation
    comparisons: Mapping[str, bool]
    mismatches: tuple[str, ...]
    passed: bool

    def __post_init__(self) -> None:
        canonical_comparisons = dict(sorted(self.comparisons.items()))
        if (
            not canonical_comparisons
            or any(
                not isinstance(key, str) or not isinstance(value, bool)
                for key, value in canonical_comparisons.items()
            )
            or tuple(sorted(key for key, value in canonical_comparisons.items() if not value))
            != self.mismatches
            or self.passed != all(canonical_comparisons.values())
        ):
            raise OutcomeEquivalenceAuditError("audit comparison result is internally inconsistent")

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": AUDIT_SCHEMA,
            "audit_kind": AUDIT_KIND,
            "audit_version": AUDIT_VERSION,
            "comparisons": dict(sorted(self.comparisons.items())),
            "execution_policy": {
                "checkpoint_load": "FORCED_NONE",
                "control_plane_mutations": "NO_OP",
                "daily_artifact_policy": "MUST_REUSE_IDENTICAL_CONTENT",
                "replay_start": "FIRST_PLANNED_SOURCE_DATE",
            },
            "mismatches": list(self.mismatches),
            "observed": self.observed.as_dict(),
            "passed": self.passed,
            "query_id": P5_QUERY_ID,
            "subject": self.subject.as_dict(),
        }


def compare_outcome_equivalence(
    subject: OutcomeEquivalenceSubject,
    observed: OutcomeEquivalenceObservation,
) -> OutcomeEquivalenceAuditReport:
    """Compare every governed final and daily-chain identity."""

    comparisons = {
        "cache_manifest_sha256": (observed.cache_manifest_sha256 == subject.cache_manifest_sha256),
        "cell_summaries_sha256": (observed.cell_summaries_sha256 == subject.cell_summaries_sha256),
        "checkpoint_chain_sha256": (
            len(observed.checkpoints) == len(subject.checkpoints)
            and observed.checkpoint_chain_sha256 == subject.checkpoint_chain_sha256
        ),
        "checkpoints_all_reused": (
            observed.checkpoint_publication_count == len(subject.checkpoints)
            and observed.checkpoint_reused_count == len(subject.checkpoints)
        ),
        "control_plane_noops": (
            observed.start_noop_count == 1 and observed.complete_noop_count == 1
        ),
        "detail_record_count": observed.detail_record_count == subject.detail_record_count,
        "detail_shard_manifest_sha256": (
            observed.detail_shard_manifest_sha256 == subject.detail_shard_manifest_sha256
        ),
        "detail_shards_all_reused": (
            observed.detail_shard_publication_count == len(subject.checkpoints)
            and observed.detail_shard_reused_count == len(subject.checkpoints)
        ),
        "final_checkpoint_sequence": (
            observed.final_checkpoint_sequence == subject.final_checkpoint_sequence
        ),
        "final_checkpoint_sha256": (
            observed.final_checkpoint_sha256 == subject.final_checkpoint_sha256
        ),
        "final_result_reused": observed.final_result_disposition == "REUSED",
        "forced_uninterrupted_start": observed.checkpoint_load_count == 1,
        "input_lineage_sha256": (observed.input_lineage_sha256 == subject.input_lineage_sha256),
        "manifest_identity": (
            observed.outcome_replay_manifest_id == subject.outcome_replay_manifest_id
        ),
        "result_artifact_byte_size": (
            observed.result_artifact_byte_size == subject.result_artifact_byte_size
        ),
        "result_artifact_sha256": (
            observed.result_artifact_sha256 == subject.result_artifact_sha256
        ),
        "run_fingerprint": observed.run_fingerprint == subject.run_fingerprint,
        "source_event_count": observed.source_event_count == subject.source_event_count,
        "summary_row_count": observed.summary_row_count == subject.summary_row_count,
    }
    mismatches = tuple(sorted(key for key, matches in comparisons.items() if not matches))
    return OutcomeEquivalenceAuditReport(
        subject=subject,
        observed=observed,
        comparisons=comparisons,
        mismatches=mismatches,
        passed=not mismatches,
    )


@dataclass(frozen=True, slots=True)
class PublishedOutcomeEquivalenceAudit:
    """One immutable content-addressed audit JSON publication."""

    path: Path
    relative_uri: str
    sha256: str
    byte_size: int
    disposition: Literal["CREATED", "REUSED"]
    report: OutcomeEquivalenceAuditReport


def _data_layout(data_root: Path | str) -> tuple[Path, Path]:
    requested = Path(data_root)
    if not requested.is_absolute():
        requested = Path.cwd() / requested
    lexical = Path(os.path.abspath(os.fspath(requested)))
    try:
        resolved = lexical.resolve(strict=True)
        mode = lexical.lstat().st_mode
    except OSError as error:
        raise OutcomeEquivalenceAuditError("data_root does not exist") from error
    if (
        resolved != lexical
        or lexical.name != "data"
        or not stat.S_ISDIR(mode)
        or stat.S_ISLNK(mode)
    ):
        raise OutcomeEquivalenceAuditError("data_root must be the real non-symlink data directory")
    derived = lexical / "derived"
    try:
        derived_mode = derived.lstat().st_mode
    except OSError as error:
        raise OutcomeEquivalenceAuditError("data/derived does not exist") from error
    if not stat.S_ISDIR(derived_mode) or stat.S_ISLNK(derived_mode):
        raise OutcomeEquivalenceAuditError("data/derived must be a real directory")
    return lexical, derived


@dataclass(frozen=True, slots=True)
class _NodeIdentity:
    device: int
    inode: int
    file_type: int


@dataclass(frozen=True, slots=True)
class _FileIdentity:
    device: int
    inode: int
    mode: int
    size: int
    modified_ns: int
    changed_ns: int


def _node_identity(value: os.stat_result) -> _NodeIdentity:
    return _NodeIdentity(
        device=value.st_dev,
        inode=value.st_ino,
        file_type=stat.S_IFMT(value.st_mode),
    )


def _file_identity(value: os.stat_result) -> _FileIdentity:
    return _FileIdentity(
        device=value.st_dev,
        inode=value.st_ino,
        mode=value.st_mode,
        size=value.st_size,
        modified_ns=value.st_mtime_ns,
        changed_ns=value.st_ctime_ns,
    )


def _directory_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


@dataclass(slots=True)
class _HeldAuditDirectory:
    data_root: Path
    derived: Path
    path: Path
    paths: tuple[Path, ...]
    names: tuple[str | None, ...]
    descriptors: tuple[int, ...]
    identities: tuple[_NodeIdentity, ...]

    @property
    def descriptor(self) -> int:
        return self.descriptors[-1]

    def close(self) -> None:
        for descriptor in reversed(self.descriptors):
            if descriptor >= 0:
                os.close(descriptor)
        self.descriptors = ()


def _verify_audit_directory(held: _HeldAuditDirectory) -> None:
    if not held.descriptors:
        raise OutcomeEquivalenceAuditError("audit directory is no longer held")
    for index, (path, name, descriptor, identity) in enumerate(
        zip(
            held.paths,
            held.names,
            held.descriptors,
            held.identities,
            strict=True,
        )
    ):
        try:
            opened = os.fstat(descriptor)
            lexical = path.lstat()
            relative = (
                None
                if index == 0
                else os.stat(
                    str(name),
                    dir_fd=held.descriptors[index - 1],
                    follow_symlinks=False,
                )
            )
        except OSError as error:
            raise OutcomeEquivalenceAuditError("audit directory identity disappeared") from error
        if (
            not stat.S_ISDIR(opened.st_mode)
            or stat.S_ISLNK(lexical.st_mode)
            or _node_identity(opened) != identity
            or _node_identity(lexical) != identity
            or relative is not None
            and (stat.S_ISLNK(relative.st_mode) or _node_identity(relative) != identity)
        ):
            raise OutcomeEquivalenceAuditError("audit directory identity changed while held")


def _open_audit_directory(
    data_root: Path | str,
    *,
    create: bool,
) -> _HeldAuditDirectory:
    root, derived = _data_layout(data_root)
    paths = [root, derived]
    paths.extend(
        derived / Path(*AUDIT_DIRECTORY.parts[:index])
        for index in range(1, len(AUDIT_DIRECTORY.parts) + 1)
    )
    names: list[str | None] = [None, "derived", *AUDIT_DIRECTORY.parts]
    descriptors: list[int] = []
    identities: list[_NodeIdentity] = []
    try:
        before_root = root.lstat()
        root_descriptor = os.open(root, _directory_flags())
        after_root = root.lstat()
        opened_root = os.fstat(root_descriptor)
        root_identity = _node_identity(before_root)
        if (
            stat.S_ISLNK(before_root.st_mode)
            or not stat.S_ISDIR(opened_root.st_mode)
            or _node_identity(after_root) != root_identity
            or _node_identity(opened_root) != root_identity
        ):
            os.close(root_descriptor)
            raise OutcomeEquivalenceAuditError(
                "data_root identity changed while opening the audit namespace"
            )
        descriptors.append(root_descriptor)
        identities.append(root_identity)
        for index, name in enumerate(names[1:], start=1):
            parent_descriptor = descriptors[-1]
            if create and index >= 2:
                try:
                    os.mkdir(name, mode=0o755, dir_fd=parent_descriptor)
                except FileExistsError:
                    pass
            try:
                before = os.stat(
                    name,
                    dir_fd=parent_descriptor,
                    follow_symlinks=False,
                )
                descriptor = os.open(
                    name,
                    _directory_flags(),
                    dir_fd=parent_descriptor,
                )
                opened = os.fstat(descriptor)
                after = os.stat(
                    name,
                    dir_fd=parent_descriptor,
                    follow_symlinks=False,
                )
            except OSError as error:
                raise OutcomeEquivalenceAuditError(
                    "cannot securely open the audit namespace"
                ) from error
            identity = _node_identity(before)
            if (
                stat.S_ISLNK(before.st_mode)
                or not stat.S_ISDIR(opened.st_mode)
                or _node_identity(opened) != identity
                or _node_identity(after) != identity
            ):
                os.close(descriptor)
                raise OutcomeEquivalenceAuditError(
                    "audit namespace component changed while opening"
                )
            descriptors.append(descriptor)
            identities.append(identity)
        held = _HeldAuditDirectory(
            data_root=root,
            derived=derived,
            path=derived / AUDIT_DIRECTORY,
            paths=tuple(paths),
            names=tuple(names),
            descriptors=tuple(descriptors),
            identities=tuple(identities),
        )
        _verify_audit_directory(held)
        return held
    except Exception:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
        raise


def _descriptor_bytes(descriptor: int, byte_size: int) -> bytes:
    chunks: list[bytes] = []
    offset = 0
    while offset < byte_size:
        chunk = os.pread(descriptor, min(1024 * 1024, byte_size - offset), offset)
        if not chunk:
            break
        chunks.append(chunk)
        offset += len(chunk)
    if offset != byte_size:
        raise OutcomeEquivalenceAuditError("audit artifact became truncated while reading")
    return b"".join(chunks)


def _read_held_audit_file(
    directory: _HeldAuditDirectory,
    *,
    filename: str,
    expected_sha256: str,
    expected_content: bytes | None = None,
) -> tuple[Path, bytes, _FileIdentity]:
    try:
        before = os.stat(
            filename,
            dir_fd=directory.descriptor,
            follow_symlinks=False,
        )
        descriptor = os.open(
            filename,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=directory.descriptor,
        )
    except OSError as error:
        raise OutcomeEquivalenceAuditError("cannot securely open the audit artifact") from error
    try:
        opened = os.fstat(descriptor)
        identity = _file_identity(opened)
        content = _descriptor_bytes(descriptor, opened.st_size)
        after_open = os.fstat(descriptor)
        after_entry = os.stat(
            filename,
            dir_fd=directory.descriptor,
            follow_symlinks=False,
        )
        path = directory.path / filename
        lexical = path.lstat()
        if (
            not stat.S_ISREG(opened.st_mode)
            or stat.S_ISLNK(before.st_mode)
            or opened.st_mode & _WRITE_BITS
            or _file_identity(before) != identity
            or _file_identity(after_open) != identity
            or _file_identity(after_entry) != identity
            or _file_identity(lexical) != identity
            or hashlib.sha256(content).hexdigest() != expected_sha256
            or expected_content is not None
            and content != expected_content
        ):
            raise OutcomeEquivalenceAuditError(
                "audit artifact identity or content changed while held"
            )
        _verify_audit_directory(directory)
        return path, content, identity
    except OSError as error:
        raise OutcomeEquivalenceAuditError("audit artifact path identity disappeared") from error
    finally:
        os.close(descriptor)


def _load_audit_bytes(
    *,
    data_root: Path | str,
    digest: str,
    expected_content: bytes | None = None,
) -> tuple[Path, str, bytes]:
    directory = _open_audit_directory(data_root, create=False)
    try:
        filename = f"sha256={digest}.json"
        path, content, identity = _read_held_audit_file(
            directory,
            filename=filename,
            expected_sha256=digest,
            expected_content=expected_content,
        )
        _verify_audit_directory(directory)
        try:
            if _file_identity(path.lstat()) != identity:
                raise OutcomeEquivalenceAuditError(
                    "audit artifact lexical identity changed before return"
                )
        except OSError as error:
            raise OutcomeEquivalenceAuditError(
                "audit artifact disappeared before return"
            ) from error
        return path, path.relative_to(directory.derived).as_posix(), content
    finally:
        directory.close()


def publish_outcome_equivalence_audit(
    report: OutcomeEquivalenceAuditReport,
    *,
    data_root: Path | str,
) -> PublishedOutcomeEquivalenceAudit:
    """Atomically publish canonical, read-only, content-addressed audit evidence."""

    if not isinstance(report, OutcomeEquivalenceAuditReport):
        raise OutcomeEquivalenceAuditError("report must be an OutcomeEquivalenceAuditReport")
    content = canonical_json_bytes(report.as_dict()) + b"\n"
    digest = hashlib.sha256(content).hexdigest()
    directory = _open_audit_directory(data_root, create=True)
    temporary_name: str | None = None
    try:
        descriptor: int | None = None
        for _ in range(128):
            candidate = f".equivalence-audit-{secrets.token_hex(16)}.tmp"
            try:
                descriptor = os.open(
                    candidate,
                    os.O_RDWR
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=directory.descriptor,
                )
            except FileExistsError:
                continue
            temporary_name = candidate
            break
        if descriptor is None or temporary_name is None:
            raise OutcomeEquivalenceAuditError("cannot allocate an audit temporary file")
        try:
            offset = 0
            while offset < len(content):
                written = os.write(descriptor, content[offset:])
                if written <= 0:
                    raise OutcomeEquivalenceAuditError("audit temporary write made no progress")
                offset += written
            os.fsync(descriptor)
            os.fchmod(descriptor, 0o444)
            if _descriptor_bytes(descriptor, len(content)) != content:
                raise OutcomeEquivalenceAuditError("audit temporary content drift")
        finally:
            os.close(descriptor)
        target_name = f"sha256={digest}.json"
        disposition: Literal["CREATED", "REUSED"] = "CREATED"
        try:
            os.link(
                temporary_name,
                target_name,
                src_dir_fd=directory.descriptor,
                dst_dir_fd=directory.descriptor,
                follow_symlinks=False,
            )
        except FileExistsError:
            disposition = "REUSED"
        _read_held_audit_file(
            directory,
            filename=target_name,
            expected_sha256=digest,
            expected_content=content,
        )
        os.fsync(directory.descriptor)
        _verify_audit_directory(directory)
    finally:
        if temporary_name is not None:
            try:
                os.unlink(temporary_name, dir_fd=directory.descriptor)
                os.fsync(directory.descriptor)
            except FileNotFoundError:
                pass
        directory.close()
    target, relative_uri, verified_content = _load_audit_bytes(
        data_root=data_root,
        digest=digest,
        expected_content=content,
    )
    if verified_content != content:  # pragma: no cover - helper already proves equality
        raise OutcomeEquivalenceAuditError("published audit content drift")
    return PublishedOutcomeEquivalenceAudit(
        path=target,
        relative_uri=relative_uri,
        sha256=digest,
        byte_size=len(content),
        disposition=disposition,
        report=report,
    )


def _observation_from_document(
    value: object,
    *,
    subject: OutcomeEquivalenceSubject,
) -> OutcomeEquivalenceObservation:
    if not isinstance(value, Mapping):
        raise OutcomeEquivalenceAuditError("audit observed identity must be an object")
    return OutcomeEquivalenceObservation(
        outcome_replay_manifest_id=_positive_integer(
            value.get("outcome_replay_manifest_id"),
            label="outcome_replay_manifest_id",
        ),
        run_fingerprint=_sha256(value.get("run_fingerprint"), label="run_fingerprint"),
        cache_manifest_sha256=_sha256(
            value.get("cache_manifest_sha256"),
            label="cache_manifest_sha256",
        ),
        result_artifact_sha256=_sha256(
            value.get("result_artifact_sha256"),
            label="result_artifact_sha256",
        ),
        result_artifact_byte_size=_positive_integer(
            value.get("result_artifact_byte_size"),
            label="result_artifact_byte_size",
        ),
        cell_summaries_sha256=_sha256(
            value.get("cell_summaries_sha256"),
            label="cell_summaries_sha256",
        ),
        detail_shard_manifest_sha256=_sha256(
            value.get("detail_shard_manifest_sha256"),
            label="detail_shard_manifest_sha256",
        ),
        input_lineage_sha256=_sha256(
            value.get("input_lineage_sha256"),
            label="input_lineage_sha256",
        ),
        final_checkpoint_sha256=_sha256(
            value.get("final_checkpoint_sha256"),
            label="final_checkpoint_sha256",
        ),
        final_checkpoint_sequence=_positive_integer(
            value.get("final_checkpoint_sequence"),
            label="final_checkpoint_sequence",
        ),
        source_event_count=_positive_integer(
            value.get("source_event_count"),
            label="source_event_count",
        ),
        detail_record_count=_positive_integer(
            value.get("detail_record_count"),
            label="detail_record_count",
        ),
        summary_row_count=_positive_integer(
            value.get("summary_row_count"),
            label="summary_row_count",
        ),
        # A PASSED artifact's chain digest is validated against this already
        # byte-verified registry subject below.  The compact audit JSON does
        # not duplicate all 485 checkpoint rows.
        checkpoints=subject.checkpoints,
        detail_shard_publication_count=_nonnegative_integer(
            value.get("detail_shard_publication_count"),
            label="detail_shard_publication_count",
        ),
        detail_shard_reused_count=_nonnegative_integer(
            value.get("detail_shard_reused_count"),
            label="detail_shard_reused_count",
        ),
        checkpoint_publication_count=_nonnegative_integer(
            value.get("checkpoint_publication_count"),
            label="checkpoint_publication_count",
        ),
        checkpoint_reused_count=_nonnegative_integer(
            value.get("checkpoint_reused_count"),
            label="checkpoint_reused_count",
        ),
        final_result_disposition=str(value.get("final_result_disposition")),
        checkpoint_load_count=_nonnegative_integer(
            value.get("checkpoint_load_count"),
            label="checkpoint_load_count",
        ),
        start_noop_count=_nonnegative_integer(
            value.get("start_noop_count"),
            label="start_noop_count",
        ),
        complete_noop_count=_nonnegative_integer(
            value.get("complete_noop_count"),
            label="complete_noop_count",
        ),
    )


def load_outcome_equivalence_audit(
    *,
    data_root: Path | str,
    expected_sha256: str,
    subject: OutcomeEquivalenceSubject,
) -> PublishedOutcomeEquivalenceAudit:
    """Load and canonically validate an already-registered audit artifact."""

    digest = _sha256(expected_sha256, label="expected_sha256")
    path, relative_uri, content = _load_audit_bytes(
        data_root=data_root,
        digest=digest,
    )
    try:
        document = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise OutcomeEquivalenceAuditError("registered audit artifact is not JSON") from error
    if not isinstance(document, Mapping) or canonical_json_bytes(document) + b"\n" != content:
        raise OutcomeEquivalenceAuditError("registered audit artifact is not canonical JSON")
    if document.get("subject") != subject.as_dict():
        raise OutcomeEquivalenceAuditError("registered audit subject identity drift")
    raw_comparisons = document.get("comparisons")
    raw_mismatches = document.get("mismatches")
    if (
        not isinstance(raw_comparisons, Mapping)
        or any(
            not isinstance(key, str) or not isinstance(value, bool)
            for key, value in raw_comparisons.items()
        )
        or not isinstance(raw_mismatches, list)
        or any(not isinstance(item, str) for item in raw_mismatches)
        or document.get("passed") is not True
    ):
        raise OutcomeEquivalenceAuditError("registered audit comparison result is invalid")
    observed = _observation_from_document(document.get("observed"), subject=subject)
    if document.get("observed", {}).get("checkpoint_chain_sha256") != (
        observed.checkpoint_chain_sha256
    ):
        raise OutcomeEquivalenceAuditError("registered audit checkpoint-chain identity drift")
    expected_report = compare_outcome_equivalence(subject, observed)
    if expected_report.as_dict() != document:
        raise OutcomeEquivalenceAuditError("registered audit semantic content drift")
    return PublishedOutcomeEquivalenceAudit(
        path=path,
        relative_uri=relative_uri,
        sha256=digest,
        byte_size=len(content),
        disposition="REUSED",
        report=expected_report,
    )


@dataclass(frozen=True, slots=True)
class OutcomeEquivalenceAuditServices:
    """Injectable registry, replay, publication, and persistence boundary."""

    load_subject: Callable[..., object]
    register_audit: Callable[..., object]
    register_spec: Callable[..., object]
    reserve_attempt: Callable[..., object]
    start_attempt: Callable[..., object]
    finish_attempt: Callable[..., object]
    find_subject_audit: Callable[..., object]
    load_attempt_audit: Callable[..., object]
    run_replay: Callable[..., tuple[object, object, int, int, int, int]] = _run_replay
    publish_audit: Callable[..., PublishedOutcomeEquivalenceAudit] = (
        publish_outcome_equivalence_audit
    )
    load_audit: Callable[..., PublishedOutcomeEquivalenceAudit] = load_outcome_equivalence_audit


@dataclass(frozen=True, slots=True)
class OutcomeEquivalenceAuditExecution:
    """Complete audit result including the registry registration response."""

    report: OutcomeEquivalenceAuditReport
    artifact: PublishedOutcomeEquivalenceAudit
    registration: object


@dataclass(frozen=True, slots=True)
class GovernedOutcomeEquivalenceAuditReport:
    """Operator-facing identity of an executed or safely reused governed audit."""

    disposition: Literal["SUCCEEDED", "SKIPPED_DUPLICATE"]
    outcome_replay_manifest_id: int
    predecessor_run_fingerprint: str
    validation_run_fingerprint: str
    validation_research_run_spec_id: int
    validation_research_run_attempt_id: int
    reused_validation_attempt_id: int | None
    outcome_equivalence_audit_id: int
    audit_artifact_path: Path
    audit_artifact_sha256: str
    checkpoint_chain_sha256: str
    checkpoint_count: int
    source_event_count: int
    detail_record_count: int
    summary_row_count: int
    passed: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "audit_artifact_path": str(self.audit_artifact_path),
            "audit_artifact_sha256": self.audit_artifact_sha256,
            "audit_version": AUDIT_VERSION,
            "checkpoint_chain_sha256": self.checkpoint_chain_sha256,
            "checkpoint_count": self.checkpoint_count,
            "detail_record_count": self.detail_record_count,
            "disposition": self.disposition,
            "outcome_equivalence_audit_id": self.outcome_equivalence_audit_id,
            "outcome_replay_manifest_id": self.outcome_replay_manifest_id,
            "passed": self.passed,
            "predecessor_run_fingerprint": self.predecessor_run_fingerprint,
            "query_id": P5_QUERY_ID,
            "reused_validation_attempt_id": self.reused_validation_attempt_id,
            "source_event_count": self.source_event_count,
            "summary_row_count": self.summary_row_count,
            "validation_research_run_attempt_id": (self.validation_research_run_attempt_id),
            "validation_research_run_spec_id": self.validation_research_run_spec_id,
            "validation_run_fingerprint": self.validation_run_fingerprint,
        }


@dataclass(frozen=True, slots=True)
class _AuditReservation:
    outcome_replay_manifest_id: int


@dataclass(frozen=True, slots=True)
class _ReplayIdentity:
    """Only the historical fingerprint consumed by ``_run_replay``."""

    fingerprint: str


@dataclass(frozen=True, slots=True)
class _AuditCheckpointRegistration:
    checkpoint_artifact_sha256: str


@dataclass(frozen=True, slots=True)
class _CodeProvenanceIdentity:
    code_commit: str
    code_snapshot_sha256: str
    dependency_lock_sha256: str


@dataclass(frozen=True, slots=True)
class _PostgresRuntimeIdentity:
    """Immutable canonical bytes for the audit's PostgreSQL runtime state."""

    canonical_bytes: bytes

    def as_dict(self) -> dict[str, object]:
        value = json.loads(self.canonical_bytes)
        if not isinstance(value, dict):  # pragma: no cover - capture proves this
            raise OutcomeEquivalenceAuditError(
                "captured PostgreSQL runtime identity is not an object"
            )
        return value


def _capture_code_provenance(
    *,
    project: Path,
    data: Path,
    pipeline_services: OutcomePipelineServices,
) -> _CodeProvenanceIdentity:
    code_commit = pipeline_services.git_head(project)
    snapshot = pipeline_services.build_snapshot(project, code_commit=code_commit)
    published = pipeline_services.publish_snapshot(snapshot, data_root=data)
    if getattr(published, "sha256", None) != snapshot.sha256:
        raise OutcomeEquivalenceAuditError("published audit code snapshot identity drift")
    dependency_sha256 = pipeline_services.dependency_hash(project)
    return _CodeProvenanceIdentity(
        code_commit=code_commit,
        code_snapshot_sha256=snapshot.sha256,
        dependency_lock_sha256=dependency_sha256,
    )


def _verify_code_provenance(
    *,
    project: Path,
    identity: _CodeProvenanceIdentity,
    pipeline_services: OutcomePipelineServices,
) -> None:
    if (
        pipeline_services.git_head(project) != identity.code_commit
        or pipeline_services.build_snapshot(
            project,
            code_commit=identity.code_commit,
        ).sha256
        != identity.code_snapshot_sha256
        or pipeline_services.dependency_hash(project) != identity.dependency_lock_sha256
    ):
        raise OutcomeEquivalenceAuditError(
            "code snapshot, dependency lock, or Git identity changed during the audit"
        )


def _capture_postgres_runtime_identity(
    *,
    database_url: str,
    migrations_directory: Path,
    pipeline_services: OutcomePipelineServices,
) -> _PostgresRuntimeIdentity:
    """Deep-freeze the exact server and schema-migration identity."""

    try:
        observed = pipeline_services.postgres_runtime(
            database_url,
            migrations_directory=migrations_directory,
        )
        if not isinstance(observed, Mapping):
            raise TypeError("PostgreSQL runtime identity must be a mapping")
        canonical = canonical_json_bytes(observed)
        decoded = json.loads(canonical)
        if not isinstance(decoded, dict):
            raise TypeError("PostgreSQL runtime identity must remain an object")
    except Exception as error:
        raise OutcomeEquivalenceAuditError(
            "cannot verify PostgreSQL runtime/schema_migrations identity"
        ) from error
    return _PostgresRuntimeIdentity(canonical_bytes=canonical)


def _verify_audit_provenance(
    *,
    project: Path,
    database_url: str,
    code_identity: _CodeProvenanceIdentity,
    postgres_identity: _PostgresRuntimeIdentity,
    pipeline_services: OutcomePipelineServices,
) -> None:
    """Fail closed if executable or PostgreSQL provenance changed mid-audit."""

    _verify_code_provenance(
        project=project,
        identity=code_identity,
        pipeline_services=pipeline_services,
    )
    observed = _capture_postgres_runtime_identity(
        database_url=database_url,
        migrations_directory=project / "migrations",
        pipeline_services=pipeline_services,
    )
    if observed.canonical_bytes != postgres_identity.canonical_bytes:
        raise OutcomeEquivalenceAuditError(
            "PostgreSQL runtime/schema_migrations identity changed during the audit"
        )


def _audit_parameters(subject: OutcomeEquivalenceSubject) -> dict[str, object]:
    return {
        "audit_kind": AUDIT_KIND,
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


def _make_audit_run_spec(
    *,
    prepared: PreparedOutcomeInputs,
    subject: OutcomeEquivalenceSubject,
    provenance: _CodeProvenanceIdentity,
    runtime: Mapping[str, object],
    feature_sha256: str,
) -> RunSpec:
    """Build the separate current-provenance VALIDATION execution identity."""

    screening = prepared.config.screening_bundle
    return RunSpec(
        campaign_id=CAMPAIGN_ID,
        experiment_id=None,
        run_kind="VALIDATION",
        engine_version=AUDIT_ENGINE_VERSION,
        source_manifest_hashes={
            "phase1a_p5_cache_manifest_v1": subject.cache_manifest_sha256,
            "phase1a_p5_checkpoint_chain_v1": subject.checkpoint_chain_sha256,
            "phase1a_p5_discovery_artifacts_registry_v1": (subject.source_artifact_manifest_sha256),
            "phase1a_p5_outcome_result_v1": subject.result_artifact_sha256,
        },
        eligible_calendar_version=CALENDAR_VERSION,
        eligible_calendar_sha256=prepared.calendar.sha256,
        split_version=SPLIT_VERSION,
        split_sha256=prepared.split.sha256,
        feature_version=FEATURE_VERSION,
        feature_sha256=feature_sha256,
        outcome_version=screening.outcome_version,
        outcome_sha256=screening.barrier_grid.sha256,
        cost_version=screening.cost_version,
        cost_sha256=screening.cost.sha256,
        execution_version=screening.execution_version,
        execution_sha256=screening.execution.sha256,
        code_commit=provenance.code_commit,
        code_snapshot_sha256=provenance.code_snapshot_sha256,
        dependency_lock_sha256=provenance.dependency_lock_sha256,
        runtime_environment=runtime,
        random_seed=AUDIT_RANDOM_SEED,
        direction="BOTH",
        signal_policy={
            "query_id": P5_QUERY_ID,
            "subject": "BYTE_VERIFIED_SUCCEEDED_REPLAY",
        },
        entry_policy={
            "checkpoint_load": "FORCED_NONE",
            "replay_start": "FIRST_PLANNED_SOURCE_DATE",
        },
        barrier_policy={
            "comparison": "BYTE_IDENTICAL_DAILY_CHAIN_AND_FINAL_RESULT",
            "publication": "MUST_REUSE_IDENTICAL_CONTENT",
        },
        terminal_policy={
            "control_plane_mutations": "NO_OP",
            "validation_attempt_completion": "ATOMIC_REGISTRY_AUDIT",
        },
        parameters=_audit_parameters(subject),
    )


class _UninterruptedReplayCapture:
    """Replace replay-ledger mutations and capture every artifact publication."""

    def __init__(self, subject: OutcomeEquivalenceSubject) -> None:
        self.subject = subject
        self.checkpoint_load_count = 0
        self.start_noop_count = 0
        self.complete_noop_count = 0
        self.detail_shards: list[object] = []
        self.checkpoints: list[OutcomeEquivalenceCheckpoint] = []
        self.checkpoint_dispositions: list[str] = []
        self.final_results: list[object] = []

    def _validate_control_identity(self, kwargs: Mapping[str, object]) -> None:
        if (
            kwargs.get("outcome_replay_manifest_id") != self.subject.outcome_replay_manifest_id
            or kwargs.get("run_fingerprint") != self.subject.run_fingerprint
        ):
            raise OutcomeEquivalenceAuditError("audit replay control identity drift")

    def start_replay(self, database_url: str, **kwargs: object) -> object:
        del database_url
        self._validate_control_identity(kwargs)
        self.start_noop_count += 1
        return self.subject

    def load_checkpoint(self, database_url: str, **kwargs: object) -> None:
        del database_url
        self._validate_control_identity(kwargs)
        self.checkpoint_load_count += 1

    def publish_result_shard(
        self,
        publisher: Callable[..., object],
        *args: object,
        **kwargs: object,
    ) -> object:
        published = publisher(*args, **kwargs)
        self.detail_shards.append(published)
        return published

    def publish_checkpoint(
        self,
        publisher: Callable[..., object],
        *args: object,
        **kwargs: object,
    ) -> object:
        published = publisher(*args, **kwargs)
        progress = _member(published, "progress_metadata", default={})
        if not isinstance(progress, Mapping):
            raise OutcomeEquivalenceAuditError("published checkpoint progress is invalid")
        checkpoint = OutcomeEquivalenceCheckpoint(
            checkpoint_sequence=_positive_integer(
                _member(published, "checkpoint_sequence"),
                label="checkpoint_sequence",
            ),
            checkpoint_artifact_sha256=_sha256(
                _member(published, "sha256", "checkpoint_artifact_sha256"),
                label="checkpoint_artifact_sha256",
            ),
            checkpoint_artifact_byte_size=_positive_integer(
                _member(published, "byte_size", "checkpoint_artifact_byte_size"),
                label="checkpoint_artifact_byte_size",
            ),
            predecessor_checkpoint_sha256=(
                None
                if kwargs.get("predecessor_checkpoint_sha256") is None
                else _sha256(
                    kwargs.get("predecessor_checkpoint_sha256"),
                    label="predecessor_checkpoint_sha256",
                )
            ),
            last_completed_source_date=_source_date(
                _member(published, "last_completed_source_date"),
                label="last_completed_source_date",
            ),
            source_event_count=_positive_integer(
                kwargs.get("source_event_count"),
                label="source_event_count",
            ),
            progress_metadata_sha256=canonical_sha256(progress),
        )
        self.checkpoints.append(checkpoint)
        self.checkpoint_dispositions.append(str(_member(published, "disposition")))
        return published

    def register_checkpoint(self, database_url: str, **kwargs: object) -> object:
        del database_url
        self._validate_control_identity(kwargs)
        if not self.checkpoints:
            raise OutcomeEquivalenceAuditError("checkpoint registered before publication")
        published = self.checkpoints[-1]
        path = kwargs.get("checkpoint_artifact_path")
        if (
            kwargs.get("checkpoint_sequence") != published.checkpoint_sequence
            or not isinstance(path, Path)
            or path.name != f"sha256={published.checkpoint_artifact_sha256}.json"
        ):
            raise OutcomeEquivalenceAuditError("checkpoint no-op registration identity drift")
        return _AuditCheckpointRegistration(
            checkpoint_artifact_sha256=published.checkpoint_artifact_sha256
        )

    def publish_result(
        self,
        publisher: Callable[..., object],
        *args: object,
        **kwargs: object,
    ) -> object:
        published = publisher(*args, **kwargs)
        self.final_results.append(published)
        return published

    def complete_replay(self, database_url: str, **kwargs: object) -> object:
        del database_url
        self._validate_control_identity(kwargs)
        self.complete_noop_count += 1
        return self.subject


def _artifact_attribute(value: object, name: str, *, label: str) -> object:
    observed = _member(value, name)
    if observed is None:
        artifact = _member(value, "artifact")
        observed = _member(artifact, name)
    if observed is None:
        raise OutcomeEquivalenceAuditError(f"{label} has no {name}")
    return observed


def _observation(
    *,
    subject: OutcomeEquivalenceSubject,
    capture: _UninterruptedReplayCapture,
    result: object,
    final_checkpoint: object,
    completed_source_date_count: int,
    source_event_count: int,
    detail_record_count: int,
    summary_row_count: int,
) -> OutcomeEquivalenceObservation:
    if len(capture.final_results) != 1:
        raise OutcomeEquivalenceAuditError("audit replay must publish exactly one final result")
    document = _member(result, "document")
    if not isinstance(document, Mapping):
        raise OutcomeEquivalenceAuditError("verified audit result has no canonical document")
    if completed_source_date_count != len(capture.checkpoints):
        raise OutcomeEquivalenceAuditError("audit replay completion count differs from checkpoints")
    checkpoint_sha256 = _sha256(
        _artifact_attribute(final_checkpoint, "sha256", label="final checkpoint"),
        label="final_checkpoint_sha256",
    )
    return OutcomeEquivalenceObservation(
        outcome_replay_manifest_id=subject.outcome_replay_manifest_id,
        run_fingerprint=subject.run_fingerprint,
        cache_manifest_sha256=_sha256(
            _member(document.get("cache_manifest"), "artifact_sha256", "sha256"),
            label="cache_manifest_sha256",
        ),
        result_artifact_sha256=_sha256(
            _artifact_attribute(result, "sha256", label="verified result"),
            label="result_artifact_sha256",
        ),
        result_artifact_byte_size=_positive_integer(
            _artifact_attribute(result, "byte_size", label="verified result"),
            label="result_artifact_byte_size",
        ),
        cell_summaries_sha256=_sha256(
            document.get("cell_summaries_sha256"),
            label="cell_summaries_sha256",
        ),
        detail_shard_manifest_sha256=_sha256(
            document.get("detail_shard_manifest_sha256"),
            label="detail_shard_manifest_sha256",
        ),
        input_lineage_sha256=_sha256(
            document.get("input_lineage_sha256"),
            label="input_lineage_sha256",
        ),
        final_checkpoint_sha256=checkpoint_sha256,
        final_checkpoint_sequence=_positive_integer(
            _artifact_attribute(
                final_checkpoint,
                "checkpoint_sequence",
                label="final checkpoint",
            ),
            label="final_checkpoint_sequence",
        ),
        source_event_count=_positive_integer(
            source_event_count,
            label="source_event_count",
        ),
        detail_record_count=_positive_integer(
            detail_record_count,
            label="detail_record_count",
        ),
        summary_row_count=_positive_integer(
            summary_row_count,
            label="summary_row_count",
        ),
        checkpoints=tuple(capture.checkpoints),
        detail_shard_publication_count=len(capture.detail_shards),
        detail_shard_reused_count=sum(
            str(_member(item, "disposition")) == "REUSED" for item in capture.detail_shards
        ),
        checkpoint_publication_count=len(capture.checkpoints),
        checkpoint_reused_count=sum(
            disposition == "REUSED" for disposition in capture.checkpoint_dispositions
        ),
        final_result_disposition=str(
            _artifact_attribute(capture.final_results[0], "disposition", label="final result")
        ),
        checkpoint_load_count=capture.checkpoint_load_count,
        start_noop_count=capture.start_noop_count,
        complete_noop_count=capture.complete_noop_count,
    )


def execute_uninterrupted_p5_replay(
    *,
    subject: OutcomeEquivalenceSubject,
    prepared: PreparedOutcomeInputs,
    reports: tuple[Any, ...],
    terminal_resolution: Any,
    cache_manifest: object,
    run_spec: Any,
    database_url: str,
    data_root: Path,
    pipeline_services: OutcomePipelineServices,
    run_replay: Callable[..., tuple[object, object, int, int, int, int]] = _run_replay,
    progress_callback: Callable[[OutcomeProgress], None] | None = None,
) -> OutcomeEquivalenceObservation:
    """Run p5 from the beginning while leaving its successful DB rows untouched."""

    if not isinstance(subject, OutcomeEquivalenceSubject):
        raise OutcomeEquivalenceAuditError("subject must be an OutcomeEquivalenceSubject")
    if getattr(run_spec, "fingerprint", None) != subject.run_fingerprint:
        raise OutcomeEquivalenceAuditError("audit RunSpec differs from the successful subject")
    prepared_query_id = getattr(getattr(prepared, "config", None), "query_id", None)
    if prepared_query_id != subject.query_id:
        raise OutcomeEquivalenceAuditError("audit prepared inputs differ from the p5 subject")
    capture = _UninterruptedReplayCapture(subject)
    original_artifacts = pipeline_services.artifacts
    audit_artifacts = replace(
        original_artifacts,
        publish_result_shard=lambda *args, **kwargs: capture.publish_result_shard(
            original_artifacts.publish_result_shard,
            *args,
            **kwargs,
        ),
        publish_checkpoint=lambda *args, **kwargs: capture.publish_checkpoint(
            original_artifacts.publish_checkpoint,
            *args,
            **kwargs,
        ),
        publish_result=lambda *args, **kwargs: capture.publish_result(
            original_artifacts.publish_result,
            *args,
            **kwargs,
        ),
    )
    audit_pipeline_services = replace(
        pipeline_services,
        start_replay=capture.start_replay,
        load_checkpoint=capture.load_checkpoint,
        register_checkpoint=capture.register_checkpoint,
        complete_replay=capture.complete_replay,
        artifacts=audit_artifacts,
    )
    replay_result = run_replay(
        prepared=prepared,
        reports=reports,
        terminal_resolution=terminal_resolution,
        cache_manifest=cache_manifest,
        run_spec=run_spec,
        reservation=_AuditReservation(subject.outcome_replay_manifest_id),
        database_url=database_url,
        data=data_root,
        services=audit_pipeline_services,
        progress_callback=progress_callback,
    )
    if not isinstance(replay_result, tuple) or len(replay_result) != 6:
        raise OutcomeEquivalenceAuditError("audit replay returned an invalid completion report")
    result, final_checkpoint, completed, events, records, summaries = replay_result
    return _observation(
        subject=subject,
        capture=capture,
        result=result,
        final_checkpoint=final_checkpoint,
        completed_source_date_count=_positive_integer(
            completed,
            label="completed_source_date_count",
        ),
        source_event_count=_positive_integer(events, label="source_event_count"),
        detail_record_count=_positive_integer(records, label="detail_record_count"),
        summary_row_count=_positive_integer(summaries, label="summary_row_count"),
    )


def default_outcome_equivalence_audit_services() -> OutcomeEquivalenceAuditServices:
    """Resolve the registry API lazily so its schema can evolve independently."""

    try:
        from systematic_fx.db import outcome_registry
        from systematic_fx.db.run_registry import (
            finish_run_attempt,
            register_run_spec,
            reserve_run_attempt,
            start_run_attempt,
        )

        loader = outcome_registry.load_phase1a_p5_audit_subject
        registrar = outcome_registry.register_phase1a_p5_equivalence_audit
    except (AttributeError, ImportError) as error:  # pragma: no cover - integration guard
        raise OutcomeEquivalenceAuditError("p5 equivalence registry API is unavailable") from error
    return OutcomeEquivalenceAuditServices(
        load_subject=loader,
        register_audit=registrar,
        register_spec=register_run_spec,
        reserve_attempt=reserve_run_attempt,
        start_attempt=start_run_attempt,
        finish_attempt=finish_run_attempt,
        find_subject_audit=(outcome_registry.find_phase1a_p5_equivalence_audit_for_subject),
        load_attempt_audit=(outcome_registry.load_phase1a_p5_equivalence_audit_for_attempt),
    )


def _execute_and_register_p5_audit(
    *,
    database_url: str,
    data_root: Path,
    validation_research_run_attempt_id: int,
    registry_subject: object,
    subject: OutcomeEquivalenceSubject,
    prepared: PreparedOutcomeInputs,
    reports: tuple[Any, ...],
    terminal_resolution: Any,
    cache_manifest: object,
    pipeline_services: OutcomePipelineServices,
    services: OutcomeEquivalenceAuditServices,
    pre_registration_check: Callable[[], None],
    progress_callback: Callable[[OutcomeProgress], None] | None = None,
) -> OutcomeEquivalenceAuditExecution:
    """Rerun, compare, provenance-check, publish, and register one proof."""

    observed = execute_uninterrupted_p5_replay(
        subject=subject,
        prepared=prepared,
        reports=reports,
        terminal_resolution=terminal_resolution,
        cache_manifest=cache_manifest,
        run_spec=_ReplayIdentity(subject.run_fingerprint),
        database_url=database_url,
        data_root=data_root,
        pipeline_services=pipeline_services,
        run_replay=services.run_replay,
        progress_callback=progress_callback,
    )
    report = compare_outcome_equivalence(subject, observed)
    if not report.passed:
        artifact = services.publish_audit(report, data_root=data_root)
        raise OutcomeEquivalenceAuditError(
            "uninterrupted p5 replay differs from the resumed subject; "
            f"audit evidence: {artifact.path}"
        )
    # Re-hash the exact executable/config snapshot and dependency lock after
    # the hours-long pass.  A byte change prevents a PASSED registry write.
    pre_registration_check()
    artifact = services.publish_audit(report, data_root=data_root)
    registration = services.register_audit(
        database_url,
        validation_research_run_attempt_id=_positive_integer(
            validation_research_run_attempt_id,
            label="validation_research_run_attempt_id",
        ),
        subject=registry_subject,
        audit_artifact_path=artifact.path,
        data_root=data_root,
    )
    return OutcomeEquivalenceAuditExecution(
        report=report,
        artifact=artifact,
        registration=registration,
    )


def _validate_duplicate_gate(
    gate: object,
    *,
    subject: OutcomeEquivalenceSubject,
) -> tuple[int, str]:
    expected = {
        "predecessor_outcome_replay_manifest_id": subject.outcome_replay_manifest_id,
        "predecessor_run_fingerprint": subject.run_fingerprint,
        "predecessor_result_artifact_sha256": subject.result_artifact_sha256,
        "predecessor_input_lineage_sha256": subject.input_lineage_sha256,
        "predecessor_cell_summaries_sha256": subject.cell_summaries_sha256,
        "predecessor_detail_shard_manifest_sha256": (subject.detail_shard_manifest_sha256),
        "predecessor_final_checkpoint_sha256": subject.final_checkpoint_sha256,
    }
    drift = [name for name, value in expected.items() if _member(gate, name) != value]
    if drift:
        raise OutcomeEquivalenceAuditError(
            "existing equivalence audit predecessor drift: " + ", ".join(sorted(drift))
        )
    return (
        _positive_integer(
            _member(gate, "equivalence_audit_id"),
            label="equivalence_audit_id",
        ),
        _sha256(
            _member(gate, "equivalence_audit_artifact_sha256"),
            label="equivalence_audit_artifact_sha256",
        ),
    )


def _load_verified_duplicate_artifact(
    *,
    loaded: object,
    subject: OutcomeEquivalenceSubject,
    data: Path,
    services: OutcomeEquivalenceAuditServices,
    expected_validation_attempt_id: int | None = None,
    expected_validation_run_fingerprint: str | None = None,
) -> tuple[object, PublishedOutcomeEquivalenceAudit]:
    gate = _member(loaded, "predecessor_gate")
    stored = _member(loaded, "audit")
    stored_path = _member(loaded, "audit_artifact_path")
    if gate is None or stored is None or not isinstance(stored_path, Path):
        raise OutcomeEquivalenceAuditError("loaded equivalence audit is incomplete")
    audit_id, audit_sha256 = _validate_duplicate_gate(gate, subject=subject)
    expected = {
        "outcome_equivalence_audit_id": audit_id,
        "predecessor_outcome_replay_manifest_id": (subject.outcome_replay_manifest_id),
        "audit_artifact_sha256": audit_sha256,
        "checkpoint_chain_sha256": subject.checkpoint_chain_sha256,
        "passed": True,
    }
    if expected_validation_attempt_id is not None:
        expected["validation_research_run_attempt_id"] = expected_validation_attempt_id
    if expected_validation_run_fingerprint is not None:
        expected["validation_run_fingerprint"] = expected_validation_run_fingerprint
    drift = [name for name, value in expected.items() if _member(stored, name) != value]
    if drift:
        raise OutcomeEquivalenceAuditError(
            "loaded equivalence audit identity drift: " + ", ".join(sorted(drift))
        )
    artifact = services.load_audit(
        data_root=data,
        expected_sha256=audit_sha256,
        subject=subject,
    )
    # Both loaders independently enforce the exact lexical audit namespace.
    # Do not call ``resolve`` here: following a path again would reintroduce a
    # symlink race after the fd-native byte verification has completed.
    if artifact.path != stored_path:
        raise OutcomeEquivalenceAuditError("loaded equivalence audit artifact path drift")
    return stored, artifact


def _governed_report(
    *,
    disposition: Literal["SUCCEEDED", "SKIPPED_DUPLICATE"],
    subject: OutcomeEquivalenceSubject,
    validation_run_fingerprint: str,
    validation_research_run_spec_id: int,
    validation_research_run_attempt_id: int,
    reused_validation_attempt_id: int | None,
    outcome_equivalence_audit_id: int,
    artifact: PublishedOutcomeEquivalenceAudit,
) -> GovernedOutcomeEquivalenceAuditReport:
    if not artifact.report.passed:
        raise OutcomeEquivalenceAuditError("a governed audit report must be PASSED")
    return GovernedOutcomeEquivalenceAuditReport(
        disposition=disposition,
        outcome_replay_manifest_id=subject.outcome_replay_manifest_id,
        predecessor_run_fingerprint=subject.run_fingerprint,
        validation_run_fingerprint=_sha256(
            validation_run_fingerprint,
            label="validation_run_fingerprint",
        ),
        validation_research_run_spec_id=_positive_integer(
            validation_research_run_spec_id,
            label="validation_research_run_spec_id",
        ),
        validation_research_run_attempt_id=_positive_integer(
            validation_research_run_attempt_id,
            label="validation_research_run_attempt_id",
        ),
        reused_validation_attempt_id=(
            None
            if reused_validation_attempt_id is None
            else _positive_integer(
                reused_validation_attempt_id,
                label="reused_validation_attempt_id",
            )
        ),
        outcome_equivalence_audit_id=_positive_integer(
            outcome_equivalence_audit_id,
            label="outcome_equivalence_audit_id",
        ),
        audit_artifact_path=artifact.path,
        audit_artifact_sha256=artifact.sha256,
        checkpoint_chain_sha256=subject.checkpoint_chain_sha256,
        checkpoint_count=len(subject.checkpoints),
        source_event_count=subject.source_event_count,
        detail_record_count=subject.detail_record_count,
        summary_row_count=subject.summary_row_count,
        passed=True,
    )


def _failure_message(error: Exception) -> str:
    return f"{type(error).__name__}: {error}"[:4000]


def run_phase1a_p5_outcome_equivalence_audit(
    *,
    project_root: Path | str,
    data_root: Path | str,
    database_url: str,
    outcome_replay_manifest_id: int | None = None,
    pipeline_services: OutcomePipelineServices | None = None,
    services: OutcomeEquivalenceAuditServices | None = None,
    progress_callback: Callable[[OutcomeProgress], None] | None = None,
) -> GovernedOutcomeEquivalenceAuditReport:
    """Execute or safely reuse the campaign-level governed p5 audit."""

    if not isinstance(database_url, str) or not database_url.strip():
        raise OutcomeEquivalenceAuditError("database_url must be a non-empty string")
    if outcome_replay_manifest_id is not None:
        _positive_integer(
            outcome_replay_manifest_id,
            label="outcome_replay_manifest_id",
        )
    if progress_callback is not None and not callable(progress_callback):
        raise OutcomeEquivalenceAuditError("progress_callback must be callable")
    try:
        project = _strict_root(project_root, label="project_root")
        data = _strict_root(data_root, label="data_root", expected_name="data")
        raw, manifests = _pipeline_data_layout(data, create_derived=False)
        pipeline = pipeline_services or _default_pipeline_services()
        audit = services or default_outcome_equivalence_audit_services()

        # The DB loader verifies the successful p5 result, all 485 checkpoint
        # artifacts, and their immutable files before any validation run exists.
        registry_subject = audit.load_subject(
            database_url,
            data_root=data,
            outcome_replay_manifest_id=outcome_replay_manifest_id,
        )
        subject = outcome_equivalence_subject_from_registry(registry_subject)
        existing = audit.find_subject_audit(
            database_url,
            predecessor_outcome_replay_manifest_id=(subject.outcome_replay_manifest_id),
            data_root=data,
        )
        if existing is not None:
            stored, artifact = _load_verified_duplicate_artifact(
                loaded=existing,
                subject=subject,
                data=data,
                services=audit,
            )
            original_attempt_id = _positive_integer(
                _member(stored, "validation_research_run_attempt_id"),
                label="validation_research_run_attempt_id",
            )
            return _governed_report(
                disposition="SKIPPED_DUPLICATE",
                subject=subject,
                validation_run_fingerprint=_sha256(
                    _member(stored, "validation_run_fingerprint"),
                    label="validation_run_fingerprint",
                ),
                validation_research_run_spec_id=_positive_integer(
                    _member(stored, "validation_research_run_spec_id"),
                    label="validation_research_run_spec_id",
                ),
                validation_research_run_attempt_id=original_attempt_id,
                reused_validation_attempt_id=original_attempt_id,
                outcome_equivalence_audit_id=_positive_integer(
                    _member(stored, "outcome_equivalence_audit_id"),
                    label="outcome_equivalence_audit_id",
                ),
                artifact=artifact,
            )
        prepared = _prepare_inputs(
            project=project,
            data=data,
            raw=raw,
            manifests=manifests,
            database_url=database_url,
            services=pipeline,
            publish_control_plane=False,
            config_path=OUTCOME_CONFIG_RELATIVE_PATH,
        )
        if (
            prepared.config.query_id != P5_QUERY_ID
            or prepared.config.outcome_config_id != subject.outcome_config_id
            or prepared.source_artifacts.source_artifact_manifest_sha256
            != subject.source_artifact_manifest_sha256
        ):
            raise OutcomeEquivalenceAuditError(
                "prepared frozen p5 inputs differ from the successful audit subject"
            )
        cache_manifest = pipeline.artifacts.find_cache_manifest(
            data_root=data,
            cache_plan_sha256=prepared.plan.cache_plan_sha256,
            input_manifest_sha256=prepared.discovery.input_manifest_sha256,
            verify_cache_content=False,
        )
        if cache_manifest is None:
            raise OutcomeEquivalenceAuditError(
                "the immutable p5 cache manifest is missing; the audit will not rebuild it"
            )
        reports = _validate_cache_reports(prepared.plan, cache_manifest.reports)
        cache_sha256 = _report_sha(cache_manifest, label="cache manifest")
        if cache_sha256 != subject.cache_manifest_sha256:
            raise OutcomeEquivalenceAuditError("p5 cache manifest differs from the subject")
        terminal_resolution = _resolve_terminals(prepared.plan, reports)
        if progress_callback is not None:
            progress_callback(
                OutcomeProgress(
                    stage="CACHE",
                    completed=len(reports),
                    total=len(reports),
                    cache_reused_count=len(reports),
                )
            )

        provenance = _capture_code_provenance(
            project=project,
            data=data,
            pipeline_services=pipeline,
        )
        runtime = dict(pipeline.runtime())
        postgres_identity = _capture_postgres_runtime_identity(
            database_url=database_url,
            migrations_directory=project / "migrations",
            pipeline_services=pipeline,
        )
        runtime["postgresql"] = postgres_identity.as_dict()
        runtime["phase1a_outcome_equivalence_audit"] = {
            "audit_version": AUDIT_VERSION,
            "checkpoint_load": "FORCED_NONE",
            "engine_version": AUDIT_ENGINE_VERSION,
        }
        feature_sha256 = load_phase1a_screening_config(
            project / "configs/features/phase1a_mbp10_screening_v1.toml"
        ).sha256
        validation_spec = _make_audit_run_spec(
            prepared=prepared,
            subject=subject,
            provenance=provenance,
            runtime=runtime,
            feature_sha256=feature_sha256,
        )
        # This is deliberately immediately before immutable RunSpec
        # registration: the fingerprint binds the exact dirty source snapshot.
        _verify_audit_provenance(
            project=project,
            database_url=database_url,
            code_identity=provenance,
            postgres_identity=postgres_identity,
            pipeline_services=pipeline,
        )
        registration = audit.register_spec(database_url, validation_spec)
        reservation = audit.reserve_attempt(
            database_url,
            run_fingerprint=validation_spec.fingerprint,
        )
        if not bool(_member(reservation, "execute")):
            reused_attempt_id = _positive_integer(
                _member(reservation, "reused_attempt_id"),
                label="reused_validation_attempt_id",
            )
            loaded = audit.load_attempt_audit(
                database_url,
                validation_research_run_attempt_id=reused_attempt_id,
                data_root=data,
            )
            stored, artifact = _load_verified_duplicate_artifact(
                loaded=loaded,
                subject=subject,
                data=data,
                services=audit,
                expected_validation_attempt_id=reused_attempt_id,
                expected_validation_run_fingerprint=validation_spec.fingerprint,
            )
            return _governed_report(
                disposition="SKIPPED_DUPLICATE",
                subject=subject,
                validation_run_fingerprint=validation_spec.fingerprint,
                validation_research_run_spec_id=_positive_integer(
                    _member(registration, "research_run_spec_id"),
                    label="validation_research_run_spec_id",
                ),
                validation_research_run_attempt_id=_positive_integer(
                    _member(reservation, "research_run_attempt_id"),
                    label="validation_research_run_attempt_id",
                ),
                reused_validation_attempt_id=reused_attempt_id,
                outcome_equivalence_audit_id=_positive_integer(
                    _member(stored, "outcome_equivalence_audit_id"),
                    label="outcome_equivalence_audit_id",
                ),
                artifact=artifact,
            )

        attempt_id = _positive_integer(
            _member(reservation, "research_run_attempt_id"),
            label="validation_research_run_attempt_id",
        )
        audit.start_attempt(
            database_url,
            research_run_attempt_id=attempt_id,
        )
        try:
            execution = _execute_and_register_p5_audit(
                database_url=database_url,
                data_root=data,
                validation_research_run_attempt_id=attempt_id,
                registry_subject=registry_subject,
                subject=subject,
                prepared=prepared,
                reports=reports,
                terminal_resolution=terminal_resolution,
                cache_manifest=cache_manifest,
                pipeline_services=pipeline,
                services=audit,
                pre_registration_check=lambda: _verify_audit_provenance(
                    project=project,
                    database_url=database_url,
                    code_identity=provenance,
                    postgres_identity=postgres_identity,
                    pipeline_services=pipeline,
                ),
                progress_callback=progress_callback,
            )
        except Exception as error:
            try:
                audit.finish_attempt(
                    database_url,
                    research_run_attempt_id=attempt_id,
                    status="FAILED",
                    error_message=_failure_message(error),
                )
            except Exception as failure_error:
                raise OutcomeEquivalenceAuditError(
                    "p5 equivalence audit failed and its validation attempt "
                    "could not be terminalized"
                ) from failure_error
            if isinstance(error, OutcomeEquivalenceAuditError):
                raise
            raise OutcomeEquivalenceAuditError(_failure_message(error)) from error

        audit_id = _positive_integer(
            _member(execution.registration, "outcome_equivalence_audit_id"),
            label="outcome_equivalence_audit_id",
        )
        return _governed_report(
            disposition="SUCCEEDED",
            subject=subject,
            validation_run_fingerprint=validation_spec.fingerprint,
            validation_research_run_spec_id=_positive_integer(
                _member(registration, "research_run_spec_id"),
                label="validation_research_run_spec_id",
            ),
            validation_research_run_attempt_id=attempt_id,
            reused_validation_attempt_id=None,
            outcome_equivalence_audit_id=audit_id,
            artifact=execution.artifact,
        )
    except OutcomeEquivalenceAuditError:
        raise
    except Exception as error:
        raise OutcomeEquivalenceAuditError(_failure_message(error)) from error
