"""Reproducible orchestration for one governed five-date Phase 1A Discovery slice.

The pipeline deliberately exposes only ``split.discovery``.  It rebuilds and
registers the frozen control plane, records every execution variable in immutable
campaign-level RunSpecs, and treats an already successful fingerprint as a
resume point instead of executing it again.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import tempfile
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Final
from urllib.parse import unquote, urlparse

import psycopg
from psycopg.rows import dict_row

from systematic_fx.data.contract_selection import (
    ContractSelectionResult,
    select_next_eligible_contract,
)
from systematic_fx.data.quality import load_structural_qc_config
from systematic_fx.db.data_registry import SourceFileRegistration, load_source_manifest_bundle
from systematic_fx.db.migrations import MigrationError, discover_migrations
from systematic_fx.db.pattern_registry import (
    PatternSliceObservation,
    derive_phase1a_pattern_observation,
    record_pattern_slice_observation,
)
from systematic_fx.db.research_registry import (
    Phase1ACurrentSlicePrefixReport,
    Phase1APartialRecoverySource,
    complete_discovery_run_success,
    complete_phase1a_recovery_run_success,
    load_phase1a_partial_recovery_source,
    verify_discovery_run_success,
    verify_phase1a_current_slice_prefix,
    verify_phase1a_predecessor_slice,
)
from systematic_fx.db.run_registry import (
    RunAttemptReservation,
    finish_run_attempt,
    register_run_spec,
    reserve_run_attempt,
    start_run_attempt,
)
from systematic_fx.db.screening_feature_registry import (
    FEATURE_BATCH_MANIFEST_SCHEMA,
    BatchEntryStatus,
    RawSourceReference,
    ScreeningFeatureBatchEntry,
    register_phase1a_screening_feature_batch,
)
from systematic_fx.db.screening_registry import register_phase1a_screening_campaign
from systematic_fx.features.screening import (
    FEATURE_VERSION,
    FORMULA_SHA256,
    build_phase1a_screening_features,
    load_phase1a_screening_config,
    plan_phase1a_screening_no_entry_reason,
)
from systematic_fx.research.discovery_slice import (
    DISCOVERY_FORWARD_RESULT_FIELDS,
    DISCOVERY_SLICE_SCHEMA,
    DISCOVERY_SLICE_VERSION,
    DISCOVERY_VARIABLE_FIELDS,
    FORWARD_HORIZONS,
    QUANTILES_PPM,
    RATIO_SCALE_PPM,
    analyze_phase1a_discovery_slice,
    load_discovery_slice_config,
)
from systematic_fx.research.hypotheses import (
    HypothesisBundle,
    canonical_json_bytes,
    canonical_sha256,
    load_hypothesis_bundle,
    parse_hypothesis_bundle,
)
from systematic_fx.research.provenance import (
    build_code_snapshot,
    dependency_lock_sha256,
    publish_code_snapshot,
    runtime_environment,
)
from systematic_fx.research.run_spec import RunSpec
from systematic_fx.research.screening_config import load_conservative_screening_bundle
from systematic_fx.validation.splits import (
    CALENDAR_VERSION,
    CAMPAIGN_ID,
    SPLIT_VERSION,
    build_phase1a_screening_calendar,
    build_phase1a_screening_split,
    publish_phase1a_screening_artifacts,
)

PIPELINE_VERSION: Final = "phase1a_discovery_pipeline_v1"
FEATURE_RUN_ENGINE_VERSION: Final = "phase1a_screening_feature_builder_v1"
DISCOVERY_RUN_ENGINE_VERSION: Final = "phase1a_fixed_query_discovery_v1"
QUERY_RUN_ENGINE_VERSION: Final = "phase1a_fixed_query_projection_v1"
RECOVERY_CONTROL_ENGINE_VERSION: Final = "phase1a_partial_recovery_control_v1"
RECOVERY_CONTROL_SCHEMA: Final = "systematic_fx.phase1a_partial_recovery_control.v1"
RECOVERY_MANIFEST_SCHEMA: Final = "systematic_fx.phase1a_partial_recovery_manifest.v1"
RECOVERY_PROJECTION_SCHEMA: Final = "systematic_fx.phase1a_query_recovery_projection.v1"
RECOVERY_REGISTRAR_SCHEMA: Final = "systematic_fx.phase1a_pattern_recovery_registrar.v1"
DISCOVERY_SLICE_SIZE: Final = 5
DISCOVERY_SLICE_COUNT: Final = 99
MAX_DISCOVERY_SLICE_INDEX: Final = DISCOVERY_SLICE_COUNT - 1
RANDOM_SEED: Final = 0
SOURCE_MANIFEST_KEY: Final = "mbp10_source_sha256_v1"
QC_MANIFEST_KEY: Final = "mbp10_structural_qc_v1"
FOOTER_MANIFEST_KEY: Final = "mbp10_footer_manifest_v1"
MISSING_PREVIOUS_REASON: Final = "MISSING_PREVIOUS_COMPLETED_SESSION"
UNQUALIFIED_PREVIOUS_REASON: Final = "PREVIOUS_COMPLETED_SOURCE_NOT_QUALIFIED"
_QUERY_RESULT_LIMIT: Final = 20
_SUPPORTED_SCHEMA_MIGRATION_VERSIONS: Final = tuple(range(1, 28))


class Phase1APipelineError(RuntimeError):
    """One governed slice could not be completed or safely resumed."""


@dataclass(frozen=True, slots=True)
class ResolvedRunArtifact:
    """Immutable artifact attached to an already successful run attempt."""

    artifact_id: int
    path: Path
    sha256: str
    artifact_type: str


@dataclass(frozen=True, slots=True)
class PipelineRunReport:
    """Compact state for one feature, AI-slice, or query RunSpec."""

    run_kind: str
    run_fingerprint: str
    research_run_spec_id: int
    research_run_attempt_id: int
    attempt_status: str
    executed: bool
    result_artifact_id: int

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class Phase1ASliceReport:
    """Compact, non-sealed completion report for one Discovery slice."""

    pipeline_version: str
    slice_index: int
    source_dates: tuple[str, ...]
    built_source_dates: tuple[str, ...]
    no_entry_reasons: tuple[tuple[str, str], ...]
    calendar_sha256: str
    split_sha256: str
    code_commit: str
    code_snapshot_sha256: str
    code_snapshot_disposition: str
    analysis_code_commit: str
    analysis_code_snapshot_sha256: str
    recovery_mode: bool
    recovery_run: PipelineRunReport | None
    recovery_manifest_path: Path | None
    recovery_manifest_sha256: str | None
    campaign_id: int
    campaign_key: str
    feature_run: PipelineRunReport
    ai_slice_run: PipelineRunReport
    query_runs: tuple[PipelineRunReport, ...]
    discovery_artifact_path: Path
    discovery_artifact_sha256: str
    eligible_row_count: int
    nonzero_support_query_count: int
    pattern_observation_count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "ai_slice_run": self.ai_slice_run.as_dict(),
            "built_source_dates": list(self.built_source_dates),
            "calendar_sha256": self.calendar_sha256,
            "campaign_id": self.campaign_id,
            "campaign_key": self.campaign_key,
            "code_commit": self.code_commit,
            "code_snapshot_disposition": self.code_snapshot_disposition,
            "code_snapshot_sha256": self.code_snapshot_sha256,
            "analysis_code_commit": self.analysis_code_commit,
            "analysis_code_snapshot_sha256": self.analysis_code_snapshot_sha256,
            "discovery_artifact_path": str(self.discovery_artifact_path),
            "discovery_artifact_sha256": self.discovery_artifact_sha256,
            "eligible_row_count": self.eligible_row_count,
            "feature_run": self.feature_run.as_dict(),
            "no_entry_reasons": dict(self.no_entry_reasons),
            "nonzero_support_query_count": self.nonzero_support_query_count,
            "pattern_observation_count": self.pattern_observation_count,
            "pipeline_version": self.pipeline_version,
            "recovery_manifest_path": (
                str(self.recovery_manifest_path)
                if self.recovery_manifest_path is not None
                else None
            ),
            "recovery_manifest_sha256": self.recovery_manifest_sha256,
            "recovery_mode": self.recovery_mode,
            "recovery_run": self.recovery_run.as_dict() if self.recovery_run else None,
            "query_runs": [item.as_dict() for item in self.query_runs],
            "slice_index": self.slice_index,
            "source_dates": list(self.source_dates),
            "split_sha256": self.split_sha256,
        }


@dataclass(frozen=True, slots=True)
class _PlannedEntry:
    source: RawSourceReference
    source_path: Path
    status: BatchEntryStatus
    previous_source: RawSourceReference | None
    selection: ContractSelectionResult | None
    no_entry_reason: str | None


@dataclass(frozen=True, slots=True)
class PipelineServices:
    """Injectable side-effect boundary used by synthetic unit tests."""

    build_calendar: Callable[..., Any]
    build_split: Callable[..., Any]
    publish_calendar_split: Callable[..., Any]
    git_head: Callable[[Path], str]
    build_snapshot: Callable[..., Any]
    publish_snapshot: Callable[..., Any]
    dependency_hash: Callable[[Path], str]
    runtime: Callable[[], dict[str, object]]
    postgres_runtime: Callable[..., dict[str, object]]
    load_source_bundle: Callable[..., Any]
    register_campaign: Callable[..., Any]
    select_contract: Callable[..., ContractSelectionResult]
    plan_no_entry_reason: Callable[..., str | None]
    register_spec: Callable[..., Any]
    reserve_attempt: Callable[..., RunAttemptReservation]
    start_attempt: Callable[..., Any]
    finish_attempt: Callable[..., Any]
    build_features: Callable[..., Any]
    register_feature_batch: Callable[..., Any]
    analyze_slice: Callable[..., Any]
    complete_discovery_success: Callable[..., Any]
    complete_recovery_success: Callable[..., Any]
    verify_discovery_success: Callable[..., Any]
    verify_current_slice_prefix: Callable[..., Any]
    load_recovery_source: Callable[..., Any]
    verify_predecessor_slice: Callable[..., Any]
    record_pattern: Callable[..., Any]
    resolve_artifact: Callable[..., ResolvedRunArtifact]


def _git_head(project_root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--verify", "HEAD"],
            cwd=project_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise Phase1APipelineError("cannot resolve the repository base commit") from exc
    commit = result.stdout.strip()
    if len(commit) not in {40, 64} or any(
        character not in "0123456789abcdef" for character in commit
    ):
        raise Phase1APipelineError("git HEAD is not one full lowercase object ID")
    return commit


def _postgres_runtime(
    database_url: str,
    *,
    migrations_directory: Path | None = None,
) -> dict[str, object]:
    """Capture and verify the exact non-secret PostgreSQL schema identity."""

    try:
        expected = discover_migrations(migrations_directory)
    except MigrationError as exc:
        raise Phase1APipelineError("cannot resolve the supported PostgreSQL schema") from exc
    expected_versions = tuple(migration.version for migration in expected)
    if expected_versions != _SUPPORTED_SCHEMA_MIGRATION_VERSIONS:
        raise Phase1APipelineError(
            "Phase1A requires the checked-in PostgreSQL migrations 0001 through 0027 exactly"
        )

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        version_row = connection.execute(
            """
            SELECT current_setting('server_version') AS server_version,
                   current_setting('server_version_num') AS server_version_num
            """
        ).fetchone()
        applied_rows = connection.execute(
            """
            SELECT version, name, checksum
            FROM systematic_fx.schema_migrations
            ORDER BY version
            """
        ).fetchall()
    if version_row is None:
        raise Phase1APipelineError("PostgreSQL returned no runtime version row")

    try:
        applied = tuple(
            {
                "version": int(row["version"]),
                "name": str(row["name"]),
                "checksum": str(row["checksum"]),
            }
            for row in applied_rows
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise Phase1APipelineError(
            "PostgreSQL returned an invalid schema migration identity"
        ) from exc
    expected_identity = tuple(
        {
            "version": migration.version,
            "name": migration.name,
            "checksum": migration.checksum,
        }
        for migration in expected
    )
    if applied != expected_identity:
        raise Phase1APipelineError(
            "PostgreSQL schema migrations do not exactly match checked-in versions 0001-0027"
        )

    migration_document = list(applied)
    return {
        "server_version": str(version_row["server_version"]),
        "server_version_num": str(version_row["server_version_num"]),
        "schema_migrations": migration_document,
        "schema_migrations_sha256": canonical_sha256(migration_document),
    }


def _path_from_file_uri(value: object) -> Path:
    if not isinstance(value, str):
        raise Phase1APipelineError("successful artifact URI is not a string")
    parsed = urlparse(value)
    if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
        raise Phase1APipelineError("successful artifact must use a local file URI")
    return Path(unquote(parsed.path))


def _resolve_reused_artifact(
    database_url: str,
    *,
    reused_attempt_id: int,
    data_root: Path,
) -> ResolvedRunArtifact:
    """Resolve the exact artifact attached to a duplicate's successful attempt."""

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        row = connection.execute(
            """
            SELECT a.artifact_id, a.artifact_type, a.uri, a.sha256
            FROM systematic_fx.research_run_attempts r
            JOIN systematic_fx.artifacts a ON a.artifact_id = r.result_artifact_id
            WHERE r.research_run_attempt_id = %s AND r.status = 'SUCCEEDED'
            """,
            (reused_attempt_id,),
        ).fetchone()
    if row is None:
        raise Phase1APipelineError("duplicate attempt does not resolve to a successful artifact")
    path = _path_from_file_uri(row["uri"]).expanduser().resolve(strict=True)
    derived = (data_root / "derived").resolve(strict=True)
    if not path.is_relative_to(derived) or path.is_symlink() or not path.is_file():
        raise Phase1APipelineError("resolved successful artifact is outside data/derived")
    return ResolvedRunArtifact(
        artifact_id=int(row["artifact_id"]),
        path=path,
        sha256=str(row["sha256"]),
        artifact_type=str(row["artifact_type"]),
    )


DEFAULT_SERVICES: Final = PipelineServices(
    build_calendar=build_phase1a_screening_calendar,
    build_split=build_phase1a_screening_split,
    publish_calendar_split=publish_phase1a_screening_artifacts,
    git_head=_git_head,
    build_snapshot=build_code_snapshot,
    publish_snapshot=publish_code_snapshot,
    dependency_hash=dependency_lock_sha256,
    runtime=runtime_environment,
    postgres_runtime=_postgres_runtime,
    load_source_bundle=load_source_manifest_bundle,
    register_campaign=register_phase1a_screening_campaign,
    select_contract=select_next_eligible_contract,
    plan_no_entry_reason=plan_phase1a_screening_no_entry_reason,
    register_spec=register_run_spec,
    reserve_attempt=reserve_run_attempt,
    start_attempt=start_run_attempt,
    finish_attempt=finish_run_attempt,
    build_features=build_phase1a_screening_features,
    register_feature_batch=register_phase1a_screening_feature_batch,
    analyze_slice=analyze_phase1a_discovery_slice,
    complete_discovery_success=complete_discovery_run_success,
    complete_recovery_success=complete_phase1a_recovery_run_success,
    verify_discovery_success=verify_discovery_run_success,
    verify_current_slice_prefix=verify_phase1a_current_slice_prefix,
    load_recovery_source=load_phase1a_partial_recovery_source,
    verify_predecessor_slice=verify_phase1a_predecessor_slice,
    record_pattern=record_pattern_slice_observation,
    resolve_artifact=_resolve_reused_artifact,
)


def _strict_directory(value: Path | str, *, label: str, expected_name: str | None = None) -> Path:
    requested = Path(value).expanduser()
    if requested.is_symlink():
        raise Phase1APipelineError(f"{label} cannot be a symbolic link")
    try:
        resolved = requested.resolve(strict=True)
    except FileNotFoundError as exc:
        raise Phase1APipelineError(f"{label} does not exist") from exc
    if not resolved.is_dir() or (expected_name is not None and resolved.name != expected_name):
        raise Phase1APipelineError(f"{label} is not the expected directory")
    return resolved


def _canonical_value(value: object) -> object:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, float):
        raise Phase1APipelineError("frozen TOML policies cannot contain binary floats")
    if isinstance(value, Mapping):
        return {str(key): _canonical_value(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [_canonical_value(item) for item in value]
    raise Phase1APipelineError(f"unsupported frozen policy value: {type(value).__name__}")


def _load_toml(path: Path) -> tuple[dict[str, object], bytes]:
    """Stable-read one regular TOML file and return its detached document and bytes."""

    if path.is_symlink():
        raise Phase1APipelineError(f"frozen TOML input is unsafe or missing: {path.name}")
    try:
        before = path.stat()
        if not stat.S_ISREG(before.st_mode):
            raise Phase1APipelineError(f"frozen TOML input is unsafe or missing: {path.name}")
        with path.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                raise Phase1APipelineError(f"frozen TOML changed while opening: {path.name}")
            raw_bytes = handle.read()
            after = os.fstat(handle.fileno())
        identity_before = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        identity_after = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        if identity_before != identity_after or len(raw_bytes) != opened.st_size:
            raise Phase1APipelineError(f"frozen TOML changed while reading: {path.name}")
        raw = tomllib.loads(raw_bytes.decode("utf-8"))
    except Phase1APipelineError:
        raise
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise Phase1APipelineError(f"cannot load frozen TOML input: {path.name}") from exc
    canonical = _canonical_value(raw)
    if not isinstance(canonical, dict):  # pragma: no cover - a TOML document is an object
        raise Phase1APipelineError("frozen TOML input did not decode to an object")
    return canonical, raw_bytes


def _frozen_inputs(
    project_root: Path,
    *,
    screening: Any,
    feature_config: Any,
    discovery_config: Any,
    hypotheses: HypothesisBundle,
    qc_config: Any,
) -> dict[str, object]:
    paths_and_hashes = {
        "campaign": (screening.campaign.path, screening.campaign.sha256, "CANONICAL_TOML"),
        "cost": (screening.cost.path, screening.cost.sha256, "CANONICAL_TOML"),
        "execution": (
            screening.execution.path,
            screening.execution.sha256,
            "CANONICAL_TOML",
        ),
        "barrier_grid": (
            screening.barrier_grid.path,
            screening.barrier_grid.sha256,
            "CANONICAL_TOML",
        ),
        "feature": (feature_config.path, feature_config.sha256, "RAW_BYTES"),
        "discovery_query": (
            discovery_config.path,
            discovery_config.sha256,
            "RAW_BYTES",
        ),
        "parent_hypotheses": (
            project_root / "configs/research/phase1_parent_hypotheses_v1.toml",
            hypotheses.config_sha256,
            "HYPOTHESIS_REGISTRATION",
        ),
        "structural_qc": (
            project_root / "configs/data/mbp10_structural_qc_v1.toml",
            qc_config.sha256,
            "QC_SEMANTIC_TABLE",
        ),
    }
    frozen: dict[str, object] = {}
    for key, (path, expected_digest, hash_mode) in paths_and_hashes.items():
        resolved = Path(path).resolve(strict=True)
        document, raw_bytes = _load_toml(resolved)
        if hash_mode == "RAW_BYTES":
            observed_digest = hashlib.sha256(raw_bytes).hexdigest()
        elif hash_mode == "CANONICAL_TOML":
            observed_digest = canonical_sha256(document)
        elif hash_mode == "HYPOTHESIS_REGISTRATION":
            observed_digest = parse_hypothesis_bundle(document).config_sha256
        elif hash_mode == "QC_SEMANTIC_TABLE":
            quality = document.get("quality")
            if not isinstance(quality, dict):
                raise Phase1APipelineError("structural QC TOML lacks its quality table")
            observed_digest = canonical_sha256(quality)
        else:  # pragma: no cover - closed local table
            raise AssertionError(hash_mode)
        if observed_digest != expected_digest:
            raise Phase1APipelineError(f"frozen TOML identity drift: {resolved.name}")
        frozen[key] = {
            "document": document,
            "hash_mode": hash_mode,
            "relative_path": resolved.relative_to(project_root).as_posix(),
            "sha256": expected_digest,
        }
    return frozen


def _source_document(reference: RawSourceReference) -> dict[str, object]:
    return {
        "relative_uri": reference.relative_uri,
        "sha256": reference.sha256,
        "source_date": reference.source_date.isoformat(),
    }


def _raw_reference(record: SourceFileRegistration) -> RawSourceReference:
    return RawSourceReference(
        source_date=record.source_date,
        relative_uri=record.relative_uri,
        sha256=record.sha256,
    )


def _plan_entries(
    *,
    data_root: Path,
    requested_dates: tuple[date, ...],
    records: Sequence[SourceFileRegistration],
    qualified_dates: frozenset[date],
    select_contract: Callable[..., ContractSelectionResult],
    plan_no_entry_reason: Callable[..., str | None],
    calendar: Any,
    config_path: Path,
) -> tuple[_PlannedEntry, ...]:
    positions = {record.source_date: index for index, record in enumerate(records)}
    if len(positions) != len(records):
        raise Phase1APipelineError("source manifest contains duplicate source dates")
    plans: list[_PlannedEntry] = []
    for source_date in requested_dates:
        position = positions.get(source_date)
        if position is None:
            raise Phase1APipelineError("Discovery source date is absent from the source manifest")
        current_record = records[position]
        current = _raw_reference(current_record)
        current_path = data_root / "mbp-10" / current.relative_uri
        previous_record = records[position - 1] if position > 0 else None
        if previous_record is None:
            plans.append(
                _PlannedEntry(
                    source=current,
                    source_path=current_path,
                    status=BatchEntryStatus.RECORDED_NO_ENTRY,
                    previous_source=None,
                    selection=None,
                    no_entry_reason=MISSING_PREVIOUS_REASON,
                )
            )
            continue
        previous = _raw_reference(previous_record)
        previous_path = data_root / "mbp-10" / previous.relative_uri
        if previous.source_date not in qualified_dates:
            plans.append(
                _PlannedEntry(
                    source=current,
                    source_path=current_path,
                    status=BatchEntryStatus.RECORDED_NO_ENTRY,
                    previous_source=previous,
                    selection=None,
                    no_entry_reason=UNQUALIFIED_PREVIOUS_REASON,
                )
            )
            continue
        selection = select_contract(
            previous_path,
            current_path,
            previous_source_date=previous.source_date,
            eligible_source_date=source_date,
            previous_source_sha256=previous.sha256,
            eligible_source_sha256=current.sha256,
        )
        no_entry_reason = plan_no_entry_reason(
            current_path,
            data_root=data_root,
            source_date=source_date,
            selection=selection,
            calendar=calendar,
            config_path=config_path,
        )
        plans.append(
            _PlannedEntry(
                source=current,
                source_path=current_path,
                status=(
                    BatchEntryStatus.RECORDED_NO_ENTRY
                    if no_entry_reason is not None
                    else BatchEntryStatus.BUILT
                ),
                previous_source=previous,
                selection=selection,
                no_entry_reason=no_entry_reason,
            )
        )
    return tuple(plans)


def _feature_batch_parameters(
    plans: Sequence[_PlannedEntry],
    *,
    slice_index: int,
    config_sha256: str,
    frozen_inputs: Mapping[str, object],
) -> dict[str, object]:
    batch_entries = [
        {
            "current_source": _source_document(plan.source),
            "no_entry_reason": plan.no_entry_reason,
            "previous_source": (
                _source_document(plan.previous_source) if plan.previous_source is not None else None
            ),
            "previous_volume_sha256": (
                plan.selection.previous_volume.sha256 if plan.selection is not None else None
            ),
            "previous_volume_document": (
                plan.selection.previous_volume.as_dict() if plan.selection is not None else None
            ),
            "selection_sha256": plan.selection.sha256 if plan.selection is not None else None,
            "selection_document": (
                plan.selection.as_dict() if plan.selection is not None else None
            ),
            "status": plan.status.value,
        }
        for plan in plans
    ]
    all_sources = {
        (reference.source_date, reference.relative_uri): reference
        for plan in plans
        for reference in (plan.source, plan.previous_source)
        if reference is not None
    }
    no_entry = [plan for plan in plans if plan.status is BatchEntryStatus.RECORDED_NO_ENTRY]
    return {
        "batch_entries": batch_entries,
        "batch_source_dates": [plan.source.source_date.isoformat() for plan in plans],
        "batch_status_by_date": {
            plan.source.source_date.isoformat(): plan.status.value for plan in plans
        },
        "config_sha256": config_sha256,
        "definition_status_available": False,
        "formula_sha256": FORMULA_SHA256,
        "frozen_toml_inputs": dict(frozen_inputs),
        "no_entry_reason_by_date": {
            plan.source.source_date.isoformat(): plan.no_entry_reason for plan in no_entry
        },
        "pipeline_version": PIPELINE_VERSION,
        "previous_volume_sha256_by_date": {
            plan.source.source_date.isoformat(): plan.selection.previous_volume.sha256
            for plan in plans
            if plan.selection is not None
        },
        "raw_sources": [
            _source_document(reference)
            for reference in sorted(
                all_sources.values(),
                key=lambda item: (item.source_date, item.relative_uri),
            )
        ],
        "research_eligible": False,
        "screening_only": True,
        "selection_documents_by_date": {
            plan.source.source_date.isoformat(): plan.selection.as_dict()
            for plan in plans
            if plan.selection is not None
        },
        "selection_sha256_by_date": {
            plan.source.source_date.isoformat(): plan.selection.sha256
            for plan in plans
            if plan.selection is not None
        },
        "slice_index": slice_index,
    }


def _policies(
    frozen_inputs: Mapping[str, object],
) -> tuple[
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
]:
    feature = frozen_inputs["feature"]
    discovery = frozen_inputs["discovery_query"]
    campaign = frozen_inputs["campaign"]
    execution = frozen_inputs["execution"]
    barrier = frozen_inputs["barrier_grid"]
    assert isinstance(feature, dict) and isinstance(discovery, dict)
    assert isinstance(campaign, dict) and isinstance(execution, dict)
    assert isinstance(barrier, dict)
    execution_document = execution["document"]
    campaign_document = campaign["document"]
    assert isinstance(execution_document, dict) and isinstance(campaign_document, dict)
    return (
        {
            "discovery_query_config": discovery,
            "feature_config": feature,
            "signal_cadence_seconds": 300,
        },
        {
            "entry_gate": execution_document["entry_gate"],
            "entry_order": execution_document["entry_order"],
            "latency": execution_document["latency"],
            "source_config": execution,
        },
        {
            "barrier_grid": barrier,
            "event_ordering": execution_document["event_ordering"],
            "stop": execution_document["stop"],
            "take_profit": execution_document["take_profit"],
        },
        {
            "authority": campaign_document["authority"],
            "bracket": execution_document["bracket"],
            "terminal_exit": execution_document["terminal_exit"],
        },
    )


def _make_run_spec(
    *,
    run_kind: str,
    engine_version: str,
    calendar: Any,
    split: Any,
    screening: Any,
    feature_config: Any,
    footer_manifest_sha256: str,
    code_commit: str,
    code_snapshot_sha256: str,
    dependency_sha256: str,
    runtime: Mapping[str, object],
    frozen_inputs: Mapping[str, object],
    parameters: Mapping[str, object],
) -> RunSpec:
    signal_policy, entry_policy, barrier_policy, terminal_policy = _policies(frozen_inputs)
    return RunSpec(
        campaign_id=CAMPAIGN_ID,
        experiment_id=None,
        run_kind=run_kind,
        engine_version=engine_version,
        source_manifest_hashes={
            FOOTER_MANIFEST_KEY: footer_manifest_sha256,
            SOURCE_MANIFEST_KEY: calendar.source_manifest_sha256,
            QC_MANIFEST_KEY: calendar.qc_manifest_sha256,
        },
        eligible_calendar_version=CALENDAR_VERSION,
        eligible_calendar_sha256=calendar.sha256,
        split_version=SPLIT_VERSION,
        split_sha256=split.sha256,
        feature_version=FEATURE_VERSION,
        feature_sha256=feature_config.sha256,
        outcome_version=screening.outcome_version,
        outcome_sha256=screening.barrier_grid.sha256,
        cost_version=screening.cost_version,
        cost_sha256=screening.cost.sha256,
        execution_version=screening.execution_version,
        execution_sha256=screening.execution.sha256,
        code_commit=code_commit,
        code_snapshot_sha256=code_snapshot_sha256,
        dependency_lock_sha256=dependency_sha256,
        runtime_environment=runtime,
        random_seed=RANDOM_SEED,
        direction="BOTH",
        signal_policy=signal_policy,
        entry_policy=entry_policy,
        barrier_policy=barrier_policy,
        terminal_policy=terminal_policy,
        parameters=parameters,
    )


def _run_spec_from_recovery_source(
    parent_spec: Mapping[str, object],
    *,
    run_kind: str,
    engine_version: str,
    code_commit: str,
    code_snapshot_sha256: str,
    dependency_sha256: str,
    runtime: Mapping[str, object],
    parameters: Mapping[str, object],
) -> RunSpec:
    """Copy every research variable from the immutable AI source, changing only execution."""

    def versioned(name: str) -> tuple[str, str]:
        value = parent_spec.get(name)
        if not isinstance(value, Mapping):
            raise Phase1APipelineError(f"recovery source {name} identity is invalid")
        version = value.get("version")
        sha256 = value.get("sha256")
        if not isinstance(version, str) or not isinstance(sha256, str):
            raise Phase1APipelineError(f"recovery source {name} identity is incomplete")
        return version, sha256

    source_hashes = parent_spec.get("source_manifest_hashes")
    if not isinstance(source_hashes, Mapping):
        raise Phase1APipelineError("recovery source manifest identities are invalid")
    calendar_version, calendar_sha256 = versioned("eligible_calendar")
    split_version, split_sha256 = versioned("split")
    feature_version, feature_sha256 = versioned("feature")
    outcome_version, outcome_sha256 = versioned("outcome")
    cost_version, cost_sha256 = versioned("cost")
    execution_version, execution_sha256 = versioned("execution")
    policies: dict[str, Mapping[str, object]] = {}
    for name in ("signal_policy", "entry_policy", "barrier_policy", "terminal_policy"):
        value = parent_spec.get(name)
        if not isinstance(value, Mapping):
            raise Phase1APipelineError(f"recovery source {name} is invalid")
        policies[name] = value
    random_seed = parent_spec.get("random_seed")
    direction = parent_spec.get("direction")
    if isinstance(random_seed, bool) or not isinstance(random_seed, int):
        raise Phase1APipelineError("recovery source random_seed is invalid")
    if not isinstance(direction, str):
        raise Phase1APipelineError("recovery source direction is invalid")
    return RunSpec(
        campaign_id=CAMPAIGN_ID,
        experiment_id=None,
        run_kind=run_kind,
        engine_version=engine_version,
        source_manifest_hashes={str(key): str(value) for key, value in source_hashes.items()},
        eligible_calendar_version=calendar_version,
        eligible_calendar_sha256=calendar_sha256,
        split_version=split_version,
        split_sha256=split_sha256,
        feature_version=feature_version,
        feature_sha256=feature_sha256,
        outcome_version=outcome_version,
        outcome_sha256=outcome_sha256,
        cost_version=cost_version,
        cost_sha256=cost_sha256,
        execution_version=execution_version,
        execution_sha256=execution_sha256,
        code_commit=code_commit,
        code_snapshot_sha256=code_snapshot_sha256,
        dependency_lock_sha256=dependency_sha256,
        runtime_environment=runtime,
        random_seed=random_seed,
        direction=direction,
        signal_policy=policies["signal_policy"],
        entry_policy=policies["entry_policy"],
        barrier_policy=policies["barrier_policy"],
        terminal_policy=policies["terminal_policy"],
        parameters=parameters,
    )


def _publish_recovery_manifest(
    *,
    data_root: Path,
    document: Mapping[str, object],
) -> ResolvedRunArtifact:
    content = canonical_json_bytes(document) + b"\n"
    sha256 = hashlib.sha256(content).hexdigest()
    derived_path = data_root / "derived"
    manifests_path = derived_path / "manifests"
    directory_path = manifests_path / "phase1a_partial_recovery_v1"
    if derived_path.is_symlink() or not derived_path.is_dir():
        raise Phase1APipelineError("recovery manifest directory is unsafe")
    for path in (manifests_path, directory_path):
        if path.exists():
            if path.is_symlink() or not path.is_dir():
                raise Phase1APipelineError("recovery manifest directory is unsafe")
        else:
            path.mkdir()
    derived = derived_path.resolve(strict=True)
    directory = directory_path.resolve(strict=True)
    if not directory.is_relative_to(derived):
        raise Phase1APipelineError("recovery manifest directory is unsafe")
    destination = directory / f"sha256={sha256}.json"
    if destination.exists():
        raw, observed = _read_stable_artifact(destination)
        if raw != content or observed != sha256:
            raise Phase1APipelineError("existing recovery manifest content drift")
        if destination.stat().st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH):
            raise Phase1APipelineError("existing recovery manifest is writable")
    else:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=directory,
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o444)
            try:
                os.link(temporary, destination, follow_symlinks=False)
            except FileExistsError:
                raw, observed = _read_stable_artifact(destination)
                if raw != content or observed != sha256:
                    raise Phase1APipelineError("concurrent recovery manifest content drift")
        finally:
            if temporary.exists():
                temporary.unlink()
        directory_descriptor = os.open(
            directory,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    published = destination.stat()
    if published.st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH):
        raise Phase1APipelineError("published recovery manifest is writable")
    return ResolvedRunArtifact(
        artifact_id=0,
        path=destination,
        sha256=sha256,
        artifact_type="PHASE1A_SLICE_RECOVERY_MANIFEST",
    )


def _artifact_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolved_artifact_path(path: Path, *, data_root: Path) -> Path:
    requested = Path(path).expanduser()
    if requested.is_symlink():
        raise Phase1APipelineError("result artifact cannot be a symbolic link")
    try:
        resolved = requested.resolve(strict=True)
    except FileNotFoundError as exc:
        raise Phase1APipelineError("result artifact is missing") from exc
    derived = (data_root / "derived").resolve(strict=True)
    if not resolved.is_file() or not resolved.is_relative_to(derived):
        raise Phase1APipelineError("result artifact must remain below data/derived")
    return resolved


def _validated_artifact_path(path: Path, *, data_root: Path, expected_sha256: str) -> Path:
    resolved = _resolved_artifact_path(path, data_root=data_root)
    if _artifact_sha256(resolved) != expected_sha256:
        raise Phase1APipelineError("result artifact SHA-256 drift")
    return resolved


def _artifact_file_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_stable_artifact(path: Path) -> tuple[bytes, str]:
    """Read/hash one held non-symlink inode and prove its path binding stayed fixed."""

    no_follow = getattr(os, "O_NOFOLLOW", None)
    if no_follow is None:  # pragma: no cover - governed research requires POSIX semantics
        raise Phase1APipelineError("result artifact cannot be opened without O_NOFOLLOW")
    descriptor: int | None = None
    try:
        before_path = path.lstat()
        if stat.S_ISLNK(before_path.st_mode) or not stat.S_ISREG(before_path.st_mode):
            raise Phase1APipelineError("result artifact must be a regular non-symlink file")
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | no_follow)
        before_descriptor = os.fstat(descriptor)
        if _artifact_file_identity(before_descriptor) != _artifact_file_identity(before_path):
            raise Phase1APipelineError("result artifact changed before it was opened")
        digest = hashlib.sha256()
        chunks: list[bytes] = []
        byte_size = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
            chunks.append(chunk)
            byte_size += len(chunk)
        after_descriptor = os.fstat(descriptor)
        after_path = path.lstat()
        identities = {
            _artifact_file_identity(before_path),
            _artifact_file_identity(before_descriptor),
            _artifact_file_identity(after_descriptor),
            _artifact_file_identity(after_path),
        }
        if len(identities) != 1 or byte_size != before_descriptor.st_size:
            raise Phase1APipelineError("result artifact changed while it was read")
        return b"".join(chunks), digest.hexdigest()
    except Phase1APipelineError:
        raise
    except OSError as exc:
        raise Phase1APipelineError("result artifact could not be read safely") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _json_artifact(
    artifact: ResolvedRunArtifact,
    *,
    data_root: Path,
    expected_schema: str,
) -> tuple[Path, dict[str, object]]:
    path = _resolved_artifact_path(artifact.path, data_root=data_root)
    raw, observed_sha256 = _read_stable_artifact(path)
    if observed_sha256 != artifact.sha256:
        raise Phase1APipelineError("result artifact SHA-256 drift")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Phase1APipelineError("result artifact is not valid canonical JSON") from exc
    if not isinstance(value, dict) or value.get("artifact_schema") != expected_schema:
        raise Phase1APipelineError("result artifact schema drift")
    return path, value


def _reserve_run(
    *,
    services: PipelineServices,
    database_url: str,
    data_root: Path,
    run_spec: RunSpec,
    parent_run_fingerprint: str | None,
    slice_index: int,
    execute: Callable[[], ResolvedRunArtifact],
) -> tuple[PipelineRunReport, ResolvedRunArtifact]:
    registration = services.register_spec(
        database_url,
        run_spec,
        parent_run_fingerprint=parent_run_fingerprint,
    )
    reservation = services.reserve_attempt(
        database_url,
        run_fingerprint=run_spec.fingerprint,
    )
    if not reservation.execute:
        if reservation.reused_attempt_id is None:
            raise Phase1APipelineError("duplicate reservation lacks its reused successful attempt")
        artifact = services.resolve_artifact(
            database_url,
            reused_attempt_id=reservation.reused_attempt_id,
            data_root=data_root,
        )
        report = PipelineRunReport(
            run_kind=run_spec.run_kind,
            run_fingerprint=run_spec.fingerprint,
            research_run_spec_id=int(registration.research_run_spec_id),
            research_run_attempt_id=reservation.research_run_attempt_id,
            attempt_status=reservation.status,
            executed=False,
            result_artifact_id=artifact.artifact_id,
        )
        return report, artifact

    services.start_attempt(
        database_url,
        research_run_attempt_id=reservation.research_run_attempt_id,
    )
    try:
        artifact = execute()
        services.finish_attempt(
            database_url,
            research_run_attempt_id=reservation.research_run_attempt_id,
            status="SUCCEEDED",
            result_summary={
                "artifact_sha256": artifact.sha256,
                "pipeline_version": PIPELINE_VERSION,
                "run_fingerprint": run_spec.fingerprint,
                "slice_index": slice_index,
            },
            result_artifact_id=artifact.artifact_id,
        )
    except Exception as exc:
        safe_error = f"Phase1A {run_spec.run_kind} slice execution failed ({type(exc).__name__})"
        try:
            services.finish_attempt(
                database_url,
                research_run_attempt_id=reservation.research_run_attempt_id,
                status="FAILED",
                result_summary={
                    "pipeline_version": PIPELINE_VERSION,
                    "run_fingerprint": run_spec.fingerprint,
                    "slice_index": slice_index,
                },
                error_message=safe_error,
            )
        except Exception as terminal_error:
            raise Phase1APipelineError(
                f"{safe_error}; terminal FAILED transition also failed"
            ) from terminal_error
        raise Phase1APipelineError(safe_error) from exc
    report = PipelineRunReport(
        run_kind=run_spec.run_kind,
        run_fingerprint=run_spec.fingerprint,
        research_run_spec_id=int(registration.research_run_spec_id),
        research_run_attempt_id=reservation.research_run_attempt_id,
        attempt_status="SUCCEEDED",
        executed=True,
        result_artifact_id=artifact.artifact_id,
    )
    return report, artifact


def _reserve_recovery_run(
    *,
    services: PipelineServices,
    database_url: str,
    data_root: Path,
    run_spec: RunSpec,
    parent_run_fingerprint: str,
    slice_index: int,
    manifest: ResolvedRunArtifact,
    code_commit: str,
) -> tuple[PipelineRunReport, ResolvedRunArtifact]:
    registration = services.register_spec(
        database_url,
        run_spec,
        parent_run_fingerprint=parent_run_fingerprint,
    )
    reservation = services.reserve_attempt(
        database_url,
        run_fingerprint=run_spec.fingerprint,
    )
    if not reservation.execute:
        if reservation.reused_attempt_id is None:
            raise Phase1APipelineError("duplicate recovery lacks its successful attempt")
        artifact = services.resolve_artifact(
            database_url,
            reused_attempt_id=reservation.reused_attempt_id,
            data_root=data_root,
        )
        if (
            artifact.artifact_type != "PHASE1A_SLICE_RECOVERY_MANIFEST"
            or artifact.sha256 != manifest.sha256
            or artifact.path != manifest.path.resolve(strict=True)
        ):
            raise Phase1APipelineError("duplicate recovery manifest identity drift")
        return (
            PipelineRunReport(
                run_kind=run_spec.run_kind,
                run_fingerprint=run_spec.fingerprint,
                research_run_spec_id=int(registration.research_run_spec_id),
                research_run_attempt_id=reservation.research_run_attempt_id,
                attempt_status=reservation.status,
                executed=False,
                result_artifact_id=artifact.artifact_id,
            ),
            artifact,
        )

    services.start_attempt(
        database_url,
        research_run_attempt_id=reservation.research_run_attempt_id,
    )
    try:
        completion = services.complete_recovery_success(
            database_url,
            research_run_attempt_id=reservation.research_run_attempt_id,
            campaign_key=CAMPAIGN_ID,
            run_fingerprint=run_spec.fingerprint,
            code_commit=code_commit,
            expected_manifest_sha256=manifest.sha256,
            recovery_manifest_path=manifest.path,
            artifacts_root=data_root / "derived",
        )
        artifact_id = _required_artifact_id(
            completion.result_artifact_id,
            label="partial-recovery control completion",
        )
    except Exception as exc:
        safe_error = f"Phase1A recovery control failed ({type(exc).__name__})"
        try:
            services.finish_attempt(
                database_url,
                research_run_attempt_id=reservation.research_run_attempt_id,
                status="FAILED",
                result_summary={
                    "manifest_sha256": manifest.sha256,
                    "pipeline_version": PIPELINE_VERSION,
                    "run_fingerprint": run_spec.fingerprint,
                    "slice_index": slice_index,
                },
                error_message=safe_error,
            )
        except Exception as terminal_error:
            raise Phase1APipelineError(
                f"{safe_error}; terminal FAILED transition also failed"
            ) from terminal_error
        raise Phase1APipelineError(safe_error) from exc
    artifact = ResolvedRunArtifact(
        artifact_id=artifact_id,
        path=manifest.path,
        sha256=manifest.sha256,
        artifact_type="PHASE1A_SLICE_RECOVERY_MANIFEST",
    )
    return (
        PipelineRunReport(
            run_kind=run_spec.run_kind,
            run_fingerprint=run_spec.fingerprint,
            research_run_spec_id=int(registration.research_run_spec_id),
            research_run_attempt_id=reservation.research_run_attempt_id,
            attempt_status="SUCCEEDED",
            executed=True,
            result_artifact_id=artifact_id,
        ),
        artifact,
    )


def _reserve_discovery_run(
    *,
    services: PipelineServices,
    database_url: str,
    data_root: Path,
    run_spec: RunSpec,
    parent_run_fingerprint: str,
    slice_index: int,
    campaign_key: str,
    exposure_key: str,
    exposure_type: str,
    source_interval_start: datetime,
    source_interval_end: datetime,
    query_spec: Mapping[str, object],
    exposure_result_summary: Mapping[str, object],
    code_commit: str,
    config_sha256: str,
    build_artifact: Callable[[], tuple[Path, str]],
) -> tuple[PipelineRunReport, ResolvedRunArtifact]:
    """Reserve AI/QUERY work and commit success+artifact+exposure atomically."""

    parameters = json.loads(run_spec.canonical_json()).get("parameters")
    if (
        not isinstance(parameters, dict)
        or parameters.get("parent_run_fingerprint") != parent_run_fingerprint
    ):
        raise Phase1APipelineError("Discovery RunSpec parent fingerprint identity drift")
    registration = services.register_spec(
        database_url,
        run_spec,
        parent_run_fingerprint=parent_run_fingerprint,
    )
    reservation = services.reserve_attempt(
        database_url,
        run_fingerprint=run_spec.fingerprint,
    )
    if not reservation.execute:
        if reservation.reused_attempt_id is None:
            raise Phase1APipelineError("duplicate reservation lacks its reused successful attempt")
        artifact = services.resolve_artifact(
            database_url,
            reused_attempt_id=reservation.reused_attempt_id,
            data_root=data_root,
        )
        services.verify_discovery_success(
            database_url,
            campaign_key=campaign_key,
            exposure_key=exposure_key,
            exposure_type=exposure_type,
            source_interval_start=source_interval_start,
            source_interval_end=source_interval_end,
            query_spec=query_spec,
            exposure_result_summary=dict(exposure_result_summary),
            code_commit=code_commit,
            config_sha256=config_sha256,
            run_fingerprint=run_spec.fingerprint,
            result_artifact_id=artifact.artifact_id,
        )
        return (
            PipelineRunReport(
                run_kind=run_spec.run_kind,
                run_fingerprint=run_spec.fingerprint,
                research_run_spec_id=int(registration.research_run_spec_id),
                research_run_attempt_id=reservation.research_run_attempt_id,
                attempt_status=reservation.status,
                executed=False,
                result_artifact_id=artifact.artifact_id,
            ),
            artifact,
        )

    services.start_attempt(
        database_url,
        research_run_attempt_id=reservation.research_run_attempt_id,
    )
    try:
        artifact_path, artifact_sha256 = build_artifact()
        artifact_path = _validated_artifact_path(
            artifact_path,
            data_root=data_root,
            expected_sha256=artifact_sha256,
        )
        completion = services.complete_discovery_success(
            database_url,
            research_run_attempt_id=reservation.research_run_attempt_id,
            campaign_key=campaign_key,
            exposure_key=exposure_key,
            exposure_type=exposure_type,
            source_interval_start=source_interval_start,
            source_interval_end=source_interval_end,
            query_spec=query_spec,
            exposure_result_summary=dict(exposure_result_summary),
            attempt_result_summary={
                "artifact_sha256": artifact_sha256,
                "pipeline_version": PIPELINE_VERSION,
                "run_fingerprint": run_spec.fingerprint,
                "slice_index": slice_index,
            },
            code_commit=code_commit,
            config_sha256=config_sha256,
            run_fingerprint=run_spec.fingerprint,
            expected_artifact_sha256=artifact_sha256,
            result_artifact_path=artifact_path,
            artifacts_root=data_root / "derived",
        )
        artifact_id = _required_artifact_id(
            completion.result_artifact_id,
            label="atomic Discovery completion",
        )
    except Exception as exc:
        safe_error = f"Phase1A {run_spec.run_kind} slice execution failed ({type(exc).__name__})"
        try:
            services.finish_attempt(
                database_url,
                research_run_attempt_id=reservation.research_run_attempt_id,
                status="FAILED",
                result_summary={
                    "pipeline_version": PIPELINE_VERSION,
                    "run_fingerprint": run_spec.fingerprint,
                    "slice_index": slice_index,
                },
                error_message=safe_error,
            )
        except Exception as terminal_error:
            raise Phase1APipelineError(
                f"{safe_error}; terminal FAILED transition also failed"
            ) from terminal_error
        raise Phase1APipelineError(safe_error) from exc

    artifact = ResolvedRunArtifact(
        artifact_id=artifact_id,
        path=artifact_path,
        sha256=artifact_sha256,
        artifact_type="DISCOVERY_EXPOSURE_RESULT",
    )
    return (
        PipelineRunReport(
            run_kind=run_spec.run_kind,
            run_fingerprint=run_spec.fingerprint,
            research_run_spec_id=int(registration.research_run_spec_id),
            research_run_attempt_id=reservation.research_run_attempt_id,
            attempt_status="SUCCEEDED",
            executed=True,
            result_artifact_id=artifact_id,
        ),
        artifact,
    )


def _feature_inputs_from_manifest(
    artifact: ResolvedRunArtifact,
    *,
    data_root: Path,
    run_fingerprint: str,
    plans: Sequence[_PlannedEntry],
) -> tuple[dict[date, Path], dict[date, str]]:
    _, document = _json_artifact(
        artifact,
        data_root=data_root,
        expected_schema=FEATURE_BATCH_MANIFEST_SCHEMA,
    )
    run_spec = document.get("run_spec")
    batch = document.get("batch")
    if not isinstance(run_spec, dict) or run_spec.get("run_fingerprint") != run_fingerprint:
        raise Phase1APipelineError("feature manifest RunSpec fingerprint drift")
    if not isinstance(batch, dict) or not isinstance(batch.get("entries"), list):
        raise Phase1APipelineError("feature manifest batch is invalid")
    entries = batch["entries"]
    if len(entries) != len(plans):
        raise Phase1APipelineError("feature manifest batch cardinality drift")
    feature_paths: dict[date, Path] = {}
    feature_hashes: dict[date, str] = {}
    for plan, raw_entry in zip(plans, entries, strict=True):
        if not isinstance(raw_entry, dict) or raw_entry.get("status") != plan.status.value:
            raise Phase1APipelineError("feature manifest batch status drift")
        current = raw_entry.get("current_source")
        if current != _source_document(plan.source):
            raise Phase1APipelineError("feature manifest current source drift")
        if plan.status is BatchEntryStatus.RECORDED_NO_ENTRY:
            if raw_entry.get("no_entry_reason") != plan.no_entry_reason:
                raise Phase1APipelineError("feature manifest no-entry reason drift")
            continue
        artifacts = raw_entry.get("artifacts")
        if not isinstance(artifacts, list):
            raise Phase1APipelineError("feature manifest built entry lacks artifacts")
        matches = [
            item for item in artifacts if isinstance(item, dict) and item.get("granularity") == "5m"
        ]
        if len(matches) != 1:
            raise Phase1APipelineError("feature manifest requires exactly one 5m artifact")
        relative_uri = matches[0].get("original_relative_uri")
        sha256 = matches[0].get("sha256")
        if not isinstance(relative_uri, str) or not isinstance(sha256, str):
            raise Phase1APipelineError("feature manifest 5m artifact identity is invalid")
        path = (data_root / relative_uri).resolve(strict=True)
        if not path.is_relative_to((data_root / "derived/research_5m").resolve(strict=True)):
            raise Phase1APipelineError("feature manifest 5m path escaped research_5m")
        feature_paths[plan.source.source_date] = path
        feature_hashes[plan.source.source_date] = sha256
    return feature_paths, feature_hashes


def _slice_interval(source_dates: Sequence[date]) -> tuple[datetime, datetime]:
    return (
        datetime.combine(source_dates[0], time.min, tzinfo=UTC),
        datetime.combine(source_dates[-1] + timedelta(days=1), time.min, tzinfo=UTC),
    )


def _ensure_data_layout(data_root: Path) -> tuple[Path, Path, Path]:
    raw_root = data_root / "mbp-10"
    if raw_root.is_symlink() or not raw_root.is_dir():
        raise Phase1APipelineError("data/mbp-10 must be an existing non-symlink directory")
    derived = data_root / "derived"
    manifests = derived / "manifests"
    for path, label in ((derived, "data/derived"), (manifests, "data/derived/manifests")):
        if path.is_symlink():
            raise Phase1APipelineError(f"{label} cannot be a symbolic link")
        path.mkdir(mode=0o755, parents=True, exist_ok=True)
        if not path.is_dir():
            raise Phase1APipelineError(f"{label} is not a directory")
    return (
        raw_root.resolve(strict=True),
        derived.resolve(strict=True),
        manifests.resolve(strict=True),
    )


def _relative_to_data(path: Path, *, data_root: Path) -> str:
    try:
        return path.resolve(strict=True).relative_to(data_root).as_posix()
    except (FileNotFoundError, ValueError) as exc:
        raise Phase1APipelineError("artifact path is not an existing data-relative path") from exc


def _required_artifact_id(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise Phase1APipelineError(f"{label} did not return a positive artifact ID")
    return value


def _feature_entries(
    plans: Sequence[_PlannedEntry],
    *,
    services: PipelineServices,
    data_root: Path,
    calendar: Any,
    code_snapshot_sha256: str,
    config_path: Path,
) -> tuple[ScreeningFeatureBatchEntry, ...]:
    entries: list[ScreeningFeatureBatchEntry] = []
    for plan in plans:
        if plan.status is BatchEntryStatus.RECORDED_NO_ENTRY:
            entries.append(
                ScreeningFeatureBatchEntry(
                    source=plan.source,
                    status=plan.status,
                    previous_source=plan.previous_source,
                    selection=plan.selection,
                    no_entry_reason=plan.no_entry_reason,
                )
            )
            continue
        if plan.selection is None or plan.previous_source is None:
            raise Phase1APipelineError("built feature plan lacks selection lineage")
        report = services.build_features(
            plan.source_path,
            data_root=data_root,
            source_date=plan.source.source_date,
            selection=plan.selection,
            calendar=calendar,
            code_snapshot_sha256=code_snapshot_sha256,
            config_path=config_path,
        )
        entries.append(
            ScreeningFeatureBatchEntry(
                source=plan.source,
                status=plan.status,
                report=report,
                selection=plan.selection,
                previous_source=plan.previous_source,
            )
        )
    return tuple(entries)


def _truncating_division(numerator: int, denominator: int) -> int:
    return numerator // denominator if numerator >= 0 else -((-numerator) // denominator)


def _expected_integer_distribution(values: Sequence[int]) -> dict[str, object]:
    if not values:
        return {
            "count": 0,
            "maximum": None,
            "mean_trunc": None,
            "minimum": None,
            "quantiles_ppm": {},
        }
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "maximum": ordered[-1],
        "mean_trunc": _truncating_division(sum(ordered), len(ordered)),
        "minimum": ordered[0],
        "quantiles_ppm": {
            str(quantile): ordered[(quantile * (len(ordered) - 1)) // RATIO_SCALE_PPM]
            for quantile in QUANTILES_PPM
        },
    }


def _validate_query_result_evidence(
    result: Mapping[str, object],
    *,
    requested_source_dates: Sequence[date],
) -> None:
    if set(result) != {
        "definition",
        "direction_counts",
        "forward",
        "occurrences",
        "source_date_count",
        "support_count",
    }:
        raise Phase1APipelineError("Discovery query result field schema drift")
    support_count = result.get("support_count")
    source_date_count = result.get("source_date_count")
    occurrences = result.get("occurrences")
    if (
        isinstance(support_count, bool)
        or not isinstance(support_count, int)
        or support_count < 0
        or isinstance(source_date_count, bool)
        or not isinstance(source_date_count, int)
        or source_date_count < 0
        or not isinstance(occurrences, list)
        or len(occurrences) != support_count
    ):
        raise Phase1APipelineError("Discovery query support evidence drift")

    requested = {day.isoformat() for day in requested_source_dates}
    direction_counts = {"LONG": 0, "SHORT": 0}
    supported_dates: set[str] = set()
    seen_occurrences: set[tuple[str, int, str]] = set()
    prior_order: tuple[str, int] | None = None
    resolved: dict[int, list[dict[str, int]]] = {horizon: [] for horizon in FORWARD_HORIZONS}
    unresolved = dict.fromkeys(FORWARD_HORIZONS, 0)
    horizon_keys = {str(horizon) for horizon in FORWARD_HORIZONS}
    forward_result_fields = set(DISCOVERY_FORWARD_RESULT_FIELDS)

    for occurrence in occurrences:
        if not isinstance(occurrence, dict) or set(occurrence) != {
            "bucket_end_ns",
            "direction",
            "forward",
            "source_date",
            "variables",
        }:
            raise Phase1APipelineError("Discovery occurrence field schema drift")
        bucket_end_ns = occurrence.get("bucket_end_ns")
        direction = occurrence.get("direction")
        source_date = occurrence.get("source_date")
        variables = occurrence.get("variables")
        occurrence_forward = occurrence.get("forward")
        if (
            isinstance(bucket_end_ns, bool)
            or not isinstance(bucket_end_ns, int)
            or bucket_end_ns <= 0
            or direction not in direction_counts
            or not isinstance(source_date, str)
            or source_date not in requested
            or not isinstance(variables, dict)
            or set(variables) != set(DISCOVERY_VARIABLE_FIELDS)
            or any(isinstance(value, float) for value in variables.values())
            or not isinstance(occurrence_forward, dict)
            or set(occurrence_forward) != horizon_keys
        ):
            raise Phase1APipelineError("Discovery occurrence identity/variable schema drift")
        order = (source_date, bucket_end_ns)
        identity = (source_date, bucket_end_ns, str(direction))
        if (prior_order is not None and order <= prior_order) or identity in seen_occurrences:
            raise Phase1APipelineError("Discovery occurrences are duplicated or out of order")
        prior_order = order
        seen_occurrences.add(identity)
        direction_counts[str(direction)] += 1
        supported_dates.add(source_date)
        for horizon in FORWARD_HORIZONS:
            outcome = occurrence_forward[str(horizon)]
            if outcome is None:
                unresolved[horizon] += 1
                continue
            if (
                not isinstance(outcome, dict)
                or set(outcome) != forward_result_fields
                or any(
                    isinstance(value, bool) or not isinstance(value, int)
                    for value in outcome.values()
                )
            ):
                raise Phase1APipelineError("Discovery forward outcome schema drift")
            resolved[horizon].append(
                {key: int(outcome[key]) for key in DISCOVERY_FORWARD_RESULT_FIELDS}
            )

    if result.get("direction_counts") != direction_counts:
        raise Phase1APipelineError("Discovery direction counts do not match occurrences")
    if source_date_count != len(supported_dates):
        raise Phase1APipelineError("Discovery source-date support count drift")
    forward_summary = result.get("forward")
    if not isinstance(forward_summary, dict) or set(forward_summary) != horizon_keys:
        raise Phase1APipelineError("Discovery forward summary horizon schema drift")
    for horizon in FORWARD_HORIZONS:
        outcomes = resolved[horizon]
        aligned = [item["aligned_close_x2_ticks"] for item in outcomes]
        adverse = [item["maximum_adverse_excursion_x2_ticks"] for item in outcomes]
        favorable = [item["maximum_favorable_excursion_x2_ticks"] for item in outcomes]
        positive = sum(value > 0 for value in aligned)
        expected_summary = {
            "aligned_close_x2_ticks": _expected_integer_distribution(aligned),
            "maximum_adverse_excursion_x2_ticks": _expected_integer_distribution(adverse),
            "maximum_favorable_excursion_x2_ticks": _expected_integer_distribution(favorable),
            "negative_count": sum(value < 0 for value in aligned),
            "positive_count": positive,
            "positive_rate_ppm": (
                _truncating_division(positive * RATIO_SCALE_PPM, len(aligned)) if aligned else None
            ),
            "resolved_count": len(aligned),
            "unresolved_count": unresolved[horizon],
            "zero_count": sum(value == 0 for value in aligned),
        }
        if forward_summary[str(horizon)] != expected_summary:
            raise Phase1APipelineError("Discovery forward summary arithmetic drift")


def _validate_discovery_document(
    artifact: ResolvedRunArtifact,
    *,
    data_root: Path,
    run_fingerprint: str,
    source_dates: Sequence[date],
    discovery_config: Any,
    feature_sha256_by_date: Mapping[date, str],
    no_entry_reasons: Mapping[date, str],
) -> tuple[Path, dict[str, object]]:
    path, document = _json_artifact(
        artifact,
        data_root=data_root,
        expected_schema=DISCOVERY_SLICE_SCHEMA,
    )
    if document.get("artifact_version") != DISCOVERY_SLICE_VERSION:
        raise Phase1APipelineError("Discovery artifact version drift")
    if document.get("run_fingerprint") != run_fingerprint:
        raise Phase1APipelineError("Discovery artifact RunSpec fingerprint drift")
    if document.get("requested_source_dates") != [day.isoformat() for day in source_dates]:
        raise Phase1APipelineError("Discovery artifact source-date slice drift")
    expected_no_entry = {
        day.isoformat(): reason for day, reason in sorted(no_entry_reasons.items())
    }
    if document.get("no_entry_reasons") != expected_no_entry:
        raise Phase1APipelineError("Discovery artifact no-entry evidence drift")
    feature_inputs = document.get("feature_inputs")
    if not isinstance(feature_inputs, list):
        raise Phase1APipelineError("Discovery artifact feature identities are invalid")
    feature_identity_by_date: dict[str, Mapping[str, object]] = {}
    for item in feature_inputs:
        if not isinstance(item, dict) or not isinstance(item.get("source_date"), str):
            raise Phase1APipelineError("Discovery artifact feature identity is invalid")
        source_date = item["source_date"]
        if source_date in feature_identity_by_date:
            raise Phase1APipelineError("Discovery artifact repeats a feature source date")
        feature_identity_by_date[source_date] = item
    expected_feature_hashes = {
        day.isoformat(): digest for day, digest in sorted(feature_sha256_by_date.items())
    }
    if set(feature_identity_by_date) != set(expected_feature_hashes) or any(
        feature_identity_by_date[day].get("sha256") != digest
        for day, digest in expected_feature_hashes.items()
    ):
        raise Phase1APipelineError("Discovery artifact feature-input SHA identity drift")
    config = document.get("config")
    if not isinstance(config, dict) or config != {
        "definition_sha256": discovery_config.definition_sha256,
        "relative_path": "configs/research/phase1a_discovery_slice_v1.toml",
        "sha256": discovery_config.sha256,
    }:
        raise Phase1APipelineError("Discovery artifact query-config identity drift")
    query_results = document.get("query_results")
    expected_definitions = [query.as_dict() for query in discovery_config.candidate_queries]
    if not isinstance(query_results, list) or len(query_results) != len(expected_definitions):
        raise Phase1APipelineError("Discovery artifact query cardinality drift")
    for result, definition in zip(query_results, expected_definitions, strict=True):
        if not isinstance(result, dict) or result.get("definition") != definition:
            raise Phase1APipelineError("Discovery artifact query definition/order drift")
        _validate_query_result_evidence(
            result,
            requested_source_dates=source_dates,
        )
    summary = document.get("summary")
    if not isinstance(summary, dict):
        raise Phase1APipelineError("Discovery artifact summary is missing")
    if summary.get("candidate_query_count") != len(expected_definitions):
        raise Phase1APipelineError("Discovery artifact query summary drift")
    nonzero_count = sum(int(result["support_count"] > 0) for result in query_results)
    if (
        summary.get("nonzero_support_query_count") != nonzero_count
        or summary.get("zero_support_query_count") != len(query_results) - nonzero_count
    ):
        raise Phase1APipelineError("Discovery artifact support summary drift")
    for key in ("eligible_rows", "feature_rows"):
        value = summary.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise Phase1APipelineError("Discovery artifact row summary is invalid")
    return path, document


def _counterexamples(query_result: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    occurrences = query_result.get("occurrences")
    if not isinstance(occurrences, list):
        raise Phase1APipelineError("query result occurrences are invalid")
    selected: list[Mapping[str, object]] = []
    for occurrence in occurrences:
        if not isinstance(occurrence, dict):
            raise Phase1APipelineError("query occurrence is invalid")
        forward = occurrence.get("forward")
        horizon = forward.get("12") if isinstance(forward, dict) else None
        aligned = horizon.get("aligned_close_x2_ticks") if isinstance(horizon, dict) else None
        if isinstance(aligned, int) and not isinstance(aligned, bool) and aligned <= 0:
            selected.append(occurrence)
            if len(selected) == _QUERY_RESULT_LIMIT:
                break
    return tuple(selected)


def _parent_rationale(
    definition: Mapping[str, object],
    *,
    hypotheses: HypothesisBundle,
) -> str:
    parent_ids = definition.get("parent_hypothesis_ids")
    if not isinstance(parent_ids, list) or not parent_ids:
        raise Phase1APipelineError("candidate query lacks parent hypotheses")
    by_id = {item.hypothesis_id: item for item in hypotheses.hypotheses}
    try:
        parents = [by_id[str(parent_id)] for parent_id in parent_ids]
    except KeyError as exc:
        raise Phase1APipelineError(
            "candidate query references an unknown parent hypothesis"
        ) from exc
    return " | ".join(f"{parent.hypothesis_id}: {parent.economic_rationale}" for parent in parents)


def _pattern_observation(
    *,
    query_result: Mapping[str, object],
    query_run_fingerprint: str,
    exposure_key: str,
    discovery_artifact_sha256: str,
    discovery_document: Mapping[str, object],
    feature_manifest_sha256: str,
    footer_manifest_sha256: str,
    calendar: Any,
    screening: Any,
    feature_config: Any,
    discovery_config: Any,
    hypotheses: HypothesisBundle,
    code_snapshot_sha256: str,
) -> PatternSliceObservation:
    definition = query_result.get("definition")
    if not isinstance(definition, dict):
        raise Phase1APipelineError("query result definition is invalid")
    query_id = definition.get("id")
    conditions = definition.get("conditions")
    direction_rule = definition.get("direction_rule")
    support_count = query_result.get("support_count")
    if (
        not isinstance(query_id, str)
        or not isinstance(conditions, list)
        or not all(isinstance(value, str) for value in conditions)
        or not isinstance(direction_rule, str)
        or isinstance(support_count, bool)
        or not isinstance(support_count, int)
    ):
        raise Phase1APipelineError("query result identity is invalid")
    feature_inputs = discovery_document.get("feature_inputs")
    if not isinstance(feature_inputs, list):
        raise Phase1APipelineError("Discovery artifact feature identities are invalid")
    return PatternSliceObservation(
        campaign_key=CAMPAIGN_ID,
        pattern_key=f"{CAMPAIGN_ID}:{query_id}",
        query_id=query_id,
        run_fingerprint=query_run_fingerprint,
        exposure_key=exposure_key,
        query_definition=definition,
        feature_identity={
            "calendar_sha256": calendar.sha256,
            "code_snapshot_sha256": code_snapshot_sha256,
            "discovery_artifact_sha256": discovery_artifact_sha256,
            "discovery_config_sha256": discovery_config.sha256,
            "feature_config_sha256": feature_config.sha256,
            "feature_inputs": feature_inputs,
            "feature_manifest_sha256": feature_manifest_sha256,
            "feature_version": FEATURE_VERSION,
            "footer_manifest_sha256": footer_manifest_sha256,
            "formula_sha256": FORMULA_SHA256,
            "qc_manifest_sha256": calendar.qc_manifest_sha256,
            "source_manifest_sha256": calendar.source_manifest_sha256,
        },
        direction="BOTH",
        entry_condition=f"direction_rule={direction_rule}; " + "; ".join(conditions),
        economic_rationale=_parent_rationale(definition, hypotheses=hypotheses),
        applicable_regime={
            "authority": "OPEN_OBSERVATION",
            "definition_status_available": False,
            "parent_hypothesis_ids": definition["parent_hypothesis_ids"],
            "research_eligible": False,
            "screening_only": True,
            "signal_cadence_seconds": 300,
        },
        counterexamples=_counterexamples(query_result),
        support_count=support_count,
        candidate_barrier_region={
            "cell_count": len(screening.barrier_ticks) ** 2,
            "stop_loss_pips": [value // 2 for value in screening.barrier_ticks],
            "stop_loss_ticks": list(screening.barrier_ticks),
            "status": "NOT_EVALUATED_IN_DISCOVERY_SLICE",
            "take_profit_pips": [value // 2 for value in screening.barrier_ticks],
            "take_profit_ticks": list(screening.barrier_ticks),
        },
        forward_first_touch_summary={
            "direction_counts": query_result.get("direction_counts"),
            "forward_close_and_excursion_proxy": query_result.get("forward"),
            "first_touch_status": "NOT_COMPUTED",
            "reason": "DISCOVERY_SLICE_HAS_NO_EVENT_LEVEL_FIRST_TOUCH_OUTCOME",
            "source_date_count": query_result.get("source_date_count"),
        },
        cost_assumptions={
            "allocated_fixed_cost_ticks": screening.allocated_fixed_cost_ticks,
            "baseline_cost_floor_ticks": screening.baseline_cost_floor_ticks,
            "cost_config_sha256": screening.cost.sha256,
            "cost_model_version": screening.cost_version,
            "status": "RECORDED_NOT_APPLIED_TO_FORWARD_PROXY",
            "variable_cost_ticks": screening.variable_cost_ticks,
        },
    )


def _recovery_source_artifact(
    source: Phase1APartialRecoverySource,
    *,
    data_root: Path,
) -> ResolvedRunArtifact:
    path = _path_from_file_uri(source.result_artifact_uri).resolve(strict=True)
    artifact = ResolvedRunArtifact(
        artifact_id=source.result_artifact_id,
        path=path,
        sha256=source.result_artifact_sha256,
        artifact_type="DISCOVERY_EXPOSURE_RESULT",
    )
    _resolved_artifact_path(path, data_root=data_root)
    return artifact


def _source_mapping(value: object, *, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise Phase1APipelineError(f"{label} is not a canonical mapping")
    return {str(key): item for key, item in value.items()}


def _recovered_run_report(
    *,
    run_kind: str,
    run_fingerprint: str,
    research_run_spec_id: int,
    research_run_attempt_id: int,
    result_artifact_id: int,
) -> PipelineRunReport:
    return PipelineRunReport(
        run_kind=run_kind,
        run_fingerprint=run_fingerprint,
        research_run_spec_id=research_run_spec_id,
        research_run_attempt_id=research_run_attempt_id,
        attempt_status="SUCCEEDED",
        executed=False,
        result_artifact_id=result_artifact_id,
    )


def _recover_phase1a_discovery_slice(
    *,
    database_url: str,
    data_root: Path,
    slice_index: int,
    source_dates: Sequence[date],
    interval_start: datetime,
    interval_end: datetime,
    prefix: Phase1ACurrentSlicePrefixReport,
    services: PipelineServices,
    campaign: Any,
    calendar: Any,
    split: Any,
    discovery_config: Any,
    code_commit: str,
    code_snapshot_sha256: str,
    code_snapshot_disposition: str,
    dependency_sha256: str,
    runtime: Mapping[str, object],
) -> Phase1ASliceReport:
    """Finish an immutable registration prefix without rebuilding features or analysis."""

    source = services.load_recovery_source(
        database_url,
        campaign_key=CAMPAIGN_ID,
        prefix=prefix,
    )
    artifact = _recovery_source_artifact(source, data_root=data_root)
    ai_spec = _source_mapping(source.ai_canonical_spec, label="source AI RunSpec")
    ai_parameters = _source_mapping(
        ai_spec.get("parameters"),
        label="source AI parameters",
    )
    feature_inputs_by_date = _source_mapping(
        ai_parameters.get("feature_inputs_by_date"),
        label="source AI feature inputs",
    )
    feature_hashes: dict[date, str] = {}
    for value, identity in feature_inputs_by_date.items():
        try:
            source_date = date.fromisoformat(value)
        except ValueError as exc:
            raise Phase1APipelineError("source AI feature date is invalid") from exc
        identity_mapping = _source_mapping(identity, label="source AI feature identity")
        sha256 = identity_mapping.get("sha256")
        if not isinstance(sha256, str):
            raise Phase1APipelineError("source AI feature SHA-256 is invalid")
        feature_hashes[source_date] = sha256
    raw_no_entry = _source_mapping(
        ai_parameters.get("no_entry_reason_by_date"),
        label="source AI no-entry reasons",
    )
    no_entry_reasons: dict[date, str] = {}
    for value, reason in raw_no_entry.items():
        try:
            source_date = date.fromisoformat(value)
        except ValueError as exc:
            raise Phase1APipelineError("source AI no-entry date is invalid") from exc
        if not isinstance(reason, str) or not reason:
            raise Phase1APipelineError("source AI no-entry reason is invalid")
        no_entry_reasons[source_date] = reason
    ai_path, discovery_document = _validate_discovery_document(
        artifact,
        data_root=data_root,
        run_fingerprint=source.ai_run_fingerprint,
        source_dates=source_dates,
        discovery_config=discovery_config,
        feature_sha256_by_date=feature_hashes,
        no_entry_reasons=no_entry_reasons,
    )
    if discovery_document.get("code_snapshot_sha256") != ai_spec.get("code_snapshot_sha256"):
        raise Phase1APipelineError("source Discovery artifact analysis-code identity drift")
    query_results_value = discovery_document.get("query_results")
    if not isinstance(query_results_value, list):
        raise Phase1APipelineError("source Discovery artifact has no query results")
    query_results = tuple(
        _source_mapping(value, label="source query result") for value in query_results_value
    )
    query_ids = tuple(
        str(_source_mapping(item.get("definition"), label="source query definition")["id"])
        for item in query_results
    )
    if (
        tuple(item.query_id for item in source.query_prefix)
        != query_ids[: len(source.query_prefix)]
    ):
        raise Phase1APipelineError("partial QUERY prefix does not match artifact order")
    if len(source.pattern_ids) not in {
        len(source.query_prefix),
        max(0, len(source.query_prefix) - 1),
    }:
        raise Phase1APipelineError("partial pattern prefix cardinality drift")

    feature_run = _recovered_run_report(
        run_kind="FEATURE_BUILD",
        run_fingerprint=source.feature_run_fingerprint,
        research_run_spec_id=source.feature_run_spec_id,
        research_run_attempt_id=source.feature_success_attempt_id,
        result_artifact_id=source.feature_result_artifact_id,
    )
    ai_run = _recovered_run_report(
        run_kind="AI_SLICE",
        run_fingerprint=source.ai_run_fingerprint,
        research_run_spec_id=source.ai_run_spec_id,
        research_run_attempt_id=source.ai_success_attempt_id,
        result_artifact_id=source.result_artifact_id,
    )
    query_runs = [
        _recovered_run_report(
            run_kind="QUERY",
            run_fingerprint=item.run_fingerprint,
            research_run_spec_id=item.research_run_spec_id,
            research_run_attempt_id=item.success_attempt_id,
            result_artifact_id=source.result_artifact_id,
        )
        for item in source.query_prefix
    ]
    analysis_code_commit = ai_spec.get("code_commit")
    analysis_code_snapshot_sha256 = ai_spec.get("code_snapshot_sha256")
    if not isinstance(analysis_code_commit, str) or not isinstance(
        analysis_code_snapshot_sha256,
        str,
    ):
        raise Phase1APipelineError("source analysis code identity is invalid")

    summary = _source_mapping(discovery_document.get("summary"), label="Discovery summary")
    eligible_rows = summary.get("eligible_rows")
    nonzero_queries = summary.get("nonzero_support_query_count")
    if (
        isinstance(eligible_rows, bool)
        or not isinstance(eligible_rows, int)
        or isinstance(nonzero_queries, bool)
        or not isinstance(nonzero_queries, int)
    ):
        raise Phase1APipelineError("Discovery summary counts are invalid")

    if len(source.query_prefix) == len(query_results) and len(source.pattern_ids) == len(
        query_results
    ):
        return Phase1ASliceReport(
            pipeline_version=PIPELINE_VERSION,
            slice_index=slice_index,
            source_dates=tuple(day.isoformat() for day in source_dates),
            built_source_dates=tuple(sorted(day.isoformat() for day in feature_hashes)),
            no_entry_reasons=tuple(
                (day.isoformat(), reason) for day, reason in sorted(no_entry_reasons.items())
            ),
            calendar_sha256=calendar.sha256,
            split_sha256=split.sha256,
            code_commit=code_commit,
            code_snapshot_sha256=code_snapshot_sha256,
            code_snapshot_disposition=code_snapshot_disposition,
            analysis_code_commit=analysis_code_commit,
            analysis_code_snapshot_sha256=analysis_code_snapshot_sha256,
            recovery_mode=analysis_code_snapshot_sha256 != code_snapshot_sha256,
            recovery_run=None,
            recovery_manifest_path=None,
            recovery_manifest_sha256=None,
            campaign_id=int(campaign.campaign_id),
            campaign_key=campaign.campaign_key,
            feature_run=feature_run,
            ai_slice_run=ai_run,
            query_runs=tuple(query_runs),
            discovery_artifact_path=ai_path,
            discovery_artifact_sha256=artifact.sha256,
            eligible_row_count=eligible_rows,
            nonzero_support_query_count=nonzero_queries,
            pattern_observation_count=len(query_results),
        )

    derived_root = (data_root / "derived").resolve(strict=True)
    source_relative_path = ai_path.relative_to(derived_root).as_posix()
    existing_queries = [
        {
            "canonical_sha256": canonical_sha256(item.canonical_spec),
            "discovery_exposure_id": item.discovery_exposure_id,
            "pattern_recorded": index < len(source.pattern_ids),
            "query_id": item.query_id,
            "research_run_spec_id": item.research_run_spec_id,
            "run_fingerprint": item.run_fingerprint,
            "success_attempt_id": item.success_attempt_id,
        }
        for index, item in enumerate(source.query_prefix)
    ]
    repair_ids = (
        [source.missing_pattern_query_id] if source.missing_pattern_query_id is not None else []
    )
    projected_ids = list(query_ids[len(source.query_prefix) :])
    runtime_document = dict(runtime)
    manifest_document: dict[str, object] = {
        "artifact_schema": RECOVERY_MANIFEST_SCHEMA,
        "campaign_key": CAMPAIGN_ID,
        "no_research_recomputation": True,
        "pipeline_version": PIPELINE_VERSION,
        "planned_actions": {
            "project_missing_query_ids": projected_ids,
            "repair_existing_query_pattern_ids": repair_ids,
            "total_query_count": len(query_results),
        },
        "query_evidence": [
            {
                "definition_sha256": canonical_sha256(item["definition"]),
                "query_id": query_id,
                "query_result_sha256": canonical_sha256(item),
                "source_date_count": item.get("source_date_count"),
                "support_count": item.get("support_count"),
            }
            for query_id, item in zip(query_ids, query_results, strict=True)
        ],
        "recovery_execution": {
            "code_commit": code_commit,
            "code_snapshot_sha256": code_snapshot_sha256,
            "dependency_lock_sha256": dependency_sha256,
            "runtime_environment": runtime_document,
            "runtime_environment_sha256": canonical_sha256(runtime_document),
        },
        "requested_source_dates": [day.isoformat() for day in source_dates],
        "slice_index": slice_index,
        "source_prefix": {
            "ai": {
                "canonical_sha256": canonical_sha256(source.ai_canonical_spec),
                "code_commit": analysis_code_commit,
                "code_snapshot_sha256": analysis_code_snapshot_sha256,
                "discovery_exposure_id": source.ai_exposure_id,
                "research_run_spec_id": source.ai_run_spec_id,
                "run_fingerprint": source.ai_run_fingerprint,
                "success_attempt_id": source.ai_success_attempt_id,
            },
            "discovery_artifact": {
                "artifact_id": source.result_artifact_id,
                "byte_size": source.result_artifact_byte_size,
                "relative_path": source_relative_path,
                "sha256": source.result_artifact_sha256,
            },
            "existing_pattern_ids": list(source.pattern_ids),
            "existing_queries": existing_queries,
            "feature": {
                "canonical_sha256": canonical_sha256(source.feature_canonical_spec),
                "research_run_spec_id": source.feature_run_spec_id,
                "result_artifact_id": source.feature_result_artifact_id,
                "run_fingerprint": source.feature_run_fingerprint,
                "success_attempt_id": source.feature_success_attempt_id,
            },
            "missing_pattern_query_id": source.missing_pattern_query_id,
        },
    }
    recovery_manifest = _publish_recovery_manifest(
        data_root=data_root,
        document=manifest_document,
    )
    recovery_relative_path = recovery_manifest.path.relative_to(derived_root).as_posix()
    control_parameters = {
        "artifact_schema": RECOVERY_CONTROL_SCHEMA,
        "discovery_artifact_sha256": source.result_artifact_sha256,
        "no_research_recomputation": True,
        "parent_run_fingerprint": source.ai_run_fingerprint,
        "pipeline_version": PIPELINE_VERSION,
        "recovery_manifest_relative_path": recovery_relative_path,
        "recovery_manifest_sha256": recovery_manifest.sha256,
        "requested_source_dates": [day.isoformat() for day in source_dates],
        "slice_index": slice_index,
        "source_ai_canonical_sha256": canonical_sha256(source.ai_canonical_spec),
        "source_ai_code_snapshot_sha256": analysis_code_snapshot_sha256,
        "source_artifact_id": source.result_artifact_id,
        "source_artifact_relative_path": source_relative_path,
    }
    control_spec = _run_spec_from_recovery_source(
        source.ai_canonical_spec,
        run_kind="VALIDATION",
        engine_version=RECOVERY_CONTROL_ENGINE_VERSION,
        code_commit=code_commit,
        code_snapshot_sha256=code_snapshot_sha256,
        dependency_sha256=dependency_sha256,
        runtime=runtime,
        parameters=control_parameters,
    )
    recovery_run, recovery_artifact = _reserve_recovery_run(
        services=services,
        database_url=database_url,
        data_root=data_root,
        run_spec=control_spec,
        parent_run_fingerprint=source.ai_run_fingerprint,
        slice_index=slice_index,
        manifest=recovery_manifest,
        code_commit=code_commit,
    )
    recovery_identity = {
        "recovery_code_commit": code_commit,
        "recovery_code_snapshot_sha256": code_snapshot_sha256,
        "recovery_control_run_fingerprint": control_spec.fingerprint,
        "recovery_manifest_artifact_id": recovery_artifact.artifact_id,
        "recovery_manifest_relative_path": recovery_relative_path,
        "recovery_manifest_sha256": recovery_manifest.sha256,
        "recovery_runtime_sha256": canonical_sha256(runtime_document),
        "source_ai_canonical_sha256": canonical_sha256(source.ai_canonical_spec),
        "source_ai_code_snapshot_sha256": analysis_code_snapshot_sha256,
        "source_ai_run_fingerprint": source.ai_run_fingerprint,
        "source_artifact_id": source.result_artifact_id,
        "source_artifact_sha256": source.result_artifact_sha256,
    }

    if source.missing_pattern_query_id is not None:
        index = query_ids.index(source.missing_pattern_query_id)
        existing = source.query_prefix[index]
        existing_parameters = _source_mapping(
            existing.canonical_spec.get("parameters"),
            label="existing QUERY parameters",
        )
        registrar = None
        if existing_parameters.get("recovery_projection") is None:
            registrar = {
                "artifact_schema": RECOVERY_REGISTRAR_SCHEMA,
                "mode": "IMMUTABLE_AI_ARTIFACT_PATTERN_RECOVERY",
                "no_research_recomputation": True,
                **recovery_identity,
            }
        observation = derive_phase1a_pattern_observation(
            campaign_key=CAMPAIGN_ID,
            query_run_fingerprint=existing.run_fingerprint,
            exposure_key=f"{CAMPAIGN_ID}:query:{slice_index:02d}:{existing.query_id}",
            query_run_spec=existing.canonical_spec,
            ai_run_spec=source.ai_canonical_spec,
            artifact_sha256=source.result_artifact_sha256,
            artifact_document=discovery_document,
            query_result=query_results[index],
            rollup_registrar=registrar,
        )
        services.record_pattern(database_url, observation)

    frozen_inputs = _source_mapping(
        ai_parameters.get("frozen_toml_inputs"),
        label="source AI frozen inputs",
    )
    for index in range(len(source.query_prefix), len(query_results)):
        query_result = query_results[index]
        definition = _source_mapping(
            query_result.get("definition"),
            label="recovery query definition",
        )
        query_id = query_ids[index]
        definition_sha256 = canonical_sha256(definition)
        projection = {
            "artifact_schema": RECOVERY_PROJECTION_SCHEMA,
            "mode": "IMMUTABLE_AI_ARTIFACT_PROJECTION",
            "no_research_recomputation": True,
            **recovery_identity,
        }
        query_parameters = {
            "candidate_query": definition,
            "discovery_artifact_relative_path": _relative_to_data(ai_path, data_root=data_root),
            "discovery_artifact_sha256": source.result_artifact_sha256,
            "frozen_toml_inputs": frozen_inputs,
            "parent_run_fingerprint": source.ai_run_fingerprint,
            "pipeline_version": PIPELINE_VERSION,
            "query_definition_sha256": definition_sha256,
            "query_result_sha256": canonical_sha256(query_result),
            "recovery_projection": projection,
            "requested_source_dates": [day.isoformat() for day in source_dates],
            "research_eligible": False,
            "screening_only": True,
            "slice_index": slice_index,
        }
        query_spec = _run_spec_from_recovery_source(
            source.ai_canonical_spec,
            run_kind="QUERY",
            engine_version=QUERY_RUN_ENGINE_VERSION,
            code_commit=code_commit,
            code_snapshot_sha256=code_snapshot_sha256,
            dependency_sha256=dependency_sha256,
            runtime=runtime,
            parameters=query_parameters,
        )
        exposure_key = f"{CAMPAIGN_ID}:query:{slice_index:02d}:{query_id}"

        def reuse_source_artifact() -> tuple[Path, str]:
            return ai_path, source.result_artifact_sha256

        query_run, query_artifact = _reserve_discovery_run(
            services=services,
            database_url=database_url,
            data_root=data_root,
            run_spec=query_spec,
            parent_run_fingerprint=source.ai_run_fingerprint,
            slice_index=slice_index,
            campaign_key=CAMPAIGN_ID,
            exposure_key=exposure_key,
            exposure_type="QUERY",
            source_interval_start=interval_start,
            source_interval_end=interval_end,
            query_spec={
                "candidate_query": definition,
                "query_definition_sha256": definition_sha256,
                "run_fingerprint": query_spec.fingerprint,
            },
            exposure_result_summary={
                "artifact_sha256": source.result_artifact_sha256,
                "direction_counts": query_result.get("direction_counts"),
                "source_date_count": query_result.get("source_date_count"),
                "support_count": query_result.get("support_count"),
            },
            code_commit=code_commit,
            config_sha256=discovery_config.sha256,
            build_artifact=reuse_source_artifact,
        )
        if query_artifact.artifact_id != source.result_artifact_id:
            raise Phase1APipelineError("recovery QUERY resolved a different source artifact")
        observation = derive_phase1a_pattern_observation(
            campaign_key=CAMPAIGN_ID,
            query_run_fingerprint=query_spec.fingerprint,
            exposure_key=exposure_key,
            query_run_spec=json.loads(query_spec.canonical_json()),
            ai_run_spec=source.ai_canonical_spec,
            artifact_sha256=source.result_artifact_sha256,
            artifact_document=discovery_document,
            query_result=query_result,
        )
        services.record_pattern(database_url, observation)
        query_runs.append(query_run)

    final_prefix = services.verify_current_slice_prefix(
        database_url,
        campaign_key=CAMPAIGN_ID,
        slice_index=slice_index,
        source_interval_start=interval_start,
        source_interval_end=interval_end,
        requested_source_dates=source_dates,
        expected_feature_run_fingerprint=None,
        query_definition_sha256_by_id={
            query.query_id: canonical_sha256(query.as_dict())
            for query in discovery_config.candidate_queries
        },
    )
    if (
        not isinstance(final_prefix, Phase1ACurrentSlicePrefixReport)
        or len(final_prefix.query_exposure_ids) != len(query_results)
        or len(final_prefix.pattern_ids) != len(query_results)
        or final_prefix.missing_pattern_query_id is not None
    ):
        raise Phase1APipelineError("partial recovery did not produce a complete slice prefix")
    return Phase1ASliceReport(
        pipeline_version=PIPELINE_VERSION,
        slice_index=slice_index,
        source_dates=tuple(day.isoformat() for day in source_dates),
        built_source_dates=tuple(sorted(day.isoformat() for day in feature_hashes)),
        no_entry_reasons=tuple(
            (day.isoformat(), reason) for day, reason in sorted(no_entry_reasons.items())
        ),
        calendar_sha256=calendar.sha256,
        split_sha256=split.sha256,
        code_commit=code_commit,
        code_snapshot_sha256=code_snapshot_sha256,
        code_snapshot_disposition=code_snapshot_disposition,
        analysis_code_commit=analysis_code_commit,
        analysis_code_snapshot_sha256=analysis_code_snapshot_sha256,
        recovery_mode=True,
        recovery_run=recovery_run,
        recovery_manifest_path=recovery_manifest.path,
        recovery_manifest_sha256=recovery_manifest.sha256,
        campaign_id=int(campaign.campaign_id),
        campaign_key=campaign.campaign_key,
        feature_run=feature_run,
        ai_slice_run=ai_run,
        query_runs=tuple(query_runs),
        discovery_artifact_path=ai_path,
        discovery_artifact_sha256=artifact.sha256,
        eligible_row_count=eligible_rows,
        nonzero_support_query_count=nonzero_queries,
        pattern_observation_count=len(query_results),
    )


def _run_phase1a_discovery_slice(
    *,
    project_root: Path | str,
    data_root: Path | str,
    database_url: str,
    slice_index: int,
    services: PipelineServices,
) -> Phase1ASliceReport:
    if isinstance(slice_index, bool) or not isinstance(slice_index, int):
        raise Phase1APipelineError("slice_index must be an integer from 0 through 98")
    if not 0 <= slice_index <= MAX_DISCOVERY_SLICE_INDEX:
        raise Phase1APipelineError("slice_index must be between 0 and 98 inclusive")
    if not isinstance(database_url, str) or not database_url.strip():
        raise Phase1APipelineError("database_url must be a non-empty string")

    project = _strict_directory(project_root, label="project_root")
    data = _strict_directory(data_root, label="data_root", expected_name="data")
    _, _, manifests = _ensure_data_layout(data)
    footer_manifest_path = manifests / "mbp10_footer_manifest_v1.jsonl"
    source_manifest_path = manifests / "mbp10_source_sha256_v1.jsonl"
    qc_manifest_path = manifests / "mbp10_structural_qc_v1.jsonl"

    screening = load_conservative_screening_bundle(project)
    feature_config = load_phase1a_screening_config(
        project / "configs/features/phase1a_mbp10_screening_v1.toml"
    )
    discovery_config = load_discovery_slice_config(
        project / "configs/research/phase1a_discovery_slice_v1.toml"
    )
    hypotheses = load_hypothesis_bundle(
        project / "configs/research/phase1_parent_hypotheses_v1.toml"
    )
    qc_config = load_structural_qc_config(project / "configs/data/mbp10_structural_qc_v1.toml")
    frozen_inputs = _frozen_inputs(
        project,
        screening=screening,
        feature_config=feature_config,
        discovery_config=discovery_config,
        hypotheses=hypotheses,
        qc_config=qc_config,
    )

    calendar = services.build_calendar(source_manifest_path, qc_manifest_path)
    split = services.build_split(calendar)
    discovery_dates = tuple(split.discovery)
    expected_discovery_dates = DISCOVERY_SLICE_COUNT * DISCOVERY_SLICE_SIZE
    if len(discovery_dates) != expected_discovery_dates:
        raise Phase1APipelineError(
            f"Discovery calendar must contain exactly {expected_discovery_dates} source dates"
        )
    offset = slice_index * DISCOVERY_SLICE_SIZE
    source_dates = discovery_dates[offset : offset + DISCOVERY_SLICE_SIZE]
    if len(source_dates) != DISCOVERY_SLICE_SIZE:
        raise Phase1APipelineError("Discovery slice does not contain exactly five source dates")
    interval_start, interval_end = _slice_interval(source_dates)

    publication = services.publish_calendar_split(
        calendar,
        split,
        manifest_directory=manifests,
    )
    code_commit = services.git_head(project)
    snapshot = services.build_snapshot(project, code_commit=code_commit)
    published_snapshot = services.publish_snapshot(snapshot, data_root=data)
    if snapshot.sha256 != published_snapshot.sha256:
        raise Phase1APipelineError("published code snapshot identity drift")
    dependency_sha256 = services.dependency_hash(project)
    runtime = dict(services.runtime())
    runtime["postgresql"] = services.postgres_runtime(
        database_url,
        migrations_directory=project / "migrations",
    )
    runtime["phase1a_pipeline"] = {
        "pipeline_version": PIPELINE_VERSION,
        "slice_index": slice_index,
    }

    campaign = services.register_campaign(
        database_url,
        project_root=project,
        data_root=data,
        calendar=calendar,
        split=split,
        calendar_artifact_path=publication.calendar_path,
        split_artifact_path=publication.split_path,
        code_snapshot_artifact_path=published_snapshot.path,
        code_commit=code_commit,
        code_snapshot_sha256=snapshot.sha256,
        cost_input_manifest_sha256=screening.cost.sha256,
    )
    if campaign.campaign_key != CAMPAIGN_ID:
        raise Phase1APipelineError("registered campaign key drift")
    if slice_index > 0:
        prior_source_dates = discovery_dates[offset - DISCOVERY_SLICE_SIZE : offset]
        if len(prior_source_dates) != DISCOVERY_SLICE_SIZE:
            raise Phase1APipelineError(
                "previous Discovery slice does not contain exactly five source dates"
            )
        prior_interval_start, prior_interval_end = _slice_interval(prior_source_dates)
        services.verify_predecessor_slice(
            database_url,
            campaign_key=CAMPAIGN_ID,
            prior_slice_index=slice_index - 1,
            source_interval_start=prior_interval_start,
            source_interval_end=prior_interval_end,
            requested_source_dates=prior_source_dates,
            query_definition_sha256_by_id={
                query.query_id: canonical_sha256(query.as_dict())
                for query in discovery_config.candidate_queries
            },
        )

    query_definition_sha256_by_id = {
        query.query_id: canonical_sha256(query.as_dict())
        for query in discovery_config.candidate_queries
    }
    current_prefix = services.verify_current_slice_prefix(
        database_url,
        campaign_key=CAMPAIGN_ID,
        slice_index=slice_index,
        source_interval_start=interval_start,
        source_interval_end=interval_end,
        requested_source_dates=source_dates,
        expected_feature_run_fingerprint=None,
        query_definition_sha256_by_id=query_definition_sha256_by_id,
    )
    if not isinstance(current_prefix, Phase1ACurrentSlicePrefixReport):
        raise Phase1APipelineError("current-slice prefix verifier returned an invalid report")
    if current_prefix.state == "RESUMABLE":
        return _recover_phase1a_discovery_slice(
            database_url=database_url,
            data_root=data,
            slice_index=slice_index,
            source_dates=source_dates,
            interval_start=interval_start,
            interval_end=interval_end,
            prefix=current_prefix,
            services=services,
            campaign=campaign,
            calendar=calendar,
            split=split,
            discovery_config=discovery_config,
            code_commit=code_commit,
            code_snapshot_sha256=snapshot.sha256,
            code_snapshot_disposition=published_snapshot.disposition,
            dependency_sha256=dependency_sha256,
            runtime=runtime,
        )
    if current_prefix.state not in {"EMPTY", "FAILED_FEATURE_RETRYABLE"}:
        raise Phase1APipelineError(
            f"current-slice prefix state is not recoverable: {current_prefix.state}"
        )

    source_bundle = services.load_source_bundle(
        footer_manifest_path,
        source_manifest_path,
    )
    if Path(source_bundle.footer_manifest_path).resolve(
        strict=True
    ) != footer_manifest_path.resolve(strict=True) or Path(
        source_bundle.hash_manifest_path
    ).resolve(strict=True) != source_manifest_path.resolve(strict=True):
        raise Phase1APipelineError("source bundle resolved different manifest paths")
    if source_bundle.hash_manifest_sha256 != calendar.source_manifest_sha256:
        raise Phase1APipelineError("source bundle and calendar manifest identities drift")
    footer_manifest_sha256 = source_bundle.footer_manifest_sha256
    plans = _plan_entries(
        data_root=data,
        requested_dates=source_dates,
        records=source_bundle.records,
        qualified_dates=frozenset(calendar.source_dates),
        select_contract=services.select_contract,
        plan_no_entry_reason=services.plan_no_entry_reason,
        calendar=calendar,
        config_path=feature_config.path,
    )
    no_entry_reasons = {
        plan.source.source_date: plan.no_entry_reason
        for plan in plans
        if plan.no_entry_reason is not None
    }
    feature_spec = _make_run_spec(
        run_kind="FEATURE_BUILD",
        engine_version=FEATURE_RUN_ENGINE_VERSION,
        calendar=calendar,
        split=split,
        screening=screening,
        feature_config=feature_config,
        footer_manifest_sha256=footer_manifest_sha256,
        code_commit=code_commit,
        code_snapshot_sha256=snapshot.sha256,
        dependency_sha256=dependency_sha256,
        runtime=runtime,
        frozen_inputs=frozen_inputs,
        parameters=_feature_batch_parameters(
            plans,
            slice_index=slice_index,
            config_sha256=feature_config.sha256,
            frozen_inputs=frozen_inputs,
        ),
    )
    services.verify_current_slice_prefix(
        database_url,
        campaign_key=CAMPAIGN_ID,
        slice_index=slice_index,
        source_interval_start=interval_start,
        source_interval_end=interval_end,
        requested_source_dates=source_dates,
        expected_feature_run_fingerprint=feature_spec.fingerprint,
        query_definition_sha256_by_id=query_definition_sha256_by_id,
    )

    def execute_feature_build() -> ResolvedRunArtifact:
        entries = _feature_entries(
            plans,
            services=services,
            data_root=data,
            calendar=calendar,
            code_snapshot_sha256=snapshot.sha256,
            config_path=feature_config.path,
        )
        registration = services.register_feature_batch(
            database_url,
            data_root=data,
            calendar=calendar,
            run_spec=feature_spec,
            entries=entries,
        )
        artifact = ResolvedRunArtifact(
            artifact_id=_required_artifact_id(
                registration.manifest_artifact_id,
                label="feature batch registration",
            ),
            path=Path(registration.manifest_path),
            sha256=str(registration.manifest_sha256),
            artifact_type="PHASE1A_FEATURE_BUILD_MANIFEST",
        )
        _feature_inputs_from_manifest(
            artifact,
            data_root=data,
            run_fingerprint=feature_spec.fingerprint,
            plans=plans,
        )
        return artifact

    feature_run, feature_artifact = _reserve_run(
        services=services,
        database_url=database_url,
        data_root=data,
        run_spec=feature_spec,
        parent_run_fingerprint=None,
        slice_index=slice_index,
        execute=execute_feature_build,
    )
    if feature_artifact.artifact_type != "PHASE1A_FEATURE_BUILD_MANIFEST":
        raise Phase1APipelineError("feature run resolved a non-feature-manifest artifact")
    feature_paths, feature_hashes = _feature_inputs_from_manifest(
        feature_artifact,
        data_root=data,
        run_fingerprint=feature_spec.fingerprint,
        plans=plans,
    )
    feature_input_identities = {
        day.isoformat(): {
            "relative_path": _relative_to_data(path, data_root=data),
            "sha256": feature_hashes[day],
        }
        for day, path in sorted(feature_paths.items())
    }

    ai_parameters = {
        "analysis_authority": "OPEN_OBSERVATION",
        "candidate_queries": [query.as_dict() for query in discovery_config.candidate_queries],
        "candidate_query_definition_sha256": discovery_config.definition_sha256,
        "feature_inputs_by_date": feature_input_identities,
        "feature_manifest_relative_path": _relative_to_data(
            feature_artifact.path,
            data_root=data,
        ),
        "feature_manifest_sha256": feature_artifact.sha256,
        "frozen_toml_inputs": frozen_inputs,
        "no_entry_reason_by_date": {
            day.isoformat(): reason for day, reason in sorted(no_entry_reasons.items())
        },
        "parent_run_fingerprint": feature_spec.fingerprint,
        "pipeline_version": PIPELINE_VERSION,
        "requested_source_dates": [day.isoformat() for day in source_dates],
        "research_eligible": False,
        "screening_only": True,
        "slice_index": slice_index,
    }
    ai_spec = _make_run_spec(
        run_kind="AI_SLICE",
        engine_version=DISCOVERY_RUN_ENGINE_VERSION,
        calendar=calendar,
        split=split,
        screening=screening,
        feature_config=feature_config,
        footer_manifest_sha256=footer_manifest_sha256,
        code_commit=code_commit,
        code_snapshot_sha256=snapshot.sha256,
        dependency_sha256=dependency_sha256,
        runtime=runtime,
        frozen_inputs=frozen_inputs,
        parameters=ai_parameters,
    )
    ai_exposure_key = f"{CAMPAIGN_ID}:ai-slice:{slice_index:02d}"
    ai_query_spec = {
        "candidate_queries": ai_parameters["candidate_queries"],
        "definition_sha256": discovery_config.definition_sha256,
        "run_fingerprint": ai_spec.fingerprint,
    }
    ai_exposure_summary: dict[str, object] = {
        "candidate_query_count": len(discovery_config.candidate_queries),
        "feature_manifest_sha256": feature_artifact.sha256,
        "requested_source_dates": [day.isoformat() for day in source_dates],
        "screening_only": True,
    }

    def build_ai_artifact() -> tuple[Path, str]:
        report = services.analyze_slice(
            feature_paths,
            expected_sha256_by_date=feature_hashes,
            requested_source_dates=source_dates,
            no_entry_reasons=no_entry_reasons,
            data_root=data,
            code_snapshot_sha256=snapshot.sha256,
            run_fingerprint=ai_spec.fingerprint,
            config_path=discovery_config.path,
        )
        candidate = ResolvedRunArtifact(
            artifact_id=0,
            path=Path(report.path),
            sha256=str(report.sha256),
            artifact_type="DISCOVERY_EXPOSURE_RESULT",
        )
        _validate_discovery_document(
            candidate,
            data_root=data,
            run_fingerprint=ai_spec.fingerprint,
            source_dates=source_dates,
            discovery_config=discovery_config,
            feature_sha256_by_date=feature_hashes,
            no_entry_reasons=no_entry_reasons,
        )
        return candidate.path, candidate.sha256

    ai_run, ai_artifact = _reserve_discovery_run(
        services=services,
        database_url=database_url,
        data_root=data,
        run_spec=ai_spec,
        parent_run_fingerprint=feature_spec.fingerprint,
        slice_index=slice_index,
        campaign_key=CAMPAIGN_ID,
        exposure_key=ai_exposure_key,
        exposure_type="AI_SLICE",
        source_interval_start=interval_start,
        source_interval_end=interval_end,
        query_spec=ai_query_spec,
        exposure_result_summary=ai_exposure_summary,
        code_commit=code_commit,
        config_sha256=discovery_config.sha256,
        build_artifact=build_ai_artifact,
    )
    if ai_artifact.artifact_type != "DISCOVERY_EXPOSURE_RESULT":
        raise Phase1APipelineError("AI slice resolved an unexpected artifact type")
    ai_path, discovery_document = _validate_discovery_document(
        ai_artifact,
        data_root=data,
        run_fingerprint=ai_spec.fingerprint,
        source_dates=source_dates,
        discovery_config=discovery_config,
        feature_sha256_by_date=feature_hashes,
        no_entry_reasons=no_entry_reasons,
    )

    query_results = discovery_document["query_results"]
    assert isinstance(query_results, list)
    query_runs: list[PipelineRunReport] = []
    pattern_count = 0
    for query_result in query_results:
        assert isinstance(query_result, dict)
        definition = query_result["definition"]
        assert isinstance(definition, dict)
        query_id = definition["id"]
        assert isinstance(query_id, str)
        definition_sha256 = canonical_sha256(definition)
        query_parameters = {
            "candidate_query": definition,
            "discovery_artifact_relative_path": _relative_to_data(ai_path, data_root=data),
            "discovery_artifact_sha256": ai_artifact.sha256,
            "frozen_toml_inputs": frozen_inputs,
            "parent_run_fingerprint": ai_spec.fingerprint,
            "pipeline_version": PIPELINE_VERSION,
            "query_definition_sha256": definition_sha256,
            "query_result_sha256": canonical_sha256(query_result),
            "requested_source_dates": [day.isoformat() for day in source_dates],
            "research_eligible": False,
            "screening_only": True,
            "slice_index": slice_index,
        }
        query_spec = _make_run_spec(
            run_kind="QUERY",
            engine_version=QUERY_RUN_ENGINE_VERSION,
            calendar=calendar,
            split=split,
            screening=screening,
            feature_config=feature_config,
            footer_manifest_sha256=footer_manifest_sha256,
            code_commit=code_commit,
            code_snapshot_sha256=snapshot.sha256,
            dependency_sha256=dependency_sha256,
            runtime=runtime,
            frozen_inputs=frozen_inputs,
            parameters=query_parameters,
        )
        query_exposure_key = f"{CAMPAIGN_ID}:query:{slice_index:02d}:{query_id}"
        query_exposure_spec = {
            "candidate_query": definition,
            "query_definition_sha256": definition_sha256,
            "run_fingerprint": query_spec.fingerprint,
        }
        query_exposure_summary = {
            "artifact_sha256": ai_artifact.sha256,
            "direction_counts": query_result.get("direction_counts"),
            "source_date_count": query_result.get("source_date_count"),
            "support_count": query_result.get("support_count"),
        }

        def reuse_ai_artifact() -> tuple[Path, str]:
            return ai_path, ai_artifact.sha256

        query_run, query_artifact = _reserve_discovery_run(
            services=services,
            database_url=database_url,
            data_root=data,
            run_spec=query_spec,
            parent_run_fingerprint=ai_spec.fingerprint,
            slice_index=slice_index,
            campaign_key=CAMPAIGN_ID,
            exposure_key=query_exposure_key,
            exposure_type="QUERY",
            source_interval_start=interval_start,
            source_interval_end=interval_end,
            query_spec=query_exposure_spec,
            exposure_result_summary=query_exposure_summary,
            code_commit=code_commit,
            config_sha256=discovery_config.sha256,
            build_artifact=reuse_ai_artifact,
        )
        if query_artifact.sha256 != ai_artifact.sha256:
            raise Phase1APipelineError("QUERY run resolved a different Discovery artifact")
        observation = _pattern_observation(
            query_result=query_result,
            query_run_fingerprint=query_spec.fingerprint,
            exposure_key=query_exposure_key,
            discovery_artifact_sha256=ai_artifact.sha256,
            discovery_document=discovery_document,
            feature_manifest_sha256=feature_artifact.sha256,
            footer_manifest_sha256=footer_manifest_sha256,
            calendar=calendar,
            screening=screening,
            feature_config=feature_config,
            discovery_config=discovery_config,
            hypotheses=hypotheses,
            code_snapshot_sha256=snapshot.sha256,
        )
        services.record_pattern(database_url, observation)
        pattern_count += 1
        query_runs.append(query_run)

    summary = discovery_document["summary"]
    assert isinstance(summary, dict)
    eligible_rows = summary.get("eligible_rows")
    nonzero_queries = summary.get("nonzero_support_query_count")
    if (
        isinstance(eligible_rows, bool)
        or not isinstance(eligible_rows, int)
        or isinstance(nonzero_queries, bool)
        or not isinstance(nonzero_queries, int)
    ):
        raise Phase1APipelineError("Discovery summary counts are invalid")
    return Phase1ASliceReport(
        pipeline_version=PIPELINE_VERSION,
        slice_index=slice_index,
        source_dates=tuple(day.isoformat() for day in source_dates),
        built_source_dates=tuple(
            plan.source.source_date.isoformat()
            for plan in plans
            if plan.status is BatchEntryStatus.BUILT
        ),
        no_entry_reasons=tuple(
            (day.isoformat(), reason) for day, reason in sorted(no_entry_reasons.items())
        ),
        calendar_sha256=calendar.sha256,
        split_sha256=split.sha256,
        code_commit=code_commit,
        code_snapshot_sha256=snapshot.sha256,
        code_snapshot_disposition=published_snapshot.disposition,
        analysis_code_commit=code_commit,
        analysis_code_snapshot_sha256=snapshot.sha256,
        recovery_mode=False,
        recovery_run=None,
        recovery_manifest_path=None,
        recovery_manifest_sha256=None,
        campaign_id=int(campaign.campaign_id),
        campaign_key=campaign.campaign_key,
        feature_run=feature_run,
        ai_slice_run=ai_run,
        query_runs=tuple(query_runs),
        discovery_artifact_path=ai_path,
        discovery_artifact_sha256=ai_artifact.sha256,
        eligible_row_count=eligible_rows,
        nonzero_support_query_count=nonzero_queries,
        pattern_observation_count=pattern_count,
    )


def run_phase1a_discovery_slice(
    *,
    project_root: Path | str,
    data_root: Path | str,
    database_url: str,
    slice_index: int = 0,
    services: PipelineServices = DEFAULT_SERVICES,
) -> Phase1ASliceReport:
    """Execute or exactly resume one governed five-date Discovery slice."""

    try:
        return _run_phase1a_discovery_slice(
            project_root=project_root,
            data_root=data_root,
            database_url=database_url,
            slice_index=slice_index,
            services=services,
        )
    except Phase1APipelineError:
        raise
    except Exception as exc:
        raise Phase1APipelineError(
            f"Phase1A pipeline failed before completion ({type(exc).__name__})"
        ) from exc
