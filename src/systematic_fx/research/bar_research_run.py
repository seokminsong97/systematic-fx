"""Governed orchestration for the frozen bar-pattern Discovery campaign.

This module deliberately separates the outcome-free plan from execution.  A
``LoadedBarDatasetManifest`` has already passed the manifest loader's complete
content/lineage checks.  The orchestrator then freezes provenance, registers
all 216 candidate RunSpecs, reserves and starts every executable attempt, and
only then permits the single streaming Discovery pass to observe outcomes.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import tempfile
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, BinaryIO, Final, Literal

import psycopg
from psycopg.rows import dict_row

from systematic_fx.backtest.barriers import BARRIER_TICKS
from systematic_fx.backtest.economics import (
    BASE_MONTHLY_FIXED_POOL_USD,
    EXPECTED_MONTHLY_ROUND_TRIPS,
    TICK_VALUE_USD,
)
from systematic_fx.db.bar_registry import (
    BAR_CATALOG_EXPERIMENT_KEY,
    BAR_COST_VERSION,
    BAR_DATASET_MANIFEST_KEY,
    BAR_EXECUTION_VERSION,
    BAR_FEATURE_VERSION,
    BAR_OUTCOME_VERSION,
    RAW_SOURCE_MANIFEST_KEY,
    BarTerminalResult,
    abort_bar_run_attempt,
    candidate_trial_parameters,
    publish_bar_registration_artifact,
    publish_bar_terminal_result_artifact,
    register_bar_campaign,
    register_bar_run_spec,
    register_published_bar_artifact,
    register_terminal_bar_result,
    validate_completed_bar_campaign,
    validate_reused_bar_attempts,
)
from systematic_fx.db.migrations import discover_migrations
from systematic_fx.db.run_registry import reserve_run_attempt, start_run_attempt
from systematic_fx.research import bar_discovery as bar_discovery_module
from systematic_fx.research.bar_artifacts import (
    BarArtifactDescriptor,
    PublishedBarArtifact,
    publish_bar_artifact_bytes,
    publish_bar_artifact_open_file,
    verify_published_bar_artifact,
)
from systematic_fx.research.bar_config import (
    ALLOCATED_VARIANT_COUNT,
    BAR_PATTERN_CAMPAIGN_KEY,
    BAR_PATTERN_CONFIG_RELATIVE_PATH,
    BAR_PATTERN_QUALIFICATION_STATUS,
    BAR_PATTERN_SCREENING_ONLY,
    BAR_SOURCE_MANIFEST_SHA256,
    BarPatternCandidate,
    BarPatternResearchConfig,
    load_bar_pattern_config,
)
from systematic_fx.research.bar_discovery import (
    DISCOVERY_EVIDENCE_SCHEMA,
    DISCOVERY_RESULT_SCHEMA,
    DISCOVERY_SPOOL_VERSION,
    BarDiscoveryProgress,
    BarDiscoveryResult,
    run_streaming_bar_pattern_discovery,
)
from systematic_fx.research.bar_pipeline import (
    BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
    LoadedBarDatasetManifest,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.provenance import (
    CODE_SNAPSHOT_SCHEMA,
    CodeSnapshot,
    build_code_snapshot,
    dependency_lock_sha256,
    runtime_environment,
)
from systematic_fx.research.run_spec import RunSpec
from systematic_fx.validation.bar_splits import BarSplitPlan, plan_bar_splits

BAR_RESEARCH_RUN_SCHEMA: Final = "systematic_fx.bar_pattern_research_run.v1"
BAR_DISCOVERY_LINEAGE_SCHEMA: Final = "systematic_fx.bar_discovery_lineage.v1"
BAR_CODE_SNAPSHOT_ARTIFACT_SCHEMA_SHA256: Final = canonical_sha256(
    {
        "artifact_schema": CODE_SNAPSHOT_SCHEMA,
        "content": "reconstructible_source_config_migration_bytes",
        "required_fields": ["artifact_schema", "code_commit", "file_count", "files"],
    }
)
BAR_GLOBAL_DISCOVERY_RESULT_SCHEMA_SHA256: Final = canonical_sha256(
    {
        "artifact_schema": DISCOVERY_RESULT_SCHEMA,
        "canonicalization": "ASCII_ESCAPED_SORTED_KEYS_COMPACT_JSON_NEWLINE",
        "record_count": ALLOCATED_VARIANT_COUNT,
        "required_fields": [
            "candidate_catalog_sha256",
            "candidate_results",
            "config_semantic_sha256",
            "dataset_build_sha256",
            "evidence_manifest",
            "outcome_span_policy_sha256",
            "source_identity_sha256",
            "split_plan_sha256",
        ],
    }
)
BAR_DATASET_KEY: Final = "glbx_mdp3_mbp_10_6e_fut_v1"
BAR_DISCOVERY_ENGINE_VERSION: Final = "bar_pattern_streaming_discovery_v1"
BAR_ELIGIBLE_CALENDAR_VERSION: Final = "bar_dataset_eligible_calendar_v1"
BAR_SPLIT_VERSION: Final = "bar_pattern_splits_v1"
BAR_RANDOM_SEED: Final = 0
BAR_EVIDENCE_MATCH_SHARD_MAX_RECORDS: Final = 4_096
BAR_EVIDENCE_REPLAY_SHARD_MAX_RECORDS: Final = 256
SUPPORTED_MIGRATIONS: Final = tuple(range(1, 27))

# These identities describe the one approved dataset handoff.  Config hashes
# are intentionally not duplicated here: the authoritative loader constants
# and the exact config bytes inside the code snapshot are checked instead.
EXPECTED_DATASET_MANIFEST_SHA256: Final = (
    "e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc"
)
EXPECTED_DATASET_HANDOFF_SHA256: Final = (
    "26b1bb96f7323cae13bbe5d670c12f3e85615bbb9aab56932ce6523e67af7b00"
)
EXPECTED_SPLIT_PLAN_SHA256: Final = (
    "5594725f6769a706018d414a5b27e3903f1d7d1cc22c98e93b6e973ead1af043"
)

_GIT_OBJECT_ID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")
_SHA256 = re.compile(r"[0-9a-f]{64}")

RunMode = Literal["PLAN_ONLY", "RUN"]


class BarResearchRunError(RuntimeError):
    """A governed Discovery run could not preserve its frozen contract."""


@dataclass(frozen=True, slots=True)
class PreparedBarResearchRun:
    """Outcome-free, immutable plan derived from the verified dataset handoff."""

    project_root: Path
    data_root: Path
    dataset: LoadedBarDatasetManifest
    config: BarPatternResearchConfig
    split_plan: BarSplitPlan
    dataset_handoff_sha256: str

    @property
    def candidate_keys(self) -> tuple[str, ...]:
        return tuple(item.candidate_key for item in self.config.candidates)

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_catalog_sha256": self.config.candidate_catalog_sha256,
            "candidate_count": len(self.config.candidates),
            "campaign_definition_sha256": self.config.definition_sha256,
            "config_file_sha256": self.config.sha256,
            "config_semantic_sha256": self.config.semantic_sha256,
            "dataset_handoff_sha256": self.dataset_handoff_sha256,
            "dataset_manifest_sha256": self.dataset.dataset_manifest_sha256,
            "outcome_span_policy_sha256": self.dataset.outcome_span_policy_sha256,
            "raw_source_manifest_sha256": self.dataset.source_manifest_sha256,
            "schema": BAR_RESEARCH_RUN_SCHEMA,
            "split_plan_sha256": self.split_plan.sha256,
        }


@dataclass(frozen=True, slots=True)
class BarRunProvenance:
    """Exact source, dependency, runtime, and database identity for execution."""

    code_commit: str
    snapshot: CodeSnapshot
    snapshot_artifact: PublishedBarArtifact
    dependency_lock_sha256: str
    runtime_environment: Mapping[str, object]
    postgres_migrations_sha256: str


@dataclass(frozen=True, slots=True)
class BarCandidateRunReport:
    candidate_key: str
    run_fingerprint: str
    research_run_attempt_id: int | None
    disposition: str
    final_label: str | None = None
    trial_status: str | None = None
    terminal_artifact_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class BarResearchRunReport:
    mode: RunMode
    disposition: str
    plan: PreparedBarResearchRun
    candidate_runs: tuple[BarCandidateRunReport, ...] = ()
    discovery_result_sha256: str | None = None
    global_result_artifact_identity_sha256: str | None = None
    evidence_manifest_sha256: str | None = None
    finalist_keys: tuple[str, ...] = ()
    final_label_counts: tuple[tuple[str, int], ...] = ()
    evidence_artifact_count: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_runs": [
                {
                    "candidate_key": item.candidate_key,
                    "disposition": item.disposition,
                    "final_label": item.final_label,
                    "research_run_attempt_id": item.research_run_attempt_id,
                    "run_fingerprint": item.run_fingerprint,
                    "terminal_artifact_sha256": item.terminal_artifact_sha256,
                    "trial_status": item.trial_status,
                }
                for item in self.candidate_runs
            ],
            "discovery_result_sha256": self.discovery_result_sha256,
            "disposition": self.disposition,
            "evidence_artifact_count": self.evidence_artifact_count,
            "evidence_manifest_sha256": self.evidence_manifest_sha256,
            "final_label_counts": dict(self.final_label_counts),
            "finalist_keys": list(self.finalist_keys),
            "global_result_artifact_identity_sha256": (self.global_result_artifact_identity_sha256),
            "mode": self.mode,
            "plan": self.plan.as_dict(),
            "schema": BAR_RESEARCH_RUN_SCHEMA,
        }


@dataclass(frozen=True, slots=True)
class BarResearchRunProgress:
    stage: str
    completed: int
    total: int
    candidate_key: str | None = None
    discovery: BarDiscoveryProgress | None = None


@dataclass(frozen=True, slots=True)
class _AttemptCleanupIdentity:
    research_run_attempt_id: int
    run_fingerprint: str


@dataclass(frozen=True, slots=True)
class BarResearchRunServices:
    """Side-effect boundary used by production and no-database unit tests."""

    load_config: Callable[..., BarPatternResearchConfig]
    plan_splits: Callable[[Sequence[Any]], BarSplitPlan]
    git_head: Callable[[Path], str]
    build_snapshot: Callable[..., CodeSnapshot]
    publish_snapshot: Callable[..., PublishedBarArtifact]
    dependency_hash: Callable[[Path], str]
    runtime: Callable[[], dict[str, object]]
    postgres_runtime: Callable[..., dict[str, object]]
    verify_artifact: Callable[[Path, PublishedBarArtifact], None]
    publish_registration: Callable[..., PublishedBarArtifact]
    register_campaign: Callable[..., Any]
    register_artifact: Callable[..., Any]
    register_spec: Callable[..., Any]
    reserve_attempt: Callable[..., Any]
    start_attempt: Callable[..., Any]
    validate_reused_attempts: Callable[..., Any]
    validate_completed_campaign: Callable[..., Any]
    run_discovery: Callable[..., Any]
    validate_discovery: Callable[..., None]
    publish_global_result: Callable[..., PublishedBarArtifact]
    publish_terminal: Callable[..., PublishedBarArtifact]
    register_terminal: Callable[..., Any]
    fail_attempt: Callable[..., Any]


def _strict_project_root(value: Path | str) -> tuple[Path, Path]:
    requested = Path(value).expanduser()
    if requested.is_symlink():
        raise BarResearchRunError("project_root cannot be a symbolic link")
    try:
        root = requested.resolve(strict=True)
    except OSError as error:
        raise BarResearchRunError("project_root does not exist") from error
    if not root.is_dir():
        raise BarResearchRunError("project_root must be a directory")
    data = root / "data"
    if data.is_symlink() or not data.is_dir():
        raise BarResearchRunError("project data/ must be an existing non-symlink directory")
    return root, data.resolve(strict=True)


def _git_head(project_root: Path) -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--verify", "HEAD"],
            cwd=project_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise BarResearchRunError("cannot resolve Git HEAD") from error
    value = completed.stdout.strip()
    if _GIT_OBJECT_ID.fullmatch(value) is None:
        raise BarResearchRunError("Git HEAD is not a full lowercase object ID")
    return value


def _postgres_runtime(database_url: str, *, migrations_directory: Path) -> dict[str, object]:
    migrations = discover_migrations(migrations_directory)
    if tuple(item.version for item in migrations) != SUPPORTED_MIGRATIONS:
        raise BarResearchRunError("bar Discovery requires migrations 0001 through 0026 exactly")
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        version = connection.execute(
            "SELECT current_setting('server_version') AS version, "
            "current_setting('server_version_num') AS version_num"
        ).fetchone()
        rows = connection.execute(
            "SELECT version, name, checksum FROM systematic_fx.schema_migrations ORDER BY version"
        ).fetchall()
    expected = [
        {"checksum": item.checksum, "name": item.name, "version": item.version}
        for item in migrations
    ]
    observed = [
        {
            "checksum": str(item["checksum"]),
            "name": str(item["name"]),
            "version": int(item["version"]),
        }
        for item in rows
    ]
    if version is None or observed != expected:
        raise BarResearchRunError("PostgreSQL migration identity drift")
    return {
        "schema_migrations": observed,
        "schema_migrations_sha256": canonical_sha256(observed),
        "server_version": str(version["version"]),
        "server_version_num": str(version["version_num"]),
    }


def bar_code_snapshot_artifact_descriptor(
    snapshot: CodeSnapshot,
    dataset: LoadedBarDatasetManifest,
) -> BarArtifactDescriptor:
    """Bind reconstructible code bytes to the exact bar dataset handoff."""

    if not isinstance(snapshot, CodeSnapshot):
        raise BarResearchRunError("snapshot must be a CodeSnapshot")
    if not isinstance(dataset, LoadedBarDatasetManifest):
        raise BarResearchRunError("dataset must be a LoadedBarDatasetManifest")
    return BarArtifactDescriptor(
        artifact_key=f"{BAR_PATTERN_CAMPAIGN_KEY}:code_snapshot:{snapshot.sha256}",
        artifact_type="bar_code_snapshot",
        artifact_schema=CODE_SNAPSHOT_SCHEMA,
        artifact_version=2,
        record_count=len(snapshot.files),
        schema_sha256=BAR_CODE_SNAPSHOT_ARTIFACT_SCHEMA_SHA256,
        source_manifest_sha256=dataset.dataset_manifest_sha256,
        logical_identity={
            "code_commit": snapshot.code_commit,
            "code_snapshot_sha256": snapshot.sha256,
            "dataset_handoff_sha256": dataset.handoff_sha256,
            "dataset_manifest_sha256": dataset.dataset_manifest_sha256,
            "outcome_span_policy_sha256": dataset.outcome_span_policy_sha256,
            "raw_source_manifest_sha256": dataset.source_manifest_sha256,
            "supported_migrations": list(SUPPORTED_MIGRATIONS),
        },
        media_type="application/json",
        file_suffix=".json",
    )


def publish_bar_code_snapshot(
    project_root: Path,
    snapshot: CodeSnapshot,
    *,
    dataset: LoadedBarDatasetManifest,
) -> PublishedBarArtifact:
    """Publish code bytes through the held-dirfd immutable artifact primitive."""

    if hashlib.sha256(snapshot.canonical_bytes).hexdigest() != snapshot.sha256:
        raise BarResearchRunError("code snapshot content identity drift")
    descriptor = bar_code_snapshot_artifact_descriptor(snapshot, dataset)
    artifact = publish_bar_artifact_bytes(project_root, descriptor, snapshot.canonical_bytes)
    if artifact.sha256 != snapshot.sha256:
        raise BarResearchRunError("published code snapshot identity drift")
    return artifact


def _global_result_scalar_values(result: BarDiscoveryResult) -> dict[str, object]:
    """Return every canonical root value except the two potentially huge arrays."""

    return {
        "artifact_schema": DISCOVERY_RESULT_SCHEMA,
        "budget_rejected_keys": list(result.budget_rejected_keys),
        "candidate_catalog_sha256": result.candidate_catalog_sha256,
        "candidate_count": len(result.candidate_results),
        "config_semantic_sha256": result.config_semantic_sha256,
        "dataset_build_sha256": result.dataset_build_sha256,
        "decision_dates": [item.isoformat() for item in result.decision_dates],
        "evaluated_count": result.evaluated_count,
        "evidence_manifest": (
            None if result.evidence_manifest is None else result.evidence_manifest.as_dict()
        ),
        "loaded_bar_counts": [
            {"row_count": count, "timeframe_seconds": timeframe}
            for timeframe, count in result.loaded_bar_counts
        ],
        "loaded_source_dates": [item.isoformat() for item in result.loaded_source_dates],
        "matched_signal_count": result.matched_signal_count,
        "outcome_span_policy_sha256": result.outcome_span_policy_sha256,
        "replay_catalog_count": len(result.replay_catalog),
        "ranked_finalist_keys": list(result.ranked_finalist_keys),
        "source_identity_sha256": result.source_identity_sha256,
        "split_plan_sha256": result.split_plan_sha256,
    }


def _discovery_json_fragment(value: object) -> bytes:
    """Encode one fragment exactly as ``bar_discovery._canonical_bytes`` does."""

    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError) as error:
        raise BarResearchRunError("global Discovery result is not strict canonical JSON") from error


def _stage_global_discovery_result(
    result: BarDiscoveryResult,
    *,
    data_root: Path,
) -> tuple[BinaryIO, str, int]:
    """Stream the exact canonical preimage into one caller-held descriptor.

    Production streaming results intentionally have an empty replay catalog;
    its compact replay evidence lives in the separately authenticated Parquet
    spool.  Candidate dictionaries are materialized one at a time so the
    global seal does not duplicate all 216 surfaces in memory.  The returned
    file remains open so publication never needs to trust or reopen a pathname.
    """

    if result.replay_catalog:
        raise BarResearchRunError("production global result requires an empty replay catalog")
    values = _global_result_scalar_values(result)
    keys = tuple(sorted((*values, "candidate_results", "replay_catalog")))
    # Ownership intentionally transfers to the caller/publisher.
    staged = tempfile.TemporaryFile(  # noqa: SIM115
        dir=data_root,
        prefix=".bar-global-result-",
        suffix=".json",
    )
    digest = hashlib.sha256()
    byte_size = 0

    def write(content: bytes) -> None:
        nonlocal byte_size
        staged.write(content)
        digest.update(content)
        byte_size += len(content)

    try:
        write(b"{")
        for key_index, key in enumerate(keys):
            if key_index:
                write(b",")
            write(_discovery_json_fragment(key))
            write(b":")
            if key == "candidate_results":
                write(b"[")
                for candidate_index, candidate in enumerate(result.candidate_results):
                    if candidate_index:
                        write(b",")
                    write(_discovery_json_fragment(candidate.as_dict()))
                write(b"]")
            elif key == "replay_catalog":
                write(b"[]")
            else:
                write(_discovery_json_fragment(values[key]))
        # Discovery's canonical preimage is one compact JSON value followed by
        # exactly one LF.  This byte is part of ``BarDiscoveryResult.sha256``.
        write(b"}\n")
        staged.flush()
        os.fsync(staged.fileno())
        return staged, digest.hexdigest(), byte_size
    except BaseException:
        staged.close()
        raise


def publish_global_bar_discovery_result(
    project_root: Path,
    result: BarDiscoveryResult,
    *,
    prepared: PreparedBarResearchRun,
) -> PublishedBarArtifact:
    """Publish the exact global Discovery preimage without a second huge buffer."""

    if not isinstance(result, BarDiscoveryResult):
        raise BarResearchRunError("global result must be a BarDiscoveryResult")
    evidence = result.evidence_manifest
    if evidence is None:
        raise BarResearchRunError("global result requires an evidence manifest")
    observed_candidate_keys = tuple(
        item.candidate.candidate_key for item in result.candidate_results
    )
    if observed_candidate_keys != prepared.candidate_keys:
        raise BarResearchRunError("global result requires the exact ordered 216-candidate catalog")
    if (
        result.source_identity_sha256 != prepared.dataset.dataset_manifest_sha256
        or result.dataset_build_sha256 != prepared.dataset.dataset_manifest_sha256
        or result.outcome_span_policy_sha256 != prepared.dataset.outcome_span_policy_sha256
        or result.config_semantic_sha256 != prepared.config.semantic_sha256
        or result.candidate_catalog_sha256 != prepared.config.candidate_catalog_sha256
        or result.split_plan_sha256 != prepared.split_plan.sha256
    ):
        raise BarResearchRunError("global result lineage differs from the prepared plan")
    staged, content_sha256, byte_size = _stage_global_discovery_result(
        result,
        data_root=prepared.data_root,
    )
    descriptor = BarArtifactDescriptor(
        artifact_key=(f"{BAR_PATTERN_CAMPAIGN_KEY}:global_discovery_result:{content_sha256}"),
        artifact_type="bar_global_discovery_result",
        artifact_schema=DISCOVERY_RESULT_SCHEMA,
        artifact_version=1,
        record_count=len(result.candidate_results),
        schema_sha256=BAR_GLOBAL_DISCOVERY_RESULT_SCHEMA_SHA256,
        source_manifest_sha256=prepared.dataset.dataset_manifest_sha256,
        logical_identity={
            "candidate_catalog_sha256": prepared.config.candidate_catalog_sha256,
            "config_semantic_sha256": prepared.config.semantic_sha256,
            "dataset_handoff_sha256": prepared.dataset_handoff_sha256,
            "dataset_manifest_sha256": prepared.dataset.dataset_manifest_sha256,
            "discovery_result_sha256": content_sha256,
            "evidence_artifact_identity_sha256": evidence.artifact.descriptor.identity_sha256,
            "evidence_identity_sha256": evidence.evidence_identity_sha256,
            "evidence_manifest_sha256": evidence.sha256,
            "outcome_span_policy_sha256": prepared.dataset.outcome_span_policy_sha256,
            "raw_source_manifest_sha256": prepared.dataset.source_manifest_sha256,
            "split_plan_sha256": prepared.split_plan.sha256,
        },
        media_type="application/json",
        file_suffix=".json",
    )
    try:
        artifact = publish_bar_artifact_open_file(
            project_root,
            descriptor,
            staged,
        )
    finally:
        staged.close()
    if artifact.sha256 != content_sha256 or artifact.byte_size != byte_size:
        raise BarResearchRunError("published global Discovery preimage identity drift")
    return artifact


def _verify_artifact(project_root: Path, artifact: PublishedBarArtifact) -> None:
    verify_published_bar_artifact(project_root, artifact)


def _default_services() -> BarResearchRunServices:
    return BarResearchRunServices(
        load_config=load_bar_pattern_config,
        plan_splits=plan_bar_splits,
        git_head=_git_head,
        build_snapshot=build_code_snapshot,
        publish_snapshot=publish_bar_code_snapshot,
        dependency_hash=dependency_lock_sha256,
        runtime=runtime_environment,
        postgres_runtime=_postgres_runtime,
        verify_artifact=_verify_artifact,
        publish_registration=publish_bar_registration_artifact,
        register_campaign=register_bar_campaign,
        register_artifact=register_published_bar_artifact,
        register_spec=register_bar_run_spec,
        reserve_attempt=reserve_run_attempt,
        start_attempt=start_run_attempt,
        validate_reused_attempts=validate_reused_bar_attempts,
        validate_completed_campaign=validate_completed_bar_campaign,
        run_discovery=run_streaming_bar_pattern_discovery,
        validate_discovery=_validate_discovery_result,
        publish_global_result=publish_global_bar_discovery_result,
        publish_terminal=publish_bar_terminal_result_artifact,
        register_terminal=register_terminal_bar_result,
        fail_attempt=abort_bar_run_attempt,
    )


def _snapshot_config_sha256(snapshot: CodeSnapshot) -> str:
    relative = BAR_PATTERN_CONFIG_RELATIVE_PATH.as_posix()
    matches = [item.sha256 for item in snapshot.files if item.relative_path == relative]
    if len(matches) != 1:
        raise BarResearchRunError("code snapshot does not contain exactly one bar config")
    return matches[0]


def prepare_bar_research_run(
    project_root: Path | str,
    dataset: LoadedBarDatasetManifest,
    *,
    services: BarResearchRunServices | None = None,
) -> PreparedBarResearchRun:
    """Derive and verify the complete outcome-free Discovery plan."""

    active = services or _default_services()
    root, data = _strict_project_root(project_root)
    if not isinstance(dataset, LoadedBarDatasetManifest):
        raise BarResearchRunError("dataset must be a verified LoadedBarDatasetManifest")
    config = active.load_config(root)
    if len(config.candidates) != ALLOCATED_VARIANT_COUNT:
        raise BarResearchRunError("config does not contain the exact 216 candidates")
    if dataset.dataset_manifest_sha256 != EXPECTED_DATASET_MANIFEST_SHA256:
        raise BarResearchRunError("dataset manifest differs from the approved Discovery input")
    if dataset.source_manifest_sha256 != BAR_SOURCE_MANIFEST_SHA256:
        raise BarResearchRunError("dataset raw-source lineage drift")
    if dataset.outcome_span_policy_sha256 != BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256:
        raise BarResearchRunError("dataset outcome-span policy drift")
    handoff_sha256 = dataset.handoff_sha256
    if handoff_sha256 != EXPECTED_DATASET_HANDOFF_SHA256:
        raise BarResearchRunError("dataset Discovery handoff identity drift")
    split_plan = active.plan_splits(dataset.eligible_active_dates)
    if split_plan.sha256 != EXPECTED_SPLIT_PLAN_SHA256:
        raise BarResearchRunError("derived split plan differs from the preregistered split")
    if config.canonical_parameters()["source"]["source_manifest_sha256"] != (
        dataset.source_manifest_sha256
    ):
        raise BarResearchRunError("config and dataset raw-source lineage differ")
    return PreparedBarResearchRun(
        project_root=root,
        data_root=data,
        dataset=dataset,
        config=config,
        split_plan=split_plan,
        dataset_handoff_sha256=handoff_sha256,
    )


def _plain(value: object) -> dict[str, object]:
    decoded = json.loads(canonical_json_bytes(value))
    if not isinstance(decoded, dict):
        raise BarResearchRunError("policy must encode a canonical object")
    return decoded


def _assert_discovery_evidence_buffer_contract() -> None:
    if (
        getattr(bar_discovery_module, "_MATCH_SHARD_MAX_RECORDS", None)
        != BAR_EVIDENCE_MATCH_SHARD_MAX_RECORDS
        or getattr(bar_discovery_module, "_REPLAY_SHARD_MAX_RECORDS", None)
        != BAR_EVIDENCE_REPLAY_SHARD_MAX_RECORDS
    ):
        raise BarResearchRunError("Discovery evidence buffer constants drifted")


def _policy_documents(
    prepared: PreparedBarResearchRun,
    candidate: BarPatternCandidate,
) -> dict[str, dict[str, object]]:
    _assert_discovery_evidence_buffer_contract()
    campaign = prepared.config.canonical_parameters()
    entry = _plain(campaign["entry"])
    barriers = _plain(campaign["barriers"])
    bars = _plain(campaign["bars"])
    market = _plain(campaign["market"])
    scenarios = list(campaign["execution_scenarios"])
    signal = {
        "bars": bars,
        "candidate_definition": candidate.definition_payload(),
        "candidate_definition_sha256": candidate.definition_sha256,
        "candidate_key": candidate.candidate_key,
        "decision_visibility": "DISCOVERY_DECISION_DATES_ONLY",
        "schema": "systematic_fx.bar_signal_policy.v1",
    }
    entry_policy = {
        **entry,
        "candidate_timeframe_seconds": candidate.timeframe_seconds,
        "schema": "systematic_fx.bar_entry_policy.v1",
    }
    barrier = {
        **barriers,
        "grid_evaluation": "ALL_CELLS_NO_EARLY_PRUNING",
        "ticks_per_pip": market["ticks_per_pip"],
        "schema": "systematic_fx.bar_barrier_policy.v1",
    }
    terminal = {
        "holding_limit_policy": entry["holding_limit_policy"],
        "normal_market_closure_policy": entry["normal_market_closure_policy"],
        "outcome_span_policy_sha256": prepared.dataset.outcome_span_policy_sha256,
        "split_boundary_policy": "TERMINAL_EXIT_AT_DISCOVERY_END",
        "terminal_boundary_types": entry["terminal_boundary_types"],
        "schema": "systematic_fx.bar_terminal_policy.v1",
    }
    cost = {
        "base_monthly_fixed_pool_usd": format(BASE_MONTHLY_FIXED_POOL_USD, "f"),
        "execution_scenarios": scenarios,
        "expected_monthly_round_trips": EXPECTED_MONTHLY_ROUND_TRIPS,
        "fixed_cost_allocation": "MONTHLY_POOL_DIVIDED_BY_ROUND_TRIPS_CEILING_TICKS",
        "market": market,
        "schema": "systematic_fx.bar_cost_policy.v1",
        "tick_value_usd": format(TICK_VALUE_USD, "f"),
    }
    selection = {
        "candidate_budget": campaign["candidate_budget"],
        "discovery_economic_gates": campaign["discovery_economic_gates"],
        "discovery_support_gates": campaign["discovery_support_gates"],
        "finalist_limit": campaign["holdout_gates"]["maximum_finalists"],
        "ranking_order": [
            "positive_block_count_desc",
            "worst_block_moderate_ev_desc",
            "overall_moderate_ev_desc",
            "moderate_maximum_drawdown_asc",
            "selected_stop_loss_ticks_asc",
            "selected_take_profit_ticks_asc",
            "candidate_key_asc",
        ],
        "schema": "systematic_fx.bar_selection_policy.v1",
    }
    evidence = {
        "candidate_summary": "FULL_3_SCENARIO_484_CELL_SURFACES",
        "evidence_schema": DISCOVERY_EVIDENCE_SCHEMA,
        "match_shard_max_records": BAR_EVIDENCE_MATCH_SHARD_MAX_RECORDS,
        "publication": "CONTENT_ADDRESSED_HELD_DIRFD",
        "record_kinds": ["matches", "replays"],
        "replay_shard_max_records": BAR_EVIDENCE_REPLAY_SHARD_MAX_RECORDS,
        "spool_version": DISCOVERY_SPOOL_VERSION,
        "schema": "systematic_fx.bar_evidence_policy.v1",
    }
    execution = {
        "barrier_policy": barrier,
        "entry_policy": entry_policy,
        "one_position_policy": "INDEPENDENT_PER_CANDIDATE_SCENARIO_TP_SL_CELL",
        "same_second_first_touch_policy": entry["same_second_first_touch_policy"],
        "terminal_policy": terminal,
        "schema": "systematic_fx.bar_execution_policy.v1",
    }
    outcome = {
        "barrier_policy": barrier,
        "one_second_path_source": "VERIFIED_SELECTED_CONTRACT_TRADE_BARS",
        "outcome_span_policy_sha256": prepared.dataset.outcome_span_policy_sha256,
        "same_second_first_touch_policy": entry["same_second_first_touch_policy"],
        "terminal_policy": terminal,
        "schema": "systematic_fx.bar_outcome_policy.v1",
    }
    return {
        "barrier": barrier,
        "cost": cost,
        "entry": entry_policy,
        "evidence": evidence,
        "execution": execution,
        "outcome": outcome,
        "selection": selection,
        "signal": signal,
        "terminal": terminal,
    }


def build_bar_candidate_run_specs(
    prepared: PreparedBarResearchRun,
    provenance: BarRunProvenance,
) -> tuple[RunSpec, ...]:
    """Build exactly one complete, candidate-specific RunSpec per variant."""

    calendar_document = {
        "dataset_handoff_sha256": prepared.dataset_handoff_sha256,
        "eligible_active_dates": [item.isoformat() for item in prepared.split_plan.eligible_dates],
        "schema": "systematic_fx.bar_eligible_calendar.v1",
    }
    calendar_sha256 = canonical_sha256(calendar_document)
    specs: list[RunSpec] = []
    for candidate in prepared.config.candidates:
        policies = _policy_documents(prepared, candidate)
        trial_parameters = candidate_trial_parameters(
            prepared.config,
            prepared.split_plan,
            candidate,
            raw_source_manifest_sha256=prepared.dataset.source_manifest_sha256,
            bar_dataset_manifest_sha256=prepared.dataset.dataset_manifest_sha256,
        )
        parameters = {
            "bar_cost_policy": policies["cost"],
            "bar_barrier_policy_sha256": canonical_sha256(policies["barrier"]),
            "bar_campaign_definition_sha256": prepared.config.definition_sha256,
            "bar_candidate_catalog_sha256": prepared.config.candidate_catalog_sha256,
            "bar_candidate_definition_sha256": candidate.definition_sha256,
            "bar_candidate_key": candidate.candidate_key,
            "bar_code_snapshot_artifact_identity_sha256": (
                provenance.snapshot_artifact.descriptor.identity_sha256
            ),
            "bar_config_file_sha256": prepared.config.sha256,
            "bar_config_semantic_sha256": prepared.config.semantic_sha256,
            "bar_cost_policy_sha256": canonical_sha256(policies["cost"]),
            "bar_dataset_handoff_sha256": prepared.dataset_handoff_sha256,
            "bar_dataset_manifest_sha256": prepared.dataset.dataset_manifest_sha256,
            "bar_entry_policy_sha256": canonical_sha256(policies["entry"]),
            "bar_evidence_policy": policies["evidence"],
            "bar_evidence_policy_sha256": canonical_sha256(policies["evidence"]),
            "bar_execution_policy": policies["execution"],
            "bar_outcome_policy": policies["outcome"],
            "bar_outcome_span_policy_sha256": prepared.dataset.outcome_span_policy_sha256,
            "bar_postgres_migrations_sha256": provenance.postgres_migrations_sha256,
            "bar_raw_source_manifest_sha256": prepared.dataset.source_manifest_sha256,
            "bar_screening_only": BAR_PATTERN_SCREENING_ONLY,
            "bar_selection_policy": policies["selection"],
            "bar_selection_policy_sha256": canonical_sha256(policies["selection"]),
            "bar_split_plan_sha256": prepared.split_plan.sha256,
            "bar_trial_parameters_sha256": canonical_sha256(trial_parameters),
            "qualification_status": BAR_PATTERN_QUALIFICATION_STATUS,
        }
        specs.append(
            RunSpec(
                campaign_id=BAR_PATTERN_CAMPAIGN_KEY,
                experiment_id=BAR_CATALOG_EXPERIMENT_KEY,
                run_kind="SCREEN",
                engine_version=BAR_DISCOVERY_ENGINE_VERSION,
                source_manifest_hashes={
                    RAW_SOURCE_MANIFEST_KEY: prepared.dataset.source_manifest_sha256,
                    BAR_DATASET_MANIFEST_KEY: prepared.dataset.dataset_manifest_sha256,
                },
                eligible_calendar_version=BAR_ELIGIBLE_CALENDAR_VERSION,
                eligible_calendar_sha256=calendar_sha256,
                split_version=BAR_SPLIT_VERSION,
                split_sha256=prepared.split_plan.sha256,
                feature_version=BAR_FEATURE_VERSION,
                feature_sha256=canonical_sha256(policies["signal"]),
                outcome_version=BAR_OUTCOME_VERSION,
                outcome_sha256=canonical_sha256(policies["outcome"]),
                cost_version=BAR_COST_VERSION,
                cost_sha256=canonical_sha256(policies["cost"]),
                execution_version=BAR_EXECUTION_VERSION,
                execution_sha256=canonical_sha256(policies["execution"]),
                code_commit=provenance.code_commit,
                code_snapshot_sha256=provenance.snapshot.sha256,
                dependency_lock_sha256=provenance.dependency_lock_sha256,
                runtime_environment=provenance.runtime_environment,
                random_seed=BAR_RANDOM_SEED,
                direction=candidate.direction.value,
                signal_policy=policies["signal"],
                entry_policy=policies["entry"],
                barrier_policy=policies["barrier"],
                terminal_policy=policies["terminal"],
                parameters=parameters,
            )
        )
    result = tuple(specs)
    if len(result) != ALLOCATED_VARIANT_COUNT or len({item.fingerprint for item in result}) != len(
        result
    ):
        raise BarResearchRunError("candidate RunSpecs are incomplete or collide")
    return result


def _capture_provenance(
    prepared: PreparedBarResearchRun,
    database_url: str,
    services: BarResearchRunServices,
) -> BarRunProvenance:
    commit = services.git_head(prepared.project_root)
    snapshot = services.build_snapshot(prepared.project_root, code_commit=commit)
    if _snapshot_config_sha256(snapshot) != prepared.config.sha256:
        raise BarResearchRunError("loaded config bytes differ from the code snapshot")
    reloaded_config = services.load_config(prepared.project_root)
    if (
        reloaded_config.sha256 != prepared.config.sha256
        or reloaded_config.semantic_sha256 != prepared.config.semantic_sha256
        or reloaded_config.definition_sha256 != prepared.config.definition_sha256
    ):
        raise BarResearchRunError("bar config changed while capturing the code snapshot")
    artifact = services.publish_snapshot(
        prepared.project_root,
        snapshot,
        dataset=prepared.dataset,
    )
    if artifact.sha256 != snapshot.sha256:
        raise BarResearchRunError("published code snapshot differs from captured bytes")
    services.verify_artifact(prepared.project_root, artifact)
    dependency_sha256 = services.dependency_hash(prepared.project_root)
    postgres = services.postgres_runtime(
        database_url,
        migrations_directory=prepared.project_root / "migrations",
    )
    migration_sha256 = postgres.get("schema_migrations_sha256")
    if not isinstance(migration_sha256, str) or _SHA256.fullmatch(migration_sha256) is None:
        raise BarResearchRunError("PostgreSQL runtime lacks a migration SHA-256")
    runtime = dict(services.runtime())
    runtime["postgresql"] = postgres
    runtime["bar_research_run"] = {
        "code_snapshot_artifact_identity_sha256": artifact.descriptor.identity_sha256,
        "dataset_handoff_sha256": prepared.dataset_handoff_sha256,
        "engine_version": BAR_DISCOVERY_ENGINE_VERSION,
        "orchestration": "REGISTER_AND_START_ALL_BEFORE_SINGLE_DISCOVERY_PASS",
    }
    canonical_sha256(runtime)  # strict-JSON validation
    return BarRunProvenance(
        code_commit=commit,
        snapshot=snapshot,
        snapshot_artifact=artifact,
        dependency_lock_sha256=dependency_sha256,
        runtime_environment=runtime,
        postgres_migrations_sha256=migration_sha256,
    )


def _assert_provenance_unchanged(
    prepared: PreparedBarResearchRun,
    provenance: BarRunProvenance,
    database_url: str,
    services: BarResearchRunServices,
) -> None:
    current_config = services.load_config(prepared.project_root)
    current_snapshot = services.build_snapshot(
        prepared.project_root,
        code_commit=provenance.code_commit,
    )
    current_postgres = services.postgres_runtime(
        database_url,
        migrations_directory=prepared.project_root / "migrations",
    )
    current_runtime = dict(services.runtime())
    current_runtime["postgresql"] = current_postgres
    current_runtime["bar_research_run"] = dict(
        provenance.runtime_environment["bar_research_run"]  # type: ignore[arg-type]
    )
    unchanged = (
        services.git_head(prepared.project_root) == provenance.code_commit
        and current_snapshot.sha256 == provenance.snapshot.sha256
        and _snapshot_config_sha256(current_snapshot) == current_config.sha256
        and current_config.sha256 == prepared.config.sha256
        and current_config.semantic_sha256 == prepared.config.semantic_sha256
        and current_config.definition_sha256 == prepared.config.definition_sha256
        and current_config.candidate_catalog_sha256 == prepared.config.candidate_catalog_sha256
        and services.dependency_hash(prepared.project_root) == provenance.dependency_lock_sha256
        and canonical_sha256(current_runtime) == canonical_sha256(provenance.runtime_environment)
        and current_postgres.get("schema_migrations_sha256")
        == provenance.postgres_migrations_sha256
        and services.plan_splits(prepared.dataset.eligible_active_dates).sha256
        == prepared.split_plan.sha256
    )
    services.verify_artifact(prepared.project_root, provenance.snapshot_artifact)
    if not unchanged:
        raise BarResearchRunError("code, config, dependency, runtime, database, or split drift")


def _discovery_dates(split_plan: BarSplitPlan) -> tuple[tuple[Any, ...], tuple[Any, ...]]:
    start = split_plan.discovery.start_active_ordinal - 1
    end = split_plan.discovery.end_active_ordinal
    loaded = split_plan.eligible_dates[start:end]
    decision_end = split_plan.discovery.decision_end_date
    if decision_end is None:
        raise BarResearchRunError("Discovery has no decision boundary")
    decisions = tuple(item for item in loaded if item <= decision_end)
    return loaded, decisions


def _validate_discovery_result(
    result: BarDiscoveryResult,
    *,
    prepared: PreparedBarResearchRun,
) -> None:
    """Reject incomplete surfaces or lineage before terminal publication."""

    if not isinstance(result, BarDiscoveryResult):
        raise BarResearchRunError("Discovery returned an unsupported result type")
    expected_loaded, expected_decisions = _discovery_dates(prepared.split_plan)
    if (
        result.source_identity_sha256 != prepared.dataset.dataset_manifest_sha256
        or result.dataset_build_sha256 != prepared.dataset.dataset_manifest_sha256
        or result.outcome_span_policy_sha256 != prepared.dataset.outcome_span_policy_sha256
        or result.config_semantic_sha256 != prepared.config.semantic_sha256
        or result.candidate_catalog_sha256 != prepared.config.candidate_catalog_sha256
        or result.split_plan_sha256 != prepared.split_plan.sha256
        or result.loaded_source_dates != expected_loaded
        or result.decision_dates != expected_decisions
        or result.replay_catalog
    ):
        raise BarResearchRunError("Discovery result lineage or visible date boundary drift")
    expected_candidates = tuple(item.candidate_key for item in prepared.config.candidates)
    observed_candidates = tuple(item.candidate.candidate_key for item in result.candidate_results)
    if observed_candidates != expected_candidates:
        raise BarResearchRunError("Discovery did not return the exact ordered 216 candidates")
    evidence = result.evidence_manifest
    if evidence is None or (
        evidence.source_identity_sha256 != prepared.dataset.dataset_manifest_sha256
        or evidence.source_manifest_sha256 != prepared.dataset.source_manifest_sha256
        or evidence.dataset_build_sha256 != prepared.dataset.dataset_manifest_sha256
        or evidence.outcome_span_policy_sha256 != prepared.dataset.outcome_span_policy_sha256
        or evidence.split_plan_sha256 != prepared.split_plan.sha256
        or evidence.config_semantic_sha256 != prepared.config.semantic_sha256
        or evidence.candidate_catalog_sha256 != prepared.config.candidate_catalog_sha256
    ):
        raise BarResearchRunError("Discovery evidence manifest lineage drift")
    axis = {(tp, sl) for tp in BARRIER_TICKS for sl in BARRIER_TICKS}
    scenario_ids = tuple(item.scenario_id for item in prepared.config.execution_scenarios)
    selected: list[str] = []
    budget_rejected: list[str] = []
    for item in result.candidate_results:
        if tuple(value.scenario_id for value in item.economics) != scenario_ids:
            raise BarResearchRunError("candidate economics scenarios are incomplete")
        for scenario in item.economics:
            identities = {(cell.take_profit_ticks, cell.stop_loss_ticks) for cell in scenario.cells}
            if len(scenario.cells) != len(axis) or identities != axis:
                raise BarResearchRunError("candidate economics surface is not the full 484 cells")
        if item.final_label == "DISCOVERY_FINALIST_SELECTED":
            selected.append(item.candidate.candidate_key)
        elif item.final_label == "DISCOVERY_FINALIST_BUDGET_REJECTED":
            budget_rejected.append(item.candidate.candidate_key)
        elif item.final_label not in {"SUPPORT_REJECT", "ECONOMIC_REJECT"}:
            raise BarResearchRunError("candidate has an invalid final Discovery label")
    if tuple(selected) != result.ranked_finalist_keys or tuple(budget_rejected) != (
        result.budget_rejected_keys
    ):
        raise BarResearchRunError("ranked finalist labels do not balance")
    if len(selected) > 10:
        raise BarResearchRunError("Discovery finalist budget drift")


def _notify(
    callback: Callable[[BarResearchRunProgress], None] | None,
    stage: str,
    completed: int,
    total: int,
    *,
    candidate_key: str | None = None,
    discovery: BarDiscoveryProgress | None = None,
) -> None:
    if callback is not None:
        callback(
            BarResearchRunProgress(
                stage=stage,
                completed=completed,
                total=total,
                candidate_key=candidate_key,
                discovery=discovery,
            )
        )


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def _evidence_lineage(
    result: Any,
    prepared: PreparedBarResearchRun,
    provenance: BarRunProvenance,
    global_result_artifact: PublishedBarArtifact,
) -> dict[str, object]:
    evidence = result.evidence_manifest
    return {
        "candidate_catalog_sha256": prepared.config.candidate_catalog_sha256,
        "code_snapshot_artifact_identity_sha256": (
            provenance.snapshot_artifact.descriptor.identity_sha256
        ),
        "code_snapshot_sha256": provenance.snapshot.sha256,
        "config_file_sha256": prepared.config.sha256,
        "config_semantic_sha256": prepared.config.semantic_sha256,
        "dataset_handoff_sha256": prepared.dataset_handoff_sha256,
        "dataset_manifest_sha256": prepared.dataset.dataset_manifest_sha256,
        "discovery_result_schema": DISCOVERY_RESULT_SCHEMA,
        "discovery_result_sha256": global_result_artifact.sha256,
        "evidence_artifact_identity_sha256": evidence.artifact.descriptor.identity_sha256,
        "evidence_identity_sha256": evidence.evidence_identity_sha256,
        "evidence_manifest_sha256": evidence.sha256,
        "evidence_matched_record_count": evidence.matched_record_count,
        "evidence_replay_record_count": evidence.replay_record_count,
        "evidence_shard_count": len(evidence.shards),
        "global_result_artifact_identity_sha256": (
            global_result_artifact.descriptor.identity_sha256
        ),
        "global_result_artifact_sha256": global_result_artifact.sha256,
        "outcome_span_policy_sha256": prepared.dataset.outcome_span_policy_sha256,
        "postgres_migrations_sha256": provenance.postgres_migrations_sha256,
        "raw_source_manifest_sha256": prepared.dataset.source_manifest_sha256,
        "schema": BAR_DISCOVERY_LINEAGE_SCHEMA,
        "split_plan_sha256": prepared.split_plan.sha256,
    }


def _terminal_payloads(
    candidate_result: Any,
    *,
    lineage: Mapping[str, object],
) -> tuple[str, str, dict[str, object], dict[str, object]]:
    final_label = str(candidate_result.final_label)
    selected = final_label == "DISCOVERY_FINALIST_SELECTED"
    trial_status = "SUCCEEDED" if selected else "REJECTED"
    decision_label = "DISCOVERY_FINALIST" if selected else "SCREENING_REJECT"
    decision = candidate_result.decision
    compact = {
        "candidate_definition_sha256": candidate_result.candidate.definition_sha256,
        "candidate_key": candidate_result.candidate.candidate_key,
        "decision_label": decision_label,
        "decision_trigger_count": candidate_result.decision_trigger_count,
        "discovery_result_sha256": lineage["discovery_result_sha256"],
        "distinct_signal_day_count": candidate_result.support.distinct_signal_day_count,
        "evidence_artifact_identity_sha256": lineage["evidence_artifact_identity_sha256"],
        "evidence_identity_sha256": lineage["evidence_identity_sha256"],
        "evidence_manifest_sha256": lineage["evidence_manifest_sha256"],
        "final_label": final_label,
        "global_result_artifact_identity_sha256": lineage["global_result_artifact_identity_sha256"],
        "global_result_artifact_sha256": lineage["global_result_artifact_sha256"],
        "matched_signal_count": candidate_result.matched_signal_count,
        "positive_component_size": decision.positive_component_size,
        "qualification_status": BAR_PATTERN_QUALIFICATION_STATUS,
        "raw_signal_count": candidate_result.support.raw_signal_count,
        "rejection_reasons": list(decision.rejection_reasons),
        "screening_only": BAR_PATTERN_SCREENING_ONLY,
        "selected_buy_sell_loss_formula": decision.selected_buy_sell_loss_formula,
        "selected_stop_loss_ticks": decision.selected_stop_loss_ticks,
        "selected_take_profit_ticks": decision.selected_take_profit_ticks,
        "moderate_ev_ticks": _decimal_text(decision.overall_moderate_ev_ticks),
    }
    full = candidate_result.as_dict()
    full["discovery_lineage"] = dict(lineage)
    return trial_status, decision_label, compact, full


def _register_evidence(
    database_url: str,
    prepared: PreparedBarResearchRun,
    result: Any,
    services: BarResearchRunServices,
    progress: Callable[[BarResearchRunProgress], None] | None,
) -> int:
    artifacts = [item.artifact for item in result.evidence_manifest.shards]
    artifacts.append(result.evidence_manifest.artifact)
    for index, artifact in enumerate(artifacts, start=1):
        services.register_artifact(
            database_url,
            prepared.project_root,
            artifact,
        )
        _notify(progress, "EVIDENCE_REGISTERED", index, len(artifacts))
    return len(artifacts)


def _fail_running(
    database_url: str,
    active_attempts: Mapping[str, _AttemptCleanupIdentity],
    error: BaseException,
    services: BarResearchRunServices,
) -> tuple[str, ...]:
    failures: list[str] = []
    error_text = f"{type(error).__name__}: {error}"[:2_000]
    for candidate_key, identity in tuple(active_attempts.items()):
        try:
            services.fail_attempt(
                database_url,
                research_run_attempt_id=identity.research_run_attempt_id,
                candidate_key=candidate_key,
                run_fingerprint=identity.run_fingerprint,
                result_summary={
                    "candidate_key": candidate_key,
                    "reason": "GOVERNED_DISCOVERY_ABORTED",
                    "schema": BAR_RESEARCH_RUN_SCHEMA,
                },
                error_message=error_text,
            )
        except Exception as cleanup_error:  # noqa: BLE001 - audit every unresolved ledger row
            failures.append(f"{candidate_key}:{type(cleanup_error).__name__}")
    return tuple(failures)


def _registered_spec_id(registration: Any, spec: RunSpec) -> int:
    identifier = getattr(registration, "research_run_spec_id", None)
    if isinstance(identifier, bool) or not isinstance(identifier, int) or identifier <= 0:
        raise BarResearchRunError("RunSpec registration returned an invalid identifier")
    if getattr(registration, "run_fingerprint", None) != spec.fingerprint:
        raise BarResearchRunError("RunSpec registration returned a different fingerprint")
    return identifier


def _validate_reservation(
    reservation: Any,
    *,
    research_run_spec_id: int,
) -> None:
    attempt_id = getattr(reservation, "research_run_attempt_id", None)
    if isinstance(attempt_id, bool) or not isinstance(attempt_id, int) or attempt_id <= 0:
        raise BarResearchRunError("attempt reservation returned an invalid identifier")
    if getattr(reservation, "research_run_spec_id", None) != research_run_spec_id:
        raise BarResearchRunError("attempt reservation is bound to a different RunSpec")
    attempt_number = getattr(reservation, "attempt_number", None)
    if (
        isinstance(attempt_number, bool)
        or not isinstance(attempt_number, int)
        or attempt_number <= 0
    ):
        raise BarResearchRunError("attempt reservation has an invalid attempt number")
    execute = getattr(reservation, "execute", None)
    status = getattr(reservation, "status", None)
    reused_attempt_id = getattr(reservation, "reused_attempt_id", None)
    if execute is True:
        if status != "QUEUED" or reused_attempt_id is not None:
            raise BarResearchRunError("executable reservation is not an exact QUEUED attempt")
    elif execute is False:
        if (
            status != "SKIPPED_DUPLICATE"
            or isinstance(reused_attempt_id, bool)
            or not isinstance(reused_attempt_id, int)
            or reused_attempt_id <= 0
            or reused_attempt_id == attempt_id
        ):
            raise BarResearchRunError("duplicate reservation has invalid reuse lineage")
    else:
        raise BarResearchRunError("attempt reservation execute flag is not boolean")


def _report_sha256(report: Any, field: str) -> str:
    value = getattr(report, field, None)
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise BarResearchRunError(f"reuse report has invalid {field}")
    return value


def _validate_reuse_report(
    report: Any,
    *,
    prepared: PreparedBarResearchRun,
    spec_by_key: Mapping[str, RunSpec],
    reservation_bindings: Mapping[str, tuple[RunSpec, Any]],
    expected_keys: tuple[str, ...],
    completion_view: bool,
) -> tuple[BarCandidateRunReport, ...]:
    """Validate the DB adapter's live-artifact consensus before trusting it."""

    candidates = getattr(report, "candidates", None)
    if not isinstance(candidates, tuple):
        raise BarResearchRunError("reuse report candidates must be an ordered tuple")
    observed_keys = tuple(getattr(item, "candidate_key", None) for item in candidates)
    if observed_keys != expected_keys:
        raise BarResearchRunError("reuse report candidates differ from the expected catalog")
    candidate_reports: list[BarCandidateRunReport] = []
    for candidate_key, item in zip(expected_keys, candidates, strict=True):
        spec = spec_by_key[candidate_key]
        if getattr(item, "run_fingerprint", None) != spec.fingerprint:
            raise BarResearchRunError("reuse report RunSpec fingerprint drift")
        duplicate_attempt_id = getattr(item, "duplicate_attempt_id", None)
        reused_attempt_id = getattr(item, "reused_attempt_id", None)
        if (
            isinstance(reused_attempt_id, bool)
            or not isinstance(reused_attempt_id, int)
            or (reused_attempt_id <= 0)
        ):
            raise BarResearchRunError("reuse report has invalid succeeded-attempt lineage")
        if completion_view:
            if duplicate_attempt_id is not None:
                raise BarResearchRunError("completion view cannot name a duplicate attempt")
        else:
            reservation = reservation_bindings[candidate_key][1]
            if (
                duplicate_attempt_id != reservation.research_run_attempt_id
                or reused_attempt_id != reservation.reused_attempt_id
            ):
                raise BarResearchRunError("reuse report attempt lineage drift")
        trial_status = getattr(item, "trial_status", None)
        final_label = getattr(item, "final_label", None)
        expected_trial_status = (
            "SUCCEEDED" if final_label == "DISCOVERY_FINALIST_SELECTED" else "REJECTED"
        )
        if (
            final_label
            not in {
                "SUPPORT_REJECT",
                "ECONOMIC_REJECT",
                "DISCOVERY_FINALIST_SELECTED",
                "DISCOVERY_FINALIST_BUDGET_REJECTED",
            }
            or trial_status != expected_trial_status
        ):
            raise BarResearchRunError("reuse report terminal candidate state is invalid")
        terminal_sha256 = getattr(item, "terminal_artifact_sha256", None)
        if not isinstance(terminal_sha256, str) or _SHA256.fullmatch(terminal_sha256) is None:
            raise BarResearchRunError("reuse report terminal artifact SHA-256 is invalid")
        candidate_reports.append(
            BarCandidateRunReport(
                candidate_key=candidate_key,
                run_fingerprint=spec.fingerprint,
                research_run_attempt_id=(
                    reused_attempt_id if completion_view else duplicate_attempt_id
                ),
                disposition=("COMPLETION_VALIDATED" if completion_view else "SKIPPED_DUPLICATE"),
                final_label=final_label,
                trial_status=trial_status,
                terminal_artifact_sha256=terminal_sha256,
            )
        )

    for field in (
        "global_result_artifact_sha256",
        "global_result_artifact_identity_sha256",
        "evidence_manifest_sha256",
        "evidence_artifact_identity_sha256",
        "evidence_identity_sha256",
    ):
        _report_sha256(report, field)
    finalist_keys = getattr(report, "finalist_keys", None)
    if (
        not isinstance(finalist_keys, tuple)
        or len(finalist_keys) > 10
        or len(set(finalist_keys)) != len(finalist_keys)
        or any(key not in prepared.candidate_keys for key in finalist_keys)
    ):
        raise BarResearchRunError("reuse report finalist keys are invalid")
    final_label_counts = getattr(report, "final_label_counts", None)
    if not isinstance(final_label_counts, tuple) or final_label_counts != tuple(
        sorted(final_label_counts)
    ):
        raise BarResearchRunError("reuse report final-label counts are not canonical")
    try:
        counts = dict(final_label_counts)
    except (TypeError, ValueError) as error:
        raise BarResearchRunError("reuse report final-label counts are invalid") from error
    if (
        set(counts)
        - {
            "SUPPORT_REJECT",
            "ECONOMIC_REJECT",
            "DISCOVERY_FINALIST_SELECTED",
            "DISCOVERY_FINALIST_BUDGET_REJECTED",
        }
        or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in counts.values()
        )
        or sum(counts.values()) != ALLOCATED_VARIANT_COUNT
    ):
        raise BarResearchRunError("reuse report final-label counts do not cover 216 candidates")
    if len(expected_keys) == ALLOCATED_VARIANT_COUNT:
        observed_counts = Counter(item.final_label for item in candidate_reports)
        if tuple(sorted(observed_counts.items())) != final_label_counts:
            raise BarResearchRunError("reuse report candidate labels do not balance")
        selected = {
            item.candidate_key
            for item in candidate_reports
            if item.final_label == "DISCOVERY_FINALIST_SELECTED"
        }
        if selected != set(finalist_keys):
            raise BarResearchRunError("reuse report finalist identities do not balance")
    return tuple(candidate_reports)


def _assert_result_consensus(
    report: Any,
    *,
    discovery_result: Any,
    global_result_artifact: PublishedBarArtifact,
) -> None:
    evidence = discovery_result.evidence_manifest
    labels = tuple(
        sorted(Counter(item.final_label for item in discovery_result.candidate_results).items())
    )
    if (
        global_result_artifact.sha256 != _report_sha256(report, "global_result_artifact_sha256")
        or global_result_artifact.descriptor.identity_sha256
        != _report_sha256(report, "global_result_artifact_identity_sha256")
        or evidence.sha256 != _report_sha256(report, "evidence_manifest_sha256")
        or evidence.artifact.descriptor.identity_sha256
        != _report_sha256(report, "evidence_artifact_identity_sha256")
        or evidence.evidence_identity_sha256 != _report_sha256(report, "evidence_identity_sha256")
        or tuple(discovery_result.ranked_finalist_keys) != tuple(report.finalist_keys)
        or labels != tuple(report.final_label_counts)
    ):
        raise BarResearchRunError("new Discovery result differs from persisted duplicate consensus")


def _validate_terminal_registration(
    registration: Any,
    *,
    research_run_spec_id: int,
    result: BarTerminalResult,
) -> None:
    expected = {
        "research_run_attempt_id": result.research_run_attempt_id,
        "research_run_spec_id": research_run_spec_id,
        "attempt_status": "SUCCEEDED",
        "trial_status": result.trial_status,
        "decision_label": result.decision_label,
    }
    if any(getattr(registration, key, None) != value for key, value in expected.items()):
        raise BarResearchRunError("terminal registration returned a different terminal identity")


def execute_prepared_bar_research_run(
    prepared: PreparedBarResearchRun,
    *,
    mode: RunMode = "PLAN_ONLY",
    database_url: str | None = None,
    services: BarResearchRunServices | None = None,
    progress: Callable[[BarResearchRunProgress], None] | None = None,
) -> BarResearchRunReport:
    """Execute a prepared plan without allowing outcomes before preregistration."""

    if not isinstance(prepared, PreparedBarResearchRun):
        raise BarResearchRunError("prepared must be a PreparedBarResearchRun")
    if mode not in {"PLAN_ONLY", "RUN"}:
        raise BarResearchRunError("mode must be PLAN_ONLY or RUN")
    _notify(progress, "PLAN_READY", ALLOCATED_VARIANT_COUNT, ALLOCATED_VARIANT_COUNT)
    if mode == "PLAN_ONLY":
        return BarResearchRunReport(mode=mode, disposition="PLANNED", plan=prepared)
    if not isinstance(database_url, str) or not database_url.strip():
        raise BarResearchRunError("RUN requires a non-empty database_url")
    active = services or _default_services()
    active_attempts: dict[str, _AttemptCleanupIdentity] = {}
    try:
        provenance = _capture_provenance(prepared, database_url, active)
        _notify(progress, "PROVENANCE_FROZEN", 1, 1)
        registration_artifact = active.publish_registration(
            prepared.project_root,
            prepared.config,
            prepared.split_plan,
            raw_source_manifest_sha256=prepared.dataset.source_manifest_sha256,
            bar_dataset_manifest_sha256=prepared.dataset.dataset_manifest_sha256,
            code_commit=provenance.code_commit,
        )
        active.register_campaign(
            database_url,
            prepared.project_root,
            dataset_key=BAR_DATASET_KEY,
            config=prepared.config,
            split_plan=prepared.split_plan,
            raw_source_manifest_sha256=prepared.dataset.source_manifest_sha256,
            bar_dataset_manifest_sha256=prepared.dataset.dataset_manifest_sha256,
            code_commit=provenance.code_commit,
            registration_artifact=registration_artifact,
        )
        active.register_artifact(
            database_url,
            prepared.project_root,
            provenance.snapshot_artifact,
        )
        _notify(progress, "CAMPAIGN_REGISTERED", ALLOCATED_VARIANT_COUNT, ALLOCATED_VARIANT_COUNT)

        specs = build_bar_candidate_run_specs(prepared, provenance)
        spec_by_key = dict(zip(prepared.candidate_keys, specs, strict=True))
        spec_registration_ids: dict[str, int] = {}
        for index, candidate in enumerate(prepared.config.candidates, start=1):
            spec = spec_by_key[candidate.candidate_key]
            registration = active.register_spec(
                database_url,
                spec,
                config=prepared.config,
                split_plan=prepared.split_plan,
                candidate_key=candidate.candidate_key,
                raw_source_manifest_sha256=prepared.dataset.source_manifest_sha256,
                bar_dataset_manifest_sha256=prepared.dataset.dataset_manifest_sha256,
            )
            spec_registration_ids[candidate.candidate_key] = _registered_spec_id(
                registration,
                spec,
            )
            _notify(
                progress,
                "RUN_SPEC_REGISTERED",
                index,
                len(specs),
                candidate_key=candidate.candidate_key,
            )

        reservation_bindings: dict[str, tuple[RunSpec, Any]] = {}
        for index, candidate in enumerate(prepared.config.candidates, start=1):
            spec = spec_by_key[candidate.candidate_key]
            reservation = active.reserve_attempt(
                database_url,
                run_fingerprint=spec.fingerprint,
            )
            if getattr(reservation, "execute", None) is True:
                attempt_id = getattr(reservation, "research_run_attempt_id", None)
                if (
                    isinstance(attempt_id, bool)
                    or not isinstance(attempt_id, int)
                    or attempt_id <= 0
                ):
                    raise BarResearchRunError(
                        "executable reservation returned an invalid identifier"
                    )
                # This is deliberately the first action after a successful
                # executable reservation response.  Every later validation or
                # start failure can therefore abort a durable QUEUED/RUNNING row.
                active_attempts[candidate.candidate_key] = _AttemptCleanupIdentity(
                    research_run_attempt_id=attempt_id,
                    run_fingerprint=spec.fingerprint,
                )
            _validate_reservation(
                reservation,
                research_run_spec_id=spec_registration_ids[candidate.candidate_key],
            )
            reservation_bindings[candidate.candidate_key] = (spec, reservation)
            if reservation.execute:
                state = active.start_attempt(
                    database_url,
                    research_run_attempt_id=reservation.research_run_attempt_id,
                )
                if (
                    getattr(state, "status", None) != "RUNNING"
                    or getattr(state, "research_run_attempt_id", None)
                    != reservation.research_run_attempt_id
                    or getattr(state, "research_run_spec_id", None)
                    != spec_registration_ids[candidate.candidate_key]
                    or getattr(state, "attempt_number", None) != reservation.attempt_number
                ):
                    raise BarResearchRunError(
                        "started attempt did not return its exact RUNNING identity"
                    )
            _notify(
                progress,
                "ATTEMPT_PREREGISTERED",
                index,
                len(specs),
                candidate_key=candidate.candidate_key,
            )

        duplicate_keys = tuple(
            key for key in prepared.candidate_keys if not reservation_bindings[key][1].execute
        )
        reused_report = None
        reports: list[BarCandidateRunReport] = []
        if duplicate_keys:
            reused_report = active.validate_reused_attempts(
                database_url,
                prepared.project_root,
                config=prepared.config,
                split_plan=prepared.split_plan,
                reservations=reservation_bindings,
                raw_source_manifest_sha256=prepared.dataset.source_manifest_sha256,
                bar_dataset_manifest_sha256=prepared.dataset.dataset_manifest_sha256,
            )
            reports.extend(
                _validate_reuse_report(
                    reused_report,
                    prepared=prepared,
                    spec_by_key=spec_by_key,
                    reservation_bindings=reservation_bindings,
                    expected_keys=duplicate_keys,
                    completion_view=False,
                )
            )
            _notify(
                progress,
                "REUSED_ATTEMPTS_VALIDATED",
                len(duplicate_keys),
                len(duplicate_keys),
            )

        if not active_attempts:
            if reused_report is None or len(reports) != ALLOCATED_VARIANT_COUNT:
                raise BarResearchRunError("all-duplicate run lacks a complete validated consensus")
            _assert_provenance_unchanged(prepared, provenance, database_url, active)
            return BarResearchRunReport(
                mode=mode,
                disposition="SKIPPED_DUPLICATE",
                plan=prepared,
                candidate_runs=tuple(reports),
                discovery_result_sha256=reused_report.global_result_artifact_sha256,
                global_result_artifact_identity_sha256=(
                    reused_report.global_result_artifact_identity_sha256
                ),
                evidence_manifest_sha256=reused_report.evidence_manifest_sha256,
                finalist_keys=tuple(reused_report.finalist_keys),
                final_label_counts=tuple(reused_report.final_label_counts),
            )

        _assert_provenance_unchanged(prepared, provenance, database_url, active)
        # This is the first outcome-observing call.  Every executable candidate
        # is already represented by a RUNNING attempt at this point.
        discovery_result = active.run_discovery(
            prepared.dataset,
            split_plan=prepared.split_plan,
            data_root=prepared.data_root,
            candidates=prepared.config.candidates,
            progress=lambda item: _notify(
                progress,
                "DISCOVERY",
                item.completed_active_dates,
                item.total_active_dates,
                discovery=item,
            ),
        )
        active.validate_discovery(discovery_result, prepared=prepared)
        _assert_provenance_unchanged(prepared, provenance, database_url, active)
        global_result_artifact = active.publish_global_result(
            prepared.project_root,
            discovery_result,
            prepared=prepared,
        )
        active.verify_artifact(prepared.project_root, global_result_artifact)
        if reused_report is not None:
            _assert_result_consensus(
                reused_report,
                discovery_result=discovery_result,
                global_result_artifact=global_result_artifact,
            )
        evidence_artifact_count = _register_evidence(
            database_url,
            prepared,
            discovery_result,
            active,
            progress,
        )
        active.register_artifact(
            database_url,
            prepared.project_root,
            global_result_artifact,
        )
        # Global/evidence streaming and registration may be lengthy.  Recheck
        # the complete source/runtime/database contract at the exact boundary
        # before emitting the first candidate terminal artifact.
        _assert_provenance_unchanged(prepared, provenance, database_url, active)
        lineage = _evidence_lineage(
            discovery_result,
            prepared,
            provenance,
            global_result_artifact,
        )
        candidate_results = {
            item.candidate.candidate_key: item for item in discovery_result.candidate_results
        }
        published: dict[str, tuple[Any, ...]] = {}
        executable_keys = tuple(active_attempts)
        for index, candidate_key in enumerate(executable_keys, start=1):
            candidate_result = candidate_results[candidate_key]
            trial_status, decision_label, compact, full = _terminal_payloads(
                candidate_result,
                lineage=lineage,
            )
            spec = spec_by_key[candidate_key]
            artifact = active.publish_terminal(
                prepared.project_root,
                prepared.config,
                candidate_key=candidate_key,
                raw_source_manifest_sha256=prepared.dataset.source_manifest_sha256,
                bar_dataset_manifest_sha256=prepared.dataset.dataset_manifest_sha256,
                split_plan_sha256=prepared.split_plan.sha256,
                run_fingerprint=spec.fingerprint,
                trial_status=trial_status,
                decision_label=decision_label,
                compact_result=compact,
                candidate_result=full,
            )
            published[candidate_key] = (
                trial_status,
                decision_label,
                compact,
                artifact,
                candidate_result.final_label,
            )
            _notify(
                progress,
                "TERMINAL_PUBLISHED",
                index,
                len(executable_keys),
                candidate_key=candidate_key,
            )

        # Close the publication window: neither source bytes nor runtime may
        # change while the 216 immutable terminal documents are emitted.
        _assert_provenance_unchanged(prepared, provenance, database_url, active)
        for index, candidate_key in enumerate(executable_keys, start=1):
            # Every terminal commit is a separate immutable database boundary.
            # Rebuild provenance and re-read schema migrations before each one
            # so a mid-batch code/config/database change cannot be partly blessed.
            _assert_provenance_unchanged(prepared, provenance, database_url, active)
            trial_status, decision_label, compact, artifact, final_label = published[candidate_key]
            spec = spec_by_key[candidate_key]
            attempt_id = active_attempts[candidate_key].research_run_attempt_id
            terminal = BarTerminalResult(
                candidate_key=candidate_key,
                candidate_definition_sha256=prepared.config.candidate(
                    candidate_key
                ).definition_sha256,
                run_fingerprint=spec.fingerprint,
                research_run_attempt_id=attempt_id,
                trial_status=trial_status,
                decision_label=decision_label,
                compact_result=compact,
                artifact=artifact,
            )
            terminal_registration = active.register_terminal(
                database_url,
                prepared.project_root,
                config=prepared.config,
                result=terminal,
            )
            _validate_terminal_registration(
                terminal_registration,
                research_run_spec_id=spec_registration_ids[candidate_key],
                result=terminal,
            )
            del active_attempts[candidate_key]
            reports.append(
                BarCandidateRunReport(
                    candidate_key=candidate_key,
                    run_fingerprint=spec.fingerprint,
                    research_run_attempt_id=attempt_id,
                    disposition="TERMINAL_REGISTERED",
                    final_label=final_label,
                    trial_status=trial_status,
                    terminal_artifact_sha256=artifact.sha256,
                )
            )
            _notify(
                progress,
                "TERMINAL_REGISTERED",
                index,
                len(executable_keys),
                candidate_key=candidate_key,
            )
        _assert_provenance_unchanged(prepared, provenance, database_url, active)
        completion_report = active.validate_completed_campaign(
            database_url,
            prepared.project_root,
            config=prepared.config,
            split_plan=prepared.split_plan,
            run_specs=spec_by_key,
            raw_source_manifest_sha256=prepared.dataset.source_manifest_sha256,
            bar_dataset_manifest_sha256=prepared.dataset.dataset_manifest_sha256,
        )
        completed_candidates = _validate_reuse_report(
            completion_report,
            prepared=prepared,
            spec_by_key=spec_by_key,
            reservation_bindings=reservation_bindings,
            expected_keys=prepared.candidate_keys,
            completion_view=True,
        )
        _assert_result_consensus(
            completion_report,
            discovery_result=discovery_result,
            global_result_artifact=global_result_artifact,
        )
        completed_by_key = {item.candidate_key: item for item in completed_candidates}
        for item in reports:
            completed = completed_by_key[item.candidate_key]
            if (
                completed.run_fingerprint != item.run_fingerprint
                or completed.final_label != item.final_label
                or completed.trial_status != item.trial_status
                or completed.terminal_artifact_sha256 != item.terminal_artifact_sha256
            ):
                raise BarResearchRunError("aggregate completion differs from this run")
        _assert_provenance_unchanged(prepared, provenance, database_url, active)
        _notify(
            progress,
            "CAMPAIGN_COMPLETION_VALIDATED",
            ALLOCATED_VARIANT_COUNT,
            ALLOCATED_VARIANT_COUNT,
        )
        ordered_reports = tuple(
            sorted(reports, key=lambda item: prepared.candidate_keys.index(item.candidate_key))
        )
        return BarResearchRunReport(
            mode=mode,
            disposition="COMPLETED",
            plan=prepared,
            candidate_runs=ordered_reports,
            discovery_result_sha256=completion_report.global_result_artifact_sha256,
            evidence_manifest_sha256=completion_report.evidence_manifest_sha256,
            finalist_keys=tuple(completion_report.finalist_keys),
            final_label_counts=tuple(completion_report.final_label_counts),
            evidence_artifact_count=evidence_artifact_count,
            global_result_artifact_identity_sha256=(
                completion_report.global_result_artifact_identity_sha256
            ),
        )
    except BaseException as error:
        cleanup_failures = _fail_running(database_url, active_attempts, error, active)
        suffix = "" if not cleanup_failures else f"; cleanup failures={cleanup_failures}"
        raise BarResearchRunError(f"governed bar Discovery failed{suffix}") from error


def run_governed_bar_pattern_discovery(
    project_root: Path | str,
    dataset: LoadedBarDatasetManifest,
    *,
    mode: RunMode = "PLAN_ONLY",
    database_url: str | None = None,
    services: BarResearchRunServices | None = None,
    progress: Callable[[BarResearchRunProgress], None] | None = None,
) -> BarResearchRunReport:
    """Prepare and optionally execute the complete governed Discovery program."""

    active = services or _default_services()
    prepared = prepare_bar_research_run(project_root, dataset, services=active)
    return execute_prepared_bar_research_run(
        prepared,
        mode=mode,
        database_url=database_url,
        services=active,
        progress=progress,
    )
