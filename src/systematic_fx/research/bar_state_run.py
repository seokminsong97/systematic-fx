"""Governed orchestration for State-Conditional Bar Model v2 Discovery.

The public runner has two hard boundaries:

* planning is read-only and never captures/publishes provenance; and
* execution pre-registers and binds all twelve candidate RunSpecs before the
  first label or economic outcome can be computed.

Only the visible Discovery decision interval and its twenty-day outcome tail
are handed to the computation engine.  Progress events deliberately contain
stage/count/RSS only; partial fold labels, scores, or economics are never
returned through the operator channel.
"""

from __future__ import annotations

import re
import subprocess
import tempfile
from collections import Counter, defaultdict
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from fractions import Fraction
from itertools import pairwise
from pathlib import Path
from typing import Any, BinaryIO, Final, Literal, Protocol

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg.rows import dict_row

from systematic_fx.db.bar_registry import register_published_bar_artifact
from systematic_fx.db.bar_state_registry import (
    BAR_STATE_COST_VERSION,
    BAR_STATE_ELIGIBLE_CALENDAR_VERSION,
    BAR_STATE_EXECUTION_VERSION,
    BAR_STATE_FEATURE_VERSION,
    BAR_STATE_OUTCOME_VERSION,
    BAR_STATE_SPLIT_VERSION,
    BarStateRegistryDefinition,
    BarStateReuseValidationReport,
    abort_bar_state_run_attempt,
    build_bar_state_registration_document,
    candidate_trial_parameters,
    register_bar_state_artifact_link,
    register_bar_state_campaign,
    register_bar_state_run_spec,
    register_terminal_bar_state_result,
    require_clean_bar_state_predecessor,
    validate_reused_bar_state_attempt,
)
from systematic_fx.db.migrations import discover_migrations
from systematic_fx.db.run_registry import reserve_run_attempt, start_run_attempt
from systematic_fx.features.bars import TradeBar, load_trade_bar_artifact
from systematic_fx.research.bar_artifacts import PublishedBarArtifact
from systematic_fx.research.bar_pipeline import (
    BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
    LoadedBarDatasetManifest,
    load_bar_dataset_manifest,
)
from systematic_fx.research.bar_state_artifacts import (
    BAR_STATE_ARTIFACT_SCHEMA_BY_KIND,
    BAR_STATE_BAR_DATASET_MANIFEST_SHA256,
    BAR_STATE_RAW_SOURCE_MANIFEST_SHA256,
    BarStateArtifactError,
    BarStateArtifactLineage,
    bar_state_global_result_projection,
    bar_state_model_package_projection,
    bar_state_price_policy_from_selection,
    bar_state_terminal_compact_summary,
    frozen_bar_state_discovery_scope,
    ordered_parent_artifacts,
    publish_bar_state_json,
    publish_bar_state_parquet,
    publish_bar_state_parquet_open_file,
    validate_bar_state_global_bootstrap,
)
from systematic_fx.research.bar_state_config import (
    BAR_STATE_V2_PROFILE,
    BarStateCampaignProfile,
    BarStateResearchConfig,
    load_bar_state_config,
    require_bar_state_campaign_profile,
)
from systematic_fx.research.bar_state_features import (
    BarStateFeatureRow,
    BarStateFeatureSpec,
    iter_bar_state_features,
)
from systematic_fx.research.bar_state_labels import (
    MAX_ONE_SECOND_PATH_ROWS,
    BarStateLabel,
    IncompleteStateLabelHorizon,
    StateOneSecondPathIndex,
    StateVerifiedEntryBar,
    label_bar_state_feature,
)
from systematic_fx.research.bar_state_model import (
    BarStateModelError,
    BarStateModelHyperparameters,
    CanonicalBarStateModel,
    fit_bar_state_model,
)
from systematic_fx.research.bar_state_portfolio import (
    StatePortfolioPathSource,
    StatePortfolioSignal,
    StateTradeRecord,
    stream_state_portfolio,
)
from systematic_fx.research.bar_state_selection import (
    STATE_SELECTION_SCHEMA,
    StateCandidateSelection,
    StateCellMultiplicityResult,
    StateFoldEvaluationCalendar,
    select_state_finalists,
    summarize_candidate_support,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.provenance import (
    CodeSnapshot,
    build_code_snapshot,
    dependency_lock_sha256,
    runtime_environment,
)
from systematic_fx.research.run_spec import RunSpec
from systematic_fx.validation.bar_splits import BarSplitPlan
from systematic_fx.validation.bar_state_splits import (
    BAR_STATE_FROZEN_BOOTSTRAP_EVALUATION_CALENDAR_SHA256,
    BarStateSplitPlan,
    frozen_bar_state_bootstrap_evaluation_calendar,
    plan_bar_state_splits,
    require_frozen_bar_state_split,
)

BAR_STATE_RUN_SCHEMA: Final = "systematic_fx.bar_state_research_run.v1"
BAR_STATE_DATASET_MANIFEST_RELATIVE_PATH: Final = Path(
    "data/derived/bar_patterns/trade_bar_dataset_manifest/"
    "identity_sha256=fe038f67b69235c0d56064ffa34379f073087fcfda63fe4cda3fa2d9ec89cb44/"
    "sha256=e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc.json"
)
BAR_STATE_EXPECTED_DATASET_HANDOFF_SHA256: Final = (
    "26b1bb96f7323cae13bbe5d670c12f3e85615bbb9aab56932ce6523e67af7b00"
)
BAR_STATE_SUPPORTED_MIGRATIONS: Final = tuple(range(1, 29))
BAR_STATE_RANDOM_SEED: Final = 20_260_809
BAR_STATE_DISCOVERY_ONE_SECOND_ROW_COUNT: Final = 7_573_041
BAR_STATE_DISCOVERY_OUTCOME_SPAN_COUNT: Final = 10
BAR_STATE_TRADE_SPOOL_BATCH_ROWS: Final = 4_096

RunMode = Literal["PLAN_ONLY", "RUN"]

_SHA256 = re.compile(r"[0-9a-f]{64}")
_GIT_OBJECT_ID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")


class BarStateRunError(RuntimeError):
    """The v2 plan, provenance, orchestration, or computation is unsafe."""


def _model_hyperparameters(
    profile: BarStateCampaignProfile,
) -> BarStateModelHyperparameters:
    selected = require_bar_state_campaign_profile(profile)
    return BarStateModelHyperparameters(max_iter=selected.model_max_iter)


class BarStateEngine(Protocol):
    """Production computation boundary implemented by the streaming engine."""

    def __call__(
        self,
        prepared: PreparedBarStateRun,
        *,
        candidate_keys: tuple[str, ...],
        progress: Callable[[BarStateRunProgress], None] | None,
    ) -> BarStateEngineResult: ...


@dataclass(slots=True)
class BarStateParquetPayload:
    """One bounded table or caller-held streamed Parquet result."""

    artifact_key_suffix: str
    split_key: str
    shard_ordinal: int
    logical_identity: Mapping[str, object]
    table: pa.Table | None = None
    source: BinaryIO | None = None
    row_count: int | None = None
    schema: pa.Schema | None = None

    def __post_init__(self) -> None:
        if bool(self.table is not None) == bool(self.source is not None):
            raise BarStateRunError("Parquet payload requires exactly one storage form")
        if self.split_key not in {
            "discovery",
            "discovery_inner_1",
            "discovery_inner_2",
            "discovery_inner_3",
        }:
            raise BarStateRunError("Parquet payload is outside Discovery")
        if (
            isinstance(self.shard_ordinal, bool)
            or not isinstance(self.shard_ordinal, int)
            or self.shard_ordinal < 0
        ):
            raise BarStateRunError("Parquet payload shard ordinal is invalid")
        if self.table is not None:
            if self.row_count is not None or self.schema is not None:
                raise BarStateRunError("table payload cannot override row count/schema")
        elif (
            isinstance(self.row_count, bool)
            or not isinstance(self.row_count, int)
            or self.row_count < 0
            or not isinstance(self.schema, pa.Schema)
        ):
            raise BarStateRunError("streamed payload requires exact row count/schema")

    def close(self) -> None:
        if self.source is not None:
            self.source.close()


@dataclass(frozen=True, slots=True)
class PreparedBarStateRun:
    project_root: Path
    data_root: Path
    dataset: LoadedBarDatasetManifest
    config: BarStateResearchConfig
    outer_split_plan: BarSplitPlan
    split_plan: BarStateSplitPlan
    registry_definition: BarStateRegistryDefinition

    @property
    def profile(self) -> BarStateCampaignProfile:
        return self.config.profile

    @property
    def candidate_keys(self) -> tuple[str, ...]:
        return tuple(item.candidate_key for item in self.config.candidates)

    @property
    def discovery_partitions(self) -> tuple[Any, ...]:
        """Return only ordinals 1..489, including the label outcome tail."""

        scope = frozen_bar_state_discovery_scope()
        return self.dataset.partitions[
            scope.start_active_ordinal - 1 : scope.outcome_end_active_ordinal
        ]

    @property
    def discovery_decision_dates(self) -> tuple[Any, ...]:
        scope = frozen_bar_state_discovery_scope()
        return self.dataset.eligible_active_dates[
            scope.start_active_ordinal - 1 : scope.decision_end_active_ordinal
        ]

    @property
    def discovery_active_dates(self) -> tuple[Any, ...]:
        scope = frozen_bar_state_discovery_scope()
        return self.dataset.eligible_active_dates[
            scope.start_active_ordinal - 1 : scope.outcome_end_active_ordinal
        ]

    def as_dict(self) -> dict[str, object]:
        return {
            "authorized_stage": "DISCOVERY_ONLY",
            "bar_dataset_manifest_sha256": self.dataset.dataset_manifest_sha256,
            "candidate_catalog_sha256": self.config.candidate_catalog_sha256,
            "candidate_count": len(self.config.candidates),
            "config_file_sha256": self.config.sha256,
            "config_semantic_sha256": self.config.semantic_sha256,
            "dataset_handoff_sha256": self.dataset.handoff_sha256,
            "discovery_partition_count": len(self.discovery_partitions),
            "discovery_scope": frozen_bar_state_discovery_scope().as_dict(),
            "nested_split_sha256": self.split_plan.sha256,
            "outer_split_sha256": self.outer_split_plan.sha256,
            "raw_source_manifest_sha256": self.dataset.source_manifest_sha256,
            "schema": BAR_STATE_RUN_SCHEMA,
        }


@dataclass(frozen=True, slots=True)
class BarStateRunProvenance:
    code_commit: str
    snapshot: CodeSnapshot
    dependency_lock_sha256: str
    runtime_environment: Mapping[str, object]
    runtime_environment_sha256: str
    postgres_migrations_sha256: str
    code_snapshot_artifact: PublishedBarArtifact | None = None


@dataclass(frozen=True, slots=True)
class BarStateRunProgress:
    stage: str
    completed: int
    total: int
    rss_bytes: int

    def as_dict(self) -> dict[str, object]:
        return {
            "completed": self.completed,
            "rss_bytes": self.rss_bytes,
            "stage": self.stage,
            "total": self.total,
        }


@dataclass(frozen=True, slots=True)
class BarStateCandidateEngineArtifacts:
    """Candidate-specific immutable output contract returned by the engine."""

    candidate_key: str
    model_documents: tuple[Mapping[str, object], ...]
    oos_trade_tables: tuple[BarStateParquetPayload, ...]
    terminal_document: Mapping[str, object]
    decision_label: str
    trial_status: str
    compact_summary: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class BarStateEngineResult:
    """Complete result returned only after all folds and candidates finish."""

    feature_tables: tuple[BarStateParquetPayload, ...]
    label_tables: tuple[BarStateParquetPayload, ...]
    candidate_results: tuple[BarStateCandidateEngineArtifacts, ...]
    global_document: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class BarStateCandidateRunReport:
    candidate_key: str
    run_fingerprint: str
    research_run_attempt_id: int | None
    disposition: str
    terminal_artifact_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class BarStateResearchRunReport:
    mode: RunMode
    disposition: str
    plan: PreparedBarStateRun
    candidate_runs: tuple[BarStateCandidateRunReport, ...] = ()
    global_result_sha256: str | None = None
    finalist_keys: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_runs": [
                {
                    "candidate_key": item.candidate_key,
                    "disposition": item.disposition,
                    "research_run_attempt_id": item.research_run_attempt_id,
                    "run_fingerprint": item.run_fingerprint,
                    "terminal_artifact_sha256": item.terminal_artifact_sha256,
                }
                for item in self.candidate_runs
            ],
            "disposition": self.disposition,
            "finalist_keys": list(self.finalist_keys),
            "global_result_sha256": self.global_result_sha256,
            "mode": self.mode,
            "plan": self.plan.as_dict(),
            "schema": BAR_STATE_RUN_SCHEMA,
        }


@dataclass(frozen=True, slots=True)
class BarStateRunServices:
    load_config: Callable[..., BarStateResearchConfig]
    plan_splits: Callable[[Sequence[Any]], BarStateSplitPlan]
    git_head: Callable[[Path], str]
    build_snapshot: Callable[..., CodeSnapshot]
    dependency_hash: Callable[[Path], str]
    runtime: Callable[[], dict[str, object]]
    postgres_runtime: Callable[..., dict[str, object]]
    require_predecessor: Callable[..., Any]
    register_artifact: Callable[..., Any]
    register_campaign: Callable[..., Any]
    register_spec: Callable[..., Any]
    reserve_attempt: Callable[..., Any]
    start_attempt: Callable[..., Any]
    validate_duplicate: Callable[..., Any]
    abort_attempt: Callable[..., Any]
    link_artifact: Callable[..., Any]
    terminalize: Callable[..., Any]
    publish_json: Callable[..., PublishedBarArtifact]
    publish_parquet: Callable[..., PublishedBarArtifact]
    publish_parquet_file: Callable[..., PublishedBarArtifact]
    engine: BarStateEngine


def _rss_bytes() -> int:
    try:
        import resource

        value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        return value if __import__("sys").platform == "darwin" else value * 1024
    except (ImportError, OSError, ValueError):
        return 0


def _notify(
    callback: Callable[[BarStateRunProgress], None] | None,
    *,
    stage: str,
    completed: int,
    total: int,
) -> None:
    if callback is not None:
        callback(BarStateRunProgress(stage, completed, total, _rss_bytes()))


def _strict_project_root(value: Path | str) -> tuple[Path, Path]:
    requested = Path(value).expanduser()
    if requested.is_symlink():
        raise BarStateRunError("project_root cannot be a symbolic link")
    try:
        root = requested.resolve(strict=True)
    except OSError as error:
        raise BarStateRunError("project_root does not exist") from error
    data = root / "data"
    if not root.is_dir() or data.is_symlink() or not data.is_dir():
        raise BarStateRunError("project root requires a real data/ directory")
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
        raise BarStateRunError("cannot resolve Git HEAD") from error
    value = completed.stdout.strip()
    if _GIT_OBJECT_ID.fullmatch(value) is None:
        raise BarStateRunError("Git HEAD is not a full lowercase object ID")
    return value


def _postgres_runtime(database_url: str, *, migrations_directory: Path) -> dict[str, object]:
    migrations = discover_migrations(migrations_directory)
    if tuple(item.version for item in migrations) != BAR_STATE_SUPPORTED_MIGRATIONS:
        raise BarStateRunError("bar-state Discovery requires migrations 0001 through 0028")
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
        raise BarStateRunError("PostgreSQL migration identity drift")
    return {
        "schema_migrations": observed,
        "schema_migrations_sha256": canonical_sha256(observed),
        "server_version": str(version["version"]),
        "server_version_num": str(version["version_num"]),
    }


def _registry_definition(
    config: BarStateResearchConfig,
    split_plan: BarStateSplitPlan,
) -> BarStateRegistryDefinition:
    return BarStateRegistryDefinition(
        config_file_sha256=config.sha256,
        config_semantic_sha256=config.semantic_sha256,
        campaign_definition=config.as_dict(),
        campaign_definition_sha256=config.definition_sha256,
        candidate_catalog_sha256=config.candidate_catalog_sha256,
        training_plan=split_plan.as_dict(),
        training_plan_sha256=split_plan.sha256,
        candidates=tuple(item.as_dict() for item in config.candidates),
        profile=config.profile,
    )


def prepare_bar_state_run(
    project_root: Path | str,
    dataset: LoadedBarDatasetManifest,
    *,
    profile: BarStateCampaignProfile = BAR_STATE_V2_PROFILE,
    services: BarStateRunServices | None = None,
) -> PreparedBarStateRun:
    """Build the outcome-free v2 plan and prove its Discovery-only slice."""

    selected_profile = require_bar_state_campaign_profile(profile)
    root, data = _strict_project_root(project_root)
    active = services or _default_services()
    if not isinstance(dataset, LoadedBarDatasetManifest):
        raise BarStateRunError("dataset must be a verified LoadedBarDatasetManifest")
    if (
        dataset.dataset_manifest_sha256 != BAR_STATE_BAR_DATASET_MANIFEST_SHA256
        or dataset.source_manifest_sha256 != BAR_STATE_RAW_SOURCE_MANIFEST_SHA256
        or dataset.outcome_span_policy_sha256 != BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256
        or dataset.handoff_sha256 != BAR_STATE_EXPECTED_DATASET_HANDOFF_SHA256
    ):
        raise BarStateRunError("verified bar dataset lineage differs from v2")
    config = active.load_config(root, profile=selected_profile)
    if config.profile != selected_profile:
        raise BarStateRunError("loaded config belongs to another campaign profile")
    split = active.plan_splits(dataset.eligible_active_dates)
    require_frozen_bar_state_split(split)
    if split.outer_plan.sha256 != frozen_bar_state_discovery_scope().split_plan_sha256:
        raise BarStateRunError("outer split differs from the frozen Discovery scope")
    if len(config.candidates) != 12:
        raise BarStateRunError("bar-state config must contain exactly 12 candidates")
    prepared = PreparedBarStateRun(
        project_root=root,
        data_root=data,
        dataset=dataset,
        config=config,
        outer_split_plan=split.outer_plan,
        split_plan=split,
        registry_definition=_registry_definition(config, split),
    )
    scope = frozen_bar_state_discovery_scope()
    if (
        len(prepared.discovery_partitions) != scope.outcome_end_active_ordinal
        or prepared.discovery_active_dates[0].isoformat() != scope.start_date
        or prepared.discovery_active_dates[-1].isoformat() != scope.outcome_end_date
        or prepared.discovery_decision_dates[-1].isoformat() != scope.decision_end_date
    ):
        raise BarStateRunError("Discovery partition slice is incomplete or overreaching")
    return prepared


def load_prepared_bar_state_run(
    project_root: Path | str,
    *,
    manifest_path: Path | None = None,
    profile: BarStateCampaignProfile = BAR_STATE_V2_PROFILE,
    services: BarStateRunServices | None = None,
) -> PreparedBarStateRun:
    """Load the actual held-file-verified bar manifest, then prepare v2."""

    root, _ = _strict_project_root(project_root)
    requested = manifest_path or root / BAR_STATE_DATASET_MANIFEST_RELATIVE_PATH
    dataset = load_bar_dataset_manifest(
        requested,
        expected_sha256=BAR_STATE_BAR_DATASET_MANIFEST_SHA256,
    )
    return prepare_bar_state_run(root, dataset, profile=profile, services=services)


def _policy_documents(candidate: Mapping[str, object]) -> dict[str, dict[str, object]]:
    feature = dict(candidate["feature_policy"])  # type: ignore[arg-type]
    label = dict(candidate["label_policy"])  # type: ignore[arg-type]
    cost = dict(candidate["cost_model"])  # type: ignore[arg-type]
    entry = dict(candidate["entry_policy"])  # type: ignore[arg-type]
    barrier = dict(candidate["economic_barrier_policy"])  # type: ignore[arg-type]
    prediction = dict(candidate["prediction_policy"])  # type: ignore[arg-type]
    signal = {
        "authorized_stage": "DISCOVERY_ONLY",
        "candidate_key": candidate["candidate_key"],
        "feature_policy": feature,
        "prediction_policy": prediction,
        "schema": "systematic_fx.bar_state_signal_policy.v1",
    }
    execution = {
        "economic_barrier_policy": barrier,
        "entry_policy": entry,
        "prediction_policy": prediction,
    }
    terminal = {
        "authorized_stage": "DISCOVERY_ONLY",
        "boundary_event_ordering": label["boundary_event_ordering"],
        "contract_boundary_policy": label["contract_boundary_policy"],
        "observation_horizon_active_days": label["observation_horizon_active_days"],
        "quality_boundary_policy": label["quality_boundary_policy"],
        "split_boundary_policy": "DISCOVERY_OUTCOME_TAIL_END",
    }
    return {
        "barrier": barrier,
        "cost": cost,
        "entry": entry,
        "execution": execution,
        "feature": feature,
        "label": label,
        "signal": signal,
        "terminal": terminal,
    }


def build_bar_state_run_specs(
    prepared: PreparedBarStateRun,
    provenance: BarStateRunProvenance,
) -> tuple[RunSpec, ...]:
    """Build twelve complete candidate RunSpecs without database access."""

    calendar_document = {
        "dataset_handoff_sha256": prepared.dataset.handoff_sha256,
        "eligible_active_dates": [
            item.isoformat() for item in prepared.dataset.eligible_active_dates
        ],
        "schema": "systematic_fx.bar_state_eligible_calendar.v1",
    }
    calendar_sha256 = canonical_sha256(calendar_document)
    specs: list[RunSpec] = []
    for candidate_object in prepared.config.candidates:
        candidate = candidate_object.as_dict()
        policies = _policy_documents(candidate)
        trial_parameters = candidate_trial_parameters(
            prepared.registry_definition,
            candidate_object.candidate_key,
            split_plan=prepared.outer_split_plan,
        )
        specs.append(
            RunSpec(
                campaign_id=prepared.profile.campaign_key,
                experiment_id=prepared.profile.experiment_key,
                run_kind="MODEL_FIT",
                engine_version=prepared.profile.engine_version,
                source_manifest_hashes={
                    "raw_mbp10_source_manifest_v1": prepared.dataset.source_manifest_sha256,
                    "selected_trade_bar_dataset_manifest_v1": (
                        prepared.dataset.dataset_manifest_sha256
                    ),
                },
                eligible_calendar_version=BAR_STATE_ELIGIBLE_CALENDAR_VERSION,
                eligible_calendar_sha256=calendar_sha256,
                split_version=BAR_STATE_SPLIT_VERSION,
                split_sha256=prepared.split_plan.sha256,
                feature_version=BAR_STATE_FEATURE_VERSION,
                feature_sha256=canonical_sha256(policies["feature"]),
                outcome_version=BAR_STATE_OUTCOME_VERSION,
                outcome_sha256=canonical_sha256(policies["label"]),
                cost_version=BAR_STATE_COST_VERSION,
                cost_sha256=canonical_sha256(policies["cost"]),
                execution_version=BAR_STATE_EXECUTION_VERSION,
                execution_sha256=canonical_sha256(policies["execution"]),
                code_commit=provenance.code_commit,
                code_snapshot_sha256=provenance.snapshot.sha256,
                dependency_lock_sha256=provenance.dependency_lock_sha256,
                runtime_environment=provenance.runtime_environment,
                random_seed=BAR_STATE_RANDOM_SEED,
                direction="BOTH",
                signal_policy=policies["signal"],
                entry_policy=policies["entry"],
                barrier_policy=policies["barrier"],
                terminal_policy=policies["terminal"],
                parameters={
                    "authorized_stage": "DISCOVERY_ONLY",
                    "bar_state_candidate_catalog_sha256": (
                        prepared.config.candidate_catalog_sha256
                    ),
                    "bar_state_candidate_definition_sha256": (candidate_object.definition_sha256),
                    "bar_state_candidate_key": candidate_object.candidate_key,
                    "bar_state_config_file_sha256": prepared.config.sha256,
                    "bar_state_config_semantic_sha256": prepared.config.semantic_sha256,
                    "bar_state_discovery_scope_sha256": (frozen_bar_state_discovery_scope().sha256),
                    "bar_state_training_plan_sha256": prepared.split_plan.sha256,
                    "bar_state_trial_parameters_sha256": canonical_sha256(trial_parameters),
                },
            )
        )
    result = tuple(specs)
    if (
        len(result) != 12
        or len({item.fingerprint for item in result}) != 12
        or tuple(item.parameters["bar_state_candidate_key"] for item in result)
        != prepared.candidate_keys
    ):
        raise BarStateRunError("bar-state RunSpecs are incomplete, reordered, or collide")
    return result


def _capture_provenance(
    prepared: PreparedBarStateRun,
    database_url: str,
    services: BarStateRunServices,
) -> BarStateRunProvenance:
    commit = services.git_head(prepared.project_root)
    snapshot = services.build_snapshot(prepared.project_root, code_commit=commit)
    config_matches = tuple(
        item.sha256
        for item in snapshot.files
        if item.relative_path == prepared.profile.config_relative_path.as_posix()
    )
    if config_matches != (prepared.config.sha256,):
        raise BarStateRunError("code snapshot does not bind the loaded v2 config")
    dependency = services.dependency_hash(prepared.project_root)
    postgres = services.postgres_runtime(
        database_url,
        migrations_directory=prepared.project_root / "migrations",
    )
    migration_sha = postgres.get("schema_migrations_sha256")
    if not isinstance(migration_sha, str) or _SHA256.fullmatch(migration_sha) is None:
        raise BarStateRunError("PostgreSQL runtime lacks a migration identity")
    runtime = dict(services.runtime())
    runtime["postgresql"] = postgres
    runtime["bar_state_run"] = {
        "authorized_stage": "DISCOVERY_ONLY",
        "dataset_handoff_sha256": prepared.dataset.handoff_sha256,
        "engine_version": prepared.profile.engine_version,
        "orchestration": "BIND_AND_START_ALL_PENDING_BEFORE_OUTCOMES",
    }
    runtime_sha = canonical_sha256(runtime)
    return BarStateRunProvenance(
        code_commit=commit,
        snapshot=snapshot,
        dependency_lock_sha256=dependency,
        runtime_environment=runtime,
        runtime_environment_sha256=runtime_sha,
        postgres_migrations_sha256=migration_sha,
    )


def _publish_code_snapshot(
    prepared: PreparedBarStateRun,
    provenance: BarStateRunProvenance,
    specs: Sequence[RunSpec],
) -> PublishedBarArtifact:
    lineage = BarStateArtifactLineage(
        config_file_sha256=prepared.config.sha256,
        config_semantic_sha256=prepared.config.semantic_sha256,
        candidate_catalog_sha256=prepared.config.candidate_catalog_sha256,
        training_plan_sha256=prepared.split_plan.sha256,
        code_snapshot_sha256=provenance.snapshot.sha256,
        dependency_lock_sha256=provenance.dependency_lock_sha256,
        runtime_environment_sha256=provenance.runtime_environment_sha256,
        ordered_run_set_sha256=canonical_sha256([item.fingerprint for item in specs]),
        discovery_scope=frozen_bar_state_discovery_scope(),
    )
    artifact = publish_bar_state_json(
        prepared.project_root,
        kind="CODE_SNAPSHOT",
        artifact_key_suffix=provenance.snapshot.sha256,
        document=provenance.snapshot.payload,
        record_count=len(provenance.snapshot.files),
        lineage=lineage,
        logical_identity={
            "code_commit": provenance.code_commit,
            "code_snapshot_sha256": provenance.snapshot.sha256,
        },
        profile=prepared.profile,
    )
    if artifact.sha256 != provenance.snapshot.sha256:
        raise BarStateRunError("published code snapshot bytes differ from capture")
    return artifact


def _publish_registration(
    prepared: PreparedBarStateRun,
    provenance: BarStateRunProvenance,
    specs: Sequence[RunSpec],
    code_artifact: PublishedBarArtifact,
) -> tuple[PublishedBarArtifact, dict[str, object]]:
    fingerprints = tuple(item.fingerprint for item in specs)
    document = build_bar_state_registration_document(
        prepared.registry_definition,
        split_plan=prepared.outer_split_plan,
        code_commit=provenance.code_commit,
        code_snapshot_sha256=provenance.snapshot.sha256,
        dependency_lock_sha256=provenance.dependency_lock_sha256,
        runtime_environment=provenance.runtime_environment,
        ordered_run_fingerprints=fingerprints,
    )
    lineage = BarStateArtifactLineage(
        config_file_sha256=prepared.config.sha256,
        config_semantic_sha256=prepared.config.semantic_sha256,
        candidate_catalog_sha256=prepared.config.candidate_catalog_sha256,
        training_plan_sha256=prepared.split_plan.sha256,
        code_snapshot_sha256=provenance.snapshot.sha256,
        dependency_lock_sha256=provenance.dependency_lock_sha256,
        runtime_environment_sha256=provenance.runtime_environment_sha256,
        ordered_run_set_sha256=canonical_sha256(list(fingerprints)),
        discovery_scope=frozen_bar_state_discovery_scope(),
        parent_artifacts=ordered_parent_artifacts((code_artifact,)),
    )
    artifact = publish_bar_state_json(
        prepared.project_root,
        kind="REGISTRATION",
        artifact_key_suffix=prepared.config.definition_sha256,
        document=document,
        record_count=12,
        lineage=lineage,
        logical_identity={
            "campaign_definition_sha256": prepared.config.definition_sha256,
            "candidate_catalog_sha256": prepared.config.candidate_catalog_sha256,
        },
        profile=prepared.profile,
    )
    return artifact, document


@dataclass(frozen=True, slots=True)
class _OutcomeSpanBar:
    bar: TradeBar
    outcome_span_id: int

    def __getattr__(self, name: str) -> object:
        return getattr(self.bar, name)


def _artifact_for_timeframe(partition: Any, timeframe_seconds: int) -> Any:
    matches = tuple(
        item for item in partition.artifacts if item.timeframe_seconds == timeframe_seconds
    )
    if len(matches) != 1:
        raise BarStateRunError("bar partition lacks one exact requested timeframe")
    return matches[0]


def _load_partition_bars(
    prepared: PreparedBarStateRun,
    partition: Any,
    timeframe_seconds: int,
) -> tuple[TradeBar, ...]:
    if not any(partition is item for item in prepared.discovery_partitions):
        raise BarStateRunError("bar loader cannot open sealed WF/HOLDOUT partitions")
    return load_trade_bar_artifact(
        prepared.data_root,
        _artifact_for_timeframe(partition, timeframe_seconds),
        expected_plan_sha256=partition.plan_sha256,
        expected_source_sha256=partition.source_sha256,
        expected_source_date=partition.source_date,
    )


class _VerifiedOutcomeSpanSource(StatePortfolioPathSource):
    """Load and release exactly one manifest-bound one-second span at a time."""

    def __init__(
        self,
        prepared: PreparedBarStateRun,
        *,
        progress: Callable[[BarStateRunProgress], None] | None,
        stage: str,
    ) -> None:
        self._prepared = prepared
        grouped: dict[int, list[Any]] = defaultdict(list)
        for partition in prepared.discovery_partitions:
            grouped[partition.outcome_span_id].append(partition)
        self._partitions = {key: tuple(value) for key, value in sorted(grouped.items())}
        counts = {
            key: sum(_artifact_for_timeframe(item, 1).row_count for item in values)
            for key, values in self._partitions.items()
        }
        if (
            len(counts) != BAR_STATE_DISCOVERY_OUTCOME_SPAN_COUNT
            or sum(counts.values()) != BAR_STATE_DISCOVERY_ONE_SECOND_ROW_COUNT
            or max(counts.values()) > MAX_ONE_SECOND_PATH_ROWS
        ):
            raise BarStateRunError("one-second outcome-span memory preflight drift")
        self.row_counts = counts
        self._progress = progress
        self._stage = stage
        self._opened = 0
        self._active_path_id: int | None = None

    @contextmanager
    def open_path(self, path_id: int) -> Iterator[StateOneSecondPathIndex]:
        if self._active_path_id is not None:
            raise BarStateRunError("one-second source cannot keep two outcome spans resident")
        try:
            partitions = self._partitions[path_id]
        except KeyError as error:
            raise BarStateRunError("requested outcome span is outside Discovery") from error
        wrapped: list[_OutcomeSpanBar] = []
        for partition in partitions:
            wrapped.extend(
                _OutcomeSpanBar(item, path_id)
                for item in _load_partition_bars(self._prepared, partition, 1)
            )
        if len(wrapped) != self.row_counts[path_id]:
            raise BarStateRunError("loaded outcome span row count differs from manifest")
        path = StateOneSecondPathIndex(wrapped, path_id=path_id)
        del wrapped
        self._active_path_id = path_id
        self._opened += 1
        try:
            _notify(
                self._progress,
                stage=self._stage,
                completed=self._opened,
                total=len(self._partitions),
            )
            yield path
        finally:
            del path
            self._active_path_id = None


def _feature_arrow_schema(
    *,
    profile: BarStateCampaignProfile = BAR_STATE_V2_PROFILE,
) -> pa.Schema:
    selected = require_bar_state_campaign_profile(profile)
    list_child_name = "element" if selected.version_id == "V2B" else "item"
    string_list = pa.list_(pa.field(list_child_name, pa.string()))
    return pa.schema(
        [
            pa.field("feature_set_id", pa.string(), nullable=False),
            pa.field("feature_names", string_list, nullable=False),
            pa.field("timeframe_seconds", pa.int32(), nullable=False),
            pa.field("segment_id", pa.uint64(), nullable=False),
            pa.field("contract", pa.string(), nullable=False),
            pa.field("source_date", pa.date32(), nullable=False),
            pa.field("signal_start_ns", pa.int64(), nullable=False),
            pa.field("decision_ns", pa.int64(), nullable=False),
            pa.field("atr_true_range_sum_ticks", pa.int64(), nullable=False),
            pa.field("volatility_ticks", pa.int32(), nullable=False),
            pa.field("values_hex", string_list, nullable=False),
        ],
        metadata={
            b"systematic_fx.artifact_schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["FEATURE"].encode(
                "ascii"
            ),
            b"systematic_fx.row_schema": b"systematic_fx.bar_state_feature.v1",
        },
    )


def _feature_table(
    rows: Sequence[BarStateFeatureRow],
    *,
    profile: BarStateCampaignProfile = BAR_STATE_V2_PROFILE,
) -> pa.Table:
    records = [
        {
            "atr_true_range_sum_ticks": item.atr_true_range_sum_ticks,
            "contract": item.contract,
            "decision_ns": item.decision_ns,
            "feature_names": list(item.feature_names),
            "feature_set_id": item.feature_set_id,
            "segment_id": item.segment_id,
            "signal_start_ns": item.signal_start_ns,
            "source_date": item.source_date,
            "timeframe_seconds": item.timeframe_seconds,
            "values_hex": [value.hex() for value in item.values],
            "volatility_ticks": item.volatility_ticks,
        }
        for item in rows
    ]
    return pa.Table.from_pylist(records, schema=_feature_arrow_schema(profile=profile))


def _label_arrow_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("label", pa.string(), nullable=False),
            pa.field("censor_reason", pa.string()),
            pa.field("timeframe_seconds", pa.int32(), nullable=False),
            pa.field("segment_id", pa.uint64(), nullable=False),
            pa.field("contract", pa.string(), nullable=False),
            pa.field("signal_start_ns", pa.int64(), nullable=False),
            pa.field("decision_ns", pa.int64(), nullable=False),
            pa.field("entry_path_id", pa.int32(), nullable=False),
            pa.field("entry_path_index", pa.int64(), nullable=False),
            pa.field("entry_signal_bar_start_ns", pa.int64(), nullable=False),
            pa.field("entry_signal_bar_end_ns", pa.int64(), nullable=False),
            pa.field("entry_start_ns", pa.int64(), nullable=False),
            pa.field("entry_price_ticks", pa.int64(), nullable=False),
            pa.field("volatility_ticks", pa.int32(), nullable=False),
            pa.field("upper_barrier_ticks", pa.int64(), nullable=False),
            pa.field("lower_barrier_ticks", pa.int64(), nullable=False),
            pa.field("upper_hit_path_index", pa.int64()),
            pa.field("lower_hit_path_index", pa.int64()),
            pa.field("terminal_path_index", pa.int64(), nullable=False),
            pa.field("terminal_start_ns", pa.int64(), nullable=False),
            pa.field("horizon_start_date", pa.date32(), nullable=False),
            pa.field("horizon_terminal_date", pa.date32(), nullable=False),
            pa.field("path_truncated_before_horizon", pa.bool_(), nullable=False),
        ],
        metadata={
            b"systematic_fx.artifact_schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["LABEL"].encode(
                "ascii"
            ),
            b"systematic_fx.row_schema": b"systematic_fx.bar_state_label.v1",
        },
    )


def _label_table(rows: Sequence[BarStateLabel]) -> pa.Table:
    records = [
        {
            "censor_reason": (None if item.censor_reason is None else item.censor_reason.value),
            "contract": item.contract,
            "decision_ns": item.decision_ns,
            "entry_path_id": item.entry_path_id,
            "entry_path_index": item.entry_path_index,
            "entry_price_ticks": item.entry_price_ticks,
            "entry_signal_bar_end_ns": item.entry_signal_bar_end_ns,
            "entry_signal_bar_start_ns": item.entry_signal_bar_start_ns,
            "entry_start_ns": item.entry_start_ns,
            "horizon_start_date": item.horizon_start_date,
            "horizon_terminal_date": item.horizon_terminal_date,
            "label": item.label.value,
            "lower_barrier_ticks": item.lower_barrier_ticks,
            "lower_hit_path_index": item.lower_hit_path_index,
            "path_truncated_before_horizon": item.path_truncated_before_horizon,
            "segment_id": item.segment_id,
            "signal_start_ns": item.signal_start_ns,
            "terminal_path_index": item.terminal_path_index,
            "terminal_start_ns": item.terminal_start_ns,
            "timeframe_seconds": item.timeframe_seconds,
            "upper_barrier_ticks": item.upper_barrier_ticks,
            "upper_hit_path_index": item.upper_hit_path_index,
            "volatility_ticks": item.volatility_ticks,
        }
        for item in rows
    ]
    return pa.Table.from_pylist(records, schema=_label_arrow_schema())


def _trade_arrow_schema(candidate_key: str) -> pa.Schema:
    multiplier = pa.struct(
        [
            pa.field("denominator", pa.int32(), nullable=False),
            pa.field("numerator", pa.int32(), nullable=False),
        ]
    )
    return pa.schema(
        [
            pa.field("signal_id", pa.string(), nullable=False),
            pa.field("candidate_key", pa.string(), nullable=False),
            pa.field("fold_key", pa.string(), nullable=False),
            pa.field("block_key", pa.string(), nullable=False),
            pa.field("signal_active_date", pa.string(), nullable=False),
            pa.field("entry_active_date", pa.string(), nullable=False),
            pa.field("exit_active_date", pa.string(), nullable=False),
            pa.field("entry_utc_month", pa.string(), nullable=False),
            pa.field("exit_utc_month", pa.string(), nullable=False),
            pa.field("contract", pa.string(), nullable=False),
            pa.field("scenario_id", pa.string(), nullable=False),
            pa.field("direction", pa.string(), nullable=False),
            pa.field("take_profit_multiplier", multiplier, nullable=False),
            pa.field("stop_loss_multiplier", multiplier, nullable=False),
            pa.field("take_profit_ticks", pa.int32(), nullable=False),
            pa.field("stop_loss_ticks", pa.int32(), nullable=False),
            pa.field("entry_path_index", pa.int64(), nullable=False),
            pa.field("exit_path_index", pa.int64(), nullable=False),
            pa.field("entry_fill_price_ticks", pa.int64(), nullable=False),
            pa.field("exit_fill_price_ticks", pa.int64(), nullable=False),
            pa.field("buying_price_ticks", pa.int64(), nullable=False),
            pa.field("selling_price_ticks", pa.int64(), nullable=False),
            pa.field("take_profit_target_price_ticks", pa.int64(), nullable=False),
            pa.field("loss_trigger_price_ticks", pa.int64(), nullable=False),
            pa.field("outcome", pa.string(), nullable=False),
            pa.field("same_second_stop_first", pa.bool_(), nullable=False),
            pa.field("gross_pnl_ticks", pa.int64(), nullable=False),
            pa.field("variable_cost_ticks", pa.int32(), nullable=False),
            pa.field("allocated_fixed_cost_ticks", pa.int32(), nullable=False),
            pa.field("fully_loaded_net_pnl_ticks", pa.int64(), nullable=False),
        ],
        metadata={
            b"systematic_fx.artifact_schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["OOS_TRADE"].encode(
                "ascii"
            ),
            b"systematic_fx.candidate_key": candidate_key.encode("ascii"),
            b"systematic_fx.row_schema": b"systematic_fx.bar_state_oos_trade.v1",
        },
    )


class _TradeSpool:
    def __init__(self, directory: Path, candidate_key: str) -> None:
        self.candidate_key = candidate_key
        self.schema = _trade_arrow_schema(candidate_key)
        self.source = tempfile.TemporaryFile(  # noqa: SIM115 - returned to publisher
            mode="w+b",
            prefix=f"{candidate_key}_",
            suffix=".parquet",
            dir=directory,
        )
        self.writer = pq.ParquetWriter(
            self.source,
            self.schema,
            compression="zstd",
            use_dictionary=False,
            write_statistics=True,
            version="2.6",
        )
        self.buffer: list[dict[str, object]] = []
        self.row_count = 0
        self.finished = False

    def append(self, record: StateTradeRecord) -> None:
        if record.candidate_key != self.candidate_key or self.finished:
            raise BarStateRunError("trade spool candidate or lifecycle drift")
        self.buffer.append(record.as_dict())
        if len(self.buffer) >= BAR_STATE_TRADE_SPOOL_BATCH_ROWS:
            self._flush()

    def _flush(self) -> None:
        if not self.buffer:
            return
        table = pa.Table.from_pylist(self.buffer, schema=self.schema)
        self.writer.write_table(table, row_group_size=BAR_STATE_TRADE_SPOOL_BATCH_ROWS)
        self.row_count += len(self.buffer)
        self.buffer.clear()

    def finish(self) -> BarStateParquetPayload:
        if self.finished:
            raise BarStateRunError("trade spool was already finalized")
        self._flush()
        self.writer.close()
        self.source.flush()
        self.source.seek(0)
        self.finished = True
        return BarStateParquetPayload(
            artifact_key_suffix=self.candidate_key,
            split_key="discovery",
            shard_ordinal=0,
            logical_identity={
                "candidate_key": self.candidate_key,
                "row_count": self.row_count,
                "row_schema": "systematic_fx.bar_state_oos_trade.v1",
            },
            source=self.source,
            row_count=self.row_count,
            schema=self.schema,
        )

    def close(self) -> None:
        if not self.finished:
            self.writer.close()
        self.source.close()


def _trade_spool_directory(prepared: PreparedBarStateRun) -> Path:
    directory = (
        prepared.data_root
        / "derived"
        / "bar_patterns"
        / "checkpoints"
        / prepared.profile.campaign_key
    )
    directory.mkdir(mode=0o755, parents=True, exist_ok=True)
    if directory.is_symlink() or not directory.is_dir():
        raise BarStateRunError("trade spool directory is unsafe")
    return directory.resolve(strict=True)


def _feature_identity(row: BarStateFeatureRow) -> tuple[str, int, int]:
    return row.contract, row.signal_start_ns, row.decision_ns


def _build_discovery_features(
    prepared: PreparedBarStateRun,
    progress: Callable[[BarStateRunProgress], None] | None,
) -> tuple[
    dict[int, tuple[TradeBar, ...]],
    dict[tuple[int, str], tuple[BarStateFeatureRow, ...]],
    tuple[BarStateParquetPayload, ...],
    dict[tuple[int, str], Mapping[str, int]],
]:
    signal_bars: dict[int, list[TradeBar]] = {300: [], 1_800: []}
    total = len(prepared.discovery_partitions)
    for ordinal, partition in enumerate(prepared.discovery_partitions, start=1):
        for timeframe, bars in signal_bars.items():
            bars.extend(_load_partition_bars(prepared, partition, timeframe))
        if ordinal == 1 or ordinal % 25 == 0 or ordinal == total:
            _notify(progress, stage="SIGNAL_BARS_VERIFIED", completed=ordinal, total=total)
    frozen_bars = {key: tuple(value) for key, value in signal_bars.items()}
    decision_end = prepared.discovery_decision_dates[-1]
    groups: dict[tuple[int, str], tuple[BarStateFeatureRow, ...]] = {}
    feature_qc: dict[tuple[int, str], Mapping[str, int]] = {}
    payloads: list[BarStateParquetPayload] = []
    group_order = (
        (300, "MORPHOLOGY"),
        (300, "STATE"),
        (1_800, "MORPHOLOGY"),
        (1_800, "STATE"),
    )
    for shard, (timeframe, feature_set_id) in enumerate(group_order):
        source_date_by_identity = {
            (item.contract, item.end_ns): item.source_date for item in frozen_bars[timeframe]
        }
        exclusion_counts: Counter[str] = Counter()

        def record_exclusion(
            item: Any,
            *,
            date_by_identity: Mapping[tuple[str, int], date] = (source_date_by_identity),
            counts: Counter[str] = exclusion_counts,
        ) -> None:
            try:
                source_date = date_by_identity[item.contract, item.decision_ns]
            except KeyError as error:
                raise BarStateRunError(
                    "feature exclusion is absent from verified signal bars"
                ) from error
            if source_date <= decision_end:
                counts[item.reason] += 1

        rows = tuple(
            item
            for item in iter_bar_state_features(
                frozen_bars[timeframe],
                spec=BarStateFeatureSpec.frozen(timeframe, feature_set_id),
                completed_30m_bars=frozen_bars[1_800],
                exclusion_sink=record_exclusion,
            )
            if item.source_date <= decision_end
        )
        if not rows:
            raise BarStateRunError("feature extraction returned no Discovery rows")
        groups[timeframe, feature_set_id] = rows
        frozen_exclusions = dict(sorted(exclusion_counts.items()))
        feature_qc[timeframe, feature_set_id] = frozen_exclusions
        payloads.append(
            BarStateParquetPayload(
                artifact_key_suffix=(f"tf{timeframe:04d}_fs{feature_set_id.lower()}_discovery"),
                split_key="discovery",
                shard_ordinal=shard,
                logical_identity={
                    "decision_end_date": decision_end.isoformat(),
                    "exclusion_counts_by_reason": frozen_exclusions,
                    "feature_set_id": feature_set_id,
                    "row_count": len(rows),
                    "timeframe_seconds": timeframe,
                },
                table=_feature_table(rows, profile=prepared.profile),
            )
        )
        _notify(
            progress,
            stage="FEATURE_GROUP_COMPLETE",
            completed=shard + 1,
            total=len(group_order),
        )
    return frozen_bars, groups, tuple(payloads), feature_qc


def _entry_bar_maps(
    signal_bars: Mapping[int, Sequence[TradeBar]],
    *,
    outcome_span_by_date: Mapping[date, int],
) -> dict[int, dict[tuple[str, date, int, int], StateVerifiedEntryBar]]:
    """Link adjacent observed signal bars within one manifest outcome span."""

    result: dict[
        int,
        dict[tuple[str, date, int, int], StateVerifiedEntryBar],
    ] = {}
    for timeframe, bars in signal_bars.items():
        canonical = tuple(
            sorted(
                bars,
                key=lambda item: (
                    item.start_ns,
                    item.end_ns,
                    item.contract,
                    item.segment_id,
                ),
            )
        )
        if tuple(bars) != canonical:
            raise BarStateRunError("signal bars are not in canonical chronological order")
        grouped: dict[tuple[str, int], list[TradeBar]] = defaultdict(list)
        for bar in bars:
            if bar.timeframe_seconds != timeframe:
                raise BarStateRunError("signal bar is stored under the wrong timeframe")
            try:
                outcome_span_id = outcome_span_by_date[bar.source_date]
            except KeyError as error:
                raise BarStateRunError(
                    "signal bar date is outside the manifest outcome spans"
                ) from error
            grouped[bar.contract, outcome_span_id].append(bar)

        mapping: dict[tuple[str, date, int, int], StateVerifiedEntryBar] = {}
        for (contract, outcome_span_id), values in sorted(grouped.items()):
            ordered = tuple(sorted(values, key=lambda item: (item.start_ns, item.end_ns)))
            for predecessor, entry in pairwise(ordered):
                if predecessor.contract != contract or entry.contract != contract:
                    raise BarStateRunError("signal-bar successor group crosses a contract")
                if entry.start_ns < predecessor.end_ns:
                    raise BarStateRunError("observed signal bars overlap")
                predecessor_span = outcome_span_by_date[predecessor.source_date]
                entry_span = outcome_span_by_date[entry.source_date]
                if predecessor_span != outcome_span_id or entry_span != outcome_span_id:
                    raise BarStateRunError("signal-bar successor group crosses an outcome span")
                key = (
                    predecessor.contract,
                    predecessor.source_date,
                    predecessor.start_ns,
                    predecessor.end_ns,
                )
                if key in mapping:
                    raise BarStateRunError("signal bar entry identity is duplicated")
                mapping[key] = StateVerifiedEntryBar.from_adjacent(
                    predecessor,
                    entry,
                    predecessor_outcome_span_id=predecessor_span,
                    entry_outcome_span_id=entry_span,
                )
        result[timeframe] = mapping
    return result


def _fold_terminal_indices(
    prepared: PreparedBarStateRun,
) -> dict[tuple[int, int], int]:
    grouped: dict[int, list[Any]] = defaultdict(list)
    for partition in prepared.discovery_partitions:
        grouped[partition.outcome_span_id].append(partition)
    result: dict[tuple[int, int], int] = {}
    for path_id, partitions in grouped.items():
        for fold in prepared.split_plan.inner_folds:
            rows = sum(
                _artifact_for_timeframe(item, 1).row_count
                for item in partitions
                if item.source_date <= fold.outcome_tail.end_date
            )
            if rows:
                result[path_id, fold.fold_number] = rows - 1
    return result


def _build_discovery_labels(
    prepared: PreparedBarStateRun,
    signal_bars: Mapping[int, Sequence[TradeBar]],
    features: Mapping[tuple[int, str], Sequence[BarStateFeatureRow]],
    progress: Callable[[BarStateRunProgress], None] | None,
) -> tuple[
    dict[tuple[int, str], tuple[BarStateLabel, ...]],
    dict[tuple[int, str], dict[tuple[str, int, int], BarStateLabel]],
    tuple[BarStateParquetPayload, ...],
    dict[tuple[int, int], int],
    dict[str, object],
]:
    path_by_date = {
        item.source_date: item.outcome_span_id for item in prepared.discovery_partitions
    }
    entries = _entry_bar_maps(signal_bars, outcome_span_by_date=path_by_date)
    pending: dict[
        int,
        list[tuple[tuple[int, str], BarStateFeatureRow, StateVerifiedEntryBar]],
    ] = defaultdict(list)
    for group, rows in features.items():
        timeframe, _ = group
        for row in rows:
            entry = entries[timeframe].get(
                (
                    row.contract,
                    row.source_date,
                    row.signal_start_ns,
                    row.decision_ns,
                )
            )
            if entry is None or entry.contract != row.contract:
                continue
            pending[entry.outcome_span_id].append((group, row, entry))

    labels: dict[tuple[int, str], list[BarStateLabel]] = {key: [] for key in features}
    path_source = _VerifiedOutcomeSpanSource(
        prepared,
        progress=progress,
        stage="LABEL_OUTCOME_SPAN_COMPLETE",
    )
    for path_id in sorted(pending):
        with path_source.open_path(path_id) as path:
            for group, row, entry in pending[path_id]:
                try:
                    label = label_bar_state_feature(
                        row,
                        entry_bar=entry,
                        path=path,
                        active_dates=prepared.discovery_active_dates,
                    )
                except IncompleteStateLabelHorizon:
                    continue
                labels[group].append(label)

    frozen_labels: dict[tuple[int, str], tuple[BarStateLabel, ...]] = {}
    lookups: dict[
        tuple[int, str],
        dict[tuple[str, int, int], BarStateLabel],
    ] = {}
    payloads: list[BarStateParquetPayload] = []
    for shard, group in enumerate(features):
        ordered = tuple(
            sorted(
                labels[group],
                key=lambda item: (item.decision_ns, item.contract, item.signal_start_ns),
            )
        )
        if not ordered:
            raise BarStateRunError("label construction returned no Discovery rows")
        lookup: dict[tuple[str, int, int], BarStateLabel] = {}
        for item in ordered:
            key = item.contract, item.signal_start_ns, item.decision_ns
            if key in lookup:
                raise BarStateRunError("label identity is duplicated")
            lookup[key] = item
        timeframe, feature_set_id = group
        frozen_labels[group] = ordered
        lookups[group] = lookup
        payloads.append(
            BarStateParquetPayload(
                artifact_key_suffix=(f"tf{timeframe:04d}_fs{feature_set_id.lower()}_discovery"),
                split_key="discovery",
                shard_ordinal=shard,
                logical_identity={
                    "feature_set_id": feature_set_id,
                    "label_horizon_active_days": 20,
                    "row_count": len(ordered),
                    "timeframe_seconds": timeframe,
                },
                table=_label_table(ordered),
            )
        )
    memory_plan = {
        "maximum_resident_one_second_rows": max(path_source.row_counts.values()),
        "one_second_row_count": sum(path_source.row_counts.values()),
        "outcome_span_count": len(path_source.row_counts),
        "resident_outcome_span_limit": 1,
    }
    return (
        frozen_labels,
        lookups,
        tuple(payloads),
        _fold_terminal_indices(prepared),
        memory_plan,
    )


def _fit_models_and_build_signals(
    prepared: PreparedBarStateRun,
    features: Mapping[tuple[int, str], Sequence[BarStateFeatureRow]],
    label_lookups: Mapping[tuple[int, str], Mapping[tuple[str, int, int], BarStateLabel]],
    fold_terminals: Mapping[tuple[int, int], int],
    progress: Callable[[BarStateRunProgress], None] | None,
) -> tuple[
    dict[tuple[int, str, int], CanonicalBarStateModel],
    dict[str, tuple[Mapping[str, object], ...]],
    tuple[StatePortfolioSignal, ...],
    dict[str, object],
]:
    models: dict[tuple[int, str, int], CanonicalBarStateModel] = {}
    total_fits = len(features) * len(prepared.split_plan.inner_folds)
    fit_count = 0
    for group, rows in features.items():
        lookup = label_lookups[group]
        timeframe, feature_set_id = group
        for fold in prepared.split_plan.inner_folds:
            train_rows: list[BarStateFeatureRow] = []
            train_labels: list[BarStateLabel] = []
            for row in rows:
                if row.source_date > fold.train.end_date:
                    break
                label = lookup.get(_feature_identity(row))
                if label is None:
                    continue
                if label.horizon_terminal_date > fold.purge.end_date:
                    raise BarStateRunError("training label is not fixed-horizon mature")
                train_rows.append(row)
                train_labels.append(label)
            model_id = (
                f"bsv2_tf{timeframe:04d}_fs{feature_set_id.lower()}_"
                f"discovery_inner_{fold.fold_number}"
            )
            models[timeframe, feature_set_id, fold.fold_number] = fit_bar_state_model(
                train_rows,
                train_labels,
                model_id=model_id,
                hyperparameters=_model_hyperparameters(prepared.profile),
            )
            fit_count += 1
            _notify(progress, stage="MODEL_FIT_COMPLETE", completed=fit_count, total=total_fits)

    documents: dict[str, tuple[Mapping[str, object], ...]] = {}
    signals: list[StatePortfolioSignal] = []
    signal_counts: dict[str, dict[str, int]] = {}
    for candidate_index, candidate in enumerate(prepared.config.candidates, start=1):
        group = candidate.timeframe_seconds, candidate.feature_set.feature_set_id
        rows = features[group]
        lookup = label_lookups[group]
        margin = Fraction(
            candidate.confidence_margin.numerator,
            candidate.confidence_margin.denominator,
        )
        model_documents: list[Mapping[str, object]] = []
        candidate_counts = {"LONG": 0, "NO_TRADE": 0, "SHORT": 0}
        for fold in prepared.split_plan.inner_folds:
            model = models[group[0], group[1], fold.fold_number]
            model_documents.append(
                {
                    "fold_key": f"discovery_inner_{fold.fold_number}",
                    "model": model.as_dict(),
                    "model_sha256": model.sha256,
                    "schema": "systematic_fx.bar_state_fold_model.v1",
                }
            )
            for row in rows:
                if not (
                    fold.oos_decisions.start_date <= row.source_date <= fold.oos_decisions.end_date
                ):
                    continue
                prediction = model.predict(row.values, margin=margin)
                candidate_counts[prediction.decision.value] += 1
                label = lookup.get(_feature_identity(row))
                path_id: int | None = None
                entry_index: int | None = None
                terminal_index: int | None = None
                entry_active_date: date | None = None
                entry_utc_month: str | None = None
                no_fill_reason: str | None = None
                if prediction.decision.value != "NO_TRADE":
                    if label is None:
                        no_fill_reason = (
                            "MISSING_IMMEDIATE_OBSERVED_SUCCESSOR_WITHIN_OUTCOME_SPAN_"
                            "OR_COMPLETE_HORIZON"
                        )
                    else:
                        path_id = label.entry_path_id
                        entry_index = label.entry_path_index
                        entry_active_date = label.horizon_start_date
                        entry_utc_month = datetime.fromtimestamp(
                            label.entry_start_ns // 1_000_000_000,
                            UTC,
                        ).strftime("%Y-%m")
                        try:
                            terminal_index = fold_terminals[
                                label.entry_path_id,
                                fold.fold_number,
                            ]
                        except KeyError as error:
                            raise BarStateRunError(
                                "OOS signal lacks a fold terminal path index"
                            ) from error
                        if terminal_index < entry_index:
                            raise BarStateRunError("OOS fold terminal precedes entry")
                fold_key = f"discovery_inner_{fold.fold_number}"
                signals.append(
                    StatePortfolioSignal(
                        signal_id=canonical_sha256(
                            {
                                "candidate_key": candidate.candidate_key,
                                "decision_ns": row.decision_ns,
                                "fold_key": fold_key,
                                "schema": "systematic_fx.bar_state_signal_id.v1",
                            }
                        ),
                        candidate_key=candidate.candidate_key,
                        fold_key=fold_key,
                        block_key=fold_key,
                        decision_ns=row.decision_ns,
                        signal_active_date=row.source_date,
                        entry_active_date=entry_active_date,
                        entry_utc_month=entry_utc_month,
                        contract=row.contract,
                        decision=prediction.decision,
                        atr_true_range_sum_ticks=row.atr_true_range_sum_ticks,
                        path_id=path_id,
                        entry_path_index=entry_index,
                        fold_terminal_path_index=terminal_index,
                        no_fill_reason=no_fill_reason,
                    )
                )
        documents[candidate.candidate_key] = tuple(model_documents)
        signal_counts[candidate.candidate_key] = candidate_counts
        _notify(
            progress,
            stage="CANDIDATE_SIGNALS_COMPLETE",
            completed=candidate_index,
            total=len(prepared.config.candidates),
        )
    ordered_signals = tuple(
        sorted(
            signals,
            key=lambda item: (item.decision_ns, item.candidate_key, item.signal_id),
        )
    )
    if tuple(sorted({item.candidate_key for item in ordered_signals})) != tuple(
        sorted(prepared.candidate_keys)
    ):
        raise BarStateRunError("OOS signal stream does not cover all twelve candidates")
    signal_summary = {
        "candidate_signal_decision_counts": signal_counts,
        "signal_count": len(ordered_signals),
    }
    return models, documents, ordered_signals, signal_summary


def _fit_discovery_finalist_models(
    prepared: PreparedBarStateRun,
    features: Mapping[tuple[int, str], Sequence[BarStateFeatureRow]],
    label_lookups: Mapping[tuple[int, str], Mapping[tuple[str, int, int], BarStateLabel]],
    finalist_keys: Sequence[str],
    progress: Callable[[BarStateRunProgress], None] | None,
) -> tuple[
    dict[tuple[int, str], Mapping[str, object]],
    tuple[Mapping[str, object], ...],
]:
    """Refit selected model groups on the complete visible Discovery sample."""

    finalists = tuple(finalist_keys)
    if (
        len(finalists) > 4
        or len(set(finalists)) != len(finalists)
        or any(item not in prepared.candidate_keys for item in finalists)
    ):
        raise BarStateRunError("finalist set is outside the frozen candidate budget")
    fit_span = prepared.split_plan.discovery_final_fit
    maturity_tail = prepared.split_plan.discovery_final_label_tail
    if (
        fit_span.start_active_ordinal != 1
        or fit_span.end_active_ordinal != 469
        or maturity_tail.start_active_ordinal != 470
        or maturity_tail.end_active_ordinal != 489
    ):
        raise BarStateRunError("Discovery final-fit span differs from ordinals 1..469/489")

    candidate_by_key = {item.candidate_key: item for item in prepared.config.candidates}
    selected_groups = tuple(
        sorted(
            {
                (
                    candidate_by_key[key].timeframe_seconds,
                    candidate_by_key[key].feature_set.feature_set_id,
                )
                for key in finalists
            }
        )
    )
    models: dict[tuple[int, str], Mapping[str, object]] = {}
    for index, group in enumerate(selected_groups, start=1):
        rows: list[BarStateFeatureRow] = []
        labels: list[BarStateLabel] = []
        lookup = label_lookups[group]
        for row in features[group]:
            if row.source_date > fit_span.end_date:
                break
            label = lookup.get(_feature_identity(row))
            if label is None:
                continue
            if label.horizon_terminal_date > maturity_tail.end_date:
                raise BarStateRunError("Discovery final-fit label exceeds the frozen maturity tail")
            rows.append(row)
            labels.append(label)
        if not rows:
            raise BarStateRunError("Discovery finalist group has no mature training rows")
        timeframe, feature_set_id = group
        model = fit_bar_state_model(
            rows,
            labels,
            model_id=(f"bsv2_tf{timeframe:04d}_fs{feature_set_id.lower()}_discovery_final_fit"),
            hyperparameters=_model_hyperparameters(prepared.profile),
        )
        document: Mapping[str, object] = {
            "fit_key": "discovery_final_fit",
            "label_maturity_end_active_ordinal": 489,
            "model": model.as_dict(),
            "model_sha256": model.sha256,
            "schema": "systematic_fx.bar_state_final_fit_model.v1",
            "training_decision_end_active_ordinal": 469,
        }
        models[group] = document
        _notify(
            progress,
            stage="DISCOVERY_FINAL_MODEL_FIT_COMPLETE",
            completed=index,
            total=len(selected_groups),
        )

    bindings = tuple(
        {
            "candidate_key": key,
            "feature_set_id": candidate_by_key[key].feature_set.feature_set_id,
            "model_sha256": models[
                (
                    candidate_by_key[key].timeframe_seconds,
                    candidate_by_key[key].feature_set.feature_set_id,
                )
            ]["model_sha256"],
            "timeframe_seconds": candidate_by_key[key].timeframe_seconds,
        }
        for key in finalists
    )
    return models, bindings


def _fraction_document(value: Fraction | None) -> dict[str, int] | None:
    if value is None:
        return None
    return {"denominator": value.denominator, "numerator": value.numerator}


def _selection_candidate_document(value: StateCandidateSelection) -> dict[str, object]:
    return {
        "bootstrap_lower_bound_ev_ticks": _fraction_document(value.bootstrap_lower_bound_ev_ticks),
        "candidate_key": value.candidate_key,
        "final_label": value.final_label,
        "maximum_drawdown_ticks": value.maximum_drawdown_ticks,
        "moderate_ev_ticks": (
            None if value.moderate_ev_ticks is None else format(value.moderate_ev_ticks, "f")
        ),
        "positive_component_size": value.positive_component_size,
        "positive_inner_fold_count": value.positive_inner_fold_count,
        "rejection_reasons": list(value.rejection_reasons),
        "selected_stop_loss_index": value.selected_stop_loss_index,
        "selected_stop_loss_multiplier": _fraction_document(value.selected_stop_loss_multiplier),
        "selected_take_profit_index": value.selected_take_profit_index,
        "selected_take_profit_multiplier": _fraction_document(
            value.selected_take_profit_multiplier
        ),
        "worst_fold_moderate_ev_ticks": (
            None
            if value.worst_fold_moderate_ev_ticks is None
            else format(value.worst_fold_moderate_ev_ticks, "f")
        ),
    }


def _multiplicity_document(value: StateCellMultiplicityResult) -> dict[str, object]:
    return {
        "adjusted_p_value": _fraction_document(value.adjusted_p_value),
        "bh_rejected": value.bh_rejected,
        "bootstrap_lower_bound_ev_ticks": _fraction_document(value.bootstrap_lower_bound_ev_ticks),
        "candidate_key": value.candidate_key,
        "deterministic_gate_passed": value.deterministic_gate_passed,
        "raw_p_value": _fraction_document(value.raw_p_value),
        "rejection_reasons": list(value.rejection_reasons),
        "stop_loss_index": value.stop_loss_index,
        "take_profit_index": value.take_profit_index,
    }


def _selected_price_policy(value: StateCandidateSelection) -> dict[str, object]:
    try:
        return bar_state_price_policy_from_selection(_selection_candidate_document(value))
    except BarStateArtifactError as error:  # pragma: no cover - selection is typed/frozen
        raise BarStateRunError(
            "selected price policy cannot bind the frozen 7-axis cell"
        ) from error


def _fold_evaluation_calendars(
    prepared: PreparedBarStateRun,
) -> tuple[StateFoldEvaluationCalendar, ...]:
    dates = prepared.dataset.eligible_active_dates
    result = tuple(
        StateFoldEvaluationCalendar(
            fold_key=f"discovery_inner_{fold.fold_number}",
            active_dates=dates[
                fold.oos_decisions.start_active_ordinal - 1 : fold.outcome_tail.end_active_ordinal
            ],
        )
        for fold in prepared.split_plan.inner_folds
    )
    if tuple(len(item.active_dates) for item in result) != (117, 117, 137):
        raise BarStateRunError("Discovery evaluation calendars drifted")
    return result


def _production_engine(
    prepared: PreparedBarStateRun,
    *,
    candidate_keys: tuple[str, ...],
    progress: Callable[[BarStateRunProgress], None] | None,
) -> BarStateEngineResult:
    """Execute the actual verified Discovery computation with one resident path."""

    if candidate_keys != prepared.candidate_keys or len(candidate_keys) != 12:
        raise BarStateRunError("production engine requires the exact candidate catalog")
    signal_bars, features, feature_payloads, feature_qc = _build_discovery_features(
        prepared,
        progress,
    )
    (
        _labels,
        label_lookups,
        label_payloads,
        fold_terminals,
        path_memory_plan,
    ) = _build_discovery_labels(
        prepared,
        signal_bars,
        features,
        progress,
    )
    _models, model_documents, signals, signal_summary = _fit_models_and_build_signals(
        prepared,
        features,
        label_lookups,
        fold_terminals,
        progress,
    )
    calendars = _fold_evaluation_calendars(prepared)
    observed_months = tuple(
        sorted(
            {
                f"{active_date.year:04d}-{active_date.month:02d}"
                for calendar in calendars
                for active_date in calendar.active_dates
            }
        )
    )
    directory = _trade_spool_directory(prepared)
    spools = {key: _TradeSpool(directory, key) for key in candidate_keys}
    handed_off = False
    try:
        path_source = _VerifiedOutcomeSpanSource(
            prepared,
            progress=progress,
            stage="PORTFOLIO_OUTCOME_SPAN_COMPLETE",
        )
        portfolio = stream_state_portfolio(
            signals,
            path_source=path_source,
            observed_utc_months=observed_months,
            trade_sink=lambda item: spools[item.candidate_key].append(item),
            progress=lambda item: _notify(
                progress,
                stage="PORTFOLIO_SIGNAL_REPLAY",
                completed=item.completed_signal_count,
                total=item.total_signal_count,
            ),
            progress_every=1_000,
        )
        oos_payloads = {key: (spools[key].finish(),) for key in candidate_keys}
        supports = summarize_candidate_support(
            signals,
            timeframe_by_candidate={
                item.candidate_key: item.timeframe_seconds for item in prepared.config.candidates
            },
        )
        selection = select_state_finalists(
            portfolio,
            candidate_order=candidate_keys,
            supports=supports,
            split_plan=prepared.split_plan,
            progress=lambda item: _notify(
                progress,
                stage="SELECTION_BOOTSTRAP_CELL",
                completed=item.completed_bootstrap_cell_count,
                total=item.total_state_cell_count,
            ),
        )
        final_fit_models, final_fit_bindings = _fit_discovery_finalist_models(
            prepared,
            features,
            label_lookups,
            selection.finalist_keys,
            progress,
        )
        candidate_by_key = {item.candidate_key: item for item in prepared.config.candidates}
        for candidate_key in selection.finalist_keys:
            candidate = candidate_by_key[candidate_key]
            group = candidate.timeframe_seconds, candidate.feature_set.feature_set_id
            model_documents[candidate_key] = (
                *model_documents[candidate_key],
                final_fit_models[group],
            )
        final_fit_binding_by_key = {str(item["candidate_key"]): item for item in final_fit_bindings}
        support_documents = {
            item.candidate_key: {
                "candidate_key": item.candidate_key,
                "distinct_signal_day_count": item.distinct_signal_day_count,
                "raw_directional_signal_count": item.raw_directional_signal_count,
                "raw_signal_count_by_fold": [
                    {"fold_key": key, "signal_count": count}
                    for key, count in item.raw_signal_count_by_fold
                ],
                "timeframe_seconds": item.timeframe_seconds,
            }
            for item in supports
        }
        cell_documents = [item.as_dict() for item in portfolio.cells]
        selection_documents = [
            _selection_candidate_document(item) for item in selection.candidate_results
        ]
        multiplicity_documents = [
            _multiplicity_document(item) for item in selection.multiplicity_results
        ]
        bootstrap_evaluation_calendar = frozen_bar_state_bootstrap_evaluation_calendar(
            prepared.split_plan
        )
        global_document = {
            "axis_resolutions": [
                {
                    "axis_vector_sha256": list(item.axis_vector_sha256),
                    "candidate_key": item.candidate_key,
                    "filled_directional_signal_count": item.filled_directional_signal_count,
                    "per_signal_distinct_count_histogram": [
                        {"distinct_count": key, "signal_count": count}
                        for key, count in item.per_signal_distinct_count_histogram
                    ],
                    "unique_axis_vector_count": item.unique_axis_vector_count,
                }
                for item in portfolio.axis_resolutions
            ],
            "bh_family_size": selection.bh_family_size,
            "bootstrap_convention": selection.bootstrap_convention,
            "bootstrap_evaluation_calendar": bootstrap_evaluation_calendar,
            "bootstrap_evaluation_calendar_sha256": (
                BAR_STATE_FROZEN_BOOTSTRAP_EVALUATION_CALENDAR_SHA256
            ),
            "candidate_results": selection_documents,
            "candidate_support": [support_documents[key] for key in candidate_keys],
            "cell_summaries": cell_documents,
            "discovery_final_fit_models": [
                {
                    "feature_set_id": feature_set_id,
                    **dict(document),
                    "timeframe_seconds": timeframe,
                }
                for (timeframe, feature_set_id), document in sorted(final_fit_models.items())
            ],
            "discovery_finalist_model_bindings": [dict(item) for item in final_fit_bindings],
            "feature_exclusion_qc": [
                {
                    "exclusion_counts_by_reason": dict(counts),
                    "feature_set_id": feature_set_id,
                    "timeframe_seconds": timeframe,
                }
                for (timeframe, feature_set_id), counts in sorted(feature_qc.items())
            ],
            "finalist_keys": list(selection.finalist_keys),
            "memory_plan": {
                "accumulator_count": portfolio.memory_plan.accumulator_count,
                "candidate_count": portfolio.memory_plan.candidate_count,
                "grid_cell_count": portfolio.memory_plan.grid_cell_count,
                "input_signal_count": portfolio.memory_plan.input_signal_count,
                "maximum_input_signal_count": (portfolio.memory_plan.maximum_input_signal_count),
                "retained_trade_record_count": (portfolio.memory_plan.retained_trade_record_count),
                "scenario_count": portfolio.memory_plan.scenario_count,
                **path_memory_plan,
            },
            "multiplicity_results": multiplicity_documents,
            "observed_utc_months": list(portfolio.observed_utc_months),
            "portfolio_executed_trade_record_count": (portfolio.executed_trade_record_count),
            "portfolio_signal_count": portfolio.signal_count,
            "schema": STATE_SELECTION_SCHEMA,
            **signal_summary,
        }
        selection_by_key = {item.candidate_key: item for item in selection.candidate_results}
        candidate_results: list[BarStateCandidateEngineArtifacts] = []
        for candidate_key in candidate_keys:
            chosen = selection_by_key[candidate_key]
            selected = chosen.final_label == "FINALIST"
            candidate_cells = [
                item for item in multiplicity_documents if item["candidate_key"] == candidate_key
            ]
            terminal_document = {
                "candidate_selection": _selection_candidate_document(chosen),
                "candidate_support": support_documents[candidate_key],
                "discovery_final_fit_model": (
                    None
                    if candidate_key not in final_fit_binding_by_key
                    else dict(final_fit_binding_by_key[candidate_key])
                ),
                "multiplicity_cells": candidate_cells,
                "price_policy": _selected_price_policy(chosen),
                "schema": "systematic_fx.bar_state_candidate_result.v1",
            }
            compact = {
                "candidate_key": candidate_key,
                "final_label": chosen.final_label,
                "discovery_final_fit_model_sha256": (
                    None
                    if candidate_key not in final_fit_binding_by_key
                    else final_fit_binding_by_key[candidate_key]["model_sha256"]
                ),
                "positive_component_size": chosen.positive_component_size,
                "price_policy": _selected_price_policy(chosen),
                "rejection_reasons": list(chosen.rejection_reasons),
                "selected_stop_loss_index": chosen.selected_stop_loss_index,
                "selected_take_profit_index": chosen.selected_take_profit_index,
            }
            candidate_results.append(
                BarStateCandidateEngineArtifacts(
                    candidate_key=candidate_key,
                    model_documents=model_documents[candidate_key],
                    oos_trade_tables=oos_payloads[candidate_key],
                    terminal_document=terminal_document,
                    decision_label=("DISCOVERY_FINALIST" if selected else "DISCOVERY_REJECT"),
                    trial_status="SUCCEEDED" if selected else "REJECTED",
                    compact_summary=compact,
                )
            )
        handed_off = True
        return BarStateEngineResult(
            feature_tables=feature_payloads,
            label_tables=label_payloads,
            candidate_results=tuple(candidate_results),
            global_document=global_document,
        )
    finally:
        if not handed_off:
            for spool in spools.values():
                spool.close()


def _unavailable_engine(
    prepared: PreparedBarStateRun,
    *,
    candidate_keys: tuple[str, ...],
    progress: Callable[[BarStateRunProgress], None] | None,
) -> BarStateEngineResult:
    del prepared, candidate_keys, progress
    raise BarStateRunError("production bar-state Discovery engine is unavailable")


def _default_services() -> BarStateRunServices:
    return BarStateRunServices(
        load_config=load_bar_state_config,
        plan_splits=plan_bar_state_splits,
        git_head=_git_head,
        build_snapshot=build_code_snapshot,
        dependency_hash=dependency_lock_sha256,
        runtime=runtime_environment,
        postgres_runtime=_postgres_runtime,
        require_predecessor=require_clean_bar_state_predecessor,
        register_artifact=register_published_bar_artifact,
        register_campaign=register_bar_state_campaign,
        register_spec=register_bar_state_run_spec,
        reserve_attempt=reserve_run_attempt,
        start_attempt=start_run_attempt,
        validate_duplicate=validate_reused_bar_state_attempt,
        abort_attempt=abort_bar_state_run_attempt,
        link_artifact=register_bar_state_artifact_link,
        terminalize=register_terminal_bar_state_result,
        publish_json=publish_bar_state_json,
        publish_parquet=publish_bar_state_parquet,
        publish_parquet_file=publish_bar_state_parquet_open_file,
        engine=_production_engine,
    )


@dataclass(frozen=True, slots=True)
class _ActiveAttempt:
    research_run_attempt_id: int
    run_fingerprint: str


def _registered_spec_id(registration: Any, spec: RunSpec) -> int:
    identifier = getattr(registration, "research_run_spec_id", None)
    if isinstance(identifier, bool) or not isinstance(identifier, int) or identifier <= 0:
        raise BarStateRunError("RunSpec registration returned an invalid identifier")
    if getattr(registration, "run_fingerprint", None) != spec.fingerprint:
        raise BarStateRunError("RunSpec registration returned a different fingerprint")
    return identifier


def _validate_reservation(reservation: Any, *, research_run_spec_id: int) -> None:
    attempt_id = getattr(reservation, "research_run_attempt_id", None)
    attempt_number = getattr(reservation, "attempt_number", None)
    if (
        isinstance(attempt_id, bool)
        or not isinstance(attempt_id, int)
        or attempt_id <= 0
        or isinstance(attempt_number, bool)
        or not isinstance(attempt_number, int)
        or attempt_number <= 0
    ):
        raise BarStateRunError("attempt reservation returned an invalid identity")
    if getattr(reservation, "research_run_spec_id", None) != research_run_spec_id:
        raise BarStateRunError("attempt reservation belongs to another RunSpec")
    execute = getattr(reservation, "execute", None)
    status = getattr(reservation, "status", None)
    reused = getattr(reservation, "reused_attempt_id", None)
    if execute is True:
        if status != "QUEUED" or reused is not None:
            raise BarStateRunError("executable reservation is not exactly QUEUED")
    elif execute is False:
        if (
            status != "SKIPPED_DUPLICATE"
            or isinstance(reused, bool)
            or not isinstance(reused, int)
            or reused <= 0
            or reused == attempt_id
        ):
            raise BarStateRunError("duplicate reservation has invalid reuse lineage")
    else:
        raise BarStateRunError("attempt reservation execute flag is invalid")


def _assert_provenance_unchanged(
    prepared: PreparedBarStateRun,
    provenance: BarStateRunProvenance,
    database_url: str,
    services: BarStateRunServices,
) -> None:
    observed = _capture_provenance(prepared, database_url, services)
    if (
        observed.code_commit != provenance.code_commit
        or observed.snapshot.sha256 != provenance.snapshot.sha256
        or observed.dependency_lock_sha256 != provenance.dependency_lock_sha256
        or observed.runtime_environment_sha256 != provenance.runtime_environment_sha256
        or observed.postgres_migrations_sha256 != provenance.postgres_migrations_sha256
    ):
        raise BarStateRunError("code, configuration, runtime, or database drifted during run")


def _artifact_lineage(
    prepared: PreparedBarStateRun,
    provenance: BarStateRunProvenance,
    specs: Sequence[RunSpec],
    *,
    parents: Sequence[PublishedBarArtifact] = (),
    candidate_key: str | None = None,
) -> BarStateArtifactLineage:
    candidate_definition_sha256: str | None = None
    run_fingerprint: str | None = None
    if candidate_key is not None:
        try:
            index = prepared.candidate_keys.index(candidate_key)
        except ValueError as error:
            raise BarStateRunError("artifact candidate is outside the frozen catalog") from error
        candidate_definition_sha256 = prepared.config.candidates[index].definition_sha256
        run_fingerprint = specs[index].fingerprint
    return BarStateArtifactLineage(
        config_file_sha256=prepared.config.sha256,
        config_semantic_sha256=prepared.config.semantic_sha256,
        candidate_catalog_sha256=prepared.config.candidate_catalog_sha256,
        training_plan_sha256=prepared.split_plan.sha256,
        code_snapshot_sha256=provenance.snapshot.sha256,
        dependency_lock_sha256=provenance.dependency_lock_sha256,
        runtime_environment_sha256=provenance.runtime_environment_sha256,
        ordered_run_set_sha256=canonical_sha256([item.fingerprint for item in specs]),
        discovery_scope=frozen_bar_state_discovery_scope(),
        candidate_key=candidate_key,
        candidate_definition_sha256=candidate_definition_sha256,
        run_fingerprint=run_fingerprint,
        parent_artifacts=ordered_parent_artifacts(parents),
    )


def _validate_parquet_payloads(
    values: Sequence[BarStateParquetPayload],
    *,
    label: str,
) -> None:
    if not values:
        raise BarStateRunError(f"engine returned no {label} evidence")
    identities = tuple(
        (item.split_key, item.shard_ordinal, item.artifact_key_suffix) for item in values
    )
    if len(set(identities)) != len(identities):
        raise BarStateRunError(f"engine returned duplicate {label} evidence identities")


def _verify_model_document(
    document: Mapping[str, object],
    *,
    expected_model_id: str,
    expected_timeframe_seconds: int,
    expected_feature_set_id: str,
    expected_hyperparameters: BarStateModelHyperparameters,
) -> CanonicalBarStateModel:
    model_object = document.get("model")
    if not isinstance(model_object, Mapping):
        raise BarStateRunError("candidate model document lacks a canonical model")
    try:
        model = CanonicalBarStateModel.from_canonical_bytes(
            canonical_json_bytes(model_object) + b"\n",
            expected_hyperparameters=expected_hyperparameters,
        )
    except (BarStateModelError, TypeError, ValueError) as error:
        raise BarStateRunError("candidate model document failed strict decoding") from error
    if (
        document.get("model_sha256") != model.sha256
        or model.model_id != expected_model_id
        or model.timeframe_seconds != expected_timeframe_seconds
        or model.feature_set_id != expected_feature_set_id
    ):
        raise BarStateRunError("candidate model identity/content drift")
    return model


def _validate_final_fit_bindings(
    prepared: PreparedBarStateRun,
    candidates: Sequence[BarStateCandidateEngineArtifacts],
    global_document: Mapping[str, object],
) -> None:
    """Prove that only selected candidates bind the exact final Discovery refit."""

    try:
        projection = bar_state_global_result_projection(
            {
                "candidate_count": len(candidates),
                "discovery_result": dict(global_document),
                "schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["GLOBAL_RESULT"],
            },
            profile=prepared.profile,
        )
        finalists = tuple(global_document["finalist_keys"])  # type: ignore[arg-type]
        global_models = tuple(global_document["discovery_final_fit_models"])  # type: ignore[arg-type]
        global_bindings = tuple(
            global_document["discovery_finalist_model_bindings"]  # type: ignore[arg-type]
        )
        global_candidates = tuple(global_document["candidate_results"])  # type: ignore[arg-type]
    except (BarStateArtifactError, KeyError, TypeError) as error:
        raise BarStateRunError("engine result lacks final-fit governance evidence") from error
    if (
        len(finalists) > 4
        or len(set(finalists)) != len(finalists)
        or any(not isinstance(key, str) or key not in prepared.candidate_keys for key in finalists)
    ):
        raise BarStateRunError("engine finalist set differs from the frozen catalog/budget")
    if len(global_candidates) != len(prepared.candidate_keys) or any(
        not isinstance(item, Mapping) for item in global_candidates
    ):
        raise BarStateRunError("global result lacks the exact candidate result universe")
    global_candidate_by_key = {
        item.get("candidate_key"): item for item in global_candidates if isinstance(item, Mapping)
    }
    if set(global_candidate_by_key) != set(prepared.candidate_keys):
        raise BarStateRunError("global candidate result identities differ from the catalog")
    if set(projection.candidate_selections) != set(prepared.candidate_keys):
        raise BarStateRunError("global candidate result identities differ from the catalog")

    candidate_by_key = {item.candidate_key: item for item in prepared.config.candidates}
    expected_bindings: list[dict[str, object]] = []
    final_document_by_group: dict[tuple[int, str], dict[str, object]] = {}
    inner_package_sha256_by_group: dict[tuple[int, str], tuple[str, ...]] = {}
    for candidate in candidates:
        selected = candidate.candidate_key in finalists
        try:
            projected_compact = bar_state_terminal_compact_summary(
                {
                    "candidate_key": candidate.candidate_key,
                    "compact_summary": dict(candidate.compact_summary),
                    "decision_label": candidate.decision_label,
                    "result": dict(candidate.terminal_document),
                    "schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["TERMINAL_RESULT"],
                    "trial_status": candidate.trial_status,
                }
            )
        except BarStateArtifactError as error:
            raise BarStateRunError(
                "candidate compact summary differs from immutable terminal evidence"
            ) from error
        if dict(candidate.compact_summary) != projected_compact:
            raise BarStateRunError(
                "candidate compact summary differs from immutable terminal evidence"
            )
        if candidate.trial_status != (
            "SUCCEEDED" if selected else "REJECTED"
        ) or candidate.decision_label != ("DISCOVERY_FINALIST" if selected else "DISCOVERY_REJECT"):
            raise BarStateRunError("candidate terminal state differs from finalist membership")
        selection = candidate.terminal_document.get("candidate_selection")
        terminal_evidence_slice = {
            "candidate_support": candidate.terminal_document.get("candidate_support"),
            "multiplicity_cells": candidate.terminal_document.get("multiplicity_cells"),
        }
        if (
            not isinstance(selection, Mapping)
            or terminal_evidence_slice
            != dict(projection.candidate_evidence_slice_by_key[candidate.candidate_key])
            or dict(selection) != dict(global_candidate_by_key[candidate.candidate_key])
            or (selection.get("final_label") == "FINALIST") != selected
            or candidate.compact_summary.get("final_label") != selection.get("final_label")
        ):
            raise BarStateRunError("candidate selection label differs from global finalists")
        inner = tuple(item for item in candidate.model_documents if "fold_key" in item)
        final = tuple(
            item
            for item in candidate.model_documents
            if item.get("fit_key") == "discovery_final_fit"
        )
        if tuple(item.get("fold_key") for item in inner) != (
            "discovery_inner_1",
            "discovery_inner_2",
            "discovery_inner_3",
        ) or len(candidate.model_documents) != 3 + int(selected):
            raise BarStateRunError("candidate model package differs from three inner fits")
        if len(final) != int(selected):
            raise BarStateRunError("candidate final-fit cardinality differs from selection")
        spec = candidate_by_key[candidate.candidate_key]
        group = spec.timeframe_seconds, spec.feature_set.feature_set_id
        for fold_number, document in enumerate(inner, start=1):
            if document.get("schema") != "systematic_fx.bar_state_fold_model.v1":
                raise BarStateRunError("inner model package schema drift")
            _verify_model_document(
                document,
                expected_model_id=(
                    f"bsv2_tf{group[0]:04d}_fs{group[1].lower()}_discovery_inner_{fold_number}"
                ),
                expected_timeframe_seconds=group[0],
                expected_feature_set_id=group[1],
                expected_hyperparameters=_model_hyperparameters(prepared.profile),
            )
        inner_package_sha256 = tuple(canonical_sha256(document) for document in inner)
        prior_inner_package = inner_package_sha256_by_group.setdefault(
            group,
            inner_package_sha256,
        )
        if prior_inner_package != inner_package_sha256:
            raise BarStateRunError("margin variants produced different inner MODEL packages")
        terminal_binding = candidate.terminal_document.get("discovery_final_fit_model")
        compact_binding = candidate.compact_summary.get("discovery_final_fit_model_sha256")
        if not selected:
            if terminal_binding is not None or compact_binding is not None:
                raise BarStateRunError("non-finalist claims a Discovery final-fit model")
            continue
        document = dict(final[0])
        if (
            document.get("schema") != "systematic_fx.bar_state_final_fit_model.v1"
            or document.get("training_decision_end_active_ordinal") != 469
            or document.get("label_maturity_end_active_ordinal") != 489
        ):
            raise BarStateRunError("Discovery final-fit model boundary/schema drift")
        final_model = _verify_model_document(
            document,
            expected_model_id=(f"bsv2_tf{group[0]:04d}_fs{group[1].lower()}_discovery_final_fit"),
            expected_timeframe_seconds=group[0],
            expected_feature_set_id=group[1],
            expected_hyperparameters=_model_hyperparameters(prepared.profile),
        )
        model_sha256 = final_model.sha256
        existing = final_document_by_group.setdefault(group, document)
        if canonical_sha256(existing) != canonical_sha256(document):
            raise BarStateRunError("same feature group produced different final-fit models")
        expected_binding = {
            "candidate_key": candidate.candidate_key,
            "feature_set_id": group[1],
            "model_sha256": model_sha256,
            "timeframe_seconds": group[0],
        }
        if terminal_binding != expected_binding or compact_binding != model_sha256:
            raise BarStateRunError("candidate terminal final-fit binding drift")
        expected_bindings.append(expected_binding)

    if tuple(item["candidate_key"] for item in expected_bindings) != finalists:
        expected_by_key = {str(item["candidate_key"]): item for item in expected_bindings}
        try:
            expected_bindings = [expected_by_key[key] for key in finalists]
        except KeyError as error:  # pragma: no cover - membership checked above
            raise BarStateRunError("finalist model binding is incomplete") from error
    if canonical_sha256(list(global_bindings)) != canonical_sha256(expected_bindings):
        raise BarStateRunError("global finalist-to-model bindings drift")
    expected_global_models = [
        {
            "feature_set_id": feature_set_id,
            **document,
            "timeframe_seconds": timeframe,
        }
        for (timeframe, feature_set_id), document in sorted(final_document_by_group.items())
    ]
    if canonical_sha256(list(global_models)) != canonical_sha256(expected_global_models):
        raise BarStateRunError("global Discovery final-fit model catalog drift")


def _validate_engine_result(
    prepared: PreparedBarStateRun,
    result: BarStateEngineResult,
) -> tuple[str, ...]:
    if not isinstance(result, BarStateEngineResult):
        raise BarStateRunError("engine returned an invalid result contract")
    _validate_parquet_payloads(result.feature_tables, label="feature")
    _validate_parquet_payloads(result.label_tables, label="label")
    keys = tuple(item.candidate_key for item in result.candidate_results)
    if keys != prepared.candidate_keys:
        raise BarStateRunError("engine candidate results are incomplete or reordered")
    for candidate in result.candidate_results:
        if not candidate.model_documents or not candidate.oos_trade_tables:
            raise BarStateRunError("candidate lacks model or OOS trade evidence")
        _validate_parquet_payloads(candidate.oos_trade_tables, label="OOS trade")
        if candidate.trial_status not in {"SUCCEEDED", "REJECTED"}:
            raise BarStateRunError("candidate trial status is invalid")
        if not candidate.decision_label.strip():
            raise BarStateRunError("candidate decision label is empty")
        canonical_sha256(candidate.terminal_document)
        if len(canonical_sha256(candidate.compact_summary)) != 64:  # pragma: no cover
            raise BarStateRunError("candidate compact summary is invalid")
    canonical_sha256(result.global_document)
    global_artifact_document = {
        "candidate_count": len(result.candidate_results),
        "discovery_result": dict(result.global_document),
        "schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["GLOBAL_RESULT"],
    }
    try:
        validate_bar_state_global_bootstrap(
            global_artifact_document,
            split_plan=prepared.split_plan,
            profile=prepared.profile,
        )
    except BarStateArtifactError as error:
        raise BarStateRunError(
            "engine GLOBAL bootstrap evidence differs from the frozen replay"
        ) from error
    _validate_final_fit_bindings(
        prepared,
        result.candidate_results,
        result.global_document,
    )
    return tuple(result.global_document["finalist_keys"])  # type: ignore[arg-type]


def _publish_parquet_payload(
    prepared: PreparedBarStateRun,
    services: BarStateRunServices,
    *,
    kind: Literal["FEATURE", "LABEL", "OOS_TRADE"],
    payload: BarStateParquetPayload,
    lineage: BarStateArtifactLineage,
) -> PublishedBarArtifact:
    arguments = {
        "kind": kind,
        "artifact_key_suffix": payload.artifact_key_suffix,
        "lineage": lineage,
        "logical_identity": payload.logical_identity,
        "profile": prepared.profile,
    }
    if payload.table is not None:
        return services.publish_parquet(
            prepared.project_root,
            table=payload.table,
            **arguments,
        )
    assert payload.source is not None
    assert payload.row_count is not None
    assert payload.schema is not None
    return services.publish_parquet_file(
        prepared.project_root,
        source=payload.source,
        row_count=payload.row_count,
        schema=payload.schema,
        **arguments,
    )


@dataclass(frozen=True, slots=True)
class _PublishedEngineResult:
    shared: tuple[tuple[str, BarStateParquetPayload, PublishedBarArtifact], ...]
    models: Mapping[str, PublishedBarArtifact]
    oos: Mapping[str, tuple[tuple[BarStateParquetPayload, PublishedBarArtifact], ...]]
    global_artifact: PublishedBarArtifact
    terminals: Mapping[str, PublishedBarArtifact]


def _evidence_identity(
    role: str,
    split_key: str,
    shard_ordinal: int,
    artifact: PublishedBarArtifact,
) -> tuple[str, str, int, str, str, str]:
    lineage = artifact.descriptor.logical_identity.get("lineage")
    if not isinstance(lineage, Mapping):
        raise BarStateRunError("published evidence lacks canonical lineage")
    return (
        role,
        split_key,
        shard_ordinal,
        artifact.descriptor.identity_sha256,
        artifact.sha256,
        canonical_sha256(lineage),
    )


def _published_candidate_evidence(
    published: _PublishedEngineResult,
    candidate_key: str,
) -> tuple[tuple[str, str, int, str, str, str], ...]:
    values = [
        _evidence_identity(role, payload.split_key, payload.shard_ordinal, artifact)
        for role, payload, artifact in published.shared
    ]
    values.append(
        _evidence_identity(
            "MODEL",
            "discovery",
            0,
            published.models[candidate_key],
        )
    )
    values.extend(
        _evidence_identity(
            "OOS_TRADE",
            payload.split_key,
            payload.shard_ordinal,
            artifact,
        )
        for payload, artifact in published.oos[candidate_key]
    )
    values.extend(
        (
            _evidence_identity(
                "GLOBAL_RESULT",
                "discovery",
                0,
                published.global_artifact,
            ),
            _evidence_identity(
                "TERMINAL_RESULT",
                "discovery",
                0,
                published.terminals[candidate_key],
            ),
        )
    )
    return tuple(sorted(values))


def _validate_duplicate_consensus(
    prepared: PreparedBarStateRun,
    reports: Mapping[str, BarStateReuseValidationReport],
    *,
    published: _PublishedEngineResult | None = None,
) -> tuple[str, tuple[str, ...], dict[str, str]]:
    """Require reused candidates to share one exact immutable result universe."""

    if not reports:
        raise BarStateRunError("duplicate consensus requires at least one report")
    if published is None and set(reports) != set(prepared.candidate_keys):
        raise BarStateRunError("all-duplicate consensus requires all twelve candidates")
    if not set(reports) <= set(prepared.candidate_keys):
        raise BarStateRunError("duplicate consensus contains an unknown candidate")
    global_identity_sha256: str | None = None
    global_sha256: str | None = None
    global_document_sha256: str | None = None
    global_evidence_projection_sha256: str | None = None
    finalist_keys: tuple[str, ...] | None = None
    terminal_sha256_by_key: dict[str, str] = {}
    for candidate_key, report in reports.items():
        if (
            not isinstance(report, BarStateReuseValidationReport)
            or report.candidate_key != candidate_key
        ):
            raise BarStateRunError("duplicate validation report identity drift")
        keys = report.finalist_keys
        if (
            len(keys) > 4
            or len(set(keys)) != len(keys)
            or any(not isinstance(key, str) or key not in prepared.candidate_keys for key in keys)
        ):
            raise BarStateRunError("duplicate finalist set differs from frozen budget")
        document_sha256 = report.global_document_sha256
        if global_sha256 is None:
            global_identity_sha256 = report.global_artifact_identity_sha256
            global_sha256 = report.global_artifact_sha256
            global_document_sha256 = document_sha256
            global_evidence_projection_sha256 = report.global_evidence_projection_sha256
            finalist_keys = keys
        elif (
            global_identity_sha256 != report.global_artifact_identity_sha256
            or global_sha256 != report.global_artifact_sha256
            or global_document_sha256 != document_sha256
            or global_evidence_projection_sha256 != report.global_evidence_projection_sha256
            or finalist_keys != keys
        ):
            raise BarStateRunError("duplicate candidates do not share one global result")
        if (candidate_key in keys) != (report.trial_status == "SUCCEEDED"):
            raise BarStateRunError("duplicate terminal state differs from global finalists")
        null_binding_sha256 = canonical_sha256(None)
        if (
            len(report.candidate_evidence_slice_sha256) != 64
            or any(
                value not in "0123456789abcdef" for value in report.candidate_evidence_slice_sha256
            )
            or len(report.candidate_selection_sha256) != 64
            or any(value not in "0123456789abcdef" for value in report.candidate_selection_sha256)
            or len(report.finalist_model_binding_sha256) != 64
            or any(
                value not in "0123456789abcdef" for value in report.finalist_model_binding_sha256
            )
            or len(report.candidate_selection_projection_sha256) != 64
            or any(
                value not in "0123456789abcdef"
                for value in report.candidate_selection_projection_sha256
            )
            or len(report.global_evidence_projection_sha256) != 64
            or any(
                value not in "0123456789abcdef"
                for value in report.global_evidence_projection_sha256
            )
            or len(report.model_package_projection_sha256) != 64
            or any(
                value not in "0123456789abcdef" for value in report.model_package_projection_sha256
            )
            or (report.trial_status == "SUCCEEDED")
            == (report.finalist_model_binding_sha256 == null_binding_sha256)
        ):
            raise BarStateRunError("duplicate semantic binding hashes drifted")
        if report.decision_label != (
            "DISCOVERY_FINALIST" if report.trial_status == "SUCCEEDED" else "DISCOVERY_REJECT"
        ):
            raise BarStateRunError("duplicate decision label differs from trial status")
        terminal_sha256_by_key[candidate_key] = report.terminal_artifact_sha256
        if published is not None:
            reused = tuple(
                sorted(
                    _evidence_identity(
                        item.artifact_role,
                        item.split_key,
                        item.shard_ordinal,
                        item.artifact,
                    )
                    for item in report.artifacts
                )
            )
            if reused != _published_candidate_evidence(published, candidate_key):
                raise BarStateRunError("recomputed evidence differs from exact duplicate consensus")
    assert global_sha256 is not None
    assert global_identity_sha256 is not None
    assert finalist_keys is not None
    if published is not None and (
        published.global_artifact.descriptor.identity_sha256 != global_identity_sha256
        or published.global_artifact.sha256 != global_sha256
    ):
        raise BarStateRunError("recomputed global artifact differs from duplicates")
    return global_sha256, finalist_keys, terminal_sha256_by_key


def _publish_engine_result(
    prepared: PreparedBarStateRun,
    provenance: BarStateRunProvenance,
    specs: Sequence[RunSpec],
    result: BarStateEngineResult,
    services: BarStateRunServices,
    *,
    code_artifact: PublishedBarArtifact,
    registration_artifact: PublishedBarArtifact,
    progress: Callable[[BarStateRunProgress], None] | None,
) -> _PublishedEngineResult:
    global_document = {
        "candidate_count": len(result.candidate_results),
        "discovery_result": dict(result.global_document),
        "schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["GLOBAL_RESULT"],
    }
    try:
        global_projection = bar_state_global_result_projection(
            global_document,
            profile=prepared.profile,
        )
    except BarStateArtifactError as error:  # pragma: no cover - validated before publication
        raise BarStateRunError(
            "validated engine GLOBAL evidence drifted before publication"
        ) from error

    shared: list[tuple[str, BarStateParquetPayload, PublishedBarArtifact]] = []
    shared_parents = (code_artifact, registration_artifact)
    for role, payloads in (
        ("FEATURE", result.feature_tables),
        ("LABEL", result.label_tables),
    ):
        for payload in payloads:
            artifact = _publish_parquet_payload(
                prepared,
                services,
                kind=role,  # type: ignore[arg-type]
                payload=payload,
                lineage=_artifact_lineage(
                    prepared,
                    provenance,
                    specs,
                    parents=shared_parents,
                ),
            )
            shared.append((role, payload, artifact))
    _notify(progress, stage="SHARED_EVIDENCE_PUBLISHED", completed=len(shared), total=len(shared))

    models: dict[str, PublishedBarArtifact] = {}
    model_package_projection_sha256_by_key: dict[str, str] = {}
    oos: dict[str, tuple[tuple[BarStateParquetPayload, PublishedBarArtifact], ...]] = {}
    for index, candidate in enumerate(result.candidate_results, start=1):
        candidate_selection = candidate.terminal_document["candidate_selection"]
        finalist_binding = candidate.terminal_document["discovery_final_fit_model"]
        model_document = {
            "candidate_key": candidate.candidate_key,
            "fold_models": [dict(item) for item in candidate.model_documents],
            "fold_model_count": len(candidate.model_documents),
            "schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["MODEL"],
        }
        try:
            package_projection = bar_state_model_package_projection(
                model_document,
                expected_candidate_key=candidate.candidate_key,
                expected_binding=(
                    None if finalist_binding is None else finalist_binding  # type: ignore[arg-type]
                ),
                profile=prepared.profile,
            )
        except BarStateArtifactError as error:  # pragma: no cover - engine output is typed
            raise BarStateRunError("candidate MODEL package semantic projection drifted") from error
        model_package_projection_sha256_by_key[candidate.candidate_key] = package_projection.sha256
        model = services.publish_json(
            prepared.project_root,
            kind="MODEL",
            artifact_key_suffix=candidate.candidate_key,
            document=model_document,
            record_count=len(candidate.model_documents),
            lineage=_artifact_lineage(
                prepared,
                provenance,
                specs,
                parents=tuple(item[2] for item in shared),
                candidate_key=candidate.candidate_key,
            ),
            logical_identity={
                "candidate_key": candidate.candidate_key,
                "candidate_selection_sha256": canonical_sha256(candidate_selection),
                "candidate_selection_projection_sha256": (
                    global_projection.candidate_selection_projection_sha256_by_key[
                        candidate.candidate_key
                    ]
                ),
                "global_evidence_projection_sha256": (global_projection.evidence_projection_sha256),
                "finalist_model_binding": finalist_binding,
                "finalist_model_binding_sha256": canonical_sha256(finalist_binding),
                "model_package_projection": dict(package_projection.projection),
                "model_package_projection_sha256": package_projection.sha256,
            },
            profile=prepared.profile,
        )
        models[candidate.candidate_key] = model
        candidate_oos: list[tuple[BarStateParquetPayload, PublishedBarArtifact]] = []
        expected_oos_rows = global_projection.candidate_oos_trade_record_count_by_key[
            candidate.candidate_key
        ]
        if (
            len(candidate.oos_trade_tables) != 1
            or sum(payload.row_count for payload in candidate.oos_trade_tables) != expected_oos_rows
        ):
            raise BarStateRunError("candidate OOS trade rows differ from GLOBAL portfolio cells")
        for payload in candidate.oos_trade_tables:
            artifact = _publish_parquet_payload(
                prepared,
                services,
                kind="OOS_TRADE",
                payload=payload,
                lineage=_artifact_lineage(
                    prepared,
                    provenance,
                    specs,
                    parents=(model, *tuple(item[2] for item in shared if item[0] == "LABEL")),
                    candidate_key=candidate.candidate_key,
                ),
            )
            candidate_oos.append((payload, artifact))
        oos[candidate.candidate_key] = tuple(candidate_oos)
        _notify(
            progress,
            stage="CANDIDATE_EVIDENCE_PUBLISHED",
            completed=index,
            total=len(result.candidate_results),
        )

    all_evidence = (
        tuple(item[2] for item in shared)
        + tuple(models.values())
        + tuple(artifact for values in oos.values() for _, artifact in values)
    )
    global_artifact = services.publish_json(
        prepared.project_root,
        kind="GLOBAL_RESULT",
        artifact_key_suffix=prepared.config.candidate_catalog_sha256,
        document=global_document,
        record_count=len(result.candidate_results),
        lineage=_artifact_lineage(
            prepared,
            provenance,
            specs,
            parents=all_evidence,
        ),
        logical_identity={
            "candidate_catalog_sha256": prepared.config.candidate_catalog_sha256,
            "candidate_evidence_slice_sha256_by_key": dict(
                global_projection.candidate_evidence_slice_sha256_by_key
            ),
            "candidate_oos_trade_record_count_by_key": dict(
                global_projection.candidate_oos_trade_record_count_by_key
            ),
            "candidate_selection_sha256_by_key": dict(
                global_projection.candidate_selection_sha256_by_key
            ),
            "candidate_selection_projection_sha256_by_key": dict(
                global_projection.candidate_selection_projection_sha256_by_key
            ),
            "global_evidence_projection_sha256": (global_projection.evidence_projection_sha256),
            "finalist_model_binding_sha256_by_key": dict(
                global_projection.finalist_model_binding_sha256_by_key
            ),
            "finalist_model_binding_by_key": {
                candidate_key: (
                    None
                    if (binding := global_projection.finalist_bindings.get(candidate_key)) is None
                    else dict(binding)
                )
                for candidate_key in global_projection.candidate_selections
            },
            "model_package_projection_sha256_by_key": dict(
                sorted(model_package_projection_sha256_by_key.items())
            ),
        },
        profile=prepared.profile,
    )
    terminals: dict[str, PublishedBarArtifact] = {}
    for candidate in result.candidate_results:
        document = {
            "candidate_key": candidate.candidate_key,
            "compact_summary": dict(candidate.compact_summary),
            "decision_label": candidate.decision_label,
            "result": dict(candidate.terminal_document),
            "schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["TERMINAL_RESULT"],
            "trial_status": candidate.trial_status,
        }
        terminals[candidate.candidate_key] = services.publish_json(
            prepared.project_root,
            kind="TERMINAL_RESULT",
            artifact_key_suffix=candidate.candidate_key,
            document=document,
            record_count=1,
            lineage=_artifact_lineage(
                prepared,
                provenance,
                specs,
                parents=(
                    global_artifact,
                    models[candidate.candidate_key],
                    *tuple(item[1] for item in oos[candidate.candidate_key]),
                ),
                candidate_key=candidate.candidate_key,
            ),
            logical_identity={
                "candidate_key": candidate.candidate_key,
                "candidate_evidence_slice_sha256": (
                    global_projection.candidate_evidence_slice_sha256_by_key[
                        candidate.candidate_key
                    ]
                ),
                "candidate_selection_sha256": canonical_sha256(
                    candidate.terminal_document["candidate_selection"]
                ),
                "candidate_selection_projection_sha256": (
                    global_projection.candidate_selection_projection_sha256_by_key[
                        candidate.candidate_key
                    ]
                ),
                "compact_summary_sha256": canonical_sha256(candidate.compact_summary),
                "decision_label": candidate.decision_label,
                "global_evidence_projection_sha256": (global_projection.evidence_projection_sha256),
                "finalist_model_binding": candidate.terminal_document["discovery_final_fit_model"],
                "finalist_model_binding_sha256": canonical_sha256(
                    candidate.terminal_document["discovery_final_fit_model"]
                ),
                "model_package_projection_sha256": (
                    model_package_projection_sha256_by_key[candidate.candidate_key]
                ),
                "trial_status": candidate.trial_status,
            },
            profile=prepared.profile,
        )
    _notify(
        progress,
        stage="COMPLETE_RESULT_PUBLISHED",
        completed=len(terminals),
        total=len(terminals),
    )
    return _PublishedEngineResult(
        shared=tuple(shared),
        models=models,
        oos=oos,
        global_artifact=global_artifact,
        terminals=terminals,
    )


def _close_engine_payloads(result: BarStateEngineResult | None) -> None:
    if result is None:
        return
    for payload in (*result.feature_tables, *result.label_tables):
        payload.close()
    for candidate in result.candidate_results:
        for payload in candidate.oos_trade_tables:
            payload.close()


def _abort_active_attempts(
    database_url: str,
    active_attempts: Mapping[str, _ActiveAttempt],
    services: BarStateRunServices,
    *,
    profile: BarStateCampaignProfile,
) -> tuple[str, ...]:
    failures: list[str] = []
    for candidate_key, identity in tuple(active_attempts.items()):
        try:
            services.abort_attempt(
                database_url,
                research_run_attempt_id=identity.research_run_attempt_id,
                candidate_key=candidate_key,
                run_fingerprint=identity.run_fingerprint,
                error_message="Governed bar-state Discovery aborted before complete publication",
                profile=profile,
            )
        except Exception as cleanup_error:  # noqa: BLE001 - report unresolved ledger rows
            failures.append(f"{candidate_key}:{type(cleanup_error).__name__}")
    return tuple(failures)


def execute_prepared_bar_state_run(
    prepared: PreparedBarStateRun,
    *,
    mode: RunMode = "PLAN_ONLY",
    database_url: str | None = None,
    services: BarStateRunServices | None = None,
    progress: Callable[[BarStateRunProgress], None] | None = None,
) -> BarStateResearchRunReport:
    """Run the frozen Discovery program after all twelve candidates are bound."""

    if not isinstance(prepared, PreparedBarStateRun):
        raise BarStateRunError("prepared must be a PreparedBarStateRun")
    if mode not in {"PLAN_ONLY", "RUN"}:
        raise BarStateRunError("mode must be PLAN_ONLY or RUN")
    _notify(progress, stage="PLAN_READY", completed=12, total=12)
    if mode == "PLAN_ONLY":
        return BarStateResearchRunReport(mode=mode, disposition="PLANNED", plan=prepared)
    if not isinstance(database_url, str) or not database_url.strip():
        raise BarStateRunError("RUN requires a non-empty database_url")
    active = services or _default_services()
    active_attempts: dict[str, _ActiveAttempt] = {}
    engine_result: BarStateEngineResult | None = None
    try:
        if prepared.profile.amends_campaign_key is not None:
            active.require_predecessor(
                database_url,
                successor_profile=prepared.profile,
            )
            _notify(progress, stage="PREDECESSOR_GATE_VERIFIED", completed=1, total=1)
        provenance = _capture_provenance(prepared, database_url, active)
        specs = build_bar_state_run_specs(prepared, provenance)
        code_artifact = _publish_code_snapshot(prepared, provenance, specs)
        registration_artifact, registration_document = _publish_registration(
            prepared,
            provenance,
            specs,
            code_artifact,
        )
        active.register_campaign(
            database_url,
            prepared.project_root,
            definition=prepared.registry_definition,
            split_plan=prepared.outer_split_plan,
            code_commit=provenance.code_commit,
            registration_artifact=registration_artifact,
            expected_registration_document=registration_document,
        )
        active.register_artifact(database_url, prepared.project_root, code_artifact)
        _notify(progress, stage="CAMPAIGN_PREREGISTERED", completed=12, total=12)

        spec_ids: dict[str, int] = {}
        spec_by_key = dict(zip(prepared.candidate_keys, specs, strict=True))
        for index, candidate_key in enumerate(prepared.candidate_keys, start=1):
            spec = spec_by_key[candidate_key]
            registration = active.register_spec(
                database_url,
                spec,
                definition=prepared.registry_definition,
                split_plan=prepared.outer_split_plan,
                candidate_key=candidate_key,
            )
            spec_ids[candidate_key] = _registered_spec_id(registration, spec)
            _notify(progress, stage="RUN_SPEC_BOUND", completed=index, total=12)

        reservations: dict[str, Any] = {}
        duplicate_reports: dict[str, BarStateReuseValidationReport] = {}
        for index, candidate_key in enumerate(prepared.candidate_keys, start=1):
            spec = spec_by_key[candidate_key]
            reservation = active.reserve_attempt(
                database_url,
                run_fingerprint=spec.fingerprint,
            )
            if getattr(reservation, "execute", None) is True:
                attempt_id = getattr(reservation, "research_run_attempt_id", None)
                if isinstance(attempt_id, bool) or not isinstance(attempt_id, int):
                    raise BarStateRunError("executable reservation identity is invalid")
                active_attempts[candidate_key] = _ActiveAttempt(
                    attempt_id,
                    spec.fingerprint,
                )
            _validate_reservation(
                reservation,
                research_run_spec_id=spec_ids[candidate_key],
            )
            reservations[candidate_key] = reservation
            if reservation.execute:
                state = active.start_attempt(
                    database_url,
                    research_run_attempt_id=reservation.research_run_attempt_id,
                )
                if (
                    getattr(state, "status", None) != "RUNNING"
                    or getattr(state, "research_run_spec_id", None) != spec_ids[candidate_key]
                    or getattr(state, "research_run_attempt_id", None)
                    != reservation.research_run_attempt_id
                ):
                    raise BarStateRunError("started attempt identity drift")
            _notify(progress, stage="ATTEMPT_BOUND", completed=index, total=12)

        for candidate_key in prepared.candidate_keys:
            reservation = reservations[candidate_key]
            if not reservation.execute:
                duplicate_reports[candidate_key] = active.validate_duplicate(
                    database_url,
                    prepared.project_root,
                    reservation=reservation,
                    candidate_key=candidate_key,
                    profile=prepared.profile,
                )
        if duplicate_reports:
            _notify(
                progress,
                stage="DUPLICATE_EVIDENCE_VERIFIED",
                completed=len(duplicate_reports),
                total=len(duplicate_reports),
            )
        if not active_attempts:
            global_sha256, finalists, terminal_sha256_by_key = _validate_duplicate_consensus(
                prepared, duplicate_reports
            )
            _assert_provenance_unchanged(prepared, provenance, database_url, active)
            return BarStateResearchRunReport(
                mode=mode,
                disposition="SKIPPED_DUPLICATE",
                plan=prepared,
                candidate_runs=tuple(
                    BarStateCandidateRunReport(
                        candidate_key=key,
                        run_fingerprint=spec_by_key[key].fingerprint,
                        research_run_attempt_id=reservations[key].research_run_attempt_id,
                        disposition="SKIPPED_DUPLICATE",
                        terminal_artifact_sha256=terminal_sha256_by_key[key],
                    )
                    for key in prepared.candidate_keys
                ),
                global_result_sha256=global_sha256,
                finalist_keys=finalists,
            )

        _assert_provenance_unchanged(prepared, provenance, database_url, active)
        # First outcome-observing call: all executable attempts are RUNNING and
        # every frozen candidate has already been registered and bound.
        engine_result = active.engine(
            prepared,
            candidate_keys=prepared.candidate_keys,
            progress=progress,
        )
        engine_finalists = _validate_engine_result(prepared, engine_result)
        _assert_provenance_unchanged(prepared, provenance, database_url, active)
        published = _publish_engine_result(
            prepared,
            provenance,
            specs,
            engine_result,
            active,
            code_artifact=code_artifact,
            registration_artifact=registration_artifact,
            progress=progress,
        )
        _assert_provenance_unchanged(prepared, provenance, database_url, active)
        if duplicate_reports:
            _validate_duplicate_consensus(
                prepared,
                duplicate_reports,
                published=published,
            )

        reports: list[BarStateCandidateRunReport] = []
        for candidate_key in prepared.candidate_keys:
            if candidate_key not in active_attempts:
                reports.append(
                    BarStateCandidateRunReport(
                        candidate_key=candidate_key,
                        run_fingerprint=spec_by_key[candidate_key].fingerprint,
                        research_run_attempt_id=reservations[candidate_key].research_run_attempt_id,
                        disposition="SKIPPED_DUPLICATE",
                        terminal_artifact_sha256=duplicate_reports[
                            candidate_key
                        ].terminal_artifact_sha256,
                    )
                )
        by_key = {item.candidate_key: item for item in engine_result.candidate_results}
        executable_keys = tuple(key for key in prepared.candidate_keys if key in active_attempts)
        for index, candidate_key in enumerate(executable_keys, start=1):
            _assert_provenance_unchanged(prepared, provenance, database_url, active)
            attempt = active_attempts[candidate_key]
            candidate = by_key[candidate_key]
            for role, payload, artifact in published.shared:
                active.link_artifact(
                    database_url,
                    prepared.project_root,
                    research_run_attempt_id=attempt.research_run_attempt_id,
                    candidate_key=candidate_key,
                    artifact_role=role,
                    split_key=payload.split_key,
                    shard_ordinal=payload.shard_ordinal,
                    artifact=artifact,
                    profile=prepared.profile,
                )
            active.link_artifact(
                database_url,
                prepared.project_root,
                research_run_attempt_id=attempt.research_run_attempt_id,
                candidate_key=candidate_key,
                artifact_role="MODEL",
                split_key="discovery",
                shard_ordinal=0,
                artifact=published.models[candidate_key],
                profile=prepared.profile,
            )
            for payload, artifact in published.oos[candidate_key]:
                active.link_artifact(
                    database_url,
                    prepared.project_root,
                    research_run_attempt_id=attempt.research_run_attempt_id,
                    candidate_key=candidate_key,
                    artifact_role="OOS_TRADE",
                    split_key=payload.split_key,
                    shard_ordinal=payload.shard_ordinal,
                    artifact=artifact,
                    profile=prepared.profile,
                )
            for role, artifact in (
                ("GLOBAL_RESULT", published.global_artifact),
                ("TERMINAL_RESULT", published.terminals[candidate_key]),
            ):
                active.link_artifact(
                    database_url,
                    prepared.project_root,
                    research_run_attempt_id=attempt.research_run_attempt_id,
                    candidate_key=candidate_key,
                    artifact_role=role,
                    split_key="discovery",
                    shard_ordinal=0,
                    artifact=artifact,
                    profile=prepared.profile,
                )
            terminal = active.terminalize(
                database_url,
                prepared.project_root,
                research_run_attempt_id=attempt.research_run_attempt_id,
                candidate_key=candidate_key,
                trial_status=candidate.trial_status,
                decision_label=candidate.decision_label,
                compact_summary=candidate.compact_summary,
                profile=prepared.profile,
            )
            if (
                getattr(terminal, "research_run_attempt_id", None)
                != attempt.research_run_attempt_id
                or getattr(terminal, "research_run_spec_id", None) != spec_ids[candidate_key]
                or getattr(terminal, "trial_status", None) != candidate.trial_status
            ):
                raise BarStateRunError("terminal registration identity drift")
            del active_attempts[candidate_key]
            reports.append(
                BarStateCandidateRunReport(
                    candidate_key=candidate_key,
                    run_fingerprint=spec_by_key[candidate_key].fingerprint,
                    research_run_attempt_id=attempt.research_run_attempt_id,
                    disposition="TERMINAL_REGISTERED",
                    terminal_artifact_sha256=published.terminals[candidate_key].sha256,
                )
            )
            _notify(
                progress,
                stage="CANDIDATE_TERMINAL_REGISTERED",
                completed=index,
                total=len(executable_keys),
            )
        _assert_provenance_unchanged(prepared, provenance, database_url, active)
        reports_by_key = {item.candidate_key: item for item in reports}
        finalists = engine_finalists
        if len(finalists) > 4:
            raise BarStateRunError("engine exceeded the four-candidate finalist budget")
        return BarStateResearchRunReport(
            mode=mode,
            disposition="COMPLETED",
            plan=prepared,
            candidate_runs=tuple(reports_by_key[key] for key in prepared.candidate_keys),
            global_result_sha256=published.global_artifact.sha256,
            finalist_keys=finalists,
        )
    except BaseException as error:
        cleanup = _abort_active_attempts(
            database_url,
            active_attempts,
            active,
            profile=prepared.profile,
        )
        suffix = "" if not cleanup else f"; cleanup failures={cleanup}"
        raise BarStateRunError(f"governed bar-state Discovery failed{suffix}") from error
    finally:
        _close_engine_payloads(engine_result)


def run_governed_bar_state_discovery(
    project_root: Path | str,
    *,
    mode: RunMode = "PLAN_ONLY",
    database_url: str | None = None,
    manifest_path: Path | None = None,
    profile: BarStateCampaignProfile = BAR_STATE_V2_PROFILE,
    services: BarStateRunServices | None = None,
    progress: Callable[[BarStateRunProgress], None] | None = None,
) -> BarStateResearchRunReport:
    """Load the verified manifest and plan or execute Discovery v2."""

    active = services or _default_services()
    prepared = load_prepared_bar_state_run(
        project_root,
        manifest_path=manifest_path,
        profile=profile,
        services=active,
    )
    return execute_prepared_bar_state_run(
        prepared,
        mode=mode,
        database_url=database_url,
        services=active,
        progress=progress,
    )
