"""Runnable, resumable Phase 1A governed shared-outcome orchestration.

The expensive raw decode is the only parallel stage.  Every scenario, signal,
direction, and barrier cell subsequently shares one chronological event pass.
Source-date checkpoints contain the complete replay state plus an immutable
manifest of already-published detail shards, so a resumed process neither
replays nor silently drops an economic result.
"""

from __future__ import annotations

import heapq
import subprocess
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any, Final, Literal

import psycopg
from psycopg.rows import dict_row

from systematic_fx.backtest.barriers import BARRIER_TICKS, Direction
from systematic_fx.backtest.event_cache import (
    MAX_CACHE_WORKERS,
    CachedExecutableQuote,
    DailyCacheReport,
    build_daily_cache_batch,
    read_daily_executable_cache,
)
from systematic_fx.backtest.shared_replay import (
    EXECUTION_SCENARIOS,
    SharedExecutableQuote,
    SharedReplay,
)
from systematic_fx.db.data_registry import load_source_manifest_bundle
from systematic_fx.db.migrations import discover_migrations
from systematic_fx.db.outcome_registry import (
    OUTCOME_ENGINE_VERSION,
    OutcomePredecessorGate,
    OutcomeSourceArtifactSet,
    complete_phase1a_outcome_replay,
    fail_phase1a_outcome_replay,
    load_latest_phase1a_outcome_checkpoint,
    load_phase1a_outcome_source_artifacts,
    load_phase1a_p1_predecessor_gate,
    phase1a_outcome_parameters,
    register_phase1a_outcome_checkpoint,
    reserve_phase1a_outcome_replay,
    start_phase1a_outcome_replay,
    validate_complete_cell_summaries,
)
from systematic_fx.db.run_registry import register_run_spec
from systematic_fx.features.screening import (
    FEATURE_VERSION,
    load_phase1a_screening_config,
)
from systematic_fx.research.hypotheses import canonical_sha256
from systematic_fx.research.outcome_config import (
    OUTCOME_CONFIG_RELATIVE_PATH,
    P1_OUTCOME_CONFIG_RELATIVE_PATH,
    P1_QUERY_ID,
    P5_QUERY_ID,
    OutcomeReplayConfig,
    load_outcome_replay_config,
)
from systematic_fx.research.outcome_economics import OutcomeEconomicsAccumulator
from systematic_fx.research.outcome_inputs import (
    CanonicalDiscoveryArtifact,
    DailyReplayPartition,
    DiscoveryInputs,
    OutcomeInputError,
    OutcomeInputPlan,
    TerminalResolution,
    apply_terminal_resolution,
    load_configured_discovery_inputs,
    plan_configured_replay_inputs,
    resolve_terminal_partitions,
)
from systematic_fx.research.provenance import (
    build_code_snapshot,
    dependency_lock_sha256,
    publish_code_snapshot,
    runtime_environment,
)
from systematic_fx.research.run_spec import RunSpec
from systematic_fx.validation.splits import (
    CALENDAR_VERSION,
    CAMPAIGN_ID,
    SPLIT_VERSION,
    build_phase1a_screening_calendar,
    build_phase1a_screening_split,
    publish_phase1a_screening_artifacts,
)

P5_PIPELINE_VERSION: Final = "phase1a_p5_outcome_pipeline_v1"
P1_PIPELINE_VERSION: Final = "phase1a_p1_05_outcome_pipeline_v1"
PIPELINE_VERSION: Final = P5_PIPELINE_VERSION
RANDOM_SEED: Final = 0
EXPECTED_SUMMARY_COUNT: Final = 3 * 2 * len(BARRIER_TICKS) ** 2
_SUPPORTED_MIGRATIONS: Final = tuple(range(1, 25))
_MODES: Final = frozenset({"PLAN_ONLY", "CACHE_ONLY", "RUN"})


class Phase1AOutcomePipelineError(RuntimeError):
    """A governed outcome replay could not be planned or resumed safely."""


@dataclass(frozen=True, slots=True)
class OutcomeProgress:
    """One compact, non-economic operator progress event."""

    stage: Literal["CACHE", "CHECKPOINT"]
    completed: int
    total: int
    source_date: date | None = None
    raw_symbol: str | None = None
    cache_created_count: int = 0
    cache_reused_count: int = 0
    source_event_count: int = 0
    detail_record_count: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "cache_created_count": self.cache_created_count,
            "cache_reused_count": self.cache_reused_count,
            "completed": self.completed,
            "detail_record_count": self.detail_record_count,
            "raw_symbol": self.raw_symbol,
            "source_date": (None if self.source_date is None else self.source_date.isoformat()),
            "source_event_count": self.source_event_count,
            "stage": self.stage,
            "total": self.total,
        }


@dataclass(frozen=True, slots=True)
class PreparedOutcomeInputs:
    """Fully verified, performance-free inputs used by all execution modes."""

    config: OutcomeReplayConfig
    calendar: Any
    split: Any
    discovery: DiscoveryInputs
    plan: OutcomeInputPlan
    source_artifacts: OutcomeSourceArtifactSet
    calendar_path: Path
    split_path: Path


@dataclass(frozen=True, slots=True)
class OutcomePipelineReport:
    """Small operator-facing report; row-level results stay below ``data/derived``."""

    pipeline_version: str
    mode: str
    query_id: str
    signal_count: int
    long_signal_count: int
    short_signal_count: int
    signal_source_date_count: int
    contract_count: int
    cache_partition_count: int
    portable_artifact_manifest_sha256: str
    rich_source_artifact_manifest_sha256: str
    signal_manifest_sha256: str
    input_plan_sha256: str
    calendar_sha256: str
    split_sha256: str
    cache_manifest_sha256: str | None = None
    terminal_resolution_sha256: str | None = None
    terminal_fallback_contract_count: int = 0
    run_fingerprint: str | None = None
    outcome_replay_manifest_id: int | None = None
    completed_source_date_count: int = 0
    source_event_count: int = 0
    detail_record_count: int = 0
    summary_row_count: int = 0
    result_artifact_path: Path | None = None
    result_artifact_sha256: str | None = None
    final_checkpoint_path: Path | None = None
    final_checkpoint_sha256: str | None = None
    final_checkpoint_sequence: int | None = None
    disposition: str = "PLANNED"

    def as_dict(self) -> dict[str, object]:
        return {
            "cache_manifest_sha256": self.cache_manifest_sha256,
            "cache_partition_count": self.cache_partition_count,
            "calendar_sha256": self.calendar_sha256,
            "completed_source_date_count": self.completed_source_date_count,
            "contract_count": self.contract_count,
            "detail_record_count": self.detail_record_count,
            "disposition": self.disposition,
            "final_checkpoint_path": (
                None if self.final_checkpoint_path is None else str(self.final_checkpoint_path)
            ),
            "final_checkpoint_sequence": self.final_checkpoint_sequence,
            "final_checkpoint_sha256": self.final_checkpoint_sha256,
            "input_plan_sha256": self.input_plan_sha256,
            "long_signal_count": self.long_signal_count,
            "mode": self.mode,
            "outcome_replay_manifest_id": self.outcome_replay_manifest_id,
            "pipeline_version": self.pipeline_version,
            "portable_artifact_manifest_sha256": (self.portable_artifact_manifest_sha256),
            "query_id": self.query_id,
            "result_artifact_path": (
                None if self.result_artifact_path is None else str(self.result_artifact_path)
            ),
            "result_artifact_sha256": self.result_artifact_sha256,
            "rich_source_artifact_manifest_sha256": (self.rich_source_artifact_manifest_sha256),
            "run_fingerprint": self.run_fingerprint,
            "short_signal_count": self.short_signal_count,
            "signal_count": self.signal_count,
            "signal_manifest_sha256": self.signal_manifest_sha256,
            "signal_source_date_count": self.signal_source_date_count,
            "source_event_count": self.source_event_count,
            "split_sha256": self.split_sha256,
            "summary_row_count": self.summary_row_count,
            "terminal_fallback_contract_count": self.terminal_fallback_contract_count,
            "terminal_resolution_sha256": self.terminal_resolution_sha256,
        }


@dataclass(frozen=True, slots=True)
class OutcomeArtifactServices:
    """Adapter for immutable cache/detail/checkpoint/result artifact publication."""

    find_cache_manifest: Callable[..., Any]
    publish_cache_manifest: Callable[..., Any]
    publish_result_shard: Callable[..., Any]
    read_result_shard: Callable[..., Any]
    publish_checkpoint: Callable[..., Any]
    load_checkpoint_artifact: Callable[..., Any]
    publish_result: Callable[..., Any]
    load_result: Callable[..., Any]


@dataclass(frozen=True, slots=True)
class OutcomePipelineServices:
    """Injectable side-effect boundary used by synthetic unit tests."""

    load_config: Callable[..., OutcomeReplayConfig]
    build_calendar: Callable[..., Any]
    build_split: Callable[..., Any]
    publish_calendar_split: Callable[..., Any]
    load_source_bundle: Callable[..., Any]
    load_source_artifacts: Callable[..., OutcomeSourceArtifactSet]
    load_discovery: Callable[..., DiscoveryInputs]
    plan_inputs: Callable[..., OutcomeInputPlan]
    build_caches: Callable[..., tuple[DailyCacheReport, ...]]
    read_cache: Callable[[DailyCacheReport], Iterator[CachedExecutableQuote]]
    git_head: Callable[[Path], str]
    build_snapshot: Callable[..., Any]
    publish_snapshot: Callable[..., Any]
    dependency_hash: Callable[[Path], str]
    runtime: Callable[[], dict[str, object]]
    postgres_runtime: Callable[..., dict[str, object]]
    register_spec: Callable[..., Any]
    reserve_replay: Callable[..., Any]
    start_replay: Callable[..., Any]
    load_checkpoint: Callable[..., Any]
    register_checkpoint: Callable[..., Any]
    complete_replay: Callable[..., Any]
    fail_replay: Callable[..., Any]
    load_predecessor_gate: Callable[..., OutcomePredecessorGate]
    replay_factory: Callable[[Sequence[Any]], SharedReplay]
    replay_from_checkpoint: Callable[[Mapping[str, object]], SharedReplay]
    economics_factory: Callable[..., OutcomeEconomicsAccumulator]
    artifacts: OutcomeArtifactServices


def _git_head(project_root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--verify", "HEAD"],
            cwd=project_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise Phase1AOutcomePipelineError("cannot resolve Git HEAD") from error
    value = result.stdout.strip()
    if len(value) not in {40, 64} or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise Phase1AOutcomePipelineError("Git HEAD is not a full lowercase object ID")
    return value


def _postgres_runtime(database_url: str, *, migrations_directory: Path) -> dict[str, object]:
    migrations = discover_migrations(migrations_directory)
    if tuple(item.version for item in migrations) != _SUPPORTED_MIGRATIONS:
        raise Phase1AOutcomePipelineError("outcome replay requires migrations 0001-0024")
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        version = connection.execute(
            "SELECT current_setting('server_version') AS version, "
            "current_setting('server_version_num') AS version_num"
        ).fetchone()
        applied = connection.execute(
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
        for item in applied
    ]
    if observed != expected or version is None:
        raise Phase1AOutcomePipelineError("PostgreSQL migration identity drift")
    return {
        "server_version": str(version["version"]),
        "server_version_num": str(version["version_num"]),
        "schema_migrations": observed,
        "schema_migrations_sha256": canonical_sha256(observed),
    }


def _artifact_services() -> OutcomeArtifactServices:
    """Import the artifact layer lazily to keep planning tests lightweight."""

    def invoke(name: str, *args: object, **kwargs: object) -> Any:
        try:
            from systematic_fx.research import outcome_artifacts

            function = getattr(outcome_artifacts, name)
        except (ImportError, AttributeError) as error:  # pragma: no cover - integration guard
            raise Phase1AOutcomePipelineError(
                "outcome artifact publisher API is unavailable"
            ) from error
        return function(*args, **kwargs)

    return OutcomeArtifactServices(
        find_cache_manifest=lambda *args, **kwargs: invoke("find_cache_manifest", *args, **kwargs),
        publish_cache_manifest=lambda *args, **kwargs: invoke(
            "publish_cache_manifest", *args, **kwargs
        ),
        publish_result_shard=lambda *args, **kwargs: invoke(
            "publish_detail_shard", *args, **kwargs
        ),
        read_result_shard=lambda *args, **kwargs: invoke("load_detail_shard", *args, **kwargs),
        publish_checkpoint=lambda *args, **kwargs: invoke(
            "publish_outcome_checkpoint", *args, **kwargs
        ),
        load_checkpoint_artifact=lambda *args, **kwargs: invoke(
            "load_outcome_checkpoint", *args, **kwargs
        ),
        publish_result=lambda *args, **kwargs: invoke(
            "publish_final_result_manifest", *args, **kwargs
        ),
        load_result=lambda *args, **kwargs: invoke("load_final_result_manifest", *args, **kwargs),
    )


def _default_services() -> OutcomePipelineServices:
    return OutcomePipelineServices(
        load_config=load_outcome_replay_config,
        build_calendar=build_phase1a_screening_calendar,
        build_split=build_phase1a_screening_split,
        publish_calendar_split=publish_phase1a_screening_artifacts,
        load_source_bundle=load_source_manifest_bundle,
        load_source_artifacts=load_phase1a_outcome_source_artifacts,
        load_discovery=load_configured_discovery_inputs,
        plan_inputs=plan_configured_replay_inputs,
        build_caches=build_daily_cache_batch,
        read_cache=read_daily_executable_cache,
        git_head=_git_head,
        build_snapshot=build_code_snapshot,
        publish_snapshot=publish_code_snapshot,
        dependency_hash=dependency_lock_sha256,
        runtime=runtime_environment,
        postgres_runtime=_postgres_runtime,
        register_spec=register_run_spec,
        reserve_replay=reserve_phase1a_outcome_replay,
        start_replay=start_phase1a_outcome_replay,
        load_checkpoint=load_latest_phase1a_outcome_checkpoint,
        register_checkpoint=register_phase1a_outcome_checkpoint,
        complete_replay=complete_phase1a_outcome_replay,
        fail_replay=fail_phase1a_outcome_replay,
        load_predecessor_gate=load_phase1a_p1_predecessor_gate,
        replay_factory=SharedReplay,
        replay_from_checkpoint=SharedReplay.from_checkpoint,
        economics_factory=OutcomeEconomicsAccumulator,
        artifacts=_artifact_services(),
    )


def _strict_root(value: Path | str, *, label: str, expected_name: str | None = None) -> Path:
    requested = Path(value).expanduser()
    if requested.is_symlink():
        raise Phase1AOutcomePipelineError(f"{label} cannot be a symbolic link")
    try:
        root = requested.resolve(strict=True)
    except OSError as error:
        raise Phase1AOutcomePipelineError(f"{label} does not exist") from error
    if not root.is_dir() or (expected_name is not None and root.name != expected_name):
        raise Phase1AOutcomePipelineError(f"{label} is not the expected directory")
    return root


def _data_layout(data_root: Path, *, create_derived: bool) -> tuple[Path, Path]:
    raw = data_root / "mbp-10"
    derived = data_root / "derived"
    if raw.is_symlink() or not raw.is_dir():
        raise Phase1AOutcomePipelineError("data/mbp-10 must be a non-symlink directory")
    if create_derived:
        derived.mkdir(parents=True, exist_ok=True)
    if derived.is_symlink() or not derived.is_dir():
        raise Phase1AOutcomePipelineError("data/derived must be a non-symlink directory")
    manifests = derived / "manifests"
    if create_derived:
        manifests.mkdir(parents=True, exist_ok=True)
    if manifests.is_symlink() or not manifests.is_dir():
        raise Phase1AOutcomePipelineError("data/derived/manifests is unsafe")
    return raw.resolve(strict=True), manifests.resolve(strict=True)


def _pipeline_version(query_id: str) -> str:
    if query_id == P5_QUERY_ID:
        return P5_PIPELINE_VERSION
    if query_id == P1_QUERY_ID:
        return P1_PIPELINE_VERSION
    raise Phase1AOutcomePipelineError("unsupported governed outcome query")


def _artifact_identity(config: OutcomeReplayConfig) -> Any:
    """Build the candidate artifact namespace lazily for lightweight planning."""

    try:
        from systematic_fx.research.outcome_artifacts import OutcomeArtifactIdentity

        return OutcomeArtifactIdentity(
            query_id=config.query_id,
            outcome_config_id=config.outcome_config_id,
            outcome_artifact_schema=config.outcome_artifact_schema,
            source_slice_count=len(config.slice_indices),
            source_occurrence_count=config.expected_signal_count,
            summary_row_count=config.expected_summary_row_count,
        )
    except (ImportError, TypeError, ValueError) as error:
        raise Phase1AOutcomePipelineError("outcome artifact identity is invalid") from error


def _as_discovery_descriptors(
    source: OutcomeSourceArtifactSet,
) -> tuple[CanonicalDiscoveryArtifact, ...]:
    return tuple(
        CanonicalDiscoveryArtifact(
            slice_index=item.slice_index,
            path=item.path,
            sha256=item.sha256,
            byte_size=item.byte_size,
        )
        for item in source.artifacts
    )


def _validate_engine_scenarios(config: OutcomeReplayConfig) -> None:
    configured = {
        item.scenario_id: (
            item.routing_delay_ns,
            item.entry_additional_adverse_ticks,
            item.take_profit_trade_through_ticks,
            item.stop_total_minimum_adverse_ticks,
            item.other_market_exit_additional_adverse_ticks,
        )
        for item in config.scenarios
    }
    implemented = {
        item.scenario_id: (
            item.routing_delay_ns,
            item.entry_adverse_ticks,
            item.take_profit_trade_through_ticks,
            item.stop_minimum_adverse_ticks,
            item.terminal_exit_adverse_ticks,
        )
        for item in EXECUTION_SCENARIOS
    }
    if configured != implemented or any(
        item.stop_latency_ns != item.routing_delay_ns for item in EXECUTION_SCENARIOS
    ):
        raise Phase1AOutcomePipelineError("shared replay execution scenario drift")


def _validate_inputs(prepared: PreparedOutcomeInputs) -> None:
    config = prepared.config
    discovery = prepared.discovery
    plan = prepared.plan
    directions = Counter(signal.direction for signal in discovery.signals)
    source_dates = {signal.source_date for signal in discovery.signals}
    contracts = {signal.contract for signal in discovery.signals}
    planned_source_dates = tuple(partition.key[0] for partition in plan.partitions)
    completed_source_dates = tuple(dict.fromkeys(planned_source_dates))
    expected_directions = dict(config.expected_direction_counts)
    checks = (
        (
            tuple(item.slice_index for item in discovery.artifacts) == config.slice_indices,
            "frozen source slices",
        ),
        (len(discovery.signals) == config.expected_signal_count, "frozen signals"),
        (
            directions[Direction.LONG] == expected_directions["LONG"],
            "frozen LONG signals",
        ),
        (
            directions[Direction.SHORT] == expected_directions["SHORT"],
            "frozen SHORT signals",
        ),
        (
            len(source_dates) == config.expected_signal_source_date_count,
            "frozen signal dates",
        ),
        (len(contracts) == config.expected_contract_count, "frozen contracts"),
        (
            len(plan.partitions) == config.expected_cache_partition_count,
            "frozen cache partitions",
        ),
        (
            len(completed_source_dates) == config.expected_completed_source_date_count,
            "frozen completed source dates",
        ),
        (
            planned_source_dates == tuple(sorted(planned_source_dates))
            and len(planned_source_dates) == len(completed_source_dates),
            "one ordered cache partition per completed source date",
        ),
        (
            bool(completed_source_dates)
            and completed_source_dates[0] == config.expected_first_completed_source_date,
            "frozen first completed source date",
        ),
        (
            bool(completed_source_dates)
            and completed_source_dates[-1] == config.expected_last_completed_source_date,
            "frozen last completed source date",
        ),
        (
            discovery.artifact_manifest_sha256 == config.expected_artifact_manifest_sha256,
            "portable artifact manifest",
        ),
        (
            discovery.signal_manifest_sha256 == config.expected_signal_manifest_sha256,
            "signal manifest",
        ),
        (plan.plan_sha256 == config.expected_input_plan_sha256, "input plan"),
    )
    failed = [label for condition, label in checks if not condition]
    if failed:
        raise Phase1AOutcomePipelineError(
            f"frozen {config.query_id} input drift: " + ", ".join(failed)
        )
    if (
        config.expected_signal_count != len(discovery.signals)
        or config.expected_cache_partition_count != len(plan.partitions)
        or config.expected_completed_source_date_count != len(completed_source_dates)
        or config.expected_last_completed_source_date != completed_source_dates[-1]
        or plan.discovery_input_manifest_sha256 != discovery.input_manifest_sha256
        or prepared.source_artifacts.occurrence_count != len(discovery.signals)
        # The input plan binds the ordered source-date sequence, while the
        # calendar artifact SHA binds the complete calendar document (including
        # its source/QC lineage).  They are deliberately distinct identities.
        or plan.calendar_sha256
        != canonical_sha256(
            [source_date.isoformat() for source_date in prepared.calendar.source_dates]
        )
        or prepared.split.calendar_sha256 != prepared.calendar.sha256
    ):
        raise Phase1AOutcomePipelineError("outcome config, calendar, and planned inputs drift")
    _validate_engine_scenarios(config)


def _prepare_inputs(
    *,
    project: Path,
    data: Path,
    raw: Path,
    manifests: Path,
    database_url: str,
    services: OutcomePipelineServices,
    publish_control_plane: bool,
    config_path: Path,
) -> PreparedOutcomeInputs:
    config = services.load_config(project, config_path=config_path)
    source_path = project / config.source_sha256_manifest_relative
    footer_path = project / config.source_footer_manifest_relative
    qc_path = manifests / "mbp10_structural_qc_v1.jsonl"
    calendar = services.build_calendar(source_path, qc_path)
    split = services.build_split(calendar)
    requested_calendar = project / config.eligible_calendar_relative
    requested_split = project / config.split_relative
    if publish_control_plane:
        publication = services.publish_calendar_split(
            calendar,
            split,
            manifest_directory=manifests,
        )
        try:
            expected_calendar = requested_calendar.resolve(strict=True)
            expected_split = requested_split.resolve(strict=True)
        except OSError as error:  # pragma: no cover - publisher promises both files
            raise Phase1AOutcomePipelineError(
                "calendar/split publication did not create both artifacts"
            ) from error
        if (
            Path(publication.calendar_path).resolve(strict=True) != expected_calendar
            or Path(publication.split_path).resolve(strict=True) != expected_split
            or publication.calendar_sha256 != calendar.sha256
            or publication.split_sha256 != split.sha256
        ):
            raise Phase1AOutcomePipelineError("calendar/split publication identity drift")
    else:
        # PLAN_ONLY is a genuinely read-only audit.  It verifies the already
        # published immutable control-plane bytes instead of "reusing" them
        # through a publisher that could create files.
        for path, expected, label in (
            (requested_calendar, calendar.canonical_json(), "calendar"),
            (requested_split, split.canonical_json(), "split"),
        ):
            if path.is_symlink() or not path.is_file() or path.read_bytes() != expected:
                raise Phase1AOutcomePipelineError(
                    f"published {label} artifact differs from the rebuilt identity"
                )
        expected_calendar = requested_calendar.resolve(strict=True)
        expected_split = requested_split.resolve(strict=True)
    bundle = services.load_source_bundle(footer_path, source_path)
    if (
        bundle.hash_manifest_sha256 != calendar.source_manifest_sha256
        or Path(bundle.footer_manifest_path).resolve(strict=True)
        != footer_path.resolve(strict=True)
        or Path(bundle.hash_manifest_path).resolve(strict=True) != source_path.resolve(strict=True)
    ):
        raise Phase1AOutcomePipelineError("source manifest bundle identity drift")
    source_artifacts = services.load_source_artifacts(
        database_url,
        data_root=data,
        query_id=config.query_id,
    )
    discovery = services.load_discovery(_as_discovery_descriptors(source_artifacts), config)
    plan = services.plan_inputs(
        config,
        discovery,
        source_manifest=bundle,
        mbp10_root=raw,
        calendar_source_dates=calendar.source_dates,
    )
    prepared = PreparedOutcomeInputs(
        config=config,
        calendar=calendar,
        split=split,
        discovery=discovery,
        plan=plan,
        source_artifacts=source_artifacts,
        calendar_path=expected_calendar,
        split_path=expected_split,
    )
    _validate_inputs(prepared)
    return prepared


def _base_report(prepared: PreparedOutcomeInputs, *, mode: str) -> OutcomePipelineReport:
    directions = Counter(signal.direction for signal in prepared.discovery.signals)
    return OutcomePipelineReport(
        pipeline_version=_pipeline_version(prepared.config.query_id),
        mode=mode,
        query_id=prepared.config.query_id,
        signal_count=len(prepared.discovery.signals),
        long_signal_count=directions[Direction.LONG],
        short_signal_count=directions[Direction.SHORT],
        signal_source_date_count=len({item.source_date for item in prepared.discovery.signals}),
        contract_count=len({item.contract for item in prepared.discovery.signals}),
        cache_partition_count=len(prepared.plan.partitions),
        portable_artifact_manifest_sha256=prepared.discovery.artifact_manifest_sha256,
        rich_source_artifact_manifest_sha256=(
            prepared.source_artifacts.source_artifact_manifest_sha256
        ),
        signal_manifest_sha256=prepared.discovery.signal_manifest_sha256,
        input_plan_sha256=prepared.plan.plan_sha256,
        calendar_sha256=prepared.calendar.sha256,
        split_sha256=prepared.split.sha256,
    )


def _validate_cache_reports(
    plan: OutcomeInputPlan,
    reports: Sequence[DailyCacheReport],
) -> tuple[DailyCacheReport, ...]:
    values = tuple(reports)
    if len(values) != len(plan.partitions):
        raise Phase1AOutcomePipelineError("cache report cardinality drift")
    for partition, report in zip(plan.partitions, values, strict=True):
        if (
            (report.source_date, report.raw_symbol) != partition.key
            or report.source_sha256 != partition.cache_spec.source_sha256
            or report.event_index_offset != partition.cache_spec.event_index_offset
            or report.cached_quote_count <= 0
        ):
            raise Phase1AOutcomePipelineError("cache report lineage drift")
    return values


def _resolve_terminals(
    plan: OutcomeInputPlan,
    reports: Sequence[DailyCacheReport],
) -> TerminalResolution:
    try:
        return resolve_terminal_partitions(plan, reports)
    except OutcomeInputError as error:
        raise Phase1AOutcomePipelineError(f"terminal resolution failed: {error}") from error


def _report_sha(value: object, *, label: str) -> str:
    digest = getattr(value, "sha256", None)
    if digest is None and hasattr(value, "artifact"):
        digest = getattr(value.artifact, "sha256", None)
    if not isinstance(digest, str) or len(digest) != 64:
        raise Phase1AOutcomePipelineError(f"{label} publisher returned no SHA-256")
    return digest


def _report_path(value: object, *, label: str) -> Path:
    path = getattr(value, "path", None)
    if path is None and hasattr(value, "artifact"):
        path = getattr(value.artifact, "path", None)
    if not isinstance(path, Path):
        raise Phase1AOutcomePipelineError(f"{label} publisher returned no Path")
    return path


def _checkpoint_sequence(value: object) -> int:
    sequence = getattr(value, "checkpoint_sequence", None)
    if sequence is None and hasattr(value, "artifact"):
        sequence = getattr(value.artifact, "checkpoint_sequence", None)
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence <= 0:
        raise Phase1AOutcomePipelineError("final checkpoint has no valid sequence")
    return sequence


def _checkpoint_source_date(value: object) -> date:
    source_date = getattr(value, "last_completed_source_date", None)
    if source_date is None and hasattr(value, "artifact"):
        source_date = getattr(value.artifact, "last_completed_source_date", None)
    if not isinstance(source_date, date):
        raise Phase1AOutcomePipelineError("final checkpoint has no valid source date")
    return source_date


def _shared_events_for_partition(
    partition: DailyReplayPartition,
    report: DailyCacheReport,
    *,
    read_cache: Callable[[DailyCacheReport], Iterator[CachedExecutableQuote]],
    terminal_resolution: TerminalResolution | None = None,
) -> Iterator[SharedExecutableQuote]:
    suppress_after_terminal = False
    resolved_terminal_ts_recv_ns: int | None = None
    if terminal_resolution is None:
        terminal_index = report.last_valid_event_index if partition.terminal else None
    else:
        terminal_key = terminal_resolution.terminal_key_by_contract.get(
            partition.cache_spec.raw_symbol
        )
        if terminal_key is None:
            raise Phase1AOutcomePipelineError("partition contract has no terminal resolution")
        suppress_after_terminal = partition.key[0] > terminal_key[0]
        terminal_index = report.last_valid_event_index if partition.key == terminal_key else None
        if partition.key == terminal_key:
            contract_resolution = next(
                item
                for item in terminal_resolution.contracts
                if item.contract_key == partition.cache_spec.raw_symbol
            )
            if (
                report.last_valid_event_index != contract_resolution.terminal_event_index
                or report.last_valid_ts_recv_ns != contract_resolution.terminal_ts_recv_ns
            ):
                raise Phase1AOutcomePipelineError("terminal report differs from its resolution")
            resolved_terminal_ts_recv_ns = contract_resolution.terminal_ts_recv_ns
    emitted_terminal = False
    for cached in read_cache(report):
        if suppress_after_terminal:
            # The reverse scan proved that every later contract partition has
            # no executable quote.  Consume it completely for cache-integrity
            # validation, but do not append invalid post-terminal observations
            # to the economic event stream.
            continue
        if terminal_index is not None and cached.quote.event_index > terminal_index:
            # Continue consuming the reader so its end-of-stream row counts,
            # valid counts, and terminal metadata are still verified.  These
            # trailing invalid observations are deliberately absent from the
            # economic stream after the mandatory terminal quote.
            continue
        terminal = terminal_index is not None and cached.quote.event_index == terminal_index
        if terminal and (
            not cached.quote.valid
            or resolved_terminal_ts_recv_ns is not None
            and cached.quote.ts_recv_ns != resolved_terminal_ts_recv_ns
        ):
            raise Phase1AOutcomePipelineError(
                "resolved terminal cache row is not its reported executable quote"
            )
        yield SharedExecutableQuote(
            contract_key=cached.contract_key,
            quote=cached.quote,
            source_date=cached.source_date,
            session_ordinal=partition.session_ordinal,
            sequence=cached.sequence,
            terminal=terminal,
        )
        emitted_terminal = emitted_terminal or terminal
    if terminal_index is not None and not emitted_terminal:
        raise Phase1AOutcomePipelineError("terminal cache quote was not emitted")


def merge_daily_shared_events(
    partitions: Sequence[DailyReplayPartition],
    reports: Sequence[DailyCacheReport],
    *,
    read_cache: Callable[[DailyCacheReport], Iterator[CachedExecutableQuote]] = (
        read_daily_executable_cache
    ),
    terminal_resolution: TerminalResolution | None = None,
) -> Iterator[SharedExecutableQuote]:
    """Merge one source date across contracts by the frozen total ordering."""

    partition_values = tuple(partitions)
    report_values = tuple(reports)
    if not partition_values or len(partition_values) != len(report_values):
        raise Phase1AOutcomePipelineError("daily cache merge inputs differ in cardinality")
    source_dates = {item.key[0] for item in partition_values}
    if len(source_dates) != 1:
        raise Phase1AOutcomePipelineError("daily cache merge requires exactly one source date")
    streams = [
        _shared_events_for_partition(
            partition,
            report,
            read_cache=read_cache,
            terminal_resolution=terminal_resolution,
        )
        for partition, report in zip(partition_values, report_values, strict=True)
    ]
    yield from heapq.merge(*streams, key=lambda event: event.ordering_key)


def _policies(
    config: OutcomeReplayConfig,
    terminal_resolution: TerminalResolution,
) -> tuple[dict[str, object], ...]:
    return (
        {
            "discovery_config_sha256": config.discovery_config_sha256,
            "discovery_definition_sha256": config.discovery_definition_sha256,
            "query_id": config.query_id,
            "signal_time_field": "bucket_end_ns",
            "variables_retained": "ALL_DISCOVERY_VARIABLE_FIELDS",
        },
        {
            "entry_gate": "LAST_VALID_BBO_AT_DECISION_WITH_MAX_AGE_1S",
            "entry_order": "ONE_ROUTED_IOC_LIMIT_WITHOUT_RETRY",
            "scenarios": [item.as_dict() for item in config.scenarios],
        },
        {
            "barrier_ticks": list(config.barrier_ticks),
            "same_timestamp_tie_break": "STOP_FIRST",
            "take_profit_fill": "TRADE_THROUGH_THEN_LIMIT_FILL",
            "stop_fill": "TRIGGER_THEN_LATENCY_MARKET_EXIT",
        },
        {
            "first_touch_active_sessions": config.first_touch_observation_sessions,
            "portfolio_continues_after_censor": True,
            "terminal_exit": terminal_resolution.terminal_exit_policy,
            "terminal_partition_resolution": (terminal_resolution.partition_resolution_policy),
            "terminal_resolution_sha256": terminal_resolution.sha256,
        },
    )


def _make_run_spec(
    prepared: PreparedOutcomeInputs,
    *,
    cache_manifest_sha256: str,
    terminal_resolution: TerminalResolution,
    code_commit: str,
    code_snapshot_sha256: str,
    dependency_sha256: str,
    runtime: Mapping[str, object],
    feature_sha256: str,
    predecessor_gate: OutcomePredecessorGate | None = None,
) -> RunSpec:
    config = prepared.config
    screening = config.screening_bundle
    parameters = {
        **config.canonical_parameters(),
        **phase1a_outcome_parameters(
            prepared.source_artifacts.source_artifact_manifest_sha256,
            query_id=config.query_id,
            predecessor_gate=predecessor_gate,
        ),
        "cache_manifest_sha256": cache_manifest_sha256,
        "cache_partition_count": len(prepared.plan.partitions),
        "input_plan_sha256": prepared.plan.plan_sha256,
        "portable_discovery_artifact_manifest_sha256": (
            prepared.discovery.artifact_manifest_sha256
        ),
        "portable_discovery_input_manifest_sha256": (prepared.discovery.input_manifest_sha256),
        "portable_signal_manifest_sha256": prepared.discovery.signal_manifest_sha256,
        "source_record_manifest_sha256": prepared.plan.source_record_manifest_sha256,
        "terminal_resolution": terminal_resolution.as_dict(),
        "terminal_resolution_sha256": terminal_resolution.sha256,
        "pipeline_version": _pipeline_version(config.query_id),
    }
    signal, entry, barrier, terminal = _policies(config, terminal_resolution)
    source_namespace = "phase1a_p5" if config.query_id == P5_QUERY_ID else "phase1a_p1_05"
    source_manifest_hashes = {
        "mbp10_footer_manifest_v1": prepared.plan.footer_manifest_sha256,
        "mbp10_source_sha256_v1": prepared.plan.source_hash_manifest_sha256,
        f"{source_namespace}_cache_manifest_v1": cache_manifest_sha256,
        f"{source_namespace}_discovery_artifacts_portable_v1": (
            prepared.discovery.artifact_manifest_sha256
        ),
        f"{source_namespace}_discovery_artifacts_registry_v1": (
            prepared.source_artifacts.source_artifact_manifest_sha256
        ),
        f"{source_namespace}_signal_manifest_v1": prepared.discovery.signal_manifest_sha256,
    }
    if predecessor_gate is not None:
        source_manifest_hashes["phase1a_p5_equivalence_audit_v1"] = (
            predecessor_gate.equivalence_audit_artifact_sha256
        )
    return RunSpec(
        campaign_id=CAMPAIGN_ID,
        experiment_id=None,
        run_kind="OUTCOME_BUILD",
        engine_version=OUTCOME_ENGINE_VERSION,
        source_manifest_hashes=source_manifest_hashes,
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
        code_commit=code_commit,
        code_snapshot_sha256=code_snapshot_sha256,
        dependency_lock_sha256=dependency_sha256,
        runtime_environment=runtime,
        random_seed=RANDOM_SEED,
        direction="BOTH",
        signal_policy=signal,
        entry_policy=entry,
        barrier_policy=barrier,
        terminal_policy=terminal,
        parameters=parameters,
    )


def _date_groups(
    plan: OutcomeInputPlan,
    reports: Sequence[DailyCacheReport],
    terminal_resolution: TerminalResolution,
) -> tuple[tuple[date, tuple[DailyReplayPartition, ...], tuple[DailyCacheReport, ...]], ...]:
    partitions = apply_terminal_resolution(plan, terminal_resolution)
    rows: list[tuple[date, tuple[DailyReplayPartition, ...], tuple[DailyCacheReport, ...]]] = []
    cursor = 0
    while cursor < len(partitions):
        source_date = partitions[cursor].key[0]
        end = cursor + 1
        while end < len(partitions) and partitions[end].key[0] == source_date:
            end += 1
        rows.append((source_date, partitions[cursor:end], tuple(reports[cursor:end])))
        cursor = end
    return tuple(rows)


def _input_lineage(
    prepared: PreparedOutcomeInputs,
    terminal_resolution: TerminalResolution,
    predecessor_gate: OutcomePredecessorGate | None = None,
) -> dict[str, object]:
    """Portable and registry-rich identities copied into every replay artifact."""

    lineage = {
        "cache_plan_sha256": prepared.plan.cache_plan_sha256,
        "calendar_sha256": prepared.calendar.sha256,
        "discovery_input_manifest_sha256": prepared.discovery.input_manifest_sha256,
        "expected_completed_source_date_count": (
            prepared.config.expected_completed_source_date_count
        ),
        "expected_last_completed_source_date": (
            prepared.config.expected_last_completed_source_date.isoformat()
        ),
        "footer_manifest_sha256": prepared.plan.footer_manifest_sha256,
        "input_plan_sha256": prepared.plan.plan_sha256,
        "portable_artifact_manifest_sha256": (prepared.discovery.artifact_manifest_sha256),
        "rich_source_artifact_manifest_sha256": (
            prepared.source_artifacts.source_artifact_manifest_sha256
        ),
        "signal_manifest_sha256": prepared.discovery.signal_manifest_sha256,
        "source_hash_manifest_sha256": prepared.plan.source_hash_manifest_sha256,
        "source_record_manifest_sha256": prepared.plan.source_record_manifest_sha256,
        "split_sha256": prepared.split.sha256,
        "terminal_resolution_sha256": terminal_resolution.sha256,
    }
    if predecessor_gate is not None:
        lineage.update(predecessor_gate.parameters)
    return lineage


def _economics(prepared: PreparedOutcomeInputs, services: OutcomePipelineServices) -> Any:
    return services.economics_factory(
        signal_directions={
            signal.signal_id: signal.direction for signal in prepared.discovery.signals
        },
        # Fixed monthly operating costs apply to every month in which the
        # replay portfolio is active, including zero-fill months after the
        # last signal while an occupied position continues toward expiry.
        observed_utc_months=tuple(
            sorted({partition.key[0].strftime("%Y-%m") for partition in prepared.plan.partitions})
        ),
        scenarios=prepared.config.scenarios,
    )


def _run_replay(
    *,
    prepared: PreparedOutcomeInputs,
    reports: tuple[DailyCacheReport, ...],
    terminal_resolution: TerminalResolution,
    cache_manifest: object,
    run_spec: RunSpec,
    reservation: object,
    database_url: str,
    data: Path,
    services: OutcomePipelineServices,
    predecessor_gate: OutcomePredecessorGate | None = None,
    progress_callback: Callable[[OutcomeProgress], None] | None = None,
) -> tuple[object, object, int, int, int, int]:
    groups = _date_groups(prepared.plan, reports, terminal_resolution)
    if (
        len(groups) != prepared.config.expected_completed_source_date_count
        or not groups
        or groups[-1][0] != prepared.config.expected_last_completed_source_date
    ):
        raise Phase1AOutcomePipelineError("completed source-date plan differs from frozen policy")
    manifest_id = int(reservation.outcome_replay_manifest_id)
    identity = _artifact_identity(prepared.config)
    services.start_replay(
        database_url,
        outcome_replay_manifest_id=manifest_id,
        run_fingerprint=run_spec.fingerprint,
        data_root=data,
    )
    latest = services.load_checkpoint(
        database_url,
        outcome_replay_manifest_id=manifest_id,
        run_fingerprint=run_spec.fingerprint,
        data_root=data,
    )
    economics = _economics(prepared, services)
    shards: list[Any] = []
    predecessor: str | None = None
    completed_count = 0
    final_checkpoint: object | None = None
    if latest is None:
        replay = services.replay_factory(
            tuple(signal.to_seed() for signal in prepared.discovery.signals)
        )
    else:
        loaded = services.artifacts.load_checkpoint_artifact(
            latest.checkpoint_artifact_path,
            data_root=data,
            expected_sha256=latest.checkpoint_artifact_sha256,
            expected_byte_size=latest.checkpoint_artifact_byte_size,
            expected_progress_metadata=latest.progress_metadata,
            # The cumulative detail lineage is consumed immediately below, one
            # daily shard at a time.  Loading it here as well would materialize
            # all prior records and double the resume I/O.
            # Cache artifacts are still byte-hashed against their immutable
            # manifest; decoding all 485 partitions here would duplicate the
            # original validated pass and the remaining replay reads.
            verify_cache_content=False,
            verify_detail_content=False,
            retain_detail_records=False,
            identity=identity,
        )
        if _report_sha(loaded.cache_manifest, label="checkpoint cache manifest") != _report_sha(
            cache_manifest, label="cache manifest"
        ):
            raise Phase1AOutcomePipelineError("resume cache manifest identity drift")
        replay = loaded.replay
        final_checkpoint = loaded
        shards.extend(loaded.detail_shards)
        if loaded.input_lineage != _input_lineage(
            prepared,
            terminal_resolution,
            predecessor_gate,
        ):
            raise Phase1AOutcomePipelineError("resume input lineage drift")
        if loaded.loaded_detail_shards:
            raise Phase1AOutcomePipelineError("resume checkpoint unexpectedly retained detail rows")
        for shard in loaded.detail_shards:
            loaded_shard = services.artifacts.read_result_shard(
                shard,
                data_root=data,
                identity=identity,
            )
            economics.extend(loaded_shard.records)
            del loaded_shard
        completed_count = int(latest.completed_source_date_count)
        predecessor = str(latest.checkpoint_artifact_sha256)
        if (
            replay.source_event_count != int(latest.source_event_count)
            or replay.drained_record_count != economics.record_count
            or len(shards) != completed_count
        ):
            raise Phase1AOutcomePipelineError("checkpoint event count drift")

    if completed_count > len(groups):
        raise Phase1AOutcomePipelineError("checkpoint exceeds the input date plan")
    if latest is not None and (
        completed_count == 0
        or groups[completed_count - 1][0] != latest.last_completed_source_date
        or replay.completed_source_date != latest.last_completed_source_date
    ):
        raise Phase1AOutcomePipelineError("checkpoint source-date boundary drift")
    for sequence, (source_date, partitions, daily_reports) in enumerate(
        groups[completed_count:], start=completed_count + 1
    ):
        replay.process(
            merge_daily_shared_events(
                partitions,
                daily_reports,
                read_cache=services.read_cache,
                terminal_resolution=terminal_resolution,
            )
        )
        replay.complete_source_date(source_date)
        if sequence == len(groups):
            replay.finish()
        records = replay.drain_result_records()
        economics.extend(records)
        shard = services.artifacts.publish_result_shard(
            records,
            data_root=data,
            run_fingerprint=run_spec.fingerprint,
            shard_sequence=sequence,
            source_date=source_date,
            identity=identity,
        )
        shards.append(shard)
        checkpoint = services.artifacts.publish_checkpoint(
            data_root=data,
            outcome_replay_manifest_id=manifest_id,
            run_fingerprint=run_spec.fingerprint,
            checkpoint_sequence=sequence,
            completed_source_date_count=sequence,
            last_completed_source_date=source_date,
            source_event_count=replay.source_event_count,
            predecessor_checkpoint_sha256=predecessor,
            replay_state=replay.checkpoint(),
            detail_shards=tuple(shards),
            cache_manifest=cache_manifest,
            input_lineage=_input_lineage(
                prepared,
                terminal_resolution,
                predecessor_gate,
            ),
            identity=identity,
        )
        checkpoint_path = _report_path(checkpoint, label="checkpoint")
        registered = services.register_checkpoint(
            database_url,
            outcome_replay_manifest_id=manifest_id,
            run_fingerprint=run_spec.fingerprint,
            checkpoint_sequence=sequence,
            completed_source_date_count=sequence,
            last_completed_source_date=source_date,
            source_event_count=replay.source_event_count,
            predecessor_checkpoint_sha256=predecessor,
            progress_metadata=checkpoint.progress_metadata,
            checkpoint_artifact_path=checkpoint_path,
            data_root=data,
            query_id=prepared.config.query_id,
        )
        predecessor = str(registered.checkpoint_artifact_sha256)
        final_checkpoint = checkpoint
        if progress_callback is not None:
            progress_callback(
                OutcomeProgress(
                    stage="CHECKPOINT",
                    completed=sequence,
                    total=len(groups),
                    source_date=source_date,
                    source_event_count=replay.source_event_count,
                    detail_record_count=replay.result_record_count,
                )
            )

    if not replay.finished:
        raise Phase1AOutcomePipelineError("replay did not reach its mandatory terminal state")
    if final_checkpoint is None:
        raise Phase1AOutcomePipelineError("finished replay has no final checkpoint identity")
    if (
        _checkpoint_sequence(final_checkpoint)
        != prepared.config.expected_completed_source_date_count
        or _checkpoint_source_date(final_checkpoint)
        != prepared.config.expected_last_completed_source_date
        or replay.completed_source_date != prepared.config.expected_last_completed_source_date
    ):
        raise Phase1AOutcomePipelineError(
            "final checkpoint differs from frozen completion boundary"
        )
    summaries = economics.finalize()
    ordered, _ = validate_complete_cell_summaries(
        summaries,
        query_id=prepared.config.query_id,
    )
    result = services.artifacts.publish_result(
        data_root=data,
        run_fingerprint=run_spec.fingerprint,
        source_artifact_manifest_sha256=(prepared.source_artifacts.source_artifact_manifest_sha256),
        cell_summaries=ordered,
        detail_shards=tuple(shards),
        cache_manifest=cache_manifest,
        input_lineage=_input_lineage(
            prepared,
            terminal_resolution,
            predecessor_gate,
        ),
        final_checkpoint=final_checkpoint,
        identity=identity,
    )
    verified_result = services.artifacts.load_result(
        result,
        data_root=data,
        # Every cache was hash-, schema-, and row-validated by the economic
        # pass.  The final reload still validates the cache-manifest hash and
        # lineage, but avoids decoding all 485 caches a second time.  Detail
        # shards are fully validated once here with one-shard peak memory.
        verify_cache_content=False,
        verify_detail_content=True,
        identity=identity,
    )
    if _report_sha(verified_result, label="verified result") != _report_sha(
        result, label="published result"
    ) or _report_path(verified_result, label="verified result") != _report_path(
        result, label="published result"
    ):
        raise Phase1AOutcomePipelineError("strict final result reload identity drift")
    services.complete_replay(
        database_url,
        outcome_replay_manifest_id=manifest_id,
        run_fingerprint=run_spec.fingerprint,
        cell_summaries=ordered,
        result_artifact_path=_report_path(verified_result, label="result"),
        data_root=data,
        query_id=prepared.config.query_id,
    )
    return (
        verified_result,
        final_checkpoint,
        len(groups),
        replay.source_event_count,
        replay.result_record_count,
        len(ordered),
    )


def _run_phase1a_outcomes(
    *,
    project_root: Path | str,
    data_root: Path | str,
    database_url: str,
    config_relative_path: Path,
    mode: Literal["PLAN_ONLY", "CACHE_ONLY", "RUN"] = "RUN",
    max_cache_workers: int | None = None,
    services: OutcomePipelineServices | None = None,
    progress_callback: Callable[[OutcomeProgress], None] | None = None,
) -> OutcomePipelineReport:
    """Plan, cache, or execute one complete governed shared replay."""

    if mode not in _MODES:
        raise Phase1AOutcomePipelineError("mode must be PLAN_ONLY, CACHE_ONLY, or RUN")
    if not isinstance(database_url, str) or not database_url.strip():
        raise Phase1AOutcomePipelineError("database_url must be a non-empty string")
    if max_cache_workers is not None and (
        isinstance(max_cache_workers, bool)
        or not isinstance(max_cache_workers, int)
        or not 1 <= max_cache_workers <= MAX_CACHE_WORKERS
    ):
        raise Phase1AOutcomePipelineError("max_cache_workers must be between 1 and 4")
    if progress_callback is not None and not callable(progress_callback):
        raise Phase1AOutcomePipelineError("progress_callback must be callable")
    project = _strict_root(project_root, label="project_root")
    data = _strict_root(data_root, label="data_root", expected_name="data")
    raw, manifests = _data_layout(data, create_derived=mode != "PLAN_ONLY")
    active = services or _default_services()
    prepared = _prepare_inputs(
        project=project,
        data=data,
        raw=raw,
        manifests=manifests,
        database_url=database_url,
        services=active,
        publish_control_plane=mode != "PLAN_ONLY",
        config_path=project / config_relative_path,
    )
    report = _base_report(prepared, mode=mode)
    if mode == "PLAN_ONLY":
        return report

    workers = (
        prepared.config.maximum_cache_workers if max_cache_workers is None else max_cache_workers
    )
    if not 1 <= workers <= MAX_CACHE_WORKERS:  # config loader owns its exact integer type
        raise Phase1AOutcomePipelineError("max_cache_workers must be between 1 and 4")
    code_commit: str | None = None
    snapshot: Any | None = None
    dependency_sha256: str | None = None
    runtime: dict[str, object] | None = None
    feature_sha256: str | None = None
    predecessor_gate: OutcomePredecessorGate | None = None
    if mode == "RUN":
        if prepared.config.query_id == P1_QUERY_ID:
            predecessor_gate = active.load_predecessor_gate(
                database_url,
                data_root=data,
            )
        # Fail on schema/provenance drift before spending hours decoding raw
        # data.  The snapshot is checked again after cache construction so a
        # worker cannot silently execute bytes different from this identity.
        code_commit = active.git_head(project)
        snapshot = active.build_snapshot(project, code_commit=code_commit)
        published_snapshot = active.publish_snapshot(snapshot, data_root=data)
        if snapshot.sha256 != published_snapshot.sha256:
            raise Phase1AOutcomePipelineError("published code snapshot identity drift")
        dependency_sha256 = active.dependency_hash(project)
        runtime = dict(active.runtime())
        runtime["postgresql"] = active.postgres_runtime(
            database_url,
            migrations_directory=project / "migrations",
        )
        runtime["phase1a_outcome_pipeline"] = {
            "cache_workers": workers,
            "pipeline_version": _pipeline_version(prepared.config.query_id),
        }
        feature_sha256 = load_phase1a_screening_config(
            project / "configs/features/phase1a_mbp10_screening_v1.toml"
        ).sha256
    loaded_cache = active.artifacts.find_cache_manifest(
        data_root=data,
        cache_plan_sha256=prepared.plan.cache_plan_sha256,
        input_manifest_sha256=prepared.discovery.input_manifest_sha256,
        # A full run hash- and row-validates every cache as it consumes the
        # single economic pass.  CACHE_ONLY has no later reader, so it performs
        # the complete content audit here.
        verify_cache_content=mode == "CACHE_ONLY",
    )
    if loaded_cache is None:
        cache_created_count = 0
        cache_reused_count = 0

        def cache_progress(
            cache_report: DailyCacheReport,
            completed_count: int,
            total_count: int,
        ) -> None:
            nonlocal cache_created_count, cache_reused_count
            if cache_report.disposition == "CREATED":
                cache_created_count += 1
            else:
                cache_reused_count += 1
            if progress_callback is not None:
                progress_callback(
                    OutcomeProgress(
                        stage="CACHE",
                        completed=completed_count,
                        total=total_count,
                        source_date=cache_report.source_date,
                        raw_symbol=cache_report.raw_symbol,
                        cache_created_count=cache_created_count,
                        cache_reused_count=cache_reused_count,
                    )
                )

        reports = _validate_cache_reports(
            prepared.plan,
            active.build_caches(
                prepared.plan.cache_specs,
                data_root=data,
                max_workers=workers,
                progress_callback=cache_progress,
            ),
        )
        cache_manifest = active.artifacts.publish_cache_manifest(
            reports,
            data_root=data,
            cache_plan_sha256=prepared.plan.cache_plan_sha256,
            input_manifest_sha256=prepared.discovery.input_manifest_sha256,
        )
    else:
        reports = _validate_cache_reports(prepared.plan, loaded_cache.reports)
        cache_manifest = loaded_cache
        if progress_callback is not None:
            progress_callback(
                OutcomeProgress(
                    stage="CACHE",
                    completed=len(reports),
                    total=len(reports),
                    cache_reused_count=len(reports),
                )
            )
    terminal_resolution = _resolve_terminals(prepared.plan, reports)
    cache_sha = _report_sha(cache_manifest, label="cache manifest")
    report = replace(
        report,
        terminal_resolution_sha256=terminal_resolution.sha256,
        terminal_fallback_contract_count=sum(
            item.trailing_non_executable_partition_count > 0
            for item in terminal_resolution.contracts
        ),
    )
    if mode == "CACHE_ONLY":
        return replace(
            report,
            cache_manifest_sha256=cache_sha,
            disposition="CACHED",
        )

    if (
        code_commit is None
        or snapshot is None
        or dependency_sha256 is None
        or runtime is None
        or feature_sha256 is None
        or active.git_head(project) != code_commit
        or active.build_snapshot(project, code_commit=code_commit).sha256 != snapshot.sha256
        or active.dependency_hash(project) != dependency_sha256
    ):
        raise Phase1AOutcomePipelineError("code or dependency identity changed during cache build")
    run_spec = _make_run_spec(
        prepared,
        cache_manifest_sha256=cache_sha,
        terminal_resolution=terminal_resolution,
        code_commit=code_commit,
        code_snapshot_sha256=snapshot.sha256,
        dependency_sha256=dependency_sha256,
        runtime=runtime,
        feature_sha256=feature_sha256,
        predecessor_gate=predecessor_gate,
    )
    active.register_spec(database_url, run_spec)
    reservation = active.reserve_replay(
        database_url,
        run_fingerprint=run_spec.fingerprint,
        source_artifact_manifest_sha256=(prepared.source_artifacts.source_artifact_manifest_sha256),
        query_id=prepared.config.query_id,
        predecessor_equivalence_audit_id=(
            None if predecessor_gate is None else predecessor_gate.equivalence_audit_id
        ),
        data_root=data,
    )
    manifest_id = int(reservation.outcome_replay_manifest_id)
    if not reservation.execute:
        return replace(
            report,
            cache_manifest_sha256=cache_sha,
            run_fingerprint=run_spec.fingerprint,
            outcome_replay_manifest_id=manifest_id,
            disposition="SKIPPED_DUPLICATE",
        )
    try:
        result, final_checkpoint, completed, events, records, summaries = _run_replay(
            prepared=prepared,
            reports=reports,
            terminal_resolution=terminal_resolution,
            cache_manifest=cache_manifest,
            run_spec=run_spec,
            reservation=reservation,
            database_url=database_url,
            data=data,
            services=active,
            predecessor_gate=predecessor_gate,
            progress_callback=progress_callback,
        )
    except Exception as error:
        message = f"{type(error).__name__}: {error}"[:4000]
        try:
            active.fail_replay(
                database_url,
                outcome_replay_manifest_id=manifest_id,
                run_fingerprint=run_spec.fingerprint,
                error_message=message,
            )
        except Exception as failure_error:
            raise Phase1AOutcomePipelineError(
                f"outcome replay failed and failure registration also failed: {message}"
            ) from failure_error
        if isinstance(error, Phase1AOutcomePipelineError):
            raise
        raise Phase1AOutcomePipelineError(message) from error
    return replace(
        report,
        cache_manifest_sha256=cache_sha,
        run_fingerprint=run_spec.fingerprint,
        outcome_replay_manifest_id=manifest_id,
        completed_source_date_count=completed,
        source_event_count=events,
        detail_record_count=records,
        summary_row_count=summaries,
        result_artifact_path=_report_path(result, label="result"),
        result_artifact_sha256=_report_sha(result, label="result"),
        final_checkpoint_path=_report_path(final_checkpoint, label="final checkpoint"),
        final_checkpoint_sha256=_report_sha(final_checkpoint, label="final checkpoint"),
        final_checkpoint_sequence=_checkpoint_sequence(final_checkpoint),
        disposition="SUCCEEDED",
    )


def run_phase1a_p5_outcomes(
    *,
    project_root: Path | str,
    data_root: Path | str,
    database_url: str,
    mode: Literal["PLAN_ONLY", "CACHE_ONLY", "RUN"] = "RUN",
    max_cache_workers: int | None = None,
    services: OutcomePipelineServices | None = None,
    progress_callback: Callable[[OutcomeProgress], None] | None = None,
) -> OutcomePipelineReport:
    """Plan, cache, execute, or exactly resume the governed p5 replay."""

    try:
        return _run_phase1a_outcomes(
            project_root=project_root,
            data_root=data_root,
            database_url=database_url,
            config_relative_path=OUTCOME_CONFIG_RELATIVE_PATH,
            mode=mode,
            max_cache_workers=max_cache_workers,
            services=services,
            progress_callback=progress_callback,
        )
    except Phase1AOutcomePipelineError:
        raise
    except Exception as error:
        raise Phase1AOutcomePipelineError(
            f"Phase 1A p5 outcome pipeline failed ({type(error).__name__})"
        ) from error


def run_phase1a_p1_05_outcomes(
    *,
    project_root: Path | str,
    data_root: Path | str,
    database_url: str,
    mode: Literal["PLAN_ONLY", "CACHE_ONLY", "RUN"] = "RUN",
    max_cache_workers: int | None = None,
    services: OutcomePipelineServices | None = None,
    progress_callback: Callable[[OutcomeProgress], None] | None = None,
) -> OutcomePipelineReport:
    """Plan, cache, execute, or exactly resume the governed p1_05 replay."""

    try:
        return _run_phase1a_outcomes(
            project_root=project_root,
            data_root=data_root,
            database_url=database_url,
            config_relative_path=P1_OUTCOME_CONFIG_RELATIVE_PATH,
            mode=mode,
            max_cache_workers=max_cache_workers,
            services=services,
            progress_callback=progress_callback,
        )
    except Phase1AOutcomePipelineError:
        raise
    except Exception as error:
        raise Phase1AOutcomePipelineError(
            f"Phase 1A p1_05 outcome pipeline failed ({type(error).__name__})"
        ) from error


__all__ = [
    "OutcomeArtifactServices",
    "OutcomePipelineReport",
    "OutcomePipelineServices",
    "OutcomeProgress",
    "Phase1AOutcomePipelineError",
    "PreparedOutcomeInputs",
    "merge_daily_shared_events",
    "run_phase1a_p1_05_outcomes",
    "run_phase1a_p5_outcomes",
]
