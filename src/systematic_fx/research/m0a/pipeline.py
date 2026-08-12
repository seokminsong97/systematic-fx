"""CLI-friendly deterministic orchestration for one durable M0a epoch."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from systematic_fx.research.m0a.config import EpochConfig, load_epoch
from systematic_fx.research.m0a.daemon import (
    CrashHook,
    DaemonRunReport,
    SystemErrorClassifier,
    start_daemon,
)
from systematic_fx.research.m0a.evaluate import AdmissionRules, evaluate_candidate
from systematic_fx.research.m0a.family import StrategyCandidate, generate_candidates
from systematic_fx.research.m0a.features import build_features as _build_features
from systematic_fx.research.m0a.fixture import build_fixture
from systematic_fx.research.m0a.labels import build_labels as _build_labels
from systematic_fx.research.m0a.ledger import (
    DurableEpochEvaluation,
    EpochReport,
    InvariantReport,
    M0aLedger,
    M0aLedgerError,
)
from systematic_fx.research.m0a.model import EventFeature, MarketFixture, QuoteAwareLabel

_WRITE_BITS = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH


@dataclass(frozen=True, slots=True)
class DatasetArtifact:
    artifact_type: str
    artifact_sha256: str
    byte_size: int
    row_count: int
    path: Path
    created: bool


@dataclass(frozen=True, slots=True)
class BuiltM0aInputs:
    epoch: EpochConfig
    fixture: MarketFixture
    features: tuple[EventFeature, ...]
    labels: tuple[QuoteAwareLabel, ...]


@dataclass(frozen=True, slots=True)
class PersistedM0aInputs:
    epoch: EpochConfig
    fixture_artifact: DatasetArtifact
    feature_artifact: DatasetArtifact
    label_artifact: DatasetArtifact


@dataclass(frozen=True, slots=True)
class PersistedFeatureInputs:
    epoch: EpochConfig
    fixture_artifact: DatasetArtifact
    feature_artifact: DatasetArtifact


@dataclass(frozen=True, slots=True)
class EpochPipelineReport:
    epoch_id: str
    epoch_hash: str
    candidate_seed: int
    evaluation_seed: int
    real_candidate_sha256s: tuple[str, ...]
    null_candidate_sha256s: tuple[str, ...]
    daemon: DaemonRunReport
    ledger: EpochReport
    invariants: InvariantReport


def _publish_jsonl(
    *,
    root: str | Path,
    epoch_id: str,
    artifact_type: str,
    header: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> DatasetArtifact:
    documents = ({"record_type": "MANIFEST", **dict(header)}, *rows)
    content = b"".join(
        json.dumps(
            document,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
        for document in documents
    )
    sha256 = hashlib.sha256(content).hexdigest()
    parent = Path(root).expanduser().resolve() / epoch_id / artifact_type.lower()
    parent.mkdir(parents=True, exist_ok=True)
    path = parent / f"sha256={sha256}.jsonl"
    if path.exists():
        observed = path.read_bytes()
        if observed != content or path.is_symlink():
            raise M0aLedgerError(f"existing {artifact_type} artifact content drift")
        if path.stat().st_mode & _WRITE_BITS:
            path.chmod(path.stat().st_mode & ~_WRITE_BITS)
        return DatasetArtifact(artifact_type, sha256, len(content), len(rows), path, False)
    descriptor, temporary_name = tempfile.mkstemp(prefix=".m0a-input-", dir=parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o444)
        try:
            os.link(temporary, path)
            created = True
        except FileExistsError:
            created = False
        if path.read_bytes() != content or path.is_symlink():
            raise M0aLedgerError(f"published {artifact_type} artifact bytes drift")
        parent_fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
        return DatasetArtifact(artifact_type, sha256, len(content), len(rows), path, created)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _load_jsonl(artifact: DatasetArtifact) -> tuple[Mapping[str, Any], tuple[dict[str, Any], ...]]:
    if artifact.path.is_symlink() or not artifact.path.is_file():
        raise M0aLedgerError(f"{artifact.artifact_type} artifact is not a regular file")
    content = artifact.path.read_bytes()
    if (
        hashlib.sha256(content).hexdigest() != artifact.artifact_sha256
        or len(content) != artifact.byte_size
    ):
        raise M0aLedgerError(f"{artifact.artifact_type} artifact byte identity drift")
    try:
        documents = tuple(json.loads(line) for line in content.splitlines())
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise M0aLedgerError(f"{artifact.artifact_type} artifact is invalid JSONL") from error
    if not documents or documents[0].get("record_type") != "MANIFEST":
        raise M0aLedgerError(f"{artifact.artifact_type} artifact lacks its manifest")
    rows = tuple(documents[1:])
    if len(rows) != artifact.row_count:
        raise M0aLedgerError(f"{artifact.artifact_type} artifact row-count drift")
    return documents[0], rows


def build_features(
    epoch: EpochConfig,
    fixture: MarketFixture,
    *,
    artifact_root: str | Path | None = None,
) -> tuple[EventFeature, ...] | DatasetArtifact:
    """Build causal features, optionally as immutable content-addressed JSONL."""

    features = _build_features(epoch, fixture)
    if artifact_root is None:
        return features
    return _publish_jsonl(
        root=artifact_root,
        epoch_id=epoch.epoch_id,
        artifact_type="FEATURES",
        header={
            "artifact_schema": "systematic_fx.m0a_features.v1",
            "dataset_hash": epoch.dataset_hash,
            "epoch_hash": epoch.epoch_hash,
            "feature_version": epoch.feature_version,
            "row_count": len(features),
        },
        rows=tuple(feature.as_dict() for feature in features),
    )


def build_labels(
    epoch: EpochConfig,
    fixture: MarketFixture,
    features: Sequence[EventFeature],
    *,
    artifact_root: str | Path | None = None,
) -> tuple[QuoteAwareLabel, ...] | DatasetArtifact:
    """Build quote-aware labels, optionally as immutable content-addressed JSONL."""

    labels = _build_labels(epoch, fixture, features)
    if artifact_root is None:
        return labels
    return _publish_jsonl(
        root=artifact_root,
        epoch_id=epoch.epoch_id,
        artifact_type="LABELS",
        header={
            "artifact_schema": "systematic_fx.m0a_labels.v1",
            "dataset_hash": epoch.dataset_hash,
            "epoch_hash": epoch.epoch_hash,
            "label_version": epoch.label_version,
            "row_count": len(labels),
        },
        rows=tuple(label.as_dict() for label in labels),
    )


def build_epoch_inputs(epoch_or_path: EpochConfig | str | Path) -> BuiltM0aInputs:
    epoch = load_epoch(epoch_or_path) if isinstance(epoch_or_path, (str, Path)) else epoch_or_path
    epoch.verify_unchanged()
    fixture = build_fixture(epoch)
    features = _build_features(epoch, fixture)
    labels = _build_labels(epoch, fixture, features)
    epoch.verify_unchanged()
    return BuiltM0aInputs(epoch, fixture, features, labels)


def persist_epoch_inputs(
    epoch_or_path: EpochConfig | str | Path,
    *,
    artifact_root: str | Path,
) -> PersistedM0aInputs:
    """Publish fixture, feature, and label inputs with exact reopenable hashes."""

    inputs = build_epoch_inputs(epoch_or_path)
    fixture = _publish_jsonl(
        root=artifact_root,
        epoch_id=inputs.epoch.epoch_id,
        artifact_type="FIXTURE",
        header={
            "artifact_schema": "systematic_fx.m0a_fixture.v1",
            "dataset_hash": inputs.epoch.dataset_hash,
            "epoch_hash": inputs.epoch.epoch_hash,
            "fixture_version": inputs.epoch.fixture_version,
            "row_count": 1,
        },
        rows=(inputs.fixture.as_dict(),),
    )
    features = _publish_jsonl(
        root=artifact_root,
        epoch_id=inputs.epoch.epoch_id,
        artifact_type="FEATURES",
        header={
            "artifact_schema": "systematic_fx.m0a_features.v1",
            "dataset_hash": inputs.epoch.dataset_hash,
            "epoch_hash": inputs.epoch.epoch_hash,
            "feature_version": inputs.epoch.feature_version,
            "row_count": len(inputs.features),
        },
        rows=tuple(feature.as_dict() for feature in inputs.features),
    )
    labels = _publish_jsonl(
        root=artifact_root,
        epoch_id=inputs.epoch.epoch_id,
        artifact_type="LABELS",
        header={
            "artifact_schema": "systematic_fx.m0a_labels.v1",
            "dataset_hash": inputs.epoch.dataset_hash,
            "epoch_hash": inputs.epoch.epoch_hash,
            "label_version": inputs.epoch.label_version,
            "row_count": len(inputs.labels),
        },
        rows=tuple(label.as_dict() for label in inputs.labels),
    )
    assert isinstance(features, DatasetArtifact)
    assert isinstance(labels, DatasetArtifact)
    return PersistedM0aInputs(inputs.epoch, fixture, features, labels)


def load_persisted_epoch_inputs(persisted: PersistedM0aInputs) -> BuiltM0aInputs:
    """Reopen and reconstruct exact future inputs without rebuilding them."""

    epoch = persisted.epoch
    epoch.verify_unchanged()
    fixture_header, fixture_rows = _load_jsonl(persisted.fixture_artifact)
    feature_header, feature_rows = _load_jsonl(persisted.feature_artifact)
    label_header, label_rows = _load_jsonl(persisted.label_artifact)
    for header, expected in (
        (
            fixture_header,
            {
                "artifact_schema": "systematic_fx.m0a_fixture.v1",
                "fixture_version": epoch.fixture_version,
                "row_count": 1,
            },
        ),
        (
            feature_header,
            {
                "artifact_schema": "systematic_fx.m0a_features.v1",
                "feature_version": epoch.feature_version,
                "row_count": persisted.feature_artifact.row_count,
            },
        ),
        (
            label_header,
            {
                "artifact_schema": "systematic_fx.m0a_labels.v1",
                "label_version": epoch.label_version,
                "row_count": persisted.label_artifact.row_count,
            },
        ),
    ):
        if (
            any(header.get(key) != value for key, value in expected.items())
            or header.get("epoch_hash") != epoch.epoch_hash
            or header.get("dataset_hash") != epoch.dataset_hash
        ):
            raise M0aLedgerError("persisted M0a input manifest identity drift")
    if len(fixture_rows) != 1:
        raise M0aLedgerError("persisted fixture must contain exactly one row")
    fixture = MarketFixture.from_dict(fixture_rows[0])
    features = tuple(EventFeature.from_dict(row) for row in feature_rows)
    labels = tuple(QuoteAwareLabel.from_dict(row) for row in label_rows)
    if fixture.content_sha256 != epoch.dataset_hash:
        raise M0aLedgerError("persisted fixture content differs from epoch dataset hash")
    return BuiltM0aInputs(epoch, fixture, features, labels)


def _load_persisted_feature_inputs(
    persisted: PersistedFeatureInputs,
) -> tuple[EpochConfig, MarketFixture, tuple[EventFeature, ...]]:
    epoch = persisted.epoch
    epoch.verify_unchanged()
    fixture_header, fixture_rows = _load_jsonl(persisted.fixture_artifact)
    feature_header, feature_rows = _load_jsonl(persisted.feature_artifact)
    expected_headers = (
        (
            fixture_header,
            {
                "artifact_schema": "systematic_fx.m0a_fixture.v1",
                "fixture_version": epoch.fixture_version,
                "row_count": 1,
            },
        ),
        (
            feature_header,
            {
                "artifact_schema": "systematic_fx.m0a_features.v1",
                "feature_version": epoch.feature_version,
                "row_count": persisted.feature_artifact.row_count,
            },
        ),
    )
    for header, expected in expected_headers:
        if (
            any(header.get(key) != value for key, value in expected.items())
            or header.get("epoch_hash") != epoch.epoch_hash
            or header.get("dataset_hash") != epoch.dataset_hash
        ):
            raise M0aLedgerError("persisted M0a feature input identity drift")
    if len(fixture_rows) != 1:
        raise M0aLedgerError("persisted fixture must contain exactly one row")
    fixture = MarketFixture.from_dict(fixture_rows[0])
    features = tuple(EventFeature.from_dict(row) for row in feature_rows)
    if fixture.content_sha256 != epoch.dataset_hash:
        raise M0aLedgerError("persisted fixture content differs from epoch dataset hash")
    return epoch, fixture, features


def _discover_artifact(
    *,
    artifact_root: str | Path,
    epoch_id: str,
    artifact_type: str,
) -> DatasetArtifact | None:
    directory = Path(artifact_root).expanduser().resolve() / epoch_id / artifact_type.lower()
    if not directory.exists():
        return None
    paths = tuple(sorted(directory.glob("sha256=*.jsonl")))
    if len(paths) != 1:
        raise M0aLedgerError(
            f"{artifact_type} namespace must contain exactly one content-addressed artifact"
        )
    path = paths[0]
    content = path.read_bytes()
    sha256 = path.stem.removeprefix("sha256=")
    if hashlib.sha256(content).hexdigest() != sha256:
        raise M0aLedgerError(f"{artifact_type} artifact filename/content hash drift")
    try:
        header = json.loads(content.splitlines()[0])
    except (IndexError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise M0aLedgerError(f"{artifact_type} artifact manifest cannot be decoded") from error
    row_count = header.get("row_count")
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
        raise M0aLedgerError(f"{artifact_type} artifact row count is invalid")
    return DatasetArtifact(artifact_type, sha256, len(content), row_count, path, False)


def discover_persisted_epoch_inputs(
    epoch_or_path: EpochConfig | str | Path,
    *,
    artifact_root: str | Path,
) -> PersistedM0aInputs | None:
    """Find one exact complete input bundle; reject a partial persisted bundle."""

    epoch = load_epoch(epoch_or_path) if isinstance(epoch_or_path, (str, Path)) else epoch_or_path
    discovered = tuple(
        _discover_artifact(
            artifact_root=artifact_root,
            epoch_id=epoch.epoch_id,
            artifact_type=artifact_type,
        )
        for artifact_type in ("FIXTURE", "FEATURES", "LABELS")
    )
    present = sum(item is not None for item in discovered)
    if present == 0:
        return None
    if discovered[0] is not None and discovered[1] is not None and discovered[2] is None:
        return None
    if present != len(discovered):
        raise M0aLedgerError("persisted M0a input bundle is partial")
    fixture, features, labels = discovered
    assert fixture is not None and features is not None and labels is not None
    persisted = PersistedM0aInputs(epoch, fixture, features, labels)
    load_persisted_epoch_inputs(persisted)
    return persisted


def _discover_feature_inputs(
    epoch: EpochConfig,
    *,
    artifact_root: str | Path,
) -> PersistedFeatureInputs | None:
    fixture = _discover_artifact(
        artifact_root=artifact_root,
        epoch_id=epoch.epoch_id,
        artifact_type="FIXTURE",
    )
    features = _discover_artifact(
        artifact_root=artifact_root,
        epoch_id=epoch.epoch_id,
        artifact_type="FEATURES",
    )
    if fixture is None and features is None:
        return None
    if fixture is None or features is None:
        raise M0aLedgerError("persisted M0a fixture/features bundle is partial")
    persisted = PersistedFeatureInputs(epoch, fixture, features)
    _load_persisted_feature_inputs(persisted)
    return persisted


def build_feature_artifact(
    epoch_path: str | Path,
    *,
    artifact_root: str | Path,
) -> PersistedFeatureInputs:
    """Publish the fixture and features required by the build-features command."""

    epoch = load_epoch(epoch_path)
    fixture = build_fixture(epoch)
    features = _build_features(epoch, fixture)
    fixture_artifact = _publish_jsonl(
        root=artifact_root,
        epoch_id=epoch.epoch_id,
        artifact_type="FIXTURE",
        header={
            "artifact_schema": "systematic_fx.m0a_fixture.v1",
            "dataset_hash": epoch.dataset_hash,
            "epoch_hash": epoch.epoch_hash,
            "fixture_version": epoch.fixture_version,
            "row_count": 1,
        },
        rows=(fixture.as_dict(),),
    )
    feature_artifact = _publish_jsonl(
        root=artifact_root,
        epoch_id=epoch.epoch_id,
        artifact_type="FEATURES",
        header={
            "artifact_schema": "systematic_fx.m0a_features.v1",
            "dataset_hash": epoch.dataset_hash,
            "epoch_hash": epoch.epoch_hash,
            "feature_version": epoch.feature_version,
            "row_count": len(features),
        },
        rows=tuple(feature.as_dict() for feature in features),
    )
    return PersistedFeatureInputs(epoch, fixture_artifact, feature_artifact)


def build_label_artifact(
    epoch_path: str | Path,
    *,
    artifact_root: str | Path,
) -> DatasetArtifact:
    """Reopen exact fixture/features and publish labels for build-labels."""

    epoch = load_epoch(epoch_path)
    persisted = _discover_feature_inputs(epoch, artifact_root=artifact_root)
    if persisted is None:
        raise M0aLedgerError("build-labels requires build-features artifacts first")
    epoch, fixture, features = _load_persisted_feature_inputs(persisted)
    artifact = build_labels(
        epoch,
        fixture,
        features,
        artifact_root=artifact_root,
    )
    assert isinstance(artifact, DatasetArtifact)
    return artifact


def ensure_persisted_epoch_inputs(
    epoch: EpochConfig,
    *,
    artifact_root: str | Path,
) -> PersistedM0aInputs:
    """Resume the documented build-features/build-labels sequence exactly."""

    complete = discover_persisted_epoch_inputs(epoch, artifact_root=artifact_root)
    if complete is not None:
        return complete
    partial = _discover_feature_inputs(epoch, artifact_root=artifact_root)
    if partial is None:
        partial = build_feature_artifact(epoch.manifest_path, artifact_root=artifact_root)
    label = build_label_artifact(epoch.manifest_path, artifact_root=artifact_root)
    complete = PersistedM0aInputs(
        epoch,
        partial.fixture_artifact,
        partial.feature_artifact,
        label,
    )
    load_persisted_epoch_inputs(complete)
    return complete


def _null_apis() -> tuple[Callable[..., tuple[object, ...]], Callable[..., object]]:
    """Load sibling null-control APIs only after their module is importable."""

    from systematic_fx.research.m0a.controls import generate_null_candidates
    from systematic_fx.research.m0a.evaluate import evaluate_null_candidate

    return generate_null_candidates, evaluate_null_candidate


def generate_epoch_candidates(
    epoch: EpochConfig,
    *,
    candidate_seed: int | None = None,
    null_seed: int | None = None,
) -> tuple[tuple[StrategyCandidate, ...], tuple[object, ...]]:
    """Generate the exact immutable REAL and explicit NULL budgets."""

    epoch.verify_unchanged()
    candidate_seed = epoch.random_seeds[0] if candidate_seed is None else candidate_seed
    null_seed = epoch.random_seeds[1] if null_seed is None else null_seed
    real = generate_candidates(
        budget=epoch.real_candidate_budget,
        seed=candidate_seed,
        barriers=epoch.barrier_specs,
        family_id=epoch.family_id,
        search_space=epoch.family_search_space,
    )
    generate_null_candidates, _ = _null_apis()
    nulls = tuple(generate_null_candidates(real, seed=null_seed))
    if len(real) != epoch.real_candidate_budget:
        raise M0aLedgerError("real candidate generator did not exhaust the immutable budget")
    if len(nulls) != epoch.null_candidate_budget:
        raise M0aLedgerError("explicit null candidate count differs from the immutable null budget")
    hashes = tuple(item.candidate_hash for item in real) + tuple(
        str(item.candidate_hash)
        for item in nulls  # type: ignore[attr-defined]
    )
    if len(hashes) != len(set(hashes)):
        raise M0aLedgerError("REAL and NULL epoch candidates contain duplicate hashes")
    return real, nulls


def prepare_epoch_ledger(
    ledger: M0aLedger,
    epoch: EpochConfig,
    *,
    candidate_seed: int | None = None,
    null_seed: int | None = None,
    system_error_threshold: int | None = None,
) -> tuple[tuple[StrategyCandidate, ...], tuple[object, ...]]:
    """Exactly register the finite candidate budget, idempotently on restart."""

    manifest_candidate_seed, manifest_null_seed = epoch.random_seeds[:2]
    if candidate_seed is not None and candidate_seed != manifest_candidate_seed:
        raise M0aLedgerError("candidate_seed cannot override the immutable epoch manifest")
    if null_seed is not None and null_seed != manifest_null_seed:
        raise M0aLedgerError("null_seed cannot override the immutable epoch manifest")
    ledger.ensure_epoch(epoch, system_error_threshold=system_error_threshold)
    real, nulls = generate_epoch_candidates(
        epoch,
        candidate_seed=manifest_candidate_seed,
        null_seed=manifest_null_seed,
    )
    for candidate in real:
        registration = ledger.register_candidate(epoch.epoch_id, candidate, candidate_kind="REAL")
        if registration.budget_exhausted:
            raise M0aLedgerError("real candidate budget exhausted before deterministic plan ended")
    for candidate in nulls:
        registration = ledger.register_candidate(epoch.epoch_id, candidate, candidate_kind="NULL")
        if registration.budget_exhausted:
            raise M0aLedgerError("null candidate budget exhausted before deterministic plan ended")
    ledger.mark_generation_complete(epoch.epoch_id)
    return real, nulls


def _candidate_map(candidates: Sequence[object]) -> dict[str, object]:
    return {
        str(candidate.candidate_hash): candidate  # type: ignore[attr-defined]
        for candidate in candidates
    }


def run_epoch_pipeline(
    epoch_path: str | Path,
    *,
    ledger_path: str | Path,
    artifact_root: str | Path | None = None,
    worker_id: str = "m0a-worker-1",
    candidate_seed: int | None = None,
    null_seed: int | None = None,
    evaluation_seed: int | None = None,
    lease_seconds: int | None = None,
    system_error_threshold: int | None = None,
    max_cycles: int | None = None,
    max_completed_attempts: int | None = None,
    crash_hook: CrashHook | None = None,
    system_error_classifier: SystemErrorClassifier | None = None,
    evaluation_options: Mapping[str, Any] | None = None,
) -> EpochPipelineReport:
    """Build deterministic inputs, resume the ledger, and run a bounded daemon."""

    epoch = load_epoch(epoch_path)
    manifest_candidate_seed, manifest_null_seed, manifest_evaluation_seed = epoch.random_seeds[:3]
    for supplied, expected, label in (
        (candidate_seed, manifest_candidate_seed, "candidate_seed"),
        (null_seed, manifest_null_seed, "null_seed"),
        (evaluation_seed, manifest_evaluation_seed, "evaluation_seed"),
        (lease_seconds, epoch.daemon_lease_seconds, "lease_seconds"),
        (
            system_error_threshold,
            epoch.daemon_system_error_threshold,
            "system_error_threshold",
        ),
        (max_cycles, epoch.daemon_run_epoch_max_cycles, "max_cycles"),
    ):
        if supplied is not None and supplied != expected:
            raise M0aLedgerError(f"{label} cannot override the immutable epoch manifest")
    if evaluation_options is not None:
        expected_options = dict(epoch.evaluation_options)
        expected_options["purge_seconds"] = None
        expected_options.pop("purge_policy")
        if dict(evaluation_options) != expected_options:
            raise M0aLedgerError("evaluation_options cannot override the immutable epoch manifest")
    if epoch.evaluation_purge_policy != "MAX_HOLD_PLUS_FEATURE_LOOKBACK":
        raise M0aLedgerError("unsupported immutable evaluation purge policy")
    input_artifact_root = (
        Path(artifact_root).expanduser().resolve()
        if artifact_root is not None
        else Path(ledger_path).expanduser().resolve().parent / f"{Path(ledger_path).stem}-artifacts"
    )
    persisted = ensure_persisted_epoch_inputs(epoch, artifact_root=input_artifact_root)
    inputs = load_persisted_epoch_inputs(persisted)
    candidate_seed = manifest_candidate_seed
    null_seed = manifest_null_seed
    evaluation_seed = manifest_evaluation_seed
    lease_seconds = epoch.daemon_lease_seconds
    system_error_threshold = epoch.daemon_system_error_threshold
    max_cycles = epoch.daemon_run_epoch_max_cycles
    ledger = M0aLedger(
        ledger_path,
        artifact_root=artifact_root,
        default_lease_seconds=lease_seconds,
        default_system_error_threshold=system_error_threshold,
    )
    real, nulls = prepare_epoch_ledger(
        ledger,
        epoch,
        candidate_seed=candidate_seed,
        null_seed=null_seed,
        system_error_threshold=system_error_threshold,
    )
    options: dict[str, Any] = dict(epoch.evaluation_options)
    options.pop("purge_policy")
    options["purge_seconds"] = None
    options["admission_rules"] = AdmissionRules.from_config(epoch)
    _, evaluate_null_candidate = _null_apis()
    parent_by_hash = {candidate.candidate_hash: candidate for candidate in real}

    def evaluate(candidate: object) -> object:
        if isinstance(candidate, StrategyCandidate):
            return evaluate_candidate(
                candidate,
                inputs.features,
                inputs.labels,
                seed=evaluation_seed,
                **options,
            )
        parent = parent_by_hash[candidate.parent_candidate_hash]  # type: ignore[attr-defined]
        null_options = {
            key: value
            for key, value in options.items()
            if key
            in {
                "cooldown_seconds",
                "stressed_cost_denominator",
                "stressed_cost_numerator",
            }
        }
        return evaluate_null_candidate(
            candidate,
            parent,
            inputs.features,
            inputs.labels,
            **null_options,
        )

    candidates = _candidate_map((*real, *nulls))
    all_steps = []
    remaining_cycles = max_cycles
    completed_total = 0
    worker_generation = 1
    while remaining_cycles > 0:
        remaining_requested = (
            None if max_completed_attempts is None else max_completed_attempts - completed_total
        )
        if remaining_requested is not None and remaining_requested <= 0:
            break
        restart_cap = epoch.daemon_worker_restart_after_experiments
        chunk_cap = (
            restart_cap if remaining_requested is None else min(restart_cap, remaining_requested)
        )
        chunk = start_daemon(
            ledger,
            epoch_id=epoch.epoch_id,
            worker_id=f"{worker_id}-generation-{worker_generation}",
            evaluator=evaluate,
            candidates=candidates,
            lease_seconds=lease_seconds,
            crash_hook=crash_hook,
            system_error_classifier=system_error_classifier,
            max_cycles=remaining_cycles,
            max_completed_attempts=chunk_cap,
            poll_interval_seconds=epoch.daemon_poll_interval_milliseconds / 1_000,
            stop_when_idle=epoch.daemon_run_epoch_stop_when_idle,
        )
        all_steps.extend(chunk.steps)
        remaining_cycles -= len(chunk.steps)
        completed_total += chunk.completed_count
        if (
            not chunk.steps
            or chunk.epoch.status in {"COMPLETED", "HALTED"}
            or chunk.steps[-1].disposition in {"IDLE", "HALTED"}
        ):
            break
        worker_generation += 1
    daemon = DaemonRunReport(
        epoch_id=epoch.epoch_id,
        worker_id=worker_id,
        steps=tuple(all_steps),
        epoch=ledger.report(epoch.epoch_id),
    )
    epoch.verify_unchanged()
    invariants = ledger.verify_invariants(epoch.epoch_id)
    return EpochPipelineReport(
        epoch_id=epoch.epoch_id,
        epoch_hash=epoch.epoch_hash,
        candidate_seed=candidate_seed,
        evaluation_seed=evaluation_seed,
        real_candidate_sha256s=tuple(candidate.candidate_hash for candidate in real),
        null_candidate_sha256s=tuple(
            str(candidate.candidate_hash)  # type: ignore[attr-defined]
            for candidate in nulls
        ),
        daemon=daemon,
        ledger=ledger.report(epoch.epoch_id),
        invariants=invariants,
    )


run_epoch = run_epoch_pipeline


def report_epoch(
    ledger_path: str | Path,
    epoch_id: str,
    *,
    artifact_root: str | Path | None = None,
) -> EpochReport:
    return M0aLedger(ledger_path, artifact_root=artifact_root).report(epoch_id)


def load_epoch_evaluation(
    ledger_path: str | Path,
    epoch_id: str,
    *,
    artifact_root: str | Path | None = None,
) -> DurableEpochEvaluation:
    """Verify and reconstruct durable report data without rerunning economics."""

    return M0aLedger(ledger_path, artifact_root=artifact_root).load_epoch_evaluation(epoch_id)


def render_report_from_ledger(
    ledger_path: str | Path,
    epoch_id: str,
    *,
    artifact_root: str | Path | None = None,
    epoch_path: str | Path | None = None,
) -> str:
    """Render Markdown exclusively from verified SQLite/file evidence."""

    from systematic_fx.research.m0a.report import (
        EpochReportMetadata,
        render_durable_markdown_report,
    )

    durable = load_epoch_evaluation(
        ledger_path,
        epoch_id,
        artifact_root=artifact_root,
    )
    epoch = durable.epoch_record
    state_root = (
        Path(artifact_root).expanduser().resolve()
        if artifact_root is not None
        else Path(ledger_path).expanduser().resolve().parent / f"{Path(ledger_path).stem}-artifacts"
    )
    if epoch_path is not None:
        epoch_config = load_epoch(epoch_path)
        if epoch_config.epoch_id != epoch_id or epoch_config.epoch_hash != epoch["epoch_hash"]:
            raise M0aLedgerError("report epoch manifest differs from durable ledger identity")
    label_artifact = _discover_artifact(
        artifact_root=state_root,
        epoch_id=epoch_id,
        artifact_type="LABELS",
    )
    if label_artifact is None:
        raise M0aLedgerError("durable report requires the exact persisted label artifact")
    label_header, label_rows = _load_jsonl(label_artifact)
    if (
        label_header.get("artifact_schema") != "systematic_fx.m0a_labels.v1"
        or label_header.get("epoch_hash") != epoch["epoch_hash"]
        or label_header.get("dataset_hash") != epoch["dataset_hash"]
        or label_header.get("label_version") != epoch["label_version"]
    ):
        raise M0aLedgerError("durable report label manifest identity drift")
    labels = tuple(QuoteAwareLabel.from_dict(row) for row in label_rows)
    diagnostic_counts = {
        "ambiguous_label_count": sum(label.ambiguous for label in labels),
        "roll_exclusion_count": sum(label.invalid_reason == "ROLL_GUARD" for label in labels),
        "session_exclusion_count": sum(
            label.invalid_reason == "WOULD_CROSS_SESSION_CLOSE" for label in labels
        ),
    }
    metadata = EpochReportMetadata.from_mapping(
        {
            **epoch,
            **diagnostic_counts,
            "candidate_registered_at": {
                str(record["candidate_sha256"]): str(record["registered_at"])
                for record in durable.candidate_records
                if record["registered_at"] is not None
            },
            "retry_count": durable.retry_count,
        }
    )
    return render_durable_markdown_report(
        durable.epoch_record,
        durable.candidate_records,
        metadata,
    )


def verify_epoch_invariants(
    ledger_path: str | Path,
    epoch_id: str,
    *,
    artifact_root: str | Path | None = None,
) -> InvariantReport:
    return M0aLedger(ledger_path, artifact_root=artifact_root).verify_invariants(epoch_id)


report = report_epoch
verify = verify_epoch_invariants
