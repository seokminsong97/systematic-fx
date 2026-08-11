from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, replace
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Self

import pytest

from systematic_fx import cli
from systematic_fx.db.migrations import discover_migrations
from systematic_fx.db.research_registry import Phase1ACurrentSlicePrefixReport
from systematic_fx.db.run_registry import RunAttemptReservation
from systematic_fx.db.screening_feature_registry import (
    FEATURE_BATCH_MANIFEST_SCHEMA,
    BatchEntryStatus,
)
from systematic_fx.features.screening import (
    NO_POSITIVE_PREVIOUS_SOURCE_TRADE_VOLUME,
    NO_PROVEN_COMPLETE_OBSERVED_1S_BUCKET,
)
from systematic_fx.research.discovery_slice import (
    DISCOVERY_SLICE_SCHEMA,
    DISCOVERY_SLICE_VERSION,
    DISCOVERY_VARIABLE_FIELDS,
    FORWARD_HORIZONS,
    load_discovery_slice_config,
)
from systematic_fx.research.hypotheses import canonical_sha256
from systematic_fx.research.phase1a_pipeline import (
    _SUPPORTED_SCHEMA_MIGRATION_VERSIONS,
    DEFAULT_SERVICES,
    MISSING_PREVIOUS_REASON,
    UNQUALIFIED_PREVIOUS_REASON,
    Phase1APipelineError,
    ResolvedRunArtifact,
    _expected_integer_distribution,
    _json_artifact,
    _plan_entries,
    _postgres_runtime,
    _validate_query_result_evidence,
    run_phase1a_discovery_slice,
)

_SHA_A = "a" * 64
_SHA_B = "b" * 64
_SHA_C = "c" * 64
_COMMIT = "d" * 40
_PROJECT_ROOT = Path(__file__).resolve().parents[2]


class _PostgresResult:
    def __init__(
        self,
        *,
        row: dict[str, object] | None = None,
        rows: list[dict[str, object]] | None = None,
    ) -> None:
        self._row = row
        self._rows = rows

    def fetchone(self) -> dict[str, object] | None:
        return self._row

    def fetchall(self) -> list[dict[str, object]] | None:
        return self._rows


class _PostgresConnection:
    def __init__(self, migration_rows: list[dict[str, object]]) -> None:
        self.migration_rows = migration_rows
        self.queries: list[str] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        del args

    def execute(self, query: str) -> _PostgresResult:
        self.queries.append(query)
        if "server_version_num" in query:
            return _PostgresResult(row={"server_version": "18.4", "server_version_num": "180004"})
        return _PostgresResult(rows=self.migration_rows)


def _repository_migration_rows() -> list[dict[str, object]]:
    return [
        {
            "version": migration.version,
            "name": migration.name,
            "checksum": migration.checksum,
        }
        for migration in discover_migrations(_PROJECT_ROOT / "migrations")
    ]


def test_phase1a_pipeline_supports_exact_bar_state_v2a_migration() -> None:
    migrations = discover_migrations(_PROJECT_ROOT / "migrations")
    migration_by_version = {item.version: item for item in migrations}

    assert tuple(item.version for item in migrations) == (_SUPPORTED_SCHEMA_MIGRATION_VERSIONS)
    assert _SUPPORTED_SCHEMA_MIGRATION_VERSIONS == tuple(range(1, 27))
    assert migration_by_version[24].checksum == (
        "4aa845757f1a220c8d5595d4db6053f6374d99d067ab7e20c3e40ea22d610010"
    )
    assert migration_by_version[25].checksum == (
        "e08aa486bf9a65b2875e92866ae5e939fc56dc5d871010dfdb4b9085550749dd"
    )
    assert migrations[-1].name == "bar_state_v2a_optimizer_cap_amendment"
    assert migrations[-1].checksum == (
        "232badda3e76fca79f93fcff059de6f3404fc797eb26a93c9483fd554cfe20bb"
    )


@dataclass(frozen=True)
class _Document:
    sha256: str
    document: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return self.document


@dataclass(frozen=True)
class _Selection:
    sha256: str
    previous_volume: _Document
    document: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return self.document


class _DiscoveryOnlySplit:
    def __init__(self, discovery: tuple[date, ...]) -> None:
        self.discovery = discovery
        self.sha256 = _SHA_C

    def __getattr__(self, name: str) -> object:
        if name in {"walk_forward_folds", "embargo", "sealed_holdout", "outcome_tail"}:
            raise AssertionError(f"pipeline read forbidden split field: {name}")
        raise AttributeError(name)


@dataclass
class _Harness:
    services: Any
    state: dict[str, Any]
    data_root: Path


def _write_json_artifact(path: Path, document: dict[str, object]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
    path.write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()


def _resolved_json_artifact(path: Path, document: dict[str, object]) -> ResolvedRunArtifact:
    digest = _write_json_artifact(path, document)
    return ResolvedRunArtifact(
        artifact_id=1,
        path=path,
        sha256=digest,
        artifact_type="TEST_JSON",
    )


def _source_document(source: Any) -> dict[str, object]:
    return {
        "relative_uri": source.relative_uri,
        "sha256": source.sha256,
        "source_date": source.source_date.isoformat(),
    }


def _harness(tmp_path: Path, *, duplicate: bool = False) -> _Harness:
    data_root = tmp_path / "data"
    raw_root = data_root / "mbp-10"
    manifests = data_root / "derived/manifests"
    raw_root.mkdir(parents=True, exist_ok=True)
    manifests.mkdir(parents=True, exist_ok=True)
    for name in (
        "mbp10_footer_manifest_v1.jsonl",
        "mbp10_source_sha256_v1.jsonl",
        "mbp10_structural_qc_v1.jsonl",
    ):
        (manifests / name).write_text("{}\n", encoding="utf-8")

    first = date(2022, 1, 3)
    discovery = tuple(first + timedelta(days=index) for index in range(495))
    records = []
    for index, source_date in enumerate(discovery[:10]):
        relative_uri = f"source-{index}.parquet"
        (raw_root / relative_uri).write_bytes(f"raw-{index}".encode())
        records.append(
            SimpleNamespace(
                source_date=source_date,
                relative_uri=relative_uri,
                sha256=f"{index + 1:064x}",
            )
        )
    calendar = SimpleNamespace(
        source_dates=discovery,
        source_manifest_sha256=_SHA_A,
        qc_manifest_sha256=_SHA_B,
        qc_config_sha256="e" * 64,
        sha256="f" * 64,
    )
    split = _DiscoveryOnlySplit(discovery)
    state: dict[str, Any] = {
        "analyze_calls": 0,
        "build_calls": 0,
        "campaign_calls": 0,
        "complete_calls": [],
        "current_prefix_calls": [],
        "events": [],
        "finish_calls": [],
        "patterns": [],
        "plan_no_entry_calls": [],
        "predecessor_calls": [],
        "registrations": [],
        "specs": [],
        "verify_calls": [],
    }
    feature_artifact: dict[str, object] = {}
    discovery_artifact: dict[str, object] = {}

    calendar_path = manifests / "calendar.json"
    split_path = manifests / "split.json"
    calendar_path.write_text("calendar", encoding="utf-8")
    split_path.write_text("split", encoding="utf-8")

    def select_contract(*args: object, **kwargs: object) -> _Selection:
        del args
        previous_day = kwargs["previous_source_date"]
        eligible_day = kwargs["eligible_source_date"]
        assert kwargs["previous_source_sha256"]
        assert kwargs["eligible_source_sha256"]
        volume = _Document(
            sha256=hashlib.sha256(f"volume-{eligible_day}".encode()).hexdigest(),
            document={
                "eligible_source_date": str(eligible_day),
                "previous_source_date": str(previous_day),
                "trade_rows": 100,
                "trade_volume": 1_000,
            },
        )
        return _Selection(
            sha256=hashlib.sha256(f"selection-{eligible_day}".encode()).hexdigest(),
            previous_volume=volume,
            document={
                "eligible_source_date": str(eligible_day),
                "previous_source_date": str(previous_day),
                "selected": {"instrument_id": 1, "raw_symbol": "6EH2"},
            },
        )

    def build_features(*args: object, **kwargs: object) -> object:
        del args, kwargs
        state["build_calls"] += 1
        return SimpleNamespace()

    def plan_no_entry_reason(*args: object, **kwargs: object) -> None:
        state["plan_no_entry_calls"].append((args, kwargs))

    def register_feature_batch(
        database_url: str,
        *,
        data_root: Path,
        calendar: object,
        run_spec: object,
        entries: tuple[object, ...],
    ) -> object:
        del database_url, calendar
        state["feature_batch_entries"] = entries
        manifest_entries: list[dict[str, object]] = []
        for entry in entries:
            item: dict[str, object] = {
                "current_source": _source_document(entry.source),
                "status": entry.status.value,
            }
            if entry.status is BatchEntryStatus.RECORDED_NO_ENTRY:
                item["no_entry_reason"] = entry.no_entry_reason
                item["selection_audit"] = (
                    {
                        "contract_selection_sha256": entry.selection.sha256,
                        "previous_volume_sha256": entry.selection.previous_volume.sha256,
                        "selection_document": entry.selection.as_dict(),
                    }
                    if entry.selection is not None
                    else None
                )
            else:
                feature_path = (
                    data_root
                    / "derived/research_5m"
                    / f"source_date={entry.source.source_date.isoformat()}"
                    / "part-000.parquet"
                )
                feature_path.parent.mkdir(parents=True, exist_ok=True)
                feature_path.write_bytes(b"synthetic-feature")
                item["artifacts"] = [
                    {
                        "granularity": "5m",
                        "original_relative_uri": feature_path.relative_to(data_root).as_posix(),
                        "sha256": hashlib.sha256(feature_path.read_bytes()).hexdigest(),
                    }
                ]
            manifest_entries.append(item)
        document = {
            "artifact_schema": FEATURE_BATCH_MANIFEST_SCHEMA,
            "batch": {"entries": manifest_entries},
            "run_spec": {"run_fingerprint": run_spec.fingerprint},
        }
        state["feature_manifest_document"] = document
        path = data_root / "derived/manifests/synthetic-feature-batch.json"
        digest = _write_json_artifact(path, document)
        feature_artifact.update(path=path, sha256=digest, artifact_id=101)
        return SimpleNamespace(
            manifest_artifact_id=101,
            manifest_path=path,
            manifest_sha256=digest,
        )

    discovery_config = load_discovery_slice_config(
        _PROJECT_ROOT / "configs/research/phase1a_discovery_slice_v1.toml"
    )

    def analyze_slice(
        feature_paths_by_date: dict[date, Path],
        **kwargs: object,
    ) -> object:
        state["analyze_calls"] += 1
        requested = kwargs["requested_source_dates"]
        fingerprint = kwargs["run_fingerprint"]
        query_results: list[dict[str, object]] = []
        for index, query in enumerate(discovery_config.candidate_queries):
            occurrences: list[dict[str, object]] = []
            if index == 0:
                outcome = {
                    "aligned_close_x2_ticks": 0,
                    "maximum_adverse_excursion_x2_ticks": 0,
                    "maximum_favorable_excursion_x2_ticks": 0,
                }
                occurrences.append(
                    {
                        "bucket_end_ns": 1,
                        "direction": "LONG",
                        "forward": {str(horizon): dict(outcome) for horizon in FORWARD_HORIZONS},
                        "source_date": requested[1].isoformat(),
                        "variables": {field: 0 for field in DISCOVERY_VARIABLE_FIELDS},
                    }
                )
            aligned = [0] if occurrences else []
            distribution = _expected_integer_distribution(aligned)
            query_results.append(
                {
                    "definition": query.as_dict(),
                    "direction_counts": {"LONG": len(occurrences), "SHORT": 0},
                    "forward": {
                        str(horizon): {
                            "aligned_close_x2_ticks": distribution,
                            "maximum_adverse_excursion_x2_ticks": distribution,
                            "maximum_favorable_excursion_x2_ticks": distribution,
                            "negative_count": 0,
                            "positive_count": 0,
                            "positive_rate_ppm": 0 if occurrences else None,
                            "resolved_count": len(occurrences),
                            "unresolved_count": 0,
                            "zero_count": len(occurrences),
                        }
                        for horizon in FORWARD_HORIZONS
                    },
                    "occurrences": occurrences,
                    "source_date_count": int(bool(occurrences)),
                    "support_count": len(occurrences),
                }
            )
        document = {
            "artifact_schema": DISCOVERY_SLICE_SCHEMA,
            "artifact_version": DISCOVERY_SLICE_VERSION,
            "config": {
                "definition_sha256": discovery_config.definition_sha256,
                "relative_path": "configs/research/phase1a_discovery_slice_v1.toml",
                "sha256": discovery_config.sha256,
            },
            "feature_inputs": [
                {
                    "path": path.relative_to(data_root).as_posix(),
                    "sha256": kwargs["expected_sha256_by_date"][source_date],
                    "source_date": source_date.isoformat(),
                }
                for source_date, path in sorted(feature_paths_by_date.items())
            ],
            "no_entry_reasons": {
                source_date.isoformat(): reason
                for source_date, reason in sorted(kwargs["no_entry_reasons"].items())
            },
            "query_results": query_results,
            "requested_source_dates": [day.isoformat() for day in requested],
            "run_fingerprint": fingerprint,
            "summary": {
                "candidate_query_count": 11,
                "eligible_rows": 20,
                "feature_rows": 40,
                "nonzero_support_query_count": 1,
                "zero_support_query_count": 10,
            },
        }
        path = data_root / "derived/manifests/synthetic-discovery.json"
        digest = _write_json_artifact(path, document)
        discovery_artifact.update(path=path, sha256=digest, artifact_id=202)
        return SimpleNamespace(path=path, sha256=digest)

    def register_spec(
        database_url: str,
        run_spec: object,
        *,
        parent_run_fingerprint: str | None,
    ) -> object:
        del database_url
        state["events"].append("spec")
        state["specs"].append(run_spec)
        state["registrations"].append((run_spec, parent_run_fingerprint))
        return SimpleNamespace(research_run_spec_id=len(state["specs"]))

    def reserve_attempt(database_url: str, *, run_fingerprint: str) -> RunAttemptReservation:
        del database_url, run_fingerprint
        index = len(state.setdefault("reservations", [])) + 1
        state["reservations"].append(index)
        if duplicate:
            return RunAttemptReservation(
                research_run_attempt_id=100 + index,
                research_run_spec_id=index,
                attempt_number=2,
                status="SKIPPED_DUPLICATE",
                execute=False,
                reused_attempt_id=index,
            )
        return RunAttemptReservation(
            research_run_attempt_id=index,
            research_run_spec_id=index,
            attempt_number=1,
            status="QUEUED",
            execute=True,
            reused_attempt_id=None,
        )

    def finish_attempt(database_url: str, **kwargs: object) -> object:
        del database_url
        state["finish_calls"].append(kwargs)
        return SimpleNamespace(status=kwargs["status"])

    def complete_success(database_url: str, **kwargs: object) -> object:
        del database_url
        state["complete_calls"].append(kwargs)
        return SimpleNamespace(result_artifact_id=202)

    def resolve_artifact(
        database_url: str,
        *,
        reused_attempt_id: int,
        data_root: Path,
    ) -> ResolvedRunArtifact:
        del database_url, data_root
        source = feature_artifact if reused_attempt_id == 1 else discovery_artifact
        artifact_type = (
            "PHASE1A_FEATURE_BUILD_MANIFEST"
            if reused_attempt_id == 1
            else "DISCOVERY_EXPOSURE_RESULT"
        )
        return ResolvedRunArtifact(
            artifact_id=int(source["artifact_id"]),
            path=Path(source["path"]),
            sha256=str(source["sha256"]),
            artifact_type=artifact_type,
        )

    snapshot_path = manifests / "snapshot.json"
    snapshot_path.write_text("snapshot", encoding="utf-8")

    def register_campaign(*args: object, **kwargs: object) -> object:
        del args, kwargs
        state["campaign_calls"] += 1
        state["events"].append("campaign")
        return SimpleNamespace(
            campaign_id=7,
            campaign_key="phase1a_conservative_screening_v1",
        )

    def verify_current_prefix(*args: object, **kwargs: object) -> object:
        del args
        state["current_prefix_calls"].append(kwargs)
        return Phase1ACurrentSlicePrefixReport(
            slice_index=int(kwargs["slice_index"]),
            state="EMPTY",
            feature_run_spec_id=None,
            ai_exposure_id=None,
            query_exposure_ids=(),
            pattern_ids=(),
            result_artifact_id=None,
            missing_pattern_query_id=None,
        )

    services = replace(
        DEFAULT_SERVICES,
        build_calendar=lambda *args, **kwargs: calendar,
        build_split=lambda value: split,
        publish_calendar_split=lambda *args, **kwargs: SimpleNamespace(
            calendar_path=calendar_path,
            split_path=split_path,
        ),
        git_head=lambda root: _COMMIT,
        build_snapshot=lambda *args, **kwargs: SimpleNamespace(sha256=_SHA_A),
        publish_snapshot=lambda *args, **kwargs: SimpleNamespace(
            path=snapshot_path,
            sha256=_SHA_A,
            disposition="CREATED",
        ),
        dependency_hash=lambda root: _SHA_B,
        runtime=lambda: {"runtime": "synthetic"},
        postgres_runtime=lambda database_url, **kwargs: {"server_version": "18.4"},
        load_source_bundle=lambda *args, **kwargs: SimpleNamespace(
            footer_manifest_path=manifests / "mbp10_footer_manifest_v1.jsonl",
            hash_manifest_path=manifests / "mbp10_source_sha256_v1.jsonl",
            footer_manifest_sha256="9" * 64,
            hash_manifest_sha256=_SHA_A,
            records=tuple(records),
        ),
        register_campaign=register_campaign,
        select_contract=select_contract,
        plan_no_entry_reason=plan_no_entry_reason,
        register_spec=register_spec,
        reserve_attempt=reserve_attempt,
        start_attempt=lambda *args, **kwargs: state.setdefault("starts", []).append(kwargs),
        finish_attempt=finish_attempt,
        build_features=build_features,
        register_feature_batch=register_feature_batch,
        analyze_slice=analyze_slice,
        complete_discovery_success=complete_success,
        verify_discovery_success=lambda *args, **kwargs: state["verify_calls"].append(kwargs),
        verify_current_slice_prefix=verify_current_prefix,
        verify_predecessor_slice=lambda *args, **kwargs: state["predecessor_calls"].append(kwargs),
        record_pattern=lambda database_url, observation: state["patterns"].append(observation),
        resolve_artifact=resolve_artifact,
    )
    state["feature_artifact"] = feature_artifact
    state["discovery_artifact"] = discovery_artifact
    return _Harness(services=services, state=state, data_root=data_root)


def test_postgres_runtime_records_exact_supported_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration_rows = _repository_migration_rows()
    connection = _PostgresConnection(migration_rows)
    monkeypatch.setattr(
        "systematic_fx.research.phase1a_pipeline.psycopg.connect",
        lambda *args, **kwargs: connection,
    )

    runtime = _postgres_runtime(
        "postgresql://synthetic",
        migrations_directory=_PROJECT_ROOT / "migrations",
    )

    assert runtime == {
        "server_version": "18.4",
        "server_version_num": "180004",
        "schema_migrations": migration_rows,
        "schema_migrations_sha256": canonical_sha256(migration_rows),
    }
    assert "ORDER BY version" in connection.queries[1]


@pytest.mark.parametrize("drift", ("missing_latest", "checksum"))
def test_postgres_runtime_rejects_missing_or_drifted_schema(
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    migration_rows = _repository_migration_rows()
    if drift == "missing_latest":
        migration_rows.pop()
    else:
        migration_rows[4] = {**migration_rows[4], "checksum": "0" * 64}
    connection = _PostgresConnection(migration_rows)
    monkeypatch.setattr(
        "systematic_fx.research.phase1a_pipeline.psycopg.connect",
        lambda *args, **kwargs: connection,
    )

    with pytest.raises(Phase1APipelineError, match="do not exactly match"):
        _postgres_runtime(
            "postgresql://synthetic",
            migrations_directory=_PROJECT_ROOT / "migrations",
        )


def test_json_artifact_hashes_and_parses_the_same_held_inode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data"
    path = data_root / "derived/manifests/result.json"
    document = {"artifact_schema": "test.schema.v1", "value": 7}
    artifact = _resolved_json_artifact(path, document)

    def forbid_path_reread(_path: Path) -> bytes:
        raise AssertionError("_json_artifact reopened the validated path")

    monkeypatch.setattr(Path, "read_bytes", forbid_path_reread)

    resolved, observed = _json_artifact(
        artifact,
        data_root=data_root,
        expected_schema="test.schema.v1",
    )

    assert resolved == path.resolve()
    assert observed == document


def test_json_artifact_rejects_path_inode_swap_during_held_fd_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data"
    path = data_root / "derived/manifests/result.json"
    document = {"artifact_schema": "test.schema.v1", "value": 7}
    artifact = _resolved_json_artifact(path, document)
    replacement = path.with_name("replacement.json")
    _write_json_artifact(replacement, document)
    real_read = os.read
    swapped = False

    def swapping_read(descriptor: int, byte_count: int) -> bytes:
        nonlocal swapped
        chunk = real_read(descriptor, byte_count)
        if chunk and not swapped:
            swapped = True
            os.replace(replacement, path)
        return chunk

    monkeypatch.setattr("systematic_fx.research.phase1a_pipeline.os.read", swapping_read)

    with pytest.raises(Phase1APipelineError, match="changed while it was read"):
        _json_artifact(
            artifact,
            data_root=data_root,
            expected_schema="test.schema.v1",
        )

    assert swapped


def test_phase1a_slice_records_full_lineage_and_all_eleven_queries(tmp_path: Path) -> None:
    harness = _harness(tmp_path)

    report = run_phase1a_discovery_slice(
        project_root=_PROJECT_ROOT,
        data_root=harness.data_root,
        database_url="postgresql://synthetic",
        services=harness.services,
    )

    assert report.source_dates == tuple(
        (date(2022, 1, 3) + timedelta(days=index)).isoformat() for index in range(5)
    )
    assert report.pattern_observation_count == 11
    assert len(report.query_runs) == 11
    assert harness.state["build_calls"] == 4
    assert harness.state["analyze_calls"] == 1
    assert harness.state["predecessor_calls"] == []
    assert len(harness.state["current_prefix_calls"]) == 2
    assert harness.state["events"][0] == "campaign"
    assert [spec.run_kind for spec in harness.state["specs"]] == [
        "FEATURE_BUILD",
        "AI_SLICE",
        *(["QUERY"] * 11),
    ]

    feature_parameters = json.loads(harness.state["specs"][0].canonical_json())["parameters"]
    assert feature_parameters["batch_entries"][0]["no_entry_reason"] == (MISSING_PREVIOUS_REASON)
    for entry in feature_parameters["batch_entries"][1:]:
        assert entry["selection_document"]
        assert entry["selection_sha256"]
        assert entry["previous_volume_document"]
        assert entry["previous_volume_sha256"]
    assert len(feature_parameters["raw_sources"]) == 5
    assert len(feature_parameters["frozen_toml_inputs"]) == 8
    assert dict(harness.state["specs"][0].source_manifest_hashes) == {
        "mbp10_footer_manifest_v1": "9" * 64,
        "mbp10_source_sha256_v1": _SHA_A,
        "mbp10_structural_qc_v1": _SHA_B,
    }
    assert all(
        dict(spec.source_manifest_hashes) == dict(harness.state["specs"][0].source_manifest_hashes)
        for spec in harness.state["specs"]
    )

    for spec, parent in harness.state["registrations"][1:]:
        parameters = json.loads(spec.canonical_json())["parameters"]
        assert parameters["parent_run_fingerprint"] == parent
    query_parameters = json.loads(harness.state["specs"][2].canonical_json())["parameters"]
    assert query_parameters["candidate_query"]
    assert query_parameters["query_definition_sha256"]
    assert query_parameters["discovery_artifact_sha256"]
    assert query_parameters["query_result_sha256"]

    supports = [observation.support_count for observation in harness.state["patterns"]]
    assert supports == [1, *([0] * 10)]
    assert all(observation.economic_rationale for observation in harness.state["patterns"])
    assert all(
        observation.feature_identity["footer_manifest_sha256"] == "9" * 64
        for observation in harness.state["patterns"]
    )
    assert [call["exposure_key"] for call in harness.state["complete_calls"]] == [
        "phase1a_conservative_screening_v1:ai-slice:00",
        *[
            f"phase1a_conservative_screening_v1:query:00:{observation.query_id}"
            for observation in harness.state["patterns"]
        ],
    ]
    artifact = json.loads(Path(report.discovery_artifact_path).read_bytes())
    assert set(artifact["query_results"][0]["occurrences"][0]["variables"]) == set(
        DISCOVERY_VARIABLE_FIELDS
    )


def test_child_fingerprints_bind_parent_and_parent_artifact(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    run_phase1a_discovery_slice(
        project_root=_PROJECT_ROOT,
        data_root=harness.data_root,
        database_url="postgresql://synthetic",
        services=harness.services,
    )
    ai_spec = harness.state["specs"][1]
    ai_parameters = json.loads(ai_spec.canonical_json())["parameters"]
    changed_ai_parent = replace(
        ai_spec,
        parameters={**ai_parameters, "parent_run_fingerprint": "0" * 64},
    )
    changed_feature_artifact = replace(
        ai_spec,
        parameters={**ai_parameters, "feature_manifest_sha256": "1" * 64},
    )
    assert changed_ai_parent.fingerprint != ai_spec.fingerprint
    assert changed_feature_artifact.fingerprint != ai_spec.fingerprint

    query_spec = harness.state["specs"][2]
    query_parameters = json.loads(query_spec.canonical_json())["parameters"]
    changed_query_parent = replace(
        query_spec,
        parameters={**query_parameters, "parent_run_fingerprint": "2" * 64},
    )
    changed_discovery_artifact = replace(
        query_spec,
        parameters={**query_parameters, "discovery_artifact_sha256": "3" * 64},
    )
    assert changed_query_parent.fingerprint != query_spec.fingerprint
    assert changed_discovery_artifact.fingerprint != query_spec.fingerprint


@pytest.mark.parametrize("drift", ("missing_variable", "missing_horizon", "arithmetic"))
def test_query_result_schema_rejects_incomplete_variable_or_forward_evidence(
    tmp_path: Path,
    drift: str,
) -> None:
    harness = _harness(tmp_path)
    report = run_phase1a_discovery_slice(
        project_root=_PROJECT_ROOT,
        data_root=harness.data_root,
        database_url="postgresql://synthetic",
        services=harness.services,
    )
    document = json.loads(Path(report.discovery_artifact_path).read_bytes())
    result = document["query_results"][0]
    if drift == "missing_variable":
        result["occurrences"][0]["variables"].pop(DISCOVERY_VARIABLE_FIELDS[0])
    elif drift == "missing_horizon":
        result["occurrences"][0]["forward"].pop("1")
    else:
        result["forward"]["12"]["resolved_count"] = 99

    with pytest.raises(Phase1APipelineError, match="schema drift|arithmetic drift"):
        _validate_query_result_evidence(
            result,
            requested_source_dates=tuple(date.fromisoformat(day) for day in report.source_dates),
        )


def test_duplicate_slice_reuses_artifacts_without_reexecution(tmp_path: Path) -> None:
    fresh = _harness(tmp_path)
    run_phase1a_discovery_slice(
        project_root=_PROJECT_ROOT,
        data_root=fresh.data_root,
        database_url="postgresql://synthetic",
        services=fresh.services,
    )
    duplicate = _harness(tmp_path, duplicate=True)
    duplicate.state["feature_artifact"].update(fresh.state["feature_artifact"])
    duplicate.state["discovery_artifact"].update(fresh.state["discovery_artifact"])

    report = run_phase1a_discovery_slice(
        project_root=_PROJECT_ROOT,
        data_root=duplicate.data_root,
        database_url="postgresql://synthetic",
        services=duplicate.services,
    )

    assert report.feature_run.executed is False
    assert report.ai_slice_run.executed is False
    assert all(run.executed is False for run in report.query_runs)
    assert duplicate.state["build_calls"] == 0
    assert duplicate.state["analyze_calls"] == 0
    assert duplicate.state["complete_calls"] == []
    assert len(duplicate.state["verify_calls"]) == 12
    assert len(duplicate.state["patterns"]) == 11


def test_empty_current_slice_prefix_can_run(tmp_path: Path) -> None:
    harness = _harness(tmp_path)

    def accept_prefix(*args: object, **kwargs: object) -> object:
        del args
        if kwargs["expected_feature_run_fingerprint"] is None:
            assert harness.state["specs"] == []
            assert harness.state.get("reservations", []) == []
        harness.state["current_prefix_calls"].append(kwargs)
        return Phase1ACurrentSlicePrefixReport(
            slice_index=0,
            state="EMPTY",
            feature_run_spec_id=None,
            ai_exposure_id=None,
            query_exposure_ids=(),
            pattern_ids=(),
            result_artifact_id=None,
            missing_pattern_query_id=None,
        )

    services = replace(harness.services, verify_current_slice_prefix=accept_prefix)
    report = run_phase1a_discovery_slice(
        project_root=_PROJECT_ROOT,
        data_root=harness.data_root,
        database_url="postgresql://synthetic",
        services=services,
    )

    assert report.pattern_observation_count == 11
    assert len(harness.state["current_prefix_calls"]) == 2
    early, exact = harness.state["current_prefix_calls"]
    assert early["slice_index"] == 0
    assert early["expected_feature_run_fingerprint"] is None
    assert exact["expected_feature_run_fingerprint"] == harness.state["specs"][0].fingerprint
    assert len(early["query_definition_sha256_by_id"]) == 11


def test_failed_feature_prefix_retries_as_fresh_governed_execution(tmp_path: Path) -> None:
    harness = _harness(tmp_path)

    def retryable_prefix(*args: object, **kwargs: object) -> object:
        del args
        harness.state["current_prefix_calls"].append(kwargs)
        return Phase1ACurrentSlicePrefixReport(
            slice_index=0,
            state="FAILED_FEATURE_RETRYABLE",
            feature_run_spec_id=41,
            ai_exposure_id=None,
            query_exposure_ids=(),
            pattern_ids=(),
            result_artifact_id=None,
            missing_pattern_query_id=None,
        )

    report = run_phase1a_discovery_slice(
        project_root=_PROJECT_ROOT,
        data_root=harness.data_root,
        database_url="postgresql://synthetic",
        services=replace(harness.services, verify_current_slice_prefix=retryable_prefix),
    )

    assert report.feature_run.executed is True
    assert harness.state["build_calls"] == 4
    assert len(harness.state["current_prefix_calls"]) == 2


@pytest.mark.parametrize(
    "reason",
    (
        NO_PROVEN_COMPLETE_OBSERVED_1S_BUCKET,
        NO_POSITIVE_PREVIOUS_SOURCE_TRADE_VOLUME,
    ),
)
def test_planned_selected_no_entry_preserves_selection_and_previous_volume(
    tmp_path: Path,
    reason: str,
) -> None:
    harness = _harness(tmp_path)
    final_day = date(2022, 1, 7)

    def classify(*args: object, **kwargs: object) -> str | None:
        harness.state["plan_no_entry_calls"].append((args, kwargs))
        if kwargs["source_date"] == final_day:
            return reason
        return None

    report = run_phase1a_discovery_slice(
        project_root=_PROJECT_ROOT,
        data_root=harness.data_root,
        database_url="postgresql://synthetic",
        services=replace(harness.services, plan_no_entry_reason=classify),
    )

    assert harness.state["build_calls"] == 3
    assert report.built_source_dates == ("2022-01-04", "2022-01-05", "2022-01-06")
    assert report.no_entry_reasons[-1] == (
        final_day.isoformat(),
        reason,
    )
    feature_parameters = json.loads(harness.state["specs"][0].canonical_json())["parameters"]
    final_parameters = feature_parameters["batch_entries"][-1]
    assert final_parameters["status"] == BatchEntryStatus.RECORDED_NO_ENTRY.value
    assert final_parameters["no_entry_reason"] == reason
    assert final_parameters["selection_document"]
    assert final_parameters["selection_sha256"]
    assert final_parameters["previous_volume_document"]
    assert final_parameters["previous_volume_sha256"]
    assert (
        feature_parameters["selection_sha256_by_date"][final_day.isoformat()]
        == (final_parameters["selection_sha256"])
    )
    assert (
        feature_parameters["previous_volume_sha256_by_date"][final_day.isoformat()]
        == (final_parameters["previous_volume_sha256"])
    )
    final_entry = harness.state["feature_batch_entries"][-1]
    assert final_entry.selection is not None
    selection_audit = harness.state["feature_manifest_document"]["batch"]["entries"][-1][
        "selection_audit"
    ]
    assert selection_audit["contract_selection_sha256"] == final_entry.selection.sha256
    assert selection_audit["previous_volume_sha256"] == (
        final_entry.selection.previous_volume.sha256
    )
    assert selection_audit["selection_document"] == final_entry.selection.as_dict()


def test_conflicting_current_slice_is_rejected_before_any_run_attempt(tmp_path: Path) -> None:
    harness = _harness(tmp_path)

    def reject_prefix(*args: object, **kwargs: object) -> object:
        del args, kwargs
        assert harness.state["specs"] == []
        assert harness.state.get("reservations", []) == []
        raise RuntimeError("CURRENT_SLICE_IDENTITY_CONFLICT")

    services = replace(harness.services, verify_current_slice_prefix=reject_prefix)
    with pytest.raises(Phase1APipelineError, match="RuntimeError"):
        run_phase1a_discovery_slice(
            project_root=_PROJECT_ROOT,
            data_root=harness.data_root,
            database_url="postgresql://synthetic",
            services=services,
        )

    assert harness.state["campaign_calls"] == 1
    assert harness.state["specs"] == []
    assert harness.state.get("reservations", []) == []
    assert harness.state["build_calls"] == 0
    assert harness.state["analyze_calls"] == 0


@pytest.mark.parametrize(
    "reason",
    ("PREDECESSOR_SLICE_MISSING", "PREDECESSOR_SLICE_PARTIAL"),
)
def test_out_of_order_or_partial_slice_is_rejected_before_any_run_attempt(
    tmp_path: Path,
    reason: str,
) -> None:
    harness = _harness(tmp_path)

    def reject_predecessor(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RuntimeError(reason)

    services = replace(harness.services, verify_predecessor_slice=reject_predecessor)
    with pytest.raises(Phase1APipelineError, match="RuntimeError"):
        run_phase1a_discovery_slice(
            project_root=_PROJECT_ROOT,
            data_root=harness.data_root,
            database_url="postgresql://synthetic",
            slice_index=1,
            services=services,
        )

    assert harness.state["campaign_calls"] == 1
    assert harness.state["specs"] == []
    assert harness.state.get("reservations", []) == []
    assert harness.state["build_calls"] == 0
    assert harness.state["analyze_calls"] == 0


def test_later_slice_with_exact_predecessor_can_run(tmp_path: Path) -> None:
    harness = _harness(tmp_path)

    report = run_phase1a_discovery_slice(
        project_root=_PROJECT_ROOT,
        data_root=harness.data_root,
        database_url="postgresql://synthetic",
        slice_index=1,
        services=harness.services,
    )

    assert report.slice_index == 1
    assert harness.state["build_calls"] == 5
    assert len(harness.state["predecessor_calls"]) == 1
    preflight = harness.state["predecessor_calls"][0]
    assert preflight["prior_slice_index"] == 0
    assert preflight["requested_source_dates"] == tuple(
        date(2022, 1, 3) + timedelta(days=index) for index in range(5)
    )
    assert len(preflight["query_definition_sha256_by_id"]) == 11
    assert len(harness.state["specs"]) == 13


def test_feature_failure_terminalizes_started_attempt(tmp_path: Path) -> None:
    harness = _harness(tmp_path)

    def fail_build(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RuntimeError("sensitive builder detail")

    services = replace(harness.services, build_features=fail_build)
    with pytest.raises(Phase1APipelineError, match="FEATURE_BUILD") as raised:
        run_phase1a_discovery_slice(
            project_root=_PROJECT_ROOT,
            data_root=harness.data_root,
            database_url="postgresql://synthetic",
            services=services,
        )

    assert "sensitive builder detail" not in str(raised.value)
    assert harness.state["finish_calls"][-1]["status"] == "FAILED"
    assert [spec.run_kind for spec in harness.state["specs"]] == ["FEATURE_BUILD"]


def test_atomic_discovery_failure_rolls_to_failed_without_pattern_exposure(
    tmp_path: Path,
) -> None:
    harness = _harness(tmp_path)

    def fail_atomic(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RuntimeError("transaction rolled back")

    services = replace(harness.services, complete_discovery_success=fail_atomic)
    with pytest.raises(Phase1APipelineError, match="AI_SLICE"):
        run_phase1a_discovery_slice(
            project_root=_PROJECT_ROOT,
            data_root=harness.data_root,
            database_url="postgresql://synthetic",
            services=services,
        )

    assert harness.state["finish_calls"][-1]["status"] == "FAILED"
    assert harness.state["patterns"] == []
    assert [spec.run_kind for spec in harness.state["specs"]] == [
        "FEATURE_BUILD",
        "AI_SLICE",
    ]


def test_planner_does_not_skip_back_over_unqualified_immediate_predecessor(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    (data_root / "mbp-10").mkdir(parents=True)
    first = date(2022, 1, 3)
    records = tuple(
        SimpleNamespace(
            source_date=first + timedelta(days=index),
            relative_uri=f"source-{index}.parquet",
            sha256=f"{index + 1:064x}",
        )
        for index in range(3)
    )
    calls: list[object] = []
    plans = _plan_entries(
        data_root=data_root,
        requested_dates=(records[0].source_date, records[2].source_date),
        records=records,
        qualified_dates=frozenset({records[0].source_date, records[2].source_date}),
        select_contract=lambda *args, **kwargs: calls.append((args, kwargs)),
        plan_no_entry_reason=lambda *args, **kwargs: None,
        calendar=SimpleNamespace(),
        config_path=_PROJECT_ROOT / "configs/features/phase1a_mbp10_screening_v1.toml",
    )

    assert plans[0].no_entry_reason == MISSING_PREVIOUS_REASON
    assert plans[0].previous_source is None
    assert plans[1].no_entry_reason == UNQUALIFIED_PREVIOUS_REASON
    assert plans[1].previous_source is not None
    assert plans[1].previous_source.source_date == records[1].source_date
    assert calls == []


def test_phase1a_cli_defaults_and_json(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    parser = cli.build_parser()
    args = parser.parse_args(["research", "phase1a-slice", "--json"])
    assert args.slice_index == 0

    captured: dict[str, object] = {}

    def fake_runner(**kwargs: object) -> object:
        captured.update(kwargs)
        return SimpleNamespace(as_dict=lambda: {"slice_index": kwargs["slice_index"]})

    monkeypatch.setattr(
        "systematic_fx.research.phase1a_pipeline.run_phase1a_discovery_slice",
        fake_runner,
    )
    monkeypatch.setattr(
        cli.Settings,
        "from_env",
        lambda: SimpleNamespace(data_root=Path("data"), database_url="postgresql://test"),
    )
    assert args.handler(args) == 0
    assert captured["slice_index"] == 0
    assert json.loads(capsys.readouterr().out) == {"slice_index": 0}


def test_phase1a_cli_rejects_negative_slice_index() -> None:
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["research", "phase1a-slice", "--slice-index", "-1"])
