from __future__ import annotations

import json
from collections.abc import Iterable
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Self
from unittest.mock import patch

import pytest
from psycopg import IsolationLevel

from systematic_fx.db.research_registry import (
    Phase1ACurrentSlicePrefixReport,
    Phase1APredecessorSliceReport,
    ResearchRegistryDriftError,
    ResearchRegistryError,
    _phase1a_predecessor_inputs,
    _validate_phase1a_ai_predecessor,
    _validate_phase1a_pattern_predecessor,
    _validate_phase1a_query_predecessor,
    verify_phase1a_current_slice_prefix,
    verify_phase1a_predecessor_slice,
)
from systematic_fx.research.hypotheses import canonical_sha256
from systematic_fx.research.run_spec import RunSpec

CAMPAIGN = "phase1a_conservative_screening_v1"
SOURCE_DATES = tuple(date(2022, 1, day) for day in (3, 4, 5, 6, 7))
START = datetime(2022, 1, 3, tzinfo=UTC)
END = datetime(2022, 1, 8, tzinfo=UTC)
ARTIFACT_SHA256 = "b" * 64
FEATURE_FINGERPRINT = "c" * 64
FEATURE_ARTIFACT_SHA256 = "d" * 64
AI_ENGINE_VERSION = "phase1a_fixed_query_discovery_v1"
QUERY_ENGINE_VERSION = "phase1a_fixed_query_projection_v1"
DISCOVERY_ARTIFACT_RELATIVE_PATH = "derived/research_5m/discovery.json"
_ARTIFACT_URI: str | None = None


@pytest.fixture(autouse=True)
def _bypass_filesystem_artifact_check(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """SQL identity mocks use a separate test for content-addressed file reachability."""

    global _ARTIFACT_URI
    artifact = tmp_path / "data" / DISCOVERY_ARTIFACT_RELATIVE_PATH
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"fixture discovery artifact")
    _ARTIFACT_URI = artifact.resolve().as_uri()
    monkeypatch.setattr(
        "systematic_fx.db.research_registry._verify_phase1a_artifact_file",
        lambda row: None,
    )
    monkeypatch.setattr(
        "systematic_fx.db.research_registry._phase1a_load_discovery_query_evidence",
        lambda *args, **kwargs: _artifact_query_evidence(),
    )


class _DbResult:
    def __init__(self, value: object) -> None:
        self.value = value

    def fetchone(self) -> dict[str, Any] | None:
        if self.value is None or isinstance(self.value, dict):
            return self.value
        raise AssertionError("result is not a single row")

    def fetchall(self) -> list[dict[str, Any]]:
        if isinstance(self.value, list):
            return self.value
        raise AssertionError("result is not a row list")


class _Transaction:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _DbConnection:
    def __init__(self, responses: Iterable[object]) -> None:
        self.responses = list(responses)
        self.calls: list[str] = []
        self.isolation_level: IsolationLevel | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def transaction(self) -> _Transaction:
        return _Transaction()

    def execute(self, sql: str, parameters: object = ()) -> _DbResult:
        del parameters
        self.calls.append(" ".join(sql.split()))
        if not self.responses:
            raise AssertionError(f"unexpected SQL: {sql}")
        return _DbResult(self.responses.pop(0))


def _definitions() -> tuple[list[dict[str, object]], dict[str, str]]:
    definitions = [{"id": f"q{index:02d}", "rule": f"f{index} >= {index}"} for index in range(11)]
    return definitions, {
        str(definition["id"]): canonical_sha256(definition) for definition in definitions
    }


def _query_result_sha256(index: int, definition: dict[str, object]) -> str:
    return canonical_sha256({"definition": definition, "fixture_index": index})


def _artifact_query_evidence() -> dict[str, dict[str, object]]:
    definitions, hashes = _definitions()
    return {
        str(definition["id"]): {
            "definition_sha256": hashes[str(definition["id"])],
            "query_result_sha256": _query_result_sha256(index, definition),
            "result_summary": {
                "artifact_sha256": ARTIFACT_SHA256,
                "direction_counts": {"LONG": index, "SHORT": 0},
                "source_date_count": min(index, len(SOURCE_DATES)),
                "support_count": index,
            },
        }
        for index, definition in enumerate(definitions)
    }


def _frozen_inputs() -> dict[str, object]:
    return {
        "campaign": {"sha256": "1" * 64},
        "discovery_query": {"sha256": "2" * 64},
    }


def _ai_run_spec(definitions: list[dict[str, object]]) -> RunSpec:
    return RunSpec(
        campaign_id=CAMPAIGN,
        experiment_id=None,
        run_kind="AI_SLICE",
        engine_version=AI_ENGINE_VERSION,
        source_manifest_hashes={
            "mbp10_footer_manifest_v1": "3" * 64,
            "mbp10_source_sha256_v1": "4" * 64,
            "mbp10_structural_qc_v1": "5" * 64,
        },
        eligible_calendar_version="calendar-v1",
        eligible_calendar_sha256="6" * 64,
        split_version="split-v1",
        split_sha256="7" * 64,
        feature_version="feature-v1",
        feature_sha256="8" * 64,
        outcome_version="outcome-v1",
        outcome_sha256="9" * 64,
        cost_version="cost-v1",
        cost_sha256="a" * 64,
        execution_version="execution-v1",
        execution_sha256="e" * 64,
        code_commit="1" * 40,
        code_snapshot_sha256="f" * 64,
        dependency_lock_sha256="0" * 64,
        runtime_environment={"python": "3.12.13"},
        random_seed=0,
        direction="BOTH",
        signal_policy={"signal_cadence_seconds": 300},
        entry_policy={"entry": "NEXT_EVENT"},
        barrier_policy={"same_event": "LOSS_FIRST"},
        terminal_policy={"open_position": "UNRESOLVED"},
        parameters={
            "analysis_authority": "OPEN_OBSERVATION",
            "candidate_queries": definitions,
            "candidate_query_definition_sha256": canonical_sha256(definitions),
            "feature_inputs_by_date": {
                "2022-01-03": {
                    "relative_path": "derived/research_5m/part.parquet",
                    "sha256": "1" * 64,
                }
            },
            "feature_manifest_relative_path": "derived/manifests/feature.json",
            "feature_manifest_sha256": FEATURE_ARTIFACT_SHA256,
            "frozen_toml_inputs": _frozen_inputs(),
            "no_entry_reason_by_date": {},
            "parent_run_fingerprint": FEATURE_FINGERPRINT,
            "pipeline_version": "phase1a_discovery_pipeline_v1",
            "requested_source_dates": [day.isoformat() for day in SOURCE_DATES],
            "research_eligible": False,
            "screening_only": True,
            "slice_index": 0,
        },
    )


def _canonical_spec(run_spec: RunSpec) -> dict[str, object]:
    value = json.loads(run_spec.canonical_json())
    assert isinstance(value, dict)
    return value


def _ai_fingerprint() -> str:
    return _ai_run_spec(_definitions()[0]).fingerprint


def _artifact_uri() -> str:
    assert _ARTIFACT_URI is not None
    return _ARTIFACT_URI


def _ai_row() -> tuple[dict[str, object], list[dict[str, object]], dict[str, str]]:
    definitions, hashes = _definitions()
    definition_sha256 = canonical_sha256(definitions)
    run_spec = _ai_run_spec(definitions)
    run_fingerprint = run_spec.fingerprint
    exposure_key = f"{CAMPAIGN}:ai-slice:00"
    row: dict[str, object] = {
        "discovery_exposure_id": 10,
        "campaign_id": 1,
        "exposure_key": exposure_key,
        "exposure_type": "AI_SLICE",
        "source_interval_start": START,
        "source_interval_end": END,
        "visible_to_ai": True,
        "research_eligible": False,
        "query_spec": {
            "candidate_queries": definitions,
            "definition_sha256": definition_sha256,
            "run_fingerprint": run_fingerprint,
        },
        "result_summary": {
            "candidate_query_count": 11,
            "feature_manifest_sha256": FEATURE_ARTIFACT_SHA256,
            "requested_source_dates": [day.isoformat() for day in SOURCE_DATES],
            "screening_only": True,
        },
        "result_artifact_id": 20,
        "research_run_spec_id": 30,
        "run_fingerprint": run_fingerprint,
        "run_kind": "AI_SLICE",
        "engine_version": AI_ENGINE_VERSION,
        "parent_run_spec_id": 29,
        "canonical_spec": _canonical_spec(run_spec),
        "artifact_key": f"{CAMPAIGN}:discovery-exposure:{exposure_key}:{ARTIFACT_SHA256}",
        "artifact_type": "DISCOVERY_EXPOSURE_RESULT",
        "artifact_uri": _artifact_uri(),
        "artifact_sha256": ARTIFACT_SHA256,
        "artifact_byte_size": 100,
        "artifact_media_type": "application/json",
        "artifact_metadata": {
            "campaign_key": CAMPAIGN,
            "exposure_key": exposure_key,
            "exposure_type": "AI_SLICE",
            "run_fingerprint": run_fingerprint,
        },
    }
    return row, definitions, hashes


def _query_row(
    index: int,
    definition: dict[str, object],
    definition_sha256: str,
) -> dict[str, object]:
    definitions, _ = _definitions()
    ai_spec = _ai_run_spec(definitions)
    query_id = str(definition["id"])
    query_result_sha256 = _query_result_sha256(index, definition)
    query_spec = replace(
        ai_spec,
        run_kind="QUERY",
        engine_version=QUERY_ENGINE_VERSION,
        parameters={
            "candidate_query": definition,
            "discovery_artifact_relative_path": DISCOVERY_ARTIFACT_RELATIVE_PATH,
            "discovery_artifact_sha256": ARTIFACT_SHA256,
            "frozen_toml_inputs": _frozen_inputs(),
            "parent_run_fingerprint": ai_spec.fingerprint,
            "pipeline_version": "phase1a_discovery_pipeline_v1",
            "query_definition_sha256": definition_sha256,
            "query_result_sha256": query_result_sha256,
            "requested_source_dates": [day.isoformat() for day in SOURCE_DATES],
            "research_eligible": False,
            "screening_only": True,
            "slice_index": 0,
        },
    )
    fingerprint = query_spec.fingerprint
    exposure_key = f"{CAMPAIGN}:query:00:{query_id}"
    return {
        "discovery_exposure_id": 100 + index,
        "exposure_key": exposure_key,
        "exposure_type": "QUERY",
        "source_interval_start": START,
        "source_interval_end": END,
        "visible_to_ai": True,
        "research_eligible": False,
        "query_spec": {
            "candidate_query": definition,
            "query_definition_sha256": definition_sha256,
            "run_fingerprint": fingerprint,
        },
        "result_summary": {
            "artifact_sha256": ARTIFACT_SHA256,
            "direction_counts": {"LONG": index, "SHORT": 0},
            "source_date_count": min(index, 5),
            "support_count": index,
        },
        "result_artifact_id": 20,
        "research_run_spec_id": 200 + index,
        "run_fingerprint": fingerprint,
        "run_kind": "QUERY",
        "engine_version": QUERY_ENGINE_VERSION,
        "parent_run_spec_id": 30,
        "canonical_spec": _canonical_spec(query_spec),
        "artifact_type": "DISCOVERY_EXPOSURE_RESULT",
        "artifact_uri": _artifact_uri(),
        "artifact_sha256": ARTIFACT_SHA256,
    }


def _pattern_row(
    index: int,
    definition: dict[str, object],
    definition_sha256: str,
    query_row: dict[str, object],
) -> dict[str, object]:
    query_id = str(definition["id"])
    return {
        "pattern_id": 300 + index,
        "pattern_key": f"{CAMPAIGN}:{query_id}",
        "feature_definition_versions": {
            "rollup_schema": "systematic_fx.phase1a_pattern_rollup.v1",
            "slice_identities": [
                {
                    "discovery_exposure_id": query_row["discovery_exposure_id"],
                    "feature_identity": {"manifest_sha256": "c" * 64},
                    "query_definition": definition,
                    "query_definition_sha256": definition_sha256,
                    "run_fingerprint": query_row["run_fingerprint"],
                }
            ],
        },
        "forward_first_touch_summaries": {
            "rollup_schema": "systematic_fx.phase1a_pattern_rollup.v1",
            "slice_observations": [
                {
                    "counterexamples": [],
                    "discovery_exposure_id": query_row["discovery_exposure_id"],
                    "exposure_key": query_row["exposure_key"],
                    "forward_first_touch_summary": {"12": {"resolved": index}},
                    "query_definition_sha256": definition_sha256,
                    "research_run_spec_id": query_row["research_run_spec_id"],
                    "result_artifact_id": query_row["result_artifact_id"],
                    "run_fingerprint": query_row["run_fingerprint"],
                    "source_interval_end": END.isoformat(),
                    "source_interval_start": START.isoformat(),
                    "support_count": index,
                }
            ],
        },
    }


def _public_fixture() -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    dict[str, str],
]:
    ai_row, definitions, hashes = _ai_row()
    query_rows = [
        _query_row(index, definition, hashes[str(definition["id"])])
        for index, definition in enumerate(definitions)
    ]
    success_rows = [
        {"research_run_spec_id": 30, "result_artifact_id": 20},
        *(
            {
                "research_run_spec_id": query_row["research_run_spec_id"],
                "result_artifact_id": 20,
            }
            for query_row in query_rows
        ),
    ]
    pattern_rows = [
        _pattern_row(index, definition, hashes[str(definition["id"])], query_rows[index])
        for index, definition in enumerate(definitions)
    ]
    return [ai_row, *query_rows], success_rows, pattern_rows, hashes


def _feature_parent_success() -> dict[str, object]:
    return {
        "research_run_spec_id": 29,
        "run_fingerprint": FEATURE_FINGERPRINT,
        "run_kind": "FEATURE_BUILD",
        "result_artifact_id": 19,
        "artifact_sha256": FEATURE_ARTIFACT_SHA256,
        "artifact_type": "PHASE1A_FEATURE_BUILD_MANIFEST",
    }


def _verify_with_mocked_database(
    exposure_rows: list[dict[str, object]],
    success_rows: list[dict[str, object]],
    pattern_rows: list[dict[str, object]],
    hashes: dict[str, str],
) -> tuple[Phase1APredecessorSliceReport, _DbConnection]:
    connection = _DbConnection(
        [
            {},
            {"campaign_id": 1, "campaign_key": CAMPAIGN},
            exposure_rows,
            [_feature_parent_success()],
            success_rows,
            pattern_rows,
        ]
    )
    with patch(
        "systematic_fx.db.research_registry.psycopg.connect",
        return_value=connection,
    ):
        report = verify_phase1a_predecessor_slice(
            "postgresql:///fixture",
            campaign_key=CAMPAIGN,
            prior_slice_index=0,
            source_interval_start=START,
            source_interval_end=END,
            requested_source_dates=SOURCE_DATES,
            query_definition_sha256_by_id=hashes,
        )
    return report, connection


def test_predecessor_input_contract_requires_exact_dates_and_query_budget() -> None:
    _, _, start, end, source_dates, query_definitions = _phase1a_predecessor_inputs(
        "postgresql://fixture",
        campaign_key=CAMPAIGN,
        prior_slice_index=0,
        source_interval_start=START,
        source_interval_end=END,
        requested_source_dates=SOURCE_DATES,
        query_definition_sha256_by_id=_definitions()[1],
    )

    assert (start, end, source_dates) == (START, END, SOURCE_DATES)
    assert len(query_definitions) == 11

    with pytest.raises(ResearchRegistryError, match="exactly span"):
        _phase1a_predecessor_inputs(
            "postgresql://fixture",
            campaign_key=CAMPAIGN,
            prior_slice_index=0,
            source_interval_start=START,
            source_interval_end=datetime(2022, 1, 9, tzinfo=UTC),
            requested_source_dates=SOURCE_DATES,
            query_definition_sha256_by_id=_definitions()[1],
        )
    with pytest.raises(ResearchRegistryError, match="requires 11 queries"):
        _phase1a_predecessor_inputs(
            "postgresql://fixture",
            campaign_key=CAMPAIGN,
            prior_slice_index=0,
            source_interval_start=START,
            source_interval_end=END,
            requested_source_dates=SOURCE_DATES,
            query_definition_sha256_by_id=dict(list(_definitions()[1].items())[:-1]),
        )


def test_exact_ai_and_query_slice_lineage_is_accepted() -> None:
    ai_row, definitions, hashes = _ai_row()
    candidate_definitions, artifact_id, artifact_sha, run_spec_id, run_fingerprint = (
        _validate_phase1a_ai_predecessor(
            ai_row,
            campaign_key=CAMPAIGN,
            prior_slice_index=0,
            requested_date_strings=[day.isoformat() for day in SOURCE_DATES],
            query_definitions=tuple(hashes.items()),
        )
    )
    assert candidate_definitions == tuple(definitions)
    assert (artifact_id, artifact_sha, run_spec_id, run_fingerprint) == (
        20,
        ARTIFACT_SHA256,
        30,
        _ai_fingerprint(),
    )

    query_row = _query_row(0, definitions[0], hashes["q00"])
    ai_spec = _canonical_spec(_ai_run_spec(definitions))
    _validate_phase1a_query_predecessor(
        _DbConnection([]),
        query_row,
        campaign_key=CAMPAIGN,
        prior_slice_index=0,
        requested_date_strings=[day.isoformat() for day in SOURCE_DATES],
        expected_query_id="q00",
        expected_definition_sha256=hashes["q00"],
        expected_definition=definitions[0],
        ai_artifact_id=20,
        ai_artifact_sha256=ARTIFACT_SHA256,
        ai_run_spec_id=30,
        ai_run_fingerprint=_ai_fingerprint(),
        ai_canonical_spec=ai_spec,
        artifact_query_evidence=_artifact_query_evidence()["q00"],
    )


def _rehash_query_row(row: dict[str, object]) -> None:
    canonical_spec = row["canonical_spec"]
    query_spec = row["query_spec"]
    assert isinstance(canonical_spec, dict)
    assert isinstance(query_spec, dict)
    fingerprint = canonical_sha256(canonical_spec)
    row["run_fingerprint"] = fingerprint
    query_spec["run_fingerprint"] = fingerprint


def _validate_q00(row: dict[str, object], *, connection: _DbConnection | None = None) -> None:
    definitions, hashes = _definitions()
    _validate_phase1a_query_predecessor(
        connection or _DbConnection([]),
        row,
        campaign_key=CAMPAIGN,
        prior_slice_index=0,
        requested_date_strings=[day.isoformat() for day in SOURCE_DATES],
        expected_query_id="q00",
        expected_definition_sha256=hashes["q00"],
        expected_definition=definitions[0],
        ai_artifact_id=20,
        ai_artifact_sha256=ARTIFACT_SHA256,
        ai_run_spec_id=30,
        ai_run_fingerprint=_ai_fingerprint(),
        ai_canonical_spec=_canonical_spec(_ai_run_spec(definitions)),
        artifact_query_evidence=_artifact_query_evidence()["q00"],
    )


def test_fixed_ai_and_query_engine_versions_are_required() -> None:
    ai_row, definitions, hashes = _ai_row()
    ai_row["engine_version"] = "unexpected-ai-engine"
    with pytest.raises(ResearchRegistryDriftError, match="AI_SLICE RunSpec"):
        _validate_phase1a_ai_predecessor(
            ai_row,
            campaign_key=CAMPAIGN,
            prior_slice_index=0,
            requested_date_strings=[day.isoformat() for day in SOURCE_DATES],
            query_definitions=tuple(hashes.items()),
        )

    query_row = _query_row(0, definitions[0], hashes["q00"])
    query_row["engine_version"] = "unexpected-query-engine"
    with pytest.raises(ResearchRegistryDriftError, match="RunSpec parentage"):
        _validate_q00(query_row)


def test_ai_slice_rejects_an_extra_parameter() -> None:
    row, _, hashes = _ai_row()
    canonical_spec = row["canonical_spec"]
    query_spec = row["query_spec"]
    metadata = row["artifact_metadata"]
    assert isinstance(canonical_spec, dict)
    assert isinstance(query_spec, dict)
    assert isinstance(metadata, dict)
    parameters = canonical_spec["parameters"]
    assert isinstance(parameters, dict)
    parameters["untracked_variable"] = "must-fail-closed"
    fingerprint = canonical_sha256(canonical_spec)
    row["run_fingerprint"] = fingerprint
    query_spec["run_fingerprint"] = fingerprint
    metadata["run_fingerprint"] = fingerprint

    with pytest.raises(ResearchRegistryDriftError, match="RunSpec parameters fields drift"):
        _validate_phase1a_ai_predecessor(
            row,
            campaign_key=CAMPAIGN,
            prior_slice_index=0,
            requested_date_strings=[day.isoformat() for day in SOURCE_DATES],
            query_definitions=tuple(hashes.items()),
        )


def test_normal_query_rejects_an_extra_parameter() -> None:
    definitions, hashes = _definitions()
    row = _query_row(0, definitions[0], hashes["q00"])
    canonical_spec = row["canonical_spec"]
    assert isinstance(canonical_spec, dict)
    parameters = canonical_spec["parameters"]
    assert isinstance(parameters, dict)
    parameters["untracked_variable"] = "must-fail-closed"
    _rehash_query_row(row)

    with pytest.raises(ResearchRegistryDriftError, match="RunSpec parameters fields drift"):
        _validate_q00(row)


def _complete_recovery_projection() -> dict[str, object]:
    return {
        "artifact_schema": "systematic_fx.phase1a_query_recovery_projection.v1",
        "mode": "IMMUTABLE_AI_ARTIFACT_PROJECTION",
        "no_research_recomputation": True,
        "recovery_code_commit": "2" * 40,
        "recovery_code_snapshot_sha256": "1" * 64,
        "recovery_control_run_fingerprint": "2" * 64,
        "recovery_manifest_artifact_id": 50,
        "recovery_manifest_relative_path": "manifests/recovery.json",
        "recovery_manifest_sha256": "3" * 64,
        "recovery_runtime_sha256": "4" * 64,
        "source_ai_canonical_sha256": canonical_sha256(
            _canonical_spec(_ai_run_spec(_definitions()[0]))
        ),
        "source_ai_code_snapshot_sha256": "f" * 64,
        "source_ai_run_fingerprint": _ai_fingerprint(),
        "source_artifact_id": 20,
        "source_artifact_sha256": ARTIFACT_SHA256,
    }


@pytest.mark.parametrize("shape", ["missing", "extra", "null"])
def test_recovery_projection_requires_one_exact_non_null_shape(shape: str) -> None:
    definitions, hashes = _definitions()
    row = _query_row(0, definitions[0], hashes["q00"])
    canonical_spec = row["canonical_spec"]
    assert isinstance(canonical_spec, dict)
    parameters = canonical_spec["parameters"]
    assert isinstance(parameters, dict)
    projection: object = _complete_recovery_projection()
    if shape == "missing":
        assert isinstance(projection, dict)
        projection.pop("recovery_manifest_sha256")
    elif shape == "extra":
        assert isinstance(projection, dict)
        projection["untracked_variable"] = "must-fail-closed"
    elif shape == "null":
        projection = None
    else:  # pragma: no cover - parametrization is exhaustive
        raise AssertionError(shape)
    parameters["recovery_projection"] = projection
    _rehash_query_row(row)
    connection = _DbConnection([])

    with pytest.raises(ResearchRegistryDriftError):
        _validate_q00(row, connection=connection)
    assert connection.calls == []


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("query_spec", "definition_sha256"), "f" * 64),
        (("canonical_spec", "parameters", "requested_source_dates"), ["2022-01-03"]),
        (("artifact_metadata", "exposure_type"), "QUERY"),
    ],
)
def test_ai_slice_definition_date_and_artifact_drift_is_rejected(
    path: tuple[str, ...],
    value: object,
) -> None:
    row, _, hashes = _ai_row()
    target: dict[str, object] = row
    for key in path[:-1]:
        child = target[key]
        assert isinstance(child, dict)
        target = child
    target[path[-1]] = value

    with pytest.raises(ResearchRegistryDriftError):
        _validate_phase1a_ai_predecessor(
            row,
            campaign_key=CAMPAIGN,
            prior_slice_index=0,
            requested_date_strings=[day.isoformat() for day in SOURCE_DATES],
            query_definitions=tuple(hashes.items()),
        )


def test_pattern_rollup_requires_one_exact_observation_per_query() -> None:
    _, definitions, hashes = _ai_row()
    query_rows = {
        str(definition["id"]): _query_row(index, definition, hashes[str(definition["id"])])
        for index, definition in enumerate(definitions)
    }
    patterns = [
        _pattern_row(
            index, definition, hashes[str(definition["id"])], query_rows[str(definition["id"])]
        )
        for index, definition in enumerate(definitions)
    ]

    pattern_ids = _validate_phase1a_pattern_predecessor(
        patterns,
        campaign_key=CAMPAIGN,
        source_interval_start=START,
        source_interval_end=END,
        query_rows_by_id=query_rows,
        query_definitions=tuple(hashes.items()),
        definitions_by_id={str(value["id"]): value for value in definitions},
    )
    assert pattern_ids == tuple(range(300, 311))

    missing = deepcopy(patterns)
    feature_document = missing[0]["feature_definition_versions"]
    assert isinstance(feature_document, dict)
    feature_document["slice_identities"] = []
    with pytest.raises(ResearchRegistryDriftError, match="exactly one"):
        _validate_phase1a_pattern_predecessor(
            missing,
            campaign_key=CAMPAIGN,
            source_interval_start=START,
            source_interval_end=END,
            query_rows_by_id=query_rows,
            query_definitions=tuple(hashes.items()),
            definitions_by_id={str(value["id"]): value for value in definitions},
        )

    extra = deepcopy(patterns)
    summary_document = extra[0]["forward_first_touch_summaries"]
    assert isinstance(summary_document, dict)
    observations = summary_document["slice_observations"]
    assert isinstance(observations, list)
    unexpected = deepcopy(observations[0])
    assert isinstance(unexpected, dict)
    unexpected["discovery_exposure_id"] = 9999
    observations.append(unexpected)
    with pytest.raises(ResearchRegistryDriftError, match="unexpected"):
        _validate_phase1a_pattern_predecessor(
            extra,
            campaign_key=CAMPAIGN,
            source_interval_start=START,
            source_interval_end=END,
            query_rows_by_id=query_rows,
            query_definitions=tuple(hashes.items()),
            definitions_by_id={str(value["id"]): value for value in definitions},
        )


def test_public_verifier_returns_only_exact_ordered_slice_identities() -> None:
    exposure_rows, success_rows, pattern_rows, hashes = _public_fixture()

    report, connection = _verify_with_mocked_database(
        exposure_rows,
        success_rows,
        pattern_rows,
        hashes,
    )

    assert report == Phase1APredecessorSliceReport(
        prior_slice_index=0,
        ai_exposure_id=10,
        query_exposure_ids=tuple(range(100, 111)),
        pattern_ids=tuple(range(300, 311)),
        result_artifact_id=20,
    )
    assert connection.isolation_level is IsolationLevel.SERIALIZABLE
    assert len(connection.calls) == 6
    assert "requested_source_dates" in connection.calls[2]
    assert "parent_run_spec_id" not in connection.calls[3]
    assert "status = 'SUCCEEDED'" in connection.calls[4]
    assert "pattern_ledger" in connection.calls[5]


@pytest.mark.parametrize(
    "drift",
    [
        "missing_query",
        "extra_query",
        "query_definition",
        "query_artifact",
        "successful_attempt",
        "pattern_observation",
    ],
)
def test_public_verifier_rejects_partial_or_mismatched_database_slice(drift: str) -> None:
    exposure_rows, success_rows, pattern_rows, hashes = _public_fixture()
    if drift == "missing_query":
        exposure_rows.pop()
    elif drift == "extra_query":
        extra = deepcopy(exposure_rows[-1])
        extra["discovery_exposure_id"] = 999
        exposure_rows.append(extra)
    elif drift == "query_definition":
        query_spec = exposure_rows[1]["query_spec"]
        assert isinstance(query_spec, dict)
        candidate = query_spec["candidate_query"]
        assert isinstance(candidate, dict)
        candidate["rule"] = "drifted"
    elif drift == "query_artifact":
        exposure_rows[1]["artifact_sha256"] = "f" * 64
    elif drift == "successful_attempt":
        success_rows.pop()
    elif drift == "pattern_observation":
        summary = pattern_rows[0]["forward_first_touch_summaries"]
        assert isinstance(summary, dict)
        summary["slice_observations"] = []
    else:  # pragma: no cover - parametrization is exhaustive
        raise AssertionError(drift)

    connection = _DbConnection(
        [
            {},
            {"campaign_id": 1, "campaign_key": CAMPAIGN},
            exposure_rows,
            [_feature_parent_success()],
            success_rows,
            pattern_rows,
        ]
    )
    with (
        patch(
            "systematic_fx.db.research_registry.psycopg.connect",
            return_value=connection,
        ),
        pytest.raises(ResearchRegistryDriftError),
    ):
        verify_phase1a_predecessor_slice(
            "postgresql:///fixture",
            campaign_key=CAMPAIGN,
            prior_slice_index=0,
            source_interval_start=START,
            source_interval_end=END,
            requested_source_dates=SOURCE_DATES,
            query_definition_sha256_by_id=hashes,
        )


def test_current_slice_prefix_accepts_empty_boundary() -> None:
    _, _, _, hashes = _public_fixture()
    connection = _DbConnection([{}, {"campaign_id": 1, "campaign_key": CAMPAIGN}, [], [], []])
    with patch(
        "systematic_fx.db.research_registry.psycopg.connect",
        return_value=connection,
    ):
        report = verify_phase1a_current_slice_prefix(
            "postgresql:///fixture",
            campaign_key=CAMPAIGN,
            slice_index=0,
            source_interval_start=START,
            source_interval_end=END,
            requested_source_dates=SOURCE_DATES,
            expected_feature_run_fingerprint=FEATURE_FINGERPRINT,
            query_definition_sha256_by_id=hashes,
        )

    assert report == Phase1ACurrentSlicePrefixReport(
        slice_index=0,
        state="EMPTY",
        feature_run_spec_id=None,
        ai_exposure_id=None,
        query_exposure_ids=(),
        pattern_ids=(),
        result_artifact_id=None,
        missing_pattern_query_id=None,
    )


def _failed_feature_attempt_row(
    *,
    research_run_spec_id: int = 40,
    research_run_attempt_id: int | None = 50,
    run_kind: str = "FEATURE_BUILD",
    status: str | None = "FAILED",
    result_artifact_id: int | None = None,
    reused_attempt_id: int | None = None,
    trade_ledger_artifact_id: int | None = None,
) -> dict[str, object]:
    return {
        "research_run_spec_id": research_run_spec_id,
        "run_kind": run_kind,
        "research_run_attempt_id": research_run_attempt_id,
        "status": status,
        "result_artifact_id": result_artifact_id,
        "reused_attempt_id": reused_attempt_id,
        "trade_ledger_artifact_id": trade_ledger_artifact_id,
    }


def _verify_feature_only_current_slice(
    slice_run_rows: list[dict[str, object]],
    *,
    pattern_rows: list[dict[str, object]] | None = None,
) -> tuple[Phase1ACurrentSlicePrefixReport, _DbConnection]:
    hashes = _definitions()[1]
    connection = _DbConnection(
        [
            {},
            {"campaign_id": 1, "campaign_key": CAMPAIGN},
            [],
            pattern_rows or [],
            slice_run_rows,
        ]
    )
    with patch(
        "systematic_fx.db.research_registry.psycopg.connect",
        return_value=connection,
    ):
        report = verify_phase1a_current_slice_prefix(
            "postgresql:///fixture",
            campaign_key=CAMPAIGN,
            slice_index=0,
            source_interval_start=START,
            source_interval_end=END,
            requested_source_dates=SOURCE_DATES,
            expected_feature_run_fingerprint=FEATURE_FINGERPRINT,
            query_definition_sha256_by_id=hashes,
        )
    return report, connection


def test_current_slice_prefix_marks_only_clean_failed_features_retryable() -> None:
    report, connection = _verify_feature_only_current_slice(
        [
            _failed_feature_attempt_row(),
            _failed_feature_attempt_row(research_run_attempt_id=51),
            _failed_feature_attempt_row(
                research_run_spec_id=41,
                research_run_attempt_id=52,
            ),
        ]
    )

    assert report == Phase1ACurrentSlicePrefixReport(
        slice_index=0,
        state="FAILED_FEATURE_RETRYABLE",
        feature_run_spec_id=None,
        ai_exposure_id=None,
        query_exposure_ids=(),
        pattern_ids=(),
        result_artifact_id=None,
        missing_pattern_query_id=None,
    )
    assert "attempt.result_artifact_id" in connection.calls[4]
    assert "attempt.reused_attempt_id" in connection.calls[4]
    assert "attempt.trade_ledger_artifact_id" in connection.calls[4]


@pytest.mark.parametrize(
    "status",
    ["QUEUED", "RUNNING", "SUCCEEDED", "REJECTED", "CANCELLED", "SKIPPED_DUPLICATE"],
)
def test_current_slice_prefix_rejects_nonfailed_feature_attempts(status: str) -> None:
    rows = [
        _failed_feature_attempt_row(),
        _failed_feature_attempt_row(research_run_attempt_id=51, status=status),
    ]

    with pytest.raises(ResearchRegistryDriftError, match="solely of terminal FAILED"):
        _verify_feature_only_current_slice(rows)


@pytest.mark.parametrize(
    "overrides",
    [
        {"research_run_attempt_id": None, "status": None},
        {"run_kind": "AI_SLICE"},
        {"result_artifact_id": 60},
        {"reused_attempt_id": 61},
        {"trade_ledger_artifact_id": 62},
    ],
)
def test_current_slice_prefix_rejects_ambiguous_failed_feature_state(
    overrides: dict[str, object],
) -> None:
    rows = [_failed_feature_attempt_row(), _failed_feature_attempt_row(**overrides)]

    with pytest.raises(ResearchRegistryDriftError):
        _verify_feature_only_current_slice(rows)


def test_current_slice_prefix_rejects_pattern_state_with_failed_features() -> None:
    pattern_rows = _public_fixture()[2][:1]

    with pytest.raises(ResearchRegistryDriftError, match="unexpected pattern observation"):
        _verify_feature_only_current_slice(
            [_failed_feature_attempt_row()],
            pattern_rows=pattern_rows,
        )


def _verify_current_prefix(
    *,
    query_count: int,
    pattern_count: int,
    expected_feature_fingerprint: str = FEATURE_FINGERPRINT,
) -> Phase1ACurrentSlicePrefixReport:
    exposure_rows, success_rows, pattern_rows, hashes = _public_fixture()
    selected_exposures = exposure_rows[: query_count + 1]
    selected_successes = success_rows[: query_count + 1]
    selected_patterns = pattern_rows[:pattern_count]
    connection = _DbConnection(
        [
            {},
            {"campaign_id": 1, "campaign_key": CAMPAIGN},
            selected_exposures,
            selected_patterns,
            [_feature_parent_success()],
            selected_successes,
        ]
    )
    with patch(
        "systematic_fx.db.research_registry.psycopg.connect",
        return_value=connection,
    ):
        return verify_phase1a_current_slice_prefix(
            "postgresql:///fixture",
            campaign_key=CAMPAIGN,
            slice_index=0,
            source_interval_start=START,
            source_interval_end=END,
            requested_source_dates=SOURCE_DATES,
            expected_feature_run_fingerprint=expected_feature_fingerprint,
            query_definition_sha256_by_id=hashes,
        )


def test_current_slice_prefix_accepts_ordered_queries_and_one_final_missing_pattern() -> None:
    report = _verify_current_prefix(query_count=3, pattern_count=2)

    assert report.state == "RESUMABLE"
    assert report.feature_run_spec_id == 29
    assert report.ai_exposure_id == 10
    assert report.query_exposure_ids == (100, 101, 102)
    assert report.pattern_ids == (300, 301)
    assert report.result_artifact_id == 20
    assert report.missing_pattern_query_id == "q02"


def test_current_slice_prefix_rejects_different_feature_parent() -> None:
    with pytest.raises(ResearchRegistryDriftError, match="different FEATURE_BUILD"):
        _verify_current_prefix(
            query_count=0,
            pattern_count=0,
            expected_feature_fingerprint="f" * 64,
        )


def test_current_slice_prefix_rejects_query_gap_and_multiple_missing_patterns() -> None:
    exposure_rows, success_rows, pattern_rows, hashes = _public_fixture()
    gap_connection = _DbConnection(
        [
            {},
            {"campaign_id": 1, "campaign_key": CAMPAIGN},
            [exposure_rows[0], exposure_rows[1], exposure_rows[3]],
            pattern_rows[:2],
            [_feature_parent_success()],
        ]
    )
    with (
        patch(
            "systematic_fx.db.research_registry.psycopg.connect",
            return_value=gap_connection,
        ),
        pytest.raises(ResearchRegistryDriftError, match="ordered config prefix"),
    ):
        verify_phase1a_current_slice_prefix(
            "postgresql:///fixture",
            campaign_key=CAMPAIGN,
            slice_index=0,
            source_interval_start=START,
            source_interval_end=END,
            requested_source_dates=SOURCE_DATES,
            expected_feature_run_fingerprint=FEATURE_FINGERPRINT,
            query_definition_sha256_by_id=hashes,
        )

    missing_patterns_connection = _DbConnection(
        [
            {},
            {"campaign_id": 1, "campaign_key": CAMPAIGN},
            exposure_rows[:4],
            pattern_rows[:1],
            [_feature_parent_success()],
            success_rows[:4],
        ]
    )
    with (
        patch(
            "systematic_fx.db.research_registry.psycopg.connect",
            return_value=missing_patterns_connection,
        ),
        pytest.raises(ResearchRegistryDriftError, match="exactly one"),
    ):
        verify_phase1a_current_slice_prefix(
            "postgresql:///fixture",
            campaign_key=CAMPAIGN,
            slice_index=0,
            source_interval_start=START,
            source_interval_end=END,
            requested_source_dates=SOURCE_DATES,
            expected_feature_run_fingerprint=FEATURE_FINGERPRINT,
            query_definition_sha256_by_id=hashes,
        )
