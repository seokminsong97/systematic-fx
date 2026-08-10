from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from systematic_fx.features.bars import DailyPlanStatus
from systematic_fx.research.bar_pipeline import (
    BAR_DATASET_ACTIVE_DATE_COUNT,
    BAR_DATASET_ARTIFACT_COUNT,
    BAR_DATASET_BUILD_PLAN_SHA256,
    BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
    BAR_SOURCE_FILE_COUNT,
    BAR_SOURCE_MANIFEST_SHA256,
    BarPipelineError,
    _advance_outcome_span,
    _OutcomeSpanState,
    load_bar_dataset_manifest,
    load_bar_dataset_plan,
)

ROOT = Path(__file__).resolve().parents[2]
REAL_DATASET_MANIFEST_SHA256 = "e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc"
REAL_DATASET_MANIFEST = ROOT / (
    "data/derived/bar_patterns/trade_bar_dataset_manifest/"
    "identity_sha256=b0ecab04cdd3626d3c488f9108c8e9184f5dd610f51950ab7e7f74a5b7524297/"
    f"sha256={REAL_DATASET_MANIFEST_SHA256}.json"
)


@pytest.fixture(scope="module")
def real_manifest_bytes() -> bytes:
    return REAL_DATASET_MANIFEST.read_bytes()


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        + b"\n"
    )


def _immutable_manifest(tmp_path: Path, content: bytes) -> tuple[Path, str]:
    digest = hashlib.sha256(content).hexdigest()
    path = tmp_path / f"sha256={digest}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    path.chmod(0o444)
    return path.resolve(), digest


def test_real_source_manifest_is_frozen_and_fully_bound() -> None:
    plan = load_bar_dataset_plan(ROOT)

    assert plan.source_manifest_sha256 == BAR_SOURCE_MANIFEST_SHA256
    assert len(plan.source_files) == BAR_SOURCE_FILE_COUNT == 1434
    assert plan.source_files[0].source_date.isoformat() == "2022-01-02"
    assert plan.source_files[-1].source_date.isoformat() == "2026-07-31"
    assert len(plan.sha256) == 64
    assert plan.canonical_bytes.endswith(b"\n")


def test_loader_rejects_a_project_without_the_frozen_manifest(tmp_path: Path) -> None:
    (tmp_path / "data").mkdir()

    with pytest.raises((BarPipelineError, FileNotFoundError)):
        load_bar_dataset_plan(tmp_path)


def test_real_bar_dataset_manifest_is_fully_verified_for_discovery() -> None:
    loaded = load_bar_dataset_manifest(
        REAL_DATASET_MANIFEST,
        expected_sha256=REAL_DATASET_MANIFEST_SHA256,
    )

    assert loaded.dataset_manifest_sha256 == REAL_DATASET_MANIFEST_SHA256
    assert loaded.source_manifest_sha256 == BAR_SOURCE_MANIFEST_SHA256
    assert len(loaded.eligible_active_dates) == BAR_DATASET_ACTIVE_DATE_COUNT == 1413
    assert len(loaded.partitions) == BAR_DATASET_ACTIVE_DATE_COUNT
    assert sum(len(item.artifacts) for item in loaded.partitions) == (BAR_DATASET_ARTIFACT_COUNT)
    assert loaded.eligible_active_dates[0].isoformat() == "2022-01-03"
    assert loaded.eligible_active_dates[-1].isoformat() == "2026-07-31"
    assert loaded.partitions[0].plan_sha256 == (
        "1192d2353fd3ab99601d2003a5cf977fd6d4c93d02b25ca3c4d9e9b2c7de5935"
    )
    assert loaded.partitions[0].contract == "6EH2"
    assert loaded.partitions[0].outcome_span_id == 1
    assert loaded.partitions[-1].contract == "6EU6"
    assert loaded.partitions[-1].outcome_span_id == 31
    assert tuple(item.timeframe_seconds for item in loaded.partitions[0].artifacts) == (
        1,
        60,
        300,
        1800,
        3600,
    )
    assert BAR_DATASET_BUILD_PLAN_SHA256 == (
        "c46323e70e389dd2f7bca4b0e3e42ad86b1a9b7b502834512906e38b4651d0dc"
    )
    assert loaded.outcome_span_policy_sha256 == (BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256)
    identity = loaded.identity_dict()
    assert identity["outcome_span_policy_sha256"] == (BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256)
    assert identity["partitions"][0]["contract"] == "6EH2"
    assert identity["partitions"][0]["outcome_span_id"] == 1
    assert loaded.handoff_sha256 == (
        "26b1bb96f7323cae13bbe5d670c12f3e85615bbb9aab56932ce6523e67af7b00"
    )


def test_outcome_spans_preserve_market_gaps_and_break_on_quality_or_contract() -> None:
    loaded = load_bar_dataset_manifest(
        REAL_DATASET_MANIFEST,
        expected_sha256=REAL_DATASET_MANIFEST_SHA256,
    )
    by_date = {item.source_date.isoformat(): item for item in loaded.partitions}

    assert by_date["2022-01-07"].outcome_span_id == 1
    assert by_date["2022-01-09"].outcome_span_id == 1
    assert by_date["2022-02-27"].contract == "6EH2"
    assert by_date["2022-03-01"].contract == "6EM2"
    assert by_date["2022-03-01"].outcome_span_id == 2
    assert by_date["2024-06-28"].contract == "6EU4"
    assert by_date["2024-06-28"].outcome_span_id == 17
    assert by_date["2024-07-02"].contract == "6EU4"
    assert by_date["2024-07-02"].outcome_span_id == 18


def test_zero_trade_same_contract_preserves_the_outcome_span() -> None:
    state, first = _advance_outcome_span(
        _OutcomeSpanState(),
        status=DailyPlanStatus.SELECTED,
        selected_contract="6EH2",
        has_artifacts=True,
    )
    state, zero_trade = _advance_outcome_span(
        state,
        status=DailyPlanStatus.SELECTED,
        selected_contract="6EH2",
        has_artifacts=False,
    )
    state, resumed = _advance_outcome_span(
        state,
        status=DailyPlanStatus.SELECTED,
        selected_contract="6EH2",
        has_artifacts=True,
    )

    assert first == resumed == 1
    assert zero_trade is None
    assert state.outcome_span_id == 1


@pytest.mark.parametrize(
    "status",
    tuple(item for item in DailyPlanStatus if item is not DailyPlanStatus.SELECTED),
)
def test_every_unqualified_plan_breaks_before_the_next_active_partition(
    status: DailyPlanStatus,
) -> None:
    state, first = _advance_outcome_span(
        _OutcomeSpanState(),
        status=DailyPlanStatus.SELECTED,
        selected_contract="6EH2",
        has_artifacts=True,
    )
    state, unqualified = _advance_outcome_span(
        state,
        status=status,
        selected_contract=None,
        has_artifacts=False,
    )
    state, resumed = _advance_outcome_span(
        state,
        status=DailyPlanStatus.SELECTED,
        selected_contract="6EH2",
        has_artifacts=True,
    )

    assert first == 1
    assert unqualified is None
    assert resumed == state.outcome_span_id == 2


def test_selected_contract_change_breaks_with_or_without_zero_trade_report() -> None:
    state, _ = _advance_outcome_span(
        _OutcomeSpanState(),
        status=DailyPlanStatus.SELECTED,
        selected_contract="6EH2",
        has_artifacts=True,
    )
    state, zero_trade = _advance_outcome_span(
        state,
        status=DailyPlanStatus.SELECTED,
        selected_contract="6EM2",
        has_artifacts=False,
    )
    state, changed = _advance_outcome_span(
        state,
        status=DailyPlanStatus.SELECTED,
        selected_contract="6EM2",
        has_artifacts=True,
    )
    state, direct_change = _advance_outcome_span(
        state,
        status=DailyPlanStatus.SELECTED,
        selected_contract="6EU2",
        has_artifacts=True,
    )

    assert zero_trade is None
    assert changed == 2
    assert direct_change == state.outcome_span_id == 3


def test_manifest_loader_rejects_a_sha_filename_mismatch() -> None:
    with pytest.raises(BarPipelineError, match="filename differs"):
        load_bar_dataset_manifest(
            REAL_DATASET_MANIFEST,
            expected_sha256="0" * 64,
        )


def test_manifest_loader_rejects_a_writable_inode(
    tmp_path: Path,
    real_manifest_bytes: bytes,
) -> None:
    digest = hashlib.sha256(real_manifest_bytes).hexdigest()
    path = tmp_path / f"sha256={digest}.json"
    path.write_bytes(real_manifest_bytes)
    path.chmod(0o644)

    with pytest.raises(BarPipelineError, match="immutable and read-only"):
        load_bar_dataset_manifest(path.resolve(), expected_sha256=digest)


def test_manifest_loader_rejects_a_symlink_leaf(tmp_path: Path) -> None:
    link = tmp_path / f"sha256={REAL_DATASET_MANIFEST_SHA256}.json"
    link.symlink_to(REAL_DATASET_MANIFEST)

    with pytest.raises(BarPipelineError, match="unsafe or inaccessible"):
        load_bar_dataset_manifest(
            link.absolute(),
            expected_sha256=REAL_DATASET_MANIFEST_SHA256,
        )


def test_manifest_loader_rejects_duplicate_and_noncanonical_json(
    tmp_path: Path,
    real_manifest_bytes: bytes,
) -> None:
    duplicate = real_manifest_bytes.replace(
        b'{"artifact_schema":',
        b'{"artifact_schema":"systematic_fx.trade_bar_dataset_manifest.v1","artifact_schema":',
        1,
    )
    duplicate_path, duplicate_sha = _immutable_manifest(tmp_path / "duplicate", duplicate)
    with pytest.raises(BarPipelineError, match="duplicate key"):
        load_bar_dataset_manifest(duplicate_path, expected_sha256=duplicate_sha)

    noncanonical_path, noncanonical_sha = _immutable_manifest(
        tmp_path / "noncanonical",
        real_manifest_bytes + b"\n",
    )
    with pytest.raises(BarPipelineError, match="not exact canonical JSON"):
        load_bar_dataset_manifest(noncanonical_path, expected_sha256=noncanonical_sha)


def test_manifest_loader_recomputes_report_and_descriptor_identities(
    tmp_path: Path,
    real_manifest_bytes: bytes,
) -> None:
    report_drift = json.loads(real_manifest_bytes)
    report_drift["reports"][1]["report_sha256"] = "0" * 64
    report_path, report_sha = _immutable_manifest(
        tmp_path / "report",
        _canonical_bytes(report_drift),
    )
    with pytest.raises(BarPipelineError, match="canonical report SHA-256"):
        load_bar_dataset_manifest(report_path, expected_sha256=report_sha)

    descriptor_drift = json.loads(real_manifest_bytes)
    descriptor_drift["reports"][1]["artifacts"][0]["relative_uri"] = (
        f"derived/trade_bars/version=trade_bar_v1/timeframe=1s/sha256={'0' * 64}.parquet"
    )
    descriptor_path, descriptor_sha = _immutable_manifest(
        tmp_path / "descriptor",
        _canonical_bytes(descriptor_drift),
    )
    with pytest.raises(BarPipelineError, match="path or descriptor identity drift"):
        load_bar_dataset_manifest(descriptor_path, expected_sha256=descriptor_sha)


def test_manifest_loader_recomputes_aggregate_totals(
    tmp_path: Path,
    real_manifest_bytes: bytes,
) -> None:
    document = json.loads(real_manifest_bytes)
    document["totals"]["artifact_count"] -= 1
    path, digest = _immutable_manifest(tmp_path, _canonical_bytes(document))

    with pytest.raises(BarPipelineError, match="aggregate totals"):
        load_bar_dataset_manifest(path, expected_sha256=digest)
