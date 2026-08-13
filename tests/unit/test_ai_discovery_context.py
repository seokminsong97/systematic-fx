from __future__ import annotations

import os
import stat
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from systematic_fx.features.bars import ONE_SECOND_NS, TradeBar
from systematic_fx.research.ai_discovery_context import (
    BAR_COLUMNS,
    DAILY_SUMMARY_COLUMNS,
    EXPECTED_AI_DISCOVERY_CONTEXT_BYTES,
    EXPECTED_AI_DISCOVERY_CONTEXT_IDENTITY_SHA256,
    EXPECTED_AI_DISCOVERY_CONTEXT_SHA256,
    EXPECTED_DATASET_HANDOFF_SHA256,
    EXPECTED_DATASET_MANIFEST_SHA256,
    EXPECTED_DISCOVERY_ACTIVE_DAYS,
    EXPECTED_DISCOVERY_BAR_ROWS,
    EXPECTED_DISCOVERY_CALENDAR_SHA256,
    EXPECTED_DISCOVERY_DECISION_DAYS,
    EXPECTED_DISCOVERY_SOURCE_BYTES,
    EXPECTED_SPLIT_PLAN_SHA256,
    MAX_BAR_ROWS,
    MAX_CONTEXT_BYTES,
    MAX_SOURCE_ARTIFACT_BYTES,
    MAX_THRESHOLD_SUPPORT_ROWS,
    THRESHOLD_LATTICE_SHA256,
    THRESHOLD_SUPPORT_COLUMNS,
    AIDiscoveryContextArtifact,
    AIDiscoveryContextError,
    _assert_safe_context,
    _build_context_document,
    _ContextSourceSpec,
    _parse_context_bytes,
    _publish_context_document,
    _reopen_context_for_spec,
    _validate_context_document,
    build_ai_discovery_context,
    load_ai_discovery_context,
    reopen_ai_discovery_context,
)
from systematic_fx.research.bar_artifacts import BarArtifactDriftError
from systematic_fx.research.bar_config import BAR_SOURCE_MANIFEST_SHA256
from systematic_fx.research.hypotheses import canonical_json_bytes

ROOT = Path(__file__).resolve().parents[2]
RUN_REAL = os.environ.get("SYSTEMATIC_FX_RUN_AI_DISCOVERY_CONTEXT_REAL") == "1"


def _spec(*, row_count: int = 6, source_bytes: int = 600) -> _ContextSourceSpec:
    dates = tuple(date(2022, 1, day) for day in range(3, 8))
    return _ContextSourceSpec(
        dataset_manifest_sha256="a" * 64,
        dataset_handoff_sha256="b" * 64,
        raw_source_manifest_sha256="c" * 64,
        split_plan_sha256="d" * 64,
        discovery_dates=dates,
        reporting_blocks=tuple((item,) for item in dates[:4]),
        expected_bar_row_count=row_count,
        source_artifact_byte_count=source_bytes,
    )


def _bar(
    source_date: date,
    minute: int,
    *,
    open_ticks: int,
    high_ticks: int,
    low_ticks: int,
    close_ticks: int,
) -> TradeBar:
    start_seconds = (
        int(
            datetime(
                source_date.year,
                source_date.month,
                source_date.day,
                tzinfo=UTC,
            ).timestamp()
        )
        + minute * 60
    )
    start_ns = start_seconds * ONE_SECOND_NS
    return TradeBar(
        timeframe_seconds=300,
        segment_id=1,
        contract="SYNTHETIC",
        source_date=source_date,
        start_ns=start_ns,
        end_ns=start_ns + 300 * ONE_SECOND_NS,
        first_trade_ns=start_ns,
        last_trade_ns=start_ns + ONE_SECOND_NS,
        open_ticks=open_ticks,
        high_ticks=high_ticks,
        low_ticks=low_ticks,
        close_ticks=close_ticks,
        trade_count=2,
        volume=3,
        observed_subbars=2,
    )


def _bars() -> tuple[TradeBar, ...]:
    dates = _spec().discovery_dates
    return (
        _bar(dates[0], 0, open_ticks=100, high_ticks=110, low_ticks=90, close_ticks=105),
        _bar(dates[0], 5, open_ticks=105, high_ticks=108, low_ticks=95, close_ticks=99),
        _bar(dates[1], 0, open_ticks=100, high_ticks=100, low_ticks=100, close_ticks=100),
        _bar(dates[2], 0, open_ticks=100, high_ticks=104, low_ticks=99, close_ticks=103),
        _bar(dates[3], 0, open_ticks=100, high_ticks=101, low_ticks=95, close_ticks=96),
        # The last date is the visible, non-decision tail.  Its extreme range must not
        # contribute to finite-lattice support.
        _bar(dates[4], 0, open_ticks=100, high_ticks=200, low_ticks=100, close_ticks=200),
    )


def _document() -> dict[str, object]:
    return _build_context_document(_spec(), _bars())


def test_projection_keeps_only_point_in_time_morphology_and_exact_calendar() -> None:
    document = _document()
    first = document["bars"][0]
    assert dict(zip(BAR_COLUMNS, first, strict=True)) == {
        "block_number": 1,
        "close_location_ppm": 750_000,
        "decision_eligible": True,
        "end_ns": first[1] + 300 * ONE_SECOND_NS,
        "lower_wick_ppm": 500_000,
        "range_ticks": 20,
        "signed_body_ppm": 250_000,
        "source_date": "2022-01-03",
        "start_ns": first[1],
        "upper_wick_ppm": 250_000,
    }
    zero_range = document["bars"][2]
    assert zero_range[5:] == [0, 0, 500_000, 0, 0]
    assert document["source"] == _spec().source_document()
    assert [row[0] for row in document["daily_summaries"]] == [
        item.isoformat() for item in _spec().discovery_dates
    ]
    assert [row[3] for row in document["block_summaries"]] == [1, 1, 1, 1]


def test_daily_block_summaries_and_lattice_support_replay_exactly() -> None:
    document = _document()
    first_daily = dict(zip(DAILY_SUMMARY_COLUMNS, document["daily_summaries"][0], strict=True))
    assert first_daily["bar_count"] == 2
    assert first_daily["positive_body_count"] == 1
    assert first_daily["negative_body_count"] == 1
    assert first_daily["range_ticks_sum"] == 33

    lattice = document["threshold_lattice"]
    assert lattice["support_columns"] == list(THRESHOLD_SUPPORT_COLUMNS)
    assert len(lattice["supports"]) <= MAX_THRESHOLD_SUPPORT_ROWS
    support = {
        (row[0], row[1], row[2]): dict(zip(THRESHOLD_SUPPORT_COLUMNS, row, strict=True))
        for row in lattice["supports"]
    }
    # Only the non-decision tail has range >= 32; it is intentionally excluded.
    assert support[("range_ticks", "GE", 32)]["bar_count"] == 0
    assert support[("range_ticks", "GE", 1)]["active_date_count"] == 3
    assert document["morphology"]["lattice_sha256"] == THRESHOLD_LATTICE_SHA256
    assert _validate_context_document(document, _spec()) == document


def test_context_has_no_raw_price_source_path_or_evaluation_vocabulary() -> None:
    document = _document()
    _assert_safe_context(document)
    content = canonical_json_bytes(document)
    for forbidden in (
        b'"contract"',
        b'"open_ticks"',
        b'"close_ticks"',
        b'"relative_uri"',
        b'"volume"',
        b"derived/trade_bars",
        b"SYNTHETIC",
        b'"result"',
        b'"pnl"',
        b'"holdout"',
    ):
        assert forbidden not in content


def test_canonical_document_and_publication_replay_are_deterministic(tmp_path: Path) -> None:
    spec = _spec()
    first_document = _build_context_document(spec, iter(_bars()))
    second_document = _build_context_document(spec, iter(_bars()))
    assert canonical_json_bytes(first_document) == canonical_json_bytes(second_document)

    first = _publish_context_document(tmp_path, first_document, spec)
    second = _publish_context_document(tmp_path, second_document, spec)
    assert first == second
    assert first.path.is_relative_to(tmp_path / "data/derived/bar_patterns")
    assert first.path.name == f"sha256={first.sha256}.json"
    assert stat.S_IMODE(first.path.stat().st_mode) & 0o222 == 0
    assert first.byte_size < MAX_CONTEXT_BYTES
    assert _reopen_context_for_spec(tmp_path, first, spec) == first_document


def test_reopen_rejects_content_tamper(tmp_path: Path) -> None:
    spec = _spec()
    artifact = _publish_context_document(tmp_path, _document(), spec)
    artifact.path.chmod(0o644)
    artifact.path.write_bytes(b"{}")
    artifact.path.chmod(0o444)

    with pytest.raises(BarArtifactDriftError, match="content differs"):
        _reopen_context_for_spec(tmp_path, artifact, spec)


def test_strict_parser_rejects_duplicate_and_noncanonical_json() -> None:
    with pytest.raises(AIDiscoveryContextError, match="duplicate key"):
        _parse_context_bytes(b'{"schema":1,"schema":2}')
    with pytest.raises(AIDiscoveryContextError, match="exact canonical JSON"):
        _parse_context_bytes(b'{ "schema": 1 }')


def test_chronology_summary_and_source_drift_are_rejected() -> None:
    spec = _spec()
    with pytest.raises(AIDiscoveryContextError, match="strictly chronological"):
        _build_context_document(spec, tuple(reversed(_bars())))

    summary_drift = _document()
    summary_drift["daily_summaries"][0][3] += 1
    with pytest.raises(AIDiscoveryContextError, match="summaries or finite lattice"):
        _validate_context_document(summary_drift, spec)

    source_drift = _document()
    source_drift["source"]["dataset_manifest_sha256"] = "f" * 64
    with pytest.raises(AIDiscoveryContextError, match="source identity drift"):
        _validate_context_document(source_drift, spec)


def test_precommitted_row_and_source_byte_caps_cannot_be_relaxed() -> None:
    with pytest.raises(AIDiscoveryContextError, match="expected_bar_row_count"):
        _spec(row_count=MAX_BAR_ROWS + 1)
    with pytest.raises(AIDiscoveryContextError, match="source_artifact_byte_count"):
        _spec(source_bytes=MAX_SOURCE_ARTIFACT_BYTES + 1)


def test_public_builder_rejects_symlink_roots_before_manifest_access(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    (project / "data").mkdir()
    alias = tmp_path / "project-alias"
    alias.symlink_to(project, target_is_directory=True)

    with pytest.raises(AIDiscoveryContextError, match="symbolic link"):
        build_ai_discovery_context(alias)


def test_frozen_source_identities_and_boundaries_are_explicit() -> None:
    assert EXPECTED_DATASET_MANIFEST_SHA256 == (
        "e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc"
    )
    assert EXPECTED_DATASET_HANDOFF_SHA256 == (
        "26b1bb96f7323cae13bbe5d670c12f3e85615bbb9aab56932ce6523e67af7b00"
    )
    assert EXPECTED_SPLIT_PLAN_SHA256 == (
        "5594725f6769a706018d414a5b27e3903f1d7d1cc22c98e93b6e973ead1af043"
    )
    assert EXPECTED_DISCOVERY_CALENDAR_SHA256 == (
        "88a28f1d66d0476629ea8fa0faa0a5c95e946756b191da7effbb11de805c1684"
    )
    assert BAR_SOURCE_MANIFEST_SHA256 == (
        "14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de"
    )
    assert EXPECTED_DISCOVERY_ACTIVE_DAYS == 489
    assert EXPECTED_DISCOVERY_DECISION_DAYS == 469
    assert EXPECTED_DISCOVERY_BAR_ROWS == 111_297
    assert EXPECTED_DISCOVERY_SOURCE_BYTES == 11_227_098


@pytest.mark.skipif(
    not RUN_REAL,
    reason="set SYSTEMATIC_FX_RUN_AI_DISCOVERY_CONTEXT_REAL=1 for the real 489-day gate",
)
def test_real_489_day_context_build_and_reopen() -> None:
    artifact = build_ai_discovery_context(ROOT)
    document = reopen_ai_discovery_context(ROOT, artifact)

    assert artifact.sha256 == EXPECTED_AI_DISCOVERY_CONTEXT_SHA256
    assert artifact.byte_size == EXPECTED_AI_DISCOVERY_CONTEXT_BYTES
    assert (
        artifact.published.descriptor.identity_sha256
        == EXPECTED_AI_DISCOVERY_CONTEXT_IDENTITY_SHA256
    )
    assert artifact.byte_size <= MAX_CONTEXT_BYTES
    assert artifact.path.is_relative_to(ROOT / "data/derived/bar_patterns")
    assert document["source"]["active_date_count"] == EXPECTED_DISCOVERY_ACTIVE_DAYS
    assert document["source"]["bar_row_count"] == EXPECTED_DISCOVERY_BAR_ROWS
    assert len(document["daily_summaries"]) == EXPECTED_DISCOVERY_ACTIVE_DAYS

    identity = artifact.as_dict()
    assert "path" not in identity
    assert "uri" not in identity
    assert load_ai_discovery_context(ROOT, identity=identity) == artifact
    assert AIDiscoveryContextArtifact.from_dict(ROOT, identity) == artifact

    drifted = dict(identity)
    drifted["content_sha256"] = "0" * 64
    with pytest.raises(AIDiscoveryContextError, match="artifact identity drift"):
        load_ai_discovery_context(ROOT, identity=drifted)
