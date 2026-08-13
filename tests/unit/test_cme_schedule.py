from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from systematic_fx.data.cme_schedule import (
    CmeScheduleEvidenceError,
    load_cme_schedule_archive,
    unavailable_schedule_decision,
    verify_schedule_upstream_source,
)

PROJECT = Path(__file__).resolve().parents[2]
FIXTURE = PROJECT / "tests/fixtures/cme_schedule_archive_fixture_v1.toml"
UPSTREAM_FIXTURE = PROJECT / "tests/fixtures/cme_schedule_upstream_fixture_v1.txt"


def _ns(month: int, day: int, hour: int, minute: int = 0) -> int:
    return int(datetime(2022, month, day, hour, minute, tzinfo=UTC).timestamp() * 1e9)


def test_schedule_fixture_requires_explicit_test_only_opt_in_and_hashes_exact_bytes(
    tmp_path: Path,
) -> None:
    with pytest.raises(CmeScheduleEvidenceError, match="test-only opt-in"):
        load_cme_schedule_archive(FIXTURE)
    archive = load_cme_schedule_archive(FIXTURE, allow_test_fixture=True)
    assert archive.is_test_fixture
    assert archive.source_id == "TEST_ONLY_NOT_CME_SCHEDULE_EVIDENCE"
    assert archive.source_sha256 == hashlib.sha256(UPSTREAM_FIXTURE.read_bytes()).hexdigest()
    assert archive.sha256 == hashlib.sha256(FIXTURE.read_bytes()).hexdigest()
    assert archive.sha256 == "6476edaaa819177dbcd3bc337bcc906d9d40e8a92d8777256c78b43ce3d55864"
    verified = verify_schedule_upstream_source(archive, UPSTREAM_FIXTURE)
    assert verified.source_bytes_verified
    verified.verify_unchanged()

    mutated = tmp_path / "mutated-source.txt"
    mutated.write_text("not the frozen source")
    with pytest.raises(CmeScheduleEvidenceError, match="drifted"):
        verify_schedule_upstream_source(archive, mutated)


def test_as_of_uses_only_already_published_schedule_revision() -> None:
    archive = load_cme_schedule_archive(FIXTURE, allow_test_fixture=True)
    before_revision = archive.session_as_of(date(2022, 9, 1), _ns(8, 15, 0))
    assert before_revision.coverage_verified
    assert before_revision.session is not None
    assert before_revision.session.revision == 1
    assert before_revision.session.close_ts_ns == _ns(9, 1, 21)

    after_revision = archive.session_as_of(date(2022, 9, 1), _ns(8, 31, 13))
    assert after_revision.session is not None
    assert after_revision.session.revision == 2
    assert after_revision.session.close_ts_ns == _ns(9, 1, 20)

    unpublished = archive.session_as_of(date(2022, 9, 1), _ns(7, 31, 23))
    assert not unpublished.coverage_verified
    assert unpublished.reason == "SCHEDULE_NOT_YET_PUBLISHED"


def test_entry_window_uses_as_of_close_and_rejects_known_break() -> None:
    archive = load_cme_schedule_archive(FIXTURE, allow_test_fixture=True)
    event = _ns(9, 1, 19, 30)
    revised = archive.entry_window_as_of(
        event,
        3600,
        as_of_ts_ns=event,
    )
    assert revised.schedule_verified and not revised.eligible
    assert revised.reason == "CROSSES_SCHEDULED_CLOSE"

    crosses_break = archive.entry_window_as_of(
        _ns(9, 1, 9, 59),
        600,
        as_of_ts_ns=_ns(9, 1, 9, 59),
    )
    assert not crosses_break.eligible
    assert crosses_break.reason == "CROSSES_SCHEDULED_BREAK"


def test_entry_window_cannot_use_schedule_knowledge_from_after_the_event() -> None:
    archive = load_cme_schedule_archive(FIXTURE, allow_test_fixture=True)
    event = _ns(9, 1, 19, 30)
    with pytest.raises(CmeScheduleEvidenceError, match="exactly as of the event"):
        archive.entry_window_as_of(event, 600, as_of_ts_ns=event + 1)


def test_previous_completed_session_comes_from_archive_not_weekday_arithmetic() -> None:
    archive = load_cme_schedule_archive(FIXTURE, allow_test_fixture=True)
    target = archive.sessions[-1]
    assert (
        archive.previous_completed_trading_date_as_of(
            target.trading_date,
            as_of_ts_ns=target.open_ts_ns,
        )
        == archive.sessions[-2].trading_date
    )


def test_previous_completed_session_must_be_closed_by_the_as_of_instant() -> None:
    archive = load_cme_schedule_archive(FIXTURE, allow_test_fixture=True)
    target = archive.sessions[-1]
    before_prior_close = archive.sessions[-2].close_ts_ns - 1
    with pytest.raises(CmeScheduleEvidenceError, match="previous completed session"):
        archive.previous_completed_session_as_of(
            target.trading_date,
            as_of_ts_ns=before_prior_close,
        )


def test_coverage_overlap_and_path_fail_closed(tmp_path: Path) -> None:
    archive = load_cme_schedule_archive(FIXTURE, allow_test_fixture=True)
    outside = archive.session_as_of(date(2022, 9, 4), _ns(9, 1, 0))
    assert not outside.coverage_verified
    assert outside.reason == "SCHEDULE_OUTSIDE_ARCHIVE_COVERAGE"
    assert unavailable_schedule_decision().reason == "SCHEDULE_ARCHIVE_NOT_SUPPLIED"

    overlap = tmp_path / "overlap.toml"
    overlap.write_text(
        FIXTURE.read_text().replace(
            "open_ts_ns = 1662069600000000000",
            "open_ts_ns = 1662060000000000000",
            1,
        )
    )
    with pytest.raises(CmeScheduleEvidenceError, match="overlap"):
        load_cme_schedule_archive(overlap, allow_test_fixture=True)

    actual = tmp_path / "actual"
    actual.mkdir()
    copied = actual / "schedule.toml"
    copied.write_bytes(FIXTURE.read_bytes())
    alias = tmp_path / "alias"
    alias.symlink_to(actual, target_is_directory=True)
    with pytest.raises(CmeScheduleEvidenceError, match="symbolic link"):
        load_cme_schedule_archive(alias / copied.name, allow_test_fixture=True)

    protected = tmp_path / "sealed-schedule.toml"
    protected.write_bytes(FIXTURE.read_bytes())
    with pytest.raises(CmeScheduleEvidenceError, match="search-safe"):
        load_cme_schedule_archive(protected, allow_test_fixture=True)
