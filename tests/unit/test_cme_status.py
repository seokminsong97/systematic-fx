from __future__ import annotations

import hashlib
from dataclasses import fields, replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from systematic_fx.data.cme_reference import load_cme_6e_reference
from systematic_fx.data.cme_schedule import (
    load_cme_schedule_archive,
    verify_schedule_upstream_source,
)
from systematic_fx.data.cme_status import (
    CmeStatusEvidenceError,
    CmeTradingStatusEvidence,
    TradingStatus,
    TradingStatusObservation,
    load_cme_trading_status_evidence,
    unavailable_status_decision,
    verify_status_upstream_source,
)

PROJECT = Path(__file__).resolve().parents[2]
FIXTURE = PROJECT / "tests/fixtures/cme_trading_status_fixture_v1.toml"
UPSTREAM = PROJECT / "tests/fixtures/cme_trading_status_upstream_fixture_v1.txt"
REFERENCE = PROJECT / "configs/data/cme_6e_reference_v1.toml"
SCHEDULE = PROJECT / "tests/fixtures/cme_schedule_archive_fixture_v1.toml"
SCHEDULE_SOURCE = PROJECT / "tests/fixtures/cme_schedule_upstream_fixture_v1.txt"


def _ns(hour: int, second: int = 0) -> int:
    return int(datetime(2022, 9, 1, hour, 0, second, tzinfo=UTC).timestamp() * 1e9)


def test_status_fixture_requires_explicit_test_only_opt_in() -> None:
    with pytest.raises(CmeStatusEvidenceError, match="test-only opt-in"):
        load_cme_trading_status_evidence(FIXTURE)
    evidence = load_cme_trading_status_evidence(FIXTURE, allow_test_fixture=True)
    assert evidence.is_test_fixture
    assert evidence.source_id == "TEST_ONLY_NOT_CME_EVIDENCE"
    assert evidence.sha256 == hashlib.sha256(FIXTURE.read_bytes()).hexdigest()
    assert evidence.sha256 == "33d88da2f8947487a09846e53a90b4be0db4f937fd3f69f105d63326250a5a59"
    verified = verify_status_upstream_source(evidence, UPSTREAM)
    assert verified.source_bytes_verified
    verified.verify_unchanged()


def test_status_lookup_never_reads_a_future_observation_and_expires_stale_open() -> None:
    evidence = load_cme_trading_status_evidence(FIXTURE, allow_test_fixture=True)
    before_publication = evidence.status_at(_ns(13), venue="CME_GLOBEX", product_root="6E")
    assert not before_publication.coverage_verified
    assert before_publication.reason == "STATUS_NOT_YET_OBSERVED"

    opened = evidence.status_at(_ns(13, 2), venue="CME_GLOBEX", product_root="6E")
    assert opened.coverage_verified and opened.is_open
    assert opened.status is TradingStatus.OPEN

    stale = evidence.status_at(
        _ns(13) + 902 * 1_000_000_000,
        venue="CME_GLOBEX",
        product_root="6E",
    )
    assert not stale.coverage_verified
    assert stale.reason == "STATUS_OBSERVATION_STALE"

    before_halt_publication = evidence.status_at(_ns(14), venue="CME_GLOBEX", product_root="6E")
    assert not before_halt_publication.coverage_verified
    assert before_halt_publication.status is TradingStatus.OPEN
    halted = evidence.status_at(_ns(14, 2), venue="CME_GLOBEX", product_root="6E")
    assert halted.coverage_verified and not halted.is_open
    assert halted.reason == "STATUS_HALTED"


def test_calendar_and_status_are_both_required_for_entry() -> None:
    reference = load_cme_6e_reference(REFERENCE)
    event_ts_ns = _ns(13, 2)
    absent = reference.entry_eligibility("6EZ2", event_ts_ns, 3600)
    assert not absent.eligible
    assert absent.reason == "SCHEDULE_ARCHIVE_REQUIRED_FOR_ENTRY"

    evidence = load_cme_trading_status_evidence(FIXTURE, allow_test_fixture=True)
    schedule = verify_schedule_upstream_source(
        load_cme_schedule_archive(SCHEDULE, allow_test_fixture=True),
        SCHEDULE_SOURCE,
    )
    evidence = verify_status_upstream_source(evidence, UPSTREAM)
    verified = reference.entry_eligibility(
        "6EZ2",
        event_ts_ns,
        3600,
        status_evidence=evidence,
        schedule_archive=schedule,
    )
    assert not verified.eligible
    assert verified.reason == "SCHEDULE_TEST_FIXTURE_NOT_ADMISSIBLE"

    verified = reference.entry_eligibility(
        "6EZ2",
        event_ts_ns,
        3600,
        status_evidence=evidence,
        schedule_archive=schedule,
        allow_test_evidence=True,
    )
    assert verified.eligible
    assert verified.status_coverage
    assert verified.status_evidence_sha256 == evidence.sha256
    assert verified.status_observed_ts_ns == _ns(13, 1)

    halted = reference.entry_eligibility(
        "6EZ2",
        _ns(14, 2),
        3600,
        status_evidence=evidence,
        schedule_archive=schedule,
        allow_test_evidence=True,
    )
    assert not halted.eligible
    assert halted.reason == "STATUS_HALTED"


def test_entry_rejects_unverified_or_constructed_status_and_missing_schedule() -> None:
    reference = load_cme_6e_reference(REFERENCE)
    event_ts_ns = _ns(13, 2)
    loaded = load_cme_trading_status_evidence(FIXTURE, allow_test_fixture=True)
    assert (
        reference.entry_eligibility("6EZ2", event_ts_ns, 3600, status_evidence=loaded).reason
        == "SCHEDULE_ARCHIVE_REQUIRED_FOR_ENTRY"
    )

    schedule = verify_schedule_upstream_source(
        load_cme_schedule_archive(SCHEDULE, allow_test_fixture=True),
        SCHEDULE_SOURCE,
    )
    forged = CmeTradingStatusEvidence(
        version="forged",
        sha256="a" * 64,
        evidence_kind="CME_MARKET_STATUS_FEED_ARCHIVE",
        venue="CME_GLOBEX",
        product_root="6E",
        source_id="forged",
        covered_start_ts_ns=event_ts_ns - 1_000_000_000,
        covered_end_ts_ns=event_ts_ns + 1_000_000_000,
        maximum_observation_age_seconds=60,
        observations=(
            TradingStatusObservation(
                event_ts_ns - 1,
                event_ts_ns - 1,
                1,
                TradingStatus.OPEN,
            ),
        ),
        source_sha256="b" * 64,
    )
    rejected = reference.entry_eligibility(
        "6EZ2",
        event_ts_ns,
        3600,
        status_evidence=forged,
        schedule_archive=schedule,
        allow_test_evidence=True,
    )
    assert not rejected.eligible
    assert rejected.reason == "STATUS_EVIDENCE_IDENTITY_UNVERIFIED"


def test_entry_rejects_status_subclass_and_decision_identity_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reference = load_cme_6e_reference(REFERENCE)
    event_ts_ns = _ns(13, 2)
    schedule = verify_schedule_upstream_source(
        load_cme_schedule_archive(SCHEDULE, allow_test_fixture=True),
        SCHEDULE_SOURCE,
    )
    evidence = verify_status_upstream_source(
        load_cme_trading_status_evidence(FIXTURE, allow_test_fixture=True),
        UPSTREAM,
    )

    class ForgedStatusEvidence(CmeTradingStatusEvidence):
        __slots__ = ()

        def verify_unchanged(self) -> None:
            return None

    forged = ForgedStatusEvidence(
        **{field.name: getattr(evidence, field.name) for field in fields(evidence)}
    )
    subclass_result = reference.entry_eligibility(
        "6EZ2",
        event_ts_ns,
        3600,
        status_evidence=forged,
        schedule_archive=schedule,
        allow_test_evidence=True,
    )
    assert not subclass_result.eligible
    assert subclass_result.reason == "STATUS_EVIDENCE_INTERFACE_INVALID"

    original_status_at = CmeTradingStatusEvidence.status_at

    def wrong_identity(
        self: CmeTradingStatusEvidence,
        event_ts_ns: int,
        *,
        venue: str,
        product_root: str,
    ):
        decision = original_status_at(
            self,
            event_ts_ns,
            venue=venue,
            product_root=product_root,
        )
        return replace(decision, evidence_sha256="0" * 64)

    monkeypatch.setattr(CmeTradingStatusEvidence, "status_at", wrong_identity)
    drifted = reference.entry_eligibility(
        "6EZ2",
        event_ts_ns,
        3600,
        status_evidence=evidence,
        schedule_archive=schedule,
        allow_test_evidence=True,
    )
    assert not drifted.eligible
    assert drifted.reason == "STATUS_EVIDENCE_IDENTITY_INVALID"


def test_unavailable_and_wrong_scope_decisions_fail_closed() -> None:
    assert unavailable_status_decision().reason == "STATUS_EVIDENCE_NOT_SUPPLIED"
    evidence = load_cme_trading_status_evidence(FIXTURE, allow_test_fixture=True)
    wrong_scope = evidence.status_at(_ns(13, 2), venue="CME_GLOBEX", product_root="ES")
    assert not wrong_scope.coverage_verified
    assert wrong_scope.reason == "STATUS_SCOPE_MISMATCH"


def test_status_evidence_rejects_ancestor_symlink_and_protected_path(tmp_path: Path) -> None:
    actual = tmp_path / "actual"
    actual.mkdir()
    copied = actual / "status.toml"
    copied.write_bytes(FIXTURE.read_bytes())
    alias = tmp_path / "alias"
    alias.symlink_to(actual, target_is_directory=True)
    with pytest.raises(CmeStatusEvidenceError, match="symbolic link"):
        load_cme_trading_status_evidence(alias / copied.name, allow_test_fixture=True)

    protected = tmp_path / "forward-status.toml"
    protected.write_bytes(FIXTURE.read_bytes())
    with pytest.raises(CmeStatusEvidenceError, match="search-safe"):
        load_cme_trading_status_evidence(protected, allow_test_fixture=True)


def test_status_evidence_rejects_sequence_and_effective_time_regression(tmp_path: Path) -> None:
    text = FIXTURE.read_text()
    duplicate_sequence = tmp_path / "duplicate-sequence.toml"
    duplicate_sequence.write_text(text.replace("source_sequence = 2", "source_sequence = 1"))
    with pytest.raises(CmeStatusEvidenceError, match="source_sequence"):
        load_cme_trading_status_evidence(duplicate_sequence, allow_test_fixture=True)

    regressed_effective = tmp_path / "regressed-effective.toml"
    regressed_effective.write_text(
        text.replace(
            "effective_ts_ns = 1662040800000000000",
            "effective_ts_ns = 1662037000000000000",
        )
    )
    with pytest.raises(CmeStatusEvidenceError, match="effective timestamps"):
        load_cme_trading_status_evidence(regressed_effective, allow_test_fixture=True)
