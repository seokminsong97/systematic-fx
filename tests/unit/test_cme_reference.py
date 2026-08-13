from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from systematic_fx.data.cme_reference import (
    ActiveContractCandidate,
    CmeReferenceError,
    load_cme_6e_reference,
    select_active_contract_for_trading_date,
)
from systematic_fx.data.cme_schedule import (
    load_cme_schedule_archive,
    verify_schedule_upstream_source,
)

REFERENCE = Path("configs/data/cme_6e_reference_v1.toml")
SCHEDULE_FIXTURE = Path("tests/fixtures/cme_schedule_archive_fixture_v1.toml")
SCHEDULE_SOURCE = Path("tests/fixtures/cme_schedule_upstream_fixture_v1.txt")


def _ns(value: datetime) -> int:
    return int(value.timestamp()) * 1_000_000_000


def _verified_schedule():
    return verify_schedule_upstream_source(
        load_cme_schedule_archive(SCHEDULE_FIXTURE, allow_test_fixture=True),
        SCHEDULE_SOURCE,
    )


def test_trade_date_session_uses_chicago_dst_and_spans_utc_source_dates() -> None:
    reference = load_cme_6e_reference(REFERENCE)
    session = reference.session_for(date(2022, 9, 1))
    assert session.open_ts_ns == _ns(datetime(2022, 8, 31, 22, tzinfo=UTC))
    assert session.close_ts_ns == _ns(datetime(2022, 9, 1, 21, tzinfo=UTC))
    assert session.session_id == "CME_GLOBEX_6E:2022-09-01"
    assert not session.status_coverage


def test_weekend_and_uncovered_dates_fail_closed() -> None:
    reference = load_cme_6e_reference(REFERENCE)
    with pytest.raises(CmeReferenceError, match="weekend"):
        reference.session_for(date(2022, 9, 3))
    with pytest.raises(CmeReferenceError, match="outside"):
        reference.session_for(date(2022, 9, 5))


def test_contract_terms_expiry_and_delivery_guard_are_frozen() -> None:
    reference = load_cme_6e_reference(REFERENCE)
    contract = reference.contract("6EU2", as_of_date=date(2022, 8, 31))
    assert (contract.tick_size_numerator, contract.tick_size_denominator) == (1, 20_000)
    assert contract.contract_multiplier == 125_000
    assert contract.delivery_date == date(2022, 9, 21)
    assert contract.last_trade_ts_ns == _ns(datetime(2022, 9, 19, 14, 16, tzinfo=UTC))
    assert contract.roll_guard_start_ts_ns == _ns(datetime(2022, 9, 11, 22, tzinfo=UTC))
    assert contract.roll_guard_start_ts_ns < contract.last_trade_ts_ns


def test_schedule_only_reference_never_claims_full_entry_eligibility() -> None:
    reference = load_cme_6e_reference(REFERENCE)
    result = reference.scheduled_entry_eligibility(
        "6EZ2", _ns(datetime(2022, 9, 1, 13, tzinfo=UTC)), 3600
    )
    assert not result.eligible
    assert result.reason == "SCHEDULE_ONLY_STATUS_UNVERIFIED"
    assert not result.status_coverage


def test_hold_may_not_cross_scheduled_close() -> None:
    reference = load_cme_6e_reference(REFERENCE)
    result = reference.scheduled_entry_eligibility(
        "6EZ2", _ns(datetime(2022, 9, 2, 20, 30, tzinfo=UTC)), 3600
    )
    assert not result.eligible
    assert result.reason == "CROSSES_SCHEDULED_CLOSE"


def test_archived_schedule_revision_overrides_recurring_hours_point_in_time() -> None:
    reference = load_cme_6e_reference(REFERENCE)
    archive = load_cme_schedule_archive(SCHEDULE_FIXTURE, allow_test_fixture=True)
    event_ts_ns = _ns(datetime(2022, 9, 1, 19, 30, tzinfo=UTC))
    recurring = reference.scheduled_entry_eligibility("6EZ2", event_ts_ns, 3600)
    archived = reference.scheduled_entry_eligibility(
        "6EZ2",
        event_ts_ns,
        3600,
        schedule_archive=archive,
    )
    assert recurring.reason == "SCHEDULE_ONLY_STATUS_UNVERIFIED"
    assert archived.reason == "CROSSES_SCHEDULED_CLOSE"
    assert archived.next_scheduled_close_ts_ns == _ns(datetime(2022, 9, 1, 20, tzinfo=UTC))


def test_active_selection_uses_only_prior_date_volume_and_is_deterministic() -> None:
    reference = load_cme_6e_reference(REFERENCE)
    result = select_active_contract_for_trading_date(
        reference,
        trading_date=date(2022, 9, 1),
        evidence_date=date(2022, 8, 31),
        candidates=(
            ActiveContractCandidate(11, "6EU2", 90),
            ActiveContractCandidate(22, "6EZ2", 170),
        ),
    )
    assert result.selected.raw_symbol == "6EZ2"
    assert result.evidence_date < result.trading_date
    with pytest.raises(CmeReferenceError, match="exact previous"):
        select_active_contract_for_trading_date(
            reference,
            trading_date=date(2022, 9, 1),
            evidence_date=date(2022, 9, 1),
            candidates=(ActiveContractCandidate(22, "6EZ2", 999),),
        )


def test_selection_rejects_stale_evidence_and_serializes_lineage() -> None:
    reference = load_cme_6e_reference(REFERENCE)
    with pytest.raises(CmeReferenceError, match="exact previous"):
        select_active_contract_for_trading_date(
            reference,
            trading_date=date(2022, 9, 2),
            evidence_date=date(2022, 8, 31),
            candidates=(ActiveContractCandidate(22, "6EZ2", 10),),
        )
    assert reference.as_dict()["status_coverage"] is False
    assert reference.session_for(date(2022, 9, 1)).as_dict()["trading_date"] == "2022-09-01"


def test_volume_winner_is_recorded_even_when_roll_guard_blocks_entry() -> None:
    bounded = load_cme_6e_reference(REFERENCE)
    reference = replace(bounded, covered_end_exclusive=date(2022, 9, 20))
    selection = select_active_contract_for_trading_date(
        reference,
        trading_date=date(2022, 9, 16),
        evidence_date=date(2022, 9, 15),
        candidates=(
            ActiveContractCandidate(44_629, "6EU2", 158_500),
            ActiveContractCandidate(191_026, "6EZ2", 62_531),
        ),
    )
    assert selection.selected.raw_symbol == "6EU2"
    event_ts_ns = _ns(datetime(2022, 9, 16, 13, tzinfo=UTC))
    entry = reference.scheduled_entry_eligibility("6EU2", event_ts_ns, 3600)
    assert not entry.eligible
    assert entry.reason == "ROLL_GUARD"


def test_previous_friday_volume_selects_z2_before_monday_opens() -> None:
    bounded = load_cme_6e_reference(REFERENCE)
    reference = replace(bounded, covered_end_exclusive=date(2022, 9, 20))
    selection = select_active_contract_for_trading_date(
        reference,
        trading_date=date(2022, 9, 19),
        evidence_date=date(2022, 9, 16),
        candidates=(
            ActiveContractCandidate(44_629, "6EU2", 24_706),
            ActiveContractCandidate(191_026, "6EZ2", 224_580),
        ),
    )
    assert selection.selected.raw_symbol == "6EZ2"


def test_reference_identity_is_exact_file_sha256() -> None:
    import hashlib

    reference = load_cme_6e_reference(REFERENCE)
    assert reference.sha256 == hashlib.sha256(REFERENCE.read_bytes()).hexdigest()


def test_entry_eligibility_rejects_forged_status_provider() -> None:
    reference = load_cme_6e_reference(REFERENCE)
    event_ts_ns = _ns(datetime(2022, 9, 1, 13, tzinfo=UTC))

    class ForgedProvider:
        def status_at(self, event_ts_ns: int, *, venue: str, product_root: str):
            del venue, product_root
            return type(
                "Decision",
                (),
                {
                    "coverage_verified": True,
                    "is_open": True,
                    "reason": "STATUS_OPEN_VERIFIED",
                    "status": "OPEN",
                    "evidence_sha256": "not-a-hash",
                    "observed_ts_ns": event_ts_ns + 1,
                },
            )()

    decision = reference.entry_eligibility(
        "6EZ2",
        event_ts_ns,
        3600,
        status_evidence=ForgedProvider(),
        schedule_archive=_verified_schedule(),
        allow_test_evidence=True,
    )
    assert not decision.eligible
    assert decision.reason == "STATUS_EVIDENCE_INTERFACE_INVALID"


def test_entry_eligibility_rejects_structurally_plausible_duck_provider() -> None:
    reference = load_cme_6e_reference(REFERENCE)
    event_ts_ns = _ns(datetime(2022, 9, 1, 13, tzinfo=UTC))

    class PlausibleButUnauthenticatedProvider:
        def status_at(self, event_ts_ns: int, *, venue: str, product_root: str):
            del venue, product_root
            return type(
                "Decision",
                (),
                {
                    "coverage_verified": True,
                    "is_open": True,
                    "reason": "STATUS_OPEN_VERIFIED",
                    "status": "OPEN",
                    "evidence_sha256": "a" * 64,
                    "observed_ts_ns": event_ts_ns,
                },
            )()

    decision = reference.entry_eligibility(
        "6EZ2",
        event_ts_ns,
        3600,
        status_evidence=PlausibleButUnauthenticatedProvider(),  # type: ignore[arg-type]
        schedule_archive=_verified_schedule(),
        allow_test_evidence=True,
    )
    assert not decision.eligible
    assert decision.reason == "STATUS_EVIDENCE_INTERFACE_INVALID"


def test_entry_eligibility_rejects_forged_covered_closed_identity() -> None:
    reference = load_cme_6e_reference(REFERENCE)
    event_ts_ns = _ns(datetime(2022, 9, 1, 13, tzinfo=UTC))

    class ForgedClosedProvider:
        def status_at(self, event_ts_ns: int, *, venue: str, product_root: str):
            del venue, product_root
            return type(
                "Decision",
                (),
                {
                    "coverage_verified": True,
                    "is_open": False,
                    "reason": "STATUS_CLOSED",
                    "status": "CLOSED",
                    "evidence_sha256": "not-a-hash",
                    "observed_ts_ns": event_ts_ns + 1,
                },
            )()

    decision = reference.entry_eligibility(
        "6EZ2",
        event_ts_ns,
        3600,
        status_evidence=ForgedClosedProvider(),
        schedule_archive=_verified_schedule(),
        allow_test_evidence=True,
    )
    assert not decision.eligible
    assert decision.reason == "STATUS_EVIDENCE_INTERFACE_INVALID"
    assert not decision.status_coverage
