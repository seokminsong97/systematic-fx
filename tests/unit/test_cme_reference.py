from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from systematic_fx.data.cme_reference import (
    ActiveContractCandidate,
    CmeReferenceError,
    load_cme_6e_reference,
    select_active_contract_for_trading_date,
)

REFERENCE = Path("configs/data/cme_6e_reference_v1.toml")


def _ns(value: datetime) -> int:
    return int(value.timestamp()) * 1_000_000_000


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


def test_reference_identity_is_exact_file_sha256() -> None:
    import hashlib

    reference = load_cme_6e_reference(REFERENCE)
    assert reference.sha256 == hashlib.sha256(REFERENCE.read_bytes()).hexdigest()
