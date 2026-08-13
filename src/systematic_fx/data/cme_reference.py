"""Immutable CME 6E scheduled-session and delivery-risk reference.

This module describes only scheduled Globex availability.  It deliberately
does not infer unscheduled halts or status from absent MBP-10 rows.
"""

from __future__ import annotations

import hashlib
import re
import tomllib
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from systematic_fx.data.cme_schedule import CmeScheduleArchive, CmeScheduleEvidenceError
from systematic_fx.data.cme_status import CmeStatusEvidenceError, CmeTradingStatusEvidence

_NS = 1_000_000_000
_SYMBOL = re.compile(r"^6E[FGHJKMNQUVXZ][0-9]{1,2}$")


class CmeReferenceError(ValueError):
    """The reference cannot prove a requested scheduled-market fact."""


@dataclass(frozen=True, slots=True)
class TimeInterval:
    start_ts_ns: int
    end_ts_ns: int

    def __post_init__(self) -> None:
        if self.start_ts_ns >= self.end_ts_ns:
            raise CmeReferenceError("time interval must have positive duration")

    def as_dict(self) -> dict[str, int]:
        return {"start_ts_ns": self.start_ts_ns, "end_ts_ns": self.end_ts_ns}


@dataclass(frozen=True, slots=True)
class TradingSession:
    session_id: str
    trading_date: date
    open_ts_ns: int
    close_ts_ns: int
    breaks: tuple[TimeInterval, ...]
    holiday_name: str | None
    schedule_kind: str
    source_id: str
    status_coverage: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "breaks": [item.as_dict() for item in self.breaks],
            "close_ts_ns": self.close_ts_ns,
            "holiday_name": self.holiday_name,
            "open_ts_ns": self.open_ts_ns,
            "schedule_kind": self.schedule_kind,
            "session_id": self.session_id,
            "source_id": self.source_id,
            "status_coverage": self.status_coverage,
            "trading_date": self.trading_date.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class ContractMetadata:
    raw_symbol: str
    contract_month: date
    tick_size_numerator: int
    tick_size_denominator: int
    contract_multiplier: int
    currency: str
    settlement: str
    activation_date: date
    last_trade_ts_ns: int
    delivery_date: date
    roll_guard_start_ts_ns: int
    source_ids: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "activation_date": self.activation_date.isoformat(),
            "contract_month": self.contract_month.isoformat(),
            "contract_multiplier": self.contract_multiplier,
            "currency": self.currency,
            "delivery_date": self.delivery_date.isoformat(),
            "last_trade_ts_ns": self.last_trade_ts_ns,
            "raw_symbol": self.raw_symbol,
            "roll_guard_start_ts_ns": self.roll_guard_start_ts_ns,
            "settlement": self.settlement,
            "source_ids": list(self.source_ids),
            "tick_size_denominator": self.tick_size_denominator,
            "tick_size_numerator": self.tick_size_numerator,
        }


@dataclass(frozen=True, slots=True)
class ScheduledEntryEligibility:
    eligible: bool
    reason: str
    session_id: str | None
    trading_date: date | None
    next_scheduled_close_ts_ns: int | None
    status_coverage: bool
    status_evidence_sha256: str | None = None
    status_observed_ts_ns: int | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "eligible": self.eligible,
            "next_scheduled_close_ts_ns": self.next_scheduled_close_ts_ns,
            "reason": self.reason,
            "session_id": self.session_id,
            "status_coverage": self.status_coverage,
            "status_evidence_sha256": self.status_evidence_sha256,
            "status_observed_ts_ns": self.status_observed_ts_ns,
            "trading_date": self.trading_date.isoformat() if self.trading_date else None,
        }


@dataclass(frozen=True, slots=True)
class ActiveContractCandidate:
    instrument_id: int
    raw_symbol: str
    previous_trade_volume: int

    def as_dict(self) -> dict[str, object]:
        return {
            "instrument_id": self.instrument_id,
            "previous_trade_volume": self.previous_trade_volume,
            "raw_symbol": self.raw_symbol,
        }


@dataclass(frozen=True, slots=True)
class ActiveContractSelection:
    trading_date: date
    evidence_date: date
    selected: ActiveContractCandidate
    candidates: tuple[ActiveContractCandidate, ...]
    policy_version: str = "previous_completed_trading_date_volume_v1"

    def as_dict(self) -> dict[str, object]:
        return {
            "candidates": [item.as_dict() for item in self.candidates],
            "evidence_date": self.evidence_date.isoformat(),
            "policy_version": self.policy_version,
            "selected": self.selected.as_dict(),
            "trading_date": self.trading_date.isoformat(),
        }


def _ns(value: datetime) -> int:
    return int(value.timestamp()) * _NS


def _parse_clock(value: str) -> time:
    try:
        return time.fromisoformat(value)
    except ValueError as error:
        raise CmeReferenceError(f"invalid local clock: {value!r}") from error


@dataclass(frozen=True, slots=True)
class Cme6EReference:
    version: str
    sha256: str
    covered_start: date
    covered_end_exclusive: date
    timezone: str
    status_coverage: bool
    regular_open_local: time
    regular_close_local: time
    roll_guard_business_days: int
    contracts: tuple[ContractMetadata, ...]
    source_ids: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "contracts": [item.as_dict() for item in self.contracts],
            "covered_end_exclusive": self.covered_end_exclusive.isoformat(),
            "covered_start": self.covered_start.isoformat(),
            "roll_guard_business_days": self.roll_guard_business_days,
            "sha256": self.sha256,
            "source_ids": list(self.source_ids),
            "status_coverage": self.status_coverage,
            "timezone": self.timezone,
            "version": self.version,
        }

    def previous_scheduled_trading_date(self, trading_date: date) -> date:
        """Previous weekday inside v1's holiday-free bounded coverage."""

        candidate = trading_date - timedelta(days=1)
        while candidate.weekday() >= 5:
            candidate -= timedelta(days=1)
        if candidate < self.covered_start:
            raise CmeReferenceError("previous trading date is outside frozen coverage")
        return candidate

    def session_for(self, trading_date: date) -> TradingSession:
        if not self.covered_start <= trading_date < self.covered_end_exclusive:
            raise CmeReferenceError("trading date is outside frozen calendar coverage")
        if trading_date.weekday() >= 5:
            raise CmeReferenceError("weekend has no CME 6E trading-date session")
        zone = ZoneInfo(self.timezone)
        open_day = trading_date - timedelta(days=1)
        opened = datetime.combine(open_day, self.regular_open_local, zone)
        closed = datetime.combine(trading_date, self.regular_close_local, zone)
        return TradingSession(
            session_id=f"CME_GLOBEX_6E:{trading_date.isoformat()}",
            trading_date=trading_date,
            open_ts_ns=_ns(opened.astimezone(UTC)),
            close_ts_ns=_ns(closed.astimezone(UTC)),
            breaks=(),
            holiday_name=None,
            schedule_kind="REGULAR",
            source_id=self.source_ids[1],
            status_coverage=self.status_coverage,
        )

    def contract(self, raw_symbol: str, *, as_of_date: date) -> ContractMetadata:
        if not _SYMBOL.fullmatch(raw_symbol):
            raise CmeReferenceError("not a 6E outright symbol")
        for item in self.contracts:
            if item.raw_symbol == raw_symbol and item.activation_date <= as_of_date:
                return item
        raise CmeReferenceError("contract is absent or not yet active in frozen reference")

    def scheduled_entry_eligibility(
        self,
        raw_symbol: str,
        event_ts_ns: int,
        max_hold_seconds: int,
        *,
        schedule_archive: CmeScheduleArchive | None = None,
    ) -> ScheduledEntryEligibility:
        if max_hold_seconds <= 0:
            raise CmeReferenceError("max_hold_seconds must be positive")
        if schedule_archive is not None:
            if (
                not isinstance(schedule_archive, CmeScheduleArchive)
                or schedule_archive.venue != "CME_GLOBEX"
                or schedule_archive.product_root != "6E"
            ):
                raise CmeReferenceError("schedule archive is not canonical CME Globex 6E")
            window = schedule_archive.entry_window_as_of(
                event_ts_ns,
                max_hold_seconds,
                as_of_ts_ns=event_ts_ns,
            )
            if window.session is None:
                return ScheduledEntryEligibility(False, window.reason, None, None, None, False)
            archived_session = window.session
            contract = self.contract(raw_symbol, as_of_date=archived_session.trading_date)
            end_ts = event_ts_ns + max_hold_seconds * _NS
            if event_ts_ns >= contract.roll_guard_start_ts_ns:
                reason = "ROLL_GUARD"
            elif end_ts > contract.roll_guard_start_ts_ns:
                reason = "CROSSES_ROLL_GUARD"
            elif not window.eligible:
                reason = window.reason
            else:
                reason = "SCHEDULE_ONLY_STATUS_UNVERIFIED"
            return ScheduledEntryEligibility(
                eligible=False,
                reason=reason,
                session_id=f"CME_GLOBEX_6E:{archived_session.trading_date.isoformat()}",
                trading_date=archived_session.trading_date,
                next_scheduled_close_ts_ns=archived_session.close_ts_ns,
                status_coverage=False,
            )
        matching: TradingSession | None = None
        day = self.covered_start
        while day < self.covered_end_exclusive:
            if day.weekday() < 5:
                session = self.session_for(day)
                if session.open_ts_ns <= event_ts_ns < session.close_ts_ns:
                    matching = session
                    break
            day += timedelta(days=1)
        if matching is None:
            return ScheduledEntryEligibility(
                False, "OUTSIDE_SCHEDULED_SESSION", None, None, None, False
            )
        contract = self.contract(raw_symbol, as_of_date=matching.trading_date)
        end_ts = event_ts_ns + max_hold_seconds * _NS
        if event_ts_ns >= contract.roll_guard_start_ts_ns:
            reason = "ROLL_GUARD"
        elif end_ts > matching.close_ts_ns:
            reason = "CROSSES_SCHEDULED_CLOSE"
        elif end_ts > contract.roll_guard_start_ts_ns:
            reason = "CROSSES_ROLL_GUARD"
        else:
            reason = "SCHEDULE_ONLY_STATUS_UNVERIFIED"
        return ScheduledEntryEligibility(
            eligible=False,
            reason=reason,
            session_id=matching.session_id,
            trading_date=matching.trading_date,
            next_scheduled_close_ts_ns=matching.close_ts_ns,
            status_coverage=False,
        )

    def entry_eligibility(
        self,
        raw_symbol: str,
        event_ts_ns: int,
        max_hold_seconds: int,
        *,
        status_evidence: CmeTradingStatusEvidence | None = None,
        schedule_archive: CmeScheduleArchive | None = None,
        allow_test_evidence: bool = False,
    ) -> ScheduledEntryEligibility:
        """Combine the immutable schedule with point-in-time status evidence.

        No status provider means no eligible entry.  A verified OPEN snapshot
        proves only entry-time operability; future halt transitions must be
        consumed by execution replay and are never anticipated here.
        """

        scheduled = self.scheduled_entry_eligibility(
            raw_symbol,
            event_ts_ns,
            max_hold_seconds,
            schedule_archive=schedule_archive,
        )
        if schedule_archive is None:
            return ScheduledEntryEligibility(
                False,
                "SCHEDULE_ARCHIVE_REQUIRED_FOR_ENTRY",
                scheduled.session_id,
                scheduled.trading_date,
                scheduled.next_scheduled_close_ts_ns,
                False,
            )
        try:
            schedule_archive.verify_unchanged()
        except (CmeScheduleEvidenceError, OSError):
            return ScheduledEntryEligibility(
                False,
                "SCHEDULE_ARCHIVE_IDENTITY_UNVERIFIED",
                scheduled.session_id,
                scheduled.trading_date,
                scheduled.next_scheduled_close_ts_ns,
                False,
            )
        if schedule_archive.is_test_fixture and not allow_test_evidence:
            return ScheduledEntryEligibility(
                False,
                "SCHEDULE_TEST_FIXTURE_NOT_ADMISSIBLE",
                scheduled.session_id,
                scheduled.trading_date,
                scheduled.next_scheduled_close_ts_ns,
                False,
            )
        if scheduled.reason != "SCHEDULE_ONLY_STATUS_UNVERIFIED":
            return scheduled
        if status_evidence is None:
            return scheduled
        if type(status_evidence) is not CmeTradingStatusEvidence:
            return ScheduledEntryEligibility(
                False,
                "STATUS_EVIDENCE_INTERFACE_INVALID",
                scheduled.session_id,
                scheduled.trading_date,
                scheduled.next_scheduled_close_ts_ns,
                False,
            )
        try:
            status_evidence.verify_unchanged()
        except (CmeStatusEvidenceError, OSError):
            return ScheduledEntryEligibility(
                False,
                "STATUS_EVIDENCE_IDENTITY_UNVERIFIED",
                scheduled.session_id,
                scheduled.trading_date,
                scheduled.next_scheduled_close_ts_ns,
                False,
            )
        if status_evidence.is_test_fixture and not allow_test_evidence:
            return ScheduledEntryEligibility(
                False,
                "STATUS_TEST_FIXTURE_NOT_ADMISSIBLE",
                scheduled.session_id,
                scheduled.trading_date,
                scheduled.next_scheduled_close_ts_ns,
                False,
            )
        status_at = getattr(status_evidence, "status_at", None)
        if not callable(status_at):
            return ScheduledEntryEligibility(
                False,
                "STATUS_EVIDENCE_INTERFACE_INVALID",
                scheduled.session_id,
                scheduled.trading_date,
                scheduled.next_scheduled_close_ts_ns,
                False,
            )
        try:
            decision = status_at(
                event_ts_ns,
                venue="CME_GLOBEX",
                product_root="6E",
            )
        except (OSError, RuntimeError, TypeError, ValueError):
            return ScheduledEntryEligibility(
                False,
                "STATUS_EVIDENCE_UNAVAILABLE",
                scheduled.session_id,
                scheduled.trading_date,
                scheduled.next_scheduled_close_ts_ns,
                False,
            )
        covered = getattr(decision, "coverage_verified", None) is True
        is_open = getattr(decision, "is_open", None) is True
        reason = str(getattr(decision, "reason", "STATUS_EVIDENCE_UNVERIFIED"))
        evidence_sha256 = getattr(decision, "evidence_sha256", None)
        observed_ts_ns = getattr(decision, "observed_ts_ns", None)
        status = getattr(decision, "status", None)
        status_value = getattr(status, "value", status)
        status_valid = status_value in {"OPEN", "HALTED", "CLOSED", "UNKNOWN"}
        identity_valid = (
            isinstance(evidence_sha256, str)
            and evidence_sha256 == status_evidence.sha256
            and len(evidence_sha256) == 64
            and all(character in "0123456789abcdef" for character in evidence_sha256)
            and isinstance(observed_ts_ns, int)
            and not isinstance(observed_ts_ns, bool)
            and observed_ts_ns <= event_ts_ns
            and status_valid
        )
        if covered and (not identity_valid or is_open != (status_value == "OPEN")):
            return ScheduledEntryEligibility(
                False,
                "STATUS_EVIDENCE_IDENTITY_INVALID",
                scheduled.session_id,
                scheduled.trading_date,
                scheduled.next_scheduled_close_ts_ns,
                False,
            )
        if not covered or not is_open:
            return ScheduledEntryEligibility(
                False,
                reason,
                scheduled.session_id,
                scheduled.trading_date,
                scheduled.next_scheduled_close_ts_ns,
                covered,
                evidence_sha256,
                observed_ts_ns,
            )
        return ScheduledEntryEligibility(
            True,
            "STATUS_OPEN_VERIFIED",
            scheduled.session_id,
            scheduled.trading_date,
            scheduled.next_scheduled_close_ts_ns,
            True,
            evidence_sha256,
            observed_ts_ns,
        )


def select_active_contract_for_trading_date(
    reference: Cme6EReference,
    *,
    trading_date: date,
    evidence_date: date,
    candidates: tuple[ActiveContractCandidate, ...],
) -> ActiveContractSelection:
    """Rank only prior-completed-trading-date volume; never same-day rows."""

    reference.session_for(trading_date)
    if evidence_date != reference.previous_scheduled_trading_date(trading_date):
        raise CmeReferenceError(
            "active-contract volume evidence must be the exact previous scheduled trading date"
        )
    if not candidates:
        raise CmeReferenceError("active-contract candidates must not be empty")
    if len({item.instrument_id for item in candidates}) != len(candidates):
        raise CmeReferenceError("candidate instrument IDs must be unique")
    resolved_contracts: dict[int, ContractMetadata] = {}
    for item in candidates:
        if item.instrument_id <= 0 or item.previous_trade_volume < 0:
            raise CmeReferenceError("invalid active-contract candidate")
        contract = reference.contract(item.raw_symbol, as_of_date=trading_date)
        resolved_contracts[item.instrument_id] = contract
        # Selection is a point-in-time market fact, not entry authorization.
        # The winning contract may already be inside its delivery roll guard;
        # scheduled_entry_eligibility must then reject a new position while the
        # mapping remains an honest record of the previous session's volume.
    ranked = tuple(
        sorted(
            candidates,
            key=lambda item: (
                -item.previous_trade_volume,
                resolved_contracts[item.instrument_id].contract_month,
                item.instrument_id,
                item.raw_symbol,
            ),
        )
    )
    return ActiveContractSelection(trading_date, evidence_date, ranked[0], ranked)


def load_cme_6e_reference(path: Path | str) -> Cme6EReference:
    requested = Path(path).expanduser()
    if requested.is_symlink() or not requested.is_file():
        raise CmeReferenceError("reference must be a regular non-symlink file")
    content = requested.read_bytes()
    try:
        data = tomllib.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise CmeReferenceError("invalid CME reference TOML") from error
    ref, product = data["reference"], data["product"]
    source_ids = tuple(item["source_id"] for item in data["sources"])
    if ref["schema"] != "systematic_fx.cme_6e_reference.v1":
        raise CmeReferenceError("unsupported CME reference schema")
    if not ref["scheduled_hours_only"] or ref["status_coverage"]:
        raise CmeReferenceError("v1 must remain schedule-only and status-unverified")
    if product["root"] != "6E" or product["venue"] != "CME_GLOBEX":
        raise CmeReferenceError("reference is not CME Globex 6E")
    last_clock = _parse_clock(product["last_trade_local"])
    contracts: list[ContractMetadata] = []
    for item in data["contracts"]:
        local_last = datetime.combine(
            item["last_trade_date"], last_clock, ZoneInfo(ref["timezone"])
        )
        guard_day = item["last_trade_date"]
        remaining = product["roll_guard_business_days"]
        while remaining:
            guard_day -= timedelta(days=1)
            if guard_day.weekday() < 5:
                remaining -= 1
        guard = datetime.combine(
            guard_day - timedelta(days=1),
            _parse_clock(product["regular_open_local"]),
            ZoneInfo(ref["timezone"]),
        )
        contracts.append(
            ContractMetadata(
                raw_symbol=item["raw_symbol"],
                contract_month=item["contract_month"],
                tick_size_numerator=product["tick_size_numerator"],
                tick_size_denominator=product["tick_size_denominator"],
                contract_multiplier=product["contract_multiplier"],
                currency=product["currency"],
                settlement=product["settlement"],
                activation_date=item["activation_date"],
                last_trade_ts_ns=_ns(local_last.astimezone(UTC)),
                delivery_date=item["delivery_date"],
                roll_guard_start_ts_ns=_ns(guard.astimezone(UTC)),
                source_ids=source_ids,
            )
        )
    return Cme6EReference(
        version=ref["version"],
        sha256=hashlib.sha256(content).hexdigest(),
        covered_start=ref["covered_start"],
        covered_end_exclusive=ref["covered_end_exclusive"],
        timezone=ref["timezone"],
        status_coverage=ref["status_coverage"],
        regular_open_local=_parse_clock(product["regular_open_local"]),
        regular_close_local=_parse_clock(product["regular_close_local"]),
        roll_guard_business_days=product["roll_guard_business_days"],
        contracts=tuple(contracts),
        source_ids=source_ids,
    )
