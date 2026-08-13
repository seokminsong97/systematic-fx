"""Point-in-time immutable CME schedule-archive evidence."""

from __future__ import annotations

import hashlib
import tomllib
from dataclasses import dataclass, replace
from datetime import date, timedelta
from pathlib import Path


class CmeScheduleEvidenceError(ValueError):
    """Schedule evidence is absent, unsafe, or internally inconsistent."""


@dataclass(frozen=True, slots=True)
class ScheduleBreak:
    start_ts_ns: int
    end_ts_ns: int

    def __post_init__(self) -> None:
        if self.start_ts_ns >= self.end_ts_ns:
            raise CmeScheduleEvidenceError("schedule break must have positive duration")

    def as_dict(self) -> dict[str, int]:
        return {"end_ts_ns": self.end_ts_ns, "start_ts_ns": self.start_ts_ns}


@dataclass(frozen=True, slots=True)
class ArchivedTradingSession:
    trading_date: date
    revision: int
    published_ts_ns: int
    open_ts_ns: int
    close_ts_ns: int
    breaks: tuple[ScheduleBreak, ...]
    schedule_kind: str
    holiday_name: str

    def __post_init__(self) -> None:
        if self.revision <= 0 or self.published_ts_ns < 0:
            raise CmeScheduleEvidenceError("schedule revision and publication must be positive")
        if self.open_ts_ns >= self.close_ts_ns:
            raise CmeScheduleEvidenceError("archived session must have positive duration")
        ordering = tuple((item.start_ts_ns, item.end_ts_ns) for item in self.breaks)
        if ordering != tuple(sorted(ordering)):
            raise CmeScheduleEvidenceError("schedule breaks must be increasing")
        prior_end = self.open_ts_ns
        for item in self.breaks:
            if item.start_ts_ns < prior_end or item.end_ts_ns > self.close_ts_ns:
                raise CmeScheduleEvidenceError(
                    "schedule breaks must be disjoint and contained by the session"
                )
            prior_end = item.end_ts_ns

    def as_dict(self) -> dict[str, object]:
        return {
            "breaks": [item.as_dict() for item in self.breaks],
            "close_ts_ns": self.close_ts_ns,
            "holiday_name": self.holiday_name,
            "open_ts_ns": self.open_ts_ns,
            "published_ts_ns": self.published_ts_ns,
            "revision": self.revision,
            "schedule_kind": self.schedule_kind,
            "trading_date": self.trading_date.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class ScheduleEvidenceDecision:
    coverage_verified: bool
    reason: str
    session: ArchivedTradingSession | None
    archive_sha256: str | None


@dataclass(frozen=True, slots=True)
class ScheduleWindowDecision:
    schedule_verified: bool
    eligible: bool
    reason: str
    session: ArchivedTradingSession | None
    archive_sha256: str | None


@dataclass(frozen=True, slots=True)
class CmeScheduleArchive:
    version: str
    sha256: str
    evidence_kind: str
    venue: str
    product_root: str
    timezone: str
    source_id: str
    source_sha256: str
    covered_start: date
    covered_end_exclusive: date
    sessions: tuple[ArchivedTradingSession, ...]
    archive_path: Path | None = None
    verified_source_path: Path | None = None

    def __post_init__(self) -> None:
        if self.covered_start >= self.covered_end_exclusive:
            raise CmeScheduleEvidenceError("schedule coverage must have positive duration")
        if not self.sessions:
            raise CmeScheduleEvidenceError("schedule archive must contain sessions")
        keys = tuple((item.trading_date, item.revision) for item in self.sessions)
        if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
            raise CmeScheduleEvidenceError(
                "schedule records must have unique increasing date/revision keys"
            )
        grouped: dict[date, list[ArchivedTradingSession]] = {}
        for item in self.sessions:
            if not self.covered_start <= item.trading_date < self.covered_end_exclusive:
                raise CmeScheduleEvidenceError("schedule record is outside declared coverage")
            grouped.setdefault(item.trading_date, []).append(item)
        for records in grouped.values():
            publications = tuple(item.published_ts_ns for item in records)
            if publications != tuple(sorted(publications)) or len(publications) != len(
                set(publications)
            ):
                raise CmeScheduleEvidenceError(
                    "schedule revisions must have increasing unique publication timestamps"
                )
        # Revisions for the same date intentionally overlap; records for
        # different trading dates may never claim simultaneous sessions.
        for index, left in enumerate(self.sessions):
            for right in self.sessions[index + 1 :]:
                if left.trading_date == right.trading_date:
                    continue
                if max(left.open_ts_ns, right.open_ts_ns) < min(
                    left.close_ts_ns, right.close_ts_ns
                ):
                    raise CmeScheduleEvidenceError("distinct archived sessions overlap")

    @property
    def is_test_fixture(self) -> bool:
        return self.evidence_kind == "DETERMINISTIC_TEST_FIXTURE"

    @property
    def source_bytes_verified(self) -> bool:
        return self.archive_path is not None and self.verified_source_path is not None

    def verify_unchanged(self) -> None:
        """Reopen both archived schedule and upstream bytes before authorization."""

        if self.archive_path is None or self.verified_source_path is None:
            raise CmeScheduleEvidenceError("schedule archive upstream bytes are not verified")
        loaded = load_cme_schedule_archive(
            self.archive_path,
            allow_test_fixture=self.is_test_fixture,
        )
        if replace(self, archive_path=None, verified_source_path=None) != replace(
            loaded,
            archive_path=None,
            verified_source_path=None,
        ):
            raise CmeScheduleEvidenceError("schedule archive semantic identity drifted")
        _verify_source_sha256(self.verified_source_path, self.source_sha256)

    def session_as_of(self, trading_date: date, as_of_ts_ns: int) -> ScheduleEvidenceDecision:
        if not self.covered_start <= trading_date < self.covered_end_exclusive:
            return _unavailable("SCHEDULE_OUTSIDE_ARCHIVE_COVERAGE", self.sha256)
        known = tuple(
            item
            for item in self.sessions
            if item.trading_date == trading_date and item.published_ts_ns <= as_of_ts_ns
        )
        if not known:
            return _unavailable("SCHEDULE_NOT_YET_PUBLISHED", self.sha256)
        return ScheduleEvidenceDecision(True, "SCHEDULE_VERIFIED", known[-1], self.sha256)

    def entry_window_as_of(
        self,
        event_ts_ns: int,
        max_hold_seconds: int,
        *,
        as_of_ts_ns: int,
    ) -> ScheduleWindowDecision:
        """Check known close/break boundaries without asserting trading status."""

        if max_hold_seconds <= 0:
            raise CmeScheduleEvidenceError("max_hold_seconds must be positive")
        if as_of_ts_ns != event_ts_ns:
            raise CmeScheduleEvidenceError(
                "entry eligibility must use schedule knowledge exactly as of the event"
            )
        known: list[ArchivedTradingSession] = []
        cursor = self.covered_start
        while cursor < self.covered_end_exclusive:
            decision = self.session_as_of(cursor, as_of_ts_ns)
            if decision.coverage_verified and decision.session is not None:
                known.append(decision.session)
            cursor += timedelta(days=1)
        matches = tuple(item for item in known if item.open_ts_ns <= event_ts_ns < item.close_ts_ns)
        if len(matches) != 1:
            return ScheduleWindowDecision(
                False, False, "OUTSIDE_VERIFIED_SCHEDULE", None, self.sha256
            )
        session = matches[0]
        end_ts_ns = event_ts_ns + max_hold_seconds * 1_000_000_000
        if end_ts_ns > session.close_ts_ns:
            return ScheduleWindowDecision(
                True, False, "CROSSES_SCHEDULED_CLOSE", session, self.sha256
            )
        if any(
            event_ts_ns < item.end_ts_ns and end_ts_ns > item.start_ts_ns for item in session.breaks
        ):
            return ScheduleWindowDecision(
                True, False, "CROSSES_SCHEDULED_BREAK", session, self.sha256
            )
        return ScheduleWindowDecision(True, True, "SCHEDULE_WINDOW_VERIFIED", session, self.sha256)

    def previous_completed_session_as_of(
        self,
        trading_date: date,
        *,
        as_of_ts_ns: int,
    ) -> ArchivedTradingSession:
        """Resolve the prior completed session from archived revisions.

        Weekday subtraction is intentionally insufficient: holidays and
        schedule revisions change which session was last complete at the
        target open, and that fact must come from already-published bytes.
        """

        target = self.session_as_of(trading_date, as_of_ts_ns)
        if not target.coverage_verified or target.session is None:
            raise CmeScheduleEvidenceError("target session was not known as of selection time")
        candidates: list[ArchivedTradingSession] = []
        cursor = self.covered_start
        while cursor < trading_date:
            decision = self.session_as_of(cursor, as_of_ts_ns)
            if (
                decision.coverage_verified
                and decision.session is not None
                and decision.session.close_ts_ns <= as_of_ts_ns
                and decision.session.close_ts_ns <= target.session.open_ts_ns
            ):
                candidates.append(decision.session)
            cursor += timedelta(days=1)
        if not candidates:
            raise CmeScheduleEvidenceError(
                "previous completed session is outside schedule archive coverage"
            )
        return max(candidates, key=lambda item: (item.close_ts_ns, item.trading_date))

    def previous_completed_trading_date_as_of(
        self,
        trading_date: date,
        *,
        as_of_ts_ns: int,
    ) -> date:
        """Return the date of :meth:`previous_completed_session_as_of`."""

        return self.previous_completed_session_as_of(
            trading_date,
            as_of_ts_ns=as_of_ts_ns,
        ).trading_date


def _unavailable(reason: str, sha256: str | None = None) -> ScheduleEvidenceDecision:
    return ScheduleEvidenceDecision(False, reason, None, sha256)


def unavailable_schedule_decision() -> ScheduleEvidenceDecision:
    return _unavailable("SCHEDULE_ARCHIVE_NOT_SUPPLIED")


def verify_schedule_upstream_source(
    archive: CmeScheduleArchive,
    source_path: str | Path,
) -> CmeScheduleArchive:
    """Verify the separately archived upstream bytes named by the schedule."""

    requested = Path(source_path).expanduser()
    _reject_unsafe_path(requested)
    if not requested.is_file():
        raise CmeScheduleEvidenceError("schedule upstream source must be a regular file")
    resolved = requested.resolve(strict=True)
    _verify_source_sha256(resolved, archive.source_sha256)
    if archive.archive_path is None:
        raise CmeScheduleEvidenceError("schedule archive did not retain its source path")
    verified = replace(archive, verified_source_path=resolved)
    verified.verify_unchanged()
    return verified


def _verify_source_sha256(path: Path, expected_sha256: str) -> None:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    if digest.hexdigest() != expected_sha256:
        raise CmeScheduleEvidenceError("schedule upstream source SHA-256 drifted")


def _require_exact_keys(value: object, expected: set[str], *, label: str) -> None:
    if not isinstance(value, dict) or set(value) != expected:
        raise CmeScheduleEvidenceError(f"{label} keys differ from the frozen schema")


def _reject_unsafe_path(path: Path) -> None:
    unsafe = ("holdout", "sealed", "credential", "forward")
    if ".." in path.parts or any(
        any(token in part.casefold() for token in unsafe) for part in path.parts
    ):
        raise CmeScheduleEvidenceError("schedule archive path is not search-safe")
    absolute = path if path.is_absolute() else Path.cwd() / path
    cursor = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        cursor /= part
        if cursor.is_symlink():
            raise CmeScheduleEvidenceError("schedule archive cannot traverse a symbolic link")


def load_cme_schedule_archive(
    path: str | Path,
    *,
    allow_test_fixture: bool = False,
) -> CmeScheduleArchive:
    """Load immutable schedule evidence; test fixtures require explicit opt-in."""

    requested = Path(path).expanduser()
    _reject_unsafe_path(requested)
    if not requested.is_file():
        raise CmeScheduleEvidenceError("schedule archive must be a regular file")
    raw = requested.read_bytes()
    try:
        document = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise CmeScheduleEvidenceError("schedule archive must be valid UTF-8 TOML") from error
    _require_exact_keys(document, {"archive", "sessions"}, label="schedule document")
    head = document["archive"]
    _require_exact_keys(
        head,
        {
            "schema",
            "version",
            "evidence_kind",
            "venue",
            "product_root",
            "timezone",
            "source_id",
            "source_sha256",
            "covered_start",
            "covered_end_exclusive",
        },
        label="schedule archive",
    )
    if head["schema"] != "systematic_fx.cme_schedule_archive.v1":
        raise CmeScheduleEvidenceError("unsupported CME schedule archive schema")
    kind = str(head["evidence_kind"])
    if kind not in {"CME_SCHEDULE_ARCHIVE", "DETERMINISTIC_TEST_FIXTURE"}:
        raise CmeScheduleEvidenceError("unsupported CME schedule evidence kind")
    if kind == "DETERMINISTIC_TEST_FIXTURE" and not allow_test_fixture:
        raise CmeScheduleEvidenceError("test schedule requires explicit test-only opt-in")
    if head["venue"] != "CME_GLOBEX" or head["product_root"] != "6E":
        raise CmeScheduleEvidenceError("schedule archive is not CME Globex 6E")
    if not str(head["version"]) or not str(head["source_id"]):
        raise CmeScheduleEvidenceError("schedule archive requires version and source identity")
    source_sha256 = str(head["source_sha256"])
    if len(source_sha256) != 64 or any(c not in "0123456789abcdef" for c in source_sha256):
        raise CmeScheduleEvidenceError("schedule upstream source SHA-256 is invalid")
    sessions: list[ArchivedTradingSession] = []
    for item in document["sessions"]:
        _require_exact_keys(
            item,
            {
                "trading_date",
                "revision",
                "published_ts_ns",
                "open_ts_ns",
                "close_ts_ns",
                "breaks",
                "schedule_kind",
                "holiday_name",
            },
            label="schedule record",
        )
        breaks: list[ScheduleBreak] = []
        for value in item["breaks"]:
            _require_exact_keys(value, {"start_ts_ns", "end_ts_ns"}, label="schedule break")
            breaks.append(ScheduleBreak(int(value["start_ts_ns"]), int(value["end_ts_ns"])))
        sessions.append(
            ArchivedTradingSession(
                trading_date=item["trading_date"],
                revision=int(item["revision"]),
                published_ts_ns=int(item["published_ts_ns"]),
                open_ts_ns=int(item["open_ts_ns"]),
                close_ts_ns=int(item["close_ts_ns"]),
                breaks=tuple(breaks),
                schedule_kind=str(item["schedule_kind"]),
                holiday_name=str(item["holiday_name"]),
            )
        )
    return CmeScheduleArchive(
        version=str(head["version"]),
        sha256=hashlib.sha256(raw).hexdigest(),
        evidence_kind=kind,
        venue=str(head["venue"]),
        product_root=str(head["product_root"]),
        timezone=str(head["timezone"]),
        source_id=str(head["source_id"]),
        source_sha256=source_sha256,
        covered_start=head["covered_start"],
        covered_end_exclusive=head["covered_end_exclusive"],
        sessions=tuple(sessions),
        archive_path=requested.resolve(strict=True),
    )
