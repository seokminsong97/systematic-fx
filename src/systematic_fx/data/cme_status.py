"""Point-in-time CME trading-status evidence.

Scheduled exchange hours are not evidence that a market was operational at a
particular instant.  This module deliberately keeps archived status evidence
separate from the calendar reference and fails closed when a snapshot is
missing, future-published, stale, or outside its declared coverage.
"""

from __future__ import annotations

import hashlib
import tomllib
from dataclasses import dataclass, replace
from enum import Enum
from pathlib import Path
from typing import Protocol


class CmeStatusEvidenceError(ValueError):
    """Status evidence is malformed or cannot prove the requested fact."""


class TradingStatus(str, Enum):
    OPEN = "OPEN"
    HALTED = "HALTED"
    CLOSED = "CLOSED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class TradingStatusObservation:
    effective_ts_ns: int
    observed_ts_ns: int
    source_sequence: int
    status: TradingStatus

    def __post_init__(self) -> None:
        if self.effective_ts_ns < 0 or self.observed_ts_ns < self.effective_ts_ns:
            raise CmeStatusEvidenceError(
                "status observation must be published no earlier than its effective time"
            )
        if self.source_sequence < 0:
            raise CmeStatusEvidenceError("status source_sequence must be non-negative")

    def as_dict(self) -> dict[str, object]:
        return {
            "effective_ts_ns": self.effective_ts_ns,
            "observed_ts_ns": self.observed_ts_ns,
            "source_sequence": self.source_sequence,
            "status": self.status.value,
        }


@dataclass(frozen=True, slots=True)
class TradingStatusDecision:
    coverage_verified: bool
    is_open: bool
    reason: str
    status: TradingStatus | None
    observed_ts_ns: int | None
    evidence_sha256: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "coverage_verified": self.coverage_verified,
            "evidence_sha256": self.evidence_sha256,
            "is_open": self.is_open,
            "observed_ts_ns": self.observed_ts_ns,
            "reason": self.reason,
            "status": None if self.status is None else self.status.value,
        }


class TradingStatusEvidenceProvider(Protocol):
    """Minimal read-only interface accepted by CME entry eligibility."""

    def status_at(
        self,
        event_ts_ns: int,
        *,
        venue: str,
        product_root: str,
    ) -> TradingStatusDecision: ...


@dataclass(frozen=True, slots=True)
class CmeTradingStatusEvidence:
    version: str
    sha256: str
    evidence_kind: str
    venue: str
    product_root: str
    source_id: str
    covered_start_ts_ns: int
    covered_end_ts_ns: int
    maximum_observation_age_seconds: int
    observations: tuple[TradingStatusObservation, ...]
    source_sha256: str
    evidence_path: Path | None = None
    verified_source_path: Path | None = None

    def __post_init__(self) -> None:
        if self.covered_start_ts_ns >= self.covered_end_ts_ns:
            raise CmeStatusEvidenceError("status evidence coverage must have positive duration")
        if self.maximum_observation_age_seconds <= 0:
            raise CmeStatusEvidenceError("status maximum observation age must be positive")
        if not self.observations:
            raise CmeStatusEvidenceError("status evidence must contain observations")
        ordering = tuple(
            (item.observed_ts_ns, item.effective_ts_ns, item.source_sequence)
            for item in self.observations
        )
        if ordering != tuple(sorted(ordering)) or len(ordering) != len(set(ordering)):
            raise CmeStatusEvidenceError(
                "status observations must be unique and canonically increasing"
            )
        source_sequences = tuple(item.source_sequence for item in self.observations)
        effective_times = tuple(item.effective_ts_ns for item in self.observations)
        if source_sequences != tuple(sorted(source_sequences)) or len(source_sequences) != len(
            set(source_sequences)
        ):
            raise CmeStatusEvidenceError(
                "status source_sequence must be unique and strictly increasing"
            )
        if effective_times != tuple(sorted(effective_times)):
            raise CmeStatusEvidenceError(
                "status effective timestamps cannot regress as observations arrive"
            )
        if any(
            not self.covered_start_ts_ns <= item.observed_ts_ns < self.covered_end_ts_ns
            for item in self.observations
        ):
            raise CmeStatusEvidenceError("status observation is outside declared coverage")

    @property
    def is_test_fixture(self) -> bool:
        return self.evidence_kind == "DETERMINISTIC_TEST_FIXTURE"

    @property
    def source_bytes_verified(self) -> bool:
        return self.evidence_path is not None and self.verified_source_path is not None

    def verify_unchanged(self) -> None:
        if self.evidence_path is None or self.verified_source_path is None:
            raise CmeStatusEvidenceError("status upstream bytes are not verified")
        loaded = load_cme_trading_status_evidence(
            self.evidence_path,
            allow_test_fixture=self.is_test_fixture,
        )
        if replace(self, evidence_path=None, verified_source_path=None) != replace(
            loaded,
            evidence_path=None,
            verified_source_path=None,
        ):
            raise CmeStatusEvidenceError("status evidence semantic identity drifted")
        _verify_source_sha256(self.verified_source_path, self.source_sha256)

    def status_at(
        self,
        event_ts_ns: int,
        *,
        venue: str,
        product_root: str,
    ) -> TradingStatusDecision:
        """Return only evidence that was already observable at ``event_ts_ns``.

        An OPEN result proves the entry-time status only.  It does not predict
        future halts; a later status transition belongs in execution replay.
        """

        if venue != self.venue or product_root != self.product_root:
            return _unverified("STATUS_SCOPE_MISMATCH", self.sha256)
        if not self.covered_start_ts_ns <= event_ts_ns < self.covered_end_ts_ns:
            return _unverified("STATUS_OUTSIDE_EVIDENCE_COVERAGE", self.sha256)
        known = tuple(
            item
            for item in self.observations
            if item.effective_ts_ns <= event_ts_ns and item.observed_ts_ns <= event_ts_ns
        )
        if not known:
            return _unverified("STATUS_NOT_YET_OBSERVED", self.sha256)
        latest = max(
            known,
            key=lambda item: (
                item.observed_ts_ns,
                item.effective_ts_ns,
                item.source_sequence,
            ),
        )
        maximum_age_ns = self.maximum_observation_age_seconds * 1_000_000_000
        if event_ts_ns - latest.observed_ts_ns > maximum_age_ns:
            return TradingStatusDecision(
                coverage_verified=False,
                is_open=False,
                reason="STATUS_OBSERVATION_STALE",
                status=latest.status,
                observed_ts_ns=latest.observed_ts_ns,
                evidence_sha256=self.sha256,
            )
        return TradingStatusDecision(
            coverage_verified=True,
            is_open=latest.status is TradingStatus.OPEN,
            reason=(
                "STATUS_OPEN_VERIFIED"
                if latest.status is TradingStatus.OPEN
                else f"STATUS_{latest.status.value}"
            ),
            status=latest.status,
            observed_ts_ns=latest.observed_ts_ns,
            evidence_sha256=self.sha256,
        )


def _unverified(reason: str, sha256: str | None = None) -> TradingStatusDecision:
    return TradingStatusDecision(False, False, reason, None, None, sha256)


def unavailable_status_decision() -> TradingStatusDecision:
    """Canonical fail-closed result when no status source was supplied."""

    return _unverified("STATUS_EVIDENCE_NOT_SUPPLIED")


def _require_exact_keys(value: object, expected: set[str], *, label: str) -> None:
    if not isinstance(value, dict) or set(value) != expected:
        raise CmeStatusEvidenceError(f"{label} keys differ from the frozen schema")


def _reject_unsafe_path(path: Path) -> None:
    unsafe = ("holdout", "sealed", "credential", "forward")
    if ".." in path.parts or any(
        any(token in part.casefold() for token in unsafe) for part in path.parts
    ):
        raise CmeStatusEvidenceError("status evidence path is not search-safe")
    absolute = path if path.is_absolute() else Path.cwd() / path
    cursor = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        cursor /= part
        if cursor.is_symlink():
            raise CmeStatusEvidenceError("status evidence cannot traverse a symbolic link")


def load_cme_trading_status_evidence(
    path: str | Path,
    *,
    allow_test_fixture: bool = False,
) -> CmeTradingStatusEvidence:
    """Load immutable status evidence; test fixtures require explicit opt-in."""

    requested = Path(path).expanduser()
    _reject_unsafe_path(requested)
    if not requested.is_file():
        raise CmeStatusEvidenceError("status evidence must be a regular non-symlink file")
    raw = requested.read_bytes()
    try:
        document = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise CmeStatusEvidenceError("status evidence must be valid UTF-8 TOML") from error
    _require_exact_keys(document, {"evidence", "observations"}, label="status document")
    evidence = document["evidence"]
    _require_exact_keys(
        evidence,
        {
            "schema",
            "version",
            "evidence_kind",
            "venue",
            "product_root",
            "source_id",
            "source_sha256",
            "covered_start_ts_ns",
            "covered_end_ts_ns",
            "maximum_observation_age_seconds",
        },
        label="status evidence",
    )
    if evidence["schema"] != "systematic_fx.cme_trading_status_evidence.v1":
        raise CmeStatusEvidenceError("unsupported CME status evidence schema")
    kind = str(evidence["evidence_kind"])
    if kind not in {"CME_MARKET_STATUS_FEED_ARCHIVE", "DETERMINISTIC_TEST_FIXTURE"}:
        raise CmeStatusEvidenceError("unsupported CME status evidence kind")
    if kind == "DETERMINISTIC_TEST_FIXTURE" and not allow_test_fixture:
        raise CmeStatusEvidenceError("test status evidence requires explicit test-only opt-in")
    if evidence["venue"] != "CME_GLOBEX" or evidence["product_root"] != "6E":
        raise CmeStatusEvidenceError("status evidence is not scoped to CME Globex 6E")
    if not str(evidence["version"]) or not str(evidence["source_id"]):
        raise CmeStatusEvidenceError("status evidence requires version and source identity")
    source_sha256 = str(evidence["source_sha256"])
    if len(source_sha256) != 64 or any(c not in "0123456789abcdef" for c in source_sha256):
        raise CmeStatusEvidenceError("status upstream source SHA-256 is invalid")
    observations: list[TradingStatusObservation] = []
    for item in document["observations"]:
        _require_exact_keys(
            item,
            {"effective_ts_ns", "observed_ts_ns", "source_sequence", "status"},
            label="status observation",
        )
        try:
            status = TradingStatus(str(item["status"]))
        except ValueError as error:
            raise CmeStatusEvidenceError("unsupported CME trading status") from error
        observations.append(
            TradingStatusObservation(
                effective_ts_ns=int(item["effective_ts_ns"]),
                observed_ts_ns=int(item["observed_ts_ns"]),
                source_sequence=int(item["source_sequence"]),
                status=status,
            )
        )
    return CmeTradingStatusEvidence(
        version=str(evidence["version"]),
        sha256=hashlib.sha256(raw).hexdigest(),
        evidence_kind=kind,
        venue=str(evidence["venue"]),
        product_root=str(evidence["product_root"]),
        source_id=str(evidence["source_id"]),
        covered_start_ts_ns=int(evidence["covered_start_ts_ns"]),
        covered_end_ts_ns=int(evidence["covered_end_ts_ns"]),
        maximum_observation_age_seconds=int(evidence["maximum_observation_age_seconds"]),
        observations=tuple(observations),
        source_sha256=source_sha256,
        evidence_path=requested.resolve(strict=True),
    )


def verify_status_upstream_source(
    evidence: CmeTradingStatusEvidence,
    source_path: str | Path,
) -> CmeTradingStatusEvidence:
    """Bind separately archived status-feed bytes to an immutable evidence file."""

    requested = Path(source_path).expanduser()
    _reject_unsafe_path(requested)
    if not requested.is_file():
        raise CmeStatusEvidenceError("status upstream source must be a regular file")
    resolved = requested.resolve(strict=True)
    _verify_source_sha256(resolved, evidence.source_sha256)
    if evidence.evidence_path is None:
        raise CmeStatusEvidenceError("status evidence did not retain its source path")
    verified = replace(evidence, verified_source_path=resolved)
    verified.verify_unchanged()
    return verified


def _verify_source_sha256(path: Path, expected_sha256: str) -> None:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    if digest.hexdigest() != expected_sha256:
        raise CmeStatusEvidenceError("status upstream source SHA-256 drifted")
