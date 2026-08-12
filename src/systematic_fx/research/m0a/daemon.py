"""Lease-based bounded daemon core for durable M0a candidate evaluation."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from systematic_fx.research.m0a.ledger import (
    AttemptCompletion,
    AttemptLease,
    EpochReport,
    M0aLedger,
    M0aLedgerError,
    StaleRecovery,
)
from systematic_fx.research.m0a.model import M0aDataError

CrashStage = Literal["AFTER_CLAIM", "AFTER_EVALUATE", "AFTER_ARTIFACT_PUBLISH"]


class ForcedCrash(RuntimeError):
    """Test hook that intentionally leaves a RUNNING lease for restart recovery."""


class CandidateEvaluationFailure(RuntimeError):
    """A deterministic candidate-level failure that must not halt the daemon."""


def force_crash_after_claim(stage: CrashStage, lease: AttemptLease) -> None:
    """Reusable hook for ``--simulate-crash-after-claim`` and recovery tests."""

    if stage == "AFTER_CLAIM":
        raise ForcedCrash(f"simulated crash after claiming attempt {lease.attempt_id}")


@dataclass(frozen=True, slots=True)
class DaemonStepReport:
    epoch_id: str
    disposition: str
    recovered: tuple[StaleRecovery, ...] = ()
    lease: AttemptLease | None = None
    completion: AttemptCompletion | None = None
    error_type: str | None = None
    error_message: str | None = None
    epoch_halted: bool = False


@dataclass(frozen=True, slots=True)
class DaemonRunReport:
    epoch_id: str
    worker_id: str
    steps: tuple[DaemonStepReport, ...]
    epoch: EpochReport

    @property
    def completed_count(self) -> int:
        return sum(step.disposition == "COMPLETED" for step in self.steps)

    @property
    def failed_count(self) -> int:
        return sum(step.disposition == "FAILED" for step in self.steps)


CandidateResolver = Callable[[AttemptLease], object]
CandidateEvaluator = Callable[[object], object]
CrashHook = Callable[[CrashStage, AttemptLease], None]
SystemErrorClassifier = Callable[[BaseException], bool]


def _candidate_resolver(
    candidates: Mapping[str, object] | CandidateResolver | None,
) -> CandidateResolver:
    if candidates is None:
        return lambda lease: lease.candidate_payload
    if callable(candidates):
        return candidates

    def resolve(lease: AttemptLease) -> object:
        try:
            return candidates[lease.candidate_sha256]
        except KeyError as error:
            raise M0aLedgerError(
                f"candidate {lease.candidate_sha256} is absent from deterministic replay"
            ) from error

    return resolve


def daemon_once(
    ledger: M0aLedger,
    *,
    epoch_id: str,
    worker_id: str,
    evaluator: CandidateEvaluator,
    candidates: Mapping[str, object] | CandidateResolver | None = None,
    lease_seconds: int | None = None,
    crash_hook: CrashHook | None = None,
    system_error_classifier: SystemErrorClassifier | None = None,
    now: datetime | None = None,
) -> DaemonStepReport:
    """Recover stale work and process at most one candidate.

    Ordinary candidate failures are terminal for that candidate and are returned
    to the caller so the next invocation can continue.  ``ForcedCrash`` is never
    caught: the RUNNING lease deliberately remains durable until it expires.
    """

    recovered = ledger.recover_stale_attempts(epoch_id, now=now)
    if any(item.epoch_halted for item in recovered):
        return DaemonStepReport(
            epoch_id=epoch_id,
            disposition="HALTED",
            recovered=recovered,
            epoch_halted=True,
        )
    lease = ledger.claim_next_attempt(
        epoch_id,
        lease_owner=worker_id,
        lease_seconds=lease_seconds,
        now=now,
    )
    if lease is None:
        epoch = ledger.report(epoch_id)
        disposition = "HALTED" if epoch.status == "HALTED" else "IDLE"
        return DaemonStepReport(
            epoch_id=epoch_id,
            disposition=disposition,
            recovered=recovered,
            epoch_halted=epoch.status == "HALTED",
        )
    if crash_hook is not None:
        crash_hook("AFTER_CLAIM", lease)
    resolver = _candidate_resolver(candidates)
    try:
        candidate = resolver(lease)
        evaluation = evaluator(candidate)
        if crash_hook is not None:
            crash_hook("AFTER_EVALUATE", lease)
        artifact = ledger.publish_result_artifact(lease, evaluation)
        if crash_hook is not None:
            crash_hook("AFTER_ARTIFACT_PUBLISH", lease)
        completion = ledger.complete_attempt(lease, evaluation, artifact=artifact, now=now)
        return DaemonStepReport(
            epoch_id=epoch_id,
            disposition="COMPLETED",
            recovered=recovered,
            lease=lease,
            completion=completion,
        )
    except ForcedCrash:
        raise
    except Exception as error:  # noqa: BLE001 - candidate isolation is the daemon contract
        classifier = system_error_classifier or (
            lambda candidate_error: (
                not isinstance(
                    candidate_error,
                    (
                        CandidateEvaluationFailure,
                        M0aDataError,
                        ValueError,
                        ArithmeticError,
                        AssertionError,
                    ),
                )
            )
        )
        system_error = bool(classifier(error))
        halted = ledger.fail_attempt(
            lease,
            error=error,
            system_error=system_error,
            now=now,
        )
        return DaemonStepReport(
            epoch_id=epoch_id,
            disposition="FAILED",
            recovered=recovered,
            lease=lease,
            error_type=type(error).__name__,
            error_message=str(error),
            epoch_halted=halted,
        )


def start_daemon(
    ledger: M0aLedger,
    *,
    epoch_id: str,
    worker_id: str,
    evaluator: CandidateEvaluator,
    candidates: Mapping[str, object] | CandidateResolver | None = None,
    lease_seconds: int | None = None,
    crash_hook: CrashHook | None = None,
    system_error_classifier: SystemErrorClassifier | None = None,
    max_cycles: int | None = 1_000,
    max_completed_attempts: int | None = None,
    poll_interval_seconds: float = 0.0,
    stop_when_idle: bool = True,
    stop_event: threading.Event | None = None,
) -> DaemonRunReport:
    """Run bounded daemon iterations until idle, halted, stopped, or capped."""

    if max_cycles is not None and max_cycles <= 0:
        raise M0aLedgerError("max_cycles must be positive")
    if max_completed_attempts is not None and max_completed_attempts <= 0:
        raise M0aLedgerError("max_completed_attempts must be positive when supplied")
    if poll_interval_seconds < 0:
        raise M0aLedgerError("poll_interval_seconds cannot be negative")
    reports: list[DaemonStepReport] = []
    completed = 0
    cycles = 0
    while max_cycles is None or cycles < max_cycles:
        cycles += 1
        if stop_event is not None and stop_event.is_set():
            break
        step = daemon_once(
            ledger,
            epoch_id=epoch_id,
            worker_id=worker_id,
            evaluator=evaluator,
            candidates=candidates,
            lease_seconds=lease_seconds,
            crash_hook=crash_hook,
            system_error_classifier=system_error_classifier,
        )
        reports.append(step)
        completed += step.disposition == "COMPLETED"
        if step.epoch_halted:
            break
        if max_completed_attempts is not None and completed >= max_completed_attempts:
            break
        if step.disposition == "IDLE" and stop_when_idle:
            break
        if poll_interval_seconds:
            time.sleep(poll_interval_seconds)
    return DaemonRunReport(
        epoch_id=epoch_id,
        worker_id=worker_id,
        steps=tuple(reports),
        epoch=ledger.report(epoch_id),
    )


run_daemon = start_daemon


class M0aDaemon:
    """CLI-friendly configured facade around :func:`daemon_once`."""

    def __init__(
        self,
        ledger: M0aLedger,
        *,
        epoch_id: str,
        worker_id: str,
        evaluator: CandidateEvaluator,
        candidates: Mapping[str, object] | CandidateResolver | None = None,
        lease_seconds: int | None = None,
        crash_hook: CrashHook | None = None,
        system_error_classifier: SystemErrorClassifier | None = None,
    ) -> None:
        self.ledger = ledger
        self.epoch_id = epoch_id
        self.worker_id = worker_id
        self.evaluator = evaluator
        self.candidates = candidates
        self.lease_seconds = lease_seconds
        self.crash_hook = crash_hook
        self.system_error_classifier = system_error_classifier

    def once(self) -> DaemonStepReport:
        return daemon_once(
            self.ledger,
            epoch_id=self.epoch_id,
            worker_id=self.worker_id,
            evaluator=self.evaluator,
            candidates=self.candidates,
            lease_seconds=self.lease_seconds,
            crash_hook=self.crash_hook,
            system_error_classifier=self.system_error_classifier,
        )

    def start(self, **options: Any) -> DaemonRunReport:
        return start_daemon(
            self.ledger,
            epoch_id=self.epoch_id,
            worker_id=self.worker_id,
            evaluator=self.evaluator,
            candidates=self.candidates,
            lease_seconds=self.lease_seconds,
            crash_hook=self.crash_hook,
            system_error_classifier=self.system_error_classifier,
            **options,
        )
