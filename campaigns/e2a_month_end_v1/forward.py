"""Fail-closed forward-shadow scaffold for the frozen e2a candidate.

This module deliberately has no market-data listener, scheduler, broker adapter,
order writer, or Paper/Live authority.  Version 1 can only precommit and verify a
canonical shadow plan.  Resolving a blocker requires a new content-addressed plan;
this plan can never be mutated into an armed one.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
import tempfile
import tomllib
from calendar import monthrange
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Final, Never
from zoneinfo import ZoneInfo

from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256

from .config import (
    CAMPAIGN_ID,
    CANDIDATE_ID,
    DATASET_MANIFEST_RELATIVE_PATH,
    DATASET_MANIFEST_SHA256,
    HANDOVER_PROMPT_SHA256,
    HANDOVER_SOURCE_ARTIFACT_SHA256S,
    E2AConfig,
    frozen_config,
)
from .engine import E2AReproductionError, verified_readonly_file

FORWARD_PLAN_SCHEMA: Final = "systematic_fx.e2a_forward_plan.v1"
FORWARD_OBSERVATION_SCHEMA: Final = "systematic_fx.e2a_shadow_observation.v1"
FORWARD_LEDGER_EVENT_SCHEMA: Final = "systematic_fx.e2a_forward_ledger_event.v1"
FORWARD_STATUS_SCHEMA: Final = "systematic_fx.e2a_forward_status.v1"
FORWARD_PLAN_KEY: Final = "e2a_month_end_24h_forward_2026_08_v1"
FORWARD_PLAN_ARTIFACT_TYPE: Final = "E2A_FORWARD_PLAN"
FORWARD_LIFECYCLE_STATUS: Final = "PLANNED_NOT_ARMABLE"
FORWARD_AUTHORITY_SCOPE: Final = "OFFLINE_SHADOW_ONLY"
PLAN_REGISTERED: Final = "PLAN_REGISTERED"
DEFAULT_FORWARD_STATE_ROOT: Final = Path("data/derived/forward_validation/e2a_month_end_v1")
OBSERVE_UNAVAILABLE_CODE: Final = "SHADOW_OBSERVE_UNAVAILABLE_NO_FORWARD_SOURCE_ADAPTER"

_WRITE_BITS: Final = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ARTIFACT_TYPE = re.compile(r"[A-Z][A-Z0-9_]{0,95}\Z")
_EVENT_NAME = re.compile(r"event-([0-9]{8})\.json\Z")
_LONDON: Final = ZoneInfo("Europe/London")
_REPOSITORY_ROOT: Final = Path(__file__).resolve().parents[2]

_USER_DECISIONS: Final = (
    (
        "PAPER_GATE_PRECEDENCE",
        "HANDOVER_SECTION_9_VS_VALIDATION_SECTION_15_90_DAYS_75_FILLS",
    ),
    (
        "PAPER_SAFETY_PROTECTION_POLICY",
        "FROZEN_NO_TP_SL_VS_PHASE2_AND_PHASE4_MANDATORY_BROKER_OCO",
    ),
    (
        "GATE_DENOMINATOR_AND_STOP_RULE",
        "12_CALENDAR_OPPORTUNITIES_VS_SIGNALS_VS_FILLS_AND_EXTENSION_POLICY",
    ),
    (
        "WIN_AND_POSITIVE_CONCENTRATION_DEFINITION",
        "SIMULATED_GROSS_VS_VERIFIED_FEE_NET_VS_RECONCILED_EXECUTION",
    ),
    (
        "SLIPPAGE_REFERENCE_AND_MISSING_FILL_POLICY",
        "EXACT_BBO_CLOCK_SIDE_WEIGHTING_PARTIAL_REJECT_AND_NO_FILL_TREATMENT",
    ),
    (
        "PROSPECTIVE_ELIGIBILITY_POLICY",
        "WHOLE_DAY_STRUCTURAL_QC_BACKSHIFT_IS_NOT_CAUSALLY_KNOWN_AT_1500_LONDON",
    ),
    (
        "PROSPECTIVE_GT96H_GAP_EXIT_POLICY",
        "PRE_GAP_TERMINAL_QUOTE_IS_NOT_CAUSALLY_IDENTIFIABLE_UNTIL_LATER",
    ),
    (
        "BOOK_RESET_AND_RECOVERY_EXECUTION_POLICY",
        "HANDOVER_DID_NOT_FREEZE_REARM_SEMANTICS_AND_REPO_REQUIRES_ADJACENT_CLEAN_BUCKET",
    ),
    (
        "FIXED_COST_ALLOCATION_POLICY",
        "PORTFOLIO_LEVEL_VS_EXPECTED_FILL_ALLOCATION",
    ),
    (
        "VERIFIED_FEE_SCHEDULE_ARTIFACT",
        "ACTUAL_PLATFORM_COMMISSION_AND_FEE_EVIDENCE_REQUIRED",
    ),
    (
        "PLATFORM_ACCOUNT_QUANTITY_AUTHORIZATION_AND_SCHEDULER_POLICY",
        "NO_PLATFORM_ACCOUNT_QUANTITY_SCHEDULER_HOST_OUTAGE_OR_CATCHUP_DECISION",
    ),
)

_SCAFFOLD_BLOCKERS: Final = (
    "NO_FORWARD_SOURCE_OBSERVER_IMPLEMENTATION",
    "NO_LIVE_MARKET_DATA_ADAPTER",
    "NO_PAPER_BROKER_ORDER_OR_RECONCILIATION_ADAPTER",
    "NO_SCHEDULER_OR_SERVICE_STATE",
    "NO_EXTERNAL_TIMESTAMP_OR_COMMITTED_LEDGER_TAIL",
    "NO_VERIFIED_ACTUAL_FEE_SCHEDULE",
    "NO_AUTHORITATIVE_FUTURE_TRADING_STATUS_OR_SCHEDULE_FEED",
)


class E2AForwardError(RuntimeError):
    """The shadow plan, artifact, or predecessor ledger failed closed."""


class E2AForwardUnavailable(E2AForwardError):
    """A requested operation is explicitly unavailable in shadow-only v1."""

    def __init__(self, code: str = OBSERVE_UNAVAILABLE_CODE) -> None:
        super().__init__(code)
        self.code = code


def _require_sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise E2AForwardError(f"{label} is not a lowercase SHA-256")
    return value


def _canonical_document(value: Mapping[str, object]) -> tuple[dict[str, object], bytes]:
    try:
        payload = canonical_json_bytes(value)
        decoded = json.loads(payload)
    except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise E2AForwardError("forward artifact is not strict canonical JSON") from error
    if not isinstance(decoded, dict):
        raise E2AForwardError("forward artifact must be a JSON object")
    return decoded, payload


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(None):
        raise E2AForwardError("ledger timestamp must be explicit UTC")
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _validate_utc_text(value: object) -> str:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise E2AForwardError("ledger timestamp is not explicit UTC")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise E2AForwardError("ledger timestamp is invalid") from error
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(None):
        raise E2AForwardError("ledger timestamp is not UTC")
    return value


def _parse_utc_text(value: object) -> datetime:
    validated = _validate_utc_text(value)
    return datetime.fromisoformat(validated)


def _first_provisional_decision(plan: E2AForwardPlan) -> datetime:
    document = plan.as_dict()
    opportunities = document["forward_window"]["opportunities"]
    if not isinstance(opportunities, list) or not opportunities:
        raise E2AForwardError("forward plan has no first decision boundary")
    first = opportunities[0]
    if not isinstance(first, dict) or set(first).isdisjoint({"decision_utc"}):
        raise E2AForwardError("forward plan first decision boundary differs")
    return _parse_utc_text(first["decision_utc"])


def _require_before_first_decision(plan: E2AForwardPlan, recorded_at: datetime) -> None:
    if recorded_at.tzinfo is None or recorded_at.utcoffset() != UTC.utcoffset(None):
        raise E2AForwardError("registration clock is not explicit UTC")
    if recorded_at >= _first_provisional_decision(plan):
        raise E2AForwardError(
            "forward plan cannot be locally registered at or after the first provisional decision"
        )


def _safe_directory(path: Path, *, create: bool) -> Path:
    if path.is_symlink():
        raise E2AForwardError(f"forward state directory is symbolic: {path}")
    if create:
        path.mkdir(parents=True, exist_ok=True)
    if not path.is_dir():
        raise E2AForwardError(f"forward state directory is missing: {path}")
    return path.resolve(strict=True)


def _source_file_identity(relative_path: str) -> dict[str, object]:
    path = _REPOSITORY_ROOT / relative_path
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise E2AForwardError(
            f"forward implementation source is unsafe: {relative_path}"
        ) from error
    try:
        before = os.fstat(descriptor)
        visible = path.stat(follow_symlinks=False)
        if (
            not stat.S_ISREG(before.st_mode)
            or not stat.S_ISREG(visible.st_mode)
            or (before.st_dev, before.st_ino) != (visible.st_dev, visible.st_ino)
        ):
            raise E2AForwardError(
                f"forward implementation source identity differs: {relative_path}"
            )
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 1024 * 1024):
            chunks.append(chunk)
        after = os.fstat(descriptor)
        final_visible = path.stat(follow_symlinks=False)
        before_identity = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        after_identity = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        if before_identity != after_identity or (after.st_dev, after.st_ino, after.st_size) != (
            final_visible.st_dev,
            final_visible.st_ino,
            final_visible.st_size,
        ):
            raise E2AForwardError(f"forward implementation source changed: {relative_path}")
        payload = b"".join(chunks)
        if len(payload) != before.st_size:
            raise E2AForwardError(f"forward implementation source size differs: {relative_path}")
    finally:
        os.close(descriptor)
    return {
        "byte_size": len(payload),
        "relative_path": relative_path,
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def _verified_evidence_identity(
    relative_path: str,
    expected_sha256: str,
) -> dict[str, object]:
    path = _REPOSITORY_ROOT / relative_path
    try:
        with verified_readonly_file(
            path,
            expected_sha256=expected_sha256,
            relative_path=relative_path,
        ) as (_handle, identity):
            return identity.as_dict()
    except E2AReproductionError as error:
        raise E2AForwardError(
            f"forward evidence identity failed closed: {relative_path}"
        ) from error


def _registration_audit_contracts() -> list[dict[str, object]]:
    relative_path = "configs/campaigns/e2a_month_end_v1.toml"
    source_identity = _source_file_identity(relative_path)
    try:
        with verified_readonly_file(
            _REPOSITORY_ROOT / relative_path,
            expected_sha256=str(source_identity["sha256"]),
            relative_path=relative_path,
        ) as (handle, _identity):
            document = tomllib.load(handle)
    except (E2AReproductionError, tomllib.TOMLDecodeError) as error:
        raise E2AForwardError("e2a registration TOML failed closed") from error
    historical = document.get("historical_evidence")
    if not isinstance(historical, dict):
        raise E2AForwardError("e2a registration lacks historical_evidence")
    specifications = (
        (
            "legacy_audit_artifact",
            "systematic_fx.e2a_handover_raw_audit.v1",
        ),
        (
            "strict_physical_audit_artifact",
            "systematic_fx.e2a_strict_physical_audit.v1",
        ),
    )
    expected_keys = {
        "artifact_schema",
        "artifact_type",
        "audit_body_sha256",
        "byte_size",
        "content_sha256",
        "media_type",
        "relative_uri",
        "repository_byte_size",
        "repository_content_sha256",
        "repository_relative_path",
    }
    contracts: list[dict[str, object]] = []
    for table_name, expected_schema in specifications:
        raw = historical.get(table_name)
        if not isinstance(raw, dict) or set(raw) != expected_keys:
            raise E2AForwardError(f"{table_name} keys differ")
        if raw["artifact_schema"] != expected_schema:
            raise E2AForwardError(f"{table_name} schema differs")
        artifact = ForwardArtifactIdentity(
            artifact_type=raw["artifact_type"],
            byte_size=raw["byte_size"],
            content_sha256=raw["content_sha256"],
            media_type=raw["media_type"],
            relative_uri=raw["relative_uri"],
        )
        body_sha256 = _require_sha256(
            raw["audit_body_sha256"],
            label=f"{table_name} audit_body_sha256",
        )
        repository_sha256 = _require_sha256(
            raw["repository_content_sha256"],
            label=f"{table_name} repository_content_sha256",
        )
        repository_path = raw["repository_relative_path"]
        if not isinstance(repository_path, str):
            raise E2AForwardError(f"{table_name} repository path differs")
        repository_identity = _verified_evidence_identity(
            repository_path,
            repository_sha256,
        )
        if repository_identity["byte_size"] != raw["repository_byte_size"]:
            raise E2AForwardError(f"{table_name} repository byte size differs")
        contracts.append(
            {
                "artifact_schema": expected_schema,
                "audit_body_sha256": body_sha256,
                "canonical_artifact": artifact.as_dict(),
                "repository_mirror": repository_identity,
            }
        )
    return contracts


def _state_path(
    project_root: Path | str,
    state_root: Path | str | None,
) -> Path:
    project = Path(project_root).expanduser().resolve()
    if project != _REPOSITORY_ROOT:
        raise E2AForwardError(
            "project_root differs from the checkout that supplied the forward code"
        )
    requested = (
        project / DEFAULT_FORWARD_STATE_ROOT
        if state_root is None
        else Path(state_root).expanduser()
    )
    if not requested.is_absolute():
        requested = project / requested
    if requested.is_symlink():
        raise E2AForwardError("forward state root cannot be a symbolic link")
    return requested.resolve(strict=False)


def _safe_state_tree(root: Path, *, create: bool) -> tuple[Path, Path, Path, Path]:
    state = _safe_directory(root, create=create)
    artifacts = _safe_directory(state / "artifacts", create=create)
    ledger = _safe_directory(state / "ledger", create=create)
    events = _safe_directory(ledger / "events", create=create)
    staging = _safe_directory(ledger / "staging", create=create)
    return state, artifacts, events, staging


def _write_all(descriptor: int, payload: bytes) -> None:
    remaining = memoryview(payload)
    while remaining:
        written = os.write(descriptor, remaining)
        if written <= 0:
            raise E2AForwardError("immutable write made no progress")
        remaining = remaining[written:]


def _read_immutable(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise E2AForwardError(f"immutable file cannot be opened safely: {path}") from error
    try:
        before = os.fstat(descriptor)
        try:
            visible = path.stat(follow_symlinks=False)
        except OSError as error:
            raise E2AForwardError(f"immutable file disappeared: {path}") from error
        if (
            not stat.S_ISREG(before.st_mode)
            or not stat.S_ISREG(visible.st_mode)
            or (before.st_dev, before.st_ino) != (visible.st_dev, visible.st_ino)
            or before.st_nlink != 1
            or before.st_mode & _WRITE_BITS
        ):
            raise E2AForwardError(f"immutable file identity or mode differs: {path}")
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 1024 * 1024):
            chunks.append(chunk)
        after = os.fstat(descriptor)
        try:
            final_visible = path.stat(follow_symlinks=False)
        except OSError as error:
            raise E2AForwardError(f"immutable file disappeared: {path}") from error
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ) or (after.st_dev, after.st_ino, after.st_size) != (
            final_visible.st_dev,
            final_visible.st_ino,
            final_visible.st_size,
        ):
            raise E2AForwardError(f"immutable file changed while being read: {path}")
        payload = b"".join(chunks)
        if len(payload) != before.st_size:
            raise E2AForwardError(f"immutable file size differs: {path}")
        return payload
    finally:
        os.close(descriptor)


def _publish_immutable(
    destination: Path,
    payload: bytes,
    *,
    staging_root: Path,
    allow_identical: bool,
) -> None:
    if destination.is_symlink():
        raise E2AForwardError(f"immutable destination is symbolic: {destination}")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.stem}-",
        suffix=".tmp",
        dir=staging_root,
    )
    temporary = Path(temporary_name)
    try:
        _write_all(descriptor, payload)
        os.fsync(descriptor)
        os.fchmod(descriptor, 0o444)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, destination, follow_symlinks=False)
        except FileExistsError as error:
            if not allow_identical or _read_immutable(destination) != payload:
                raise E2AForwardError(f"immutable publication conflicts: {destination}") from error
        directory_descriptor = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
    if _read_immutable(destination) != payload:
        raise E2AForwardError("immutable publication did not replay exactly")


@contextmanager
def _exclusive_mutation(state_root: Path) -> Iterator[None]:
    lock_path = state_root / ".mutation.lock"
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as error:
        raise E2AForwardError("forward mutation lock cannot be opened safely") from error
    try:
        metadata = os.fstat(descriptor)
        visible = lock_path.stat(follow_symlinks=False)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or (metadata.st_dev, metadata.st_ino) != (visible.st_dev, visible.st_ino)
        ):
            raise E2AForwardError("forward mutation lock is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise E2AForwardError("another forward-plan writer is active") from error
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _last_weekday(year: int, month: int) -> date:
    candidate = date(year, month, monthrange(year, month)[1])
    while candidate.weekday() >= 5:
        candidate = date.fromordinal(candidate.toordinal() - 1)
    return candidate


def _month_sequence(start: date, count: int) -> tuple[tuple[int, int], ...]:
    months: list[tuple[int, int]] = []
    year, month = start.year, start.month
    for _ in range(count):
        months.append((year, month))
        month += 1
        if month == 13:
            month = 1
            year += 1
    return tuple(months)


def _provisional_opportunities(config: E2AConfig) -> tuple[dict[str, object], ...]:
    opportunities: list[dict[str, object]] = []
    for ordinal, (year, month) in enumerate(
        _month_sequence(config.forward_opportunity_start, config.minimum_forward_events),
        start=1,
    ):
        candidate = _last_weekday(year, month)
        local = datetime(
            candidate.year,
            candidate.month,
            candidate.day,
            config.decision_hour,
            config.decision_minute,
            tzinfo=_LONDON,
        )
        opportunities.append(
            {
                "calendar_month": f"{year:04d}-{month:02d}",
                "candidate_date": candidate.isoformat(),
                "decision_local": local.isoformat(timespec="seconds"),
                "decision_utc": local.astimezone(UTC)
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z"),
                "eligibility_status": "PROVISIONAL_NOT_ELIGIBILITY_DECISION",
                "ordinal": ordinal,
                "timezone": config.timezone,
            }
        )
    return tuple(opportunities)


def _blockers(config: E2AConfig) -> tuple[dict[str, object], ...]:
    ordered = tuple(dict.fromkeys((*config.arm_blockers, *_SCAFFOLD_BLOCKERS)))
    return tuple({"blocker_key": blocker, "status": "UNRESOLVED"} for blocker in ordered)


def _decisions() -> tuple[dict[str, object], ...]:
    return tuple(
        {
            "blocking": True,
            "conflict_reference": conflict_reference,
            "decision_key": decision_key,
            "resolution": None,
            "status": "USER_DECISION_REQUIRED",
        }
        for decision_key, conflict_reference in _USER_DECISIONS
    )


def _plan_document(config: E2AConfig) -> dict[str, object]:
    opportunities = _provisional_opportunities(config)
    return {
        "artifact_schema": FORWARD_PLAN_SCHEMA,
        "authority": {
            "allowed_execution_mode": FORWARD_AUTHORITY_SCOPE,
            "lifecycle_status": FORWARD_LIFECYCLE_STATUS,
            "live_market_data_adapter": "ABSENT",
            "paper_broker_adapter": "ABSENT",
            "paper_order_authority": False,
            "scheduler_backend": "ABSENT",
        },
        "candidate_registration": {
            "campaign_id": CAMPAIGN_ID,
            "campaign_status": config.campaign_status,
            "candidate_config_sha256": config.semantic_sha256,
            "candidate_count": config.candidate_count,
            "candidate_id": CANDIDATE_ID,
        },
        "evidence_disclosure": {
            "closed_map_policy": config.closed_map_policy,
            "consumed_holdout_disclosure": config.consumed_holdout_disclosure,
            "discovery_vs_preregistered": config.discovery_preregistered_disclosure,
            "historical_evidence_conflicts": list(config.evidence_conflicts),
        },
        "forward_window": {
            "end": config.forward_opportunity_end.isoformat(),
            "missing_event_policy": config.missing_event_policy,
            "opportunities": list(opportunities),
            "opportunity_count": len(opportunities),
            "opportunity_semantics": (
                "PROVISIONAL_LAST_MON_FRI_CANDIDATES_NOT_ELIGIBILITY_DECISIONS"
            ),
            "start": config.forward_opportunity_start.isoformat(),
            "timezone": config.timezone,
        },
        "frozen_rule": {
            "contract_selection": "PREVIOUS_SESSION_VOLUME_TRADE_BAR_V1",
            "direction": "NEGATIVE_SIGN_P15_MINUS_MONTH_OPEN",
            "entry": {
                "decision_offset_seconds": config.entry_delay_seconds,
                "long_fill_side": "BEST_ASK",
                "order_type": "MARKETABLE",
                "short_fill_side": "BEST_BID",
                "wait_cap_seconds": config.entry_wait_seconds,
            },
            "event_day": ("LAST_MON_FRI_CALENDAR_DAY_THAT_IS_AN_ELIGIBLE_TRADING_DAY"),
            "exit": {
                "first_valid_opposite_quote_at_or_after_target": True,
                "force_exit_on_contract_change": True,
                "holding_seconds": config.holding_seconds,
                "maximum_stream_gap_seconds": config.maximum_stream_gap_seconds,
                "walk_across_weekends": True,
            },
            "instrument": config.instrument,
            "missing_or_equal_signal": "SKIP",
            "month_open": {
                "lookup": (
                    "LAST_1S_TRADE_CLOSE_AT_OR_BEFORE_FIRST_ELIGIBLE_DAY_"
                    "FIRST_BAR_START_PLUS_60_SECONDS"
                ),
                "lookup_seconds": config.month_open_lookup_seconds,
                "staleness_seconds": config.month_open_staleness_seconds,
            },
            "p15": {
                "lookup": "LAST_PHYSICAL_TRADE_AT_OR_BEFORE_DECISION",
                "staleness_seconds": config.p15_staleness_seconds,
            },
            "position": {
                "maximum_concurrent_positions": config.maximum_concurrent_positions,
                "pyramiding": False,
                "stop_loss": None,
                "take_profit": None,
            },
            "structural_qc_eligibility": "INHERITED_NOT_PROSPECTIVELY_RESOLVED",
            "time_anchor": "15:00:00_EUROPE_LONDON_DST_AWARE",
        },
        "gates": {
            "evaluation_status": "NOT_EVALUABLE_UNRESOLVED_AND_NO_PAPER_EXECUTION",
            "gate_expression": (
                "NET_AT_MEASURED_COST_GT_0 AND "
                "(WINS_GTE_7_OF_12 OR NET_TICKS_GT_120) AND "
                "MAX_EVENT_SHARE_OF_GROSS_POSITIVES_LTE_0_50 AND "
                "AVERAGE_SLIPPAGE_VS_SIMULATED_BBO_LTE_1_TICK_PER_SIDE"
            ),
            "look_policy": "ONE_LOOK_AFTER_FROZEN_HORIZON_NO_INTERIM_PASS_OR_RETUNING",
            "measured_cost_policy": config.primary_cost_policy,
            "minimum_event_count": config.minimum_forward_events,
            "minimum_win_count": config.minimum_wins,
            "net_ticks_e6_alternative_strictly_greater_than": (
                config.alternative_net_ticks * 1_000_000
            ),
            "net_ticks_e6_strictly_greater_than": 0,
            "positive_event_concentration_maximum_ppm": 500_000,
            "slippage_ticks_e6_per_side_average_maximum": 1_000_000,
            "stress_diagnostic_debit_ticks": list(config.stress_debit_ticks),
            "verified_fee_schedule_artifact": None,
        },
        "implementation_blockers": list(_blockers(config)),
        "plan_key": FORWARD_PLAN_KEY,
        "provenance": {
            "campaign_config_sha256": config.semantic_sha256,
            "dataset_manifest_relative_path": str(DATASET_MANIFEST_RELATIVE_PATH),
            "dataset_manifest_sha256": DATASET_MANIFEST_SHA256,
            "handover_prompt_sha256": HANDOVER_PROMPT_SHA256,
            "audit_artifacts": _registration_audit_contracts(),
            "evidence_files": [
                _verified_evidence_identity(
                    str(DATASET_MANIFEST_RELATIVE_PATH),
                    DATASET_MANIFEST_SHA256,
                ),
                _verified_evidence_identity(
                    "data/handover_lab/CODEX_HANDOVER_PROMPT.md",
                    HANDOVER_PROMPT_SHA256,
                ),
                *(
                    _verified_evidence_identity(relative_path, sha256)
                    for relative_path, sha256 in HANDOVER_SOURCE_ARTIFACT_SHA256S
                ),
            ],
            "implementation_files": [
                _source_file_identity("campaigns/e2a_month_end_v1/config.py"),
                _source_file_identity("campaigns/e2a_month_end_v1/engine.py"),
                _source_file_identity("campaigns/e2a_month_end_v1/strict.py"),
                _source_file_identity("campaigns/e2a_month_end_v1/forward.py"),
                _source_file_identity("scripts/audit_e2a_handover.py"),
                _source_file_identity("scripts/audit_e2a_strict_physical.py"),
                _source_file_identity("scripts/run_e2a_forward_validation.py"),
            ],
            "registration_files": [
                _source_file_identity("configs/campaigns/e2a_month_end_v1.toml"),
                _source_file_identity("docs/research/E2A_MONTH_END_V1.md"),
            ],
            "policy_files": [
                _source_file_identity("docs/DESIGN.md"),
                _source_file_identity("docs/VALIDATION.md"),
                _source_file_identity("docs/phases/PHASE_1_DESIGN.md"),
                _source_file_identity("docs/phases/PHASE_2_DESIGN.md"),
            ],
        },
        "source_contract": {
            "delivery_mode": "LOCAL_BATCH_ONLY_NOT_REAL_TIME",
            "future_observation_implementation": "UNAVAILABLE",
            "per_observation_source_manifest_sha256_required": True,
            "source_kind": "LOCAL_REPO_IMMUTABLE_MBP10_PARQUET",
        },
        "user_decisions_required": list(_decisions()),
    }


@dataclass(frozen=True, slots=True)
class E2AForwardPlan:
    """Canonical bytes for the immutable shadow-only plan."""

    canonical_bytes: bytes

    def __post_init__(self) -> None:
        try:
            document = json.loads(self.canonical_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise E2AForwardError("forward plan is invalid JSON") from error
        if not isinstance(document, dict) or canonical_json_bytes(document) != self.canonical_bytes:
            raise E2AForwardError("forward plan bytes are not canonical")
        expected_keys = {
            "artifact_schema",
            "authority",
            "candidate_registration",
            "evidence_disclosure",
            "forward_window",
            "frozen_rule",
            "gates",
            "implementation_blockers",
            "plan_key",
            "provenance",
            "source_contract",
            "user_decisions_required",
        }
        if set(document) != expected_keys:
            raise E2AForwardError("forward plan keys differ")
        if document["artifact_schema"] != FORWARD_PLAN_SCHEMA:
            raise E2AForwardError("forward plan schema differs")
        if document["plan_key"] != FORWARD_PLAN_KEY:
            raise E2AForwardError("forward plan key differs")
        if document["authority"] != {
            "allowed_execution_mode": FORWARD_AUTHORITY_SCOPE,
            "lifecycle_status": FORWARD_LIFECYCLE_STATUS,
            "live_market_data_adapter": "ABSENT",
            "paper_broker_adapter": "ABSENT",
            "paper_order_authority": False,
            "scheduler_backend": "ABSENT",
        }:
            raise E2AForwardError("forward plan authority drifted")

    def as_dict(self) -> dict[str, object]:
        document = json.loads(self.canonical_bytes)
        if not isinstance(document, dict):  # pragma: no cover - guarded in __post_init__
            raise E2AForwardError("forward plan ceased to be an object")
        return document

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


def build_e2a_forward_plan(config: E2AConfig | None = None) -> E2AForwardPlan:
    """Build the one deterministic, permanently unarmable v1 plan."""

    selected = frozen_config() if config is None else config
    document = _plan_document(selected)
    return E2AForwardPlan(canonical_json_bytes(document))


@dataclass(frozen=True, slots=True)
class E2AShadowObservation:
    """Schema boundary for future immutable observations; v1 cannot record one."""

    canonical_bytes: bytes

    def __post_init__(self) -> None:
        try:
            document = json.loads(self.canonical_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise E2AForwardError("shadow observation is invalid JSON") from error
        expected_keys = {
            "artifact_schema",
            "calendar_eligibility",
            "opportunity",
            "paper_execution",
            "plan_sha256",
            "signal",
            "simulated_execution",
            "source_lineage",
        }
        if (
            not isinstance(document, dict)
            or canonical_json_bytes(document) != self.canonical_bytes
            or set(document) != expected_keys
            or document["artifact_schema"] != FORWARD_OBSERVATION_SCHEMA
        ):
            raise E2AForwardError("shadow observation schema or canonical bytes differ")
        _require_sha256(document["plan_sha256"], label="observation plan_sha256")
        paper_execution = document["paper_execution"]
        if paper_execution != {
            "fill_ids": [],
            "order_ids": [],
            "platform": None,
            "slippage_ticks_e6_per_side": None,
            "status": "NOT_OBSERVED_NO_BROKER_ADAPTER",
        }:
            raise E2AForwardError("shadow observation invented Paper execution evidence")

    def as_dict(self) -> dict[str, object]:
        document = json.loads(self.canonical_bytes)
        if not isinstance(document, dict):  # pragma: no cover - guarded above
            raise E2AForwardError("shadow observation ceased to be an object")
        return document


@dataclass(frozen=True, slots=True)
class ForwardArtifactIdentity:
    artifact_type: str
    byte_size: int
    content_sha256: str
    media_type: str
    relative_uri: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.artifact_type, str)
            or _ARTIFACT_TYPE.fullmatch(self.artifact_type) is None
        ):
            raise E2AForwardError("artifact type is invalid")
        if isinstance(self.byte_size, bool) or not isinstance(self.byte_size, int):
            raise E2AForwardError("artifact byte size is invalid")
        if self.byte_size < 1 or self.byte_size > 64 * 1024 * 1024:
            raise E2AForwardError("artifact byte size is outside its bound")
        _require_sha256(self.content_sha256, label="artifact content_sha256")
        if self.media_type != "application/json":
            raise E2AForwardError("forward artifacts must be canonical JSON")
        expected_uri = f"artifacts/{self.artifact_type.lower()}/sha256={self.content_sha256}.json"
        if self.relative_uri != expected_uri:
            raise E2AForwardError("artifact relative URI differs from its identity")

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_type": self.artifact_type,
            "byte_size": self.byte_size,
            "content_sha256": self.content_sha256,
            "media_type": self.media_type,
            "relative_uri": self.relative_uri,
        }

    @classmethod
    def from_dict(cls, value: object) -> ForwardArtifactIdentity:
        if not isinstance(value, dict) or set(value) != {
            "artifact_type",
            "byte_size",
            "content_sha256",
            "media_type",
            "relative_uri",
        }:
            raise E2AForwardError("artifact identity keys differ")
        return cls(
            artifact_type=value["artifact_type"],
            byte_size=value["byte_size"],
            content_sha256=value["content_sha256"],
            media_type=value["media_type"],
            relative_uri=value["relative_uri"],
        )


def _artifact_identity(
    artifact_type: str,
    payload: bytes,
) -> ForwardArtifactIdentity:
    digest = hashlib.sha256(payload).hexdigest()
    return ForwardArtifactIdentity(
        artifact_type=artifact_type,
        byte_size=len(payload),
        content_sha256=digest,
        media_type="application/json",
        relative_uri=f"artifacts/{artifact_type.lower()}/sha256={digest}.json",
    )


def publish_forward_artifact(
    state_root: Path | str,
    *,
    artifact_type: str,
    document: Mapping[str, object],
) -> ForwardArtifactIdentity:
    """Publish one canonical content-addressed artifact without replacement."""

    if not isinstance(artifact_type, str) or _ARTIFACT_TYPE.fullmatch(artifact_type) is None:
        raise E2AForwardError("artifact type is invalid")
    _, payload = _canonical_document(document)
    identity = _artifact_identity(artifact_type, payload)
    state, artifacts, _, staging = _safe_state_tree(Path(state_root), create=True)
    artifact_root = _safe_directory(artifacts / artifact_type.lower(), create=True)
    destination = state / identity.relative_uri
    if destination.parent != artifact_root:
        raise E2AForwardError("artifact destination escaped its fixed root")
    _publish_immutable(destination, payload, staging_root=staging, allow_identical=True)
    return identity


def verify_forward_artifact(
    state_root: Path | str,
    identity: ForwardArtifactIdentity,
    *,
    expected_bytes: bytes | None = None,
) -> bytes:
    """Verify path, canonical bytes, size, digest, and optional reconstruction."""

    state = _safe_directory(Path(state_root), create=False)
    path = state / identity.relative_uri
    if not path.resolve(strict=False).is_relative_to(state):
        raise E2AForwardError("artifact path escaped forward state")
    payload = _read_immutable(path)
    if len(payload) != identity.byte_size:
        raise E2AForwardError("artifact byte size drifted")
    if hashlib.sha256(payload).hexdigest() != identity.content_sha256:
        raise E2AForwardError("artifact content hash drifted")
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise E2AForwardError("artifact JSON is invalid") from error
    if not isinstance(document, dict) or canonical_json_bytes(document) != payload:
        raise E2AForwardError("artifact bytes are not canonical JSON")
    if expected_bytes is not None and payload != expected_bytes:
        raise E2AForwardError("artifact differs from deterministic reconstruction")
    return payload


def _audit_contracts_from_plan(
    plan: E2AForwardPlan,
) -> tuple[dict[str, object], ...]:
    provenance = plan.as_dict()["provenance"]
    if not isinstance(provenance, dict):
        raise E2AForwardError("forward plan provenance differs")
    raw_contracts = provenance.get("audit_artifacts")
    if not isinstance(raw_contracts, list) or len(raw_contracts) != 2:
        raise E2AForwardError("forward plan audit-artifact contract differs")
    expected_keys = {
        "artifact_schema",
        "audit_body_sha256",
        "canonical_artifact",
        "repository_mirror",
    }
    contracts: list[dict[str, object]] = []
    for raw in raw_contracts:
        if not isinstance(raw, dict) or set(raw) != expected_keys:
            raise E2AForwardError("forward audit-artifact entry differs")
        _require_sha256(raw["audit_body_sha256"], label="audit body sha256")
        ForwardArtifactIdentity.from_dict(raw["canonical_artifact"])
        mirror = raw["repository_mirror"]
        if not isinstance(mirror, dict) or set(mirror) != {
            "byte_size",
            "relative_path",
            "sha256",
        }:
            raise E2AForwardError("forward audit repository mirror differs")
        _require_sha256(mirror["sha256"], label="audit repository mirror sha256")
        contracts.append(raw)
    return tuple(contracts)


def _audit_document_from_repository_mirror(
    contract: Mapping[str, object],
) -> dict[str, object]:
    mirror = contract["repository_mirror"]
    if not isinstance(mirror, dict):  # pragma: no cover - validated by caller
        raise E2AForwardError("audit repository mirror ceased to be an object")
    relative_path = mirror["relative_path"]
    expected_sha256 = mirror["sha256"]
    if not isinstance(relative_path, str) or not isinstance(expected_sha256, str):
        raise E2AForwardError("audit repository mirror identity differs")
    try:
        with verified_readonly_file(
            _REPOSITORY_ROOT / relative_path,
            expected_sha256=expected_sha256,
            relative_path=relative_path,
        ) as (handle, identity):
            payload = handle.read()
    except E2AReproductionError as error:
        raise E2AForwardError("audit repository mirror failed closed") from error
    if identity.byte_size != mirror["byte_size"] or not payload.endswith(b"\n"):
        raise E2AForwardError("audit repository mirror framing differs")
    canonical_payload = payload[:-1]
    try:
        document = json.loads(canonical_payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise E2AForwardError("audit repository mirror JSON is invalid") from error
    if not isinstance(document, dict) or canonical_json_bytes(document) != canonical_payload:
        raise E2AForwardError("audit repository mirror body is not canonical")
    if (
        document.get("artifact_schema") != contract["artifact_schema"]
        or document.get("audit_sha256") != contract["audit_body_sha256"]
    ):
        raise E2AForwardError("audit repository mirror semantic identity differs")
    body = dict(document)
    claimed_body_sha256 = body.pop("audit_sha256")
    if canonical_sha256(body) != claimed_body_sha256:
        raise E2AForwardError("audit repository mirror body hash differs")
    expected_artifact = ForwardArtifactIdentity.from_dict(contract["canonical_artifact"])
    if _artifact_identity(expected_artifact.artifact_type, canonical_payload) != expected_artifact:
        raise E2AForwardError("audit canonical artifact identity differs")
    return document


def _verify_audit_lineage(
    document: Mapping[str, object],
    plan: E2AForwardPlan,
) -> None:
    plan_document = plan.as_dict()
    provenance = plan_document["provenance"]
    registration = plan_document["candidate_registration"]
    if not isinstance(provenance, dict) or not isinstance(registration, dict):
        raise E2AForwardError("forward plan lineage sections differ")
    if (
        document.get("campaign_config_sha256") != registration["candidate_config_sha256"]
        or document.get("dataset_manifest_sha256") != provenance["dataset_manifest_sha256"]
    ):
        raise E2AForwardError("audit campaign or dataset lineage differs")
    implementation_files = provenance["implementation_files"]
    if not isinstance(implementation_files, list):
        raise E2AForwardError("forward implementation lineage differs")
    implementation_by_path = {
        item["relative_path"]: item["sha256"]
        for item in implementation_files
        if isinstance(item, dict)
        and isinstance(item.get("relative_path"), str)
        and isinstance(item.get("sha256"), str)
    }
    if len(implementation_by_path) != len(implementation_files):
        raise E2AForwardError("forward implementation lineage is not one-to-one")
    schema = document.get("artifact_schema")
    if schema == "systematic_fx.e2a_handover_raw_audit.v1":
        expected_implementation = {
            "config_py": implementation_by_path["campaigns/e2a_month_end_v1/config.py"],
            "engine_py": implementation_by_path["campaigns/e2a_month_end_v1/engine.py"],
            "runner": implementation_by_path["scripts/audit_e2a_handover.py"],
        }
        if document.get("implementation_sha256s") != expected_implementation:
            raise E2AForwardError("handover audit implementation lineage differs")
        raw_inputs = document.get("handover_source_artifacts")
        if not isinstance(raw_inputs, list):
            raise E2AForwardError("handover audit source lineage differs")
        observed_inputs = {
            item["relative_uri"]: item["sha256"]
            for item in raw_inputs
            if isinstance(item, dict)
            and isinstance(item.get("relative_uri"), str)
            and isinstance(item.get("sha256"), str)
        }
        expected_inputs = dict(HANDOVER_SOURCE_ARTIFACT_SHA256S)
        if observed_inputs != expected_inputs or len(observed_inputs) != len(raw_inputs):
            raise E2AForwardError("handover audit source hashes differ")
    elif schema == "systematic_fx.e2a_strict_physical_audit.v1":
        expected_dependencies = {
            "config_py": implementation_by_path["campaigns/e2a_month_end_v1/config.py"],
            "engine_py": implementation_by_path["campaigns/e2a_month_end_v1/engine.py"],
        }
        if document.get("governed_dependency_sha256s") != expected_dependencies:
            raise E2AForwardError("strict audit governed dependency lineage differs")
        if (
            document.get("implementation_sha256")
            != implementation_by_path["campaigns/e2a_month_end_v1/strict.py"]
            or document.get("runner_sha256")
            != implementation_by_path["scripts/audit_e2a_strict_physical.py"]
        ):
            raise E2AForwardError("strict audit implementation lineage differs")
        raw_inputs = document.get("handover_input_sha256s")
        if not isinstance(raw_inputs, list):
            raise E2AForwardError("strict audit source lineage differs")
        observed_inputs = {
            item["relative_path"]: item["sha256"]
            for item in raw_inputs
            if isinstance(item, dict)
            and isinstance(item.get("relative_path"), str)
            and isinstance(item.get("sha256"), str)
        }
        expected_inputs = {
            relative_path: sha256
            for relative_path, sha256 in HANDOVER_SOURCE_ARTIFACT_SHA256S
            if relative_path.endswith((".parquet", "holdout_e2a.json"))
        }
        if observed_inputs != expected_inputs or len(observed_inputs) != len(raw_inputs):
            raise E2AForwardError("strict audit source hashes differ")
    else:  # pragma: no cover - schema guarded by the plan contract
        raise E2AForwardError("unknown audit schema")


def _publish_required_audit_artifacts(
    state_root: Path,
    plan: E2AForwardPlan,
) -> None:
    for contract in _audit_contracts_from_plan(plan):
        document = _audit_document_from_repository_mirror(contract)
        _verify_audit_lineage(document, plan)
        expected = ForwardArtifactIdentity.from_dict(contract["canonical_artifact"])
        observed = publish_forward_artifact(
            state_root,
            artifact_type=expected.artifact_type,
            document=document,
        )
        if observed != expected:
            raise E2AForwardError("required audit publication identity differs")


def _verify_required_audit_artifacts(
    state_root: Path,
    plan: E2AForwardPlan,
) -> None:
    for contract in _audit_contracts_from_plan(plan):
        expected = ForwardArtifactIdentity.from_dict(contract["canonical_artifact"])
        payload = verify_forward_artifact(state_root, expected)
        document = json.loads(payload)
        if (
            document.get("artifact_schema") != contract["artifact_schema"]
            or document.get("audit_sha256") != contract["audit_body_sha256"]
        ):
            raise E2AForwardError("required audit artifact semantic identity differs")
        body = dict(document)
        claimed_body_sha256 = body.pop("audit_sha256")
        if canonical_sha256(body) != claimed_body_sha256:
            raise E2AForwardError("required audit artifact body hash differs")
        _verify_audit_lineage(document, plan)


@dataclass(frozen=True, slots=True)
class ForwardLedgerEvent:
    sequence: int
    predecessor_sha256: str | None
    event_type: str
    plan_sha256: str
    recorded_at_utc: str
    payload: Mapping[str, object]

    def __post_init__(self) -> None:
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int):
            raise E2AForwardError("ledger sequence is invalid")
        if self.sequence != 1:
            raise E2AForwardError("shadow-only v1 permits exactly one registration event")
        if self.predecessor_sha256 is not None:
            raise E2AForwardError("first ledger event cannot have a predecessor")
        if self.event_type != PLAN_REGISTERED:
            raise E2AForwardError("shadow-only v1 ledger event type is not permitted")
        _require_sha256(self.plan_sha256, label="ledger plan_sha256")
        _validate_utc_text(self.recorded_at_utc)
        if not isinstance(self.payload, Mapping) or set(self.payload) != {"plan_artifact"}:
            raise E2AForwardError("PLAN_REGISTERED payload keys differ")
        identity = ForwardArtifactIdentity.from_dict(self.payload["plan_artifact"])
        if identity.artifact_type != FORWARD_PLAN_ARTIFACT_TYPE:
            raise E2AForwardError("PLAN_REGISTERED artifact role differs")
        if identity.content_sha256 != self.plan_sha256:
            raise E2AForwardError("ledger plan and artifact identities differ")

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": FORWARD_LEDGER_EVENT_SCHEMA,
            "event_type": self.event_type,
            "payload": dict(self.payload),
            "plan_sha256": self.plan_sha256,
            "predecessor_sha256": self.predecessor_sha256,
            "recorded_at_utc": self.recorded_at_utc,
            "sequence": self.sequence,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())

    @classmethod
    def from_dict(cls, value: object) -> ForwardLedgerEvent:
        if not isinstance(value, dict) or set(value) != {
            "artifact_schema",
            "event_type",
            "payload",
            "plan_sha256",
            "predecessor_sha256",
            "recorded_at_utc",
            "sequence",
        }:
            raise E2AForwardError("ledger event keys differ")
        if value["artifact_schema"] != FORWARD_LEDGER_EVENT_SCHEMA:
            raise E2AForwardError("ledger event schema differs")
        return cls(
            sequence=value["sequence"],
            predecessor_sha256=value["predecessor_sha256"],
            event_type=value["event_type"],
            plan_sha256=value["plan_sha256"],
            recorded_at_utc=value["recorded_at_utc"],
            payload=value["payload"],
        )


class E2AForwardLedger:
    """One-event, append-preserved predecessor ledger for the v1 plan."""

    def __init__(self, state_root: Path | str, *, create: bool) -> None:
        self.state_root, _, self.events_root, self.staging_root = _safe_state_tree(
            Path(state_root),
            create=create,
        )

    def _event_paths(self) -> tuple[Path, ...]:
        paths: dict[int, Path] = {}
        for path in self.events_root.iterdir():
            match = _EVENT_NAME.fullmatch(path.name)
            if path.is_symlink() or match is None:
                raise E2AForwardError("ledger contains an unsafe or unknown file")
            sequence = int(match.group(1))
            if sequence in paths:
                raise E2AForwardError("ledger sequence is duplicated")
            paths[sequence] = path
        ordered = tuple(paths[index] for index in sorted(paths))
        if tuple(sorted(paths)) != tuple(range(1, len(paths) + 1)):
            raise E2AForwardError("ledger sequence is not contiguous")
        return ordered

    def verify(self, plan: E2AForwardPlan) -> tuple[ForwardLedgerEvent, ...]:
        paths = self._event_paths()
        if len(paths) != 1:
            raise E2AForwardError("registered shadow-only ledger must contain one event")
        raw = _read_immutable(paths[0])
        try:
            document = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise E2AForwardError("ledger event is invalid JSON") from error
        if not isinstance(document, dict) or canonical_json_bytes(document) != raw:
            raise E2AForwardError("ledger event bytes are not canonical")
        event = ForwardLedgerEvent.from_dict(document)
        if event.plan_sha256 != plan.sha256:
            raise E2AForwardError("ledger belongs to another forward plan")
        if _parse_utc_text(event.recorded_at_utc) >= _first_provisional_decision(plan):
            raise E2AForwardError(
                "PLAN_REGISTERED was not recorded before the first provisional decision"
            )
        identity = ForwardArtifactIdentity.from_dict(event.payload["plan_artifact"])
        verify_forward_artifact(
            self.state_root,
            identity,
            expected_bytes=plan.canonical_bytes,
        )
        return (event,)

    def register_plan(self, plan: E2AForwardPlan) -> tuple[ForwardLedgerEvent, ...]:
        with _exclusive_mutation(self.state_root):
            paths = self._event_paths()
            if paths:
                return self.verify(plan)
            recorded_at = _now_utc()
            _require_before_first_decision(plan, recorded_at)
            identity = publish_forward_artifact(
                self.state_root,
                artifact_type=FORWARD_PLAN_ARTIFACT_TYPE,
                document=plan.as_dict(),
            )
            event = ForwardLedgerEvent(
                sequence=1,
                predecessor_sha256=None,
                event_type=PLAN_REGISTERED,
                plan_sha256=plan.sha256,
                recorded_at_utc=_utc_text(recorded_at),
                payload={"plan_artifact": identity.as_dict()},
            )
            destination = self.events_root / "event-00000001.json"
            _publish_immutable(
                destination,
                canonical_json_bytes(event.as_dict()),
                staging_root=self.staging_root,
                allow_identical=False,
            )
            verified = self.verify(plan)
            if verified[0].sha256 != event.sha256:
                raise E2AForwardError("ledger append did not replay exactly")
            return verified


@dataclass(frozen=True, slots=True)
class E2AForwardStatus:
    plan_sha256: str
    plan_artifact: ForwardArtifactIdentity
    plan_registered: bool
    plan_artifact_published: bool
    event_count: int
    event_tail_sha256: str | None
    next_provisional_opportunity: Mapping[str, object]
    implementation_blockers: tuple[Mapping[str, object], ...]
    user_decisions_required: tuple[Mapping[str, object], ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": FORWARD_STATUS_SCHEMA,
            "authority_scope": FORWARD_AUTHORITY_SCOPE,
            "event_count": self.event_count,
            "event_tail_sha256": self.event_tail_sha256,
            "implementation_blockers": [dict(item) for item in self.implementation_blockers],
            "lifecycle_status": FORWARD_LIFECYCLE_STATUS,
            "next_provisional_opportunity": dict(self.next_provisional_opportunity),
            "paper_order_authority": False,
            "plan_artifact": self.plan_artifact.as_dict(),
            "plan_artifact_published": self.plan_artifact_published,
            "plan_registered": self.plan_registered,
            "plan_sha256": self.plan_sha256,
            "registration_status": (
                "REGISTERED_APPEND_ONLY_LOCAL_LEDGER"
                if self.plan_registered
                else "NOT_PRECOMMITTED"
            ),
            "registration_timing": (
                "LOCALLY_RECORDED_BEFORE_FIRST_PROVISIONAL_DECISION_NOT_EXTERNALLY_TIMESTAMPED"
                if self.plan_registered
                else "NOT_REGISTERED"
            ),
            "user_decisions_required": [dict(item) for item in self.user_decisions_required],
        }


def _expected_plan_identity(plan: E2AForwardPlan) -> ForwardArtifactIdentity:
    return _artifact_identity(FORWARD_PLAN_ARTIFACT_TYPE, plan.canonical_bytes)


def _status(
    plan: E2AForwardPlan,
    *,
    identity: ForwardArtifactIdentity,
    artifact_published: bool,
    events: tuple[ForwardLedgerEvent, ...],
) -> E2AForwardStatus:
    document = plan.as_dict()
    opportunities = document["forward_window"]["opportunities"]
    blockers = tuple(document["implementation_blockers"])
    decisions = tuple(document["user_decisions_required"])
    if not isinstance(opportunities, list) or not opportunities:
        raise E2AForwardError("forward plan has no provisional opportunities")
    return E2AForwardStatus(
        plan_sha256=plan.sha256,
        plan_artifact=identity,
        plan_registered=bool(events),
        plan_artifact_published=artifact_published,
        event_count=len(events),
        event_tail_sha256=events[-1].sha256 if events else None,
        next_provisional_opportunity=opportunities[0],
        implementation_blockers=blockers,
        user_decisions_required=decisions,
    )


def _state_has_ledger_entries(root: Path) -> bool:
    if not root.exists():
        return False
    state = _safe_directory(root, create=False)
    events_root = state / "ledger/events"
    if events_root.is_symlink():
        raise E2AForwardError("forward ledger event root is symbolic")
    if not events_root.exists():
        return False
    if not events_root.is_dir():
        raise E2AForwardError("forward ledger event root is not a directory")
    return next(events_root.iterdir(), None) is not None


def precommit_e2a_forward(
    project_root: Path | str,
    *,
    state_root: Path | str | None = None,
) -> E2AForwardStatus:
    """Publish the canonical plan and append its sole registration event."""

    plan = build_e2a_forward_plan()
    root = _state_path(project_root, state_root)
    if _state_has_ledger_entries(root):
        existing = status_e2a_forward(project_root, state_root=root)
        if existing.plan_registered:
            return existing
    _require_before_first_decision(plan, _now_utc())
    _publish_required_audit_artifacts(root, plan)
    _verify_required_audit_artifacts(root, plan)
    ledger = E2AForwardLedger(root, create=True)
    events = ledger.register_plan(plan)
    identity = ForwardArtifactIdentity.from_dict(events[0].payload["plan_artifact"])
    return _status(
        plan,
        identity=identity,
        artifact_published=True,
        events=events,
    )


def status_e2a_forward(
    project_root: Path | str,
    *,
    state_root: Path | str | None = None,
) -> E2AForwardStatus:
    """Read the plan status without creating state or claiming scheduler authority."""

    plan = build_e2a_forward_plan()
    identity = _expected_plan_identity(plan)
    root = _state_path(project_root, state_root)
    if not root.exists():
        return _status(
            plan,
            identity=identity,
            artifact_published=False,
            events=(),
        )
    state = _safe_directory(root, create=False)
    _verify_required_audit_artifacts(state, plan)
    artifact_path = state / identity.relative_uri
    artifact_published = artifact_path.exists() or artifact_path.is_symlink()
    if artifact_published:
        verify_forward_artifact(state, identity, expected_bytes=plan.canonical_bytes)
    events_root = state / "ledger/events"
    if not events_root.exists():
        if events_root.is_symlink():
            raise E2AForwardError("forward ledger event root is a broken symbolic link")
        return _status(
            plan,
            identity=identity,
            artifact_published=artifact_published,
            events=(),
        )
    ledger = E2AForwardLedger(state, create=False)
    paths = ledger._event_paths()
    if not paths:
        return _status(
            plan,
            identity=identity,
            artifact_published=artifact_published,
            events=(),
        )
    if not artifact_published:
        raise E2AForwardError("ledger registration exists without its plan artifact")
    events = ledger.verify(plan)
    return _status(
        plan,
        identity=identity,
        artifact_published=True,
        events=events,
    )


def verify_e2a_forward(
    project_root: Path | str,
    *,
    state_root: Path | str | None = None,
) -> E2AForwardStatus:
    """Verify the registered plan, immutable artifact, and entire event chain."""

    plan = build_e2a_forward_plan()
    root = _state_path(project_root, state_root)
    if not root.exists():
        raise E2AForwardError("forward plan has not been precommitted")
    _verify_required_audit_artifacts(root, plan)
    ledger = E2AForwardLedger(root, create=False)
    events = ledger.verify(plan)
    identity = ForwardArtifactIdentity.from_dict(events[0].payload["plan_artifact"])
    return _status(
        plan,
        identity=identity,
        artifact_published=True,
        events=events,
    )


def observe_shadow_e2a_forward(
    project_root: Path | str,
    *,
    state_root: Path | str | None = None,
) -> Never:
    """Refuse observation until a separately governed causal source adapter exists."""

    del project_root, state_root
    raise E2AForwardUnavailable()


def record_shadow_observation_e2a_forward(
    project_root: Path | str,
    *,
    state_root: Path | str | None = None,
) -> Never:
    """Refuse recording as well as observation until the causal adapter exists."""

    return observe_shadow_e2a_forward(project_root, state_root=state_root)


__all__ = [
    "DEFAULT_FORWARD_STATE_ROOT",
    "FORWARD_AUTHORITY_SCOPE",
    "FORWARD_LIFECYCLE_STATUS",
    "FORWARD_OBSERVATION_SCHEMA",
    "FORWARD_PLAN_ARTIFACT_TYPE",
    "FORWARD_PLAN_KEY",
    "FORWARD_PLAN_SCHEMA",
    "OBSERVE_UNAVAILABLE_CODE",
    "E2AForwardError",
    "E2AForwardLedger",
    "E2AForwardPlan",
    "E2AForwardStatus",
    "E2AForwardUnavailable",
    "E2AShadowObservation",
    "ForwardArtifactIdentity",
    "ForwardLedgerEvent",
    "build_e2a_forward_plan",
    "observe_shadow_e2a_forward",
    "precommit_e2a_forward",
    "publish_forward_artifact",
    "record_shadow_observation_e2a_forward",
    "status_e2a_forward",
    "verify_e2a_forward",
    "verify_forward_artifact",
]
