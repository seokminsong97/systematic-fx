"""Canonical Phase 1A source-date proxy calendar and performance-free splits.

This module deliberately stops below research eligibility.  MBP-10 source dates
cannot establish point-in-time instrument definitions or trading status, so the
result is a screening-only proxy and can never authorize ``PASS_BACKTEST``.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import stat
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path, PurePosixPath
from typing import Final, Literal

CALENDAR_SCHEMA: Final = "systematic_fx.phase1a_screening_source_date_calendar.v1"
SPLIT_SCHEMA: Final = "systematic_fx.phase1a_screening_source_date_split.v1"
CAMPAIGN_ID: Final = "phase1a_conservative_screening_v1"
CALENDAR_VERSION: Final = "phase1a_screening_source_date_calendar_v1"
SPLIT_VERSION: Final = "phase1a_performance_free_split_v1"
CALENDAR_ARTIFACT_FILENAME: Final = f"{CALENDAR_VERSION}.json"
SPLIT_ARTIFACT_FILENAME: Final = f"{SPLIT_VERSION}.json"

MINIMUM_ELIGIBLE_SOURCE_DATES: Final = 740
EMBARGO_SOURCE_DATES: Final = 20
SEALED_HOLDOUT_SOURCE_DATES: Final = 120
OUTCOME_TAIL_SOURCE_DATES: Final = 20
DISCOVERY_BASE_SOURCE_DATES: Final = 220
PRE_RESERVATION_BASE_SOURCE_DATES: Final = 580
WALK_FORWARD_FOLD_COUNT: Final = 5
MINIMUM_FOLD_SOURCE_DATES: Final = 72

PHASE1A_EXCLUDED_SOURCE_DATES: Final = (
    date(2024, 6, 30),
    date(2024, 7, 1),
    date(2024, 7, 14),
    date(2026, 4, 19),
    date(2026, 6, 7),
    date(2026, 6, 21),
)

_SOURCE_ARTIFACT_KEYS: Final = frozenset({"byte_size", "relative_uri", "sha256", "source_date"})
_QC_ARTIFACT_KEYS: Final = frozenset(
    {
        "artifact_schema",
        "checker_version",
        "config_sha256",
        "coverage_complete",
        "diagnostic_counts",
        "expected_row_count",
        "expected_row_group_count",
        "first_ts_recv_ns",
        "hard_violation_count",
        "hard_violation_counts",
        "last_ts_recv_ns",
        "relative_uri",
        "research_eligible",
        "result",
        "scanned_row_count",
        "scanned_row_group_count",
        "schema_fingerprint",
        "source_byte_size",
        "source_date",
        "source_manifest_sha256",
        "source_sha256",
    }
)
_QC_ARTIFACT_SCHEMA: Final = "systematic_fx.mbp10_structural_qc_file.v1"
_QC_CHECKER_VERSION: Final = "mbp10_structural_qc_v1"
_SHA256_PATTERN: Final = re.compile(r"[0-9a-f]{64}")
_SOURCE_URI_PATTERN: Final = re.compile(
    r"(?P<year>[0-9]{4})/(?P<month>[0-9]{2})/(?P<day>[0-9]{2})/"
    r"glbx-mdp3-(?P<stamp>[0-9]{8})\.mbp-10\.parquet"
)


class SplitValidationError(ValueError):
    """An input manifest, exclusion policy, calendar, or split is inconsistent."""


@dataclass(frozen=True, slots=True)
class _SourceRecord:
    relative_uri: str
    source_date: date
    sha256: str
    byte_size: int


def _canonical_json_bytes(payload: Mapping[str, object]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _canonical_json_line(payload: Mapping[str, object]) -> bytes:
    return _canonical_json_bytes(payload) + b"\n"


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise SplitValidationError(f"duplicate JSON object key: {key!r}")
        result[key] = value
    return result


def _reject_nonfinite_json(value: str) -> object:
    raise SplitValidationError(f"non-finite JSON value is prohibited: {value}")


def _strict_manifest_path(path: Path | str, *, label: str) -> Path:
    requested = Path(path).expanduser()
    if requested.is_symlink():
        raise SplitValidationError(f"{label} cannot be a symbolic link: {requested}")
    try:
        resolved = requested.resolve(strict=True)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"{label} does not exist: {requested}") from exc
    if not stat.S_ISREG(resolved.lstat().st_mode):
        raise SplitValidationError(f"{label} must be a regular file: {resolved}")
    return resolved


def _read_canonical_jsonl(
    path: Path | str,
    *,
    label: str,
    expected_keys: frozenset[str],
) -> tuple[str, tuple[dict[str, object], ...]]:
    resolved = _strict_manifest_path(path, label=label)
    digest = hashlib.sha256()
    records: list[dict[str, object]] = []

    with resolved.open("rb") as handle:
        before = os.fstat(handle.fileno())
        for line_number, raw_line in enumerate(handle, start=1):
            digest.update(raw_line)
            try:
                decoded = raw_line.decode("utf-8")
                parsed = json.loads(
                    decoded,
                    object_pairs_hook=_reject_duplicate_keys,
                    parse_constant=_reject_nonfinite_json,
                )
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise SplitValidationError(f"invalid {label} JSON on line {line_number}") from exc
            if not isinstance(parsed, dict) or set(parsed) != expected_keys:
                raise SplitValidationError(
                    f"invalid {label} fields on line {line_number}: "
                    f"expected {sorted(expected_keys)}"
                )
            if raw_line != _canonical_json_line(parsed):
                raise SplitValidationError(f"{label} line {line_number} is not canonical JSONL")
            records.append(parsed)
        after = os.fstat(handle.fileno())

    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if identity_before != identity_after:
        raise SplitValidationError(f"{label} changed while it was read")
    if not records:
        raise SplitValidationError(f"{label} is empty")
    return digest.hexdigest(), tuple(records)


def _required_int(value: object, *, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise SplitValidationError(f"{label} must be an integer >= {minimum}")
    return value


def _required_sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise SplitValidationError(f"{label} must be a lowercase 64-character SHA-256")
    return value


def _parse_source_uri(value: object) -> tuple[str, date]:
    if not isinstance(value, str) or not value or "\\" in value:
        raise SplitValidationError("relative_uri must be a canonical POSIX path")
    pure = PurePosixPath(value)
    if (
        pure.is_absolute()
        or pure.as_posix() != value
        or any(part in {"", ".", ".."} for part in pure.parts)
    ):
        raise SplitValidationError(f"unsafe relative_uri: {value!r}")
    match = _SOURCE_URI_PATTERN.fullmatch(value)
    if match is None:
        raise SplitValidationError(f"invalid MBP-10 relative_uri: {value!r}")
    try:
        parsed = date(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
        )
    except ValueError as exc:
        raise SplitValidationError(f"invalid source date in relative_uri: {value!r}") from exc
    if parsed.strftime("%Y%m%d") != match.group("stamp"):
        raise SplitValidationError(f"partition and filename dates disagree: {value!r}")
    return value, parsed


def _parse_iso_date(value: object, *, label: str) -> date:
    if isinstance(value, datetime):
        raise SplitValidationError(f"{label} must be a date, not a datetime")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise SplitValidationError(f"{label} must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise SplitValidationError(f"{label} must be an ISO date") from exc
    if value != parsed.isoformat():
        raise SplitValidationError(f"{label} must be a canonical ISO date")
    return parsed


def _parse_source_manifest(
    path: Path | str,
) -> tuple[str, tuple[_SourceRecord, ...]]:
    manifest_sha256, raw_records = _read_canonical_jsonl(
        path,
        label="source SHA manifest",
        expected_keys=_SOURCE_ARTIFACT_KEYS,
    )
    sources: list[_SourceRecord] = []
    previous_uri: str | None = None
    previous_date: date | None = None

    for line_number, record in enumerate(raw_records, start=1):
        uri, uri_date = _parse_source_uri(record["relative_uri"])
        source_date_text = record["source_date"]
        if not isinstance(source_date_text, str) or source_date_text != uri_date.isoformat():
            raise SplitValidationError(
                f"source manifest date disagrees with relative_uri on line {line_number}"
            )
        if previous_uri is not None and uri <= previous_uri:
            raise SplitValidationError("source manifest URIs must be unique and strictly ordered")
        if previous_date is not None and uri_date <= previous_date:
            reason = "duplicate" if uri_date == previous_date else "reverse-ordered"
            raise SplitValidationError(f"source manifest contains a {reason} source date")

        source_sha = _required_sha256(
            record["sha256"], label=f"source manifest line {line_number} sha256"
        )
        byte_size = _required_int(
            record["byte_size"], label=f"source manifest line {line_number} byte_size"
        )
        sources.append(
            _SourceRecord(
                relative_uri=uri,
                source_date=uri_date,
                sha256=source_sha,
                byte_size=byte_size,
            )
        )
        previous_uri = uri
        previous_date = uri_date

    return manifest_sha256, tuple(sources)


def _normalize_exclusions(values: Sequence[date | str]) -> tuple[date, ...]:
    if isinstance(values, (str, bytes)):
        raise SplitValidationError("excluded_source_dates must be an ordered date sequence")
    parsed = tuple(
        _parse_iso_date(value, label=f"excluded_source_dates[{index}]")
        for index, value in enumerate(values)
    )
    if len(set(parsed)) != len(parsed):
        raise SplitValidationError("excluded_source_dates contains duplicate dates")
    if tuple(sorted(parsed)) != parsed:
        raise SplitValidationError("excluded_source_dates must be strictly ordered")
    if parsed != PHASE1A_EXCLUDED_SOURCE_DATES:
        raise SplitValidationError(
            "Phase 1A exclusion drift: exclusions must equal the six frozen source dates"
        )
    return parsed


def _validate_count_mapping(value: object, *, label: str) -> int:
    if not isinstance(value, dict):
        raise SplitValidationError(f"{label} must be an object")
    total = 0
    for key, count in value.items():
        if not isinstance(key, str) or not key:
            raise SplitValidationError(f"{label} keys must be non-empty strings")
        total += _required_int(count, label=f"{label}.{key}")
    return total


@dataclass(frozen=True, slots=True)
class Phase1AScreeningCalendar:
    """Immutable source-date proxy calendar bound to two canonical manifests."""

    source_dates: tuple[date, ...]
    excluded_source_dates: tuple[date, ...]
    source_manifest_sha256: str
    qc_manifest_sha256: str
    source_record_count: int
    qc_pass_record_count: int
    qc_fail_record_count: int
    qc_config_sha256: str
    schema_fingerprint: str

    def __post_init__(self) -> None:
        _validate_strict_dates(self.source_dates, label="source_dates")
        if self.excluded_source_dates != PHASE1A_EXCLUDED_SOURCE_DATES:
            raise SplitValidationError("calendar exclusion dates drifted from Phase 1A policy")
        if set(self.source_dates) & set(self.excluded_source_dates):
            raise SplitValidationError("excluded source dates cannot remain in the calendar")
        _required_sha256(self.source_manifest_sha256, label="source_manifest_sha256")
        _required_sha256(self.qc_manifest_sha256, label="qc_manifest_sha256")
        _required_sha256(self.qc_config_sha256, label="qc_config_sha256")
        _required_sha256(self.schema_fingerprint, label="schema_fingerprint")
        if self.source_record_count != len(self.source_dates) + len(self.excluded_source_dates):
            raise SplitValidationError("calendar source record count is inconsistent")
        if self.qc_pass_record_count != len(self.source_dates):
            raise SplitValidationError("calendar QC PASS count is inconsistent")
        if self.qc_fail_record_count != len(self.excluded_source_dates):
            raise SplitValidationError("calendar QC FAIL count is inconsistent")

    @property
    def payload(self) -> dict[str, object]:
        return {
            "artifact_schema": CALENDAR_SCHEMA,
            "calendar_version": CALENDAR_VERSION,
            "campaign_id": CAMPAIGN_ID,
            "authority": {
                "maximum_positive_label": "SCREENING_SURVIVOR",
                "pass_backtest_allowed": False,
                "paper_allowed": False,
                "screening_only": True,
            },
            "qualification_semantics": {
                "calendar_kind": "SOURCE_DATE_PROXY",
                "definition_data_available": False,
                "status_data_available": False,
                "eligible_active_day_calendar": False,
                "performance_values_used": False,
                "warning": (
                    "MBP-10 source-date proxy only; missing definition/status inputs block "
                    "PASS_BACKTEST"
                ),
            },
            "source_manifest": {
                "artifact": "mbp10_source_sha256_v1",
                "record_count": self.source_record_count,
                "sha256": self.source_manifest_sha256,
            },
            "full_structural_qc_manifest": {
                "artifact_schema": _QC_ARTIFACT_SCHEMA,
                "checker_version": _QC_CHECKER_VERSION,
                "config_sha256": self.qc_config_sha256,
                "fail_record_count": self.qc_fail_record_count,
                "pass_record_count": self.qc_pass_record_count,
                "record_count": self.source_record_count,
                "schema_fingerprint": self.schema_fingerprint,
                "sha256": self.qc_manifest_sha256,
            },
            "exclusion_policy": {
                "dates": [value.isoformat() for value in self.excluded_source_dates],
                "effect": "EXCLUDE_ENTIRE_SOURCE_DATE",
                "expected_count": len(PHASE1A_EXCLUDED_SOURCE_DATES),
                "raw_fail_preserved": True,
                "reclassification_allowed": False,
            },
            "eligible_source_date_proxy": {
                "count": len(self.source_dates),
                "first": self.source_dates[0].isoformat(),
                "last": self.source_dates[-1].isoformat(),
                "source_dates": [value.isoformat() for value in self.source_dates],
                "strictly_increasing": True,
            },
        }

    def canonical_json(self) -> bytes:
        return _canonical_json_bytes(self.payload)

    @property
    def sha256(self) -> str:
        return _sha256(self.canonical_json())


def _validate_strict_dates(values: Sequence[date], *, label: str) -> None:
    if not values:
        raise SplitValidationError(f"{label} cannot be empty")
    previous: date | None = None
    for index, value in enumerate(values):
        if isinstance(value, datetime) or not isinstance(value, date):
            raise SplitValidationError(f"{label}[{index}] must be a date")
        if previous is not None and value <= previous:
            reason = "duplicate" if value == previous else "reverse-ordered"
            raise SplitValidationError(f"{label} contains a {reason} date")
        previous = value


def build_phase1a_screening_calendar(
    source_manifest_path: Path | str,
    qc_manifest_path: Path | str,
    *,
    excluded_source_dates: Sequence[date | str] = PHASE1A_EXCLUDED_SOURCE_DATES,
) -> Phase1AScreeningCalendar:
    """Strictly join full source SHA and QC manifests into a source-date proxy.

    Both inputs must be canonical JSONL, complete, strictly ordered, and have
    identical source identities.  Exactly the six frozen dates must be QC
    ``FAIL``; every other source must be a complete QC ``PASS``.
    """

    exclusions = _normalize_exclusions(excluded_source_dates)
    excluded_set = frozenset(exclusions)
    source_manifest_sha256, sources = _parse_source_manifest(source_manifest_path)
    qc_manifest_sha256, qc_records = _read_canonical_jsonl(
        qc_manifest_path,
        label="full QC manifest",
        expected_keys=_QC_ARTIFACT_KEYS,
    )
    if len(qc_records) != len(sources):
        raise SplitValidationError("manifest mismatch: source SHA and full QC record counts differ")

    eligible_dates: list[date] = []
    observed_fail_dates: list[date] = []
    qc_config_sha256: str | None = None
    schema_fingerprint: str | None = None

    for line_number, (source, qc) in enumerate(zip(sources, qc_records, strict=True), start=1):
        identity = (
            qc["relative_uri"],
            qc["source_date"],
            qc["source_sha256"],
            qc["source_byte_size"],
        )
        expected_identity = (
            source.relative_uri,
            source.source_date.isoformat(),
            source.sha256,
            source.byte_size,
        )
        if identity != expected_identity:
            raise SplitValidationError(
                f"manifest mismatch on line {line_number}: QC source identity differs"
            )
        if qc["artifact_schema"] != _QC_ARTIFACT_SCHEMA:
            raise SplitValidationError(f"unexpected QC artifact schema on line {line_number}")
        if qc["checker_version"] != _QC_CHECKER_VERSION:
            raise SplitValidationError(f"unexpected QC checker version on line {line_number}")
        if qc["source_manifest_sha256"] != source_manifest_sha256:
            raise SplitValidationError(
                f"manifest mismatch on line {line_number}: source manifest SHA-256 differs"
            )
        if qc["coverage_complete"] is not True:
            raise SplitValidationError(f"incomplete QC coverage for {source.relative_uri}")
        if qc["research_eligible"] is not False:
            raise SplitValidationError(
                "structural QC records must not claim overall research eligibility"
            )

        expected_rows = _required_int(
            qc["expected_row_count"], label=f"QC line {line_number} expected_row_count", minimum=1
        )
        scanned_rows = _required_int(
            qc["scanned_row_count"], label=f"QC line {line_number} scanned_row_count", minimum=1
        )
        expected_groups = _required_int(
            qc["expected_row_group_count"],
            label=f"QC line {line_number} expected_row_group_count",
            minimum=1,
        )
        scanned_groups = _required_int(
            qc["scanned_row_group_count"],
            label=f"QC line {line_number} scanned_row_group_count",
            minimum=1,
        )
        if (scanned_rows, scanned_groups) != (expected_rows, expected_groups):
            raise SplitValidationError(f"incomplete QC scan for {source.relative_uri}")

        hard_total = _required_int(
            qc["hard_violation_count"], label=f"QC line {line_number} hard_violation_count"
        )
        if (
            _validate_count_mapping(
                qc["hard_violation_counts"], label=f"QC line {line_number} hard_violation_counts"
            )
            != hard_total
        ):
            raise SplitValidationError(f"QC hard-violation total mismatch on line {line_number}")
        _validate_count_mapping(
            qc["diagnostic_counts"], label=f"QC line {line_number} diagnostic_counts"
        )

        record_config_sha = _required_sha256(
            qc["config_sha256"], label=f"QC line {line_number} config_sha256"
        )
        record_schema_sha = _required_sha256(
            qc["schema_fingerprint"], label=f"QC line {line_number} schema_fingerprint"
        )
        if qc_config_sha256 is None:
            qc_config_sha256 = record_config_sha
            schema_fingerprint = record_schema_sha
        elif (record_config_sha, record_schema_sha) != (qc_config_sha256, schema_fingerprint):
            raise SplitValidationError("full QC manifest contains config or schema drift")

        result = qc["result"]
        if source.source_date in excluded_set:
            if result != "FAIL" or hard_total <= 0:
                raise SplitValidationError(
                    f"Phase 1A exclusion/QC drift for {source.source_date.isoformat()}: "
                    "frozen excluded date must preserve QC FAIL"
                )
            observed_fail_dates.append(source.source_date)
        else:
            if result != "PASS" or hard_total != 0:
                raise SplitValidationError(
                    f"QC non-PASS on non-excluded date {source.source_date.isoformat()}"
                )
            eligible_dates.append(source.source_date)

    if tuple(observed_fail_dates) != exclusions:
        raise SplitValidationError(
            "Phase 1A exclusion drift: full QC FAIL dates do not equal frozen exclusions"
        )
    if qc_config_sha256 is None or schema_fingerprint is None:  # pragma: no cover - nonempty guard
        raise SplitValidationError("full QC manifest is empty")

    return Phase1AScreeningCalendar(
        source_dates=tuple(eligible_dates),
        excluded_source_dates=exclusions,
        source_manifest_sha256=source_manifest_sha256,
        qc_manifest_sha256=qc_manifest_sha256,
        source_record_count=len(sources),
        qc_pass_record_count=len(eligible_dates),
        qc_fail_record_count=len(observed_fail_dates),
        qc_config_sha256=qc_config_sha256,
        schema_fingerprint=schema_fingerprint,
    )


def _period_payload(values: tuple[date, ...]) -> dict[str, object]:
    return {
        "count": len(values),
        "first": values[0].isoformat(),
        "last": values[-1].isoformat(),
        "source_dates": [value.isoformat() for value in values],
    }


@dataclass(frozen=True, slots=True)
class Phase1AScreeningSplit:
    """Chronological, performance-independent partition of one proxy calendar."""

    calendar_sha256: str
    eligible_source_date_count: int
    discovery: tuple[date, ...]
    walk_forward_folds: tuple[tuple[date, ...], ...]
    embargo: tuple[date, ...]
    sealed_holdout: tuple[date, ...]
    outcome_tail: tuple[date, ...]

    def __post_init__(self) -> None:
        _required_sha256(self.calendar_sha256, label="calendar_sha256")
        periods = (
            ("discovery", self.discovery),
            *(
                (f"walk_forward_folds[{index}]", fold)
                for index, fold in enumerate(self.walk_forward_folds)
            ),
            ("embargo", self.embargo),
            ("sealed_holdout", self.sealed_holdout),
            ("outcome_tail", self.outcome_tail),
        )
        if len(self.walk_forward_folds) != WALK_FORWARD_FOLD_COUNT:
            raise SplitValidationError("split must contain exactly five walk-forward folds")
        for label, values in periods:
            _validate_strict_dates(values, label=label)
        combined = tuple(value for _, values in periods for value in values)
        _validate_strict_dates(combined, label="combined split dates")
        if len(combined) != self.eligible_source_date_count:
            raise SplitValidationError("split does not cover its eligible calendar count")
        if len(self.embargo) != EMBARGO_SOURCE_DATES:
            raise SplitValidationError("embargo must contain exactly 20 source dates")
        if len(self.sealed_holdout) != SEALED_HOLDOUT_SOURCE_DATES:
            raise SplitValidationError("sealed holdout must contain exactly 120 source dates")
        if len(self.outcome_tail) != OUTCOME_TAIL_SOURCE_DATES:
            raise SplitValidationError("outcome tail must contain exactly 20 source dates")
        if any(len(fold) < MINIMUM_FOLD_SOURCE_DATES for fold in self.walk_forward_folds):
            raise SplitValidationError("each walk-forward fold must contain at least 72 dates")

    @property
    def payload(self) -> dict[str, object]:
        pre_reservation_count = len(self.discovery) + sum(
            len(fold) for fold in self.walk_forward_folds
        )
        fold_sizes = [len(fold) for fold in self.walk_forward_folds]
        return {
            "artifact_schema": SPLIT_SCHEMA,
            "split_version": SPLIT_VERSION,
            "campaign_id": CAMPAIGN_ID,
            "calendar": {
                "calendar_schema": CALENDAR_SCHEMA,
                "calendar_sha256": self.calendar_sha256,
                "eligible_source_date_count": self.eligible_source_date_count,
                "semantics": "SOURCE_DATE_PROXY_WITHOUT_DEFINITION_OR_STATUS",
            },
            "authority": {
                "maximum_positive_label": "SCREENING_SURVIVOR",
                "pass_backtest_allowed": False,
                "paper_allowed": False,
                "screening_only": True,
            },
            "construction": {
                "definition_data_available": False,
                "status_data_available": False,
                "performance_values_used": False,
                "randomness_used": False,
                "order_basis": "STRICTLY_INCREASING_ELIGIBLE_SOURCE_DATE_PROXY",
                "minimum_eligible_source_dates": MINIMUM_ELIGIBLE_SOURCE_DATES,
                "pre_reservation_count_p": pre_reservation_count,
                "extra_count": pre_reservation_count - PRE_RESERVATION_BASE_SOURCE_DATES,
                "discovery_formula": "220 + floor(2 * (P - 580) / 5)",
                "walk_forward_fold_count": WALK_FORWARD_FOLD_COUNT,
                "minimum_fold_source_dates": MINIMUM_FOLD_SOURCE_DATES,
                "fold_remainder_assignment": "ONE_EACH_FROM_OLDEST_FOLD",
                "partition_order": [
                    "DISCOVERY",
                    "WALK_FORWARD_1",
                    "WALK_FORWARD_2",
                    "WALK_FORWARD_3",
                    "WALK_FORWARD_4",
                    "WALK_FORWARD_5",
                    "EMBARGO",
                    "SEALED_HOLDOUT",
                    "OUTCOME_TAIL",
                ],
            },
            "partitions": {
                "discovery": _period_payload(self.discovery),
                "walk_forward": [
                    {
                        "fold_id": f"WALK_FORWARD_{index}",
                        "oldest_first_ordinal": index,
                        **_period_payload(fold),
                    }
                    for index, fold in enumerate(self.walk_forward_folds, start=1)
                ],
                "fold_sizes_oldest_first": fold_sizes,
                "embargo": {"sealed": True, **_period_payload(self.embargo)},
                "sealed_holdout": {"sealed": True, **_period_payload(self.sealed_holdout)},
                "outcome_tail": {
                    "new_holdout_signals_allowed": False,
                    "sealed": True,
                    **_period_payload(self.outcome_tail),
                },
            },
            "warning": (
                "Screening source-date proxy only; definition/status qualification and a "
                "separate validation campaign are required before PASS_BACKTEST"
            ),
        }

    def canonical_json(self) -> bytes:
        return _canonical_json_bytes(self.payload)

    @property
    def sha256(self) -> str:
        return _sha256(self.canonical_json())


def build_phase1a_screening_split(
    calendar: Phase1AScreeningCalendar,
) -> Phase1AScreeningSplit:
    """Create the frozen five-fold split without accepting performance values."""

    if not isinstance(calendar, Phase1AScreeningCalendar):
        raise TypeError("calendar must be a Phase1AScreeningCalendar")
    source_dates = calendar.source_dates
    if len(source_dates) < MINIMUM_ELIGIBLE_SOURCE_DATES:
        raise SplitValidationError(
            f"at least {MINIMUM_ELIGIBLE_SOURCE_DATES} eligible source dates are required"
        )

    reservation_count = (
        EMBARGO_SOURCE_DATES + SEALED_HOLDOUT_SOURCE_DATES + OUTCOME_TAIL_SOURCE_DATES
    )
    pre_reservation_count = len(source_dates) - reservation_count
    if pre_reservation_count < PRE_RESERVATION_BASE_SOURCE_DATES:
        raise SplitValidationError("pre-reservation source-date pool P must be at least 580")

    extra_count = pre_reservation_count - PRE_RESERVATION_BASE_SOURCE_DATES
    discovery_count = DISCOVERY_BASE_SOURCE_DATES + (2 * extra_count) // 5
    walk_forward_count = pre_reservation_count - discovery_count
    fold_base, fold_remainder = divmod(walk_forward_count, WALK_FORWARD_FOLD_COUNT)
    fold_sizes = tuple(
        fold_base + (1 if index < fold_remainder else 0) for index in range(WALK_FORWARD_FOLD_COUNT)
    )
    if min(fold_sizes) < MINIMUM_FOLD_SOURCE_DATES:
        raise SplitValidationError("walk-forward allocation produced a fold shorter than 72")

    cursor = 0
    discovery = source_dates[cursor : cursor + discovery_count]
    cursor += discovery_count
    folds: list[tuple[date, ...]] = []
    for fold_size in fold_sizes:
        folds.append(source_dates[cursor : cursor + fold_size])
        cursor += fold_size
    embargo = source_dates[cursor : cursor + EMBARGO_SOURCE_DATES]
    cursor += EMBARGO_SOURCE_DATES
    sealed_holdout = source_dates[cursor : cursor + SEALED_HOLDOUT_SOURCE_DATES]
    cursor += SEALED_HOLDOUT_SOURCE_DATES
    outcome_tail = source_dates[cursor : cursor + OUTCOME_TAIL_SOURCE_DATES]
    cursor += OUTCOME_TAIL_SOURCE_DATES
    if cursor != len(source_dates):  # pragma: no cover - arithmetic invariant
        raise SplitValidationError("split arithmetic did not consume the complete calendar")

    return Phase1AScreeningSplit(
        calendar_sha256=calendar.sha256,
        eligible_source_date_count=len(source_dates),
        discovery=discovery,
        walk_forward_folds=tuple(folds),
        embargo=embargo,
        sealed_holdout=sealed_holdout,
        outcome_tail=outcome_tail,
    )


PublicationDisposition = Literal["CREATED", "REUSED"]


@dataclass(frozen=True, slots=True)
class Phase1AArtifactPublication:
    """Paths and identities of one bounded immutable artifact publication."""

    manifest_directory: Path
    calendar_path: Path
    split_path: Path
    calendar_sha256: str
    split_sha256: str
    calendar_disposition: PublicationDisposition
    split_disposition: PublicationDisposition

    @property
    def payload(self) -> dict[str, object]:
        return {
            "manifest_directory": self.manifest_directory.as_posix(),
            "calendar": {
                "disposition": self.calendar_disposition,
                "path": self.calendar_path.as_posix(),
                "sha256": self.calendar_sha256,
            },
            "split": {
                "disposition": self.split_disposition,
                "path": self.split_path.as_posix(),
                "sha256": self.split_sha256,
            },
        }


def _strict_publish_directory(value: Path | str) -> Path:
    requested = Path(value).expanduser()
    if ".." in requested.parts:
        raise SplitValidationError("manifest_directory cannot contain parent traversal")
    lexical_tail = (requested.parent.parent.name, requested.parent.name, requested.name)
    if lexical_tail != ("data", "derived", "manifests"):
        raise SplitValidationError(
            "manifest_directory must explicitly end in data/derived/manifests"
        )

    for label, component in (
        ("data directory", requested.parent.parent),
        ("derived directory", requested.parent),
        ("manifest directory", requested),
    ):
        try:
            mode = component.lstat().st_mode
        except FileNotFoundError as exc:
            raise SplitValidationError(f"{label} does not exist: {component}") from exc
        if stat.S_ISLNK(mode):
            raise SplitValidationError(f"{label} cannot be a symbolic link: {component}")
        if not stat.S_ISDIR(mode):
            raise SplitValidationError(f"{label} must be a directory: {component}")

    resolved = requested.resolve(strict=True)
    if tuple(resolved.parts[-3:]) != ("data", "derived", "manifests"):
        raise SplitValidationError(
            "resolved manifest_directory must remain beneath data/derived/manifests"
        )
    return resolved


def _open_publish_directory(directory: Path) -> tuple[int, tuple[int, int]]:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(directory, flags)
    except OSError as exc:
        raise SplitValidationError(f"cannot safely open manifest_directory: {directory}") from exc
    opened = os.fstat(descriptor)
    current = directory.lstat()
    if not stat.S_ISDIR(opened.st_mode) or (opened.st_dev, opened.st_ino) != (
        current.st_dev,
        current.st_ino,
    ):
        os.close(descriptor)
        raise SplitValidationError("manifest_directory identity changed while opening")
    return descriptor, (opened.st_dev, opened.st_ino)


def _verify_directory_binding(
    directory: Path,
    expected_identity: tuple[int, int],
) -> None:
    try:
        current = directory.lstat()
    except FileNotFoundError as exc:
        raise SplitValidationError("manifest_directory disappeared during publication") from exc
    if stat.S_ISLNK(current.st_mode) or (current.st_dev, current.st_ino) != expected_identity:
        raise SplitValidationError("manifest_directory identity drift during publication")


def _existing_artifact_bytes(
    directory_descriptor: int,
    filename: str,
    *,
    label: str,
) -> bytes | None:
    try:
        path_identity = os.stat(filename, dir_fd=directory_descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(path_identity.st_mode):
        raise SplitValidationError(f"existing {label} target is unsafe or not a regular file")

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(filename, flags, dir_fd=directory_descriptor)
    except OSError as exc:
        raise SplitValidationError(f"cannot safely open existing {label} target") from exc
    try:
        opened_identity = os.fstat(descriptor)
        if not stat.S_ISREG(opened_identity.st_mode) or (
            opened_identity.st_dev,
            opened_identity.st_ino,
        ) != (path_identity.st_dev, path_identity.st_ino):
            raise SplitValidationError(f"existing {label} target changed while opening")
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 1024 * 1024):
            chunks.append(chunk)
        after_read = os.fstat(descriptor)
        if (
            after_read.st_size,
            after_read.st_mtime_ns,
            after_read.st_ctime_ns,
        ) != (
            opened_identity.st_size,
            opened_identity.st_mtime_ns,
            opened_identity.st_ctime_ns,
        ):
            raise SplitValidationError(f"existing {label} target changed while reading")
        current = os.stat(filename, dir_fd=directory_descriptor, follow_symlinks=False)
        if (current.st_dev, current.st_ino) != (opened_identity.st_dev, opened_identity.st_ino):
            raise SplitValidationError(f"existing {label} target identity drift")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _preflight_artifact(
    directory_descriptor: int,
    filename: str,
    expected: bytes,
    *,
    label: str,
) -> bool:
    existing = _existing_artifact_bytes(directory_descriptor, filename, label=label)
    if existing is None:
        return False
    if existing != expected:
        raise SplitValidationError(f"existing immutable {label} content drift")
    return True


def _open_temporary_artifact(
    directory_descriptor: int,
    filename: str,
) -> tuple[int, str]:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    for _ in range(100):
        temporary_name = f".{filename}.{secrets.token_hex(12)}.tmp"
        try:
            descriptor = os.open(
                temporary_name,
                flags,
                0o600,
                dir_fd=directory_descriptor,
            )
        except FileExistsError:
            continue
        return descriptor, temporary_name
    raise SplitValidationError("could not allocate a unique temporary artifact name")


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:  # pragma: no cover - OS contract guard
            raise OSError("short write while staging canonical artifact")
        view = view[written:]


def _publish_artifact(
    directory_descriptor: int,
    filename: str,
    payload: bytes,
    *,
    label: str,
    preexisting_identical: bool,
) -> PublicationDisposition:
    if preexisting_identical:
        return "REUSED"

    descriptor, temporary_name = _open_temporary_artifact(directory_descriptor, filename)
    descriptor_open = True
    try:
        _write_all(descriptor, payload)
        os.fchmod(descriptor, 0o444)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor_open = False
        try:
            os.link(
                temporary_name,
                filename,
                src_dir_fd=directory_descriptor,
                dst_dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
        except FileExistsError:
            existing = _existing_artifact_bytes(
                directory_descriptor,
                filename,
                label=label,
            )
            if existing != payload:
                raise SplitValidationError(f"existing immutable {label} content drift")
            disposition: PublicationDisposition = "REUSED"
        else:
            disposition = "CREATED"
        return disposition
    finally:
        if descriptor_open:
            os.close(descriptor)
        try:
            os.unlink(temporary_name, dir_fd=directory_descriptor)
        except FileNotFoundError:
            pass
        os.fsync(directory_descriptor)


def _split_dates(split: Phase1AScreeningSplit) -> tuple[date, ...]:
    return (
        split.discovery
        + tuple(value for fold in split.walk_forward_folds for value in fold)
        + split.embargo
        + split.sealed_holdout
        + split.outcome_tail
    )


def publish_phase1a_screening_artifacts(
    calendar: Phase1AScreeningCalendar,
    split: Phase1AScreeningSplit,
    *,
    manifest_directory: Path | str,
) -> Phase1AArtifactPublication:
    """Atomically publish canonical calendar and split bytes below ``data/derived``.

    The target directory must already exist and explicitly end in
    ``data/derived/manifests``.  Fixed versioned filenames prevent path
    injection.  Existing byte-identical files are reused; any other existing
    object or content drift is rejected without overwrite.
    """

    if not isinstance(calendar, Phase1AScreeningCalendar):
        raise TypeError("calendar must be a Phase1AScreeningCalendar")
    if not isinstance(split, Phase1AScreeningSplit):
        raise TypeError("split must be a Phase1AScreeningSplit")
    if split.calendar_sha256 != calendar.sha256 or _split_dates(split) != calendar.source_dates:
        raise SplitValidationError("split is not bound to the supplied canonical calendar")

    calendar_bytes = calendar.canonical_json()
    split_bytes = split.canonical_json()
    directory = _strict_publish_directory(manifest_directory)
    directory_descriptor, directory_identity = _open_publish_directory(directory)
    try:
        _verify_directory_binding(directory, directory_identity)
        # Check both destinations before creating either one, preventing an
        # ordinary pre-existing drift in the second file from causing a partial
        # publication of the first.
        calendar_exists = _preflight_artifact(
            directory_descriptor,
            CALENDAR_ARTIFACT_FILENAME,
            calendar_bytes,
            label="calendar artifact",
        )
        split_exists = _preflight_artifact(
            directory_descriptor,
            SPLIT_ARTIFACT_FILENAME,
            split_bytes,
            label="split artifact",
        )
        calendar_disposition = _publish_artifact(
            directory_descriptor,
            CALENDAR_ARTIFACT_FILENAME,
            calendar_bytes,
            label="calendar artifact",
            preexisting_identical=calendar_exists,
        )
        split_disposition = _publish_artifact(
            directory_descriptor,
            SPLIT_ARTIFACT_FILENAME,
            split_bytes,
            label="split artifact",
            preexisting_identical=split_exists,
        )
        _verify_directory_binding(directory, directory_identity)
    finally:
        os.close(directory_descriptor)

    return Phase1AArtifactPublication(
        manifest_directory=directory,
        calendar_path=directory / CALENDAR_ARTIFACT_FILENAME,
        split_path=directory / SPLIT_ARTIFACT_FILENAME,
        calendar_sha256=calendar.sha256,
        split_sha256=split.sha256,
        calendar_disposition=calendar_disposition,
        split_disposition=split_disposition,
    )
