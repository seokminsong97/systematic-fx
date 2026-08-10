"""Governed orchestration primitives for the isolated bar-pattern campaign."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path, PurePosixPath
from typing import Final

from systematic_fx.features.bars import (
    BAR_VERSION,
    REPORT_SCHEMA,
    SUPPORTED_TIMEFRAMES_SECONDS,
    TIMEFRAME_LABELS,
    ContractVolume,
    DailyBarBuildReport,
    DailyBarPlan,
    DailyPlanStatus,
    DailyVolumeSummary,
    SegmentTail,
    TradeBarArtifactDescriptor,
    TradeBarError,
    build_daily_trade_bar_artifacts,
)
from systematic_fx.features.bars import canonical_sha256 as bar_canonical_sha256
from systematic_fx.research.bar_config import (
    BAR_SOURCE_FILE_COUNT,
    BAR_SOURCE_MANIFEST_SHA256,
)

BAR_DATASET_MANIFEST_SCHEMA: Final = "systematic_fx.trade_bar_dataset_manifest.v1"
BAR_DATASET_BUILD_PLAN_SHA256: Final = (
    "c46323e70e389dd2f7bca4b0e3e42ad86b1a9b7b502834512906e38b4651d0dc"
)
BAR_DATASET_REPORT_COUNT: Final = BAR_SOURCE_FILE_COUNT
BAR_DATASET_ACTIVE_DATE_COUNT: Final = 1_413
BAR_DATASET_ARTIFACT_COUNT: Final = BAR_DATASET_ACTIVE_DATE_COUNT * len(
    SUPPORTED_TIMEFRAMES_SECONDS
)
BAR_DATASET_FIRST_SOURCE_DATE: Final = date(2022, 1, 2)
BAR_DATASET_LAST_SOURCE_DATE: Final = date(2026, 7, 31)
BAR_DATASET_HANDOFF_SCHEMA: Final = "systematic_fx.loaded_trade_bar_dataset_manifest.v1"
BAR_DATASET_OUTCOME_SPAN_POLICY_VERSION: Final = "trade_bar_outcome_span_v1"
BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256: Final = bar_canonical_sha256(
    {
        "active_partition_rule": "REPORT_HAS_EXACT_COMPLETE_ARTIFACT_SET",
        "contract_change_rule": (
            "BREAK_BEFORE_NEXT_ACTIVE_WHEN_SELECTED_CONTRACT_DIFFERS_FROM_ACTIVE_SPAN"
        ),
        "first_span_id": 1,
        "market_closed_rule": "ABSENT_RAW_CALENDAR_DATE_DOES_NOT_BREAK",
        "span_id_rule": "DENSE_POSITIVE_MONOTONIC_GLOBAL_ID",
        "unqualified_plan_rule": "STATUS_NOT_SELECTED_BREAKS_BEFORE_NEXT_ACTIVE",
        "version": BAR_DATASET_OUTCOME_SPAN_POLICY_VERSION,
        "zero_trade_rule": ("SELECTED_SAME_CONTRACT_PRESERVES;SELECTED_CONTRACT_CHANGE_BREAKS"),
    }
)
SOURCE_MANIFEST_RELATIVE_PATH: Final = Path("data/derived/manifests/mbp10_source_sha256_v1.jsonl")
RAW_SOURCE_RELATIVE_ROOT: Final = Path("data/mbp-10")

_MAX_DATASET_MANIFEST_BYTES: Final = 16 * 1024 * 1024
_WRITE_BITS: Final = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
_TOP_LEVEL_KEYS: Final = frozenset(
    {
        "artifact_schema",
        "bar_version",
        "build_plan_sha256",
        "eligible_active_date_count",
        "eligible_active_dates",
        "reports",
        "source_file_count",
        "source_manifest_sha256",
        "timeframes_seconds",
        "totals",
    }
)
_REPORT_KEYS: Final = frozenset(
    {
        "artifacts",
        "bad_ts_recv_trades_excluded",
        "current_volume_summary",
        "link_sha256_by_timeframe",
        "plan",
        "report_sha256",
        "segment_tail",
        "selected_trade_count",
        "source_row_count",
        "source_scanned",
        "source_trade_count",
    }
)
_PLAN_KEYS: Final = frozenset(
    {
        "artifact_schema",
        "bar_version",
        "gap_break_seconds",
        "mapping_sha256",
        "previous_segment_tail_sha256",
        "previous_source_date",
        "previous_volume_sha256",
        "qc_exclusion_policy_sha256",
        "segment_policy_version",
        "selected_contract",
        "selected_contract_month",
        "selected_instrument_id",
        "selected_previous_trade_count",
        "selected_previous_volume",
        "selection_policy_sha256",
        "selection_policy_version",
        "sha256",
        "source_date",
        "source_sha256",
        "status",
        "timeframes_seconds",
    }
)
_VOLUME_KEYS: Final = frozenset(
    {
        "artifact_schema",
        "contracts",
        "qc_eligible",
        "sha256",
        "source_date",
        "source_sha256",
    }
)
_CONTRACT_VOLUME_KEYS: Final = frozenset(
    {
        "contract_month",
        "instrument_ids",
        "raw_symbols",
        "trade_count",
        "volume",
    }
)
_SEGMENT_TAIL_KEYS: Final = frozenset(
    {
        "contract",
        "last_bar_end_ns",
        "last_trade_ns",
        "segment_id",
        "source_date",
    }
)
_ARTIFACT_KEYS: Final = frozenset(
    {
        "byte_size",
        "relative_uri",
        "row_count",
        "sha256",
        "timeframe_seconds",
    }
)
_LINK_KEYS: Final = frozenset({"sha256", "timeframe_seconds"})
_TOTAL_KEYS: Final = frozenset(
    {
        "artifact_count",
        "bad_ts_recv_trades_excluded",
        "selected_trade_count",
        "source_row_count",
        "source_trade_count",
    }
)


class BarPipelineError(RuntimeError):
    """Bar research inputs or execution state violate a frozen contract."""


@dataclass(frozen=True, slots=True)
class BarSourceFile:
    """One exact raw source identity from the existing SHA-256 manifest."""

    source_date: date
    relative_uri: str
    sha256: str
    byte_size: int
    path: Path

    def semantic_dict(self) -> dict[str, object]:
        return {
            "byte_size": self.byte_size,
            "relative_uri": self.relative_uri,
            "sha256": self.sha256,
            "source_date": self.source_date.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class BarDatasetPlan:
    """Frozen ordered source plan before raw trade projection starts."""

    project_root: Path
    data_root: Path
    source_manifest_path: Path
    source_manifest_sha256: str
    source_files: tuple[BarSourceFile, ...]

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(
            {
                "bar_version": BAR_VERSION,
                "source_file_count": len(self.source_files),
                "source_files": [item.semantic_dict() for item in self.source_files],
                "source_manifest_sha256": self.source_manifest_sha256,
                "timeframes_seconds": list(SUPPORTED_TIMEFRAMES_SECONDS),
            }
        )

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


@dataclass(frozen=True, slots=True)
class BarDatasetBuildResult:
    """Complete deterministic reports and the sealed manifest bytes."""

    plan: BarDatasetPlan
    reports: tuple[DailyBarBuildReport, ...]
    eligible_active_dates: tuple[date, ...]
    canonical_bytes: bytes
    sha256: str

    def as_dict(self) -> dict[str, object]:
        value = json.loads(self.canonical_bytes)
        if not isinstance(value, dict):  # pragma: no cover - fixed canonical root
            raise BarPipelineError("bar dataset manifest is not an object")
        return value


@dataclass(frozen=True, slots=True)
class BarDatasetPartition:
    """One active date's compact, manifest-verified Discovery handoff."""

    source_date: date
    contract: str
    outcome_span_id: int
    plan_sha256: str
    source_sha256: str
    artifacts: tuple[TradeBarArtifactDescriptor, ...]

    def __post_init__(self) -> None:
        if isinstance(self.source_date, datetime) or not isinstance(self.source_date, date):
            raise BarPipelineError("bar dataset partition source_date must be a date")
        _text(self.contract, label="bar dataset partition contract")
        _integer(
            self.outcome_span_id,
            label="bar dataset partition outcome_span_id",
            minimum=1,
        )
        _sha256(self.plan_sha256, label="bar dataset partition plan_sha256")
        _sha256(self.source_sha256, label="bar dataset partition source_sha256")
        if not isinstance(self.artifacts, tuple) or any(
            not isinstance(item, TradeBarArtifactDescriptor) for item in self.artifacts
        ):
            raise BarPipelineError("bar dataset partition artifacts must be a tuple")
        timeframes = tuple(item.timeframe_seconds for item in self.artifacts)
        if timeframes != SUPPORTED_TIMEFRAMES_SECONDS:
            raise BarPipelineError("bar dataset partition timeframes are incomplete or unordered")

    def identity_dict(self) -> dict[str, object]:
        return {
            "artifacts": [item.semantic_dict() for item in self.artifacts],
            "contract": self.contract,
            "outcome_span_id": self.outcome_span_id,
            "plan_sha256": self.plan_sha256,
            "source_date": self.source_date.isoformat(),
            "source_sha256": self.source_sha256,
        }


@dataclass(frozen=True, slots=True)
class LoadedBarDatasetManifest:
    """Fully validated manifest identity with only Discovery-facing state retained."""

    dataset_manifest_sha256: str
    source_manifest_sha256: str
    outcome_span_policy_sha256: str
    eligible_active_dates: tuple[date, ...]
    partitions: tuple[BarDatasetPartition, ...]

    def __post_init__(self) -> None:
        _sha256(self.dataset_manifest_sha256, label="bar dataset manifest sha256")
        _sha256(self.source_manifest_sha256, label="bar dataset source manifest sha256")
        if (
            _sha256(
                self.outcome_span_policy_sha256,
                label="bar dataset outcome span policy sha256",
            )
            != BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256
        ):
            raise BarPipelineError("bar dataset outcome-span policy identity drift")
        if not isinstance(self.eligible_active_dates, tuple) or any(
            isinstance(item, datetime) or not isinstance(item, date)
            for item in self.eligible_active_dates
        ):
            raise BarPipelineError("eligible active dates must be a tuple of dates")
        if len(
            self.eligible_active_dates
        ) != BAR_DATASET_ACTIVE_DATE_COUNT or self.eligible_active_dates != tuple(
            sorted(set(self.eligible_active_dates))
        ):
            raise BarPipelineError("eligible active dates are incomplete or unordered")
        if not isinstance(self.partitions, tuple) or any(
            not isinstance(item, BarDatasetPartition) for item in self.partitions
        ):
            raise BarPipelineError("bar dataset partitions must be a tuple")
        if tuple(item.source_date for item in self.partitions) != self.eligible_active_dates:
            raise BarPipelineError("bar dataset partitions differ from eligible active dates")
        span_ids = tuple(item.outcome_span_id for item in self.partitions)
        if (
            not span_ids
            or span_ids != tuple(sorted(span_ids))
            or tuple(sorted(set(span_ids))) != tuple(range(1, span_ids[-1] + 1))
        ):
            raise BarPipelineError("outcome span IDs must be dense, positive, and monotonic")
        contract_by_span: dict[int, str] = {}
        for partition in self.partitions:
            prior = contract_by_span.setdefault(partition.outcome_span_id, partition.contract)
            if prior != partition.contract:
                raise BarPipelineError("one outcome span cannot contain multiple contracts")

    def identity_dict(self) -> dict[str, object]:
        """Canonical handoff identity consumed by downstream Discovery spools."""

        return {
            "artifact_schema": BAR_DATASET_HANDOFF_SCHEMA,
            "bar_version": BAR_VERSION,
            "build_plan_sha256": BAR_DATASET_BUILD_PLAN_SHA256,
            "dataset_manifest_sha256": self.dataset_manifest_sha256,
            "eligible_active_dates": [item.isoformat() for item in self.eligible_active_dates],
            "outcome_span_policy_sha256": self.outcome_span_policy_sha256,
            "outcome_span_policy_version": BAR_DATASET_OUTCOME_SPAN_POLICY_VERSION,
            "partitions": [item.identity_dict() for item in self.partitions],
            "source_manifest_sha256": self.source_manifest_sha256,
        }

    @property
    def handoff_sha256(self) -> str:
        return bar_canonical_sha256(self.identity_dict())


def _canonical_bytes(value: object) -> bytes:
    try:
        return (
            json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("ascii")
            + b"\n"
        )
    except (TypeError, ValueError) as error:
        raise BarPipelineError("manifest payload is not canonical JSON") from error


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise BarPipelineError(f"{label} must be a lowercase SHA-256")
    return value


def _source_record(
    value: object,
    *,
    line_number: int,
    raw_root: Path,
) -> BarSourceFile:
    if not isinstance(value, Mapping) or set(value) != {
        "byte_size",
        "relative_uri",
        "sha256",
        "source_date",
    }:
        raise BarPipelineError(f"source manifest line {line_number} has an invalid schema")
    try:
        source_date = date.fromisoformat(str(value["source_date"]))
    except ValueError as error:
        raise BarPipelineError(f"source manifest line {line_number} has an invalid date") from error
    relative_uri = value["relative_uri"]
    if not isinstance(relative_uri, str) or not relative_uri:
        raise BarPipelineError(f"source manifest line {line_number} has an invalid URI")
    relative = PurePosixPath(relative_uri)
    if relative.is_absolute() or ".." in relative.parts or relative.as_posix() != relative_uri:
        raise BarPipelineError(f"source manifest line {line_number} URI is not canonical")
    sha256 = _sha256(value["sha256"], label=f"source manifest line {line_number} sha256")
    byte_size = value["byte_size"]
    if isinstance(byte_size, bool) or not isinstance(byte_size, int) or byte_size <= 0:
        raise BarPipelineError(f"source manifest line {line_number} has an invalid byte size")
    path = raw_root.joinpath(*relative.parts)
    if path.is_symlink() or not path.is_file():
        raise BarPipelineError(f"raw source is missing or unsafe: {relative_uri}")
    observed = path.stat().st_size
    if observed != byte_size:
        raise BarPipelineError(
            f"raw source size drift for {relative_uri}: expected {byte_size}, observed {observed}"
        )
    return BarSourceFile(
        source_date=source_date,
        relative_uri=relative_uri,
        sha256=sha256,
        byte_size=byte_size,
        path=path,
    )


def load_bar_dataset_plan(project_root: Path) -> BarDatasetPlan:
    """Load and structurally rebind all 1,434 already-hashed raw inputs."""

    root = project_root.expanduser().resolve()
    data_root = (root / "data").resolve(strict=True)
    if data_root.is_symlink() or not data_root.is_dir():
        raise BarPipelineError("project data root must be a real directory")
    manifest = (root / SOURCE_MANIFEST_RELATIVE_PATH).resolve(strict=True)
    if manifest.is_symlink() or not manifest.is_file() or not manifest.is_relative_to(data_root):
        raise BarPipelineError("source SHA-256 manifest path is unsafe")
    raw = manifest.read_bytes()
    digest = _sha256_bytes(raw)
    if digest != BAR_SOURCE_MANIFEST_SHA256:
        raise BarPipelineError("source SHA-256 manifest differs from the frozen identity")
    raw_root = (root / RAW_SOURCE_RELATIVE_ROOT).resolve(strict=True)
    if raw_root.is_symlink() or not raw_root.is_dir() or not raw_root.is_relative_to(data_root):
        raise BarPipelineError("raw MBP-10 root is unsafe")
    records: list[BarSourceFile] = []
    for line_number, line in enumerate(raw.splitlines(), start=1):
        try:
            value = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise BarPipelineError(
                f"source SHA-256 manifest line {line_number} is invalid JSON"
            ) from error
        records.append(
            _source_record(
                value,
                line_number=line_number,
                raw_root=raw_root,
            )
        )
    source_files = tuple(records)
    if len(source_files) != BAR_SOURCE_FILE_COUNT:
        raise BarPipelineError(
            f"source manifest must contain exactly {BAR_SOURCE_FILE_COUNT} files"
        )
    dates = tuple(item.source_date for item in source_files)
    if dates != tuple(sorted(set(dates))):
        raise BarPipelineError("source manifest dates must be unique and strictly increasing")
    if dates[0] != date(2022, 1, 2) or dates[-1] != date(2026, 7, 31):
        raise BarPipelineError("source manifest date extent differs from the frozen campaign")
    return BarDatasetPlan(
        project_root=root,
        data_root=data_root,
        source_manifest_path=manifest,
        source_manifest_sha256=digest,
        source_files=source_files,
    )


def _report_payload(report: DailyBarBuildReport) -> dict[str, object]:
    return {
        "artifacts": [item.semantic_dict() for item in report.artifacts],
        "bad_ts_recv_trades_excluded": report.bad_ts_recv_trades_excluded,
        "current_volume_summary": (
            None
            if report.current_volume_summary is None
            else report.current_volume_summary.as_dict()
        ),
        "link_sha256_by_timeframe": [
            {"sha256": digest, "timeframe_seconds": timeframe}
            for timeframe, digest in report.link_sha256_by_timeframe
        ],
        "plan": report.plan.as_dict(),
        "report_sha256": report.sha256,
        "segment_tail": None if report.segment_tail is None else report.segment_tail.as_dict(),
        "selected_trade_count": report.selected_trade_count,
        "source_row_count": report.source_row_count,
        "source_scanned": report.source_scanned,
        "source_trade_count": report.source_trade_count,
    }


def execute_bar_dataset_build(
    plan: BarDatasetPlan,
    *,
    progress: Callable[[int, int, DailyBarBuildReport], None] | None = None,
    daily_builder: Callable[..., DailyBarBuildReport] = build_daily_trade_bar_artifacts,
) -> BarDatasetBuildResult:
    """Build every daily partition sequentially and seal a disposition-free manifest."""

    if not isinstance(plan, BarDatasetPlan):
        raise BarPipelineError("plan must be a BarDatasetPlan")
    previous_volume: DailyVolumeSummary | None = None
    previous_tail: SegmentTail | None = None
    reports: list[DailyBarBuildReport] = []
    active_dates: list[date] = []
    for ordinal, source in enumerate(plan.source_files, start=1):
        report = daily_builder(
            source.path,
            data_root=plan.data_root,
            source_date=source.source_date,
            verified_source_sha256=source.sha256,
            previous_volume_summary=previous_volume,
            previous_segment_tail=previous_tail,
        )
        if not isinstance(report, DailyBarBuildReport):
            raise BarPipelineError("daily builder returned the wrong result type")
        if (
            report.plan.source_date != source.source_date
            or report.plan.source_sha256 != source.sha256
        ):
            raise BarPipelineError("daily report differs from its frozen raw source")
        if report.artifacts:
            observed_timeframes = tuple(item.timeframe_seconds for item in report.artifacts)
            if observed_timeframes != SUPPORTED_TIMEFRAMES_SECONDS:
                raise BarPipelineError("a daily bar publication is only partially complete")
            active_dates.append(source.source_date)
        if report.current_volume_summary is not None:
            previous_volume = report.current_volume_summary
        previous_tail = report.segment_tail
        reports.append(report)
        if progress is not None:
            progress(ordinal, len(plan.source_files), report)

    report_values = tuple(reports)
    eligible_dates = tuple(active_dates)
    payload = {
        "artifact_schema": BAR_DATASET_MANIFEST_SCHEMA,
        "bar_version": BAR_VERSION,
        "build_plan_sha256": plan.sha256,
        "eligible_active_date_count": len(eligible_dates),
        "eligible_active_dates": [item.isoformat() for item in eligible_dates],
        "reports": [_report_payload(item) for item in report_values],
        "source_file_count": len(plan.source_files),
        "source_manifest_sha256": plan.source_manifest_sha256,
        "timeframes_seconds": list(SUPPORTED_TIMEFRAMES_SECONDS),
        "totals": {
            "artifact_count": sum(len(item.artifacts) for item in report_values),
            "bad_ts_recv_trades_excluded": sum(
                item.bad_ts_recv_trades_excluded for item in report_values
            ),
            "selected_trade_count": sum(item.selected_trade_count for item in report_values),
            "source_row_count": sum(item.source_row_count for item in report_values),
            "source_trade_count": sum(item.source_trade_count for item in report_values),
        },
    }
    canonical = _canonical_bytes(payload)
    return BarDatasetBuildResult(
        plan=plan,
        reports=report_values,
        eligible_active_dates=eligible_dates,
        canonical_bytes=canonical,
        sha256=_sha256_bytes(canonical),
    )


def _directory_identity(value: os.stat_result) -> tuple[int, int, int]:
    return value.st_dev, value.st_ino, stat.S_IFMT(value.st_mode)


def _file_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _verify_held_manifest_path(
    *,
    directory_descriptors: tuple[int, ...],
    directory_identities: tuple[tuple[int, int, int], ...],
    parent_components: tuple[str, ...],
    leaf_descriptor: int,
    leaf_name: str,
    leaf_identity: tuple[int, int, int, int, int, int],
) -> None:
    if len(directory_descriptors) != len(directory_identities):  # pragma: no cover
        raise BarPipelineError("held manifest directory identity is incomplete")
    for index, descriptor in enumerate(directory_descriptors):
        if _directory_identity(os.fstat(descriptor)) != directory_identities[index]:
            raise BarPipelineError("held manifest directory changed during verification")
        if index == 0:
            bound = os.stat(os.sep, follow_symlinks=False)
        else:
            bound = os.stat(
                parent_components[index - 1],
                dir_fd=directory_descriptors[index - 1],
                follow_symlinks=False,
            )
        if _directory_identity(bound) != directory_identities[index]:
            raise BarPipelineError("manifest ancestor no longer names the held directory")
    if _file_identity(os.fstat(leaf_descriptor)) != leaf_identity:
        raise BarPipelineError("held bar dataset manifest changed during verification")
    bound_leaf = os.stat(
        leaf_name,
        dir_fd=directory_descriptors[-1],
        follow_symlinks=False,
    )
    if _file_identity(bound_leaf) != leaf_identity:
        raise BarPipelineError("manifest path no longer names the held immutable file")


def _read_immutable_manifest(path: Path, *, expected_sha256: str) -> bytes:
    if not isinstance(path, Path) or not path.is_absolute():
        raise BarPipelineError("bar dataset manifest path must be an absolute Path")
    expected = _sha256(expected_sha256, label="expected bar dataset manifest sha256")
    expected_name = f"sha256={expected}.json"
    if path.name != expected_name:
        raise BarPipelineError("bar dataset manifest filename differs from its expected SHA-256")
    if path.anchor != os.sep:
        raise BarPipelineError("bar dataset manifest must use the local absolute path namespace")
    components = tuple(path.parts[1:])
    if not components or any(item in {"", ".", ".."} for item in components):
        raise BarPipelineError("bar dataset manifest path contains an unsafe component")
    parent_components = components[:-1]
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    file_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    directories: list[int] = []
    identities: list[tuple[int, int, int]] = []
    leaf_descriptor = -1
    try:
        root_descriptor = os.open(os.sep, directory_flags)
        directories.append(root_descriptor)
        identities.append(_directory_identity(os.fstat(root_descriptor)))
        for component in parent_components:
            descriptor = os.open(component, directory_flags, dir_fd=directories[-1])
            directories.append(descriptor)
            identities.append(_directory_identity(os.fstat(descriptor)))
        leaf_descriptor = os.open(components[-1], file_flags, dir_fd=directories[-1])
        details = os.fstat(leaf_descriptor)
        if not stat.S_ISREG(details.st_mode):
            raise BarPipelineError("bar dataset manifest must be a regular file")
        if details.st_mode & _WRITE_BITS:
            raise BarPipelineError("bar dataset manifest must be immutable and read-only")
        if details.st_size <= 0 or details.st_size > _MAX_DATASET_MANIFEST_BYTES:
            raise BarPipelineError("bar dataset manifest byte size is outside the safe bound")
        leaf_identity = _file_identity(details)
        held_directories = tuple(directories)
        held_identities = tuple(identities)
        _verify_held_manifest_path(
            directory_descriptors=held_directories,
            directory_identities=held_identities,
            parent_components=parent_components,
            leaf_descriptor=leaf_descriptor,
            leaf_name=components[-1],
            leaf_identity=leaf_identity,
        )
        content = bytearray()
        digest = hashlib.sha256()
        while chunk := os.read(leaf_descriptor, 1024 * 1024):
            content.extend(chunk)
            digest.update(chunk)
            if len(content) > _MAX_DATASET_MANIFEST_BYTES:
                raise BarPipelineError("bar dataset manifest exceeds the safe read bound")
        if len(content) != details.st_size:
            raise BarPipelineError("bar dataset manifest changed while it was read")
        if digest.hexdigest() != expected:
            raise BarPipelineError("bar dataset manifest differs from its expected SHA-256")
        _verify_held_manifest_path(
            directory_descriptors=held_directories,
            directory_identities=held_identities,
            parent_components=parent_components,
            leaf_descriptor=leaf_descriptor,
            leaf_name=components[-1],
            leaf_identity=leaf_identity,
        )
        return bytes(content)
    except BarPipelineError:
        raise
    except OSError as error:
        raise BarPipelineError("bar dataset manifest path is unsafe or inaccessible") from error
    finally:
        if leaf_descriptor >= 0:
            os.close(leaf_descriptor)
        for descriptor in reversed(directories):
            os.close(descriptor)


def _strict_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise BarPipelineError(f"bar dataset manifest contains duplicate key {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise BarPipelineError(f"bar dataset manifest contains invalid JSON constant {value}")


def _parse_canonical_manifest(content: bytes) -> dict[str, object]:
    try:
        text = content.decode("ascii")
        value = json.loads(
            text,
            object_pairs_hook=_strict_json_object,
            parse_constant=_reject_json_constant,
        )
    except BarPipelineError:
        raise
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise BarPipelineError("bar dataset manifest is not strict JSON") from error
    if not isinstance(value, dict):
        raise BarPipelineError("bar dataset manifest root must be an object")
    try:
        canonical = _canonical_bytes(value)
    except RecursionError as error:
        raise BarPipelineError("bar dataset manifest exceeds the safe JSON depth") from error
    if canonical != content:
        raise BarPipelineError("bar dataset manifest bytes are not exact canonical JSON")
    return value


def _object(
    value: object,
    *,
    label: str,
    keys: frozenset[str],
) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise BarPipelineError(f"{label} has an invalid exact schema")
    return value


def _list(value: object, *, label: str) -> list[object]:
    if not isinstance(value, list):
        raise BarPipelineError(f"{label} must be a list")
    return value


def _integer(value: object, *, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise BarPipelineError(f"{label} must be an integer >= {minimum}")
    return value


def _optional_integer(
    value: object,
    *,
    label: str,
    minimum: int = 0,
) -> int | None:
    if value is None:
        return None
    return _integer(value, label=label, minimum=minimum)


def _boolean(value: object, *, label: str) -> bool:
    if not isinstance(value, bool):
        raise BarPipelineError(f"{label} must be a boolean")
    return value


def _text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise BarPipelineError(f"{label} must be a canonical non-empty string")
    return value


def _optional_text(value: object, *, label: str) -> str | None:
    if value is None:
        return None
    return _text(value, label=label)


def _date(value: object, *, label: str) -> date:
    if not isinstance(value, str):
        raise BarPipelineError(f"{label} must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise BarPipelineError(f"{label} must be an ISO date") from error
    if parsed.isoformat() != value:
        raise BarPipelineError(f"{label} must be a canonical ISO date")
    return parsed


def _optional_date(value: object, *, label: str) -> date | None:
    if value is None:
        return None
    return _date(value, label=label)


def _daily_plan(value: object, *, report_index: int) -> DailyBarPlan:
    label = f"bar dataset report {report_index} plan"
    document = _object(value, label=label, keys=_PLAN_KEYS)
    status_value = _text(document["status"], label=f"{label} status")
    try:
        status = DailyPlanStatus(status_value)
        plan = DailyBarPlan(
            source_date=_date(document["source_date"], label=f"{label} source_date"),
            source_sha256=_sha256(document["source_sha256"], label=f"{label} source_sha256"),
            status=status,
            mapping_sha256=_sha256(
                document["mapping_sha256"],
                label=f"{label} mapping_sha256",
            ),
            previous_volume_sha256=(
                None
                if document["previous_volume_sha256"] is None
                else _sha256(
                    document["previous_volume_sha256"],
                    label=f"{label} previous_volume_sha256",
                )
            ),
            previous_source_date=_optional_date(
                document["previous_source_date"],
                label=f"{label} previous_source_date",
            ),
            selected_instrument_id=_optional_integer(
                document["selected_instrument_id"],
                label=f"{label} selected_instrument_id",
            ),
            selected_contract=_optional_text(
                document["selected_contract"],
                label=f"{label} selected_contract",
            ),
            selected_contract_month=_optional_date(
                document["selected_contract_month"],
                label=f"{label} selected_contract_month",
            ),
            selected_previous_trade_count=_optional_integer(
                document["selected_previous_trade_count"],
                label=f"{label} selected_previous_trade_count",
                minimum=1,
            ),
            selected_previous_volume=_optional_integer(
                document["selected_previous_volume"],
                label=f"{label} selected_previous_volume",
                minimum=1,
            ),
            previous_segment_tail_sha256=(
                None
                if document["previous_segment_tail_sha256"] is None
                else _sha256(
                    document["previous_segment_tail_sha256"],
                    label=f"{label} previous_segment_tail_sha256",
                )
            ),
            gap_break_seconds=_integer(
                document["gap_break_seconds"],
                label=f"{label} gap_break_seconds",
                minimum=1,
            ),
        )
    except (TradeBarError, ValueError) as error:
        raise BarPipelineError(f"{label} violates the daily plan contract") from error
    if plan.as_dict() != document:
        raise BarPipelineError(f"{label} differs from its canonical plan identity")
    return plan


def _volume_summary(
    value: object,
    *,
    report_index: int,
) -> DailyVolumeSummary | None:
    if value is None:
        return None
    label = f"bar dataset report {report_index} volume summary"
    document = _object(value, label=label, keys=_VOLUME_KEYS)
    contracts: list[ContractVolume] = []
    for contract_index, raw_contract in enumerate(
        _list(document["contracts"], label=f"{label} contracts"),
        start=1,
    ):
        contract_label = f"{label} contract {contract_index}"
        contract = _object(
            raw_contract,
            label=contract_label,
            keys=_CONTRACT_VOLUME_KEYS,
        )
        try:
            restored = ContractVolume(
                contract_month=_date(
                    contract["contract_month"],
                    label=f"{contract_label} contract_month",
                ),
                instrument_ids=tuple(
                    _integer(
                        item,
                        label=f"{contract_label} instrument_id",
                    )
                    for item in _list(
                        contract["instrument_ids"],
                        label=f"{contract_label} instrument_ids",
                    )
                ),
                raw_symbols=tuple(
                    _text(item, label=f"{contract_label} raw_symbol")
                    for item in _list(
                        contract["raw_symbols"],
                        label=f"{contract_label} raw_symbols",
                    )
                ),
                trade_count=_integer(
                    contract["trade_count"],
                    label=f"{contract_label} trade_count",
                ),
                volume=_integer(
                    contract["volume"],
                    label=f"{contract_label} volume",
                ),
            )
        except TradeBarError as error:
            raise BarPipelineError(f"{contract_label} violates the volume contract") from error
        if restored.as_dict() != contract:
            raise BarPipelineError(f"{contract_label} is not canonical")
        contracts.append(restored)
    try:
        summary = DailyVolumeSummary(
            source_date=_date(
                document["source_date"],
                label=f"{label} source_date",
            ),
            source_sha256=_sha256(
                document["source_sha256"],
                label=f"{label} source_sha256",
            ),
            qc_eligible=_boolean(
                document["qc_eligible"],
                label=f"{label} qc_eligible",
            ),
            contracts=tuple(contracts),
        )
    except TradeBarError as error:
        raise BarPipelineError(f"{label} violates the daily volume contract") from error
    if summary.as_dict() != document:
        raise BarPipelineError(f"{label} differs from its canonical identity")
    return summary


def _segment_tail(value: object, *, report_index: int) -> SegmentTail | None:
    if value is None:
        return None
    label = f"bar dataset report {report_index} segment tail"
    document = _object(value, label=label, keys=_SEGMENT_TAIL_KEYS)
    try:
        tail = SegmentTail(
            contract=_text(document["contract"], label=f"{label} contract"),
            segment_id=_integer(
                document["segment_id"],
                label=f"{label} segment_id",
                minimum=1,
            ),
            source_date=_date(
                document["source_date"],
                label=f"{label} source_date",
            ),
            last_bar_end_ns=_integer(
                document["last_bar_end_ns"],
                label=f"{label} last_bar_end_ns",
                minimum=1,
            ),
            last_trade_ns=_integer(
                document["last_trade_ns"],
                label=f"{label} last_trade_ns",
            ),
        )
    except TradeBarError as error:
        raise BarPipelineError(f"{label} violates the segment-tail contract") from error
    if tail.as_dict() != document:
        raise BarPipelineError(f"{label} is not canonical")
    return tail


def _artifact_descriptors(
    value: object,
    *,
    report_index: int,
) -> tuple[TradeBarArtifactDescriptor, ...]:
    label = f"bar dataset report {report_index} artifacts"
    restored: list[TradeBarArtifactDescriptor] = []
    for artifact_index, raw_artifact in enumerate(_list(value, label=label), start=1):
        artifact_label = f"{label} item {artifact_index}"
        document = _object(raw_artifact, label=artifact_label, keys=_ARTIFACT_KEYS)
        try:
            descriptor = TradeBarArtifactDescriptor.from_mapping(document)
        except TradeBarError as error:
            raise BarPipelineError(f"{artifact_label} violates the artifact contract") from error
        timeframe_label = TIMEFRAME_LABELS[descriptor.timeframe_seconds]
        expected_uri = (
            f"derived/trade_bars/version={BAR_VERSION}/timeframe={timeframe_label}/"
            f"sha256={descriptor.sha256}.parquet"
        )
        relative = PurePosixPath(descriptor.relative_uri)
        if (
            descriptor.relative_uri != expected_uri
            or relative.is_absolute()
            or ".." in relative.parts
            or relative.as_posix() != descriptor.relative_uri
            or descriptor.semantic_dict() != document
        ):
            raise BarPipelineError(f"{artifact_label} path or descriptor identity drift")
        restored.append(descriptor)
    timeframes = tuple(item.timeframe_seconds for item in restored)
    if timeframes not in {(), SUPPORTED_TIMEFRAMES_SECONDS}:
        raise BarPipelineError(f"{label} must be empty or contain the exact timeframe set")
    return tuple(restored)


def _links(
    value: object,
    *,
    report_index: int,
) -> tuple[tuple[int, str], ...]:
    label = f"bar dataset report {report_index} next-bar links"
    restored: list[tuple[int, str]] = []
    for link_index, raw_link in enumerate(_list(value, label=label), start=1):
        link_label = f"{label} item {link_index}"
        document = _object(raw_link, label=link_label, keys=_LINK_KEYS)
        restored.append(
            (
                _integer(
                    document["timeframe_seconds"],
                    label=f"{link_label} timeframe_seconds",
                    minimum=1,
                ),
                _sha256(document["sha256"], label=f"{link_label} sha256"),
            )
        )
    return tuple(restored)


@dataclass(frozen=True, slots=True)
class _ValidatedBarReport:
    plan: DailyBarPlan
    volume_summary: DailyVolumeSummary | None
    segment_tail: SegmentTail | None
    artifacts: tuple[TradeBarArtifactDescriptor, ...]
    artifact_count: int
    bad_ts_recv_trades_excluded: int
    selected_trade_count: int
    source_row_count: int
    source_trade_count: int


@dataclass(frozen=True, slots=True)
class _OutcomeSpanState:
    active_contract: str | None = None
    outcome_span_id: int = 0
    break_before_next_active: bool = False


def _advance_outcome_span(
    state: _OutcomeSpanState,
    *,
    status: DailyPlanStatus,
    selected_contract: str | None,
    has_artifacts: bool,
) -> tuple[_OutcomeSpanState, int | None]:
    """Apply the frozen quality/contract boundary policy to one ordered report."""

    if not isinstance(state, _OutcomeSpanState):
        raise BarPipelineError("outcome span state has the wrong type")
    if not isinstance(status, DailyPlanStatus) or not isinstance(has_artifacts, bool):
        raise BarPipelineError("outcome span observation has invalid types")
    if status is not DailyPlanStatus.SELECTED:
        if has_artifacts or selected_contract is not None:
            raise BarPipelineError("an unqualified plan cannot publish an outcome partition")
        return (
            _OutcomeSpanState(
                active_contract=state.active_contract,
                outcome_span_id=state.outcome_span_id,
                break_before_next_active=True,
            ),
            None,
        )
    contract = _text(selected_contract, label="selected outcome-span contract")
    if not has_artifacts:
        contract_changed = state.active_contract is not None and contract != state.active_contract
        return (
            _OutcomeSpanState(
                active_contract=state.active_contract,
                outcome_span_id=state.outcome_span_id,
                break_before_next_active=(state.break_before_next_active or contract_changed),
            ),
            None,
        )
    begins_new_span = (
        state.outcome_span_id == 0
        or state.break_before_next_active
        or contract != state.active_contract
    )
    assigned = state.outcome_span_id + int(begins_new_span)
    return (
        _OutcomeSpanState(
            active_contract=contract,
            outcome_span_id=assigned,
            break_before_next_active=False,
        ),
        assigned,
    )


def _validated_report(value: object, *, report_index: int) -> _ValidatedBarReport:
    label = f"bar dataset report {report_index}"
    document = _object(value, label=label, keys=_REPORT_KEYS)
    plan = _daily_plan(document["plan"], report_index=report_index)
    volume = _volume_summary(document["current_volume_summary"], report_index=report_index)
    tail = _segment_tail(document["segment_tail"], report_index=report_index)
    artifacts = _artifact_descriptors(document["artifacts"], report_index=report_index)
    links = _links(document["link_sha256_by_timeframe"], report_index=report_index)
    source_scanned = _boolean(
        document["source_scanned"],
        label=f"{label} source_scanned",
    )
    source_row_count = _integer(
        document["source_row_count"],
        label=f"{label} source_row_count",
    )
    source_trade_count = _integer(
        document["source_trade_count"],
        label=f"{label} source_trade_count",
    )
    selected_trade_count = _integer(
        document["selected_trade_count"],
        label=f"{label} selected_trade_count",
    )
    excluded_count = _integer(
        document["bad_ts_recv_trades_excluded"],
        label=f"{label} bad_ts_recv_trades_excluded",
    )
    if not (
        selected_trade_count <= source_trade_count <= source_row_count
        and excluded_count <= source_trade_count
    ):
        raise BarPipelineError(f"{label} count ordering is inconsistent")
    qc_excluded = plan.status is DailyPlanStatus.QC_EXCLUDED
    if source_scanned == qc_excluded:
        raise BarPipelineError(f"{label} source-scanned state differs from its plan")
    if (volume is None) != qc_excluded:
        raise BarPipelineError(f"{label} volume evidence differs from its QC state")
    if volume is not None and (
        not volume.qc_eligible
        or volume.source_date != plan.source_date
        or volume.source_sha256 != plan.source_sha256
    ):
        raise BarPipelineError(f"{label} volume evidence differs from its raw source")
    if bool(artifacts) != (selected_trade_count > 0):
        raise BarPipelineError(f"{label} artifact presence differs from selected trades")
    if selected_trade_count > 0 and plan.status is not DailyPlanStatus.SELECTED:
        raise BarPipelineError(f"{label} published bars without a selected plan")
    artifact_timeframes = tuple(item.timeframe_seconds for item in artifacts)
    link_timeframes = tuple(item[0] for item in links)
    if link_timeframes != artifact_timeframes:
        raise BarPipelineError(f"{label} next-bar links differ from its artifacts")
    report_payload = {
        "artifact_schema": REPORT_SCHEMA,
        "artifacts": [item.semantic_dict() for item in artifacts],
        "bad_ts_recv_trades_excluded": excluded_count,
        "bar_version": BAR_VERSION,
        "current_volume_sha256": None if volume is None else volume.sha256,
        "link_sha256_by_timeframe": [
            {"sha256": digest, "timeframe_seconds": timeframe} for timeframe, digest in links
        ],
        "plan_sha256": plan.sha256,
        "segment_tail": None if tail is None else tail.as_dict(),
        "selected_trade_count": selected_trade_count,
        "source_row_count": source_row_count,
        "source_scanned": source_scanned,
        "source_trade_count": source_trade_count,
    }
    report_sha256 = _sha256(
        document["report_sha256"],
        label=f"{label} report_sha256",
    )
    if bar_canonical_sha256(report_payload) != report_sha256:
        raise BarPipelineError(f"{label} differs from its canonical report SHA-256")
    return _ValidatedBarReport(
        plan=plan,
        volume_summary=volume,
        segment_tail=tail,
        artifacts=artifacts,
        artifact_count=len(artifacts),
        bad_ts_recv_trades_excluded=excluded_count,
        selected_trade_count=selected_trade_count,
        source_row_count=source_row_count,
        source_trade_count=source_trade_count,
    )


def load_bar_dataset_manifest(
    manifest_path: Path,
    *,
    expected_sha256: str,
) -> LoadedBarDatasetManifest:
    """Read and fully validate the frozen trade-bar dataset manifest without writes."""

    digest = _sha256(expected_sha256, label="expected bar dataset manifest sha256")
    content = _read_immutable_manifest(manifest_path, expected_sha256=digest)
    document = _parse_canonical_manifest(content)
    root = _object(document, label="bar dataset manifest", keys=_TOP_LEVEL_KEYS)
    if root["artifact_schema"] != BAR_DATASET_MANIFEST_SCHEMA:
        raise BarPipelineError("bar dataset manifest schema differs from the frozen contract")
    if root["bar_version"] != BAR_VERSION:
        raise BarPipelineError("bar dataset manifest bar version drift")
    if _sha256(root["build_plan_sha256"], label="bar dataset build_plan_sha256") != (
        BAR_DATASET_BUILD_PLAN_SHA256
    ):
        raise BarPipelineError("bar dataset build plan differs from the frozen identity")
    if (
        _sha256(
            root["source_manifest_sha256"],
            label="bar dataset source_manifest_sha256",
        )
        != BAR_SOURCE_MANIFEST_SHA256
    ):
        raise BarPipelineError("bar dataset source manifest differs from the frozen identity")
    if _integer(root["source_file_count"], label="bar dataset source_file_count") != (
        BAR_DATASET_REPORT_COUNT
    ):
        raise BarPipelineError("bar dataset source file count differs from the frozen campaign")
    timeframes = tuple(
        _integer(item, label="bar dataset timeframe", minimum=1)
        for item in _list(root["timeframes_seconds"], label="bar dataset timeframes")
    )
    if timeframes != SUPPORTED_TIMEFRAMES_SECONDS:
        raise BarPipelineError("bar dataset timeframes differ from the frozen bar version")
    report_documents = _list(root["reports"], label="bar dataset reports")
    if len(report_documents) != BAR_DATASET_REPORT_COUNT:
        raise BarPipelineError("bar dataset must contain exactly 1,434 daily reports")

    report_dates: list[date] = []
    partitions: list[BarDatasetPartition] = []
    previous_volume: DailyVolumeSummary | None = None
    previous_tail: SegmentTail | None = None
    outcome_span_state = _OutcomeSpanState()
    aggregate = {key: 0 for key in _TOTAL_KEYS}
    for report_index, raw_report in enumerate(report_documents, start=1):
        report = _validated_report(raw_report, report_index=report_index)
        expected_volume_sha256 = None if previous_volume is None else previous_volume.sha256
        expected_volume_date = None if previous_volume is None else previous_volume.source_date
        if report.plan.status is DailyPlanStatus.QC_EXCLUDED:
            volume_lineage_matches = (
                report.plan.previous_volume_sha256 is None
                and report.plan.previous_source_date is None
            )
        else:
            volume_lineage_matches = (
                report.plan.previous_volume_sha256 == expected_volume_sha256
                and report.plan.previous_source_date == expected_volume_date
            )
        if not volume_lineage_matches:
            raise BarPipelineError(
                f"bar dataset report {report_index} previous-volume lineage drift"
            )
        expected_tail_sha256 = None if previous_tail is None else previous_tail.sha256
        if report.plan.previous_segment_tail_sha256 != expected_tail_sha256:
            raise BarPipelineError(
                f"bar dataset report {report_index} previous-segment lineage drift"
            )
        if not report.artifacts:
            if report.segment_tail != previous_tail:
                raise BarPipelineError(
                    f"bar dataset report {report_index} changed a segment without bars"
                )
        elif (
            report.segment_tail is None
            or report.segment_tail.source_date != report.plan.source_date
            or report.segment_tail.contract != report.plan.selected_contract
        ):
            raise BarPipelineError(f"bar dataset report {report_index} active segment-tail drift")
        report_dates.append(report.plan.source_date)
        outcome_span_state, assigned_span_id = _advance_outcome_span(
            outcome_span_state,
            status=report.plan.status,
            selected_contract=report.plan.selected_contract,
            has_artifacts=bool(report.artifacts),
        )
        if assigned_span_id is not None:
            contract = report.plan.selected_contract
            if contract is None:  # Guarded by `_advance_outcome_span`.
                raise AssertionError("active outcome partition lost its selected contract")
            partitions.append(
                BarDatasetPartition(
                    source_date=report.plan.source_date,
                    contract=contract,
                    outcome_span_id=assigned_span_id,
                    plan_sha256=report.plan.sha256,
                    source_sha256=report.plan.source_sha256,
                    artifacts=report.artifacts,
                )
            )
        if report.volume_summary is not None:
            previous_volume = report.volume_summary
        previous_tail = report.segment_tail
        aggregate["artifact_count"] += report.artifact_count
        aggregate["bad_ts_recv_trades_excluded"] += report.bad_ts_recv_trades_excluded
        aggregate["selected_trade_count"] += report.selected_trade_count
        aggregate["source_row_count"] += report.source_row_count
        aggregate["source_trade_count"] += report.source_trade_count

    ordered_report_dates = tuple(report_dates)
    if (
        ordered_report_dates != tuple(sorted(set(ordered_report_dates)))
        or ordered_report_dates[0] != BAR_DATASET_FIRST_SOURCE_DATE
        or ordered_report_dates[-1] != BAR_DATASET_LAST_SOURCE_DATE
    ):
        raise BarPipelineError("bar dataset report dates are incomplete or unordered")
    active_dates = tuple(
        _date(item, label="bar dataset eligible active date")
        for item in _list(
            root["eligible_active_dates"],
            label="bar dataset eligible_active_dates",
        )
    )
    active_count = _integer(
        root["eligible_active_date_count"],
        label="bar dataset eligible_active_date_count",
    )
    partition_dates = tuple(item.source_date for item in partitions)
    if (
        active_count != BAR_DATASET_ACTIVE_DATE_COUNT
        or len(active_dates) != BAR_DATASET_ACTIVE_DATE_COUNT
        or active_dates != tuple(sorted(set(active_dates)))
        or active_dates != partition_dates
    ):
        raise BarPipelineError("bar dataset active-date handoff is incomplete or unordered")
    if aggregate["artifact_count"] != BAR_DATASET_ARTIFACT_COUNT:
        raise BarPipelineError("bar dataset artifact count differs from the frozen campaign")
    totals = _object(root["totals"], label="bar dataset totals", keys=_TOTAL_KEYS)
    parsed_totals = {
        key: _integer(totals[key], label=f"bar dataset totals {key}") for key in sorted(_TOTAL_KEYS)
    }
    if parsed_totals != aggregate:
        raise BarPipelineError("bar dataset aggregate totals differ from its daily reports")
    return LoadedBarDatasetManifest(
        dataset_manifest_sha256=digest,
        source_manifest_sha256=BAR_SOURCE_MANIFEST_SHA256,
        outcome_span_policy_sha256=BAR_DATASET_OUTCOME_SPAN_POLICY_SHA256,
        eligible_active_dates=active_dates,
        partitions=tuple(partitions),
    )
