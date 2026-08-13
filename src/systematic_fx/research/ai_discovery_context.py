"""Bounded, outcome-free AI context for bar-pattern hypothesis discovery.

The public builder accepts only a project root.  It reopens the one approved
trade-bar manifest, derives the preregistered split, and reads only the 5-minute
artifacts assigned to visible Discovery.  The published JSON contains completed
bar morphology, calendar summaries, and a finite threshold/support lattice.  It
does not contain source paths, contracts, prices, executions, or evaluation
fields.

This module deliberately stops at hypothesis context.  It neither evaluates a
pattern nor registers/promotes research state.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Final

from systematic_fx.features.bars import (
    BAR_VERSION,
    ONE_SECOND_NS,
    TradeBar,
    TradeBarArtifactDescriptor,
    load_trade_bar_artifact,
)
from systematic_fx.research.bar_artifacts import (
    BarArtifactDescriptor,
    PublishedBarArtifact,
    open_verified_bar_artifact,
    publish_bar_artifact_bytes,
)
from systematic_fx.research.bar_config import BAR_SOURCE_MANIFEST_SHA256
from systematic_fx.research.bar_pipeline import (
    BarDatasetPartition,
    BarPipelineError,
    load_bar_dataset_manifest,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.validation.bar_splits import BarSplitPlan, plan_bar_splits

AI_DISCOVERY_CONTEXT_SCHEMA: Final = "systematic_fx.ai_discovery_context.v1"
AI_DISCOVERY_CONTEXT_ARTIFACT_SCHEMA: Final = "systematic_fx.ai_discovery_context_artifact.v1"
AI_MORPHOLOGY_VERSION: Final = "completed_5m_bar_morphology_v1"

EXPECTED_DATASET_MANIFEST_SHA256: Final = (
    "e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc"
)
EXPECTED_DATASET_HANDOFF_SHA256: Final = (
    "26b1bb96f7323cae13bbe5d670c12f3e85615bbb9aab56932ce6523e67af7b00"
)
EXPECTED_SPLIT_PLAN_SHA256: Final = (
    "5594725f6769a706018d414a5b27e3903f1d7d1cc22c98e93b6e973ead1af043"
)
EXPECTED_DISCOVERY_CALENDAR_SHA256: Final = (
    "88a28f1d66d0476629ea8fa0faa0a5c95e946756b191da7effbb11de805c1684"
)
EXPECTED_DISCOVERY_START_DATE: Final = date(2022, 1, 3)
EXPECTED_DISCOVERY_END_DATE: Final = date(2023, 8, 2)
EXPECTED_DECISION_END_DATE: Final = date(2023, 7, 10)
EXPECTED_DISCOVERY_ACTIVE_DAYS: Final = 489
EXPECTED_DISCOVERY_DECISION_DAYS: Final = 469
EXPECTED_DISCOVERY_BAR_ROWS: Final = 111_297
EXPECTED_DISCOVERY_SOURCE_BYTES: Final = 11_227_098
EXPECTED_REPORTING_BLOCK_LENGTHS: Final = (118, 117, 117, 117)
EXPECTED_AI_DISCOVERY_CONTEXT_SHA256: Final = (
    "a7219ac7c2a27f16cdbdfae58a9fe17c4d69372d315444987cc3605c4ff633a4"
)
EXPECTED_AI_DISCOVERY_CONTEXT_IDENTITY_SHA256: Final = (
    "12de34f325b5788330401e10275cad8e06471d589f561b003387d220c25806cd"
)
EXPECTED_AI_DISCOVERY_CONTEXT_BYTES: Final = 9_971_349

TIMEFRAME_SECONDS: Final = 300
RATIO_SCALE: Final = 1_000_000
MAX_BAR_ROWS: Final = 120_000
MAX_SOURCE_ARTIFACT_BYTES: Final = 16 * 1024 * 1024
MAX_CONTEXT_BYTES: Final = 32 * 1024 * 1024
MAX_THRESHOLD_SUPPORT_ROWS: Final = 64
MAX_CANDIDATES: Final = 64
MAX_CONDITIONS_PER_CANDIDATE: Final = 3
MINIMUM_ACTIVE_DATE_OPTIONS: Final = (20, 40, 80, 120)

_MANIFEST_RELATIVE_PATH: Final = Path(
    "data/derived/bar_patterns/trade_bar_dataset_manifest/"
    "identity_sha256=b0ecab04cdd3626d3c488f9108c8e9184f5dd610f51950ab7e7f74a5b7524297/"
    f"sha256={EXPECTED_DATASET_MANIFEST_SHA256}.json"
)
_SHA256_LENGTH: Final = 64

BAR_COLUMNS: Final = (
    "source_date",
    "start_ns",
    "end_ns",
    "block_number",
    "decision_eligible",
    "range_ticks",
    "signed_body_ppm",
    "close_location_ppm",
    "upper_wick_ppm",
    "lower_wick_ppm",
)
_AGGREGATE_COLUMNS: Final = (
    "bar_count",
    "first_start_ns",
    "last_end_ns",
    "positive_body_count",
    "negative_body_count",
    "flat_body_count",
    "zero_range_count",
    "range_ticks_min",
    "range_ticks_max",
    "range_ticks_sum",
    "absolute_body_ppm_sum",
    "close_location_ppm_sum",
    "upper_wick_ppm_sum",
    "lower_wick_ppm_sum",
)
DAILY_SUMMARY_COLUMNS: Final = (
    "source_date",
    "block_number",
    "decision_eligible",
    *_AGGREGATE_COLUMNS,
)
BLOCK_SUMMARY_COLUMNS: Final = (
    "block_number",
    "start_date",
    "end_date",
    "active_date_count",
    *_AGGREGATE_COLUMNS,
)
THRESHOLD_SUPPORT_COLUMNS: Final = (
    "feature",
    "operator",
    "threshold",
    "bar_count",
    "active_date_count",
    "block_1_bar_count",
    "block_2_bar_count",
    "block_3_bar_count",
    "block_4_bar_count",
)


@dataclass(frozen=True, slots=True)
class _ThresholdAxis:
    feature: str
    operator: str
    thresholds: tuple[int, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "feature": self.feature,
            "operator": self.operator,
            "thresholds": list(self.thresholds),
        }


_THRESHOLD_AXES: Final = (
    _ThresholdAxis("range_ticks", "GE", (1, 2, 4, 8, 12, 16, 24, 32)),
    _ThresholdAxis(
        "absolute_body_ppm",
        "GE",
        (100_000, 250_000, 500_000, 750_000, 900_000),
    ),
    _ThresholdAxis("signed_body_ppm", "GE", (100_000, 250_000, 500_000, 750_000)),
    _ThresholdAxis(
        "signed_body_ppm",
        "LE",
        (-900_000, -750_000, -500_000, -250_000, -100_000),
    ),
    _ThresholdAxis("close_location_ppm", "GE", (600_000, 700_000, 800_000, 900_000)),
    _ThresholdAxis("close_location_ppm", "LE", (100_000, 200_000, 300_000, 400_000)),
    _ThresholdAxis("upper_wick_ppm", "GE", (100_000, 250_000, 500_000, 750_000)),
    _ThresholdAxis("lower_wick_ppm", "GE", (100_000, 250_000, 500_000, 750_000)),
)
_LATTICE_DEFINITION: Final = {
    "axes": [axis.as_dict() for axis in _THRESHOLD_AXES],
    "maximum_candidates": MAX_CANDIDATES,
    "maximum_conditions_per_candidate": MAX_CONDITIONS_PER_CANDIDATE,
    "minimum_active_date_options": list(MINIMUM_ACTIVE_DATE_OPTIONS),
}
THRESHOLD_LATTICE_SHA256: Final = canonical_sha256(_LATTICE_DEFINITION)

_AUTHORITY: Final = {
    "content_policy": "COMPLETED_5M_BAR_MORPHOLOGY_ONLY",
    "data_role": "DISCOVERY",
    "maximum_status": "HYPOTHESIS_CONTEXT_ONLY",
    "visibility": "VISIBLE",
}
_MORPHOLOGY_CONTRACT: Final = {
    "availability": "AFTER_BAR_CLOSE",
    "feature_version": AI_MORPHOLOGY_VERSION,
    "integer_ratio_scale": RATIO_SCALE,
    "integer_rounding": "TRUNCATE_TOWARD_ZERO",
    "lattice_sha256": THRESHOLD_LATTICE_SHA256,
    "maximum_bar_rows": MAX_BAR_ROWS,
    "maximum_source_artifact_bytes": MAX_SOURCE_ARTIFACT_BYTES,
    "zero_range_policy": "MIDPOINT_LOCATION_AND_ZERO_BODY_WICKS",
}
_TOP_LEVEL_KEYS: Final = frozenset(
    {
        "authority",
        "bar_columns",
        "bars",
        "block_summaries",
        "block_summary_columns",
        "daily_summaries",
        "daily_summary_columns",
        "morphology",
        "schema",
        "source",
        "threshold_lattice",
    }
)
_SOURCE_KEYS: Final = frozenset(
    {
        "active_date_count",
        "bar_row_count",
        "bar_version",
        "dataset_handoff_sha256",
        "dataset_manifest_sha256",
        "decision_date_count",
        "decision_end_date",
        "discovery_calendar_sha256",
        "discovery_end_date",
        "discovery_start_date",
        "raw_source_manifest_sha256",
        "reporting_block_count",
        "source_artifact_byte_count",
        "split_plan_sha256",
        "timeframe_seconds",
    }
)
_THRESHOLD_LATTICE_KEYS: Final = frozenset(
    {
        "axes",
        "maximum_candidates",
        "maximum_conditions_per_candidate",
        "minimum_active_date_options",
        "support_columns",
        "supports",
    }
)
_FORBIDDEN_EXACT_KEYS: Final = frozenset(
    {
        "buy_volume",
        "close_ticks",
        "contract",
        "data_root",
        "first_trade_ns",
        "high_ticks",
        "last_trade_ns",
        "low_ticks",
        "observed_subbars",
        "open_ticks",
        "path",
        "relative_uri",
        "segment_id",
        "sell_volume",
        "trade_count",
        "volume",
    }
)
_FORBIDDEN_TOKENS: Final = (
    "future",
    "holdout",
    "label",
    "next_bar",
    "outcome",
    "pnl",
    "profit",
    "result",
    "target",
    "walk_forward",
)
_SCHEMA_IDENTITY: Final = {
    "artifact_schema": AI_DISCOVERY_CONTEXT_SCHEMA,
    "bar_columns": list(BAR_COLUMNS),
    "block_summary_columns": list(BLOCK_SUMMARY_COLUMNS),
    "daily_summary_columns": list(DAILY_SUMMARY_COLUMNS),
    "exact_top_level_keys": sorted(_TOP_LEVEL_KEYS),
    "source_keys": sorted(_SOURCE_KEYS),
    "support_columns": list(THRESHOLD_SUPPORT_COLUMNS),
    "threshold_lattice_keys": sorted(_THRESHOLD_LATTICE_KEYS),
}
AI_DISCOVERY_CONTEXT_SCHEMA_SHA256: Final = canonical_sha256(_SCHEMA_IDENTITY)


class AIDiscoveryContextError(RuntimeError):
    """The bounded Discovery projection or its immutable artifact is invalid."""


@dataclass(frozen=True, slots=True)
class AIDiscoveryContextArtifact:
    """A content-addressed context artifact safe to hand to a hypothesis agent."""

    published: PublishedBarArtifact

    def __post_init__(self) -> None:
        if not isinstance(self.published, PublishedBarArtifact):
            raise AIDiscoveryContextError("published must be a PublishedBarArtifact")
        descriptor = self.published.descriptor
        if (
            descriptor.artifact_type != "ai_discovery_context"
            or descriptor.artifact_schema != AI_DISCOVERY_CONTEXT_SCHEMA
            or descriptor.schema_sha256 != AI_DISCOVERY_CONTEXT_SCHEMA_SHA256
        ):
            raise AIDiscoveryContextError("published artifact is not an AI Discovery context")

    @property
    def path(self) -> Path:
        return self.published.path

    @property
    def sha256(self) -> str:
        return self.published.sha256

    @property
    def byte_size(self) -> int:
        return self.published.byte_size

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-safe identity without serializing a filesystem path."""

        return {
            "artifact_identity_sha256": self.published.descriptor.identity_sha256,
            "byte_size": self.byte_size,
            "content_sha256": self.sha256,
            "schema": AI_DISCOVERY_CONTEXT_ARTIFACT_SCHEMA,
        }

    @classmethod
    def from_dict(
        cls,
        project_root: Path | str,
        value: Mapping[str, object],
    ) -> AIDiscoveryContextArtifact:
        """Reconstruct and verify the frozen content address from path-free identity."""

        return load_ai_discovery_context(project_root, identity=value)


@dataclass(frozen=True, slots=True)
class _ContextSourceSpec:
    dataset_manifest_sha256: str
    dataset_handoff_sha256: str
    raw_source_manifest_sha256: str
    split_plan_sha256: str
    discovery_dates: tuple[date, ...]
    reporting_blocks: tuple[tuple[date, ...], ...]
    expected_bar_row_count: int
    source_artifact_byte_count: int

    def __post_init__(self) -> None:
        for label, value in (
            ("dataset_manifest_sha256", self.dataset_manifest_sha256),
            ("dataset_handoff_sha256", self.dataset_handoff_sha256),
            ("raw_source_manifest_sha256", self.raw_source_manifest_sha256),
            ("split_plan_sha256", self.split_plan_sha256),
        ):
            _sha256(value, label=label)
        if (
            not isinstance(self.discovery_dates, tuple)
            or not self.discovery_dates
            or any(
                isinstance(item, datetime) or not isinstance(item, date)
                for item in self.discovery_dates
            )
            or self.discovery_dates != tuple(sorted(set(self.discovery_dates)))
        ):
            raise AIDiscoveryContextError(
                "Discovery dates must be a non-empty, strictly increasing date tuple"
            )
        if len(self.reporting_blocks) != 4 or any(not block for block in self.reporting_blocks):
            raise AIDiscoveryContextError("exactly four non-empty reporting blocks are required")
        decision_dates = tuple(item for block in self.reporting_blocks for item in block)
        if decision_dates != self.discovery_dates[: len(decision_dates)]:
            raise AIDiscoveryContextError(
                "reporting blocks must exactly partition the Discovery decision prefix"
            )
        if len(decision_dates) >= len(self.discovery_dates):
            raise AIDiscoveryContextError("Discovery must retain a non-decision visibility tail")
        _integer(
            self.expected_bar_row_count,
            label="expected_bar_row_count",
            minimum=1,
            maximum=MAX_BAR_ROWS,
        )
        _integer(
            self.source_artifact_byte_count,
            label="source_artifact_byte_count",
            minimum=1,
            maximum=MAX_SOURCE_ARTIFACT_BYTES,
        )

    @property
    def decision_dates(self) -> tuple[date, ...]:
        return tuple(item for block in self.reporting_blocks for item in block)

    @property
    def decision_end_date(self) -> date:
        return self.decision_dates[-1]

    @property
    def calendar_sha256(self) -> str:
        return canonical_sha256([item.isoformat() for item in self.discovery_dates])

    @property
    def block_number_by_date(self) -> dict[date, int]:
        return {
            item: number
            for number, block in enumerate(self.reporting_blocks, start=1)
            for item in block
        }

    def source_document(self) -> dict[str, object]:
        return {
            "active_date_count": len(self.discovery_dates),
            "bar_row_count": self.expected_bar_row_count,
            "bar_version": BAR_VERSION,
            "dataset_handoff_sha256": self.dataset_handoff_sha256,
            "dataset_manifest_sha256": self.dataset_manifest_sha256,
            "decision_date_count": len(self.decision_dates),
            "decision_end_date": self.decision_end_date.isoformat(),
            "discovery_calendar_sha256": self.calendar_sha256,
            "discovery_end_date": self.discovery_dates[-1].isoformat(),
            "discovery_start_date": self.discovery_dates[0].isoformat(),
            "raw_source_manifest_sha256": self.raw_source_manifest_sha256,
            "reporting_block_count": len(self.reporting_blocks),
            "source_artifact_byte_count": self.source_artifact_byte_count,
            "split_plan_sha256": self.split_plan_sha256,
            "timeframe_seconds": TIMEFRAME_SECONDS,
        }


@dataclass(frozen=True, slots=True)
class _MorphologyRow:
    source_date: date
    start_ns: int
    end_ns: int
    block_number: int | None
    decision_eligible: bool
    range_ticks: int
    signed_body_ppm: int
    close_location_ppm: int
    upper_wick_ppm: int
    lower_wick_ppm: int

    def as_list(self) -> list[object]:
        return [
            self.source_date.isoformat(),
            self.start_ns,
            self.end_ns,
            self.block_number,
            self.decision_eligible,
            self.range_ticks,
            self.signed_body_ppm,
            self.close_location_ppm,
            self.upper_wick_ppm,
            self.lower_wick_ppm,
        ]


@dataclass(slots=True)
class _Aggregate:
    bar_count: int = 0
    first_start_ns: int | None = None
    last_end_ns: int | None = None
    positive_body_count: int = 0
    negative_body_count: int = 0
    flat_body_count: int = 0
    zero_range_count: int = 0
    range_ticks_min: int | None = None
    range_ticks_max: int | None = None
    range_ticks_sum: int = 0
    absolute_body_ppm_sum: int = 0
    close_location_ppm_sum: int = 0
    upper_wick_ppm_sum: int = 0
    lower_wick_ppm_sum: int = 0

    def add(self, row: _MorphologyRow) -> None:
        self.bar_count += 1
        if self.first_start_ns is None:
            self.first_start_ns = row.start_ns
        self.last_end_ns = row.end_ns
        if row.signed_body_ppm > 0:
            self.positive_body_count += 1
        elif row.signed_body_ppm < 0:
            self.negative_body_count += 1
        else:
            self.flat_body_count += 1
        if row.range_ticks == 0:
            self.zero_range_count += 1
        if self.range_ticks_min is None or row.range_ticks < self.range_ticks_min:
            self.range_ticks_min = row.range_ticks
        if self.range_ticks_max is None or row.range_ticks > self.range_ticks_max:
            self.range_ticks_max = row.range_ticks
        self.range_ticks_sum += row.range_ticks
        self.absolute_body_ppm_sum += abs(row.signed_body_ppm)
        self.close_location_ppm_sum += row.close_location_ppm
        self.upper_wick_ppm_sum += row.upper_wick_ppm
        self.lower_wick_ppm_sum += row.lower_wick_ppm

    def as_list(self) -> list[int]:
        if (
            self.bar_count <= 0
            or self.first_start_ns is None
            or self.last_end_ns is None
            or self.range_ticks_min is None
            or self.range_ticks_max is None
        ):
            raise AIDiscoveryContextError("every Discovery summary group must contain bars")
        return [
            self.bar_count,
            self.first_start_ns,
            self.last_end_ns,
            self.positive_body_count,
            self.negative_body_count,
            self.flat_body_count,
            self.zero_range_count,
            self.range_ticks_min,
            self.range_ticks_max,
            self.range_ticks_sum,
            self.absolute_body_ppm_sum,
            self.close_location_ppm_sum,
            self.upper_wick_ppm_sum,
            self.lower_wick_ppm_sum,
        ]


def _sha256(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _SHA256_LENGTH
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise AIDiscoveryContextError(f"{label} must be a lowercase SHA-256")
    return value


def _integer(
    value: object,
    *,
    label: str,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise AIDiscoveryContextError(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        raise AIDiscoveryContextError(f"{label} must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise AIDiscoveryContextError(f"{label} must be <= {maximum}")
    return value


def _canonical_date(value: object, *, label: str) -> date:
    if not isinstance(value, str):
        raise AIDiscoveryContextError(f"{label} must be a canonical ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise AIDiscoveryContextError(f"{label} must be a canonical ISO date") from error
    if parsed.isoformat() != value:
        raise AIDiscoveryContextError(f"{label} must be a canonical ISO date")
    return parsed


def _ratio_ppm(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise AIDiscoveryContextError("morphology ratio denominator must be positive")
    magnitude = (abs(numerator) * RATIO_SCALE) // denominator
    return -magnitude if numerator < 0 else magnitude


def _morphology_row(
    bar: TradeBar,
    *,
    discovery_dates: frozenset[date],
    block_number_by_date: Mapping[date, int],
) -> _MorphologyRow:
    if not isinstance(bar, TradeBar):
        raise AIDiscoveryContextError("verified bar stream must contain TradeBar values")
    if bar.timeframe_seconds != TIMEFRAME_SECONDS:
        raise AIDiscoveryContextError("AI context accepts only five-minute bars")
    block_number = block_number_by_date.get(bar.source_date)
    decision_eligible = block_number is not None
    if bar.source_date not in discovery_dates:
        raise AIDiscoveryContextError("bar date is outside visible Discovery")
    spread = bar.high_ticks - bar.low_ticks
    if spread == 0:
        signed_body = 0
        close_location = RATIO_SCALE // 2
        upper_wick = 0
        lower_wick = 0
    else:
        signed_body = _ratio_ppm(bar.close_ticks - bar.open_ticks, spread)
        close_location = _ratio_ppm(bar.close_ticks - bar.low_ticks, spread)
        upper_wick = _ratio_ppm(
            bar.high_ticks - max(bar.open_ticks, bar.close_ticks),
            spread,
        )
        lower_wick = _ratio_ppm(
            min(bar.open_ticks, bar.close_ticks) - bar.low_ticks,
            spread,
        )
    return _MorphologyRow(
        source_date=bar.source_date,
        start_ns=bar.start_ns,
        end_ns=bar.end_ns,
        block_number=block_number,
        decision_eligible=decision_eligible,
        range_ticks=spread,
        signed_body_ppm=signed_body,
        close_location_ppm=close_location,
        upper_wick_ppm=upper_wick,
        lower_wick_ppm=lower_wick,
    )


def _validate_morphology_rows(
    spec: _ContextSourceSpec,
    rows: Iterable[_MorphologyRow],
) -> tuple[_MorphologyRow, ...]:
    values: list[_MorphologyRow] = []
    previous_start: int | None = None
    discovery_dates = set(spec.discovery_dates)
    block_by_date = spec.block_number_by_date
    width_ns = TIMEFRAME_SECONDS * ONE_SECOND_NS
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, _MorphologyRow):
            raise AIDiscoveryContextError("morphology rows have an invalid type")
        if index > MAX_BAR_ROWS:
            raise AIDiscoveryContextError("AI context exceeds the precommitted bar-row cap")
        if row.source_date not in discovery_dates:
            raise AIDiscoveryContextError("morphology row lies outside visible Discovery")
        expected_block = block_by_date.get(row.source_date)
        if row.block_number != expected_block or row.decision_eligible != (
            expected_block is not None
        ):
            raise AIDiscoveryContextError("morphology row decision/block assignment drift")
        _integer(row.start_ns, label="bar start_ns", minimum=0)
        _integer(row.end_ns, label="bar end_ns", minimum=1)
        if row.start_ns % width_ns or row.end_ns != row.start_ns + width_ns:
            raise AIDiscoveryContextError("morphology bar interval is not an aligned five minutes")
        try:
            timestamp_date = datetime.fromtimestamp(row.start_ns // ONE_SECOND_NS, tz=UTC).date()
        except (OSError, OverflowError, ValueError) as error:
            raise AIDiscoveryContextError(
                "morphology bar timestamp is outside UTC range"
            ) from error
        if timestamp_date != row.source_date:
            raise AIDiscoveryContextError("morphology bar timestamp/date mismatch")
        if previous_start is not None and row.start_ns <= previous_start:
            raise AIDiscoveryContextError("morphology bars must be strictly chronological")
        previous_start = row.start_ns
        _integer(row.range_ticks, label="range_ticks", minimum=0)
        _integer(
            row.signed_body_ppm,
            label="signed_body_ppm",
            minimum=-RATIO_SCALE,
            maximum=RATIO_SCALE,
        )
        for field_name in (
            "close_location_ppm",
            "upper_wick_ppm",
            "lower_wick_ppm",
        ):
            _integer(
                getattr(row, field_name),
                label=field_name,
                minimum=0,
                maximum=RATIO_SCALE,
            )
        if row.range_ticks == 0:
            if (
                row.signed_body_ppm != 0
                or row.close_location_ppm != RATIO_SCALE // 2
                or row.upper_wick_ppm != 0
                or row.lower_wick_ppm != 0
            ):
                raise AIDiscoveryContextError("zero-range morphology policy drift")
        else:
            decomposition = abs(row.signed_body_ppm) + row.upper_wick_ppm + row.lower_wick_ppm
            if not RATIO_SCALE - 2 <= decomposition <= RATIO_SCALE:
                raise AIDiscoveryContextError("bar morphology does not decompose its range")
        values.append(row)
    if len(values) != spec.expected_bar_row_count:
        raise AIDiscoveryContextError("bar-row count differs from the verified descriptors")
    observed_dates = tuple(dict.fromkeys(item.source_date for item in values))
    if observed_dates != spec.discovery_dates:
        raise AIDiscoveryContextError("bar rows do not cover the exact Discovery calendar")
    return tuple(values)


def _feature_value(row: _MorphologyRow, feature: str) -> int:
    if feature == "absolute_body_ppm":
        return abs(row.signed_body_ppm)
    value = getattr(row, feature, None)
    if isinstance(value, bool) or not isinstance(value, int):  # pragma: no cover - fixed axes
        raise AIDiscoveryContextError("threshold axis references an invalid integer feature")
    return value


def _threshold_supports(rows: tuple[_MorphologyRow, ...]) -> list[list[object]]:
    support_row_count = sum(len(axis.thresholds) for axis in _THRESHOLD_AXES)
    if support_row_count > MAX_THRESHOLD_SUPPORT_ROWS:  # pragma: no cover - frozen constant
        raise AIDiscoveryContextError("threshold lattice exceeds its precommitted support-row cap")
    decision_rows = tuple(item for item in rows if item.decision_eligible)
    result: list[list[object]] = []
    for axis in _THRESHOLD_AXES:
        for threshold in axis.thresholds:
            matched_dates: set[date] = set()
            block_counts = [0, 0, 0, 0]
            match_count = 0
            for row in decision_rows:
                feature_value = _feature_value(row, axis.feature)
                matched = (
                    feature_value >= threshold
                    if axis.operator == "GE"
                    else feature_value <= threshold
                )
                if not matched:
                    continue
                match_count += 1
                matched_dates.add(row.source_date)
                if row.block_number is None:  # pragma: no cover - decision invariant
                    raise AIDiscoveryContextError("decision row lost its reporting block")
                block_counts[row.block_number - 1] += 1
            result.append(
                [
                    axis.feature,
                    axis.operator,
                    threshold,
                    match_count,
                    len(matched_dates),
                    *block_counts,
                ]
            )
    return result


def _document_from_morphology_rows(
    spec: _ContextSourceSpec,
    rows: Iterable[_MorphologyRow],
) -> dict[str, object]:
    values = _validate_morphology_rows(spec, rows)
    daily = {item: _Aggregate() for item in spec.discovery_dates}
    blocks = [_Aggregate() for _ in spec.reporting_blocks]
    for row in values:
        daily[row.source_date].add(row)
        if row.block_number is not None:
            blocks[row.block_number - 1].add(row)

    daily_summaries: list[list[object]] = []
    block_by_date = spec.block_number_by_date
    for source_date in spec.discovery_dates:
        block_number = block_by_date.get(source_date)
        daily_summaries.append(
            [
                source_date.isoformat(),
                block_number,
                block_number is not None,
                *daily[source_date].as_list(),
            ]
        )
    block_summaries = [
        [
            number,
            block_dates[0].isoformat(),
            block_dates[-1].isoformat(),
            len(block_dates),
            *blocks[number - 1].as_list(),
        ]
        for number, block_dates in enumerate(spec.reporting_blocks, start=1)
    ]
    document: dict[str, object] = {
        "authority": dict(_AUTHORITY),
        "bar_columns": list(BAR_COLUMNS),
        "bars": [item.as_list() for item in values],
        "block_summaries": block_summaries,
        "block_summary_columns": list(BLOCK_SUMMARY_COLUMNS),
        "daily_summaries": daily_summaries,
        "daily_summary_columns": list(DAILY_SUMMARY_COLUMNS),
        "morphology": dict(_MORPHOLOGY_CONTRACT),
        "schema": AI_DISCOVERY_CONTEXT_SCHEMA,
        "source": spec.source_document(),
        "threshold_lattice": {
            **_LATTICE_DEFINITION,
            "support_columns": list(THRESHOLD_SUPPORT_COLUMNS),
            "supports": _threshold_supports(values),
        },
    }
    _assert_safe_context(document)
    return document


def _build_context_document(
    spec: _ContextSourceSpec,
    bars: Iterable[TradeBar],
) -> dict[str, object]:
    """Project a stream already bound to ``spec`` into the safe context schema."""

    if not isinstance(spec, _ContextSourceSpec):
        raise AIDiscoveryContextError("spec must be a verified context source specification")
    discovery_dates = frozenset(spec.discovery_dates)
    block_number_by_date = spec.block_number_by_date
    rows = (
        _morphology_row(
            item,
            discovery_dates=discovery_dates,
            block_number_by_date=block_number_by_date,
        )
        for item in bars
    )
    return _document_from_morphology_rows(spec, rows)


def _assert_safe_context(value: object) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise AIDiscoveryContextError("AI context keys must be strings")
            lowered = key.lower()
            if key in _FORBIDDEN_EXACT_KEYS or any(token in lowered for token in _FORBIDDEN_TOKENS):
                raise AIDiscoveryContextError(f"AI context contains forbidden field {key!r}")
            _assert_safe_context(item)
        return
    if isinstance(value, list | tuple):
        for item in value:
            _assert_safe_context(item)
        return
    if isinstance(value, str):
        lowered = value.lower()
        if not value.isascii():
            raise AIDiscoveryContextError("AI context text must use the bounded ASCII vocabulary")
        if "/" in value or "\\" in value or any(token in lowered for token in _FORBIDDEN_TOKENS):
            raise AIDiscoveryContextError("AI context contains a path or forbidden vocabulary")


def _strict_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise AIDiscoveryContextError(f"AI context contains duplicate key {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise AIDiscoveryContextError(f"AI context contains invalid JSON constant {value}")


def _parse_context_bytes(content: bytes) -> dict[str, object]:
    if not isinstance(content, bytes) or not content or len(content) > MAX_CONTEXT_BYTES:
        raise AIDiscoveryContextError("AI context byte size is outside the safe bound")
    try:
        value = json.loads(
            content.decode("utf-8"),
            object_pairs_hook=_strict_json_object,
            parse_constant=_reject_json_constant,
        )
    except AIDiscoveryContextError:
        raise
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise AIDiscoveryContextError("AI context is not strict JSON") from error
    if not isinstance(value, dict):
        raise AIDiscoveryContextError("AI context root must be an object")
    try:
        canonical = canonical_json_bytes(value)
    except (TypeError, ValueError, RecursionError) as error:
        raise AIDiscoveryContextError("AI context is not canonical research JSON") from error
    if canonical != content:
        raise AIDiscoveryContextError("AI context bytes are not exact canonical JSON")
    _assert_safe_context(value)
    return value


def _object(value: object, *, label: str, keys: frozenset[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise AIDiscoveryContextError(f"{label} has an invalid exact schema")
    return value


def _list(value: object, *, label: str) -> list[object]:
    if not isinstance(value, list):
        raise AIDiscoveryContextError(f"{label} must be a list")
    return value


def _rows_from_document(value: object) -> tuple[_MorphologyRow, ...]:
    raw_rows = _list(value, label="bars")
    if len(raw_rows) > MAX_BAR_ROWS:
        raise AIDiscoveryContextError("AI context exceeds the precommitted bar-row cap")
    rows: list[_MorphologyRow] = []
    for index, raw in enumerate(raw_rows, start=1):
        if not isinstance(raw, list) or len(raw) != len(BAR_COLUMNS):
            raise AIDiscoveryContextError(f"bar row {index} has an invalid exact schema")
        block_value = raw[3]
        if block_value is not None:
            block_value = _integer(
                block_value,
                label=f"bar row {index} block_number",
                minimum=1,
                maximum=4,
            )
        if not isinstance(raw[4], bool):
            raise AIDiscoveryContextError(f"bar row {index} decision_eligible must be boolean")
        rows.append(
            _MorphologyRow(
                source_date=_canonical_date(raw[0], label=f"bar row {index} source_date"),
                start_ns=_integer(raw[1], label=f"bar row {index} start_ns", minimum=0),
                end_ns=_integer(raw[2], label=f"bar row {index} end_ns", minimum=1),
                block_number=block_value,
                decision_eligible=raw[4],
                range_ticks=_integer(raw[5], label=f"bar row {index} range_ticks", minimum=0),
                signed_body_ppm=_integer(raw[6], label=f"bar row {index} signed_body_ppm"),
                close_location_ppm=_integer(raw[7], label=f"bar row {index} close_location_ppm"),
                upper_wick_ppm=_integer(raw[8], label=f"bar row {index} upper_wick_ppm"),
                lower_wick_ppm=_integer(raw[9], label=f"bar row {index} lower_wick_ppm"),
            )
        )
    return tuple(rows)


def _validate_context_document(
    document: object,
    spec: _ContextSourceSpec,
) -> dict[str, object]:
    root = _object(document, label="AI context", keys=_TOP_LEVEL_KEYS)
    if root["schema"] != AI_DISCOVERY_CONTEXT_SCHEMA:
        raise AIDiscoveryContextError("AI context schema identity drift")
    if root["authority"] != _AUTHORITY:
        raise AIDiscoveryContextError("AI context authority drift")
    if root["morphology"] != _MORPHOLOGY_CONTRACT:
        raise AIDiscoveryContextError("AI context morphology contract drift")
    if root["source"] != spec.source_document():
        raise AIDiscoveryContextError("AI context source identity drift")
    if root["bar_columns"] != list(BAR_COLUMNS):
        raise AIDiscoveryContextError("AI context bar columns drift")
    if root["daily_summary_columns"] != list(DAILY_SUMMARY_COLUMNS):
        raise AIDiscoveryContextError("AI context daily-summary columns drift")
    if root["block_summary_columns"] != list(BLOCK_SUMMARY_COLUMNS):
        raise AIDiscoveryContextError("AI context block-summary columns drift")
    lattice = _object(
        root["threshold_lattice"],
        label="threshold lattice",
        keys=_THRESHOLD_LATTICE_KEYS,
    )
    if lattice["support_columns"] != list(THRESHOLD_SUPPORT_COLUMNS):
        raise AIDiscoveryContextError("threshold support columns drift")
    rows = _rows_from_document(root["bars"])
    expected = _document_from_morphology_rows(spec, rows)
    if root != expected:
        raise AIDiscoveryContextError("AI context summaries or finite lattice drift")
    return root


def _descriptor_for_document(
    document: Mapping[str, object],
    spec: _ContextSourceSpec,
) -> BarArtifactDescriptor:
    return BarArtifactDescriptor(
        artifact_key="ai_pattern_discovery_v1:context",
        artifact_type="ai_discovery_context",
        artifact_schema=AI_DISCOVERY_CONTEXT_SCHEMA,
        artifact_version=1,
        record_count=spec.expected_bar_row_count,
        schema_sha256=AI_DISCOVERY_CONTEXT_SCHEMA_SHA256,
        source_manifest_sha256=spec.raw_source_manifest_sha256,
        logical_identity={
            "bar_row_count": spec.expected_bar_row_count,
            "dataset_handoff_sha256": spec.dataset_handoff_sha256,
            "dataset_manifest_sha256": spec.dataset_manifest_sha256,
            "discovery_calendar_sha256": spec.calendar_sha256,
            "feature_version": AI_MORPHOLOGY_VERSION,
            "lattice_sha256": THRESHOLD_LATTICE_SHA256,
            "split_plan_sha256": spec.split_plan_sha256,
        },
        media_type="application/json",
        file_suffix=".json",
        root_kind="bar_patterns",
    )


def _frozen_context_descriptor() -> BarArtifactDescriptor:
    descriptor = BarArtifactDescriptor(
        artifact_key="ai_pattern_discovery_v1:context",
        artifact_type="ai_discovery_context",
        artifact_schema=AI_DISCOVERY_CONTEXT_SCHEMA,
        artifact_version=1,
        record_count=EXPECTED_DISCOVERY_BAR_ROWS,
        schema_sha256=AI_DISCOVERY_CONTEXT_SCHEMA_SHA256,
        source_manifest_sha256=BAR_SOURCE_MANIFEST_SHA256,
        logical_identity={
            "bar_row_count": EXPECTED_DISCOVERY_BAR_ROWS,
            "dataset_handoff_sha256": EXPECTED_DATASET_HANDOFF_SHA256,
            "dataset_manifest_sha256": EXPECTED_DATASET_MANIFEST_SHA256,
            "discovery_calendar_sha256": EXPECTED_DISCOVERY_CALENDAR_SHA256,
            "feature_version": AI_MORPHOLOGY_VERSION,
            "lattice_sha256": THRESHOLD_LATTICE_SHA256,
            "split_plan_sha256": EXPECTED_SPLIT_PLAN_SHA256,
        },
        media_type="application/json",
        file_suffix=".json",
        root_kind="bar_patterns",
    )
    if descriptor.identity_sha256 != EXPECTED_AI_DISCOVERY_CONTEXT_IDENTITY_SHA256:
        raise AIDiscoveryContextError("frozen AI context descriptor identity drift")
    return descriptor


def _publish_context_document(
    project_root: Path,
    document: dict[str, object],
    spec: _ContextSourceSpec,
) -> AIDiscoveryContextArtifact:
    validated = _validate_context_document(document, spec)
    content = canonical_json_bytes(validated)
    if len(content) > MAX_CONTEXT_BYTES:
        raise AIDiscoveryContextError("canonical AI context exceeds the publication byte cap")
    descriptor = _descriptor_for_document(validated, spec)
    published = publish_bar_artifact_bytes(project_root, descriptor, content)
    return AIDiscoveryContextArtifact(published=published)


def _read_context_artifact(
    project_root: Path,
    artifact: AIDiscoveryContextArtifact,
) -> dict[str, object]:
    if not isinstance(artifact, AIDiscoveryContextArtifact):
        raise AIDiscoveryContextError("artifact must be an AIDiscoveryContextArtifact")
    with open_verified_bar_artifact(project_root, artifact.published) as opened:
        os.lseek(opened.descriptor, 0, os.SEEK_SET)
        content = bytearray()
        while chunk := os.read(opened.descriptor, 1024 * 1024):
            content.extend(chunk)
            if len(content) > MAX_CONTEXT_BYTES:
                raise AIDiscoveryContextError("AI context exceeds the safe reopen byte cap")
        if len(content) != artifact.byte_size:
            raise AIDiscoveryContextError("AI context byte count changed while reopened")
        return _parse_context_bytes(bytes(content))


def _reopen_context_for_spec(
    project_root: Path,
    artifact: AIDiscoveryContextArtifact,
    spec: _ContextSourceSpec,
) -> dict[str, object]:
    document = _read_context_artifact(project_root, artifact)
    return _validate_reopened_document(artifact, document, spec)


def _validate_reopened_document(
    artifact: AIDiscoveryContextArtifact,
    document: dict[str, object],
    spec: _ContextSourceSpec,
) -> dict[str, object]:
    validated = _validate_context_document(document, spec)
    expected_descriptor = _descriptor_for_document(validated, spec)
    if artifact.published.descriptor != expected_descriptor:
        raise AIDiscoveryContextError("AI context artifact identity descriptor drift")
    expected_sha256 = hashlib.sha256(canonical_json_bytes(validated)).hexdigest()
    if artifact.sha256 != expected_sha256:
        raise AIDiscoveryContextError("AI context content address drift")
    return validated


def _strict_project_root(value: Path | str) -> tuple[Path, Path]:
    requested = Path(value).expanduser()
    if requested.is_symlink():
        raise AIDiscoveryContextError("project_root cannot be a symbolic link")
    try:
        root = requested.resolve(strict=True)
    except OSError as error:
        raise AIDiscoveryContextError("project_root does not exist") from error
    if not root.is_dir():
        raise AIDiscoveryContextError("project_root must be a directory")
    data = root / "data"
    if data.is_symlink() or not data.is_dir():
        raise AIDiscoveryContextError("project data directory must be an existing non-symlink")
    try:
        resolved_data = data.resolve(strict=True)
    except OSError as error:  # pragma: no cover - guarded by is_dir
        raise AIDiscoveryContextError("project data directory is inaccessible") from error
    if resolved_data.parent != root:
        raise AIDiscoveryContextError("project data directory escapes the project root")
    return root, resolved_data


def _five_minute_descriptor(partition: BarDatasetPartition) -> TradeBarArtifactDescriptor:
    matches = [item for item in partition.artifacts if item.timeframe_seconds == TIMEFRAME_SECONDS]
    if len(matches) != 1:
        raise AIDiscoveryContextError("Discovery partition lacks one exact five-minute artifact")
    descriptor = matches[0]
    expected_uri = (
        f"derived/trade_bars/version={BAR_VERSION}/timeframe=5m/sha256={descriptor.sha256}.parquet"
    )
    if descriptor.relative_uri != expected_uri:
        raise AIDiscoveryContextError("five-minute descriptor URI is not canonical")
    return descriptor


def _reporting_block_dates(
    split_plan: BarSplitPlan,
    discovery_dates: tuple[date, ...],
) -> tuple[tuple[date, ...], ...]:
    blocks: list[tuple[date, ...]] = []
    for block in split_plan.discovery_reporting_blocks:
        start_index = block.start_active_ordinal - 1
        end_index = block.end_active_ordinal
        values = discovery_dates[start_index:end_index]
        if (
            not values
            or values[0] != block.start_date
            or values[-1] != block.end_date
            or len(values) != block.active_day_count
        ):
            raise AIDiscoveryContextError("Discovery reporting-block calendar drift")
        blocks.append(values)
    return tuple(blocks)


def _frozen_projection_inputs(
    project_root: Path | str,
) -> tuple[Path, Path, _ContextSourceSpec, tuple[BarDatasetPartition, ...]]:
    root, data_root = _strict_project_root(project_root)
    manifest_path = root / _MANIFEST_RELATIVE_PATH
    try:
        dataset = load_bar_dataset_manifest(
            manifest_path,
            expected_sha256=EXPECTED_DATASET_MANIFEST_SHA256,
        )
    except (BarPipelineError, OSError) as error:
        raise AIDiscoveryContextError("approved trade-bar manifest verification failed") from error
    if dataset.dataset_manifest_sha256 != EXPECTED_DATASET_MANIFEST_SHA256:
        raise AIDiscoveryContextError("trade-bar dataset manifest identity drift")
    if dataset.source_manifest_sha256 != BAR_SOURCE_MANIFEST_SHA256:
        raise AIDiscoveryContextError("raw source manifest identity drift")
    if dataset.handoff_sha256 != EXPECTED_DATASET_HANDOFF_SHA256:
        raise AIDiscoveryContextError("trade-bar dataset handoff identity drift")
    try:
        split_plan = plan_bar_splits(dataset.eligible_active_dates)
    except ValueError as error:
        raise AIDiscoveryContextError("frozen split derivation failed") from error
    if split_plan.sha256 != EXPECTED_SPLIT_PLAN_SHA256:
        raise AIDiscoveryContextError("derived split plan identity drift")
    discovery_count = split_plan.discovery.active_day_count
    discovery_dates = dataset.eligible_active_dates[:discovery_count]
    partitions = dataset.partitions[:discovery_count]
    if tuple(item.source_date for item in partitions) != discovery_dates:
        raise AIDiscoveryContextError("manifest partitions differ from Discovery dates")
    if (
        len(discovery_dates) != EXPECTED_DISCOVERY_ACTIVE_DAYS
        or discovery_dates[0] != EXPECTED_DISCOVERY_START_DATE
        or discovery_dates[-1] != EXPECTED_DISCOVERY_END_DATE
        or split_plan.discovery.decision_end_date != EXPECTED_DECISION_END_DATE
        or canonical_sha256([item.isoformat() for item in discovery_dates])
        != EXPECTED_DISCOVERY_CALENDAR_SHA256
    ):
        raise AIDiscoveryContextError("visible Discovery calendar identity drift")
    blocks = _reporting_block_dates(split_plan, discovery_dates)
    if tuple(len(item) for item in blocks) != EXPECTED_REPORTING_BLOCK_LENGTHS:
        raise AIDiscoveryContextError("Discovery reporting-block lengths drift")
    if sum(len(item) for item in blocks) != EXPECTED_DISCOVERY_DECISION_DAYS:
        raise AIDiscoveryContextError("Discovery decision-day count drift")

    row_count = 0
    byte_count = 0
    for partition in partitions:
        descriptor = _five_minute_descriptor(partition)
        row_count += descriptor.row_count
        byte_count += descriptor.byte_size
        if row_count > MAX_BAR_ROWS or byte_count > MAX_SOURCE_ARTIFACT_BYTES:
            raise AIDiscoveryContextError("verified Discovery descriptors exceed source caps")
    if row_count != EXPECTED_DISCOVERY_BAR_ROWS:
        raise AIDiscoveryContextError("Discovery five-minute row count drift")
    if byte_count != EXPECTED_DISCOVERY_SOURCE_BYTES:
        raise AIDiscoveryContextError("Discovery five-minute artifact byte count drift")
    spec = _ContextSourceSpec(
        dataset_manifest_sha256=dataset.dataset_manifest_sha256,
        dataset_handoff_sha256=dataset.handoff_sha256,
        raw_source_manifest_sha256=dataset.source_manifest_sha256,
        split_plan_sha256=split_plan.sha256,
        discovery_dates=discovery_dates,
        reporting_blocks=blocks,
        expected_bar_row_count=row_count,
        source_artifact_byte_count=byte_count,
    )
    return root, data_root, spec, partitions


def _load_discovery_bars(
    data_root: Path,
    partitions: tuple[BarDatasetPartition, ...],
) -> Iterable[TradeBar]:
    for partition in partitions:
        descriptor = _five_minute_descriptor(partition)
        try:
            bars = load_trade_bar_artifact(
                data_root,
                descriptor,
                expected_plan_sha256=partition.plan_sha256,
                expected_source_sha256=partition.source_sha256,
                expected_source_date=partition.source_date,
            )
        except (OSError, ValueError) as error:
            raise AIDiscoveryContextError(
                f"verified five-minute artifact load failed for {partition.source_date.isoformat()}"
            ) from error
        if len(bars) != descriptor.row_count:
            raise AIDiscoveryContextError("loaded five-minute row count differs from descriptor")
        yield from bars


def _frozen_spec_from_document(document: object) -> _ContextSourceSpec:
    root = _object(document, label="AI context", keys=_TOP_LEVEL_KEYS)
    source = _object(root["source"], label="AI context source", keys=_SOURCE_KEYS)
    frozen_source_fields = {
        "active_date_count": EXPECTED_DISCOVERY_ACTIVE_DAYS,
        "bar_row_count": EXPECTED_DISCOVERY_BAR_ROWS,
        "bar_version": BAR_VERSION,
        "dataset_handoff_sha256": EXPECTED_DATASET_HANDOFF_SHA256,
        "dataset_manifest_sha256": EXPECTED_DATASET_MANIFEST_SHA256,
        "decision_date_count": EXPECTED_DISCOVERY_DECISION_DAYS,
        "decision_end_date": EXPECTED_DECISION_END_DATE.isoformat(),
        "discovery_calendar_sha256": EXPECTED_DISCOVERY_CALENDAR_SHA256,
        "discovery_end_date": EXPECTED_DISCOVERY_END_DATE.isoformat(),
        "discovery_start_date": EXPECTED_DISCOVERY_START_DATE.isoformat(),
        "raw_source_manifest_sha256": BAR_SOURCE_MANIFEST_SHA256,
        "reporting_block_count": 4,
        "source_artifact_byte_count": EXPECTED_DISCOVERY_SOURCE_BYTES,
        "split_plan_sha256": EXPECTED_SPLIT_PLAN_SHA256,
        "timeframe_seconds": TIMEFRAME_SECONDS,
    }
    if source != frozen_source_fields:
        raise AIDiscoveryContextError("reopened AI context differs from frozen source identity")
    daily_rows = _list(root["daily_summaries"], label="daily summaries")
    if len(daily_rows) != EXPECTED_DISCOVERY_ACTIVE_DAYS:
        raise AIDiscoveryContextError("reopened AI context daily calendar length drift")
    discovery_dates: list[date] = []
    for index, row in enumerate(daily_rows, start=1):
        if not isinstance(row, list) or len(row) != len(DAILY_SUMMARY_COLUMNS):
            raise AIDiscoveryContextError(f"daily summary {index} has an invalid exact schema")
        discovery_dates.append(_canonical_date(row[0], label=f"daily summary {index} source_date"))
    dates = tuple(discovery_dates)
    if (
        dates != tuple(sorted(set(dates)))
        or dates[0] != EXPECTED_DISCOVERY_START_DATE
        or dates[-1] != EXPECTED_DISCOVERY_END_DATE
        or canonical_sha256([item.isoformat() for item in dates])
        != EXPECTED_DISCOVERY_CALENDAR_SHA256
    ):
        raise AIDiscoveryContextError("reopened AI context calendar identity drift")
    blocks: list[tuple[date, ...]] = []
    cursor = 0
    for length in EXPECTED_REPORTING_BLOCK_LENGTHS:
        blocks.append(dates[cursor : cursor + length])
        cursor += length
    if cursor != EXPECTED_DISCOVERY_DECISION_DAYS:
        raise AIDiscoveryContextError("frozen reporting-block allocation drift")
    return _ContextSourceSpec(
        dataset_manifest_sha256=EXPECTED_DATASET_MANIFEST_SHA256,
        dataset_handoff_sha256=EXPECTED_DATASET_HANDOFF_SHA256,
        raw_source_manifest_sha256=BAR_SOURCE_MANIFEST_SHA256,
        split_plan_sha256=EXPECTED_SPLIT_PLAN_SHA256,
        discovery_dates=dates,
        reporting_blocks=tuple(blocks),
        expected_bar_row_count=EXPECTED_DISCOVERY_BAR_ROWS,
        source_artifact_byte_count=EXPECTED_DISCOVERY_SOURCE_BYTES,
    )


def build_ai_discovery_context(
    project_root: Path | str,
) -> AIDiscoveryContextArtifact:
    """Build and immutably publish the one approved visible-Discovery AI context."""

    root, data_root, spec, partitions = _frozen_projection_inputs(project_root)
    document = _build_context_document(spec, _load_discovery_bars(data_root, partitions))
    artifact = _publish_context_document(root, document, spec)
    if (
        artifact.sha256 != EXPECTED_AI_DISCOVERY_CONTEXT_SHA256
        or artifact.published.descriptor.identity_sha256
        != EXPECTED_AI_DISCOVERY_CONTEXT_IDENTITY_SHA256
    ):
        raise AIDiscoveryContextError("AI context differs from its frozen content identity")
    return artifact


def reopen_ai_discovery_context(
    project_root: Path | str,
    artifact: AIDiscoveryContextArtifact,
) -> dict[str, object]:
    """Safely reopen, rehash, parse, and semantically replay a frozen context."""

    root, _ = _strict_project_root(project_root)
    if not isinstance(artifact, AIDiscoveryContextArtifact):
        raise AIDiscoveryContextError("artifact must be an AIDiscoveryContextArtifact")
    if (
        artifact.sha256 != EXPECTED_AI_DISCOVERY_CONTEXT_SHA256
        or artifact.published.descriptor.identity_sha256
        != EXPECTED_AI_DISCOVERY_CONTEXT_IDENTITY_SHA256
    ):
        raise AIDiscoveryContextError("AI context differs from its frozen content identity")
    document = _read_context_artifact(root, artifact)
    spec = _frozen_spec_from_document(document)
    return _validate_reopened_document(artifact, document, spec)


def load_ai_discovery_context(
    project_root: Path | str,
    *,
    identity: Mapping[str, object] | None = None,
) -> AIDiscoveryContextArtifact:
    """Reconstruct and verify the frozen artifact without accepting a source path.

    ``identity`` is the optional output of :meth:`AIDiscoveryContextArtifact.as_dict`.
    Omitting it loads the sole content address frozen by this module.  Arbitrary
    paths, hashes, sizes, and descriptor identities are never accepted.
    """

    root, _ = _strict_project_root(project_root)
    expected_identity: dict[str, object] = {
        "artifact_identity_sha256": EXPECTED_AI_DISCOVERY_CONTEXT_IDENTITY_SHA256,
        "byte_size": EXPECTED_AI_DISCOVERY_CONTEXT_BYTES,
        "content_sha256": EXPECTED_AI_DISCOVERY_CONTEXT_SHA256,
        "schema": AI_DISCOVERY_CONTEXT_ARTIFACT_SCHEMA,
    }
    if identity is not None and (
        not isinstance(identity, Mapping) or dict(identity) != expected_identity
    ):
        raise AIDiscoveryContextError("AI context path-free artifact identity drift")
    descriptor = _frozen_context_descriptor()
    path = (
        root / descriptor.relative_directory / f"sha256={EXPECTED_AI_DISCOVERY_CONTEXT_SHA256}.json"
    )
    artifact = AIDiscoveryContextArtifact(
        published=PublishedBarArtifact(
            descriptor=descriptor,
            path=path,
            sha256=EXPECTED_AI_DISCOVERY_CONTEXT_SHA256,
            byte_size=EXPECTED_AI_DISCOVERY_CONTEXT_BYTES,
        )
    )
    reopen_ai_discovery_context(root, artifact)
    return artifact


def verify_ai_discovery_context(
    project_root: Path | str,
    artifact: AIDiscoveryContextArtifact,
) -> None:
    """Raise on any content, path, source, schema, chronology, summary, or lattice drift."""

    reopen_ai_discovery_context(project_root, artifact)
