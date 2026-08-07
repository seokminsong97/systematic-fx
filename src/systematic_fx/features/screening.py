"""Deterministic Phase 1A MBP-10 screening feature artifacts.

The builder consumes a previously frozen :class:`ContractSelectionResult` and
creates source-local screening features.  It deliberately records that
definition/status inputs are unavailable, so no row can claim research,
``PASS_BACKTEST``, Paper, or Live eligibility.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
import tomllib
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Final, Literal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from systematic_fx.data.contract_selection import (
    CONTRACT_SELECTION_POLICY_VERSION,
    CONTRACT_SELECTION_SCHEMA,
    ContractSelectionResult,
)
from systematic_fx.data.contracts import (
    UNDEFINED_PRICE,
    Mbp10ContractError,
    compute_schema_fingerprint,
    decode_dbn_metadata,
    validate_mbp10_contract,
)
from systematic_fx.data.instruments import InstrumentKind, parse_instrument_mappings
from systematic_fx.data.quality import StructuralQcError, load_structural_qc_config
from systematic_fx.features import pilot
from systematic_fx.validation.splits import Phase1AScreeningCalendar

FEATURE_VERSION: Final = "phase1a_mbp10_screening_v1"
CONFIG_SCHEMA_VERSION: Final = 1
SCREENING_ONLY: Final = True
RESEARCH_ELIGIBLE: Final = False
DEFINITION_STATUS_AVAILABLE: Final = False
PRICE_SCALE: Final = "1e-9"
TICK_SIZE_RAW: Final = 50_000
DEPTH_LEVELS: Final = (1, 3, 5, 10)
ONE_SECOND_NS: Final = 1_000_000_000
FIVE_MINUTE_NS: Final = 300 * ONE_SECOND_NS
QUOTE_FRESH_MAX_AGE_MS: Final = 1_000
QUOTE_FRESH_MAX_AGE_NS: Final = QUOTE_FRESH_MAX_AGE_MS * 1_000_000
_UNIX_EPOCH_DATE: Final = date(1970, 1, 1)
DEFAULT_CONFIG_PATH: Final = (
    Path(__file__).resolve().parents[3] / "configs/features/phase1a_mbp10_screening_v1.toml"
)
DEFAULT_QC_CONFIG_PATH: Final = (
    Path(__file__).resolve().parents[3] / "configs/data/mbp10_structural_qc_v1.toml"
)

_UINT32_MAX: Final = 2**32 - 1
_INT64_MIN: Final = -(2**63)
_INT64_MAX: Final = 2**63 - 1
_SHA256 = re.compile(r"[0-9a-f]{64}")
_SAFE_SYMBOL = re.compile(r"^[A-Z0-9]+[FGHJKMNQUVXZ][0-9]{1,2}$")
_CALENDAR_RELATIVE_PATH: Final = "derived/manifests/phase1a_screening_source_date_calendar_v1.json"
_SOURCE_MANIFEST_RELATIVE_PATH: Final = "derived/manifests/mbp10_source_sha256_v1.jsonl"
_QC_MANIFEST_RELATIVE_PATH: Final = "derived/manifests/mbp10_structural_qc_v1.jsonl"
_SOURCE_MANIFEST_FIELDS: Final = frozenset({"byte_size", "relative_uri", "sha256", "source_date"})
_QC_MANIFEST_FIELDS: Final = frozenset(
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
_UNPROVEN_CLOSED_BOUNDARY: Final = "UNPROVEN_CLOSED_BOUNDARY"
NO_PROVEN_COMPLETE_OBSERVED_1S_BUCKET: Final = "NO_PROVEN_COMPLETE_OBSERVED_1S_BUCKET"
NO_POSITIVE_PREVIOUS_SOURCE_TRADE_VOLUME: Final = "NO_POSITIVE_PREVIOUS_SOURCE_TRADE_VOLUME"

SCREENING_FORMULAS: Final[tuple[tuple[str, str], ...]] = (
    (
        "selection",
        (
            "verified ContractSelectionResult with frozen previous-source-only information "
            "boundary, eligible date, selected footer ID/symbol, canonical hashes, and "
            "positive prior volume"
        ),
    ),
    (
        "qualification",
        (
            "canonical Phase1AScreeningCalendar and its source/QC manifests bind the eligible "
            "source date, raw bytes, QC PASS/config, and schema fingerprint"
        ),
    ),
    (
        "event_order",
        "ts_recv interpreted in physical Parquet row order across sequential row groups",
    ),
    (
        "one_second_bucket",
        "ceil(ts_recv_ns / 1000000000) * 1000000000; interval (end-1s,end]",
    ),
    (
        "source_boundary_exclusion",
        (
            "exclude the partial 1s bucket ending at source start and exclude 1s/5m buckets "
            "ending at source end as UNPROVEN_CLOSED_BOUNDARY"
        ),
    ),
    (
        "late_event",
        "ignore a selected row when its one-second bucket precedes the current open bucket",
    ),
    (
        "no_forward_fill",
        "emit only observed selected-contract seconds and never synthesize or rewrite a row",
    ),
    (
        "book_snapshot",
        "BBO and depth are the last selected physical row in each observed second",
    ),
    (
        "tick_units",
        "one 6E tick equals 50000 raw 1e-9 price units; non-divisible prices are off-grid",
    ),
    (
        "imbalance_numerator",
        "bid cumulative size minus ask cumulative size at L1,L3,L5,L10",
    ),
    (
        "imbalance_denominator",
        "bid cumulative size plus ask cumulative size at L1,L3,L5,L10",
    ),
    (
        "imbalance_signed_ppm",
        (
            "truncate toward zero of numerator times 1000000 divided by denominator; "
            "null when denominator is zero"
        ),
    ),
    (
        "depth_change",
        (
            "current minus prior cumulative depth only when both seconds are valid and "
            "bucket ends differ by exactly one second"
        ),
    ),
    (
        "recovery_state",
        (
            "state starts unknown; MAYBE_BAD_BOOK or invalid recovery persists; a valid "
            "snapshot or structurally valid empty reset is an invalid marker second, and "
            "only an exactly adjacent clean base-valid observed second rearms at its close"
        ),
    ),
    (
        "valid_second",
        (
            "proven-boundary defined uncrossed unlocked on-tick BBO, safe arithmetic, no bad "
            "flags or recovery marker, and recovery state rearmed"
        ),
    ),
    (
        "quote_age",
        (
            "floor((bucket_end_ns - last_valid_selected_row_ts_recv_ns) / 1000000) "
            "without carrying book fields"
        ),
    ),
    (
        "five_minute_bucket",
        ("ceil(one_second_bucket_end_ns / 300000000000) * 300000000000; interval (end-5m,end]"),
    ),
    (
        "integer_mean",
        "truncate toward zero of the exact Python integer sum divided by the observation count",
    ),
    (
        "imbalance_sign_changes",
        "remove zero numerator signs then count adjacent positive-negative flips",
    ),
    (
        "imbalance_persistence",
        (
            "terminal equal-sign run length and truncate toward zero of run times 1000000 "
            "divided by valid sign count"
        ),
    ),
    (
        "missing_seconds",
        "300 minus distinct emitted observed seconds in the right-closed five-minute bucket",
    ),
    (
        "source_local_signal_input_valid",
        (
            "complete source window with 300 observed valid seconds, "
            "no stale/recovery-required/locked/crossed state, and fresh decision quote"
        ),
    ),
    (
        "signal_input_valid",
        (
            "source_local_signal_input_valid and definition_status_available; "
            "definition_status_available is false in Phase1A"
        ),
    ),
    (
        "artifact_identity",
        (
            "raw/config/formula/selection/calendar/source-manifest/QC-manifest/QC-config/schema "
            "and caller-verified lowercase code snapshot SHA-256 are recorded in report and "
            "Parquet metadata"
        ),
    ),
    (
        "parquet_encoding",
        (
            "PyArrow Parquet with zstd compression, dictionary disabled, statistics enabled, "
            "and row_group_size 65536"
        ),
    ),
)


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


FORMULA_SHA256: Final = hashlib.sha256(
    _canonical_json(
        [{"definition": definition, "name": name} for name, definition in SCREENING_FORMULAS]
    )
).hexdigest()

_IDENTITY_FIELDS = [
    pa.field("feature_version", pa.string(), nullable=False),
    pa.field("screening_only", pa.bool_(), nullable=False),
    pa.field("definition_status_available", pa.bool_(), nullable=False),
    pa.field("source_date", pa.date32(), nullable=False),
    pa.field("contract", pa.string(), nullable=False),
    pa.field("instrument_id", pa.uint32(), nullable=False),
    pa.field("bucket_end", pa.timestamp("ns", tz="UTC"), nullable=False),
]

_ACTION_NAMES: Final = ("a", "c", "f", "m", "n", "r", "t", "other")
_FLOW_FIELDS = [
    pa.field("event_count", pa.uint64(), nullable=False),
    *(pa.field(f"action_{name}_count", pa.uint64(), nullable=False) for name in _ACTION_NAMES),
    pa.field("trade_count", pa.uint64(), nullable=False),
    pa.field("trade_volume", pa.uint64(), nullable=False),
    pa.field("aggressor_buy_volume", pa.uint64(), nullable=False),
    pa.field("aggressor_sell_volume", pa.uint64(), nullable=False),
    pa.field("unknown_side_trade_volume", pa.uint64(), nullable=False),
    pa.field("signed_trade_volume", pa.int64(), nullable=False),
]
_FLOW_NAMES: Final = tuple(field.name for field in _FLOW_FIELDS)

_ONE_SECOND_LEVEL_FIELDS = [
    field
    for level in DEPTH_LEVELS
    for field in (
        pa.field(f"bid_cum_size_l{level}", pa.uint64(), nullable=False),
        pa.field(f"ask_cum_size_l{level}", pa.uint64(), nullable=False),
        pa.field(f"imbalance_numerator_l{level}", pa.int64(), nullable=False),
        pa.field(f"imbalance_denominator_l{level}", pa.uint64(), nullable=False),
        pa.field(f"imbalance_signed_ppm_l{level}", pa.int32(), nullable=True),
        pa.field(f"bid_depth_change_l{level}", pa.int64(), nullable=True),
        pa.field(f"ask_depth_change_l{level}", pa.int64(), nullable=True),
        pa.field(f"imbalance_numerator_change_l{level}", pa.int64(), nullable=True),
    )
]

ONE_SECOND_SCHEMA: Final = pa.schema(
    [
        *_IDENTITY_FIELDS,
        pa.field("source_last_row", pa.uint64(), nullable=False),
        pa.field("last_ts_recv", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("last_action", pa.string(), nullable=False),
        pa.field("last_side", pa.string(), nullable=False),
        pa.field("last_flags", pa.uint8(), nullable=False),
        pa.field("bid_px_00_raw", pa.int64(), nullable=True),
        pa.field("ask_px_00_raw", pa.int64(), nullable=True),
        pa.field("bid_px_00_ticks", pa.int64(), nullable=True),
        pa.field("ask_px_00_ticks", pa.int64(), nullable=True),
        pa.field("mid_px_x2_raw", pa.int64(), nullable=True),
        pa.field("spread_raw", pa.int64(), nullable=True),
        pa.field("spread_ticks", pa.int64(), nullable=True),
        pa.field("bid_size_00", pa.uint32(), nullable=True),
        pa.field("ask_size_00", pa.uint32(), nullable=True),
        pa.field("bid_count_00", pa.uint32(), nullable=True),
        pa.field("ask_count_00", pa.uint32(), nullable=True),
        pa.field("bid_valid_levels", pa.uint8(), nullable=False),
        pa.field("ask_valid_levels", pa.uint8(), nullable=False),
        *_ONE_SECOND_LEVEL_FIELDS,
        *_FLOW_FIELDS,
        pa.field("observed_second", pa.bool_(), nullable=False),
        pa.field("missing_second", pa.bool_(), nullable=False),
        pa.field("book_missing", pa.bool_(), nullable=False),
        pa.field("base_book_valid", pa.bool_(), nullable=False),
        pa.field("valid_second", pa.bool_(), nullable=False),
        pa.field("locked_book", pa.bool_(), nullable=False),
        pa.field("crossed_book", pa.bool_(), nullable=False),
        pa.field("maybe_bad_book", pa.bool_(), nullable=False),
        pa.field("bad_ts_recv", pa.bool_(), nullable=False),
        pa.field("snapshot_row", pa.bool_(), nullable=False),
        pa.field("reset_seen", pa.bool_(), nullable=False),
        pa.field("recovery_required_at_open", pa.bool_(), nullable=False),
        pa.field("recovery_marker_seen", pa.bool_(), nullable=False),
        pa.field("recovery_rearmed", pa.bool_(), nullable=False),
        pa.field("recovery_required_at_close", pa.bool_(), nullable=False),
        pa.field("price_arithmetic_overflow", pa.bool_(), nullable=False),
        pa.field("price_on_tick_grid", pa.bool_(), nullable=False),
        pa.field("quote_age_ms", pa.uint32(), nullable=True),
        pa.field("quote_fresh", pa.bool_(), nullable=False),
        pa.field("stale_second", pa.bool_(), nullable=False),
    ]
)

_SUMMARY_STAT_NAMES: Final = ("first", "last", "min", "max", "mean_trunc")
_FIVE_MINUTE_LEVEL_FIELDS = [
    field
    for level in DEPTH_LEVELS
    for field in (
        *(
            pa.field(f"bid_cum_size_l{level}_{name}", pa.uint64(), nullable=True)
            for name in _SUMMARY_STAT_NAMES
        ),
        *(
            pa.field(f"ask_cum_size_l{level}_{name}", pa.uint64(), nullable=True)
            for name in _SUMMARY_STAT_NAMES
        ),
        *(
            pa.field(f"imbalance_numerator_l{level}_{name}", pa.int64(), nullable=True)
            for name in _SUMMARY_STAT_NAMES
        ),
        *(
            pa.field(f"imbalance_denominator_l{level}_{name}", pa.uint64(), nullable=True)
            for name in _SUMMARY_STAT_NAMES
        ),
        *(
            pa.field(f"imbalance_signed_ppm_l{level}_{name}", pa.int32(), nullable=True)
            for name in _SUMMARY_STAT_NAMES
        ),
        pa.field(f"imbalance_sign_changes_l{level}", pa.uint16(), nullable=False),
        pa.field(f"imbalance_positive_seconds_l{level}", pa.uint16(), nullable=False),
        pa.field(f"imbalance_negative_seconds_l{level}", pa.uint16(), nullable=False),
        pa.field(f"imbalance_zero_seconds_l{level}", pa.uint16(), nullable=False),
        pa.field(f"imbalance_observed_seconds_l{level}", pa.uint16(), nullable=False),
        pa.field(
            f"imbalance_last_sign_persistence_seconds_l{level}",
            pa.uint16(),
            nullable=False,
        ),
        pa.field(
            f"imbalance_last_sign_persistence_ppm_l{level}",
            pa.uint32(),
            nullable=True,
        ),
    )
]

FIVE_MINUTE_SCHEMA: Final = pa.schema(
    [
        *_IDENTITY_FIELDS,
        pa.field("first_1s_bucket_end", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("last_1s_bucket_end", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("last_source_row", pa.uint64(), nullable=False),
        pa.field("last_valid_quote_ts_recv", pa.timestamp("ns", tz="UTC"), nullable=True),
        pa.field("decision_quote_age_ms", pa.uint32(), nullable=True),
        pa.field("decision_quote_fresh", pa.bool_(), nullable=False),
        pa.field("last_bid_px_00_raw", pa.int64(), nullable=True),
        pa.field("last_ask_px_00_raw", pa.int64(), nullable=True),
        pa.field("last_bid_px_00_ticks", pa.int64(), nullable=True),
        pa.field("last_ask_px_00_ticks", pa.int64(), nullable=True),
        pa.field("last_spread_raw", pa.int64(), nullable=True),
        pa.field("last_spread_ticks", pa.int64(), nullable=True),
        pa.field("mid_px_x2_raw_open", pa.int64(), nullable=True),
        pa.field("mid_px_x2_raw_high", pa.int64(), nullable=True),
        pa.field("mid_px_x2_raw_low", pa.int64(), nullable=True),
        pa.field("mid_px_x2_raw_close", pa.int64(), nullable=True),
        pa.field("mid_px_x2_raw_mean_trunc", pa.int64(), nullable=True),
        pa.field("spread_raw_first", pa.int64(), nullable=True),
        pa.field("spread_raw_last", pa.int64(), nullable=True),
        pa.field("spread_raw_min", pa.int64(), nullable=True),
        pa.field("spread_raw_max", pa.int64(), nullable=True),
        pa.field("spread_raw_mean_trunc", pa.int64(), nullable=True),
        *_FIVE_MINUTE_LEVEL_FIELDS,
        *_FLOW_FIELDS,
        pa.field("observed_seconds", pa.uint16(), nullable=False),
        pa.field("missing_seconds", pa.uint16(), nullable=False),
        pa.field("valid_seconds", pa.uint16(), nullable=False),
        pa.field("invalid_seconds", pa.uint16(), nullable=False),
        pa.field("stale_seconds", pa.uint16(), nullable=False),
        pa.field("book_missing_seconds", pa.uint16(), nullable=False),
        pa.field("locked_seconds", pa.uint16(), nullable=False),
        pa.field("crossed_seconds", pa.uint16(), nullable=False),
        pa.field("maybe_bad_book_seconds", pa.uint16(), nullable=False),
        pa.field("bad_ts_recv_seconds", pa.uint16(), nullable=False),
        pa.field("reset_seen_seconds", pa.uint16(), nullable=False),
        pa.field("recovery_marker_seconds", pa.uint16(), nullable=False),
        pa.field("recovery_required_seconds", pa.uint16(), nullable=False),
        pa.field("recovery_rearmed_seconds", pa.uint16(), nullable=False),
        pa.field("snapshot_seconds", pa.uint16(), nullable=False),
        pa.field("off_tick_grid_seconds", pa.uint16(), nullable=False),
        pa.field("source_window_complete", pa.bool_(), nullable=False),
        pa.field("closed_bucket", pa.bool_(), nullable=False),
        pa.field("source_local_signal_input_valid", pa.bool_(), nullable=False),
        pa.field("signal_input_valid", pa.bool_(), nullable=False),
    ]
)


class ScreeningFeatureBuildError(ValueError):
    """The source, selection, config, arithmetic, or immutable output is invalid."""


class ScreeningFeaturePathError(ScreeningFeatureBuildError):
    """An input or output path violates the data/derived containment contract."""


@dataclass(frozen=True, slots=True)
class ScreeningFeatureConfig:
    path: Path
    sha256: str
    formula_sha256: str


ArtifactDisposition = Literal["CREATED", "REUSED"]


@dataclass(frozen=True, slots=True)
class ScreeningArtifactReport:
    path: str
    disposition: ArtifactDisposition
    sha256: str
    rows: int
    schema_sha256: str
    min_bucket_end: str
    max_bucket_end: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ScreeningFeatureBuildReport:
    feature_version: str
    screening_only: bool
    research_eligible: bool
    definition_status_available: bool
    source_path: str
    source_date: str
    source_sha256: str
    source_schema_sha256: str
    source_manifest_sha256: str
    qc_manifest_sha256: str
    qc_config_sha256: str
    calendar_sha256: str
    code_snapshot_sha256: str
    source_rows: int
    selected_rows: int
    late_rows_ignored: int
    source_start_partial_one_second_excluded: int
    unproven_closed_boundary_one_second_excluded: int
    unproven_closed_boundary_five_minute_excluded: int
    config_path: str
    config_sha256: str
    formula_sha256: str
    contract_selection_sha256: str
    previous_volume_sha256: str
    previous_source_date: str
    instrument_id: int
    contract: str
    contract_month: str
    previous_trade_rows: int
    previous_trade_volume: int
    one_second: ScreeningArtifactReport
    five_minute: ScreeningArtifactReport

    def as_dict(self) -> dict[str, object]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), ensure_ascii=False, sort_keys=True)


def _expected_config() -> dict[str, object]:
    return {
        "feature_set": {
            "id": FEATURE_VERSION,
            "schema_version": CONFIG_SCHEMA_VERSION,
            "implementation": "systematic_fx.features.screening",
            "source_schema": "GLBX.MDP3/mbp-10",
            "screening_only": True,
            "research_eligible": False,
            "definition_status_available": False,
            "code_snapshot_sha256_required": True,
            "price_encoding": "fixed_int64",
            "price_scale": PRICE_SCALE,
            "undefined_price": UNDEFINED_PRICE,
            "tick_size_raw": TICK_SIZE_RAW,
            "depth_levels": list(DEPTH_LEVELS),
        },
        "selection": {
            "input_type": "ContractSelectionResult",
            "eligible_source_date_must_match": True,
            "selected_footer_mapping_must_match": True,
            "selection_sha256_must_verify": True,
            "previous_volume_sha256_must_verify": True,
            "positive_previous_trade_rows_required": True,
            "positive_previous_trade_volume_required": True,
            "expiry_month_candidate_allowed": False,
        },
        "qualification": {
            "input_type": "Phase1AScreeningCalendar",
            "calendar_relative_path": _CALENDAR_RELATIVE_PATH,
            "source_manifest_relative_path": _SOURCE_MANIFEST_RELATIVE_PATH,
            "qc_manifest_relative_path": _QC_MANIFEST_RELATIVE_PATH,
            "qc_config_path": "configs/data/mbp10_structural_qc_v1.toml",
            "source_date_must_be_calendar_eligible": True,
            "source_record_must_match_raw": True,
            "qc_record_must_be_complete_pass": True,
            "schema_fingerprint_must_match": True,
            "calendar_sha256_must_verify": True,
        },
        "availability": {
            "event_clock": "ts_recv",
            "input_order": "physical Parquet row order across sequential row groups",
            "one_second_width_ns": ONE_SECOND_NS,
            "five_minute_width_ns": FIVE_MINUTE_NS,
            "close_rule": "right_closed",
            "late_event_policy": "ignore rows whose 1s bucket precedes the open bucket",
            "emit_unobserved_seconds": False,
            "forward_fill": False,
            "closed_bucket_rewrite": False,
            "decision_quote_fresh_max_age_ms": QUOTE_FRESH_MAX_AGE_MS,
            "source_start_boundary_policy": (
                "exclude partial right-closed 1s bucket ending at source start"
            ),
            "source_end_boundary_policy": (
                "exclude right-closed 1s and 5m buckets ending at source end as "
                "UNPROVEN_CLOSED_BOUNDARY"
            ),
        },
        "one_second": {
            "book_snapshot": "last selected physical row in the observed second",
            "depth_change": "only versus the immediately adjacent prior valid observed second",
            "recovery_policy": (
                "source starts unknown; MAYBE_BAD_BOOK persists until a valid snapshot or "
                "structurally valid empty reset; the marker second is invalid and only the "
                "exactly adjacent clean base-valid observed second rearms at close"
            ),
            "imbalance_levels": list(DEPTH_LEVELS),
            "imbalance_numerator": "bid_cumulative_size - ask_cumulative_size",
            "imbalance_denominator": "bid_cumulative_size + ask_cumulative_size",
            "imbalance_signed_ppm": (
                "truncate_toward_zero(1000000 * numerator / denominator); null at zero denominator"
            ),
            "price_on_tick_grid_required_for_valid_second": True,
        },
        "five_minute": {
            "source": "proven-boundary closed observed one-second Phase1A rows only",
            "integer_mean": "truncate_toward_zero(sum(values) / count(values))",
            "imbalance_sign_changes": "adjacent changes after zero signs are removed",
            "imbalance_persistence": (
                "terminal equal-sign run divided by valid sign observations in integer ppm"
            ),
            "missing_seconds": "300 - observed_seconds",
            "source_local_signal_input_valid": (
                "complete 300-second source-local valid and fresh window"
            ),
            "signal_input_valid": (
                "source_local_signal_input_valid AND definition_status_available"
            ),
        },
        "output": {
            "one_second_root": "data/derived/features_1s",
            "five_minute_root": "data/derived/research_5m",
            "partition": (
                "version=<feature_version>/contract=<symbol>/"
                "source_date=YYYY-MM-DD/part-000.parquet"
            ),
            "publication": (
                "same-directory fsynced temporary file plus atomic no-overwrite hard link"
            ),
            "existing_identical": "REUSE",
            "existing_different": "REJECT",
            "report_artifact": False,
        },
        "formulas": [
            {"name": name, "definition": definition} for name, definition in SCREENING_FORMULAS
        ],
    }


def load_phase1a_screening_config(
    path: Path | str = DEFAULT_CONFIG_PATH,
) -> ScreeningFeatureConfig:
    requested = Path(path).expanduser()
    if requested.is_symlink():
        raise ScreeningFeaturePathError("feature config cannot be a symbolic link")
    try:
        resolved = requested.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ScreeningFeaturePathError(f"feature config does not exist: {requested}") from exc
    if not stat.S_ISREG(resolved.lstat().st_mode):
        raise ScreeningFeaturePathError(f"feature config must be a regular file: {resolved}")
    raw = resolved.read_bytes()
    try:
        document = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ScreeningFeatureBuildError("feature config is not valid UTF-8 TOML") from exc
    if document != _expected_config():
        raise ScreeningFeatureBuildError(
            "Phase1A feature config semantics drifted; create a new feature version"
        )
    return ScreeningFeatureConfig(
        path=resolved,
        sha256=hashlib.sha256(raw).hexdigest(),
        formula_sha256=FORMULA_SHA256,
    )


def _parse_source_date(value: date | str) -> date:
    if isinstance(value, datetime):
        raise TypeError("source_date must be a date or ISO string, not datetime")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise TypeError("source_date must be a date or ISO string")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ScreeningFeatureBuildError(f"invalid ISO source_date: {value!r}") from exc
    if value != parsed.isoformat():
        raise ScreeningFeatureBuildError("source_date must use canonical ISO format")
    return parsed


def _utc_ns(value: date) -> int:
    return (value - _UNIX_EPOCH_DATE).days * 86_400 * ONE_SECOND_NS


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _validate_input_paths(
    raw_parquet_path: Path | str,
    data_root: Path | str,
    *,
    symbol: str,
    source_date: date,
) -> tuple[Path, Path, Path, Path]:
    root_input = Path(data_root).expanduser()
    if root_input.is_symlink():
        raise ScreeningFeaturePathError("data_root cannot be a symbolic link")
    try:
        root = root_input.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ScreeningFeaturePathError(f"data_root does not exist: {root_input}") from exc
    if not stat.S_ISDIR(root.lstat().st_mode):
        raise ScreeningFeaturePathError("data_root must be a directory")
    if root.name != "data":
        raise ScreeningFeaturePathError("data_root must be the explicit data directory")

    raw_input = Path(raw_parquet_path).expanduser()
    if raw_input.is_symlink():
        raise ScreeningFeaturePathError("raw source cannot be a symbolic link")
    try:
        raw = raw_input.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ScreeningFeaturePathError(f"raw source does not exist: {raw_input}") from exc
    if not stat.S_ISREG(raw.lstat().st_mode) or raw.suffix.lower() != ".parquet":
        raise ScreeningFeaturePathError("raw source must be a regular .parquet file")
    if not _is_relative_to(raw, root):
        raise ScreeningFeaturePathError("raw source must be contained by data_root")

    derived = root / "derived"
    if _is_relative_to(raw, derived.resolve(strict=False)):
        raise ScreeningFeaturePathError("raw source cannot be inside data_root/derived")
    if not _SAFE_SYMBOL.fullmatch(symbol):
        raise ScreeningFeatureBuildError("selected symbol is not one safe 6E outright component")

    partition = (
        f"version={FEATURE_VERSION}",
        f"contract={symbol}",
        f"source_date={source_date.isoformat()}",
        "part-000.parquet",
    )
    one_second = derived.joinpath("features_1s", *partition)
    five_minute = derived.joinpath("research_5m", *partition)
    for target, required_root in (
        (one_second, derived / "features_1s"),
        (five_minute, derived / "research_5m"),
    ):
        if not _is_relative_to(target.resolve(strict=False), required_root.resolve(strict=False)):
            raise ScreeningFeaturePathError(f"output target escapes its derived root: {target}")
        if target.resolve(strict=False) == raw:
            raise ScreeningFeaturePathError("output target resolves to the raw source")
    return root, raw, one_second, five_minute


def _required_sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ScreeningFeatureBuildError(f"{label} must be a lowercase SHA-256")
    return value


def _sha256_file(path: Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ScreeningFeaturePathError(f"cannot safely open source: {path}") from exc
    digest = hashlib.sha256()
    try:
        identity = os.fstat(descriptor)
        if not stat.S_ISREG(identity.st_mode):
            raise ScreeningFeaturePathError(f"source is not a regular file: {path}")
        while chunk := os.read(descriptor, 8 * 1024 * 1024):
            digest.update(chunk)
        after = os.fstat(descriptor)
        if (
            identity.st_dev,
            identity.st_ino,
            identity.st_size,
            identity.st_mtime_ns,
            identity.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise ScreeningFeatureBuildError("raw source changed while hashing")
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def _strict_qualification_file(data_root: Path, relative_path: str, *, label: str) -> Path:
    relative = Path(*relative_path.split("/"))
    target = data_root / relative
    current = data_root
    for component in relative.parts:
        current /= component
        if current.is_symlink():
            raise ScreeningFeaturePathError(f"{label} cannot contain symbolic links")
    try:
        resolved = target.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ScreeningFeaturePathError(f"{label} does not exist: {target}") from exc
    if not _is_relative_to(resolved, data_root) or not stat.S_ISREG(resolved.lstat().st_mode):
        raise ScreeningFeaturePathError(f"{label} must be a regular file below data_root")
    return resolved


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ScreeningFeatureBuildError(f"duplicate JSON key in qualification evidence: {key}")
        result[key] = value
    return result


def _load_canonical_jsonl(
    path: Path,
    *,
    label: str,
    expected_fields: frozenset[str],
) -> tuple[str, list[dict[str, object]]]:
    digest = hashlib.sha256()
    records: list[dict[str, object]] = []
    with path.open("rb") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            digest.update(raw_line)
            if not raw_line.endswith(b"\n"):
                raise ScreeningFeatureBuildError(
                    f"{label} line {line_number} is not newline-terminated"
                )
            try:
                value = json.loads(raw_line, object_pairs_hook=_reject_duplicate_json_keys)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ScreeningFeatureBuildError(
                    f"{label} line {line_number} is not valid JSON"
                ) from exc
            if not isinstance(value, dict) or frozenset(value) != expected_fields:
                raise ScreeningFeatureBuildError(f"{label} line {line_number} has invalid fields")
            if raw_line != _canonical_json(value) + b"\n":
                raise ScreeningFeatureBuildError(
                    f"{label} line {line_number} is not canonical JSONL"
                )
            records.append(value)
    if not records:
        raise ScreeningFeatureBuildError(f"{label} cannot be empty")
    return digest.hexdigest(), records


def _required_nonnegative_int(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ScreeningFeatureBuildError(f"{label} must be a nonnegative integer")
    return value


@dataclass(frozen=True, slots=True)
class _QualificationEvidence:
    source_sha256: str
    source_byte_size: int
    source_rows: int
    first_ts_recv_ns: int
    last_ts_recv_ns: int
    snapshot_rows: int
    source_manifest_sha256: str
    qc_manifest_sha256: str
    qc_config_sha256: str
    schema_fingerprint: str
    calendar_sha256: str


def _verify_planned_no_entry_source(
    raw: Path,
    *,
    qualification: _QualificationEvidence,
    selection: ContractSelectionResult,
    source_date: date,
    allow_zero_previous_trade_volume: bool,
) -> None:
    """Fully bind a planned no-entry decision to the qualified source and selection."""

    if _sha256_file(raw) != qualification.source_sha256:
        raise ScreeningFeatureBuildError("raw source SHA-256 disagrees with source manifest")
    try:
        parquet = pq.ParquetFile(raw)
    except (OSError, pa.ArrowException) as exc:
        raise ScreeningFeatureBuildError(f"cannot open raw Parquet: {raw}") from exc
    _, _, source_schema_sha = _verify_selection(
        selection,
        source_date=source_date,
        parquet=parquet,
        allow_zero_previous_trade_volume=allow_zero_previous_trade_volume,
    )
    if source_schema_sha != qualification.schema_fingerprint:
        raise ScreeningFeatureBuildError(
            "raw schema fingerprint disagrees with Phase1A calendar/QC evidence"
        )


def _verify_qualification_evidence(
    *,
    data_root: Path,
    raw: Path,
    source_date: date,
    calendar: Phase1AScreeningCalendar,
) -> _QualificationEvidence:
    if not isinstance(calendar, Phase1AScreeningCalendar):
        raise TypeError("calendar must be a Phase1AScreeningCalendar")
    if source_date not in calendar.source_dates:
        raise ScreeningFeatureBuildError("source_date is not eligible in the Phase1A calendar")

    calendar_path = _strict_qualification_file(
        data_root,
        _CALENDAR_RELATIVE_PATH,
        label="Phase1A calendar artifact",
    )
    canonical_calendar = calendar.canonical_json()
    calendar_sha256 = hashlib.sha256(canonical_calendar).hexdigest()
    if calendar.sha256 != calendar_sha256 or calendar_path.read_bytes() != canonical_calendar:
        raise ScreeningFeatureBuildError("Phase1A calendar artifact or SHA-256 drift")
    if DEFAULT_QC_CONFIG_PATH.is_symlink():
        raise ScreeningFeaturePathError("structural QC config cannot be a symbolic link")
    try:
        qc_config_path = DEFAULT_QC_CONFIG_PATH.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ScreeningFeaturePathError("structural QC config does not exist") from exc
    if not stat.S_ISREG(qc_config_path.lstat().st_mode):
        raise ScreeningFeaturePathError("structural QC config must be a regular file")
    try:
        qc_config_sha256 = load_structural_qc_config(qc_config_path).sha256
    except StructuralQcError as exc:
        raise ScreeningFeatureBuildError("structural QC config is invalid") from exc
    if qc_config_sha256 != calendar.qc_config_sha256:
        raise ScreeningFeatureBuildError(
            "structural QC semantic config SHA-256 disagrees with calendar"
        )

    source_manifest_path = _strict_qualification_file(
        data_root,
        _SOURCE_MANIFEST_RELATIVE_PATH,
        label="source manifest",
    )
    source_manifest_sha256, source_records = _load_canonical_jsonl(
        source_manifest_path,
        label="source manifest",
        expected_fields=_SOURCE_MANIFEST_FIELDS,
    )
    if source_manifest_sha256 != calendar.source_manifest_sha256:
        raise ScreeningFeatureBuildError("source manifest SHA-256 disagrees with calendar")

    qc_manifest_path = _strict_qualification_file(
        data_root,
        _QC_MANIFEST_RELATIVE_PATH,
        label="full structural QC manifest",
    )
    qc_manifest_sha256, qc_records = _load_canonical_jsonl(
        qc_manifest_path,
        label="full structural QC manifest",
        expected_fields=_QC_MANIFEST_FIELDS,
    )
    if qc_manifest_sha256 != calendar.qc_manifest_sha256:
        raise ScreeningFeatureBuildError("QC manifest SHA-256 disagrees with calendar")
    if len(source_records) != calendar.source_record_count or len(qc_records) != len(
        source_records
    ):
        raise ScreeningFeatureBuildError("calendar/source/QC manifest record counts disagree")

    expected_dates = tuple(sorted((*calendar.source_dates, *calendar.excluded_source_dates)))
    observed_dates: list[date] = []
    eligible = frozenset(calendar.source_dates)
    excluded = frozenset(calendar.excluded_source_dates)
    pass_count = 0
    fail_count = 0
    selected_source: dict[str, object] | None = None
    selected_qc: dict[str, object] | None = None
    previous_uri: str | None = None
    for line_number, (source, qc) in enumerate(
        zip(source_records, qc_records, strict=True), start=1
    ):
        source_date_text = source["source_date"]
        if not isinstance(source_date_text, str):
            raise ScreeningFeatureBuildError(
                f"source manifest line {line_number} source_date must be an ISO string"
            )
        try:
            record_date = date.fromisoformat(source_date_text)
        except ValueError as exc:
            raise ScreeningFeatureBuildError(
                f"source manifest line {line_number} has invalid source_date"
            ) from exc
        if source_date_text != record_date.isoformat():
            raise ScreeningFeatureBuildError(
                f"source manifest line {line_number} source_date is not canonical"
            )
        relative_uri = source["relative_uri"]
        if not isinstance(relative_uri, str) or not relative_uri:
            raise ScreeningFeatureBuildError(
                f"source manifest line {line_number} relative_uri is invalid"
            )
        if previous_uri is not None and relative_uri <= previous_uri:
            raise ScreeningFeatureBuildError("source manifest relative_uri order drift")
        previous_uri = relative_uri
        source_sha = _required_sha256(
            source["sha256"], label=f"source manifest line {line_number} sha256"
        )
        source_size = _required_nonnegative_int(
            source["byte_size"], label=f"source manifest line {line_number} byte_size"
        )
        observed_dates.append(record_date)

        if (
            qc["artifact_schema"] != _QC_ARTIFACT_SCHEMA
            or qc["checker_version"] != _QC_CHECKER_VERSION
            or qc["relative_uri"] != relative_uri
            or qc["source_date"] != source_date_text
            or qc["source_sha256"] != source_sha
            or qc["source_byte_size"] != source_size
            or qc["source_manifest_sha256"] != source_manifest_sha256
            or qc["config_sha256"] != calendar.qc_config_sha256
            or qc["schema_fingerprint"] != calendar.schema_fingerprint
            or qc["coverage_complete"] is not True
            or qc["research_eligible"] is not False
            or qc["scanned_row_count"] != qc["expected_row_count"]
            or qc["scanned_row_group_count"] != qc["expected_row_group_count"]
        ):
            raise ScreeningFeatureBuildError(
                f"source/QC qualification identity drift on line {line_number}"
            )
        hard_count = _required_nonnegative_int(
            qc["hard_violation_count"],
            label=f"QC line {line_number} hard_violation_count",
        )
        if record_date in eligible:
            if qc["result"] != "PASS" or hard_count != 0:
                raise ScreeningFeatureBuildError(
                    f"eligible source date lacks complete structural QC PASS: {source_date_text}"
                )
            pass_count += 1
        elif record_date in excluded:
            if qc["result"] != "FAIL" or hard_count <= 0:
                raise ScreeningFeatureBuildError(
                    f"frozen excluded source date lost its QC FAIL: {source_date_text}"
                )
            fail_count += 1
        else:
            raise ScreeningFeatureBuildError(
                f"manifest source date is absent from the Phase1A calendar: {source_date_text}"
            )
        if record_date == source_date:
            if selected_source is not None:
                raise ScreeningFeatureBuildError("source_date appears more than once in manifests")
            selected_source = source
            selected_qc = qc

    if tuple(observed_dates) != expected_dates:
        raise ScreeningFeatureBuildError("calendar and manifest source-date coverage disagree")
    if (pass_count, fail_count) != (
        calendar.qc_pass_record_count,
        calendar.qc_fail_record_count,
    ):
        raise ScreeningFeatureBuildError("calendar and QC PASS/FAIL counts disagree")
    if selected_source is None or selected_qc is None:  # pragma: no cover - eligibility guard
        raise ScreeningFeatureBuildError("eligible source date is absent from manifests")

    try:
        raw_relative_uri = raw.relative_to(data_root / "mbp-10").as_posix()
    except ValueError as exc:
        raise ScreeningFeaturePathError("raw source must be below data_root/mbp-10") from exc
    if selected_source["relative_uri"] != raw_relative_uri:
        raise ScreeningFeatureBuildError("raw source path disagrees with source manifest record")
    source_rows = _required_nonnegative_int(
        selected_qc["expected_row_count"], label="qualified QC expected_row_count"
    )
    first_ts_recv_ns = _required_nonnegative_int(
        selected_qc["first_ts_recv_ns"], label="qualified QC first_ts_recv_ns"
    )
    last_ts_recv_ns = _required_nonnegative_int(
        selected_qc["last_ts_recv_ns"], label="qualified QC last_ts_recv_ns"
    )
    diagnostics = selected_qc["diagnostic_counts"]
    if not isinstance(diagnostics, dict):
        raise ScreeningFeatureBuildError("qualified QC diagnostic_counts must be an object")
    snapshot_rows = _required_nonnegative_int(
        diagnostics.get("snapshot_flag_rows", 0),
        label="qualified QC snapshot_flag_rows",
    )
    if source_rows <= 0 or first_ts_recv_ns > last_ts_recv_ns or snapshot_rows > source_rows:
        raise ScreeningFeatureBuildError("qualified QC source envelope is invalid")
    return _QualificationEvidence(
        source_sha256=_required_sha256(selected_source["sha256"], label="qualified source SHA-256"),
        source_byte_size=_required_nonnegative_int(
            selected_source["byte_size"], label="qualified source byte_size"
        ),
        source_rows=source_rows,
        first_ts_recv_ns=first_ts_recv_ns,
        last_ts_recv_ns=last_ts_recv_ns,
        snapshot_rows=snapshot_rows,
        source_manifest_sha256=source_manifest_sha256,
        qc_manifest_sha256=qc_manifest_sha256,
        qc_config_sha256=calendar.qc_config_sha256,
        schema_fingerprint=calendar.schema_fingerprint,
        calendar_sha256=calendar_sha256,
    )


def plan_phase1a_screening_no_entry_reason(
    raw_parquet_path: Path | str,
    *,
    data_root: Path | str,
    source_date: date | str,
    selection: ContractSelectionResult,
    calendar: Phase1AScreeningCalendar,
    config_path: Path | str = DEFAULT_CONFIG_PATH,
) -> str | None:
    """Classify one narrowly proven whole-session no-entry condition before a run.

    The planner recognizes either a canonical selection with no positive
    previous-source trade evidence or the narrow snapshot-only start condition
    proven by hash-bound structural QC.  All other feature-build errors remain
    fail-closed in the builder.
    """

    parsed_date = _parse_source_date(source_date)
    load_phase1a_screening_config(config_path)
    if not isinstance(selection, ContractSelectionResult):
        raise TypeError("selection must be a ContractSelectionResult")
    selected = selection.selected
    root, raw, _, _ = _validate_input_paths(
        raw_parquet_path,
        data_root,
        symbol=selected.raw_symbol,
        source_date=parsed_date,
    )
    qualification = _verify_qualification_evidence(
        data_root=root,
        raw=raw,
        source_date=parsed_date,
        calendar=calendar,
    )
    selection_document = _verify_canonical_audit_bytes(
        selection.canonical_bytes,
        selection.sha256,
        label="contract selection",
    )
    _verify_canonical_audit_bytes(
        selection.previous_volume.canonical_bytes,
        selection.previous_volume.sha256,
        label="previous volume",
    )
    if (
        selection.eligible_source_date != parsed_date
        or selection.eligible_source_sha256 != qualification.source_sha256
        or selection_document.get("eligible_source_date") != parsed_date.isoformat()
        or selection_document.get("eligible_source_sha256") != qualification.source_sha256
        or selection_document.get("selected") != selected.as_dict()
    ):
        raise ScreeningFeatureBuildError("planning selection and qualified source differ")
    if raw.stat().st_size != qualification.source_byte_size:
        raise ScreeningFeatureBuildError("raw source byte-size disagrees with source manifest")

    previous_trade_rows = selected.previous_trade_rows
    previous_trade_volume = selected.previous_trade_volume
    if (
        isinstance(previous_trade_rows, bool)
        or not isinstance(previous_trade_rows, int)
        or previous_trade_rows < 0
        or isinstance(previous_trade_volume, bool)
        or not isinstance(previous_trade_volume, int)
        or previous_trade_volume < 0
    ):
        raise ScreeningFeatureBuildError(
            "selected contract prior trade rows and volume must be nonnegative integers"
        )
    if (previous_trade_rows == 0) != (previous_trade_volume == 0):
        raise ScreeningFeatureBuildError(
            "selected contract prior trade rows and volume positivity disagree"
        )
    if previous_trade_volume == 0:
        _verify_planned_no_entry_source(
            raw,
            qualification=qualification,
            selection=selection,
            source_date=parsed_date,
            allow_zero_previous_trade_volume=True,
        )
        return NO_POSITIVE_PREVIOUS_SOURCE_TRADE_VOLUME

    source_start_ns = _utc_ns(parsed_date)
    snapshot_start_boundary_only = (
        qualification.snapshot_rows == qualification.source_rows
        and qualification.first_ts_recv_ns == source_start_ns
        and qualification.last_ts_recv_ns == source_start_ns
    )
    if not snapshot_start_boundary_only:
        return None
    _verify_planned_no_entry_source(
        raw,
        qualification=qualification,
        selection=selection,
        source_date=parsed_date,
        allow_zero_previous_trade_volume=False,
    )
    return NO_PROVEN_COMPLETE_OBSERVED_1S_BUCKET


def _verify_canonical_audit_bytes(payload: bytes, sha256: str, *, label: str) -> dict[str, object]:
    if hashlib.sha256(payload).hexdigest() != sha256:
        raise ScreeningFeatureBuildError(f"{label} SHA-256 mismatch")
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ScreeningFeatureBuildError(f"{label} canonical bytes are invalid JSON") from exc
    if not isinstance(document, dict) or _canonical_json(document) != payload:
        raise ScreeningFeatureBuildError(f"{label} bytes are not canonical JSON")
    return document


def _verify_selection(
    selection: ContractSelectionResult,
    *,
    source_date: date,
    parquet: pq.ParquetFile,
    allow_zero_previous_trade_volume: bool = False,
) -> tuple[int, str, str]:
    if not isinstance(selection, ContractSelectionResult):
        raise TypeError("selection must be a ContractSelectionResult")
    selection_document = _verify_canonical_audit_bytes(
        selection.canonical_bytes,
        selection.sha256,
        label="contract selection",
    )
    previous_document = _verify_canonical_audit_bytes(
        selection.previous_volume.canonical_bytes,
        selection.previous_volume.sha256,
        label="previous volume",
    )
    if selection.eligible_source_date != source_date:
        raise ScreeningFeatureBuildError("selection eligible_source_date != source_date")
    if selection.previous_source_date >= source_date:
        raise ScreeningFeatureBuildError("selection previous_source_date must precede source_date")
    if selection.previous_volume.source_date != selection.previous_source_date:
        raise ScreeningFeatureBuildError("selection previous-volume source date drift")
    if not selection.candidates or selection.selected != selection.candidates[0]:
        raise ScreeningFeatureBuildError("selection selected candidate must be ranked first")
    if selection.candidates.count(selection.selected) != 1:
        raise ScreeningFeatureBuildError("selection selected candidate must be unique")
    selected = selection.selected
    if (
        selected.previous_trade_rows < 0
        or selected.previous_trade_volume < 0
        or (selected.previous_trade_rows == 0) != (selected.previous_trade_volume == 0)
    ):
        raise ScreeningFeatureBuildError(
            "selected contract prior trade rows and volume positivity disagree"
        )
    if not allow_zero_previous_trade_volume and (
        selected.previous_trade_rows == 0 or selected.previous_trade_volume == 0
    ):
        raise ScreeningFeatureBuildError("selected contract requires positive prior trade volume")
    if not 0 <= selected.instrument_id <= _UINT32_MAX:
        raise ScreeningFeatureBuildError("selected instrument_id is outside uint32")
    if selected.contract_month <= date(source_date.year, source_date.month, 1):
        raise ScreeningFeatureBuildError("selected contract violates expiry-month exclusion")

    expected_selected = selected.as_dict()
    if (
        selection_document.get("artifact_schema") != CONTRACT_SELECTION_SCHEMA
        or selection_document.get("policy_version") != CONTRACT_SELECTION_POLICY_VERSION
        or selection_document.get("information_boundary")
        != {
            "eligible_source_rows_read": False,
            "volume_source": "PREVIOUS_SOURCE_DATE_ONLY",
        }
        or selection_document.get("eligible_source_date") != source_date.isoformat()
        or selection_document.get("previous_source_date")
        != selection.previous_source_date.isoformat()
        or selection_document.get("previous_volume_sha256") != selection.previous_volume.sha256
        or selection_document.get("selected") != expected_selected
        or selection_document.get("candidates")
        != [candidate.as_dict() for candidate in selection.candidates]
    ):
        raise ScreeningFeatureBuildError("contract selection canonical document drift")
    if previous_document.get("source_date") != selection.previous_source_date.isoformat():
        raise ScreeningFeatureBuildError("previous volume canonical document date drift")
    if (
        previous_document.get("artifact_schema") != f"{CONTRACT_SELECTION_SCHEMA}.previous_volume"
        or previous_document.get("policy_version") != CONTRACT_SELECTION_POLICY_VERSION
        or previous_document.get("contracts")
        != [contract.as_dict() for contract in selection.previous_volume.contracts]
    ):
        raise ScreeningFeatureBuildError("previous volume canonical document drift")
    volume_matches = [
        contract
        for contract in selection.previous_volume.contracts
        if contract.contract_month == selected.contract_month
    ]
    if len(volume_matches) > 1:
        raise ScreeningFeatureBuildError(
            "selected candidate prior volume disagrees with canonical previous-volume evidence"
        )
    expected_trade_rows = volume_matches[0].trade_rows if volume_matches else 0
    expected_trade_volume = volume_matches[0].trade_volume if volume_matches else 0
    if (
        expected_trade_rows != selected.previous_trade_rows
        or expected_trade_volume != selected.previous_trade_volume
    ):
        raise ScreeningFeatureBuildError(
            "selected candidate prior volume disagrees with canonical previous-volume evidence"
        )

    try:
        validate_mbp10_contract(parquet.schema_arrow)
        metadata_payload = (parquet.schema_arrow.metadata or {})[b"dbn.metadata"]
        metadata = decode_dbn_metadata(metadata_payload)
        mappings = parse_instrument_mappings(metadata_payload)
    except (KeyError, Mbp10ContractError) as exc:
        raise ScreeningFeatureBuildError(f"invalid raw MBP-10 footer: {exc}") from exc
    start = metadata.get("start")
    end = metadata.get("end")
    if (
        isinstance(start, bool)
        or not isinstance(start, int)
        or isinstance(end, bool)
        or not isinstance(end, int)
        or start != _utc_ns(source_date)
        or end != _utc_ns(source_date + timedelta(days=1))
    ):
        raise ScreeningFeatureBuildError(
            "raw footer must describe the exact selected UTC source-date interval"
        )
    active_for_id = [
        mapping
        for mapping in mappings
        if mapping.interval_start <= source_date < mapping.interval_end
        and mapping.instrument_id == selected.instrument_id
    ]
    exact = [
        mapping
        for mapping in active_for_id
        if mapping.raw_symbol == selected.raw_symbol and mapping.kind is InstrumentKind.OUTRIGHT
    ]
    if len(active_for_id) != 1 or len(exact) != 1:
        raise ScreeningFeatureBuildError(
            "selected eligible date/id/symbol does not match exactly one active footer outright"
        )
    return (
        selected.instrument_id,
        selected.raw_symbol,
        compute_schema_fingerprint(parquet.schema_arrow),
    )


_BOOK_AUDIT_COLUMNS: Final = tuple(
    f"{side}_{kind}_{level:02d}"
    for level in range(10)
    for side in ("bid", "ask")
    for kind in ("px", "sz", "ct")
)
_RecoveryOutcome = Literal["RECOVERED", "INVALIDATED"]


@dataclass(slots=True)
class _ObservedSecondAudit:
    last_ts_recv: int
    flags_or: int = 0
    recovery_marker_seen: bool = False
    last_recovery_outcome: _RecoveryOutcome | None = None


def _row_book_structure(
    columns: dict[str, list[object]],
    index: int,
) -> tuple[bool, bool]:
    structurally_valid = True
    empty = True
    prior_price: dict[str, int | None] = {"bid": None, "ask": None}
    undefined_seen = {"bid": False, "ask": False}
    try:
        for level in range(10):
            suffix = f"{level:02d}"
            for side in ("bid", "ask"):
                price = int(columns[f"{side}_px_{suffix}"][index])
                size = int(columns[f"{side}_sz_{suffix}"][index])
                count = int(columns[f"{side}_ct_{suffix}"][index])
                defined = price != UNDEFINED_PRICE
                if defined:
                    empty = False
                    if undefined_seen[side] or size <= 0 or count <= 0 or count > size:
                        structurally_valid = False
                    previous = prior_price[side]
                    if previous is not None and (
                        (side == "bid" and price >= previous)
                        or (side == "ask" and price <= previous)
                    ):
                        structurally_valid = False
                    prior_price[side] = price
                else:
                    undefined_seen[side] = True
                    if size != 0 or count != 0:
                        structurally_valid = False
    except (TypeError, ValueError, OverflowError) as exc:
        raise ScreeningFeatureBuildError(
            "selected recovery audit contains invalid book values"
        ) from exc
    return structurally_valid, empty


def _selected_second_audits(
    parquet: pq.ParquetFile,
    *,
    instrument_id: int,
    source_date: date,
) -> tuple[dict[int, _ObservedSecondAudit], int, int]:
    day_start = _utc_ns(source_date)
    day_end = _utc_ns(source_date + timedelta(days=1))
    current_bucket: int | None = None
    audits: dict[int, _ObservedSecondAudit] = {}
    selected_rows = 0
    late_rows = 0
    for row_group_index in range(parquet.num_row_groups):
        table = parquet.read_row_group(
            row_group_index,
            columns=[
                "ts_recv",
                "instrument_id",
                "action",
                "side",
                "flags",
                *_BOOK_AUDIT_COLUMNS,
            ],
            use_threads=False,
        )
        columns = {name: table[name].to_pylist() for name in table.column_names}
        timestamps = pc.cast(table["ts_recv"], pa.int64()).to_pylist()
        for index, timestamp_value in enumerate(timestamps):
            if int(columns["instrument_id"][index]) != instrument_id:
                continue
            selected_rows += 1
            timestamp_ns = int(timestamp_value)
            if not day_start <= timestamp_ns < day_end:
                raise ScreeningFeatureBuildError("selected ts_recv lies outside source_date")
            bucket = pilot._right_closed_bucket_end_ns(timestamp_ns, ONE_SECOND_NS)
            if current_bucket is not None and bucket < current_bucket:
                late_rows += 1
                continue
            current_bucket = bucket
            audit = audits.get(bucket)
            if audit is None:
                audit = _ObservedSecondAudit(last_ts_recv=timestamp_ns)
                audits[bucket] = audit
            audit.last_ts_recv = timestamp_ns
            flags = int(columns["flags"][index])
            action = str(columns["action"][index])
            side = str(columns["side"][index])
            audit.flags_or |= flags
            local_valid, empty = _row_book_structure(columns, index)
            snapshot = bool(flags & pilot.F_SNAPSHOT)
            reset = action == "R"
            if snapshot or reset:
                audit.recovery_marker_seen = True
            invalid_reset = reset and (side != "N" or not empty)
            if flags & pilot.F_MAYBE_BAD_BOOK or not local_valid or invalid_reset:
                audit.last_recovery_outcome = "INVALIDATED"
            elif snapshot or reset:
                audit.last_recovery_outcome = "RECOVERED"
    return audits, selected_rows, late_rows


def _safe_int64(value: int, *, label: str) -> int:
    if not _INT64_MIN <= value <= _INT64_MAX:
        raise ScreeningFeatureBuildError(f"{label} exceeds int64")
    return value


def _trunc_div(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise ValueError("denominator must be positive")
    if numerator >= 0:
        return numerator // denominator
    return -((-numerator) // denominator)


def _signed_ppm(numerator: int, denominator: int) -> int | None:
    if denominator == 0:
        return None
    result = _trunc_div(numerator * 1_000_000, denominator)
    if not -(10**6) <= result <= 10**6:
        raise ScreeningFeatureBuildError("imbalance signed ppm is outside [-1000000,1000000]")
    return result


def _exact_ticks(value: object) -> int | None:
    if value is None:
        return None
    raw = int(value)
    if raw % TICK_SIZE_RAW:
        return None
    return raw // TICK_SIZE_RAW


def _exclude_source_boundary_records(
    rows: list[dict[str, object]],
    *,
    source_date: date,
) -> tuple[list[dict[str, object]], int, int]:
    day_start = _utc_ns(source_date)
    day_end = _utc_ns(source_date + timedelta(days=1))
    kept: list[dict[str, object]] = []
    start_partial = 0
    unproven_end = 0
    for row in rows:
        bucket_end = int(row["bucket_end"])
        if bucket_end == day_start:
            start_partial += 1
        elif bucket_end == day_end:
            unproven_end += 1
        elif day_start < bucket_end < day_end:
            kept.append(row)
        else:
            raise ScreeningFeatureBuildError("1s bucket lies outside source boundary policy")
    if not kept:
        raise ScreeningFeatureBuildError("source has no proven complete observed 1s bucket")
    return kept, start_partial, unproven_end


def _enrich_one_second_records(
    pilot_rows: list[dict[str, object]],
    *,
    source_date: date,
    instrument_id: int,
    symbol: str,
    audits_by_bucket: dict[int, _ObservedSecondAudit],
) -> list[dict[str, object]]:
    enriched: list[dict[str, object]] = []
    recovery_required = True
    expected_rearm_bucket: int | None = None
    prior_valid_bucket: int | None = None
    prior_depths: dict[str, int] = {}

    for pilot_row in pilot_rows:
        bucket_end = int(pilot_row["bucket_end"])
        try:
            audit = audits_by_bucket[bucket_end]
        except KeyError as exc:
            raise ScreeningFeatureBuildError(
                "event-order audit is missing an emitted second"
            ) from exc
        last_ts_recv = audit.last_ts_recv
        bid_ticks = _exact_ticks(pilot_row["bid_px_00_raw"])
        ask_ticks = _exact_ticks(pilot_row["ask_px_00_raw"])
        spread_ticks = _exact_ticks(pilot_row["spread_raw"])
        bbo_defined = (
            pilot_row["bid_px_00_raw"] is not None and pilot_row["ask_px_00_raw"] is not None
        )
        price_on_tick_grid = bbo_defined and bid_ticks is not None and ask_ticks is not None
        reset_seen = int(pilot_row["action_r_count"]) > 0
        flags_or = audit.flags_or
        maybe_bad_book = bool(flags_or & pilot.F_MAYBE_BAD_BOOK)
        bad_ts_recv = bool(flags_or & pilot.F_BAD_TS_RECV)
        base_book_valid = (
            not bool(pilot_row["book_missing"])
            and not bool(pilot_row["locked_book"])
            and not bool(pilot_row["crossed_book"])
            and not maybe_bad_book
            and not bad_ts_recv
            and not reset_seen
            and not bool(pilot_row["price_arithmetic_overflow"])
            and price_on_tick_grid
        )
        recovery_required_at_open = recovery_required
        recovery_rearmed = False
        if audit.last_recovery_outcome == "RECOVERED":
            recovery_required = True
            expected_rearm_bucket = bucket_end + ONE_SECOND_NS
        elif audit.last_recovery_outcome == "INVALIDATED":
            recovery_required = True
            expected_rearm_bucket = None
        elif recovery_required:
            if (
                expected_rearm_bucket == bucket_end
                and base_book_valid
                and not audit.recovery_marker_seen
            ):
                recovery_required = False
                expected_rearm_bucket = None
                recovery_rearmed = True
            else:
                expected_rearm_bucket = None
        valid_second = base_book_valid and not audit.recovery_marker_seen and not recovery_required

        result = dict(pilot_row)
        result.pop("research_eligible", None)
        result.pop("reset_row", None)
        result.update(
            {
                "feature_version": FEATURE_VERSION,
                "screening_only": SCREENING_ONLY,
                "definition_status_available": DEFINITION_STATUS_AVAILABLE,
                "source_date": source_date,
                "contract": symbol,
                "instrument_id": instrument_id,
                "last_ts_recv": last_ts_recv,
                "bid_px_00_ticks": bid_ticks,
                "ask_px_00_ticks": ask_ticks,
                "spread_ticks": spread_ticks,
                "base_book_valid": base_book_valid,
                "valid_second": valid_second,
                "snapshot_row": bool(flags_or & pilot.F_SNAPSHOT),
                "reset_seen": reset_seen,
                "recovery_required_at_open": recovery_required_at_open,
                "recovery_marker_seen": audit.recovery_marker_seen,
                "recovery_rearmed": recovery_rearmed,
                "recovery_required_at_close": recovery_required,
                "price_on_tick_grid": price_on_tick_grid,
                "maybe_bad_book": maybe_bad_book,
                "bad_ts_recv": bad_ts_recv,
            }
        )

        current_depths: dict[str, int] = {}
        adjacent_prior = (
            valid_second
            and prior_valid_bucket is not None
            and bucket_end - prior_valid_bucket == ONE_SECOND_NS
        )
        for level in DEPTH_LEVELS:
            bid_depth = int(pilot_row[f"bid_cum_size_l{level}"])
            ask_depth = int(pilot_row[f"ask_cum_size_l{level}"])
            numerator = _safe_int64(
                bid_depth - ask_depth,
                label=f"imbalance numerator L{level}",
            )
            denominator = bid_depth + ask_depth
            if denominator >= 2**64:
                raise ScreeningFeatureBuildError(f"imbalance denominator L{level} exceeds uint64")
            result[f"imbalance_numerator_l{level}"] = numerator
            result[f"imbalance_denominator_l{level}"] = denominator
            result[f"imbalance_signed_ppm_l{level}"] = _signed_ppm(numerator, denominator)
            for side, value in (("bid", bid_depth), ("ask", ask_depth)):
                key = f"{side}_cum_size_l{level}"
                current_depths[key] = value
                result[f"{side}_depth_change_l{level}"] = (
                    _safe_int64(value - prior_depths[key], label=f"{side} depth change L{level}")
                    if adjacent_prior
                    else None
                )
            prior_numerator_key = f"imbalance_numerator_l{level}"
            current_depths[prior_numerator_key] = numerator
            result[f"imbalance_numerator_change_l{level}"] = (
                _safe_int64(
                    numerator - prior_depths[prior_numerator_key],
                    label=f"imbalance numerator change L{level}",
                )
                if adjacent_prior
                else None
            )

        if valid_second:
            age_ns = bucket_end - last_ts_recv
            if not 0 <= age_ns <= ONE_SECOND_NS:
                raise ScreeningFeatureBuildError(
                    "valid second quote timestamp is outside its bucket"
                )
            quote_age_ms = age_ns // 1_000_000
            quote_fresh = age_ns <= QUOTE_FRESH_MAX_AGE_NS
            prior_valid_bucket = bucket_end
            prior_depths = current_depths
        else:
            quote_age_ms = None
            quote_fresh = False
            prior_valid_bucket = None
            prior_depths = {}
        result["quote_age_ms"] = quote_age_ms
        result["quote_fresh"] = quote_fresh
        result["stale_second"] = valid_second and not quote_fresh
        enriched.append(result)
    return enriched


def _integer_summary(values: list[int]) -> dict[str, int | None]:
    if not values:
        return {name: None for name in _SUMMARY_STAT_NAMES}
    return {
        "first": values[0],
        "last": values[-1],
        "min": min(values),
        "max": max(values),
        "mean_trunc": _trunc_div(sum(values), len(values)),
    }


def _put_summary(
    result: dict[str, object],
    *,
    prefix: str,
    values: list[int],
) -> None:
    for name, value in _integer_summary(values).items():
        result[f"{prefix}_{name}"] = value


def _flow_totals(rows: list[dict[str, object]]) -> dict[str, int]:
    return {name: sum(int(row[name]) for row in rows) for name in _FLOW_NAMES}


def _five_minute_record(
    rows: list[dict[str, object]],
    *,
    bucket_end: int,
    source_date: date,
) -> dict[str, object]:
    observed = len(rows)
    if not 1 <= observed <= 300:
        raise ScreeningFeatureBuildError("5m bucket must contain 1..300 observed seconds")
    first = rows[0]
    last = rows[-1]
    valid_rows = [row for row in rows if bool(row["valid_second"])]
    mids = [int(row["mid_px_x2_raw"]) for row in valid_rows]
    spreads = [int(row["spread_raw"]) for row in valid_rows]
    last_quote_row = valid_rows[-1] if valid_rows else None
    if last_quote_row is None:
        last_quote_ts = None
        decision_age_ms = None
        decision_fresh = False
    else:
        last_quote_ts = int(last_quote_row["last_ts_recv"])
        age_ns = bucket_end - last_quote_ts
        if age_ns < 0:
            raise ScreeningFeatureBuildError("decision quote occurs after bucket close")
        decision_age_ms = age_ns // 1_000_000
        decision_fresh = age_ns <= QUOTE_FRESH_MAX_AGE_NS

    day_start = _utc_ns(source_date)
    day_end = _utc_ns(source_date + timedelta(days=1))
    source_window_complete = bucket_end - FIVE_MINUTE_NS >= day_start and bucket_end < day_end
    valid_seconds = len(valid_rows)
    stale_seconds = sum(bool(row["stale_second"]) for row in rows)
    reset_seen_seconds = sum(bool(row["reset_seen"]) for row in rows)
    recovery_marker_seconds = sum(bool(row["recovery_marker_seen"]) for row in rows)
    recovery_required_seconds = sum(bool(row["recovery_required_at_close"]) for row in rows)
    recovery_rearmed_seconds = sum(bool(row["recovery_rearmed"]) for row in rows)
    locked_seconds = sum(bool(row["locked_book"]) for row in rows)
    crossed_seconds = sum(bool(row["crossed_book"]) for row in rows)
    source_local_valid = (
        source_window_complete
        and observed == 300
        and valid_seconds == 300
        and stale_seconds == 0
        and reset_seen_seconds == 0
        and recovery_marker_seconds == 0
        and recovery_required_seconds == 0
        and locked_seconds == 0
        and crossed_seconds == 0
        and decision_fresh
    )

    result: dict[str, object] = {
        "feature_version": FEATURE_VERSION,
        "screening_only": SCREENING_ONLY,
        "definition_status_available": DEFINITION_STATUS_AVAILABLE,
        "source_date": source_date,
        "contract": first["contract"],
        "instrument_id": first["instrument_id"],
        "bucket_end": bucket_end,
        "first_1s_bucket_end": first["bucket_end"],
        "last_1s_bucket_end": last["bucket_end"],
        "last_source_row": last["source_last_row"],
        "last_valid_quote_ts_recv": last_quote_ts,
        "decision_quote_age_ms": decision_age_ms,
        "decision_quote_fresh": decision_fresh,
        "last_bid_px_00_raw": last["bid_px_00_raw"],
        "last_ask_px_00_raw": last["ask_px_00_raw"],
        "last_bid_px_00_ticks": last["bid_px_00_ticks"],
        "last_ask_px_00_ticks": last["ask_px_00_ticks"],
        "last_spread_raw": last["spread_raw"],
        "last_spread_ticks": last["spread_ticks"],
        "mid_px_x2_raw_open": mids[0] if mids else None,
        "mid_px_x2_raw_high": max(mids) if mids else None,
        "mid_px_x2_raw_low": min(mids) if mids else None,
        "mid_px_x2_raw_close": mids[-1] if mids else None,
        "mid_px_x2_raw_mean_trunc": _trunc_div(sum(mids), len(mids)) if mids else None,
        "spread_raw_first": spreads[0] if spreads else None,
        "spread_raw_last": spreads[-1] if spreads else None,
        "spread_raw_min": min(spreads) if spreads else None,
        "spread_raw_max": max(spreads) if spreads else None,
        "spread_raw_mean_trunc": _trunc_div(sum(spreads), len(spreads)) if spreads else None,
        **_flow_totals(rows),
        "observed_seconds": observed,
        "missing_seconds": 300 - observed,
        "valid_seconds": valid_seconds,
        "invalid_seconds": observed - valid_seconds,
        "stale_seconds": stale_seconds,
        "book_missing_seconds": sum(bool(row["book_missing"]) for row in rows),
        "locked_seconds": locked_seconds,
        "crossed_seconds": crossed_seconds,
        "maybe_bad_book_seconds": sum(bool(row["maybe_bad_book"]) for row in rows),
        "bad_ts_recv_seconds": sum(bool(row["bad_ts_recv"]) for row in rows),
        "reset_seen_seconds": reset_seen_seconds,
        "recovery_marker_seconds": recovery_marker_seconds,
        "recovery_required_seconds": recovery_required_seconds,
        "recovery_rearmed_seconds": recovery_rearmed_seconds,
        "snapshot_seconds": sum(bool(row["snapshot_row"]) for row in rows),
        "off_tick_grid_seconds": sum(not bool(row["price_on_tick_grid"]) for row in rows),
        "source_window_complete": source_window_complete,
        "closed_bucket": True,
        "source_local_signal_input_valid": source_local_valid,
        "signal_input_valid": source_local_valid and DEFINITION_STATUS_AVAILABLE,
    }

    for level in DEPTH_LEVELS:
        bids = [int(row[f"bid_cum_size_l{level}"]) for row in valid_rows]
        asks = [int(row[f"ask_cum_size_l{level}"]) for row in valid_rows]
        numerators = [int(row[f"imbalance_numerator_l{level}"]) for row in valid_rows]
        denominators = [int(row[f"imbalance_denominator_l{level}"]) for row in valid_rows]
        ppms = [
            int(row[f"imbalance_signed_ppm_l{level}"])
            for row in valid_rows
            if row[f"imbalance_signed_ppm_l{level}"] is not None
        ]
        _put_summary(result, prefix=f"bid_cum_size_l{level}", values=bids)
        _put_summary(result, prefix=f"ask_cum_size_l{level}", values=asks)
        _put_summary(result, prefix=f"imbalance_numerator_l{level}", values=numerators)
        _put_summary(result, prefix=f"imbalance_denominator_l{level}", values=denominators)
        _put_summary(result, prefix=f"imbalance_signed_ppm_l{level}", values=ppms)

        signs = [(value > 0) - (value < 0) for value in numerators]
        nonzero_signs = [value for value in signs if value]
        sign_changes = sum(left != right for left, right in pairwise(nonzero_signs))
        if signs:
            last_sign = signs[-1]
            persistence = 0
            for sign in reversed(signs):
                if sign != last_sign:
                    break
                persistence += 1
            persistence_ppm = persistence * 1_000_000 // len(signs)
        else:
            persistence = 0
            persistence_ppm = None
        result[f"imbalance_sign_changes_l{level}"] = sign_changes
        result[f"imbalance_positive_seconds_l{level}"] = signs.count(1)
        result[f"imbalance_negative_seconds_l{level}"] = signs.count(-1)
        result[f"imbalance_zero_seconds_l{level}"] = signs.count(0)
        result[f"imbalance_observed_seconds_l{level}"] = len(signs)
        result[f"imbalance_last_sign_persistence_seconds_l{level}"] = persistence
        result[f"imbalance_last_sign_persistence_ppm_l{level}"] = persistence_ppm
    return result


def _build_five_minute_records(
    rows: list[dict[str, object]],
    *,
    source_date: date,
) -> tuple[list[dict[str, object]], int]:
    records: list[dict[str, object]] = []
    day_end = _utc_ns(source_date + timedelta(days=1))
    unproven_boundary_excluded = 0
    current_end: int | None = None
    current_rows: list[dict[str, object]] = []

    def close_current() -> None:
        nonlocal unproven_boundary_excluded
        if current_end is None:
            return
        if current_end == day_end:
            unproven_boundary_excluded += 1
            return
        records.append(
            _five_minute_record(
                current_rows,
                bucket_end=current_end,
                source_date=source_date,
            )
        )

    for row in rows:
        bucket_end = pilot._right_closed_bucket_end_ns(int(row["bucket_end"]), FIVE_MINUTE_NS)
        if current_end is None:
            current_end = bucket_end
        elif bucket_end != current_end:
            close_current()
            current_rows = []
            current_end = bucket_end
        current_rows.append(row)
    if current_end is not None:
        close_current()
    if not records:
        raise ScreeningFeatureBuildError("source has no proven complete observed 5m bucket")
    return records, unproven_boundary_excluded


@dataclass(frozen=True, slots=True)
class _AuditIdentity:
    source_date: date
    source_sha256: str
    source_schema_sha256: str
    source_manifest_sha256: str
    qc_manifest_sha256: str
    qc_config_sha256: str
    calendar_sha256: str
    code_snapshot_sha256: str
    config_sha256: str
    selection_sha256: str
    previous_volume_sha256: str
    previous_source_date: date
    instrument_id: int
    symbol: str
    contract_month: date
    previous_trade_rows: int
    previous_trade_volume: int


def _artifact_schema(
    base: pa.Schema,
    *,
    granularity: str,
    audit: _AuditIdentity,
) -> pa.Schema:
    metadata = {
        b"systematic_fx.feature_version": FEATURE_VERSION.encode(),
        b"systematic_fx.formula_sha256": FORMULA_SHA256.encode(),
        b"systematic_fx.granularity": granularity.encode(),
        b"systematic_fx.price_scale": PRICE_SCALE.encode(),
        b"systematic_fx.tick_size_raw": str(TICK_SIZE_RAW).encode(),
        b"systematic_fx.screening_only": b"true",
        b"systematic_fx.research_eligible": b"false",
        b"systematic_fx.definition_status_available": b"false",
        b"systematic_fx.source_date": audit.source_date.isoformat().encode(),
        b"systematic_fx.source_sha256": audit.source_sha256.encode(),
        b"systematic_fx.source_schema_sha256": audit.source_schema_sha256.encode(),
        b"systematic_fx.source_manifest_sha256": audit.source_manifest_sha256.encode(),
        b"systematic_fx.qc_manifest_sha256": audit.qc_manifest_sha256.encode(),
        b"systematic_fx.qc_config_sha256": audit.qc_config_sha256.encode(),
        b"systematic_fx.calendar_sha256": audit.calendar_sha256.encode(),
        b"systematic_fx.code_snapshot_sha256": audit.code_snapshot_sha256.encode(),
        b"systematic_fx.config_sha256": audit.config_sha256.encode(),
        b"systematic_fx.source_start_boundary_policy": b"EXCLUDE_PARTIAL_RIGHT_CLOSED",
        b"systematic_fx.source_end_boundary_policy": _UNPROVEN_CLOSED_BOUNDARY.encode(),
        b"systematic_fx.contract_selection_sha256": audit.selection_sha256.encode(),
        b"systematic_fx.previous_volume_sha256": audit.previous_volume_sha256.encode(),
        b"systematic_fx.previous_source_date": audit.previous_source_date.isoformat().encode(),
        b"systematic_fx.instrument_id": str(audit.instrument_id).encode(),
        b"systematic_fx.contract": audit.symbol.encode(),
        b"systematic_fx.contract_month": audit.contract_month.isoformat().encode(),
        b"systematic_fx.previous_trade_rows": str(audit.previous_trade_rows).encode(),
        b"systematic_fx.previous_trade_volume": str(audit.previous_trade_volume).encode(),
    }
    return base.with_metadata(metadata)


def _schema_sha256(schema: pa.Schema) -> str:
    metadata = {
        key.decode(): value.decode() for key, value in sorted((schema.metadata or {}).items())
    }
    document = {
        "fields": [
            {"name": field.name, "nullable": field.nullable, "type": str(field.type)}
            for field in schema
        ],
        "metadata": metadata,
    }
    return hashlib.sha256(_canonical_json(document)).hexdigest()


def _ensure_safe_parent(data_root: Path, target: Path) -> None:
    try:
        relative = target.parent.relative_to(data_root)
    except ValueError as exc:
        raise ScreeningFeaturePathError("output parent escapes data_root") from exc
    current = data_root
    for part in relative.parts:
        current /= part
        if current.exists() or current.is_symlink():
            mode = current.lstat().st_mode
            if stat.S_ISLNK(mode):
                raise ScreeningFeaturePathError(f"symlink output component is forbidden: {current}")
            if not stat.S_ISDIR(mode):
                raise ScreeningFeaturePathError(f"output component is not a directory: {current}")
        else:
            current.mkdir(mode=0o700)
    if target.is_symlink():
        raise ScreeningFeaturePathError(f"output target cannot be a symbolic link: {target}")
    if target.exists() and not stat.S_ISREG(target.lstat().st_mode):
        raise ScreeningFeaturePathError(f"output target must be a regular file: {target}")


def _timestamp_iso(value: object) -> str:
    if not isinstance(value, datetime):
        raise ScreeningFeatureBuildError("bucket timestamp did not decode as datetime")
    return value.isoformat()


@dataclass(frozen=True, slots=True)
class _StagedArtifact:
    temporary: Path
    target: Path
    sha256: str
    rows: int
    schema_sha256: str
    min_bucket_end: str
    max_bucket_end: str

    def report(self, disposition: ArtifactDisposition) -> ScreeningArtifactReport:
        return ScreeningArtifactReport(
            path=str(self.target),
            disposition=disposition,
            sha256=self.sha256,
            rows=self.rows,
            schema_sha256=self.schema_sha256,
            min_bucket_end=self.min_bucket_end,
            max_bucket_end=self.max_bucket_end,
        )


def _stage_table(
    table: pa.Table,
    target: Path,
    *,
    data_root: Path,
) -> _StagedArtifact:
    _ensure_safe_parent(data_root, target)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=target.parent,
        prefix=f".{target.name}.",
        suffix=".tmp",
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        pq.write_table(
            table,
            temporary,
            compression="zstd",
            use_dictionary=False,
            write_statistics=True,
            row_group_size=65_536,
        )
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o444)
        check = pq.ParquetFile(temporary)
        if check.metadata.num_rows != table.num_rows or check.schema_arrow != table.schema:
            raise ScreeningFeatureBuildError(f"staged Parquet validation failed: {target}")
        buckets = check.read(columns=["bucket_end"])["bucket_end"]
        minimum = pc.min(buckets).as_py()
        maximum = pc.max(buckets).as_py()
        return _StagedArtifact(
            temporary=temporary,
            target=target,
            sha256=_sha256_file(temporary),
            rows=table.num_rows,
            schema_sha256=_schema_sha256(table.schema),
            min_bucket_end=_timestamp_iso(minimum),
            max_bucket_end=_timestamp_iso(maximum),
        )
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _existing_identical(target: Path, candidate: Path) -> bool | None:
    if target.is_symlink():
        raise ScreeningFeaturePathError(f"existing output is a symbolic link: {target}")
    if not target.exists():
        return None
    if not stat.S_ISREG(target.lstat().st_mode):
        raise ScreeningFeaturePathError(f"existing output is not regular: {target}")
    if target.stat().st_size != candidate.stat().st_size:
        return False
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    target_fd = os.open(target, flags)
    candidate_fd = os.open(candidate, flags)
    try:
        while True:
            left = os.read(target_fd, 1024 * 1024)
            right = os.read(candidate_fd, 1024 * 1024)
            if left != right:
                return False
            if not left:
                return True
    finally:
        os.close(target_fd)
        os.close(candidate_fd)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish_staged(
    artifacts: tuple[_StagedArtifact, ...],
) -> tuple[ArtifactDisposition, ...]:
    preflight: list[bool] = []
    for artifact in artifacts:
        identical = _existing_identical(artifact.target, artifact.temporary)
        if identical is False:
            raise ScreeningFeatureBuildError(
                f"existing immutable feature artifact content drift: {artifact.target}"
            )
        preflight.append(bool(identical))

    dispositions: list[ArtifactDisposition] = []
    created: list[_StagedArtifact] = []
    try:
        for artifact, already_exists in zip(artifacts, preflight, strict=True):
            if already_exists:
                dispositions.append("REUSED")
                continue
            try:
                os.link(artifact.temporary, artifact.target, follow_symlinks=False)
            except FileExistsError:
                if _existing_identical(artifact.target, artifact.temporary) is not True:
                    raise ScreeningFeatureBuildError(
                        f"concurrent immutable feature artifact drift: {artifact.target}"
                    )
                dispositions.append("REUSED")
            else:
                created.append(artifact)
                dispositions.append("CREATED")
            _fsync_directory(artifact.target.parent)
        return tuple(dispositions)
    except Exception:
        for artifact in created:
            try:
                if artifact.target.stat().st_ino == artifact.temporary.stat().st_ino:
                    artifact.target.unlink()
                    _fsync_directory(artifact.target.parent)
            except FileNotFoundError:
                pass
        raise
    finally:
        for artifact in artifacts:
            artifact.temporary.unlink(missing_ok=True)
        for directory in {artifact.target.parent for artifact in artifacts}:
            _fsync_directory(directory)


def build_phase1a_screening_features(
    raw_parquet_path: Path | str,
    *,
    data_root: Path | str,
    source_date: date | str,
    selection: ContractSelectionResult,
    calendar: Phase1AScreeningCalendar,
    code_snapshot_sha256: str,
    config_path: Path | str = DEFAULT_CONFIG_PATH,
) -> ScreeningFeatureBuildReport:
    """Build and immutably publish selected-contract Phase 1A 1s/5m features."""

    parsed_date = _parse_source_date(source_date)
    code_snapshot_sha = _required_sha256(
        code_snapshot_sha256,
        label="code_snapshot_sha256",
    )
    config = load_phase1a_screening_config(config_path)
    if not isinstance(selection, ContractSelectionResult):
        raise TypeError("selection must be a ContractSelectionResult")
    selected = selection.selected
    root, raw, one_second_path, five_minute_path = _validate_input_paths(
        raw_parquet_path,
        data_root,
        symbol=selected.raw_symbol,
        source_date=parsed_date,
    )
    source_identity_before = raw.stat()
    qualification = _verify_qualification_evidence(
        data_root=root,
        raw=raw,
        source_date=parsed_date,
        calendar=calendar,
    )
    if source_identity_before.st_size != qualification.source_byte_size:
        raise ScreeningFeatureBuildError("raw source byte-size disagrees with source manifest")
    actual_source_sha = _sha256_file(raw)
    if actual_source_sha != qualification.source_sha256:
        raise ScreeningFeatureBuildError("raw source SHA-256 disagrees with source manifest")

    try:
        parquet = pq.ParquetFile(raw)
    except (OSError, pa.ArrowException) as exc:
        raise ScreeningFeatureBuildError(f"cannot open raw Parquet: {raw}") from exc
    instrument_id, symbol, source_schema_sha = _verify_selection(
        selection,
        source_date=parsed_date,
        parquet=parquet,
    )
    if source_schema_sha != qualification.schema_fingerprint:
        raise ScreeningFeatureBuildError(
            "raw schema fingerprint disagrees with Phase1A calendar/QC evidence"
        )
    try:
        pilot_rows, selected_rows, late_rows = pilot._read_one_second_records(
            parquet,
            instrument_id=instrument_id,
            symbol=symbol,
            source_date=parsed_date,
        )
    except (pilot.PilotBuildError, Mbp10ContractError) as exc:
        raise ScreeningFeatureBuildError(f"cannot build selected 1s core: {exc}") from exc
    audits_by_bucket, audit_selected_rows, audit_late_rows = _selected_second_audits(
        parquet,
        instrument_id=instrument_id,
        source_date=parsed_date,
    )
    if (audit_selected_rows, audit_late_rows) != (selected_rows, late_rows):
        raise ScreeningFeatureBuildError("physical-order event audit disagrees with 1s core")
    enriched_rows = _enrich_one_second_records(
        pilot_rows,
        source_date=parsed_date,
        instrument_id=instrument_id,
        symbol=symbol,
        audits_by_bucket=audits_by_bucket,
    )
    (
        one_second_rows,
        source_start_partial_excluded,
        unproven_one_second_excluded,
    ) = _exclude_source_boundary_records(
        enriched_rows,
        source_date=parsed_date,
    )
    five_minute_rows, unproven_five_minute_excluded = _build_five_minute_records(
        one_second_rows,
        source_date=parsed_date,
    )
    if unproven_one_second_excluded:
        unproven_five_minute_excluded = max(unproven_five_minute_excluded, 1)

    source_identity_after = raw.stat()
    if (
        source_identity_before.st_dev,
        source_identity_before.st_ino,
        source_identity_before.st_size,
        source_identity_before.st_mtime_ns,
        source_identity_before.st_ctime_ns,
    ) != (
        source_identity_after.st_dev,
        source_identity_after.st_ino,
        source_identity_after.st_size,
        source_identity_after.st_mtime_ns,
        source_identity_after.st_ctime_ns,
    ):
        raise ScreeningFeatureBuildError("raw source changed during feature construction")

    audit = _AuditIdentity(
        source_date=parsed_date,
        source_sha256=actual_source_sha,
        source_schema_sha256=source_schema_sha,
        source_manifest_sha256=qualification.source_manifest_sha256,
        qc_manifest_sha256=qualification.qc_manifest_sha256,
        qc_config_sha256=qualification.qc_config_sha256,
        calendar_sha256=qualification.calendar_sha256,
        code_snapshot_sha256=code_snapshot_sha,
        config_sha256=config.sha256,
        selection_sha256=selection.sha256,
        previous_volume_sha256=selection.previous_volume.sha256,
        previous_source_date=selection.previous_source_date,
        instrument_id=instrument_id,
        symbol=symbol,
        contract_month=selected.contract_month,
        previous_trade_rows=selected.previous_trade_rows,
        previous_trade_volume=selected.previous_trade_volume,
    )
    one_second_schema = _artifact_schema(ONE_SECOND_SCHEMA, granularity="1s", audit=audit)
    five_minute_schema = _artifact_schema(FIVE_MINUTE_SCHEMA, granularity="5m", audit=audit)
    try:
        one_second_table = pa.Table.from_pylist(one_second_rows, schema=one_second_schema)
        five_minute_table = pa.Table.from_pylist(five_minute_rows, schema=five_minute_schema)
    except (pa.ArrowException, OverflowError, ValueError, TypeError) as exc:
        raise ScreeningFeatureBuildError(
            f"feature rows violate the frozen Arrow schema: {exc}"
        ) from exc

    staged: list[_StagedArtifact] = []
    try:
        staged.append(_stage_table(one_second_table, one_second_path, data_root=root))
        staged.append(_stage_table(five_minute_table, five_minute_path, data_root=root))
        dispositions = _publish_staged(tuple(staged))
    except Exception:
        for artifact in staged:
            artifact.temporary.unlink(missing_ok=True)
        raise

    return ScreeningFeatureBuildReport(
        feature_version=FEATURE_VERSION,
        screening_only=SCREENING_ONLY,
        research_eligible=RESEARCH_ELIGIBLE,
        definition_status_available=DEFINITION_STATUS_AVAILABLE,
        source_path=str(raw),
        source_date=parsed_date.isoformat(),
        source_sha256=actual_source_sha,
        source_schema_sha256=source_schema_sha,
        source_manifest_sha256=qualification.source_manifest_sha256,
        qc_manifest_sha256=qualification.qc_manifest_sha256,
        qc_config_sha256=qualification.qc_config_sha256,
        calendar_sha256=qualification.calendar_sha256,
        code_snapshot_sha256=code_snapshot_sha,
        source_rows=parquet.metadata.num_rows,
        selected_rows=selected_rows,
        late_rows_ignored=late_rows,
        source_start_partial_one_second_excluded=source_start_partial_excluded,
        unproven_closed_boundary_one_second_excluded=unproven_one_second_excluded,
        unproven_closed_boundary_five_minute_excluded=unproven_five_minute_excluded,
        config_path=str(config.path),
        config_sha256=config.sha256,
        formula_sha256=config.formula_sha256,
        contract_selection_sha256=selection.sha256,
        previous_volume_sha256=selection.previous_volume.sha256,
        previous_source_date=selection.previous_source_date.isoformat(),
        instrument_id=instrument_id,
        contract=symbol,
        contract_month=selected.contract_month.isoformat(),
        previous_trade_rows=selected.previous_trade_rows,
        previous_trade_volume=selected.previous_trade_volume,
        one_second=staged[0].report(dispositions[0]),
        five_minute=staged[1].report(dispositions[1]),
    )
