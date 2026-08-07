"""Deterministic, screening-only summaries for one five-source-date AI slice."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Final, Literal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from systematic_fx.features.screening import FEATURE_VERSION, FIVE_MINUTE_SCHEMA, FORMULA_SHA256

DISCOVERY_SLICE_SCHEMA: Final = "systematic_fx.phase1a_discovery_slice.v1"
DISCOVERY_SLICE_VERSION: Final = "phase1a_discovery_slice_v1"
CONFIG_RELATIVE_PATH: Final = "configs/research/phase1a_discovery_slice_v1.toml"
DEFAULT_CONFIG_PATH: Final = (
    Path(__file__).resolve().parents[3] / "configs/research/phase1a_discovery_slice_v1.toml"
)
FIVE_MINUTE_NS: Final = 300_000_000_000
TICK_SIZE_RAW: Final = 50_000
RATIO_SCALE_PPM: Final = 1_000_000
FORWARD_HORIZONS: Final = (1, 3, 6, 12)
QUANTILES_PPM: Final = (0, 50_000, 250_000, 500_000, 750_000, 950_000, 1_000_000)
_SHA256 = re.compile(r"[0-9a-f]{64}")
_SAFE_CONTRACT = re.compile(r"[A-Z0-9][A-Z0-9._-]{0,31}")


class DiscoverySliceError(ValueError):
    """One slice input, formula, or immutable output is invalid."""


@dataclass(frozen=True, slots=True)
class CandidateQuery:
    query_id: str
    parent_hypothesis_ids: tuple[str, ...]
    direction_rule: str
    conditions: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "conditions": list(self.conditions),
            "direction_rule": self.direction_rule,
            "id": self.query_id,
            "parent_hypothesis_ids": list(self.parent_hypothesis_ids),
        }


@dataclass(frozen=True, slots=True)
class DiscoverySliceConfig:
    path: Path
    sha256: str
    definition_sha256: str
    candidate_queries: tuple[CandidateQuery, ...]


@dataclass(frozen=True, slots=True)
class DiscoverySliceReport:
    path: Path
    sha256: str
    byte_size: int
    disposition: Literal["CREATED", "REUSED"]
    requested_source_dates: tuple[str, ...]
    feature_source_dates: tuple[str, ...]
    no_entry_source_dates: tuple[str, ...]
    total_rows: int
    eligible_rows: int
    candidate_query_count: int
    nonzero_support_query_count: int

    def as_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["path"] = str(self.path)
        return value


_QUERY_DEFINITIONS: Final = (
    CandidateQuery(
        "p2_01_l1_persistent_continuation",
        ("p2_01_l1_imbalance_persistence",),
        "SIGN_L1_LAST",
        (
            "abs(l1_last_ppm)>=400000",
            "l1_persistence_ppm>=750000",
            "l1_sign_changes<=20",
            "last_spread_ticks<=2",
        ),
    ),
    CandidateQuery(
        "p2_02_multilevel_agreement_continuation",
        ("p2_02_multilevel_imbalance_agreement",),
        "COMMON_SIGN_L1_L3_L5_L10",
        (
            "all_abs_last_ppm>=250000",
            "all_same_nonzero_sign",
            "min_persistence_ppm>=600000",
            "last_spread_ticks<=2",
        ),
    ),
    CandidateQuery(
        "p2_05_stable_l5_low_flip_continuation",
        ("p2_05_stable_imbalance_low_flip",),
        "SIGN_L5_MEAN",
        (
            "abs(l5_mean_ppm)>=250000",
            "l5_sign_changes<=8",
            "l5_persistence_ppm>=800000",
            "last_spread_ticks<=2",
        ),
    ),
    CandidateQuery(
        "p3_01_flow_price_confirmation_continuation",
        ("p3_03_flow_price_confirmation",),
        "SIGN_SIGNED_FLOW",
        (
            "abs(signed_flow_ppm)>=300000",
            "same_sign_bar_move",
            "abs(bar_move_x2_ticks)>=4",
            "last_spread_ticks<=2",
        ),
    ),
    CandidateQuery(
        "p3_02_flow_absorption_reversal",
        (
            "p3_01_buy_flow_absorption_reversal",
            "p3_02_sell_flow_absorption_reversal",
        ),
        "OPPOSITE_SIGN_SIGNED_FLOW",
        (
            "abs(signed_flow_ppm)>=400000",
            "bar_move_opposes_flow_or_abs<=2",
            "l10_opposes_flow_or_abs<100000",
            "last_spread_ticks<=2",
        ),
    ),
    CandidateQuery(
        "p1_01_depth_supported_move_continuation",
        ("p1_01_supported_move_continuation",),
        "SIGN_BAR_MOVE",
        (
            "abs(bar_move_x2_ticks)>=16",
            "same_sign_l5_last",
            "abs(l5_last_ppm)>=250000",
            "trade_volume>0",
            "last_spread_ticks<=2",
        ),
    ),
    CandidateQuery(
        "p1_05_unconfirmed_move_reversal",
        ("p1_05_unconfirmed_move_reversal",),
        "OPPOSITE_SIGN_BAR_MOVE",
        (
            "abs(bar_move_x2_ticks)>=16",
            "abs(signed_flow_ppm)<100000",
            "abs(l5_mean_ppm)<100000",
            "last_spread_ticks<=2",
        ),
    ),
    CandidateQuery(
        "p1_03_spread_shock_recovery_reversal",
        ("p1_03_spread_shock_recovery_reversal",),
        "OPPOSITE_SIGN_BAR_MOVE",
        (
            "spread_max_ticks>=4",
            "last_spread_ticks<=2",
            "abs(bar_move_x2_ticks)>=8",
        ),
    ),
    CandidateQuery(
        "p4_01_opposite_depth_depletion_continuation",
        (
            "p4_01_ask_depletion_upward_continuation",
            "p4_02_bid_depletion_downward_continuation",
        ),
        "SIGN_BAR_MOVE",
        (
            "abs(bar_move_x2_ticks)>=8",
            "opposite_l5_depth_depleted",
            "supporting_l5_depth_not_depleted",
            "last_spread_ticks<=2",
        ),
    ),
    CandidateQuery(
        "p4_02_depth_resistance_reversal",
        ("p4_03_depletion_replenishment_reversal",),
        "OPPOSITE_SIGN_BAR_MOVE",
        (
            "abs(bar_move_x2_ticks)>=8",
            "opposite_l5_depth_replenished",
            "supporting_l5_depth_depleted",
            "last_spread_ticks<=2",
        ),
    ),
    CandidateQuery(
        "p5_01_range_expansion_flow_continuation",
        ("p5_03_volatility_expansion_continuation",),
        "SIGN_BAR_MOVE",
        (
            "bar_range_x2_ticks>=32",
            "abs(bar_move_x2_ticks)>=8",
            "same_sign_signed_flow",
            "last_spread_ticks<=2",
        ),
    ),
)

_DISTRIBUTION_FIELDS: Final = (
    "last_spread_ticks",
    "trade_count",
    "trade_volume",
    "signed_trade_volume",
    "event_count",
    "imbalance_signed_ppm_l1_last",
    "imbalance_signed_ppm_l3_last",
    "imbalance_signed_ppm_l5_last",
    "imbalance_signed_ppm_l10_last",
    "imbalance_last_sign_persistence_ppm_l1",
    "imbalance_last_sign_persistence_ppm_l5",
    "imbalance_sign_changes_l1",
    "imbalance_sign_changes_l5",
)

_REQUIRED_COLUMNS: Final = tuple(
    dict.fromkeys(
        (
            "feature_version",
            "screening_only",
            "definition_status_available",
            "source_date",
            "contract",
            "instrument_id",
            "bucket_end",
            "source_local_signal_input_valid",
            "signal_input_valid",
            "observed_seconds",
            "valid_seconds",
            "missing_seconds",
            "invalid_seconds",
            "stale_seconds",
            "reset_seen_seconds",
            "maybe_bad_book_seconds",
            "last_spread_ticks",
            "spread_raw_max",
            "mid_px_x2_raw_open",
            "mid_px_x2_raw_high",
            "mid_px_x2_raw_low",
            "mid_px_x2_raw_close",
            "trade_count",
            "trade_volume",
            "signed_trade_volume",
            "event_count",
            "bid_cum_size_l5_first",
            "bid_cum_size_l5_last",
            "ask_cum_size_l5_first",
            "ask_cum_size_l5_last",
            *(
                field
                for level in (1, 3, 5, 10)
                for field in (
                    f"imbalance_signed_ppm_l{level}_last",
                    f"imbalance_signed_ppm_l{level}_mean_trunc",
                    f"imbalance_sign_changes_l{level}",
                    f"imbalance_last_sign_persistence_ppm_l{level}",
                )
            ),
        )
    )
)
DISCOVERY_VARIABLE_FIELDS: Final = tuple(
    field for field in _REQUIRED_COLUMNS if field not in {"source_date", "bucket_end"}
) + (
    "bar_move_x2_ticks",
    "bar_range_x2_ticks",
    "signed_flow_ppm",
    "spread_max_ticks",
)
DISCOVERY_FORWARD_RESULT_FIELDS: Final = (
    "aligned_close_x2_ticks",
    "maximum_adverse_excursion_x2_ticks",
    "maximum_favorable_excursion_x2_ticks",
)


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_stable_regular_file(path: Path, *, label: str) -> bytes:
    """Read one exact inode without following the leaf symlink or accepting mutation."""

    try:
        before = path.lstat()
    except OSError as exc:
        raise DiscoverySliceError(f"cannot inspect {label}: {path}") from exc
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise DiscoverySliceError(f"{label} must be a regular non-symlink file: {path}")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise DiscoverySliceError(f"cannot safely open {label}: {path}") from exc
    try:
        opened = os.fstat(descriptor)
        if _stat_identity(opened) != _stat_identity(before):
            raise DiscoverySliceError(f"{label} changed before it was opened: {path}")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after_descriptor = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    try:
        after_path = path.lstat()
    except OSError as exc:
        raise DiscoverySliceError(f"{label} disappeared while it was read: {path}") from exc
    identities = {
        _stat_identity(before),
        _stat_identity(opened),
        _stat_identity(after_descriptor),
        _stat_identity(after_path),
    }
    if len(identities) != 1:
        raise DiscoverySliceError(f"{label} changed while it was read: {path}")
    return b"".join(chunks)


def _assert_no_symlink_components(path: Path, *, anchor: Path, label: str) -> None:
    """Reject symlinks in every existing component at or below an explicit anchor."""

    try:
        relative = path.relative_to(anchor)
    except ValueError as exc:
        raise DiscoverySliceError(f"{label} escapes its required root") from exc
    current = anchor
    components = (Path(), *relative.parents[::-1], relative)
    seen: set[Path] = set()
    for component in components:
        candidate = current if component == Path() else anchor / component
        if candidate in seen:
            continue
        seen.add(candidate)
        try:
            mode = candidate.lstat().st_mode
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise DiscoverySliceError(f"cannot inspect {label} component: {candidate}") from exc
        if stat.S_ISLNK(mode):
            raise DiscoverySliceError(f"{label} cannot contain a symbolic link: {candidate}")


def _absolute_lexical(path: Path) -> Path:
    return Path(os.path.abspath(os.fspath(path)))


def _expected_config() -> dict[str, object]:
    return {
        "query": {
            "id": DISCOVERY_SLICE_VERSION,
            "schema_version": 1,
            "source_dates_per_slice": 5,
            "signal_cadence_seconds": 300,
            "forward_horizons_bars": list(FORWARD_HORIZONS),
            "distribution_quantiles_ppm": list(QUANTILES_PPM),
            "cross_source_date_forward_fill": False,
            "forward_path_requires_contiguous_valid_bars": True,
            "forward_price_reference": "CURRENT_BAR_CLOSE",
            "excursion_includes_entry_zero": True,
            "performance_based_early_stop": False,
            "emit_zero_support_queries": True,
            "retain_all_occurrences": True,
            "retain_all_query_variables": True,
            "retain_per_occurrence_forward_results": True,
            "screening_only": True,
            "maximum_authority": "OPEN_OBSERVATION",
        },
        "eligibility": {
            "required_field": "source_local_signal_input_valid",
            "required_value": True,
            "definition_status_required": False,
            "signal_input_valid_field_must_remain_false": True,
            "missing_source_date_behavior": "RECORDED_NO_ENTRY",
            "unresolved_forward_behavior": "RETAIN_UNRESOLVED",
        },
        "units": {
            "raw_price_scale": "1e-9",
            "tick_size_raw": TICK_SIZE_RAW,
            "mid_move_unit": "x2_ticks",
            "signed_ratio_scale_ppm": RATIO_SCALE_PPM,
            "integer_division": "TRUNCATE_TOWARD_ZERO",
        },
        "candidate_queries": [query.as_dict() for query in _QUERY_DEFINITIONS],
    }


def load_discovery_slice_config(
    path: Path | str = DEFAULT_CONFIG_PATH,
) -> DiscoverySliceConfig:
    requested = Path(path).expanduser()
    if requested.is_symlink():
        raise DiscoverySliceError("slice config cannot be a symbolic link")
    try:
        resolved = requested.resolve(strict=True)
    except FileNotFoundError as exc:
        raise DiscoverySliceError(f"slice config does not exist: {requested}") from exc
    raw = _read_stable_regular_file(resolved, label="slice config")
    try:
        document = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise DiscoverySliceError("slice config is not valid UTF-8 TOML") from exc
    if document != _expected_config():
        raise DiscoverySliceError("Phase1A slice query semantics drifted; create a new version")
    definitions = [query.as_dict() for query in _QUERY_DEFINITIONS]
    return DiscoverySliceConfig(
        path=resolved,
        sha256=hashlib.sha256(raw).hexdigest(),
        definition_sha256=hashlib.sha256(_canonical_json(definitions)).hexdigest(),
        candidate_queries=_QUERY_DEFINITIONS,
    )


def _sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise DiscoverySliceError(f"{label} must be a lowercase SHA-256")
    return value


def _date(value: object, *, label: str) -> date:
    if isinstance(value, datetime):
        raise DiscoverySliceError(f"{label} must not be a datetime")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise DiscoverySliceError(f"{label} must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise DiscoverySliceError(f"{label} must be an ISO date") from exc
    if value != parsed.isoformat():
        raise DiscoverySliceError(f"{label} must use canonical ISO format")
    return parsed


def _trunc_div(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise DiscoverySliceError("integer division denominator must be positive")
    return numerator // denominator if numerator >= 0 else -((-numerator) // denominator)


def _sign(value: int) -> int:
    return (value > 0) - (value < 0)


def _required_int(row: Mapping[str, object], field: str) -> int:
    value = row.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise DiscoverySliceError(f"eligible row requires integer {field}")
    return value


def _row_state(row: Mapping[str, object]) -> dict[str, int]:
    close = _required_int(row, "mid_px_x2_raw_close")
    open_ = _required_int(row, "mid_px_x2_raw_open")
    high = _required_int(row, "mid_px_x2_raw_high")
    low = _required_int(row, "mid_px_x2_raw_low")
    if any(value % TICK_SIZE_RAW for value in (close, open_, high, low)):
        raise DiscoverySliceError("eligible mid x2 prices must align to raw tick units")
    if high < max(open_, close) or low > min(open_, close) or high < low:
        raise DiscoverySliceError("eligible mid x2 OHLC ordering is invalid")
    trade_volume = _required_int(row, "trade_volume")
    signed_volume = _required_int(row, "signed_trade_volume")
    return {
        "bar_move_x2_ticks": (close - open_) // TICK_SIZE_RAW,
        "bar_range_x2_ticks": (high - low) // TICK_SIZE_RAW,
        "signed_flow_ppm": (
            _trunc_div(signed_volume * RATIO_SCALE_PPM, trade_volume) if trade_volume > 0 else 0
        ),
    }


def _spread_max_ticks(row: Mapping[str, object]) -> int:
    raw = _required_int(row, "spread_raw_max")
    if raw % TICK_SIZE_RAW:
        raise DiscoverySliceError("eligible spread_raw_max is off the tick grid")
    return raw // TICK_SIZE_RAW


def _research_variables(row: Mapping[str, object], state: Mapping[str, int]) -> dict[str, object]:
    """Retain every source and derived value available to the frozen query rules."""

    variables = {
        field: row.get(field)
        for field in _REQUIRED_COLUMNS
        if field not in {"source_date", "bucket_end"}
    }
    variables.update(state)
    variables["spread_max_ticks"] = _spread_max_ticks(row)
    if set(variables) != set(DISCOVERY_VARIABLE_FIELDS):
        raise DiscoverySliceError("Discovery variable schema drifted; create a new version")
    return variables


def _rule_p2_l1(row: Mapping[str, object], state: Mapping[str, int]) -> int | None:
    del state
    value = _required_int(row, "imbalance_signed_ppm_l1_last")
    direction = _sign(value)
    if (
        direction
        and abs(value) >= 400_000
        and _required_int(row, "imbalance_last_sign_persistence_ppm_l1") >= 750_000
        and _required_int(row, "imbalance_sign_changes_l1") <= 20
        and _required_int(row, "last_spread_ticks") <= 2
    ):
        return direction
    return None


def _rule_p2_multilevel(row: Mapping[str, object], state: Mapping[str, int]) -> int | None:
    del state
    values = [_required_int(row, f"imbalance_signed_ppm_l{level}_last") for level in (1, 3, 5, 10)]
    signs = {_sign(value) for value in values}
    if (
        len(signs) == 1
        and 0 not in signs
        and min(abs(value) for value in values) >= 250_000
        and min(
            _required_int(row, f"imbalance_last_sign_persistence_ppm_l{level}")
            for level in (1, 3, 5, 10)
        )
        >= 600_000
        and _required_int(row, "last_spread_ticks") <= 2
    ):
        return signs.pop()
    return None


def _rule_p2_l5(row: Mapping[str, object], state: Mapping[str, int]) -> int | None:
    del state
    value = _required_int(row, "imbalance_signed_ppm_l5_mean_trunc")
    direction = _sign(value)
    if (
        direction
        and abs(value) >= 250_000
        and _required_int(row, "imbalance_sign_changes_l5") <= 8
        and _required_int(row, "imbalance_last_sign_persistence_ppm_l5") >= 800_000
        and _required_int(row, "last_spread_ticks") <= 2
    ):
        return direction
    return None


def _rule_flow_confirmation(row: Mapping[str, object], state: Mapping[str, int]) -> int | None:
    flow = state["signed_flow_ppm"]
    move = state["bar_move_x2_ticks"]
    direction = _sign(flow)
    if (
        direction
        and abs(flow) >= 300_000
        and _sign(move) == direction
        and abs(move) >= 4
        and _required_int(row, "last_spread_ticks") <= 2
    ):
        return direction
    return None


def _rule_flow_absorption(row: Mapping[str, object], state: Mapping[str, int]) -> int | None:
    flow = state["signed_flow_ppm"]
    move = state["bar_move_x2_ticks"]
    depth = _required_int(row, "imbalance_signed_ppm_l10_last")
    direction = _sign(flow)
    if (
        direction
        and abs(flow) >= 400_000
        and (_sign(move) == -direction or abs(move) <= 2)
        and (_sign(depth) == -direction or abs(depth) < 100_000)
        and _required_int(row, "last_spread_ticks") <= 2
    ):
        return -direction
    return None


def _rule_depth_supported(row: Mapping[str, object], state: Mapping[str, int]) -> int | None:
    move = state["bar_move_x2_ticks"]
    direction = _sign(move)
    imbalance = _required_int(row, "imbalance_signed_ppm_l5_last")
    if (
        direction
        and abs(move) >= 16
        and _sign(imbalance) == direction
        and abs(imbalance) >= 250_000
        and _required_int(row, "trade_volume") > 0
        and _required_int(row, "last_spread_ticks") <= 2
    ):
        return direction
    return None


def _rule_unconfirmed_reversal(row: Mapping[str, object], state: Mapping[str, int]) -> int | None:
    move = state["bar_move_x2_ticks"]
    direction = _sign(move)
    if (
        direction
        and abs(move) >= 16
        and abs(state["signed_flow_ppm"]) < 100_000
        and abs(_required_int(row, "imbalance_signed_ppm_l5_mean_trunc")) < 100_000
        and _required_int(row, "last_spread_ticks") <= 2
    ):
        return -direction
    return None


def _rule_spread_recovery(row: Mapping[str, object], state: Mapping[str, int]) -> int | None:
    move = state["bar_move_x2_ticks"]
    direction = _sign(move)
    if (
        direction
        and _spread_max_ticks(row) >= 4
        and _required_int(row, "last_spread_ticks") <= 2
        and abs(move) >= 8
    ):
        return -direction
    return None


def _depth_changes(row: Mapping[str, object]) -> tuple[int, int]:
    bid = _required_int(row, "bid_cum_size_l5_last") - _required_int(row, "bid_cum_size_l5_first")
    ask = _required_int(row, "ask_cum_size_l5_last") - _required_int(row, "ask_cum_size_l5_first")
    return bid, ask


def _rule_depth_depletion(row: Mapping[str, object], state: Mapping[str, int]) -> int | None:
    move = state["bar_move_x2_ticks"]
    direction = _sign(move)
    bid_change, ask_change = _depth_changes(row)
    matches = (direction == 1 and ask_change < 0 <= bid_change) or (
        direction == -1 and bid_change < 0 <= ask_change
    )
    if direction and abs(move) >= 8 and matches and _required_int(row, "last_spread_ticks") <= 2:
        return direction
    return None


def _rule_depth_resistance(row: Mapping[str, object], state: Mapping[str, int]) -> int | None:
    move = state["bar_move_x2_ticks"]
    direction = _sign(move)
    bid_change, ask_change = _depth_changes(row)
    matches = (direction == 1 and ask_change > 0 > bid_change) or (
        direction == -1 and bid_change > 0 > ask_change
    )
    if direction and abs(move) >= 8 and matches and _required_int(row, "last_spread_ticks") <= 2:
        return -direction
    return None


def _rule_range_flow(row: Mapping[str, object], state: Mapping[str, int]) -> int | None:
    move = state["bar_move_x2_ticks"]
    direction = _sign(move)
    if (
        direction
        and state["bar_range_x2_ticks"] >= 32
        and abs(move) >= 8
        and _sign(state["signed_flow_ppm"]) == direction
        and _required_int(row, "last_spread_ticks") <= 2
    ):
        return direction
    return None


_RULES: Final[Mapping[str, Callable[[Mapping[str, object], Mapping[str, int]], int | None]]] = {
    "p2_01_l1_persistent_continuation": _rule_p2_l1,
    "p2_02_multilevel_agreement_continuation": _rule_p2_multilevel,
    "p2_05_stable_l5_low_flip_continuation": _rule_p2_l5,
    "p3_01_flow_price_confirmation_continuation": _rule_flow_confirmation,
    "p3_02_flow_absorption_reversal": _rule_flow_absorption,
    "p1_01_depth_supported_move_continuation": _rule_depth_supported,
    "p1_05_unconfirmed_move_reversal": _rule_unconfirmed_reversal,
    "p1_03_spread_shock_recovery_reversal": _rule_spread_recovery,
    "p4_01_opposite_depth_depletion_continuation": _rule_depth_depletion,
    "p4_02_depth_resistance_reversal": _rule_depth_resistance,
    "p5_01_range_expansion_flow_continuation": _rule_range_flow,
}


def _datetime_ns(value: object) -> int:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise DiscoverySliceError("bucket_end must decode as timezone-aware datetime")
    utc = value.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = utc - epoch
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


def _read_feature_file(
    path: Path,
    *,
    source_date: date,
    expected_sha256: str,
    data_root: Path,
    code_snapshot_sha256: str,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    requested = _absolute_lexical(path.expanduser())
    required_root = data_root / "derived/research_5m"
    if requested.suffix != ".parquet":
        raise DiscoverySliceError("5m feature input must use the .parquet suffix")
    _assert_no_symlink_components(
        requested,
        anchor=data_root,
        label="5m feature input",
    )
    try:
        requested.relative_to(required_root)
    except ValueError as exc:
        raise DiscoverySliceError(
            "5m feature input must remain under data/derived/research_5m"
        ) from exc
    raw = _read_stable_regular_file(requested, label="5m feature input")
    actual_sha = hashlib.sha256(raw).hexdigest()
    byte_size = len(raw)
    if actual_sha != expected_sha256:
        raise DiscoverySliceError(f"feature SHA-256 mismatch for {source_date.isoformat()}")
    try:
        parquet = pq.ParquetFile(pa.BufferReader(raw))
    except (OSError, pa.ArrowException) as exc:
        raise DiscoverySliceError(f"cannot decode 5m feature Parquet: {requested}") from exc
    schema = parquet.schema_arrow
    missing = [name for name in _REQUIRED_COLUMNS if schema.get_field_index(name) < 0]
    if missing:
        raise DiscoverySliceError(f"feature input lacks required columns: {missing}")
    if not schema.equals(FIVE_MINUTE_SCHEMA, check_metadata=False):
        raise DiscoverySliceError("feature input full 5m schema drift")
    try:
        metadata = {
            key.decode("utf-8"): value.decode("utf-8")
            for key, value in (schema.metadata or {}).items()
        }
    except UnicodeDecodeError as exc:
        raise DiscoverySliceError("feature metadata must be valid UTF-8") from exc
    expected_metadata = {
        "systematic_fx.feature_version": FEATURE_VERSION,
        "systematic_fx.formula_sha256": FORMULA_SHA256,
        "systematic_fx.granularity": "5m",
        "systematic_fx.price_scale": "1e-9",
        "systematic_fx.tick_size_raw": str(TICK_SIZE_RAW),
        "systematic_fx.screening_only": "true",
        "systematic_fx.research_eligible": "false",
        "systematic_fx.definition_status_available": "false",
        "systematic_fx.source_date": source_date.isoformat(),
        "systematic_fx.code_snapshot_sha256": code_snapshot_sha256,
        "systematic_fx.source_start_boundary_policy": "EXCLUDE_PARTIAL_RIGHT_CLOSED",
        "systematic_fx.source_end_boundary_policy": "UNPROVEN_CLOSED_BOUNDARY",
    }
    mismatches = [key for key, value in expected_metadata.items() if metadata.get(key) != value]
    if mismatches:
        raise DiscoverySliceError(
            f"feature metadata drift for {source_date.isoformat()}: {sorted(mismatches)}"
        )
    provenance_hash_keys = (
        "systematic_fx.source_sha256",
        "systematic_fx.source_schema_sha256",
        "systematic_fx.source_manifest_sha256",
        "systematic_fx.qc_manifest_sha256",
        "systematic_fx.qc_config_sha256",
        "systematic_fx.calendar_sha256",
        "systematic_fx.config_sha256",
        "systematic_fx.contract_selection_sha256",
        "systematic_fx.previous_volume_sha256",
    )
    provenance_hashes = {
        key: _sha256(metadata.get(key), label=f"feature metadata {key}")
        for key in provenance_hash_keys
    }
    contract = metadata.get("systematic_fx.contract")
    if not isinstance(contract, str) or _SAFE_CONTRACT.fullmatch(contract) is None:
        raise DiscoverySliceError("feature metadata contract is missing or unsafe")
    canonical_path = (
        required_root
        / f"version={FEATURE_VERSION}"
        / f"contract={contract}"
        / f"source_date={source_date.isoformat()}"
        / "part-000.parquet"
    )
    if requested != canonical_path:
        raise DiscoverySliceError(
            "5m feature input path does not match its canonical version/contract/date partition"
        )
    for integer_key in (
        "systematic_fx.instrument_id",
        "systematic_fx.previous_trade_rows",
        "systematic_fx.previous_trade_volume",
    ):
        value = metadata.get(integer_key, "")
        if not value.isascii() or not value.isdecimal():
            raise DiscoverySliceError(
                f"feature metadata {integer_key} must be a nonnegative integer"
            )
    if int(metadata["systematic_fx.instrument_id"]) <= 0:
        raise DiscoverySliceError("feature metadata instrument_id must be positive")
    previous_source_date = _date(
        metadata.get("systematic_fx.previous_source_date"),
        label="feature metadata previous_source_date",
    )
    if previous_source_date >= source_date:
        raise DiscoverySliceError("feature previous_source_date must precede source_date")
    contract_month = _date(
        metadata.get("systematic_fx.contract_month"),
        label="feature metadata contract_month",
    )
    try:
        table = parquet.read(columns=list(_REQUIRED_COLUMNS), use_threads=False)
    except (OSError, pa.ArrowException) as exc:
        raise DiscoverySliceError(f"cannot read 5m feature rows: {requested}") from exc
    if table.num_rows <= 0:
        raise DiscoverySliceError("5m feature input cannot be empty")
    row_identity = {
        "feature_version": FEATURE_VERSION,
        "screening_only": True,
        "definition_status_available": False,
        "contract": contract,
        "instrument_id": int(metadata["systematic_fx.instrument_id"]),
    }
    for field, expected in row_identity.items():
        if table[field].null_count or (
            pc.all(pc.equal(table[field], pa.scalar(expected))).as_py() is not True
        ):
            raise DiscoverySliceError(f"feature row identity drift: {field}")
    if table["source_date"].null_count or (
        pc.all(pc.equal(table["source_date"], pa.scalar(source_date))).as_py() is not True
    ):
        raise DiscoverySliceError("feature rows contain a different source_date")
    if table["source_local_signal_input_valid"].null_count:
        raise DiscoverySliceError("source_local_signal_input_valid cannot contain null")
    if table["signal_input_valid"].null_count or (
        pc.any(table["signal_input_valid"]).as_py() is not False
    ):
        raise DiscoverySliceError("Phase1A rows cannot claim signal_input_valid")
    rows = table.to_pylist()
    prior_ns: int | None = None
    source_start = _datetime_ns(
        datetime(source_date.year, source_date.month, source_date.day, tzinfo=UTC)
    )
    source_end = source_start + 86_400_000_000_000
    for row in rows:
        bucket_ns = _datetime_ns(row["bucket_end"])
        if prior_ns is not None and bucket_ns <= prior_ns:
            raise DiscoverySliceError("feature bucket_end must be strictly increasing")
        if bucket_ns % FIVE_MINUTE_NS or not source_start < bucket_ns < source_end:
            raise DiscoverySliceError(
                "feature bucket_end must be a 5m UTC boundary strictly inside source_date"
            )
        prior_ns = bucket_ns
        row["bucket_end_ns"] = bucket_ns
    identity = {
        "byte_size": byte_size,
        "calendar_sha256": provenance_hashes["systematic_fx.calendar_sha256"],
        "config_sha256": provenance_hashes["systematic_fx.config_sha256"],
        "contract": contract,
        "contract_month": contract_month.isoformat(),
        "formula_sha256": FORMULA_SHA256,
        "instrument_id": int(metadata["systematic_fx.instrument_id"]),
        "metadata": {
            key.removeprefix("systematic_fx."): metadata[key]
            for key in sorted(metadata)
            if key.startswith("systematic_fx.")
        },
        "path": requested.relative_to(data_root).as_posix(),
        "previous_source_date": previous_source_date.isoformat(),
        "previous_trade_rows": int(metadata["systematic_fx.previous_trade_rows"]),
        "previous_trade_volume": int(metadata["systematic_fx.previous_trade_volume"]),
        "previous_volume_sha256": provenance_hashes["systematic_fx.previous_volume_sha256"],
        "qc_config_sha256": provenance_hashes["systematic_fx.qc_config_sha256"],
        "qc_manifest_sha256": provenance_hashes["systematic_fx.qc_manifest_sha256"],
        "rows": len(rows),
        "selection_sha256": provenance_hashes["systematic_fx.contract_selection_sha256"],
        "sha256": actual_sha,
        "source_manifest_sha256": provenance_hashes["systematic_fx.source_manifest_sha256"],
        "source_schema_sha256": provenance_hashes["systematic_fx.source_schema_sha256"],
        "source_sha256": provenance_hashes["systematic_fx.source_sha256"],
    }
    return rows, identity


def _integer_distribution(values: Sequence[int]) -> dict[str, object]:
    if not values:
        return {
            "count": 0,
            "maximum": None,
            "mean_trunc": None,
            "minimum": None,
            "quantiles_ppm": {},
        }
    ordered = sorted(values)
    quantiles = {
        str(quantile): ordered[(quantile * (len(ordered) - 1)) // RATIO_SCALE_PPM]
        for quantile in QUANTILES_PPM
    }
    return {
        "count": len(ordered),
        "maximum": ordered[-1],
        "mean_trunc": _trunc_div(sum(ordered), len(ordered)),
        "minimum": ordered[0],
        "quantiles_ppm": quantiles,
    }


def _forward_result(
    rows_by_timestamp: Mapping[int, Mapping[str, object]],
    row: Mapping[str, object],
    *,
    direction: int,
    horizon: int,
) -> dict[str, int] | None:
    if direction not in (-1, 1):
        raise DiscoverySliceError("forward direction must be -1 or 1")
    start = _required_int(row, "bucket_end_ns")
    future_rows: list[Mapping[str, object]] = []
    for step in range(1, horizon + 1):
        future = rows_by_timestamp.get(start + step * FIVE_MINUTE_NS)
        if future is None or future.get("source_local_signal_input_valid") is not True:
            return None
        future_rows.append(future)
    current_close = _required_int(row, "mid_px_x2_raw_close")
    target_close = _required_int(future_rows[-1], "mid_px_x2_raw_close")
    highs = [_required_int(item, "mid_px_x2_raw_high") for item in future_rows]
    lows = [_required_int(item, "mid_px_x2_raw_low") for item in future_rows]
    if direction == 1:
        favorable = max(0, max(highs) - current_close)
        adverse = min(0, min(lows) - current_close)
    else:
        favorable = max(0, current_close - min(lows))
        adverse = min(0, current_close - max(highs))
    close_change = direction * (target_close - current_close)
    values = (close_change, favorable, adverse)
    if any(value % TICK_SIZE_RAW for value in values):
        raise DiscoverySliceError("forward mid-price difference is off raw tick units")
    return {
        "aligned_close_x2_ticks": _trunc_div(close_change, TICK_SIZE_RAW),
        "maximum_adverse_excursion_x2_ticks": _trunc_div(adverse, TICK_SIZE_RAW),
        "maximum_favorable_excursion_x2_ticks": _trunc_div(favorable, TICK_SIZE_RAW),
    }


def _summarize_forward(values: Sequence[dict[str, int]], unresolved: int) -> dict[str, object]:
    aligned = [item["aligned_close_x2_ticks"] for item in values]
    favorable = [item["maximum_favorable_excursion_x2_ticks"] for item in values]
    adverse = [item["maximum_adverse_excursion_x2_ticks"] for item in values]
    positive = sum(value > 0 for value in aligned)
    return {
        "aligned_close_x2_ticks": _integer_distribution(aligned),
        "maximum_adverse_excursion_x2_ticks": _integer_distribution(adverse),
        "maximum_favorable_excursion_x2_ticks": _integer_distribution(favorable),
        "negative_count": sum(value < 0 for value in aligned),
        "positive_count": positive,
        "positive_rate_ppm": (
            _trunc_div(positive * RATIO_SCALE_PPM, len(aligned)) if aligned else None
        ),
        "resolved_count": len(aligned),
        "unresolved_count": unresolved,
        "zero_count": sum(value == 0 for value in aligned),
    }


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish(path: Path, payload: bytes, *, data_root: Path) -> Literal["CREATED", "REUSED"]:
    derived = data_root / "derived"
    try:
        path.relative_to(derived)
    except ValueError as exc:
        raise DiscoverySliceError("slice output must remain under data/derived") from exc
    _assert_no_symlink_components(path, anchor=derived, label="slice output path")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise DiscoverySliceError(f"cannot create slice output directory: {path.parent}") from exc
    _assert_no_symlink_components(path, anchor=derived, label="slice output path")
    if path.exists():
        if _read_stable_regular_file(path, label="slice output artifact") != payload:
            raise DiscoverySliceError(f"existing immutable slice artifact drift: {path}")
        return "REUSED"
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o444)
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            _assert_no_symlink_components(path, anchor=derived, label="slice output path")
            if _read_stable_regular_file(path, label="slice output artifact") != payload:
                raise DiscoverySliceError(f"concurrent slice artifact drift: {path}")
            return "REUSED"
        _fsync_directory(path.parent)
        return "CREATED"
    finally:
        temporary.unlink(missing_ok=True)


def analyze_phase1a_discovery_slice(
    feature_paths_by_date: Mapping[date | str, Path | str],
    *,
    expected_sha256_by_date: Mapping[date | str, str],
    requested_source_dates: Sequence[date | str],
    no_entry_reasons: Mapping[date | str, str],
    data_root: Path | str,
    code_snapshot_sha256: str,
    run_fingerprint: str,
    config_path: Path | str = DEFAULT_CONFIG_PATH,
) -> DiscoverySliceReport:
    """Analyze and publish one complete five-date slice without adaptive thresholds."""

    config = load_discovery_slice_config(config_path)
    code_snapshot = _sha256(code_snapshot_sha256, label="code_snapshot_sha256")
    fingerprint = _sha256(run_fingerprint, label="run_fingerprint")
    requested = tuple(
        _date(value, label="requested_source_dates") for value in requested_source_dates
    )
    if len(requested) != 5 or tuple(sorted(set(requested))) != requested:
        raise DiscoverySliceError("requested_source_dates must be five unique increasing dates")
    paths: dict[date, Path] = {}
    for key, value in feature_paths_by_date.items():
        parsed = _date(key, label="feature_paths_by_date key")
        if parsed in paths:
            raise DiscoverySliceError("feature_paths_by_date has duplicate normalized dates")
        try:
            paths[parsed] = Path(value)
        except TypeError as exc:
            raise DiscoverySliceError("every feature path must be path-like") from exc
    hashes: dict[date, str] = {}
    for key, value in expected_sha256_by_date.items():
        parsed = _date(key, label="expected_sha256_by_date key")
        if parsed in hashes:
            raise DiscoverySliceError("expected_sha256_by_date has duplicate normalized dates")
        hashes[parsed] = _sha256(value, label="expected feature SHA-256")
    no_entry: dict[date, str] = {}
    for key, value in no_entry_reasons.items():
        parsed = _date(key, label="no_entry_reasons key")
        if parsed in no_entry:
            raise DiscoverySliceError("no_entry_reasons has duplicate normalized dates")
        no_entry[parsed] = value
    if set(paths) != set(hashes):
        raise DiscoverySliceError("feature path and SHA date sets differ")
    if set(paths) | set(no_entry) != set(requested) or set(paths) & set(no_entry):
        raise DiscoverySliceError("feature and no-entry dates must exactly partition the slice")
    if any(not isinstance(reason, str) or not reason.strip() for reason in no_entry.values()):
        raise DiscoverySliceError("every no-entry date requires a non-empty reason")

    root_input = Path(data_root).expanduser()
    if root_input.is_symlink():
        raise DiscoverySliceError("data_root cannot be a symbolic link")
    try:
        root = root_input.resolve(strict=True)
    except FileNotFoundError as exc:
        raise DiscoverySliceError("data_root does not exist") from exc
    if root.name != "data" or not stat.S_ISDIR(root.lstat().st_mode):
        raise DiscoverySliceError("data_root must be an existing directory named data")
    derived = root / "derived"
    if not derived.exists() or not stat.S_ISDIR(derived.lstat().st_mode) or derived.is_symlink():
        raise DiscoverySliceError("data_root must be an existing directory named data")
    rows_by_date: dict[date, list[dict[str, object]]] = {}
    feature_inputs: list[dict[str, object]] = []
    for source_date in sorted(paths):
        rows, identity = _read_feature_file(
            paths[source_date],
            source_date=source_date,
            expected_sha256=hashes[source_date],
            data_root=root,
            code_snapshot_sha256=code_snapshot,
        )
        rows_by_date[source_date] = rows
        feature_inputs.append({"source_date": source_date.isoformat(), **identity})

    eligible_rows = [
        row
        for source_date in sorted(rows_by_date)
        for row in rows_by_date[source_date]
        if row["source_local_signal_input_valid"] is True
    ]
    distributions = {
        field: _integer_distribution(
            [_required_int(row, field) for row in eligible_rows if row.get(field) is not None]
        )
        for field in _DISTRIBUTION_FIELDS
    }

    coverage: list[dict[str, object]] = []
    for source_date in requested:
        if source_date in no_entry:
            coverage.append(
                {
                    "eligible_rows": 0,
                    "feature_rows": 0,
                    "no_entry_reason": no_entry[source_date],
                    "source_date": source_date.isoformat(),
                }
            )
            continue
        rows = rows_by_date[source_date]
        coverage.append(
            {
                "eligible_rows": sum(
                    row["source_local_signal_input_valid"] is True for row in rows
                ),
                "feature_rows": len(rows),
                "invalid_seconds": sum(_required_int(row, "invalid_seconds") for row in rows),
                "maybe_bad_book_seconds": sum(
                    _required_int(row, "maybe_bad_book_seconds") for row in rows
                ),
                "missing_seconds": sum(_required_int(row, "missing_seconds") for row in rows),
                "no_entry_reason": None,
                "reset_seen_seconds": sum(_required_int(row, "reset_seen_seconds") for row in rows),
                "source_date": source_date.isoformat(),
                "stale_seconds": sum(_required_int(row, "stale_seconds") for row in rows),
            }
        )

    query_results: list[dict[str, object]] = []
    for query in config.candidate_queries:
        rule = _RULES[query.query_id]
        occurrences: list[dict[str, object]] = []
        forward_by_horizon: dict[int, list[dict[str, int]]] = {
            horizon: [] for horizon in FORWARD_HORIZONS
        }
        unresolved = dict.fromkeys(FORWARD_HORIZONS, 0)
        source_dates_with_support: set[str] = set()
        direction_counts = {"LONG": 0, "SHORT": 0}
        for source_date in sorted(rows_by_date):
            rows = rows_by_date[source_date]
            by_timestamp = {_required_int(row, "bucket_end_ns"): row for row in rows}
            for row in rows:
                if row["source_local_signal_input_valid"] is not True:
                    continue
                state = _row_state(row)
                direction = rule(row, state)
                if direction is None:
                    continue
                if direction not in (-1, 1):  # pragma: no cover - frozen rules are exhaustive
                    raise DiscoverySliceError(
                        f"query rule returned invalid direction: {query.query_id}"
                    )
                direction_name = "LONG" if direction == 1 else "SHORT"
                direction_counts[direction_name] += 1
                source_dates_with_support.add(source_date.isoformat())
                occurrence_forward: dict[str, dict[str, int] | None] = {}
                occurrence = {
                    "bucket_end_ns": _required_int(row, "bucket_end_ns"),
                    "direction": direction_name,
                    "forward": occurrence_forward,
                    "source_date": source_date.isoformat(),
                    "variables": _research_variables(row, state),
                }
                for horizon in FORWARD_HORIZONS:
                    outcome = _forward_result(
                        by_timestamp,
                        row,
                        direction=direction,
                        horizon=horizon,
                    )
                    if outcome is None:
                        unresolved[horizon] += 1
                    else:
                        forward_by_horizon[horizon].append(outcome)
                    occurrence_forward[str(horizon)] = outcome
                occurrences.append(occurrence)
        query_results.append(
            {
                "definition": query.as_dict(),
                "direction_counts": direction_counts,
                "forward": {
                    str(horizon): _summarize_forward(
                        forward_by_horizon[horizon], unresolved[horizon]
                    )
                    for horizon in FORWARD_HORIZONS
                },
                "occurrences": occurrences,
                "source_date_count": len(source_dates_with_support),
                "support_count": len(occurrences),
            }
        )

    document = {
        "artifact_schema": DISCOVERY_SLICE_SCHEMA,
        "artifact_version": DISCOVERY_SLICE_VERSION,
        "authority": {
            "maximum_authority": "OPEN_OBSERVATION",
            "pass_backtest_allowed": False,
            "screening_survivor_allowed": False,
            "screening_only": True,
        },
        "code_snapshot_sha256": code_snapshot,
        "config": {
            "definition_sha256": config.definition_sha256,
            "relative_path": CONFIG_RELATIVE_PATH,
            "sha256": config.sha256,
        },
        "coverage": coverage,
        "feature_distributions": distributions,
        "feature_inputs": feature_inputs,
        "no_entry_reasons": {
            source_date.isoformat(): no_entry[source_date] for source_date in sorted(no_entry)
        },
        "query_results": query_results,
        "requested_source_dates": [value.isoformat() for value in requested],
        "run_fingerprint": fingerprint,
        "summary": {
            "candidate_query_count": len(query_results),
            "eligible_rows": len(eligible_rows),
            "feature_rows": sum(len(rows) for rows in rows_by_date.values()),
            "nonzero_support_query_count": sum(
                int(result["support_count"] > 0) for result in query_results
            ),
            "zero_support_query_count": sum(
                int(result["support_count"] == 0) for result in query_results
            ),
        },
    }
    payload = _canonical_json(document) + b"\n"
    digest = hashlib.sha256(payload).hexdigest()
    target = root / "derived/manifests" / DISCOVERY_SLICE_VERSION / f"sha256={digest}.json"
    disposition = _publish(target, payload, data_root=root)
    return DiscoverySliceReport(
        path=target,
        sha256=digest,
        byte_size=len(payload),
        disposition=disposition,
        requested_source_dates=tuple(value.isoformat() for value in requested),
        feature_source_dates=tuple(value.isoformat() for value in sorted(paths)),
        no_entry_source_dates=tuple(value.isoformat() for value in sorted(no_entry)),
        total_rows=sum(len(rows) for rows in rows_by_date.values()),
        eligible_rows=len(eligible_rows),
        candidate_query_count=len(query_results),
        nonzero_support_query_count=sum(
            int(result["support_count"] > 0) for result in query_results
        ),
    )
