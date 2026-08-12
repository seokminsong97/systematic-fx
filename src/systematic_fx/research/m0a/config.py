"""Fail-closed loading for immutable, finite-budget M0a epoch manifests."""

from __future__ import annotations

import hashlib
import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from fractions import Fraction
from pathlib import Path
from typing import Any

from systematic_fx.research.hypotheses import canonical_sha256
from systematic_fx.research.m0a.family import (
    PullbackContinuationSearchSpace,
    StrategyFamilyError,
)
from systematic_fx.research.m0a.model import BarrierSpec, Direction, M0aConfigError

_FORBIDDEN_KEY_PARTS = ("holdout", "sealed", "credential")
_ALLOWED_TOP_LEVEL = {
    "epoch",
    "data",
    "features",
    "family",
    "barriers",
    "execution",
    "admission",
    "evaluation",
    "daemon",
    "fixture",
}
_EXPECTED_BARRIER_VALUES = {
    "k_tp": ("0.75", "1.00", "1.25"),
    "k_sl": ("0.50", "0.75", "1.00"),
    "max_hold_minutes": (30, 60, 120),
}
_EXPECTED_TABLE_KEYS = {
    "epoch": {
        "schema_version",
        "epoch_id",
        "parent_epoch_id",
        "dataset_version",
        "dataset_hash",
        "feature_version",
        "label_version",
        "execution_model_version",
        "code_commit",
        "code_snapshot_sha256",
        "family_id",
        "real_candidate_budget",
        "null_candidate_budget",
        "random_seeds",
    },
    "data": {
        "decision_clock_seconds",
        "feature_timeframes_seconds",
        "session_policy",
        "active_contract_policy",
        "hold_same_instrument_until_exit",
        "no_entry_inside_roll_guard",
    },
    "features": {
        "atr_lookback_bars",
        "quantile_lookback_bars",
        "short_trend_lookback_bars",
    },
    "family": {
        "trend_1h_min_ticks",
        "pullback_length_min",
        "pullback_length_max",
        "close_location_threshold_ppm",
        "volatility_quantile_min_ppm",
        "volatility_quantile_max_ppm",
        "imbalance_threshold_ppm",
        "directions",
        "feature_tier",
        "max_generation_attempts_per_candidate",
        "min_generation_attempts",
    },
    "barriers": {"k_tp", "k_sl", "max_hold_minutes"},
    "execution": {
        "route_delay_seconds",
        "entry_adverse_ticks",
        "tp_trade_through_ticks",
        "round_trip_cost_ticks",
    },
    "admission": {
        "min_raw_events",
        "min_sequential_trades",
        "min_active_days",
        "min_tp_probability_ppm",
        "min_positive_folds",
        "require_positive_net_ev",
    },
    "evaluation": {
        "cooldown_seconds",
        "feature_lookback_seconds",
        "purge_policy",
        "fold_count",
        "control_block_size",
        "stressed_cost_numerator",
        "stressed_cost_denominator",
    },
    "daemon": {
        "lease_seconds",
        "system_error_threshold",
        "worker_restart_after_experiments",
        "run_epoch_max_cycles",
        "run_epoch_stop_when_idle",
        "poll_interval_milliseconds",
    },
    "fixture": {"fixture_version"},
}


def compute_code_snapshot_sha256() -> str:
    """Hash the exact M0a Python runtime plus its CLI adapter.

    The source set is ``src/systematic_fx/cli.py`` plus every direct ``*.py``
    child of ``src/systematic_fx/research/m0a``, sorted by path.  For each file,
    canonical JSON records its ``systematic_fx``-relative POSIX path, exact byte
    size, and SHA-256 of the exact bytes; the canonical record array is itself
    SHA-256 hashed.  The epoch TOML is intentionally excluded to avoid a
    self-referential digest.
    """

    module_directory = Path(__file__).resolve().parent
    package_directory = module_directory.parents[1]
    paths = (*sorted(module_directory.glob("*.py")), package_directory / "cli.py")
    if not paths or any(not path.is_file() or path.is_symlink() for path in paths):
        raise M0aConfigError("M0a code snapshot source set is incomplete or unsafe")
    payload = [
        {
            "relative_path": path.relative_to(package_directory).as_posix(),
            "byte_size": len(content := path.read_bytes()),
            "sha256": hashlib.sha256(content).hexdigest(),
        }
        for path in paths
    ]
    return canonical_sha256(payload)


def _required(table: Mapping[str, Any], key: str, expected_type: type[Any]) -> Any:
    try:
        value = table[key]
    except KeyError as exc:
        raise M0aConfigError(f"missing required manifest key: {key}") from exc
    if expected_type is int and isinstance(value, bool):
        raise M0aConfigError(f"manifest key {key} must be an integer")
    if not isinstance(value, expected_type):
        raise M0aConfigError(
            f"manifest key {key} must be {expected_type.__name__}, got {type(value).__name__}"
        )
    return value


def _reject_unsafe_keys(value: Any, *, location: str = "manifest") -> None:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key).lower()
            if (
                any(fragment in key for fragment in _FORBIDDEN_KEY_PARTS)
                or key == "path"
                or key.endswith("_path")
            ):
                raise M0aConfigError(f"forbidden research manifest key at {location}.{raw_key}")
            _reject_unsafe_keys(child, location=f"{location}.{raw_key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_unsafe_keys(child, location=f"{location}[{index}]")
    elif isinstance(value, float):
        raise M0aConfigError(
            f"binary floating point is forbidden in immutable manifests ({location}); use a string"
        )


def _reject_holdout_environment() -> None:
    leaked = sorted(
        name for name in os.environ if name.upper().startswith("SYSTEMATIC_FX_HOLDOUT_")
    )
    if leaked:
        raise M0aConfigError(
            "research process must not receive sealed-holdout environment variables: "
            + ", ".join(leaked)
        )


def _fraction(value: str, *, key: str) -> Fraction:
    try:
        parsed = Fraction(value)
    except (ValueError, ZeroDivisionError) as exc:
        raise M0aConfigError(f"{key} contains an invalid rational: {value!r}") from exc
    if parsed <= 0:
        raise M0aConfigError(f"{key} values must be positive")
    return parsed


def _validate_sha256(value: str, *, key: str) -> str:
    if len(value) != 64:
        raise M0aConfigError(f"{key} must be a 64-character SHA-256 digest")
    try:
        int(value, 16)
    except ValueError as exc:
        raise M0aConfigError(f"{key} must contain hexadecimal characters only") from exc
    return value.lower()


def _integer_axis(table: Mapping[str, Any], key: str) -> tuple[int, ...]:
    values = _required(table, key, list)
    if not values or any(isinstance(value, bool) or not isinstance(value, int) for value in values):
        raise M0aConfigError(f"{key} must be a non-empty integer array")
    result = tuple(int(value) for value in values)
    if len(set(result)) != len(result):
        raise M0aConfigError(f"{key} must not contain duplicate values")
    return result


@dataclass(frozen=True, slots=True)
class EpochConfig:
    epoch_id: str
    schema_version: int
    parent_epoch_id: str | None
    dataset_version: str
    dataset_hash: str
    feature_version: str
    label_version: str
    execution_model_version: str
    code_commit: str
    code_snapshot_sha256: str
    family_id: str
    real_candidate_budget: int
    null_candidate_budget: int
    random_seeds: tuple[int, ...]
    decision_clock_seconds: int
    feature_timeframes_seconds: tuple[int, ...]
    session_policy: str
    active_contract_policy: str
    hold_same_instrument_until_exit: bool
    no_entry_inside_roll_guard: bool
    atr_lookback_bars: int
    quantile_lookback_bars: int
    short_trend_lookback_bars: int
    family_search_space: PullbackContinuationSearchSpace
    barrier_specs: tuple[BarrierSpec, ...]
    route_delay_seconds: int
    entry_adverse_ticks: int
    tp_trade_through_ticks: int
    round_trip_cost_ticks: int
    admission_min_raw_events: int
    admission_min_sequential_trades: int
    admission_min_active_days: int
    admission_min_tp_probability_ppm: int
    admission_min_positive_folds: int
    admission_require_positive_net_ev: bool
    evaluation_cooldown_seconds: int
    evaluation_feature_lookback_seconds: int
    evaluation_purge_policy: str
    evaluation_fold_count: int
    evaluation_control_block_size: int
    evaluation_stressed_cost_numerator: int
    evaluation_stressed_cost_denominator: int
    daemon_lease_seconds: int
    daemon_system_error_threshold: int
    daemon_worker_restart_after_experiments: int
    daemon_run_epoch_max_cycles: int
    daemon_run_epoch_stop_when_idle: bool
    daemon_poll_interval_milliseconds: int
    fixture_version: str
    epoch_hash: str
    file_sha256: str
    _manifest_path: Path

    @property
    def manifest_path(self) -> Path:
        return self._manifest_path

    @property
    def search_budget(self) -> int:
        return self.real_candidate_budget

    @property
    def null_budget(self) -> int:
        return self.null_candidate_budget

    @property
    def admission_rules(self) -> dict[str, int | bool]:
        """Return the immutable search-data screen without importing evaluation."""

        return {
            "min_raw_events": self.admission_min_raw_events,
            "min_sequential_trades": self.admission_min_sequential_trades,
            "min_active_days": self.admission_min_active_days,
            "min_tp_probability_ppm": self.admission_min_tp_probability_ppm,
            "min_positive_folds": self.admission_min_positive_folds,
            "require_positive_net_ev": self.admission_require_positive_net_ev,
        }

    @property
    def evaluation_options(self) -> dict[str, int | str]:
        return {
            "cooldown_seconds": self.evaluation_cooldown_seconds,
            "feature_lookback_seconds": self.evaluation_feature_lookback_seconds,
            "purge_policy": self.evaluation_purge_policy,
            "fold_count": self.evaluation_fold_count,
            "control_block_size": self.evaluation_control_block_size,
            "stressed_cost_numerator": self.evaluation_stressed_cost_numerator,
            "stressed_cost_denominator": self.evaluation_stressed_cost_denominator,
        }

    @property
    def daemon_options(self) -> dict[str, int | bool]:
        return {
            "lease_seconds": self.daemon_lease_seconds,
            "system_error_threshold": self.daemon_system_error_threshold,
            "worker_restart_after_experiments": self.daemon_worker_restart_after_experiments,
            "run_epoch_max_cycles": self.daemon_run_epoch_max_cycles,
            "run_epoch_stop_when_idle": self.daemon_run_epoch_stop_when_idle,
            "poll_interval_milliseconds": self.daemon_poll_interval_milliseconds,
        }

    def verify_unchanged(self) -> None:
        """Fail if the manifest or holdout-free process boundary changed after load."""

        _reject_holdout_environment()
        try:
            current = self._manifest_path.read_bytes()
        except OSError as exc:
            raise M0aConfigError(
                f"cannot re-read immutable manifest: {self._manifest_path}"
            ) from exc
        digest = hashlib.sha256(current).hexdigest()
        if digest != self.file_sha256:
            raise M0aConfigError("epoch manifest changed after it was loaded")
        if compute_code_snapshot_sha256() != self.code_snapshot_sha256:
            raise M0aConfigError("M0a runtime code changed after the epoch was precommitted")

    def as_dict(self) -> dict[str, Any]:
        """Return the precommitted semantic values (excluding the local file location)."""

        return {
            "epoch_id": self.epoch_id,
            "schema_version": self.schema_version,
            "parent_epoch_id": self.parent_epoch_id,
            "dataset_version": self.dataset_version,
            "dataset_hash": self.dataset_hash,
            "feature_version": self.feature_version,
            "label_version": self.label_version,
            "execution_model_version": self.execution_model_version,
            "code_commit": self.code_commit,
            "code_snapshot_sha256": self.code_snapshot_sha256,
            "family_id": self.family_id,
            "real_candidate_budget": self.real_candidate_budget,
            "null_candidate_budget": self.null_candidate_budget,
            "random_seeds": list(self.random_seeds),
            "decision_clock_seconds": self.decision_clock_seconds,
            "feature_timeframes_seconds": list(self.feature_timeframes_seconds),
            "session_policy": self.session_policy,
            "active_contract_policy": self.active_contract_policy,
            "hold_same_instrument_until_exit": self.hold_same_instrument_until_exit,
            "no_entry_inside_roll_guard": self.no_entry_inside_roll_guard,
            "atr_lookback_bars": self.atr_lookback_bars,
            "quantile_lookback_bars": self.quantile_lookback_bars,
            "short_trend_lookback_bars": self.short_trend_lookback_bars,
            "family_search_space": self.family_search_space.as_dict(),
            "barrier_specs": [spec.as_dict() for spec in self.barrier_specs],
            "route_delay_seconds": self.route_delay_seconds,
            "entry_adverse_ticks": self.entry_adverse_ticks,
            "tp_trade_through_ticks": self.tp_trade_through_ticks,
            "round_trip_cost_ticks": self.round_trip_cost_ticks,
            "admission_min_raw_events": self.admission_min_raw_events,
            "admission_min_sequential_trades": self.admission_min_sequential_trades,
            "admission_min_active_days": self.admission_min_active_days,
            "admission_min_tp_probability_ppm": self.admission_min_tp_probability_ppm,
            "admission_min_positive_folds": self.admission_min_positive_folds,
            "admission_require_positive_net_ev": self.admission_require_positive_net_ev,
            "evaluation_cooldown_seconds": self.evaluation_cooldown_seconds,
            "evaluation_feature_lookback_seconds": self.evaluation_feature_lookback_seconds,
            "evaluation_purge_policy": self.evaluation_purge_policy,
            "evaluation_fold_count": self.evaluation_fold_count,
            "evaluation_control_block_size": self.evaluation_control_block_size,
            "evaluation_stressed_cost_numerator": self.evaluation_stressed_cost_numerator,
            "evaluation_stressed_cost_denominator": self.evaluation_stressed_cost_denominator,
            "daemon_lease_seconds": self.daemon_lease_seconds,
            "daemon_system_error_threshold": self.daemon_system_error_threshold,
            "daemon_worker_restart_after_experiments": (
                self.daemon_worker_restart_after_experiments
            ),
            "daemon_run_epoch_max_cycles": self.daemon_run_epoch_max_cycles,
            "daemon_run_epoch_stop_when_idle": self.daemon_run_epoch_stop_when_idle,
            "daemon_poll_interval_milliseconds": self.daemon_poll_interval_milliseconds,
            "fixture_version": self.fixture_version,
            "epoch_hash": self.epoch_hash,
            "file_sha256": self.file_sha256,
        }


def load_epoch(path: str | Path) -> EpochConfig:
    """Load and validate a finite, immutable, holdout-blind epoch manifest."""

    _reject_holdout_environment()
    supplied_path = Path(path).expanduser()
    if supplied_path.is_symlink():
        raise M0aConfigError("epoch manifest must be a regular, non-symlink TOML file")
    manifest_path = supplied_path.resolve(strict=True)
    if not manifest_path.is_file():
        raise M0aConfigError("epoch manifest must be a regular, non-symlink TOML file")
    raw = manifest_path.read_bytes()
    file_sha256 = hashlib.sha256(raw).hexdigest()
    try:
        document = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise M0aConfigError(f"invalid UTF-8 TOML manifest: {manifest_path}") from exc

    _reject_unsafe_keys(document)
    if set(document) != _ALLOWED_TOP_LEVEL:
        missing = sorted(_ALLOWED_TOP_LEVEL - set(document))
        extra = sorted(set(document) - _ALLOWED_TOP_LEVEL)
        raise M0aConfigError(
            f"manifest tables differ from schema; missing={missing}, extra={extra}"
        )
    epoch_hash = canonical_sha256(document)

    epoch = document["epoch"]
    data = document["data"]
    features = document["features"]
    family = document["family"]
    barriers = document["barriers"]
    execution = document["execution"]
    admission = document["admission"]
    evaluation = document["evaluation"]
    daemon = document["daemon"]
    fixture = document["fixture"]
    for name, table in (
        ("epoch", epoch),
        ("data", data),
        ("features", features),
        ("family", family),
        ("barriers", barriers),
        ("execution", execution),
        ("admission", admission),
        ("evaluation", evaluation),
        ("daemon", daemon),
        ("fixture", fixture),
    ):
        if not isinstance(table, Mapping):
            raise M0aConfigError(f"[{name}] must be a TOML table")
        expected_keys = _EXPECTED_TABLE_KEYS[name]
        if set(table) != expected_keys:
            missing = sorted(expected_keys - set(table))
            extra = sorted(set(table) - expected_keys)
            raise M0aConfigError(
                f"[{name}] keys differ from schema; missing={missing}, extra={extra}"
            )

    schema_version = _required(epoch, "schema_version", int)
    if schema_version != 1:
        raise M0aConfigError("only M0a epoch schema_version=1 is supported")
    parent = _required(epoch, "parent_epoch_id", str) or None
    dataset_hash = _validate_sha256(_required(epoch, "dataset_hash", str), key="dataset_hash")
    real_budget = _required(epoch, "real_candidate_budget", int)
    null_budget = _required(epoch, "null_candidate_budget", int)
    if not (0 < real_budget <= 10_000 and 0 < null_budget <= 10_000):
        raise M0aConfigError("real and null budgets must be finite integers in [1, 10000]")
    seeds_raw = _required(epoch, "random_seeds", list)
    if not seeds_raw or any(
        isinstance(seed, bool) or not isinstance(seed, int) for seed in seeds_raw
    ):
        raise M0aConfigError("random_seeds must be a non-empty integer array")
    seeds = tuple(int(seed) for seed in seeds_raw)
    if len(set(seeds)) != len(seeds):
        raise M0aConfigError("random_seeds must be unique")

    decision_clock = _required(data, "decision_clock_seconds", int)
    timeframes_raw = _required(data, "feature_timeframes_seconds", list)
    if decision_clock != 300 or tuple(timeframes_raw) != (300, 1800, 3600):
        raise M0aConfigError("M0a requires a 5m decision clock with 5m/30m/1h features")
    session_policy = _required(data, "session_policy", str)
    active_policy = _required(data, "active_contract_policy", str)
    hold_same = _required(data, "hold_same_instrument_until_exit", bool)
    no_roll_entry = _required(data, "no_entry_inside_roll_guard", bool)
    if session_policy != "NO_CROSS_CLOSED_MARKET":
        raise M0aConfigError("M0a only supports NO_CROSS_CLOSED_MARKET")
    if active_policy != "previous_day_volume" or not hold_same or not no_roll_entry:
        raise M0aConfigError("M0a contract invariants cannot be disabled")

    atr_lookback = _required(features, "atr_lookback_bars", int)
    quantile_lookback = _required(features, "quantile_lookback_bars", int)
    short_trend_lookback = _required(features, "short_trend_lookback_bars", int)
    if min(atr_lookback, quantile_lookback, short_trend_lookback) <= 0:
        raise M0aConfigError("feature lookbacks must be positive")
    if max(atr_lookback, quantile_lookback, short_trend_lookback) > 288:
        raise M0aConfigError("fixture feature lookbacks must be finite and no greater than one day")

    imbalance_raw = _required(family, "imbalance_threshold_ppm", list)
    if not imbalance_raw or any(not isinstance(value, str) for value in imbalance_raw):
        raise M0aConfigError("imbalance_threshold_ppm must be a non-empty string array")
    try:
        imbalance_axis = tuple(None if value == "NONE" else int(value) for value in imbalance_raw)
    except ValueError as exc:
        raise M0aConfigError("imbalance_threshold_ppm contains an invalid value") from exc
    if len(set(imbalance_axis)) != len(imbalance_axis):
        raise M0aConfigError("imbalance_threshold_ppm must not contain duplicates")
    directions_raw = _required(family, "directions", list)
    try:
        directions = tuple(Direction(str(value)) for value in directions_raw)
    except ValueError as exc:
        raise M0aConfigError("family directions must contain only long and short") from exc
    try:
        family_search_space = PullbackContinuationSearchSpace(
            trend_1h_min_ticks=_integer_axis(family, "trend_1h_min_ticks"),
            pullback_length_min=_integer_axis(family, "pullback_length_min"),
            pullback_length_max=_integer_axis(family, "pullback_length_max"),
            close_location_threshold_ppm=_integer_axis(family, "close_location_threshold_ppm"),
            volatility_quantile_min_ppm=_integer_axis(family, "volatility_quantile_min_ppm"),
            volatility_quantile_max_ppm=_integer_axis(family, "volatility_quantile_max_ppm"),
            imbalance_threshold_ppm=imbalance_axis,
            directions=directions,
            feature_tier=_required(family, "feature_tier", str),
            max_generation_attempts_per_candidate=_required(
                family, "max_generation_attempts_per_candidate", int
            ),
            min_generation_attempts=_required(family, "min_generation_attempts", int),
        )
    except StrategyFamilyError as exc:
        raise M0aConfigError(f"invalid family search space: {exc}") from exc

    barrier_values: dict[str, tuple[Any, ...]] = {}
    for key, expected in _EXPECTED_BARRIER_VALUES.items():
        raw_values = _required(barriers, key, list)
        values = tuple(raw_values)
        if values != expected:
            raise M0aConfigError(f"M0a {key} grid must be exactly {expected!r}")
        barrier_values[key] = values
    barrier_specs: list[BarrierSpec] = []
    for k_tp_text in barrier_values["k_tp"]:
        k_tp = _fraction(k_tp_text, key="k_tp")
        for k_sl_text in barrier_values["k_sl"]:
            k_sl = _fraction(k_sl_text, key="k_sl")
            for hold_minutes in barrier_values["max_hold_minutes"]:
                barrier_specs.append(
                    BarrierSpec(
                        barrier_id=(
                            f"tp{k_tp.numerator:03d}of{k_tp.denominator:03d}_"
                            f"sl{k_sl.numerator:03d}of{k_sl.denominator:03d}_"
                            f"h{int(hold_minutes):03d}m"
                        ),
                        k_tp_num=k_tp.numerator,
                        k_tp_den=k_tp.denominator,
                        k_sl_num=k_sl.numerator,
                        k_sl_den=k_sl.denominator,
                        max_hold_seconds=int(hold_minutes) * 60,
                    )
                )

    route_delay = _required(execution, "route_delay_seconds", int)
    entry_adverse = _required(execution, "entry_adverse_ticks", int)
    tp_through = _required(execution, "tp_trade_through_ticks", int)
    cost_ticks = _required(execution, "round_trip_cost_ticks", int)
    if route_delay != 1 or entry_adverse != 1 or tp_through != 1 or cost_ticks < 0:
        raise M0aConfigError(
            "M0a execution requires one-second routing, one adverse entry tick, "
            "and one-tick TP trade-through"
        )

    admission_min_raw = _required(admission, "min_raw_events", int)
    admission_min_sequential = _required(admission, "min_sequential_trades", int)
    admission_min_days = _required(admission, "min_active_days", int)
    admission_min_probability = _required(admission, "min_tp_probability_ppm", int)
    admission_min_folds = _required(admission, "min_positive_folds", int)
    admission_require_ev = _required(admission, "require_positive_net_ev", bool)
    if (
        min(
            admission_min_raw,
            admission_min_sequential,
            admission_min_days,
            admission_min_folds,
        )
        < 0
    ):
        raise M0aConfigError("admission count thresholds must be non-negative")
    if not 0 <= admission_min_probability <= 1_000_000:
        raise M0aConfigError("admission TP probability threshold must be in ppm")

    evaluation_cooldown = _required(evaluation, "cooldown_seconds", int)
    evaluation_lookback = _required(evaluation, "feature_lookback_seconds", int)
    evaluation_purge = _required(evaluation, "purge_policy", str)
    evaluation_folds = _required(evaluation, "fold_count", int)
    evaluation_control_block = _required(evaluation, "control_block_size", int)
    evaluation_stress_num = _required(evaluation, "stressed_cost_numerator", int)
    evaluation_stress_den = _required(evaluation, "stressed_cost_denominator", int)
    if (
        evaluation_cooldown < 0
        or min(
            evaluation_lookback,
            evaluation_folds,
            evaluation_control_block,
            evaluation_stress_num,
            evaluation_stress_den,
        )
        <= 0
    ):
        raise M0aConfigError("evaluation runtime values must be finite and positive")
    if evaluation_purge != "MAX_HOLD_PLUS_FEATURE_LOOKBACK":
        raise M0aConfigError("unsupported M0a purge policy")

    daemon_lease = _required(daemon, "lease_seconds", int)
    daemon_error_threshold = _required(daemon, "system_error_threshold", int)
    daemon_restart = _required(daemon, "worker_restart_after_experiments", int)
    daemon_max_cycles = _required(daemon, "run_epoch_max_cycles", int)
    daemon_stop_idle = _required(daemon, "run_epoch_stop_when_idle", bool)
    daemon_poll_ms = _required(daemon, "poll_interval_milliseconds", int)
    if min(daemon_lease, daemon_error_threshold, daemon_restart, daemon_max_cycles) <= 0:
        raise M0aConfigError("daemon lease, threshold, restart, and cycle bounds must be positive")
    if daemon_poll_ms < 0:
        raise M0aConfigError("daemon poll interval cannot be negative")

    family_id = _required(epoch, "family_id", str)
    if family_id != "pullback_continuation_v1":
        raise M0aConfigError("M0a commits exactly one strategy family")

    code_commit = _required(epoch, "code_commit", str)
    if len(code_commit) != 40:
        raise M0aConfigError("code_commit must be a full 40-character Git object id")
    try:
        int(code_commit, 16)
    except ValueError as exc:
        raise M0aConfigError("code_commit must be hexadecimal") from exc
    code_snapshot_sha256 = _validate_sha256(
        _required(epoch, "code_snapshot_sha256", str), key="code_snapshot_sha256"
    )
    if code_snapshot_sha256 == "0" * 64:
        raise M0aConfigError("code_snapshot_sha256 cannot use the zero sentinel")

    config = EpochConfig(
        epoch_id=_required(epoch, "epoch_id", str),
        schema_version=schema_version,
        parent_epoch_id=parent,
        dataset_version=_required(epoch, "dataset_version", str),
        dataset_hash=dataset_hash,
        feature_version=_required(epoch, "feature_version", str),
        label_version=_required(epoch, "label_version", str),
        execution_model_version=_required(epoch, "execution_model_version", str),
        code_commit=code_commit,
        code_snapshot_sha256=code_snapshot_sha256,
        family_id=family_id,
        real_candidate_budget=real_budget,
        null_candidate_budget=null_budget,
        random_seeds=seeds,
        decision_clock_seconds=decision_clock,
        feature_timeframes_seconds=tuple(int(value) for value in timeframes_raw),
        session_policy=session_policy,
        active_contract_policy=active_policy,
        hold_same_instrument_until_exit=hold_same,
        no_entry_inside_roll_guard=no_roll_entry,
        atr_lookback_bars=atr_lookback,
        quantile_lookback_bars=quantile_lookback,
        short_trend_lookback_bars=short_trend_lookback,
        family_search_space=family_search_space,
        barrier_specs=tuple(barrier_specs),
        route_delay_seconds=route_delay,
        entry_adverse_ticks=entry_adverse,
        tp_trade_through_ticks=tp_through,
        round_trip_cost_ticks=cost_ticks,
        admission_min_raw_events=admission_min_raw,
        admission_min_sequential_trades=admission_min_sequential,
        admission_min_active_days=admission_min_days,
        admission_min_tp_probability_ppm=admission_min_probability,
        admission_min_positive_folds=admission_min_folds,
        admission_require_positive_net_ev=admission_require_ev,
        evaluation_cooldown_seconds=evaluation_cooldown,
        evaluation_feature_lookback_seconds=evaluation_lookback,
        evaluation_purge_policy=evaluation_purge,
        evaluation_fold_count=evaluation_folds,
        evaluation_control_block_size=evaluation_control_block,
        evaluation_stressed_cost_numerator=evaluation_stress_num,
        evaluation_stressed_cost_denominator=evaluation_stress_den,
        daemon_lease_seconds=daemon_lease,
        daemon_system_error_threshold=daemon_error_threshold,
        daemon_worker_restart_after_experiments=daemon_restart,
        daemon_run_epoch_max_cycles=daemon_max_cycles,
        daemon_run_epoch_stop_when_idle=daemon_stop_idle,
        daemon_poll_interval_milliseconds=daemon_poll_ms,
        fixture_version=_required(fixture, "fixture_version", str),
        epoch_hash=epoch_hash,
        file_sha256=file_sha256,
        _manifest_path=manifest_path,
    )
    if not config.epoch_id or not config.dataset_version or not config.fixture_version:
        raise M0aConfigError("epoch, dataset, and fixture versions must not be empty")
    if compute_code_snapshot_sha256() != config.code_snapshot_sha256:
        raise M0aConfigError("code_snapshot_sha256 differs from the current M0a runtime source")
    return config
