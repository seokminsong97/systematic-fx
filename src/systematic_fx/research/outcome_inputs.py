"""Strict, deterministic inputs and daily-cache plans for the p5 replay.

Discovery artifacts are immutable evidence, not trusted Python objects.  This
module reopens every canonical artifact without following a leaf symlink,
checks its exact content identity, and retains every frozen occurrence
variable.  It then expands the signal contracts across the eligible
source-date calendar.  A contract is cached from its first signal date through
the last eligible source date before its expiry month so portfolio occupancy
can continue after the scanner's independent 20-session first-touch label is
censored.

No file is written here.  Absolute source paths are deliberately excluded from
canonical hashes; their manifest-relative URI and content SHA-256 are the
portable source identity.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from typing import Final

from systematic_fx.backtest.barriers import Direction
from systematic_fx.backtest.event_cache import DailyCacheReport, DailyCacheSpec
from systematic_fx.backtest.shared_replay import FIRST_TOUCH_ACTIVE_SESSIONS, SignalSeed
from systematic_fx.data.contract_selection import (
    ContractSelectionError,
    resolve_6e_contract_month,
)
from systematic_fx.db.data_registry import SourceFileRegistration, SourceManifestBundle
from systematic_fx.research.discovery_slice import (
    DISCOVERY_FORWARD_RESULT_FIELDS,
    DISCOVERY_SLICE_SCHEMA,
    DISCOVERY_SLICE_VERSION,
    DISCOVERY_VARIABLE_FIELDS,
    FORWARD_HORIZONS,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.outcome_config import (
    EXPECTED_LONG_SIGNAL_COUNT,
    EXPECTED_SHORT_SIGNAL_COUNT,
    EXPECTED_SIGNAL_COUNT,
    EXPECTED_SLICE_INDICES,
    P5_QUERY_ID,
    TERMINAL_EXIT_POLICY,
    TERMINAL_PARTITION_RESOLUTION_POLICY,
)

_SHA256: Final = re.compile(r"[0-9a-f]{64}")
_TARGET_DEFINITION: Final = {
    "conditions": [
        "bar_range_x2_ticks>=32",
        "abs(bar_move_x2_ticks)>=8",
        "same_sign_signed_flow",
        "last_spread_ticks<=2",
    ],
    "direction_rule": "SIGN_BAR_MOVE",
    "id": P5_QUERY_ID,
    "parent_hypothesis_ids": ["p5_03_volatility_expansion_continuation"],
}
_ARTIFACT_FIELDS: Final = {
    "artifact_schema",
    "artifact_version",
    "authority",
    "code_snapshot_sha256",
    "config",
    "coverage",
    "feature_distributions",
    "feature_inputs",
    "no_entry_reasons",
    "query_results",
    "requested_source_dates",
    "run_fingerprint",
    "summary",
}
_QUERY_RESULT_FIELDS: Final = {
    "definition",
    "direction_counts",
    "forward",
    "occurrences",
    "source_date_count",
    "support_count",
}
_OCCURRENCE_FIELDS: Final = {
    "bucket_end_ns",
    "direction",
    "forward",
    "source_date",
    "variables",
}
_JSON_SCALAR_TYPES: Final = (str, int, bool, type(None))

type JsonScalar = str | int | bool | None


class OutcomeInputError(ValueError):
    """Canonical p5 evidence or its deterministic replay plan is invalid."""


def _sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise OutcomeInputError(f"{label} must be a lowercase SHA-256")
    return value


def _integer(value: object, *, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise OutcomeInputError(f"{label} must be an integer >= {minimum}")
    return value


def _text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise OutcomeInputError(f"{label} must be a canonical non-empty string")
    return value


def _date(value: object, *, label: str) -> date:
    if isinstance(value, datetime):
        raise OutcomeInputError(f"{label} must not be a datetime")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise OutcomeInputError(f"{label} must be a canonical ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise OutcomeInputError(f"{label} must be a canonical ISO date") from error
    if parsed.isoformat() != value:
        raise OutcomeInputError(f"{label} must be a canonical ISO date")
    return parsed


def _strict_dates(values: Sequence[date], *, label: str) -> tuple[date, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence) or not values:
        raise OutcomeInputError(f"{label} must be a non-empty ordered sequence")
    result: list[date] = []
    prior: date | None = None
    for index, value in enumerate(values):
        parsed = _date(value, label=f"{label}[{index}]")
        if prior is not None and parsed <= prior:
            raise OutcomeInputError(f"{label} must be strictly increasing and unique")
        result.append(parsed)
        prior = parsed
    return tuple(result)


def _file_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_verified_artifact(descriptor: CanonicalDiscoveryArtifact) -> bytes:
    path = Path(os.path.abspath(os.fspath(descriptor.path.expanduser())))
    try:
        before = path.lstat()
    except OSError as error:
        raise OutcomeInputError(f"cannot inspect Discovery artifact: {path}") from error
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise OutcomeInputError(f"Discovery artifact must be a non-symlink regular file: {path}")
    if before.st_size != descriptor.byte_size:
        raise OutcomeInputError(f"Discovery artifact byte-size drift: {path}")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        file_descriptor = os.open(path, flags)
    except OSError as error:
        raise OutcomeInputError(f"cannot safely open Discovery artifact: {path}") from error
    digest = hashlib.sha256()
    chunks: list[bytes] = []
    opened: os.stat_result | None = None
    after_descriptor: os.stat_result | None = None
    try:
        try:
            opened = os.fstat(file_descriptor)
            if _file_identity(opened) != _file_identity(before):
                raise OutcomeInputError(f"Discovery artifact changed before open: {path}")
            while chunk := os.read(file_descriptor, 1024 * 1024):
                digest.update(chunk)
                chunks.append(chunk)
            after_descriptor = os.fstat(file_descriptor)
        except OSError as error:
            raise OutcomeInputError(f"cannot read Discovery artifact: {path}") from error
    finally:
        os.close(file_descriptor)
    try:
        after_path = path.lstat()
    except OSError as error:
        raise OutcomeInputError(f"Discovery artifact disappeared while reading: {path}") from error
    if opened is None or after_descriptor is None:  # pragma: no cover - successful reads set both
        raise OutcomeInputError(f"Discovery artifact could not be inspected: {path}")
    identities = {
        _file_identity(before),
        _file_identity(opened),
        _file_identity(after_descriptor),
        _file_identity(after_path),
    }
    if len(identities) != 1:
        raise OutcomeInputError(f"Discovery artifact changed while reading: {path}")
    if digest.hexdigest() != descriptor.sha256:
        raise OutcomeInputError(f"Discovery artifact SHA-256 drift: {path}")
    return b"".join(chunks)


def _json_document(payload: bytes, *, descriptor: CanonicalDiscoveryArtifact) -> dict[str, object]:
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise OutcomeInputError(
            f"Discovery artifact {descriptor.slice_index} is not valid UTF-8 JSON"
        ) from error
    if not isinstance(document, dict):
        raise OutcomeInputError("Discovery artifact root must be an object")
    try:
        canonical = canonical_json_bytes(document) + b"\n"
    except TypeError as error:
        raise OutcomeInputError("Discovery artifacts cannot contain binary floats") from error
    if payload != canonical:
        raise OutcomeInputError("Discovery artifact is not canonical newline-terminated JSON")
    if set(document) != _ARTIFACT_FIELDS:
        raise OutcomeInputError("Discovery artifact top-level schema drift")
    if (
        document.get("artifact_schema") != DISCOVERY_SLICE_SCHEMA
        or document.get("artifact_version") != DISCOVERY_SLICE_VERSION
    ):
        raise OutcomeInputError("Discovery artifact schema/version drift")
    authority = document.get("authority")
    if authority != {
        "maximum_authority": "OPEN_OBSERVATION",
        "pass_backtest_allowed": False,
        "screening_survivor_allowed": False,
        "screening_only": True,
    }:
        raise OutcomeInputError("Discovery artifact authority drift")
    _sha256(document.get("code_snapshot_sha256"), label="code_snapshot_sha256")
    _sha256(document.get("run_fingerprint"), label="run_fingerprint")
    config = document.get("config")
    if not isinstance(config, dict) or set(config) != {
        "definition_sha256",
        "relative_path",
        "sha256",
    }:
        raise OutcomeInputError("Discovery artifact config lineage drift")
    _sha256(config.get("definition_sha256"), label="Discovery definition_sha256")
    _sha256(config.get("sha256"), label="Discovery config_sha256")
    if config.get("relative_path") != "configs/research/phase1a_discovery_slice_v1.toml":
        raise OutcomeInputError("Discovery config relative path drift")
    return document


@dataclass(frozen=True, slots=True)
class CanonicalDiscoveryArtifact:
    """Database-resolved identity of one canonical successful AI slice artifact."""

    slice_index: int
    path: Path
    sha256: str
    byte_size: int

    def __post_init__(self) -> None:
        _integer(self.slice_index, label="slice_index")
        if not isinstance(self.path, Path):
            raise OutcomeInputError("Discovery artifact path must be a Path")
        _sha256(self.sha256, label="Discovery artifact sha256")
        _integer(self.byte_size, label="Discovery artifact byte_size", minimum=1)

    def as_dict(self) -> dict[str, object]:
        return {
            "byte_size": self.byte_size,
            "path": str(self.path),
            "sha256": self.sha256,
            "slice_index": self.slice_index,
        }

    def canonical_identity(self) -> dict[str, object]:
        """Portable artifact identity; local absolute paths are not hashed."""

        return {
            "byte_size": self.byte_size,
            "sha256": self.sha256,
            "slice_index": self.slice_index,
        }


@dataclass(frozen=True, slots=True)
class ReplaySignal:
    """One immutable p5 signal with every Discovery variable retained."""

    signal_id: str
    slice_index: int
    occurrence_index: int
    source_date: date
    bucket_end_ns: int
    direction: Direction
    contract: str
    variables: tuple[tuple[str, JsonScalar], ...]

    def __post_init__(self) -> None:
        _text(self.signal_id, label="signal_id")
        _integer(self.slice_index, label="signal slice_index")
        _integer(self.occurrence_index, label="signal occurrence_index")
        _date(self.source_date, label="signal source_date")
        _integer(self.bucket_end_ns, label="bucket_end_ns", minimum=1)
        if not isinstance(self.direction, Direction):
            raise OutcomeInputError("signal direction must be LONG or SHORT")
        _text(self.contract, label="signal contract")
        if not isinstance(self.variables, tuple) or any(
            not isinstance(item, tuple) or len(item) != 2 for item in self.variables
        ):
            raise OutcomeInputError("signal variables must be an ordered tuple")
        names = tuple(name for name, _ in self.variables)
        if names != DISCOVERY_VARIABLE_FIELDS:
            raise OutcomeInputError("signal variable schema/order drift")
        for name, value in self.variables:
            if not isinstance(name, str) or not isinstance(value, _JSON_SCALAR_TYPES):
                raise OutcomeInputError(f"signal variable {name!r} is not a JSON scalar")
            if isinstance(value, float):  # bool/int distinction is intentional above
                raise OutcomeInputError(f"signal variable {name!r} cannot be a float")
        if self.variable_map["contract"] != self.contract:
            raise OutcomeInputError("signal contract differs from variables.contract")
        try:
            timestamp_date = datetime.fromtimestamp(
                self.bucket_end_ns // 1_000_000_000,
                UTC,
            ).date()
        except (OSError, OverflowError, ValueError) as error:
            raise OutcomeInputError("bucket_end_ns is outside the supported UTC range") from error
        if timestamp_date != self.source_date:
            raise OutcomeInputError("bucket_end_ns UTC date differs from signal source_date")

    @property
    def variable_map(self) -> dict[str, JsonScalar]:
        return dict(self.variables)

    @property
    def utc_month(self) -> str:
        seconds = self.bucket_end_ns // 1_000_000_000
        return datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m")

    def to_seed(self) -> SignalSeed:
        return SignalSeed(
            signal_id=self.signal_id,
            decision_ts_recv_ns=self.bucket_end_ns,
            utc_month=self.utc_month,
            direction=self.direction,
            contract_key=self.contract,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "bucket_end_ns": self.bucket_end_ns,
            "contract": self.contract,
            "direction": self.direction.value,
            "occurrence_index": self.occurrence_index,
            "signal_id": self.signal_id,
            "slice_index": self.slice_index,
            "source_date": self.source_date.isoformat(),
            "variables": self.variable_map,
        }


@dataclass(frozen=True, slots=True)
class P5DiscoveryInputs:
    """Verified artifacts and their ordered complete p5 signal evidence."""

    artifacts: tuple[CanonicalDiscoveryArtifact, ...]
    signals: tuple[ReplaySignal, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.artifacts, tuple) or any(
            not isinstance(artifact, CanonicalDiscoveryArtifact) for artifact in self.artifacts
        ):
            raise OutcomeInputError("artifacts must be CanonicalDiscoveryArtifact values")
        artifact_indices = tuple(artifact.slice_index for artifact in self.artifacts)
        if artifact_indices != tuple(sorted(set(artifact_indices))):
            raise OutcomeInputError("Discovery artifacts must be uniquely ordered by slice")
        if not isinstance(self.signals, tuple) or any(
            not isinstance(signal, ReplaySignal) for signal in self.signals
        ):
            raise OutcomeInputError("signals must be ReplaySignal values")
        signal_ids: set[str] = set()
        prior_order: tuple[date, int] | None = None
        for signal in self.signals:
            order = (signal.source_date, signal.bucket_end_ns)
            if prior_order is not None and order <= prior_order:
                raise OutcomeInputError("p5 signals must be uniquely ordered by source time")
            if signal.signal_id in signal_ids:
                raise OutcomeInputError("p5 signal IDs must be unique")
            signal_ids.add(signal.signal_id)
            prior_order = order

    @property
    def artifact_manifest_sha256(self) -> str:
        return canonical_sha256([artifact.canonical_identity() for artifact in self.artifacts])

    @property
    def signal_manifest_sha256(self) -> str:
        return canonical_sha256([signal.as_dict() for signal in self.signals])

    @property
    def input_manifest_sha256(self) -> str:
        return canonical_sha256(
            {
                "artifact_manifest_sha256": self.artifact_manifest_sha256,
                "query_id": P5_QUERY_ID,
                "signal_manifest_sha256": self.signal_manifest_sha256,
            }
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_manifest_sha256": self.artifact_manifest_sha256,
            "artifacts": [artifact.as_dict() for artifact in self.artifacts],
            "input_manifest_sha256": self.input_manifest_sha256,
            "query_id": P5_QUERY_ID,
            "signal_manifest_sha256": self.signal_manifest_sha256,
            "signals": [signal.as_dict() for signal in self.signals],
        }


def _requested_dates(document: Mapping[str, object], *, slice_index: int) -> tuple[date, ...]:
    raw = document.get("requested_source_dates")
    if not isinstance(raw, list) or len(raw) != 5:
        raise OutcomeInputError(f"Discovery slice {slice_index} must contain five source dates")
    return _strict_dates(raw, label=f"Discovery slice {slice_index} requested_source_dates")  # type: ignore[arg-type]


def _target_query(document: Mapping[str, object], *, slice_index: int) -> Mapping[str, object]:
    raw = document.get("query_results")
    if not isinstance(raw, list) or len(raw) != 11:
        raise OutcomeInputError(f"Discovery slice {slice_index} query cardinality drift")
    query_ids: list[str] = []
    target: Mapping[str, object] | None = None
    for value in raw:
        if not isinstance(value, dict) or set(value) != _QUERY_RESULT_FIELDS:
            raise OutcomeInputError(f"Discovery slice {slice_index} query result schema drift")
        definition = value.get("definition")
        if not isinstance(definition, dict):
            raise OutcomeInputError(f"Discovery slice {slice_index} query definition drift")
        query_id = _text(definition.get("id"), label="Discovery query id")
        query_ids.append(query_id)
        if query_id == P5_QUERY_ID:
            if target is not None:
                raise OutcomeInputError(f"Discovery slice {slice_index} repeats p5 query")
            target = value
    if len(query_ids) != len(set(query_ids)) or target is None:
        raise OutcomeInputError(f"Discovery slice {slice_index} query identity drift")
    if target.get("definition") != _TARGET_DEFINITION:
        raise OutcomeInputError(f"Discovery slice {slice_index} p5 definition drift")
    return target


def _validate_forward(value: object, *, label: str) -> None:
    expected_horizons = {str(horizon) for horizon in FORWARD_HORIZONS}
    if not isinstance(value, dict) or set(value) != expected_horizons:
        raise OutcomeInputError(f"{label} forward horizon schema drift")
    expected_fields = set(DISCOVERY_FORWARD_RESULT_FIELDS)
    for horizon, result in value.items():
        if result is None:
            continue
        if (
            not isinstance(result, dict)
            or set(result) != expected_fields
            or any(isinstance(item, bool) or not isinstance(item, int) for item in result.values())
        ):
            raise OutcomeInputError(f"{label} forward result {horizon} drift")


def _slice_signals(
    result: Mapping[str, object],
    *,
    slice_index: int,
    requested_dates: tuple[date, ...],
) -> tuple[ReplaySignal, ...]:
    support_count = _integer(result.get("support_count"), label="p5 support_count")
    occurrences = result.get("occurrences")
    if not isinstance(occurrences, list) or len(occurrences) != support_count:
        raise OutcomeInputError(f"Discovery slice {slice_index} p5 support evidence drift")
    requested = set(requested_dates)
    signals: list[ReplaySignal] = []
    prior_order: tuple[date, int] | None = None
    counts: Counter[str] = Counter()
    supported_dates: set[date] = set()
    for occurrence_index, occurrence in enumerate(occurrences):
        label = f"Discovery slice {slice_index} occurrence {occurrence_index}"
        if not isinstance(occurrence, dict) or set(occurrence) != _OCCURRENCE_FIELDS:
            raise OutcomeInputError(f"{label} schema drift")
        source_date = _date(occurrence.get("source_date"), label=f"{label} source_date")
        bucket_end_ns = _integer(
            occurrence.get("bucket_end_ns"), label=f"{label} bucket_end_ns", minimum=1
        )
        if source_date not in requested:
            raise OutcomeInputError(f"{label} source date is outside its five-date slice")
        order = (source_date, bucket_end_ns)
        if prior_order is not None and order <= prior_order:
            raise OutcomeInputError(f"Discovery slice {slice_index} occurrences are not ordered")
        prior_order = order
        direction_text = occurrence.get("direction")
        try:
            direction = Direction(direction_text)
        except (TypeError, ValueError) as error:
            raise OutcomeInputError(f"{label} direction must be LONG or SHORT") from error
        variables = occurrence.get("variables")
        if not isinstance(variables, dict) or set(variables) != set(DISCOVERY_VARIABLE_FIELDS):
            raise OutcomeInputError(f"{label} variable schema drift")
        if any(not isinstance(value, _JSON_SCALAR_TYPES) for value in variables.values()):
            raise OutcomeInputError(f"{label} variables must be non-float JSON scalars")
        contract = _text(variables.get("contract"), label=f"{label} variables.contract")
        try:
            resolve_6e_contract_month(contract, source_date=source_date)
        except ContractSelectionError as error:
            raise OutcomeInputError(f"{label} has an invalid 6E contract") from error
        _validate_forward(occurrence.get("forward"), label=label)
        signal_id = f"{P5_QUERY_ID}:slice={slice_index:02d}:occurrence={occurrence_index:06d}"
        signals.append(
            ReplaySignal(
                signal_id=signal_id,
                slice_index=slice_index,
                occurrence_index=occurrence_index,
                source_date=source_date,
                bucket_end_ns=bucket_end_ns,
                direction=direction,
                contract=contract,
                variables=tuple((field, variables[field]) for field in DISCOVERY_VARIABLE_FIELDS),
            )
        )
        counts[direction.value] += 1
        supported_dates.add(source_date)
    if result.get("direction_counts") != {
        "LONG": counts["LONG"],
        "SHORT": counts["SHORT"],
    }:
        raise OutcomeInputError(f"Discovery slice {slice_index} p5 direction counts drift")
    if result.get("source_date_count") != len(supported_dates):
        raise OutcomeInputError(f"Discovery slice {slice_index} p5 source-date count drift")
    return tuple(signals)


def load_p5_discovery_inputs(
    artifacts: Sequence[CanonicalDiscoveryArtifact],
) -> P5DiscoveryInputs:
    """Verify the exact 99 canonical Discovery artifacts and extract 1,111 signals."""

    if isinstance(artifacts, (str, bytes)) or not isinstance(artifacts, Sequence):
        raise OutcomeInputError("artifacts must be an ordered sequence")
    if any(not isinstance(artifact, CanonicalDiscoveryArtifact) for artifact in artifacts):
        raise OutcomeInputError("every artifact must be a CanonicalDiscoveryArtifact")
    ordered = tuple(sorted(artifacts, key=lambda artifact: artifact.slice_index))
    if tuple(artifact.slice_index for artifact in ordered) != EXPECTED_SLICE_INDICES:
        raise OutcomeInputError("canonical Discovery artifacts must cover slices 0 through 98")
    if len({artifact.sha256 for artifact in ordered}) != len(ordered):
        raise OutcomeInputError("canonical Discovery artifacts repeat a content identity")
    signals: list[ReplaySignal] = []
    prior_requested_date: date | None = None
    prior_signal_order: tuple[date, int] | None = None
    for descriptor in ordered:
        payload = _read_verified_artifact(descriptor)
        document = _json_document(payload, descriptor=descriptor)
        requested_dates = _requested_dates(document, slice_index=descriptor.slice_index)
        if prior_requested_date is not None and requested_dates[0] <= prior_requested_date:
            raise OutcomeInputError("Discovery slice source-date ranges overlap or regress")
        prior_requested_date = requested_dates[-1]
        target = _target_query(document, slice_index=descriptor.slice_index)
        for signal in _slice_signals(
            target,
            slice_index=descriptor.slice_index,
            requested_dates=requested_dates,
        ):
            order = (signal.source_date, signal.bucket_end_ns)
            if prior_signal_order is not None and order <= prior_signal_order:
                raise OutcomeInputError("p5 signals are duplicated or globally out of order")
            prior_signal_order = order
            signals.append(signal)
    direction_counts = Counter(signal.direction for signal in signals)
    if len(signals) != EXPECTED_SIGNAL_COUNT or direction_counts != Counter(
        {
            Direction.LONG: EXPECTED_LONG_SIGNAL_COUNT,
            Direction.SHORT: EXPECTED_SHORT_SIGNAL_COUNT,
        }
    ):
        raise OutcomeInputError("frozen p5 totals must equal 1,111 signals (LONG 529, SHORT 582)")
    return P5DiscoveryInputs(artifacts=ordered, signals=tuple(signals))


@dataclass(frozen=True, slots=True)
class DailyReplayPartition:
    """One cache request plus its per-contract active-session position.

    ``terminal`` marks the nominal final eligible source-date partition in the
    frozen raw-cache request plan.  It is not executable terminal authority:
    after all cache reports exist, :func:`resolve_terminal_partitions` scans
    each contract in reverse and may move the effective terminal to an earlier
    partition whose report actually contains an executable quote.
    """

    cache_spec: DailyCacheSpec
    source_relative_uri: str
    session_ordinal: int
    contract_expiry_month: date
    terminal: bool

    @property
    def key(self) -> tuple[date, str]:
        return self.cache_spec.semantic_key

    def as_dict(self) -> dict[str, object]:
        return {
            "contract_expiry_month": self.contract_expiry_month.isoformat(),
            "event_index_offset": self.cache_spec.event_index_offset,
            "raw_symbol": self.cache_spec.raw_symbol,
            "session_ordinal": self.session_ordinal,
            "source_date": self.cache_spec.source_date.isoformat(),
            "source_relative_uri": self.source_relative_uri,
            "source_sha256": self.cache_spec.source_sha256,
            "terminal": self.terminal,
        }


@dataclass(frozen=True, slots=True)
class OutcomeInputPlan:
    """Portable lineage and executable daily-cache requests for one replay."""

    discovery_input_manifest_sha256: str
    footer_manifest_sha256: str
    source_hash_manifest_sha256: str
    source_record_manifest_sha256: str
    calendar_sha256: str
    partitions: tuple[DailyReplayPartition, ...]

    @property
    def cache_specs(self) -> tuple[DailyCacheSpec, ...]:
        return tuple(partition.cache_spec for partition in self.partitions)

    @property
    def session_ordinal_by_key(self) -> dict[tuple[date, str], int]:
        """Metadata used to construct each cache row's SharedExecutableQuote."""

        return {partition.key: partition.session_ordinal for partition in self.partitions}

    @property
    def cache_plan_sha256(self) -> str:
        return canonical_sha256([partition.as_dict() for partition in self.partitions])

    @property
    def plan_sha256(self) -> str:
        return canonical_sha256(
            {
                "cache_plan_sha256": self.cache_plan_sha256,
                "calendar_sha256": self.calendar_sha256,
                "discovery_input_manifest_sha256": self.discovery_input_manifest_sha256,
                "footer_manifest_sha256": self.footer_manifest_sha256,
                "source_hash_manifest_sha256": self.source_hash_manifest_sha256,
                "source_record_manifest_sha256": self.source_record_manifest_sha256,
            }
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "cache_plan_sha256": self.cache_plan_sha256,
            "calendar_sha256": self.calendar_sha256,
            "discovery_input_manifest_sha256": self.discovery_input_manifest_sha256,
            "first_touch_observation_policy": {
                "active_sessions": FIRST_TOUCH_ACTIVE_SESSIONS,
                "portfolio_position_continues_after_censor": True,
            },
            "footer_manifest_sha256": self.footer_manifest_sha256,
            "partitions": [partition.as_dict() for partition in self.partitions],
            "plan_sha256": self.plan_sha256,
            "source_hash_manifest_sha256": self.source_hash_manifest_sha256,
            "source_record_manifest_sha256": self.source_record_manifest_sha256,
        }


@dataclass(frozen=True, slots=True)
class ContractTerminalResolution:
    """The cache-report-proven terminal quote selected for one contract."""

    contract_key: str
    eligible_partition_count: int
    terminal_source_date: date
    terminal_event_index: int
    terminal_ts_recv_ns: int
    trailing_non_executable_partition_count: int

    def __post_init__(self) -> None:
        _text(self.contract_key, label="terminal contract_key")
        _integer(
            self.eligible_partition_count,
            label="terminal eligible_partition_count",
            minimum=1,
        )
        _date(self.terminal_source_date, label="terminal_source_date")
        _integer(self.terminal_event_index, label="terminal_event_index")
        _integer(self.terminal_ts_recv_ns, label="terminal_ts_recv_ns", minimum=1)
        trailing = _integer(
            self.trailing_non_executable_partition_count,
            label="terminal trailing_non_executable_partition_count",
        )
        if trailing >= self.eligible_partition_count:
            raise OutcomeInputError("terminal fallback cannot skip every eligible partition")

    @property
    def terminal_key(self) -> tuple[date, str]:
        return self.terminal_source_date, self.contract_key

    def as_dict(self) -> dict[str, object]:
        return {
            "contract_key": self.contract_key,
            "eligible_partition_count": self.eligible_partition_count,
            "terminal_event_index": self.terminal_event_index,
            "terminal_source_date": self.terminal_source_date.isoformat(),
            "terminal_ts_recv_ns": self.terminal_ts_recv_ns,
            "trailing_non_executable_partition_count": (
                self.trailing_non_executable_partition_count
            ),
        }


@dataclass(frozen=True, slots=True)
class TerminalResolution:
    """Deterministic contract-terminal decisions derived from cache reports."""

    contracts: tuple[ContractTerminalResolution, ...]
    terminal_exit_policy: str = TERMINAL_EXIT_POLICY
    partition_resolution_policy: str = TERMINAL_PARTITION_RESOLUTION_POLICY

    def __post_init__(self) -> None:
        if self.terminal_exit_policy != TERMINAL_EXIT_POLICY:
            raise OutcomeInputError("terminal exit policy drift")
        if self.partition_resolution_policy != TERMINAL_PARTITION_RESOLUTION_POLICY:
            raise OutcomeInputError("terminal partition-resolution policy drift")
        if not isinstance(self.contracts, tuple) or not self.contracts:
            raise OutcomeInputError("terminal resolution requires at least one contract")
        contract_keys = tuple(item.contract_key for item in self.contracts)
        if contract_keys != tuple(sorted(set(contract_keys))):
            raise OutcomeInputError("terminal resolutions must be uniquely ordered by contract")

    @property
    def terminal_key_by_contract(self) -> dict[str, tuple[date, str]]:
        return {item.contract_key: item.terminal_key for item in self.contracts}

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())

    def as_dict(self) -> dict[str, object]:
        return {
            "contracts": [item.as_dict() for item in self.contracts],
            "partition_resolution_policy": self.partition_resolution_policy,
            "terminal_exit_policy": self.terminal_exit_policy,
        }


def resolve_terminal_partitions(
    plan: OutcomeInputPlan,
    reports: Sequence[DailyCacheReport],
) -> TerminalResolution:
    """Resolve each contract's terminal by reverse-scanning verified reports.

    The nominal final calendar partition is never assumed executable.  A
    contract falls back across any trailing partitions with zero valid quotes;
    no valid quote anywhere before the expiry-month boundary is a hard failure.
    """

    if not isinstance(plan, OutcomeInputPlan) or not plan.partitions:
        raise OutcomeInputError("terminal resolution requires a non-empty input plan")
    if isinstance(reports, (str, bytes)) or not isinstance(reports, Sequence):
        raise OutcomeInputError("terminal resolution reports must be an ordered sequence")
    values = tuple(reports)
    if len(values) != len(plan.partitions):
        raise OutcomeInputError("terminal resolution report cardinality drift")
    grouped: dict[str, list[tuple[DailyReplayPartition, DailyCacheReport]]] = {}
    for partition, report in zip(plan.partitions, values, strict=True):
        if not isinstance(report, DailyCacheReport):
            raise OutcomeInputError("terminal resolution requires DailyCacheReport values")
        if (
            (report.source_date, report.raw_symbol) != partition.key
            or report.source_sha256 != partition.cache_spec.source_sha256
            or report.event_index_offset != partition.cache_spec.event_index_offset
            or isinstance(report.cached_quote_count, bool)
            or not isinstance(report.cached_quote_count, int)
            or report.cached_quote_count <= 0
            or isinstance(report.valid_quote_count, bool)
            or not isinstance(report.valid_quote_count, int)
            or not 0 <= report.valid_quote_count <= report.cached_quote_count
        ):
            raise OutcomeInputError("terminal resolution cache-report lineage drift")
        has_event = report.last_valid_event_index is not None
        has_time = report.last_valid_ts_recv_ns is not None
        if has_event != has_time or (report.valid_quote_count > 0) != has_event:
            raise OutcomeInputError("terminal resolution valid-quote metadata drift")
        if has_event:
            assert report.last_valid_event_index is not None
            assert report.last_valid_ts_recv_ns is not None
            if (
                not report.first_event_index
                <= report.last_valid_event_index
                <= report.last_event_index
                or not report.first_ts_recv_ns
                <= report.last_valid_ts_recv_ns
                <= report.last_ts_recv_ns
            ):
                raise OutcomeInputError("terminal resolution last-valid bounds drift")
        grouped.setdefault(partition.cache_spec.raw_symbol, []).append((partition, report))

    resolutions: list[ContractTerminalResolution] = []
    for contract_key in sorted(grouped):
        candidates = grouped[contract_key]
        source_dates = tuple(partition.key[0] for partition, _ in candidates)
        ordinals = tuple(partition.session_ordinal for partition, _ in candidates)
        expiry_months = {partition.contract_expiry_month for partition, _ in candidates}
        if (
            source_dates != tuple(sorted(set(source_dates)))
            or ordinals != tuple(range(len(candidates)))
            or len(expiry_months) != 1
            or any(source_date >= next(iter(expiry_months)) for source_date in source_dates)
        ):
            raise OutcomeInputError(f"terminal candidate plan drift for {contract_key}")
        selected: tuple[DailyReplayPartition, DailyCacheReport] | None = None
        trailing = 0
        for candidate in reversed(candidates):
            if candidate[1].valid_quote_count > 0:
                selected = candidate
                break
            trailing += 1
        if selected is None:
            raise OutcomeInputError(
                f"contract {contract_key} has no executable quote before expiry month"
            )
        partition, report = selected
        assert report.last_valid_event_index is not None
        assert report.last_valid_ts_recv_ns is not None
        resolutions.append(
            ContractTerminalResolution(
                contract_key=contract_key,
                eligible_partition_count=len(candidates),
                terminal_source_date=partition.key[0],
                terminal_event_index=report.last_valid_event_index,
                terminal_ts_recv_ns=report.last_valid_ts_recv_ns,
                trailing_non_executable_partition_count=trailing,
            )
        )
    return TerminalResolution(contracts=tuple(resolutions))


def apply_terminal_resolution(
    plan: OutcomeInputPlan,
    resolution: TerminalResolution,
) -> tuple[DailyReplayPartition, ...]:
    """Return execution partitions with exactly the resolved terminals marked."""

    if not isinstance(plan, OutcomeInputPlan) or not isinstance(resolution, TerminalResolution):
        raise OutcomeInputError("terminal application requires a plan and resolution")
    terminal_keys = set(resolution.terminal_key_by_contract.values())
    planned_contracts = {partition.cache_spec.raw_symbol for partition in plan.partitions}
    if set(resolution.terminal_key_by_contract) != planned_contracts:
        raise OutcomeInputError("terminal resolution contract set differs from input plan")
    result = tuple(
        replace(partition, terminal=partition.key in terminal_keys) for partition in plan.partitions
    )
    if {partition.key for partition in result if partition.terminal} != terminal_keys:
        raise OutcomeInputError("terminal resolution key is outside the input plan")
    return result


def _source_root(value: Path | str) -> Path:
    requested = Path(value).expanduser()
    if requested.is_symlink():
        raise OutcomeInputError("mbp10_root cannot be a symbolic link")
    try:
        root = requested.resolve(strict=True)
    except OSError as error:
        raise OutcomeInputError("mbp10_root does not exist") from error
    if not root.is_dir():
        raise OutcomeInputError("mbp10_root must be a directory")
    return root


def _relative_uri(value: object) -> PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value:
        raise OutcomeInputError("source relative_uri is invalid")
    parsed = PurePosixPath(value)
    if (
        parsed.is_absolute()
        or parsed.as_posix() != value
        or any(part in {"", ".", ".."} for part in parsed.parts)
    ):
        raise OutcomeInputError("source relative_uri is unsafe")
    return parsed


def _source_path(root: Path, record: SourceFileRegistration) -> Path:
    relative = _relative_uri(record.relative_uri)
    current = root
    for part in relative.parts:
        current = current / part
        try:
            mode = current.lstat().st_mode
        except OSError as error:
            raise OutcomeInputError(f"planned source path does not exist: {current}") from error
        if stat.S_ISLNK(mode):
            raise OutcomeInputError(f"planned source path contains a symbolic link: {current}")
    if not stat.S_ISREG(current.lstat().st_mode):
        raise OutcomeInputError(f"planned source path is not a regular file: {current}")
    if current.stat().st_size != record.byte_size:
        raise OutcomeInputError(f"planned source byte-size drift: {current}")
    resolved = current.resolve(strict=True)
    if not resolved.is_relative_to(root):
        raise OutcomeInputError("planned source path escapes mbp10_root")
    return resolved


def _manifest_records(
    bundle: SourceManifestBundle,
) -> tuple[tuple[SourceFileRegistration, int], ...]:
    if not isinstance(bundle, SourceManifestBundle) or not bundle.records:
        raise OutcomeInputError("source_manifest must be a non-empty SourceManifestBundle")
    _sha256(bundle.footer_manifest_sha256, label="footer_manifest_sha256")
    _sha256(bundle.hash_manifest_sha256, label="hash_manifest_sha256")
    rows: list[tuple[SourceFileRegistration, int]] = []
    offset = 0
    prior_date: date | None = None
    prior_uri: str | None = None
    for index, record in enumerate(bundle.records):
        source_date = _date(record.source_date, label=f"source record {index} date")
        _sha256(record.sha256, label=f"source record {index} sha256")
        row_count = _integer(record.row_count, label=f"source record {index} row_count")
        _integer(record.byte_size, label=f"source record {index} byte_size", minimum=1)
        _relative_uri(record.relative_uri)
        if prior_date is not None and source_date <= prior_date:
            raise OutcomeInputError("source manifest dates must be strictly increasing")
        if prior_uri is not None and record.relative_uri <= prior_uri:
            raise OutcomeInputError("source manifest relative URIs must be strictly increasing")
        rows.append((record, offset))
        offset += row_count
        prior_date = source_date
        prior_uri = record.relative_uri
    if bundle.total_source_bytes != sum(record.byte_size for record, _ in rows):
        raise OutcomeInputError("source manifest total_source_bytes drift")
    if (
        bundle.first_source_date != rows[0][0].source_date
        or bundle.last_source_date != rows[-1][0].source_date
    ):
        raise OutcomeInputError("source manifest date bounds drift")
    return tuple(rows)


def plan_p5_replay_inputs(
    discovery: P5DiscoveryInputs,
    *,
    source_manifest: SourceManifestBundle,
    mbp10_root: Path | str,
    calendar_source_dates: Sequence[date],
) -> OutcomeInputPlan:
    """Plan every unique ``(date, contract)`` cache needed through expiry.

    The first-touch label remains the shared scanner's 20-active-session policy.
    This planner intentionally continues each contract to expiry so an occupied
    portfolio cell remains unavailable after label censoring until its actual
    barrier or mandatory terminal exit releases it.
    """

    if not isinstance(discovery, P5DiscoveryInputs) or not discovery.signals:
        raise OutcomeInputError("discovery must contain at least one p5 signal")
    calendar = _strict_dates(calendar_source_dates, label="calendar_source_dates")
    calendar_set = set(calendar)
    if any(signal.source_date not in calendar_set for signal in discovery.signals):
        raise OutcomeInputError("every p5 signal date must belong to the canonical calendar")
    root = _source_root(mbp10_root)
    manifest_rows = _manifest_records(source_manifest)
    records_by_date = {record.source_date: (record, offset) for record, offset in manifest_rows}
    missing_calendar = [day for day in calendar if day not in records_by_date]
    if missing_calendar:
        raise OutcomeInputError(
            f"canonical calendar date has no source manifest record: {missing_calendar[0]}"
        )

    source_record_document = [
        {
            "byte_size": record.byte_size,
            "event_index_offset": offset,
            "relative_uri": record.relative_uri,
            "row_count": record.row_count,
            "sha256": record.sha256,
            "source_date": record.source_date.isoformat(),
        }
        for record, offset in manifest_rows
    ]
    first_signal_by_contract: dict[str, date] = {}
    expiry_by_contract: dict[str, date] = {}
    for signal in discovery.signals:
        first_signal_by_contract[signal.contract] = min(
            signal.source_date,
            first_signal_by_contract.get(signal.contract, signal.source_date),
        )
    for contract, first_signal_date in first_signal_by_contract.items():
        try:
            expiry = resolve_6e_contract_month(contract, source_date=first_signal_date)
        except ContractSelectionError as error:
            raise OutcomeInputError(f"cannot resolve expiry for {contract}") from error
        if any(
            signal.source_date >= expiry
            for signal in discovery.signals
            if signal.contract == contract
        ):
            raise OutcomeInputError(f"contract {contract} has a signal in or after expiry month")
        if calendar[-1] < expiry:
            raise OutcomeInputError(
                f"canonical calendar does not reach expiry month for contract {contract}"
            )
        expiry_by_contract[contract] = expiry

    partitions_by_contract: dict[str, list[DailyReplayPartition]] = {}
    for contract in sorted(first_signal_by_contract):
        first_signal_date = first_signal_by_contract[contract]
        expiry = expiry_by_contract[contract]
        eligible = tuple(day for day in calendar if first_signal_date <= day < expiry)
        if not eligible:
            raise OutcomeInputError(f"contract {contract} has no eligible cache sessions")
        contract_partitions: list[DailyReplayPartition] = []
        for ordinal, source_date in enumerate(eligible):
            record, offset = records_by_date[source_date]
            path = _source_path(root, record)
            contract_partitions.append(
                DailyReplayPartition(
                    cache_spec=DailyCacheSpec(
                        source_date=source_date,
                        source_parquet_path=path,
                        source_sha256=record.sha256,
                        raw_symbol=contract,
                        event_index_offset=offset,
                    ),
                    source_relative_uri=record.relative_uri,
                    session_ordinal=ordinal,
                    contract_expiry_month=expiry,
                    terminal=source_date == eligible[-1],
                )
            )
        partitions_by_contract[contract] = contract_partitions

    partitions = tuple(
        sorted(
            (
                partition
                for contract_partitions in partitions_by_contract.values()
                for partition in contract_partitions
            ),
            key=lambda partition: partition.key,
        )
    )
    keys = tuple(partition.key for partition in partitions)
    if len(keys) != len(set(keys)):
        raise OutcomeInputError("cache plan repeats a (source_date, raw_symbol) key")
    for contract, contract_partitions in partitions_by_contract.items():
        ordinals = tuple(partition.session_ordinal for partition in contract_partitions)
        if ordinals != tuple(range(len(contract_partitions))):
            raise OutcomeInputError(f"session ordinals are not monotonic for {contract}")
        if sum(partition.terminal for partition in contract_partitions) != 1:
            raise OutcomeInputError(f"contract {contract} requires exactly one terminal partition")

    return OutcomeInputPlan(
        discovery_input_manifest_sha256=discovery.input_manifest_sha256,
        footer_manifest_sha256=source_manifest.footer_manifest_sha256,
        source_hash_manifest_sha256=source_manifest.hash_manifest_sha256,
        source_record_manifest_sha256=canonical_sha256(source_record_document),
        calendar_sha256=canonical_sha256([day.isoformat() for day in calendar]),
        partitions=partitions,
    )
