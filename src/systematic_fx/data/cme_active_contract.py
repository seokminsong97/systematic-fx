"""Immutable previous-session-volume evidence for CME active contracts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tomllib
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path, PurePosixPath

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from systematic_fx.data.cme_schedule import CmeScheduleArchive, CmeScheduleEvidenceError
from systematic_fx.data.contracts import validate_mbp10_contract
from systematic_fx.data.instruments import InstrumentKind, parse_instrument_mappings

_SYMBOL = re.compile(r"^6E[FGHJKMNQUVXZ][0-9]{1,2}$")
_BOUNDED_WEEKDAY_FALLBACK_MANIFEST_SHA256 = (
    "0df60badcfc0f191f22ca26b2d0ee6a439cb6c90b715580398577af8bcfc5b82"
)


class ActiveContractEvidenceError(ValueError):
    """Active-contract evidence is incomplete, mutated, or not point-in-time."""


@dataclass(frozen=True, slots=True)
class VolumeSource:
    source_date: date
    relative_uri: str
    sha256: str
    role: str

    def as_dict(self) -> dict[str, object]:
        return {
            "relative_uri": self.relative_uri,
            "role": self.role,
            "sha256": self.sha256,
            "source_date": self.source_date.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class SessionVolume:
    raw_symbol: str
    instrument_id: int
    trade_rows: int
    trade_volume: int

    def as_dict(self) -> dict[str, object]:
        return {
            "instrument_id": self.instrument_id,
            "raw_symbol": self.raw_symbol,
            "trade_rows": self.trade_rows,
            "trade_volume": self.trade_volume,
        }


@dataclass(frozen=True, slots=True)
class ActiveContractMappingSpec:
    trading_date: date
    evidence_trading_date: date
    evidence_open_ts_ns: int
    evidence_close_ts_ns: int
    selection_available_ts_ns: int
    target_session_open_ts_ns: int
    evidence_source_dates: tuple[date, ...]
    expected: tuple[SessionVolume, ...]

    def __post_init__(self) -> None:
        if self.evidence_trading_date >= self.trading_date:
            raise ActiveContractEvidenceError("volume evidence must predate the target session")
        if not (
            self.evidence_open_ts_ns
            < self.evidence_close_ts_ns
            == self.selection_available_ts_ns
            <= self.target_session_open_ts_ns
        ):
            raise ActiveContractEvidenceError(
                "selection must become available only after the evidence session closes"
            )
        if len(self.expected) < 2:
            raise ActiveContractEvidenceError("at least two outright contracts must be compared")
        if len({item.raw_symbol for item in self.expected}) != len(self.expected):
            raise ActiveContractEvidenceError("active-contract symbols must be unique")

    def as_dict(self) -> dict[str, object]:
        return {
            "candidates": [item.as_dict() for item in self.expected],
            "evidence_close_ts_ns": self.evidence_close_ts_ns,
            "evidence_open_ts_ns": self.evidence_open_ts_ns,
            "evidence_source_dates": [item.isoformat() for item in self.evidence_source_dates],
            "evidence_trading_date": self.evidence_trading_date.isoformat(),
            "selection_available_ts_ns": self.selection_available_ts_ns,
            "target_session_open_ts_ns": self.target_session_open_ts_ns,
            "trading_date": self.trading_date.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class ActiveContractVolumeManifest:
    version: str
    sha256: str
    semantic_sha256: str
    source_schema: str
    policy_version: str
    schedule_verification_mode: str
    schedule_archive_sha256: str | None
    schedule_source_sha256: str | None
    sources: tuple[VolumeSource, ...]
    mappings: tuple[ActiveContractMappingSpec, ...]

    def semantic_payload(self) -> dict[str, object]:
        return {
            "artifact_schema": "systematic_fx.cme_active_contract_volume_manifest.v1",
            "mappings": [item.as_dict() for item in self.mappings],
            "policy_version": self.policy_version,
            "source_schema": self.source_schema,
            "sources": [item.as_dict() for item in self.sources],
            "version": self.version,
        }


@dataclass(frozen=True, slots=True)
class MaterializedActiveContractMapping:
    trading_date: date
    evidence_trading_date: date
    selection_available_ts_ns: int
    selected: SessionVolume
    candidates: tuple[SessionVolume, ...]
    evidence_manifest_sha256: str
    policy_version: str

    def as_dict(self) -> dict[str, object]:
        return {
            "candidates": [item.as_dict() for item in self.candidates],
            "evidence_manifest_sha256": self.evidence_manifest_sha256,
            "evidence_trading_date": self.evidence_trading_date.isoformat(),
            "policy_version": self.policy_version,
            "selected": self.selected.as_dict(),
            "selection_available_ts_ns": self.selection_available_ts_ns,
            "trading_date": self.trading_date.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class ActiveContractMappingArtifact:
    manifest_file_sha256: str
    manifest_semantic_sha256: str
    policy_version: str
    schedule_verification_mode: str
    schedule_archive_sha256: str | None
    schedule_source_sha256: str | None
    mappings: tuple[MaterializedActiveContractMapping, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": "systematic_fx.cme_active_contract_mapping_artifact.v1",
            "manifest_file_sha256": self.manifest_file_sha256,
            "manifest_semantic_sha256": self.manifest_semantic_sha256,
            "mappings": [item.as_dict() for item in self.mappings],
            "policy_version": self.policy_version,
            "schedule_archive_sha256": self.schedule_archive_sha256,
            "schedule_source_sha256": self.schedule_source_sha256,
            "schedule_verification_mode": self.schedule_verification_mode,
        }

    @property
    def content_sha256(self) -> str:
        return _canonical_sha256(self.as_dict())


def active_contract_mapping_as_of(
    mappings: tuple[MaterializedActiveContractMapping, ...],
    *,
    trading_date: date,
    as_of_ts_ns: int,
) -> MaterializedActiveContractMapping:
    """Expose a mapping only after the prior session has fully closed."""

    matches = tuple(item for item in mappings if item.trading_date == trading_date)
    if len(matches) != 1:
        raise ActiveContractEvidenceError("one exact target-date mapping is required")
    mapping = matches[0]
    if as_of_ts_ns < mapping.selection_available_ts_ns:
        raise ActiveContractEvidenceError("active-contract mapping was not yet observable")
    return mapping


def _canonical_sha256(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _require_exact_keys(value: object, expected: set[str], *, label: str) -> None:
    if not isinstance(value, dict) or set(value) != expected:
        raise ActiveContractEvidenceError(f"{label} keys differ from the frozen schema")


def _sha(value: object, *, label: str) -> str:
    result = str(value)
    if len(result) != 64 or any(c not in "0123456789abcdef" for c in result):
        raise ActiveContractEvidenceError(f"{label} must be a lowercase SHA-256")
    return result


def _relative_uri(value: object) -> str:
    text = str(value)
    path = PurePosixPath(text)
    if (
        not text
        or path.is_absolute()
        or path.as_posix() != text
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ActiveContractEvidenceError("volume source URI must be bounded and relative")
    return text


def _reject_unsafe_path(path: Path, *, label: str) -> None:
    unsafe = ("holdout", "sealed", "credential", "forward")
    if any(any(token in part.casefold() for token in unsafe) for part in path.parts):
        raise ActiveContractEvidenceError(f"{label} cannot name protected storage")
    absolute = path if path.is_absolute() else Path.cwd() / path
    cursor = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        cursor /= part
        if cursor.is_symlink():
            raise ActiveContractEvidenceError(f"{label} cannot traverse a symbolic link")


def _previous_weekday(value: date) -> date:
    candidate = value - timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate


def _intersecting_utc_dates(open_ts_ns: int, close_ts_ns: int) -> tuple[date, ...]:
    first = datetime.fromtimestamp(open_ts_ns // 1_000_000_000, tz=UTC).date()
    last = datetime.fromtimestamp((close_ts_ns - 1) // 1_000_000_000, tz=UTC).date()
    values: list[date] = []
    cursor = first
    while cursor <= last:
        values.append(cursor)
        cursor += timedelta(days=1)
    return tuple(values)


def load_active_contract_volume_manifest(
    path: str | Path,
    *,
    schedule_archive: CmeScheduleArchive | None = None,
    allow_bounded_weekday_fallback: bool = False,
) -> ActiveContractVolumeManifest:
    """Load an exact source allowlist and precommitted session totals."""

    requested = Path(path).expanduser()
    if ".." in requested.parts:
        raise ActiveContractEvidenceError("volume manifest cannot contain traversal")
    _reject_unsafe_path(requested, label="volume manifest")
    if not requested.is_file():
        raise ActiveContractEvidenceError("volume manifest must be a regular non-symlink file")
    raw = requested.read_bytes()
    try:
        document = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise ActiveContractEvidenceError("volume manifest must be valid UTF-8 TOML") from error
    _require_exact_keys(document, {"manifest", "sources", "mappings"}, label="manifest")
    head = document["manifest"]
    _require_exact_keys(
        head,
        {"schema", "version", "source_schema", "venue", "product_root", "policy_version"},
        label="manifest header",
    )
    if head["schema"] != "systematic_fx.cme_active_contract_volume.v1":
        raise ActiveContractEvidenceError("unsupported active-contract manifest schema")
    if head["venue"] != "CME_GLOBEX" or head["product_root"] != "6E":
        raise ActiveContractEvidenceError("active-contract manifest is not CME Globex 6E")
    if head["source_schema"] != "GLBX.MDP3/mbp-10":
        raise ActiveContractEvidenceError("active-contract source schema must be MBP-10")
    if head["policy_version"] != "previous_completed_trading_date_volume_v1":
        raise ActiveContractEvidenceError("unsupported active-contract policy")
    sources: list[VolumeSource] = []
    for item in document["sources"]:
        _require_exact_keys(
            item, {"source_date", "relative_uri", "sha256", "role"}, label="volume source"
        )
        if item["role"] not in {"EVIDENCE", "EVIDENCE_AND_TARGET", "TARGET_CONTEXT"}:
            raise ActiveContractEvidenceError("unsupported volume source role")
        sources.append(
            VolumeSource(
                source_date=item["source_date"],
                relative_uri=_relative_uri(item["relative_uri"]),
                sha256=_sha(item["sha256"], label="source sha256"),
                role=str(item["role"]),
            )
        )
    if tuple(item.source_date for item in sources) != tuple(
        sorted({item.source_date for item in sources})
    ):
        raise ActiveContractEvidenceError("volume sources must have unique increasing dates")
    source_dates = {item.source_date for item in sources}
    source_role_by_date = {item.source_date: item.role for item in sources}
    mappings: list[ActiveContractMappingSpec] = []
    for item in document["mappings"]:
        _require_exact_keys(
            item,
            {
                "trading_date",
                "evidence_trading_date",
                "evidence_open_ts_ns",
                "evidence_close_ts_ns",
                "selection_available_ts_ns",
                "target_session_open_ts_ns",
                "evidence_source_dates",
                "candidates",
            },
            label="mapping",
        )
        evidence_sources = tuple(item["evidence_source_dates"])
        if not evidence_sources or not set(evidence_sources) <= source_dates:
            raise ActiveContractEvidenceError("mapping references a non-allowlisted source")
        candidates: list[SessionVolume] = []
        for candidate in item["candidates"]:
            _require_exact_keys(
                candidate,
                {"raw_symbol", "instrument_id", "trade_rows", "trade_volume"},
                label="mapping candidate",
            )
            symbol = str(candidate["raw_symbol"])
            values = (
                int(candidate["instrument_id"]),
                int(candidate["trade_rows"]),
                int(candidate["trade_volume"]),
            )
            if not _SYMBOL.fullmatch(symbol) or values[0] <= 0 or min(values[1:]) < 0:
                raise ActiveContractEvidenceError("invalid mapping candidate")
            candidates.append(SessionVolume(symbol, *values))
        mapping = ActiveContractMappingSpec(
            trading_date=item["trading_date"],
            evidence_trading_date=item["evidence_trading_date"],
            evidence_open_ts_ns=int(item["evidence_open_ts_ns"]),
            evidence_close_ts_ns=int(item["evidence_close_ts_ns"]),
            selection_available_ts_ns=int(item["selection_available_ts_ns"]),
            target_session_open_ts_ns=int(item["target_session_open_ts_ns"]),
            evidence_source_dates=evidence_sources,
            expected=tuple(candidates),
        )
        if schedule_archive is None:
            if not allow_bounded_weekday_fallback:
                raise ActiveContractEvidenceError(
                    "active-contract selection requires a verified schedule archive"
                )
            if mapping.evidence_trading_date != _previous_weekday(mapping.trading_date):
                raise ActiveContractEvidenceError(
                    "holiday-aware predecessor requires a verified schedule archive"
                )
        else:
            try:
                if not schedule_archive.source_bytes_verified:
                    raise CmeScheduleEvidenceError(
                        "schedule archive upstream bytes are not verified"
                    )
                schedule_archive.verify_unchanged()
                target_decision = schedule_archive.session_as_of(
                    mapping.trading_date,
                    mapping.selection_available_ts_ns,
                )
                previous = schedule_archive.previous_completed_session_as_of(
                    mapping.trading_date,
                    as_of_ts_ns=mapping.selection_available_ts_ns,
                )
            except CmeScheduleEvidenceError as error:
                raise ActiveContractEvidenceError(
                    "schedule archive cannot prove the previous completed session"
                ) from error
            if target_decision.session is None:
                raise ActiveContractEvidenceError(
                    "schedule archive cannot prove the target session"
                )
            if (
                mapping.evidence_trading_date != previous.trading_date
                or mapping.evidence_open_ts_ns != previous.open_ts_ns
                or mapping.evidence_close_ts_ns != previous.close_ts_ns
                or mapping.target_session_open_ts_ns != target_decision.session.open_ts_ns
            ):
                raise ActiveContractEvidenceError(
                    "volume evidence timestamps differ from archived sessions"
                )
        if mapping.evidence_source_dates != _intersecting_utc_dates(
            mapping.evidence_open_ts_ns, mapping.evidence_close_ts_ns
        ):
            raise ActiveContractEvidenceError(
                "evidence sources must exactly cover the completed session UTC partitions"
            )
        if any(source_role_by_date[day] == "TARGET_CONTEXT" for day in evidence_sources):
            raise ActiveContractEvidenceError("target-only source cannot influence selection")
        mappings.append(mapping)
    if tuple(item.trading_date for item in mappings) != tuple(
        sorted({item.trading_date for item in mappings})
    ):
        raise ActiveContractEvidenceError("mapping target dates must be unique and increasing")
    manifest_sha256 = hashlib.sha256(raw).hexdigest()
    if (
        schedule_archive is None
        and allow_bounded_weekday_fallback
        and manifest_sha256 != _BOUNDED_WEEKDAY_FALLBACK_MANIFEST_SHA256
    ):
        raise ActiveContractEvidenceError(
            "bounded weekday fallback is restricted to the exact checked-in manifest"
        )
    schedule_verification_mode = (
        "VERIFIED_SCHEDULE_ARCHIVE"
        if schedule_archive is not None
        else "BOUNDED_WEEKDAY_FALLBACK_NOT_CALENDAR_EVIDENCE"
    )
    provisional = ActiveContractVolumeManifest(
        version=str(head["version"]),
        sha256=manifest_sha256,
        semantic_sha256="",
        source_schema=str(head["source_schema"]),
        policy_version=str(head["policy_version"]),
        schedule_verification_mode=schedule_verification_mode,
        schedule_archive_sha256=(None if schedule_archive is None else schedule_archive.sha256),
        schedule_source_sha256=(
            None if schedule_archive is None else schedule_archive.source_sha256
        ),
        sources=tuple(sources),
        mappings=tuple(mappings),
    )
    return ActiveContractVolumeManifest(
        version=provisional.version,
        sha256=provisional.sha256,
        semantic_sha256=_canonical_sha256(provisional.semantic_payload()),
        source_schema=provisional.source_schema,
        policy_version=provisional.policy_version,
        schedule_verification_mode=provisional.schedule_verification_mode,
        schedule_archive_sha256=provisional.schedule_archive_sha256,
        schedule_source_sha256=provisional.schedule_source_sha256,
        sources=provisional.sources,
        mappings=provisional.mappings,
    )


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def materialize_active_contract_mappings(
    manifest: ActiveContractVolumeManifest,
    *,
    data_root: str | Path,
    verify_source_hashes: bool = True,
) -> tuple[MaterializedActiveContractMapping, ...]:
    """Stream exact MBP-10 files and verify prior-session volume winners."""

    requested_root = Path(os.fspath(data_root)).expanduser()
    if ".." in requested_root.parts:
        raise ActiveContractEvidenceError("data root cannot contain traversal")
    _reject_unsafe_path(requested_root, label="data root")
    root = requested_root.resolve(strict=True)
    source_by_date = {item.source_date: item for item in manifest.sources}
    # Only evidence partitions may be opened.  TARGET_CONTEXT exists in the
    # immutable manifest to document the target session, but opening it while
    # selecting that session's contract would unnecessarily expand the
    # point-in-time read boundary (and make the selector depend on future
    # bytes it never uses).
    required_source_dates = {
        source_date
        for mapping in manifest.mappings
        for source_date in mapping.evidence_source_dates
    }
    paths: dict[date, Path] = {}
    for source in manifest.sources:
        if source.source_date not in required_source_dates:
            continue
        requested = root / "mbp-10" / source.relative_uri
        _reject_unsafe_path(requested, label="volume source")
        path = requested.resolve(strict=True)
        if not path.is_relative_to(root / "mbp-10"):
            raise ActiveContractEvidenceError("volume source escaped the MBP-10 root")
        if verify_source_hashes and _file_sha256(path) != source.sha256:
            raise ActiveContractEvidenceError("volume source SHA-256 drifted")
        paths[source.source_date] = path
    outputs: list[MaterializedActiveContractMapping] = []
    for mapping in manifest.mappings:
        totals = {(item.raw_symbol, item.instrument_id): [0, 0] for item in mapping.expected}
        for source_date in mapping.evidence_source_dates:
            source = source_by_date[source_date]
            path = paths[source_date]
            parquet = pq.ParquetFile(path)
            validate_mbp10_contract(parquet.schema_arrow)
            metadata = parquet.schema_arrow.metadata or {}
            mappings = parse_instrument_mappings(metadata[b"dbn.metadata"])
            for symbol, instrument_id in totals:
                if not any(
                    item.raw_symbol == symbol
                    and item.instrument_id == instrument_id
                    and item.kind is InstrumentKind.OUTRIGHT
                    and item.interval_start <= source_date < item.interval_end
                    for item in mappings
                ):
                    raise ActiveContractEvidenceError(
                        "source instrument mapping differs from the candidate allowlist"
                    )
            for row_group in range(parquet.num_row_groups):
                table = parquet.read_row_group(
                    row_group,
                    columns=["ts_recv", "instrument_id", "action", "size"],
                    use_threads=False,
                )
                ts_ns = pc.cast(table["ts_recv"], pa.int64())
                in_session = pc.and_(
                    pc.greater_equal(ts_ns, mapping.evidence_open_ts_ns),
                    pc.less(ts_ns, mapping.evidence_close_ts_ns),
                )
                in_session_table = table.filter(in_session)
                trades = in_session_table.filter(pc.equal(in_session_table["action"], "T"))
                for key, total in totals.items():
                    selected = trades.filter(pc.equal(trades["instrument_id"], key[1]))
                    total[0] += selected.num_rows
                    volume = pc.sum(pc.cast(selected["size"], pa.int64())).as_py()
                    total[1] += 0 if volume is None else int(volume)
        observed = tuple(
            SessionVolume(
                item.raw_symbol, item.instrument_id, *totals[(item.raw_symbol, item.instrument_id)]
            )
            for item in mapping.expected
        )
        if observed != mapping.expected:
            raise ActiveContractEvidenceError("observed previous-session volume totals drifted")
        ranked = tuple(
            sorted(
                observed,
                key=lambda item: (-item.trade_volume, item.raw_symbol, item.instrument_id),
            )
        )
        if ranked[0].trade_volume == ranked[1].trade_volume:
            raise ActiveContractEvidenceError("top previous-session volume is tied")
        outputs.append(
            MaterializedActiveContractMapping(
                trading_date=mapping.trading_date,
                evidence_trading_date=mapping.evidence_trading_date,
                selection_available_ts_ns=mapping.selection_available_ts_ns,
                selected=ranked[0],
                candidates=ranked,
                evidence_manifest_sha256=manifest.sha256,
                policy_version=manifest.policy_version,
            )
        )
    return tuple(outputs)


def materialize_active_contract_mapping_artifact(
    manifest: ActiveContractVolumeManifest,
    *,
    data_root: str | Path,
    verify_source_hashes: bool = True,
) -> ActiveContractMappingArtifact:
    """CLI-ready content-addressed wrapper around exact volume materialization."""

    mappings = materialize_active_contract_mappings(
        manifest,
        data_root=data_root,
        verify_source_hashes=verify_source_hashes,
    )
    return ActiveContractMappingArtifact(
        manifest_file_sha256=manifest.sha256,
        manifest_semantic_sha256=manifest.semantic_sha256,
        policy_version=manifest.policy_version,
        schedule_verification_mode=manifest.schedule_verification_mode,
        schedule_archive_sha256=manifest.schedule_archive_sha256,
        schedule_source_sha256=manifest.schedule_source_sha256,
        mappings=mappings,
    )


def verify_active_contract_mapping_artifact(
    artifact: ActiveContractMappingArtifact,
    manifest: ActiveContractVolumeManifest,
    *,
    data_root: str | Path,
    verify_source_hashes: bool = True,
) -> None:
    """Recompute the bounded mapping and reject any byte or semantic drift."""

    expected = materialize_active_contract_mapping_artifact(
        manifest,
        data_root=data_root,
        verify_source_hashes=verify_source_hashes,
    )
    if artifact != expected or artifact.content_sha256 != expected.content_sha256:
        raise ActiveContractEvidenceError("active-contract mapping artifact drifted")
