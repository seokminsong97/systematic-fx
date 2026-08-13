"""Immutable, bounded shards for the M0b quote-aware first-passage labels.

The real-slice materializer deliberately emits one canonical label JSONL file.
This module turns that exact artifact into restart-friendly shards without
recomputing an outcome or changing row order.  Event groups are never split,
so a downstream sequential worker can checkpoint only at shard boundaries
without observing half of a barrier grid.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.m0b.materialize import _safe_root
from systematic_fx.research.m0b.model import RealSliceBuild, RealSliceError

STORE_SCHEMA: Final = "systematic_fx.m0b_first_passage_store.v1"
SHARD_SCHEMA: Final = "systematic_fx.m0b_first_passage_shard.v1"
_LABEL_SCHEMA: Final = "systematic_fx.m0b_quote_label.v1"
_WRITE_BITS: Final = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
_DIRECTION_RANK: Final = {"LONG": 0, "SHORT": 1}
_MAX_BARRIER_COMPONENT: Final = 1_000_000
_MAX_HOLD_SECONDS: Final = 31_536_000


class FirstPassageStoreError(RealSliceError):
    """A first-passage input, shard, or manifest failed closed."""


def _sha256(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise FirstPassageStoreError(f"{label} must be a lowercase SHA-256")
    return value


def _positive_integer(value: object, *, label: str, allow_zero: bool = False) -> int:
    minimum = 0 if allow_zero else 1
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        qualifier = "non-negative" if allow_zero else "positive"
        raise FirstPassageStoreError(f"{label} must be a {qualifier} integer")
    return value


def _leaf(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value or Path(value).name != value:
        raise FirstPassageStoreError(f"{label} must be a direct relative leaf")
    return value


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(root: Path) -> None:
    descriptor = os.open(root, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish_immutable(root: Path, relative_uri: str, payload: bytes) -> None:
    """Publish once with a hard link; an existing path must have exact bytes."""

    target = root / _leaf(relative_uri, label="artifact URI")
    expected_sha256 = hashlib.sha256(payload).hexdigest()
    if target.exists() or target.is_symlink():
        if (
            target.is_symlink()
            or not target.is_file()
            or target.stat().st_mode & _WRITE_BITS
            or _file_sha256(target) != expected_sha256
        ):
            raise FirstPassageStoreError("existing content-addressed artifact is corrupt")
        return
    descriptor, temporary_name = tempfile.mkstemp(prefix=".m0b-publish-", dir=root)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o444)
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError:
            if (
                target.is_symlink()
                or not target.is_file()
                or target.stat().st_mode & _WRITE_BITS
                or _file_sha256(target) != expected_sha256
            ):
                raise FirstPassageStoreError("artifact publication collided with different bytes")
        _fsync_directory(root)
    finally:
        temporary.unlink(missing_ok=True)


@dataclass(frozen=True, slots=True)
class FirstPassageStoreSpec:
    """Precommitted resource and lineage boundary for one shard build."""

    slice_id: str
    real_slice_build_sha256: str
    label_artifact_sha256: str
    feature_artifact_sha256: str
    label_row_count: int
    label_version: str
    shard_row_target: int
    max_rows: int
    search_only: bool = True
    sealed_holdout_untouched: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.slice_id, str) or not self.slice_id.strip():
            raise FirstPassageStoreError("slice_id must be non-empty")
        _sha256(self.real_slice_build_sha256, label="real_slice_build_sha256")
        _sha256(self.label_artifact_sha256, label="label_artifact_sha256")
        _sha256(self.feature_artifact_sha256, label="feature_artifact_sha256")
        _positive_integer(self.label_row_count, label="label_row_count", allow_zero=True)
        _positive_integer(self.shard_row_target, label="shard_row_target")
        _positive_integer(self.max_rows, label="max_rows")
        if self.label_row_count > self.max_rows:
            raise FirstPassageStoreError("label row count exceeds the precommitted bound")
        if not isinstance(self.label_version, str) or not self.label_version.strip():
            raise FirstPassageStoreError("label_version must be non-empty")
        if not self.search_only or not self.sealed_holdout_untouched:
            raise FirstPassageStoreError("first-passage authority must remain search-only")

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": "systematic_fx.m0b_first_passage_store_spec.v1",
            "label_artifact_sha256": self.label_artifact_sha256,
            "feature_artifact_sha256": self.feature_artifact_sha256,
            "label_row_count": self.label_row_count,
            "label_version": self.label_version,
            "max_rows": self.max_rows,
            "real_slice_build_sha256": self.real_slice_build_sha256,
            "sealed_holdout_untouched": self.sealed_holdout_untouched,
            "search_only": self.search_only,
            "shard_row_target": self.shard_row_target,
            "slice_id": self.slice_id,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


@dataclass(frozen=True, slots=True)
class FirstPassageShard:
    ordinal: int
    row_count: int
    byte_size: int
    content_sha256: str
    relative_uri: str
    first_event_key: tuple[int, int, str]
    last_event_key: tuple[int, int, str]

    def __post_init__(self) -> None:
        _positive_integer(self.ordinal, label="shard ordinal")
        _positive_integer(self.row_count, label="shard row_count")
        _positive_integer(self.byte_size, label="shard byte_size")
        _sha256(self.content_sha256, label="shard content_sha256")
        _leaf(self.relative_uri, label="shard relative_uri")
        for label, key in (("first", self.first_event_key), ("last", self.last_event_key)):
            if (
                not isinstance(key, tuple)
                or len(key) != 3
                or any(isinstance(item, bool) for item in key[:2])
                or not all(isinstance(item, int) for item in key[:2])
                or not isinstance(key[2], str)
                or not key[2]
            ):
                raise FirstPassageStoreError(f"shard {label} event key is invalid")
        if self.first_event_key > self.last_event_key:
            raise FirstPassageStoreError("shard event-key range is reversed")

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": SHARD_SCHEMA,
            "byte_size": self.byte_size,
            "content_sha256": self.content_sha256,
            "first_event_key": list(self.first_event_key),
            "last_event_key": list(self.last_event_key),
            "ordinal": self.ordinal,
            "relative_uri": self.relative_uri,
            "row_count": self.row_count,
        }

    @classmethod
    def from_dict(cls, value: object) -> FirstPassageShard:
        if not isinstance(value, dict) or value.get("artifact_schema") != SHARD_SCHEMA:
            raise FirstPassageStoreError("first-passage shard identity schema differs")
        try:
            return cls(
                ordinal=int(value["ordinal"]),
                row_count=int(value["row_count"]),
                byte_size=int(value["byte_size"]),
                content_sha256=str(value["content_sha256"]),
                relative_uri=str(value["relative_uri"]),
                first_event_key=tuple(value["first_event_key"]),  # type: ignore[arg-type]
                last_event_key=tuple(value["last_event_key"]),  # type: ignore[arg-type]
            )
        except (KeyError, TypeError, ValueError) as error:
            raise FirstPassageStoreError("first-passage shard identity is malformed") from error


@dataclass(frozen=True, slots=True)
class FirstPassageStore:
    spec_sha256: str
    source_label_sha256: str
    source_feature_sha256: str
    source_build_sha256: str
    label_version: str
    row_count: int
    shard_row_target: int
    shards: tuple[FirstPassageShard, ...]
    search_only: bool = True
    sealed_holdout_untouched: bool = True

    def __post_init__(self) -> None:
        _sha256(self.spec_sha256, label="store spec_sha256")
        _sha256(self.source_label_sha256, label="store source_label_sha256")
        _sha256(self.source_feature_sha256, label="store source_feature_sha256")
        _sha256(self.source_build_sha256, label="store source_build_sha256")
        _positive_integer(self.row_count, label="store row_count", allow_zero=True)
        _positive_integer(self.shard_row_target, label="store shard_row_target")
        if not isinstance(self.label_version, str) or not self.label_version:
            raise FirstPassageStoreError("store label_version must be non-empty")
        if not self.search_only or not self.sealed_holdout_untouched:
            raise FirstPassageStoreError("first-passage store exceeded search authority")
        if tuple(shard.ordinal for shard in self.shards) != tuple(range(1, len(self.shards) + 1)):
            raise FirstPassageStoreError("first-passage shard ordinals are not contiguous")
        if sum(shard.row_count for shard in self.shards) != self.row_count:
            raise FirstPassageStoreError("first-passage shard cardinality differs")
        if bool(self.shards) != bool(self.row_count):
            raise FirstPassageStoreError("empty store/shard shape differs")
        for previous, current in zip(self.shards, self.shards[1:], strict=False):
            if previous.last_event_key >= current.first_event_key:
                raise FirstPassageStoreError("first-passage shard event ranges overlap")

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": STORE_SCHEMA,
            "label_version": self.label_version,
            "row_count": self.row_count,
            "sealed_holdout_untouched": self.sealed_holdout_untouched,
            "search_only": self.search_only,
            "shard_row_target": self.shard_row_target,
            "shards": [shard.as_dict() for shard in self.shards],
            "source_build_sha256": self.source_build_sha256,
            "source_label_sha256": self.source_label_sha256,
            "source_feature_sha256": self.source_feature_sha256,
            "spec_sha256": self.spec_sha256,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())

    @classmethod
    def from_dict(cls, value: object) -> FirstPassageStore:
        if not isinstance(value, dict) or value.get("artifact_schema") != STORE_SCHEMA:
            raise FirstPassageStoreError("first-passage store schema differs")
        try:
            raw_shards = value["shards"]
            if not isinstance(raw_shards, list):
                raise TypeError
            return cls(
                spec_sha256=str(value["spec_sha256"]),
                source_label_sha256=str(value["source_label_sha256"]),
                source_feature_sha256=str(value["source_feature_sha256"]),
                source_build_sha256=str(value["source_build_sha256"]),
                label_version=str(value["label_version"]),
                row_count=int(value["row_count"]),
                shard_row_target=int(value["shard_row_target"]),
                shards=tuple(FirstPassageShard.from_dict(item) for item in raw_shards),
                search_only=bool(value["search_only"]),
                sealed_holdout_untouched=bool(value["sealed_holdout_untouched"]),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise FirstPassageStoreError("first-passage store manifest is malformed") from error


def _event_key(row: dict[str, Any]) -> tuple[int, int, str]:
    try:
        event_ts = row["event_ts_ns"]
        instrument_id = row["instrument_id"]
        session_id = row["session_id"]
    except KeyError as error:
        raise FirstPassageStoreError("label row lacks its event identity") from error
    if (
        isinstance(event_ts, bool)
        or not isinstance(event_ts, int)
        or event_ts < 0
        or isinstance(instrument_id, bool)
        or not isinstance(instrument_id, int)
        or instrument_id <= 0
        or not isinstance(session_id, str)
        or not session_id
    ):
        raise FirstPassageStoreError("label event identity is invalid")
    return event_ts, instrument_id, session_id


def _label_key(row: dict[str, Any]) -> tuple[object, ...]:
    event_key = _event_key(row)
    direction = row.get("direction")
    if direction not in _DIRECTION_RANK:
        raise FirstPassageStoreError("label direction is invalid")
    integer_fields: list[int] = []
    for field in ("k_tp_num", "k_tp_den", "k_sl_num", "k_sl_den", "max_hold_seconds"):
        value = row.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise FirstPassageStoreError(f"label {field} is invalid")
        maximum = _MAX_HOLD_SECONDS if field == "max_hold_seconds" else _MAX_BARRIER_COMPONENT
        if value > maximum:
            raise FirstPassageStoreError(f"label {field} exceeds its governed maximum")
        integer_fields.append(value)
    barrier_id = row.get("barrier_id")
    expected_barrier_id = (
        f"tp{integer_fields[0]}of{integer_fields[1]}_"
        f"sl{integer_fields[2]}of{integer_fields[3]}_h{integer_fields[4]}"
    )
    if barrier_id != expected_barrier_id:
        raise FirstPassageStoreError("label barrier_id differs from its exact rational fields")
    return (*event_key, _DIRECTION_RANK[direction], *integer_fields, barrier_id)


def _flush_shard(
    root: Path,
    *,
    ordinal: int,
    rows: list[bytes],
    first_event_key: tuple[int, int, str],
    last_event_key: tuple[int, int, str],
) -> FirstPassageShard:
    payload = b"".join(rows)
    content_sha256 = hashlib.sha256(payload).hexdigest()
    relative_uri = f"first-passage-shard-{ordinal:06d}-{content_sha256}.jsonl"
    _publish_immutable(root, relative_uri, payload)
    return FirstPassageShard(
        ordinal=ordinal,
        row_count=len(rows),
        byte_size=len(payload),
        content_sha256=content_sha256,
        relative_uri=relative_uri,
        first_event_key=first_event_key,
        last_event_key=last_event_key,
    )


def build_first_passage_store(
    spec: FirstPassageStoreSpec,
    build: RealSliceBuild,
    *,
    staged_root: str | Path,
    output_root: str | Path,
) -> FirstPassageStore:
    """Shard exactly one verified M0b label artifact under a finite bound."""

    if not isinstance(spec, FirstPassageStoreSpec) or not isinstance(build, RealSliceBuild):
        raise FirstPassageStoreError("store build requires canonical spec and real-slice build")
    if (
        build.slice_id != spec.slice_id
        or build.sha256 != spec.real_slice_build_sha256
        or build.label_manifest.content_sha256 != spec.label_artifact_sha256
        or build.feature_manifest.content_sha256 != spec.feature_artifact_sha256
        or build.label_manifest.row_count != spec.label_row_count
        or not build.search_only
        or not build.sealed_holdout_untouched
    ):
        raise FirstPassageStoreError("store spec differs from the immutable real-slice build")
    source_root = _safe_root(staged_root, label="staged_root")
    destination = _safe_root(output_root, label="first_passage_root", create=True)
    relative_label = build.label_manifest.relative_uri
    if relative_label is None:
        raise FirstPassageStoreError("real-slice build has no materialized label URI")
    expected_label_uri = f"label-{spec.label_artifact_sha256}.jsonl"
    if relative_label != expected_label_uri:
        raise FirstPassageStoreError("label filename is not content-addressed by its build")
    label_path = source_root / _leaf(relative_label, label="label URI")
    if label_path.is_symlink() or not label_path.is_file():
        raise FirstPassageStoreError("label artifact is absent or symbolic")

    shards: list[FirstPassageShard] = []
    buffered_rows: list[bytes] = []
    buffered_first: tuple[int, int, str] | None = None
    buffered_last: tuple[int, int, str] | None = None
    current_group: tuple[int, int, str] | None = None
    current_group_rows: list[bytes] = []
    prior_label_key: tuple[object, ...] | None = None
    row_count = 0
    source_digest = hashlib.sha256()

    def append_group(group_key: tuple[int, int, str], group_rows: list[bytes]) -> None:
        nonlocal buffered_rows, buffered_first, buffered_last
        if buffered_rows and len(buffered_rows) + len(group_rows) > spec.shard_row_target:
            assert buffered_first is not None and buffered_last is not None
            shards.append(
                _flush_shard(
                    destination,
                    ordinal=len(shards) + 1,
                    rows=buffered_rows,
                    first_event_key=buffered_first,
                    last_event_key=buffered_last,
                )
            )
            buffered_rows = []
            buffered_first = None
        if buffered_first is None:
            buffered_first = group_key
        buffered_rows.extend(group_rows)
        buffered_last = group_key

    with label_path.open("rb") as handle:
        for number, payload in enumerate(handle, start=1):
            source_digest.update(payload)
            if not payload.endswith(b"\n"):
                raise FirstPassageStoreError("label JSONL has an unterminated row")
            try:
                row = json.loads(payload)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise FirstPassageStoreError(f"label row {number} is invalid JSON") from error
            if (
                not isinstance(row, dict)
                or canonical_json_bytes(row) + b"\n" != payload
                or row.get("artifact_schema") != _LABEL_SCHEMA
                or row.get("label_version") != spec.label_version
                or row.get("parent_feature_manifest_sha256")
                != build.feature_manifest.content_sha256
            ):
                raise FirstPassageStoreError("label row differs from its canonical lineage")
            label_key = _label_key(row)
            if prior_label_key is not None and label_key <= prior_label_key:
                raise FirstPassageStoreError("label rows are not strictly ordered and unique")
            prior_label_key = label_key
            event_key = _event_key(row)
            if current_group is not None and event_key != current_group:
                append_group(current_group, current_group_rows)
                current_group_rows = []
            current_group = event_key
            current_group_rows.append(payload)
            row_count += 1
            if row_count > spec.max_rows:
                raise FirstPassageStoreError("label rows exceed the precommitted store bound")
    if current_group is not None:
        append_group(current_group, current_group_rows)
    if buffered_rows:
        assert buffered_first is not None and buffered_last is not None
        shards.append(
            _flush_shard(
                destination,
                ordinal=len(shards) + 1,
                rows=buffered_rows,
                first_event_key=buffered_first,
                last_event_key=buffered_last,
            )
        )
    if row_count != spec.label_row_count or source_digest.hexdigest() != spec.label_artifact_sha256:
        raise FirstPassageStoreError("label bytes or cardinality differ from the precommitment")
    store = FirstPassageStore(
        spec_sha256=spec.sha256,
        source_label_sha256=spec.label_artifact_sha256,
        source_feature_sha256=spec.feature_artifact_sha256,
        source_build_sha256=spec.real_slice_build_sha256,
        label_version=spec.label_version,
        row_count=row_count,
        shard_row_target=spec.shard_row_target,
        shards=tuple(shards),
    )
    payload = canonical_json_bytes(store.as_dict())
    relative_uri = f"first-passage-store-{store.sha256}.json"
    _publish_immutable(destination, relative_uri, payload)
    return store


def load_first_passage_store(
    path: str | Path,
    *,
    verify_shards: bool = True,
) -> FirstPassageStore:
    """Load an exact manifest and optionally reconcile every referenced shard."""

    requested = Path(path).expanduser()
    root = _safe_root(requested.parent, label="first_passage_root")
    manifest = root / _leaf(requested.name, label="first-passage manifest")
    if manifest.is_symlink() or not manifest.is_file():
        raise FirstPassageStoreError("first-passage manifest is absent or symbolic")
    if manifest.stat().st_mode & _WRITE_BITS:
        raise FirstPassageStoreError("first-passage manifest is not immutable")
    payload = manifest.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    if manifest.name != f"first-passage-store-{digest}.json":
        raise FirstPassageStoreError("first-passage manifest filename/hash differ")
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FirstPassageStoreError("first-passage manifest is invalid JSON") from error
    if not isinstance(document, dict) or canonical_json_bytes(document) != payload:
        raise FirstPassageStoreError("first-passage manifest is not canonical")
    store = FirstPassageStore.from_dict(document)
    if store.sha256 != digest:
        raise FirstPassageStoreError("first-passage manifest semantic identity differs")
    if verify_shards:
        source_digest = hashlib.sha256()
        total_rows = 0
        prior_label_key: tuple[object, ...] | None = None
        for shard in store.shards:
            shard_path = root / shard.relative_uri
            if (
                shard_path.is_symlink()
                or not shard_path.is_file()
                or shard_path.stat().st_mode & _WRITE_BITS
                or shard_path.stat().st_size != shard.byte_size
                or _file_sha256(shard_path) != shard.content_sha256
            ):
                raise FirstPassageStoreError("first-passage shard bytes differ")
            rows = 0
            first: tuple[int, int, str] | None = None
            last: tuple[int, int, str] | None = None
            with shard_path.open("rb") as handle:
                for payload_row in handle:
                    source_digest.update(payload_row)
                    try:
                        row = json.loads(payload_row)
                    except (UnicodeDecodeError, json.JSONDecodeError) as error:
                        raise FirstPassageStoreError(
                            "first-passage shard row is invalid"
                        ) from error
                    if (
                        not isinstance(row, dict)
                        or canonical_json_bytes(row) + b"\n" != payload_row
                        or row.get("artifact_schema") != _LABEL_SCHEMA
                        or row.get("label_version") != store.label_version
                        or row.get("parent_feature_manifest_sha256") != store.source_feature_sha256
                    ):
                        raise FirstPassageStoreError(
                            "first-passage shard row or lineage is not canonical"
                        )
                    label_key = _label_key(row)
                    if prior_label_key is not None and label_key <= prior_label_key:
                        raise FirstPassageStoreError(
                            "first-passage shard rows are not globally ordered"
                        )
                    prior_label_key = label_key
                    key = _event_key(row)
                    first = key if first is None else first
                    last = key
                    rows += 1
                    total_rows += 1
            if (
                rows != shard.row_count
                or first != shard.first_event_key
                or last != shard.last_event_key
            ):
                raise FirstPassageStoreError("first-passage shard semantic range differs")
        if total_rows != store.row_count or source_digest.hexdigest() != store.source_label_sha256:
            raise FirstPassageStoreError(
                "first-passage shards do not reconstruct the source label artifact"
            )
    return store
