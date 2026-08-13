"""Exact checked-in configuration for the M0b first-passage store."""

from __future__ import annotations

import hashlib
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from systematic_fx.research.hypotheses import canonical_sha256
from systematic_fx.research.m0b.config import _resolve_existing_search_path
from systematic_fx.research.m0b.first_passage_store import (
    FirstPassageStore,
    FirstPassageStoreError,
    FirstPassageStoreSpec,
    _sha256,
)

CONFIG_SCHEMA: Final = "systematic_fx.m0b_first_passage_store_config.v1"


def _exact(value: object, expected: set[str], *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        raise FirstPassageStoreError(f"{label} keys differ from the frozen schema")
    return value


def _integer(value: object, *, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise FirstPassageStoreError(f"{label} must be an integer >= {minimum}")
    return value


def _string(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise FirstPassageStoreError(f"{label} must be a canonical non-empty string")
    return value


@dataclass(frozen=True, slots=True)
class FirstPassageStoreConfig:
    path: Path
    file_sha256: str
    semantic_sha256: str
    store_spec: FirstPassageStoreSpec
    expected_store_sha256: str
    store_schema: str
    store_version: str

    def __post_init__(self) -> None:
        _sha256(self.file_sha256, label="config file SHA-256")
        _sha256(self.semantic_sha256, label="config semantic SHA-256")
        _sha256(self.expected_store_sha256, label="expected store SHA-256")
        if self.store_schema != "systematic_fx.m0b_first_passage_store.v1":
            raise FirstPassageStoreError("configured store schema differs")
        if self.store_version != "m0b_first_passage_store_v1":
            raise FirstPassageStoreError("configured store version differs")

    def verify_unchanged(self) -> None:
        if load_first_passage_store_config(self.path) != self:
            raise FirstPassageStoreError("first-passage config changed after load")

    def verify_store(self, store: FirstPassageStore) -> None:
        if (
            not isinstance(store, FirstPassageStore)
            or store.sha256 != self.expected_store_sha256
            or store.spec_sha256 != self.store_spec.sha256
            or store.source_build_sha256 != self.store_spec.real_slice_build_sha256
            or store.source_label_sha256 != self.store_spec.label_artifact_sha256
            or store.source_feature_sha256 != self.store_spec.feature_artifact_sha256
            or store.row_count != self.store_spec.label_row_count
        ):
            raise FirstPassageStoreError("first-passage store differs from checked-in config")


def load_first_passage_store_config(path: str | Path) -> FirstPassageStoreConfig:
    """Load a config only when its sidecar byte hash and semantic hash agree."""

    resolved = _resolve_existing_search_path(
        Path(path).expanduser(),
        label="first-passage store config",
        kind="file",
    )
    sidecar = _resolve_existing_search_path(
        resolved.with_suffix(resolved.suffix + ".sha256"),
        label="first-passage store config SHA-256",
        kind="file",
    )
    try:
        file_sha256 = sidecar.read_text(encoding="ascii").strip()
    except UnicodeDecodeError as error:
        raise FirstPassageStoreError("config SHA-256 sidecar is not ASCII") from error
    _sha256(file_sha256, label="config file SHA-256")
    payload = resolved.read_bytes()
    if hashlib.sha256(payload).hexdigest() != file_sha256:
        raise FirstPassageStoreError("first-passage config bytes differ from the sidecar")
    try:
        document = tomllib.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise FirstPassageStoreError("first-passage store config is invalid TOML") from error
    root = _exact(
        document,
        {"config", "source", "sharding", "expected"},
        label="first-passage config",
    )
    config = _exact(root["config"], {"artifact_schema", "semantic_sha256"}, label="config")
    source = _exact(
        root["source"],
        {
            "slice_id",
            "real_slice_build_sha256",
            "label_artifact_sha256",
            "feature_artifact_sha256",
            "label_row_count",
            "label_version",
            "search_only",
            "sealed_holdout_untouched",
        },
        label="source",
    )
    sharding = _exact(
        root["sharding"],
        {"store_schema", "store_version", "shard_row_target", "max_rows"},
        label="sharding",
    )
    expected = _exact(
        root["expected"],
        {"store_spec_sha256", "store_sha256"},
        label="expected",
    )
    semantic_document = {
        "artifact_schema": _string(config["artifact_schema"], label="artifact_schema"),
        "expected": dict(expected),
        "sharding": dict(sharding),
        "source": dict(source),
    }
    if semantic_document["artifact_schema"] != CONFIG_SCHEMA:
        raise FirstPassageStoreError("first-passage config schema differs")
    semantic_sha256 = _sha256(config["semantic_sha256"], label="semantic SHA-256")
    if canonical_sha256(semantic_document) != semantic_sha256:
        raise FirstPassageStoreError("first-passage config semantic hash differs")
    spec = FirstPassageStoreSpec(
        slice_id=_string(source["slice_id"], label="slice_id"),
        real_slice_build_sha256=_sha256(
            source["real_slice_build_sha256"], label="real_slice_build_sha256"
        ),
        label_artifact_sha256=_sha256(
            source["label_artifact_sha256"], label="label_artifact_sha256"
        ),
        feature_artifact_sha256=_sha256(
            source["feature_artifact_sha256"], label="feature_artifact_sha256"
        ),
        label_row_count=_integer(source["label_row_count"], label="label_row_count"),
        label_version=_string(source["label_version"], label="label_version"),
        shard_row_target=_integer(
            sharding["shard_row_target"], label="shard_row_target", minimum=1
        ),
        max_rows=_integer(sharding["max_rows"], label="max_rows", minimum=1),
        search_only=source["search_only"] is True,
        sealed_holdout_untouched=source["sealed_holdout_untouched"] is True,
    )
    if _sha256(expected["store_spec_sha256"], label="store_spec_sha256") != spec.sha256:
        raise FirstPassageStoreError("configured store spec hash differs")
    return FirstPassageStoreConfig(
        path=resolved,
        file_sha256=file_sha256,
        semantic_sha256=semantic_sha256,
        store_spec=spec,
        expected_store_sha256=_sha256(expected["store_sha256"], label="store_sha256"),
        store_schema=_string(sharding["store_schema"], label="store_schema"),
        store_version=_string(sharding["store_version"], label="store_version"),
    )
