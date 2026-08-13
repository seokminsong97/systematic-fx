"""Immutable records for the staged M0b real-data slice."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

from systematic_fx.research.hypotheses import canonical_sha256


class RealSliceError(ValueError):
    """A staged source, reference, session, or artifact failed closed."""


@dataclass(frozen=True, slots=True)
class SourceSpec:
    source_date: date
    relative_uri: str
    sha256: str

    def __post_init__(self) -> None:
        if len(self.sha256) != 64 or any(c not in "0123456789abcdef" for c in self.sha256):
            raise RealSliceError("source SHA-256 must be lowercase hexadecimal")
        if not self.relative_uri or self.relative_uri.startswith(("/", "..")):
            raise RealSliceError("source URI must be relative and bounded")

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_date": self.source_date.isoformat(),
            "relative_uri": self.relative_uri,
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class SessionSlice:
    trading_date: date
    role: str
    session_id: str
    open_ts_ns: int
    close_ts_ns: int
    raw_symbol: str
    instrument_id: int
    source_dates: tuple[date, ...]
    cache_status: str
    active_selection_proven: bool

    def __post_init__(self) -> None:
        if self.active_selection_proven:
            raise RealSliceError("schedule-only M0b cannot assert an active execution contract")

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["trading_date"] = self.trading_date.isoformat()
        value["source_dates"] = [item.isoformat() for item in self.source_dates]
        return value

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> SessionSlice:
        return cls(
            trading_date=date.fromisoformat(str(value["trading_date"])),
            role=str(value["role"]),
            session_id=str(value["session_id"]),
            open_ts_ns=int(value["open_ts_ns"]),
            close_ts_ns=int(value["close_ts_ns"]),
            raw_symbol=str(value["raw_symbol"]),
            instrument_id=int(value["instrument_id"]),
            source_dates=tuple(date.fromisoformat(str(item)) for item in value["source_dates"]),
            cache_status=str(value["cache_status"]),
            active_selection_proven=bool(value["active_selection_proven"]),
        )


@dataclass(frozen=True, slots=True)
class ArtifactIdentity:
    artifact_type: str
    row_count: int
    content_sha256: str
    parent_sha256: str | None
    relative_uri: str | None = None

    def __post_init__(self) -> None:
        if not self.artifact_type or self.row_count < 0:
            raise RealSliceError("artifact identity requires a type and non-negative row count")
        for label, value in (
            ("content", self.content_sha256),
            ("parent", self.parent_sha256),
        ):
            if value is not None and (
                len(value) != 64 or any(character not in "0123456789abcdef" for character in value)
            ):
                raise RealSliceError(f"artifact {label} SHA-256 is invalid")
        if self.relative_uri is not None and (
            not self.relative_uri or Path(self.relative_uri).name != self.relative_uri
        ):
            raise RealSliceError("artifact URI must be a direct relative leaf")

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ArtifactIdentity:
        return cls(
            artifact_type=str(value["artifact_type"]),
            row_count=int(value["row_count"]),
            content_sha256=str(value["content_sha256"]),
            parent_sha256=(
                None if value.get("parent_sha256") is None else str(value["parent_sha256"])
            ),
            relative_uri=(
                None if value.get("relative_uri") is None else str(value["relative_uri"])
            ),
        )


@dataclass(frozen=True, slots=True)
class RealSliceBuild:
    slice_id: str
    config_hash: str
    source_manifest: ArtifactIdentity
    quote_manifest: ArtifactIdentity
    feature_manifest: ArtifactIdentity
    label_manifest: ArtifactIdentity
    sessions: tuple[SessionSlice, ...]
    search_only: bool = True
    sealed_holdout_untouched: bool = True

    def __post_init__(self) -> None:
        if not self.search_only or not self.sealed_holdout_untouched:
            raise RealSliceError(
                "M0b build authority must remain search-only with holdout untouched"
            )
        if self.quote_manifest.parent_sha256 != self.source_manifest.content_sha256:
            raise RealSliceError("quote/source artifact lineage is broken")
        if self.feature_manifest.parent_sha256 != self.quote_manifest.content_sha256:
            raise RealSliceError("feature/quote artifact lineage is broken")
        if self.label_manifest.parent_sha256 != self.feature_manifest.content_sha256:
            raise RealSliceError("label/feature artifact lineage is broken")

    def as_dict(self) -> dict[str, Any]:
        return {
            "slice_id": self.slice_id,
            "config_hash": self.config_hash,
            "source_manifest": self.source_manifest.as_dict(),
            "quote_manifest": self.quote_manifest.as_dict(),
            "feature_manifest": self.feature_manifest.as_dict(),
            "label_manifest": self.label_manifest.as_dict(),
            "sessions": [item.as_dict() for item in self.sessions],
            "search_only": self.search_only,
            "sealed_holdout_untouched": self.sealed_holdout_untouched,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> RealSliceBuild:
        return cls(
            slice_id=str(value["slice_id"]),
            config_hash=str(value["config_hash"]),
            source_manifest=ArtifactIdentity.from_dict(value["source_manifest"]),
            quote_manifest=ArtifactIdentity.from_dict(value["quote_manifest"]),
            feature_manifest=ArtifactIdentity.from_dict(value["feature_manifest"]),
            label_manifest=ArtifactIdentity.from_dict(value["label_manifest"]),
            sessions=tuple(SessionSlice.from_dict(item) for item in value["sessions"]),
            search_only=bool(value["search_only"]),
            sealed_holdout_untouched=bool(value["sealed_holdout_untouched"]),
        )
