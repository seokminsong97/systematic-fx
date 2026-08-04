"""Read and validate Parquet footers without materializing MBP-10 event rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from systematic_fx.data.contracts import (
    Mbp10ContractError,
    Mbp10ContractMetadata,
    compute_schema_fingerprint,
    decode_dbn_metadata,
    validate_mbp10_contract,
)
from systematic_fx.data.instruments import (
    InstrumentKind,
    InstrumentMapping,
    parse_instrument_mappings,
)


def _required_int(metadata: dict[str, Any], key: str) -> int:
    value = metadata.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise Mbp10ContractError(f"dbn.metadata {key!r} must be an integer")
    return value


def _utc_date_from_ns(timestamp_ns: int) -> date:
    return datetime.fromtimestamp(timestamp_ns // 1_000_000_000, tz=UTC).date()


def _required_string_list(metadata: dict[str, Any], key: str) -> tuple[str, ...]:
    value = metadata.get(key)
    if not isinstance(value, list):
        raise Mbp10ContractError(f"dbn.metadata {key!r} must be a list")
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item:
            raise Mbp10ContractError(f"dbn.metadata {key}[{index}] must be a non-empty string")
    return tuple(sorted(value))


@dataclass(frozen=True)
class Mbp10Footer:
    """Catalog-safe subset of a valid daily file's footer metadata."""

    path: Path
    file_size_bytes: int
    row_count: int
    row_group_count: int
    column_count: int
    serialized_footer_bytes: int
    created_by: str | None
    dbn_start_ns: int
    dbn_end_ns: int
    source_date: date
    source_end_date: date
    contract: Mbp10ContractMetadata
    schema_fingerprint: str
    symbols: tuple[str, ...]
    partial: tuple[str, ...]
    not_found: tuple[str, ...]
    mappings: tuple[InstrumentMapping, ...]

    @property
    def unique_instrument_count(self) -> int:
        return len({mapping.instrument_id for mapping in self.mappings})

    def kind_counts(self) -> dict[InstrumentKind, int]:
        counts = {kind: 0 for kind in InstrumentKind}
        for mapping in self.mappings:
            counts[mapping.kind] += 1
        return counts

    def as_dict(
        self,
        *,
        relative_to: Path | None = None,
        include_mappings: bool = True,
    ) -> dict[str, object]:
        output_path = self.path
        if relative_to is not None:
            output_path = self.path.relative_to(relative_to.expanduser().resolve())

        counts = self.kind_counts()
        result: dict[str, object] = {
            "column_count": self.column_count,
            "contract": self.contract.as_dict(),
            "created_by": self.created_by,
            "dbn_end_ns": self.dbn_end_ns,
            "dbn_start_ns": self.dbn_start_ns,
            "file_size_bytes": self.file_size_bytes,
            "instrument_kind_counts": {kind.value: counts[kind] for kind in InstrumentKind},
            "mapping_interval_count": len(self.mappings),
            "path": output_path.as_posix(),
            "row_count": self.row_count,
            "row_group_count": self.row_group_count,
            "schema_fingerprint": self.schema_fingerprint,
            "serialized_footer_bytes": self.serialized_footer_bytes,
            "source_date": self.source_date.isoformat(),
            "source_end_date": self.source_end_date.isoformat(),
            "symbols": list(self.symbols),
            "partial": list(self.partial),
            "not_found": list(self.not_found),
            "unique_instrument_count": self.unique_instrument_count,
        }
        if include_mappings:
            result["instrument_mappings"] = [mapping.as_dict() for mapping in self.mappings]
        return result


def read_mbp10_footer(path: Path | str) -> Mbp10Footer:
    """Open only a Parquet footer and enforce the complete MBP-10 raw contract."""

    resolved = Path(path).expanduser().resolve()
    parquet_file = pq.ParquetFile(resolved)
    schema = parquet_file.schema_arrow
    contract = validate_mbp10_contract(schema)

    schema_metadata = schema.metadata or {}
    payload = schema_metadata.get(b"dbn.metadata")
    if payload is None:
        raise Mbp10ContractError("schema metadata 'dbn.metadata' is missing")
    dbn_metadata = decode_dbn_metadata(payload)
    dbn_start_ns = _required_int(dbn_metadata, "start")
    dbn_end_ns = _required_int(dbn_metadata, "end")
    if dbn_start_ns < 0 or dbn_end_ns <= dbn_start_ns:
        raise Mbp10ContractError("dbn.metadata end must be greater than its non-negative start")

    file_metadata = parquet_file.metadata
    return Mbp10Footer(
        path=resolved,
        file_size_bytes=resolved.stat().st_size,
        row_count=file_metadata.num_rows,
        row_group_count=file_metadata.num_row_groups,
        column_count=file_metadata.num_columns,
        serialized_footer_bytes=file_metadata.serialized_size,
        created_by=file_metadata.created_by,
        dbn_start_ns=dbn_start_ns,
        dbn_end_ns=dbn_end_ns,
        source_date=_utc_date_from_ns(dbn_start_ns),
        source_end_date=_utc_date_from_ns(dbn_end_ns),
        contract=contract,
        schema_fingerprint=compute_schema_fingerprint(schema, contract),
        symbols=_required_string_list(dbn_metadata, "symbols"),
        partial=_required_string_list(dbn_metadata, "partial"),
        not_found=_required_string_list(dbn_metadata, "not_found"),
        mappings=parse_instrument_mappings(payload),
    )
