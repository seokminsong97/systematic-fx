"""Streaming footer catalog and deterministic JSONL manifest generation."""

from __future__ import annotations

import json
import os
import re
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TextIO

from systematic_fx.data.footer import Mbp10Footer, read_mbp10_footer
from systematic_fx.data.instruments import InstrumentKind

_SOURCE_FILENAME = re.compile(r"glbx-mdp3-(?P<date>[0-9]{8})\.mbp-10\.parquet")


class CatalogScanError(ValueError):
    """Raised when daily source layout and footer metadata disagree."""


@dataclass(frozen=True)
class CatalogSummary:
    dataset_root: Path
    file_count: int
    total_file_bytes: int
    total_rows: int
    total_row_groups: int
    mapping_interval_count: int
    unique_instrument_count: int
    schema_fingerprints: tuple[str, ...]
    request_symbols: tuple[str, ...]
    outright_mapping_count: int
    calendar_spread_mapping_count: int
    unknown_mapping_count: int
    partial_symbol_count: int
    not_found_symbol_count: int
    files_with_partial: int
    files_with_not_found: int
    first_source_date: date | None
    last_source_date: date | None
    start_date_filter: date | None
    end_date_filter: date | None
    limit: int | None
    manifest_path: Path | None

    def as_dict(self) -> dict[str, object]:
        return {
            "calendar_spread_mapping_count": self.calendar_spread_mapping_count,
            "dataset_root": self.dataset_root.as_posix(),
            "end_date_filter": self.end_date_filter.isoformat() if self.end_date_filter else None,
            "file_count": self.file_count,
            "files_with_not_found": self.files_with_not_found,
            "files_with_partial": self.files_with_partial,
            "first_source_date": (
                self.first_source_date.isoformat() if self.first_source_date else None
            ),
            "last_source_date": (
                self.last_source_date.isoformat() if self.last_source_date else None
            ),
            "limit": self.limit,
            "manifest_path": self.manifest_path.as_posix() if self.manifest_path else None,
            "mapping_interval_count": self.mapping_interval_count,
            "not_found_symbol_count": self.not_found_symbol_count,
            "outright_mapping_count": self.outright_mapping_count,
            "partial_symbol_count": self.partial_symbol_count,
            "request_symbols": list(self.request_symbols),
            "schema_fingerprint_count": len(self.schema_fingerprints),
            "schema_fingerprints": list(self.schema_fingerprints),
            "start_date_filter": (
                self.start_date_filter.isoformat() if self.start_date_filter else None
            ),
            "total_file_bytes": self.total_file_bytes,
            "total_row_groups": self.total_row_groups,
            "total_rows": self.total_rows,
            "unique_instrument_count": self.unique_instrument_count,
            "unknown_mapping_count": self.unknown_mapping_count,
        }


def _source_date(path: Path) -> date:
    match = _SOURCE_FILENAME.fullmatch(path.name)
    if match is None:
        raise CatalogScanError(f"unrecognized MBP-10 source filename: {path.name}")
    try:
        source_date = date.fromisoformat(
            f"{match.group('date')[0:4]}-{match.group('date')[4:6]}-{match.group('date')[6:8]}"
        )
    except ValueError as exc:
        raise CatalogScanError(f"invalid source date in filename: {path.name}") from exc

    expected_partition = (
        f"{source_date.year:04d}",
        f"{source_date.month:02d}",
        f"{source_date.day:02d}",
    )
    actual_partition = path.parts[-4:-1]
    if actual_partition != expected_partition:
        raise CatalogScanError(
            f"source partition mismatch for {path}: expected "
            f"{'/'.join(expected_partition)}, found {'/'.join(actual_partition)}"
        )
    return source_date


def _validate_selection(
    start_date: date | None,
    end_date: date | None,
    limit: int | None,
) -> None:
    if start_date is not None and not isinstance(start_date, date):
        raise TypeError("start_date must be a date or None")
    if end_date is not None and not isinstance(end_date, date):
        raise TypeError("end_date must be a date or None")
    if start_date is not None and end_date is not None and end_date < start_date:
        raise ValueError("end_date must be on or after start_date")
    if isinstance(limit, bool) or (limit is not None and (not isinstance(limit, int) or limit < 0)):
        raise ValueError("limit must be a non-negative integer or None")


def _iter_parquet_paths(dataset_root: Path) -> Iterator[Path]:
    for directory, directory_names, file_names in os.walk(dataset_root):
        directory_names.sort()
        for file_name in sorted(file_names):
            if file_name.endswith(".parquet"):
                yield Path(directory) / file_name


def iter_catalog_entries(
    dataset_root: Path | str,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int | None = None,
) -> Iterator[Mbp10Footer]:
    """Yield validated footers in path order, optionally selecting a bounded pilot."""

    _validate_selection(start_date, end_date, limit)
    root = Path(dataset_root).expanduser().resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"MBP-10 dataset directory does not exist: {root}")

    emitted = 0
    for path in _iter_parquet_paths(root):
        path_source_date = _source_date(path)
        if start_date is not None and path_source_date < start_date:
            continue
        if end_date is not None and path_source_date > end_date:
            continue
        if limit is not None and emitted >= limit:
            break

        footer = read_mbp10_footer(path)
        if footer.source_date != path_source_date:
            raise CatalogScanError(
                f"source date mismatch for {path}: filename={path_source_date.isoformat()}, "
                f"dbn.metadata={footer.source_date.isoformat()}"
            )
        yield footer
        emitted += 1


def _open_manifest(path: Path) -> tuple[TextIO, Path]:
    path.parent.mkdir(parents=True, exist_ok=True)
    # The caller owns this handle across the complete streaming scan and closes it
    # in its ``finally`` block before the atomic rename.
    handle = tempfile.NamedTemporaryFile(  # noqa: SIM115
        mode="w",
        encoding="utf-8",
        newline="\n",
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        delete=False,
    )
    return handle, Path(handle.name)


def scan_catalog(
    dataset_root: Path | str,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int | None = None,
    manifest_path: Path | str | None = None,
    include_mappings: bool = True,
) -> CatalogSummary:
    """Aggregate validated footers and optionally atomically write one JSON object per file."""

    root = Path(dataset_root).expanduser().resolve()
    final_manifest = (
        Path(manifest_path).expanduser().resolve() if manifest_path is not None else None
    )
    manifest_handle: TextIO | None = None
    temporary_manifest: Path | None = None
    if final_manifest is not None:
        manifest_handle, temporary_manifest = _open_manifest(final_manifest)

    file_count = 0
    total_file_bytes = 0
    total_rows = 0
    total_row_groups = 0
    mapping_interval_count = 0
    instrument_ids: set[int] = set()
    schema_fingerprints: set[str] = set()
    request_symbols: set[str] = set()
    kind_counts = {kind: 0 for kind in InstrumentKind}
    partial_symbol_count = 0
    not_found_symbol_count = 0
    files_with_partial = 0
    files_with_not_found = 0
    first_source_date: date | None = None
    last_source_date: date | None = None

    try:
        for footer in iter_catalog_entries(
            root,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        ):
            file_count += 1
            total_file_bytes += footer.file_size_bytes
            total_rows += footer.row_count
            total_row_groups += footer.row_group_count
            mapping_interval_count += len(footer.mappings)
            instrument_ids.update(mapping.instrument_id for mapping in footer.mappings)
            schema_fingerprints.add(footer.schema_fingerprint)
            request_symbols.update(footer.symbols)
            partial_symbol_count += len(footer.partial)
            not_found_symbol_count += len(footer.not_found)
            files_with_partial += bool(footer.partial)
            files_with_not_found += bool(footer.not_found)
            for mapping in footer.mappings:
                kind_counts[mapping.kind] += 1
            first_source_date = (
                footer.source_date
                if first_source_date is None
                else min(first_source_date, footer.source_date)
            )
            last_source_date = (
                footer.source_date
                if last_source_date is None
                else max(last_source_date, footer.source_date)
            )

            if manifest_handle is not None:
                record = footer.as_dict(
                    relative_to=root,
                    include_mappings=include_mappings,
                )
                manifest_handle.write(
                    json.dumps(
                        record,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                )
                manifest_handle.write("\n")

        if (
            manifest_handle is not None
            and final_manifest is not None
            and temporary_manifest is not None
        ):
            manifest_handle.flush()
            os.fsync(manifest_handle.fileno())
            manifest_handle.close()
            manifest_handle = None
            os.replace(temporary_manifest, final_manifest)
            temporary_manifest = None
    except BaseException:
        if manifest_handle is not None:
            manifest_handle.close()
        if temporary_manifest is not None:
            temporary_manifest.unlink(missing_ok=True)
        raise

    return CatalogSummary(
        dataset_root=root,
        file_count=file_count,
        total_file_bytes=total_file_bytes,
        total_rows=total_rows,
        total_row_groups=total_row_groups,
        mapping_interval_count=mapping_interval_count,
        unique_instrument_count=len(instrument_ids),
        schema_fingerprints=tuple(sorted(schema_fingerprints)),
        request_symbols=tuple(sorted(request_symbols)),
        outright_mapping_count=kind_counts[InstrumentKind.OUTRIGHT],
        calendar_spread_mapping_count=kind_counts[InstrumentKind.CALENDAR_SPREAD],
        unknown_mapping_count=kind_counts[InstrumentKind.UNKNOWN],
        partial_symbol_count=partial_symbol_count,
        not_found_symbol_count=not_found_symbol_count,
        files_with_partial=files_with_partial,
        files_with_not_found=files_with_not_found,
        first_source_date=first_source_date,
        last_source_date=last_source_date,
        start_date_filter=start_date,
        end_date_filter=end_date,
        limit=limit,
        manifest_path=final_manifest,
    )
