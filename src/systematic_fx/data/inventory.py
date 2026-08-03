"""Filesystem inventory for immutable daily MBP-10 Parquet sources."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path


_DAILY_FILE_PATTERN = re.compile(
    r"(?P<year>\d{4})/(?P<month>\d{2})/(?P<day>\d{2})/"
    r"glbx-mdp3-(?P<stamp>\d{8})\.mbp-10\.parquet"
)


@dataclass(frozen=True)
class InventorySummary:
    dataset_root: Path
    file_count: int
    total_bytes: int
    first_source_date: date | None
    last_source_date: date | None
    invalid_layout_files: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "dataset_root": str(self.dataset_root),
            "file_count": self.file_count,
            "total_bytes": self.total_bytes,
            "first_source_date": (
                self.first_source_date.isoformat() if self.first_source_date else None
            ),
            "last_source_date": (
                self.last_source_date.isoformat() if self.last_source_date else None
            ),
            "invalid_layout_files": list(self.invalid_layout_files),
        }


def _source_date(relative_path: Path) -> date | None:
    match = _DAILY_FILE_PATTERN.fullmatch(relative_path.as_posix())
    if match is None:
        return None

    try:
        path_date = date(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
        )
    except ValueError:
        return None

    return path_date if path_date.strftime("%Y%m%d") == match.group("stamp") else None


def summarize_inventory(dataset_root: Path) -> InventorySummary:
    """Read file metadata only; Parquet event rows and sealed data stay untouched."""

    root = dataset_root.expanduser().resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"MBP-10 dataset directory does not exist: {root}")

    file_count = 0
    total_bytes = 0
    source_dates: list[date] = []
    invalid_layout_files: list[str] = []

    for parquet_path in sorted(root.rglob("*.parquet")):
        relative_path = parquet_path.relative_to(root)
        file_count += 1
        total_bytes += parquet_path.stat().st_size

        parsed_date = _source_date(relative_path)
        if parsed_date is None:
            invalid_layout_files.append(relative_path.as_posix())
        else:
            source_dates.append(parsed_date)

    return InventorySummary(
        dataset_root=root,
        file_count=file_count,
        total_bytes=total_bytes,
        first_source_date=min(source_dates) if source_dates else None,
        last_source_date=max(source_dates) if source_dates else None,
        invalid_layout_files=tuple(invalid_layout_files),
    )
