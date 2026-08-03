"""Small, dependency-free runtime settings layer."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _environment_path(name: str, default: Path) -> Path:
    value = os.environ.get(name)
    return Path(value).expanduser() if value else default


@dataclass(frozen=True)
class Settings:
    """Machine-specific locations; research parameters belong in ``configs/``."""

    data_root: Path
    artifacts_root: Path
    database_url: str | None

    @property
    def mbp10_root(self) -> Path:
        return self.data_root / "mbp-10"

    @classmethod
    def from_env(cls, *, working_directory: Path | None = None) -> "Settings":
        base = (working_directory or Path.cwd()).resolve()
        return cls(
            data_root=_environment_path("SYSTEMATIC_FX_DATA_ROOT", base / "data"),
            artifacts_root=_environment_path(
                "SYSTEMATIC_FX_ARTIFACTS_ROOT", base / "artifacts"
            ),
            database_url=os.environ.get("SYSTEMATIC_FX_DATABASE_URL"),
        )
