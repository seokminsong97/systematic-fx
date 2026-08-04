"""Workspace-local PostgreSQL 18 lifecycle management without TCP exposure."""

from __future__ import annotations

import os
import re
import stat
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from urllib.parse import quote, urlencode

POSTGRESQL_MAJOR_VERSION = 18
POSTGRESQL_PORT = 55432
DEFAULT_BIN_DIRECTORY = Path("/Library/PostgreSQL/18/bin")

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
_VERSION = re.compile(r"\bPostgreSQL\)?\s+(?P<major>[0-9]+)(?:\.[0-9]+)?\b")
_MANAGED_INCLUDE_MARKER = "# systematic-fx local cluster: managed include"


class LocalClusterError(RuntimeError):
    """A local PostgreSQL cluster operation failed."""


class LocalClusterSafetyError(LocalClusterError):
    """A path or configuration could expose or overwrite an unsafe target."""


class ClusterState(str, Enum):
    """Observable local cluster lifecycle states."""

    UNINITIALIZED = "uninitialized"
    STOPPED = "stopped"
    RUNNING = "running"


@dataclass(frozen=True)
class ClusterStatus:
    """Current cluster state and its non-network connection coordinates."""

    state: ClusterState
    root: Path
    socket_directory: Path
    port: int = POSTGRESQL_PORT

    @property
    def initialized(self) -> bool:
        return self.state is not ClusterState.UNINITIALIZED

    @property
    def running(self) -> bool:
        return self.state is ClusterState.RUNNING


class LocalPostgresCluster:
    """Manage one PostgreSQL 18 cluster below ``<workspace>/.local/postgres``."""

    def __init__(
        self,
        root: Path,
        *,
        bin_directory: Path = DEFAULT_BIN_DIRECTORY,
    ) -> None:
        if not isinstance(root, Path):
            raise TypeError("root must be passed explicitly as pathlib.Path")
        if not isinstance(bin_directory, Path):
            raise TypeError("bin_directory must be pathlib.Path")

        self.root = self._validate_root(root)
        self.bin_directory = bin_directory.expanduser().resolve(strict=False)
        self.data_directory = self.root / "data"
        self.socket_directory = self.root / "socket"
        self.log_directory = self.root / "logs"
        self.log_file = self.log_directory / "postgresql.log"
        self.managed_config = self.root / "systematic_fx.conf"

    @staticmethod
    def _validate_root(root: Path) -> Path:
        if not root.is_absolute():
            raise LocalClusterSafetyError("local PostgreSQL root must be an absolute path")
        if any(character in str(root) for character in ("\n", "\r", "\x00")):
            raise LocalClusterSafetyError("local PostgreSQL root contains a control character")

        resolved = root.expanduser().resolve(strict=False)
        filesystem_root = Path(resolved.anchor)
        home = Path.home().resolve()
        if resolved in (filesystem_root, home, home.parent):
            raise LocalClusterSafetyError("refusing a filesystem, home, or home-parent root")
        if resolved.name != "postgres" or resolved.parent.name != ".local":
            raise LocalClusterSafetyError("local PostgreSQL root must end with .local/postgres")

        workspace = resolved.parent.parent
        if workspace in (filesystem_root, home) or not workspace.is_dir():
            raise LocalClusterSafetyError(
                "local PostgreSQL root must belong to an existing non-home workspace"
            )
        return resolved

    @staticmethod
    def _validate_identifier(identifier: str, *, label: str) -> str:
        if not _SAFE_IDENTIFIER.fullmatch(identifier):
            raise LocalClusterSafetyError(
                f"{label} must be 1-63 ASCII letters, digits, or underscores "
                "and must start with a letter or underscore"
            )
        return identifier

    @staticmethod
    def _ensure_directory(path: Path, *, mode: int = 0o700) -> None:
        if path.is_symlink():
            raise LocalClusterSafetyError(f"managed directory must not be a symlink: {path}")
        if path.exists() and not path.is_dir():
            raise LocalClusterSafetyError(f"managed directory is not a directory: {path}")
        path.mkdir(parents=True, exist_ok=True, mode=mode)
        path.chmod(mode)
        actual_mode = stat.S_IMODE(path.stat().st_mode)
        if actual_mode != mode:
            raise LocalClusterSafetyError(
                f"managed directory has mode {actual_mode:04o}, expected {mode:04o}: {path}"
            )

    @staticmethod
    def _reject_symlink(path: Path, *, label: str) -> None:
        if path.is_symlink():
            raise LocalClusterSafetyError(f"{label} must not be a symlink: {path}")

    def _prepare_layout(self) -> None:
        self._ensure_directory(self.root)
        self._ensure_directory(self.data_directory)
        self._ensure_directory(self.socket_directory)
        self._ensure_directory(self.log_directory)

    def _binary(self, name: str) -> Path:
        if name not in {"initdb", "pg_ctl", "postgres"}:
            raise ValueError(f"unsupported PostgreSQL executable: {name}")
        binary = self.bin_directory / name
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise LocalClusterError(f"PostgreSQL executable is missing or not executable: {binary}")
        return binary

    @staticmethod
    def _invoke(
        arguments: Sequence[str | Path],
        *,
        accepted_returncodes: frozenset[int] = frozenset((0,)),
    ) -> subprocess.CompletedProcess[str]:
        argv = [str(argument) for argument in arguments]
        environment = os.environ.copy()
        environment.setdefault("PGAPPNAME", "systematic-fx-local-cluster")
        completed = subprocess.run(
            argv,
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        if completed.returncode not in accepted_returncodes:
            detail = completed.stderr.strip() or completed.stdout.strip() or "unknown error"
            raise LocalClusterError(f"PostgreSQL command failed: {detail}")
        return completed

    def _verify_installation(self) -> None:
        completed = self._invoke((self._binary("postgres"), "--version"))
        match = _VERSION.search(completed.stdout.strip())
        if match is None:
            raise LocalClusterError("could not determine installed PostgreSQL version")
        major = int(match.group("major"))
        if major != POSTGRESQL_MAJOR_VERSION:
            raise LocalClusterError(
                f"PostgreSQL {POSTGRESQL_MAJOR_VERSION} is required; found major {major}"
            )

    def _is_initialized(self) -> bool:
        self._reject_symlink(self.root, label="local PostgreSQL root")
        self._reject_symlink(self.data_directory, label="PostgreSQL data directory")
        if self.root.exists() and not self.root.is_dir():
            raise LocalClusterSafetyError(f"local PostgreSQL root is not a directory: {self.root}")
        if self.data_directory.exists() and not self.data_directory.is_dir():
            raise LocalClusterSafetyError(
                f"PostgreSQL data path is not a directory: {self.data_directory}"
            )
        version_file = self.data_directory / "PG_VERSION"
        self._reject_symlink(version_file, label="PG_VERSION")
        return version_file.is_file()

    def _validate_data_version(self) -> None:
        version_file = self.data_directory / "PG_VERSION"
        if not version_file.is_file():
            raise LocalClusterError("local PostgreSQL cluster is not initialized")
        version = version_file.read_text(encoding="ascii").strip()
        if version != str(POSTGRESQL_MAJOR_VERSION):
            raise LocalClusterError(
                f"data directory requires PostgreSQL {version!r}, expected "
                f"{POSTGRESQL_MAJOR_VERSION}"
            )

    @staticmethod
    def _configuration_string(value: str) -> str:
        if any(character in value for character in ("\n", "\r", "\x00")):
            raise LocalClusterSafetyError("PostgreSQL configuration value is unsafe")
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"

    def _managed_configuration_text(self) -> str:
        socket_value = self._configuration_string(str(self.socket_directory))
        return (
            "# Managed by systematic-fx. Manual changes are replaced.\n"
            "listen_addresses = ''\n"
            f"port = {POSTGRESQL_PORT}\n"
            f"unix_socket_directories = {socket_value}\n"
            "unix_socket_permissions = 0700\n"
        )

    def _ensure_managed_configuration(self) -> None:
        postgresql_config = self.data_directory / "postgresql.conf"
        self._reject_symlink(postgresql_config, label="postgresql.conf")
        self._reject_symlink(self.managed_config, label="managed configuration")
        if not postgresql_config.is_file():
            raise LocalClusterError(f"postgresql.conf is missing: {postgresql_config}")

        managed_text = self._managed_configuration_text()
        if (
            not self.managed_config.exists()
            or self.managed_config.read_text(encoding="utf-8") != managed_text
        ):
            self.managed_config.write_text(managed_text, encoding="utf-8")
        self.managed_config.chmod(0o600)

        include_value = self._configuration_string(str(self.managed_config))
        include_block = f"{_MANAGED_INCLUDE_MARKER}\ninclude = {include_value}\n"
        base_text = postgresql_config.read_text(encoding="utf-8")
        marker_count = base_text.count(_MANAGED_INCLUDE_MARKER)
        if marker_count > 1:
            raise LocalClusterSafetyError("postgresql.conf has duplicate managed includes")
        if marker_count == 1:
            if include_block not in base_text:
                raise LocalClusterSafetyError("postgresql.conf managed include was modified")
            return

        separator = "" if not base_text or base_text.endswith("\n") else "\n"
        with postgresql_config.open("a", encoding="utf-8") as config_file:
            config_file.write(f"{separator}\n{include_block}")

    def _verify_effective_configuration(self) -> None:
        expected = {
            "listen_addresses": "",
            "port": str(POSTGRESQL_PORT),
            "unix_socket_directories": str(self.socket_directory),
            # PostgreSQL reports this integer GUC in decimal even when configured
            # with the conventional octal spelling ``0700``.
            "unix_socket_permissions": str(0o700),
        }
        postgres = self._binary("postgres")
        for setting, expected_value in expected.items():
            completed = self._invoke((postgres, "-D", self.data_directory, "-C", setting))
            actual = completed.stdout.strip()
            if actual != expected_value:
                raise LocalClusterSafetyError(
                    f"unsafe effective PostgreSQL setting {setting}: "
                    f"expected {expected_value!r}, found {actual!r}"
                )

    def _prepare_log_file(self) -> None:
        self._reject_symlink(self.log_file, label="PostgreSQL log")
        descriptor = os.open(
            self.log_file,
            os.O_APPEND | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        os.close(descriptor)
        self.log_file.chmod(0o600)

    def init(self) -> bool:
        """Initialize and securely configure the cluster; return whether it was created."""

        self._verify_installation()
        self._prepare_layout()
        if self._is_initialized():
            self._validate_data_version()
            self._ensure_managed_configuration()
            self._verify_effective_configuration()
            return False

        if any(self.data_directory.iterdir()):
            raise LocalClusterSafetyError(
                f"refusing to initialize a non-empty data directory: {self.data_directory}"
            )

        self._invoke(
            (
                self._binary("initdb"),
                "--pgdata",
                self.data_directory,
                "--auth-local=trust",
                "--auth-host=reject",
                "--encoding=UTF8",
                "--locale=C",
                "--data-checksums",
                "--no-instructions",
            )
        )
        if not self._is_initialized():
            raise LocalClusterError("initdb succeeded without creating PG_VERSION")
        self._validate_data_version()
        self._ensure_managed_configuration()
        self._verify_effective_configuration()
        return True

    def status(self) -> ClusterStatus:
        """Return uninitialized, stopped, or running without changing cluster state."""

        if not self._is_initialized():
            return ClusterStatus(
                state=ClusterState.UNINITIALIZED,
                root=self.root,
                socket_directory=self.socket_directory,
            )
        self._validate_data_version()
        completed = self._invoke(
            (self._binary("pg_ctl"), "--pgdata", self.data_directory, "status"),
            accepted_returncodes=frozenset((0, 3)),
        )
        state = ClusterState.RUNNING if completed.returncode == 0 else ClusterState.STOPPED
        return ClusterStatus(
            state=state,
            root=self.root,
            socket_directory=self.socket_directory,
        )

    def start(self) -> bool:
        """Start the cluster if stopped; return whether this call started it."""

        if not self._is_initialized():
            raise LocalClusterError("local PostgreSQL cluster is not initialized")
        self._verify_installation()
        self._prepare_layout()
        self._validate_data_version()
        self._ensure_managed_configuration()
        self._verify_effective_configuration()
        if self.status().running:
            return False
        self._prepare_log_file()
        self._invoke(
            (
                self._binary("pg_ctl"),
                "--pgdata",
                self.data_directory,
                "--log",
                self.log_file,
                "--wait",
                "start",
            )
        )
        return True

    def stop(self) -> bool:
        """Fast-stop the cluster if running; return whether this call stopped it."""

        current = self.status()
        if not current.initialized or not current.running:
            return False
        self._invoke(
            (
                self._binary("pg_ctl"),
                "--pgdata",
                self.data_directory,
                "--wait",
                "--mode=fast",
                "stop",
            )
        )
        return True

    def connection_url(self, database: str = "postgres") -> str:
        """Return a passwordless libpq URL that uses only the private Unix socket."""

        database_name = self._validate_identifier(database, label="database")
        query = urlencode(
            {"host": str(self.socket_directory), "port": str(POSTGRESQL_PORT)},
            quote_via=quote,
        )
        return f"postgresql:///{quote(database_name, safe='')}?{query}"
