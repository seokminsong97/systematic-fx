import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

from systematic_fx.db.local_cluster import (
    POSTGRESQL_PORT,
    ClusterState,
    LocalClusterError,
    LocalClusterSafetyError,
    LocalPostgresCluster,
)


def _completed(
    stdout: str = "",
    *,
    returncode: int = 0,
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], returncode, stdout=stdout, stderr=stderr)


class LocalPostgresClusterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.workspace = Path(self.temporary_directory.name) / "workspace"
        self.workspace.mkdir()
        self.root = self.workspace / ".local" / "postgres"
        self.bin_directory = self.workspace / "postgres-bin"
        self.bin_directory.mkdir()
        for name in ("initdb", "pg_ctl", "postgres"):
            binary = self.bin_directory / name
            binary.touch(mode=0o700)
            binary.chmod(0o700)
        self.cluster = LocalPostgresCluster(
            self.root,
            bin_directory=self.bin_directory,
        )

    def _seed_data_directory(self, *, version: str = "18") -> None:
        self.cluster.data_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        (self.cluster.data_directory / "PG_VERSION").write_text(
            f"{version}\n",
            encoding="ascii",
        )
        (self.cluster.data_directory / "postgresql.conf").write_text(
            "# initialized PostgreSQL configuration\n",
            encoding="utf-8",
        )

    @staticmethod
    def _setting_result(arguments: list[str]) -> subprocess.CompletedProcess[str]:
        setting = arguments[-1]
        values = {
            "listen_addresses": "\n",
            "port": f"{POSTGRESQL_PORT}\n",
            "unix_socket_permissions": f"{0o700}\n",
        }
        if setting == "unix_socket_directories":
            raise AssertionError("socket result needs the cluster path")
        return _completed(values[setting])

    @patch("systematic_fx.db.local_cluster.subprocess.run")
    def test_init_creates_private_postgres_18_cluster_idempotently(self, run: Mock) -> None:
        def execute(arguments: list[str], **_kwargs):
            executable = Path(arguments[0]).name
            if executable == "postgres" and arguments[-1] == "--version":
                return _completed("postgres (PostgreSQL) 18.4\n")
            if executable == "initdb":
                self._seed_data_directory()
                return _completed()
            if executable == "postgres" and "-C" in arguments:
                setting = arguments[-1]
                if setting == "unix_socket_directories":
                    return _completed(f"{self.cluster.socket_directory}\n")
                return self._setting_result(arguments)
            raise AssertionError(f"unexpected command: {arguments}")

        run.side_effect = execute

        self.assertTrue(self.cluster.init())
        self.assertFalse(self.cluster.init())

        init_calls = [
            invocation.args[0]
            for invocation in run.call_args_list
            if Path(invocation.args[0][0]).name == "initdb"
        ]
        self.assertEqual(len(init_calls), 1)
        init_arguments = init_calls[0]
        self.assertIn("--auth-local=trust", init_arguments)
        self.assertIn("--auth-host=reject", init_arguments)
        self.assertIn("--data-checksums", init_arguments)
        self.assertNotIn("5432", " ".join(init_arguments))
        for invocation in run.call_args_list:
            self.assertIsInstance(invocation.args[0], list)
            self.assertNotIn("shell", invocation.kwargs)

        managed_config = self.cluster.managed_config.read_text(encoding="utf-8")
        self.assertIn("listen_addresses = ''", managed_config)
        self.assertIn(f"port = {POSTGRESQL_PORT}", managed_config)
        self.assertIn(
            f"unix_socket_directories = '{self.cluster.socket_directory}'",
            managed_config,
        )
        self.assertIn("unix_socket_permissions = 0700", managed_config)
        postgresql_config = (self.cluster.data_directory / "postgresql.conf").read_text(
            encoding="utf-8"
        )
        self.assertEqual(
            postgresql_config.count("# systematic-fx local cluster: managed include"),
            1,
        )
        for directory in (
            self.cluster.root,
            self.cluster.data_directory,
            self.cluster.socket_directory,
            self.cluster.log_directory,
        ):
            self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(self.cluster.managed_config.stat().st_mode), 0o600)

    def test_status_reports_uninitialized_without_running_a_command(self) -> None:
        with patch("systematic_fx.db.local_cluster.subprocess.run") as run:
            status = self.cluster.status()

        self.assertEqual(status.state, ClusterState.UNINITIALIZED)
        self.assertFalse(status.initialized)
        self.assertFalse(status.running)
        run.assert_not_called()

    @patch("systematic_fx.db.local_cluster.subprocess.run")
    def test_status_distinguishes_stopped_and_running(self, run: Mock) -> None:
        self._seed_data_directory()
        run.side_effect = [_completed(returncode=3), _completed()]

        stopped = self.cluster.status()
        running = self.cluster.status()

        self.assertEqual(stopped.state, ClusterState.STOPPED)
        self.assertEqual(running.state, ClusterState.RUNNING)

    @patch("systematic_fx.db.local_cluster.subprocess.run")
    def test_start_validates_effective_settings_and_logs_below_root(self, run: Mock) -> None:
        self._seed_data_directory()

        def execute(arguments: list[str], **_kwargs):
            executable = Path(arguments[0]).name
            if executable == "postgres" and arguments[-1] == "--version":
                return _completed("postgres (PostgreSQL) 18.4\n")
            if executable == "pg_ctl" and arguments[-1] == "status":
                return _completed(returncode=3)
            if executable == "postgres" and "-C" in arguments:
                setting = arguments[-1]
                if setting == "unix_socket_directories":
                    return _completed(f"{self.cluster.socket_directory}\n")
                return self._setting_result(arguments)
            if executable == "pg_ctl" and arguments[-1] == "start":
                return _completed()
            raise AssertionError(f"unexpected command: {arguments}")

        run.side_effect = execute

        self.assertTrue(self.cluster.start())

        start_arguments = run.call_args_list[-1].args[0]
        self.assertEqual(Path(start_arguments[0]).name, "pg_ctl")
        self.assertEqual(start_arguments[-1], "start")
        self.assertEqual(
            start_arguments[start_arguments.index("--log") + 1],
            str(self.cluster.log_file),
        )
        self.assertTrue(self.cluster.log_file.is_relative_to(self.cluster.root))
        self.assertEqual(stat.S_IMODE(self.cluster.log_file.stat().st_mode), 0o600)
        self.assertNotIn("localhost", " ".join(start_arguments))
        self.assertNotIn("5432", " ".join(start_arguments))

    @patch("systematic_fx.db.local_cluster.subprocess.run")
    def test_start_refuses_an_effective_tcp_listener(self, run: Mock) -> None:
        self._seed_data_directory()

        def execute(arguments: list[str], **_kwargs):
            executable = Path(arguments[0]).name
            if executable == "postgres" and arguments[-1] == "--version":
                return _completed("postgres (PostgreSQL) 18.4\n")
            if executable == "pg_ctl":
                return _completed(returncode=3)
            if executable == "postgres" and arguments[-1] == "listen_addresses":
                return _completed("localhost\n")
            raise AssertionError(f"unexpected command: {arguments}")

        run.side_effect = execute

        with self.assertRaisesRegex(LocalClusterSafetyError, "listen_addresses"):
            self.cluster.start()

        commands = [invocation.args[0] for invocation in run.call_args_list]
        self.assertFalse(any(command[-1] == "start" for command in commands))

    @patch("systematic_fx.db.local_cluster.subprocess.run")
    def test_repeated_start_does_not_start_again(self, run: Mock) -> None:
        self._seed_data_directory()

        def execute(arguments: list[str], **_kwargs):
            executable = Path(arguments[0]).name
            if executable == "postgres" and arguments[-1] == "--version":
                return _completed("postgres (PostgreSQL) 18.4\n")
            if executable == "postgres" and "-C" in arguments:
                setting = arguments[-1]
                if setting == "unix_socket_directories":
                    return _completed(f"{self.cluster.socket_directory}\n")
                return self._setting_result(arguments)
            if executable == "pg_ctl" and arguments[-1] == "status":
                return _completed()
            raise AssertionError(f"unexpected command: {arguments}")

        run.side_effect = execute

        self.assertFalse(self.cluster.start())
        commands = [invocation.args[0] for invocation in run.call_args_list]
        self.assertFalse(any(command[-1] == "start" for command in commands))

    @patch("systematic_fx.db.local_cluster.subprocess.run")
    def test_stop_is_idempotent_and_uses_fast_mode(self, run: Mock) -> None:
        self._seed_data_directory()
        run.side_effect = [
            _completed(),
            _completed(),
            _completed(returncode=3),
        ]

        self.assertTrue(self.cluster.stop())
        self.assertFalse(self.cluster.stop())

        stop_arguments = run.call_args_list[1].args[0]
        self.assertEqual(Path(stop_arguments[0]).name, "pg_ctl")
        self.assertIn("--mode=fast", stop_arguments)
        self.assertEqual(stop_arguments[-1], "stop")

    @patch("systematic_fx.db.local_cluster.subprocess.run")
    def test_rejects_wrong_server_and_data_major_versions(self, run: Mock) -> None:
        run.return_value = _completed("postgres (PostgreSQL) 17.8\n")
        with self.assertRaisesRegex(LocalClusterError, "found major 17"):
            self.cluster.init()

        self._seed_data_directory(version="17")
        with self.assertRaisesRegex(LocalClusterError, "expected 18"):
            self.cluster.status()

    def test_connection_url_uses_only_private_socket_and_fixed_port(self) -> None:
        connection_url = self.cluster.connection_url("systematic_fx")
        parsed = urlsplit(connection_url)
        query = parse_qs(parsed.query)

        self.assertEqual(parsed.scheme, "postgresql")
        self.assertEqual(parsed.path, "/systematic_fx")
        self.assertEqual(query["host"], [str(self.cluster.socket_directory)])
        self.assertEqual(query["port"], [str(POSTGRESQL_PORT)])
        self.assertNotEqual(query["port"], ["5432"])
        self.assertNotIn("localhost", connection_url)
        self.assertIsNone(parsed.password)

        with self.assertRaises(LocalClusterSafetyError):
            self.cluster.connection_url("postgres?host=localhost")

    def test_rejects_broad_relative_home_and_misdirected_roots(self) -> None:
        unsafe_roots = (
            Path(".local/postgres"),
            Path("/"),
            Path.home(),
            Path.home() / ".local" / "postgres",
            self.workspace,
            self.workspace / "postgres",
        )

        for unsafe_root in unsafe_roots:
            with (
                self.subTest(unsafe_root=unsafe_root),
                self.assertRaises(LocalClusterSafetyError),
            ):
                LocalPostgresCluster(unsafe_root, bin_directory=self.bin_directory)

    def test_rejects_symlinked_managed_directory(self) -> None:
        outside = self.workspace / "outside"
        outside.mkdir()
        self.root.parent.mkdir()
        os.symlink(outside, self.root)

        with self.assertRaises(LocalClusterSafetyError):
            LocalPostgresCluster(self.root, bin_directory=self.bin_directory)

    def test_status_refuses_data_directory_symlink_created_after_construction(self) -> None:
        outside = self.workspace / "outside-data"
        outside.mkdir()
        (outside / "PG_VERSION").write_text("18\n", encoding="ascii")
        self.root.mkdir(parents=True)
        os.symlink(outside, self.cluster.data_directory)

        with (
            patch("systematic_fx.db.local_cluster.subprocess.run") as run,
            self.assertRaisesRegex(LocalClusterSafetyError, "data directory"),
        ):
            self.cluster.status()

        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
