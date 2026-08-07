import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from systematic_fx import cli
from systematic_fx.config import Settings
from systematic_fx.db import research_registry


class ExposureCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = Settings(
            data_root=Path("/tmp/systematic-fx-exposure-data"),
            artifacts_root=Path("/tmp/systematic-fx-exposure-artifacts"),
            database_url="postgresql:///systematic_fx",
        )

    @staticmethod
    def _spec() -> dict[str, object]:
        return {
            "campaign_key": "phase1_discovery_v1",
            "code_version": "test-code",
            "config_sha256": "0" * 64,
            "exposure_key": "phase1:test-exposure",
            "exposure_type": "SUMMARY",
            "query_spec": {"performance_fields_requested": False},
            "research_eligible": False,
            "result_summary": {"result": "FAIL"},
            "source_interval_end": "2026-08-01T00:00:00Z",
            "source_interval_start": "2022-01-02T00:00:00Z",
            "visible_to_ai": True,
        }

    @staticmethod
    def _result() -> SimpleNamespace:
        return SimpleNamespace(
            campaign_id=1,
            created_artifact=True,
            created_exposure=True,
            discovery_exposure_id=2,
            exposure_key="phase1:test-exposure",
            research_run_spec_id=None,
            result_artifact_id=3,
        )

    def _invoke(
        self,
        directory: Path,
        spec: dict[str, object],
    ) -> tuple[int, mock.Mock]:
        spec_path = directory / "exposure.json"
        spec_path.write_text(json.dumps(spec), encoding="utf-8")
        arguments = cli.build_parser().parse_args(
            ["research", "exposure", "--spec", str(spec_path), "--json"]
        )
        with (
            mock.patch.object(cli.Settings, "from_env", return_value=self.settings),
            mock.patch.object(
                research_registry,
                "record_discovery_exposure",
                return_value=self._result(),
            ) as register,
            mock.patch("builtins.print"),
        ):
            exit_code = arguments.handler(arguments)
        return exit_code, register

    def test_optional_result_artifact_paths_are_passed_as_paths(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            artifact_root = directory / "data" / "derived"
            artifact_path = artifact_root / "anomaly-detail.jsonl"
            spec = self._spec()
            spec["result_artifact_path"] = str(artifact_path)
            spec["result_artifacts_root"] = str(artifact_root)

            exit_code, register = self._invoke(directory, spec)

        self.assertEqual(exit_code, 0)
        call = register.call_args
        self.assertEqual(call.kwargs["result_artifact_path"], artifact_path)
        self.assertEqual(call.kwargs["artifacts_root"], artifact_root)
        self.assertIsInstance(call.kwargs["result_artifact_path"], Path)
        self.assertIsInstance(call.kwargs["artifacts_root"], Path)

    def test_one_sided_result_artifact_spec_is_rejected_before_registration(self) -> None:
        cases = (
            ("result_artifact_path", "/tmp/anomaly-detail.jsonl"),
            ("result_artifacts_root", "/tmp/data/derived"),
        )
        for field, value in cases:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory_name:
                spec = self._spec()
                spec[field] = value

                exit_code, register = self._invoke(Path(directory_name), spec)

                self.assertEqual(exit_code, 2)
                register.assert_not_called()

    def test_existing_spec_without_result_artifact_remains_compatible(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            exit_code, register = self._invoke(Path(directory_name), self._spec())

        self.assertEqual(exit_code, 0)
        call = register.call_args
        self.assertNotIn("result_artifact_path", call.kwargs)
        self.assertNotIn("artifacts_root", call.kwargs)
        self.assertIsNone(call.kwargs["run_fingerprint"])

    def test_optional_run_fingerprint_is_forwarded(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            spec = self._spec()
            spec["run_fingerprint"] = "a" * 64
            exit_code, register = self._invoke(Path(directory_name), spec)

        self.assertEqual(exit_code, 0)
        self.assertEqual(register.call_args.kwargs["run_fingerprint"], "a" * 64)


if __name__ == "__main__":
    unittest.main()
