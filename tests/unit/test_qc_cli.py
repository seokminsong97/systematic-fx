import unittest
from pathlib import Path
from unittest import mock

from systematic_fx import cli
from systematic_fx.config import Settings
from systematic_fx.data import qc_mutations, quality
from systematic_fx.db import full_qc_registry


class StructuralQcCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = Settings(
            data_root=Path("/tmp/systematic-fx-qc-data"),
            artifacts_root=Path("/tmp/systematic-fx-qc-artifacts"),
            database_url="postgresql:///systematic_fx",
        )

    def test_parser_exposes_scan_and_registration_defaults(self) -> None:
        scan = cli.build_parser().parse_args(["data", "qc"])
        inspect = cli.build_parser().parse_args(["data", "inspect-qc-mutations"])
        register = cli.build_parser().parse_args(["data", "register-qc"])

        self.assertIs(scan.handler, cli._quality_command)
        self.assertEqual(
            scan.config,
            Path("configs/data/mbp10_structural_qc_v1.toml"),
        )
        self.assertEqual(scan.manifest_name, "mbp10_structural_qc_v1.jsonl")
        self.assertEqual(scan.progress_every, 250)
        self.assertIs(inspect.handler, cli._inspect_qc_mutations_command)
        self.assertEqual(
            inspect.output_name,
            "mbp10_clean_trade_none_book_mutations_v1.jsonl",
        )
        self.assertIs(register.handler, cli._register_quality_command)
        self.assertEqual(register.dataset_key, "glbx_mdp3_mbp_10_6e_fut_v1")

    def test_mutation_inspection_uses_derived_defaults(self) -> None:
        result = mock.MagicMock()
        result.as_dict.return_value = {"mutation_count": 1, "status": "COMPLETE"}
        arguments = cli.build_parser().parse_args(["data", "inspect-qc-mutations", "--json"])
        with (
            mock.patch.object(cli.Settings, "from_env", return_value=self.settings),
            mock.patch.object(
                qc_mutations,
                "inspect_qc_mutations",
                return_value=result,
            ) as inspect,
            mock.patch("builtins.print"),
        ):
            exit_code = arguments.handler(arguments)

        self.assertEqual(exit_code, 0)
        inspect.assert_called_once_with(
            self.settings.data_root,
            qc_manifest_path=(
                self.settings.derived_root / "manifests" / "mbp10_structural_qc_v1.jsonl"
            ),
            dataset_root=self.settings.mbp10_root,
            output_name="mbp10_clean_trade_none_book_mutations_v1.jsonl",
        )

    def test_mutation_inspection_error_returns_two(self) -> None:
        arguments = cli.build_parser().parse_args(["data", "inspect-qc-mutations"])
        with (
            mock.patch.object(cli.Settings, "from_env", return_value=self.settings),
            mock.patch.object(
                qc_mutations,
                "inspect_qc_mutations",
                side_effect=qc_mutations.QcMutationInspectionError("unsafe evidence"),
            ),
            mock.patch("builtins.print"),
        ):
            exit_code = arguments.handler(arguments)

        self.assertEqual(exit_code, 2)

    def test_scan_exit_code_reflects_only_structural_failure(self) -> None:
        result = mock.MagicMock(failed_file_count=0)
        result.as_dict.return_value = {"failed_file_count": 0}
        arguments = cli.build_parser().parse_args(["data", "qc", "--json"])
        with (
            mock.patch.object(cli.Settings, "from_env", return_value=self.settings),
            mock.patch.object(quality, "scan_structural_quality", return_value=result) as scan,
            mock.patch("builtins.print"),
        ):
            exit_code = arguments.handler(arguments)

        self.assertEqual(exit_code, 0)
        scan.assert_called_once_with(
            self.settings.data_root,
            config_path=Path("configs/data/mbp10_structural_qc_v1.toml"),
            source_manifest_path=(
                self.settings.derived_root / "manifests" / "mbp10_source_sha256_v1.jsonl"
            ),
            dataset_root=self.settings.mbp10_root,
            manifest_name="mbp10_structural_qc_v1.jsonl",
            checkpoint_name=None,
            progress_callback=mock.ANY,
        )

        result.failed_file_count = 1
        with (
            mock.patch.object(cli.Settings, "from_env", return_value=self.settings),
            mock.patch.object(quality, "scan_structural_quality", return_value=result),
            mock.patch("builtins.print"),
        ):
            exit_code = arguments.handler(arguments)
        self.assertEqual(exit_code, 1)

    def test_scan_execution_error_returns_two(self) -> None:
        arguments = cli.build_parser().parse_args(["data", "qc"])
        with (
            mock.patch.object(cli.Settings, "from_env", return_value=self.settings),
            mock.patch.object(
                quality,
                "scan_structural_quality",
                side_effect=quality.StructuralQcError("unsafe input"),
            ),
            mock.patch("builtins.print"),
        ):
            exit_code = arguments.handler(arguments)

        self.assertEqual(exit_code, 2)

    def test_registration_warn_is_non_gating_and_structural_fail_is_one(self) -> None:
        result = mock.MagicMock(aggregate_result="PASS", diagnostic_result="WARN")
        result.as_dict.return_value = {
            "aggregate_result": "PASS",
            "diagnostic_result": "WARN",
        }
        arguments = cli.build_parser().parse_args(["data", "register-qc", "--json"])
        with (
            mock.patch.object(cli.Settings, "from_env", return_value=self.settings),
            mock.patch.object(
                full_qc_registry,
                "register_full_qc_scan",
                return_value=result,
            ) as register,
            mock.patch("builtins.print"),
        ):
            exit_code = arguments.handler(arguments)

        self.assertEqual(exit_code, 0)
        register.assert_called_once_with(
            self.settings.database_url,
            data_root=self.settings.data_root,
            dataset_key="glbx_mdp3_mbp_10_6e_fut_v1",
            scan_manifest_path=(
                self.settings.derived_root / "manifests" / "mbp10_structural_qc_v1.jsonl"
            ),
            source_manifest_path=(
                self.settings.derived_root / "manifests" / "mbp10_source_sha256_v1.jsonl"
            ),
        )

        result.aggregate_result = "FAIL"
        with (
            mock.patch.object(cli.Settings, "from_env", return_value=self.settings),
            mock.patch.object(
                full_qc_registry,
                "register_full_qc_scan",
                return_value=result,
            ),
            mock.patch("builtins.print"),
        ):
            exit_code = arguments.handler(arguments)
        self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
