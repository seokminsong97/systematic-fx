"""Command-line composition root for local research workflows."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path

from systematic_fx.config import Settings
from systematic_fx.data.inventory import summarize_inventory


def _format_bytes(value: int) -> str:
    amount = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if amount < 1024.0 or unit == "TiB":
            return f"{amount:.2f} {unit}"
        amount /= 1024.0
    raise AssertionError("unreachable")


def _inventory_command(args: argparse.Namespace) -> int:
    settings = Settings.from_env()
    dataset_root = args.root or settings.mbp10_root

    try:
        summary = summarize_inventory(dataset_root)
    except FileNotFoundError as error:
        print(error)
        return 2

    if args.json:
        print(json.dumps(summary.as_dict(), indent=2, sort_keys=True))
    else:
        print(f"dataset_root: {summary.dataset_root}")
        print(f"parquet_files: {summary.file_count}")
        print(f"total_size: {_format_bytes(summary.total_bytes)}")
        print(f"first_source_date: {summary.first_source_date or '-'}")
        print(f"last_source_date: {summary.last_source_date or '-'}")
        print(f"invalid_layout_files: {len(summary.invalid_layout_files)}")

    return 1 if summary.invalid_layout_files else 0


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"invalid ISO date: {value}") from error


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"expected a positive integer: {value}") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError(f"expected a positive integer: {value}")
    return parsed


def _catalog_command(args: argparse.Namespace) -> int:
    from systematic_fx.data.catalog import scan_catalog

    settings = Settings.from_env()
    dataset_root = args.root or settings.mbp10_root
    try:
        summary = scan_catalog(
            dataset_root,
            start_date=args.start_date,
            end_date=args.end_date,
            limit=args.limit,
            manifest_path=args.manifest,
            include_mappings=not args.omit_mappings,
        )
    except (FileNotFoundError, ValueError) as error:
        print(error)
        return 2

    if args.json:
        print(json.dumps(summary.as_dict(), indent=2, sort_keys=True))
    else:
        print(f"dataset_root: {summary.dataset_root}")
        print(f"parquet_files: {summary.file_count}")
        print(f"total_size: {_format_bytes(summary.total_file_bytes)}")
        print(f"total_rows: {summary.total_rows}")
        print(f"total_row_groups: {summary.total_row_groups}")
        print(f"mapping_intervals: {summary.mapping_interval_count}")
        print(f"outright_mappings: {summary.outright_mapping_count}")
        print(f"calendar_spread_mappings: {summary.calendar_spread_mapping_count}")
        print(f"unknown_mappings: {summary.unknown_mapping_count}")
        print(f"unique_instrument_ids: {summary.unique_instrument_count}")
        print(f"schema_fingerprints: {list(summary.schema_fingerprints)}")
        print(f"request_symbols: {list(summary.request_symbols)}")
        print(f"files_with_partial: {summary.files_with_partial}")
        print(f"partial_symbols: {summary.partial_symbol_count}")
        print(f"files_with_not_found: {summary.files_with_not_found}")
        print(f"not_found_symbols: {summary.not_found_symbol_count}")
        print(f"first_source_date: {summary.first_source_date or '-'}")
        print(f"last_source_date: {summary.last_source_date or '-'}")
        print(f"manifest_path: {summary.manifest_path or '-'}")

    structural_failure = (
        summary.unknown_mapping_count > 0
        or summary.not_found_symbol_count > 0
        or len(summary.schema_fingerprints) != 1
    )
    return 1 if structural_failure else 0


def _smoke_command(args: argparse.Namespace) -> int:
    from systematic_fx.data.smoke import SmokeCheckError, smoke_check_parquet

    try:
        result = smoke_check_parquet(args.file, max_row_groups=args.row_groups)
    except (FileNotFoundError, SmokeCheckError, ValueError) as error:
        print(error)
        return 2

    if args.json:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    else:
        for name, value in result.as_dict().items():
            print(f"{name}: {value}")
    return 0 if result.passed else 1


def _hash_command(args: argparse.Namespace) -> int:
    from systematic_fx.data.hashing import (
        HashManifestError,
        HashProgress,
        build_sha256_manifest,
    )

    settings = Settings.from_env()
    dataset_root = args.root or settings.mbp10_root
    footer_manifest = args.footer_manifest or (
        settings.derived_root / "manifests" / "mbp10_footer_manifest_v1.jsonl"
    )

    def report_progress(progress: HashProgress) -> None:
        if args.json:
            return
        should_report = (
            progress.status == "COMPLETE"
            or progress.file_index == 1
            or progress.file_index % args.progress_every == 0
            or progress.file_index == progress.file_count
        )
        if not should_report:
            return
        print(
            f"{progress.status.lower()}: {progress.file_index}/{progress.file_count} "
            f"({_format_bytes(progress.bytes_processed)}/{_format_bytes(progress.total_bytes)})",
            file=sys.stderr,
            flush=True,
        )

    try:
        result = build_sha256_manifest(
            settings.data_root,
            dataset_root=dataset_root,
            footer_manifest=footer_manifest,
            manifest_name=args.manifest_name,
            checkpoint_name=args.checkpoint_name,
            chunk_size_bytes=args.chunk_size_mib * 1024 * 1024,
            progress_callback=report_progress,
        )
    except (FileNotFoundError, NotADirectoryError, HashManifestError, ValueError) as error:
        print(error)
        return 2

    if args.json:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    else:
        for name, value in result.as_dict().items():
            print(f"{name}: {value}")
    return 0


def _quality_command(args: argparse.Namespace) -> int:
    from pyarrow import ArrowException

    from systematic_fx.data.quality import (
        StructuralQcError,
        StructuralQcProgress,
        scan_structural_quality,
    )

    settings = Settings.from_env()
    dataset_root = args.root or settings.mbp10_root
    source_manifest = args.source_manifest or (
        settings.derived_root / "manifests" / "mbp10_source_sha256_v1.jsonl"
    )

    def report_progress(progress: StructuralQcProgress) -> None:
        if args.json:
            return
        should_report = (
            progress.status == "COMPLETE"
            or progress.row_groups_complete == 1
            or progress.row_groups_complete % args.progress_every == 0
        )
        if not should_report:
            return
        row_group = (
            "-"
            if progress.row_group_index is None
            else f"{progress.row_group_index + 1}/{progress.row_groups_in_file}"
        )
        print(
            f"{progress.status.lower()}: file {progress.file_index}/{progress.file_count} "
            f"row_group {row_group} complete={progress.row_groups_complete} "
            f"rows={progress.rows_complete}",
            file=sys.stderr,
            flush=True,
        )

    try:
        result = scan_structural_quality(
            settings.data_root,
            config_path=args.config,
            source_manifest_path=source_manifest,
            dataset_root=dataset_root,
            manifest_name=args.manifest_name,
            checkpoint_name=args.checkpoint_name,
            progress_callback=report_progress,
        )
    except (OSError, ArrowException, StructuralQcError) as error:
        print(error)
        return 2

    if args.json:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    else:
        for name, value in result.as_dict().items():
            print(f"{name}: {value}")
    return 1 if result.failed_file_count > 0 else 0


def _inspect_qc_mutations_command(args: argparse.Namespace) -> int:
    from pyarrow import ArrowException

    from systematic_fx.data.qc_mutations import inspect_qc_mutations

    settings = Settings.from_env()
    dataset_root = args.root or settings.mbp10_root
    qc_manifest = args.qc_manifest or (
        settings.derived_root / "manifests" / "mbp10_structural_qc_v1.jsonl"
    )
    try:
        result = inspect_qc_mutations(
            settings.data_root,
            qc_manifest_path=qc_manifest,
            dataset_root=dataset_root,
            output_name=args.output_name,
        )
    except (OSError, ArrowException, ValueError) as error:
        print(error)
        return 2

    if args.json:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    else:
        for name, value in result.as_dict().items():
            print(f"{name}: {value}")
    return 0


def _pilot_features_command(args: argparse.Namespace) -> int:
    from systematic_fx.features.pilot import (
        PilotBuildError,
        build_pilot_features,
    )

    settings = Settings.from_env()
    try:
        result = build_pilot_features(
            args.file,
            data_root=settings.data_root,
            instrument_id=args.instrument_id,
            symbol=args.symbol,
            source_date=args.source_date,
        )
    except (FileNotFoundError, PilotBuildError, ValueError) as error:
        print(error)
        return 2

    if args.json:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    else:
        for name, value in result.as_dict().items():
            print(f"{name}: {value}")
    return 0


def _register_data_command(args: argparse.Namespace) -> int:
    from systematic_fx.db.data_registry import (
        DataRegistryError,
        DatasetRegistration,
        register_source_manifests,
    )

    settings = Settings.from_env()
    database_url = args.database_url or settings.database_url
    if not database_url:
        print("database URL is required via --database-url or SYSTEMATIC_FX_DATABASE_URL")
        return 2

    footer_manifest = args.footer_manifest or (
        settings.derived_root / "manifests" / "mbp10_footer_manifest_v1.jsonl"
    )
    hash_manifest = args.hash_manifest or (
        settings.derived_root / "manifests" / "mbp10_source_sha256_v1.jsonl"
    )
    dataset = DatasetRegistration(
        dataset_key=args.dataset_key,
        root_uri=settings.mbp10_root.expanduser().resolve().as_uri(),
    )
    try:
        result = register_source_manifests(
            database_url,
            footer_manifest_path=footer_manifest,
            hash_manifest_path=hash_manifest,
            dataset=dataset,
        )
    except DataRegistryError as error:
        print(error)
        return 2

    if args.json:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    else:
        for name, value in result.as_dict().items():
            print(f"{name}: {value}")
    return 0


def _qualify_data_command(args: argparse.Namespace) -> int:
    from systematic_fx.db.qualification_registry import (
        QualificationRegistryError,
        register_source_qualification,
    )

    settings = Settings.from_env()
    database_url = args.database_url or settings.database_url
    if not database_url:
        print("database URL is required via --database-url or SYSTEMATIC_FX_DATABASE_URL")
        return 2
    footer_manifest = args.footer_manifest or (
        settings.derived_root / "manifests" / "mbp10_footer_manifest_v1.jsonl"
    )
    hash_manifest = args.hash_manifest or (
        settings.derived_root / "manifests" / "mbp10_source_sha256_v1.jsonl"
    )
    try:
        result = register_source_qualification(
            database_url,
            data_root=settings.data_root,
            dataset_key=args.dataset_key,
            footer_manifest_path=footer_manifest,
            hash_manifest_path=hash_manifest,
            report_name=args.report_name,
        )
    except (FileNotFoundError, QualificationRegistryError) as error:
        print(error)
        return 2

    if args.json:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    else:
        for name, value in result.as_dict().items():
            print(f"{name}: {value}")
    return 1 if result.overall_status == "BLOCKED" else 0


def _register_quality_command(args: argparse.Namespace) -> int:
    from systematic_fx.db.full_qc_registry import (
        FullQcRegistryError,
        register_full_qc_scan,
    )

    settings = Settings.from_env()
    database_url = args.database_url or settings.database_url
    if not database_url:
        print("database URL is required via --database-url or SYSTEMATIC_FX_DATABASE_URL")
        return 2
    scan_manifest = args.scan_manifest or (
        settings.derived_root / "manifests" / "mbp10_structural_qc_v1.jsonl"
    )
    source_manifest = args.source_manifest or (
        settings.derived_root / "manifests" / "mbp10_source_sha256_v1.jsonl"
    )
    try:
        result = register_full_qc_scan(
            database_url,
            data_root=settings.data_root,
            dataset_key=args.dataset_key,
            scan_manifest_path=scan_manifest,
            source_manifest_path=source_manifest,
        )
    except (OSError, FullQcRegistryError) as error:
        print(error)
        return 2

    if args.json:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    else:
        for name, value in result.as_dict().items():
            print(f"{name}: {value}")
    if result.aggregate_result == "FAIL":
        return 1
    return 0 if result.aggregate_result == "PASS" else 2


def _register_research_command(args: argparse.Namespace) -> int:
    from systematic_fx.db.research_registry import (
        ResearchRegistryError,
        register_parent_hypothesis_bundle,
    )

    settings = Settings.from_env()
    database_url = args.database_url or settings.database_url
    if not database_url:
        print("database URL is required via --database-url or SYSTEMATIC_FX_DATABASE_URL")
        return 2
    try:
        result = register_parent_hypothesis_bundle(
            database_url,
            campaign_config_path=args.campaign_config,
            hypothesis_config_path=args.hypothesis_config,
            cost_config_path=args.cost_config,
            execution_config_path=args.execution_config,
            data_root=settings.data_root,
            artifacts_root=settings.artifacts_root,
            code_commit=args.code_version,
        )
    except ResearchRegistryError as error:
        print(error)
        return 2

    report = {
        "artifact_path": result.artifact_path.as_posix(),
        "artifact_sha256": result.artifact_sha256,
        "campaign_id": result.campaign_id,
        "campaign_key": result.campaign_key,
        "created_artifact": result.created_artifact,
        "created_campaign": result.created_campaign,
        "created_dataset": result.created_dataset,
        "created_experiments": result.created_experiments,
        "created_job": result.created_job,
        "dataset_id": result.dataset_id,
        "dataset_key": result.dataset_key,
        "experiment_count": len(result.experiment_ids),
        "job_id": result.job_id,
    }
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        for name, value in report.items():
            print(f"{name}: {value}")
    return 0


def _parse_datetime(value: object, *, label: str) -> datetime:
    if not isinstance(value, str):
        raise TypeError(f"{label} must be an ISO timestamp string")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise ValueError(f"{label} is not an ISO timestamp: {value}") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{label} must include a UTC offset")
    return parsed


def _record_exposure_command(args: argparse.Namespace) -> int:
    from systematic_fx.db.research_registry import (
        ResearchRegistryError,
        record_discovery_exposure,
    )

    settings = Settings.from_env()
    database_url = args.database_url or settings.database_url
    if not database_url:
        print("database URL is required via --database-url or SYSTEMATIC_FX_DATABASE_URL")
        return 2
    try:
        with args.spec.expanduser().open("r", encoding="utf-8") as handle:
            spec = json.load(handle)
        if not isinstance(spec, dict):
            raise TypeError("exposure spec must be a JSON object")
        has_result_artifact = "result_artifact_path" in spec
        has_result_artifacts_root = "result_artifacts_root" in spec
        if has_result_artifact != has_result_artifacts_root:
            raise ValueError(
                "result_artifact_path and result_artifacts_root must be provided together"
            )
        result_artifact_arguments = {}
        if has_result_artifact:
            result_artifact_arguments = {
                "result_artifact_path": Path(spec["result_artifact_path"]),
                "artifacts_root": Path(spec["result_artifacts_root"]),
            }
        result = record_discovery_exposure(
            database_url,
            campaign_key=spec["campaign_key"],
            exposure_key=spec["exposure_key"],
            exposure_type=spec["exposure_type"],
            source_interval_start=_parse_datetime(
                spec["source_interval_start"],
                label="source_interval_start",
            ),
            source_interval_end=_parse_datetime(
                spec["source_interval_end"],
                label="source_interval_end",
            ),
            query_spec=spec["query_spec"],
            result_summary=spec["result_summary"],
            visible_to_ai=spec["visible_to_ai"],
            research_eligible=spec["research_eligible"],
            code_commit=spec["code_version"],
            config_sha256=spec["config_sha256"],
            **result_artifact_arguments,
        )
    except (FileNotFoundError, KeyError, TypeError, json.JSONDecodeError, ValueError) as error:
        print(f"invalid exposure spec: {error}")
        return 2
    except ResearchRegistryError as error:
        print(error)
        return 2

    report = {
        "campaign_id": result.campaign_id,
        "created_artifact": result.created_artifact,
        "created_exposure": result.created_exposure,
        "discovery_exposure_id": result.discovery_exposure_id,
        "exposure_key": result.exposure_key,
        "result_artifact_id": result.result_artifact_id,
    }
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        for name, value in report.items():
            print(f"{name}: {value}")
    return 0


def _register_pilot_lineage_command(args: argparse.Namespace) -> int:
    from systematic_fx.db.derived_registry import (
        DerivedRegistryError,
        register_pilot_build_report,
    )
    from systematic_fx.features.pilot import ArtifactReport, PilotBuildReport

    settings = Settings.from_env()
    database_url = args.database_url or settings.database_url
    if not database_url:
        print("database URL is required via --database-url or SYSTEMATIC_FX_DATABASE_URL")
        return 2
    try:
        with args.report.expanduser().open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
        if not isinstance(raw, dict):
            raise TypeError("pilot report must be a JSON object")
        one_second = ArtifactReport(**raw["one_second"])
        five_minute = ArtifactReport(**raw["five_minute"])
        report_fields = {
            key: value for key, value in raw.items() if key not in {"one_second", "five_minute"}
        }
        report = PilotBuildReport(
            **report_fields,
            one_second=one_second,
            five_minute=five_minute,
        )
        result = register_pilot_build_report(
            database_url,
            data_root=settings.data_root,
            dataset_key=args.dataset_key,
            source_relative_uri=args.source_relative_uri,
            report=report,
            config_sha256=args.config_sha256,
            code_commit=args.code_version,
            source_manifest_sha256=args.source_manifest_sha256,
        )
    except (FileNotFoundError, KeyError, TypeError, json.JSONDecodeError) as error:
        print(f"invalid pilot report: {error}")
        return 2
    except DerivedRegistryError as error:
        print(error)
        return 2

    if args.json:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    else:
        for name, value in result.as_dict().items():
            print(f"{name}: {value}")
    return 0


def _migrate_command(args: argparse.Namespace) -> int:
    from systematic_fx.db.migrations import MigrationError, apply_migrations

    settings = Settings.from_env()
    database_url = args.database_url or settings.database_url
    if not database_url:
        print("database URL is required via --database-url or SYSTEMATIC_FX_DATABASE_URL")
        return 2

    try:
        report = apply_migrations(database_url, directory=args.migrations_dir)
    except MigrationError as error:
        print(error)
        return 2

    print(f"applied_migrations: {list(report.applied)}")
    print(f"skipped_migrations: {list(report.skipped)}")
    return 0


def _local_cluster():
    from systematic_fx.db.local_cluster import LocalPostgresCluster

    Settings.from_env()
    root = (
        Path(
            os.environ.get(
                "SYSTEMATIC_FX_LOCAL_PG_ROOT",
                str(Path.cwd() / ".local" / "postgres"),
            )
        )
        .expanduser()
        .resolve()
    )
    bin_directory = (
        Path(os.environ.get("SYSTEMATIC_FX_PG_BIN", "/Library/PostgreSQL/18/bin"))
        .expanduser()
        .resolve()
    )
    return LocalPostgresCluster(root, bin_directory=bin_directory)


def _local_database_command(args: argparse.Namespace) -> int:
    from systematic_fx.db.local_cluster import LocalClusterError

    try:
        cluster = _local_cluster()
        if args.local_action == "init":
            changed = cluster.init()
            print(f"initialized_now: {changed}")
        elif args.local_action == "start":
            cluster.init()
            changed = cluster.start()
            print(f"started_now: {changed}")
        elif args.local_action == "stop":
            changed = cluster.stop()
            print(f"stopped_now: {changed}")
        status = cluster.status()
    except LocalClusterError as error:
        print(error)
        return 2

    print(f"state: {status.state.value}")
    print(f"socket_directory: {status.socket_directory}")
    print(f"port: {status.port}")
    return 0


def _bootstrap_command(args: argparse.Namespace) -> int:
    from systematic_fx.db.bootstrap import DatabaseBootstrapError, bootstrap_database

    settings = Settings.from_env()
    admin_database_url = args.admin_database_url or os.environ.get(
        "SYSTEMATIC_FX_ADMIN_DATABASE_URL"
    )
    application_database_url = args.database_url or settings.database_url
    if not admin_database_url or not application_database_url:
        print("both SYSTEMATIC_FX_ADMIN_DATABASE_URL and SYSTEMATIC_FX_DATABASE_URL are required")
        return 2

    try:
        report = bootstrap_database(
            admin_database_url,
            application_database_url,
            owner_role=args.owner_role,
            migrations_directory=args.migrations_dir,
        )
    except DatabaseBootstrapError as error:
        print(error)
        return 2

    print(f"database: {report.database_name}")
    print(f"owner: {report.database_owner}")
    print(f"created_now: {report.created_database}")
    print(f"applied_migrations: {list(report.migrations.applied)}")
    print(f"skipped_migrations: {list(report.migrations.skipped)}")
    return 0


def _bootstrap_test_command(args: argparse.Namespace) -> int:
    from systematic_fx.db.bootstrap import (
        DatabaseBootstrapError,
        bootstrap_test_database,
    )

    Settings.from_env()
    admin_database_url = args.admin_database_url or os.environ.get(
        "SYSTEMATIC_FX_ADMIN_DATABASE_URL"
    )
    test_database_url = args.database_url or os.environ.get("SYSTEMATIC_FX_TEST_DATABASE_URL")
    if not admin_database_url or not test_database_url:
        print(
            "both SYSTEMATIC_FX_ADMIN_DATABASE_URL and SYSTEMATIC_FX_TEST_DATABASE_URL are required"
        )
        return 2

    try:
        report = bootstrap_test_database(
            admin_database_url,
            test_database_url,
            owner_role=args.owner_role,
            migrations_directory=args.migrations_dir,
        )
    except DatabaseBootstrapError as error:
        print(error)
        return 2

    print(f"database: {report.database_name}")
    print(f"owner: {report.database_owner}")
    print(f"created_now: {report.created_database}")
    print(f"applied_migrations: {list(report.migrations.applied)}")
    print(f"skipped_migrations: {list(report.migrations.skipped)}")
    return 0


def _doctor_command(args: argparse.Namespace) -> int:
    from systematic_fx.environment import check_environment

    report = check_environment(
        Settings.from_env(),
        require_database=args.require_database,
    )
    if args.json:
        print(json.dumps(report.as_dict(), indent=2, sort_keys=True))
    else:
        for check in report.checks:
            version = f" ({check.version})" if check.version else ""
            print(f"{check.status.upper():7} {check.name}{version}: {check.detail}")
        print(f"ready: {report.passed}")
        print(f"required_failures: {len(report.required_failures)}")
        print(f"warnings: {len(report.warnings)}")
    return 0 if report.passed else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="systematic-fx",
        description="CME 6E research and deterministic backtesting tools",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    doctor_parser = commands.add_parser(
        "doctor",
        help="verify the local research runtime and storage prerequisites",
    )
    doctor_parser.add_argument(
        "--require-database",
        action="store_true",
        help="fail unless the configured PostgreSQL database is reachable",
    )
    doctor_parser.add_argument("--json", action="store_true", help="emit JSON")
    doctor_parser.set_defaults(handler=_doctor_command)

    data_parser = commands.add_parser("data", help="inspect and prepare market data")
    data_commands = data_parser.add_subparsers(dest="data_command", required=True)

    inventory_parser = data_commands.add_parser(
        "inventory",
        help="summarize daily MBP-10 Parquet files without loading event rows",
    )
    inventory_parser.add_argument(
        "--root",
        type=Path,
        help="MBP-10 dataset root (default: $SYSTEMATIC_FX_DATA_ROOT/mbp-10)",
    )
    inventory_parser.add_argument("--json", action="store_true", help="emit JSON")
    inventory_parser.set_defaults(handler=_inventory_command)

    catalog_parser = data_commands.add_parser(
        "catalog",
        help="validate Parquet footers and optionally write a deterministic manifest",
    )
    catalog_parser.add_argument(
        "--root",
        type=Path,
        help="MBP-10 dataset root (default: $SYSTEMATIC_FX_DATA_ROOT/mbp-10)",
    )
    catalog_parser.add_argument("--start-date", type=_iso_date)
    catalog_parser.add_argument("--end-date", type=_iso_date)
    catalog_parser.add_argument("--limit", type=int)
    catalog_parser.add_argument("--manifest", type=Path)
    catalog_parser.add_argument(
        "--omit-mappings",
        action="store_true",
        help="exclude per-instrument mappings from the JSONL manifest",
    )
    catalog_parser.add_argument("--json", action="store_true", help="emit JSON summary")
    catalog_parser.set_defaults(handler=_catalog_command)

    smoke_parser = data_commands.add_parser(
        "smoke",
        help="run bounded structural checks against event rows",
    )
    smoke_parser.add_argument("file", type=Path)
    smoke_parser.add_argument("--row-groups", type=int, default=1)
    smoke_parser.add_argument("--json", action="store_true", help="emit JSON")
    smoke_parser.set_defaults(handler=_smoke_command)

    hash_parser = data_commands.add_parser(
        "hash",
        help="build a resumable full-content SHA-256 manifest below data/derived",
    )
    hash_parser.add_argument(
        "--root",
        type=Path,
        help="MBP-10 dataset root (default: $SYSTEMATIC_FX_DATA_ROOT/mbp-10)",
    )
    hash_parser.add_argument(
        "--footer-manifest",
        type=Path,
        help="validated footer manifest (default: data/derived/manifests/...)",
    )
    hash_parser.add_argument(
        "--manifest-name",
        default="mbp10_source_sha256_v1.jsonl",
        help="output filename below data/derived/manifests",
    )
    hash_parser.add_argument(
        "--checkpoint-name",
        help="checkpoint filename below data/derived/manifests",
    )
    hash_parser.add_argument("--chunk-size-mib", type=_positive_int, default=8)
    hash_parser.add_argument("--progress-every", type=_positive_int, default=25)
    hash_parser.add_argument("--json", action="store_true", help="emit final JSON report")
    hash_parser.set_defaults(handler=_hash_command)

    quality_parser = data_commands.add_parser(
        "qc",
        help="run the resumable every-row-group MBP-10 structural scan",
    )
    quality_parser.add_argument(
        "--root",
        type=Path,
        help="MBP-10 dataset root (default: $SYSTEMATIC_FX_DATA_ROOT/mbp-10)",
    )
    quality_parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/data/mbp10_structural_qc_v1.toml"),
        help="frozen structural-QC config",
    )
    quality_parser.add_argument(
        "--source-manifest",
        type=Path,
        help="full-content source manifest (default: data/derived/manifests/...)",
    )
    quality_parser.add_argument(
        "--manifest-name",
        default="mbp10_structural_qc_v1.jsonl",
        help="final filename below data/derived/manifests",
    )
    quality_parser.add_argument(
        "--checkpoint-name",
        help="restart checkpoint filename below data/derived/manifests",
    )
    quality_parser.add_argument("--progress-every", type=_positive_int, default=250)
    quality_parser.add_argument("--json", action="store_true", help="emit final JSON report")
    quality_parser.set_defaults(handler=_quality_command)

    inspect_qc_mutations_parser = data_commands.add_parser(
        "inspect-qc-mutations",
        help="replay failed QC sources into deterministic row-level mutation evidence",
    )
    inspect_qc_mutations_parser.add_argument(
        "--root",
        type=Path,
        help="MBP-10 dataset root (default: $SYSTEMATIC_FX_DATA_ROOT/mbp-10)",
    )
    inspect_qc_mutations_parser.add_argument(
        "--qc-manifest",
        type=Path,
        help="final structural-QC manifest (default: data/derived/manifests/...)",
    )
    inspect_qc_mutations_parser.add_argument(
        "--output-name",
        default="mbp10_clean_trade_none_book_mutations_v1.jsonl",
        help="immutable output filename below data/derived/manifests",
    )
    inspect_qc_mutations_parser.add_argument(
        "--json", action="store_true", help="emit final JSON report"
    )
    inspect_qc_mutations_parser.set_defaults(handler=_inspect_qc_mutations_command)

    register_data_parser = data_commands.add_parser(
        "register",
        help="atomically register paired footer/content manifests in PostgreSQL",
    )
    register_data_parser.add_argument(
        "--dataset-key",
        default="glbx_mdp3_mbp_10_6e_fut_v1",
    )
    register_data_parser.add_argument("--footer-manifest", type=Path)
    register_data_parser.add_argument("--hash-manifest", type=Path)
    register_data_parser.add_argument("--database-url")
    register_data_parser.add_argument("--json", action="store_true", help="emit JSON")
    register_data_parser.set_defaults(handler=_register_data_command)

    qualify_data_parser = data_commands.add_parser(
        "qualify",
        help="register bounded source qualification checks and canonical evidence",
    )
    qualify_data_parser.add_argument(
        "--dataset-key",
        default="glbx_mdp3_mbp_10_6e_fut_v1",
    )
    qualify_data_parser.add_argument("--footer-manifest", type=Path)
    qualify_data_parser.add_argument("--hash-manifest", type=Path)
    qualify_data_parser.add_argument(
        "--report-name",
        default="mbp10_source_qualification_v1.json",
    )
    qualify_data_parser.add_argument("--database-url")
    qualify_data_parser.add_argument("--json", action="store_true", help="emit JSON")
    qualify_data_parser.set_defaults(handler=_qualify_data_command)

    register_quality_parser = data_commands.add_parser(
        "register-qc",
        help="register a completed structural-QC manifest without changing data status",
    )
    register_quality_parser.add_argument(
        "--dataset-key",
        default="glbx_mdp3_mbp_10_6e_fut_v1",
    )
    register_quality_parser.add_argument("--scan-manifest", type=Path)
    register_quality_parser.add_argument("--source-manifest", type=Path)
    register_quality_parser.add_argument("--database-url")
    register_quality_parser.add_argument("--json", action="store_true", help="emit JSON")
    register_quality_parser.set_defaults(handler=_register_quality_command)

    features_parser = commands.add_parser(
        "features",
        help="build versioned feature partitions below data/derived",
    )
    features_commands = features_parser.add_subparsers(
        dest="features_command",
        required=True,
    )
    pilot_parser = features_commands.add_parser(
        "pilot",
        help="build an explicit, non-research one-day 1s/5m pipeline pilot",
    )
    pilot_parser.add_argument("file", type=Path, help="one explicit raw MBP-10 Parquet file")
    pilot_parser.add_argument("--instrument-id", type=_positive_int, required=True)
    pilot_parser.add_argument("--symbol", required=True, help="explicit outright raw symbol")
    pilot_parser.add_argument("--source-date", type=_iso_date, required=True)
    pilot_parser.add_argument("--json", action="store_true", help="emit JSON report")
    pilot_parser.set_defaults(handler=_pilot_features_command)

    pilot_lineage_parser = features_commands.add_parser(
        "register-pilot",
        help="verify and register a non-research pilot build and its source lineage",
    )
    pilot_lineage_parser.add_argument("--report", type=Path, required=True)
    pilot_lineage_parser.add_argument(
        "--dataset-key",
        default="glbx_mdp3_mbp_10_6e_fut_v1",
    )
    pilot_lineage_parser.add_argument("--source-relative-uri", required=True)
    pilot_lineage_parser.add_argument("--config-sha256", required=True)
    pilot_lineage_parser.add_argument("--code-version", required=True)
    pilot_lineage_parser.add_argument("--source-manifest-sha256", required=True)
    pilot_lineage_parser.add_argument("--database-url")
    pilot_lineage_parser.add_argument("--json", action="store_true", help="emit JSON")
    pilot_lineage_parser.set_defaults(handler=_register_pilot_lineage_command)

    research_parser = commands.add_parser(
        "research",
        help="register governed research state and Discovery exposure",
    )
    research_commands = research_parser.add_subparsers(
        dest="research_command",
        required=True,
    )
    register_research_parser = research_commands.add_parser(
        "register",
        help="register the DRAFT campaign and 60 a-priori parent experiments",
    )
    register_research_parser.add_argument(
        "--campaign-config",
        type=Path,
        default=Path("configs/campaigns/phase1_discovery_v1.toml"),
    )
    register_research_parser.add_argument(
        "--hypothesis-config",
        type=Path,
        default=Path("configs/research/phase1_parent_hypotheses_v1.toml"),
    )
    register_research_parser.add_argument(
        "--cost-config",
        type=Path,
        default=Path("configs/costs/cost_pending_v1.toml"),
    )
    register_research_parser.add_argument(
        "--execution-config",
        type=Path,
        default=Path("configs/execution/execution_pending_v1.toml"),
    )
    register_research_parser.add_argument("--code-version", required=True)
    register_research_parser.add_argument("--database-url")
    register_research_parser.add_argument("--json", action="store_true", help="emit JSON")
    register_research_parser.set_defaults(handler=_register_research_command)

    exposure_parser = research_commands.add_parser(
        "exposure",
        help="record one immutable AI-visible query or non-research pipeline pilot",
    )
    exposure_parser.add_argument("--spec", type=Path, required=True)
    exposure_parser.add_argument("--database-url")
    exposure_parser.add_argument("--json", action="store_true", help="emit JSON")
    exposure_parser.set_defaults(handler=_record_exposure_command)

    db_parser = commands.add_parser("db", help="manage the PostgreSQL control plane")
    db_commands = db_parser.add_subparsers(dest="db_command", required=True)
    migrate_parser = db_commands.add_parser(
        "migrate",
        help="apply checksum-verified ordered SQL migrations",
    )
    migrate_parser.add_argument("--database-url")
    migrate_parser.add_argument("--migrations-dir", type=Path)
    migrate_parser.set_defaults(handler=_migrate_command)

    local_parser = db_commands.add_parser(
        "local",
        help="manage the private workspace PostgreSQL 18 cluster",
    )
    local_parser.add_argument(
        "local_action",
        choices=("init", "start", "stop", "status"),
    )
    local_parser.set_defaults(handler=_local_database_command)

    bootstrap_parser = db_commands.add_parser(
        "bootstrap",
        help="create systematic_fx if needed and apply migrations",
    )
    bootstrap_parser.add_argument("--admin-database-url")
    bootstrap_parser.add_argument("--database-url")
    bootstrap_parser.add_argument("--owner-role")
    bootstrap_parser.add_argument("--migrations-dir", type=Path)
    bootstrap_parser.set_defaults(handler=_bootstrap_command)

    bootstrap_test_parser = db_commands.add_parser(
        "bootstrap-test",
        help="create isolated systematic_fx_test if needed and apply migrations",
    )
    bootstrap_test_parser.add_argument("--admin-database-url")
    bootstrap_test_parser.add_argument("--database-url")
    bootstrap_test_parser.add_argument("--owner-role")
    bootstrap_test_parser.add_argument("--migrations-dir", type=Path)
    bootstrap_test_parser.set_defaults(handler=_bootstrap_test_command)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.handler(args))
