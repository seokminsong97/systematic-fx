"""Command-line composition root for local research workflows."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from pathlib import Path
from time import sleep

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


def _nonnegative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"expected a non-negative integer: {value}") from error
    if parsed < 0:
        raise argparse.ArgumentTypeError(f"expected a non-negative integer: {value}")
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
            run_fingerprint=spec.get("run_fingerprint"),
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
        "research_run_spec_id": result.research_run_spec_id,
        "result_artifact_id": result.result_artifact_id,
    }
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        for name, value in report.items():
            print(f"{name}: {value}")
    return 0


def _phase1a_slice_command(args: argparse.Namespace) -> int:
    from systematic_fx.research.phase1a_pipeline import (
        Phase1APipelineError,
        run_phase1a_discovery_slice,
    )

    settings = Settings.from_env()
    database_url = args.database_url or settings.database_url
    if not database_url:
        print("database URL is required via --database-url or SYSTEMATIC_FX_DATABASE_URL")
        return 2
    try:
        report = run_phase1a_discovery_slice(
            project_root=Path.cwd(),
            data_root=settings.data_root,
            database_url=database_url,
            slice_index=args.slice_index,
        )
    except Phase1APipelineError as error:
        print(error)
        return 2

    payload = report.as_dict()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for name, value in payload.items():
            print(f"{name}: {value}")
    return 0


def _phase1a_outcomes_command(args: argparse.Namespace, *, runner_name: str) -> int:
    from systematic_fx.research import phase1a_outcome_pipeline

    Phase1AOutcomePipelineError = phase1a_outcome_pipeline.Phase1AOutcomePipelineError
    runner = getattr(phase1a_outcome_pipeline, runner_name)

    settings = Settings.from_env()
    database_url = args.database_url or settings.database_url
    if not database_url:
        print("database URL is required via --database-url or SYSTEMATIC_FX_DATABASE_URL")
        return 2
    mode = "PLAN_ONLY" if args.plan_only else "CACHE_ONLY" if args.cache_only else "RUN"

    def report_progress(progress: object) -> None:
        payload = progress.as_dict()
        stage = payload["stage"]
        completed = int(payload["completed"])
        total = int(payload["total"])
        if stage == "CACHE" and completed not in {1, total} and completed % 10 != 0:
            return
        if stage == "CACHE":
            message = (
                f"cache: {completed}/{total} "
                f"created={payload['cache_created_count']} "
                f"reused={payload['cache_reused_count']}"
            )
            if payload["source_date"] is not None:
                message += f" date={payload['source_date']} symbol={payload['raw_symbol']}"
        else:
            message = (
                f"checkpoint: {completed}/{total} date={payload['source_date']} "
                f"events={payload['source_event_count']} "
                f"detail_rows={payload['detail_record_count']}"
            )
        print(message, file=sys.stderr, flush=True)

    try:
        report = runner(
            project_root=Path.cwd(),
            data_root=settings.data_root,
            database_url=database_url,
            mode=mode,
            max_cache_workers=args.max_cache_workers,
            progress_callback=report_progress,
        )
    except Phase1AOutcomePipelineError as error:
        print(error)
        return 2

    payload = report.as_dict()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for name, value in payload.items():
            print(f"{name}: {value}")
    return 0


def _phase1a_p5_outcomes_command(args: argparse.Namespace) -> int:
    return _phase1a_outcomes_command(args, runner_name="run_phase1a_p5_outcomes")


def _phase1a_p1_05_outcomes_command(args: argparse.Namespace) -> int:
    return _phase1a_outcomes_command(args, runner_name="run_phase1a_p1_05_outcomes")


def _phase1a_p4_pair_outcomes_command(args: argparse.Namespace) -> int:
    return _phase1a_outcomes_command(args, runner_name="run_phase1a_p4_outcome_pair")


def _m0a_json_value(value: object) -> object:
    """Convert CLI report objects into plain deterministic JSON values."""

    if is_dataclass(value) and not isinstance(value, type):
        return _m0a_json_value(asdict(value))
    if isinstance(value, dict):
        return {str(key): _m0a_json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_m0a_json_value(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _m0a_state_paths(args: argparse.Namespace) -> tuple[Path, Path, Path]:
    state_root = args.state_root.expanduser().resolve()
    return state_root / "ledger.sqlite3", state_root / "artifacts", state_root


def _m0a_emit(payload: object, *, emit_json: bool) -> None:
    value = _m0a_json_value(payload)
    if emit_json:
        print(json.dumps(value, indent=2, sort_keys=True))
        return
    if isinstance(value, dict):
        for key, item in value.items():
            print(f"{key}: {item}")
        return
    print(value)


def _m0a_build_features_command(args: argparse.Namespace) -> int:
    from systematic_fx.research.m0a.ledger import M0aLedgerError
    from systematic_fx.research.m0a.model import M0aError
    from systematic_fx.research.m0a.pipeline import build_feature_artifact

    _, artifact_root, _ = _m0a_state_paths(args)
    try:
        result = build_feature_artifact(args.epoch, artifact_root=artifact_root)
    except (M0aError, M0aLedgerError, OSError) as error:
        print(error, file=sys.stderr)
        return 2
    _m0a_emit(result, emit_json=args.json)
    return 0


def _m0a_build_labels_command(args: argparse.Namespace) -> int:
    from systematic_fx.research.m0a.ledger import M0aLedgerError
    from systematic_fx.research.m0a.model import M0aError
    from systematic_fx.research.m0a.pipeline import build_label_artifact

    _, artifact_root, _ = _m0a_state_paths(args)
    try:
        result = build_label_artifact(args.epoch, artifact_root=artifact_root)
    except (M0aError, M0aLedgerError, OSError) as error:
        print(error, file=sys.stderr)
        return 2
    _m0a_emit(result, emit_json=args.json)
    return 0


def _m0a_run_once(args: argparse.Namespace) -> tuple[int, object | None]:
    from systematic_fx.research.m0a.daemon import ForcedCrash, force_crash_after_claim
    from systematic_fx.research.m0a.ledger import M0aLedgerError
    from systematic_fx.research.m0a.model import M0aError
    from systematic_fx.research.m0a.pipeline import run_epoch_pipeline

    ledger_path, artifact_root, _ = _m0a_state_paths(args)
    try:
        result = run_epoch_pipeline(
            args.epoch,
            ledger_path=ledger_path,
            artifact_root=artifact_root,
            worker_id=args.worker_id,
            crash_hook=force_crash_after_claim if args.simulate_crash_after_claim else None,
        )
    except ForcedCrash as error:
        print(error, file=sys.stderr)
        return 75, None
    except (M0aError, M0aLedgerError, OSError) as error:
        print(error, file=sys.stderr)
        return 2, None
    if not result.invariants.valid or result.ledger.status == "HALTED":
        return 1, result
    # Another live worker may still own an unexpired lease.  That is a healthy,
    # resumable state, but a one-shot command must not report the epoch complete.
    return (0 if result.ledger.status == "COMPLETED" else 3), result


def _m0a_run_epoch_command(args: argparse.Namespace) -> int:
    status, result = _m0a_run_once(args)
    if result is not None:
        _m0a_emit(result, emit_json=args.json)
    return status


def _m0a_daemon_start_command(args: argparse.Namespace) -> int:
    """Run one finite epoch, optionally keeping the idle process alive."""

    while True:
        status, result = _m0a_run_once(args)
        if result is not None:
            _m0a_emit(result, emit_json=args.json)
        if status not in {0, 3} or not args.keep_alive:
            return status
        try:
            sleep(args.keep_alive_poll_seconds)
        except KeyboardInterrupt:
            return 0


def _m0a_report_command(args: argparse.Namespace) -> int:
    from systematic_fx.research.m0a.config import load_epoch
    from systematic_fx.research.m0a.ledger import M0aLedgerError
    from systematic_fx.research.m0a.model import M0aError
    from systematic_fx.research.m0a.pipeline import render_report_from_ledger

    ledger_path, artifact_root, _ = _m0a_state_paths(args)
    try:
        epoch = load_epoch(args.epoch)
        content = render_report_from_ledger(
            ledger_path,
            epoch.epoch_id,
            artifact_root=artifact_root,
            epoch_path=args.epoch,
        )
        output = (
            args.output.expanduser().resolve()
            if args.output is not None
            else (Path.cwd() / "reports" / "generated" / f"{epoch.epoch_id}.md").resolve()
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(f".{output.name}.tmp-{os.getpid()}")
        temporary.write_text(content, encoding="utf-8")
        os.replace(temporary, output)
    except (M0aError, M0aLedgerError, OSError) as error:
        print(error, file=sys.stderr)
        return 2
    if args.json:
        _m0a_emit(
            {
                "epoch_id": epoch.epoch_id,
                "report_path": output,
                "byte_size": len(content.encode("utf-8")),
            },
            emit_json=True,
        )
    else:
        print(content, end="")
        print(f"report_path: {output}", file=sys.stderr)
    return 0


def _m0a_verify_invariants_command(args: argparse.Namespace) -> int:
    from systematic_fx.research.m0a.config import load_epoch
    from systematic_fx.research.m0a.ledger import M0aLedgerError
    from systematic_fx.research.m0a.model import M0aError
    from systematic_fx.research.m0a.pipeline import verify_epoch_invariants

    ledger_path, artifact_root, _ = _m0a_state_paths(args)
    try:
        epoch = load_epoch(args.epoch)
        result = verify_epoch_invariants(
            ledger_path,
            epoch.epoch_id,
            artifact_root=artifact_root,
        )
    except (M0aError, M0aLedgerError, OSError) as error:
        print(error, file=sys.stderr)
        return 2
    _m0a_emit(result, emit_json=args.json)
    return 0 if result.valid else 1


def _m0b_real_slice_command(args: argparse.Namespace) -> int:
    from systematic_fx.research.m0b import (
        RealSliceError,
        load_materialized_real_slice,
        load_real_slice_config,
        materialize_real_slice,
        verify_real_slice,
    )

    settings = Settings.from_env()
    try:
        config = load_real_slice_config(args.config)
        output_root = (
            args.output_root if args.output_root is not None else Path.cwd() / config.staged_root
        )
        if args.m0b_action == "materialize-real-slice":
            build = materialize_real_slice(
                config,
                data_root=args.data_root or settings.data_root,
                output_root=output_root,
            )
        else:
            if args.build is None:
                raise RealSliceError("--build is required for verify-real-slice")
            build = load_materialized_real_slice(args.build)
            verify_real_slice(
                build,
                config,
                data_root=args.data_root or settings.data_root,
                staged_root=output_root,
            )
    except (FileNotFoundError, OSError, RealSliceError) as error:
        print(error, file=sys.stderr)
        return 2
    _m0a_emit(
        {
            **build.as_dict(),
            "build_sha256": build.sha256,
            "status": "SEARCH_CONTEXT_ONLY_STATUS_UNVERIFIED",
        },
        emit_json=args.json,
    )
    return 0


def _m0b_active_contract_command(args: argparse.Namespace) -> int:
    from systematic_fx.data.cme_active_contract import (
        ActiveContractEvidenceError,
        load_active_contract_volume_manifest,
        materialize_active_contract_mapping_artifact,
    )
    from systematic_fx.data.cme_schedule import (
        CmeScheduleEvidenceError,
        load_cme_schedule_archive,
        verify_schedule_upstream_source,
    )

    settings = Settings.from_env()
    try:
        if (args.schedule_archive is None) != (args.schedule_source is None):
            raise ActiveContractEvidenceError(
                "schedule archive and archived source must be supplied together"
            )
        schedule = None
        if args.schedule_archive is not None:
            schedule = load_cme_schedule_archive(
                args.schedule_archive,
                allow_test_fixture=args.allow_test_fixture,
            )
            schedule = verify_schedule_upstream_source(schedule, args.schedule_source)
        manifest = load_active_contract_volume_manifest(
            args.manifest,
            schedule_archive=schedule,
            allow_bounded_weekday_fallback=args.allow_bounded_weekday_fallback,
        )
        artifact = materialize_active_contract_mapping_artifact(
            manifest,
            data_root=args.data_root or settings.data_root,
            verify_source_hashes=True,
        )
    except (
        ActiveContractEvidenceError,
        CmeScheduleEvidenceError,
        FileNotFoundError,
        OSError,
    ) as error:
        print(error, file=sys.stderr)
        return 2
    _m0a_emit(
        {
            **artifact.as_dict(),
            "content_sha256": artifact.content_sha256,
            "status": "POINT_IN_TIME_ACTIVE_MAPPING_VERIFIED_NOT_ENTRY_AUTHORIZATION",
        },
        emit_json=args.json,
    )
    return 0


def _m0b_schedule_archive_command(args: argparse.Namespace) -> int:
    from systematic_fx.data.cme_schedule import (
        CmeScheduleEvidenceError,
        load_cme_schedule_archive,
        verify_schedule_upstream_source,
    )

    try:
        archive = load_cme_schedule_archive(
            args.archive,
            allow_test_fixture=args.allow_test_fixture,
        )
        archive = verify_schedule_upstream_source(archive, args.source)
    except (CmeScheduleEvidenceError, FileNotFoundError, OSError) as error:
        print(error, file=sys.stderr)
        return 2
    _m0a_emit(
        {
            "archive_sha256": archive.sha256,
            "covered_end_exclusive": archive.covered_end_exclusive,
            "covered_start": archive.covered_start,
            "evidence_kind": archive.evidence_kind,
            "product_root": archive.product_root,
            "session_revision_count": len(archive.sessions),
            "source_id": archive.source_id,
            "source_sha256": archive.source_sha256,
            "status": "SCHEDULE_ARCHIVE_VERIFIED_NOT_TRADING_STATUS",
            "venue": archive.venue,
            "version": archive.version,
        },
        emit_json=args.json,
    )
    return 0


def _m0b_status_evidence_command(args: argparse.Namespace) -> int:
    from systematic_fx.data.cme_status import (
        CmeStatusEvidenceError,
        load_cme_trading_status_evidence,
        verify_status_upstream_source,
    )

    try:
        evidence = load_cme_trading_status_evidence(
            args.evidence,
            allow_test_fixture=args.allow_test_fixture,
        )
        evidence = verify_status_upstream_source(evidence, args.source)
        decision = None
        if args.event_ts_ns is not None:
            decision = evidence.status_at(
                args.event_ts_ns,
                venue="CME_GLOBEX",
                product_root="6E",
            )
    except (CmeStatusEvidenceError, FileNotFoundError, OSError) as error:
        print(error, file=sys.stderr)
        return 2
    _m0a_emit(
        {
            "covered_end_ts_ns": evidence.covered_end_ts_ns,
            "covered_start_ts_ns": evidence.covered_start_ts_ns,
            "decision": None if decision is None else decision.as_dict(),
            "evidence_kind": evidence.evidence_kind,
            "evidence_sha256": evidence.sha256,
            "maximum_observation_age_seconds": evidence.maximum_observation_age_seconds,
            "observation_count": len(evidence.observations),
            "product_root": evidence.product_root,
            "source_id": evidence.source_id,
            "source_sha256": evidence.source_sha256,
            "status": "POINT_IN_TIME_STATUS_EVIDENCE_VERIFIED",
            "venue": evidence.venue,
            "version": evidence.version,
        },
        emit_json=args.json,
    )
    return 0


def _m0b_first_passage_command(args: argparse.Namespace) -> int:
    from systematic_fx.research.m0b.first_passage_store import (
        FirstPassageStoreError,
        build_first_passage_store,
        load_first_passage_store,
    )
    from systematic_fx.research.m0b.materialize import load_materialized_real_slice
    from systematic_fx.research.m0b.model import RealSliceError
    from systematic_fx.research.m0b.store_config import load_first_passage_store_config

    try:
        config = load_first_passage_store_config(args.store_config)
        if args.m0b_action == "build-first-passage-store":
            build = load_materialized_real_slice(args.build)
            source_root = args.staged_root or args.build.parent
            store = build_first_passage_store(
                config.store_spec,
                build,
                staged_root=source_root,
                output_root=args.store_root,
            )
            manifest_path = args.store_root / f"first-passage-store-{store.sha256}.json"
        else:
            store = load_first_passage_store(args.store, verify_shards=True)
            manifest_path = args.store
        config.verify_store(store)
    except (FirstPassageStoreError, RealSliceError, FileNotFoundError, OSError) as error:
        print(error, file=sys.stderr)
        return 2
    _m0a_emit(
        {
            **store.as_dict(),
            "manifest_path": manifest_path,
            "store_sha256": store.sha256,
            "status": "SEARCH_ONLY_FIRST_PASSAGE_STORE_VERIFIED",
        },
        emit_json=args.json,
    )
    return 0


def _m0b_worker_cycle_command(args: argparse.Namespace) -> int:
    import psycopg

    from systematic_fx.db.m0b_worker_access import (
        M0bWorkerAccessError,
        verify_m0b_worker_access,
    )
    from systematic_fx.db.m0b_worker_registry import M0bWorkerRegistryError
    from systematic_fx.research.m0b.runner import M0bRunnerError, run_claimed_worker_cycle

    database_url = args.database_url or os.environ.get("SYSTEMATIC_FX_M0B_WORKER_DATABASE_URL")
    expected_session_user = args.expected_session_user or os.environ.get(
        "SYSTEMATIC_FX_M0B_WORKER_DATABASE_USER"
    )
    if not database_url or not expected_session_user:
        print("M0b worker database URL and expected session user are required", file=sys.stderr)
        return 2
    try:
        access = verify_m0b_worker_access(
            database_url,
            expected_session_user=expected_session_user,
        )
        result = run_claimed_worker_cycle(
            database_url,
            epoch_key=args.epoch_key,
            worker_id=args.worker_id,
            worker_root=args.worker_root,
        )
    except (
        M0bRunnerError,
        M0bWorkerAccessError,
        M0bWorkerRegistryError,
        OSError,
        psycopg.Error,
        ValueError,
    ) as error:
        _m0a_emit(
            {"error": str(error), "status": "FAILED_CLOSED"},
            emit_json=args.json,
        )
        return 2
    _m0a_emit(
        {
            "access_status": access.status,
            "candidate_sha256": result.candidate_sha256,
            "error": result.error,
            "research_run_attempt_id": result.research_run_attempt_id,
            "result": result.result,
            "status": result.status,
            "work_spec_sha256": result.work_spec_sha256,
        },
        emit_json=args.json,
    )
    return 1 if result.status == "FAILED" else 0


def _ai_pattern_discovery_command(args: argparse.Namespace) -> int:
    from systematic_fx.research.ai_discovery_context import AIDiscoveryContextError
    from systematic_fx.research.ai_pattern_config import AIPatternConfigError
    from systematic_fx.research.ai_pattern_discovery import PatternDiscoveryError
    from systematic_fx.research.ai_pattern_run import (
        AIPatternRunError,
        publish_ai_pattern_markdown_report,
        run_ai_pattern_research,
        verify_ai_pattern_research,
    )

    try:
        project_root = Path.cwd()
        if args.ai_pattern_action == "run":
            run = run_ai_pattern_research(project_root)
            report_path = publish_ai_pattern_markdown_report(project_root, run)
        else:
            run = verify_ai_pattern_research(project_root)
            report_path = project_root / "reports/generated/ai_pattern_discovery_batch_1.md"
            if not report_path.is_file():
                report_path = None
    except (
        AIDiscoveryContextError,
        AIPatternConfigError,
        AIPatternRunError,
        FileNotFoundError,
        OSError,
        PatternDiscoveryError,
        ValueError,
    ) as error:
        print(error, file=sys.stderr)
        return 2
    _m0a_emit(
        {**run.as_dict(), "markdown_report_path": report_path},
        emit_json=args.json,
    )
    return 0


def _verify_holdout_isolation_command(args: argparse.Namespace) -> int:
    import psycopg

    from systematic_fx.db.holdout_isolation import (
        HoldoutIsolationError,
        verify_research_holdout_isolation,
    )

    database_url = args.database_url or os.environ.get("SYSTEMATIC_FX_RESEARCH_DATABASE_URL")
    expected_session_user = args.expected_session_user or os.environ.get(
        "SYSTEMATIC_FX_RESEARCH_DATABASE_USER"
    )
    if not database_url or not expected_session_user:
        print(
            "research database URL and expected session user are required",
            file=sys.stderr,
        )
        return 2
    try:
        report = verify_research_holdout_isolation(
            database_url,
            expected_session_user=expected_session_user,
        )
    except (HoldoutIsolationError, psycopg.Error) as error:
        _m0a_emit(
            {"status": "NOT_PROVISIONED", "error": str(error)},
            emit_json=args.json,
        )
        return 2
    _m0a_emit(report, emit_json=args.json)
    return 0


def _verify_m0b_worker_access_command(args: argparse.Namespace) -> int:
    import psycopg

    from systematic_fx.db.m0b_worker_access import (
        M0bWorkerAccessError,
        verify_m0b_worker_access,
    )

    database_url = args.database_url or os.environ.get("SYSTEMATIC_FX_M0B_WORKER_DATABASE_URL")
    expected_session_user = args.expected_session_user or os.environ.get(
        "SYSTEMATIC_FX_M0B_WORKER_DATABASE_USER"
    )
    if not database_url or not expected_session_user:
        print("M0b worker database URL and expected session user are required", file=sys.stderr)
        return 2
    try:
        report = verify_m0b_worker_access(
            database_url,
            expected_session_user=expected_session_user,
        )
    except (M0bWorkerAccessError, psycopg.Error) as error:
        _m0a_emit({"status": "NOT_PROVISIONED", "error": str(error)}, emit_json=args.json)
        return 2
    _m0a_emit(report, emit_json=args.json)
    return 0


def _phase1a_p5_equivalence_audit_command(args: argparse.Namespace) -> int:
    from systematic_fx.research.outcome_equivalence_audit import (
        OutcomeEquivalenceAuditError,
        run_phase1a_p5_outcome_equivalence_audit,
    )

    settings = Settings.from_env()
    database_url = args.database_url or settings.database_url
    if not database_url:
        print("database URL is required via --database-url or SYSTEMATIC_FX_DATABASE_URL")
        return 2

    def report_progress(progress: object) -> None:
        if args.json:
            return
        payload = progress.as_dict()
        completed = int(payload["completed"])
        total = int(payload["total"])
        if payload["stage"] == "CACHE":
            print(
                f"cache: {completed}/{total} reused={payload['cache_reused_count']}",
                file=sys.stderr,
                flush=True,
            )
            return
        print(
            f"checkpoint: {completed}/{total} date={payload['source_date']} "
            f"events={payload['source_event_count']} "
            f"detail_rows={payload['detail_record_count']}",
            file=sys.stderr,
            flush=True,
        )

    try:
        report = run_phase1a_p5_outcome_equivalence_audit(
            project_root=Path.cwd(),
            data_root=settings.data_root,
            database_url=database_url,
            outcome_replay_manifest_id=args.outcome_replay_manifest_id,
            progress_callback=report_progress,
        )
    except OutcomeEquivalenceAuditError as error:
        print(error)
        return 2

    payload = report.as_dict()
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for name, value in payload.items():
            print(f"{name}: {value}")
    return 0 if report.passed else 1


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

    phase1a_slice_parser = research_commands.add_parser(
        "phase1a-slice",
        help="run or exactly resume one governed five-date Phase 1A Discovery slice",
    )
    phase1a_slice_parser.add_argument(
        "--slice-index",
        type=_nonnegative_int,
        default=0,
        help="zero-based Discovery slice index (default: 0; maximum: 98)",
    )
    phase1a_slice_parser.add_argument("--database-url")
    phase1a_slice_parser.add_argument("--json", action="store_true", help="emit JSON")
    phase1a_slice_parser.set_defaults(handler=_phase1a_slice_command)

    phase1a_outcome_parser = research_commands.add_parser(
        "phase1a-p5-outcomes",
        help="plan, cache, run, or exactly resume the governed p5 shared outcome replay",
    )
    phase1a_outcome_mode = phase1a_outcome_parser.add_mutually_exclusive_group()
    phase1a_outcome_mode.add_argument(
        "--plan-only",
        action="store_true",
        help="verify the exact 99-slice/1,111-signal/485-partition plan without building caches",
    )
    phase1a_outcome_mode.add_argument(
        "--cache-only",
        action="store_true",
        help="build and verify immutable daily caches without starting the economic replay",
    )
    phase1a_outcome_parser.add_argument(
        "--max-cache-workers",
        type=_positive_int,
        choices=range(1, 5),
        help="parallel raw-cache builders (1-4; defaults to the governed config)",
    )
    phase1a_outcome_parser.add_argument("--database-url")
    phase1a_outcome_parser.add_argument("--json", action="store_true", help="emit JSON")
    phase1a_outcome_parser.set_defaults(handler=_phase1a_p5_outcomes_command)

    phase1a_p5_audit_parser = research_commands.add_parser(
        "phase1a-p5-equivalence-audit",
        help="prove byte equivalence of uninterrupted and resumed governed p5 replays",
    )
    phase1a_p5_audit_parser.add_argument(
        "--outcome-replay-manifest-id",
        type=_positive_int,
        help="explicit successful p5 replay manifest (required only if selection is ambiguous)",
    )
    phase1a_p5_audit_parser.add_argument("--database-url")
    phase1a_p5_audit_parser.add_argument(
        "--json",
        action="store_true",
        help="emit final JSON report",
    )
    phase1a_p5_audit_parser.set_defaults(handler=_phase1a_p5_equivalence_audit_command)

    phase1a_p1_outcome_parser = research_commands.add_parser(
        "phase1a-p1-05-outcomes",
        help="plan, cache, run, or exactly resume the governed p1_05 shared outcome replay",
    )
    phase1a_p1_outcome_mode = phase1a_p1_outcome_parser.add_mutually_exclusive_group()
    phase1a_p1_outcome_mode.add_argument(
        "--plan-only",
        action="store_true",
        help="verify the exact 99-slice/943-signal/478-partition plan without building caches",
    )
    phase1a_p1_outcome_mode.add_argument(
        "--cache-only",
        action="store_true",
        help="build and verify immutable daily caches without starting the economic replay",
    )
    phase1a_p1_outcome_parser.add_argument(
        "--max-cache-workers",
        type=_positive_int,
        choices=range(1, 5),
        help="parallel raw-cache builders (1-4; defaults to the governed config)",
    )
    phase1a_p1_outcome_parser.add_argument("--database-url")
    phase1a_p1_outcome_parser.add_argument("--json", action="store_true", help="emit JSON")
    phase1a_p1_outcome_parser.set_defaults(handler=_phase1a_p1_05_outcomes_command)

    phase1a_p4_pair_parser = research_commands.add_parser(
        "phase1a-p4-pair-outcomes",
        help="plan, cache, or atomically run both governed P4 shared outcome replays",
    )
    phase1a_p4_pair_mode = phase1a_p4_pair_parser.add_mutually_exclusive_group()
    phase1a_p4_pair_mode.add_argument(
        "--plan-only",
        action="store_true",
        help="verify both fixed 99-slice/674-signal plans without building caches",
    )
    phase1a_p4_pair_mode.add_argument(
        "--cache-only",
        action="store_true",
        help="build and verify both immutable cache plans without economic replay",
    )
    phase1a_p4_pair_parser.add_argument(
        "--max-cache-workers",
        type=_positive_int,
        choices=range(1, 5),
        help="parallel raw-cache builders per candidate (1-4; governed default otherwise)",
    )
    phase1a_p4_pair_parser.add_argument("--database-url")
    phase1a_p4_pair_parser.add_argument(
        "--json", action="store_true", help="emit both terminal reports as one JSON object"
    )
    phase1a_p4_pair_parser.set_defaults(handler=_phase1a_p4_pair_outcomes_command)

    m0a_parser = research_commands.add_parser(
        "m0a",
        help="run the deterministic finite-budget M0a research walking skeleton",
    )
    m0a_commands = m0a_parser.add_subparsers(dest="m0a_command", required=True)

    def add_m0a_common(command: argparse.ArgumentParser) -> None:
        command.add_argument(
            "--epoch",
            type=Path,
            default=Path("epochs/m0a_fixture_v1.toml"),
            help="immutable M0a epoch manifest",
        )
        command.add_argument(
            "--state-root",
            type=Path,
            default=Path(".local/m0a"),
            help="local SQLite ledger and content-addressed artifact root",
        )
        command.add_argument("--json", action="store_true", help="emit JSON")

    m0a_build_features = m0a_commands.add_parser(
        "build-features",
        help="build the fixture and point-in-time feature artifacts idempotently",
    )
    add_m0a_common(m0a_build_features)
    m0a_build_features.set_defaults(handler=_m0a_build_features_command)

    m0a_build_labels = m0a_commands.add_parser(
        "build-labels",
        help="reopen exact feature inputs and build quote-aware label artifacts",
    )
    add_m0a_common(m0a_build_labels)
    m0a_build_labels.set_defaults(handler=_m0a_build_labels_command)

    def add_m0a_run(command: argparse.ArgumentParser) -> None:
        add_m0a_common(command)
        command.add_argument("--worker-id", default="m0a-worker-1")
        command.add_argument(
            "--simulate-crash-after-claim",
            action="store_true",
            help="leave one RUNNING lease to verify restart recovery",
        )

    m0a_run_epoch = m0a_commands.add_parser(
        "run-epoch",
        help="register and drain exactly the precommitted REAL and NULL budgets",
    )
    add_m0a_run(m0a_run_epoch)
    m0a_run_epoch.set_defaults(handler=_m0a_run_epoch_command)

    m0a_daemon = m0a_commands.add_parser(
        "daemon",
        help="operate the LLM-free lease-based research daemon",
    )
    m0a_daemon_commands = m0a_daemon.add_subparsers(dest="m0a_daemon_command", required=True)
    m0a_daemon_start = m0a_daemon_commands.add_parser(
        "start",
        help="resume the finite epoch and optionally remain healthy while idle",
    )
    add_m0a_run(m0a_daemon_start)
    m0a_daemon_start.add_argument(
        "--keep-alive",
        action="store_true",
        help="stay alive after the finite epoch budget is exhausted",
    )
    m0a_daemon_start.add_argument(
        "--keep-alive-poll-seconds",
        type=_positive_int,
        default=30,
    )
    m0a_daemon_start.set_defaults(handler=_m0a_daemon_start_command)

    m0a_report = m0a_commands.add_parser(
        "report",
        help="verify durable artifacts and render a search-data Markdown report",
    )
    add_m0a_common(m0a_report)
    m0a_report.add_argument("--output", type=Path)
    m0a_report.set_defaults(handler=_m0a_report_command)

    m0a_verify = m0a_commands.add_parser(
        "verify-invariants",
        help="verify budget, lineage, attempt, artifact, and event-chain invariants",
    )
    add_m0a_common(m0a_verify)
    m0a_verify.set_defaults(handler=_m0a_verify_invariants_command)

    m0b_parser = research_commands.add_parser(
        "m0b",
        help="materialize or verify the bounded search-only real CME 6E bridge",
    )
    m0b_commands = m0b_parser.add_subparsers(dest="m0b_action", required=True)

    def add_m0b_real_slice_common(command: argparse.ArgumentParser) -> None:
        command.add_argument(
            "--config",
            type=Path,
            default=Path("configs/research/m0b_real_slice_v1.toml"),
        )
        command.add_argument("--data-root", type=Path)
        command.add_argument("--output-root", type=Path)
        command.add_argument("--json", action="store_true", help="emit JSON")

    m0b_materialize = m0b_commands.add_parser(
        "materialize-real-slice",
        help="stream the exact four-file allowlist into immutable quote/features/labels",
    )
    add_m0b_real_slice_common(m0b_materialize)
    m0b_materialize.set_defaults(handler=_m0b_real_slice_command)

    m0b_verify = m0b_commands.add_parser(
        "verify-real-slice",
        help="reopen one exact build manifest and verify every content-addressed artifact",
    )
    add_m0b_real_slice_common(m0b_verify)
    m0b_verify.add_argument("--build", type=Path, required=True)
    m0b_verify.set_defaults(handler=_m0b_real_slice_command)

    m0b_active = m0b_commands.add_parser(
        "verify-active-contract-mapping",
        help="recompute the exact prior-session-volume mapping from its raw allowlist",
    )
    m0b_active.add_argument(
        "--manifest",
        type=Path,
        default=Path("configs/data/cme_6e_active_contract_roll_context_v1.toml"),
    )
    m0b_active.add_argument("--data-root", type=Path)
    m0b_active.add_argument("--schedule-archive", type=Path)
    m0b_active.add_argument("--schedule-source", type=Path)
    m0b_active.add_argument(
        "--allow-test-fixture",
        action="store_true",
        help="explicitly permit deterministic schedule fixture evidence",
    )
    m0b_active.add_argument(
        "--allow-bounded-weekday-fallback",
        action="store_true",
        help="permit the exact holiday-free bounded roll-context manifest without an archive",
    )
    m0b_active.add_argument("--json", action="store_true", help="emit JSON")
    m0b_active.set_defaults(handler=_m0b_active_contract_command)

    m0b_schedule = m0b_commands.add_parser(
        "verify-schedule-archive",
        help="verify immutable CME schedule revisions and their archived source bytes",
    )
    m0b_schedule.add_argument("--archive", type=Path, required=True)
    m0b_schedule.add_argument("--source", type=Path, required=True)
    m0b_schedule.add_argument(
        "--allow-test-fixture",
        action="store_true",
        help="explicitly permit deterministic fixture evidence",
    )
    m0b_schedule.add_argument("--json", action="store_true", help="emit JSON")
    m0b_schedule.set_defaults(handler=_m0b_schedule_archive_command)

    m0b_status = m0b_commands.add_parser(
        "verify-status-evidence",
        help="verify point-in-time CME status evidence without inferring it from hours",
    )
    m0b_status.add_argument("--evidence", type=Path, required=True)
    m0b_status.add_argument("--source", type=Path, required=True)
    m0b_status.add_argument("--event-ts-ns", type=_nonnegative_int)
    m0b_status.add_argument(
        "--allow-test-fixture",
        action="store_true",
        help="explicitly permit deterministic fixture evidence",
    )
    m0b_status.add_argument("--json", action="store_true", help="emit JSON")
    m0b_status.set_defaults(handler=_m0b_status_evidence_command)

    m0b_store_build = m0b_commands.add_parser(
        "build-first-passage-store",
        help="shard one exact quote-aware label artifact at complete event boundaries",
    )
    m0b_store_build.add_argument(
        "--store-config",
        type=Path,
        default=Path("configs/research/m0b_first_passage_store_v1.toml"),
    )
    m0b_store_build.add_argument("--build", type=Path, required=True)
    m0b_store_build.add_argument("--staged-root", type=Path)
    m0b_store_build.add_argument("--store-root", type=Path, required=True)
    m0b_store_build.add_argument("--json", action="store_true", help="emit JSON")
    m0b_store_build.set_defaults(handler=_m0b_first_passage_command)

    m0b_store_verify = m0b_commands.add_parser(
        "verify-first-passage-store",
        help="verify an immutable store manifest and every content-addressed shard",
    )
    m0b_store_verify.add_argument(
        "--store-config",
        type=Path,
        default=Path("configs/research/m0b_first_passage_store_v1.toml"),
    )
    m0b_store_verify.add_argument("--store", type=Path, required=True)
    m0b_store_verify.add_argument("--json", action="store_true", help="emit JSON")
    m0b_store_verify.set_defaults(handler=_m0b_first_passage_command)

    m0b_worker_cycle = m0b_commands.add_parser(
        "worker-cycle",
        help="claim and execute at most one immutable pre-registered M0b work item",
    )
    m0b_worker_cycle.add_argument("--epoch-key", required=True)
    m0b_worker_cycle.add_argument("--worker-id", required=True)
    m0b_worker_cycle.add_argument("--worker-root", type=Path, required=True)
    m0b_worker_cycle.add_argument("--database-url")
    m0b_worker_cycle.add_argument("--expected-session-user")
    m0b_worker_cycle.add_argument("--json", action="store_true", help="emit JSON")
    m0b_worker_cycle.set_defaults(handler=_m0b_worker_cycle_command)

    ai_pattern_parser = research_commands.add_parser(
        "ai-pattern",
        help="autonomously mine or replay the one finite outcome-blind Discovery proposal batch",
    )
    ai_pattern_commands = ai_pattern_parser.add_subparsers(
        dest="ai_pattern_action",
        required=True,
    )
    ai_pattern_run = ai_pattern_commands.add_parser(
        "run",
        help="precommit, mine, and freeze exactly twelve proposal-only hypotheses",
    )
    ai_pattern_run.add_argument("--json", action="store_true", help="emit JSON")
    ai_pattern_run.set_defaults(handler=_ai_pattern_discovery_command)
    ai_pattern_verify = ai_pattern_commands.add_parser(
        "verify",
        help="reopen every input/artifact and deterministically reproduce the frozen batch",
    )
    ai_pattern_verify.add_argument("--json", action="store_true", help="emit JSON")
    ai_pattern_verify.set_defaults(handler=_ai_pattern_discovery_command)

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

    holdout_verify_parser = db_commands.add_parser(
        "verify-holdout-isolation",
        help="prove sealed-holdout denial from the daemon's actual unprivileged login",
    )
    holdout_verify_parser.add_argument("--database-url")
    holdout_verify_parser.add_argument("--expected-session-user")
    holdout_verify_parser.add_argument("--json", action="store_true", help="emit JSON")
    holdout_verify_parser.set_defaults(handler=_verify_holdout_isolation_command)

    worker_verify_parser = db_commands.add_parser(
        "verify-m0b-worker-access",
        help="prove the least-privilege M0b worker LOGIN boundary",
    )
    worker_verify_parser.add_argument("--database-url")
    worker_verify_parser.add_argument("--expected-session-user")
    worker_verify_parser.add_argument("--json", action="store_true", help="emit JSON")
    worker_verify_parser.set_defaults(handler=_verify_m0b_worker_access_command)

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
