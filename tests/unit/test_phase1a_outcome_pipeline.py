from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from systematic_fx import cli
from systematic_fx.backtest.barriers import Direction, ExecutableQuote
from systematic_fx.backtest.event_cache import (
    _CACHE_FIELDS,
    CACHE_SCHEMA,
    CACHE_VERSION,
    CachedExecutableQuote,
    DailyCacheReport,
    DailyCacheSpec,
    read_daily_executable_cache,
)
from systematic_fx.backtest.shared_replay import SharedReplay, SignalSeed
from systematic_fx.db.migrations import discover_migrations
from systematic_fx.db.outcome_registry import (
    OutcomeRegistryStateError,
    phase1a_p5_outcome_parameters,
)
from systematic_fx.research.outcome_artifacts import (
    load_final_result_manifest,
    load_outcome_checkpoint,
    publish_cache_manifest,
    publish_detail_shard,
    publish_final_result_manifest,
    publish_outcome_checkpoint,
)
from systematic_fx.research.outcome_config import load_outcome_replay_config
from systematic_fx.research.outcome_economics import OutcomeEconomicsAccumulator
from systematic_fx.research.outcome_inputs import (
    ContractTerminalResolution,
    DailyReplayPartition,
    OutcomeInputPlan,
    TerminalResolution,
)
from systematic_fx.research.phase1a_outcome_pipeline import (
    _SUPPORTED_MIGRATIONS,
    OutcomePipelineReport,
    OutcomeProgress,
    P4OutcomePairReport,
    Phase1AOutcomePipelineError,
    PreparedOutcomeCompletion,
    ReservedOutcomeExecution,
    _input_lineage,
    _make_run_spec,
    _run_replay,
    merge_daily_shared_events,
    run_phase1a_p4_outcome_pair,
)

EXPECTED_MIGRATION_0028_SHA256 = "fb5683dca1b054516b6ee94b721aeeb1ac9662993ac7495d41961fb66e5e172e"
EXPECTED_MIGRATION_0029_SHA256 = "5f6d002fb0f9ad89b0b8eb8256799df14e00879a8fcf4a95faab414b06d9ac45"
EXPECTED_MIGRATION_0030_SHA256 = "da6bed73dd947c5d9575364f7580c0acaacf74e561e1a231f51b56e64bbc1414"


def test_outcome_pipeline_supports_exact_p4_paired_outcome_migration() -> None:
    migrations = discover_migrations(Path(__file__).resolve().parents[2] / "migrations")
    migration_by_version = {item.version: item for item in migrations}

    assert tuple(item.version for item in migrations) == _SUPPORTED_MIGRATIONS
    assert _SUPPORTED_MIGRATIONS == tuple(range(1, 31))
    assert migration_by_version[24].checksum == (
        "4aa845757f1a220c8d5595d4db6053f6374d99d067ab7e20c3e40ea22d610010"
    )
    assert migration_by_version[25].checksum == (
        "e08aa486bf9a65b2875e92866ae5e939fc56dc5d871010dfdb4b9085550749dd"
    )
    assert migration_by_version[26].checksum == (
        "232badda3e76fca79f93fcff059de6f3404fc797eb26a93c9483fd554cfe20bb"
    )
    assert migration_by_version[27].checksum == (
        "f0f69db031dc555b260da1fceef5f1fb4087f25717f1472ae4b006e77182cdb8"
    )
    assert migration_by_version[28].name == "phase1a_p4_paired_outcomes"
    assert migration_by_version[28].checksum == EXPECTED_MIGRATION_0028_SHA256
    assert migration_by_version[29].name == "m0b_governed_control_plane"
    assert migration_by_version[29].checksum == EXPECTED_MIGRATION_0029_SHA256
    assert migration_by_version[30].name == "m0b_numeric_admission_worker_api"
    assert migration_by_version[30].checksum == EXPECTED_MIGRATION_0030_SHA256


class _Phase1ACompleteTestEconomics(OutcomeEconomicsAccumulator):
    """Keep real detail accounting while shaping tiny fixtures as a full frozen surface."""

    def finalize(self):
        return tuple(
            replace(
                summary,
                signal_count=529 if summary.direction == "LONG" else 582,
                entry_fill_count=0,
                entry_not_filled_count=529 if summary.direction == "LONG" else 582,
                skipped_occupied_count=0,
                take_profit_first_count=0,
                stop_first_count=0,
                terminal_exit_count=0,
                censored_count=0,
                gross_pnl_ticks=0,
                variable_cost_ticks=0,
                allocated_fixed_cost_ticks=0,
                fully_loaded_net_pnl_ticks=0,
                fully_loaded_net_ev_ticks=None,
                fully_loaded_net_pnl_usd=Decimal(0),
                calendar_month_net_pnl_usd=Decimal(0),
                profit_factor=None,
                maximum_drawdown_usd=Decimal(0),
                complete=True,
            )
            for summary in super().finalize()
        )


def _partition(symbol: str, *, terminal: bool) -> DailyReplayPartition:
    source_date = date(2022, 1, 3)
    return DailyReplayPartition(
        cache_spec=DailyCacheSpec(
            source_date=source_date,
            source_parquet_path=Path(f"/{symbol}.parquet"),
            source_sha256="a" * 64,
            raw_symbol=symbol,
            event_index_offset=0,
        ),
        source_relative_uri=f"raw/{symbol}.parquet",
        session_ordinal=0,
        contract_expiry_month=date(2022, 3, 1),
        terminal=terminal,
    )


def _report(symbol: str, *, terminal_index: int | None) -> DailyCacheReport:
    return DailyCacheReport(
        path=Path(f"/{symbol}.cache.parquet"),
        sha256="b" * 64,
        byte_size=100,
        disposition="CREATED",
        source_date=date(2022, 1, 3),
        source_path=f"/{symbol}.parquet",
        source_sha256="a" * 64,
        raw_symbol=symbol,
        instrument_id=1,
        event_index_offset=0,
        source_row_count=10,
        cached_quote_count=3 if symbol == "6EH2" else 2,
        valid_quote_count=2,
        first_event_index=1 if symbol == "6EH2" else 2,
        last_event_index=5 if symbol == "6EH2" else 3,
        first_ts_recv_ns=100 if symbol == "6EH2" else 200,
        last_ts_recv_ns=400 if symbol == "6EH2" else 300,
        last_valid_event_index=terminal_index,
        last_valid_ts_recv_ns=300,
    )


def _resolution(
    *,
    contract_key: str,
    source_date: date,
    event_index: int,
    ts_recv_ns: int,
    eligible_partition_count: int = 1,
    trailing_non_executable_partition_count: int = 0,
) -> TerminalResolution:
    return TerminalResolution(
        contracts=(
            ContractTerminalResolution(
                contract_key=contract_key,
                eligible_partition_count=eligible_partition_count,
                terminal_source_date=source_date,
                terminal_event_index=event_index,
                terminal_ts_recv_ns=ts_recv_ns,
                trailing_non_executable_partition_count=(trailing_non_executable_partition_count),
            ),
        )
    )


def _cached(
    symbol: str, event_index: int, ts_recv_ns: int, *, valid: bool = True
) -> CachedExecutableQuote:
    return CachedExecutableQuote(
        contract_key=symbol,
        source_date=date(2022, 1, 3),
        source_sha256="a" * 64,
        sequence=event_index,
        source_row_index=event_index,
        row_group_index=0,
        row_index=event_index,
        invalid_reason=None if valid else "RESET_INVALIDATED",
        quote=ExecutableQuote(
            event_index=event_index,
            ts_recv_ns=ts_recv_ns,
            best_bid_ticks=100 if valid else None,
            best_ask_ticks=102 if valid else None,
            valid=valid,
        ),
    )


def _write_tiny_cache(data_root: Path, *, source_date: date) -> DailyCacheReport:
    raw_directory = data_root / "mbp-10"
    raw_directory.mkdir(exist_ok=True)
    source = raw_directory / f"{source_date.isoformat()}.parquet"
    source.write_bytes(b"tiny-raw-source")
    source_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    event_index = 10
    timestamp = 1_704_153_600_000_000_000
    metadata = {
        "cache_schema": CACHE_SCHEMA,
        "cache_version": CACHE_VERSION,
        "event_index_offset": event_index,
        "instrument_id": 7,
        "raw_symbol": "6EH4",
        "source_date": source_date.isoformat(),
        "source_relative_uri": f"mbp-10/{source_date.isoformat()}.parquet",
        "source_row_count": 1,
        "source_sha256": source_sha256,
    }
    schema = pa.schema(
        _CACHE_FIELDS,
        metadata={
            b"systematic_fx.cache": json.dumps(
                metadata,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        },
    )
    table = pa.Table.from_pylist(
        [
            {
                "event_index": event_index,
                "ts_recv_ns": timestamp,
                "best_bid_ticks": 100,
                "best_ask_ticks": 101,
                "valid": True,
                "sequence": 1,
                "source_row_index": 0,
                "row_group_index": 0,
                "row_index": 0,
                "invalid_reason": None,
            }
        ],
        schema=schema,
    )
    directory = data_root / "derived/backtest_event_cache/phase1a_daily_executable_cache_v1"
    directory.mkdir(parents=True, exist_ok=True)
    temporary = directory / "tiny.parquet"
    pq.write_table(table, temporary, compression="zstd", version="2.6")
    payload = temporary.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    path = directory / f"sha256={digest}.parquet"
    temporary.rename(path)
    path.chmod(0o444)
    return DailyCacheReport(
        path=path,
        sha256=digest,
        byte_size=len(payload),
        disposition="CREATED",
        source_date=source_date,
        source_path=str(source.resolve()),
        source_sha256=source_sha256,
        raw_symbol="6EH4",
        instrument_id=7,
        event_index_offset=event_index,
        source_row_count=1,
        cached_quote_count=1,
        valid_quote_count=1,
        first_event_index=event_index,
        last_event_index=event_index,
        first_ts_recv_ns=timestamp,
        last_ts_recv_ns=timestamp,
        last_valid_event_index=event_index,
        last_valid_ts_recv_ns=timestamp,
    )


def test_daily_merge_is_global_and_skips_rows_after_terminal_quote() -> None:
    partitions = (
        _partition("6EH2", terminal=True),
        _partition("6EM2", terminal=False),
    )
    reports = (
        _report("6EH2", terminal_index=4),
        _report("6EM2", terminal_index=None),
    )
    rows = {
        "6EH2": (
            _cached("6EH2", 1, 100),
            _cached("6EH2", 4, 300),
            _cached("6EH2", 5, 400, valid=False),
        ),
        "6EM2": (
            _cached("6EM2", 2, 200),
            _cached("6EM2", 3, 300),
        ),
    }

    events = tuple(
        merge_daily_shared_events(
            partitions,
            reports,
            read_cache=lambda report: iter(rows[report.raw_symbol]),
        )
    )

    assert [event.quote.event_index for event in events] == [1, 2, 3, 4]
    assert [event.ordering_key for event in events] == sorted(
        event.ordering_key for event in events
    )
    assert [event.quote.event_index for event in events if event.terminal] == [4]


def test_terminal_partition_requires_its_reported_valid_quote() -> None:
    partition = _partition("6EH2", terminal=True)
    report = _report("6EH2", terminal_index=4)

    with pytest.raises(Phase1AOutcomePipelineError, match="terminal cache quote"):
        tuple(
            merge_daily_shared_events(
                (partition,),
                (report,),
                read_cache=lambda _: iter((_cached("6EH2", 1, 100),)),
            )
        )


def test_resolved_terminal_suppresses_later_invalid_only_partition() -> None:
    trailing_day = date(2022, 1, 4)
    base = _partition("6EH2", terminal=False)
    partition = replace(
        base,
        cache_spec=replace(base.cache_spec, source_date=trailing_day),
        session_ordinal=1,
    )
    report = replace(
        _report("6EH2", terminal_index=None),
        source_date=trailing_day,
        valid_quote_count=0,
        last_valid_event_index=None,
        last_valid_ts_recv_ns=None,
    )
    cached_rows = (
        replace(
            _cached("6EH2", 5, 400, valid=False),
            source_date=trailing_day,
        ),
    )
    consumed: list[int] = []

    def read_cache(_: DailyCacheReport):
        for row in cached_rows:
            consumed.append(row.quote.event_index)
            yield row

    events = tuple(
        merge_daily_shared_events(
            (partition,),
            (report,),
            read_cache=read_cache,
            terminal_resolution=_resolution(
                contract_key="6EH2",
                source_date=date(2022, 1, 3),
                event_index=4,
                ts_recv_ns=300,
                eligible_partition_count=2,
                trailing_non_executable_partition_count=1,
            ),
        )
    )

    assert events == ()
    assert consumed == [5]


def test_phase1a_outcome_cli_modes_and_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: Any,
) -> None:
    arguments = cli.build_parser().parse_args(
        [
            "research",
            "phase1a-p5-outcomes",
            "--cache-only",
            "--max-cache-workers",
            "3",
            "--json",
        ]
    )
    captured: dict[str, object] = {}

    def fake_runner(**kwargs: object) -> object:
        captured.update(kwargs)
        callback = kwargs["progress_callback"]
        callback(
            OutcomeProgress(
                stage="CACHE",
                completed=10,
                total=20,
                cache_created_count=7,
                cache_reused_count=3,
            )
        )
        callback(
            OutcomeProgress(
                stage="CHECKPOINT",
                completed=2,
                total=12,
                source_date=date(2022, 1, 4),
                source_event_count=123,
                detail_record_count=456,
            )
        )
        return SimpleNamespace(as_dict=lambda: {"mode": kwargs["mode"]})

    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline.run_phase1a_p5_outcomes",
        fake_runner,
    )
    monkeypatch.setattr(
        cli.Settings,
        "from_env",
        lambda: SimpleNamespace(data_root=Path("data"), database_url="postgresql://test"),
    )

    assert arguments.handler(arguments) == 0
    assert captured["mode"] == "CACHE_ONLY"
    assert captured["max_cache_workers"] == 3
    captured_output = capsys.readouterr()
    assert json.loads(captured_output.out) == {"mode": "CACHE_ONLY"}
    assert "cache: 10/20 created=7 reused=3" in captured_output.err
    assert "checkpoint: 2/12 date=2022-01-04 events=123 detail_rows=456" in (captured_output.err)


def test_phase1a_outcome_cli_rejects_conflicting_mode_and_worker_bounds() -> None:
    parser = cli.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "research",
                "phase1a-p5-outcomes",
                "--plan-only",
                "--cache-only",
            ]
        )
    with pytest.raises(SystemExit):
        parser.parse_args(["research", "phase1a-p5-outcomes", "--max-cache-workers", "5"])
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "research",
                "phase1a-p1-05-outcomes",
                "--plan-only",
                "--cache-only",
            ]
        )
    with pytest.raises(SystemExit):
        parser.parse_args(["research", "phase1a-p1-05-outcomes", "--max-cache-workers", "5"])


def test_phase1a_p1_outcome_cli_selects_second_runner(
    monkeypatch: pytest.MonkeyPatch,
    capsys: Any,
) -> None:
    arguments = cli.build_parser().parse_args(
        ["research", "phase1a-p1-05-outcomes", "--plan-only", "--json"]
    )
    captured: dict[str, object] = {}

    def fake_runner(**kwargs: object) -> object:
        captured.update(kwargs)
        return SimpleNamespace(as_dict=lambda: {"query_id": "p1_05_unconfirmed_move_reversal"})

    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline.run_phase1a_p1_05_outcomes",
        fake_runner,
    )
    monkeypatch.setattr(
        cli.Settings,
        "from_env",
        lambda: SimpleNamespace(data_root=Path("data"), database_url="postgresql://test"),
    )

    assert arguments.handler(arguments) == 0
    assert captured["mode"] == "PLAN_ONLY"
    assert json.loads(capsys.readouterr().out) == {"query_id": "p1_05_unconfirmed_move_reversal"}


def _p4_operator_report(query_id: str, *, mode: str) -> OutcomePipelineReport:
    return OutcomePipelineReport(
        pipeline_version=f"{query_id}.pipeline",
        mode=mode,
        query_id=query_id,
        signal_count=(334 if query_id.startswith("p4_01") else 340),
        long_signal_count=(175 if query_id.startswith("p4_01") else 159),
        short_signal_count=(159 if query_id.startswith("p4_01") else 181),
        signal_source_date_count=(143 if query_id.startswith("p4_01") else 155),
        contract_count=7,
        cache_partition_count=(472 if query_id.startswith("p4_01") else 455),
        portable_artifact_manifest_sha256="1" * 64,
        rich_source_artifact_manifest_sha256="2" * 64,
        signal_manifest_sha256="3" * 64,
        input_plan_sha256="4" * 64,
        calendar_sha256="5" * 64,
        split_sha256="6" * 64,
    )


_P4_TEST_QUERY_IDS = (
    "p4_01_opposite_depth_depletion_continuation",
    "p4_02_depth_resistance_reversal",
)


def _p4_test_release(*, batch_id: int, release_id: int | None = None) -> object:
    return SimpleNamespace(
        release_sha256="e" * 64,
        p4_pair_batch_id=batch_id,
        p4_pair_release_id=batch_id + 1 if release_id is None else release_id,
        pair_id="phase1a_p4_liquidity_transition_pair_v1",
        p4_01_outcome_replay_manifest_id=1,
        p4_02_outcome_replay_manifest_id=2,
        p4_01_run_fingerprint="1" * 64,
        p4_02_run_fingerprint="2" * 64,
        p4_01_result_artifact_sha256="a" * 64,
        p4_02_result_artifact_sha256="b" * 64,
        p4_01_cell_summaries_sha256="f" * 64,
        p4_02_cell_summaries_sha256="0" * 64,
        decision_sha256s={
            _P4_TEST_QUERY_IDS[0]: {"LONG": "1" * 64, "SHORT": "2" * 64},
            _P4_TEST_QUERY_IDS[1]: {"LONG": "3" * 64, "SHORT": "4" * 64},
        },
        pair_config_sha256=("d83f28fae463643fc8969f8944b41c8b87254362fe709344afb7cfd240b8ea5f"),
        pair_economic_cell_count=1_936,
        cumulative_economic_cell_count=3_872,
    )


def _p4_test_executions(
    tmp_path: Path,
    services: object,
    *,
    execute_flags: tuple[bool, bool],
) -> tuple[ReservedOutcomeExecution, ReservedOutcomeExecution]:
    configs = tuple(
        load_outcome_replay_config(
            Path.cwd(),
            config_path=Path(
                "configs/research/phase1a_p4_01_outcome_replay_v1.toml"
                if index == 1
                else "configs/research/phase1a_p4_02_outcome_replay_v1.toml"
            ),
        )
        for index in (1, 2)
    )
    return tuple(
        ReservedOutcomeExecution(
            prepared=SimpleNamespace(config=config),
            reports=(),
            terminal_resolution=SimpleNamespace(),
            cache_manifest=SimpleNamespace(),
            run_spec=SimpleNamespace(fingerprint=str(index) * 64),
            reservation=SimpleNamespace(
                outcome_replay_manifest_id=index,
                execute=execute_flags[index - 1],
            ),
            report=_p4_operator_report(config.query_id, mode="RUN"),
            database_url="postgresql://test",
            data=tmp_path,
            services=services,
            predecessor_gate=None,
            progress_callback=None,
        )
        for index, config in enumerate(configs, start=1)
    )


def _patch_p4_test_preparation(
    monkeypatch: pytest.MonkeyPatch,
    executions: tuple[ReservedOutcomeExecution, ReservedOutcomeExecution],
) -> None:
    def fake_prepare(**kwargs: object) -> OutcomePipelineReport | ReservedOutcomeExecution:
        path = Path(kwargs["config_relative_path"])
        index = 0 if "p4_01" in path.name else 1
        if kwargs["mode"] == "PLAN_ONLY":
            return _p4_operator_report(_P4_TEST_QUERY_IDS[index], mode="PLAN_ONLY")
        return executions[index]

    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline._prepare_phase1a_outcomes",
        fake_prepare,
    )


def test_p4_pair_plan_preflights_both_in_fixed_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    def fake_prepare(**kwargs: object) -> OutcomePipelineReport:
        path = Path(kwargs["config_relative_path"])
        query_id = (
            "p4_01_opposite_depth_depletion_continuation"
            if "p4_01" in path.name
            else "p4_02_depth_resistance_reversal"
        )
        calls.append((query_id, str(kwargs["mode"])))
        return _p4_operator_report(query_id, mode=str(kwargs["mode"]))

    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline._prepare_phase1a_outcomes",
        fake_prepare,
    )

    report = run_phase1a_p4_outcome_pair(
        project_root=Path.cwd(),
        data_root=Path.cwd() / "data",
        database_url="postgresql://test",
        mode="PLAN_ONLY",
    )

    assert isinstance(report, P4OutcomePairReport)
    assert report.disposition == "PAIR_PLANNED"
    assert calls == [
        ("p4_01_opposite_depth_depletion_continuation", "PLAN_ONLY"),
        ("p4_02_depth_resistance_reversal", "PLAN_ONLY"),
    ]
    assert list(report.as_dict()["reports"]) == [
        "p4_01_opposite_depth_depletion_continuation",
        "p4_02_depth_resistance_reversal",
    ]


def test_p4_pair_registry_capabilities_fail_before_first_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def fake_prepare(**kwargs: object) -> OutcomePipelineReport:
        mode = str(kwargs["mode"])
        calls.append(mode)
        path = Path(kwargs["config_relative_path"])
        query_id = (
            "p4_01_opposite_depth_depletion_continuation"
            if "p4_01" in path.name
            else "p4_02_depth_resistance_reversal"
        )
        return _p4_operator_report(query_id, mode=mode)

    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline._prepare_phase1a_outcomes",
        fake_prepare,
    )

    with pytest.raises(Phase1AOutcomePipelineError, match="service preflight failed"):
        run_phase1a_p4_outcome_pair(
            project_root=Path.cwd(),
            data_root=Path.cwd() / "data",
            database_url="postgresql://test",
            services=SimpleNamespace(),
        )

    assert calls == ["PLAN_ONLY", "PLAN_ONLY"]


def test_p4_pair_reserves_both_before_economics_and_completes_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    first_query = "p4_01_opposite_depth_depletion_continuation"
    second_query = "p4_02_depth_resistance_reversal"
    events: list[str] = []
    result_objects = {
        first_query: SimpleNamespace(path=tmp_path / "first.json", sha256="a" * 64),
        second_query: SimpleNamespace(path=tmp_path / "second.json", sha256="b" * 64),
    }
    checkpoint_objects = {
        first_query: SimpleNamespace(
            path=tmp_path / "first-checkpoint.json",
            sha256="c" * 64,
            checkpoint_sequence=472,
        ),
        second_query: SimpleNamespace(
            path=tmp_path / "second-checkpoint.json",
            sha256="d" * 64,
            checkpoint_sequence=455,
        ),
    }

    services = SimpleNamespace()
    executions: dict[str, ReservedOutcomeExecution] = {}
    for index, query_id in enumerate((first_query, second_query), start=1):
        executions[query_id] = ReservedOutcomeExecution(
            prepared=SimpleNamespace(config=SimpleNamespace(query_id=query_id)),
            reports=(),
            terminal_resolution=SimpleNamespace(),
            cache_manifest=SimpleNamespace(),
            run_spec=SimpleNamespace(fingerprint=str(index) * 64),
            reservation=SimpleNamespace(outcome_replay_manifest_id=index, execute=True),
            report=_p4_operator_report(query_id, mode="RUN"),
            database_url="postgresql://test",
            data=tmp_path,
            services=services,
            predecessor_gate=None,
            progress_callback=None,
        )

    def fake_prepare(**kwargs: object) -> OutcomePipelineReport | ReservedOutcomeExecution:
        path = Path(kwargs["config_relative_path"])
        query_id = first_query if "p4_01" in path.name else second_query
        mode = str(kwargs["mode"])
        events.append(f"prepare:{query_id}:{mode}")
        if mode == "PLAN_ONLY":
            return _p4_operator_report(query_id, mode=mode)
        assert kwargs["p4_pair_config_sha256"] == (
            "d83f28fae463643fc8969f8944b41c8b87254362fe709344afb7cfd240b8ea5f"
        )
        return executions[query_id]

    def reserve_pair(*_args: object, **_kwargs: object) -> object:
        events.append("reserve_pair")
        return SimpleNamespace(
            p4_pair_batch_id=9,
            pair_id="phase1a_p4_liquidity_transition_pair_v1",
            status="PREPARED",
            p4_01_outcome_replay_manifest_id=1,
            p4_02_outcome_replay_manifest_id=2,
        )

    def fake_execute(
        execution: ReservedOutcomeExecution, **kwargs: object
    ) -> PreparedOutcomeCompletion:
        query_id = execution.prepared.config.query_id
        events.append(f"execute:{query_id}")
        assert kwargs == {"defer_registry_completion": True, "register_failure": False}
        first = query_id == first_query
        return PreparedOutcomeCompletion(
            result=result_objects[query_id],
            final_checkpoint=checkpoint_objects[query_id],
            completed_source_date_count=472 if first else 455,
            source_event_count=100 if first else 200,
            detail_record_count=484_968 if first else 493_680,
            summary_row_count=2_904,
            cell_summaries=(None,) * 2_904,
        )

    def complete_pair(*_args: object, **kwargs: object) -> object:
        events.append("complete_pair")
        assert kwargs["p4_pair_batch_id"] == 9
        assert [member.query_id for member in kwargs["members"]] == [first_query, second_query]
        release = SimpleNamespace(
            release_sha256="e" * 64,
            p4_pair_batch_id=9,
            p4_pair_release_id=10,
            pair_id="phase1a_p4_liquidity_transition_pair_v1",
            p4_01_outcome_replay_manifest_id=1,
            p4_02_outcome_replay_manifest_id=2,
            p4_01_run_fingerprint="1" * 64,
            p4_02_run_fingerprint="2" * 64,
            p4_01_result_artifact_sha256="a" * 64,
            p4_02_result_artifact_sha256="b" * 64,
            p4_01_cell_summaries_sha256="f" * 64,
            p4_02_cell_summaries_sha256="0" * 64,
            decision_sha256s={
                first_query: {"LONG": "1" * 64, "SHORT": "2" * 64},
                second_query: {"LONG": "3" * 64, "SHORT": "4" * 64},
            },
            pair_config_sha256=("d83f28fae463643fc8969f8944b41c8b87254362fe709344afb7cfd240b8ea5f"),
            pair_economic_cell_count=1_936,
            cumulative_economic_cell_count=3_872,
        )
        completions = (
            SimpleNamespace(
                outcome_replay_manifest_id=1,
                run_fingerprint="1" * 64,
                completed=True,
                summary_row_count=2_904,
                result_artifact_sha256="a" * 64,
                cell_summaries_sha256="f" * 64,
            ),
            SimpleNamespace(
                outcome_replay_manifest_id=2,
                run_fingerprint="2" * 64,
                completed=True,
                summary_row_count=2_904,
                result_artifact_sha256="b" * 64,
                cell_summaries_sha256="0" * 64,
            ),
        )
        return SimpleNamespace(completed=True, release=release, completions=completions)

    services.reserve_pair = reserve_pair
    services.complete_pair = complete_pair
    services.fail_pair = lambda *_args, **_kwargs: pytest.fail("unexpected pair failure")
    services.load_pair_release = lambda *_args, **_kwargs: pytest.fail(
        "unexpected duplicate release load"
    )
    services.fail_unpaired = lambda *_args, **_kwargs: pytest.fail("unexpected unpaired cleanup")
    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline._prepare_phase1a_outcomes",
        fake_prepare,
    )
    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline._execute_reserved_outcome",
        fake_execute,
    )

    report = run_phase1a_p4_outcome_pair(
        project_root=Path.cwd(),
        data_root=tmp_path,
        database_url="postgresql://test",
        services=services,
    )

    assert report.disposition == "PAIR_RELEASED"
    assert report.p4_pair_batch_id == 9
    assert report.p4_pair_release_id == 10
    assert report.pair_release_sha256 == "e" * 64
    assert events == [
        f"prepare:{first_query}:PLAN_ONLY",
        f"prepare:{second_query}:PLAN_ONLY",
        f"prepare:{first_query}:RUN",
        f"prepare:{second_query}:RUN",
        "reserve_pair",
        f"execute:{first_query}",
        f"execute:{second_query}",
        "complete_pair",
    ]


def test_p4_pair_recovers_success_when_complete_commits_then_raises(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    query_ids = (
        "p4_01_opposite_depth_depletion_continuation",
        "p4_02_depth_resistance_reversal",
    )
    services = SimpleNamespace()
    executions = _p4_test_executions(
        tmp_path,
        services,
        execute_flags=(True, True),
    )
    completions = tuple(
        PreparedOutcomeCompletion(
            result=SimpleNamespace(
                path=tmp_path / f"result-{index}.json",
                sha256=("a" if index == 1 else "b") * 64,
            ),
            final_checkpoint=SimpleNamespace(
                path=tmp_path / f"checkpoint-{index}.json",
                sha256=("c" if index == 1 else "d") * 64,
                checkpoint_sequence=472 if index == 1 else 455,
            ),
            completed_source_date_count=472 if index == 1 else 455,
            source_event_count=index,
            detail_record_count=484_968 if index == 1 else 493_680,
            summary_row_count=2_904,
            cell_summaries=(None,) * 2_904,
        )
        for index in (1, 2)
    )

    def fake_prepare(**kwargs: object) -> OutcomePipelineReport | ReservedOutcomeExecution:
        path = Path(kwargs["config_relative_path"])
        index = 0 if "p4_01" in path.name else 1
        if kwargs["mode"] == "PLAN_ONLY":
            return _p4_operator_report(query_ids[index], mode="PLAN_ONLY")
        return executions[index]

    services.reserve_pair = lambda *_args, **_kwargs: SimpleNamespace(
        p4_pair_batch_id=31,
        pair_id="phase1a_p4_liquidity_transition_pair_v1",
        status="PREPARED",
        p4_01_outcome_replay_manifest_id=1,
        p4_02_outcome_replay_manifest_id=2,
    )
    visible_release: list[object] = []

    def commit_then_raise(*_args: object, **_kwargs: object) -> object:
        visible_release.append(_p4_test_release(batch_id=31, release_id=32))
        raise ConnectionError("connection lost after commit")

    loaded: list[dict[str, object]] = []

    def load_release(*_args: object, **kwargs: object) -> object:
        assert visible_release
        loaded.append(kwargs)
        return visible_release[0]

    services.complete_pair = commit_then_raise
    services.load_pair_release = load_release
    services.fail_pair = lambda *_args, **_kwargs: pytest.fail(
        "a verified committed release must not be failed"
    )
    services.fail_unpaired = lambda *_args, **_kwargs: pytest.fail("unexpected unpaired cleanup")
    execution_index = 0

    def fake_execute(*_args: object, **_kwargs: object) -> PreparedOutcomeCompletion:
        nonlocal execution_index
        completion = completions[execution_index]
        execution_index += 1
        return completion

    recovered: list[object] = []

    def validate_recovery(
        release: object,
        _executions: object,
        observed_completions: object,
        **_kwargs: object,
    ) -> tuple[int, int, str]:
        recovered.append(release)
        assert tuple(observed_completions) == completions
        return (31, 32, "e" * 64)

    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline._prepare_phase1a_outcomes",
        fake_prepare,
    )
    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline._execute_reserved_outcome",
        fake_execute,
    )
    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline._validate_p4_recovered_release",
        validate_recovery,
    )

    report = run_phase1a_p4_outcome_pair(
        project_root=Path.cwd(),
        data_root=tmp_path,
        database_url="postgresql://test",
        services=services,
    )

    assert report.disposition == "PAIR_RELEASED"
    assert (report.p4_pair_batch_id, report.p4_pair_release_id) == (31, 32)
    assert report.pair_release_sha256 == "e" * 64
    assert len(loaded) == 1
    assert loaded[0]["p4_01_outcome_replay_manifest_id"] == 1
    assert loaded[0]["p4_02_outcome_replay_manifest_id"] == 2
    assert recovered == visible_release

    execution_index = 0

    def unavailable_release_loader(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("release verification database unavailable")

    services.load_pair_release = unavailable_release_loader
    with pytest.raises(
        Phase1AOutcomePipelineError,
        match="completion remains ambiguous; no failure transition was attempted",
    ):
        run_phase1a_p4_outcome_pair(
            project_root=Path.cwd(),
            data_root=tmp_path,
            database_url="postgresql://test",
            services=services,
        )

    execution_index = 0

    def absent_release_loader(*_args: object, **_kwargs: object) -> object:
        raise OutcomeRegistryStateError(
            "exactly one released P4 pair is required for duplicate reuse"
        )

    failed_batches: list[int] = []
    services.load_pair_release = absent_release_loader
    services.fail_pair = lambda *_args, **kwargs: (
        failed_batches.append(int(kwargs["p4_pair_batch_id"])) or SimpleNamespace(status="FAILED")
    )
    with pytest.raises(Phase1AOutcomePipelineError, match="connection lost after commit"):
        run_phase1a_p4_outcome_pair(
            project_root=Path.cwd(),
            data_root=tmp_path,
            database_url="postgresql://test",
            services=services,
        )
    assert failed_batches == [31]


def test_p4_pair_failure_marks_both_members_atomically(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    query_ids = (
        "p4_01_opposite_depth_depletion_continuation",
        "p4_02_depth_resistance_reversal",
    )
    services = SimpleNamespace()
    executions = tuple(
        ReservedOutcomeExecution(
            prepared=SimpleNamespace(config=SimpleNamespace(query_id=query_id)),
            reports=(),
            terminal_resolution=SimpleNamespace(),
            cache_manifest=SimpleNamespace(),
            run_spec=SimpleNamespace(fingerprint=str(index) * 64),
            reservation=SimpleNamespace(outcome_replay_manifest_id=index, execute=True),
            report=_p4_operator_report(query_id, mode="RUN"),
            database_url="postgresql://test",
            data=tmp_path,
            services=services,
            predecessor_gate=None,
            progress_callback=None,
        )
        for index, query_id in enumerate(query_ids, start=1)
    )
    prepared_calls = 0

    def fake_prepare(**kwargs: object) -> OutcomePipelineReport | ReservedOutcomeExecution:
        nonlocal prepared_calls
        path = Path(kwargs["config_relative_path"])
        index = 0 if "p4_01" in path.name else 1
        if kwargs["mode"] == "PLAN_ONLY":
            return _p4_operator_report(query_ids[index], mode="PLAN_ONLY")
        prepared_calls += 1
        return executions[index]

    services.reserve_pair = lambda *_args, **_kwargs: SimpleNamespace(
        p4_pair_batch_id=11,
        pair_id="phase1a_p4_liquidity_transition_pair_v1",
        status="PREPARED",
        p4_01_outcome_replay_manifest_id=1,
        p4_02_outcome_replay_manifest_id=2,
    )
    failed: list[dict[str, object]] = []
    services.fail_pair = lambda *_args, **kwargs: (
        failed.append(kwargs) or SimpleNamespace(status="FAILED")
    )
    services.complete_pair = lambda *_args, **_kwargs: pytest.fail("unexpected completion")
    services.load_pair_release = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        OutcomeRegistryStateError("exactly one released P4 pair is required for duplicate reuse")
    )
    services.fail_unpaired = lambda *_args, **_kwargs: pytest.fail("unexpected unpaired cleanup")

    def fake_execute(execution: ReservedOutcomeExecution, **_kwargs: object) -> object:
        if execution.prepared.config.query_id == query_ids[1]:
            raise RuntimeError("deliberate second-member failure")
        return PreparedOutcomeCompletion(
            result=SimpleNamespace(),
            final_checkpoint=SimpleNamespace(),
            completed_source_date_count=472,
            source_event_count=1,
            detail_record_count=484_968,
            summary_row_count=2_904,
            cell_summaries=(),
        )

    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline._prepare_phase1a_outcomes",
        fake_prepare,
    )
    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline._execute_reserved_outcome",
        fake_execute,
    )

    with pytest.raises(Phase1AOutcomePipelineError, match="deliberate second-member failure"):
        run_phase1a_p4_outcome_pair(
            project_root=Path.cwd(),
            data_root=tmp_path,
            database_url="postgresql://test",
            services=services,
        )

    assert prepared_calls == 2
    assert failed == [
        {
            "p4_pair_batch_id": 11,
            "p4_01_run_fingerprint": "1" * 64,
            "p4_02_run_fingerprint": "2" * 64,
            "error_message": "RuntimeError: deliberate second-member failure",
        }
    ]


def test_p4_second_preparation_failure_cleans_first_queued_member(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    first_query = "p4_01_opposite_depth_depletion_continuation"
    second_query = "p4_02_depth_resistance_reversal"
    services = SimpleNamespace()
    first_execution = ReservedOutcomeExecution(
        prepared=SimpleNamespace(config=SimpleNamespace(query_id=first_query)),
        reports=(),
        terminal_resolution=SimpleNamespace(),
        cache_manifest=SimpleNamespace(),
        run_spec=SimpleNamespace(fingerprint="1" * 64),
        reservation=SimpleNamespace(outcome_replay_manifest_id=1, execute=True),
        report=_p4_operator_report(first_query, mode="RUN"),
        database_url="postgresql://test",
        data=tmp_path,
        services=services,
        predecessor_gate=None,
        progress_callback=None,
    )
    actual_prepares = 0

    def fake_prepare(**kwargs: object) -> OutcomePipelineReport | ReservedOutcomeExecution:
        nonlocal actual_prepares
        path = Path(kwargs["config_relative_path"])
        query_id = first_query if "p4_01" in path.name else second_query
        if kwargs["mode"] == "PLAN_ONLY":
            return _p4_operator_report(query_id, mode="PLAN_ONLY")
        actual_prepares += 1
        if query_id == first_query:
            return first_execution
        raise RuntimeError("deliberate second preparation failure")

    cleaned: list[dict[str, object]] = []
    services.fail_unpaired = lambda *_args, **kwargs: (
        cleaned.append(kwargs) or SimpleNamespace(status="FAILED")
    )
    services.reserve_pair = lambda *_args, **_kwargs: pytest.fail("unexpected pair reserve")
    services.complete_pair = lambda *_args, **_kwargs: pytest.fail("unexpected completion")
    services.fail_pair = lambda *_args, **_kwargs: pytest.fail("unexpected pair failure")
    services.load_pair_release = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        OutcomeRegistryStateError("exactly one released P4 pair is required for duplicate reuse")
    )
    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline._prepare_phase1a_outcomes",
        fake_prepare,
    )

    with pytest.raises(Phase1AOutcomePipelineError, match="second preparation failure"):
        run_phase1a_p4_outcome_pair(
            project_root=Path.cwd(),
            data_root=tmp_path,
            database_url="postgresql://test",
            services=services,
        )

    assert actual_prepares == 2
    assert cleaned == [
        {
            "outcome_replay_manifest_id": 1,
            "run_fingerprint": "1" * 64,
            "error_message": "RuntimeError: deliberate second preparation failure",
        }
    ]


def test_p4_mixed_duplicate_and_new_reservation_cleans_new_member(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    query_ids = (
        "p4_01_opposite_depth_depletion_continuation",
        "p4_02_depth_resistance_reversal",
    )
    services = SimpleNamespace()
    executions = tuple(
        ReservedOutcomeExecution(
            prepared=SimpleNamespace(config=SimpleNamespace(query_id=query_id)),
            reports=(),
            terminal_resolution=SimpleNamespace(),
            cache_manifest=SimpleNamespace(),
            run_spec=SimpleNamespace(fingerprint=str(index) * 64),
            reservation=SimpleNamespace(
                outcome_replay_manifest_id=index,
                execute=index == 2,
            ),
            report=_p4_operator_report(query_id, mode="RUN"),
            database_url="postgresql://test",
            data=tmp_path,
            services=services,
            predecessor_gate=None,
            progress_callback=None,
        )
        for index, query_id in enumerate(query_ids, start=1)
    )

    def fake_prepare(**kwargs: object) -> OutcomePipelineReport | ReservedOutcomeExecution:
        path = Path(kwargs["config_relative_path"])
        index = 0 if "p4_01" in path.name else 1
        if kwargs["mode"] == "PLAN_ONLY":
            return _p4_operator_report(query_ids[index], mode="PLAN_ONLY")
        return executions[index]

    cleaned: list[int] = []
    services.fail_unpaired = lambda *_args, **kwargs: (
        cleaned.append(int(kwargs["outcome_replay_manifest_id"]))
        or SimpleNamespace(status="FAILED")
    )
    services.reserve_pair = lambda *_args, **_kwargs: pytest.fail("unexpected pair reserve")
    services.complete_pair = lambda *_args, **_kwargs: pytest.fail("unexpected completion")
    services.fail_pair = lambda *_args, **_kwargs: pytest.fail("unexpected pair failure")
    services.load_pair_release = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        OutcomeRegistryStateError("exactly one released P4 pair is required for duplicate reuse")
    )
    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline._prepare_phase1a_outcomes",
        fake_prepare,
    )

    with pytest.raises(Phase1AOutcomePipelineError, match="cannot mix"):
        run_phase1a_p4_outcome_pair(
            project_root=Path.cwd(),
            data_root=tmp_path,
            database_url="postgresql://test",
            services=services,
        )

    assert cleaned == [2]


@pytest.mark.parametrize("execute_flags", [(True, False), (False, True)])
def test_p4_mixed_reservation_hydrates_concurrently_released_pair(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    execute_flags: tuple[bool, bool],
) -> None:
    services = SimpleNamespace()
    executions = _p4_test_executions(
        tmp_path,
        services,
        execute_flags=execute_flags,
    )
    _patch_p4_test_preparation(monkeypatch, executions)
    loaded: list[dict[str, object]] = []
    services.load_pair_release = lambda *_args, **kwargs: (
        loaded.append(kwargs) or _p4_test_release(batch_id=41)
    )
    services.reserve_pair = lambda *_args, **_kwargs: pytest.fail("unexpected pair reserve")
    services.complete_pair = lambda *_args, **_kwargs: pytest.fail("unexpected completion")
    services.fail_pair = lambda *_args, **_kwargs: pytest.fail("unexpected pair failure")
    services.fail_unpaired = lambda *_args, **_kwargs: pytest.fail(
        "a released pair must not be cleaned as unpaired"
    )

    report = run_phase1a_p4_outcome_pair(
        project_root=Path.cwd(),
        data_root=tmp_path,
        database_url="postgresql://test",
        services=services,
    )

    assert (report.p4_pair_batch_id, report.p4_pair_release_id) == (41, 42)
    assert (report.first.disposition, report.second.disposition) == (
        "SKIPPED_DUPLICATE",
        "SKIPPED_DUPLICATE",
    )
    assert len(loaded) == 1


def test_p4_released_pair_reservation_hydrates_without_economics_or_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    services = SimpleNamespace()
    executions = _p4_test_executions(
        tmp_path,
        services,
        execute_flags=(True, True),
    )
    _patch_p4_test_preparation(monkeypatch, executions)
    services.reserve_pair = lambda *_args, **_kwargs: SimpleNamespace(
        p4_pair_batch_id=51,
        pair_id="phase1a_p4_liquidity_transition_pair_v1",
        status="RELEASED",
        p4_01_outcome_replay_manifest_id=1,
        p4_02_outcome_replay_manifest_id=2,
    )
    loaded: list[dict[str, object]] = []
    services.load_pair_release = lambda *_args, **kwargs: (
        loaded.append(kwargs) or _p4_test_release(batch_id=51)
    )
    services.complete_pair = lambda *_args, **_kwargs: pytest.fail("unexpected completion")
    services.fail_pair = lambda *_args, **_kwargs: pytest.fail(
        "a RELEASED reservation must not be failed"
    )
    services.fail_unpaired = lambda *_args, **_kwargs: pytest.fail("unexpected cleanup")
    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline._execute_reserved_outcome",
        lambda *_args, **_kwargs: pytest.fail("released pair must not execute economics"),
    )

    report = run_phase1a_p4_outcome_pair(
        project_root=Path.cwd(),
        data_root=tmp_path,
        database_url="postgresql://test",
        services=services,
    )

    assert report.p4_pair_batch_id == 51
    assert (report.first.disposition, report.second.disposition) == (
        "SKIPPED_DUPLICATE",
        "SKIPPED_DUPLICATE",
    )
    assert len(loaded) == 1


def test_p4_prepared_then_start_error_hydrates_peer_release(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    services = SimpleNamespace()
    executions = _p4_test_executions(
        tmp_path,
        services,
        execute_flags=(True, True),
    )
    _patch_p4_test_preparation(monkeypatch, executions)
    services.reserve_pair = lambda *_args, **_kwargs: SimpleNamespace(
        p4_pair_batch_id=61,
        pair_id="phase1a_p4_liquidity_transition_pair_v1",
        status="PREPARED",
        p4_01_outcome_replay_manifest_id=1,
        p4_02_outcome_replay_manifest_id=2,
    )
    peer_released: list[bool] = []

    def start_after_peer_release(*_args: object, **_kwargs: object) -> object:
        peer_released.append(True)
        raise OutcomeRegistryStateError("P4 pair is already RELEASED")

    def load_release(*_args: object, **_kwargs: object) -> object:
        assert peer_released
        return _p4_test_release(batch_id=61)

    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline._execute_reserved_outcome",
        start_after_peer_release,
    )
    services.load_pair_release = load_release
    services.complete_pair = lambda *_args, **_kwargs: pytest.fail("unexpected completion")
    services.fail_pair = lambda *_args, **_kwargs: pytest.fail(
        "a peer-released pair must not be failed"
    )
    services.fail_unpaired = lambda *_args, **_kwargs: pytest.fail("unexpected cleanup")

    report = run_phase1a_p4_outcome_pair(
        project_root=Path.cwd(),
        data_root=tmp_path,
        database_url="postgresql://test",
        services=services,
    )

    assert report.p4_pair_batch_id == 61
    assert (report.first.disposition, report.second.disposition) == (
        "SKIPPED_DUPLICATE",
        "SKIPPED_DUPLICATE",
    )


def test_p4_failure_rechecks_release_after_atomic_fail_rejects_race(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    services = SimpleNamespace()
    executions = _p4_test_executions(
        tmp_path,
        services,
        execute_flags=(True, True),
    )
    _patch_p4_test_preparation(monkeypatch, executions)
    services.reserve_pair = lambda *_args, **_kwargs: SimpleNamespace(
        p4_pair_batch_id=71,
        pair_id="phase1a_p4_liquidity_transition_pair_v1",
        status="PREPARED",
        p4_01_outcome_replay_manifest_id=1,
        p4_02_outcome_replay_manifest_id=2,
    )
    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline._execute_reserved_outcome",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("start race")),
    )
    load_count = 0

    def load_release(*_args: object, **_kwargs: object) -> object:
        nonlocal load_count
        load_count += 1
        if load_count == 1:
            raise OutcomeRegistryStateError(
                "exactly one released P4 pair is required for duplicate reuse"
            )
        return _p4_test_release(batch_id=71)

    fail_calls: list[int] = []

    def fail_after_peer_release(*_args: object, **kwargs: object) -> object:
        fail_calls.append(int(kwargs["p4_pair_batch_id"]))
        raise OutcomeRegistryStateError("P4 pair is already RELEASED")

    services.load_pair_release = load_release
    services.fail_pair = fail_after_peer_release
    services.complete_pair = lambda *_args, **_kwargs: pytest.fail("unexpected completion")
    services.fail_unpaired = lambda *_args, **_kwargs: pytest.fail("unexpected cleanup")

    report = run_phase1a_p4_outcome_pair(
        project_root=Path.cwd(),
        data_root=tmp_path,
        database_url="postgresql://test",
        services=services,
    )

    assert report.p4_pair_batch_id == 71
    assert fail_calls == [71]
    assert load_count == 2
    assert (report.first.disposition, report.second.disposition) == (
        "SKIPPED_DUPLICATE",
        "SKIPPED_DUPLICATE",
    )


def test_p4_mixed_cleanup_rechecks_release_after_unpaired_rejection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    services = SimpleNamespace()
    executions = _p4_test_executions(
        tmp_path,
        services,
        execute_flags=(False, True),
    )
    _patch_p4_test_preparation(monkeypatch, executions)
    load_count = 0

    def load_release(*_args: object, **_kwargs: object) -> object:
        nonlocal load_count
        load_count += 1
        if load_count == 1:
            raise OutcomeRegistryStateError(
                "exactly one released P4 pair is required for duplicate reuse"
            )
        return _p4_test_release(batch_id=81)

    cleanup_calls: list[int] = []

    def reject_unpaired_cleanup(*_args: object, **kwargs: object) -> object:
        cleanup_calls.append(int(kwargs["outcome_replay_manifest_id"]))
        raise OutcomeRegistryStateError("P4 member is now bound to a RELEASED pair")

    services.load_pair_release = load_release
    services.fail_unpaired = reject_unpaired_cleanup
    services.reserve_pair = lambda *_args, **_kwargs: pytest.fail("unexpected pair reserve")
    services.complete_pair = lambda *_args, **_kwargs: pytest.fail("unexpected completion")
    services.fail_pair = lambda *_args, **_kwargs: pytest.fail("unexpected pair failure")

    report = run_phase1a_p4_outcome_pair(
        project_root=Path.cwd(),
        data_root=tmp_path,
        database_url="postgresql://test",
        services=services,
    )

    assert report.p4_pair_batch_id == 81
    assert cleanup_calls == [2]
    assert load_count == 2
    assert (report.first.disposition, report.second.disposition) == (
        "SKIPPED_DUPLICATE",
        "SKIPPED_DUPLICATE",
    )


def test_p4_pair_duplicate_requires_exact_existing_release(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    query_ids = (
        "p4_01_opposite_depth_depletion_continuation",
        "p4_02_depth_resistance_reversal",
    )
    services = SimpleNamespace()
    executions = tuple(
        ReservedOutcomeExecution(
            prepared=SimpleNamespace(
                config=replace(
                    load_outcome_replay_config(
                        Path.cwd(),
                        config_path=Path(
                            "configs/research/phase1a_p4_01_outcome_replay_v1.toml"
                            if index == 1
                            else "configs/research/phase1a_p4_02_outcome_replay_v1.toml"
                        ),
                    )
                )
            ),
            reports=(),
            terminal_resolution=SimpleNamespace(),
            cache_manifest=SimpleNamespace(),
            run_spec=SimpleNamespace(fingerprint=str(index) * 64),
            reservation=SimpleNamespace(outcome_replay_manifest_id=index, execute=False),
            report=_p4_operator_report(query_id, mode="RUN"),
            database_url="postgresql://test",
            data=tmp_path,
            services=services,
            predecessor_gate=None,
            progress_callback=None,
        )
        for index, query_id in enumerate(query_ids, start=1)
    )

    def fake_prepare(**kwargs: object) -> OutcomePipelineReport | ReservedOutcomeExecution:
        path = Path(kwargs["config_relative_path"])
        index = 0 if "p4_01" in path.name else 1
        if kwargs["mode"] == "PLAN_ONLY":
            return _p4_operator_report(query_ids[index], mode="PLAN_ONLY")
        return executions[index]

    release = SimpleNamespace(
        release_sha256="e" * 64,
        p4_pair_batch_id=21,
        p4_pair_release_id=22,
        pair_id="phase1a_p4_liquidity_transition_pair_v1",
        p4_01_outcome_replay_manifest_id=1,
        p4_02_outcome_replay_manifest_id=2,
        p4_01_run_fingerprint="1" * 64,
        p4_02_run_fingerprint="2" * 64,
        p4_01_result_artifact_sha256="a" * 64,
        p4_02_result_artifact_sha256="b" * 64,
        decision_sha256s={
            query_ids[0]: {"LONG": "1" * 64, "SHORT": "2" * 64},
            query_ids[1]: {"LONG": "3" * 64, "SHORT": "4" * 64},
        },
        pair_config_sha256=("d83f28fae463643fc8969f8944b41c8b87254362fe709344afb7cfd240b8ea5f"),
        pair_economic_cell_count=1_936,
        cumulative_economic_cell_count=3_872,
    )
    loaded: list[dict[str, object]] = []
    services.load_pair_release = lambda *_args, **kwargs: loaded.append(kwargs) or release
    services.reserve_pair = lambda *_args, **_kwargs: pytest.fail("unexpected pair reserve")
    services.complete_pair = lambda *_args, **_kwargs: pytest.fail("unexpected completion")
    services.fail_pair = lambda *_args, **_kwargs: pytest.fail("unexpected pair failure")
    services.fail_unpaired = lambda *_args, **_kwargs: pytest.fail("unexpected unpaired cleanup")
    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline._prepare_phase1a_outcomes",
        fake_prepare,
    )

    report = run_phase1a_p4_outcome_pair(
        project_root=Path.cwd(),
        data_root=tmp_path,
        database_url="postgresql://test",
        services=services,
    )

    assert report.disposition == "PAIR_RELEASED"
    assert report.p4_pair_batch_id == 21
    assert report.p4_pair_release_id == 22
    assert report.pair_release_sha256 == "e" * 64
    assert (report.first.disposition, report.second.disposition) == (
        "SKIPPED_DUPLICATE",
        "SKIPPED_DUPLICATE",
    )
    assert sum(item.summary_row_count for item in (report.first, report.second)) == 5_808
    assert len(loaded) == 1
    assert loaded[0]["data_root"] == tmp_path


def test_phase1a_p4_pair_cli_exposes_no_individual_candidate_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: Any,
) -> None:
    parser = cli.build_parser()
    arguments = parser.parse_args(["research", "phase1a-p4-pair-outcomes", "--plan-only", "--json"])

    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline.run_phase1a_p4_outcome_pair",
        lambda **kwargs: SimpleNamespace(as_dict=lambda: {"mode": kwargs["mode"]}),
    )
    monkeypatch.setattr(
        cli.Settings,
        "from_env",
        lambda: SimpleNamespace(data_root=Path("data"), database_url="postgresql://test"),
    )

    assert arguments.handler(arguments) == 0
    assert json.loads(capsys.readouterr().out) == {"mode": "PLAN_ONLY"}
    with pytest.raises(SystemExit):
        parser.parse_args(["research", "phase1a-p4-01-outcomes"])
    with pytest.raises(SystemExit):
        parser.parse_args(["research", "phase1a-p4-02-outcomes"])


def test_outcome_run_spec_records_rich_portable_plan_and_cache_lineage() -> None:
    config = load_outcome_replay_config(Path.cwd())
    rich_sha = "1" * 64
    prepared = SimpleNamespace(
        config=config,
        calendar=SimpleNamespace(sha256="2" * 64),
        split=SimpleNamespace(sha256="3" * 64),
        source_artifacts=SimpleNamespace(source_artifact_manifest_sha256=rich_sha),
        discovery=SimpleNamespace(
            artifact_manifest_sha256="4" * 64,
            input_manifest_sha256="5" * 64,
            signal_manifest_sha256="6" * 64,
        ),
        plan=SimpleNamespace(
            partitions=tuple(range(485)),
            plan_sha256="7" * 64,
            source_record_manifest_sha256="8" * 64,
            footer_manifest_sha256="9" * 64,
            source_hash_manifest_sha256="a" * 64,
        ),
    )

    spec = _make_run_spec(
        prepared,
        cache_manifest_sha256="b" * 64,
        terminal_resolution=_resolution(
            contract_key="6EH2",
            source_date=date(2022, 2, 28),
            event_index=10,
            ts_recv_ns=100,
        ),
        code_commit="c" * 40,
        code_snapshot_sha256="d" * 64,
        dependency_sha256="e" * 64,
        runtime={"test": True},
        feature_sha256="f" * 64,
    )
    parameters = json.loads(spec.canonical_json())["parameters"]

    assert spec.run_kind == "OUTCOME_BUILD"
    assert spec.source_manifest_hashes["phase1a_p5_cache_manifest_v1"] == "b" * 64
    assert parameters["portable_discovery_artifact_manifest_sha256"] == "4" * 64
    assert parameters["portable_signal_manifest_sha256"] == "6" * 64
    assert parameters["input_plan_sha256"] == "7" * 64
    assert parameters["expected_completed_source_date_count"] == 485
    assert parameters["expected_last_completed_source_date"] == "2023-08-31"
    assert (
        parameters["terminal_resolution_sha256"]
        == json.loads(spec.canonical_json())["terminal_policy"]["terminal_resolution_sha256"]
    )
    for name, value in phase1a_p5_outcome_parameters(rich_sha).items():
        assert parameters[name] == value


def test_shared_orchestration_checkpoints_each_date_and_costs_all_cache_months(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    (data_root / "derived").mkdir(parents=True)
    first_day = date(2022, 1, 3)
    second_day = date(2022, 2, 1)
    partitions = (
        DailyReplayPartition(
            cache_spec=DailyCacheSpec(
                source_date=first_day,
                source_parquet_path=Path("/first.parquet"),
                source_sha256="1" * 64,
                raw_symbol="6EH2",
                event_index_offset=0,
            ),
            source_relative_uri="raw/first.parquet",
            session_ordinal=0,
            contract_expiry_month=date(2022, 3, 1),
            terminal=False,
        ),
        DailyReplayPartition(
            cache_spec=DailyCacheSpec(
                source_date=second_day,
                source_parquet_path=Path("/second.parquet"),
                source_sha256="2" * 64,
                raw_symbol="6EH2",
                event_index_offset=1,
            ),
            source_relative_uri="raw/second.parquet",
            session_ordinal=1,
            contract_expiry_month=date(2022, 3, 1),
            terminal=True,
        ),
    )

    def report(day: date, event_index: int, source_sha256: str) -> DailyCacheReport:
        timestamp = 100 + event_index * 100
        return DailyCacheReport(
            path=Path(f"/{day.isoformat()}.cache.parquet"),
            sha256="3" * 64,
            byte_size=100,
            disposition="CREATED",
            source_date=day,
            source_path=f"/{day.isoformat()}.parquet",
            source_sha256=source_sha256,
            raw_symbol="6EH2",
            instrument_id=1,
            event_index_offset=event_index - 1,
            source_row_count=1,
            cached_quote_count=1,
            valid_quote_count=1,
            first_event_index=event_index,
            last_event_index=event_index,
            first_ts_recv_ns=timestamp,
            last_ts_recv_ns=timestamp,
            last_valid_event_index=event_index,
            last_valid_ts_recv_ns=timestamp,
        )

    reports = (
        report(first_day, 1, "1" * 64),
        report(second_day, 2, "2" * 64),
    )
    config = replace(
        load_outcome_replay_config(Path.cwd()),
        expected_completed_source_date_count=2,
        expected_last_completed_source_date=second_day,
    )

    class Signal:
        signal_id = "signal-1"
        direction = Direction.LONG

        @staticmethod
        def to_seed() -> SignalSeed:
            return SignalSeed(
                signal_id="signal-1",
                decision_ts_recv_ns=100,
                utc_month="2022-01",
                direction=Direction.LONG,
                contract_key="6EH2",
            )

    prepared = SimpleNamespace(
        config=config,
        calendar=SimpleNamespace(sha256="9" * 64),
        split=SimpleNamespace(sha256="a" * 64),
        discovery=SimpleNamespace(
            signals=(Signal(),),
            input_manifest_sha256="b" * 64,
            artifact_manifest_sha256="c" * 64,
            signal_manifest_sha256="d" * 64,
        ),
        plan=OutcomeInputPlan(
            discovery_input_manifest_sha256="b" * 64,
            footer_manifest_sha256="5" * 64,
            source_hash_manifest_sha256="7" * 64,
            source_record_manifest_sha256="8" * 64,
            calendar_sha256="9" * 64,
            partitions=partitions,
        ),
        source_artifacts=SimpleNamespace(source_artifact_manifest_sha256="e" * 64),
    )
    months: list[tuple[str, ...]] = []
    detail_rows: list[tuple[object, ...]] = []
    checkpoint_calls: list[dict[str, object]] = []
    progress_events: list[OutcomeProgress] = []

    def economics_factory(**kwargs: object) -> OutcomeEconomicsAccumulator:
        months.append(kwargs["observed_utc_months"])  # type: ignore[arg-type]
        return _Phase1ACompleteTestEconomics(**kwargs)  # type: ignore[arg-type]

    def read_cache(cache_report: DailyCacheReport):
        event_index = cache_report.first_event_index
        yield CachedExecutableQuote(
            contract_key="6EH2",
            source_date=cache_report.source_date,
            source_sha256=cache_report.source_sha256,
            sequence=event_index,
            source_row_index=0,
            row_group_index=0,
            row_index=0,
            invalid_reason=None,
            quote=ExecutableQuote(
                event_index=event_index,
                ts_recv_ns=cache_report.first_ts_recv_ns,
                best_bid_ticks=100,
                best_ask_ticks=102,
                valid=True,
            ),
        )

    def publish_shard(records: tuple[object, ...], **kwargs: object) -> object:
        detail_rows.append(records)
        return SimpleNamespace(
            records=records,
            shard_sequence=kwargs["shard_sequence"],
            sha256=f"{int(kwargs['shard_sequence']):064x}",
        )

    def publish_checkpoint(**kwargs: object) -> object:
        checkpoint_calls.append(kwargs)
        sequence = int(kwargs["checkpoint_sequence"])
        return SimpleNamespace(
            path=tmp_path / f"checkpoint-{sequence}.json",
            sha256=f"{sequence + 10:064x}",
            checkpoint_sequence=sequence,
            last_completed_source_date=kwargs["last_completed_source_date"],
            progress_metadata={
                "cache_manifest_sha256": "f" * 64,
                "checkpoint_sequence": sequence,
            },
        )

    def register_checkpoint(*args: object, **kwargs: object) -> object:
        sequence = int(kwargs["checkpoint_sequence"])
        return SimpleNamespace(checkpoint_artifact_sha256=f"{sequence + 10:064x}")

    final = SimpleNamespace(path=tmp_path / "final.json", sha256="0" * 64)
    artifacts = SimpleNamespace(
        publish_result_shard=publish_shard,
        read_result_shard=lambda *args, **kwargs: None,
        publish_checkpoint=publish_checkpoint,
        load_checkpoint_artifact=lambda *args, **kwargs: None,
        publish_result=lambda **kwargs: final,
        load_result=lambda result, **kwargs: result,
    )
    services = SimpleNamespace(
        start_replay=lambda *args, **kwargs: None,
        load_checkpoint=lambda *args, **kwargs: None,
        replay_factory=SharedReplay,
        replay_from_checkpoint=SharedReplay.from_checkpoint,
        economics_factory=economics_factory,
        read_cache=read_cache,
        artifacts=artifacts,
        register_checkpoint=register_checkpoint,
        complete_replay=lambda *args, **kwargs: None,
    )

    result, final_checkpoint, completed, events, records, summaries = _run_replay(
        prepared=prepared,
        reports=reports,
        terminal_resolution=_resolution(
            contract_key="6EH2",
            source_date=second_day,
            event_index=2,
            ts_recv_ns=300,
            eligible_partition_count=2,
        ),
        cache_manifest=SimpleNamespace(sha256="f" * 64),
        run_spec=SimpleNamespace(fingerprint="1" * 64),
        reservation=SimpleNamespace(outcome_replay_manifest_id=1),
        database_url="postgresql://unused",
        data=data_root,
        services=services,
        progress_callback=progress_events.append,
    )

    assert result is final
    assert final_checkpoint is not None
    assert (completed, events, records, summaries) == (2, 2, 1_452, 2_904)
    assert months == [("2022-01", "2022-02")]
    assert [len(rows) for rows in detail_rows] == [1_452, 0]
    assert [call["checkpoint_sequence"] for call in checkpoint_calls] == [1, 2]
    assert checkpoint_calls[-1]["input_lineage"]["expected_completed_source_date_count"] == 2
    assert checkpoint_calls[-1]["input_lineage"]["expected_last_completed_source_date"] == (
        "2022-02-01"
    )
    assert [item.completed for item in progress_events] == [1, 2]
    assert progress_events[-1].source_event_count == 2
    assert progress_events[-1].detail_record_count == 1_452


def test_resume_streams_each_detail_shard_and_final_reload_uses_bounded_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    (data_root / "derived").mkdir(parents=True)
    source_date = date(2022, 1, 3)
    partition = _partition("6EH2", terminal=True)
    cache_report = _report("6EH2", terminal_index=4)
    terminal_resolution = _resolution(
        contract_key="6EH2",
        source_date=source_date,
        event_index=4,
        ts_recv_ns=300,
    )
    config = replace(
        load_outcome_replay_config(Path.cwd()),
        expected_completed_source_date_count=1,
        expected_last_completed_source_date=source_date,
    )
    prepared = SimpleNamespace(
        config=config,
        calendar=SimpleNamespace(sha256="1" * 64),
        split=SimpleNamespace(sha256="2" * 64),
        discovery=SimpleNamespace(
            signals=(SimpleNamespace(signal_id="signal-1", direction=Direction.LONG),),
            input_manifest_sha256="3" * 64,
            artifact_manifest_sha256="4" * 64,
            signal_manifest_sha256="5" * 64,
        ),
        plan=OutcomeInputPlan(
            discovery_input_manifest_sha256="3" * 64,
            footer_manifest_sha256="7" * 64,
            source_hash_manifest_sha256="9" * 64,
            source_record_manifest_sha256="a" * 64,
            calendar_sha256="1" * 64,
            partitions=(partition,),
        ),
        source_artifacts=SimpleNamespace(source_artifact_manifest_sha256="b" * 64),
    )
    cache_manifest = SimpleNamespace(sha256="c" * 64)
    shard = SimpleNamespace(shard_sequence=1)

    class Economics:
        def __init__(self) -> None:
            self.record_count = 0
            self.records: list[object] = []

        def extend(self, records: tuple[object, ...]) -> None:
            self.records.extend(records)
            self.record_count += len(records)

        @staticmethod
        def finalize() -> tuple[str, ...]:
            return ("summary",)

    replay = SimpleNamespace(
        source_event_count=17,
        drained_record_count=2,
        result_record_count=2,
        completed_source_date=source_date,
        finished=True,
    )
    checkpoint_path = tmp_path / "checkpoint.json"
    loaded_checkpoint = SimpleNamespace(
        replay=replay,
        detail_shards=(shard,),
        loaded_detail_shards=(),
        cache_manifest=cache_manifest,
        input_lineage=_input_lineage(prepared, terminal_resolution),
        checkpoint_sequence=1,
        last_completed_source_date=source_date,
        path=checkpoint_path,
        sha256="d" * 64,
    )
    latest = SimpleNamespace(
        checkpoint_artifact_path=checkpoint_path,
        checkpoint_artifact_sha256="d" * 64,
        checkpoint_artifact_byte_size=100,
        progress_metadata={"progress": True},
        completed_source_date_count=1,
        last_completed_source_date=source_date,
        source_event_count=17,
    )
    checkpoint_load_kwargs: list[dict[str, object]] = []
    shard_reads: list[object] = []
    final_load_kwargs: list[dict[str, object]] = []

    def load_checkpoint_artifact(*args: object, **kwargs: object) -> object:
        checkpoint_load_kwargs.append(dict(kwargs))
        return loaded_checkpoint

    def read_result_shard(value: object, **kwargs: object) -> object:
        shard_reads.append(value)
        return SimpleNamespace(records=("row-1", "row-2"))

    final = SimpleNamespace(path=tmp_path / "final.json", sha256="e" * 64)

    def load_result(value: object, **kwargs: object) -> object:
        final_load_kwargs.append(dict(kwargs))
        return value

    artifacts = SimpleNamespace(
        load_checkpoint_artifact=load_checkpoint_artifact,
        read_result_shard=read_result_shard,
        publish_result=lambda **kwargs: final,
        load_result=load_result,
    )
    services = SimpleNamespace(
        start_replay=lambda *args, **kwargs: None,
        load_checkpoint=lambda *args, **kwargs: latest,
        economics_factory=lambda **kwargs: Economics(),
        artifacts=artifacts,
        complete_replay=lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "systematic_fx.research.phase1a_outcome_pipeline.validate_complete_cell_summaries",
        lambda summaries, **kwargs: (tuple(summaries), "f" * 64),
    )

    result, final_checkpoint, completed, events, records, summaries = _run_replay(
        prepared=prepared,
        reports=(cache_report,),
        terminal_resolution=terminal_resolution,
        cache_manifest=cache_manifest,
        run_spec=SimpleNamespace(fingerprint="0" * 64),
        reservation=SimpleNamespace(outcome_replay_manifest_id=7),
        database_url="postgresql://unused",
        data=data_root,
        services=services,
    )

    assert result is final
    assert final_checkpoint is loaded_checkpoint
    assert (completed, events, records, summaries) == (1, 17, 2, 1)
    assert shard_reads == [shard]
    assert checkpoint_load_kwargs[0]["verify_detail_content"] is False
    assert checkpoint_load_kwargs[0]["retain_detail_records"] is False
    assert checkpoint_load_kwargs[0]["verify_cache_content"] is False
    assert len(final_load_kwargs) == 1
    assert final_load_kwargs[0]["data_root"] == data_root
    assert final_load_kwargs[0]["verify_cache_content"] is False
    assert final_load_kwargs[0]["verify_detail_content"] is True
    assert final_load_kwargs[0]["identity"].query_id == "p5_01_range_expansion_flow_continuation"


def test_pipeline_core_roundtrips_real_cache_shard_checkpoint_and_final_artifacts(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    (data_root / "derived").mkdir(parents=True)
    source_date = date(2024, 1, 2)
    cache_report = _write_tiny_cache(data_root, source_date=source_date)
    cache_manifest = publish_cache_manifest(
        [cache_report],
        data_root=data_root,
        cache_plan_sha256="4" * 64,
        input_manifest_sha256="b" * 64,
    )
    partition = DailyReplayPartition(
        cache_spec=DailyCacheSpec(
            source_date=source_date,
            source_parquet_path=Path(cache_report.source_path),
            source_sha256=cache_report.source_sha256,
            raw_symbol=cache_report.raw_symbol,
            event_index_offset=cache_report.event_index_offset,
        ),
        source_relative_uri="mbp-10/2024-01-02.parquet",
        session_ordinal=0,
        contract_expiry_month=date(2024, 3, 1),
        terminal=True,
    )
    terminal_resolution = _resolution(
        contract_key=cache_report.raw_symbol,
        source_date=source_date,
        event_index=cache_report.last_valid_event_index,  # type: ignore[arg-type]
        ts_recv_ns=cache_report.last_valid_ts_recv_ns,  # type: ignore[arg-type]
    )
    config = replace(
        load_outcome_replay_config(Path.cwd()),
        expected_completed_source_date_count=1,
        expected_last_completed_source_date=source_date,
    )

    class Signal:
        signal_id = "signal-1"
        direction = Direction.LONG

        @staticmethod
        def to_seed() -> SignalSeed:
            return SignalSeed(
                signal_id="signal-1",
                decision_ts_recv_ns=cache_report.first_ts_recv_ns - 2_000_000_000,
                utc_month="2024-01",
                direction=Direction.LONG,
                contract_key="6EH4",
            )

    prepared = SimpleNamespace(
        config=config,
        calendar=SimpleNamespace(sha256="9" * 64),
        split=SimpleNamespace(sha256="a" * 64),
        discovery=SimpleNamespace(
            signals=(Signal(),),
            input_manifest_sha256="b" * 64,
            artifact_manifest_sha256="c" * 64,
            signal_manifest_sha256="d" * 64,
        ),
        plan=OutcomeInputPlan(
            discovery_input_manifest_sha256="b" * 64,
            footer_manifest_sha256="5" * 64,
            source_hash_manifest_sha256="7" * 64,
            source_record_manifest_sha256="8" * 64,
            calendar_sha256="9" * 64,
            partitions=(partition,),
        ),
        source_artifacts=SimpleNamespace(source_artifact_manifest_sha256="e" * 64),
    )
    checkpoints: list[object] = []
    completed_paths: list[Path] = []

    def publish_checkpoint(**kwargs: object) -> object:
        artifact = publish_outcome_checkpoint(**kwargs)  # type: ignore[arg-type]
        checkpoints.append(artifact)
        return artifact

    def register_checkpoint(*args: object, **kwargs: object) -> object:
        artifact = checkpoints[-1]
        assert kwargs["checkpoint_artifact_path"] == artifact.path
        assert kwargs["progress_metadata"] == artifact.progress_metadata
        return SimpleNamespace(checkpoint_artifact_sha256=artifact.sha256)

    artifacts = SimpleNamespace(
        publish_result_shard=publish_detail_shard,
        read_result_shard=lambda *args, **kwargs: None,
        publish_checkpoint=publish_checkpoint,
        load_checkpoint_artifact=load_outcome_checkpoint,
        publish_result=publish_final_result_manifest,
        load_result=load_final_result_manifest,
    )
    services = SimpleNamespace(
        start_replay=lambda *args, **kwargs: None,
        load_checkpoint=lambda *args, **kwargs: None,
        replay_factory=SharedReplay,
        replay_from_checkpoint=SharedReplay.from_checkpoint,
        economics_factory=_Phase1ACompleteTestEconomics,
        read_cache=read_daily_executable_cache,
        artifacts=artifacts,
        register_checkpoint=register_checkpoint,
        complete_replay=lambda *args, **kwargs: completed_paths.append(
            kwargs["result_artifact_path"]
        ),
    )

    result, final_checkpoint, completed, events, records, summaries = _run_replay(
        prepared=prepared,
        reports=(cache_report,),
        terminal_resolution=terminal_resolution,
        cache_manifest=cache_manifest,
        run_spec=SimpleNamespace(fingerprint="1" * 64),
        reservation=SimpleNamespace(outcome_replay_manifest_id=7),
        database_url="postgresql://unused",
        data=data_root,
        services=services,
    )

    loaded_checkpoint = load_outcome_checkpoint(
        checkpoints[0],  # type: ignore[arg-type]
        data_root=data_root,
        expected_progress_metadata=checkpoints[0].progress_metadata,
    )
    loaded_result = result
    assert (completed, events, records, summaries) == (1, 1, 1_452, 2_904)
    assert loaded_checkpoint.replay.finished
    assert len(loaded_checkpoint.loaded_detail_shards[0].records) == 1_452
    assert final_checkpoint is checkpoints[0]
    assert loaded_result.artifact.sha256 == result.artifact.sha256
    assert loaded_result.artifact.summary_row_count == 2_904
    assert loaded_result.input_lineage["expected_completed_source_date_count"] == 1
    assert loaded_result.input_lineage["expected_last_completed_source_date"] == "2024-01-02"
    assert loaded_result.final_checkpoint.artifact.checkpoint_sequence == 1
    assert loaded_result.final_checkpoint.artifact.last_completed_source_date == source_date
    assert completed_paths == [result.artifact.path]
