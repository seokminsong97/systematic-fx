"""PostgreSQL release gate for State-Conditional Bar Model v2 governance.

This test intentionally refuses the persistent application and integration
databases.  The caller must create a uniquely named disposable database and
provide its URL through ``SYSTEMATIC_FX_BAR_STATE_GATE_DATABASE_URL``.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import replace
from decimal import Decimal
from fractions import Fraction
from functools import lru_cache
from pathlib import Path

import psycopg
import pyarrow as pa
import pytest
from psycopg import IsolationLevel
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.db.bar_registry import register_published_bar_artifact
from systematic_fx.db.bar_state_registry import (
    BAR_STATE_DATASET_KEY,
    BarStateRegistryDriftError,
    abort_bar_state_run_attempt,
    register_bar_state_artifact_link,
    register_bar_state_campaign,
    register_bar_state_run_spec,
    register_terminal_bar_state_result,
    require_clean_bar_state_predecessor,
    validate_reused_bar_state_attempt,
)
from systematic_fx.db.bootstrap import _url_database_name
from systematic_fx.db.migrations import apply_migrations, discover_migrations
from systematic_fx.db.run_registry import (
    RunAttemptReservation,
    reserve_run_attempt,
    start_run_attempt,
)
from systematic_fx.research.bar_state_artifacts import (
    BAR_STATE_ARTIFACT_SCHEMA_BY_KIND,
    BAR_STATE_BAR_DATASET_MANIFEST_SHA256,
    BAR_STATE_RAW_SOURCE_MANIFEST_SHA256,
    BarStateArtifactError,
    BarStateArtifactLineage,
    bar_state_candidate_selection_projection,
    bar_state_global_result_projection,
    bar_state_model_package_projection,
    bar_state_price_policy_from_selection,
    frozen_bar_state_discovery_scope,
    load_verified_bar_state_json,
    publish_bar_state_json,
    publish_bar_state_parquet,
)
from systematic_fx.research.bar_state_config import (
    BAR_STATE_V2_PROFILE,
    BAR_STATE_V2A_PROFILE,
    BAR_STATE_V2B_PROFILE,
    BarStateCampaignProfile,
)
from systematic_fx.research.bar_state_features import FEATURE_NAMES_BY_SET
from systematic_fx.research.bar_state_model import (
    BAR_STATE_V2_MODEL_HYPERPARAMETERS,
    BAR_STATE_V2A_MODEL_HYPERPARAMETERS,
    STATE_MODEL_CLASSES,
    CanonicalBarStateModel,
)
from systematic_fx.research.bar_state_run import (
    BarStateRunProvenance,
    PreparedBarStateRun,
    _feature_arrow_schema,
    _publish_code_snapshot,
    _publish_registration,
    _validate_duplicate_consensus,
    build_bar_state_run_specs,
    load_prepared_bar_state_run,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.provenance import CodeSnapshot
from systematic_fx.validation.bar_state_splits import (
    BAR_STATE_FROZEN_BOOTSTRAP_EVALUATION_CALENDAR_SHA256,
    frozen_bar_state_bootstrap_evaluation_calendar,
)

ROOT = Path(__file__).resolve().parents[2]
DATABASE_ENV = "SYSTEMATIC_FX_BAR_STATE_GATE_DATABASE_URL"
V2A_DATABASE_ENV = "SYSTEMATIC_FX_BAR_STATE_V2A_GATE_DATABASE_URL"
V2B_DATABASE_ENV = "SYSTEMATIC_FX_BAR_STATE_V2B_GATE_DATABASE_URL"
EXPECTED_MIGRATION_0024_SHA256 = "4aa845757f1a220c8d5595d4db6053f6374d99d067ab7e20c3e40ea22d610010"
EXPECTED_MIGRATION_0025_SHA256 = "e08aa486bf9a65b2875e92866ae5e939fc56dc5d871010dfdb4b9085550749dd"
EXPECTED_MIGRATION_0026_SHA256 = "232badda3e76fca79f93fcff059de6f3404fc797eb26a93c9483fd554cfe20bb"
EXPECTED_MIGRATION_0027_SHA256 = "f0f69db031dc555b260da1fceef5f1fb4087f25717f1472ae4b006e77182cdb8"
SAFE_DATABASE_NAME = re.compile(r"systematic_fx_bar_state_gate_[0-9a-f]{12}")
SAFE_V2A_DATABASE_NAME = re.compile(r"systematic_fx_bar_state_v2a_gate_[0-9a-f]{12}")
SAFE_V2B_DATABASE_NAME = re.compile(r"systematic_fx_bar_state_v2b_gate_[0-9a-f]{12}")


@lru_cache(maxsize=1)
def _gate_bootstrap_evaluation_calendar() -> dict[str, object]:
    return frozen_bar_state_bootstrap_evaluation_calendar(
        load_prepared_bar_state_run(ROOT).split_plan
    )


@lru_cache(maxsize=1)
def _gate_bootstrap_daily_dates() -> tuple[str, ...]:
    calendar = _gate_bootstrap_evaluation_calendar()
    folds = calendar["folds"]
    assert isinstance(folds, list)
    counts = (14, 13, 13)
    result: list[str] = []
    for raw_fold, count in zip(folds, counts, strict=True):
        assert isinstance(raw_fold, dict)
        active_dates = raw_fold["active_dates"]
        assert isinstance(active_dates, list)
        result.extend(
            str(active_dates[index * len(active_dates) // count]) for index in range(count)
        )
    return tuple(result)


def _gate_model(
    model_id: str,
    timeframe_seconds: int,
    feature_set_id: str,
    *,
    profile: BarStateCampaignProfile = BAR_STATE_V2_PROFILE,
) -> CanonicalBarStateModel:
    names = FEATURE_NAMES_BY_SET[feature_set_id]
    width = len(names)
    return CanonicalBarStateModel(
        model_id=model_id,
        timeframe_seconds=timeframe_seconds,
        feature_set_id=feature_set_id,
        feature_names=names,
        classes=STATE_MODEL_CLASSES,
        scaler_mean=(0.0,) * width,
        scaler_scale=(1.0,) * width,
        coefficients=tuple((0.0,) * width for _ in STATE_MODEL_CLASSES),
        intercepts=(0.0,) * len(STATE_MODEL_CLASSES),
        training_row_count=3,
        training_class_counts=tuple((item, 1) for item in STATE_MODEL_CLASSES),
        training_rows_sha256="a" * 64,
        sklearn_version="gate",
        numpy_version="gate",
        python_version="gate",
        optimizer_iterations=(1,) * len(STATE_MODEL_CLASSES),
        hyperparameters=(
            BAR_STATE_V2A_MODEL_HYPERPARAMETERS
            if profile.version_id in {"V2A", "V2B"}
            else BAR_STATE_V2_MODEL_HYPERPARAMETERS
        ),
    )


def _disposable_database_url() -> str:
    database_url = os.environ.get(DATABASE_ENV)
    if not database_url:
        pytest.skip(f"{DATABASE_ENV} is not set")
    database_name = _url_database_name(database_url, label=DATABASE_ENV)
    if database_name is None or SAFE_DATABASE_NAME.fullmatch(database_name) is None:
        raise RuntimeError(
            f"{DATABASE_ENV} must target a disposable database named "
            "systematic_fx_bar_state_gate_<12 lowercase hex characters>"
        )
    if database_name in {"systematic_fx", "systematic_fx_test", "postgres"}:
        raise RuntimeError("bar-state release gate refuses persistent databases")
    return database_url


def _disposable_v2a_database_url() -> str:
    database_url = os.environ.get(V2A_DATABASE_ENV)
    if not database_url:
        pytest.skip(f"{V2A_DATABASE_ENV} is not set")
    database_name = _url_database_name(database_url, label=V2A_DATABASE_ENV)
    if database_name is None or SAFE_V2A_DATABASE_NAME.fullmatch(database_name) is None:
        raise RuntimeError(
            f"{V2A_DATABASE_ENV} must target a disposable database named "
            "systematic_fx_bar_state_v2a_gate_<12 lowercase hex characters>"
        )
    if database_name in {"systematic_fx", "systematic_fx_test", "postgres"}:
        raise RuntimeError("bar-state V2A release gate refuses persistent databases")
    return database_url


def _disposable_v2b_database_url() -> str:
    database_url = os.environ.get(V2B_DATABASE_ENV)
    if not database_url:
        pytest.skip(f"{V2B_DATABASE_ENV} is not set")
    database_name = _url_database_name(database_url, label=V2B_DATABASE_ENV)
    if database_name is None or SAFE_V2B_DATABASE_NAME.fullmatch(database_name) is None:
        raise RuntimeError(
            f"{V2B_DATABASE_ENV} must target a disposable database named "
            "systematic_fx_bar_state_v2b_gate_<12 lowercase hex characters>"
        )
    if database_name in {"systematic_fx", "systematic_fx_test", "postgres"}:
        raise RuntimeError("bar-state V2B release gate refuses persistent databases")
    return database_url


def _provenance(
    database_url: str,
    *,
    code_commit: str = "1" * 40,
) -> BarStateRunProvenance:
    snapshot_document = {
        "artifact_schema": "systematic_fx.code_snapshot.v2",
        "code_commit": code_commit,
        "file_count": 0,
        "files": [],
    }
    snapshot_bytes = canonical_json_bytes(snapshot_document)
    snapshot = CodeSnapshot(
        code_commit=code_commit,
        files=(),
        canonical_bytes=snapshot_bytes,
        sha256=hashlib.sha256(snapshot_bytes).hexdigest(),
    )
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        migrations = connection.execute(
            "SELECT version, name, checksum FROM systematic_fx.schema_migrations ORDER BY version"
        ).fetchall()
        server = connection.execute(
            "SELECT current_setting('server_version') AS version, "
            "current_setting('server_version_num') AS version_num"
        ).fetchone()
    migration_document = [dict(row) for row in migrations]
    runtime = {
        "artifact_schema": "systematic_fx.runtime_environment.v1",
        "bar_state_gate": {"purpose": "POSTGRESQL_GOVERNANCE_RELEASE_GATE"},
        "postgresql": {
            "schema_migrations": migration_document,
            "schema_migrations_sha256": canonical_sha256(migration_document),
            "server_version": str(server["version"]),
            "server_version_num": str(server["version_num"]),
        },
    }
    return BarStateRunProvenance(
        code_commit=code_commit,
        snapshot=snapshot,
        dependency_lock_sha256="2" * 64,
        runtime_environment=runtime,
        runtime_environment_sha256=canonical_sha256(runtime),
        postgres_migrations_sha256=canonical_sha256(migration_document),
    )


def _seed_dataset(
    database_url: str,
    artifact_root: Path,
    *,
    manifest_sha256: str,
) -> None:
    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            INSERT INTO systematic_fx.datasets
                (dataset_key, provider, feed, data_schema, root_uri, status,
                 manifest_sha256)
            VALUES (%s, 'gate', 'gate', 'trade-bars', %s, 'VALIDATING', %s)
            """,
            (
                BAR_STATE_DATASET_KEY,
                artifact_root.as_posix(),
                manifest_sha256,
            ),
        )


def _shared_lineage(
    prepared: object, provenance: BarStateRunProvenance, specs: tuple
) -> BarStateArtifactLineage:
    return BarStateArtifactLineage(
        config_file_sha256=prepared.config.sha256,
        config_semantic_sha256=prepared.config.semantic_sha256,
        candidate_catalog_sha256=prepared.config.candidate_catalog_sha256,
        training_plan_sha256=prepared.split_plan.sha256,
        code_snapshot_sha256=provenance.snapshot.sha256,
        dependency_lock_sha256=provenance.dependency_lock_sha256,
        runtime_environment_sha256=provenance.runtime_environment_sha256,
        ordered_run_set_sha256=canonical_sha256([item.fingerprint for item in specs]),
        discovery_scope=frozen_bar_state_discovery_scope(),
    )


def _candidate_lineage(
    shared: BarStateArtifactLineage,
    *,
    candidate_key: str,
    candidate_definition_sha256: str,
    run_fingerprint: str,
) -> BarStateArtifactLineage:
    return BarStateArtifactLineage(
        config_file_sha256=shared.config_file_sha256,
        config_semantic_sha256=shared.config_semantic_sha256,
        candidate_catalog_sha256=shared.candidate_catalog_sha256,
        training_plan_sha256=shared.training_plan_sha256,
        code_snapshot_sha256=shared.code_snapshot_sha256,
        dependency_lock_sha256=shared.dependency_lock_sha256,
        runtime_environment_sha256=shared.runtime_environment_sha256,
        ordered_run_set_sha256=shared.ordered_run_set_sha256,
        discovery_scope=shared.discovery_scope,
        candidate_key=candidate_key,
        candidate_definition_sha256=candidate_definition_sha256,
        run_fingerprint=run_fingerprint,
    )


def _insert_link_sql() -> str:
    return """
        INSERT INTO systematic_fx.bar_state_artifact_links
            (campaign_id, experiment_trial_id, research_run_spec_id,
             research_run_attempt_id, artifact_id, artifact_role,
             split_key, shard_ordinal, artifact_identity_sha256,
             content_sha256, lineage_sha256)
        SELECT run_spec.campaign_id, trial.experiment_trial_id,
               run_spec.research_run_spec_id, attempt.research_run_attempt_id,
               %s, %s, 'discovery', %s, %s, %s, %s
        FROM systematic_fx.research_run_attempts AS attempt
        JOIN systematic_fx.research_run_specs AS run_spec
          ON run_spec.research_run_spec_id = attempt.research_run_spec_id
        JOIN systematic_fx.experiment_trials AS trial
          ON trial.research_run_spec_id = run_spec.research_run_spec_id
        WHERE attempt.research_run_attempt_id = %s
    """


def _insert_extra_runspec_sql() -> str:
    return """
        INSERT INTO systematic_fx.research_run_specs
            (run_fingerprint, canonicalization_schema, canonicalization_version,
             campaign_id, experiment_id, parent_run_spec_id, run_kind,
             engine_version, canonical_spec, source_manifest_hashes,
             eligible_calendar_version, eligible_calendar_sha256,
             split_version, split_sha256, feature_version, feature_sha256,
             outcome_version, outcome_sha256, cost_version, cost_sha256,
             execution_version, execution_sha256, code_commit,
             dependency_lock_sha256, deterministic_seed, direction,
             code_snapshot_sha256)
        SELECT %s, canonicalization_schema, canonicalization_version,
               campaign_id, experiment_id, parent_run_spec_id, run_kind,
               engine_version, canonical_spec, source_manifest_hashes,
               eligible_calendar_version, eligible_calendar_sha256,
               split_version, split_sha256, feature_version, feature_sha256,
               outcome_version, outcome_sha256, cost_version, cost_sha256,
               execution_version, execution_sha256, code_commit,
               dependency_lock_sha256, deterministic_seed, direction,
               code_snapshot_sha256
        FROM systematic_fx.research_run_specs
        WHERE research_run_spec_id = %s
    """


def _link_parameters(
    artifact: object,
    *,
    artifact_id: int,
    artifact_role: str,
    shard_ordinal: int,
    attempt_id: int,
) -> tuple[object, ...]:
    lineage = artifact.descriptor.logical_identity["lineage"]
    return (
        artifact_id,
        artifact_role,
        shard_ordinal,
        artifact.descriptor.identity_sha256,
        artifact.sha256,
        canonical_sha256(lineage),
        attempt_id,
    )


def _register_artifact(database_url: str, artifact_root: Path, artifact: object) -> int:
    report = register_published_bar_artifact(database_url, artifact_root, artifact)
    return report.artifact_id


_GATE_EMPTY_CELL_REASONS = (
    "BASELINE_NET_EV",
    "POSITIVE_COMPONENT_SIZE",
    "POSITIVE_3X3_STABILITY",
    "NEIGHBOR_MEDIAN_EV",
    "MINIMUM_FILLED_ROUND_TRIPS",
    "MINIMUM_FILLS_PER_FOLD",
    "MINIMUM_POSITIVE_FOLDS",
    "MODERATE_NET_PNL",
    "MODERATE_CALENDAR_NET_PNL",
    "MODERATE_PROFIT_FACTOR",
    "MODERATE_WORST_FOLD_EV",
    "SEVERE_NET_EV",
    "FOLD_POSITIVE_GROSS_CONCENTRATION",
    "CONTRACT_POSITIVE_GROSS_CONCENTRATION",
)
_GATE_REJECTED_CANDIDATE_REASONS = tuple(sorted(_GATE_EMPTY_CELL_REASONS)) + ("BH_MULTIPLICITY",)


def _gate_candidate_selection(
    candidate_key: str,
    *,
    selected: bool,
    capped: bool = False,
) -> dict[str, object]:
    has_selected_cell = selected or capped
    selected_multiplier = {"denominator": 2, "numerator": 3} if has_selected_cell else None
    return {
        "bootstrap_lower_bound_ev_ticks": (
            {"denominator": 1, "numerator": 40} if has_selected_cell else None
        ),
        "candidate_key": candidate_key,
        "final_label": "FINALIST" if selected else "REJECTED",
        "maximum_drawdown_ticks": 8 if has_selected_cell else None,
        "moderate_ev_ticks": "40" if has_selected_cell else None,
        "positive_component_size": 9 if has_selected_cell else 0,
        "positive_inner_fold_count": 3 if has_selected_cell else 0,
        "rejection_reasons": (
            []
            if selected
            else ["MAXIMUM_FINALIST_LIMIT"]
            if capped
            else list(_GATE_REJECTED_CANDIDATE_REASONS)
        ),
        "selected_stop_loss_index": 3 if has_selected_cell else None,
        "selected_stop_loss_multiplier": selected_multiplier,
        "selected_take_profit_index": 3 if has_selected_cell else None,
        "selected_take_profit_multiplier": selected_multiplier,
        "worst_fold_moderate_ev_ticks": "40" if has_selected_cell else None,
    }


def _gate_candidate_dimensions(candidate_key: str) -> tuple[int, str]:
    matched = re.fullmatch(
        r"bsv2_tf(?P<timeframe>0300|1800)_fs(?P<feature_set>morphology|state)_"
        r"cm(?:005|010|015)",
        candidate_key,
    )
    if matched is None:  # pragma: no cover - caller uses the frozen catalog
        raise AssertionError("gate candidate key drifted")
    return int(matched.group("timeframe")), matched.group("feature_set").upper()


_GATE_MULTIPLIERS = (
    Fraction(1, 2),
    Fraction(3, 4),
    Fraction(1, 1),
    Fraction(3, 2),
    Fraction(2, 1),
    Fraction(3, 1),
    Fraction(4, 1),
)
_GATE_SCENARIOS = (
    ("BASELINE", 4, 4, "-8000"),
    ("MODERATE_COMBINED", 5, 5, "-10000"),
    ("SEVERE_DIAGNOSTIC", 6, 6, "-12000"),
)
_GATE_MONTHS = tuple(
    f"{year:04d}-{month:02d}"
    for year, month in (
        *((2022, month) for month in range(5, 13)),
        *((2023, month) for month in range(1, 9)),
    )
)


def _gate_fraction(value: Fraction) -> dict[str, int]:
    return {"denominator": value.denominator, "numerator": value.numerator}


def _gate_bh_adjust(
    raw_state_p: dict[tuple[str, int, int], Fraction],
) -> dict[tuple[str, int, int], tuple[Fraction, bool]]:
    family: list[tuple[Fraction, tuple[str, int, int] | None, str]] = [
        (Fraction(1), None, f"predecessor_{index:03d}") for index in range(216)
    ]
    family.extend(
        (value, key, f"state:{key[0]}:{key[1]}:{key[2]}")
        for key, value in sorted(raw_state_p.items())
    )
    ordered = sorted(family, key=lambda item: (item[0], item[2]))
    cutoff: Fraction | None = None
    for rank, (value, _key, _stable) in enumerate(ordered, start=1):
        if value <= Fraction(1, 20) * rank / 804:
            cutoff = value
    adjusted: dict[str, Fraction] = {}
    running = Fraction(1)
    for rank in range(804, 0, -1):
        value, _key, stable = ordered[rank - 1]
        running = min(running, value * 804 / rank)
        adjusted[stable] = min(Fraction(1), running)
    return {
        key: (
            adjusted[f"state:{key[0]}:{key[1]}:{key[2]}"],
            cutoff is not None and value <= cutoff,
        )
        for key, value in raw_state_p.items()
    }


def _gate_global_evidence(
    candidate_keys: tuple[str, ...],
    candidate_results: list[dict[str, object]],
    *,
    qc_variant: bool,
) -> dict[str, object]:
    bootstrap_evaluation_calendar = _gate_bootstrap_evaluation_calendar()
    bootstrap_daily_dates = _gate_bootstrap_daily_dates()
    selection_by_key = {str(item["candidate_key"]): item for item in candidate_results}
    eligible_candidate_keys = tuple(
        key
        for key in candidate_keys
        if selection_by_key[key]["final_label"] == "FINALIST"
        or selection_by_key[key]["rejection_reasons"] == ["MAXIMUM_FINALIST_LIMIT"]
    )
    positive_cell_keys: set[tuple[str, int, int]] = set()
    selected_cell_keys: set[tuple[str, int, int]] = set()
    for key in eligible_candidate_keys:
        selected_tp = int(selection_by_key[key]["selected_take_profit_index"])
        selected_sl = int(selection_by_key[key]["selected_stop_loss_index"])
        positive_tp_start = min(max(selected_tp - 2, 0), 2)
        positive_sl_start = min(max(selected_sl - 2, 0), 2)
        positive_cell_keys.update(
            (key, tp, sl)
            for tp in range(positive_tp_start, positive_tp_start + 5)
            for sl in range(positive_sl_start, positive_sl_start + 5)
        )
        selected_cell_keys.update(
            (key, tp, sl)
            for tp in range(selected_tp - 1, selected_tp + 2)
            for sl in range(selected_sl - 1, selected_sl + 2)
        )
    raw_p = {
        (key, tp, sl): (Fraction(1, 10_001) if (key, tp, sl) in selected_cell_keys else Fraction(1))
        for key in candidate_keys
        for tp in range(7)
        for sl in range(7)
    }
    adjusted = _gate_bh_adjust(raw_p)
    multiplicity = []
    for coordinate, raw_value in sorted(raw_p.items()):
        candidate_key, tp_index, sl_index = coordinate
        adjusted_value, rejected = adjusted[coordinate]
        eligible = coordinate in selected_cell_keys
        reasons = (
            []
            if eligible
            else ["POSITIVE_3X3_STABILITY"]
            if coordinate in positive_cell_keys
            else list(_GATE_EMPTY_CELL_REASONS)
        )
        multiplicity.append(
            {
                "adjusted_p_value": _gate_fraction(adjusted_value),
                "bh_rejected": rejected,
                "bootstrap_lower_bound_ev_ticks": (
                    _gate_fraction(Fraction(40)) if eligible else None
                ),
                "candidate_key": candidate_key,
                "deterministic_gate_passed": eligible,
                "raw_p_value": _gate_fraction(raw_value),
                "rejection_reasons": reasons,
                "stop_loss_index": sl_index,
                "take_profit_index": tp_index,
            }
        )

    cells: list[dict[str, object]] = []
    for candidate_key in candidate_keys:
        for (
            scenario_id,
            variable_cost_per_fill,
            fixed_cost_per_fill,
            calendar_net,
        ) in _GATE_SCENARIOS:
            for tp_index, tp_multiplier in enumerate(_GATE_MULTIPLIERS):
                for sl_index, sl_multiplier in enumerate(_GATE_MULTIPLIERS):
                    positive_cell = (candidate_key, tp_index, sl_index) in positive_cell_keys
                    entry_fill_count = 40 if positive_cell else 0
                    skipped_count = 0 if positive_cell else 40
                    scenario_ev = {
                        "BASELINE": 1,
                        "MODERATE_COMBINED": 40,
                        "SEVERE_DIAGNOSTIC": 0,
                    }[scenario_id]
                    net = entry_fill_count * scenario_ev
                    variable_cost = entry_fill_count * variable_cost_per_fill
                    allocated_cost = entry_fill_count * fixed_cost_per_fill
                    gross = net + variable_cost + allocated_cost
                    actual_calendar_net = format(
                        Decimal(calendar_net) + Decimal(gross - variable_cost) * Decimal("6.25"),
                        "f",
                    )
                    block_fill_counts = (14, 13, 13) if positive_cell else (0, 0, 0)
                    block_positive_base, block_positive_remainder = divmod(gross, 3)
                    cells.append(
                        {
                            "allocated_fixed_cost_ticks": allocated_cost,
                            "blocks": [
                                {
                                    "block_key": f"discovery_inner_{fold}",
                                    "entry_fill_count": block_fill_counts[fold - 1],
                                    "fully_loaded_net_ev_ticks": (
                                        str(scenario_ev) if positive_cell else None
                                    ),
                                    "fully_loaded_net_pnl_ticks": (
                                        block_fill_counts[fold - 1] * scenario_ev
                                    ),
                                    "maximum_drawdown_ticks": 0,
                                    "positive_gross_ticks": (
                                        block_positive_base
                                        + (1 if fold <= block_positive_remainder else 0)
                                        if positive_cell
                                        else 0
                                    ),
                                }
                                for fold in (1, 2, 3)
                            ],
                            "calendar_month_net_pnl_usd": actual_calendar_net,
                            "candidate_key": candidate_key,
                            "cell_id": (
                                f"tpm{tp_multiplier.numerator}_{tp_multiplier.denominator}"
                                f"_slm{sl_multiplier.numerator}_{sl_multiplier.denominator}"
                            ),
                            "distinct_stop_loss_distance_count": 1,
                            "distinct_take_profit_distance_count": 1,
                            "daily_net_pnl_ticks": (
                                [
                                    {
                                        "active_date": active_date,
                                        "net_pnl_ticks": scenario_ev,
                                    }
                                    for active_date in bootstrap_daily_dates
                                ]
                                if positive_cell
                                else []
                            ),
                            "daily_fill_count": (
                                [
                                    {
                                        "active_date": active_date,
                                        "fill_count": 1,
                                    }
                                    for active_date in bootstrap_daily_dates
                                ]
                                if positive_cell
                                else []
                            ),
                            "entry_fill_count": entry_fill_count,
                            "entry_not_filled_count": 120,
                            "fully_loaded_net_ev_ticks": (
                                str(scenario_ev) if positive_cell else None
                            ),
                            "fully_loaded_net_pnl_ticks": net,
                            "gross_pnl_ticks": gross,
                            "maximum_drawdown_ticks": (
                                int(selection_by_key[candidate_key]["maximum_drawdown_ticks"])
                                if positive_cell
                                else 0
                            ),
                            "no_trade_count": 0,
                            "profit_factor": "2" if positive_cell else None,
                            "positive_gross_by_contract": (
                                [
                                    {"contract": "ES", "positive_gross_ticks": gross // 2},
                                    {"contract": "NQ", "positive_gross_ticks": gross // 2},
                                ]
                                if positive_cell
                                else []
                            ),
                            "same_second_stop_first_count": 0,
                            "scenario_id": scenario_id,
                            "signal_count": 160,
                            "skipped_occupied_count": skipped_count,
                            "stop_first_count": 0,
                            "stop_loss_multiplier": _gate_fraction(sl_multiplier),
                            "take_profit_first_count": entry_fill_count,
                            "take_profit_multiplier": _gate_fraction(tp_multiplier),
                            "terminal_exit_count": 0,
                            "variable_cost_ticks": variable_cost,
                        }
                    )

    candidate_support = []
    axes = []
    decision_counts: dict[str, object] = {}
    for candidate_key in candidate_keys:
        timeframe, _feature_set = _gate_candidate_dimensions(candidate_key)
        candidate_support.append(
            {
                "candidate_key": candidate_key,
                "distinct_signal_day_count": 40,
                "raw_directional_signal_count": 160,
                "raw_signal_count_by_fold": [
                    {
                        "fold_key": f"discovery_inner_{fold}",
                        "signal_count": (54, 53, 53)[fold - 1],
                    }
                    for fold in (1, 2, 3)
                ],
                "timeframe_seconds": timeframe,
            }
        )
        axes.append(
            {
                "axis_vector_sha256": [
                    "0" * 64,
                    "1" * 64,
                    "2" * 64,
                    "3" * 64,
                    "0" * 64,
                    "1" * 64,
                    "2" * 64,
                ],
                "candidate_key": candidate_key,
                "filled_directional_signal_count": 40,
                "per_signal_distinct_count_histogram": [{"distinct_count": 4, "signal_count": 40}],
                "unique_axis_vector_count": 4,
            }
        )
        decision_counts[candidate_key] = {"LONG": 160, "NO_TRADE": 0, "SHORT": 0}

    feature_qc = [
        {
            "exclusion_counts_by_reason": (
                {"ZERO_ATR": 1}
                if qc_variant and timeframe == 300 and feature_set_id == "MORPHOLOGY"
                else {}
            ),
            "feature_set_id": feature_set_id,
            "timeframe_seconds": timeframe,
        }
        for timeframe, feature_set_id in (
            (300, "MORPHOLOGY"),
            (300, "STATE"),
            (1800, "MORPHOLOGY"),
            (1800, "STATE"),
        )
    ]
    return {
        "axis_resolutions": axes,
        "bh_family_size": 804,
        "bootstrap_convention": (
            "FOLD_LOCAL_STATIONARY_PCG64_ALIGNED_EXIT_DAY_NET_AND_FILL_COUNTS"
        ),
        "bootstrap_evaluation_calendar": bootstrap_evaluation_calendar,
        "bootstrap_evaluation_calendar_sha256": (
            BAR_STATE_FROZEN_BOOTSTRAP_EVALUATION_CALENDAR_SHA256
        ),
        "candidate_results": candidate_results,
        "candidate_signal_decision_counts": decision_counts,
        "candidate_support": candidate_support,
        "cell_summaries": cells,
        "feature_exclusion_qc": feature_qc,
        "finalist_keys": [
            key for key in candidate_keys if selection_by_key[key]["final_label"] == "FINALIST"
        ],
        "memory_plan": {
            "accumulator_count": 1764,
            "candidate_count": 12,
            "grid_cell_count": 49,
            "input_signal_count": 1_920,
            "maximum_input_signal_count": 1_000_000,
            "maximum_resident_one_second_rows": 1_481_453,
            "one_second_row_count": 7_573_041,
            "outcome_span_count": 10,
            "resident_outcome_span_limit": 1,
            "retained_trade_record_count": 0,
            "scenario_count": 3,
        },
        "multiplicity_results": multiplicity,
        "observed_utc_months": list(_GATE_MONTHS),
        "portfolio_executed_trade_record_count": 3 * 40 * len(positive_cell_keys),
        "portfolio_signal_count": 1_920,
        "schema": "systematic_fx.bar_state_selection.v1",
        "signal_count": 1_920,
    }


def _gate_model_package_document(
    candidate_key: str,
    *,
    selected: bool,
    profile: BarStateCampaignProfile = BAR_STATE_V2_PROFILE,
) -> tuple[dict[str, object], dict[str, object] | None]:
    timeframe, feature_set_id = _gate_candidate_dimensions(candidate_key)
    models: list[dict[str, object]] = []
    for fold in (1, 2, 3):
        inner_model = _gate_model(
            f"bsv2_tf{timeframe:04d}_fs{feature_set_id.lower()}_discovery_inner_{fold}",
            timeframe,
            feature_set_id,
            profile=profile,
        )
        models.append(
            {
                "fold_key": f"discovery_inner_{fold}",
                "model": inner_model.as_dict(),
                "model_sha256": inner_model.sha256,
                "schema": "systematic_fx.bar_state_fold_model.v1",
            }
        )
    binding: dict[str, object] | None = None
    if selected:
        final_model = _gate_model(
            f"bsv2_tf{timeframe:04d}_fs{feature_set_id.lower()}_discovery_final_fit",
            timeframe,
            feature_set_id,
            profile=profile,
        )
        binding = {
            "candidate_key": candidate_key,
            "feature_set_id": feature_set_id,
            "model_sha256": final_model.sha256,
            "timeframe_seconds": timeframe,
        }
        models.append(
            {
                "fit_key": "discovery_final_fit",
                "label_maturity_end_active_ordinal": 489,
                "model": final_model.as_dict(),
                "model_sha256": final_model.sha256,
                "schema": "systematic_fx.bar_state_final_fit_model.v1",
                "training_decision_end_active_ordinal": 469,
            }
        )
    return (
        {
            "candidate_key": candidate_key,
            "fold_model_count": len(models),
            "fold_models": models,
            "schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["MODEL"],
        },
        binding,
    )


def _gate_full_global_document(
    candidate_keys: tuple[str, ...],
    *,
    qc_variant: bool = False,
    profile: BarStateCampaignProfile = BAR_STATE_V2_PROFILE,
) -> dict[str, object]:
    finalist_keys = candidate_keys[:4]
    capped_key = candidate_keys[4]
    candidate_results = [
        _gate_candidate_selection(
            key,
            selected=key in finalist_keys,
            capped=key == capped_key,
        )
        for key in candidate_keys
    ]
    final_models_by_group: dict[tuple[int, str], CanonicalBarStateModel] = {}
    bindings: list[dict[str, object]] = []
    for finalist_key in finalist_keys:
        timeframe, feature_set_id = _gate_candidate_dimensions(finalist_key)
        final_model = final_models_by_group.setdefault(
            (timeframe, feature_set_id),
            _gate_model(
                f"bsv2_tf{timeframe:04d}_fs{feature_set_id.lower()}_discovery_final_fit",
                timeframe,
                feature_set_id,
                profile=profile,
            ),
        )
        bindings.append(
            {
                "candidate_key": finalist_key,
                "feature_set_id": feature_set_id,
                "model_sha256": final_model.sha256,
                "timeframe_seconds": timeframe,
            }
        )
    discovery = _gate_global_evidence(
        candidate_keys,
        candidate_results,
        qc_variant=qc_variant,
    )
    discovery.update(
        {
            "discovery_final_fit_models": [
                {
                    "feature_set_id": feature_set_id,
                    "fit_key": "discovery_final_fit",
                    "label_maturity_end_active_ordinal": 489,
                    "model": model.as_dict(),
                    "model_sha256": model.sha256,
                    "schema": "systematic_fx.bar_state_final_fit_model.v1",
                    "timeframe_seconds": timeframe,
                    "training_decision_end_active_ordinal": 469,
                }
                for (timeframe, feature_set_id), model in sorted(final_models_by_group.items())
            ],
            "discovery_finalist_model_bindings": bindings,
        }
    )
    return {
        "candidate_count": 12,
        "discovery_result": discovery,
        "schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["GLOBAL_RESULT"],
    }


def _publish_shared_evidence(
    artifact_root: Path,
    lineage: BarStateArtifactLineage,
    *,
    candidate_keys: tuple[str, ...],
    profile: BarStateCampaignProfile = BAR_STATE_V2_PROFILE,
) -> tuple[dict[str, list[object]], object, object]:
    table = pa.table({"gate_value": pa.array([1], type=pa.int64())})
    feature_schema = _feature_arrow_schema(profile=profile)
    feature_table = (
        pa.Table.from_arrays(
            [pa.array([], type=field.type) for field in feature_schema],
            schema=feature_schema,
        )
        if profile.version_id == "V2B"
        else table
    )
    shared: dict[str, list[object]] = {"FEATURE": [], "LABEL": []}
    for role in ("FEATURE", "LABEL"):
        for shard in range(4):
            shared[role].append(
                publish_bar_state_parquet(
                    artifact_root,
                    kind=role,
                    artifact_key_suffix=f"gate-{role.lower()}-{shard}",
                    table=feature_table if role == "FEATURE" else table,
                    lineage=lineage,
                    logical_identity={
                        "shard_ordinal": shard,
                        "split_key": "discovery",
                    },
                    profile=profile,
                )
            )
    finalist_keys = candidate_keys[:4]
    document_a = _gate_full_global_document(candidate_keys, profile=profile)
    document_b = _gate_full_global_document(
        candidate_keys,
        qc_variant=True,
        profile=profile,
    )

    def global_logical(document: dict[str, object]) -> dict[str, object]:
        projection = bar_state_global_result_projection(document, profile=profile)
        model_package_hashes = {}
        for key in candidate_keys:
            package_document, package_binding = _gate_model_package_document(
                key,
                selected=key in finalist_keys,
                profile=profile,
            )
            model_package_hashes[key] = bar_state_model_package_projection(
                package_document,
                expected_candidate_key=key,
                expected_binding=package_binding,
                profile=profile,
            ).sha256
        return {
            "candidate_evidence_slice_sha256_by_key": dict(
                projection.candidate_evidence_slice_sha256_by_key
            ),
            "candidate_oos_trade_record_count_by_key": dict(
                projection.candidate_oos_trade_record_count_by_key
            ),
            "candidate_selection_sha256_by_key": dict(projection.candidate_selection_sha256_by_key),
            "candidate_selection_projection_sha256_by_key": dict(
                projection.candidate_selection_projection_sha256_by_key
            ),
            "finalist_model_binding_by_key": {
                key: (
                    None
                    if (candidate_binding := projection.finalist_bindings.get(key)) is None
                    else dict(candidate_binding)
                )
                for key in projection.candidate_selections
            },
            "finalist_model_binding_sha256_by_key": dict(
                projection.finalist_model_binding_sha256_by_key
            ),
            "global_evidence_projection_sha256": projection.evidence_projection_sha256,
            "model_package_projection_sha256_by_key": model_package_hashes,
            "split_key": "discovery",
        }

    global_a = publish_bar_state_json(
        artifact_root,
        kind="GLOBAL_RESULT",
        artifact_key_suffix="gate-global-a",
        document=document_a,
        record_count=12,
        lineage=lineage,
        logical_identity={**global_logical(document_a), "variant": "a"},
        profile=profile,
    )
    global_b = publish_bar_state_json(
        artifact_root,
        kind="GLOBAL_RESULT",
        artifact_key_suffix="gate-global-b",
        document=document_b,
        record_count=12,
        lineage=lineage,
        logical_identity={**global_logical(document_b), "variant": "b"},
        profile=profile,
    )
    return shared, global_a, global_b


def _publish_candidate_evidence(
    artifact_root: Path,
    *,
    candidate_key: str,
    global_document: dict[str, object],
    lineage: BarStateArtifactLineage,
    trial_status: str,
    capped: bool = False,
    profile: BarStateCampaignProfile = BAR_STATE_V2_PROFILE,
) -> dict[str, object]:
    decision_label = "DISCOVERY_FINALIST" if trial_status == "SUCCEEDED" else "DISCOVERY_REJECT"
    selected = trial_status == "SUCCEEDED"
    global_projection = bar_state_global_result_projection(global_document, profile=profile)
    selection = dict(global_projection.candidate_selections[candidate_key])
    assert (selection["final_label"] == "FINALIST") == selected
    assert (selection["rejection_reasons"] == ["MAXIMUM_FINALIST_LIMIT"]) == capped
    candidate_evidence_slice = dict(
        global_projection.candidate_evidence_slice_by_key[candidate_key]
    )
    candidate_evidence_slice_sha256 = canonical_sha256(candidate_evidence_slice)
    expected_oos_trade_record_count = global_projection.candidate_oos_trade_record_count_by_key[
        candidate_key
    ]
    price_policy = bar_state_price_policy_from_selection(selection)
    timeframe, feature_set_id = _gate_candidate_dimensions(candidate_key)
    final_model = _gate_model(
        f"bsv2_tf{timeframe:04d}_fs{feature_set_id.lower()}_discovery_final_fit",
        timeframe,
        feature_set_id,
        profile=profile,
    )
    binding = (
        {
            "candidate_key": candidate_key,
            "feature_set_id": feature_set_id,
            "model_sha256": final_model.sha256,
            "timeframe_seconds": timeframe,
        }
        if selected
        else None
    )
    compact_summary = {
        "candidate_key": candidate_key,
        "discovery_final_fit_model_sha256": final_model.sha256 if selected else None,
        "final_label": selection["final_label"],
        "positive_component_size": selection["positive_component_size"],
        "price_policy": price_policy,
        "rejection_reasons": selection["rejection_reasons"],
        "selected_stop_loss_index": selection["selected_stop_loss_index"],
        "selected_take_profit_index": selection["selected_take_profit_index"],
    }
    table = pa.table(
        {
            "gate_value": pa.array(
                range(expected_oos_trade_record_count),
                type=pa.int64(),
            )
        }
    )
    model_document, package_binding = _gate_model_package_document(
        candidate_key,
        selected=selected,
        profile=profile,
    )
    assert package_binding == binding
    package_projection = bar_state_model_package_projection(
        model_document,
        expected_candidate_key=candidate_key,
        expected_binding=binding,
        profile=profile,
    )
    model = publish_bar_state_json(
        artifact_root,
        kind="MODEL",
        artifact_key_suffix=f"gate-model-{candidate_key}",
        document=model_document,
        record_count=package_projection.record_count,
        lineage=lineage,
        logical_identity={
            "candidate_key": candidate_key,
            "candidate_selection_sha256": canonical_sha256(selection),
            "candidate_selection_projection_sha256": canonical_sha256(
                bar_state_candidate_selection_projection(selection)
            ),
            "finalist_model_binding": binding,
            "finalist_model_binding_sha256": canonical_sha256(binding),
            "global_evidence_projection_sha256": (global_projection.evidence_projection_sha256),
            "model_package_projection": dict(package_projection.projection),
            "model_package_projection_sha256": package_projection.sha256,
            "split_key": "discovery",
        },
        profile=profile,
    )
    oos = publish_bar_state_parquet(
        artifact_root,
        kind="OOS_TRADE",
        artifact_key_suffix=f"gate-oos-{candidate_key}",
        table=table,
        lineage=lineage,
        logical_identity={
            "candidate_key": candidate_key,
            "row_count": expected_oos_trade_record_count,
            "split_key": "discovery",
        },
        profile=profile,
    )
    terminal = publish_bar_state_json(
        artifact_root,
        kind="TERMINAL_RESULT",
        artifact_key_suffix=f"gate-terminal-{candidate_key}",
        document={
            "candidate_key": candidate_key,
            "compact_summary": compact_summary,
            "decision_label": decision_label,
            "result": {
                "candidate_selection": selection,
                "candidate_support": candidate_evidence_slice["candidate_support"],
                "discovery_final_fit_model": binding,
                "multiplicity_cells": candidate_evidence_slice["multiplicity_cells"],
                "price_policy": price_policy,
                "schema": "systematic_fx.bar_state_candidate_result.v1",
            },
            "schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["TERMINAL_RESULT"],
            "trial_status": trial_status,
        },
        record_count=1,
        lineage=lineage,
        logical_identity={
            "candidate_key": candidate_key,
            "candidate_selection_sha256": canonical_sha256(selection),
            "candidate_evidence_slice_sha256": candidate_evidence_slice_sha256,
            "candidate_selection_projection_sha256": canonical_sha256(
                bar_state_candidate_selection_projection(selection)
            ),
            "compact_summary_sha256": canonical_sha256(compact_summary),
            "decision_label": decision_label,
            "finalist_model_binding": binding,
            "finalist_model_binding_sha256": canonical_sha256(binding),
            "global_evidence_projection_sha256": (global_projection.evidence_projection_sha256),
            "model_package_projection_sha256": package_projection.sha256,
            "split_key": "discovery",
            "trial_status": trial_status,
        },
        profile=profile,
    )
    return {
        "COMPACT_SUMMARY": compact_summary,
        "MODEL": model,
        "OOS_TRADE": oos,
        "TERMINAL_RESULT": terminal,
    }


def _register_gate_campaign(
    database_url: str,
    artifact_root: Path,
    *,
    profile: BarStateCampaignProfile,
    code_commit: str,
) -> tuple[PreparedBarStateRun, BarStateRunProvenance, tuple, dict[str, object]]:
    prepared = load_prepared_bar_state_run(ROOT, profile=profile)
    provenance = _provenance(database_url, code_commit=code_commit)
    specs = build_bar_state_run_specs(prepared, provenance)
    artifact_prepared = replace(
        prepared,
        project_root=artifact_root,
        data_root=artifact_root / "data",
    )
    code_artifact = _publish_code_snapshot(artifact_prepared, provenance, specs)
    registration_artifact, registration_document = _publish_registration(
        artifact_prepared,
        provenance,
        specs,
        code_artifact,
    )
    report = register_bar_state_campaign(
        database_url,
        artifact_root,
        definition=prepared.registry_definition,
        split_plan=prepared.outer_split_plan,
        code_commit=provenance.code_commit,
        registration_artifact=registration_artifact,
        expected_registration_document=registration_document,
    )
    assert report.created_trials in {0, 12}
    register_published_bar_artifact(database_url, artifact_root, code_artifact)
    registrations = {
        candidate_key: register_bar_state_run_spec(
            database_url,
            spec,
            definition=prepared.registry_definition,
            split_plan=prepared.outer_split_plan,
            candidate_key=candidate_key,
        )
        for candidate_key, spec in zip(prepared.candidate_keys, specs, strict=True)
    }
    return prepared, provenance, specs, registrations


def _complete_gate_campaign_lifecycle(
    database_url: str,
    artifact_root: Path,
    *,
    prepared: PreparedBarStateRun,
    provenance: BarStateRunProvenance,
    specs: tuple,
    profile: BarStateCampaignProfile,
    pre_link_probe: Callable[[Mapping[str, RunAttemptReservation]], None] | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    reservations = {
        candidate_key: reserve_run_attempt(database_url, run_fingerprint=spec.fingerprint)
        for candidate_key, spec in zip(prepared.candidate_keys, specs, strict=True)
    }
    for reservation in reservations.values():
        start_run_attempt(
            database_url,
            research_run_attempt_id=reservation.research_run_attempt_id,
        )
    if pre_link_probe is not None:
        pre_link_probe(reservations)
    shared_lineage = _shared_lineage(prepared, provenance, specs)
    shared, global_result, _ = _publish_shared_evidence(
        artifact_root,
        shared_lineage,
        candidate_keys=prepared.candidate_keys,
        profile=profile,
    )
    global_document = load_verified_bar_state_json(
        artifact_root,
        global_result,
        profile=profile,
    )
    finalist_keys = prepared.candidate_keys[:4]
    capped_key = prepared.candidate_keys[4]
    candidate_evidence: dict[str, dict[str, object]] = {}
    for candidate_key, candidate, spec in zip(
        prepared.candidate_keys,
        prepared.config.candidates,
        specs,
        strict=True,
    ):
        lineage = _candidate_lineage(
            shared_lineage,
            candidate_key=candidate_key,
            candidate_definition_sha256=candidate.definition_sha256,
            run_fingerprint=spec.fingerprint,
        )
        candidate_evidence[candidate_key] = _publish_candidate_evidence(
            artifact_root,
            candidate_key=candidate_key,
            global_document=global_document,
            lineage=lineage,
            trial_status=("SUCCEEDED" if candidate_key in finalist_keys else "REJECTED"),
            capped=candidate_key == capped_key,
            profile=profile,
        )
    for candidate_key in prepared.candidate_keys:
        attempt_id = reservations[candidate_key].research_run_attempt_id
        for role in ("FEATURE", "LABEL"):
            for shard_ordinal, artifact in enumerate(shared[role]):
                register_bar_state_artifact_link(
                    database_url,
                    artifact_root,
                    research_run_attempt_id=attempt_id,
                    candidate_key=candidate_key,
                    artifact_role=role,
                    split_key="discovery",
                    shard_ordinal=shard_ordinal,
                    artifact=artifact,
                    profile=profile,
                )
        for role in ("MODEL", "OOS_TRADE"):
            register_bar_state_artifact_link(
                database_url,
                artifact_root,
                research_run_attempt_id=attempt_id,
                candidate_key=candidate_key,
                artifact_role=role,
                split_key="discovery",
                shard_ordinal=0,
                artifact=candidate_evidence[candidate_key][role],
                profile=profile,
            )
        for role, artifact in (
            ("GLOBAL_RESULT", global_result),
            ("TERMINAL_RESULT", candidate_evidence[candidate_key]["TERMINAL_RESULT"]),
        ):
            register_bar_state_artifact_link(
                database_url,
                artifact_root,
                research_run_attempt_id=attempt_id,
                candidate_key=candidate_key,
                artifact_role=role,
                split_key="discovery",
                shard_ordinal=0,
                artifact=artifact,
                profile=profile,
            )
    for candidate_key in prepared.candidate_keys:
        trial_status = "SUCCEEDED" if candidate_key in finalist_keys else "REJECTED"
        report = register_terminal_bar_state_result(
            database_url,
            artifact_root,
            research_run_attempt_id=reservations[candidate_key].research_run_attempt_id,
            candidate_key=candidate_key,
            trial_status=trial_status,
            decision_label=(
                "DISCOVERY_FINALIST" if trial_status == "SUCCEEDED" else "DISCOVERY_REJECT"
            ),
            compact_summary=candidate_evidence[candidate_key]["COMPACT_SUMMARY"],
            profile=profile,
        )
        assert report.trial_status == trial_status
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        terminal_counts = connection.execute(
            """
            SELECT
                count(*) FILTER (WHERE attempt.status = 'SUCCEEDED') AS attempts,
                count(*) FILTER (WHERE trial.status = 'SUCCEEDED') AS finalists,
                count(*) FILTER (WHERE trial.status = 'REJECTED') AS rejects,
                count(*) FILTER (
                    WHERE attempt.status = 'SUCCEEDED'
                      AND trial.status IN ('SUCCEEDED', 'REJECTED')
                      AND attempt.result_summary = trial.result_summary
                ) AS exact_pairs
            FROM systematic_fx.research_run_attempts AS attempt
            JOIN systematic_fx.research_run_specs AS run_spec
              ON run_spec.research_run_spec_id = attempt.research_run_spec_id
            JOIN systematic_fx.campaigns AS campaign
              ON campaign.campaign_id = run_spec.campaign_id
            JOIN systematic_fx.experiment_trials AS trial
              ON trial.research_run_spec_id = run_spec.research_run_spec_id
            WHERE campaign.campaign_key = %s
            """,
            (profile.campaign_key,),
        ).fetchone()
        link_counts = connection.execute(
            """
            SELECT count(*) AS links,
                   count(DISTINCT artifact_identity_sha256)
                       FILTER (WHERE artifact_role = 'GLOBAL_RESULT')
                       AS global_identities
            FROM systematic_fx.bar_state_artifact_links AS link
            JOIN systematic_fx.campaigns AS campaign
              ON campaign.campaign_id = link.campaign_id
            WHERE campaign.campaign_key = %s
            """,
            (profile.campaign_key,),
        ).fetchone()
    return dict(terminal_counts), dict(link_counts)


def test_bar_state_v2_postgresql_release_gate(tmp_path: Path) -> None:
    database_url = _disposable_database_url()
    migrations = discover_migrations(ROOT / "migrations")
    assert tuple(item.version for item in migrations) == tuple(range(1, 28))
    migration_by_version = {item.version: item for item in migrations}
    assert migration_by_version[24].checksum == EXPECTED_MIGRATION_0024_SHA256
    assert migration_by_version[25].checksum == EXPECTED_MIGRATION_0025_SHA256
    assert migration_by_version[26].checksum == EXPECTED_MIGRATION_0026_SHA256
    assert migration_by_version[27].checksum == EXPECTED_MIGRATION_0027_SHA256

    first = apply_migrations(
        database_url,
        directory=ROOT / "migrations",
        psql_binary=os.environ.get("SYSTEMATIC_FX_PSQL"),
    )
    assert first.applied == tuple(range(1, 28))
    assert first.skipped == ()
    repeated = apply_migrations(
        database_url,
        directory=ROOT / "migrations",
        psql_binary=os.environ.get("SYSTEMATIC_FX_PSQL"),
    )
    assert repeated.applied == ()
    assert repeated.skipped == tuple(range(1, 28))

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        latest = connection.execute(
            "SELECT version, name, checksum FROM systematic_fx.schema_migrations "
            "ORDER BY version DESC LIMIT 1"
        ).fetchone()
        trigger_count = connection.execute(
            """
            SELECT count(*) AS count
            FROM pg_catalog.pg_trigger AS trigger
            JOIN pg_catalog.pg_class AS relation ON relation.oid = trigger.tgrelid
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'systematic_fx'
              AND relation.relname = 'bar_state_artifact_links'
              AND trigger.tgname = 'bar_state_artifact_links_publication_refresh'
              AND NOT trigger.tgisinternal
            """
        ).fetchone()["count"]
        canonical_index_guard = connection.execute(
            """
            SELECT
                systematic_fx.bar_state_economic_multiplier('1'::jsonb) IS NOT NULL
                    AS canonical_integer,
                systematic_fx.bar_state_economic_multiplier('1.0'::jsonb) IS NULL
                    AS decimal_rejected,
                systematic_fx.bar_state_economic_multiplier('"1"'::jsonb) IS NULL
                    AS string_rejected
            """
        ).fetchone()
    assert latest == {
        "version": 27,
        "name": "bar_state_v2b_parquet_schema_amendment",
        "checksum": EXPECTED_MIGRATION_0027_SHA256,
    }
    assert trigger_count == 1
    assert canonical_index_guard == {
        "canonical_integer": True,
        "decimal_rejected": True,
        "string_rejected": True,
    }

    prepared = load_prepared_bar_state_run(ROOT)
    provenance = _provenance(database_url)
    specs = build_bar_state_run_specs(prepared, provenance)
    artifact_prepared = replace(
        prepared,
        project_root=tmp_path,
        data_root=tmp_path / "data",
    )
    code_artifact = _publish_code_snapshot(artifact_prepared, provenance, specs)
    registration_artifact, registration_document = _publish_registration(
        artifact_prepared,
        provenance,
        specs,
        code_artifact,
    )
    _seed_dataset(
        database_url,
        tmp_path,
        manifest_sha256=BAR_STATE_BAR_DATASET_MANIFEST_SHA256,
    )
    with pytest.raises(
        BarStateRegistryDriftError,
        match="dataset glbx_mdp3_mbp_10_6e_fut_v1 field 'manifest_sha256' drifted",
    ):
        register_bar_state_campaign(
            database_url,
            tmp_path,
            definition=prepared.registry_definition,
            split_plan=prepared.outer_split_plan,
            code_commit=provenance.code_commit,
            registration_artifact=registration_artifact,
            expected_registration_document=registration_document,
        )
    with psycopg.connect(database_url) as connection:
        connection.execute(
            "UPDATE systematic_fx.datasets SET manifest_sha256 = %s WHERE dataset_key = %s",
            (BAR_STATE_RAW_SOURCE_MANIFEST_SHA256, BAR_STATE_DATASET_KEY),
        )
    campaign = register_bar_state_campaign(
        database_url,
        tmp_path,
        definition=prepared.registry_definition,
        split_plan=prepared.outer_split_plan,
        code_commit=provenance.code_commit,
        registration_artifact=registration_artifact,
        expected_registration_document=registration_document,
    )
    assert campaign.created_trials == 12
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        lineage = connection.execute(
            """
            SELECT dataset.manifest_sha256 AS raw_source_manifest_sha256,
                   campaign.data_manifest_sha256 AS bar_dataset_manifest_sha256
            FROM systematic_fx.campaigns AS campaign
            JOIN systematic_fx.datasets AS dataset
              ON dataset.dataset_id = campaign.dataset_id
            WHERE campaign.campaign_key = %s
            """,
            ("bar_state_conditional_v2",),
        ).fetchone()
    assert lineage == {
        "raw_source_manifest_sha256": BAR_STATE_RAW_SOURCE_MANIFEST_SHA256,
        "bar_dataset_manifest_sha256": BAR_STATE_BAR_DATASET_MANIFEST_SHA256,
    }
    register_published_bar_artifact(database_url, tmp_path, code_artifact)

    registrations = {}
    reservations = {}
    for candidate_key, spec in zip(prepared.candidate_keys, specs, strict=True):
        registrations[candidate_key] = register_bar_state_run_spec(
            database_url,
            spec,
            definition=prepared.registry_definition,
            split_plan=prepared.outer_split_plan,
            candidate_key=candidate_key,
        )
    for candidate_key, spec in zip(prepared.candidate_keys, specs, strict=True):
        reservation = reserve_run_attempt(database_url, run_fingerprint=spec.fingerprint)
        assert reservation.execute and reservation.status == "QUEUED"
        state = start_run_attempt(
            database_url,
            research_run_attempt_id=reservation.research_run_attempt_id,
        )
        assert state.status == "RUNNING"
        reservations[candidate_key] = reservation

    with psycopg.connect(database_url) as connection:
        counts = connection.execute(
            """
            SELECT
                (SELECT count(*) FROM systematic_fx.experiment_trials) AS trials,
                (SELECT count(*) FROM systematic_fx.research_run_specs) AS specs,
                (SELECT count(*) FROM systematic_fx.research_run_attempts
                 WHERE status = 'RUNNING') AS running
            """
        ).fetchone()
    assert counts == (12, 12, 12)

    shared_lineage = _shared_lineage(prepared, provenance, specs)
    first_key, second_key = prepared.candidate_keys[:2]
    finalist_keys = prepared.candidate_keys[:4]
    capped_key = prepared.candidate_keys[4]
    shared, global_a, global_b = _publish_shared_evidence(
        tmp_path,
        shared_lineage,
        candidate_keys=prepared.candidate_keys,
    )
    global_a_id = _register_artifact(database_url, tmp_path, global_a)
    global_b_id = _register_artifact(database_url, tmp_path, global_b)
    first_attempt = reservations[first_key].research_run_attempt_id
    second_attempt = reservations[second_key].research_run_attempt_id

    valid_global_document = load_verified_bar_state_json(tmp_path, global_a)
    missing_models_document = json.loads(json.dumps(valid_global_document))
    del missing_models_document["discovery_result"]["discovery_final_fit_models"]
    missing_models_global = publish_bar_state_json(
        tmp_path,
        kind="GLOBAL_RESULT",
        artifact_key_suffix="gate-global-missing-final-fit-models",
        document=missing_models_document,
        record_count=12,
        lineage=shared_lineage,
        logical_identity={"split_key": "discovery", "variant": "missing-models"},
    )
    with pytest.raises(BarStateArtifactError, match="unexpected schema or key set"):
        register_bar_state_artifact_link(
            database_url,
            tmp_path,
            research_run_attempt_id=first_attempt,
            candidate_key=first_key,
            artifact_role="GLOBAL_RESULT",
            split_key="discovery",
            shard_ordinal=0,
            artifact=missing_models_global,
        )

    forged_global_metadata = publish_bar_state_json(
        tmp_path,
        kind="GLOBAL_RESULT",
        artifact_key_suffix="gate-global-missing-binding-map",
        document=valid_global_document,
        record_count=12,
        lineage=shared_lineage,
        logical_identity={
            "candidate_selection_sha256_by_key": global_a.descriptor.logical_identity[
                "candidate_selection_sha256_by_key"
            ],
            "finalist_model_binding_sha256_by_key": global_a.descriptor.logical_identity[
                "finalist_model_binding_sha256_by_key"
            ],
            "split_key": "discovery",
        },
    )
    forged_global_metadata_id = _register_artifact(
        database_url,
        tmp_path,
        forged_global_metadata,
    )
    with (
        pytest.raises(
            psycopg.errors.RaiseException,
            match="global bar-state artifact semantic hash catalog drifted",
        ),
        psycopg.connect(database_url) as connection,
        connection.transaction(),
    ):
        connection.execute(
            _insert_link_sql(),
            _link_parameters(
                forged_global_metadata,
                artifact_id=forged_global_metadata_id,
                artifact_role="GLOBAL_RESULT",
                shard_ordinal=0,
                attempt_id=first_attempt,
            ),
        )

    # Nullable candidate lineage keys are still mandatory for shared artifacts.
    feature = shared["FEATURE"][0]
    feature_id = _register_artifact(database_url, tmp_path, feature)
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        row = connection.execute(
            "SELECT * FROM systematic_fx.artifacts WHERE artifact_id = %s",
            (feature_id,),
        ).fetchone()
        forged_metadata = json.loads(json.dumps(row["metadata"]))
        del forged_metadata["logical_identity"]["lineage"]["candidate_key"]
        identity_document = {
            key: value
            for key, value in forged_metadata.items()
            if key not in {"artifact_identity_sha256", "content_sha256"}
        }
        forged_identity = canonical_sha256(identity_document)
        forged_metadata["artifact_identity_sha256"] = forged_identity
        forged_id = connection.execute(
            """
            INSERT INTO systematic_fx.artifacts
                (artifact_key, artifact_type, uri, sha256, byte_size,
                 media_type, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING artifact_id
            """,
            (
                f"{feature.descriptor.artifact_key}:missing-null-lineage",
                row["artifact_type"],
                f"{row['uri']}.missing-null-lineage",
                row["sha256"],
                row["byte_size"],
                row["media_type"],
                Jsonb(forged_metadata),
            ),
        ).fetchone()["artifact_id"]
    missing_lineage_parameters = (
        forged_id,
        "FEATURE",
        0,
        forged_identity,
        feature.sha256,
        canonical_sha256(forged_metadata["logical_identity"]["lineage"]),
        first_attempt,
    )
    with (
        pytest.raises(psycopg.errors.RaiseException, match="bytes or lineage drifted"),
        psycopg.connect(database_url) as connection,
        connection.transaction(),
    ):
        connection.execute(_insert_link_sql(), missing_lineage_parameters)

    first_candidate = prepared.config.candidates[0]
    first_spec = specs[0]
    first_lineage = _candidate_lineage(
        shared_lineage,
        candidate_key=first_key,
        candidate_definition_sha256=first_candidate.definition_sha256,
        run_fingerprint=first_spec.fingerprint,
    )
    forged_model = publish_bar_state_json(
        tmp_path,
        kind="MODEL",
        artifact_key_suffix="gate-forged-top-level-candidate",
        document={"schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["MODEL"]},
        record_count=1,
        lineage=first_lineage,
        logical_identity={"candidate_key": second_key, "split_key": "discovery"},
    )
    forged_model_id = _register_artifact(database_url, tmp_path, forged_model)
    with (
        pytest.raises(
            psycopg.errors.RaiseException,
            match="candidate-specific bar-state artifact lineage drifted",
        ),
        psycopg.connect(database_url) as connection,
        connection.transaction(),
    ):
        connection.execute(
            _insert_link_sql(),
            _link_parameters(
                forged_model,
                artifact_id=forged_model_id,
                artifact_role="MODEL",
                shard_ordinal=0,
                attempt_id=first_attempt,
            ),
        )

    forged_terminal = publish_bar_state_json(
        tmp_path,
        kind="TERMINAL_RESULT",
        artifact_key_suffix="gate-forged-terminal-status",
        document={"schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["TERMINAL_RESULT"]},
        record_count=1,
        lineage=first_lineage,
        logical_identity={
            "candidate_key": first_key,
            "decision_label": "DISCOVERY_FINALIST",
            "split_key": "discovery",
            "trial_status": "REJECTED",
        },
    )
    forged_terminal_id = _register_artifact(database_url, tmp_path, forged_terminal)
    with (
        pytest.raises(
            psycopg.errors.RaiseException,
            match="terminal bar-state artifact decision/status drifted",
        ),
        psycopg.connect(database_url) as connection,
        connection.transaction(),
    ):
        connection.execute(
            _insert_link_sql(),
            _link_parameters(
                forged_terminal,
                artifact_id=forged_terminal_id,
                artifact_role="TERMINAL_RESULT",
                shard_ordinal=0,
                attempt_id=first_attempt,
            ),
        )

    # Hold the campaign-row lock after GLOBAL A, then prove concurrent GLOBAL B
    # cannot commit a write-skewed consensus.
    with psycopg.connect(database_url) as connection:
        outbox_before = connection.execute(
            "SELECT request_version FROM systematic_fx.publication_outbox "
            "WHERE scope_key = 'public-research'"
        ).fetchone()[0]
    connection_a = psycopg.connect(database_url)
    connection_a.isolation_level = IsolationLevel.SERIALIZABLE
    connection_a.execute(
        _insert_link_sql(),
        _link_parameters(
            global_a,
            artifact_id=global_a_id,
            artifact_role="GLOBAL_RESULT",
            shard_ordinal=0,
            attempt_id=first_attempt,
        ),
    )
    started = threading.Event()

    def insert_conflicting_global() -> None:
        with psycopg.connect(database_url) as connection_b:
            connection_b.isolation_level = IsolationLevel.SERIALIZABLE
            with connection_b.transaction():
                started.set()
                connection_b.execute(
                    _insert_link_sql(),
                    _link_parameters(
                        global_b,
                        artifact_id=global_b_id,
                        artifact_role="GLOBAL_RESULT",
                        shard_ordinal=0,
                        attempt_id=second_attempt,
                    ),
                )

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(insert_conflicting_global)
            assert started.wait(timeout=5)
            with pytest.raises(FutureTimeoutError):
                future.result(timeout=0.25)
            connection_a.commit()
            with pytest.raises(
                (
                    psycopg.errors.RaiseException,
                    psycopg.errors.SerializationFailure,
                )
            ) as conflict:
                future.result(timeout=5)
            if isinstance(conflict.value, psycopg.errors.RaiseException):
                assert "one exact global result" in str(conflict.value)
    finally:
        connection_a.close()
    with psycopg.connect(database_url) as connection:
        outbox_after = connection.execute(
            "SELECT request_version FROM systematic_fx.publication_outbox "
            "WHERE scope_key = 'public-research'"
        ).fetchone()[0]
        globals_after_conflict = connection.execute(
            "SELECT count(*) FROM systematic_fx.bar_state_artifact_links "
            "WHERE artifact_role = 'GLOBAL_RESULT'"
        ).fetchone()[0]
    assert outbox_after > outbox_before
    assert globals_after_conflict == 1

    candidate_evidence: dict[str, dict[str, object]] = {}
    for candidate_key, candidate, spec in zip(
        prepared.candidate_keys,
        prepared.config.candidates,
        specs,
        strict=True,
    ):
        trial_status = "SUCCEEDED" if candidate_key in finalist_keys else "REJECTED"
        lineage = _candidate_lineage(
            shared_lineage,
            candidate_key=candidate_key,
            candidate_definition_sha256=candidate.definition_sha256,
            run_fingerprint=spec.fingerprint,
        )
        candidate_evidence[candidate_key] = _publish_candidate_evidence(
            tmp_path,
            candidate_key=candidate_key,
            global_document=valid_global_document,
            lineage=lineage,
            trial_status=trial_status,
            capped=candidate_key == capped_key,
        )

    second_candidate = prepared.config.candidates[1]
    second_lineage = _candidate_lineage(
        shared_lineage,
        candidate_key=second_key,
        candidate_definition_sha256=second_candidate.definition_sha256,
        run_fingerprint=specs[1].fingerprint,
    )
    expected_second_oos_rows = candidate_evidence[second_key]["OOS_TRADE"].descriptor.record_count
    mismatched_oos = publish_bar_state_parquet(
        tmp_path,
        kind="OOS_TRADE",
        artifact_key_suffix=f"gate-oos-row-mismatch-{second_key}",
        table=pa.table(
            {
                "gate_value": pa.array(
                    range(expected_second_oos_rows + 1),
                    type=pa.int64(),
                )
            }
        ),
        lineage=second_lineage,
        logical_identity={
            "candidate_key": second_key,
            "row_count": expected_second_oos_rows + 1,
            "split_key": "discovery",
        },
    )
    mismatched_oos_id = _register_artifact(database_url, tmp_path, mismatched_oos)
    second_model = candidate_evidence[second_key]["MODEL"]
    second_model_id = _register_artifact(database_url, tmp_path, second_model)
    second_terminal = candidate_evidence[second_key]["TERMINAL_RESULT"]
    second_terminal_id = _register_artifact(database_url, tmp_path, second_terminal)
    with (
        pytest.raises(
            psycopg.errors.RaiseException,
            match="differs from GLOBAL semantic binding",
        ),
        psycopg.connect(database_url) as connection,
        connection.transaction(),
    ):
        for role, artifact, artifact_id in (
            ("OOS_TRADE", mismatched_oos, mismatched_oos_id),
            ("GLOBAL_RESULT", global_a, global_a_id),
            ("MODEL", second_model, second_model_id),
            ("TERMINAL_RESULT", second_terminal, second_terminal_id),
        ):
            connection.execute(
                _insert_link_sql(),
                _link_parameters(
                    artifact,
                    artifact_id=artifact_id,
                    artifact_role=role,
                    shard_ordinal=0,
                    attempt_id=second_attempt,
                ),
            )

    valid_first_model = candidate_evidence[first_key]["MODEL"]
    missing_package_model = publish_bar_state_json(
        tmp_path,
        kind="MODEL",
        artifact_key_suffix="gate-model-missing-package-projection",
        document=load_verified_bar_state_json(tmp_path, valid_first_model),
        record_count=valid_first_model.descriptor.record_count,
        lineage=first_lineage,
        logical_identity={
            key: value
            for key, value in valid_first_model.descriptor.logical_identity.items()
            if key
            not in {
                "artifact_kind",
                "campaign_key",
                "lineage",
                "lineage_sha256",
                "model_package_projection",
                "model_package_projection_sha256",
            }
        },
    )
    missing_package_model_id = _register_artifact(
        database_url,
        tmp_path,
        missing_package_model,
    )
    with (
        pytest.raises(
            psycopg.errors.RaiseException,
            match="MODEL bar-state artifact semantic identity drifted",
        ),
        psycopg.connect(database_url) as connection,
        connection.transaction(),
    ):
        connection.execute(
            _insert_link_sql(),
            _link_parameters(
                missing_package_model,
                artifact_id=missing_package_model_id,
                artifact_role="MODEL",
                shard_ordinal=0,
                attempt_id=first_attempt,
            ),
        )

    valid_first_terminal = candidate_evidence[first_key]["TERMINAL_RESULT"]
    forged_binding_document = load_verified_bar_state_json(tmp_path, valid_first_terminal)
    forged_binding = {
        **forged_binding_document["result"]["discovery_final_fit_model"],
        "model_sha256": "f" * 64,
    }
    forged_binding_document["result"]["discovery_final_fit_model"] = forged_binding
    forged_binding_document["compact_summary"]["discovery_final_fit_model_sha256"] = "f" * 64
    forged_binding_terminal = publish_bar_state_json(
        tmp_path,
        kind="TERMINAL_RESULT",
        artifact_key_suffix="gate-forged-global-binding",
        document=forged_binding_document,
        record_count=1,
        lineage=first_lineage,
        logical_identity={
            **{
                key: value
                for key, value in valid_first_terminal.descriptor.logical_identity.items()
                if key not in {"artifact_kind", "campaign_key", "lineage", "lineage_sha256"}
            },
            "compact_summary_sha256": canonical_sha256(forged_binding_document["compact_summary"]),
            "finalist_model_binding": forged_binding,
            "finalist_model_binding_sha256": canonical_sha256(forged_binding),
        },
    )
    forged_hash_terminal = publish_bar_state_json(
        tmp_path,
        kind="TERMINAL_RESULT",
        artifact_key_suffix="gate-forged-selection-hash",
        document=load_verified_bar_state_json(tmp_path, valid_first_terminal),
        record_count=1,
        lineage=first_lineage,
        logical_identity={
            **{
                key: value
                for key, value in valid_first_terminal.descriptor.logical_identity.items()
                if key
                in {
                    "candidate_key",
                    "compact_summary_sha256",
                    "decision_label",
                    "finalist_model_binding",
                    "finalist_model_binding_sha256",
                    "candidate_selection_projection_sha256",
                    "global_evidence_projection_sha256",
                    "model_package_projection_sha256",
                    "split_key",
                    "trial_status",
                }
            },
            "candidate_selection_sha256": "f" * 64,
        },
    )
    forged_hash_terminal_id = _register_artifact(
        database_url,
        tmp_path,
        forged_hash_terminal,
    )
    forged_projection_terminal = publish_bar_state_json(
        tmp_path,
        kind="TERMINAL_RESULT",
        artifact_key_suffix="gate-forged-selection-projection-hash",
        document=load_verified_bar_state_json(tmp_path, valid_first_terminal),
        record_count=1,
        lineage=first_lineage,
        logical_identity={
            **{
                key: value
                for key, value in valid_first_terminal.descriptor.logical_identity.items()
                if key not in {"artifact_kind", "campaign_key", "lineage", "lineage_sha256"}
            },
            "candidate_selection_projection_sha256": "e" * 64,
        },
    )
    forged_projection_terminal_id = _register_artifact(
        database_url,
        tmp_path,
        forged_projection_terminal,
    )
    forged_slice_document = load_verified_bar_state_json(tmp_path, valid_first_terminal)
    forged_slice_document["result"]["candidate_support"]["distinct_signal_day_count"] = 39
    forged_slice = {
        "candidate_support": forged_slice_document["result"]["candidate_support"],
        "multiplicity_cells": forged_slice_document["result"]["multiplicity_cells"],
    }
    forged_slice_terminal = publish_bar_state_json(
        tmp_path,
        kind="TERMINAL_RESULT",
        artifact_key_suffix="gate-forged-candidate-evidence-slice",
        document=forged_slice_document,
        record_count=1,
        lineage=first_lineage,
        logical_identity={
            **{
                key: value
                for key, value in valid_first_terminal.descriptor.logical_identity.items()
                if key not in {"artifact_kind", "campaign_key", "lineage", "lineage_sha256"}
            },
            "candidate_evidence_slice_sha256": canonical_sha256(forged_slice),
        },
    )
    forged_slice_terminal_id = _register_artifact(
        database_url,
        tmp_path,
        forged_slice_terminal,
    )
    forged_inner_document = load_verified_bar_state_json(tmp_path, valid_first_terminal)
    forged_inner_document["result"]["candidate_selection"]["positive_component_size"] = 99
    forged_inner_terminal = publish_bar_state_json(
        tmp_path,
        kind="TERMINAL_RESULT",
        artifact_key_suffix="gate-forged-inner-result",
        document=forged_inner_document,
        record_count=1,
        lineage=first_lineage,
        logical_identity={
            "candidate_key": first_key,
            "compact_summary_sha256": canonical_sha256(forged_inner_document["compact_summary"]),
            "decision_label": "DISCOVERY_FINALIST",
            "split_key": "discovery",
            "trial_status": "SUCCEEDED",
        },
    )
    with pytest.raises(
        BarStateArtifactError,
        match="terminal compact summary differs from its result projection",
    ):
        register_bar_state_artifact_link(
            database_url,
            tmp_path,
            research_run_attempt_id=first_attempt,
            candidate_key=first_key,
            artifact_role="TERMINAL_RESULT",
            split_key="discovery",
            shard_ordinal=0,
            artifact=forged_inner_terminal,
        )

    for candidate_key in prepared.candidate_keys:
        attempt_id = reservations[candidate_key].research_run_attempt_id
        for role in ("FEATURE", "LABEL"):
            for shard, artifact in enumerate(shared[role]):
                register_bar_state_artifact_link(
                    database_url,
                    tmp_path,
                    research_run_attempt_id=attempt_id,
                    candidate_key=candidate_key,
                    artifact_role=role,
                    split_key="discovery",
                    shard_ordinal=shard,
                    artifact=artifact,
                )
        for role in ("MODEL", "OOS_TRADE"):
            register_bar_state_artifact_link(
                database_url,
                tmp_path,
                research_run_attempt_id=attempt_id,
                candidate_key=candidate_key,
                artifact_role=role,
                split_key="discovery",
                shard_ordinal=0,
                artifact=candidate_evidence[candidate_key][role],
            )
        if candidate_key != first_key:
            register_bar_state_artifact_link(
                database_url,
                tmp_path,
                research_run_attempt_id=attempt_id,
                candidate_key=candidate_key,
                artifact_role="GLOBAL_RESULT",
                split_key="discovery",
                shard_ordinal=0,
                artifact=global_a,
            )
        if candidate_key == first_key:
            with pytest.raises(
                BarStateRegistryDriftError,
                match="GLOBAL semantic binding",
            ):
                register_bar_state_artifact_link(
                    database_url,
                    tmp_path,
                    research_run_attempt_id=attempt_id,
                    candidate_key=candidate_key,
                    artifact_role="TERMINAL_RESULT",
                    split_key="discovery",
                    shard_ordinal=0,
                    artifact=forged_slice_terminal,
                )
            with (
                pytest.raises(
                    psycopg.errors.RaiseException,
                    match="differs from GLOBAL semantic binding",
                ),
                psycopg.connect(database_url) as connection,
                connection.transaction(),
            ):
                connection.execute(
                    _insert_link_sql(),
                    _link_parameters(
                        forged_slice_terminal,
                        artifact_id=forged_slice_terminal_id,
                        artifact_role="TERMINAL_RESULT",
                        shard_ordinal=0,
                        attempt_id=attempt_id,
                    ),
                )
            with pytest.raises(
                BarStateRegistryDriftError,
                match="GLOBAL semantic binding",
            ):
                register_bar_state_artifact_link(
                    database_url,
                    tmp_path,
                    research_run_attempt_id=attempt_id,
                    candidate_key=candidate_key,
                    artifact_role="TERMINAL_RESULT",
                    split_key="discovery",
                    shard_ordinal=0,
                    artifact=forged_binding_terminal,
                )
            with (
                pytest.raises(
                    psycopg.errors.RaiseException,
                    match="differs from GLOBAL semantic binding",
                ),
                psycopg.connect(database_url) as connection,
                connection.transaction(),
            ):
                connection.execute(
                    _insert_link_sql(),
                    _link_parameters(
                        forged_hash_terminal,
                        artifact_id=forged_hash_terminal_id,
                        artifact_role="TERMINAL_RESULT",
                        shard_ordinal=0,
                        attempt_id=attempt_id,
                    ),
                )
            with (
                pytest.raises(
                    psycopg.errors.RaiseException,
                    match="differs from GLOBAL semantic binding",
                ),
                psycopg.connect(database_url) as connection,
                connection.transaction(),
            ):
                connection.execute(
                    _insert_link_sql(),
                    _link_parameters(
                        forged_projection_terminal,
                        artifact_id=forged_projection_terminal_id,
                        artifact_role="TERMINAL_RESULT",
                        shard_ordinal=0,
                        attempt_id=attempt_id,
                    ),
                )
        register_bar_state_artifact_link(
            database_url,
            tmp_path,
            research_run_attempt_id=attempt_id,
            candidate_key=candidate_key,
            artifact_role="TERMINAL_RESULT",
            split_key="discovery",
            shard_ordinal=0,
            artifact=candidate_evidence[candidate_key]["TERMINAL_RESULT"],
        )

    terminal_artifact = candidate_evidence[first_key]["TERMINAL_RESULT"]
    first_compact_summary = candidate_evidence[first_key]["COMPACT_SUMMARY"]
    with pytest.raises(
        BarStateRegistryDriftError,
        match="terminal artifact differs from the compact terminal summary",
    ):
        register_terminal_bar_state_result(
            database_url,
            tmp_path,
            research_run_attempt_id=first_attempt,
            candidate_key=first_key,
            trial_status="SUCCEEDED",
            decision_label="DISCOVERY_FINALIST",
            compact_summary={
                **first_compact_summary,
                "positive_component_size": 10,
            },
        )
    with psycopg.connect(database_url) as connection:
        terminal_artifact_id = connection.execute(
            "SELECT artifact_id FROM systematic_fx.artifacts WHERE artifact_key = %s",
            (terminal_artifact.descriptor.artifact_key,),
        ).fetchone()[0]
    with (
        pytest.raises(
            psycopg.errors.RaiseException,
            match="succeeded bar-state attempt lacks complete exact evidence",
        ),
        psycopg.connect(database_url) as connection,
        connection.transaction(),
    ):
        connection.execute(
            """
                UPDATE systematic_fx.research_run_attempts
                SET status = 'SUCCEEDED', result_artifact_id = %s,
                    result_summary = %s, finished_at = statement_timestamp()
                WHERE research_run_attempt_id = %s
                """,
            (terminal_artifact_id, Jsonb({"forged": True}), first_attempt),
        )
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        link_rows = connection.execute(
            """
            SELECT artifact_id, artifact_identity_sha256, artifact_role,
                   content_sha256, lineage_sha256, shard_ordinal, split_key
            FROM systematic_fx.bar_state_artifact_links
            WHERE research_run_attempt_id = %s
            ORDER BY artifact_role, split_key, shard_ordinal
            """,
            (first_attempt,),
        ).fetchall()
    link_manifest = [
        {
            "artifact_id": int(row["artifact_id"]),
            "artifact_identity_sha256": str(row["artifact_identity_sha256"]),
            "artifact_role": str(row["artifact_role"]),
            "content_sha256": str(row["content_sha256"]),
            "lineage_sha256": str(row["lineage_sha256"]),
            "shard_ordinal": int(row["shard_ordinal"]),
            "split_key": str(row["split_key"]),
        }
        for row in link_rows
    ]
    role_counts: dict[str, int] = {}
    for row in link_rows:
        role = str(row["artifact_role"])
        role_counts[role] = role_counts.get(role, 0) + 1
    valid_summary = {
        "artifact_link_manifest_sha256": canonical_sha256(link_manifest),
        "artifact_role_counts": dict(sorted(role_counts.items())),
        "attempt_status": "SUCCEEDED",
        "candidate_key": first_key,
        "candidate_evidence_slice_sha256": terminal_artifact.descriptor.logical_identity[
            "candidate_evidence_slice_sha256"
        ],
        "candidate_selection_sha256": terminal_artifact.descriptor.logical_identity[
            "candidate_selection_sha256"
        ],
        "candidate_selection_projection_sha256": (
            terminal_artifact.descriptor.logical_identity["candidate_selection_projection_sha256"]
        ),
        "compact_summary": first_compact_summary,
        "decision_label": "DISCOVERY_FINALIST",
        "finalist_model_binding_sha256": terminal_artifact.descriptor.logical_identity[
            "finalist_model_binding_sha256"
        ],
        "global_evidence_projection_sha256": terminal_artifact.descriptor.logical_identity[
            "global_evidence_projection_sha256"
        ],
        "model_package_projection_sha256": terminal_artifact.descriptor.logical_identity[
            "model_package_projection_sha256"
        ],
        "result_artifact_id": terminal_artifact_id,
        "run_fingerprint": first_spec.fingerprint,
        "schema": "systematic_fx.bar_state_terminal_summary.v1",
        "trial_status": "SUCCEEDED",
    }
    forged_price_summary = json.loads(json.dumps(valid_summary))
    forged_price_summary["compact_summary"]["price_policy"]["entry_reference"] = "ARBITRARY_PRICE"
    with (
        pytest.raises(
            psycopg.errors.RaiseException,
            match="succeeded bar-state attempt lacks complete exact evidence",
        ),
        psycopg.connect(database_url) as connection,
        connection.transaction(),
    ):
        connection.execute(
            """
            UPDATE systematic_fx.research_run_attempts
            SET status = 'SUCCEEDED', result_artifact_id = %s,
                result_summary = %s, finished_at = statement_timestamp()
            WHERE research_run_attempt_id = %s
            """,
            (terminal_artifact_id, Jsonb(forged_price_summary), first_attempt),
        )
    mismatched_trial_summary = {
        **valid_summary,
        "compact_summary": {
            **first_compact_summary,
            "positive_component_size": 10,
        },
    }
    with (
        pytest.raises(
            psycopg.errors.RaiseException,
            match="terminal RunSpec requires one exact attempt/trial pair",
        ),
        psycopg.connect(database_url) as connection,
        connection.transaction(),
    ):
        connection.execute(
            """
            UPDATE systematic_fx.research_run_attempts
            SET status = 'SUCCEEDED', result_artifact_id = %s,
                result_summary = %s, finished_at = statement_timestamp()
            WHERE research_run_attempt_id = %s
            """,
            (terminal_artifact_id, Jsonb(valid_summary), first_attempt),
        )
        connection.execute(
            """
            UPDATE systematic_fx.experiment_trials
            SET status = 'SUCCEEDED', result_summary = %s,
                started_at = statement_timestamp(),
                finished_at = statement_timestamp()
            WHERE research_run_spec_id = %s
            """,
            (
                Jsonb(mismatched_trial_summary),
                registrations[first_key].research_run_spec_id,
            ),
        )

    for candidate_key in prepared.candidate_keys:
        trial_status = "SUCCEEDED" if candidate_key in finalist_keys else "REJECTED"
        decision_label = "DISCOVERY_FINALIST" if trial_status == "SUCCEEDED" else "DISCOVERY_REJECT"
        report = register_terminal_bar_state_result(
            database_url,
            tmp_path,
            research_run_attempt_id=reservations[candidate_key].research_run_attempt_id,
            candidate_key=candidate_key,
            trial_status=trial_status,
            decision_label=decision_label,
            compact_summary=candidate_evidence[candidate_key]["COMPACT_SUMMARY"],
        )
        assert report.candidate_key == candidate_key
        assert report.trial_status == trial_status

    duplicate_reports = {}
    for candidate_key, spec in zip(prepared.candidate_keys, specs, strict=True):
        duplicate = reserve_run_attempt(database_url, run_fingerprint=spec.fingerprint)
        assert not duplicate.execute and duplicate.status == "SKIPPED_DUPLICATE"
        duplicate_reports[candidate_key] = validate_reused_bar_state_attempt(
            database_url,
            tmp_path,
            reservation=duplicate,
            candidate_key=candidate_key,
        )
    duplicate_global_sha256, duplicate_finalists, duplicate_terminals = (
        _validate_duplicate_consensus(prepared, duplicate_reports)
    )
    assert duplicate_global_sha256 == global_a.sha256
    assert duplicate_finalists == finalist_keys
    assert set(duplicate_terminals) == set(prepared.candidate_keys)

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        terminal_counts = connection.execute(
            """
            SELECT
                (SELECT count(*) FROM systematic_fx.research_run_attempts
                 WHERE status = 'SUCCEEDED') AS attempts,
                (SELECT count(*) FROM systematic_fx.experiment_trials
                 WHERE status = 'SUCCEEDED') AS finalists,
                (SELECT count(*) FROM systematic_fx.experiment_trials
                 WHERE status = 'REJECTED') AS rejects,
                (SELECT count(*) FROM systematic_fx.research_run_attempts
                 WHERE status = 'SKIPPED_DUPLICATE') AS duplicates,
                (SELECT count(*) FROM systematic_fx.bar_state_artifact_links) AS links,
                (SELECT count(DISTINCT artifact_identity_sha256)
                 FROM systematic_fx.bar_state_artifact_links
                 WHERE artifact_role = 'GLOBAL_RESULT') AS global_identities
            """
        ).fetchone()
        exact_pairs = connection.execute(
            """
            SELECT count(*) AS count
            FROM systematic_fx.research_run_attempts AS attempt
            JOIN systematic_fx.experiment_trials AS trial
              ON trial.research_run_spec_id = attempt.research_run_spec_id
            JOIN systematic_fx.bar_state_artifact_links AS terminal_link
              ON terminal_link.research_run_attempt_id = attempt.research_run_attempt_id
             AND terminal_link.artifact_role = 'TERMINAL_RESULT'
            WHERE attempt.status = 'SUCCEEDED'
              AND trial.status IN ('SUCCEEDED', 'REJECTED')
              AND attempt.result_summary = trial.result_summary
              AND attempt.result_artifact_id = terminal_link.artifact_id
            """
        ).fetchone()["count"]
    assert terminal_counts == {
        "attempts": 12,
        "finalists": 4,
        "rejects": 8,
        "duplicates": 12,
        "links": 144,
        "global_identities": 1,
    }
    assert exact_pairs == 12


def test_bar_state_v2a_postgresql_release_gate(tmp_path: Path) -> None:
    database_url = _disposable_v2a_database_url()
    migrations = discover_migrations(ROOT / "migrations")
    assert tuple(item.version for item in migrations) == tuple(range(1, 28))
    first = apply_migrations(
        database_url,
        directory=ROOT / "migrations",
        psql_binary=os.environ.get("SYSTEMATIC_FX_PSQL"),
    )
    assert first.applied == tuple(range(1, 28))
    repeated = apply_migrations(
        database_url,
        directory=ROOT / "migrations",
        psql_binary=os.environ.get("SYSTEMATIC_FX_PSQL"),
    )
    assert repeated.applied == ()
    assert repeated.skipped == tuple(range(1, 28))

    _seed_dataset(
        database_url,
        tmp_path,
        manifest_sha256=BAR_STATE_RAW_SOURCE_MANIFEST_SHA256,
    )
    (
        predecessor,
        predecessor_provenance,
        predecessor_specs,
        predecessor_registrations,
    ) = _register_gate_campaign(
        database_url,
        tmp_path,
        profile=BAR_STATE_V2_PROFILE,
        code_commit=BAR_STATE_V2A_PROFILE.predecessor_code_commit or "",
    )
    predecessor_reservations = {
        candidate_key: reserve_run_attempt(database_url, run_fingerprint=spec.fingerprint)
        for candidate_key, spec in zip(
            predecessor.candidate_keys,
            predecessor_specs,
            strict=True,
        )
    }
    first_predecessor_key = predecessor.candidate_keys[0]
    with psycopg.connect(database_url) as connection:
        connection.execute(
            "UPDATE systematic_fx.datasets SET dataset_key = %s WHERE dataset_key = %s",
            ("glbx_mdp3_mbp_10_6e_fut_v1_drifted", BAR_STATE_DATASET_KEY),
        )
        with (
            pytest.raises(
                psycopg.errors.RaiseException,
                match="requires all 12 exact prebound candidates",
            ),
            connection.transaction(),
        ):
            connection.execute(
                """
                UPDATE systematic_fx.research_run_attempts
                SET status = 'FAILED', result_summary = %s,
                    error_message = 'invalid predecessor cleanup',
                    finished_at = statement_timestamp()
                WHERE research_run_attempt_id = %s
                """,
                (
                    Jsonb(
                        {
                            "candidate_key": first_predecessor_key,
                            "run_fingerprint": predecessor_specs[0].fingerprint,
                        }
                    ),
                    predecessor_reservations[first_predecessor_key].research_run_attempt_id,
                ),
            )
        connection.rollback()
    for candidate_key, spec in zip(
        predecessor.candidate_keys,
        predecessor_specs,
        strict=True,
    ):
        reservation = predecessor_reservations[candidate_key]
        start_run_attempt(
            database_url,
            research_run_attempt_id=reservation.research_run_attempt_id,
        )
        abort_bar_state_run_attempt(
            database_url,
            research_run_attempt_id=reservation.research_run_attempt_id,
            candidate_key=candidate_key,
            run_fingerprint=spec.fingerprint,
            error_message="frozen optimizer cap exhausted",
            profile=BAR_STATE_V2_PROFILE,
        )
    predecessor_report = require_clean_bar_state_predecessor(database_url)
    assert predecessor_report.candidate_count == 12
    assert predecessor_report.failed_attempt_count == 12
    with psycopg.connect(database_url) as connection:
        predecessor_snapshot = connection.execute(
            """
            SELECT systematic_fx.canonical_jsonb_sha256(jsonb_build_object(
                'attempts', (
                    SELECT jsonb_agg(to_jsonb(attempt) ORDER BY attempt.research_run_attempt_id)
                    FROM systematic_fx.research_run_attempts AS attempt
                    JOIN systematic_fx.research_run_specs AS run_spec
                      ON run_spec.research_run_spec_id = attempt.research_run_spec_id
                    JOIN systematic_fx.campaigns AS campaign
                      ON campaign.campaign_id = run_spec.campaign_id
                    WHERE campaign.campaign_key = 'bar_state_conditional_v2'
                ),
                'specs', (
                    SELECT jsonb_agg(to_jsonb(run_spec) ORDER BY run_spec.research_run_spec_id)
                    FROM systematic_fx.research_run_specs AS run_spec
                    JOIN systematic_fx.campaigns AS campaign
                      ON campaign.campaign_id = run_spec.campaign_id
                    WHERE campaign.campaign_key = 'bar_state_conditional_v2'
                ),
                'trials', (
                    SELECT jsonb_agg(to_jsonb(trial) ORDER BY trial.experiment_trial_id)
                    FROM systematic_fx.experiment_trials AS trial
                    JOIN systematic_fx.experiments AS experiment
                      ON experiment.experiment_id = trial.experiment_id
                    JOIN systematic_fx.campaigns AS campaign
                      ON campaign.campaign_id = experiment.campaign_id
                    WHERE campaign.campaign_key = 'bar_state_conditional_v2'
                )
            )) AS sha256
            """
        ).fetchone()[0]

    successor = load_prepared_bar_state_run(ROOT, profile=BAR_STATE_V2A_PROFILE)
    successor_provenance = _provenance(database_url, code_commit="3" * 40)
    successor_specs = build_bar_state_run_specs(successor, successor_provenance)
    # Hold the exact predecessor row through the V2A INSERT.  A concurrent V2
    # RunSpec append must wait, then fail after the successor commits.
    successor_connection = psycopg.connect(database_url)
    successor_connection.execute(
        """
        INSERT INTO systematic_fx.campaigns
            (campaign_key, dataset_id, name, status, selected_start_date,
             selected_end_date, roll_cutoff_date, data_manifest_sha256,
             feature_version, outcome_version, cost_model_version,
             execution_model_version, code_commit, config_sha256,
             split_policy, trial_budget, finalist_budget, frozen_at)
        SELECT %s, dataset_id, %s, status, selected_start_date,
               selected_end_date, roll_cutoff_date, data_manifest_sha256,
               feature_version, outcome_version, cost_model_version,
               execution_model_version, %s, %s, split_policy,
               trial_budget, finalist_budget, statement_timestamp()
        FROM systematic_fx.campaigns
        WHERE campaign_key = %s
        """,
        (
            BAR_STATE_V2A_PROFILE.campaign_key,
            BAR_STATE_V2A_PROFILE.campaign_name,
            successor_provenance.code_commit,
            BAR_STATE_V2A_PROFILE.campaign_definition_sha256,
            BAR_STATE_V2_PROFILE.campaign_key,
        ),
    )
    concurrent_started = threading.Event()

    def insert_late_predecessor_spec() -> None:
        with psycopg.connect(database_url) as connection:
            concurrent_started.set()
            connection.execute(
                _insert_extra_runspec_sql(),
                (
                    "f" * 64,
                    predecessor_registrations[predecessor.candidate_keys[0]].research_run_spec_id,
                ),
            )

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(insert_late_predecessor_spec)
            assert concurrent_started.wait(timeout=5)
            with pytest.raises(FutureTimeoutError):
                future.result(timeout=0.25)
            successor_connection.commit()
            with pytest.raises(psycopg.errors.RaiseException, match="RunSpec catalog is frozen"):
                future.result(timeout=5)
    finally:
        successor_connection.close()

    prepared, provenance, specs, _registrations = _register_gate_campaign(
        database_url,
        tmp_path,
        profile=BAR_STATE_V2A_PROFILE,
        code_commit=successor_provenance.code_commit,
    )
    assert provenance.code_commit == successor_provenance.code_commit
    assert tuple(item.fingerprint for item in specs) == tuple(
        item.fingerprint for item in successor_specs
    )

    first_predecessor_registration = predecessor_registrations[predecessor.candidate_keys[0]]
    with (
        pytest.raises(psycopg.errors.RaiseException, match="predecessor lifecycle is frozen"),
        psycopg.connect(database_url) as connection,
        connection.transaction(),
    ):
        connection.execute(
            """
            INSERT INTO systematic_fx.research_run_attempts
                (research_run_spec_id, attempt_number, status)
            VALUES (%s, 2, 'QUEUED')
            """,
            (first_predecessor_registration.research_run_spec_id,),
        )
    with (
        pytest.raises(psycopg.errors.RaiseException, match="predecessor lifecycle is frozen"),
        psycopg.connect(database_url) as connection,
        connection.transaction(),
    ):
        connection.execute(
            """
            UPDATE systematic_fx.experiment_trials
            SET result_summary = result_summary
            WHERE research_run_spec_id = %s
            """,
            (first_predecessor_registration.research_run_spec_id,),
        )
    with (
        pytest.raises(psycopg.errors.RaiseException, match="RunSpec catalog is frozen"),
        psycopg.connect(database_url) as connection,
        connection.transaction(),
    ):
        connection.execute(
            _insert_extra_runspec_sql(),
            ("e" * 64, first_predecessor_registration.research_run_spec_id),
        )

    reservations = {
        candidate_key: reserve_run_attempt(database_url, run_fingerprint=spec.fingerprint)
        for candidate_key, spec in zip(prepared.candidate_keys, specs, strict=True)
    }
    last_key = prepared.candidate_keys[-1]
    for candidate_key in prepared.candidate_keys[:-1]:
        start_run_attempt(
            database_url,
            research_run_attempt_id=reservations[candidate_key].research_run_attempt_id,
        )

    # A mutable dataset identity drift blocks starts, but the narrowly scoped
    # QUEUED/RUNNING -> FAILED cleanup path remains available.  Roll back the
    # probe so the same twelve attempts can complete the happy lifecycle.
    with psycopg.connect(database_url) as connection:
        connection.execute(
            "UPDATE systematic_fx.datasets SET dataset_key = %s WHERE dataset_key = %s",
            ("glbx_mdp3_mbp_10_6e_fut_v1_drifted", BAR_STATE_DATASET_KEY),
        )
        with (
            pytest.raises(
                psycopg.errors.RaiseException,
                match="requires all 12 exact prebound candidates",
            ),
            connection.transaction(),
        ):
            connection.execute(
                """
                UPDATE systematic_fx.research_run_attempts
                SET status = 'RUNNING', started_at = statement_timestamp()
                WHERE research_run_attempt_id = %s
                """,
                (reservations[last_key].research_run_attempt_id,),
            )
        connection.execute(
            """
            UPDATE systematic_fx.research_run_attempts
            SET status = 'FAILED', result_summary = %s,
                error_message = 'identity drift cleanup',
                finished_at = statement_timestamp()
            WHERE research_run_attempt_id = %s
            """,
            (
                Jsonb(
                    {
                        "candidate_key": last_key,
                        "run_fingerprint": specs[-1].fingerprint,
                    }
                ),
                reservations[last_key].research_run_attempt_id,
            ),
        )
        assert (
            connection.execute(
                "SELECT status FROM systematic_fx.research_run_attempts "
                "WHERE research_run_attempt_id = %s",
                (reservations[last_key].research_run_attempt_id,),
            ).fetchone()[0]
            == "FAILED"
        )
        connection.rollback()
    start_run_attempt(
        database_url,
        research_run_attempt_id=reservations[last_key].research_run_attempt_id,
    )

    with psycopg.connect(database_url) as connection:
        coexistence = connection.execute(
            """
            SELECT count(*)
            FROM (
                SELECT trial.trial_key
                FROM systematic_fx.experiment_trials AS trial
                JOIN systematic_fx.experiments AS experiment
                  ON experiment.experiment_id = trial.experiment_id
                JOIN systematic_fx.campaigns AS campaign
                  ON campaign.campaign_id = experiment.campaign_id
                WHERE campaign.campaign_key IN (%s, %s)
                GROUP BY trial.trial_key
                HAVING count(*) = 2
            ) AS shared_keys
            """,
            (BAR_STATE_V2_PROFILE.campaign_key, BAR_STATE_V2A_PROFILE.campaign_key),
        ).fetchone()[0]
        trigger_counts = dict(
            connection.execute(
                """
                SELECT trigger.tgname, count(*)
                FROM pg_catalog.pg_trigger AS trigger
                JOIN pg_catalog.pg_class AS relation ON relation.oid = trigger.tgrelid
                JOIN pg_catalog.pg_namespace AS namespace
                  ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = 'systematic_fx'
                  AND trigger.tgname IN (
                      'campaigns_require_bar_state_v2a_predecessor',
                      'research_run_specs_freeze_bar_state_v2_predecessor',
                      'bar_state_artifact_links_publication_refresh'
                  )
                  AND NOT trigger.tgisinternal
                GROUP BY trigger.tgname
                """
            ).fetchall()
        )
    assert coexistence == 12
    assert trigger_counts == {
        "bar_state_artifact_links_publication_refresh": 1,
        "campaigns_require_bar_state_v2a_predecessor": 1,
        "research_run_specs_freeze_bar_state_v2_predecessor": 1,
    }

    shared_lineage = _shared_lineage(prepared, provenance, specs)
    shared, global_result, _ = _publish_shared_evidence(
        tmp_path,
        shared_lineage,
        candidate_keys=prepared.candidate_keys,
        profile=BAR_STATE_V2A_PROFILE,
    )
    global_document = load_verified_bar_state_json(
        tmp_path,
        global_result,
        profile=BAR_STATE_V2A_PROFILE,
    )
    candidate_evidence: dict[str, dict[str, object]] = {}
    finalist_keys = prepared.candidate_keys[:4]
    capped_key = prepared.candidate_keys[4]
    for candidate_key, candidate, spec in zip(
        prepared.candidate_keys,
        prepared.config.candidates,
        specs,
        strict=True,
    ):
        lineage = _candidate_lineage(
            shared_lineage,
            candidate_key=candidate_key,
            candidate_definition_sha256=candidate.definition_sha256,
            run_fingerprint=spec.fingerprint,
        )
        candidate_evidence[candidate_key] = _publish_candidate_evidence(
            tmp_path,
            candidate_key=candidate_key,
            global_document=global_document,
            lineage=lineage,
            trial_status=("SUCCEEDED" if candidate_key in finalist_keys else "REJECTED"),
            capped=candidate_key == capped_key,
            profile=BAR_STATE_V2A_PROFILE,
        )

    first_key = prepared.candidate_keys[0]
    first_attempt_id = reservations[first_key].research_run_attempt_id
    first_feature = shared["FEATURE"][0]
    first_feature_id = _register_artifact(database_url, tmp_path, first_feature)

    # Current dataset identity is a live safety boundary for publication and
    # success, but never prevents fail-clean abort of an active attempt.
    with psycopg.connect(database_url) as connection:
        connection.execute(
            "UPDATE systematic_fx.datasets SET dataset_key = %s WHERE dataset_key = %s",
            ("glbx_mdp3_mbp_10_6e_fut_v1_drifted", BAR_STATE_DATASET_KEY),
        )
        with (
            pytest.raises(
                psycopg.errors.RaiseException,
                match="requires its RUNNING exact candidate",
            ),
            connection.transaction(),
        ):
            connection.execute(
                _insert_link_sql(),
                _link_parameters(
                    first_feature,
                    artifact_id=first_feature_id,
                    artifact_role="FEATURE",
                    shard_ordinal=0,
                    attempt_id=first_attempt_id,
                ),
            )
        with (
            pytest.raises(
                psycopg.errors.RaiseException,
                match="requires all 12 exact prebound candidates",
            ),
            connection.transaction(),
        ):
            connection.execute(
                """
                UPDATE systematic_fx.research_run_attempts
                SET status = 'SUCCEEDED', finished_at = statement_timestamp()
                WHERE research_run_attempt_id = %s
                """,
                (first_attempt_id,),
            )
        connection.execute(
            """
            UPDATE systematic_fx.research_run_attempts
            SET status = 'FAILED', result_summary = %s,
                error_message = 'identity drift cleanup',
                finished_at = statement_timestamp()
            WHERE research_run_attempt_id = %s
            """,
            (
                Jsonb(
                    {
                        "candidate_key": first_key,
                        "run_fingerprint": specs[0].fingerprint,
                    }
                ),
                first_attempt_id,
            ),
        )
        assert (
            connection.execute(
                "SELECT status FROM systematic_fx.research_run_attempts "
                "WHERE research_run_attempt_id = %s",
                (first_attempt_id,),
            ).fetchone()[0]
            == "FAILED"
        )
        connection.rollback()

    predecessor_artifact_prepared = replace(
        predecessor,
        project_root=tmp_path,
        data_root=tmp_path / "data",
    )
    cross_profile_artifact = _publish_code_snapshot(
        predecessor_artifact_prepared,
        predecessor_provenance,
        predecessor_specs,
    )
    with psycopg.connect(database_url) as connection:
        cross_profile_artifact_id = connection.execute(
            "SELECT artifact_id FROM systematic_fx.artifacts WHERE artifact_key = %s",
            (cross_profile_artifact.descriptor.artifact_key,),
        ).fetchone()[0]
    with (
        pytest.raises(psycopg.errors.RaiseException, match="bytes or lineage drifted"),
        psycopg.connect(database_url) as connection,
        connection.transaction(),
    ):
        connection.execute(
            _insert_link_sql(),
            _link_parameters(
                cross_profile_artifact,
                artifact_id=cross_profile_artifact_id,
                artifact_role="FEATURE",
                shard_ordinal=0,
                attempt_id=first_attempt_id,
            ),
        )

    for candidate_key in prepared.candidate_keys:
        attempt_id = reservations[candidate_key].research_run_attempt_id
        for role in ("FEATURE", "LABEL"):
            for shard_ordinal, artifact in enumerate(shared[role]):
                register_bar_state_artifact_link(
                    database_url,
                    tmp_path,
                    research_run_attempt_id=attempt_id,
                    candidate_key=candidate_key,
                    artifact_role=role,
                    split_key="discovery",
                    shard_ordinal=shard_ordinal,
                    artifact=artifact,
                    profile=BAR_STATE_V2A_PROFILE,
                )
        for role in ("MODEL", "OOS_TRADE"):
            register_bar_state_artifact_link(
                database_url,
                tmp_path,
                research_run_attempt_id=attempt_id,
                candidate_key=candidate_key,
                artifact_role=role,
                split_key="discovery",
                shard_ordinal=0,
                artifact=candidate_evidence[candidate_key][role],
                profile=BAR_STATE_V2A_PROFILE,
            )
        register_bar_state_artifact_link(
            database_url,
            tmp_path,
            research_run_attempt_id=attempt_id,
            candidate_key=candidate_key,
            artifact_role="GLOBAL_RESULT",
            split_key="discovery",
            shard_ordinal=0,
            artifact=global_result,
            profile=BAR_STATE_V2A_PROFILE,
        )
        register_bar_state_artifact_link(
            database_url,
            tmp_path,
            research_run_attempt_id=attempt_id,
            candidate_key=candidate_key,
            artifact_role="TERMINAL_RESULT",
            split_key="discovery",
            shard_ordinal=0,
            artifact=candidate_evidence[candidate_key]["TERMINAL_RESULT"],
            profile=BAR_STATE_V2A_PROFILE,
        )

    for candidate_key in prepared.candidate_keys:
        trial_status = "SUCCEEDED" if candidate_key in finalist_keys else "REJECTED"
        decision_label = "DISCOVERY_FINALIST" if trial_status == "SUCCEEDED" else "DISCOVERY_REJECT"
        report = register_terminal_bar_state_result(
            database_url,
            tmp_path,
            research_run_attempt_id=reservations[candidate_key].research_run_attempt_id,
            candidate_key=candidate_key,
            trial_status=trial_status,
            decision_label=decision_label,
            compact_summary=candidate_evidence[candidate_key]["COMPACT_SUMMARY"],
            profile=BAR_STATE_V2A_PROFILE,
        )
        assert report.candidate_key == candidate_key
        assert report.trial_status == trial_status

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        terminal_counts = connection.execute(
            """
            SELECT
                count(*) FILTER (WHERE attempt.status = 'SUCCEEDED') AS attempts,
                count(*) FILTER (WHERE trial.status = 'SUCCEEDED') AS finalists,
                count(*) FILTER (WHERE trial.status = 'REJECTED') AS rejects,
                count(*) FILTER (
                    WHERE attempt.status = 'SUCCEEDED'
                      AND trial.status IN ('SUCCEEDED', 'REJECTED')
                      AND attempt.result_summary = trial.result_summary
                ) AS exact_pairs
            FROM systematic_fx.research_run_attempts AS attempt
            JOIN systematic_fx.research_run_specs AS run_spec
              ON run_spec.research_run_spec_id = attempt.research_run_spec_id
            JOIN systematic_fx.campaigns AS campaign
              ON campaign.campaign_id = run_spec.campaign_id
            JOIN systematic_fx.experiment_trials AS trial
              ON trial.research_run_spec_id = run_spec.research_run_spec_id
            WHERE campaign.campaign_key = %s
            """,
            (BAR_STATE_V2A_PROFILE.campaign_key,),
        ).fetchone()
        link_counts = connection.execute(
            """
            SELECT count(*) AS links,
                   count(DISTINCT artifact_identity_sha256)
                       FILTER (WHERE artifact_role = 'GLOBAL_RESULT')
                       AS global_identities
            FROM systematic_fx.bar_state_artifact_links AS link
            JOIN systematic_fx.campaigns AS campaign
              ON campaign.campaign_id = link.campaign_id
            WHERE campaign.campaign_key = %s
            """,
            (BAR_STATE_V2A_PROFILE.campaign_key,),
        ).fetchone()
        predecessor_snapshot_after = connection.execute(
            """
            SELECT systematic_fx.canonical_jsonb_sha256(jsonb_build_object(
                'attempts', (
                    SELECT jsonb_agg(to_jsonb(attempt) ORDER BY attempt.research_run_attempt_id)
                    FROM systematic_fx.research_run_attempts AS attempt
                    JOIN systematic_fx.research_run_specs AS run_spec
                      ON run_spec.research_run_spec_id = attempt.research_run_spec_id
                    JOIN systematic_fx.campaigns AS campaign
                      ON campaign.campaign_id = run_spec.campaign_id
                    WHERE campaign.campaign_key = 'bar_state_conditional_v2'
                ),
                'specs', (
                    SELECT jsonb_agg(to_jsonb(run_spec) ORDER BY run_spec.research_run_spec_id)
                    FROM systematic_fx.research_run_specs AS run_spec
                    JOIN systematic_fx.campaigns AS campaign
                      ON campaign.campaign_id = run_spec.campaign_id
                    WHERE campaign.campaign_key = 'bar_state_conditional_v2'
                ),
                'trials', (
                    SELECT jsonb_agg(to_jsonb(trial) ORDER BY trial.experiment_trial_id)
                    FROM systematic_fx.experiment_trials AS trial
                    JOIN systematic_fx.experiments AS experiment
                      ON experiment.experiment_id = trial.experiment_id
                    JOIN systematic_fx.campaigns AS campaign
                      ON campaign.campaign_id = experiment.campaign_id
                    WHERE campaign.campaign_key = 'bar_state_conditional_v2'
                )
            )) AS sha256
            """
        ).fetchone()["sha256"]
        latest = connection.execute(
            "SELECT version, name, checksum FROM systematic_fx.schema_migrations "
            "ORDER BY version DESC LIMIT 1"
        ).fetchone()
    assert terminal_counts == {
        "attempts": 12,
        "finalists": 4,
        "rejects": 8,
        "exact_pairs": 12,
    }
    assert link_counts == {"links": 144, "global_identities": 1}
    assert predecessor_snapshot_after == predecessor_snapshot
    assert latest["version"] == 27
    assert latest["name"] == "bar_state_v2b_parquet_schema_amendment"
    assert latest["checksum"] == EXPECTED_MIGRATION_0027_SHA256


def test_bar_state_v2b_postgresql_release_gate(tmp_path: Path) -> None:
    database_url = _disposable_v2b_database_url()
    migrations = discover_migrations(ROOT / "migrations")
    assert tuple(item.version for item in migrations) == tuple(range(1, 28))
    first = apply_migrations(
        database_url,
        directory=ROOT / "migrations",
        psql_binary=os.environ.get("SYSTEMATIC_FX_PSQL"),
    )
    assert first.applied == tuple(range(1, 28))
    repeated = apply_migrations(
        database_url,
        directory=ROOT / "migrations",
        psql_binary=os.environ.get("SYSTEMATIC_FX_PSQL"),
    )
    assert repeated.applied == ()
    assert repeated.skipped == tuple(range(1, 28))
    _seed_dataset(
        database_url,
        tmp_path,
        manifest_sha256=BAR_STATE_RAW_SOURCE_MANIFEST_SHA256,
    )

    v2, _v2_provenance, v2_specs, _ = _register_gate_campaign(
        database_url,
        tmp_path,
        profile=BAR_STATE_V2_PROFILE,
        code_commit=BAR_STATE_V2A_PROFILE.predecessor_code_commit or "",
    )
    for candidate_key, spec in zip(v2.candidate_keys, v2_specs, strict=True):
        reservation = reserve_run_attempt(database_url, run_fingerprint=spec.fingerprint)
        start_run_attempt(
            database_url,
            research_run_attempt_id=reservation.research_run_attempt_id,
        )
        abort_bar_state_run_attempt(
            database_url,
            research_run_attempt_id=reservation.research_run_attempt_id,
            candidate_key=candidate_key,
            run_fingerprint=spec.fingerprint,
            error_message="frozen V2 optimizer cap exhausted",
            profile=BAR_STATE_V2_PROFILE,
        )

    v2a, v2a_provenance, v2a_specs, v2a_registrations = _register_gate_campaign(
        database_url,
        tmp_path,
        profile=BAR_STATE_V2A_PROFILE,
        code_commit=BAR_STATE_V2B_PROFILE.predecessor_code_commit or "",
    )
    for candidate_key, spec in zip(v2a.candidate_keys, v2a_specs, strict=True):
        reservation = reserve_run_attempt(database_url, run_fingerprint=spec.fingerprint)
        start_run_attempt(
            database_url,
            research_run_attempt_id=reservation.research_run_attempt_id,
        )
        abort_bar_state_run_attempt(
            database_url,
            research_run_attempt_id=reservation.research_run_attempt_id,
            candidate_key=candidate_key,
            run_fingerprint=spec.fingerprint,
            error_message="V2A FEATURE evidence publication failed, first execution",
            profile=BAR_STATE_V2A_PROFILE,
        )
    with pytest.raises(BarStateRegistryDriftError, match="failed attempts.*drifted"):
        require_clean_bar_state_predecessor(
            database_url,
            successor_profile=BAR_STATE_V2B_PROFILE,
        )
    with psycopg.connect(database_url) as connection:
        assert (
            connection.execute(
                "SELECT systematic_fx.bar_state_v2b_predecessor_is_clean()"
            ).fetchone()[0]
            is False
        )

    for candidate_key, spec in zip(v2a.candidate_keys, v2a_specs, strict=True):
        reservation = reserve_run_attempt(database_url, run_fingerprint=spec.fingerprint)
        assert reservation.attempt_number == 2
        start_run_attempt(
            database_url,
            research_run_attempt_id=reservation.research_run_attempt_id,
        )
        abort_bar_state_run_attempt(
            database_url,
            research_run_attempt_id=reservation.research_run_attempt_id,
            candidate_key=candidate_key,
            run_fingerprint=spec.fingerprint,
            error_message="V2A FEATURE evidence publication failed, exact retry",
            profile=BAR_STATE_V2A_PROFILE,
        )
    predecessor_report = require_clean_bar_state_predecessor(
        database_url,
        successor_profile=BAR_STATE_V2B_PROFILE,
    )
    assert predecessor_report.candidate_count == 12
    assert predecessor_report.failed_attempt_count == 24

    v2a_artifact_prepared = replace(
        v2a,
        project_root=tmp_path,
        data_root=tmp_path / "data",
    )
    v2a_code_artifact = _publish_code_snapshot(
        v2a_artifact_prepared,
        v2a_provenance,
        v2a_specs,
    )
    with psycopg.connect(database_url) as connection:
        connection.execute("SET LOCAL session_replication_role = replica")
        connection.execute(
            "UPDATE systematic_fx.campaigns "
            "SET selected_start_date = DATE '2022-01-04' "
            "WHERE campaign_key = %s",
            (BAR_STATE_V2A_PROFILE.campaign_key,),
        )
        assert (
            connection.execute(
                "SELECT systematic_fx.bar_state_v2b_predecessor_is_clean()"
            ).fetchone()[0]
            is False
        )
        connection.rollback()

        connection.execute("SET LOCAL session_replication_role = replica")
        connection.execute(
            "UPDATE systematic_fx.experiments SET tick_size = 0.00006 WHERE experiment_key = %s",
            (BAR_STATE_V2A_PROFILE.experiment_key,),
        )
        assert (
            connection.execute(
                "SELECT systematic_fx.bar_state_v2b_predecessor_is_clean()"
            ).fetchone()[0]
            is False
        )
        connection.rollback()

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        assert (
            connection.execute(
                "SELECT systematic_fx.bar_state_v2b_predecessor_is_clean() AS is_clean"
            ).fetchone()["is_clean"]
            is True
        )
        v2a_code_artifact_id = connection.execute(
            "SELECT artifact_id FROM systematic_fx.artifacts WHERE artifact_key = %s",
            (v2a_code_artifact.descriptor.artifact_key,),
        ).fetchone()["artifact_id"]
        v2a_snapshot = connection.execute(
            """
            SELECT systematic_fx.canonical_jsonb_sha256(jsonb_build_object(
                'attempts', (
                    SELECT jsonb_agg(to_jsonb(attempt) ORDER BY attempt.research_run_attempt_id)
                    FROM systematic_fx.research_run_attempts AS attempt
                    JOIN systematic_fx.research_run_specs AS run_spec
                      ON run_spec.research_run_spec_id = attempt.research_run_spec_id
                    WHERE run_spec.campaign_id = campaign.campaign_id
                ),
                'artifacts', (
                    SELECT jsonb_agg(to_jsonb(artifact) ORDER BY artifact.artifact_id)
                    FROM systematic_fx.artifacts AS artifact
                    WHERE artifact.artifact_type = %s
                ),
                'specs', (
                    SELECT jsonb_agg(to_jsonb(run_spec) ORDER BY run_spec.research_run_spec_id)
                    FROM systematic_fx.research_run_specs AS run_spec
                    WHERE run_spec.campaign_id = campaign.campaign_id
                ),
                'trials', (
                    SELECT jsonb_agg(to_jsonb(trial) ORDER BY trial.experiment_trial_id)
                    FROM systematic_fx.experiment_trials AS trial
                    JOIN systematic_fx.experiments AS experiment
                      ON experiment.experiment_id = trial.experiment_id
                    WHERE experiment.campaign_id = campaign.campaign_id
                )
            )) AS sha256
            FROM systematic_fx.campaigns AS campaign
            WHERE campaign.campaign_key = %s
            """,
            (BAR_STATE_V2A_PROFILE.artifact_type, BAR_STATE_V2A_PROFILE.campaign_key),
        ).fetchone()["sha256"]

    v2b = load_prepared_bar_state_run(ROOT, profile=BAR_STATE_V2B_PROFILE)
    v2b_provenance = _provenance(database_url, code_commit="4" * 40)
    v2b_specs = build_bar_state_run_specs(v2b, v2b_provenance)
    with (
        pytest.raises(psycopg.errors.RaiseException, match="campaign identity is not exact"),
        psycopg.connect(database_url) as connection,
        connection.transaction(),
    ):
        connection.execute(
            """
            INSERT INTO systematic_fx.campaigns
                (campaign_key, dataset_id, name, status, selected_start_date,
                 selected_end_date, roll_cutoff_date, data_manifest_sha256,
                 feature_version, outcome_version, cost_model_version,
                 execution_model_version, code_commit, config_sha256,
                 split_policy, trial_budget, finalist_budget, frozen_at)
            SELECT %s, dataset_id, 'forged V2B', status, selected_start_date,
                   selected_end_date, roll_cutoff_date, data_manifest_sha256,
                   feature_version, outcome_version, cost_model_version,
                   execution_model_version, %s, %s, split_policy,
                   trial_budget, finalist_budget, statement_timestamp()
            FROM systematic_fx.campaigns
            WHERE campaign_key = %s
            """,
            (
                BAR_STATE_V2B_PROFILE.campaign_key,
                v2b_provenance.code_commit,
                BAR_STATE_V2B_PROFILE.campaign_definition_sha256,
                BAR_STATE_V2A_PROFILE.campaign_key,
            ),
        )

    successor_connection = psycopg.connect(database_url)
    successor_connection.execute(
        """
        INSERT INTO systematic_fx.campaigns
            (campaign_key, dataset_id, name, status, selected_start_date,
             selected_end_date, roll_cutoff_date, data_manifest_sha256,
             feature_version, outcome_version, cost_model_version,
             execution_model_version, code_commit, config_sha256,
             split_policy, trial_budget, finalist_budget, frozen_at)
        SELECT %s, dataset_id, %s, status, selected_start_date,
               selected_end_date, roll_cutoff_date, data_manifest_sha256,
               feature_version, outcome_version, cost_model_version,
               execution_model_version, %s, %s, split_policy,
               trial_budget, finalist_budget, statement_timestamp()
        FROM systematic_fx.campaigns
        WHERE campaign_key = %s
        """,
        (
            BAR_STATE_V2B_PROFILE.campaign_key,
            BAR_STATE_V2B_PROFILE.campaign_name,
            v2b_provenance.code_commit,
            BAR_STATE_V2B_PROFILE.campaign_definition_sha256,
            BAR_STATE_V2A_PROFILE.campaign_key,
        ),
    )
    concurrent_started = threading.Event()

    def insert_late_v2a_attempt() -> None:
        with psycopg.connect(database_url) as connection:
            concurrent_started.set()
            connection.execute(
                """
                INSERT INTO systematic_fx.research_run_attempts
                    (research_run_spec_id, attempt_number, status)
                VALUES (%s, 3, 'QUEUED')
                """,
                (v2a_registrations[v2a.candidate_keys[0]].research_run_spec_id,),
            )

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(insert_late_v2a_attempt)
            assert concurrent_started.wait(timeout=5)
            with pytest.raises(FutureTimeoutError):
                future.result(timeout=0.25)
            successor_connection.commit()
            with pytest.raises(
                psycopg.errors.RaiseException,
                match="V2A predecessor attempt lifecycle is frozen",
            ):
                future.result(timeout=5)
    finally:
        successor_connection.close()

    prepared, provenance, specs, _ = _register_gate_campaign(
        database_url,
        tmp_path,
        profile=BAR_STATE_V2B_PROFILE,
        code_commit=v2b_provenance.code_commit,
    )
    assert tuple(item.fingerprint for item in specs) == tuple(
        item.fingerprint for item in v2b_specs
    )
    idempotent_code = register_published_bar_artifact(
        database_url,
        tmp_path,
        v2a_code_artifact,
    )
    assert idempotent_code.created is False

    with (
        pytest.raises(psycopg.errors.RaiseException, match="artifacts are frozen"),
        psycopg.connect(database_url) as connection,
        connection.transaction(),
    ):
        connection.execute(
            """
            INSERT INTO systematic_fx.artifacts
                (artifact_key, artifact_type, uri, sha256, byte_size,
                 media_type, producer_job_id, metadata)
            SELECT artifact_key || ':late', artifact_type, uri || ':late', sha256,
                   byte_size, media_type, producer_job_id, metadata
            FROM systematic_fx.artifacts
            WHERE artifact_id = %s
            """,
            (v2a_code_artifact_id,),
        )
    with (
        pytest.raises(psycopg.errors.RaiseException, match="one exact frozen experiment"),
        psycopg.connect(database_url) as connection,
        connection.transaction(),
    ):
        connection.execute(
            """
            INSERT INTO systematic_fx.experiments
                (experiment_key, campaign_id, pattern_id, parent_experiment_id,
                 primary_family, status, hypothesis, direction, model_family,
                 tick_size, tick_value, feature_definition_versions,
                 search_boundary, cost_assumptions, execution_assumptions,
                 trial_budget, trials_registered, registration_artifact_id,
                 code_commit, config_sha256, frozen_at)
            SELECT experiment_key || ':extra', campaign_id, pattern_id,
                   parent_experiment_id, primary_family, status, hypothesis,
                   direction, model_family, tick_size, tick_value,
                   feature_definition_versions, search_boundary,
                   cost_assumptions, execution_assumptions, trial_budget,
                   trials_registered, registration_artifact_id, code_commit,
                   config_sha256, statement_timestamp()
            FROM systematic_fx.experiments
            WHERE experiment_key = %s
            """,
            (BAR_STATE_V2B_PROFILE.experiment_key,),
        )

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        coexistence = connection.execute(
            """
            WITH governed AS (
                SELECT trial.experiment_trial_id, trial.trial_key,
                       trial.research_run_spec_id, run_spec.run_fingerprint
                FROM systematic_fx.experiment_trials AS trial
                JOIN systematic_fx.experiments AS experiment
                  ON experiment.experiment_id = trial.experiment_id
                JOIN systematic_fx.campaigns AS campaign
                  ON campaign.campaign_id = experiment.campaign_id
                JOIN systematic_fx.research_run_specs AS run_spec
                  ON run_spec.research_run_spec_id = trial.research_run_spec_id
                WHERE campaign.campaign_key IN (%s, %s, %s)
            )
            SELECT count(*) AS trial_count,
                   count(DISTINCT research_run_spec_id) AS spec_count,
                   count(DISTINCT run_fingerprint) AS fingerprint_count,
                   count(*) FILTER (
                       WHERE systematic_fx.bar_state_run_spec_matches_trial(
                           research_run_spec_id, experiment_trial_id
                       )
                   ) AS exact_match_count,
                   (
                       SELECT count(*)
                       FROM (
                           SELECT trial_key
                           FROM governed
                           GROUP BY trial_key
                           HAVING count(*) = 3
                       ) AS shared_keys
                   ) AS shared_candidate_count
            FROM governed
            """,
            (
                BAR_STATE_V2_PROFILE.campaign_key,
                BAR_STATE_V2A_PROFILE.campaign_key,
                BAR_STATE_V2B_PROFILE.campaign_key,
            ),
        ).fetchone()
        trigger_rows = connection.execute(
            """
                SELECT trigger.tgname, trigger.tgtype, count(*)
                FROM pg_catalog.pg_trigger AS trigger
                JOIN pg_catalog.pg_class AS relation ON relation.oid = trigger.tgrelid
                JOIN pg_catalog.pg_namespace AS namespace
                  ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = 'systematic_fx'
                  AND trigger.tgname IN (
                      'aa_artifacts_freeze_bar_state_predecessor',
                      'aa_bar_state_artifact_links_freeze_predecessor',
                      'aa_experiment_trials_freeze_bar_state_predecessor',
                      'aa_experiments_freeze_bar_state_predecessor',
                      'aa_research_run_attempts_freeze_bar_state_v2a_predecessor',
                      'bar_state_artifact_links_enforce_v2b_feature_schema',
                      'bar_state_artifact_links_publication_refresh',
                      'campaigns_require_bar_state_v2b_predecessor',
                      'research_run_specs_freeze_bar_state_v2_predecessor'
                  )
                  AND NOT trigger.tgisinternal
                GROUP BY trigger.tgname, trigger.tgtype
                """
        ).fetchall()
        trigger_contracts = {row["tgname"]: (row["tgtype"], row["count"]) for row in trigger_rows}
    assert coexistence == {
        "trial_count": 36,
        "spec_count": 36,
        "fingerprint_count": 36,
        "exact_match_count": 36,
        "shared_candidate_count": 12,
    }
    assert trigger_contracts == {
        "aa_artifacts_freeze_bar_state_predecessor": (7, 1),
        "aa_bar_state_artifact_links_freeze_predecessor": (7, 1),
        "aa_experiment_trials_freeze_bar_state_predecessor": (31, 1),
        "aa_experiments_freeze_bar_state_predecessor": (23, 1),
        "aa_research_run_attempts_freeze_bar_state_v2a_predecessor": (31, 1),
        "bar_state_artifact_links_enforce_v2b_feature_schema": (7, 1),
        "bar_state_artifact_links_publication_refresh": (28, 1),
        "campaigns_require_bar_state_v2b_predecessor": (7, 1),
        "research_run_specs_freeze_bar_state_v2_predecessor": (7, 1),
    }

    def pre_link_negatives(
        reservations: Mapping[str, RunAttemptReservation],
    ) -> None:
        first_attempt_id = reservations[prepared.candidate_keys[0]].research_run_attempt_id
        with (
            pytest.raises(psycopg.errors.RaiseException, match="bytes or lineage drifted"),
            psycopg.connect(database_url) as connection,
            connection.transaction(),
        ):
            connection.execute(
                _insert_link_sql(),
                _link_parameters(
                    v2a_code_artifact,
                    artifact_id=v2a_code_artifact_id,
                    artifact_role="FEATURE",
                    shard_ordinal=0,
                    attempt_id=first_attempt_id,
                ),
            )
        wrong_feature = publish_bar_state_parquet(
            tmp_path,
            kind="FEATURE",
            artifact_key_suffix="gate-wrong-v2b-feature-schema",
            table=pa.table({"gate_value": pa.array([1], type=pa.int64())}),
            lineage=_shared_lineage(prepared, provenance, specs),
            logical_identity={"shard_ordinal": 0, "split_key": "discovery"},
            profile=BAR_STATE_V2B_PROFILE,
        )
        wrong_feature_id = _register_artifact(database_url, tmp_path, wrong_feature)
        with (
            pytest.raises(
                psycopg.errors.RaiseException,
                match="exact round-trip Arrow schema",
            ),
            psycopg.connect(database_url) as connection,
            connection.transaction(),
        ):
            connection.execute(
                _insert_link_sql(),
                _link_parameters(
                    wrong_feature,
                    artifact_id=wrong_feature_id,
                    artifact_role="FEATURE",
                    shard_ordinal=0,
                    attempt_id=first_attempt_id,
                ),
            )

    terminal_counts, link_counts = _complete_gate_campaign_lifecycle(
        database_url,
        tmp_path,
        prepared=prepared,
        provenance=provenance,
        specs=specs,
        profile=BAR_STATE_V2B_PROFILE,
        pre_link_probe=pre_link_negatives,
    )
    assert terminal_counts == {
        "attempts": 12,
        "finalists": 4,
        "rejects": 8,
        "exact_pairs": 12,
    }
    assert link_counts == {"links": 144, "global_identities": 1}

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        v2a_snapshot_after = connection.execute(
            """
            SELECT systematic_fx.canonical_jsonb_sha256(jsonb_build_object(
                'attempts', (
                    SELECT jsonb_agg(to_jsonb(attempt) ORDER BY attempt.research_run_attempt_id)
                    FROM systematic_fx.research_run_attempts AS attempt
                    JOIN systematic_fx.research_run_specs AS run_spec
                      ON run_spec.research_run_spec_id = attempt.research_run_spec_id
                    WHERE run_spec.campaign_id = campaign.campaign_id
                ),
                'artifacts', (
                    SELECT jsonb_agg(to_jsonb(artifact) ORDER BY artifact.artifact_id)
                    FROM systematic_fx.artifacts AS artifact
                    WHERE artifact.artifact_type = %s
                ),
                'specs', (
                    SELECT jsonb_agg(to_jsonb(run_spec) ORDER BY run_spec.research_run_spec_id)
                    FROM systematic_fx.research_run_specs AS run_spec
                    WHERE run_spec.campaign_id = campaign.campaign_id
                ),
                'trials', (
                    SELECT jsonb_agg(to_jsonb(trial) ORDER BY trial.experiment_trial_id)
                    FROM systematic_fx.experiment_trials AS trial
                    JOIN systematic_fx.experiments AS experiment
                      ON experiment.experiment_id = trial.experiment_id
                    WHERE experiment.campaign_id = campaign.campaign_id
                )
            )) AS sha256
            FROM systematic_fx.campaigns AS campaign
            WHERE campaign.campaign_key = %s
            """,
            (BAR_STATE_V2A_PROFILE.artifact_type, BAR_STATE_V2A_PROFILE.campaign_key),
        ).fetchone()["sha256"]
        latest = connection.execute(
            "SELECT version, name, checksum FROM systematic_fx.schema_migrations "
            "ORDER BY version DESC LIMIT 1"
        ).fetchone()
    assert v2a_snapshot_after == v2a_snapshot
    assert latest == {
        "version": 27,
        "name": "bar_state_v2b_parquet_schema_amendment",
        "checksum": EXPECTED_MIGRATION_0027_SHA256,
    }
