from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Callable, Mapping
from copy import deepcopy
from fractions import Fraction
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from systematic_fx.config.settings import Settings
from systematic_fx.db.m0b_registry import register_m0b_candidate
from systematic_fx.db.migrations import apply_migrations
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.m0b.first_passage_store import (
    FirstPassageShard,
    FirstPassageStore,
)
from systematic_fx.research.m0b.worker import (
    CandidateWorkArtifact,
    CandidateWorkSpec,
    NumericAdmissionRules,
    VolatilityBarrierSpec,
    load_candidate_work_artifact,
    publish_candidate_work_manifest,
    publish_signal_artifact,
)
from systematic_fx.research.run_spec import RunSpec


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _default_work_barrier() -> VolatilityBarrierSpec:
    return VolatilityBarrierSpec(
        barrier_id="tp1of1_sl3of4_h3600",
        k_tp_num=1,
        k_tp_den=1,
        k_sl_num=3,
        k_sl_den=4,
        max_hold_seconds=3600,
    )


def _default_evaluation_policy() -> dict[str, object]:
    return {
        "artifact_schema": "systematic_fx.m0b_worker_evaluation_policy.v1",
        "checkpoint_shard_interval": 1,
        "cooldown_ns": 0,
        "max_signals": 3,
        "max_trades": 3,
        "search_fold_count": 1,
        "stress_extra_cost_ticks": 1,
    }


def _control_label_payload(feature_sha256: str) -> bytes:
    rows = []
    for event_ts_ns, session_id, barrier in (
        (100, "D1", _default_work_barrier()),
        (
            200,
            "D2",
            VolatilityBarrierSpec(
                barrier_id="tp3of4_sl1of2_h1800",
                k_tp_num=3,
                k_tp_den=4,
                k_sl_num=1,
                k_sl_den=2,
                max_hold_seconds=1800,
            ),
        ),
    ):
        rows.append(
            {
                "artifact_schema": "systematic_fx.m0b_quote_label.v1",
                "barrier_id": barrier.barrier_id,
                "direction": "LONG",
                "event_ts_ns": event_ts_ns,
                "instrument_id": 1,
                "k_sl_den": barrier.k_sl_den,
                "k_sl_num": barrier.k_sl_num,
                "k_tp_den": barrier.k_tp_den,
                "k_tp_num": barrier.k_tp_num,
                "label_version": "m0b_quote_labels_v1",
                "max_hold_seconds": barrier.max_hold_seconds,
                "parent_feature_manifest_sha256": feature_sha256,
                "session_id": session_id,
            }
        )
    return b"".join(canonical_json_bytes(row) + b"\n" for row in rows)


def _publish_control_bytes(path: Path, payload: bytes) -> None:
    if path.exists() or path.is_symlink():
        assert path.is_file() and not path.is_symlink()
        assert path.read_bytes() == payload
        assert not path.stat().st_mode & 0o222
        return
    path.write_bytes(payload)
    path.chmod(0o444)


def _candidate_work_artifact(
    fixture: Mapping[str, object],
    *,
    artifact_root: Path,
    candidate_sha256: str,
    candidate_kind: str,
    direction: str,
    seed: int,
    barrier: VolatilityBarrierSpec | None = None,
) -> CandidateWorkArtifact:
    """Build detached canonical work bytes for registration-only PG gates."""

    identity = fixture["identity"]
    assert isinstance(identity, Mapping)
    root = artifact_root
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    label_payload = _control_label_payload(str(identity["feature_sha256"]))
    label_sha256 = hashlib.sha256(label_payload).hexdigest()
    assert label_sha256 == identity["label_sha256"]
    shard_sha256 = label_sha256
    shard_uri = f"first-passage-shard-000001-{shard_sha256}.jsonl"
    _publish_control_bytes(root / shard_uri, label_payload)
    shard = FirstPassageShard(
        ordinal=1,
        row_count=2,
        byte_size=len(label_payload),
        content_sha256=shard_sha256,
        relative_uri=shard_uri,
        first_event_key=(100, 1, "D1"),
        last_event_key=(200, 1, "D2"),
    )
    store = FirstPassageStore(
        spec_sha256=_digest(f"m0b:control:store-spec:{identity['dataset_sha256']}:{label_sha256}"),
        source_label_sha256=label_sha256,
        source_feature_sha256=str(identity["feature_sha256"]),
        source_build_sha256=str(identity["dataset_sha256"]),
        label_version=str(identity["label_version"]),
        row_count=2,
        shard_row_target=2,
        shards=(shard,),
    )
    store_payload = canonical_json_bytes(store.as_dict())
    _publish_control_bytes(
        root / f"first-passage-store-{store.sha256}.json",
        store_payload,
    )
    signal = publish_signal_artifact(
        root,
        candidate_sha256=candidate_sha256,
        feature_sha256=str(identity["feature_sha256"]),
        rows=[
            {
                "artifact_schema": "systematic_fx.m0b_candidate_signal.v1",
                "candidate_sha256": candidate_sha256,
                "event_ts_ns": 100,
                "feature_sha256": identity["feature_sha256"],
                "instrument_id": 1,
                "search_fold": 0,
                "session_id": "D1",
            }
        ],
        max_signals=3,
        search_fold_count=1,
    )
    rules = NumericAdmissionRules(
        min_raw_events=3,
        min_flat_trades=2,
        min_sequential_trades=2,
        min_active_days=1,
        min_tp_probability_ppm=500_000,
        require_positive_net_ev=True,
        min_positive_search_folds=1,
        max_stressed_cost_ev_floor_ticks=0,
    )
    work = CandidateWorkSpec(
        epoch_sha256=str(fixture["epoch_sha256"]),
        candidate_sha256=candidate_sha256,
        first_passage_store_sha256=store.sha256,
        signals=signal,
        candidate_kind=candidate_kind,
        direction=direction,
        barrier=barrier or _default_work_barrier(),
        cooldown_ns=0,
        stress_extra_cost_ticks=1,
        search_fold_count=1,
        max_signals=3,
        max_trades=3,
        checkpoint_shard_interval=1,
        deterministic_seed=seed,
        code_snapshot_sha256=str(identity["code_snapshot_sha256"]),
        cost_sha256=str(identity["cost_sha256"]),
        execution_sha256=str(identity["execution_sha256"]),
        split_sha256=str(identity["split_sha256"]),
        admission_rules=rules,
    )
    work_uri = publish_candidate_work_manifest(root, work)
    return load_candidate_work_artifact(root / work_uri)


def _insert_artifact(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    key: str,
    artifact_type: str,
    sha256: str,
    byte_size: int,
    metadata: Mapping[str, object],
) -> int:
    row = connection.execute(
        """
        INSERT INTO systematic_fx.artifacts
            (artifact_key, artifact_type, uri, sha256, byte_size, media_type, metadata)
        VALUES (%s, %s, %s, %s, %s, 'application/json', %s)
        RETURNING artifact_id
        """,
        (
            key,
            artifact_type,
            f"m0b-gate://{key}/sha256={sha256}.json",
            sha256,
            byte_size,
            Jsonb(dict(metadata)),
        ),
    ).fetchone()
    assert row is not None
    return int(row["artifact_id"])


def _epoch_document(
    *,
    epoch_key: str,
    identity: Mapping[str, str],
    real_candidate_budget: int = 1,
    null_candidate_budget: int = 2,
) -> dict[str, object]:
    return {
        "artifact_schema": "systematic_fx.m0b_epoch.v1",
        "epoch_key": epoch_key,
        "parent_epoch": None,
        "dataset": {
            "version": identity["dataset_version"],
            "sha256": identity["dataset_sha256"],
        },
        "calendar": {
            "version": identity["calendar_version"],
            "sha256": identity["calendar_sha256"],
        },
        "contract_reference": {
            "version": identity["contract_reference_version"],
            "sha256": identity["contract_reference_sha256"],
        },
        "split": {
            "version": identity["split_version"],
            "sha256": identity["split_sha256"],
        },
        "feature": {
            "version": identity["feature_version"],
            "sha256": identity["feature_sha256"],
        },
        "label": {
            "version": identity["label_version"],
            "sha256": identity["label_sha256"],
        },
        "cost": {
            "version": identity["cost_version"],
            "sha256": identity["cost_sha256"],
        },
        "execution": {
            "version": identity["execution_version"],
            "sha256": identity["execution_sha256"],
        },
        "engine_version": identity["engine_version"],
        "code_commit": identity["code_commit"],
        "code_snapshot_sha256": identity["code_snapshot_sha256"],
        "dependency_lock_sha256": identity["dependency_lock_sha256"],
        "authority": "SEARCH_ONLY_NOT_HOLDOUT_NOT_FORWARD",
        "strategy_families": ["pullback_continuation_v1"],
        "admission_rules": {
            "contract_version": "m0b_numeric_admission_v1",
            "maximum_authority": "REGISTER",
            "min_raw_events": 3,
            "min_flat_trades": 2,
            "min_sequential_trades": 2,
            "min_active_days": 1,
            "min_tp_probability_ppm": 500_000,
            "require_positive_net_ev": True,
            "min_positive_search_folds": 1,
            "max_stressed_cost_ev_floor_ticks": 0,
        },
        "budgets": {
            "real": real_candidate_budget,
            "null": null_candidate_budget,
        },
        "retry": {"max_attempts_per_candidate": 3},
        "search_space": {
            "parameter_ranges": {
                "pullback_length": [2, 3, 4],
                "trend_quantile": ["0.60", "0.70"],
                "volatility_regime": ["MID"],
            },
            "barrier_grid": {
                "k_tp": ["0.75", "1.00"],
                "k_sl": ["0.50", "0.75"],
                "max_hold_minutes": [30, 60],
            },
        },
        "random_seeds": [7, 11],
        "null_controls": ["CIRCULAR_TIME_SHIFT", "MATCHED_RANDOM_ENTRY"],
        "execution_assumptions": {
            "entry_latency": "NEXT_ELIGIBLE_QUOTE_PLUS_ONE_ADVERSE_TICK",
            "passive_tp_fill": "TRADE_THROUGH_ONLY",
            "stop_execution": "MARKETABLE_CONSERVATIVE",
        },
        "session_policy": "NO_CROSS_CLOSED_MARKET",
        "roll_policy": {
            "selection": "PREVIOUS_DAY_VOLUME",
            "hold_same_instrument_until_exit": True,
            "no_entry_inside_roll_guard": True,
        },
    }


def _insert_epoch(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    campaign_id: int,
    manifest_artifact_id: int,
    manifest_sha256: str,
    manifest_byte_size: int,
    epoch_key: str,
    canonical_epoch: Mapping[str, object],
    identity: Mapping[str, str],
    real_candidate_budget: int = 1,
    null_candidate_budget: int = 2,
) -> int:
    row = connection.execute(
        """
        INSERT INTO systematic_fx.m0b_epochs
            (epoch_key, epoch_sha256, canonical_epoch, campaign_id,
             manifest_artifact_id, manifest_artifact_sha256,
             manifest_artifact_byte_size, dataset_version, dataset_sha256,
             calendar_version, calendar_sha256, contract_reference_version,
             contract_reference_sha256, split_version, split_sha256,
             feature_version, feature_sha256, label_version, label_sha256,
             cost_version, cost_sha256, execution_version, execution_sha256,
             engine_version, code_commit, code_snapshot_sha256,
             dependency_lock_sha256, real_candidate_budget, null_candidate_budget,
             max_attempts_per_candidate)
        VALUES
            (%(epoch_key)s, %(epoch_sha256)s, %(canonical_epoch)s, %(campaign_id)s,
             %(manifest_artifact_id)s, %(manifest_sha256)s,
             %(manifest_byte_size)s, %(dataset_version)s, %(dataset_sha256)s,
             %(calendar_version)s, %(calendar_sha256)s,
             %(contract_reference_version)s, %(contract_reference_sha256)s,
             %(split_version)s, %(split_sha256)s, %(feature_version)s,
             %(feature_sha256)s, %(label_version)s, %(label_sha256)s,
             %(cost_version)s, %(cost_sha256)s, %(execution_version)s,
             %(execution_sha256)s, %(engine_version)s, %(code_commit)s,
             %(code_snapshot_sha256)s, %(dependency_lock_sha256)s,
             %(real_candidate_budget)s, %(null_candidate_budget)s, 3)
        RETURNING m0b_epoch_id
        """,
        {
            **dict(identity),
            "epoch_key": epoch_key,
            "epoch_sha256": canonical_sha256(canonical_epoch),
            "canonical_epoch": Jsonb(dict(canonical_epoch)),
            "campaign_id": campaign_id,
            "manifest_artifact_id": manifest_artifact_id,
            "manifest_sha256": manifest_sha256,
            "manifest_byte_size": manifest_byte_size,
            "real_candidate_budget": real_candidate_budget,
            "null_candidate_budget": null_candidate_budget,
        },
    ).fetchone()
    assert row is not None
    return int(row["m0b_epoch_id"])


def _run_spec(
    *,
    campaign_key: str,
    experiment_key: str,
    epoch_sha256: str,
    candidate_sha256: str,
    identity: Mapping[str, str],
    seed: int,
    family: str = "pullback_continuation_v1",
    parent_fingerprint: str | None = None,
    source_manifest_hashes: Mapping[str, str] | None = None,
    direction: str = "LONG",
    work_spec_sha256: str | None = None,
    work_artifact: CandidateWorkArtifact | None = None,
) -> RunSpec:
    resolved_work_sha256 = (
        work_artifact.content_sha256 if work_artifact is not None else work_spec_sha256
    ) or _digest(f"m0b:synthetic-work:{candidate_sha256}:{seed}:{parent_fingerprint}")
    barrier = (
        work_artifact.work.barrier.as_dict()
        if work_artifact is not None
        else _default_work_barrier().as_dict()
    )
    evaluation_policy = (
        work_artifact.work.evaluation_policy
        if work_artifact is not None
        else _default_evaluation_policy()
    )
    parameters = {
        "data_role": "SEARCH",
        "split_role": "DISCOVERY",
        "m0b_epoch_sha256": epoch_sha256,
        "m0b_dataset_sha256": identity["dataset_sha256"],
        "m0b_contract_reference_sha256": identity["contract_reference_sha256"],
        "m0b_candidate_sha256": candidate_sha256,
        "m0b_barrier_sha256": canonical_sha256(barrier),
        "m0b_evaluation_policy_sha256": canonical_sha256(evaluation_policy),
        "m0b_work_spec_sha256": resolved_work_sha256,
    }
    if parent_fingerprint is not None:
        parameters["parent_run_fingerprint"] = parent_fingerprint
    return RunSpec(
        campaign_id=campaign_key,
        experiment_id=experiment_key,
        run_kind="SCREEN",
        engine_version=identity["engine_version"],
        source_manifest_hashes=(
            source_manifest_hashes
            if source_manifest_hashes is not None
            else {"dataset": identity["dataset_sha256"]}
        ),
        eligible_calendar_version=identity["calendar_version"],
        eligible_calendar_sha256=identity["calendar_sha256"],
        split_version=identity["split_version"],
        split_sha256=identity["split_sha256"],
        feature_version=identity["feature_version"],
        feature_sha256=identity["feature_sha256"],
        outcome_version=identity["label_version"],
        outcome_sha256=identity["label_sha256"],
        cost_version=identity["cost_version"],
        cost_sha256=identity["cost_sha256"],
        execution_version=identity["execution_version"],
        execution_sha256=identity["execution_sha256"],
        code_commit=identity["code_commit"],
        code_snapshot_sha256=identity["code_snapshot_sha256"],
        dependency_lock_sha256=identity["dependency_lock_sha256"],
        runtime_environment={"gate": "disposable-postgresql", "version": 1},
        random_seed=seed,
        direction=direction,
        signal_policy={"family": family},
        entry_policy={"latency": "next_eligible_quote_plus_one_adverse_tick"},
        barrier_policy=barrier,
        terminal_policy={"session_policy": "NO_CROSS_CLOSED_MARKET"},
        parameters=parameters,
    )


def _insert_run_spec_direct(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    run_spec: RunSpec,
    campaign_id: int,
    experiment_id: int,
    parent_run_spec_id: int | None = None,
) -> int:
    """Insert a RunSpec in the caller transaction so negative cases roll back cleanly."""

    canonical_spec = json.loads(run_spec.canonical_json())
    row = connection.execute(
        """
        INSERT INTO systematic_fx.research_run_specs
            (run_fingerprint, canonicalization_schema, canonicalization_version,
             campaign_id, experiment_id, parent_run_spec_id, run_kind,
             engine_version, canonical_spec, source_manifest_hashes,
             eligible_calendar_version, eligible_calendar_sha256,
             split_version, split_sha256, feature_version, feature_sha256,
             outcome_version, outcome_sha256, cost_version, cost_sha256,
             execution_version, execution_sha256, code_commit,
             code_snapshot_sha256, dependency_lock_sha256,
             deterministic_seed, direction)
        VALUES (%s, 'systematic_fx.research_run_spec.v2', 2, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s)
        RETURNING research_run_spec_id
        """,
        (
            run_spec.fingerprint,
            campaign_id,
            experiment_id,
            parent_run_spec_id,
            run_spec.run_kind,
            run_spec.engine_version,
            Jsonb(canonical_spec),
            Jsonb(dict(run_spec.source_manifest_hashes)),
            run_spec.eligible_calendar_version,
            run_spec.eligible_calendar_sha256,
            run_spec.split_version,
            run_spec.split_sha256,
            run_spec.feature_version,
            run_spec.feature_sha256,
            run_spec.outcome_version,
            run_spec.outcome_sha256,
            run_spec.cost_version,
            run_spec.cost_sha256,
            run_spec.execution_version,
            run_spec.execution_sha256,
            run_spec.code_commit,
            run_spec.code_snapshot_sha256,
            run_spec.dependency_lock_sha256,
            run_spec.random_seed,
            run_spec.direction,
        ),
    ).fetchone()
    assert row is not None
    return int(row["research_run_spec_id"])


def _insert_candidate(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    epoch_id: int,
    parent_candidate_id: int | None,
    research_run_spec_id: int,
    candidate_kind: str,
    candidate_sha256: str,
    canonical_candidate: Mapping[str, object],
    ordinal: int = 1,
    work_barrier: Mapping[str, object] | None = None,
) -> int:
    binding = connection.execute(
        """
        SELECT epoch.epoch_sha256, epoch.dataset_sha256, epoch.feature_sha256,
               epoch.label_sha256, epoch.code_snapshot_sha256,
               epoch.cost_sha256, epoch.execution_sha256, epoch.split_sha256,
               systematic_fx.canonical_jsonb_sha256(
                   epoch.canonical_epoch -> 'admission_rules') AS rules_sha256,
               run_spec.canonical_spec #>> '{parameters,m0b_work_spec_sha256}'
                   AS work_spec_sha256,
               run_spec.deterministic_seed::text AS deterministic_seed,
               run_spec.direction
          FROM systematic_fx.m0b_epochs epoch
          JOIN systematic_fx.research_run_specs run_spec
            ON run_spec.research_run_spec_id = %s
         WHERE epoch.m0b_epoch_id = %s
        """,
        (research_run_spec_id, epoch_id),
    ).fetchone()
    assert binding is not None
    work_sha256 = str(binding["work_spec_sha256"])
    candidate_barrier = canonical_candidate["barrier"]
    assert isinstance(candidate_barrier, Mapping)
    k_tp = Fraction(str(candidate_barrier["k_tp"]))
    k_sl = Fraction(str(candidate_barrier["k_sl"]))
    max_hold_seconds = int(candidate_barrier["max_hold_minutes"]) * 60
    barrier = (
        dict(work_barrier)
        if work_barrier is not None
        else {
            "artifact_schema": "systematic_fx.m0b_volatility_barrier.v1",
            "barrier_id": (
                f"tp{k_tp.numerator}of{k_tp.denominator}_"
                f"sl{k_sl.numerator}of{k_sl.denominator}_h{max_hold_seconds}"
            ),
            "k_sl_den": k_sl.denominator,
            "k_sl_num": k_sl.numerator,
            "k_tp_den": k_tp.denominator,
            "k_tp_num": k_tp.numerator,
            "max_hold_seconds": max_hold_seconds,
        }
    )
    evaluation_policy = {
        "artifact_schema": "systematic_fx.m0b_worker_evaluation_policy.v1",
        "checkpoint_shard_interval": 1,
        "cooldown_ns": 0,
        "max_signals": 3,
        "max_trades": 3,
        "search_fold_count": 1,
        "stress_extra_cost_ticks": 1,
    }
    work_metadata = {
        "admission_rules_sha256": binding["rules_sha256"],
        "barrier": barrier,
        "barrier_sha256": canonical_sha256(barrier),
        "candidate_kind": candidate_kind,
        "candidate_sha256": candidate_sha256,
        "code_snapshot_sha256": binding["code_snapshot_sha256"],
        "cost_sha256": binding["cost_sha256"],
        "data_role": "SEARCH",
        "deterministic_seed": int(binding["deterministic_seed"]),
        "direction": canonical_candidate.get("direction"),
        "epoch_sha256": binding["epoch_sha256"],
        "evaluation_policy": evaluation_policy,
        "evaluation_policy_sha256": canonical_sha256(evaluation_policy),
        "execution_sha256": binding["execution_sha256"],
        "first_passage_store_sha256": _digest(f"direct:store:{candidate_sha256}"),
        "identity_schema": "systematic_fx.m0b.candidate_work.v2",
        "signal_artifact_sha256": _digest(f"direct:signal:{candidate_sha256}"),
        "source_build_sha256": binding["dataset_sha256"],
        "source_feature_sha256": binding["feature_sha256"],
        "source_label_sha256": binding["label_sha256"],
        "split_sha256": binding["split_sha256"],
        "work_spec_sha256": work_sha256,
    }
    artifact = connection.execute(
        """
        INSERT INTO systematic_fx.artifacts
            (artifact_key, artifact_type, uri, sha256, byte_size, media_type, metadata)
        VALUES (%s, 'M0B_CANDIDATE_WORK', %s, %s, 137, 'application/json', %s)
        RETURNING artifact_id
        """,
        (
            f"m0b-candidate-work:{binding['epoch_sha256']}:{candidate_sha256}:{work_sha256}",
            (
                f"m0b-work://search/{binding['epoch_sha256']}/{candidate_sha256}/"
                f"sha256={work_sha256}.json"
            ),
            work_sha256,
            Jsonb(work_metadata),
        ),
    ).fetchone()
    assert artifact is not None
    row = connection.execute(
        """
        INSERT INTO systematic_fx.m0b_candidates
            (m0b_epoch_id, parent_candidate_id, research_run_spec_id,
             work_artifact_id, candidate_kind, ordinal, candidate_sha256,
             canonical_candidate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING m0b_candidate_id
        """,
        (
            epoch_id,
            parent_candidate_id,
            research_run_spec_id,
            artifact["artifact_id"],
            candidate_kind,
            ordinal,
            candidate_sha256,
            Jsonb(dict(canonical_candidate)),
        ),
    ).fetchone()
    assert row is not None
    return int(row["m0b_candidate_id"])


def _checkpoint_cursor(
    *, candidate_id: int, attempt_id: int, work_spec_sha256: str
) -> dict[str, object]:
    return {
        "artifact_schema": "systematic_fx.m0b_checkpoint.v1",
        "m0b_candidate_id": candidate_id,
        "research_run_attempt_id": attempt_id,
        "checkpoint_sequence": 1,
        "predecessor_sha256": None,
        "state": {
            "accepted_tp_count": 0,
            "active_session_ids": [],
            "complete": False,
            "fold_net_pnl_ticks": [0],
            "fold_trade_counts": [0],
            "ineligible_signal_count": 0,
            "matching_label_count": 0,
            "missing_label_count": 0,
            "next_available_ts_ns": None,
            "next_shard_ordinal": 2,
            "next_signal_index": 0,
            "overlap_signal_count": 0,
            "raw_event_count": 0,
            "raw_net_pnl_ticks": 0,
            "raw_tp_count": 0,
            "result_artifact": None,
            "sequential_net_pnl_ticks": 0,
            "sequential_stressed_net_pnl_ticks": 0,
            "sequential_trade_count": 0,
            "state_schema": "systematic_fx.m0b_worker_state.v1",
            "trade_shards": [
                {
                    "byte_size": 1,
                    "content_sha256": _digest(f"m0b:control:trade:{candidate_id}:{attempt_id}"),
                    "first_store_shard": 1,
                    "last_store_shard": 1,
                    "ordinal": 1,
                    "relative_uri": (
                        "candidate-trades-000001-"
                        f"{_digest(f'm0b:control:trade:{candidate_id}:{attempt_id}')}.json"
                    ),
                    "row_count": 0,
                }
            ],
            "work_spec_sha256": work_spec_sha256,
        },
    }


def _result_metadata(
    fixture: Mapping[str, object],
    *,
    candidate_id: int,
    candidate_sha256: str,
    attempt_id: int,
    result_sha256: str,
) -> dict[str, object]:
    return {
        "identity_schema": "systematic_fx.m0b.result.v1",
        "epoch_sha256": fixture["epoch_sha256"],
        "m0b_epoch_id": fixture["epoch_id"],
        "candidate_sha256": candidate_sha256,
        "m0b_candidate_id": candidate_id,
        "research_run_attempt_id": attempt_id,
        "result_sha256": result_sha256,
        "admission_rules_sha256": fixture["admission_rules_sha256"],
    }


def _expect_rejection(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    label: str,
    operation: Callable[[], object],
    message_fragment: str | None = None,
) -> None:
    with pytest.raises(psycopg.Error) as captured, connection.transaction():
        operation()
    if message_fragment is not None:
        assert message_fragment.lower() in str(captured.value).lower(), (
            f"{label} raised an unexpected error: {captured.value}"
        )


def _create_fixture(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    identity_overrides: Mapping[str, str] | None = None,
    real_candidate_budget: int = 1,
    null_candidate_budget: int = 2,
) -> dict[str, object]:
    identity = {
        "dataset_version": "m0b_real_slice_v1",
        "dataset_sha256": _digest("m0b:dataset"),
        "calendar_version": "cme_6e_calendar_v1",
        "calendar_sha256": _digest("m0b:calendar"),
        "contract_reference_version": "cme_6e_reference_v1",
        "contract_reference_sha256": _digest("m0b:contract-reference"),
        "split_version": "m0b_walk_forward_v1",
        "split_sha256": _digest("m0b:split"),
        "feature_version": "m0b_features_v1",
        "feature_sha256": _digest("m0b:feature"),
        "label_version": "m0b_quote_labels_v1",
        "label_sha256": hashlib.sha256(_control_label_payload(_digest("m0b:feature"))).hexdigest(),
        "cost_version": "m0b_cost_v1",
        "cost_sha256": _digest("m0b:cost"),
        "execution_version": "m0b_conservative_execution_v1",
        "execution_sha256": _digest("m0b:execution"),
        "engine_version": "m0b_pg_gate_v1",
        "code_commit": "a" * 40,
        "code_snapshot_sha256": _digest("m0b:code-snapshot"),
        "dependency_lock_sha256": _digest("m0b:dependency-lock"),
    }
    if identity_overrides is not None:
        unknown = set(identity_overrides) - set(identity)
        if unknown:
            raise AssertionError(f"unknown M0b identity overrides: {sorted(unknown)}")
        identity.update(identity_overrides)
    campaign_key = "m0b_pg_gate_v1"
    experiment_key = "m0b_pg_gate_pullback_v1"
    epoch_key = "m0b-pg-gate-epoch-v1"
    epoch_document = _epoch_document(
        epoch_key=epoch_key,
        identity=identity,
        real_candidate_budget=real_candidate_budget,
        null_candidate_budget=null_candidate_budget,
    )
    epoch_sha256 = canonical_sha256(epoch_document)
    manifest_sha256 = _digest("m0b:epoch-manifest")

    with connection.transaction():
        dataset = connection.execute(
            """
            INSERT INTO systematic_fx.datasets
                (dataset_key, provider, feed, data_schema, root_uri, status,
                 manifest_sha256, metadata)
            VALUES ('m0b_pg_gate_dataset', 'Databento', 'GLBX.MDP3', 'MBP-10',
                    'm0b-gate://search-data', 'READY', %s, %s)
            RETURNING dataset_id
            """,
            (
                identity["dataset_sha256"],
                Jsonb(
                    {
                        "data_role": "SEARCH",
                        "dataset_version": identity["dataset_version"],
                    }
                ),
            ),
        ).fetchone()
        assert dataset is not None
        dataset_id = int(dataset["dataset_id"])
        campaign = connection.execute(
            """
            INSERT INTO systematic_fx.campaigns
                (campaign_key, dataset_id, name, status, selected_start_date,
                 selected_end_date, roll_cutoff_date, data_manifest_sha256,
                 feature_version, outcome_version, cost_model_version,
                 execution_model_version, code_commit, config_sha256, split_policy,
                 trial_budget, finalist_budget, frozen_at)
            VALUES (%s, %s, 'M0b disposable PostgreSQL gate', 'FROZEN',
                    DATE '2022-08-30', DATE '2022-09-02', DATE '2022-09-01',
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, 1,
                    statement_timestamp())
            RETURNING campaign_id
            """,
            (
                campaign_key,
                dataset_id,
                identity["dataset_sha256"],
                identity["feature_version"],
                identity["label_version"],
                identity["cost_version"],
                identity["execution_version"],
                identity["code_commit"],
                _digest("m0b:campaign-config"),
                Jsonb({"data_role": "SEARCH", "policy": identity["split_version"]}),
                real_candidate_budget + null_candidate_budget,
            ),
        ).fetchone()
        assert campaign is not None
        campaign_id = int(campaign["campaign_id"])
        experiment_registration_id = _insert_artifact(
            connection,
            key="m0b-pg-gate-experiment-registration",
            artifact_type="M0B_EXPERIMENT_REGISTRATION",
            sha256=_digest("m0b:experiment-registration"),
            byte_size=127,
            metadata={"identity_schema": "systematic_fx.m0b.experiment_registration.v1"},
        )
        experiment = connection.execute(
            """
            INSERT INTO systematic_fx.experiments
                (experiment_key, campaign_id, primary_family, status, hypothesis,
                 direction, model_family, tick_size, tick_value,
                 feature_definition_versions, search_boundary, cost_assumptions,
                 execution_assumptions, trial_budget, registration_artifact_id,
                 code_commit, config_sha256, frozen_at)
            VALUES (%s, %s, 'pullback_continuation_v1', 'FROZEN',
                    'Point-in-time pullback continuation screen', 'BOTH', 'RULE_BASED',
                    0.00005, 6.25, %s, %s, %s, %s, %s, %s, %s, %s,
                    statement_timestamp())
            RETURNING experiment_id
            """,
            (
                experiment_key,
                campaign_id,
                Jsonb({"feature_version": identity["feature_version"]}),
                Jsonb(
                    {
                        "real_budget": real_candidate_budget,
                        "null_budget": null_candidate_budget,
                    }
                ),
                Jsonb({"model": "conservative"}),
                Jsonb({"passive_tp": "trade_through"}),
                real_candidate_budget + null_candidate_budget,
                experiment_registration_id,
                identity["code_commit"],
                _digest("m0b:experiment-config"),
            ),
        ).fetchone()
        assert experiment is not None
        manifest_artifact_id = _insert_artifact(
            connection,
            key="m0b-pg-gate-epoch-manifest",
            artifact_type="M0B_EPOCH_MANIFEST",
            sha256=manifest_sha256,
            byte_size=211,
            metadata={
                "identity_schema": "systematic_fx.m0b.epoch_manifest.v1",
                "epoch_sha256": epoch_sha256,
            },
        )
        epoch_id = _insert_epoch(
            connection,
            campaign_id=campaign_id,
            manifest_artifact_id=manifest_artifact_id,
            manifest_sha256=manifest_sha256,
            manifest_byte_size=211,
            epoch_key=epoch_key,
            canonical_epoch=epoch_document,
            identity=identity,
            real_candidate_budget=real_candidate_budget,
            null_candidate_budget=null_candidate_budget,
        )
    return {
        "identity": identity,
        "dataset_id": dataset_id,
        "campaign_id": campaign_id,
        "campaign_key": campaign_key,
        "experiment_id": int(experiment["experiment_id"]),
        "experiment_key": experiment_key,
        "epoch_id": epoch_id,
        "epoch_key": epoch_key,
        "epoch_sha256": epoch_sha256,
        "admission_rules_sha256": canonical_sha256(epoch_document["admission_rules"]),
        "manifest_artifact_id": manifest_artifact_id,
    }


def _assert_draft_epoch_rejected(
    connection: psycopg.Connection[dict[str, Any]], fixture: Mapping[str, object]
) -> None:
    identity = fixture["identity"]
    assert isinstance(identity, Mapping)
    draft_key = "m0b-pg-gate-draft"
    document = _epoch_document(epoch_key=draft_key, identity=identity)
    epoch_sha256 = canonical_sha256(document)
    manifest_sha256 = _digest("m0b:draft-manifest")
    with connection.transaction():
        campaign = connection.execute(
            """
            INSERT INTO systematic_fx.campaigns
                (campaign_key, dataset_id, name, status, data_manifest_sha256,
                 feature_version, outcome_version, cost_model_version,
                 execution_model_version, code_commit, config_sha256, split_policy,
                 trial_budget, finalist_budget)
            VALUES ('m0b_pg_gate_draft', %s, 'DRAFT must fail', 'DRAFT', %s, %s,
                    %s, %s, %s, %s, %s, %s, 3, 1)
            RETURNING campaign_id
            """,
            (
                fixture["dataset_id"],
                identity["dataset_sha256"],
                identity["feature_version"],
                identity["label_version"],
                identity["cost_version"],
                identity["execution_version"],
                identity["code_commit"],
                _digest("m0b:draft-config"),
                Jsonb({"data_role": "SEARCH"}),
            ),
        ).fetchone()
        assert campaign is not None
        manifest_id = _insert_artifact(
            connection,
            key="m0b-pg-gate-draft-manifest",
            artifact_type="M0B_EPOCH_MANIFEST",
            sha256=manifest_sha256,
            byte_size=113,
            metadata={
                "identity_schema": "systematic_fx.m0b.epoch_manifest.v1",
                "epoch_sha256": epoch_sha256,
            },
        )

    _expect_rejection(
        connection,
        label="DRAFT campaign epoch",
        operation=lambda: _insert_epoch(
            connection,
            campaign_id=int(campaign["campaign_id"]),
            manifest_artifact_id=manifest_id,
            manifest_sha256=manifest_sha256,
            manifest_byte_size=113,
            epoch_key=draft_key,
            canonical_epoch=document,
            identity=identity,
        ),
        message_fragment="frozen open unrevealed campaign",
    )


def _assert_epoch_search_contract_required(
    connection: psycopg.Connection[dict[str, Any]], fixture: Mapping[str, object]
) -> None:
    identity = fixture["identity"]
    assert isinstance(identity, Mapping)
    required_paths = (
        ("search_space",),
        ("search_space", "parameter_ranges"),
        ("search_space", "barrier_grid"),
        ("random_seeds",),
        ("null_controls",),
        ("execution_assumptions",),
        ("session_policy",),
        ("roll_policy",),
    )

    for sequence, path in enumerate(required_paths, start=1):
        epoch_key = f"m0b-pg-gate-missing-{sequence}"
        document = deepcopy(_epoch_document(epoch_key=epoch_key, identity=identity))
        target: dict[str, object] = document
        for component in path[:-1]:
            nested = target[component]
            assert isinstance(nested, dict)
            target = nested
        del target[path[-1]]
        epoch_sha256 = canonical_sha256(document)
        manifest_sha256 = _digest(f"m0b:missing-contract:{sequence}")
        with connection.transaction():
            campaign = connection.execute(
                """
                INSERT INTO systematic_fx.campaigns
                    (campaign_key, dataset_id, name, status, data_manifest_sha256,
                     feature_version, outcome_version, cost_model_version,
                     execution_model_version, code_commit, config_sha256,
                     split_policy, trial_budget, finalist_budget, frozen_at)
                VALUES (%s, %s, %s, 'FROZEN', %s, %s, %s, %s, %s, %s, %s,
                        %s, 3, 1, statement_timestamp())
                RETURNING campaign_id
                """,
                (
                    f"m0b_pg_gate_missing_contract_{sequence}",
                    fixture["dataset_id"],
                    f"Missing canonical epoch field {'.'.join(path)}",
                    identity["dataset_sha256"],
                    identity["feature_version"],
                    identity["label_version"],
                    identity["cost_version"],
                    identity["execution_version"],
                    identity["code_commit"],
                    _digest(f"m0b:missing-contract-config:{sequence}"),
                    Jsonb({"data_role": "SEARCH", "case": ".".join(path)}),
                ),
            ).fetchone()
            assert campaign is not None
            campaign_id = int(campaign["campaign_id"])

        def insert_invalid_epoch(
            *,
            bound_sequence: int = sequence,
            bound_manifest_sha256: str = manifest_sha256,
            bound_epoch_sha256: str = epoch_sha256,
            bound_epoch_key: str = epoch_key,
            bound_document: Mapping[str, object] = document,
            bound_campaign_id: int = campaign_id,
        ) -> int:
            manifest_id = _insert_artifact(
                connection,
                key=f"m0b-pg-gate-missing-contract-{bound_sequence}",
                artifact_type="M0B_EPOCH_MANIFEST",
                sha256=bound_manifest_sha256,
                byte_size=100 + bound_sequence,
                metadata={
                    "identity_schema": "systematic_fx.m0b.epoch_manifest.v1",
                    "epoch_sha256": bound_epoch_sha256,
                },
            )
            return _insert_epoch(
                connection,
                campaign_id=bound_campaign_id,
                manifest_artifact_id=manifest_id,
                manifest_sha256=bound_manifest_sha256,
                manifest_byte_size=100 + bound_sequence,
                epoch_key=bound_epoch_key,
                canonical_epoch=bound_document,
                identity=identity,
            )

        _expect_rejection(
            connection,
            label=f"missing canonical epoch field {'.'.join(path)}",
            operation=insert_invalid_epoch,
            message_fragment="canonical epoch identity mismatch",
        )


def _assert_epoch_run_spec_bootstrap_lock(database_url: str, fixture: Mapping[str, object]) -> None:
    """Both bootstrap paths must serialize on the exact campaign row."""

    identity = fixture["identity"]
    assert isinstance(identity, Mapping)
    epoch_key = "m0b-pg-gate-bootstrap-lock"
    document = _epoch_document(epoch_key=epoch_key, identity=identity)
    epoch_sha256 = canonical_sha256(document)
    manifest_sha256 = _digest("m0b:bootstrap-lock-manifest")
    candidate_sha256 = _digest("m0b:bootstrap-lock-candidate")

    with psycopg.connect(database_url, row_factory=dict_row) as setup, setup.transaction():
        campaign = setup.execute(
            """
                INSERT INTO systematic_fx.campaigns
                    (campaign_key, dataset_id, name, status, data_manifest_sha256,
                     feature_version, outcome_version, cost_model_version,
                     execution_model_version, code_commit, config_sha256,
                     split_policy, trial_budget, finalist_budget, frozen_at)
                VALUES ('m0b_pg_gate_bootstrap_lock', %s,
                        'Bootstrap namespace lock', 'FROZEN', %s, %s, %s, %s,
                        %s, %s, %s, %s, 3, 1, statement_timestamp())
                RETURNING campaign_id
                """,
            (
                fixture["dataset_id"],
                identity["dataset_sha256"],
                identity["feature_version"],
                identity["label_version"],
                identity["cost_version"],
                identity["execution_version"],
                identity["code_commit"],
                _digest("m0b:bootstrap-lock-config"),
                Jsonb({"data_role": "SEARCH"}),
            ),
        ).fetchone()
        assert campaign is not None
        campaign_id = int(campaign["campaign_id"])
        registration_artifact_id = _insert_artifact(
            setup,
            key="m0b-pg-gate-bootstrap-lock-experiment-registration",
            artifact_type="M0B_EXPERIMENT_REGISTRATION",
            sha256=_digest("m0b:bootstrap-lock-experiment-registration"),
            byte_size=91,
            metadata={"identity_schema": "systematic_fx.m0b.experiment_registration.v1"},
        )
        experiment = setup.execute(
            """
                INSERT INTO systematic_fx.experiments
                    (experiment_key, campaign_id, primary_family, status, hypothesis,
                     direction, model_family, tick_size, tick_value,
                     feature_definition_versions, search_boundary, cost_assumptions,
                     execution_assumptions, trial_budget, registration_artifact_id,
                     code_commit, config_sha256, frozen_at)
                VALUES ('m0b_pg_gate_bootstrap_lock_experiment', %s,
                        'pullback_continuation_v1', 'FROZEN', 'lock test', 'BOTH',
                        'RULE_BASED', 0.00005, 6.25, '{}'::jsonb, '{}'::jsonb,
                        '{}'::jsonb, '{}'::jsonb, 3, %s, %s, %s,
                        statement_timestamp())
                RETURNING experiment_id
                """,
            (
                campaign_id,
                registration_artifact_id,
                identity["code_commit"],
                _digest("m0b:bootstrap-lock-experiment-config"),
            ),
        ).fetchone()
        assert experiment is not None
        experiment_id = int(experiment["experiment_id"])
        manifest_id = _insert_artifact(
            setup,
            key="m0b-pg-gate-bootstrap-lock-manifest",
            artifact_type="M0B_EPOCH_MANIFEST",
            sha256=manifest_sha256,
            byte_size=151,
            metadata={
                "identity_schema": "systematic_fx.m0b.epoch_manifest.v1",
                "epoch_sha256": epoch_sha256,
            },
        )

    run_spec = _run_spec(
        campaign_key="m0b_pg_gate_bootstrap_lock",
        experiment_key="m0b_pg_gate_bootstrap_lock_experiment",
        epoch_sha256=epoch_sha256,
        candidate_sha256=candidate_sha256,
        identity=identity,
        seed=7,
    )
    with (
        psycopg.connect(database_url, row_factory=dict_row) as epoch_connection,
        psycopg.connect(database_url, row_factory=dict_row) as run_connection,
    ):
        with (
            pytest.raises(RuntimeError, match="rollback reverse lock probe"),
            run_connection.transaction(),
        ):
            _insert_run_spec_direct(
                run_connection,
                run_spec=run_spec,
                campaign_id=campaign_id,
                experiment_id=experiment_id,
            )
            with pytest.raises(psycopg.errors.LockNotAvailable), epoch_connection.transaction():
                epoch_connection.execute("SET LOCAL lock_timeout = '100ms'")
                _insert_epoch(
                    epoch_connection,
                    campaign_id=campaign_id,
                    manifest_artifact_id=manifest_id,
                    manifest_sha256=manifest_sha256,
                    manifest_byte_size=151,
                    epoch_key=epoch_key,
                    canonical_epoch=document,
                    identity=identity,
                )
            raise RuntimeError("rollback reverse lock probe")

        epoch_transaction = epoch_connection.transaction()
        epoch_transaction.__enter__()
        try:
            _insert_epoch(
                epoch_connection,
                campaign_id=campaign_id,
                manifest_artifact_id=manifest_id,
                manifest_sha256=manifest_sha256,
                manifest_byte_size=151,
                epoch_key=epoch_key,
                canonical_epoch=document,
                identity=identity,
            )
            with pytest.raises(psycopg.errors.LockNotAvailable), run_connection.transaction():
                run_connection.execute("SET LOCAL lock_timeout = '100ms'")
                _insert_run_spec_direct(
                    run_connection,
                    run_spec=run_spec,
                    campaign_id=campaign_id,
                    experiment_id=experiment_id,
                )
        finally:
            epoch_transaction.__exit__(None, None, None)

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        _expect_rejection(
            connection,
            label="post-bootstrap generic RunSpec",
            operation=lambda: _insert_run_spec_direct(
                connection,
                run_spec=run_spec,
                campaign_id=campaign_id,
                experiment_id=experiment_id,
            ),
            message_fragment="register atomically one-to-one",
        )


def _assert_epoch_dataset_binding_rejected(
    connection: psycopg.Connection[dict[str, Any]], fixture: Mapping[str, object]
) -> None:
    identity = fixture["identity"]
    assert isinstance(identity, Mapping)
    bad_dataset_hash = _digest("m0b:catalog-dataset-drift")
    epoch_key = "m0b-pg-gate-dataset-drift"
    document = _epoch_document(epoch_key=epoch_key, identity=identity)
    epoch_sha256 = canonical_sha256(document)
    manifest_sha256 = _digest("m0b:dataset-drift-manifest")
    with connection.transaction():
        dataset = connection.execute(
            """
            INSERT INTO systematic_fx.datasets
                (dataset_key, provider, feed, data_schema, root_uri, status,
                 manifest_sha256, metadata)
            VALUES ('m0b_pg_gate_dataset_drift', 'Databento', 'GLBX.MDP3',
                    'MBP-10', 'm0b-gate://dataset-drift', 'READY', %s, %s)
            RETURNING dataset_id
            """,
            (
                bad_dataset_hash,
                Jsonb(
                    {
                        "data_role": "SEARCH",
                        "dataset_version": identity["dataset_version"],
                    }
                ),
            ),
        ).fetchone()
        assert dataset is not None
        campaign = connection.execute(
            """
            INSERT INTO systematic_fx.campaigns
                (campaign_key, dataset_id, name, status, data_manifest_sha256,
                 feature_version, outcome_version, cost_model_version,
                 execution_model_version, code_commit, config_sha256,
                 split_policy, trial_budget, finalist_budget, frozen_at)
            VALUES ('m0b_pg_gate_dataset_drift', %s, 'Dataset drift must fail',
                    'FROZEN', %s, %s, %s, %s, %s, %s, %s, %s, 3, 1,
                    statement_timestamp())
            RETURNING campaign_id
            """,
            (
                dataset["dataset_id"],
                identity["dataset_sha256"],
                identity["feature_version"],
                identity["label_version"],
                identity["cost_version"],
                identity["execution_version"],
                identity["code_commit"],
                _digest("m0b:dataset-drift-config"),
                Jsonb({"data_role": "SEARCH"}),
            ),
        ).fetchone()
        assert campaign is not None
        manifest_id = _insert_artifact(
            connection,
            key="m0b-pg-gate-dataset-drift-manifest",
            artifact_type="M0B_EPOCH_MANIFEST",
            sha256=manifest_sha256,
            byte_size=157,
            metadata={
                "identity_schema": "systematic_fx.m0b.epoch_manifest.v1",
                "epoch_sha256": epoch_sha256,
            },
        )
    _expect_rejection(
        connection,
        label="epoch/catalog dataset manifest mismatch",
        operation=lambda: _insert_epoch(
            connection,
            campaign_id=int(campaign["campaign_id"]),
            manifest_artifact_id=manifest_id,
            manifest_sha256=manifest_sha256,
            manifest_byte_size=157,
            epoch_key=epoch_key,
            canonical_epoch=document,
            identity=identity,
        ),
        message_fragment="frozen campaign",
    )


def _start_candidate_attempt(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    candidate_id: int,
    run_spec_id: int,
) -> int:
    connection.execute(
        """
        UPDATE systematic_fx.m0b_candidates
        SET status = 'RUNNING', started_at = statement_timestamp()
        WHERE m0b_candidate_id = %s
        """,
        (candidate_id,),
    )
    attempt = connection.execute(
        """
        INSERT INTO systematic_fx.research_run_attempts
            (research_run_spec_id, attempt_number)
        VALUES (%s, 1)
        RETURNING research_run_attempt_id
        """,
        (run_spec_id,),
    ).fetchone()
    assert attempt is not None
    attempt_id = int(attempt["research_run_attempt_id"])
    connection.execute(
        """
        UPDATE systematic_fx.research_run_attempts
        SET status = 'RUNNING', started_at = statement_timestamp()
        WHERE research_run_attempt_id = %s
        """,
        (attempt_id,),
    )
    work_spec_sha256 = connection.execute(
        """
        SELECT artifact.sha256
          FROM systematic_fx.m0b_candidates candidate
          JOIN systematic_fx.artifacts artifact
            ON artifact.artifact_id = candidate.work_artifact_id
         WHERE candidate.m0b_candidate_id = %s
        """,
        (candidate_id,),
    ).fetchone()
    assert work_spec_sha256 is not None
    cursor = _checkpoint_cursor(
        candidate_id=candidate_id,
        attempt_id=attempt_id,
        work_spec_sha256=str(work_spec_sha256["sha256"]),
    )
    connection.execute(
        """
        INSERT INTO systematic_fx.m0b_checkpoints
            (m0b_candidate_id, research_run_attempt_id, checkpoint_sequence,
             checkpoint_sha256, predecessor_sha256, cursor)
        VALUES (%s, %s, 1, %s, NULL, %s)
        """,
        (candidate_id, attempt_id, canonical_sha256(cursor), Jsonb(cursor)),
    )
    return attempt_id


def _finish_candidate(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    candidate_id: int,
    attempt_id: int,
    result_artifact_id: int,
    result_sha256: str,
    result_byte_size: int,
    terminal_status: str,
    result_summary_mutation: Mapping[str, object] | None = None,
) -> None:
    lineage = connection.execute(
        """
        SELECT epoch.epoch_sha256, candidate.candidate_sha256,
               systematic_fx.canonical_jsonb_sha256(
                   epoch.canonical_epoch -> 'admission_rules')
                   AS admission_rules_sha256
        FROM systematic_fx.m0b_candidates AS candidate
        JOIN systematic_fx.m0b_epochs AS epoch USING (m0b_epoch_id)
        WHERE candidate.m0b_candidate_id = %s
        """,
        (candidate_id,),
    ).fetchone()
    assert lineage is not None
    terminal_metrics: dict[str, object] = {
        "raw_events": 8 if terminal_status == "REGISTERED" else 1,
        "flat_trades": 5 if terminal_status == "REGISTERED" else 1,
        "sequential_trades": 4 if terminal_status == "REGISTERED" else 1,
        "active_days": 2 if terminal_status == "REGISTERED" else 1,
        "tp_probability_ppm": 750_000 if terminal_status == "REGISTERED" else 0,
        "positive_search_folds": 2 if terminal_status == "REGISTERED" else 0,
        "net_pnl_ticks": 12 if terminal_status == "REGISTERED" else -3,
        "stressed_net_pnl_ticks": 4 if terminal_status == "REGISTERED" else -5,
    }
    terminal_metrics_sha256 = canonical_sha256(terminal_metrics)
    connection.execute(
        """
        INSERT INTO systematic_fx.m0b_admission_decisions
            (m0b_candidate_id, research_run_attempt_id, result_artifact_id,
             admission_rules_sha256, metrics, metrics_sha256, classification)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            candidate_id,
            attempt_id,
            result_artifact_id,
            lineage["admission_rules_sha256"],
            Jsonb(terminal_metrics),
            terminal_metrics_sha256,
            terminal_status,
        ),
    )
    result_summary: dict[str, object] = {
        "identity_schema": "systematic_fx.m0b.result_summary.v1",
        "epoch_sha256": lineage["epoch_sha256"],
        "candidate_sha256": lineage["candidate_sha256"],
        "result_artifact_id": result_artifact_id,
        "result_sha256": result_sha256,
        "data_role": "SEARCH",
        "classification": terminal_status,
        "admission_rules_sha256": lineage["admission_rules_sha256"],
        "terminal_metrics_sha256": terminal_metrics_sha256,
    }
    if result_summary_mutation is not None:
        result_summary.update(result_summary_mutation)
    connection.execute(
        """
        UPDATE systematic_fx.research_run_attempts
        SET status = 'SUCCEEDED', result_artifact_id = %s,
            result_summary = %s, finished_at = statement_timestamp()
        WHERE research_run_attempt_id = %s
        """,
        (
            result_artifact_id,
            Jsonb(result_summary),
            attempt_id,
        ),
    )
    connection.execute(
        """
        INSERT INTO systematic_fx.m0b_artifact_links
            (m0b_candidate_id, research_run_attempt_id, artifact_id,
             artifact_role, artifact_sha256, artifact_byte_size)
        VALUES (%s, %s, %s, 'RESULT', %s, %s)
        """,
        (
            candidate_id,
            attempt_id,
            result_artifact_id,
            result_sha256,
            result_byte_size,
        ),
    )
    if terminal_status == "REGISTERED":
        connection.execute(
            """
            UPDATE systematic_fx.m0b_candidates
            SET status = 'REGISTERED', finished_at = statement_timestamp(),
                registered_at = statement_timestamp()
            WHERE m0b_candidate_id = %s
            """,
            (candidate_id,),
        )
    else:
        connection.execute(
            """
            UPDATE systematic_fx.m0b_candidates
            SET status = 'SCREENED_OUT', finished_at = statement_timestamp()
            WHERE m0b_candidate_id = %s
            """,
            (candidate_id,),
        )


def _exercise_lifecycle(database_url: str, *, artifact_root: Path) -> None:
    with psycopg.connect(database_url, autocommit=True, row_factory=dict_row) as connection:
        fixture = _create_fixture(connection)
        _assert_draft_epoch_rejected(connection, fixture)
        _assert_epoch_search_contract_required(connection, fixture)
        _assert_epoch_dataset_binding_rejected(connection, fixture)
        _assert_epoch_run_spec_bootstrap_lock(database_url, fixture)
        identity = fixture["identity"]
        assert isinstance(identity, Mapping)

        real_candidate = {
            "artifact_schema": "systematic_fx.m0b_candidate.v1",
            "candidate_kind": "REAL",
            "family_id": "pullback_continuation_v1",
            "ordinal": 1,
            "random_seed": 7,
            "direction": "LONG",
            "cost": {
                "version": identity["cost_version"],
                "sha256": identity["cost_sha256"],
            },
            "parameters": {
                "pullback_length": 3,
                "trend_quantile": "0.70",
                "volatility_regime": "MID",
            },
            "barrier": {"k_tp": "1.00", "k_sl": "0.75", "max_hold_minutes": 60},
        }
        real_sha256 = canonical_sha256(real_candidate)
        real_work = _candidate_work_artifact(
            fixture,
            artifact_root=artifact_root,
            candidate_sha256=real_sha256,
            candidate_kind="REAL",
            direction="LONG",
            seed=7,
        )
        real_spec = _run_spec(
            campaign_key=str(fixture["campaign_key"]),
            experiment_key=str(fixture["experiment_key"]),
            epoch_sha256=str(fixture["epoch_sha256"]),
            candidate_sha256=real_sha256,
            identity=identity,
            seed=7,
            work_spec_sha256=real_work.content_sha256,
            work_artifact=real_work,
        )
        wrong_barrier = VolatilityBarrierSpec(
            barrier_id="tp3of4_sl1of2_h1800",
            k_tp_num=3,
            k_tp_den=4,
            k_sl_num=1,
            k_sl_den=2,
            max_hold_seconds=1800,
        )
        wrong_barrier_work = _candidate_work_artifact(
            fixture,
            artifact_root=artifact_root,
            candidate_sha256=real_sha256,
            candidate_kind="REAL",
            direction="LONG",
            seed=7,
            barrier=wrong_barrier,
        )
        wrong_barrier_spec = _run_spec(
            campaign_key=str(fixture["campaign_key"]),
            experiment_key=str(fixture["experiment_key"]),
            epoch_sha256=str(fixture["epoch_sha256"]),
            candidate_sha256=real_sha256,
            identity=identity,
            seed=7,
            work_artifact=wrong_barrier_work,
        )

        def insert_mismatched_work_barrier() -> int:
            run_spec_id = _insert_run_spec_direct(
                connection,
                run_spec=wrong_barrier_spec,
                campaign_id=int(fixture["campaign_id"]),
                experiment_id=int(fixture["experiment_id"]),
            )
            return _insert_candidate(
                connection,
                epoch_id=int(fixture["epoch_id"]),
                parent_candidate_id=None,
                research_run_spec_id=run_spec_id,
                candidate_kind="REAL",
                candidate_sha256=real_sha256,
                canonical_candidate=real_candidate,
                work_barrier=wrong_barrier.as_dict(),
            )

        _expect_rejection(
            connection,
            label="CandidateWork cannot switch the registered candidate barrier",
            operation=insert_mismatched_work_barrier,
            message_fragment="CandidateWork artifact identity differs",
        )
        real_registration = register_m0b_candidate(
            database_url,
            epoch_key=str(fixture["epoch_key"]),
            run_spec=real_spec,
            candidate_kind="REAL",
            ordinal=1,
            canonical_candidate=real_candidate,
            work_artifact=real_work,
        )
        repeated_real_registration = register_m0b_candidate(
            database_url,
            epoch_key=str(fixture["epoch_key"]),
            run_spec=real_spec,
            candidate_kind="REAL",
            ordinal=1,
            canonical_candidate=real_candidate,
            work_artifact=real_work,
        )
        assert repeated_real_registration.m0b_candidate_id == real_registration.m0b_candidate_id
        assert (
            repeated_real_registration.research_run_spec_id
            == real_registration.research_run_spec_id
        )
        assert repeated_real_registration.run_fingerprint == real_spec.fingerprint
        assert repeated_real_registration.candidate_sha256 == real_sha256
        assert repeated_real_registration.created is False

        mismatched_seed_spec = _run_spec(
            campaign_key=str(fixture["campaign_key"]),
            experiment_key=str(fixture["experiment_key"]),
            epoch_sha256=str(fixture["epoch_sha256"]),
            candidate_sha256=real_sha256,
            identity=identity,
            seed=999,
        )

        def insert_seed_mismatch() -> int:
            run_spec_id = _insert_run_spec_direct(
                connection,
                run_spec=mismatched_seed_spec,
                campaign_id=int(fixture["campaign_id"]),
                experiment_id=int(fixture["experiment_id"]),
            )
            return _insert_candidate(
                connection,
                epoch_id=int(fixture["epoch_id"]),
                parent_candidate_id=None,
                research_run_spec_id=run_spec_id,
                candidate_kind="REAL",
                candidate_sha256=real_sha256,
                canonical_candidate=real_candidate,
            )

        _expect_rejection(
            connection,
            label="canonical candidate seed mismatch",
            operation=insert_seed_mismatch,
        )

        poisoned_source_spec = _run_spec(
            campaign_key=str(fixture["campaign_key"]),
            experiment_key=str(fixture["experiment_key"]),
            epoch_sha256=str(fixture["epoch_sha256"]),
            candidate_sha256=real_sha256,
            identity=identity,
            seed=7,
            source_manifest_hashes={
                "dataset": identity["dataset_sha256"],
                "sealed_holdout": _digest("m0b:forbidden-holdout-source"),
            },
        )
        _expect_rejection(
            connection,
            label="RunSpec forbids extra or holdout source hashes",
            operation=lambda: _insert_run_spec_direct(
                connection,
                run_spec=poisoned_source_spec,
                campaign_id=int(fixture["campaign_id"]),
                experiment_id=int(fixture["experiment_id"]),
            ),
        )

        out_of_range_candidate = deepcopy(real_candidate)
        out_of_range_candidate["parameters"]["pullback_length"] = 999
        out_of_range_sha256 = canonical_sha256(out_of_range_candidate)
        out_of_range_spec = _run_spec(
            campaign_key=str(fixture["campaign_key"]),
            experiment_key=str(fixture["experiment_key"]),
            epoch_sha256=str(fixture["epoch_sha256"]),
            candidate_sha256=out_of_range_sha256,
            identity=identity,
            seed=7,
        )

        def insert_out_of_range_candidate() -> int:
            run_spec_id = _insert_run_spec_direct(
                connection,
                run_spec=out_of_range_spec,
                campaign_id=int(fixture["campaign_id"]),
                experiment_id=int(fixture["experiment_id"]),
            )
            return _insert_candidate(
                connection,
                epoch_id=int(fixture["epoch_id"]),
                parent_candidate_id=None,
                research_run_spec_id=run_spec_id,
                candidate_kind="REAL",
                candidate_sha256=out_of_range_sha256,
                canonical_candidate=out_of_range_candidate,
            )

        _expect_rejection(
            connection,
            label="candidate parameter outside frozen search space",
            operation=insert_out_of_range_candidate,
        )

        for label, candidate_mutation, seed in (
            (
                "candidate barrier outside frozen grid",
                lambda candidate: candidate["barrier"].update({"k_tp": "9.99"}),
                7,
            ),
            (
                "candidate seed outside frozen random seeds",
                lambda candidate: candidate.update({"random_seed": 999}),
                999,
            ),
        ):
            invalid_candidate = deepcopy(real_candidate)
            candidate_mutation(invalid_candidate)
            invalid_sha256 = canonical_sha256(invalid_candidate)
            invalid_spec = _run_spec(
                campaign_key=str(fixture["campaign_key"]),
                experiment_key=str(fixture["experiment_key"]),
                epoch_sha256=str(fixture["epoch_sha256"]),
                candidate_sha256=invalid_sha256,
                identity=identity,
                seed=seed,
            )

            def insert_invalid_search_value(
                candidate: Mapping[str, object] = invalid_candidate,
                candidate_sha256: str = invalid_sha256,
                run_spec: RunSpec = invalid_spec,
            ) -> int:
                run_spec_id = _insert_run_spec_direct(
                    connection,
                    run_spec=run_spec,
                    campaign_id=int(fixture["campaign_id"]),
                    experiment_id=int(fixture["experiment_id"]),
                )
                return _insert_candidate(
                    connection,
                    epoch_id=int(fixture["epoch_id"]),
                    parent_candidate_id=None,
                    research_run_spec_id=run_spec_id,
                    candidate_kind="REAL",
                    candidate_sha256=candidate_sha256,
                    canonical_candidate=candidate,
                )

            _expect_rejection(
                connection,
                label=label,
                operation=insert_invalid_search_value,
            )

        breakout_candidate = deepcopy(real_candidate)
        breakout_candidate["family_id"] = "breakout_v1"
        breakout_sha256 = canonical_sha256(breakout_candidate)
        breakout_spec = _run_spec(
            campaign_key=str(fixture["campaign_key"]),
            experiment_key=str(fixture["experiment_key"]),
            epoch_sha256=str(fixture["epoch_sha256"]),
            candidate_sha256=breakout_sha256,
            identity=identity,
            seed=7,
            family="breakout_v1",
        )

        def insert_out_of_family_candidate() -> int:
            connection.execute(
                """
                UPDATE systematic_fx.experiments
                SET primary_family = 'breakout_v1'
                WHERE experiment_key = %s
                """,
                (fixture["experiment_key"],),
            )
            run_spec_id = _insert_run_spec_direct(
                connection,
                run_spec=breakout_spec,
                campaign_id=int(fixture["campaign_id"]),
                experiment_id=int(fixture["experiment_id"]),
            )
            return _insert_candidate(
                connection,
                epoch_id=int(fixture["epoch_id"]),
                parent_candidate_id=None,
                research_run_spec_id=run_spec_id,
                candidate_kind="REAL",
                candidate_sha256=breakout_sha256,
                canonical_candidate=breakout_candidate,
            )

        _expect_rejection(
            connection,
            label="family outside frozen epoch search space",
            operation=insert_out_of_family_candidate,
        )

        _expect_rejection(
            connection,
            label="epoch identity mutation during start",
            operation=lambda: connection.execute(
                """
                UPDATE systematic_fx.m0b_epochs
                SET status = 'RUNNING', started_at = statement_timestamp(),
                    cost_version = 'mutated-cost',
                    engine_version = 'mutated-engine',
                    max_attempts_per_candidate = 9
                WHERE m0b_epoch_id = %s
                """,
                (fixture["epoch_id"],),
            ),
            message_fragment="epoch identity is immutable",
        )
        with connection.transaction():
            connection.execute(
                "UPDATE systematic_fx.campaigns SET status = 'RUNNING' WHERE campaign_id = %s",
                (fixture["campaign_id"],),
            )
        _expect_rejection(
            connection,
            label="epoch start before creation",
            operation=lambda: connection.execute(
                """
                UPDATE systematic_fx.m0b_epochs
                SET status = 'RUNNING', started_at = created_at - interval '1 second'
                WHERE m0b_epoch_id = %s
                """,
                (fixture["epoch_id"],),
            ),
        )
        with connection.transaction():
            connection.execute(
                """
                UPDATE systematic_fx.m0b_epochs
                SET status = 'RUNNING', started_at = statement_timestamp()
                WHERE m0b_epoch_id = %s
                """,
                (fixture["epoch_id"],),
            )

        _expect_rejection(
            connection,
            label="nonterminal candidates block epoch terminalization",
            operation=lambda: connection.execute(
                """
                UPDATE systematic_fx.m0b_epochs
                SET status = 'FAILED', finished_at = statement_timestamp(),
                    error_message = 'orphan RunSpec must block this transition'
                WHERE m0b_epoch_id = %s
                """,
                (fixture["epoch_id"],),
            ),
        )
        _expect_rejection(
            connection,
            label="epoch completion cannot rewrite start time",
            operation=lambda: connection.execute(
                """
                UPDATE systematic_fx.m0b_epochs
                SET status = 'FAILED', started_at = started_at + interval '1 second',
                    finished_at = statement_timestamp(), error_message = 'immutable start'
                WHERE m0b_epoch_id = %s
                """,
                (fixture["epoch_id"],),
            ),
        )
        _expect_rejection(
            connection,
            label="epoch finish before start",
            operation=lambda: connection.execute(
                """
                UPDATE systematic_fx.m0b_epochs
                SET status = 'FAILED', finished_at = started_at - interval '1 second',
                    error_message = 'invalid timestamp ordering'
                WHERE m0b_epoch_id = %s
                """,
                (fixture["epoch_id"],),
            ),
        )

        _expect_rejection(
            connection,
            label="forged candidate hash",
            operation=lambda: _insert_candidate(
                connection,
                epoch_id=int(fixture["epoch_id"]),
                parent_candidate_id=None,
                research_run_spec_id=real_registration.research_run_spec_id,
                candidate_kind="REAL",
                candidate_sha256=_digest("m0b:forged-candidate"),
                canonical_candidate=real_candidate,
            ),
            message_fragment="provenance differs",
        )
        real_id = real_registration.m0b_candidate_id

        null_candidate = {
            "artifact_schema": "systematic_fx.m0b_candidate.v1",
            "candidate_kind": "NULL",
            "family_id": "pullback_continuation_v1",
            "control": "CIRCULAR_TIME_SHIFT",
            "ordinal": 1,
            "random_seed": 11,
            "direction": "LONG",
            "cost": {
                "version": identity["cost_version"],
                "sha256": identity["cost_sha256"],
            },
            "parent_candidate_sha256": real_sha256,
            "parameters": {
                "pullback_length": 3,
                "trend_quantile": "0.70",
                "volatility_regime": "MID",
            },
            "barrier": {"k_tp": "1.00", "k_sl": "0.75", "max_hold_minutes": 60},
            "null_control": "CIRCULAR_TIME_SHIFT",
        }
        null_sha256 = canonical_sha256(null_candidate)
        null_work = _candidate_work_artifact(
            fixture,
            artifact_root=artifact_root,
            candidate_sha256=null_sha256,
            candidate_kind="NULL",
            direction="LONG",
            seed=11,
        )
        null_spec = _run_spec(
            campaign_key=str(fixture["campaign_key"]),
            experiment_key=str(fixture["experiment_key"]),
            epoch_sha256=str(fixture["epoch_sha256"]),
            candidate_sha256=null_sha256,
            identity=identity,
            seed=11,
            parent_fingerprint=real_spec.fingerprint,
            work_spec_sha256=null_work.content_sha256,
            work_artifact=null_work,
        )

        invalid_null_candidate = deepcopy(null_candidate)
        invalid_null_candidate["control"] = "UNDECLARED_CONTROL"
        invalid_null_candidate["null_control"] = "UNDECLARED_CONTROL"
        invalid_null_sha256 = canonical_sha256(invalid_null_candidate)
        invalid_null_spec = _run_spec(
            campaign_key=str(fixture["campaign_key"]),
            experiment_key=str(fixture["experiment_key"]),
            epoch_sha256=str(fixture["epoch_sha256"]),
            candidate_sha256=invalid_null_sha256,
            identity=identity,
            seed=11,
            parent_fingerprint=real_spec.fingerprint,
        )

        def insert_undeclared_null_control() -> int:
            run_spec_id = _insert_run_spec_direct(
                connection,
                run_spec=invalid_null_spec,
                campaign_id=int(fixture["campaign_id"]),
                experiment_id=int(fixture["experiment_id"]),
                parent_run_spec_id=real_registration.research_run_spec_id,
            )
            return _insert_candidate(
                connection,
                epoch_id=int(fixture["epoch_id"]),
                parent_candidate_id=real_id,
                research_run_spec_id=run_spec_id,
                candidate_kind="NULL",
                candidate_sha256=invalid_null_sha256,
                canonical_candidate=invalid_null_candidate,
            )

        _expect_rejection(
            connection,
            label="NULL control outside frozen null-controls set",
            operation=insert_undeclared_null_control,
        )
        null_registration = register_m0b_candidate(
            database_url,
            epoch_key=str(fixture["epoch_key"]),
            run_spec=null_spec,
            candidate_kind="NULL",
            ordinal=1,
            canonical_candidate=null_candidate,
            parent_candidate_sha256=real_sha256,
            work_artifact=null_work,
        )
        null_id = null_registration.m0b_candidate_id

        duplicate_null_candidate = deepcopy(null_candidate)
        duplicate_null_candidate["ordinal"] = 2
        duplicate_null_candidate["random_seed"] = 7
        duplicate_null_sha256 = canonical_sha256(duplicate_null_candidate)
        duplicate_null_spec = _run_spec(
            campaign_key=str(fixture["campaign_key"]),
            experiment_key=str(fixture["experiment_key"]),
            epoch_sha256=str(fixture["epoch_sha256"]),
            candidate_sha256=duplicate_null_sha256,
            identity=identity,
            seed=7,
            parent_fingerprint=real_spec.fingerprint,
        )
        matched_null_candidate = deepcopy(duplicate_null_candidate)
        matched_null_candidate["control"] = "MATCHED_RANDOM_ENTRY"
        matched_null_candidate["null_control"] = "MATCHED_RANDOM_ENTRY"
        matched_null_sha256 = canonical_sha256(matched_null_candidate)
        matched_null_work = _candidate_work_artifact(
            fixture,
            artifact_root=artifact_root,
            candidate_sha256=matched_null_sha256,
            candidate_kind="NULL",
            direction="LONG",
            seed=7,
        )
        matched_null_spec = _run_spec(
            campaign_key=str(fixture["campaign_key"]),
            experiment_key=str(fixture["experiment_key"]),
            epoch_sha256=str(fixture["epoch_sha256"]),
            candidate_sha256=matched_null_sha256,
            identity=identity,
            seed=7,
            parent_fingerprint=real_spec.fingerprint,
            work_spec_sha256=matched_null_work.content_sha256,
            work_artifact=matched_null_work,
        )

        with connection.transaction():
            real_attempt_id = _start_candidate_attempt(
                connection,
                candidate_id=real_id,
                run_spec_id=real_registration.research_run_spec_id,
            )
            null_attempt_id = _start_candidate_attempt(
                connection,
                candidate_id=null_id,
                run_spec_id=null_registration.research_run_spec_id,
            )
            forbidden_failure_trade_ledger_id = _insert_artifact(
                connection,
                key="m0b-pg-gate-forbidden-failure-trade-ledger",
                artifact_type="M0B_TRADE_LEDGER",
                sha256=_digest("m0b:forbidden-failure-trade-ledger"),
                byte_size=83,
                metadata={"identity_schema": "systematic_fx.m0b.trade_ledger.v1"},
            )

        _expect_rejection(
            connection,
            label="attempt lifecycle regression",
            operation=lambda: connection.execute(
                """
                UPDATE systematic_fx.research_run_attempts
                SET status = 'QUEUED' WHERE research_run_attempt_id = %s
                """,
                (real_attempt_id,),
            ),
            message_fragment="invalid M0b attempt transition",
        )
        _expect_rejection(
            connection,
            label="FAILED attempt cannot bind unsupported trade ledger",
            operation=lambda: connection.execute(
                """
                UPDATE systematic_fx.research_run_attempts
                SET status = 'FAILED', finished_at = statement_timestamp(),
                    error_message = 'expected failure', trade_ledger_artifact_id = %s
                WHERE research_run_attempt_id = %s
                """,
                (forbidden_failure_trade_ledger_id, real_attempt_id),
            ),
            message_fragment="FAILED attempt shape",
        )
        _expect_rejection(
            connection,
            label="FAILED attempt cannot carry result summary",
            operation=lambda: connection.execute(
                """
                UPDATE systematic_fx.research_run_attempts
                SET status = 'FAILED', finished_at = statement_timestamp(),
                    error_message = 'expected failure', result_summary = '{"fake": true}'
                WHERE research_run_attempt_id = %s
                """,
                (null_attempt_id,),
            ),
            message_fragment="FAILED attempt shape",
        )
        _expect_rejection(
            connection,
            label="candidate failure with active attempt",
            operation=lambda: connection.execute(
                """
                UPDATE systematic_fx.m0b_candidates
                SET status = 'FAILED', finished_at = statement_timestamp(),
                    error_message = 'must roll back while attempt is active'
                WHERE m0b_candidate_id = %s
                """,
                (real_id,),
            ),
            message_fragment="latest failure",
        )
        _expect_rejection(
            connection,
            label="epoch failure with active attempts",
            operation=lambda: connection.execute(
                """
                UPDATE systematic_fx.m0b_epochs
                SET status = 'FAILED', finished_at = statement_timestamp(),
                    error_message = 'must quiesce first'
                WHERE m0b_epoch_id = %s
                """,
                (fixture["epoch_id"],),
            ),
            message_fragment="active or unpaired attempts",
        )
        _expect_rejection(
            connection,
            label="frozen experiment mutation",
            operation=lambda: connection.execute(
                """
                UPDATE systematic_fx.experiments
                SET hypothesis = 'post-registration mutation'
                WHERE experiment_key = %s
                """,
                (fixture["experiment_key"],),
            ),
            message_fragment="frozen experiment identity",
        )

        real_result_sha256 = _digest("m0b:real-result")
        null_result_sha256 = _digest("m0b:null-result")
        with connection.transaction():
            real_result_id = _insert_artifact(
                connection,
                key="m0b-pg-gate-real-result",
                artifact_type="M0B_RESULT",
                sha256=real_result_sha256,
                byte_size=307,
                metadata=_result_metadata(
                    fixture,
                    candidate_id=real_id,
                    candidate_sha256=real_sha256,
                    attempt_id=real_attempt_id,
                    result_sha256=real_result_sha256,
                ),
            )
            null_result_id = _insert_artifact(
                connection,
                key="m0b-pg-gate-null-result",
                artifact_type="M0B_RESULT",
                sha256=null_result_sha256,
                byte_size=281,
                metadata=_result_metadata(
                    fixture,
                    candidate_id=null_id,
                    candidate_sha256=null_sha256,
                    attempt_id=null_attempt_id,
                    result_sha256=null_result_sha256,
                ),
            )

            wrong_result_ids: list[tuple[int, str, int]] = []
            binding_mutations = (
                ("candidate", {"candidate_sha256": null_sha256}),
                ("epoch", {"epoch_sha256": _digest("m0b:wrong-epoch")}),
                ("attempt", {"research_run_attempt_id": null_attempt_id}),
                ("extra", {"sealed_holdout_sha256": _digest("m0b:forbidden-holdout")}),
            )
            for binding, mutation in binding_mutations:
                wrong_sha256 = _digest(f"m0b:wrong-result:{binding}")
                wrong_byte_size = 170 + len(binding)
                metadata = _result_metadata(
                    fixture,
                    candidate_id=real_id,
                    candidate_sha256=real_sha256,
                    attempt_id=real_attempt_id,
                    result_sha256=wrong_sha256,
                )
                metadata.update(mutation)
                wrong_id = _insert_artifact(
                    connection,
                    key=f"m0b-pg-gate-wrong-{binding}-result",
                    artifact_type="M0B_RESULT",
                    sha256=wrong_sha256,
                    byte_size=wrong_byte_size,
                    metadata=metadata,
                )
                wrong_result_ids.append((wrong_id, wrong_sha256, wrong_byte_size))

        _expect_rejection(
            connection,
            label="SUCCEEDED result summary exact lineage binding",
            operation=lambda: _finish_candidate(
                connection,
                candidate_id=real_id,
                attempt_id=real_attempt_id,
                result_artifact_id=real_result_id,
                result_sha256=real_result_sha256,
                result_byte_size=307,
                terminal_status="REGISTERED",
                result_summary_mutation={"candidate_sha256": null_sha256},
            ),
            message_fragment="result summary",
        )

        for wrong_result_id, wrong_result_sha256, wrong_result_byte_size in wrong_result_ids:

            def finish_wrong_result(
                result_id: int = wrong_result_id,
                result_sha256: str = wrong_result_sha256,
                result_byte_size: int = wrong_result_byte_size,
            ) -> None:
                _finish_candidate(
                    connection,
                    candidate_id=real_id,
                    attempt_id=real_attempt_id,
                    result_artifact_id=result_id,
                    result_sha256=result_sha256,
                    result_byte_size=result_byte_size,
                    terminal_status="REGISTERED",
                )

            _expect_rejection(
                connection,
                label="RESULT metadata exact lineage binding",
                operation=finish_wrong_result,
            )

        with connection.transaction():
            detail_sha256 = _digest("m0b:single-use-detail")
            detail_id = _insert_artifact(
                connection,
                key="m0b-pg-gate-single-use-detail",
                artifact_type="M0B_DETAIL",
                sha256=detail_sha256,
                byte_size=61,
                metadata={"identity_schema": "systematic_fx.m0b.detail.v1"},
            )
            connection.execute(
                """
                INSERT INTO systematic_fx.m0b_artifact_links
                    (m0b_candidate_id, research_run_attempt_id, artifact_id,
                     artifact_role, artifact_sha256, artifact_byte_size)
                VALUES (%s, %s, %s, 'DETAIL', %s, 61)
                """,
                (real_id, real_attempt_id, detail_id, detail_sha256),
            )

        _expect_rejection(
            connection,
            label="M0b artifact single-use",
            operation=lambda: connection.execute(
                """
                INSERT INTO systematic_fx.m0b_artifact_links
                    (m0b_candidate_id, research_run_attempt_id, artifact_id,
                     artifact_role, artifact_sha256, artifact_byte_size)
                VALUES (%s, %s, %s, 'DETAIL', %s, 61)
                """,
                (null_id, null_attempt_id, detail_id, detail_sha256),
            ),
        )
        _expect_rejection(
            connection,
            label="NULL candidate cannot REGISTER",
            operation=lambda: _finish_candidate(
                connection,
                candidate_id=null_id,
                attempt_id=null_attempt_id,
                result_artifact_id=null_result_id,
                result_sha256=null_result_sha256,
                result_byte_size=281,
                terminal_status="REGISTERED",
            ),
        )

        with connection.transaction():
            _finish_candidate(
                connection,
                candidate_id=real_id,
                attempt_id=real_attempt_id,
                result_artifact_id=real_result_id,
                result_sha256=real_result_sha256,
                result_byte_size=307,
                terminal_status="REGISTERED",
            )

        _expect_rejection(
            connection,
            label="post-terminal attempt",
            operation=lambda: connection.execute(
                """
                INSERT INTO systematic_fx.research_run_attempts
                    (research_run_spec_id, attempt_number)
                VALUES (%s, 2)
                """,
                (real_registration.research_run_spec_id,),
            ),
            message_fragment="terminal M0b candidates",
        )
        stale_cursor = _checkpoint_cursor(
            candidate_id=real_id,
            attempt_id=real_attempt_id,
            work_spec_sha256=real_work.content_sha256,
        )
        stale_cursor["checkpoint_sequence"] = 2
        stale_cursor["predecessor_sha256"] = canonical_sha256(
            _checkpoint_cursor(
                candidate_id=real_id,
                attempt_id=real_attempt_id,
                work_spec_sha256=real_work.content_sha256,
            )
        )
        _expect_rejection(
            connection,
            label="post-terminal checkpoint",
            operation=lambda: connection.execute(
                """
                INSERT INTO systematic_fx.m0b_checkpoints
                    (m0b_candidate_id, research_run_attempt_id, checkpoint_sequence,
                     checkpoint_sha256, predecessor_sha256, cursor)
                VALUES (%s, %s, 2, %s, %s, %s)
                """,
                (
                    real_id,
                    real_attempt_id,
                    canonical_sha256(stale_cursor),
                    stale_cursor["predecessor_sha256"],
                    Jsonb(stale_cursor),
                ),
            ),
            message_fragment="active unrevealed search attempt",
        )
        _expect_rejection(
            connection,
            label="protected result artifact mutation",
            operation=lambda: connection.execute(
                "UPDATE systematic_fx.artifacts SET byte_size = byte_size + 1 WHERE artifact_id = %s",
                (real_result_id,),
            ),
            message_fragment="governed artifacts are immutable",
        )

        with connection.transaction():
            _finish_candidate(
                connection,
                candidate_id=null_id,
                attempt_id=null_attempt_id,
                result_artifact_id=null_result_id,
                result_sha256=null_result_sha256,
                result_byte_size=281,
                terminal_status="SCREENED_OUT",
            )

        def complete_without_matched_control() -> None:
            duplicate_run_spec_id = _insert_run_spec_direct(
                connection,
                run_spec=duplicate_null_spec,
                campaign_id=int(fixture["campaign_id"]),
                experiment_id=int(fixture["experiment_id"]),
                parent_run_spec_id=real_registration.research_run_spec_id,
            )
            duplicate_candidate_id = _insert_candidate(
                connection,
                epoch_id=int(fixture["epoch_id"]),
                parent_candidate_id=real_id,
                research_run_spec_id=duplicate_run_spec_id,
                candidate_kind="NULL",
                candidate_sha256=duplicate_null_sha256,
                canonical_candidate=duplicate_null_candidate,
                ordinal=2,
            )
            duplicate_attempt_id = _start_candidate_attempt(
                connection,
                candidate_id=duplicate_candidate_id,
                run_spec_id=duplicate_run_spec_id,
            )
            duplicate_result_sha256 = _digest("m0b:duplicate-null-result")
            duplicate_result_id = _insert_artifact(
                connection,
                key="m0b-pg-gate-duplicate-null-result",
                artifact_type="M0B_RESULT",
                sha256=duplicate_result_sha256,
                byte_size=263,
                metadata=_result_metadata(
                    fixture,
                    candidate_id=duplicate_candidate_id,
                    candidate_sha256=duplicate_null_sha256,
                    attempt_id=duplicate_attempt_id,
                    result_sha256=duplicate_result_sha256,
                ),
            )
            _finish_candidate(
                connection,
                candidate_id=duplicate_candidate_id,
                attempt_id=duplicate_attempt_id,
                result_artifact_id=duplicate_result_id,
                result_sha256=duplicate_result_sha256,
                result_byte_size=263,
                terminal_status="SCREENED_OUT",
            )
            connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
            connection.execute(
                """
                UPDATE systematic_fx.m0b_epochs
                SET status = 'COMPLETED', finished_at = statement_timestamp()
                WHERE m0b_epoch_id = %s
                """,
                (fixture["epoch_id"],),
            )

        _expect_rejection(
            connection,
            label="COMPLETED requires both declared null controls",
            operation=complete_without_matched_control,
            message_fragment="null-control coverage",
        )

        matched_null_registration = register_m0b_candidate(
            database_url,
            epoch_key=str(fixture["epoch_key"]),
            run_spec=matched_null_spec,
            candidate_kind="NULL",
            ordinal=2,
            canonical_candidate=matched_null_candidate,
            parent_candidate_sha256=real_sha256,
            work_artifact=matched_null_work,
        )
        matched_null_id = matched_null_registration.m0b_candidate_id
        with connection.transaction():
            matched_null_attempt_id = _start_candidate_attempt(
                connection,
                candidate_id=matched_null_id,
                run_spec_id=matched_null_registration.research_run_spec_id,
            )
            matched_null_result_sha256 = _digest("m0b:matched-null-result")
            matched_null_result_id = _insert_artifact(
                connection,
                key="m0b-pg-gate-matched-null-result",
                artifact_type="M0B_RESULT",
                sha256=matched_null_result_sha256,
                byte_size=269,
                metadata=_result_metadata(
                    fixture,
                    candidate_id=matched_null_id,
                    candidate_sha256=matched_null_sha256,
                    attempt_id=matched_null_attempt_id,
                    result_sha256=matched_null_result_sha256,
                ),
            )
            _finish_candidate(
                connection,
                candidate_id=matched_null_id,
                attempt_id=matched_null_attempt_id,
                result_artifact_id=matched_null_result_id,
                result_sha256=matched_null_result_sha256,
                result_byte_size=269,
                terminal_status="SCREENED_OUT",
            )
        with connection.transaction():
            connection.execute(
                """
                UPDATE systematic_fx.m0b_epochs
                SET status = 'COMPLETED', finished_at = statement_timestamp()
                WHERE m0b_epoch_id = %s
                """,
                (fixture["epoch_id"],),
            )

        second_key = "m0b-pg-gate-second-epoch"
        second_document = _epoch_document(epoch_key=second_key, identity=identity)
        second_epoch_sha256 = canonical_sha256(second_document)
        second_manifest_sha256 = _digest("m0b:second-manifest")
        with connection.transaction():
            second_manifest_id = _insert_artifact(
                connection,
                key="m0b-pg-gate-second-manifest",
                artifact_type="M0B_EPOCH_MANIFEST",
                sha256=second_manifest_sha256,
                byte_size=199,
                metadata={
                    "identity_schema": "systematic_fx.m0b.epoch_manifest.v1",
                    "epoch_sha256": second_epoch_sha256,
                },
            )
        _expect_rejection(
            connection,
            label="second epoch for campaign",
            operation=lambda: _insert_epoch(
                connection,
                campaign_id=int(fixture["campaign_id"]),
                manifest_artifact_id=second_manifest_id,
                manifest_sha256=second_manifest_sha256,
                manifest_byte_size=199,
                epoch_key=second_key,
                canonical_epoch=second_document,
                identity=identity,
            ),
        )

        summary = connection.execute(
            """
            SELECT epoch.status AS epoch_status,
                   count(DISTINCT candidate.m0b_candidate_id)
                       FILTER (WHERE candidate.candidate_kind = 'REAL') AS real_count,
                   count(DISTINCT candidate.m0b_candidate_id)
                       FILTER (WHERE candidate.candidate_kind = 'NULL') AS null_count,
                   count(DISTINCT candidate.m0b_candidate_id)
                       FILTER (WHERE candidate.status = 'REGISTERED') AS registered_count,
                   count(DISTINCT candidate.m0b_candidate_id)
                       FILTER (WHERE candidate.status = 'SCREENED_OUT') AS screened_count,
                   count(DISTINCT checkpoint.m0b_checkpoint_id) AS checkpoint_count,
                   count(DISTINCT link.m0b_artifact_link_id)
                       FILTER (WHERE link.artifact_role = 'RESULT') AS result_link_count
            FROM systematic_fx.m0b_epochs AS epoch
            JOIN systematic_fx.m0b_candidates AS candidate USING (m0b_epoch_id)
            JOIN systematic_fx.m0b_checkpoints AS checkpoint USING (m0b_candidate_id)
            JOIN systematic_fx.m0b_artifact_links AS link USING (m0b_candidate_id)
            WHERE epoch.m0b_epoch_id = %s
            GROUP BY epoch.status
            """,
            (fixture["epoch_id"],),
        ).fetchone()
        assert summary == {
            "epoch_status": "COMPLETED",
            "real_count": 1,
            "null_count": 2,
            "registered_count": 1,
            "screened_count": 2,
            "checkpoint_count": 3,
            "result_link_count": 3,
        }
        attempt_count = connection.execute(
            """
            SELECT count(*)
            FROM systematic_fx.research_run_attempts AS attempt
            JOIN systematic_fx.m0b_candidates AS candidate USING (research_run_spec_id)
            WHERE candidate.m0b_epoch_id = %s AND attempt.status = 'SUCCEEDED'
            """,
            (fixture["epoch_id"],),
        ).fetchone()
        assert attempt_count is not None and attempt_count["count"] == 3
        print(f"M0B POSITIVE lifecycle={dict(summary)} succeeded_attempts=3")
        print(
            "M0B NEGATIVE draft/hash/seed/orphan-attempt/family/state-regression/"
            "failure/terminal/null-coverage/checkpoint/artifact/experiment/"
            "one-epoch=REJECTED"
        )


def _main() -> None:
    settings = Settings.from_env(working_directory=Path.cwd())
    base = conninfo_to_dict(settings.database_url)
    database_name = f"systematic_fx_m0b_gate_{os.getpid()}"
    admin_url = make_conninfo(**{**base, "dbname": "postgres"})
    database_url = make_conninfo(**{**base, "dbname": database_name})
    created = False
    try:
        with psycopg.connect(admin_url, autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
        created = True
        first = apply_migrations(database_url)
        repeated = apply_migrations(database_url)
        assert first.applied == tuple(range(1, 31)) and first.skipped == ()
        assert repeated.applied == () and repeated.skipped == tuple(range(1, 31))
        with psycopg.connect(database_url) as connection:
            versions = tuple(
                row[0]
                for row in connection.execute(
                    "SELECT version FROM systematic_fx.schema_migrations ORDER BY version"
                ).fetchall()
            )
        assert versions == tuple(range(1, 31))
        print("M0B MIGRATIONS fresh=1..30 repeated=all-skipped")
        with tempfile.TemporaryDirectory(
            prefix="systematic-fx-m0b-control-artifacts-",
            dir="/private/tmp",
        ) as artifact_directory:
            _exercise_lifecycle(
                database_url,
                artifact_root=Path(artifact_directory),
            )
    finally:
        if created:
            with psycopg.connect(admin_url, autocommit=True) as connection:
                connection.execute(
                    sql.SQL("DROP DATABASE {}").format(sql.Identifier(database_name))
                )
    with psycopg.connect(admin_url) as connection:
        remaining = connection.execute(
            "SELECT count(*) FROM pg_database WHERE datname = %s",
            (database_name,),
        ).fetchone()
    assert remaining is not None and remaining[0] == 0
    print("M0B CLEANUP disposable_databases_remaining=0")


def test_m0b_governed_control_plane_postgres() -> None:
    if os.environ.get("SYSTEMATIC_FX_RUN_M0B_PG_GATE") != "1":
        pytest.skip("set SYSTEMATIC_FX_RUN_M0B_PG_GATE=1 for the disposable M0b gate")
    _main()


if __name__ == "__main__":
    _main()
