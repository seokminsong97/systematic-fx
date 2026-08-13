from __future__ import annotations

import os
import secrets
import shutil
import subprocess
import tempfile
from dataclasses import replace
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

from systematic_fx.config.settings import Settings
from systematic_fx.db.m0b_registry import M0bRegistryError, register_m0b_candidate
from systematic_fx.db.m0b_worker_access import M0bWorkerAccessError, verify_m0b_worker_access
from systematic_fx.db.m0b_worker_registry import (
    M0bWorkerRegistryError,
    checkpoint_m0b_work,
    claim_m0b_work,
    fail_m0b_work,
    terminalize_m0b_work,
)
from systematic_fx.db.migrations import apply_migrations
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.m0b.first_passage_store import (
    FirstPassageStore,
    FirstPassageStoreSpec,
    build_first_passage_store,
)
from systematic_fx.research.m0b.model import ArtifactIdentity, RealSliceBuild
from systematic_fx.research.m0b.runner import run_claimed_worker_cycle
from systematic_fx.research.m0b.worker import (
    CandidateWorkSpec,
    NumericAdmissionRules,
    VolatilityBarrierSpec,
    load_candidate_work_artifact,
    publish_candidate_work_manifest,
    publish_signal_artifact,
)
from systematic_fx.research.provenance import build_code_snapshot, dependency_lock_sha256
from tests.integration.test_m0b_control_plane_postgres import (
    _create_fixture,
    _digest,
    _run_spec,
)


def _provision(database_url: str) -> None:
    psql = shutil.which("psql")
    if psql is None:
        pytest.skip("psql is required for the M0b worker capability gate")
    subprocess.run(
        (
            psql,
            "-X",
            "--set=ON_ERROR_STOP=1",
            f"--dbname={database_url}",
            "--file=deploy/postgres/provision_m0b_holdout.sql",
        ),
        cwd=Path.cwd(),
        check=True,
        capture_output=True,
        text=True,
    )


def _runner_label(
    *, event_ts_ns: int, session_id: str, outcome: str, exit_ts_ns: int, net_ticks: int
) -> dict[str, object]:
    return {
        "artifact_schema": "systematic_fx.m0b_quote_label.v1",
        "barrier_id": "tp3of4_sl1of2_h1800",
        "direction": "LONG",
        "entry_eligible": True,
        "entry_price_ticks": 100,
        "entry_ts_ns": event_ts_ns + 1,
        "event_ts_ns": event_ts_ns,
        "exit_price_ticks": 100 + net_ticks + 2,
        "exit_ts_ns": exit_ts_ns,
        "first_touch_ts_ns": exit_ts_ns,
        "first_touch_type": outcome,
        "instrument_id": 1,
        "k_sl_den": 2,
        "k_sl_num": 1,
        "k_tp_den": 4,
        "k_tp_num": 3,
        "label_version": "m0b_quote_labels_v1",
        "max_hold_seconds": 1800,
        "mechanical_outcome_valid": True,
        "net_pnl_ticks": net_ticks,
        "parent_feature_manifest_sha256": _digest("m0b:runner:feature"),
        "session_id": session_id,
        "timeout": False,
    }


def _build_runner_store(root: Path) -> tuple[RealSliceBuild, FirstPassageStore]:
    labels = (
        _runner_label(
            event_ts_ns=100,
            session_id="D1",
            outcome="TP_FIRST",
            exit_ts_ns=130,
            net_ticks=3,
        ),
        _runner_label(
            event_ts_ns=200,
            session_id="D2",
            outcome="SL_FIRST",
            exit_ts_ns=210,
            net_ticks=-1,
        ),
        _runner_label(
            event_ts_ns=300,
            session_id="D3",
            outcome="TP_FIRST",
            exit_ts_ns=310,
            net_ticks=4,
        ),
    )
    label_payload = b"".join(canonical_json_bytes(row) + b"\n" for row in labels)
    label_sha256 = __import__("hashlib").sha256(label_payload).hexdigest()
    label_uri = f"label-{label_sha256}.jsonl"
    (root / label_uri).write_bytes(label_payload)
    (root / label_uri).chmod(0o444)
    source_sha256 = _digest("m0b:runner:source")
    quote_sha256 = _digest("m0b:runner:quote")
    feature_sha256 = _digest("m0b:runner:feature")
    build = RealSliceBuild(
        slice_id="m0b-runner-pg-gate-v1",
        config_hash=_digest("m0b:runner:config"),
        source_manifest=ArtifactIdentity("SOURCE", 1, source_sha256, None, "source.json"),
        quote_manifest=ArtifactIdentity("QUOTE", 3, quote_sha256, source_sha256, "quote.jsonl"),
        feature_manifest=ArtifactIdentity(
            "FEATURE", 3, feature_sha256, quote_sha256, "feature.jsonl"
        ),
        label_manifest=ArtifactIdentity("LABEL", 3, label_sha256, feature_sha256, label_uri),
        sessions=(),
    )
    store = build_first_passage_store(
        FirstPassageStoreSpec(
            slice_id=build.slice_id,
            real_slice_build_sha256=build.sha256,
            label_artifact_sha256=label_sha256,
            feature_artifact_sha256=feature_sha256,
            label_row_count=3,
            label_version="m0b_quote_labels_v1",
            shard_row_target=2,
            max_rows=3,
        ),
        build,
        staged_root=root,
        output_root=root,
    )
    return build, store


def _runner_work_artifact(
    root: Path,
    *,
    fixture: dict[str, object],
    candidate_sha256: str,
    store: FirstPassageStore,
    deterministic_seed: int = 7,
):
    identity = fixture["identity"]
    assert isinstance(identity, dict)
    signals = publish_signal_artifact(
        root,
        candidate_sha256=candidate_sha256,
        feature_sha256=str(identity["feature_sha256"]),
        rows=[
            {
                "artifact_schema": "systematic_fx.m0b_candidate_signal.v1",
                "candidate_sha256": candidate_sha256,
                "event_ts_ns": event_ts,
                "feature_sha256": identity["feature_sha256"],
                "instrument_id": 1,
                "search_fold": 0,
                "session_id": session_id,
            }
            for event_ts, session_id in ((100, "D1"), (200, "D2"), (300, "D3"))
        ],
        max_signals=3,
        search_fold_count=1,
    )
    work = CandidateWorkSpec(
        epoch_sha256=str(fixture["epoch_sha256"]),
        candidate_sha256=candidate_sha256,
        first_passage_store_sha256=store.sha256,
        signals=signals,
        candidate_kind="REAL",
        direction="LONG",
        barrier=VolatilityBarrierSpec(
            barrier_id="tp3of4_sl1of2_h1800",
            k_tp_num=3,
            k_tp_den=4,
            k_sl_num=1,
            k_sl_den=2,
            max_hold_seconds=1800,
        ),
        cooldown_ns=0,
        stress_extra_cost_ticks=1,
        search_fold_count=1,
        max_signals=3,
        max_trades=3,
        checkpoint_shard_interval=1,
        deterministic_seed=deterministic_seed,
        code_snapshot_sha256=str(identity["code_snapshot_sha256"]),
        cost_sha256=str(identity["cost_sha256"]),
        execution_sha256=str(identity["execution_sha256"]),
        split_sha256=str(identity["split_sha256"]),
        admission_rules=NumericAdmissionRules(
            min_raw_events=3,
            min_flat_trades=2,
            min_sequential_trades=2,
            min_active_days=1,
            min_tp_probability_ppm=500_000,
            require_positive_net_ev=True,
            min_positive_search_folds=1,
            max_stressed_cost_ev_floor_ticks=0,
        ),
    )
    uri = publish_candidate_work_manifest(root, work)
    return load_candidate_work_artifact(root / uri)


def _main() -> None:
    settings = Settings.from_env(working_directory=Path.cwd())
    base = conninfo_to_dict(settings.database_url)
    database_name = f"systematic_fx_m0b_worker_gate_{os.getpid()}"
    login_name = f"systematic_fx_m0b_worker_login_{os.getpid()}"
    hijack_login_name = f"systematic_fx_m0b_worker_hijack_{os.getpid()}"
    login_password = secrets.token_urlsafe(24)
    hijack_login_password = secrets.token_urlsafe(24)
    admin_url = make_conninfo(**{**base, "dbname": "postgres"})
    database_url = make_conninfo(**{**base, "dbname": database_name})
    runner_root = Path(tempfile.mkdtemp(prefix="m0b-runner-pg-gate-", dir="/private/tmp"))
    build, store = _build_runner_store(runner_root)
    created = False
    login_created = False
    hijack_login_created = False
    try:
        with psycopg.connect(admin_url, autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
        created = True
        report = apply_migrations(database_url)
        assert report.applied == tuple(range(1, 31))
        _provision(database_url)
        _provision(database_url)

        project_root = Path.cwd().resolve(strict=True)
        code_commit = subprocess.run(
            ("git", "-C", os.fspath(project_root), "rev-parse", "--verify", "HEAD"),
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        code_snapshot = build_code_snapshot(project_root, code_commit=code_commit)

        with psycopg.connect(database_url, autocommit=True, row_factory=dict_row) as admin:
            fixture = _create_fixture(
                admin,
                real_candidate_budget=2,
                identity_overrides={
                    "dataset_sha256": build.sha256,
                    "feature_sha256": build.feature_manifest.content_sha256,
                    "label_sha256": build.label_manifest.content_sha256,
                    "code_commit": code_commit,
                    "code_snapshot_sha256": code_snapshot.sha256,
                    "dependency_lock_sha256": dependency_lock_sha256(project_root),
                },
            )
        identity = fixture["identity"]
        assert isinstance(identity, dict)
        low_level_candidate = {
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
            "barrier": {"k_tp": "0.75", "k_sl": "0.50", "max_hold_minutes": 30},
        }
        low_level_candidate_sha256 = canonical_sha256(low_level_candidate)
        low_level_work_artifact = _runner_work_artifact(
            runner_root,
            fixture=fixture,
            candidate_sha256=low_level_candidate_sha256,
            store=store,
            deterministic_seed=7,
        )
        low_level_run_spec = _run_spec(
            campaign_key=str(fixture["campaign_key"]),
            experiment_key=str(fixture["experiment_key"]),
            epoch_sha256=str(fixture["epoch_sha256"]),
            candidate_sha256=low_level_candidate_sha256,
            identity=identity,
            seed=7,
            work_spec_sha256=low_level_work_artifact.content_sha256,
            work_artifact=low_level_work_artifact,
        )
        low_level_registration = register_m0b_candidate(
            database_url,
            epoch_key=str(fixture["epoch_key"]),
            run_spec=low_level_run_spec,
            candidate_kind="REAL",
            ordinal=1,
            canonical_candidate=low_level_candidate,
            work_artifact=low_level_work_artifact,
        )
        candidate = {
            **low_level_candidate,
            "ordinal": 2,
            "random_seed": 11,
            "barrier": {"k_tp": "0.75", "k_sl": "0.50", "max_hold_minutes": 30},
        }
        candidate_sha256 = canonical_sha256(candidate)
        work_artifact = _runner_work_artifact(
            runner_root,
            fixture=fixture,
            candidate_sha256=candidate_sha256,
            store=store,
            deterministic_seed=11,
        )
        run_spec = _run_spec(
            campaign_key=str(fixture["campaign_key"]),
            experiment_key=str(fixture["experiment_key"]),
            epoch_sha256=str(fixture["epoch_sha256"]),
            candidate_sha256=candidate_sha256,
            identity=identity,
            seed=11,
            work_spec_sha256=work_artifact.content_sha256,
            work_artifact=work_artifact,
        )
        with pytest.raises(M0bRegistryError, match="identities differ"):
            register_m0b_candidate(
                database_url,
                epoch_key=str(fixture["epoch_key"]),
                run_spec=run_spec,
                candidate_kind="REAL",
                ordinal=2,
                canonical_candidate=candidate,
                work_artifact=replace(
                    work_artifact,
                    source_label_sha256=_digest("m0b:forged:label-lineage"),
                ),
            )
        registration = register_m0b_candidate(
            database_url,
            epoch_key=str(fixture["epoch_key"]),
            run_spec=run_spec,
            candidate_kind="REAL",
            ordinal=2,
            canonical_candidate=candidate,
            work_artifact=work_artifact,
        )
        with psycopg.connect(database_url, autocommit=True) as admin:
            trigger_security = admin.execute(
                """
                SELECT owner.rolname AS owner, routine.prosecdef,
                       has_table_privilege(owner.rolname,
                           'systematic_fx.m0b_epochs', 'SELECT') AS can_read
                  FROM pg_proc routine
                  JOIN pg_roles owner ON owner.oid = routine.proowner
                 WHERE routine.oid =
                       'systematic_fx.validate_m0b_candidate_update_context()'::regprocedure
                """
            ).fetchone()
            assert trigger_security is not None
            assert trigger_security[1] is False
            with pytest.raises(psycopg.Error):
                admin.execute(
                    """
                    UPDATE systematic_fx.m0b_candidates
                       SET work_artifact_id = %s
                     WHERE m0b_candidate_id = %s
                    """,
                    (fixture["manifest_artifact_id"], registration.m0b_candidate_id),
                )
            admin.execute(
                "UPDATE systematic_fx.campaigns SET status = 'RUNNING' WHERE campaign_id = %s",
                (fixture["campaign_id"],),
            )
            admin.execute(
                """
                UPDATE systematic_fx.m0b_epochs
                   SET status = 'RUNNING', started_at = statement_timestamp()
                 WHERE m0b_epoch_id = %s
                """,
                (fixture["epoch_id"],),
            )

        with psycopg.connect(admin_url, autocommit=True) as cluster_admin:
            cluster_admin.execute(
                sql.SQL(
                    "CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB "
                    "NOCREATEROLE INHERIT NOREPLICATION NOBYPASSRLS"
                ).format(sql.Identifier(login_name), sql.Literal(login_password))
            )
            cluster_admin.execute(
                sql.SQL("GRANT systematic_fx_m0b_worker TO {}").format(sql.Identifier(login_name))
            )
            cluster_admin.execute(
                sql.SQL(
                    "CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB "
                    "NOCREATEROLE INHERIT NOREPLICATION NOBYPASSRLS"
                ).format(sql.Identifier(hijack_login_name), sql.Literal(hijack_login_password))
            )
            cluster_admin.execute(
                sql.SQL("GRANT systematic_fx_m0b_worker TO {}").format(
                    sql.Identifier(hijack_login_name)
                )
            )
        login_created = True
        hijack_login_created = True
        worker_url = make_conninfo(
            **{
                **conninfo_to_dict(database_url),
                "user": login_name,
                "password": login_password,
            }
        )
        hijack_worker_url = make_conninfo(
            **{
                **conninfo_to_dict(database_url),
                "user": hijack_login_name,
                "password": hijack_login_password,
            }
        )
        access = verify_m0b_worker_access(worker_url, expected_session_user=login_name)
        assert access.capability_count == 4
        assert access.direct_write_count == 0
        assert not access.sealed_access

        with psycopg.connect(admin_url, autocommit=True) as cluster_admin:
            cluster_admin.execute("ALTER ROLE systematic_fx_m0b_worker CREATEROLE")
        try:
            with pytest.raises(M0bWorkerAccessError, match="capability role attributes drifted"):
                verify_m0b_worker_access(worker_url, expected_session_user=login_name)
        finally:
            with psycopg.connect(admin_url, autocommit=True) as cluster_admin:
                cluster_admin.execute("ALTER ROLE systematic_fx_m0b_worker NOCREATEROLE")
        verify_m0b_worker_access(worker_url, expected_session_user=login_name)
        with psycopg.connect(admin_url, autocommit=True) as cluster_admin:
            cluster_admin.execute("ALTER ROLE systematic_fx_m0b_worker_api_owner CREATEROLE")
        try:
            with pytest.raises(M0bWorkerAccessError, match="capability role attributes drifted"):
                verify_m0b_worker_access(worker_url, expected_session_user=login_name)
        finally:
            with psycopg.connect(admin_url, autocommit=True) as cluster_admin:
                cluster_admin.execute("ALTER ROLE systematic_fx_m0b_worker_api_owner NOCREATEROLE")
        verify_m0b_worker_access(worker_url, expected_session_user=login_name)

        with psycopg.connect(admin_url, autocommit=True) as cluster_admin:
            cluster_admin.execute(
                sql.SQL("GRANT CREATE ON DATABASE {} TO {}").format(
                    sql.Identifier(database_name), sql.Identifier(login_name)
                )
            )
        try:
            with pytest.raises(
                M0bWorkerAccessError,
                match="privilege outside its capability role",
            ):
                verify_m0b_worker_access(worker_url, expected_session_user=login_name)
        finally:
            with psycopg.connect(admin_url, autocommit=True) as cluster_admin:
                cluster_admin.execute(
                    sql.SQL("REVOKE CREATE ON DATABASE {} FROM {}").format(
                        sql.Identifier(database_name), sql.Identifier(login_name)
                    )
                )
        verify_m0b_worker_access(worker_url, expected_session_user=login_name)

        with psycopg.connect(database_url, autocommit=True) as admin:
            admin.execute("CREATE TABLE public.m0b_rogue_write (value integer)")
            admin.execute("GRANT INSERT ON public.m0b_rogue_write TO systematic_fx_m0b_worker")
        with pytest.raises(M0bWorkerAccessError, match="forbidden direct object mutation"):
            verify_m0b_worker_access(worker_url, expected_session_user=login_name)
        with psycopg.connect(database_url, autocommit=True) as admin:
            admin.execute("REVOKE ALL ON public.m0b_rogue_write FROM systematic_fx_m0b_worker")
            admin.execute("GRANT SELECT ON public.m0b_rogue_write TO systematic_fx_m0b_worker")
        with pytest.raises(M0bWorkerAccessError, match="direct read allowlist drifted"):
            verify_m0b_worker_access(worker_url, expected_session_user=login_name)
        with psycopg.connect(database_url, autocommit=True) as admin:
            admin.execute("REVOKE ALL ON public.m0b_rogue_write FROM systematic_fx_m0b_worker")
            admin.execute("DROP TABLE public.m0b_rogue_write")
            admin.execute("CREATE SEQUENCE public.m0b_rogue_sequence")
            admin.execute(
                "GRANT USAGE ON SEQUENCE public.m0b_rogue_sequence TO systematic_fx_m0b_worker"
            )
        with pytest.raises(M0bWorkerAccessError, match="forbidden direct object mutation"):
            verify_m0b_worker_access(worker_url, expected_session_user=login_name)
        with psycopg.connect(database_url, autocommit=True) as admin:
            admin.execute(
                "REVOKE ALL ON SEQUENCE public.m0b_rogue_sequence FROM systematic_fx_m0b_worker"
            )
            admin.execute("DROP SEQUENCE public.m0b_rogue_sequence")
            admin.execute(
                """
                CREATE FUNCTION public.m0b_rogue_execute()
                RETURNS integer LANGUAGE sql SECURITY DEFINER AS 'SELECT 1'
                """
            )
            admin.execute("REVOKE ALL ON FUNCTION public.m0b_rogue_execute() FROM PUBLIC")
            admin.execute(
                "GRANT EXECUTE ON FUNCTION public.m0b_rogue_execute() TO systematic_fx_m0b_worker"
            )
        with pytest.raises(M0bWorkerAccessError, match="SECURITY DEFINER allowlist drifted"):
            verify_m0b_worker_access(worker_url, expected_session_user=login_name)
        with psycopg.connect(database_url, autocommit=True) as admin:
            admin.execute(
                "REVOKE ALL ON FUNCTION public.m0b_rogue_execute() FROM systematic_fx_m0b_worker"
            )
            admin.execute("DROP FUNCTION public.m0b_rogue_execute()")
        verify_m0b_worker_access(worker_url, expected_session_user=login_name)

        first_token = _digest("m0b:worker:first-lease")
        first = claim_m0b_work(
            worker_url,
            epoch_key=str(fixture["epoch_key"]),
            worker_id="worker-gate-1",
            lease_token_sha256=first_token,
            lease_seconds=300,
        )
        assert first is not None and first.attempt_number == 1
        assert first.epoch_sha256 == fixture["epoch_sha256"]
        assert first.m0b_candidate_id == low_level_registration.m0b_candidate_id
        assert first.work_spec_sha256 == low_level_work_artifact.content_sha256
        assert first.work_spec_byte_size == low_level_work_artifact.byte_size
        assert (first.attempt_status, first.lease_status) == ("RUNNING", "ACTIVE")
        renewed = claim_m0b_work(
            worker_url,
            epoch_key=str(fixture["epoch_key"]),
            worker_id="worker-gate-1",
            lease_token_sha256=first_token,
            lease_seconds=600,
        )
        assert renewed is not None
        assert renewed.research_run_attempt_id == first.research_run_attempt_id
        assert renewed.leased_until >= first.leased_until
        with (
            psycopg.connect(worker_url, autocommit=True) as worker,
            pytest.raises(psycopg.errors.InsufficientPrivilege),
        ):
            worker.execute("SELECT * FROM systematic_fx.m0b_worker_leases")
        with pytest.raises(M0bWorkerRegistryError):
            claim_m0b_work(
                hijack_worker_url,
                epoch_key=str(fixture["epoch_key"]),
                worker_id="worker-gate-1",
                lease_token_sha256=first_token,
                lease_seconds=300,
            )
        with pytest.raises(M0bWorkerRegistryError):
            fail_m0b_work(
                hijack_worker_url,
                candidate_id=first.m0b_candidate_id,
                attempt_id=first.research_run_attempt_id,
                lease_token_sha256=first_token,
                error_message="cross-login bearer-token hijack",
                retryable=True,
            )
        no_checkpoint_metrics = {
            "active_days": 0,
            "flat_trades": 0,
            "net_pnl_ticks": 0,
            "positive_search_folds": 0,
            "raw_events": 0,
            "sequential_trades": 0,
            "stressed_net_pnl_ticks": 0,
            "tp_probability_ppm": 0,
        }
        with pytest.raises(M0bWorkerRegistryError, match="latest complete checkpoint"):
            terminalize_m0b_work(
                worker_url,
                candidate_id=first.m0b_candidate_id,
                attempt_id=first.research_run_attempt_id,
                lease_token_sha256=first_token,
                result_sha256=_digest("m0b:no-checkpoint-result"),
                result_byte_size=100,
                metrics=no_checkpoint_metrics,
            )
        with psycopg.connect(database_url, autocommit=True) as admin:
            admin.execute("SET session_replication_role = replica")
            try:
                admin.execute(
                    """
                    UPDATE systematic_fx.m0b_worker_leases
                       SET leased_until = statement_timestamp() - interval '1 second'
                     WHERE research_run_attempt_id = %s
                    """,
                    (first.research_run_attempt_id,),
                )
            finally:
                admin.execute("SET session_replication_role = origin")
        expired_incomplete_replay = claim_m0b_work(
            worker_url,
            epoch_key=str(fixture["epoch_key"]),
            worker_id="worker-gate-1",
            lease_token_sha256=first_token,
            lease_seconds=300,
        )
        assert expired_incomplete_replay is not None
        assert expired_incomplete_replay.research_run_attempt_id == first.research_run_attempt_id
        assert expired_incomplete_replay.leased_until > first.leased_until
        failed_status = fail_m0b_work(
            worker_url,
            candidate_id=first.m0b_candidate_id,
            attempt_id=first.research_run_attempt_id,
            lease_token_sha256=first_token,
            error_message="deterministic retry gate",
            retryable=True,
        )
        assert failed_status == "RUNNING"
        assert (
            fail_m0b_work(
                worker_url,
                candidate_id=first.m0b_candidate_id,
                attempt_id=first.research_run_attempt_id,
                lease_token_sha256=first_token,
                error_message="deterministic retry gate",
                retryable=True,
            )
            == "RUNNING"
        )
        with pytest.raises(M0bWorkerRegistryError, match="identity drifted"):
            fail_m0b_work(
                worker_url,
                candidate_id=first.m0b_candidate_id,
                attempt_id=first.research_run_attempt_id,
                lease_token_sha256=first_token,
                error_message="forged retry replay",
                retryable=True,
            )
        retry_token = _digest("m0b:worker:retry-lease")
        retry_claim = claim_m0b_work(
            worker_url,
            epoch_key=str(fixture["epoch_key"]),
            worker_id="worker-gate-retry",
            lease_token_sha256=retry_token,
            lease_seconds=300,
        )
        assert retry_claim is not None and retry_claim.attempt_number == 2
        assert retry_claim.m0b_candidate_id == low_level_registration.m0b_candidate_id
        with psycopg.connect(database_url, autocommit=True) as admin:
            admin.execute("SET session_replication_role = replica")
            try:
                admin.execute(
                    """
                    UPDATE systematic_fx.m0b_worker_leases
                       SET leased_until = statement_timestamp() - interval '1 second'
                     WHERE research_run_attempt_id = %s
                    """,
                    (retry_claim.research_run_attempt_id,),
                )
            finally:
                admin.execute("SET session_replication_role = origin")
        recovered_token = _digest("m0b:worker:recovered-lease")
        recovered = claim_m0b_work(
            worker_url,
            epoch_key=str(fixture["epoch_key"]),
            worker_id="worker-gate-recovered",
            lease_token_sha256=recovered_token,
            lease_seconds=300,
        )
        assert recovered is not None and recovered.attempt_number == 3
        assert recovered.m0b_candidate_id == low_level_registration.m0b_candidate_id
        with psycopg.connect(database_url, row_factory=dict_row) as admin:
            crashed_attempt = admin.execute(
                """
                SELECT status, error_message FROM systematic_fx.research_run_attempts
                 WHERE research_run_attempt_id = %s
                """,
                (retry_claim.research_run_attempt_id,),
            ).fetchone()
        assert crashed_attempt is not None
        assert crashed_attempt["status"] == "FAILED"
        assert "expired" in crashed_attempt["error_message"]
        low_level_trade_sha256 = _digest("m0b:worker:low-level-trade-shard")
        low_level_checkpoint_state = {
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
                    "content_sha256": low_level_trade_sha256,
                    "first_store_shard": 1,
                    "last_store_shard": 1,
                    "ordinal": 1,
                    "relative_uri": (f"candidate-trades-000001-{low_level_trade_sha256}.json"),
                    "row_count": 0,
                }
            ],
            "work_spec_sha256": low_level_work_artifact.content_sha256,
        }
        _, low_level_checkpoint_sha256 = checkpoint_m0b_work(
            worker_url,
            candidate_id=recovered.m0b_candidate_id,
            attempt_id=recovered.research_run_attempt_id,
            lease_token_sha256=recovered_token,
            checkpoint_sequence=1,
            predecessor_sha256=None,
            state=low_level_checkpoint_state,
        )
        _, replayed_checkpoint_sha256 = checkpoint_m0b_work(
            worker_url,
            candidate_id=recovered.m0b_candidate_id,
            attempt_id=recovered.research_run_attempt_id,
            lease_token_sha256=recovered_token,
            checkpoint_sequence=1,
            predecessor_sha256=None,
            state=low_level_checkpoint_state,
        )
        assert replayed_checkpoint_sha256 == low_level_checkpoint_sha256
        second_trade_sha256 = _digest("m0b:worker:multi-store-shard")
        second_trade_identity = {
            "byte_size": 1,
            "content_sha256": second_trade_sha256,
            "first_store_shard": 2,
            "last_store_shard": 3,
            "ordinal": 2,
            "relative_uri": f"candidate-trades-000002-{second_trade_sha256}.json",
            "row_count": 0,
        }
        multi_store_checkpoint_state = {
            **low_level_checkpoint_state,
            "next_shard_ordinal": 4,
            "trade_shards": [
                *low_level_checkpoint_state["trade_shards"],
                second_trade_identity,
            ],
        }
        with pytest.raises(M0bWorkerRegistryError, match="external reference or state"):
            checkpoint_m0b_work(
                worker_url,
                candidate_id=recovered.m0b_candidate_id,
                attempt_id=recovered.research_run_attempt_id,
                lease_token_sha256=recovered_token,
                checkpoint_sequence=2,
                predecessor_sha256=low_level_checkpoint_sha256,
                state={
                    **multi_store_checkpoint_state,
                    "work_spec_sha256": _digest("m0b:forged:checkpoint-work"),
                },
            )
        with pytest.raises(M0bWorkerRegistryError, match="external reference or state"):
            checkpoint_m0b_work(
                worker_url,
                candidate_id=recovered.m0b_candidate_id,
                attempt_id=recovered.research_run_attempt_id,
                lease_token_sha256=recovered_token,
                checkpoint_sequence=2,
                predecessor_sha256=low_level_checkpoint_sha256,
                state={
                    **multi_store_checkpoint_state,
                    "trade_shards": [
                        *low_level_checkpoint_state["trade_shards"],
                        {
                            **second_trade_identity,
                            "relative_uri": "../../sealed/credential.json",
                        },
                    ],
                },
            )
        with pytest.raises(M0bWorkerRegistryError, match="external reference or state"):
            checkpoint_m0b_work(
                worker_url,
                candidate_id=recovered.m0b_candidate_id,
                attempt_id=recovered.research_run_attempt_id,
                lease_token_sha256=recovered_token,
                checkpoint_sequence=2,
                predecessor_sha256=low_level_checkpoint_sha256,
                state={
                    **multi_store_checkpoint_state,
                    "accepted_tp_count": 1,
                    "active_session_ids": ["forged-session"],
                    "fold_net_pnl_ticks": [100],
                    "fold_trade_counts": [1],
                    "matching_label_count": 1,
                    "next_signal_index": 1,
                    "raw_event_count": 1,
                    "raw_net_pnl_ticks": 100,
                    "raw_tp_count": 1,
                    "sequential_net_pnl_ticks": 100,
                    "sequential_stressed_net_pnl_ticks": 99,
                    "sequential_trade_count": 1,
                },
            )
        checkpoint_m0b_work(
            worker_url,
            candidate_id=recovered.m0b_candidate_id,
            attempt_id=recovered.research_run_attempt_id,
            lease_token_sha256=recovered_token,
            checkpoint_sequence=2,
            predecessor_sha256=low_level_checkpoint_sha256,
            state=multi_store_checkpoint_state,
        )
        third_trade_sha256 = _digest("m0b:worker:terminal-store-shard")
        checkpoint_result_sha256 = _digest("m0b:worker:checkpoint-bound-result")
        complete_checkpoint_state = {
            **multi_store_checkpoint_state,
            "complete": True,
            "next_shard_ordinal": 5,
            "result_artifact": {
                "byte_size": 101,
                "classification": "SCREENED_OUT",
                "content_sha256": checkpoint_result_sha256,
                "metrics": no_checkpoint_metrics,
                "relative_uri": f"candidate-result-{checkpoint_result_sha256}.json",
            },
            "trade_shards": [
                *multi_store_checkpoint_state["trade_shards"],
                {
                    "byte_size": 1,
                    "content_sha256": third_trade_sha256,
                    "first_store_shard": 4,
                    "last_store_shard": 4,
                    "ordinal": 3,
                    "relative_uri": f"candidate-trades-000003-{third_trade_sha256}.json",
                    "row_count": 0,
                },
            ],
        }
        _, complete_checkpoint_sha256 = checkpoint_m0b_work(
            worker_url,
            candidate_id=recovered.m0b_candidate_id,
            attempt_id=recovered.research_run_attempt_id,
            lease_token_sha256=recovered_token,
            checkpoint_sequence=3,
            predecessor_sha256=canonical_sha256(
                {
                    "artifact_schema": "systematic_fx.m0b_checkpoint.v1",
                    "checkpoint_sequence": 2,
                    "m0b_candidate_id": recovered.m0b_candidate_id,
                    "predecessor_sha256": low_level_checkpoint_sha256,
                    "research_run_attempt_id": recovered.research_run_attempt_id,
                    "state": multi_store_checkpoint_state,
                }
            ),
            state=complete_checkpoint_state,
        )
        assert len(complete_checkpoint_sha256) == 64
        with pytest.raises(M0bWorkerRegistryError, match="latest complete checkpoint"):
            terminalize_m0b_work(
                worker_url,
                candidate_id=recovered.m0b_candidate_id,
                attempt_id=recovered.research_run_attempt_id,
                lease_token_sha256=recovered_token,
                result_sha256=_digest("m0b:forged-terminal-result"),
                result_byte_size=101,
                metrics=no_checkpoint_metrics,
            )
        with pytest.raises(M0bWorkerRegistryError, match="latest complete checkpoint"):
            terminalize_m0b_work(
                worker_url,
                candidate_id=recovered.m0b_candidate_id,
                attempt_id=recovered.research_run_attempt_id,
                lease_token_sha256=recovered_token,
                result_sha256=checkpoint_result_sha256,
                result_byte_size=102,
                metrics=no_checkpoint_metrics,
            )
        with pytest.raises(M0bWorkerRegistryError, match="latest complete checkpoint"):
            terminalize_m0b_work(
                worker_url,
                candidate_id=recovered.m0b_candidate_id,
                attempt_id=recovered.research_run_attempt_id,
                lease_token_sha256=recovered_token,
                result_sha256=checkpoint_result_sha256,
                result_byte_size=101,
                metrics={**no_checkpoint_metrics, "raw_events": 1},
            )
        with pytest.raises(M0bWorkerRegistryError, match="complete checkpoint is terminal"):
            checkpoint_m0b_work(
                worker_url,
                candidate_id=recovered.m0b_candidate_id,
                attempt_id=recovered.research_run_attempt_id,
                lease_token_sha256=recovered_token,
                checkpoint_sequence=4,
                predecessor_sha256=complete_checkpoint_sha256,
                state=complete_checkpoint_state,
            )
        with pytest.raises(M0bWorkerRegistryError, match="must terminalize, not fail"):
            fail_m0b_work(
                worker_url,
                candidate_id=recovered.m0b_candidate_id,
                attempt_id=recovered.research_run_attempt_id,
                lease_token_sha256=recovered_token,
                error_message="deterministic retry gate",
                retryable=True,
            )
        with psycopg.connect(database_url, autocommit=True) as admin:
            admin.execute("SET session_replication_role = replica")
            try:
                admin.execute(
                    """
                    UPDATE systematic_fx.m0b_worker_leases
                       SET leased_until = statement_timestamp() - interval '1 second'
                     WHERE research_run_attempt_id = %s
                    """,
                    (recovered.research_run_attempt_id,),
                )
            finally:
                admin.execute("SET session_replication_role = origin")
        complete_replay = claim_m0b_work(
            worker_url,
            epoch_key=str(fixture["epoch_key"]),
            worker_id="worker-gate-recovered",
            lease_token_sha256=recovered_token,
            lease_seconds=300,
        )
        assert complete_replay is not None
        assert complete_replay.research_run_attempt_id == recovered.research_run_attempt_id
        assert (complete_replay.attempt_status, complete_replay.lease_status) == (
            "RUNNING",
            "ACTIVE",
        )
        terminalized_screen = terminalize_m0b_work(
            worker_url,
            candidate_id=recovered.m0b_candidate_id,
            attempt_id=recovered.research_run_attempt_id,
            lease_token_sha256=recovered_token,
            result_sha256=checkpoint_result_sha256,
            result_byte_size=101,
            metrics=no_checkpoint_metrics,
        )
        assert terminalized_screen.classification == "SCREENED_OUT"
        released_replay = claim_m0b_work(
            worker_url,
            epoch_key=str(fixture["epoch_key"]),
            worker_id="worker-gate-recovered",
            lease_token_sha256=recovered_token,
            lease_seconds=300,
        )
        assert released_replay is not None
        assert (released_replay.attempt_status, released_replay.lease_status) == (
            "SUCCEEDED",
            "RELEASED",
        )

        cycle = run_claimed_worker_cycle(
            worker_url,
            epoch_key=str(fixture["epoch_key"]),
            worker_id="worker-gate-runner",
            worker_root=runner_root,
        )
        if cycle.status != "COMPLETED":
            raise AssertionError(f"runner E2E failed: {cycle.error}")
        assert cycle.status == "COMPLETED", cycle.error
        assert cycle.result is not None and cycle.result.classification == "REGISTERED"
        assert cycle.result.metrics == {
            "active_days": 3,
            "flat_trades": 3,
            "net_pnl_ticks": 6,
            "positive_search_folds": 1,
            "raw_events": 3,
            "sequential_trades": 3,
            "stressed_net_pnl_ticks": 3,
            "tp_probability_ppm": 666667,
        }
        assert not list(runner_root.glob("m0b-worker-lease-*.json"))
        with psycopg.connect(database_url, row_factory=dict_row) as admin:
            lease = admin.execute(
                """
                SELECT lease_token_sha256
                  FROM systematic_fx.m0b_worker_leases
                 WHERE worker_id = 'worker-gate-runner'
                """
            ).fetchone()
        assert lease is not None
        result_path = runner_root / str(cycle.result.result_relative_uri)
        repeated_terminal = terminalize_m0b_work(
            worker_url,
            candidate_id=registration.m0b_candidate_id,
            attempt_id=int(cycle.research_run_attempt_id),
            lease_token_sha256=str(lease["lease_token_sha256"]),
            result_sha256=str(cycle.result.result_sha256),
            result_byte_size=result_path.stat().st_size,
            metrics=cycle.result.metrics,
        )
        assert repeated_terminal.classification == "REGISTERED"
        terminal_claim_replay = claim_m0b_work(
            worker_url,
            epoch_key=str(fixture["epoch_key"]),
            worker_id="worker-gate-runner",
            lease_token_sha256=str(lease["lease_token_sha256"]),
        )
        assert terminal_claim_replay is not None
        assert (terminal_claim_replay.attempt_status, terminal_claim_replay.lease_status) == (
            "SUCCEEDED",
            "RELEASED",
        )
        idle = run_claimed_worker_cycle(
            worker_url,
            epoch_key=str(fixture["epoch_key"]),
            worker_id="worker-gate-runner-idle",
            worker_root=runner_root,
        )
        assert idle.status == "IDLE"
        assert not list(runner_root.glob("m0b-worker-lease-*.json"))

        with psycopg.connect(worker_url, autocommit=True) as worker:
            for statement in (
                "UPDATE systematic_fx.m0b_epochs SET real_candidate_budget = 99",
                "UPDATE systematic_fx.campaigns SET status = 'CLOSED'",
                "INSERT INTO systematic_fx.m0b_candidates "
                "(m0b_epoch_id,research_run_spec_id,candidate_kind,ordinal,"
                "candidate_sha256,canonical_candidate) VALUES (1,1,'REAL',99,'"
                + _digest("forged")
                + "','{}')",
                ("UPDATE systematic_fx.strategies SET status = 'PAPER_ELIGIBLE' WHERE false"),
                "SELECT * FROM systematic_fx_sealed.holdout_artifacts",
            ):
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    worker.execute(statement)
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                worker.execute("SET ROLE systematic_fx_m0b_worker_api_owner")

        with psycopg.connect(database_url, row_factory=dict_row) as admin:
            observed = admin.execute(
                """
                SELECT candidate.status, candidate.registered_at IS NOT NULL AS registered,
                       count(DISTINCT attempt.research_run_attempt_id) AS attempts,
                       count(DISTINCT decision.m0b_admission_decision_id) AS decisions,
                       count(DISTINCT lease.research_run_attempt_id) AS leases
                  FROM systematic_fx.m0b_candidates candidate
                  JOIN systematic_fx.research_run_attempts attempt
                    USING (research_run_spec_id)
                  LEFT JOIN systematic_fx.m0b_admission_decisions decision
                    USING (m0b_candidate_id)
                  LEFT JOIN systematic_fx.m0b_worker_leases lease
                    USING (m0b_candidate_id)
                 WHERE candidate.m0b_candidate_id = %s
                 GROUP BY candidate.status, candidate.registered_at
                """,
                (registration.m0b_candidate_id,),
            ).fetchone()
        assert observed == {
            "status": "REGISTERED",
            "registered": True,
            "attempts": 1,
            "decisions": 1,
            "leases": 1,
        }
        with psycopg.connect(database_url, row_factory=dict_row) as admin:
            low_level_observed = admin.execute(
                """
                SELECT candidate.status, candidate.registered_at IS NOT NULL AS registered,
                       count(DISTINCT attempt.research_run_attempt_id) AS attempts,
                       count(DISTINCT decision.m0b_admission_decision_id) AS decisions,
                       count(DISTINCT lease.research_run_attempt_id) AS leases
                  FROM systematic_fx.m0b_candidates candidate
                  JOIN systematic_fx.research_run_attempts attempt
                    USING (research_run_spec_id)
                  LEFT JOIN systematic_fx.m0b_admission_decisions decision
                    USING (m0b_candidate_id)
                  LEFT JOIN systematic_fx.m0b_worker_leases lease
                    USING (m0b_candidate_id)
                 WHERE candidate.m0b_candidate_id = %s
                 GROUP BY candidate.status, candidate.registered_at
                """,
                (low_level_registration.m0b_candidate_id,),
            ).fetchone()
        assert low_level_observed == {
            "status": "SCREENED_OUT",
            "registered": False,
            "attempts": 3,
            "decisions": 1,
            "leases": 3,
        }
        print("M0B WORKER actual-login=ALLOWLIST_ONLY retry=RESUMED terminal=DB_DERIVED")
    finally:
        if hijack_login_created:
            with psycopg.connect(admin_url, autocommit=True) as connection:
                connection.execute(
                    sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(hijack_login_name))
                )
        if login_created:
            with psycopg.connect(admin_url, autocommit=True) as connection:
                connection.execute(
                    sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(login_name))
                )
        if created:
            with psycopg.connect(admin_url, autocommit=True) as connection:
                connection.execute(
                    sql.SQL("DROP DATABASE {}").format(sql.Identifier(database_name))
                )
        shutil.rmtree(runner_root)


def test_m0b_worker_capability_postgres() -> None:
    if os.environ.get("SYSTEMATIC_FX_RUN_M0B_PG_GATE") != "1":
        pytest.skip("set SYSTEMATIC_FX_RUN_M0B_PG_GATE=1 for the disposable M0b gate")
    _main()


if __name__ == "__main__":
    _main()
