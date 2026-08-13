from __future__ import annotations

import os
import secrets
import shutil
import subprocess
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

from systematic_fx.config.settings import Settings
from systematic_fx.db.holdout_isolation import (
    HoldoutIsolationError,
    verify_research_holdout_isolation,
)
from systematic_fx.db.migrations import apply_migrations


def _run_provisioning(database_url: str) -> None:
    psql = shutil.which("psql")
    if psql is None:
        pytest.skip("psql is required for the provisioning-script integration gate")
    try:
        subprocess.run(
            (
                psql,
                "-X",
                "--set=ON_ERROR_STOP=1",
                f"--dbname={database_url}",
                "--file=deploy/postgres/provision_m0b_holdout.sql",
            ),
            check=True,
            cwd=Path.cwd(),
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as error:
        raise AssertionError(
            f"holdout provisioning failed:\nstdout={error.stdout}\nstderr={error.stderr}"
        ) from error


def _exercise_acl(database_url: str, *, login_name: str, login_password: str) -> None:
    with psycopg.connect(database_url, autocommit=True, row_factory=dict_row) as connection:
        identity = connection.execute(
            """
            SELECT namespace_owner.rolname AS schema_owner,
                   table_owner.rolname AS table_owner,
                   relation.relkind, relation.relpersistence,
                   has_schema_privilege(
                       'systematic_fx_research_daemon',
                       'systematic_fx_sealed', 'USAGE') AS research_usage,
                   has_table_privilege(
                       'systematic_fx_research_daemon',
                       'systematic_fx_sealed.holdout_artifacts', 'SELECT')
                       AS research_select,
                   has_table_privilege(
                       'systematic_fx_holdout_executor',
                       'systematic_fx_sealed.holdout_artifacts', 'SELECT')
                       AS executor_select,
                   has_table_privilege(
                       'systematic_fx_holdout_executor',
                       'systematic_fx_sealed.holdout_artifacts', 'UPDATE')
                       AS executor_update
              FROM pg_namespace AS namespace
              JOIN pg_roles AS namespace_owner ON namespace_owner.oid = namespace.nspowner
              JOIN pg_class AS relation ON relation.relnamespace = namespace.oid
              JOIN pg_roles AS table_owner ON table_owner.oid = relation.relowner
             WHERE namespace.nspname = 'systematic_fx_sealed'
               AND relation.relname = 'holdout_artifacts'
            """
        ).fetchone()
        assert identity == {
            "schema_owner": "systematic_fx_holdout_owner",
            "table_owner": "systematic_fx_holdout_owner",
            "relkind": "r",
            "relpersistence": "p",
            "research_usage": False,
            "research_select": False,
            "executor_select": True,
            "executor_update": False,
        }

        governed_write = connection.execute(
            """
            SELECT count(*) AS count
              FROM unnest(ARRAY[
                'artifacts', 'research_run_specs', 'research_run_attempts',
                'm0b_candidates', 'm0b_checkpoints', 'm0b_artifact_links',
                'experiments', 'strategies', 'backtest_runs'
              ]) AS governed(table_name)
             WHERE has_table_privilege(
                 'systematic_fx_research_daemon',
                 format('systematic_fx.%I', table_name),
                 'INSERT,UPDATE,DELETE,TRUNCATE')
            """
        ).fetchone()
        assert governed_write == {"count": 0}

        connection.execute("SET ROLE systematic_fx_research_daemon")
        try:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                connection.execute("SELECT * FROM systematic_fx_sealed.holdout_artifacts LIMIT 1")
        finally:
            connection.execute("RESET ROLE")

        connection.execute("SET ROLE systematic_fx_holdout_executor")
        try:
            assert connection.execute(
                "SELECT count(*) FROM systematic_fx_sealed.holdout_artifacts"
            ).fetchone() == {"count": 0}
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                connection.execute(
                    "UPDATE systematic_fx_sealed.holdout_artifacts SET byte_size = byte_size"
                )
        finally:
            connection.execute("RESET ROLE")

    login_url = make_conninfo(
        **{
            **conninfo_to_dict(database_url),
            "user": login_name,
            "password": login_password,
        }
    )
    report = verify_research_holdout_isolation(
        login_url,
        expected_session_user=login_name,
    )
    assert report.status == "ACCESS_DENIED_VERIFIED"
    assert report.direct_read_denied


def _main() -> None:
    settings = Settings.from_env(working_directory=Path.cwd())
    base = conninfo_to_dict(settings.database_url)
    database_name = f"systematic_fx_m0b_holdout_gate_{os.getpid()}"
    admin_url = make_conninfo(**{**base, "dbname": "postgres"})
    database_url = make_conninfo(**{**base, "dbname": database_name})
    created = False
    login_name = f"systematic_fx_m0b_research_gate_{os.getpid()}"
    login_password = secrets.token_urlsafe(24)
    try:
        with psycopg.connect(admin_url, autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
        created = True
        apply_migrations(database_url)
        _run_provisioning(database_url)
        _run_provisioning(database_url)
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute(
                sql.SQL(
                    "CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB "
                    "NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
                ).format(sql.Identifier(login_name), sql.Literal(login_password))
            )
            connection.execute(
                sql.SQL("GRANT systematic_fx_research_daemon TO {}").format(
                    sql.Identifier(login_name)
                )
            )
        _exercise_acl(
            database_url,
            login_name=login_name,
            login_password=login_password,
        )

        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute(
                """
                CREATE FUNCTION public.m0b_gate_forbidden_definer()
                RETURNS integer LANGUAGE sql SECURITY DEFINER
                AS 'SELECT 1'
                """
            )
            connection.execute(
                "GRANT EXECUTE ON FUNCTION public.m0b_gate_forbidden_definer() TO PUBLIC"
            )
        login_url = make_conninfo(
            **{
                **conninfo_to_dict(database_url),
                "user": login_name,
                "password": login_password,
            }
        )
        with pytest.raises(HoldoutIsolationError, match="SECURITY DEFINER"):
            verify_research_holdout_isolation(
                login_url,
                expected_session_user=login_name,
            )
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute("DROP FUNCTION public.m0b_gate_forbidden_definer()")

            connection.execute(
                """
                CREATE FUNCTION public.m0b_gate_group_only_definer()
                RETURNS integer LANGUAGE sql SECURITY DEFINER
                AS 'SELECT 1'
                """
            )
            connection.execute(
                "REVOKE ALL ON FUNCTION public.m0b_gate_group_only_definer() FROM PUBLIC"
            )
            connection.execute(
                "GRANT EXECUTE ON FUNCTION public.m0b_gate_group_only_definer() "
                "TO systematic_fx_research_daemon"
            )
        with pytest.raises(HoldoutIsolationError, match="SECURITY DEFINER"):
            verify_research_holdout_isolation(
                login_url,
                expected_session_user=login_name,
            )
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute("DROP FUNCTION public.m0b_gate_group_only_definer()")

            connection.execute("CREATE TABLE public.m0b_gate_forbidden_view_owner(value integer)")
            connection.execute(
                "ALTER TABLE public.m0b_gate_forbidden_view_owner "
                "OWNER TO systematic_fx_holdout_owner"
            )
        with pytest.raises(HoldoutIsolationError, match="outside"):
            verify_research_holdout_isolation(
                login_url,
                expected_session_user=login_name,
            )
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute("DROP TABLE public.m0b_gate_forbidden_view_owner")
        print("M0B HOLDOUT provisioning=IDEMPOTENT research=DENIED executor=SELECT_ONLY")
    finally:
        with psycopg.connect(admin_url, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(login_name)))
        if created:
            with psycopg.connect(admin_url, autocommit=True) as connection:
                connection.execute(
                    sql.SQL("DROP DATABASE {}").format(sql.Identifier(database_name))
                )
    with psycopg.connect(admin_url) as connection:
        remaining = connection.execute(
            "SELECT count(*) FROM pg_database WHERE datname = %s", (database_name,)
        ).fetchone()
    assert remaining is not None and remaining[0] == 0
    print("M0B HOLDOUT cleanup disposable_databases_remaining=0")


def test_m0b_holdout_provisioning_postgres() -> None:
    if os.environ.get("SYSTEMATIC_FX_RUN_M0B_PG_GATE") != "1":
        pytest.skip("set SYSTEMATIC_FX_RUN_M0B_PG_GATE=1 for the disposable M0b gate")
    _main()


if __name__ == "__main__":
    _main()
